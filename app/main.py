"""FastAPI application with background polling."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.database import async_session_maker, close_db, init_db
from app.models import Event, SourceStatus
from app.schemas import EventRead
from app.sources.registry import ACTIVE_SOURCES
from app.streaming.sse import ConnectionManager

# Configure logging
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global polling task
polling_task: asyncio.Task | None = None


async def poll_source(source) -> tuple[str, int, str | None]:
    """Poll a single event source.

    Args:
        source: Event source instance

    Returns:
        Tuple of (source_name, event_count, error_message)
    """
    source_name = source.source_name
    error_message = None
    event_count = 0

    try:
        logger.info(f"Polling {source_name}...")

        # Fetch events from source
        events = await source.fetch_events()

        # Insert events into database
        async with async_session_maker() as session:
            for event_create in events:
                try:
                    # Check if event already exists
                    existing = await session.scalar(
                        select(Event).where(Event.external_id == event_create.external_id)
                    )

                    if existing:
                        logger.debug(
                            f"Event {event_create.external_id} already exists, skipping",
                            extra={"external_id": event_create.external_id},
                        )
                        continue

                    # Insert new event
                    event_model = Event(**event_create.model_dump())
                    session.add(event_model)
                    await session.flush()

                    # Convert to EventRead for broadcasting
                    event_read = EventRead.model_validate(event_model)

                    # Broadcast to SSE connections
                    await ConnectionManager.broadcast_event(event_read)

                    event_count += 1

                except Exception as e:
                    logger.warning(
                        f"Failed to insert event from {source_name}: {e}",
                        extra={
                            "source": source_name,
                            "external_id": event_create.external_id,
                            "error": str(e),
                        },
                    )
                    continue

            await session.commit()

        logger.info(
            f"Processed {event_count} new events from {source_name}",
            extra={
                "source": source_name,
                "new_events": event_count,
            },
        )

    except Exception as e:
        error_message = str(e)
        logger.error(
            f"Error polling {source_name}: {e}",
            extra={
                "source": source_name,
                "error": error_message,
            },
        )

    return source_name, event_count, error_message


async def update_source_status(
    source_name: str,
    event_count: int,
    error_message: str | None,
) -> None:
    """Update source status in database.

    Args:
        source_name: Name of the source
        event_count: Number of events fetched
        error_message: Error message if polling failed
    """
    async with async_session_maker() as session:
        now = datetime.now(timezone.utc)

        # Upsert source status
        stmt = pg_insert(SourceStatus).values(
            source_name=source_name,
            last_poll=now,
            last_success=now if error_message is None else None,
            last_error=error_message,
            error_count=str(1) if error_message else "0",
            is_healthy=str(error_message is None).lower(),
        )

        # On conflict, update fields
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_name"],
            set_={
                "last_poll": now,
                "last_success": stmt.excluded.last_success
                if error_message is None
                else SourceStatus.last_success,
                "last_error": stmt.excluded.last_error,
                "error_count": stmt.excluded.error_count
                if error_message
                else "0",
                "is_healthy": stmt.excluded.is_healthy,
            },
        )

        await session.execute(stmt)
        await session.commit()


async def poll_all_sources() -> None:
    """Background task to poll all event sources.

    Runs continuously, polling sources based on their configured intervals.
    Uses asyncio.gather() for concurrent polling with error isolation.
    """
    logger.info("Starting background polling task...")

    # Track last poll time for each source
    last_poll_times: dict[str, datetime] = {}

    while True:
        try:
            # Determine which sources need polling
            sources_to_poll = []
            now = datetime.now(timezone.utc)

            for source in ACTIVE_SOURCES:
                last_poll = last_poll_times.get(source.source_name)

                # Poll if never polled or interval has elapsed
                if last_poll is None or (now - last_poll).total_seconds() >= source.poll_interval:
                    sources_to_poll.append(source)
                    last_poll_times[source.source_name] = now

            if sources_to_poll:
                logger.debug(
                    f"Polling {len(sources_to_poll)} source(s): {[s.source_name for s in sources_to_poll]}"
                )

                # Poll all sources concurrently (return_exceptions=True prevents one failure from stopping others)
                results = await asyncio.gather(
                    *[poll_source(source) for source in sources_to_poll],
                    return_exceptions=True,
                )

                # Update source statuses
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Polling task failed: {result}")
                        continue

                    source_name, event_count, error_message = result
                    await update_source_status(source_name, event_count, error_message)

            # Sleep for a short interval before checking again
            await asyncio.sleep(10)

        except asyncio.CancelledError:
            logger.info("Polling task cancelled")
            break
        except Exception as e:
            logger.error(f"Unexpected error in polling loop: {e}", exc_info=True)
            await asyncio.sleep(30)  # Back off on error


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager.

    Handles startup and shutdown tasks:
    - Initialize database tables
    - Start background polling task
    - Clean up on shutdown
    """
    global polling_task

    # Startup
    logger.info("Starting EventStream API...")

    # Initialize database
    await init_db()
    logger.info("Database initialized")

    # Start background polling
    polling_task = asyncio.create_task(poll_all_sources())
    logger.info("Background polling task started")

    yield

    # Shutdown
    logger.info("Shutting down EventStream API...")

    # Cancel polling task
    if polling_task:
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass

    # Close database connections
    await close_db()
    logger.info("Database connections closed")

    # Close HTTP clients in sources
    for source in ACTIVE_SOURCES:
        await source.close()
    logger.info("Event source clients closed")


# Create FastAPI app
app = FastAPI(
    title="EventStream API",
    description="Real-time event aggregator from GitHub, HackerNews, and Reddit",
    version="1.0.0",
    lifespan=lifespan,
)

# Add rate limiting middleware
if settings.rate_limit_enabled:
    from slowapi import _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded

    from app.middleware import limiter

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Import and register routers
from app.api import events, health

app.include_router(events.router, prefix="/api", tags=["events"])
app.include_router(health.router, prefix="/api", tags=["health"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "EventStream API",
        "version": "1.0.0",
        "docs": "/docs",
        "stream": "/api/stream",
    }
