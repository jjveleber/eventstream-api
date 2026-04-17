"""Health check endpoints."""

import logging

from fastapi import APIRouter, Depends
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import SourceStatus
from app.schemas import HealthResponse
from app.streaming.sse import ConnectionManager

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check(db: AsyncSession = Depends(get_db)) -> HealthResponse:
    """Health check endpoint.

    Checks:
    - Database connectivity
    - Source health status
    - SSE connection count

    Returns:
        Health status with details
    """
    # Check database connectivity
    db_status = "healthy"
    try:
        await db.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "unhealthy"

    # Get source statuses
    result = await db.execute(select(SourceStatus))
    source_statuses = result.scalars().all()

    sources = []
    for status in source_statuses:
        sources.append({
            "name": status.source_name,
            "is_healthy": status.is_healthy == "true",
            "last_poll": status.last_poll.isoformat() if status.last_poll else None,
            "last_success": status.last_success.isoformat() if status.last_success else None,
            "last_error": status.last_error,
        })

    # Overall status
    all_healthy = db_status == "healthy" and all(
        s["is_healthy"] for s in sources if sources
    )
    overall_status = "healthy" if all_healthy else "degraded"

    return HealthResponse(
        status=overall_status,
        database=db_status,
        sources=sources,
    )


@router.get("/metrics")
async def get_metrics(db: AsyncSession = Depends(get_db)):
    """Get system metrics.

    Returns:
        Metrics about SSE connections, events, and sources
    """
    # Get SSE connection count
    sse_connections = ConnectionManager.get_connection_count()

    # Get source statuses
    result = await db.execute(select(SourceStatus))
    source_statuses = result.scalars().all()

    # Get event counts per source
    from app.models import Event
    from sqlalchemy import func

    event_counts_result = await db.execute(
        select(Event.source, func.count(Event.id))
        .group_by(Event.source)
    )
    event_counts = {row[0]: row[1] for row in event_counts_result}

    return {
        "sse_connections": sse_connections,
        "sources": {
            status.source_name: {
                "is_healthy": status.is_healthy == "true",
                "last_poll": status.last_poll.isoformat() if status.last_poll else None,
                "last_success": status.last_success.isoformat() if status.last_success else None,
                "error_count": int(status.error_count) if status.error_count else 0,
                "total_events": event_counts.get(status.source_name, 0),
            }
            for status in source_statuses
        },
    }
