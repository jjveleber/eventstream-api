"""Event API endpoints."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.middleware import limiter, verify_api_key
from app.models import Event
from app.schemas import EventRead
from app.sources.registry import get_all_source_names
from app.streaming.sse import create_event_stream

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(verify_api_key)] if settings.api_key_enabled else [])


@router.get("/events", response_model=list[EventRead])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
async def get_events(
    request: Request,
    source: Annotated[str | None, Query(description="Filter by source name")] = None,
    limit: Annotated[int, Query(description="Maximum number of events", ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(description="Number of events to skip", ge=0)] = 0,
    db: AsyncSession = Depends(get_db),
) -> list[EventRead]:
    """Get list of events with optional filtering.

    Args:
        source: Optional source filter (e.g., 'github', 'hackernews', 'reddit')
        limit: Maximum number of events to return (max 1000)
        offset: Number of events to skip for pagination
        db: Database session

    Returns:
        List of events ordered by timestamp descending
    """
    # Build query
    query = select(Event).order_by(Event.timestamp.desc())

    # Apply source filter if provided
    if source:
        valid_sources = get_all_source_names()
        if source not in valid_sources:
            logger.warning(
                f"Invalid source filter: {source}",
                extra={"source": source, "valid_sources": valid_sources},
            )
            return []
        query = query.where(Event.source == source)

    # Apply pagination
    query = query.offset(offset).limit(limit)

    # Execute query
    result = await db.execute(query)
    events = result.scalars().all()

    logger.info(
        f"Retrieved {len(events)} events",
        extra={
            "source": source,
            "limit": limit,
            "offset": offset,
            "count": len(events),
        },
    )

    return [EventRead.model_validate(event) for event in events]


@router.get("/stream")
@limiter.limit("10/minute")  # Lower limit for SSE streams
async def stream_events(
    request: Request,
    sources: Annotated[
        str | None,
        Query(description="Comma-separated list of sources to filter (e.g., 'github,reddit')"),
    ] = None,
):
    """Stream real-time events via Server-Sent Events (SSE).

    Args:
        sources: Optional comma-separated list of sources to filter

    Returns:
        EventSourceResponse with SSE stream
    """
    # Parse source filters
    source_filters: set[str] | None = None
    if sources:
        source_filters = set(s.strip() for s in sources.split(",") if s.strip())

        # Validate sources
        valid_sources = set(get_all_source_names())
        invalid_sources = source_filters - valid_sources
        if invalid_sources:
            logger.warning(
                f"Invalid source filters: {invalid_sources}",
                extra={
                    "requested": list(source_filters),
                    "invalid": list(invalid_sources),
                    "valid": list(valid_sources),
                },
            )
            # Remove invalid sources
            source_filters = source_filters & valid_sources

    logger.info(
        "New SSE stream connection",
        extra={"source_filters": list(source_filters) if source_filters else "all"},
    )

    return create_event_stream(source_filters)


@router.get("/sources", response_model=list[dict])
async def get_sources():
    """Get list of all available event sources.

    Returns:
        List of source information including names and poll intervals
    """
    from app.sources.registry import ACTIVE_SOURCES

    sources = [
        {
            "name": source.source_name,
            "base_url": source.base_url,
            "poll_interval": source.poll_interval,
        }
        for source in ACTIVE_SOURCES
    ]

    return sources
