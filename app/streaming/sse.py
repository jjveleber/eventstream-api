"""Server-Sent Events (SSE) connection management."""

import asyncio
import logging
import uuid
from collections import defaultdict
from typing import Any

from sse_starlette.sse import EventSourceResponse

from app.schemas import EventRead

logger = logging.getLogger(__name__)

# In-memory connection registry: connection_id -> (queue, filters)
active_connections: dict[str, tuple[asyncio.Queue, set[str]]] = {}


class ConnectionManager:
    """Manages SSE connections and broadcasts events to clients."""

    @staticmethod
    def add_connection(connection_id: str, source_filters: set[str] | None = None) -> asyncio.Queue:
        """Add a new SSE connection.

        Args:
            connection_id: Unique identifier for this connection
            source_filters: Set of source names to filter events (None = all sources)

        Returns:
            asyncio.Queue for this connection
        """
        queue: asyncio.Queue = asyncio.Queue()
        filters = source_filters or set()

        active_connections[connection_id] = (queue, filters)

        logger.info(
            f"SSE connection added: {connection_id}",
            extra={
                "connection_id": connection_id,
                "filters": list(filters) if filters else "all",
                "total_connections": len(active_connections),
            },
        )

        return queue

    @staticmethod
    def remove_connection(connection_id: str) -> None:
        """Remove an SSE connection.

        Args:
            connection_id: Unique identifier for the connection to remove
        """
        if connection_id in active_connections:
            del active_connections[connection_id]
            logger.info(
                f"SSE connection removed: {connection_id}",
                extra={
                    "connection_id": connection_id,
                    "remaining_connections": len(active_connections),
                },
            )

    @staticmethod
    async def broadcast_event(event: EventRead) -> int:
        """Broadcast event to all active SSE connections.

        Filters events based on each connection's source filters.

        Args:
            event: Event to broadcast

        Returns:
            Number of connections that received the event
        """
        if not active_connections:
            return 0

        broadcast_count = 0

        for connection_id, (queue, filters) in list(active_connections.items()):
            try:
                # Apply source filter
                if filters and event.source not in filters:
                    continue

                # Put event in queue (non-blocking)
                await queue.put(event)
                broadcast_count += 1

            except Exception as e:
                logger.error(
                    f"Failed to broadcast to connection {connection_id}: {e}",
                    extra={
                        "connection_id": connection_id,
                        "error": str(e),
                    },
                )

        if broadcast_count > 0:
            logger.debug(
                f"Event broadcasted to {broadcast_count} connection(s)",
                extra={
                    "event_id": str(event.id),
                    "source": event.source,
                    "broadcast_count": broadcast_count,
                },
            )

        return broadcast_count

    @staticmethod
    def get_connection_count() -> int:
        """Get count of active SSE connections.

        Returns:
            Number of active connections
        """
        return len(active_connections)


async def event_stream_generator(
    connection_id: str,
    queue: asyncio.Queue,
) -> Any:
    """Generate SSE events from queue.

    Args:
        connection_id: Unique connection identifier
        queue: Queue containing events for this connection

    Yields:
        SSE event dictionaries
    """
    try:
        while True:
            # Wait for next event in queue
            event: EventRead = await queue.get()

            # Convert event to SSE format
            yield {
                "event": "new_event",
                "data": event.model_dump_json(),
            }

    except asyncio.CancelledError:
        logger.info(f"SSE stream cancelled for connection {connection_id}")
        raise
    except Exception as e:
        logger.error(
            f"Error in SSE generator for {connection_id}: {e}",
            extra={
                "connection_id": connection_id,
                "error": str(e),
            },
        )
        raise
    finally:
        # Clean up connection on disconnect
        ConnectionManager.remove_connection(connection_id)


def create_event_stream(source_filters: set[str] | None = None) -> EventSourceResponse:
    """Create a new SSE event stream.

    Args:
        source_filters: Optional set of source names to filter events

    Returns:
        EventSourceResponse for FastAPI
    """
    connection_id = str(uuid.uuid4())
    queue = ConnectionManager.add_connection(connection_id, source_filters)

    return EventSourceResponse(
        event_stream_generator(connection_id, queue),
        headers={
            "X-Connection-ID": connection_id,
        },
    )
