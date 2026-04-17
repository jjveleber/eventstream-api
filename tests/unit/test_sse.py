"""Unit tests for SSE connection management."""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.schemas import EventRead
from app.streaming.sse import ConnectionManager


@pytest.mark.asyncio
async def test_add_remove_connection():
    """Test adding and removing SSE connections."""
    # Initially no connections
    assert ConnectionManager.get_connection_count() == 0

    # Add connection
    connection_id = str(uuid4())
    queue = ConnectionManager.add_connection(connection_id)

    assert ConnectionManager.get_connection_count() == 1
    assert isinstance(queue, asyncio.Queue)

    # Remove connection
    ConnectionManager.remove_connection(connection_id)
    assert ConnectionManager.get_connection_count() == 0


@pytest.mark.asyncio
async def test_broadcast_event():
    """Test broadcasting events to connections."""
    # Create test event
    event = EventRead(
        id=uuid4(),
        source="github",
        event_type="PushEvent",
        title="Test event",
        url="https://github.com/test",
        external_id="test_123",
        metadata={},
        timestamp=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
    )

    # Add two connections
    connection_id1 = str(uuid4())
    queue1 = ConnectionManager.add_connection(connection_id1)

    connection_id2 = str(uuid4())
    queue2 = ConnectionManager.add_connection(connection_id2)

    # Broadcast event
    count = await ConnectionManager.broadcast_event(event)

    assert count == 2

    # Both queues should have the event
    event1 = await asyncio.wait_for(queue1.get(), timeout=1.0)
    assert event1.id == event.id

    event2 = await asyncio.wait_for(queue2.get(), timeout=1.0)
    assert event2.id == event.id

    # Cleanup
    ConnectionManager.remove_connection(connection_id1)
    ConnectionManager.remove_connection(connection_id2)


@pytest.mark.asyncio
async def test_broadcast_with_source_filter():
    """Test broadcasting events with source filters."""
    # Create events from different sources
    github_event = EventRead(
        id=uuid4(),
        source="github",
        event_type="PushEvent",
        title="GitHub event",
        url="https://github.com/test",
        external_id="github_123",
        metadata={},
        timestamp=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
    )

    reddit_event = EventRead(
        id=uuid4(),
        source="reddit",
        event_type="post",
        title="Reddit event",
        url="https://reddit.com/test",
        external_id="reddit_123",
        metadata={},
        timestamp=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
    )

    # Connection 1: only GitHub events
    connection_id1 = str(uuid4())
    queue1 = ConnectionManager.add_connection(connection_id1, source_filters={"github"})

    # Connection 2: all events
    connection_id2 = str(uuid4())
    queue2 = ConnectionManager.add_connection(connection_id2)

    # Broadcast GitHub event
    count = await ConnectionManager.broadcast_event(github_event)
    assert count == 2  # Both connections receive it

    # Broadcast Reddit event
    count = await ConnectionManager.broadcast_event(reddit_event)
    assert count == 1  # Only connection 2 receives it

    # Check queues
    assert queue1.qsize() == 1  # Only GitHub event
    assert queue2.qsize() == 2  # Both events

    # Cleanup
    ConnectionManager.remove_connection(connection_id1)
    ConnectionManager.remove_connection(connection_id2)


@pytest.mark.asyncio
async def test_broadcast_to_no_connections():
    """Test broadcasting when no connections exist."""
    event = EventRead(
        id=uuid4(),
        source="github",
        event_type="PushEvent",
        title="Test event",
        url="https://github.com/test",
        external_id="test_123",
        metadata={},
        timestamp=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
    )

    count = await ConnectionManager.broadcast_event(event)
    assert count == 0
