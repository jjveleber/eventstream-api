"""Integration tests for background polling with real database."""

import asyncio
from datetime import datetime, timezone

import pytest
import pytest_asyncio
import respx
from httpx import Response
from sqlalchemy import select
from testcontainers.postgres import PostgresContainer

from app.database import Base, async_session_maker, engine
from app.main import poll_source, update_source_status
from app.models import Event, SourceStatus
from app.sources.github import GitHubEventSource


@pytest.fixture(scope="module")
def postgres_container():
    """Start PostgreSQL container for integration tests."""
    with PostgresContainer("postgres:16") as postgres:
        yield postgres


@pytest_asyncio.fixture
async def postgres_engine(postgres_container):
    """Create async engine for PostgreSQL container."""
    from sqlalchemy.ext.asyncio import create_async_engine

    db_url = postgres_container.get_connection_url()
    # Handle both postgresql:// and postgresql+psycopg2://
    if "postgresql+psycopg2://" in db_url:
        db_url = db_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")
    elif "postgresql://" in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+asyncpg://")

    test_engine = create_async_engine(db_url, echo=False)

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield test_engine

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await test_engine.dispose()


@pytest_asyncio.fixture
async def postgres_session(postgres_engine):
    """Create session for PostgreSQL tests."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    session_maker = async_sessionmaker(
        postgres_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with session_maker() as session:
        yield session


@pytest.mark.asyncio
@respx.mock
async def test_poll_source_integration(postgres_session):
    """Test full polling flow with real database."""
    # Mock GitHub API
    mock_events = [
        {
            "id": "12345",
            "type": "PushEvent",
            "actor": {"login": "testuser"},
            "repo": {"name": "test/repo"},
            "payload": {"ref": "refs/heads/main", "commits": [{"sha": "abc"}]},
            "created_at": "2026-04-17T10:00:00Z",
        }
    ]

    respx.get("https://api.github.com/events").mock(
        return_value=Response(200, json=mock_events)
    )

    # Create source and poll
    source = GitHubEventSource()

    # Poll source (this will insert events into DB)
    # We need to patch async_session_maker to use our test session
    from unittest.mock import AsyncMock, patch

    async def mock_session_maker():
        yield postgres_session

    with patch("app.main.async_session_maker") as mock_maker:
        mock_maker.return_value.__aenter__ = AsyncMock(return_value=postgres_session)
        mock_maker.return_value.__aexit__ = AsyncMock(return_value=None)

        source_name, event_count, error_message = await poll_source(source)

    # Verify results
    assert source_name == "github"
    assert event_count == 1
    assert error_message is None

    # Verify event was inserted
    result = await postgres_session.execute(select(Event))
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].source == "github"
    assert events[0].external_id == "github_12345"

    await source.close()


@pytest.mark.asyncio
async def test_update_source_status_integration(postgres_session):
    """Test source status update with real database."""
    from unittest.mock import AsyncMock, patch

    async def mock_session_maker():
        yield postgres_session

    with patch("app.main.async_session_maker") as mock_maker:
        mock_maker.return_value.__aenter__ = AsyncMock(return_value=postgres_session)
        mock_maker.return_value.__aexit__ = AsyncMock(return_value=None)

        # Update status (success)
        await update_source_status("github", 5, None)

    # Verify status was created/updated
    result = await postgres_session.execute(
        select(SourceStatus).where(SourceStatus.source_name == "github")
    )
    status = result.scalar_one()

    assert status.source_name == "github"
    assert status.is_healthy == "true"
    assert status.last_error is None
    assert status.error_count == "0"

    # Update with error
    with patch("app.main.async_session_maker") as mock_maker:
        mock_maker.return_value.__aenter__ = AsyncMock(return_value=postgres_session)
        mock_maker.return_value.__aexit__ = AsyncMock(return_value=None)

        await update_source_status("github", 0, "API Error")

    # Verify error was recorded
    await postgres_session.refresh(status)
    assert status.is_healthy == "false"
    assert status.last_error == "API Error"
    assert status.error_count == "1"


@pytest.mark.asyncio
@respx.mock
async def test_duplicate_event_prevention(postgres_session):
    """Test that duplicate events are not inserted."""
    # Mock same event twice
    mock_events = [
        {
            "id": "12345",
            "type": "PushEvent",
            "actor": {"login": "testuser"},
            "repo": {"name": "test/repo"},
            "payload": {"ref": "refs/heads/main", "commits": [{"sha": "abc"}]},
            "created_at": "2026-04-17T10:00:00Z",
        }
    ]

    respx.get("https://api.github.com/events").mock(
        return_value=Response(200, json=mock_events)
    )

    source = GitHubEventSource()

    from unittest.mock import AsyncMock, patch

    async def mock_session_maker():
        yield postgres_session

    # First poll - should insert event
    with patch("app.main.async_session_maker") as mock_maker:
        mock_maker.return_value.__aenter__ = AsyncMock(return_value=postgres_session)
        mock_maker.return_value.__aexit__ = AsyncMock(return_value=None)

        source_name, event_count, error_message = await poll_source(source)

    assert event_count == 1

    # Second poll - should skip duplicate
    with patch("app.main.async_session_maker") as mock_maker:
        mock_maker.return_value.__aenter__ = AsyncMock(return_value=postgres_session)
        mock_maker.return_value.__aexit__ = AsyncMock(return_value=None)

        source_name, event_count, error_message = await poll_source(source)

    assert event_count == 0  # No new events

    # Verify only one event in DB
    result = await postgres_session.execute(select(Event))
    events = result.scalars().all()
    assert len(events) == 1

    await source.close()
