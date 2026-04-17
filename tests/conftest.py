"""Pytest configuration and fixtures."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.config import Settings
from app.database import Base, get_db
from app.main import app
from app.models import Event, SourceStatus


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_settings():
    """Test settings with SQLite database."""
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        log_level="DEBUG",
        github_poll_interval=60,
        hackernews_poll_interval=120,
        reddit_poll_interval=120,
        rate_limit_enabled=False,
        api_key_enabled=False,
    )


@pytest_asyncio.fixture
async def test_engine(test_settings):
    """Create test database engine."""
    engine = create_async_engine(
        test_settings.database_url,
        echo=False,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )

    # Enable foreign keys for SQLite
    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture
async def test_session_maker(test_engine):
    """Create test session maker."""
    return async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


@pytest_asyncio.fixture
async def db_session(test_session_maker) -> AsyncGenerator[AsyncSession, None]:
    """Create test database session."""
    async with test_session_maker() as session:
        yield session


@pytest_asyncio.fixture
async def test_client(test_engine, test_session_maker):
    """Create test HTTP client."""

    # Override database dependency
    async def override_get_db():
        async with test_session_maker() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client

    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def sample_events(db_session: AsyncSession) -> list[Event]:
    """Create sample events in database."""
    events = [
        Event(
            source="github",
            event_type="PushEvent",
            title="user1 pushed 3 commits to repo/test",
            url="https://github.com/repo/test",
            external_id="github_123",
            event_metadata={"actor": "user1", "repo": "repo/test"},
            timestamp=datetime.now(timezone.utc),
        ),
        Event(
            source="hackernews",
            event_type="story",
            title="Test Story (by user2, 100 points, 50 comments)",
            url="https://news.ycombinator.com/item?id=123",
            external_id="hackernews_123",
            event_metadata={"author": "user2", "score": 100},
            timestamp=datetime.now(timezone.utc),
        ),
        Event(
            source="reddit",
            event_type="post",
            title="r/test: Test Post (by u/user3, 500 upvotes, 20 comments)",
            url="https://reddit.com/r/test/comments/123",
            external_id="reddit_123",
            event_metadata={"subreddit": "test", "author": "user3"},
            timestamp=datetime.now(timezone.utc),
        ),
    ]

    for event in events:
        db_session.add(event)

    await db_session.commit()

    return events


@pytest_asyncio.fixture
async def sample_source_status(db_session: AsyncSession) -> list[SourceStatus]:
    """Create sample source status in database."""
    statuses = [
        SourceStatus(
            source_name="github",
            last_poll=datetime.now(timezone.utc),
            last_success=datetime.now(timezone.utc),
            last_error=None,
            error_count="0",
            is_healthy="true",
        ),
        SourceStatus(
            source_name="hackernews",
            last_poll=datetime.now(timezone.utc),
            last_success=datetime.now(timezone.utc),
            last_error=None,
            error_count="0",
            is_healthy="true",
        ),
    ]

    for status in statuses:
        db_session.add(status)

    await db_session.commit()

    return statuses
