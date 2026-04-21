# EventStream API - Development Guidelines

## Project Type

**Backend Microservice (Real-time Event Aggregator)**

EventStream API polls multiple external APIs, normalizes their data into a unified event stream, and pushes updates to connected clients via Server-Sent Events (SSE). Used as backend for real-time dashboards and monitoring tools.

## Tech Stack

- **Framework:** FastAPI 0.115
- **Language:** Python 3.12
- **Database:** PostgreSQL 16 (async with SQLAlchemy)
- **HTTP Client:** httpx (async)
- **SSE:** sse-starlette
- **Deployment:** Docker

## General Development Guidelines

Behavioral guidelines to reduce common LLM coding mistakes.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## Architecture Principles

### FastAPI Best Practices

1. **Project Structure:**
   ```
   app/
   ├── api/          # Route handlers
   ├── sources/      # External API adapters
   ├── streaming/    # SSE connection management
   ├── models.py     # SQLAlchemy models
   ├── schemas.py    # Pydantic models
   ├── database.py   # DB connection
   └── config.py     # Settings
   ```

2. **Async First:**
   - Use `async def` for all route handlers
   - Use `await` for all I/O operations (DB, HTTP, cache)
   - Never use blocking calls (`requests`, `time.sleep`) - use async equivalents

3. **Dependency Injection:**
   ```python
   # Good: Use FastAPI dependencies
   from fastapi import Depends
   from app.database import get_db
   
   @app.get("/events")
   async def get_events(db: AsyncSession = Depends(get_db)):
       events = await db.execute(select(Event))
       return events.scalars().all()
   ```

4. **Pydantic Models:**
   - Use Pydantic for request/response validation (not dataclasses)
   - One schema per model minimum (EventRead, EventCreate)
   - Use `ConfigDict` for ORM mode: `model_config = ConfigDict(from_attributes=True)`

### External API Integration

1. **Adapter Pattern:**
   ```python
   class BaseEventSource(ABC):
       """Abstract base for all external API sources."""
       
       source_name: str
       base_url: str
       poll_interval: int  # seconds
       
       @abstractmethod
       async def fetch_events(self) -> List[Event]:
           """Fetch and normalize events from external API."""
           pass
       
       async def _make_request(self, endpoint: str) -> dict:
           """Make async HTTP request with error handling."""
           async with httpx.AsyncClient() as client:
               response = await client.get(f"{self.base_url}/{endpoint}")
               response.raise_for_status()
               return response.json()
   ```

2. **Event Normalization:**
   - All sources must return unified `Event` schema
   - Required fields: `type`, `source`, `timestamp`, `title`, `url`
   - Optional: `metadata` (JSONField for source-specific data)

3. **Polling Strategy:**
   - Background task runs on startup: `@app.on_event("startup")`
   - Each source has configurable poll interval
   - Use `asyncio.gather()` to poll multiple sources concurrently
   - Store last poll timestamp to avoid duplicate events

4. **Error Handling:**
   - Don't crash poller if one source fails
   - Log error, update source status, continue with other sources
   - Retry on next poll cycle (don't infinite retry)
   - Store last error in database for monitoring

### Server-Sent Events (SSE)

1. **Connection Management:**
   ```python
   from sse_starlette.sse import EventSourceResponse
   from collections import defaultdict
   
   # In-memory connection registry
   active_connections: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
   
   @app.get("/stream")
   async def stream_events(sources: str = Query(None)):
       connection_id = str(uuid.uuid4())
       queue = asyncio.Queue()
       active_connections[connection_id] = queue
       
       async def event_generator():
           try:
               while True:
                   event = await queue.get()
                   yield {
                       "event": "new_event",
                       "data": event.json()
                   }
           finally:
               del active_connections[connection_id]
       
       return EventSourceResponse(event_generator())
   ```

2. **Broadcasting:**
   - When new event arrives, push to all active SSE connections
   - Filter events based on client query params (`?sources=github,reddit`)
   - Handle disconnects gracefully (remove from registry)

3. **SSE Format:**
   ```
   event: new_event
   data: {"type": "issue", "source": "github", "title": "...", "url": "..."}
   
   ```

### Database Design (SQLAlchemy Async)

1. **Async Setup:**
   ```python
   from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
   from sqlalchemy.orm import sessionmaker
   
   engine = create_async_engine(DATABASE_URL, echo=True)
   async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
   
   async def get_db():
       async with async_session() as session:
           yield session
   ```

2. **Model Example:**
   ```python
   from sqlalchemy import Column, String, DateTime, JSON
   from sqlalchemy.dialects.postgresql import UUID
   import uuid
   
   class Event(Base):
       __tablename__ = "events"
       
       id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
       source = Column(String(50), nullable=False, index=True)
       event_type = Column(String(50), nullable=False)
       title = Column(String(500), nullable=False)
       url = Column(String(1000), nullable=False)
       external_id = Column(String(255), unique=True, nullable=False)
       metadata = Column(JSON, default={})
       timestamp = Column(DateTime(timezone=True), nullable=False)
       created_at = Column(DateTime(timezone=True), server_default=func.now())
       
       __table_args__ = (
           Index('idx_source_timestamp', 'source', 'timestamp'),
       )
   ```

3. **Deduplication:**
   - Use `external_id` field (unique constraint)
   - Before inserting, check if event exists: `await db.scalar(select(Event).where(Event.external_id == external_id))`
   - Use upsert pattern for idempotency

### Security

1. **Rate Limiting:**
   - Limit SSE connections per IP (prevent DoS)
   - Use `slowapi` library: `@limiter.limit("10/minute")`

2. **Input Validation:**
   - Validate all query params with Pydantic
   - Limit page size on `/events` endpoint (max 1000)
   - Sanitize source filter inputs

3. **External API Safety:**
   - Set timeout on all HTTP requests (10 seconds)
   - Validate response schema before processing
   - Never execute external data as code

### Testing Requirements

1. **Coverage Target:** 70%+ for adapters, streaming, API

2. **Test Structure:**
   ```
   tests/
   ├── test_sources.py     # External API adapters
   ├── test_streaming.py   # SSE connection handling
   ├── test_api.py         # REST endpoints
   └── conftest.py         # Fixtures, test database
   ```

3. **What to Test:**
   - ✅ Each source adapter (fetch and normalize)
   - ✅ Event deduplication logic
   - ✅ SSE connection and broadcast
   - ✅ REST API endpoints (list events, source status)
   - ✅ Background polling task
   - ✅ Error handling (external API failures)

4. **Testing Tools:**
   - `pytest` + `pytest-asyncio`
   - `httpx.AsyncClient` for testing FastAPI endpoints
   - `respx` library to mock external HTTP calls
   - Test database: SQLite in-memory

5. **Async Testing:**
   ```python
   import pytest
   from httpx import AsyncClient
   
   @pytest.mark.asyncio
   async def test_get_events():
       async with AsyncClient(app=app, base_url="http://test") as client:
           response = await client.get("/events")
       assert response.status_code == 200
   ```

### Code Quality Standards

1. **Type Hints (Critical for Python):**
   ```python
   # Good: Full type hints
   async def fetch_events(self, limit: int = 100) -> List[Event]:
       ...
   
   # Good: Use typing module for complex types
   from typing import Optional, Dict, Any
   
   async def process_event(data: Dict[str, Any]) -> Optional[Event]:
       ...
   ```

2. **Error Handling:**
   - Use FastAPI's HTTPException for API errors
   - Catch specific exceptions (not bare `except:`)
   - Return meaningful error messages
   - Log all errors with structured logging

3. **Logging:**
   ```python
   import logging
   
   logger = logging.getLogger(__name__)
   
   # Good: Structured logging
   logger.info(f"Polling {source_name}", extra={
       "source": source_name,
       "last_poll": last_poll_time,
       "event_count": len(events)
   })
   ```

4. **Code Style:**
   - Use `black` formatter (line length: 100)
   - Use `ruff` linter
   - Follow PEP 8
   - Docstrings for all public functions (Google style)

### Performance Considerations

1. **Async Concurrency:**
   ```python
   # Good: Poll all sources concurrently
   results = await asyncio.gather(
       source1.fetch_events(),
       source2.fetch_events(),
       source3.fetch_events(),
       return_exceptions=True  # Don't fail all if one fails
   )
   ```

2. **Database:**
   - Use async SQLAlchemy (don't block event loop)
   - Batch insert events (don't insert one by one)
   - Add indexes on frequently queried fields
   - Implement pagination on `/events` endpoint

3. **Memory:**
   - Limit SSE connection count (store in Redis if scaling)
   - Clean up old events periodically (keep last 1000 per source)
   - Don't store full event stream in memory

### Documentation Requirements

1. **README.md Must Include:**
   - What problem this solves
   - Quick start (Docker up)
   - API documentation with examples
   - How to connect SSE client (curl + JavaScript example)
   - How to add new event sources
   - Architecture diagram

2. **API Documentation:**
   - FastAPI auto-generates OpenAPI docs at `/docs`
   - Add descriptions to all endpoints
   - Include response examples

3. **Adding New Sources:**
   ```markdown
   ## Adding a New Event Source
   
   1. Create adapter in `app/sources/newsource.py`:
      - Inherit from `BaseEventSource`
      - Implement `fetch_events()` method
      - Normalize data to `Event` schema
   
   2. Register in `app/sources/registry.py`:
      - Add to `ACTIVE_SOURCES` list
   
   3. Test adapter with `pytest tests/test_sources.py::test_newsource`
   ```

## FastAPI-Specific Patterns

### Settings Management

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str
    log_level: str = "INFO"
    github_poll_interval: int = 60
    hackernews_poll_interval: int = 120
    
    class Config:
        env_file = ".env"

settings = Settings()
```

### Background Task Pattern

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    task = asyncio.create_task(poll_all_sources())
    yield
    # Shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)
```

### Dependency Injection for Config

```python
from fastapi import Depends

def get_settings() -> Settings:
    return Settings()

@app.get("/status")
async def status(settings: Settings = Depends(get_settings)):
    return {"poll_intervals": {
        "github": settings.github_poll_interval,
        "hackernews": settings.hackernews_poll_interval
    }}
```

## Deployment

### Docker

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Environment Variables

Required:
- `DATABASE_URL`: PostgreSQL connection (e.g., `postgresql+asyncpg://user:pass@host/db`)

Optional:
- `LOG_LEVEL`: Logging level (default: INFO)
- `GITHUB_POLL_INTERVAL`: Seconds between GitHub polls (default: 60)
- `HACKERNEWS_POLL_INTERVAL`: Seconds between HN polls (default: 120)

## Definition of Done

A feature is complete when:
- ✅ Code written with full type hints
- ✅ Tests written with >70% coverage
- ✅ All async operations use `await` (no blocking calls)
- ✅ External API adapters handle errors gracefully
- ✅ SSE streaming works with client example
- ✅ Database queries use async SQLAlchemy
- ✅ API documented in OpenAPI (FastAPI /docs)
- ✅ Docker container builds and runs
- ✅ Logging added for monitoring
- ✅ No secrets in code

## Common Pitfalls to Avoid

1. **Don't use `requests` library** - use `httpx` (async)
2. **Don't use `time.sleep()`** - use `asyncio.sleep()`
3. **Don't use sync SQLAlchemy** - use async engine
4. **Don't forget `await`** - will cause silent failures
5. **Don't store SSE connections in global dict** - will leak memory without cleanup
6. **Don't poll too frequently** - respect external API rate limits
7. **Don't expose raw errors to clients** - use HTTPException
8. **Don't skip event deduplication** - will create duplicate events

This is a portfolio project demonstrating async Python expertise - make it production-grade.
