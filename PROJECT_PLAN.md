# EventStream API - Project Plan

**Project Type:** Backend Microservice (Real-time Event Aggregator)  
**Build Time:** 1-2 days  
**Difficulty:** Intermediate

## What You're Building

EventStream API polls external APIs (GitHub, HackerNews, Reddit), normalizes data into unified events, and streams them real-time via Server-Sent Events (SSE). Backend for dashboards needing live data from multiple sources.

**Key Value:** One API provides real-time feed from multiple sources with unified schema.

## Clarifying Questions (Ask User First)

1. **Which external APIs?** GitHub events, HackerNews, Reddit, OpenWeather?  
   *Recommendation: Start with 3 free APIs*

2. **Event storage?** PostgreSQL for replay or stream-only (ephemeral)?  
   *Recommendation: Store last 1000 events per source for replay*

3. **Polling frequency?** Every 60s for GitHub, 120s for HackerNews?  
   *Recommendation: Yes, configurable per source*

4. **Filtering?** Let clients filter stream by source/type?  
   *Recommendation: Yes, via query params*

## Tech Stack

- **Framework:** FastAPI 0.115 + Python 3.12
- **Database:** PostgreSQL 16 (async SQLAlchemy)
- **HTTP Client:** httpx (async)
- **SSE:** sse-starlette
- **Deployment:** Docker

## Implementation Plan

### Phase 1: Foundation (2 hours)

**Setup:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi==0.115.0 uvicorn[standard]==0.30.6 httpx==0.27.2 \
  sse-starlette==2.1.3 sqlalchemy[asyncio]==2.0.31 asyncpg==0.29.0 \
  pydantic-settings==2.4.0

mkdir -p app/{api,sources,streaming}
touch app/{__init__.py,config.py,database.py,models.py,schemas.py}
touch app/api/{__init__.py,events.py,health.py}
touch app/sources/{__init__.py,base.py,github.py,hackernews.py}
touch app/streaming/{__init__.py,sse.py}
```

**Models (app/models.py):**
```python
from sqlalchemy import Column, String, DateTime, JSON, Index
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
    
    __table_args__ = (Index('idx_source_timestamp', 'source', 'timestamp'),)
```

### Phase 2: External API Adapters (3 hours)

**Base Adapter (app/sources/base.py):**
```python
from abc import ABC, abstractmethod
import httpx

class BaseEventSource(ABC):
    source_name: str
    base_url: str
    poll_interval: int
    
    @abstractmethod
    async def fetch_events(self) -> List[Event]:
        pass
    
    async def _make_request(self, endpoint: str):
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{self.base_url}/{endpoint}")
            response.raise_for_status()
            return response.json()
```

**GitHub Adapter (app/sources/github.py):**
```python
class GitHubEventSource(BaseEventSource):
    source_name = "github"
    base_url = "https://api.github.com"
    poll_interval = 60
    
    async def fetch_events(self) -> List[Event]:
        data = await self._make_request("/events/public")
        events = []
        
        for item in data[:30]:
            events.append(Event(
                source="github",
                event_type=item["type"],
                title=item["repo"]["name"],
                url=f"https://github.com/{item['repo']['name']}",
                external_id=item["id"],
                metadata={"actor": item["actor"]["login"]},
                timestamp=item["created_at"]
            ))
        
        return events
```

**Similar adapters for HackerNews, Reddit**

### Phase 3: Background Polling (2 hours)

**Polling Loop (main.py):**
```python
from contextlib import asynccontextmanager
import asyncio

active_sources = [GitHubEventSource(), HackerNewsEventSource()]

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_all_sources())
    yield
    task.cancel()

async def poll_all_sources():
    while True:
        for source in active_sources:
            try:
                events = await source.fetch_events()
                for event in events:
                    if not await event_exists(event.external_id):
                        await save_event(event)
                        await broadcast_to_sse(event)
            except Exception as e:
                logger.error(f"Poll failed for {source.source_name}: {e}")
        
        await asyncio.sleep(30)

app = FastAPI(lifespan=lifespan)
```

### Phase 4: SSE Streaming (2 hours)

**SSE Connection Manager (app/streaming/sse.py):**
```python
from sse_starlette.sse import EventSourceResponse
from collections import defaultdict
import asyncio

active_connections: dict[str, asyncio.Queue] = {}

@app.get("/stream")
async def stream_events(sources: str = Query(None)):
    connection_id = str(uuid.uuid4())
    queue = asyncio.Queue()
    active_connections[connection_id] = queue
    
    async def event_generator():
        try:
            while True:
                event = await queue.get()
                if sources is None or event.source in sources.split(','):
                    yield {
                        "event": "new_event",
                        "data": event.json()
                    }
        finally:
            del active_connections[connection_id]
    
    return EventSourceResponse(event_generator())

async def broadcast_to_sse(event):
    for queue in active_connections.values():
        await queue.put(event)
```

**API Endpoints (app/api/events.py):**
```python
@app.get("/events")
async def get_events(
    source: str = None,
    limit: int = Query(100, le=1000),
    db: AsyncSession = Depends(get_db)
):
    query = select(Event).order_by(Event.timestamp.desc()).limit(limit)
    if source:
        query = query.where(Event.source == source)
    
    result = await db.execute(query)
    return result.scalars().all()

@app.get("/sources")
async def get_sources():
    return [{
        "name": s.source_name,
        "interval": s.poll_interval,
        "enabled": True
    } for s in active_sources]
```

### Phase 5: Docker & Testing (2 hours)

**Dockerfile:**
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**docker-compose.yml:**
```yaml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    env_file: .env
    depends_on:
      - db

  db:
    image: postgres:16
    environment:
      POSTGRES_DB: eventstream
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
```

**Testing:**
```python
# tests/test_sources.py
@pytest.mark.asyncio
@respx.mock
async def test_github_source():
    respx.get("https://api.github.com/events/public").mock(
        return_value=httpx.Response(200, json=[...])
    )
    
    source = GitHubEventSource()
    events = await source.fetch_events()
    assert len(events) > 0
    assert events[0].source == "github"
```

## Environment Variables

```
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db/eventstream
GITHUB_POLL_INTERVAL=60
HACKERNEWS_POLL_INTERVAL=120
LOG_LEVEL=INFO
```

## Testing Locally

```bash
# Start services
docker-compose up

# SSE client (curl)
curl -N http://localhost:8000/stream

# Filter by source
curl -N "http://localhost:8000/stream?sources=github,reddit"

# Get event history
curl "http://localhost:8000/events?source=github&limit=50"

# Check sources status
curl http://localhost:8000/sources
```

## Success Criteria

- ✅ Polls 3+ external APIs every 30-120s
- ✅ Stores events in PostgreSQL with deduplication
- ✅ SSE endpoint streams new events real-time
- ✅ REST API returns event history
- ✅ Docker Compose works
- ✅ Tests >70% coverage

## Reference

See CLAUDE.md for FastAPI async patterns, testing strategies, and best practices.
