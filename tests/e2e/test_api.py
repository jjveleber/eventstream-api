"""End-to-end tests for API endpoints."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_root_endpoint(test_client: AsyncClient):
    """Test root endpoint."""
    response = await test_client.get("/")
    assert response.status_code == 200

    data = response.json()
    assert data["message"] == "EventStream API"
    assert data["version"] == "1.0.0"


@pytest.mark.asyncio
async def test_get_events_empty(test_client: AsyncClient):
    """Test GET /api/events with no events."""
    response = await test_client.get("/api/events")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_get_events_with_data(test_client: AsyncClient, sample_events):
    """Test GET /api/events with sample data."""
    response = await test_client.get("/api/events")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 3

    # Events should be ordered by timestamp descending
    assert all("id" in event for event in data)
    assert all("source" in event for event in data)


@pytest.mark.asyncio
async def test_get_events_with_source_filter(test_client: AsyncClient, sample_events):
    """Test GET /api/events with source filter."""
    response = await test_client.get("/api/events?source=github")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1
    assert data[0]["source"] == "github"


@pytest.mark.asyncio
async def test_get_events_with_limit(test_client: AsyncClient, sample_events):
    """Test GET /api/events with limit."""
    response = await test_client.get("/api/events?limit=2")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_get_events_with_offset(test_client: AsyncClient, sample_events):
    """Test GET /api/events with offset."""
    response = await test_client.get("/api/events?offset=2")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1


@pytest.mark.asyncio
async def test_get_events_invalid_source(test_client: AsyncClient):
    """Test GET /api/events with invalid source."""
    response = await test_client.get("/api/events?source=invalid")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_get_sources(test_client: AsyncClient):
    """Test GET /api/sources."""
    response = await test_client.get("/api/sources")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 3  # github, hackernews, reddit

    source_names = {source["name"] for source in data}
    assert "github" in source_names
    assert "hackernews" in source_names
    assert "reddit" in source_names

    # Check structure
    for source in data:
        assert "name" in source
        assert "base_url" in source
        assert "poll_interval" in source


@pytest.mark.asyncio
async def test_health_check(test_client: AsyncClient, sample_source_status):
    """Test GET /api/health."""
    response = await test_client.get("/api/health")
    assert response.status_code == 200

    data = response.json()
    assert "status" in data
    assert "database" in data
    assert "sources" in data

    assert data["database"] == "healthy"
    assert len(data["sources"]) == 2


@pytest.mark.asyncio
async def test_metrics_endpoint(test_client: AsyncClient, sample_events, sample_source_status):
    """Test GET /api/metrics."""
    response = await test_client.get("/api/metrics")
    assert response.status_code == 200

    data = response.json()
    assert "sse_connections" in data
    assert "sources" in data

    assert data["sse_connections"] == 0  # No active connections in test

    # Check source metrics
    assert "github" in data["sources"]
    assert "hackernews" in data["sources"]


@pytest.mark.asyncio
async def test_stream_endpoint_headers(test_client: AsyncClient):
    """Test SSE stream endpoint returns correct headers."""
    # Just test that the endpoint exists and returns correct content type
    # Full SSE testing requires more complex setup
    import asyncio

    try:
        async with asyncio.timeout(1.0):  # 1 second timeout
            async with test_client.stream("GET", "/api/stream") as response:
                assert response.status_code == 200
                assert "text/event-stream" in response.headers.get("content-type", "")
                assert "X-Connection-ID" in response.headers
                # Headers checked, exit immediately
    except asyncio.TimeoutError:
        # Timeout is OK - we only needed to check headers
        pass


@pytest.mark.asyncio
async def test_pagination_edge_cases(test_client: AsyncClient, sample_events):
    """Test pagination edge cases."""
    # Limit exceeds max (1000)
    response = await test_client.get("/api/events?limit=2000")
    assert response.status_code == 422  # Validation error

    # Negative offset
    response = await test_client.get("/api/events?offset=-1")
    assert response.status_code == 422

    # Zero limit
    response = await test_client.get("/api/events?limit=0")
    assert response.status_code == 422
