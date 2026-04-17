"""Unit tests for event source adapters."""

import json
from pathlib import Path

import pytest
import respx
from httpx import Response

from app.sources.github import GitHubEventSource
from app.sources.hackernews import HackerNewsEventSource
from app.sources.reddit import RedditEventSource

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def github_mock_data():
    """Load GitHub test fixture."""
    with open(FIXTURES_DIR / "github_events.json") as f:
        return json.load(f)


@pytest.fixture
def hackernews_mock_data():
    """Load HackerNews test fixture."""
    with open(FIXTURES_DIR / "hackernews_story.json") as f:
        return json.load(f)


@pytest.fixture
def reddit_mock_data():
    """Load Reddit test fixture."""
    with open(FIXTURES_DIR / "reddit_posts.json") as f:
        return json.load(f)


@pytest.mark.asyncio
@respx.mock
async def test_github_fetch_events(github_mock_data):
    """Test GitHub event fetching and normalization."""
    # Mock GitHub API
    respx.get("https://api.github.com/events").mock(
        return_value=Response(200, json=github_mock_data)
    )

    # Create source and fetch events
    source = GitHubEventSource()
    events = await source.fetch_events(limit=10)

    # Verify events were fetched and normalized
    assert len(events) == 2

    # Check first event (DeleteEvent)
    event1 = events[0]
    assert event1.source == "github"
    assert event1.event_type == "DeleteEvent"
    assert "github-actions[bot]" in event1.title
    assert "deleted branch" in event1.title
    assert event1.url == "https://github.com/eso/homebrew-pipelines"
    assert event1.external_id == "github_10716336707"
    assert event1.metadata["actor"] == "github-actions[bot]"

    # Check second event (PushEvent)
    event2 = events[1]
    assert event2.source == "github"
    assert event2.event_type == "PushEvent"
    assert "pushed 2 commit(s)" in event2.title
    assert event2.external_id == "github_10716336461"

    await source.close()


@pytest.mark.asyncio
@respx.mock
async def test_hackernews_fetch_events(hackernews_mock_data):
    """Test HackerNews event fetching and normalization."""
    # Mock HackerNews API
    respx.get("https://hacker-news.firebaseio.com/v0/topstories.json").mock(
        return_value=Response(200, json=[47806725])
    )
    respx.get("https://hacker-news.firebaseio.com/v0/item/47806725.json").mock(
        return_value=Response(200, json=hackernews_mock_data)
    )

    # Create source and fetch events
    source = HackerNewsEventSource()
    events = await source.fetch_events(limit=10)

    # Verify events were fetched and normalized
    assert len(events) == 1

    event = events[0]
    assert event.source == "hackernews"
    assert event.event_type == "story"
    assert "Test Story Title" in event.title
    assert "by testuser" in event.title
    assert "311 points" in event.title
    assert event.url == "https://example.com/test-article"
    assert event.external_id == "hackernews_47806725"
    assert event.metadata["author"] == "testuser"
    assert event.metadata["score"] == 311

    await source.close()


@pytest.mark.asyncio
@respx.mock
async def test_reddit_fetch_events(reddit_mock_data):
    """Test Reddit event fetching and normalization."""
    # Mock Reddit API
    respx.get("https://www.reddit.com/r/all/hot.json").mock(
        return_value=Response(200, json=reddit_mock_data)
    )

    # Create source and fetch events
    source = RedditEventSource()
    events = await source.fetch_events(limit=10)

    # Verify events were fetched and normalized
    assert len(events) == 1

    event = events[0]
    assert event.source == "reddit"
    assert event.event_type == "link"  # External URL, not self post
    assert "r/test:" in event.title
    assert "Test Reddit Post" in event.title
    assert "by u/testuser" in event.title
    assert "24106 upvotes" in event.title
    assert event.url == "https://example.com/test"
    assert event.external_id == "reddit_1so1b77"
    assert event.metadata["subreddit"] == "test"
    assert event.metadata["author"] == "testuser"

    await source.close()


@pytest.mark.asyncio
@respx.mock
async def test_github_api_error():
    """Test GitHub adapter handles API errors gracefully."""
    # Mock API error
    respx.get("https://api.github.com/events").mock(
        return_value=Response(500, json={"message": "Internal Server Error"})
    )

    source = GitHubEventSource()

    with pytest.raises(Exception):
        await source.fetch_events()

    await source.close()


@pytest.mark.asyncio
@respx.mock
async def test_hackernews_invalid_story_type(hackernews_mock_data):
    """Test HackerNews adapter skips non-story items."""
    # Modify fixture to be a comment instead of story
    hackernews_mock_data["type"] = "comment"

    respx.get("https://hacker-news.firebaseio.com/v0/topstories.json").mock(
        return_value=Response(200, json=[47806725])
    )
    respx.get("https://hacker-news.firebaseio.com/v0/item/47806725.json").mock(
        return_value=Response(200, json=hackernews_mock_data)
    )

    source = HackerNewsEventSource()
    events = await source.fetch_events(limit=10)

    # Should skip comment and return empty list
    assert len(events) == 0

    await source.close()


@pytest.mark.asyncio
@respx.mock
async def test_reddit_empty_response():
    """Test Reddit adapter handles empty response."""
    respx.get("https://www.reddit.com/r/all/hot.json").mock(
        return_value=Response(200, json={"kind": "Listing", "data": {"children": []}})
    )

    source = RedditEventSource()
    events = await source.fetch_events(limit=10)

    assert len(events) == 0

    await source.close()
