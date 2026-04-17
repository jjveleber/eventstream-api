"""HackerNews event source adapter."""

import logging
from datetime import datetime, timezone

from app.config import settings
from app.schemas import EventCreate
from app.sources.base import BaseEventSource

logger = logging.getLogger(__name__)


class HackerNewsEventSource(BaseEventSource):
    """Fetches top stories from HackerNews API.

    HackerNews API: https://github.com/HackerNews/API
    Fetches top story IDs, then fetches details for each story.
    """

    source_name = "hackernews"
    base_url = "https://hacker-news.firebaseio.com/v0"
    poll_interval = settings.hackernews_poll_interval

    async def fetch_events(self, limit: int = 30) -> list[EventCreate]:
        """Fetch top stories from HackerNews.

        Args:
            limit: Maximum number of stories to fetch (default 30)

        Returns:
            List of normalized EventCreate instances
        """
        try:
            # Step 1: Get top story IDs
            story_ids = await self._make_request(endpoint="/topstories.json")

            if not isinstance(story_ids, list) or not story_ids:
                logger.warning(f"No story IDs returned from {self.source_name}")
                return []

            # Step 2: Fetch details for top N stories
            events = []
            for story_id in story_ids[:limit]:
                try:
                    story_data = await self._make_request(endpoint=f"/item/{story_id}.json")

                    # Only process stories (not comments, jobs, etc.)
                    if isinstance(story_data, dict) and story_data.get("type") == "story":
                        event = self._normalize_event(story_data)
                        if event:
                            events.append(event)
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch/normalize HN story {story_id}: {e}",
                        extra={
                            "source": self.source_name,
                            "story_id": story_id,
                            "error": str(e),
                        },
                    )
                    continue

            logger.info(
                f"Fetched {len(events)} events from {self.source_name}",
                extra={
                    "source": self.source_name,
                    "event_count": len(events),
                },
            )

            return events

        except Exception as e:
            logger.error(
                f"Failed to fetch events from {self.source_name}: {e}",
                extra={
                    "source": self.source_name,
                    "error": str(e),
                },
            )
            raise

    def _normalize_event(self, item: dict) -> EventCreate | None:
        """Normalize HackerNews story to EventCreate schema.

        Args:
            item: Raw HackerNews story data

        Returns:
            Normalized EventCreate instance or None if story should be skipped
        """
        story_id = item.get("id")
        title = item.get("title")
        url = item.get("url")  # External URL if available
        by = item.get("by", "unknown")
        time_unix = item.get("time")
        score = item.get("score", 0)
        descendants = item.get("descendants", 0)  # Comment count

        if not story_id or not title or not time_unix:
            return None

        # Parse Unix timestamp
        try:
            timestamp = datetime.fromtimestamp(time_unix, tz=timezone.utc)
        except Exception:
            logger.warning(f"Invalid timestamp in HN story: {time_unix}")
            return None

        # Use HN discussion URL if no external URL
        hn_url = f"https://news.ycombinator.com/item?id={story_id}"
        if not url:
            url = hn_url

        # Build event title
        event_title = f"{title} (by {by}, {score} points, {descendants} comments)"

        # Store metadata
        metadata = {
            "author": by,
            "score": score,
            "descendants": descendants,
            "hn_url": hn_url,
            "external_url": item.get("url"),
            "type": item.get("type"),
        }

        return EventCreate(
            source=self.source_name,
            event_type="story",
            title=event_title,
            url=url,
            external_id=f"hackernews_{story_id}",
            metadata=metadata,
            timestamp=timestamp,
        )
