"""Reddit event source adapter."""

import logging
from datetime import datetime, timezone

from app.config import settings
from app.schemas import EventCreate
from app.sources.base import BaseEventSource

logger = logging.getLogger(__name__)


class RedditEventSource(BaseEventSource):
    """Fetches hot posts from Reddit /r/all.

    Reddit JSON API: https://www.reddit.com/r/all/hot.json
    No authentication required for public endpoints.
    """

    source_name = "reddit"
    base_url = "https://www.reddit.com"
    poll_interval = settings.reddit_poll_interval

    async def fetch_events(self, limit: int = 25) -> list[EventCreate]:
        """Fetch hot posts from /r/all.

        Args:
            limit: Maximum number of posts to fetch (default 25, max 100)

        Returns:
            List of normalized EventCreate instances
        """
        try:
            # Fetch hot posts from /r/all
            response = await self._make_request(
                endpoint="/r/all/hot.json",
                params={"limit": min(limit, 100)},
            )

            if not isinstance(response, dict):
                logger.warning(f"Unexpected response format from {self.source_name}")
                return []

            # Reddit response structure: {"kind": "Listing", "data": {"children": [...]}}
            children = response.get("data", {}).get("children", [])
            if not children:
                logger.warning(f"No posts returned from {self.source_name}")
                return []

            events = []
            for item in children:
                try:
                    # Each child is {"kind": "t3", "data": {...}}
                    post_data = item.get("data", {})
                    if post_data:
                        event = self._normalize_event(post_data)
                        if event:
                            events.append(event)
                except Exception as e:
                    logger.warning(
                        f"Failed to normalize Reddit post: {e}",
                        extra={
                            "source": self.source_name,
                            "post_id": item.get("data", {}).get("id"),
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
        """Normalize Reddit post to EventCreate schema.

        Args:
            item: Raw Reddit post data (from "data" field)

        Returns:
            Normalized EventCreate instance or None if post should be skipped
        """
        post_id = item.get("id")
        title = item.get("title")
        subreddit = item.get("subreddit")
        author = item.get("author", "[deleted]")
        created_utc = item.get("created_utc")
        ups = item.get("ups", 0)
        num_comments = item.get("num_comments", 0)
        permalink = item.get("permalink")
        url = item.get("url")  # May be external link or reddit post itself

        if not post_id or not title or not created_utc or not permalink:
            return None

        # Parse Unix timestamp
        try:
            timestamp = datetime.fromtimestamp(created_utc, tz=timezone.utc)
        except Exception:
            logger.warning(f"Invalid timestamp in Reddit post: {created_utc}")
            return None

        # Build Reddit URL
        reddit_url = f"https://www.reddit.com{permalink}"

        # Determine event type based on post content
        event_type = self._determine_event_type(item)

        # Build event title with subreddit context
        event_title = f"r/{subreddit}: {title} (by u/{author}, {ups} upvotes, {num_comments} comments)"

        # Store metadata
        metadata = {
            "subreddit": subreddit,
            "author": author,
            "ups": ups,
            "upvote_ratio": item.get("upvote_ratio"),
            "num_comments": num_comments,
            "permalink": reddit_url,
            "external_url": url if url != reddit_url else None,
            "is_self": item.get("is_self", False),
            "is_video": item.get("is_video", False),
            "over_18": item.get("over_18", False),
            "domain": item.get("domain"),
        }

        # Use external URL if it's a link post, otherwise use Reddit URL
        final_url = url if url and not item.get("is_self") else reddit_url

        return EventCreate(
            source=self.source_name,
            event_type=event_type,
            title=event_title,
            url=final_url,
            external_id=f"reddit_{post_id}",
            metadata=metadata,
            timestamp=timestamp,
        )

    def _determine_event_type(self, item: dict) -> str:
        """Determine event type based on post characteristics.

        Args:
            item: Reddit post data

        Returns:
            Event type string
        """
        if item.get("is_self"):
            return "self_post"
        elif item.get("is_video"):
            return "video"
        elif item.get("post_hint") == "image":
            return "image"
        elif item.get("post_hint") == "link":
            return "link"
        else:
            return "post"
