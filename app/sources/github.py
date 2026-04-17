"""GitHub event source adapter."""

import logging
from datetime import datetime

from app.config import settings
from app.schemas import EventCreate
from app.sources.base import BaseEventSource

logger = logging.getLogger(__name__)


class GitHubEventSource(BaseEventSource):
    """Fetches public events from GitHub API.

    GitHub API endpoint: https://api.github.com/events
    Returns up to 300 public events from the last hour.
    """

    source_name = "github"
    base_url = "https://api.github.com"
    poll_interval = settings.github_poll_interval

    async def fetch_events(self, limit: int = 100) -> list[EventCreate]:
        """Fetch public events from GitHub.

        Args:
            limit: Maximum number of events to fetch (max 100 per API page)

        Returns:
            List of normalized EventCreate instances
        """
        try:
            # GitHub events endpoint returns up to 100 events per page
            response = await self._make_request(
                endpoint="/events",
                params={"per_page": min(limit, 100)},
            )

            if not isinstance(response, list):
                logger.warning(f"Unexpected response format from {self.source_name}")
                return []

            events = []
            for item in response:
                try:
                    event = self._normalize_event(item)
                    if event:
                        events.append(event)
                except Exception as e:
                    logger.warning(
                        f"Failed to normalize GitHub event: {e}",
                        extra={
                            "source": self.source_name,
                            "event_id": item.get("id"),
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
        """Normalize GitHub event to EventCreate schema.

        GitHub event types: PushEvent, IssuesEvent, PullRequestEvent, etc.

        Args:
            item: Raw GitHub event data

        Returns:
            Normalized EventCreate instance or None if event should be skipped
        """
        event_type = item.get("type", "UnknownEvent")
        event_id = item.get("id")
        repo_name = item.get("repo", {}).get("name", "unknown")
        actor = item.get("actor", {}).get("login", "unknown")
        created_at = item.get("created_at")

        if not event_id or not created_at:
            return None

        # Parse timestamp
        try:
            timestamp = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except Exception:
            logger.warning(f"Invalid timestamp in GitHub event: {created_at}")
            return None

        # Build title based on event type
        title = self._build_title(event_type, actor, repo_name, item.get("payload", {}))

        # Build URL - point to repo or specific resource if available
        url = f"https://github.com/{repo_name}"
        if event_type == "IssuesEvent":
            issue_number = item.get("payload", {}).get("issue", {}).get("number")
            if issue_number:
                url = f"{url}/issues/{issue_number}"
        elif event_type == "PullRequestEvent":
            pr_number = item.get("payload", {}).get("pull_request", {}).get("number")
            if pr_number:
                url = f"{url}/pull/{pr_number}"

        # Store full payload in metadata for debugging/analytics
        metadata = {
            "actor": actor,
            "repo": repo_name,
            "payload": item.get("payload", {}),
            "org": item.get("org", {}).get("login") if item.get("org") else None,
        }

        return EventCreate(
            source=self.source_name,
            event_type=event_type,
            title=title,
            url=url,
            external_id=f"github_{event_id}",
            metadata=metadata,
            timestamp=timestamp,
        )

    def _build_title(self, event_type: str, actor: str, repo: str, payload: dict) -> str:
        """Build human-readable title for GitHub event.

        Args:
            event_type: GitHub event type (PushEvent, IssuesEvent, etc.)
            actor: GitHub username who triggered the event
            repo: Repository name
            payload: Event payload with type-specific data

        Returns:
            Human-readable event title
        """
        if event_type == "PushEvent":
            ref = payload.get("ref", "").replace("refs/heads/", "")
            commit_count = len(payload.get("commits", []))
            return f"{actor} pushed {commit_count} commit(s) to {repo}:{ref}"

        elif event_type == "IssuesEvent":
            action = payload.get("action", "updated")
            issue_title = payload.get("issue", {}).get("title", "")
            return f"{actor} {action} issue in {repo}: {issue_title}"

        elif event_type == "PullRequestEvent":
            action = payload.get("action", "updated")
            pr_title = payload.get("pull_request", {}).get("title", "")
            return f"{actor} {action} PR in {repo}: {pr_title}"

        elif event_type == "CreateEvent":
            ref_type = payload.get("ref_type", "repository")
            ref = payload.get("ref", "")
            if ref:
                return f"{actor} created {ref_type} '{ref}' in {repo}"
            return f"{actor} created {ref_type} in {repo}"

        elif event_type == "DeleteEvent":
            ref_type = payload.get("ref_type", "branch")
            ref = payload.get("ref", "")
            return f"{actor} deleted {ref_type} '{ref}' in {repo}"

        elif event_type == "ForkEvent":
            return f"{actor} forked {repo}"

        elif event_type == "WatchEvent":
            return f"{actor} starred {repo}"

        elif event_type == "IssueCommentEvent":
            action = payload.get("action", "created")
            return f"{actor} {action} comment on issue in {repo}"

        else:
            # Generic fallback
            return f"{actor} triggered {event_type} in {repo}"
