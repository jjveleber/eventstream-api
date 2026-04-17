"""Registry of all active event sources."""

from app.sources.github import GitHubEventSource
from app.sources.hackernews import HackerNewsEventSource
from app.sources.reddit import RedditEventSource

# List of all active event sources
# Add new sources here to enable polling
ACTIVE_SOURCES = [
    GitHubEventSource(),
    HackerNewsEventSource(),
    RedditEventSource(),
]


def get_source_by_name(source_name: str):
    """Get event source by name.

    Args:
        source_name: Name of the source (e.g., 'github', 'hackernews', 'reddit')

    Returns:
        Event source instance or None if not found
    """
    for source in ACTIVE_SOURCES:
        if source.source_name == source_name:
            return source
    return None


def get_all_source_names() -> list[str]:
    """Get list of all active source names.

    Returns:
        List of source names
    """
    return [source.source_name for source in ACTIVE_SOURCES]
