"""Base class for external API event sources."""

import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx

from app.schemas import EventCreate

logger = logging.getLogger(__name__)


class BaseEventSource(ABC):
    """Abstract base class for all external API sources.

    Each source adapter must implement:
    - source_name: Unique identifier for this source
    - base_url: Base URL for the external API
    - poll_interval: Seconds between polls
    - fetch_events(): Fetch and normalize events from API
    """

    source_name: str
    base_url: str
    poll_interval: int

    def __init__(self) -> None:
        """Initialize the event source."""
        self.client: httpx.AsyncClient | None = None

    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10.0,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Make async HTTP request with error handling.

        Args:
            endpoint: API endpoint path (appended to base_url)
            method: HTTP method (GET, POST, etc.)
            params: Query parameters
            headers: HTTP headers
            timeout: Request timeout in seconds

        Returns:
            JSON response data

        Raises:
            httpx.HTTPError: On request failure
        """
        if self.client is None:
            self.client = httpx.AsyncClient()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Default headers
        default_headers = {
            "User-Agent": f"EventStreamAPI/1.0 ({self.source_name})",
            "Accept": "application/json",
        }
        if headers:
            default_headers.update(headers)

        logger.debug(
            f"Making {method} request to {url}",
            extra={
                "source": self.source_name,
                "url": url,
                "params": params,
            },
        )

        try:
            response = await self.client.request(
                method=method,
                url=url,
                params=params,
                headers=default_headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(
                f"HTTP error fetching from {self.source_name}: {e}",
                extra={
                    "source": self.source_name,
                    "url": url,
                    "error": str(e),
                },
            )
            raise

    @abstractmethod
    async def fetch_events(self, limit: int = 100) -> list[EventCreate]:
        """Fetch and normalize events from external API.

        Args:
            limit: Maximum number of events to fetch

        Returns:
            List of normalized EventCreate instances
        """
        pass

    async def close(self) -> None:
        """Close HTTP client connection."""
        if self.client:
            await self.client.aclose()
            self.client = None

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} source={self.source_name} poll_interval={self.poll_interval}>"
