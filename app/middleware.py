"""Middleware for rate limiting and authentication."""

import logging
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.config import settings

logger = logging.getLogger(__name__)

# Initialize rate limiter
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[f"{settings.rate_limit_per_minute}/minute"] if settings.rate_limit_enabled else [],
)


async def verify_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    """Verify API key if authentication is enabled.

    Args:
        x_api_key: API key from X-API-Key header

    Raises:
        HTTPException: If API key is invalid or missing
    """
    if not settings.api_key_enabled:
        return

    if not settings.api_key:
        logger.warning("API key authentication enabled but no key configured")
        return

    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    if x_api_key != settings.api_key:
        logger.warning(
            "Invalid API key attempt",
            extra={"provided_key": x_api_key[:8] + "..." if x_api_key else None},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
