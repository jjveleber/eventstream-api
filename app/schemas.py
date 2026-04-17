"""Pydantic schemas for request/response validation."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EventBase(BaseModel):
    """Base event schema."""

    source: str = Field(..., max_length=50)
    event_type: str = Field(..., max_length=50)
    title: str = Field(..., max_length=500)
    url: str = Field(..., max_length=1000)
    external_id: str = Field(..., max_length=255)
    metadata: dict = Field(default_factory=dict)
    timestamp: datetime


class EventCreate(EventBase):
    """Schema for creating events."""

    pass


class EventRead(EventBase):
    """Schema for reading events."""

    id: UUID
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

    @classmethod
    def model_validate(cls, obj, **kwargs):
        """Custom validation to handle event_metadata -> metadata mapping from DB."""
        if hasattr(obj, "event_metadata"):
            # Create dict with renamed field
            obj_dict = {
                "id": obj.id,
                "source": obj.source,
                "event_type": obj.event_type,
                "title": obj.title,
                "url": obj.url,
                "external_id": obj.external_id,
                "metadata": obj.event_metadata,  # Map event_metadata to metadata
                "timestamp": obj.timestamp,
                "created_at": obj.created_at,
            }
            return super().model_validate(obj_dict, **kwargs)
        return super().model_validate(obj, **kwargs)


class SourceStatusRead(BaseModel):
    """Schema for source health status."""

    source_name: str
    last_poll: datetime | None
    last_success: datetime | None
    last_error: str | None
    error_count: int
    is_healthy: bool

    model_config = ConfigDict(from_attributes=True)


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    database: str
    sources: list[dict[str, str | bool | None]]
