"""SQLAlchemy models for events."""

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Index, JSON, String, func
from sqlalchemy.dialects.postgresql import UUID

from app.database import Base


class Event(Base):
    """Event model for storing normalized events from external sources."""

    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String(50), nullable=False, index=True)
    event_type = Column(String(50), nullable=False)
    title = Column(String(500), nullable=False)
    url = Column(String(1000), nullable=False)
    external_id = Column(String(255), unique=True, nullable=False)
    event_metadata = Column("metadata", JSON, default=dict)  # Renamed to avoid reserved name
    timestamp = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (Index("idx_source_timestamp", "source", "timestamp"),)

    def __repr__(self) -> str:
        return f"<Event {self.source}:{self.event_type} - {self.title[:50]}>"


class SourceStatus(Base):
    """Track health status of each event source."""

    __tablename__ = "source_status"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_name = Column(String(50), unique=True, nullable=False)
    last_poll = Column(DateTime(timezone=True), nullable=True)
    last_success = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(String(1000), nullable=True)
    error_count = Column(String(10), default="0")
    is_healthy = Column(String(10), default="true")

    def __repr__(self) -> str:
        return f"<SourceStatus {self.source_name} - healthy={self.is_healthy}>"
