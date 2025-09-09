"""
Event.

Data object to hold information about notification events.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class NotificationEvent(BaseModel):
    """
    Definition of a notification event and its content.

    Attributes:
        id: Unique event identifier
        source: Event source (e.g., "/eoapi/stac/pgstac")
        type: Event type (e.g., "org.eoapi.stac.item")
        operation: Operation performed (INSERT, UPDATE, DELETE)
        collection: STAC collection ID
        item_id: STAC item ID
        timestamp: Event timestamp
        data: Additional event data
    """

    id: str = Field(default_factory=lambda: str(uuid4()))
    source: str
    type: str
    operation: str
    collection: str
    item_id: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    data: dict[str, Any] = Field(default_factory=dict)
