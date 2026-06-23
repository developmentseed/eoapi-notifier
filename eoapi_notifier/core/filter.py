"""Event filtering by collection, operation, and bbox."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .event import NotificationEvent
from .ogc import canonical_operation


class FilterConfig(BaseModel):
    """Single filter block; unset fields are ignored."""

    model_config = ConfigDict(extra="forbid")

    collections: list[str] | None = None
    operations: list[str] | None = None
    bbox: list[float] | None = Field(default=None, min_length=4, max_length=4)


def parse_filters(raw: list[dict[str, Any]]) -> list[FilterConfig]:
    """Parse filter blocks from config."""
    return [FilterConfig.model_validate(block) for block in raw]


def _event_bbox(event: NotificationEvent) -> list[float] | None:
    if event.bbox and len(event.bbox) >= 4:
        return event.bbox
    if not event.geometry:
        return None
    coords = event.geometry.get("coordinates")
    if not coords:
        return None
    # Flatten coordinate pairs for a simple bbox estimate
    flat: list[float] = []

    def walk(node: Any) -> None:
        if isinstance(node, int | float):
            flat.append(float(node))
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(coords)
    if len(flat) < 2:
        return None
    xs = flat[0::2]
    ys = flat[1::2]
    return [min(xs), min(ys), max(xs), max(ys)]


def _bbox_overlaps(a: list[float], b: list[float]) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def _matches_block(event: NotificationEvent, block: FilterConfig) -> bool:
    if block.collections is not None and event.collection not in block.collections:
        return False
    if block.operations is not None:
        event_op = canonical_operation(event.operation) or event.operation.lower()
        allowed = {canonical_operation(op) or op.lower() for op in block.operations}
        if event_op not in allowed:
            return False
    if block.bbox is not None:
        event_bbox = _event_bbox(event)
        if event_bbox is None or not _bbox_overlaps(event_bbox, block.bbox):
            return False
    return True


def matches(event: NotificationEvent, filters: list[FilterConfig]) -> bool:
    """Return True if event passes filters (OR across blocks, AND within block)."""
    if not filters:
        return True
    return any(_matches_block(event, block) for block in filters)
