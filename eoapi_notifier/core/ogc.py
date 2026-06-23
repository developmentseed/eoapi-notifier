"""OGC PubSub CloudEvents-JSON message builder."""

from typing import Any

from cloudevents.http import CloudEvent

from .event import NotificationEvent

OGC_ITEM_TYPE_PREFIX = "org.ogc.api.collection.item"

# Raw pgSTAC and correlated semantic operations mapped to OGC verbs.
_CANONICAL_OPERATIONS = {
    "insert": "create",
    "item_created": "create",
    "create": "create",
    "update": "replace",
    "item_updated": "replace",
    "replace": "replace",
    "delete": "delete",
    "item_deleted": "delete",
}


def canonical_operation(operation: str) -> str | None:
    """Map a raw or semantic operation to an OGC verb (create/replace/delete)."""
    return _CANONICAL_OPERATIONS.get(operation.lower())


def build_cloudevent(
    event: NotificationEvent, source: str = "/eoapi/stac"
) -> CloudEvent:
    """Build an OGC PubSub CloudEvent from a notification event."""
    verb = canonical_operation(event.operation)

    attributes: dict[str, Any] = {
        "specversion": "1.0",
        "id": event.id,
        "source": source,
        "type": f"{OGC_ITEM_TYPE_PREFIX}.{verb or event.operation.lower()}",
        "time": event.timestamp.isoformat(),
    }

    if event.item_id:
        attributes["subject"] = event.item_id
    if event.collection:
        attributes["collection"] = event.collection

    if verb == "delete":
        attributes["datacontenttype"] = "text/plain"
        data: Any = event.item_id or ""
    else:
        attributes["datacontenttype"] = "application/json"
        data = {
            "type": "Feature",
            "id": event.item_id,
            "collection": event.collection,
            "geometry": event.geometry,
            "bbox": event.bbox,
            "properties": event.data,
        }

    return CloudEvent(attributes, data)
