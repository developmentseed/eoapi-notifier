"""Tests for OGC PubSub CloudEvents builder and event filters."""

import json

import pytest
from cloudevents.conversion import to_structured

from eoapi_notifier.core.event import NotificationEvent
from eoapi_notifier.core.filter import FilterConfig, matches, parse_filters
from eoapi_notifier.core.ogc import build_cloudevent


class TestBuildCloudevent:
    """Test OGC CloudEvent construction."""

    @pytest.fixture
    def item_event(self) -> NotificationEvent:
        return NotificationEvent(
            source="/eoapi/stac/pgstac",
            type="org.eoapi.stac.item",
            operation="item_created",
            collection="sentinel-2-l2a",
            item_id="item-1",
            geometry={
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
            },
            bbox=[0, 0, 1, 1],
            data={"datetime": "2024-01-01T00:00:00Z"},
        )

    def test_create_event_type_and_body(self, item_event: NotificationEvent) -> None:
        cloud_event = build_cloudevent(item_event)
        assert cloud_event["type"] == "org.ogc.api.collection.item.create"
        assert cloud_event["datacontenttype"] == "application/json"
        assert "dataschema" not in cloud_event
        assert cloud_event.data["geometry"] == item_event.geometry
        assert cloud_event.data["bbox"] == item_event.bbox

    def test_delete_event_is_text_plain(self) -> None:
        event = NotificationEvent(
            source="/test",
            type="test",
            operation="item_deleted",
            collection="c",
            item_id="item-99",
        )
        cloud_event = build_cloudevent(event)
        assert cloud_event["type"] == "org.ogc.api.collection.item.delete"
        assert cloud_event["datacontenttype"] == "text/plain"
        assert "dataschema" not in cloud_event
        assert cloud_event.data == "item-99"

    def test_structured_json_roundtrip(self, item_event: NotificationEvent) -> None:
        _, body = to_structured(build_cloudevent(item_event))
        payload = json.loads(body)
        assert payload["type"] == "org.ogc.api.collection.item.create"
        assert payload["data"]["geometry"]["type"] == "Polygon"


class TestEventFilter:
    """Test event filtering."""

    def test_empty_filters_pass_all(self) -> None:
        event = NotificationEvent(
            source="/test", type="t", operation="INSERT", collection="c"
        )
        assert matches(event, []) is True

    def test_collection_filter(self) -> None:
        event = NotificationEvent(
            source="/test", type="t", operation="INSERT", collection="a"
        )
        filters = [FilterConfig(collections=["a"])]
        assert matches(event, filters) is True
        assert matches(event, [FilterConfig(collections=["b"])]) is False

    def test_operation_filter(self) -> None:
        event = NotificationEvent(
            source="/test", type="t", operation="item_updated", collection="c"
        )
        assert matches(event, [FilterConfig(operations=["replace"])]) is True
        assert matches(event, [FilterConfig(operations=["delete"])]) is False

    def test_bbox_filter(self) -> None:
        event = NotificationEvent(
            source="/test",
            type="t",
            operation="INSERT",
            collection="c",
            bbox=[0, 0, 1, 1],
        )
        assert matches(event, [FilterConfig(bbox=[0, 0, 2, 2])]) is True
        assert matches(event, [FilterConfig(bbox=[2, 2, 3, 3])]) is False

    def test_bbox_filter_from_geometry(self) -> None:
        event = NotificationEvent(
            source="/test",
            type="t",
            operation="INSERT",
            collection="c",
            geometry={"type": "Point", "coordinates": [1, 1]},
        )
        assert matches(event, [FilterConfig(bbox=[0, 0, 2, 2])]) is True
        assert matches(event, [FilterConfig(bbox=[5, 5, 6, 6])]) is False

    def test_parse_filters_from_config(self) -> None:
        filters = parse_filters([{"collections": ["c1"], "operations": ["create"]}])
        assert len(filters) == 1
        assert filters[0].collections == ["c1"]
