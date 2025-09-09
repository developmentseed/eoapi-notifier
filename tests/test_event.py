"""
Tests for the NotificationEvent model.

This module tests only the core event.py functionality without any plugin dependencies.
All plugins are mocked to maintain separation of concerns.
"""

import json
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from eoapi_notifier.core.event import NotificationEvent


class TestNotificationEventCreation:
    """Test NotificationEvent creation and initialization."""

    def test_create_with_required_fields_only(self) -> None:
        """Test creating event with only required fields."""
        event = NotificationEvent(
            source="/test/source",
            type="test.event",
            operation="INSERT",
            collection="test_collection",
        )

        assert event.source == "/test/source"
        assert event.type == "test.event"
        assert event.operation == "INSERT"
        assert event.collection == "test_collection"
        assert event.item_id is None
        assert event.data == {}

    def test_create_with_all_fields(self) -> None:
        """Test creating event with all fields specified."""
        custom_data = {"key": "value", "number": 42}
        custom_timestamp = datetime(2023, 12, 1, 10, 30, 0, tzinfo=UTC)

        event = NotificationEvent(
            source="/custom/source",
            type="custom.event",
            operation="UPDATE",
            collection="custom_collection",
            item_id="custom_item_123",
            timestamp=custom_timestamp,
            data=custom_data,
        )

        assert event.source == "/custom/source"
        assert event.type == "custom.event"
        assert event.operation == "UPDATE"
        assert event.collection == "custom_collection"
        assert event.item_id == "custom_item_123"
        assert event.timestamp == custom_timestamp
        assert event.data == custom_data

    def test_create_with_empty_strings(self) -> None:
        """Test creating event with empty string values."""
        event = NotificationEvent(
            source="", type="", operation="", collection="", item_id=""
        )

        assert event.source == ""
        assert event.type == ""
        assert event.operation == ""
        assert event.collection == ""
        assert event.item_id == ""


class TestNotificationEventDefaults:
    """Test default value generation for NotificationEvent."""

    def test_id_auto_generation(self) -> None:
        """Test that ID is automatically generated as valid UUID."""
        event = NotificationEvent(
            source="/test", type="test", operation="INSERT", collection="test"
        )

        assert event.id is not None
        assert isinstance(event.id, str)
        # Should be valid UUID
        uuid_obj = UUID(event.id)
        assert str(uuid_obj) == event.id

    def test_id_uniqueness(self) -> None:
        """Test that generated IDs are unique."""
        events = []
        for _ in range(10):
            event = NotificationEvent(
                source="/test", type="test", operation="INSERT", collection="test"
            )
            events.append(event)

        # All IDs should be unique
        ids = [event.id for event in events]
        assert len(set(ids)) == len(ids)

    def test_timestamp_auto_generation(self) -> None:
        """Test that timestamp is automatically generated."""
        before = datetime.now(UTC)

        event = NotificationEvent(
            source="/test", type="test", operation="INSERT", collection="test"
        )

        after = datetime.now(UTC)

        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)
        assert event.timestamp.tzinfo == UTC
        assert before <= event.timestamp <= after

    def test_data_default_empty_dict(self) -> None:
        """Test that data defaults to empty dictionary."""
        event = NotificationEvent(
            source="/test", type="test", operation="INSERT", collection="test"
        )

        assert event.data == {}
        assert isinstance(event.data, dict)

    def test_data_independence(self) -> None:
        """Test that default data dicts are independent instances."""
        event1 = NotificationEvent(
            source="/test", type="test", operation="INSERT", collection="test"
        )
        event2 = NotificationEvent(
            source="/test", type="test", operation="INSERT", collection="test"
        )

        # Modify one event's data
        event1.data["key"] = "value"

        # Other event should be unaffected
        assert event2.data == {}
        assert event1.data == {"key": "value"}


class TestNotificationEventValidation:
    """Test Pydantic validation for NotificationEvent."""

    def test_missing_required_fields(self) -> None:
        """Test validation fails when required fields are missing."""
        with pytest.raises(ValidationError) as exc_info:
            NotificationEvent()  # type: ignore[call-arg]

        errors = exc_info.value.errors()
        required_fields = {"source", "type", "operation", "collection"}
        error_fields = {error["loc"][0] for error in errors}
        assert required_fields.issubset(error_fields)

    def test_partial_required_fields(self) -> None:
        """Test validation fails when only some required fields provided."""
        with pytest.raises(ValidationError):
            NotificationEvent(source="/test")  # type: ignore[call-arg]

        with pytest.raises(ValidationError):
            NotificationEvent(source="/test", type="test")  # type: ignore[call-arg]

        with pytest.raises(ValidationError):
            NotificationEvent(source="/test", type="test", operation="INSERT")  # type: ignore[call-arg]

    def test_explicit_none_for_optional_fields(self) -> None:
        """Test that optional fields can be explicitly set to None."""
        event = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            item_id=None,
        )

        assert event.item_id is None

    def test_field_type_validation(self) -> None:
        """Test that field types are validated."""
        # All string fields should accept strings
        event = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            item_id="item",
        )
        assert isinstance(event.source, str)
        assert isinstance(event.type, str)
        assert isinstance(event.operation, str)
        assert isinstance(event.collection, str)
        assert isinstance(event.item_id, str)


class TestNotificationEventData:
    """Test the data field functionality."""

    def test_data_with_nested_dict(self) -> None:
        """Test data field with nested dictionary."""
        nested_data = {
            "properties": {"datetime": "2023-12-01T10:30:00Z", "instruments": ["MSI"]},
            "geometry": {"coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
        }

        event = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            data=nested_data,
        )

        assert event.data == nested_data
        assert event.data["properties"]["datetime"] == "2023-12-01T10:30:00Z"
        assert event.data["geometry"]["coordinates"][0][0] == [0, 0]

    def test_data_with_various_types(self) -> None:
        """Test data field with various Python types."""
        complex_data = {
            "string": "test",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "list": [1, 2, 3],
            "null": None,
            "nested": {"deep": {"value": "found"}},
        }

        event = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            data=complex_data,
        )

        assert event.data == complex_data
        assert event.data["string"] == "test"
        assert event.data["integer"] == 42
        assert event.data["boolean"] is True
        assert event.data["null"] is None
        assert event.data["nested"]["deep"]["value"] == "found"

    def test_data_modification_after_creation(self) -> None:
        """Test that data can be modified after event creation."""
        event = NotificationEvent(
            source="/test", type="test", operation="INSERT", collection="test"
        )

        # Modify data
        event.data["added_field"] = "new_value"
        event.data["count"] = 100

        assert event.data["added_field"] == "new_value"
        assert event.data["count"] == 100


class TestNotificationEventSerialization:
    """Test serialization and deserialization of NotificationEvent."""

    def test_model_dump(self) -> None:
        """Test converting event to dictionary."""
        custom_timestamp = datetime(2023, 12, 1, 10, 30, 0, tzinfo=UTC)
        event = NotificationEvent(
            source="/test/source",
            type="test.event",
            operation="UPDATE",
            collection="test_collection",
            item_id="test_item",
            timestamp=custom_timestamp,
            data={"key": "value"},
        )

        event_dict = event.model_dump()

        assert isinstance(event_dict, dict)
        assert event_dict["source"] == "/test/source"
        assert event_dict["type"] == "test.event"
        assert event_dict["operation"] == "UPDATE"
        assert event_dict["collection"] == "test_collection"
        assert event_dict["item_id"] == "test_item"
        assert event_dict["data"] == {"key": "value"}
        # ID should be present and valid
        assert "id" in event_dict
        assert UUID(event_dict["id"])

    def test_json_serialization(self) -> None:
        """Test JSON serialization of event."""
        event = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            data={"nested": {"value": 42}},
        )

        json_str = event.model_dump_json()
        assert isinstance(json_str, str)

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed["source"] == "/test"
        assert parsed["data"]["nested"]["value"] == 42

    def test_model_reconstruction(self) -> None:
        """Test reconstructing event from dictionary."""
        original_data = {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "source": "/test/source",
            "type": "test.event",
            "operation": "DELETE",
            "collection": "test_collection",
            "item_id": "test_item",
            "timestamp": "2023-12-01T10:30:00+00:00",
            "data": {"extra": "info"},
        }

        event = NotificationEvent(**original_data)  # type: ignore[arg-type]

        assert event.id == original_data["id"]
        assert event.source == original_data["source"]
        assert event.type == original_data["type"]
        assert event.operation == original_data["operation"]
        assert event.collection == original_data["collection"]
        assert event.item_id == original_data["item_id"]
        assert event.data == original_data["data"]

    def test_json_deserialization(self) -> None:
        """Test reconstructing event from JSON string."""
        json_data = {
            "source": "/test",
            "type": "test",
            "operation": "INSERT",
            "collection": "test",
            "item_id": "item_123",
            "data": {"count": 5},
        }

        json_str = json.dumps(json_data)
        event_dict = json.loads(json_str)
        event = NotificationEvent(**event_dict)

        assert event.source == "/test"
        assert event.operation == "INSERT"
        assert event.item_id == "item_123"
        assert event.data["count"] == 5


class TestNotificationEventEdgeCases:
    """Test edge cases and special scenarios."""

    def test_very_long_strings(self) -> None:
        """Test with very long string values."""
        long_string = "x" * 10000

        event = NotificationEvent(
            source=long_string,
            type=long_string,
            operation=long_string,
            collection=long_string,
            item_id=long_string,
        )

        assert len(event.source) == 10000
        assert event.source == long_string

    def test_unicode_strings(self) -> None:
        """Test with unicode characters."""
        unicode_data = {
            "source": "/测试/источник",
            "type": "тест.événement",
            "operation": "插入",
            "collection": "コレクション",
            "item_id": "元素_123",
            "data": {"名前": "値", "🚀": "rocket"},
        }

        event = NotificationEvent(**unicode_data)  # type: ignore[arg-type]

        assert event.source == "/测试/источник"
        assert event.type == "тест.événement"
        assert event.operation == "插入"
        assert event.data["🚀"] == "rocket"

    def test_timestamp_timezone_handling(self) -> None:
        """Test timestamp with different timezone formats."""
        # UTC timestamp
        utc_time = datetime(2023, 12, 1, 10, 30, 0, tzinfo=UTC)
        event = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            timestamp=utc_time,
        )
        assert event.timestamp.tzinfo == UTC

        # Different timezone
        other_tz = timezone(timedelta(hours=5))
        tz_time = datetime(2023, 12, 1, 10, 30, 0, tzinfo=other_tz)
        event2 = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            timestamp=tz_time,
        )
        assert event2.timestamp.tzinfo == other_tz

    def test_large_data_payload(self) -> None:
        """Test with large data payload."""
        large_data = {}
        for i in range(1000):
            large_data[f"key_{i}"] = f"value_{i}" * 100

        event = NotificationEvent(
            source="/test",
            type="test",
            operation="INSERT",
            collection="test",
            data=large_data,
        )

        assert len(event.data) == 1000
        assert event.data["key_999"].startswith("value_999")

    def test_empty_collection_and_item_handling(self) -> None:
        """Test behavior with empty collection and item_id."""
        event = NotificationEvent(
            source="/test",
            type="test",
            operation="DELETE",
            collection="",  # Empty collection
            item_id="",  # Empty item_id
        )

        assert event.collection == ""
        assert event.item_id == ""

    def test_operations_case_sensitivity(self) -> None:
        """Test that operations preserve case."""
        operations = [
            "INSERT",
            "insert",
            "Insert",
            "UPDATE",
            "update",
            "DELETE",
            "delete",
        ]

        for op in operations:
            event = NotificationEvent(
                source="/test", type="test", operation=op, collection="test"
            )
            assert event.operation == op


class TestNotificationEventComparison:
    """Test event comparison and equality."""

    def test_events_with_same_data_different_ids(self) -> None:
        """Test that events with same data but different IDs are not equal by ID."""
        base_data = {
            "source": "/test",
            "type": "test",
            "operation": "INSERT",
            "collection": "test",
            "data": {"key": "value"},
        }

        event1 = NotificationEvent(**base_data)  # type: ignore[arg-type]
        event2 = NotificationEvent(**base_data)  # type: ignore[arg-type]

        # IDs should be different (auto-generated)
        assert event1.id != event2.id

        # But other fields should match
        assert event1.source == event2.source
        assert event1.operation == event2.operation
        assert event1.data == event2.data

    def test_event_field_access(self) -> None:
        """Test accessing all event fields."""
        event = NotificationEvent(
            source="/eoapi/stac/pgstac",
            type="org.eoapi.stac.item",
            operation="UPDATE",
            collection="sentinel2",
            item_id="S2A_test_item",
        )

        # All fields should be accessible
        assert hasattr(event, "id")
        assert hasattr(event, "source")
        assert hasattr(event, "type")
        assert hasattr(event, "operation")
        assert hasattr(event, "collection")
        assert hasattr(event, "item_id")
        assert hasattr(event, "timestamp")
        assert hasattr(event, "data")

        # Check specific values
        assert event.source == "/eoapi/stac/pgstac"
        assert event.type == "org.eoapi.stac.item"
        assert event.operation == "UPDATE"
        assert event.collection == "sentinel2"
        assert event.item_id == "S2A_test_item"
