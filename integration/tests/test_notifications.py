"""
PostgreSQL notification tests.

Tests that database notifications work correctly with pgSTAC triggers using
shared constants.
"""

import asyncio
import json
import time

import pytest
from shared.constants import Database, TestData


@pytest.mark.notifications
@pytest.mark.asyncio
class TestNotifications:
    """Test PostgreSQL notification functionality."""

    async def test_direct_notification_listener(self, postgres_connection, db_helper):
        """Test direct notification listening."""
        notifications = []

        def notification_handler(connection, pid, channel, payload):
            notifications.append(
                {
                    "channel": channel,
                    "payload": json.loads(payload),
                    "timestamp": time.time(),
                }
            )

        await postgres_connection.add_listener(Database.CHANNEL, notification_handler)

        # Trigger notification via test item insertion
        test_id = f"notify-test-{int(time.time())}"
        await db_helper.insert_test_item(test_id, TestData.COLLECTION_IDS[0])

        # Wait for notification
        await asyncio.sleep(1)

        await postgres_connection.remove_listener(
            Database.CHANNEL, notification_handler
        )

        assert len(notifications) == 1
        notification = notifications[0]
        assert notification["channel"] == Database.CHANNEL
        assert notification["payload"]["operation"] == "INSERT"
        assert len(notification["payload"]["items"]) == 1
        assert (
            notification["payload"]["items"][0]["collection"]
            == TestData.COLLECTION_IDS[0]
        )
        assert notification["payload"]["items"][0]["id"] == test_id

    async def test_insert_notification(self, postgres_connection, db_helper):
        """Test notification on item INSERT."""
        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(json.loads(payload))

        await postgres_connection.add_listener(Database.CHANNEL, handler)

        test_id = f"insert-test-{int(time.time())}"
        await db_helper.insert_test_item(test_id, TestData.COLLECTION_IDS[0])

        await asyncio.sleep(1)
        await postgres_connection.remove_listener(Database.CHANNEL, handler)

        assert len(notifications) == 1
        assert notifications[0]["operation"] == "INSERT"
        assert notifications[0]["items"][0]["id"] == test_id

    async def test_update_notification(self, postgres_connection, db_helper):
        """Test notification on item UPDATE."""
        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(json.loads(payload))

        # Insert item first
        test_id = f"update-test-{int(time.time())}"
        await db_helper.insert_test_item(test_id, TestData.COLLECTION_IDS[0])

        await postgres_connection.add_listener(Database.CHANNEL, handler)

        # Update item
        await db_helper.update_test_item(test_id, TestData.COLLECTION_IDS[0])

        await asyncio.sleep(1)
        await postgres_connection.remove_listener(Database.CHANNEL, handler)

        assert len(notifications) == 1
        assert notifications[0]["operation"] == "UPDATE"
        assert notifications[0]["items"][0]["id"] == test_id

    async def test_delete_notification(self, postgres_connection, db_helper):
        """Test notification on item DELETE."""
        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(json.loads(payload))

        # Insert item first
        test_id = f"delete-test-{int(time.time())}"
        await db_helper.insert_test_item(test_id, TestData.COLLECTION_IDS[0])

        await postgres_connection.add_listener(Database.CHANNEL, handler)

        # Delete item
        await db_helper.delete_test_item(test_id, TestData.COLLECTION_IDS[0])

        await asyncio.sleep(1)
        await postgres_connection.remove_listener(Database.CHANNEL, handler)

        assert len(notifications) == 1
        assert notifications[0]["operation"] == "DELETE"
        assert notifications[0]["items"][0]["id"] == test_id

    async def test_multiple_operations_sequence(self, postgres_connection, db_helper):
        """Test sequence of INSERT, UPDATE, DELETE operations."""
        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(json.loads(payload))

        await postgres_connection.add_listener(Database.CHANNEL, handler)

        test_id = f"sequence-test-{int(time.time())}"

        # INSERT
        await db_helper.insert_test_item(test_id, TestData.COLLECTION_IDS[0])
        await asyncio.sleep(0.5)

        # UPDATE
        await db_helper.update_test_item(test_id, TestData.COLLECTION_IDS[0])
        await asyncio.sleep(0.5)

        # DELETE
        await db_helper.delete_test_item(test_id, TestData.COLLECTION_IDS[0])
        await asyncio.sleep(1)

        await postgres_connection.remove_listener(Database.CHANNEL, handler)

        assert len(notifications) == 3
        operations = [n["operation"] for n in notifications]
        assert operations == ["INSERT", "UPDATE", "DELETE"]

        # All should reference the same item
        for notification in notifications:
            assert notification["items"][0]["id"] == test_id
            assert notification["items"][0]["collection"] == TestData.COLLECTION_IDS[0]

    async def test_notification_payload_structure(self, postgres_connection, db_helper):
        """Test notification payload has correct structure."""
        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(json.loads(payload))

        await postgres_connection.add_listener(Database.CHANNEL, handler)

        test_id = f"payload-test-{int(time.time())}"
        await db_helper.insert_test_item(test_id, TestData.COLLECTION_IDS[0])

        await asyncio.sleep(1)
        await postgres_connection.remove_listener(Database.CHANNEL, handler)

        assert len(notifications) == 1
        payload = notifications[0]

        # Check required fields
        assert "operation" in payload
        assert "items" in payload
        assert isinstance(payload["items"], list)
        assert len(payload["items"]) == 1

        item = payload["items"][0]
        assert "collection" in item
        assert "id" in item
        assert item["collection"] == TestData.COLLECTION_IDS[0]
        assert item["id"] == test_id

    async def test_multiple_collections(self, postgres_connection, db_helper):
        """Test notifications work with different collections."""
        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(json.loads(payload))

        await postgres_connection.add_listener(Database.CHANNEL, handler)

        test_time = int(time.time())

        # Insert into different collections
        for i, collection_id in enumerate(TestData.COLLECTION_IDS):
            test_id = f"multi-collection-test-{test_time}-{i}"
            await db_helper.insert_test_item(test_id, collection_id)
            await asyncio.sleep(0.5)

        await asyncio.sleep(1)
        await postgres_connection.remove_listener(Database.CHANNEL, handler)

        assert len(notifications) >= len(TestData.COLLECTION_IDS)

        # Check we got notifications for different collections
        collections_notified = set()
        for notification in notifications:
            if notification["operation"] == "INSERT":
                collections_notified.add(notification["items"][0]["collection"])

        assert len(collections_notified) >= 2, (
            f"Expected notifications from multiple collections, "
            f"got: {collections_notified}"
        )
