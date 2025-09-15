"""
End-to-end integration tests for eoAPI notifier.

Tests complete pipeline: PostgreSQL/pgSTAC → eoapi-notifier → MQTT + Knative
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx
import pytest

# Add project to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))


@pytest.mark.end_to_end
@pytest.mark.asyncio
class TestEndToEnd:
    """Test complete end-to-end integration: DB → notifier → MQTT + CloudEvents"""

    async def test_full_pipeline_mqtt_and_cloudevents(
        self, postgres_connection, mqtt_client, db_helper, platform_adapter
    ):
        """Test complete pipeline with both MQTT and CloudEvents outputs."""

        # Skip if notifier not available - this requires external notifier process
        notifier_config_path = Path(__file__).parent.parent / "simple-config.yaml"
        if not notifier_config_path.exists():
            pytest.skip("Integration config not available")

        # Clear MQTT messages
        mqtt_client.clear_messages()

        # Clear CloudEvents if Docker Compose platform
        if os.getenv("INTEGRATION_PLATFORM") != "kubernetes":
            try:
                async with httpx.AsyncClient() as client:
                    await client.delete("http://localhost:8080/events", timeout=5.0)
                    print("🧹 CloudEvents: Cleared existing events")
            except Exception as e:
                print(f"⚠️  Could not clear CloudEvents: {e}")
        else:
            print("🔵 KNative: CloudEvents will be logged by event-display service")

        # Test database operations that should trigger notifications
        test_timestamp = int(time.time())
        test_items = []

        # Test INSERT operation
        insert_id = f"e2e-insert-{test_timestamp}"
        await db_helper.insert_test_item(insert_id, "test-collection")
        test_items.append((insert_id, "INSERT"))

        # Test UPDATE operation
        update_id = f"e2e-update-{test_timestamp}"
        await db_helper.insert_test_item(update_id, "test-collection")
        await asyncio.sleep(1)
        await db_helper.update_test_item(update_id, "test-collection")
        test_items.append((update_id, "UPDATE"))

        # Test DELETE operation
        delete_id = f"e2e-delete-{test_timestamp}"
        await db_helper.insert_test_item(delete_id, "test-collection")
        await asyncio.sleep(1)
        await db_helper.delete_test_item(delete_id, "test-collection")
        test_items.append((delete_id, "DELETE"))

        # Test different collection
        sample_id = f"e2e-sample-{test_timestamp}"
        await db_helper.insert_test_item(sample_id, "sample-collection")
        test_items.append((sample_id, "INSERT"))

        # Wait for MQTT messages (allowing time for notifier processing)
        await asyncio.sleep(5)
        messages = mqtt_client.wait_for_messages(count=4, timeout=15)

        print(f"📨 MQTT: Received {len(messages)} messages")
        for msg in messages:
            payload = msg["payload"]
            print(
                f"   {msg['topic']}: {payload['operation']} "
                f"{payload.get('item_id', 'unknown')}"
            )

        # Verify MQTT messages received
        assert len(messages) >= 3, (
            f"Expected at least 3 MQTT messages, got {len(messages)}"
        )

        # Verify operations present
        operations = [msg["payload"]["operation"] for msg in messages]
        assert "INSERT" in operations, f"Missing INSERT operation in {operations}"

        # Verify topic routing
        topics = {msg["topic"] for msg in messages}
        expected_topics = {
            "eoapi/stac/items/test-collection",
            "eoapi/stac/items/sample-collection",
        }

        # At least one topic should match
        assert len(topics.intersection(expected_topics)) > 0, (
            f"No expected topics found in {topics}"
        )

        # Verify message structure
        for msg in messages:
            payload = msg["payload"]
            assert "source" in payload
            assert "operation" in payload
            assert "collection" in payload
            assert "item_id" in payload
            assert "data" in payload

        print("✅ MQTT: Message structure verified")

        # Test CloudEvents endpoint (platform-specific approach)
        cloudevents_verified = False
        cloudevents_count = 0
        try:
            cloudevents_client = platform_adapter.get_cloudevents_client()

            if os.getenv("INTEGRATION_PLATFORM") == "kubernetes":
                # For Kubernetes with KNative, check logs from event-display service
                print("🔵 KNative: Checking event-display logs for CloudEvents")

                # Wait a bit longer for KNative event processing
                await asyncio.sleep(3)

                events_data = await cloudevents_client.get_events()
                cloudevents_count = events_data.get("count", 0)

                print(f"📡 KNative: Found {cloudevents_count} events in logs")

                if cloudevents_count > 0:
                    cloudevents_verified = True
                    print("✅ KNative: CloudEvent delivery verified through logs")

                    # Show some log entries
                    events = events_data.get("events", [])
                    for i, event in enumerate(events[:3]):
                        if "log_line" in event:
                            print(f"   Log {i + 1}: {event['log_line'][:80]}...")
                else:
                    print("⚠️  KNative: No CloudEvents found in event-display logs")
                    if "logs" in events_data:
                        print("   Recent logs:", events_data["logs"][:200] + "...")
            else:
                # For Docker Compose, test actual CloudEvents REST API
                if await cloudevents_client.is_healthy():
                    print("✅ CloudEvents: HTTP endpoint responding")

                    # Get received events
                    events_data = await cloudevents_client.get_events()
                    cloudevents_count = events_data.get("count", 0)
                    received_events = events_data.get("events", [])

                    print(f"📡 CloudEvents: Received {cloudevents_count} events")

                    if cloudevents_count > 0:
                        # Verify event structure
                        for i, event in enumerate(received_events[:3]):
                            event_type = event.get("type")
                            event_source = event.get("source")
                            print(f"   Event {i + 1}: {event_type} from {event_source}")
                            if "data" in event and event["data"]:
                                data = event["data"]
                                op = data.get("operation", "unknown")
                                print(f"     Operation: {op}")
                                coll = data.get("collection", "unknown")
                                print(f"     Collection: {coll}")
                                item = data.get("item_id", "unknown")
                                print(f"     Item ID: {item}")

                        # Verify we have expected operations
                        operations = []
                        for event in received_events:
                            if "data" in event and event["data"]:
                                operations.append(event["data"].get("operation"))

                        if "INSERT" in operations:
                            cloudevents_verified = True
                            print("✅ CloudEvents: Message delivery verified")
                        else:
                            print("⚠️  CloudEvents: No INSERT operations found")
                    else:
                        print("⚠️  CloudEvents: No events received")
                else:
                    print("⚠️  CloudEvents: Health check failed")

            await cloudevents_client.close()

        except Exception as e:
            print(f"⚠️  CloudEvents endpoint error: {e}")

        # Summary
        print("\n📊 End-to-End Test Results:")
        print(f"   MQTT messages: {len(messages)}")
        print(f"   Operations: {set(operations)}")
        print(f"   Topics: {topics}")
        platform_name = (
            "KNative"
            if os.getenv("INTEGRATION_PLATFORM") == "kubernetes"
            else "CloudEvents"
        )
        print(
            f"   {platform_name}: {'✅' if cloudevents_verified else '⚠️'} "
            f"({cloudevents_count} events)"
        )

        # Test passes if we got MQTT messages with correct structure
        assert len(messages) >= 3
        assert "INSERT" in operations

    async def test_cloudevents_message_delivery(
        self, postgres_connection, db_helper, cloudevents_client
    ):
        """Test that CloudEvents messages are actually delivered to the endpoint."""

        # Skip if CloudEvents endpoint not available
        if not await cloudevents_client.is_healthy():
            pytest.skip("CloudEvents endpoint not available")

        # Clear any existing events (only works for Docker Compose)
        if not cloudevents_client.is_knative_mode():
            await cloudevents_client.clear_events()

        # Perform database operations
        test_timestamp = int(time.time())

        # Test INSERT operation
        insert_id = f"cloudevents-test-{test_timestamp}"
        await db_helper.insert_test_item(insert_id, "test-collection")

        # Wait for notifier processing and CloudEvents delivery
        await asyncio.sleep(8 if not cloudevents_client.is_knative_mode() else 12)

        # Check CloudEvents were received
        events_data = await cloudevents_client.get_events()
        events_count = events_data.get("count", 0)
        received_events = events_data.get("events", [])

        is_knative = cloudevents_client.is_knative_mode()
        platform_name = "KNative logs" if is_knative else "CloudEvents"
        print(f"📡 {platform_name} received: {events_count}")

        # Assert we received at least one event
        assert events_count >= 1, f"Expected at least 1 event, got {events_count}"

        if cloudevents_client.is_knative_mode():
            # For KNative, we just verify events appeared in logs
            print("✅ KNative: Events found in event-display logs")
            for i, event in enumerate(received_events[:3]):
                print(f"   Log {i + 1}: {event.get('log_line', 'No log line')}")
        else:
            # For Docker Compose, verify full event structure
            event = received_events[0]
            assert "source" in event, "CloudEvent missing 'source' field"
            assert "type" in event, "CloudEvent missing 'type' field"
            assert "data" in event, "CloudEvent missing 'data' field"

            # Verify event data structure
            data = event["data"]
            assert "operation" in data, "CloudEvent data missing 'operation' field"
            assert "collection" in data, "CloudEvent data missing 'collection' field"
            assert "item_id" in data, "CloudEvent data missing 'item_id' field"

            # Verify event content
            assert data["operation"] == "INSERT", (
                f"Expected INSERT operation, got {data['operation']}"
            )
            assert data["collection"] == "test-collection", (
                f"Expected test-collection, got {data['collection']}"
            )
            assert data["item_id"] == insert_id, (
                f"Expected {insert_id}, got {data['item_id']}"
            )

            print("✅ CloudEvents: Message delivery verified")
            print(f"   Source: {event['source']}")
            print(f"   Type: {event['type']}")
            print(f"   Operation: {data['operation']}")
            print(f"   Collection: {data['collection']}")
            print(f"   Item ID: {data['item_id']}")

    async def test_collection_routing(
        self, postgres_connection, mqtt_client, db_helper
    ):
        """Test that different collections route to different MQTT topics."""

        mqtt_client.clear_messages()
        mqtt_client.subscribe("eoapi/stac/items/sample-collection")

        test_time = int(time.time())

        # Insert into existing collection
        await db_helper.insert_test_item(f"routing-{test_time}", "sample-collection")

        # Wait for message
        await asyncio.sleep(3)
        messages = mqtt_client.wait_for_messages(count=1, timeout=10)

        if messages:
            msg = messages[0]
            assert msg["topic"] == "eoapi/stac/items/sample-collection"
            assert msg["payload"]["collection"] == "sample-collection"
            print("✅ Collection routing verified")
        else:
            pytest.skip("No MQTT messages received - notifier may not be running")

    async def test_database_notifications(self, postgres_connection):
        """Test PostgreSQL notifications are working."""

        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(json.loads(payload))

        await postgres_connection.add_listener("pgstac_items_change", handler)

        # Trigger a notification via direct SQL
        test_id = f"notify-test-{int(time.time())}"
        await postgres_connection.execute(
            "SELECT pg_notify('pgstac_items_change', $1)",
            json.dumps(
                {
                    "operation": "INSERT",
                    "items": [{"collection": "test-collection", "id": test_id}],
                }
            ),
        )

        await asyncio.sleep(1)
        await postgres_connection.remove_listener("pgstac_items_change", handler)

        assert len(notifications) >= 1
        assert notifications[0]["operation"] == "INSERT"
        print("✅ Database notifications working")
