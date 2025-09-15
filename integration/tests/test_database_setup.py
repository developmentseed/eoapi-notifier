"""
Database setup validation tests.

Tests that PostgreSQL and pgSTAC are properly initialized using shared constants.
"""

import pytest
from shared.constants import Database, TestData


@pytest.mark.database
@pytest.mark.asyncio
class TestDatabaseSetup:
    """Test database initialization and schema setup."""

    async def test_postgres_connection(self, postgres_connection):
        """Test basic PostgreSQL connection."""
        result = await postgres_connection.fetchval("SELECT 1")
        assert result == 1

    async def test_pgstac_schema_exists(self, postgres_connection):
        """Test that pgSTAC schema and core tables exist."""
        tables = await postgres_connection.fetch(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{Database.SCHEMA}'
            ORDER BY table_name
        """)

        table_names = {row["table_name"] for row in tables}
        required_tables = {"collections", "items"}

        assert required_tables.issubset(table_names), (
            f"Missing tables: {required_tables - table_names}"
        )

    async def test_test_collections_exist(self, db_helper):
        """Test that test collections are created."""
        collections = await db_helper.get_collections()

        collection_ids = {col["id"] for col in collections}
        expected_collections = set(TestData.COLLECTION_IDS)

        assert expected_collections.issubset(collection_ids), (
            f"Missing collections: {expected_collections - collection_ids}"
        )

    async def test_notification_triggers_exist(self, postgres_connection):
        """Test that notification triggers are installed."""
        triggers = await postgres_connection.fetch(f"""
            SELECT trigger_name
            FROM information_schema.triggers
            WHERE event_object_schema = '{Database.SCHEMA}'
              AND event_object_table = 'items'
              AND trigger_name LIKE 'notify_items_change%'
        """)

        trigger_names = {row["trigger_name"] for row in triggers}
        expected_triggers = {
            "notify_items_change_insert",
            "notify_items_change_update",
            "notify_items_change_delete",
        }

        assert expected_triggers.issubset(trigger_names), (
            f"Missing triggers: {expected_triggers - trigger_names}"
        )

    async def test_notification_function_exists(self, postgres_connection):
        """Test that notification function exists."""
        functions = await postgres_connection.fetch("""
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_name = 'notify_items_change_func'
              AND routine_type = 'FUNCTION'
        """)

        assert len(functions) == 1, "Notification function not found"

    async def test_notification_channel(self, postgres_connection):
        """Test notification channel constant is correct."""
        # This test ensures our channel constant matches what's used in triggers
        assert Database.CHANNEL == "pgstac_items_change"

        # Verify we can listen to the channel
        notifications = []

        def handler(connection, pid, channel, payload):
            notifications.append(channel)

        await postgres_connection.add_listener(Database.CHANNEL, handler)
        await postgres_connection.remove_listener(Database.CHANNEL, handler)
        # Just testing that listener setup/teardown works without error
