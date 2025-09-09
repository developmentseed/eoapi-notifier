"""
Tests for pgstac source plugin.

"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eoapi_notifier.core.event import NotificationEvent
from eoapi_notifier.core.plugin import PluginError, PluginMetadata
from eoapi_notifier.core.registry import create_source, get_available_sources
from eoapi_notifier.sources.pgstac import PgSTACSource, PgSTACSourceConfig


class TestPgSTACSourceConfig:
    """Test pgSTAC source configuration."""

    def test_config_implements_protocol(self) -> None:
        """Test that config implements required protocol methods."""
        config = PgSTACSourceConfig()

        # Test required protocol methods exist
        assert hasattr(config, "get_sample_config")
        assert hasattr(config, "get_metadata")
        assert hasattr(config, "get_connection_info")
        assert hasattr(config, "get_status_info")

        # Test they return expected types
        assert isinstance(config.get_sample_config(), dict)
        assert isinstance(config.get_metadata(), PluginMetadata)
        assert isinstance(config.get_connection_info(), str)
        assert isinstance(config.get_status_info(), dict)

    def test_default_configuration(self) -> None:
        """Test default configuration values."""
        config = PgSTACSourceConfig()

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "pgstac"
        assert config.user == "postgres"
        assert config.password == ""
        assert config.channel == "pgstac_items"
        assert config.event_source == "/eoapi/stac/pgstac"

    def test_custom_configuration(self) -> None:
        """Test configuration with custom values."""
        config = PgSTACSourceConfig(
            host="custom-host",
            port=9999,
            database="test-db",
            user="testuser",
            password="secret",
            channel="test-channel",
            queue_size=500,
            listen_timeout=2.0,
            event_source="/test/source",
            event_type="custom.event.type",
        )

        assert config.host == "custom-host"
        assert config.port == 9999
        assert config.database == "test-db"
        assert config.user == "testuser"
        assert config.password == "secret"
        assert config.channel == "test-channel"
        assert config.queue_size == 500
        assert config.listen_timeout == 2.0
        assert config.event_source == "/test/source"
        assert config.event_type == "custom.event.type"

    def test_port_validation_error(self) -> None:
        """Test port validation."""
        # Valid ports should work
        PgSTACSourceConfig(port=1)
        PgSTACSourceConfig(port=65535)

        # Invalid ports should raise validation error
        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            PgSTACSourceConfig(port=0)

        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            PgSTACSourceConfig(port=65536)

    def test_get_sample_config(self) -> None:
        """Test sample configuration generation."""
        sample = PgSTACSourceConfig.get_sample_config()

        expected_keys = {
            "host",
            "port",
            "database",
            "user",
            "password",
            "channel",
            "max_reconnect_attempts",
            "reconnect_delay",
        }
        assert set(sample.keys()) == expected_keys
        assert isinstance(sample["port"], int)
        assert sample["port"] == 5432

    def test_get_metadata(self) -> None:
        """Test metadata generation."""
        metadata = PgSTACSourceConfig.get_metadata()

        assert metadata.name == "pgstac"
        assert "pgSTAC LISTEN/NOTIFY" in metadata.description
        assert metadata.version == "2.0.0"
        assert metadata.category == "database"
        assert "postgresql" in metadata.tags
        assert "pgstac" in metadata.tags
        assert "stac" in metadata.tags
        assert metadata.priority == 10

    def test_connection_info(self) -> None:
        """Test connection info string generation."""
        config = PgSTACSourceConfig(host="test-host", port=1234, database="test-db")
        info = config.get_connection_info()

        assert info == "postgresql://postgres@test-host:1234/test-db"

    def test_status_info(self) -> None:
        """Test status info generation."""
        config = PgSTACSourceConfig(
            host="test-host",
            port=1234,
            database="test-db",
            user="test-user",
            channel="test-channel",
        )
        status = config.get_status_info()

        assert status["Host"] == "test-host:1234"
        assert status["Database"] == "test-db"
        # User is no longer included in status info
        assert status["Channel"] == "test-channel"


class TestPgSTACSourceRegistry:
    """Test pgSTAC source registry integration."""

    def test_postgres_source_registered(self) -> None:
        """Test that postgres source is registered."""
        available = get_available_sources()
        assert "postgres" in available

    def test_create_source_from_registry(self) -> None:
        """Test creating pgSTAC source through registry."""
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "pgstac",
            "user": "test",
            "password": "test",
        }

        source = create_source("postgres", config)

        assert isinstance(source, PgSTACSource)
        assert isinstance(source.config, PgSTACSourceConfig)
        assert source.config.host == "localhost"
        assert source.config.user == "test"

    def test_create_source_with_validation_error(self) -> None:
        """Test source creation with invalid configuration."""
        config = {
            "port": 99999,  # Invalid port
        }

        with pytest.raises(PluginError):  # Should raise validation error
            create_source("postgres", config)


class TestPgSTACSource:
    """Test pgSTAC source functionality."""

    @pytest.fixture
    def mock_config(self) -> PgSTACSourceConfig:
        """Create a mock configuration for testing."""
        return PgSTACSourceConfig(
            host="test-host",
            port=5432,
            database="test-db",
            user="test-user",
            password="test-pass",
            channel="test-channel",
        )

    @pytest.fixture
    def source(self, mock_config: PgSTACSourceConfig) -> PgSTACSource:
        """Create a pgSTAC source for testing."""
        return PgSTACSource(mock_config)

    def test_source_initialization(
        self, source: PgSTACSource, mock_config: PgSTACSourceConfig
    ) -> None:
        """Test source initialization."""
        assert source.config == mock_config
        assert not source.is_running
        assert not source.is_started
        assert source._connection is None
        assert source._notification_queue is None

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_start_and_stop(
        self, mock_asyncpg: MagicMock, source: PgSTACSource
    ) -> None:
        """Test source start and stop lifecycle."""
        # Mock the asyncpg connection
        mock_connection = AsyncMock()
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)

        # Test start
        await source.start()

        assert source.is_running
        assert source.is_started
        assert source._connection == mock_connection
        assert source._notification_queue is not None

        # Verify connection setup
        mock_asyncpg.connect.assert_called_once_with(
            host="test-host",
            port=5432,
            database="test-db",
            user="test-user",
            password="test-pass",
        )
        mock_connection.execute.assert_called_once_with("LISTEN test-channel")
        mock_connection.add_listener.assert_called_once()

        # Test stop
        await source.stop()

        assert not source.is_running
        assert not source.is_started
        assert source._connection is None
        assert source._notification_queue is None

        # Verify cleanup
        mock_connection.remove_listener.assert_called_once()
        mock_connection.execute.assert_called_with("UNLISTEN test-channel")
        mock_connection.close.assert_called_once()

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_source_double_start_error(
        self, mock_asyncpg: MagicMock, source: PgSTACSource
    ) -> None:
        """Test that starting an already started source raises error."""
        mock_connection = AsyncMock()
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)

        await source.start()

        with pytest.raises(RuntimeError, match="already started"):
            await source.start()

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_listen_without_connection_error(
        self, mock_asyncpg: MagicMock, source: PgSTACSource
    ) -> None:
        """Test that listening without connection raises error."""
        with pytest.raises(RuntimeError, match="pgSTAC source not started"):
            async for _event in source.listen():
                pass

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_notification_processing(
        self, mock_asyncpg: MagicMock, source: PgSTACSource
    ) -> None:
        """Test processing of notifications."""
        # Mock connection and setup
        mock_connection = AsyncMock()
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)

        await source.start()

        # Mock notification payload
        payload = (
            '{"operation": "INSERT", "collection": "test-collection", '
            '"item_id": "test-item", "timestamp": "2023-01-01T00:00:00Z"}'
        )

        # Simulate notification callback
        source._notification_callback(None, 0, "test-channel", payload)

        # Process the notification
        events = []
        async for event in source.listen():
            events.append(event)
            break  # Only process one event for test

        # Verify event processing
        assert len(events) == 1
        event = events[0]

        assert isinstance(event, NotificationEvent)
        assert event.source == "/eoapi/stac/pgstac"
        assert event.type == "org.eoapi.stac.item"
        assert event.operation == "INSERT"
        assert event.collection == "test-collection"
        assert event.item_id == "test-item"

        await source.stop()

    def test_process_notification_payload_invalid_json(
        self, source: PgSTACSource
    ) -> None:
        """Test handling of invalid JSON payload."""
        result = source._process_notification_payload("invalid json")
        assert result is None

    def test_from_pgstac_notification_basic(self, source: PgSTACSource) -> None:
        """Test conversion from pgSTAC notification to NotificationEvent."""
        payload = {
            "operation": "UPDATE",
            "collection": "test-collection",
            "item_id": "test-item-123",
            "timestamp": "2023-01-01T12:00:00Z",
        }

        event = source._from_pgstac_notification(payload)

        assert event.source == "/eoapi/stac/pgstac"
        assert event.type == "org.eoapi.stac.item"
        assert event.operation == "UPDATE"
        assert event.collection == "test-collection"
        assert event.item_id == "test-item-123"
        assert event.data == payload

    def test_from_pgstac_notification_custom_types(self) -> None:
        """Test notification conversion with custom event type."""
        config = PgSTACSourceConfig(event_type="custom.stac.item")
        source = PgSTACSource(config)

        payload: dict[str, Any] = {
            "operation": "INSERT",
            "collection": "test-collection",
            "item_id": "test-item",
        }

        event = source._from_pgstac_notification(payload)

        assert event.type == "custom.stac.item"

    def test_from_pgstac_notification_defaults(self, source: PgSTACSource) -> None:
        """Test notification conversion with missing fields."""
        payload: dict[str, Any] = {}  # Minimal payload

        event = source._from_pgstac_notification(payload)

        assert event.operation == "INSERT"  # Default
        assert event.collection == "unknown"  # Default
        assert event.item_id is None
        assert event.timestamp is not None  # Auto-generated

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_context_manager(
        self, mock_asyncpg: MagicMock, source: PgSTACSource
    ) -> None:
        """Test source as async context manager."""
        mock_connection = AsyncMock()
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)

        async with source:
            assert source.is_running
            assert source.is_started

        assert not source.is_running
        assert not source.is_started

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_event_stream_context(
        self, mock_asyncpg: MagicMock, source: PgSTACSource
    ) -> None:
        """Test event stream async context manager."""
        mock_connection = AsyncMock()
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)

        async with source.event_stream() as stream:
            assert source.is_running
            # Stream should be available but we won't iterate for this test
            assert stream is not None

        assert not source.is_running

    def test_get_status(self, source: PgSTACSource) -> None:
        """Test source status reporting."""
        status = source.get_status()

        assert status["name"] == "pgstac"
        assert status["type"] == "PgSTACSource"
        assert status["running"] is False
        assert status["started"] is False
        assert "metadata" in status
        assert (
            status["metadata"]["description"]
            == "Resilient pgSTAC LISTEN/NOTIFY source with auto-reconnection"
        )
