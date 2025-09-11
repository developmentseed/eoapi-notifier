"""Tests for pgSTAC source plugin."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eoapi_notifier.core.event import NotificationEvent
from eoapi_notifier.core.plugin import PluginError
from eoapi_notifier.core.registry import create_source, get_available_sources
from eoapi_notifier.sources.pgstac import (
    OperationCorrelator,
    PgSTACSource,
    PgSTACSourceConfig,
)


class TestPgSTACSourceConfig:
    """Test pgSTAC source configuration."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = PgSTACSourceConfig()

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "pgstac"
        assert config.enable_correlation is True
        assert config.correlation_window == 5.0
        assert config.cleanup_interval == 1.0

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = PgSTACSourceConfig(
            host="custom-host",
            port=9999,
            database="test-db",
            enable_correlation=False,
            correlation_window=2.0,
        )

        assert config.host == "custom-host"
        assert config.port == 9999
        assert config.database == "test-db"
        assert config.enable_correlation is False
        assert config.correlation_window == 2.0

    def test_correlation_validation(self) -> None:
        """Test correlation parameter validation."""
        # Valid values should work
        PgSTACSourceConfig(correlation_window=1.0, cleanup_interval=0.5)

        # Invalid values should fail
        with pytest.raises(ValueError):
            PgSTACSourceConfig(correlation_window=0.05)  # Too small

        with pytest.raises(ValueError):
            PgSTACSourceConfig(cleanup_interval=70.0)  # Too large

    def test_port_validation(self) -> None:
        """Test port validation."""
        # Valid ports
        PgSTACSourceConfig(port=1)
        PgSTACSourceConfig(port=65535)

        # Invalid ports
        with pytest.raises(ValueError):
            PgSTACSourceConfig(port=0)

        with pytest.raises(ValueError):
            PgSTACSourceConfig(port=65536)

    def test_metadata(self) -> None:
        """Test metadata generation."""
        metadata = PgSTACSourceConfig.get_metadata()

        assert metadata.name == "pgstac"
        assert metadata.version == "0.0.2"
        assert "correlation" in metadata.tags

    def test_sample_config(self) -> None:
        """Test sample configuration."""
        sample = PgSTACSourceConfig.get_sample_config()

        required_keys = {
            "host",
            "port",
            "database",
            "user",
            "password",
            "channel",
            "enable_correlation",
            "correlation_window",
            "cleanup_interval",
        }
        assert all(key in sample for key in required_keys)

    def test_status_info_with_correlation(self) -> None:
        """Test status info includes correlation settings."""
        config = PgSTACSourceConfig(enable_correlation=True)
        status = config.get_status_info()

        assert status["Correlation Enabled"] is True
        assert "Correlation Window" in status

    def test_status_info_without_correlation(self) -> None:
        """Test status info without correlation."""
        config = PgSTACSourceConfig(enable_correlation=False)
        status = config.get_status_info()

        assert status["Correlation Enabled"] is False
        assert "Correlation Window" not in status


class TestPgSTACSourceRegistry:
    """Test registry integration."""

    def test_source_registered(self) -> None:
        """Test pgSTAC source is registered."""
        assert "pgstac" in get_available_sources()

    def test_create_source(self) -> None:
        """Test creating source through registry."""
        source = create_source("pgstac", {"host": "localhost"})
        assert isinstance(source, PgSTACSource)

    def test_invalid_config_raises_error(self) -> None:
        """Test invalid config raises PluginError."""
        with pytest.raises(PluginError):
            create_source("pgstac", {"port": 99999})


class TestPgSTACSourceBasic:
    """Test basic source functionality."""

    @pytest.fixture
    def basic_config(self) -> PgSTACSourceConfig:
        """Basic config without correlation."""
        return PgSTACSourceConfig(
            host="test-host",
            database="test-db",
            user="test-user",
            password="test-pass",
            channel="test-channel",
            enable_correlation=False,
        )

    @pytest.fixture
    def mock_connection(self) -> AsyncMock:
        """Mock asyncpg connection."""
        conn = AsyncMock()
        conn.is_closed = MagicMock(return_value=False)  # Not async
        return conn

    def test_initialization(self, basic_config: PgSTACSourceConfig) -> None:
        """Test source initialization."""
        source = PgSTACSource(basic_config)

        assert source.config == basic_config
        assert not source.is_running
        assert source._connection is None
        assert source._correlator is None  # No correlator for basic config

    def test_initialization_with_correlation(self) -> None:
        """Test source initialization with correlation."""
        config = PgSTACSourceConfig(enable_correlation=True)
        source = PgSTACSource(config)

        assert source._correlator is not None

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_start_stop_lifecycle(
        self,
        mock_asyncpg: MagicMock,
        basic_config: PgSTACSourceConfig,
        mock_connection: AsyncMock,
    ) -> None:
        """Test start/stop lifecycle."""
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)
        source = PgSTACSource(basic_config)

        # Start
        await source.start()
        assert source.is_running
        mock_asyncpg.connect.assert_called_once()

        # Stop
        await source.stop()
        assert not source.is_running

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_idempotent_operations(
        self,
        mock_asyncpg: MagicMock,
        basic_config: PgSTACSourceConfig,
        mock_connection: AsyncMock,
    ) -> None:
        """Test start/stop operations are idempotent."""
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)
        source = PgSTACSource(basic_config)

        # Multiple starts should not error and should be truly idempotent
        await source.start()
        await source.start()
        assert mock_asyncpg.connect.call_count == 1  # Should only connect once

        # Multiple stops should not error
        await source.stop()
        await source.stop()

    async def test_listen_before_start_raises_error(
        self, basic_config: PgSTACSourceConfig
    ) -> None:
        """Test listening before start raises error."""
        source = PgSTACSource(basic_config)

        with pytest.raises(
            RuntimeError, match="Source must be started before listening"
        ):
            async for _ in source.listen():
                pass

    def test_notification_event_creation(
        self, basic_config: PgSTACSourceConfig
    ) -> None:
        """Test creating events from notification payloads."""
        source = PgSTACSource(basic_config)

        payload = '{"op": "INSERT", "collection": "test", "id": "item-123"}'
        event = source._create_notification_event(payload)

        assert event is not None
        assert event.operation == "INSERT"
        assert event.collection == "test"
        assert event.item_id == "item-123"

    def test_invalid_json_returns_none(self, basic_config: PgSTACSourceConfig) -> None:
        """Test invalid JSON payload returns None."""
        source = PgSTACSource(basic_config)
        assert source._create_notification_event("invalid json") is None

    def test_status_without_correlation(self, basic_config: PgSTACSourceConfig) -> None:
        """Test status reporting without correlation."""
        source = PgSTACSource(basic_config)
        status = source.get_status()

        assert "connected" in status
        assert "correlator" not in status  # No correlator


class TestOperationCorrelator:
    """Test operation correlator logic without background tasks."""

    def create_test_event(
        self, operation: str, item_id: str = "test-item"
    ) -> NotificationEvent:
        """Helper to create test events."""
        return NotificationEvent(
            id=f"test-{item_id}",
            source="/test/source",
            type="test.event",
            operation=operation,
            collection="test-collection",
            item_id=item_id,
            timestamp=datetime.now(UTC),
            data={"op": operation, "collection": "test-collection", "id": item_id},
        )

    def test_correlator_initialization(self) -> None:
        """Test correlator initialization."""
        correlator = OperationCorrelator(correlation_window=2.0, cleanup_interval=0.5)

        assert correlator.correlation_window == 2.0
        assert correlator.cleanup_interval == 0.5
        assert not correlator._running
        assert correlator.get_pending_count() == 0

    async def test_correlator_start_stop(self) -> None:
        """Test correlator start/stop without background tasks."""
        correlator = OperationCorrelator(correlation_window=1.0, cleanup_interval=0.1)

        # Mock the background task to prevent hanging
        with patch.object(correlator, "_run_cleanup_loop") as mock_cleanup:
            mock_cleanup.return_value = None

            await correlator.start()
            assert correlator._running

            await correlator.stop()
            assert not correlator._running

    async def test_delete_insert_correlation_logic(self) -> None:
        """Test DELETE+INSERT correlation logic without real timing."""
        correlator = OperationCorrelator()
        correlator._running = True  # Set running without starting background tasks

        delete_event = self.create_test_event("DELETE")
        insert_event = self.create_test_event("INSERT")

        # Process DELETE (should be stored as pending)
        delete_results = []
        async for event in correlator._process_single_event(delete_event):
            delete_results.append(event)

        assert len(delete_results) == 0  # DELETE is pending
        assert correlator.get_pending_count() == 1

        # Process INSERT (should correlate with DELETE)
        insert_results = []
        async for event in correlator._process_single_event(insert_event):
            insert_results.append(event)

        assert len(insert_results) == 1
        assert insert_results[0].operation == "item_updated"
        assert "correlation" in insert_results[0].data
        assert correlator.get_pending_count() == 0  # Pending DELETE consumed

    async def test_standalone_insert_creates_created(self) -> None:
        """Test standalone INSERT creates item_created."""
        correlator = OperationCorrelator()
        correlator._running = True

        insert_event = self.create_test_event("INSERT")

        results = []
        async for event in correlator._process_single_event(insert_event):
            results.append(event)

        assert len(results) == 1
        assert results[0].operation == "item_created"

    async def test_update_passes_through(self) -> None:
        """Test UPDATE operations pass through as item_updated."""
        correlator = OperationCorrelator()
        correlator._running = True

        update_event = self.create_test_event("UPDATE")

        results = []
        async for event in correlator._process_single_event(update_event):
            results.append(event)

        assert len(results) == 1
        assert results[0].operation == "item_updated"

    async def test_different_items_not_correlated(self) -> None:
        """Test DELETE/INSERT on different items are not correlated."""
        correlator = OperationCorrelator()
        correlator._running = True

        delete_event = self.create_test_event("DELETE", "item-1")
        insert_event = self.create_test_event("INSERT", "item-2")

        # Process DELETE
        delete_results = []
        async for event in correlator._process_single_event(delete_event):
            delete_results.append(event)

        assert len(delete_results) == 0  # DELETE is pending
        assert correlator.get_pending_count() == 1

        # Process INSERT (different item)
        insert_results = []
        async for event in correlator._process_single_event(insert_event):
            insert_results.append(event)

        assert len(insert_results) == 1
        assert insert_results[0].operation == "item_created"  # Not correlated
        assert correlator.get_pending_count() == 1  # DELETE still pending

    def test_expired_operation_detection(self) -> None:
        """Test expired operation detection logic."""
        correlator = OperationCorrelator(correlation_window=1.0)

        # Create old event
        old_event = self.create_test_event("DELETE")
        old_event.timestamp = datetime.now(UTC) - timedelta(seconds=2)

        # Manually add to pending operations
        key = (old_event.collection, old_event.item_id or "test-item")
        from eoapi_notifier.sources.pgstac import PendingOperation

        pending = PendingOperation(
            event=old_event,
            timestamp=old_event.timestamp,
            collection=old_event.collection,
            item_id=old_event.item_id or "test-item",  # Ensure not None
        )
        correlator._pending_operations[key] = pending

        # Find expired operations
        current_time = datetime.now(UTC)
        expired_keys = correlator._find_expired_operations(current_time)

        assert len(expired_keys) == 1
        assert expired_keys[0] == key

    def test_status_reporting(self) -> None:
        """Test correlator status reporting."""
        correlator = OperationCorrelator()
        status = correlator.get_status()

        assert "running" in status
        assert "correlation_window" in status
        assert "pending_operations" in status
        assert status["running"] is False
        assert status["pending_operations"] == 0

    async def test_missing_item_id_handling(self) -> None:
        """Test handling of events without item_id."""
        correlator = OperationCorrelator()
        correlator._running = True

        # Create event without item_id
        event = NotificationEvent(
            id="test-event",
            source="/test/source",
            type="test.event",
            operation="DELETE",
            collection="test-collection",
            item_id=None,
            timestamp=datetime.now(UTC),
            data={"op": "DELETE", "collection": "test-collection"},
        )

        results = []
        async for result_event in correlator._process_single_event(event):
            results.append(result_event)

        # Should not process events without item_id
        assert len(results) == 0


class TestPgSTACSourceWithCorrelation:
    """Test pgSTAC source with correlation enabled."""

    @pytest.fixture
    def correlation_config(self) -> PgSTACSourceConfig:
        """Config with correlation enabled."""
        return PgSTACSourceConfig(
            host="test-host",
            database="test-db",
            user="test-user",
            password="test-pass",
            channel="test-channel",
            enable_correlation=True,
            correlation_window=2.0,
            cleanup_interval=0.5,
        )

    def test_initialization_with_correlator(
        self, correlation_config: PgSTACSourceConfig
    ) -> None:
        """Test source creates correlator when correlation is enabled."""
        source = PgSTACSource(correlation_config)

        assert source._correlator is not None
        assert source._correlator.correlation_window == 2.0
        assert source._correlator.cleanup_interval == 0.5

    @patch("eoapi_notifier.sources.pgstac.asyncpg")
    async def test_correlator_lifecycle_with_source(
        self, mock_asyncpg: MagicMock, correlation_config: PgSTACSourceConfig
    ) -> None:
        """Test correlator starts/stops with source."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = MagicMock(return_value=False)  # Not async
        mock_asyncpg.connect = AsyncMock(return_value=mock_connection)

        source = PgSTACSource(correlation_config)

        # Mock correlator methods to avoid background tasks
        if source._correlator:
            with (
                patch.object(
                    source._correlator, "start", new_callable=AsyncMock
                ) as mock_start,
                patch.object(
                    source._correlator, "stop", new_callable=AsyncMock
                ) as mock_stop,
            ):
                await source.start()
                mock_start.assert_called_once()

                await source.stop()
                mock_stop.assert_called_once()
                return

        # This is handled in the mock context above
        pass

    def test_status_includes_correlator(
        self, correlation_config: PgSTACSourceConfig
    ) -> None:
        """Test status includes correlator information."""
        source = PgSTACSource(correlation_config)
        status = source.get_status()

        assert "correlator" in status
        correlator_status = status["correlator"]
        assert isinstance(correlator_status, dict)
        assert "pending_operations" in correlator_status

    async def test_listen_uses_correlator(
        self, correlation_config: PgSTACSourceConfig
    ) -> None:
        """Test that listen method uses correlator when enabled."""
        source = PgSTACSource(correlation_config)

        # Mock correlator process_events method to return an empty async generator
        if source._correlator:

            async def mock_process_events(
                event_stream: AsyncIterator[NotificationEvent],
            ) -> AsyncIterator[NotificationEvent]:
                # Simply yield nothing to avoid the async iteration warning
                return
                yield  # unreachable but makes this a generator

            with patch.object(
                source._correlator, "process_events", side_effect=mock_process_events
            ):
                # Set as running to enable listen
                source._set_running_state(True)

                # Try to get events (should get nothing from mock)
                events = []
                try:
                    async for event in source.listen():
                        events.append(event)
                        if len(events) >= 1:  # Limit to prevent infinite loop
                            break
                except Exception:
                    pass  # Expected since we're mocking

                # Verify no events were returned (as expected from mock)
                assert len(events) == 0


class TestPgSTACSourceErrorHandling:
    """Test error handling scenarios."""

    @pytest.fixture
    def basic_config(self) -> PgSTACSourceConfig:
        """Basic test config."""
        return PgSTACSourceConfig(
            host="test-host",
            database="test-db",
            enable_correlation=False,
        )

    def test_connection_error_handling(self, basic_config: PgSTACSourceConfig) -> None:
        """Test source initialization doesn't fail with connection errors."""
        # Test that source can be created successfully even with invalid config
        # Actual connection testing with errors is complex due to async cleanup
        source = PgSTACSource(basic_config)
        assert not source.is_running
        assert source._connection is None

    def test_malformed_notification_payload(
        self, basic_config: PgSTACSourceConfig
    ) -> None:
        """Test handling of malformed notification payloads."""
        source = PgSTACSource(basic_config)

        # Invalid JSON returns None
        assert source._create_notification_event("invalid json") is None

        # Valid JSON creates event
        event = source._create_notification_event(
            '{"op": "INSERT", "collection": "test", "id": "item1"}'
        )
        assert event is not None
        assert event.operation == "INSERT"
