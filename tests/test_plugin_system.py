"""
Tests for the plugin system.

"""

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from pytest import fixture

if TYPE_CHECKING:
    pass

from eoapi_notifier.core.event import NotificationEvent
from eoapi_notifier.core.plugin import (
    BaseOutput,
    BasePluginConfig,
    BaseSource,
    PluginError,
    PluginLifecycleError,
    PluginMetadata,
    PluginValidationError,
)
from eoapi_notifier.core.registry import (
    ComponentRegistry,
    output_registry,
    source_registry,
)


# Mock Configuration Classes
class MockSourceConfig(BasePluginConfig):
    """Mock source configuration implementing the protocol."""

    test_param: str = "default"
    poll_interval: float = 1.0
    event_source: str = "/test/source"

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {
            "test_param": "sample",
            "poll_interval": 5.0,
            "event_source": "/test/source",
        }

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        return PluginMetadata(
            name="mock_source",
            description="Test source for unit tests",
            version="1.0.0",
            tags=["test", "mock"],
            category="source",
        )

    def get_connection_info(self) -> str:
        return f"Test connection: {self.test_param}"

    def get_status_info(self) -> dict[str, Any]:
        return {
            "Test Param": self.test_param,
            "Poll Interval": f"{self.poll_interval}s",
            "Event Source": self.event_source,
        }


class MockOutputConfig(BasePluginConfig):
    """Mock output configuration implementing the protocol."""

    output_url: str = "http://localhost:8080"
    batch_size: int = 10
    timeout: float = 5.0

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {"output_url": "http://example.com", "batch_size": 50, "timeout": 10.0}

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        return PluginMetadata(
            name="mock_output",
            description="Test output for unit tests",
            version="1.0.0",
            tags=["test", "mock"],
            category="output",
        )

    def get_connection_info(self) -> str:
        return f"Output URL: {self.output_url}"

    def get_status_info(self) -> dict[str, Any]:
        return {
            "Output URL": self.output_url,
            "Batch Size": self.batch_size,
            "Timeout": f"{self.timeout}s",
        }


# Mock Plugin Classes
class MockSource(BaseSource[MockSourceConfig]):
    """Mock source implementation."""

    def __init__(self, config: MockSourceConfig):
        super().__init__(config)
        self.events_to_yield: list[NotificationEvent] = []
        self.start_called = False
        self.stop_called = False

    async def start(self) -> None:
        """Start the mock source."""
        await super().start()
        self.start_called = True

    async def stop(self) -> None:
        """Stop the mock source."""
        await super().stop()
        self.stop_called = True

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        """Listen for events from the source."""
        for event in self.events_to_yield:
            yield event
            await asyncio.sleep(0.01)  # Small delay to prevent tight loop

    def add_test_event(self, event: NotificationEvent) -> None:
        """Add an event for testing."""
        self.events_to_yield.append(event)


class MockOutput(BaseOutput[MockOutputConfig]):
    """Mock output implementation."""

    def __init__(self, config: MockOutputConfig):
        super().__init__(config)
        self.sent_events: list[NotificationEvent] = []
        self.start_called = False
        self.stop_called = False
        self.should_fail = False

    async def start(self) -> None:
        """Start the mock output."""
        await super().start()
        self.start_called = True

    async def stop(self) -> None:
        """Stop the mock output."""
        await super().stop()
        self.stop_called = True

    async def send_event(self, event: NotificationEvent) -> bool:
        """Send a single event."""
        if self.should_fail:
            raise ConnectionError("Mock connection failure")

        self.sent_events.append(event)
        return True

    def set_should_fail(self, should_fail: bool) -> None:
        """Control whether send_event should fail."""
        self.should_fail = should_fail


class BrokenConfig(BasePluginConfig):
    """Config class missing required protocol methods."""

    pass  # Missing protocol method implementations


# Test Factory Fixtures
class PluginFactory:
    """Factory for creating test plugins and configurations."""

    @staticmethod
    def create_source_config(**kwargs: Any) -> MockSourceConfig:
        """Create a source config with optional overrides."""
        defaults = {
            "test_param": "factory_test",
            "poll_interval": 1.0,
            "event_source": "/factory/source",
        }
        defaults.update(kwargs)
        return MockSourceConfig(**defaults)  # type: ignore[arg-type]

    @staticmethod
    def create_output_config(**kwargs: Any) -> MockOutputConfig:
        """Create an output config with optional overrides."""
        defaults = {
            "output_url": "http://factory.test",
            "batch_size": 20,
            "timeout": 5.0,
        }
        defaults.update(kwargs)
        return MockOutputConfig(**defaults)  # type: ignore[arg-type]

    @staticmethod
    def create_source(config: MockSourceConfig | None = None) -> MockSource:
        """Create a source instance."""
        if config is None:
            config = PluginFactory.create_source_config()
        return MockSource(config)

    @staticmethod
    def create_output(config: MockOutputConfig | None = None) -> MockOutput:
        """Create an output instance."""
        if config is None:
            config = PluginFactory.create_output_config()
        return MockOutput(config)

    @staticmethod
    def create_event(**kwargs: Any) -> NotificationEvent:
        """Create a notification event with optional overrides."""
        defaults = {
            "source": "/factory/source",
            "type": "org.factory.test",
            "operation": "INSERT",
            "collection": "test",
            "item_id": "test-item",
            "data": {"test": True, "factory_created": True},
        }
        defaults.update(kwargs)
        return NotificationEvent(**defaults)  # type: ignore[arg-type]


@fixture
def plugin_factory() -> PluginFactory:
    """Provide plugin factory for tests."""
    return PluginFactory()


@fixture
def test_event() -> NotificationEvent:
    """Provide a standard test event."""
    return PluginFactory.create_event()


# Configuration Protocol Tests
class TestPluginConfigProtocol:
    """Test plugin configuration protocol compliance."""

    def test_mock_configs_implement_protocol(
        self, plugin_factory: PluginFactory
    ) -> None:
        """Test that mock configs implement the protocol correctly."""
        source_config = plugin_factory.create_source_config()
        output_config = plugin_factory.create_output_config()

        # Test protocol methods exist and work
        assert callable(source_config.get_sample_config)
        assert callable(source_config.get_metadata)
        assert callable(source_config.get_connection_info)
        assert callable(source_config.get_status_info)

        assert callable(output_config.get_sample_config)
        assert callable(output_config.get_metadata)
        assert callable(output_config.get_connection_info)
        assert callable(output_config.get_status_info)

    def test_broken_config_missing_protocol_methods(self) -> None:
        """Test that broken config is detected."""
        # This should still work as BasePluginConfig provides default implementations
        config = BrokenConfig()
        assert config.get_connection_info() == "Connection: BrokenConfig"


# Enhanced Plugin Lifecycle Tests
class TestPluginLifecycle:
    """Test enhanced plugin lifecycle management."""

    @pytest.mark.parametrize(
        "plugin_type,factory_method",
        [
            ("source", "create_source"),
            ("output", "create_output"),
        ],
    )
    def test_plugin_initialization(
        self, plugin_factory: PluginFactory, plugin_type: str, factory_method: str
    ) -> None:
        """Test plugin initialization for both sources and outputs."""
        plugin = getattr(plugin_factory, factory_method)()

        assert plugin.config is not None
        assert plugin.logger is not None
        assert not plugin.is_running
        assert not plugin.is_started
        assert plugin_type in plugin.config.get_metadata().category

    @pytest.mark.parametrize(
        "plugin_type,factory_method",
        [
            ("source", "create_source"),
            ("output", "create_output"),
        ],
    )
    @pytest.mark.asyncio
    async def test_plugin_lifecycle_methods(
        self, plugin_factory: PluginFactory, plugin_type: str, factory_method: str
    ) -> None:
        """Test start/stop lifecycle for both plugin types."""
        plugin = getattr(plugin_factory, factory_method)()

        # Initial state
        assert not plugin.is_running
        assert not plugin.is_started

        # Start plugin
        await plugin.start()
        assert plugin.is_running
        assert plugin.is_started
        assert plugin.start_called

        # Stop plugin
        await plugin.stop()
        assert not plugin.is_running
        assert not plugin.is_started
        assert plugin.stop_called

    @pytest.mark.asyncio
    async def test_plugin_restart(self, plugin_factory: PluginFactory) -> None:
        """Test plugin restart functionality."""
        plugin = plugin_factory.create_source()

        await plugin.start()
        assert plugin.is_running

        await plugin.restart()
        assert plugin.is_running
        assert plugin.start_called
        assert plugin.stop_called

    @pytest.mark.asyncio
    async def test_double_start_raises_error(
        self, plugin_factory: PluginFactory
    ) -> None:
        """Test that starting an already started plugin raises an error."""
        plugin = plugin_factory.create_source()

        await plugin.start()
        with pytest.raises(RuntimeError, match="already started"):
            await plugin.start()

    @pytest.mark.asyncio
    async def test_stop_never_started_plugin(
        self, plugin_factory: PluginFactory
    ) -> None:
        """Test stopping a never-started plugin doesn't raise error."""
        plugin = plugin_factory.create_source()

        # Should not raise
        await plugin.stop()
        assert not plugin.is_running


# Async Context Manager Tests
class TestAsyncContextManagers:
    """Test async context manager functionality."""

    @pytest.mark.asyncio
    async def test_plugin_as_context_manager(
        self, plugin_factory: PluginFactory
    ) -> None:
        """Test plugin as async context manager."""
        plugin = plugin_factory.create_source()

        async with plugin as ctx_plugin:
            assert ctx_plugin is plugin
            assert plugin.is_running
            assert plugin.start_called

        assert not plugin.is_running
        assert plugin.stop_called

    @pytest.mark.asyncio
    async def test_source_event_stream_context(
        self, plugin_factory: PluginFactory, test_event: NotificationEvent
    ) -> None:
        """Test source event stream context manager."""
        source = plugin_factory.create_source()
        source.add_test_event(test_event)

        events_received = []
        async with source.event_stream() as event_stream:
            assert source.is_running
            async for event in event_stream:
                events_received.append(event)
                break  # Just test one event

        assert len(events_received) == 1
        assert events_received[0] == test_event
        assert not source.is_running


# Enhanced Output Tests
class TestEnhancedOutput:
    """Test enhanced output functionality."""

    @pytest.mark.asyncio
    async def test_send_with_retry_success(
        self, plugin_factory: PluginFactory, test_event: NotificationEvent
    ) -> None:
        """Test successful send with retry."""
        output = plugin_factory.create_output()
        await output.start()

        result = await output.send_with_retry(test_event, max_retries=3)
        assert result is True
        assert len(output.sent_events) == 1

        await output.stop()

    @pytest.mark.asyncio
    async def test_send_with_retry_failure(
        self, plugin_factory: PluginFactory, test_event: NotificationEvent
    ) -> None:
        """Test send with retry after failures."""
        output = plugin_factory.create_output()
        output.set_should_fail(True)
        await output.start()

        result = await output.send_with_retry(
            test_event, max_retries=2, backoff_factor=0.001
        )
        assert result is False
        assert len(output.sent_events) == 0

        await output.stop()

    @pytest.mark.asyncio
    async def test_send_batch_with_errors(self, plugin_factory: PluginFactory) -> None:
        """Test batch sending with some errors."""
        output = plugin_factory.create_output()
        await output.start()

        events = [
            plugin_factory.create_event(data={"id": i}, item_id=f"item-{i}")
            for i in range(5)
        ]

        # Mock send_event to fail on specific events

        async def selective_send(event: NotificationEvent) -> bool:
            if event.data.get("id") == 2:
                raise ConnectionError("Selective failure")
            return True

        with patch.object(output, "send_event", side_effect=selective_send):
            success_count = await output.send_batch(events)
            assert success_count == 4  # All except the failing one

        await output.stop()


# Validation Chain Tests
# Simple Registry Tests
class TestRegistryIntegration:
    """Test registry integration with plugin system."""

    @pytest.mark.parametrize(
        "registry,plugin_type",
        [
            (source_registry, "postgres"),
            (output_registry, "mqtt"),
        ],
    )
    def test_registry_built_in_plugins(
        self, registry: ComponentRegistry[Any, Any], plugin_type: str
    ) -> None:
        """Test built-in plugin registration."""
        assert registry.is_registered(plugin_type)
        assert plugin_type in registry.list_registered()


# Error Handling Tests
class TestEnhancedErrorHandling:
    """Test enhanced error handling."""

    def test_plugin_error_with_metadata(self) -> None:
        """Test plugin error with metadata."""
        metadata = {"config_key": "value", "attempt": 1}
        error = PluginError("test_plugin", "Something went wrong", metadata=metadata)

        assert error.plugin_name == "test_plugin"
        assert error.metadata == metadata
        assert "test_plugin" in str(error)

    def test_plugin_validation_error(self) -> None:
        """Test plugin validation error."""
        error = PluginValidationError("test_plugin", "Invalid config")
        assert isinstance(error, PluginError)
        assert "Invalid config" in str(error)

    def test_plugin_lifecycle_error(self) -> None:
        """Test plugin lifecycle error."""
        error = PluginLifecycleError("test_plugin", "Failed to start")
        assert isinstance(error, PluginError)
        assert "Failed to start" in str(error)


# Integration Tests
class TestSystemIntegration:
    """Test system integration scenarios."""

    @pytest.mark.asyncio
    async def test_end_to_end_plugin_usage(
        self, plugin_factory: PluginFactory, test_event: NotificationEvent
    ) -> None:
        """Test end-to-end plugin usage with context managers."""
        source = plugin_factory.create_source()
        output = plugin_factory.create_output()

        source.add_test_event(test_event)

        # Use both plugins in context managers
        async with source, output:
            assert source.is_running
            assert output.is_running

            # Process events from source to output
            async for event in source.listen():
                success = await output.send_event(event)
                assert success
                break

        # Both should be stopped
        assert not source.is_running
        assert not output.is_running

        # Event should have been processed
        assert len(output.sent_events) == 1
        assert output.sent_events[0] == test_event

    @pytest.mark.asyncio
    async def test_plugin_status_reporting(self, plugin_factory: PluginFactory) -> None:
        """Test comprehensive plugin status reporting."""
        plugin = plugin_factory.create_source()

        # Test status when stopped
        status = plugin.get_status()
        assert status["running"] is False
        assert status["started"] is False
        assert status["name"] == "mock_source"
        assert status["metadata"]["description"] == "Test source for unit tests"
        assert "test" in status["metadata"]["tags"]

        # Test status when running
        async with plugin:
            status = plugin.get_status()
            assert status["running"] is True
            assert status["started"] is True
