"""
Tests for CloudEvents output plugin.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cloudevents.http import CloudEvent

from eoapi_notifier.core.event import NotificationEvent
from eoapi_notifier.core.plugin import PluginMetadata
from eoapi_notifier.outputs.cloudevents import CloudEventsAdapter, CloudEventsConfig


class TestCloudEventsConfig:
    """Test CloudEvents output configuration."""

    def test_config_implements_protocol(self) -> None:
        """Test that config implements required protocol methods."""
        config = CloudEventsConfig()

        assert isinstance(config.get_sample_config(), dict)
        assert isinstance(config.get_metadata(), PluginMetadata)
        assert isinstance(config.get_connection_info(), str)
        assert isinstance(config.get_status_info(), dict)

    def test_default_configuration(self) -> None:
        """Test default configuration values."""
        config = CloudEventsConfig()

        assert config.endpoint is None
        assert config.source == "/eoapi/stac"
        assert config.event_type == "org.eoapi.stac"
        assert config.timeout == 30.0
        assert config.max_retries == 3

    def test_endpoint_validation_error(self) -> None:
        """Test endpoint validation."""
        with pytest.raises(ValueError, match="must start with http"):
            CloudEventsConfig(endpoint="invalid-url")

    def test_get_metadata(self) -> None:
        """Test metadata retrieval."""
        metadata = CloudEventsConfig.get_metadata()

        assert metadata.name == "cloudevents"
        assert "CloudEvents" in metadata.description
        assert "http" in metadata.tags

    def test_connection_info(self) -> None:
        """Test connection info string."""
        config = CloudEventsConfig(endpoint="https://example.com/webhook")
        assert "POST https://example.com/webhook" in config.get_connection_info()


class TestCloudEventsAdapter:
    """Test CloudEvents output adapter."""

    @pytest.fixture
    def config(self) -> CloudEventsConfig:
        """Create test configuration."""
        return CloudEventsConfig(endpoint="https://example.com/webhook")

    @pytest.fixture
    def adapter(self, config: CloudEventsConfig) -> CloudEventsAdapter:
        """Create test adapter."""
        return CloudEventsAdapter(config)

    @pytest.fixture
    def sample_event(self) -> NotificationEvent:
        """Create sample notification event."""
        return NotificationEvent(
            source="/test/source",
            type="test.type",
            operation="INSERT",
            collection="test-collection",
            item_id="test-item",
        )

    async def test_start_success(self, adapter: CloudEventsAdapter) -> None:
        """Test successful adapter start."""
        with patch("httpx.AsyncClient") as mock_client:
            await adapter.start()

            assert adapter.is_running
            assert adapter._client is not None
            mock_client.assert_called_once()

    async def test_start_no_endpoint(self) -> None:
        """Test start failure without endpoint."""
        config = CloudEventsConfig()
        adapter = CloudEventsAdapter(config)

        with pytest.raises(ValueError, match="endpoint configuration required"):
            await adapter.start()

    @patch.dict(os.environ, {"K_SINK": "https://k8s.example.com"})
    async def test_start_with_k_sink_env(self) -> None:
        """Test start with K_SINK environment variable."""
        config = CloudEventsConfig()
        adapter = CloudEventsAdapter(config)

        with patch("httpx.AsyncClient"):
            await adapter.start()
            assert adapter.is_running

    async def test_stop(self, adapter: CloudEventsAdapter) -> None:
        """Test adapter stop."""
        mock_client = AsyncMock()
        adapter._client = mock_client

        await adapter.stop()

        mock_client.aclose.assert_called_once()
        assert adapter._client is None

    async def test_send_event_success(
        self, adapter: CloudEventsAdapter, sample_event: NotificationEvent
    ) -> None:
        """Test successful event sending."""
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_client.post.return_value = mock_response
        adapter._client = mock_client
        adapter._running = True

        with patch("eoapi_notifier.outputs.cloudevents.to_binary") as mock_to_binary:
            mock_to_binary.return_value = ({"ce-id": "test"}, b"data")

            result = await adapter.send_event(sample_event)

            assert result is True
            mock_client.post.assert_called_once()
            mock_response.raise_for_status.assert_called_once()

    async def test_send_event_no_client(
        self, adapter: CloudEventsAdapter, sample_event: NotificationEvent
    ) -> None:
        """Test sending event without client."""
        result = await adapter.send_event(sample_event)
        assert result is False

    async def test_send_event_timeout_error(
        self, adapter: CloudEventsAdapter, sample_event: NotificationEvent
    ) -> None:
        """Test sending event with timeout error."""
        import httpx

        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.TimeoutException("Request timeout")
        adapter._client = mock_client
        adapter._running = True

        result = await adapter.send_event(sample_event)
        assert result is False

    async def test_send_event_http_status_error(
        self, adapter: CloudEventsAdapter, sample_event: NotificationEvent
    ) -> None:
        """Test sending event with HTTP status error."""
        import httpx

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.reason_phrase = "Internal Server Error"
        mock_client.post.side_effect = httpx.HTTPStatusError(
            "Server error", request=MagicMock(), response=mock_response
        )
        adapter._client = mock_client
        adapter._running = True

        result = await adapter.send_event(sample_event)
        assert result is False

    async def test_send_event_http_error(
        self, adapter: CloudEventsAdapter, sample_event: NotificationEvent
    ) -> None:
        """Test sending event with HTTP error."""
        mock_client = AsyncMock()
        mock_client.post.side_effect = Exception("HTTP error")
        adapter._client = mock_client
        adapter._running = True

        result = await adapter.send_event(sample_event)
        assert result is False

    def test_convert_to_cloudevent(
        self, adapter: CloudEventsAdapter, sample_event: NotificationEvent
    ) -> None:
        """Test NotificationEvent to CloudEvent conversion."""
        cloud_event = adapter._convert_to_cloudevent(sample_event)

        assert isinstance(cloud_event, CloudEvent)
        assert cloud_event["source"] == "/eoapi/stac"
        assert cloud_event["type"] == "org.eoapi.stac.created"
        assert cloud_event["subject"] == "test-item"
        assert cloud_event["collection"] == "test-collection"

    @patch.dict(
        os.environ,
        {
            "K_SOURCE": "/custom/source",
            "K_TYPE": "custom.type",
        },
    )
    def test_convert_with_env_vars(self, sample_event: NotificationEvent) -> None:
        """Test CloudEvent conversion with environment variables."""
        # Create a new config and adapter after setting environment variables
        config = CloudEventsConfig()
        adapter = CloudEventsAdapter(config)
        cloud_event = adapter._convert_to_cloudevent(sample_event)

        assert cloud_event["source"] == "/custom/source"
        assert cloud_event["type"] == "custom.type.created"

    def test_operation_mapping(self, adapter: CloudEventsAdapter) -> None:
        """Test operation to event type mapping."""
        test_cases = [
            ("INSERT", "created"),
            ("UPDATE", "updated"),
            ("DELETE", "deleted"),
            ("UNKNOWN", "unknown"),
        ]

        for operation, expected in test_cases:
            event = NotificationEvent(
                source="/test",
                type="test",
                operation=operation,
                collection="test",
            )
            cloud_event = adapter._convert_to_cloudevent(event)
            assert cloud_event["type"].endswith(f".{expected}")

    async def test_health_check(self, adapter: CloudEventsAdapter) -> None:
        """Test health check."""
        # Not running
        assert await adapter.health_check() is False

        # Running but no client
        adapter._running = True
        assert await adapter.health_check() is False

        # Running with client
        adapter._client = MagicMock()
        assert await adapter.health_check() is True
