"""
Tests for MQTT output plugin.
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from eoapi_notifier.core.event import NotificationEvent
from eoapi_notifier.core.plugin import PluginError, PluginMetadata
from eoapi_notifier.core.registry import create_output, get_available_outputs
from eoapi_notifier.outputs.mqtt import MQTTAdapter, MQTTConfig


class TestMQTTConfig:
    """Test MQTT output configuration."""

    def test_config_implements_protocol(self) -> None:
        """Test that config implements required protocol methods."""
        config = MQTTConfig()

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
        config = MQTTConfig()

        assert config.broker_host == "localhost"
        assert config.broker_port == 1883
        assert config.username is None
        assert config.password is None
        assert config.topic == "stac/notifications"
        assert config.qos == 1
        assert config.timeout == 30.0
        assert config.keepalive == 60
        assert config.use_tls is False
        assert config.clean_session is True
        assert config.client_id is None

    def test_custom_configuration(self) -> None:
        """Test configuration with custom values."""
        config = MQTTConfig(
            broker_host="mqtt.example.com",
            broker_port=8883,
            username="testuser",
            password="testpass",
            topic="custom/topic",
            qos=2,
            timeout=60.0,
            keepalive=120,
            use_tls=True,
            clean_session=False,
            client_id="test-client-123",
        )

        assert config.broker_host == "mqtt.example.com"
        assert config.broker_port == 8883
        assert config.username == "testuser"
        assert config.password == "testpass"
        assert config.topic == "custom/topic"
        assert config.qos == 2
        assert config.timeout == 60.0
        assert config.keepalive == 120
        assert config.use_tls is True
        assert config.clean_session is False
        assert config.client_id == "test-client-123"

    def test_port_validation_error(self) -> None:
        """Test port validation."""
        # Valid ports should work
        MQTTConfig(broker_port=1)
        MQTTConfig(broker_port=65535)

        # Invalid ports should raise validation error
        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            MQTTConfig(broker_port=0)

        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            MQTTConfig(broker_port=65536)

    def test_qos_validation_error(self) -> None:
        """Test QoS validation."""
        # Valid QoS levels should work
        MQTTConfig(qos=0)
        MQTTConfig(qos=1)
        MQTTConfig(qos=2)

        # Invalid QoS levels should raise validation error
        with pytest.raises(ValueError, match="QoS must be 0, 1, or 2"):
            MQTTConfig(qos=3)

        with pytest.raises(ValueError, match="QoS must be 0, 1, or 2"):
            MQTTConfig(qos=-1)

    def test_get_sample_config(self) -> None:
        """Test sample configuration generation."""
        sample = MQTTConfig.get_sample_config()

        expected_keys = {
            "broker_host",
            "broker_port",
            "username",
            "password",
            "topic",
            "qos",
            "timeout",
            "keepalive",
            "use_tls",
            "clean_session",
            "client_id",
            "max_reconnect_attempts",
            "reconnect_delay",
            "queue_size",
        }
        assert set(sample.keys()) == expected_keys
        assert isinstance(sample["broker_port"], int)
        assert sample["broker_port"] == 1883
        assert sample["qos"] == 1

    def test_get_metadata(self) -> None:
        """Test metadata generation."""
        metadata = MQTTConfig.get_metadata()

        assert metadata.name == "mqtt"
        assert "MQTT broker output adapter" in metadata.description

        assert metadata.category == "messaging"
        assert "mqtt" in metadata.tags
        assert "messaging" in metadata.tags
        assert "broker" in metadata.tags
        assert "publish" in metadata.tags
        assert metadata.priority == 10

    def test_connection_info(self) -> None:
        """Test connection info string generation."""
        config = MQTTConfig(
            broker_host="test-broker", broker_port=1884, topic="test/topic"
        )
        info = config.get_connection_info()

        assert info == "mqtt://test-broker:1884 → test/topic"

    def test_connection_info_with_tls(self) -> None:
        """Test connection info with TLS enabled."""
        config = MQTTConfig(
            broker_host="secure-broker",
            broker_port=8883,
            topic="secure/topic",
            use_tls=True,
        )
        info = config.get_connection_info()

        assert info == "mqtts://secure-broker:8883 → secure/topic"

    def test_status_info(self) -> None:
        """Test status info generation."""
        config = MQTTConfig(
            broker_host="test-host",
            broker_port=1883,
            topic="test/topic",
            qos=2,
            use_tls=True,
            username="testuser",
            client_id="test-client",
        )
        status = config.get_status_info()

        assert status["Broker"] == "test-host:1883"
        assert status["Topic"] == "test/topic"
        assert status["QoS"] == 2
        assert status["TLS"] == "Yes"
        assert status["Username"] == "testuser"
        assert status["Client ID"] == "test-client"

    def test_status_info_defaults(self) -> None:
        """Test status info with default values."""
        config = MQTTConfig()
        status = config.get_status_info()

        assert status["TLS"] == "No"
        assert status["Username"] == "None"
        assert status["Client ID"] == "Auto-generated"


class TestMQTTAdapterRegistry:
    """Test MQTT adapter registry integration."""

    def test_mqtt_output_registered(self) -> None:
        """Test that MQTT output is registered."""
        available = get_available_outputs()
        assert "mqtt" in available

    def test_create_output_from_registry(self) -> None:
        """Test creating MQTT adapter through registry."""
        config = {
            "broker_host": "test-broker",
            "broker_port": 1883,
            "topic": "test/topic",
            "username": "test",
            "password": "test",
        }

        output = create_output("mqtt", config)

        assert isinstance(output, MQTTAdapter)
        assert isinstance(output.config, MQTTConfig)
        assert output.config.broker_host == "test-broker"
        assert output.config.username == "test"

    def test_create_output_with_validation_error(self) -> None:
        """Test output creation with invalid configuration."""
        config = {
            "broker_port": 99999,  # Invalid port
        }

        with pytest.raises(PluginError):  # Should raise validation error
            create_output("mqtt", config)


class TestMQTTAdapter:
    """Test MQTT adapter functionality."""

    @pytest.fixture
    def mock_config(self) -> MQTTConfig:
        """Create a mock configuration for testing."""
        return MQTTConfig(
            broker_host="test-broker",
            broker_port=1883,
            topic="test/topic",
            username="test-user",
            password="test-pass",
            qos=1,
            timeout=10.0,
        )

    @pytest.fixture
    def adapter(self, mock_config: MQTTConfig) -> MQTTAdapter:
        """Create an MQTT adapter for testing."""
        return MQTTAdapter(mock_config)

    @pytest.fixture
    def sample_event(self) -> NotificationEvent:
        """Create a sample notification event for testing."""
        return NotificationEvent(
            source="/test/source",
            type="test.event",
            operation="INSERT",
            collection="test-collection",
            item_id="test-item-123",
        )

    def test_adapter_initialization(
        self, adapter: MQTTAdapter, mock_config: MQTTConfig
    ) -> None:
        """Test adapter initialization."""
        assert adapter.config == mock_config
        assert not adapter.is_running
        assert not adapter.is_started
        assert adapter._client is None
        assert adapter._connected is False
        assert adapter._connection_event is None
        assert len(adapter._publish_results) == 0

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_start_and_stop_success(
        self, mock_mqtt: MagicMock, adapter: MQTTAdapter
    ) -> None:
        """Test successful adapter start and stop lifecycle."""
        # Mock MQTT client
        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client
        mock_client.is_connected.return_value = True

        # Start the adapter
        start_task = asyncio.create_task(adapter.start())

        # Simulate successful connection
        await asyncio.sleep(0.1)
        adapter._connected = True
        if adapter._connection_event:
            adapter._connection_event.set()

        await start_task

        # Verify initialization
        assert adapter.is_running
        assert adapter.is_started
        assert adapter._client == mock_client
        assert adapter._connected is True

        # Verify client setup
        mock_mqtt.Client.assert_called_once_with(
            client_id="", clean_session=True, protocol=mock_mqtt.MQTTv311
        )
        mock_client.username_pw_set.assert_called_once_with("test-user", "test-pass")
        mock_client.connect_async.assert_called_once_with("test-broker", 1883, 60)
        mock_client.loop_start.assert_called_once()

        # Test stop
        await adapter.stop()

        assert not adapter.is_running
        assert not adapter.is_started
        assert adapter._client is None
        assert adapter._connected is False

        # Verify cleanup
        mock_client.disconnect.assert_called_once()
        mock_client.loop_stop.assert_called_once()

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_start_connection_timeout(
        self, mock_mqtt: MagicMock, adapter: MQTTAdapter
    ) -> None:
        """Test start with connection timeout."""
        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client

        # Use a short timeout for testing
        adapter.config.timeout = 0.1

        # Start should complete without raising TimeoutError
        # (handles timeout in background)
        await adapter.start()

        # Verify client was created and configured
        mock_client.loop_start.assert_called()
        assert adapter._client is not None

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_start_with_tls(self, mock_mqtt: MagicMock) -> None:
        """Test adapter start with TLS configuration."""
        config = MQTTConfig(use_tls=True, timeout=0.5)
        adapter = MQTTAdapter(config)

        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client

        # Mock SSL context creation
        with patch("eoapi_notifier.outputs.mqtt.ssl") as mock_ssl:
            mock_context = MagicMock()
            mock_ssl.create_default_context.return_value = mock_context

            start_task = asyncio.create_task(adapter.start())

            # Simulate connection
            await asyncio.sleep(0.1)
            adapter._connected = True
            if adapter._connection_event:
                adapter._connection_event.set()

            await start_task

            # Verify TLS setup
            mock_ssl.create_default_context.assert_called_once()
            mock_client.tls_set_context.assert_called_once_with(mock_context)

        await adapter.stop()

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_send_event_success_qos0(
        self,
        mock_mqtt: MagicMock,
        adapter: MQTTAdapter,
        sample_event: NotificationEvent,
    ) -> None:
        """Test successful event sending with QoS 0."""
        adapter.config.qos = 0
        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client
        adapter._client = mock_client
        adapter._connected = True

        # Mock publish return
        mock_msg_info = MagicMock()
        mock_client.publish.return_value = mock_msg_info

        result = await adapter.send_event(sample_event)

        assert result is True

        # Verify publish call
        mock_client.publish.assert_called_once()
        call_args = mock_client.publish.call_args
        assert call_args[1]["topic"] == "test/topic/test-collection"
        assert call_args[1]["qos"] == 0
        assert call_args[1]["retain"] is False

        # Verify payload content
        payload_bytes = call_args[1]["payload"]
        payload_str = payload_bytes.decode("utf-8")
        payload_data = json.loads(payload_str)

        assert payload_data["id"] == sample_event.id
        assert payload_data["source"] == "/test/source"
        assert payload_data["type"] == "test.event"
        assert payload_data["operation"] == "INSERT"
        assert payload_data["collection"] == "test-collection"
        assert payload_data["item_id"] == "test-item-123"

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_send_event_success_qos1(
        self,
        mock_mqtt: MagicMock,
        adapter: MQTTAdapter,
        sample_event: NotificationEvent,
    ) -> None:
        """Test successful event sending with QoS 1."""
        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client
        adapter._client = mock_client
        adapter._connected = True

        # Mock publish return
        mock_msg_info = MagicMock()
        mock_msg_info.mid = 12345
        mock_client.publish.return_value = mock_msg_info

        # Start send_event task
        send_task = asyncio.create_task(adapter.send_event(sample_event))

        # Simulate publish callback
        await asyncio.sleep(0.1)
        adapter._on_publish(mock_client, None, 12345)

        result = await send_task

        assert result is True
        assert 12345 not in adapter._publish_results

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_send_event_timeout(
        self,
        mock_mqtt: MagicMock,
        adapter: MQTTAdapter,
        sample_event: NotificationEvent,
    ) -> None:
        """Test event sending with timeout."""
        adapter.config.timeout = 0.1  # Very short timeout
        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client
        adapter._client = mock_client
        adapter._connected = True

        # Mock publish return
        mock_msg_info = MagicMock()
        mock_msg_info.mid = 12345
        mock_client.publish.return_value = mock_msg_info

        # Send event - should timeout
        result = await adapter.send_event(sample_event)

        assert result is False
        assert 12345 not in adapter._publish_results

    async def test_send_event_not_connected(
        self, adapter: MQTTAdapter, sample_event: NotificationEvent
    ) -> None:
        """Test event sending when not connected."""
        result = await adapter.send_event(sample_event)

        assert result is False

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_send_event_no_collection(
        self, mock_mqtt: MagicMock, adapter: MQTTAdapter
    ) -> None:
        """Test event sending with empty collection (uses base topic)."""
        # Create event with empty collection
        event = NotificationEvent(
            source="/test/source",
            type="test.event",
            operation="INSERT",
            collection="",
            item_id="test-item",
        )

        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client
        adapter._client = mock_client
        adapter._connected = True
        adapter.config.qos = 0

        mock_client.publish.return_value = MagicMock()

        result = await adapter.send_event(event)

        assert result is True

        # Verify topic is base topic (empty collection is falsy)
        call_args = mock_client.publish.call_args
        assert call_args[1]["topic"] == "test/topic"

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_send_event_exception(
        self,
        mock_mqtt: MagicMock,
        adapter: MQTTAdapter,
        sample_event: NotificationEvent,
    ) -> None:
        """Test event sending with exception."""
        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client
        adapter._client = mock_client
        adapter._connected = True

        # Mock publish to raise exception
        mock_client.publish.side_effect = Exception("Publish failed")

        result = await adapter.send_event(sample_event)

        assert result is False

    def test_on_connect_success(self, adapter: MQTTAdapter) -> None:
        """Test successful connection callback."""
        adapter._connection_event = asyncio.Event()

        adapter._on_connect(None, None, {}, 0)

        assert adapter._connected is True
        assert adapter._connection_event.is_set()

    def test_on_connect_failure(self, adapter: MQTTAdapter) -> None:
        """Test failed connection callback."""
        adapter._connection_event = asyncio.Event()

        adapter._on_connect(None, None, {}, 1)  # Non-zero return code

        assert adapter._connected is False
        assert adapter._connection_event.is_set()

    def test_on_disconnect_unexpected(self, adapter: MQTTAdapter) -> None:
        """Test unexpected disconnection callback."""
        adapter._connected = True

        adapter._on_disconnect(None, None, 1)  # Non-zero indicates unexpected

        assert adapter._connected is False

    def test_on_disconnect_expected(self, adapter: MQTTAdapter) -> None:
        """Test expected disconnection callback."""
        adapter._connected = True

        adapter._on_disconnect(None, None, 0)  # Zero indicates expected

        assert adapter._connected is False

    def test_on_publish_callback(self, adapter: MQTTAdapter) -> None:
        """Test publish completion callback."""
        # Create a future for message ID
        future: asyncio.Future[bool] = asyncio.Future()
        adapter._publish_results[12345] = future

        adapter._on_publish(None, None, 12345)

        assert 12345 not in adapter._publish_results
        assert future.result() is True

    def test_on_publish_callback_unknown_mid(self, adapter: MQTTAdapter) -> None:
        """Test publish callback with unknown message ID."""
        # Should not raise exception
        adapter._on_publish(None, None, 99999)

    async def test_health_check_healthy(self, adapter: MQTTAdapter) -> None:
        """Test health check when adapter is healthy."""
        mock_client = MagicMock()
        mock_client.is_connected.return_value = True

        adapter._running = True
        adapter._client = mock_client
        adapter._connected = True

        result = await adapter.health_check()

        assert result is True
        mock_client.is_connected.assert_called_once()

    async def test_health_check_not_running(self, adapter: MQTTAdapter) -> None:
        """Test health check when adapter is not running."""
        result = await adapter.health_check()

        assert result is False

    async def test_health_check_no_client(self, adapter: MQTTAdapter) -> None:
        """Test health check with no client."""
        adapter._running = True
        adapter._client = None

        result = await adapter.health_check()

        assert result is False

    async def test_health_check_not_connected(self, adapter: MQTTAdapter) -> None:
        """Test health check when not connected."""
        adapter._running = True
        adapter._client = MagicMock()
        adapter._connected = False

        result = await adapter.health_check()

        assert result is False

    async def test_health_check_exception(self, adapter: MQTTAdapter) -> None:
        """Test health check with exception."""
        mock_client = MagicMock()
        mock_client.is_connected.side_effect = Exception("Health check failed")

        adapter._running = True
        adapter._client = mock_client
        adapter._connected = True

        result = await adapter.health_check()

        assert result is False

    @patch("eoapi_notifier.outputs.mqtt.mqtt")
    async def test_context_manager(
        self, mock_mqtt: MagicMock, adapter: MQTTAdapter
    ) -> None:
        """Test adapter as async context manager."""
        mock_client = MagicMock()
        mock_mqtt.Client.return_value = mock_client

        # Mock the start method to avoid connection complexity
        async def mock_start() -> None:
            adapter._running = True
            adapter._started = True
            await asyncio.sleep(0.01)  # Small delay to simulate work

        with patch.object(adapter, "start", side_effect=mock_start):
            async with adapter:
                assert adapter.is_running
                assert adapter.is_started

        assert not adapter.is_running
        assert not adapter.is_started

    def test_get_status(self, adapter: MQTTAdapter) -> None:
        """Test adapter status reporting."""
        status = adapter.get_status()

        assert status["name"] == "mqtt"
        assert status["type"] == "MQTTAdapter"
        assert status["running"] is False
        assert status["started"] is False
        assert "metadata" in status
        assert (
            status["metadata"]["description"]
            == "MQTT broker output adapter for notification events"
        )
