"""
MQTT output adapter.

Sends notification events to MQTT broker as JSON messages using paho-mqtt
with asyncio integration.
"""

import asyncio
import json
import ssl
from typing import Any

import paho.mqtt.client as mqtt
from pydantic import field_validator

from ..core.event import NotificationEvent
from ..core.plugin import BaseOutput, BasePluginConfig, PluginMetadata


class MQTTConfig(BasePluginConfig):
    """Configuration for MQTT output adapter."""

    broker_host: str = "localhost"
    broker_port: int = 1883
    username: str | None = None
    password: str | None = None
    topic: str = "stac/notifications"
    qos: int = 1
    timeout: float = 30.0
    keepalive: int = 60
    use_tls: bool = False
    clean_session: bool = True
    client_id: str | None = None

    # Reconnection settings
    max_reconnect_attempts: int = -1  # -1 for infinite
    reconnect_delay: float = 5.0
    reconnect_backoff_factor: float = 2.0
    max_reconnect_delay: float = 60.0

    # Message queuing
    queue_size: int = 1000

    @field_validator("broker_port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("qos")
    @classmethod
    def validate_qos(cls, v: int) -> int:
        if v not in (0, 1, 2):
            raise ValueError("QoS must be 0, 1, or 2")
        return v

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        """Get sample configuration for this output."""
        return {
            "broker_host": "localhost",
            "broker_port": 1883,
            "username": None,
            "password": None,
            "topic": "stac/notifications",
            "qos": 1,
            "timeout": 30.0,
            "keepalive": 60,
            "use_tls": False,
            "clean_session": True,
            "client_id": None,
            "max_reconnect_attempts": -1,
            "reconnect_delay": 5.0,
            "queue_size": 1000,
        }

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        """Get structured metadata for this output type."""
        return PluginMetadata(
            name="mqtt",
            description="MQTT broker output adapter for notification events",
            category="messaging",
            tags=["mqtt", "messaging", "broker", "publish"],
            priority=10,
        )

    def get_connection_info(self) -> str:
        """Get connection info string for display."""
        protocol = "mqtts" if self.use_tls else "mqtt"
        return f"{protocol}://{self.broker_host}:{self.broker_port} → {self.topic}"

    def get_status_info(self) -> dict[str, Any]:
        """Get status information for display."""
        return {
            "Broker": f"{self.broker_host}:{self.broker_port}",
            "Topic": self.topic,
            "QoS": self.qos,
            "TLS": "Yes" if self.use_tls else "No",
            "Username": self.username if self.username else "None",
            "Client ID": self.client_id if self.client_id else "Auto-generated",
        }


class MQTTAdapter(BaseOutput):
    """
    MQTT output adapter using paho-mqtt with asyncio integration.

    Sends notification events to MQTT broker as JSON messages.
    Events are published to a configurable topic with QoS settings.
    """

    def __init__(self, config: MQTTConfig) -> None:
        """Initialize MQTT adapter."""
        super().__init__(config)
        self.config: MQTTConfig = config
        self._client: mqtt.Client | None = None
        self._connected: bool = False
        self._connection_event: asyncio.Event | None = None
        self._publish_results: dict[int, asyncio.Future] = {}
        self._reconnect_attempts: int = 0
        self._current_delay: float = config.reconnect_delay
        self._message_queue: asyncio.Queue | None = None

    async def start(self) -> None:
        """Start the MQTT client."""
        self.logger.info(f"Starting MQTT adapter: {self.config.get_connection_info()}")
        self.logger.debug(
            f"MQTT config: QoS={self.config.qos}, timeout={self.config.timeout}s, "
            f"keepalive={self.config.keepalive}s"
        )
        self.logger.debug(
            f"MQTT broker: {self.config.broker_host}:{self.config.broker_port}, "
            f"TLS={self.config.use_tls}"
        )

        # Create connection event and message queue
        self.logger.debug("Creating connection event and message queue...")
        self._connection_event = asyncio.Event()
        self._message_queue = asyncio.Queue(maxsize=self.config.queue_size)
        self.logger.debug(
            f"✓ Message queue created with max size: {self.config.queue_size}"
        )

        # Create MQTT client
        client_id = self.config.client_id or ""
        self.logger.debug(
            f"Creating MQTT client with ID: '{client_id}' "
            f"(clean_session={self.config.clean_session})"
        )
        self._client = mqtt.Client(
            client_id=client_id,
            clean_session=self.config.clean_session,
            protocol=mqtt.MQTTv311,
        )
        self.logger.debug("✓ MQTT client created")

        # Set up callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_publish = self._on_publish

        # Configure authentication
        if self.config.username and self.config.password:
            self.logger.debug(
                f"Setting authentication for user: {self.config.username}"
            )
            self._client.username_pw_set(self.config.username, self.config.password)
        else:
            self.logger.debug("No authentication configured")

        # Configure TLS
        if self.config.use_tls:
            self.logger.debug("Configuring TLS connection")
            tls_context = ssl.create_default_context()
            self._client.tls_set_context(tls_context)
        else:
            self.logger.debug("Using plain text connection (no TLS)")

        # Set keepalive
        self._client.keepalive = self.config.keepalive

        # Connect to broker
        try:
            self.logger.debug(
                f"Step 1: Attempting connection to MQTT broker: "
                f"{self.config.broker_host}:{self.config.broker_port}"
            )
            self._client.connect_async(
                self.config.broker_host,
                self.config.broker_port,
                self.config.keepalive,
            )
            self.logger.debug("Step 2: Starting MQTT client network loop...")
            self._client.loop_start()
            self.logger.debug("Step 3: MQTT client loop started successfully")

            # Call parent start method immediately (don't wait for connection)
            self.logger.debug("Step 4: Calling parent start method...")
            await super().start()
            self.logger.debug("Step 5: Parent start method completed")

            # Start background task for processing queued messages
            self.logger.debug("Step 6: Starting background message queue processor...")
            asyncio.create_task(self._process_message_queue())

            # Start background task to monitor initial connection
            self.logger.debug("Step 7: Starting background connection monitor...")
            asyncio.create_task(self._monitor_initial_connection())

            self.logger.info(
                f"✓ MQTT adapter started successfully (connecting in background), "
                f"will publish to topic: {self.config.topic}"
            )

        except Exception as e:
            self.logger.error(f"✗ Failed to start MQTT adapter: {e}", exc_info=True)
            if self._client:
                self.logger.debug("Cleaning up failed MQTT client...")
                self._client.loop_stop()
                self._client = None
            raise

    async def stop(self) -> None:
        """Stop the MQTT client."""
        if self._client:
            try:
                # Disconnect from broker
                self._client.disconnect()
                self._client.loop_stop()

                # Cancel any pending publish operations
                for future in self._publish_results.values():
                    if not future.done():
                        future.cancel()
                self._publish_results.clear()

            except Exception as e:
                self.logger.warning(f"Error disconnecting from MQTT broker: {e}")
            finally:
                self._client = None
                self._connected = False
                self._connection_event = None
                self._message_queue = None

        # Call parent stop method
        await super().stop()
        self.logger.info("MQTT adapter stopped")

    def _on_connect(
        self, client: mqtt.Client, userdata: Any, flags: dict, rc: int
    ) -> None:
        """Callback for MQTT connection."""
        self.logger.debug(
            f"🔗 MQTT connection callback received: rc={rc}, flags={flags}"
        )
        if rc == 0:
            self._connected = True
            self._reconnect_attempts = 0
            self._current_delay = self.config.reconnect_delay
            self.logger.info(
                f"✅ Successfully connected to MQTT broker "
                f"{self.config.broker_host}:{self.config.broker_port}"
            )
        else:
            self._connected = False
            self.logger.error(
                f"❌ Failed to connect to MQTT broker "
                f"{self.config.broker_host}:{self.config.broker_port}: "
                f"{mqtt.connack_string(rc)} (code: {rc})"
            )

        if self._connection_event:
            self.logger.debug("Setting connection event to notify waiting tasks")
            self._connection_event.set()
        else:
            self.logger.warning(
                "Connection event is None - cannot notify waiting tasks"
            )

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int) -> None:
        """Callback for MQTT disconnection."""
        self.logger.debug(f"MQTT disconnection callback: rc={rc}")
        self._connected = False
        if rc != 0:
            self.logger.warning(
                f"⚠ Unexpected MQTT disconnection: {mqtt.error_string(rc)} (code: {rc})"
            )
            # Start reconnection task
            if self._running:
                self.logger.debug("Scheduling reconnection attempt...")
                asyncio.create_task(self._reconnect_if_needed())
        else:
            self.logger.info("✓ MQTT client disconnected cleanly")

    def _on_publish(self, client: mqtt.Client, userdata: Any, mid: int) -> None:
        """Callback for MQTT publish completion."""
        self.logger.debug(f"MQTT publish acknowledgment received: mid={mid}")
        if mid in self._publish_results:
            future = self._publish_results.pop(mid)
            if not future.done():
                future.set_result(True)
                self.logger.debug(f"✓ Publish confirmed for message ID: {mid}")
        else:
            self.logger.warning(f"Received publish ack for unknown message ID: {mid}")

    async def send_event(self, event: NotificationEvent) -> bool:
        """
        Send a notification event to MQTT broker.

        Args:
            event: Event to send

        Returns:
            True if sent successfully, False otherwise
        """
        if not self._client or not self._connected:
            self.logger.warning(
                f"📤 MQTT not connected (client={self._client is not None}, "
                f"connected={self._connected})"
            )
            # Queue message for later delivery
            if self._message_queue:
                try:
                    await self._message_queue.put(event)
                    self.logger.info(
                        f"Queued event {event.id} for later delivery "
                        f"(queue size: {self._message_queue.qsize()})"
                    )
                    return True
                except asyncio.QueueFull:
                    self.logger.error(
                        f"Message queue full ({self.config.queue_size} items), "
                        f"dropping event {event.id}"
                    )
            return False

        try:
            # Convert event to JSON payload
            payload = {
                "id": event.id,
                "source": event.source,
                "type": event.type,
                "operation": event.operation,
                "collection": event.collection,
                "item_id": event.item_id,
                "timestamp": event.timestamp.isoformat(),
                "data": event.data,
            }

            # Determine topic (could be collection-specific)
            topic = (
                f"{self.config.topic}/{event.collection}"
                if event.collection
                else self.config.topic
            )

            # Publish to MQTT
            message = json.dumps(payload, default=str)
            self.logger.debug(
                f"Publishing event {event.id} to topic '{topic}' "
                f"(QoS {self.config.qos})"
            )
            self.logger.debug(f"Message payload: {message}")
            msg_info = self._client.publish(
                topic=topic,
                payload=message.encode("utf-8"),
                qos=self.config.qos,
                retain=False,
            )
            self.logger.debug(f"Publish initiated with message ID: {msg_info.mid}")

            # For QoS 0, return immediately
            if self.config.qos == 0:
                self.logger.debug(
                    f"✓ Published event {event.id} to {topic} "
                    f"(QoS 0 - no acknowledgment)"
                )
                return True

            # For QoS 1 and 2, wait for acknowledgment
            future: asyncio.Future[bool] = asyncio.Future()
            self._publish_results[msg_info.mid] = future

            try:
                # Wait for publish acknowledgment with timeout
                result = await asyncio.wait_for(future, timeout=self.config.timeout)
                self.logger.debug(
                    f"✓ Successfully published event {event.id} to {topic} "
                    f"(acknowledged)"
                )
                return result
            except TimeoutError:
                self.logger.error(
                    f"✗ Timeout publishing event {event.id} to MQTT "
                    f"(waited {self.config.timeout}s)"
                )
                self._publish_results.pop(msg_info.mid, None)
                return False

        except Exception as e:
            self.logger.error(
                f"✗ Error publishing event {event.id} to MQTT: {e}", exc_info=True
            )
            # Queue message for retry if still running
            if self._running and self._message_queue:
                try:
                    await self._message_queue.put(event)
                    self.logger.info(
                        f"Queued failed event {event.id} for retry "
                        f"(queue size: {self._message_queue.qsize()})"
                    )
                except asyncio.QueueFull:
                    self.logger.error(
                        f"Queue full ({self.config.queue_size} items), "
                        f"dropping failed event {event.id}"
                    )
            return False

    async def _process_message_queue(self) -> None:
        """Process queued messages when connection is restored."""
        self.logger.debug("Message queue processor started")
        processed_count = 0

        while self._running:
            try:
                if not self._connected or not self._message_queue:
                    if not self._connected:
                        self.logger.debug(
                            "Waiting for MQTT connection to process queue..."
                        )
                    await asyncio.sleep(1.0)
                    continue

                # Process queued messages
                try:
                    event = await asyncio.wait_for(
                        self._message_queue.get(), timeout=1.0
                    )
                    processed_count += 1
                    self.logger.debug(
                        f"Processing queued event #{processed_count}: {event.id}"
                    )
                    if self._connected:
                        success = await self.send_event(event)
                        if success:
                            self.logger.debug(
                                f"✓ Successfully processed queued event {event.id}"
                            )
                        else:
                            self.logger.warning(
                                f"✗ Failed to process queued event {event.id}"
                            )
                    else:
                        # Put back in queue if disconnected
                        self.logger.debug(
                            f"Connection lost, re-queueing event {event.id}"
                        )
                        await self._message_queue.put(event)
                except TimeoutError:
                    continue

            except Exception as e:
                self.logger.error(f"Error processing message queue: {e}", exc_info=True)
                await asyncio.sleep(1.0)

        self.logger.debug(
            f"Message queue processor stopped (processed {processed_count} events)"
        )

    async def _monitor_initial_connection(self) -> None:
        """Monitor initial MQTT connection attempt."""
        self.logger.debug("🔍 Starting MQTT connection monitoring task...")

        # Store local reference to avoid race condition with stop()
        connection_event = self._connection_event

        try:
            # Check if connection event is available
            if connection_event is None:
                self.logger.warning(
                    "⚠️  Connection event is None, cannot monitor connection"
                )
                return

            self.logger.debug(
                f"⏳ Waiting up to {self.config.timeout}s for MQTT connection..."
            )
            # Wait for connection with timeout
            await asyncio.wait_for(connection_event.wait(), timeout=self.config.timeout)
            if self._connected:
                self.logger.info(
                    "✅ Initial MQTT connection monitoring completed - "
                    "connection successful!"
                )
            else:
                self.logger.warning(
                    "⚠️  Connection event was set but _connected=False, "
                    "starting retry task"
                )
                asyncio.create_task(self._background_connection_retry())
        except TimeoutError:
            self.logger.warning(
                f"⏰ Initial MQTT connection timeout after {self.config.timeout}s, "
                f"starting background retry task"
            )
            asyncio.create_task(self._background_connection_retry())
        except Exception as e:
            self.logger.error(
                f"💥 Error during initial MQTT connection monitoring: {e}",
                exc_info=True,
            )
            asyncio.create_task(self._background_connection_retry())

    async def _background_connection_retry(self) -> None:
        """Background task to retry MQTT connection."""
        self.logger.debug("Starting background MQTT connection retry...")

        while self._running and not self._connected:
            try:
                await asyncio.sleep(self._current_delay)

                if not self._running:
                    break

                self.logger.info(
                    f"Attempting MQTT reconnection "
                    f"(attempt {self._reconnect_attempts + 1})..."
                )

                # Reset connection event
                self._connection_event = asyncio.Event()

                # Try to reconnect
                if self._client:
                    self._client.reconnect()

                    # Wait for connection with timeout
                    try:
                        if self._connection_event:
                            await asyncio.wait_for(
                                self._connection_event.wait(),
                                timeout=self.config.timeout,
                            )
                        if self._connected:
                            self.logger.info(
                                "✓ Background MQTT reconnection successful"
                            )
                            break
                    except TimeoutError:
                        self.logger.warning(
                            "Background MQTT reconnection attempt timed out"
                        )

            except Exception as e:
                self.logger.error(
                    f"Background MQTT reconnection failed: {e}", exc_info=True
                )

                # Exponential backoff
                self._reconnect_attempts += 1
                self._current_delay = min(
                    self._current_delay * self.config.reconnect_backoff_factor,
                    self.config.max_reconnect_delay,
                )

        self.logger.info("Background MQTT connection retry task ended")

    async def _reconnect_if_needed(self) -> None:
        """Attempt reconnection with exponential backoff."""
        if not self._running or not self._should_reconnect():
            self.logger.debug("Reconnection not needed or disabled")
            return

        self.logger.info(
            f"⏳ Reconnecting in {self._current_delay:.1f}s "
            f"(attempt {self._reconnect_attempts + 1})"
        )
        await asyncio.sleep(self._current_delay)

        self._reconnect_attempts += 1

        try:
            if self._client:
                self._client.reconnect()
                # Wait for connection
                if self._connection_event:
                    await asyncio.wait_for(
                        self._connection_event.wait(), timeout=self.config.timeout
                    )
                if self._connected:
                    self.logger.info("✓ Reconnection successful")
                    return

        except Exception as e:
            self.logger.error(f"✗ Reconnection failed: {e}", exc_info=True)

        # Exponential backoff
        self._current_delay = min(
            self._current_delay * self.config.reconnect_backoff_factor,
            self.config.max_reconnect_delay,
        )

        # Schedule next reconnection attempt
        if self._should_reconnect():
            asyncio.create_task(self._reconnect_if_needed())

    def _should_reconnect(self) -> bool:
        """Check if we should attempt to reconnect."""
        if not self._running:
            return False

        if self.config.max_reconnect_attempts == -1:
            return True

        return self._reconnect_attempts < self.config.max_reconnect_attempts

    async def health_check(self) -> bool:
        """
        Perform health check on the MQTT connection.

        Returns:
            True if healthy, False otherwise
        """
        if not self._running or not self._client or not self._connected:
            return False

        try:
            # Check if client is still connected
            return bool(self._client.is_connected())
        except Exception as e:
            self.logger.warning(f"MQTT health check failed: {e}")
            return False
