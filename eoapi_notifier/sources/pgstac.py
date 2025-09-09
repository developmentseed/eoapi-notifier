"""
pgSTAC source plugin.

Connects to pgSTAC and listens for item change notifications using
LISTEN/NOTIFY.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import asyncpg
from pydantic import ConfigDict, field_validator

from ..core.event import NotificationEvent
from ..core.plugin import BasePluginConfig, BaseSource, PluginMetadata


class PgSTACSourceConfig(BasePluginConfig):
    """Configuration for pgSTAC notification source."""

    model_config = ConfigDict(extra="forbid")

    # Connection parameters
    host: str = "localhost"
    port: int = 5432
    database: str = "pgstac"
    user: str = "postgres"
    password: str = ""

    # LISTEN/NOTIFY settings
    channel: str = "pgstac_items"
    queue_size: int = 1000
    listen_timeout: float = 1.0

    # Reconnection settings
    max_reconnect_attempts: int = -1  # -1 for infinite
    reconnect_delay: float = 5.0
    reconnect_backoff_factor: float = 2.0
    max_reconnect_delay: float = 60.0

    # Event configuration
    event_source: str = "/eoapi/stac/pgstac"
    event_type: str = "org.eoapi.stac.item"

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        """Get sample configuration for this source."""
        return {
            "host": "localhost",
            "port": 5432,
            "database": "pgstac",
            "user": "postgres",
            "password": "your-password",
            "channel": "pgstac_items",
            "max_reconnect_attempts": -1,
            "reconnect_delay": 5.0,
        }

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        """Get structured metadata for this source type."""
        return PluginMetadata(
            name="pgstac",
            description="Resilient pgSTAC LISTEN/NOTIFY source with auto-reconnection",
            version="2.0.0",
            category="database",
            tags=["postgresql", "pgstac", "stac", "listen-notify", "resilient"],
            priority=10,
        )

    def get_connection_info(self) -> str:
        """Get connection info string for display."""
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

    def get_status_info(self) -> dict[str, Any]:
        """Get status information for display."""
        return {
            "Host": f"{self.host}:{self.port}",
            "Database": self.database,
            "Channel": self.channel,
        }


class PgSTACSource(BaseSource):
    """
    Resilient pgSTAC source for notifications with auto-reconnection.
    """

    def __init__(self, config: PgSTACSourceConfig):
        """Initialize pgSTAC source."""
        super().__init__(config)
        self._connection: asyncpg.Connection | None = None
        self._notification_queue: asyncio.Queue | None = None
        self._reconnect_attempts: int = 0
        self._current_delay: float = config.reconnect_delay
        self._connected: bool = False

    async def start(self) -> None:
        """Start pgSTAC connection and setup listener."""
        self.logger.info(f"Starting pgSTAC source: {self.config.get_connection_info()}")

        self._notification_queue = asyncio.Queue(maxsize=self.config.queue_size)
        await self._connect()
        await super().start()

    async def stop(self) -> None:
        """Stop pgSTAC connection and cleanup."""
        await self._disconnect()
        self._notification_queue = None
        await super().stop()
        self.logger.info("pgSTAC source stopped")

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        """Listen for pgSTAC notifications with automatic reconnection."""
        if not self._notification_queue:
            raise RuntimeError("pgSTAC source not started")

        while self._running:
            try:
                # Ensure we're connected
                if not self._connected:
                    await self._reconnect_if_needed()
                    if not self._connected:
                        await asyncio.sleep(1.0)
                        continue

                # Wait for notification
                payload = await asyncio.wait_for(
                    self._notification_queue.get(),
                    timeout=self.config.listen_timeout,
                )

                if payload:
                    event = self._process_notification_payload(payload)
                    if event:
                        yield event

            except TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in listen loop: {e}")
                self._connected = False
                await asyncio.sleep(1.0)

    async def _connect(self) -> bool:
        """Establish connection to PostgreSQL."""
        try:
            self.logger.info("Connecting to PostgreSQL...")

            self._connection = await asyncpg.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
            )

            # Setup listener
            if self._connection is None:
                raise RuntimeError("Connection is None after successful connect")
            await self._connection.execute(f"LISTEN {self.config.channel}")
            await self._connection.add_listener(
                self.config.channel, self._notification_callback
            )

            self._connected = True
            self._reconnect_attempts = 0
            self._current_delay = self.config.reconnect_delay

            self.logger.info(
                f"Connected and listening to channel: {self.config.channel}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            self._connected = False
            await self._disconnect()
            return False

    async def _disconnect(self) -> None:
        """Close PostgreSQL connection safely."""
        self._connected = False
        if self._connection:
            try:
                await self._connection.remove_listener(
                    self.config.channel, self._notification_callback
                )
                await self._connection.execute(f"UNLISTEN {self.config.channel}")
                await self._connection.close()
            except Exception as e:
                self.logger.warning(f"Error during disconnect: {e}")
            finally:
                self._connection = None

    async def _reconnect_if_needed(self) -> None:
        """Attempt reconnection with exponential backoff."""
        if not self._should_reconnect():
            return

        self.logger.info(
            f"Reconnecting in {self._current_delay:.1f}s "
            f"(attempt {self._reconnect_attempts + 1})"
        )
        await asyncio.sleep(self._current_delay)

        self._reconnect_attempts += 1

        if await self._connect():
            self.logger.info("Reconnection successful")
        else:
            # Exponential backoff
            self._current_delay = min(
                self._current_delay * self.config.reconnect_backoff_factor,
                self.config.max_reconnect_delay,
            )

    def _should_reconnect(self) -> bool:
        """Check if we should attempt to reconnect."""
        if not self._running:
            return False

        if self.config.max_reconnect_attempts == -1:
            return True

        return bool(self._reconnect_attempts < self.config.max_reconnect_attempts)

    def _notification_callback(
        self, connection: Any, pid: int, channel: str, payload: str
    ) -> None:
        """Callback for asyncpg notifications."""
        if self._notification_queue:
            try:
                self._notification_queue.put_nowait(payload)
            except asyncio.QueueFull:
                self.logger.warning("Notification queue full, dropping notification")

    def _from_pgstac_notification(self, payload: dict[str, Any]) -> NotificationEvent:
        """Create NotificationEvent from pgSTAC LISTEN/NOTIFY payload."""
        operation = payload.get("operation") or payload.get("event", "INSERT")
        collection = payload.get("collection", "unknown")
        item_id = payload.get("item_id") or payload.get("id")

        # Handle timestamp parsing
        timestamp = payload.get("timestamp") or payload.get("datetime")
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                timestamp = datetime.now(UTC)
        elif not timestamp:
            timestamp = datetime.now(UTC)

        return NotificationEvent(
            source=self.config.event_source,
            type=self.config.event_type,
            operation=operation,
            collection=collection,
            item_id=item_id,
            timestamp=timestamp,
            data=payload,
        )

    def _process_notification_payload(self, payload: str) -> NotificationEvent | None:
        """Process a pgSTAC notification payload into a NotificationEvent."""
        try:
            payload_data = json.loads(payload)
            event = self._from_pgstac_notification(payload_data)
            self.logger.debug(
                f"Processed: {event.operation} on {event.collection}/{event.item_id}"
            )
            return event
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse notification payload: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing notification: {e}")
            return None
