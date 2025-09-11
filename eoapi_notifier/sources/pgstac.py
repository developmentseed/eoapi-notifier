"""
pgSTAC source plugin.

Connects to pgSTAC and listens for item change notifications using
LISTEN/NOTIFY. It includes optional operation correlation to transform
pgSTAC's DELETE+INSERT pairs into semantic UPDATE events.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import asyncpg
from pydantic import ConfigDict, Field, field_validator

from ..core.event import NotificationEvent
from ..core.plugin import BasePluginConfig, BaseSource, PluginMetadata

# Constants
DEFAULT_CORRELATION_WINDOW = 5.0
DEFAULT_CLEANUP_INTERVAL = 1.0
DEFAULT_EVENT_QUEUE_SIZE = 1000
MIN_CORRELATION_WINDOW = 0.1
MAX_CORRELATION_WINDOW = 300.0
MIN_CLEANUP_INTERVAL = 0.1
MAX_CLEANUP_INTERVAL = 60.0


@dataclass
class PendingOperation:
    """Represents an operation waiting for potential correlation."""

    event: NotificationEvent
    timestamp: datetime
    collection: str
    item_id: str


class OperationCorrelator:
    """
    Correlates pgSTAC operations to provide semantic event interpretation.

    Transforms raw pgSTAC DELETE+INSERT pairs into meaningful UPDATE operations
    by maintaining a sliding correlation window.
    """

    def __init__(
        self,
        correlation_window: float = DEFAULT_CORRELATION_WINDOW,
        cleanup_interval: float = DEFAULT_CLEANUP_INTERVAL,
        expired_callback: Callable[[NotificationEvent], Any] | None = None,
    ):
        """
        Initialize the operation correlator.

        Args:
            correlation_window: Time window in seconds to correlate operations
            cleanup_interval: How often to clean up expired operations in seconds
            expired_callback: Optional callback for expired DELETE operations
        """
        self.correlation_window = correlation_window
        self.cleanup_interval = cleanup_interval
        self.expired_callback = expired_callback
        self._pending_operations: dict[tuple[str, str], PendingOperation] = {}
        self._cleanup_task: asyncio.Task[None] | None = None
        self._running = False
        self.logger = logging.getLogger(__name__)

    # Lifecycle Methods

    async def start(self) -> None:
        """Start the correlator and begin cleanup task."""
        if self._running:
            return

        self._running = True
        self._cleanup_task = asyncio.create_task(self._run_cleanup_loop())
        self.logger.info(
            "Started operation correlator (window=%ss, cleanup=%ss)",
            self.correlation_window,
            self.cleanup_interval,
        )

    async def stop(self) -> None:
        """Stop the correlator and cleanup resources."""
        if not self._running:
            return

        self._running = False
        await self._cancel_cleanup_task()
        await self._process_remaining_operations()
        self.logger.info("Stopped operation correlator")

    # Event Processing Methods

    async def process_events(
        self, event_stream: AsyncIterator[NotificationEvent]
    ) -> AsyncIterator[NotificationEvent]:
        """
        Process a stream of events and yield correlated semantic events.

        Args:
            event_stream: Input stream of raw pgSTAC events

        Yields:
            NotificationEvent objects with semantic operations:
            - item_created (INSERT with no prior DELETE)
            - item_updated (INSERT matching a prior DELETE)
            - item_deleted (DELETE with no subsequent INSERT)
        """
        if not self._running:
            await self.start()

        async for event in event_stream:
            async for correlated_event in self._process_single_event(event):
                yield correlated_event

    async def _process_single_event(
        self, event: NotificationEvent
    ) -> AsyncIterator[NotificationEvent]:
        """Process a single event and yield any resulting correlated events."""
        operation = event.operation.upper()

        if not event.item_id:
            self.logger.warning(
                "Event missing item_id, skipping correlation: %s", event
            )
            return

        correlation_key = (event.collection, event.item_id)

        if operation == "DELETE":
            await self._handle_delete_operation(event, correlation_key)
        elif operation == "INSERT":
            async for result_event in self._handle_insert_operation(
                event, correlation_key
            ):
                yield result_event
        elif operation == "UPDATE":
            yield self._create_semantic_event(event, "item_updated")
        else:
            # Pass through unknown operations unchanged
            yield event

    # Operation Handler Methods

    async def _handle_delete_operation(
        self, event: NotificationEvent, key: tuple[str, str]
    ) -> None:
        """Store DELETE operation for potential correlation with future INSERT."""
        collection, item_id = key

        pending = PendingOperation(
            event=event,
            timestamp=event.timestamp,
            collection=collection,
            item_id=item_id,
        )

        self._pending_operations[key] = pending
        self.logger.debug("Stored DELETE for correlation: %s/%s", collection, item_id)

    async def _handle_insert_operation(
        self, event: NotificationEvent, key: tuple[str, str]
    ) -> AsyncIterator[NotificationEvent]:
        """Handle INSERT operation by checking for correlation with prior DELETE."""
        collection, item_id = key
        pending_operation = self._pending_operations.pop(key, None)

        if pending_operation:
            # This is a correlated UPDATE operation
            time_diff = (event.timestamp - pending_operation.timestamp).total_seconds()

            self.logger.debug(
                "Correlated DELETE+INSERT as UPDATE: %s/%s (%.2fs apart)",
                collection,
                item_id,
                time_diff,
            )

            yield self._create_update_event(event, pending_operation, time_diff)
        else:
            # This is a genuine CREATE operation
            self.logger.debug("Real INSERT (creation): %s/%s", collection, item_id)
            yield self._create_semantic_event(event, "item_created")

    # Event Creation Methods

    def _create_semantic_event(
        self, event: NotificationEvent, operation: str
    ) -> NotificationEvent:
        """Create a semantic event with the specified operation."""
        semantic_event = event.model_copy()
        semantic_event.operation = operation
        return semantic_event

    def _create_update_event(
        self,
        insert_event: NotificationEvent,
        delete_operation: PendingOperation,
        time_diff: float,
    ) -> NotificationEvent:
        """Create an UPDATE event with correlation metadata."""
        update_event = insert_event.model_copy()
        update_event.operation = "item_updated"

        # Add correlation metadata
        update_event.data = {
            **insert_event.data,
            "correlation": {
                "delete_timestamp": delete_operation.timestamp.isoformat(),
                "insert_timestamp": insert_event.timestamp.isoformat(),
                "correlation_time_seconds": time_diff,
            },
        }

        return update_event

    # Cleanup Methods

    async def _run_cleanup_loop(self) -> None:
        """Background task to clean up expired operations."""
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                if self._running:  # Check again after sleep
                    await self._cleanup_expired_operations()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in cleanup loop: %s", e)

    async def _cleanup_expired_operations(self) -> None:
        """Process and emit events for expired DELETE operations."""
        now = datetime.now(UTC)
        expired_keys = self._find_expired_operations(now)

        if not expired_keys:
            return

        for key in expired_keys:
            if key in self._pending_operations:
                pending = self._pending_operations.pop(key)
                await self._handle_expired_operation(pending)

        self.logger.debug("Processed %d expired operations", len(expired_keys))

    def _find_expired_operations(self, current_time: datetime) -> list[tuple[str, str]]:
        """Find operations that have exceeded the correlation window."""
        expired_keys = []

        for key, pending in self._pending_operations.items():
            age = (current_time - pending.timestamp).total_seconds()
            if age >= self.correlation_window:
                expired_keys.append(key)

        return expired_keys

    async def _handle_expired_operation(self, pending: PendingOperation) -> None:
        """Handle an expired DELETE operation by treating it as a real deletion."""
        self.logger.debug(
            "DELETE expired, treating as real deletion: %s/%s",
            pending.collection,
            pending.item_id,
        )

        deleted_event = self._create_semantic_event(pending.event, "item_deleted")

        if self.expired_callback:
            await self._invoke_expired_callback(deleted_event)

    async def _invoke_expired_callback(self, event: NotificationEvent) -> None:
        """Safely invoke the expired callback."""
        if self.expired_callback is None:
            return

        try:
            if asyncio.iscoroutinefunction(self.expired_callback):
                await self.expired_callback(event)
            else:
                self.expired_callback(event)
        except Exception as e:
            self.logger.error("Error in expired callback: %s", e)

    # Lifecycle Helper Methods

    async def _cancel_cleanup_task(self) -> None:
        """Cancel the cleanup task if it's running."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _process_remaining_operations(self) -> None:
        """Process any remaining operations on shutdown."""
        remaining_operations = list(self._pending_operations.values())
        self._pending_operations.clear()

        for pending in remaining_operations:
            self.logger.debug(
                "Processing remaining operation on shutdown: %s/%s",
                pending.collection,
                pending.item_id,
            )

    # Status and Monitoring Methods

    def get_pending_count(self) -> int:
        """Get the number of pending operations."""
        return len(self._pending_operations)

    def get_oldest_pending_age(self) -> float | None:
        """Get the age in seconds of the oldest pending operation."""
        if not self._pending_operations:
            return None

        now = datetime.now(UTC)
        oldest = min(op.timestamp for op in self._pending_operations.values())
        return (now - oldest).total_seconds()

    def get_status(self) -> dict[str, Any]:
        """Get correlator status information."""
        return {
            "running": self._running,
            "correlation_window": self.correlation_window,
            "cleanup_interval": self.cleanup_interval,
            "pending_operations": self.get_pending_count(),
            "oldest_pending_age": self.get_oldest_pending_age(),
        }


class PgSTACSourceConfig(BasePluginConfig):
    """Configuration for pgSTAC notification source with correlation capabilities."""

    model_config = ConfigDict(extra="forbid")

    host: str = "localhost"
    port: int = 5432
    database: str = "pgstac"
    user: str = "postgres"
    password: str = ""

    channel: str = "pgstac_items"
    tables: list[str] | None = None

    max_reconnect_attempts: int = -1  # -1 for infinite
    reconnect_delay: float = 5.0
    listen_timeout: float = 5.0

    event_source: str = "/eoapi/stac/pgstac"
    event_type: str = "org.eoapi.stac.item"

    enable_correlation: bool = Field(
        default=True,
        description="Enable DELETE+INSERT correlation for UPDATE semantics",
    )

    correlation_window: float = Field(
        default=DEFAULT_CORRELATION_WINDOW,
        description="Time window in seconds to correlate operations",
        gt=MIN_CORRELATION_WINDOW,
        le=MAX_CORRELATION_WINDOW,
    )

    cleanup_interval: float = Field(
        default=DEFAULT_CLEANUP_INTERVAL,
        description="How often to check for expired operations in seconds",
        gt=MIN_CLEANUP_INTERVAL,
        le=MAX_CLEANUP_INTERVAL,
    )

    event_queue_size: int = Field(
        default=DEFAULT_EVENT_QUEUE_SIZE,
        description="Maximum size of internal event queue",
        gt=0,
        le=10000,
    )

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
            "enable_correlation": True,
            "correlation_window": DEFAULT_CORRELATION_WINDOW,
            "cleanup_interval": DEFAULT_CLEANUP_INTERVAL,
            "event_queue_size": DEFAULT_EVENT_QUEUE_SIZE,
        }

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        """Get structured metadata for this source type."""
        return PluginMetadata(
            name="pgstac",
            description="pgSTAC LISTEN/NOTIFY source",
            category="database",
            tags=[
                "postgresql",
                "pgstac",
                "stac",
                "listen-notify",
                "correlation",
                "resilient",
            ],
            priority=10,
        )

    def get_connection_info(self) -> str:
        """Get connection info string for display."""
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

    def get_status_info(self) -> dict[str, Any]:
        """Get status information for display."""
        status = {
            "Host": f"{self.host}:{self.port}",
            "Database": self.database,
            "Channel": self.channel,
            "Correlation Enabled": self.enable_correlation,
        }

        if self.enable_correlation:
            status.update(
                {
                    "Correlation Window": f"{self.correlation_window}s",
                    "Cleanup Interval": f"{self.cleanup_interval}s",
                }
            )

        return status


class PgSTACSource(BaseSource):
    """
        pgSTAC source for notifications with auto-reconnection.
    d
    """

    def __init__(self, config: PgSTACSourceConfig):
        """Initialize pgSTAC source."""
        super().__init__(config)

        # Database connection state
        self._connection: asyncpg.Connection | None = None
        self._notification_queue: asyncio.Queue | None = None
        self._reconnect_attempts: int = 0
        self._current_delay: float = config.reconnect_delay
        self._connected: bool = False

        # Event processing
        self._event_queue: asyncio.Queue[NotificationEvent] = asyncio.Queue(
            maxsize=config.event_queue_size
        )
        self._processing_task: asyncio.Task[None] | None = None

        # Operation correlation (optional)
        self._correlator: OperationCorrelator | None = None
        if config.enable_correlation:
            self._correlator = OperationCorrelator(
                correlation_window=config.correlation_window,
                cleanup_interval=config.cleanup_interval,
                expired_callback=self._handle_expired_delete,
            )

    async def start(self) -> None:
        """Start pgSTAC connection and setup listener."""
        if self.is_running:
            return

        correlation_mode = "with correlation" if self._correlator else "raw events only"
        self.logger.info(
            "Starting pgSTAC source (%s): %s",
            correlation_mode,
            self.config.get_connection_info(),
        )

        self.logger.debug("Step 1: Establishing database connection...")
        await self._establish_connection()
        self.logger.debug("✓ Database connection established")

        if self._connected:
            self.logger.debug("Step 2: Setting up PostgreSQL listener...")
            await self._setup_listener()
            self.logger.debug("✓ PostgreSQL listener setup complete")
        else:
            self.logger.debug(
                "Step 2: Skipping listener setup, will setup after connection"
            )

        self.logger.debug("Step 3: Starting operation correlator...")
        await self._start_correlator()
        self.logger.debug("✓ Operation correlator started")

        self.logger.debug("Step 4: Starting event processing...")
        self._start_event_processing()
        self.logger.debug("✓ Event processing started")

        self._set_running_state(True)
        self.logger.info("✓ pgSTAC source fully started and ready")

    async def stop(self) -> None:
        """Stop pgSTAC source and cleanup resources."""
        if not self.is_running:
            return

        self.logger.debug("Stopping pgSTAC source")
        self._set_running_state(False)

        await self._stop_event_processing()
        await self._stop_correlator()
        await self._cleanup_connection()

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        """Listen for notification events from pgSTAC."""
        if not self.is_running:
            raise RuntimeError("Source must be started before listening")

        if self._correlator:
            async for event in self._correlator.process_events(
                self._raw_event_stream()
            ):
                yield event
        else:
            async for event in self._raw_event_stream():
                yield event

    async def _establish_connection(self) -> None:
        """Attempt initial database connection, setup background retry if needed."""
        try:
            self._connection = await asyncpg.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
            )

            self._connected = True
            self._reconnect_attempts = 0
            self._current_delay = self.config.reconnect_delay

            self.logger.info(
                "✓ Connected to pgSTAC database at %s:%s/%s",
                self.config.host,
                self.config.port,
                self.config.database,
            )

        except Exception as e:
            self.logger.warning("Initial database connection failed: %s", e)
            self.logger.debug("Database connection will be retried in background")
            # Start background connection retry task
            asyncio.create_task(self._background_connection_retry())

    async def _handle_connection_error(self, error: Exception) -> None:
        """Handle connection errors with exponential backoff."""
        self._reconnect_attempts += 1

        if (
            self.config.max_reconnect_attempts > 0
            and self._reconnect_attempts > self.config.max_reconnect_attempts
        ):
            raise RuntimeError(
                f"Max reconnection attempts exceeded: {error}"
            ) from error

        self.logger.warning(
            "Connection failed (attempt %d): %s. Retrying in %.1fs",
            self._reconnect_attempts,
            error,
            self._current_delay,
        )

        await asyncio.sleep(self._current_delay)
        self._current_delay = min(
            self._current_delay * 1.5, 60.0
        )  # Exponential backoff

    async def _setup_listener(self) -> None:
        """Setup PostgreSQL LISTEN for notifications."""
        if not self._connection or not self._connected:
            self.logger.warning("No database connection available for listener setup")
            return

        self.logger.debug("Executing LISTEN %s...", self.config.channel)
        await self._connection.execute(f"LISTEN {self.config.channel}")

        if not self._notification_queue:
            self.logger.debug("Creating notification queue...")
            self._notification_queue = asyncio.Queue()

        self.logger.debug("Adding notification listener...")
        await self._connection.add_listener(
            self.config.channel, self._handle_notification
        )

        self.logger.info("✓ Listening on channel: %s", self.config.channel)

    async def _cleanup_connection(self) -> None:
        """Clean up database connection and listener."""
        if self._connection:
            try:
                if not self._connection.is_closed():
                    await self._connection.remove_listener(
                        self.config.channel, self._handle_notification
                    )
                    await self._connection.execute(f"UNLISTEN {self.config.channel}")
                    await self._connection.close()
            except Exception as e:
                self.logger.warning("Error during connection cleanup: %s", e)
            finally:
                self._connection = None
                self._connected = False

    def _start_event_processing(self) -> None:
        """Start the background event processing task."""
        self.logger.debug("Creating background task for notification processing...")
        self._processing_task = asyncio.create_task(self._process_notifications())
        self.logger.debug("✓ Background notification processing task created")

    async def _stop_event_processing(self) -> None:
        """Stop the event processing task."""
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

    async def _process_notifications(self) -> None:
        """Background task to process incoming notifications."""
        self.logger.debug("Starting notification processing loop...")
        notification_count = 0

        while self.is_running:
            try:
                if not self._notification_queue or not self._connected:
                    if not self._connected:
                        self.logger.debug("Waiting for database connection...")
                    await asyncio.sleep(1.0)
                    continue

                notification = await asyncio.wait_for(
                    self._notification_queue.get(),
                    timeout=self.config.listen_timeout,
                )

                notification_count += 1
                self.logger.debug(
                    "Processing notification #%d: %s",
                    notification_count,
                    notification[:100],
                )

                event = self._create_notification_event(notification)
                if event:
                    await self._event_queue.put(event)
                    self.logger.debug(
                        "✓ Event #%d queued: %s", notification_count, event.id
                    )
                else:
                    self.logger.warning(
                        "✗ Failed to create event from notification #%d",
                        notification_count,
                    )

            except TimeoutError:
                # Log periodic heartbeat to show we're alive
                if notification_count == 0:
                    self.logger.debug(
                        "Waiting for PostgreSQL notifications... (listening on %s)",
                        self.config.channel,
                    )
                continue  # Normal timeout, keep listening
            except asyncio.CancelledError:
                self.logger.debug("Notification processing cancelled")
                break
            except Exception as e:
                self.logger.error(
                    "Error processing notification #%d: %s",
                    notification_count,
                    e,
                    exc_info=True,
                )

    def _handle_notification(
        self, connection: asyncpg.Connection, pid: int, channel: str, payload: str
    ) -> None:
        """Handle incoming PostgreSQL notification."""
        self.logger.debug(
            "Received PostgreSQL notification on channel %s (pid=%d): %s",
            channel,
            pid,
            payload[:200],
        )

        if self._notification_queue:
            try:
                self._notification_queue.put_nowait(payload)
                self.logger.debug("✓ Notification queued for processing")
            except asyncio.QueueFull:
                self.logger.error(
                    "✗ Notification queue full (%d items), dropping notification",
                    self._notification_queue.qsize(),
                )

    def _create_notification_event(self, payload: str) -> NotificationEvent | None:
        """Create NotificationEvent from PostgreSQL notification payload."""
        try:
            self.logger.debug("Parsing notification payload: %s", payload)
            data = json.loads(payload)

            event = NotificationEvent(
                id=f"pgstac-{data.get('id', 'unknown')}",
                source=self.config.event_source,
                type=self.config.event_type,
                operation=data.get("op", "unknown"),
                collection=data.get("collection", "unknown"),
                item_id=data.get("id"),
                timestamp=datetime.now(UTC),
                data=data,
            )

            self.logger.debug(
                "✓ Created event: %s (op=%s, collection=%s, item_id=%s)",
                event.id,
                event.operation,
                event.collection,
                event.item_id,
            )
            return event

        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(
                "✗ Invalid notification payload: %s - payload was: %s", e, payload
            )
            return None

    async def _raw_event_stream(self) -> AsyncIterator[NotificationEvent]:
        """Generate raw event stream from the event queue."""
        self.logger.debug("Starting raw event stream from queue...")
        event_stream_count = 0

        while self.is_running:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                event_stream_count += 1
                self.logger.debug(
                    "Streaming event #%d from queue: %s", event_stream_count, event.id
                )
                yield event
            except TimeoutError:
                # Periodic logging to show we're waiting for events
                if event_stream_count == 0:
                    self.logger.debug(
                        "Raw event stream waiting for events... (queue size: %d)",
                        self._event_queue.qsize(),
                    )
                continue
            except Exception as e:
                self.logger.error("Error in raw event stream: %s", e, exc_info=True)
                break

        self.logger.debug(
            "Raw event stream ended (streamed %d events)", event_stream_count
        )

    async def _background_connection_retry(self) -> None:
        """Background task to retry database connection."""
        self.logger.debug("Starting background database connection retry...")

        while self.is_running and not self._connected:
            try:
                await asyncio.sleep(self._current_delay)

                if not self.is_running:
                    break

                self.logger.debug(
                    "Attempting database reconnection (attempt %d)...",
                    self._reconnect_attempts + 1,
                )

                self._connection = await asyncpg.connect(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.user,
                    password=self.config.password,
                )

                self._connected = True
                self._reconnect_attempts = 0
                self._current_delay = self.config.reconnect_delay

                self.logger.info(
                    "✓ Background reconnection successful at %s:%s/%s",
                    self.config.host,
                    self.config.port,
                    self.config.database,
                )

                # Setup listener now that we're connected
                await self._setup_listener()
                self.logger.debug("✓ PostgreSQL listener setup after reconnection")
                break

            except Exception as e:
                await self._handle_connection_error(e)

        self.logger.debug("Background connection retry task ended")

    # Correlator Management Methods

    async def _start_correlator(self) -> None:
        """Start the operation correlator if enabled."""
        if self._correlator:
            await self._correlator.start()

    async def _stop_correlator(self) -> None:
        """Stop the operation correlator if enabled."""
        if self._correlator:
            await self._correlator.stop()

    async def _handle_expired_delete(self, event: NotificationEvent) -> None:
        """Handle expired DELETE operations from the correlator."""
        self.logger.debug(
            "Handling expired DELETE: %s/%s", event.collection, event.item_id
        )
        # In this implementation, expired deletes are handled by the correlator
        # This callback could be used to emit the event to external systems

    # State Management Methods

    def _set_running_state(self, running: bool) -> None:
        """Set the running state consistently."""
        self._running = running
        self._started = running

    # Status Methods

    def get_status(self) -> dict[str, Any]:
        """Get comprehensive source status."""
        status = {
            "connected": self._connected,
            "reconnect_attempts": self._reconnect_attempts,
            "event_queue_size": self._event_queue.qsize(),
            "notification_queue_size": self._notification_queue.qsize()
            if self._notification_queue
            else 0,
            "running": self.is_running,
            "started": self.is_started,
        }

        if self._correlator:
            status["correlator"] = self._correlator.get_status()  # type: ignore[assignment]

        return status
