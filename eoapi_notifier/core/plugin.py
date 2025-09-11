"""
Plugin system.

Provides protocol-based plugin architecture with type safety, structured metadata,
and async context management for notification sources and outputs.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel, ConfigDict

from .event import NotificationEvent

# Type variables for generic plugin system
T = TypeVar("T", bound="BasePlugin[Any]")
C = TypeVar("C", bound="BasePluginConfig")


@dataclass(frozen=True)
class PluginMetadata:
    """Structured metadata for plugins."""

    name: str
    description: str
    tags: list[str] = field(default_factory=list)
    category: str = "unknown"
    priority: int = 0


@runtime_checkable
class PluginConfigProtocol(Protocol):
    """Protocol for plugin configurations with required interface methods."""

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        """Get sample configuration for this plugin type."""
        ...

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        """Get structured metadata for this plugin type."""
        ...

    def get_connection_info(self) -> str:
        """Get connection info string for display."""
        ...

    def get_status_info(self) -> dict[str, Any]:
        """Get detailed status information for display."""
        ...


class BasePluginConfig(BaseModel):
    """
    Base configuration class for plugins implementing the protocol.

    Provides common configuration methods and Pydantic validation.
    """

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        """Default implementation - subclasses should override."""
        return {}

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        """Default implementation - subclasses should override."""
        return PluginMetadata(
            name=cls.__name__, description=f"Configuration for {cls.__name__}"
        )

    def get_connection_info(self) -> str:
        """Default implementation - subclasses should override."""
        return f"Connection: {self.__class__.__name__}"

    def get_status_info(self) -> dict[str, Any]:
        """Default implementation - subclasses should override."""
        return {"config_type": self.__class__.__name__}


class BasePlugin(ABC, Generic[C]):
    """
    Abstract base class for all plugins with lifecycle management and async context
    support.

    Provides common functionality for sources and outputs with type-safe configuration.
    """

    def __init__(self, config: C) -> None:
        """Initialize base plugin."""
        if not isinstance(config, PluginConfigProtocol):
            raise TypeError(
                f"Config must implement PluginConfigProtocol, got {type(config)}"
            )

        self.config: C = config
        self.logger: logging.Logger = logging.getLogger(self.__class__.__module__)
        self._running: bool = False
        self._started: bool = False

    @abstractmethod
    async def start(self) -> None:
        """
        Start the plugin.

        Should set _running to True and initialize any connections.
        Must be implemented by subclasses.
        """
        if self._started:
            raise RuntimeError(f"Plugin {self.__class__.__name__} is already started")
        self._running = True
        self._started = True

    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the plugin.

        Should set _running to False and clean up any resources.
        Must be implemented by subclasses.
        """
        if not self._started:
            return  # Already stopped or never started
        self._running = False
        self._started = False

    async def restart(self) -> None:
        """Restart the plugin."""
        if self._started:
            await self.stop()
        await self.start()

    @property
    def is_running(self) -> bool:
        """Check if plugin is currently running."""
        return self._running

    @property
    def is_started(self) -> bool:
        """Check if plugin has been started (even if not currently running)."""
        return self._started

    def get_status(self) -> dict[str, Any]:
        """Get plugin status information."""
        metadata = self.config.get_metadata()
        return {
            "name": metadata.name,
            "type": self.__class__.__name__,
            "running": self._running,
            "started": self._started,
            "config_type": self.config.__class__.__name__,
            "metadata": {
                "description": metadata.description,
                "tags": metadata.tags,
                "category": metadata.category,
            },
        }

    async def __aenter__(self: T) -> T:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.stop()


class BaseSource(BasePlugin[C]):
    """
    Abstract base class for notification sources.

    Sources listen for events from external systems and yield NotificationEvent objects.
    """

    @abstractmethod
    def listen(self) -> AsyncIterator[NotificationEvent]:
        """
        Listen for events from the source.

        Yields:
            NotificationEvent objects as they are received

        Should run continuously while _running is True.
        Must be implemented by subclasses.
        """
        pass

    @asynccontextmanager
    async def event_stream(self) -> AsyncIterator[AsyncIterator[NotificationEvent]]:
        """Async context manager for event streaming."""
        async with self:
            yield self.listen()


class BaseOutput(BasePlugin[C]):
    """
    Abstract base class for notification outputs.

    Outputs send NotificationEvent objects to external systems.
    """

    @abstractmethod
    async def send_event(self, event: NotificationEvent) -> bool:
        """
        Send a single notification event.

        Args:
            event: Event to send

        Returns:
            True if sent successfully, False otherwise

        Must be implemented by subclasses.
        """
        pass

    async def send_batch(self, events: list[NotificationEvent]) -> int:
        """
        Send multiple events in batch.

        Default implementation sends events one by one.
        Subclasses can override for more efficient batch processing.

        Args:
            events: List of events to send

        Returns:
            Number of events successfully sent
        """
        success_count = 0
        for event in events:
            try:
                if await self.send_event(event):
                    success_count += 1
            except Exception as e:
                self.logger.error(f"Failed to send event: {e}")

        return success_count

    async def health_check(self) -> bool:
        """
        Perform health check on the output.

        Default implementation checks if running.
        Subclasses can override for more sophisticated health checks.

        Returns:
            True if healthy, False otherwise
        """
        return self._running

    async def send_with_retry(
        self,
        event: NotificationEvent,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> bool:
        """
        Send event with retry logic.

        Args:
            event: Event to send
            max_retries: Maximum number of retry attempts
            backoff_factor: Backoff multiplier for retry delays

        Returns:
            True if sent successfully, False otherwise
        """
        import asyncio

        for attempt in range(max_retries + 1):
            try:
                return await self.send_event(event)
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(
                        f"Failed to send event after {max_retries} retries: {e}"
                    )
                    return False

                delay = backoff_factor * (2**attempt)
                self.logger.warning(
                    f"Send attempt {attempt + 1} failed, retrying in {delay}s: {e}"
                )
                await asyncio.sleep(delay)

        return False


class PluginError(Exception):
    """Base exception for plugin-related errors."""

    def __init__(
        self,
        plugin_name: str,
        message: str,
        cause: Exception | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """
        Initialize plugin error.

        Args:
            plugin_name: Name of the plugin that caused the error
            message: Error message
            cause: Optional underlying exception
            metadata: Optional additional error context
        """
        self.plugin_name = plugin_name
        self.cause = cause
        self.metadata = metadata or {}
        super().__init__(f"Plugin '{plugin_name}': {message}")


class PluginConfigError(PluginError):
    """Error in plugin configuration."""

    pass


class PluginConnectionError(PluginError):
    """Error connecting to external system."""

    pass


class PluginValidationError(PluginError):
    """Error during plugin validation."""

    pass


class PluginLifecycleError(PluginError):
    """Error during plugin lifecycle operations."""

    pass
