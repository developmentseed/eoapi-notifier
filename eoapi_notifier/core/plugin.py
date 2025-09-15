"""
Plugin system.

Provides protocol-based plugin architecture with type safety, structured metadata,
and async context management for notification sources and outputs.
"""

import os
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar, runtime_checkable

from loguru import logger
from pydantic import BaseModel, ConfigDict, model_validator

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
    Automatically applies environment variable overrides for all config fields.
    """

    model_config = ConfigDict(extra="forbid")

    def _get_plugin_prefix(self) -> str:
        """
        Extract plugin prefix from config class name.

        Examples:
        - PgSTACSourceConfig -> PGSTAC
        - MQTTConfig -> MQTT
        - CloudEventsConfig -> CLOUDEVENTS
        """
        class_name = self.__class__.__name__

        # Remove common suffixes
        for suffix in ["SourceConfig", "Config", "Source", "Output"]:
            if class_name.endswith(suffix):
                class_name = class_name[: -len(suffix)]
                break

        # Convert to uppercase and handle special cases
        if class_name.lower() == "pgstac":
            return "PGSTAC"
        elif class_name.lower() == "cloudevents":
            return "CLOUDEVENTS"
        elif class_name.lower() == "mqtt":
            return "MQTT"
        else:
            return class_name.upper()

    @model_validator(mode="after")
    def apply_env_overrides(self) -> "BasePluginConfig":
        """
        Apply environment variable overrides for all configuration fields.

        Uses simple plugin-prefixed environment variables:
        - PGSTAC_HOST, PGSTAC_PORT, PGSTAC_PASSWORD, etc.
        - MQTT_BROKER_HOST, MQTT_TIMEOUT, MQTT_USE_TLS, etc.
        - CLOUDEVENTS_ENDPOINT, CLOUDEVENTS_TIMEOUT, etc.
        """
        plugin_prefix = self._get_plugin_prefix()

        for field_name, field_info in self.model_fields.items():
            # Check for plugin-prefixed environment variable
            env_var_name = f"{plugin_prefix}_{field_name.upper()}"
            env_value = os.getenv(env_var_name)

            if env_value is None:
                continue

            try:
                # Get the field's type annotation
                field_type = field_info.annotation

                # Handle Union types (like str | None) safely
                origin = getattr(field_type, "__origin__", None)
                if origin is not None:
                    args = getattr(field_type, "__args__", ())
                    if len(args) > 0:
                        # For Union types, use the first non-None type
                        non_none_types = [arg for arg in args if arg is not type(None)]
                        if non_none_types:
                            field_type = non_none_types[0]
                    elif origin is list:
                        # Handle list types - split by comma
                        list_value = [
                            item.strip()
                            for item in env_value.split(",")
                            if item.strip()
                        ]
                        setattr(self, field_name, list_value)
                        logger.debug(
                            f"Applied env override: {env_var_name}={env_value} -> "
                            f"{field_name}={list_value}"
                        )
                        continue

                # Convert environment variable value to appropriate type
                converted_value: Any
                if field_type is bool or (
                    isinstance(field_type, type) and issubclass(field_type, bool)
                ):
                    # Handle boolean conversion
                    converted_value = env_value.lower() in ("true", "1", "yes", "on")
                elif field_type is int or (
                    isinstance(field_type, type) and issubclass(field_type, int)
                ):
                    converted_value = int(env_value)
                elif field_type is float or (
                    isinstance(field_type, type) and issubclass(field_type, float)
                ):
                    converted_value = float(env_value)
                else:
                    # Default to string
                    converted_value = env_value

                # Apply the override
                setattr(self, field_name, converted_value)
                logger.debug(
                    f"Applied env override: {env_var_name}={env_value} -> "
                    f"{field_name}={converted_value}"
                )

            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Failed to apply env override {env_var_name}={env_value} to "
                    f"field {field_name}: {e}"
                )

        return self

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
        self.logger = logger
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
