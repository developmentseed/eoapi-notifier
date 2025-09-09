"""
Registry system.

Provides a type-safe plugin registry for notification sources and outputs
with lazy loading and configuration validation.
"""

import logging
from importlib import import_module
from typing import Any, Generic, TypeVar

from pydantic import ValidationError

from .plugin import BaseOutput, BasePlugin, BasePluginConfig, BaseSource, PluginError

logger = logging.getLogger(__name__)

# Type variables for generic registry
T = TypeVar("T", bound=BasePlugin[Any])


class ComponentRegistry(Generic[T]):
    """Type-safe registry for plugin components with lazy loading."""

    def __init__(self, base_type: type[T]) -> None:
        """Initialize registry for specific plugin type."""
        self.base_type = base_type
        self._registered: dict[
            str, tuple[str, str, str]
        ] = {}  # name -> (module, class, config_class)  # noqa: E501
        self._loaded_classes: dict[
            str, tuple[type[T], type[Any]]
        ] = {}  # cached classes

    def register(
        self,
        name: str,
        module_path: str,
        class_name: str,
        config_class_name: str,
    ) -> None:
        """Register a component for lazy loading."""
        self._registered[name] = (module_path, class_name, config_class_name)
        logger.debug(f"Registered {name}: {module_path}.{class_name}")

    def is_registered(self, name: str) -> bool:
        """Check if component is registered."""
        return name in self._registered

    def list_registered(self) -> list[str]:
        """List all registered component names."""
        return list(self._registered.keys())

    def get_component_class(self, name: str) -> tuple[type[T], type[Any]]:
        """Get component and config classes, loading if needed."""
        if name not in self._registered:
            raise ValueError(f"Unknown component type: {name}")

        # Return cached if already loaded
        if name in self._loaded_classes:
            return self._loaded_classes[name]

        # Load classes
        module_path, class_name, config_class_name = self._registered[name]

        try:
            module = import_module(module_path)

            # Get component class
            component_class = getattr(module, class_name)
            if not issubclass(component_class, self.base_type):
                raise TypeError(
                    f"{class_name} is not a subclass of {self.base_type.__name__}"
                )

            # Get config class
            config_class = getattr(module, config_class_name)
            if not issubclass(config_class, BasePluginConfig):
                raise TypeError(
                    f"{config_class_name} is not a subclass of BasePluginConfig"
                )

            # Cache and return - runtime validation ensures type safety
            self._loaded_classes[name] = (component_class, config_class)
            logger.debug(f"Loaded {name}: {class_name} with {config_class_name}")
            return (component_class, config_class)

        except ImportError as e:
            raise ImportError(f"Cannot import module {module_path}: {e}") from e
        except AttributeError as e:
            raise AttributeError(f"Cannot find class in {module_path}: {e}") from e

    def create_component(self, name: str, config: dict[str, Any]) -> T:
        """Create component instance with validated configuration."""
        component_class, config_class = self.get_component_class(name)

        # Validate configuration
        try:
            validated_config = config_class(**config)
        except ValidationError as e:
            raise PluginError(name, f"Invalid configuration: {e}") from e

        # Create instance
        try:
            return component_class(validated_config)
        except Exception as e:
            raise PluginError(name, f"Failed to create instance: {e}") from e


class SourceRegistry(ComponentRegistry[BaseSource[Any]]):
    """Registry for notification sources."""

    def __init__(self) -> None:
        super().__init__(BaseSource)  # type: ignore[type-abstract]
        self._register_builtin_sources()

    def _register_builtin_sources(self) -> None:
        """Register built-in source types."""
        self.register(
            name="postgres",
            module_path="eoapi_notifier.sources.pgstac",
            class_name="PgSTACSource",
            config_class_name="PgSTACSourceConfig",
        )


class OutputRegistry(ComponentRegistry[BaseOutput[Any]]):
    """Registry for notification outputs."""

    def __init__(self) -> None:
        super().__init__(BaseOutput)  # type: ignore[type-abstract]
        self._register_builtin_outputs()

    def _register_builtin_outputs(self) -> None:
        """Register built-in output types."""
        self.register(
            name="mqtt",
            module_path="eoapi_notifier.outputs.mqtt",
            class_name="MQTTAdapter",
            config_class_name="MQTTConfig",
        )


# Global registry instances
source_registry = SourceRegistry()
output_registry = OutputRegistry()


# Core API functions
def register_source(
    name: str, module_path: str, class_name: str, config_class_name: str
) -> None:
    """Register a custom source type."""
    source_registry.register(name, module_path, class_name, config_class_name)


def register_output(
    name: str, module_path: str, class_name: str, config_class_name: str
) -> None:
    """Register a custom output type."""
    output_registry.register(name, module_path, class_name, config_class_name)


def create_source(source_type: str, config: dict[str, Any]) -> BaseSource[Any]:
    """Create a source instance from configuration."""
    return source_registry.create_component(source_type, config)


def create_output(output_type: str, config: dict[str, Any]) -> BaseOutput[Any]:
    """Create an output instance from configuration."""
    return output_registry.create_component(output_type, config)


def get_available_sources() -> list[str]:
    """Get list of available source types."""
    return source_registry.list_registered()


def get_available_outputs() -> list[str]:
    """Get list of available output types."""
    return output_registry.list_registered()
