"""
Tests for the simplified registry system.

Tests the core registry functionality for plugin registration,
lazy loading, and component creation.
"""

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import Mock, patch

import pytest

from eoapi_notifier.core.plugin import (
    BasePlugin,
    BasePluginConfig,
    BaseSource,
    PluginError,
)
from eoapi_notifier.core.registry import (
    ComponentRegistry,
    OutputRegistry,
    SourceRegistry,
    create_output,
    create_source,
    get_available_outputs,
    get_available_sources,
    output_registry,
    register_output,
    register_source,
    source_registry,
)


# Test concrete plugin classes for testing registry
class MockTestPluginConfig(BasePluginConfig):
    """Test plugin configuration."""

    test_value: str = "default"
    name: str = "test"
    value: int = 42

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {"test_value": "sample", "name": "test", "value": 42}


class MockTestPlugin(BasePlugin[MockTestPluginConfig]):
    """Test concrete plugin implementation."""

    async def start(self) -> None:
        """Start the test plugin."""
        await super().start()

    async def stop(self) -> None:
        """Stop the test plugin."""
        await super().stop()


# Mock classes for testing
class MockPluginConfig(BasePluginConfig):
    """Mock plugin configuration for testing."""

    name: str = "test"
    value: int = 42

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {"name": "mock", "value": 100}


class MockPlugin(BasePlugin[MockPluginConfig]):
    """Mock plugin for testing."""

    async def start(self) -> None:
        await super().start()

    async def stop(self) -> None:
        await super().stop()


class MockSourceConfig(BasePluginConfig):
    """Mock source configuration."""

    source_param: str = "default"
    timeout: float = 30.0

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {"source_param": "test", "timeout": 60.0}


class MockSource(BaseSource[MockSourceConfig]):
    """Mock source implementation."""

    async def start(self) -> None:
        await super().start()

    async def stop(self) -> None:
        await super().stop()

    async def listen(self) -> AsyncIterator[Any]:
        """Mock listen method."""
        if False:  # pragma: no cover
            yield  # This makes it an async generator


class TestComponentRegistry:
    """Test core ComponentRegistry functionality."""

    def test_registry_initialization(self) -> None:
        """Test registry initialization."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        assert registry.base_type == MockPlugin
        assert len(registry.list_registered()) == 0

    def test_register_component(self) -> None:
        """Test component registration."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)

        registry.register(
            name="test_plugin",
            module_path="test.module",
            class_name="TestClass",
            config_class_name="TestConfig",
        )

        assert registry.is_registered("test_plugin")
        assert "test_plugin" in registry.list_registered()

    def test_is_registered_false_for_unknown(self) -> None:
        """Test is_registered returns False for unknown component."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        assert not registry.is_registered("nonexistent")

    @patch("eoapi_notifier.core.registry.import_module")
    def test_get_component_class_successful(self, mock_import: Mock) -> None:
        """Test successful component class loading."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)

        # Setup mock module
        mock_module = Mock()
        mock_module.MockPlugin = MockPlugin
        mock_module.MockPluginConfig = MockPluginConfig
        mock_import.return_value = mock_module

        registry.register(
            name="mock_plugin",
            module_path="test.module",
            class_name="MockPlugin",
            config_class_name="MockPluginConfig",
        )

        component_class, config_class = registry.get_component_class("mock_plugin")
        assert component_class is MockPlugin
        assert config_class is MockPluginConfig

        # Should be cached on second call
        component_class2, config_class2 = registry.get_component_class("mock_plugin")
        assert component_class2 is MockPlugin
        assert config_class2 is MockPluginConfig
        # import_module should only be called once due to caching
        assert mock_import.call_count == 1

    def test_get_component_class_unknown(self) -> None:
        """Test getting unknown component raises error."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)

        with pytest.raises(ValueError, match="Unknown component type"):
            registry.get_component_class("nonexistent")

    @patch("eoapi_notifier.core.registry.import_module")
    def test_get_component_class_import_error(self, mock_import: Mock) -> None:
        """Test import error handling."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        mock_import.side_effect = ImportError("Module not found")

        registry.register(
            name="test_plugin",
            module_path="nonexistent.module",
            class_name="TestClass",
            config_class_name="TestConfig",
        )

        with pytest.raises(ImportError, match="Cannot import module"):
            registry.get_component_class("test_plugin")

    @patch("eoapi_notifier.core.registry.import_module")
    def test_create_component_success(self, mock_import: Mock) -> None:
        """Test successful component creation."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)

        mock_module = Mock()
        mock_module.MockPlugin = MockPlugin
        mock_module.MockPluginConfig = MockPluginConfig
        mock_import.return_value = mock_module

        registry.register(
            name="mock_plugin",
            module_path="test.module",
            class_name="MockPlugin",
            config_class_name="MockPluginConfig",
        )

        config = {"name": "test", "value": 42}
        component = registry.create_component("mock_plugin", config)

        assert isinstance(component, MockPlugin)
        assert component.config.name == "test"
        assert component.config.value == 42

    @patch("eoapi_notifier.core.registry.import_module")
    def test_create_component_invalid_config(self, mock_import: Mock) -> None:
        """Test component creation with invalid config."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)

        mock_module = Mock()
        mock_module.MockPlugin = MockPlugin
        mock_module.MockPluginConfig = MockPluginConfig
        mock_import.return_value = mock_module

        registry.register(
            name="mock_plugin",
            module_path="test.module",
            class_name="MockPlugin",
            config_class_name="MockPluginConfig",
        )

        invalid_config = {"invalid_field": "value"}

        with pytest.raises(PluginError, match="Invalid configuration"):
            registry.create_component("mock_plugin", invalid_config)


class TestBuiltinRegistries:
    """Test built-in source and output registries."""

    def test_source_registry_initialization(self) -> None:
        """Test source registry has built-in sources."""
        assert isinstance(source_registry, SourceRegistry)
        available = source_registry.list_registered()
        assert "pgstac" in available

    def test_output_registry_initialization(self) -> None:
        """Test output registry has built-in outputs."""
        assert isinstance(output_registry, OutputRegistry)
        available = output_registry.list_registered()
        assert "mqtt" in available

    def test_register_custom_source(self) -> None:
        """Test registering custom source type."""
        original_count = len(source_registry.list_registered())

        register_source(
            name="custom_source",
            module_path="custom.module",
            class_name="CustomSource",
            config_class_name="CustomSourceConfig",
        )

        assert len(source_registry.list_registered()) == original_count + 1
        assert source_registry.is_registered("custom_source")

    def test_register_custom_output(self) -> None:
        """Test registering custom output type."""
        original_count = len(output_registry.list_registered())

        register_output(
            name="custom_output",
            module_path="custom.module",
            class_name="CustomOutput",
            config_class_name="CustomOutputConfig",
        )

        assert len(output_registry.list_registered()) == original_count + 1
        assert output_registry.is_registered("custom_output")

    def test_get_available_sources(self) -> None:
        """Test getting available source types."""
        sources = get_available_sources()
        assert isinstance(sources, list)
        assert "pgstac" in sources

    def test_get_available_outputs(self) -> None:
        """Test getting available output types."""
        outputs = get_available_outputs()
        assert isinstance(outputs, list)
        assert "mqtt" in outputs

    def test_create_source_function(self) -> None:
        """Test create_source convenience function."""
        # Test that unknown source types raise ValueError
        with pytest.raises(ValueError, match="Unknown component type"):
            create_source("nonexistent_source", {})

    def test_create_output_function(self) -> None:
        """Test create_output convenience function."""
        # Test that unknown output types raise ValueError
        with pytest.raises(ValueError, match="Unknown component type"):
            create_output("nonexistent_output", {})

    def test_registry_isolation(self) -> None:
        """Test that different registry instances are isolated."""
        reg1: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        reg2: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)

        reg1.register("plugin1", "module1", "Class1", "Config1")
        reg2.register("plugin2", "module2", "Class2", "Config2")

        assert reg1.is_registered("plugin1")
        assert not reg1.is_registered("plugin2")
        assert reg2.is_registered("plugin2")
        assert not reg2.is_registered("plugin1")


class TestRegistryEdgeCases:
    """Test edge cases and error conditions."""

    @patch("eoapi_notifier.core.registry.import_module")
    def test_missing_class_in_module(self, mock_import: Mock) -> None:
        """Test handling of missing class in module."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        mock_module = Mock(spec=[])  # Empty spec means no attributes
        mock_import.return_value = mock_module

        registry.register(
            name="missing_class",
            module_path="test.module",
            class_name="MissingClass",
            config_class_name="MissingConfig",
        )

        with pytest.raises(AttributeError, match="Cannot find class"):
            registry.get_component_class("missing_class")

    @patch("eoapi_notifier.core.registry.import_module")
    def test_wrong_base_class(self, mock_import: Mock) -> None:
        """Test handling of class with wrong base class."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        mock_module = Mock()

        class WrongBaseClass:
            """Class that doesn't inherit from expected base."""

            pass

        mock_module.WrongClass = WrongBaseClass
        mock_module.WrongConfig = MockPluginConfig
        mock_import.return_value = mock_module

        registry.register(
            name="wrong_base",
            module_path="test.module",
            class_name="WrongClass",
            config_class_name="WrongConfig",
        )

        with pytest.raises(TypeError, match="is not a subclass"):
            registry.get_component_class("wrong_base")

    @patch("eoapi_notifier.core.registry.import_module")
    def test_wrong_config_base_class(self, mock_import: Mock) -> None:
        """Test handling of config class with wrong base class."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        mock_module = Mock()

        class WrongConfigClass:
            """Config class that doesn't inherit from BaseModel."""

            pass

        mock_module.GoodClass = MockPlugin
        mock_module.WrongConfig = WrongConfigClass
        mock_import.return_value = mock_module

        registry.register(
            name="wrong_config",
            module_path="test.module",
            class_name="GoodClass",
            config_class_name="WrongConfig",
        )

        with pytest.raises(TypeError, match="is not a subclass of BasePluginConfig"):
            registry.get_component_class("wrong_config")

    @patch("eoapi_notifier.core.registry.import_module")
    def test_instance_creation_failure(self, mock_import: Mock) -> None:
        """Test handling of instance creation failure."""
        registry: ComponentRegistry[MockPlugin] = ComponentRegistry(MockPlugin)
        mock_module = Mock()

        # Create a component class that raises an exception during instantiation
        class FailingPlugin(MockPlugin):
            def __init__(self, config: MockPluginConfig) -> None:
                raise RuntimeError("Instance creation failed")

        mock_module.FailingPlugin = FailingPlugin
        mock_module.MockPluginConfig = MockPluginConfig
        mock_import.return_value = mock_module

        registry.register(
            name="failing_plugin",
            module_path="test.module",
            class_name="FailingPlugin",
            config_class_name="MockPluginConfig",
        )

        with pytest.raises(PluginError, match="Failed to create instance"):
            registry.create_component("failing_plugin", {})
