"""Tests for NotifierApp core functionality."""

import asyncio
import signal
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import yaml

from eoapi_notifier.core.main import NotifierApp, setup_logging


class TestSetupLogging:
    """Test setup_logging function."""

    @patch("eoapi_notifier.core.main.logger")
    def test_setup_logging_default_level(self, mock_logger: MagicMock) -> None:
        """Test setup_logging with default level."""
        setup_logging()
        mock_logger.remove.assert_called_once()
        mock_logger.add.assert_called_once()

    @patch("eoapi_notifier.core.main.logger")
    def test_setup_logging_custom_level(self, mock_logger: MagicMock) -> None:
        """Test setup_logging with custom level."""
        setup_logging("DEBUG")
        mock_logger.remove.assert_called_once()
        mock_logger.add.assert_called_once()


class TestNotifierApp:
    """Test NotifierApp functionality."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.app = NotifierApp()

    def test_initialization(self) -> None:
        """Test NotifierApp initialization."""
        assert self.app.sources == []
        assert self.app.outputs == []
        assert not self.app.is_running
        assert not self.app.is_shutdown_requested

    def test_load_config_valid_yaml(self) -> None:
        """Test loading valid YAML configuration."""
        config_data = {
            "sources": [{"type": "postgres", "config": {"host": "localhost"}}],
            "outputs": [{"type": "mqtt", "config": {"broker_host": "localhost"}}],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = Path(f.name)

        try:
            result = self.app.load_config(config_path)
            assert result == config_data
        finally:
            config_path.unlink()

    def test_load_config_nonexistent_file(self) -> None:
        """Test loading non-existent configuration file."""
        config_path = Path("nonexistent.yaml")

        with pytest.raises(FileNotFoundError):
            self.app.load_config(config_path)

    def test_load_config_invalid_yaml(self) -> None:
        """Test loading invalid YAML configuration."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [unclosed")
            config_path = Path(f.name)

        try:
            with pytest.raises(yaml.YAMLError):
                self.app.load_config(config_path)
        finally:
            config_path.unlink()

    def test_load_config_non_dict_content(self) -> None:
        """Test loading YAML that's not a dictionary."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(["not", "a", "dictionary"], f)
            config_path = Path(f.name)

        try:
            with pytest.raises(
                ValueError,
                match="Configuration file must contain a YAML object/dictionary",
            ):
                self.app.load_config(config_path)
        finally:
            config_path.unlink()

    def test_load_config_none_content(self) -> None:
        """Test loading empty YAML file (returns None)."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("")  # Empty file
            config_path = Path(f.name)

        try:
            with pytest.raises(
                ValueError,
                match="Configuration file must contain a YAML object/dictionary",
            ):
                self.app.load_config(config_path)
        finally:
            config_path.unlink()

    @patch("eoapi_notifier.core.main.create_source")
    @patch("eoapi_notifier.core.main.create_output")
    def test_create_plugins_success(
        self, mock_create_output: MagicMock, mock_create_source: MagicMock
    ) -> None:
        """Test successful plugin creation."""
        mock_source = Mock()
        mock_output = Mock()
        mock_create_source.return_value = mock_source
        mock_create_output.return_value = mock_output

        config = {
            "sources": [{"type": "postgres", "config": {"host": "localhost"}}],
            "outputs": [{"type": "mqtt", "config": {"broker_host": "localhost"}}],
        }

        self.app.create_plugins(config)

        assert len(self.app.sources) == 1
        assert len(self.app.outputs) == 1
        assert self.app.sources[0] == mock_source
        assert self.app.outputs[0] == mock_output

        mock_create_source.assert_called_once_with("postgres", {"host": "localhost"})
        mock_create_output.assert_called_once_with("mqtt", {"broker_host": "localhost"})

    @patch("eoapi_notifier.core.main.create_source")
    def test_create_plugins_missing_source_type(
        self, mock_create_source: MagicMock
    ) -> None:
        """Test plugin creation with missing source type."""
        config = {
            "sources": [{"config": {"host": "localhost"}}]  # Missing "type"
        }

        self.app.create_plugins(config)

        assert len(self.app.sources) == 0
        mock_create_source.assert_not_called()

    @patch("eoapi_notifier.core.main.create_output")
    def test_create_plugins_missing_output_type(
        self, mock_create_output: MagicMock
    ) -> None:
        """Test plugin creation with missing output type."""
        config = {
            "outputs": [{"config": {"broker_host": "localhost"}}]  # Missing "type"
        }

        self.app.create_plugins(config)

        assert len(self.app.outputs) == 0
        mock_create_output.assert_not_called()

    @patch("eoapi_notifier.core.main.create_source")
    def test_create_plugins_source_creation_error(
        self, mock_create_source: MagicMock
    ) -> None:
        """Test handling of source creation errors."""
        mock_create_source.side_effect = Exception("Creation failed")

        config = {"sources": [{"type": "postgres", "config": {"host": "localhost"}}]}

        self.app.create_plugins(config)

        assert len(self.app.sources) == 0

    @patch("eoapi_notifier.core.main.create_output")
    def test_create_plugins_output_creation_error(
        self, mock_create_output: MagicMock
    ) -> None:
        """Test handling of output creation errors."""
        mock_create_output.side_effect = Exception("Creation failed")

        config = {"outputs": [{"type": "mqtt", "config": {"broker_host": "localhost"}}]}

        self.app.create_plugins(config)

        assert len(self.app.outputs) == 0

    def test_create_plugins_empty_config(self) -> None:
        """Test plugin creation with empty configuration."""
        config: dict[str, Any] = {}

        self.app.create_plugins(config)

        assert len(self.app.sources) == 0
        assert len(self.app.outputs) == 0

    async def test_start_plugins_success(self) -> None:
        """Test successful plugin startup."""
        mock_source = AsyncMock()
        mock_output = AsyncMock()

        self.app.sources = [mock_source]
        self.app.outputs = [mock_output]

        await self.app.start_plugins()

        mock_source.start.assert_called_once()
        mock_output.start.assert_called_once()

    async def test_start_plugins_source_error(self) -> None:
        """Test plugin startup with source error."""
        mock_source = AsyncMock()
        mock_source.start.side_effect = Exception("Start failed")

        self.app.sources = [mock_source]

        await self.app.start_plugins()

        mock_source.start.assert_called_once()

    async def test_start_plugins_output_error(self) -> None:
        """Test plugin startup with output error."""
        mock_output = AsyncMock()
        mock_output.start.side_effect = Exception("Start failed")

        self.app.outputs = [mock_output]

        await self.app.start_plugins()

        mock_output.start.assert_called_once()

    async def test_stop_plugins_success(self) -> None:
        """Test successful plugin shutdown."""
        mock_source = AsyncMock()
        mock_output = AsyncMock()

        self.app.sources = [mock_source]
        self.app.outputs = [mock_output]

        await self.app.stop_plugins()

        mock_source.stop.assert_called_once()
        mock_output.stop.assert_called_once()

    async def test_stop_plugins_source_error(self) -> None:
        """Test plugin shutdown with source error."""
        mock_source = AsyncMock()
        mock_source.stop.side_effect = Exception("Stop failed")

        self.app.sources = [mock_source]

        await self.app.stop_plugins()

        mock_source.stop.assert_called_once()

    async def test_stop_plugins_output_error(self) -> None:
        """Test plugin shutdown with output error."""
        mock_output = AsyncMock()
        mock_output.stop.side_effect = Exception("Stop failed")

        self.app.outputs = [mock_output]

        await self.app.stop_plugins()

        mock_output.stop.assert_called_once()

    async def test_process_events_no_sources(self) -> None:
        """Test process_events with no sources."""
        await self.app.process_events()
        assert not self.app.is_running

    async def test_process_events_no_outputs(self) -> None:
        """Test process_events with no outputs."""
        mock_source = AsyncMock()
        self.app.sources = [mock_source]

        await self.app.process_events()
        assert not self.app.is_running

    async def test_process_events_success(self) -> None:
        """Test successful event processing."""
        # Create mock event
        mock_event = Mock()

        # Create mock source
        mock_source = Mock()
        mock_source.__class__.__name__ = "MockSource"

        # Create async generator that yields event then allows shutdown
        async def mock_listen() -> AsyncIterator[Any]:
            yield mock_event
            # Check for shutdown after yielding
            while not self.app._shutdown_event.is_set():
                await asyncio.sleep(0.01)

        mock_source.listen = mock_listen

        # Create mock output
        mock_output = AsyncMock()
        mock_output.send_event.return_value = True
        mock_output.__class__.__name__ = "MockOutput"

        self.app.sources = [mock_source]
        self.app.outputs = [mock_output]

        # Start event processing in background and trigger shutdown
        async def trigger_shutdown() -> None:
            await asyncio.sleep(0.1)
            self.app._shutdown_event.set()

        await asyncio.gather(self.app.process_events(), trigger_shutdown())

        mock_output.send_event.assert_called_with(mock_event)

    async def test_process_events_output_error(self) -> None:
        """Test event processing with output error."""
        mock_event = Mock()

        # Create mock source
        mock_source = Mock()
        mock_source.__class__.__name__ = "MockSource"

        async def mock_listen() -> AsyncIterator[Any]:
            yield mock_event
            # Check for shutdown after yielding
            while not self.app._shutdown_event.is_set():
                await asyncio.sleep(0.01)

        mock_source.listen = mock_listen

        mock_output = AsyncMock()
        mock_output.send_event.side_effect = Exception("Send failed")
        mock_output.__class__.__name__ = "MockOutput"

        self.app.sources = [mock_source]
        self.app.outputs = [mock_output]

        # Start event processing in background and trigger shutdown
        async def trigger_shutdown() -> None:
            await asyncio.sleep(0.1)
            self.app._shutdown_event.set()

        await asyncio.gather(self.app.process_events(), trigger_shutdown())

        mock_output.send_event.assert_called_with(mock_event)

    async def test_process_events_source_error(self) -> None:
        """Test event processing with source error."""
        mock_source = Mock()
        mock_source.__class__.__name__ = "MockSource"

        async def mock_listen() -> None:
            raise Exception("Listen failed")

        mock_source.listen = mock_listen

        self.app.sources = [mock_source]
        self.app.outputs = [AsyncMock()]

        await self.app.process_events()

    def test_setup_signal_handlers(self) -> None:
        """Test signal handler setup."""
        original_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
        original_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)

        try:
            self.app.setup_signal_handlers()

            # Verify handlers were set (not default)
            assert signal.signal(signal.SIGINT, signal.SIG_DFL) != signal.SIG_DFL
            assert signal.signal(signal.SIGTERM, signal.SIG_DFL) != signal.SIG_DFL
        finally:
            # Restore original handlers
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

    def test_signal_handler_functionality(self) -> None:
        """Test that signal handler sets shutdown event."""
        self.app.setup_signal_handlers()

        # Get the current handler
        handler = signal.signal(signal.SIGINT, signal.SIG_DFL)

        # Call the handler manually
        assert not self.app.is_shutdown_requested
        if callable(handler):
            handler(signal.SIGINT, None)
            assert self.app.is_shutdown_requested

    async def test_shutdown(self) -> None:
        """Test shutdown method."""
        assert not self.app.is_shutdown_requested

        await self.app.shutdown()

        assert self.app.is_shutdown_requested

    def test_is_running_property(self) -> None:
        """Test is_running property."""
        assert not self.app.is_running

        self.app._running = True
        assert self.app.is_running

        self.app._running = False
        assert not self.app.is_running

    def test_is_shutdown_requested_property(self) -> None:
        """Test is_shutdown_requested property."""
        assert not self.app.is_shutdown_requested

        self.app._shutdown_event.set()
        assert self.app.is_shutdown_requested

    async def test_run_success(self) -> None:
        """Test successful run method."""
        config_data = {"sources": [{"type": "test"}], "outputs": [{"type": "test"}]}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = Path(f.name)

        try:
            with patch.object(self.app, "create_plugins") as mock_create:
                # Simulate having plugins after create_plugins
                def setup_plugins(config: Any) -> None:
                    self.app.sources = [Mock()]
                    self.app.outputs = [Mock()]

                mock_create.side_effect = setup_plugins

                with patch.object(self.app, "setup_signal_handlers") as mock_signals:
                    with patch.object(self.app, "start_plugins") as mock_start:
                        with patch.object(self.app, "process_events") as mock_process:
                            with patch.object(self.app, "stop_plugins") as mock_stop:
                                await self.app.run(config_path)

                                mock_create.assert_called_once_with(config_data)
                                mock_signals.assert_called_once()
                                mock_start.assert_called_once()
                                mock_process.assert_called_once()
                                mock_stop.assert_called_once()
        finally:
            config_path.unlink()

    async def test_run_no_plugins(self) -> None:
        """Test run with no plugins configured."""
        config_data: dict[str, Any] = {"sources": [], "outputs": []}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = Path(f.name)

        try:
            with patch.object(self.app, "start_plugins") as mock_start:
                with patch.object(self.app, "process_events") as mock_process:
                    with patch.object(self.app, "stop_plugins") as mock_stop:
                        await self.app.run(config_path)

                        # Should not start plugins or process events if no plugins
                        mock_start.assert_not_called()
                        mock_process.assert_not_called()
                        mock_stop.assert_called_once()
        finally:
            config_path.unlink()

    async def test_run_config_load_error(self) -> None:
        """Test run with configuration loading error."""
        config_path = Path("nonexistent.yaml")

        with pytest.raises(FileNotFoundError):
            await self.app.run(config_path)

    async def test_run_keyboard_interrupt(self) -> None:
        """Test run with KeyboardInterrupt."""
        config_data: dict[str, Any] = {"sources": [], "outputs": []}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = Path(f.name)

        try:
            with patch.object(self.app, "create_plugins"):
                with patch.object(self.app, "setup_signal_handlers"):
                    with patch.object(self.app, "start_plugins"):
                        with patch.object(
                            self.app, "process_events", side_effect=KeyboardInterrupt
                        ):
                            with patch.object(self.app, "stop_plugins") as mock_stop:
                                await self.app.run(config_path)
                                mock_stop.assert_called_once()
        finally:
            config_path.unlink()

    async def test_run_application_error(self) -> None:
        """Test run with application error."""
        config_data: dict[str, Any] = {
            "sources": [{"type": "test"}],
            "outputs": [{"type": "test"}],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = Path(f.name)

        try:
            with patch.object(self.app, "create_plugins") as mock_create:
                # Simulate having plugins after create_plugins
                def setup_plugins(config: Any) -> None:
                    self.app.sources = [Mock()]
                    self.app.outputs = [Mock()]

                mock_create.side_effect = setup_plugins

                with patch.object(self.app, "setup_signal_handlers"):
                    with patch.object(
                        self.app,
                        "start_plugins",
                        side_effect=RuntimeError("Test error"),
                    ):
                        with patch.object(self.app, "stop_plugins") as mock_stop:
                            with pytest.raises(RuntimeError, match="Test error"):
                                await self.app.run(config_path)
                            mock_stop.assert_called_once()
        finally:
            config_path.unlink()

    async def test_run_ensures_cleanup(self) -> None:
        """Test run ensures cleanup even if start fails."""
        config_data = {"sources": [{"type": "test"}], "outputs": [{"type": "test"}]}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = Path(f.name)

        try:
            with patch.object(self.app, "create_plugins") as mock_create:
                # Simulate having plugins after create_plugins
                def setup_plugins(config: Any) -> None:
                    self.app.sources = [Mock()]
                    self.app.outputs = [Mock()]

                mock_create.side_effect = setup_plugins

                with patch.object(self.app, "setup_signal_handlers"):
                    with patch.object(
                        self.app, "start_plugins", side_effect=Exception("Start failed")
                    ):
                        with patch.object(self.app, "stop_plugins") as mock_stop:
                            with pytest.raises(Exception, match="Start failed"):
                                await self.app.run(config_path)
                            # Cleanup should still happen
                            mock_stop.assert_called_once()
        finally:
            config_path.unlink()
