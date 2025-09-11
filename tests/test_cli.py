"""Tests for CLI functionality."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from click.testing import CliRunner

from eoapi_notifier.cli import main


class TestCLI:
    """Test CLI functionality."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_help_flag(self) -> None:
        """Test --help flag displays help message."""
        result = self.runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Message handler for eoAPI components" in result.output
        assert "CONFIG_FILE" in result.output
        assert "--log-level" in result.output

    def test_version_flag(self) -> None:
        """Test --version flag displays version."""
        result = self.runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "eoapi-notifier, version" in result.output

    def test_missing_config_argument(self) -> None:
        """Test CLI fails when config file argument is missing."""
        result = self.runner.invoke(main, [])
        assert result.exit_code == 2
        assert "Missing argument 'CONFIG_FILE'" in result.output

    def test_nonexistent_config_file(self) -> None:
        """Test CLI fails when config file doesn't exist."""
        result = self.runner.invoke(main, ["nonexistent.yaml"])
        assert result.exit_code == 2
        assert "File 'nonexistent.yaml' does not exist" in result.output

    def test_invalid_log_level(self) -> None:
        """Test CLI fails with invalid log level."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"sources": [], "outputs": []}, f)
            config_path = f.name

        try:
            result = self.runner.invoke(main, ["--log-level", "INVALID", config_path])
            assert result.exit_code == 2
            assert "Invalid value for '--log-level'" in result.output
        finally:
            Path(config_path).unlink()

    def test_valid_log_levels(self) -> None:
        """Test all valid log levels are accepted."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"sources": [], "outputs": []}, f)
            config_path = f.name

        valid_levels = [
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "debug",
            "info",
            "warning",
            "error",
        ]

        try:
            for level in valid_levels:
                with patch("eoapi_notifier.cli.NotifierApp") as mock_app_class:
                    mock_app = AsyncMock()
                    mock_app_class.return_value = mock_app

                    # Mock asyncio.run to avoid actually running the app
                    with patch("asyncio.run") as mock_run:
                        result = self.runner.invoke(
                            main, ["--log-level", level, config_path]
                        )
                        assert result.exit_code == 0, (
                            f"Failed for log level {level}: {result.output}"
                        )
                        mock_run.assert_called_once()
        finally:
            Path(config_path).unlink()

    @patch("eoapi_notifier.cli.setup_logging")
    @patch("eoapi_notifier.cli.NotifierApp")
    @patch("asyncio.run")
    def test_successful_run(
        self,
        mock_run: MagicMock,
        mock_app_class: MagicMock,
        mock_setup_logging: MagicMock,
    ) -> None:
        """Test successful CLI run with valid configuration."""
        # Create test config file
        config_data = {
            "sources": [{"type": "pgstac", "config": {"host": "localhost"}}],
            "outputs": [{"type": "mqtt", "config": {"broker_host": "localhost"}}],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = f.name

        try:
            mock_app = AsyncMock()
            mock_app_class.return_value = mock_app

            result = self.runner.invoke(main, [config_path])

            assert result.exit_code == 0
            mock_setup_logging.assert_called_once_with("INFO")
            mock_app_class.assert_called_once()
            mock_run.assert_called_once()
        finally:
            Path(config_path).unlink()

    @patch("eoapi_notifier.cli.setup_logging")
    @patch("eoapi_notifier.cli.NotifierApp")
    @patch("asyncio.run")
    def test_debug_log_level(
        self,
        mock_run: MagicMock,
        mock_app_class: MagicMock,
        mock_setup_logging: MagicMock,
    ) -> None:
        """Test CLI with DEBUG log level."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"sources": [], "outputs": []}, f)
            config_path = f.name

        try:
            mock_app = AsyncMock()
            mock_app_class.return_value = mock_app

            result = self.runner.invoke(main, ["--log-level", "DEBUG", config_path])

            assert result.exit_code == 0
            mock_setup_logging.assert_called_once_with("DEBUG")
        finally:
            Path(config_path).unlink()

    @patch("eoapi_notifier.cli.logger")
    @patch("eoapi_notifier.cli.setup_logging")
    @patch("eoapi_notifier.cli.NotifierApp")
    @patch("asyncio.run")
    def test_keyboard_interrupt_handling(
        self,
        mock_run: MagicMock,
        mock_app_class: MagicMock,
        mock_setup_logging: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test CLI handles KeyboardInterrupt gracefully."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"sources": [], "outputs": []}, f)
            config_path = f.name

        try:
            mock_app = AsyncMock()
            mock_app_class.return_value = mock_app
            mock_run.side_effect = KeyboardInterrupt()

            result = self.runner.invoke(main, [config_path])

            assert result.exit_code == 0  # Should exit gracefully
            mock_logger.info.assert_called_with("Application interrupted by user")
        finally:
            Path(config_path).unlink()

    @patch("eoapi_notifier.cli.logger")
    @patch("eoapi_notifier.cli.setup_logging")
    @patch("eoapi_notifier.cli.NotifierApp")
    @patch("asyncio.run")
    def test_application_error_handling(
        self,
        mock_run: MagicMock,
        mock_app_class: MagicMock,
        mock_setup_logging: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test CLI handles application errors properly."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"sources": [], "outputs": []}, f)
            config_path = f.name

        try:
            mock_app = AsyncMock()
            mock_app_class.return_value = mock_app
            mock_run.side_effect = RuntimeError("Test error")

            result = self.runner.invoke(main, [config_path])

            assert result.exit_code == 1
            mock_logger.error.assert_called_with("Application failed: Test error")
        finally:
            Path(config_path).unlink()

    def test_config_path_passed_correctly(self) -> None:
        """Test that config path is passed correctly to NotifierApp."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"sources": [], "outputs": []}, f)
            config_path = f.name

        try:
            with patch("eoapi_notifier.cli.NotifierApp") as mock_app_class:
                mock_app = AsyncMock()
                mock_app_class.return_value = mock_app

                with patch("asyncio.run") as mock_run:
                    result = self.runner.invoke(main, [config_path])
                    assert result.exit_code == 0

                    # Verify the run method was called with the correct Path
                    mock_run.assert_called_once()
                    mock_app.run.assert_called_once()
                    run_call_args = mock_app.run.call_args[0][0]
                    assert isinstance(run_call_args, Path)
                    assert str(run_call_args) == config_path
        finally:
            Path(config_path).unlink()

    def test_case_insensitive_log_levels(self) -> None:
        """Test that log levels are case insensitive."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"sources": [], "outputs": []}, f)
            config_path = f.name

        try:
            test_cases = [
                ("debug", "DEBUG"),
                ("Debug", "DEBUG"),
                ("DEBUG", "DEBUG"),
                ("info", "INFO"),
                ("Info", "INFO"),
                ("INFO", "INFO"),
                ("warning", "WARNING"),
                ("Warning", "WARNING"),
                ("WARNING", "WARNING"),
                ("error", "ERROR"),
                ("Error", "ERROR"),
                ("ERROR", "ERROR"),
            ]

            for input_level, expected_level in test_cases:
                with patch("eoapi_notifier.cli.setup_logging") as mock_setup_logging:
                    with patch("eoapi_notifier.cli.NotifierApp") as mock_app_class:
                        mock_app = AsyncMock()
                        mock_app_class.return_value = mock_app

                        with patch("asyncio.run"):
                            result = self.runner.invoke(
                                main, ["--log-level", input_level, config_path]
                            )
                            assert result.exit_code == 0, f"Failed for {input_level}"
                            mock_setup_logging.assert_called_once_with(expected_level)
        finally:
            Path(config_path).unlink()

    def test_directory_as_config_rejected(self) -> None:
        """Test that directories are rejected as config files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(main, [temp_dir])
            assert result.exit_code == 2
            assert "is a directory" in result.output.lower()
