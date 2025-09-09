"""Tests for eoapi_notifier logging utilities using loguru."""

from io import StringIO
from unittest.mock import patch

from loguru import logger as loguru_logger

from eoapi_notifier.logging import get_logger, logger, setup_logging


def test_setup_logging_returns_logger() -> None:
    """Test that setup_logging returns the loguru logger."""
    test_logger = setup_logging()

    # Loguru returns the logger instance
    assert test_logger is not None
    # Should be the same as the global loguru logger
    assert test_logger == loguru_logger


def test_setup_logging_with_debug_level() -> None:
    """Test setup_logging with DEBUG level."""
    # Capture output to test if DEBUG messages would be logged
    output = StringIO()
    setup_logging(level="DEBUG", sink=output)

    # Test that we can log at DEBUG level
    loguru_logger.debug("Test debug message")

    # Check that the message was captured
    output_content = output.getvalue()
    assert "Test debug message" in output_content
    assert "DEBUG" in output_content


def test_setup_logging_with_custom_format() -> None:
    """Test setup_logging with custom format string."""
    custom_format = "{level} | {message}"
    output = StringIO()
    setup_logging(format_string=custom_format, sink=output)

    loguru_logger.info("Custom format test")

    output_content = output.getvalue()
    assert "Custom format test" in output_content
    assert "INFO" in output_content


def test_setup_logging_no_timestamp() -> None:
    """Test setup_logging without timestamp."""
    output = StringIO()
    setup_logging(include_timestamp=False, sink=output)

    loguru_logger.info("No timestamp test")

    output_content = output.getvalue()
    assert "No timestamp test" in output_content
    # Should not contain timestamp patterns (Python 3.12 improved string methods)
    assert (
        ":" not in output_content or output_content.count(":") <= 1
    )  # Only from format separators


def test_get_logger_default() -> None:
    """Test get_logger with default parameters."""
    test_logger = get_logger()

    # Should return loguru logger with binding
    assert test_logger is not None
    # Test that it can log
    with patch("sys.stdout", StringIO()):
        test_logger.info("Test message")


def test_get_logger_with_name() -> None:
    """Test get_logger with custom name."""
    test_logger = get_logger("test_module")

    # Should return loguru logger with bound context
    assert test_logger is not None
    # Test that it can log with context
    with patch("sys.stdout", StringIO()):
        test_logger.info("Test message with context")


def test_default_logger_instance() -> None:
    """Test that the default logger instance exists and works."""
    assert logger is not None

    # Test that we can use it to log by setting up a test sink
    output = StringIO()
    # Remove existing handlers and add test sink
    from loguru import logger as loguru_logger

    loguru_logger.remove()
    loguru_logger.add(output, level="INFO")

    logger.info("Default logger test")
    output_content = output.getvalue()
    assert "Default logger test" in output_content


def test_logging_integration() -> None:
    """Test that logging works end-to-end with different levels."""
    output = StringIO()
    setup_logging(level="DEBUG", sink=output)

    # Test different log levels
    loguru_logger.debug("Debug message")
    loguru_logger.info("Info message")
    loguru_logger.warning("Warning message")
    loguru_logger.error("Error message")

    output_content = output.getvalue()

    # All messages should be present
    assert "Debug message" in output_content
    assert "Info message" in output_content
    assert "Warning message" in output_content
    assert "Error message" in output_content


def test_logger_with_context() -> None:
    """Test that logger context binding works."""
    output = StringIO()
    setup_logging(sink=output)

    # Get logger with context
    contextual_logger = get_logger("test_context")
    contextual_logger.info("Message with context")

    output_content = output.getvalue()
    assert "Message with context" in output_content


def test_multiple_setup_calls() -> None:
    """Test that multiple setup calls work correctly."""
    output1 = StringIO()
    setup_logging(level="INFO", sink=output1)
    loguru_logger.info("First setup")

    output2 = StringIO()
    setup_logging(level="DEBUG", sink=output2)
    loguru_logger.debug("Second setup")

    # Each should capture their respective messages
    assert "First setup" in output1.getvalue()
    assert "Second setup" in output2.getvalue()


def test_logging_levels_hierarchy() -> None:
    """Test that logging level hierarchy works correctly."""
    output = StringIO()
    setup_logging(level="WARNING", sink=output)

    # These should not appear (below WARNING level)
    loguru_logger.debug("Debug message")
    loguru_logger.info("Info message")

    # These should appear (WARNING level and above)
    loguru_logger.warning("Warning message")
    loguru_logger.error("Error message")

    output_content = output.getvalue()

    # Only WARNING and ERROR should be present
    assert "Debug message" not in output_content
    assert "Info message" not in output_content
    assert "Warning message" in output_content
    assert "Error message" in output_content
