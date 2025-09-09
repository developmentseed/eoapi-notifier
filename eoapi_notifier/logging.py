"""Logging utilities for eoapi_notifier using loguru."""

import sys
from typing import Any

from loguru import logger


def setup_logging(
    level: str | int = "INFO",
    format_string: str | None = None,
    include_timestamp: bool = True,
    sink: Any | None = None,
) -> Any:
    """Set up logging for the eoapi_notifier package using loguru.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL or int)
        format_string: Custom format string for log messages
        include_timestamp: Whether to include timestamp in log messages
        sink: Output sink (defaults to sys.stdout)

    Returns:
        Configured logger instance (loguru logger)
    """
    # Remove default handler
    logger.remove()

    # Set default sink
    if sink is None:
        sink = sys.stdout

    # Create default format if none provided
    if format_string is None:
        if include_timestamp:
            format_string = (
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>eoapi_notifier</cyan> | "
                "<level>{message}</level>"
            )
        else:
            format_string = (
                "<level>{level: <8}</level> | "
                "<cyan>eoapi_notifier</cyan> | "
                "<level>{message}</level>"
            )

    # Add new handler with configuration
    logger.add(
        sink,
        level=level,
        format=format_string,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )

    return logger


def get_logger(name: str | None = None) -> Any:
    """Get a logger instance for the eoapi_notifier package.

    With loguru, there's typically one global logger instance,
    but we can bind context for different modules.

    Args:
        name: Optional name for context (will be bound to logger)

    Returns:
        Logger instance with optional bound context
    """
    if name:
        return logger.bind(module=name)
    else:
        return logger.bind(module="eoapi_notifier")


# Default logger instance with basic setup
logger.remove()  # Remove default handler
logger.add(
    sys.stdout,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>eoapi_notifier</cyan> | "
        "<level>{message}</level>"
    ),
    colorize=True,
)

# Export the configured logger
__all__ = ["setup_logging", "get_logger", "logger"]
