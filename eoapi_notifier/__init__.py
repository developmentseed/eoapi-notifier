"""eoAPI Notifier - Message handler for eoAPI components."""

from .logging import get_logger, logger, setup_logging

__version__ = "0.0.1"


__all__ = ["__version__", "version", "get_logger", "setup_logging", "logger"]


def version() -> None:
    """Print the current version."""
    print(f"eoapi-notifier version: {__version__}")


if __name__ == "__main__":
    version()
