"""eoAPI Notifier - Message handler for eoAPI components."""

from importlib.metadata import version as _version

from .core.main import NotifierApp, setup_logging
from .core.registry import (
    create_output,
    create_source,
    get_available_outputs,
    get_available_sources,
    register_output,
    register_source,
)
from .logging import get_logger, logger

__version__ = _version("eoapi-notifier")


__all__ = [
    "__version__",
    "version",
    "get_logger",
    "setup_logging",
    "logger",
    "NotifierApp",
    "create_source",
    "create_output",
    "register_source",
    "register_output",
    "get_available_sources",
    "get_available_outputs",
]


def version() -> None:
    """Print the current version."""
    print(f"eoapi-notifier version: {__version__}")


if __name__ == "__main__":
    version()
