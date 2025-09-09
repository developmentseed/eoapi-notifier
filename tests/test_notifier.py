"""Tests for eoapi_notifier module."""

from io import StringIO
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

from eoapi_notifier import __version__, get_logger, logger, setup_logging, version

if TYPE_CHECKING:
    pass


def test_version_attribute() -> None:
    """Test that __version__ is set and is a valid format."""
    assert isinstance(__version__, str)
    assert __version__ != ""
    # Should follow semantic versioning pattern
    parts = __version__.split(".")
    assert len(parts) >= 3
    for part in parts[:3]:  # Major.minor.patch should be numeric
        assert part.isdigit()


def test_version_function() -> None:
    """Test that version function prints expected output."""
    with patch("sys.stdout", new=StringIO()) as fake_out:
        version()
        output = fake_out.getvalue().strip()
        assert "eoapi-notifier version:" in output
        assert __version__ in output


def test_logging_imports() -> None:
    """Test that logging utilities are properly exposed from main package."""
    # Test that functions are importable and callable
    test_logger: Any = get_logger()
    assert test_logger is not None

    configured_logger: Any = setup_logging()
    assert configured_logger is not None

    # Test that the default logger is available
    assert logger is not None

    # Test basic logging functionality
    with patch("sys.stdout", new=StringIO()):
        logger.info("Test import message")
        test_logger.info("Test get_logger message")
