"""Tests for eoapi_notifier module."""

from io import StringIO
from unittest.mock import patch

from eoapi_notifier import __version__, version


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
