"""
Tests for eoapi_notifier module.
"""

import re
from io import StringIO
from unittest.mock import patch


from eoapi_notifier import get_version, version, __version__


def test_get_version():
    """Test that get_version returns a valid version string."""
    ver = get_version()
    assert isinstance(ver, str)
    assert ver != ""
    # Check if it's a valid version format (e.g., "0.0.1" or "unknown")
    assert ver == "unknown" or re.match(r"\d+\.\d+\.\d+", ver)


def test_version_attribute():
    """Test that __version__ is set."""
    assert isinstance(__version__, str)
    assert __version__ != ""


def test_version_function():
    """Test that version function prints expected output."""
    with patch("sys.stdout", new=StringIO()) as fake_out:
        version()
        output = fake_out.getvalue()
        assert "Version:" in output
        assert __version__ in output
