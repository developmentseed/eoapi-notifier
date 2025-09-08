"""
eoAPI Notifier - Message handler for eoAPI components.
"""

import re
from pathlib import Path

def get_version():
    """Get version from pyproject.toml."""
    try:
        pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
        content = pyproject_path.read_text()
        match = re.search(r'version\s*=\s*"([^"]+)"', content)
        return match.group(1) if match else "unknown"
    except Exception:
        return "unknown"

__version__ = get_version()

def version():
    print(f"Version: {__version__}")

if __name__ == "__main__":
    version()
