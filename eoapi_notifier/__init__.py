"""eoAPI Notifier - Message handler for eoAPI components."""

__version__ = "0.0.1"

__all__ = ["__version__", "version"]


def version() -> None:
    """Print the current version."""
    print(f"eoapi-notifier version: {__version__}")


if __name__ == "__main__":
    version()
