"""CLI entry point for eoapi-notifier."""

import asyncio
import sys
from pathlib import Path

import click
from loguru import logger

from .core.main import NotifierApp, setup_logging


@click.command()
@click.argument(
    "config",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    metavar="CONFIG_FILE",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Set logging level (default: INFO)",
)
@click.version_option(prog_name="eoapi-notifier")
def main(config: Path, log_level: str) -> None:
    """Message handler for eoAPI components.

    CONFIG_FILE: Path to YAML configuration file
    """
    # Setup logging
    setup_logging(log_level.upper())

    # Create and run application
    app = NotifierApp()

    try:
        asyncio.run(app.run(config))
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
