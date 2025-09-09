"""Core application logic for eoapi-notifier."""

import asyncio
import signal
import sys
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from .registry import create_output, create_source


class NotifierApp:
    """Main application class for the notifier."""

    def __init__(self) -> None:
        """Initialize the application."""
        self.sources: list[Any] = []
        self.outputs: list[Any] = []
        self._shutdown_event = asyncio.Event()
        self._running = False

    def load_config(self, config_path: Path) -> dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with config_path.open() as f:
                config = yaml.safe_load(f)

            # Ensure config is a dictionary
            if not isinstance(config, dict):
                raise ValueError(
                    f"Configuration file must contain a YAML object/dictionary, "
                    f"got {type(config)}"
                )

            logger.info(f"Loaded configuration from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML configuration: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def create_plugins(self, config: dict[str, Any]) -> None:
        """Create source and output plugins from configuration."""
        # Create sources
        sources_config = config.get("sources", [])
        for source_config in sources_config:
            source_type = source_config.get("type")
            if not source_type:
                logger.error("Source configuration missing 'type' field")
                continue

            try:
                source = create_source(source_type, source_config.get("config", {}))
                self.sources.append(source)
                logger.info(f"Created source: {source_type}")
            except Exception as e:
                logger.error(f"Failed to create source {source_type}: {e}")

        # Create outputs
        outputs_config = config.get("outputs", [])
        for output_config in outputs_config:
            output_type = output_config.get("type")
            if not output_type:
                logger.error("Output configuration missing 'type' field")
                continue

            try:
                output = create_output(output_type, output_config.get("config", {}))
                self.outputs.append(output)
                logger.info(f"Created output: {output_type}")
            except Exception as e:
                logger.error(f"Failed to create output {output_type}: {e}")

    async def start_plugins(self) -> None:
        """Start all plugins."""
        # Start sources
        for source in self.sources:
            try:
                await source.start()
                logger.info(f"Started source: {source.__class__.__name__}")
            except Exception as e:
                logger.error(f"Failed to start source {source.__class__.__name__}: {e}")

        # Start outputs
        for output in self.outputs:
            try:
                await output.start()
                logger.info(f"Started output: {output.__class__.__name__}")
            except Exception as e:
                logger.error(f"Failed to start output {output.__class__.__name__}: {e}")

    async def stop_plugins(self) -> None:
        """Stop all plugins."""
        # Stop sources
        for source in self.sources:
            try:
                await source.stop()
                logger.info(f"Stopped source: {source.__class__.__name__}")
            except Exception as e:
                logger.error(f"Failed to stop source {source.__class__.__name__}: {e}")

        # Stop outputs
        for output in self.outputs:
            try:
                await output.stop()
                logger.info(f"Stopped output: {output.__class__.__name__}")
            except Exception as e:
                logger.error(f"Failed to stop output {output.__class__.__name__}: {e}")

    async def process_events(self) -> None:
        """Main event processing loop."""
        if not self.sources:
            logger.warning("No sources configured")
            return

        if not self.outputs:
            logger.warning("No outputs configured")
            return

        logger.info("Starting event processing...")
        self._running = True

        # Create event processing tasks
        tasks = []
        for source in self.sources:
            task = asyncio.create_task(self._process_source_events(source))
            tasks.append(task)

        # Wait for shutdown or task completion
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Event processing cancelled")
        finally:
            self._running = False

    async def _process_source_events(self, source: Any) -> None:
        """Process events from a single source."""
        source_name = source.__class__.__name__
        logger.debug(f"Starting event processing for source: {source_name}")

        try:
            async for event in source.listen():
                # Check for shutdown before processing
                if self._shutdown_event.is_set():
                    logger.debug(f"Shutdown requested, stopping {source_name}")
                    break

                logger.debug(f"Received event from {source_name}: {event}")

                # Send event to all outputs
                for output in self.outputs:
                    try:
                        success = await output.send_event(event)
                        if success:
                            logger.debug(
                                f"Successfully sent event via "
                                f"{output.__class__.__name__}"
                            )
                        else:
                            logger.warning(
                                f"Failed to send event via {output.__class__.__name__}"
                            )
                    except Exception as e:
                        logger.error(
                            f"Error sending event via {output.__class__.__name__}: {e}"
                        )

        except Exception as e:
            logger.error(f"Error processing events from {source_name}: {e}")
        finally:
            logger.debug(f"Stopped processing events for source: {source_name}")

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum: int, frame: Any) -> None:
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def shutdown(self) -> None:
        """Initiate graceful shutdown."""
        logger.info("Shutting down application...")
        self._shutdown_event.set()

        # Give running tasks a moment to see the shutdown event
        await asyncio.sleep(0.1)

    @property
    def is_running(self) -> bool:
        """Check if the application is running."""
        return self._running

    @property
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_event.is_set()

    async def run(self, config_path: Path) -> None:
        """Run the application with the given configuration."""
        try:
            # Load configuration
            config = self.load_config(config_path)

            # Create plugins
            self.create_plugins(config)

            if not self.sources and not self.outputs:
                logger.error("No plugins configured")
                return

            # Setup signal handlers
            self.setup_signal_handlers()

            # Start plugins
            await self.start_plugins()
            logger.info("Application started successfully")

            # Process events until shutdown
            await self.process_events()

        except KeyboardInterrupt:
            logger.info("Application interrupted by user")
        except Exception as e:
            logger.error(f"Application error: {e}")
            raise
        finally:
            # Stop plugins
            await self.stop_plugins()
            logger.info("Application stopped")


def setup_logging(level: str = "INFO") -> None:
    """Configure application logging."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )
