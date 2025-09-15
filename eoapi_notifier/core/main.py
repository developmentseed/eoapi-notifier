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
            logger.info(
                "Environment variables will be applied as overrides by individual "
                "plugins"
            )
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
        for i, source_config in enumerate(sources_config):
            source_type = source_config.get("type")
            source_config_data = source_config.get("config", {})
            if not source_type:
                logger.error(
                    f"Source configuration {i} missing 'type' field: {source_config}"
                )
                continue

            try:
                logger.debug(
                    f"Creating source {source_type} with environment variable "
                    f"overrides..."
                )
                source = create_source(source_type, source_config_data)
                self.sources.append(source)
                logger.info(f"Created source: {source_type}")
            except Exception as e:
                logger.error(f"Failed to create source {source_type}: {e}")
                logger.debug(
                    f"Source creation error details - type: {source_type}, "
                    f"config: {source_config_data}",
                    exc_info=True,
                )

        # Create outputs
        outputs_config = config.get("outputs", [])
        for i, output_config in enumerate(outputs_config):
            output_type = output_config.get("type")
            output_config_data = output_config.get("config", {})
            if not output_type:
                logger.error(
                    f"Output configuration {i} missing 'type' field: {output_config}"
                )
                continue

            try:
                logger.debug(
                    f"Creating output {output_type} with environment variable "
                    f"overrides..."
                )
                output = create_output(output_type, output_config_data)
                self.outputs.append(output)
                logger.info(f"Created output: {output_type}")
            except Exception as e:
                logger.error(f"Failed to create output {output_type}: {e}")
                logger.debug(
                    f"Output creation error details - type: {output_type}, "
                    f"config: {output_config_data}",
                    exc_info=True,
                )

    async def start_plugins(self) -> None:
        """Start all plugins."""
        logger.info(
            f"Starting {len(self.sources)} sources and {len(self.outputs)} outputs"
        )

        # Start sources
        for i, source in enumerate(self.sources):
            source_name = source.__class__.__name__
            try:
                logger.debug(
                    f"Starting source {i + 1}/{len(self.sources)}: {source_name}"
                )
                await source.start()
                logger.debug(f"✓ Successfully started source: {source_name}")
            except Exception as e:
                logger.error(
                    f"✗ Failed to start source {source_name}: {e}", exc_info=True
                )
                logger.info(f"Source {source_name} will retry connection in background")

        # Start outputs
        for i, output in enumerate(self.outputs):
            output_name = output.__class__.__name__
            try:
                logger.debug(
                    f"Starting output {i + 1}/{len(self.outputs)}: {output_name}"
                )
                await output.start()
                logger.debug(f"✓ Successfully started output: {output_name}")
            except Exception as e:
                logger.error(
                    f"✗ Failed to start output {output_name}: {e}", exc_info=True
                )
                logger.info(f"Output {output_name} will retry connection in background")

        logger.info("All plugins started successfully")

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

        logger.info(
            f"Starting event processing with {len(self.sources)} sources "
            f"and {len(self.outputs)} outputs..."
        )
        self._running = True

        # Create event processing tasks
        tasks = []
        for i, source in enumerate(self.sources):
            source_name = source.__class__.__name__
            logger.debug(
                f"Creating processing task {i + 1}/{len(self.sources)} "
                f"for source: {source_name}"
            )
            task = asyncio.create_task(self._process_source_events(source))
            tasks.append(task)

        logger.debug(
            f"Created {len(tasks)} event processing tasks, starting event loop..."
        )

        # Wait for shutdown or task completion
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"Event processing tasks completed with results: {results}")
        except asyncio.CancelledError:
            logger.info("Event processing cancelled")
        except Exception as e:
            logger.error(f"Unexpected error in event processing: {e}", exc_info=True)
        finally:
            self._running = False
            logger.info("Event processing loop stopped")

    async def _process_source_events(self, source: Any) -> None:
        """Process events from a single source."""
        source_name = source.__class__.__name__
        logger.debug(f"Starting event processing for source: {source_name}")

        event_count = 0
        last_heartbeat = asyncio.get_event_loop().time()
        heartbeat_interval = 30.0  # Log heartbeat every 30 seconds

        try:
            logger.debug(f"Calling source.listen() for {source_name}...")

            async for event in source.listen():
                event_count += 1
                current_time = asyncio.get_event_loop().time()

                # Periodic heartbeat to show we're alive
                if current_time - last_heartbeat > heartbeat_interval:
                    logger.info(
                        f"Event processing: {source_name} processed "
                        f"{event_count} events"
                    )
                    last_heartbeat = current_time

                # Check for shutdown before processing
                if self._shutdown_event.is_set():
                    logger.info(f"Shutdown requested, stopping {source_name}")
                    break

                logger.debug(
                    f"Received event #{event_count} from {source_name}: {event}"
                )

                # Send event to all outputs
                for output in self.outputs:
                    output_name = output.__class__.__name__
                    try:
                        logger.debug(f"Sending event {event.id} to {output_name}...")
                        success = await output.send_event(event)
                        if success:
                            logger.debug(
                                f"Successfully sent event {event.id} via {output_name}"
                            )
                        else:
                            logger.warning(
                                f"Failed to send event {event.id} via {output_name}"
                            )
                    except Exception as e:
                        logger.error(
                            f"Error sending event {event.id} via {output_name}: {e}",
                            exc_info=True,
                        )

        except Exception as e:
            logger.error(
                f"Error processing events from {source_name}: {e}", exc_info=True
            )
            raise  # Re-raise to ensure task failure is visible
        finally:
            if event_count > 0:
                logger.info(
                    f"Event processing summary: {source_name} "
                    f"processed {event_count} total events"
                )
            else:
                logger.debug(
                    f"Stopped processing events for source: {source_name} "
                    f"(no events processed)"
                )

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
            logger.debug("Loading configuration...")
            config = self.load_config(config_path)

            # Create plugins
            logger.debug("Creating plugins...")
            self.create_plugins(config)

            if not self.sources and not self.outputs:
                logger.error("No plugins configured")
                return

            # Setup signal handlers
            logger.debug("Setting up signal handlers...")
            self.setup_signal_handlers()

            # Start plugins
            logger.debug("Starting plugins...")
            await self.start_plugins()
            logger.info("✓ Application started successfully")

            # Process events until shutdown
            logger.debug("Beginning event processing...")
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
