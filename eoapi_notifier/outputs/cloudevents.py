"""
CloudEvents output adapter.

Sends notification events as CloudEvents via HTTP POST.
Supports standard CloudEvents environment variables and KNative SinkBinding.
"""

import json
import os
from typing import Any

import httpx
from cloudevents.conversion import to_structured
from pydantic import field_validator, model_validator

from ..core.event import NotificationEvent
from ..core.ogc import build_cloudevent
from ..core.plugin import BaseOutput, BasePluginConfig, PluginMetadata


class CloudEventsConfig(BasePluginConfig):
    """Configuration for CloudEvents output adapter with environment variable
    support."""

    endpoint: str | None = None
    source: str = "/eoapi/stac"
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 1.0
    overrides: dict[str, str] = {}

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v: str | None) -> str | None:
        if v and not v.startswith(("http://", "https://")):
            raise ValueError("endpoint must start with http:// or https://")
        return v

    @model_validator(mode="after")
    def apply_knative_overrides(self) -> "CloudEventsConfig":
        """Apply KNative SinkBinding environment variables as special case."""
        if k_sink := os.getenv("K_SINK"):
            self.endpoint = k_sink

        if k_ce_overrides := os.getenv("K_CE_OVERRIDES"):
            overrides_data = json.loads(k_ce_overrides)
            self.overrides = overrides_data.get("extensions", {})

        return self

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {
            "endpoint": None,  # Uses K_SINK env var if not set
            "source": "/eoapi/stac",
            "timeout": 30.0,
            "max_retries": 3,
            "retry_backoff": 1.0,
        }

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        return PluginMetadata(
            name="cloudevents",
            description="CloudEvents HTTP output adapter",
            category="http",
            tags=["cloudevents", "http", "webhook"],
            priority=10,
        )

    def get_connection_info(self) -> str:
        url = self.endpoint or os.getenv("K_SINK", "K_SINK env var")
        return f"POST {url}"

    def get_status_info(self) -> dict[str, Any]:
        return {
            "Endpoint": self.endpoint or "K_SINK env var",
            "Source": self.source,
            "Timeout": f"{self.timeout}s",
            "Max Retries": self.max_retries,
        }


class CloudEventsAdapter(BaseOutput):
    """CloudEvents HTTP output adapter."""

    def __init__(self, config: CloudEventsConfig) -> None:
        super().__init__(config)
        self.config: CloudEventsConfig = config
        self._client: httpx.AsyncClient | None = None
        # Parse K_CE_OVERRIDES once during initialization
        self._ce_extensions = self._parse_k_ce_overrides()

    def _parse_k_ce_overrides(self) -> dict[str, str]:
        """Parse K_CE_OVERRIDES environment variable once during initialization."""
        k_ce_overrides = os.getenv("K_CE_OVERRIDES")
        if not k_ce_overrides:
            return {}

        try:
            overrides_data = json.loads(k_ce_overrides)
            extensions = overrides_data.get("extensions", {})
            if isinstance(extensions, dict):
                return {str(k): str(v) for k, v in extensions.items()}
            return {}
        except json.JSONDecodeError:
            self.logger.warning(
                "Invalid K_CE_OVERRIDES JSON, ignoring: %s", k_ce_overrides
            )
            return {}

    async def start(self) -> None:
        """Start the HTTP client."""
        self.logger.info(
            f"Starting CloudEvents adapter: {self.config.get_connection_info()}"
        )
        self.logger.debug(
            f"CloudEvents config: timeout={self.config.timeout}s, "
            f"max_retries={self.config.max_retries}"
        )

        endpoint = self.config.endpoint
        if not endpoint:
            raise ValueError(
                "endpoint configuration required (can be set via config, K_SINK, "
                "or CLOUDEVENTS_ENDPOINT env vars)"
            )

        self.logger.debug(f"Step 1: Resolved endpoint: {endpoint}")

        # Create HTTP client
        self.logger.debug("Step 2: Creating HTTP client...")
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout),
            follow_redirects=True,
        )
        self.logger.debug("✓ HTTP client created")

        # Call parent start method
        self.logger.debug("Step 3: Calling parent start method...")
        await super().start()
        self.logger.debug("✓ Parent start method completed")

        self.logger.info(
            f"✅ CloudEvents adapter started successfully, "
            f"will send events to: {endpoint}"
        )

    async def stop(self) -> None:
        """Stop the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        await super().stop()
        self.logger.info("✓ CloudEvents adapter stopped")

    async def send_event(self, event: NotificationEvent) -> bool:
        """Send notification event as CloudEvent."""
        if not self._client:
            self.logger.warning(
                f"📤 HTTP client not available, cannot send event {event.id}"
            )
            return False

        try:
            endpoint = self.config.endpoint

            # Convert to CloudEvent
            self.logger.debug(f"Converting event {event.id} to CloudEvent format...")
            cloud_event = build_cloudevent(event, source=self.config.source)
            for key, value in self._ce_extensions.items():
                cloud_event[key] = str(value)
            self.logger.debug(
                f"CloudEvent created: id={cloud_event['id']}, "
                f"type={cloud_event['type']}"
            )

            # Convert to CloudEvents-JSON
            self.logger.debug("Converting CloudEvent to structured format...")
            structured_headers, structured_body = to_structured(cloud_event)
            headers = dict(structured_headers)
            self.logger.debug(
                f"Structured conversion complete, headers: {list(headers.keys())}"
            )

            # Send HTTP POST
            self.logger.debug(
                f"Sending CloudEvent {cloud_event['id']} to {endpoint} "
                f"(timeout: {self.config.timeout}s)"
            )
            response = await self._client.post(
                endpoint, headers=headers, content=structured_body
            )
            response.raise_for_status()

            self.logger.debug(
                f"✓ Successfully sent CloudEvent {cloud_event['id']} to {endpoint} "
                f"(status: {response.status_code})"
            )
            return True

        except httpx.TimeoutException as e:
            self.logger.error(
                f"✗ Timeout sending CloudEvent {event.id} to {endpoint} "
                f"(waited {self.config.timeout}s): {e}"
            )
            return False
        except httpx.HTTPStatusError as e:
            self.logger.error(
                f"✗ HTTP error sending CloudEvent {event.id} to {endpoint}: "
                f"{e.response.status_code} {e.response.reason_phrase}"
            )
            return False
        except Exception as e:
            self.logger.error(
                f"✗ Error sending CloudEvent {event.id} to {endpoint}: {e}",
                exc_info=True,
            )
            return False

    async def health_check(self) -> bool:
        """Check if the adapter is healthy."""
        return self._running and self._client is not None
