"""
CloudEvents output adapter.

Sends notification events as CloudEvents via HTTP POST.
Supports standard CloudEvents environment variables and KNative SinkBinding.
"""

import os
from typing import Any
from uuid import uuid4

import httpx
from cloudevents.conversion import to_binary
from cloudevents.http import CloudEvent
from pydantic import field_validator, model_validator

from ..core.event import NotificationEvent
from ..core.plugin import BaseOutput, BasePluginConfig, PluginMetadata


class CloudEventsConfig(BasePluginConfig):
    """Configuration for CloudEvents output adapter with environment variable
    support."""

    endpoint: str | None = None
    source: str = "/eoapi/stac"
    event_type: str = "org.eoapi.stac"
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 1.0
    max_header_length: int = 2048

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

        return self

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {
            "endpoint": None,  # Uses K_SINK env var if not set
            "source": "/eoapi/stac",
            "event_type": "org.eoapi.stac",
            "timeout": 30.0,
            "max_retries": 3,
            "retry_backoff": 1.0,
            "max_header_length": 2048,
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
            "Event Type": self.event_type,
            "Timeout": f"{self.timeout}s",
            "Max Retries": self.max_retries,
        }


class CloudEventsAdapter(BaseOutput):
    """CloudEvents HTTP output adapter."""

    def __init__(self, config: CloudEventsConfig) -> None:
        super().__init__(config)
        self.config: CloudEventsConfig = config
        self._client: httpx.AsyncClient | None = None

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
            cloud_event = self._convert_to_cloudevent(event)
            self.logger.debug(
                f"CloudEvent created: id={cloud_event['id']}, "
                f"type={cloud_event['type']}"
            )

            # Convert to binary format
            self.logger.debug("Converting CloudEvent to binary format...")
            headers, data = to_binary(cloud_event)
            self.logger.debug(
                f"Binary conversion complete, headers: {list(headers.keys())}"
            )

            # Send HTTP POST
            self.logger.debug(
                f"Sending CloudEvent {cloud_event['id']} to {endpoint} "
                f"(timeout: {self.config.timeout}s)"
            )
            response = await self._client.post(endpoint, headers=headers, data=data)
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

    def _truncate_header(self, value: str | None) -> str | None:
        """Truncate header value to max_header_length if needed."""
        if not value:
            return value
        if len(value.encode("utf-8")) <= self.config.max_header_length:
            return value
        # Truncate to byte limit, ensuring valid UTF-8
        truncated = value.encode("utf-8")[: self.config.max_header_length]
        return truncated.decode("utf-8", errors="ignore")

    def _convert_to_cloudevent(self, event: NotificationEvent) -> CloudEvent:
        """Convert NotificationEvent to CloudEvent."""
        # Use config values which now include environment overrides
        source = self.config.source
        event_type_base = self.config.event_type

        # Map operation to event type suffix
        operation_map = {"INSERT": "created", "UPDATE": "updated", "DELETE": "deleted"}
        operation = operation_map.get(event.operation.upper(), event.operation.lower())

        attributes = {
            "id": str(uuid4()),
            "source": source,
            "type": f"{event_type_base}.{operation}",
            "time": event.timestamp.isoformat(),
            "datacontenttype": "application/json",
        }

        # Add subject if item_id exists
        if event.item_id:
            truncated_subject = self._truncate_header(event.item_id)
            if truncated_subject:
                attributes["subject"] = truncated_subject

        # Add collection attribute
        if event.collection:
            truncated_collection = self._truncate_header(event.collection)
            if truncated_collection:
                attributes["collection"] = truncated_collection

        # Event data payload
        data = {
            "id": event.id,
            "source": event.source,
            "type": event.type,
            "operation": event.operation,
            "collection": event.collection,
            "item_id": event.item_id,
            "timestamp": event.timestamp.isoformat(),
            **event.data,
        }

        return CloudEvent(attributes, data)

    async def health_check(self) -> bool:
        """Check if the adapter is healthy."""
        return self._running and self._client is not None
