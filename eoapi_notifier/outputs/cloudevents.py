"""
CloudEvents output adapter.

Sends notification events as CloudEvents via HTTP POST.
Supports standard CloudEvents environment variables and KNative SinkBinding.
"""

import json
import os
from typing import Any
from uuid import uuid4

import httpx
from cloudevents.conversion import to_binary
from cloudevents.http import CloudEvent
from pydantic import BaseModel, field_validator, model_validator

from ..core.event import NotificationEvent
from ..core.plugin import BaseOutput, BasePluginConfig, PluginMetadata


class RefConfig(BaseModel):
    """Kubernetes resource reference configuration."""

    apiVersion: str
    kind: str
    name: str
    namespace: str | None = None


class DestinationConfig(BaseModel):
    """Destination configuration - either ref or url."""

    ref: RefConfig | None = None
    url: str | None = None

    @model_validator(mode="after")
    def validate_mutually_exclusive(self) -> "DestinationConfig":
        if self.ref and self.url:
            raise ValueError(
                "destination.ref and destination.url are mutually exclusive"
            )
        if not self.ref and not self.url:
            raise ValueError(
                "Either destination.ref or destination.url must be specified"
            )
        return self

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v and not v.startswith(("http://", "https://")):
            raise ValueError("destination.url must start with http:// or https://")
        return v


class CloudEventsConfig(BasePluginConfig):
    """Configuration for CloudEvents output adapter with environment variable
    support."""

    destination: DestinationConfig
    source: str = "/eoapi/stac"
    event_type: str = "org.eoapi.stac"
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 1.0

    @model_validator(mode="after")
    def apply_knative_overrides(self) -> "CloudEventsConfig":
        """Apply KNative SinkBinding environment variables as special case."""
        # K_SINK overrides destination for ref-based configs (KNative SinkBinding)
        if k_sink := os.getenv("K_SINK"):
            if self.destination.ref:
                # For ref-based destinations, K_SINK provides the resolved URL
                self.destination = DestinationConfig(url=k_sink)

        # K_SOURCE overrides source
        if k_source := os.getenv("K_SOURCE"):
            self.source = k_source

        # K_TYPE overrides event_type
        if k_type := os.getenv("K_TYPE"):
            self.event_type = k_type

        return self

    @classmethod
    def get_sample_config(cls) -> dict[str, Any]:
        return {
            "destination": {
                "ref": {
                    "apiVersion": "messaging.knative.dev/v1",
                    "kind": "Broker",
                    "name": "eoapi-broker",
                    "namespace": "serverless",
                }
                # "url": "https://example.com/webhook"  # mutually exclusive with ref
            },
            "source": "/eoapi/stac",
            "event_type": "org.eoapi.stac",
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
        if self.destination.url:
            url = self.destination.url
        elif self.destination.ref:
            ref_name = f"{self.destination.ref.kind}/{self.destination.ref.name}"
            url = os.getenv("K_SINK", f"K_SINK env var -> {ref_name}")
        else:
            url = "unresolved"
        return f"POST {url}"

    def get_status_info(self) -> dict[str, Any]:
        if self.destination.url:
            endpoint_info = self.destination.url
        elif self.destination.ref:
            endpoint_info = (
                f"{self.destination.ref.kind}/{self.destination.ref.name} (via K_SINK)"
            )
        else:
            endpoint_info = "unresolved"

        return {
            "Destination": endpoint_info,
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

        # Get endpoint URL
        if self.config.destination.url:
            endpoint = self.config.destination.url
        elif self.config.destination.ref:
            k_sink = os.getenv("K_SINK")
            if not k_sink:
                raise ValueError(
                    f"K_SINK environment variable required for ref destination "
                    f"{self.config.destination.ref.kind}/{self.config.destination.ref.name}"
                )
            endpoint = k_sink
        else:
            raise ValueError("destination.ref or destination.url must be configured")

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
            # Get endpoint URL
            if self.config.destination.url:
                endpoint = self.config.destination.url
            else:
                k_sink = os.getenv("K_SINK")
                if not k_sink:
                    self.logger.error("K_SINK not available for ref destination")
                    return False
                endpoint = k_sink

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

    def _convert_to_cloudevent(self, event: NotificationEvent) -> CloudEvent:
        """Convert NotificationEvent to CloudEvent."""
        # Use config values which now include environment overrides
        source = self.config.source
        event_type_base = self.config.event_type

        # Apply KNative CE overrides if present
        ce_overrides = {}
        if k_ce_overrides := os.getenv("K_CE_OVERRIDES"):
            try:
                ce_overrides = json.loads(k_ce_overrides)
            except json.JSONDecodeError:
                self.logger.warning(
                    "Invalid K_CE_OVERRIDES JSON, ignoring: %s", k_ce_overrides
                )

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
            attributes["subject"] = event.item_id

        # Add collection attribute
        if event.collection:
            attributes["collection"] = event.collection

        # Apply KNative CE overrides
        attributes.update(ce_overrides)

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
