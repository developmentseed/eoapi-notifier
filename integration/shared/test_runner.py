#!/usr/bin/env python3
"""
Platform-agnostic test runner for eoAPI notifier integration tests.

Provides unified interfaces for tests to work on both Docker Compose and Kubernetes.
"""

import asyncio
import json
import os
import time
from abc import ABC, abstractmethod

import asyncpg
import httpx
import paho.mqtt.client as mqtt

try:
    from .constants import (
        MQTT,
        Kubernetes,
        Ports,
        Services,
        Timeouts,
        get_cloudevents_endpoint,
        get_mqtt_config,
        get_postgres_config,
    )
    from .test_data import create_test_item, get_datetime_for_collection
except ImportError:
    from constants import (
        MQTT,
        Kubernetes,
        Ports,
        Services,
        Timeouts,
        get_cloudevents_endpoint,
        get_mqtt_config,
        get_postgres_config,
    )
    from test_data import create_test_item, get_datetime_for_collection


class PlatformAdapter(ABC):
    """Abstract adapter for different deployment platforms."""

    @abstractmethod
    async def get_postgres_connection(self) -> asyncpg.Connection:
        """Get PostgreSQL connection."""
        pass

    @abstractmethod
    def get_mqtt_client(self) -> "MQTTTestClient":
        """Get MQTT test client."""
        pass

    @abstractmethod
    def get_cloudevents_client(self) -> "CloudEventsTestClient":
        """Get CloudEvents test client."""
        pass

    @abstractmethod
    async def wait_for_services(self, timeout: int = Timeouts.SERVICE_STARTUP) -> None:
        """Wait for all services to be ready."""
        pass

    @abstractmethod
    def get_service_logs(self, service: str) -> str:
        """Get logs from a service."""
        pass


class DockerComposeAdapter(PlatformAdapter):
    """Adapter for Docker Compose environment."""

    def __init__(self):
        # Use environment variables directly when running in test environments
        if os.getenv("POSTGRES_HOST"):
            self.postgres_config = {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", "5433")),
                "database": os.getenv("POSTGRES_DATABASE", "postgis"),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            }
            self.mqtt_config = {
                "host": os.getenv("MQTT_HOST", "localhost"),
                "port": int(os.getenv("MQTT_PORT", "1884")),
            }
            self.cloudevents_endpoint = os.getenv(
                "CLOUDEVENTS_ENDPOINT", "http://localhost:8080"
            )
        else:
            # Fallback to shared constants
            self.postgres_config = get_postgres_config("docker-compose")
            self.mqtt_config = get_mqtt_config("docker-compose")
            self.cloudevents_endpoint = get_cloudevents_endpoint("docker-compose")

    async def get_postgres_connection(self) -> asyncpg.Connection:
        return await asyncpg.connect(**self.postgres_config)

    def get_mqtt_client(self) -> "MQTTTestClient":
        return MQTTTestClient(**self.mqtt_config)

    def get_cloudevents_client(self) -> "CloudEventsTestClient":
        return CloudEventsTestClient(self.cloudevents_endpoint)

    async def wait_for_services(self, timeout: int = Timeouts.SERVICE_STARTUP) -> None:
        import socket

        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check PostgreSQL
            pg_ok = False
            try:
                conn = await asyncpg.connect(**self.postgres_config, timeout=5)
                await conn.close()
                pg_ok = True
            except Exception:
                pass

            # Check MQTT
            mqtt_ok = False
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                mqtt_ok = (
                    sock.connect_ex(
                        (self.mqtt_config["host"], self.mqtt_config["port"])
                    )
                    == 0
                )
                sock.close()
            except Exception:
                pass

            if pg_ok and mqtt_ok:
                return

            await asyncio.sleep(2)

        raise RuntimeError(f"Services not ready within {timeout}s")

    def get_service_logs(self, service: str) -> str:
        import subprocess

        result = subprocess.run(
            ["docker-compose", "logs", service], capture_output=True, text=True
        )
        return result.stdout


class KubernetesAdapter(PlatformAdapter):
    """Adapter for Kubernetes environment."""

    def __init__(self):
        self.namespace = os.getenv("TEST_NAMESPACE", Kubernetes.NAMESPACE)

        # Use environment variables directly when running in test containers
        if os.getenv("POSTGRES_HOST"):
            self.postgres_config = {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "dbname": os.getenv("POSTGRES_DATABASE", "testdb"),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD", "testpass"),
            }
            self.mqtt_config = {
                "host": os.getenv("MQTT_HOST", "mqtt"),
                "port": int(os.getenv("MQTT_PORT", "1883")),
            }
        else:
            # Fallback to shared constants
            self.postgres_config = get_postgres_config("kubernetes")
            self.mqtt_config = get_mqtt_config("kubernetes")

        self.cloudevents_endpoint = get_cloudevents_endpoint("kubernetes")

    async def get_postgres_connection(self) -> asyncpg.Connection:
        return await asyncpg.connect(**self.postgres_config)

    def get_mqtt_client(self) -> "MQTTTestClient":
        return MQTTTestClient(**self.mqtt_config)

    def get_cloudevents_client(self) -> "CloudEventsTestClient":
        return CloudEventsTestClient(
            self.cloudevents_endpoint, is_knative=True, namespace=self.namespace
        )

    async def wait_for_services(self, timeout: int = Timeouts.SERVICE_STARTUP) -> None:
        import socket

        start_time = asyncio.get_event_loop().time()

        # Test PostgreSQL
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                conn = await asyncpg.connect(**self.postgres_config, timeout=5)
                await conn.close()
                break
            except Exception:
                await asyncio.sleep(2)
        else:
            raise RuntimeError(f"PostgreSQL not ready within {timeout}s")

        # Test MQTT
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(
                    (self.mqtt_config["host"], self.mqtt_config["port"])
                )
                sock.close()
                if result == 0:
                    break
            except Exception:
                pass
            await asyncio.sleep(2)
        else:
            raise RuntimeError(f"MQTT not ready within {timeout}s")

    def get_service_logs(self, service: str) -> str:
        import subprocess

        result = subprocess.run(
            [
                "kubectl",
                "logs",
                "-l",
                f"app={service}",
                "-n",
                self.namespace,
                "--tail=50",
            ],
            capture_output=True,
            text=True,
        )
        return result.stdout


class MQTTTestClient:
    """MQTT client for testing."""

    def __init__(self, host: str = "localhost", port: int = Ports.MQTT):
        self.host = host
        self.port = port
        self.messages: list = []
        self.connected = False
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        self.connected = rc == 0

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            self.messages.append(
                {"topic": msg.topic, "payload": payload, "timestamp": time.time()}
            )
        except Exception as e:
            print(f"Error processing MQTT message: {e}")

    def connect(self):
        self.client.connect(self.host, self.port, MQTT.KEEPALIVE)
        self.client.loop_start()

        # Wait for connection
        for _ in range(50):
            if self.connected:
                break
            time.sleep(0.1)

        if not self.connected:
            raise RuntimeError("Failed to connect to MQTT broker")

    def subscribe(self, topic: str):
        self.client.subscribe(topic)

    def wait_for_messages(self, count: int = 1, timeout: int = 10) -> list:
        start_time = time.time()
        while len(self.messages) < count and time.time() - start_time < timeout:
            time.sleep(0.1)
        return self.messages[:count]

    def clear_messages(self):
        self.messages.clear()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
        self.connected = False


class CloudEventsTestClient:
    """CloudEvents HTTP client for testing."""

    def __init__(
        self, endpoint_url: str, is_knative: bool = False, namespace: str = None
    ):
        self.endpoint_url = endpoint_url
        self.is_knative = is_knative
        self.namespace = namespace or Kubernetes.NAMESPACE
        self.client = httpx.AsyncClient()

    async def get_health(self) -> dict:
        """Get health status of CloudEvents endpoint."""
        if self.is_knative:
            try:
                response = await self.client.get(f"{self.endpoint_url}/", timeout=5.0)
                return {"status": "ready", "knative": True}
            except Exception:
                return {"status": "unavailable", "knative": True}
        else:
            response = await self.client.get(f"{self.endpoint_url}/health", timeout=5.0)
            response.raise_for_status()
            return response.json()

    async def get_events(self) -> dict:
        """Get all received CloudEvents."""
        if self.is_knative:
            import subprocess

            try:
                result = subprocess.run(
                    [
                        "kubectl",
                        "logs",
                        "-l",
                        f"serving.knative.dev/service={Services.EVENT_DISPLAY}",
                        "-n",
                        self.namespace,
                        "--tail=50",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if result.returncode == 0:
                    events = []
                    for line in result.stdout.split("\n"):
                        if "cloudevents.Event" in line or "Event received" in line:
                            events.append(
                                {
                                    "timestamp": time.time(),
                                    "log_line": line.strip(),
                                    "source": "knative-event-display",
                                }
                            )
                    return {"count": len(events), "events": events}
                else:
                    return {"count": 0, "events": [], "error": result.stderr}
            except Exception as e:
                return {"count": 0, "events": [], "error": str(e)}
        else:
            response = await self.client.get(f"{self.endpoint_url}/events", timeout=5.0)
            response.raise_for_status()
            return response.json()

    async def clear_events(self) -> dict:
        """Clear all received CloudEvents."""
        if self.is_knative:
            return {
                "cleared": 0,
                "message": "KNative event-display logs cannot be cleared",
            }
        else:
            response = await self.client.delete(
                f"{self.endpoint_url}/events", timeout=5.0
            )
            response.raise_for_status()
            return response.json()

    async def wait_for_events(self, count: int = 1, timeout: int = 30) -> list:
        """Wait for a specific number of CloudEvents."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                events_data = await self.get_events()
                received_events = events_data.get("events", [])
                if len(received_events) >= count:
                    return received_events[:count]
            except Exception:
                pass
            await asyncio.sleep(0.5 if not self.is_knative else 2.0)
        return []

    async def is_healthy(self) -> bool:
        """Check if CloudEvents endpoint is healthy."""
        try:
            health = await self.get_health()
            return health.get("status") in ["ready", "ok"]
        except Exception:
            return False

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    def is_knative_mode(self) -> bool:
        """Check if running in KNative mode."""
        return self.is_knative


class DatabaseHelper:
    """Helper for database operations."""

    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def insert_test_item(
        self, item_id: str, collection: str = "test-collection"
    ) -> dict:
        """Insert a test item and return its data."""
        current_time = get_datetime_for_collection(collection)
        item_data = create_test_item(item_id, collection)

        await self.connection.execute(
            """
            INSERT INTO pgstac.items (id, collection, geometry, datetime,
                                    end_datetime, content)
            VALUES ($1, $2, ST_GeomFromText('POINT(0 0)', 4326), $4, $4, $3)
        """,
            item_id,
            collection,
            json.dumps(item_data),
            current_time,
        )

        return item_data

    async def update_test_item(
        self, item_id: str, collection: str = "test-collection"
    ) -> dict:
        """Update a test item and return its data."""
        current_time = get_datetime_for_collection(collection)
        item_data = create_test_item(item_id, collection)
        item_data["properties"]["updated"] = True
        item_data["properties"]["timestamp"] = int(time.time())

        await self.connection.execute(
            """
            UPDATE pgstac.items
            SET content = $3, datetime = $4, end_datetime = $4
            WHERE id = $1 AND collection = $2
        """,
            item_id,
            collection,
            json.dumps(item_data),
            current_time,
        )

        return item_data

    async def delete_test_item(self, item_id: str, collection: str = "test-collection"):
        """Delete a test item."""
        await self.connection.execute(
            """
            DELETE FROM pgstac.items WHERE id = $1 AND collection = $2
        """,
            item_id,
            collection,
        )

    async def get_collections(self) -> list:
        """Get list of collections."""
        result = await self.connection.fetch("""
            SELECT id, content->>'title' as title FROM pgstac.collections
        """)
        return [{"id": row["id"], "title": row["title"]} for row in result]

    async def create_test_collection(
        self, collection_id: str, title: str = None
    ) -> dict:
        """Create a test collection if it doesn't exist."""
        if title is None:
            title = f"Test Collection {collection_id}"

        collection_data = {
            "id": collection_id,
            "type": "Collection",
            "stac_version": "1.0.0",
            "title": title,
            "description": f"Test collection for integration testing: {collection_id}",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2023-01-01T00:00:00Z", None]]},
            },
        }

        await self.connection.execute(
            """
            INSERT INTO pgstac.collections (content) VALUES ($1)
            ON CONFLICT (id) DO NOTHING
        """,
            json.dumps(collection_data),
        )

        return collection_data


def get_platform_adapter() -> PlatformAdapter:
    """Get platform adapter based on environment."""
    platform = os.getenv("INTEGRATION_PLATFORM", "").lower()

    if platform == "kubernetes":
        return KubernetesAdapter()
    elif platform == "docker-compose":
        return DockerComposeAdapter()
    else:
        # Auto-detect
        try:
            import subprocess

            subprocess.run(["kubectl", "cluster-info"], capture_output=True, check=True)
            return KubernetesAdapter()
        except Exception:
            return DockerComposeAdapter()


# Global adapter instance for tests
_adapter: PlatformAdapter | None = None


def get_adapter() -> PlatformAdapter:
    """Get the global platform adapter instance."""
    global _adapter
    if _adapter is None:
        _adapter = get_platform_adapter()
    return _adapter
