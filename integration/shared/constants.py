#!/usr/bin/env python3
"""
Shared constants for integration testing.

Centralizes all repeated configuration values to follow DRY principle.
"""


# Service Configuration
class Services:
    POSTGRES_IMAGE = "postgis/postgis:16-3.4"
    MQTT_IMAGE = "eclipse-mosquitto:2.0"
    PYTHON_IMAGE = "python:3.12-slim"

    POSTGRES = "postgres"
    MQTT = "mqtt"
    CLOUDEVENTS = "cloudevents-endpoint"
    EOAPI_NOTIFIER = "eoapi-notifier"
    PGSTAC_MIGRATE = "pgstac-migrate"
    EVENT_DISPLAY = "event-display"


# Platform-specific Ports
class Ports:
    # Internal (Kubernetes)
    POSTGRES = 5432
    MQTT = 1883
    CLOUDEVENTS = 8080

    # External (Docker Compose)
    POSTGRES_EXTERNAL = 5433
    MQTT_EXTERNAL = 1884
    MQTT_WS_EXTERNAL = 9002
    CLOUDEVENTS_EXTERNAL = 8080
    PGADMIN_EXTERNAL = 8082
    MQTT_CLIENT_EXTERNAL = 8083


# Database Configuration
class Database:
    NAME = "postgis"
    NAME_K8S = "testdb"
    USER = "postgres"
    PASSWORD = "postgres"
    PASSWORD_K8S = "testpass"

    CHANNEL = "pgstac_items_change"
    SCHEMA = "pgstac"


# MQTT Configuration
class MQTT:
    TOPIC_PREFIX = "eoapi/stac/items"
    QOS = 1
    KEEPALIVE = 60
    CLEAN_SESSION = True


# CloudEvents Configuration
class CloudEvents:
    SOURCE = "/eoapi/stac/pgstac"
    EVENT_TYPE = "org.eoapi.stac"
    ITEM_EVENT_TYPE = "org.eoapi.stac.item"


# Test Data
class TestData:
    COLLECTIONS = [
        {
            "id": "test-collection",
            "title": "Test Collection",
            "description": "A test collection for integration testing",
        },
        {
            "id": "sample-collection",
            "title": "Sample Collection",
            "description": "Another collection for testing",
        },
    ]

    COLLECTION_IDS = ["test-collection", "sample-collection"]


# Timeouts and Retries
class Timeouts:
    SERVICE_STARTUP = 60
    POSTGRES_READY = 120
    MQTT_READY = 30
    CLOUDEVENTS_READY = 30
    KNATIVE_READY = 120
    JOB_COMPLETE = 300
    TEST_TIMEOUT = 600

    LISTEN_TIMEOUT = 30.0
    HTTP_TIMEOUT = 30.0
    MQTT_TIMEOUT = 30.0


class Retries:
    MAX_RECONNECT = 10
    MAX_HTTP_RETRIES = 3
    RECONNECT_DELAY = 3.0
    RETRY_BACKOFF = 1.0
    HTTP_BACKOFF = 2.0


# Kubernetes Configuration
class Kubernetes:
    NAMESPACE = "integration-test"
    PROJECT_NAME = "eoapi-notifier-test"

    LABELS = {"app": "integration-test"}


# Docker Compose Configuration
class DockerCompose:
    PROJECT_NAME = "eoapi-notifier-test"
    NETWORK = "eoapi-network"


# Resource Limits
class Resources:
    POSTGRES = {
        "requests": {"cpu": "200m", "memory": "256Mi"},
        "limits": {"cpu": "500m", "memory": "512Mi"},
    }

    MQTT = {
        "requests": {"cpu": "50m", "memory": "64Mi"},
        "limits": {"cpu": "100m", "memory": "128Mi"},
    }

    NOTIFIER = {
        "requests": {"cpu": "100m", "memory": "128Mi"},
        "limits": {"cpu": "500m", "memory": "256Mi"},
    }

    CLOUDEVENTS = {
        "requests": {"cpu": "50m", "memory": "64Mi"},
        "limits": {"cpu": "200m", "memory": "128Mi"},
    }


# SQL Queries
class SQL:
    NOTIFICATION_FUNCTION = """
    CREATE OR REPLACE FUNCTION notify_items_change_func()
    RETURNS TRIGGER AS $$
    DECLARE
    BEGIN
        PERFORM pg_notify('pgstac_items_change'::text, json_build_object(
                'operation', TG_OP,
                'items', jsonb_agg(
                    jsonb_build_object(
                        'collection', data.collection,
                        'id', data.id
                    )
                )
            )::text
            )
            FROM data
        ;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """

    NOTIFICATION_TRIGGERS = {
        "insert": """
        CREATE OR REPLACE TRIGGER notify_items_change_insert
            AFTER INSERT ON pgstac.items
            REFERENCING NEW TABLE AS data
            FOR EACH STATEMENT EXECUTE FUNCTION notify_items_change_func();
        """,
        "update": """
        CREATE OR REPLACE TRIGGER notify_items_change_update
            AFTER UPDATE ON pgstac.items
            REFERENCING NEW TABLE AS data
            FOR EACH STATEMENT EXECUTE FUNCTION notify_items_change_func();
        """,
        "delete": """
        CREATE OR REPLACE TRIGGER notify_items_change_delete
            AFTER DELETE ON pgstac.items
            REFERENCING OLD TABLE AS data
            FOR EACH STATEMENT EXECUTE FUNCTION notify_items_change_func();
        """,
    }


# Utility Functions
def get_postgres_config(platform: str = "docker-compose") -> dict:
    """Get PostgreSQL configuration for platform."""
    if platform == "kubernetes":
        return {
            "host": Services.POSTGRES,
            "port": Ports.POSTGRES,
            "dbname": Database.NAME_K8S,
            "user": Database.USER,
            "password": Database.PASSWORD_K8S,
        }
    else:
        return {
            "host": "localhost",
            "port": Ports.POSTGRES_EXTERNAL,
            "dbname": Database.NAME,
            "user": Database.USER,
            "password": Database.PASSWORD,
        }


def get_mqtt_config(platform: str = "docker-compose") -> dict:
    """Get MQTT configuration for platform."""
    if platform == "kubernetes":
        return {
            "host": Services.MQTT,
            "port": Ports.MQTT,
        }
    else:
        return {
            "host": "localhost",
            "port": Ports.MQTT_EXTERNAL,
        }


def get_cloudevents_endpoint(platform: str = "docker-compose") -> str:
    """Get CloudEvents endpoint URL for platform."""
    if platform == "kubernetes":
        return (
            f"http://{Services.EVENT_DISPLAY}.{Kubernetes.NAMESPACE}.svc.cluster.local"
        )
    else:
        return f"http://localhost:{Ports.CLOUDEVENTS_EXTERNAL}"
