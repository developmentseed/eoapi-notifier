#!/usr/bin/env python3
"""
Configuration template generator for integration testing.

Generates platform-specific configurations from shared templates.
"""

from typing import Any

import yaml

try:
    from .constants import (
        MQTT,
        CloudEvents,
        Database,
        Ports,
        Retries,
        Timeouts,
        get_cloudevents_endpoint,
        get_mqtt_config,
        get_postgres_config,
    )
except ImportError:
    from constants import (
        MQTT,
        CloudEvents,
        Database,
        Ports,
        Retries,
        Timeouts,
        get_cloudevents_endpoint,
        get_mqtt_config,
        get_postgres_config,
    )


class ConfigTemplate:
    """Generates platform-specific configurations from templates."""

    def __init__(self, platform: str = "docker-compose"):
        self.platform = platform
        self.postgres_config = get_postgres_config(platform)
        self.mqtt_config = get_mqtt_config(platform)
        self.cloudevents_endpoint = get_cloudevents_endpoint(platform)

    def generate_notifier_config(self) -> dict[str, Any]:
        """Generate eoapi-notifier configuration."""
        return {
            "sources": [
                {
                    "type": "pgstac",
                    "config": {
                        "host": self.postgres_config["host"],
                        "port": self.postgres_config["port"],
                        "database": self.postgres_config["dbname"],
                        "user": self.postgres_config["user"],
                        "password": self.postgres_config["password"],
                        "channel": Database.CHANNEL,
                        "event_source": CloudEvents.SOURCE,
                        "event_type": CloudEvents.ITEM_EVENT_TYPE,
                        "max_reconnect_attempts": Retries.MAX_RECONNECT,
                        "reconnect_delay": Retries.RECONNECT_DELAY,
                        "listen_timeout": Timeouts.LISTEN_TIMEOUT,
                        "enable_correlation": False,
                    },
                }
            ],
            "outputs": [
                {
                    "type": "mqtt",
                    "config": {
                        "broker_host": self.mqtt_config["host"],
                        "broker_port": self.mqtt_config["port"],
                        "topic": MQTT.TOPIC_PREFIX,
                        "qos": MQTT.QOS,
                        "timeout": Timeouts.MQTT_TIMEOUT,
                        "keepalive": MQTT.KEEPALIVE,
                        "clean_session": MQTT.CLEAN_SESSION,
                        "max_reconnect_attempts": Retries.MAX_RECONNECT,
                        "reconnect_delay": Retries.RECONNECT_DELAY,
                        "queue_size": 1000,
                    },
                },
                {
                    "type": "cloudevents",
                    "config": {
                        "endpoint": self.cloudevents_endpoint,
                        "source": CloudEvents.SOURCE,
                        "event_type": CloudEvents.EVENT_TYPE,
                        "timeout": Timeouts.HTTP_TIMEOUT,
                        "max_retries": Retries.MAX_HTTP_RETRIES,
                        "retry_backoff": Retries.RETRY_BACKOFF,
                    },
                },
            ],
        }

    def generate_mosquitto_config(self) -> str:
        """Generate Mosquitto MQTT broker configuration."""
        return f"""# Mosquitto MQTT Broker Configuration for Integration Testing
user mosquitto
max_connections -1

# Logging
log_dest stdout
log_type error
log_type warning
log_type notice
log_type information
connection_messages true
log_timestamp true

# Network
listener {Ports.MQTT}
protocol mqtt

# Security (permissive for testing)
allow_anonymous true

# Persistence
persistence true
persistence_location /mosquitto/data/
autosave_interval 300

# QoS
max_queued_messages 1000
max_inflight_messages 20

# Keep alive
max_keepalive 65535

# Clients
allow_zero_length_clientid true
auto_id_prefix auto-
"""

    def generate_postgres_env(self) -> dict[str, str]:
        """Generate PostgreSQL environment variables."""
        return {
            "POSTGRES_DB": self.postgres_config["dbname"],
            "POSTGRES_USER": self.postgres_config["user"],
            "POSTGRES_PASSWORD": self.postgres_config["password"],
            "POSTGRES_HOST_AUTH_METHOD": "trust",
            "POSTGRES_INITDB_ARGS": "--auth-host=trust --auth-local=trust",
        }

    def generate_pgstac_env(self) -> dict[str, str]:
        """Generate pgSTAC setup environment variables."""
        return {
            "PGHOST": self.postgres_config["host"],
            "PGPORT": str(self.postgres_config["port"]),
            "PGDATABASE": self.postgres_config["dbname"],
            "PGUSER": self.postgres_config["user"],
            "PGPASSWORD": self.postgres_config["password"],
        }

    def save_yaml(self, config: dict[str, Any], filename: str) -> None:
        """Save configuration as YAML file."""
        with open(filename, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    def save_text(self, content: str, filename: str) -> None:
        """Save content as text file."""
        with open(filename, "w") as f:
            f.write(content)


def generate_all_configs(platform: str, output_dir: str) -> None:
    """Generate all configuration files for a platform."""
    import os

    template = ConfigTemplate(platform)
    os.makedirs(output_dir, exist_ok=True)

    # Generate notifier config
    notifier_config = template.generate_notifier_config()
    template.save_yaml(notifier_config, f"{output_dir}/integration-config.yaml")

    # Generate mosquitto config
    mosquitto_config = template.generate_mosquitto_config()
    template.save_text(mosquitto_config, f"{output_dir}/mosquitto.conf")

    print(f"✅ Generated configs for {platform} in {output_dir}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python config_template.py <platform> <output_dir>")
        print("Platforms: docker-compose, kubernetes")
        sys.exit(1)

    platform, output_dir = sys.argv[1], sys.argv[2]
    generate_all_configs(platform, output_dir)
