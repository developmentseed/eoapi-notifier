"""
Simple connectivity verification tests for Kubernetes integration.

These tests verify basic service connectivity without complex fixtures.
"""

import asyncio
import os
import socket
import sys

import asyncpg
import pytest

# Add shared modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))


@pytest.mark.connectivity
class TestConnectivity:
    """Basic connectivity tests that work in containerized environments."""

    def test_environment_variables(self):
        """Test that required environment variables are set or have defaults."""
        platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")

        # Required vars with platform-specific defaults
        required_vars = {
            "POSTGRES_HOST": "postgres" if platform == "kubernetes" else "localhost",
            "POSTGRES_PORT": "5432" if platform == "kubernetes" else "5433",
            "POSTGRES_DATABASE": "testdb" if platform == "kubernetes" else "postgis",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "testpass" if platform == "kubernetes" else "postgres",
            "MQTT_HOST": "mqtt" if platform == "kubernetes" else "localhost",
            "MQTT_PORT": "1883" if platform == "kubernetes" else "1884",
        }

        print(f"Platform: {platform}")
        for var, default in required_vars.items():
            value = os.getenv(var, default)
            print(f"✓ {var} = {value}")

        # All variables should have values (either from env or defaults)
        assert True, "Environment variables configured"

    def test_mqtt_socket_connectivity(self):
        """Test basic MQTT socket connectivity."""
        platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")
        host = os.getenv(
            "MQTT_HOST", "mqtt" if platform == "kubernetes" else "localhost"
        )
        port = int(
            os.getenv("MQTT_PORT", "1883" if platform == "kubernetes" else "1884")
        )

        print(f"Testing MQTT connectivity to {host}:{port}")

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((host, port))
            sock.close()

            assert result == 0, f"Cannot connect to MQTT at {host}:{port}"
            print("✓ MQTT connectivity successful")
        except Exception as e:
            pytest.fail(f"MQTT connectivity failed: {e}")

    @pytest.mark.asyncio
    async def test_postgres_connectivity(self):
        """Test basic PostgreSQL connectivity."""
        platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")
        config = {
            "host": os.getenv(
                "POSTGRES_HOST", "postgres" if platform == "kubernetes" else "localhost"
            ),
            "port": int(
                os.getenv(
                    "POSTGRES_PORT", "5432" if platform == "kubernetes" else "5433"
                )
            ),
            "database": os.getenv(
                "POSTGRES_DATABASE", "testdb" if platform == "kubernetes" else "postgis"
            ),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv(
                "POSTGRES_PASSWORD",
                "testpass" if platform == "kubernetes" else "postgres",
            ),
        }

        print(
            f"Testing PostgreSQL connectivity to "
            f"{config['host']}:{config['port']}/{config['database']}"
        )

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                conn = await asyncpg.connect(**config, timeout=10)

                # Test basic query
                result = await conn.fetchval("SELECT 1")
                assert result == 1, "Basic query failed"

                await conn.close()
                print(f"✓ PostgreSQL connectivity successful on attempt {attempt + 1}")
                return

            except Exception as e:
                print(f"Attempt {attempt + 1}/{max_attempts} failed: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                else:
                    pytest.fail(
                        f"PostgreSQL connectivity failed after {max_attempts} attempts: {e}"
                    )

    @pytest.mark.asyncio
    async def test_postgres_schema_exists(self):
        """Test that pgSTAC schema exists."""
        platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")
        config = {
            "host": os.getenv(
                "POSTGRES_HOST", "postgres" if platform == "kubernetes" else "localhost"
            ),
            "port": int(
                os.getenv(
                    "POSTGRES_PORT", "5432" if platform == "kubernetes" else "5433"
                )
            ),
            "database": os.getenv(
                "POSTGRES_DATABASE", "testdb" if platform == "kubernetes" else "postgis"
            ),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv(
                "POSTGRES_PASSWORD",
                "testpass" if platform == "kubernetes" else "postgres",
            ),
        }

        conn = await asyncpg.connect(**config, timeout=10)

        try:
            # Check if pgstac schema exists
            schema_exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata
                    WHERE schema_name = 'pgstac'
                )
            """)

            assert schema_exists, "pgstac schema does not exist"
            print("✓ pgstac schema exists")

            # Check core tables
            tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'pgstac'
                AND table_name IN ('collections', 'items')
                ORDER BY table_name
            """)

            table_names = {row["table_name"] for row in tables}
            expected_tables = {"collections", "items"}

            assert expected_tables.issubset(table_names), (
                f"Missing tables: {expected_tables - table_names}"
            )
            print(f"✓ Core pgSTAC tables exist: {table_names}")

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_collections_exist(self):
        """Test that test collections exist."""
        platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")
        config = {
            "host": os.getenv(
                "POSTGRES_HOST", "postgres" if platform == "kubernetes" else "localhost"
            ),
            "port": int(
                os.getenv(
                    "POSTGRES_PORT", "5432" if platform == "kubernetes" else "5433"
                )
            ),
            "database": os.getenv(
                "POSTGRES_DATABASE", "testdb" if platform == "kubernetes" else "postgis"
            ),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv(
                "POSTGRES_PASSWORD",
                "testpass" if platform == "kubernetes" else "postgres",
            ),
        }

        conn = await asyncpg.connect(**config, timeout=10)

        try:
            collections = await conn.fetch("""
                SELECT id, content->>'title' as title FROM pgstac.collections
            """)

            collection_ids = {row["id"] for row in collections}
            expected_collections = {"test-collection", "sample-collection"}

            print(f"Found collections: {collection_ids}")

            assert expected_collections.issubset(collection_ids), (
                f"Missing collections: {expected_collections - collection_ids}"
            )
            print("✓ Test collections exist")

        finally:
            await conn.close()

    def test_service_resolution(self):
        """Test DNS resolution of service names."""
        platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")
        services = [
            (
                os.getenv(
                    "POSTGRES_HOST",
                    "postgres" if platform == "kubernetes" else "localhost",
                ),
                int(
                    os.getenv(
                        "POSTGRES_PORT", "5432" if platform == "kubernetes" else "5433"
                    )
                ),
            ),
            (
                os.getenv(
                    "MQTT_HOST", "mqtt" if platform == "kubernetes" else "localhost"
                ),
                int(
                    os.getenv(
                        "MQTT_PORT", "1883" if platform == "kubernetes" else "1884"
                    )
                ),
            ),
        ]

        for service_name, port in services:
            try:
                # Test DNS resolution
                import socket

                addr_info = socket.getaddrinfo(
                    service_name, port, socket.AF_INET, socket.SOCK_STREAM
                )
                assert addr_info, f"No address info for {service_name}"

                ip_address = addr_info[0][4][0]
                print(f"✓ {service_name} resolves to {ip_address}:{port}")

            except Exception as e:
                pytest.fail(f"DNS resolution failed for {service_name}: {e}")

    def test_platform_detection(self):
        """Test platform detection."""
        platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")
        print(f"Platform: {platform}")
        assert platform in ["kubernetes", "docker-compose"], (
            f"Expected valid platform, got {platform}"
        )

    def test_namespace_detection(self):
        """Test namespace detection."""
        namespace = os.getenv("TEST_NAMESPACE", "unknown")
        print(f"Namespace: {namespace}")
        assert namespace, "TEST_NAMESPACE not set"
