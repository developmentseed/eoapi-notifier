"""
Shared pytest configuration for integration tests.
"""

import asyncio
import os
import sys
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

# Add integration directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.test_runner import DatabaseHelper, get_adapter


def pytest_configure(config):
    """Configure pytest markers and asyncio mode."""
    config.addinivalue_line("markers", "database: marks tests as database tests")
    config.addinivalue_line(
        "markers", "notifications: marks tests as notification tests"
    )
    config.addinivalue_line("markers", "end_to_end: marks tests as end-to-end tests")
    # Set asyncio mode to auto to handle async fixtures properly
    config.option.asyncio_mode = "auto"


@pytest.fixture(scope="session")
def event_loop():
    """Create session-wide event loop."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def platform_adapter():
    """Platform adapter for the test session."""
    adapter = get_adapter()
    await adapter.wait_for_services()
    return adapter


@pytest_asyncio.fixture
async def postgres_connection(platform_adapter) -> AsyncGenerator:
    """PostgreSQL connection fixture."""
    conn = await platform_adapter.get_postgres_connection()
    yield conn
    await conn.close()


@pytest.fixture
def mqtt_client(platform_adapter):
    """MQTT client fixture."""
    client = platform_adapter.get_mqtt_client()
    client.connect()
    client.subscribe("eoapi/stac/items/+")
    yield client
    client.disconnect()


@pytest_asyncio.fixture
async def cloudevents_client(platform_adapter):
    """CloudEvents client fixture."""
    client = platform_adapter.get_cloudevents_client()
    yield client
    await client.close()


@pytest_asyncio.fixture
async def db_helper(postgres_connection):
    """Database helper fixture."""
    return DatabaseHelper(postgres_connection)


@pytest.fixture(autouse=True)
def test_cleanup():
    """Auto cleanup fixture."""
    yield
    # Any cleanup needed after each test
