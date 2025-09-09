# Development Guide

## Development Setup

```bash
uv sync --extra dev
uv run pre-commit install
uv run pre-commit run --all-files
```

## Testing

```bash
uv sync --extra test
uv run pytest
uv run pytest --cov=eoapi_notifier --cov-report=html
```

## Creating Plugins

Two plugin types:
- **Sources**: Listen for events from external systems
- **Outputs**: Send events to external systems

All plugins need:
1. Configuration class (inherits `BasePluginConfig`)
2. Plugin class (inherits `BaseSource` or `BaseOutput`)
3. Registration in the registry

### Source Plugin Example

```python
from typing import Any
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from eoapi_notifier.core.plugin import BasePluginConfig, BaseSource, PluginMetadata
from eoapi_notifier.core.event import NotificationEvent

class MySourceConfig(BasePluginConfig):
    host: str = "localhost"
    port: int = 8080
    api_key: str = ""
    poll_interval: float = 5.0

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        return PluginMetadata(
            name="mysource",
            description="Custom API polling source",
            version="1.0.0",
            category="api",
        )

class MySource(BaseSource):
    def __init__(self, config: MySourceConfig):
        super().__init__(config)
        self.config = config
        self._session = None

    async def start(self) -> None:
        # Initialize connections here
        await super().start()

    async def stop(self) -> None:
        # Cleanup connections here
        await super().stop()

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        while self._running:
            try:
                await asyncio.sleep(self.config.poll_interval)
                data = await self._fetch_data()

                if data:
                    yield NotificationEvent(
                        source=f"/my-source/{self.config.host}",
                        type="com.example.data.change",
                        operation="UPDATE",
                        collection="my_collection",
                        item_id=data.get("id"),
                        timestamp=datetime.now(UTC),
                        data=data,
                    )
            except Exception as e:
                self.logger.error(f"Listen error: {e}")
                await asyncio.sleep(1.0)

    async def _fetch_data(self) -> dict | None:
        # Your data fetching logic
        return {"id": "example", "status": "updated"}
```

### Output Plugin Example

```python
from eoapi_notifier.core.plugin import BasePluginConfig, BaseOutput, PluginMetadata
from eoapi_notifier.core.event import NotificationEvent

class MyOutputConfig(BasePluginConfig):
    webhook_url: str
    timeout: float = 30.0
    headers: dict[str, str] = {}

    @classmethod
    def get_metadata(cls) -> PluginMetadata:
        return PluginMetadata(
            name="webhook",
            description="HTTP webhook output",
            version="1.0.0",
            category="http",
        )

class MyOutput(BaseOutput):
    def __init__(self, config: MyOutputConfig):
        super().__init__(config)
        self.config = config
        self._session = None

    async def start(self) -> None:
        # Initialize HTTP session
        await super().start()

    async def stop(self) -> None:
        # Cleanup session
        await super().stop()

    async def send_event(self, event: NotificationEvent) -> bool:
        try:
            payload = {
                "id": event.id,
                "source": event.source,
                "type": event.type,
                "operation": event.operation,
                "collection": event.collection,
                "item_id": event.item_id,
                "timestamp": event.timestamp.isoformat(),
                "data": event.data,
            }

            # Send HTTP request here
            # async with self._session.post(self.config.webhook_url, json=payload) as response:
            #     response.raise_for_status()
            #     return True

            return True
        except Exception as e:
            self.logger.error(f"Send failed: {e}")
            return False
```

### Registration

Add to `eoapi_notifier/core/registry.py`:

```python
# In SourceRegistry._register_builtin_sources():
self.register(
    name="mysource",
    module_path="eoapi_notifier.sources.mysource",
    class_name="MySource",
    config_class_name="MySourceConfig",
)

# In OutputRegistry._register_builtin_outputs():
self.register(
    name="webhook",
    module_path="eoapi_notifier.outputs.webhook",
    class_name="MyOutput",
    config_class_name="MyOutputConfig",
)
```

### File Structure

```
eoapi_notifier/
├── sources/
│   └── mysource.py
├── outputs/
│   └── webhook.py
```

### Testing

```python
import pytest
from eoapi_notifier.sources.mysource import MySource, MySourceConfig

@pytest.fixture
async def source():
    config = MySourceConfig(host="localhost", api_key="test")
    source = MySource(config)
    await source.start()
    yield source
    await source.stop()

async def test_source_lifecycle(source):
    assert source.is_running
```

### Configuration

```yaml
sources:
  - type: mysource
    config:
      host: api.example.com
      api_key: your-key

outputs:
  - type: webhook
    config:
      webhook_url: https://hooks.example.com/notify
```
