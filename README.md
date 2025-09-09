# eoAPI Notifier

Message handler for eoAPI components. A middleware tool that listens to sources for messages and forwards them to output receivers.

## Requirements

- Python 3.12 or higher

## Installation

Install using `uv`:

```bash
uv add eoapi-notifier
```

## Testing

Install test dependencies and run tests with `uv`:

```bash
uv sync --extra test
uv run pytest
```

## Development

Install development dependencies including pre-commit:

```bash
uv sync --extra dev
```

Set up pre-commit hooks:

```bash
uv run pre-commit install
```

Run pre-commit manually:

```bash
uv run pre-commit run --all-files
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
