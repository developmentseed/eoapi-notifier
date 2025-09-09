# Contributing

## Setup

```bash
git clone https://github.com/developmentseed/eoapi-notifier.git
cd eoapi-notifier
uv sync
uv run pre-commit install
```

## Testing

```bash
# Run tests
uv run pytest

# Test Docker
docker build -t eoapi-notifier:test .
docker run --rm eoapi-notifier:test --help

# Test Helm chart
helm lint helm-chart/eoapi-notifier
helm template test helm-chart/eoapi-notifier

# Run Helm chart tests (requires running Kubernetes cluster)
helm install test-release helm-chart/eoapi-notifier
helm test test-release
helm uninstall test-release
```

## Releases

Create and push a tag to trigger automated release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

This automatically:
- Builds and pushes Docker image to `ghcr.io/developmentseed/eoapi-notifier`
- Publishes Helm chart to `oci://ghcr.io/developmentseed/charts/eoapi-notifier`

## Code Style

- Use `uv run ruff format` and `uv run ruff check --fix`
- Add type hints for new code
- Pre-commit hooks enforce style

## Pull Requests

1. Create feature branch
2. Make changes with tests
3. Ensure `uv run pytest` and pre-commit hooks pass
4. Submit PR with clear description