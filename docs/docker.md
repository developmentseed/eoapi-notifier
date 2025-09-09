## Docker

The application is available as a Docker image hosted on GitHub Container Registry (GHCR). Docker images are automatically built and published when new releases are created.

### Pull the Image

```bash
# Pull the latest version
docker pull ghcr.io/developmentseed/eoapi-notifier:latest

# Pull a specific version (replace v1.0.0 with desired version)
docker pull ghcr.io/developmentseed/eoapi-notifier:v1.0.0
```

### Running with Docker

**Show help:**
```bash
docker run --rm ghcr.io/developmentseed/eoapi-notifier:latest
```

**Show version:**
```bash
docker run --rm ghcr.io/developmentseed/eoapi-notifier:latest --version
```

**Run with a configuration file:**
```bash
docker run --rm -v /path/to/your/config.yaml:/app/config.yaml \
  ghcr.io/developmentseed/eoapi-notifier:latest config.yaml
```

**Run with custom log level:**
```bash
docker run --rm -v /path/to/your/config.yaml:/app/config.yaml \
  ghcr.io/developmentseed/eoapi-notifier:latest --log-level DEBUG config.yaml
```

### Building Locally

If you prefer to build the Docker image locally:

```bash
docker build -t eoapi-notifier .
docker run --rm eoapi-notifier --help
```

The Docker image is based on `python:3.12-slim` and includes all necessary dependencies. The image runs as a non-root user for security.
