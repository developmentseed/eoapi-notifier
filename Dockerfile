# A stage rather than COPY --from=<image>, as Dependabot only reads FROM lines
FROM ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 AS uv

FROM python:3.12-slim@sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv, which the chart calls at runtime, and remove pip, which nothing does
COPY --from=uv /uv /usr/local/bin/
RUN python -m pip uninstall -y pip setuptools wheel

# Set work directory
WORKDIR /app

# Copy all necessary files for the build
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY eoapi_notifier/ ./eoapi_notifier/

# Install dependencies and the package. --frozen, not --locked: release-please
# bumps the version in pyproject.toml but not in uv.lock
RUN uv sync --frozen --no-dev --no-cache

# Create a non-root user
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

# The environment is already installed: `uv run` must not sync it when a pod starts
ENV UV_NO_SYNC=1

# Set the entry point
ENTRYPOINT ["uv", "run", "eoapi-notifier"]
CMD ["--help"]