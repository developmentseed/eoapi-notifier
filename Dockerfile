FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Set work directory
WORKDIR /app

# Copy all necessary files for the build
COPY pyproject.toml README.md LICENSE ./
COPY eoapi_notifier/ ./eoapi_notifier/

# Install dependencies and the package
RUN uv sync --no-dev --no-cache

# Create a non-root user
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

# Set the entry point
ENTRYPOINT ["uv", "run", "eoapi-notifier"]
CMD ["--help"]