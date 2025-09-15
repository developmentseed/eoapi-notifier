#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../shared/common.sh"

PROJECT_NAME="eoapi-notifier-test"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

log_step "Tearing down Docker Compose environment"

# Check if docker-compose is available
if command_exists docker-compose; then
    COMPOSE_CMD="docker-compose"
elif docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    log_warn "docker-compose not found, skipping cleanup"
    exit 0
fi

# Stop and remove all services
log_step "Stopping and removing containers"
$COMPOSE_CMD -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down -v --remove-orphans

# Clean up any dangling volumes
log_step "Cleaning up volumes"
docker volume prune -f >/dev/null 2>&1 || true

log_success "Docker Compose environment cleaned up"