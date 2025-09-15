#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../shared/common.sh"

# Import constants
PYTHON_CMD="python3"
if command_exists python; then
    PYTHON_CMD="python"
fi

# Get project constants
PROJECT_NAME=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import DockerCompose; print(DockerCompose.PROJECT_NAME)")
POSTGRES_PORT=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import Ports; print(Ports.POSTGRES_EXTERNAL)")
MQTT_PORT=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import Ports; print(Ports.MQTT_EXTERNAL)")
CLOUDEVENTS_PORT=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import Ports; print(Ports.CLOUDEVENTS_EXTERNAL)")

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

log_step "Setting up Docker Compose environment"

# Check prerequisites
if ! command_exists docker; then
    log_error "Docker not found"
    exit 1
fi

if command_exists docker-compose; then
    COMPOSE_CMD="docker-compose"
elif docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    log_error "docker-compose not found"
    exit 1
fi

# Generate platform-specific configs
log_step "Generating configuration files"
cd "$SCRIPT_DIR/../shared"
$PYTHON_CMD config_template.py docker-compose "$SCRIPT_DIR/config"

# Stop any existing services
log_step "Cleaning up existing services"
$COMPOSE_CMD -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down -v --remove-orphans >/dev/null 2>&1 || true

# Start infrastructure services
log_step "Starting infrastructure services"
$COMPOSE_CMD -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d postgres mqtt cloudevents-endpoint

# Wait for services using shared utilities
wait_for_condition \
    "check_postgres_health localhost $POSTGRES_PORT postgres postgis" \
    60 2 "PostgreSQL"

wait_for_condition \
    "check_mqtt_health localhost $MQTT_PORT" \
    30 2 "MQTT broker"

wait_for_condition \
    "check_http_health http://localhost:$CLOUDEVENTS_PORT/health" \
    30 2 "CloudEvents endpoint"

# Run pgSTAC setup using shared utility
log_step "Setting up pgSTAC schema and test data"
export INTEGRATION_PLATFORM="docker-compose"
cd "$SCRIPT_DIR/../shared"
$PYTHON_CMD pgstac_setup.py

if [ $? -ne 0 ]; then
    log_error "pgSTAC setup failed"
    exit 1
fi

# Start eoapi-notifier service
log_step "Starting eoapi-notifier service"
$COMPOSE_CMD -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d eoapi-notifier

# Wait for notifier to be ready
log_step "Waiting for eoapi-notifier to start"
sleep 5

# Verify notifier is running
if ! $COMPOSE_CMD -f "$COMPOSE_FILE" -p "$PROJECT_NAME" ps eoapi-notifier | grep -q "Up"; then
    log_error "eoapi-notifier failed to start"
    log_step "Showing eoapi-notifier logs:"
    $COMPOSE_CMD -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs eoapi-notifier --tail=20
    exit 1
fi

log_success "Docker Compose environment ready"
log_info "PostgreSQL: localhost:$POSTGRES_PORT"
log_info "MQTT: localhost:$MQTT_PORT" 
log_info "CloudEvents: localhost:$CLOUDEVENTS_PORT"
log_info "eoapi-notifier: Running and connected"

# Export environment variables for tests
export INTEGRATION_PLATFORM="docker-compose"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT="$POSTGRES_PORT"
export POSTGRES_DATABASE="postgis"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="postgres"
export MQTT_HOST="localhost"
export MQTT_PORT="$MQTT_PORT"
export CLOUDEVENTS_ENDPOINT="http://localhost:$CLOUDEVENTS_PORT"
export TEST_NAMESPACE="docker-compose"