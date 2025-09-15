#!/bin/bash

# Common utilities for integration testing

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}ℹ️  $*${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $*${NC}"
}

log_warn() {
    echo -e "${YELLOW}⚠️  $*${NC}"
}

log_error() {
    echo -e "${RED}❌ $*${NC}" >&2
}

log_step() {
    echo -e "${BLUE}🔧 $*${NC}"
}

# Wait for condition with timeout
wait_for_condition() {
    local condition_func="$1"
    local timeout="${2:-60}"
    local interval="${3:-2}"
    local desc="${4:-condition}"
    
    log_step "Waiting for $desc (timeout: ${timeout}s)..."
    
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if $condition_func; then
            log_success "$desc ready"
            return 0
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
        log_info "Waiting... (${elapsed}s/${timeout}s)"
    done
    
    log_error "$desc not ready within ${timeout}s"
    return 1
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check PostgreSQL health
check_postgres_health() {
    local host="${1:-localhost}"
    local port="${2:-5432}"
    local user="${3:-postgres}"
    local db="${4:-postgres}"
    
    pg_isready -h "$host" -p "$port" -U "$user" -d "$db" >/dev/null 2>&1
}

# Check MQTT broker health
check_mqtt_health() {
    local host="${1:-localhost}"
    local port="${2:-1883}"
    
    nc -z "$host" "$port" >/dev/null 2>&1
}

# Check HTTP endpoint health
check_http_health() {
    local url="$1"
    local expected_code="${2:-200}"
    
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
    [ "$code" = "$expected_code" ]
}

# Retry command with backoff
retry_with_backoff() {
    local max_attempts="$1"
    local delay="$2"
    shift 2
    
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        if "$@"; then
            return 0
        fi
        
        if [ $attempt -lt $max_attempts ]; then
            log_warn "Attempt $attempt failed, retrying in ${delay}s..."
            sleep "$delay"
            delay=$((delay * 2)) # Exponential backoff
        fi
        attempt=$((attempt + 1))
    done
    
    log_error "All $max_attempts attempts failed"
    return 1
}

# Get free port
get_free_port() {
    python -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'
}

# Check if port is in use
port_in_use() {
    local port="$1"
    lsof -i ":$port" >/dev/null 2>&1
}

# Clean up background processes by pattern
cleanup_processes() {
    local pattern="$1"
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        log_step "Cleaning up processes matching: $pattern"
        echo "$pids" | xargs kill -TERM 2>/dev/null || true
        sleep 2
        echo "$pids" | xargs kill -KILL 2>/dev/null || true
    fi
}