#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../shared/common.sh"

NAMESPACE="integration-test"

log_step "Tearing down Kubernetes environment"

# Check if kubectl is available
if ! command_exists kubectl; then
    log_warn "kubectl not found, skipping cleanup"
    exit 0
fi

# Kill port-forwarding processes
log_step "Stopping port forwarding"
if [ -f /tmp/k8s-postgres-pf.pid ]; then
    PG_PF_PID=$(cat /tmp/k8s-postgres-pf.pid)
    kill "$PG_PF_PID" 2>/dev/null || true
    rm -f /tmp/k8s-postgres-pf.pid
fi

if [ -f /tmp/k8s-mqtt-pf.pid ]; then
    MQTT_PF_PID=$(cat /tmp/k8s-mqtt-pf.pid)
    kill "$MQTT_PF_PID" 2>/dev/null || true
    rm -f /tmp/k8s-mqtt-pf.pid
fi

# Clean up any remaining port-forward processes
cleanup_processes "kubectl.*port-forward"

# Delete namespace (this removes all resources)
log_step "Deleting namespace: $NAMESPACE"
kubectl delete namespace "$NAMESPACE" --ignore-not-found=true

# Wait for namespace deletion
log_step "Waiting for namespace deletion to complete"
while kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; do
    sleep 2
done

log_success "Kubernetes environment cleaned up"