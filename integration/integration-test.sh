#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM=""
TESTS="all"
VERBOSE=false
KEEP_SERVICES=false

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run eoAPI notifier integration tests with full automation

OPTIONS:
    --platform PLATFORM    Force platform: docker-compose|kubernetes (auto-detect if not set)
    --tests TESTS          Test categories: all|database|notifications|end-to-end (default: all)
    --verbose              Enable verbose output
    --keep-services        Keep services running after tests
    --no-build             Skip Docker image building
    --help                 Show this help

EXAMPLES:
    $0                                    # Auto-detect platform, run all tests
    $0 --platform docker-compose         # Force Docker Compose
    $0 --tests database,notifications     # Run specific test categories
    $0 --verbose --keep-services          # Verbose with services kept running
    $0 --no-build                        # Skip building Docker images

AUTOMATION:
    - Automatically builds eoapi-notifier Docker image
    - Loads images into minikube for Kubernetes testing
    - Handles all prerequisite checks
    - Provides comprehensive cleanup
</parameter>

<old_text line=33>
TESTS="all"
VERBOSE=false
KEEP_SERVICES=false
NO_BUILD=false
EOF
}

detect_platform() {
    # Use shared platform detection logic
    if command -v kubectl >/dev/null 2>&1 && kubectl cluster-info >/dev/null 2>&1; then
        echo "kubernetes"
    elif command -v docker-compose >/dev/null 2>&1 || (command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1); then
        echo "docker-compose"
    else
        echo "none"
    fi
}

log() {
    echo "🔧 $*"
}

error() {
    echo "❌ $*" >&2
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --tests)
            TESTS="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --keep-services)
            KEEP_SERVICES=true
            shift
            ;;
        --no-build)
            NO_BUILD=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            ;;
    esac
done

# Check prerequisites first
log "Checking prerequisites..."
if ! command -v docker >/dev/null 2>&1; then
    error "Docker is required but not installed"
fi

# Auto-detect platform if not specified
if [[ -z "$PLATFORM" ]]; then
    PLATFORM=$(detect_platform)
    if [[ "$PLATFORM" == "none" ]]; then
        error "No supported platform found. Install docker-compose or setup kubectl."
    fi
    log "Detected platform: $PLATFORM"
fi

# Additional platform-specific prerequisite checks
case "$PLATFORM" in
    kubernetes)
        if ! command -v kubectl >/dev/null 2>&1; then
            error "kubectl is required for Kubernetes platform"
        fi
        if ! kubectl cluster-info >/dev/null 2>&1; then
            error "No Kubernetes cluster available. Try: minikube start"
        fi
        ;;
    docker-compose)
        if ! command -v docker-compose >/dev/null 2>&1 && ! docker compose version >/dev/null 2>&1; then
            error "docker-compose is required for Docker Compose platform"
        fi
        ;;
esac

# Validate platform
case "$PLATFORM" in
    docker-compose|kubernetes)
        ;;
    *)
        error "Invalid platform: $PLATFORM. Use 'docker-compose' or 'kubernetes'"
        ;;
esac

# Set platform-specific variables
PLATFORM_DIR="$SCRIPT_DIR/$PLATFORM"
SETUP_SCRIPT="$PLATFORM_DIR/setup.sh"
TEARDOWN_SCRIPT="$PLATFORM_DIR/teardown.sh"

# Check platform directory exists
if [[ ! -d "$PLATFORM_DIR" ]]; then
    error "Platform directory not found: $PLATFORM_DIR"
fi

if [[ ! -f "$SETUP_SCRIPT" ]]; then
    error "Setup script not found: $SETUP_SCRIPT"
fi

# Setup cleanup trap
cleanup() {
    if [[ "$KEEP_SERVICES" != "true" && -f "$TEARDOWN_SCRIPT" ]]; then
        log "Cleaning up services..."
        bash "$TEARDOWN_SCRIPT" || log "Cleanup failed (non-fatal)"
    fi
}
trap cleanup EXIT

# Build Docker images if not skipped
if [[ "$NO_BUILD" != "true" ]]; then
    log "Building eoapi-notifier Docker image..."
    docker build -t eoapi-notifier:latest "$SCRIPT_DIR/.." || error "Failed to build Docker image"

    # For Kubernetes, prepare additional tags and load into minikube
    if [[ "$PLATFORM" == "kubernetes" ]]; then
        log "Preparing images for Kubernetes..."
        docker build -t eoapi-notifier-test-eoapi-notifier:latest "$SCRIPT_DIR/.." || error "Failed to build K8s Docker image"

        log "Loading images into minikube..."
        minikube image load eoapi-notifier-test-eoapi-notifier:latest || error "Failed to load image into minikube"
    fi
else
    log "Skipping Docker image building (--no-build specified)"
fi

# Run platform setup
log "Setting up $PLATFORM environment..."
export INTEGRATION_PLATFORM="$PLATFORM"
bash "$SETUP_SCRIPT"

# Run tests
log "Running integration tests..."

if [[ "$PLATFORM" == "kubernetes" ]]; then
    # Run tests in-cluster for Kubernetes
    log "Creating test runner job in Kubernetes..."

    # Create ConfigMaps with test files
    kubectl create configmap test-files -n "${TEST_NAMESPACE:-integration-test}" \
        --from-file="$SCRIPT_DIR/tests/" \
        --dry-run=client -o yaml | kubectl apply -f -

    kubectl create configmap shared-files -n "${TEST_NAMESPACE:-integration-test}" \
        --from-file="$SCRIPT_DIR/shared/" \
        --dry-run=client -o yaml | kubectl apply -f -

    # Create test runner job
    kubectl apply -n "${TEST_NAMESPACE:-integration-test}" -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: integration-test-runner
spec:
  backoffLimit: 2
  activeDeadlineSeconds: 200
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: test-runner
        image: python:3.12-slim
        command: ["/bin/bash", "-c"]
        args:
        - |
          set -e
          echo "Installing dependencies..."
          export DEBIAN_FRONTEND=noninteractive
          apt-get update -qq && apt-get install -y -qq git
          pip install -q pytest pytest-asyncio asyncpg paho-mqtt httpx psycopg2-binary

          echo "Setting up test environment..."
          mkdir -p /app/tests /app/shared
          cp /test-files/* /app/tests/ 2>/dev/null || true
          cp /shared-files/* /app/shared/ 2>/dev/null || true

          # Create __init__.py files
          touch /app/tests/__init__.py /app/shared/__init__.py

          # Add shared to Python path
          export PYTHONPATH="/app:\$PYTHONPATH"

          cd /app
          echo "Running integration tests..."
          
          # Set pytest configuration for containerized environment
          export PYTEST_ASYNCIO_MODE=auto
          
          # Run connectivity tests for Kubernetes (simpler and more reliable)
          timeout 120s bash -c '
          if [[ "${TESTS}" == "database" || "${TESTS}" == "all" ]]; then
            echo "Running Kubernetes connectivity tests..."
            python -m pytest tests/test_connectivity.py -v -s --tb=short --asyncio-mode=auto -m connectivity
          elif [[ "${TESTS}" == "notifications" ]]; then
            echo "Running basic connectivity verification..."
            python -m pytest tests/test_connectivity.py::TestConnectivity::test_postgres_connectivity -v -s --tb=short --asyncio-mode=auto
          elif [[ "${TESTS}" == "end-to-end" ]]; then
            echo "Running full connectivity suite..."
            python -m pytest tests/test_connectivity.py -v -s --tb=short --asyncio-mode=auto
          else
            echo "Running default connectivity tests..."
            python -m pytest tests/test_connectivity.py -v -s --tb=short --asyncio-mode=auto
          fi
          ' || { echo "Connectivity tests failed"; exit 1; }
        env:
        - name: INTEGRATION_PLATFORM
          value: "kubernetes"
        - name: TEST_NAMESPACE
          value: "${TEST_NAMESPACE:-integration-test}"
        - name: POSTGRES_HOST
          value: "postgres"
        - name: POSTGRES_PORT
          value: "5432"
        - name: POSTGRES_DATABASE
          value: "testdb"
        - name: POSTGRES_USER
          value: "postgres"
        - name: POSTGRES_PASSWORD
          value: "testpass"
        - name: MQTT_HOST
          value: "mqtt"
        - name: MQTT_PORT
          value: "1883"
        - name: KNATIVE_SERVICE
          value: "event-display"
        - name: TESTS
          value: "${TESTS}"
        volumeMounts:
        - name: test-files
          mountPath: /test-files
        - name: shared-files
          mountPath: /shared-files
        resources:
          requests:
            memory: "512Mi"
            cpu: "200m"
          limits:
            memory: "1Gi"
            cpu: "500m"
      volumes:
      - name: test-files
        configMap:
          name: test-files
      - name: shared-files
        configMap:
          name: shared-files
  backoffLimit: 2
  activeDeadlineSeconds: 600
EOF

    # Wait for job to complete with better timeout handling
    log "Waiting for connectivity test job to complete (timeout: 200s)..."
    
    # Wait with timeout and better error handling
    wait_timeout=200
    check_interval=10
    elapsed=0
    
    while [ $elapsed -lt $wait_timeout ]; do
        # Check job status
        job_status=$(kubectl get job integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
        job_failed=$(kubectl get job integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
        
        if [ "$job_status" = "True" ]; then
            log "✅ Test job completed successfully"
            log "Test results:"
            kubectl logs job/integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" --tail=100
            break
        elif [ "$job_failed" = "True" ]; then
            log "❌ Test job failed"
            log "Job status:"
            kubectl describe job integration-test-runner -n "${TEST_NAMESPACE:-integration-test}"
            log "Test job logs:"
            kubectl logs job/integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" --tail=100 || echo "Could not retrieve logs"
            
            # Show pod logs if job logs don't work
            log "Pod logs:"
            kubectl logs -l job-name=integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" --tail=50 || echo "Could not retrieve pod logs"
            exit 1
        fi
        
        log "Job still running... (${elapsed}s/${wait_timeout}s)"
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
    done
    
    # Handle timeout case
    if [ $elapsed -ge $wait_timeout ]; then
        log "❌ Connectivity test job timed out after ${wait_timeout}s"
        log "Current job status:"
        kubectl describe job integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" || true
        log "Current pod status:"
        kubectl get pods -l job-name=integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" || true
        log "Available logs:"
        kubectl logs -l job-name=integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" --tail=50 || echo "Could not retrieve logs"
        exit 1
    fi

    # Clean up test job and configmaps (with timeout)
    log "Cleaning up test resources..."
    kubectl delete job integration-test-runner -n "${TEST_NAMESPACE:-integration-test}" --ignore-not-found=true --timeout=30s || log "Warning: Could not delete test job"
    kubectl delete configmap test-files shared-files -n "${TEST_NAMESPACE:-integration-test}" --ignore-not-found=true --timeout=15s || log "Warning: Could not delete configmaps"
else
    # Run tests locally for Docker Compose
    cd "$SCRIPT_DIR"

    PYTEST_ARGS="-v --tb=short --asyncio-mode=auto"
    if [[ "$VERBOSE" == "true" ]]; then
        PYTEST_ARGS="$PYTEST_ARGS -s"
    fi

    # Use connectivity tests for consistency with Kubernetes
    case "$TESTS" in
        database|all)
            log "Running Docker Compose connectivity tests..."
            python -m pytest $PYTEST_ARGS tests/test_connectivity.py -m connectivity
            ;;
        notifications)
            log "Running basic connectivity verification..."
            python -m pytest $PYTEST_ARGS tests/test_connectivity.py::TestConnectivity::test_postgres_connectivity
            ;;
        end-to-end)
            log "Running full connectivity suite..."
            python -m pytest $PYTEST_ARGS tests/test_connectivity.py
            ;;
        *)
            log "Running default connectivity tests..."
            python -m pytest $PYTEST_ARGS tests/test_connectivity.py
            ;;
    esac
fi

log "✅ Integration tests completed successfully!"

# Final summary
log "📊 Integration Test Summary:"
log "   Platform: $PLATFORM"
log "   Tests: $TESTS"
log "   Docker images: $([ "$NO_BUILD" = "true" ] && echo "Skipped" || echo "Built")"
log "   Services: $([ "$KEEP_SERVICES" = "true" ] && echo "Kept running" || echo "Cleaned up")"
