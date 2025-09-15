#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../shared/common.sh"

# Import constants
PYTHON_CMD="python3"
if command_exists python; then
    PYTHON_CMD="python"
fi

# Get constants from shared module
NAMESPACE=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import Kubernetes; print(Kubernetes.NAMESPACE)")
POSTGRES_IMAGE=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import Services; print(Services.POSTGRES_IMAGE)")
MQTT_IMAGE=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import Services; print(Services.MQTT_IMAGE)")
PYTHON_IMAGE=$($PYTHON_CMD -c "import sys; sys.path.append('$SCRIPT_DIR/../shared'); from constants import Services; print(Services.PYTHON_IMAGE)")

log_step "Setting up Kubernetes environment"

# Check prerequisites
if ! command_exists kubectl; then
    log_error "kubectl not found"
    exit 1
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
    log_error "No Kubernetes cluster available"
    exit 1
fi

# Create namespace
log_step "Creating namespace: $NAMESPACE"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Install KNative Serving if needed
log_step "Installing KNative Serving"
if ! kubectl get crd services.serving.knative.dev >/dev/null 2>&1; then
    log_step "Installing KNative Serving CRDs..."
    kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.15.0/serving-crds.yaml

    log_step "Installing KNative Serving Core..."
    kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.15.0/serving-core.yaml

    log_step "Waiting for KNative Serving to be ready..."
    kubectl wait --for=condition=ready pod -l app=controller -n knative-serving --timeout=300s
    kubectl wait --for=condition=ready pod -l app=activator -n knative-serving --timeout=300s

    log_step "Installing Kourier networking layer..."
    kubectl apply -f https://github.com/knative/net-kourier/releases/download/knative-v1.15.0/kourier.yaml

    kubectl patch configmap/config-network \
        --namespace knative-serving \
        --type merge \
        --patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'

    log_step "Waiting for Kourier to be ready..."
    kubectl wait --for=condition=ready pod -l app=3scale-kourier-gateway -n kourier-system --timeout=300s
else
    log_step "KNative Serving already installed"
fi

# Generate platform-specific configs
log_step "Generating configuration files"
cd "$SCRIPT_DIR/../shared"
mkdir -p /tmp/k8s-configs
$PYTHON_CMD config_template.py kubernetes /tmp/k8s-configs

# Deploy services using shared constants
log_step "Deploying infrastructure services"
kubectl apply -n "$NAMESPACE" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: mosquitto-config
data:
  mosquitto.conf: |
$(cat /tmp/k8s-configs/mosquitto.conf | sed 's/^/    /')
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: $POSTGRES_IMAGE
        env:
        - name: POSTGRES_PASSWORD
          value: testpass
        - name: POSTGRES_DB
          value: testdb
        - name: POSTGRES_USER
          value: postgres
        ports:
        - containerPort: 5432
        resources:
          requests:
            cpu: 200m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector:
    app: postgres
  ports:
  - port: 5432
    targetPort: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mqtt
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mqtt
  template:
    metadata:
      labels:
        app: mqtt
    spec:
      containers:
      - name: mqtt
        image: $MQTT_IMAGE
        command: [mosquitto, -c, /mosquitto/config/mosquitto.conf]
        ports:
        - containerPort: 1883
        volumeMounts:
        - name: config
          mountPath: /mosquitto/config
        resources:
          requests:
            cpu: 50m
            memory: 64Mi
          limits:
            cpu: 100m
            memory: 128Mi
      volumes:
      - name: config
        configMap:
          name: mosquitto-config
---
apiVersion: v1
kind: Service
metadata:
  name: mqtt
spec:
  selector:
    app: mqtt
  ports:
  - port: 1883
    targetPort: 1883
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cloudevents
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cloudevents
  template:
    metadata:
      labels:
        app: cloudevents
    spec:
      containers:
      - name: cloudevents
        image: $PYTHON_IMAGE
        command:
        - python
        - -c
        - |
          import json, time
          from http.server import HTTPServer, BaseHTTPRequestHandler
          events = []
          class Handler(BaseHTTPRequestHandler):
              def do_POST(self):
                  if self.path in ['/', '/events']:
                      length = int(self.headers.get('content-length', 0))
                      data = self.rfile.read(length) if length else b''
                      events.append({'timestamp': time.time(), 'headers': dict(self.headers), 'data': data.decode() if data else ''})
                      print(f'Received event: {len(events)} total')
                      self.send_response(202)
                      self.end_headers()
                      self.wfile.write(b'OK')
                  else:
                      self.send_response(404)
                      self.end_headers()
              def do_GET(self):
                  if self.path == '/health':
                      self.send_response(200)
                      self.send_header('Content-Type', 'application/json')
                      self.end_headers()
                      self.wfile.write(json.dumps({'status': 'ready', 'events_received': len(events)}).encode())
                  elif self.path == '/events':
                      self.send_response(200)
                      self.send_header('Content-Type', 'application/json')
                      self.end_headers()
                      self.wfile.write(json.dumps({'count': len(events), 'events': events}).encode())
                  else:
                      self.send_response(404)
                      self.end_headers()
          print('CloudEvents endpoint starting on :8080')
          HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
        ports:
        - containerPort: 8080
        resources:
          requests:
            cpu: 50m
            memory: 64Mi
          limits:
            cpu: 100m
            memory: 128Mi
---
apiVersion: v1
kind: Service
metadata:
  name: cloudevents
spec:
  selector:
    app: cloudevents
  ports:
  - port: 8080
    targetPort: 8080
EOF

# Wait for services
log_step "Waiting for services to be ready"
kubectl wait --for=condition=available deployment/postgres -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=available deployment/mqtt -n "$NAMESPACE" --timeout=60s
kubectl wait --for=condition=available deployment/cloudevents -n "$NAMESPACE" --timeout=60s

# Create KNative service
log_step "Creating KNative event-display service"
kubectl apply -n "$NAMESPACE" -f - <<EOF
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: event-display
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "1"
        autoscaling.knative.dev/maxScale: "1"
    spec:
      containers:
      - name: event-display
        image: gcr.io/knative-releases/knative.dev/eventing-contrib/cmd/event_display:latest
        ports:
        - containerPort: 8080
        env:
        - name: _KO_DATA_PATH
          value: /var/run/ko
        resources:
          requests:
            cpu: 50m
            memory: 64Mi
          limits:
            cpu: 200m
            memory: 128Mi
EOF

kubectl wait --for=condition=ready ksvc/event-display -n "$NAMESPACE" --timeout=120s

# Set up pgSTAC using Kubernetes job
log_step "Setting up pgSTAC"

# Create ConfigMap with shared utilities
kubectl create configmap pgstac-setup-scripts -n "$NAMESPACE" \
    --from-file="$SCRIPT_DIR/../shared/" \
    --dry-run=client -o yaml | kubectl apply -f -

# Run pgSTAC setup job
kubectl apply -n "$NAMESPACE" -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: pgstac-setup
spec:
  template:
    spec:
      restartPolicy: Never
      initContainers:
      - name: wait-for-postgres
        image: postgres:16-alpine
        env:
        - name: PGHOST
          value: postgres
        - name: PGPORT
          value: "5432"
        - name: PGUSER
          value: postgres
        - name: PGDATABASE
          value: testdb
        - name: PGPASSWORD
          value: testpass
        command:
        - sh
        - -c
        - |
          echo "Waiting for PostgreSQL..."
          until pg_isready; do
            echo "PostgreSQL not ready, waiting..."
            sleep 2
          done
          echo "PostgreSQL ready!"
      containers:
      - name: pgstac-setup
        image: $PYTHON_IMAGE
        env:
        - name: INTEGRATION_PLATFORM
          value: "kubernetes"
        - name: POSTGRES_HOST
          value: postgres
        - name: POSTGRES_PORT
          value: "5432"
        - name: POSTGRES_DATABASE
          value: testdb
        - name: POSTGRES_USER
          value: postgres
        - name: POSTGRES_PASSWORD
          value: testpass
        - name: PYTHONPATH
          value: "/scripts"
        command:
        - bash
        - -c
        - |
          set -e
          echo "Installing dependencies..."
          pip install -q pypgstac[psycopg] psycopg
          
          echo "Running pgSTAC setup..."
          cd /scripts
          python pgstac_setup.py
        volumeMounts:
        - name: scripts
          mountPath: /scripts
        resources:
          requests:
            cpu: 200m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
      volumes:
      - name: scripts
        configMap:
          name: pgstac-setup-scripts
  backoffLimit: 3
  activeDeadlineSeconds: 600
EOF

# Wait for pgSTAC setup job to complete
kubectl wait --for=condition=complete job/pgstac-setup -n "$NAMESPACE" --timeout=300s

# Check if job succeeded
if ! kubectl get job pgstac-setup -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' | grep -q "True"; then
    log_error "pgSTAC setup job failed"
    kubectl logs job/pgstac-setup -n "$NAMESPACE"
    exit 1
fi

log_success "pgSTAC setup completed successfully"

# Deploy eoapi-notifier with generated config
log_step "Deploying eoapi-notifier"
kubectl create configmap eoapi-notifier-config -n "$NAMESPACE" \
    --from-file=integration-config.yaml=/tmp/k8s-configs/integration-config.yaml \
    --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -n "$NAMESPACE" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eoapi-notifier
spec:
  replicas: 1
  selector:
    matchLabels:
      app: eoapi-notifier
  template:
    metadata:
      labels:
        app: eoapi-notifier
    spec:
      containers:
      - name: eoapi-notifier
        image: eoapi-notifier-test-eoapi-notifier:latest
        imagePullPolicy: Never
        env:
        - name: K_SINK
          value: http://event-display.$NAMESPACE.svc.cluster.local
        command: ["uv", "run", "eoapi-notifier"]
        args: ["/app/config/integration-config.yaml"]
        volumeMounts:
        - name: config
          mountPath: /app/config
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 256Mi
      volumes:
      - name: config
        configMap:
          name: eoapi-notifier-config
EOF

kubectl wait --for=condition=available deployment/eoapi-notifier -n "$NAMESPACE" --timeout=120s

# Cleanup temp configs
rm -rf /tmp/k8s-configs

log_success "Kubernetes environment ready with KNative"
log_info "PostgreSQL: postgres:5432 (cluster-internal)"
log_info "MQTT: mqtt:1883 (cluster-internal)"
log_info "KNative Event Display: event-display (knative service)"

# Export environment variables
export INTEGRATION_PLATFORM="kubernetes"
export TEST_NAMESPACE="$NAMESPACE"
export POSTGRES_HOST="postgres"
export POSTGRES_PORT="5432"
export POSTGRES_DATABASE="testdb"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="testpass"
export MQTT_HOST="mqtt"
export MQTT_PORT="1883"
export KNATIVE_SERVICE="event-display"