# Integration Testing

End-to-end testing for eoAPI notifier. Tests complete pipeline: PostgreSQL/pgSTAC → eoAPI notifier → MQTT/CloudEvents.

## Quick Start

```bash
# Auto-detect platform and run all tests
./integration-test.sh

# Force specific platform
./integration-test.sh --platform docker-compose
./integration-test.sh --platform kubernetes

# Run specific test categories
./integration-test.sh --tests database,notifications
./integration-test.sh --tests end-to-end --verbose
```

## Structure

```
integration/
├── integration-test.sh              # Main entry point
├── tests/                          # Platform-agnostic tests
│   ├── test_database_setup.py      # Database & pgSTAC validation
│   ├── test_notifications.py       # PostgreSQL notifications
│   └── test_end_to_end.py          # Complete pipeline tests
├── docker-compose/                 # Docker Compose environment
│   ├── docker-compose.yml         # Services definition
│   └── config/                    # Generated configs (dynamic)
├── kubernetes/                     # Kubernetes environment
│   └── setup.sh                   # K8s deployment script
└── shared/                        # Centralized utilities (DRY)
    ├── common.sh                  # Shell utilities
    ├── constants.py               # Shared configuration constants
    ├── config_template.py         # Configuration generator
    ├── pgstac_setup.py            # Consolidated database setup
    ├── test_data.py               # Centralized test data
    └── test_runner.py             # Platform abstraction
```

## Test Categories

- **database**: PostgreSQL connection, pgSTAC schema, test data
- **notifications**: Database trigger functionality, payload structure
- **end-to-end**: Complete message flow (DB → Notifier → MQTT)

## Platforms

### Docker Compose (Default)
- **Services**: PostgreSQL (5433), MQTT (1884), CloudEvents endpoint
- **Setup**: Auto-generates configs and runs setup via `docker-compose/setup.sh`
- **Requirements**: Docker, docker-compose

### Kubernetes
- **Services**: PostgreSQL, MQTT, KNative event-display
- **Setup**: Creates `integration-test` namespace with KNative SinkBinding
- **CloudEvents**: Uses KNative Serving and SinkBinding (K_SINK environment variable)
- **Requirements**: kubectl, K8s cluster with KNative installed

**Configuration**: All platform-specific configs are generated dynamically from shared templates using centralized constants.

## Manual Testing

```bash
# Start services only (Docker Compose)
cd docker-compose && ./setup.sh

# Connect to database
psql -h localhost -p 5433 -U postgres -d postgis

# Monitor MQTT
docker-compose exec mqtt mosquitto_sub -h localhost -t "eoapi/stac/items/+"

# Monitor CloudEvents (Docker Compose)
curl http://localhost:8080/events

# Monitor KNative events (Kubernetes)
kubectl logs -l serving.knative.dev/service=event-display -n integration-test -f

# Generate configs manually (optional)
cd shared && python config_template.py docker-compose ../docker-compose/config

# Cleanup
cd docker-compose && ./teardown.sh
```

## Examples

```bash
# Quick test run
./integration-test.sh

# Debug specific component
./integration-test.sh --tests database --verbose

# Keep services for inspection
./integration-test.sh --keep-services

# Kubernetes with verbose output
./integration-test.sh --platform kubernetes --verbose
```

## Pipeline

**Expected Flow**:
- **MQTT**: pgSTAC Operation → PostgreSQL NOTIFY → eoapi-notifier → MQTT Topic (`eoapi/stac/items/{collection}`)
- **CloudEvents**:
  - Docker Compose: → Custom CloudEvents HTTP endpoint
  - Kubernetes: → KNative Serving (event-display) via SinkBinding (K_SINK)

**Success Output**:
```
✅ Database setup tests: PASS
✅ Notification tests: PASS
✅ End-to-end tests: PASS
🎉 Integration tests completed successfully!
```
