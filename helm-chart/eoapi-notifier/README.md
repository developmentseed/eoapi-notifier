# eoAPI Notifier Helm Chart

Helm chart for eoAPI Notifier - message handler for PostgreSQL/pgSTAC to MQTT.

## Installation

```bash
# Install latest
helm install eoapi-notifier oci://ghcr.io/developmentseed/charts/eoapi-notifier

# Install specific version
helm install eoapi-notifier oci://ghcr.io/developmentseed/charts/eoapi-notifier --version 1.0.0

# With custom values
helm install eoapi-notifier oci://ghcr.io/developmentseed/charts/eoapi-notifier -f values.yaml
```

## Configuration

```yaml
config:
  sources:
    - type: postgres
      config:
        host: postgresql
        port: 5432
        database: postgis
        username: postgres
        password: password

  outputs:
    - type: mqtt
      config:
        broker_host: mqtt-broker
        broker_port: 1883

    - type: cloudevents
      config:
        source: /eoapi/pgstac
        event_type: org.eoapi.stac.item
        destination:
          ref:
            apiVersion: messaging.knative.dev/v1
            kind: Broker
            name: my-channel-1
            namespace: serverless

secrets:
  postgresql:
    create: true
    username: postgres
    password: password

resources:
  limits:
    cpu: 500m
    memory: 512Mi
  requests:
    cpu: 100m
    memory: 128Mi
```

## KNative SinkBinding Support

The chart automatically creates KNative SinkBinding resources for CloudEvents outputs, resolving object references to URLs via the `K_SINK` environment variable.

### Configuration

```yaml
outputs:
  - type: cloudevents
    config:
      source: /eoapi/pgstac
      event_type: org.eoapi.stac.item
      destination:
        ref:
          apiVersion: messaging.knative.dev/v1
          kind: Broker
          name: my-broker
          namespace: default  # optional
```
