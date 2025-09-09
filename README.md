# eoAPI-notifier

Message handler for eoAPI components. A middleware tool that listens to sources for messages and forwards them to output receivers.

## Requirements

- Python 3.12 or higher

## Installation

Install using `uv`:

```bash
uv add eoapi-notifier
```

## Usage

The notifier provides a CLI tool to run the message handler with a YAML configuration file.

### Command Line Interface

Run the notifier with a configuration file:

```bash
eoapi-notifier config.yaml
```

Set logging level:

```bash
eoapi-notifier --log-level DEBUG config.yaml
```

Show help:

```bash
eoapi-notifier --help
```

Show version:

```bash
eoapi-notifier --version
```

### Configuration

Create a YAML configuration file to specify sources (where messages come from) and outputs (where messages are sent). Here's a basic example:

```yaml
# Sources: Define where notifications come from
sources:
  - type: postgres
    config:
      host: localhost
      port: 5432
      database: postgis
      username: username
      password: password

# Outputs: Define where notifications are sent
outputs:
  - type: mqtt
    config:
      broker_host: localhost
      broker_port: 1883
```

See [examples/config.yaml](./examples/config.yaml) for a complete configuration example with all available options.

### Available Plugins

#### Sources
- `postgres`: Monitor PostgreSQL/pgSTAC database changes

#### Outputs
- `mqtt`: Publish events to MQTT broker

## Development

For development setup, testing, and creating new plugins, see the [Development Guide](docs/development.md).

## Contributing

We welcome contributions to eoAPI-notifier! Whether you want to fix a bug, add a new feature, or create a custom plugin, your contributions are appreciated.

- Found a bug or have a feature request? [Open an issue](https://github.com/developmentseed/eoapi-notifier/issues).
- Have a fix, improvement, or you want to add a new plugin? [Submit a pull request](https://github.com/developmentseed/eoapi-notifier/pulls) with your changes.

Please make sure to read the [Development Guide](docs/development.md) for setup instructions and coding standards.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
