# Installation

Get Pythia up and running in minutes.

## Requirements

- **Python**: 3.8 or higher
- **Operating System**: Linux, macOS, or Windows
- **Message Broker**: Redis, Kafka, or RabbitMQ (optional for development)

## Install from PyPI

```bash
pip install pythia-framework
```

## Install from Source

For the latest development version:

```bash
git clone https://github.com/ralonso20/pythia.git
cd pythia
pip install -e .
```

## Broker-Specific Dependencies

Choose your message broker and install the corresponding dependencies:

=== "Redis"

    ```bash
    pip install pythia-framework[redis]
    ```

=== "Kafka"

    ```bash
    pip install pythia-framework[kafka]
    ```

=== "RabbitMQ"

    ```bash
    pip install pythia-framework[rabbitmq]
    ```

=== "All Brokers"

    ```bash
    pip install pythia-framework[all]
    ```

## Quick Setup Test

Verify your installation:

```python
import pythia
print(f"Pythia version: {pythia.__version__}")
```

## Next Steps

- [Quick Start Guide](quick-start.md) - Create your first worker in 5 minutes
- [Your First Worker](first-worker.md) - Step-by-step tutorial
- [Configuration](../user-guide/configuration.md) - Advanced setup options
