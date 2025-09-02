# Quick Start

Get your first Pythia worker running in 5 minutes.

## Create Your First Worker

Pythia workers are Python classes that inherit from the base `Worker` class:

```python
import asyncio
from pythia.core import Worker, Message
from pythia.config import WorkerConfig

class MyFirstWorker(Worker):
    """A simple message processing worker"""

    async def process_message(self, message: Message) -> Any:
        """Process a single message"""
        print(f"Processing message: {message.body}")

        # Your business logic here
        result = message.body.upper()

        return {"processed": result, "worker": "my-first-worker"}

# Configuration
config = WorkerConfig(
    worker_name="my-first-worker",
    broker_type="redis",  # or "kafka", "rabbitmq"
    max_concurrent=5
)

# Run the worker
if __name__ == "__main__":
    worker = MyFirstWorker(config=config)
    asyncio.run(worker.start())
```

## Configuration with Auto-Detection

Pythia can auto-detect your broker configuration:

```python
from pythia.config import auto_detect_config

# Auto-detect from environment or running services
config = auto_detect_config()
worker = MyFirstWorker(config=config)
```

## Environment Configuration

Set configuration via environment variables:

```bash
export PYTHIA_WORKER_NAME="my-worker"
export PYTHIA_BROKER_TYPE="kafka"
export PYTHIA_MAX_CONCURRENT=10
export PYTHIA_LOG_LEVEL="DEBUG"
```

```python
from pythia.config import WorkerConfig

# Automatically loads from environment
config = WorkerConfig()
worker = MyFirstWorker(config=config)
```

## Broker-Specific Quick Start

=== "Redis"

    ```python
    from pythia.config.redis import RedisConfig

    redis_config = RedisConfig(
        host="localhost",
        port=6379,
        queue_name="my-queue"
    )

    config = WorkerConfig(broker_type="redis")
    worker = MyFirstWorker(config=config)
    ```

=== "Kafka"

    ```python
    from pythia.config.kafka import KafkaConfig

    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic="my-topic",
        group_id="my-group"
    )

    config = WorkerConfig(broker_type="kafka")
    worker = MyFirstWorker(config=config)
    ```

=== "RabbitMQ"

    ```python
    from pythia.config.rabbitmq import RabbitMQConfig

    rabbitmq_config = RabbitMQConfig(
        host="localhost",
        queue_name="my-queue"
    )

    config = WorkerConfig(broker_type="rabbitmq")
    worker = MyFirstWorker(config=config)
    ```

## Testing Your Worker

```python
import pytest
from pythia.utils.testing import WorkerTestCase

class TestMyFirstWorker(WorkerTestCase):
    worker_class = MyFirstWorker

    async def test_message_processing(self):
        message = self.create_test_message({"data": "hello"})
        result = await self.worker.process_message(message)

        assert result["processed"] == "HELLO"
        assert result["worker"] == "my-first-worker"
```

## Next Steps

- [Your First Worker](first-worker.md) - Detailed tutorial with real examples
- [Worker Lifecycle](../user-guide/worker-lifecycle.md) - Understanding worker startup and shutdown
- [Configuration Guide](../user-guide/configuration.md) - Complete configuration options
