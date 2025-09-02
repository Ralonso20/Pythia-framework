# Core Classes

This reference covers the core classes of the Pythia framework.

## Worker

::: pythia.core.worker.Worker
    The main base class for creating message processing workers.

### Constructor

```python
from pythia import Worker
from pythia.config import WorkerConfig, MetricsConfig

worker = Worker(
    config=WorkerConfig(worker_name="my-worker"),
    metrics_config=MetricsConfig(enabled=True)
)
```

### Class Attributes

- **source**: Optional[MessageBroker] - Single message source
- **sink**: Optional[MessageProducer] - Single message sink
- **sources**: Optional[List[MessageBroker]] - Multiple message sources
- **sinks**: Optional[List[MessageProducer]] - Multiple message sinks

### Key Methods

#### Abstract Methods (Must Implement)

```python
async def process(self, message: Message) -> Any:
    """Process a single message"""
    pass
```

#### Lifecycle Methods

```python
async def startup(self) -> None:
    """Called when worker starts"""
    pass

async def shutdown(self) -> None:
    """Called when worker shuts down"""
    pass

async def health_check(self) -> bool:
    """Check if worker is healthy"""
    return True
```

#### Message Handling

```python
async def send_to_sink(self, data: Any, sink_index: int = 0) -> None:
    """Send data to specific sink"""
    pass

async def broadcast(self, data: Any) -> None:
    """Send data to all sinks"""
    pass
```

#### Runtime Methods

```python
def run(self) -> None:
    """Run worker synchronously"""
    pass

async def run_async(self) -> None:
    """Run worker asynchronously"""
    pass

def stop(self) -> None:
    """Stop the worker"""
    pass

def get_stats(self) -> Dict[str, Any]:
    """Get worker statistics"""
    pass
```

### Example Usage

```python
from pythia import Worker
from pythia.brokers.kafka import KafkaConsumer, KafkaProducer

class OrderProcessor(Worker):
    source = KafkaConsumer(topic="orders")
    sink = KafkaProducer(topic="processed-orders")

    async def process(self, message):
        order_data = message.body
        processed_order = await self.process_order(order_data)
        await self.send_to_sink(processed_order)
        return processed_order

    async def process_order(self, order):
        # Business logic here
        return {"id": order["id"], "status": "processed"}

# Run the worker
if __name__ == "__main__":
    worker = OrderProcessor()
    worker.run()
```

## BatchWorker

::: pythia.core.worker.BatchWorker
    Worker for processing messages in batches.

```python
from pythia import BatchWorker
from pythia.brokers.kafka import KafkaConsumer

class BatchOrderProcessor(BatchWorker):
    source = KafkaConsumer(topic="orders")
    batch_size = 100
    batch_timeout = 5.0  # seconds

    async def process_batch(self, messages: List[Message]) -> List[Any]:
        """Process multiple messages at once"""
        orders = [msg.body for msg in messages]
        processed = await self.bulk_process_orders(orders)
        return processed
```

## SimpleWorker

::: pythia.core.worker.SimpleWorker
    Simplified worker created from functions.

```python
from pythia import simple_worker
from pythia.brokers.kafka import KafkaConsumer

@simple_worker(source=KafkaConsumer(topic="events"))
async def event_handler(message):
    """Simple function-based worker"""
    print(f"Received event: {message.body}")
    return {"processed": True}

# Run it
if __name__ == "__main__":
    event_handler.run()
```

## Message

::: pythia.core.message.Message
    Universal message abstraction for all brokers.

### Constructor

```python
from pythia.core.message import Message
from datetime import datetime

message = Message(
    body={"user_id": 123, "action": "login"},
    headers={"source": "auth-service"},
    message_id="msg-123",
    timestamp=datetime.utcnow(),
    source_info={"topic": "user-events", "partition": 0}
)
```

### Attributes

- **body**: Any - Message payload (dict, string, bytes, etc.)
- **headers**: Dict[str, Any] - Message headers/metadata
- **message_id**: str - Unique message identifier
- **timestamp**: datetime - Message timestamp
- **source_info**: Dict[str, Any] - Broker-specific information
- **retry_count**: int - Number of processing retries
- **max_retries**: int - Maximum allowed retries

### Factory Methods

```python
# Create from Kafka message
kafka_message = Message.from_kafka(kafka_record)

# Create from RabbitMQ message
rabbitmq_message = Message.from_rabbitmq(rabbitmq_message)

# Create from Redis message
redis_message = Message.from_redis(redis_data, stream_name)
```

### Methods

```python
def to_dict(self) -> Dict[str, Any]:
    """Convert message to dictionary"""
    pass

def should_retry(self) -> bool:
    """Check if message should be retried"""
    pass

def increment_retry(self) -> None:
    """Increment retry counter"""
    pass
```

### Example Usage

```python
from pythia.core.message import Message

# Create a message
message = Message(
    body={"order_id": 12345, "amount": 99.99},
    headers={"priority": "high", "source": "web"},
    message_id="order-12345"
)

# Check if retryable
if message.should_retry():
    message.increment_retry()
    await retry_processing(message)

# Convert to dict for serialization
message_dict = message.to_dict()
```

## LifecycleManager

::: pythia.core.lifecycle.LifecycleManager
    Manages worker startup, shutdown, and signal handling.

### Constructor

```python
from pythia.core.lifecycle import LifecycleManager

lifecycle = LifecycleManager(
    startup_timeout=30.0,
    shutdown_timeout=30.0,
    graceful_shutdown=True
)
```

### Methods

```python
async def startup(self, components: List[Any]) -> None:
    """Start all components"""
    pass

async def shutdown(self, components: List[Any]) -> None:
    """Shutdown all components"""
    pass

def setup_signal_handlers(self, stop_callback: Callable) -> None:
    """Setup SIGINT/SIGTERM handlers"""
    pass
```

### Example Usage

```python
from pythia.core.lifecycle import LifecycleManager

class MyWorker(Worker):
    def __init__(self):
        super().__init__()
        self.lifecycle = LifecycleManager()
        self.lifecycle.setup_signal_handlers(self.stop)

    async def startup(self):
        await self.lifecycle.startup([self.source, self.sink])

    async def shutdown(self):
        await self.lifecycle.shutdown([self.source, self.sink])
```

## WorkerRunner

::: pythia.core.lifecycle.WorkerRunner
    Utility for running workers with proper lifecycle management.

```python
from pythia.core.lifecycle import WorkerRunner

# Run worker with proper lifecycle
runner = WorkerRunner(worker_instance)
await runner.run()

# Or run synchronously
runner.run_sync()
```

## Configuration Classes

### WorkerConfig

```python
from pythia.config import WorkerConfig

config = WorkerConfig(
    worker_name="order-processor",
    worker_id="worker-1",
    batch_size=100,
    batch_timeout=5.0,
    max_retries=3,
    retry_delay=1.0,
    log_level="INFO",
    enable_metrics=True,
    health_check_interval=30.0,
)
```

### MetricsConfig

```python
from pythia.monitoring import MetricsConfig

metrics_config = MetricsConfig(
    enabled=True,
    port=8000,
    path="/metrics",
    push_gateway_url="http://prometheus-pushgateway:9091",
    job_name="pythia-worker"
)
```

## Error Handling

### PythiaError

```python
from pythia.exceptions import PythiaError, WorkerError, BrokerError

try:
    await worker.process(message)
except WorkerError as e:
    logger.error(f"Worker error: {e}")
except BrokerError as e:
    logger.error(f"Broker error: {e}")
except PythiaError as e:
    logger.error(f"Framework error: {e}")
```

### Retry Decorators

```python
from pythia.utils.retry import retry_on_failure

class MyWorker(Worker):
    @retry_on_failure(max_retries=3, delay=1.0)
    async def process(self, message):
        # This method will auto-retry on failure
        result = await self.risky_operation(message.body)
        return result
```

## Testing Utilities

### WorkerTestCase

```python
from pythia.testing import WorkerTestCase

class TestMyWorker(WorkerTestCase):
    def setUp(self):
        self.worker = MyWorker()

    async def test_message_processing(self):
        message = self.create_message({"test": "data"})
        result = await self.worker.process(message)
        self.assertEqual(result["status"], "processed")

    def test_worker_health(self):
        self.assertTrue(self.worker.health_check())
```

### MockBrokers

```python
from pythia.testing.mocks import MockConsumer, MockProducer

# Use in tests
mock_consumer = MockConsumer(messages=[
    {"id": 1, "data": "test1"},
    {"id": 2, "data": "test2"},
])

worker = MyWorker()
worker.source = mock_consumer
```
