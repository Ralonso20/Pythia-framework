# Kafka Integration

Complete guide to using Pythia with Apache Kafka using confluent-kafka library.

## Overview

Pythia's Kafka integration uses **confluent-kafka** (not kafka-python) for superior performance and reliability. Our benchmarks achieved 1,872 msg/s with 2.0ms P95 latency.

!!! info "Library Choice"
    Pythia uses `confluent-kafka` instead of `kafka-python` for better performance, more features, and active maintenance by Confluent.

## Quick Start

```python
from pythia.core import Worker
from pythia.config import WorkerConfig
from pythia.config.kafka import KafkaConfig

# Basic Kafka configuration
kafka_config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    topics=["my-topic"]
)

config = WorkerConfig(broker_type="kafka")
```

## Configuration Options

### Basic Consumer Configuration

```python
from pythia.config.kafka import KafkaConfig

kafka_config = KafkaConfig(
    # Connection
    bootstrap_servers="localhost:9092",

    # Consumer settings
    group_id="email-processors",
    topics=["email-events", "user-events"],
    auto_offset_reset="earliest",        # or "latest"
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,

    # Performance
    max_poll_records=500,
    fetch_min_bytes=1024,
    fetch_max_wait_ms=500
)
```

### High-Performance Configuration

Based on our benchmarks (1,872 msg/s), optimal settings:

```python
kafka_config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="high-throughput-group",
    topics=["events"],

    # Optimized for throughput
    max_poll_records=1000,              # Larger batches
    fetch_min_bytes=50000,              # Wait for more data
    fetch_max_wait_ms=100,              # But don't wait too long

    # Session management
    session_timeout_ms=30000,           # Longer session timeout
    heartbeat_interval_ms=3000,         # Regular heartbeats
    max_poll_interval_ms=600000,        # 10 minutes max processing

    # Producer optimization
    acks="1",                           # Balance durability/speed
    retries=3,
    batch_size=16384,                   # 16KB batches
    linger_ms=5,                        # Small delay for batching
)
```

### Security Configuration

#### SASL/PLAIN Authentication

```python
kafka_config = KafkaConfig(
    bootstrap_servers="kafka.example.com:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="your-username",
    sasl_password="your-password",

    # SSL settings
    ssl_ca_location="/path/to/ca.crt"
)
```

#### Mutual TLS (mTLS)

```python
kafka_config = KafkaConfig(
    bootstrap_servers="kafka.example.com:9093",
    security_protocol="SSL",
    ssl_ca_location="/path/to/ca.crt",
    ssl_certificate_location="/path/to/client.crt",
    ssl_key_location="/path/to/client.key",
    ssl_key_password="key-password"  # If key is encrypted
)
```

## Working with Kafka

### Basic Consumer Worker

```python
import json
import asyncio
from typing import Any, Dict
from pythia.core import Worker, Message
from pythia.config import WorkerConfig
from pythia.config.kafka import KafkaConfig

class OrderProcessor(Worker):
    """Process order events from Kafka"""

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process a single order event"""
        try:
            # Parse Kafka message
            order_data = json.loads(message.body)

            self.logger.info(
                f"Processing order {order_data['order_id']}",
                extra={
                    "order_id": order_data["order_id"],
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset
                }
            )

            # Business logic
            result = await self._process_order(order_data)

            # Acknowledge message (auto-commit handles this)
            return result

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in message: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to process order: {e}")
            raise

    async def _process_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process the order (business logic)"""
        # Simulate processing
        await asyncio.sleep(0.1)

        return {
            "order_id": order_data["order_id"],
            "status": "processed",
            "processed_by": self.config.worker_id
        }

# Configuration
kafka_config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="order-processors",
    topics=["orders", "order-updates"]
)

config = WorkerConfig(
    worker_name="order-processor",
    broker_type="kafka",
    max_concurrent=10
)

# Run worker
if __name__ == "__main__":
    worker = OrderProcessor(config=config)
    asyncio.run(worker.start())
```

### Producer Integration

```python
from pythia.brokers.kafka.producer import KafkaProducer

class OrderProcessorWithOutput(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Setup producer for output
        producer_config = kafka_config.to_producer_config()
        self.producer = KafkaProducer(producer_config)

    async def process_message(self, message: Message) -> Dict[str, Any]:
        order_data = json.loads(message.body)
        result = await self._process_order(order_data)

        # Send result to output topic
        await self.producer.send_async(
            topic="processed-orders",
            key=str(order_data["order_id"]),
            value=json.dumps(result)
        )

        return result

    async def on_shutdown(self):
        """Cleanup producer on shutdown"""
        await self.producer.close()
        await super().on_shutdown()
```

## Topic Management

### Automatic Topic Creation

```python
from pythia.config.kafka import KafkaTopicConfig

# Define topic configuration
topic_config = KafkaTopicConfig(
    name="user-events",
    num_partitions=6,                    # Scale for throughput
    replication_factor=3,                # High availability
    cleanup_policy="delete",             # or "compact"
    retention_ms=7 * 24 * 3600 * 1000,  # 7 days
    segment_ms=24 * 3600 * 1000,         # 1 day segments
    max_message_bytes=1024 * 1024        # 1MB max message
)

# Worker can auto-create topics
class EventProcessor(Worker):
    topic_configs = [topic_config]  # Auto-create on startup
```

### Manual Topic Administration

```python
from pythia.brokers.kafka.admin import KafkaAdminClient

async def setup_topics():
    admin = KafkaAdminClient(kafka_config.to_confluent_config())

    # Create topics
    await admin.create_topics([
        {
            "topic": "events",
            "num_partitions": 12,
            "replication_factor": 3,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "604800000",  # 7 days
                "compression.type": "gzip"
            }
        }
    ])

    # List topics
    topics = await admin.list_topics()
    print(f"Available topics: {topics}")
```

## Error Handling & Resilience

### Retry Configuration

```python
from pythia.config import ResilienceConfig

resilience_config = ResilienceConfig(
    max_retries=5,
    retry_delay=1.0,
    retry_backoff=2.0,              # Exponential backoff
    retry_max_delay=60.0,

    # Kafka-specific timeouts
    processing_timeout=300,         # 5 minutes per message
    connection_timeout=30
)

config = WorkerConfig(
    broker_type="kafka",
    resilience=resilience_config
)
```

### Dead Letter Queue Pattern

```python
class EventProcessorWithDLQ(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.dlq_producer = KafkaProducer(kafka_config.to_producer_config())

    async def process_message(self, message: Message) -> Any:
        try:
            return await self._process_event(message)
        except Exception as e:
            # Send to dead letter queue after max retries
            if message.retry_count >= self.config.max_retries:
                await self._send_to_dlq(message, str(e))
                return None
            raise

    async def _send_to_dlq(self, message: Message, error: str):
        dlq_message = {
            "original_topic": message.topic,
            "original_partition": message.partition,
            "original_offset": message.offset,
            "error": error,
            "timestamp": message.timestamp,
            "body": message.body
        }

        await self.dlq_producer.send_async(
            topic=f"{message.topic}-dlq",
            value=json.dumps(dlq_message)
        )
```

## Monitoring & Metrics

### Kafka-Specific Metrics

```python
from pythia.config import MetricsConfig

metrics_config = MetricsConfig(
    enabled=True,
    prometheus_enabled=True,
    custom_metrics={
        "kafka_consumer_lag": True,        # Monitor lag
        "kafka_partition_assignment": True, # Track partitions
        "kafka_commit_latency": True,      # Monitor commits
        "kafka_fetch_latency": True,       # Monitor fetch times
        "kafka_throughput": True           # Messages/sec
    }
)
```

### Consumer Lag Monitoring

```python
class MonitoredKafkaWorker(Worker):
    async def on_startup(self):
        # Setup lag monitoring
        self.lag_monitor = KafkaLagMonitor(
            kafka_config=kafka_config,
            group_id="order-processors"
        )
        await self.lag_monitor.start()

    async def process_message(self, message: Message) -> Any:
        # Record processing metrics
        with self.metrics.timer("kafka_message_processing_time"):
            result = await self._process_message(message)

        # Update throughput counter
        self.metrics.counter("kafka_messages_processed").inc()

        return result
```

## Performance Optimization

### Consumer Tuning

```python
# High-throughput configuration
kafka_config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="high-perf-group",

    # Fetch optimization
    fetch_min_bytes=50000,           # Wait for 50KB
    fetch_max_wait_ms=100,           # Max 100ms wait
    max_poll_records=2000,           # Large batches

    # Connection optimization
    session_timeout_ms=45000,        # Longer sessions
    heartbeat_interval_ms=3000,      # Regular heartbeats

    # Commit optimization
    enable_auto_commit=False,        # Manual commits
    auto_commit_interval_ms=1000     # If auto-commit enabled
)
```

### Producer Tuning

```python
# High-throughput producer
producer_config = kafka_config.to_producer_config()
producer_config.update({
    "acks": "1",                     # Faster than "all"
    "batch.size": 65536,             # 64KB batches
    "linger.ms": 10,                 # Wait 10ms for batching
    "compression.type": "gzip",      # Compress messages
    "buffer.memory": 67108864,       # 64MB buffer
    "max.in.flight.requests.per.connection": 5
})
```

## Testing

```python
import pytest
from pythia.utils.testing import WorkerTestCase
from confluent_kafka import Producer

class TestKafkaWorker(WorkerTestCase):
    def setup_method(self):
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092'
        })

    async def test_order_processing(self):
        # Send test message
        test_order = {
            "order_id": "12345",
            "amount": 99.99,
            "customer_id": "cust_123"
        }

        self.producer.produce(
            topic="orders",
            key="12345",
            value=json.dumps(test_order)
        )
        self.producer.flush()

        # Process message
        message = await self.get_next_message()
        result = await self.worker.process_message(message)

        assert result["order_id"] == "12345"
        assert result["status"] == "processed"
```

## Production Deployment

### Docker Compose with Kafka

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: true

  pythia-worker:
    image: my-kafka-worker
    depends_on:
      - kafka
    environment:
      - PYTHIA_BROKER_TYPE=kafka
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_GROUP_ID=production-group
      - PYTHIA_MAX_CONCURRENT=20
    deploy:
      replicas: 5
```

### Production Kafka Configuration

```python
# Production-optimized configuration
production_kafka_config = KafkaConfig(
    bootstrap_servers="kafka1:9092,kafka2:9092,kafka3:9092",

    # Consumer settings
    group_id="production-workers",
    auto_offset_reset="earliest",
    enable_auto_commit=False,          # Manual commits for reliability

    # Performance settings
    max_poll_records=1000,
    fetch_min_bytes=50000,
    fetch_max_wait_ms=500,
    session_timeout_ms=30000,
    heartbeat_interval_ms=3000,

    # Security
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="your-username",
    sasl_password="your-password"
)
```

## Benchmark Results

Our Kafka integration achieved solid performance:

| Metric | Value |
|--------|-------|
| **Throughput** | 1,872 msg/s |
| **P95 Latency** | 2.0ms |
| **P99 Latency** | 5.0ms |
| **CPU Usage** | 9.0% |
| **Memory Usage** | 7,728 MB |
| **Error Rate** | 0% |

## Troubleshooting

### Common Issues

1. **Consumer lag**
   ```python
   # Increase batch sizes
   kafka_config = KafkaConfig(
       max_poll_records=2000,
       fetch_min_bytes=100000
   )
   ```

2. **Rebalancing issues**
   ```python
   # Tune session timeouts
   kafka_config = KafkaConfig(
       session_timeout_ms=45000,
       heartbeat_interval_ms=3000,
       max_poll_interval_ms=600000
   )
   ```

3. **Connection timeouts**
   ```python
   # Add retry logic
   resilience_config = ResilienceConfig(
       connection_timeout=60,
       max_retries=5
   )
   ```

## Migration from kafka-python

If migrating from `kafka-python`, key differences:

| kafka-python | confluent-kafka | Notes |
|-------------|-----------------|-------|
| `KafkaConsumer` | `Consumer` | Different API |
| `KafkaProducer` | `Producer` | Better performance |
| `bootstrap_servers` | `bootstrap.servers` | Dot notation |
| Python objects | C library | Much faster |

## Next Steps

- [RabbitMQ Integration](rabbitmq.md) - Compare with RabbitMQ setup
- [Performance Benchmarks](../performance/benchmarks.md) - Detailed performance comparison
- [Configuration Guide](../user-guide/configuration.md) - Advanced configuration options
