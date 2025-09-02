# Broker Interfaces

This reference covers message broker interfaces and implementations in Pythia.

## Base Interfaces

### MessageBroker

::: pythia.brokers.base.MessageBroker
    Abstract base class for all message consumers.

```python
from pythia.brokers.base import MessageBroker
from typing import AsyncIterator
from pythia.core.message import Message

class CustomConsumer(MessageBroker):
    async def connect(self) -> None:
        """Establish connection to broker"""
        pass

    async def disconnect(self) -> None:
        """Close connection to broker"""
        pass

    async def consume(self) -> AsyncIterator[Message]:
        """Consume messages from broker"""
        async for message in self._fetch_messages():
            yield message

    async def health_check(self) -> bool:
        """Check broker health"""
        return True
```

### MessageProducer

::: pythia.brokers.base.MessageProducer
    Abstract base class for all message producers.

```python
from pythia.brokers.base import MessageProducer
from pythia.core.message import Message

class CustomProducer(MessageProducer):
    async def connect(self) -> None:
        """Establish connection to broker"""
        pass

    async def disconnect(self) -> None:
        """Close connection to broker"""
        pass

    async def send(self, message: Message) -> None:
        """Send message to broker"""
        await self._publish_message(message)

    async def health_check(self) -> bool:
        """Check broker health"""
        return True
```

## Kafka Brokers

### KafkaConsumer

::: pythia.brokers.kafka.consumer.KafkaConsumer
    Kafka message consumer implementation.

```python
from pythia.brokers.kafka import KafkaConsumer, KafkaConfig

# Basic usage
consumer = KafkaConsumer(
    topic="orders",
    consumer_group="order-processors",
    bootstrap_servers="localhost:9092"
)

# With configuration
config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    session_timeout_ms=30000,
    heartbeat_interval_ms=3000,
    auto_offset_reset="earliest"
)

consumer = KafkaConsumer(
    topic="orders",
    consumer_group="processors",
    config=config
)
```

#### Constructor Parameters

- **topic**: str - Kafka topic to consume from
- **consumer_group**: str - Consumer group ID
- **bootstrap_servers**: str - Kafka broker addresses (optional)
- **config**: KafkaConfig - Configuration object (optional)
- **batch_size**: int - Messages per batch (default: 100)
- **auto_offset_reset**: str - Offset reset strategy

#### Methods

```python
async def consume(self) -> AsyncIterator[Message]:
    """Consume messages from Kafka topic"""
    pass

async def commit_offsets(self) -> None:
    """Manually commit current offsets"""
    pass

async def seek_to_beginning(self) -> None:
    """Seek to beginning of topic"""
    pass

async def seek_to_end(self) -> None:
    """Seek to end of topic"""
    pass
```

### KafkaProducer

::: pythia.brokers.kafka.producer.KafkaProducer
    Kafka message producer implementation.

```python
from pythia.brokers.kafka import KafkaProducer, KafkaConfig

# Basic usage
producer = KafkaProducer(
    topic="processed-orders",
    bootstrap_servers="localhost:9092"
)

# With partitioning
producer = KafkaProducer(
    topic="events",
    partition_key=lambda msg: msg.get("user_id"),
    bootstrap_servers="localhost:9092"
)
```

#### Constructor Parameters

- **topic**: str - Kafka topic to produce to
- **bootstrap_servers**: str - Kafka broker addresses (optional)
- **config**: KafkaConfig - Configuration object (optional)
- **partition_key**: callable - Function to determine partition
- **key_serializer**: callable - Key serialization function
- **value_serializer**: callable - Value serialization function

#### Methods

```python
async def send(self, message: Message) -> None:
    """Send message to Kafka topic"""
    pass

async def send_batch(self, messages: List[Message]) -> None:
    """Send multiple messages"""
    pass

async def flush(self) -> None:
    """Flush pending messages"""
    pass
```

## Redis Brokers

### RedisStreamsConsumer

::: pythia.brokers.redis.streams.RedisStreamsConsumer
    Redis Streams consumer implementation.

```python
from pythia.brokers.redis import RedisStreamsConsumer, RedisConfig

# Basic usage
consumer = RedisStreamsConsumer(
    stream="orders",
    consumer_group="processors",
    consumer_name="worker-1"
)

# With configuration
config = RedisConfig(
    host="localhost",
    port=6379,
    password="secret",
    max_connections=10
)

consumer = RedisStreamsConsumer(
    stream="events",
    consumer_group="handlers",
    consumer_name="handler-1",
    config=config
)
```

#### Constructor Parameters

- **stream**: str - Redis stream name
- **consumer_group**: str - Consumer group name
- **consumer_name**: str - Unique consumer name
- **config**: RedisConfig - Configuration object (optional)
- **batch_size**: int - Messages per batch
- **block_time**: int - Blocking timeout in milliseconds
- **start_id**: str - Starting message ID (default: ">")

### RedisStreamsProducer

::: pythia.brokers.redis.streams.RedisStreamsProducer
    Redis Streams producer implementation.

```python
from pythia.brokers.redis import RedisStreamsProducer

producer = RedisStreamsProducer(
    stream="orders",
    max_length=10000,  # Trim stream to max length
    approximate=True   # Approximate trimming for performance
)
```

### RedisPubSubConsumer

::: pythia.brokers.redis.pubsub.RedisPubSubConsumer
    Redis Pub/Sub consumer implementation.

```python
from pythia.brokers.redis import RedisPubSubConsumer

# Subscribe to channels
consumer = RedisPubSubConsumer(
    channels=["orders", "events"],
    config=redis_config
)

# Subscribe to patterns
consumer = RedisPubSubConsumer(
    patterns=["user.*", "order.*"],
    config=redis_config
)
```

### RedisListConsumer

::: pythia.brokers.redis.lists.RedisListConsumer
    Redis List consumer (BRPOP/BLPOP) implementation.

```python
from pythia.brokers.redis import RedisListConsumer

consumer = RedisListConsumer(
    queue="task_queue",
    timeout=10,  # Block timeout in seconds
    config=redis_config
)
```

## RabbitMQ Brokers

### RabbitMQConsumer

::: pythia.brokers.rabbitmq.consumer.RabbitMQConsumer
    RabbitMQ message consumer implementation.

```python
from pythia.brokers.rabbitmq import RabbitMQConsumer, RabbitMQConfig

# Basic queue consumer
consumer = RabbitMQConsumer(
    queue="orders",
    host="localhost",
    port=5672
)

# With exchange and routing
consumer = RabbitMQConsumer(
    queue="order_processing",
    exchange="orders",
    routing_key="new_order",
    exchange_type="topic"
)

# With configuration
config = RabbitMQConfig(
    host="rabbitmq.example.com",
    port=5672,
    username="user",
    password="pass",
    virtual_host="/",
    heartbeat=600
)

consumer = RabbitMQConsumer(
    queue="tasks",
    config=config,
    prefetch_count=100,
    auto_ack=False
)
```

#### Constructor Parameters

- **queue**: str - Queue name to consume from
- **exchange**: str - Exchange name (optional)
- **routing_key**: str - Routing key (optional)
- **exchange_type**: str - Exchange type (direct, topic, fanout, headers)
- **config**: RabbitMQConfig - Configuration object (optional)
- **prefetch_count**: int - Prefetch message count
- **auto_ack**: bool - Automatic acknowledgment
- **durable**: bool - Durable queue declaration

### RabbitMQProducer

::: pythia.brokers.rabbitmq.producer.RabbitMQProducer
    RabbitMQ message producer implementation.

```python
from pythia.brokers.rabbitmq import RabbitMQProducer

# Basic producer
producer = RabbitMQProducer(
    exchange="orders",
    routing_key="new_order"
)

# Queue producer
producer = RabbitMQProducer(
    queue="direct_queue"
)
```

## HTTP Brokers

### PollerWorker (Consumer)

::: pythia.brokers.http.poller.PollerWorker
    HTTP polling consumer implementation.

```python
from pythia.brokers.http import PollerWorker

poller = PollerWorker(
    url="https://api.example.com/events",
    poll_interval=30.0,  # Poll every 30 seconds
    headers={"Authorization": "Bearer token"},
    params={"limit": 100},
    method="GET"
)
```

### WebhookSenderWorker (Producer)

::: pythia.brokers.http.webhook.WebhookSenderWorker
    HTTP webhook producer implementation.

```python
from pythia.brokers.http import WebhookSenderWorker

webhook = WebhookSenderWorker(
    webhook_urls=[
        "https://api.partner1.com/webhooks",
        "https://api.partner2.com/webhooks"
    ],
    headers={"Content-Type": "application/json"},
    timeout=10.0,
    max_retries=3
)
```

## Cloud Brokers

### AWS SQS/SNS

```python
from pythia.brokers.cloud.aws import SQSConsumer, SNSProducer

# SQS Consumer
sqs_consumer = SQSConsumer(
    queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
    max_messages=10,
    wait_time_seconds=20,  # Long polling
    visibility_timeout_seconds=30
)

# SNS Producer
sns_producer = SNSProducer(
    topic_arn="arn:aws:sns:us-east-1:123456789012:my-topic",
    message_attributes={
        "event_type": {"DataType": "String", "StringValue": "order"}
    }
)
```

### Google Cloud Pub/Sub

```python
from pythia.brokers.cloud.gcp import PubSubSubscriber, PubSubPublisher

# Pub/Sub Subscriber
subscriber = PubSubSubscriber(
    subscription_name="projects/my-project/subscriptions/my-subscription",
    max_messages=100,
    ack_deadline_seconds=600
)

# Pub/Sub Publisher
publisher = PubSubPublisher(
    topic_name="projects/my-project/topics/events",
    ordering_key="user_id",  # Message ordering
    attributes={"source": "web_app"}
)
```

### Azure Service Bus

```python
from pythia.brokers.cloud.azure import (
    ServiceBusConsumer,
    ServiceBusProducer,
    StorageQueueConsumer,
    StorageQueueProducer
)

# Service Bus Consumer
sb_consumer = ServiceBusConsumer(
    queue_name="orders",
    max_messages=32,
    max_wait_time=5
)

# Storage Queue Consumer
storage_consumer = StorageQueueConsumer(
    queue_name="background_tasks",
    visibility_timeout=30,
    message_count=32
)
```

## Database Brokers

### CDCWorker (Consumer)

::: pythia.brokers.database.cdc.CDCWorker
    Change Data Capture consumer implementation.

```python
from pythia.brokers.database import CDCWorker

cdc = CDCWorker(
    db_url="postgresql://user:pass@localhost/db",
    tables=["users", "orders", "products"],
    poll_interval=5.0,
    batch_size=100
)
```

## Configuration Classes

### KafkaConfig

```python
from pythia.config.kafka import KafkaConfig

config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="user",
    sasl_password="pass",

    # Consumer settings
    session_timeout_ms=30000,
    heartbeat_interval_ms=3000,
    max_poll_interval_ms=300000,
    auto_offset_reset="earliest",

    # Producer settings
    acks="all",
    retries=3,
    batch_size=16384,
    linger_ms=10,
    buffer_memory=33554432
)
```

### RedisConfig

```python
from pythia.config.redis import RedisConfig

config = RedisConfig(
    host="localhost",
    port=6379,
    password="secret",
    db=0,

    # Connection pooling
    max_connections=20,
    retry_on_timeout=True,
    socket_keepalive=True,

    # Timeouts
    socket_connect_timeout=5,
    socket_timeout=5,
    response_timeout=5
)
```

### RabbitMQConfig

```python
from pythia.config.rabbitmq import RabbitMQConfig

config = RabbitMQConfig(
    host="localhost",
    port=5672,
    username="guest",
    password="guest",
    virtual_host="/",

    # Connection settings
    heartbeat=600,
    connection_attempts=3,
    retry_delay=5,

    # SSL settings
    ssl_enabled=False,
    ssl_certfile="/path/to/cert.pem",
    ssl_keyfile="/path/to/key.pem",
    ssl_ca_certs="/path/to/ca.pem"
)
```

### HTTPConfig

```python
from pythia.config.http import HTTPConfig

config = HTTPConfig(
    timeout=30.0,
    max_retries=3,
    retry_delay=1.0,

    # Connection pooling
    max_connections=100,
    max_keepalive_connections=20,
    keepalive_expiry=5.0,

    # Circuit breaker
    enable_circuit_breaker=True,
    circuit_breaker_threshold=5,
    circuit_breaker_timeout=60.0
)
```

### CloudConfig (AWS/GCP/Azure)

```python
from pythia.config.cloud import AWSConfig, GCPConfig, AzureConfig

# AWS Configuration
aws_config = AWSConfig(
    region="us-east-1",
    access_key_id="AKIAIOSFODNN7EXAMPLE",
    secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    timeout=30.0,
    max_retries=3
)

# GCP Configuration
gcp_config = GCPConfig(
    project_id="my-project",
    credentials_path="/path/to/service-account.json",
    timeout=30.0,
    max_retries=3
)

# Azure Configuration
azure_config = AzureConfig(
    connection_string="Endpoint=sb://...",
    timeout=30.0,
    max_retries=3
)
```

## Error Handling

### BrokerError

```python
from pythia.exceptions import BrokerError, ConnectionError, MessageError

try:
    await consumer.connect()
except ConnectionError as e:
    logger.error(f"Failed to connect: {e}")
except BrokerError as e:
    logger.error(f"Broker error: {e}")
```

### Retry Mechanisms

```python
from pythia.utils.retry import RetryConfig

retry_config = RetryConfig(
    max_retries=3,
    initial_delay=1.0,
    max_delay=60.0,
    exponential_backoff=True,
    jitter=True
)

# Apply to broker
consumer = KafkaConsumer(
    topic="orders",
    retry_config=retry_config
)
```
