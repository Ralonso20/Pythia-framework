# Redis Integration

Complete guide to using Pythia with Redis as your message broker.

## Overview

Redis integration in Pythia supports multiple patterns:
- **Lists** - Simple queue-based messaging
- **Streams** - Advanced event streaming with consumer groups
- **Pub/Sub** - Publisher/subscriber messaging pattern

## Quick Start

```python
from pythia.core import Worker
from pythia.config import WorkerConfig
from pythia.config.redis import RedisConfig

# Basic Redis configuration
redis_config = RedisConfig(
    host="localhost",
    port=6379,
    queue="my-queue"  # For lists-based messaging
)

config = WorkerConfig(broker_type="redis")
```

## Configuration Options

### Basic Connection

```python
redis_config = RedisConfig(
    host="localhost",
    port=6379,
    db=0,
    password=None,  # Optional authentication
)
```

### Lists-Based Queues (Recommended)

```python
redis_config = RedisConfig(
    host="localhost",
    port=6379,
    queue="task-queue",           # Queue name
    batch_size=10,                # Process in batches
    block_timeout_ms=1000         # Polling timeout
)
```

### Streams-Based Processing

```python
redis_config = RedisConfig(
    host="localhost",
    port=6379,
    stream="events-stream",       # Stream name
    consumer_group="workers",     # Consumer group
    batch_size=50,               # Larger batches for streams
    max_stream_length=10000      # Limit stream size
)
```

### Pub/Sub Pattern

```python
redis_config = RedisConfig(
    host="localhost",
    port=6379,
    channel="notifications",      # Pub/Sub channel
    batch_size=1                 # Process individually
)
```

## Performance Optimizations

Based on our benchmark results (3,304 msg/s), here are the optimal settings:

```python
from pythia.config.redis import RedisConfig

# High-performance configuration
redis_config = RedisConfig(
    host="localhost",
    port=6379,

    # Queue settings
    queue="high-throughput-queue",
    batch_size=100,               # Larger batches for throughput
    block_timeout_ms=100,         # Shorter polling for responsiveness

    # Connection optimization
    connection_pool_size=20,      # More connections
    socket_keepalive=True,        # Keep connections alive
    socket_timeout=5,             # Connection timeout
    retry_on_timeout=True,        # Auto-retry on timeout

    # Health monitoring
    health_check_interval=30      # Regular health checks
)
```

## Working with Different Redis Patterns

### 1. Lists (LPUSH/BRPOP)

Best for simple task queues:

```python
class TaskWorker(Worker):
    async def process_message(self, message):
        # Process task from Redis list
        task_data = json.loads(message.body)
        result = await self.execute_task(task_data)
        return result

# Producer side
redis_client.lpush("task-queue", json.dumps(task_data))
```

### 2. Streams (XREAD/XACK)

Best for event processing with replay capability:

```python
class EventWorker(Worker):
    async def process_message(self, message):
        # Process event from Redis stream
        event_data = message.fields  # Stream fields
        await self.handle_event(event_data)

        # Acknowledge processing
        await self.ack_message(message)

# Producer side
redis_client.xadd("events-stream", {"event": "user_registered", "user_id": "123"})
```

### 3. Pub/Sub (PUBLISH/SUBSCRIBE)

Best for real-time notifications:

```python
class NotificationWorker(Worker):
    async def process_message(self, message):
        # Process real-time notification
        notification = json.loads(message.body)
        await self.send_notification(notification)

# Producer side
redis_client.publish("notifications", json.dumps(notification_data))
```

## Error Handling & Resilience

```python
from pythia.config import ResilienceConfig

resilience_config = ResilienceConfig(
    max_retries=5,                # Retry failed messages
    retry_delay=1.0,             # Initial delay
    retry_backoff=2.0,           # Exponential backoff
    circuit_breaker_enabled=True, # Circuit breaker protection
    processing_timeout=30        # Per-message timeout
)

config = WorkerConfig(
    broker_type="redis",
    resilience=resilience_config
)
```

## Monitoring & Metrics

Enable Redis-specific metrics:

```python
from pythia.config import MetricsConfig

metrics_config = MetricsConfig(
    enabled=True,
    prometheus_enabled=True,
    custom_metrics={
        "redis_queue_length": True,      # Monitor queue depth
        "redis_connection_pool": True,   # Monitor connections
        "redis_memory_usage": True       # Monitor Redis memory
    }
)

worker = TaskWorker(
    config=config,
    metrics_config=metrics_config
)
```

## Production Deployment

### Docker Compose Example

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    command: redis-server --maxmemory 1gb --maxmemory-policy allkeys-lru
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  pythia-worker:
    image: my-pythia-worker
    depends_on:
      - redis
    environment:
      - PYTHIA_BROKER_TYPE=redis
      - REDIS_HOST=redis
      - REDIS_QUEUE=production-queue
      - PYTHIA_MAX_CONCURRENT=10
    deploy:
      replicas: 3

volumes:
  redis_data:
```

### Production Redis Configuration

```conf
# redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
appendonly yes
appendfsync everysec
```

## Testing

```python
import pytest
from pythia.utils.testing import WorkerTestCase
from redis import Redis

class TestRedisWorker(WorkerTestCase):
    def setup_method(self):
        self.redis = Redis(host='localhost', port=6379, decode_responses=True)
        self.redis.flushdb()  # Clean test database

    async def test_message_processing(self):
        # Add test message
        self.redis.lpush("test-queue", '{"task": "test"}')

        # Process message
        message = await self.get_next_message()
        result = await self.worker.process_message(message)

        assert result is not None
```

## Benchmark Results

Our Redis integration achieved exceptional performance:

| Metric | Value |
|--------|-------|
| **Throughput** | 3,304 msg/s |
| **P95 Latency** | 0.6ms |
| **P99 Latency** | 2.2ms |
| **CPU Usage** | 4.2% |
| **Memory Usage** | 7,877 MB |
| **Error Rate** | 0% |

## Troubleshooting

### Common Issues

1. **Connection timeouts**
   ```python
   # Increase timeout values
   redis_config = RedisConfig(
       socket_timeout=10,
       connection_timeout=30
   )
   ```

2. **Memory issues**
   ```python
   # Use stream trimming
   redis_config = RedisConfig(
       max_stream_length=50000,
       trim_strategy="maxlen"
   )
   ```

3. **High CPU usage**
   ```python
   # Increase batch sizes
   redis_config = RedisConfig(
       batch_size=200,
       block_timeout_ms=1000
   )
   ```

## Next Steps

- [Configuration Guide](../user-guide/configuration.md) - Complete configuration reference
- [Performance Benchmarks](../performance/benchmarks.md) - Detailed performance analysis
- [Kafka Integration](kafka.md) - Compare with Kafka setup
