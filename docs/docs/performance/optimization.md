# Performance Optimization

This guide covers performance optimization techniques for Pythia workers and messaging systems.

## 🎯 Worker Optimization

### Message Processing

```python
from pythia import Worker
from pythia.brokers.kafka import KafkaConsumer

class OptimizedWorker(Worker):
    source = KafkaConsumer(
        topic="orders",
        batch_size=100,  # Process in batches
        max_poll_records=500,
        fetch_min_bytes=1024,
    )

    async def process(self, message):
        # Minimize processing time
        await self.handle_message_fast(message)

    async def handle_message_fast(self, message):
        # Use async operations for I/O
        async with self.session.begin():
            await self.save_to_database(message.body)
```

### Batch Processing

```python
from pythia import BatchWorker

class BatchOptimizedWorker(BatchWorker):
    source = KafkaConsumer(topic="events")
    batch_size = 50
    batch_timeout = 5.0

    async def process_batch(self, messages):
        # Process multiple messages at once
        data = [msg.body for msg in messages]
        await self.bulk_insert(data)
```

## 🚀 Broker Optimization

### Kafka Configuration

```python
from pythia.brokers.kafka import KafkaConfig, KafkaConsumer

config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    # Consumer optimization
    session_timeout_ms=30000,
    heartbeat_interval_ms=3000,
    max_poll_interval_ms=300000,
    # Performance tuning
    fetch_min_bytes=50000,
    fetch_max_wait_ms=500,
    max_partition_fetch_bytes=1048576,
    # Connection pooling
    connections_max_idle_ms=540000,
    request_timeout_ms=30000,
)

consumer = KafkaConsumer(
    topic="high_volume_topic",
    config=config,
    batch_size=1000,
)
```

### Redis Optimization

```python
from pythia.brokers.redis import RedisConfig, RedisStreamsConsumer

config = RedisConfig(
    host="localhost",
    port=6379,
    # Connection pooling
    max_connections=20,
    retry_on_timeout=True,
    socket_keepalive=True,
    socket_keepalive_options={},
    # Performance settings
    socket_connect_timeout=5,
    socket_timeout=5,
)

consumer = RedisStreamsConsumer(
    stream="orders",
    config=config,
    batch_size=100,
    block_time=1000,  # 1 second blocking
)
```

### RabbitMQ Optimization

```python
from pythia.brokers.rabbitmq import RabbitMQConfig, RabbitMQConsumer

config = RabbitMQConfig(
    host="localhost",
    port=5672,
    # Connection optimization
    heartbeat=600,
    connection_attempts=3,
    retry_delay=5,
    # Performance settings
    socket_timeout=5,
    stack_timeout=5,
)

consumer = RabbitMQConsumer(
    queue="orders",
    config=config,
    prefetch_count=100,  # Process multiple messages
    auto_ack=False,
)
```

## 📊 Memory Optimization

### Message Handling

```python
class MemoryEfficientWorker(Worker):
    source = KafkaConsumer(topic="large_messages")

    async def process(self, message):
        # Process message in chunks for large payloads
        if len(message.body) > 10_000_000:  # 10MB
            await self.process_large_message(message)
        else:
            await self.process_normal_message(message)

    async def process_large_message(self, message):
        # Stream processing for large messages
        chunk_size = 1024 * 1024  # 1MB chunks
        for i in range(0, len(message.body), chunk_size):
            chunk = message.body[i:i + chunk_size]
            await self.process_chunk(chunk)
```

### Connection Pooling

```python
from pythia import Worker
import aiohttp

class PooledHTTPWorker(Worker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Reuse HTTP connections
        self.session = None

    async def startup(self):
        await super().startup()
        connector = aiohttp.TCPConnector(
            limit=100,  # Total connection limit
            limit_per_host=30,  # Per-host connection limit
            keepalive_timeout=300,
        )
        self.session = aiohttp.ClientSession(connector=connector)

    async def shutdown(self):
        if self.session:
            await self.session.close()
        await super().shutdown()
```

## ⚡ CPU Optimization

### Async/Await Best Practices

```python
import asyncio

class AsyncOptimizedWorker(Worker):
    async def process(self, message):
        # Run CPU-intensive tasks in executor
        if message.headers.get("cpu_intensive"):
            result = await asyncio.get_event_loop().run_in_executor(
                None, self.cpu_intensive_task, message.body
            )
        else:
            result = await self.async_task(message.body)

        return result

    def cpu_intensive_task(self, data):
        # Synchronous CPU-bound operation
        return self.heavy_computation(data)

    async def async_task(self, data):
        # I/O-bound async operation
        async with aiohttp.ClientSession() as session:
            async with session.post("/api/process", json=data) as response:
                return await response.json()
```

### Concurrency Control

```python
import asyncio

class ConcurrencyControlWorker(Worker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Limit concurrent operations
        self.semaphore = asyncio.Semaphore(10)

    async def process(self, message):
        async with self.semaphore:
            # Process with concurrency limit
            await self.handle_message(message)
```

## 🔧 System-Level Optimization

### Worker Configuration

```python
from pythia.config import WorkerConfig

config = WorkerConfig(
    # Processing configuration
    batch_size=100,
    batch_timeout=5.0,
    max_retries=3,
    retry_delay=1.0,

    # Resource limits
    max_memory_mb=512,
    max_cpu_percent=80,

    # Monitoring
    metrics_enabled=True,
    health_check_interval=30,
)
```

### Environment Variables

```bash
# Python optimizations
export PYTHONOPTIMIZE=1
export PYTHONDONTWRITEBYTECODE=1

# asyncio optimizations
export PYTHONASYNCIODEBUG=0

# Memory settings
export MALLOC_ARENA_MAX=2

# Worker settings
export PYTHIA_BATCH_SIZE=100
export PYTHIA_MAX_WORKERS=4
export PYTHIA_PREFETCH_COUNT=200
```

## 📈 Monitoring & Profiling

### Built-in Metrics

```python
from pythia import Worker
from pythia.monitoring import MetricsCollector

class MonitoredWorker(Worker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metrics = MetricsCollector()

    async def process(self, message):
        with self.metrics.timer("message_processing_time"):
            result = await self.handle_message(message)
            self.metrics.increment("messages_processed")
            return result
```

### Performance Testing

```python
import time
import asyncio
from pythia.testing import WorkerTestCase

class PerformanceTest(WorkerTestCase):
    async def test_throughput(self):
        worker = self.create_worker()

        # Send 1000 messages
        messages = [self.create_message(i) for i in range(1000)]

        start_time = time.time()
        await worker.process_batch(messages)
        end_time = time.time()

        throughput = len(messages) / (end_time - start_time)
        self.assertGreater(throughput, 100)  # > 100 msg/sec
```

## 🎯 Performance Checklist

### Pre-Production Checklist

- [ ] **Batch Processing**: Use appropriate batch sizes (50-1000)
- [ ] **Connection Pooling**: Configure broker connection limits
- [ ] **Memory Limits**: Set max memory per worker
- [ ] **CPU Optimization**: Use async/await properly
- [ ] **Monitoring**: Enable metrics collection
- [ ] **Resource Limits**: Configure system resource limits
- [ ] **Error Handling**: Implement efficient retry strategies
- [ ] **Testing**: Run load tests with realistic data

### Common Performance Issues

1. **Small Batch Sizes**: Use batches of 50-1000 messages
2. **Blocking I/O**: Always use async operations
3. **Memory Leaks**: Monitor memory usage over time
4. **Connection Exhaustion**: Configure connection pooling
5. **CPU Bottlenecks**: Profile and optimize hot paths

## 📚 Additional Resources

- [Asyncio Performance Tips](https://docs.python.org/3/library/asyncio-dev.html)
- [Kafka Performance Tuning](https://kafka.apache.org/documentation/#tuning)
- [Redis Performance Optimization](https://redis.io/topics/benchmarks)
- [RabbitMQ Performance Tuning](https://www.rabbitmq.com/documentation.html)
