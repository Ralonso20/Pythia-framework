# Scaling Pythia Workers

This guide covers horizontal and vertical scaling strategies for Pythia-based applications.

## 🚀 Horizontal Scaling

### Multiple Worker Processes

```python
# worker.py
from pythia import Worker
from pythia.brokers.kafka import KafkaConsumer

class OrderProcessor(Worker):
    source = KafkaConsumer(
        topic="orders",
        consumer_group="order-processors",  # Scale with multiple consumers
    )

    async def process(self, message):
        await self.process_order(message.body)
```

```bash
# Run multiple instances
python worker.py &  # Process 1
python worker.py &  # Process 2
python worker.py &  # Process 3
python worker.py &  # Process 4
```

### Docker Scaling

```yaml
# docker-compose.yml
version: '3.8'
services:
  order-processor:
    build: .
    command: python worker.py
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - CONSUMER_GROUP=order-processors
    deploy:
      replicas: 4  # Run 4 instances
    depends_on:
      - kafka
```

### Kubernetes Scaling

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-processor
spec:
  replicas: 10  # Start with 10 replicas
  selector:
    matchLabels:
      app: order-processor
  template:
    metadata:
      labels:
        app: order-processor
    spec:
      containers:
      - name: worker
        image: my-app/order-processor:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
---
# Auto-scaling
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: order-processor-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-processor
  minReplicas: 5
  maxReplicas: 50
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

## ⬆️ Vertical Scaling

### Multi-threaded Processing

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pythia import Worker

class VerticalScaledWorker(Worker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Create thread pool for CPU-intensive tasks
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def process(self, message):
        # Offload CPU-intensive work to thread pool
        if message.headers.get("cpu_intensive"):
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                self.cpu_bound_task,
                message.body
            )
        else:
            result = await self.io_bound_task(message.body)

        return result

    def cpu_bound_task(self, data):
        # CPU-intensive synchronous processing
        return self.heavy_computation(data)

    async def io_bound_task(self, data):
        # Async I/O operations
        async with aiohttp.ClientSession() as session:
            return await self.call_api(session, data)
```

### Resource Configuration

```python
from pythia.config import WorkerConfig

# High-performance worker configuration
config = WorkerConfig(
    # Increase batch processing
    batch_size=500,
    batch_timeout=2.0,

    # Optimize memory usage
    max_memory_mb=2048,  # 2GB limit

    # Increase concurrency
    max_concurrent_tasks=20,

    # Connection pooling
    max_connections=50,
)
```

## 📊 Load Balancing Strategies

### Consumer Groups (Kafka)

```python
# Worker instance 1
worker1 = Worker()
worker1.source = KafkaConsumer(
    topic="orders",
    consumer_group="order-processors",  # Same group
    partition_assignment_strategy="range"
)

# Worker instance 2
worker2 = Worker()
worker2.source = KafkaConsumer(
    topic="orders",
    consumer_group="order-processors",  # Same group
    partition_assignment_strategy="range"
)
```

### Queue Distribution (RabbitMQ)

```python
from pythia.brokers.rabbitmq import RabbitMQConsumer

# Multiple workers consuming from same queue
class LoadBalancedWorker(Worker):
    source = RabbitMQConsumer(
        queue="tasks",
        prefetch_count=10,  # Process 10 messages at once
        auto_ack=False,     # Manual acknowledgment
    )
```

### Redis Streams Consumer Groups

```python
from pythia.brokers.redis import RedisStreamsConsumer

class StreamWorker(Worker):
    source = RedisStreamsConsumer(
        stream="events",
        consumer_group="processors",
        consumer_name="worker-1",  # Unique name per instance
    )
```

## 🌍 Geographic Distribution

### Multi-Region Setup

```python
# US East worker
class USEastWorker(Worker):
    source = KafkaConsumer(
        topic="orders-us-east",
        bootstrap_servers="kafka-us-east:9092"
    )

# EU West worker
class EUWestWorker(Worker):
    source = KafkaConsumer(
        topic="orders-eu-west",
        bootstrap_servers="kafka-eu-west:9092"
    )
```

### Cross-Region Replication

```yaml
# Kafka cross-region replication
version: '3.8'
services:
  mirror-maker:
    image: confluentinc/cp-kafka:latest
    command: >
      kafka-mirror-maker
      --consumer.config /config/consumer.properties
      --producer.config /config/producer.properties
      --whitelist="orders.*"
    volumes:
      - ./config:/config
```

## 📈 Scaling Patterns

### Fan-Out Pattern

```python
from pythia import Worker
from pythia.brokers.kafka import KafkaConsumer, KafkaProducer

class FanOutWorker(Worker):
    source = KafkaConsumer(topic="incoming")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Multiple output topics for scaling
        self.order_producer = KafkaProducer(topic="orders")
        self.inventory_producer = KafkaProducer(topic="inventory")
        self.billing_producer = KafkaProducer(topic="billing")

    async def process(self, message):
        event = message.body

        # Fan out to multiple specialized workers
        if event["type"] == "order":
            await self.order_producer.send(event)
        elif event["type"] == "inventory":
            await self.inventory_producer.send(event)
        elif event["type"] == "billing":
            await self.billing_producer.send(event)
```

### Pipeline Pattern

```python
# Stage 1: Data ingestion
class DataIngestionWorker(Worker):
    source = KafkaConsumer(topic="raw-data")
    sink = KafkaProducer(topic="validated-data")

    async def process(self, message):
        validated = await self.validate_data(message.body)
        await self.sink.send(validated)

# Stage 2: Data processing
class DataProcessingWorker(Worker):
    source = KafkaConsumer(topic="validated-data")
    sink = KafkaProducer(topic="processed-data")

    async def process(self, message):
        processed = await self.process_data(message.body)
        await self.sink.send(processed)

# Stage 3: Data storage
class DataStorageWorker(Worker):
    source = KafkaConsumer(topic="processed-data")

    async def process(self, message):
        await self.store_data(message.body)
```

## 🔧 Auto-Scaling Configuration

### CPU-Based Scaling

```python
import psutil
from pythia import Worker

class AutoScalingWorker(Worker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cpu_threshold = 80.0
        self.scale_check_interval = 60  # seconds

    async def should_scale_up(self):
        cpu_percent = psutil.cpu_percent(interval=1)
        return cpu_percent > self.cpu_threshold

    async def should_scale_down(self):
        cpu_percent = psutil.cpu_percent(interval=1)
        return cpu_percent < 30.0
```

### Queue-Based Scaling

```python
from pythia.brokers.kafka import KafkaAdmin

class QueueBasedScaling(Worker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.admin = KafkaAdmin()
        self.lag_threshold = 1000

    async def check_consumer_lag(self):
        lag = await self.admin.get_consumer_lag("order-processors", "orders")
        return lag > self.lag_threshold
```

## 🏗️ Infrastructure Scaling

### Container Orchestration

```yaml
# Docker Swarm scaling
version: '3.8'
services:
  worker:
    image: my-app/worker:latest
    deploy:
      replicas: 5
      update_config:
        parallelism: 2
        delay: 10s
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
      resources:
        limits:
          cpus: '0.5'
          memory: 512M
        reservations:
          cpus: '0.25'
          memory: 256M
```

### Service Mesh Integration

```yaml
# Istio service mesh
apiVersion: networking.istio.io/v1alpha3
kind: DestinationRule
metadata:
  name: worker-destination
spec:
  host: worker-service
  trafficPolicy:
    loadBalancer:
      simple: LEAST_CONN
  subsets:
  - name: v1
    labels:
      version: v1
    trafficPolicy:
      connectionPool:
        tcp:
          maxConnections: 10
        http:
          http1MaxPendingRequests: 10
          maxRequestsPerConnection: 2
```

## 📊 Monitoring Scaling

### Metrics Collection

```python
from pythia.monitoring import MetricsCollector
import prometheus_client

class ScalingMetricsWorker(Worker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metrics = MetricsCollector()

        # Prometheus metrics
        self.messages_processed = prometheus_client.Counter(
            'messages_processed_total',
            'Total messages processed'
        )
        self.processing_time = prometheus_client.Histogram(
            'message_processing_seconds',
            'Time spent processing messages'
        )
        self.queue_size = prometheus_client.Gauge(
            'queue_size',
            'Current queue size'
        )

    async def process(self, message):
        with self.processing_time.time():
            result = await self.handle_message(message)
            self.messages_processed.inc()
            return result
```

### Alerting Rules

```yaml
# Prometheus alerting rules
groups:
- name: pythia-scaling
  rules:
  - alert: HighMessageLag
    expr: kafka_consumer_lag_sum > 10000
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High message lag detected"

  - alert: HighCPUUsage
    expr: rate(container_cpu_usage_seconds_total[5m]) * 100 > 80
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Worker CPU usage is high"

  - alert: LowThroughput
    expr: rate(messages_processed_total[5m]) < 10
    for: 3m
    labels:
      severity: warning
    annotations:
      summary: "Message processing throughput is low"
```

## 🎯 Scaling Best Practices

### Design Principles

1. **Stateless Workers**: Design workers without local state
2. **Idempotent Processing**: Handle duplicate messages gracefully
3. **Circuit Breakers**: Implement failure isolation
4. **Graceful Shutdown**: Handle termination signals properly
5. **Health Checks**: Implement readiness and liveness probes

### Configuration Management

```python
import os
from pythia.config import WorkerConfig

def get_scaling_config():
    """Get configuration optimized for current environment"""
    env = os.getenv("ENVIRONMENT", "development")

    if env == "production":
        return WorkerConfig(
            batch_size=1000,
            max_concurrent_tasks=50,
            max_memory_mb=2048,
        )
    elif env == "staging":
        return WorkerConfig(
            batch_size=500,
            max_concurrent_tasks=25,
            max_memory_mb=1024,
        )
    else:
        return WorkerConfig(
            batch_size=100,
            max_concurrent_tasks=10,
            max_memory_mb=512,
        )
```

### Testing Scaling

```python
import asyncio
from pythia.testing import LoadTester

async def test_horizontal_scaling():
    """Test worker performance with multiple instances"""
    load_tester = LoadTester(
        worker_class=OrderProcessor,
        num_workers=10,
        messages_per_second=1000,
        duration_seconds=300,
    )

    results = await load_tester.run()

    assert results.avg_throughput > 800  # msg/sec
    assert results.avg_latency < 0.1     # 100ms
    assert results.error_rate < 0.01     # 1%
```

## 📚 Additional Resources

- [Kafka Scaling Guide](https://kafka.apache.org/documentation/#basic_ops_cluster_expansion)
- [Kubernetes HPA Documentation](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Docker Swarm Scaling](https://docs.docker.com/engine/swarm/services/#scale-a-service)
- [Redis Cluster Setup](https://redis.io/topics/cluster-tutorial)
