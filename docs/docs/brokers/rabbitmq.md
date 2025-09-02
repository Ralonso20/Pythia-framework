# RabbitMQ Integration

Complete guide to using Pythia with RabbitMQ for advanced message routing and enterprise messaging patterns.

## Overview

RabbitMQ integration in Pythia provides enterprise-grade messaging with sophisticated routing, reliability guarantees, and management features. Our benchmarks achieved 1,292 msg/s with excellent resource efficiency (6.8% CPU usage).

!!! info "Enterprise Messaging"
    RabbitMQ excels at complex routing scenarios, message durability, and enterprise integration patterns that Redis and Kafka don't natively support.

## Quick Start

```python
from pythia.core import Worker
from pythia.config import WorkerConfig
from pythia.config.rabbitmq import RabbitMQConfig

# Basic RabbitMQ configuration
rabbitmq_config = RabbitMQConfig(
    url="amqp://guest:guest@localhost:5672/",
    queue="task-queue",
    exchange="tasks-exchange",
    routing_key="tasks"
)

config = WorkerConfig(broker_type="rabbitmq")
```

## Configuration Options

### Basic Queue Configuration

```python
from pythia.config.rabbitmq import RabbitMQConfig

# Simple work queue pattern
rabbitmq_config = RabbitMQConfig(
    url="amqp://user:password@localhost:5672/vhost",
    queue="work-queue",
    durable=True,                # Survive broker restarts
    auto_ack=False,             # Manual acknowledgment
    prefetch_count=10           # Fair dispatch
)
```

### Advanced Exchange Patterns

#### Direct Exchange (Point-to-Point)

```python
rabbitmq_config = RabbitMQConfig(
    url="amqp://localhost:5672/",
    queue="orders-queue",
    exchange="orders-exchange",
    routing_key="order.created",
    durable=True
)
```

#### Topic Exchange (Pattern Matching)

```python
rabbitmq_config = RabbitMQConfig(
    url="amqp://localhost:5672/",
    queue="notifications-queue",
    exchange="events-exchange",
    routing_key="user.*.created",    # Wildcard routing
    durable=True
)
```

#### Fanout Exchange (Broadcast)

```python
rabbitmq_config = RabbitMQConfig(
    url="amqp://localhost:5672/",
    queue="audit-queue",
    exchange="broadcast-exchange",
    routing_key="",                  # Ignored in fanout
    durable=True
)
```

### High-Performance Configuration

Based on our benchmarks (1,292 msg/s), optimized settings:

```python
rabbitmq_config = RabbitMQConfig(
    url="amqp://localhost:5672/",
    queue="high-throughput-queue",
    exchange="performance-exchange",
    routing_key="perf",

    # Performance optimization
    durable=True,                    # Persistence for reliability
    auto_ack=False,                 # Manual ack for safety
    prefetch_count=100,             # Higher prefetch for throughput

    # Connection optimization
    heartbeat=600,                  # 10 minutes heartbeat
    connection_attempts=5,          # More retry attempts
    retry_delay=1.0                # Faster reconnection
)
```

## Working with RabbitMQ

### Basic Consumer Worker

```python
import json
import asyncio
from typing import Any, Dict
from pythia.core import Worker, Message
from pythia.config import WorkerConfig
from pythia.config.rabbitmq import RabbitMQConfig

class EmailNotificationWorker(Worker):
    """Process email notifications from RabbitMQ"""

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process an email notification"""
        try:
            # Parse RabbitMQ message
            notification_data = json.loads(message.body)

            self.logger.info(
                f"Processing notification {notification_data.get('id')}",
                extra={
                    "notification_type": notification_data.get("type"),
                    "exchange": message.exchange,
                    "routing_key": message.routing_key,
                    "delivery_tag": message.delivery_tag
                }
            )

            # Business logic
            result = await self._send_notification(notification_data)

            # Manual acknowledgment (important for reliability)
            await message.ack()

            return result

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in message: {e}")
            # Reject message and don't requeue
            await message.nack(requeue=False)
            raise

        except Exception as e:
            self.logger.error(f"Failed to process notification: {e}")
            # Reject and requeue for retry
            await message.nack(requeue=True)
            raise

    async def _send_notification(self, notification_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send the notification (business logic)"""
        # Simulate notification sending
        await asyncio.sleep(0.1)

        notification_type = notification_data.get("type")
        recipient = notification_data.get("recipient")

        # Different handling based on type
        if notification_type == "email":
            await self._send_email(recipient, notification_data)
        elif notification_type == "sms":
            await self._send_sms(recipient, notification_data)
        elif notification_type == "push":
            await self._send_push(recipient, notification_data)

        return {
            "status": "sent",
            "type": notification_type,
            "recipient": recipient,
            "processed_by": self.config.worker_id
        }

    async def _send_email(self, recipient: str, data: Dict):
        """Send email notification"""
        self.logger.info(f"Sending email to {recipient}")
        # Email sending logic here

    async def _send_sms(self, recipient: str, data: Dict):
        """Send SMS notification"""
        self.logger.info(f"Sending SMS to {recipient}")
        # SMS sending logic here

    async def _send_push(self, recipient: str, data: Dict):
        """Send push notification"""
        self.logger.info(f"Sending push notification to {recipient}")
        # Push notification logic here

# Configuration
rabbitmq_config = RabbitMQConfig(
    url="amqp://localhost:5672/",
    queue="notifications-queue",
    exchange="notifications-exchange",
    routing_key="notification.*",
    prefetch_count=20
)

config = WorkerConfig(
    worker_name="notification-worker",
    broker_type="rabbitmq",
    max_concurrent=10
)

# Run worker
if __name__ == "__main__":
    worker = EmailNotificationWorker(config=config)
    asyncio.run(worker.start())
```

### Publisher Integration

```python
from pythia.brokers.rabbitmq.producer import RabbitMQProducer

class OrderProcessorWithNotifications(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Setup publisher for notifications
        self.publisher = RabbitMQProducer(rabbitmq_config)

    async def process_message(self, message: Message) -> Dict[str, Any]:
        order_data = json.loads(message.body)
        result = await self._process_order(order_data)

        # Publish notification based on result
        await self._publish_notifications(order_data, result)

        return result

    async def _publish_notifications(self, order_data: Dict, result: Dict):
        """Publish different notifications based on processing result"""

        # Email notification
        email_notification = {
            "type": "email",
            "recipient": order_data["customer_email"],
            "template": "order_confirmation",
            "data": result
        }

        await self.publisher.publish(
            message=json.dumps(email_notification),
            exchange="notifications-exchange",
            routing_key="notification.email"
        )

        # SMS for high-value orders
        if order_data.get("amount", 0) > 1000:
            sms_notification = {
                "type": "sms",
                "recipient": order_data["customer_phone"],
                "template": "high_value_order",
                "data": result
            }

            await self.publisher.publish(
                message=json.dumps(sms_notification),
                exchange="notifications-exchange",
                routing_key="notification.sms.priority"
            )

        # Internal audit notification
        audit_notification = {
            "type": "audit",
            "order_id": order_data["order_id"],
            "amount": order_data["amount"],
            "processed_by": self.config.worker_id
        }

        await self.publisher.publish(
            message=json.dumps(audit_notification),
            exchange="audit-exchange",  # Different exchange
            routing_key="order.processed"
        )

    async def on_shutdown(self):
        """Cleanup publisher on shutdown"""
        await self.publisher.close()
        await super().on_shutdown()
```

## Exchange Types & Routing Patterns

### 1. Direct Exchange (Exact Matching)

```python
# Producer
await publisher.publish(
    message=json.dumps({"task": "send_email"}),
    exchange="tasks-direct",
    routing_key="email.send"
)

# Consumer
rabbitmq_config = RabbitMQConfig(
    queue="email-queue",
    exchange="tasks-direct",
    routing_key="email.send"  # Exact match required
)
```

### 2. Topic Exchange (Pattern Matching)

```python
# Producer - multiple routing keys
await publisher.publish(
    message=json.dumps({"event": "user_registered"}),
    exchange="events-topic",
    routing_key="user.registered.premium"
)

# Consumer - pattern matching
rabbitmq_config = RabbitMQConfig(
    queue="user-events-queue",
    exchange="events-topic",
    routing_key="user.*"        # Matches user.registered, user.updated, etc.
)

# More specific pattern
rabbitmq_config = RabbitMQConfig(
    queue="premium-events-queue",
    exchange="events-topic",
    routing_key="*.*.premium"   # Matches any.any.premium
)
```

### 3. Fanout Exchange (Broadcast)

```python
# Producer
await publisher.publish(
    message=json.dumps({"alert": "system_maintenance"}),
    exchange="broadcast-fanout",
    routing_key=""  # Ignored in fanout
)

# Multiple consumers receive the same message
consumer1_config = RabbitMQConfig(
    queue="alerts-email-queue",
    exchange="broadcast-fanout"
)

consumer2_config = RabbitMQConfig(
    queue="alerts-sms-queue",
    exchange="broadcast-fanout"
)
```

### 4. Headers Exchange (Attribute Matching)

```python
# Producer with headers
await publisher.publish(
    message=json.dumps({"task": "process"}),
    exchange="tasks-headers",
    routing_key="",
    headers={
        "priority": "high",
        "department": "finance",
        "region": "us-east"
    }
)

# Consumer matching headers
rabbitmq_config = RabbitMQConfig(
    queue="priority-queue",
    exchange="tasks-headers",
    headers_match={
        "priority": "high",
        "x-match": "any"  # Match any header
    }
)
```

## Message Durability & Reliability

### Durable Queues & Messages

```python
# Durable configuration for reliability
rabbitmq_config = RabbitMQConfig(
    url="amqp://localhost:5672/",
    queue="critical-tasks",
    exchange="critical-exchange",
    routing_key="critical",

    # Durability settings
    durable=True,                    # Queue survives broker restart
    auto_ack=False,                 # Manual acknowledgment required

    # Message persistence (set by publisher)
    delivery_mode=2                 # Persistent messages
)
```

### Dead Letter Exchange (DLQ)

```python
class TaskWorkerWithDLQ(Worker):
    async def process_message(self, message: Message) -> Any:
        try:
            return await self._process_task(message)
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")

            # Check retry count from headers
            retry_count = message.headers.get("x-retry-count", 0)

            if retry_count >= 3:
                # Send to DLQ by rejecting without requeue
                await message.nack(requeue=False)
                return None
            else:
                # Increment retry count and requeue
                message.headers["x-retry-count"] = retry_count + 1
                await message.nack(requeue=True)
                raise

# Queue configuration with DLQ
rabbitmq_config = RabbitMQConfig(
    queue="tasks-queue",
    exchange="tasks-exchange",
    routing_key="task",

    # DLQ configuration
    queue_arguments={
        "x-dead-letter-exchange": "dlq-exchange",
        "x-dead-letter-routing-key": "failed-task",
        "x-message-ttl": 300000,  # 5 minutes TTL
        "x-max-retries": 3
    }
)
```

## Error Handling & Resilience

### Connection Resilience

```python
from pythia.config import ResilienceConfig

# Robust connection handling
resilience_config = ResilienceConfig(
    max_retries=5,
    retry_delay=2.0,
    retry_backoff=1.5,              # Linear backoff for RabbitMQ
    retry_max_delay=30.0,

    # RabbitMQ-specific timeouts
    connection_timeout=30,
    processing_timeout=300
)

rabbitmq_config = RabbitMQConfig(
    url="amqp://localhost:5672/",

    # Connection resilience
    connection_attempts=5,          # Retry connection
    retry_delay=2.0,               # Delay between attempts
    heartbeat=300,                 # 5 minute heartbeat

    # Channel settings
    prefetch_count=50,             # Balance throughput/memory
    auto_ack=False                 # Manual ack for reliability
)
```

### Message Acknowledgment Patterns

```python
class ReliableMessageProcessor(Worker):
    async def process_message(self, message: Message) -> Any:
        """Reliable message processing with proper acknowledgment"""
        try:
            # Process the message
            result = await self._process_business_logic(message)

            # Only acknowledge after successful processing
            await message.ack()

            return result

        except RecoverableError as e:
            # Temporary error - requeue for retry
            self.logger.warning(f"Recoverable error, requeueing: {e}")
            await message.nack(requeue=True)

        except PermanentError as e:
            # Permanent error - don't requeue
            self.logger.error(f"Permanent error, discarding: {e}")
            await message.nack(requeue=False)

        except Exception as e:
            # Unknown error - be conservative and requeue
            self.logger.error(f"Unknown error, requeueing: {e}")
            await message.nack(requeue=True)
            raise
```

## Monitoring & Management

### RabbitMQ Management Integration

```python
from pythia.config import MetricsConfig

# RabbitMQ-specific monitoring
metrics_config = MetricsConfig(
    enabled=True,
    prometheus_enabled=True,
    custom_metrics={
        "rabbitmq_queue_depth": True,        # Monitor queue lengths
        "rabbitmq_connection_count": True,   # Active connections
        "rabbitmq_consumer_count": True,     # Active consumers
        "rabbitmq_message_rates": True,      # Publish/deliver rates
        "rabbitmq_memory_usage": True        # Broker memory usage
    }
)
```

### Queue Monitoring

```python
class MonitoredRabbitMQWorker(Worker):
    async def on_startup(self):
        # Setup queue monitoring
        self.queue_monitor = RabbitMQQueueMonitor(
            management_url="http://localhost:15672",
            username="guest",
            password="guest",
            queues=["notifications-queue", "tasks-queue"]
        )
        await self.queue_monitor.start()

    async def process_message(self, message: Message) -> Any:
        # Record processing metrics
        with self.metrics.timer("rabbitmq_message_processing_time"):
            result = await self._process_message(message)

        # Update counters
        self.metrics.counter("rabbitmq_messages_processed").inc()

        # Monitor queue depth
        queue_depth = await self.queue_monitor.get_queue_depth("notifications-queue")
        self.metrics.gauge("rabbitmq_queue_depth").set(queue_depth)

        return result
```

## Testing

### Unit Testing

```python
import pytest
from pythia.utils.testing import WorkerTestCase
from unittest.mock import AsyncMock

class TestRabbitMQWorker(WorkerTestCase):
    def setup_method(self):
        # Mock RabbitMQ connections for testing
        self.mock_connection = AsyncMock()
        self.mock_channel = AsyncMock()

    async def test_notification_processing(self):
        # Create test message
        notification_data = {
            "type": "email",
            "recipient": "user@example.com",
            "template": "welcome",
            "data": {"name": "John"}
        }

        message = self.create_test_message(
            body=json.dumps(notification_data),
            exchange="notifications-exchange",
            routing_key="notification.email"
        )

        # Process message
        result = await self.worker.process_message(message)

        assert result["status"] == "sent"
        assert result["type"] == "email"
        assert result["recipient"] == "user@example.com"

    async def test_error_handling(self):
        # Create invalid message
        invalid_message = self.create_test_message(
            body="invalid json"
        )

        # Should handle gracefully
        with pytest.raises(json.JSONDecodeError):
            await self.worker.process_message(invalid_message)
```

### Integration Testing

```python
import pika
import json

class TestRabbitMQIntegration:
    def setup_method(self):
        # Setup test RabbitMQ connection
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()

        # Declare test queue
        self.channel.queue_declare(queue='test-queue', durable=True)

    def test_end_to_end_processing(self):
        # Send test message
        test_message = {
            "task": "test_task",
            "data": {"key": "value"}
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='test-queue',
            body=json.dumps(test_message),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent
        )

        # Worker should process this message
        # Add assertions based on expected side effects

    def teardown_method(self):
        self.channel.queue_delete(queue='test-queue')
        self.connection.close()
```

## Production Deployment

### Docker Compose with RabbitMQ

```yaml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3.12-management-alpine
    hostname: rabbitmq
    ports:
      - "5672:5672"      # AMQP port
      - "15672:15672"    # Management UI
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: secure_password
      RABBITMQ_DEFAULT_VHOST: production
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf

  pythia-worker:
    image: my-rabbitmq-worker
    depends_on:
      - rabbitmq
    environment:
      - PYTHIA_BROKER_TYPE=rabbitmq
      - RABBITMQ_URL=amqp://admin:secure_password@rabbitmq:5672/production
      - RABBITMQ_QUEUE=production-queue
      - PYTHIA_MAX_CONCURRENT=20
    deploy:
      replicas: 3

volumes:
  rabbitmq_data:
```

### Production RabbitMQ Configuration

```conf
# rabbitmq.conf
# Memory and disk thresholds
vm_memory_high_watermark.relative = 0.6
disk_free_limit.relative = 1.0

# Connection limits
num_acceptors.tcp = 10
handshake_timeout = 10000

# Queue settings
queue_master_locator = min-masters
```

### Production Python Configuration

```python
# Production-optimized RabbitMQ configuration
production_rabbitmq_config = RabbitMQConfig(
    url="amqp://admin:secure_password@rabbitmq-cluster:5672/production",

    # Queue settings
    queue="production-tasks",
    exchange="production-exchange",
    routing_key="task",
    durable=True,

    # Performance optimization
    prefetch_count=100,             # Higher prefetch for throughput
    auto_ack=False,                # Reliability over speed

    # Connection optimization
    heartbeat=300,                 # 5 minutes
    connection_attempts=5,         # Retry connections
    retry_delay=1.0,              # Quick retry

    # Security
    verify_ssl=True,
    ca_cert_path="/etc/ssl/ca.crt"
)
```

## Benchmark Results

Our RabbitMQ integration achieved solid performance with excellent resource efficiency:

| Metric | Value |
|--------|-------|
| **Throughput** | 1,292 msg/s |
| **P95 Latency** | 0.0ms* |
| **P99 Latency** | 0.0ms* |
| **CPU Usage** | **6.8%** |
| **Memory Usage** | 7,893 MB |
| **Error Rate** | 0% |

*_Simplified latency measurement in benchmarks_

## Troubleshooting

### Common Issues

1. **Connection drops**
   ```python
   # Increase heartbeat interval
   rabbitmq_config = RabbitMQConfig(
       heartbeat=600,
       connection_attempts=10,
       retry_delay=2.0
   )
   ```

2. **Queue buildup**
   ```python
   # Increase prefetch and workers
   rabbitmq_config = RabbitMQConfig(
       prefetch_count=200,
       # Scale workers horizontally
   )
   ```

3. **Memory issues on broker**
   ```bash
   # Check RabbitMQ memory usage
   rabbitmqctl status

   # Set memory high watermark
   rabbitmqctl set_vm_memory_high_watermark 0.4
   ```

## RabbitMQ vs Other Brokers

| Feature | RabbitMQ | Kafka | Redis |
|---------|----------|-------|-------|
| **Routing** | ✅ Advanced | ❌ Topic-based | ❌ Basic |
| **Durability** | ✅ Excellent | ✅ Excellent | ⚠️ Limited |
| **Management** | ✅ Built-in UI | ⚠️ External tools | ⚠️ CLI only |
| **Throughput** | ⚠️ Medium | ✅ High | ✅ Highest |
| **Latency** | ✅ Low | ⚠️ Medium | ✅ Lowest |
| **Complexity** | ⚠️ High | ⚠️ High | ✅ Simple |

## Next Steps

- [Performance Benchmarks](../performance/benchmarks.md) - Complete broker comparison
- [Configuration Guide](../user-guide/configuration.md) - Advanced configuration patterns
- [Examples](../examples/rabbitmq-worker.md) - Real-world implementation examples
