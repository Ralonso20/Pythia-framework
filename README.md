<div align="center">
  <img src="static/logo_full_horizontal.webp">

  # Pythia - Modern Python Worker Framework

  **A modern library for creating efficient and scalable workers in Python**

  [![PyPI version](https://badge.fury.io/py/pythia-framework.svg)](https://badge.fury.io/py/pythia-framework)
  [![Python Support](https://img.shields.io/pypi/pyversions/pythia-framework.svg)](https://pypi.org/project/pythia-framework/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI/CD Pipeline](https://github.com/Ralonso20/Pythia-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/Ralonso20/Pythia-framework/actions/workflows/ci.yml)
  [![codecov](https://codecov.io/github/Ralonso20/Pythia-framework/graph/badge.svg?token=b6n9A4sNae)](https://codecov.io/github/Ralonso20/Pythia-framework)
  [![Documentation Status](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://pythia-framework.github.io/pythia/)
  [![Code style: black](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
  [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/pythia-framework/pythia/pulls)

</div>

Pythia is a framework that simplifies creating message processing workers, background jobs, and asynchronous tasks. Based on production-proven patterns, it abstracts configuration complexity and allows you to create robust workers in minutes.

## ✨ Key Features

- **⚡ Rapid development**: From days to minutes to create a functional worker
- **🔄 Flexibility**: Support for multiple message brokers (Kafka, RabbitMQ, Redis, Database CDC)
- **🚀 Production ready**: Best practices included by default
- **🎯 Zero configuration**: Automatic configuration from environment variables
- **📝 Type safety**: Native integration with Pydantic v2
- **📊 Observability**: Structured logging and automatic metrics
- **⚙️ Lifecycle management**: Graceful startup/shutdown with signal handling
- **🗄️ Database Workers**: Change Data Capture (CDC) and database synchronization

## 📦 Installation

```bash
# Basic installation
uv add pythia-framework
# or with pip
pip install pythia-framework

# Individual brokers
uv add pythia-framework[kafka]          # Apache Kafka only
uv add pythia-framework[rabbitmq]       # RabbitMQ only
uv add pythia-framework[redis]          # Redis only
uv add pythia-framework[database]       # Database CDC only

# Cloud providers
uv add pythia-framework[aws]            # AWS SQS/SNS only
uv add pythia-framework[gcp]            # Google Pub/Sub only
uv add pythia-framework[azure]          # Azure Service Bus only

# Complete bundles
uv add pythia-framework[brokers]        # All traditional brokers
uv add pythia-framework[cloud]          # All cloud providers
uv add pythia-framework[all]            # Everything included
```

## 🚀 Quick Start

### Basic Worker

```python
from pythia import Worker
from pythia.brokers.kafka import KafkaConsumer
from pydantic import BaseModel

class ApprovalEvent(BaseModel):
    id: str
    status: str
    user_id: str

class ApprovalWorker(Worker):
    source = KafkaConsumer(
        topics=["approvals"],
        group_id="approval-worker"
    )

    async def process(self, event: ApprovalEvent):
        print(f"Processing approval {event.id}: {event.status}")
        # Your business logic here

# Run
if __name__ == "__main__":
    ApprovalWorker().run_sync()
```

### Database CDC Worker

```python
from pythia.brokers.database import CDCWorker, DatabaseChange

class UserCDCWorker(CDCWorker):
    def __init__(self):
        super().__init__(
            connection_string="postgresql://user:pass@localhost/db",
            tables=["users", "orders"],
            poll_interval=5.0
        )

    async def process_change(self, change: DatabaseChange):
        print(f"Change detected in {change.table}: {change.change_type}")
        # Process database change
        return {"processed": True}

# Run
if __name__ == "__main__":
    UserCDCWorker().run_sync()
```

## 📡 Supported Brokers

| Broker | Status | Description |
|--------|--------|-------------|
| **Kafka** | ✅ Stable | Consumer/Producer with confluent-kafka |
| **RabbitMQ** | ✅ Stable | AMQP with aio-pika |
| **Redis** | ✅ Stable | Pub/Sub and Streams |
| **Database CDC** | ✅ Stable | Change Data Capture with SQLAlchemy |
| **HTTP** | ✅ Stable | HTTP polling and webhooks |
| **AWS SQS/SNS** | ✅ Stable | Amazon SQS consumer and SNS producer |
| **GCP Pub/Sub** | ✅ Stable | Google Cloud Pub/Sub subscriber and publisher |
| **Azure Service Bus** | ✅ Stable | Azure Service Bus consumer and producer |
| **Azure Storage Queue** | ✅ Stable | Azure Storage Queue consumer and producer |

## 🗄️ Database Workers

### Change Data Capture (CDC)

```python
from pythia.brokers.database import CDCWorker, DatabaseChange, ChangeType

class OrderCDCWorker(CDCWorker):
    def __init__(self):
        super().__init__(
            connection_string="postgresql://localhost/ecommerce",
            tables=["orders", "payments"],
            poll_interval=2.0,
            timestamp_column="updated_at"
        )

    async def process_change(self, change: DatabaseChange):
        if change.change_type == ChangeType.INSERT:
            await self.handle_new_record(change)
        elif change.change_type == ChangeType.UPDATE:
            await self.handle_updated_record(change)

        return {"status": "processed", "table": change.table}
```

### Database Synchronization

```python
from pythia.brokers.database import SyncWorker

class DataSyncWorker(SyncWorker):
    def __init__(self):
        super().__init__(
            source_connection="postgresql://prod-db/main",
            target_connection="postgresql://analytics-db/replica",
            sync_config={
                "mode": "incremental",  # or "full"
                "batch_size": 1000,
                "timestamp_column": "updated_at"
            }
        )

    async def process(self):
        # Sync specific tables
        await self.sync_table("users")
        await self.sync_table("orders")
```

## 📡 HTTP Workers

### API Polling Worker

```python
from pythia.brokers.http import PollerWorker

class PaymentStatusPoller(PollerWorker):
    def __init__(self):
        super().__init__(
            url="https://api.payments.com/status",
            interval=30,  # Poll every 30 seconds
            method="GET",
            headers={"Authorization": "Bearer your-token"}
        )

    async def process_message(self, message):
        # Process API response
        data = message.body
        if data.get("status") == "completed":
            await self.handle_payment_completed(data)

        return {"processed": True}
```

### Webhook Sender Worker

```python
from pythia.brokers.http import WebhookSenderWorker

class NotificationWebhook(WebhookSenderWorker):
    def __init__(self):
        super().__init__(base_url="https://hooks.example.com")

    async def process(self):
        # Send webhooks based on your logic
        await self.send_webhook(
            endpoint="/notifications",
            data={"event": "user_created", "user_id": 123}
        )

        # Broadcast to multiple endpoints
        await self.broadcast_webhook(
            endpoints=["/webhook1", "/webhook2"],
            data={"event": "system_alert", "level": "warning"}
        )
```

## ☁️ Cloud Workers

### AWS SQS Consumer

```python
from pythia.brokers.cloud.aws import SQSConsumer
from pythia.config.cloud import AWSConfig

class OrderProcessor(SQSConsumer):
    def __init__(self):
        aws_config = AWSConfig(
            region="us-east-1",
            queue_url="https://sqs.us-east-1.amazonaws.com/123/orders"
        )
        super().__init__(
            queue_url="https://sqs.us-east-1.amazonaws.com/123/orders",
            aws_config=aws_config
        )

    async def process_message(self, message):
        # Process SQS message
        order_data = message.body
        print(f"Processing order: {order_data.get('order_id')}")
        return {"processed": True}

# Run
if __name__ == "__main__":
    OrderProcessor().run_sync()
```

### AWS SNS Producer

```python
from pythia.brokers.cloud.aws import SNSProducer

class NotificationSender(SNSProducer):
    def __init__(self):
        super().__init__(topic_arn="arn:aws:sns:us-east-1:123:notifications")

    async def send_user_notification(self, user_data):
        await self.publish_message(
            message={"event": "user_created", "user_id": user_data["id"]},
            subject="New User Registration"
        )
```

### GCP Pub/Sub Subscriber

```python
from pythia.brokers.cloud.gcp import PubSubSubscriber
from pythia.config.cloud import GCPConfig

class MessageProcessor(PubSubSubscriber):
    def __init__(self):
        gcp_config = GCPConfig(
            project_id="my-gcp-project",
            subscription_name="message-subscription"
        )
        super().__init__(
            subscription_path="projects/my-gcp-project/subscriptions/message-subscription",
            gcp_config=gcp_config
        )

    async def process_message(self, message):
        # Process Pub/Sub message
        data = message.body
        print(f"Processing message: {data.get('event_type')}")
        return {"processed": True}

# Run
if __name__ == "__main__":
    MessageProcessor().run_sync()
```

### GCP Pub/Sub Publisher

```python
from pythia.brokers.cloud.gcp import PubSubPublisher

class EventPublisher(PubSubPublisher):
    def __init__(self):
        super().__init__(topic_path="projects/my-gcp-project/topics/events")

    async def publish_event(self, event_data):
        await self.publish_message(
            message={"event": "user_activity", "data": event_data},
            attributes={"source": "user-service", "timestamp": "2025-09-01"},
            ordering_key=f"user-{event_data.get('user_id')}"
        )
```

### Azure Service Bus Consumer

```python
from pythia.brokers.cloud.azure import ServiceBusConsumer
from pythia.config.cloud import AzureConfig

class OrderProcessor(ServiceBusConsumer):
    def __init__(self):
        azure_config = AzureConfig(
            service_bus_connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test",
            service_bus_queue_name="orders"
        )
        super().__init__(
            queue_name="orders",
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test",
            azure_config=azure_config
        )

    async def process_message(self, message):
        # Process Service Bus message
        order_data = message.body
        print(f"Processing order: {order_data.get('order_id')}")
        return {"processed": True}

# Run
if __name__ == "__main__":
    OrderProcessor().run_sync()
```

### Azure Storage Queue Producer

```python
from pythia.brokers.cloud.azure import StorageQueueProducer

class TaskProducer(StorageQueueProducer):
    def __init__(self):
        super().__init__(
            queue_name="tasks",
            connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net"
        )

    async def send_task(self, task_data):
        await self.send_message(
            message={"task": "process_image", "image_id": task_data["id"]},
            visibility_timeout=60,
            time_to_live=3600  # 1 hour TTL
        )
```

## 🔧 Configuration

### Environment Variables

```bash
# Worker config
PYTHIA_WORKER_NAME=my-worker
PYTHIA_LOG_LEVEL=INFO
PYTHIA_MAX_RETRIES=3

# Kafka config
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=my-group
KAFKA_TOPICS=events,notifications

# Database config
DATABASE_URL=postgresql://user:pass@localhost/db
DATABASE_POLL_INTERVAL=5.0

# AWS config
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123/queue
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123:topic

# GCP config
GCP_PROJECT_ID=my-gcp-project
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
PUBSUB_SUBSCRIPTION=projects/my-project/subscriptions/my-subscription
PUBSUB_TOPIC=projects/my-project/topics/my-topic

# Azure config
AZURE_SERVICE_BUS_CONNECTION_STRING=Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test
SERVICE_BUS_QUEUE_NAME=orders
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net
STORAGE_QUEUE_NAME=tasks
```

## 🧪 Testing

```bash
# Install development dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Tests with coverage
uv run pytest --cov=pythia

# Linting
uv run ruff check .
uv run ruff format .
```

## 🏗️ Project Structure

```
pythia/
├── core/                   # Core framework
│   ├── worker.py          # Base Worker class
│   ├── message.py         # Message abstraction
│   └── lifecycle.py       # Lifecycle management
├── brokers/               # Broker adapters
│   ├── kafka/            # Kafka (Confluent)
│   ├── rabbitmq/         # RabbitMQ (aio-pika)
│   ├── redis/            # Redis Pub/Sub and Streams
│   └── database/         # Database CDC and Sync
├── config/               # Configuration system
├── logging/              # Logging with Loguru
└── utils/                # Utilities
```

## 🗺️ Roadmap

- [x] **Core Framework** - Worker base, lifecycle, configuration
- [x] **Kafka Integration** - Consumer/Producer con confluent-kafka
- [x] **RabbitMQ Support** - Consumer/Producer con aio-pika
- [x] **Redis Support** - Pub/Sub y Streams
- [x] **Database Workers** - CDC y sincronización con SQLAlchemy
- [x] **HTTP Workers** - Webhooks, polling, HTTP clients
- [X] **CLI Tools** - Worker generation, monitoring
- [X] **Cloud Brokers** - AWS SQS/SNS, GCP Pub/Sub, Azure Service Bus
- [ ] **Monitoring Dashboard** - Metrics, health checks, web UI

## 📖 Documentation

Visit our complete documentation at: [https://ralonso20.github.io/pythia/](https://ralonso20.github.io/pythia/) (coming soon)

## 🤝 Contributing

1. Fork the project
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for more details.

## 🎯 Inspiration

Pythia is inspired by frameworks like:
- **Celery** (Python) - For distributed workers
- **Apache Kafka Streams** (Java) - For stream processing
- **Spring Boot** (Java) - For automatic configuration
- **Sidekiq** (Ruby) - For simplicity and elegance

## 📞 Support

- 📧 **Issues**: [GitHub Issues](https://github.com/ralonso20/pythia/issues)
- 📚 **Documentation**: [**Documentation**](https://ralonso20.github.io/pythia/)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/ralonso20/pythia/discussions)

---

**Pythia**: *From configuration complexity to business logic simplicity.*
