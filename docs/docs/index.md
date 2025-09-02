<div align="center">
  <img src="assets/images/logo_full_horizontal.webp" alt="Pythia Framework" width="300"/>
</div>

# Pythia Framework

**Modern Python Worker Framework for All Types of Background Processing**

Pythia is a comprehensive, high-performance Python framework that simplifies creating scalable workers for message processing, background jobs, HTTP polling, and more. Built with production-grade features and proven performance benchmarks.

## 🔄 Worker Types Supported

### 1. **Message-Based Workers**
- **Kafka**, **RabbitMQ**, **Redis** - Event streaming and queuing
- High-throughput message processing with proven benchmarks
- Advanced routing, durability, and error handling patterns

### 2. **Background Job Workers**
- **Async task processing** with priority queues
- **Scheduled jobs** and cron-like functionality
- **Retry logic** with exponential backoff
- Built-in job status tracking and management

### 3. **HTTP-Based Workers**
- **API Pollers** - Fetch data from REST APIs periodically
- **Webhook Processors** - Handle incoming HTTP callbacks
- **Circuit breakers** and retry policies for resilience

### 4. **Database Workers**
- **Change Data Capture (CDC)** - Monitor database changes in real-time
- **Data synchronization** - Cross-database replication and sync
- **PostgreSQL & MySQL support** - Native CDC and sync capabilities

## ⚡ Key Features

- **🚀 High Performance**: Optimized for throughput with async/await support
- **🔄 Multiple Patterns**: Message brokers, job queues, HTTP polling, webhooks
- **📊 Built-in Monitoring**: Comprehensive metrics and observability
- **⚙️ Zero Configuration**: Smart auto-detection with manual override
- **🛡️ Production Ready**: Battle-tested with comprehensive error handling
- **🧪 Fully Tested**: Complete test suite with performance benchmarks

## 📈 Performance Highlights

Based on our comprehensive benchmarking:

| Broker | Throughput | P95 Latency | CPU Usage |
|--------|------------|-------------|-----------|
| **Redis** | 3,304 msg/s | 0.6ms | 4.2% |
| **Kafka** | 1,872 msg/s | 2.0ms | 9.0% |
| **RabbitMQ** | 1,292 msg/s | 0.0ms | 6.8% |

## 🚀 Quick Start Examples

### Message Processing Worker

```python
from pythia.core import Worker, Message
from pythia.config import WorkerConfig

class EventProcessor(Worker):
    async def process_message(self, message: Message):
        print(f"Processing event: {message.body}")
        return {"status": "processed", "event_id": message.message_id}

config = WorkerConfig(broker_type="redis")
worker = EventProcessor(config=config)
```

### Background Job Worker

```python
from pythia.jobs import BackgroundJobWorker, JobProcessor, JobResult

class EmailProcessor(JobProcessor):
    async def process(self, job):
        # Send email logic
        await send_email(job.kwargs['to'], job.kwargs['subject'])
        return JobResult(success=True, result="Email sent")

worker = BackgroundJobWorker(processor=EmailProcessor())
await worker.submit_job("send_welcome", kwargs={"to": "user@example.com"})
```

### HTTP Polling Worker

```python
from pythia.http.poller import HTTPPoller

poller = HTTPPoller(
    url="https://api.example.com/events",
    interval=60,  # Poll every minute
    method="GET"
)

async for message in poller.consume():
    data = json.loads(message.body)
    print(f"Received {len(data)} events from API")
```

### Database CDC Worker

```python
from pythia.brokers.database import CDCWorker, DatabaseChange

class UserChangeProcessor(CDCWorker):
    async def process_change(self, change: DatabaseChange):
        if change.table == "users":
            await self.send_welcome_email(change.new_data)
        return {"processed": True, "change_id": change.primary_key}

worker = UserChangeProcessor(
    connection_string="postgresql://user:pass@localhost/db",
    tables=["users", "orders"],
    poll_interval=5.0
)
```

### HTTP Workers

```python
from pythia.brokers.http import PollerWorker, WebhookSenderWorker

# API Polling Worker
class PaymentStatusPoller(PollerWorker):
    def __init__(self):
        super().__init__(
            url="https://api.payments.com/status",
            interval=30,  # Poll every 30 seconds
            headers={"Authorization": "Bearer your-token"}
        )

    async def process_message(self, message):
        if message.body.get("status") == "completed":
            await self.handle_payment_completed(message.body)

# Webhook Sender Worker
class NotificationSender(WebhookSenderWorker):
    def __init__(self):
        super().__init__(base_url="https://hooks.example.com")

    async def notify_user_created(self, user_data):
        await self.send_webhook(
            endpoint="/notifications",
            data={"event": "user_created", "user": user_data}
        )
```

## 📖 Documentation Structure

- **[Getting Started](getting-started/installation.md)** - Installation and first steps
- **[User Guide](user-guide/worker-lifecycle.md)** - Core concepts and patterns
- **[Broker Integration](brokers/redis.md)** - Specific broker configurations
- **[Performance](performance/benchmarks.md)** - Optimization and scaling guides
- **[API Reference](api/core.md)** - Complete class and method documentation
- **[Examples](examples/basic-worker.md)** - Real-world implementation examples

## 🎯 Why Choose Pythia?

- **Developer Experience**: Intuitive API with comprehensive documentation
- **Performance**: Proven performance with detailed benchmarks
- **Flexibility**: Support multiple message brokers with unified interface
- **Monitoring**: Built-in metrics and observability out of the box
- **Community**: Open source with active development and support

---

<div align="center">
  <img src="assets/images/logo_minimal.webp" alt="Pythia Logo" width="80"/>

  **Ready to build high-performance workers?**

  [Get started](getting-started/installation.md) in less than 5 minutes!
</div>
