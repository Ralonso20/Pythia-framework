# Worker Classes

This reference covers specialized worker classes in the Pythia framework.

## HTTP Workers

### HTTPWorker

::: pythia.brokers.http.HTTPWorker
    Base class for HTTP-based workers.

```python
from pythia.brokers.http import HTTPWorker
import aiohttp

class APIWorker(HTTPWorker):
    def __init__(self, base_url: str, **kwargs):
        super().__init__(**kwargs)
        self.base_url = base_url
        self.session = None

    async def startup(self):
        await super().startup()
        self.session = aiohttp.ClientSession()

    async def shutdown(self):
        if self.session:
            await self.session.close()
        await super().shutdown()
```

#### Configuration

```python
from pythia.config.http import HTTPConfig

http_config = HTTPConfig(
    timeout=30.0,
    max_retries=3,
    retry_delay=1.0,
    max_connections=100,
    enable_circuit_breaker=True,
    circuit_breaker_threshold=5,
    circuit_breaker_timeout=60.0,
)
```

### PollerWorker

::: pythia.brokers.http.PollerWorker
    Worker that polls HTTP endpoints for data.

```python
from pythia.brokers.http import PollerWorker

class APIPollerWorker(PollerWorker):
    def __init__(self, api_url: str, poll_interval: float = 30.0):
        super().__init__(
            url=api_url,
            poll_interval=poll_interval,
            headers={"Accept": "application/json"}
        )

    async def process(self, message):
        # Process data from API
        data = message.body
        processed = await self.transform_data(data)
        return processed

    async def transform_data(self, data):
        # Transform API response
        return {"processed": True, "count": len(data)}
```

#### Configuration Options

- **url**: str - API endpoint to poll
- **poll_interval**: float - Seconds between polls
- **headers**: Dict[str, str] - HTTP headers
- **params**: Dict[str, Any] - Query parameters
- **timeout**: float - Request timeout
- **method**: str - HTTP method (default: GET)

### WebhookSenderWorker

::: pythia.brokers.http.WebhookSenderWorker
    Worker that sends webhooks to external services.

```python
from pythia.brokers.http import WebhookSenderWorker
from pythia.brokers.kafka import KafkaConsumer

class OrderWebhookWorker(WebhookSenderWorker):
    source = KafkaConsumer(topic="orders")

    def __init__(self):
        super().__init__(
            webhook_urls=[
                "https://api.partner1.com/webhooks",
                "https://api.partner2.com/webhooks"
            ],
            headers={"Content-Type": "application/json"},
            timeout=10.0
        )

    async def process(self, message):
        order = message.body
        webhook_payload = {
            "event": "order.created",
            "data": order,
            "timestamp": message.timestamp.isoformat()
        }

        # Send to all webhooks
        await self.broadcast_webhook(webhook_payload)
        return {"webhook_sent": True}
```

#### Methods

```python
async def send_webhook(self, data: Any, url: str = None) -> Dict:
    """Send webhook to specific URL"""
    pass

async def broadcast_webhook(self, data: Any) -> List[Dict]:
    """Send webhook to all configured URLs"""
    pass

async def send_webhook_from_message(self, message: Message) -> Dict:
    """Send webhook from message data"""
    pass
```

## Database Workers

### DatabaseWorker

::: pythia.brokers.database.DatabaseWorker
    Base class for database-based workers.

```python
from pythia.brokers.database import DatabaseWorker
from sqlalchemy.ext.asyncio import create_async_engine

class DBWorker(DatabaseWorker):
    def __init__(self, db_url: str):
        super().__init__()
        self.engine = create_async_engine(db_url)

    async def startup(self):
        await super().startup()
        # Initialize database connection

    async def shutdown(self):
        await self.engine.dispose()
        await super().shutdown()
```

### CDCWorker (Change Data Capture)

::: pythia.brokers.database.CDCWorker
    Worker for monitoring database changes.

```python
from pythia.brokers.database import CDCWorker
from pythia.brokers.kafka import KafkaProducer

class UserCDCWorker(CDCWorker):
    sink = KafkaProducer(topic="user-changes")

    def __init__(self, db_url: str):
        super().__init__(
            db_url=db_url,
            tables=["users", "user_profiles"],
            poll_interval=5.0
        )

    async def process(self, message):
        change = message.body

        # Transform database change to event
        event = {
            "table": change["table"],
            "operation": change["operation"],  # INSERT, UPDATE, DELETE
            "old_values": change.get("old_values"),
            "new_values": change.get("new_values"),
            "timestamp": change["timestamp"]
        }

        await self.send_to_sink(event)
        return event
```

#### Configuration

```python
from pythia.config.database import DatabaseConfig

db_config = DatabaseConfig(
    url="postgresql+asyncpg://user:pass@localhost/db",
    tables=["orders", "users"],
    poll_interval=10.0,
    batch_size=100,
    enable_snapshots=True
)
```

### SyncWorker

::: pythia.brokers.database.SyncWorker
    Worker for synchronizing data between databases.

```python
from pythia.brokers.database import SyncWorker

class DatabaseSyncWorker(SyncWorker):
    def __init__(self, source_url: str, target_url: str):
        super().__init__(
            source_db_url=source_url,
            target_db_url=target_url,
            sync_tables=["products", "inventory"],
            sync_interval=3600.0  # 1 hour
        )

    async def process(self, message):
        sync_data = message.body

        # Apply changes to target database
        await self.apply_changes(sync_data)

        return {"synced_records": len(sync_data["changes"])}

    async def apply_changes(self, sync_data):
        # Custom sync logic
        for change in sync_data["changes"]:
            await self.apply_single_change(change)
```

## Background Job Workers

### JobWorker

::: pythia.jobs.worker.JobWorker
    Worker for processing background jobs.

```python
from pythia.jobs import JobWorker, Job
from pythia.brokers.redis import RedisListConsumer

class BackgroundJobWorker(JobWorker):
    source = RedisListConsumer(queue="background_jobs")

    async def process(self, message):
        job_data = message.body
        job = Job.from_dict(job_data)

        # Execute the job
        result = await self.execute_job(job)

        # Update job status
        await self.update_job_status(job.id, "completed", result)

        return result

    async def execute_job(self, job: Job):
        if job.type == "send_email":
            return await self.send_email(job.payload)
        elif job.type == "process_image":
            return await self.process_image(job.payload)
        else:
            raise ValueError(f"Unknown job type: {job.type}")
```

### ScheduledJobWorker

::: pythia.jobs.scheduler.ScheduledJobWorker
    Worker for scheduled/cron jobs.

```python
from pythia.jobs import ScheduledJobWorker, CronSchedule

class CronWorker(ScheduledJobWorker):
    def __init__(self):
        super().__init__()

        # Add scheduled jobs
        self.add_job(
            func=self.daily_cleanup,
            schedule=CronSchedule(hour=2, minute=0),  # 2 AM daily
            job_id="daily_cleanup"
        )

        self.add_job(
            func=self.send_reports,
            schedule=CronSchedule(day_of_week=1, hour=9),  # Monday 9 AM
            job_id="weekly_reports"
        )

    async def daily_cleanup(self):
        """Daily cleanup task"""
        await self.cleanup_old_files()
        await self.cleanup_temp_data()
        return {"cleaned": True}

    async def send_reports(self):
        """Weekly reports task"""
        report = await self.generate_weekly_report()
        await self.send_email_report(report)
        return {"report_sent": True}
```

## Cloud Workers

### AWS Workers

```python
from pythia.brokers.cloud.aws import SQSConsumer, SNSProducer

class AWSWorker(Worker):
    source = SQSConsumer(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
    )
    sink = SNSProducer(
        topic_arn="arn:aws:sns:us-east-1:123456789012:my-topic"
    )

    async def process(self, message):
        # Process AWS SQS message
        data = message.body
        processed = await self.handle_aws_message(data)

        # Send result to SNS
        await self.send_to_sink(processed)
        return processed
```

### GCP Workers

```python
from pythia.brokers.cloud.gcp import PubSubSubscriber, PubSubPublisher

class GCPWorker(Worker):
    source = PubSubSubscriber(
        subscription_name="projects/my-project/subscriptions/my-subscription"
    )
    sink = PubSubPublisher(
        topic_name="projects/my-project/topics/processed-events"
    )

    async def process(self, message):
        # Process GCP Pub/Sub message
        event_data = message.body
        result = await self.process_event(event_data)

        await self.send_to_sink(result)
        return result
```

### Azure Workers

```python
from pythia.brokers.cloud.azure import ServiceBusConsumer, ServiceBusProducer

class AzureWorker(Worker):
    source = ServiceBusConsumer(queue_name="input-queue")
    sink = ServiceBusProducer(queue_name="output-queue")

    async def process(self, message):
        # Process Azure Service Bus message
        data = message.body
        processed = await self.transform_data(data)

        await self.send_to_sink(processed)
        return processed
```

## Specialized Patterns

### Pipeline Worker

```python
from pythia import Worker
from typing import List, Any

class PipelineWorker(Worker):
    """Worker that processes messages through multiple stages"""

    def __init__(self, stages: List[callable], **kwargs):
        super().__init__(**kwargs)
        self.stages = stages

    async def process(self, message):
        data = message.body

        # Process through each stage
        for i, stage in enumerate(self.stages):
            try:
                data = await self.run_stage(stage, data, i)
            except Exception as e:
                self.logger.error(f"Stage {i} failed: {e}")
                raise

        return data

    async def run_stage(self, stage: callable, data: Any, stage_num: int):
        self.logger.info(f"Running stage {stage_num}")

        if asyncio.iscoroutinefunction(stage):
            return await stage(data)
        else:
            return stage(data)

# Usage
def validate_data(data):
    # Validation logic
    return data

async def enrich_data(data):
    # Enrichment logic
    return data

def transform_data(data):
    # Transformation logic
    return data

pipeline = PipelineWorker(stages=[validate_data, enrich_data, transform_data])
```

### Multi-Source Worker

```python
from pythia import Worker
from pythia.brokers.kafka import KafkaConsumer
from pythia.brokers.redis import RedisStreamsConsumer

class MultiSourceWorker(Worker):
    """Worker that consumes from multiple sources"""

    sources = [
        KafkaConsumer(topic="orders"),
        RedisStreamsConsumer(stream="events"),
    ]

    async def process(self, message):
        # Handle message based on source
        source_type = message.source_info.get("broker_type")

        if source_type == "kafka":
            return await self.handle_kafka_message(message)
        elif source_type == "redis":
            return await self.handle_redis_message(message)
        else:
            return await self.handle_generic_message(message)
```

### Circuit Breaker Worker

```python
from pythia import Worker
from pythia.utils.circuit_breaker import CircuitBreaker

class CircuitBreakerWorker(Worker):
    """Worker with circuit breaker pattern"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
            expected_exception=Exception
        )

    async def process(self, message):
        async with self.circuit_breaker:
            return await self.risky_operation(message)

    async def risky_operation(self, message):
        # Operation that might fail
        result = await self.external_api_call(message.body)
        return result
```
