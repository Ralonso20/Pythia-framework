# Basic Worker Examples

Collection of basic worker examples demonstrating different patterns and use cases.

## Message Processing Worker

Simple message processing worker for handling events:

```python
import asyncio
import json
from typing import Any, Dict
from pythia.core import Worker, Message
from pythia.config import WorkerConfig
from pythia.config.redis import RedisConfig

class EventProcessor(Worker):
    """Basic event processing worker"""

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process a single event"""
        try:
            # Parse event data
            event_data = json.loads(message.body)

            self.logger.info(f"Processing event: {event_data.get('type')}")

            # Process based on event type
            if event_data['type'] == 'user_registered':
                return await self._handle_user_registration(event_data)
            elif event_data['type'] == 'order_created':
                return await self._handle_order_creation(event_data)
            else:
                return await self._handle_generic_event(event_data)

        except Exception as e:
            self.logger.error(f"Failed to process event: {e}")
            raise

    async def _handle_user_registration(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle user registration event"""
        user_id = event['data']['user_id']
        email = event['data']['email']

        # Send welcome email (simulate)
        await asyncio.sleep(0.1)

        return {
            "status": "processed",
            "action": "welcome_email_sent",
            "user_id": user_id,
            "email": email
        }

    async def _handle_order_creation(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle order creation event"""
        order_id = event['data']['order_id']

        # Process order (simulate)
        await asyncio.sleep(0.2)

        return {
            "status": "processed",
            "action": "order_confirmed",
            "order_id": order_id
        }

    async def _handle_generic_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle generic events"""
        return {
            "status": "processed",
            "action": "logged",
            "event_type": event.get('type', 'unknown')
        }

# Configuration and startup
async def main():
    # Redis configuration
    redis_config = RedisConfig(
        host="localhost",
        port=6379,
        queue="events-queue"
    )

    # Worker configuration
    config = WorkerConfig(
        worker_name="event-processor",
        broker_type="redis",
        max_concurrent=5,
        log_level="INFO"
    )

    # Create and start worker
    worker = EventProcessor(config=config)
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
```

## Background Job Worker

Background job processing with retry logic:

```python
import asyncio
import smtplib
from email.mime.text import MIMEText
from typing import Dict, Any
from pythia.jobs import BackgroundJobWorker, JobProcessor, Job, JobResult
from pythia.jobs.queue import MemoryJobQueue
from pythia.config import WorkerConfig

class EmailJobProcessor(JobProcessor):
    """Process email sending jobs"""

    def __init__(self):
        self.smtp_server = "smtp.example.com"
        self.smtp_port = 587

    async def process(self, job: Job) -> JobResult:
        """Process email job"""
        try:
            # Extract job data
            email_data = job.kwargs

            # Send email
            await self._send_email(
                to_email=email_data['to'],
                subject=email_data['subject'],
                body=email_data['body']
            )

            return JobResult(
                success=True,
                result={"email_sent": True, "recipient": email_data['to']}
            )

        except Exception as e:
            return JobResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__
            )

    async def _send_email(self, to_email: str, subject: str, body: str):
        """Send email via SMTP (simulated)"""
        # Simulate email sending
        await asyncio.sleep(0.5)

        print(f"📧 Sending email to {to_email}")
        print(f"   Subject: {subject}")
        print(f"   Body: {body[:50]}...")

# Example usage
async def run_email_worker():
    """Run email background job worker"""

    # Create job queue
    queue = MemoryJobQueue()

    # Create email processor
    processor = EmailJobProcessor()

    # Create worker
    worker = BackgroundJobWorker(
        queue=queue,
        processor=processor,
        max_concurrent_jobs=10,
        polling_interval=1.0
    )

    # Submit some test jobs
    await worker.submit_job(
        name="welcome_email",
        func="send_email",
        kwargs={
            "to": "user@example.com",
            "subject": "Welcome!",
            "body": "Thanks for joining our service!"
        }
    )

    await worker.submit_job(
        name="password_reset",
        func="send_email",
        kwargs={
            "to": "user2@example.com",
            "subject": "Password Reset",
            "body": "Click here to reset your password"
        }
    )

    # Start processing
    print("🚀 Starting email worker...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(run_email_worker())
```

## HTTP Polling Worker

Polling external APIs for data:

```python
import asyncio
import json
from typing import Dict, Any, List
from pythia.core import Worker, Message
from pythia.http.poller import HTTPPoller
from pythia.config import WorkerConfig

class APIPollingWorker(Worker):
    """Worker that polls external APIs"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Setup HTTP poller
        self.poller = HTTPPoller(
            url="https://api.github.com/repos/python/cpython/events",
            interval=300,  # Poll every 5 minutes
            method="GET",
            headers={"Accept": "application/vnd.github.v3+json"}
        )

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process polled API data"""
        try:
            # Parse GitHub events
            events = json.loads(message.body)

            processed_events = []
            for event in events[:5]:  # Process first 5 events
                processed_event = await self._process_github_event(event)
                processed_events.append(processed_event)

            return {
                "status": "processed",
                "events_processed": len(processed_events),
                "events": processed_events
            }

        except Exception as e:
            self.logger.error(f"Failed to process API data: {e}")
            raise

    async def _process_github_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single GitHub event"""
        return {
            "id": event.get("id"),
            "type": event.get("type"),
            "actor": event.get("actor", {}).get("login"),
            "repo": event.get("repo", {}).get("name"),
            "created_at": event.get("created_at")
        }

    async def start_polling(self):
        """Start the HTTP poller"""
        async for message in self.poller.consume():
            await self.process_message(message)

# Run the polling worker
async def main():
    config = WorkerConfig(
        worker_name="github-poller",
        log_level="INFO"
    )

    worker = APIPollingWorker(config)
    await worker.start_polling()

if __name__ == "__main__":
    asyncio.run(main())
```

## Webhook Processing Worker

Processing incoming webhooks:

```python
import asyncio
import json
import hmac
import hashlib
from typing import Dict, Any
from pythia.core import Worker, Message
from pythia.http.webhook import WebhookClient
from pythia.config import WorkerConfig

class WebhookProcessor(Worker):
    """Process incoming webhook events"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Setup webhook client for responses
        self.webhook_client = WebhookClient(
            base_url="https://api.partner.com"
        )

        self.webhook_secret = "your-webhook-secret"

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process webhook message"""
        try:
            # Extract webhook data
            webhook_data = json.loads(message.body)

            # Verify webhook signature (if provided)
            if not await self._verify_webhook_signature(message):
                raise ValueError("Invalid webhook signature")

            # Process based on webhook type
            event_type = webhook_data.get("type")

            if event_type == "payment.completed":
                return await self._handle_payment_completed(webhook_data)
            elif event_type == "user.updated":
                return await self._handle_user_updated(webhook_data)
            else:
                return await self._handle_unknown_webhook(webhook_data)

        except Exception as e:
            self.logger.error(f"Webhook processing failed: {e}")
            raise

    async def _verify_webhook_signature(self, message: Message) -> bool:
        """Verify webhook signature"""
        signature = message.headers.get("X-Webhook-Signature")
        if not signature:
            return True  # Skip verification if no signature

        # Calculate expected signature
        expected = hmac.new(
            self.webhook_secret.encode(),
            message.body.encode(),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected)

    async def _handle_payment_completed(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle payment completion webhook"""
        payment_id = data["data"]["payment_id"]
        amount = data["data"]["amount"]

        self.logger.info(f"Payment completed: {payment_id} for ${amount}")

        # Send confirmation webhook
        confirmation_sent = await self.webhook_client.send(
            "/payment-confirmation",
            {
                "payment_id": payment_id,
                "status": "processed",
                "processed_at": data.get("created_at")
            }
        )

        return {
            "status": "processed",
            "event_type": "payment_completed",
            "payment_id": payment_id,
            "confirmation_sent": confirmation_sent
        }

    async def _handle_user_updated(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle user update webhook"""
        user_id = data["data"]["user_id"]

        # Process user update
        await self._sync_user_data(user_id, data["data"])

        return {
            "status": "processed",
            "event_type": "user_updated",
            "user_id": user_id
        }

    async def _handle_unknown_webhook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle unknown webhook types"""
        self.logger.warning(f"Unknown webhook type: {data.get('type')}")

        return {
            "status": "logged",
            "event_type": "unknown",
            "webhook_type": data.get("type")
        }

    async def _sync_user_data(self, user_id: str, user_data: Dict[str, Any]):
        """Sync user data with local database"""
        # Simulate database update
        await asyncio.sleep(0.1)
        self.logger.info(f"Synced user data for {user_id}")

# Usage example
async def main():
    config = WorkerConfig(
        worker_name="webhook-processor",
        broker_type="kafka",  # Webhooks received via Kafka
        log_level="INFO"
    )

    worker = WebhookProcessor(config)
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
```

## Batch Processing Worker

Processing messages in batches for efficiency:

```python
import asyncio
import json
from typing import List, Dict, Any
from pythia.core import Worker, Message
from pythia.config import WorkerConfig

class BatchProcessor(Worker):
    """Process messages in batches for better performance"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.batch_size = 50
        self.batch_timeout = 30  # seconds
        self.message_buffer = []

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Add message to batch buffer"""
        self.message_buffer.append(message)

        # Process batch when full or timeout reached
        if len(self.message_buffer) >= self.batch_size:
            return await self._process_batch()

        return {"status": "buffered", "buffer_size": len(self.message_buffer)}

    async def _process_batch(self) -> Dict[str, Any]:
        """Process accumulated batch of messages"""
        if not self.message_buffer:
            return {"status": "no_messages", "processed": 0}

        batch = self.message_buffer.copy()
        self.message_buffer.clear()

        self.logger.info(f"Processing batch of {len(batch)} messages")

        try:
            # Extract all data
            batch_data = []
            for message in batch:
                data = json.loads(message.body)
                batch_data.append(data)

            # Process batch efficiently (e.g., bulk database operation)
            results = await self._bulk_process(batch_data)

            return {
                "status": "processed",
                "batch_size": len(batch),
                "processed": len(results),
                "results": results
            }

        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            # Put messages back in buffer for retry
            self.message_buffer.extend(batch)
            raise

    async def _bulk_process(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Bulk process the batch data"""
        # Simulate bulk database operation
        await asyncio.sleep(0.5)  # Simulate processing time

        results = []
        for item in data:
            results.append({
                "id": item.get("id"),
                "status": "processed",
                "processed_at": asyncio.get_event_loop().time()
            })

        return results

    async def on_shutdown(self):
        """Process remaining messages on shutdown"""
        if self.message_buffer:
            self.logger.info("Processing remaining messages on shutdown")
            await self._process_batch()

        await super().on_shutdown()

# Usage
async def main():
    config = WorkerConfig(
        worker_name="batch-processor",
        broker_type="redis",
        max_concurrent=1,  # Single-threaded for batch processing
        log_level="INFO"
    )

    worker = BatchProcessor(config)
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
```

## Testing Workers

Basic testing patterns for workers:

```python
import pytest
import json
from pythia.utils.testing import WorkerTestCase
from pythia.core import Message

class TestEventProcessor(WorkerTestCase):
    worker_class = EventProcessor

    async def test_user_registration_event(self):
        """Test user registration event processing"""
        event_data = {
            "type": "user_registered",
            "data": {
                "user_id": "123",
                "email": "test@example.com"
            }
        }

        message = self.create_test_message(json.dumps(event_data))
        result = await self.worker.process_message(message)

        assert result["status"] == "processed"
        assert result["action"] == "welcome_email_sent"
        assert result["user_id"] == "123"

    async def test_unknown_event_type(self):
        """Test handling of unknown event types"""
        event_data = {
            "type": "unknown_event",
            "data": {}
        }

        message = self.create_test_message(json.dumps(event_data))
        result = await self.worker.process_message(message)

        assert result["status"] == "processed"
        assert result["action"] == "logged"
        assert result["event_type"] == "unknown_event"

# Run tests
if __name__ == "__main__":
    pytest.main([__file__])
```

## Next Steps

- [Redis Worker](redis-worker.md) - Redis-specific worker examples
- [Kafka Worker](kafka-worker.md) - Kafka-specific worker examples
- [RabbitMQ Worker](rabbitmq-worker.md) - RabbitMQ-specific worker examples
- [User Guide](../user-guide/worker-lifecycle.md) - Understanding worker lifecycle
