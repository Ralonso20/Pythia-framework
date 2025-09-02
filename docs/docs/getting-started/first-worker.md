# Your First Worker

Step-by-step tutorial to build a complete worker with error handling, logging, and monitoring.

## Project Setup

Create a new directory for your worker:

```bash
mkdir my-pythia-worker
cd my-pythia-worker
```

Install Pythia:

```bash
pip install pythia-framework[redis]  # or [kafka] or [rabbitmq]
```

## Step 1: Basic Worker Structure

Create `worker.py`:

```python
import asyncio
import json
from datetime import datetime
from typing import Any, Dict

from pythia.core import Worker, Message
from pythia.config import WorkerConfig
from pythia.logging import get_pythia_logger


class EmailProcessorWorker(Worker):
    """
    Email processing worker that validates and processes email messages
    """

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.processed_count = 0

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process an email message"""
        self.logger.info(f"Processing message {message.message_id}")

        try:
            # Parse the message body
            email_data = json.loads(message.body)

            # Validate required fields
            if not self._validate_email_data(email_data):
                raise ValueError("Invalid email data structure")

            # Process the email
            result = await self._process_email(email_data)

            self.processed_count += 1
            self.logger.info(f"Successfully processed email to {email_data['to']}")

            return result

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse message body: {e}")
            raise

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            raise

    def _validate_email_data(self, data: Dict[str, Any]) -> bool:
        """Validate email data structure"""
        required_fields = ['to', 'subject', 'body']
        return all(field in data for field in required_fields)

    async def _process_email(self, email_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process the email (simulate sending)"""
        # Simulate email processing delay
        await asyncio.sleep(0.1)

        # Return processing result
        return {
            "status": "sent",
            "to": email_data["to"],
            "subject": email_data["subject"],
            "processed_at": datetime.now().isoformat(),
            "worker_id": self.config.worker_id,
            "message_count": self.processed_count
        }

    async def on_startup(self):
        """Called when worker starts"""
        self.logger.info("Email processor worker starting up")
        await super().on_startup()

    async def on_shutdown(self):
        """Called when worker shuts down"""
        self.logger.info(f"Email processor shutting down. Processed {self.processed_count} messages")
        await super().on_shutdown()
```

## Step 2: Configuration

Create `config.py`:

```python
from pythia.config import WorkerConfig
from pythia.config.redis import RedisConfig

def get_worker_config() -> WorkerConfig:
    """Get worker configuration"""
    return WorkerConfig(
        worker_name="email-processor",
        broker_type="redis",
        max_concurrent=5,
        max_retries=3,
        retry_delay=1.0,
        log_level="INFO",
        health_check_interval=30
    )

def get_redis_config() -> RedisConfig:
    """Get Redis broker configuration"""
    return RedisConfig(
        host="localhost",
        port=6379,
        db=0,
        queue_name="email-queue",
        connection_pool_size=10
    )
```

## Step 3: Main Application

Create `main.py`:

```python
import asyncio
import signal
import sys
from worker import EmailProcessorWorker
from config import get_worker_config

async def main():
    """Main application entry point"""
    # Create worker configuration
    config = get_worker_config()

    # Create and start worker
    worker = EmailProcessorWorker(config)

    # Setup graceful shutdown
    def signal_handler():
        print("Received shutdown signal...")
        asyncio.create_task(worker.stop())

    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda s, f: signal_handler())

    try:
        # Start the worker
        await worker.start()
    except KeyboardInterrupt:
        print("Received keyboard interrupt")
    except Exception as e:
        print(f"Worker failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
```

## Step 4: Testing

Create `test_worker.py`:

```python
import pytest
import json
from pythia.utils.testing import WorkerTestCase
from worker import EmailProcessorWorker
from config import get_worker_config

class TestEmailProcessorWorker(WorkerTestCase):
    worker_class = EmailProcessorWorker
    config = get_worker_config()

    async def test_valid_email_processing(self):
        """Test processing valid email message"""
        email_data = {
            "to": "user@example.com",
            "subject": "Test Email",
            "body": "This is a test email"
        }

        message = self.create_test_message(json.dumps(email_data))
        result = await self.worker.process_message(message)

        assert result["status"] == "sent"
        assert result["to"] == "user@example.com"
        assert result["subject"] == "Test Email"
        assert "processed_at" in result

    async def test_invalid_email_data(self):
        """Test handling invalid email data"""
        invalid_data = {"to": "user@example.com"}  # Missing required fields

        message = self.create_test_message(json.dumps(invalid_data))

        with pytest.raises(ValueError):
            await self.worker.process_message(message)

    async def test_malformed_json(self):
        """Test handling malformed JSON"""
        message = self.create_test_message("invalid json")

        with pytest.raises(json.JSONDecodeError):
            await self.worker.process_message(message)

# Run tests
if __name__ == "__main__":
    pytest.main([__file__])
```

## Step 5: Running Your Worker

1. **Start Redis** (if using Redis):
   ```bash
   redis-server
   ```

2. **Run the worker**:
   ```bash
   python main.py
   ```

3. **Send test messages** (in another terminal):
   ```bash
   redis-cli LPUSH email-queue '{"to": "test@example.com", "subject": "Hello", "body": "Test message"}'
   ```

4. **Run tests**:
   ```bash
   python test_worker.py
   ```

## Step 6: Production Deployment

Create `docker-compose.yml`:

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  email-worker:
    build: .
    depends_on:
      - redis
    environment:
      - PYTHIA_BROKER_TYPE=redis
      - PYTHIA_LOG_LEVEL=INFO
      - REDIS_HOST=redis
    restart: unless-stopped
    deploy:
      replicas: 3
```

Create `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

## What You've Built

✅ **Complete worker** with error handling and logging
✅ **Configuration management** with environment support
✅ **Testing suite** with comprehensive test cases
✅ **Production deployment** with Docker and scaling
✅ **Graceful shutdown** handling
✅ **Performance monitoring** built-in

## Next Steps

- [Worker Lifecycle](../user-guide/worker-lifecycle.md) - Deep dive into worker internals
- [Configuration Guide](../user-guide/configuration.md) - Advanced configuration options
- [Error Handling](../user-guide/error-handling.md) - Robust error handling patterns
- [Monitoring](../performance/optimization.md) - Production monitoring setup
