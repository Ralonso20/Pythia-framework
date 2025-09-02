# Message Handling

Complete guide to processing messages in Pythia workers, including message structure, acknowledgment patterns, and processing strategies.

## Message Structure

Pythia provides a unified message abstraction that works across all brokers:

```python
from pythia.core import Message
from datetime import datetime
from typing import Dict, Any

# Universal message structure
@dataclass
class Message:
    # Core data
    body: Union[str, bytes, dict, Any]    # Message payload
    message_id: str                       # Unique identifier
    timestamp: datetime                   # Message timestamp
    headers: Dict[str, Any]              # Message headers

    # Broker-specific metadata
    topic: Optional[str] = None          # Kafka topic
    queue: Optional[str] = None          # Queue name
    routing_key: Optional[str] = None    # RabbitMQ routing key
    partition: Optional[int] = None      # Kafka partition
    offset: Optional[int] = None         # Kafka offset
    exchange: Optional[str] = None       # RabbitMQ exchange

    # Redis-specific fields
    stream_id: Optional[str] = None      # Redis stream ID
    channel: Optional[str] = None        # Redis channel

    # RabbitMQ-specific fields
    delivery_tag: Optional[int] = None   # RabbitMQ delivery tag

    # Processing metadata
    retry_count: int = 0                 # Current retry attempt
    max_retries: int = 3                # Maximum retries allowed
```

## Basic Message Processing

### Simple Message Handler

```python
import json
from pythia.core import Worker, Message

class BasicMessageProcessor(Worker):
    async def process_message(self, message: Message) -> Any:
        """Process a single message"""
        try:
            # Parse message body if it's JSON
            if isinstance(message.body, str):
                data = json.loads(message.body)
            else:
                data = message.body

            self.logger.info(
                f"Processing message {message.message_id}",
                extra={
                    "message_id": message.message_id,
                    "timestamp": message.timestamp,
                    "data_type": type(data).__name__
                }
            )

            # Your business logic here
            result = await self._process_data(data)

            return {
                "status": "success",
                "message_id": message.message_id,
                "result": result,
                "processed_at": datetime.utcnow().isoformat()
            }

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in message: {e}")
            raise ValueError(f"Invalid message format: {e}")

        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            raise

    async def _process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Business logic implementation"""
        # Implement your specific processing logic
        return {"processed": True, "data": data}
```

## Message Acknowledgment Patterns

### Manual Acknowledgment (Recommended)

```python
class ReliableMessageProcessor(Worker):
    async def process_message(self, message: Message) -> Any:
        """Process message with manual acknowledgment"""
        try:
            # Process the message
            result = await self._process_business_logic(message)

            # Acknowledge only after successful processing
            await self._acknowledge_message(message)

            return result

        except RecoverableError as e:
            # Temporary error - reject and requeue
            self.logger.warning(f"Recoverable error, rejecting: {e}")
            await self._reject_message(message, requeue=True)
            raise

        except PermanentError as e:
            # Permanent error - reject without requeue
            self.logger.error(f"Permanent error, discarding: {e}")
            await self._reject_message(message, requeue=False)
            raise

    async def _acknowledge_message(self, message: Message):
        """Acknowledge message based on broker type"""
        if hasattr(message, 'ack') and callable(message.ack):
            await message.ack()
        else:
            # Framework handles acknowledgment automatically
            pass

    async def _reject_message(self, message: Message, requeue: bool = True):
        """Reject message based on broker type"""
        if hasattr(message, 'nack') and callable(message.nack):
            await message.nack(requeue=requeue)
        else:
            # Framework handles rejection automatically
            if not requeue:
                # Move to dead letter queue if available
                await self._send_to_dlq(message)
```

### Auto Acknowledgment

```python
class FastMessageProcessor(Worker):
    """For high-throughput, low-reliability scenarios"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        # Enable auto-acknowledgment in broker config
        # This acknowledges messages immediately upon receipt

    async def process_message(self, message: Message) -> Any:
        """Process with auto-acknowledgment"""
        # Message is already acknowledged
        # If processing fails, message is lost
        result = await self._fast_processing(message)
        return result
```

## Message Processing Patterns

### 1. Single Message Processing

```python
class SingleMessageProcessor(Worker):
    """Process one message at a time"""

    async def process_message(self, message: Message) -> Any:
        """Process individual message"""
        start_time = time.time()

        try:
            # Extract and validate data
            data = await self._extract_data(message)
            await self._validate_data(data)

            # Process the data
            result = await self._business_logic(data)

            # Record metrics
            processing_time = time.time() - start_time
            self.metrics.histogram("message_processing_seconds").observe(processing_time)

            return result

        except ValidationError as e:
            self.logger.error(f"Validation failed: {e}")
            # Don't retry validation errors
            raise PermanentError(str(e))

    async def _extract_data(self, message: Message) -> Dict[str, Any]:
        """Extract data from message"""
        if isinstance(message.body, dict):
            return message.body
        elif isinstance(message.body, str):
            return json.loads(message.body)
        else:
            raise ValueError(f"Unsupported message body type: {type(message.body)}")

    async def _validate_data(self, data: Dict[str, Any]):
        """Validate message data"""
        required_fields = ["id", "action", "data"]
        missing_fields = [field for field in required_fields if field not in data]

        if missing_fields:
            raise ValidationError(f"Missing required fields: {missing_fields}")
```

### 2. Batch Processing

```python
from typing import List

class BatchMessageProcessor(Worker):
    """Process messages in batches for efficiency"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.batch_size = config.batch_size or 10
        self.batch_timeout = 30  # seconds

    async def process_messages_batch(self, messages: List[Message]) -> List[Any]:
        """Process a batch of messages"""
        self.logger.info(f"Processing batch of {len(messages)} messages")

        try:
            # Extract all data
            batch_data = [await self._extract_data(msg) for msg in messages]

            # Process as batch (more efficient for database operations)
            results = await self._process_batch(batch_data)

            # Acknowledge all messages on success
            for message in messages:
                await self._acknowledge_message(message)

            return results

        except Exception as e:
            # Handle batch failure
            await self._handle_batch_error(messages, e)
            raise

    async def _process_batch(self, batch_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process entire batch efficiently"""
        # Example: Bulk database insert
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                results = []
                for data in batch_data:
                    result = await conn.fetchrow(
                        "INSERT INTO processed_data (data) VALUES ($1) RETURNING id",
                        json.dumps(data)
                    )
                    results.append({"id": result["id"], "status": "processed"})
                return results

    async def _handle_batch_error(self, messages: List[Message], error: Exception):
        """Handle batch processing error"""
        self.logger.error(f"Batch processing failed: {error}")

        # Try processing messages individually
        for message in messages:
            try:
                await self.process_message(message)
            except Exception:
                await self._reject_message(message, requeue=True)
```

### 3. Streaming Processing

```python
import asyncio
from asyncio import Queue

class StreamingProcessor(Worker):
    """Process messages in a continuous stream"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.message_queue = Queue(maxsize=1000)
        self.processing_task = None

    async def on_startup(self):
        """Start streaming processor"""
        await super().on_startup()
        self.processing_task = asyncio.create_task(self._stream_processor())

    async def on_shutdown(self):
        """Stop streaming processor"""
        if self.processing_task:
            self.processing_task.cancel()
        await super().on_shutdown()

    async def process_message(self, message: Message) -> Any:
        """Add message to stream queue"""
        try:
            await self.message_queue.put(message)
            return {"status": "queued"}
        except asyncio.QueueFull:
            self.logger.warning("Message queue full, rejecting message")
            raise TemporaryError("Queue full, retry later")

    async def _stream_processor(self):
        """Continuous stream processor"""
        while True:
            try:
                # Get message with timeout
                message = await asyncio.wait_for(
                    self.message_queue.get(), timeout=1.0
                )

                # Process in stream
                await self._process_streaming_message(message)

            except asyncio.TimeoutError:
                # No message available, continue
                continue
            except Exception as e:
                self.logger.error(f"Stream processing error: {e}")

    async def _process_streaming_message(self, message: Message):
        """Process message in streaming context"""
        try:
            # Your streaming logic here
            result = await self._stream_business_logic(message)
            await self._acknowledge_message(message)

        except Exception as e:
            await self._reject_message(message, requeue=True)
            raise
```

## Message Routing & Filtering

### Content-Based Routing

```python
class RoutingProcessor(Worker):
    """Route messages based on content"""

    async def process_message(self, message: Message) -> Any:
        """Route message based on content"""
        data = json.loads(message.body)
        message_type = data.get("type")

        # Route based on message type
        if message_type == "user_registration":
            return await self._handle_user_registration(data)
        elif message_type == "order_created":
            return await self._handle_order_created(data)
        elif message_type == "payment_processed":
            return await self._handle_payment_processed(data)
        else:
            self.logger.warning(f"Unknown message type: {message_type}")
            raise ValueError(f"Unsupported message type: {message_type}")

    async def _handle_user_registration(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle user registration message"""
        user_id = data["user_id"]
        email = data["email"]

        # Send welcome email
        await self._send_welcome_email(email)

        # Update user profile
        await self._update_user_profile(user_id, data)

        return {"status": "user_registered", "user_id": user_id}
```

### Header-Based Filtering

```python
class FilteredProcessor(Worker):
    """Process messages based on headers"""

    async def process_message(self, message: Message) -> Any:
        """Process only messages matching criteria"""

        # Check priority header
        priority = message.headers.get("priority", "normal")
        if priority == "low":
            # Skip low priority messages during high load
            if await self._is_high_load():
                self.logger.info("Skipping low priority message during high load")
                await self._defer_message(message)
                return {"status": "deferred"}

        # Check region header
        region = message.headers.get("region")
        if region and region not in ["us-east", "us-west"]:
            self.logger.info(f"Skipping message from unsupported region: {region}")
            return {"status": "skipped", "reason": "unsupported_region"}

        # Process the message
        return await self._process_filtered_message(message)

    async def _is_high_load(self) -> bool:
        """Check if system is under high load"""
        # Check various metrics
        cpu_usage = await self._get_cpu_usage()
        queue_depth = await self._get_queue_depth()

        return cpu_usage > 80 or queue_depth > 1000
```

## Error Handling in Message Processing

### Retry Mechanisms

```python
class RetryableProcessor(Worker):
    """Processor with sophisticated retry logic"""

    async def process_message(self, message: Message) -> Any:
        """Process with retry logic"""
        try:
            return await self._attempt_processing(message)

        except RecoverableError as e:
            if message.retry_count < message.max_retries:
                await self._schedule_retry(message, e)
                return {"status": "retry_scheduled"}
            else:
                await self._handle_max_retries_exceeded(message, e)
                raise PermanentError(f"Max retries exceeded: {e}")

    async def _schedule_retry(self, message: Message, error: Exception):
        """Schedule message retry with backoff"""
        retry_delay = self._calculate_retry_delay(message.retry_count)

        self.logger.info(
            f"Scheduling retry {message.retry_count + 1} in {retry_delay}s: {error}"
        )

        # Update retry count
        message.retry_count += 1
        message.headers["retry_count"] = message.retry_count
        message.headers["last_error"] = str(error)
        message.headers["retry_scheduled_at"] = datetime.utcnow().isoformat()

        # Schedule for later processing
        await self._publish_delayed_message(message, retry_delay)

    def _calculate_retry_delay(self, retry_count: int) -> int:
        """Calculate exponential backoff delay"""
        base_delay = 2
        max_delay = 300  # 5 minutes
        delay = min(base_delay ** retry_count, max_delay)
        return delay
```

### Dead Letter Queue Handling

```python
class DLQProcessor(Worker):
    """Processor with dead letter queue handling"""

    async def _handle_max_retries_exceeded(self, message: Message, error: Exception):
        """Handle message that exceeded max retries"""
        dlq_message = {
            "original_message": message.to_dict(),
            "final_error": str(error),
            "retry_history": message.headers.get("retry_history", []),
            "failed_at": datetime.utcnow().isoformat(),
            "worker_id": self.config.worker_id
        }

        # Send to dead letter queue
        await self._send_to_dlq(dlq_message)

        # Alert monitoring
        self.metrics.counter("messages_sent_to_dlq_total").inc()

        # Log for investigation
        self.logger.error(
            f"Message sent to DLQ after {message.max_retries} retries: {error}",
            extra={
                "message_id": message.message_id,
                "final_error": str(error)
            }
        )

    async def _send_to_dlq(self, dlq_message: Dict[str, Any]):
        """Send message to dead letter queue"""
        # Implementation depends on broker type
        if self.config.broker_type == "rabbitmq":
            await self._send_to_rabbitmq_dlq(dlq_message)
        elif self.config.broker_type == "kafka":
            await self._send_to_kafka_dlq(dlq_message)
        else:
            # Generic DLQ (could be database, file, etc.)
            await self._send_to_generic_dlq(dlq_message)
```

## Performance Optimization

### Message Prefetching

```python
class OptimizedProcessor(Worker):
    """Processor optimized for high throughput"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        # Configure prefetch for better performance
        self.prefetch_count = config.prefetch_count or 100

    async def process_message(self, message: Message) -> Any:
        """Optimized message processing"""
        # Use connection pooling
        async with self.connection_pool.acquire() as conn:
            # Batch operations when possible
            result = await self._optimized_processing(message, conn)

        return result
```

### Parallel Processing

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

class ParallelProcessor(Worker):
    """Process messages in parallel"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def process_message(self, message: Message) -> Any:
        """Process message in parallel"""
        # CPU-bound work in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor,
            self._cpu_intensive_work,
            message.body
        )

        return result

    def _cpu_intensive_work(self, data) -> Dict[str, Any]:
        """CPU-intensive processing in thread"""
        # Heavy computation here
        import time
        time.sleep(0.1)  # Simulate work
        return {"processed": True, "data": data}
```

## Testing Message Handling

```python
import pytest
from pythia.utils.testing import WorkerTestCase

class TestMessageHandling(WorkerTestCase):
    async def test_successful_processing(self):
        """Test successful message processing"""
        test_data = {"id": "123", "action": "create", "data": {"name": "test"}}
        message = self.create_test_message(json.dumps(test_data))

        result = await self.worker.process_message(message)

        assert result["status"] == "success"
        assert result["message_id"] == message.message_id

    async def test_invalid_message_format(self):
        """Test handling of invalid message format"""
        invalid_message = self.create_test_message("invalid json")

        with pytest.raises(ValueError):
            await self.worker.process_message(invalid_message)

    async def test_retry_logic(self):
        """Test message retry logic"""
        # Create message that will fail
        message = self.create_test_message('{"should_fail": true}')

        # Mock failure on first attempt
        with pytest.raises(RecoverableError):
            await self.worker.process_message(message)

        # Verify retry was scheduled
        assert message.retry_count == 1
```

## Next Steps

- [Configuration](configuration.md) - Complete configuration guide
- [Error Handling](error-handling.md) - Advanced error handling patterns
- [Performance Optimization](../performance/optimization.md) - Performance tuning guide
