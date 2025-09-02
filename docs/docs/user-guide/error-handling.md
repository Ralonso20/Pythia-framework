# Error Handling

Comprehensive guide to robust error handling patterns in Pythia workers.

## Overview

Effective error handling is crucial for building reliable workers. Pythia provides multiple strategies for handling different types of errors, from transient network issues to permanent data corruption.

## Error Classification

### Error Types

```python
from pythia.exceptions import (
    RecoverableError,      # Temporary errors that should be retried
    PermanentError,        # Errors that shouldn't be retried
    ValidationError,       # Data validation failures
    ConnectionError,       # Broker connection issues
    TimeoutError,         # Processing timeout errors
    CircuitBreakerError   # Circuit breaker triggered
)

class EmailProcessor(Worker):
    async def process_message(self, message: Message) -> Any:
        try:
            return await self._send_email(message)

        except aiohttp.ClientConnectionError as e:
            # Network issue - should retry
            raise RecoverableError(f"Network error: {e}")

        except InvalidEmailAddressError as e:
            # Bad data - don't retry
            raise PermanentError(f"Invalid email: {e}")

        except EmailServiceQuotaExceeded as e:
            # Rate limit - retry later
            raise RecoverableError(f"Rate limited: {e}")

        except Exception as e:
            # Unknown error - be conservative and retry
            self.logger.error(f"Unknown error: {e}")
            raise RecoverableError(f"Unexpected error: {e}")
```

## Retry Mechanisms

### Basic Retry Logic

```python
from pythia.config import ResilienceConfig

class RetryableWorker(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Configure retry behavior
        self.resilience_config = ResilienceConfig(
            max_retries=5,                    # Maximum retry attempts
            retry_delay=1.0,                  # Initial delay
            retry_backoff=2.0,                # Exponential backoff
            retry_max_delay=60.0,             # Maximum delay cap

            # Jitter to prevent thundering herd
            retry_jitter=True,
            retry_jitter_max=0.1              # ±10% jitter
        )

    async def process_message(self, message: Message) -> Any:
        """Process with automatic retry handling"""
        for attempt in range(self.resilience_config.max_retries + 1):
            try:
                return await self._attempt_processing(message, attempt)

            except RecoverableError as e:
                if attempt >= self.resilience_config.max_retries:
                    # Final attempt failed
                    self.logger.error(f"All retry attempts failed: {e}")
                    await self._handle_final_failure(message, e)
                    raise PermanentError(f"Max retries exceeded: {e}")

                # Calculate delay for next attempt
                delay = self._calculate_retry_delay(attempt)
                self.logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {e}")

                await asyncio.sleep(delay)

            except PermanentError:
                # Don't retry permanent errors
                raise

    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and jitter"""
        base_delay = self.resilience_config.retry_delay
        backoff = self.resilience_config.retry_backoff ** attempt
        delay = min(base_delay * backoff, self.resilience_config.retry_max_delay)

        # Add jitter to prevent thundering herd
        if self.resilience_config.retry_jitter:
            jitter_range = delay * self.resilience_config.retry_jitter_max
            jitter = random.uniform(-jitter_range, jitter_range)
            delay += jitter

        return max(0.1, delay)  # Minimum delay
```

### Advanced Retry Patterns

```python
from datetime import datetime, timedelta
from typing import Dict, Type, Callable

class AdvancedRetryWorker(Worker):
    """Worker with sophisticated retry patterns"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Define retry strategies per error type
        self.retry_strategies = {
            ConnectionError: self._connection_retry_strategy,
            TimeoutError: self._timeout_retry_strategy,
            RateLimitError: self._rate_limit_retry_strategy,
            ValidationError: self._validation_retry_strategy
        }

    async def process_message(self, message: Message) -> Any:
        """Process with error-specific retry strategies"""
        last_error = None

        for attempt in range(self.resilience_config.max_retries + 1):
            try:
                return await self._attempt_processing(message, attempt)

            except Exception as e:
                last_error = e
                error_type = type(e)

                # Get retry strategy for this error type
                strategy = self.retry_strategies.get(error_type, self._default_retry_strategy)

                # Check if we should retry
                should_retry, delay = await strategy(e, attempt, message)

                if not should_retry or attempt >= self.resilience_config.max_retries:
                    break

                self.logger.warning(
                    f"Attempt {attempt + 1} failed with {error_type.__name__}, "
                    f"retrying in {delay}s: {e}"
                )

                await asyncio.sleep(delay)

        # All retries exhausted
        await self._handle_final_failure(message, last_error)
        raise PermanentError(f"All retry attempts failed: {last_error}")

    async def _connection_retry_strategy(
        self, error: Exception, attempt: int, message: Message
    ) -> tuple[bool, float]:
        """Strategy for connection errors"""
        # Retry connection errors with exponential backoff
        delay = min(2.0 ** attempt, 30.0)  # Cap at 30 seconds
        return True, delay

    async def _timeout_retry_strategy(
        self, error: Exception, attempt: int, message: Message
    ) -> tuple[bool, float]:
        """Strategy for timeout errors"""
        # Increase timeout on subsequent attempts
        if attempt < 3:
            # Quick retries for first few attempts
            delay = 1.0 * (attempt + 1)
            return True, delay
        else:
            # Longer delays for later attempts
            delay = 10.0 * (attempt - 2)
            return True, min(delay, 60.0)

    async def _rate_limit_retry_strategy(
        self, error: Exception, attempt: int, message: Message
    ) -> tuple[bool, float]:
        """Strategy for rate limit errors"""
        # Use longer delays for rate limits
        base_delay = 30.0  # 30 seconds
        delay = base_delay * (attempt + 1)

        # Check rate limit headers if available
        if hasattr(error, 'retry_after'):
            delay = max(delay, error.retry_after)

        return True, min(delay, 300.0)  # Cap at 5 minutes

    async def _validation_retry_strategy(
        self, error: Exception, attempt: int, message: Message
    ) -> tuple[bool, float]:
        """Strategy for validation errors"""
        # Don't retry validation errors
        return False, 0.0

    async def _default_retry_strategy(
        self, error: Exception, attempt: int, message: Message
    ) -> tuple[bool, float]:
        """Default retry strategy for unknown errors"""
        # Conservative exponential backoff
        delay = min(1.0 * (2 ** attempt), 60.0)
        return True, delay
```

## Circuit Breaker Pattern

```python
import asyncio
from enum import Enum
from datetime import datetime, timedelta

class CircuitBreakerState(Enum):
    CLOSED = "closed"        # Normal operation
    OPEN = "open"           # Failing, reject requests
    HALF_OPEN = "half_open" # Testing if service recovered

class CircuitBreaker:
    """Circuit breaker implementation"""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: Type[Exception] = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED

    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
            else:
                raise CircuitBreakerError("Circuit breaker is OPEN")

        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result

        except self.expected_exception as e:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        return (
            self.last_failure_time and
            datetime.now() - self.last_failure_time > timedelta(seconds=self.recovery_timeout)
        )

    def _on_success(self):
        """Handle successful execution"""
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED

    def _on_failure(self):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN

class CircuitBreakerWorker(Worker):
    """Worker with circuit breaker protection"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Create circuit breakers for different services
        self.database_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
            expected_exception=DatabaseError
        )

        self.api_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30,
            expected_exception=APIError
        )

    async def process_message(self, message: Message) -> Any:
        """Process with circuit breaker protection"""
        try:
            # Database operation with circuit breaker
            data = await self.database_breaker.call(
                self._fetch_from_database, message.body
            )

            # API call with circuit breaker
            result = await self.api_breaker.call(
                self._call_external_api, data
            )

            return result

        except CircuitBreakerError as e:
            self.logger.warning(f"Circuit breaker triggered: {e}")
            # Fallback to cached data or default response
            return await self._fallback_processing(message)
```

## Dead Letter Queue (DLQ) Handling

```python
class DLQHandler:
    """Dead Letter Queue handler"""

    def __init__(self, worker_config: WorkerConfig):
        self.config = worker_config
        self.dlq_producer = None

    async def send_to_dlq(
        self,
        original_message: Message,
        final_error: Exception,
        retry_history: List[Dict[str, Any]] = None
    ):
        """Send message to dead letter queue"""
        dlq_message = {
            "original_message": {
                "id": original_message.message_id,
                "body": original_message.body,
                "headers": original_message.headers,
                "timestamp": original_message.timestamp.isoformat()
            },
            "error_details": {
                "error_type": type(final_error).__name__,
                "error_message": str(final_error),
                "error_traceback": traceback.format_exc()
            },
            "processing_details": {
                "worker_id": self.config.worker_id,
                "worker_name": self.config.worker_name,
                "failed_at": datetime.utcnow().isoformat(),
                "retry_count": original_message.retry_count,
                "retry_history": retry_history or []
            },
            "broker_metadata": {
                "topic": original_message.topic,
                "queue": original_message.queue,
                "partition": original_message.partition,
                "offset": original_message.offset
            }
        }

        # Send to appropriate DLQ based on broker type
        if self.config.broker_type == "kafka":
            await self._send_to_kafka_dlq(dlq_message)
        elif self.config.broker_type == "rabbitmq":
            await self._send_to_rabbitmq_dlq(dlq_message)
        else:
            await self._send_to_generic_dlq(dlq_message)

    async def _send_to_kafka_dlq(self, dlq_message: Dict[str, Any]):
        """Send to Kafka DLQ topic"""
        dlq_topic = f"{dlq_message['broker_metadata']['topic']}-dlq"

        # Use Kafka producer to send to DLQ topic
        await self.dlq_producer.send(
            topic=dlq_topic,
            value=json.dumps(dlq_message),
            key=dlq_message["original_message"]["id"]
        )

    async def _send_to_rabbitmq_dlq(self, dlq_message: Dict[str, Any]):
        """Send to RabbitMQ DLQ exchange"""
        dlq_exchange = "dlq-exchange"
        dlq_routing_key = f"dlq.{dlq_message['broker_metadata']['queue']}"

        # Use RabbitMQ producer to send to DLQ
        await self.dlq_producer.publish(
            exchange=dlq_exchange,
            routing_key=dlq_routing_key,
            message=json.dumps(dlq_message)
        )

class DLQAwareWorker(Worker):
    """Worker with DLQ support"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.dlq_handler = DLQHandler(config)
        self.retry_history = []

    async def _handle_final_failure(self, message: Message, error: Exception):
        """Handle message that failed all retry attempts"""
        # Add final error to retry history
        self.retry_history.append({
            "attempt": message.retry_count + 1,
            "error": str(error),
            "timestamp": datetime.utcnow().isoformat()
        })

        # Send to DLQ
        await self.dlq_handler.send_to_dlq(
            original_message=message,
            final_error=error,
            retry_history=self.retry_history
        )

        # Update metrics
        self.metrics.counter("messages_sent_to_dlq_total").inc()

        # Log for monitoring
        self.logger.error(
            f"Message sent to DLQ after {message.retry_count} retries",
            extra={
                "message_id": message.message_id,
                "error": str(error),
                "retry_count": message.retry_count
            }
        )
```

## Error Monitoring & Alerting

```python
class MonitoredErrorHandler:
    """Error handler with monitoring and alerting"""

    def __init__(self, worker: Worker):
        self.worker = worker
        self.error_counts = defaultdict(int)
        self.error_rate_threshold = 0.1  # 10% error rate threshold
        self.alert_cooldown = 300  # 5 minutes
        self.last_alert_time = {}

    async def handle_error(
        self,
        error: Exception,
        message: Message,
        context: Dict[str, Any] = None
    ):
        """Handle error with monitoring"""
        error_type = type(error).__name__

        # Update error metrics
        self.worker.metrics.counter(f"errors_total").inc()
        self.worker.metrics.counter(f"errors_by_type").labels(error_type=error_type).inc()

        # Track error rate
        self.error_counts[error_type] += 1

        # Check if error rate is too high
        await self._check_error_rate_threshold(error_type)

        # Log structured error
        self.worker.logger.error(
            f"Processing error: {error}",
            extra={
                "error_type": error_type,
                "message_id": message.message_id,
                "worker_id": self.worker.config.worker_id,
                "context": context or {},
                "traceback": traceback.format_exc()
            }
        )

        # Send to error tracking service (Sentry, Rollbar, etc.)
        await self._send_to_error_tracking(error, message, context)

    async def _check_error_rate_threshold(self, error_type: str):
        """Check if error rate exceeds threshold"""
        total_processed = self.worker.metrics.counter("messages_processed_total")._value
        error_count = self.error_counts[error_type]

        if total_processed > 100:  # Only check after processing some messages
            error_rate = error_count / total_processed

            if error_rate > self.error_rate_threshold:
                await self._send_high_error_rate_alert(error_type, error_rate)

    async def _send_high_error_rate_alert(self, error_type: str, error_rate: float):
        """Send alert for high error rate"""
        now = datetime.utcnow()
        last_alert = self.last_alert_time.get(error_type)

        # Check cooldown
        if last_alert and (now - last_alert).seconds < self.alert_cooldown:
            return

        alert_message = (
            f"High error rate detected for {error_type}: {error_rate:.2%}\n"
            f"Worker: {self.worker.config.worker_name}\n"
            f"Worker ID: {self.worker.config.worker_id}"
        )

        # Send alert (implementation depends on your alerting system)
        await self._send_alert(alert_message, severity="high")

        self.last_alert_time[error_type] = now

    async def _send_to_error_tracking(
        self,
        error: Exception,
        message: Message,
        context: Dict[str, Any] = None
    ):
        """Send error to tracking service"""
        # Example with Sentry
        try:
            import sentry_sdk

            with sentry_sdk.configure_scope() as scope:
                scope.set_tag("worker_name", self.worker.config.worker_name)
                scope.set_tag("worker_id", self.worker.config.worker_id)
                scope.set_tag("message_id", message.message_id)

                if context:
                    for key, value in context.items():
                        scope.set_extra(key, value)

                sentry_sdk.capture_exception(error)

        except ImportError:
            # Sentry not available, skip
            pass
```

## Graceful Degradation

```python
class GracefulWorker(Worker):
    """Worker that degrades gracefully when services fail"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.degraded_mode = False
        self.service_health = {
            "database": True,
            "cache": True,
            "external_api": True
        }

    async def process_message(self, message: Message) -> Any:
        """Process with graceful degradation"""
        if self.degraded_mode:
            return await self._process_degraded_mode(message)

        try:
            return await self._process_normal_mode(message)

        except ServiceUnavailableError as e:
            service = e.service_name
            self.service_health[service] = False

            # Check if we should enter degraded mode
            if self._should_enter_degraded_mode():
                self.degraded_mode = True
                self.logger.warning(f"Entering degraded mode due to {service} failure")

            return await self._process_degraded_mode(message)

    def _should_enter_degraded_mode(self) -> bool:
        """Determine if we should enter degraded mode"""
        # Enter degraded mode if critical services are down
        critical_services = ["database"]
        return not all(
            self.service_health.get(service, False)
            for service in critical_services
        )

    async def _process_normal_mode(self, message: Message) -> Any:
        """Full-featured processing"""
        # Fetch from database
        data = await self._fetch_from_database(message.body)

        # Get from cache
        cached_data = await self._get_from_cache(data.id)

        # Call external API
        api_result = await self._call_external_api(data)

        return {
            "status": "success",
            "mode": "normal",
            "result": api_result
        }

    async def _process_degraded_mode(self, message: Message) -> Any:
        """Degraded processing with fallbacks"""
        self.logger.info("Processing in degraded mode")

        try:
            # Try to use cached data
            if self.service_health.get("cache", False):
                cached_result = await self._get_cached_fallback(message.body)
                if cached_result:
                    return {
                        "status": "success",
                        "mode": "degraded_cache",
                        "result": cached_result
                    }

            # Use default/static response
            return {
                "status": "success",
                "mode": "degraded_default",
                "result": self._get_default_response(message.body)
            }

        except Exception as e:
            # Last resort: queue for later processing
            await self._queue_for_later(message)
            return {
                "status": "queued",
                "mode": "degraded_queued",
                "message": "Queued for later processing"
            }

    async def _periodic_health_check(self):
        """Periodically check service health"""
        while True:
            for service in self.service_health:
                try:
                    if service == "database":
                        await self._check_database_health()
                    elif service == "cache":
                        await self._check_cache_health()
                    elif service == "external_api":
                        await self._check_api_health()

                    self.service_health[service] = True

                except Exception:
                    self.service_health[service] = False

            # Exit degraded mode if all services are healthy
            if self.degraded_mode and all(self.service_health.values()):
                self.degraded_mode = False
                self.logger.info("Exiting degraded mode - all services healthy")

            await asyncio.sleep(30)  # Check every 30 seconds
```

## Testing Error Handling

```python
import pytest
from unittest.mock import AsyncMock, patch

class TestErrorHandling:
    """Test suite for error handling"""

    @pytest.fixture
    def worker(self):
        config = WorkerConfig(max_retries=3)
        return EmailProcessor(config)

    async def test_recoverable_error_retry(self, worker):
        """Test that recoverable errors are retried"""
        message = Message(body="test@example.com")

        # Mock the email service to fail twice then succeed
        with patch.object(worker, '_send_email') as mock_send:
            mock_send.side_effect = [
                RecoverableError("Network error"),
                RecoverableError("Network error"),
                {"status": "sent"}
            ]

            result = await worker.process_message(message)

            assert result["status"] == "sent"
            assert mock_send.call_count == 3

    async def test_permanent_error_no_retry(self, worker):
        """Test that permanent errors are not retried"""
        message = Message(body="invalid-email")

        with patch.object(worker, '_send_email') as mock_send:
            mock_send.side_effect = PermanentError("Invalid email")

            with pytest.raises(PermanentError):
                await worker.process_message(message)

            assert mock_send.call_count == 1

    async def test_max_retries_exceeded(self, worker):
        """Test behavior when max retries exceeded"""
        message = Message(body="test@example.com")

        with patch.object(worker, '_send_email') as mock_send:
            mock_send.side_effect = RecoverableError("Persistent failure")

            with pytest.raises(PermanentError) as exc_info:
                await worker.process_message(message)

            assert "Max retries exceeded" in str(exc_info.value)
            assert mock_send.call_count == 4  # Initial + 3 retries

    async def test_circuit_breaker_opens(self):
        """Test circuit breaker opens after threshold failures"""
        breaker = CircuitBreaker(failure_threshold=3)

        async def failing_function():
            raise Exception("Service unavailable")

        # Trigger failures to open circuit breaker
        for _ in range(3):
            with pytest.raises(Exception):
                await breaker.call(failing_function)

        # Circuit breaker should now be open
        with pytest.raises(CircuitBreakerError):
            await breaker.call(failing_function)
```

## Best Practices

### 1. Error Classification

- **Be specific**: Use custom exception types for different error categories
- **Document behavior**: Clearly document which errors are recoverable
- **Fail fast**: Don't retry validation or configuration errors

### 2. Retry Strategy

- **Exponential backoff**: Prevent overwhelming failed services
- **Jitter**: Add randomness to prevent thundering herd
- **Max delay**: Cap retry delays to reasonable values
- **Circuit breakers**: Protect downstream services

### 3. Monitoring

- **Error rates**: Track error rates over time
- **Error types**: Monitor specific error patterns
- **DLQ metrics**: Track messages sent to dead letter queues
- **Recovery time**: Measure how quickly services recover

### 4. Alerting

- **Threshold-based**: Alert on error rate thresholds
- **Cooldown periods**: Prevent alert spam
- **Context**: Include relevant context in alerts
- **Escalation**: Define escalation paths for critical errors

## Next Steps

- [Performance Optimization](../performance/optimization.md) - Performance tuning techniques
- [Monitoring Setup](../performance/benchmarks.md) - Production monitoring
- [API Reference](../api/core.md) - Complete API documentation
