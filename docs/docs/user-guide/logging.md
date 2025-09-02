# Logging with Loguru

Pythia uses [Loguru](https://github.com/Delgan/loguru) as its default logging framework, providing structured, flexible, and high-performance logging out of the box.

## 🌟 Why Loguru?

- **🚀 Zero Configuration**: Works perfectly with sensible defaults
- **📊 Structured Logging**: JSON output with custom fields
- **🎨 Beautiful Console Output**: Colored, readable logs for development
- **⚡ High Performance**: Async-compatible with minimal overhead
- **🔧 Highly Configurable**: Custom formatters, filters, and handlers

## 🚀 Quick Start

### Basic Usage

Loguru is automatically configured when you create a Pythia worker:

```python
from pythia import Worker
from pythia.brokers.redis import RedisConsumer
from loguru import logger

class EmailWorker(Worker):
    source = RedisConsumer(queue_name="emails")

    async def process(self, message):
        logger.info("Processing email", email=message.body.get("email"))

        try:
            # Process email logic
            await self.send_email(message.body)
            logger.success("Email sent successfully",
                         email=message.body.get("email"))
        except Exception as e:
            logger.error("Failed to send email",
                        email=message.body.get("email"),
                        error=str(e))
            raise

        return {"status": "sent", "email": message.body.get("email")}
```

### Structured Logging

Use keyword arguments to add structured data to your logs:

```python
logger.info("User action",
           user_id=123,
           action="login",
           ip="192.168.1.1",
           timestamp=datetime.now().isoformat())
```

## ⚙️ Configuration

### Environment Variables

Configure logging behavior using environment variables:

```bash
# Log level (TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL)
PYTHIA_LOG_LEVEL=INFO

# Log format (console, json, custom)
PYTHIA_LOG_FORMAT=console

# Log file path (optional)
PYTHIA_LOG_FILE=/app/logs/worker.log

# Disable console logging
PYTHIA_LOG_CONSOLE=false

# Enable log sampling for high-volume scenarios
PYTHIA_LOG_SAMPLING_RATE=0.1  # Log 10% of messages
```

### Programmatic Configuration

```python
from pythia.logging import setup_logging
from loguru import logger

# Custom configuration
setup_logging(
    level="DEBUG",
    format_type="json",
    log_file="/app/logs/worker.log",
    console_enabled=True,
    sampling_rate=1.0  # Log everything
)

class MyWorker(Worker):
    async def process(self, message):
        logger.debug("Processing message",
                    message_id=message.message_id,
                    queue=message.metadata.get("queue"))
```

## 📝 Log Formats

### Console Format (Development)

Perfect for local development with colored, readable output:

```python
from pythia.logging import setup_logging

setup_logging(format_type="console", level="DEBUG")
```

Output:
```
2025-09-02 10:30:15.123 | INFO     | my_worker:process:15 - Processing email email=user@example.com
2025-09-02 10:30:15.456 | SUCCESS  | my_worker:process:22 - Email sent successfully email=user@example.com
```

### JSON Format (Production)

Structured JSON logs for production systems and log aggregation:

```python
setup_logging(format_type="json", level="INFO")
```

Output:
```json
{
  "timestamp": "2025-09-02T10:30:15.123456Z",
  "level": "INFO",
  "module": "my_worker",
  "function": "process",
  "line": 15,
  "message": "Processing email",
  "email": "user@example.com",
  "worker_id": "worker-123",
  "trace_id": "abc123def456"
}
```

### Custom Format

Create your own log format:

```python
from pythia.logging import setup_logging

custom_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level> | "
    "{extra}"
)

setup_logging(
    format_type="custom",
    custom_format=custom_format,
    level="INFO"
)
```

## 🎯 Log Levels and Filtering

### Available Log Levels

```python
from loguru import logger

# TRACE - Very detailed debugging information
logger.trace("Entering function", function="process_message")

# DEBUG - Detailed debugging information
logger.debug("Processing step", step=1, data={"key": "value"})

# INFO - General information about program execution
logger.info("Worker started", worker_type="EmailWorker")

# SUCCESS - Successful operation (Loguru-specific)
logger.success("Message processed successfully", message_id="123")

# WARNING - Something unexpected happened
logger.warning("Retry attempt", attempt=2, max_retries=3)

# ERROR - Serious problem occurred
logger.error("Processing failed", error=str(e), message_id="123")

# CRITICAL - Very serious error occurred
logger.critical("Database connection lost", database="primary")
```

### Filtering by Module

Filter logs by specific modules or components:

```python
from pythia.logging import setup_logging

# Only log from specific modules
setup_logging(
    level="DEBUG",
    filter_modules=["my_worker", "pythia.brokers.kafka"]
)

# Exclude specific modules
setup_logging(
    level="DEBUG",
    exclude_modules=["urllib3", "asyncio"]
)
```

## 📊 Performance Considerations

### Log Sampling

For high-throughput scenarios, use log sampling to reduce I/O:

```python
from pythia.logging import setup_logging

# Log only 10% of messages
setup_logging(
    level="INFO",
    sampling_rate=0.1,
    sampling_strategy="random"  # or "systematic"
)

class HighVolumeWorker(Worker):
    async def process(self, message):
        # This will only be logged 10% of the time
        logger.info("Processing message", message_id=message.message_id)
```

### Async Logging

Enable async logging for better performance:

```python
from pythia.logging import setup_async_logging

# Async logging with buffer
setup_async_logging(
    buffer_size=1000,
    flush_interval=5.0  # seconds
)
```

## 🔧 Custom Formatters

### Adding Context Information

Automatically add worker context to all logs:

```python
from pythia.logging.decorators import with_worker_context
from loguru import logger

class MyWorker(Worker):
    def __init__(self):
        super().__init__()
        # Add worker context to all logs
        logger.configure(
            extra={
                "worker_id": f"worker-{id(self)}",
                "worker_type": self.__class__.__name__,
                "broker_type": self.source.__class__.__name__
            }
        )

    @with_worker_context
    async def process(self, message):
        # All logs will automatically include worker context
        logger.info("Processing message", message_id=message.message_id)
```

### Message Correlation

Track messages across your entire pipeline:

```python
import uuid
from loguru import logger

class TrackableWorker(Worker):
    async def process(self, message):
        # Generate or extract correlation ID
        correlation_id = message.headers.get("correlation_id") or str(uuid.uuid4())

        with logger.contextualize(correlation_id=correlation_id):
            logger.info("Starting message processing")

            # All nested logs will include correlation_id
            await self.process_step_1(message)
            await self.process_step_2(message)

            logger.success("Message processing completed")

    async def process_step_1(self, message):
        # This log will automatically include correlation_id
        logger.debug("Executing step 1")

    async def process_step_2(self, message):
        # This log will automatically include correlation_id
        logger.debug("Executing step 2")
```

## 🛡️ Error Handling and Logging

### Exception Logging

Automatically log exceptions with full context:

```python
from pythia.logging.decorators import log_exceptions
from loguru import logger

class SafeWorker(Worker):
    @log_exceptions(level="ERROR", reraise=True)
    async def process(self, message):
        # Any exception will be automatically logged with context
        result = await self.risky_operation(message)
        return result

    async def risky_operation(self, message):
        try:
            # Risky code here
            return await external_api_call(message.body)
        except ValueError as e:
            logger.warning("Validation error",
                          error=str(e),
                          message_data=message.body)
            raise
        except Exception as e:
            logger.error("Unexpected error in risky_operation",
                        error=str(e),
                        error_type=type(e).__name__,
                        message_id=message.message_id)
            raise
```

## 🔍 Log Analysis and Monitoring

### Integration with Observability Stack

Configure Loguru to work with your observability tools:

```python
from pythia.logging import setup_logging
import json

# Configure for Grafana Loki
setup_logging(
    format_type="json",
    log_file="/app/logs/worker.jsonl",
    extra_fields={
        "service": "email-worker",
        "version": "1.0.0",
        "environment": "production"
    }
)

# Add custom log processor for OpenTelemetry
def add_trace_context(record):
    from opentelemetry import trace
    span = trace.get_current_span()
    if span:
        record["extra"]["trace_id"] = format(span.get_span_context().trace_id, "032x")
        record["extra"]["span_id"] = format(span.get_span_context().span_id, "016x")

logger.configure(
    processors=[add_trace_context]
)
```

### Health Check Logging

Log health check information for monitoring:

```python
from pythia.logging import health_logger

class MonitoredWorker(Worker):
    async def startup(self):
        health_logger.info("Worker starting up",
                          worker_type=self.__class__.__name__)

    async def process(self, message):
        # Regular processing...
        pass

    async def health_check(self):
        try:
            # Check database connection, etc.
            health_logger.success("Health check passed")
            return {"status": "healthy"}
        except Exception as e:
            health_logger.error("Health check failed", error=str(e))
            return {"status": "unhealthy", "error": str(e)}
```

## 📈 Best Practices

### 1. Use Structured Logging

Always prefer structured logging over string formatting:

```python
# ❌ Don't do this
logger.info(f"Processing order {order_id} for user {user_id}")

# ✅ Do this instead
logger.info("Processing order", order_id=order_id, user_id=user_id)
```

### 2. Log Meaningful Information

Include context that helps with debugging:

```python
logger.info("Message processed successfully",
           message_id=message.message_id,
           processing_time_ms=processing_time * 1000,
           queue_depth=queue.depth(),
           retry_count=message.retry_count)
```

### 3. Use Appropriate Log Levels

```python
# Use INFO for important business events
logger.info("Order created", order_id=order.id, customer_id=customer.id)

# Use DEBUG for detailed debugging information
logger.debug("Validation step", field="email", value="user@example.com", valid=True)

# Use SUCCESS for completed operations (Loguru-specific)
logger.success("Email sent", recipient="user@example.com", template="welcome")

# Use WARNING for recoverable issues
logger.warning("Retry required", attempt=2, max_attempts=3, reason="timeout")
```

### 4. Handle Sensitive Data

Never log sensitive information:

```python
# ❌ Never log sensitive data
logger.info("User login", password=password, credit_card=cc_number)

# ✅ Log safely
logger.info("User login",
           user_id=user.id,
           login_method="password",
           password_length=len(password))
```

## 🧪 Testing Logging

### Testing Log Output

Test that your workers log correctly:

```python
import pytest
from loguru import logger
from pythia.utils.testing import capture_logs

class TestWorkerLogging:
    def test_successful_processing_logs(self):
        worker = MyWorker()
        message = create_test_message({"email": "test@example.com"})

        with capture_logs() as logs:
            result = await worker.process(message)

        # Assert logs were generated
        assert len(logs) >= 2
        assert logs[0].level == "INFO"
        assert "Processing email" in logs[0].message
        assert logs[0].email == "test@example.com"

        assert logs[1].level == "SUCCESS"
        assert "Email sent successfully" in logs[1].message

    def test_error_logging(self):
        worker = MyWorker()
        message = create_test_message({"invalid": "data"})

        with capture_logs() as logs:
            with pytest.raises(ValidationError):
                await worker.process(message)

        # Assert error was logged
        error_logs = [log for log in logs if log.level == "ERROR"]
        assert len(error_logs) == 1
        assert "Failed to send email" in error_logs[0].message
```

---

## 📚 Additional Resources

- **Loguru Documentation**: [https://loguru.readthedocs.io/](https://loguru.readthedocs.io/)
- **Structured Logging Best Practices**: [Link to best practices guide]
- **Integration with Grafana**: [Link to Grafana integration guide]
- **OpenTelemetry Integration**: [Link to OpenTelemetry guide]

---

Ready to implement robust logging in your Pythia workers? Start with the basic configuration and gradually add more advanced features as your needs grow!
