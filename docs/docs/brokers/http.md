# HTTP Workers

Pythia's **HTTP Workers** enable efficient HTTP-based tasks like API polling and webhook sending with built-in resilience patterns.

## 🔍 HTTP Worker Types

### 1. **PollerWorker** - API Polling
Worker that performs HTTP polling to external APIs at regular intervals.

### 2. **WebhookSenderWorker** - Webhook Sending
Worker that sends HTTP webhooks with automatic retry and broadcast support.

### 3. **HTTPWorker** - Base Class
Base class that provides common HTTP functionality for all workers.

## 🚀 PollerWorker

### Basic Use Case

```python
from pythia.brokers.http import PollerWorker
from pythia.config.http import PollerConfig

class PaymentStatusPoller(PollerWorker):
    def __init__(self):
        super().__init__(
            url="https://api.payments.com/status",
            interval=30,  # Poll every 30 seconds
            method="GET",
            headers={"Authorization": "Bearer your-token"},
            params={"status": "pending"}
        )

    async def process_message(self, message):
        """Process HTTP response from polling"""
        data = message.body

        # Handle different response types
        if isinstance(data, list):
            for item in data:
                await self.process_payment_update(item)
        else:
            await self.process_payment_update(data)

        return {"processed": True, "count": len(data) if isinstance(data, list) else 1}

    async def process_payment_update(self, payment_data):
        """Process individual payment update"""
        payment_id = payment_data.get("id")
        status = payment_data.get("status")

        if status == "completed":
            await self.handle_payment_completed(payment_id)
        elif status == "failed":
            await self.handle_payment_failed(payment_id, payment_data.get("error"))

        self.logger.info(f"Processed payment {payment_id} with status {status}")

# Run the worker
if __name__ == "__main__":
    poller = PaymentStatusPoller()
    poller.run_sync()
```

### Advanced Configuration

```python
from pythia.config.http import PollerConfig

class AdvancedAPIPoller(PollerWorker):
    def __init__(self):
        # Advanced poller configuration
        poller_config = PollerConfig(
            base_url="https://api.example.com",
            url="https://api.example.com/events",
            interval=60,
            method="POST",
            connect_timeout=10.0,
            read_timeout=30.0,
            max_connections=50,
            verify_ssl=True,
            follow_redirects=True,
            user_agent="MyApp/1.0"
        )

        super().__init__(
            url="https://api.example.com/events",
            interval=60,
            method="POST",
            headers={
                "Authorization": "Bearer your-token",
                "Content-Type": "application/json"
            },
            poller_config=poller_config
        )

    async def process_message(self, message):
        # Process response with metadata
        response_data = message.body
        status_code = message.headers.get("status_code")
        timestamp = message.timestamp

        self.logger.info(
            f"Received response with status {status_code} at {timestamp}",
            response_size=len(str(response_data))
        )

        return {"status": "processed", "response_code": status_code}
```

### Custom Data Extraction

```python
def extract_events(response_data):
    """Custom data extractor function"""
    if "events" in response_data:
        return response_data["events"]  # Return list of events
    return [response_data]  # Single event

class EventPoller(PollerWorker):
    def __init__(self):
        super().__init__(
            url="https://api.events.com/feed",
            interval=120,
            data_extractor=extract_events  # Custom data extraction
        )

    async def process_message(self, message):
        """Each message contains a single extracted event"""
        event = message.body
        event_type = event.get("type")

        if event_type == "user_signup":
            await self.handle_user_signup(event)
        elif event_type == "purchase":
            await self.handle_purchase(event)

        return {"event_type": event_type, "processed": True}
```

## 📤 WebhookSenderWorker

### Basic Use Case

```python
from pythia.brokers.http import WebhookSenderWorker
from pythia.config.http import WebhookConfig

class OrderNotificationSender(WebhookSenderWorker):
    def __init__(self):
        super().__init__(
            base_url="https://webhooks.example.com",
            webhook_config=WebhookConfig(
                base_url="https://webhooks.example.com",
                timeout=30,
                max_retries=3,
                retry_delay=2.0
            )
        )

    async def process(self):
        """Main processing loop - override based on your needs"""
        # This is where you'd consume from another source
        # and send webhooks based on that data
        pass

    async def send_order_created(self, order_data):
        """Send order created webhook"""
        success = await self.send_webhook(
            endpoint="/orders/created",
            data={
                "event": "order_created",
                "order_id": order_data["id"],
                "customer_id": order_data["customer_id"],
                "amount": order_data["total"],
                "timestamp": order_data["created_at"]
            },
            headers={"X-Event-Source": "order-service"}
        )

        if success:
            self.logger.info(f"Order created webhook sent for order {order_data['id']}")
        else:
            self.logger.error(f"Failed to send order created webhook for order {order_data['id']}")

        return success
```

### Worker that Consumes and Sends Webhooks

```python
from pythia.core.message import Message
from pythia.brokers.redis import RedisListConsumer

class OrderWebhookRelay(WebhookSenderWorker):
    def __init__(self):
        super().__init__(base_url="https://partner-webhooks.com")

        # Source to consume messages
        self.source = RedisListConsumer(queue="order_events")

    async def process(self):
        """Consume from Redis and send webhooks"""
        async for message in self.source.consume():
            await self.process_message(message)

    async def process_message(self, message: Message):
        """Process message from Redis and send webhook"""
        order_data = message.body
        event_type = order_data.get("event_type")

        # Route to different webhook endpoints based on event type
        if event_type == "order_created":
            endpoint = "/webhooks/orders/created"
        elif event_type == "order_updated":
            endpoint = "/webhooks/orders/updated"
        elif event_type == "order_cancelled":
            endpoint = "/webhooks/orders/cancelled"
        else:
            self.logger.warning(f"Unknown event type: {event_type}")
            return {"error": "unknown_event_type"}

        # Send webhook
        success = await self.send_webhook_from_message(
            message=message,
            endpoint=endpoint,
            headers={"X-Source": "order-service", "X-Event-Type": event_type}
        )

        return {"webhook_sent": success, "event_type": event_type}
```

### Broadcast to Multiple Endpoints

```python
class SystemAlertBroadcaster(WebhookSenderWorker):
    def __init__(self):
        super().__init__(base_url="https://alerts.example.com")

    async def broadcast_system_alert(self, alert_data, urgent=False):
        """Broadcast system alert to multiple endpoints"""

        # Define endpoints based on urgency
        if urgent:
            endpoints = [
                "/critical/alerts",
                "/slack/alerts",
                "/email/alerts",
                "/sms/alerts"
            ]
            fail_fast = True  # Stop on first failure for urgent alerts
        else:
            endpoints = [
                "/general/alerts",
                "/slack/alerts"
            ]
            fail_fast = False  # Continue even if some fail

        # Broadcast to all endpoints
        results = await self.broadcast_webhook(
            endpoints=endpoints,
            data={
                "alert_type": alert_data["type"],
                "severity": "urgent" if urgent else "normal",
                "message": alert_data["message"],
                "timestamp": alert_data["timestamp"],
                "source": alert_data.get("source", "system")
            },
            headers={"X-Alert-Priority": "high" if urgent else "normal"},
            fail_fast=fail_fast
        )

        # Log results
        successful = sum(results.values())
        total = len(results)

        self.logger.info(
            f"Alert broadcast completed: {successful}/{total} successful",
            results=results,
            alert_type=alert_data["type"]
        )

        return results
```

## 🔧 Configuration

### Environment Variables

```bash
# HTTP Client Configuration
HTTP_BASE_URL=https://api.example.com
HTTP_TIMEOUT=30
HTTP_MAX_RETRIES=3
HTTP_RETRY_DELAY=1.0
HTTP_RETRY_BACKOFF=2.0

# Authentication
HTTP_AUTH_TYPE=bearer
HTTP_BEARER_TOKEN=your-bearer-token
HTTP_API_KEY=your-api-key
HTTP_API_KEY_HEADER=X-API-Key

# SSL/TLS
HTTP_SSL_VERIFY=true
HTTP_SSL_CERT_FILE=/path/to/cert.pem
HTTP_SSL_KEY_FILE=/path/to/key.pem

# Connection Settings
HTTP_CONNECTION_POOL_SIZE=100
HTTP_MAX_KEEPALIVE_CONNECTIONS=20
HTTP_KEEPALIVE_EXPIRY=300

# Poller Specific
POLLER_URL=https://api.example.com/events
POLLER_INTERVAL=60
POLLER_METHOD=GET
POLLER_CONNECT_TIMEOUT=10.0
POLLER_READ_TIMEOUT=30.0
```

### Programmatic Configuration

```python
from pythia.http import HTTPClientConfig
from pythia.config.http import PollerConfig, WebhookConfig

# HTTP Client base config
http_config = HTTPClientConfig(
    max_connections=100,
    connect_timeout=5.0,
    read_timeout=30.0,
    verify_ssl=True,
    default_headers={"User-Agent": "MyApp/1.0"}
)

# Poller specific config
poller_config = PollerConfig(
    base_url="https://api.example.com",
    url="https://api.example.com/events",
    interval=60,
    method="GET",
    connect_timeout=10.0,
    max_connections=50
)

# Webhook specific config
webhook_config = WebhookConfig(
    base_url="https://webhooks.example.com",
    timeout=30,
    max_retries=5,
    retry_delay=2.0,
    retry_backoff=2.0,
    endpoints={
        "orders": "/orders/webhook",
        "users": "/users/webhook",
        "alerts": "/system/alerts"
    }
)
```

## 🛡️ Resilience Patterns

### Circuit Breaker

```python
from pythia.http import CircuitBreakerConfig

class ResilientAPIPoller(PollerWorker):
    def __init__(self):
        # Configure circuit breaker
        circuit_config = CircuitBreakerConfig(
            failure_threshold=5,    # Open after 5 failures
            timeout_seconds=60,     # Stay open for 60 seconds
            expected_exception=Exception
        )

        http_config = HTTPClientConfig(
            circuit_breaker_config=circuit_config
        )

        super().__init__(
            url="https://unreliable-api.com/data",
            interval=30,
            http_config=http_config
        )

    async def process_message(self, message):
        try:
            # Process normally
            return await self.handle_api_response(message.body)
        except Exception as e:
            # Circuit breaker will handle failures
            self.logger.error(f"API processing failed: {e}")
            return {"error": str(e), "processed": False}
```

### Retry Policies

```python
from pythia.http import RetryConfig, RetryStrategy

class RetryableWebhookSender(WebhookSenderWorker):
    def __init__(self):
        # Configure custom retry policy
        retry_config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL,
            max_attempts=5,
            initial_delay=1.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter=True  # Add randomness to prevent thundering herd
        )

        http_config = HTTPClientConfig(
            retry_config=retry_config
        )

        super().__init__(
            base_url="https://webhooks.example.com",
            http_config=http_config
        )

    async def send_critical_webhook(self, data):
        """Send webhook with custom retry logic"""
        success = await self.send_webhook(
            endpoint="/critical/alerts",
            data=data,
            headers={"X-Priority": "critical"}
        )

        if not success:
            # Log failure after all retries exhausted
            self.logger.critical(
                "Critical webhook failed after all retries",
                data=data
            )

        return success
```

## 📊 Monitoring and Logging

### Structured Logging

```python
class MonitoredPoller(PollerWorker):
    def __init__(self):
        super().__init__(
            url="https://api.example.com/metrics",
            interval=60
        )
        self.metrics = {
            "requests_sent": 0,
            "responses_received": 0,
            "errors_count": 0
        }

    async def process_message(self, message):
        self.metrics["responses_received"] += 1

        # Log with context
        self.logger.info(
            "Processing API response",
            url=self.url,
            status_code=message.headers.get("status_code"),
            response_size=len(str(message.body)),
            metrics=self.metrics
        )

        try:
            result = await self.handle_response(message.body)
            return result
        except Exception as e:
            self.metrics["errors_count"] += 1
            self.logger.error(
                "Error processing API response",
                error=str(e),
                metrics=self.metrics,
                exc_info=True
            )
            raise
```

### Health Checks

```python
class HealthCheckPoller(PollerWorker):
    def __init__(self):
        super().__init__(
            url="https://api.example.com/health",
            interval=30
        )
        self.last_successful_poll = None
        self.consecutive_failures = 0

    async def process_message(self, message):
        """Monitor API health"""
        from datetime import datetime

        status_code = message.headers.get("status_code", 0)

        if 200 <= status_code < 300:
            self.last_successful_poll = datetime.now()
            self.consecutive_failures = 0

            self.logger.info(
                "API health check successful",
                status_code=status_code,
                response_time_ms=message.headers.get("response_time", 0)
            )
        else:
            self.consecutive_failures += 1

            self.logger.warning(
                "API health check failed",
                status_code=status_code,
                consecutive_failures=self.consecutive_failures
            )

            # Alert on multiple failures
            if self.consecutive_failures >= 3:
                await self.send_health_alert()

        return {
            "healthy": 200 <= status_code < 300,
            "status_code": status_code,
            "consecutive_failures": self.consecutive_failures
        }

    async def send_health_alert(self):
        """Send alert when API is unhealthy"""
        self.logger.critical(
            f"API health check failed {self.consecutive_failures} times consecutively",
            url=self.url
        )
```

## 🎯 Common Use Cases

### 1. **Payment API Integration**
```python
class PaymentWebhookProcessor(WebhookSenderWorker):
    async def process_payment_event(self, payment_data):
        if payment_data["status"] == "completed":
            await self.send_webhook("/payments/completed", payment_data)
        elif payment_data["status"] == "failed":
            await self.send_webhook("/payments/failed", payment_data)
```

### 2. **External API Monitoring**
```python
class APIMonitor(PollerWorker):
    def __init__(self):
        super().__init__(url="https://status.example.com/api", interval=60)

    async def process_message(self, message):
        if message.body.get("status") != "operational":
            await self.alert_ops_team(message.body)
```

### 3. **Data Synchronization**
```python
class DataSyncPoller(PollerWorker):
    async def process_message(self, message):
        # Sync external data to local database
        await self.sync_data_to_database(message.body)
```

---

**HTTP Workers** provide a robust and scalable way to work with HTTP APIs in Pythia, with built-in resilience patterns and flexible configuration to adapt to any use case.
