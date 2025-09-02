# HTTP Workers

Complete guide to implementing HTTP-based workers for API polling, webhook processing, and web scraping.

## Overview

HTTP workers handle external HTTP integrations including:
- **API Pollers** - Periodically fetch data from REST APIs
- **Webhook Processors** - Handle incoming HTTP callbacks
- **Web Scrapers** - Extract data from web pages
- **Health Checkers** - Monitor external service availability

## HTTP Polling Workers

### Basic API Poller

```python
import asyncio
import json
from typing import Dict, Any, List, Optional
from pythia.core import Worker, Message
from pythia.http.poller import HTTPPoller
from pythia.config import WorkerConfig

class APIPollingWorker(Worker):
    """Worker that polls external APIs for data"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Setup HTTP poller for GitHub API
        self.github_poller = HTTPPoller(
            url="https://api.github.com/repos/python/cpython/events",
            interval=300,  # Poll every 5 minutes
            method="GET",
            headers={
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "Pythia-Worker/1.0"
            }
        )

    async def start_polling(self):
        """Start polling for GitHub events"""
        self.logger.info("Starting GitHub events polling...")

        async for message in self.github_poller.consume():
            try:
                await self.process_message(message)
            except Exception as e:
                self.logger.error(f"Failed to process polled data: {e}")
                # Continue polling despite errors

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process GitHub events from API"""
        try:
            # Parse GitHub events
            events = json.loads(message.body)

            if not isinstance(events, list):
                self.logger.warning("Unexpected API response format")
                return {"status": "error", "message": "Invalid response format"}

            # Process each event
            processed_events = []
            for event in events[:10]:  # Process first 10 events
                processed_event = await self._process_github_event(event)
                if processed_event:
                    processed_events.append(processed_event)

            self.logger.info(f"Processed {len(processed_events)} GitHub events")

            return {
                "status": "processed",
                "events_processed": len(processed_events),
                "events": processed_events,
                "poll_timestamp": message.timestamp.isoformat()
            }

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse API response: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing GitHub events: {e}")
            raise

    async def _process_github_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single GitHub event"""
        try:
            event_type = event.get("type")

            # Filter for interesting events
            interesting_events = ["PushEvent", "PullRequestEvent", "IssuesEvent", "ReleaseEvent"]

            if event_type not in interesting_events:
                return None

            processed = {
                "id": event.get("id"),
                "type": event_type,
                "actor": event.get("actor", {}).get("login"),
                "repo": event.get("repo", {}).get("name"),
                "created_at": event.get("created_at"),
                "public": event.get("public", False)
            }

            # Add event-specific data
            if event_type == "PushEvent":
                payload = event.get("payload", {})
                processed["commits"] = len(payload.get("commits", []))
                processed["branch"] = payload.get("ref", "").replace("refs/heads/", "")

            elif event_type == "PullRequestEvent":
                pr = event.get("payload", {}).get("pull_request", {})
                processed["pr_action"] = event.get("payload", {}).get("action")
                processed["pr_title"] = pr.get("title")
                processed["pr_state"] = pr.get("state")

            elif event_type == "ReleaseEvent":
                release = event.get("payload", {}).get("release", {})
                processed["release_tag"] = release.get("tag_name")
                processed["release_name"] = release.get("name")
                processed["prerelease"] = release.get("prerelease", False)

            return processed

        except Exception as e:
            self.logger.error(f"Error processing event {event.get('id')}: {e}")
            return None

# Usage example
async def run_github_poller():
    config = WorkerConfig(
        worker_name="github-events-poller",
        log_level="INFO"
    )

    worker = APIPollingWorker(config)

    # Start polling (runs indefinitely)
    await worker.start_polling()

if __name__ == "__main__":
    asyncio.run(run_github_poller())
```

### Advanced Multi-Endpoint Poller

```python
import asyncio
import json
from typing import Dict, Any, List
from dataclasses import dataclass
from pythia.core import Worker, Message
from pythia.http.poller import HTTPPoller
from pythia.config import WorkerConfig

@dataclass
class EndpointConfig:
    """Configuration for API endpoint"""
    name: str
    url: str
    interval: int
    headers: Dict[str, str] = None
    params: Dict[str, str] = None
    method: str = "GET"

class MultiEndpointPoller(Worker):
    """Poll multiple API endpoints with different intervals"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Define multiple endpoints to poll
        self.endpoints = [
            EndpointConfig(
                name="github_events",
                url="https://api.github.com/repos/python/cpython/events",
                interval=300,  # 5 minutes
                headers={"Accept": "application/vnd.github.v3+json"}
            ),
            EndpointConfig(
                name="hackernews_top",
                url="https://hacker-news.firebaseio.com/v0/topstories.json",
                interval=600,  # 10 minutes
            ),
            EndpointConfig(
                name="jsonplaceholder_posts",
                url="https://jsonplaceholder.typicode.com/posts",
                interval=1800,  # 30 minutes
                params={"_limit": "10"}
            )
        ]

        self.pollers = {}

    async def on_startup(self):
        """Setup all pollers on startup"""
        await super().on_startup()

        for endpoint in self.endpoints:
            poller = HTTPPoller(
                url=endpoint.url,
                interval=endpoint.interval,
                method=endpoint.method,
                headers=endpoint.headers or {},
                params=endpoint.params or {}
            )
            self.pollers[endpoint.name] = poller

        self.logger.info(f"Initialized {len(self.pollers)} API pollers")

    async def start_polling(self):
        """Start polling all endpoints concurrently"""
        polling_tasks = []

        for endpoint_name, poller in self.pollers.items():
            task = asyncio.create_task(
                self._poll_endpoint(endpoint_name, poller)
            )
            polling_tasks.append(task)

        self.logger.info("Starting concurrent polling of all endpoints...")

        # Wait for all polling tasks
        try:
            await asyncio.gather(*polling_tasks)
        except Exception as e:
            self.logger.error(f"Polling error: {e}")
        finally:
            # Cancel remaining tasks
            for task in polling_tasks:
                if not task.done():
                    task.cancel()

    async def _poll_endpoint(self, endpoint_name: str, poller: HTTPPoller):
        """Poll a specific endpoint"""
        self.logger.info(f"Starting poller for {endpoint_name}")

        try:
            async for message in poller.consume():
                # Add endpoint context to message
                message.headers["endpoint_name"] = endpoint_name

                # Process the message
                result = await self.process_message(message)

                self.logger.info(
                    f"Processed data from {endpoint_name}",
                    extra={"endpoint": endpoint_name, "result": result}
                )

        except Exception as e:
            self.logger.error(f"Error polling {endpoint_name}: {e}")
            # Implement exponential backoff or circuit breaker here
            await asyncio.sleep(60)  # Wait before retrying

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process message based on endpoint"""
        endpoint_name = message.headers.get("endpoint_name")

        if endpoint_name == "github_events":
            return await self._process_github_events(message)
        elif endpoint_name == "hackernews_top":
            return await self._process_hackernews(message)
        elif endpoint_name == "jsonplaceholder_posts":
            return await self._process_jsonplaceholder(message)
        else:
            return {"status": "unknown_endpoint", "endpoint": endpoint_name}

    async def _process_github_events(self, message: Message) -> Dict[str, Any]:
        """Process GitHub events"""
        events = json.loads(message.body)

        # Extract relevant information
        event_types = {}
        for event in events[:20]:  # First 20 events
            event_type = event.get("type", "unknown")
            event_types[event_type] = event_types.get(event_type, 0) + 1

        return {
            "endpoint": "github_events",
            "total_events": len(events),
            "event_types": event_types,
            "most_common": max(event_types.items(), key=lambda x: x[1]) if event_types else None
        }

    async def _process_hackernews(self, message: Message) -> Dict[str, Any]:
        """Process Hacker News top stories"""
        story_ids = json.loads(message.body)

        return {
            "endpoint": "hackernews_top",
            "total_stories": len(story_ids),
            "top_story_id": story_ids[0] if story_ids else None,
            "story_id_range": f"{min(story_ids)}-{max(story_ids)}" if story_ids else None
        }

    async def _process_jsonplaceholder(self, message: Message) -> Dict[str, Any]:
        """Process JSONPlaceholder posts"""
        posts = json.loads(message.body)

        user_posts = {}
        for post in posts:
            user_id = post.get("userId")
            user_posts[user_id] = user_posts.get(user_id, 0) + 1

        return {
            "endpoint": "jsonplaceholder_posts",
            "total_posts": len(posts),
            "unique_users": len(user_posts),
            "posts_per_user": user_posts
        }

# Usage
async def run_multi_endpoint_poller():
    config = WorkerConfig(
        worker_name="multi-endpoint-poller",
        log_level="INFO"
    )

    worker = MultiEndpointPoller(config)
    await worker.on_startup()
    await worker.start_polling()

if __name__ == "__main__":
    asyncio.run(run_multi_endpoint_poller())
```

## Webhook Processing Workers

### Basic Webhook Processor

```python
import asyncio
import json
import hmac
import hashlib
from typing import Dict, Any, Optional
from pythia.core import Worker, Message
from pythia.http.webhook import WebhookClient
from pythia.config import WorkerConfig

class WebhookProcessor(Worker):
    """Process incoming webhook events"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Setup webhook client for sending responses
        self.webhook_client = WebhookClient(
            base_url="https://api.partner.com"
        )

        # Webhook verification settings
        self.webhook_secrets = {
            "stripe": "whsec_stripe_secret_key",
            "github": "github_webhook_secret",
            "shopify": "shopify_webhook_secret"
        }

    async def process_message(self, message: Message) -> Dict[str, Any]:
        """Process incoming webhook"""
        try:
            # Determine webhook source
            webhook_source = self._identify_webhook_source(message)

            # Verify webhook signature
            if not await self._verify_webhook_signature(message, webhook_source):
                raise ValueError(f"Invalid webhook signature from {webhook_source}")

            # Parse webhook data
            webhook_data = json.loads(message.body)

            self.logger.info(
                f"Processing {webhook_source} webhook",
                extra={
                    "source": webhook_source,
                    "event_type": webhook_data.get("type"),
                    "webhook_id": webhook_data.get("id")
                }
            )

            # Route to appropriate handler
            if webhook_source == "stripe":
                return await self._handle_stripe_webhook(webhook_data)
            elif webhook_source == "github":
                return await self._handle_github_webhook(webhook_data)
            elif webhook_source == "shopify":
                return await self._handle_shopify_webhook(webhook_data)
            else:
                return await self._handle_unknown_webhook(webhook_data, webhook_source)

        except Exception as e:
            self.logger.error(f"Webhook processing failed: {e}")
            raise

    def _identify_webhook_source(self, message: Message) -> str:
        """Identify the source of the webhook"""
        headers = message.headers

        if "stripe-signature" in headers:
            return "stripe"
        elif "x-github-event" in headers:
            return "github"
        elif "x-shopify-topic" in headers:
            return "shopify"
        else:
            # Try to identify from User-Agent or other headers
            user_agent = headers.get("user-agent", "").lower()
            if "stripe" in user_agent:
                return "stripe"
            elif "github" in user_agent:
                return "github"
            elif "shopify" in user_agent:
                return "shopify"

        return "unknown"

    async def _verify_webhook_signature(self, message: Message, source: str) -> bool:
        """Verify webhook signature"""
        if source not in self.webhook_secrets:
            self.logger.warning(f"No secret configured for webhook source: {source}")
            return True  # Skip verification

        secret = self.webhook_secrets[source]
        headers = message.headers

        if source == "stripe":
            return self._verify_stripe_signature(message.body, headers.get("stripe-signature"), secret)
        elif source == "github":
            return self._verify_github_signature(message.body, headers.get("x-hub-signature-256"), secret)
        elif source == "shopify":
            return self._verify_shopify_signature(message.body, headers.get("x-shopify-hmac-sha256"), secret)

        return True

    def _verify_stripe_signature(self, body: str, signature: str, secret: str) -> bool:
        """Verify Stripe webhook signature"""
        if not signature:
            return False

        # Stripe signature format: t=timestamp,v1=signature
        elements = signature.split(",")
        signature_dict = {}

        for element in elements:
            key, value = element.split("=", 1)
            signature_dict[key] = value

        timestamp = signature_dict.get("t")
        v1_signature = signature_dict.get("v1")

        if not timestamp or not v1_signature:
            return False

        # Create expected signature
        payload = f"{timestamp}.{body}"
        expected_signature = hmac.new(
            secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(v1_signature, expected_signature)

    def _verify_github_signature(self, body: str, signature: str, secret: str) -> bool:
        """Verify GitHub webhook signature"""
        if not signature or not signature.startswith("sha256="):
            return False

        expected_signature = "sha256=" + hmac.new(
            secret.encode(),
            body.encode(),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected_signature)

    def _verify_shopify_signature(self, body: str, signature: str, secret: str) -> bool:
        """Verify Shopify webhook signature"""
        if not signature:
            return False

        expected_signature = hmac.new(
            secret.encode(),
            body.encode(),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected_signature)

    async def _handle_stripe_webhook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Stripe webhook events"""
        event_type = data.get("type")
        event_data = data.get("data", {})

        self.logger.info(f"Processing Stripe event: {event_type}")

        if event_type == "payment_intent.succeeded":
            return await self._handle_stripe_payment_succeeded(event_data)
        elif event_type == "invoice.payment_failed":
            return await self._handle_stripe_payment_failed(event_data)
        elif event_type == "customer.subscription.updated":
            return await self._handle_stripe_subscription_updated(event_data)
        else:
            return {"status": "ignored", "event_type": event_type, "source": "stripe"}

    async def _handle_github_webhook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle GitHub webhook events"""
        action = data.get("action")

        if "pull_request" in data:
            return await self._handle_github_pr_event(data, action)
        elif "issue" in data:
            return await self._handle_github_issue_event(data, action)
        elif "release" in data:
            return await self._handle_github_release_event(data, action)
        else:
            return {"status": "ignored", "action": action, "source": "github"}

    async def _handle_shopify_webhook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Shopify webhook events"""
        # Shopify webhook topic is in headers, but we can infer from data structure
        if "line_items" in data:
            return await self._handle_shopify_order_event(data)
        elif "email" in data and "first_name" in data:
            return await self._handle_shopify_customer_event(data)
        else:
            return {"status": "processed", "data_keys": list(data.keys()), "source": "shopify"}

    async def _handle_stripe_payment_succeeded(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle successful Stripe payment"""
        payment_intent = event_data.get("object", {})
        amount = payment_intent.get("amount", 0) / 100  # Convert from cents
        customer_id = payment_intent.get("customer")

        # Process payment success (update database, send confirmation, etc.)
        await self._update_payment_status(payment_intent.get("id"), "succeeded")
        await self._send_payment_confirmation(customer_id, amount)

        return {
            "status": "processed",
            "event_type": "payment_succeeded",
            "amount": amount,
            "customer_id": customer_id,
            "source": "stripe"
        }

    async def _handle_github_pr_event(self, data: Dict[str, Any], action: str) -> Dict[str, Any]:
        """Handle GitHub PR events"""
        pr = data.get("pull_request", {})
        pr_number = pr.get("number")
        pr_title = pr.get("title")

        if action == "opened":
            # Run CI checks, notify team, etc.
            await self._trigger_pr_checks(pr_number)
            await self._notify_team_pr_opened(pr_number, pr_title)
        elif action == "closed" and pr.get("merged"):
            # Handle merged PR
            await self._handle_pr_merged(pr_number)

        return {
            "status": "processed",
            "event_type": f"pr_{action}",
            "pr_number": pr_number,
            "merged": pr.get("merged", False),
            "source": "github"
        }

    async def _handle_shopify_order_event(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Shopify order events"""
        order_id = data.get("id")
        customer_email = data.get("email")
        total_price = data.get("total_price")

        # Process order (update inventory, send confirmation, etc.)
        await self._update_inventory(data.get("line_items", []))
        await self._send_order_confirmation(customer_email, order_id)

        return {
            "status": "processed",
            "event_type": "order_created",
            "order_id": order_id,
            "total_price": total_price,
            "source": "shopify"
        }

    async def _handle_unknown_webhook(self, data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Handle unknown webhook types"""
        self.logger.warning(f"Unknown webhook from {source}: {data.keys()}")

        # Store for later analysis or forward to dead letter queue
        await self._store_unknown_webhook(data, source)

        return {
            "status": "logged",
            "source": source,
            "data_keys": list(data.keys())
        }

    # Helper methods (implement based on your business logic)
    async def _update_payment_status(self, payment_id: str, status: str):
        """Update payment status in database"""
        self.logger.info(f"Updating payment {payment_id} to {status}")
        # Database update logic here

    async def _send_payment_confirmation(self, customer_id: str, amount: float):
        """Send payment confirmation to customer"""
        self.logger.info(f"Sending payment confirmation to customer {customer_id} for ${amount}")
        # Email/notification logic here

    async def _trigger_pr_checks(self, pr_number: int):
        """Trigger CI checks for PR"""
        self.logger.info(f"Triggering CI checks for PR #{pr_number}")
        # CI integration logic here

    async def _notify_team_pr_opened(self, pr_number: int, title: str):
        """Notify team of new PR"""
        self.logger.info(f"Notifying team of PR #{pr_number}: {title}")
        # Slack/Teams notification logic here

    async def _handle_pr_merged(self, pr_number: int):
        """Handle merged PR"""
        self.logger.info(f"PR #{pr_number} was merged")
        # Deployment trigger, cleanup, etc.

    async def _update_inventory(self, line_items: List[Dict]):
        """Update inventory based on order items"""
        for item in line_items:
            product_id = item.get("product_id")
            quantity = item.get("quantity", 0)
            self.logger.info(f"Updating inventory for product {product_id}: -{quantity}")
        # Inventory update logic here

    async def _send_order_confirmation(self, email: str, order_id: str):
        """Send order confirmation email"""
        self.logger.info(f"Sending order confirmation for {order_id} to {email}")
        # Email sending logic here

    async def _store_unknown_webhook(self, data: Dict[str, Any], source: str):
        """Store unknown webhook for analysis"""
        self.logger.info(f"Storing unknown webhook from {source}")
        # Storage logic here

# Usage example with message broker integration
async def run_webhook_processor():
    # Configure for Kafka to receive webhooks
    config = WorkerConfig(
        worker_name="webhook-processor",
        broker_type="kafka",  # Webhooks received via Kafka topic
        log_level="INFO"
    )

    worker = WebhookProcessor(config)
    await worker.start()

if __name__ == "__main__":
    asyncio.run(run_webhook_processor())
```

## Health Check Workers

### Service Health Monitor

```python
import asyncio
import aiohttp
from typing import Dict, Any, List
from dataclasses import dataclass
from datetime import datetime, timedelta
from pythia.core import Worker
from pythia.config import WorkerConfig

@dataclass
class HealthCheckConfig:
    name: str
    url: str
    method: str = "GET"
    timeout: int = 10
    expected_status: int = 200
    interval: int = 60
    headers: Dict[str, str] = None

class HealthCheckWorker(Worker):
    """Monitor health of external services"""

    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Define services to monitor
        self.services = [
            HealthCheckConfig(
                name="main_api",
                url="https://api.example.com/health",
                timeout=5,
                interval=30
            ),
            HealthCheckConfig(
                name="database",
                url="https://db.example.com/ping",
                timeout=10,
                interval=60
            ),
            HealthCheckConfig(
                name="redis_cache",
                url="https://cache.example.com/ping",
                timeout=3,
                interval=30
            ),
            HealthCheckConfig(
                name="external_service",
                url="https://partner.example.com/status",
                headers={"Authorization": "Bearer token"},
                interval=120
            )
        ]

        self.health_status = {}
        self.last_check = {}

    async def on_startup(self):
        """Initialize health monitoring"""
        await super().on_startup()

        # Initialize status tracking
        for service in self.services:
            self.health_status[service.name] = None
            self.last_check[service.name] = None

        # Start health checking
        asyncio.create_task(self._start_health_monitoring())

    async def _start_health_monitoring(self):
        """Start monitoring all services"""
        self.logger.info(f"Starting health monitoring for {len(self.services)} services")

        # Create tasks for each service
        tasks = []
        for service in self.services:
            task = asyncio.create_task(self._monitor_service(service))
            tasks.append(task)

        # Wait for all monitoring tasks
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _monitor_service(self, service: HealthCheckConfig):
        """Monitor a single service continuously"""
        while True:
            try:
                await self._check_service_health(service)
                await asyncio.sleep(service.interval)
            except Exception as e:
                self.logger.error(f"Error monitoring {service.name}: {e}")
                await asyncio.sleep(service.interval)

    async def _check_service_health(self, service: HealthCheckConfig):
        """Check health of a single service"""
        check_start = datetime.now()

        try:
            timeout = aiohttp.ClientTimeout(total=service.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:

                async with session.request(
                    method=service.method,
                    url=service.url,
                    headers=service.headers or {}
                ) as response:

                    response_time = (datetime.now() - check_start).total_seconds()

                    # Check if response is healthy
                    is_healthy = response.status == service.expected_status

                    # Update status
                    previous_status = self.health_status.get(service.name)
                    self.health_status[service.name] = {
                        "healthy": is_healthy,
                        "status_code": response.status,
                        "response_time": response_time,
                        "checked_at": check_start.isoformat(),
                        "error": None
                    }
                    self.last_check[service.name] = check_start

                    # Log status changes
                    if previous_status is None:
                        self.logger.info(f"Initial health check for {service.name}: {'✅' if is_healthy else '❌'}")
                    elif previous_status["healthy"] != is_healthy:
                        status_change = "recovered" if is_healthy else "failed"
                        self.logger.warning(f"Service {service.name} {status_change}")

                        # Send alert for status changes
                        await self._send_health_alert(service, is_healthy, response_time)

                    # Log periodic status for healthy services
                    if is_healthy:
                        self.logger.debug(
                            f"Health check passed for {service.name}",
                            extra={
                                "service": service.name,
                                "response_time": response_time,
                                "status_code": response.status
                            }
                        )

        except asyncio.TimeoutError:
            await self._handle_health_check_error(service, "Timeout", check_start)
        except aiohttp.ClientError as e:
            await self._handle_health_check_error(service, f"Client error: {e}", check_start)
        except Exception as e:
            await self._handle_health_check_error(service, f"Unexpected error: {e}", check_start)

    async def _handle_health_check_error(self, service: HealthCheckConfig, error: str, check_start: datetime):
        """Handle health check errors"""
        response_time = (datetime.now() - check_start).total_seconds()

        previous_status = self.health_status.get(service.name)
        self.health_status[service.name] = {
            "healthy": False,
            "status_code": None,
            "response_time": response_time,
            "checked_at": check_start.isoformat(),
            "error": error
        }
        self.last_check[service.name] = check_start

        # Log and alert on status change
        if previous_status is None or previous_status.get("healthy", False):
            self.logger.error(f"Health check failed for {service.name}: {error}")
            await self._send_health_alert(service, False, response_time, error)

    async def _send_health_alert(self, service: HealthCheckConfig, is_healthy: bool, response_time: float, error: str = None):
        """Send health status alert"""
        status = "RECOVERED" if is_healthy else "FAILED"

        alert_data = {
            "service": service.name,
            "status": status,
            "url": service.url,
            "response_time": response_time,
            "timestamp": datetime.now().isoformat()
        }

        if error:
            alert_data["error"] = error

        # Send to monitoring system (implement based on your needs)
        self.logger.warning(f"ALERT: Service {service.name} {status}", extra=alert_data)

        # Here you would integrate with:
        # - Slack/Teams notifications
        # - PagerDuty/OpsGenie
        # - Email alerts
        # - Webhook notifications

    async def get_health_summary(self) -> Dict[str, Any]:
        """Get overall health summary"""
        healthy_count = sum(1 for status in self.health_status.values()
                          if status and status.get("healthy", False))
        total_count = len(self.services)

        # Find services with issues
        unhealthy_services = []
        for service_name, status in self.health_status.items():
            if not status or not status.get("healthy", False):
                unhealthy_services.append({
                    "name": service_name,
                    "error": status.get("error") if status else "Not checked yet",
                    "last_check": self.last_check.get(service_name)
                })

        return {
            "overall_health": "healthy" if healthy_count == total_count else "degraded",
            "healthy_services": healthy_count,
            "total_services": total_count,
            "health_percentage": (healthy_count / total_count) * 100 if total_count > 0 else 0,
            "unhealthy_services": unhealthy_services,
            "last_update": datetime.now().isoformat()
        }

    async def process_message(self, message):
        """Health check worker doesn't process traditional messages"""
        # Could be used to process health check requests or configuration updates
        pass

# Usage example
async def run_health_monitor():
    config = WorkerConfig(
        worker_name="health-monitor",
        log_level="INFO"
    )

    worker = HealthCheckWorker(config)
    await worker.on_startup()

    # Monitor health status
    async def status_reporter():
        while True:
            await asyncio.sleep(300)  # Report every 5 minutes
            summary = await worker.get_health_summary()
            print(f"\n🏥 Health Summary:")
            print(f"   Overall: {summary['overall_health'].upper()}")
            print(f"   Healthy: {summary['healthy_services']}/{summary['total_services']} ({summary['health_percentage']:.1f}%)")

            if summary['unhealthy_services']:
                print("   ❌ Issues:")
                for service in summary['unhealthy_services']:
                    print(f"      - {service['name']}: {service['error']}")

    # Start monitoring and reporting
    await asyncio.gather(
        status_reporter(),
        return_exceptions=True
    )

if __name__ == "__main__":
    asyncio.run(run_health_monitor())
```

## Best Practices

### 1. Rate Limiting and Respect API Limits

```python
class RateLimitedPoller(HTTPPoller):
    def __init__(self, *args, rate_limit: int = 60, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate_limit = rate_limit  # requests per minute
        self.request_times = []

    async def _check_rate_limit(self):
        """Check and enforce rate limiting"""
        now = datetime.now()

        # Remove old requests (older than 1 minute)
        self.request_times = [
            req_time for req_time in self.request_times
            if now - req_time < timedelta(minutes=1)
        ]

        # Check if we're at the limit
        if len(self.request_times) >= self.rate_limit:
            wait_time = 60 - (now - self.request_times[0]).seconds
            await asyncio.sleep(wait_time)

        self.request_times.append(now)
```

### 2. Circuit Breaker Pattern

```python
from pythia.http.circuit_breaker import CircuitBreaker

class ResilientHTTPWorker(Worker):
    def __init__(self, config):
        super().__init__(config)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60
        )

    async def make_http_request(self, url: str):
        """Make HTTP request with circuit breaker protection"""
        try:
            return await self.circuit_breaker.call(
                self._http_request, url
            )
        except CircuitBreakerError:
            # Circuit is open, use fallback
            return await self._fallback_response()
```

### 3. Error Handling and Retries

```python
class RetryableHTTPWorker(Worker):
    async def process_with_retry(self, request_func, max_retries: int = 3):
        """Process HTTP request with retry logic"""
        for attempt in range(max_retries + 1):
            try:
                return await request_func()
            except aiohttp.ClientTimeout:
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    await asyncio.sleep(wait_time)
                else:
                    raise
            except aiohttp.ClientError as e:
                if attempt < max_retries and self._is_retryable_error(e):
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise

    def _is_retryable_error(self, error) -> bool:
        """Determine if error is retryable"""
        # Implement based on your needs
        return isinstance(error, (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError))
```

## Testing HTTP Workers

```python
import pytest
from unittest.mock import AsyncMock, patch
import aiohttp

class TestAPIPollingWorker:
    @pytest.fixture
    def worker(self):
        config = WorkerConfig(worker_name="test-poller")
        return APIPollingWorker(config)

    @pytest.mark.asyncio
    async def test_github_event_processing(self, worker):
        """Test GitHub event processing"""
        mock_events = [
            {
                "id": "123",
                "type": "PushEvent",
                "actor": {"login": "testuser"},
                "repo": {"name": "test/repo"},
                "created_at": "2024-01-01T00:00:00Z"
            }
        ]

        message = Message(body=json.dumps(mock_events))
        result = await worker.process_message(message)

        assert result["status"] == "processed"
        assert result["events_processed"] == 1

    @pytest.mark.asyncio
    async def test_webhook_signature_verification(self):
        """Test webhook signature verification"""
        processor = WebhookProcessor(WorkerConfig())

        # Mock Stripe webhook
        body = '{"type":"payment_intent.succeeded"}'
        signature = "t=1234567890,v1=test_signature"

        with patch.object(processor, '_verify_stripe_signature') as mock_verify:
            mock_verify.return_value = True

            message = Message(
                body=body,
                headers={"stripe-signature": signature}
            )

            result = await processor._verify_webhook_signature(message, "stripe")
            assert result is True
```

## Next Steps

- [Background Jobs](background-jobs.md) - Job queue worker patterns
- [Message Workers](../brokers/kafka.md) - Message-based worker examples
- [Error Handling](../user-guide/error-handling.md) - Advanced error handling patterns
