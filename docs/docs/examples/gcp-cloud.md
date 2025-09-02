# GCP Pub/Sub Example

This example shows how to build a real-time analytics pipeline using Google Cloud Pub/Sub with Pythia for processing user events and generating insights.

## Architecture

```
Web App → Pub/Sub Topic (user-events) → Analytics Processor
                                     ↓
                               Pub/Sub Topic (insights) → Dashboard Updater
```

## Prerequisites

Install Pythia with GCP support:

```bash
pip install pythia[gcp]
```

Set up GCP credentials and resources:

```bash
# Service account authentication
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GCP_PROJECT_ID="your-project-id"

# Pub/Sub resources
export PUBSUB_USER_EVENTS_TOPIC="user-events"
export PUBSUB_USER_EVENTS_SUBSCRIPTION="user-events-analytics"
export PUBSUB_INSIGHTS_TOPIC="insights"
export PUBSUB_INSIGHTS_SUBSCRIPTION="insights-dashboard"
```

## User Events Analytics Worker

```python
# analytics_processor.py
import asyncio
import json
from datetime import datetime, timedelta
from collections import defaultdict
from pythia import Worker
from pythia.brokers.cloud import PubSubSubscriber, PubSubPublisher
from pythia.models import Message

class UserEventsAnalytics(Worker):
    """Processes user events and generates real-time analytics."""

    source = PubSubSubscriber(
        subscription_name="${PUBSUB_USER_EVENTS_SUBSCRIPTION}",
        max_messages=50,
        ack_deadline=60  # 60 seconds to process
    )

    def __init__(self):
        super().__init__()
        # Publisher for insights
        self.insights_publisher = PubSubPublisher(
            topic_name="${PUBSUB_INSIGHTS_TOPIC}"
        )

        # In-memory analytics state (use Redis in production)
        self.user_sessions = defaultdict(dict)
        self.page_views = defaultdict(int)
        self.conversion_funnel = defaultdict(int)

    async def process(self, message: Message) -> dict:
        """Process user event and generate analytics."""
        event_data = message.body
        event_type = event_data.get("event_type")
        user_id = event_data.get("user_id")
        timestamp = datetime.fromisoformat(event_data.get("timestamp"))

        print(f"Processing {event_type} for user {user_id}")

        # Update analytics based on event type
        if event_type == "page_view":
            return await self._process_page_view(event_data, timestamp)
        elif event_type == "button_click":
            return await self._process_button_click(event_data, timestamp)
        elif event_type == "purchase":
            return await self._process_purchase(event_data, timestamp)
        elif event_type == "user_signup":
            return await self._process_signup(event_data, timestamp)
        else:
            print(f"Unknown event type: {event_type}")
            return {"status": "ignored"}

    async def _process_page_view(self, event_data: dict, timestamp: datetime) -> dict:
        """Process page view event."""
        user_id = event_data["user_id"]
        page = event_data.get("page", "unknown")

        # Update page view counters
        self.page_views[page] += 1

        # Update user session
        session = self.user_sessions[user_id]
        session["last_activity"] = timestamp
        session.setdefault("pages_visited", []).append({
            "page": page,
            "timestamp": timestamp.isoformat()
        })

        # Publish real-time page view insight
        await self.insights_publisher.send(
            message=Message(body={
                "insight_type": "page_popularity",
                "page": page,
                "view_count": self.page_views[page],
                "timestamp": timestamp.isoformat()
            }),
            ordering_key=f"page:{page}",
            attributes={
                "insight_type": "page_popularity",
                "page": page
            }
        )

        return {"status": "processed", "page": page, "user_id": user_id}

    async def _process_button_click(self, event_data: dict, timestamp: datetime) -> dict:
        """Process button click event."""
        user_id = event_data["user_id"]
        button_id = event_data.get("button_id")
        page = event_data.get("page")

        # Track conversion funnel
        funnel_step = self._get_funnel_step(button_id, page)
        if funnel_step:
            self.conversion_funnel[funnel_step] += 1

            # Publish funnel insight
            await self.insights_publisher.send(
                message=Message(body={
                    "insight_type": "conversion_funnel",
                    "step": funnel_step,
                    "count": self.conversion_funnel[funnel_step],
                    "timestamp": timestamp.isoformat()
                }),
                ordering_key="funnel:conversion",
                attributes={
                    "insight_type": "conversion_funnel",
                    "funnel_step": funnel_step
                }
            )

        return {"status": "processed", "button_id": button_id, "funnel_step": funnel_step}

    async def _process_purchase(self, event_data: dict, timestamp: datetime) -> dict:
        """Process purchase event."""
        user_id = event_data["user_id"]
        amount = event_data.get("amount", 0)
        items = event_data.get("items", [])

        # Update conversion funnel
        self.conversion_funnel["purchase"] += 1

        # Calculate session value
        session = self.user_sessions[user_id]
        session["total_spent"] = session.get("total_spent", 0) + amount
        session["purchases"] = session.get("purchases", 0) + 1

        # Publish purchase insights
        await self.insights_publisher.send(
            message=Message(body={
                "insight_type": "purchase_analytics",
                "user_id": user_id,
                "amount": amount,
                "items_count": len(items),
                "session_value": session["total_spent"],
                "timestamp": timestamp.isoformat()
            }),
            ordering_key=f"user:{user_id}",
            attributes={
                "insight_type": "purchase_analytics",
                "user_id": str(user_id),
                "amount": str(amount)
            }
        )

        return {
            "status": "processed",
            "amount": amount,
            "user_id": user_id,
            "session_value": session["total_spent"]
        }

    async def _process_signup(self, event_data: dict, timestamp: datetime) -> dict:
        """Process user signup event."""
        user_id = event_data["user_id"]
        source = event_data.get("source", "direct")

        # Track acquisition
        self.conversion_funnel["signup"] += 1

        # Initialize user session
        self.user_sessions[user_id] = {
            "signup_time": timestamp,
            "signup_source": source,
            "total_spent": 0,
            "purchases": 0,
            "pages_visited": []
        }

        # Publish acquisition insight
        await self.insights_publisher.send(
            message=Message(body={
                "insight_type": "user_acquisition",
                "user_id": user_id,
                "source": source,
                "total_signups": self.conversion_funnel["signup"],
                "timestamp": timestamp.isoformat()
            }),
            ordering_key=f"acquisition:{source}",
            attributes={
                "insight_type": "user_acquisition",
                "source": source,
                "user_id": str(user_id)
            }
        )

        return {"status": "processed", "user_id": user_id, "source": source}

    def _get_funnel_step(self, button_id: str, page: str) -> str:
        """Map button clicks to funnel steps."""
        funnel_mapping = {
            ("home", "cta_button"): "landing_cta",
            ("pricing", "select_plan"): "plan_selection",
            ("signup", "create_account"): "signup_attempt",
            ("checkout", "purchase_button"): "purchase_attempt"
        }
        return funnel_mapping.get((page, button_id))

# Run the analytics worker
if __name__ == "__main__":
    worker = UserEventsAnalytics()
    asyncio.run(worker.start())
```

## Dashboard Updater Worker

```python
# dashboard_updater.py
import asyncio
import json
from pythia import Worker
from pythia.brokers.cloud import PubSubSubscriber
from pythia.models import Message

class DashboardUpdater(Worker):
    """Updates real-time dashboard with analytics insights."""

    source = PubSubSubscriber(
        subscription_name="${PUBSUB_INSIGHTS_SUBSCRIPTION}",
        max_messages=20,
        ack_deadline=30
    )

    def __init__(self):
        super().__init__()
        # In production, this would connect to your dashboard backend
        self.dashboard_state = {
            "page_views": {},
            "conversion_funnel": {},
            "revenue_metrics": {},
            "user_acquisition": {}
        }

    async def process(self, message: Message) -> dict:
        """Update dashboard with new insights."""
        insight_data = message.body
        insight_type = insight_data.get("insight_type")

        print(f"Updating dashboard with {insight_type} insight")

        if insight_type == "page_popularity":
            return await self._update_page_views(insight_data)
        elif insight_type == "conversion_funnel":
            return await self._update_funnel_metrics(insight_data)
        elif insight_type == "purchase_analytics":
            return await self._update_revenue_metrics(insight_data)
        elif insight_type == "user_acquisition":
            return await self._update_acquisition_metrics(insight_data)
        else:
            return {"status": "ignored", "reason": f"Unknown insight type: {insight_type}"}

    async def _update_page_views(self, insight_data: dict) -> dict:
        """Update page view dashboard."""
        page = insight_data["page"]
        view_count = insight_data["view_count"]

        self.dashboard_state["page_views"][page] = view_count

        # Simulate dashboard API call
        await asyncio.sleep(0.1)
        print(f"📊 Dashboard updated: {page} has {view_count} views")

        return {"status": "updated", "component": "page_views", "page": page}

    async def _update_funnel_metrics(self, insight_data: dict) -> dict:
        """Update conversion funnel dashboard."""
        step = insight_data["step"]
        count = insight_data["count"]

        self.dashboard_state["conversion_funnel"][step] = count

        # Calculate conversion rates
        funnel_order = ["landing_cta", "plan_selection", "signup_attempt", "purchase_attempt", "purchase"]
        conversion_rates = {}

        for i, current_step in enumerate(funnel_order[1:], 1):
            previous_step = funnel_order[i-1]
            current_count = self.dashboard_state["conversion_funnel"].get(current_step, 0)
            previous_count = self.dashboard_state["conversion_funnel"].get(previous_step, 1)
            conversion_rates[f"{previous_step}_to_{current_step}"] = (current_count / previous_count) * 100 if previous_count > 0 else 0

        await asyncio.sleep(0.1)
        print(f"🎯 Funnel updated: {step} = {count} ({conversion_rates})")

        return {"status": "updated", "component": "conversion_funnel", "step": step, "rates": conversion_rates}

    async def _update_revenue_metrics(self, insight_data: dict) -> dict:
        """Update revenue dashboard."""
        amount = insight_data["amount"]
        session_value = insight_data["session_value"]

        current_revenue = self.dashboard_state["revenue_metrics"].get("total_revenue", 0)
        self.dashboard_state["revenue_metrics"]["total_revenue"] = current_revenue + amount
        self.dashboard_state["revenue_metrics"]["avg_session_value"] = session_value

        await asyncio.sleep(0.1)
        print(f"💰 Revenue updated: +${amount} (Total: ${current_revenue + amount})")

        return {"status": "updated", "component": "revenue", "new_revenue": amount}

    async def _update_acquisition_metrics(self, insight_data: dict) -> dict:
        """Update user acquisition dashboard."""
        source = insight_data["source"]
        total_signups = insight_data["total_signups"]

        if "sources" not in self.dashboard_state["user_acquisition"]:
            self.dashboard_state["user_acquisition"]["sources"] = {}

        self.dashboard_state["user_acquisition"]["sources"][source] = self.dashboard_state["user_acquisition"]["sources"].get(source, 0) + 1
        self.dashboard_state["user_acquisition"]["total_signups"] = total_signups

        await asyncio.sleep(0.1)
        print(f"👥 Acquisition updated: {source} (+1), Total signups: {total_signups}")

        return {"status": "updated", "component": "acquisition", "source": source}

# Run the dashboard updater
if __name__ == "__main__":
    worker = DashboardUpdater()
    asyncio.run(worker.start())
```

## Event Generator (for Testing)

```python
# event_generator.py
import asyncio
import json
import random
from datetime import datetime
from pythia.brokers.cloud import PubSubPublisher
from pythia.models import Message

class EventGenerator:
    """Generates realistic user events for testing."""

    def __init__(self):
        self.publisher = PubSubPublisher(
            topic_name="${PUBSUB_USER_EVENTS_TOPIC}"
        )

        self.pages = ["home", "pricing", "signup", "checkout", "dashboard"]
        self.buttons = {
            "home": ["cta_button", "learn_more", "pricing_link"],
            "pricing": ["select_plan", "contact_sales"],
            "signup": ["create_account", "google_signup"],
            "checkout": ["purchase_button", "back_button"]
        }
        self.sources = ["google", "facebook", "direct", "referral", "email"]

    async def generate_user_session(self, user_id: int):
        """Generate a realistic user session with multiple events."""
        source = random.choice(self.sources)

        # Start with signup
        await self._send_event("user_signup", {
            "user_id": user_id,
            "source": source,
            "timestamp": datetime.now().isoformat()
        })

        await asyncio.sleep(0.5)

        # Browse some pages
        current_page = "home"
        for _ in range(random.randint(2, 6)):
            # Page view
            await self._send_event("page_view", {
                "user_id": user_id,
                "page": current_page,
                "timestamp": datetime.now().isoformat()
            })

            await asyncio.sleep(random.uniform(0.1, 0.5))

            # Maybe click a button
            if current_page in self.buttons and random.random() < 0.7:
                button = random.choice(self.buttons[current_page])
                await self._send_event("button_click", {
                    "user_id": user_id,
                    "page": current_page,
                    "button_id": button,
                    "timestamp": datetime.now().isoformat()
                })

                await asyncio.sleep(random.uniform(0.1, 0.3))

            # Navigate to next page
            current_page = random.choice(self.pages)

        # Maybe make a purchase (30% chance)
        if random.random() < 0.3:
            await self._send_event("purchase", {
                "user_id": user_id,
                "amount": round(random.uniform(29.99, 199.99), 2),
                "items": [f"item_{i}" for i in range(random.randint(1, 3))],
                "timestamp": datetime.now().isoformat()
            })

    async def _send_event(self, event_type: str, event_data: dict):
        """Send an event to Pub/Sub."""
        event_data["event_type"] = event_type

        await self.publisher.send(
            message=Message(body=event_data),
            ordering_key=f"user:{event_data['user_id']}",
            attributes={
                "event_type": event_type,
                "user_id": str(event_data["user_id"])
            }
        )

        print(f"📤 Sent {event_type} for user {event_data['user_id']}")

async def run_simulation(num_users: int = 10):
    """Run a simulation with multiple users."""
    generator = EventGenerator()

    # Generate sessions for multiple users concurrently
    tasks = [
        generator.generate_user_session(user_id)
        for user_id in range(1, num_users + 1)
    ]

    await asyncio.gather(*tasks)
    print(f"✅ Generated events for {num_users} users")

if __name__ == "__main__":
    asyncio.run(run_simulation(num_users=20))
```

## GCP Pub/Sub Setup

Create the required topics and subscriptions:

```bash
# Create topics
gcloud pubsub topics create user-events
gcloud pubsub topics create insights

# Create subscriptions
gcloud pubsub subscriptions create user-events-analytics --topic=user-events
gcloud pubsub subscriptions create insights-dashboard --topic=insights

# Set up IAM permissions (replace with your service account email)
gcloud pubsub topics add-iam-policy-binding user-events \
    --member="serviceAccount:your-service-account@your-project.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"

gcloud pubsub subscriptions add-iam-policy-binding user-events-analytics \
    --member="serviceAccount:your-service-account@your-project.iam.gserviceaccount.com" \
    --role="roles/pubsub.subscriber"
```

## Running the Example

1. **Start the workers:**

```bash
# Terminal 1: Analytics processor
python analytics_processor.py

# Terminal 2: Dashboard updater
python dashboard_updater.py
```

2. **Generate test events:**

```bash
# Terminal 3: Event generator
python event_generator.py
```

## Monitoring with Google Cloud

### Cloud Monitoring Metrics

Set up monitoring for your Pub/Sub resources:

```python
# monitoring_setup.py
from google.cloud import monitoring_v3

def create_pubsub_alerts(project_id: str):
    """Create monitoring alerts for Pub/Sub metrics."""
    client = monitoring_v3.AlertPolicyServiceClient()
    project_name = f"projects/{project_id}"

    # Alert for high message backlog
    policy = monitoring_v3.AlertPolicy(
        display_name="High Pub/Sub Message Backlog",
        conditions=[
            monitoring_v3.AlertPolicy.Condition(
                display_name="Message backlog > 1000",
                condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                    filter='resource.type="pubsub_subscription"',
                    comparison=monitoring_v3.ComparisonType.COMPARISON_GREATER_THAN,
                    threshold_value=1000,
                )
            )
        ]
    )

    client.create_alert_policy(name=project_name, alert_policy=policy)
```

### Error Handling and Dead Letter Topics

```python
from pythia.brokers.cloud import PubSubSubscriber
from google.api_core.exceptions import GoogleAPIError

class RobustAnalyticsProcessor(Worker):
    source = PubSubSubscriber(
        subscription_name="${PUBSUB_USER_EVENTS_SUBSCRIPTION}",
        max_messages=10,
        ack_deadline=60
    )

    async def process(self, message: Message) -> dict:
        try:
            return await self._process_event(message.body)
        except GoogleAPIError as e:
            print(f"Google API error: {e}")
            # Handle API issues (retry, dead letter, etc.)
            raise
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in message: {e}")
            # Don't retry for malformed messages
            return {"status": "skipped", "reason": "invalid_json"}
```

## Best Practices

1. **Message Ordering**: Use ordering keys for events that need to be processed in sequence
2. **Batch Processing**: Process multiple messages together to improve throughput
3. **Error Handling**: Implement dead letter topics for failed messages
4. **Monitoring**: Set up Cloud Monitoring alerts for message backlogs and processing errors
5. **Cost Optimization**: Use appropriate acknowledgment deadlines and message retention policies
6. **Scaling**: Use multiple worker instances with the same subscription for horizontal scaling

This example demonstrates how to build a real-time analytics pipeline using Google Cloud Pub/Sub with Pythia's cloud workers.
