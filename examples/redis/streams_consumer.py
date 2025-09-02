#!/usr/bin/env python3
"""
Redis Streams Consumer Example with Pythia

This example demonstrates:
1. Creating a Redis Streams consumer worker
2. Consumer groups for scalable processing
3. Real-time event stream processing
4. Automatic stream management

Requirements:
- Docker with Redis running (see docker-compose.yml)
- pip install pythia[redis]

Usage:
- Start Redis: docker-compose up -d redis
- Run consumer: python streams_consumer.py
- Generate events: python streams_consumer.py generate-data
"""

import asyncio
import json
import random
import time
from typing import Dict, Any
from pydantic import BaseModel

from pythia import Worker
from pythia.brokers.redis import RedisStreamsConsumer, RedisStreamsProducer
from pythia.config.redis import RedisConfig


# Event schemas
class ActivityEvent(BaseModel):
    """User activity event"""

    user_id: str
    session_id: str
    event_type: str  # page_view, click, scroll, etc.
    page_url: str
    timestamp: float
    properties: Dict[str, Any] = {}


class RedisActivityProcessor(Worker):
    """
    Redis Streams consumer that processes user activity events
    """

    def __init__(self, worker_id: str | None = None):
        self.worker_id = worker_id or f"worker-{random.randint(1000, 9999)}"

        # Configure Redis Streams consumer
        self.source = RedisStreamsConsumer(
            stream="user-activity",
            consumer_group="activity-processors",
            consumer_name=self.worker_id,
            config=RedisConfig(
                host="localhost",
                port=6379,
                db=0,
                batch_size=10,  # Process 10 messages at a time
                block_timeout_ms=1000,  # Block for 1 second waiting for messages
            ),
        )

        # Producer for processed events
        self.processed_producer = RedisStreamsProducer(
            stream="processed-activity",
            config=RedisConfig(
                host="localhost",
                port=6379,
                db=0,
                max_stream_length=10000,  # Keep last 10K processed events
            ),
        )

        super().__init__()

        # Session tracking
        self.session_data = {}

    async def process(self, message) -> None:
        """Process each activity event"""
        try:
            # Parse the Redis stream message
            # message.body contains the stream fields as dict
            event_data = message.body

            # Ensure event_data is a dict
            if not isinstance(event_data, dict):
                raise ValueError(f"Expected dict message body, got {type(event_data)}")

            # Convert to our model
            activity = ActivityEvent(
                user_id=str(event_data["user_id"]),
                session_id=str(event_data["session_id"]),
                event_type=str(event_data["event_type"]),
                page_url=str(event_data["page_url"]),
                timestamp=float(event_data["timestamp"]),
                properties=json.loads(event_data.get("properties", "{}")),
            )

            self.logger.info(
                f"Processing {activity.event_type} for user {activity.user_id}",
                extra={
                    "user_id": activity.user_id,
                    "session_id": activity.session_id,
                    "event_type": activity.event_type,
                    "page_url": activity.page_url,
                    "worker_id": self.worker_id,
                    "stream_id": message.message_id,
                },
            )

            # Business logic - track user sessions
            await self._update_session_tracking(activity)

            # Business logic - detect important events
            insights = await self._generate_insights(activity)

            # Create processed event
            processed_event = {
                "original_event": activity.model_dump(),
                "insights": insights,
                "processed_by": self.worker_id,
                "processed_at": time.time(),
                "stream_id": message.message_id,
            }

            # Send to processed stream
            await self.processed_producer.send(processed_event)

            self.logger.debug(f"✅ Processed event {message.message_id}")

        except Exception as e:
            self.logger.error(
                f"Error processing stream message: {e}",
                extra={"stream_id": message.message_id, "worker_id": self.worker_id},
            )
            raise

    async def _update_session_tracking(self, activity: ActivityEvent):
        """Update session tracking data"""
        session_key = f"{activity.user_id}:{activity.session_id}"

        if session_key not in self.session_data:
            self.session_data[session_key] = {
                "start_time": activity.timestamp,
                "last_activity": activity.timestamp,
                "page_views": 0,
                "clicks": 0,
                "pages": set(),
            }

        session = self.session_data[session_key]
        session["last_activity"] = activity.timestamp
        session["pages"].add(activity.page_url)

        if activity.event_type == "page_view":
            session["page_views"] += 1
        elif activity.event_type == "click":
            session["clicks"] += 1

    async def _generate_insights(self, activity: ActivityEvent) -> Dict[str, Any]:
        """Generate insights from the activity"""
        session_key = f"{activity.user_id}:{activity.session_id}"
        session = self.session_data.get(session_key, {})

        insights = {
            "session_duration": activity.timestamp - session.get("start_time", activity.timestamp),
            "pages_in_session": len(session.get("pages", set())),
            "is_bounce": session.get("page_views", 0) <= 1,
            "engagement_score": min(
                session.get("clicks", 0) * 0.5 + session.get("page_views", 0) * 0.3,
                10.0,
            ),
        }

        # Detect patterns
        if activity.event_type == "page_view" and "checkout" in activity.page_url:
            insights["intent"] = "purchase"
        elif activity.event_type == "click" and "product" in activity.page_url:
            insights["intent"] = "product_interest"
        else:
            insights["intent"] = "browsing"

        return insights

    async def on_startup(self):
        """Worker startup"""
        self.logger.info(f"🚀 Redis Activity Processor starting (ID: {self.worker_id})")
        # Log stream and consumer group info (Redis specific)
        stream_name = getattr(self.source, "stream", "unknown")
        consumer_group = getattr(self.source, "consumer_group", "unknown")
        self.logger.info(f"📥 Consuming from stream: {stream_name}")
        self.logger.info(f"👥 Consumer group: {consumer_group}")
        self.logger.info(f"📤 Publishing to: {self.processed_producer.stream}")

    async def on_shutdown(self):
        """Worker shutdown"""
        self.logger.info(f"🛑 Redis Activity Processor shutting down (ID: {self.worker_id})")

        # Log session summary
        if self.session_data:
            self.logger.info(f"Tracked {len(self.session_data)} active sessions")


# Data generator for testing
async def generate_test_data():
    """Generate realistic user activity data"""
    producer = RedisStreamsProducer(
        stream="user-activity",
        config=RedisConfig(
            host="localhost",
            port=6379,
            db=0,
            max_stream_length=50000,  # Keep last 50K activity events
        ),
    )

    await producer.connect()

    # Sample data
    users = [f"user_{i:03d}" for i in range(1, 21)]  # 20 users
    pages = [
        "/home",
        "/products",
        "/product/123",
        "/product/456",
        "/cart",
        "/checkout",
        "/account",
        "/help",
        "/about",
    ]
    event_types = ["page_view", "click", "scroll", "hover"]

    print("🔄 Generating user activity events...")

    # Generate events for 60 seconds
    end_time = time.time() + 60
    event_count = 0

    while time.time() < end_time:
        # Random event
        user_id = random.choice(users)
        session_id = f"sess_{random.randint(10000, 99999)}"
        event_type = random.choice(event_types)
        page_url = random.choice(pages)

        # Create realistic properties
        properties = {}
        if event_type == "click":
            properties["element"] = random.choice(["button", "link", "image"])
        elif event_type == "scroll":
            properties["scroll_depth"] = random.randint(10, 90)

        # Create event
        event = {
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "page_url": page_url,
            "timestamp": str(time.time()),
            "properties": json.dumps(properties),
        }

        # Send to stream
        stream_id = await producer.send(event)
        event_count += 1

        if event_count % 10 == 0:
            print(f"📊 Generated {event_count} events... (latest: {stream_id})")

        # Variable delay to simulate real traffic
        await asyncio.sleep(random.uniform(0.1, 2.0))

    await producer.disconnect()
    print(f"✅ Generated {event_count} activity events")


# Stream monitoring
async def monitor_streams():
    """Monitor Redis streams status"""
    import redis.asyncio as redis

    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

    try:
        # Get stream info
        activity_info = await r.xinfo_stream("user-activity")
        processed_info = await r.xinfo_stream("processed-activity")

        print("📊 Stream Status:")
        print(f"  user-activity: {activity_info['length']} messages")
        print(f"  processed-activity: {processed_info['length']} messages")

        # Get consumer group info
        try:
            group_info = await r.xinfo_groups("user-activity")
            for group in group_info:
                print(
                    f"  Consumer group '{group['name']}': {group['consumers']} consumers, {group['pending']} pending"
                )
        except Exception:
            print("  No consumer groups found")

    except Exception as e:
        print(f"Error monitoring streams: {e}")
    finally:
        await r.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "generate-data":
            print("🔄 Starting activity data generator...")
            asyncio.run(generate_test_data())

        elif command == "monitor":
            print("📊 Monitoring Redis streams...")
            asyncio.run(monitor_streams())

        elif command.startswith("worker"):
            # Allow multiple workers: python streams_consumer.py worker-1
            worker_id = command if len(command.split("-")) == 2 else None
            worker = RedisActivityProcessor(worker_id=worker_id)
            worker.run_sync()

        else:
            print(f"Unknown command: {command}")
            print("Usage: python streams_consumer.py [generate-data|monitor|worker-<id>]")
    else:
        # Run default worker
        print("🚀 Starting Redis Streams Consumer...")
        print("💡 Commands:")
        print("  python streams_consumer.py generate-data  # Generate test events")
        print("  python streams_consumer.py monitor        # Monitor stream status")
        print("  python streams_consumer.py worker-1       # Start worker with specific ID")

        worker = RedisActivityProcessor()
        worker.run_sync()
