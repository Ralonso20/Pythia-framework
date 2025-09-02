#!/usr/bin/env python3
"""
Basic Kafka Consumer/Producer Example with Pythia

This example demonstrates:
1. Creating a simple Kafka consumer worker
2. Processing JSON messages
3. Producing messages to another topic
4. Error handling and logging

Requirements:
- Docker with Kafka running (see docker-compose.yml)
- pip install pythia[kafka]

Usage:
- Start Kafka: docker-compose up -d
- Run consumer: python basic_consumer_producer.py
- In another terminal, produce test messages
"""

import asyncio
import json
from typing import Dict, Any
from pydantic import BaseModel

from pythia import Worker
from pythia.brokers.kafka import KafkaConsumer, KafkaProducer
from pythia.config.kafka import KafkaConfig


# Define message schema with Pydantic
class UserEvent(BaseModel):
    """User event message schema"""

    user_id: str
    event_type: str
    timestamp: str
    data: Dict[str, Any] = {}


class KafkaProcessorWorker(Worker):
    """
    Simple Kafka consumer that processes user events and forwards them
    """

    def __init__(self):
        # Configure Kafka consumer
        self.source = KafkaConsumer(
            topics=["user-events"],
            group_id="user-event-processor",
            config=KafkaConfig(
                bootstrap_servers="localhost:9092",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            ),
        )

        # Configure Kafka producer for output
        self.sink = KafkaProducer(
            topic="processed-events",
            config=KafkaConfig(
                bootstrap_servers="localhost:9092",
            ),
        )

        super().__init__()

    async def process(self, message) -> None:
        """Process each user event message"""
        try:
            # Parse message body as JSON
            event_data = json.loads(message.body)

            # Validate with Pydantic model
            user_event = UserEvent(**event_data)

            self.logger.info(
                f"Processing {user_event.event_type} for user {user_event.user_id}",
                extra={
                    "user_id": user_event.user_id,
                    "event_type": user_event.event_type,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )

            # Business logic - enrich the event
            processed_event = {
                "original": user_event.dict(),
                "processed_at": user_event.timestamp,
                "processor": "kafka-example-worker",
                "status": "processed",
            }

            # Send to output topic
            await self.sink.send(json.dumps(processed_event))

            self.logger.info(f"Successfully processed event for user {user_event.user_id}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in message: {e}")
            # Could send to dead letter queue here

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            # Could implement retry logic here
            raise  # Re-raise to trigger retry

    async def on_startup(self):
        """Called when worker starts"""
        self.logger.info("🚀 Kafka Processor Worker starting...")
        self.logger.info(f"📥 Consuming from: {self.source.topics}")
        self.logger.info(f"📤 Producing to: {self.sink.topic}")

    async def on_shutdown(self):
        """Called when worker stops"""
        self.logger.info("🛑 Kafka Processor Worker shutting down...")


# Example producer function for testing
async def produce_test_messages():
    """Produce some test messages for the worker to process"""
    producer = KafkaProducer(
        topic="user-events", config=KafkaConfig(bootstrap_servers="localhost:9092")
    )

    await producer.connect()

    test_events = [
        {
            "user_id": "user_001",
            "event_type": "login",
            "timestamp": "2025-08-31T10:00:00Z",
            "data": {"ip": "192.168.1.1", "device": "mobile"},
        },
        {
            "user_id": "user_002",
            "event_type": "purchase",
            "timestamp": "2025-08-31T10:01:00Z",
            "data": {"amount": 29.99, "product_id": "prod_123"},
        },
        {
            "user_id": "user_001",
            "event_type": "logout",
            "timestamp": "2025-08-31T10:05:00Z",
            "data": {"session_duration": 300},
        },
    ]

    for event in test_events:
        await producer.send(json.dumps(event))
        print(f"✅ Sent: {event['event_type']} for {event['user_id']}")
        await asyncio.sleep(1)  # Small delay between messages

    await producer.disconnect()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "produce":
        # Run producer to generate test messages
        print("🔄 Producing test messages...")
        asyncio.run(produce_test_messages())
        print("✅ Done producing test messages")
    else:
        # Run the worker
        print("🚀 Starting Kafka Consumer Worker...")
        print("💡 Tip: Run 'python basic_consumer_producer.py produce' to send test messages")
        worker = KafkaProcessorWorker()
        worker.run_sync()
