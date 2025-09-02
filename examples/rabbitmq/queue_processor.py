#!/usr/bin/env python3
"""
RabbitMQ Queue Processor Example with Pythia

This example demonstrates:
1. Creating a RabbitMQ consumer worker
2. Processing messages with acknowledgments
3. Dead letter queue handling
4. Error recovery and retry

Requirements:
- Docker with RabbitMQ running (see docker-compose.yml)
- pip install pythia[rabbitmq]

Usage:
- Start RabbitMQ: docker-compose up -d rabbitmq
- Run consumer: python queue_processor.py
- Send messages: python queue_processor.py send-messages
"""

import asyncio
import json
import random
from typing import Dict, Any
from pydantic import BaseModel
from datetime import datetime, timezone

from pythia import Worker
from pythia.brokers.rabbitmq import RabbitMQConsumer, RabbitMQProducer
from pythia.config.rabbitmq import RabbitMQConfig


# Message schemas
class OrderEvent(BaseModel):
    """Order processing event"""

    order_id: str
    customer_id: str
    amount: float
    items: list[Dict[str, Any]]
    timestamp: str
    priority: str = "normal"  # low, normal, high


class RabbitMQOrderProcessor(Worker):
    """
    RabbitMQ consumer that processes order events
    """

    def __init__(self):
        # Configure RabbitMQ consumer
        self.source = RabbitMQConsumer(
            queue="order-processing",
            exchange="orders",
            routing_key="order.*",
            exchange_type="topic",
            config=RabbitMQConfig(
                url="amqp://guest:guest@localhost:5672",
                prefetch_count=5,  # Process 5 messages concurrently
                auto_ack=False,  # Manual acknowledgment for reliability
            ),
        )

        # Configure producer for notifications
        self.notification_producer = RabbitMQProducer(
            exchange="notifications",
            routing_key="order.processed",
            config=RabbitMQConfig(url="amqp://guest:guest@localhost:5672"),
        )

        super().__init__()

    async def process(self, message) -> None:
        """Process each order event"""
        try:
            # Parse message - handle different message body types
            if isinstance(message.body, str):
                order_data = json.loads(message.body)
            elif isinstance(message.body, bytes):
                order_data = json.loads(message.body.decode("utf-8"))
            elif isinstance(message.body, dict):
                order_data = message.body
            else:
                raise ValueError(f"Unsupported message body type: {type(message.body)}")
            order = OrderEvent(**order_data)

            self.logger.info(
                f"Processing order {order.order_id} (${order.amount:.2f})",
                extra={
                    "order_id": order.order_id,
                    "customer_id": order.customer_id,
                    "amount": order.amount,
                    "priority": order.priority,
                },
            )

            # Simulate processing based on priority
            if order.priority == "high":
                processing_time = 0.5
            elif order.priority == "normal":
                processing_time = 1.0
            else:  # low priority
                processing_time = 2.0

            await asyncio.sleep(processing_time)

            # Simulate occasional failures for demo
            if random.random() < 0.1:  # 10% failure rate
                raise Exception(f"Payment processing failed for order {order.order_id}")

            # Process the order (business logic here)
            await self._process_order_logic(order)

            # Send notification
            notification = {
                "type": "order_processed",
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "status": "success",
            }

            await self.notification_producer.send(json.dumps(notification))

            # Acknowledge message via the consumer (RabbitMQ specific)
            # Note: In practice, you'd use self.source.acknowledge(message)
            # For this example, we'll use the raw message approach
            if (
                hasattr(message, "_raw_message")
                and message._raw_message is not None
                and hasattr(message._raw_message, "ack")
            ):
                message._raw_message.ack()

            self.logger.info(f"✅ Successfully processed order {order.order_id}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in message: {e}")
            # Reject message - send to DLQ
            if (
                hasattr(message, "_raw_message")
                and message._raw_message is not None
                and hasattr(message._raw_message, "nack")
            ):
                message._raw_message.nack(requeue=False)

        except Exception as e:
            self.logger.error(f"Error processing order: {e}")

            # Check retry count (RabbitMQ headers)
            retry_count = message.headers.get("x-retry-count", 0)

            if retry_count < 3:
                # Retry with delay
                self.logger.info(f"Retrying message (attempt {retry_count + 1})")
                # Retry the message
                if (
                    hasattr(message, "_raw_message")
                    and message._raw_message is not None
                    and hasattr(message._raw_message, "nack")
                ):
                    message._raw_message.nack(requeue=True)
            else:
                # Give up, send to dead letter queue
                self.logger.error("Max retries exceeded, sending to DLQ")
                # Give up - send to DLQ
                if (
                    hasattr(message, "_raw_message")
                    and message._raw_message is not None
                    and hasattr(message._raw_message, "nack")
                ):
                    message._raw_message.nack(requeue=False)

    async def _process_order_logic(self, order: OrderEvent):
        """Simulate actual order processing logic"""
        # Use the order data for processing
        self.logger.debug(f"Processing order {order.order_id} with {len(order.items)} items")

        # Validate inventory
        await asyncio.sleep(0.1)

        # Process payment
        await asyncio.sleep(0.2)

        # Update inventory
        await asyncio.sleep(0.1)

        # Generate invoice
        await asyncio.sleep(0.1)

    async def on_startup(self):
        """Worker startup"""
        self.logger.info("🚀 RabbitMQ Order Processor starting...")
        # Log queue name if available (RabbitMQ specific)
        queue_name = getattr(self.source, "queue_name", "unknown")
        self.logger.info(f"📥 Consuming from: {queue_name}")
        self.logger.info(f"📤 Notifications to: {self.notification_producer.exchange}")

    async def on_shutdown(self):
        """Worker shutdown"""
        self.logger.info("🛑 RabbitMQ Order Processor shutting down...")


# Message producer for testing
async def send_test_messages():
    """Send test order messages"""
    producer = RabbitMQProducer(
        exchange="orders",
        routing_key="order.new",
        config=RabbitMQConfig(url="amqp://guest:guest@localhost:5672"),
    )

    await producer.connect()

    test_orders = [
        {
            "order_id": f"ORD-{random.randint(1000, 9999)}",
            "customer_id": f"CUST-{random.randint(100, 999)}",
            "amount": round(random.uniform(10.0, 500.0), 2),
            "items": [
                {"product_id": "PROD-001", "quantity": 2, "price": 25.99},
                {"product_id": "PROD-002", "quantity": 1, "price": 15.50},
            ],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "priority": random.choice(["low", "normal", "high"]),
        }
        for _ in range(10)
    ]

    for order in test_orders:
        await producer.send(json.dumps(order))
        print(
            f"📦 Sent order {order['order_id']} (${order['amount']:.2f}, {order['priority']} priority)"
        )
        await asyncio.sleep(0.5)

    await producer.disconnect()
    print("✅ Done sending test orders")


# Setup RabbitMQ queues and exchanges
async def setup_rabbitmq():
    """Setup RabbitMQ exchanges and queues"""
    import aio_pika

    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672")
    channel = await connection.channel()

    # Create exchanges
    orders_exchange = await channel.declare_exchange(
        "orders", aio_pika.ExchangeType.TOPIC, durable=True
    )
    notifications_exchange = await channel.declare_exchange(
        "notifications", aio_pika.ExchangeType.TOPIC, durable=True
    )

    # Create queues
    order_queue = await channel.declare_queue("order-processing", durable=True)
    # Create DLQ for failed messages
    await channel.declare_queue("order-processing-dlq", durable=True)
    notification_queue = await channel.declare_queue("notifications", durable=True)

    # Bind queues
    await order_queue.bind(orders_exchange, "order.*")
    await notification_queue.bind(notifications_exchange, "order.*")

    await connection.close()
    print("🔧 RabbitMQ setup complete")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "send-messages":
            print("📤 Sending test messages...")
            asyncio.run(send_test_messages())

        elif command == "setup":
            print("🔧 Setting up RabbitMQ...")
            asyncio.run(setup_rabbitmq())

        else:
            print(f"Unknown command: {command}")
            print("Usage: python queue_processor.py [send-messages|setup]")

    else:
        # Run the worker
        print("🚀 Starting RabbitMQ Order Processor...")
        print("💡 Tip: Run 'python queue_processor.py send-messages' to send test orders")
        print("💡 Run 'python queue_processor.py setup' to setup exchanges/queues")

        worker = RabbitMQOrderProcessor()
        worker.run_sync()
