#!/usr/bin/env python3
"""
Example Pythia Worker - Demonstrates the basic usage of the framework

This example creates a simple worker that:
1. Consumes messages from a Kafka topic
2. Processes approval events
3. Sends notifications via webhook

Usage:
    python example_worker.py
"""

import asyncio
from pydantic import BaseModel
from pythia import Worker, Message
from pythia.brokers.kafka import KafkaConsumer, KafkaProducer
from pythia.config import WorkerConfig


class ApprovalEvent(BaseModel):
    """Approval event data model"""

    id: str
    status: str
    user_id: str
    amount: float
    timestamp: str


class ApprovalWorker(Worker):
    """Example approval processing worker"""

    # Configure sources and sinks at class level
    source = KafkaConsumer(topics=["approvals"], group_id="approval-worker")

    sink = KafkaProducer(topic="notifications")

    async def process(self, message: Message) -> None:
        """Process approval messages"""

        try:
            # Parse the approval event
            if isinstance(message.body, dict):
                event = ApprovalEvent(**message.body)
            else:
                # Handle JSON string or bytes
                import json

                if isinstance(message.body, (str, bytes)):
                    event_data = json.loads(message.body)
                    if isinstance(event_data, dict):
                        event = ApprovalEvent(**event_data)
                    else:
                        raise ValueError(f"Expected dict from JSON, got {type(event_data)}")
                else:
                    raise ValueError(f"Unsupported message body type: {type(message.body)}")

            self.logger.info(
                "Processing approval event",
                event_id=event.id,
                status=event.status,
                user_id=event.user_id,
                amount=event.amount,
            )

            # Business logic: process the approval
            notification = await self.process_approval(event)

            # Send notification
            if self._sinks:
                await self._sinks[0].send(notification)

            self.logger.info(
                "Approval processed successfully",
                event_id=event.id,
                notification_sent=True,
            )

        except Exception as e:
            self.logger.error(
                "Failed to process approval",
                error=e,
                message_id=message.message_id,
            )
            raise

    async def process_approval(self, event: ApprovalEvent) -> dict:
        """Business logic for processing approvals"""

        # Simulate some processing time
        await asyncio.sleep(0.1)

        # Create notification based on approval status
        notification = {
            "type": "approval_notification",
            "event_id": event.id,
            "user_id": event.user_id,
            "status": event.status,
            "message": f"Your approval for ${event.amount} has been {event.status.lower()}",
            "timestamp": event.timestamp,
        }

        # Add status-specific logic
        if event.status.lower() == "approved":
            notification["priority"] = "normal"
            notification["channels"] = ",".join(["email", "app"])
        elif event.status.lower() == "rejected":
            notification["priority"] = "high"
            notification["channels"] = ",".join(["email", "sms", "app"])
            notification["reason"] = "Please contact support for more details"

        return notification

    async def startup(self) -> None:
        """Custom startup logic"""
        self.logger.info("Approval worker starting up")
        # Add any custom initialization here

    async def shutdown(self) -> None:
        """Custom shutdown logic"""
        self.logger.info("Approval worker shutting down")
        # Add any cleanup logic here

    async def health_check(self) -> bool:
        """Custom health check"""
        # Add any custom health checks here
        # For example, check external service availability
        return True

    async def handle_dead_letter(self, message: Message, error: Exception) -> None:
        """Handle messages that failed processing after all retries"""
        self.logger.error(
            "Message moved to dead letter queue",
            error=error,
            message_id=message.message_id,
            retry_count=message.retry_count,
        )

        # Could send to a dead letter topic or external system
        dead_letter_data = {
            "original_message": message.to_dict(),
            "error": str(error),
            "failed_at": message.timestamp.isoformat(),
        }

        # Example: send to dead letter topic
        try:
            if self._sinks:
                await self._sinks[0].send(dead_letter_data, topic="dead-letters")
        except Exception as e:
            self.logger.error(f"Failed to send to dead letter queue: {e}")


async def main():
    """Main entry point"""

    # Configuration can be provided or auto-detected from environment
    config = WorkerConfig(
        worker_name="approval-worker",
        log_level="INFO",
        max_retries=3,
        retry_delay=2.0,
    )

    # Create and run the worker
    worker = ApprovalWorker(config=config)

    print("🚀 Starting Approval Worker...")
    print("   Topics: approvals -> notifications")
    print("   Press Ctrl+C to stop")
    print()

    # Run the worker (this will block until shutdown)
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Worker stopped by user")
    except Exception as e:
        print(f"\n❌ Worker failed: {e}")
        exit(1)
