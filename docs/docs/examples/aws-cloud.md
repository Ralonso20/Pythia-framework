# AWS SQS/SNS Example

This example demonstrates how to build a distributed order processing system using AWS SQS and SNS with Pythia.

## Architecture

```
Order API → SNS Topic → SQS Queue → Order Processor
                    ↓
                Email Queue → Email Service
```

## Prerequisites

Install Pythia with AWS support:

```bash
pip install pythia[aws]
```

Set up AWS credentials and infrastructure:

```bash
# AWS credentials
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"

# SQS Queue URLs
export SQS_ORDERS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/123456789012/orders"
export SQS_EMAILS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/123456789012/emails"

# SNS Topic ARN
export SNS_ORDER_EVENTS_TOPIC="arn:aws:sns:us-east-1:123456789012:order-events"
```

## Order Processing Worker

```python
# order_processor.py
import asyncio
import json
from pythia import Worker
from pythia.brokers.cloud import SQSConsumer, SNSProducer
from pythia.models import Message

class OrderProcessor(Worker):
    """Processes incoming orders from SQS and publishes events to SNS."""

    source = SQSConsumer(
        queue_url="${SQS_ORDERS_QUEUE_URL}",
        max_messages=10,
        wait_time=20  # Long polling
    )

    def __init__(self):
        super().__init__()
        # SNS producer for order events
        self.event_publisher = SNSProducer(
            topic_arn="${SNS_ORDER_EVENTS_TOPIC}"
        )

    async def process(self, message: Message) -> dict:
        """Process an order and publish events."""
        order_data = message.body
        order_id = order_data.get("order_id")

        print(f"Processing order {order_id}")

        try:
            # Validate order
            if not self._validate_order(order_data):
                raise ValueError(f"Invalid order data: {order_id}")

            # Process payment
            payment_result = await self._process_payment(order_data)

            # Update inventory
            await self._update_inventory(order_data)

            # Publish order processed event
            await self.event_publisher.send(
                message=Message(
                    body={
                        "event_type": "order.processed",
                        "order_id": order_id,
                        "customer_id": order_data.get("customer_id"),
                        "total_amount": order_data.get("total_amount"),
                        "payment_id": payment_result["payment_id"]
                    }
                ),
                subject="Order Processed",
                message_attributes={
                    "event_type": {"DataType": "String", "StringValue": "order.processed"},
                    "order_id": {"DataType": "String", "StringValue": str(order_id)}
                }
            )

            return {
                "status": "processed",
                "order_id": order_id,
                "payment_id": payment_result["payment_id"]
            }

        except Exception as e:
            print(f"Error processing order {order_id}: {e}")

            # Publish order failed event
            await self.event_publisher.send(
                message=Message(
                    body={
                        "event_type": "order.failed",
                        "order_id": order_id,
                        "error": str(e)
                    }
                ),
                subject="Order Failed",
                message_attributes={
                    "event_type": {"DataType": "String", "StringValue": "order.failed"},
                    "order_id": {"DataType": "String", "StringValue": str(order_id)}
                }
            )

            # Re-raise to mark message as failed
            raise

    def _validate_order(self, order_data: dict) -> bool:
        """Validate order data."""
        required_fields = ["order_id", "customer_id", "items", "total_amount"]
        return all(field in order_data for field in required_fields)

    async def _process_payment(self, order_data: dict) -> dict:
        """Simulate payment processing."""
        await asyncio.sleep(0.1)  # Simulate API call
        return {
            "payment_id": f"pay_{order_data['order_id']}",
            "status": "completed"
        }

    async def _update_inventory(self, order_data: dict) -> None:
        """Update inventory levels."""
        await asyncio.sleep(0.05)  # Simulate database update
        print(f"Inventory updated for order {order_data['order_id']}")

# Run the worker
if __name__ == "__main__":
    worker = OrderProcessor()
    asyncio.run(worker.start())
```

## Email Notification Worker

```python
# email_service.py
import asyncio
from pythia import Worker
from pythia.brokers.cloud import SQSConsumer
from pythia.models import Message

class EmailService(Worker):
    """Sends email notifications based on order events."""

    source = SQSConsumer(
        queue_url="${SQS_EMAILS_QUEUE_URL}",
        max_messages=5,
        wait_time=10
    )

    async def process(self, message: Message) -> dict:
        """Send email notification."""
        event_data = message.body
        event_type = event_data.get("event_type")

        if event_type == "order.processed":
            return await self._send_order_confirmation(event_data)
        elif event_type == "order.failed":
            return await self._send_order_failure_notification(event_data)
        else:
            print(f"Unknown event type: {event_type}")
            return {"status": "skipped"}

    async def _send_order_confirmation(self, event_data: dict) -> dict:
        """Send order confirmation email."""
        order_id = event_data["order_id"]
        customer_id = event_data["customer_id"]

        # Simulate email sending
        await asyncio.sleep(0.2)

        print(f"✉️ Confirmation email sent for order {order_id} to customer {customer_id}")

        return {
            "status": "sent",
            "email_type": "order_confirmation",
            "order_id": order_id
        }

    async def _send_order_failure_notification(self, event_data: dict) -> dict:
        """Send order failure notification."""
        order_id = event_data["order_id"]
        error = event_data.get("error", "Unknown error")

        # Simulate email sending
        await asyncio.sleep(0.1)

        print(f"📧 Failure notification sent for order {order_id}: {error}")

        return {
            "status": "sent",
            "email_type": "order_failure",
            "order_id": order_id
        }

# Run the worker
if __name__ == "__main__":
    worker = EmailService()
    asyncio.run(worker.start())
```

## SNS to SQS Configuration

Configure SNS topic to fan out messages to multiple SQS queues:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:us-east-1:123456789012:emails",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:sns:us-east-1:123456789012:order-events"
        }
      }
    }
  ]
}
```

## Testing the System

Create a test script to send orders:

```python
# test_orders.py
import asyncio
import json
from pythia.brokers.cloud import SQSProducer
from pythia.models import Message

async def send_test_order():
    """Send a test order to the SQS queue."""
    producer = SQSProducer(queue_url="${SQS_ORDERS_QUEUE_URL}")

    test_order = {
        "order_id": "ORD-12345",
        "customer_id": "CUST-789",
        "items": [
            {"product_id": "PROD-1", "quantity": 2, "price": 29.99},
            {"product_id": "PROD-2", "quantity": 1, "price": 49.99}
        ],
        "total_amount": 109.97,
        "shipping_address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345"
        }
    }

    await producer.send(Message(body=test_order))
    print(f"Test order {test_order['order_id']} sent!")

if __name__ == "__main__":
    asyncio.run(send_test_order())
```

## Running the Example

1. **Start the workers:**

```bash
# Terminal 1: Order processor
python order_processor.py

# Terminal 2: Email service
python email_service.py
```

2. **Send test orders:**

```bash
# Terminal 3: Send test data
python test_orders.py
```

## Monitoring and Troubleshooting

### CloudWatch Metrics

Monitor your workers with AWS CloudWatch:

- **SQS Queue Metrics**: ApproximateNumberOfMessages, ApproximateNumberOfMessagesVisible
- **SNS Topic Metrics**: NumberOfMessagesPublished, NumberOfNotificationsFailed

### Error Handling

```python
from pythia.brokers.cloud import SQSConsumer
from botocore.exceptions import BotoCoreError, ClientError

class RobustOrderProcessor(Worker):
    source = SQSConsumer(
        queue_url="${SQS_ORDERS_QUEUE_URL}",
        max_retries=3,  # Built-in retry handling
        timeout=30
    )

    async def process(self, message: Message) -> dict:
        try:
            # Your processing logic
            return await self._process_order(message.body)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['QueueDoesNotExist', 'InvalidQueueUrl']:
                print(f"AWS configuration error: {e}")
                # Handle configuration issues
            raise
        except BotoCoreError as e:
            print(f"AWS service error: {e}")
            raise
```

## Best Practices

1. **Queue Configuration**: Use dead letter queues for failed messages
2. **Batch Processing**: Process multiple messages together when possible
3. **Monitoring**: Set up CloudWatch alarms for queue depth and processing errors
4. **Security**: Use IAM roles instead of access keys in production
5. **Cost Optimization**: Use long polling to reduce SQS requests

This example demonstrates a production-ready microservices architecture using AWS SQS/SNS with Pythia's cloud workers.
