# Cloud Message Queues

Pythia's **Cloud Workers** provide complete support for the main cloud messaging platforms: **AWS**, **Google Cloud**, and **Azure**. Implemented following the anti-over-engineering principle, they offer consistent APIs and simple configuration.

## 🌟 Key Features

- **📦 Modular Installation**: Install only the providers you need
- **🔄 Unified API**: Same interface for all cloud providers
- **⚡ Simple Configuration**: Standard environment variables
- **🛡️ Error Handling**: Platform-specific error management
- **🔧 Lazy Loading**: No unnecessary dependencies

## 🚀 Installation

```bash
# AWS only
uv add pythia[aws]

# Google Cloud only
uv add pythia[gcp]

# Azure only
uv add pythia[azure]

# All cloud providers
uv add pythia[cloud]
```

---

# ☁️ Amazon Web Services (AWS)

Complete support for **Amazon SQS** (Simple Queue Service) and **Amazon SNS** (Simple Notification Service).

## 📥 SQS Consumer

### Basic Example

```python
from pythia.brokers.cloud.aws import SQSConsumer
from pythia.config.cloud import AWSConfig

class OrderProcessor(SQSConsumer):
    def __init__(self):
        aws_config = AWSConfig(
            region="us-east-1",
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/orders"
        )
        super().__init__(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/orders",
            aws_config=aws_config
        )

    async def process_message(self, message):
        """Process SQS message"""
        order_data = message.body
        order_id = order_data.get('order_id')

        # Your business logic
        print(f"Processing order: {order_id}")
        await self.process_order(order_data)

        return {"processed": True, "order_id": order_id}

    async def process_order(self, order_data):
        # Simulate processing
        await asyncio.sleep(0.1)
        print(f"Order {order_data['order_id']} completed")

# Execute
if __name__ == "__main__":
    OrderProcessor().run_sync()
```

### Advanced Configuration

```python
from pythia.config.cloud import AWSConfig

class AdvancedSQSProcessor(SQSConsumer):
    def __init__(self):
        aws_config = AWSConfig(
            region="us-west-2",
            access_key_id="AKIA...",  # Or use environment variables
            secret_access_key="...",
            endpoint_url="http://localhost:4566",  # For LocalStack
            queue_url="https://sqs.us-west-2.amazonaws.com/123/priority-orders",
            visibility_timeout=60,  # 1 minute
            wait_time_seconds=20,   # Long polling
            max_messages=5          # Batch size
        )

        super().__init__(
            queue_url=aws_config.queue_url,
            aws_config=aws_config
        )

    async def process_message(self, message):
        # Access to SQS metadata
        message_id = message.headers.get('MessageId')
        receipt_handle = message.headers.get('ReceiptHandle')
        delivery_count = message.headers.get('MD5OfBody')

        # Custom attributes
        priority = message.headers.get('attr_priority', 'normal')
        source = message.headers.get('attr_source', 'unknown')

        self.logger.info(
            f"Processing SQS message {message_id}",
            priority=priority,
            source=source
        )

        return {
            "message_id": message_id,
            "priority": priority,
            "processed": True
        }
```

## 📤 SNS Producer

### Basic Example

```python
from pythia.brokers.cloud.aws import SNSProducer

class NotificationSender(SNSProducer):
    def __init__(self):
        super().__init__(
            topic_arn="arn:aws:sns:us-east-1:123456789:user-notifications"
        )

    async def send_welcome_notification(self, user_data):
        """Send welcome notification"""
        message_data = {
            "event": "user_welcome",
            "user_id": user_data["id"],
            "email": user_data["email"],
            "timestamp": datetime.now().isoformat()
        }

        message_id = await self.publish_message(
            message=message_data,
            subject="Welcome to our platform",
            message_attributes={
                "event_type": "welcome",
                "priority": "high",
                "user_segment": user_data.get("segment", "standard")
            }
        )

        if message_id:
            self.logger.info(f"Welcome notification sent: {message_id}")
        return message_id

    async def send_from_pythia_message(self, pythia_message):
        """Send from Pythia message"""
        return await self.publish_from_pythia_message(
            message=pythia_message,
            subject="System Alert"
        )
```

### AWS Environment Variables

```bash
# AWS Credentials
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_ENDPOINT_URL=http://localhost:4566  # For LocalStack

# SQS
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123/queue-name

# SNS
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123:topic-name
```

---

# 🏗️ Google Cloud Platform (GCP)

Complete support for **Google Cloud Pub/Sub** with subscriber and publisher.

## 📥 Pub/Sub Subscriber

### Basic Example

```python
from pythia.brokers.cloud.gcp import PubSubSubscriber
from pythia.config.cloud import GCPConfig

class EventProcessor(PubSubSubscriber):
    def __init__(self):
        gcp_config = GCPConfig(
            project_id="my-gcp-project",
            subscription_name="event-processor-sub"
        )
        super().__init__(
            subscription_path="projects/my-gcp-project/subscriptions/event-processor-sub",
            gcp_config=gcp_config
        )

    async def process_message(self, message):
        """Process Pub/Sub message"""
        event_data = message.body
        event_type = event_data.get('type', 'unknown')

        # Message metadata
        message_id = message.headers.get('message_id')
        publish_time = message.headers.get('publish_time')
        ordering_key = message.headers.get('ordering_key')

        self.logger.info(
            f"Processing Pub/Sub event: {event_type}",
            message_id=message_id,
            ordering_key=ordering_key
        )

        # Routing by event type
        if event_type == 'user_signup':
            await self.handle_user_signup(event_data)
        elif event_type == 'order_placed':
            await self.handle_order_placed(event_data)
        else:
            self.logger.warning(f"Unknown event type: {event_type}")

        return {
            "event_type": event_type,
            "processed": True,
            "message_id": message_id
        }

    async def handle_user_signup(self, event_data):
        # Specific logic for signup
        user_id = event_data.get('user_id')
        print(f"New user signup: {user_id}")

    async def handle_order_placed(self, event_data):
        # Specific logic for orders
        order_id = event_data.get('order_id')
        print(f"New order placed: {order_id}")
```

### Automatic Project ID Extraction

```python
# The project_id is automatically extracted from the path
subscriber = PubSubSubscriber(
    subscription_path="projects/my-project/subscriptions/my-sub"
    # project_id is automatically detected as "my-project"
)
```

## 📤 Pub/Sub Publisher

### Basic Example

```python
from pythia.brokers.cloud.gcp import PubSubPublisher

class EventPublisher(PubSubPublisher):
    def __init__(self):
        super().__init__(
            topic_path="projects/my-gcp-project/topics/user-events"
        )

    async def publish_user_event(self, user_id, event_type, event_data):
        """Publish user event with ordering"""
        message_data = {
            "user_id": user_id,
            "event_type": event_type,
            "data": event_data,
            "timestamp": datetime.now().isoformat()
        }

        message_id = await self.publish_message(
            message=message_data,
            attributes={
                "event_type": event_type,
                "user_segment": event_data.get("segment", "standard"),
                "source": "user-service",
                "version": "1.0"
            },
            ordering_key=f"user-{user_id}"  # Guarantees order per user
        )

        if message_id:
            self.logger.info(f"User event published: {message_id}")
        return message_id

    async def broadcast_system_alert(self, alert_data):
        """Broadcast without ordering key"""
        return await self.publish_message(
            message={
                "alert_type": alert_data["type"],
                "severity": alert_data["severity"],
                "message": alert_data["message"],
                "timestamp": datetime.now().isoformat()
            },
            attributes={
                "alert_type": alert_data["type"],
                "severity": alert_data["severity"]
            }
            # No ordering_key for broadcast
        )
```

### GCP Environment Variables

```bash
# Authentication
GCP_PROJECT_ID=my-gcp-project
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Pub/Sub
PUBSUB_SUBSCRIPTION=projects/my-project/subscriptions/my-subscription
PUBSUB_TOPIC=projects/my-project/topics/my-topic
```

---

# 🔷 Microsoft Azure

Complete support for **Azure Service Bus** and **Azure Storage Queues**.

## 📥 Service Bus Consumer

### Basic Example

```python
from pythia.brokers.cloud.azure import ServiceBusConsumer
from pythia.config.cloud import AzureConfig

class TaskProcessor(ServiceBusConsumer):
    def __init__(self):
        azure_config = AzureConfig(
            service_bus_connection_string="Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=...",
            service_bus_queue_name="task-queue"
        )
        super().__init__(
            queue_name="task-queue",
            azure_config=azure_config
        )

    async def process_message(self, message):
        """Process Service Bus message"""
        task_data = message.body

        # Message metadata
        message_id = message.headers.get('message_id')
        correlation_id = message.headers.get('correlation_id')
        content_type = message.headers.get('content_type')
        delivery_count = message.headers.get('delivery_count')

        # Custom properties
        priority = message.headers.get('prop_priority', 'normal')
        task_type = message.headers.get('prop_task_type', 'generic')

        self.logger.info(
            f"Processing Service Bus task: {task_type}",
            message_id=message_id,
            correlation_id=correlation_id,
            priority=priority,
            delivery_count=delivery_count
        )

        # Type-based processing
        if task_type == 'image_processing':
            await self.process_image_task(task_data)
        elif task_type == 'email_send':
            await self.process_email_task(task_data)
        else:
            await self.process_generic_task(task_data)

        return {
            "task_type": task_type,
            "processed": True,
            "correlation_id": correlation_id
        }

    async def process_image_task(self, task_data):
        # Specific logic for image processing
        image_id = task_data.get('image_id')
        print(f"Processing image: {image_id}")

    async def process_email_task(self, task_data):
        # Specific logic for email sending
        email_id = task_data.get('email_id')
        print(f"Sending email: {email_id}")

    async def process_generic_task(self, task_data):
        # Generic logic
        print(f"Processing generic task: {task_data}")
```

## 📤 Service Bus Producer

```python
from pythia.brokers.cloud.azure import ServiceBusProducer

class TaskScheduler(ServiceBusProducer):
    def __init__(self):
        super().__init__(
            queue_name="task-queue",
            connection_string="Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=..."
        )

    async def schedule_image_processing(self, image_data):
        """Schedule image processing"""
        task_data = {
            "task_type": "image_processing",
            "image_id": image_data["id"],
            "image_url": image_data["url"],
            "processing_options": image_data.get("options", {})
        }

        success = await self.send_message(
            message=task_data,
            properties={
                "task_type": "image_processing",
                "priority": "high",
                "source": "upload-service"
            },
            correlation_id=f"img-{image_data['id']}",
            content_type="application/json"
        )

        if success:
            self.logger.info(f"Image processing task scheduled: {image_data['id']}")
        return success
```

## 📥 Storage Queue Consumer

### Basic Example

```python
from pythia.brokers.cloud.azure import StorageQueueConsumer

class SimpleTaskProcessor(StorageQueueConsumer):
    def __init__(self):
        super().__init__(
            queue_name="simple-tasks",
            connection_string="DefaultEndpointsProtocol=https;AccountName=mystorage;AccountKey=...;EndpointSuffix=core.windows.net"
        )

    async def process_message(self, message):
        """Process Storage Queue message"""
        task_data = message.body

        # Message metadata
        message_id = message.headers.get('message_id')
        dequeue_count = message.headers.get('dequeue_count')
        insertion_time = message.headers.get('insertion_time')

        self.logger.info(
            f"Processing Storage Queue task",
            message_id=message_id,
            dequeue_count=dequeue_count,
            insertion_time=insertion_time
        )

        # Process simple task
        task_type = task_data.get('type', 'unknown')
        if task_type == 'cleanup':
            await self.perform_cleanup(task_data)
        elif task_type == 'backup':
            await self.perform_backup(task_data)

        return {
            "task_type": task_type,
            "processed": True,
            "message_id": message_id
        }

    async def perform_cleanup(self, task_data):
        # Cleanup logic
        print(f"Performing cleanup: {task_data}")

    async def perform_backup(self, task_data):
        # Backup logic
        print(f"Performing backup: {task_data}")
```

## 📤 Storage Queue Producer

```python
from pythia.brokers.cloud.azure import StorageQueueProducer

class MaintenanceScheduler(StorageQueueProducer):
    def __init__(self):
        super().__init__(
            queue_name="maintenance-tasks",
            connection_string="DefaultEndpointsProtocol=https;AccountName=mystorage;AccountKey=...;EndpointSuffix=core.windows.net"
        )

    async def schedule_cleanup_task(self, cleanup_data):
        """Schedule cleanup task"""
        task_data = {
            "type": "cleanup",
            "target": cleanup_data["target"],
            "options": cleanup_data.get("options", {}),
            "scheduled_time": datetime.now().isoformat()
        }

        message_id = await self.send_message(
            message=task_data,
            visibility_timeout=300,  # 5 minutes invisible initially
            time_to_live=86400      # TTL of 24 hours
        )

        if message_id:
            self.logger.info(f"Cleanup task scheduled: {message_id}")
        return message_id

    async def schedule_backup_task(self, backup_data):
        """Schedule backup task"""
        return await self.send_message(
            message={
                "type": "backup",
                "database": backup_data["database"],
                "backup_type": backup_data.get("type", "full")
            },
            time_to_live=172800  # TTL of 48 hours for backups
        )
```

### Azure Environment Variables

```bash
# Service Bus
AZURE_SERVICE_BUS_CONNECTION_STRING=Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=...
SERVICE_BUS_QUEUE_NAME=task-queue

# Storage Queue
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=mystorage;AccountKey=...;EndpointSuffix=core.windows.net
STORAGE_QUEUE_NAME=simple-tasks
```

---

# 🔧 Advanced Configuration

## Programmatic Configuration

```python
from pythia.config.cloud import AWSConfig, GCPConfig, AzureConfig, CloudProviderConfig

# Unified configuration
cloud_config = CloudProviderConfig.from_env()

# Provider-specific configuration
aws_config = AWSConfig(
    region="us-east-1",
    queue_url="https://sqs.us-east-1.amazonaws.com/123/queue",
    visibility_timeout=60,
    max_messages=10
)

gcp_config = GCPConfig(
    project_id="my-project",
    subscription_name="my-subscription",
    max_messages=100,
    ack_deadline_seconds=600
)

azure_config = AzureConfig(
    service_bus_connection_string="Endpoint=sb://...",
    service_bus_queue_name="my-queue",
    max_messages=32,
    visibility_timeout=30
)
```

## Multi-Cloud Worker Pattern

```python
from pythia.brokers.cloud.aws import SQSConsumer
from pythia.brokers.cloud.gcp import PubSubSubscriber
from pythia.brokers.cloud.azure import ServiceBusConsumer

class MultiCloudProcessor:
    def __init__(self):
        # Configure multiple consumers
        self.aws_consumer = SQSConsumer(
            queue_url="https://sqs.us-east-1.amazonaws.com/123/aws-tasks"
        )
        self.gcp_subscriber = PubSubSubscriber(
            subscription_path="projects/my-project/subscriptions/gcp-tasks"
        )
        self.azure_consumer = ServiceBusConsumer(
            queue_name="azure-tasks",
            connection_string="Endpoint=sb://..."
        )

    async def run_all(self):
        """Run all consumers concurrently"""
        await asyncio.gather(
            self.aws_consumer.run(),
            self.gcp_subscriber.run(),
            self.azure_consumer.run()
        )
```

---

# 🛡️ Error Handling and Retries

## AWS SQS - Visibility Timeout

```python
class ResilientSQSProcessor(SQSConsumer):
    async def process_message(self, message):
        try:
            # Your logic here
            await self.complex_processing(message.body)
            return {"processed": True}
        except TemporaryError as e:
            # Don't return result -> message will be visible again
            self.logger.warning(f"Temporary error, will retry: {e}")
            return None  # This causes the message to be retried
        except PermanentError as e:
            # Return False to avoid infinite retries
            self.logger.error(f"Permanent error: {e}")
            await self.send_to_dlq(message)
            return {"processed": False, "error": str(e)}
```

## GCP Pub/Sub - Acknowledgment Control

```python
class ResilientPubSubProcessor(PubSubSubscriber):
    async def process_message(self, message):
        try:
            await self.process_event(message.body)
            return {"processed": True}  # Message will be acknowledged
        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            # Don't return anything -> message will NOT be acknowledged
            # Will be retried automatically
```

## Azure Service Bus - Complete vs Abandon

```python
class ResilientServiceBusProcessor(ServiceBusConsumer):
    async def process_message(self, message):
        try:
            result = await self.process_task(message.body)
            return result  # Complete message
        except RetriableError as e:
            # The worker automatically does abandon_message
            # when there's an exception
            raise  # This causes abandon and retry
        except NonRetriableError as e:
            # Return result to complete and avoid retries
            await self.handle_poison_message(message)
            return {"processed": False, "error": "poison_message"}
```

---

# 📊 Monitoring and Observability

## Structured Logging

```python
class MonitoredCloudProcessor(SQSConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics = {
            "messages_processed": 0,
            "errors_count": 0,
            "processing_times": []
        }

    async def process_message(self, message):
        start_time = time.time()

        try:
            # Your processing
            result = await self.handle_message(message.body)

            # Success metrics
            processing_time = time.time() - start_time
            self.metrics["messages_processed"] += 1
            self.metrics["processing_times"].append(processing_time)

            self.logger.info(
                "Message processed successfully",
                message_id=message.headers.get('MessageId'),
                processing_time_ms=processing_time * 1000,
                queue_name=self.queue_name,
                metrics=self.metrics
            )

            return result

        except Exception as e:
            self.metrics["errors_count"] += 1
            self.logger.error(
                "Message processing failed",
                message_id=message.headers.get('MessageId'),
                error=str(e),
                error_type=type(e).__name__,
                queue_name=self.queue_name,
                metrics=self.metrics,
                exc_info=True
            )
            raise
```

---

# 🎯 Common Use Cases

## 1. **Image Processing Pipeline**

```python
# AWS SQS to receive tasks
class ImageProcessor(SQSConsumer):
    async def process_message(self, message):
        image_data = message.body

        # Process image
        processed_image = await self.process_image(image_data)

        # Notify completion via SNS
        notification_sender = SNSProducer(topic_arn="arn:aws:sns:...")
        await notification_sender.publish_message({
            "event": "image_processed",
            "image_id": image_data["id"],
            "result_url": processed_image["url"]
        })

        return {"processed": True}
```

## 2. **Multi-Channel Notification System**

```python
# GCP Pub/Sub for user events
class NotificationDispatcher(PubSubSubscriber):
    async def process_message(self, message):
        event = message.body
        user_id = event["user_id"]

        # Send to Azure Service Bus for processing
        service_bus_producer = ServiceBusProducer(queue_name="notifications")
        await service_bus_producer.send_message(
            message={
                "user_id": user_id,
                "notification_type": event["type"],
                "channels": ["email", "push", "sms"]
            },
            correlation_id=f"user-{user_id}"
        )

        return {"dispatched": True}
```

## 3. **Backup and Maintenance System**

```python
# Azure Storage Queue for maintenance tasks
class MaintenanceWorker(StorageQueueConsumer):
    async def process_message(self, message):
        task = message.body

        if task["type"] == "backup":
            await self.perform_backup(task["database"])
        elif task["type"] == "cleanup":
            await self.cleanup_old_files(task["path"])
        elif task["type"] == "health_check":
            health_status = await self.check_system_health()

            # Report to AWS SNS if there are issues
            if not health_status["healthy"]:
                sns_producer = SNSProducer(topic_arn="arn:aws:sns:...")
                await sns_producer.publish_message(
                    message=health_status,
                    subject="System Health Alert"
                )

        return {"completed": True}
```

---

Pythia's **Cloud Workers** provide a unified and powerful experience for working with the main cloud messaging platforms, maintaining the simplicity and elegance that characterizes the framework.

Start building your cloud-native workers today! 🚀
