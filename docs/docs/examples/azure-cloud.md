# Azure Service Bus & Storage Queues Example

This example demonstrates building a comprehensive document processing pipeline using both Azure Service Bus for critical operations and Azure Storage Queues for background tasks.

## Architecture

```
Document Upload → Service Bus Queue → Document Processor
                                   ↓
                            Storage Queue → Thumbnail Generator
                                   ↓
                            Storage Queue → OCR Processor
                                   ↓
                            Service Bus → Notification Service
```

## Prerequisites

Install Pythia with Azure support:

```bash
pip install pythia[azure]
```

Set up Azure resources and credentials:

```bash
# Azure Service Bus (for critical operations)
export AZURE_SERVICE_BUS_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key"
export SERVICE_BUS_DOCUMENTS_QUEUE="document-processing"
export SERVICE_BUS_NOTIFICATIONS_QUEUE="notifications"

# Azure Storage (for background tasks)
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=yourstorageaccount;AccountKey=your-key;EndpointSuffix=core.windows.net"
export STORAGE_THUMBNAILS_QUEUE="thumbnails"
export STORAGE_OCR_QUEUE="ocr-processing"
```

## Document Processing Worker (Service Bus)

```python
# document_processor.py
import asyncio
import json
import os
from datetime import datetime
from pythia import Worker
from pythia.brokers.cloud import ServiceBusConsumer, StorageQueueProducer, ServiceBusProducer
from pythia.models import Message

class DocumentProcessor(Worker):
    """Main document processor using Azure Service Bus for reliable processing."""

    source = ServiceBusConsumer(
        queue_name="${SERVICE_BUS_DOCUMENTS_QUEUE}",
        max_messages=5,
        max_wait_time=30  # 30 seconds max wait
    )

    def __init__(self):
        super().__init__()
        # Storage Queue producers for background tasks
        self.thumbnail_producer = StorageQueueProducer(
            queue_name="${STORAGE_THUMBNAILS_QUEUE}",
            auto_create_queue=True
        )
        self.ocr_producer = StorageQueueProducer(
            queue_name="${STORAGE_OCR_QUEUE}",
            auto_create_queue=True
        )
        # Service Bus producer for notifications
        self.notification_producer = ServiceBusProducer(
            queue_name="${SERVICE_BUS_NOTIFICATIONS_QUEUE}"
        )

    async def process(self, message: Message) -> dict:
        """Process uploaded document and trigger background tasks."""
        doc_data = message.body
        document_id = doc_data.get("document_id")
        file_path = doc_data.get("file_path")
        file_type = doc_data.get("file_type", "").lower()
        user_id = doc_data.get("user_id")

        print(f"🔄 Processing document {document_id} ({file_type})")

        try:
            # Validate document
            if not await self._validate_document(file_path, file_type):
                raise ValueError(f"Invalid document: {document_id}")

            # Save document metadata to database
            metadata = await self._save_document_metadata(doc_data)

            # Trigger background tasks based on file type
            background_tasks = []

            # Always generate thumbnail for supported formats
            if file_type in ['pdf', 'png', 'jpg', 'jpeg', 'gif', 'bmp']:
                thumbnail_task = {
                    "task_type": "thumbnail",
                    "document_id": document_id,
                    "file_path": file_path,
                    "file_type": file_type,
                    "user_id": user_id,
                    "priority": "normal"
                }
                await self.thumbnail_producer.send(
                    Message(body=thumbnail_task),
                    message_ttl=3600  # 1 hour TTL
                )
                background_tasks.append("thumbnail")

            # OCR processing for documents and images
            if file_type in ['pdf', 'png', 'jpg', 'jpeg', 'tiff']:
                ocr_task = {
                    "task_type": "ocr",
                    "document_id": document_id,
                    "file_path": file_path,
                    "file_type": file_type,
                    "user_id": user_id,
                    "language": doc_data.get("language", "en"),
                    "priority": "high" if file_type == "pdf" else "normal"
                }
                await self.ocr_producer.send(
                    Message(body=ocr_task),
                    message_ttl=7200  # 2 hours TTL
                )
                background_tasks.append("ocr")

            # Send processing complete notification
            await self.notification_producer.send(
                message=Message(body={
                    "notification_type": "document_processed",
                    "document_id": document_id,
                    "user_id": user_id,
                    "file_type": file_type,
                    "background_tasks": background_tasks,
                    "processed_at": datetime.now().isoformat()
                }),
                # Service Bus message properties
                correlation_id=f"doc_{document_id}",
                subject="Document Processing Complete"
            )

            print(f"✅ Document {document_id} processed successfully")

            return {
                "status": "processed",
                "document_id": document_id,
                "metadata": metadata,
                "background_tasks": background_tasks
            }

        except Exception as e:
            print(f"❌ Error processing document {document_id}: {e}")

            # Send error notification
            await self.notification_producer.send(
                message=Message(body={
                    "notification_type": "document_error",
                    "document_id": document_id,
                    "user_id": user_id,
                    "error": str(e),
                    "failed_at": datetime.now().isoformat()
                }),
                correlation_id=f"doc_{document_id}_error"
            )

            # Re-raise to mark message as failed (will go to dead letter queue)
            raise

    async def _validate_document(self, file_path: str, file_type: str) -> bool:
        """Validate document exists and is accessible."""
        if not file_path or not file_type:
            return False

        # Simulate file validation
        await asyncio.sleep(0.1)

        # Check file extension
        allowed_types = ['pdf', 'doc', 'docx', 'txt', 'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff']
        return file_type in allowed_types

    async def _save_document_metadata(self, doc_data: dict) -> dict:
        """Save document metadata to database."""
        await asyncio.sleep(0.2)  # Simulate database operation

        metadata = {
            "document_id": doc_data["document_id"],
            "original_filename": doc_data.get("filename"),
            "file_size": doc_data.get("file_size", 0),
            "mime_type": doc_data.get("mime_type"),
            "uploaded_by": doc_data.get("user_id"),
            "uploaded_at": datetime.now().isoformat(),
            "status": "processing"
        }

        print(f"💾 Saved metadata for document {doc_data['document_id']}")
        return metadata

# Run the document processor
if __name__ == "__main__":
    worker = DocumentProcessor()
    asyncio.run(worker.start())
```

## Thumbnail Generator (Storage Queue)

```python
# thumbnail_generator.py
import asyncio
import json
from pythia import Worker
from pythia.brokers.cloud import StorageQueueConsumer
from pythia.models import Message

class ThumbnailGenerator(Worker):
    """Generates thumbnails for documents using Azure Storage Queues."""

    source = StorageQueueConsumer(
        queue_name="${STORAGE_THUMBNAILS_QUEUE}",
        visibility_timeout=300,  # 5 minutes to process
        max_messages=10
    )

    async def process(self, message: Message) -> dict:
        """Generate thumbnail for document."""
        task_data = message.body
        document_id = task_data.get("document_id")
        file_path = task_data.get("file_path")
        file_type = task_data.get("file_type")

        print(f"🖼️  Generating thumbnail for document {document_id}")

        try:
            # Generate thumbnail based on file type
            if file_type == "pdf":
                thumbnail_path = await self._generate_pdf_thumbnail(file_path, document_id)
            elif file_type in ['png', 'jpg', 'jpeg', 'gif', 'bmp']:
                thumbnail_path = await self._generate_image_thumbnail(file_path, document_id)
            else:
                return {"status": "skipped", "reason": f"Unsupported file type: {file_type}"}

            # Save thumbnail metadata
            await self._save_thumbnail_metadata(document_id, thumbnail_path)

            print(f"✅ Thumbnail generated: {thumbnail_path}")

            return {
                "status": "completed",
                "document_id": document_id,
                "thumbnail_path": thumbnail_path,
                "file_type": file_type
            }

        except Exception as e:
            print(f"❌ Failed to generate thumbnail for {document_id}: {e}")

            # For background tasks, we might want to retry a few times
            retry_count = task_data.get("retry_count", 0)
            if retry_count < 3:
                print(f"🔄 Will retry thumbnail generation (attempt {retry_count + 1})")
                # In a real implementation, you'd re-queue with retry_count + 1
                raise  # This will make the message visible again after visibility_timeout
            else:
                print(f"💀 Max retries reached for document {document_id}")
                return {"status": "failed", "max_retries_reached": True}

    async def _generate_pdf_thumbnail(self, file_path: str, document_id: str) -> str:
        """Generate thumbnail from PDF first page."""
        await asyncio.sleep(2.0)  # Simulate PDF processing

        thumbnail_path = f"/thumbnails/{document_id}_thumb.jpg"
        print(f"📄 Generated PDF thumbnail: {thumbnail_path}")
        return thumbnail_path

    async def _generate_image_thumbnail(self, file_path: str, document_id: str) -> str:
        """Generate thumbnail from image file."""
        await asyncio.sleep(0.5)  # Simulate image processing

        thumbnail_path = f"/thumbnails/{document_id}_thumb.jpg"
        print(f"🖼️  Generated image thumbnail: {thumbnail_path}")
        return thumbnail_path

    async def _save_thumbnail_metadata(self, document_id: str, thumbnail_path: str):
        """Save thumbnail metadata to database."""
        await asyncio.sleep(0.1)  # Simulate database update
        print(f"💾 Updated document {document_id} with thumbnail path")

# Run the thumbnail generator
if __name__ == "__main__":
    worker = ThumbnailGenerator()
    asyncio.run(worker.start())
```

## OCR Processor (Storage Queue)

```python
# ocr_processor.py
import asyncio
import random
from pythia import Worker
from pythia.brokers.cloud import StorageQueueConsumer
from pythia.models import Message

class OCRProcessor(Worker):
    """Performs OCR on documents using Azure Storage Queues."""

    source = StorageQueueConsumer(
        queue_name="${STORAGE_OCR_QUEUE}",
        visibility_timeout=600,  # 10 minutes for OCR processing
        max_messages=3  # Limit concurrent OCR jobs
    )

    async def process(self, message: Message) -> dict:
        """Perform OCR on document."""
        task_data = message.body
        document_id = task_data.get("document_id")
        file_path = task_data.get("file_path")
        file_type = task_data.get("file_type")
        language = task_data.get("language", "en")

        print(f"🔍 Starting OCR for document {document_id} ({file_type}, {language})")

        try:
            # Perform OCR based on file type
            if file_type == "pdf":
                text_content = await self._ocr_pdf(file_path, language)
            elif file_type in ['png', 'jpg', 'jpeg', 'tiff']:
                text_content = await self._ocr_image(file_path, language)
            else:
                return {"status": "skipped", "reason": f"OCR not supported for {file_type}"}

            # Save extracted text
            await self._save_extracted_text(document_id, text_content, language)

            # Calculate confidence score and word count
            word_count = len(text_content.split()) if text_content else 0
            confidence = random.uniform(0.85, 0.98)  # Simulate OCR confidence

            print(f"✅ OCR completed: {word_count} words extracted with {confidence:.2%} confidence")

            return {
                "status": "completed",
                "document_id": document_id,
                "word_count": word_count,
                "confidence": confidence,
                "language": language,
                "text_length": len(text_content)
            }

        except Exception as e:
            print(f"❌ OCR failed for document {document_id}: {e}")

            # OCR can be expensive, so we're more conservative with retries
            retry_count = task_data.get("retry_count", 0)
            if retry_count < 2:  # Only 2 retries for OCR
                print(f"🔄 Will retry OCR (attempt {retry_count + 1})")
                raise
            else:
                print(f"💀 Max OCR retries reached for document {document_id}")
                return {"status": "failed", "max_retries_reached": True}

    async def _ocr_pdf(self, file_path: str, language: str) -> str:
        """Perform OCR on PDF document."""
        # Simulate complex PDF OCR processing
        await asyncio.sleep(5.0)

        sample_text = f"""
        Sample extracted text from PDF document.
        Language: {language}

        This is a multi-page document with various formatting.
        The OCR process has successfully extracted text from
        all pages including tables, headers, and footers.

        Processing timestamp: {asyncio.get_event_loop().time()}
        """

        return sample_text.strip()

    async def _ocr_image(self, file_path: str, language: str) -> str:
        """Perform OCR on image file."""
        # Simulate image OCR processing
        await asyncio.sleep(2.0)

        sample_text = f"Text extracted from image file in {language} language. Quality: High"
        return sample_text

    async def _save_extracted_text(self, document_id: str, text_content: str, language: str):
        """Save extracted text to database."""
        await asyncio.sleep(0.3)  # Simulate database operation
        print(f"💾 Saved {len(text_content)} characters of extracted text for document {document_id}")

# Run the OCR processor
if __name__ == "__main__":
    worker = OCRProcessor()
    asyncio.run(worker.start())
```

## Notification Service (Service Bus)

```python
# notification_service.py
import asyncio
from datetime import datetime
from pythia import Worker
from pythia.brokers.cloud import ServiceBusConsumer
from pythia.models import Message

class NotificationService(Worker):
    """Handles notifications using Azure Service Bus for reliability."""

    source = ServiceBusConsumer(
        queue_name="${SERVICE_BUS_NOTIFICATIONS_QUEUE}",
        max_messages=20,
        max_wait_time=10
    )

    async def process(self, message: Message) -> dict:
        """Send notification to user."""
        notification_data = message.body
        notification_type = notification_data.get("notification_type")
        user_id = notification_data.get("user_id")

        print(f"📢 Sending {notification_type} notification to user {user_id}")

        if notification_type == "document_processed":
            return await self._send_processing_complete_notification(notification_data)
        elif notification_type == "document_error":
            return await self._send_error_notification(notification_data)
        else:
            return {"status": "ignored", "reason": f"Unknown notification type: {notification_type}"}

    async def _send_processing_complete_notification(self, data: dict) -> dict:
        """Send document processing complete notification."""
        document_id = data["document_id"]
        user_id = data["user_id"]
        background_tasks = data.get("background_tasks", [])

        # Simulate sending email/push notification
        await asyncio.sleep(0.3)

        notification_content = f"""
        ✅ Document Processing Complete

        Document ID: {document_id}
        Processed: {data.get('processed_at')}

        Background tasks initiated:
        {', '.join(background_tasks) if background_tasks else 'None'}

        Your document is now available in your dashboard.
        """

        print(f"📧 Sent completion notification to user {user_id}")
        print(notification_content)

        return {
            "status": "sent",
            "notification_type": "document_processed",
            "user_id": user_id,
            "document_id": document_id
        }

    async def _send_error_notification(self, data: dict) -> dict:
        """Send document processing error notification."""
        document_id = data["document_id"]
        user_id = data["user_id"]
        error = data.get("error", "Unknown error")

        # Simulate sending error notification
        await asyncio.sleep(0.2)

        notification_content = f"""
        ❌ Document Processing Failed

        Document ID: {document_id}
        Failed: {data.get('failed_at')}
        Error: {error}

        Please try uploading your document again or contact support
        if the problem persists.
        """

        print(f"🚨 Sent error notification to user {user_id}")
        print(notification_content)

        return {
            "status": "sent",
            "notification_type": "document_error",
            "user_id": user_id,
            "document_id": document_id,
            "error": error
        }

# Run the notification service
if __name__ == "__main__":
    worker = NotificationService()
    asyncio.run(worker.start())
```

## Document Uploader (Test Client)

```python
# document_uploader.py
import asyncio
import uuid
from datetime import datetime
from pythia.brokers.cloud import ServiceBusProducer
from pythia.models import Message

class DocumentUploader:
    """Simulates document uploads by sending messages to Service Bus."""

    def __init__(self):
        self.producer = ServiceBusProducer(
            queue_name="${SERVICE_BUS_DOCUMENTS_QUEUE}"
        )

    async def upload_document(self, filename: str, file_type: str, user_id: int = 1):
        """Simulate document upload."""
        document_id = str(uuid.uuid4())

        document_data = {
            "document_id": document_id,
            "filename": filename,
            "file_path": f"/uploads/{document_id}.{file_type}",
            "file_type": file_type,
            "file_size": 1024 * 1024 * 2,  # 2MB
            "mime_type": self._get_mime_type(file_type),
            "user_id": user_id,
            "uploaded_at": datetime.now().isoformat(),
            "language": "en"
        }

        await self.producer.send(
            message=Message(body=document_data),
            correlation_id=document_id,
            subject=f"Document Upload: {filename}"
        )

        print(f"📤 Uploaded document: {filename} (ID: {document_id})")
        return document_id

    def _get_mime_type(self, file_type: str) -> str:
        """Get MIME type for file extension."""
        mime_types = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'txt': 'text/plain',
            'png': 'image/png',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'gif': 'image/gif'
        }
        return mime_types.get(file_type, 'application/octet-stream')

async def upload_test_documents():
    """Upload various test documents."""
    uploader = DocumentUploader()

    test_docs = [
        ("invoice_2024.pdf", "pdf"),
        ("presentation.pdf", "pdf"),
        ("scanned_receipt.jpg", "jpg"),
        ("contract.docx", "docx"),
        ("diagram.png", "png"),
        ("notes.txt", "txt")
    ]

    for filename, file_type in test_docs:
        await uploader.upload_document(filename, file_type)
        await asyncio.sleep(1)  # Space out uploads

if __name__ == "__main__":
    asyncio.run(upload_test_documents())
```

## Azure Resource Setup

Create the required Azure resources:

```bash
# Create Resource Group
az group create --name pythia-docs --location eastus

# Create Service Bus Namespace
az servicebus namespace create --resource-group pythia-docs --name pythia-sb --location eastus

# Create Service Bus Queues
az servicebus queue create --resource-group pythia-docs --namespace-name pythia-sb --name document-processing
az servicebus queue create --resource-group pythia-docs --namespace-name pythia-sb --name notifications

# Create Storage Account
az storage account create --resource-group pythia-docs --name pythiastorage --location eastus --sku Standard_LRS

# Get connection strings
az servicebus namespace authorization-rule keys list --resource-group pythia-docs --namespace-name pythia-sb --name RootManageSharedAccessKey
az storage account show-connection-string --resource-group pythia-docs --name pythiastorage
```

## Running the Complete System

1. **Start all workers:**

```bash
# Terminal 1: Document processor (Service Bus)
python document_processor.py

# Terminal 2: Thumbnail generator (Storage Queue)
python thumbnail_generator.py

# Terminal 3: OCR processor (Storage Queue)
python ocr_processor.py

# Terminal 4: Notification service (Service Bus)
python notification_service.py
```

2. **Upload test documents:**

```bash
# Terminal 5: Upload documents
python document_uploader.py
```

## Error Handling and Dead Letter Queues

```python
# Configure dead letter queues in Azure
from azure.servicebus import ServiceBusClient
from azure.servicebus.management import ServiceBusAdministrationClient

def setup_dead_letter_handling():
    """Set up dead letter queue handling."""
    mgmt_client = ServiceBusAdministrationClient.from_connection_string(
        "${AZURE_SERVICE_BUS_CONNECTION_STRING}"
    )

    # Update queue properties for dead letter handling
    queue_properties = mgmt_client.get_queue("document-processing")
    queue_properties.max_delivery_count = 3  # Move to DLQ after 3 attempts
    queue_properties.dead_lettering_on_message_expiration = True

    mgmt_client.update_queue(queue_properties)
```

## Monitoring with Azure

### Application Insights Integration

```python
# monitoring.py
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

def setup_monitoring():
    """Set up Azure Application Insights monitoring."""
    tracer_provider = TracerProvider()
    trace.set_tracer_provider(tracer_provider)

    exporter = AzureMonitorTraceExporter(
        connection_string="InstrumentationKey=your-key"
    )

    span_processor = BatchSpanProcessor(exporter)
    tracer_provider.add_span_processor(span_processor)
```

## Best Practices

1. **Service Bus vs Storage Queues**:
   - Use Service Bus for critical operations requiring guaranteed delivery
   - Use Storage Queues for background tasks where occasional message loss is acceptable

2. **Message Handling**:
   - Set appropriate visibility timeouts based on processing time
   - Implement dead letter queue handling for failed messages
   - Use correlation IDs for message tracking

3. **Scaling**:
   - Run multiple instances of workers for horizontal scaling
   - Use auto-scaling based on queue depth metrics
   - Monitor processing times and adjust worker counts

4. **Cost Optimization**:
   - Use Storage Queues for high-volume, low-priority tasks
   - Set appropriate message TTL to avoid storage costs
   - Monitor and optimize message sizes

This example demonstrates a production-ready document processing pipeline using both Azure Service Bus and Storage Queues with Pythia's cloud workers.
