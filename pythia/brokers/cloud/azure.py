"""
Azure Service Bus and Storage Queue workers for Pythia
"""

import asyncio
from typing import Any, Optional, Dict
import json

try:
    from azure.servicebus import ServiceBusClient, ServiceBusMessage
    from azure.servicebus.exceptions import ServiceBusError
    from azure.storage.queue import QueueServiceClient
    from azure.core.exceptions import AzureError

    HAS_AZURE_SERVICEBUS = True
    HAS_AZURE_STORAGE = True
except ImportError:
    HAS_AZURE_SERVICEBUS = False
    HAS_AZURE_STORAGE = False

from .base import CloudWorker, CloudConfig
from ...config.cloud import AzureConfig
from ...core.message import Message


class ServiceBusConsumer(CloudWorker):
    """Worker that consumes messages from Azure Service Bus"""

    def __init__(
        self,
        queue_name: str,
        connection_string: Optional[str] = None,
        azure_config: Optional[AzureConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_AZURE_SERVICEBUS:
            raise ImportError(
                "azure-servicebus is required for Azure Service Bus support. "
                "Install with: pip install 'pythia[azure]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.queue_name = queue_name
        self.azure_config = azure_config or AzureConfig()
        self.connection_string = (
            connection_string or self.azure_config.service_bus_connection_string
        )

        if not self.connection_string:
            raise ValueError("Azure Service Bus connection string is required")

        self.servicebus_client = None
        self.receiver = None

    async def connect(self) -> None:
        """Initialize Service Bus client and receiver"""
        await super().connect()

        try:
            # Create Service Bus client
            self.servicebus_client = ServiceBusClient.from_connection_string(
                conn_str=self.connection_string
            )

            # Create receiver for the queue
            self.receiver = self.servicebus_client.get_queue_receiver(
                queue_name=self.queue_name,
                max_wait_time=30,  # 30 seconds wait for messages
            )

            self.logger.info(f"Service Bus consumer connected to queue: {self.queue_name}")

        except Exception as e:
            self.logger.error(f"Failed to connect to Azure Service Bus: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Service Bus client and receiver"""
        if self.receiver:
            self.receiver.close()
            self.receiver = None
        if self.servicebus_client:
            self.servicebus_client.close()
            self.servicebus_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """Main processing loop for Service Bus messages"""
        if not self.receiver:
            await self.connect()

        self.logger.info("Starting Service Bus message consumption")

        while True:
            try:
                # Receive messages (blocking call with timeout)
                messages = self.receiver.receive_messages(
                    max_message_count=self.azure_config.max_messages,
                    max_wait_time=30,  # 30 seconds timeout
                )

                if not messages:
                    # No messages, continue polling
                    await asyncio.sleep(1)
                    continue

                # Process each message
                for sb_message in messages:
                    try:
                        # Convert Service Bus message to Pythia Message
                        message = self._convert_servicebus_message(sb_message)

                        # Process the message
                        result = await self.process_message(message)

                        # Complete message if processed successfully
                        if result is not None:  # Allow False as valid result
                            self.receiver.complete_message(sb_message)

                    except Exception as e:
                        self.logger.error(
                            f"Error processing Service Bus message: {e}",
                            message_id=sb_message.message_id,
                            exc_info=True,
                        )
                        # Message will be retried automatically by Service Bus
                        self.receiver.abandon_message(sb_message)

            except ServiceBusError as e:
                self.logger.error(f"Azure Service Bus error: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on Azure errors

            except Exception as e:
                self.logger.error(f"Unexpected error in Service Bus processing: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def process_message(self, message: Message) -> Any:
        """
        Process a single message from Service Bus
        Override this method in subclasses
        """
        self.logger.info(f"Processing Service Bus message: {message.headers.get('message_id')}")
        return {"processed": True}

    def _convert_servicebus_message(self, sb_message) -> Message:
        """Convert Service Bus message to Pythia Message"""
        # Parse body (handle JSON if possible)
        if hasattr(sb_message, "body"):
            body = str(sb_message.body)
        else:
            body = str(sb_message)

        try:
            body = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            # Keep as string if not valid JSON
            pass

        # Extract headers from message properties
        headers = {
            "message_id": sb_message.message_id,
            "correlation_id": sb_message.correlation_id,
            "content_type": sb_message.content_type,
            "reply_to": sb_message.reply_to,
            "time_to_live": sb_message.time_to_live.total_seconds()
            if sb_message.time_to_live
            else None,
            "delivery_count": sb_message.delivery_count,
            "enqueued_time": sb_message.enqueued_time_utc.isoformat()
            if sb_message.enqueued_time_utc
            else None,
        }

        # Add custom properties as headers
        if hasattr(sb_message, "application_properties") and sb_message.application_properties:
            for key, value in sb_message.application_properties.items():
                headers[f"prop_{key}"] = value

        return Message(body=body, headers=headers)


class ServiceBusProducer(CloudWorker):
    """Worker that sends messages to Azure Service Bus"""

    def __init__(
        self,
        queue_name: str,
        connection_string: Optional[str] = None,
        azure_config: Optional[AzureConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_AZURE_SERVICEBUS:
            raise ImportError(
                "azure-servicebus is required for Azure Service Bus support. "
                "Install with: pip install 'pythia[azure]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.queue_name = queue_name
        self.azure_config = azure_config or AzureConfig()
        self.connection_string = (
            connection_string or self.azure_config.service_bus_connection_string
        )

        if not self.connection_string:
            raise ValueError("Azure Service Bus connection string is required")

        self.servicebus_client = None
        self.sender = None

    async def connect(self) -> None:
        """Initialize Service Bus client and sender"""
        await super().connect()

        try:
            # Create Service Bus client
            self.servicebus_client = ServiceBusClient.from_connection_string(
                conn_str=self.connection_string
            )

            # Create sender for the queue
            self.sender = self.servicebus_client.get_queue_sender(queue_name=self.queue_name)

            self.logger.info(f"Service Bus producer connected to queue: {self.queue_name}")

        except Exception as e:
            self.logger.error(f"Failed to connect to Azure Service Bus: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Service Bus client and sender"""
        if self.sender:
            self.sender.close()
            self.sender = None
        if self.servicebus_client:
            self.servicebus_client.close()
            self.servicebus_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """
        Base process method - override in subclasses to define
        how messages are generated and sent
        """
        self.logger.info("Service Bus producer started - override process() method")

        # Base implementation just waits
        while True:
            await asyncio.sleep(1)

    async def send_message(
        self,
        message: Any,
        properties: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> bool:
        """
        Send a message to Service Bus queue

        Args:
            message: Message to send (will be JSON serialized if dict/list)
            properties: Optional application properties
            correlation_id: Optional correlation ID
            content_type: Optional content type

        Returns:
            True if message was sent successfully, False otherwise
        """
        if not self.sender:
            await self.connect()

        try:
            # Serialize message
            if isinstance(message, (dict, list)):
                message_body = json.dumps(message)
                content_type = content_type or "application/json"
            else:
                message_body = str(message)
                content_type = content_type or "text/plain"

            # Create Service Bus message
            sb_message = ServiceBusMessage(
                body=message_body, content_type=content_type, correlation_id=correlation_id
            )

            # Add application properties
            if properties:
                sb_message.application_properties = properties

            # Send message
            self.sender.send_messages(sb_message)

            self.logger.info(f"Sent Service Bus message with correlation_id: {correlation_id}")
            return True

        except ServiceBusError as e:
            self.logger.error(f"Failed to send Service Bus message: {e}", exc_info=True)
            return False

    async def send_from_pythia_message(
        self, message: Message, correlation_id: Optional[str] = None
    ) -> bool:
        """
        Send a Pythia Message to Service Bus

        Args:
            message: Pythia message to send
            correlation_id: Optional correlation ID

        Returns:
            True if message was sent successfully, False otherwise
        """
        # Extract properties from headers
        properties = {}
        content_type = None

        if message.headers:
            for key, value in message.headers.items():
                if key == "content_type":
                    content_type = value
                elif not key.startswith("_"):  # Skip internal headers
                    properties[key] = value

        return await self.send_message(
            message=message.body,
            properties=properties,
            correlation_id=correlation_id,
            content_type=content_type,
        )


class StorageQueueConsumer(CloudWorker):
    """Worker that consumes messages from Azure Storage Queues"""

    def __init__(
        self,
        queue_name: str,
        connection_string: Optional[str] = None,
        azure_config: Optional[AzureConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_AZURE_STORAGE:
            raise ImportError(
                "azure-storage-queue is required for Azure Storage Queue support. "
                "Install with: pip install 'pythia[azure]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.queue_name = queue_name
        self.azure_config = azure_config or AzureConfig()
        self.connection_string = connection_string or self.azure_config.storage_connection_string

        if not self.connection_string:
            raise ValueError("Azure Storage connection string is required")

        self.queue_service_client = None
        self.queue_client = None

    async def connect(self) -> None:
        """Initialize Storage Queue client"""
        await super().connect()

        try:
            # Create Queue service client
            self.queue_service_client = QueueServiceClient.from_connection_string(
                conn_str=self.connection_string
            )

            # Get queue client
            self.queue_client = self.queue_service_client.get_queue_client(queue=self.queue_name)

            # Create queue if it doesn't exist
            self.queue_client.create_queue()

            self.logger.info(f"Storage Queue consumer connected to: {self.queue_name}")

        except Exception as e:
            self.logger.error(f"Failed to connect to Azure Storage Queue: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Storage Queue client"""
        # Azure Storage clients don't need explicit closing
        self.queue_client = None
        self.queue_service_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """Main processing loop for Storage Queue messages"""
        if not self.queue_client:
            await self.connect()

        self.logger.info("Starting Storage Queue message consumption")

        while True:
            try:
                # Receive messages (non-blocking)
                messages = self.queue_client.receive_messages(
                    messages_per_page=self.azure_config.max_messages,
                    visibility_timeout=self.azure_config.visibility_timeout,
                )

                message_found = False

                # Process each message
                for storage_message in messages:
                    message_found = True
                    try:
                        # Convert Storage Queue message to Pythia Message
                        message = self._convert_storage_message(storage_message)

                        # Process the message
                        result = await self.process_message(message)

                        # Delete message if processed successfully
                        if result is not None:  # Allow False as valid result
                            self.queue_client.delete_message(storage_message)

                    except Exception as e:
                        self.logger.error(
                            f"Error processing Storage Queue message: {e}",
                            message_id=storage_message.id,
                            exc_info=True,
                        )
                        # Message will become visible again after visibility timeout

                if not message_found:
                    # No messages, wait before polling again
                    await asyncio.sleep(5)

            except AzureError as e:
                self.logger.error(f"Azure Storage Queue error: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on Azure errors

            except Exception as e:
                self.logger.error(
                    f"Unexpected error in Storage Queue processing: {e}", exc_info=True
                )
                await asyncio.sleep(1)

    async def process_message(self, message: Message) -> Any:
        """
        Process a single message from Storage Queue
        Override this method in subclasses
        """
        self.logger.info(f"Processing Storage Queue message: {message.headers.get('message_id')}")
        return {"processed": True}

    def _convert_storage_message(self, storage_message) -> Message:
        """Convert Storage Queue message to Pythia Message"""
        # Parse content (handle JSON if possible)
        body = storage_message.content
        try:
            body = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            # Keep as string if not valid JSON
            pass

        # Extract headers from message metadata
        headers = {
            "message_id": storage_message.id,
            "pop_receipt": storage_message.pop_receipt,
            "insertion_time": storage_message.insertion_time.isoformat()
            if storage_message.insertion_time
            else None,
            "expiration_time": storage_message.expiration_time.isoformat()
            if storage_message.expiration_time
            else None,
            "next_visible_time": storage_message.next_visible_time.isoformat()
            if storage_message.next_visible_time
            else None,
            "dequeue_count": storage_message.dequeue_count,
        }

        return Message(body=body, headers=headers)


class StorageQueueProducer(CloudWorker):
    """Worker that sends messages to Azure Storage Queues"""

    def __init__(
        self,
        queue_name: str,
        connection_string: Optional[str] = None,
        azure_config: Optional[AzureConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_AZURE_STORAGE:
            raise ImportError(
                "azure-storage-queue is required for Azure Storage Queue support. "
                "Install with: pip install 'pythia[azure]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.queue_name = queue_name
        self.azure_config = azure_config or AzureConfig()
        self.connection_string = connection_string or self.azure_config.storage_connection_string

        if not self.connection_string:
            raise ValueError("Azure Storage connection string is required")

        self.queue_service_client = None
        self.queue_client = None

    async def connect(self) -> None:
        """Initialize Storage Queue client"""
        await super().connect()

        try:
            # Create Queue service client
            self.queue_service_client = QueueServiceClient.from_connection_string(
                conn_str=self.connection_string
            )

            # Get queue client
            self.queue_client = self.queue_service_client.get_queue_client(queue=self.queue_name)

            # Create queue if it doesn't exist
            self.queue_client.create_queue()

            self.logger.info(f"Storage Queue producer connected to: {self.queue_name}")

        except Exception as e:
            self.logger.error(f"Failed to connect to Azure Storage Queue: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Storage Queue client"""
        # Azure Storage clients don't need explicit closing
        self.queue_client = None
        self.queue_service_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """
        Base process method - override in subclasses to define
        how messages are generated and sent
        """
        self.logger.info("Storage Queue producer started - override process() method")

        # Base implementation just waits
        while True:
            await asyncio.sleep(1)

    async def send_message(
        self,
        message: Any,
        visibility_timeout: Optional[int] = None,
        time_to_live: Optional[int] = None,
    ) -> Optional[str]:
        """
        Send a message to Storage Queue

        Args:
            message: Message to send (will be JSON serialized if dict/list)
            visibility_timeout: Optional initial visibility timeout
            time_to_live: Optional message TTL in seconds

        Returns:
            Message ID if successful, None if failed
        """
        if not self.queue_client:
            await self.connect()

        try:
            # Serialize message
            if isinstance(message, (dict, list)):
                message_content = json.dumps(message)
            else:
                message_content = str(message)

            # Send message
            response = self.queue_client.send_message(
                content=message_content,
                visibility_timeout=visibility_timeout,
                time_to_live=time_to_live,
            )

            message_id = response.id
            self.logger.info(f"Sent Storage Queue message: {message_id}")
            return message_id

        except AzureError as e:
            self.logger.error(f"Failed to send Storage Queue message: {e}", exc_info=True)
            return None

    async def send_from_pythia_message(
        self,
        message: Message,
        visibility_timeout: Optional[int] = None,
        time_to_live: Optional[int] = None,
    ) -> Optional[str]:
        """
        Send a Pythia Message to Storage Queue

        Args:
            message: Pythia message to send
            visibility_timeout: Optional initial visibility timeout
            time_to_live: Optional message TTL in seconds

        Returns:
            Message ID if successful, None if failed
        """
        return await self.send_message(
            message=message.body, visibility_timeout=visibility_timeout, time_to_live=time_to_live
        )
