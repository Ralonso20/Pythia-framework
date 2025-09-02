"""
Tests for Azure cloud workers
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json
from datetime import datetime

# Import the Azure workers
try:
    from pythia.brokers.cloud.azure import (
        ServiceBusConsumer,
        ServiceBusProducer,
        StorageQueueConsumer,
        StorageQueueProducer
    )
    from pythia.config.cloud import AzureConfig
    from pythia.core.message import Message
    HAS_AZURE_SUPPORT = True
except ImportError as e:
    HAS_AZURE_SUPPORT = False

# Skip all tests if Azure dependencies are not available
pytestmark = pytest.mark.skipif(
    not HAS_AZURE_SUPPORT,
    reason="Azure dependencies not installed. Install with: pip install 'pythia[azure]'"
)


# Test worker implementations
class TestServiceBusConsumerImpl(ServiceBusConsumer):
    """Concrete Service Bus consumer for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_messages = []

    async def process_message(self, message):
        """Process a message from Service Bus"""
        self.processed_messages.append(message)
        return {"processed": True, "message_id": message.headers.get("message_id")}


class TestServiceBusProducerImpl(ServiceBusProducer):
    """Concrete Service Bus producer for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sent_messages = []

    async def process(self):
        """For testing, track process calls"""
        pass


class TestStorageQueueConsumerImpl(StorageQueueConsumer):
    """Concrete Storage Queue consumer for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_messages = []

    async def process_message(self, message):
        """Process a message from Storage Queue"""
        self.processed_messages.append(message)
        return {"processed": True, "message_id": message.headers.get("message_id")}


class TestStorageQueueProducerImpl(StorageQueueProducer):
    """Concrete Storage Queue producer for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sent_messages = []

    async def process(self):
        """For testing, track process calls"""
        pass


class TestServiceBusConsumer:
    """Test Service Bus Consumer functionality"""

    @pytest.mark.asyncio
    async def test_servicebus_consumer_initialization(self):
        """Test Service Bus consumer initialization"""
        azure_config = AzureConfig(
            service_bus_connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            service_bus_queue_name="test-queue"
        )

        consumer = TestServiceBusConsumerImpl(
            queue_name="test-queue",
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            azure_config=azure_config
        )

        assert consumer.queue_name == "test-queue"
        assert consumer.azure_config == azure_config
        assert consumer.servicebus_client is None

    @pytest.mark.asyncio
    async def test_servicebus_consumer_connection(self):
        """Test Service Bus consumer connection"""
        with patch("azure.servicebus.ServiceBusClient.from_connection_string") as mock_client_factory:
            mock_client = MagicMock()
            mock_receiver = MagicMock()
            mock_client_factory.return_value = mock_client
            mock_client.get_queue_receiver.return_value = mock_receiver

            consumer = TestServiceBusConsumerImpl(
                queue_name="test-queue",
                connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
            )

            await consumer.connect()

            mock_client_factory.assert_called_once()
            mock_client.get_queue_receiver.assert_called_once_with(
                queue_name="test-queue",
                max_wait_time=30
            )
            assert consumer.servicebus_client == mock_client
            assert consumer.receiver == mock_receiver

            await consumer.disconnect()
            mock_receiver.close.assert_called_once()
            mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_convert_servicebus_message(self):
        """Test converting Service Bus message to Pythia Message"""
        consumer = TestServiceBusConsumerImpl(
            queue_name="test-queue",
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
        )

        # Mock Service Bus message
        mock_sb_message = MagicMock()
        mock_sb_message.body = json.dumps({"user_id": 123, "action": "created"})
        mock_sb_message.message_id = "test-message-id"
        mock_sb_message.correlation_id = "test-correlation-id"
        mock_sb_message.content_type = "application/json"
        mock_sb_message.reply_to = "reply-queue"
        mock_sb_message.time_to_live = None
        mock_sb_message.delivery_count = 1
        mock_sb_message.enqueued_time_utc = datetime(2023, 1, 1, 12, 0, 0)
        mock_sb_message.application_properties = {"source": "user-service"}

        message = consumer._convert_servicebus_message(mock_sb_message)

        assert message.body == {"user_id": 123, "action": "created"}
        assert message.headers["message_id"] == "test-message-id"
        assert message.headers["correlation_id"] == "test-correlation-id"
        assert message.headers["content_type"] == "application/json"
        assert message.headers["prop_source"] == "user-service"

    @pytest.mark.asyncio
    async def test_missing_connection_string_error(self):
        """Test error when connection string is missing"""
        with pytest.raises(ValueError) as exc_info:
            TestServiceBusConsumerImpl(
                queue_name="test-queue",
                connection_string=None,
                azure_config=AzureConfig()  # No connection string in config
            )

        assert "Azure Service Bus connection string is required" in str(exc_info.value)


class TestServiceBusProducer:
    """Test Service Bus Producer functionality"""

    @pytest.mark.asyncio
    async def test_servicebus_producer_initialization(self):
        """Test Service Bus producer initialization"""
        azure_config = AzureConfig(
            service_bus_connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            service_bus_queue_name="test-queue"
        )

        producer = TestServiceBusProducerImpl(
            queue_name="test-queue",
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            azure_config=azure_config
        )

        assert producer.queue_name == "test-queue"
        assert producer.azure_config == azure_config
        assert producer.servicebus_client is None

    @pytest.mark.asyncio
    async def test_servicebus_producer_connection(self):
        """Test Service Bus producer connection"""
        with patch("azure.servicebus.ServiceBusClient.from_connection_string") as mock_client_factory:
            mock_client = MagicMock()
            mock_sender = MagicMock()
            mock_client_factory.return_value = mock_client
            mock_client.get_queue_sender.return_value = mock_sender

            producer = TestServiceBusProducerImpl(
                queue_name="test-queue",
                connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
            )

            await producer.connect()

            mock_client_factory.assert_called_once()
            mock_client.get_queue_sender.assert_called_once_with(queue_name="test-queue")
            assert producer.servicebus_client == mock_client
            assert producer.sender == mock_sender

            await producer.disconnect()
            mock_sender.close.assert_called_once()
            mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_message_dict(self):
        """Test sending dictionary message to Service Bus"""
        with patch("azure.servicebus.ServiceBusClient.from_connection_string") as mock_client_factory, \
             patch("pythia.brokers.cloud.azure.ServiceBusMessage") as mock_message_class:

            mock_client = MagicMock()
            mock_sender = MagicMock()
            mock_message = MagicMock()

            mock_client_factory.return_value = mock_client
            mock_client.get_queue_sender.return_value = mock_sender
            mock_message_class.return_value = mock_message

            producer = TestServiceBusProducerImpl(
                queue_name="test-queue",
                connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
            )
            producer.sender = mock_sender

            message_data = {"user_id": 123, "event": "user_created"}
            result = await producer.send_message(
                message=message_data,
                properties={"source": "user-service"},
                correlation_id="test-correlation-id"
            )

            assert result is True
            mock_message_class.assert_called_once_with(
                body=json.dumps(message_data),
                content_type="application/json",
                correlation_id="test-correlation-id"
            )
            mock_sender.send_messages.assert_called_once_with(mock_message)

    @pytest.mark.asyncio
    async def test_send_from_pythia_message(self):
        """Test sending Pythia message to Service Bus"""
        with patch("azure.servicebus.ServiceBusClient.from_connection_string") as mock_client_factory, \
             patch("pythia.brokers.cloud.azure.ServiceBusMessage") as mock_message_class:

            mock_client = MagicMock()
            mock_sender = MagicMock()
            mock_message = MagicMock()

            mock_client_factory.return_value = mock_client
            mock_client.get_queue_sender.return_value = mock_sender
            mock_message_class.return_value = mock_message

            producer = TestServiceBusProducerImpl(
                queue_name="test-queue",
                connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
            )
            producer.sender = mock_sender

            pythia_message = Message(
                body={"user_id": 123, "action": "created"},
                headers={"source": "user-service", "content_type": "application/json"}
            )

            result = await producer.send_from_pythia_message(
                message=pythia_message,
                correlation_id="test-correlation-id"
            )

            assert result is True
            mock_message_class.assert_called_once_with(
                body=json.dumps({"user_id": 123, "action": "created"}),
                content_type="application/json",
                correlation_id="test-correlation-id"
            )


class TestStorageQueueConsumer:
    """Test Storage Queue Consumer functionality"""

    @pytest.mark.asyncio
    async def test_storage_queue_consumer_initialization(self):
        """Test Storage Queue consumer initialization"""
        azure_config = AzureConfig(
            storage_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            storage_queue_name="test-queue"
        )

        consumer = TestStorageQueueConsumerImpl(
            queue_name="test-queue",
            connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            azure_config=azure_config
        )

        assert consumer.queue_name == "test-queue"
        assert consumer.azure_config == azure_config
        assert consumer.queue_service_client is None

    @pytest.mark.asyncio
    async def test_storage_queue_consumer_connection(self):
        """Test Storage Queue consumer connection"""
        with patch("azure.storage.queue.QueueServiceClient.from_connection_string") as mock_service_factory:
            mock_service_client = MagicMock()
            mock_queue_client = MagicMock()
            mock_service_factory.return_value = mock_service_client
            mock_service_client.get_queue_client.return_value = mock_queue_client

            consumer = TestStorageQueueConsumerImpl(
                queue_name="test-queue",
                connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net"
            )

            await consumer.connect()

            mock_service_factory.assert_called_once()
            mock_service_client.get_queue_client.assert_called_once_with(queue="test-queue")
            mock_queue_client.create_queue.assert_called_once()
            assert consumer.queue_service_client == mock_service_client
            assert consumer.queue_client == mock_queue_client

            await consumer.disconnect()
            assert consumer.queue_client is None

    @pytest.mark.asyncio
    async def test_convert_storage_message(self):
        """Test converting Storage Queue message to Pythia Message"""
        consumer = TestStorageQueueConsumerImpl(
            queue_name="test-queue",
            connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net"
        )

        # Mock Storage Queue message
        mock_storage_message = MagicMock()
        mock_storage_message.content = json.dumps({"order_id": 456, "status": "pending"})
        mock_storage_message.id = "test-message-id"
        mock_storage_message.pop_receipt = "test-pop-receipt"
        mock_storage_message.insertion_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_storage_message.expiration_time = datetime(2023, 1, 2, 12, 0, 0)
        mock_storage_message.next_visible_time = datetime(2023, 1, 1, 12, 5, 0)
        mock_storage_message.dequeue_count = 1

        message = consumer._convert_storage_message(mock_storage_message)

        assert message.body == {"order_id": 456, "status": "pending"}
        assert message.headers["message_id"] == "test-message-id"
        assert message.headers["pop_receipt"] == "test-pop-receipt"
        assert message.headers["dequeue_count"] == 1


class TestStorageQueueProducer:
    """Test Storage Queue Producer functionality"""

    @pytest.mark.asyncio
    async def test_storage_queue_producer_initialization(self):
        """Test Storage Queue producer initialization"""
        azure_config = AzureConfig(
            storage_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            storage_queue_name="test-queue"
        )

        producer = TestStorageQueueProducerImpl(
            queue_name="test-queue",
            connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            azure_config=azure_config
        )

        assert producer.queue_name == "test-queue"
        assert producer.azure_config == azure_config
        assert producer.queue_service_client is None

    @pytest.mark.asyncio
    async def test_send_storage_queue_message(self):
        """Test sending message to Storage Queue"""
        with patch("azure.storage.queue.QueueServiceClient.from_connection_string") as mock_service_factory:
            mock_service_client = MagicMock()
            mock_queue_client = MagicMock()
            mock_response = MagicMock()
            mock_response.id = "sent-message-id"

            mock_service_factory.return_value = mock_service_client
            mock_service_client.get_queue_client.return_value = mock_queue_client
            mock_queue_client.send_message.return_value = mock_response

            producer = TestStorageQueueProducerImpl(
                queue_name="test-queue",
                connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net"
            )
            producer.queue_client = mock_queue_client

            message_data = {"order_id": 789, "action": "process"}
            message_id = await producer.send_message(
                message=message_data,
                visibility_timeout=30,
                time_to_live=3600
            )

            assert message_id == "sent-message-id"
            mock_queue_client.send_message.assert_called_once_with(
                content=json.dumps(message_data),
                visibility_timeout=30,
                time_to_live=3600
            )

    @pytest.mark.asyncio
    async def test_send_from_pythia_message_storage_queue(self):
        """Test sending Pythia message to Storage Queue"""
        with patch("azure.storage.queue.QueueServiceClient.from_connection_string") as mock_service_factory:
            mock_service_client = MagicMock()
            mock_queue_client = MagicMock()
            mock_response = MagicMock()
            mock_response.id = "sent-message-id"

            mock_service_factory.return_value = mock_service_client
            mock_service_client.get_queue_client.return_value = mock_queue_client
            mock_queue_client.send_message.return_value = mock_response

            producer = TestStorageQueueProducerImpl(
                queue_name="test-queue",
                connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net"
            )
            producer.queue_client = mock_queue_client

            pythia_message = Message(
                body={"order_id": 789, "action": "process"},
                headers={"source": "order-service"}
            )

            message_id = await producer.send_from_pythia_message(
                message=pythia_message,
                visibility_timeout=60
            )

            assert message_id == "sent-message-id"
            mock_queue_client.send_message.assert_called_once_with(
                content=json.dumps({"order_id": 789, "action": "process"}),
                visibility_timeout=60,
                time_to_live=None
            )


@pytest.mark.asyncio
async def test_import_error_handling():
    """Test handling of missing Azure dependencies"""
    with patch("pythia.brokers.cloud.azure.HAS_AZURE_SERVICEBUS", False):
        with pytest.raises(ImportError) as exc_info:
            ServiceBusConsumer(
                queue_name="test",
                connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
            )

        assert "azure-servicebus is required for Azure Service Bus support" in str(exc_info.value)
        assert "pip install 'pythia[azure]'" in str(exc_info.value)

    with patch("pythia.brokers.cloud.azure.HAS_AZURE_STORAGE", False):
        with pytest.raises(ImportError) as exc_info:
            StorageQueueConsumer(
                queue_name="test",
                connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net"
            )

        assert "azure-storage-queue is required for Azure Storage Queue support" in str(exc_info.value)
        assert "pip install 'pythia[azure]'" in str(exc_info.value)


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
