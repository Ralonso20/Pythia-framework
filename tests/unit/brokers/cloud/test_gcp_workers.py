"""
Tests for Google Cloud Pub/Sub workers
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json
from datetime import datetime

# Import the GCP workers
try:
    from pythia.brokers.cloud.gcp import PubSubSubscriber, PubSubPublisher
    from pythia.config.cloud import GCPConfig
    from pythia.core.message import Message
    HAS_GCP_SUPPORT = True
except ImportError as e:
    HAS_GCP_SUPPORT = False

# Skip all tests if GCP dependencies are not available
pytestmark = pytest.mark.skipif(
    not HAS_GCP_SUPPORT,
    reason="GCP dependencies not installed. Install with: pip install 'pythia[gcp]'"
)


# Test worker implementations
class TestPubSubSubscriberImpl(PubSubSubscriber):
    """Concrete Pub/Sub subscriber for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_messages = []

    async def process_message(self, message):
        """Process a message from Pub/Sub"""
        self.processed_messages.append(message)
        return {"processed": True, "message_id": message.headers.get("message_id")}


class TestPubSubPublisherImpl(PubSubPublisher):
    """Concrete Pub/Sub publisher for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.published_messages = []

    async def process(self):
        """For testing, track process calls"""
        pass


class TestPubSubSubscriber:
    """Test Pub/Sub Subscriber functionality"""

    @pytest.mark.asyncio
    async def test_pubsub_subscriber_initialization(self):
        """Test Pub/Sub subscriber initialization"""
        gcp_config = GCPConfig(
            project_id="test-project",
            subscription_name="test-subscription"
        )

        subscriber = TestPubSubSubscriberImpl(
            subscription_path="projects/test-project/subscriptions/test-subscription",
            project_id="test-project",
            gcp_config=gcp_config
        )

        assert subscriber.subscription_path == "projects/test-project/subscriptions/test-subscription"
        assert subscriber.project_id == "test-project"
        assert subscriber.gcp_config == gcp_config
        assert subscriber.subscriber_client is None

    @pytest.mark.asyncio
    async def test_pubsub_subscriber_connection(self):
        """Test Pub/Sub subscriber connection"""
        with patch("google.cloud.pubsub_v1.SubscriberClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            subscriber = TestPubSubSubscriberImpl(
                subscription_path="projects/test-project/subscriptions/test-subscription"
            )

            await subscriber.connect()

            mock_client_class.assert_called_once()
            mock_client.get_subscription.assert_called_once()
            assert subscriber.subscriber_client == mock_client

            await subscriber.disconnect()
            mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_convert_pubsub_message(self):
        """Test converting Pub/Sub message to Pythia Message"""
        subscriber = TestPubSubSubscriberImpl(
            subscription_path="projects/test-project/subscriptions/test-subscription"
        )

        # Mock Pub/Sub message
        mock_pubsub_message = MagicMock()
        mock_pubsub_message.data = json.dumps({"user_id": 123, "action": "created"}).encode('utf-8')
        mock_pubsub_message.message_id = "test-message-id"
        mock_pubsub_message.publish_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_pubsub_message.ordering_key = "user-123"
        mock_pubsub_message.attributes = {"source": "user-service", "version": "1.0"}

        message = subscriber._convert_pubsub_message(mock_pubsub_message)

        assert message.body == {"user_id": 123, "action": "created"}
        assert message.headers["message_id"] == "test-message-id"
        assert message.headers["ordering_key"] == "user-123"
        assert message.headers["attr_source"] == "user-service"
        assert message.headers["attr_version"] == "1.0"

    @pytest.mark.asyncio
    async def test_convert_pubsub_message_plain_text(self):
        """Test converting plain text Pub/Sub message"""
        subscriber = TestPubSubSubscriberImpl(
            subscription_path="projects/test-project/subscriptions/test-subscription"
        )

        mock_pubsub_message = MagicMock()
        mock_pubsub_message.data = "plain text message".encode('utf-8')
        mock_pubsub_message.message_id = "test-message-id"
        mock_pubsub_message.publish_time = None
        mock_pubsub_message.ordering_key = ""
        mock_pubsub_message.attributes = {}

        message = subscriber._convert_pubsub_message(mock_pubsub_message)

        assert message.body == "plain text message"
        assert message.headers["message_id"] == "test-message-id"
        assert message.headers["publish_time"] is None
        assert message.headers["ordering_key"] is None

    @pytest.mark.asyncio
    async def test_project_id_extraction_from_subscription_path(self):
        """Test project ID extraction from subscription path"""
        subscriber = TestPubSubSubscriberImpl(
            subscription_path="projects/extracted-project/subscriptions/test-sub"
        )

        assert subscriber.project_id == "extracted-project"


class TestPubSubPublisher:
    """Test Pub/Sub Publisher functionality"""

    @pytest.mark.asyncio
    async def test_pubsub_publisher_initialization(self):
        """Test Pub/Sub publisher initialization"""
        gcp_config = GCPConfig(
            project_id="test-project",
            topic_name="test-topic"
        )

        publisher = TestPubSubPublisherImpl(
            topic_path="projects/test-project/topics/test-topic",
            project_id="test-project",
            gcp_config=gcp_config
        )

        assert publisher.topic_path == "projects/test-project/topics/test-topic"
        assert publisher.project_id == "test-project"
        assert publisher.gcp_config == gcp_config
        assert publisher.publisher_client is None

    @pytest.mark.asyncio
    async def test_pubsub_publisher_connection(self):
        """Test Pub/Sub publisher connection"""
        with patch("google.cloud.pubsub_v1.PublisherClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            publisher = TestPubSubPublisherImpl(
                topic_path="projects/test-project/topics/test-topic"
            )

            await publisher.connect()

            mock_client_class.assert_called_once()
            mock_client.get_topic.assert_called_once()
            assert publisher.publisher_client == mock_client

            await publisher.disconnect()
            assert publisher.publisher_client is None

    @pytest.mark.asyncio
    async def test_publish_message_dict(self):
        """Test publishing dictionary message to Pub/Sub"""
        with patch("google.cloud.pubsub_v1.PublisherClient") as mock_client_class:
            mock_client = MagicMock()
            mock_future = MagicMock()
            mock_future.result.return_value = "published-message-id"
            mock_client.publish.return_value = mock_future
            mock_client_class.return_value = mock_client

            publisher = TestPubSubPublisherImpl(
                topic_path="projects/test-project/topics/test-topic"
            )
            publisher.publisher_client = mock_client

            message_data = {"user_id": 123, "event": "user_created"}
            message_id = await publisher.publish_message(
                message=message_data,
                attributes={"source": "user-service", "version": "1.0"},
                ordering_key="user-123"
            )

            assert message_id == "published-message-id"
            mock_client.publish.assert_called_once_with(
                topic="projects/test-project/topics/test-topic",
                data=json.dumps(message_data).encode('utf-8'),
                ordering_key="user-123",
                source="user-service",
                version="1.0"
            )

    @pytest.mark.asyncio
    async def test_publish_message_string(self):
        """Test publishing string message to Pub/Sub"""
        with patch("google.cloud.pubsub_v1.PublisherClient") as mock_client_class:
            mock_client = MagicMock()
            mock_future = MagicMock()
            mock_future.result.return_value = "published-message-id"
            mock_client.publish.return_value = mock_future
            mock_client_class.return_value = mock_client

            publisher = TestPubSubPublisherImpl(
                topic_path="projects/test-project/topics/test-topic"
            )
            publisher.publisher_client = mock_client

            message_id = await publisher.publish_message("Hello World")

            assert message_id == "published-message-id"
            mock_client.publish.assert_called_once_with(
                topic="projects/test-project/topics/test-topic",
                data="Hello World".encode('utf-8'),
                ordering_key=None
            )

    @pytest.mark.asyncio
    async def test_publish_from_pythia_message(self):
        """Test publishing Pythia message to Pub/Sub"""
        with patch("google.cloud.pubsub_v1.PublisherClient") as mock_client_class:
            mock_client = MagicMock()
            mock_future = MagicMock()
            mock_future.result.return_value = "published-message-id"
            mock_client.publish.return_value = mock_future
            mock_client_class.return_value = mock_client

            publisher = TestPubSubPublisherImpl(
                topic_path="projects/test-project/topics/test-topic"
            )
            publisher.publisher_client = mock_client

            pythia_message = Message(
                body={"user_id": 123, "action": "created"},
                headers={"source": "user-service", "version": "1.0"}
            )

            message_id = await publisher.publish_from_pythia_message(
                message=pythia_message,
                ordering_key="user-123"
            )

            assert message_id == "published-message-id"
            mock_client.publish.assert_called_once_with(
                topic="projects/test-project/topics/test-topic",
                data=json.dumps({"user_id": 123, "action": "created"}).encode('utf-8'),
                ordering_key="user-123",
                source="user-service",
                version="1.0"
            )

    @pytest.mark.asyncio
    async def test_project_id_extraction_from_topic_path(self):
        """Test project ID extraction from topic path"""
        publisher = TestPubSubPublisherImpl(
            topic_path="projects/extracted-project/topics/test-topic"
        )

        assert publisher.project_id == "extracted-project"


@pytest.mark.asyncio
async def test_import_error_handling():
    """Test handling of missing google-cloud-pubsub dependency"""
    with patch("pythia.brokers.cloud.gcp.HAS_GCP_PUBSUB", False):
        with pytest.raises(ImportError) as exc_info:
            PubSubSubscriber(subscription_path="projects/test/subscriptions/test")

        assert "google-cloud-pubsub is required for GCP Pub/Sub support" in str(exc_info.value)
        assert "pip install 'pythia[gcp]'" in str(exc_info.value)


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
