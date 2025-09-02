"""
Tests for AWS cloud workers
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json

# Import the AWS workers
try:
    from pythia.brokers.cloud.aws import SQSConsumer, SNSProducer
    from pythia.config.cloud import AWSConfig
    from pythia.core.message import Message
    HAS_AWS_SUPPORT = True
except ImportError as e:
    HAS_AWS_SUPPORT = False

# Skip all tests if AWS dependencies are not available
pytestmark = pytest.mark.skipif(
    not HAS_AWS_SUPPORT,
    reason="AWS dependencies not installed. Install with: pip install 'pythia[aws]'"
)


# Test worker implementations
class TestSQSConsumerImpl(SQSConsumer):
    """Concrete SQS consumer for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_messages = []

    async def process_message(self, message):
        """Process a message from SQS"""
        self.processed_messages.append(message)
        return {"processed": True, "message_id": message.headers.get("MessageId")}


class TestSNSProducerImpl(SNSProducer):
    """Concrete SNS producer for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.published_messages = []

    async def process(self):
        """For testing, track process calls"""
        pass


class TestSQSConsumer:
    """Test SQS Consumer functionality"""

    @pytest.mark.asyncio
    async def test_sqs_consumer_initialization(self):
        """Test SQS consumer initialization"""
        aws_config = AWSConfig(
            region="us-west-2",
            access_key_id="test-key",
            secret_access_key="test-secret",
            queue_url="https://sqs.us-west-2.amazonaws.com/123/test-queue"
        )

        consumer = TestSQSConsumerImpl(
            queue_url="https://sqs.us-west-2.amazonaws.com/123/test-queue",
            aws_config=aws_config
        )

        assert consumer.queue_url == "https://sqs.us-west-2.amazonaws.com/123/test-queue"
        assert consumer.aws_config == aws_config
        assert consumer.sqs_client is None

    @pytest.mark.asyncio
    async def test_sqs_consumer_connection(self):
        """Test SQS consumer connection"""
        with patch("boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client

            consumer = TestSQSConsumerImpl(
                queue_url="https://sqs.us-west-2.amazonaws.com/123/test-queue"
            )

            await consumer.connect()

            mock_session.assert_called_once()
            assert consumer.sqs_client == mock_client

            await consumer.disconnect()
            assert consumer.sqs_client is None

    @pytest.mark.asyncio
    async def test_convert_sqs_message(self):
        """Test converting SQS message to Pythia Message"""
        consumer = TestSQSConsumerImpl(
            queue_url="https://sqs.us-west-2.amazonaws.com/123/test-queue"
        )

        # Test JSON message
        sqs_message = {
            "Body": json.dumps({"user_id": 123, "action": "created"}),
            "MessageId": "test-message-id",
            "ReceiptHandle": "test-receipt-handle",
            "MD5OfBody": "test-md5",
            "MessageAttributes": {
                "source": {
                    "StringValue": "user-service",
                    "DataType": "String"
                }
            }
        }

        message = consumer._convert_sqs_message(sqs_message)

        assert message.body == {"user_id": 123, "action": "created"}
        assert message.headers["MessageId"] == "test-message-id"
        assert message.headers["ReceiptHandle"] == "test-receipt-handle"
        assert message.headers["attr_source"] == "user-service"

    @pytest.mark.asyncio
    async def test_convert_sqs_message_plain_text(self):
        """Test converting plain text SQS message"""
        consumer = TestSQSConsumerImpl(
            queue_url="https://sqs.us-west-2.amazonaws.com/123/test-queue"
        )

        sqs_message = {
            "Body": "plain text message",
            "MessageId": "test-message-id",
            "ReceiptHandle": "test-receipt-handle"
        }

        message = consumer._convert_sqs_message(sqs_message)

        assert message.body == "plain text message"
        assert message.headers["MessageId"] == "test-message-id"

    @pytest.mark.asyncio
    async def test_delete_message(self):
        """Test deleting SQS message"""
        with patch("boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client

            consumer = TestSQSConsumerImpl(
                queue_url="https://sqs.us-west-2.amazonaws.com/123/test-queue"
            )
            consumer.sqs_client = mock_client

            await consumer._delete_message("test-receipt-handle")

            mock_client.delete_message.assert_called_once_with(
                QueueUrl="https://sqs.us-west-2.amazonaws.com/123/test-queue",
                ReceiptHandle="test-receipt-handle"
            )


class TestSNSProducer:
    """Test SNS Producer functionality"""

    @pytest.mark.asyncio
    async def test_sns_producer_initialization(self):
        """Test SNS producer initialization"""
        aws_config = AWSConfig(
            region="us-west-2",
            topic_arn="arn:aws:sns:us-west-2:123:test-topic"
        )

        producer = TestSNSProducerImpl(
            topic_arn="arn:aws:sns:us-west-2:123:test-topic",
            aws_config=aws_config
        )

        assert producer.topic_arn == "arn:aws:sns:us-west-2:123:test-topic"
        assert producer.aws_config == aws_config
        assert producer.sns_client is None

    @pytest.mark.asyncio
    async def test_sns_producer_connection(self):
        """Test SNS producer connection"""
        with patch("boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client

            producer = TestSNSProducerImpl(
                topic_arn="arn:aws:sns:us-west-2:123:test-topic"
            )

            await producer.connect()

            mock_session.assert_called_once()
            assert producer.sns_client == mock_client

            await producer.disconnect()
            assert producer.sns_client is None

    @pytest.mark.asyncio
    async def test_publish_message_dict(self):
        """Test publishing dictionary message to SNS"""
        with patch("boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_client.publish.return_value = {"MessageId": "published-message-id"}
            mock_session.return_value.client.return_value = mock_client

            producer = TestSNSProducerImpl(
                topic_arn="arn:aws:sns:us-west-2:123:test-topic"
            )
            producer.sns_client = mock_client

            message_data = {"user_id": 123, "event": "user_created"}
            message_id = await producer.publish_message(
                message=message_data,
                subject="User Event",
                message_attributes={"source": "user-service"}
            )

            assert message_id == "published-message-id"
            mock_client.publish.assert_called_once_with(
                TopicArn="arn:aws:sns:us-west-2:123:test-topic",
                Message=json.dumps(message_data),
                Subject="User Event",
                MessageAttributes={
                    "source": {
                        "DataType": "String",
                        "StringValue": "user-service"
                    }
                }
            )

    @pytest.mark.asyncio
    async def test_publish_message_string(self):
        """Test publishing string message to SNS"""
        with patch("boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_client.publish.return_value = {"MessageId": "published-message-id"}
            mock_session.return_value.client.return_value = mock_client

            producer = TestSNSProducerImpl(
                topic_arn="arn:aws:sns:us-west-2:123:test-topic"
            )
            producer.sns_client = mock_client

            message_id = await producer.publish_message("Hello World")

            assert message_id == "published-message-id"
            mock_client.publish.assert_called_once_with(
                TopicArn="arn:aws:sns:us-west-2:123:test-topic",
                Message="Hello World",
                Subject=None,
                MessageAttributes={}
            )

    @pytest.mark.asyncio
    async def test_publish_from_pythia_message(self):
        """Test publishing Pythia message to SNS"""
        with patch("boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_client.publish.return_value = {"MessageId": "published-message-id"}
            mock_session.return_value.client.return_value = mock_client

            producer = TestSNSProducerImpl(
                topic_arn="arn:aws:sns:us-west-2:123:test-topic"
            )
            producer.sns_client = mock_client

            pythia_message = Message(
                body={"user_id": 123, "action": "created"},
                headers={"source": "user-service", "version": "1.0"}
            )

            message_id = await producer.publish_from_pythia_message(
                message=pythia_message,
                subject="User Event"
            )

            assert message_id == "published-message-id"
            mock_client.publish.assert_called_once_with(
                TopicArn="arn:aws:sns:us-west-2:123:test-topic",
                Message=json.dumps({"user_id": 123, "action": "created"}),
                Subject="User Event",
                MessageAttributes={
                    "source": {
                        "DataType": "String",
                        "StringValue": "user-service"
                    },
                    "version": {
                        "DataType": "String",
                        "StringValue": "1.0"
                    }
                }
            )


@pytest.mark.asyncio
async def test_import_error_handling():
    """Test handling of missing boto3 dependency"""
    # This test would need to be run in an environment without boto3
    # For now, we just verify the error message format
    with patch("pythia.brokers.cloud.aws.HAS_BOTO3", False):
        with pytest.raises(ImportError) as exc_info:
            SQSConsumer(queue_url="test")

        assert "boto3 is required for AWS SQS support" in str(exc_info.value)
        assert "pip install 'pythia[aws]'" in str(exc_info.value)


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
