"""
Unit tests for pythia.core.message module
"""

import pytest
from datetime import datetime
from unittest.mock import Mock
from uuid import uuid4

from pythia.core.message import Message, MessageProcessor, BatchProcessor


class TestMessage:
    """Test cases for Message class"""

    @pytest.mark.unit
    def test_message_creation_with_defaults(self):
        """Test message creation with default values"""
        message = Message(body="test message")

        assert message.body == "test message"
        assert message.message_id is not None
        assert isinstance(message.timestamp, datetime)
        assert message.headers == {}
        assert message.retry_count == 0
        assert message.max_retries == 3
        assert message.topic is None
        assert message.queue is None

    @pytest.mark.unit
    def test_message_creation_with_custom_values(self):
        """Test message creation with custom values"""
        custom_id = str(uuid4())
        custom_time = datetime.now()
        custom_headers = {"source": "test", "version": "1.0"}

        message = Message(
            body={"data": "test"},
            message_id=custom_id,
            timestamp=custom_time,
            headers=custom_headers,
            topic="test-topic",
            partition=1,
            offset=100,
            max_retries=5,
        )

        assert message.body == {"data": "test"}
        assert message.message_id == custom_id
        assert message.timestamp == custom_time
        assert message.headers == custom_headers
        assert message.topic == "test-topic"
        assert message.partition == 1
        assert message.offset == 100
        assert message.max_retries == 5

    @pytest.mark.unit
    def test_message_with_different_body_types(self):
        """Test message with different body types"""
        # String body
        msg1 = Message(body="string message")
        assert msg1.body == "string message"

        # Bytes body
        msg2 = Message(body=b"bytes message")
        assert msg2.body == b"bytes message"

        # Dict body
        msg3 = Message(body={"key": "value"})
        assert msg3.body == {"key": "value"}

        # List body
        msg4 = Message(body=[1, 2, 3])
        assert msg4.body == [1, 2, 3]

        # Integer body
        msg5 = Message(body=42)
        assert msg5.body == 42

    @pytest.mark.unit
    def test_to_dict_conversion(self):
        """Test message to dictionary conversion"""
        message = Message(
            body={"test": "data"},
            message_id="test-123",
            headers={"source": "unit-test"},
            topic="test-topic",
            partition=0,
            offset=10,
        )

        result = message.to_dict()

        assert result["message_id"] == "test-123"
        assert result["body"] == {"test": "data"}
        assert result["headers"] == {"source": "unit-test"}
        assert result["topic"] == "test-topic"
        assert result["partition"] == 0
        assert result["offset"] == 10
        assert result["retry_count"] == 0
        assert result["max_retries"] == 3
        assert isinstance(result["timestamp"], str)  # Should be ISO format

    @pytest.mark.unit
    def test_retry_logic(self):
        """Test message retry logic"""
        message = Message(body="test", max_retries=2)

        # Initially should allow retry
        assert message.should_retry() is True
        assert message.retry_count == 0

        # After first retry
        message.increment_retry()
        assert message.retry_count == 1
        assert message.should_retry() is True

        # After second retry
        message.increment_retry()
        assert message.retry_count == 2
        assert message.should_retry() is False

        # After exceeding max retries
        message.increment_retry()
        assert message.retry_count == 3
        assert message.should_retry() is False

    @pytest.mark.unit
    def test_from_kafka_factory_method(self):
        """Test creating message from Kafka message"""
        # Mock Kafka message
        kafka_msg = Mock()
        kafka_msg.value.return_value = b'{"data": "test"}'
        kafka_msg.key.return_value = b"test-key"
        kafka_msg.headers.return_value = [("header1", b"value1")]
        kafka_msg.partition.return_value = 2
        kafka_msg.offset.return_value = 150

        message = Message.from_kafka(kafka_msg, topic="test-topic")

        assert message.body == b'{"data": "test"}'
        assert message.message_id == "test-key"
        assert message.headers == {"header1": b"value1"}
        assert message.topic == "test-topic"
        assert message.partition == 2
        assert message.offset == 150

    @pytest.mark.unit
    def test_from_kafka_with_no_key(self):
        """Test creating message from Kafka message with no key"""
        kafka_msg = Mock()
        kafka_msg.value.return_value = "test data"
        kafka_msg.key.return_value = None
        kafka_msg.headers.return_value = None
        kafka_msg.partition.return_value = 0
        kafka_msg.offset.return_value = 0

        message = Message.from_kafka(kafka_msg, topic="test-topic")

        assert message.body == "test data"
        assert message.message_id is not None  # Should generate UUID
        assert len(message.message_id) > 10  # UUID format
        assert message.headers == {}

    @pytest.mark.unit
    def test_from_rabbitmq_factory_method(self):
        """Test creating message from RabbitMQ message"""
        # Mock RabbitMQ message
        rabbit_msg = Mock()
        rabbit_msg.body = b'{"message": "test"}'
        rabbit_msg.message_id = "rabbitmq-123"
        rabbit_msg.headers = {"content_type": "application/json"}
        rabbit_msg.routing_key = "test.routing.key"
        rabbit_msg.exchange = "test-exchange"

        message = Message.from_rabbitmq(rabbit_msg, queue="test-queue")

        assert message.body == b'{"message": "test"}'
        assert message.message_id == "rabbitmq-123"
        assert message.headers == {"content_type": "application/json"}
        assert message.queue == "test-queue"
        assert message.routing_key == "test.routing.key"
        assert message.exchange == "test-exchange"

    @pytest.mark.unit
    def test_from_rabbitmq_with_no_message_id(self):
        """Test creating message from RabbitMQ message with no message ID"""
        rabbit_msg = Mock()
        rabbit_msg.body = "test data"
        rabbit_msg.message_id = None
        rabbit_msg.headers = None
        rabbit_msg.routing_key = "test.key"
        rabbit_msg.exchange = "test-exchange"

        message = Message.from_rabbitmq(rabbit_msg, queue="test-queue")

        assert message.body == "test data"
        assert message.message_id is not None  # Should generate UUID
        assert message.headers == {}

    @pytest.mark.unit
    def test_from_redis_factory_method(self):
        """Test creating message from Redis stream message"""
        redis_data = {
            "id": "1634567890123-0",
            "data": {"user_id": 123, "action": "login"},
            "headers": {"source": "auth-service"},
        }

        message = Message.from_redis(redis_data, stream="user-events")

        assert message.body == {"user_id": 123, "action": "login"}
        assert message.message_id == "1634567890123-0"
        assert message.headers == {"source": "auth-service"}
        assert message.queue == "user-events"

    @pytest.mark.unit
    def test_from_redis_with_missing_fields(self):
        """Test creating message from Redis with missing fields"""
        redis_data = {"some": "data"}  # Missing id, data, headers

        message = Message.from_redis(redis_data, stream="test-stream")

        assert message.body == {}  # Default empty dict for missing 'data'
        assert message.message_id is not None  # Should generate UUID
        assert message.headers == {}
        assert message.queue == "test-stream"

    @pytest.mark.unit
    def test_message_with_all_broker_fields(self):
        """Test message with fields from all brokers"""
        message = Message(
            body="universal message",
            # Kafka fields
            topic="kafka-topic",
            partition=1,
            offset=100,
            # RabbitMQ fields
            queue="rabbitmq-queue",
            routing_key="rabbitmq.key",
            exchange="rabbitmq-exchange",
            delivery_tag=123,
            # Redis fields
            stream_id="redis-stream-123",
            channel="redis-channel",
            pattern="redis.*",
        )

        # All fields should be preserved
        assert message.topic == "kafka-topic"
        assert message.partition == 1
        assert message.offset == 100
        assert message.queue == "rabbitmq-queue"
        assert message.routing_key == "rabbitmq.key"
        assert message.exchange == "rabbitmq-exchange"
        assert message.delivery_tag == 123
        assert message.stream_id == "redis-stream-123"
        assert message.channel == "redis-channel"
        assert message.pattern == "redis.*"

    @pytest.mark.unit
    def test_message_headers_manipulation(self):
        """Test header manipulation"""
        message = Message(body="test")

        # Initially empty
        assert message.headers == {}

        # Add headers
        message.headers["source"] = "test"
        message.headers["timestamp"] = "2024-01-01"

        assert message.headers["source"] == "test"
        assert message.headers["timestamp"] == "2024-01-01"

        # Modify headers
        message.headers["source"] = "modified"
        assert message.headers["source"] == "modified"

    @pytest.mark.unit
    def test_message_equality_and_hashing(self):
        """Test message equality (dataclass behavior)"""
        # Use same timestamp for both messages
        timestamp = datetime.now()
        msg1 = Message(body="test", message_id="123", timestamp=timestamp)
        msg2 = Message(body="test", message_id="123", timestamp=timestamp)
        msg3 = Message(body="different", message_id="123", timestamp=timestamp)

        # Same content should be equal
        assert msg1 == msg2

        # Different content should not be equal
        assert msg1 != msg3

    @pytest.mark.unit
    def test_message_immutable_after_creation(self):
        """Test message fields can be modified (mutable dataclass)"""
        message = Message(body="original")

        # Should be able to modify
        message.body = "modified"
        assert message.body == "modified"

        # Headers should be modifiable
        message.headers["new"] = "value"
        assert message.headers["new"] == "value"

    @pytest.mark.unit
    def test_large_message_body(self):
        """Test handling of large message bodies"""
        large_data = {"data": "x" * 10000}  # 10K characters
        message = Message(body=large_data)

        assert message.body == large_data
        assert len(str(message.body["data"])) == 10000

    @pytest.mark.unit
    def test_message_type_fields_redis_specific(self):
        """Test Redis-specific fields"""
        message = Message(
            body="redis message",
            channel="notifications",
            pattern="notify.*",
            stream_id="stream-123-0",
        )

        result = message.to_dict()
        assert result["channel"] == "notifications"
        assert result["pattern"] == "notify.*"
        assert result["stream_id"] == "stream-123-0"

    @pytest.mark.unit
    def test_message_raw_storage(self):
        """Test raw message storage fields"""
        raw_msg = {"original": "kafka_message_object"}
        raw_data = b"raw_bytes_data"

        message = Message(
            body="processed data", _raw_message=raw_msg, _raw_data=raw_data
        )

        assert message._raw_message == raw_msg
        assert message._raw_data == raw_data


class MockMessageProcessor(MessageProcessor):
    """Mock message processor for testing"""

    def __init__(self):
        self.processed_messages = []
        self.handled_errors = []

    async def process(self, message: Message) -> str:
        self.processed_messages.append(message)
        return f"processed: {message.message_id}"

    async def handle_error(self, message: Message, error: Exception) -> bool:
        self.handled_errors.append((message, error))
        return True


class MockBatchProcessor(BatchProcessor):
    """Mock batch processor for testing"""

    def __init__(self):
        self.processed_batches = []
        self.handled_errors = []

    async def process_batch(self, messages: list[Message]) -> list[str]:
        self.processed_batches.append(messages)
        return [f"processed: {msg.message_id}" for msg in messages]

    async def handle_batch_error(
        self, messages: list[Message], error: Exception
    ) -> bool:
        self.handled_errors.append((messages, error))
        return True


class TestMessageProcessor:
    """Test cases for MessageProcessor abstract class"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_message_processor_interface(self):
        """Test MessageProcessor interface implementation"""
        processor = MockMessageProcessor()
        message = Message(body="test", message_id="test-123")

        # Test process method
        result = await processor.process(message)
        assert result == "processed: test-123"
        assert len(processor.processed_messages) == 1
        assert processor.processed_messages[0] == message

        # Test error handling
        error = ValueError("test error")
        handled = await processor.handle_error(message, error)
        assert handled is True
        assert len(processor.handled_errors) == 1
        assert processor.handled_errors[0] == (message, error)

    @pytest.mark.unit
    def test_message_processor_abstract_methods(self):
        """Test that MessageProcessor cannot be instantiated directly"""
        with pytest.raises(TypeError):
            MessageProcessor()


class TestBatchProcessor:
    """Test cases for BatchProcessor abstract class"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_batch_processor_interface(self):
        """Test BatchProcessor interface implementation"""
        processor = MockBatchProcessor()
        messages = [
            Message(body="test1", message_id="msg-1"),
            Message(body="test2", message_id="msg-2"),
            Message(body="test3", message_id="msg-3"),
        ]

        # Test batch processing
        results = await processor.process_batch(messages)
        expected = ["processed: msg-1", "processed: msg-2", "processed: msg-3"]
        assert results == expected
        assert len(processor.processed_batches) == 1
        assert processor.processed_batches[0] == messages

        # Test batch error handling
        error = RuntimeError("batch error")
        handled = await processor.handle_batch_error(messages, error)
        assert handled is True
        assert len(processor.handled_errors) == 1
        assert processor.handled_errors[0] == (messages, error)

    @pytest.mark.unit
    def test_batch_processor_abstract_methods(self):
        """Test that BatchProcessor cannot be instantiated directly"""
        with pytest.raises(TypeError):
            BatchProcessor()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_empty_batch_processing(self):
        """Test processing empty batch"""
        processor = MockBatchProcessor()
        results = await processor.process_batch([])
        assert results == []
        assert len(processor.processed_batches) == 1
        assert processor.processed_batches[0] == []


class TestMessageEdgeCases:
    """Test edge cases and error conditions"""

    @pytest.mark.unit
    def test_message_with_none_values(self):
        """Test message creation with None values"""
        message = Message(
            body=None,
            headers=None,
        )

        assert message.body is None
        assert message.headers is None  # None is allowed, factory won't trigger

    @pytest.mark.unit
    def test_message_serialization_with_complex_body(self):
        """Test message with complex nested structures"""
        complex_body = {
            "users": [
                {"id": 1, "data": {"name": "User 1", "active": True}},
                {"id": 2, "data": {"name": "User 2", "active": False}},
            ],
            "metadata": {
                "count": 2,
                "timestamp": "2024-01-01T00:00:00Z",
                "nested": {"deep": {"value": 42}},
            },
        }

        message = Message(body=complex_body)
        result = message.to_dict()

        assert result["body"] == complex_body
        assert result["body"]["users"][0]["data"]["name"] == "User 1"
        assert result["body"]["metadata"]["nested"]["deep"]["value"] == 42

    @pytest.mark.unit
    def test_message_timestamp_format(self):
        """Test timestamp is properly formatted in to_dict"""
        message = Message(body="test")
        result = message.to_dict()

        # Should be ISO format string
        assert isinstance(result["timestamp"], str)
        assert "T" in result["timestamp"]  # ISO format has T separator

        # Should be parseable back to datetime
        parsed = datetime.fromisoformat(result["timestamp"])
        assert isinstance(parsed, datetime)
