"""
Tests for testing utilities
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock

from pythia.core.message import Message
from pythia.core.worker import Worker
from pythia.utils.testing import (
    TestMessage,
    MockMessageBroker,
    MockMessageProducer,
    TestWorkerRunner,
    MockKafkaMessage,
    create_mock_kafka_messages,
    run_with_timeout
)


class TestTestMessage:
    """Test TestMessage dataclass"""

    def test_test_message_creation(self):
        """Test creating TestMessage"""
        msg = TestMessage(data="test", topic="my-topic", key="key1")
        assert msg.data == "test"
        assert msg.topic == "my-topic"
        assert msg.key == "key1"
        assert msg.headers is None
        assert msg.delay == 0.0

    def test_test_message_defaults(self):
        """Test TestMessage with defaults"""
        msg = TestMessage(data={"test": "data"})
        assert msg.data == {"test": "data"}
        assert msg.topic is None
        assert msg.key is None
        assert msg.headers is None
        assert msg.partition is None
        assert msg.delay == 0.0


class TestMockMessageBroker:
    """Test MockMessageBroker"""

    @pytest.fixture
    def broker(self):
        """Create mock broker"""
        return MockMessageBroker()

    def test_broker_initialization(self, broker):
        """Test broker initialization"""
        assert not broker.connected
        assert len(broker.messages) == 0
        assert len(broker.consumed_messages) == 0
        assert broker.connection_error is None
        assert broker.health_status is True

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, broker):
        """Test broker connection/disconnection"""
        await broker.connect()
        assert broker.connected
        
        await broker.disconnect()
        assert not broker.connected

    @pytest.mark.asyncio
    async def test_connect_with_error(self, broker):
        """Test connection with error"""
        error = ConnectionError("Test error")
        broker.set_connection_error(error)
        
        with pytest.raises(ConnectionError):
            await broker.connect()

    def test_add_message(self, broker):
        """Test adding messages"""
        broker.add_message("test data", topic="my-topic")
        assert len(broker.messages) == 1
        assert broker.messages[0].data == "test data"
        assert broker.messages[0].topic == "my-topic"

    def test_add_messages(self, broker):
        """Test adding multiple messages"""
        messages = [
            {"data": "msg1", "topic": "topic1"},
            {"data": "msg2", "topic": "topic2", "key": "key1"}
        ]
        broker.add_messages(messages)
        
        assert len(broker.messages) == 2
        assert broker.messages[0].data == "msg1"
        assert broker.messages[1].data == "msg2"
        assert broker.messages[1].key == "key1"

    @pytest.mark.asyncio
    async def test_consume_messages(self, broker):
        """Test consuming messages"""
        broker.add_message("data1", topic="topic1")
        broker.add_message("data2", topic="topic2", key="key1")
        
        await broker.connect()
        
        messages = []
        async for msg in broker.consume():
            messages.append(msg)
            if len(messages) >= 2:
                break
        
        assert len(messages) == 2
        assert messages[0].body == "data1"
        assert messages[0].topic == "topic1"
        assert messages[1].body == "data2"
        assert messages[1].message_id == "key1"

    @pytest.mark.asyncio
    async def test_consume_not_connected(self, broker):
        """Test consuming when not connected"""
        with pytest.raises(RuntimeError, match="Broker not connected"):
            async for msg in broker.consume():
                break

    @pytest.mark.asyncio
    async def test_consume_with_delay(self, broker):
        """Test consuming messages with delay"""
        broker.add_message("data1", delay=0.1)
        broker.set_consume_delay(0.05)
        
        await broker.connect()
        
        start_time = asyncio.get_event_loop().time()
        async for msg in broker.consume():
            break
        end_time = asyncio.get_event_loop().time()
        
        # Should have taken at least the delay time
        assert end_time - start_time >= 0.1

    @pytest.mark.asyncio
    async def test_health_check(self, broker):
        """Test health check"""
        assert not await broker.health_check()  # Not connected
        
        await broker.connect()
        assert await broker.health_check()
        
        broker.set_health_status(False)
        assert not await broker.health_check()


class TestMockMessageProducer:
    """Test MockMessageProducer"""

    @pytest.fixture
    def producer(self):
        """Create mock producer"""
        return MockMessageProducer()

    def test_producer_initialization(self, producer):
        """Test producer initialization"""
        assert not producer.connected
        assert len(producer.sent_messages) == 0
        assert len(producer.sent_batches) == 0

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, producer):
        """Test producer connection"""
        await producer.connect()
        assert producer.connected
        
        await producer.disconnect()
        assert not producer.connected

    @pytest.mark.asyncio
    async def test_send_message(self, producer):
        """Test sending messages"""
        await producer.connect()
        await producer.send("test data", topic="my-topic")
        
        assert len(producer.sent_messages) == 1
        sent = producer.sent_messages[0]
        assert sent["data"] == "test data"
        assert sent["kwargs"]["topic"] == "my-topic"

    @pytest.mark.asyncio
    async def test_send_not_connected(self, producer):
        """Test sending when not connected"""
        with pytest.raises(RuntimeError, match="Producer not connected"):
            await producer.send("data")

    @pytest.mark.asyncio
    async def test_send_with_error(self, producer):
        """Test sending with error"""
        error = Exception("Send error")
        producer.set_send_error(error)
        
        await producer.connect()
        with pytest.raises(Exception, match="Send error"):
            await producer.send("data")

    @pytest.mark.asyncio
    async def test_send_batch(self, producer):
        """Test sending batch"""
        await producer.connect()
        messages = ["msg1", "msg2", "msg3"]
        await producer.send_batch(messages, topic="batch-topic")
        
        assert len(producer.sent_messages) == 3
        assert len(producer.sent_batches) == 1
        assert producer.sent_batches[0] == messages

    def test_get_sent_data(self, producer):
        """Test getting sent data"""
        asyncio.run(self._send_test_data(producer))
        
        sent_data = producer.get_sent_data()
        assert sent_data == ["data1", "data2"]

    @pytest.mark.asyncio
    async def _send_test_data(self, producer):
        await producer.connect()
        await producer.send("data1")
        await producer.send("data2")

    def test_clear_sent_messages(self, producer):
        """Test clearing sent messages"""
        asyncio.run(self._send_test_data(producer))
        producer.clear_sent_messages()
        
        assert len(producer.sent_messages) == 0
        assert len(producer.sent_batches) == 0


class TestTestWorkerRunner:
    """Test TestWorkerRunner"""

    class SimpleWorker(Worker):
        def __init__(self):
            super().__init__()
            self.processed_data = []

        async def process(self, message):
            self.processed_data.append(message.body)
            return message.body.upper()

    @pytest.fixture
    def worker_with_broker(self):
        """Create worker with mock broker"""
        worker = self.SimpleWorker()
        broker = MockMessageBroker()
        broker.add_message("hello")
        broker.add_message("world")
        worker.broker = broker
        return worker

    @pytest.mark.asyncio
    async def test_runner_captures_results(self, worker_with_broker):
        """Test that runner captures processing results"""
        runner = TestWorkerRunner(worker_with_broker, timeout=2.0)
        
        # Mock the run method to avoid full startup
        async def mock_run():
            await worker_with_broker.broker.connect()
            async for message in worker_with_broker.broker.consume():
                await worker_with_broker.process(message)
        
        worker_with_broker.run = mock_run
        
        await runner.run_until_processed(2)
        
        assert len(runner.processed_messages) == 2
        runner.assert_messages_processed(2)
        runner.assert_errors_occurred(0)
        
        processed_data = runner.get_processed_data()
        assert processed_data == ["hello", "world"]

    @pytest.mark.asyncio
    async def test_runner_captures_errors(self):
        """Test that runner captures errors"""
        class FailingWorker(Worker):
            async def process(self, message):
                raise ValueError("Processing failed")
        
        worker = FailingWorker()
        broker = MockMessageBroker()
        broker.add_message("test")
        worker.broker = broker
        
        runner = TestWorkerRunner(worker, timeout=2.0)
        
        # Mock the run method
        async def mock_run():
            await broker.connect()
            async for message in broker.consume():
                await worker.process(message)
        
        worker.run = mock_run
        
        try:
            await runner.run_until_processed(1)
        except ValueError:
            pass  # Expected
        
        runner.assert_errors_occurred(1)
        assert len(runner.errors) == 1


class TestMockKafkaMessage:
    """Test MockKafkaMessage"""

    def test_kafka_message_creation(self):
        """Test creating Kafka message"""
        msg = MockKafkaMessage(
            topic="test-topic",
            partition=1,
            offset=100,
            key=b"test-key",
            value=b'{"data": "test"}'
        )
        
        assert msg.topic() == "test-topic"
        assert msg.partition() == 1
        assert msg.offset() == 100
        assert msg.key() == b"test-key"
        assert msg.value() == b'{"data": "test"}'

    def test_kafka_message_defaults(self):
        """Test Kafka message with defaults"""
        msg = MockKafkaMessage()
        
        assert msg.topic() == "test-topic"
        assert msg.partition() == 0
        assert msg.offset() == 10
        assert msg.key() is None
        assert msg.value() == b'{"test": "data"}'
        assert msg.headers() == []
        assert msg.error() is None

    def test_create_mock_kafka_messages(self):
        """Test creating multiple Kafka messages"""
        messages = create_mock_kafka_messages(3, "my-topic")
        
        assert len(messages) == 3
        assert all(msg.topic() == "my-topic" for msg in messages)
        
        # Check that messages have incremental offsets and unique keys
        for i, msg in enumerate(messages):
            assert msg.offset() == i
            assert msg.key() == f"key-{i}".encode()
            
            # Check message content
            data = json.loads(msg.value().decode())
            assert data["id"] == f"test-{i}"
            assert data["data"] == f"message {i}"


class TestUtilityFunctions:
    """Test utility functions"""

    @pytest.mark.asyncio
    async def test_run_with_timeout_success(self):
        """Test run_with_timeout with successful completion"""
        async def quick_task():
            await asyncio.sleep(0.1)
            return "success"
        
        result = await run_with_timeout(quick_task(), timeout=1.0)
        assert result == "success"

    @pytest.mark.asyncio
    async def test_run_with_timeout_failure(self):
        """Test run_with_timeout with timeout"""
        async def slow_task():
            await asyncio.sleep(2.0)
            return "too slow"
        
        with pytest.raises(asyncio.TimeoutError):
            await run_with_timeout(slow_task(), timeout=0.5)