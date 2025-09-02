"""
Pytest configuration and fixtures for Pythia tests
"""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest
from unittest.mock import Mock

# Add pythia to path for testing
sys.path.insert(0, str(Path(__file__).parent.parent))

from pythia.config.base import WorkerConfig, LogConfig
from pythia.config.kafka import KafkaConfig
from pythia.core.message import Message


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_log_file():
    """Create a temporary log file for testing"""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as f:
        log_file_path = f.name

    yield log_file_path

    # Cleanup
    try:
        os.unlink(log_file_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def worker_config():
    """Basic worker configuration for testing"""
    return WorkerConfig(
        worker_name="test-worker",
        worker_id="test-worker-123",
        log_level="DEBUG",
        max_retries=2,
        retry_delay=0.1,  # Fast retries for tests
        batch_size=5,
    )


@pytest.fixture
def kafka_config():
    """Kafka configuration for testing"""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        topics=["test-topic"],
        auto_offset_reset="earliest",
    )


@pytest.fixture
def log_config(temp_log_file):
    """Log configuration for testing"""
    return LogConfig(
        level="DEBUG",
        format="json",
        file=temp_log_file,
    )


@pytest.fixture
def sample_message():
    """Sample message for testing"""
    return Message(
        body={"id": "test-123", "data": "test data"},
        message_id="msg-123",
        headers={"source": "test"},
        topic="test-topic",
        partition=0,
        offset=10,
    )


@pytest.fixture
def sample_messages():
    """Multiple sample messages for testing"""
    messages = []
    for i in range(5):
        messages.append(
            Message(
                body={"id": f"test-{i}", "data": f"test data {i}"},
                message_id=f"msg-{i}",
                headers={"source": "test", "batch": "true"},
                topic="test-topic",
                partition=0,
                offset=10 + i,
            )
        )
    return messages


@pytest.fixture
def mock_kafka_message():
    """Mock Kafka message object"""
    mock_msg = Mock()
    mock_msg.topic.return_value = "test-topic"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 10
    mock_msg.key.return_value = b"test-key"
    mock_msg.value.return_value = b'{"id": "test-123", "data": "test data"}'
    mock_msg.headers.return_value = [("source", b"test")]
    mock_msg.error.return_value = None
    return mock_msg


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer"""
    mock_consumer = Mock()
    mock_consumer.subscribe = Mock()
    mock_consumer.poll = Mock()
    mock_consumer.close = Mock()
    mock_consumer.commit = Mock()
    mock_consumer.stats.return_value = (
        '{"msg_cnt": 10, "brokers": {"broker1": {"state": "UP"}}}'
    )
    return mock_consumer


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer"""
    mock_producer = Mock()
    mock_producer.produce = Mock()
    mock_producer.flush = Mock(return_value=0)
    mock_producer.poll = Mock()
    mock_producer.list_topics = Mock()
    mock_producer.__len__ = Mock(return_value=0)
    return mock_producer


@pytest.fixture
def env_vars():
    """Environment variables for testing"""
    test_env = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_GROUP_ID": "test-group",
        "KAFKA_TOPICS": "test-topic",
        "PYTHIA_WORKER_NAME": "test-worker",
        "PYTHIA_LOG_LEVEL": "DEBUG",
    }

    # Store original values
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    yield test_env

    # Restore original values
    for key, original_value in original_env.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture
def clean_loguru():
    """Clean Loguru configuration before and after tests"""
    from loguru import logger

    # Remove all handlers before test
    logger.remove()

    yield

    # Clean up after test
    logger.remove()


@pytest.fixture
async def mock_async_consumer():
    """Mock async consumer for testing"""

    class MockAsyncConsumer:
        def __init__(self):
            self.connected = False
            self.messages = []

        async def connect(self):
            self.connected = True

        async def disconnect(self):
            self.connected = False

        async def consume(self):
            for message in self.messages:
                yield message

        async def health_check(self):
            return self.connected

        def add_message(self, message):
            self.messages.append(message)

    return MockAsyncConsumer()


@pytest.fixture
async def mock_async_producer():
    """Mock async producer for testing"""

    class MockAsyncProducer:
        def __init__(self):
            self.connected = False
            self.sent_messages = []

        async def connect(self):
            self.connected = True

        async def disconnect(self):
            self.connected = False

        async def send(self, data, **kwargs):
            if not self.connected:
                raise RuntimeError("Producer not connected")
            self.sent_messages.append({"data": data, "kwargs": kwargs})

        async def send_batch(self, messages, **kwargs):
            for message in messages:
                await self.send(message, **kwargs)

        async def health_check(self):
            return self.connected

    return MockAsyncProducer()


class AsyncMock:
    """Helper class for mocking async functions"""

    def __init__(self, return_value=None, side_effect=None):
        self.return_value = return_value
        self.side_effect = side_effect
        self.call_count = 0
        self.call_args_list = []

    async def __call__(self, *args, **kwargs):
        self.call_count += 1
        self.call_args_list.append((args, kwargs))

        if self.side_effect:
            if isinstance(self.side_effect, Exception):
                raise self.side_effect
            elif callable(self.side_effect):
                return await self.side_effect(*args, **kwargs)
            else:
                return self.side_effect

        return self.return_value


@pytest.fixture
def async_mock():
    """Factory for creating AsyncMock instances"""
    return AsyncMock


# Helper functions for tests
def assert_message_equal(msg1: Message, msg2: Message):
    """Assert that two messages are equal"""
    assert msg1.body == msg2.body
    assert msg1.message_id == msg2.message_id
    assert msg1.headers == msg2.headers
    assert msg1.topic == msg2.topic
    assert msg1.partition == msg2.partition
    assert msg1.offset == msg2.offset


def create_test_message(id_suffix: str = "123", **kwargs) -> Message:
    """Create a test message with default values"""
    defaults = {
        "body": {"id": f"test-{id_suffix}", "data": f"test data {id_suffix}"},
        "message_id": f"msg-{id_suffix}",
        "headers": {"source": "test"},
        "topic": "test-topic",
        "partition": 0,
        "offset": int(id_suffix) if id_suffix.isdigit() else 10,
    }
    defaults.update(kwargs)
    return Message(**defaults)


# Test markers
pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration
pytest.mark.e2e = pytest.mark.e2e
pytest.mark.kafka = pytest.mark.kafka
