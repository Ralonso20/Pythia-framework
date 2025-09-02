"""
Testing utilities for Pythia framework
"""

import asyncio
import json
from typing import Any, Dict, List, Optional, AsyncIterator
from dataclasses import dataclass

from ..core.message import Message
from ..brokers.base import MessageBroker, MessageProducer


@dataclass
class TestMessage:
    """Test message data"""

    data: Any
    topic: Optional[str] = None
    key: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    partition: Optional[int] = None
    delay: float = 0.0  # Delay before yielding message


class MockMessageBroker(MessageBroker):
    """Mock message broker for testing"""

    def __init__(self):
        self.connected = False
        self.messages: List[TestMessage] = []
        self.consumed_messages: List[Message] = []
        self.connection_error = None
        self.health_status = True
        self._consume_delay = 0.0

    def add_message(self, data: Any, **kwargs) -> None:
        """Add a message to be consumed"""
        self.messages.append(TestMessage(data=data, **kwargs))

    def add_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Add multiple messages"""
        for msg in messages:
            data = msg.pop("data")
            self.add_message(data, **msg)

    async def connect(self) -> None:
        """Mock connect"""
        if self.connection_error:
            raise self.connection_error
        self.connected = True

    async def disconnect(self) -> None:
        """Mock disconnect"""
        self.connected = False

    async def consume(self) -> AsyncIterator[Message]:
        """Mock consume"""
        if not self.connected:
            raise RuntimeError("Broker not connected")

        for test_msg in self.messages:
            if test_msg.delay > 0:
                await asyncio.sleep(test_msg.delay)

            message = Message(
                body=test_msg.data,
                topic=test_msg.topic or "test-topic",
                partition=test_msg.partition or 0,
                offset=len(self.consumed_messages),
                headers=test_msg.headers or {},
            )

            if test_msg.key:
                message.message_id = test_msg.key

            self.consumed_messages.append(message)
            yield message

            # Allow other coroutines to run
            await asyncio.sleep(self._consume_delay)

    async def health_check(self) -> bool:
        """Mock health check"""
        return self.connected and self.health_status

    def set_connection_error(self, error: Exception) -> None:
        """Set connection error for testing"""
        self.connection_error = error

    def set_health_status(self, healthy: bool) -> None:
        """Set health status for testing"""
        self.health_status = healthy

    def set_consume_delay(self, delay: float) -> None:
        """Set delay between message consumption"""
        self._consume_delay = delay


class MockMessageProducer(MessageProducer):
    """Mock message producer for testing"""

    def __init__(self):
        self.connected = False
        self.sent_messages: List[Dict[str, Any]] = []
        self.sent_batches: List[List[Any]] = []
        self.connection_error = None
        self.send_error = None
        self.health_status = True
        self._send_delay = 0.0

    async def connect(self) -> None:
        """Mock connect"""
        if self.connection_error:
            raise self.connection_error
        self.connected = True

    async def disconnect(self) -> None:
        """Mock disconnect"""
        self.connected = False

    async def send(self, data: Any, **kwargs) -> None:
        """Mock send"""
        if not self.connected:
            raise RuntimeError("Producer not connected")

        if self.send_error:
            raise self.send_error

        if self._send_delay > 0:
            await asyncio.sleep(self._send_delay)

        self.sent_messages.append(
            {
                "data": data,
                "kwargs": kwargs,
                "timestamp": asyncio.get_event_loop().time(),
            }
        )

    async def send_batch(self, messages: List[Any], **kwargs) -> None:
        """Mock send batch"""
        if not self.connected:
            raise RuntimeError("Producer not connected")

        for message in messages:
            await self.send(message, **kwargs)

        self.sent_batches.append(messages)

    async def health_check(self) -> bool:
        """Mock health check"""
        return self.connected and self.health_status

    def clear_sent_messages(self) -> None:
        """Clear sent messages history"""
        self.sent_messages.clear()
        self.sent_batches.clear()

    def get_sent_data(self) -> List[Any]:
        """Get list of sent data only"""
        return [msg["data"] for msg in self.sent_messages]

    def set_connection_error(self, error: Exception) -> None:
        """Set connection error for testing"""
        self.connection_error = error

    def set_send_error(self, error: Exception) -> None:
        """Set send error for testing"""
        self.send_error = error

    def set_health_status(self, healthy: bool) -> None:
        """Set health status for testing"""
        self.health_status = healthy

    def set_send_delay(self, delay: float) -> None:
        """Set delay for send operations"""
        self._send_delay = delay


class TestWorkerRunner:
    """Test runner for workers with timeout and assertion support"""

    def __init__(self, worker, timeout: float = 5.0):
        self.worker = worker
        self.timeout = timeout
        self.processed_messages = []
        self.errors = []

        # Patch the process method to capture results
        original_process = worker.process

        async def capturing_process(message):
            try:
                result = await original_process(message)
                self.processed_messages.append(
                    {"message": message, "result": result, "success": True}
                )
                return result
            except Exception as e:
                self.errors.append({"message": message, "error": e, "success": False})
                raise

        worker.process = capturing_process

    async def run_until_processed(self, expected_count: int) -> None:
        """Run worker until expected number of messages processed"""

        async def run_worker():
            await self.worker.run()

        async def wait_for_messages():
            while len(self.processed_messages) + len(self.errors) < expected_count:
                await asyncio.sleep(0.1)
            self.worker.lifecycle.request_shutdown()

        # Run both tasks concurrently
        await asyncio.wait_for(
            asyncio.gather(run_worker(), wait_for_messages()),
            timeout=self.timeout,
        )

    def assert_messages_processed(self, count: int) -> None:
        """Assert number of messages processed successfully"""
        successful = [msg for msg in self.processed_messages if msg["success"]]
        assert len(successful) == count, (
            f"Expected {count} messages processed, got {len(successful)}"
        )

    def assert_errors_occurred(self, count: int) -> None:
        """Assert number of errors occurred"""
        assert len(self.errors) == count, f"Expected {count} errors, got {len(self.errors)}"

    def get_processed_data(self) -> List[Any]:
        """Get list of processed message data"""
        return [msg["message"].body for msg in self.processed_messages if msg["success"]]


class MockKafkaMessage:
    """Mock Kafka message for testing"""

    def __init__(
        self,
        topic: str = "test-topic",
        partition: int = 0,
        offset: int = 10,
        key: Optional[bytes] = None,
        value: Optional[bytes] = None,
        headers: Optional[List[tuple]] = None,
        error=None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value or b'{"test": "data"}'
        self._headers = headers or []
        self._error = error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def error(self):
        return self._error


def create_mock_kafka_messages(count: int, topic: str = "test-topic") -> List[MockKafkaMessage]:
    """Create multiple mock Kafka messages"""
    messages = []
    for i in range(count):
        data = {"id": f"test-{i}", "data": f"message {i}"}
        messages.append(
            MockKafkaMessage(
                topic=topic,
                offset=i,
                key=f"key-{i}".encode(),
                value=json.dumps(data).encode(),
                headers=[("source", b"test")],
            )
        )
    return messages


async def run_with_timeout(coro, timeout: float = 5.0):
    """Run coroutine with timeout"""
    return await asyncio.wait_for(coro, timeout=timeout)
