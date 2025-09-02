"""
Message abstraction layer for Pythia framework
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Union
from uuid import uuid4


@dataclass
class Message:
    """
    Universal message abstraction that works across all brokers
    """

    # Core message data
    body: Union[str, bytes, dict, Any]

    # Message metadata
    message_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    headers: Dict[str, Any] = field(default_factory=dict)

    # Broker-specific metadata (optional)
    topic: Optional[str] = None
    queue: Optional[str] = None
    routing_key: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    exchange: Optional[str] = None

    # Redis-specific fields
    stream_id: Optional[str] = None
    channel: Optional[str] = None
    pattern: Optional[str] = None

    # RabbitMQ-specific fields
    delivery_tag: Optional[int] = None

    # Raw message storage for broker-specific operations
    _raw_message: Optional[Any] = None
    _raw_data: Optional[Any] = None

    # Processing metadata
    retry_count: int = 0
    max_retries: int = 3

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary"""
        return {
            "message_id": self.message_id,
            "timestamp": self.timestamp.isoformat(),
            "headers": self.headers,
            "body": self.body,
            "topic": self.topic,
            "queue": self.queue,
            "routing_key": self.routing_key,
            "partition": self.partition,
            "offset": self.offset,
            "exchange": self.exchange,
            "stream_id": self.stream_id,
            "channel": self.channel,
            "pattern": self.pattern,
            "delivery_tag": self.delivery_tag,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
        }

    def should_retry(self) -> bool:
        """Check if message should be retried"""
        return self.retry_count < self.max_retries

    def increment_retry(self) -> None:
        """Increment retry counter"""
        self.retry_count += 1

    @classmethod
    def from_kafka(cls, kafka_msg, topic: str) -> "Message":
        """Create message from Kafka message"""
        return cls(
            body=kafka_msg.value(),
            message_id=kafka_msg.key().decode() if kafka_msg.key() else str(uuid4()),
            headers=dict(kafka_msg.headers() or []),
            topic=topic,
            partition=kafka_msg.partition(),
            offset=kafka_msg.offset(),
        )

    @classmethod
    def from_rabbitmq(cls, rabbit_msg, queue: str) -> "Message":
        """Create message from RabbitMQ message"""
        return cls(
            body=rabbit_msg.body,
            message_id=rabbit_msg.message_id or str(uuid4()),
            headers=dict(rabbit_msg.headers or {}),
            queue=queue,
            routing_key=rabbit_msg.routing_key,
            exchange=rabbit_msg.exchange,
        )

    @classmethod
    def from_redis(cls, redis_data: Dict[str, Any], stream: str) -> "Message":
        """Create message from Redis stream message"""
        return cls(
            body=redis_data.get("data", {}),
            message_id=redis_data.get("id", str(uuid4())),
            headers=redis_data.get("headers", {}),
            queue=stream,
        )


class MessageProcessor(ABC):
    """Abstract base for message processors"""

    @abstractmethod
    async def process(self, message: Message) -> Any:
        """Process a single message"""
        pass

    @abstractmethod
    async def handle_error(self, message: Message, error: Exception) -> bool:
        """
        Handle processing error
        Returns True if error was handled, False to re-raise
        """
        pass


class BatchProcessor(ABC):
    """Abstract base for batch processors"""

    @abstractmethod
    async def process_batch(self, messages: list[Message]) -> list[Any]:
        """Process a batch of messages"""
        pass

    @abstractmethod
    async def handle_batch_error(self, messages: list[Message], error: Exception) -> bool:
        """Handle batch processing error"""
        pass
