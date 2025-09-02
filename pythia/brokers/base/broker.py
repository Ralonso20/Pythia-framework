"""
Abstract base classes for message brokers
"""

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List

from ...core.message import Message


class MessageBroker(ABC):
    """Abstract base class for message brokers (consumers)"""

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the message broker"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the message broker"""
        pass

    @abstractmethod
    async def consume(self) -> AsyncIterator[Message]:
        """Consume messages from the broker"""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the broker connection is healthy"""
        pass

    def get_stats(self) -> Dict[str, Any]:
        """Get broker statistics"""
        return {}


class MessageProducer(ABC):
    """Abstract base class for message producers"""

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the message broker"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the message broker"""
        pass

    @abstractmethod
    async def send(self, data: Any, **kwargs) -> None:
        """Send a message"""
        pass

    @abstractmethod
    async def send_batch(self, messages: List[Any], **kwargs) -> None:
        """Send a batch of messages"""
        pass

    async def send_delayed(self, data: Any, delay_seconds: int = 0, **kwargs) -> None:
        """Send a delayed message (default implementation uses immediate send)"""
        if delay_seconds > 0:
            import asyncio

            await asyncio.sleep(delay_seconds)
        await self.send(data, **kwargs)

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the producer connection is healthy"""
        pass

    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        return {}
