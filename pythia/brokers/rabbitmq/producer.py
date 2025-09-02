"""
RabbitMQ Producer implementation for Pythia framework
"""

import json
from typing import Any, Dict, List, Optional, Union

import aio_pika
from aio_pika import Message as RabbitMessage, Channel
from aio_pika.abc import AbstractExchange, AbstractRobustConnection

from pythia.core.message import Message
from pythia.brokers.base.broker import MessageProducer
from pythia.config.rabbitmq import RabbitMQConfig
from pythia.logging.setup import get_pythia_logger


class RabbitMQProducer(MessageProducer):
    """
    RabbitMQ Producer implementation using aio-pika

    Example:
        producer = RabbitMQProducer(
            exchange="my-exchange",
            default_routing_key="my.routing.key"
        )

        await producer.send({"message": "Hello World"})
    """

    def __init__(
        self,
        exchange: str = "",
        default_routing_key: str = "",
        exchange_type: str = "direct",
        config: Optional[RabbitMQConfig] = None,
        **kwargs,
    ):
        """
        Initialize RabbitMQ producer

        Args:
            exchange: Exchange name to publish to
            default_routing_key: Default routing key for messages
            exchange_type: Exchange type (direct, topic, fanout, headers)
            config: RabbitMQ configuration
            **kwargs: Additional configuration
        """
        self.exchange_name = exchange
        self.default_routing_key = default_routing_key
        self.exchange_type = exchange_type

        # Simplified config creation
        if config:
            self.config = config
        else:
            self.config = RabbitMQConfig(
                url=kwargs.get("url", "amqp://guest:guest@localhost:5672/"),
                exchange=exchange,
                routing_key=default_routing_key,
            )

        self.logger = get_pythia_logger(f"RabbitMQProducer[{exchange or 'default'}]")

        # Connection management
        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[Channel] = None
        self.exchange: Optional[AbstractExchange] = None

    async def connect(self) -> None:
        """Establish connection to RabbitMQ"""
        if self.is_connected():
            return

        try:
            self.logger.info("Connecting to RabbitMQ", url=self.config.url)

            # Create connection
            self.connection = await aio_pika.connect_robust(
                self.config.url,
                heartbeat=self.config.heartbeat,
                connection_attempts=self.config.connection_attempts,
                retry_delay=self.config.retry_delay,
            )

            # Create channel
            self.channel = await self.connection.channel()

            # Declare exchange if specified
            if self.exchange_name:
                self.exchange = await self.channel.declare_exchange(
                    self.exchange_name,
                    type=aio_pika.ExchangeType(self.exchange_type),
                    durable=self.config.durable,
                )

                self.logger.info(
                    "Exchange declared",
                    exchange=self.exchange_name,
                    type=self.exchange_type,
                )
            else:
                # Use default exchange
                self.exchange = self.channel.default_exchange

            self.logger.info("Successfully connected to RabbitMQ")

        except Exception as e:
            self.logger.error("Failed to connect to RabbitMQ", error=str(e), url=self.config.url)
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close connection to RabbitMQ"""
        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                self.logger.info("Disconnected from RabbitMQ")
        except Exception as e:
            self.logger.warning("Error during disconnect", error=str(e))
        finally:
            self.connection = None
            self.channel = None
            self.exchange = None

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ"""
        return (
            self.connection is not None
            and not self.connection.is_closed
            and self.channel is not None
            and not self.channel.is_closed
        )

    async def send(
        self,
        message: Union[Dict[str, Any], Message, str, bytes],
        routing_key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> bool:
        """
        Send a single message to RabbitMQ

        Args:
            message: Message to send (dict, Message object, string, or bytes)
            routing_key: Routing key (overrides default)
            headers: Message headers
            **kwargs: Additional message properties

        Returns:
            bool: True if sent successfully
        """
        if not self.is_connected():
            await self.connect()

        if not self.exchange:
            raise RuntimeError("Exchange not initialized")

        try:
            # Convert to RabbitMQ message
            rabbit_message = self._create_rabbit_message(message, headers, **kwargs)

            # Determine routing key
            final_routing_key = routing_key or self.default_routing_key

            # Send message
            await self.exchange.publish(rabbit_message, routing_key=final_routing_key)

            self.logger.debug(
                "Message sent successfully",
                routing_key=final_routing_key,
                message_size=len(rabbit_message.body),
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to send message",
                error=str(e),
                routing_key=routing_key or self.default_routing_key,
            )
            raise

    async def send_batch(
        self,
        messages: List[Union[Dict[str, Any], Message, str, bytes]],
        routing_key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> int:
        """
        Send multiple messages to RabbitMQ

        Args:
            messages: List of messages to send
            routing_key: Routing key for all messages
            headers: Headers for all messages
            **kwargs: Additional message properties

        Returns:
            int: Number of messages sent successfully
        """
        if not messages:
            return 0

        if not self.is_connected():
            await self.connect()

        if not self.exchange:
            raise RuntimeError("Exchange not initialized")

        sent_count = 0
        final_routing_key = routing_key or self.default_routing_key

        self.logger.info(
            "Sending message batch", count=len(messages), routing_key=final_routing_key
        )

        for i, message in enumerate(messages):
            try:
                rabbit_message = self._create_rabbit_message(message, headers, **kwargs)
                await self.exchange.publish(rabbit_message, routing_key=final_routing_key)
                sent_count += 1

            except Exception as e:
                self.logger.error(
                    "Failed to send message in batch",
                    error=str(e),
                    message_index=i,
                    routing_key=final_routing_key,
                )
                # Continue with other messages

        self.logger.info(
            "Batch send completed",
            sent=sent_count,
            total=len(messages),
            success_rate=f"{sent_count / len(messages) * 100:.1f}%",
        )

        return sent_count

    def _create_rabbit_message(
        self,
        message: Union[Dict[str, Any], Message, str, bytes],
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> RabbitMessage:
        """Convert message to RabbitMQ message format"""

        # Handle different message types
        if isinstance(message, Message):
            # Pythia Message object
            body = message.body
            message_headers = {**(message.headers or {}), **(headers or {})}
            message_id = message.message_id
        elif isinstance(message, dict):
            # Dictionary - serialize to JSON
            body = json.dumps(message, ensure_ascii=False).encode("utf-8")
            message_headers = headers or {}
            message_id = None
        elif isinstance(message, str):
            # String
            body = message.encode("utf-8")
            message_headers = headers or {}
            message_id = None
        elif isinstance(message, bytes):
            # Raw bytes
            body = message
            message_headers = headers or {}
            message_id = None
        else:
            # Try to serialize as JSON
            try:
                body = json.dumps(message, ensure_ascii=False).encode("utf-8")
                message_headers = headers or {}
                message_id = None
            except TypeError as e:
                raise ValueError(f"Cannot serialize message of type {type(message)}: {e}")

        # Create RabbitMQ message
        rabbit_message = RabbitMessage(
            body=body,
            headers=message_headers,
            message_id=message_id,
            timestamp=kwargs.get("timestamp"),
            expiration=kwargs.get("expiration"),
            priority=kwargs.get("priority", 0),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            if kwargs.get("persistent", True)
            else aio_pika.DeliveryMode.NOT_PERSISTENT,
            correlation_id=kwargs.get("correlation_id"),
            reply_to=kwargs.get("reply_to"),
            content_type=kwargs.get("content_type", "application/json"),
            content_encoding=kwargs.get("content_encoding", "utf-8"),
        )

        return rabbit_message

    async def send_to_queue(
        self,
        queue_name: str,
        message: Union[Dict[str, Any], Message, str, bytes],
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> bool:
        """
        Send message directly to a queue (using default exchange)

        Args:
            queue_name: Target queue name
            message: Message to send
            headers: Message headers
            **kwargs: Additional message properties

        Returns:
            bool: True if sent successfully
        """
        if not self.is_connected():
            await self.connect()

        if not self.channel:
            raise RuntimeError("Channel not initialized")

        try:
            rabbit_message = self._create_rabbit_message(message, headers, **kwargs)

            # Use default exchange with queue name as routing key
            await self.channel.default_exchange.publish(rabbit_message, routing_key=queue_name)

            self.logger.debug(
                "Message sent to queue",
                queue=queue_name,
                message_size=len(rabbit_message.body),
            )

            return True

        except Exception as e:
            self.logger.error("Failed to send message to queue", error=str(e), queue=queue_name)
            raise

    async def health_check(self) -> bool:
        """Check if producer is healthy"""
        try:
            if not self.is_connected():
                return False

            # Try to declare a temporary queue to test connection
            if self.channel:
                temp_queue = await self.channel.declare_queue("", exclusive=True, auto_delete=True)
                await temp_queue.delete()
                return True

            return False

        except Exception as e:
            self.logger.warning("Health check failed", error=str(e))
            return False

    def __repr__(self) -> str:
        return (
            f"RabbitMQProducer(exchange={self.exchange_name}, "
            f"routing_key={self.default_routing_key})"
        )
