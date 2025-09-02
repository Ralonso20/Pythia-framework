"""
RabbitMQ Consumer implementation for Pythia framework
"""

from typing import AsyncIterator, Dict, Any, Optional

import aio_pika
from aio_pika import Channel, Queue
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

from pythia.core.message import Message
from pythia.brokers.base.broker import MessageBroker
from pythia.config.rabbitmq import RabbitMQConfig
from pythia.logging.setup import get_pythia_logger


class RabbitMQConsumer(MessageBroker):
    """
    RabbitMQ Consumer implementation using aio-pika

    Example:
        consumer = RabbitMQConsumer(
            queue="my-queue",
            exchange="my-exchange",
            routing_key="my.routing.key"
        )

        async for message in consumer.consume():
            print(f"Received: {message.body}")
    """

    def __init__(
        self,
        queue: str,
        exchange: Optional[str] = None,
        routing_key: Optional[str] = None,
        exchange_type: str = "direct",
        config: Optional[RabbitMQConfig] = None,
        **kwargs,
    ):
        """
        Initialize RabbitMQ consumer

        Args:
            queue: Queue name to consume from
            exchange: Exchange name (optional)
            routing_key: Routing key for binding (optional)
            exchange_type: Exchange type (direct, topic, fanout, headers)
            config: RabbitMQ configuration
            **kwargs: Additional configuration
        """
        self.queue_name = queue
        self.exchange_name = exchange
        self.routing_key = routing_key or queue
        self.exchange_type = exchange_type

        # Simplified config creation
        if config:
            self.config = config
        else:
            self.config = RabbitMQConfig(
                url=kwargs.get("url", "amqp://guest:guest@localhost:5672/"),
                queue=queue,
                exchange=exchange,
                routing_key=self.routing_key,
            )

        self.logger = get_pythia_logger(f"RabbitMQConsumer[{queue}]")

        # Connection management
        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[Channel] = None
        self.queue: Optional[Queue] = None

        self._consuming = False

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
            await self.channel.set_qos(prefetch_count=self.config.prefetch_count)

            # Declare exchange if specified
            exchange = None
            if self.exchange_name:
                exchange = await self.channel.declare_exchange(
                    self.exchange_name,
                    type=aio_pika.ExchangeType(self.exchange_type),
                    durable=self.config.durable,
                )

                self.logger.info(
                    "Exchange declared",
                    exchange=self.exchange_name,
                    type=self.exchange_type,
                )

            # Declare queue
            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=self.config.durable,
            )

            # Bind queue to exchange if specified
            if exchange and self.routing_key:
                await self.queue.bind(exchange, routing_key=self.routing_key)
                self.logger.info(
                    "Queue bound to exchange",
                    queue=self.queue_name,
                    exchange=self.exchange_name,
                    routing_key=self.routing_key,
                )

            self.logger.info(
                "Successfully connected to RabbitMQ",
                queue=self.queue_name,
                exchange=self.exchange_name,
            )

        except Exception as e:
            self.logger.error("Failed to connect to RabbitMQ", error=str(e), url=self.config.url)
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close connection to RabbitMQ"""
        self._consuming = False

        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                self.logger.info("Disconnected from RabbitMQ")
        except Exception as e:
            self.logger.warning("Error during disconnect", error=str(e))
        finally:
            self.connection = None
            self.channel = None
            self.queue = None

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ"""
        return (
            self.connection is not None
            and not self.connection.is_closed
            and self.channel is not None
            and not self.channel.is_closed
        )

    async def consume(self) -> AsyncIterator[Message]:
        """
        Consume messages from RabbitMQ queue

        Yields:
            Message: Pythia Message objects
        """
        if not self.is_connected():
            await self.connect()

        if not self.queue:
            raise RuntimeError("Queue not initialized")

        self._consuming = True
        self.logger.info("Starting message consumption", queue=self.queue_name)

        try:
            async with self.queue.iterator() as queue_iter:
                async for rabbit_message in queue_iter:
                    if not self._consuming:
                        break

                    try:
                        # Convert RabbitMQ message to Pythia message
                        message = self._convert_message(rabbit_message)

                        self.logger.debug(
                            "Message received",
                            message_id=message.message_id,
                            routing_key=rabbit_message.routing_key,
                        )

                        yield message

                        # Auto-acknowledge if enabled
                        if self.config.auto_ack:
                            rabbit_message.ack()

                    except Exception as e:
                        self.logger.error(
                            "Error processing message",
                            error=str(e),
                            routing_key=getattr(rabbit_message, "routing_key", "unknown"),
                        )
                        # Reject message without requeue to prevent infinite loop
                        rabbit_message.reject(requeue=False)

        except Exception as e:
            self.logger.error("Error in message consumption", error=str(e))
            raise
        finally:
            self._consuming = False
            self.logger.info("Stopped message consumption")

    def _convert_message(self, rabbit_message: AbstractIncomingMessage) -> Message:
        """Convert RabbitMQ message to Pythia Message"""
        try:
            # Parse message body
            body = rabbit_message.body
            if body:
                try:
                    import json

                    # Try to decode as JSON first
                    body = json.loads(body.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Fall back to string or bytes
                    try:
                        body = body.decode("utf-8")
                    except UnicodeDecodeError:
                        body = body

            # Extract headers
            headers = {}
            if rabbit_message.headers:
                headers = dict(rabbit_message.headers)

            # Create message
            message = Message(
                body=body,
                message_id=rabbit_message.message_id or rabbit_message.correlation_id,
                headers=headers,
                queue=self.queue_name,
                routing_key=rabbit_message.routing_key,
                exchange=rabbit_message.exchange,
                delivery_tag=rabbit_message.delivery_tag,
                timestamp=rabbit_message.timestamp,
                _raw_message=rabbit_message,  # Store for ACK/NACK
            )

            return message

        except Exception as e:
            self.logger.error(
                "Failed to convert RabbitMQ message",
                error=str(e),
                message_id=getattr(rabbit_message, "message_id", "unknown"),
            )
            raise

    async def acknowledge(self, message: Message) -> None:
        """Acknowledge message processing"""
        if hasattr(message, "_raw_message") and message._raw_message:
            try:
                message._raw_message.ack()
                self.logger.debug("Message acknowledged", message_id=message.message_id)
            except Exception as e:
                self.logger.error("Failed to acknowledge message", error=str(e))

    async def reject(self, message: Message, requeue: bool = False) -> None:
        """Reject message"""
        if hasattr(message, "_raw_message") and message._raw_message:
            try:
                message._raw_message.reject(requeue=requeue)
                self.logger.debug(
                    "Message rejected", message_id=message.message_id, requeue=requeue
                )
            except Exception as e:
                self.logger.error("Failed to reject message", error=str(e))

    async def health_check(self) -> bool:
        """Check if consumer is healthy"""
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

    async def get_queue_info(self) -> Dict[str, Any]:
        """Get queue information"""
        if not self.is_connected() or not self.queue:
            return {}

        try:
            queue_info = await self.queue.channel.queue_declare(self.queue_name, passive=True)

            return {
                "queue": self.queue_name,
                "message_count": queue_info.method.message_count,
                "consumer_count": queue_info.method.consumer_count,
            }

        except Exception as e:
            self.logger.warning("Failed to get queue info", error=str(e))
            return {}

    def __repr__(self) -> str:
        return (
            f"RabbitMQConsumer(queue={self.queue_name}, "
            f"exchange={self.exchange_name}, routing_key={self.routing_key})"
        )
