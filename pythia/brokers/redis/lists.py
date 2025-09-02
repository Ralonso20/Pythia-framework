"""
Redis Lists implementation for Pythia framework (Queue pattern)
"""

import asyncio
import json
from typing import AsyncIterator, Dict, Any, Optional, List, Union

import redis.asyncio as redis

from pythia.core.message import Message
from pythia.brokers.base import BaseConsumer, BaseProducer
from pythia.config.redis import RedisConfig
from pythia.logging import get_pythia_logger


class RedisListConsumer(BaseConsumer):
    """
    Redis List Consumer implementation (Queue pattern)

    Example:
        consumer = RedisListConsumer(queue="my-queue")

        async for message in consumer.consume():
            print(f"Received: {message.body}")
    """

    def __init__(self, queue: str, config: Optional[RedisConfig] = None, **kwargs):
        """
        Initialize Redis List consumer

        Args:
            queue: Queue name (Redis list key)
            config: Redis configuration
            **kwargs: Additional configuration
        """
        super().__init__()

        self.queue = queue

        # Merge config with kwargs
        if config:
            self.config = config
        else:
            config_dict = {
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6379),
                "queue": queue,
                **kwargs,
            }
            self.config = RedisConfig(**config_dict)

        self.logger = get_pythia_logger(f"RedisListConsumer[{queue}]")

        # Connection management
        self.redis: Optional[redis.Redis] = None
        self._consuming = False

    async def connect(self) -> None:
        """Establish connection to Redis"""
        if self.is_connected():
            return

        try:
            self.logger.info(
                "Connecting to Redis",
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
            )

            # Create Redis connection
            self.redis = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                socket_keepalive=self.config.socket_keepalive,
                socket_keepalive_options=self.config.socket_keepalive_options,
                health_check_interval=self.config.health_check_interval,
                max_connections=self.config.max_connections,
            )

            # Test connection
            await self.redis.ping()

            self.logger.info("Successfully connected to Redis Lists")

        except Exception as e:
            self.logger.error(
                "Failed to connect to Redis",
                error=str(e),
                host=self.config.host,
                port=self.config.port,
            )
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close connection to Redis"""
        self._consuming = False

        try:
            if self.redis:
                await self.redis.aclose()
                self.logger.info("Disconnected from Redis")
        except Exception as e:
            self.logger.warning("Error during disconnect", error=str(e))
        finally:
            self.redis = None

    def is_connected(self) -> bool:
        """Check if connected to Redis"""
        return self.redis is not None

    async def consume(self) -> AsyncIterator[Message]:
        """
        Consume messages from Redis List (blocking pop)

        Yields:
            Message: Pythia Message objects
        """
        if not self.is_connected():
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        self._consuming = True
        self.logger.info("Starting message consumption", queue=self.queue)

        try:
            while self._consuming:
                try:
                    # Blocking pop from list (BLPOP)
                    result = await self.redis.blpop(
                        self.queue,
                        timeout=self.config.block_timeout_ms / 1000
                        if self.config.block_timeout_ms
                        else 1,
                    )

                    if result is None:
                        # Timeout occurred
                        continue

                    queue_name, message_data = result

                    try:
                        message = self._convert_message(message_data)

                        self.logger.debug(
                            "Message received",
                            queue=queue_name,
                            message_size=len(message_data),
                        )

                        yield message

                    except Exception as e:
                        self.logger.error(
                            "Error processing message",
                            error=str(e),
                            queue=queue_name,
                            message_data=message_data[:100] + "..."
                            if len(message_data) > 100
                            else message_data,
                        )

                except redis.ConnectionError as e:
                    self.logger.error("Redis connection error", error=str(e))
                    # Try to reconnect
                    await self.disconnect()
                    await asyncio.sleep(1)
                    continue

                except Exception as e:
                    self.logger.error("Error in message consumption", error=str(e))
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error("Fatal error in consumption loop", error=str(e))
            raise
        finally:
            self._consuming = False
            self.logger.info("Stopped message consumption")

    def _convert_message(self, message_data: str) -> Message:
        """Convert Redis List message to Pythia Message"""
        try:
            # Try to parse as JSON
            try:
                data = json.loads(message_data)

                # Check if it's a structured message
                if isinstance(data, dict) and "body" in data:
                    # Structured format: {"body": {...}, "headers": {...}, "message_id": "..."}
                    body = data.get("body")
                    headers = data.get("headers", {})
                    message_id = data.get("message_id")
                else:
                    # Simple JSON data
                    body = data
                    headers = {}
                    message_id = None

            except json.JSONDecodeError:
                # Not JSON, treat as string
                body = message_data
                headers = {}
                message_id = None

            # Create message
            message = Message(
                body=body,
                message_id=message_id,
                headers=headers,
                queue=self.queue,
                _raw_data=message_data,
            )

            return message

        except Exception as e:
            self.logger.error(
                "Failed to convert Redis message",
                error=str(e),
                message_data=message_data[:100] + "..."
                if len(message_data) > 100
                else message_data,
            )
            raise

    async def health_check(self) -> bool:
        """Check if consumer is healthy"""
        try:
            if not self.redis:
                return False

            await self.redis.ping()
            return True

        except Exception as e:
            self.logger.warning("Health check failed", error=str(e))
            return False

    async def get_queue_info(self) -> Dict[str, Any]:
        """Get queue information"""
        if not self.redis:
            return {}

        try:
            queue_length = await self.redis.llen(self.queue)
            return {
                "queue": self.queue,
                "length": queue_length,
            }

        except Exception as e:
            self.logger.warning("Failed to get queue info", error=str(e))
            return {}


class RedisListProducer(BaseProducer):
    """
    Redis List Producer implementation (Queue pattern)

    Example:
        producer = RedisListProducer(queue="my-queue")
        await producer.send({"message": "Hello World"})
    """

    def __init__(self, queue: str, config: Optional[RedisConfig] = None, **kwargs):
        """
        Initialize Redis List producer

        Args:
            queue: Queue name (Redis list key)
            config: Redis configuration
            **kwargs: Additional configuration
        """
        super().__init__()

        self.queue = queue

        # Merge config with kwargs
        if config:
            self.config = config
        else:
            config_dict = {
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6379),
                "queue": queue,
                **kwargs,
            }
            self.config = RedisConfig(**config_dict)

        self.logger = get_pythia_logger(f"RedisListProducer[{queue}]")

        # Connection management
        self.redis: Optional[redis.Redis] = None

    async def connect(self) -> None:
        """Establish connection to Redis"""
        if self.is_connected():
            return

        try:
            self.logger.info(
                "Connecting to Redis",
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
            )

            # Create Redis connection
            self.redis = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                socket_keepalive=self.config.socket_keepalive,
                socket_keepalive_options=self.config.socket_keepalive_options,
                health_check_interval=self.config.health_check_interval,
                max_connections=self.config.max_connections,
            )

            # Test connection
            await self.redis.ping()

            self.logger.info("Successfully connected to Redis Lists")

        except Exception as e:
            self.logger.error(
                "Failed to connect to Redis",
                error=str(e),
                host=self.config.host,
                port=self.config.port,
            )
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close connection to Redis"""
        try:
            if self.redis:
                await self.redis.aclose()
                self.logger.info("Disconnected from Redis")
        except Exception as e:
            self.logger.warning("Error during disconnect", error=str(e))
        finally:
            self.redis = None

    def is_connected(self) -> bool:
        """Check if connected to Redis"""
        return self.redis is not None

    async def send(
        self,
        message: Union[Dict[str, Any], Message, str, bytes],
        headers: Optional[Dict[str, str]] = None,
        message_id: Optional[str] = None,
        priority: bool = False,
        **kwargs,
    ) -> bool:
        """
        Send a single message to Redis List

        Args:
            message: Message to send
            headers: Message headers
            message_id: Optional message ID
            priority: If True, add to front of queue (LPUSH), else to back (RPUSH)
            **kwargs: Additional options

        Returns:
            bool: True if sent successfully
        """
        if not self.is_connected():
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        try:
            # Serialize message
            message_str = self._serialize_message(message, headers, message_id)

            # Add to list (queue)
            if priority:
                # High priority - add to front
                await self.redis.lpush(self.queue, message_str)
            else:
                # Normal priority - add to back
                await self.redis.rpush(self.queue, message_str)

            # Apply max queue length if configured
            if self.config.max_list_length > 0:
                await self.redis.ltrim(self.queue, 0, self.config.max_list_length - 1)

            self.logger.debug(
                "Message sent successfully",
                queue=self.queue,
                priority=priority,
                message_size=len(message_str),
            )

            return True

        except Exception as e:
            self.logger.error("Failed to send message", error=str(e), queue=self.queue)
            raise

    async def send_batch(
        self,
        messages: List[Union[Dict[str, Any], Message, str, bytes]],
        headers: Optional[Dict[str, str]] = None,
        priority: bool = False,
        **kwargs,
    ) -> int:
        """
        Send multiple messages to Redis List

        Args:
            messages: List of messages to send
            headers: Headers for all messages
            priority: If True, add to front of queue, else to back
            **kwargs: Additional options

        Returns:
            int: Number of messages sent successfully
        """
        if not messages:
            return 0

        if not self.is_connected():
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        self.logger.info(
            "Sending message batch",
            count=len(messages),
            queue=self.queue,
            priority=priority,
        )

        # Serialize all messages
        serialized_messages = []
        for message in messages:
            try:
                message_str = self._serialize_message(message, headers)
                serialized_messages.append(message_str)
            except Exception as e:
                self.logger.error("Failed to serialize message in batch", error=str(e))

        if not serialized_messages:
            return 0

        try:
            # Batch send using pipeline for efficiency
            async with self.redis.pipeline() as pipe:
                for message_str in serialized_messages:
                    if priority:
                        pipe.lpush(self.queue, message_str)
                    else:
                        pipe.rpush(self.queue, message_str)

                # Apply max queue length if configured
                if self.config.max_list_length > 0:
                    pipe.ltrim(self.queue, 0, self.config.max_list_length - 1)

                await pipe.execute()

            sent_count = len(serialized_messages)

            self.logger.info(
                "Batch send completed",
                sent=sent_count,
                total=len(messages),
                success_rate=f"{sent_count / len(messages) * 100:.1f}%",
            )

            return sent_count

        except Exception as e:
            self.logger.error("Failed to send message batch", error=str(e), queue=self.queue)
            raise

    def _serialize_message(
        self,
        message: Union[Dict[str, Any], Message, str, bytes],
        headers: Optional[Dict[str, str]] = None,
        message_id: Optional[str] = None,
    ) -> str:
        """Serialize message for Redis List storage"""

        # Handle different message types
        if isinstance(message, Message):
            # Pythia Message object - create structured format
            data = {
                "body": message.body,
                "headers": {**(message.headers or {}), **(headers or {})},
                "message_id": message.message_id or message_id,
                "timestamp": message.timestamp.isoformat() if message.timestamp else None,
            }
        elif isinstance(message, dict):
            # Dictionary - check if it's already structured
            if "body" in message:
                # Already structured
                data = message
                if headers:
                    data["headers"] = {**(data.get("headers", {})), **headers}
                if message_id:
                    data["message_id"] = message_id
            else:
                # Simple dict - wrap it
                data = {
                    "body": message,
                    "headers": headers or {},
                    "message_id": message_id,
                }
        elif isinstance(message, str):
            # String - wrap it
            data = {"body": message, "headers": headers or {}, "message_id": message_id}
        elif isinstance(message, bytes):
            # Bytes - decode and wrap
            try:
                decoded = message.decode("utf-8")
            except UnicodeDecodeError:
                decoded = message.hex()  # Store as hex string

            data = {"body": decoded, "headers": headers or {}, "message_id": message_id}
        else:
            # Try to serialize as JSON
            data = {"body": message, "headers": headers or {}, "message_id": message_id}

        # Serialize to JSON
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except TypeError as e:
            raise ValueError(f"Cannot serialize message: {e}")

    async def send_priority(
        self, message: Union[Dict[str, Any], Message, str, bytes], **kwargs
    ) -> bool:
        """Send a high-priority message (adds to front of queue)"""
        return await self.send(message, priority=True, **kwargs)

    async def health_check(self) -> bool:
        """Check if producer is healthy"""
        try:
            if not self.redis:
                return False

            await self.redis.ping()
            return True

        except Exception as e:
            self.logger.warning("Health check failed", error=str(e))
            return False

    def __repr__(self) -> str:
        return f"RedisListProducer(queue={self.queue})"
