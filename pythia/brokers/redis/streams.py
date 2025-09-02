"""
Redis Streams implementation for Pythia framework - Simplified
"""

import asyncio
import json
from typing import AsyncIterator, Dict, Any, Optional, List, Union

import redis.asyncio as redis

from pythia.core.message import Message
from pythia.brokers.base.broker import MessageBroker, MessageProducer
from pythia.config.redis import RedisConfig
from pythia.logging.setup import get_pythia_logger


class RedisStreamsConsumer(MessageBroker):
    """Simplified Redis Streams Consumer"""

    def __init__(
        self,
        stream: str,
        consumer_group: str,
        consumer_name: Optional[str] = None,
        config: Optional[RedisConfig] = None,
        **kwargs,
    ):
        self.stream = stream
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name or f"pythia-{id(self)}"

        if config:
            self.config = config
        else:
            self.config = RedisConfig(
                host=kwargs.get("host", "localhost"),
                port=kwargs.get("port", 6379),
                stream=stream,
                consumer_group=consumer_group,
            )

        self.logger = get_pythia_logger(f"RedisStreamsConsumer[{stream}]")
        self.redis: Optional[redis.Redis] = None
        self._consuming = False

    async def connect(self) -> None:
        """Establish connection to Redis"""
        if self.redis:
            return

        try:
            self.logger.info("Connecting to Redis", host=self.config.host, port=self.config.port)

            self.redis = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                socket_keepalive=self.config.socket_keepalive,
                health_check_interval=self.config.health_check_interval,
                max_connections=self.config.max_connections,
            )

            await self.redis.ping()

            # Create consumer group if it doesn't exist
            try:
                await self.redis.xgroup_create(
                    self.stream, self.consumer_group, id="0", mkstream=True
                )
                self.logger.info(
                    "Consumer group created",
                    stream=self.stream,
                    group=self.consumer_group,
                )
            except redis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

            self.logger.info("Successfully connected to Redis Streams")

        except Exception as e:
            self.logger.error("Failed to connect to Redis", error=str(e))
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close connection to Redis"""
        self._consuming = False

        if self.redis:
            try:
                await self.redis.aclose()
                self.logger.info("Disconnected from Redis")
            except Exception as e:
                self.logger.warning("Error during disconnect", error=str(e))
            finally:
                self.redis = None

    async def consume(self) -> AsyncIterator[Message]:
        """Consume messages from Redis Stream"""
        if not self.redis:
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        self._consuming = True
        self.logger.info("Starting message consumption", stream=self.stream)

        try:
            while self._consuming:
                try:
                    messages = await self.redis.xreadgroup(
                        self.consumer_group,
                        self.consumer_name,
                        {self.stream: ">"},
                        count=self.config.batch_size,
                        block=self.config.block_timeout_ms,
                    )

                    if not messages:
                        continue

                    for stream_name, stream_messages in messages:
                        for message_id, fields in stream_messages:
                            try:
                                message = self._convert_message(message_id, fields)
                                self.logger.debug("Message received", message_id=message_id)
                                yield message
                            except Exception as e:
                                self.logger.error(
                                    "Error processing message",
                                    error=str(e),
                                    message_id=message_id,
                                )
                                # Acknowledge to prevent redelivery
                                await self.redis.xack(self.stream, self.consumer_group, message_id)

                except redis.ConnectionError as e:
                    self.logger.error("Redis connection error", error=str(e))
                    await self.disconnect()
                    await asyncio.sleep(1)
                    continue

        except Exception as e:
            self.logger.error("Fatal error in consumption loop", error=str(e))
            raise
        finally:
            self._consuming = False
            self.logger.info("Stopped message consumption")

    def _convert_message(self, message_id: str, fields: Dict[str, str]) -> Message:
        """Convert Redis Stream message to Pythia Message"""
        try:
            # Extract body
            body_str = fields.get("body", "{}")
            try:
                body = json.loads(body_str)
            except json.JSONDecodeError:
                body = body_str

            # Extract headers (fields starting with 'header_')
            headers = {}
            for key, value in fields.items():
                if key.startswith("header_"):
                    headers[key[7:]] = value  # Remove 'header_' prefix

            return Message(
                body=body,
                message_id=message_id,
                headers=headers,
                stream_id=message_id,
                _raw_data=fields,
            )

        except Exception as e:
            self.logger.error(
                "Failed to convert Redis message", error=str(e), message_id=message_id
            )
            raise

    async def acknowledge(self, message: Message) -> None:
        """Acknowledge message processing"""
        if not self.redis:
            return

        try:
            await self.redis.xack(self.stream, self.consumer_group, message.message_id)
            self.logger.debug("Message acknowledged", message_id=message.message_id)
        except Exception as e:
            self.logger.error("Failed to acknowledge message", error=str(e))

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


class RedisStreamsProducer(MessageProducer):
    """Simplified Redis Streams Producer"""

    def __init__(self, stream: str, config: Optional[RedisConfig] = None, **kwargs):
        self.stream = stream

        if config:
            self.config = config
        else:
            self.config = RedisConfig(
                host=kwargs.get("host", "localhost"),
                port=kwargs.get("port", 6379),
                stream=stream,
            )

        self.logger = get_pythia_logger(f"RedisStreamsProducer[{stream}]")
        self.redis: Optional[redis.Redis] = None

    async def connect(self) -> None:
        """Establish connection to Redis"""
        if self.redis:
            return

        try:
            self.logger.info("Connecting to Redis", host=self.config.host, port=self.config.port)

            self.redis = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                socket_keepalive=self.config.socket_keepalive,
                health_check_interval=self.config.health_check_interval,
                max_connections=self.config.max_connections,
            )

            await self.redis.ping()
            self.logger.info("Successfully connected to Redis Streams")

        except Exception as e:
            self.logger.error("Failed to connect to Redis", error=str(e))
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close connection to Redis"""
        if self.redis:
            try:
                await self.redis.aclose()
                self.logger.info("Disconnected from Redis")
            except Exception as e:
                self.logger.warning("Error during disconnect", error=str(e))
            finally:
                self.redis = None

    async def send(
        self,
        message: Union[Dict[str, Any], Message, str, bytes],
        message_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> bool:
        """Send a single message to Redis Stream"""
        if not self.redis:
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        try:
            fields = self._create_fields(message, headers, **kwargs)

            result = await self.redis.xadd(
                self.stream,
                fields,
                id=message_id or "*",
                maxlen=self.config.max_stream_length,
                approximate=True,
            )

            self.logger.debug("Message sent successfully", stream=self.stream, message_id=result)
            return True

        except Exception as e:
            self.logger.error("Failed to send message", error=str(e), stream=self.stream)
            raise

    async def send_batch(
        self,
        messages: List[Union[Dict[str, Any], Message, str, bytes]],
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> int:
        """Send multiple messages to Redis Stream"""
        if not messages:
            return 0

        if not self.redis:
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        sent_count = 0
        self.logger.info("Sending message batch", count=len(messages), stream=self.stream)

        for i, message in enumerate(messages):
            try:
                fields = self._create_fields(message, headers, **kwargs)
                await self.redis.xadd(
                    self.stream,
                    fields,
                    maxlen=self.config.max_stream_length,
                    approximate=True,
                )
                sent_count += 1
            except Exception as e:
                self.logger.error("Failed to send message in batch", error=str(e), message_index=i)

        self.logger.info("Batch send completed", sent=sent_count, total=len(messages))
        return sent_count

    def _create_fields(
        self,
        message: Union[Dict[str, Any], Message, str, bytes],
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> Dict[str, str]:
        """Convert message to Redis Stream fields format"""
        fields = {}

        # Handle different message types
        if isinstance(message, Message):
            body = message.body
            message_headers = {**(message.headers or {}), **(headers or {})}
        elif isinstance(message, dict):
            body = message
            message_headers = headers or {}
        elif isinstance(message, str):
            body = message
            message_headers = headers or {}
        elif isinstance(message, bytes):
            try:
                body = message.decode("utf-8")
            except UnicodeDecodeError:
                body = message.hex()
            message_headers = headers or {}
        else:
            body = message
            message_headers = headers or {}

        # Add body as JSON string
        if isinstance(body, (dict, list)):
            fields["body"] = json.dumps(body, ensure_ascii=False)
        else:
            fields["body"] = str(body)

        # Add headers with prefix
        for key, value in message_headers.items():
            fields[f"header_{key}"] = str(value)

        # Add additional kwargs as fields
        for key, value in kwargs.items():
            fields[key] = str(value)

        return fields

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
        return f"RedisStreamsProducer(stream={self.stream})"
