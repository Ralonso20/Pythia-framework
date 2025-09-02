"""
Redis Pub/Sub implementation for Pythia framework
"""

import asyncio
import json
from typing import AsyncIterator, Dict, Any, Optional, List, Union

import redis.asyncio as redis

from pythia.core.message import Message
from pythia.brokers.base.broker import MessageBroker, MessageProducer
from pythia.config.redis import RedisConfig
from pythia.logging.setup import get_pythia_logger


class RedisPubSubConsumer(MessageBroker):
    """
    Redis Pub/Sub Consumer implementation

    Example:
        consumer = RedisPubSubConsumer(
            channels=["channel1", "channel2"],
            patterns=["news.*", "alerts.*"]
        )

        async for message in consumer.consume():
            print(f"Received: {message.body}")
    """

    def __init__(
        self,
        channels: Optional[List[str]] = None,
        patterns: Optional[List[str]] = None,
        config: Optional[RedisConfig] = None,
        **kwargs,
    ):
        """
        Initialize Redis Pub/Sub consumer

        Args:
            channels: List of channels to subscribe to
            patterns: List of patterns to subscribe to
            config: Redis configuration
            **kwargs: Additional configuration
        """
        self.channels = channels or []
        self.patterns = patterns or []

        if not self.channels and not self.patterns:
            raise ValueError("Must specify at least one channel or pattern")

        # Merge config with kwargs
        if config:
            self.config = config
        else:
            config_dict = {
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6379),
                **kwargs,
            }
            self.config = RedisConfig(**config_dict)

        self.logger = get_pythia_logger(
            f"RedisPubSubConsumer[{','.join(self.channels + self.patterns)}]"
        )

        # Connection management
        self.redis: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
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

            # Create pubsub object
            self.pubsub = self.redis.pubsub()

            # Subscribe to channels
            if self.channels:
                await self.pubsub.subscribe(*self.channels)
                self.logger.info("Subscribed to channels", channels=self.channels)

            # Subscribe to patterns
            if self.patterns:
                await self.pubsub.psubscribe(*self.patterns)
                self.logger.info("Subscribed to patterns", patterns=self.patterns)

            self.logger.info("Successfully connected to Redis Pub/Sub")

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
            if self.pubsub:
                await self.pubsub.close()
            if self.redis:
                await self.redis.aclose()
            self.logger.info("Disconnected from Redis")
        except Exception as e:
            self.logger.warning("Error during disconnect", error=str(e))
        finally:
            self.redis = None
            self.pubsub = None

    def is_connected(self) -> bool:
        """Check if connected to Redis"""
        return self.redis is not None and self.pubsub is not None

    async def consume(self) -> AsyncIterator[Message]:
        """
        Consume messages from Redis Pub/Sub

        Yields:
            Message: Pythia Message objects
        """
        if not self.is_connected():
            await self.connect()

        if not self.pubsub:
            raise RuntimeError("PubSub connection not initialized")

        self._consuming = True
        self.logger.info(
            "Starting message consumption",
            channels=self.channels,
            patterns=self.patterns,
        )

        try:
            while self._consuming:
                try:
                    # Get message with timeout
                    message = await self.pubsub.get_message(
                        timeout=self.config.block_timeout_ms / 1000
                        if self.config.block_timeout_ms
                        else 1.0
                    )

                    if message is None:
                        continue

                    # Skip subscription confirmation messages
                    if message["type"] in (
                        "subscribe",
                        "psubscribe",
                        "unsubscribe",
                        "punsubscribe",
                    ):
                        continue

                    try:
                        pythia_message = self._convert_message(message)

                        self.logger.debug(
                            "Message received",
                            channel=message.get("channel"),
                            pattern=message.get("pattern"),
                            type=message.get("type"),
                        )

                        yield pythia_message

                    except Exception as e:
                        self.logger.error(
                            "Error processing message",
                            error=str(e),
                            channel=message.get("channel"),
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

    def _convert_message(self, redis_message: Dict[str, Any]) -> Message:
        """Convert Redis Pub/Sub message to Pythia Message"""
        try:
            # Extract data
            data = redis_message.get("data")
            channel = redis_message.get("channel")
            pattern = redis_message.get("pattern")
            message_type = redis_message.get("type", "message")

            # Try to parse data as JSON
            try:
                if isinstance(data, str):
                    body = json.loads(data)
                else:
                    body = data
            except (json.JSONDecodeError, TypeError):
                # If not JSON, treat as string
                body = data

            # Create message
            message = Message(
                body=body,
                message_id="redis-pubsub-msg",  # Pub/Sub doesn't have message IDs
                headers={
                    "redis_channel": channel,
                    "redis_pattern": pattern,
                    "redis_type": message_type,
                },
                channel=channel,
                pattern=pattern,
                _raw_message=redis_message,
            )

            return message

        except Exception as e:
            self.logger.error(
                "Failed to convert Redis message",
                error=str(e),
                channel=redis_message.get("channel"),
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


class RedisPubSubProducer(MessageProducer):
    """
    Redis Pub/Sub Producer implementation

    Example:
        producer = RedisPubSubProducer()
        await producer.send("Hello World", channel="my-channel")
    """

    def __init__(
        self,
        default_channel: Optional[str] = None,
        config: Optional[RedisConfig] = None,
        **kwargs,
    ):
        """
        Initialize Redis Pub/Sub producer

        Args:
            default_channel: Default channel to publish to
            config: Redis configuration
            **kwargs: Additional configuration
        """
        self.default_channel = default_channel

        # Merge config with kwargs
        if config:
            self.config = config
        else:
            config_dict = {
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6379),
                **kwargs,
            }
            self.config = RedisConfig(**config_dict)

        self.logger = get_pythia_logger(f"RedisPubSubProducer[{default_channel or 'multi'}]")

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

            self.logger.info("Successfully connected to Redis Pub/Sub")

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
        channel: Optional[str] = None,
        **kwargs,
    ) -> bool:
        """
        Send a single message to Redis channel

        Args:
            message: Message to send
            channel: Channel to publish to (overrides default)
            **kwargs: Additional options

        Returns:
            bool: True if sent successfully
        """
        if not self.is_connected():
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        # Determine channel
        final_channel = channel or self.default_channel
        if not final_channel:
            raise ValueError("No channel specified and no default channel set")

        try:
            # Convert message to string
            message_str = self._serialize_message(message)

            # Publish to channel
            subscribers = await self.redis.publish(final_channel, message_str)

            self.logger.debug(
                "Message sent successfully",
                channel=final_channel,
                subscribers=subscribers,
                message_size=len(message_str),
            )

            return True

        except Exception as e:
            self.logger.error("Failed to send message", error=str(e), channel=final_channel)
            raise

    async def send_batch(
        self,
        messages: List[Union[Dict[str, Any], Message, str, bytes]],
        channel: Optional[str] = None,
        **kwargs,
    ) -> int:
        """
        Send multiple messages to Redis channel

        Args:
            messages: List of messages to send
            channel: Channel to publish to
            **kwargs: Additional options

        Returns:
            int: Number of messages sent successfully
        """
        if not messages:
            return 0

        final_channel = channel or self.default_channel
        if not final_channel:
            raise ValueError("No channel specified and no default channel set")

        if not self.is_connected():
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        sent_count = 0

        self.logger.info("Sending message batch", count=len(messages), channel=final_channel)

        for i, message in enumerate(messages):
            try:
                message_str = self._serialize_message(message)
                await self.redis.publish(final_channel, message_str)
                sent_count += 1

            except Exception as e:
                self.logger.error(
                    "Failed to send message in batch",
                    error=str(e),
                    message_index=i,
                    channel=final_channel,
                )
                # Continue with other messages

        self.logger.info(
            "Batch send completed",
            sent=sent_count,
            total=len(messages),
            success_rate=f"{sent_count / len(messages) * 100:.1f}%",
        )

        return sent_count

    def _serialize_message(self, message: Union[Dict[str, Any], Message, str, bytes]) -> str:
        """Convert message to string format for Redis Pub/Sub"""

        if isinstance(message, Message):
            # Pythia Message object - serialize the body
            if isinstance(message.body, (dict, list)):
                return json.dumps(message.body, ensure_ascii=False)
            else:
                return str(message.body)
        elif isinstance(message, dict):
            # Dictionary - serialize to JSON
            return json.dumps(message, ensure_ascii=False)
        elif isinstance(message, str):
            # String - use as is
            return message
        elif isinstance(message, bytes):
            # Bytes - decode if possible
            try:
                return message.decode("utf-8")
            except UnicodeDecodeError:
                # Fall back to hex representation
                return message.hex()
        else:
            # Try to serialize as JSON
            try:
                return json.dumps(message, ensure_ascii=False)
            except TypeError:
                return str(message)

    async def publish_to_multiple_channels(
        self, message: Union[Dict[str, Any], Message, str, bytes], channels: List[str]
    ) -> Dict[str, int]:
        """
        Publish message to multiple channels

        Args:
            message: Message to send
            channels: List of channels to publish to

        Returns:
            Dict[str, int]: Channel -> subscriber count mapping
        """
        if not self.is_connected():
            await self.connect()

        if not self.redis:
            raise RuntimeError("Redis connection not initialized")

        results = {}
        message_str = self._serialize_message(message)

        for channel in channels:
            try:
                subscribers = await self.redis.publish(channel, message_str)
                results[channel] = subscribers

                self.logger.debug(
                    "Message published to channel",
                    channel=channel,
                    subscribers=subscribers,
                )

            except Exception as e:
                self.logger.error("Failed to publish to channel", error=str(e), channel=channel)
                results[channel] = 0

        return results

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
        return f"RedisPubSubProducer(default_channel={self.default_channel})"
