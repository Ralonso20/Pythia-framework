"""
Unit tests for pythia.brokers.redis.pubsub module
"""

import pytest
from unittest.mock import AsyncMock, patch

from pythia.brokers.redis.pubsub import RedisPubSubConsumer, RedisPubSubProducer
from pythia.core.message import Message
from pythia.config.redis import RedisConfig


class TestRedisPubSubConsumer:
    """Test cases for RedisPubSubConsumer"""

    @pytest.mark.unit
    def test_consumer_initialization_with_channels(self):
        """Test consumer initialization with channels"""
        channels = ["channel1", "channel2"]
        consumer = RedisPubSubConsumer(channels=channels)

        assert consumer.channels == channels
        assert consumer.patterns == []
        assert not consumer.is_connected()

    @pytest.mark.unit
    def test_consumer_initialization_with_patterns(self):
        """Test consumer initialization with patterns"""
        patterns = ["news.*", "alerts.*"]
        consumer = RedisPubSubConsumer(patterns=patterns)

        assert consumer.channels == []
        assert consumer.patterns == patterns

    @pytest.mark.unit
    def test_consumer_initialization_requires_channels_or_patterns(self):
        """Test that consumer requires at least channels or patterns"""
        with pytest.raises(
            ValueError, match="Must specify at least one channel or pattern"
        ):
            RedisPubSubConsumer()

    @pytest.mark.unit
    def test_consumer_initialization_with_config(self):
        """Test consumer initialization with Redis config"""
        config = RedisConfig(host="redis-server", port=6380, db=1)
        consumer = RedisPubSubConsumer(channels=["test"], config=config)

        assert consumer.config.host == "redis-server"
        assert consumer.config.port == 6380
        assert consumer.config.db == 1

    @pytest.mark.unit
    def test_consumer_initialization_with_kwargs(self):
        """Test consumer initialization with kwargs"""
        consumer = RedisPubSubConsumer(
            channels=["test"], host="custom-host", port=6380, password="secret"
        )

        assert consumer.config.host == "custom-host"
        assert consumer.config.port == 6380
        assert consumer.config.password == "secret"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_connect_success(self):
        """Test successful Redis connection"""
        consumer = RedisPubSubConsumer(channels=["test"])

        with patch("pythia.brokers.redis.pubsub.redis.Redis") as mock_redis_class:
            mock_redis = AsyncMock()
            mock_redis_class.return_value = mock_redis
            mock_redis.ping = AsyncMock()
            mock_pubsub = AsyncMock()
            mock_redis.pubsub = lambda: mock_pubsub

            await consumer.connect()

            assert consumer.is_connected()
            mock_redis.ping.assert_called_once()
            mock_pubsub.subscribe.assert_called_once_with("test")

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_connect_with_patterns(self):
        """Test connection with pattern subscription"""
        consumer = RedisPubSubConsumer(patterns=["news.*"])

        with patch("pythia.brokers.redis.pubsub.redis.Redis") as mock_redis_class:
            mock_redis = AsyncMock()
            mock_redis_class.return_value = mock_redis
            mock_redis.ping = AsyncMock()
            mock_pubsub = AsyncMock()
            mock_redis.pubsub = lambda: mock_pubsub

            await consumer.connect()

            mock_pubsub.psubscribe.assert_called_once_with("news.*")

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_connect_failure(self):
        """Test connection failure handling"""
        consumer = RedisPubSubConsumer(channels=["test"])

        with patch("pythia.brokers.redis.pubsub.redis.Redis") as mock_redis_class:
            mock_redis = AsyncMock()
            mock_redis_class.return_value = mock_redis
            mock_redis.ping = AsyncMock(
                side_effect=ConnectionError("Connection failed")
            )

            with pytest.raises(ConnectionError):
                await consumer.connect()

            assert not consumer.is_connected()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_disconnect(self):
        """Test Redis disconnection"""
        consumer = RedisPubSubConsumer(channels=["test"])

        # Mock connection
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        consumer.redis = mock_redis
        consumer.pubsub = mock_pubsub

        await consumer.disconnect()

        mock_pubsub.close.assert_called_once()
        mock_redis.aclose.assert_called_once()
        assert not consumer.is_connected()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_success(self):
        """Test successful health check"""
        consumer = RedisPubSubConsumer(channels=["test"])

        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock()
        consumer.redis = mock_redis

        result = await consumer.health_check()
        assert result is True
        mock_redis.ping.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_failure(self):
        """Test health check failure"""
        consumer = RedisPubSubConsumer(channels=["test"])

        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(side_effect=Exception("Ping failed"))
        consumer.redis = mock_redis

        result = await consumer.health_check()
        assert result is False

    @pytest.mark.unit
    def test_convert_message_json_data(self):
        """Test converting Redis message with JSON data"""
        consumer = RedisPubSubConsumer(channels=["test"])

        redis_message = {
            "type": "message",
            "channel": "test-channel",
            "data": '{"user_id": 123, "action": "login"}',
        }

        message = consumer._convert_message(redis_message)

        assert isinstance(message, Message)
        assert message.body == {"user_id": 123, "action": "login"}
        assert message.headers["redis_channel"] == "test-channel"
        assert message.headers["redis_type"] == "message"

    @pytest.mark.unit
    def test_convert_message_string_data(self):
        """Test converting Redis message with string data"""
        consumer = RedisPubSubConsumer(channels=["test"])

        redis_message = {
            "type": "message",
            "channel": "test-channel",
            "data": "simple string message",
        }

        message = consumer._convert_message(redis_message)

        assert message.body == "simple string message"
        assert message.headers["redis_channel"] == "test-channel"

    @pytest.mark.unit
    def test_convert_message_with_pattern(self):
        """Test converting Redis message with pattern matching"""
        consumer = RedisPubSubConsumer(patterns=["news.*"])

        redis_message = {
            "type": "pmessage",
            "channel": "news.sports",
            "pattern": "news.*",
            "data": "Breaking sports news",
        }

        message = consumer._convert_message(redis_message)

        assert message.body == "Breaking sports news"
        assert message.headers["redis_channel"] == "news.sports"
        assert message.headers["redis_pattern"] == "news.*"
        assert message.headers["redis_type"] == "pmessage"


class TestRedisPubSubProducer:
    """Test cases for RedisPubSubProducer"""

    @pytest.mark.unit
    def test_producer_initialization(self):
        """Test producer initialization"""
        producer = RedisPubSubProducer(default_channel="output")

        assert producer.default_channel == "output"
        assert not producer.is_connected()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_send_message_success(self):
        """Test successful message sending"""
        producer = RedisPubSubProducer(default_channel="output")

        with patch("pythia.brokers.redis.pubsub.redis.Redis") as mock_redis_class:
            mock_redis = AsyncMock()
            mock_redis_class.return_value = mock_redis
            mock_redis.ping = AsyncMock()
            mock_redis.publish = AsyncMock(return_value=2)  # 2 subscribers

            await producer.connect()
            result = await producer.send({"message": "test"})

            assert result is True
            mock_redis.publish.assert_called_once_with("output", '{"message": "test"}')

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_send_message_to_specific_channel(self):
        """Test sending message to specific channel"""
        producer = RedisPubSubProducer()

        with patch("pythia.brokers.redis.pubsub.redis.Redis") as mock_redis_class:
            mock_redis = AsyncMock()
            mock_redis_class.return_value = mock_redis
            mock_redis.ping = AsyncMock()
            mock_redis.publish = AsyncMock(return_value=1)

            await producer.connect()
            result = await producer.send("test message", channel="specific-channel")

            assert result is True
            mock_redis.publish.assert_called_once_with(
                "specific-channel", "test message"
            )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_send_without_channel_raises_error(self):
        """Test sending without channel raises error"""
        producer = RedisPubSubProducer()  # No default channel

        mock_redis = AsyncMock()
        producer.redis = mock_redis

        with pytest.raises(ValueError, match="No channel specified"):
            await producer.send("test message")

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_send_batch_messages(self):
        """Test sending batch of messages"""
        producer = RedisPubSubProducer(default_channel="batch")

        with patch("pythia.brokers.redis.pubsub.redis.Redis") as mock_redis_class:
            mock_redis = AsyncMock()
            mock_redis_class.return_value = mock_redis
            mock_redis.ping = AsyncMock()
            mock_redis.publish = AsyncMock(return_value=1)

            await producer.connect()

            messages = ["msg1", "msg2", "msg3"]
            count = await producer.send_batch(messages)

            assert count == 3
            assert mock_redis.publish.call_count == 3

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_to_multiple_channels(self):
        """Test publishing to multiple channels"""
        producer = RedisPubSubProducer()

        with patch("pythia.brokers.redis.pubsub.redis.Redis") as mock_redis_class:
            mock_redis = AsyncMock()
            mock_redis_class.return_value = mock_redis
            mock_redis.ping = AsyncMock()
            mock_redis.publish = AsyncMock(return_value=2)

            await producer.connect()

            channels = ["channel1", "channel2", "channel3"]
            results = await producer.publish_to_multiple_channels("broadcast", channels)

            assert len(results) == 3
            assert all(count == 2 for count in results.values())
            assert mock_redis.publish.call_count == 3

    @pytest.mark.unit
    def test_serialize_message_types(self):
        """Test message serialization for different types"""
        producer = RedisPubSubProducer()

        # Dict message
        dict_result = producer._serialize_message({"key": "value"})
        assert dict_result == '{"key": "value"}'

        # String message
        str_result = producer._serialize_message("simple string")
        assert str_result == "simple string"

        # Bytes message
        bytes_result = producer._serialize_message(b"byte string")
        assert bytes_result == "byte string"

        # Message object
        message = Message(body={"data": "test"})
        msg_result = producer._serialize_message(message)
        assert msg_result == '{"data": "test"}'

    @pytest.mark.unit
    def test_serialize_message_fallback(self):
        """Test message serialization fallback to string"""
        producer = RedisPubSubProducer()

        # Non-serializable object should convert to string
        class CustomObject:
            def __str__(self):
                return "custom object"

        result = producer._serialize_message(CustomObject())
        assert result == "custom object"


class TestRedisPubSubIntegration:
    """Integration-style tests for Redis PubSub"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_consumer_message_filtering(self):
        """Test consumer handles subscription confirmation messages"""
        consumer = RedisPubSubConsumer(channels=["test"])

        # Mock redis and pubsub
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        consumer.redis = mock_redis
        consumer.pubsub = mock_pubsub
        consumer._consuming = True

        # Mock get_message to return subscription confirmation first, then actual message
        messages = [
            {"type": "subscribe", "channel": "test", "data": 1},  # Should skip
            {"type": "message", "channel": "test", "data": "actual message"},
            None,  # End iteration
        ]

        async def mock_get_message(timeout=None):
            if messages:
                return messages.pop(0)
            consumer._consuming = False  # Stop after processing
            return None

        mock_pubsub.get_message = mock_get_message

        # Consume messages
        actual_messages = []
        async for message in consumer.consume():
            actual_messages.append(message)

        # Should only get one actual message, not subscription confirmation
        assert len(actual_messages) == 1
        assert actual_messages[0].body == "actual message"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_consumer_error_recovery(self):
        """Test consumer recovers from connection errors"""
        consumer = RedisPubSubConsumer(channels=["test"])

        # Mock components
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        consumer.redis = mock_redis
        consumer.pubsub = mock_pubsub
        consumer._consuming = True

        call_count = 0

        async def mock_get_message(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Connection error")  # First call fails
            elif call_count == 2:
                consumer._consuming = False  # Stop after recovery attempt
                return None
            return None

        mock_pubsub.get_message = mock_get_message

        with patch.object(consumer, "disconnect", new_callable=AsyncMock):
            messages = []
            async for message in consumer.consume():
                messages.append(message)

        # Should handle error gracefully
        assert len(messages) == 0
        assert call_count == 2
