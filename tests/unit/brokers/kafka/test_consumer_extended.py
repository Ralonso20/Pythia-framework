"""
Comprehensive tests for Kafka consumer implementation
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from confluent_kafka import Consumer, KafkaError
import json

from pythia.brokers.kafka.consumer import KafkaConsumer
from pythia.config.kafka import KafkaConfig
from pythia.core.message import Message


class TestKafkaConsumer:
    """Test Kafka consumer functionality"""

    @pytest.fixture
    def kafka_config(self):
        """Create test Kafka config"""
        return KafkaConfig(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            topics=["test-topic"]
        )

    @pytest.fixture
    def kafka_consumer(self, kafka_config):
        """Create KafkaConsumer instance"""
        return KafkaConsumer(config=kafka_config)

    def test_consumer_initialization_default_config(self):
        """Test consumer initialization with default config"""
        consumer = KafkaConsumer()

        assert consumer.kafka_config is not None
        assert isinstance(consumer.kafka_config, KafkaConfig)
        assert consumer.consumer is None
        assert consumer._subscribed is False

    def test_consumer_initialization_with_topics_and_group(self):
        """Test consumer initialization with topics and group_id"""
        topics = ["topic1", "topic2"]
        group_id = "my-group"

        consumer = KafkaConsumer(topics=topics, group_id=group_id)

        assert consumer.kafka_config.topics == topics
        assert consumer.kafka_config.group_id == group_id

    def test_consumer_initialization_with_kafka_config_params(self):
        """Test consumer initialization with additional kafka config params"""
        consumer = KafkaConsumer(
            topics=["test-topic"],
            group_id="test-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False
        )

        assert consumer.kafka_config.topics == ["test-topic"]
        assert consumer.kafka_config.group_id == "test-group"

    def test_consumer_initialization_custom_config(self, kafka_config):
        """Test consumer initialization with custom config"""
        consumer = KafkaConsumer(config=kafka_config)

        assert consumer.kafka_config == kafka_config
        assert consumer.kafka_config.topics == ["test-topic"]
        assert consumer.kafka_config.group_id == "test-group"

    @patch('pythia.brokers.kafka.consumer.Consumer')
    @pytest.mark.asyncio
    async def test_connect_impl_success(self, mock_consumer_class, kafka_consumer):
        """Test successful consumer connection"""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer

        await kafka_consumer._connect_impl()

        assert kafka_consumer.consumer == mock_consumer
        assert kafka_consumer._connection == mock_consumer
        mock_consumer_class.assert_called_once()

    @patch('pythia.brokers.kafka.consumer.Consumer')
    @pytest.mark.asyncio
    async def test_connect_impl_failure(self, mock_consumer_class, kafka_consumer):
        """Test consumer connection failure"""
        mock_consumer_class.side_effect = Exception("Connection failed")

        with pytest.raises(Exception) as exc_info:
            await kafka_consumer._connect_impl()

        assert "Connection failed" in str(exc_info.value)
        assert kafka_consumer.consumer is None

    @pytest.mark.asyncio
    async def test_disconnect_impl_success(self, kafka_consumer):
        """Test successful consumer disconnection"""
        # Setup mock consumer
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        await kafka_consumer._disconnect_impl()

        mock_consumer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_impl_no_consumer(self, kafka_consumer):
        """Test disconnection when no consumer exists"""
        kafka_consumer.consumer = None

        # Should not raise exception
        await kafka_consumer._disconnect_impl()

    @pytest.mark.asyncio
    async def test_disconnect_impl_failure(self, kafka_consumer):
        """Test disconnection failure"""
        mock_consumer = Mock()
        mock_consumer.close.side_effect = Exception("Disconnect failed")
        kafka_consumer.consumer = mock_consumer

        with pytest.raises(Exception) as exc_info:
            await kafka_consumer._disconnect_impl()

        assert "Disconnect failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_subscribe_success(self, kafka_consumer):
        """Test successful topic subscription"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        await kafka_consumer.subscribe(["topic1", "topic2"])

        mock_consumer.subscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_no_consumer(self, kafka_consumer):
        """Test subscription without initialized consumer"""
        kafka_consumer.consumer = None

        with pytest.raises(RuntimeError) as exc_info:
            await kafka_consumer.subscribe(["topic1"])

        assert "Consumer not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_subscribe_no_topics(self, kafka_consumer):
        """Test subscription without topics"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer
        kafka_consumer.kafka_config.topics = []

        with pytest.raises(ValueError) as exc_info:
            await kafka_consumer.subscribe()

        assert "No topics specified" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_subscribe_uses_default_topics(self, kafka_consumer):
        """Test subscription uses default topics from config"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        await kafka_consumer.subscribe()

        mock_consumer.subscribe.assert_called_once()

    @patch('pythia.core.message.Message.from_kafka')
    def test_convert_kafka_message_success(self, mock_from_kafka, kafka_consumer):
        """Test successful Kafka message conversion"""
        kafka_msg = Mock()
        kafka_msg.topic.return_value = "test-topic"

        mock_message = Mock(spec=Message)
        mock_from_kafka.return_value = mock_message

        result = kafka_consumer._convert_kafka_message(kafka_msg)

        assert result == mock_message
        mock_from_kafka.assert_called_once_with(kafka_msg, "test-topic")

    def test_convert_kafka_message_failure(self, kafka_consumer):
        """Test Kafka message conversion failure fallback"""
        kafka_msg = Mock()
        kafka_msg.topic.return_value = "test-topic"
        kafka_msg.partition.return_value = 0
        kafka_msg.offset.return_value = 123
        kafka_msg.value.return_value = b"test-data"

        with patch('pythia.core.message.Message.from_kafka') as mock_from_kafka:
            mock_from_kafka.side_effect = Exception("Conversion error")

            result = kafka_consumer._convert_kafka_message(kafka_msg)

            assert isinstance(result, Message)
            assert result.topic == "test-topic"
            assert result.partition == 0
            assert result.offset == 123
            assert result.body == b"test-data"

    def test_handle_consumer_error_partition_eof(self, kafka_consumer):
        """Test handling of partition EOF error (non-critical)"""
        kafka_msg = Mock()
        kafka_msg.topic.return_value = "test-topic"
        kafka_msg.partition.return_value = 0
        kafka_msg.offset.return_value = 123

        kafka_error = Mock()
        kafka_error.code.return_value = KafkaError._PARTITION_EOF
        kafka_msg.error.return_value = kafka_error

        is_critical = kafka_consumer._handle_consumer_error(kafka_msg)

        assert is_critical is False

    def test_handle_consumer_error_critical(self, kafka_consumer):
        """Test handling of critical consumer errors"""
        kafka_msg = Mock()
        kafka_msg.topic.return_value = "test-topic"
        kafka_msg.partition.return_value = 0

        kafka_error = Mock()
        kafka_error.code.return_value = KafkaError._AUTHENTICATION
        kafka_error.__str__ = Mock(return_value="Authentication failed")
        kafka_msg.error.return_value = kafka_error

        is_critical = kafka_consumer._handle_consumer_error(kafka_msg)

        assert is_critical is True

    @pytest.mark.asyncio
    async def test_health_check_success(self, kafka_consumer):
        """Test successful health check"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        mock_stats = {
            "brokers": {
                "broker1": {"state": "UP"},
                "broker2": {"state": "UP"}
            }
        }
        mock_consumer.stats.return_value = json.dumps(mock_stats)

        is_healthy = await kafka_consumer._health_check_impl()

        assert is_healthy is True

    @pytest.mark.asyncio
    async def test_health_check_no_consumer(self, kafka_consumer):
        """Test health check without consumer"""
        kafka_consumer.consumer = None

        is_healthy = await kafka_consumer._health_check_impl()

        assert is_healthy is False

    @pytest.mark.asyncio
    async def test_health_check_no_active_brokers(self, kafka_consumer):
        """Test health check with no active brokers"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        mock_stats = {
            "brokers": {
                "broker1": {"state": "DOWN"}
            }
        }
        mock_consumer.stats.return_value = json.dumps(mock_stats)

        is_healthy = await kafka_consumer._health_check_impl()

        assert is_healthy is False

    def test_get_statistics_success(self, kafka_consumer):
        """Test getting Kafka statistics"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        mock_stats = {
            "type": "consumer",
            "ts": 1634567890,
            "msg_cnt": 100,
            "msg_size": 1024,
            "brokers": {"broker1": {}},
            "topics": {"topic1": {}, "topic2": {}}
        }
        mock_consumer.stats.return_value = json.dumps(mock_stats)

        with patch.object(kafka_consumer, 'get_stats', return_value={"base": "stats"}):
            stats = kafka_consumer.get_statistics()

        assert "base" in stats
        assert "kafka_stats" in stats
        assert stats["kafka_stats"]["brokers_count"] == 1
        assert stats["kafka_stats"]["topics_count"] == 2

    @pytest.mark.asyncio
    async def test_commit_success(self, kafka_consumer):
        """Test successful offset commit"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        await kafka_consumer.commit()

        mock_consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_commit_with_message(self, kafka_consumer):
        """Test commit with specific message"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        mock_message = Mock(spec=Message)
        mock_message._kafka_msg = Mock()

        await kafka_consumer.commit(mock_message)

        mock_consumer.commit.assert_called_once_with(mock_message._kafka_msg)

    @pytest.mark.asyncio
    async def test_seek_success(self, kafka_consumer):
        """Test successful seek operation"""
        mock_consumer = Mock()
        kafka_consumer.consumer = mock_consumer

        with patch('confluent_kafka.TopicPartition') as mock_tp:
            mock_tp_instance = Mock()
            mock_tp.return_value = mock_tp_instance

            await kafka_consumer.seek("test-topic", 0, 100)

            mock_tp.assert_called_once_with("test-topic", 0, 100)
            mock_consumer.seek.assert_called_once_with(mock_tp_instance)

    def test_sanitize_config(self, kafka_consumer):
        """Test configuration sanitization (removes sensitive data)"""
        kafka_consumer.kafka_config.sasl_password = "secret"
        kafka_consumer.kafka_config.ssl_key_password = "secret-key"

        sanitized = kafka_consumer._sanitize_config()

        assert sanitized.get("sasl_password") == "***"
        assert sanitized.get("ssl_key_password") == "***"

    @patch('pythia.brokers.kafka.consumer.Consumer')
    @pytest.mark.asyncio
    async def test_consumer_with_security_config(self, mock_consumer_class):
        """Test consumer with security configuration"""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass"
        )

        consumer = KafkaConsumer(config=config)
        await consumer._connect_impl()

        mock_consumer_class.assert_called_once()
        # Verify security settings were passed
        call_args = mock_consumer_class.call_args[0][0]
        assert call_args["security.protocol"] == "SASL_SSL"
        assert call_args["sasl.mechanism"] == "PLAIN"
        assert call_args["sasl.username"] == "user"
        assert call_args["sasl.password"] == "pass"
