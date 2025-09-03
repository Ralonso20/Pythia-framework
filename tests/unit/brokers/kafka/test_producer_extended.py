"""
Comprehensive tests for Kafka producer implementation
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from confluent_kafka import Producer
import json

from pythia.brokers.kafka.producer import KafkaProducer
from pythia.config.kafka import KafkaConfig


class TestKafkaProducer:
    """Test Kafka producer functionality"""

    @pytest.fixture
    def kafka_config(self):
        """Create test Kafka config"""
        return KafkaConfig(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"]
        )

    @pytest.fixture
    def kafka_producer(self, kafka_config):
        """Create KafkaProducer instance"""
        return KafkaProducer(config=kafka_config)

    def test_producer_initialization_default_config(self):
        """Test producer initialization with default config"""
        producer = KafkaProducer()

        assert producer.kafka_config is not None
        assert isinstance(producer.kafka_config, KafkaConfig)
        assert producer.producer is None
        assert producer.default_topic is None
        assert producer._pending_messages == 0

    def test_producer_initialization_with_topic(self):
        """Test producer initialization with topic"""
        topic = "my-topic"
        producer = KafkaProducer(topic=topic)

        assert producer.default_topic == topic

    def test_producer_initialization_custom_config(self, kafka_config):
        """Test producer initialization with custom config"""
        producer = KafkaProducer(config=kafka_config)

        assert producer.kafka_config == kafka_config
        assert producer.kafka_config.bootstrap_servers == "localhost:9092"

    def test_producer_initialization_with_kafka_config_params(self):
        """Test producer initialization with additional kafka config params"""
        producer = KafkaProducer(
            topic="test-topic",
            batch_size=1000,
            linger_ms=10
        )

        assert producer.default_topic == "test-topic"

    @patch('pythia.brokers.kafka.producer.Producer')
    @pytest.mark.asyncio
    async def test_connect_impl_success(self, mock_producer_class, kafka_producer):
        """Test successful producer connection"""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        await kafka_producer._connect_impl()

        assert kafka_producer.producer == mock_producer
        assert kafka_producer._connection == mock_producer
        mock_producer_class.assert_called_once()

    @patch('pythia.brokers.kafka.producer.Producer')
    @pytest.mark.asyncio
    async def test_connect_impl_failure(self, mock_producer_class, kafka_producer):
        """Test producer connection failure"""
        mock_producer_class.side_effect = Exception("Connection failed")

        with pytest.raises(Exception) as exc_info:
            await kafka_producer._connect_impl()

        assert "Connection failed" in str(exc_info.value)
        assert kafka_producer.producer is None

    @pytest.mark.asyncio
    async def test_disconnect_impl_success(self, kafka_producer):
        """Test successful producer disconnection"""
        mock_producer = Mock()
        mock_producer.flush.return_value = 0
        kafka_producer.producer = mock_producer
        kafka_producer._pending_messages = 5

        await kafka_producer._disconnect_impl()

        mock_producer.flush.assert_called_once_with(timeout=10.0)

    @pytest.mark.asyncio
    async def test_disconnect_impl_with_remaining_messages(self, kafka_producer):
        """Test disconnection with remaining messages"""
        mock_producer = Mock()
        mock_producer.flush.return_value = 3  # 3 messages remaining
        kafka_producer.producer = mock_producer
        kafka_producer._pending_messages = 5

        await kafka_producer._disconnect_impl()

        mock_producer.flush.assert_called_once_with(timeout=10.0)

    @pytest.mark.asyncio
    async def test_disconnect_impl_no_producer(self, kafka_producer):
        """Test disconnection when no producer exists"""
        kafka_producer.producer = None

        # Should not raise exception
        await kafka_producer._disconnect_impl()

    @pytest.mark.asyncio
    async def test_disconnect_impl_failure(self, kafka_producer):
        """Test disconnection failure"""
        mock_producer = Mock()
        mock_producer.flush.side_effect = Exception("Flush failed")
        kafka_producer.producer = mock_producer

        with pytest.raises(Exception) as exc_info:
            await kafka_producer._disconnect_impl()

        assert "Flush failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_send_success_with_topic(self, kafka_producer):
        """Test successful message send with specified topic"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer

        test_data = {"test": "data"}

        await kafka_producer.send(test_data, topic="specific-topic")

        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args[1]["topic"] == "specific-topic"
        assert json.loads(call_args[1]["value"]) == test_data

    @pytest.mark.asyncio
    async def test_send_success_with_default_topic(self, kafka_producer):
        """Test successful message send with default topic"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer
        kafka_producer.default_topic = "default-topic"

        test_data = {"test": "data"}

        await kafka_producer.send(test_data)

        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args[1]["topic"] == "default-topic"

    @pytest.mark.asyncio
    async def test_send_no_topic_error(self, kafka_producer):
        """Test send without topic raises error"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer
        kafka_producer.default_topic = None

        with pytest.raises(ValueError) as exc_info:
            await kafka_producer.send({"test": "data"})

        assert "No topic specified" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_send_with_key_and_headers(self, kafka_producer):
        """Test send with key and headers"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer
        kafka_producer.default_topic = "test-topic"

        test_data = {"test": "data"}
        test_key = "test-key"
        test_headers = {"header1": "value1", "header2": "value2"}

        await kafka_producer.send(test_data, key=test_key, headers=test_headers)

        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args[1]["key"] == b"test-key"  # Key gets encoded to bytes
        # Headers get converted to list of tuples with byte values
        expected_headers = [("header1", b"value1"), ("header2", b"value2")]
        assert call_args[1]["headers"] == expected_headers

    @pytest.mark.asyncio
    async def test_send_with_partition(self, kafka_producer):
        """Test send with specific partition"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer
        kafka_producer.default_topic = "test-topic"

        await kafka_producer.send({"test": "data"}, partition=2)

        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args[1]["partition"] == 2

    @pytest.mark.asyncio
    async def test_send_auto_connect(self, kafka_producer):
        """Test send auto-connects if no producer"""
        kafka_producer.producer = None
        kafka_producer.default_topic = "test-topic"

        async def mock_connect():
            # Simulate connect by setting up a mock producer
            mock_producer = Mock()
            kafka_producer.producer = mock_producer

        with patch.object(kafka_producer, 'connect', side_effect=mock_connect) as mock_connect:
            await kafka_producer.send({"test": "data"})

            mock_connect.assert_called_once()

    def test_serialize_data_bytes(self, kafka_producer):
        """Test data serialization for bytes"""
        data = b"binary data"
        result = kafka_producer._serialize_data(data)
        assert result == data

    def test_serialize_data_string(self, kafka_producer):
        """Test data serialization for string"""
        data = "string data"
        result = kafka_producer._serialize_data(data)
        assert result == b"string data"

    def test_serialize_data_dict(self, kafka_producer):
        """Test data serialization for dict"""
        data = {"key": "value"}
        result = kafka_producer._serialize_data(data)
        assert result == json.dumps(data).encode("utf-8")

    def test_serialize_data_list(self, kafka_producer):
        """Test data serialization for list"""
        data = [1, 2, 3]
        result = kafka_producer._serialize_data(data)
        assert result == json.dumps(data).encode("utf-8")

    @pytest.mark.asyncio
    async def test_flush_success(self, kafka_producer):
        """Test successful flush operation"""
        mock_producer = Mock()
        mock_producer.flush.return_value = 0
        kafka_producer.producer = mock_producer

        result = await kafka_producer.flush(timeout=5.0)

        assert result == 0
        mock_producer.flush.assert_called_once_with(timeout=5.0)

    @pytest.mark.asyncio
    async def test_flush_no_producer(self, kafka_producer):
        """Test flush without producer"""
        kafka_producer.producer = None

        result = await kafka_producer.flush()

        assert result == 0

    @pytest.mark.asyncio
    async def test_flush_with_remaining_messages(self, kafka_producer):
        """Test flush with remaining messages"""
        mock_producer = Mock()
        mock_producer.flush.return_value = 3
        kafka_producer.producer = mock_producer

        result = await kafka_producer.flush()

        assert result == 3

    def test_get_statistics_success(self, kafka_producer):
        """Test getting Kafka statistics"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer
        kafka_producer._pending_messages = 5

        # Mock the len() function for internal queue length
        mock_producer.__len__ = Mock(return_value=10)

        with patch.object(kafka_producer, 'get_stats', return_value={"base": "stats"}):
            stats = kafka_producer.get_statistics()

        assert "base" in stats
        assert "pending_messages" in stats
        assert stats["pending_messages"] == 5
        assert "internal_queue_length" in stats
        assert stats["internal_queue_length"] == 10

    def test_get_statistics_no_producer(self, kafka_producer):
        """Test getting statistics without producer"""
        kafka_producer.producer = None

        with patch.object(kafka_producer, 'get_stats', return_value={"base": "stats"}):
            stats = kafka_producer.get_statistics()

        assert "base" in stats
        assert "kafka_stats" not in stats

    def test_sanitize_config(self, kafka_producer):
        """Test configuration sanitization (removes sensitive data)"""
        kafka_producer.kafka_config.sasl_password = "secret"
        kafka_producer.kafka_config.ssl_key_password = "secret-key"

        sanitized = kafka_producer._sanitize_config()

        assert sanitized.get("sasl_password") == "***"
        assert sanitized.get("ssl_key_password") == "***"

    @patch('pythia.brokers.kafka.producer.Producer')
    @pytest.mark.asyncio
    async def test_producer_with_security_config(self, mock_producer_class):
        """Test producer with security configuration"""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass"
        )

        producer = KafkaProducer(config=config)
        await producer._connect_impl()

        mock_producer_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_bytes_data(self, kafka_producer):
        """Test sending bytes data"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer
        kafka_producer.default_topic = "test-topic"

        test_data = b"binary data"

        await kafka_producer.send(test_data)

        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args[1]["value"] == test_data

    @pytest.mark.asyncio
    async def test_send_string_data(self, kafka_producer):
        """Test sending string data"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer
        kafka_producer.default_topic = "test-topic"

        test_data = "string data"

        await kafka_producer.send(test_data)

        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args[1]["value"] == b"string data"  # String gets encoded to bytes

    @pytest.mark.asyncio
    async def test_health_check_success(self, kafka_producer):
        """Test successful health check"""
        mock_producer = Mock()
        kafka_producer.producer = mock_producer

        mock_stats = {
            "brokers": {
                "broker1": {"state": "UP"},
                "broker2": {"state": "UP"}
            }
        }
        mock_producer.stats.return_value = json.dumps(mock_stats)

        is_healthy = await kafka_producer._health_check_impl()

        assert is_healthy is True

    @pytest.mark.asyncio
    async def test_health_check_no_producer(self, kafka_producer):
        """Test health check without producer"""
        kafka_producer.producer = None

        is_healthy = await kafka_producer._health_check_impl()

        assert is_healthy is False
