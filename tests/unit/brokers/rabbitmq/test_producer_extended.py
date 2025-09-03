"""
Comprehensive tests for RabbitMQ producer implementation
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from pythia.brokers.rabbitmq.producer import RabbitMQProducer
from pythia.config.rabbitmq import RabbitMQConfig


class TestRabbitMQProducer:
    """Test RabbitMQ producer functionality"""

    @pytest.fixture
    def rabbitmq_config(self):
        """Create test RabbitMQ config"""
        return RabbitMQConfig(
            url="amqp://test:test@localhost:5672/",
            exchange="test-exchange"
        )

    @pytest.fixture
    def rabbitmq_producer(self, rabbitmq_config):
        """Create RabbitMQProducer instance"""
        return RabbitMQProducer(
            exchange="test-exchange",
            config=rabbitmq_config
        )

    def test_producer_initialization_basic(self):
        """Test basic producer initialization"""
        producer = RabbitMQProducer(exchange="my-exchange")

        assert producer.exchange_name == "my-exchange"
        assert producer.default_routing_key == ""
        assert producer.exchange_type == "direct"
        assert producer.connection is None
        assert producer.channel is None
        assert producer.exchange is None

    def test_producer_initialization_with_routing_key(self):
        """Test producer initialization with routing key"""
        producer = RabbitMQProducer(
            exchange="my-exchange",
            default_routing_key="my.key",
            exchange_type="topic"
        )

        assert producer.exchange_name == "my-exchange"
        assert producer.default_routing_key == "my.key"
        assert producer.exchange_type == "topic"

    def test_producer_initialization_custom_config(self, rabbitmq_config):
        """Test producer initialization with custom config"""
        producer = RabbitMQProducer(
            exchange="test-exchange",
            config=rabbitmq_config
        )

        assert producer.config == rabbitmq_config
        assert producer.config.url == "amqp://test:test@localhost:5672/"

    def test_is_connected_false_no_connection(self, rabbitmq_producer):
        """Test is_connected returns False when no connection"""
        result = rabbitmq_producer.is_connected()
        assert result is False

    def test_is_connected_true_open_connection(self, rabbitmq_producer):
        """Test is_connected returns True when connection is open"""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.is_closed = False
        rabbitmq_producer.connection = mock_connection
        rabbitmq_producer.channel = mock_channel

        result = rabbitmq_producer.is_connected()
        assert result is True

    @patch('pythia.brokers.rabbitmq.producer.aio_pika.connect_robust')
    @pytest.mark.asyncio
    async def test_connect_success_basic(self, mock_connect, rabbitmq_producer):
        """Test successful basic connection"""
        # Setup mocks
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()

        mock_connect.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.declare_exchange.return_value = mock_exchange

        await rabbitmq_producer.connect()

        assert rabbitmq_producer.connection == mock_connection
        assert rabbitmq_producer.channel == mock_channel
        assert rabbitmq_producer.exchange == mock_exchange

        mock_connect.assert_called_once()
        mock_channel.declare_exchange.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_success(self, rabbitmq_producer):
        """Test successful disconnection"""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        rabbitmq_producer.connection = mock_connection

        await rabbitmq_producer.disconnect()

        mock_connection.close.assert_called_once()
        assert rabbitmq_producer.connection is None
        assert rabbitmq_producer.channel is None
        assert rabbitmq_producer.exchange is None

    @pytest.mark.asyncio
    async def test_send_basic_message(self, rabbitmq_producer):
        """Test sending basic message"""
        # Mock the connection state
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()

        rabbitmq_producer.connection = mock_connection
        rabbitmq_producer.channel = mock_channel
        rabbitmq_producer.exchange = mock_exchange
        rabbitmq_producer.default_routing_key = "test.key"

        test_data = {"message": "hello"}

        result = await rabbitmq_producer.send(test_data)

        assert result is True
        mock_exchange.publish.assert_called_once()

        # Verify message content
        call_args = mock_exchange.publish.call_args
        message = call_args[0][0]
        routing_key = call_args[1]["routing_key"]

        assert routing_key == "test.key"
        assert message.body is not None

    @pytest.mark.asyncio
    async def test_send_with_custom_routing_key(self, rabbitmq_producer):
        """Test sending message with custom routing key"""
        # Mock the connection state
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()

        rabbitmq_producer.connection = mock_connection
        rabbitmq_producer.channel = mock_channel
        rabbitmq_producer.exchange = mock_exchange

        result = await rabbitmq_producer.send({"data": "test"}, routing_key="custom.key")

        assert result is True
        call_args = mock_exchange.publish.call_args
        routing_key = call_args[1]["routing_key"]
        assert routing_key == "custom.key"

    @pytest.mark.asyncio
    async def test_health_check_success(self, rabbitmq_producer):
        """Test successful health check"""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_temp_queue = AsyncMock()

        rabbitmq_producer.connection = mock_connection
        rabbitmq_producer.channel = mock_channel
        mock_channel.declare_queue.return_value = mock_temp_queue

        is_healthy = await rabbitmq_producer.health_check()

        assert is_healthy is True
        mock_channel.declare_queue.assert_called_once()
        mock_temp_queue.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_no_connection(self, rabbitmq_producer):
        """Test health check with no connection"""
        rabbitmq_producer.connection = None

        is_healthy = await rabbitmq_producer.health_check()

        assert is_healthy is False

    def test_producer_logger_name(self, rabbitmq_producer):
        """Test producer logger has correct name"""
        assert "RabbitMQProducer[test-exchange]" in rabbitmq_producer.logger.name
