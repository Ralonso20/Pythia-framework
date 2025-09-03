"""
Comprehensive tests for RabbitMQ consumer implementation
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import aio_pika

from pythia.brokers.rabbitmq.consumer import RabbitMQConsumer
from pythia.config.rabbitmq import RabbitMQConfig
from pythia.core.message import Message


class TestRabbitMQConsumer:
    """Test RabbitMQ consumer functionality"""

    @pytest.fixture
    def rabbitmq_config(self):
        """Create test RabbitMQ config"""
        return RabbitMQConfig(
            url="amqp://test:test@localhost:5672/",
            queue="test-queue",
            exchange="test-exchange",
            routing_key="test.key"
        )

    @pytest.fixture
    def rabbitmq_consumer(self, rabbitmq_config):
        """Create RabbitMQConsumer instance"""
        return RabbitMQConsumer(
            queue="test-queue",
            config=rabbitmq_config
        )

    def test_consumer_initialization_basic(self):
        """Test basic consumer initialization"""
        consumer = RabbitMQConsumer(queue="my-queue")

        assert consumer.queue_name == "my-queue"
        assert consumer.exchange_name is None
        assert consumer.routing_key == "my-queue"  # Defaults to queue name
        assert consumer.exchange_type == "direct"
        assert consumer.connection is None
        assert consumer.channel is None
        assert consumer.queue is None
        assert consumer._consuming is False

    def test_consumer_initialization_with_exchange(self):
        """Test consumer initialization with exchange"""
        consumer = RabbitMQConsumer(
            queue="my-queue",
            exchange="my-exchange",
            routing_key="my.key",
            exchange_type="topic"
        )

        assert consumer.queue_name == "my-queue"
        assert consumer.exchange_name == "my-exchange"
        assert consumer.routing_key == "my.key"
        assert consumer.exchange_type == "topic"

    def test_consumer_initialization_with_kwargs(self):
        """Test consumer initialization with kwargs"""
        consumer = RabbitMQConsumer(
            queue="my-queue",
            url="amqp://test:test@localhost:5672/"
        )

        assert consumer.queue_name == "my-queue"
        assert consumer.config.url == "amqp://test:test@localhost:5672/"

    def test_consumer_initialization_custom_config(self, rabbitmq_config):
        """Test consumer initialization with custom config"""
        consumer = RabbitMQConsumer(
            queue="test-queue",
            config=rabbitmq_config
        )

        assert consumer.config == rabbitmq_config
        assert consumer.config.url == "amqp://test:test@localhost:5672/"

    def test_is_connected_false_no_connection(self, rabbitmq_consumer):
        """Test is_connected returns False when no connection"""
        result = rabbitmq_consumer.is_connected()
        assert result is False

    def test_is_connected_false_closed_connection(self, rabbitmq_consumer):
        """Test is_connected returns False when connection is closed"""
        mock_connection = Mock()
        mock_connection.is_closed = True
        rabbitmq_consumer.connection = mock_connection

        result = rabbitmq_consumer.is_connected()
        assert result is False

    def test_is_connected_true_open_connection(self, rabbitmq_consumer):
        """Test is_connected returns True when connection is open"""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.is_closed = False
        rabbitmq_consumer.connection = mock_connection
        rabbitmq_consumer.channel = mock_channel

        result = rabbitmq_consumer.is_connected()
        assert result is True

    @patch('pythia.brokers.rabbitmq.consumer.aio_pika.connect_robust')
    @pytest.mark.asyncio
    async def test_connect_success_basic(self, mock_connect, rabbitmq_consumer):
        """Test successful basic connection"""
        # Setup mocks
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_queue = AsyncMock()

        mock_connect.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.declare_queue.return_value = mock_queue

        await rabbitmq_consumer.connect()

        assert rabbitmq_consumer.connection == mock_connection
        assert rabbitmq_consumer.channel == mock_channel
        assert rabbitmq_consumer.queue == mock_queue

        mock_connect.assert_called_once()
        mock_channel.set_qos.assert_called_once()

    @patch('pythia.brokers.rabbitmq.consumer.aio_pika.connect_robust')
    @pytest.mark.asyncio
    async def test_connect_success_with_exchange(self, mock_connect):
        """Test successful connection with exchange"""
        consumer = RabbitMQConsumer(
            queue="test-queue",
            exchange="test-exchange",
            routing_key="test.key"
        )

        # Setup mocks
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_exchange = AsyncMock()

        mock_connect.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.declare_exchange.return_value = mock_exchange
        mock_channel.declare_queue.return_value = mock_queue

        await consumer.connect()

        mock_channel.declare_exchange.assert_called_once()
        mock_queue.bind.assert_called_once_with(mock_exchange, routing_key="test.key")

    @patch('pythia.brokers.rabbitmq.consumer.aio_pika.connect_robust')
    @pytest.mark.asyncio
    async def test_connect_already_connected(self, mock_connect, rabbitmq_consumer):
        """Test connect when already connected"""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.is_closed = False
        rabbitmq_consumer.connection = mock_connection
        rabbitmq_consumer.channel = mock_channel

        await rabbitmq_consumer.connect()

        # Should not attempt to connect again
        mock_connect.assert_not_called()

    @patch('pythia.brokers.rabbitmq.consumer.aio_pika.connect_robust')
    @pytest.mark.asyncio
    async def test_connect_failure(self, mock_connect, rabbitmq_consumer):
        """Test connection failure"""
        mock_connect.side_effect = Exception("Connection failed")

        with patch.object(rabbitmq_consumer, 'disconnect') as mock_disconnect:
            with pytest.raises(Exception) as exc_info:
                await rabbitmq_consumer.connect()

        assert "Connection failed" in str(exc_info.value)
        mock_disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_success(self, rabbitmq_consumer):
        """Test successful disconnection"""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        rabbitmq_consumer.connection = mock_connection
        rabbitmq_consumer._consuming = True

        await rabbitmq_consumer.disconnect()

        assert rabbitmq_consumer._consuming is False
        mock_connection.close.assert_called_once()
        assert rabbitmq_consumer.connection is None
        assert rabbitmq_consumer.channel is None
        assert rabbitmq_consumer.queue is None

    @pytest.mark.asyncio
    async def test_disconnect_no_connection(self, rabbitmq_consumer):
        """Test disconnection when no connection"""
        rabbitmq_consumer.connection = None

        # Should not raise exception
        await rabbitmq_consumer.disconnect()

        assert rabbitmq_consumer._consuming is False

    @pytest.mark.asyncio
    async def test_disconnect_connection_error(self, rabbitmq_consumer):
        """Test disconnection with error"""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.close.side_effect = Exception("Close error")
        rabbitmq_consumer.connection = mock_connection

        # Should not raise exception, just log warning
        await rabbitmq_consumer.disconnect()

        assert rabbitmq_consumer._consuming is False

    def test_convert_message_success(self, rabbitmq_consumer):
        """Test successful message conversion"""
        mock_rabbitmq_msg = Mock()
        mock_rabbitmq_msg.body = b'{"test": "data"}'
        mock_rabbitmq_msg.routing_key = "test.key"
        mock_rabbitmq_msg.exchange = "test-exchange"
        mock_rabbitmq_msg.delivery_tag = 123
        mock_rabbitmq_msg.headers = {"header1": "value1"}
        mock_rabbitmq_msg.timestamp = 1634567890
        mock_rabbitmq_msg.message_id = "msg-123"
        mock_rabbitmq_msg.correlation_id = "corr-123"

        message = rabbitmq_consumer._convert_message(mock_rabbitmq_msg)

        assert isinstance(message, Message)
        assert message.routing_key == "test.key"
        assert message.exchange == "test-exchange"
        assert message.delivery_tag == 123

    def test_convert_message_json_decode_error(self, rabbitmq_consumer):
        """Test message conversion with invalid JSON"""
        mock_rabbitmq_msg = Mock()
        mock_rabbitmq_msg.body = b'invalid-json'
        mock_rabbitmq_msg.routing_key = "test.key"
        mock_rabbitmq_msg.exchange = "test-exchange"
        mock_rabbitmq_msg.delivery_tag = 123
        mock_rabbitmq_msg.headers = {}
        mock_rabbitmq_msg.timestamp = None
        mock_rabbitmq_msg.message_id = None
        mock_rabbitmq_msg.correlation_id = None

        message = rabbitmq_consumer._convert_message(mock_rabbitmq_msg)

        assert isinstance(message, Message)
        # Should fallback to raw bytes as string
        assert message.body == "invalid-json"

    @pytest.mark.asyncio
    async def test_acknowledge_success(self, rabbitmq_consumer):
        """Test successful message acknowledgment"""
        mock_rabbitmq_msg = Mock()
        message = Message(body="test", message_id="123")
        message._raw_message = mock_rabbitmq_msg

        await rabbitmq_consumer.acknowledge(message)

        mock_rabbitmq_msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_acknowledge_no_raw_message(self, rabbitmq_consumer):
        """Test acknowledge with no raw message"""
        message = Message(body="test", message_id="123")

        # Should not raise exception
        await rabbitmq_consumer.acknowledge(message)

    @pytest.mark.asyncio
    async def test_reject_success(self, rabbitmq_consumer):
        """Test successful message rejection"""
        mock_rabbitmq_msg = Mock()
        message = Message(body="test", message_id="123")
        message._raw_message = mock_rabbitmq_msg

        await rabbitmq_consumer.reject(message, requeue=False)

        mock_rabbitmq_msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_reject_with_requeue(self, rabbitmq_consumer):
        """Test message rejection with requeue"""
        mock_rabbitmq_msg = Mock()
        message = Message(body="test", message_id="123")
        message._raw_message = mock_rabbitmq_msg

        await rabbitmq_consumer.reject(message, requeue=True)

        mock_rabbitmq_msg.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_health_check_success(self, rabbitmq_consumer):
        """Test successful health check"""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_temp_queue = AsyncMock()

        rabbitmq_consumer.connection = mock_connection
        rabbitmq_consumer.channel = mock_channel
        mock_channel.declare_queue.return_value = mock_temp_queue

        is_healthy = await rabbitmq_consumer.health_check()

        assert is_healthy is True
        mock_channel.declare_queue.assert_called_once()
        mock_temp_queue.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_no_connection(self, rabbitmq_consumer):
        """Test health check with no connection"""
        rabbitmq_consumer.connection = None

        is_healthy = await rabbitmq_consumer.health_check()

        assert is_healthy is False

    @pytest.mark.asyncio
    async def test_health_check_closed_connection(self, rabbitmq_consumer):
        """Test health check with closed connection"""
        mock_connection = Mock()
        mock_connection.is_closed = True
        rabbitmq_consumer.connection = mock_connection

        is_healthy = await rabbitmq_consumer.health_check()

        assert is_healthy is False

    @pytest.mark.asyncio
    async def test_get_queue_info_success(self, rabbitmq_consumer):
        """Test getting queue information"""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_queue = AsyncMock()

        # Mock queue declare response
        mock_method = Mock()
        mock_method.message_count = 10
        mock_method.consumer_count = 2
        mock_response = Mock()
        mock_response.method = mock_method

        rabbitmq_consumer.connection = mock_connection
        rabbitmq_consumer.channel = mock_channel
        rabbitmq_consumer.queue = mock_queue
        mock_queue.channel.queue_declare.return_value = mock_response

        info = await rabbitmq_consumer.get_queue_info()

        assert info["queue"] == "test-queue"
        assert info["message_count"] == 10
        assert info["consumer_count"] == 2

    @pytest.mark.asyncio
    async def test_consume_not_connected_raises_error(self, rabbitmq_consumer):
        """Test consume raises error when queue not initialized"""
        rabbitmq_consumer.connection = None
        rabbitmq_consumer.channel = None
        rabbitmq_consumer.queue = None

        with patch.object(rabbitmq_consumer, 'is_connected', return_value=True):
            with pytest.raises(RuntimeError) as exc_info:
                async for _ in rabbitmq_consumer.consume():
                    break

            assert "Queue not initialized" in str(exc_info.value)

    def test_consumer_logger_name(self, rabbitmq_consumer):
        """Test consumer logger has correct name"""
        assert "RabbitMQConsumer[test-queue]" in rabbitmq_consumer.logger.name
