"""
Integration tests for multi-broker support in Pythia
"""

import pytest
from unittest.mock import AsyncMock

from pythia.config.broker_factory import BrokerFactory, BrokerSwitcher, BrokerMigration
from pythia.config.kafka import KafkaConfig
from pythia.config.rabbitmq import RabbitMQConfig
from pythia.config.redis import RedisConfig
from pythia.core.message import Message
from pythia.brokers.kafka import KafkaConsumer, KafkaProducer
from pythia.brokers.rabbitmq import RabbitMQConsumer, RabbitMQProducer
from pythia.brokers.redis import (
    RedisStreamsConsumer,
    RedisStreamsProducer,
    RedisPubSubConsumer,
    RedisPubSubProducer,
    RedisListConsumer,
    RedisListProducer,
)


class TestBrokerFactory:
    """Test broker factory functionality"""

    def test_factory_initialization(self):
        """Test factory initializes with all supported brokers"""
        factory = BrokerFactory()

        supported = factory.list_supported_brokers()

        # Check main brokers are registered
        assert "kafka" in supported
        assert "rabbitmq" in supported
        assert "redis_streams" in supported
        assert "redis_pubsub" in supported
        assert "redis_lists" in supported

        # Check capabilities
        assert supported["kafka"]["consumer"]
        assert supported["kafka"]["producer"]
        assert supported["rabbitmq"]["consumer"]
        assert supported["rabbitmq"]["producer"]

    def test_create_kafka_consumer(self):
        """Test creating Kafka consumer"""
        factory = BrokerFactory()

        consumer = factory.create_consumer(
            "kafka", config=KafkaConfig(topics=["test-topic"]), topics=["test-topic"]
        )

        assert isinstance(consumer, KafkaConsumer)
        assert consumer.config.topics == ["test-topic"]

    def test_create_rabbitmq_producer(self):
        """Test creating RabbitMQ producer"""
        factory = BrokerFactory()

        producer = factory.create_producer(
            "rabbitmq",
            config=RabbitMQConfig(exchange="test-exchange"),
            exchange="test-exchange",
        )

        assert isinstance(producer, RabbitMQProducer)
        assert producer.exchange_name == "test-exchange"

    def test_create_redis_streams_consumer(self):
        """Test creating Redis Streams consumer"""
        factory = BrokerFactory()

        consumer = factory.create_consumer(
            "redis_streams",
            config=RedisConfig(stream="test-stream"),
            stream="test-stream",
            consumer_group="test-group",
        )

        assert isinstance(consumer, RedisStreamsConsumer)
        assert consumer.stream == "test-stream"
        assert consumer.consumer_group == "test-group"

    def test_create_redis_pubsub_consumer(self):
        """Test creating Redis Pub/Sub consumer"""
        factory = BrokerFactory()

        consumer = factory.create_consumer(
            "redis_pubsub", channels=["test-channel"], patterns=["test.*"]
        )

        assert isinstance(consumer, RedisPubSubConsumer)
        assert "test-channel" in consumer.channels
        assert "test.*" in consumer.patterns

    def test_create_redis_lists_producer(self):
        """Test creating Redis Lists producer"""
        factory = BrokerFactory()

        producer = factory.create_producer("redis_lists", queue="test-queue")

        assert isinstance(producer, RedisListProducer)
        assert producer.queue == "test-queue"

    def test_unsupported_broker_type(self):
        """Test error handling for unsupported broker types"""
        factory = BrokerFactory()

        with pytest.raises(ValueError, match="Unsupported consumer broker type"):
            factory.create_consumer("unsupported")

        with pytest.raises(ValueError, match="Unsupported producer broker type"):
            factory.create_producer("unsupported")

    def test_custom_broker_registration(self):
        """Test registering custom broker types"""
        factory = BrokerFactory()

        # Mock custom broker
        class CustomConsumer:
            pass

        class CustomProducer:
            pass

        class CustomConfig:
            pass

        # Register custom types
        factory.register_consumer("custom", CustomConsumer)
        factory.register_producer("custom", CustomProducer)
        factory.register_config("custom", CustomConfig)

        # Check registration
        supported = factory.list_supported_brokers()
        assert "custom" in supported
        assert supported["custom"]["consumer"]
        assert supported["custom"]["producer"]
        assert supported["custom"]["config"]


class TestBrokerSwitcher:
    """Test broker switching functionality"""

    @pytest.fixture
    def mock_consumer(self):
        consumer = AsyncMock()
        consumer.disconnect = AsyncMock()
        consumer.connect = AsyncMock()
        consumer.health_check = AsyncMock(return_value=True)
        return consumer

    @pytest.fixture
    def mock_producer(self):
        producer = AsyncMock()
        producer.disconnect = AsyncMock()
        producer.connect = AsyncMock()
        producer.health_check = AsyncMock(return_value=True)
        return producer

    @pytest.fixture
    def mock_factory(self, mock_consumer, mock_producer):
        from unittest.mock import Mock

        factory = Mock()
        # Make create_consumer a regular Mock that returns the async mock
        # Use side_effect to return different consumers each time
        factory.create_consumer.side_effect = [
            mock_consumer,
            mock_producer,
        ]  # Will be mocked per test
        factory.create_producer.return_value = mock_producer
        return factory

    @pytest.mark.asyncio
    async def test_switch_consumer(self, mock_factory):
        """Test switching between consumers"""
        switcher = BrokerSwitcher(factory=mock_factory)

        # Create separate mock consumers for the test
        kafka_consumer = AsyncMock()
        kafka_consumer.disconnect = AsyncMock()
        kafka_consumer.connect = AsyncMock()

        rabbitmq_consumer = AsyncMock()
        rabbitmq_consumer.disconnect = AsyncMock()
        rabbitmq_consumer.connect = AsyncMock()

        # Configure mock to return different consumers
        mock_factory.create_consumer.side_effect = [kafka_consumer, rabbitmq_consumer]

        # Add initial consumer
        old_consumer = await switcher.add_consumer("kafka")
        assert len(switcher.get_active_consumers()) == 1

        # Switch to new consumer
        new_consumer = await switcher.switch_consumer(
            old_consumer, "rabbitmq", config=RabbitMQConfig()
        )

        # Verify old consumer was disconnected
        old_consumer.disconnect.assert_called_once()

        # Verify new consumer was connected
        new_consumer.connect.assert_called_once()

        # Verify consumer list updated
        assert len(switcher.get_active_consumers()) == 1
        assert new_consumer in switcher.get_active_consumers()

    @pytest.mark.asyncio
    async def test_add_multiple_consumers(self, mock_factory):
        """Test adding multiple consumers"""
        switcher = BrokerSwitcher(factory=mock_factory)

        # Create separate mock consumers
        consumer1_mock = AsyncMock()
        consumer1_mock.connect = AsyncMock()
        consumer1_mock.disconnect = AsyncMock()

        consumer2_mock = AsyncMock()
        consumer2_mock.connect = AsyncMock()
        consumer2_mock.disconnect = AsyncMock()

        consumer3_mock = AsyncMock()
        consumer3_mock.connect = AsyncMock()
        consumer3_mock.disconnect = AsyncMock()

        mock_factory.create_consumer.side_effect = [
            consumer1_mock,
            consumer2_mock,
            consumer3_mock,
        ]

        # Add multiple consumers
        consumer1 = await switcher.add_consumer("kafka")
        consumer2 = await switcher.add_consumer("rabbitmq")
        consumer3 = await switcher.add_consumer("redis_streams")

        # Verify all consumers are active
        active = switcher.get_active_consumers()
        assert len(active) == 3
        assert consumer1 in active
        assert consumer2 in active
        assert consumer3 in active

    @pytest.mark.asyncio
    async def test_disconnect_all(self, mock_factory):
        """Test disconnecting all brokers"""
        switcher = BrokerSwitcher(factory=mock_factory)

        # Create mock consumers and producers
        consumer1_mock = AsyncMock()
        consumer1_mock.connect = AsyncMock()
        consumer1_mock.disconnect = AsyncMock()

        consumer2_mock = AsyncMock()
        consumer2_mock.connect = AsyncMock()
        consumer2_mock.disconnect = AsyncMock()

        producer1_mock = AsyncMock()
        producer1_mock.connect = AsyncMock()
        producer1_mock.disconnect = AsyncMock()

        producer2_mock = AsyncMock()
        producer2_mock.connect = AsyncMock()
        producer2_mock.disconnect = AsyncMock()

        mock_factory.create_consumer.side_effect = [consumer1_mock, consumer2_mock]
        mock_factory.create_producer.side_effect = [producer1_mock, producer2_mock]

        # Add consumers and producers
        consumer1 = await switcher.add_consumer("kafka")
        consumer2 = await switcher.add_consumer("rabbitmq")
        producer1 = await switcher.add_producer("kafka")
        producer2 = await switcher.add_producer("redis_streams")

        # Disconnect all
        await switcher.disconnect_all()

        # Verify all were disconnected
        consumer1.disconnect.assert_called_once()
        consumer2.disconnect.assert_called_once()
        producer1.disconnect.assert_called_once()
        producer2.disconnect.assert_called_once()

        # Verify lists are empty
        assert len(switcher.get_active_consumers()) == 0
        assert len(switcher.get_active_producers()) == 0

    @pytest.mark.asyncio
    async def test_health_check_all(self, mock_factory):
        """Test health checking all brokers"""
        switcher = BrokerSwitcher(factory=mock_factory)

        # Create mock brokers
        consumer_mock = AsyncMock()
        consumer_mock.connect = AsyncMock()
        consumer_mock.disconnect = AsyncMock()
        consumer_mock.health_check = AsyncMock(return_value=True)

        producer_mock = AsyncMock()
        producer_mock.connect = AsyncMock()
        producer_mock.disconnect = AsyncMock()
        producer_mock.health_check = AsyncMock(return_value=False)

        mock_factory.create_consumer.return_value = consumer_mock
        mock_factory.create_producer.return_value = producer_mock

        # Add brokers
        _consumer = await switcher.add_consumer("kafka")
        _producer = await switcher.add_producer("rabbitmq")

        # Run health check
        results = await switcher.health_check_all()

        # Verify results
        assert results["consumer_0"] is True
        assert results["producer_0"] is False


class TestBrokerMigration:
    """Test broker migration functionality"""

    @pytest.mark.asyncio
    async def test_migrate_messages_basic(self):
        """Test basic message migration"""
        migration = BrokerMigration()

        # Mock source broker with messages
        source = AsyncMock()
        test_messages = [
            Message(body={"id": 1, "data": "test1"}),
            Message(body={"id": 2, "data": "test2"}),
            Message(body={"id": 3, "data": "test3"}),
        ]

        async def mock_consume():
            for msg in test_messages:
                yield msg

        source.consume = mock_consume

        # Mock target broker
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Migrate messages
        count = await migration.migrate_messages(source, target, batch_size=2)

        # Verify migration
        assert count == 3
        assert target.send_batch.call_count == 2  # 2 batches (2 + 1 messages)

    @pytest.mark.asyncio
    async def test_migrate_with_transform(self):
        """Test migration with message transformation"""
        migration = BrokerMigration()

        # Mock source with one message
        source = AsyncMock()
        original_msg = Message(body={"value": 10})

        async def mock_consume():
            yield original_msg

        source.consume = mock_consume

        # Mock target
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Transform function that doubles the value
        async def transform_fn(msg):
            new_body = msg.body.copy()
            new_body["value"] *= 2
            return Message(body=new_body)

        # Migrate with transform
        count = await migration.migrate_messages(
            source, target, transform_fn=transform_fn
        )

        # Verify transformation occurred
        assert count == 1
        target.send_batch.assert_called_once()

        # Check the transformed message
        call_args = target.send_batch.call_args[0][0]  # First batch
        transformed_msg = call_args[0]  # First message in batch
        assert transformed_msg.body["value"] == 20

    @pytest.mark.asyncio
    async def test_migrate_with_max_messages(self):
        """Test migration with message limit"""
        migration = BrokerMigration()

        # Mock source with many messages
        source = AsyncMock()

        async def mock_consume():
            for i in range(100):
                yield Message(body={"id": i})

        source.consume = mock_consume

        # Mock target
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Migrate with limit
        count = await migration.migrate_messages(
            source, target, max_messages=10, batch_size=5
        )

        # Verify limit was respected
        assert count == 10
        assert target.send_batch.call_count == 2  # 2 batches of 5 each


class TestBrokerFactoryIntegration:
    """Integration tests with real broker configurations"""

    def test_kafka_config_integration(self):
        """Test Kafka integration with real config"""
        factory = BrokerFactory()

        # Create with explicit config
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            topics=["integration-test"],
            group_id="test-group",
        )

        consumer = factory.create_consumer("kafka", config=config)
        producer = factory.create_producer("kafka", config=config)

        assert isinstance(consumer, KafkaConsumer)
        assert isinstance(producer, KafkaProducer)
        assert consumer.config.bootstrap_servers == "localhost:9092"
        assert producer.config.bootstrap_servers == "localhost:9092"

    def test_rabbitmq_config_integration(self):
        """Test RabbitMQ integration with real config"""
        factory = BrokerFactory()

        config = RabbitMQConfig(
            url="amqp://localhost:5672",
            queue="integration-test",
            exchange="test-exchange",
        )

        consumer = factory.create_consumer(
            "rabbitmq", config=config, queue="integration-test"
        )
        producer = factory.create_producer(
            "rabbitmq", config=config, exchange="test-exchange"
        )

        assert isinstance(consumer, RabbitMQConsumer)
        assert isinstance(producer, RabbitMQProducer)
        assert consumer.config.url == "amqp://localhost:5672"
        assert producer.config.url == "amqp://localhost:5672"

    def test_redis_config_integration(self):
        """Test Redis integration with real config"""
        factory = BrokerFactory()

        config = RedisConfig(
            host="localhost", port=6379, db=0, stream="integration-test"
        )

        # Test all Redis variants
        streams_consumer = factory.create_consumer(
            "redis_streams",
            config=config,
            stream="integration-test",
            consumer_group="test-group",
        )
        streams_producer = factory.create_producer(
            "redis_streams", config=config, stream="integration-test"
        )

        pubsub_consumer = factory.create_consumer(
            "redis_pubsub", config=config, channels=["test-channel"]
        )
        pubsub_producer = factory.create_producer(
            "redis_pubsub", config=config, channel="test-channel"
        )

        lists_consumer = factory.create_consumer(
            "redis_lists", config=config, queue="integration-test"
        )
        lists_producer = factory.create_producer(
            "redis_lists", config=config, queue="integration-test"
        )

        # Verify types
        assert isinstance(streams_consumer, RedisStreamsConsumer)
        assert isinstance(streams_producer, RedisStreamsProducer)
        assert isinstance(pubsub_consumer, RedisPubSubConsumer)
        assert isinstance(pubsub_producer, RedisPubSubProducer)
        assert isinstance(lists_consumer, RedisListConsumer)
        assert isinstance(lists_producer, RedisListProducer)

        # Verify config propagation
        assert streams_consumer.config.host == "localhost"
        assert streams_producer.config.host == "localhost"

    def test_backward_compatibility_aliases(self):
        """Test that backward compatibility aliases work"""
        factory = BrokerFactory()

        # "redis" should still work as alias for streams
        consumer = factory.create_consumer(
            "redis", stream="test-stream", consumer_group="test-group"
        )
        producer = factory.create_producer("redis", stream="test-stream")

        assert isinstance(consumer, RedisStreamsConsumer)
        assert isinstance(producer, RedisStreamsProducer)


@pytest.mark.asyncio
class TestMultiBrokerWorkerIntegration:
    """Test multi-broker support in Worker context"""

    @pytest.mark.asyncio
    async def test_worker_with_multiple_sources(self):
        """Test worker with multiple broker sources"""
        from pythia.core.worker import Worker
        from pythia.config.base import WorkerConfig

        # Mock brokers
        kafka_consumer = AsyncMock()
        redis_consumer = AsyncMock()

        # Create worker with multiple sources
        class MultiSourceWorker(Worker):
            def __init__(self):
                super().__init__(WorkerConfig())
                self._sources = [kafka_consumer, redis_consumer]

            async def process(self, message):
                return f"processed: {message.body}"

        worker = MultiSourceWorker()

        # Test that worker initializes correctly
        assert len(worker._sources) == 2
        assert kafka_consumer in worker._sources
        assert redis_consumer in worker._sources

    @pytest.mark.asyncio
    async def test_broker_switching_in_runtime(self):
        """Test switching brokers during runtime"""
        from unittest.mock import Mock

        # Create mock factory
        mock_factory = Mock()
        switcher = BrokerSwitcher(factory=mock_factory)

        # Mock brokers
        kafka_consumer = AsyncMock()
        kafka_consumer.connect = AsyncMock()
        kafka_consumer.disconnect = AsyncMock()

        redis_consumer = AsyncMock()
        redis_consumer.connect = AsyncMock()
        redis_consumer.disconnect = AsyncMock()

        mock_factory.create_consumer.side_effect = [kafka_consumer, redis_consumer]

        # Start with Kafka
        current_consumer = await switcher.add_consumer("kafka")
        assert current_consumer == kafka_consumer

        # Switch to Redis
        new_consumer = await switcher.switch_consumer(current_consumer, "redis_streams")

        # Verify switch happened
        assert new_consumer == redis_consumer
        kafka_consumer.disconnect.assert_called_once()
        redis_consumer.connect.assert_called_once()
