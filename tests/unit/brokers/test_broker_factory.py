"""
Unit tests for broker factory
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from pythia.config.broker_factory import BrokerFactory, BrokerSwitcher, BrokerMigration
from pythia.config.kafka import KafkaConfig
from pythia.config.rabbitmq import RabbitMQConfig
from pythia.config.redis import RedisConfig
from pythia.brokers.kafka import KafkaConsumer
from pythia.brokers.rabbitmq import RabbitMQProducer
from pythia.brokers.redis import (
    RedisStreamsConsumer,
    RedisStreamsProducer,
    RedisPubSubConsumer,
    RedisPubSubProducer,
    RedisListConsumer,
    RedisListProducer,
)


class TestBrokerFactory:
    """Unit tests for BrokerFactory"""

    def test_factory_registry_initialization(self):
        """Test that factory initializes with correct registries"""
        factory = BrokerFactory()

        # Test consumer registry
        assert "kafka" in factory._consumer_registry
        assert "rabbitmq" in factory._consumer_registry
        assert "redis_streams" in factory._consumer_registry
        assert "redis_pubsub" in factory._consumer_registry
        assert "redis_lists" in factory._consumer_registry
        assert "redis" in factory._consumer_registry  # Alias

        # Test producer registry
        assert "kafka" in factory._producer_registry
        assert "rabbitmq" in factory._producer_registry
        assert "redis_streams" in factory._producer_registry
        assert "redis_pubsub" in factory._producer_registry
        assert "redis_lists" in factory._producer_registry
        assert "redis" in factory._producer_registry  # Alias

        # Test config registry
        assert "kafka" in factory._config_registry
        assert "rabbitmq" in factory._config_registry
        assert "redis" in factory._config_registry

    def test_create_consumer_with_config(self):
        """Test creating consumer with explicit config"""
        factory = BrokerFactory()

        config = KafkaConfig(topics=["test"], group_id="test-group")

        with patch.object(KafkaConsumer, "__init__", return_value=None) as mock_init:
            consumer = factory.create_consumer("kafka", config=config)

            mock_init.assert_called_once_with(config=config)
            assert isinstance(consumer, KafkaConsumer)

    def test_create_consumer_without_config(self):
        """Test creating consumer without explicit config (auto-generated)"""
        factory = BrokerFactory()

        with (
            patch.object(KafkaConsumer, "__init__", return_value=None) as mock_init,
            patch.object(
                factory, "create_config", return_value=KafkaConfig()
            ) as mock_config,
        ):
            consumer = factory.create_consumer("kafka")

            mock_config.assert_called_once_with("kafka")
            mock_init.assert_called_once()
            assert isinstance(consumer, KafkaConsumer)

    def test_create_producer_with_kwargs(self):
        """Test creating producer with additional kwargs"""
        factory = BrokerFactory()

        with patch.object(RabbitMQProducer, "__init__", return_value=None) as mock_init:
            producer = factory.create_producer(
                "rabbitmq", exchange="test-exchange", routing_key="test.key"
            )

            # Should be called with auto-generated config
            args, kwargs = mock_init.call_args
            assert "exchange" in kwargs
            assert "routing_key" in kwargs
            assert "config" in kwargs
            assert kwargs["exchange"] == "test-exchange"
            assert kwargs["routing_key"] == "test.key"
            assert isinstance(producer, RabbitMQProducer)

    def test_create_config(self):
        """Test config creation"""
        factory = BrokerFactory()

        # Test Kafka config
        kafka_config = factory.create_config("kafka")
        assert isinstance(kafka_config, KafkaConfig)

        # Test RabbitMQ config
        rabbitmq_config = factory.create_config("rabbitmq")
        assert isinstance(rabbitmq_config, RabbitMQConfig)

        # Test Redis config
        redis_config = factory.create_config("redis")
        assert isinstance(redis_config, RedisConfig)

    def test_unsupported_broker_type_consumer(self):
        """Test error handling for unsupported consumer type"""
        factory = BrokerFactory()

        with pytest.raises(
            ValueError, match="Unsupported consumer broker type: unknown"
        ):
            factory.create_consumer("unknown")

    def test_unsupported_broker_type_producer(self):
        """Test error handling for unsupported producer type"""
        factory = BrokerFactory()

        with pytest.raises(
            ValueError, match="Unsupported producer broker type: unknown"
        ):
            factory.create_producer("unknown")

    def test_unsupported_config_type(self):
        """Test error handling for unsupported config type"""
        factory = BrokerFactory()

        with pytest.raises(ValueError, match="Unsupported config broker type: unknown"):
            factory.create_config("unknown")

    def test_register_custom_consumer(self):
        """Test registering custom consumer"""
        factory = BrokerFactory()

        class CustomConsumer:
            pass

        factory.register_consumer("custom", CustomConsumer)

        assert "custom" in factory._consumer_registry
        assert factory._consumer_registry["custom"] == CustomConsumer

    def test_register_custom_producer(self):
        """Test registering custom producer"""
        factory = BrokerFactory()

        class CustomProducer:
            pass

        factory.register_producer("custom", CustomProducer)

        assert "custom" in factory._producer_registry
        assert factory._producer_registry["custom"] == CustomProducer

    def test_register_custom_config(self):
        """Test registering custom config"""
        factory = BrokerFactory()

        class CustomConfig:
            pass

        factory.register_config("custom", CustomConfig)

        assert "custom" in factory._config_registry
        assert factory._config_registry["custom"] == CustomConfig

    def test_list_supported_brokers(self):
        """Test listing supported brokers"""
        factory = BrokerFactory()

        supported = factory.list_supported_brokers()

        # Check structure
        assert isinstance(supported, dict)

        # Check Kafka entry
        assert "kafka" in supported
        assert supported["kafka"]["consumer"] is True
        assert supported["kafka"]["producer"] is True
        assert supported["kafka"]["config"] is True

        # Check RabbitMQ entry
        assert "rabbitmq" in supported
        assert supported["rabbitmq"]["consumer"] is True
        assert supported["rabbitmq"]["producer"] is True
        assert supported["rabbitmq"]["config"] is True

    def test_redis_variants_in_registry(self):
        """Test that all Redis variants are properly registered"""
        factory = BrokerFactory()

        # Test consumer registry
        assert factory._consumer_registry["redis_streams"] == RedisStreamsConsumer
        assert factory._consumer_registry["redis_pubsub"] == RedisPubSubConsumer
        assert factory._consumer_registry["redis_lists"] == RedisListConsumer
        assert factory._consumer_registry["redis"] == RedisStreamsConsumer  # Alias

        # Test producer registry
        assert factory._producer_registry["redis_streams"] == RedisStreamsProducer
        assert factory._producer_registry["redis_pubsub"] == RedisPubSubProducer
        assert factory._producer_registry["redis_lists"] == RedisListProducer
        assert factory._producer_registry["redis"] == RedisStreamsProducer  # Alias

    def test_create_http_client_webhook(self):
        """Test creating webhook HTTP client"""
        factory = BrokerFactory()

        with patch("pythia.config.broker_factory.WebhookClient") as mock_webhook:
            mock_instance = MagicMock()
            mock_webhook.return_value = mock_instance

            client = factory.create_http_client("webhook", base_url="http://test.com")

            mock_webhook.assert_called_once()
            assert client == mock_instance

    def test_create_http_client_poller(self):
        """Test creating poller HTTP client"""
        factory = BrokerFactory()

        with patch("pythia.config.broker_factory.HTTPPoller") as mock_poller:
            mock_instance = MagicMock()
            mock_poller.return_value = mock_instance

            client = factory.create_http_client("poller", url="http://test.com")

            mock_poller.assert_called_once()
            assert client == mock_instance

    def test_create_http_client_unsupported(self):
        """Test error handling for unsupported HTTP client type"""
        factory = BrokerFactory()

        with pytest.raises(ValueError, match="Unsupported HTTP client type: unknown"):
            factory.create_http_client("unknown")


class TestBrokerSwitcher:
    """Unit tests for BrokerSwitcher"""

    @pytest.fixture
    def mock_factory(self):
        """Mock factory for testing"""
        return MagicMock()

    @pytest.fixture
    def mock_consumer(self):
        """Mock consumer for testing"""
        consumer = AsyncMock()
        consumer.disconnect = AsyncMock()
        consumer.connect = AsyncMock()
        consumer.health_check = AsyncMock(return_value=True)
        return consumer

    @pytest.fixture
    def mock_producer(self):
        """Mock producer for testing"""
        producer = AsyncMock()
        producer.disconnect = AsyncMock()
        producer.connect = AsyncMock()
        producer.health_check = AsyncMock(return_value=True)
        return producer

    def test_switcher_initialization(self):
        """Test switcher initialization"""
        switcher = BrokerSwitcher()

        # Should create default factory
        assert isinstance(switcher.factory, BrokerFactory)
        assert len(switcher._active_consumers) == 0
        assert len(switcher._active_producers) == 0

    def test_switcher_initialization_with_factory(self, mock_factory):
        """Test switcher initialization with custom factory"""
        switcher = BrokerSwitcher(factory=mock_factory)

        assert switcher.factory == mock_factory

    @pytest.mark.asyncio
    async def test_add_consumer(self, mock_factory, mock_consumer):
        """Test adding consumer"""
        mock_factory.create_consumer.return_value = mock_consumer
        switcher = BrokerSwitcher(factory=mock_factory)

        consumer = await switcher.add_consumer(
            "kafka", config="test_config", test_arg="test"
        )

        # Verify factory called correctly
        mock_factory.create_consumer.assert_called_once_with(
            "kafka", "test_config", test_arg="test"
        )

        # Verify consumer connected and added
        mock_consumer.connect.assert_called_once()
        assert consumer in switcher._active_consumers
        assert len(switcher._active_consumers) == 1

    @pytest.mark.asyncio
    async def test_add_producer(self, mock_factory, mock_producer):
        """Test adding producer"""
        mock_factory.create_producer.return_value = mock_producer
        switcher = BrokerSwitcher(factory=mock_factory)

        producer = await switcher.add_producer("rabbitmq", config="test_config")

        # Verify factory called correctly
        mock_factory.create_producer.assert_called_once_with("rabbitmq", "test_config")

        # Verify producer connected and added
        mock_producer.connect.assert_called_once()
        assert producer in switcher._active_producers
        assert len(switcher._active_producers) == 1

    @pytest.mark.asyncio
    async def test_switch_consumer(self, mock_factory, mock_consumer):
        """Test switching consumer"""
        # Setup
        old_consumer = AsyncMock()
        old_consumer.disconnect = AsyncMock()

        mock_factory.create_consumer.return_value = mock_consumer
        switcher = BrokerSwitcher(factory=mock_factory)
        switcher._active_consumers = [old_consumer]  # Add old consumer directly

        # Switch consumer
        new_consumer = await switcher.switch_consumer(
            old_consumer, "redis", config="new_config"
        )

        # Verify old consumer disconnected
        old_consumer.disconnect.assert_called_once()

        # Verify new consumer created and connected
        mock_factory.create_consumer.assert_called_once_with("redis", "new_config")
        mock_consumer.connect.assert_called_once()

        # Verify consumer list updated
        assert old_consumer not in switcher._active_consumers
        assert new_consumer in switcher._active_consumers
        assert len(switcher._active_consumers) == 1

    @pytest.mark.asyncio
    async def test_switch_producer(self, mock_factory, mock_producer):
        """Test switching producer"""
        # Setup
        old_producer = AsyncMock()
        old_producer.disconnect = AsyncMock()

        mock_factory.create_producer.return_value = mock_producer
        switcher = BrokerSwitcher(factory=mock_factory)
        switcher._active_producers = [old_producer]  # Add old producer directly

        # Switch producer
        new_producer = await switcher.switch_producer(
            old_producer, "kafka", config="new_config"
        )

        # Verify old producer disconnected
        old_producer.disconnect.assert_called_once()

        # Verify new producer created and connected
        mock_factory.create_producer.assert_called_once_with("kafka", "new_config")
        mock_producer.connect.assert_called_once()

        # Verify producer list updated
        assert old_producer not in switcher._active_producers
        assert new_producer in switcher._active_producers
        assert len(switcher._active_producers) == 1

    @pytest.mark.asyncio
    async def test_disconnect_all(self, mock_factory):
        """Test disconnecting all brokers"""
        # Setup consumers and producers
        consumer1 = AsyncMock()
        consumer2 = AsyncMock()
        producer1 = AsyncMock()
        producer2 = AsyncMock()

        switcher = BrokerSwitcher(factory=mock_factory)
        switcher._active_consumers = [consumer1, consumer2]
        switcher._active_producers = [producer1, producer2]

        # Disconnect all
        await switcher.disconnect_all()

        # Verify all disconnected
        consumer1.disconnect.assert_called_once()
        consumer2.disconnect.assert_called_once()
        producer1.disconnect.assert_called_once()
        producer2.disconnect.assert_called_once()

        # Verify lists cleared
        assert len(switcher._active_consumers) == 0
        assert len(switcher._active_producers) == 0

    @pytest.mark.asyncio
    async def test_disconnect_all_with_exceptions(self, mock_factory):
        """Test disconnecting all brokers when some fail"""
        # Setup consumers with one that fails
        consumer1 = AsyncMock()
        consumer2 = AsyncMock()
        consumer1.disconnect.side_effect = Exception("Connection error")
        consumer2.disconnect = AsyncMock()  # This should still be called

        switcher = BrokerSwitcher(factory=mock_factory)
        switcher._active_consumers = [consumer1, consumer2]

        # Should not raise exception
        await switcher.disconnect_all()

        # Verify both were attempted
        consumer1.disconnect.assert_called_once()
        consumer2.disconnect.assert_called_once()

        # Verify list cleared despite exception
        assert len(switcher._active_consumers) == 0

    def test_get_active_consumers(self, mock_factory):
        """Test getting active consumers"""
        consumer1 = MagicMock()
        consumer2 = MagicMock()

        switcher = BrokerSwitcher(factory=mock_factory)
        switcher._active_consumers = [consumer1, consumer2]

        active = switcher.get_active_consumers()

        # Should return copy
        assert active == [consumer1, consumer2]
        assert active is not switcher._active_consumers  # Different object

    def test_get_active_producers(self, mock_factory):
        """Test getting active producers"""
        producer1 = MagicMock()
        producer2 = MagicMock()

        switcher = BrokerSwitcher(factory=mock_factory)
        switcher._active_producers = [producer1, producer2]

        active = switcher.get_active_producers()

        # Should return copy
        assert active == [producer1, producer2]
        assert active is not switcher._active_producers  # Different object

    @pytest.mark.asyncio
    async def test_health_check_all(self, mock_factory):
        """Test health checking all brokers"""
        # Setup brokers with different health states
        consumer1 = AsyncMock()
        consumer2 = AsyncMock()
        producer1 = AsyncMock()

        consumer1.health_check = AsyncMock(return_value=True)
        consumer2.health_check = AsyncMock(return_value=False)
        producer1.health_check = AsyncMock(side_effect=Exception("Health check failed"))

        switcher = BrokerSwitcher(factory=mock_factory)
        switcher._active_consumers = [consumer1, consumer2]
        switcher._active_producers = [producer1]

        # Run health check
        results = await switcher.health_check_all()

        # Verify results
        assert results["consumer_0"] is True
        assert results["consumer_1"] is False
        assert results["producer_0"] is False  # Exception should result in False


class TestBrokerMigration:
    """Unit tests for BrokerMigration"""

    @pytest.fixture
    def mock_factory(self):
        return MagicMock()

    def test_migration_initialization(self):
        """Test migration initialization"""
        migration = BrokerMigration()

        # Should create default factory
        assert isinstance(migration.factory, BrokerFactory)

    def test_migration_initialization_with_factory(self, mock_factory):
        """Test migration initialization with custom factory"""
        migration = BrokerMigration(factory=mock_factory)

        assert migration.factory == mock_factory

    @pytest.mark.asyncio
    async def test_migrate_messages_empty_source(self):
        """Test migrating from empty source"""
        migration = BrokerMigration()

        # Mock empty source
        source = AsyncMock()

        async def empty_consume():
            return
            yield  # Make it async generator that yields nothing

        source.consume = empty_consume

        # Mock target
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Migrate
        count = await migration.migrate_messages(source, target)

        # Should migrate 0 messages
        assert count == 0
        target.send_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_migrate_messages_single_batch(self):
        """Test migrating messages in single batch"""
        from pythia.core.message import Message

        migration = BrokerMigration()

        # Mock source with messages
        source = AsyncMock()
        messages = [Message(body=f"msg{i}") for i in range(5)]

        async def mock_consume():
            for msg in messages:
                yield msg

        source.consume = mock_consume

        # Mock target
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Migrate with large batch size
        count = await migration.migrate_messages(source, target, batch_size=10)

        # Should migrate all in one batch
        assert count == 5
        target.send_batch.assert_called_once()

        # Verify batch content
        call_args = target.send_batch.call_args[0][0]  # First argument (batch)
        assert len(call_args) == 5
        assert all(isinstance(msg, Message) for msg in call_args)

    @pytest.mark.asyncio
    async def test_migrate_messages_multiple_batches(self):
        """Test migrating messages in multiple batches"""
        from pythia.core.message import Message

        migration = BrokerMigration()

        # Mock source with messages
        source = AsyncMock()
        messages = [Message(body=f"msg{i}") for i in range(7)]

        async def mock_consume():
            for msg in messages:
                yield msg

        source.consume = mock_consume

        # Mock target
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Migrate with small batch size
        count = await migration.migrate_messages(source, target, batch_size=3)

        # Should migrate all in multiple batches
        assert count == 7
        assert target.send_batch.call_count == 3  # 3 + 3 + 1 messages

        # Verify batch sizes
        call_args_list = target.send_batch.call_args_list
        assert len(call_args_list[0][0][0]) == 3  # First batch: 3 messages
        assert len(call_args_list[1][0][0]) == 3  # Second batch: 3 messages
        assert len(call_args_list[2][0][0]) == 1  # Third batch: 1 message

    @pytest.mark.asyncio
    async def test_migrate_messages_with_transform(self):
        """Test migrating messages with transformation"""
        from pythia.core.message import Message

        migration = BrokerMigration()

        # Mock source
        source = AsyncMock()
        original_msg = Message(body={"value": 10})

        async def mock_consume():
            yield original_msg

        source.consume = mock_consume

        # Mock target
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Transform function
        async def transform_fn(msg):
            return Message(body={"value": msg.body["value"] * 2})

        # Migrate with transform
        count = await migration.migrate_messages(
            source, target, transform_fn=transform_fn
        )

        # Verify transform was applied
        assert count == 1
        call_args = target.send_batch.call_args[0][0]  # First batch
        transformed_msg = call_args[0]
        assert transformed_msg.body["value"] == 20

    @pytest.mark.asyncio
    async def test_migrate_messages_with_max_messages_limit(self):
        """Test migrating with message limit"""
        from pythia.core.message import Message

        migration = BrokerMigration()

        # Mock source with many messages
        source = AsyncMock()

        async def mock_consume():
            for i in range(100):  # More messages than limit
                yield Message(body=f"msg{i}")

        source.consume = mock_consume

        # Mock target
        target = AsyncMock()
        target.send_batch = AsyncMock()

        # Migrate with limit
        count = await migration.migrate_messages(
            source, target, max_messages=5, batch_size=2
        )

        # Should stop at exact limit
        assert count == 5

        # Verify correct number of batches
        assert target.send_batch.call_count == 3  # 2 + 2 + 1 messages
