"""
Broker factory and switching utilities for Pythia
"""

from typing import Any, Dict, List, Optional, Type, Union

from .kafka import KafkaConfig
from .rabbitmq import RabbitMQConfig
from .redis import RedisConfig
from .http import HTTPConfig
from ..brokers.base import MessageBroker, MessageProducer
from ..brokers.kafka import KafkaConsumer, KafkaProducer
from ..brokers.rabbitmq import RabbitMQConsumer, RabbitMQProducer
from ..brokers.redis import (
    RedisStreamsConsumer,
    RedisStreamsProducer,
    RedisPubSubConsumer,
    RedisPubSubProducer,
    RedisListConsumer,
    RedisListProducer,
)
from ..http import WebhookClient, HTTPPoller


class BrokerFactory:
    """Factory for creating message brokers from configuration"""

    def __init__(self):
        self._consumer_registry: Dict[str, Type[MessageBroker]] = {
            "kafka": KafkaConsumer,
            "rabbitmq": RabbitMQConsumer,
            "redis_streams": RedisStreamsConsumer,
            "redis_pubsub": RedisPubSubConsumer,
            "redis_lists": RedisListConsumer,
            # Aliases for backward compatibility
            "redis": RedisStreamsConsumer,
        }

        self._producer_registry: Dict[str, Type[MessageProducer]] = {
            "kafka": KafkaProducer,
            "rabbitmq": RabbitMQProducer,
            "redis_streams": RedisStreamsProducer,
            "redis_pubsub": RedisPubSubProducer,
            "redis_lists": RedisListProducer,
            # Aliases for backward compatibility
            "redis": RedisStreamsProducer,
        }

        self._config_registry: Dict[str, Type] = {
            "kafka": KafkaConfig,
            "rabbitmq": RabbitMQConfig,
            "redis": RedisConfig,
            "redis_streams": RedisConfig,
            "redis_pubsub": RedisConfig,
            "redis_lists": RedisConfig,
            "http": HTTPConfig,
        }

    def create_consumer(
        self, broker_type: str, config: Optional[Any] = None, **kwargs
    ) -> MessageBroker:
        """Create a consumer for the specified broker type"""

        if broker_type not in self._consumer_registry:
            raise ValueError(f"Unsupported consumer broker type: {broker_type}")

        consumer_class = self._consumer_registry[broker_type]

        if config is None:
            config = self.create_config(broker_type)

        return consumer_class(**kwargs, config=config)

    def create_producer(
        self, broker_type: str, config: Optional[Any] = None, **kwargs
    ) -> MessageProducer:
        """Create a producer for the specified broker type"""

        if broker_type not in self._producer_registry:
            raise ValueError(f"Unsupported producer broker type: {broker_type}")

        producer_class = self._producer_registry[broker_type]

        if config is None:
            config = self.create_config(broker_type)

        return producer_class(**kwargs, config=config)

    def create_config(self, broker_type: str) -> Any:
        """Create configuration for the specified broker type"""

        if broker_type not in self._config_registry:
            raise ValueError(f"Unsupported config broker type: {broker_type}")

        config_class = self._config_registry[broker_type]
        return config_class()

    def create_http_client(
        self,
        client_type: str = "webhook",
        config: Optional[HTTPConfig] = None,
        **kwargs,
    ) -> Union[WebhookClient, HTTPPoller]:
        """Create HTTP client (webhook or poller)"""

        if client_type == "webhook":
            return WebhookClient(**kwargs)  # WebhookClient doesn't take HTTPConfig
        elif client_type == "poller":
            return HTTPPoller(**kwargs)  # HTTPPoller doesn't take HTTPConfig
        else:
            raise ValueError(f"Unsupported HTTP client type: {client_type}")

    def register_consumer(self, broker_type: str, consumer_class: Type[MessageBroker]) -> None:
        """Register a custom consumer class"""
        self._consumer_registry[broker_type] = consumer_class

    def register_producer(self, broker_type: str, producer_class: Type[MessageProducer]) -> None:
        """Register a custom producer class"""
        self._producer_registry[broker_type] = producer_class

    def register_config(self, broker_type: str, config_class: Type) -> None:
        """Register a custom configuration class"""
        self._config_registry[broker_type] = config_class

    def list_supported_brokers(self) -> Dict[str, Dict[str, bool]]:
        """List all supported broker types and their capabilities"""
        brokers = {}

        all_types = set(self._consumer_registry.keys()) | set(self._producer_registry.keys())

        for broker_type in all_types:
            brokers[broker_type] = {
                "consumer": broker_type in self._consumer_registry,
                "producer": broker_type in self._producer_registry,
                "config": broker_type in self._config_registry,
            }

        return brokers


class BrokerSwitcher:
    """Utility for switching between different message brokers"""

    def __init__(self, factory: Optional[BrokerFactory] = None):
        self.factory = factory or BrokerFactory()
        self._active_consumers: List[MessageBroker] = []
        self._active_producers: List[MessageProducer] = []

    async def switch_consumer(
        self,
        from_broker: MessageBroker,
        to_broker_type: str,
        config: Optional[Any] = None,
        **kwargs,
    ) -> MessageBroker:
        """Switch from one consumer to another"""

        # Disconnect the old broker
        if from_broker in self._active_consumers:
            await from_broker.disconnect()
            self._active_consumers.remove(from_broker)

        # Create and connect the new broker
        new_broker = self.factory.create_consumer(to_broker_type, config, **kwargs)
        await new_broker.connect()
        self._active_consumers.append(new_broker)

        return new_broker

    async def switch_producer(
        self,
        from_broker: MessageProducer,
        to_broker_type: str,
        config: Optional[Any] = None,
        **kwargs,
    ) -> MessageProducer:
        """Switch from one producer to another"""

        # Disconnect the old broker
        if from_broker in self._active_producers:
            await from_broker.disconnect()
            self._active_producers.remove(from_broker)

        # Create and connect the new broker
        new_broker = self.factory.create_producer(to_broker_type, config, **kwargs)
        await new_broker.connect()
        self._active_producers.append(new_broker)

        return new_broker

    async def add_consumer(
        self, broker_type: str, config: Optional[Any] = None, **kwargs
    ) -> MessageBroker:
        """Add a new consumer without removing existing ones"""

        new_broker = self.factory.create_consumer(broker_type, config, **kwargs)
        await new_broker.connect()
        self._active_consumers.append(new_broker)

        return new_broker

    async def add_producer(
        self, broker_type: str, config: Optional[Any] = None, **kwargs
    ) -> MessageProducer:
        """Add a new producer without removing existing ones"""

        new_broker = self.factory.create_producer(broker_type, config, **kwargs)
        await new_broker.connect()
        self._active_producers.append(new_broker)

        return new_broker

    async def disconnect_all(self) -> None:
        """Disconnect all active brokers"""

        # Disconnect all consumers
        for consumer in self._active_consumers:
            try:
                await consumer.disconnect()
            except Exception:
                # Log the error but continue disconnecting others
                pass

        # Disconnect all producers
        for producer in self._active_producers:
            try:
                await producer.disconnect()
            except Exception:
                # Log the error but continue disconnecting others
                pass

        self._active_consumers.clear()
        self._active_producers.clear()

    def get_active_consumers(self) -> List[MessageBroker]:
        """Get list of active consumers"""
        return self._active_consumers.copy()

    def get_active_producers(self) -> List[MessageProducer]:
        """Get list of active producers"""
        return self._active_producers.copy()

    async def health_check_all(self) -> Dict[str, bool]:
        """Health check all active brokers"""
        results = {}

        for i, consumer in enumerate(self._active_consumers):
            try:
                results[f"consumer_{i}"] = await consumer.health_check()
            except Exception:
                results[f"consumer_{i}"] = False

        for i, producer in enumerate(self._active_producers):
            try:
                results[f"producer_{i}"] = await producer.health_check()
            except Exception:
                results[f"producer_{i}"] = False

        return results


class BrokerMigration:
    """Utilities for migrating data between brokers"""

    def __init__(self, factory: Optional[BrokerFactory] = None):
        self.factory = factory or BrokerFactory()

    async def migrate_messages(
        self,
        source_broker: MessageBroker,
        target_broker: MessageProducer,
        max_messages: Optional[int] = None,
        batch_size: int = 100,
        transform_fn: Optional[callable] = None,  # type: ignore
    ) -> int:
        """Migrate messages from source to target broker"""

        migrated_count = 0
        batch = []

        async for message in source_broker.consume():
            # Check if we've reached the limit before adding to batch
            if max_messages and migrated_count >= max_messages:
                break

            # Transform message if needed
            if transform_fn:
                message = await transform_fn(message)

            batch.append(message)

            # Process batch when full
            if len(batch) >= batch_size:
                # Don't send more than the limit
                if max_messages and migrated_count + len(batch) > max_messages:
                    # Trim batch to exact limit
                    remaining = max_messages - migrated_count
                    batch = batch[:remaining]

                await target_broker.send_batch(batch.copy())
                migrated_count += len(batch)
                batch.clear()

                # Break if we've reached the limit
                if max_messages and migrated_count >= max_messages:
                    break

        # Process remaining messages in batch
        if batch:
            # Don't send more than the limit
            if max_messages and migrated_count + len(batch) > max_messages:
                remaining = max_messages - migrated_count
                batch = batch[:remaining]

            if batch:  # Only send if there are messages left
                await target_broker.send_batch(batch.copy())
                migrated_count += len(batch)

        return migrated_count


# Global factory instance
default_factory = BrokerFactory()


def create_consumer(broker_type: str, config: Optional[Any] = None, **kwargs) -> MessageBroker:
    """Convenience function to create a consumer"""
    return default_factory.create_consumer(broker_type, config, **kwargs)


def create_producer(broker_type: str, config: Optional[Any] = None, **kwargs) -> MessageProducer:
    """Convenience function to create a producer"""
    return default_factory.create_producer(broker_type, config, **kwargs)


def create_config(broker_type: str) -> Any:
    """Convenience function to create a configuration"""
    return default_factory.create_config(broker_type)
