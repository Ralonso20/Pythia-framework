"""
Multi-broker example demonstrating Pythia's broker switching capabilities
"""

import asyncio
import json
from typing import Any

from pythia import Worker
from pythia.core.message import Message
from pythia.config.broker_factory import BrokerFactory, BrokerSwitcher
from pythia.config.kafka import KafkaConfig
from pythia.config.rabbitmq import RabbitMQConfig
from pythia.config.redis import RedisConfig


class MultiBrokerWorker(Worker):
    """
    Worker that can switch between different message brokers dynamically
    """

    def __init__(self):
        super().__init__()
        self.factory = BrokerFactory()
        self.switcher = BrokerSwitcher(self.factory)
        self.message_count = 0

    async def process(self, message: Message) -> Any:
        """Process message from any broker type"""
        self.message_count += 1

        self.logger.info(
            "Processing message",
            message_id=message.message_id,
            body=message.body,
            source=getattr(
                message,
                "queue",
                getattr(message, "topic", getattr(message, "stream", "unknown")),
            ),
            count=self.message_count,
        )

        # Process the message
        if isinstance(message.body, dict):
            result = f"Processed: {message.body.get('id', 'unknown')}"
        else:
            result = f"Processed: {str(message.body)[:50]}"

        self.logger.info("Message processed successfully", result=result)
        return result

    async def switch_to_kafka(self, topics: list = None, group_id: str = "multi-broker-group"):
        """Switch to Kafka broker"""
        self.logger.info("Switching to Kafka")

        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            topics=topics or ["multi-broker-test"],
            group_id=group_id,
        )

        # Create new consumer
        consumer = self.factory.create_consumer("kafka", config=config)

        # Replace current source
        if self._sources:
            old_consumer = self._sources[0]
            await self.switcher.switch_consumer(old_consumer, "kafka", config=config)
        else:
            await consumer.connect()
            self._sources = [consumer]

        self.logger.info("Switched to Kafka successfully")

    async def switch_to_rabbitmq(
        self, queue: str = "multi-broker-test", exchange: str = "test-exchange"
    ):
        """Switch to RabbitMQ broker"""
        self.logger.info("Switching to RabbitMQ")

        config = RabbitMQConfig(url="amqp://localhost:5672", queue=queue, exchange=exchange)

        # Create new consumer
        consumer = self.factory.create_consumer("rabbitmq", config=config)

        # Replace current source
        if self._sources:
            old_consumer = self._sources[0]
            await self.switcher.switch_consumer(old_consumer, "rabbitmq", config=config)
        else:
            await consumer.connect()
            self._sources = [consumer]

        self.logger.info("Switched to RabbitMQ successfully")

    async def switch_to_redis_streams(
        self, stream: str = "multi-broker-test", group: str = "test-group"
    ):
        """Switch to Redis Streams broker"""
        self.logger.info("Switching to Redis Streams")

        config = RedisConfig(host="localhost", port=6379, stream_name=stream, consumer_group=group)

        # Create new consumer
        consumer = self.factory.create_consumer("redis_streams", config=config)

        # Replace current source
        if self._sources:
            old_consumer = self._sources[0]
            await self.switcher.switch_consumer(old_consumer, "redis_streams", config=config)
        else:
            await consumer.connect()
            self._sources = [consumer]

        self.logger.info("Switched to Redis Streams successfully")

    async def add_secondary_consumer(self, broker_type: str, **kwargs):
        """Add a secondary consumer without removing the primary one"""
        self.logger.info(f"Adding secondary {broker_type} consumer")

        secondary_consumer = await self.switcher.add_consumer(broker_type, **kwargs)
        self._sources.append(secondary_consumer)

        self.logger.info(
            f"Added secondary {broker_type} consumer", total_sources=len(self._sources)
        )

    async def get_broker_status(self):
        """Get status of all active brokers"""
        health_status = await self.switcher.health_check_all()

        status = {
            "active_consumers": len(self.switcher.get_active_consumers()),
            "active_producers": len(self.switcher.get_active_producers()),
            "health_checks": health_status,
            "messages_processed": self.message_count,
            "supported_brokers": self.factory.list_supported_brokers(),
        }

        self.logger.info("Broker status", **status)
        return status

    async def shutdown(self):
        """Graceful shutdown - disconnect all brokers"""
        self.logger.info("Shutting down multi-broker worker")
        await self.switcher.disconnect_all()
        self.logger.info("All brokers disconnected")


class MultiBrokerProducer:
    """
    Producer that can send messages to multiple broker types
    """

    def __init__(self):
        self.factory = BrokerFactory()
        self.producers = {}
        self.logger = None

    async def setup_producers(self):
        """Setup producers for different broker types"""
        from pythia.logging import get_pythia_logger

        self.logger = get_pythia_logger("MultiBrokerProducer")

        try:
            # Kafka Producer
            kafka_config = KafkaConfig(
                bootstrap_servers="localhost:9092", topic="multi-broker-test"
            )
            self.producers["kafka"] = self.factory.create_producer("kafka", config=kafka_config)
            await self.producers["kafka"].connect()
            self.logger.info("Kafka producer ready")

        except Exception as e:
            self.logger.warning(f"Failed to setup Kafka producer: {e}")

        try:
            # RabbitMQ Producer
            rabbitmq_config = RabbitMQConfig(url="amqp://localhost:5672", exchange="test-exchange")
            self.producers["rabbitmq"] = self.factory.create_producer(
                "rabbitmq", config=rabbitmq_config
            )
            await self.producers["rabbitmq"].connect()
            self.logger.info("RabbitMQ producer ready")

        except Exception as e:
            self.logger.warning(f"Failed to setup RabbitMQ producer: {e}")

        try:
            # Redis Streams Producer
            redis_config = RedisConfig(host="localhost", port=6379, stream_name="multi-broker-test")
            self.producers["redis"] = self.factory.create_producer(
                "redis_streams", config=redis_config
            )
            await self.producers["redis"].connect()
            self.logger.info("Redis Streams producer ready")

        except Exception as e:
            self.logger.warning(f"Failed to setup Redis Streams producer: {e}")

    async def send_to_all(self, message_data: dict):
        """Send message to all available brokers"""
        results = {}

        for broker_type, producer in self.producers.items():
            try:
                success = await producer.send(message_data)
                results[broker_type] = {"success": success, "error": None}
                self.logger.info(f"Message sent to {broker_type}")
            except Exception as e:
                results[broker_type] = {"success": False, "error": str(e)}
                self.logger.error(f"Failed to send to {broker_type}: {e}")

        return results

    async def send_test_messages(self, count: int = 10):
        """Send test messages to demonstrate multi-broker functionality"""
        self.logger.info(f"Sending {count} test messages to all brokers")

        for i in range(count):
            message = {
                "id": f"test-{i}",
                "timestamp": asyncio.get_event_loop().time(),
                "data": f"Test message {i}",
                "source": "multi-broker-example",
            }

            results = await self.send_to_all(message)
            self.logger.info(f"Message {i} results", **results)

            # Small delay between messages
            await asyncio.sleep(0.1)

    async def cleanup(self):
        """Cleanup all producers"""
        for broker_type, producer in self.producers.items():
            try:
                await producer.disconnect()
                self.logger.info(f"Disconnected {broker_type} producer")
            except Exception as e:
                self.logger.error(f"Error disconnecting {broker_type} producer: {e}")


async def demonstrate_broker_switching():
    """Demonstrate dynamic broker switching"""
    print("🔄 Multi-Broker Switching Demonstration")
    print("=" * 50)

    worker = MultiBrokerWorker()
    producer = MultiBrokerProducer()

    try:
        # Setup producers
        await producer.setup_producers()

        print("\n📊 Broker Status:")
        status = await worker.get_broker_status()
        print(json.dumps(status, indent=2))

        print("\n🎯 Starting with Kafka...")
        await worker.switch_to_kafka()

        # Send some test messages
        await producer.send_test_messages(3)

        print("\n🐰 Switching to RabbitMQ...")
        await worker.switch_to_rabbitmq()
        await producer.send_test_messages(3)

        print("\n📦 Switching to Redis Streams...")
        await worker.switch_to_redis_streams()
        await producer.send_test_messages(3)

        print("\n🔄 Adding secondary Kafka consumer...")
        kafka_config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            topics=["secondary-topic"],
            group_id="secondary-group",
        )
        await worker.add_secondary_consumer("kafka", config=kafka_config)

        print("\n📊 Final Status:")
        final_status = await worker.get_broker_status()
        print(json.dumps(final_status, indent=2))

        print("\n✅ Demonstration completed successfully!")

    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        await worker.shutdown()
        await producer.cleanup()


async def demonstrate_auto_configuration():
    """Demonstrate auto-configuration capabilities"""
    print("\n🤖 Auto-Configuration Demonstration")
    print("=" * 50)

    from pythia.config.auto_config import (
        detect_all_brokers,
        validate_environment,
        auto_create_brokers,
    )

    # Detect available brokers
    available = detect_all_brokers()
    print(f"\n📋 Available brokers from environment: {available}")

    # Validate environment
    validation = validate_environment()
    print("\n✅ Environment validation:")
    print(json.dumps(validation, indent=2))

    if available:
        print(f"\n🏗️  Creating brokers for: {available}")
        brokers = auto_create_brokers(available)

        for broker_type, components in brokers.items():
            print(f"\n{broker_type.upper()}:")
            print(f"  Consumer: {type(components['consumer']).__name__}")
            print(f"  Producer: {type(components['producer']).__name__}")
            print(f"  Config: {type(components['config']).__name__}")


async def main():
    """Main demonstration function"""
    print("🐍 Pythia Multi-Broker Example")
    print("=" * 50)
    print("This example demonstrates:")
    print("  ✅ Dynamic broker switching")
    print("  ✅ Multiple simultaneous brokers")
    print("  ✅ Auto-configuration from environment")
    print("  ✅ Health monitoring")
    print("  ✅ Broker factory patterns")
    print()

    # Check if any brokers are available
    from pythia.config.auto_config import detect_all_brokers

    available = detect_all_brokers()

    if not available:
        print("⚠️  No message brokers detected in environment!")
        print("\nTo run this example, set up at least one broker:")
        print("  🔹 Kafka: KAFKA_BOOTSTRAP_SERVERS=localhost:9092")
        print("  🔹 RabbitMQ: RABBITMQ_URL=amqp://localhost:5672")
        print("  🔹 Redis: REDIS_URL=redis://localhost:6379")
        print("\nOr run with Docker:")
        print("  docker-compose up -d")
        return

    try:
        await demonstrate_auto_configuration()
        await demonstrate_broker_switching()

    except KeyboardInterrupt:
        print("\n\n⏹️  Stopped by user")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
