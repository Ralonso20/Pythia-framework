"""Message broker adapters"""

# Import all broker implementations for easy access
from .kafka import KafkaConsumer, KafkaProducer
from .rabbitmq import RabbitMQConsumer, RabbitMQProducer
from .redis import (
    RedisStreamsConsumer,
    RedisStreamsProducer,
    RedisPubSubConsumer,
    RedisPubSubProducer,
    RedisListConsumer,
    RedisListProducer,
)


# HTTP workers imported lazily to avoid circular imports
def _get_http_workers():
    """Lazy import of HTTP workers to avoid circular imports"""
    try:
        from .http import HTTPWorker, PollerWorker, WebhookSenderWorker

        return HTTPWorker, PollerWorker, WebhookSenderWorker
    except ImportError:
        return None, None, None


HTTPWorker, PollerWorker, WebhookSenderWorker = _get_http_workers()

__all__ = [
    # Kafka
    "KafkaConsumer",
    "KafkaProducer",
    # RabbitMQ
    "RabbitMQConsumer",
    "RabbitMQProducer",
    # Redis
    "RedisStreamsConsumer",
    "RedisStreamsProducer",
    "RedisPubSubConsumer",
    "RedisPubSubProducer",
    "RedisListConsumer",
    "RedisListProducer",
]

# Add HTTP workers to __all__ if successfully imported
if HTTPWorker is not None:
    __all__.extend(["HTTPWorker", "PollerWorker", "WebhookSenderWorker"])
