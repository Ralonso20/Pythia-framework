"""
Cloud message queue brokers for Pythia

Provides workers for cloud-based message queues:
- AWS SQS/SNS
- Google Cloud Pub/Sub
- Azure Service Bus & Storage Queues

Each provider has optional dependencies that must be installed separately.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import CloudWorker
    from .aws import SQSConsumer, SNSProducer
    from .gcp import PubSubSubscriber, PubSubPublisher
    from .azure import ServiceBusConsumer, StorageQueueConsumer


def _get_base_workers():
    """Lazy import base cloud workers"""
    try:
        from .base import CloudWorker

        return CloudWorker
    except ImportError:
        return None


def _get_aws_workers():
    """Lazy import AWS workers"""
    try:
        from .aws import SQSConsumer, SNSProducer

        return SQSConsumer, SNSProducer
    except ImportError:
        return None, None


def _get_gcp_workers():
    """Lazy import GCP workers"""
    try:
        from .gcp import PubSubSubscriber, PubSubPublisher

        return PubSubSubscriber, PubSubPublisher
    except ImportError:
        return None, None


def _get_azure_workers():
    """Lazy import Azure workers"""
    try:
        from .azure import ServiceBusConsumer, StorageQueueConsumer

        return ServiceBusConsumer, StorageQueueConsumer
    except ImportError:
        return None, None


# Lazy loading exports
CloudWorker = _get_base_workers()
SQSConsumer, SNSProducer = _get_aws_workers()
PubSubSubscriber, PubSubPublisher = _get_gcp_workers()
ServiceBusConsumer, StorageQueueConsumer = _get_azure_workers()

__all__ = [
    "CloudWorker",
    "SQSConsumer",
    "SNSProducer",
    "PubSubSubscriber",
    "PubSubPublisher",
    "ServiceBusConsumer",
    "StorageQueueConsumer",
]
