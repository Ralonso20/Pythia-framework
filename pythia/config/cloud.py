"""
Configuration models for cloud message brokers
"""

import os
from dataclasses import dataclass
from typing import Optional
from pydantic import BaseModel, Field


class AWSConfig(BaseModel):
    """AWS SQS/SNS configuration"""

    region: str = Field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    access_key_id: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    secret_access_key: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    endpoint_url: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_ENDPOINT_URL")
    )  # For LocalStack

    # SQS specific
    queue_url: Optional[str] = Field(default_factory=lambda: os.getenv("SQS_QUEUE_URL"))
    visibility_timeout: int = Field(default=30)
    wait_time_seconds: int = Field(default=20)  # Long polling
    max_messages: int = Field(default=10)

    # SNS specific
    topic_arn: Optional[str] = Field(default_factory=lambda: os.getenv("SNS_TOPIC_ARN"))


class GCPConfig(BaseModel):
    """Google Cloud Pub/Sub configuration"""

    project_id: str = Field(default_factory=lambda: os.getenv("GCP_PROJECT_ID", ""))
    credentials_path: Optional[str] = Field(
        default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )

    # Pub/Sub specific
    subscription_name: Optional[str] = Field(
        default_factory=lambda: os.getenv("PUBSUB_SUBSCRIPTION")
    )
    topic_name: Optional[str] = Field(default_factory=lambda: os.getenv("PUBSUB_TOPIC"))
    max_messages: int = Field(default=100)
    ack_deadline_seconds: int = Field(default=600)


class AzureConfig(BaseModel):
    """Azure Service Bus and Storage Queue configuration"""

    # Service Bus
    service_bus_connection_string: Optional[str] = Field(
        default_factory=lambda: os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING")
    )
    service_bus_queue_name: Optional[str] = Field(
        default_factory=lambda: os.getenv("SERVICE_BUS_QUEUE_NAME")
    )

    # Storage Queue
    storage_connection_string: Optional[str] = Field(
        default_factory=lambda: os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    storage_queue_name: Optional[str] = Field(
        default_factory=lambda: os.getenv("STORAGE_QUEUE_NAME")
    )

    # Common settings
    max_messages: int = Field(default=32)
    visibility_timeout: int = Field(default=30)


@dataclass
class CloudProviderConfig:
    """Unified cloud configuration wrapper"""

    aws: Optional[AWSConfig] = None
    gcp: Optional[GCPConfig] = None
    azure: Optional[AzureConfig] = None

    @classmethod
    def from_env(cls) -> "CloudProviderConfig":
        """Create configuration from environment variables"""
        return cls(aws=AWSConfig(), gcp=GCPConfig(), azure=AzureConfig())
