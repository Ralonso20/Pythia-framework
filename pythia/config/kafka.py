"""
Kafka configuration classes
"""

from typing import Any, Dict, List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from .base import BaseConfig


class KafkaConfig(BaseSettings):
    """Kafka broker configuration"""

    # Connection settings
    bootstrap_servers: str = Field(default="localhost:9092", description="Kafka bootstrap servers")

    # Consumer settings
    group_id: str = Field(default="pythia-group", description="Consumer group ID")
    topics: List[str] = Field(default=["default"], description="Topics to consume from")
    auto_offset_reset: str = Field(default="earliest", description="Auto offset reset policy")
    enable_auto_commit: bool = Field(default=True, description="Enable auto commit")
    auto_commit_interval_ms: int = Field(
        default=5000, description="Auto commit interval in milliseconds"
    )

    # Producer settings
    acks: str = Field(default="all", description="Producer acknowledgments")
    retries: int = Field(default=3, description="Producer retries")
    batch_size: int = Field(default=16384, description="Producer batch size")
    linger_ms: int = Field(default=0, description="Producer linger time in milliseconds")

    # Security settings
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism")
    sasl_username: Optional[str] = Field(default=None, description="SASL username")
    sasl_password: Optional[str] = Field(default=None, description="SASL password")

    # SSL settings
    ssl_ca_location: Optional[str] = Field(default=None, description="SSL CA location")
    ssl_certificate_location: Optional[str] = Field(
        default=None, description="SSL certificate location"
    )
    ssl_key_location: Optional[str] = Field(default=None, description="SSL key location")
    ssl_key_password: Optional[str] = Field(default=None, description="SSL key password")

    # Performance settings
    session_timeout_ms: int = Field(default=30000, description="Consumer session timeout")
    heartbeat_interval_ms: int = Field(default=3000, description="Consumer heartbeat interval")
    max_poll_interval_ms: int = Field(default=300000, description="Maximum poll interval")
    max_poll_records: int = Field(default=500, description="Maximum poll records")
    fetch_min_bytes: int = Field(default=1, description="Minimum fetch bytes")
    fetch_max_wait_ms: int = Field(default=500, description="Maximum fetch wait time")

    class Config:
        env_prefix = "KAFKA_"
        case_sensitive = False

    def to_confluent_config(self) -> Dict[str, Any]:
        """Convert to confluent-kafka configuration"""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "auto.commit.interval.ms": self.auto_commit_interval_ms,
            "session.timeout.ms": self.session_timeout_ms,
            "heartbeat.interval.ms": self.heartbeat_interval_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "max.poll.records": self.max_poll_records,
            "fetch.min.bytes": self.fetch_min_bytes,
            "fetch.max.wait.ms": self.fetch_max_wait_ms,
        }

        # Add security settings if configured
        if self.security_protocol != "PLAINTEXT":
            config["security.protocol"] = self.security_protocol

        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism

        if self.sasl_username:
            config["sasl.username"] = self.sasl_username

        if self.sasl_password:
            config["sasl.password"] = self.sasl_password

        # Add SSL settings if configured
        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location

        if self.ssl_certificate_location:
            config["ssl.certificate.location"] = self.ssl_certificate_location

        if self.ssl_key_location:
            config["ssl.key.location"] = self.ssl_key_location

        if self.ssl_key_password:
            config["ssl.key.password"] = self.ssl_key_password

        return config

    def to_producer_config(self) -> Dict[str, Any]:
        """Convert to producer configuration"""
        base_config = self.to_confluent_config()

        # Remove consumer-specific settings
        consumer_keys = [
            "group.id",
            "auto.offset.reset",
            "enable.auto.commit",
            "auto.commit.interval.ms",
            "session.timeout.ms",
            "heartbeat.interval.ms",
            "max.poll.interval.ms",
            "max.poll.records",
            "fetch.min.bytes",
            "fetch.max.wait.ms",
        ]

        producer_config = {k: v for k, v in base_config.items() if k not in consumer_keys}

        # Add producer-specific settings
        producer_config.update(
            {
                "acks": self.acks,
                "retries": self.retries,
                "batch.size": self.batch_size,
                "linger.ms": self.linger_ms,
            }
        )

        return producer_config


class KafkaTopicConfig(BaseConfig):
    """Kafka topic configuration for admin operations"""

    name: str = Field(description="Topic name")
    num_partitions: int = Field(default=3, description="Number of partitions")
    replication_factor: int = Field(default=1, description="Replication factor")
    cleanup_policy: str = Field(default="delete", description="Cleanup policy")
    retention_ms: Optional[int] = Field(default=None, description="Retention time in milliseconds")
    segment_ms: Optional[int] = Field(default=None, description="Segment time in milliseconds")
    max_message_bytes: Optional[int] = Field(default=None, description="Maximum message size")

    def to_topic_config(self) -> Dict[str, str]:
        """Convert to topic configuration dictionary"""
        config = {
            "cleanup.policy": self.cleanup_policy,
        }

        if self.retention_ms is not None:
            config["retention.ms"] = str(self.retention_ms)

        if self.segment_ms is not None:
            config["segment.ms"] = str(self.segment_ms)

        if self.max_message_bytes is not None:
            config["max.message.bytes"] = str(self.max_message_bytes)

        return config
