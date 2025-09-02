"""
Base configuration classes using Pydantic
"""

import os
from typing import Any, Dict, Optional, List
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class BaseConfig(BaseModel):
    """Base configuration class"""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.model_dump()


class WorkerConfig(BaseSettings):
    """Main worker configuration with auto-detection"""

    # Worker identification
    worker_name: str = Field(default="pythia-worker", description="Worker name")
    worker_id: str = Field(default="", description="Unique worker ID")

    # Processing settings
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: float = Field(default=1.0, description="Delay between retries in seconds")
    batch_size: int = Field(default=1, description="Batch size for processing")
    max_concurrent: int = Field(default=10, description="Maximum concurrent workers")

    # Broker configuration
    broker_type: str = Field(default="kafka", description="Message broker type")
    multi_broker: bool = Field(default=False, description="Enable multi-broker support")
    available_brokers: List[str] = Field(default=[], description="Available broker types")

    # Logging configuration
    log_level: str = Field(default="INFO", description="Log level")
    log_format: str = Field(default="json", description="Log format (json|text)")
    log_file: Optional[str] = Field(default=None, description="Log file path")

    # Health check configuration
    health_check_interval: int = Field(default=30, description="Health check interval in seconds")
    health_check_timeout: int = Field(default=10, description="Health check timeout in seconds")

    class Config:
        env_prefix = "PYTHIA_"
        case_sensitive = False

    def __post_init__(self):
        """Post-initialization to set worker_id if not provided"""
        if not self.worker_id:
            self.worker_id = f"{self.worker_name}-{os.getpid()}"


class LogConfig(BaseModel):
    """Logging configuration"""

    level: str = Field(default="INFO", description="Log level")
    format: str = Field(default="json", description="Log format")
    file: Optional[str] = Field(default=None, description="Log file path")
    rotation: Optional[str] = Field(default="1 GB", description="Log rotation size")
    retention: Optional[str] = Field(default="30 days", description="Log retention period")

    # Structured logging fields
    add_timestamp: bool = Field(default=True, description="Add timestamp to logs")
    add_worker_id: bool = Field(default=True, description="Add worker ID to logs")
    add_correlation_id: bool = Field(default=True, description="Add correlation ID to logs")


class MetricsConfig(BaseModel):
    """Metrics configuration"""

    enabled: bool = Field(default=True, description="Enable metrics collection")
    port: int = Field(default=8080, description="Metrics server port")
    path: str = Field(default="/metrics", description="Metrics endpoint path")

    # Prometheus configuration
    prometheus_enabled: bool = Field(default=True, description="Enable Prometheus metrics")
    prometheus_prefix: str = Field(default="pythia", description="Prometheus metrics prefix")

    # Custom metrics
    custom_metrics: Dict[str, Any] = Field(
        default_factory=dict, description="Custom metrics configuration"
    )


class SecurityConfig(BaseModel):
    """Security configuration"""

    # SSL/TLS settings
    ssl_enabled: bool = Field(default=False, description="Enable SSL/TLS")
    ssl_cert_file: Optional[str] = Field(default=None, description="SSL certificate file")
    ssl_key_file: Optional[str] = Field(default=None, description="SSL private key file")
    ssl_ca_file: Optional[str] = Field(default=None, description="SSL CA file")

    # Authentication
    auth_enabled: bool = Field(default=False, description="Enable authentication")
    auth_method: str = Field(default="none", description="Authentication method")

    # Encryption
    encryption_enabled: bool = Field(default=False, description="Enable field encryption")
    encryption_key: Optional[str] = Field(default=None, description="Encryption key")


class ResilienceConfig(BaseModel):
    """Resilience and retry configuration"""

    # Retry settings
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: float = Field(default=1.0, description="Initial retry delay")
    retry_backoff: float = Field(default=2.0, description="Exponential backoff multiplier")
    retry_max_delay: float = Field(default=60.0, description="Maximum retry delay")

    # Circuit breaker settings
    circuit_breaker_enabled: bool = Field(default=True, description="Enable circuit breaker")
    circuit_breaker_threshold: int = Field(
        default=5, description="Circuit breaker failure threshold"
    )
    circuit_breaker_timeout: int = Field(
        default=60, description="Circuit breaker timeout in seconds"
    )

    # Timeout settings
    processing_timeout: int = Field(default=300, description="Processing timeout in seconds")
    connection_timeout: int = Field(default=30, description="Connection timeout in seconds")
