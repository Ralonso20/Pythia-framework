"""
RabbitMQ configuration classes
"""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class RabbitMQConfig(BaseSettings):
    """RabbitMQ broker configuration - simplified"""

    # Connection settings
    url: str = Field(default="amqp://guest:guest@localhost:5672/", description="Connection URL")

    # Consumer/Producer settings
    queue: str = Field(default="default", description="Queue name")
    exchange: Optional[str] = Field(default=None, description="Exchange name")
    routing_key: str = Field(default="", description="Routing key")

    # Common settings
    durable: bool = Field(default=True, description="Durable queues/exchanges")
    auto_ack: bool = Field(default=False, description="Auto acknowledge messages")
    prefetch_count: int = Field(default=10, description="Prefetch count")

    # Connection behavior
    heartbeat: int = Field(default=600, description="Heartbeat interval")
    connection_attempts: int = Field(default=3, description="Connection attempts")
    retry_delay: float = Field(default=2.0, description="Retry delay in seconds")

    class Config:
        env_prefix = "RABBITMQ_"
        case_sensitive = False
