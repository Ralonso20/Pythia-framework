"""
Redis configuration classes
"""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class RedisConfig(BaseSettings):
    """Redis broker configuration - simplified"""

    # Connection settings
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database number")
    password: Optional[str] = Field(default=None, description="Redis password")

    # Stream/Channel/Queue settings
    stream: Optional[str] = Field(default=None, description="Stream name")
    channel: Optional[str] = Field(default=None, description="Pub/Sub channel")
    queue: Optional[str] = Field(default=None, description="List queue name")

    # Consumer settings
    consumer_group: str = Field(default="pythia-group", description="Consumer group")
    batch_size: int = Field(default=10, description="Batch size")
    block_timeout_ms: int = Field(default=1000, description="Block timeout in ms")
    max_stream_length: Optional[int] = Field(default=None, description="Max stream length")

    # Connection behavior
    socket_timeout: int = Field(default=30, description="Socket timeout")
    socket_connect_timeout: int = Field(default=30, description="Connect timeout")
    socket_keepalive: bool = Field(default=True, description="Keep alive")
    socket_keepalive_options: dict = Field(default_factory=dict, description="Keep alive options")
    health_check_interval: int = Field(default=30, description="Health check interval")
    max_connections: int = Field(default=50, description="Max connections")

    class Config:
        env_prefix = "REDIS_"
        case_sensitive = False
