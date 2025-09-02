"""
Base class for cloud workers
"""

from abc import abstractmethod
from typing import Any, Optional
from dataclasses import dataclass

from ...core.worker import Worker


@dataclass
class CloudConfig:
    """Base configuration for cloud workers"""

    region: Optional[str] = None
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    batch_size: int = 10
    max_concurrent: int = 10


class CloudWorker(Worker):
    """Base class for cloud message queue workers"""

    def __init__(self, cloud_config: Optional[CloudConfig] = None, **kwargs):
        super().__init__(**kwargs)
        self.cloud_config = cloud_config or CloudConfig()
        self.client = None

    async def connect(self) -> None:
        """Initialize cloud client - implement in subclasses"""
        self.logger.info("Cloud worker connecting...")

    async def disconnect(self) -> None:
        """Close cloud client - implement in subclasses"""
        if self.client:
            # Subclasses should implement specific cleanup
            pass
        self.logger.info("Cloud worker disconnected")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        return None

    @abstractmethod
    async def process(self) -> Any:
        """Process cloud messages - to be implemented by subclasses"""
        pass
