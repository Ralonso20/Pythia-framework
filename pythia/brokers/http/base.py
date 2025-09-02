"""
Base class for HTTP workers using HTTPClient utilities
"""

from abc import abstractmethod
from typing import Any, Optional

from ...http import PythiaHTTPClient, HTTPClientConfig

# Import Worker locally to avoid circular imports
from ...core.worker import Worker


class HTTPWorker(Worker):
    """Base class for HTTP workers using Pythia HTTP client"""

    def __init__(self, http_config: Optional[HTTPClientConfig] = None, **kwargs):
        super().__init__(**kwargs)
        self.http_config = http_config or HTTPClientConfig()
        self.http_client = None

    async def connect(self) -> None:
        """Initialize HTTP client"""
        self.http_client = PythiaHTTPClient(config=self.http_config)
        await self.http_client.connect()
        self.logger.info("HTTP client initialized")

    async def disconnect(self) -> None:
        """Close HTTP client"""
        if self.http_client:
            await self.http_client.disconnect()
        self.logger.info("HTTP client disconnected")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        return None

    @abstractmethod
    async def process(self) -> Any:
        """Process HTTP requests - to be implemented by subclasses"""
        pass
