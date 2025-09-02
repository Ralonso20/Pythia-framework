"""
HTTP Poller Worker - polls HTTP APIs at regular intervals
"""

from typing import Any, Dict, Optional, Union, List, Callable
from datetime import datetime

from .base import HTTPWorker
from ...http import HTTPPoller, HTTPClientConfig
from ...config.http import PollerConfig

# Import Message locally to avoid circular imports
from ...core.message import Message


class PollerWorker(HTTPWorker):
    """Worker that polls HTTP APIs at regular intervals"""

    def __init__(
        self,
        url: str,
        interval: Union[int, float] = 60,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        data_extractor: Optional[Callable[[Dict], List[Dict]]] = None,
        poller_config: Optional[PollerConfig] = None,
        http_config: Optional[HTTPClientConfig] = None,
        **kwargs,
    ):
        """
        Initialize HTTP poller worker

        Args:
            url: URL to poll
            interval: Polling interval in seconds
            method: HTTP method (GET, POST, etc.)
            headers: Custom headers for requests
            params: Query parameters
            data_extractor: Function to extract items from response
            poller_config: Poller-specific configuration
            http_config: HTTP client configuration
        """
        super().__init__(http_config=http_config, **kwargs)

        self.url = url
        self.interval = interval
        self.method = method
        self.headers = headers or {}
        self.params = params or {}
        self.data_extractor = data_extractor
        self.poller_config = poller_config

        # Initialize poller
        self.poller = HTTPPoller(
            url=url,
            interval=interval,
            method=method,
            headers=headers,
            params=params,
            data_extractor=data_extractor,
            config=poller_config,
        )

    async def connect(self) -> None:
        """Connect poller and HTTP client"""
        await super().connect()
        await self.poller.connect()
        self.logger.info(f"Connected to HTTP poller for {self.url}")

    async def disconnect(self) -> None:
        """Disconnect poller and HTTP client"""
        await self.poller.disconnect()
        await super().disconnect()
        self.logger.info("HTTP poller disconnected")

    async def process(self) -> Any:
        """Main processing loop - consume from HTTP poller"""
        self.logger.info(f"Starting HTTP polling of {self.url} every {self.interval}s")

        try:
            async for message in self.poller.consume():
                try:
                    result = await self.process_message(message)
                    self.logger.debug(
                        "Processed HTTP poll message",
                        url=self.url,
                        method=self.method,
                        result=result,
                    )
                except Exception as e:
                    self.logger.error(
                        "Error processing HTTP poll message",
                        url=self.url,
                        error=str(e),
                        exc_info=True,
                    )
                    # Continue polling despite errors
                    continue

        except Exception as e:
            self.logger.error(f"Error in HTTP poller: {e}", exc_info=True)
            raise

    async def process_message(self, message: Message) -> Any:
        """
        Process a message from HTTP polling

        Override this method to implement your polling logic

        Args:
            message: Message containing HTTP response data

        Returns:
            Processing result
        """
        self.logger.info(
            "Processing HTTP poll message",
            url=self.url,
            status_code=message.headers.get("status_code"),
            timestamp=message.timestamp,
        )

        # Default implementation - just log the data
        return {"processed": True, "data": message.body}

    async def get_last_poll_time(self) -> Optional[datetime]:
        """Get timestamp of last successful poll"""
        # This could be extended to store state in Redis/database
        return getattr(self.poller, "_last_poll_time", None)

    async def force_poll(self) -> Optional[Message]:
        """Force an immediate poll (outside of regular interval)"""
        self.logger.info(f"Forcing immediate poll of {self.url}")
        try:
            # Manually poll once
            response = await self.poller._poll_once()
            if response:
                message = Message(body=response, headers={"forced_poll": True})
                await self.process_message(message)
                return message
        except Exception as e:
            self.logger.error(f"Error during forced poll: {e}", exc_info=True)

        return None
