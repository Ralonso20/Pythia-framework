"""
HTTP Poller implementation for Pythia framework
"""

import asyncio
import json
from typing import AsyncIterator, Dict, Any, Optional, Union, List, Callable
from urllib.parse import urlparse
from datetime import datetime, timedelta

import httpx

from pythia.core.message import Message
from pythia.brokers.base import BaseConsumer
from pythia.config.http import PollerConfig
from pythia.logging import get_pythia_logger


class HTTPPoller(BaseConsumer):
    """
    HTTP Poller that periodically fetches data from HTTP endpoints

    Acts as a message broker consumer by polling HTTP APIs at regular intervals
    and yielding the responses as Pythia Messages.

    Example:
        poller = HTTPPoller(
            url="https://api.example.com/events",
            interval=30,  # Poll every 30 seconds
            method="GET"
        )

        async for message in poller.consume():
            print(f"Received data: {message.body}")
    """

    def __init__(
        self,
        url: str,
        interval: Union[int, float] = 60,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        data_extractor: Optional[Callable[[Dict], List[Dict]]] = None,
        config: Optional[PollerConfig] = None,
        **kwargs,
    ):
        """
        Initialize HTTP poller

        Args:
            url: URL to poll
            interval: Polling interval in seconds
            method: HTTP method (GET, POST, etc.)
            headers: HTTP headers
            params: URL parameters
            data_extractor: Function to extract individual records from response
            config: Poller configuration
            **kwargs: Additional configuration
        """
        super().__init__()

        self.url = url
        self.interval = interval
        self.method = method.upper()
        self.headers = headers or {}
        self.params = params or {}
        self.data_extractor = data_extractor

        # Merge config with kwargs
        if config:
            self.config = config
        else:
            config_dict = {
                "url": url,
                "interval": interval,
                "method": method,
                "headers": headers,
                "params": params,
                **kwargs,
            }
            self.config = PollerConfig(**config_dict)

        self.logger = get_pythia_logger(f"HTTPPoller[{urlparse(url).netloc}]")

        # State management
        self.client: Optional[httpx.AsyncClient] = None
        self._polling = False
        self._last_poll_time: Optional[datetime] = None
        self._last_etag: Optional[str] = None
        self._last_modified: Optional[str] = None

    async def connect(self) -> None:
        """Initialize HTTP client"""
        if self.client is not None:
            return

        try:
            self.logger.info(
                "Initializing HTTP poller",
                url=self.url,
                interval=self.interval,
                method=self.method,
            )

            # Create timeout configuration
            timeout = httpx.Timeout(
                connect=self.config.connect_timeout,
                read=self.config.read_timeout,
                write=self.config.write_timeout,
                pool=self.config.pool_timeout,
            )

            # Create limits configuration
            limits = httpx.Limits(
                max_keepalive_connections=self.config.max_keepalive_connections,
                max_connections=self.config.max_connections,
                keepalive_expiry=self.config.keepalive_expiry,
            )

            # Create default headers
            default_headers = {
                "User-Agent": f"Pythia-HTTPPoller/{self.config.user_agent}",
            }
            default_headers.update(self.headers)

            self.client = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                headers=default_headers,
                verify=self.config.verify_ssl,
                follow_redirects=self.config.follow_redirects,
                max_redirects=self.config.max_redirects,
            )

            self.logger.info("HTTP poller initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize HTTP poller", error=e, url=self.url)
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close HTTP client"""
        self._polling = False

        try:
            if self.client:
                await self.client.aclose()
                self.logger.info("HTTP poller disconnected")
        except Exception as e:
            self.logger.warning("Error during disconnect", error=str(e))
        finally:
            self.client = None

    def is_connected(self) -> bool:
        """Check if HTTP client is available"""
        return self.client is not None and not self.client.is_closed

    async def consume(self) -> AsyncIterator[Message]:  # type: ignore[override]
        """
        Start polling and yield messages

        Yields:
            Message: Pythia Message objects containing HTTP response data
        """
        if not self.is_connected():
            await self.connect()

        if not self.client:
            raise RuntimeError("HTTP client not initialized")

        self._polling = True
        self.logger.info(
            "Starting HTTP polling",
            url=self.url,
            interval=self.interval,
            method=self.method,
        )

        try:
            while self._polling:
                try:
                    # Calculate next poll time
                    now = datetime.now()
                    if self._last_poll_time is None:
                        # First poll - execute immediately
                        sleep_time = 0
                    else:
                        next_poll_time = self._last_poll_time + timedelta(seconds=self.interval)
                        sleep_time = max(0, (next_poll_time - now).total_seconds())

                    # Wait for next poll interval
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                        if not self._polling:
                            break

                    # Perform HTTP request
                    messages = await self._poll()
                    self._last_poll_time = datetime.now()

                    # Yield messages
                    for message in messages:
                        if not self._polling:
                            break
                        yield message

                except httpx.HTTPError as e:
                    self.logger.error("HTTP polling error", error=e, url=self.url)
                    # Wait before retrying
                    await asyncio.sleep(min(self.interval, 30))

                except Exception as e:
                    self.logger.error("Unexpected error in polling loop", error=e, url=self.url)
                    await asyncio.sleep(min(self.interval, 30))

        except Exception as e:
            self.logger.error("Fatal error in polling loop", error=e)
            raise
        finally:
            self._polling = False
            self.logger.info("Stopped HTTP polling")

    async def _poll(self) -> List[Message]:
        """Perform a single HTTP poll"""
        if not self.client:
            raise RuntimeError("HTTP client not initialized")

        # Prepare request headers with conditional request headers
        request_headers = {}
        if self.config.use_conditional_requests:
            if self._last_etag:
                request_headers["If-None-Match"] = self._last_etag
            if self._last_modified:
                request_headers["If-Modified-Since"] = self._last_modified

        self.logger.debug(
            "Polling HTTP endpoint",
            url=self.url,
            method=self.method,
            conditional_headers=bool(request_headers),
        )

        try:
            # Make HTTP request
            response = await self.client.request(
                method=self.method,
                url=self.url,
                headers=request_headers,
                params=self.params,
                json=self.config.request_body if self.method in ["POST", "PUT", "PATCH"] else None,
            )

            # Handle 304 Not Modified
            if response.status_code == 304:
                self.logger.debug("No changes since last poll (304 Not Modified)")
                return []

            # Check for successful response
            response.raise_for_status()

            # Update conditional request headers for next poll
            if self.config.use_conditional_requests:
                self._last_etag = response.headers.get("ETag")
                self._last_modified = response.headers.get("Last-Modified")

            # Parse response
            messages = await self._parse_response(response)

            self.logger.debug(
                "Poll completed successfully",
                url=self.url,
                status_code=response.status_code,
                messages_count=len(messages),
                response_size=len(response.content),
            )

            return messages

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 304:
                # Not modified - return empty list
                return []
            else:
                self.logger.error(
                    "HTTP error during poll",
                    status_code=e.response.status_code,
                    response_text=e.response.text[:200],
                    url=self.url,
                )
                raise

        except Exception as e:
            self.logger.error("Error during poll", error=e, url=self.url)
            raise

    async def _parse_response(self, response: httpx.Response) -> List[Message]:
        """Parse HTTP response into Pythia messages"""
        messages = []

        try:
            # Get content type
            content_type = response.headers.get("content-type", "").lower()

            # Parse response data
            if "application/json" in content_type:
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    self.logger.warning(
                        "Failed to parse JSON response",
                        error=str(e),
                        content_preview=response.text[:200],
                    )
                    # Fall back to text
                    data = response.text
            else:
                # Non-JSON response
                data = response.text

            # Extract individual records if data_extractor is provided
            if self.data_extractor:
                try:
                    if isinstance(data, dict):
                        records = self.data_extractor(data)
                    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                        # Handle array of objects - extract from each
                        records = []
                        for item in data:
                            extracted = self.data_extractor(item)
                            if isinstance(extracted, list):
                                records.extend(extracted)
                            else:
                                records.append(extracted)
                    else:
                        # Data is not suitable for extraction, use as is
                        records = [data]

                    if not isinstance(records, list):
                        records = [records]
                except Exception as e:
                    self.logger.warning("Data extractor failed, using raw response", error=e)
                    records = [data]
            else:
                # Use raw response as single record
                records = [data]

            # Create messages
            for i, record in enumerate(records):
                message = Message(
                    body=record,
                    message_id=f"{self.url}:{self._last_poll_time}:{i}"
                    if self._last_poll_time
                    else f"{self.url}:unknown:{i}",
                    headers={
                        "http_url": self.url,
                        "http_method": self.method,
                        "http_status_code": str(response.status_code),
                        "content_type": response.headers.get("content-type", ""),
                        "poll_time": self._last_poll_time.isoformat()
                        if self._last_poll_time
                        else "",
                    },
                    timestamp=datetime.now(),
                    _raw_data=response,
                )
                messages.append(message)

        except Exception as e:
            self.logger.error(
                "Failed to parse response",
                error=e,
                content_preview=response.text[:200],
            )
            # Create a single message with the raw response
            message = Message(
                body=response.text,
                message_id=f"{self.url}:{self._last_poll_time}"
                if self._last_poll_time
                else f"{self.url}:unknown",
                headers={
                    "http_url": self.url,
                    "http_method": self.method,
                    "http_status_code": str(response.status_code),
                    "error": "Failed to parse response",
                },
                timestamp=datetime.now(),
                _raw_data=response,
            )
            messages = [message]

        return messages

    async def health_check(self) -> bool:
        """Check if the HTTP endpoint is accessible"""
        try:
            if not self.is_connected():
                return False

            if not self.client:
                return False

            # Make a HEAD request to check endpoint availability
            response = await self.client.head(self.url, timeout=5.0)
            return response.is_success

        except Exception as e:
            self.logger.warning("Health check failed", error=str(e))
            return False

    def set_data_extractor(self, extractor: Callable[[Dict], List[Dict]]) -> None:
        """Set custom data extractor function"""
        self.data_extractor = extractor
        self.logger.info("Data extractor updated")

    def get_poll_stats(self) -> Dict[str, Any]:
        """Get polling statistics"""
        return {
            "url": self.url,
            "interval": self.interval,
            "method": self.method,
            "is_polling": self._polling,
            "last_poll_time": self._last_poll_time.isoformat() if self._last_poll_time else None,
            "has_etag": bool(self._last_etag),
            "has_last_modified": bool(self._last_modified),
        }

    def __repr__(self) -> str:
        return f"HTTPPoller(url={self.url}, interval={self.interval}s, method={self.method})"
