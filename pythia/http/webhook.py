"""
HTTP Webhook Client for Pythia framework
"""

import asyncio
from typing import Any, Dict, Optional, Union, List
from urllib.parse import urljoin

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, RetryCallState

from pythia.core.message import Message
from pythia.config.http import WebhookConfig
from pythia.logging import get_pythia_logger


def log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log retry attempts"""
    logger = get_pythia_logger("WebhookClient")
    logger.warning(
        "Retrying webhook after error",
        attempt=retry_state.attempt_number,
        next_sleep=retry_state.next_action.sleep if retry_state.next_action else 0,
    )


class WebhookClient:
    """
    HTTP Webhook Client for sending HTTP requests with retry logic

    Example:
        client = WebhookClient(base_url="https://api.example.com")

        # Send JSON data
        success = await client.send("/webhook", {"event": "user_created"})

        # Send with custom headers
        success = await client.send(
            "/webhook",
            {"event": "user_created"},
            headers={"Authorization": "Bearer token"}
        )
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        config: Optional[WebhookConfig] = None,
        **kwargs,
    ):
        """
        Initialize webhook client

        Args:
            base_url: Base URL for webhook endpoints
            config: Webhook configuration
            **kwargs: Additional configuration options
        """
        if config:
            self.config = config
        else:
            config_dict = {"base_url": base_url or kwargs.get("base_url", ""), **kwargs}
            self.config = WebhookConfig(**config_dict)

        self.logger = get_pythia_logger(f"WebhookClient[{self.config.base_url}]")

        # HTTP client management
        self.client: Optional[httpx.AsyncClient] = None
        self._closed = False

    async def __aenter__(self):
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _ensure_client(self) -> None:
        """Ensure HTTP client is initialized and open"""
        if self.client is None or self.client.is_closed:
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

            # Create headers
            default_headers = {
                "User-Agent": f"Pythia-WebhookClient/{self.config.user_agent}",
                "Content-Type": "application/json",
            }

            if self.config.default_headers:
                default_headers.update(self.config.default_headers)

            self.client = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                headers=default_headers,
                verify=self.config.verify_ssl,
                follow_redirects=self.config.follow_redirects,
                max_redirects=self.config.max_redirects,
            )
            self._closed = False

    async def close(self) -> None:
        """Close HTTP client"""
        if self.client and not self.client.is_closed:
            await self.client.aclose()
            self._closed = True

    async def send(
        self,
        endpoint: str,
        data: Union[Dict[str, Any], Message, str, bytes, None] = None,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> bool:
        """
        Send webhook request with retry logic

        Args:
            endpoint: URL endpoint (relative to base_url or absolute)
            data: Data to send (JSON, Message, string, or bytes)
            method: HTTP method (POST, PUT, PATCH, etc.)
            headers: Additional headers
            params: URL parameters
            **kwargs: Additional request options

        Returns:
            bool: True if request was successful
        """
        try:
            return await self._send_with_retry(
                endpoint=endpoint,
                data=data,
                method=method,
                headers=headers,
                params=params,
                **kwargs,
            )
        except Exception as e:
            self.logger.error(
                "All webhook retry attempts failed",
                endpoint=endpoint,
                method=method,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        before_sleep=log_retry_attempt,
    )
    async def _send_with_retry(
        self,
        endpoint: str,
        data: Union[Dict[str, Any], Message, str, bytes, None] = None,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> bool:
        """Internal method with retry logic"""
        await self._ensure_client()

        if not self.client:
            raise RuntimeError("HTTP client not initialized")

        # Build URL
        url = self._build_url(endpoint)

        # Prepare headers
        request_headers = {}
        if headers:
            request_headers.update(headers)

        # Prepare data
        json_data = None
        content = None

        if data is not None:
            if isinstance(data, Message):
                json_data = {
                    "body": data.body,
                    "message_id": data.message_id,
                    "headers": data.headers,
                    "timestamp": data.timestamp.isoformat() if data.timestamp else None,
                }
            elif isinstance(data, dict):
                json_data = data
            elif isinstance(data, str):
                content = data
                request_headers["Content-Type"] = "text/plain"
            elif isinstance(data, bytes):
                content = data
                request_headers["Content-Type"] = "application/octet-stream"
            else:
                # Try to serialize as JSON
                import json

                try:
                    json_data = json.loads(json.dumps(data, default=str))
                except Exception:
                    content = str(data)
                    request_headers["Content-Type"] = "text/plain"

        # Create context for logging
        context = {
            "url": url,
            "method": method,
            "has_data": data is not None,
        }

        try:
            self.logger.debug("Sending webhook request", **context)

            # Send request
            response = await self.client.request(
                method=method,
                url=url,
                json=json_data,
                content=content,
                headers=request_headers,
                params=params,
                **kwargs,
            )

            # Check if response is successful
            if response.is_success:
                self._log_success(response, url, method)
                return True
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"

                # Determine if error is retryable
                if self._is_retryable_status(response.status_code):
                    self.logger.warning(
                        "Webhook request failed (retryable)",
                        url=url,
                        method=method,
                        status_code=response.status_code,
                        response_text=response.text[:200],
                    )
                    raise httpx.HTTPStatusError(
                        error_msg, request=response.request, response=response
                    )
                else:
                    self.logger.error(
                        "Webhook request failed (non-retryable)",
                        url=url,
                        method=method,
                        status_code=response.status_code,
                        response_text=response.text[:200],
                    )
                    return False

        except httpx.TimeoutException as e:
            self.logger.warning(
                "Webhook request timeout (retryable)",
                url=url,
                method=method,
                error=str(e),
            )
            raise

        except httpx.ConnectError as e:
            self.logger.warning(
                "Webhook connection error (retryable)",
                url=url,
                method=method,
                error=str(e),
            )
            raise

        except Exception as e:
            # Check if client was closed and needs recreation
            if isinstance(e, RuntimeError) and "client has been closed" in str(e).lower():
                self.client = None
                raise

            self.logger.error(
                "Webhook request error",
                url=url,
                method=method,
                error=str(e),
                error_type=type(e).__name__,
            )

            # Re-raise for retry if it's a network-related error
            if isinstance(e, (httpx.NetworkError, httpx.TimeoutException)):
                raise
            else:
                return False

    async def send_batch(self, requests: List[Dict[str, Any]], **kwargs) -> Dict[str, bool]:
        """
        Send multiple webhook requests concurrently

        Args:
            requests: List of request configurations, each containing:
                - endpoint: URL endpoint
                - data: Request data (optional)
                - method: HTTP method (optional, defaults to POST)
                - headers: Request headers (optional)
                - params: URL parameters (optional)
            **kwargs: Additional options applied to all requests

        Returns:
            Dict[str, bool]: endpoint -> success mapping
        """
        if not requests:
            return {}

        self.logger.info("Sending webhook batch", count=len(requests))

        # Create tasks for concurrent execution
        tasks = []
        endpoints = []

        for req in requests:
            endpoint = req["endpoint"]
            endpoints.append(endpoint)

            task = self.send(
                endpoint=endpoint,
                data=req.get("data"),
                method=req.get("method", "POST"),
                headers=req.get("headers"),
                params=req.get("params"),
                **kwargs,
            )
            tasks.append(task)

        # Execute all requests concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        success_map = {}
        successful_count = 0

        for i, result in enumerate(results):
            endpoint = endpoints[i]
            if isinstance(result, Exception):
                self.logger.error(
                    "Batch request failed with exception",
                    endpoint=endpoint,
                    error=str(result),
                )
                success_map[endpoint] = False
            else:
                success_map[endpoint] = result
                if result:
                    successful_count += 1

        self.logger.info(
            "Webhook batch completed",
            total=len(requests),
            successful=successful_count,
            success_rate=f"{successful_count / len(requests) * 100:.1f}%",
        )

        return success_map

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint"""
        if endpoint.startswith(("http://", "https://")):
            # Absolute URL
            return endpoint
        else:
            # Relative URL - join with base_url
            if not self.config.base_url:
                raise ValueError("No base_url configured for relative endpoint")
            return urljoin(self.config.base_url.rstrip("/") + "/", endpoint.lstrip("/"))

    def _is_retryable_status(self, status_code: int) -> bool:
        """Determine if HTTP status code indicates a retryable error"""
        # Retry on server errors and some client errors
        retryable_codes = {
            408,  # Request Timeout
            429,  # Too Many Requests
            500,  # Internal Server Error
            502,  # Bad Gateway
            503,  # Service Unavailable
            504,  # Gateway Timeout
        }
        return status_code in retryable_codes

    def _log_success(self, response: httpx.Response, url: str, method: str) -> None:
        """Log successful webhook request"""
        self.logger.info(
            "Webhook request successful",
            url=url,
            method=method,
            status_code=response.status_code,
            response_time=response.elapsed.total_seconds() if response.elapsed else None,
            response_size=len(response.content),
        )

    async def health_check(self) -> bool:
        """Check if webhook endpoint is healthy"""
        if not self.config.base_url:
            return False

        try:
            health_url = urljoin(self.config.base_url.rstrip("/") + "/", "health")

            await self._ensure_client()
            if not self.client:
                return False

            response = await self.client.get(health_url, timeout=5.0)
            return response.is_success

        except Exception as e:
            self.logger.warning("Health check failed", error=str(e))
            return False

    async def get(self, endpoint: str, **kwargs) -> bool:
        """Send GET request"""
        return await self.send(endpoint, method="GET", **kwargs)

    async def post(self, endpoint: str, data: Any = None, **kwargs) -> bool:
        """Send POST request"""
        return await self.send(endpoint, data, method="POST", **kwargs)

    async def put(self, endpoint: str, data: Any = None, **kwargs) -> bool:
        """Send PUT request"""
        return await self.send(endpoint, data, method="PUT", **kwargs)

    async def patch(self, endpoint: str, data: Any = None, **kwargs) -> bool:
        """Send PATCH request"""
        return await self.send(endpoint, data, method="PATCH", **kwargs)

    async def delete(self, endpoint: str, **kwargs) -> bool:
        """Send DELETE request"""
        return await self.send(endpoint, method="DELETE", **kwargs)

    def __repr__(self) -> str:
        return f"WebhookClient(base_url={self.config.base_url})"
