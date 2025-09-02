"""
Enhanced HTTP client with circuit breaker, retry policies, and connection pooling
"""

from typing import Any, Dict, Optional, Callable, AsyncIterator
import httpx

from pythia.logging import get_pythia_logger
from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from .retry import RetryPolicy, RetryConfig


class HTTPClientConfig:
    """Configuration for enhanced HTTP client"""

    def __init__(
        self,
        # Connection pooling
        max_connections: int = 100,
        max_keepalive_connections: int = 20,
        keepalive_expiry: float = 5.0,
        # Timeouts
        connect_timeout: float = 5.0,
        read_timeout: float = 10.0,
        write_timeout: float = 5.0,
        pool_timeout: float = 5.0,
        # SSL & Security
        verify_ssl: bool = True,
        cert: Optional[str] = None,
        trust_env: bool = True,
        # HTTP behavior
        follow_redirects: bool = True,
        max_redirects: int = 20,
        # Circuit breaker (None to disable)
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        # Retry policy (None to disable)
        retry_config: Optional[RetryConfig] = None,
        # Headers
        default_headers: Optional[Dict[str, str]] = None,
        user_agent: str = "Pythia-HTTPClient/1.0",
    ):
        # Connection settings
        self.max_connections = max_connections
        self.max_keepalive_connections = max_keepalive_connections
        self.keepalive_expiry = keepalive_expiry

        # Timeout settings
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.pool_timeout = pool_timeout

        # SSL settings
        self.verify_ssl = verify_ssl
        self.cert = cert
        self.trust_env = trust_env

        # HTTP settings
        self.follow_redirects = follow_redirects
        self.max_redirects = max_redirects

        # Resilience patterns
        self.circuit_breaker_config = circuit_breaker_config
        self.retry_config = retry_config

        # Headers
        self.default_headers = default_headers or {}
        self.user_agent = user_agent


class PythiaHTTPClient:
    """
    Enhanced HTTP client with circuit breaker, retry policies, and connection pooling

    Features:
    - Automatic circuit breaker protection
    - Advanced retry policies with exponential backoff
    - Connection pooling and keep-alive
    - Comprehensive request/response middleware
    - Detailed metrics and monitoring

    Example:
        config = HTTPClientConfig(
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=5),
            retry_config=RetryConfig(max_attempts=3)
        )

        client = PythiaHTTPClient(config=config)

        async with client:
            response = await client.get("https://api.example.com/data")
    """

    def __init__(
        self,
        name: str = "default",
        config: Optional[HTTPClientConfig] = None,
        base_url: Optional[str] = None,
        on_request: Optional[Callable[[httpx.Request], None]] = None,
        on_response: Optional[Callable[[httpx.Response], None]] = None,
    ):
        self.name = name
        self.config = config or HTTPClientConfig()
        self.base_url = base_url
        self.on_request = on_request
        self.on_response = on_response

        self.logger = get_pythia_logger(f"HTTPClient[{name}]")

        # Internal state
        self._client: Optional[httpx.AsyncClient] = None
        self._circuit_breaker: Optional[CircuitBreaker] = None
        self._retry_policy: Optional[RetryPolicy] = None

        # Statistics
        self._request_count = 0
        self._success_count = 0
        self._error_count = 0

        self.logger.info(
            "HTTP client initialized",
            name=name,
            base_url=base_url,
            has_circuit_breaker=bool(config.circuit_breaker_config),
            has_retry_policy=bool(config.retry_config),
        )

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()

    async def connect(self) -> None:
        """Initialize HTTP client and resilience patterns"""
        if self._client is not None:
            return

        try:
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
            headers = {
                "User-Agent": self.config.user_agent,
                **self.config.default_headers,
            }

            # Create HTTP client
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=timeout,
                limits=limits,
                headers=headers,
                verify=self.config.verify_ssl,
                cert=self.config.cert,
                trust_env=self.config.trust_env,
                follow_redirects=self.config.follow_redirects,
                max_redirects=self.config.max_redirects,
            )

            # Initialize circuit breaker
            if self.config.circuit_breaker_config:
                self._circuit_breaker = CircuitBreaker(
                    name=f"{self.name}-circuit-breaker",
                    config=self.config.circuit_breaker_config,
                )

            # Initialize retry policy
            if self.config.retry_config:
                self._retry_policy = RetryPolicy(
                    name=f"{self.name}-retry-policy", config=self.config.retry_config
                )

            self.logger.info("HTTP client connected successfully")

        except Exception as e:
            self.logger.error("Failed to initialize HTTP client", error=str(e))
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        """Close HTTP client and cleanup resources"""
        try:
            if self._client:
                await self._client.aclose()
                self.logger.info("HTTP client disconnected")
        except Exception as e:
            self.logger.warning("Error during disconnect", error=str(e))
        finally:
            self._client = None
            self._circuit_breaker = None
            self._retry_policy = None

    async def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """
        Make HTTP request with circuit breaker and retry protection

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request parameters

        Returns:
            HTTP response

        Raises:
            CircuitBreakerError: If circuit breaker is open
            RetryExhaustedError: If all retry attempts failed
            httpx.HTTPError: HTTP-related errors
        """
        if not self._client:
            raise RuntimeError("HTTP client not initialized. Use async context manager.")

        self._request_count += 1

        async def _make_request() -> httpx.Response:
            """Internal request function"""
            # Apply middleware
            request = self._client.build_request(method, url, **kwargs)
            if self.on_request:
                try:
                    self.on_request(request)
                except Exception as e:
                    self.logger.warning("Error in request middleware", error=str(e))

            # Make request
            response = await self._client.send(request)

            # Apply response middleware
            if self.on_response:
                try:
                    self.on_response(response)
                except Exception as e:
                    self.logger.warning("Error in response middleware", error=str(e))

            # Check for HTTP errors
            response.raise_for_status()

            return response

        try:
            # Execute request with resilience patterns
            if self._circuit_breaker and self._retry_policy:
                # Both circuit breaker and retry
                async with self._circuit_breaker:
                    response = await self._retry_policy.execute(_make_request)
            elif self._circuit_breaker:
                # Circuit breaker only
                async with self._circuit_breaker:
                    response = await _make_request()
            elif self._retry_policy:
                # Retry only
                response = await self._retry_policy.execute(_make_request)
            else:
                # No resilience patterns
                response = await _make_request()

            self._success_count += 1

            self.logger.debug(
                "HTTP request successful",
                method=method,
                url=str(url),
                status_code=response.status_code,
                response_size=len(response.content),
            )

            return response

        except Exception as e:
            self._error_count += 1

            self.logger.error(
                "HTTP request failed",
                method=method,
                url=str(url),
                error=str(e),
                error_type=type(e).__name__,
            )

            raise

    # Convenience methods
    async def get(self, url: str, **kwargs) -> httpx.Response:
        """GET request"""
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs) -> httpx.Response:
        """POST request"""
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs) -> httpx.Response:
        """PUT request"""
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs) -> httpx.Response:
        """PATCH request"""
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs) -> httpx.Response:
        """DELETE request"""
        return await self.request("DELETE", url, **kwargs)

    async def head(self, url: str, **kwargs) -> httpx.Response:
        """HEAD request"""
        return await self.request("HEAD", url, **kwargs)

    async def options(self, url: str, **kwargs) -> httpx.Response:
        """OPTIONS request"""
        return await self.request("OPTIONS", url, **kwargs)

    # Streaming support
    async def stream(self, method: str, url: str, **kwargs) -> AsyncIterator[bytes]:
        """
        Stream HTTP response content

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request parameters

        Yields:
            Response content chunks
        """
        if not self._client:
            raise RuntimeError("HTTP client not initialized")

        async def _stream_request():
            async with self._client.stream(method, url, **kwargs) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    yield chunk

        try:
            if self._circuit_breaker:
                async with self._circuit_breaker:
                    async for chunk in _stream_request():
                        yield chunk
            else:
                async for chunk in _stream_request():
                    yield chunk

        except Exception as e:
            self.logger.error("HTTP streaming failed", method=method, url=str(url), error=str(e))
            raise

    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self._client is not None and not self._client.is_closed

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        stats = {
            "name": self.name,
            "is_connected": self.is_connected(),
            "request_count": self._request_count,
            "success_count": self._success_count,
            "error_count": self._error_count,
            "success_rate": (
                self._success_count / max(self._request_count, 1) if self._request_count > 0 else 0
            ),
            "config": {
                "max_connections": self.config.max_connections,
                "max_keepalive_connections": self.config.max_keepalive_connections,
                "connect_timeout": self.config.connect_timeout,
                "read_timeout": self.config.read_timeout,
            },
        }

        # Add circuit breaker stats
        if self._circuit_breaker:
            stats["circuit_breaker"] = self._circuit_breaker.get_stats()

        # Add retry policy stats
        if self._retry_policy:
            stats["retry_policy"] = self._retry_policy.get_stats()

        return stats

    async def health_check(self, url: Optional[str] = None) -> bool:
        """
        Perform health check

        Args:
            url: Optional URL to check (defaults to base_url)

        Returns:
            True if healthy, False otherwise
        """
        if not self.is_connected():
            return False

        try:
            check_url = url or self.base_url or "/"
            response = await self.head(check_url, timeout=5.0)
            return response.is_success
        except Exception as e:
            self.logger.warning("Health check failed", error=str(e))
            return False

    def __repr__(self) -> str:
        return (
            f"PythiaHTTPClient(name={self.name}, "
            f"connected={self.is_connected()}, "
            f"requests={self._request_count})"
        )
