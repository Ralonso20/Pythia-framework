"""HTTP utilities and clients"""

from .webhook import WebhookClient
from .poller import HTTPPoller
from .multi_poller import MultiHTTPPoller, ScheduledHTTPPoller

# Enhanced HTTP client with resilience patterns
from .client import PythiaHTTPClient, HTTPClientConfig
from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitBreakerError,
)
from .retry import RetryPolicy, RetryConfig, RetryStrategy, RetryExhaustedError

__all__ = [
    # Original HTTP utilities
    "WebhookClient",
    "HTTPPoller",
    "MultiHTTPPoller",
    "ScheduledHTTPPoller",
    # Enhanced HTTP client
    "PythiaHTTPClient",
    "HTTPClientConfig",
    # Circuit breaker
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CircuitBreakerError",
    # Retry policies
    "RetryPolicy",
    "RetryConfig",
    "RetryStrategy",
    "RetryExhaustedError",
]
