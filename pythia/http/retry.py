"""
Advanced retry policies with exponential backoff for HTTP clients
"""

import asyncio
import random
import time
from typing import Any, Callable, Optional, List, Type, Dict
from dataclasses import dataclass
from enum import Enum
import httpx

from pythia.logging import get_pythia_logger


class RetryStrategy(Enum):
    """Retry strategy types"""

    FIXED_DELAY = "fixed_delay"  # Fixed delay between retries
    EXPONENTIAL_BACKOFF = "exponential"  # Exponential backoff with jitter
    LINEAR_BACKOFF = "linear"  # Linear increase in delay


@dataclass
class RetryConfig:
    """Retry policy configuration"""

    max_attempts: int = 3  # Maximum retry attempts (including initial)
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay between retries
    exponential_base: float = 2.0  # Base for exponential backoff
    jitter: bool = True  # Add randomness to prevent thundering herd
    jitter_range: float = 0.1  # Jitter range (0.0 - 1.0)

    # HTTP-specific retry conditions
    retry_on_status: List[int] = None  # HTTP status codes to retry on
    retry_on_exceptions: List[Type[Exception]] = None  # Exceptions to retry on

    def __post_init__(self):
        """Set default retry conditions"""
        if self.retry_on_status is None:
            self.retry_on_status = [
                408,  # Request Timeout
                429,  # Too Many Requests
                500,  # Internal Server Error
                502,  # Bad Gateway
                503,  # Service Unavailable
                504,  # Gateway Timeout
            ]

        if self.retry_on_exceptions is None:
            self.retry_on_exceptions = [
                httpx.TimeoutException,
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.WriteTimeout,
                httpx.PoolTimeout,
                httpx.ConnectError,
                httpx.NetworkError,
            ]


@dataclass
class RetryAttempt:
    """Information about a retry attempt"""

    attempt_number: int
    delay: float
    exception: Optional[Exception]
    response: Optional[httpx.Response]
    timestamp: float
    total_elapsed: float


class RetryExhaustedError(Exception):
    """Exception raised when all retry attempts are exhausted"""

    def __init__(
        self,
        message: str,
        attempts: List[RetryAttempt],
        last_exception: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


class RetryPolicy:
    """
    Advanced retry policy with configurable strategies and conditions

    Supports:
    - Exponential backoff with jitter
    - Linear backoff
    - Fixed delay
    - HTTP-specific retry conditions
    - Detailed retry statistics

    Example:
        retry_policy = RetryPolicy(
            max_attempts=5,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=1.0,
            max_delay=30.0
        )

        # Use with HTTP client
        async with retry_policy:
            response = await client.get(url)
    """

    def __init__(
        self,
        name: str = "default",
        config: Optional[RetryConfig] = None,
        on_retry: Optional[Callable[[RetryAttempt], None]] = None,
    ):
        self.name = name
        self.config = config or RetryConfig()
        self.on_retry = on_retry
        self.logger = get_pythia_logger(f"RetryPolicy[{name}]")

        self.attempts: List[RetryAttempt] = []
        self.start_time: Optional[float] = None

        self.logger.info(
            "Retry policy initialized",
            name=name,
            max_attempts=self.config.max_attempts,
            strategy=self.config.strategy.value,
            base_delay=self.config.base_delay,
        )

    async def __aenter__(self):
        """Async context manager entry"""
        self.attempts = []
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if exc_type is None:
            # Success
            return False

        # Check if we should retry
        if self._should_retry(exc_val):
            attempt_number = len(self.attempts) + 1

            if attempt_number < self.config.max_attempts:
                # Calculate delay and retry
                delay = self._calculate_delay(attempt_number)

                attempt = RetryAttempt(
                    attempt_number=attempt_number,
                    delay=delay,
                    exception=exc_val,
                    response=getattr(exc_val, "response", None),
                    timestamp=time.time(),
                    total_elapsed=time.time() - self.start_time if self.start_time else 0,
                )
                self.attempts.append(attempt)

                self.logger.warning(
                    "Retrying after failure",
                    attempt=attempt_number,
                    max_attempts=self.config.max_attempts,
                    delay=delay,
                    error=str(exc_val),
                )

                # Call retry callback
                if self.on_retry:
                    try:
                        self.on_retry(attempt)
                    except Exception as e:
                        self.logger.error("Error in retry callback", error=str(e))

                # Wait before retry
                await asyncio.sleep(delay)

                # Suppress the exception to retry
                return True

        # No more retries or not retryable
        self._log_final_failure(exc_val)
        return False

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if an exception should trigger a retry"""
        # Check exception types
        for exc_type in self.config.retry_on_exceptions:
            if isinstance(exception, exc_type):
                return True

        # Check HTTP status codes
        if hasattr(exception, "response") and exception.response:
            status_code = exception.response.status_code
            if status_code in self.config.retry_on_status:
                return True

        return False

    def _calculate_delay(self, attempt_number: int) -> float:
        """Calculate delay for retry attempt"""
        if self.config.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.config.base_delay

        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.base_delay * attempt_number

        elif self.config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.config.base_delay * (self.config.exponential_base ** (attempt_number - 1))

        else:
            delay = self.config.base_delay

        # Apply maximum delay limit
        delay = min(delay, self.config.max_delay)

        # Add jitter to prevent thundering herd
        if self.config.jitter:
            jitter_amount = delay * self.config.jitter_range
            jitter = random.uniform(-jitter_amount, jitter_amount)
            delay += jitter
            delay = max(0, delay)  # Ensure non-negative delay

        return delay

    def _log_final_failure(self, exception: Exception) -> None:
        """Log final failure after all retries exhausted"""
        total_attempts = len(self.attempts) + 1
        total_elapsed = time.time() - self.start_time if self.start_time else 0

        self.logger.error(
            "All retry attempts exhausted",
            name=self.name,
            total_attempts=total_attempts,
            max_attempts=self.config.max_attempts,
            total_elapsed=total_elapsed,
            final_error=str(exception),
        )

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with retry policy

        Args:
            func: Async function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Function result

        Raises:
            RetryExhaustedError: If all retry attempts are exhausted
            Exception: Original exception if not retryable
        """
        self.attempts = []
        self.start_time = time.time()

        last_exception = None

        for attempt_number in range(1, self.config.max_attempts + 1):
            try:
                result = await func(*args, **kwargs)

                # Success - log if we had previous failures
                if attempt_number > 1:
                    self.logger.info(
                        "Retry successful",
                        attempt=attempt_number,
                        total_attempts=attempt_number,
                        total_elapsed=time.time() - self.start_time,
                    )

                return result

            except Exception as exc:
                last_exception = exc

                # Check if we should retry
                if attempt_number < self.config.max_attempts and self._should_retry(exc):
                    delay = self._calculate_delay(attempt_number)

                    attempt = RetryAttempt(
                        attempt_number=attempt_number,
                        delay=delay,
                        exception=exc,
                        response=getattr(exc, "response", None),
                        timestamp=time.time(),
                        total_elapsed=time.time() - self.start_time,
                    )
                    self.attempts.append(attempt)

                    self.logger.warning(
                        "Retrying after failure",
                        attempt=attempt_number,
                        max_attempts=self.config.max_attempts,
                        delay=delay,
                        error=str(exc),
                    )

                    # Call retry callback
                    if self.on_retry:
                        try:
                            self.on_retry(attempt)
                        except Exception as e:
                            self.logger.error("Error in retry callback", error=str(e))

                    await asyncio.sleep(delay)
                    continue
                else:
                    # Not retryable or max attempts reached
                    break

        # All retries exhausted
        self._log_final_failure(last_exception)

        if self._should_retry(last_exception):
            raise RetryExhaustedError(
                f"Retry policy '{self.name}' exhausted after {len(self.attempts)} attempts",
                self.attempts,
                last_exception,
            )
        else:
            # Re-raise original non-retryable exception
            raise last_exception

    def get_stats(self) -> Dict[str, Any]:
        """Get retry policy statistics"""
        total_elapsed = 0
        if self.start_time:
            if self.attempts:
                total_elapsed = self.attempts[-1].timestamp - self.start_time
            else:
                total_elapsed = time.time() - self.start_time

        return {
            "name": self.name,
            "total_attempts": len(self.attempts),
            "max_attempts": self.config.max_attempts,
            "strategy": self.config.strategy.value,
            "total_elapsed": total_elapsed,
            "config": {
                "base_delay": self.config.base_delay,
                "max_delay": self.config.max_delay,
                "exponential_base": self.config.exponential_base,
                "jitter": self.config.jitter,
                "retry_on_status": self.config.retry_on_status,
                "retry_on_exceptions": [exc.__name__ for exc in self.config.retry_on_exceptions],
            },
            "attempts": [
                {
                    "attempt_number": attempt.attempt_number,
                    "delay": attempt.delay,
                    "error": str(attempt.exception) if attempt.exception else None,
                    "status_code": (attempt.response.status_code if attempt.response else None),
                    "timestamp": attempt.timestamp,
                    "elapsed": attempt.total_elapsed,
                }
                for attempt in self.attempts
            ],
        }

    def reset(self) -> None:
        """Reset retry policy state"""
        self.attempts = []
        self.start_time = None
        self.logger.debug("Retry policy reset", name=self.name)

    def __repr__(self) -> str:
        return (
            f"RetryPolicy(name={self.name}, "
            f"max_attempts={self.config.max_attempts}, "
            f"strategy={self.config.strategy.value})"
        )
