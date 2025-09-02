"""
Advanced retry policies for Pythia jobs and message processing
"""

import asyncio
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, List, Optional, Type
from dataclasses import dataclass
from enum import Enum

from ..logging import get_pythia_logger


class RetryTrigger(str, Enum):
    """Conditions that trigger retries"""

    ANY_ERROR = "any_error"
    SPECIFIC_ERROR = "specific_error"
    HTTP_ERROR = "http_error"
    NETWORK_ERROR = "network_error"
    TIMEOUT_ERROR = "timeout_error"
    RATE_LIMIT = "rate_limit"
    TEMPORARY_FAILURE = "temporary_failure"


@dataclass
class RetryAttempt:
    """Information about a retry attempt"""

    attempt_number: int
    error: Exception
    error_type: str
    timestamp: datetime
    delay_used: float
    next_delay: Optional[float] = None


class RetryPolicy(ABC):
    """Abstract base class for retry policies"""

    @abstractmethod
    def should_retry(self, error: Exception, attempt: int, elapsed_time: float) -> bool:
        """Determine if an operation should be retried"""
        pass

    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """Get delay before next retry attempt"""
        pass

    @abstractmethod
    def get_max_attempts(self) -> int:
        """Get maximum number of retry attempts"""
        pass

    def on_retry(self, attempt: RetryAttempt) -> None:
        """Called when a retry is about to be executed"""
        pass

    def on_failure(self, final_error: Exception, attempts: List[RetryAttempt]) -> None:
        """Called when all retries have been exhausted"""
        pass


class NoRetryPolicy(RetryPolicy):
    """Policy that never retries"""

    def should_retry(self, error: Exception, attempt: int, elapsed_time: float) -> bool:
        return False

    def get_delay(self, attempt: int) -> float:
        return 0.0

    def get_max_attempts(self) -> int:
        return 1


class FixedDelayPolicy(RetryPolicy):
    """Retry with fixed delay between attempts"""

    def __init__(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        max_elapsed_time: Optional[float] = None,
        retry_on: List[RetryTrigger] = None,
        retry_on_exceptions: List[Type[Exception]] = None,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.delay = delay
        self.max_elapsed_time = max_elapsed_time
        self.retry_on = retry_on or [RetryTrigger.ANY_ERROR]
        self.retry_on_exceptions = retry_on_exceptions or []
        self.jitter = jitter

        self.logger = get_pythia_logger("FixedDelayPolicy")

    def should_retry(self, error: Exception, attempt: int, elapsed_time: float) -> bool:
        # Check attempt limit
        if attempt >= self.max_attempts:
            return False

        # Check time limit
        if self.max_elapsed_time and elapsed_time >= self.max_elapsed_time:
            return False

        # Check if error type should be retried
        return self._should_retry_error(error)

    def get_delay(self, attempt: int) -> float:
        delay = self.delay

        # Add jitter to prevent thundering herd
        if self.jitter:
            delay *= random.uniform(0.5, 1.5)

        return delay

    def get_max_attempts(self) -> int:
        return self.max_attempts

    def _should_retry_error(self, error: Exception) -> bool:
        """Check if error should trigger retry"""

        # Check specific exception types
        if self.retry_on_exceptions:
            return any(isinstance(error, exc_type) for exc_type in self.retry_on_exceptions)

        # Check retry triggers
        if RetryTrigger.ANY_ERROR in self.retry_on:
            return True

        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        for trigger in self.retry_on:
            if trigger == RetryTrigger.HTTP_ERROR and "http" in error_type:
                return True
            elif trigger == RetryTrigger.NETWORK_ERROR and any(
                term in error_str for term in ["network", "connection", "dns", "timeout"]
            ):
                return True
            elif trigger == RetryTrigger.TIMEOUT_ERROR and "timeout" in error_str:
                return True
            elif trigger == RetryTrigger.RATE_LIMIT and any(
                term in error_str for term in ["rate limit", "too many requests", "429"]
            ):
                return True

        return False


class ExponentialBackoffPolicy(RetryPolicy):
    """Retry with exponential backoff"""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 300.0,
        backoff_multiplier: float = 2.0,
        max_elapsed_time: Optional[float] = None,
        retry_on: List[RetryTrigger] = None,
        retry_on_exceptions: List[Type[Exception]] = None,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.max_elapsed_time = max_elapsed_time
        self.retry_on = retry_on or [RetryTrigger.ANY_ERROR]
        self.retry_on_exceptions = retry_on_exceptions or []
        self.jitter = jitter

        self.logger = get_pythia_logger("ExponentialBackoffPolicy")

    def should_retry(self, error: Exception, attempt: int, elapsed_time: float) -> bool:
        if attempt >= self.max_attempts:
            return False

        if self.max_elapsed_time and elapsed_time >= self.max_elapsed_time:
            return False

        return self._should_retry_error(error)

    def get_delay(self, attempt: int) -> float:
        # Calculate exponential delay
        delay = self.base_delay * (self.backoff_multiplier ** (attempt - 1))
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter:
            delay *= random.uniform(0.5, 1.5)

        return delay

    def get_max_attempts(self) -> int:
        return self.max_attempts

    def _should_retry_error(self, error: Exception) -> bool:
        """Check if error should trigger retry (same logic as FixedDelayPolicy)"""
        if self.retry_on_exceptions:
            return any(isinstance(error, exc_type) for exc_type in self.retry_on_exceptions)

        if RetryTrigger.ANY_ERROR in self.retry_on:
            return True

        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        for trigger in self.retry_on:
            if trigger == RetryTrigger.HTTP_ERROR and "http" in error_type:
                return True
            elif trigger == RetryTrigger.NETWORK_ERROR and any(
                term in error_str for term in ["network", "connection", "dns", "timeout"]
            ):
                return True
            elif trigger == RetryTrigger.TIMEOUT_ERROR and "timeout" in error_str:
                return True
            elif trigger == RetryTrigger.RATE_LIMIT and any(
                term in error_str for term in ["rate limit", "too many requests", "429"]
            ):
                return True

        return False


class LinearBackoffPolicy(RetryPolicy):
    """Retry with linear backoff (delay increases linearly)"""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        increment: float = 1.0,
        max_delay: float = 60.0,
        max_elapsed_time: Optional[float] = None,
        retry_on: List[RetryTrigger] = None,
        retry_on_exceptions: List[Type[Exception]] = None,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.increment = increment
        self.max_delay = max_delay
        self.max_elapsed_time = max_elapsed_time
        self.retry_on = retry_on or [RetryTrigger.ANY_ERROR]
        self.retry_on_exceptions = retry_on_exceptions or []
        self.jitter = jitter

        self.logger = get_pythia_logger("LinearBackoffPolicy")

    def should_retry(self, error: Exception, attempt: int, elapsed_time: float) -> bool:
        if attempt >= self.max_attempts:
            return False

        if self.max_elapsed_time and elapsed_time >= self.max_elapsed_time:
            return False

        return self._should_retry_error(error)

    def get_delay(self, attempt: int) -> float:
        delay = self.base_delay + (self.increment * (attempt - 1))
        delay = min(delay, self.max_delay)

        if self.jitter:
            delay *= random.uniform(0.5, 1.5)

        return delay

    def get_max_attempts(self) -> int:
        return self.max_attempts

    def _should_retry_error(self, error: Exception) -> bool:
        """Same error checking logic"""
        if self.retry_on_exceptions:
            return any(isinstance(error, exc_type) for exc_type in self.retry_on_exceptions)

        if RetryTrigger.ANY_ERROR in self.retry_on:
            return True

        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        for trigger in self.retry_on:
            if trigger == RetryTrigger.HTTP_ERROR and "http" in error_type:
                return True
            elif trigger == RetryTrigger.NETWORK_ERROR and any(
                term in error_str for term in ["network", "connection", "dns", "timeout"]
            ):
                return True
            elif trigger == RetryTrigger.TIMEOUT_ERROR and "timeout" in error_str:
                return True
            elif trigger == RetryTrigger.RATE_LIMIT and any(
                term in error_str for term in ["rate limit", "too many requests", "429"]
            ):
                return True

        return False


class CustomDelayPolicy(RetryPolicy):
    """Retry with custom delay function"""

    def __init__(
        self,
        max_attempts: int,
        delay_func: Callable[[int], float],
        should_retry_func: Optional[Callable[[Exception, int, float], bool]] = None,
        max_elapsed_time: Optional[float] = None,
    ):
        self.max_attempts = max_attempts
        self.delay_func = delay_func
        self.should_retry_func = should_retry_func
        self.max_elapsed_time = max_elapsed_time

        self.logger = get_pythia_logger("CustomDelayPolicy")

    def should_retry(self, error: Exception, attempt: int, elapsed_time: float) -> bool:
        if attempt >= self.max_attempts:
            return False

        if self.max_elapsed_time and elapsed_time >= self.max_elapsed_time:
            return False

        if self.should_retry_func:
            return self.should_retry_func(error, attempt, elapsed_time)

        return True  # Default to retry

    def get_delay(self, attempt: int) -> float:
        return self.delay_func(attempt)

    def get_max_attempts(self) -> int:
        return self.max_attempts


class RetryManager:
    """Manager for executing operations with retry policies"""

    def __init__(self, policy: RetryPolicy, logger_name: str = "RetryManager"):
        self.policy = policy
        self.logger = get_pythia_logger(logger_name)

    async def execute(self, operation: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute an operation with retries"""
        attempts = []
        start_time = time.time()

        for attempt in range(1, self.policy.get_max_attempts() + 1):
            try:
                # Execute the operation
                if asyncio.iscoroutinefunction(operation):
                    result = await operation(*args, **kwargs)
                else:
                    result = operation(*args, **kwargs)

                # Success - log if there were retries
                if attempt > 1:
                    elapsed_time = time.time() - start_time
                    self.logger.info(
                        f"Operation succeeded after {attempt} attempts",
                        elapsed_time=elapsed_time,
                        total_attempts=attempt,
                    )

                return result

            except Exception as error:
                elapsed_time = time.time() - start_time

                # Create retry attempt record
                retry_attempt = RetryAttempt(
                    attempt_number=attempt,
                    error=error,
                    error_type=type(error).__name__,
                    timestamp=datetime.now(),
                    delay_used=0.0,
                )
                attempts.append(retry_attempt)

                # Check if we should retry
                should_retry = self.policy.should_retry(error, attempt, elapsed_time)

                if not should_retry or attempt >= self.policy.get_max_attempts():
                    # No more retries - call policy failure handler
                    self.policy.on_failure(error, attempts)

                    self.logger.error(
                        f"Operation failed after {attempt} attempts",
                        error=str(error),
                        error_type=type(error).__name__,
                        elapsed_time=elapsed_time,
                        total_attempts=len(attempts),
                    )

                    raise error

                # Calculate delay and wait
                delay = self.policy.get_delay(attempt)
                retry_attempt.delay_used = delay
                retry_attempt.next_delay = delay

                self.logger.warning(
                    f"Attempt {attempt} failed, retrying in {delay:.2f}s",
                    error=str(error),
                    error_type=type(error).__name__,
                    delay=delay,
                    attempt=attempt,
                    max_attempts=self.policy.get_max_attempts(),
                )

                # Call policy retry handler
                self.policy.on_retry(retry_attempt)

                # Wait before retry
                await asyncio.sleep(delay)

        # This should never be reached due to the logic above
        raise RuntimeError("Unexpected end of retry loop")

    def execute_sync(self, operation: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute a synchronous operation with retries"""
        attempts = []
        start_time = time.time()

        for attempt in range(1, self.policy.get_max_attempts() + 1):
            try:
                result = operation(*args, **kwargs)

                if attempt > 1:
                    elapsed_time = time.time() - start_time
                    self.logger.info(
                        f"Operation succeeded after {attempt} attempts",
                        elapsed_time=elapsed_time,
                        total_attempts=attempt,
                    )

                return result

            except Exception as error:
                elapsed_time = time.time() - start_time

                retry_attempt = RetryAttempt(
                    attempt_number=attempt,
                    error=error,
                    error_type=type(error).__name__,
                    timestamp=datetime.now(),
                    delay_used=0.0,
                )
                attempts.append(retry_attempt)

                should_retry = self.policy.should_retry(error, attempt, elapsed_time)

                if not should_retry or attempt >= self.policy.get_max_attempts():
                    self.policy.on_failure(error, attempts)

                    self.logger.error(
                        f"Operation failed after {attempt} attempts",
                        error=str(error),
                        error_type=type(error).__name__,
                        elapsed_time=elapsed_time,
                        total_attempts=len(attempts),
                    )

                    raise error

                delay = self.policy.get_delay(attempt)
                retry_attempt.delay_used = delay

                self.logger.warning(
                    f"Attempt {attempt} failed, retrying in {delay:.2f}s",
                    error=str(error),
                    error_type=type(error).__name__,
                    delay=delay,
                    attempt=attempt,
                    max_attempts=self.policy.get_max_attempts(),
                )

                self.policy.on_retry(retry_attempt)

                time.sleep(delay)

        raise RuntimeError("Unexpected end of retry loop")


# Decorator for adding retry to functions
def with_retry(
    policy: Optional[RetryPolicy] = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    max_delay: float = 300.0,
    retry_on: List[RetryTrigger] = None,
    retry_on_exceptions: List[Type[Exception]] = None,
):
    """Decorator to add retry logic to functions"""

    if policy is None:
        policy = ExponentialBackoffPolicy(
            max_attempts=max_attempts,
            base_delay=base_delay,
            backoff_multiplier=backoff_multiplier,
            max_delay=max_delay,
            retry_on=retry_on or [RetryTrigger.ANY_ERROR],
            retry_on_exceptions=retry_on_exceptions,
        )

    def decorator(func: Callable) -> Callable:
        retry_manager = RetryManager(policy, f"RetryDecorator[{func.__name__}]")

        if asyncio.iscoroutinefunction(func):

            async def async_wrapper(*args, **kwargs):
                return await retry_manager.execute(func, *args, **kwargs)

            return async_wrapper
        else:

            def sync_wrapper(*args, **kwargs):
                return retry_manager.execute_sync(func, *args, **kwargs)

            return sync_wrapper

    return decorator


# Common retry policies factory
class CommonRetryPolicies:
    """Factory for common retry policies"""

    @staticmethod
    def immediate_retry(max_attempts: int = 3) -> FixedDelayPolicy:
        """Retry immediately without delay"""
        return FixedDelayPolicy(max_attempts=max_attempts, delay=0.0, jitter=False)

    @staticmethod
    def fixed_delay(delay: float = 1.0, max_attempts: int = 3) -> FixedDelayPolicy:
        """Fixed delay between retries"""
        return FixedDelayPolicy(max_attempts=max_attempts, delay=delay)

    @staticmethod
    def exponential_backoff(
        base_delay: float = 1.0, max_attempts: int = 3, max_delay: float = 300.0
    ) -> ExponentialBackoffPolicy:
        """Exponential backoff with reasonable defaults"""
        return ExponentialBackoffPolicy(
            max_attempts=max_attempts, base_delay=base_delay, max_delay=max_delay
        )

    @staticmethod
    def linear_backoff(
        base_delay: float = 1.0, increment: float = 1.0, max_attempts: int = 3
    ) -> LinearBackoffPolicy:
        """Linear backoff"""
        return LinearBackoffPolicy(
            max_attempts=max_attempts, base_delay=base_delay, increment=increment
        )

    @staticmethod
    def network_errors_only(
        max_attempts: int = 3, base_delay: float = 1.0
    ) -> ExponentialBackoffPolicy:
        """Only retry network-related errors"""
        return ExponentialBackoffPolicy(
            max_attempts=max_attempts,
            base_delay=base_delay,
            retry_on=[RetryTrigger.NETWORK_ERROR, RetryTrigger.TIMEOUT_ERROR],
        )

    @staticmethod
    def http_errors_only(
        max_attempts: int = 3, base_delay: float = 1.0
    ) -> ExponentialBackoffPolicy:
        """Only retry HTTP-related errors"""
        return ExponentialBackoffPolicy(
            max_attempts=max_attempts,
            base_delay=base_delay,
            retry_on=[RetryTrigger.HTTP_ERROR, RetryTrigger.RATE_LIMIT],
        )

    @staticmethod
    def no_retry() -> NoRetryPolicy:
        """Never retry"""
        return NoRetryPolicy()
