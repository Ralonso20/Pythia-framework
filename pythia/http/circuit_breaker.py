"""
Circuit Breaker pattern implementation for HTTP clients
"""

import asyncio
import time
from enum import Enum
from typing import Any, Callable, Optional, Dict
from dataclasses import dataclass, field
from collections import deque
import httpx

from pythia.logging import get_pythia_logger


class CircuitState(Enum):
    """Circuit breaker states"""

    CLOSED = "closed"  # Normal operation - requests pass through
    OPEN = "open"  # Circuit is open - requests fail fast
    HALF_OPEN = "half_open"  # Testing if service has recovered


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""

    failure_threshold: int = 5  # Number of failures to open circuit
    recovery_timeout: float = 30.0  # Seconds before attempting recovery
    success_threshold: int = 3  # Successful calls needed to close circuit in half-open
    timeout: float = 10.0  # Request timeout in seconds
    monitor_window: float = 60.0  # Rolling window for failure monitoring (seconds)
    failure_rate_threshold: float = 0.5  # Failure rate (0.0-1.0) to trigger circuit opening


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics"""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    total_requests: int = 0
    total_failures: int = 0
    total_successes: int = 0
    state_change_count: int = 0
    recent_failures: deque = field(default_factory=lambda: deque(maxlen=100))


class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker is open"""

    def __init__(self, message: str, last_error: Optional[Exception] = None):
        super().__init__(message)
        self.last_error = last_error


class CircuitBreaker:
    """
    Circuit breaker implementation for HTTP clients

    Prevents cascading failures by monitoring request failures and
    temporarily blocking requests when failure threshold is exceeded.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Circuit is open, requests fail fast with CircuitBreakerError
    - HALF_OPEN: Testing recovery, limited requests allowed

    Example:
        breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30.0
        )

        # Wrap HTTP calls
        async with breaker:
            response = await client.get(url)
    """

    def __init__(
        self,
        name: str = "default",
        config: Optional[CircuitBreakerConfig] = None,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.stats = CircuitBreakerStats()
        self.on_state_change = on_state_change
        self.logger = get_pythia_logger(f"CircuitBreaker[{name}]")

        # Thread safety
        self._lock = asyncio.Lock()
        self._last_error: Optional[Exception] = None

        self.logger.info(
            "Circuit breaker initialized",
            name=name,
            failure_threshold=self.config.failure_threshold,
            recovery_timeout=self.config.recovery_timeout,
        )

    async def __aenter__(self):
        """Async context manager entry"""
        await self._check_state()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if exc_type is None:
            # Success
            await self._record_success()
        elif exc_type == CircuitBreakerError:
            # Circuit breaker error - don't record as failure
            return False
        elif issubclass(exc_type, (httpx.HTTPError, httpx.TimeoutException)):
            # HTTP error - record as failure
            await self._record_failure(exc_val)
        else:
            # Other errors - record as failure
            await self._record_failure(exc_val)

        return False

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection

        Args:
            func: Async function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Function result

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: Original exception from wrapped function
        """
        async with self:
            return await func(*args, **kwargs)

    async def _check_state(self) -> None:
        """Check and update circuit breaker state"""
        async with self._lock:
            current_time = time.time()

            if self.stats.state == CircuitState.OPEN:
                # Check if recovery timeout has elapsed
                if (
                    self.stats.last_failure_time
                    and current_time - self.stats.last_failure_time >= self.config.recovery_timeout
                ):
                    await self._transition_to_half_open()
                else:
                    # Circuit is still open
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.name}' is OPEN. Last failure: {self._last_error}",
                        self._last_error,
                    )

            elif self.stats.state == CircuitState.HALF_OPEN:
                # In half-open state, allow limited requests
                pass

            # CLOSED state allows all requests

    async def _record_success(self) -> None:
        """Record successful operation"""
        async with self._lock:
            current_time = time.time()
            self.stats.success_count += 1
            self.stats.total_successes += 1
            self.stats.total_requests += 1

            # Remove old failures outside monitor window
            self._clean_recent_failures(current_time)

            if self.stats.state == CircuitState.HALF_OPEN:
                # In half-open, check if we should close circuit
                if self.stats.success_count >= self.config.success_threshold:
                    await self._transition_to_closed()

            self.logger.debug(
                "Success recorded",
                state=self.stats.state.value,
                success_count=self.stats.success_count,
                total_requests=self.stats.total_requests,
            )

    async def _record_failure(self, error: Exception) -> None:
        """Record failed operation"""
        async with self._lock:
            current_time = time.time()
            self.stats.failure_count += 1
            self.stats.total_failures += 1
            self.stats.total_requests += 1
            self.stats.last_failure_time = current_time
            self._last_error = error

            # Track recent failures
            self.stats.recent_failures.append(current_time)
            self._clean_recent_failures(current_time)

            self.logger.warning(
                "Failure recorded",
                state=self.stats.state.value,
                error=str(error),
                failure_count=self.stats.failure_count,
                total_requests=self.stats.total_requests,
            )

            # Check if we should open the circuit
            if self.stats.state == CircuitState.CLOSED:
                await self._check_failure_thresholds()
            elif self.stats.state == CircuitState.HALF_OPEN:
                # Any failure in half-open goes back to open
                await self._transition_to_open()

    def _clean_recent_failures(self, current_time: float) -> None:
        """Remove failures outside the monitoring window"""
        cutoff_time = current_time - self.config.monitor_window
        while self.stats.recent_failures and self.stats.recent_failures[0] < cutoff_time:
            self.stats.recent_failures.popleft()

    async def _check_failure_thresholds(self) -> None:
        """Check if failure thresholds are exceeded"""

        # Check consecutive failures threshold
        if self.stats.failure_count >= self.config.failure_threshold:
            await self._transition_to_open()
            return

        # Check failure rate in monitoring window
        recent_failure_count = len(self.stats.recent_failures)
        if recent_failure_count > 0:
            # Count all requests in the window (approximate)
            window_requests = max(recent_failure_count, 1)  # At minimum, we have the failures
            failure_rate = recent_failure_count / window_requests

            if failure_rate >= self.config.failure_rate_threshold:
                self.logger.warning(
                    "Failure rate threshold exceeded",
                    failure_rate=failure_rate,
                    threshold=self.config.failure_rate_threshold,
                    recent_failures=recent_failure_count,
                )
                await self._transition_to_open()

    async def _transition_to_open(self) -> None:
        """Transition to OPEN state"""
        old_state = self.stats.state
        self.stats.state = CircuitState.OPEN
        self.stats.state_change_count += 1

        self.logger.error(
            "Circuit breaker opened",
            name=self.name,
            failure_count=self.stats.failure_count,
            last_error=str(self._last_error) if self._last_error else None,
        )

        if self.on_state_change:
            try:
                self.on_state_change(old_state, CircuitState.OPEN)
            except Exception as e:
                self.logger.error("Error in state change callback", error=e)

    async def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state"""
        old_state = self.stats.state
        self.stats.state = CircuitState.HALF_OPEN
        self.stats.success_count = 0  # Reset success count for half-open trial
        self.stats.state_change_count += 1

        self.logger.info("Circuit breaker half-opened", name=self.name, recovery_attempt=True)

        if self.on_state_change:
            try:
                self.on_state_change(old_state, CircuitState.HALF_OPEN)
            except Exception as e:
                self.logger.error("Error in state change callback", error=e)

    async def _transition_to_closed(self) -> None:
        """Transition to CLOSED state"""
        old_state = self.stats.state
        self.stats.state = CircuitState.CLOSED
        self.stats.failure_count = 0  # Reset failure count
        self.stats.success_count = 0
        self.stats.state_change_count += 1

        self.logger.info("Circuit breaker closed", name=self.name, recovery_successful=True)

        if self.on_state_change:
            try:
                self.on_state_change(old_state, CircuitState.CLOSED)
            except Exception as e:
                self.logger.error("Error in state change callback", error=e)

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        current_time = time.time()
        self._clean_recent_failures(current_time)

        return {
            "name": self.name,
            "state": self.stats.state.value,
            "failure_count": self.stats.failure_count,
            "success_count": self.stats.success_count,
            "total_requests": self.stats.total_requests,
            "total_failures": self.stats.total_failures,
            "total_successes": self.stats.total_successes,
            "state_change_count": self.stats.state_change_count,
            "recent_failures": len(self.stats.recent_failures),
            "failure_rate": len(self.stats.recent_failures) / max(self.stats.total_requests, 1),
            "last_failure_time": self.stats.last_failure_time,
            "time_since_last_failure": (
                current_time - self.stats.last_failure_time
                if self.stats.last_failure_time
                else None
            ),
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
                "success_threshold": self.config.success_threshold,
                "monitor_window": self.config.monitor_window,
                "failure_rate_threshold": self.config.failure_rate_threshold,
            },
        }

    async def reset(self) -> None:
        """Reset circuit breaker to CLOSED state"""
        async with self._lock:
            old_state = self.stats.state
            self.stats = CircuitBreakerStats()  # Reset all stats
            self._last_error = None

            self.logger.info("Circuit breaker reset", name=self.name)

            if self.on_state_change and old_state != CircuitState.CLOSED:
                try:
                    self.on_state_change(old_state, CircuitState.CLOSED)
                except Exception as e:
                    self.logger.error("Error in state change callback", error=e)

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker(name={self.name}, "
            f"state={self.stats.state.value}, "
            f"failures={self.stats.failure_count})"
        )
