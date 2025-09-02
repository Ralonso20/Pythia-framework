"""
Built-in Prometheus metrics integration for Pythia workers
"""

import time
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
import asyncio

try:
    from prometheus_client import (
        Counter,
        Histogram,
        Gauge,
        Info,
        CollectorRegistry,
        generate_latest,
        CONTENT_TYPE_LATEST,
        start_http_server,
        push_to_gateway,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

    # Mock classes for when prometheus_client is not available
    class Counter:
        def __init__(self, *args, **kwargs):
            pass

        def inc(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

    class Histogram:
        def __init__(self, *args, **kwargs):
            pass

        def observe(self, *args, **kwargs):
            pass

        def time(self):
            return lambda f: f

        def labels(self, *args, **kwargs):
            return self

    class Gauge:
        def __init__(self, *args, **kwargs):
            pass

        def set(self, *args, **kwargs):
            pass

        def inc(self, *args, **kwargs):
            pass

        def dec(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

    class Info:
        def __init__(self, *args, **kwargs):
            pass

        def info(self, *args, **kwargs):
            pass

    class CollectorRegistry:
        def __init__(self, *args, **kwargs):
            pass

    def generate_latest(*args, **kwargs):
        return b""

    def start_http_server(*args, **kwargs):
        pass

    CONTENT_TYPE_LATEST = "text/plain"

from pythia.logging import get_pythia_logger


class MetricsMode(Enum):
    """Metrics collection modes"""

    DISABLED = "disabled"
    HTTP_SERVER = "http_server"  # Prometheus scraping via HTTP endpoint
    PUSH_GATEWAY = "push_gateway"  # Push metrics to Prometheus Gateway
    CUSTOM = "custom"  # Custom metrics handler


@dataclass
class MetricsConfig:
    """Configuration for metrics collection"""

    enabled: bool = True
    mode: MetricsMode = MetricsMode.HTTP_SERVER

    # HTTP server mode
    http_port: int = 9090
    http_host: str = "0.0.0.0"

    # Push gateway mode
    gateway_url: Optional[str] = None
    job_name: str = "pythia_worker"
    push_interval: float = 30.0  # seconds

    # General settings
    worker_name: str = "pythia_worker"
    include_worker_labels: bool = True
    custom_labels: Dict[str, str] = field(default_factory=dict)

    # Metric collection settings
    collect_system_metrics: bool = True
    collect_broker_metrics: bool = True
    collect_processing_metrics: bool = True
    collect_error_metrics: bool = True


class PythiaMetrics:
    """
    Built-in Prometheus metrics for Pythia workers

    Provides comprehensive metrics collection for:
    - Message processing performance
    - Broker health and throughput
    - Error rates and types
    - System resource usage
    - Custom application metrics

    Example:
        config = MetricsConfig(
            mode=MetricsMode.HTTP_SERVER,
            http_port=9090,
            worker_name="my-worker"
        )

        metrics = PythiaMetrics(config)
        metrics.start()

        # Record metrics
        with metrics.message_processing_time.time():
            # Process message
            pass

        metrics.messages_processed.inc()
    """

    def __init__(self, config: Optional[MetricsConfig] = None):
        self.config = config or MetricsConfig()
        self.logger = get_pythia_logger("PythiaMetrics")

        if not PROMETHEUS_AVAILABLE and self.config.enabled:
            self.logger.warning(
                "Prometheus client not available. Install with: pip install prometheus-client"
            )
            self.config.enabled = False

        # Metrics registry
        self.registry = CollectorRegistry() if self.config.enabled else None

        # Core metrics
        self._init_core_metrics()

        # Background tasks
        self._push_task: Optional[asyncio.Task] = None
        self._http_server_started = False

        # State tracking
        self._start_time = time.time()
        self._last_push = 0.0

        self.logger.info(
            "Metrics initialized",
            enabled=self.config.enabled,
            mode=self.config.mode.value if self.config.enabled else "disabled",
            worker=self.config.worker_name,
        )

    def _init_core_metrics(self) -> None:
        """Initialize core Prometheus metrics"""
        if not self.config.enabled:
            return

        base_labels = ["worker", "instance"]
        if self.config.include_worker_labels:
            base_labels.extend(self.config.custom_labels.keys())

        # Message processing metrics
        if self.config.collect_processing_metrics:
            self.messages_processed = Counter(
                "pythia_messages_processed_total",
                "Total number of messages processed",
                base_labels + ["source", "status"],
                registry=self.registry,
            )

            self.message_processing_time = Histogram(
                "pythia_message_processing_seconds",
                "Time spent processing messages",
                base_labels + ["source"],
                registry=self.registry,
                buckets=(
                    0.005,
                    0.01,
                    0.025,
                    0.05,
                    0.075,
                    0.1,
                    0.25,
                    0.5,
                    0.75,
                    1.0,
                    2.5,
                    5.0,
                    7.5,
                    10.0,
                ),
            )

            self.message_size = Histogram(
                "pythia_message_size_bytes",
                "Size of processed messages in bytes",
                base_labels + ["source"],
                registry=self.registry,
                buckets=(100, 1000, 10000, 100000, 1000000, 10000000),
            )

            self.active_messages = Gauge(
                "pythia_active_messages",
                "Number of messages currently being processed",
                base_labels,
                registry=self.registry,
            )

        # Broker metrics
        if self.config.collect_broker_metrics:
            self.broker_connections = Gauge(
                "pythia_broker_connections",
                "Number of active broker connections",
                base_labels + ["broker_type", "broker_host"],
                registry=self.registry,
            )

            self.broker_messages = Counter(
                "pythia_broker_messages_total",
                "Total messages sent/received from brokers",
                base_labels + ["broker_type", "broker_host", "direction"],
                registry=self.registry,
            )

            self.broker_errors = Counter(
                "pythia_broker_errors_total",
                "Total broker errors",
                base_labels + ["broker_type", "broker_host", "error_type"],
                registry=self.registry,
            )

        # Error metrics
        if self.config.collect_error_metrics:
            self.processing_errors = Counter(
                "pythia_processing_errors_total",
                "Total processing errors",
                base_labels + ["source", "error_type"],
                registry=self.registry,
            )

            self.retry_attempts = Counter(
                "pythia_retry_attempts_total",
                "Total retry attempts",
                base_labels + ["source", "retry_reason"],
                registry=self.registry,
            )

        # System metrics
        if self.config.collect_system_metrics:
            self.worker_uptime = Gauge(
                "pythia_worker_uptime_seconds",
                "Worker uptime in seconds",
                base_labels,
                registry=self.registry,
            )

            self.worker_info = Info(
                "pythia_worker_info",
                "Worker information",
                base_labels,
                registry=self.registry,
            )

            self.memory_usage = Gauge(
                "pythia_memory_usage_bytes",
                "Memory usage in bytes",
                base_labels,
                registry=self.registry,
            )

        # Multi-source specific metrics
        self.source_status = Gauge(
            "pythia_source_status",
            "Status of message sources (1=active, 0=inactive)",
            base_labels + ["source_name"],
            registry=self.registry,
        )

        self.routing_decisions = Counter(
            "pythia_routing_decisions_total",
            "Total routing decisions made",
            base_labels + ["source_name", "sink_name", "routing_rule"],
            registry=self.registry,
        )

    def start(self) -> None:
        """Start metrics collection"""
        if not self.config.enabled:
            return

        try:
            if self.config.mode == MetricsMode.HTTP_SERVER:
                self._start_http_server()
            elif self.config.mode == MetricsMode.PUSH_GATEWAY:
                self._start_push_gateway()

            # Set initial worker info
            if hasattr(self, "worker_info"):
                info_dict = {
                    "version": "0.1.0",
                    "mode": self.config.mode.value,
                    **self.config.custom_labels,
                }
                self.worker_info.info(info_dict)

            self.logger.info(
                "Metrics collection started",
                mode=self.config.mode.value,
                port=self.config.http_port if self.config.mode == MetricsMode.HTTP_SERVER else None,
            )

        except Exception as e:
            self.logger.error("Failed to start metrics collection", error=str(e))

    def stop(self) -> None:
        """Stop metrics collection"""
        if self._push_task and not self._push_task.done():
            self._push_task.cancel()

        self.logger.info("Metrics collection stopped")

    def _start_http_server(self) -> None:
        """Start HTTP server for Prometheus scraping"""
        if self._http_server_started:
            return

        try:
            start_http_server(self.config.http_port, self.config.http_host, registry=self.registry)
            self._http_server_started = True

            self.logger.info(
                "Metrics HTTP server started",
                host=self.config.http_host,
                port=self.config.http_port,
                endpoint=f"http://{self.config.http_host}:{self.config.http_port}/metrics",
            )

        except Exception as e:
            self.logger.error("Failed to start metrics HTTP server", error=str(e))
            raise

    def _start_push_gateway(self) -> None:
        """Start pushing metrics to Prometheus Gateway"""
        if not self.config.gateway_url:
            raise ValueError("gateway_url is required for push gateway mode")

        # Start background push task
        self._push_task = asyncio.create_task(self._push_metrics_loop())

    async def _push_metrics_loop(self) -> None:
        """Background task to push metrics to gateway"""
        while True:
            try:
                await asyncio.sleep(self.config.push_interval)

                if not PROMETHEUS_AVAILABLE:
                    continue

                # Push metrics to gateway
                push_to_gateway(
                    self.config.gateway_url,
                    job=self.config.job_name,
                    registry=self.registry,
                    grouping_key={"instance": self.config.worker_name},
                )

                self._last_push = time.time()

                self.logger.debug(
                    "Pushed metrics to gateway",
                    gateway_url=self.config.gateway_url,
                    job=self.config.job_name,
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error pushing metrics to gateway", error=str(e))

    def get_base_labels(self, **extra_labels) -> Dict[str, str]:
        """Get base labels for metrics"""
        labels = {
            "worker": self.config.worker_name,
            "instance": self.config.worker_name,
            **self.config.custom_labels,
            **extra_labels,
        }
        return labels

    def record_message_processed(
        self,
        source: str = "unknown",
        status: str = "success",
        processing_time: Optional[float] = None,
        message_size: Optional[int] = None,
    ) -> None:
        """Record message processing metrics"""
        if not self.config.enabled or not self.config.collect_processing_metrics:
            return

        labels = self.get_base_labels(source=source)

        # Increment processed counter
        self.messages_processed.labels(**labels, status=status).inc()

        # Record processing time
        if processing_time is not None:
            self.message_processing_time.labels(**labels).observe(processing_time)

        # Record message size
        if message_size is not None:
            self.message_size.labels(**labels).observe(message_size)

    def record_broker_activity(
        self,
        broker_type: str,
        broker_host: str,
        direction: str,  # "in" or "out"
        count: int = 1,
    ) -> None:
        """Record broker activity metrics"""
        if not self.config.enabled or not self.config.collect_broker_metrics:
            return

        labels = self.get_base_labels(
            broker_type=broker_type, broker_host=broker_host, direction=direction
        )

        self.broker_messages.labels(**labels).inc(count)

    def record_broker_error(self, broker_type: str, broker_host: str, error_type: str) -> None:
        """Record broker error metrics"""
        if not self.config.enabled or not self.config.collect_broker_metrics:
            return

        labels = self.get_base_labels(
            broker_type=broker_type, broker_host=broker_host, error_type=error_type
        )

        self.broker_errors.labels(**labels).inc()

    def record_processing_error(self, source: str, error_type: str) -> None:
        """Record processing error metrics"""
        if not self.config.enabled or not self.config.collect_error_metrics:
            return

        labels = self.get_base_labels(source=source, error_type=error_type)
        self.processing_errors.labels(**labels).inc()

    def record_routing_decision(
        self, source_name: str, sink_name: str, routing_rule: str = "default"
    ) -> None:
        """Record routing decision metrics"""
        if not self.config.enabled:
            return

        labels = self.get_base_labels(
            source_name=source_name, sink_name=sink_name, routing_rule=routing_rule
        )

        self.routing_decisions.labels(**labels).inc()

    def set_source_status(self, source_name: str, active: bool) -> None:
        """Set source status metric"""
        if not self.config.enabled:
            return

        labels = self.get_base_labels(source_name=source_name)
        self.source_status.labels(**labels).set(1 if active else 0)

    def update_system_metrics(self) -> None:
        """Update system metrics"""
        if not self.config.enabled or not self.config.collect_system_metrics:
            return

        try:
            import psutil
            import os

            # Update uptime
            uptime = time.time() - self._start_time
            self.worker_uptime.labels(**self.get_base_labels()).set(uptime)

            # Update memory usage
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            self.memory_usage.labels(**self.get_base_labels()).set(memory_info.rss)

        except ImportError:
            # psutil not available
            uptime = time.time() - self._start_time
            self.worker_uptime.labels(**self.get_base_labels()).set(uptime)
        except Exception as e:
            self.logger.warning("Error updating system metrics", error=str(e))

    def get_metrics_text(self) -> bytes:
        """Get metrics in Prometheus text format"""
        if not self.config.enabled:
            return b""

        return generate_latest(self.registry)

    def get_metrics_content_type(self) -> str:
        """Get metrics content type"""
        return CONTENT_TYPE_LATEST

    def create_custom_counter(
        self, name: str, description: str, labels: Optional[List[str]] = None
    ) -> Counter:
        """Create custom counter metric"""
        if not self.config.enabled:
            return Counter(name, description, labels or [])

        all_labels = ["worker", "instance"] + (labels or [])
        return Counter(name, description, all_labels, registry=self.registry)

    def create_custom_gauge(
        self, name: str, description: str, labels: Optional[List[str]] = None
    ) -> Gauge:
        """Create custom gauge metric"""
        if not self.config.enabled:
            return Gauge(name, description, labels or [])

        all_labels = ["worker", "instance"] + (labels or [])
        return Gauge(name, description, all_labels, registry=self.registry)

    def create_custom_histogram(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        buckets: Optional[tuple] = None,
    ) -> Histogram:
        """Create custom histogram metric"""
        if not self.config.enabled:
            return Histogram(name, description, labels or [])

        all_labels = ["worker", "instance"] + (labels or [])
        kwargs = {"registry": self.registry}
        if buckets:
            kwargs["buckets"] = buckets

        return Histogram(name, description, all_labels, **kwargs)

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()


# Decorators for automatic metrics collection


def measure_processing_time(metrics: PythiaMetrics, source: str = "unknown"):
    """Decorator to measure processing time"""

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                processing_time = time.time() - start_time
                metrics.record_message_processed(
                    source=source, status="success", processing_time=processing_time
                )
                return result
            except Exception as e:
                processing_time = time.time() - start_time
                metrics.record_message_processed(
                    source=source, status="error", processing_time=processing_time
                )
                metrics.record_processing_error(source, type(e).__name__)
                raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                processing_time = time.time() - start_time
                metrics.record_message_processed(
                    source=source, status="success", processing_time=processing_time
                )
                return result
            except Exception as e:
                processing_time = time.time() - start_time
                metrics.record_message_processed(
                    source=source, status="error", processing_time=processing_time
                )
                metrics.record_processing_error(source, type(e).__name__)
                raise

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


def count_calls(metrics: PythiaMetrics, counter_name: str, labels: Optional[Dict[str, str]] = None):
    """Decorator to count function calls"""

    def decorator(func):
        # Create custom counter
        counter = metrics.create_custom_counter(
            counter_name,
            f"Total calls to {func.__name__}",
            list(labels.keys()) if labels else [],
        )

        @wraps(func)
        def wrapper(*args, **kwargs):
            base_labels = metrics.get_base_labels(**(labels or {}))
            counter.labels(**base_labels).inc()
            return func(*args, **kwargs)

        return wrapper

    return decorator
