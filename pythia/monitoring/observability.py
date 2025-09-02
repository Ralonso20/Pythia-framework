"""
Complete observability setup for Pythia workers with OpenTelemetry integration
"""

import os
from typing import Optional, Dict, Any
from loguru import logger

# Try to import OpenTelemetry packages (optional dependencies)
try:
    from opentelemetry import trace, metrics as otel_metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.semconv.resource import ResourceAttributes

    OPENTELEMETRY_AVAILABLE = True
except ImportError as e:
    logger.warning("OpenTelemetry not available, observability features disabled", error=str(e))
    OPENTELEMETRY_AVAILABLE = False

    # Mock classes for when OpenTelemetry is not available
    class MockResource:
        def __init__(self, *args, **kwargs):
            pass

    class MockTracer:
        def start_span(self, name, **kwargs):
            return MockSpan()

    class MockSpan:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def set_attribute(self, key, value):
            pass

        def set_status(self, status):
            pass

        def record_exception(self, exception):
            pass

        def is_recording(self):
            return False

    class MockMeter:
        def create_counter(self, *args, **kwargs):
            return MockInstrument()

        def create_histogram(self, *args, **kwargs):
            return MockInstrument()

        def create_up_down_counter(self, *args, **kwargs):
            return MockInstrument()

    class MockInstrument:
        def add(self, *args, **kwargs):
            pass

        def record(self, *args, **kwargs):
            pass


try:
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    PROMETHEUS_EXPORTER_AVAILABLE = True
except ImportError:
    PROMETHEUS_EXPORTER_AVAILABLE = False

try:
    from prometheus_client import start_http_server

    PROMETHEUS_CLIENT_AVAILABLE = True
except ImportError:
    PROMETHEUS_CLIENT_AVAILABLE = False


def setup_observability(
    service_name: str,
    service_version: str = "1.0.0",
    otlp_endpoint: Optional[str] = None,
    metrics_enabled: bool = True,
    tracing_enabled: bool = True,
    logs_enabled: bool = True,
    metrics_port: int = 8000,
    tracing_sample_rate: float = 1.0,
    environment: Optional[str] = None,
    extra_attributes: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Setup complete observability stack for Pythia workers.

    Args:
        service_name: Name of the service
        service_version: Version of the service
        otlp_endpoint: OTLP endpoint URL (e.g., http://localhost:4317)
        metrics_enabled: Enable metrics collection
        tracing_enabled: Enable distributed tracing
        logs_enabled: Enable structured logging
        metrics_port: Port for Prometheus metrics endpoint
        tracing_sample_rate: Sampling rate for traces (0.0 to 1.0)
        environment: Environment name (dev, prod, etc.)
        extra_attributes: Additional resource attributes
    """

    if not OPENTELEMETRY_AVAILABLE:
        logger.warning("OpenTelemetry not available, observability features will be mocked")
        return

    # Get configuration from environment if not provided
    otlp_endpoint = otlp_endpoint or os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
    )
    environment = environment or os.getenv("ENVIRONMENT", "development")

    # Create resource with service information
    resource_attributes = {
        ResourceAttributes.SERVICE_NAME: service_name,
        ResourceAttributes.SERVICE_VERSION: service_version,
        ResourceAttributes.DEPLOYMENT_ENVIRONMENT: environment,
        "pythia.framework": "true",
        "pythia.version": "0.1.0",
    }

    if extra_attributes:
        resource_attributes.update(extra_attributes)

    resource = Resource.create(resource_attributes)

    # Setup tracing if enabled
    if tracing_enabled:
        _setup_tracing(resource, otlp_endpoint, tracing_sample_rate)
        logger.info(
            "OpenTelemetry tracing initialized",
            endpoint=otlp_endpoint,
            sample_rate=tracing_sample_rate,
        )

    # Setup metrics if enabled
    if metrics_enabled:
        _setup_metrics(resource, otlp_endpoint, metrics_port)
        logger.info(
            "OpenTelemetry metrics initialized",
            endpoint=otlp_endpoint,
            prometheus_port=metrics_port,
        )

    # Setup logging integration if enabled
    if logs_enabled:
        _setup_logging_integration(service_name, service_version)
        logger.info("Observability logging integration initialized")

    logger.success(
        "Complete observability stack initialized",
        service=service_name,
        version=service_version,
        tracing=tracing_enabled,
        metrics=metrics_enabled,
        logs=logs_enabled,
    )


def _setup_tracing(resource: Any, otlp_endpoint: str, sample_rate: float) -> None:
    """Setup distributed tracing with OpenTelemetry"""
    if not OPENTELEMETRY_AVAILABLE:
        return

    # Create tracer provider
    tracer_provider = TracerProvider(
        resource=resource,
        # Add sampling if needed
    )

    # Setup OTLP exporter
    otlp_exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=True,  # Use secure=True in production with proper certs
    )

    # Add span processor
    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

    # Set as global tracer provider
    trace.set_tracer_provider(tracer_provider)


def _setup_metrics(resource: Any, otlp_endpoint: str, prometheus_port: int) -> None:
    """Setup metrics collection with both OTLP and Prometheus"""
    if not OPENTELEMETRY_AVAILABLE:
        return

    readers = []

    # Setup Prometheus metrics reader if available
    if PROMETHEUS_EXPORTER_AVAILABLE:
        prometheus_reader = PrometheusMetricReader()
        readers.append(prometheus_reader)

    # Setup OTLP metrics reader
    otlp_exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
    otlp_reader = PeriodicExportingMetricReader(
        exporter=otlp_exporter,
        export_interval_millis=5000,  # Export every 5 seconds
    )
    readers.append(otlp_reader)

    # Create meter provider with available readers
    meter_provider = MeterProvider(resource=resource, metric_readers=readers)

    # Set as global meter provider
    otel_metrics.set_meter_provider(meter_provider)

    # Start Prometheus HTTP server if available
    if PROMETHEUS_CLIENT_AVAILABLE:
        try:
            start_http_server(prometheus_port)
        except Exception as e:
            logger.warning(
                "Failed to start Prometheus HTTP server", port=prometheus_port, error=str(e)
            )


def _setup_logging_integration(service_name: str, service_version: str) -> None:
    """Setup logging integration with OpenTelemetry context"""

    # Add OpenTelemetry context to logs
    def add_otel_context(record):
        # Add service information
        record["extra"]["service_name"] = service_name
        record["extra"]["service_version"] = service_version

        if OPENTELEMETRY_AVAILABLE:
            try:
                # Get current span context
                span = trace.get_current_span()
                if span and span.is_recording():
                    span_context = span.get_span_context()
                    record["extra"]["trace_id"] = format(span_context.trace_id, "032x")
                    record["extra"]["span_id"] = format(span_context.span_id, "016x")
            except Exception:
                # Silently skip if there's any issue getting span context
                pass

    # Configure logger with OpenTelemetry context
    logger.configure(processors=[add_otel_context])


def create_pythia_meter(name: str) -> Any:
    """
    Create a meter for Pythia worker metrics.

    Args:
        name: Name of the meter (usually the worker class name)

    Returns:
        OpenTelemetry Meter instance or mock if not available
    """
    if not OPENTELEMETRY_AVAILABLE:
        return MockMeter()

    return otel_metrics.get_meter(
        "pythia.worker", version="0.1.0", schema_url="https://opentelemetry.io/schemas/1.21.0"
    )


def create_pythia_tracer(name: str) -> Any:
    """
    Create a tracer for Pythia worker tracing.

    Args:
        name: Name of the tracer (usually the worker class name)

    Returns:
        OpenTelemetry Tracer instance or mock if not available
    """
    if not OPENTELEMETRY_AVAILABLE:
        return MockTracer()

    return trace.get_tracer(
        "pythia.worker", version="0.1.0", schema_url="https://opentelemetry.io/schemas/1.21.0"
    )


class ObservabilityMixin:
    """
    Mixin class to add observability to Pythia workers.

    Usage:
        class MyWorker(ObservabilityMixin, Worker):
            async def process(self, message):
                with self.start_span("process_message") as span:
                    span.set_attribute("message.type", "email")
                    # Process message...
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracer = create_pythia_tracer(self.__class__.__name__)
        self.meter = create_pythia_meter(self.__class__.__name__)

        # Create common metrics
        self.messages_processed = self.meter.create_counter(
            "pythia_messages_processed_total",
            description="Total number of messages processed",
            unit="1",
        )

        self.processing_duration = self.meter.create_histogram(
            "pythia_message_processing_duration_seconds",
            description="Message processing duration",
            unit="s",
        )

        self.active_messages = self.meter.create_up_down_counter(
            "pythia_active_messages",
            description="Number of messages currently being processed",
            unit="1",
        )

    def start_span(self, name: str, **attributes):
        """Start a new span with common attributes"""
        span = self.tracer.start_span(name)

        # Add common attributes
        span.set_attribute("worker.type", self.__class__.__name__)
        span.set_attribute("pythia.version", "0.1.0")

        # Add custom attributes
        for key, value in attributes.items():
            span.set_attribute(key, value)

        return span

    def record_message_processed(self, status: str = "success", **labels):
        """Record a processed message metric"""
        attributes = {"worker_type": self.__class__.__name__, "status": status, **labels}
        self.messages_processed.add(1, attributes)

    def record_processing_duration(self, duration_seconds: float, **labels):
        """Record message processing duration"""
        attributes = {"worker_type": self.__class__.__name__, **labels}
        self.processing_duration.record(duration_seconds, attributes)

    def increment_active_messages(self, **labels):
        """Increment active messages counter"""
        attributes = {"worker_type": self.__class__.__name__, **labels}
        self.active_messages.add(1, attributes)

    def decrement_active_messages(self, **labels):
        """Decrement active messages counter"""
        attributes = {"worker_type": self.__class__.__name__, **labels}
        self.active_messages.add(-1, attributes)


# Environment-based setup function
def setup_observability_from_env() -> None:
    """Setup observability using environment variables"""

    service_name = os.getenv("OTEL_SERVICE_NAME", "pythia-worker")
    service_version = os.getenv("OTEL_SERVICE_VERSION", "1.0.0")

    metrics_enabled = os.getenv("PYTHIA_METRICS_ENABLED", "true").lower() == "true"
    tracing_enabled = os.getenv("PYTHIA_TRACING_ENABLED", "true").lower() == "true"
    logs_enabled = os.getenv("PYTHIA_LOGS_ENABLED", "true").lower() == "true"

    metrics_port = int(os.getenv("PYTHIA_METRICS_PORT", "8000"))
    tracing_sample_rate = float(os.getenv("PYTHIA_TRACING_SAMPLE_RATE", "1.0"))

    setup_observability(
        service_name=service_name,
        service_version=service_version,
        metrics_enabled=metrics_enabled,
        tracing_enabled=tracing_enabled,
        logs_enabled=logs_enabled,
        metrics_port=metrics_port,
        tracing_sample_rate=tracing_sample_rate,
    )
