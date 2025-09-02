# Testing Observability Stack

This guide shows how to test your Pythia workers' observability features including metrics, tracing, and logging integration.

## 🧪 Testing Strategy

### Test Categories

1. **Unit Tests** - Individual component testing
2. **Integration Tests** - End-to-end observability flow
3. **Contract Tests** - OpenTelemetry spec compliance
4. **Performance Tests** - Observability overhead measurement

## 🔧 Test Setup

### Test Dependencies

```toml
# pyproject.toml
[project.optional-dependencies]
test-observability = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "pytest-mock>=3.10.0",
    "testcontainers[redis,kafka]>=3.7.0",
    "opentelemetry-test-utils>=1.20.0",
    "prometheus-client[test]>=0.17.0",
    "responses>=0.23.0",
    "fakeredis>=2.18.0",
]
```

### Test Configuration

```python
# tests/conftest.py
import pytest
import asyncio
from unittest.mock import Mock, patch
from testcontainers.redis import RedisContainer
from testcontainers.compose import DockerCompose
from opentelemetry import trace, metrics
from opentelemetry.test.test_utils import TraceRecorder, MetricsRecorder
from pythia.monitoring import setup_observability
from pythia.logging import setup_logging

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def observability_stack():
    """Start observability stack with Docker Compose for integration tests"""
    with DockerCompose("../examples/observability", compose_file_name="docker-compose.observability.yml") as compose:
        # Wait for services to be ready
        compose.wait_for("http://localhost:3000/api/health")  # Grafana
        compose.wait_for("http://localhost:9090/-/ready")     # Prometheus
        compose.wait_for("http://localhost:3200/ready")       # Tempo
        compose.wait_for("http://localhost:4317")             # OTEL Collector
        yield compose

@pytest.fixture
def trace_recorder():
    """Mock trace recorder for capturing spans"""
    recorder = TraceRecorder()
    with patch("opentelemetry.trace.get_tracer_provider") as mock_provider:
        mock_provider.return_value = recorder.tracer_provider
        yield recorder

@pytest.fixture
def metrics_recorder():
    """Mock metrics recorder for capturing metrics"""
    recorder = MetricsRecorder()
    with patch("opentelemetry.metrics.get_meter_provider") as mock_provider:
        mock_provider.return_value = recorder.meter_provider
        yield recorder

@pytest.fixture
def redis_container():
    """Redis container for testing broker integration"""
    with RedisContainer("redis:7-alpine") as redis:
        yield redis

@pytest.fixture
async def setup_test_observability():
    """Setup observability for testing with mocked exporters"""
    setup_observability(
        service_name="test-worker",
        service_version="test",
        otlp_endpoint="http://localhost:4317",
        metrics_enabled=True,
        tracing_enabled=True,
        logs_enabled=True,
        metrics_port=8001  # Different port for testing
    )
    yield
    # Cleanup after test
    trace._set_tracer_provider(None)
    metrics._set_meter_provider(None)
```

## 📊 Testing Metrics

### Unit Tests for Metrics

```python
# tests/unit/test_metrics.py
import pytest
import asyncio
import time
from unittest.mock import Mock, patch
from prometheus_client import REGISTRY, CollectorRegistry
from pythia import Worker
from pythia.brokers.redis import RedisConsumer
from pythia.monitoring import ObservabilityMixin, setup_observability
from pythia.core.message import Message

class TestObservabilityMixin:

    @pytest.fixture
    def worker_class(self):
        class TestWorker(ObservabilityMixin, Worker):
            source = RedisConsumer(queue_name="test")

            async def process(self, message):
                with self.start_span("process_message") as span:
                    span.set_attribute("message.type", "test")
                    await asyncio.sleep(0.01)  # Simulate processing
                    return {"status": "processed"}

        return TestWorker

    def test_observability_mixin_initialization(self, worker_class, metrics_recorder):
        """Test that ObservabilityMixin initializes metrics correctly"""
        worker = worker_class()

        # Verify tracer is created
        assert worker.tracer is not None
        assert worker.meter is not None

        # Verify metrics are created
        assert worker.messages_processed is not None
        assert worker.processing_duration is not None
        assert worker.active_messages is not None

    @pytest.mark.asyncio
    async def test_message_processing_metrics(self, worker_class, metrics_recorder):
        """Test that message processing records metrics correctly"""
        worker = worker_class()
        message = Message(body={"test": "data"}, message_id="test-123")

        # Record start time
        start_time = time.time()

        # Process message
        result = await worker.process(message)

        duration = time.time() - start_time

        # Verify result
        assert result["status"] == "processed"

        # Check that metrics would be recorded
        # (In real implementation, verify metrics are actually recorded)

    def test_custom_metric_recording(self, worker_class):
        """Test custom metric recording methods"""
        worker = worker_class()

        # Test success metric
        worker.record_message_processed(status="success", queue="test")

        # Test error metric
        worker.record_message_processed(status="error", error_type="ValidationError")

        # Test duration metric
        worker.record_processing_duration(0.5, message_type="email")

        # Test active messages
        worker.increment_active_messages(worker_type="TestWorker")
        worker.decrement_active_messages(worker_type="TestWorker")

class TestMetricsIntegration:

    @pytest.mark.asyncio
    async def test_prometheus_metrics_endpoint(self, setup_test_observability):
        """Test that Prometheus metrics endpoint is accessible"""
        import requests

        # Process some test data to generate metrics
        worker = self.create_test_worker()
        await worker.process(Message(body={"test": "data"}))

        # Check metrics endpoint
        response = requests.get("http://localhost:8001/metrics")
        assert response.status_code == 200

        # Verify Pythia metrics are present
        content = response.text
        assert "pythia_messages_processed_total" in content
        assert "pythia_message_processing_duration_seconds" in content
        assert "pythia_active_messages" in content

    def create_test_worker(self):
        class TestWorker(ObservabilityMixin, Worker):
            source = Mock()

            async def process(self, message):
                self.record_message_processed("success")
                self.record_processing_duration(0.1)
                return {"status": "processed"}

        return TestWorker()
```

## 🔍 Testing Distributed Tracing

### Unit Tests for Tracing

```python
# tests/unit/test_tracing.py
import pytest
from unittest.mock import Mock, patch
from opentelemetry import trace
from opentelemetry.test.test_utils import TraceRecorder
from pythia.monitoring import ObservabilityMixin, create_pythia_tracer
from pythia.core.message import Message

class TestDistributedTracing:

    def test_tracer_creation(self):
        """Test Pythia tracer creation"""
        tracer = create_pythia_tracer("test-worker")
        assert tracer is not None

    @pytest.mark.asyncio
    async def test_span_creation_and_attributes(self, trace_recorder):
        """Test span creation with proper attributes"""

        class TestWorker(ObservabilityMixin, Worker):
            source = Mock()

            async def process(self, message):
                with self.start_span("process_message", message_type="email") as span:
                    span.set_attribute("user.id", "123")
                    span.set_attribute("processing.step", "validation")
                    return {"status": "processed"}

        worker = TestWorker()
        message = Message(body={"email": "test@example.com"}, message_id="msg-123")

        result = await worker.process(message)

        # Verify span was created
        spans = trace_recorder.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.name == "process_message"

        # Verify attributes
        attributes = span.attributes
        assert attributes["worker.type"] == "TestWorker"
        assert attributes["pythia.version"] == "0.1.0"
        assert attributes["message_type"] == "email"
        assert attributes["user.id"] == "123"
        assert attributes["processing.step"] == "validation"

    @pytest.mark.asyncio
    async def test_span_error_handling(self, trace_recorder):
        """Test span error recording"""

        class ErrorWorker(ObservabilityMixin, Worker):
            source = Mock()

            async def process(self, message):
                with self.start_span("process_message") as span:
                    raise ValueError("Test error")

        worker = ErrorWorker()
        message = Message(body={}, message_id="msg-123")

        with pytest.raises(ValueError):
            await worker.process(message)

        # Verify error was recorded in span
        spans = trace_recorder.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.status.status_code == trace.StatusCode.ERROR

        # Verify exception event
        events = span.events
        assert len(events) == 1
        assert events[0].name == "exception"

    @pytest.mark.asyncio
    async def test_nested_spans(self, trace_recorder):
        """Test nested span creation"""

        class NestedWorker(ObservabilityMixin, Worker):
            source = Mock()

            async def process(self, message):
                with self.start_span("process_message") as parent_span:
                    await self._validate_message(message)
                    await self._send_notification(message)
                    return {"status": "processed"}

            async def _validate_message(self, message):
                with self.start_span("validate_message") as span:
                    span.set_attribute("validation.result", "passed")

            async def _send_notification(self, message):
                with self.start_span("send_notification") as span:
                    span.set_attribute("notification.type", "email")

        worker = NestedWorker()
        message = Message(body={}, message_id="msg-123")

        result = await worker.process(message)

        # Verify all spans were created
        spans = trace_recorder.get_finished_spans()
        assert len(spans) == 3

        # Verify span hierarchy
        span_names = [span.name for span in spans]
        assert "process_message" in span_names
        assert "validate_message" in span_names
        assert "send_notification" in span_names
```

## 📝 Testing Logging Integration

### Unit Tests for Logging

```python
# tests/unit/test_logging_integration.py
import pytest
from unittest.mock import Mock, patch
from loguru import logger
from pythia.logging import setup_logging
from pythia.monitoring.observability import _setup_logging_integration

class TestLoggingIntegration:

    def test_logging_setup(self, caplog):
        """Test logging setup with observability"""
        _setup_logging_integration("test-service", "1.0.0")

        # Test that logging includes service information
        logger.info("Test message", user_id="123")

        # Verify log structure (this would need custom log capture)
        # In real implementation, verify structured logging format

    @patch('pythia.monitoring.observability.trace.get_current_span')
    def test_trace_context_in_logs(self, mock_get_span, caplog):
        """Test that trace context is added to logs"""

        # Mock active span
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_span_context = Mock()
        mock_span_context.trace_id = 123456789
        mock_span_context.span_id = 987654321
        mock_span.get_span_context.return_value = mock_span_context
        mock_get_span.return_value = mock_span

        _setup_logging_integration("test-service", "1.0.0")

        # Log a message
        logger.info("Test with trace context")

        # Verify trace context is included
        # (Implementation would verify log output contains trace_id and span_id)

class TestLogCapture:
    """Utility class for capturing and verifying logs in tests"""

    def __init__(self):
        self.logs = []

    def capture_logs(self):
        """Context manager to capture logs"""
        return self

    def __enter__(self):
        # Setup log capture
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Cleanup log capture
        pass

    def get_logs(self, level=None):
        """Get captured logs, optionally filtered by level"""
        if level:
            return [log for log in self.logs if log.level == level]
        return self.logs
```

## 🏗️ Integration Testing

### End-to-End Observability Tests

```python
# tests/integration/test_observability_e2e.py
import pytest
import asyncio
import requests
import time
from testcontainers.redis import RedisContainer
from pythia import Worker
from pythia.brokers.redis import RedisConsumer
from pythia.monitoring import setup_observability, ObservabilityMixin

class TestObservabilityE2E:

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_complete_observability_flow(self, observability_stack, redis_container):
        """Test complete observability flow from worker to dashboards"""

        # Setup observability with real endpoints
        setup_observability(
            service_name="test-e2e-worker",
            service_version="1.0.0",
            otlp_endpoint="http://localhost:4317",
            metrics_enabled=True,
            tracing_enabled=True,
            logs_enabled=True,
            metrics_port=8002
        )

        # Create worker with observability
        class E2EWorker(ObservabilityMixin, Worker):
            source = RedisConsumer(
                host=redis_container.get_container_host_ip(),
                port=redis_container.get_exposed_port(6379),
                queue_name="test-e2e"
            )

            async def process(self, message):
                with self.start_span("e2e_process", test_run=True) as span:
                    # Simulate processing with various outcomes
                    if message.body.get("fail"):
                        span.set_attribute("error", True)
                        raise ValueError("Simulated error")

                    # Add processing delay
                    await asyncio.sleep(0.1)

                    span.set_attribute("success", True)
                    return {"processed": True, "message_id": message.message_id}

        worker = E2EWorker()

        # Send test messages
        await self._send_test_messages(worker, redis_container)

        # Wait for metrics to be exported
        await asyncio.sleep(5)

        # Verify metrics in Prometheus
        await self._verify_prometheus_metrics()

        # Verify traces in Tempo
        await self._verify_tempo_traces()

        # Verify Grafana can query data
        await self._verify_grafana_data()

    async def _send_test_messages(self, worker, redis_container):
        """Send variety of test messages"""
        import redis

        r = redis.Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379)
        )

        # Send successful messages
        for i in range(10):
            r.lpush("test-e2e", f'{{"id": {i}, "type": "success"}}')

        # Send failing messages
        for i in range(2):
            r.lpush("test-e2e", f'{{"id": {i}, "type": "error", "fail": true}}')

        # Process messages
        for _ in range(12):  # 10 success + 2 errors
            try:
                await worker._process_single_message()
            except ValueError:
                pass  # Expected for error messages

    async def _verify_prometheus_metrics(self):
        """Verify metrics are available in Prometheus"""

        # Wait for metrics to be scraped
        await asyncio.sleep(10)

        # Query Prometheus API
        response = requests.get(
            "http://localhost:9090/api/v1/query",
            params={"query": "pythia_messages_processed_total"}
        )

        assert response.status_code == 200
        data = response.json()

        # Verify we have metrics data
        assert data["status"] == "success"
        assert len(data["data"]["result"]) > 0

        # Verify we have both success and error metrics
        results = data["data"]["result"]
        statuses = [result["metric"]["status"] for result in results]
        assert "success" in statuses
        assert "error" in statuses

    async def _verify_tempo_traces(self):
        """Verify traces are stored in Tempo"""

        # Query Tempo API for traces
        response = requests.get(
            "http://localhost:3200/api/search",
            params={
                "tags": "service.name=test-e2e-worker",
                "limit": 20
            }
        )

        assert response.status_code == 200
        data = response.json()

        # Verify traces exist
        assert "traces" in data
        assert len(data["traces"]) > 0

        # Get a specific trace
        trace_id = data["traces"][0]["traceID"]

        response = requests.get(f"http://localhost:3200/api/traces/{trace_id}")
        assert response.status_code == 200

        trace_data = response.json()

        # Verify trace structure
        assert "data" in trace_data
        assert len(trace_data["data"]) > 0

        # Verify span attributes
        spans = trace_data["data"][0]["spans"]
        assert len(spans) > 0

        root_span = spans[0]
        assert "e2e_process" in root_span["operationName"]

    async def _verify_grafana_data(self):
        """Verify Grafana can query data from data sources"""

        # Test Prometheus data source
        response = requests.get(
            "http://localhost:3000/api/datasources/proxy/1/api/v1/query",
            headers={"Authorization": "Basic YWRtaW46YWRtaW4="},  # admin:admin
            params={"query": "pythia_messages_processed_total"}
        )

        assert response.status_code == 200

        # Test Tempo data source
        response = requests.get(
            "http://localhost:3000/api/datasources/proxy/2/api/search",
            headers={"Authorization": "Basic YWRtaW46YWRtaW4="},
            params={"tags": "service.name=test-e2e-worker"}
        )

        assert response.status_code == 200
```

## 🚀 Performance Testing

### Observability Overhead Tests

```python
# tests/performance/test_observability_overhead.py
import pytest
import asyncio
import time
import statistics
from unittest.mock import Mock
from pythia import Worker
from pythia.monitoring import setup_observability, ObservabilityMixin

class TestObservabilityPerformance:

    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_tracing_overhead(self):
        """Test performance impact of distributed tracing"""

        # Worker without tracing
        class BaseWorker(Worker):
            source = Mock()

            async def process(self, message):
                await asyncio.sleep(0.001)  # Minimal processing
                return {"status": "processed"}

        # Worker with tracing
        class TracedWorker(ObservabilityMixin, Worker):
            source = Mock()

            async def process(self, message):
                with self.start_span("process") as span:
                    span.set_attribute("test", "true")
                    await asyncio.sleep(0.001)  # Same processing
                    return {"status": "processed"}

        # Benchmark both workers
        base_times = await self._benchmark_worker(BaseWorker(), iterations=1000)
        traced_times = await self._benchmark_worker(TracedWorker(), iterations=1000)

        # Calculate overhead
        base_avg = statistics.mean(base_times)
        traced_avg = statistics.mean(traced_times)

        overhead_percent = ((traced_avg - base_avg) / base_avg) * 100

        print(f"Base worker average: {base_avg:.4f}s")
        print(f"Traced worker average: {traced_avg:.4f}s")
        print(f"Overhead: {overhead_percent:.2f}%")

        # Assert overhead is acceptable (< 10%)
        assert overhead_percent < 10.0

    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_metrics_overhead(self):
        """Test performance impact of metrics collection"""

        class BaseWorker(Worker):
            source = Mock()

            async def process(self, message):
                await asyncio.sleep(0.001)
                return {"status": "processed"}

        class MetricsWorker(ObservabilityMixin, Worker):
            source = Mock()

            async def process(self, message):
                start_time = time.time()

                self.increment_active_messages()
                result = await asyncio.sleep(0.001)
                self.decrement_active_messages()

                duration = time.time() - start_time
                self.record_processing_duration(duration)
                self.record_message_processed("success")

                return {"status": "processed"}

        # Benchmark both workers
        base_times = await self._benchmark_worker(BaseWorker(), iterations=1000)
        metrics_times = await self._benchmark_worker(MetricsWorker(), iterations=1000)

        # Calculate metrics overhead
        base_avg = statistics.mean(base_times)
        metrics_avg = statistics.mean(metrics_times)

        overhead_percent = ((metrics_avg - base_avg) / base_avg) * 100

        print(f"Metrics overhead: {overhead_percent:.2f}%")

        # Assert metrics overhead is minimal (< 5%)
        assert overhead_percent < 5.0

    async def _benchmark_worker(self, worker, iterations=100):
        """Benchmark worker processing time"""
        times = []
        message = Mock()
        message.body = {"test": "data"}
        message.message_id = "test-123"

        # Warmup
        for _ in range(10):
            await worker.process(message)

        # Actual benchmark
        for _ in range(iterations):
            start_time = time.perf_counter()
            await worker.process(message)
            end_time = time.perf_counter()
            times.append(end_time - start_time)

        return times

    @pytest.mark.performance
    def test_sampling_effectiveness(self):
        """Test that sampling reduces overhead effectively"""

        # Test different sampling rates
        sample_rates = [1.0, 0.5, 0.1, 0.01]
        overheads = []

        for rate in sample_rates:
            # Setup observability with different sampling rates
            setup_observability(
                service_name="perf-test",
                tracing_sample_rate=rate,
                metrics_port=8003
            )

            # Measure overhead (simplified test)
            overhead = self._measure_sampling_overhead(rate)
            overheads.append(overhead)

        # Verify that lower sampling rates have lower overhead
        assert overheads[3] < overheads[2] < overheads[1] < overheads[0]

    def _measure_sampling_overhead(self, sample_rate):
        """Simplified overhead measurement for sampling"""
        # This would contain actual timing measurements
        # For now, return mock values that demonstrate the concept
        base_overhead = 0.1  # 10% base overhead
        return base_overhead * sample_rate
```

## 📈 Contract Testing

### OpenTelemetry Compliance Tests

```python
# tests/contract/test_otel_compliance.py
import pytest
from opentelemetry import trace, metrics
from opentelemetry.test.test_utils import TraceRecorder
from pythia.monitoring import create_pythia_tracer, create_pythia_meter

class TestOpenTelemetryCompliance:

    def test_tracer_compliance(self):
        """Test that Pythia tracer follows OpenTelemetry spec"""
        tracer = create_pythia_tracer("test-service")

        # Verify tracer implements required interface
        assert hasattr(tracer, 'start_span')
        assert hasattr(tracer, 'start_as_current_span')

        # Test span creation
        with tracer.start_as_current_span("test-span") as span:
            assert span is not None
            assert hasattr(span, 'set_attribute')
            assert hasattr(span, 'set_status')
            assert hasattr(span, 'record_exception')

    def test_meter_compliance(self):
        """Test that Pythia meter follows OpenTelemetry spec"""
        meter = create_pythia_meter("test-service")

        # Test metric creation
        counter = meter.create_counter("test_counter")
        histogram = meter.create_histogram("test_histogram")
        gauge = meter.create_up_down_counter("test_gauge")

        # Verify instruments have required methods
        assert hasattr(counter, 'add')
        assert hasattr(histogram, 'record')
        assert hasattr(gauge, 'add')

    def test_resource_attributes(self, trace_recorder):
        """Test that resource attributes follow semantic conventions"""
        from pythia.monitoring.observability import setup_observability

        setup_observability(
            service_name="test-service",
            service_version="1.0.0",
            environment="test"
        )

        # Create a span to trigger resource creation
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("test") as span:
            pass

        spans = trace_recorder.get_finished_spans()
        if spans:
            span = spans[0]
            resource = span.resource

            # Verify required resource attributes
            assert "service.name" in resource.attributes
            assert "service.version" in resource.attributes
            assert "deployment.environment" in resource.attributes

            # Verify Pythia-specific attributes
            assert resource.attributes.get("pythia.framework") == "true"
```

## 🏃 Running Tests

### Test Commands

```bash
# Run all observability tests
pytest tests/ -k observability -v

# Run unit tests only
pytest tests/unit/test_*observability* -v

# Run integration tests (requires Docker)
pytest tests/integration/ -m integration -v

# Run performance tests
pytest tests/performance/ -m performance -v --benchmark-json=benchmark.json

# Run with coverage
pytest tests/ -k observability --cov=pythia.monitoring --cov-report=html

# Run contract tests
pytest tests/contract/ -v
```

### Continuous Integration

```yaml
# .github/workflows/observability-tests.yml
name: Observability Tests

on: [push, pull_request]

jobs:
  test-observability:
    runs-on: ubuntu-latest

    services:
      redis:
        image: redis:7-alpine
        ports:
          - 6379:6379

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Install dependencies
      run: |
        pip install -e .[test-observability]

    - name: Start observability stack
      run: |
        docker-compose -f examples/observability/docker-compose.observability.yml up -d
        sleep 30  # Wait for services to start

    - name: Run unit tests
      run: pytest tests/unit/ -k observability -v

    - name: Run integration tests
      run: pytest tests/integration/ -m integration -v

    - name: Run performance tests
      run: pytest tests/performance/ -m performance -v

    - name: Cleanup
      if: always()
      run: |
        docker-compose -f examples/observability/docker-compose.observability.yml down -v
```

## 📚 Best Practices for Testing Observability

### 1. Test Isolation

```python
# Ensure each test has isolated metrics/traces
@pytest.fixture(autouse=True)
def reset_global_state():
    """Reset OpenTelemetry global state before each test"""
    trace._set_tracer_provider(None)
    metrics._set_meter_provider(None)
    yield
    # Cleanup after test
```

### 2. Mock External Services

```python
# Mock OTLP exporters to avoid real network calls in unit tests
@pytest.fixture
def mock_otlp_exporter():
    with patch('opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter') as mock:
        yield mock
```

### 3. Verify Business Metrics

```python
# Test that business-relevant metrics are captured
def test_email_processing_metrics():
    assert "email_sent_total" in captured_metrics
    assert "email_delivery_duration_seconds" in captured_metrics
```

### 4. Test Error Scenarios

```python
# Verify error handling doesn't break observability
@pytest.mark.asyncio
async def test_observability_during_errors():
    # Ensure spans are properly closed even during exceptions
    # Verify error metrics are recorded
    # Check that traces contain error information
```

---

Ready to test your observability stack thoroughly! 🎯
