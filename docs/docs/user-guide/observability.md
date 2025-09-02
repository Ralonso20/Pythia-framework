# Observability with Grafana, Tempo & OpenTelemetry

Pythia provides comprehensive observability through metrics, logs, and distributed tracing using modern open-source tools: **Grafana**, **Tempo**, and **OpenTelemetry**.

## 🌟 Overview

The complete observability stack includes:

- **📊 Grafana** - Dashboards and alerting for metrics and logs
- **📈 Prometheus** - Metrics collection and storage
- **🔍 Tempo** - Distributed tracing storage and querying
- **🔄 OpenTelemetry** - Unified observability data collection
- **📝 Loki** - Log aggregation and querying (optional)

## 🚀 Quick Setup

### Docker Compose Stack

Create a complete observability stack with Docker:

```yaml
# docker-compose.observability.yml
version: '3.8'

services:
  # Grafana Dashboard
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_FEATURE_TOGGLES_ENABLE=traceqlEditor
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/var/lib/grafana/dashboards
    networks:
      - observability

  # Prometheus Metrics
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--storage.tsdb.retention.time=15d'
      - '--web.enable-lifecycle'
      - '--web.enable-admin-api'
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    networks:
      - observability

  # Tempo Tracing
  tempo:
    image: grafana/tempo:latest
    ports:
      - "3200:3200"   # Tempo
      - "14268:14268" # Jaeger ingest
      - "9411:9411"   # Zipkin
    command: [ "-config.file=/etc/tempo.yaml" ]
    volumes:
      - ./tempo/tempo.yaml:/etc/tempo.yaml
      - tempo-data:/tmp/tempo
    networks:
      - observability

  # Loki Logs (Optional)
  loki:
    image: grafana/loki:latest
    ports:
      - "3100:3100"
    command: -config.file=/etc/loki/local-config.yaml
    volumes:
      - loki-data:/loki
    networks:
      - observability

  # OpenTelemetry Collector
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel/otel-collector-config.yaml:/etc/otel-collector-config.yaml
    ports:
      - "4317:4317"   # OTLP gRPC receiver
      - "4318:4318"   # OTLP HTTP receiver
      - "8888:8888"   # Prometheus metrics
    networks:
      - observability
    depends_on:
      - tempo
      - prometheus

volumes:
  grafana-data:
  prometheus-data:
  tempo-data:
  loki-data:

networks:
  observability:
    driver: bridge
```

### Configuration Files

Create the necessary configuration files:

```bash
mkdir -p grafana/provisioning/{datasources,dashboards}
mkdir -p grafana/dashboards
mkdir -p prometheus
mkdir -p tempo
mkdir -p otel
```

#### Prometheus Configuration

```yaml
# prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'pythia-workers'
    static_configs:
      - targets: ['host.docker.internal:8000']  # Your worker metrics endpoint
    scrape_interval: 5s
    metrics_path: '/metrics'

  - job_name: 'otel-collector'
    static_configs:
      - targets: ['otel-collector:8888']

rule_files:
  - "pythia_alerts.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          # - alertmanager:9093
```

#### Tempo Configuration

```yaml
# tempo/tempo.yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    jaeger:
      protocols:
        thrift_http:
          endpoint: 0.0.0.0:14268
        grpc:
          endpoint: 0.0.0.0:14250
    zipkin:
      endpoint: 0.0.0.0:9411
    otlp:
      protocols:
        http:
          endpoint: 0.0.0.0:4318
        grpc:
          endpoint: 0.0.0.0:4317

ingester:
  trace_idle_period: 10s
  max_block_bytes: 1_000_000
  max_block_duration: 5m

compactor:
  compaction:
    compacted_block_retention: 1h

storage:
  trace:
    backend: local
    local:
      path: /tmp/tempo/traces
```

#### OpenTelemetry Collector Configuration

```yaml
# otel/otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

  prometheus:
    config:
      scrape_configs:
        - job_name: 'pythia-workers'
          static_configs:
            - targets: ['host.docker.internal:8000']

processors:
  batch:

exporters:
  # Send traces to Tempo
  otlp/tempo:
    endpoint: tempo:4317
    tls:
      insecure: true

  # Send metrics to Prometheus
  prometheus:
    endpoint: "0.0.0.0:8888"

  # Debug exporter for development
  logging:
    loglevel: debug

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/tempo, logging]

    metrics:
      receivers: [otlp, prometheus]
      processors: [batch]
      exporters: [prometheus, logging]
```

#### Grafana Data Sources

```yaml
# grafana/provisioning/datasources/datasources.yml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    access: proxy
    isDefault: true

  - name: Tempo
    type: tempo
    url: http://tempo:3200
    access: proxy

  - name: Loki
    type: loki
    url: http://loki:3100
    access: proxy
```

## 📊 Pythia Worker Integration

### Enable Metrics and Tracing

```python
from pythia import Worker
from pythia.brokers.redis import RedisConsumer
from pythia.monitoring import setup_observability
from opentelemetry import trace
from loguru import logger
import time

# Setup observability
setup_observability(
    service_name="email-worker",
    service_version="1.0.0",
    otlp_endpoint="http://localhost:4317",
    metrics_enabled=True,
    tracing_enabled=True,
    logs_enabled=True
)

tracer = trace.get_tracer(__name__)

class ObservableEmailWorker(Worker):
    source = RedisConsumer(queue_name="emails")

    async def process(self, message):
        with tracer.start_as_current_span("process_email") as span:
            # Add span attributes
            span.set_attribute("email.recipient", message.body.get("email"))
            span.set_attribute("worker.type", "email")

            try:
                start_time = time.time()

                # Process email
                result = await self._send_email(message.body)

                processing_time = time.time() - start_time
                span.set_attribute("processing.duration_ms", processing_time * 1000)
                span.set_attribute("email.status", "sent")

                logger.info("Email processed successfully",
                           email=message.body.get("email"),
                           processing_time_ms=processing_time * 1000)

                return result

            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

                logger.error("Email processing failed",
                           email=message.body.get("email"),
                           error=str(e))
                raise

    async def _send_email(self, data):
        with tracer.start_as_current_span("send_email") as span:
            span.set_attribute("email.provider", "smtp")

            # Simulate email sending
            await asyncio.sleep(0.1)

            return {"status": "sent", "message_id": "msg_123"}

# Run worker
if __name__ == "__main__":
    worker = ObservableEmailWorker()
    worker.run_sync()
```

### Custom Metrics

```python
from pythia.monitoring.metrics import Counter, Histogram, Gauge
from prometheus_client import start_http_server

class MetricsEnabledWorker(Worker):
    def __init__(self):
        super().__init__()

        # Custom metrics
        self.messages_processed = Counter(
            'pythia_messages_processed_total',
            'Total messages processed',
            ['worker_type', 'status']
        )

        self.processing_duration = Histogram(
            'pythia_message_processing_seconds',
            'Message processing duration',
            ['worker_type']
        )

        self.queue_size = Gauge(
            'pythia_queue_size',
            'Current queue size',
            ['queue_name']
        )

        # Start metrics server
        start_http_server(8000)

    async def process(self, message):
        start_time = time.time()

        try:
            result = await self._process_message(message)

            # Record success metrics
            self.messages_processed.labels(
                worker_type=self.__class__.__name__,
                status='success'
            ).inc()

            duration = time.time() - start_time
            self.processing_duration.labels(
                worker_type=self.__class__.__name__
            ).observe(duration)

            return result

        except Exception as e:
            # Record error metrics
            self.messages_processed.labels(
                worker_type=self.__class__.__name__,
                status='error'
            ).inc()
            raise

    async def health_check(self):
        # Update queue size metric
        queue_size = await self.source.get_queue_size()
        self.queue_size.labels(
            queue_name=self.source.queue_name
        ).set(queue_size)

        return {"status": "healthy", "queue_size": queue_size}
```

## 📈 Pre-built Grafana Dashboards

### Worker Overview Dashboard

```json
{
  "dashboard": {
    "id": null,
    "title": "Pythia Workers Overview",
    "description": "Overview of all Pythia workers performance and health",
    "panels": [
      {
        "title": "Messages Processed/sec",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(pythia_messages_processed_total[5m])",
            "legendFormat": "{{worker_type}} - {{status}}"
          }
        ]
      },
      {
        "title": "Processing Duration",
        "type": "timeseries",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(pythia_message_processing_seconds_bucket[5m]))",
            "legendFormat": "P95 - {{worker_type}}"
          },
          {
            "expr": "histogram_quantile(0.50, rate(pythia_message_processing_seconds_bucket[5m]))",
            "legendFormat": "P50 - {{worker_type}}"
          }
        ]
      },
      {
        "title": "Error Rate",
        "type": "timeseries",
        "targets": [
          {
            "expr": "rate(pythia_messages_processed_total{status=\"error\"}[5m]) / rate(pythia_messages_processed_total[5m]) * 100",
            "legendFormat": "Error Rate % - {{worker_type}}"
          }
        ]
      },
      {
        "title": "Queue Sizes",
        "type": "timeseries",
        "targets": [
          {
            "expr": "pythia_queue_size",
            "legendFormat": "{{queue_name}}"
          }
        ]
      }
    ]
  }
}
```

### Tracing Dashboard

Create a tracing dashboard to view distributed traces:

```json
{
  "dashboard": {
    "title": "Pythia Distributed Tracing",
    "panels": [
      {
        "title": "Trace Search",
        "type": "traces",
        "datasource": "Tempo",
        "targets": [
          {
            "query": "{ service.name=\"email-worker\" }",
            "queryType": ""
          }
        ]
      },
      {
        "title": "Service Map",
        "type": "nodeGraph",
        "datasource": "Tempo"
      }
    ]
  }
}
```

## 🚨 Alerting Rules

Create alerting rules for common issues:

```yaml
# prometheus/pythia_alerts.yml
groups:
  - name: pythia_workers
    rules:
      - alert: HighErrorRate
        expr: rate(pythia_messages_processed_total{status="error"}[5m]) / rate(pythia_messages_processed_total[5m]) * 100 > 5
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High error rate in Pythia worker"
          description: "Worker {{ $labels.worker_type }} has an error rate of {{ $value }}%"

      - alert: HighProcessingLatency
        expr: histogram_quantile(0.95, rate(pythia_message_processing_seconds_bucket[5m])) > 5
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "High processing latency in Pythia worker"
          description: "Worker {{ $labels.worker_type }} P95 latency is {{ $value }}s"

      - alert: QueueBacklog
        expr: pythia_queue_size > 1000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Large queue backlog"
          description: "Queue {{ $labels.queue_name }} has {{ $value }} pending messages"

      - alert: WorkerDown
        expr: up{job="pythia-workers"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Pythia worker is down"
          description: "Worker instance {{ $labels.instance }} is not responding"
```

## 🏃 Running the Stack

### Start Observability Stack

```bash
# Start all services
docker-compose -f docker-compose.observability.yml up -d

# Check services are running
docker-compose -f docker-compose.observability.yml ps

# View logs
docker-compose -f docker-compose.observability.yml logs -f grafana
```

### Access Dashboards

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Tempo**: http://localhost:3200

### Import Dashboards

1. Open Grafana at http://localhost:3000
2. Go to "+" → "Import"
3. Upload the JSON dashboard files
4. Configure data sources if needed

## 🔧 Production Configuration

### Environment Variables

```bash
# OpenTelemetry
OTEL_SERVICE_NAME=email-worker
OTEL_SERVICE_VERSION=1.0.0
OTEL_EXPORTER_OTLP_ENDPOINT=https://your-otel-collector:4317
OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer your-token"

# Metrics
PYTHIA_METRICS_ENABLED=true
PYTHIA_METRICS_PORT=8000
PYTHIA_METRICS_ENDPOINT=/metrics

# Tracing
PYTHIA_TRACING_ENABLED=true
PYTHIA_TRACING_SAMPLE_RATE=0.1  # Sample 10% of traces

# Logging
PYTHIA_LOG_LEVEL=INFO
PYTHIA_LOG_FORMAT=json
```

### Kubernetes Deployment

```yaml
# kubernetes/observability-stack.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
      - name: Prometheus
        type: prometheus
        url: http://prometheus:9090
        access: proxy
      - name: Tempo
        type: tempo
        url: http://tempo:3200
        access: proxy
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pythia-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: pythia-worker
  template:
    metadata:
      labels:
        app: pythia-worker
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8000"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: worker
        image: your-registry/pythia-worker:latest
        ports:
        - containerPort: 8000
          name: metrics
        env:
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: "http://otel-collector:4317"
        - name: PYTHIA_METRICS_ENABLED
          value: "true"
        - name: PYTHIA_TRACING_ENABLED
          value: "true"
```

## 📚 Best Practices

### 1. Metric Naming

Follow Prometheus naming conventions:

```python
# ✅ Good metric names
messages_processed_total
message_processing_duration_seconds
queue_size_current
worker_health_status

# ❌ Avoid these patterns
messagesProcessed
process_time_ms
queueLen
```

### 2. Trace Attributes

Add meaningful attributes to spans:

```python
span.set_attribute("user.id", user_id)
span.set_attribute("message.type", message_type)
span.set_attribute("queue.name", queue_name)
span.set_attribute("worker.version", "1.0.0")
```

### 3. Sampling Strategy

Use appropriate sampling for production:

```python
# High-volume services - sample less
setup_observability(tracing_sample_rate=0.01)  # 1%

# Critical services - sample more
setup_observability(tracing_sample_rate=0.1)   # 10%

# Development - sample everything
setup_observability(tracing_sample_rate=1.0)   # 100%
```

### 4. Dashboard Organization

Structure your dashboards:

- **Overview Dashboard** - High-level metrics across all workers
- **Service Dashboards** - Detailed metrics per worker type
- **SLA Dashboards** - SLA/SLO tracking
- **Troubleshooting Dashboards** - Error analysis and debugging

---

## 🎯 Next Steps

1. **Set up the observability stack** using Docker Compose
2. **Instrument your workers** with metrics and tracing
3. **Import the pre-built dashboards** in Grafana
4. **Configure alerting rules** for your SLAs
5. **Test the complete flow** from worker to dashboard

Ready to achieve full observability of your Pythia workers!
