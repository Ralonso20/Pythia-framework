# Pythia Performance Benchmarking Suite

Comprehensive performance testing infrastructure for the Pythia framework, designed to measure and compare performance across different message brokers (Kafka, RabbitMQ, Redis).

## 🚀 Quick Start

### 1. Setup Infrastructure

```bash
# Start monitoring stack
cd infrastructure
./setup_monitoring.sh
```

This will start:
- **Message Brokers**: Kafka, RabbitMQ, Redis
- **Monitoring**: Prometheus, Grafana, InfluxDB
- **System Metrics**: cAdvisor, Node Exporter
- **Load Testing**: Locust

### 2. Run Basic Benchmark

```bash
# Quick Redis benchmark (60s, 1000 msg/s)
./run_benchmarks.sh quick redis

# Custom benchmark
./run_benchmarks.sh broker kafka 120 2000 4 1024
#                          ^     ^   ^    ^ ^
#                       broker dur rate workers size
```

### 3. Access Monitoring

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **RabbitMQ Management**: http://localhost:15672 (guest/guest)

## 📊 Benchmark Types

### Quick Benchmarks
```bash
./run_benchmarks.sh quick redis      # 60s, 1000 msg/s
./run_benchmarks.sh quick kafka      # Standard quick test
./run_benchmarks.sh quick rabbitmq   # Single broker test
```

### Comprehensive Testing
```bash
# Full benchmark suite (all brokers, rates, configurations)
./run_benchmarks.sh comprehensive 60

# Custom comprehensive test
./run_benchmarks.sh comprehensive [duration] [base_rate]
```

### Scenario Testing
```bash
./run_benchmarks.sh scenarios  # All scenarios

# Individual scenarios
python3 scenarios/throughput_test.py --base-rate 5000
python3 scenarios/latency_test.py --duration 120
```

### Stress Testing
```bash
# Long-duration, high-load tests
./run_benchmarks.sh stress
```

## 📈 Monitoring & Metrics

### Real-time Dashboards

**Grafana Dashboards** (http://localhost:3000):
- Pythia Performance Dashboard
- System Resource Usage
- Broker-Specific Metrics

### Key Metrics Tracked

- **Throughput**: Messages processed per second
- **Latency**: P50, P95, P99 processing latencies
- **Error Rates**: Failed message percentage
- **Resource Usage**: CPU, Memory, Network I/O
- **Broker Health**: Queue depths, connection counts

### Prometheus Metrics

Custom recording rules for:
```
pythia:message_processing_rate
pythia:message_processing_latency_p95
pythia:error_rate
kafka:consumer_lag_max
rabbitmq:queue_messages_rate
redis:commands_per_second
```

## 🔧 Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Load Generator │───▶│  Pythia Workers  │───▶│   Monitoring    │
│   (Python/Locust)│    │ (Multiple Procs) │    │ (Prometheus/    │
│                 │    │                  │    │  Grafana)       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Message       │    │   Docker         │    │   Results       │
│   Brokers       │    │   Environment    │    │   Storage       │
│   (K/R/Redis)   │    │                  │    │   (JSON/DB)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📁 Directory Structure

```
benchmarks/
├── infrastructure/           # Docker & monitoring setup
│   ├── docker-compose.monitoring.yml
│   ├── prometheus.yml
│   ├── grafana/
│   └── setup_monitoring.sh
├── load_generators/          # Broker-specific load tests
│   ├── kafka_load_test.py
│   ├── rabbitmq_load_test.py
│   └── redis_load_test.py
├── scenarios/               # Specific test scenarios
│   ├── throughput_test.py
│   └── latency_test.py
├── results/                 # Benchmark results
│   ├── baseline/
│   ├── optimized/
│   └── comparison/
├── benchmark_cli.py         # Main CLI tool
└── run_benchmarks.sh       # Benchmark runner script
```

## 🛠️ CLI Tool Usage

### Basic Commands

```bash
# Run single benchmark
python3 benchmark_cli.py benchmark --broker kafka --duration 60 --rate 1000

# Compare results
python3 benchmark_cli.py compare result1.json result2.json result3.json

# List available results
python3 benchmark_cli.py list --results-dir ./results
```

### Advanced Options

```bash
python3 benchmark_cli.py benchmark \
    --broker rabbitmq \
    --duration 120 \
    --rate 2000 \
    --size 2048 \
    --workers 4 \
    --batch-size 50 \
    --results-dir ./custom_results
```

## 📊 Expected Performance Targets

| Metric | Redis | Kafka | RabbitMQ |
|--------|-------|-------|----------|
| **Throughput** | 8K msg/s | 5K msg/s | 3K msg/s |
| **Avg Latency** | < 10ms | < 20ms | < 30ms |
| **P95 Latency** | < 25ms | < 50ms | < 80ms |
| **Memory/Worker** | < 30MB | < 50MB | < 40MB |

## 🔄 Test Scenarios

### 1. Throughput Tests
- **Goal**: Find maximum sustainable message rate
- **Method**: Gradually increase rate until errors occur
- **Duration**: 2-5 minutes per test
- **Workers**: 4-8 processes

### 2. Latency Tests
- **Goal**: Measure processing latency under different loads
- **Loads**: Low (100 msg/s), Medium (1K msg/s), High (5K msg/s)
- **Sizes**: Small (256B), Medium (1KB), Large (4KB)
- **Focus**: P95/P99 latencies

### 3. Stress Tests
- **Goal**: Sustained high load performance
- **Duration**: 10+ minutes
- **Load**: Near-maximum sustainable rate
- **Monitoring**: Memory leaks, CPU degradation

### 4. Scalability Tests
- **Goal**: Performance vs worker count
- **Workers**: 1, 2, 4, 8 processes
- **Load**: Fixed rate per test
- **Analysis**: Scaling efficiency

## 🐛 Troubleshooting

### Common Issues

**Infrastructure not ready:**
```bash
./run_benchmarks.sh check  # Verify all services are running
```

**High error rates:**
- Reduce message rate
- Increase worker count
- Check broker configuration
- Monitor system resources

**Memory issues:**
```bash
# Monitor container memory
docker stats

# Check system memory
free -h
```

**Connection failures:**
```bash
# Check broker connectivity
redis-cli ping
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
curl http://localhost:15672/api/overview (RabbitMQ)
```

### Performance Tuning

**For Redis:**
- Disable persistence during benchmarks
- Use pipelining for bulk operations
- Tune maxmemory settings

**For Kafka:**
- Increase batch.size and linger.ms
- Tune num.network.threads
- Use compression (gzip/snappy)

**For RabbitMQ:**
- Enable lazy queues for large queues
- Tune prefetch_count
- Use persistent delivery only when needed

## 📈 Results Analysis

### Benchmark Reports

Results are saved as JSON files with schema:
```json
{
  "timestamp": "2025-08-31T...",
  "config": {...},
  "total_messages": 60000,
  "messages_per_second": 1000.5,
  "avg_latency_ms": 15.2,
  "p95_latency_ms": 45.8,
  "p99_latency_ms": 89.1,
  "error_count": 12,
  "error_rate": 0.0002,
  "cpu_usage_percent": 45.2,
  "memory_usage_mb": 128.5,
  "broker_metrics": {...}
}
```

### Comparison Reports

```bash
# Generate comparison
python3 benchmark_cli.py compare kafka_*.json rabbitmq_*.json redis_*.json

# View summary
./run_benchmarks.sh report
```

## 🔍 Advanced Usage

### Custom Load Generators

Create custom load tests by extending base classes:

```python
from load_generators.kafka_load_test import KafkaLoadTest

class CustomKafkaTest(KafkaLoadTest):
    def generate_test_message(self):
        # Custom message generation
        return {...}
```

### Custom Scenarios

```python
from benchmark_cli import BenchmarkRunner, BenchmarkConfig

async def custom_scenario():
    config = BenchmarkConfig(...)
    runner = BenchmarkRunner(config)
    result = await runner.run_benchmark()
    # Custom analysis
```

### Integration with CI/CD

```bash
# Run performance regression tests
./run_benchmarks.sh comprehensive 30 > benchmark_results.txt

# Check if performance meets thresholds
python3 -c "
import json
with open('results/latest/comparison_report.json') as f:
    data = json.load(f)

for broker, metrics in data.get('summary', {}).items():
    throughput = metrics.get('throughput_msg_per_sec', 0)
    if throughput < 1000:  # Threshold
        print(f'FAIL: {broker} throughput {throughput} < 1000')
        exit(1)
print('PASS: All benchmarks meet performance thresholds')
"
```

---

## ☁️ Cloud Queue Benchmarks

**NEW**: Comprehensive cloud queue benchmarking with Docker emulators!

### Quick Cloud Benchmark

```bash
# Interactive cloud benchmark runner
./run_cloud_benchmarks.py

# Or run directly
python -m benchmarks.cloud_queues.runner --provider all --report
```

### Supported Cloud Providers

| Provider | Service | Docker Emulator | Status |
|----------|---------|----------------|--------|
| **AWS** | SQS | LocalStack | ✅ Ready |
| **GCP** | Pub/Sub | GCP SDK Emulator | ✅ Ready |
| **Azure** | Service Bus | Redis Simulation | ✅ Ready |
| **Azure** | Storage Queue | Azurite | ✅ Ready |

### Cloud Test Scenarios

```bash
# Basic throughput test
python -m benchmarks.cloud_queues.runner --test basic_throughput --provider aws

# High throughput with concurrency
python -m benchmarks.cloud_queues.runner \
  --messages 5000 --producers 4 --consumers 4 --provider gcp

# Large message test
python -m benchmarks.cloud_queues.runner \
  --messages 100 --size 65536 --provider azure-storage

# Compare all providers
python -m benchmarks.cloud_queues.runner --provider all --report
```

### Cloud Benchmark Features

- 🐳 **Docker-based emulators** for local testing
- 📊 **Comprehensive metrics** (latency, throughput, errors)
- 🔄 **Multi-provider comparison** in single run
- 📈 **Detailed performance reports** with recommendations
- ⚡ **Quick tests** for rapid validation
- 🎯 **Custom configurations** for specific scenarios

### Example Cloud Results

```
🏆 Best Throughput: GCP Pub/Sub (1890.7 msg/s)
⚡ Best Latency: GCP Pub/Sub (8.23 ms)
💰 Most Cost-Effective: AWS SQS (batch operations)
🔧 Most Reliable: Azure Storage Queue (99.9% success)
```

See detailed cloud benchmarking documentation in the `cloud_queues/` directory.

---

## 📞 Support

For questions or issues:
- Check monitoring dashboards first
- Review broker logs: `docker logs <broker_name>`
- Verify system resources: `docker stats`
- Open an issue with benchmark results attached
