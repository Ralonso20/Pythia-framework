# Configuration

Complete guide to configuring Pythia workers for different environments and use cases.

## Overview

Pythia uses **Pydantic** for configuration management, providing type validation, environment variable support, and clear error messages. Configuration can be set via code, environment variables, or configuration files.

## Configuration Hierarchy

```python
from pythia.config import WorkerConfig

# 1. Code-based configuration (highest priority)
config = WorkerConfig(
    worker_name="my-worker",
    max_concurrent=10
)

# 2. Environment variables (medium priority)
# PYTHIA_WORKER_NAME=my-worker
# PYTHIA_MAX_CONCURRENT=10

# 3. Configuration files (lowest priority)
# config.yaml, config.json, or .env files
```

## Core Worker Configuration

### Basic Configuration

```python
from pythia.config import WorkerConfig

config = WorkerConfig(
    # Worker identification
    worker_name="email-processor",          # Worker name for logs/metrics
    worker_id="email-processor-001",        # Unique worker ID (auto-generated if not provided)

    # Processing settings
    max_retries=3,                          # Maximum retry attempts for failed messages
    retry_delay=1.0,                        # Delay between retries in seconds
    batch_size=10,                          # Batch size for processing
    max_concurrent=20,                      # Maximum concurrent workers

    # Broker configuration
    broker_type="kafka",                    # Message broker type (kafka/redis/rabbitmq)
    multi_broker=False,                     # Enable multi-broker support

    # Logging configuration
    log_level="INFO",                       # Log level (DEBUG, INFO, WARNING, ERROR)
    log_format="json",                      # Log format (json/text)
    log_file="/var/log/pythia/worker.log", # Log file path (optional)

    # Health check configuration
    health_check_interval=30,               # Health check interval in seconds
    health_check_timeout=10                 # Health check timeout in seconds
)
```

### Environment Variable Configuration

```bash
# Core settings
export PYTHIA_WORKER_NAME="production-worker"
export PYTHIA_WORKER_ID="prod-worker-001"
export PYTHIA_BROKER_TYPE="kafka"
export PYTHIA_MAX_CONCURRENT=50

# Retry configuration
export PYTHIA_MAX_RETRIES=5
export PYTHIA_RETRY_DELAY=2.0

# Logging
export PYTHIA_LOG_LEVEL="WARNING"
export PYTHIA_LOG_FORMAT="json"
export PYTHIA_LOG_FILE="/var/log/pythia/production.log"

# Health checks
export PYTHIA_HEALTH_CHECK_INTERVAL=60
export PYTHIA_HEALTH_CHECK_TIMEOUT=15
```

```python
from pythia.config import WorkerConfig

# Automatically loads from environment variables
config = WorkerConfig()
print(f"Worker: {config.worker_name}")  # Output: production-worker
```

## Broker-Specific Configuration

### Redis Configuration

```python
from pythia.config.redis import RedisConfig

# Basic Redis setup
redis_config = RedisConfig(
    host="localhost",
    port=6379,
    db=0,
    password=None,

    # Queue configuration
    queue="task-queue",                     # List-based queue name
    batch_size=50,                         # Messages per batch
    block_timeout_ms=1000,                 # Polling timeout

    # Connection pooling
    connection_pool_size=20,               # Pool size
    socket_keepalive=True,                 # Keep connections alive
    socket_timeout=30,                     # Socket timeout
    retry_on_timeout=True,                 # Retry on timeout

    # Health monitoring
    health_check_interval=30               # Health check frequency
)

# Stream configuration
redis_stream_config = RedisConfig(
    host="redis-cluster.internal",
    port=6379,

    # Stream settings
    stream="events-stream",                # Stream name
    consumer_group="workers",              # Consumer group
    batch_size=100,                       # Larger batches for streams
    max_stream_length=50000,              # Limit stream size

    # Consumer settings
    block_timeout_ms=5000,                # 5 second block timeout
    consumer_name="worker-001"            # Consumer identifier
)

# Environment variables for Redis
# REDIS_HOST=redis.example.com
# REDIS_PORT=6380
# REDIS_PASSWORD=secure_password
# REDIS_DB=1
# REDIS_QUEUE=production-queue
```

### Kafka Configuration

```python
from pythia.config.kafka import KafkaConfig

# Basic Kafka setup
kafka_config = KafkaConfig(
    bootstrap_servers="kafka1:9092,kafka2:9092,kafka3:9092",

    # Consumer settings
    group_id="email-processors",
    topics=["emails", "notifications"],
    auto_offset_reset="earliest",          # Start from beginning
    enable_auto_commit=False,              # Manual commits for reliability

    # Performance tuning
    max_poll_records=1000,                 # Messages per poll
    fetch_min_bytes=50000,                 # Wait for more data
    fetch_max_wait_ms=500,                 # But don't wait too long

    # Session management
    session_timeout_ms=30000,              # 30 second session timeout
    heartbeat_interval_ms=3000,            # Heartbeat every 3 seconds
    max_poll_interval_ms=600000,           # 10 minutes max processing time

    # Producer settings (for output)
    acks="all",                           # Wait for all replicas
    retries=5,                            # Retry failed sends
    batch_size=32768,                     # 32KB batches
    linger_ms=10                          # Wait 10ms for batching
)

# Security configuration
kafka_secure_config = KafkaConfig(
    bootstrap_servers="secure-kafka:9093",

    # SASL authentication
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="worker-user",
    sasl_password="secure-password",

    # SSL settings
    ssl_ca_location="/etc/ssl/ca.crt",
    ssl_certificate_location="/etc/ssl/client.crt",
    ssl_key_location="/etc/ssl/client.key",
    ssl_key_password="key-password"
)

# Environment variables for Kafka
# KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092
# KAFKA_GROUP_ID=production-group
# KAFKA_TOPICS=orders,payments,notifications
# KAFKA_SECURITY_PROTOCOL=SASL_SSL
# KAFKA_SASL_USERNAME=prod-user
# KAFKA_SASL_PASSWORD=secure-password
```

### RabbitMQ Configuration

```python
from pythia.config.rabbitmq import RabbitMQConfig

# Basic RabbitMQ setup
rabbitmq_config = RabbitMQConfig(
    url="amqp://user:password@rabbitmq.internal:5672/production",

    # Queue configuration
    queue="notifications-queue",           # Queue name
    exchange="notifications-exchange",     # Exchange name
    routing_key="notification.*",         # Routing pattern

    # Durability settings
    durable=True,                         # Survive broker restart
    auto_ack=False,                       # Manual acknowledgment

    # Performance tuning
    prefetch_count=100,                   # Messages to prefetch

    # Connection settings
    heartbeat=600,                        # 10 minute heartbeat
    connection_attempts=5,                # Retry connection attempts
    retry_delay=2.0                       # Delay between attempts
)

# Advanced routing configuration
rabbitmq_routing_config = RabbitMQConfig(
    url="amqp://rabbitmq-cluster:5672/",

    # Topic exchange for complex routing
    queue="user-events-queue",
    exchange="events-topic-exchange",
    routing_key="user.*.created",         # Pattern matching

    # Exchange configuration
    exchange_type="topic",                # topic, direct, fanout, headers
    exchange_durable=True,

    # Queue arguments for advanced features
    queue_arguments={
        "x-dead-letter-exchange": "dlq-exchange",
        "x-dead-letter-routing-key": "failed",
        "x-message-ttl": 300000,          # 5 minute TTL
        "x-max-length": 10000             # Max queue length
    }
)

# Environment variables for RabbitMQ
# RABBITMQ_URL=amqp://user:pass@rabbitmq:5672/vhost
# RABBITMQ_QUEUE=production-queue
# RABBITMQ_EXCHANGE=production-exchange
# RABBITMQ_ROUTING_KEY=task.process
# RABBITMQ_PREFETCH_COUNT=50
```

## Advanced Configuration

### Logging Configuration

```python
from pythia.config import LogConfig

log_config = LogConfig(
    level="INFO",                         # Log level
    format="json",                        # Output format (json/text)
    file="/var/log/pythia/worker.log",    # Log file path
    rotation="100 MB",                    # Rotate when file reaches size
    retention="30 days",                  # Keep logs for 30 days

    # Structured logging
    add_timestamp=True,                   # Include timestamp
    add_worker_id=True,                   # Include worker ID
    add_correlation_id=True               # Include correlation ID for tracing
)

# Advanced logging setup
class CustomWorker(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config)

        # Custom log configuration
        self.logger.configure(
            handlers=[
                {
                    "sink": sys.stdout,
                    "format": "{time} | {level} | {name} | {message}",
                    "level": "INFO"
                },
                {
                    "sink": "/var/log/pythia/errors.log",
                    "format": "{time} | {level} | {name} | {message}",
                    "level": "ERROR",
                    "rotation": "10 MB"
                }
            ]
        )
```

### Metrics Configuration

```python
from pythia.config import MetricsConfig

metrics_config = MetricsConfig(
    enabled=True,                         # Enable metrics collection
    port=8080,                           # Metrics server port
    path="/metrics",                     # Metrics endpoint path

    # Prometheus configuration
    prometheus_enabled=True,
    prometheus_prefix="pythia_worker",    # Metric name prefix

    # Custom metrics
    custom_metrics={
        "message_processing_duration": {
            "type": "histogram",
            "description": "Time spent processing messages",
            "buckets": [0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
        },
        "queue_depth": {
            "type": "gauge",
            "description": "Current queue depth"
        },
        "error_rate": {
            "type": "counter",
            "description": "Number of processing errors"
        }
    }
)

# Use in worker
class MetricsAwareWorker(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config, metrics_config=metrics_config)

    async def process_message(self, message):
        # Record processing time
        with self.metrics.timer("message_processing_duration"):
            result = await self._process_message(message)

        # Update counters
        self.metrics.counter("messages_processed_total").inc()

        return result
```

### Security Configuration

```python
from pythia.config import SecurityConfig

security_config = SecurityConfig(
    # SSL/TLS settings
    ssl_enabled=True,
    ssl_cert_file="/etc/ssl/certs/worker.crt",
    ssl_key_file="/etc/ssl/private/worker.key",
    ssl_ca_file="/etc/ssl/certs/ca.crt",

    # Authentication
    auth_enabled=True,
    auth_method="oauth2",                 # oauth2, basic, api_key

    # Field encryption
    encryption_enabled=True,
    encryption_key="base64-encoded-key"   # AES encryption key
)

# Secure worker example
class SecureWorker(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.security_config = security_config

    async def process_message(self, message):
        # Decrypt sensitive fields if needed
        if self.security_config.encryption_enabled:
            message = await self._decrypt_message(message)

        result = await self._process_secure_message(message)

        # Encrypt result if needed
        if self.security_config.encryption_enabled:
            result = await self._encrypt_result(result)

        return result
```

### Resilience Configuration

```python
from pythia.config import ResilienceConfig

resilience_config = ResilienceConfig(
    # Retry settings
    max_retries=5,                        # Maximum retry attempts
    retry_delay=1.0,                      # Initial delay
    retry_backoff=2.0,                    # Exponential backoff multiplier
    retry_max_delay=60.0,                 # Maximum delay between retries

    # Circuit breaker
    circuit_breaker_enabled=True,
    circuit_breaker_threshold=10,         # Failures to trigger breaker
    circuit_breaker_timeout=60,           # Breaker reset timeout

    # Timeouts
    processing_timeout=300,               # Per-message timeout (5 minutes)
    connection_timeout=30,                # Connection timeout

    # Rate limiting
    rate_limit_enabled=True,
    rate_limit_requests=1000,             # Requests per minute
    rate_limit_window=60                  # Time window in seconds
)

# Use resilience config
config = WorkerConfig(
    worker_name="resilient-worker",
    resilience=resilience_config
)
```

## Environment-Specific Configuration

### Development Configuration

```python
# config/development.py
from pythia.config import WorkerConfig
from pythia.config.redis import RedisConfig

def get_development_config() -> WorkerConfig:
    return WorkerConfig(
        worker_name="dev-worker",
        broker_type="redis",
        log_level="DEBUG",
        log_format="text",                # Readable format for dev
        max_concurrent=5,                 # Lower concurrency
        health_check_interval=10,         # Frequent health checks

        # Development-friendly settings
        max_retries=1,                    # Fail fast for debugging
        retry_delay=0.5                   # Quick retries
    )

def get_redis_config() -> RedisConfig:
    return RedisConfig(
        host="localhost",
        port=6379,
        db=1,                            # Use different DB for dev
        queue="dev-queue",
        batch_size=5                     # Small batches for testing
    )
```

### Production Configuration

```python
# config/production.py
from pythia.config import WorkerConfig
from pythia.config.kafka import KafkaConfig

def get_production_config() -> WorkerConfig:
    return WorkerConfig(
        worker_name="prod-worker",
        broker_type="kafka",
        log_level="INFO",
        log_format="json",               # Structured logging
        log_file="/var/log/pythia/production.log",

        # Production settings
        max_concurrent=50,               # High concurrency
        max_retries=5,                   # More retry attempts
        retry_delay=2.0,

        # Monitoring
        health_check_interval=60,

        # Security
        ssl_enabled=True
    )

def get_kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BROKERS"),
        group_id="production-workers",
        topics=["orders", "payments", "notifications"],

        # Production optimization
        max_poll_records=2000,
        fetch_min_bytes=100000,
        session_timeout_ms=45000,

        # Security
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username=os.getenv("KAFKA_USERNAME"),
        sasl_password=os.getenv("KAFKA_PASSWORD")
    )
```

### Testing Configuration

```python
# config/testing.py
from pythia.config import WorkerConfig

def get_testing_config() -> WorkerConfig:
    return WorkerConfig(
        worker_name="test-worker",
        broker_type="memory",            # In-memory broker for tests
        log_level="ERROR",               # Minimal logging
        max_concurrent=1,                # Single-threaded for tests
        max_retries=0,                   # No retries in tests
        health_check_interval=1          # Fast health checks
    )
```

## Configuration Files

### YAML Configuration

```yaml
# config.yaml
pythia:
  worker:
    name: "yaml-worker"
    max_concurrent: 20
    log_level: "INFO"

  redis:
    host: "redis.example.com"
    port: 6379
    queue: "production-queue"
    batch_size: 50

  metrics:
    enabled: true
    port: 8080
    prometheus_enabled: true
```

```python
import yaml
from pythia.config import WorkerConfig

def load_config_from_yaml(file_path: str) -> WorkerConfig:
    with open(file_path, 'r') as file:
        config_data = yaml.safe_load(file)

    return WorkerConfig(**config_data['pythia']['worker'])
```

### JSON Configuration

```json
{
  "pythia": {
    "worker": {
      "name": "json-worker",
      "max_concurrent": 30,
      "log_level": "INFO",
      "broker_type": "kafka"
    },
    "kafka": {
      "bootstrap_servers": "kafka1:9092,kafka2:9092",
      "group_id": "json-workers",
      "topics": ["events"]
    }
  }
}
```

### Environment File (.env)

```bash
# .env
PYTHIA_WORKER_NAME=env-worker
PYTHIA_BROKER_TYPE=rabbitmq
PYTHIA_MAX_CONCURRENT=25
PYTHIA_LOG_LEVEL=WARNING

RABBITMQ_URL=amqp://user:pass@rabbitmq:5672/prod
RABBITMQ_QUEUE=production-tasks
RABBITMQ_PREFETCH_COUNT=100
```

## Configuration Validation

```python
from pydantic import ValidationError
from pythia.config import WorkerConfig

def validate_config():
    """Example of configuration validation"""
    try:
        config = WorkerConfig(
            worker_name="test",
            max_concurrent=-1,           # Invalid value
            log_level="INVALID"          # Invalid level
        )
    except ValidationError as e:
        print("Configuration errors:")
        for error in e.errors():
            print(f"  {error['loc'][0]}: {error['msg']}")

# Output:
# Configuration errors:
#   max_concurrent: ensure this value is greater than 0
#   log_level: value is not a valid enumeration member
```

## Dynamic Configuration Updates

```python
class ConfigurableWorker(Worker):
    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.config_file_path = "config.yaml"
        self.last_config_check = time.time()

    async def _check_config_updates(self):
        """Check for configuration updates"""
        if time.time() - self.last_config_check > 60:  # Check every minute
            try:
                new_config = self._load_config_from_file()
                if new_config != self.config:
                    self.logger.info("Configuration updated, applying changes...")
                    await self._apply_config_changes(new_config)
                    self.config = new_config

            except Exception as e:
                self.logger.error(f"Failed to reload configuration: {e}")

            self.last_config_check = time.time()

    async def _apply_config_changes(self, new_config: WorkerConfig):
        """Apply configuration changes without restart"""
        # Update logging level
        if new_config.log_level != self.config.log_level:
            self.logger.configure(level=new_config.log_level)

        # Update concurrency
        if new_config.max_concurrent != self.config.max_concurrent:
            await self._adjust_concurrency(new_config.max_concurrent)
```

## Best Practices

### 1. Environment Separation

```python
import os

def get_config() -> WorkerConfig:
    """Get configuration based on environment"""
    env = os.getenv("ENVIRONMENT", "development")

    if env == "production":
        return get_production_config()
    elif env == "staging":
        return get_staging_config()
    else:
        return get_development_config()
```

### 2. Secret Management

```python
import os
from typing import Optional

class SecretManager:
    """Manage secrets from various sources"""

    @staticmethod
    def get_secret(key: str) -> Optional[str]:
        """Get secret from environment or secret store"""
        # Try environment first
        value = os.getenv(key)
        if value:
            return value

        # Try AWS Secrets Manager, HashiCorp Vault, etc.
        return SecretManager._get_from_secret_store(key)

    @staticmethod
    def _get_from_secret_store(key: str) -> Optional[str]:
        # Implementation depends on your secret store
        pass

# Usage
kafka_config = KafkaConfig(
    bootstrap_servers=os.getenv("KAFKA_BROKERS"),
    sasl_username=SecretManager.get_secret("KAFKA_USERNAME"),
    sasl_password=SecretManager.get_secret("KAFKA_PASSWORD")
)
```

### 3. Configuration Testing

```python
import pytest
from pythia.config import WorkerConfig

class TestConfiguration:
    def test_default_config(self):
        """Test default configuration values"""
        config = WorkerConfig()
        assert config.worker_name == "pythia-worker"
        assert config.max_retries == 3
        assert config.log_level == "INFO"

    def test_environment_override(self, monkeypatch):
        """Test environment variable overrides"""
        monkeypatch.setenv("PYTHIA_WORKER_NAME", "test-worker")
        monkeypatch.setenv("PYTHIA_MAX_RETRIES", "5")

        config = WorkerConfig()
        assert config.worker_name == "test-worker"
        assert config.max_retries == 5

    def test_invalid_config(self):
        """Test configuration validation"""
        with pytest.raises(ValidationError):
            WorkerConfig(max_concurrent=-1)
```

## Next Steps

- [Error Handling](error-handling.md) - Advanced error handling patterns
- [Performance Optimization](../performance/optimization.md) - Performance tuning
- [API Reference](../api/core.md) - Complete API documentation
