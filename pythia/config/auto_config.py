"""
Auto-configuration detection for Pythia workers
"""

import os
import json
from typing import Optional, Type, Dict, Any, List, Tuple
from .base import WorkerConfig
from .kafka import KafkaConfig
from .rabbitmq import RabbitMQConfig
from .redis import RedisConfig
from .http import HTTPConfig
from .broker_factory import BrokerFactory


def detect_broker_type() -> Optional[str]:
    """Auto-detect broker type from environment variables"""

    # Check for Kafka environment variables
    kafka_vars = [
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_BROKERS",
        "CONFLUENT_BOOTSTRAP_SERVERS",
    ]
    if any(var in os.environ for var in kafka_vars):
        return "kafka"

    # Check for RabbitMQ environment variables
    rabbitmq_vars = ["RABBITMQ_URL", "RABBITMQ_HOST", "AMQP_URL", "CLOUDAMQP_URL"]
    if any(var in os.environ for var in rabbitmq_vars):
        return "rabbitmq"

    # Check for Redis environment variables (with type detection)
    redis_vars = ["REDIS_URL", "REDIS_HOST", "REDISCLOUD_URL", "REDISTOGO_URL"]
    if any(var in os.environ for var in redis_vars):
        # Detect Redis type preference
        return detect_redis_type()

    # Check for HTTP/Webhook environment variables
    http_vars = ["WEBHOOK_URL", "HTTP_BASE_URL", "API_BASE_URL"]
    if any(var in os.environ for var in http_vars):
        return "http"

    return None


def detect_redis_type() -> str:
    """Detect specific Redis broker type from environment"""

    # Check for specific Redis type indicators
    if os.environ.get("REDIS_STREAM_NAME") or os.environ.get("REDIS_CONSUMER_GROUP"):
        return "redis_streams"
    elif os.environ.get("REDIS_CHANNEL") or os.environ.get("REDIS_PATTERN"):
        return "redis_pubsub"
    elif os.environ.get("REDIS_QUEUE_NAME") or os.environ.get("REDIS_QUEUE"):
        return "redis_lists"
    else:
        # Default to streams if no specific type detected
        return "redis_streams"


def auto_detect_config() -> WorkerConfig:
    """Auto-detect and create worker configuration"""

    broker_type = detect_broker_type()

    if not broker_type:
        raise ValueError(
            "No supported message broker configuration found in environment. "
            "Please set appropriate environment variables for Kafka, RabbitMQ, Redis, or HTTP."
        )

    # Create base worker config with detected broker type
    worker_config = WorkerConfig(broker_type=broker_type)

    # Auto-detect multiple brokers if available
    available_brokers = detect_all_brokers()
    if len(available_brokers) > 1:
        worker_config.multi_broker = True
        worker_config.available_brokers = available_brokers

    return worker_config


def create_broker_config(broker_type: str) -> Any:
    """Create broker-specific configuration"""

    config_classes = {
        "kafka": KafkaConfig,
        "rabbitmq": RabbitMQConfig,
        "redis": RedisConfig,
        "redis_streams": RedisConfig,
        "redis_pubsub": RedisConfig,
        "redis_lists": RedisConfig,
        "http": HTTPConfig,
    }

    if broker_type not in config_classes:
        raise ValueError(f"Unsupported broker type: {broker_type}")

    config_class = config_classes[broker_type]
    return config_class()


def get_config_from_env(config_class: Type, prefix: str) -> Any:
    """Create configuration from environment variables with given prefix"""

    # Collect environment variables with the specified prefix
    env_vars = {}
    for key, value in os.environ.items():
        if key.startswith(prefix):
            # Remove prefix and convert to lowercase
            config_key = key[len(prefix) :].lower()
            env_vars[config_key] = value

    # Create configuration instance
    return config_class(**env_vars)


def validate_configuration(config: Any) -> bool:
    """Validate configuration completeness"""

    try:
        # This will raise validation errors if configuration is invalid
        config.model_validate(config.model_dump())
        return True
    except Exception:
        return False


def print_configuration_help():
    """Print help message for configuration setup"""

    help_text = """
Pythia Configuration Help
========================

Environment Variables for Auto-Configuration:

KAFKA:
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  KAFKA_GROUP_ID=my-group
  KAFKA_TOPICS=topic1,topic2

RABBITMQ:
  RABBITMQ_URL=amqp://localhost:5672
  RABBITMQ_QUEUE=my-queue
  RABBITMQ_EXCHANGE=my-exchange

REDIS:
  REDIS_URL=redis://localhost:6379
  REDIS_STREAM_NAME=my-stream  # For Redis Streams
  REDIS_CONSUMER_GROUP=my-group  # For Redis Streams
  REDIS_CHANNEL=my-channel  # For Redis Pub/Sub
  REDIS_QUEUE_NAME=my-queue  # For Redis Lists

HTTP/WEBHOOKS:
  HTTP_BASE_URL=https://api.example.com
  WEBHOOK_URL=https://webhook.example.com

GENERAL WORKER:
  PYTHIA_WORKER_NAME=my-worker
  PYTHIA_LOG_LEVEL=INFO
  PYTHIA_MAX_RETRIES=3

For detailed configuration options, see the documentation.
    """

    print(help_text)


def detect_all_brokers() -> List[str]:
    """Detect all available broker types from environment"""
    available = []

    # Check each broker type
    if detect_broker_type_specific("kafka"):
        available.append("kafka")
    if detect_broker_type_specific("rabbitmq"):
        available.append("rabbitmq")

    # Check Redis variants
    if detect_broker_type_specific("redis_streams"):
        available.append("redis_streams")
    if detect_broker_type_specific("redis_pubsub"):
        available.append("redis_pubsub")
    if detect_broker_type_specific("redis_lists"):
        available.append("redis_lists")
    # Fallback for generic Redis
    elif detect_broker_type_specific("redis"):
        available.append("redis")

    if detect_broker_type_specific("http"):
        available.append("http")

    return available


def detect_broker_type_specific(broker_type: str) -> bool:
    """Check if a specific broker type is configured"""

    broker_vars = {
        "kafka": [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_BROKERS",
            "CONFLUENT_BOOTSTRAP_SERVERS",
        ],
        "rabbitmq": ["RABBITMQ_URL", "RABBITMQ_HOST", "AMQP_URL", "CLOUDAMQP_URL"],
        "redis": ["REDIS_URL", "REDIS_HOST", "REDISCLOUD_URL", "REDISTOGO_URL"],
        "redis_streams": [
            "REDIS_URL",
            "REDIS_HOST",
            "REDISCLOUD_URL",
            "REDISTOGO_URL",
            "REDIS_STREAM_NAME",
        ],
        "redis_pubsub": [
            "REDIS_URL",
            "REDIS_HOST",
            "REDISCLOUD_URL",
            "REDISTOGO_URL",
            "REDIS_CHANNEL",
        ],
        "redis_lists": [
            "REDIS_URL",
            "REDIS_HOST",
            "REDISCLOUD_URL",
            "REDISTOGO_URL",
            "REDIS_QUEUE_NAME",
        ],
        "http": ["WEBHOOK_URL", "HTTP_BASE_URL", "API_BASE_URL"],
    }

    return any(var in os.environ for var in broker_vars.get(broker_type, []))


def auto_create_brokers(broker_types: List[str]) -> Dict[str, Any]:
    """Auto-create broker instances for given types"""
    factory = BrokerFactory()
    brokers = {}

    for broker_type in broker_types:
        try:
            # Create consumer and producer for each type
            consumer = factory.create_consumer(broker_type)
            producer = factory.create_producer(broker_type)

            brokers[broker_type] = {
                "consumer": consumer,
                "producer": producer,
                "config": factory.create_config(broker_type),
            }
        except Exception:
            # Log warning but continue with other brokers
            pass

    return brokers


def create_auto_worker_config() -> Tuple[WorkerConfig, Dict[str, Any]]:
    """Create worker config and auto-detected brokers"""

    config = auto_detect_config()
    available_brokers = detect_all_brokers()

    if not available_brokers:
        raise ValueError("No brokers detected in environment")

    brokers = auto_create_brokers(available_brokers)

    return config, brokers


def validate_environment() -> Dict[str, Any]:
    """Validate environment configuration and return status"""

    status = {"valid": True, "brokers": {}, "errors": [], "warnings": []}

    available_brokers = detect_all_brokers()

    if not available_brokers:
        status["valid"] = False
        status["errors"].append("No message brokers detected in environment")
        return status

    # Test each broker configuration
    for broker_type in available_brokers:
        try:
            config = create_broker_config(broker_type)
            is_valid = validate_configuration(config)

            status["brokers"][broker_type] = {
                "available": True,
                "valid": is_valid,
                "config": config.__class__.__name__
                if hasattr(config, "__class__")
                else str(type(config)),
            }

            if not is_valid:
                status["warnings"].append(f"{broker_type} configuration may be incomplete")

        except Exception as e:
            status["brokers"][broker_type] = {
                "available": False,
                "valid": False,
                "error": str(e),
            }
            status["errors"].append(f"{broker_type}: {str(e)}")

    return status


def load_config_from_file(file_path: str) -> Dict[str, Any]:
    """Load configuration from JSON or YAML file"""

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    with open(file_path, "r") as f:
        if file_path.endswith(".json"):
            return json.load(f)
        elif file_path.endswith((".yml", ".yaml")):
            try:
                import yaml

                return yaml.safe_load(f)
            except ImportError:
                raise ImportError("PyYAML is required to load YAML configuration files")
        else:
            raise ValueError("Unsupported configuration file format. Use .json, .yml, or .yaml")


# Configuration registry for custom configurations
_config_registry: Dict[str, Type] = {}


def register_config(name: str, config_class: Type) -> None:
    """Register a custom configuration class"""
    _config_registry[name] = config_class


def get_registered_config(name: str) -> Optional[Type]:
    """Get a registered configuration class"""
    return _config_registry.get(name)


def list_registered_configs() -> Dict[str, Type]:
    """List all registered configuration classes"""
    return _config_registry.copy()
