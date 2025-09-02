#!/usr/bin/env python3
"""
Setup script for running the Pythia example

This script helps set up the environment for testing the example worker.
"""

import os
import sys
from pathlib import Path


def setup_environment():
    """Setup environment variables for the example"""

    print("🔧 Setting up environment for Pythia example...")

    # Kafka configuration
    env_vars = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_GROUP_ID": "pythia-approval-group",
        "KAFKA_TOPICS": "approvals",
        "KAFKA_AUTO_OFFSET_RESET": "earliest",
        # Worker configuration
        "PYTHIA_WORKER_NAME": "approval-worker",
        "PYTHIA_LOG_LEVEL": "INFO",
        "PYTHIA_MAX_RETRIES": "3",
        # Broker type
        "PYTHIA_BROKER_TYPE": "kafka",
    }

    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
        print(f"   {key}={value}")

    print("✅ Environment configured!")


def check_dependencies():
    """Check if required dependencies are installed"""

    print("\n🔍 Checking dependencies...")

    required_packages = [
        "confluent-kafka",
        "pydantic",
        "pydantic-settings",
        "loguru",
    ]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
            print(f"   ✅ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"   ❌ {package}")

    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("   Please install with: pip install -e .")
        return False

    print("✅ All dependencies are installed!")
    return True


def setup_pythia_path():
    """Add Pythia to Python path for development"""

    pythia_root = Path(__file__).parent
    pythia_path = str(pythia_root)

    if pythia_path not in sys.path:
        sys.path.insert(0, pythia_path)
        print(f"📁 Added to Python path: {pythia_path}")


def print_docker_compose():
    """Print Docker Compose configuration for Kafka"""

    docker_compose = """
# docker-compose.yml - Kafka setup for testing
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: true

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    """

    print("\n🐳 Docker Compose configuration for Kafka:")
    print("   Save this to docker-compose.yml and run 'docker-compose up'")
    print("   Kafka will be available at localhost:9092")
    print("   Kafka UI will be available at http://localhost:8080")
    print(docker_compose)


def create_test_messages():
    """Create a script to send test messages"""

    test_script = '''#!/usr/bin/env python3
"""
Send test messages to Kafka for the Pythia example
"""

import json
import time
from confluent_kafka import Producer
from datetime import datetime

def create_producer():
    config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'pythia-test-producer'
    }
    return Producer(config)

def send_test_messages():
    producer = create_producer()

    test_messages = [
        {
            "id": "approval-001",
            "status": "approved",
            "user_id": "user123",
            "amount": 150.00,
            "timestamp": datetime.now().isoformat()
        },
        {
            "id": "approval-002",
            "status": "rejected",
            "user_id": "user456",
            "amount": 2500.00,
            "timestamp": datetime.now().isoformat()
        },
        {
            "id": "approval-003",
            "status": "approved",
            "user_id": "user789",
            "amount": 75.50,
            "timestamp": datetime.now().isoformat()
        }
    ]

    for i, message in enumerate(test_messages):
        key = message["id"]
        value = json.dumps(message)

        producer.produce(
            topic="approvals",
            key=key,
            value=value,
            callback=lambda err, msg: print(f"Message delivered: {msg.topic()}:{msg.partition()}:{msg.offset()}" if not err else f"Error: {err}")
        )

        print(f"Sent message {i+1}: {message['id']} ({message['status']})")
        time.sleep(1)

    producer.flush()
    print("All messages sent!")

if __name__ == "__main__":
    send_test_messages()
'''

    with open("send_test_messages.py", "w") as f:
        f.write(test_script)

    print("📝 Created send_test_messages.py for testing")


def main():
    """Main setup function"""

    print("🐍 Pythia Framework - Example Setup")
    print("=" * 50)

    setup_pythia_path()
    setup_environment()

    if not check_dependencies():
        return 1

    create_test_messages()
    print_docker_compose()

    print("\n🚀 Setup complete! Next steps:")
    print("   1. Start Kafka: docker-compose up -d")
    print("   2. Run the example: python example_worker.py")
    print("   3. Send test messages: python send_test_messages.py")
    print("   4. Check Kafka UI: http://localhost:8080")

    return 0


if __name__ == "__main__":
    sys.exit(main())
