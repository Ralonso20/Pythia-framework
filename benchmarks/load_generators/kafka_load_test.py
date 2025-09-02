#!/usr/bin/env python3
"""
Kafka Load Testing Module

High-performance load testing for Kafka broker with Pythia framework.
"""

import asyncio
import time
import json
import statistics
from typing import Dict, Any
from dataclasses import dataclass
import random
import string
import multiprocessing as mp

# Kafka imports
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient


@dataclass
class KafkaLoadConfig:
    """Configuration for Kafka load testing"""

    bootstrap_servers: str
    topic: str
    message_rate: int
    message_size: int
    duration: int
    num_workers: int
    batch_size: int = 100
    compression_type: str = "gzip"
    acks: str = "1"  # 0, 1, or 'all'


class KafkaLoadTest:
    """Kafka-specific load testing implementation"""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        message_rate: int,
        message_size: int,
        duration: int,
        num_workers: int,
    ):
        self.config = KafkaLoadConfig(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            message_rate=message_rate,
            message_size=message_size,
            duration=duration,
            num_workers=num_workers,
        )

        self.latency_samples = []
        self.error_count = 0
        self.total_messages = 0

    def _delivery_callback(self, err, msg):
        """Callback for producer delivery reports"""
        if err is not None:
            self.error_count += 1
            print(f"❌ Message delivery failed: {err}")
        else:
            self.total_messages += 1
            if self.total_messages % 1000 == 0:
                worker_id = getattr(msg, "partition", lambda: 0)()
                print(f"📤 Sent {self.total_messages} messages (Worker {worker_id})")

    def generate_test_message(self) -> Dict[str, Any]:
        """Generate a test message with specified size"""
        # Create message with timestamp for latency measurement
        message_id = f"msg_{time.time_ns()}_{random.randint(1000, 9999)}"
        timestamp = time.time_ns()

        # Create payload to reach desired size
        payload_size = (
            self.config.message_size
            - len(json.dumps({"id": message_id, "timestamp": timestamp, "payload": ""}))
            - 10
        )  # Buffer for JSON overhead

        payload = "".join(
            random.choices(string.ascii_letters + string.digits, k=max(1, payload_size))
        )

        return {
            "id": message_id,
            "timestamp": timestamp,
            "payload": payload,
            "worker_id": mp.current_process().pid,
            "test_type": "kafka_load_test",
        }

    async def run(self) -> Dict[str, Any]:
        """Run the complete Kafka load test"""
        print("🚀 Starting Kafka load test...")
        print(f"   Topic: {self.config.topic}")
        print(f"   Rate: {self.config.message_rate} msg/sec")
        print(f"   Duration: {self.config.duration}s")
        print(f"   Workers: {self.config.num_workers}")

        start_time = time.time()

        # Setup Kafka topic if needed
        await self._setup_kafka_topic()

        # Run producer and consumer concurrently
        producer_task = asyncio.create_task(self._run_producers())
        consumer_task = asyncio.create_task(self._run_consumers())

        # Wait for test duration
        await asyncio.sleep(self.config.duration)

        # Stop tasks
        producer_task.cancel()
        consumer_task.cancel()

        try:
            await producer_task
        except asyncio.CancelledError:
            pass

        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        end_time = time.time()
        _duration = end_time - start_time

        # Calculate results
        avg_latency = (
            statistics.mean(self.latency_samples) if self.latency_samples else 0
        )
        p95_latency = (
            statistics.quantiles(self.latency_samples, n=20)[18]
            if len(self.latency_samples) > 10
            else avg_latency
        )
        p99_latency = (
            statistics.quantiles(self.latency_samples, n=100)[98]
            if len(self.latency_samples) > 50
            else avg_latency
        )

        # Get Kafka metrics
        broker_metrics = await self._get_kafka_metrics()

        return {
            "total_messages": self.total_messages,
            "avg_latency_ms": avg_latency / 1_000_000,  # Convert ns to ms
            "p95_latency_ms": p95_latency / 1_000_000,
            "p99_latency_ms": p99_latency / 1_000_000,
            "error_count": self.error_count,
            "broker_metrics": broker_metrics,
        }

    async def _setup_kafka_topic(self):
        """Ensure Kafka topic exists with proper configuration"""
        from confluent_kafka.admin import NewTopic

        try:
            admin_client = AdminClient(
                {
                    "bootstrap.servers": self.config.bootstrap_servers,
                    "client.id": "pythia_benchmark_admin",
                }
            )

            # Create topic if it doesn't exist
            topic_configs = {
                "compression.type": "gzip",
                "min.insync.replicas": "1",
                "segment.ms": "600000",  # 10 minutes
                "retention.ms": "3600000",  # 1 hour for benchmarks
            }

            topic = NewTopic(
                topic=self.config.topic,
                num_partitions=6,  # Good for parallelism
                replication_factor=1,
                config=topic_configs,
            )

            # Create topics
            fs = admin_client.create_topics([topic])

            # Wait for the result
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    print(f"✅ Created Kafka topic: {topic}")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"⚠️  Topic creation warning: {e}")

        except Exception as e:
            print(f"⚠️  Kafka topic setup warning: {e}")

    async def _run_producers(self):
        """Run message producers"""
        tasks = []
        messages_per_worker = self.config.message_rate // self.config.num_workers

        for worker_id in range(self.config.num_workers):
            task = asyncio.create_task(
                self._producer_worker(worker_id, messages_per_worker)
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _producer_worker(self, worker_id: int, message_rate: int):
        """Individual producer worker"""
        producer_config = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "compression.type": self.config.compression_type,
            "acks": str(self.config.acks),
            "batch.size": 16384,  # 16KB batches
            "linger.ms": 5,  # Wait up to 5ms for batching
            "queue.buffering.max.memory": 67108864,  # 64MB buffer
            "message.max.bytes": 1048576,  # 1MB max request
            "client.id": f"pythia_producer_{worker_id}",
        }

        producer = Producer(producer_config)

        interval = 1.0 / message_rate if message_rate > 0 else 0.1

        try:
            while True:
                message = self.generate_test_message()

                try:
                    # Send message asynchronously
                    producer.produce(
                        self.config.topic,
                        key=str(message["id"]),
                        value=json.dumps(message),
                        callback=self._delivery_callback,
                    )

                    # Poll for delivery callbacks
                    producer.poll(0)  # Non-blocking

                except Exception as e:
                    self.error_count += 1
                    print(f"❌ Producer error: {e}")

                # Control rate
                if interval > 0:
                    await asyncio.sleep(interval)

        except asyncio.CancelledError:
            pass
        finally:
            # Flush remaining messages
            producer.flush()

    async def _run_consumers(self):
        """Run message consumers for latency measurement"""
        consumer_config = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "group.id": "pythia_benchmark_group",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "fetch.min.bytes": 1,
            "client.id": "pythia_consumer",
        }

        consumer = Consumer(consumer_config)
        consumer.subscribe([self.config.topic])

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"❌ Consumer error: {msg.error()}")
                    continue

                try:
                    # Calculate latency
                    message_data = json.loads(msg.value().decode("utf-8"))
                    sent_timestamp = message_data.get("timestamp", 0)

                    if sent_timestamp > 0:
                        latency_ns = time.time_ns() - sent_timestamp
                        self.latency_samples.append(latency_ns)

                        # Keep only recent samples to avoid memory issues
                        if len(self.latency_samples) > 10000:
                            self.latency_samples = self.latency_samples[-5000:]

                except Exception as e:
                    print(f"❌ Consumer error: {e}")
                    self.error_count += 1

        except asyncio.CancelledError:
            pass
        finally:
            consumer.close()

    async def _get_kafka_metrics(self) -> Dict[str, Any]:
        """Get Kafka broker metrics"""
        try:
            # This would typically connect to Kafka JMX or use kafka-python admin client
            # For now, return basic metrics
            return {
                "broker_count": 1,
                "topic": self.config.topic,
                "partitions": 6,
                "replication_factor": 1,
                "compression": self.config.compression_type,
            }
        except Exception as e:
            print(f"⚠️  Could not fetch Kafka metrics: {e}")
            return {}


if __name__ == "__main__":
    # Example usage
    async def main():
        load_test = KafkaLoadTest(
            bootstrap_servers="localhost:9092",
            topic="benchmark-topic",
            message_rate=1000,
            message_size=1024,
            duration=30,
            num_workers=2,
        )

        result = await load_test.run()

        print("\n📊 Kafka Load Test Results:")
        print(f"   Total messages: {result['total_messages']}")
        print(f"   Avg latency: {result['avg_latency_ms']:.2f}ms")
        print(f"   P95 latency: {result['p95_latency_ms']:.2f}ms")
        print(f"   Error count: {result['error_count']}")

    asyncio.run(main())
