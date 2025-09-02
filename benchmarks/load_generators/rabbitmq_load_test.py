#!/usr/bin/env python3
"""
RabbitMQ Load Testing Module

High-performance load testing for RabbitMQ broker with Pythia framework.
"""

import asyncio
import time
import json
import statistics
from typing import Dict, Any
from dataclasses import dataclass
import random
import string
import aio_pika
from aio_pika import ExchangeType, DeliveryMode
from aio_pika.exceptions import AMQPException


@dataclass
class RabbitMQLoadConfig:
    """Configuration for RabbitMQ load testing"""

    connection_url: str
    queue: str
    exchange: str
    routing_key: str
    message_rate: int
    message_size: int
    duration: int
    num_workers: int
    prefetch_count: int = 10
    durable: bool = True


class RabbitMQLoadTest:
    """RabbitMQ-specific load testing implementation"""

    def __init__(
        self,
        connection_url: str,
        queue: str,
        message_rate: int,
        message_size: int,
        duration: int,
        num_workers: int,
    ):
        self.config = RabbitMQLoadConfig(
            connection_url=connection_url,
            queue=queue,
            exchange=f"{queue}_exchange",
            routing_key=queue,
            message_rate=message_rate,
            message_size=message_size,
            duration=duration,
            num_workers=num_workers,
        )

        self.latency_samples = []
        self.error_count = 0
        self.total_messages = 0

    def generate_test_message(self) -> Dict[str, Any]:
        """Generate a test message with specified size"""
        message_id = f"msg_{time.time_ns()}_{random.randint(1000, 9999)}"
        timestamp = time.time_ns()

        # Calculate payload size
        base_message = {"id": message_id, "timestamp": timestamp, "payload": ""}

        base_size = len(json.dumps(base_message))
        payload_size = max(1, self.config.message_size - base_size - 10)

        payload = "".join(
            random.choices(string.ascii_letters + string.digits, k=payload_size)
        )

        return {
            "id": message_id,
            "timestamp": timestamp,
            "payload": payload,
            "test_type": "rabbitmq_load_test",
            "priority": random.choice([1, 5, 10]),  # Message priorities
        }

    async def run(self) -> Dict[str, Any]:
        """Run the complete RabbitMQ load test"""
        print("🚀 Starting RabbitMQ load test...")
        print(f"   Queue: {self.config.queue}")
        print(f"   Rate: {self.config.message_rate} msg/sec")
        print(f"   Duration: {self.config.duration}s")
        print(f"   Workers: {self.config.num_workers}")

        start_time = time.time()

        # Setup RabbitMQ infrastructure
        await self._setup_rabbitmq()

        # Run producers and consumers concurrently
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

        # Get RabbitMQ metrics
        broker_metrics = await self._get_rabbitmq_metrics()

        return {
            "total_messages": self.total_messages,
            "avg_latency_ms": avg_latency / 1_000_000,  # Convert ns to ms
            "p95_latency_ms": p95_latency / 1_000_000,
            "p99_latency_ms": p99_latency / 1_000_000,
            "error_count": self.error_count,
            "broker_metrics": broker_metrics,
        }

    async def _setup_rabbitmq(self):
        """Setup RabbitMQ queues and exchanges"""
        try:
            connection = await aio_pika.connect_robust(self.config.connection_url)
            channel = await connection.channel()

            # Set QoS for performance
            await channel.set_qos(prefetch_count=self.config.prefetch_count)

            # Declare exchange
            exchange = await channel.declare_exchange(
                self.config.exchange, ExchangeType.DIRECT, durable=self.config.durable
            )

            # Declare queue with performance optimizations
            queue = await channel.declare_queue(
                self.config.queue,
                durable=self.config.durable,
                arguments={
                    "x-max-priority": 10,  # Enable priority queues
                    "x-queue-mode": "lazy",  # Use lazy queues for better memory usage
                },
            )

            # Bind queue to exchange
            await queue.bind(exchange, routing_key=self.config.routing_key)

            await connection.close()
            print(f"✅ RabbitMQ setup complete: {self.config.queue}")

        except Exception as e:
            print(f"❌ RabbitMQ setup error: {e}")
            raise

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
        connection = None

        try:
            connection = await aio_pika.connect_robust(self.config.connection_url)
            channel = await connection.channel()

            exchange = await channel.declare_exchange(
                self.config.exchange, ExchangeType.DIRECT, durable=self.config.durable
            )

            interval = 1.0 / message_rate if message_rate > 0 else 0.1

            while True:
                message_data = self.generate_test_message()

                try:
                    # Create AMQP message
                    message = aio_pika.Message(
                        json.dumps(message_data).encode(),
                        delivery_mode=DeliveryMode.PERSISTENT
                        if self.config.durable
                        else DeliveryMode.NOT_PERSISTENT,
                        priority=message_data.get("priority", 5),
                        timestamp=time.time(),
                    )

                    # Publish message
                    await exchange.publish(message, routing_key=self.config.routing_key)

                    self.total_messages += 1

                    if self.total_messages % 1000 == 0:
                        print(
                            f"📤 Sent {self.total_messages} messages (Worker {worker_id})"
                        )

                except AMQPException as e:
                    self.error_count += 1
                    print(f"❌ Producer error (Worker {worker_id}): {e}")

                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"❌ Producer worker {worker_id} error: {e}")
        finally:
            if connection:
                await connection.close()

    async def _run_consumers(self):
        """Run message consumers for latency measurement"""
        connection = None

        try:
            connection = await aio_pika.connect_robust(self.config.connection_url)
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self.config.prefetch_count)

            queue = await channel.declare_queue(
                self.config.queue, durable=self.config.durable
            )

            async def process_message(message: aio_pika.IncomingMessage):
                try:
                    async with message.process():
                        # Parse message
                        message_data = json.loads(message.body.decode())
                        sent_timestamp = message_data.get("timestamp", 0)

                        if sent_timestamp > 0:
                            latency_ns = time.time_ns() - sent_timestamp
                            self.latency_samples.append(latency_ns)

                            # Keep only recent samples
                            if len(self.latency_samples) > 10000:
                                self.latency_samples = self.latency_samples[-5000:]

                except Exception as e:
                    self.error_count += 1
                    print(f"❌ Consumer error: {e}")

            # Start consuming
            await queue.consume(process_message)

            # Keep consuming until cancelled
            while True:
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        finally:
            if connection:
                await connection.close()

    async def _get_rabbitmq_metrics(self) -> Dict[str, Any]:
        """Get RabbitMQ broker metrics"""
        try:
            # Connect to RabbitMQ Management API
            import aiohttp

            async with aiohttp.ClientSession() as session:
                # Get queue info
                async with session.get(
                    f"http://localhost:15672/api/queues/%2F/{self.config.queue}",
                    auth=aiohttp.BasicAuth("guest", "guest"),
                ) as response:
                    if response.status == 200:
                        queue_info = await response.json()
                        return {
                            "queue_name": self.config.queue,
                            "messages": queue_info.get("messages", 0),
                            "messages_ready": queue_info.get("messages_ready", 0),
                            "messages_unacknowledged": queue_info.get(
                                "messages_unacknowledged", 0
                            ),
                            "consumers": queue_info.get("consumers", 0),
                            "memory": queue_info.get("memory", 0),
                            "message_stats": queue_info.get("message_stats", {}),
                        }
        except Exception as e:
            print(f"⚠️  Could not fetch RabbitMQ metrics: {e}")

        return {
            "queue_name": self.config.queue,
            "exchange": self.config.exchange,
            "routing_key": self.config.routing_key,
            "durable": self.config.durable,
        }


if __name__ == "__main__":
    # Example usage
    async def main():
        load_test = RabbitMQLoadTest(
            connection_url="amqp://guest:guest@localhost:5672",
            queue="benchmark-queue",
            message_rate=1000,
            message_size=1024,
            duration=30,
            num_workers=2,
        )

        result = await load_test.run()

        print("\n📊 RabbitMQ Load Test Results:")
        print(f"   Total messages: {result['total_messages']}")
        print(f"   Avg latency: {result['avg_latency_ms']:.2f}ms")
        print(f"   P95 latency: {result['p95_latency_ms']:.2f}ms")
        print(f"   Error count: {result['error_count']}")

    asyncio.run(main())
