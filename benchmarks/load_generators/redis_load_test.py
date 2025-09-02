#!/usr/bin/env python3
"""
Redis Load Testing Module

High-performance load testing for Redis broker with Pythia framework.
"""

import asyncio
import time
import json
import statistics
from typing import Dict, Any, Optional
from dataclasses import dataclass
import random
import string
import redis.asyncio as redis
from redis.exceptions import RedisError


@dataclass
class RedisLoadConfig:
    """Configuration for Redis load testing"""

    redis_url: str
    queue: str
    stream_name: Optional[str] = None
    message_rate: int = 1000
    message_size: int = 1024
    duration: int = 60
    num_workers: int = 1
    test_type: str = "lists"  # 'lists', 'streams', 'pubsub'
    max_connections: int = 10


class RedisLoadTest:
    """Redis-specific load testing implementation"""

    def __init__(
        self,
        redis_url: str,
        queue: str,
        message_rate: int,
        message_size: int,
        duration: int,
        num_workers: int,
        test_type: str = "lists",
    ):
        self.config = RedisLoadConfig(
            redis_url=redis_url,
            queue=queue,
            stream_name=f"{queue}_stream",
            message_rate=message_rate,
            message_size=message_size,
            duration=duration,
            num_workers=num_workers,
            test_type=test_type,
        )

        self.latency_samples = []
        self.error_count = 0
        self.total_messages = 0
        self.redis_pool = None

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
            "test_type": f"redis_{self.config.test_type}_test",
            "sequence": self.total_messages,
        }

    async def run(self) -> Dict[str, Any]:
        """Run the complete Redis load test"""
        print(f"🚀 Starting Redis {self.config.test_type} load test...")
        print(f"   Queue: {self.config.queue}")
        print(f"   Rate: {self.config.message_rate} msg/sec")
        print(f"   Duration: {self.config.duration}s")
        print(f"   Workers: {self.config.num_workers}")

        # Setup Redis connection pool
        await self._setup_redis()

        start_time = time.time()

        try:
            if self.config.test_type == "lists":
                _result = await self._run_lists_test()
            elif self.config.test_type == "streams":
                _result = await self._run_streams_test()
            elif self.config.test_type == "pubsub":
                _result = await self._run_pubsub_test()
            else:
                raise ValueError(f"Unsupported test type: {self.config.test_type}")

        finally:
            await self._cleanup_redis()

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

        # Get Redis metrics
        broker_metrics = await self._get_redis_metrics()

        return {
            "total_messages": self.total_messages,
            "avg_latency_ms": avg_latency / 1_000_000,  # Convert ns to ms
            "p95_latency_ms": p95_latency / 1_000_000,
            "p99_latency_ms": p99_latency / 1_000_000,
            "error_count": self.error_count,
            "broker_metrics": broker_metrics,
        }

    async def _setup_redis(self):
        """Setup Redis connection pool"""
        try:
            self.redis_pool = redis.ConnectionPool.from_url(
                self.config.redis_url,
                max_connections=self.config.max_connections,
                decode_responses=True,
            )

            # Test connection
            r = redis.Redis(connection_pool=self.redis_pool)
            await r.ping()
            await r.close()

            print("✅ Redis connection pool ready")

        except Exception as e:
            print(f"❌ Redis setup error: {e}")
            raise

    async def _cleanup_redis(self):
        """Cleanup Redis connections"""
        if self.redis_pool:
            await self.redis_pool.disconnect()

    async def _run_lists_test(self) -> Dict[str, Any]:
        """Run Redis Lists (LPUSH/BRPOP) performance test"""
        # Run producers and consumers concurrently
        producer_task = asyncio.create_task(self._run_lists_producers())
        consumer_task = asyncio.create_task(self._run_lists_consumers())

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

        return {}

    async def _run_lists_producers(self):
        """Run Redis Lists producers"""
        tasks = []
        messages_per_worker = self.config.message_rate // self.config.num_workers

        for worker_id in range(self.config.num_workers):
            task = asyncio.create_task(
                self._lists_producer_worker(worker_id, messages_per_worker)
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _lists_producer_worker(self, worker_id: int, message_rate: int):
        """Individual Redis Lists producer worker"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            interval = 1.0 / message_rate if message_rate > 0 else 0.1

            while True:
                message_data = self.generate_test_message()

                try:
                    # Use LPUSH for atomic insertion
                    await r.lpush(self.config.queue, json.dumps(message_data))

                    self.total_messages += 1

                    if self.total_messages % 1000 == 0:
                        print(
                            f"📤 Sent {self.total_messages} messages (Worker {worker_id})"
                        )

                except RedisError as e:
                    self.error_count += 1
                    print(f"❌ Producer error (Worker {worker_id}): {e}")

                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            pass
        finally:
            await r.close()

    async def _run_lists_consumers(self):
        """Run Redis Lists consumers"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            while True:
                try:
                    # Use BRPOP with timeout for blocking pop
                    result = await r.brpop(self.config.queue, timeout=1)

                    if result:
                        _, message_json = result
                        message_data = json.loads(message_json)

                        # Calculate latency
                        sent_timestamp = message_data.get("timestamp", 0)
                        if sent_timestamp > 0:
                            latency_ns = time.time_ns() - sent_timestamp
                            self.latency_samples.append(latency_ns)

                            # Keep only recent samples
                            if len(self.latency_samples) > 10000:
                                self.latency_samples = self.latency_samples[-5000:]

                except RedisError as e:
                    self.error_count += 1
                    print(f"❌ Consumer error: {e}")
                except asyncio.TimeoutError:
                    continue

        except asyncio.CancelledError:
            pass
        finally:
            await r.close()

    async def _run_streams_test(self) -> Dict[str, Any]:
        """Run Redis Streams (XADD/XREAD) performance test"""
        # Setup stream consumer group
        await self._setup_stream_consumer_group()

        # Run producers and consumers
        producer_task = asyncio.create_task(self._run_streams_producers())
        consumer_task = asyncio.create_task(self._run_streams_consumers())

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

        return {}

    async def _setup_stream_consumer_group(self):
        """Setup Redis Streams consumer group"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            # Create consumer group
            await r.xgroup_create(
                self.config.stream_name, "benchmark_group", id="0", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                print(f"❌ Stream group creation error: {e}")
        finally:
            await r.close()

    async def _run_streams_producers(self):
        """Run Redis Streams producers"""
        tasks = []
        messages_per_worker = self.config.message_rate // self.config.num_workers

        for worker_id in range(self.config.num_workers):
            task = asyncio.create_task(
                self._streams_producer_worker(worker_id, messages_per_worker)
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _streams_producer_worker(self, worker_id: int, message_rate: int):
        """Individual Redis Streams producer worker"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            interval = 1.0 / message_rate if message_rate > 0 else 0.1

            while True:
                message_data = self.generate_test_message()

                try:
                    # Use XADD to add to stream
                    await r.xadd(self.config.stream_name, message_data)

                    self.total_messages += 1

                    if self.total_messages % 1000 == 0:
                        print(
                            f"📤 Sent {self.total_messages} messages (Worker {worker_id})"
                        )

                except RedisError as e:
                    self.error_count += 1
                    print(f"❌ Streams producer error (Worker {worker_id}): {e}")

                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            pass
        finally:
            await r.close()

    async def _run_streams_consumers(self):
        """Run Redis Streams consumers"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            consumer_name = f"consumer_{random.randint(1000, 9999)}"

            while True:
                try:
                    # Read from stream using consumer group
                    messages = await r.xreadgroup(
                        "benchmark_group",
                        consumer_name,
                        {self.config.stream_name: ">"},
                        count=10,
                        block=1000,
                    )

                    for stream_name, msgs in messages:
                        for msg_id, fields in msgs:
                            try:
                                # Calculate latency
                                sent_timestamp = int(fields.get("timestamp", 0))
                                if sent_timestamp > 0:
                                    latency_ns = time.time_ns() - sent_timestamp
                                    self.latency_samples.append(latency_ns)

                                    # Keep only recent samples
                                    if len(self.latency_samples) > 10000:
                                        self.latency_samples = self.latency_samples[
                                            -5000:
                                        ]

                                # Acknowledge message
                                await r.xack(
                                    self.config.stream_name, "benchmark_group", msg_id
                                )

                            except Exception as e:
                                self.error_count += 1
                                print(f"❌ Message processing error: {e}")

                except RedisError as e:
                    self.error_count += 1
                    print(f"❌ Streams consumer error: {e}")

        except asyncio.CancelledError:
            pass
        finally:
            await r.close()

    async def _run_pubsub_test(self) -> Dict[str, Any]:
        """Run Redis Pub/Sub performance test"""
        # Run publishers and subscribers
        publisher_task = asyncio.create_task(self._run_pubsub_publishers())
        subscriber_task = asyncio.create_task(self._run_pubsub_subscribers())

        # Wait for test duration
        await asyncio.sleep(self.config.duration)

        # Stop tasks
        publisher_task.cancel()
        subscriber_task.cancel()

        try:
            await publisher_task
        except asyncio.CancelledError:
            pass

        try:
            await subscriber_task
        except asyncio.CancelledError:
            pass

        return {}

    async def _run_pubsub_publishers(self):
        """Run Redis Pub/Sub publishers"""
        tasks = []
        messages_per_worker = self.config.message_rate // self.config.num_workers

        for worker_id in range(self.config.num_workers):
            task = asyncio.create_task(
                self._pubsub_publisher_worker(worker_id, messages_per_worker)
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _pubsub_publisher_worker(self, worker_id: int, message_rate: int):
        """Individual Redis Pub/Sub publisher worker"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            interval = 1.0 / message_rate if message_rate > 0 else 0.1
            channel = f"{self.config.queue}_channel"

            while True:
                message_data = self.generate_test_message()

                try:
                    # Publish message
                    await r.publish(channel, json.dumps(message_data))

                    self.total_messages += 1

                    if self.total_messages % 1000 == 0:
                        print(
                            f"📤 Published {self.total_messages} messages (Worker {worker_id})"
                        )

                except RedisError as e:
                    self.error_count += 1
                    print(f"❌ Publisher error (Worker {worker_id}): {e}")

                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            pass
        finally:
            await r.close()

    async def _run_pubsub_subscribers(self):
        """Run Redis Pub/Sub subscribers"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            pubsub = r.pubsub()
            channel = f"{self.config.queue}_channel"
            await pubsub.subscribe(channel)

            while True:
                try:
                    message = await pubsub.get_message(timeout=1.0)

                    if message and message["type"] == "message":
                        message_data = json.loads(message["data"])

                        # Calculate latency
                        sent_timestamp = message_data.get("timestamp", 0)
                        if sent_timestamp > 0:
                            latency_ns = time.time_ns() - sent_timestamp
                            self.latency_samples.append(latency_ns)

                            # Keep only recent samples
                            if len(self.latency_samples) > 10000:
                                self.latency_samples = self.latency_samples[-5000:]

                except Exception as e:
                    self.error_count += 1
                    print(f"❌ Subscriber error: {e}")

        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.close()
            await r.close()

    async def _get_redis_metrics(self) -> Dict[str, Any]:
        """Get Redis broker metrics"""
        r = redis.Redis(connection_pool=self.redis_pool)

        try:
            info = await r.info()

            return {
                "redis_version": info.get("redis_version", "unknown"),
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "test_type": self.config.test_type,
                "queue_name": self.config.queue,
            }

        except Exception as e:
            print(f"⚠️  Could not fetch Redis metrics: {e}")
            return {"test_type": self.config.test_type}
        finally:
            await r.close()


if __name__ == "__main__":
    # Example usage
    async def main():
        # Test Redis Lists
        load_test = RedisLoadTest(
            redis_url="redis://localhost:6379",
            queue="benchmark-queue",
            message_rate=1000,
            message_size=1024,
            duration=30,
            num_workers=2,
            test_type="lists",
        )

        result = await load_test.run()

        print("\n📊 Redis Load Test Results:")
        print(f"   Total messages: {result['total_messages']}")
        print(f"   Avg latency: {result['avg_latency_ms']:.2f}ms")
        print(f"   P95 latency: {result['p95_latency_ms']:.2f}ms")
        print(f"   Error count: {result['error_count']}")

    asyncio.run(main())
