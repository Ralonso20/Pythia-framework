"""
GCP Pub/Sub Benchmark using Google Cloud Pub/Sub Emulator
"""

import asyncio
import time
import json
import uuid
from typing import List, Tuple
from google.cloud import pubsub_v1
from google.api_core import exceptions
import os

from .base import CloudBenchmark, BenchmarkConfig


class GCPBenchmark(CloudBenchmark):
    """GCP Pub/Sub benchmark using Pub/Sub emulator Docker container"""

    def __init__(self):
        super().__init__("GCP Pub/Sub")
        self.publisher = None
        self.subscriber = None
        self.project_id = "benchmark-project"
        self.topic_name = None
        self.subscription_name = None
        self.topic_path = None
        self.subscription_path = None
        self.emulator_container = "pubsub-emulator-benchmark"

    async def setup_infrastructure(self, config: BenchmarkConfig) -> bool:
        """Setup Pub/Sub emulator container and create topic/subscription"""
        try:
            # Stop and remove existing container
            await self._run_docker_command([
                "docker", "rm", "-f", self.emulator_container
            ], ignore_errors=True)

            # Start Pub/Sub emulator container
            print("🐳 Starting GCP Pub/Sub emulator...")
            cmd = [
                "docker", "run", "-d",
                "--name", self.emulator_container,
                "-p", "8085:8085",
                "google/cloud-sdk:latest",
                "gcloud", "beta", "emulators", "pubsub", "start",
                "--host-port=0.0.0.0:8085",
                "--project=" + self.project_id
            ]

            if not await self._run_docker_command(cmd):
                return False

            self.docker_containers = [self.emulator_container]

            # Wait for emulator to be ready
            print("⏳ Waiting for Pub/Sub emulator to be ready...")
            await asyncio.sleep(10)

            # Set environment variable for emulator
            os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

            # Check if emulator is responding
            for attempt in range(30):
                try:
                    # Initialize clients
                    self.publisher = pubsub_v1.PublisherClient()
                    self.subscriber = pubsub_v1.SubscriberClient()

                    # Test connection by listing topics
                    parent = f"projects/{self.project_id}"
                    list(self.publisher.list_topics(request={"project": parent}))
                    print("✅ Pub/Sub emulator is ready!")
                    break
                except Exception as e:
                    if attempt == 29:
                        print(f"❌ Pub/Sub emulator failed to start: {e}")
                        return False
                    await asyncio.sleep(2)

            # Create topic and subscription
            self.topic_name = config.topic_name
            self.subscription_name = f"{config.topic_name}-subscription"

            self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
            self.subscription_path = self.subscriber.subscription_path(
                self.project_id, self.subscription_name
            )

            print(f"🏗️ Creating topic: {self.topic_name}")
            try:
                self.publisher.create_topic(request={"name": self.topic_path})
            except exceptions.AlreadyExists:
                pass

            print(f"🏗️ Creating subscription: {self.subscription_name}")
            try:
                self.subscriber.create_subscription(
                    request={
                        "name": self.subscription_path,
                        "topic": self.topic_path,
                        "ack_deadline_seconds": 60,
                    }
                )
            except exceptions.AlreadyExists:
                pass

            print("✅ GCP Pub/Sub infrastructure ready")
            return True

        except Exception as e:
            print(f"❌ Failed to setup GCP infrastructure: {e}")
            return False

    async def cleanup_infrastructure(self):
        """Stop and remove Pub/Sub emulator container"""
        try:
            print("🧹 Cleaning up GCP infrastructure...")

            # Clean up topics and subscriptions
            if self.subscription_path and self.subscriber:
                try:
                    self.subscriber.delete_subscription(request={"subscription": self.subscription_path})
                except:
                    pass

            if self.topic_path and self.publisher:
                try:
                    self.publisher.delete_topic(request={"topic": self.topic_path})
                except:
                    pass

            # Remove emulator environment variable
            if "PUBSUB_EMULATOR_HOST" in os.environ:
                del os.environ["PUBSUB_EMULATOR_HOST"]

            # Stop container
            if self.emulator_container in self.docker_containers:
                await self._run_docker_command([
                    "docker", "rm", "-f", self.emulator_container
                ], ignore_errors=True)

            print("✅ GCP cleanup completed")
        except Exception as e:
            print(f"⚠️ GCP cleanup error: {e}")

    async def send_messages(self, config: BenchmarkConfig) -> Tuple[int, float, List[float]]:
        """Send messages to Pub/Sub topic"""
        sent_count = 0
        latencies = []

        # Prepare messages
        messages = []
        for i in range(config.message_count):
            message_data = {
                'id': str(uuid.uuid4()),
                'timestamp': time.time(),
                'sequence': i,
                'data': 'x' * (config.message_size - 100)  # Account for metadata
            }
            messages.append(json.dumps(message_data).encode('utf-8'))

        # Send messages concurrently
        start_time = time.time()

        async def send_batch(batch_messages, producer_id):
            nonlocal sent_count
            batch_sent = 0

            for message_data in batch_messages:
                try:
                    publish_start = time.time()

                    # Publish message (this is synchronous in the client)
                    def _publish():
                        return self.publisher.publish(self.topic_path, message_data)

                    loop = asyncio.get_event_loop()
                    future = await loop.run_in_executor(None, _publish)

                    # Wait for publish to complete
                    def _wait_for_result():
                        return future.result()

                    await loop.run_in_executor(None, _wait_for_result)

                    publish_latency = (time.time() - publish_start) * 1000
                    latencies.append(publish_latency)
                    batch_sent += 1

                except Exception as e:
                    print(f"❌ Publish error in producer {producer_id}: {e}")

            sent_count += batch_sent

        # Divide messages among producers
        messages_per_producer = len(messages) // config.concurrent_producers
        producer_tasks = []

        for i in range(config.concurrent_producers):
            start_idx = i * messages_per_producer
            if i == config.concurrent_producers - 1:
                # Last producer gets remaining messages
                batch_messages = messages[start_idx:]
            else:
                end_idx = start_idx + messages_per_producer
                batch_messages = messages[start_idx:end_idx]

            producer_tasks.append(send_batch(batch_messages, i))

        await asyncio.gather(*producer_tasks)

        duration = time.time() - start_time
        return sent_count, duration, latencies

    async def receive_messages(self, config: BenchmarkConfig) -> Tuple[int, float, List[float]]:
        """Receive messages from Pub/Sub subscription"""
        received_count = 0
        latencies = []
        start_time = time.time()

        # Create consumer tasks
        async def consume_messages(consumer_id: int):
            nonlocal received_count
            consumer_received = 0
            target_messages = config.message_count // config.concurrent_consumers

            while consumer_received < target_messages:
                try:
                    receive_start = time.time()

                    # Pull messages (this is synchronous)
                    def _pull_messages():
                        return self.subscriber.pull(
                            request={
                                "subscription": self.subscription_path,
                                "max_messages": min(10, config.batch_size),
                                "allow_excess_messages": False,
                            },
                            timeout=5.0
                        )

                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(None, _pull_messages)

                    if not response.received_messages:
                        await asyncio.sleep(0.1)
                        continue

                    # Process messages
                    ack_ids = []
                    receipt_time = time.time()

                    for received_message in response.received_messages:
                        try:
                            message_data = json.loads(received_message.message.data.decode('utf-8'))
                            send_timestamp = message_data.get('timestamp', receipt_time)
                            latency = (receipt_time - send_timestamp) * 1000  # Convert to ms
                            latencies.append(latency)

                            ack_ids.append(received_message.ack_id)
                            consumer_received += 1

                        except Exception as e:
                            print(f"⚠️ Message processing error in consumer {consumer_id}: {e}")

                    # Acknowledge messages
                    if ack_ids:
                        def _acknowledge():
                            self.subscriber.acknowledge(
                                request={
                                    "subscription": self.subscription_path,
                                    "ack_ids": ack_ids,
                                }
                            )

                        await loop.run_in_executor(None, _acknowledge)

                except exceptions.DeadlineExceeded:
                    # Timeout is expected when no messages are available
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"❌ Consumer {consumer_id} error: {e}")
                    await asyncio.sleep(1)

            received_count += consumer_received

        # Start consumers
        consumer_tasks = [
            consume_messages(i) for i in range(config.concurrent_consumers)
        ]

        # Wait for all consumers to finish or timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*consumer_tasks),
                timeout=config.timeout
            )
        except asyncio.TimeoutError:
            print("⚠️ Consumer timeout reached")

        duration = time.time() - start_time
        return received_count, duration, latencies

    async def _run_docker_command(self, cmd: List[str], ignore_errors: bool = False) -> bool:
        """Run a Docker command asynchronously"""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0 and not ignore_errors:
                print(f"❌ Docker command failed: {' '.join(cmd)}")
                print(f"   Error: {stderr.decode()}")
                return False

            return True

        except Exception as e:
            if not ignore_errors:
                print(f"❌ Docker command exception: {e}")
            return False
