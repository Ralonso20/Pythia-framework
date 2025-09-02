"""
AWS SQS Benchmark using LocalStack
"""

import asyncio
import time
import json
import uuid
import subprocess
from typing import List, Tuple
import boto3
from botocore.config import Config

from .base import CloudBenchmark, BenchmarkConfig


class AWSBenchmark(CloudBenchmark):
    """AWS SQS benchmark using LocalStack Docker container"""

    def __init__(self):
        super().__init__("AWS SQS")
        self.sqs_client = None
        self.queue_url = None
        self.localstack_container = "localstack-benchmark"

    async def setup_infrastructure(self, config: BenchmarkConfig) -> bool:
        """Setup LocalStack container and create SQS queue"""
        try:
            # Stop and remove existing container
            await self._run_docker_command([
                "docker", "rm", "-f", self.localstack_container
            ], ignore_errors=True)

            # Start LocalStack container
            print("🐳 Starting LocalStack container...")
            cmd = [
                "docker", "run", "-d",
                "--name", self.localstack_container,
                "-p", "4566:4566",
                "-e", "SERVICES=sqs",
                "-e", "DEBUG=1",
                "-e", "DATA_DIR=/tmp/localstack/data",
                "localstack/localstack:latest"
            ]

            if not await self._run_docker_command(cmd):
                return False

            self.docker_containers = [self.localstack_container]

            # Wait for LocalStack to be ready
            print("⏳ Waiting for LocalStack to be ready...")
            await asyncio.sleep(10)

            # Check if LocalStack is responding
            for attempt in range(30):
                try:
                    # Configure boto3 client for LocalStack
                    self.sqs_client = boto3.client(
                        'sqs',
                        endpoint_url='http://localhost:4566',
                        aws_access_key_id='test',
                        aws_secret_access_key='test',
                        region_name='us-east-1',
                        config=Config(
                            retries={'max_attempts': 0},
                            read_timeout=60,
                            connect_timeout=60
                        )
                    )

                    # Test connection
                    self.sqs_client.list_queues()
                    print("✅ LocalStack is ready!")
                    break
                except Exception as e:
                    if attempt == 29:
                        print(f"❌ LocalStack failed to start: {e}")
                        return False
                    await asyncio.sleep(2)

            # Create SQS queue
            print(f"🏗️ Creating SQS queue: {config.queue_name}")
            response = self.sqs_client.create_queue(
                QueueName=config.queue_name,
                Attributes={
                    'VisibilityTimeoutSeconds': '30',
                    'MessageRetentionPeriod': '1209600',  # 14 days
                    'ReceiveMessageWaitTimeSeconds': '0',  # Short polling
                }
            )

            self.queue_url = response['QueueUrl']
            print(f"✅ Queue created: {self.queue_url}")

            return True

        except Exception as e:
            print(f"❌ Failed to setup AWS infrastructure: {e}")
            return False

    async def cleanup_infrastructure(self):
        """Stop and remove LocalStack container"""
        try:
            print("🧹 Cleaning up AWS infrastructure...")
            if self.localstack_container in self.docker_containers:
                await self._run_docker_command([
                    "docker", "rm", "-f", self.localstack_container
                ], ignore_errors=True)
            print("✅ AWS cleanup completed")
        except Exception as e:
            print(f"⚠️ AWS cleanup error: {e}")

    async def send_messages(self, config: BenchmarkConfig) -> Tuple[int, float, List[float]]:
        """Send messages to SQS queue"""
        sent_count = 0
        latencies = []

        # Create message batches
        batches = []
        for i in range(0, config.message_count, config.batch_size):
            batch = []
            for j in range(min(config.batch_size, config.message_count - i)):
                message_body = {
                    'id': str(uuid.uuid4()),
                    'timestamp': time.time(),
                    'sequence': i + j,
                    'data': 'x' * (config.message_size - 100)  # Account for metadata
                }
                batch.append({
                    'Id': str(i + j),
                    'MessageBody': json.dumps(message_body)
                })
            batches.append(batch)

        # Send messages concurrently
        start_time = time.time()

        async def send_batch(batch):
            nonlocal sent_count
            batch_start = time.time()

            try:
                # Run in thread since boto3 is synchronous
                def _send_batch():
                    return self.sqs_client.send_message_batch(
                        QueueUrl=self.queue_url,
                        Entries=batch
                    )

                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, _send_batch)

                batch_latency = (time.time() - batch_start) * 1000  # Convert to ms
                latencies.extend([batch_latency] * len(batch))

                # Count successful messages
                successful = len(batch) - len(response.get('Failed', []))
                sent_count += successful

                if response.get('Failed'):
                    print(f"⚠️ Batch send failures: {len(response['Failed'])}")

            except Exception as e:
                print(f"❌ Batch send error: {e}")

        # Execute batches with concurrency
        semaphore = asyncio.Semaphore(config.concurrent_producers)

        async def send_with_semaphore(batch):
            async with semaphore:
                await send_batch(batch)

        await asyncio.gather(*[send_with_semaphore(batch) for batch in batches])

        duration = time.time() - start_time
        return sent_count, duration, latencies

    async def receive_messages(self, config: BenchmarkConfig) -> Tuple[int, float, List[float]]:
        """Receive messages from SQS queue"""
        received_count = 0
        latencies = []
        start_time = time.time()

        # Create consumer tasks
        async def consume_messages(consumer_id: int):
            nonlocal received_count
            consumer_received = 0

            while consumer_received < config.message_count // config.concurrent_consumers:
                try:
                    receive_start = time.time()

                    # Run receive in thread since boto3 is synchronous
                    def _receive_messages():
                        return self.sqs_client.receive_message(
                            QueueUrl=self.queue_url,
                            MaxNumberOfMessages=min(10, config.batch_size),
                            WaitTimeSeconds=1  # Short polling
                        )

                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(None, _receive_messages)

                    messages = response.get('Messages', [])
                    if not messages:
                        await asyncio.sleep(0.1)
                        continue

                    # Process messages and calculate latency
                    receipt_time = time.time()
                    for message in messages:
                        try:
                            message_body = json.loads(message['Body'])
                            send_timestamp = message_body.get('timestamp', receipt_time)
                            latency = (receipt_time - send_timestamp) * 1000  # Convert to ms
                            latencies.append(latency)

                            consumer_received += 1

                            # Delete message
                            def _delete_message():
                                self.sqs_client.delete_message(
                                    QueueUrl=self.queue_url,
                                    ReceiptHandle=message['ReceiptHandle']
                                )

                            await loop.run_in_executor(None, _delete_message)

                        except Exception as e:
                            print(f"⚠️ Message processing error: {e}")

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
