"""
Base classes for cloud queue benchmarking
"""

import time
import asyncio
import statistics
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark runs"""
    message_count: int = 1000
    message_size: int = 1024  # bytes
    concurrent_producers: int = 1
    concurrent_consumers: int = 1
    batch_size: int = 10
    timeout: int = 300  # seconds
    warmup_messages: int = 100
    queue_name: str = "benchmark-queue"
    topic_name: str = "benchmark-topic"


@dataclass
class BenchmarkResult:
    """Results from a benchmark run"""
    provider: str
    test_name: str
    config: BenchmarkConfig
    success: bool = False
    error_message: Optional[str] = None

    # Timing metrics
    total_duration: float = 0.0
    producer_duration: float = 0.0
    consumer_duration: float = 0.0

    # Throughput metrics
    messages_sent: int = 0
    messages_received: int = 0
    messages_per_second_sent: float = 0.0
    messages_per_second_received: float = 0.0
    bytes_per_second: float = 0.0

    # Latency metrics
    latencies: List[float] = field(default_factory=list)
    avg_latency: float = 0.0
    p50_latency: float = 0.0
    p95_latency: float = 0.0
    p99_latency: float = 0.0
    min_latency: float = 0.0
    max_latency: float = 0.0

    # Error metrics
    send_errors: int = 0
    receive_errors: int = 0
    error_rate: float = 0.0

    # Resource usage
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0

    # Additional metadata
    timestamp: datetime = field(default_factory=datetime.now)
    docker_container_stats: Dict[str, Any] = field(default_factory=dict)

    def calculate_metrics(self):
        """Calculate derived metrics from raw data"""
        if self.latencies:
            self.avg_latency = statistics.mean(self.latencies)
            sorted_latencies = sorted(self.latencies)
            n = len(sorted_latencies)

            self.p50_latency = sorted_latencies[int(n * 0.5)]
            self.p95_latency = sorted_latencies[int(n * 0.95)]
            self.p99_latency = sorted_latencies[int(n * 0.99)]
            self.min_latency = min(sorted_latencies)
            self.max_latency = max(sorted_latencies)

        if self.producer_duration > 0:
            self.messages_per_second_sent = self.messages_sent / self.producer_duration
            self.bytes_per_second = (self.messages_sent * self.config.message_size) / self.producer_duration

        if self.consumer_duration > 0:
            self.messages_per_second_received = self.messages_received / self.consumer_duration

        total_messages = self.messages_sent + self.messages_received
        total_errors = self.send_errors + self.receive_errors
        if total_messages > 0:
            self.error_rate = total_errors / total_messages

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'provider': self.provider,
            'test_name': self.test_name,
            'config': {
                'message_count': self.config.message_count,
                'message_size': self.config.message_size,
                'concurrent_producers': self.config.concurrent_producers,
                'concurrent_consumers': self.config.concurrent_consumers,
                'batch_size': self.config.batch_size,
            },
            'success': self.success,
            'error_message': self.error_message,
            'metrics': {
                'total_duration': self.total_duration,
                'producer_duration': self.producer_duration,
                'consumer_duration': self.consumer_duration,
                'messages_sent': self.messages_sent,
                'messages_received': self.messages_received,
                'messages_per_second_sent': self.messages_per_second_sent,
                'messages_per_second_received': self.messages_per_second_received,
                'bytes_per_second': self.bytes_per_second,
                'avg_latency': self.avg_latency,
                'p95_latency': self.p95_latency,
                'p99_latency': self.p99_latency,
                'send_errors': self.send_errors,
                'receive_errors': self.receive_errors,
                'error_rate': self.error_rate,
                'memory_usage_mb': self.memory_usage_mb,
                'cpu_usage_percent': self.cpu_usage_percent,
            },
            'timestamp': self.timestamp.isoformat(),
            'docker_stats': self.docker_container_stats,
        }


class CloudBenchmark(ABC):
    """Base class for cloud queue benchmarks"""

    def __init__(self, provider_name: str):
        self.provider_name = provider_name
        self.docker_containers: List[str] = []
        self.results: List[BenchmarkResult] = []

    @abstractmethod
    async def setup_infrastructure(self, config: BenchmarkConfig) -> bool:
        """Setup Docker containers and infrastructure"""
        pass

    @abstractmethod
    async def cleanup_infrastructure(self):
        """Clean up Docker containers and resources"""
        pass

    @abstractmethod
    async def send_messages(self, config: BenchmarkConfig) -> tuple[int, float, List[float]]:
        """Send messages and return (sent_count, duration, latencies)"""
        pass

    @abstractmethod
    async def receive_messages(self, config: BenchmarkConfig) -> tuple[int, float, List[float]]:
        """Receive messages and return (received_count, duration, latencies)"""
        pass

    async def warmup(self, config: BenchmarkConfig):
        """Warm up the system with a small number of messages"""
        warmup_config = BenchmarkConfig(
            message_count=config.warmup_messages,
            message_size=config.message_size,
            concurrent_producers=1,
            concurrent_consumers=1,
            batch_size=min(config.batch_size, 10),
            queue_name=f"{config.queue_name}-warmup"
        )

        try:
            await self.send_messages(warmup_config)
            await self.receive_messages(warmup_config)
        except Exception as e:
            print(f"Warmup failed: {e}")

    async def get_docker_stats(self) -> Dict[str, Any]:
        """Get Docker container resource usage statistics"""
        import docker
        stats = {}

        try:
            client = docker.from_env()
            for container_name in self.docker_containers:
                try:
                    container = client.containers.get(container_name)
                    stats_stream = container.stats(stream=False, decode=True)

                    # Calculate CPU percentage
                    cpu_percent = 0.0
                    if 'cpu_stats' in stats_stream and 'precpu_stats' in stats_stream:
                        cpu_stats = stats_stream['cpu_stats']
                        precpu_stats = stats_stream['precpu_stats']

                        cpu_delta = cpu_stats.get('cpu_usage', {}).get('total_usage', 0) - \
                                  precpu_stats.get('cpu_usage', {}).get('total_usage', 0)
                        system_delta = cpu_stats.get('system_cpu_usage', 0) - \
                                     precpu_stats.get('system_cpu_usage', 0)

                        if system_delta > 0 and cpu_delta > 0:
                            cpu_percent = (cpu_delta / system_delta) * 100.0

                    # Calculate memory usage
                    memory_usage = stats_stream.get('memory_stats', {}).get('usage', 0)
                    memory_limit = stats_stream.get('memory_stats', {}).get('limit', 1)
                    memory_mb = memory_usage / (1024 * 1024)

                    stats[container_name] = {
                        'cpu_percent': cpu_percent,
                        'memory_mb': memory_mb,
                        'memory_limit_mb': memory_limit / (1024 * 1024),
                        'memory_percent': (memory_usage / memory_limit) * 100 if memory_limit > 0 else 0
                    }
                except Exception as e:
                    stats[container_name] = {'error': str(e)}

        except Exception as e:
            stats['error'] = str(e)

        return stats

    async def run_benchmark(self, config: BenchmarkConfig, test_name: str) -> BenchmarkResult:
        """Run a complete benchmark test"""
        result = BenchmarkResult(
            provider=self.provider_name,
            test_name=test_name,
            config=config
        )

        try:
            print(f"🚀 Starting {self.provider_name} benchmark: {test_name}")
            start_time = time.time()

            # Setup infrastructure
            print("📦 Setting up infrastructure...")
            if not await self.setup_infrastructure(config):
                result.error_message = "Failed to setup infrastructure"
                return result

            # Wait for services to be ready
            await asyncio.sleep(5)

            # Warmup
            print("🔥 Warming up...")
            await self.warmup(config)

            # Run producer
            print(f"📤 Sending {config.message_count} messages...")
            producer_start = time.time()
            sent_count, producer_duration, send_latencies = await self.send_messages(config)
            result.messages_sent = sent_count
            result.producer_duration = producer_duration

            # Run consumer
            print(f"📥 Receiving messages...")
            consumer_start = time.time()
            received_count, consumer_duration, receive_latencies = await self.receive_messages(config)
            result.messages_received = received_count
            result.consumer_duration = consumer_duration

            # Collect latencies
            result.latencies = send_latencies + receive_latencies

            # Get resource stats
            result.docker_container_stats = await self.get_docker_stats()

            # Calculate metrics
            result.total_duration = time.time() - start_time
            result.calculate_metrics()
            result.success = True

            print(f"✅ Benchmark completed successfully!")
            print(f"   📊 Sent: {result.messages_sent} msgs ({result.messages_per_second_sent:.1f} msg/s)")
            print(f"   📊 Received: {result.messages_received} msgs ({result.messages_per_second_received:.1f} msg/s)")
            print(f"   📊 Avg Latency: {result.avg_latency:.2f}ms")
            print(f"   📊 P95 Latency: {result.p95_latency:.2f}ms")

        except Exception as e:
            result.error_message = str(e)
            result.success = False
            print(f"❌ Benchmark failed: {e}")

        finally:
            # Always cleanup
            await self.cleanup_infrastructure()

        self.results.append(result)
        return result

    async def run_benchmark_suite(self) -> List[BenchmarkResult]:
        """Run a complete suite of benchmark tests"""
        configs = [
            # Basic throughput test
            BenchmarkConfig(
                message_count=1000,
                message_size=1024,
                concurrent_producers=1,
                concurrent_consumers=1,
                batch_size=10
            ),

            # High throughput test
            BenchmarkConfig(
                message_count=5000,
                message_size=1024,
                concurrent_producers=4,
                concurrent_consumers=4,
                batch_size=50
            ),

            # Large message test
            BenchmarkConfig(
                message_count=100,
                message_size=64 * 1024,  # 64KB
                concurrent_producers=2,
                concurrent_consumers=2,
                batch_size=5
            ),

            # Low latency test
            BenchmarkConfig(
                message_count=500,
                message_size=256,
                concurrent_producers=1,
                concurrent_consumers=1,
                batch_size=1
            ),
        ]

        test_names = [
            "basic_throughput",
            "high_throughput",
            "large_messages",
            "low_latency"
        ]

        results = []
        for config, test_name in zip(configs, test_names):
            result = await self.run_benchmark(config, test_name)
            results.append(result)

            # Wait between tests
            await asyncio.sleep(2)

        return results
