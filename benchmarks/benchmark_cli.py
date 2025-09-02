#!/usr/bin/env python3
"""
Pythia Benchmarking CLI Tool

Comprehensive benchmarking utility for Pythia framework performance testing.
"""

import asyncio
import argparse
import json
import time
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any

# Add pythia to path if running from benchmarks directory
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    # Ensure package is importable at runtime without importing unused symbols
    import importlib.util

    if importlib.util.find_spec("pythia") is None:
        raise ImportError("pythia package not found")
    import pythia  # noqa: F401
except ImportError as e:
    print(f"❌ Failed to import Pythia: {e}")
    print("Make sure you're running from the project root or have Pythia installed.")
    sys.exit(1)

import psutil
from dataclasses import dataclass, asdict


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark runs"""

    broker: str
    duration: int
    message_rate: int
    message_size: int
    workers: int
    batch_size: int
    output_format: str
    results_dir: str


@dataclass
class BenchmarkResult:
    """Results from a benchmark run"""

    timestamp: str
    config: BenchmarkConfig
    total_messages: int
    duration_seconds: float
    messages_per_second: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_count: int
    error_rate: float
    cpu_usage_percent: float
    memory_usage_mb: float
    broker_metrics: Dict[str, Any]


class BenchmarkRunner:
    """Main benchmark execution engine"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.results_dir = Path(config.results_dir)
        self.results_dir.mkdir(exist_ok=True, parents=True)

    async def run_benchmark(self) -> BenchmarkResult:
        """Run a complete benchmark test"""
        print(f"🚀 Starting benchmark: {self.config.broker} broker")
        print(
            f"   Duration: {self.config.duration}s, Rate: {self.config.message_rate} msg/s"
        )
        print(
            f"   Workers: {self.config.workers}, Message size: {self.config.message_size} bytes"
        )

        start_time = time.time()

        # Setup monitoring
        system_monitor = SystemMonitor()
        system_monitor.start()

        try:
            # Run the actual benchmark
            if self.config.broker == "kafka":
                result = await self._run_kafka_benchmark()
            elif self.config.broker == "rabbitmq":
                result = await self._run_rabbitmq_benchmark()
            elif self.config.broker == "redis":
                result = await self._run_redis_benchmark()
            else:
                raise ValueError(f"Unsupported broker: {self.config.broker}")

        finally:
            system_metrics = system_monitor.stop()

        end_time = time.time()
        duration = end_time - start_time

        # Create benchmark result
        benchmark_result = BenchmarkResult(
            timestamp=datetime.now(timezone.utc).isoformat(),
            config=self.config,
            total_messages=result["total_messages"],
            duration_seconds=duration,
            messages_per_second=result["total_messages"] / duration,
            avg_latency_ms=result["avg_latency_ms"],
            p95_latency_ms=result["p95_latency_ms"],
            p99_latency_ms=result["p99_latency_ms"],
            error_count=result["error_count"],
            error_rate=result["error_count"] / result["total_messages"]
            if result["total_messages"] > 0
            else 0,
            cpu_usage_percent=system_metrics["cpu_percent"],
            memory_usage_mb=system_metrics["memory_mb"],
            broker_metrics=result.get("broker_metrics", {}),
        )

        # Save results
        await self._save_results(benchmark_result)

        return benchmark_result

    async def _run_kafka_benchmark(self) -> Dict[str, Any]:
        """Run Kafka-specific benchmark"""
        from load_generators.kafka_load_test_simple import KafkaLoadTest

        load_test = KafkaLoadTest(
            bootstrap_servers="localhost:9092",
            topic="benchmark-topic",
            message_rate=self.config.message_rate,
            message_size=self.config.message_size,
            duration=self.config.duration,
            num_workers=self.config.workers,
        )

        return await load_test.run()

    async def _run_rabbitmq_benchmark(self) -> Dict[str, Any]:
        """Run RabbitMQ-specific benchmark"""
        from load_generators.rabbitmq_load_test import RabbitMQLoadTest

        load_test = RabbitMQLoadTest(
            connection_url="amqp://guest:guest@localhost:5672",
            queue="benchmark-queue",
            message_rate=self.config.message_rate,
            message_size=self.config.message_size,
            duration=self.config.duration,
            num_workers=self.config.workers,
        )

        return await load_test.run()

    async def _run_redis_benchmark(self) -> Dict[str, Any]:
        """Run Redis-specific benchmark"""
        from load_generators.redis_load_test import RedisLoadTest

        load_test = RedisLoadTest(
            redis_url="redis://localhost:6379",
            queue="benchmark-queue",
            message_rate=self.config.message_rate,
            message_size=self.config.message_size,
            duration=self.config.duration,
            num_workers=self.config.workers,
        )

        return await load_test.run()

    async def _save_results(self, result: BenchmarkResult):
        """Save benchmark results to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.config.broker}_benchmark_{timestamp}.json"
        filepath = self.results_dir / filename

        with open(filepath, "w") as f:
            json.dump(asdict(result), f, indent=2)

        print(f"📊 Results saved to: {filepath}")


class SystemMonitor:
    """System resource monitoring during benchmarks"""

    def __init__(self):
        self.monitoring = False
        self.cpu_samples = []
        self.memory_samples = []

    def start(self):
        """Start monitoring system resources"""
        self.monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())

    def stop(self) -> Dict[str, float]:
        """Stop monitoring and return average metrics"""
        self.monitoring = False

        if hasattr(self, "monitor_task"):
            self.monitor_task.cancel()

        return {
            "cpu_percent": sum(self.cpu_samples) / len(self.cpu_samples)
            if self.cpu_samples
            else 0,
            "memory_mb": sum(self.memory_samples) / len(self.memory_samples)
            if self.memory_samples
            else 0,
        }

    async def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            try:
                # Sample CPU and memory usage
                cpu_percent = psutil.cpu_percent(interval=0.1)
                memory_mb = psutil.virtual_memory().used / 1024 / 1024

                self.cpu_samples.append(cpu_percent)
                self.memory_samples.append(memory_mb)

                await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Monitoring error: {e}")


def create_comparison_report(results_files: List[str], output_file: str):
    """Create a comparison report from multiple benchmark results"""
    results = []

    for file_path in results_files:
        with open(file_path, "r") as f:
            results.append(json.load(f))

    # Create comparison report
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "comparison_type": "broker_performance",
        "results": results,
        "summary": {},
    }

    # Calculate summary statistics
    for result in results:
        broker = result["config"]["broker"]
        report["summary"][broker] = {
            "throughput_msg_per_sec": result["messages_per_second"],
            "avg_latency_ms": result["avg_latency_ms"],
            "p95_latency_ms": result["p95_latency_ms"],
            "error_rate_percent": result["error_rate"] * 100,
            "cpu_usage_percent": result["cpu_usage_percent"],
            "memory_usage_mb": result["memory_usage_mb"],
        }

    with open(output_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"📈 Comparison report saved to: {output_file}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Pythia Benchmarking Tool")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Benchmark command
    bench_parser = subparsers.add_parser("benchmark", help="Run performance benchmark")
    bench_parser.add_argument(
        "--broker",
        choices=["kafka", "rabbitmq", "redis"],
        required=True,
        help="Message broker to benchmark",
    )
    bench_parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds (default: 60)",
    )
    bench_parser.add_argument(
        "--rate", type=int, default=1000, help="Message rate per second (default: 1000)"
    )
    bench_parser.add_argument(
        "--size", type=int, default=1024, help="Message size in bytes (default: 1024)"
    )
    bench_parser.add_argument(
        "--workers", type=int, default=1, help="Number of worker processes (default: 1)"
    )
    bench_parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for processing (default: 10)",
    )
    bench_parser.add_argument(
        "--output-format",
        choices=["json", "csv"],
        default="json",
        help="Output format (default: json)",
    )
    bench_parser.add_argument(
        "--results-dir",
        default="./results",
        help="Results directory (default: ./results)",
    )

    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare benchmark results")
    compare_parser.add_argument(
        "files", nargs="+", help="Benchmark result files to compare"
    )
    compare_parser.add_argument(
        "--output",
        default="comparison_report.json",
        help="Output file for comparison report",
    )

    # List command
    list_parser = subparsers.add_parser("list", help="List available benchmark results")
    list_parser.add_argument(
        "--results-dir", default="./results", help="Results directory to scan"
    )

    args = parser.parse_args()

    if args.command == "benchmark":
        config = BenchmarkConfig(
            broker=args.broker,
            duration=args.duration,
            message_rate=args.rate,
            message_size=args.size,
            workers=args.workers,
            batch_size=args.batch_size,
            output_format=args.output_format,
            results_dir=args.results_dir,
        )

        runner = BenchmarkRunner(config)
        result = asyncio.run(runner.run_benchmark())

        print("\n📊 Benchmark Results:")
        print(f"   Throughput: {result.messages_per_second:.1f} msg/sec")
        print(f"   Avg Latency: {result.avg_latency_ms:.1f}ms")
        print(f"   P95 Latency: {result.p95_latency_ms:.1f}ms")
        print(f"   Error Rate: {result.error_rate * 100:.2f}%")
        print(f"   CPU Usage: {result.cpu_usage_percent:.1f}%")
        print(f"   Memory Usage: {result.memory_usage_mb:.1f}MB")

    elif args.command == "compare":
        create_comparison_report(args.files, args.output)

    elif args.command == "list":
        results_dir = Path(args.results_dir)
        if not results_dir.exists():
            print(f"❌ Results directory not found: {results_dir}")
            return

        result_files = list(results_dir.glob("*.json"))
        if not result_files:
            print(f"📁 No benchmark results found in {results_dir}")
            return

        print(f"📊 Found {len(result_files)} benchmark results:")
        for file_path in sorted(result_files):
            print(f"   {file_path.name}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
