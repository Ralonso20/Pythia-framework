#!/usr/bin/env python3
"""
Latency Testing Scenario

Tests message processing latency under different load conditions.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from benchmark_cli import BenchmarkRunner, BenchmarkConfig


class LatencyTest:
    """Latency-focused benchmark scenario"""

    def __init__(self):
        self.results = {}

    async def run_latency_tests(self, duration: int = 60):
        """Run latency tests across different load levels"""
        print("🚀 Starting Latency Test Scenario")
        print(f"   Duration: {duration} seconds per test")
        print("   Focus: Message processing latency")

        brokers = ["redis", "kafka", "rabbitmq"]

        # Test different load levels
        load_levels = [
            {"name": "Low Load", "rate": 100, "workers": 1},
            {"name": "Medium Load", "rate": 1000, "workers": 2},
            {"name": "High Load", "rate": 5000, "workers": 4},
        ]

        # Test different message sizes
        message_sizes = [
            {"name": "Small", "size": 256},
            {"name": "Medium", "size": 1024},
            {"name": "Large", "size": 4096},
        ]

        for broker in brokers:
            print(f"\n📊 Testing {broker} latency characteristics...")
            broker_results = {}

            for load in load_levels:
                for msg_size in message_sizes:
                    test_name = f"{load['name']}_{msg_size['name']}"

                    print(
                        f"   Testing {test_name}: {load['rate']} msg/s, {msg_size['size']}B messages"
                    )

                    config = BenchmarkConfig(
                        broker=broker,
                        duration=duration,
                        message_rate=load["rate"],
                        message_size=msg_size["size"],
                        workers=load["workers"],
                        batch_size=1,  # Process individually for accurate latency
                        output_format="json",
                        results_dir=f"./results/latency_{broker}",
                    )

                    runner = BenchmarkRunner(config)
                    result = await runner.run_benchmark()

                    broker_results[test_name] = {
                        "avg_latency_ms": result.avg_latency_ms,
                        "p95_latency_ms": result.p95_latency_ms,
                        "p99_latency_ms": result.p99_latency_ms,
                        "throughput": result.messages_per_second,
                        "error_rate": result.error_rate,
                    }

                    print(
                        f"      Avg: {result.avg_latency_ms:.1f}ms, "
                        f"P95: {result.p95_latency_ms:.1f}ms, "
                        f"P99: {result.p99_latency_ms:.1f}ms"
                    )

                    # Cool down between tests
                    await asyncio.sleep(5)

            self.results[broker] = broker_results

        # Generate latency comparison report
        await self._generate_latency_report()

    async def _generate_latency_report(self):
        """Generate comprehensive latency comparison report"""
        print("\n📈 Latency Test Results Summary:")
        print("=" * 80)

        # Header
        print(
            f"{'Broker':<10} {'Load':<12} {'Size':<8} {'Avg':<8} {'P95':<8} {'P99':<8} {'Rate':<8}"
        )
        print("-" * 80)

        # Results for each broker and scenario
        for broker, results in self.results.items():
            for test_name, metrics in results.items():
                load_name, size_name = test_name.split("_", 1)

                print(
                    f"{broker.upper():<10} {load_name:<12} {size_name:<8} "
                    f"{metrics['avg_latency_ms']:<8.1f} "
                    f"{metrics['p95_latency_ms']:<8.1f} "
                    f"{metrics['p99_latency_ms']:<8.1f} "
                    f"{metrics['throughput']:<8.1f}"
                )

        # Find best performers
        print("\n🏆 Best Performers:")
        print("-" * 40)

        best_avg_latency = {"broker": "", "latency": float("inf"), "test": ""}
        best_p99_latency = {"broker": "", "latency": float("inf"), "test": ""}

        for broker, results in self.results.items():
            for test_name, metrics in results.items():
                if metrics["avg_latency_ms"] < best_avg_latency["latency"]:
                    best_avg_latency = {
                        "broker": broker,
                        "latency": metrics["avg_latency_ms"],
                        "test": test_name,
                    }

                if metrics["p99_latency_ms"] < best_p99_latency["latency"]:
                    best_p99_latency = {
                        "broker": broker,
                        "latency": metrics["p99_latency_ms"],
                        "test": test_name,
                    }

        print(
            f"Lowest Avg Latency: {best_avg_latency['broker'].upper()} "
            f"({best_avg_latency['latency']:.1f}ms) in {best_avg_latency['test']}"
        )
        print(
            f"Lowest P99 Latency: {best_p99_latency['broker'].upper()} "
            f"({best_p99_latency['latency']:.1f}ms) in {best_p99_latency['test']}"
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Latency Testing Scenario")
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds (default: 60)",
    )

    args = parser.parse_args()

    test = LatencyTest()
    asyncio.run(test.run_latency_tests(args.duration))
