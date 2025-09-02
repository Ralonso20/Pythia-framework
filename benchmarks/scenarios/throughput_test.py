#!/usr/bin/env python3
"""
Throughput Testing Scenario

Tests maximum message throughput capabilities across all brokers.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from benchmark_cli import BenchmarkRunner, BenchmarkConfig


class ThroughputTest:
    """Throughput-focused benchmark scenario"""

    def __init__(self):
        self.results = {}

    async def run_all_brokers(self, base_rate: int = 5000, duration: int = 120):
        """Run throughput tests for all brokers"""
        print("🚀 Starting Throughput Test Scenario")
        print(f"   Base rate: {base_rate} msg/sec")
        print(f"   Duration: {duration} seconds")
        print("   Focus: Maximum sustainable throughput")

        brokers = ["redis", "kafka", "rabbitmq"]

        for broker in brokers:
            print(f"\n📊 Testing {broker} throughput...")

            # Start with base rate and scale up until errors occur
            current_rate = base_rate
            max_successful_rate = 0

            while current_rate <= base_rate * 3:  # Test up to 3x base rate
                config = BenchmarkConfig(
                    broker=broker,
                    duration=min(60, duration),  # Shorter tests for scaling
                    message_rate=current_rate,
                    message_size=1024,  # Standard 1KB messages
                    workers=4,  # Optimize for throughput
                    batch_size=100,
                    output_format="json",
                    results_dir=f"./results/throughput_{broker}",
                )

                runner = BenchmarkRunner(config)
                result = await runner.run_benchmark()

                # Check if this rate was successful
                error_rate = result.error_rate
                if error_rate < 0.01:  # Less than 1% error rate
                    max_successful_rate = current_rate
                    self.results[f"{broker}_{current_rate}"] = result
                    print(
                        f"   ✅ {current_rate} msg/s: {result.messages_per_second:.1f} actual, {error_rate * 100:.2f}% errors"
                    )
                else:
                    print(
                        f"   ❌ {current_rate} msg/s: Too many errors ({error_rate * 100:.2f}%)"
                    )
                    break

                # Increase rate for next test
                current_rate = int(current_rate * 1.5)

                # Cool down between tests
                await asyncio.sleep(10)

            self.results[f"{broker}_max_rate"] = max_successful_rate
            print(f"   🏆 {broker} max sustainable rate: {max_successful_rate} msg/s")

        # Summary
        print("\n📈 Throughput Test Results Summary:")
        print("=" * 50)
        for broker in brokers:
            max_rate = self.results.get(f"{broker}_max_rate", 0)
            if max_rate > 0:
                result_key = f"{broker}_{max_rate}"
                if result_key in self.results:
                    result = self.results[result_key]
                    print(
                        f"{broker.upper():>10}: {result.messages_per_second:>8.1f} msg/s "
                        f"(latency: {result.avg_latency_ms:>5.1f}ms)"
                    )
                else:
                    print(f"{broker.upper():>10}: {max_rate:>8.1f} msg/s (estimated)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Throughput Testing Scenario")
    parser.add_argument(
        "--base-rate",
        type=int,
        default=5000,
        help="Base message rate to start testing (default: 5000)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Test duration in seconds (default: 120)",
    )

    args = parser.parse_args()

    test = ThroughputTest()
    asyncio.run(test.run_all_brokers(args.base_rate, args.duration))
