"""
Cloud Queue Benchmark Runner and Results Analysis
"""

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import argparse

from .aws_benchmark import AWSBenchmark
from .gcp_benchmark import GCPBenchmark
from .azure_benchmark import AzureBenchmark, AzureStorageQueueBenchmark
from .base import BenchmarkResult, BenchmarkConfig


class BenchmarkRunner:
    """Main benchmark runner for cloud queue services"""

    def __init__(self):
        self.benchmarks = {
            'aws': AWSBenchmark(),
            'gcp': GCPBenchmark(),
            'azure-servicebus': AzureBenchmark(),
            'azure-storage': AzureStorageQueueBenchmark(),
        }
        self.results: List[BenchmarkResult] = []

    async def run_single_benchmark(self, provider: str, config: BenchmarkConfig, test_name: str) -> BenchmarkResult:
        """Run a single benchmark test"""
        if provider not in self.benchmarks:
            raise ValueError(f"Unknown provider: {provider}. Available: {list(self.benchmarks.keys())}")

        benchmark = self.benchmarks[provider]
        result = await benchmark.run_benchmark(config, test_name)
        self.results.append(result)
        return result

    async def run_provider_suite(self, provider: str) -> List[BenchmarkResult]:
        """Run complete benchmark suite for a provider"""
        if provider not in self.benchmarks:
            raise ValueError(f"Unknown provider: {provider}")

        print(f"\n🚀 Starting {provider.upper()} benchmark suite...")
        benchmark = self.benchmarks[provider]
        results = await benchmark.run_benchmark_suite()
        self.results.extend(results)
        return results

    async def run_all_providers(self) -> Dict[str, List[BenchmarkResult]]:
        """Run benchmark suite for all providers"""
        all_results = {}

        for provider in self.benchmarks.keys():
            print(f"\n" + "="*60)
            print(f"🔥 BENCHMARKING {provider.upper()}")
            print("="*60)

            try:
                results = await self.run_provider_suite(provider)
                all_results[provider] = results

                # Show summary
                successful = [r for r in results if r.success]
                print(f"\n📊 {provider.upper()} Summary:")
                print(f"   Tests: {len(results)}")
                print(f"   Successful: {len(successful)}")

                if successful:
                    avg_throughput = sum(r.messages_per_second_sent for r in successful) / len(successful)
                    avg_latency = sum(r.avg_latency for r in successful) / len(successful)
                    print(f"   Avg Throughput: {avg_throughput:.1f} msg/s")
                    print(f"   Avg Latency: {avg_latency:.2f}ms")

            except Exception as e:
                print(f"❌ {provider.upper()} benchmark suite failed: {e}")
                all_results[provider] = []

            # Wait between provider tests
            await asyncio.sleep(3)

        return all_results

    def save_results(self, filename: str = None) -> str:
        """Save benchmark results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cloud_queue_benchmark_{timestamp}.json"

        results_data = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'total_tests': len(self.results),
                'successful_tests': len([r for r in self.results if r.success]),
                'providers_tested': list(set(r.provider for r in self.results)),
            },
            'results': [result.to_dict() for result in self.results]
        }

        # Ensure benchmarks directory exists
        benchmarks_dir = Path(__file__).parent.parent
        results_file = benchmarks_dir / filename

        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2)

        print(f"📄 Results saved to: {results_file}")
        return str(results_file)

    def generate_report(self) -> str:
        """Generate a comprehensive benchmark report"""
        if not self.results:
            return "No benchmark results to report."

        report = []
        report.append("="*80)
        report.append("🐍 PYTHIA CLOUD QUEUE BENCHMARK REPORT")
        report.append("="*80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Tests: {len(self.results)}")
        report.append("")

        # Group results by provider
        provider_results = {}
        for result in self.results:
            if result.provider not in provider_results:
                provider_results[result.provider] = []
            provider_results[result.provider].append(result)

        # Performance summary table
        report.append("📊 PERFORMANCE SUMMARY")
        report.append("-" * 80)
        report.append(f"{'Provider':<20} {'Tests':<8} {'Success':<8} {'Avg Throughput':<15} {'Avg Latency':<12} {'P95 Latency':<12}")
        report.append("-" * 80)

        for provider, results in provider_results.items():
            successful = [r for r in results if r.success]
            success_rate = f"{len(successful)}/{len(results)}"

            if successful:
                avg_throughput = sum(r.messages_per_second_sent for r in successful) / len(successful)
                avg_latency = sum(r.avg_latency for r in successful) / len(successful)
                avg_p95_latency = sum(r.p95_latency for r in successful) / len(successful)

                report.append(f"{provider:<20} {len(results):<8} {success_rate:<8} {avg_throughput:<15.1f} {avg_latency:<12.2f} {avg_p95_latency:<12.2f}")
            else:
                report.append(f"{provider:<20} {len(results):<8} {success_rate:<8} {'N/A':<15} {'N/A':<12} {'N/A':<12}")

        report.append("")

        # Detailed results by provider
        for provider, results in provider_results.items():
            report.append(f"🔍 DETAILED RESULTS: {provider.upper()}")
            report.append("-" * 60)

            for result in results:
                status = "✅ SUCCESS" if result.success else "❌ FAILED"
                report.append(f"\nTest: {result.test_name} {status}")

                if result.success:
                    report.append(f"  Messages Sent: {result.messages_sent}")
                    report.append(f"  Messages Received: {result.messages_received}")
                    report.append(f"  Send Throughput: {result.messages_per_second_sent:.1f} msg/s")
                    report.append(f"  Receive Throughput: {result.messages_per_second_received:.1f} msg/s")
                    report.append(f"  Data Throughput: {result.bytes_per_second/1024/1024:.2f} MB/s")
                    report.append(f"  Average Latency: {result.avg_latency:.2f} ms")
                    report.append(f"  P95 Latency: {result.p95_latency:.2f} ms")
                    report.append(f"  P99 Latency: {result.p99_latency:.2f} ms")
                    report.append(f"  Error Rate: {result.error_rate*100:.2f}%")
                    report.append(f"  Total Duration: {result.total_duration:.2f} s")

                    # Configuration
                    config = result.config
                    report.append(f"  Config: {config.message_count} msgs, {config.message_size}B, " +
                               f"{config.concurrent_producers}P/{config.concurrent_consumers}C, " +
                               f"batch={config.batch_size}")
                else:
                    report.append(f"  Error: {result.error_message}")

            report.append("")

        # Recommendations
        report.append("💡 RECOMMENDATIONS")
        report.append("-" * 40)

        # Find best performer for each metric
        successful_results = [r for r in self.results if r.success]
        if successful_results:
            best_throughput = max(successful_results, key=lambda r: r.messages_per_second_sent)
            best_latency = min(successful_results, key=lambda r: r.avg_latency)

            report.append(f"🏆 Best Throughput: {best_throughput.provider} ({best_throughput.messages_per_second_sent:.1f} msg/s)")
            report.append(f"⚡ Best Latency: {best_latency.provider} ({best_latency.avg_latency:.2f} ms)")

            # General recommendations
            report.append("")
            report.append("General Recommendations:")
            report.append("• For high throughput: Consider AWS SQS with batch operations")
            report.append("• For low latency: GCP Pub/Sub typically has lowest latency")
            report.append("• For cost optimization: Compare pricing models for your use case")
            report.append("• For hybrid cloud: Consider message size and network latency")

        report.append("")
        report.append("="*80)
        report.append("Report generated by Pythia Cloud Queue Benchmarks")
        report.append("="*80)

        return "\n".join(report)

    def compare_providers(self, test_name: str) -> Dict[str, Any]:
        """Compare all providers for a specific test"""
        test_results = [r for r in self.results if r.test_name == test_name and r.success]

        if not test_results:
            return {"error": f"No successful results found for test: {test_name}"}

        comparison = {
            "test_name": test_name,
            "providers": {}
        }

        for result in test_results:
            comparison["providers"][result.provider] = {
                "throughput_sent": result.messages_per_second_sent,
                "throughput_received": result.messages_per_second_received,
                "avg_latency": result.avg_latency,
                "p95_latency": result.p95_latency,
                "p99_latency": result.p99_latency,
                "error_rate": result.error_rate,
                "total_duration": result.total_duration
            }

        # Find winners
        if len(test_results) > 1:
            best_throughput = max(test_results, key=lambda r: r.messages_per_second_sent)
            best_latency = min(test_results, key=lambda r: r.avg_latency)

            comparison["winners"] = {
                "best_throughput": {
                    "provider": best_throughput.provider,
                    "value": best_throughput.messages_per_second_sent
                },
                "best_latency": {
                    "provider": best_latency.provider,
                    "value": best_latency.avg_latency
                }
            }

        return comparison


async def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Pythia Cloud Queue Benchmarks")
    parser.add_argument(
        "--provider",
        choices=["aws", "gcp", "azure-servicebus", "azure-storage", "all"],
        default="all",
        help="Cloud provider to benchmark"
    )
    parser.add_argument(
        "--test",
        choices=["basic_throughput", "high_throughput", "large_messages", "low_latency"],
        help="Specific test to run (default: run all tests)"
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=1000,
        help="Number of messages to send"
    )
    parser.add_argument(
        "--size",
        type=int,
        default=1024,
        help="Message size in bytes"
    )
    parser.add_argument(
        "--producers",
        type=int,
        default=1,
        help="Number of concurrent producers"
    )
    parser.add_argument(
        "--consumers",
        type=int,
        default=1,
        help="Number of concurrent consumers"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for operations"
    )
    parser.add_argument(
        "--output",
        help="Output file for results (JSON)"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate and display text report"
    )

    args = parser.parse_args()

    # Create benchmark runner
    runner = BenchmarkRunner()

    print("🐍 Pythia Cloud Queue Benchmarks")
    print("=" * 50)
    print(f"Docker required: LocalStack, GCP Emulator, Azurite")
    print(f"Provider: {args.provider}")
    print("")

    try:
        if args.test:
            # Run specific test
            config = BenchmarkConfig(
                message_count=args.messages,
                message_size=args.size,
                concurrent_producers=args.producers,
                concurrent_consumers=args.consumers,
                batch_size=args.batch_size,
                queue_name=f"benchmark-{int(time.time())}"
            )

            if args.provider == "all":
                for provider in runner.benchmarks.keys():
                    await runner.run_single_benchmark(provider, config, args.test)
            else:
                await runner.run_single_benchmark(args.provider, config, args.test)
        else:
            # Run benchmark suite
            if args.provider == "all":
                await runner.run_all_providers()
            else:
                await runner.run_provider_suite(args.provider)

        # Save results
        results_file = runner.save_results(args.output)

        # Generate report
        if args.report or args.provider == "all":
            print("\n")
            print(runner.generate_report())

        print(f"\n✅ Benchmarking completed!")
        print(f"📄 Results saved to: {results_file}")

    except KeyboardInterrupt:
        print("\n🛑 Benchmark interrupted by user")
    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
