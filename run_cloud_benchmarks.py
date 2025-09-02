#!/usr/bin/env python3
"""
Pythia Cloud Queue Benchmarks - Convenient Runner Script

This script runs benchmarks for all supported cloud message queue services
using Docker-based emulators and local testing environments.
"""

import asyncio
import subprocess
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def check_docker():
    """Check if Docker is available and running"""
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def check_required_images():
    """Check if required Docker images are available"""
    required_images = [
        "localstack/localstack:latest",
        "google/cloud-sdk:latest",
        "redis:7-alpine",
        "mcr.microsoft.com/azure-storage/azurite:latest",
    ]

    missing_images = []

    for image in required_images:
        try:
            result = subprocess.run(
                ["docker", "image", "inspect", image], capture_output=True, timeout=5
            )
            if result.returncode != 0:
                missing_images.append(image)
        except subprocess.TimeoutExpired:
            missing_images.append(image)

    return missing_images


def pull_required_images():
    """Pull required Docker images"""
    required_images = [
        "localstack/localstack:latest",
        "google/cloud-sdk:latest",
        "redis:7-alpine",
        "mcr.microsoft.com/azure-storage/azurite:latest",
    ]

    print("📦 Pulling required Docker images...")
    for image in required_images:
        print(f"   Pulling {image}...")
        try:
            subprocess.run(
                ["docker", "pull", image],
                check=True,
                timeout=300,  # 5 minutes timeout per image
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"   ❌ Failed to pull {image}: {e}")
            return False

    print("✅ All required images pulled successfully!")
    return True


async def run_quick_test():
    """Run a quick benchmark test with minimal configuration"""
    from benchmarks.cloud_queues.base import BenchmarkConfig
    from benchmarks.cloud_queues.runner import BenchmarkRunner

    print("🚀 Running Quick Cloud Queue Benchmark Test")
    print("=" * 50)

    runner = BenchmarkRunner()

    # Quick test configuration
    config = BenchmarkConfig(
        message_count=100,
        message_size=512,
        concurrent_producers=1,
        concurrent_consumers=1,
        batch_size=5,
        timeout=60,
        queue_name="quick-test",
    )

    # Test one provider from each category
    providers_to_test = ["aws", "gcp", "azure-storage"]

    for provider in providers_to_test:
        try:
            print(f"\n🧪 Testing {provider.upper()}...")
            result = await runner.run_single_benchmark(provider, config, "quick_test")

            if result.success:
                print(
                    f"   ✅ {provider}: {result.messages_per_second_sent:.1f} msg/s, {result.avg_latency:.2f}ms latency"
                )
            else:
                print(f"   ❌ {provider}: {result.error_message}")

        except Exception as e:
            print(f"   ❌ {provider}: {e}")

    # Save and show results
    results_file = runner.save_results("quick_benchmark_results.json")
    print(f"\n📄 Quick test results saved to: {results_file}")

    return runner.results


async def run_full_benchmark():
    """Run the complete benchmark suite"""
    from benchmarks.cloud_queues.runner import BenchmarkRunner

    print("🔥 Running Full Cloud Queue Benchmark Suite")
    print("=" * 60)
    print("This will test all providers with multiple test scenarios:")
    print("• Basic throughput test (1000 msgs, 1KB)")
    print("• High throughput test (5000 msgs, 4 producers/consumers)")
    print("• Large message test (100 msgs, 64KB each)")
    print("• Low latency test (500 msgs, batch size 1)")
    print("")

    runner = BenchmarkRunner()

    try:
        results = await runner.run_all_providers()

        # Generate comprehensive report
        report = runner.generate_report()
        print("\n")
        print(report)

        # Save results with timestamp
        results_file = runner.save_results()

        print("\n🎉 Full benchmark completed!")
        print(f"📊 Total tests run: {len(runner.results)}")
        print(f"✅ Successful tests: {len([r for r in runner.results if r.success])}")
        print(f"📄 Detailed results: {results_file}")

        return results

    except KeyboardInterrupt:
        print("\n🛑 Benchmark interrupted by user")
        return None


def main():
    """Main entry point"""
    print("🐍 Pythia Cloud Queue Benchmarks")
    print("=" * 40)
    print("Testing cloud message queues with Docker emulators")
    print("")

    # Check Docker availability
    if not check_docker():
        print("❌ Docker is not available or not running.")
        print("   Please install and start Docker to run benchmarks.")
        sys.exit(1)

    print("✅ Docker is available")

    # Check required images
    missing_images = check_required_images()
    if missing_images:
        print(f"📦 Missing Docker images: {len(missing_images)}")
        print("   The following images will be pulled automatically:")
        for image in missing_images:
            print(f"   • {image}")
        print("")

        if not pull_required_images():
            print("❌ Failed to pull required images.")
            sys.exit(1)
    else:
        print("✅ All required Docker images are available")

    print("")

    # Interactive menu
    while True:
        print("Choose benchmark type:")
        print("1. Quick test (fast, minimal)")
        print("2. Full benchmark suite (comprehensive)")
        print("3. Exit")
        print("")

        try:
            choice = input("Enter your choice (1-3): ").strip()

            if choice == "1":
                print("")
                asyncio.run(run_quick_test())
                break
            elif choice == "2":
                print("")
                asyncio.run(run_full_benchmark())
                break
            elif choice == "3":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please enter 1, 2, or 3.")
                print("")

        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except EOFError:
            break


if __name__ == "__main__":
    main()
