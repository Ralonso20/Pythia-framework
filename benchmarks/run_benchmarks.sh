#!/bin/bash
set -e

# Pythia Performance Benchmarking Script

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if infrastructure is running
check_infrastructure() {
    print_status "Checking benchmark infrastructure..."

    # Check if Docker containers are running
    local containers=("kafka" "rabbitmq" "redis" "prometheus" "grafana")
    for container in "${containers[@]}"; do
        if ! docker ps | grep -q "$container"; then
            print_error "$container container is not running"
            print_status "Run './infrastructure/setup_monitoring.sh' first"
            exit 1
        fi
    done

    # Check if services are responding
    if ! curl -s http://localhost:9090 > /dev/null; then
        print_error "Prometheus is not responding"
        exit 1
    fi

    if ! curl -s http://localhost:3000 > /dev/null; then
        print_error "Grafana is not responding"
        exit 1
    fi

    print_status "✅ Infrastructure is ready"
}

# Run single broker benchmark
run_broker_benchmark() {
    local broker=$1
    local duration=${2:-60}
    local rate=${3:-1000}
    local workers=${4:-2}
    local size=${5:-1024}

    print_header "Benchmarking $broker"

    python3 benchmark_cli.py benchmark \
        --broker "$broker" \
        --duration "$duration" \
        --rate "$rate" \
        --workers "$workers" \
        --size "$size" \
        --results-dir "./results/$(date +%Y%m%d)"
}

# Run comprehensive benchmark suite
run_comprehensive_benchmarks() {
    local duration=${1:-60}
    local base_rate=${2:-1000}

    print_header "Running Comprehensive Benchmark Suite"

    # Create results directory
    local results_dir="./results/$(date +%Y%m%d_%H%M%S)_comprehensive"
    mkdir -p "$results_dir"

    print_status "Results will be saved to: $results_dir"

    # Benchmark parameters
    local brokers=("redis" "kafka" "rabbitmq")
    local rates=("$base_rate" "$((base_rate * 2))" "$((base_rate * 5))")
    local worker_counts=(1 2 4)
    local message_sizes=(512 1024 4096)

    # Run benchmarks for each combination
    for broker in "${brokers[@]}"; do
        for rate in "${rates[@]}"; do
            for workers in "${worker_counts[@]}"; do
                for size in "${message_sizes[@]}"; do
                    print_status "Testing $broker: ${rate} msg/s, ${workers} workers, ${size}B messages"

                    python3 benchmark_cli.py benchmark \
                        --broker "$broker" \
                        --duration "$duration" \
                        --rate "$rate" \
                        --workers "$workers" \
                        --size "$size" \
                        --results-dir "$results_dir"

                    # Cool down between tests
                    print_status "Cooling down for 10 seconds..."
                    sleep 10
                done
            done
        done
    done

    print_status "✅ Comprehensive benchmarks completed"

    # Generate comparison report
    print_status "Generating comparison report..."
    python3 benchmark_cli.py compare \
        "$results_dir"/*.json \
        --output "$results_dir/comparison_report.json"
}

# Run specific scenario benchmarks
run_scenario_benchmarks() {
    print_header "Running Scenario Benchmarks"

    local results_dir="./results/$(date +%Y%m%d_%H%M%S)_scenarios"
    mkdir -p "$results_dir"

    # Scenario 1: High Throughput
    print_status "Scenario 1: High Throughput Test (5000 msg/s)"
    for broker in redis kafka rabbitmq; do
        python3 benchmark_cli.py benchmark \
            --broker "$broker" \
            --duration 120 \
            --rate 5000 \
            --workers 4 \
            --size 1024 \
            --results-dir "$results_dir/high_throughput"
        sleep 15
    done

    # Scenario 2: Large Messages
    print_status "Scenario 2: Large Message Test (10KB messages)"
    for broker in redis kafka rabbitmq; do
        python3 benchmark_cli.py benchmark \
            --broker "$broker" \
            --duration 60 \
            --rate 500 \
            --workers 2 \
            --size 10240 \
            --results-dir "$results_dir/large_messages"
        sleep 10
    done

    # Scenario 3: Low Latency
    print_status "Scenario 3: Low Latency Test (low rate, single worker)"
    for broker in redis kafka rabbitmq; do
        python3 benchmark_cli.py benchmark \
            --broker "$broker" \
            --duration 60 \
            --rate 100 \
            --workers 1 \
            --size 512 \
            --results-dir "$results_dir/low_latency"
        sleep 5
    done

    # Scenario 4: Burst Load
    print_status "Scenario 4: Burst Load Test (10000 msg/s for 30s)"
    for broker in redis kafka rabbitmq; do
        python3 benchmark_cli.py benchmark \
            --broker "$broker" \
            --duration 30 \
            --rate 10000 \
            --workers 6 \
            --size 1024 \
            --results-dir "$results_dir/burst_load"
        sleep 20
    done

    print_status "✅ Scenario benchmarks completed"
}

# Run stress tests
run_stress_tests() {
    print_header "Running Stress Tests"

    local results_dir="./results/$(date +%Y%m%d_%H%M%S)_stress"
    mkdir -p "$results_dir"

    print_warning "Stress tests will run for 10 minutes and may impact system performance"
    read -p "Continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        return
    fi

    # Memory stress test
    print_status "Memory Stress Test (large messages, sustained load)"
    for broker in redis kafka rabbitmq; do
        python3 benchmark_cli.py benchmark \
            --broker "$broker" \
            --duration 300 \
            --rate 2000 \
            --workers 4 \
            --size 8192 \
            --results-dir "$results_dir/memory_stress"
        sleep 30
    done

    # CPU stress test
    print_status "CPU Stress Test (high rate, many workers)"
    for broker in redis kafka rabbitmq; do
        python3 benchmark_cli.py benchmark \
            --broker "$broker" \
            --duration 300 \
            --rate 8000 \
            --workers 8 \
            --size 1024 \
            --results-dir "$results_dir/cpu_stress"
        sleep 30
    done

    print_status "✅ Stress tests completed"
}

# Generate performance report
generate_report() {
    print_header "Generating Performance Report"

    local report_dir="./results/reports/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$report_dir"

    # Find all result files from today
    local today=$(date +%Y%m%d)
    local result_files=(./results/${today}*/*.json)

    if [ ${#result_files[@]} -eq 0 ]; then
        print_warning "No benchmark results found for today"
        return
    fi

    print_status "Found ${#result_files[@]} result files"

    # Generate comparison report
    python3 benchmark_cli.py compare \
        "${result_files[@]}" \
        --output "$report_dir/full_comparison_report.json"

    # Generate summary statistics
    python3 -c "
import json
import sys
from pathlib import Path

report_file = '$report_dir/full_comparison_report.json'
with open(report_file) as f:
    data = json.load(f)

print('📊 Performance Summary Report')
print('=' * 50)

if 'summary' in data:
    for broker, metrics in data['summary'].items():
        print(f'\n{broker.upper()} Performance:')
        print(f'  Throughput: {metrics.get(\"throughput_msg_per_sec\", 0):.1f} msg/s')
        print(f'  Avg Latency: {metrics.get(\"avg_latency_ms\", 0):.1f}ms')
        print(f'  P95 Latency: {metrics.get(\"p95_latency_ms\", 0):.1f}ms')
        print(f'  Error Rate: {metrics.get(\"error_rate_percent\", 0):.2f}%')
        print(f'  CPU Usage: {metrics.get(\"cpu_usage_percent\", 0):.1f}%')
        print(f'  Memory: {metrics.get(\"memory_usage_mb\", 0):.1f}MB')

print(f'\n📁 Full report saved to: {report_file}')
"

    print_status "✅ Performance report generated"
}

# Show usage information
show_usage() {
    echo "Pythia Performance Benchmarking Tool"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  check                    - Check if infrastructure is ready"
    echo "  quick <broker>           - Run quick benchmark (60s, 1000 msg/s)"
    echo "  broker <broker> [opts]   - Run single broker benchmark"
    echo "  comprehensive [duration] - Run comprehensive benchmark suite"
    echo "  scenarios               - Run specific scenario benchmarks"
    echo "  stress                  - Run stress tests (long duration)"
    echo "  report                  - Generate performance report from results"
    echo "  list                    - List available benchmark results"
    echo "  clean                   - Clean old benchmark results"
    echo ""
    echo "Broker options: kafka, rabbitmq, redis"
    echo ""
    echo "Examples:"
    echo "  $0 quick redis          # Quick Redis benchmark"
    echo "  $0 broker kafka 120 2000 4  # Kafka: 120s, 2000 msg/s, 4 workers"
    echo "  $0 comprehensive 60     # Full suite with 60s tests"
    echo "  $0 scenarios           # Run all scenario benchmarks"
}

# Clean old results
clean_results() {
    print_status "Cleaning old benchmark results..."

    # Remove results older than 7 days
    find ./results -name "*.json" -mtime +7 -delete
    find ./results -type d -empty -delete

    print_status "✅ Old results cleaned"
}

# List available results
list_results() {
    python3 benchmark_cli.py list --results-dir "./results"
}

# Main script logic
case "${1:-}" in
    "check")
        check_infrastructure
        ;;
    "quick")
        if [ -z "$2" ]; then
            print_error "Usage: $0 quick <broker>"
            exit 1
        fi
        check_infrastructure
        run_broker_benchmark "$2" 60 1000 2 1024
        ;;
    "broker")
        if [ -z "$2" ]; then
            print_error "Usage: $0 broker <broker> [duration] [rate] [workers] [size]"
            exit 1
        fi
        check_infrastructure
        run_broker_benchmark "$2" "${3:-60}" "${4:-1000}" "${5:-2}" "${6:-1024}"
        ;;
    "comprehensive")
        check_infrastructure
        run_comprehensive_benchmarks "${2:-60}" "${3:-1000}"
        ;;
    "scenarios")
        check_infrastructure
        run_scenario_benchmarks
        ;;
    "stress")
        check_infrastructure
        run_stress_tests
        ;;
    "report")
        generate_report
        ;;
    "list")
        list_results
        ;;
    "clean")
        clean_results
        ;;
    "help"|"--help"|"-h"|"")
        show_usage
        ;;
    *)
        print_error "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac
