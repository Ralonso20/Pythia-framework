#!/bin/bash
set -e

echo "🚀 Setting up Pythia Benchmarking Environment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose is not installed. Please install it first."
    exit 1
fi

print_status "Checking system requirements..."

# Check available memory (need at least 4GB)
AVAILABLE_MEMORY=$(free -g | awk '/^Mem:/{print $2}')
if [ "$AVAILABLE_MEMORY" -lt 4 ]; then
    print_warning "System has less than 4GB RAM. Performance tests may be limited."
fi

# Create necessary directories
print_status "Creating directories and setting permissions..."
mkdir -p {prometheus_data,grafana_data,influxdb_data,redis_data}
sudo chown -R 472:472 grafana_data  # Grafana user ID
sudo chown -R 65534:65534 prometheus_data  # nobody user ID

# Build Locust image
print_status "Building Locust Docker image..."
docker build -f Dockerfile.locust -t pythia-locust .

# Start the monitoring stack
print_status "Starting monitoring stack..."
docker-compose -f docker-compose.monitoring.yml up -d

# Wait for services to be healthy
print_status "Waiting for services to start..."

# Function to wait for service
wait_for_service() {
    local service_name=$1
    local port=$2
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:$port > /dev/null 2>&1; then
            print_status "$service_name is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    print_error "$service_name failed to start"
    return 1
}

# Wait for key services
wait_for_service "Prometheus" 9090
wait_for_service "Grafana" 3000
wait_for_service "InfluxDB" 8086

# Setup RabbitMQ management and metrics
print_status "Configuring RabbitMQ..."
sleep 10  # Give RabbitMQ time to fully start

# Enable RabbitMQ management and prometheus plugins
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_management rabbitmq_prometheus

# Create performance test exchanges and queues
docker exec rabbitmq rabbitmqadmin declare exchange name=benchmark-exchange type=direct
docker exec rabbitmq rabbitmqadmin declare queue name=benchmark-queue durable=true
docker exec rabbitmq rabbitmqadmin declare binding source=benchmark-exchange destination=benchmark-queue routing_key=test

# Create Kafka topics for benchmarking
print_status "Setting up Kafka topics..."
sleep 5  # Give Kafka time to start
docker exec kafka kafka-topics.sh --create --topic benchmark-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 || true
docker exec kafka kafka-topics.sh --create --topic high-throughput --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092 || true

# Configure Redis for performance
print_status "Configuring Redis..."
docker exec redis redis-cli CONFIG SET save ""  # Disable RDB snapshots for performance
docker exec redis redis-cli CONFIG SET appendfsync everysec  # Optimize AOF

print_status "Benchmark environment is ready!"
echo ""
echo "📊 Access URLs:"
echo "   Grafana:    http://localhost:3000 (admin/admin)"
echo "   Prometheus: http://localhost:9090"
echo "   RabbitMQ:   http://localhost:15672 (guest/guest)"
echo "   InfluxDB:   http://localhost:8086 (admin/pythia-benchmark)"
echo "   Locust:     http://localhost:8089"
echo ""
echo "🔧 Broker Endpoints:"
echo "   Kafka:      localhost:9092"
echo "   RabbitMQ:   localhost:5672"
echo "   Redis:      localhost:6379"
echo ""
print_status "To run benchmarks, use: ./run_benchmarks.sh"
