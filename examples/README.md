# Pythia Framework Examples

This directory contains practical examples demonstrating how to use Pythia for real-world message processing scenarios.

## 🚀 Quick Start

### 1. Install Pythia

```bash
# Basic installation
pip install -e ..

# With broker dependencies
pip install -e ..[kafka,rabbitmq,redis]
```

### 2. Start Message Brokers

```bash
# Start all brokers (Kafka, RabbitMQ, Redis)
docker-compose up -d

# Check services are running
docker-compose ps
```

### 3. Run Your First Example

```bash
# Run Kafka consumer (in one terminal)
python kafka/basic_consumer_producer.py

# Produce test messages (in another terminal)
python kafka/basic_consumer_producer.py produce
```

## 📁 Example Categories

### 🔄 Kafka Examples (`kafka/`)
- **`basic_consumer_producer.py`** - Simple consumer/producer with JSON processing
- **`stream_processing.py`** - Real-time stream processing with windowing
- **`error_handling.py`** - Comprehensive error handling and dead letter queues
- **`multi_partition.py`** - Multi-partition processing with load balancing

### 🐰 RabbitMQ Examples (`rabbitmq/`)
- **`queue_processor.py`** - Basic queue consumer with message acknowledgment
- **`exchange_routing.py`** - Topic and direct exchange routing
- **`priority_queue.py`** - Priority-based message processing
- **`dead_letter_handling.py`** - Dead letter queue implementation

### 🔴 Redis Examples (`redis/`)
- **`streams_consumer.py`** - Redis Streams consumer with consumer groups
- **`pubsub_messaging.py`** - Pub/Sub pattern implementation
- **`list_queue.py`** - List-based queue processing
- **`multi_instance.py`** - Multiple Redis instance handling

### 🏭 Real-World Use Cases (`use-cases/`)
- **`email_notifications/`** - Email notification worker system
- **`file_processing/`** - File processing pipeline
- **`api_sync/`** - API data synchronization
- **`event_driven/`** - Event-driven microservices
- **`batch_jobs/`** - Batch job processing system

### 🔌 Integration Examples (`integrations/`)
- **`database/`** - Database integration (PostgreSQL, MongoDB)
- **`http_apis/`** - HTTP API integration patterns
- **`monitoring/`** - Monitoring and observability setup
- **`docker/`** - Containerization examples

## 🛠️ Development Setup

### Environment Variables

Create a `.env` file for configuration:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=pythia-examples

# RabbitMQ Configuration
RABBITMQ_URL=amqp://guest:guest@localhost:5672

# Redis Configuration
REDIS_URL=redis://localhost:6379

# Logging
PYTHIA_LOG_LEVEL=INFO
```

### Testing Examples

Each example includes its own test data generator:

```bash
# Most examples support 'produce' mode
python kafka/basic_consumer_producer.py produce
python rabbitmq/queue_processor.py send-messages
python redis/streams_consumer.py generate-data
```

## 🏃‍♂️ Running Examples

### Option 1: Direct Execution
```bash
cd examples
python kafka/basic_consumer_producer.py
```

### Option 2: Using Python Module
```bash
python -m examples.kafka.basic_consumer_producer
```

### Option 3: Using Pythia CLI
```bash
pythia run examples/kafka/basic_consumer_producer.py
```

## 📊 Monitoring & Debugging

### View Broker UIs

- **RabbitMQ Management**: http://localhost:15672 (guest/guest)
- **Redis Commander**: http://localhost:8081
- **Kafka**: Use CLI tools or external UI

### Logging

All examples use structured logging. To see detailed logs:

```bash
export PYTHIA_LOG_LEVEL=DEBUG
python kafka/basic_consumer_producer.py
```

### Performance Monitoring

```bash
# Monitor worker performance
pythia monitor worker --worker kafka-example --refresh 2

# Monitor broker connectivity
pythia monitor broker --broker-type kafka
```

## 🚦 Troubleshooting

### Common Issues

1. **"Connection refused"**
   ```bash
   # Make sure brokers are running
   docker-compose ps

   # Restart if needed
   docker-compose restart
   ```

2. **"Topic does not exist"**
   ```bash
   # Kafka auto-creates topics, but you can create manually:
   docker exec pythia-kafka kafka-topics --create --topic user-events --bootstrap-server localhost:9092
   ```

3. **"ModuleNotFoundError"**
   ```bash
   # Install Pythia in development mode
   pip install -e ..
   ```

### Getting Help

- Check individual example README files for specific instructions
- Use `python example.py --help` for command-line options
- Enable debug logging with `PYTHIA_LOG_LEVEL=DEBUG`

## 📚 Learning Path

**Recommended order for beginners:**

1. Start with `kafka/basic_consumer_producer.py` - Learn fundamentals
2. Try `rabbitmq/queue_processor.py` - Different broker pattern
3. Explore `use-cases/email_notifications/` - Real-world scenario
4. Advanced: `integrations/monitoring/` - Production patterns

## 🤝 Contributing Examples

Want to add an example? Follow this structure:

```
examples/category/example_name.py
examples/category/README.md          # Specific instructions
examples/category/docker-compose.yml  # If special setup needed
examples/category/.env.example       # Environment template
```

Each example should be:
- **Self-contained** - Runnable without other examples
- **Well-documented** - Clear docstrings and comments
- **Production-ready** - Include error handling and logging
- **Tested** - Include data generation for testing
