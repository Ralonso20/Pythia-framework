# {{cookiecutter.worker_name|title}}

{{cookiecutter.description}}

**Author:** {{cookiecutter.author}} <{{cookiecutter.email}}>

## Overview

This worker processes messages through a multi-stage pipeline with the following stages:

**Pipeline:** {% for stage in cookiecutter.pipeline_stages.split(',') %}{{stage.strip()}}{% if not loop.last %} → {% endif %}{% endfor %}

**Configuration:**
- Max concurrent stages: {{cookiecutter.max_concurrent_stages}}
- Message source: {% if cookiecutter.broker_type == "kafka" %}Kafka topics: {% for topic in cookiecutter.topics.split(',') %}`{{topic.strip()}}`{% if not loop.last %}, {% endif %}{% endfor %}{% elif cookiecutter.broker_type == "rabbitmq" %}RabbitMQ queue: `{{cookiecutter.topics}}`{% else %}Redis stream: `{{cookiecutter.topics}}`{% endif %}

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Set these environment variables or update the worker configuration:

```bash
{% if cookiecutter.broker_type == "kafka" %}
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS={{cookiecutter.bootstrap_servers}}
KAFKA_GROUP_ID={{cookiecutter.group_id}}
KAFKA_TOPICS={{cookiecutter.topics}}
{% elif cookiecutter.broker_type == "rabbitmq" %}
# RabbitMQ Configuration
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_QUEUE={{cookiecutter.topics}}
{% else %}
# Redis Configuration
REDIS_URL=redis://localhost:6379
REDIS_STREAM={{cookiecutter.topics}}
REDIS_CONSUMER_GROUP={{cookiecutter.group_id}}
{% endif %}

# Worker Configuration
PYTHIA_WORKER_NAME={{cookiecutter.worker_name}}
PYTHIA_LOG_LEVEL=INFO

# Pipeline Configuration
PIPELINE_MAX_CONCURRENT={{cookiecutter.max_concurrent_stages}}
```

### Configuration File

Alternatively, create a `config.yaml` file:

```yaml
worker_name: {{cookiecutter.worker_name}}
log_level: INFO

broker_type: {{cookiecutter.broker_type}}
{% if cookiecutter.broker_type == "kafka" %}
kafka:
  bootstrap_servers: {{cookiecutter.bootstrap_servers}}
  group_id: {{cookiecutter.group_id}}
  topics: [{% for topic in cookiecutter.topics.split(',') %}"{{topic.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}]
{% elif cookiecutter.broker_type == "rabbitmq" %}
rabbitmq:
  url: amqp://guest:guest@localhost:5672/
  queue: {{cookiecutter.topics}}
  durable: true
{% else %}
redis:
  url: redis://localhost:6379
  stream: {{cookiecutter.topics}}
  consumer_group: {{cookiecutter.group_id}}
{% endif %}

# Pipeline settings
pipeline:
  max_concurrent_stages: {{cookiecutter.max_concurrent_stages}}
  stages: [{% for stage in cookiecutter.pipeline_stages.split(',') %}"{{stage.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}]
```

## Usage

### Development

Run with hot reload for development:

```bash
pythia run worker.py --reload
```

### Production

Run the worker directly:

```bash
python worker.py
```

Or with a config file:

```bash
pythia run worker.py --config config.yaml --log-level INFO
```

## Pipeline Processing

This worker uses the `PipelineProcessor` to process messages through multiple stages in sequence, with optional concurrency.

### Pipeline Stages

{% for stage in cookiecutter.pipeline_stages.split(',') %}
#### {{stage.strip()|title}} Stage

{% if stage.strip() == "validate" %}
Validates incoming message data and ensures it meets required criteria.

```python
async def validate_stage(self, data: Any, context: Dict[str, Any]) -> Any:
    # Add your validation logic
    if not data:
        raise ValueError("Empty data received")

    return {"validated": True, "data": data}
```
{% elif stage.strip() == "transform" %}
Transforms the message data into the required format for downstream processing.

```python
async def transform_stage(self, data: Any, context: Dict[str, Any]) -> Any:
    # Add your transformation logic
    transformed = process_data(data)
    return {"transformed": True, "data": transformed}
```
{% elif stage.strip() == "enrich" %}
Enriches the message with additional data from external sources.

```python
async def enrich_stage(self, data: Any, context: Dict[str, Any]) -> Any:
    # Add your enrichment logic
    enriched_data = await fetch_additional_data(data)
    return {**data, "enriched": enriched_data}
```
{% elif stage.strip() == "output" %}
Formats the final output and optionally sends to downstream systems.

```python
async def output_stage(self, data: Any, context: Dict[str, Any]) -> Any:
    # Add your output logic
    final_result = format_output(data)
    await send_to_downstream(final_result)
    return final_result
```
{% else %}
Custom processing stage for your specific business logic.

```python
async def {{stage.strip()}}_stage(self, data: Any, context: Dict[str, Any]) -> Any:
    # Add your custom processing logic
    result = process_{{stage.strip()}}(data)
    return result
```
{% endif %}

{% endfor %}

### Pipeline Context

Each stage receives a `context` dictionary containing:
- Results from previous stages
- Pipeline metadata
- Shared state across stages

```python
async def my_stage(self, data: Any, context: Dict[str, Any]) -> Any:
    # Access previous stage results
    previous_result = context.get("previous_stage_name")

    # Access pipeline metadata
    pipeline_id = context.get("pipeline_id")
    start_time = context.get("start_time")

    # Your processing logic here
    return processed_data
```

{% if cookiecutter.use_pydantic == "yes" %}
### Message Validation

This worker uses Pydantic for message validation. Update the `{{cookiecutter.worker_name|title}}Message` model in `worker.py` to match your message schema:

```python
class {{cookiecutter.worker_name|title}}Message(BaseModel):
    id: str
    timestamp: datetime
    data: dict
    user_id: Optional[str] = None
```
{% endif %}

## Monitoring

{% if cookiecutter.add_metrics == "yes" %}
### Pipeline Metrics

View worker statistics including pipeline-specific metrics:

```bash
pythia monitor worker --worker {{cookiecutter.worker_name}}
```

Pipeline metrics available:
- `total_pipeline_runs`: Total number of pipeline executions
- `successful_pipeline_runs`: Number of successful completions
- `pipeline_success_rate`: Success rate percentage
- `stage_metrics`: Per-stage processing and error counts
{% endif %}

### Logs

Monitor pipeline processing logs:

```bash
pythia monitor logs worker.log --follow
```

## Testing

Create test messages and send them to your message broker:

{% if cookiecutter.broker_type == "kafka" %}
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['{{cookiecutter.bootstrap_servers}}'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send test message
test_message = {
    "data": {"user_id": "123", "action": "test"},
    "timestamp": "2023-01-01T00:00:00Z"
}
producer.send('{{cookiecutter.topics.split(',')[0].strip()}}', test_message)
producer.flush()
```
{% elif cookiecutter.broker_type == "rabbitmq" %}
```python
import aio_pika
import json
import asyncio

async def send_test_message():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/")

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("{{cookiecutter.topics}}", durable=True)

        test_message = {
            "data": {"user_id": "123", "action": "test"},
            "timestamp": "2023-01-01T00:00:00Z"
        }

        message = aio_pika.Message(json.dumps(test_message).encode())
        await channel.default_exchange.publish(message, routing_key="{{cookiecutter.topics}}")

asyncio.run(send_test_message())
```
{% else %}
```python
import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0)

# Send test message
test_message = {
    "data": {"user_id": "123", "action": "test"},
    "timestamp": "2023-01-01T00:00:00Z"
}

r.xadd("{{cookiecutter.topics}}", test_message)
```
{% endif %}

## Pipeline Benefits

- **Modular Design**: Each stage can be developed and tested independently
- **Concurrent Processing**: Multiple stages can run concurrently when data allows
- **Error Isolation**: Failures in one stage don't affect others
- **Scalability**: Easy to add, remove, or modify pipeline stages
- **Monitoring**: Detailed metrics for each stage

## Performance Tuning

### Concurrency Settings

- **Lower concurrency**: More predictable resource usage, easier debugging
- **Higher concurrency**: Better throughput for I/O-bound stages
- **Optimal range**: Start with 2-5 concurrent stages and adjust based on your workload

### Stage Optimization

- Keep stages lightweight and focused
- Use async operations for I/O-bound tasks
- Consider caching for repeated operations
- Monitor stage-specific metrics to identify bottlenecks

## Error Handling

- Individual stage failures are logged and can trigger pipeline retries
- Context is preserved across stage executions
- Failed pipelines can be replayed from specific stages
- Comprehensive error tracking per stage

## Scaling

To scale horizontally, run multiple instances of this worker. The message broker will distribute messages across instances automatically.
