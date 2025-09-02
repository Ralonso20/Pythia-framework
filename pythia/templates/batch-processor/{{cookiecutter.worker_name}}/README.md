# {{cookiecutter.worker_name|title}}

{{cookiecutter.description}}

**Author:** {{cookiecutter.author}} <{{cookiecutter.email}}>

## Overview

This worker processes messages in batches from Kafka topic(s): {% for topic in cookiecutter.topics.split(',') %}`{{topic.strip()}}`{% if not loop.last %}, {% endif %}{% endfor %}

**Batch Configuration:**
- Batch size: {{cookiecutter.batch_size}} messages
- Max wait time: {{cookiecutter.max_wait_time}} seconds

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Set these environment variables or update the worker configuration:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS={{cookiecutter.bootstrap_servers}}
KAFKA_GROUP_ID={{cookiecutter.group_id}}
KAFKA_TOPICS={{cookiecutter.topics}}

# Worker Configuration
PYTHIA_WORKER_NAME={{cookiecutter.worker_name}}
PYTHIA_LOG_LEVEL=INFO

# Batch Processing Configuration
BATCH_SIZE={{cookiecutter.batch_size}}
MAX_WAIT_TIME={{cookiecutter.max_wait_time}}
```

### Configuration File

Alternatively, create a `config.yaml` file:

```yaml
worker_name: {{cookiecutter.worker_name}}
log_level: INFO

broker_type: kafka
kafka:
  bootstrap_servers: {{cookiecutter.bootstrap_servers}}
  group_id: {{cookiecutter.group_id}}
  topics: [{% for topic in cookiecutter.topics.split(',') %}"{{topic.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}]

# Batch processing settings
batch:
  size: {{cookiecutter.batch_size}}
  max_wait_time: {{cookiecutter.max_wait_time}}
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

## Batch Processing

This worker uses the `BatchMessageProcessor` to collect messages and process them in batches for improved efficiency.

### How it Works

1. **Message Collection**: Messages are collected up to the configured batch size ({{cookiecutter.batch_size}})
2. **Time-based Flushing**: If the batch doesn't fill within {{cookiecutter.max_wait_time}} seconds, it processes the partial batch
3. **Batch Processing**: All messages in the batch are processed together in the `process_batch` method

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
{% else %}
### Message Processing

This worker processes raw message data. The `process_batch` method receives a list of message bodies directly.
{% endif %}

### Business Logic

Implement your batch processing logic in the `process_batch` method:

```python
async def process_batch(self, {% if cookiecutter.use_pydantic == "yes" %}batch: List[{{cookiecutter.worker_name|title}}Message]{% else %}batch: List[Any]{% endif %}) -> List[Any]:
    # Process all messages in the batch together
    results = []

    for {% if cookiecutter.use_pydantic == "yes" %}validated_message{% else %}message_data{% endif %} in batch:
        # Your processing logic here
        result = process_single_item({% if cookiecutter.use_pydantic == "yes" %}validated_message.data{% else %}message_data{% endif %})
        results.append(result)

    return results
```

## Benefits of Batch Processing

- **Higher Throughput**: Process multiple messages together
- **Reduced I/O**: Fewer database/API calls by batching operations
- **Better Resource Utilization**: More efficient use of system resources
- **Transactional Processing**: Process related messages together

## Monitoring

{% if cookiecutter.add_metrics == "yes" %}
### Metrics

View worker statistics including batch-specific metrics:

```bash
pythia monitor worker --worker {{cookiecutter.worker_name}}
```

Custom batch metrics available:
- `total_batches_processed`: Number of batches processed
- `total_messages_processed`: Total number of messages
- `avg_messages_per_batch`: Average messages per batch
- `batch_size`: Configured batch size
- `max_wait_time`: Configured max wait time
{% endif %}

### Logs

Monitor worker logs:

```bash
pythia monitor logs worker.log --follow
```

## Testing

Create test messages and send them to your Kafka topic:

```python
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['{{cookiecutter.bootstrap_servers}}'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send multiple test messages to see batch processing
for i in range(15):  # Send more than batch size
    test_message = {"data": {"message_id": i, "timestamp": time.time()}}
    producer.send('{{cookiecutter.topics.split(',')[0].strip()}}', test_message)

producer.flush()
```

## Performance Tuning

### Batch Size Optimization

- **Smaller batches**: Lower latency, higher overhead
- **Larger batches**: Higher throughput, higher latency
- **Optimal range**: Start with 10-100 messages and adjust based on your use case

### Wait Time Configuration

- **Shorter wait time**: More responsive but potentially smaller batches
- **Longer wait time**: Fuller batches but higher latency
- **Balance**: Consider your latency requirements vs. throughput needs

## Error Handling

The worker includes automatic retry logic for failed messages. Individual message failures within a batch are logged but don't stop the entire batch processing.

## Scaling

To scale horizontally, run multiple instances of this worker with the same `group_id`. Kafka will automatically distribute partitions across instances.
