# {{cookiecutter.worker_name|title}}

{{cookiecutter.description}}

**Author:** {{cookiecutter.author}} <{{cookiecutter.email}}>

## Overview

This worker consumes messages from Kafka topic(s): {% for topic in cookiecutter.topics.split(',') %}`{{topic.strip()}}`{% if not loop.last %}, {% endif %}{% endfor %}

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

# Optional: Advanced Kafka Settings
KAFKA_AUTO_OFFSET_RESET={{cookiecutter.auto_offset_reset}}
KAFKA_ENABLE_AUTO_COMMIT={{cookiecutter.enable_auto_commit}}
KAFKA_MAX_POLL_RECORDS={{cookiecutter.max_poll_records}}
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
  auto_offset_reset: {{cookiecutter.auto_offset_reset}}
  enable_auto_commit: {{cookiecutter.enable_auto_commit}}
  max_poll_records: {{cookiecutter.max_poll_records}}
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

## Message Processing

{% if cookiecutter.use_pydantic == "yes" %}
This worker uses Pydantic for message validation. Update the `{{cookiecutter.worker_name|title}}Message` model in `worker.py` to match your message schema:

```python
class {{cookiecutter.worker_name|title}}Message(BaseModel):
    id: str
    timestamp: datetime
    data: dict
    user_id: Optional[str] = None
```
{% else %}
This worker processes raw message data. The `process_business_logic` method receives the message body directly.
{% endif %}

### Business Logic

Implement your processing logic in the `process_business_logic` method:

```python
async def process_business_logic(self, {% if cookiecutter.use_pydantic == "yes" %}validated_message{% else %}message_data{% endif %}):
    # Your business logic here
    result = transform_data({% if cookiecutter.use_pydantic == "yes" %}validated_message.data{% else %}message_data{% endif %})
    return result
```

## Monitoring

{% if cookiecutter.add_health_checks == "yes" %}
### Health Checks

The worker includes health check endpoints. Customize the `health_check` method for your specific needs.
{% endif %}

{% if cookiecutter.add_metrics == "yes" %}
### Metrics

View worker statistics:

```bash
pythia monitor worker --worker {{cookiecutter.worker_name}}
```

Custom metrics are available in the `get_custom_stats` method.
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

producer = KafkaProducer(
    bootstrap_servers=['{{cookiecutter.bootstrap_servers}}'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send test message
test_message = {"data": {"hello": "world"}}
producer.send('{{cookiecutter.topics.split(',')[0].strip()}}', test_message)
producer.flush()
```

## Error Handling

The worker includes automatic retry logic for failed messages. Customize error handling in the `process` method.

## Scaling

To scale horizontally, run multiple instances of this worker with the same `group_id`. Kafka will automatically distribute partitions across instances.
