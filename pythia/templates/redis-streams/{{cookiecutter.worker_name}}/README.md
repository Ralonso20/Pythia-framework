# {{cookiecutter.worker_name|title}}

{{cookiecutter.description}}

**Author:** {{cookiecutter.author}} <{{cookiecutter.email}}>

## Overview

This worker consumes messages from Redis stream: `{{cookiecutter.stream}}`

**Configuration:**
- Consumer group: `{{cookiecutter.consumer_group}}`
- Consumer name: `{{cookiecutter.consumer_name}}`
- Block timeout: {{cookiecutter.block_timeout}}ms

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Set these environment variables or update the worker configuration:

```bash
# Redis Configuration
REDIS_URL={{cookiecutter.redis_url}}
REDIS_STREAM={{cookiecutter.stream}}
REDIS_CONSUMER_GROUP={{cookiecutter.consumer_group}}
REDIS_CONSUMER_NAME={{cookiecutter.consumer_name}}

# Worker Configuration
PYTHIA_WORKER_NAME={{cookiecutter.worker_name}}
PYTHIA_LOG_LEVEL=INFO
```

### Configuration File

Alternatively, create a `config.yaml` file:

```yaml
worker_name: {{cookiecutter.worker_name}}
log_level: INFO

broker_type: redis_streams
redis:
  url: {{cookiecutter.redis_url}}
  stream: {{cookiecutter.stream}}
  consumer_group: {{cookiecutter.consumer_group}}
  consumer_name: {{cookiecutter.consumer_name}}
  block_timeout: {{cookiecutter.block_timeout}}
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

## Redis Streams

Redis Streams provides a powerful messaging and data streaming platform with:

- **Persistence**: Messages are stored and persist restarts
- **Consumer Groups**: Multiple consumers can share workload
- **Acknowledgment**: Messages are acknowledged when processed
- **Replay**: Ability to replay messages from any point in time

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

### Business Logic

Implement your processing logic in the `process_business_logic` method:

```python
async def process_business_logic(self, {% if cookiecutter.use_pydantic == "yes" %}validated_message{% else %}message_data{% endif %}):
    # Your business logic here
    result = transform_data({% if cookiecutter.use_pydantic == "yes" %}validated_message.data{% else %}message_data{% endif %})
    return result
```

## Monitoring

{% if cookiecutter.add_metrics == "yes" %}
### Metrics

View worker statistics:

```bash
pythia monitor worker --worker {{cookiecutter.worker_name}}
```

Custom metrics available:
- `total_messages_processed`: Number of messages processed
- `stream_name`: Redis stream name
- `consumer_group`: Consumer group name
- `consumer_name`: Consumer name
{% endif %}

### Logs

Monitor worker logs:

```bash
pythia monitor logs worker.log --follow
```

## Testing

Create test messages and add them to your Redis stream:

```python
import redis
import json

# Connect to Redis
r = redis.Redis.from_url("{{cookiecutter.redis_url}}")

# Add test messages to stream
test_messages = [
    {"data": {"user_id": "123", "action": "login"}},
    {"data": {"user_id": "456", "action": "purchase", "amount": 99.99}},
    {"data": {"user_id": "789", "action": "logout"}}
]

for msg in test_messages:
    message_id = r.xadd("{{cookiecutter.stream}}", msg)
    print(f"Added message: {message_id}")

# Check stream length
length = r.xlen("{{cookiecutter.stream}}")
print(f"Stream length: {length}")
```

### Redis CLI Commands

Useful Redis CLI commands for monitoring:

```bash
# View stream info
redis-cli XINFO STREAM {{cookiecutter.stream}}

# View consumer group info
redis-cli XINFO GROUPS {{cookiecutter.stream}}

# View consumer group consumers
redis-cli XINFO CONSUMERS {{cookiecutter.stream}} {{cookiecutter.consumer_group}}

# Read messages from stream
redis-cli XRANGE {{cookiecutter.stream}} - +

# Add test message
redis-cli XADD {{cookiecutter.stream}} * data '{"test": "message"}'
```

## Error Handling

The worker includes automatic retry logic for failed messages. Redis Streams ensures messages are not lost and can be retried by the same or different consumers.

### Message Acknowledgment

Messages are automatically acknowledged after successful processing. Failed messages remain in the pending entries list and can be retried.

## Scaling

To scale horizontally:

1. **Same Consumer Group**: Run multiple instances with the same `consumer_group` but different `consumer_name`. Redis will distribute messages across consumers.

2. **Different Consumer Groups**: Create multiple consumer groups for different processing patterns of the same stream.

```bash
# Scale within same group
REDIS_CONSUMER_NAME=worker-1 python worker.py &
REDIS_CONSUMER_NAME=worker-2 python worker.py &
REDIS_CONSUMER_NAME=worker-3 python worker.py &
```

## Stream Management

### Trimming Streams

To prevent streams from growing indefinitely:

```python
import redis
r = redis.Redis.from_url("{{cookiecutter.redis_url}}")

# Keep only last 1000 messages
r.xtrim("{{cookiecutter.stream}}", maxlen=1000, approximate=True)

# Keep messages from last 24 hours
import time
yesterday = int((time.time() - 86400) * 1000)
r.xtrim("{{cookiecutter.stream}}", minid=yesterday, approximate=True)
```

### Dead Letter Handling

Handle messages that fail repeatedly:

```python
# Check pending messages
pending = r.xpending("{{cookiecutter.stream}}", "{{cookiecutter.consumer_group}}")

# Claim old pending messages
old_messages = r.xautoclaim(
    "{{cookiecutter.stream}}",
    "{{cookiecutter.consumer_group}}",
    "{{cookiecutter.consumer_name}}",
    min_idle_time=60000  # 1 minute
)
```
