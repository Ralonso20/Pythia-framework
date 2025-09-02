# {{cookiecutter.worker_name|title}}

{{cookiecutter.description}}

**Author:** {{cookiecutter.author}} <{{cookiecutter.email}}>

## Overview

This worker consumes messages from RabbitMQ queue: `{{cookiecutter.queue}}`

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Set these environment variables or update the worker configuration:

```bash
# RabbitMQ Configuration
RABBITMQ_URL={{cookiecutter.rabbitmq_url}}
RABBITMQ_QUEUE={{cookiecutter.queue}}
{% if cookiecutter.exchange %}RABBITMQ_EXCHANGE={{cookiecutter.exchange}}{% endif %}
{% if cookiecutter.routing_key %}RABBITMQ_ROUTING_KEY={{cookiecutter.routing_key}}{% endif %}

# Worker Configuration
PYTHIA_WORKER_NAME={{cookiecutter.worker_name}}
PYTHIA_LOG_LEVEL=INFO
```

### Configuration File

Alternatively, create a `config.yaml` file:

```yaml
worker_name: {{cookiecutter.worker_name}}
log_level: INFO

broker_type: rabbitmq
rabbitmq:
  url: {{cookiecutter.rabbitmq_url}}
  queue: {{cookiecutter.queue}}
  {% if cookiecutter.exchange %}exchange: {{cookiecutter.exchange}}{% endif %}
  {% if cookiecutter.routing_key %}routing_key: {{cookiecutter.routing_key}}{% endif %}
  durable: {{cookiecutter.durable}}
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

Create test messages and send them to your RabbitMQ queue:

```python
import aio_pika
import json
import asyncio

async def send_test_message():
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust("{{cookiecutter.rabbitmq_url}}")

    async with connection:
        channel = await connection.channel()

        # Declare queue (if needed)
        queue = await channel.declare_queue("{{cookiecutter.queue}}", durable={{cookiecutter.durable}})

        # Send test message
        test_message = {"data": {"hello": "world"}}
        message = aio_pika.Message(
            json.dumps(test_message).encode(),
            content_type="application/json"
        )

        await channel.default_exchange.publish(
            message,
            routing_key="{{cookiecutter.queue}}"
        )

        print("Test message sent!")

# Run the test
asyncio.run(send_test_message())
```

## Error Handling

The worker includes automatic retry logic for failed messages. Customize error handling in the `process` method.

## Scaling

To scale horizontally, run multiple instances of this worker. RabbitMQ will distribute messages across multiple consumers automatically.
