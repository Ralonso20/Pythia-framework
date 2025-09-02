"""
{{cookiecutter.description}}
Author: {{cookiecutter.author}} <{{cookiecutter.email}}>
"""

import asyncio
from typing import Any{% if cookiecutter.use_pydantic == "yes" %}
from pydantic import BaseModel{% endif %}

from pythia import Worker, Message
from pythia.brokers.kafka import KafkaConsumer
from pythia.logging.setup import get_pythia_logger

{% if cookiecutter.use_pydantic == "yes" %}
# Define your message schema using Pydantic
class {{cookiecutter.worker_name|title}}Message(BaseModel):
    """Message schema for {{cookiecutter.worker_name}}"""

    # Define your message structure here
    # Example fields:
    # id: str
    # timestamp: datetime
    # data: dict
    # user_id: Optional[str] = None

    # For now, we'll accept any data
    data: Any
{% endif %}


class {{cookiecutter.worker_name|title}}Worker(Worker):
    """
    {{cookiecutter.description}}

    Consumes messages from Kafka topic: {{cookiecutter.topics}}
    """

    # Configure Kafka consumer
    source = KafkaConsumer(
        topics=[{% for topic in cookiecutter.topics.split(',') %}"{{topic.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}],
        group_id="{{cookiecutter.group_id}}",
        bootstrap_servers="{{cookiecutter.bootstrap_servers}}",
        auto_offset_reset="{{cookiecutter.auto_offset_reset}}",
        enable_auto_commit={{cookiecutter.enable_auto_commit}},
        max_poll_records={{cookiecutter.max_poll_records}}
    )

    def __init__(self):
        super().__init__()
        self.logger = get_pythia_logger("{{cookiecutter.worker_name}}")

    {% if cookiecutter.add_health_checks == "yes" %}
    async def startup(self):
        """Worker startup logic"""
        self.logger.info("{{cookiecutter.worker_name|title}}Worker starting up...")
        # Add any startup logic here

    async def shutdown(self):
        """Worker shutdown logic"""
        self.logger.info("{{cookiecutter.worker_name|title}}Worker shutting down...")
        # Add any cleanup logic here

    async def health_check(self) -> bool:
        """Health check for this worker"""
        # Add custom health checks here
        # Return False if worker is unhealthy
        return True
    {% endif %}

    async def process(self, message: Message) -> Any:
        """
        Process incoming Kafka message

        Args:
            message: Incoming message from Kafka

        Returns:
            Processed result (optional)
        """

        try:
            {% if cookiecutter.use_pydantic == "yes" %}
            # Validate message using Pydantic model
            validated_message = {{cookiecutter.worker_name|title}}Message(**message.body)

            self.logger.info(
                "Processing message",
                message_id=message.message_id,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                data_keys=list(validated_message.data.keys()) if isinstance(validated_message.data, dict) else "non-dict"
            )

            # Your business logic here
            result = await self.process_business_logic(validated_message)
            {% else %}
            self.logger.info(
                "Processing message",
                message_id=message.message_id,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset
            )

            # Your business logic here
            result = await self.process_business_logic(message.body)
            {% endif %}

            self.logger.debug("Message processed successfully", result=result)
            return result

        except Exception as e:
            self.logger.error(
                "Error processing message",
                error=str(e),
                message_id=message.message_id,
                topic=message.topic
            )
            # Re-raise to trigger retry logic
            raise

    {% if cookiecutter.use_pydantic == "yes" %}
    async def process_business_logic(self, validated_message: {{cookiecutter.worker_name|title}}Message) -> Any:
    {% else %}
    async def process_business_logic(self, message_data: Any) -> Any:
    {% endif %}
        """
        Implement your business logic here

        Args:
            {% if cookiecutter.use_pydantic == "yes" %}validated_message: Validated Pydantic message{% else %}message_data: Raw message data{% endif %}

        Returns:
            Processing result
        """

        # TODO: Replace with your actual business logic
        {% if cookiecutter.use_pydantic == "yes" %}
        # Example: Transform the data
        transformed_data = {
            "processed": True,
            "original_data": validated_message.data,
            "worker": "{{cookiecutter.worker_name}}"
        }
        {% else %}
        # Example: Transform the data
        transformed_data = {
            "processed": True,
            "original_data": message_data,
            "worker": "{{cookiecutter.worker_name}}"
        }
        {% endif %}

        # Simulate some processing time
        await asyncio.sleep(0.1)

        return transformed_data

    {% if cookiecutter.add_metrics == "yes" %}
    def get_custom_stats(self) -> dict:
        """Return custom metrics for this worker"""
        base_stats = self.get_stats()

        # Add custom metrics
        custom_stats = {
            "custom_metric_1": 42,
            "custom_metric_2": "healthy",
            # Add your custom metrics here
        }

        return {**base_stats, "custom": custom_stats}
    {% endif %}


if __name__ == "__main__":
    # Create and run the worker
    worker = {{cookiecutter.worker_name|title}}Worker()

    try:
        worker.run_sync()
    except KeyboardInterrupt:
        print("\n👋 Worker stopped by user")
    except Exception as e:
        print(f"❌ Worker failed: {e}")
        raise
