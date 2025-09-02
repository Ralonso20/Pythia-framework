"""
{{cookiecutter.description}}
Author: {{cookiecutter.author}} <{{cookiecutter.email}}>
"""

import asyncio
from typing import Any, List{% if cookiecutter.use_pydantic == "yes" %}
from pydantic import BaseModel{% endif %}

from pythia import Worker, Message
from pythia.brokers.kafka import KafkaConsumer
from pythia.logging.setup import get_pythia_logger
from pythia.processors.batch import BatchMessageProcessor

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

    Processes messages in batches from Kafka topic: {{cookiecutter.topics}}
    Batch size: {{cookiecutter.batch_size}}, Max wait time: {{cookiecutter.max_wait_time}}s
    """

    # Configure Kafka consumer
    source = KafkaConsumer(
        topics=[{% for topic in cookiecutter.topics.split(',') %}"{{topic.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}],
        group_id="{{cookiecutter.group_id}}",
        bootstrap_servers="{{cookiecutter.bootstrap_servers}}"
    )

    def __init__(self):
        super().__init__()
        self.logger = get_pythia_logger("{{cookiecutter.worker_name}}")

        # Initialize batch processor
        self.batch_processor = BatchMessageProcessor(
            batch_size={{cookiecutter.batch_size}},
            max_wait_time={{cookiecutter.max_wait_time}}
        )

    {% if cookiecutter.add_metrics == "yes" %}
    async def startup(self):
        """Worker startup logic"""
        self.logger.info("{{cookiecutter.worker_name|title}}Worker starting up...")
        # Initialize metrics tracking
        self.total_batches_processed = 0
        self.total_messages_processed = 0

    async def shutdown(self):
        """Worker shutdown logic"""
        self.logger.info("{{cookiecutter.worker_name|title}}Worker shutting down...")
        # Log final statistics
        self.logger.info(
            "Final statistics",
            total_batches=self.total_batches_processed,
            total_messages=self.total_messages_processed
        )
    {% endif %}

    async def process(self, message: Message) -> Any:
        """
        Process incoming Kafka message using batch processing

        Args:
            message: Incoming message from Kafka

        Returns:
            Processed result (optional)
        """

        try:
            {% if cookiecutter.use_pydantic == "yes" %}
            # Validate message using Pydantic model
            validated_message = {{cookiecutter.worker_name|title}}Message(**message.body)
            processed_message = validated_message
            {% else %}
            processed_message = message.body
            {% endif %}

            # Add message to batch processor
            result = await self.batch_processor.process_message(
                processed_message,
                self.process_batch
            )

            if result:
                self.logger.debug("Batch processed successfully", batch_size=len(result))
                {% if cookiecutter.add_metrics == "yes" %}
                self.total_batches_processed += 1
                self.total_messages_processed += len(result)
                {% endif %}

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
    async def process_batch(self, batch: List[{{cookiecutter.worker_name|title}}Message]) -> List[Any]:
    {% else %}
    async def process_batch(self, batch: List[Any]) -> List[Any]:
    {% endif %}
        """
        Process a batch of messages

        Args:
            batch: List of messages to process as a batch

        Returns:
            List of processed results
        """

        self.logger.info(f"Processing batch of {len(batch)} messages")

        # TODO: Replace with your actual batch processing logic
        results = []

        for i, {% if cookiecutter.use_pydantic == "yes" %}validated_message{% else %}message_data{% endif %} in enumerate(batch):
            try:
                # Your batch processing logic here
                result = {
                    "batch_position": i,
                    "processed": True,
                    {% if cookiecutter.use_pydantic == "yes" %}
                    "original_data": validated_message.data,
                    {% else %}
                    "original_data": message_data,
                    {% endif %}
                    "worker": "{{cookiecutter.worker_name}}",
                    "batch_size": len(batch)
                }

                results.append(result)

            except Exception as e:
                self.logger.error(f"Error processing message {i} in batch", error=str(e))
                # Add error result or skip depending on your requirements
                results.append({
                    "batch_position": i,
                    "processed": False,
                    "error": str(e)
                })

        # Simulate some batch processing time
        await asyncio.sleep(0.1 * len(batch))

        self.logger.debug(f"Batch processing completed", successful=len([r for r in results if r.get('processed')]))

        return results

    {% if cookiecutter.add_metrics == "yes" %}
    def get_custom_stats(self) -> dict:
        """Return custom metrics for this worker"""
        base_stats = self.get_stats()

        # Add batch-specific metrics
        custom_stats = {
            "total_batches_processed": getattr(self, 'total_batches_processed', 0),
            "total_messages_processed": getattr(self, 'total_messages_processed', 0),
            "batch_size": {{cookiecutter.batch_size}},
            "max_wait_time": {{cookiecutter.max_wait_time}},
            "avg_messages_per_batch": (
                getattr(self, 'total_messages_processed', 0) /
                max(getattr(self, 'total_batches_processed', 1), 1)
            ),
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
