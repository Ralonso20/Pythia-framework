"""
{{cookiecutter.description}}
Author: {{cookiecutter.author}} <{{cookiecutter.email}}>
"""

import asyncio
from typing import Any, Dict{% if cookiecutter.use_pydantic == "yes" %}
from pydantic import BaseModel{% endif %}

from pythia import Worker, Message
from pythia.brokers.{% if cookiecutter.broker_type == "kafka" %}kafka{% elif cookiecutter.broker_type == "rabbitmq" %}rabbitmq{% else %}redis{% endif %} import {% if cookiecutter.broker_type == "kafka" %}KafkaConsumer{% elif cookiecutter.broker_type == "rabbitmq" %}RabbitMQConsumer{% else %}RedisStreamsConsumer{% endif %}
from pythia.logging.setup import get_pythia_logger
from pythia.processors.pipeline import PipelineProcessor

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

    Pipeline stages: {% for stage in cookiecutter.pipeline_stages.split(',') %}{{stage.strip()}}{% if not loop.last %} → {% endif %}{% endfor %}
    Max concurrent stages: {{cookiecutter.max_concurrent_stages}}
    """

    # Configure message broker
    {% if cookiecutter.broker_type == "kafka" %}
    source = KafkaConsumer(
        topics=[{% for topic in cookiecutter.topics.split(',') %}"{{topic.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}],
        group_id="{{cookiecutter.group_id}}",
        bootstrap_servers="{{cookiecutter.bootstrap_servers}}"
    )
    {% elif cookiecutter.broker_type == "rabbitmq" %}
    source = RabbitMQConsumer(
        queue="{{cookiecutter.topics}}",
        durable=True
    )
    {% else %}
    source = RedisStreamsConsumer(
        stream="{{cookiecutter.topics}}",
        consumer_group="{{cookiecutter.group_id}}"
    )
    {% endif %}

    def __init__(self):
        super().__init__()
        self.logger = get_pythia_logger("{{cookiecutter.worker_name}}")

        # Initialize pipeline processor with stages
        pipeline_stages = [{% for stage in cookiecutter.pipeline_stages.split(',') %}"{{stage.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}]
        self.pipeline_processor = PipelineProcessor(
            stages=pipeline_stages,
            max_concurrent={{cookiecutter.max_concurrent_stages}}
        )

        {% if cookiecutter.add_metrics == "yes" %}
        # Initialize metrics
        self.stage_metrics = {stage: {"processed": 0, "failed": 0} for stage in pipeline_stages}
        {% endif %}

    {% if cookiecutter.add_metrics == "yes" %}
    async def startup(self):
        """Worker startup logic"""
        self.logger.info("{{cookiecutter.worker_name|title}}Worker starting up...")
        self.total_pipeline_runs = 0
        self.successful_pipeline_runs = 0

    async def shutdown(self):
        """Worker shutdown logic"""
        self.logger.info("{{cookiecutter.worker_name|title}}Worker shutting down...")
        # Log final statistics
        self.logger.info(
            "Pipeline statistics",
            total_runs=self.total_pipeline_runs,
            successful_runs=self.successful_pipeline_runs,
            success_rate=self.successful_pipeline_runs / max(self.total_pipeline_runs, 1)
        )
    {% endif %}

    async def process(self, message: Message) -> Any:
        """
        Process message through pipeline stages

        Args:
            message: Incoming message

        Returns:
            Final pipeline result
        """

        try:
            {% if cookiecutter.use_pydantic == "yes" %}
            # Validate message using Pydantic model
            validated_message = {{cookiecutter.worker_name|title}}Message(**message.body)
            input_data = validated_message
            {% else %}
            input_data = message.body
            {% endif %}

            self.logger.info(
                "Starting pipeline processing",
                message_id=message.message_id,
                {% if cookiecutter.broker_type == "kafka" %}
                topic=message.topic,
                partition=message.partition,
                offset=message.offset
                {% elif cookiecutter.broker_type == "rabbitmq" %}
                queue=message.queue,
                exchange=message.exchange
                {% else %}
                stream=message.stream
                {% endif %}
            )

            {% if cookiecutter.add_metrics == "yes" %}
            self.total_pipeline_runs += 1
            {% endif %}

            # Process through pipeline
            result = await self.pipeline_processor.process(
                data=input_data,
                stage_functions={
                    {% for stage in cookiecutter.pipeline_stages.split(',') %}
                    "{{stage.strip()}}": self.{{stage.strip()}}_stage,{% endfor %}
                }
            )

            {% if cookiecutter.add_metrics == "yes" %}
            self.successful_pipeline_runs += 1
            {% endif %}

            self.logger.debug("Pipeline processing completed", result=result)
            return result

        except Exception as e:
            self.logger.error(
                "Error in pipeline processing",
                error=str(e),
                message_id=message.message_id
            )
            # Re-raise to trigger retry logic
            raise

    # Pipeline Stage Methods
    {% for stage in cookiecutter.pipeline_stages.split(',') %}

    async def {{stage.strip()}}_stage(self, data: Any, context: Dict[str, Any]) -> Any:
        """
        {{stage.strip()|title}} stage of the pipeline

        Args:
            data: Input data for this stage
            context: Pipeline context with previous stage results

        Returns:
            Processed data for next stage
        """

        try:
            self.logger.debug("Processing {{stage.strip()}} stage", stage_input_type=type(data).__name__)

            # TODO: Implement your {{stage.strip()}} logic here
            {% if stage.strip() == "validate" %}
            # Example validation logic
            if not data:
                raise ValueError("Empty data received")

            # Add your validation rules
            processed_data = {
                "validated": True,
                "original": data,
                "stage": "{{stage.strip()}}"
            }
            {% elif stage.strip() == "transform" %}
            # Example transformation logic
            processed_data = {
                "transformed": True,
                "data": data.get("data") if isinstance(data, dict) else data,
                "stage": "{{stage.strip()}}"
            }
            {% elif stage.strip() == "enrich" %}
            # Example enrichment logic
            processed_data = {
                **data if isinstance(data, dict) else {"original": data},
                "enriched": True,
                "enriched_at": "2023-01-01T00:00:00Z",  # Replace with actual timestamp
                "stage": "{{stage.strip()}}"
            }
            {% elif stage.strip() == "output" %}
            # Example output formatting logic
            processed_data = {
                "final_result": data,
                "pipeline_completed": True,
                "stage": "{{stage.strip()}}"
            }
            {% else %}
            # Generic stage processing
            processed_data = {
                "processed_by": "{{stage.strip()}}",
                "data": data,
                "stage": "{{stage.strip()}}"
            }
            {% endif %}

            # Simulate some processing time
            await asyncio.sleep(0.05)

            {% if cookiecutter.add_metrics == "yes" %}
            self.stage_metrics["{{stage.strip()}}"]["processed"] += 1
            {% endif %}

            self.logger.debug("{{stage.strip()|title}} stage completed")
            return processed_data

        except Exception as e:
            {% if cookiecutter.add_metrics == "yes" %}
            self.stage_metrics["{{stage.strip()}}"]["failed"] += 1
            {% endif %}
            self.logger.error("Error in {{stage.strip()}} stage", error=str(e))
            raise
    {% endfor %}

    {% if cookiecutter.add_metrics == "yes" %}
    def get_custom_stats(self) -> dict:
        """Return custom pipeline metrics"""
        base_stats = self.get_stats()

        # Add pipeline-specific metrics
        custom_stats = {
            "total_pipeline_runs": getattr(self, 'total_pipeline_runs', 0),
            "successful_pipeline_runs": getattr(self, 'successful_pipeline_runs', 0),
            "pipeline_success_rate": (
                getattr(self, 'successful_pipeline_runs', 0) /
                max(getattr(self, 'total_pipeline_runs', 1), 1)
            ),
            "stage_metrics": self.stage_metrics,
            "pipeline_stages": [{% for stage in cookiecutter.pipeline_stages.split(',') %}"{{stage.strip()}}"{% if not loop.last %}, {% endif %}{% endfor %}],
            "max_concurrent_stages": {{cookiecutter.max_concurrent_stages}},
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
