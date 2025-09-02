"""
Kafka producer implementation using confluent-kafka
"""

import json
from typing import Any, Dict, List, Optional, Union, cast
from confluent_kafka import Producer

from ..base import BaseProducer
from ...config.kafka import KafkaConfig
from ...logging.decorators import log_kafka_operation


class KafkaProducer(BaseProducer):
    """Kafka producer using confluent-kafka"""

    def __init__(
        self,
        topic: Optional[str] = None,
        config: Optional[KafkaConfig] = None,
        **kafka_config,
    ):
        # Initialize config
        if config:
            self.kafka_config = config
        else:
            self.kafka_config = KafkaConfig()

        # Set default topic
        self.default_topic = topic

        # Override with provided parameters
        if kafka_config:
            for key, value in kafka_config.items():
                if hasattr(self.kafka_config, key):
                    setattr(self.kafka_config, key, value)

        super().__init__(self.kafka_config, "kafka-producer")

        self.producer: Optional[Producer] = None
        self._pending_messages = 0

    @log_kafka_operation("connect")
    async def _connect_impl(self) -> None:
        """Initialize the Kafka producer"""
        try:
            producer_config = self.kafka_config.to_producer_config()
            self.producer = Producer(producer_config)
            self._connection = self.producer

            self.logger.kafka_info(
                "Kafka producer initialized",
                config=self._sanitize_config(),
                default_topic=self.default_topic,
            )

        except Exception as e:
            self._increment_error_count()
            self.logger.kafka_error("Failed to initialize Kafka producer", error=e)
            raise

    @log_kafka_operation("disconnect")
    async def _disconnect_impl(self) -> None:
        """Close the Kafka producer"""
        if self.producer:
            try:
                producer = cast(Producer, self.producer)

                # Flush any remaining messages
                self.logger.kafka_info(f"Flushing {self._pending_messages} pending messages")
                remaining = producer.flush(timeout=10.0)

                if remaining > 0:
                    self.logger.warning(f"{remaining} messages were not delivered before shutdown")

                self.logger.kafka_info("Kafka producer closed")

            except Exception as e:
                self.logger.kafka_error("Error closing Kafka producer", error=e)
                raise

    @log_kafka_operation("send")
    async def send(
        self,
        data: Any,
        topic: Optional[str] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Send a message to Kafka"""

        if not self.producer:
            await self.connect()

        producer = cast(Producer, self.producer)
        target_topic = topic or self.default_topic

        if not target_topic:
            raise ValueError("No topic specified and no default topic configured")

        try:
            # Serialize data
            serialized_data = self._serialize_data(data)

            # Serialize key if provided
            serialized_key = None
            if key is not None:
                if isinstance(key, str):
                    serialized_key = key.encode("utf-8")
                else:
                    serialized_key = key

            # Prepare headers
            kafka_headers = None
            if headers:
                kafka_headers = [
                    (k, v.encode("utf-8") if isinstance(v, str) else v) for k, v in headers.items()
                ]

            # Delivery callback
            def delivery_callback(err, msg):
                if err:
                    self._increment_error_count()
                    self.logger.kafka_error(
                        "Message delivery failed",
                        error=str(err),
                        topic=msg.topic() if msg else target_topic,
                    )
                else:
                    self._increment_message_count()
                    self.logger.kafka_info(
                        "Message delivered",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )

                self._pending_messages -= 1

            # Send message
            self._pending_messages += 1
            producer.produce(
                topic=target_topic,
                value=serialized_data,
                key=serialized_key,
                headers=kafka_headers,
                partition=partition,
                callback=delivery_callback,
            )

            # Poll to trigger callbacks (non-blocking)
            producer.poll(0)

            self.logger.kafka_info(
                "Message queued for delivery",
                topic=target_topic,
                key=key,
                pending_messages=self._pending_messages,
            )

        except Exception as e:
            self._pending_messages -= 1
            self._increment_error_count()
            self.logger.kafka_error("Failed to send message", error=e, topic=target_topic)
            raise

    @log_kafka_operation("send_batch")
    async def send_batch(self, messages: List[Any], topic: Optional[str] = None, **kwargs) -> None:
        """Send a batch of messages"""

        if not messages:
            return

        if not self.producer:
            await self.connect()

        target_topic = topic or self.default_topic
        if not target_topic:
            raise ValueError("No topic specified and no default topic configured")

        self.logger.kafka_info(f"Sending batch of {len(messages)} messages", topic=target_topic)

        try:
            # Send all messages
            for message in messages:
                if isinstance(message, dict):
                    await self.send(
                        data=message.get("data"),
                        topic=message.get("topic", target_topic),
                        key=message.get("key"),
                        headers=message.get("headers"),
                        partition=message.get("partition"),
                    )
                else:
                    await self.send(data=message, topic=target_topic, **kwargs)

            self._increment_batch_count()
            self.logger.kafka_info(f"Batch of {len(messages)} messages queued", topic=target_topic)

        except Exception as e:
            self._increment_error_count()
            self.logger.kafka_error(
                "Failed to send batch",
                error=e,
                topic=target_topic,
                batch_size=len(messages),
            )
            raise

    async def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages and return number of remaining messages"""
        if not self.producer:
            return 0

        producer = cast(Producer, self.producer)

        try:
            remaining = producer.flush(timeout=timeout)

            if remaining == 0:
                self.logger.kafka_info("All messages flushed successfully")
            else:
                self.logger.warning(f"{remaining} messages were not delivered within timeout")

            return remaining

        except Exception as e:
            self.logger.kafka_error("Failed to flush messages", error=e)
            raise

    def _serialize_data(self, data: Any) -> bytes:
        """Serialize data for Kafka"""
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode("utf-8")
        elif isinstance(data, (dict, list)):
            return json.dumps(data, default=str).encode("utf-8")
        else:
            return str(data).encode("utf-8")

    async def _health_check_impl(self) -> bool:
        """Check Kafka producer health"""
        if not self.producer:
            return False

        try:
            producer = cast(Producer, self.producer)
            # Check if producer can be polled (indicates it's alive)
            producer.poll(0)
            return True
        except Exception as e:
            self.logger.kafka_error("Health check failed", error=e)
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Get detailed Kafka producer statistics"""
        base_stats = self.get_stats()
        base_stats["pending_messages"] = self._pending_messages

        if not self.producer:
            return base_stats

        try:
            producer = cast(Producer, self.producer)
            # Get internal queue length
            queue_length = len(producer)
            base_stats["internal_queue_length"] = queue_length

        except Exception as e:
            self.logger.kafka_error("Failed to get producer statistics", error=e)

        return base_stats

    async def send_and_wait(
        self, data: Any, topic: Optional[str] = None, timeout: float = 10.0, **kwargs
    ) -> None:
        """Send message and wait for delivery confirmation"""

        await self.send(data=data, topic=topic, **kwargs)

        # Wait for delivery
        remaining = await self.flush(timeout=timeout)

        if remaining > 0:
            raise RuntimeError(f"Message was not delivered within {timeout} seconds")

    async def get_metadata(self, topic: Optional[str] = None) -> Dict[str, Any]:
        """Get metadata for topics"""
        if not self.producer:
            await self.connect()

        producer = cast(Producer, self.producer)

        try:
            metadata = producer.list_topics(topic=topic, timeout=10)

            result = {
                "brokers": [
                    {"id": broker.id, "host": broker.host, "port": broker.port}
                    for broker in metadata.brokers.values()
                ],
                "topics": {},
            }

            for topic_name, topic_metadata in metadata.topics.items():
                result["topics"][topic_name] = {
                    "partitions": len(topic_metadata.partitions),
                    "error": str(topic_metadata.error) if topic_metadata.error else None,
                }

            return result

        except Exception as e:
            self.logger.kafka_error("Failed to get metadata", error=e)
            raise
