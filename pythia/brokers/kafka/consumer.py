"""
Kafka consumer implementation using confluent-kafka
Adapted from existing worker example code
"""

import asyncio
import json
from typing import AsyncIterator, Dict, Any, Optional, cast, List
from confluent_kafka import Consumer, KafkaError

from ..base import BaseConsumer
from ...core.message import Message
from ...config.kafka import KafkaConfig
from ...logging.decorators import log_kafka_operation


class KafkaConsumer(BaseConsumer):
    """Kafka consumer using confluent-kafka"""

    def __init__(
        self,
        topics: Optional[List[str]] = None,
        group_id: Optional[str] = None,
        config: Optional[KafkaConfig] = None,
        **kafka_config,
    ):
        # Initialize config
        if config:
            self.kafka_config = config
        else:
            self.kafka_config = KafkaConfig()

        # Override with provided parameters
        if topics:
            self.kafka_config.topics = topics
        if group_id:
            self.kafka_config.group_id = group_id
        if kafka_config:
            # Update config with additional parameters
            for key, value in kafka_config.items():
                if hasattr(self.kafka_config, key):
                    setattr(self.kafka_config, key, value)

        super().__init__(self.kafka_config, "kafka-consumer")

        self.consumer: Optional[Consumer] = None
        self._subscribed = False

    @log_kafka_operation("connect")
    async def _connect_impl(self) -> None:
        """Initialize the Kafka consumer"""
        try:
            consumer_config = self.kafka_config.to_confluent_config()
            self.consumer = Consumer(consumer_config)
            self._connection = self.consumer

            self.logger.kafka_info(
                "Kafka consumer initialized",
                config=self._sanitize_config(),
                topics=self.kafka_config.topics,
                group_id=self.kafka_config.group_id,
            )

        except Exception as e:
            self._increment_error_count()
            self.logger.kafka_error("Failed to initialize Kafka consumer", error=e)
            raise

    @log_kafka_operation("disconnect")
    async def _disconnect_impl(self) -> None:
        """Close the Kafka consumer"""
        if self.consumer:
            try:
                consumer = cast(Consumer, self.consumer)
                consumer.close()
                self.logger.kafka_info("Kafka consumer closed")
            except Exception as e:
                self.logger.kafka_error("Error closing Kafka consumer", error=e)
                raise

    @log_kafka_operation("subscribe")
    async def subscribe(self, topics: Optional[List[str]] = None) -> None:
        """Subscribe to Kafka topics"""
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")

        topics_to_subscribe = topics or self.kafka_config.topics
        if not topics_to_subscribe:
            raise ValueError("No topics specified for subscription")

        consumer = cast(Consumer, self.consumer)

        # Setup partition assignment/revocation callbacks
        def on_assign(consumer, partitions):
            self.logger.kafka_info(
                "Partitions assigned",
                partitions=[f"{p.topic}:{p.partition}" for p in partitions],
            )

        def on_revoke(consumer, partitions):
            self.logger.kafka_info(
                "Partitions revoked",
                partitions=[f"{p.topic}:{p.partition}" for p in partitions],
            )

        consumer.subscribe(topics_to_subscribe, on_assign=on_assign, on_revoke=on_revoke)
        self._subscribed = True

        self.logger.kafka_info("Subscribed to topics", topics=topics_to_subscribe)

    @log_kafka_operation("consume", include_message_details=False)
    async def consume(self) -> AsyncIterator[Message]:
        """Consume messages from Kafka"""
        if not self.consumer:
            await self.connect()

        if not self._subscribed:
            await self.subscribe()

        consumer = cast(Consumer, self.consumer)

        self.logger.kafka_info("Starting message consumption")

        try:
            while self.connected:
                try:
                    # Poll for messages (non-blocking in async context)
                    msg = await asyncio.get_event_loop().run_in_executor(None, consumer.poll, 1.0)

                    if msg is None:
                        # No message received within timeout
                        continue

                    if msg.error():
                        if self._handle_consumer_error(msg):
                            # Critical error, break the loop
                            break
                        else:
                            # Non-critical error, continue
                            continue

                    # Convert Kafka message to Pythia Message
                    message = self._convert_kafka_message(msg)
                    self._increment_message_count()

                    self.logger.kafka_info(
                        "Message received",
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        message_id=message.message_id,
                    )

                    yield message

                except asyncio.CancelledError:
                    self.logger.kafka_info("Consumer cancelled")
                    break
                except Exception as e:
                    self._increment_error_count()
                    self.logger.kafka_error("Error in message consumption loop", error=e)
                    # Continue on non-critical errors
                    await asyncio.sleep(1)

        except Exception as e:
            self._increment_error_count()
            self.logger.kafka_error("Critical error in consume loop", error=e)
            raise
        finally:
            self.logger.kafka_info("Message consumption ended")

    def _handle_consumer_error(self, msg) -> bool:
        """Handle Kafka consumer errors. Returns True if critical error"""
        error = msg.error()

        if error.code() == KafkaError._PARTITION_EOF:
            # End of partition - not critical
            self.logger.kafka_info(
                "Reached end of partition",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
            return False
        else:
            # Real error - could be critical
            self._increment_error_count()
            self.logger.kafka_error(
                "Kafka consumer error",
                error=str(error),
                error_code=error.code(),
                topic=msg.topic() if msg.topic() else "unknown",
                partition=msg.partition() if msg.partition() else "unknown",
            )

            # Critical errors that should stop consumption
            critical_errors = [
                KafkaError._AUTHENTICATION,
                KafkaError._ALL_BROKERS_DOWN,
                KafkaError._FATAL,
            ]

            return error.code() in critical_errors

    def _convert_kafka_message(self, kafka_msg) -> Message:
        """Convert Kafka message to Pythia Message"""
        try:
            return Message.from_kafka(kafka_msg, kafka_msg.topic())

        except Exception as e:
            self.logger.kafka_error("Error converting Kafka message", error=e)
            # Return a basic message with raw data
            return Message(
                body=kafka_msg.value(),
                topic=kafka_msg.topic(),
                partition=kafka_msg.partition(),
                offset=kafka_msg.offset(),
            )

    async def _health_check_impl(self) -> bool:
        """Check Kafka consumer health"""
        if not self.consumer:
            return False

        try:
            # Try to get consumer statistics
            consumer = cast(Consumer, self.consumer)
            stats_json = consumer.stats()
            if stats_json:
                stats = json.loads(stats_json)
                # Check if we have active broker connections
                brokers = stats.get("brokers", {})
                active_brokers = [b for b in brokers.values() if b.get("state") == "UP"]
                return len(active_brokers) > 0
            return True
        except Exception as e:
            self.logger.kafka_error("Health check failed", error=e)
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Get detailed Kafka consumer statistics"""
        base_stats = self.get_stats()

        if not self.consumer:
            return base_stats

        try:
            consumer = cast(Consumer, self.consumer)
            stats_json = consumer.stats()
            if stats_json:
                kafka_stats = json.loads(stats_json)
                base_stats["kafka_stats"] = {
                    "type": kafka_stats.get("type"),
                    "ts": kafka_stats.get("ts"),
                    "msg_cnt": kafka_stats.get("msg_cnt"),
                    "msg_size": kafka_stats.get("msg_size"),
                    "brokers_count": len(kafka_stats.get("brokers", {})),
                    "topics_count": len(kafka_stats.get("topics", {})),
                }
        except Exception as e:
            self.logger.kafka_error("Failed to get Kafka statistics", error=e)

        return base_stats

    async def commit(self, message: Optional[Message] = None) -> None:
        """Commit message offset"""
        if not self.consumer:
            return

        try:
            consumer = cast(Consumer, self.consumer)
            if message and hasattr(message, "_kafka_msg"):
                # Commit specific message
                consumer.commit(message._kafka_msg)
            else:
                # Commit all consumed messages
                consumer.commit()

            self.logger.kafka_info("Offsets committed")

        except Exception as e:
            self.logger.kafka_error("Failed to commit offsets", error=e)
            raise

    async def seek(self, topic: str, partition: int, offset: int) -> None:
        """Seek to specific offset"""
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")

        try:
            from confluent_kafka import TopicPartition

            consumer = cast(Consumer, self.consumer)
            tp = TopicPartition(topic, partition, offset)
            consumer.seek(tp)

            self.logger.kafka_info(
                "Seeked to offset",
                topic=topic,
                partition=partition,
                offset=offset,
            )

        except Exception as e:
            self.logger.kafka_error(
                "Failed to seek",
                error=e,
                topic=topic,
                partition=partition,
                offset=offset,
            )
            raise
