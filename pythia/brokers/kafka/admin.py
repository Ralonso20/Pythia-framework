"""
Kafka admin operations for topic management
"""

from typing import Dict, List, Optional, Any
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource

from ...config.kafka import KafkaConfig, KafkaTopicConfig
from ...logging.setup import get_pythia_logger


class KafkaAdmin:
    """Kafka admin client for topic and configuration management"""

    def __init__(self, config: Optional[KafkaConfig] = None):
        self.config = config or KafkaConfig()
        self.logger = get_pythia_logger("kafka-admin")
        self.admin_client: Optional[AdminClient] = None

    def _get_admin_client(self) -> AdminClient:
        """Get or create admin client"""
        if not self.admin_client:
            admin_config = {
                "bootstrap.servers": self.config.bootstrap_servers,
            }

            # Add security settings if configured
            if self.config.security_protocol != "PLAINTEXT":
                admin_config["security.protocol"] = self.config.security_protocol

            if self.config.sasl_mechanism:
                admin_config["sasl.mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_username:
                admin_config["sasl.username"] = self.config.sasl_username

            if self.config.sasl_password:
                admin_config["sasl.password"] = self.config.sasl_password

            self.admin_client = AdminClient(admin_config)
            self.logger.info("Kafka admin client initialized")

        return self.admin_client

    async def create_topics(
        self, topic_configs: List[KafkaTopicConfig], timeout: float = 30.0
    ) -> Dict[str, Any]:
        """Create Kafka topics"""
        admin = self._get_admin_client()

        new_topics = []
        for topic_config in topic_configs:
            new_topic = NewTopic(
                topic=topic_config.name,
                num_partitions=topic_config.num_partitions,
                replication_factor=topic_config.replication_factor,
                config=topic_config.to_topic_config(),
            )
            new_topics.append(new_topic)

        self.logger.info(f"Creating {len(new_topics)} topics")

        try:
            # Create topics
            futures = admin.create_topics(new_topics, request_timeout=timeout)

            # Wait for results
            results = {}
            for topic_name, future in futures.items():
                try:
                    future.result(timeout=timeout)
                    results[topic_name] = {"status": "created"}
                    self.logger.info(f"Topic created successfully: {topic_name}")
                except Exception as e:
                    results[topic_name] = {"status": "failed", "error": str(e)}
                    self.logger.error(f"Failed to create topic {topic_name}: {e}")

            return results

        except Exception as e:
            self.logger.error(f"Failed to create topics: {e}")
            raise

    async def delete_topics(self, topic_names: List[str], timeout: float = 30.0) -> Dict[str, Any]:
        """Delete Kafka topics"""
        admin = self._get_admin_client()

        self.logger.info(f"Deleting {len(topic_names)} topics")

        try:
            # Delete topics
            futures = admin.delete_topics(topic_names, request_timeout=timeout)

            # Wait for results
            results = {}
            for topic_name, future in futures.items():
                try:
                    future.result(timeout=timeout)
                    results[topic_name] = {"status": "deleted"}
                    self.logger.info(f"Topic deleted successfully: {topic_name}")
                except Exception as e:
                    results[topic_name] = {"status": "failed", "error": str(e)}
                    self.logger.error(f"Failed to delete topic {topic_name}: {e}")

            return results

        except Exception as e:
            self.logger.error(f"Failed to delete topics: {e}")
            raise

    async def list_topics(self, timeout: float = 10.0) -> Dict[str, Any]:
        """List all Kafka topics"""
        admin = self._get_admin_client()

        try:
            metadata = admin.list_topics(timeout=timeout)

            topics = {}
            for topic_name, topic_metadata in metadata.topics.items():
                topics[topic_name] = {
                    "partitions": len(topic_metadata.partitions),
                    "error": str(topic_metadata.error) if topic_metadata.error else None,
                }

            self.logger.info(f"Listed {len(topics)} topics")
            return {
                "topics": topics,
                "brokers": {
                    broker.id: f"{broker.host}:{broker.port}"
                    for broker in metadata.brokers.values()
                },
            }

        except Exception as e:
            self.logger.error(f"Failed to list topics: {e}")
            raise

    async def describe_topics(
        self, topic_names: List[str], timeout: float = 10.0
    ) -> Dict[str, Any]:
        """Describe specific topics"""
        admin = self._get_admin_client()

        try:
            metadata = admin.list_topics(timeout=timeout)

            result = {}
            for topic_name in topic_names:
                if topic_name in metadata.topics:
                    topic_metadata = metadata.topics[topic_name]

                    partitions = []
                    for partition in topic_metadata.partitions.values():
                        partitions.append(
                            {
                                "id": partition.id,
                                "leader": partition.leader,
                                "replicas": partition.replicas,
                                "isrs": partition.isrs,
                                "error": str(partition.error) if partition.error else None,
                            }
                        )

                    result[topic_name] = {
                        "partitions": partitions,
                        "error": str(topic_metadata.error) if topic_metadata.error else None,
                    }
                else:
                    result[topic_name] = {"error": "Topic not found"}

            self.logger.info(f"Described {len(result)} topics")
            return result

        except Exception as e:
            self.logger.error(f"Failed to describe topics: {e}")
            raise

    async def alter_configs(
        self, topic_configs: Dict[str, Dict[str, str]], timeout: float = 30.0
    ) -> Dict[str, Any]:
        """Alter topic configurations"""
        admin = self._get_admin_client()

        resources = []
        for topic_name, config_updates in topic_configs.items():
            resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name)
            resources.append((resource, config_updates))

        self.logger.info(f"Altering configuration for {len(resources)} topics")

        try:
            # Alter configurations
            futures = admin.alter_configs(dict(resources), request_timeout=timeout)

            # Wait for results
            results = {}
            for resource, future in futures.items():
                try:
                    future.result(timeout=timeout)
                    results[resource.name] = {"status": "updated"}
                    self.logger.info(f"Configuration updated for topic: {resource.name}")
                except Exception as e:
                    results[resource.name] = {"status": "failed", "error": str(e)}
                    self.logger.error(
                        f"Failed to update configuration for topic {resource.name}: {e}"
                    )

            return results

        except Exception as e:
            self.logger.error(f"Failed to alter configurations: {e}")
            raise

    async def describe_configs(
        self, topic_names: List[str], timeout: float = 10.0
    ) -> Dict[str, Any]:
        """Describe topic configurations"""
        admin = self._get_admin_client()

        resources = [ConfigResource(ConfigResource.Type.TOPIC, name) for name in topic_names]

        try:
            # Describe configurations
            futures = admin.describe_configs(resources, request_timeout=timeout)

            # Wait for results
            results = {}
            for resource, future in futures.items():
                try:
                    config = future.result(timeout=timeout)
                    results[resource.name] = {
                        "configs": {
                            name: {
                                "value": entry.value,
                                "source": str(entry.source),
                                "is_default": entry.is_default,
                                "is_sensitive": entry.is_sensitive,
                            }
                            for name, entry in config.items()
                        }
                    }
                except Exception as e:
                    results[resource.name] = {"error": str(e)}
                    self.logger.error(
                        f"Failed to describe configuration for topic {resource.name}: {e}"
                    )

            self.logger.info(f"Described configuration for {len(results)} topics")
            return results

        except Exception as e:
            self.logger.error(f"Failed to describe configurations: {e}")
            raise

    def close(self):
        """Close the admin client"""
        if self.admin_client:
            # AdminClient doesn't have explicit close method
            self.admin_client = None
            self.logger.info("Kafka admin client closed")
