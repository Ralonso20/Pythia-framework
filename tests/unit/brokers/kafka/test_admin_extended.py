"""
Comprehensive tests for Kafka admin operations
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from confluent_kafka.admin import AdminClient, NewTopic

from pythia.brokers.kafka.admin import KafkaAdmin
from pythia.config.kafka import KafkaConfig, KafkaTopicConfig


class TestKafkaAdmin:
    """Test Kafka admin functionality"""

    @pytest.fixture
    def kafka_config(self):
        """Create test Kafka config"""
        return KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="PLAINTEXT"
        )

    @pytest.fixture
    def kafka_admin(self, kafka_config):
        """Create KafkaAdmin instance"""
        return KafkaAdmin(config=kafka_config)

    @pytest.fixture
    def topic_config(self):
        """Create test topic config"""
        return KafkaTopicConfig(
            name="test-topic",
            num_partitions=3,
            replication_factor=1
        )

    def test_admin_initialization_default_config(self):
        """Test admin initialization with default config"""
        admin = KafkaAdmin()

        assert admin.config is not None
        assert isinstance(admin.config, KafkaConfig)
        assert admin.admin_client is None
        assert admin.logger is not None

    def test_admin_initialization_custom_config(self, kafka_config):
        """Test admin initialization with custom config"""
        admin = KafkaAdmin(config=kafka_config)

        assert admin.config == kafka_config
        assert admin.admin_client is None

    @patch('pythia.brokers.kafka.admin.AdminClient')
    def test_get_admin_client_creation(self, mock_admin_client, kafka_admin):
        """Test admin client creation"""
        mock_client = Mock()
        mock_admin_client.return_value = mock_client

        client = kafka_admin._get_admin_client()

        assert client == mock_client
        assert kafka_admin.admin_client == mock_client
        mock_admin_client.assert_called_once()

    @patch('pythia.brokers.kafka.admin.AdminClient')
    def test_get_admin_client_reuse(self, mock_admin_client, kafka_admin):
        """Test admin client reuse"""
        mock_client = Mock()
        mock_admin_client.return_value = mock_client

        # First call creates client
        client1 = kafka_admin._get_admin_client()
        # Second call reuses same client
        client2 = kafka_admin._get_admin_client()

        assert client1 == client2
        assert client1 == mock_client
        mock_admin_client.assert_called_once()

    @patch('pythia.brokers.kafka.admin.AdminClient')
    def test_get_admin_client_with_security(self, mock_admin_client):
        """Test admin client creation with security settings"""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass"
        )
        admin = KafkaAdmin(config=config)

        admin._get_admin_client()

        mock_admin_client.assert_called_once()
        call_args = mock_admin_client.call_args[0][0]

        assert call_args["bootstrap.servers"] == "localhost:9092"
        assert call_args["security.protocol"] == "SASL_PLAINTEXT"
        assert call_args["sasl.mechanism"] == "PLAIN"
        assert call_args["sasl.username"] == "user"
        assert call_args["sasl.password"] == "pass"

    def test_admin_config_validation(self):
        """Test admin config validation"""
        # Test with invalid bootstrap servers
        config = KafkaConfig(bootstrap_servers="")
        admin = KafkaAdmin(config=config)

        assert admin.config.bootstrap_servers == ""

    @patch('pythia.brokers.kafka.admin.AdminClient')
    def test_admin_connection_error(self, mock_admin_client, kafka_admin):
        """Test admin client connection error handling"""
        mock_admin_client.side_effect = Exception("Connection failed")

        with pytest.raises(Exception) as exc_info:
            kafka_admin._get_admin_client()

        assert "Connection failed" in str(exc_info.value)
