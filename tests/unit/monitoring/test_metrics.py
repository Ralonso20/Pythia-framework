"""
Tests for monitoring metrics
"""

import pytest
from unittest.mock import MagicMock, patch

from pythia.monitoring.metrics import PythiaMetrics, MetricsConfig, MetricsMode


class TestPythiaMetrics:
    """Test PythiaMetrics base functionality"""

    @pytest.fixture
    def config(self):
        """Create metrics config"""
        return MetricsConfig(enabled=True, worker_name="test_worker")

    @pytest.fixture
    def metrics(self, config):
        """Create metrics instance"""
        return PythiaMetrics(config)

    def test_metrics_initialization(self, metrics):
        """Test metrics initialization"""
        assert metrics.config.enabled is True
        assert metrics.config.worker_name == "test_worker"

    def test_metrics_disabled(self):
        """Test metrics when disabled"""
        config = MetricsConfig(enabled=False)
        metrics = PythiaMetrics(config)
        
        assert not metrics.config.enabled
        assert metrics.registry is None

    def test_metrics_config_defaults(self):
        """Test metrics config defaults"""
        config = MetricsConfig()
        assert config.enabled is True
        assert config.mode == MetricsMode.HTTP_SERVER
        assert config.http_port == 9090

    def test_base_labels(self, metrics):
        """Test base labels generation"""
        labels = metrics.get_base_labels(custom="value")
        
        assert labels["worker"] == "test_worker"
        assert labels["instance"] == "test_worker" 
        assert labels["custom"] == "value"

    def test_record_message_processed(self, metrics):
        """Test recording message processed"""
        # Should not raise error even if metrics disabled
        metrics.record_message_processed(
            source="test", 
            status="success",
            processing_time=1.5,
            message_size=1024
        )

    def test_record_broker_activity(self, metrics):
        """Test recording broker activity"""
        metrics.record_broker_activity(
            broker_type="kafka",
            broker_host="localhost:9092", 
            direction="in",
            count=5
        )

    def test_record_broker_error(self, metrics):
        """Test recording broker error"""
        metrics.record_broker_error(
            broker_type="redis",
            broker_host="localhost:6379",
            error_type="connection_error"
        )

    def test_record_processing_error(self, metrics):
        """Test recording processing error"""
        metrics.record_processing_error(
            source="test_source",
            error_type="ValueError"
        )

    def test_record_routing_decision(self, metrics):
        """Test recording routing decision"""
        metrics.record_routing_decision(
            source_name="kafka_source",
            sink_name="redis_sink", 
            routing_rule="default"
        )

    def test_set_source_status(self, metrics):
        """Test setting source status"""
        metrics.set_source_status("test_source", True)
        metrics.set_source_status("test_source", False)

    def test_update_system_metrics(self, metrics):
        """Test updating system metrics"""
        # Should not raise error
        metrics.update_system_metrics()

    def test_get_metrics_text_disabled(self):
        """Test getting metrics text when disabled"""
        config = MetricsConfig(enabled=False)
        metrics = PythiaMetrics(config)
        
        result = metrics.get_metrics_text()
        assert result == b""

    def test_custom_counter_creation(self, metrics):
        """Test custom counter creation"""
        counter = metrics.create_custom_counter(
            "test_counter",
            "Test counter description",
            ["label1", "label2"]
        )
        
        assert counter is not None

    def test_custom_gauge_creation(self, metrics):
        """Test custom gauge creation"""  
        gauge = metrics.create_custom_gauge(
            "test_gauge",
            "Test gauge description",
            ["label1"]
        )
        
        assert gauge is not None

    def test_custom_histogram_creation(self, metrics):
        """Test custom histogram creation"""
        histogram = metrics.create_custom_histogram(
            "test_histogram", 
            "Test histogram description",
            ["label1"],
            buckets=(0.1, 0.5, 1.0)
        )
        
        assert histogram is not None

    def test_context_manager(self, config):
        """Test context manager functionality"""
        with PythiaMetrics(config) as metrics:
            assert metrics.config.enabled is True

    @patch('pythia.monitoring.metrics.start_http_server')
    def test_start_http_server_mode(self, mock_start_server):
        """Test starting HTTP server mode"""
        config = MetricsConfig(
            enabled=True,
            mode=MetricsMode.HTTP_SERVER,
            http_port=8080
        )
        metrics = PythiaMetrics(config)
        metrics.start()
        
        mock_start_server.assert_called_once()

    def test_push_gateway_mode_without_url(self):
        """Test push gateway mode without URL raises error"""
        config = MetricsConfig(
            enabled=True,
            mode=MetricsMode.PUSH_GATEWAY,
            gateway_url=None
        )
        metrics = PythiaMetrics(config)
        
        # The error should be handled gracefully during start
        metrics.start()  # Should not raise, but log error


class TestMetricsConfig:
    """Test MetricsConfig functionality"""

    def test_config_defaults(self):
        """Test configuration defaults"""
        config = MetricsConfig()
        
        assert config.enabled is True
        assert config.mode == MetricsMode.HTTP_SERVER
        assert config.http_port == 9090
        assert config.http_host == "0.0.0.0"
        assert config.job_name == "pythia_worker"
        assert config.worker_name == "pythia_worker"

    def test_config_custom_values(self):
        """Test configuration with custom values"""
        config = MetricsConfig(
            enabled=False,
            mode=MetricsMode.PUSH_GATEWAY,
            http_port=8080,
            gateway_url="http://prometheus:9091",
            worker_name="custom_worker"
        )
        
        assert config.enabled is False
        assert config.mode == MetricsMode.PUSH_GATEWAY
        assert config.http_port == 8080
        assert config.gateway_url == "http://prometheus:9091"
        assert config.worker_name == "custom_worker"


class TestMetricsMode:
    """Test MetricsMode enum"""

    def test_metrics_modes(self):
        """Test all metrics modes exist"""
        assert MetricsMode.DISABLED.value == "disabled"
        assert MetricsMode.HTTP_SERVER.value == "http_server"
        assert MetricsMode.PUSH_GATEWAY.value == "push_gateway"
        assert MetricsMode.CUSTOM.value == "custom"