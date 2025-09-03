"""
Tests for logging setup
"""

import pytest
from unittest.mock import patch, MagicMock

from pythia.logging.setup import (
    get_pythia_logger, setup_logging, configure_logging, 
    get_logger, PythiaLogger, LoguruSetup
)
from pythia.config.base import LogConfig


class TestLoggingSetup:
    """Test logging setup functionality"""

    def test_get_pythia_logger_basic(self):
        """Test getting basic Pythia logger"""
        logger = get_pythia_logger("test_module")
        
        assert logger.name.endswith("test_module")
        # Should be Loguru logger
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'debug')

    def test_get_pythia_logger_with_extra_context(self):
        """Test getting logger with extra context"""
        logger = get_pythia_logger("test_module", worker_id="worker-123")
        
        # Logger should have context bound
        assert logger.name.endswith("test_module")

    @patch('pythia.logging.setup.logger')
    def test_setup_logging_basic(self, mock_logger):
        """Test basic logging setup"""
        setup_logging(level="INFO", format_type="console")
        
        # Should configure logger
        mock_logger.remove.assert_called()

    @patch('pythia.logging.setup.logger') 
    def test_setup_logging_json_format(self, mock_logger):
        """Test logging setup with JSON format"""
        setup_logging(level="DEBUG", format_type="json")
        
        # Should configure JSON logging
        mock_logger.remove.assert_called()

    @patch('pythia.logging.setup.logger')
    def test_setup_logging_file_output(self, mock_logger):
        """Test logging setup with file output"""
        setup_logging(
            level="INFO",
            log_file="/tmp/test.log"
        )
        
        # Should configure file output
        mock_logger.remove.assert_called()

    def test_configure_logging_function(self):
        """Test configure_logging convenience function"""
        # Should not raise error
        configure_logging(level="INFO", format="text", worker_id="test-worker")

    def test_get_logger_basic(self):
        """Test getting basic logger"""
        logger = get_logger()
        assert logger is not None

    def test_get_logger_with_name(self):
        """Test getting logger with name"""
        logger = get_logger("test_module")
        assert logger is not None

    def test_pythia_logger_initialization(self):
        """Test PythiaLogger initialization"""
        logger = PythiaLogger("test_logger", {"key": "value"})
        
        assert logger.name == "test_logger"
        assert logger.context["key"] == "value"

    def test_pythia_logger_with_context(self):
        """Test PythiaLogger with additional context"""
        logger = PythiaLogger("test")
        new_logger = logger.with_context(request_id="123")
        
        assert "request_id" in new_logger.context
        assert new_logger.context["request_id"] == "123"

    def test_pythia_logger_methods(self):
        """Test PythiaLogger logging methods"""
        logger = PythiaLogger("test")
        
        # Should not raise errors
        logger.debug("Debug message", extra="data")
        logger.info("Info message", extra="data") 
        logger.warning("Warning message", extra="data")
        logger.error("Error message", error=ValueError("test"), extra="data")
        logger.critical("Critical message", extra="data")

    def test_pythia_logger_kafka_methods(self):
        """Test PythiaLogger Kafka-specific methods"""
        logger = PythiaLogger("kafka_test")
        
        # Should not raise errors
        logger.kafka_info("Kafka connected", broker="localhost:9092")
        logger.kafka_error("Kafka error", error=ConnectionError("Failed"))

    def test_pythia_logger_webhook_methods(self):
        """Test PythiaLogger webhook-specific methods"""
        logger = PythiaLogger("webhook_test")
        
        # Should not raise errors  
        logger.webhook_info("Webhook sent", url="http://example.com")
        logger.webhook_error("Webhook failed", error=TimeoutError("Timeout"))

    def test_pythia_logger_processing_methods(self):
        """Test PythiaLogger processing-specific methods"""
        logger = PythiaLogger("processor_test")
        
        # Should not raise errors
        logger.processing_info("Processing message", message_id="123")
        logger.processing_error("Processing failed", error=ValueError("Invalid"))

    def test_get_pythia_logger_function(self):
        """Test get_pythia_logger convenience function"""
        logger = get_pythia_logger("test_module", worker_id="worker-123")
        
        assert logger.name == "test_module"
        assert "worker_id" in logger.context

    @patch('pythia.logging.setup.logger')
    def test_loguru_setup_configure_logging(self, mock_logger):
        """Test LoguruSetup.configure_logging"""
        config = LogConfig(level="INFO", format="text")
        
        LoguruSetup.configure_logging(config, worker_id="test-worker")
        
        # Should remove and add handlers
        mock_logger.remove.assert_called()
        assert mock_logger.add.call_count >= 1

    @patch('pythia.logging.setup.logger')
    def test_loguru_setup_with_file(self, mock_logger):
        """Test LoguruSetup with file logging"""
        config = LogConfig(
            level="INFO", 
            format="json",
            file="/tmp/test.log",
            rotation="1 MB",
            retention="7 days"
        )
        
        LoguruSetup.configure_logging(config)
        
        # Should add multiple handlers (console + file)
        assert mock_logger.add.call_count >= 2

    def test_console_format_generation(self):
        """Test console format generation"""
        config = LogConfig(format="json")
        format_str = LoguruSetup._get_console_format(config)
        
        assert "timestamp" in format_str
        assert "level" in format_str

    def test_file_format_generation(self):
        """Test file format generation"""
        config = LogConfig(format="text")
        format_str = LoguruSetup._get_file_format(config)
        
        assert "YYYY-MM-DD" in format_str
        assert "level" in format_str

    def test_setup_with_custom_format(self):
        """Test setup with custom format"""
        setup_logging(
            level="INFO",
            format_type="custom",
            custom_format="{time} | {level} | {message}"
        )

    def test_setup_with_filters(self):
        """Test setup with module filters"""
        setup_logging(
            level="INFO",
            filter_modules=["pythia"],
            exclude_modules=["test"]
        )

    def test_module_filter_application(self):
        """Test module filter logic"""
        from pythia.logging.setup import _apply_module_filter
        
        record = {"name": "pythia.core.worker"}
        
        # Should include when in filter list
        result = _apply_module_filter(record, ["pythia"], None)
        assert result is True
        
        # Should exclude when in exclude list  
        result = _apply_module_filter(record, None, ["pythia"])
        assert result is False
        
        # Should pass through when no filters
        result = _apply_module_filter(record, None, None)
        assert result is True