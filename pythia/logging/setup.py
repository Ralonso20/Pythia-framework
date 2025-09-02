"""
Loguru logging setup and configuration
"""

import sys
from typing import Optional, Dict, Any
from pathlib import Path
from loguru import logger

from ..config.base import LogConfig, WorkerConfig


class LoguruSetup:
    """Setup and configure Loguru logging for Pythia workers"""

    @staticmethod
    def configure_logging(
        config: LogConfig,
        worker_id: Optional[str] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Configure Loguru logging with the given configuration"""

        # Remove default handler
        logger.remove()

        # Prepare extra fields for structured logging
        extra = {"worker_id": worker_id or "unknown"}
        if extra_fields:
            extra.update(extra_fields)

        # Configure console logging
        console_format = LoguruSetup._get_console_format(config)
        logger.add(
            sys.stdout,
            format=console_format,
            level=config.level.upper(),
            colorize=True,
            enqueue=True,  # Thread-safe logging
            backtrace=True,
            diagnose=True,
        )

        # Configure file logging if specified
        if config.file:
            file_format = LoguruSetup._get_file_format(config)
            log_path = Path(config.file)

            # Ensure log directory exists
            log_path.parent.mkdir(parents=True, exist_ok=True)

            logger.add(
                log_path,
                format=file_format,
                level=config.level.upper(),
                rotation=config.rotation or "1 GB",
                retention=config.retention or "30 days",
                compression="gz",
                enqueue=True,
                backtrace=True,
                diagnose=True,
            )

        # Bind extra fields to logger
        logger.configure(extra=extra)

        # Add custom filter for worker context
        logger.add(
            lambda record: LoguruSetup._add_worker_context(record, extra),
            format="{message}",
            level="TRACE",
            filter=lambda record: False,  # This handler doesn't actually output
        )

    @staticmethod
    def _get_console_format(config: LogConfig) -> str:
        """Get console log format based on configuration"""

        if config.format.lower() == "json":
            return (
                "{{"
                '"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSS}", '
                '"level": "{level}", '
                '"worker_id": "{extra[worker_id]}", '
                '"module": "{name}", '
                '"function": "{function}", '
                '"line": {line}, '
                '"message": "{message}"'
                "}}"
            )
        else:
            # Text format with colors
            return (
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{extra[worker_id]}</cyan> | "
                "<blue>{name}:{function}:{line}</blue> | "
                "<level>{message}</level>"
            )

    @staticmethod
    def _get_file_format(config: LogConfig) -> str:
        """Get file log format based on configuration"""

        if config.format.lower() == "json":
            return (
                "{{"
                '"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSSSSSZZ}", '
                '"level": "{level}", '
                '"worker_id": "{extra[worker_id]}", '
                '"process_id": {process}, '
                '"thread_id": {thread}, '
                '"module": "{name}", '
                '"function": "{function}", '
                '"line": {line}, '
                '"message": "{message}", '
                '"extra": {extra}'
                "}}"
            )
        else:
            # Structured text format
            return (
                "{time:YYYY-MM-DD HH:mm:ss.SSSSSSZZ} | "
                "{level: <8} | "
                "PID:{process} | "
                "TID:{thread} | "
                "{extra[worker_id]} | "
                "{name}:{function}:{line} | "
                "{message}"
            )

    @staticmethod
    def _add_worker_context(record: Dict[str, Any], extra: Dict[str, Any]) -> bool:
        """Add worker context to log records"""
        record["extra"].update(extra)
        return True

    @staticmethod
    def configure_from_worker_config(config: WorkerConfig) -> None:
        """Configure logging from worker configuration"""
        log_config = LogConfig(
            level=config.log_level,
            format=config.log_format,
            file=config.log_file,
        )

        LoguruSetup.configure_logging(
            log_config,
            worker_id=config.worker_id,
            extra_fields={
                "worker_name": config.worker_name,
                "broker_type": config.broker_type,
            },
        )


def configure_logging(
    level: str = "INFO",
    format: str = "text",
    file: Optional[str] = None,
    worker_id: Optional[str] = None,
    **kwargs,
) -> None:
    """Convenience function to configure logging"""

    config = LogConfig(level=level, format=format, file=file, **kwargs)

    LoguruSetup.configure_logging(config, worker_id)


def get_logger(name: Optional[str] = None) -> Any:
    """Get a logger instance with optional name binding"""

    if name:
        return logger.bind(logger_name=name)
    return logger


class PythiaLogger:
    """Pythia-specific logger wrapper with additional functionality"""

    def __init__(self, name: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        self.name = name
        self.context = context or {}
        self._logger = logger.bind(**self.context) if self.context else logger

        if name:
            self._logger = self._logger.bind(logger_name=name)

    def with_context(self, **kwargs) -> "PythiaLogger":
        """Create a new logger with additional context"""
        new_context = {**self.context, **kwargs}
        return PythiaLogger(self.name, new_context)

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message"""
        self._logger.bind(**kwargs).debug(message)

    def info(self, message: str, **kwargs) -> None:
        """Log info message"""
        self._logger.bind(**kwargs).info(message)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message"""
        self._logger.bind(**kwargs).warning(message)

    def error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log error message with optional exception"""
        if error:
            kwargs["error_type"] = type(error).__name__
            kwargs["error_message"] = str(error)

        self._logger.bind(**kwargs).error(message)

    def critical(self, message: str, **kwargs) -> None:
        """Log critical message"""
        self._logger.bind(**kwargs).critical(message)

    def kafka_info(self, message: str, **kwargs) -> None:
        """Log Kafka-specific info message"""
        self._logger.bind(component="kafka", **kwargs).info(message)

    def kafka_error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log Kafka-specific error message"""
        kwargs["component"] = "kafka"
        self.error(message, error, **kwargs)

    def webhook_info(self, message: str, **kwargs) -> None:
        """Log webhook-specific info message"""
        self._logger.bind(component="webhook", **kwargs).info(message)

    def webhook_error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log webhook-specific error message"""
        kwargs["component"] = "webhook"
        self.error(message, error, **kwargs)

    def processing_info(self, message: str, **kwargs) -> None:
        """Log message processing info"""
        self._logger.bind(component="processor", **kwargs).info(message)

    def processing_error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log message processing error"""
        kwargs["component"] = "processor"
        self.error(message, error, **kwargs)


def get_pythia_logger(name: Optional[str] = None, **context) -> PythiaLogger:
    """Get a Pythia-specific logger instance"""
    return PythiaLogger(name, context)


def setup_logging(
    level: str = "INFO",
    format_type: str = "console",
    log_file: Optional[str] = None,
    console_enabled: bool = True,
    sampling_rate: float = 1.0,
    custom_format: Optional[str] = None,
    filter_modules: Optional[list] = None,
    exclude_modules: Optional[list] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Simple setup function for Loguru logging with common options.

    Args:
        level: Log level (TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL)
        format_type: Format type (console, json, custom)
        log_file: Optional log file path
        console_enabled: Enable console output
        sampling_rate: Log sampling rate (0.0 to 1.0)
        custom_format: Custom format string (when format_type="custom")
        filter_modules: List of modules to include
        exclude_modules: List of modules to exclude
        extra_fields: Additional fields to add to all logs
    """
    # Create config based on parameters
    log_format = "json" if format_type == "json" else "text"

    config = LogConfig(level=level, format=log_format, file=log_file)

    # Use custom format if provided
    if format_type == "custom" and custom_format:
        config.format = "custom"
        config._custom_format = custom_format

    # Configure logging
    LoguruSetup.configure_logging(config, extra_fields=extra_fields)

    # Apply additional filters if specified
    if filter_modules or exclude_modules:
        logger.configure(
            handlers=[
                {
                    "sink": sys.stdout,
                    "format": LoguruSetup._get_console_format(config),
                    "level": level,
                    "filter": lambda record: _apply_module_filter(
                        record, filter_modules, exclude_modules
                    ),
                }
            ]
        )


def _apply_module_filter(
    record: Dict[str, Any], filter_modules: Optional[list], exclude_modules: Optional[list]
) -> bool:
    """Apply module filtering to log records"""
    module_name = record.get("name", "")

    if exclude_modules:
        for exclude in exclude_modules:
            if exclude in module_name:
                return False

    if filter_modules:
        for include in filter_modules:
            if include in module_name:
                return True
        return False

    return True
