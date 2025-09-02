"""
Base consumer implementation with common functionality
"""

from typing import Any, Dict, Optional
from .broker import MessageBroker
from ...logging.setup import get_pythia_logger


class BaseConsumer(MessageBroker):
    """Base consumer with common functionality"""

    def __init__(self, config: Optional[Any] = None, logger_name: Optional[str] = None):
        self.config = config
        self.logger = get_pythia_logger(logger_name or self.__class__.__name__)
        self.connected = False
        self._connection = None
        self._stats = {
            "messages_consumed": 0,
            "errors": 0,
            "connection_attempts": 0,
        }

    async def connect(self) -> None:
        """Base connect implementation"""
        if self.connected:
            return

        self._stats["connection_attempts"] += 1
        await self._connect_impl()
        self.connected = True
        self.logger.info("Consumer connected successfully")

    async def disconnect(self) -> None:
        """Base disconnect implementation"""
        if not self.connected:
            return

        try:
            await self._disconnect_impl()
        finally:
            self.connected = False
            self._connection = None
            self.logger.info("Consumer disconnected")

    async def health_check(self) -> bool:
        """Base health check implementation"""
        return self.connected and await self._health_check_impl()

    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        return {
            **self._stats,
            "connected": self.connected,
            "config": self._sanitize_config(),
        }

    def _increment_message_count(self) -> None:
        """Increment message counter"""
        self._stats["messages_consumed"] += 1

    def _increment_error_count(self) -> None:
        """Increment error counter"""
        self._stats["errors"] += 1

    def _sanitize_config(self) -> Dict[str, Any]:
        """Sanitize config for logging/stats (remove sensitive data)"""
        if self.config is None:
            return {}
        elif hasattr(self.config, "model_dump"):
            config_dict = self.config.model_dump()
        else:
            config_dict = self.config.__dict__.copy() if hasattr(self.config, "__dict__") else {}

        # Remove sensitive fields
        sensitive_fields = [
            "password",
            "secret",
            "key",
            "token",
            "auth",
            "sasl_password",
            "ssl_key_password",
        ]

        for field in sensitive_fields:
            if field in config_dict:
                config_dict[field] = "***"

        return config_dict

    # Abstract methods that subclasses must implement
    async def _connect_impl(self) -> None:
        """Implementation-specific connect logic"""
        pass

    async def _disconnect_impl(self) -> None:
        """Implementation-specific disconnect logic"""
        pass

    async def _health_check_impl(self) -> bool:
        """Implementation-specific health check logic"""
        return True
