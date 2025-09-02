"""
Custom formatters for Pythia logging
"""

import json
from typing import Dict, Any
from datetime import datetime


class JSONFormatter:
    """JSON formatter for structured logging"""

    def __init__(self, include_extra: bool = True):
        self.include_extra = include_extra

    def format(self, record: Dict[str, Any]) -> str:
        """Format log record as JSON"""

        formatted = {
            "timestamp": record.get("time", datetime.utcnow()).isoformat(),
            "level": record.get("level", {}).get("name", "INFO"),
            "message": record.get("message", ""),
            "logger": record.get("name", ""),
            "function": record.get("function", ""),
            "line": record.get("line", 0),
            "process": record.get("process", {}).get("id", 0),
            "thread": record.get("thread", {}).get("id", 0),
        }

        # Add extra fields
        if self.include_extra and "extra" in record:
            extra = record["extra"]
            formatted.update(
                {
                    "worker_id": extra.get("worker_id"),
                    "component": extra.get("component"),
                    "operation": extra.get("operation"),
                    "correlation_id": extra.get("correlation_id"),
                }
            )

            # Add any additional extra fields
            for key, value in extra.items():
                if key not in formatted and not key.startswith("_"):
                    formatted[key] = value

        # Add exception info if present
        if "exception" in record and record["exception"]:
            exc = record["exception"]
            formatted["exception"] = {
                "type": exc.get("type", ""),
                "value": exc.get("value", ""),
                "traceback": exc.get("traceback", ""),
            }

        return json.dumps(formatted, default=str, ensure_ascii=False)


class StructuredFormatter:
    """Structured text formatter for human-readable logs"""

    def __init__(
        self,
        include_timestamp: bool = True,
        include_level: bool = True,
        include_worker_id: bool = True,
        include_component: bool = True,
        timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f",
    ):
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_worker_id = include_worker_id
        self.include_component = include_component
        self.timestamp_format = timestamp_format

    def format(self, record: Dict[str, Any]) -> str:
        """Format log record as structured text"""

        parts = []

        # Timestamp
        if self.include_timestamp:
            timestamp = record.get("time", datetime.utcnow())
            if hasattr(timestamp, "strftime"):
                parts.append(timestamp.strftime(self.timestamp_format)[:-3])  # Remove microseconds
            else:
                parts.append(str(timestamp))

        # Level
        if self.include_level:
            level = record.get("level", {}).get("name", "INFO")
            parts.append(f"[{level:8}]")

        # Worker ID
        if self.include_worker_id and "extra" in record:
            worker_id = record["extra"].get("worker_id", "unknown")
            parts.append(f"({worker_id})")

        # Component
        if self.include_component and "extra" in record:
            component = record["extra"].get("component")
            if component:
                parts.append(f"<{component}>")

        # Location info
        name = record.get("name", "")
        function = record.get("function", "")
        line = record.get("line", 0)
        if name or function:
            location = f"{name}:{function}:{line}" if name else f"{function}:{line}"
            parts.append(f"[{location}]")

        # Message
        message = record.get("message", "")
        parts.append("-")
        parts.append(message)

        # Additional context from extra
        if "extra" in record:
            extra = record["extra"]
            context_parts = []

            # Add common context fields
            for key in ["operation", "correlation_id", "request_id"]:
                value = extra.get(key)
                if value:
                    context_parts.append(f"{key}={value}")

            # Add Kafka-specific context
            if extra.get("component") == "kafka":
                for key in ["topic", "partition", "offset"]:
                    value = extra.get(key)
                    if value is not None:
                        context_parts.append(f"{key}={value}")

            # Add webhook-specific context
            if extra.get("component") == "webhook":
                for key in ["url", "status_code", "method"]:
                    value = extra.get(key)
                    if value is not None:
                        context_parts.append(f"{key}={value}")

            # Add performance metrics
            if "execution_time_seconds" in extra:
                exec_time = extra["execution_time_seconds"]
                context_parts.append(f"duration={exec_time}s")

            # Add error context
            if "error" in extra:
                error = extra["error"]
                context_parts.append(f"error={error}")

            if context_parts:
                parts.append(f"({', '.join(context_parts)})")

        return " ".join(parts)


class CompactFormatter:
    """Compact formatter for minimal log output"""

    def format(self, record: Dict[str, Any]) -> str:
        """Format log record compactly"""

        # Get basic info
        level = record.get("level", {}).get("name", "INFO")
        message = record.get("message", "")

        # Get worker context
        worker_id = "unknown"
        component = ""
        if "extra" in record:
            extra = record["extra"]
            worker_id = extra.get("worker_id", worker_id)
            component = extra.get("component", "")

        # Format based on component
        if component:
            return f"[{level[0]}] {worker_id}:{component} - {message}"
        else:
            return f"[{level[0]}] {worker_id} - {message}"


class ColoredConsoleFormatter:
    """Colored console formatter for development"""

    COLORS = {
        "TRACE": "\033[90m",  # Dark gray
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "SUCCESS": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[41m",  # Red background
        "RESET": "\033[0m",  # Reset
    }

    def format(self, record: Dict[str, Any]) -> str:
        """Format log record with colors for console"""

        level = record.get("level", {}).get("name", "INFO")
        color = self.COLORS.get(level, self.COLORS["RESET"])
        reset = self.COLORS["RESET"]

        # Get timestamp
        timestamp = record.get("time", datetime.utcnow())
        if hasattr(timestamp, "strftime"):
            time_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
        else:
            time_str = str(timestamp)

        # Get worker context
        worker_info = ""
        if "extra" in record:
            extra = record["extra"]
            worker_id = extra.get("worker_id", "unknown")
            component = extra.get("component", "")
            if component:
                worker_info = f" {worker_id}:{component}"
            else:
                worker_info = f" {worker_id}"

        # Get location
        name = record.get("name", "")
        function = record.get("function", "")
        line = record.get("line", 0)
        location = f"{name}:{function}:{line}" if name else f"{function}:{line}"

        # Message
        message = record.get("message", "")

        return f"{time_str} {color}[{level:8}]{reset}{worker_info} {location} - {message}"
