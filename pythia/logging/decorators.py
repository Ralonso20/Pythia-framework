"""
Logging decorators for Pythia workers
"""

import time
import asyncio
from functools import wraps
from typing import Callable, Optional
from loguru import logger


def log_execution(
    message: Optional[str] = None,
    level: str = "DEBUG",
    include_args: bool = False,
    include_result: bool = False,
    logger_name: Optional[str] = None,
):
    """Decorator to log function/method execution"""

    def decorator(func: Callable) -> Callable:
        func_name = func.__name__
        log_message = message or f"Executing {func_name}"

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                extra = {"function": func_name}
                if logger_name:
                    extra["logger_name"] = logger_name

                if include_args and (args or kwargs):
                    extra["args"] = args
                    extra["kwargs"] = kwargs

                logger.bind(**extra).log(level, f"{log_message} - started")

                try:
                    result = await func(*args, **kwargs)

                    if include_result:
                        extra["result"] = result

                    logger.bind(**extra).log(level, f"{log_message} - completed")
                    return result

                except Exception as e:
                    extra["error"] = str(e)
                    logger.bind(**extra).error(f"{log_message} - failed")
                    raise

            return async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                extra = {"function": func_name}
                if logger_name:
                    extra["logger_name"] = logger_name

                if include_args and (args or kwargs):
                    extra["args"] = args
                    extra["kwargs"] = kwargs

                logger.bind(**extra).log(level, f"{log_message} - started")

                try:
                    result = func(*args, **kwargs)

                    if include_result:
                        extra["result"] = result

                    logger.bind(**extra).log(level, f"{log_message} - completed")
                    return result

                except Exception as e:
                    extra["error"] = str(e)
                    logger.bind(**extra).error(f"{log_message} - failed")
                    raise

            return sync_wrapper

    return decorator


def log_errors(
    message: Optional[str] = None,
    level: str = "ERROR",
    reraise: bool = True,
    logger_name: Optional[str] = None,
):
    """Decorator to log function/method errors"""

    def decorator(func: Callable) -> Callable:
        func_name = func.__name__
        error_message = message or f"Error in {func_name}"

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    extra = {
                        "function": func_name,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    }
                    if logger_name:
                        extra["logger_name"] = logger_name

                    logger.bind(**extra).log(level, error_message)

                    if reraise:
                        raise
                    return None

            return async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    extra = {
                        "function": func_name,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    }
                    if logger_name:
                        extra["logger_name"] = logger_name

                    logger.bind(**extra).log(level, error_message)

                    if reraise:
                        raise
                    return None

            return sync_wrapper

    return decorator


def log_performance(
    message: Optional[str] = None,
    level: str = "INFO",
    threshold: Optional[float] = None,
    logger_name: Optional[str] = None,
):
    """Decorator to log function/method performance"""

    def decorator(func: Callable) -> Callable:
        func_name = func.__name__
        perf_message = message or f"Performance metrics for {func_name}"

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.perf_counter()

                try:
                    result = await func(*args, **kwargs)
                    execution_time = time.perf_counter() - start_time

                    # Only log if threshold is not set or execution time exceeds threshold
                    if threshold is None or execution_time >= threshold:
                        extra = {
                            "function": func_name,
                            "execution_time_seconds": round(execution_time, 4),
                            "execution_time_ms": round(execution_time * 1000, 2),
                        }
                        if logger_name:
                            extra["logger_name"] = logger_name

                        logger.bind(**extra).log(level, perf_message)

                    return result

                except Exception as e:
                    execution_time = time.perf_counter() - start_time
                    extra = {
                        "function": func_name,
                        "execution_time_seconds": round(execution_time, 4),
                        "error": str(e),
                    }
                    if logger_name:
                        extra["logger_name"] = logger_name

                    logger.bind(**extra).error(
                        f"{perf_message} - failed after {execution_time:.4f}s"
                    )
                    raise

            return async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.perf_counter()

                try:
                    result = func(*args, **kwargs)
                    execution_time = time.perf_counter() - start_time

                    # Only log if threshold is not set or execution time exceeds threshold
                    if threshold is None or execution_time >= threshold:
                        extra = {
                            "function": func_name,
                            "execution_time_seconds": round(execution_time, 4),
                            "execution_time_ms": round(execution_time * 1000, 2),
                        }
                        if logger_name:
                            extra["logger_name"] = logger_name

                        logger.bind(**extra).log(level, perf_message)

                    return result

                except Exception as e:
                    execution_time = time.perf_counter() - start_time
                    extra = {
                        "function": func_name,
                        "execution_time_seconds": round(execution_time, 4),
                        "error": str(e),
                    }
                    if logger_name:
                        extra["logger_name"] = logger_name

                    logger.bind(**extra).error(
                        f"{perf_message} - failed after {execution_time:.4f}s"
                    )
                    raise

            return sync_wrapper

    return decorator


def log_kafka_operation(operation: str, level: str = "DEBUG", include_message_details: bool = True):
    """Decorator specifically for Kafka operations"""

    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                extra = {
                    "component": "kafka",
                    "operation": operation,
                    "function": func.__name__,
                }

                if include_message_details and args:
                    # Try to extract message details from args
                    if hasattr(args[1] if len(args) > 1 else None, "topic"):
                        msg = args[1]
                        extra.update(
                            {
                                "topic": getattr(msg, "topic", None),
                                "partition": getattr(msg, "partition", None),
                                "offset": getattr(msg, "offset", None),
                            }
                        )

                logger.bind(**extra).log(level, f"Kafka {operation} - started")

                try:
                    result = await func(*args, **kwargs)
                    logger.bind(**extra).log(level, f"Kafka {operation} - completed")
                    return result
                except Exception as e:
                    extra["error"] = str(e)
                    logger.bind(**extra).error(f"Kafka {operation} - failed")
                    raise

            return async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                extra = {
                    "component": "kafka",
                    "operation": operation,
                    "function": func.__name__,
                }

                if include_message_details and args:
                    # Try to extract message details from args
                    if hasattr(args[1] if len(args) > 1 else None, "topic"):
                        msg = args[1]
                        extra.update(
                            {
                                "topic": getattr(msg, "topic", None),
                                "partition": getattr(msg, "partition", None),
                                "offset": getattr(msg, "offset", None),
                            }
                        )

                logger.bind(**extra).log(level, f"Kafka {operation} - started")

                try:
                    result = func(*args, **kwargs)
                    logger.bind(**extra).log(level, f"Kafka {operation} - completed")
                    return result
                except Exception as e:
                    extra["error"] = str(e)
                    logger.bind(**extra).error(f"Kafka {operation} - failed")
                    raise

            return sync_wrapper

    return decorator


def log_webhook_operation(operation: str, level: str = "DEBUG", include_url: bool = True):
    """Decorator specifically for webhook operations"""

    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                extra = {
                    "component": "webhook",
                    "operation": operation,
                    "function": func.__name__,
                }

                if include_url:
                    # Try to extract URL from args/kwargs
                    url = kwargs.get("url") or (args[1] if len(args) > 1 else None)
                    if url:
                        extra["url"] = url

                logger.bind(**extra).log(level, f"Webhook {operation} - started")

                try:
                    result = await func(*args, **kwargs)

                    # Include response status if available
                    if hasattr(result, "status_code"):
                        extra["status_code"] = result.status_code

                    logger.bind(**extra).log(level, f"Webhook {operation} - completed")
                    return result
                except Exception as e:
                    extra["error"] = str(e)
                    logger.bind(**extra).error(f"Webhook {operation} - failed")
                    raise

            return async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                extra = {
                    "component": "webhook",
                    "operation": operation,
                    "function": func.__name__,
                }

                if include_url:
                    # Try to extract URL from args/kwargs
                    url = kwargs.get("url") or (args[1] if len(args) > 1 else None)
                    if url:
                        extra["url"] = url

                logger.bind(**extra).log(level, f"Webhook {operation} - started")

                try:
                    result = func(*args, **kwargs)

                    # Include response status if available
                    if hasattr(result, "status_code"):
                        extra["status_code"] = result.status_code

                    logger.bind(**extra).log(level, f"Webhook {operation} - completed")
                    return result
                except Exception as e:
                    extra["error"] = str(e)
                    logger.bind(**extra).error(f"Webhook {operation} - failed")
                    raise

            return sync_wrapper

    return decorator
