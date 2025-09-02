"""
Base Worker class for Pythia framework
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Awaitable

from .message import Message
from .lifecycle import LifecycleManager, WorkerRunner
from ..config.base import WorkerConfig
from ..config.auto_config import auto_detect_config, create_broker_config
from ..logging.setup import LoguruSetup, get_pythia_logger
from ..brokers.base import MessageBroker, MessageProducer
from ..monitoring import PythiaMetrics, MetricsConfig


class Worker(ABC):
    """
    Base Worker class for Pythia framework

    This is the main class that users inherit from to create workers.
    It provides the framework for message processing, lifecycle management,
    logging, and broker integration.
    """

    # Class-level configuration
    source: Optional[MessageBroker] = None
    sink: Optional[MessageProducer] = None
    sources: Optional[List[MessageBroker]] = None
    sinks: Optional[List[MessageProducer]] = None

    def __init__(
        self,
        config: Optional[WorkerConfig] = None,
        metrics_config: Optional[MetricsConfig] = None,
    ):
        # Configuration
        self.config = config or WorkerConfig()

        # Setup logging
        self.logger = get_pythia_logger(
            name=self.__class__.__name__,
            worker_id=self.config.worker_id,
            worker_name=self.config.worker_name,
        )
        LoguruSetup.configure_from_worker_config(self.config)

        # Initialize metrics
        self.metrics_config = metrics_config or MetricsConfig(
            worker_name=self.config.worker_name,
            custom_labels={"worker_class": self.__class__.__name__},
        )
        self.metrics = PythiaMetrics(self.metrics_config)

        # Lifecycle management
        self.lifecycle = LifecycleManager(self.config)
        self._setup_lifecycle_hooks()

        # Processing state
        self.retry_count = 0
        self.processed_messages = 0
        self.failed_messages = 0
        self._running = False

        # Initialize brokers
        self._initialize_brokers()

        # Continuous running flag
        self._continuous = True

    def _initialize_brokers(self) -> None:
        """Initialize message brokers from class attributes or auto-detection"""

        # Initialize sources
        if self.sources:
            self._sources = self.sources
        elif self.source:
            self._sources = [self.source]
        else:
            # Try to auto-detect and create broker
            try:
                create_broker_config(self.config.broker_type)
                # Import and create appropriate broker - this will be implemented per broker
                self._sources = []  # Will be populated by specific broker implementations
            except Exception as e:
                self.logger.warning(f"No sources configured and auto-detection failed: {e}")
                self._sources = []

        # Initialize sinks
        if self.sinks:
            self._sinks = self.sinks
        elif self.sink:
            self._sinks = [self.sink]
        else:
            self._sinks = []

    def _setup_lifecycle_hooks(self) -> None:
        """Setup lifecycle hooks"""

        async def startup_hook():
            await self._startup()

        async def shutdown_hook():
            await self._shutdown()

        async def health_check_hook():
            return await self._health_check()

        self.lifecycle.add_startup_hook(startup_hook)
        self.lifecycle.add_shutdown_hook(shutdown_hook)
        self.lifecycle.add_health_check_hook(health_check_hook)

    async def _startup(self) -> None:
        """Internal startup logic"""
        self.logger.info("Starting worker startup sequence")

        # Connect to all sources
        for source in self._sources:
            if hasattr(source, "connect"):
                await source.connect()
                self.logger.info(f"Connected to source: {type(source).__name__}")

        # Connect to all sinks
        for sink in self._sinks:
            if hasattr(sink, "connect"):
                await sink.connect()
                self.logger.info(f"Connected to sink: {type(sink).__name__}")

        # Call user-defined startup
        if hasattr(self, "startup"):
            await self.startup()

        self._running = True
        self.logger.info("Worker startup completed")

    async def _shutdown(self) -> None:
        """Internal shutdown logic"""
        self.logger.info("Starting worker shutdown sequence")
        self._running = False

        # Call user-defined shutdown
        if hasattr(self, "shutdown"):
            try:
                await self.shutdown()
            except Exception as e:
                self.logger.error(f"Error in user shutdown: {e}")

        # Disconnect from all sinks
        for sink in self._sinks:
            if hasattr(sink, "disconnect"):
                try:
                    await sink.disconnect()
                    self.logger.info(f"Disconnected from sink: {type(sink).__name__}")
                except Exception as e:
                    self.logger.error(f"Error disconnecting sink: {e}")

        # Disconnect from all sources
        for source in self._sources:
            if hasattr(source, "disconnect"):
                try:
                    await source.disconnect()
                    self.logger.info(f"Disconnected from source: {type(source).__name__}")
                except Exception as e:
                    self.logger.error(f"Error disconnecting source: {e}")

        self.logger.info("Worker shutdown completed")

    async def _health_check(self) -> bool:
        """Internal health check logic"""

        try:
            # Check if worker is running
            if not self._running:
                return False

            # Check sources health
            for source in self._sources:
                if hasattr(source, "health_check"):
                    if not await source.health_check():
                        return False

            # Check sinks health
            for sink in self._sinks:
                if hasattr(sink, "health_check"):
                    if not await sink.health_check():
                        return False

            # Call user-defined health check
            if hasattr(self, "health_check"):
                return await self.health_check()

            return True

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False

    @abstractmethod
    async def process(self, message: Message) -> Any:
        """
        Process a single message

        This method must be implemented by subclasses.
        """
        pass

    async def handle_error(self, message: Message, error: Exception) -> bool:
        """
        Handle processing error

        Returns True if error was handled, False to re-raise
        """
        self.failed_messages += 1

        self.logger.error(
            "Error processing message",
            error=error,
            message_id=message.message_id,
            retry_count=message.retry_count,
        )

        # Check if message should be retried
        if message.should_retry():
            message.increment_retry()
            self.logger.info(
                "Retrying message",
                message_id=message.message_id,
                retry_count=message.retry_count,
            )

            # Retry delay
            await asyncio.sleep(self.config.retry_delay * (2 ** (message.retry_count - 1)))

            try:
                await self.process(message)
                self.processed_messages += 1
                return True
            except Exception as retry_error:
                return await self.handle_error(message, retry_error)

        # No more retries, handle as dead letter
        await self._handle_dead_letter(message, error)
        return True

    async def _handle_dead_letter(self, message: Message, error: Exception) -> None:
        """Handle messages that exceeded retry limit"""
        self.logger.error(
            "Message exceeded retry limit, moving to dead letter",
            message_id=message.message_id,
            error=str(error),
        )

        # Call user-defined dead letter handler if available
        if hasattr(self, "handle_dead_letter"):
            await self.handle_dead_letter(message, error)

    async def send(self, data: Any, sink_index: int = 0) -> None:
        """Send data to a specific sink"""
        if sink_index < len(self._sinks):
            sink = self._sinks[sink_index]
            await sink.send(data)
        else:
            raise IndexError(f"Sink index {sink_index} out of range")

    async def broadcast(self, data: Any) -> None:
        """Send data to all sinks"""
        for sink in self._sinks:
            await sink.send(data)

    async def run(self) -> None:
        """
        Main worker execution loop

        This method handles the message consumption and processing loop.
        """

        if not self._sources:
            raise RuntimeError("No message sources configured")

        # Execute startup sequence
        await self.lifecycle.startup()

        self.logger.info("Starting message processing loop")

        try:
            # Create tasks for each source
            tasks = []
            for i, source in enumerate(self._sources):
                task = asyncio.create_task(
                    self._process_source(source, i),
                    name=f"source-{i}-{type(source).__name__}",
                )
                tasks.append(task)

            # Wait for all tasks
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            self.logger.error(f"Error in main processing loop: {e}")
            raise
        finally:
            self.logger.info("Message processing loop ended")

    async def _process_source(self, source: MessageBroker, source_index: int) -> None:
        """Process messages from a specific source"""

        self.logger.info(f"Starting processing for source {source_index}: {type(source).__name__}")

        try:
            async for message in source.consume():
                if not self.lifecycle.is_running():
                    break

                try:
                    # Process the message
                    await self.process(message)
                    self.processed_messages += 1

                    self.logger.debug(
                        "Message processed successfully",
                        message_id=message.message_id,
                        source_index=source_index,
                    )

                except Exception as e:
                    await self.handle_error(message, e)

        except Exception as e:
            self.logger.error(f"Error processing source {source_index}: {e}")
            raise
        finally:
            self.logger.info(f"Finished processing source {source_index}")

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics"""
        return {
            **self.lifecycle.get_stats(),
            "processed_messages": self.processed_messages,
            "failed_messages": self.failed_messages,
            "retry_count": self.retry_count,
            "sources_count": len(self._sources),
            "sinks_count": len(self._sinks),
        }

    @classmethod
    def auto_configure(cls, **kwargs) -> "Worker":
        """Create a worker with auto-detected configuration"""
        config = auto_detect_config()
        return cls(config=config, **kwargs)

    def run_sync(self) -> None:
        """Run the worker synchronously using WorkerRunner"""
        runner = WorkerRunner(self, self.config)
        runner.run_sync()


class SimpleWorker(Worker):
    """
    Simple worker implementation for basic use cases

    Users can provide a process_func instead of subclassing.
    """

    def __init__(
        self,
        process_func: Callable[[Message], Awaitable[Any]],
        config: Optional[WorkerConfig] = None,
    ):
        self.process_func = process_func
        super().__init__(config)

    async def process(self, message: Message) -> Any:
        """Process message using the provided function"""
        return await self.process_func(message)


class BatchWorker(Worker):
    """
    Worker that processes messages in batches
    """

    def __init__(self, config: Optional[WorkerConfig] = None, batch_size: Optional[int] = None):
        super().__init__(config)
        self.batch_size = batch_size or self.config.batch_size
        self._batch: List[Message] = []

    async def process(self, message: Message) -> Any:
        """Add message to batch and process when full"""
        self._batch.append(message)

        if len(self._batch) >= self.batch_size:
            batch = self._batch.copy()
            self._batch.clear()
            return await self.process_batch(batch)

    @abstractmethod
    async def process_batch(self, messages: List[Message]) -> Any:
        """Process a batch of messages - must be implemented by subclasses"""
        pass

    async def _shutdown(self) -> None:
        """Process remaining messages in batch during shutdown"""
        if self._batch:
            self.logger.info(f"Processing remaining {len(self._batch)} messages in batch")
            await self.process_batch(self._batch)
            self._batch.clear()

        await super()._shutdown()
