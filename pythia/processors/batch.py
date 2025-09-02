"""
Batch Message Processor - Process messages in batches for efficiency
"""

import asyncio
from typing import Any, Callable, Awaitable, List, Optional
from pythia.core.message import Message
from pythia.logging.setup import get_pythia_logger


class BatchMessageProcessor:
    """
    Batch message processor for efficient bulk processing

    Example:
        def process_batch(messages):
            return [msg.body.upper() for msg in messages]

        processor = BatchMessageProcessor(process_batch, batch_size=10)
        results = await processor.process_batch(messages)
    """

    def __init__(
        self,
        process_func: Callable[[List[Message]], Awaitable[List[Any]]],
        batch_size: int = 10,
        max_wait_time: float = 5.0,
        error_handler: Optional[Callable[[List[Message], Exception], Awaitable[bool]]] = None,
        name: str = "BatchProcessor",
    ):
        """
        Initialize batch message processor

        Args:
            process_func: Function to process batch of messages
            batch_size: Maximum batch size
            max_wait_time: Maximum time to wait for batch to fill
            error_handler: Optional error handler function
            name: Processor name for logging
        """
        self.process_func = process_func
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.error_handler = error_handler
        self.name = name
        self.logger = get_pythia_logger(f"BatchMessageProcessor[{name}]")

        # Batch management
        self._batch: List[Message] = []
        self._batch_lock = asyncio.Lock()
        self._last_batch_time = asyncio.get_event_loop().time()

        # Stats
        self.processed_count = 0
        self.error_count = 0
        self.batch_count = 0

    async def add_message(self, message: Message) -> Optional[List[Any]]:
        """
        Add message to batch and process if ready

        Returns:
            Optional[List[Any]]: Results if batch was processed, None otherwise
        """
        async with self._batch_lock:
            self._batch.append(message)
            current_time = asyncio.get_event_loop().time()

            # Check if we should process the batch
            should_process = (
                len(self._batch) >= self.batch_size
                or (current_time - self._last_batch_time) >= self.max_wait_time
            )

            if should_process and self._batch:
                return await self._process_current_batch()

            return None

    async def process_batch(self, messages: List[Message]) -> List[Any]:
        """Process a specific batch of messages"""
        if not messages:
            return []

        try:
            self.logger.debug("Processing batch", batch_size=len(messages))

            results = await self.process_func(messages)
            self.processed_count += len(messages)
            self.batch_count += 1

            self.logger.debug("Batch processed successfully", batch_size=len(messages))
            return results

        except Exception as e:
            self.error_count += len(messages)
            self.logger.error("Error processing batch", error=str(e), batch_size=len(messages))

            # Try error handler if available
            if self.error_handler:
                handled = await self.error_handler(messages, e)
                if handled:
                    return []

            # Re-raise if not handled
            raise

    async def flush(self) -> Optional[List[Any]]:
        """Force process any pending messages in batch"""
        async with self._batch_lock:
            if self._batch:
                return await self._process_current_batch()
            return None

    async def _process_current_batch(self) -> List[Any]:
        """Process the current batch (assumes lock is held)"""
        batch_to_process = self._batch.copy()
        self._batch.clear()
        self._last_batch_time = asyncio.get_event_loop().time()

        return await self.process_batch(batch_to_process)

    def get_stats(self) -> dict:
        """Get processor statistics"""
        return {
            "name": self.name,
            "type": "batch",
            "batch_size": self.batch_size,
            "max_wait_time": self.max_wait_time,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "batch_count": self.batch_count,
            "pending_messages": len(self._batch),
            "success_rate": (
                self.processed_count / (self.processed_count + self.error_count) * 100
                if (self.processed_count + self.error_count) > 0
                else 0
            ),
        }

    def reset_stats(self) -> None:
        """Reset processor statistics"""
        self.processed_count = 0
        self.error_count = 0
        self.batch_count = 0

    def __repr__(self) -> str:
        return f"BatchMessageProcessor(name={self.name}, batch_size={self.batch_size})"
