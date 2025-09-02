"""
Stream Processor - Continuous stream processing with windowing
"""

import asyncio
from typing import Any, Callable, Awaitable, List, Optional, AsyncIterator
from pythia.core.message import Message
from pythia.logging.setup import get_pythia_logger


class StreamProcessor:
    """
    Stream processor for continuous message processing with windowing

    Example:
        async def process_window(messages):
            return sum(msg.body['value'] for msg in messages)

        processor = StreamProcessor(process_window, window_size=100)

        async for result in processor.process_stream(message_stream):
            print(f"Window result: {result}")
    """

    def __init__(
        self,
        process_func: Callable[[List[Message]], Awaitable[Any]],
        window_size: int = 100,
        slide_size: Optional[int] = None,
        time_window_seconds: Optional[float] = None,
        error_handler: Optional[Callable[[List[Message], Exception], Awaitable[bool]]] = None,
        name: str = "StreamProcessor",
    ):
        """
        Initialize stream processor

        Args:
            process_func: Function to process message window
            window_size: Size of processing window
            slide_size: How many messages to slide window (default: window_size)
            time_window_seconds: Optional time-based windowing
            error_handler: Optional error handler function
            name: Processor name for logging
        """
        self.process_func = process_func
        self.window_size = window_size
        self.slide_size = slide_size or window_size
        self.time_window_seconds = time_window_seconds
        self.error_handler = error_handler
        self.name = name
        self.logger = get_pythia_logger(f"StreamProcessor[{name}]")

        # Window management
        self._window: List[Message] = []
        self._window_lock = asyncio.Lock()
        self._last_window_time = asyncio.get_event_loop().time()

        # Stats
        self.processed_count = 0
        self.error_count = 0
        self.window_count = 0

    async def process_stream(self, message_stream: AsyncIterator[Message]) -> AsyncIterator[Any]:
        """
        Process a stream of messages with windowing

        Args:
            message_stream: Async iterator of messages

        Yields:
            Any: Results from processing each window
        """
        self.logger.info("Starting stream processing", window_size=self.window_size)

        async for message in message_stream:
            try:
                result = await self.add_message(message)
                if result is not None:
                    yield result

            except Exception as e:
                self.error_count += 1
                self.logger.error("Error in stream processing", error=str(e))

                if self.error_handler:
                    await self.error_handler([message], e)
                else:
                    raise

        # Process any remaining messages in window
        final_result = await self.flush()
        if final_result is not None:
            yield final_result

    async def add_message(self, message: Message) -> Optional[Any]:
        """
        Add message to window and process if ready

        Returns:
            Optional[Any]: Result if window was processed, None otherwise
        """
        async with self._window_lock:
            self._window.append(message)
            current_time = asyncio.get_event_loop().time()

            # Check if we should process the window
            should_process_size = len(self._window) >= self.window_size
            should_process_time = (
                self.time_window_seconds
                and (current_time - self._last_window_time) >= self.time_window_seconds
            )

            if should_process_size or should_process_time:
                return await self._process_current_window()

            return None

    async def flush(self) -> Optional[Any]:
        """Force process any pending messages in window"""
        async with self._window_lock:
            if self._window:
                return await self._process_current_window()
            return None

    async def _process_current_window(self) -> Any:
        """Process the current window (assumes lock is held)"""
        if not self._window:
            return None

        # Create window to process
        window_to_process = self._window[: self.window_size].copy()

        # Slide the window
        if self.slide_size >= len(self._window):
            self._window.clear()
        else:
            self._window = self._window[self.slide_size :]

        self._last_window_time = asyncio.get_event_loop().time()

        try:
            self.logger.debug("Processing window", window_size=len(window_to_process))

            result = await self.process_func(window_to_process)
            self.processed_count += len(window_to_process)
            self.window_count += 1

            self.logger.debug("Window processed successfully", window_size=len(window_to_process))
            return result

        except Exception as e:
            self.error_count += len(window_to_process)
            self.logger.error(
                "Error processing window",
                error=str(e),
                window_size=len(window_to_process),
            )

            # Try error handler if available
            if self.error_handler:
                handled = await self.error_handler(window_to_process, e)
                if handled:
                    return None

            # Re-raise if not handled
            raise

    def get_stats(self) -> dict:
        """Get processor statistics"""
        return {
            "name": self.name,
            "type": "stream",
            "window_size": self.window_size,
            "slide_size": self.slide_size,
            "time_window_seconds": self.time_window_seconds,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "window_count": self.window_count,
            "pending_messages": len(self._window),
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
        self.window_count = 0

    def __repr__(self) -> str:
        return f"StreamProcessor(name={self.name}, window_size={self.window_size})"
