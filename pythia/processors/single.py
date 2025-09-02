"""
Single Message Processor - Simple one-at-a-time message processing
"""

from typing import Any, Callable, Awaitable, Optional
from pythia.core.message import Message
from pythia.logging.setup import get_pythia_logger


class SingleMessageProcessor:
    """
    Simple single message processor for one-at-a-time processing

    Example:
        def process_message(message):
            return message.body.upper()

        processor = SingleMessageProcessor(process_message)
        result = await processor.process(message)
    """

    def __init__(
        self,
        process_func: Callable[[Message], Awaitable[Any]],
        error_handler: Optional[Callable[[Message, Exception], Awaitable[bool]]] = None,
        name: str = "SingleProcessor",
    ):
        """
        Initialize single message processor

        Args:
            process_func: Function to process each message
            error_handler: Optional error handler function
            name: Processor name for logging
        """
        self.process_func = process_func
        self.error_handler = error_handler
        self.name = name
        self.logger = get_pythia_logger(f"SingleMessageProcessor[{name}]")

        # Stats
        self.processed_count = 0
        self.error_count = 0

    async def process(self, message: Message) -> Any:
        """Process a single message"""
        try:
            self.logger.debug("Processing message", message_id=message.message_id)

            result = await self.process_func(message)
            self.processed_count += 1

            self.logger.debug("Message processed successfully", message_id=message.message_id)
            return result

        except Exception as e:
            self.error_count += 1
            self.logger.error(
                "Error processing message", error=str(e), message_id=message.message_id
            )

            # Try error handler if available
            if self.error_handler:
                handled = await self.error_handler(message, e)
                if handled:
                    return None

            # Re-raise if not handled
            raise

    def get_stats(self) -> dict:
        """Get processor statistics"""
        return {
            "name": self.name,
            "type": "single",
            "processed_count": self.processed_count,
            "error_count": self.error_count,
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

    def __repr__(self) -> str:
        return f"SingleMessageProcessor(name={self.name})"
