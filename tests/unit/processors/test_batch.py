"""
Tests for batch message processor
"""

import asyncio
import pytest
from unittest.mock import AsyncMock

from pythia.core.message import Message
from pythia.processors.batch import BatchMessageProcessor


class TestBatchMessageProcessor:
    """Test BatchMessageProcessor functionality"""

    @pytest.fixture
    def mock_process_func(self):
        """Mock process function"""
        async def process_batch(messages):
            return [msg.body.upper() for msg in messages]
        return process_batch

    @pytest.fixture
    def processor(self, mock_process_func):
        """Create processor instance"""
        return BatchMessageProcessor(
            process_func=mock_process_func,
            batch_size=3,
            max_wait_time=1.0,
            name="TestProcessor"
        )

    @pytest.fixture
    def messages(self):
        """Create test messages"""
        return [
            Message(body="hello", topic="test", partition=0, offset=i)
            for i in range(5)
        ]

    def test_processor_initialization(self, mock_process_func):
        """Test processor initialization"""
        processor = BatchMessageProcessor(
            process_func=mock_process_func,
            batch_size=5,
            max_wait_time=2.0,
            name="TestProc"
        )
        
        assert processor.batch_size == 5
        assert processor.max_wait_time == 2.0
        assert processor.name == "TestProc"
        assert processor.processed_count == 0
        assert processor.error_count == 0
        assert processor.batch_count == 0

    @pytest.mark.asyncio
    async def test_add_message_batch_size_trigger(self, processor, messages):
        """Test that batch processes when size is reached"""
        # Add messages one by one
        results = []
        for i, msg in enumerate(messages[:3]):
            result = await processor.add_message(msg)
            if result:
                results.extend(result)
        
        # Should have processed batch of 3
        assert len(results) == 3
        assert results == ["HELLO", "HELLO", "HELLO"]
        assert processor.batch_count == 1
        assert processor.processed_count == 3

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_add_message_time_trigger(self, processor, messages):
        """Test that batch processes when time limit is reached"""
        # Set a very short max_wait_time for testing
        processor.max_wait_time = 0.1
        
        # Add one message and wait
        result1 = await processor.add_message(messages[0])
        assert result1 is None  # Not processed yet
        
        # Wait for time trigger and add another
        await asyncio.sleep(0.15)
        result2 = await processor.add_message(messages[1])
        
        # Should process when time limit exceeded
        assert result2 is not None
        assert processor.batch_count >= 1

    @pytest.mark.asyncio
    async def test_process_batch_directly(self, processor, messages):
        """Test processing batch directly"""
        result = await processor.process_batch(messages[:2])
        
        assert len(result) == 2
        assert result == ["HELLO", "HELLO"]
        assert processor.processed_count == 2
        assert processor.batch_count == 1

    @pytest.mark.asyncio
    async def test_process_empty_batch(self, processor):
        """Test processing empty batch"""
        result = await processor.process_batch([])
        assert result == []
        assert processor.processed_count == 0

    @pytest.mark.asyncio
    async def test_flush_pending_messages(self, processor, messages):
        """Test flushing pending messages"""
        # Add messages without triggering batch
        await processor.add_message(messages[0])
        await processor.add_message(messages[1])
        
        assert len(processor._batch) == 2
        
        # Flush should process pending messages
        result = await processor.flush()
        assert len(result) == 2
        assert result == ["HELLO", "HELLO"]
        assert len(processor._batch) == 0

    @pytest.mark.asyncio
    async def test_flush_empty_batch(self, processor):
        """Test flushing when no pending messages"""
        result = await processor.flush()
        assert result is None

    @pytest.mark.asyncio
    async def test_error_handling_with_handler(self, mock_process_func, messages):
        """Test error handling with custom error handler"""
        # Make process function raise error
        async def failing_process(msgs):
            raise ValueError("Test error")
        
        # Create error handler
        error_handler = AsyncMock(return_value=True)
        
        processor = BatchMessageProcessor(
            process_func=failing_process,
            batch_size=2,
            error_handler=error_handler
        )
        
        result = await processor.process_batch(messages[:2])
        
        assert result == []  # Error was handled
        assert processor.error_count == 2
        error_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling_without_handler(self, messages):
        """Test error handling without custom error handler"""
        async def failing_process(msgs):
            raise ValueError("Test error")
        
        processor = BatchMessageProcessor(
            process_func=failing_process,
            batch_size=2
        )
        
        with pytest.raises(ValueError, match="Test error"):
            await processor.process_batch(messages[:2])
        
        assert processor.error_count == 2

    @pytest.mark.asyncio
    async def test_error_handler_not_handling(self, mock_process_func, messages):
        """Test error handler returning False (not handled)"""
        async def failing_process(msgs):
            raise ValueError("Test error")
        
        error_handler = AsyncMock(return_value=False)
        
        processor = BatchMessageProcessor(
            process_func=failing_process,
            batch_size=2,
            error_handler=error_handler
        )
        
        with pytest.raises(ValueError, match="Test error"):
            await processor.process_batch(messages[:2])

    def test_get_stats(self, processor):
        """Test getting processor statistics"""
        stats = processor.get_stats()
        
        expected_keys = [
            "name", "type", "batch_size", "max_wait_time",
            "processed_count", "error_count", "batch_count",
            "pending_messages", "success_rate"
        ]
        
        for key in expected_keys:
            assert key in stats
        
        assert stats["name"] == "TestProcessor"
        assert stats["type"] == "batch"
        assert stats["batch_size"] == 3
        assert stats["success_rate"] == 0  # No messages processed yet

    @pytest.mark.asyncio
    async def test_get_stats_with_data(self, processor, messages):
        """Test statistics after processing messages"""
        await processor.process_batch(messages[:3])
        
        stats = processor.get_stats()
        assert stats["processed_count"] == 3
        assert stats["batch_count"] == 1
        assert stats["success_rate"] == 100.0

    def test_reset_stats(self, processor):
        """Test resetting processor statistics"""
        processor.processed_count = 10
        processor.error_count = 2
        processor.batch_count = 5
        
        processor.reset_stats()
        
        assert processor.processed_count == 0
        assert processor.error_count == 0
        assert processor.batch_count == 0

    def test_repr(self, processor):
        """Test string representation"""
        repr_str = repr(processor)
        assert "BatchMessageProcessor" in repr_str
        assert "name=TestProcessor" in repr_str
        assert "batch_size=3" in repr_str

    @pytest.mark.asyncio
    async def test_concurrent_add_messages(self, processor, messages):
        """Test adding messages concurrently"""
        # This tests the async lock mechanism
        tasks = [processor.add_message(msg) for msg in messages[:6]]
        results = await asyncio.gather(*tasks)
        
        # Should have processed at least one batch (when size=3 reached)
        non_none_results = [r for r in results if r is not None]
        assert len(non_none_results) > 0
        assert processor.batch_count >= 1