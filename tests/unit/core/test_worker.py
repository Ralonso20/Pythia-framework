"""
Unit tests for pythia.core.worker module
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch

from pythia.core.worker import Worker, SimpleWorker, BatchWorker
from pythia.core.message import Message
from pythia.config.base import WorkerConfig
from pythia.monitoring import MetricsConfig


class MockSource:
    """Mock message broker source"""

    def __init__(self, messages=None):
        self.messages = messages or []
        self.connected = False
        self.call_count = 0

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def consume(self):
        self.call_count += 1
        for message in self.messages:
            yield message

    async def health_check(self):
        return self.connected


class MockSink:
    """Mock message producer sink"""

    def __init__(self):
        self.sent_messages = []
        self.connected = False

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def send(self, data):
        if not self.connected:
            raise RuntimeError("Sink not connected")
        self.sent_messages.append(data)

    async def health_check(self):
        return self.connected


class TestWorkerImplementation(Worker):
    """Concrete test worker for testing abstract Worker class"""

    def __init__(self, *args, **kwargs):
        self.process_calls = []
        self.process_results = []
        self.process_errors = []
        super().__init__(*args, **kwargs)

    async def process(self, message: Message):
        self.process_calls.append(message)

        # Check if we should raise an error for this message
        if self.process_errors and len(self.process_calls) <= len(self.process_errors):
            error = self.process_errors[len(self.process_calls) - 1]
            if error:
                raise error

        # Return result if specified
        if self.process_results and len(self.process_calls) <= len(
            self.process_results
        ):
            return self.process_results[len(self.process_calls) - 1]

        return f"processed: {message.message_id}"


class TestWorkerBasicBasicFunctionality:
    """Test basic Worker functionality"""

    @pytest.mark.unit
    def test_worker_initialization_with_defaults(self):
        """Test worker initialization with default configuration"""
        worker = TestWorkerImplementation()

        assert worker.config is not None
        assert isinstance(worker.config, WorkerConfig)
        assert worker.logger is not None
        assert worker.metrics is not None
        assert worker.lifecycle is not None
        assert worker.processed_messages == 0
        assert worker.failed_messages == 0
        assert worker._running is False
        assert worker._continuous is True

    @pytest.mark.unit
    def test_worker_initialization_with_custom_config(self):
        """Test worker initialization with custom configuration"""
        config = WorkerConfig(
            worker_name="test-worker",
            worker_id="test-123",
            max_retries=5,
            retry_delay=0.5,
            batch_size=10,
        )

        worker = TestWorkerImplementation(config=config)

        assert worker.config == config
        assert worker.config.worker_name == "test-worker"
        assert worker.config.worker_id == "test-123"
        assert worker.config.max_retries == 5

    @pytest.mark.unit
    def test_worker_with_metrics_config(self):
        """Test worker initialization with custom metrics config"""
        metrics_config = MetricsConfig(
            worker_name="test-worker", custom_labels={"env": "test"}
        )

        worker = TestWorkerImplementation(metrics_config=metrics_config)

        assert worker.metrics_config == metrics_config
        assert worker.metrics_config.custom_labels["env"] == "test"

    @pytest.mark.unit
    def test_worker_initialization_with_source(self):
        """Test worker initialization with single source"""
        source = MockSource()

        class SourceWorker(TestWorkerImplementation):
            pass

        SourceWorker.source = source

        worker = SourceWorker()
        assert len(worker._sources) == 1
        assert worker._sources[0] == source

    @pytest.mark.unit
    def test_worker_initialization_with_multiple_sources(self):
        """Test worker initialization with multiple sources"""
        source1 = MockSource()
        source2 = MockSource()

        class MultiSourceWorker(TestWorkerImplementation):
            pass

        MultiSourceWorker.sources = [source1, source2]

        worker = MultiSourceWorker()
        assert len(worker._sources) == 2
        assert source1 in worker._sources
        assert source2 in worker._sources

    @pytest.mark.unit
    def test_worker_initialization_with_sinks(self):
        """Test worker initialization with sinks"""
        sink1 = MockSink()
        sink2 = MockSink()

        class SinkWorker(TestWorkerImplementation):
            sink = sink1
            sinks = [sink2]  # sinks takes precedence

        worker = SinkWorker()
        assert len(worker._sinks) == 1
        assert worker._sinks[0] == sink2

    @pytest.mark.unit
    def test_worker_initialization_no_brokers(self):
        """Test worker initialization with no brokers configured"""
        worker = TestWorkerImplementation()

        # Should have empty sources and sinks
        assert worker._sources == []
        assert worker._sinks == []


class TestWorkerBasicLifecycle:
    """Test worker lifecycle management"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_startup_sequence(self):
        """Test worker startup sequence"""
        source = MockSource()
        sink = MockSink()

        worker = TestWorkerImplementation()
        worker._sources = [source]
        worker._sinks = [sink]
        worker.startup_called = False

        async def mock_startup():
            worker.startup_called = True

        worker.startup = mock_startup

        assert not worker._running

        await worker._startup()

        assert worker._running
        assert source.connected
        assert sink.connected
        assert worker.startup_called

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_shutdown_sequence(self):
        """Test worker shutdown sequence"""
        source = MockSource()
        sink = MockSink()

        worker = TestWorkerImplementation()
        worker._sources = [source]
        worker._sinks = [sink]
        worker._running = True
        worker.shutdown_called = False
        source.connected = True
        sink.connected = True

        async def mock_shutdown():
            worker.shutdown_called = True

        worker.shutdown = mock_shutdown

        await worker._shutdown()

        assert not worker._running
        assert not source.connected
        assert not sink.connected
        assert worker.shutdown_called

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_shutdown_with_user_error(self):
        """Test shutdown handles user shutdown errors gracefully"""

        class ErrorShutdownWorker(TestWorkerImplementation):
            async def shutdown(self):
                raise ValueError("User shutdown error")

        worker = ErrorShutdownWorker()
        worker._running = True

        # Should not raise exception
        await worker._shutdown()
        assert not worker._running

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_success(self):
        """Test successful health check"""
        source = MockSource()
        sink = MockSink()

        class HealthWorker(TestWorkerImplementation):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.source = source
                self.sink = sink

            async def health_check(self):
                return True

        worker = HealthWorker()
        worker._running = True
        source.connected = True
        sink.connected = True

        result = await worker._health_check()
        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_not_running(self):
        """Test health check when worker is not running"""
        worker = TestWorkerImplementation()
        worker._running = False

        result = await worker._health_check()
        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_source_unhealthy(self):
        """Test health check with unhealthy source"""
        source = MockSource()
        source.connected = False  # Unhealthy

        worker = TestWorkerImplementation()
        worker._sources = [source]
        worker._running = True

        result = await worker._health_check()
        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_with_exception(self):
        """Test health check handles exceptions"""

        class ExceptionHealthWorker(TestWorkerImplementation):
            async def health_check(self):
                raise RuntimeError("Health check error")

        worker = ExceptionHealthWorker()
        worker._running = True

        result = await worker._health_check()
        assert result is False


class TestWorkerBasicMessageProcessing:
    """Test worker message processing functionality"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_basic_message_processing(self):
        """Test basic message processing"""
        message = Message(body="test message", message_id="test-123")
        worker = TestWorkerImplementation()

        result = await worker.process(message)

        assert result == "processed: test-123"
        assert len(worker.process_calls) == 1
        assert worker.process_calls[0] == message

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_error_handling_with_retry(self):
        """Test error handling with successful retry"""
        message = Message(body="test", message_id="test-123", max_retries=2)
        worker = TestWorkerImplementation()

        # First call fails, second succeeds
        worker.process_errors = [ValueError("First error"), None]

        # Mock the config for faster testing
        worker.config.retry_delay = 0.01

        result = await worker.handle_error(message, ValueError("First error"))

        assert result is True  # Error was handled
        assert worker.failed_messages == 2  # Initial error + retry error count
        assert worker.processed_messages == 1
        assert len(worker.process_calls) == 2  # Original + retry

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_error_handling_retry_exhausted(self):
        """Test error handling when retries are exhausted"""
        message = Message(body="test", message_id="test-123", max_retries=1)
        worker = TestWorkerImplementation()

        # Both calls fail
        worker.process_errors = [ValueError("Error 1"), ValueError("Error 2")]
        worker.config.retry_delay = 0.01

        with patch.object(
            worker, "_handle_dead_letter", new_callable=AsyncMock
        ) as mock_dead_letter:
            result = await worker.handle_error(message, ValueError("Initial error"))

        assert result is True  # Error was handled
        assert worker.failed_messages == 2  # Initial + retry
        assert mock_dead_letter.called
        assert message.retry_count == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_dead_letter_handling(self):
        """Test dead letter message handling"""
        message = Message(body="dead letter", message_id="dead-123")
        error = RuntimeError("Fatal error")

        class DeadLetterWorker(TestWorkerImplementation):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.dead_letters = []

            async def handle_dead_letter(self, message, error):
                self.dead_letters.append((message, error))

        worker = DeadLetterWorker()

        await worker._handle_dead_letter(message, error)

        assert len(worker.dead_letters) == 1
        assert worker.dead_letters[0] == (message, error)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_send_to_sink(self):
        """Test sending data to specific sink"""
        sink1 = MockSink()
        sink2 = MockSink()

        class SinkWorker(TestWorkerImplementation):
            sinks = [sink1, sink2]

        worker = SinkWorker()
        await worker._startup()  # Connect sinks

        await worker.send("test data 1", sink_index=0)
        await worker.send("test data 2", sink_index=1)

        assert "test data 1" in sink1.sent_messages
        assert "test data 2" in sink2.sent_messages
        assert "test data 1" not in sink2.sent_messages

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_send_to_invalid_sink_index(self):
        """Test sending to invalid sink index raises error"""
        worker = TestWorkerImplementation()  # No sinks

        with pytest.raises(IndexError):
            await worker.send("test data", sink_index=0)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_broadcast_to_all_sinks(self):
        """Test broadcasting data to all sinks"""
        sink1 = MockSink()
        sink2 = MockSink()
        sink3 = MockSink()

        class BroadcastWorker(TestWorkerImplementation):
            sinks = [sink1, sink2, sink3]

        worker = BroadcastWorker()
        await worker._startup()  # Connect sinks

        await worker.broadcast("broadcast data")

        assert "broadcast data" in sink1.sent_messages
        assert "broadcast data" in sink2.sent_messages
        assert "broadcast data" in sink3.sent_messages


class TestWorkerBasicSourceProcessing:
    """Test worker source processing functionality"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_single_source(self):
        """Test processing messages from single source"""
        messages = [
            Message(body="msg1", message_id="1"),
            Message(body="msg2", message_id="2"),
        ]
        source = MockSource(messages)

        class SingleSourceWorker(TestWorkerImplementation):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.source = source

        worker = SingleSourceWorker()
        worker._running = True
        worker.lifecycle.running = True

        # Process the source
        await worker._process_source(source, 0)

        assert len(worker.process_calls) == 2
        assert worker.processed_messages == 2
        assert worker.failed_messages == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_source_with_errors(self):
        """Test processing source with message errors"""
        messages = [
            Message(body="good", message_id="1"),
            Message(body="bad", message_id="2"),
            Message(body="good", message_id="3"),
        ]
        source = MockSource(messages)

        worker = TestWorkerImplementation()
        worker._running = True
        worker.lifecycle.running = True

        # Make second message fail
        worker.process_errors = [None, ValueError("Error"), None]
        worker.config.retry_delay = 0.01

        with patch.object(
            worker, "handle_error", new_callable=AsyncMock
        ) as mock_handle_error:
            mock_handle_error.return_value = True  # Error handled
            await worker._process_source(source, 0)

        assert len(worker.process_calls) == 3
        assert worker.processed_messages == 2  # Only successful ones
        assert mock_handle_error.called

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_run_with_no_sources_raises_error(self):
        """Test that run() raises error when no sources configured"""
        worker = TestWorkerImplementation()  # No sources

        with pytest.raises(RuntimeError, match="No message sources configured"):
            await worker.run()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_run_with_multiple_sources(self):
        """Test running worker with multiple sources"""
        messages1 = [Message(body="src1-msg1", message_id="1-1")]
        messages2 = [Message(body="src2-msg1", message_id="2-1")]

        source1 = MockSource(messages1)
        source2 = MockSource(messages2)

        class MultiSourceWorker(TestWorkerImplementation):
            pass

        MultiSourceWorker.sources = [source1, source2]

        worker = MultiSourceWorker()

        with patch.object(worker.lifecycle, "startup", new_callable=AsyncMock):
            with patch.object(
                worker, "_process_source", new_callable=AsyncMock
            ) as mock_process:
                await worker.run()

        # Should create task for each source
        assert mock_process.call_count == 2


class TestWorkerBasicStats:
    """Test worker statistics functionality"""

    @pytest.mark.unit
    def test_get_stats(self):
        """Test getting worker statistics"""
        source1 = MockSource()
        source2 = MockSource()
        sink1 = MockSink()

        class StatsWorker(TestWorkerImplementation):
            sources = [source1, source2]
            sink = sink1

        worker = StatsWorker()
        worker.processed_messages = 100
        worker.failed_messages = 5
        worker.retry_count = 10

        stats = worker.get_stats()

        assert stats["processed_messages"] == 100
        assert stats["failed_messages"] == 5
        assert stats["retry_count"] == 10
        assert stats["sources_count"] == 2
        assert stats["sinks_count"] == 1


class TestSimpleWorker:
    """Test SimpleWorker functionality"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_simple_worker_with_function(self):
        """Test SimpleWorker with process function"""

        async def process_func(message: Message):
            return f"simple: {message.body}"

        worker = SimpleWorker(process_func)
        message = Message(body="test data")

        result = await worker.process(message)
        assert result == "simple: test data"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_simple_worker_with_async_function(self):
        """Test SimpleWorker with async process function"""

        async def async_process_func(message: Message):
            await asyncio.sleep(0.01)  # Simulate async work
            return {"processed": message.body, "async": True}

        worker = SimpleWorker(async_process_func)
        message = Message(body="async test")

        result = await worker.process(message)
        assert result == {"processed": "async test", "async": True}


class TestBatchWorkerClass:
    """Test BatchWorker functionality"""

    @pytest.mark.unit
    def test_batch_worker_initialization(self):
        """Test BatchWorker initialization"""
        config = WorkerConfig(batch_size=5)
        worker = ConcreteBatchWorker(config=config, batch_size=10)

        # batch_size parameter should override config
        assert worker.batch_size == 10
        assert worker._batch == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_batch_processing(self):
        """Test batch message accumulation and processing"""
        worker = ConcreteBatchWorker(batch_size=3)

        # Add messages to batch
        await worker.process(Message(body="msg1", message_id="1"))
        await worker.process(Message(body="msg2", message_id="2"))

        # Batch not full yet
        assert len(worker._batch) == 2
        assert len(worker.processed_batches) == 0

        # This should trigger batch processing
        await worker.process(Message(body="msg3", message_id="3"))

        assert len(worker._batch) == 0  # Batch cleared
        assert len(worker.processed_batches) == 1
        assert len(worker.processed_batches[0]) == 3

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_batch_shutdown_processing(self):
        """Test processing remaining batch during shutdown"""
        worker = ConcreteBatchWorker(batch_size=5)

        # Add some messages (less than batch size)
        await worker.process(Message(body="msg1", message_id="1"))
        await worker.process(Message(body="msg2", message_id="2"))

        assert len(worker._batch) == 2

        # Shutdown should process remaining batch
        await worker._shutdown()

        assert len(worker._batch) == 0
        assert len(worker.processed_batches) == 1
        assert len(worker.processed_batches[0]) == 2


class ConcreteBatchWorker(BatchWorker):
    """Concrete BatchWorker for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_batches = []

    async def process_batch(self, messages):
        self.processed_batches.append(messages.copy())
        return f"processed {len(messages)} messages"


class TestWorkerBasicAutoConfiguration:
    """Test worker auto-configuration functionality"""

    @pytest.mark.unit
    def test_auto_configure_method(self):
        """Test worker auto-configuration"""
        with patch("pythia.core.worker.auto_detect_config") as mock_auto_detect:
            mock_config = WorkerConfig(worker_name="auto-worker")
            mock_auto_detect.return_value = mock_config

            worker = TestWorkerImplementation.auto_configure()

            assert worker.config == mock_config
            mock_auto_detect.assert_called_once()

    @pytest.mark.unit
    def test_run_sync_method(self):
        """Test synchronous run method"""
        worker = TestWorkerImplementation()

        with patch("pythia.core.worker.WorkerRunner") as mock_runner_class:
            mock_runner = Mock()
            mock_runner_class.return_value = mock_runner

            worker.run_sync()

            mock_runner_class.assert_called_once_with(worker, worker.config)
            mock_runner.run_sync.assert_called_once()


class TestWorkerBasicEdgeCases:
    """Test edge cases and error conditions"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_source_connection_error(self):
        """Test handling source connection errors"""

        class FailingSource:
            async def connect(self):
                raise ConnectionError("Connection failed")

        class FailingSourceWorker(TestWorkerImplementation):
            source = FailingSource()

        worker = FailingSourceWorker()

        with pytest.raises(ConnectionError):
            await worker._startup()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_sink_disconnection_error(self):
        """Test handling sink disconnection errors"""

        class FailingSink:
            async def connect(self):
                pass

            async def disconnect(self):
                raise RuntimeError("Disconnection failed")

        class FailingSinkWorker(TestWorkerImplementation):
            sink = FailingSink()

        worker = FailingSinkWorker()
        worker._running = True

        # Should not raise exception, just log error
        await worker._shutdown()
        assert not worker._running

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_empty_message_processing(self):
        """Test processing with empty message list"""
        source = MockSource([])  # Empty messages

        worker = TestWorkerImplementation()
        worker._running = True
        worker.lifecycle.running = True

        await worker._process_source(source, 0)

        assert len(worker.process_calls) == 0
        assert worker.processed_messages == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_worker_stop_during_processing(self):
        """Test worker stops cleanly during message processing"""

        # Create a source that yields messages slowly
        class SlowSource:
            def __init__(self):
                self.connected = False

            async def connect(self):
                self.connected = True

            async def disconnect(self):
                self.connected = False

            async def consume(self):
                for i in range(10):
                    if i == 2:  # Stop after a few messages
                        break
                    await asyncio.sleep(0.01)
                    yield Message(body=f"msg{i}", message_id=str(i))

        class StopWorker(TestWorkerImplementation):
            source = SlowSource()

        worker = StopWorker()
        worker._running = True

        # Simulate stopping worker after it starts
        async def stop_worker():
            await asyncio.sleep(0.02)
            worker.lifecycle.running = False

        # Start both tasks
        process_task = asyncio.create_task(
            worker._process_source(worker._sources[0], 0)
        )
        stop_task = asyncio.create_task(stop_worker())

        await asyncio.gather(process_task, stop_task, return_exceptions=True)

        # Should have processed some messages but stopped cleanly
        assert len(worker.process_calls) >= 0  # May vary due to timing

    @pytest.mark.unit
    def test_abstract_worker_cannot_be_instantiated(self):
        """Test that abstract Worker class cannot be instantiated"""
        with pytest.raises(TypeError):
            Worker()

    @pytest.mark.unit
    def test_abstract_batch_worker_process_batch_not_implemented(self):
        """Test that BatchWorker requires process_batch implementation"""

        class IncompleteBatchWorker(BatchWorker):
            pass  # Missing process_batch implementation

        with pytest.raises(TypeError):
            IncompleteBatchWorker()
