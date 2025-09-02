"""
Tests for HTTP workers
"""

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

# Import the HTTP workers
try:
    from pythia.brokers.http import HTTPWorker, PollerWorker, WebhookSenderWorker
    from pythia.http import HTTPClientConfig
    from pythia.config.http import WebhookConfig, PollerConfig
    from pythia.core.message import Message
    HAS_HTTP_SUPPORT = True
except ImportError as e:
# Remove debug print for clean output
    HAS_HTTP_SUPPORT = False

# Skip all tests if HTTP dependencies are not available
pytestmark = pytest.mark.skipif(
    not HAS_HTTP_SUPPORT,
    reason="HTTP dependencies not installed."
)


# Test worker implementations
class TestHTTPWorkerImpl(HTTPWorker):
    """Concrete HTTP worker for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_requests = []

    async def process(self):
        """Main processing loop - required by base Worker class"""
        # For testing, we don't need a continuous loop
        pass


class TestPollerWorkerImpl(PollerWorker):
    """Concrete Poller worker for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_messages = []

    async def process_message(self, message):
        """Process a polled message"""
        self.processed_messages.append(message)
        return {"processed": True, "data": message.body}


class TestWebhookSenderWorkerImpl(WebhookSenderWorker):
    """Concrete Webhook sender for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sent_webhooks = []

    async def process(self):
        """For testing, just track that process was called"""
        pass


class TestHTTPWorker:
    """Test base HTTPWorker functionality"""

    @pytest.mark.asyncio
    async def test_http_worker_initialization(self):
        """Test HTTP worker initialization"""
        config = HTTPClientConfig(connect_timeout=10.0)
        worker = TestHTTPWorkerImpl(http_config=config)

        assert worker.http_config == config
        assert worker.http_config.connect_timeout == 10.0
        assert worker.http_client is None

    @pytest.mark.asyncio
    async def test_connection_lifecycle(self):
        """Test HTTP worker connection and disconnection"""
        with patch("pythia.brokers.http.base.PythiaHTTPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            worker = TestHTTPWorkerImpl()

            # Test connection
            await worker.connect()
            mock_client_class.assert_called_once()
            mock_client.connect.assert_called_once()

            # Test disconnection
            await worker.disconnect()
            mock_client.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager"""
        with patch("pythia.brokers.http.base.PythiaHTTPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            worker = TestHTTPWorkerImpl()

            async with worker:
                mock_client_class.assert_called_once()
                mock_client.connect.assert_called_once()

            mock_client.disconnect.assert_called_once()


class TestPollerWorker:
    """Test PollerWorker functionality"""

    @pytest.mark.asyncio
    async def test_poller_worker_initialization(self):
        """Test poller worker initialization"""
        worker = TestPollerWorkerImpl(
            url="https://api.example.com/data",
            interval=30,
            method="GET",
            headers={"Authorization": "Bearer token"},
            poller_config=PollerConfig(
                base_url="https://api.example.com",
                url="https://api.example.com/data"
            )
        )

        assert worker.url == "https://api.example.com/data"
        assert worker.interval == 30
        assert worker.method == "GET"
        assert worker.headers == {"Authorization": "Bearer token"}
        assert worker.poller is not None

    @pytest.mark.asyncio
    async def test_poller_connection(self):
        """Test poller worker connection"""
        with patch("pythia.brokers.http.poller.HTTPPoller") as mock_poller_class, \
             patch("pythia.brokers.http.base.PythiaHTTPClient") as mock_client_class:

            mock_poller = AsyncMock()
            mock_client = AsyncMock()
            mock_poller_class.return_value = mock_poller
            mock_client_class.return_value = mock_client

            worker = TestPollerWorkerImpl(url="https://api.example.com/data")

            await worker.connect()
            mock_poller.connect.assert_called_once()
            mock_client.connect.assert_called_once()

            await worker.disconnect()
            mock_poller.disconnect.assert_called_once()
            mock_client.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message(self):
        """Test processing messages from poller"""
        worker = TestPollerWorkerImpl(
            url="https://api.example.com/data",
            poller_config=PollerConfig(
                base_url="https://api.example.com",
                url="https://api.example.com/data"
            )
        )

        message = Message(
            body={"id": 1, "status": "active"},
            headers={"status_code": 200}
        )

        result = await worker.process_message(message)

        assert result == {"processed": True, "data": {"id": 1, "status": "active"}}
        assert len(worker.processed_messages) == 1
        assert worker.processed_messages[0] == message

    @pytest.mark.asyncio
    async def test_force_poll(self):
        """Test forcing an immediate poll"""
        with patch("pythia.brokers.http.poller.HTTPPoller") as mock_poller_class:
            mock_poller = AsyncMock()
            mock_poller._poll_once.return_value = {"forced": True}
            mock_poller_class.return_value = mock_poller

            worker = TestPollerWorkerImpl(url="https://api.example.com/data")
            worker.poller = mock_poller

            result = await worker.force_poll()

            mock_poller._poll_once.assert_called_once()
            assert result is not None
            assert result.body == {"forced": True}
            assert result.headers == {"forced_poll": True}


class TestWebhookSenderWorker:
    """Test WebhookSenderWorker functionality"""

    @pytest.mark.asyncio
    async def test_webhook_sender_initialization(self):
        """Test webhook sender initialization"""
        config = WebhookConfig(base_url="https://api.example.com")
        worker = TestWebhookSenderWorkerImpl(
            base_url="https://api.example.com",
            webhook_config=config
        )

        assert worker.base_url == "https://api.example.com"
        assert worker.webhook_config == config
        assert worker.webhook_client is not None

    @pytest.mark.asyncio
    async def test_webhook_connection(self):
        """Test webhook worker connection"""
        with patch("pythia.brokers.http.webhook_sender.WebhookClient") as mock_client_class, \
             patch("pythia.brokers.http.base.PythiaHTTPClient") as mock_http_client_class:

            mock_webhook_client = AsyncMock()
            mock_http_client = AsyncMock()
            mock_client_class.return_value = mock_webhook_client
            mock_http_client_class.return_value = mock_http_client

            worker = TestWebhookSenderWorkerImpl()

            await worker.connect()
            mock_webhook_client.connect.assert_called_once()
            mock_http_client.connect.assert_called_once()

            await worker.disconnect()
            mock_webhook_client.disconnect.assert_called_once()
            mock_http_client.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_webhook(self):
        """Test sending a webhook"""
        with patch("pythia.brokers.http.webhook_sender.WebhookClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.send.return_value = True
            mock_client_class.return_value = mock_client

            worker = TestWebhookSenderWorkerImpl()
            worker.webhook_client = mock_client

            result = await worker.send_webhook(
                endpoint="/webhook",
                data={"event": "test"},
                headers={"Custom": "header"}
            )

            assert result is True
            mock_client.send.assert_called_once_with(
                endpoint="/webhook",
                data={"event": "test"},
                headers={"Custom": "header"},
                method="POST"
            )

    @pytest.mark.asyncio
    async def test_send_webhook_from_message(self):
        """Test sending webhook from Pythia message"""
        with patch("pythia.brokers.http.webhook_sender.WebhookClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.send.return_value = True
            mock_client_class.return_value = mock_client

            worker = TestWebhookSenderWorkerImpl()
            worker.webhook_client = mock_client

            message = Message(
                body={"user_id": 123, "action": "created"},
                headers={"source": "user-service"}
            )

            result = await worker.send_webhook_from_message(
                message=message,
                endpoint="/users/webhook",
                headers={"Custom": "header"}
            )

            assert result is True
            mock_client.send.assert_called_once_with(
                endpoint="/users/webhook",
                data={"user_id": 123, "action": "created"},
                headers={"source": "user-service", "Custom": "header"},
                method="POST"
            )

    @pytest.mark.asyncio
    async def test_broadcast_webhook(self):
        """Test broadcasting webhook to multiple endpoints"""
        with patch("pythia.brokers.http.webhook_sender.WebhookClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.send.side_effect = [True, False, True]  # Mixed results
            mock_client_class.return_value = mock_client

            worker = TestWebhookSenderWorkerImpl()
            worker.webhook_client = mock_client

            endpoints = ["/webhook1", "/webhook2", "/webhook3"]
            data = {"event": "broadcast_test"}

            results = await worker.broadcast_webhook(
                endpoints=endpoints,
                data=data,
                fail_fast=False
            )

            assert results == {
                "/webhook1": True,
                "/webhook2": False,
                "/webhook3": True
            }
            assert mock_client.send.call_count == 3

    @pytest.mark.asyncio
    async def test_broadcast_webhook_fail_fast(self):
        """Test broadcasting with fail_fast=True"""
        with patch("pythia.brokers.http.webhook_sender.WebhookClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.send.side_effect = [True, False]  # Fail on second
            mock_client_class.return_value = mock_client

            worker = TestWebhookSenderWorkerImpl()
            worker.webhook_client = mock_client

            endpoints = ["/webhook1", "/webhook2", "/webhook3"]
            data = {"event": "broadcast_test"}

            results = await worker.broadcast_webhook(
                endpoints=endpoints,
                data=data,
                fail_fast=True
            )

            # Should stop after failure
            assert results == {
                "/webhook1": True,
                "/webhook2": False
            }
            assert mock_client.send.call_count == 2  # Stopped early


@pytest.mark.asyncio
async def test_error_handling():
    """Test error handling for missing dependencies"""

    # Test with invalid configuration
    with pytest.raises(Exception):  # Should raise some error
        worker = TestHTTPWorkerImpl()
        # Force an error by passing invalid config
        worker.http_config = "invalid_config"
        await worker.connect()


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
