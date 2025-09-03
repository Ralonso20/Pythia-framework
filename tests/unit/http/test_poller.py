"""
Tests for HTTP Poller
"""

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

import httpx

from pythia.http.poller import HTTPPoller
from pythia.core.message import Message
from pythia.config.http import PollerConfig


class TestHTTPPoller:
    """Test HTTPPoller functionality"""

    @pytest.fixture
    def poller(self):
        """Create basic HTTP poller"""
        return HTTPPoller(
            url="https://api.example.com/data",
            interval=60,
            method="GET",
            base_url="https://api.example.com"  # Required by HTTPConfig
        )

    @pytest.fixture
    def mock_response(self):
        """Create mock HTTP response"""
        response = MagicMock()
        response.status_code = 200
        response.is_success = True
        response.json.return_value = {"data": ["item1", "item2"]}
        response.text = '{"data": ["item1", "item2"]}'
        response.content = b'{"data": ["item1", "item2"]}'
        response.headers = {
            "content-type": "application/json",
            "etag": '"abc123"',
            "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT"
        }
        response.raise_for_status.return_value = None
        return response

    def test_poller_initialization(self):
        """Test poller initialization"""
        poller = HTTPPoller(
            url="https://api.example.com/test",
            interval=30,
            method="POST",
            headers={"Authorization": "Bearer token"},
            params={"limit": "100"},
            base_url="https://api.example.com"  # Required by HTTPConfig
        )
        
        assert poller.url == "https://api.example.com/test"
        assert poller.interval == 30
        assert poller.method == "POST"
        assert poller.headers == {"Authorization": "Bearer token"}
        assert poller.params == {"limit": "100"}
        assert not poller._polling
        assert poller.client is None

    def test_poller_with_config(self):
        """Test poller initialization with config"""
        config = PollerConfig(
            base_url="https://api.test.com",
            url="https://api.test.com",
            interval=120,
            connect_timeout=10.0,
            read_timeout=30.0
        )
        
        poller = HTTPPoller(url="https://override.com", config=config, base_url="https://api.test.com")
        
        assert poller.config.connect_timeout == 10.0
        assert poller.config.read_timeout == 30.0
        # URL from constructor should override config
        assert poller.url == "https://override.com"

    def test_poller_with_data_extractor(self):
        """Test poller with custom data extractor"""
        def extract_data(_response_data):
            return _response_data.get("items", [])
        
        poller = HTTPPoller(
            url="https://api.example.com/data",
            data_extractor=extract_data,
            base_url="https://api.example.com"
        )
        
        assert poller.data_extractor == extract_data

    @pytest.mark.asyncio
    async def test_connect(self, poller):
        """Test HTTP client connection"""
        await poller.connect()
        
        assert poller.client is not None
        assert isinstance(poller.client, httpx.AsyncClient)
        assert poller.is_connected()

    @pytest.mark.asyncio
    async def test_connect_idempotent(self, poller):
        """Test that connect is idempotent"""
        await poller.connect()
        client1 = poller.client
        
        await poller.connect()
        client2 = poller.client
        
        assert client1 is client2

    @pytest.mark.asyncio
    async def test_disconnect(self, poller):
        """Test HTTP client disconnection"""
        await poller.connect()
        assert poller.is_connected()
        
        await poller.disconnect()
        
        assert not poller.is_connected()
        assert not poller._polling
        assert poller.client is None

    @pytest.mark.asyncio
    async def test_disconnect_handles_errors(self, poller):
        """Test disconnect handles client close errors gracefully"""
        await poller.connect()
        
        # Mock client close to raise exception
        poller.client.aclose = AsyncMock(side_effect=Exception("Close error"))
        
        # Should not raise exception
        await poller.disconnect()
        assert not poller.is_connected()

    def test_is_connected_false_when_not_initialized(self, poller):
        """Test is_connected returns False when client not initialized"""
        assert not poller.is_connected()

    @pytest.mark.asyncio
    async def test_health_check_success(self, poller, mock_response):
        """Test successful health check"""
        await poller.connect()
        
        mock_response.is_success = True
        poller.client.head = AsyncMock(return_value=mock_response)
        
        result = await poller.health_check()
        assert result is True
        poller.client.head.assert_called_once_with(poller.url, timeout=5.0)

    @pytest.mark.asyncio
    async def test_health_check_failure(self, poller):
        """Test health check failure"""
        await poller.connect()
        
        poller.client.head = AsyncMock(side_effect=httpx.RequestError("Network error"))
        
        result = await poller.health_check()
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_not_connected(self, poller):
        """Test health check when not connected"""
        result = await poller.health_check()
        assert result is False

    @pytest.mark.asyncio
    async def test_poll_json_response(self, poller, mock_response):
        """Test polling with JSON response"""
        await poller.connect()
        
        poller.client.request = AsyncMock(return_value=mock_response)
        
        messages = await poller._poll()
        
        assert len(messages) == 1
        assert messages[0].body == {"data": ["item1", "item2"]}
        assert messages[0].headers["http_url"] == poller.url
        assert messages[0].headers["http_status_code"] == "200"
        assert "json" in messages[0].headers["content_type"]

    @pytest.mark.asyncio
    async def test_poll_text_response(self, poller):
        """Test polling with text response"""
        await poller.connect()
        
        response = MagicMock()
        response.status_code = 200
        response.is_success = True
        response.text = "plain text response"
        response.headers = {"content-type": "text/plain"}
        response.raise_for_status.return_value = None
        
        poller.client.request = AsyncMock(return_value=response)
        
        messages = await poller._poll()
        
        assert len(messages) == 1
        assert messages[0].body == "plain text response"

    @pytest.mark.asyncio
    async def test_poll_with_data_extractor(self, poller, mock_response):
        """Test polling with custom data extractor"""
        def extract_items(data):
            return data.get("data", [])
        
        poller.set_data_extractor(extract_items)
        await poller.connect()
        
        poller.client.request = AsyncMock(return_value=mock_response)
        
        messages = await poller._poll()
        
        assert len(messages) == 2  # Two items extracted
        assert messages[0].body == "item1"
        assert messages[1].body == "item2"

    @pytest.mark.asyncio
    async def test_poll_data_extractor_error(self, poller, mock_response):
        """Test polling when data extractor fails"""
        def failing_extractor(data):
            raise ValueError("Extraction failed")
        
        poller.set_data_extractor(failing_extractor)
        await poller.connect()
        
        poller.client.request = AsyncMock(return_value=mock_response)
        
        messages = await poller._poll()
        
        # Should fall back to raw response
        assert len(messages) == 1
        assert messages[0].body == {"data": ["item1", "item2"]}

    @pytest.mark.asyncio
    async def test_poll_with_conditional_requests(self, poller, mock_response):
        """Test polling with conditional request headers"""
        poller.config.use_conditional_requests = True
        poller._last_etag = '"old-etag"'
        poller._last_modified = "Wed, 20 Oct 2015 07:28:00 GMT"
        
        await poller.connect()
        poller.client.request = AsyncMock(return_value=mock_response)
        
        await poller._poll()
        
        # Should include conditional headers
        call_args = poller.client.request.call_args
        headers = call_args[1]['headers']
        assert headers['If-None-Match'] == '"old-etag"'
        assert headers['If-Modified-Since'] == "Wed, 20 Oct 2015 07:28:00 GMT"

    @pytest.mark.asyncio
    async def test_poll_304_not_modified(self, poller):
        """Test handling 304 Not Modified response"""
        await poller.connect()
        
        response = MagicMock()
        response.status_code = 304
        
        poller.client.request = AsyncMock(return_value=response)
        
        messages = await poller._poll()
        
        assert messages == []

    @pytest.mark.asyncio
    async def test_poll_http_error(self, poller):
        """Test handling HTTP error during poll"""
        await poller.connect()
        
        response = MagicMock()
        response.status_code = 500
        response.text = "Internal Server Error"
        
        error = httpx.HTTPStatusError("HTTP Error", request=MagicMock(), response=response)
        poller.client.request = AsyncMock(side_effect=error)
        
        with pytest.raises(httpx.HTTPStatusError):
            await poller._poll()

    @pytest.mark.asyncio
    async def test_poll_json_parse_error(self, poller):
        """Test handling JSON parse error"""
        await poller.connect()
        
        response = MagicMock()
        response.status_code = 200
        response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        response.text = "invalid json {"
        response.headers = {"content-type": "application/json"}
        response.raise_for_status.return_value = None
        
        poller.client.request = AsyncMock(return_value=response)
        
        messages = await poller._poll()
        
        # Should fall back to text
        assert len(messages) == 1
        assert messages[0].body == "invalid json {"

    @pytest.mark.asyncio
    async def test_consume_basic_flow(self, poller, mock_response):
        """Test basic consume flow"""
        poller.interval = 0.1  # Short interval for test
        await poller.connect()
        
        poller.client.request = AsyncMock(return_value=mock_response)
        
        messages = []
        poll_count = 0
        
        async for message in poller.consume():
            messages.append(message)
            poll_count += 1
            if poll_count >= 2:  # Stop after 2 polls
                poller._polling = False
                break
        
        assert len(messages) >= 2
        assert all(isinstance(msg, Message) for msg in messages)

    @pytest.mark.asyncio
    async def test_consume_auto_connect(self, poller):
        """Test that consume auto-connects if not connected"""
        assert not poller.is_connected()
        
        poller.interval = 0.1
        
        # Mock connect method to set client after connecting
        async def mock_connect():
            poller.client = MagicMock()  # Set mock client after connect
        
        poller.connect = AsyncMock(side_effect=mock_connect)
        poller._poll = AsyncMock(return_value=[Message(body="test")])
        
        messages = []
        async for message in poller.consume():
            messages.append(message)
            if len(messages) >= 1:
                poller._polling = False
                break
        
        poller.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_consume_handles_http_errors(self, poller):
        """Test consume handles HTTP errors gracefully"""
        poller.interval = 0.1
        await poller.connect()
        
        # First call fails, second succeeds
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"test": "data"}
        mock_response.headers = {"content-type": "application/json"}
        mock_response.raise_for_status.return_value = None
        
        call_count = 0
        
        def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.RequestError("Network error")
            return mock_response
        
        poller.client.request = AsyncMock(side_effect=side_effect)
        
        messages = []
        async for message in poller.consume():
            messages.append(message)
            if len(messages) >= 1:
                poller._polling = False
                break
        
        # Should recover from error and get message from second call
        assert len(messages) >= 1

    @pytest.mark.asyncio
    async def test_consume_timing(self, poller, mock_response):
        """Test consume respects polling interval"""
        poller.interval = 0.2  # 200ms interval
        await poller.connect()
        
        poller.client.request = AsyncMock(return_value=mock_response)
        
        start_time = datetime.now()
        messages = []
        
        async for message in poller.consume():
            messages.append(message)
            if len(messages) >= 2:
                poller._polling = False
                break
        
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        
        # Should take at least the interval time
        assert elapsed >= 0.2

    def test_set_data_extractor(self, poller):
        """Test setting custom data extractor"""
        def custom_extractor(data):
            return data.get("results", [])
        
        poller.set_data_extractor(custom_extractor)
        
        assert poller.data_extractor == custom_extractor

    def test_get_poll_stats(self, poller):
        """Test getting poll statistics"""
        poller._polling = True
        poller._last_poll_time = datetime.now()
        poller._last_etag = '"etag123"'
        
        stats = poller.get_poll_stats()
        
        assert stats["url"] == poller.url
        assert stats["interval"] == poller.interval
        assert stats["method"] == poller.method
        assert stats["is_polling"] is True
        assert stats["has_etag"] is True
        assert stats["last_poll_time"] is not None

    def test_repr(self, poller):
        """Test string representation"""
        repr_str = repr(poller)
        
        assert "HTTPPoller" in repr_str
        assert poller.url in repr_str
        assert str(poller.interval) in repr_str
        assert poller.method in repr_str


class TestHTTPPollerEdgeCases:
    """Test edge cases and error conditions"""

    @pytest.mark.asyncio
    async def test_poll_without_client(self):
        """Test polling without initialized client"""
        poller = HTTPPoller(url="https://example.com", base_url="https://example.com")
        
        with pytest.raises(RuntimeError, match="HTTP client not initialized"):
            await poller._poll()

    @pytest.mark.asyncio
    async def test_consume_without_client_raises_error(self):
        """Test consume raises error if connection fails"""
        poller = HTTPPoller(url="invalid://url", base_url="invalid://url")
        
        # Mock connect to fail
        with patch.object(poller, 'connect', side_effect=Exception("Connection failed")):
            with pytest.raises(Exception, match="Connection failed"):
                async for _ in poller.consume():
                    break

    @pytest.mark.asyncio
    async def test_parse_response_error_fallback(self):
        """Test response parsing error creates fallback message"""
        poller = HTTPPoller(url="https://example.com", base_url="https://example.com")
        
        response = MagicMock()
        response.text = "some response text"
        response.status_code = 200
        response.headers = {"content-type": "application/json"}
        response.json.side_effect = Exception("JSON parse error")
        
        # Test that it falls back to text when JSON parsing fails
        messages = await poller._parse_response(response)
        
        # Should create message with text fallback
        assert len(messages) == 1
        assert messages[0].body == "some response text"

    @pytest.mark.asyncio
    async def test_data_extractor_with_list_response(self):
        """Test data extractor with list response"""
        def extract_id(item):
            return item.get("id")
        
        poller = HTTPPoller(url="https://example.com", data_extractor=extract_id, base_url="https://example.com")
        
        # Mock response with list of objects
        list_response = MagicMock()
        list_response.status_code = 200
        list_response.json.return_value = [{"id": 1}, {"id": 2}, {"id": 3}]
        list_response.headers = {"content-type": "application/json"}
        list_response.text = '[{"id": 1}, {"id": 2}, {"id": 3}]'
        list_response.raise_for_status.return_value = None
        
        messages = await poller._parse_response(list_response)
        
        assert len(messages) == 3
        assert messages[0].body == 1
        assert messages[1].body == 2
        assert messages[2].body == 3

    def test_method_case_normalization(self):
        """Test HTTP method is normalized to uppercase"""
        poller = HTTPPoller(url="https://example.com", method="get", base_url="https://example.com")
        assert poller.method == "GET"
        
        poller2 = HTTPPoller(url="https://example.com", method="post", base_url="https://example.com")
        assert poller2.method == "POST"