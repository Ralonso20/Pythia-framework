"""
Webhook Sender Worker - sends HTTP webhooks with retry logic
"""

import asyncio
from typing import Any, Dict, Optional, List

from .base import HTTPWorker
from ...http import WebhookClient, HTTPClientConfig
from ...config.http import WebhookConfig

# Import Message locally to avoid circular imports
from ...core.message import Message


class WebhookSenderWorker(HTTPWorker):
    """Worker that sends HTTP webhooks with retry logic"""

    def __init__(
        self,
        base_url: Optional[str] = None,
        webhook_config: Optional[WebhookConfig] = None,
        http_config: Optional[HTTPClientConfig] = None,
        **kwargs,
    ):
        """
        Initialize webhook sender worker

        Args:
            base_url: Base URL for webhook endpoints
            webhook_config: Webhook-specific configuration
            http_config: HTTP client configuration
        """
        super().__init__(http_config=http_config, **kwargs)

        self.base_url = base_url
        self.webhook_config = webhook_config

        # Initialize webhook client
        self.webhook_client = WebhookClient(
            base_url=base_url,
            config=webhook_config,
        )

    async def connect(self) -> None:
        """Connect webhook client and HTTP client"""
        await super().connect()
        await self.webhook_client.connect()
        self.logger.info("Webhook sender connected")

    async def disconnect(self) -> None:
        """Disconnect webhook client and HTTP client"""
        await self.webhook_client.disconnect()
        await super().disconnect()
        self.logger.info("Webhook sender disconnected")

    async def process(self) -> Any:
        """
        Main processing loop - override this to define your webhook sending logic

        This base implementation does nothing - subclasses should override
        to define how they consume messages and send webhooks
        """
        self.logger.info("WebhookSenderWorker started - override process() method")

        # Base implementation - just wait
        while True:
            await asyncio.sleep(1)

    async def send_webhook(
        self,
        endpoint: str,
        data: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        method: str = "POST",
        **kwargs,
    ) -> bool:
        """
        Send a webhook to the specified endpoint

        Args:
            endpoint: Webhook endpoint (relative to base_url or absolute URL)
            data: Data to send in webhook
            headers: Additional headers
            method: HTTP method (POST, PUT, etc.)

        Returns:
            True if webhook was sent successfully, False otherwise
        """
        try:
            success = await self.webhook_client.send(
                endpoint=endpoint,
                data=data,
                headers=headers,
                method=method,
                **kwargs,
            )

            if success:
                self.logger.info(
                    "Webhook sent successfully",
                    endpoint=endpoint,
                    method=method,
                    data_keys=list(data.keys()) if isinstance(data, dict) else None,
                )
            else:
                self.logger.error(
                    "Failed to send webhook",
                    endpoint=endpoint,
                    method=method,
                )

            return success

        except Exception as e:
            self.logger.error(
                "Error sending webhook",
                endpoint=endpoint,
                method=method,
                error=str(e),
                exc_info=True,
            )
            return False

    async def send_webhook_from_message(
        self,
        message: Message,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
        method: str = "POST",
        data_transformer: Optional[callable] = None,
        **kwargs,
    ) -> bool:
        """
        Send webhook using data from a Pythia message

        Args:
            message: Pythia message containing data to send
            endpoint: Webhook endpoint
            headers: Additional headers (merged with message headers)
            method: HTTP method
            data_transformer: Optional function to transform message.body before sending

        Returns:
            True if webhook was sent successfully, False otherwise
        """
        # Transform data if transformer provided
        data = message.body
        if data_transformer and callable(data_transformer):
            try:
                data = data_transformer(data)
            except Exception as e:
                self.logger.error(f"Error transforming webhook data: {e}", exc_info=True)
                return False

        # Merge headers
        merged_headers = {}
        if message.headers:
            merged_headers.update(message.headers)
        if headers:
            merged_headers.update(headers)

        # Send webhook
        return await self.send_webhook(
            endpoint=endpoint,
            data=data,
            headers=merged_headers,
            method=method,
            **kwargs,
        )

    async def broadcast_webhook(
        self,
        endpoints: List[str],
        data: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        method: str = "POST",
        fail_fast: bool = False,
        **kwargs,
    ) -> Dict[str, bool]:
        """
        Send the same webhook to multiple endpoints

        Args:
            endpoints: List of webhook endpoints
            data: Data to send to all endpoints
            headers: Headers for all requests
            method: HTTP method for all requests
            fail_fast: If True, stop on first failure

        Returns:
            Dict mapping endpoint to success/failure status
        """
        results = {}

        self.logger.info(f"Broadcasting webhook to {len(endpoints)} endpoints")

        for endpoint in endpoints:
            success = await self.send_webhook(
                endpoint=endpoint,
                data=data,
                headers=headers,
                method=method,
                **kwargs,
            )

            results[endpoint] = success

            if fail_fast and not success:
                self.logger.warning(f"Broadcast failed fast at endpoint: {endpoint}")
                break

        success_count = sum(results.values())
        self.logger.info(
            f"Webhook broadcast completed: {success_count}/{len(endpoints)} successful",
            results=results,
        )

        return results
