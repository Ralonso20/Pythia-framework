"""
Google Cloud Pub/Sub workers for Pythia
"""

import asyncio
from typing import Any, Optional, Dict
import json

try:
    from google.cloud import pubsub_v1
    from google.auth.exceptions import GoogleAuthError
    from google.api_core.exceptions import GoogleAPIError

    HAS_GCP_PUBSUB = True
except ImportError:
    HAS_GCP_PUBSUB = False

from .base import CloudWorker, CloudConfig
from ...config.cloud import GCPConfig
from ...core.message import Message


class PubSubSubscriber(CloudWorker):
    """Worker that consumes messages from Google Cloud Pub/Sub"""

    def __init__(
        self,
        subscription_path: str,
        project_id: Optional[str] = None,
        gcp_config: Optional[GCPConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_GCP_PUBSUB:
            raise ImportError(
                "google-cloud-pubsub is required for GCP Pub/Sub support. "
                "Install with: pip install 'pythia[gcp]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.subscription_path = subscription_path
        self.project_id = project_id
        self.gcp_config = gcp_config or GCPConfig()

        # Extract project_id from subscription_path if not provided
        if not self.project_id:
            # subscription_path format: projects/{project}/subscriptions/{subscription}
            parts = subscription_path.split("/")
            if len(parts) >= 2:
                self.project_id = parts[1]
            else:
                self.project_id = self.gcp_config.project_id

        self.subscriber_client = None

    async def connect(self) -> None:
        """Initialize Pub/Sub subscriber client"""
        await super().connect()

        try:
            # Create subscriber client
            self.subscriber_client = pubsub_v1.SubscriberClient()

            # Test connection by getting subscription info
            self.subscriber_client.get_subscription(
                request={"subscription": self.subscription_path}
            )

            self.logger.info(f"Pub/Sub subscriber connected to {self.subscription_path}")

        except Exception as e:
            self.logger.error(f"Failed to connect to Pub/Sub: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Pub/Sub client"""
        if self.subscriber_client:
            self.subscriber_client.close()
            self.subscriber_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """Main processing loop for Pub/Sub messages"""
        if not self.subscriber_client:
            await self.connect()

        self.logger.info("Starting Pub/Sub message consumption")

        while True:
            try:
                # Pull messages synchronously (Google Client is sync)
                response = self.subscriber_client.pull(
                    request={
                        "subscription": self.subscription_path,
                        "max_messages": min(10, self.gcp_config.max_messages),
                    },
                    timeout=30.0,  # Timeout for long polling
                )

                if not response.received_messages:
                    # No messages, continue polling
                    await asyncio.sleep(1)
                    continue

                # Process each message
                for received_message in response.received_messages:
                    try:
                        # Convert Pub/Sub message to Pythia Message
                        message = self._convert_pubsub_message(received_message.message)

                        # Process the message
                        result = await self.process_message(message)

                        # Acknowledge message if processed successfully
                        if result is not None:  # Allow False as valid result
                            self.subscriber_client.acknowledge(
                                request={
                                    "subscription": self.subscription_path,
                                    "ack_ids": [received_message.ack_id],
                                }
                            )

                    except Exception as e:
                        self.logger.error(
                            f"Error processing Pub/Sub message: {e}",
                            message_id=received_message.message.message_id,
                            exc_info=True,
                        )
                        # Message will be redelivered if not acknowledged

            except (GoogleAuthError, GoogleAPIError) as e:
                self.logger.error(f"Google Cloud Pub/Sub error: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on GCP errors

            except Exception as e:
                self.logger.error(f"Unexpected error in Pub/Sub processing: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def process_message(self, message: Message) -> Any:
        """
        Process a single message from Pub/Sub
        Override this method in subclasses
        """
        self.logger.info(f"Processing Pub/Sub message: {message.headers.get('message_id')}")
        return {"processed": True}

    def _convert_pubsub_message(self, pubsub_message) -> Message:
        """Convert Pub/Sub message to Pythia Message"""
        # Parse data (handle JSON if possible)
        body = pubsub_message.data.decode("utf-8")
        try:
            body = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            # Keep as string if not valid JSON
            pass

        # Extract headers from message attributes and metadata
        headers = {
            "message_id": pubsub_message.message_id,
            "publish_time": pubsub_message.publish_time.isoformat()
            if pubsub_message.publish_time
            else None,
            "ordering_key": pubsub_message.ordering_key or None,
        }

        # Add message attributes as headers
        for key, value in pubsub_message.attributes.items():
            headers[f"attr_{key}"] = value

        return Message(body=body, headers=headers)


class PubSubPublisher(CloudWorker):
    """Worker that publishes messages to Google Cloud Pub/Sub"""

    def __init__(
        self,
        topic_path: str,
        project_id: Optional[str] = None,
        gcp_config: Optional[GCPConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_GCP_PUBSUB:
            raise ImportError(
                "google-cloud-pubsub is required for GCP Pub/Sub support. "
                "Install with: pip install 'pythia[gcp]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.topic_path = topic_path
        self.project_id = project_id
        self.gcp_config = gcp_config or GCPConfig()

        # Extract project_id from topic_path if not provided
        if not self.project_id:
            # topic_path format: projects/{project}/topics/{topic}
            parts = topic_path.split("/")
            if len(parts) >= 2:
                self.project_id = parts[1]
            else:
                self.project_id = self.gcp_config.project_id

        self.publisher_client = None

    async def connect(self) -> None:
        """Initialize Pub/Sub publisher client"""
        await super().connect()

        try:
            # Create publisher client
            self.publisher_client = pubsub_v1.PublisherClient()

            # Test connection by getting topic info
            self.publisher_client.get_topic(request={"topic": self.topic_path})

            self.logger.info(f"Pub/Sub publisher connected to {self.topic_path}")

        except Exception as e:
            self.logger.error(f"Failed to connect to Pub/Sub: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Pub/Sub client"""
        if self.publisher_client:
            # Wait for pending publishes to complete
            try:
                # Publisher client doesn't have explicit close method
                pass
            except Exception as e:
                self.logger.warning(f"Error during Pub/Sub publisher disconnect: {e}")
            self.publisher_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """
        Base process method - override in subclasses to define
        how messages are generated and published
        """
        self.logger.info("Pub/Sub publisher started - override process() method")

        # Base implementation just waits
        while True:
            await asyncio.sleep(1)

    async def publish_message(
        self,
        message: Any,
        attributes: Optional[Dict[str, str]] = None,
        ordering_key: Optional[str] = None,
    ) -> Optional[str]:
        """
        Publish a message to Pub/Sub topic

        Args:
            message: Message to publish (will be JSON serialized if dict/list)
            attributes: Optional message attributes
            ordering_key: Optional ordering key for message ordering

        Returns:
            Message ID if successful, None if failed
        """
        if not self.publisher_client:
            await self.connect()

        try:
            # Serialize message
            if isinstance(message, (dict, list)):
                message_data = json.dumps(message).encode("utf-8")
            else:
                message_data = str(message).encode("utf-8")

            # Prepare message attributes (must be strings)
            msg_attributes = {}
            if attributes:
                for key, value in attributes.items():
                    msg_attributes[key] = str(value)

            # Publish message - this returns a Future
            future = self.publisher_client.publish(
                topic=self.topic_path,
                data=message_data,
                ordering_key=ordering_key,
                **msg_attributes,
            )

            # Get the message ID (this blocks until publish completes)
            message_id = future.result(timeout=30.0)
            self.logger.info(f"Published Pub/Sub message: {message_id}")
            return message_id

        except (GoogleAuthError, GoogleAPIError) as e:
            self.logger.error(f"Failed to publish Pub/Sub message: {e}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error publishing message: {e}", exc_info=True)
            return None

    async def publish_from_pythia_message(
        self, message: Message, ordering_key: Optional[str] = None
    ) -> Optional[str]:
        """
        Publish a Pythia Message to Pub/Sub

        Args:
            message: Pythia message to publish
            ordering_key: Optional ordering key for message ordering

        Returns:
            Message ID if successful, None if failed
        """
        # Extract message attributes from headers
        attributes = {}
        if message.headers:
            for key, value in message.headers.items():
                if not key.startswith("_"):  # Skip internal headers
                    attributes[key] = str(value)

        return await self.publish_message(
            message=message.body, attributes=attributes, ordering_key=ordering_key
        )
