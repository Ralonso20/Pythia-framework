"""
AWS SQS/SNS workers for Pythia
"""

import asyncio
from typing import Any, Optional, Dict
import json

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

from .base import CloudWorker, CloudConfig
from ...config.cloud import AWSConfig
from ...core.message import Message


class SQSConsumer(CloudWorker):
    """Worker that consumes messages from AWS SQS"""

    def __init__(
        self,
        queue_url: str,
        aws_config: Optional[AWSConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_BOTO3:
            raise ImportError(
                "boto3 is required for AWS SQS support. Install with: pip install 'pythia[aws]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.queue_url = queue_url
        self.aws_config = aws_config or AWSConfig()
        self.sqs_client = None

    async def connect(self) -> None:
        """Initialize SQS client"""
        await super().connect()

        # Create boto3 session and client
        session = boto3.Session(
            aws_access_key_id=self.aws_config.access_key_id,
            aws_secret_access_key=self.aws_config.secret_access_key,
            region_name=self.aws_config.region,
        )

        self.sqs_client = session.client(
            "sqs",
            endpoint_url=self.aws_config.endpoint_url,  # For LocalStack testing
        )

        self.logger.info(f"SQS consumer connected to {self.queue_url}")

    async def disconnect(self) -> None:
        """Close SQS client"""
        if self.sqs_client:
            # boto3 clients don't need explicit closing
            self.sqs_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """Main processing loop for SQS messages"""
        if not self.sqs_client:
            await self.connect()

        self.logger.info("Starting SQS message consumption")

        while True:
            try:
                # Long poll for messages
                response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=self.aws_config.max_messages,
                    WaitTimeSeconds=self.aws_config.wait_time_seconds,
                    VisibilityTimeout=self.aws_config.visibility_timeout,
                )

                messages = response.get("Messages", [])
                if not messages:
                    # No messages, continue polling
                    await asyncio.sleep(1)
                    continue

                # Process each message
                for sqs_message in messages:
                    try:
                        # Convert SQS message to Pythia Message
                        message = self._convert_sqs_message(sqs_message)

                        # Process the message
                        result = await self.process_message(message)

                        # Delete message from queue if processed successfully
                        if result is not None:  # Allow False as valid result
                            await self._delete_message(sqs_message["ReceiptHandle"])

                    except Exception as e:
                        self.logger.error(
                            f"Error processing SQS message: {e}",
                            message_id=sqs_message.get("MessageId"),
                            exc_info=True,
                        )
                        # Message will become visible again after visibility timeout

            except (BotoCoreError, ClientError) as e:
                self.logger.error(f"AWS SQS error: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on AWS errors

            except Exception as e:
                self.logger.error(f"Unexpected error in SQS processing: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def process_message(self, message: Message) -> Any:
        """
        Process a single message from SQS
        Override this method in subclasses
        """
        self.logger.info(f"Processing SQS message: {message.headers.get('MessageId')}")
        return {"processed": True}

    def _convert_sqs_message(self, sqs_message: Dict) -> Message:
        """Convert SQS message to Pythia Message"""
        # Parse body (handle JSON if possible)
        body = sqs_message["Body"]
        try:
            body = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            # Keep as string if not valid JSON
            pass

        # Extract headers
        headers = {
            "MessageId": sqs_message.get("MessageId"),
            "ReceiptHandle": sqs_message.get("ReceiptHandle"),
            "MD5OfBody": sqs_message.get("MD5OfBody"),
        }

        # Add message attributes as headers
        attributes = sqs_message.get("MessageAttributes", {})
        for name, attr in attributes.items():
            headers[f"attr_{name}"] = attr.get("StringValue", attr.get("BinaryValue"))

        return Message(body=body, headers=headers)

    async def _delete_message(self, receipt_handle: str):
        """Delete processed message from SQS"""
        try:
            self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except (BotoCoreError, ClientError) as e:
            self.logger.error(f"Failed to delete SQS message: {e}")


class SNSProducer(CloudWorker):
    """Worker that publishes messages to AWS SNS"""

    def __init__(
        self,
        topic_arn: str,
        aws_config: Optional[AWSConfig] = None,
        cloud_config: Optional[CloudConfig] = None,
        **kwargs,
    ):
        if not HAS_BOTO3:
            raise ImportError(
                "boto3 is required for AWS SNS support. Install with: pip install 'pythia[aws]'"
            )

        super().__init__(cloud_config=cloud_config, **kwargs)
        self.topic_arn = topic_arn
        self.aws_config = aws_config or AWSConfig()
        self.sns_client = None

    async def connect(self) -> None:
        """Initialize SNS client"""
        await super().connect()

        # Create boto3 session and client
        session = boto3.Session(
            aws_access_key_id=self.aws_config.access_key_id,
            aws_secret_access_key=self.aws_config.secret_access_key,
            region_name=self.aws_config.region,
        )

        self.sns_client = session.client(
            "sns",
            endpoint_url=self.aws_config.endpoint_url,  # For LocalStack testing
        )

        self.logger.info(f"SNS producer connected to {self.topic_arn}")

    async def disconnect(self) -> None:
        """Close SNS client"""
        if self.sns_client:
            # boto3 clients don't need explicit closing
            self.sns_client = None
        await super().disconnect()

    async def process(self) -> Any:
        """
        Base process method - override in subclasses to define
        how messages are generated and published
        """
        self.logger.info("SNS producer started - override process() method")

        # Base implementation just waits
        while True:
            await asyncio.sleep(1)

    async def publish_message(
        self,
        message: Any,
        subject: Optional[str] = None,
        message_attributes: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Publish a message to SNS topic

        Args:
            message: Message to publish (will be JSON serialized if dict/list)
            subject: Optional subject for the message
            message_attributes: Optional message attributes

        Returns:
            Message ID if successful, None if failed
        """
        if not self.sns_client:
            await self.connect()

        try:
            # Serialize message
            if isinstance(message, (dict, list)):
                message_body = json.dumps(message)
            else:
                message_body = str(message)

            # Prepare message attributes
            attrs = {}
            if message_attributes:
                for key, value in message_attributes.items():
                    attrs[key] = {"DataType": "String", "StringValue": str(value)}

            # Publish message
            response = self.sns_client.publish(
                TopicArn=self.topic_arn,
                Message=message_body,
                Subject=subject,
                MessageAttributes=attrs,
            )

            message_id = response.get("MessageId")
            self.logger.info(f"Published SNS message: {message_id}")
            return message_id

        except (BotoCoreError, ClientError) as e:
            self.logger.error(f"Failed to publish SNS message: {e}", exc_info=True)
            return None

    async def publish_from_pythia_message(
        self, message: Message, subject: Optional[str] = None
    ) -> Optional[str]:
        """
        Publish a Pythia Message to SNS

        Args:
            message: Pythia message to publish
            subject: Optional subject for the message

        Returns:
            Message ID if successful, None if failed
        """
        # Extract message attributes from headers
        message_attributes = {}
        if message.headers:
            for key, value in message.headers.items():
                if not key.startswith("_"):  # Skip internal headers
                    message_attributes[key] = value

        return await self.publish_message(
            message=message.body, subject=subject, message_attributes=message_attributes
        )
