#!/usr/bin/env python3
"""
Email Notification Worker - Real World Use Case

This example demonstrates a production-ready email notification system:
1. Processing email requests from multiple sources
2. Template rendering with dynamic data
3. Rate limiting and queue management
4. Delivery tracking and retry logic
5. Dead letter queue for failed emails

Use Case: E-commerce platform sending transactional emails
- Order confirmations
- Shipping notifications
- Password resets
- Marketing newsletters (rate-limited)

Requirements:
- pip install pythia[redis] jinja2
- Redis running for queue management
- SMTP server configuration (Gmail, SendGrid, etc.)
"""

import asyncio
import json
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional
from pydantic import BaseModel, EmailStr
from datetime import datetime
from jinja2 import Template

from pythia import Worker
from pythia.brokers.redis import RedisListConsumer, RedisListProducer
from pythia.config.redis import RedisConfig


# Email schemas
class EmailRequest(BaseModel):
    """Email request schema"""

    to_email: EmailStr
    subject: str
    template_name: str
    template_data: Dict[str, Any] = {}
    priority: str = "normal"  # high, normal, low
    category: str = "transactional"  # transactional, marketing
    user_id: Optional[str] = None
    scheduled_at: Optional[str] = None  # ISO format


class EmailResult(BaseModel):
    """Email delivery result"""

    email_id: str
    to_email: str
    status: str  # sent, failed, retrying
    sent_at: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0


# Email templates
EMAIL_TEMPLATES = {
    "order_confirmation": {
        "subject": "Order Confirmation - #{{ order_id }}",
        "html": """
        <h2>Thank you for your order!</h2>
        <p>Dear {{ customer_name }},</p>
        <p>Your order <strong>#{{ order_id }}</strong> has been confirmed.</p>

        <h3>Order Details:</h3>
        <ul>
        {% for item in items %}
            <li>{{ item.name }} x {{ item.quantity }} - ${{ item.price }}</li>
        {% endfor %}
        </ul>

        <p><strong>Total: ${{ total }}</strong></p>
        <p>Estimated delivery: {{ delivery_date }}</p>

        <p>Thank you for shopping with us!</p>
        """,
    },
    "shipping_notification": {
        "subject": "Your order is on the way! - #{{ order_id }}",
        "html": """
        <h2>Your order has shipped!</h2>
        <p>Dear {{ customer_name }},</p>
        <p>Good news! Your order <strong>#{{ order_id }}</strong> has been shipped.</p>

        <p><strong>Tracking Number:</strong> {{ tracking_number }}</p>
        <p><strong>Carrier:</strong> {{ carrier }}</p>
        <p><strong>Expected Delivery:</strong> {{ expected_delivery }}</p>

        <p>Track your package: <a href="{{ tracking_url }}">{{ tracking_url }}</a></p>
        """,
    },
    "password_reset": {
        "subject": "Reset your password",
        "html": """
        <h2>Password Reset Request</h2>
        <p>Dear {{ user_name }},</p>
        <p>You requested to reset your password. Click the link below to set a new password:</p>

        <p><a href="{{ reset_url }}" style="background: #007cba; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Reset Password</a></p>

        <p>This link expires in 24 hours.</p>
        <p>If you didn't request this, please ignore this email.</p>
        """,
    },
}


class EmailNotificationWorker(Worker):
    """
    Production-ready email notification worker
    """

    def __init__(self, smtp_config: Dict[str, Any]):
        # Configure Redis queue consumer
        self.source = RedisListConsumer(
            queue="email-queue",
            config=RedisConfig(
                host="localhost",
                port=6379,
                db=0,
                batch_size=5,  # Process 5 emails at a time
                block_timeout_ms=5000,  # Block for 5 seconds
            ),
        )

        # Producer for results tracking
        self.results_producer = RedisListProducer(
            queue="email-results", config=RedisConfig(host="localhost", port=6379, db=0)
        )

        # Dead letter queue for failed emails
        self.dlq_producer = RedisListProducer(
            queue="email-dlq", config=RedisConfig(host="localhost", port=6379, db=0)
        )

        # SMTP configuration
        self.smtp_config = smtp_config

        # Rate limiting (emails per minute by category)
        self.rate_limits = {
            "transactional": 1000,  # High limit for important emails
            "marketing": 100,  # Rate limited for marketing
            "notifications": 500,  # Medium limit for notifications
        }

        # Track sent emails for rate limiting
        self.sent_counts = {}

        super().__init__()

    async def process(self, message) -> None:
        """Process email request"""
        try:
            # Parse email request
            email_data = json.loads(message.body)
            email_request = EmailRequest(**email_data)

            email_id = f"email_{int(time.time() * 1000)}"

            self.logger.info(
                f"Processing email: {email_request.subject}",
                extra={
                    "email_id": email_id,
                    "to_email": email_request.to_email,
                    "template": email_request.template_name,
                    "priority": email_request.priority,
                    "category": email_request.category,
                },
            )

            # Check if scheduled email
            if email_request.scheduled_at:
                scheduled_time = datetime.fromisoformat(email_request.scheduled_at)
                if datetime.utcnow() < scheduled_time:
                    self.logger.info(f"Email scheduled for later: {scheduled_time}")
                    # Re-queue for later (in production, use proper scheduling)
                    await asyncio.sleep(1)
                    raise Exception("Email not ready to send")

            # Rate limiting check
            if not await self._check_rate_limit(email_request.category):
                self.logger.warning(f"Rate limit exceeded for {email_request.category}")
                # Re-queue for later
                await asyncio.sleep(60)  # Wait 1 minute
                raise Exception("Rate limit exceeded")

            # Render email template
            html_content = await self._render_template(
                email_request.template_name, email_request.template_data
            )

            subject = await self._render_subject(
                email_request.template_name, email_request.template_data
            )

            # Send email
            await self._send_email(
                to_email=email_request.to_email,
                subject=subject,
                html_content=html_content,
                email_id=email_id,
            )

            # Record successful delivery
            result = EmailResult(
                email_id=email_id,
                to_email=email_request.to_email,
                status="sent",
                sent_at=datetime.utcnow().isoformat(),
            )

            await self.results_producer.send(result.json())

            # Update rate limiting
            await self._update_rate_limit(email_request.category)

            self.logger.info(f"✅ Email sent successfully: {email_id}")

        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")

            # Create failure record
            result = EmailResult(
                email_id=email_id if "email_id" in locals() else "unknown",
                to_email=email_request.to_email if "email_request" in locals() else "unknown",
                status="failed",
                error_message=str(e),
                retry_count=1,
            )

            # Send to dead letter queue for retry/analysis
            await self.dlq_producer.send(result.json())

            raise  # Re-raise for worker retry logic

    async def _check_rate_limit(self, category: str) -> bool:
        """Check if we can send email based on rate limits"""
        current_minute = int(time.time() // 60)
        key = f"{category}_{current_minute}"

        current_count = self.sent_counts.get(key, 0)
        limit = self.rate_limits.get(category, 100)

        return current_count < limit

    async def _update_rate_limit(self, category: str):
        """Update rate limiting counters"""
        current_minute = int(time.time() // 60)
        key = f"{category}_{current_minute}"

        self.sent_counts[key] = self.sent_counts.get(key, 0) + 1

        # Clean up old counters (keep last 5 minutes)
        old_keys = [
            k for k in self.sent_counts.keys() if int(k.split("_")[-1]) < current_minute - 5
        ]
        for old_key in old_keys:
            del self.sent_counts[old_key]

    async def _render_template(self, template_name: str, data: Dict[str, Any]) -> str:
        """Render HTML email template"""
        if template_name not in EMAIL_TEMPLATES:
            raise ValueError(f"Unknown template: {template_name}")

        template_config = EMAIL_TEMPLATES[template_name]
        template = Template(template_config["html"])

        return template.render(**data)

    async def _render_subject(self, template_name: str, data: Dict[str, Any]) -> str:
        """Render email subject"""
        if template_name not in EMAIL_TEMPLATES:
            raise ValueError(f"Unknown template: {template_name}")

        template_config = EMAIL_TEMPLATES[template_name]
        template = Template(template_config["subject"])

        return template.render(**data)

    async def _send_email(self, to_email: str, subject: str, html_content: str, email_id: str):
        """Send email via SMTP"""
        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.smtp_config["from_email"]
            msg["To"] = to_email
            msg["X-Email-ID"] = email_id  # Custom header for tracking

            # Attach HTML content
            html_part = MIMEText(html_content, "html")
            msg.attach(html_part)

            # Send via SMTP
            if self.smtp_config.get("mock", False):
                # Mock mode for testing
                await asyncio.sleep(0.1)  # Simulate network delay
                self.logger.debug(f"📧 [MOCK] Email sent to {to_email}: {subject}")
            else:
                # Real SMTP sending
                with smtplib.SMTP(self.smtp_config["host"], self.smtp_config["port"]) as server:
                    if self.smtp_config.get("use_tls"):
                        server.starttls()

                    if self.smtp_config.get("username"):
                        server.login(self.smtp_config["username"], self.smtp_config["password"])

                    server.send_message(msg)

        except Exception as e:
            self.logger.error(f"SMTP error: {e}")
            raise

    async def on_startup(self):
        """Worker startup"""
        self.logger.info("📧 Email Notification Worker starting...")
        self.logger.info(f"📥 Email queue: {self.source.queue}")
        self.logger.info(f"📊 Rate limits: {self.rate_limits}")
        self.logger.info(f"📤 SMTP: {self.smtp_config.get('host', 'mock')}")

    async def on_shutdown(self):
        """Worker shutdown"""
        self.logger.info("🛑 Email Notification Worker shutting down...")
        if self.sent_counts:
            total_sent = sum(self.sent_counts.values())
            self.logger.info(f"📊 Total emails sent this session: {total_sent}")


# Test email generator
async def generate_test_emails():
    """Generate test email requests"""
    producer = RedisListProducer(
        queue="email-queue", config=RedisConfig(host="localhost", port=6379, db=0)
    )

    await producer.connect()

    test_emails = [
        # Order confirmation
        EmailRequest(
            to_email="customer@example.com",
            subject="Order Confirmation",
            template_name="order_confirmation",
            category="transactional",
            template_data={
                "customer_name": "John Doe",
                "order_id": "ORD-12345",
                "items": [
                    {"name": "Laptop", "quantity": 1, "price": "999.99"},
                    {"name": "Mouse", "quantity": 2, "price": "25.99"},
                ],
                "total": "1051.97",
                "delivery_date": "September 5, 2025",
            },
        ),
        # Shipping notification
        EmailRequest(
            to_email="customer@example.com",
            subject="Shipping Notification",
            template_name="shipping_notification",
            category="notifications",
            template_data={
                "customer_name": "Jane Smith",
                "order_id": "ORD-12346",
                "tracking_number": "1Z999AA1234567890",
                "carrier": "UPS",
                "expected_delivery": "September 3, 2025",
                "tracking_url": "https://ups.com/track?id=1Z999AA1234567890",
            },
        ),
        # Password reset
        EmailRequest(
            to_email="user@example.com",
            subject="Password Reset",
            template_name="password_reset",
            category="transactional",
            priority="high",
            template_data={
                "user_name": "Alice Johnson",
                "reset_url": "https://example.com/reset-password?token=abc123",
            },
        ),
    ]

    for email in test_emails:
        await producer.send(email.json())
        print(f"📧 Queued: {email.template_name} to {email.to_email}")
        await asyncio.sleep(0.5)

    await producer.disconnect()
    print("✅ Test emails queued")


if __name__ == "__main__":
    import sys

    # SMTP configuration (mock by default)
    smtp_config = {
        "host": "smtp.gmail.com",
        "port": 587,
        "use_tls": True,
        "from_email": "noreply@example.com",
        "username": None,  # Set for real SMTP
        "password": None,  # Set for real SMTP
        "mock": True,  # Set to False for real email sending
    }

    if len(sys.argv) > 1 and sys.argv[1] == "generate":
        print("📧 Generating test emails...")
        asyncio.run(generate_test_emails())
    else:
        print("🚀 Starting Email Notification Worker...")
        print("💡 Run with 'generate' to create test emails:")
        print("   python email_worker.py generate")

        worker = EmailNotificationWorker(smtp_config)
        worker.run_sync()
