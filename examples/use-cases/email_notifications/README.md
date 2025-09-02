# Email Notification Worker - Production Ready Example

This example demonstrates a **production-ready email notification system** using Pythia framework. It showcases real-world patterns and practices for building reliable email workers.

## 🎯 What This Example Demonstrates

- **Queue-based email processing** with Redis Lists
- **Template rendering** with Jinja2
- **Rate limiting** by email category
- **Dead letter queue** for failed emails
- **Delivery tracking** and result logging
- **Priority handling** (high, normal, low)
- **Scheduled emails** (send later functionality)
- **SMTP integration** with mock mode for testing

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Email Queue   │───▶│  Email Worker    │───▶│   SMTP Server   │
│   (Redis List)  │    │  (Pythia)        │    │  (Gmail/etc)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Results &      │
                       │   Dead Letter    │
                       │   Queues         │
                       └──────────────────┘
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install pythia[redis] jinja2
```

### 2. Start Redis

```bash
# Using docker-compose in examples/
docker-compose up -d redis

# Or standalone Redis
docker run -d -p 6379:6379 redis:7-alpine
```

### 3. Run the Worker

```bash
python email_worker.py
```

### 4. Send Test Emails

```bash
# In another terminal
python email_worker.py generate
```

## 📧 Email Templates

The worker includes 3 built-in templates:

### Order Confirmation
```python
{
    "to_email": "customer@example.com",
    "template_name": "order_confirmation",
    "template_data": {
        "customer_name": "John Doe",
        "order_id": "ORD-12345",
        "items": [...],
        "total": "1051.97"
    }
}
```

### Shipping Notification
```python
{
    "to_email": "customer@example.com",
    "template_name": "shipping_notification",
    "template_data": {
        "tracking_number": "1Z999AA1234567890",
        "carrier": "UPS",
        "expected_delivery": "Sep 3, 2025"
    }
}
```

### Password Reset
```python
{
    "to_email": "user@example.com",
    "template_name": "password_reset",
    "template_data": {
        "user_name": "Alice",
        "reset_url": "https://example.com/reset?token=abc123"
    }
}
```

## ⚙️ Configuration

### SMTP Settings

For production, configure real SMTP:

```python
smtp_config = {
    "host": "smtp.gmail.com",
    "port": 587,
    "use_tls": True,
    "from_email": "noreply@yourcompany.com",
    "username": "your-email@gmail.com",
    "password": "your-app-password",
    "mock": False  # Enable real sending
}
```

### Rate Limiting

Emails are rate-limited by category:

```python
rate_limits = {
    "transactional": 1000,  # Order confirmations, password resets
    "marketing": 100,       # Newsletters, promotions
    "notifications": 500    # Shipping updates, alerts
}
```

## 📊 Monitoring & Observability

### Queue Monitoring

```bash
# Check queue sizes
redis-cli LLEN email-queue
redis-cli LLEN email-results
redis-cli LLEN email-dlq
```

### Worker Metrics

```bash
# Monitor with Pythia CLI
pythia monitor worker --worker email-worker --refresh 2
```

### Delivery Results

The worker publishes results to `email-results` queue:

```json
{
    "email_id": "email_1693567890123",
    "to_email": "customer@example.com",
    "status": "sent",
    "sent_at": "2025-08-31T10:30:00Z"
}
```

## 🔄 Error Handling

### Retry Logic

- **Rate limiting**: Messages are retried after delay
- **SMTP errors**: Configurable retry with exponential backoff
- **Template errors**: Sent to dead letter queue immediately

### Dead Letter Queue

Failed emails go to `email-dlq` with failure details:

```json
{
    "email_id": "email_1693567890124",
    "status": "failed",
    "error_message": "SMTP timeout",
    "retry_count": 3
}
```

## 🏭 Production Deployment

### Environment Variables

```bash
# Redis
REDIS_HOST=redis.production.com
REDIS_PORT=6379
REDIS_PASSWORD=secure-password

# SMTP
SMTP_HOST=smtp.sendgrid.net
SMTP_USERNAME=apikey
SMTP_PASSWORD=your-sendgrid-api-key

# Rate limiting
EMAIL_RATE_LIMIT_TRANSACTIONAL=2000
EMAIL_RATE_LIMIT_MARKETING=500
```

### Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY email_worker.py .

CMD ["python", "email_worker.py"]
```

### Scaling

Run multiple workers for higher throughput:

```bash
# Terminal 1
python email_worker.py

# Terminal 2
python email_worker.py

# Terminal 3
python email_worker.py
```

Redis Lists automatically distribute work between workers.

## 🧪 Testing

### Mock Mode

Default configuration uses mock SMTP for safe testing:

```python
smtp_config = {"mock": True}
```

### Load Testing

Generate high volume for testing:

```python
# Create 1000 test emails
for i in range(1000):
    email = EmailRequest(
        to_email=f"test{i}@example.com",
        template_name="order_confirmation",
        template_data={"order_id": f"ORD-{i:05d}"}
    )
    await producer.send(email.json())
```

## 🔍 Troubleshooting

### Common Issues

1. **"Connection refused to Redis"**
   ```bash
   docker-compose up -d redis
   ```

2. **"SMTP Authentication failed"**
   - Check username/password
   - Use app-specific passwords for Gmail
   - Verify SMTP host/port

3. **"Rate limit exceeded"**
   - Normal behavior - emails will be processed when limit resets
   - Adjust rate limits if needed

4. **"Template not found"**
   - Check template name spelling
   - Add custom templates to `EMAIL_TEMPLATES` dict

### Debug Mode

```bash
export PYTHIA_LOG_LEVEL=DEBUG
python email_worker.py
```

## 📚 Extension Ideas

This example can be extended with:

- **Custom templates** from database or files
- **Email tracking** with webhooks
- **A/B testing** for subject lines
- **Bounce handling** and suppression lists
- **Analytics** and delivery reporting
- **Multi-tenant** support with separate queues

## 🔗 Related Examples

- `../api_sync/` - API integration patterns
- `../batch_jobs/` - Batch processing examples
- `../../integrations/database/` - Database integration
- `../../integrations/monitoring/` - Observability setup
