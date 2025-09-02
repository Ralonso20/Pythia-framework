# Migrating from Celery to Pythia

This guide helps you migrate from Celery to Pythia smoothly. While both frameworks handle background task processing, Pythia offers modern async/await support, better broker flexibility, and simplified configuration.

## 🔄 Quick Comparison

| Feature | Celery | Pythia |
|---------|--------|---------|
| **Language** | Python (sync + async) | Python (async-first) |
| **Brokers** | Redis, RabbitMQ, SQS | Redis, RabbitMQ, Kafka + more |
| **Configuration** | Complex settings | Environment + code |
| **Type Safety** | Basic | Full Pydantic integration |
| **CLI Tools** | Basic | Rich monitoring & generation |
| **Testing** | Manual setup | Built-in test utilities |

## 🚀 Migration Steps

### Step 1: Understand the Mapping

#### Celery Task → Pythia Worker

**Celery:**
```python
from celery import Celery

app = Celery('tasks', broker='redis://localhost:6379')

@app.task
def send_email(email, subject, body):
    # Send email logic
    return f"Email sent to {email}"
```

**Pythia:**
```python
from pythia import Worker
from pythia.brokers.redis import RedisListConsumer
import json

class EmailWorker(Worker):
    def __init__(self):
        self.source = RedisListConsumer(queue="email-tasks")
        super().__init__()

    async def process(self, message):
        data = json.loads(message.body)
        # Send email logic (now async!)
        await send_email_async(data['email'], data['subject'], data['body'])
        return f"Email sent to {data['email']}"
```

### Step 2: Task Calling → Message Sending

#### Calling Tasks

**Celery:**
```python
# Immediate execution
result = send_email.delay('user@example.com', 'Hello', 'World')

# Get result
print(result.get())
```

**Pythia:**
```python
from pythia.brokers.redis import RedisListProducer
import json

# Send message to queue
producer = RedisListProducer(queue="email-tasks")
await producer.connect()

message = {
    'email': 'user@example.com',
    'subject': 'Hello',
    'body': 'World'
}

await producer.send(json.dumps(message))
await producer.disconnect()
```

### Step 3: Configuration Migration

#### Broker Configuration

**Celery (`celeryconfig.py`):**
```python
broker_url = 'redis://localhost:6379/0'
result_backend = 'redis://localhost:6379/0'

task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'
timezone = 'UTC'

worker_prefetch_multiplier = 1
task_acks_late = True
```

**Pythia (environment variables):**
```bash
# .env file
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

PYTHIA_LOG_LEVEL=INFO
PYTHIA_WORKER_NAME=email-worker
```

**Or in code:**
```python
from pythia.config.redis import RedisConfig

config = RedisConfig(
    host="localhost",
    port=6379,
    db=0
)
```

### Step 4: Worker Execution

#### Running Workers

**Celery:**
```bash
celery -A tasks worker --loglevel=info
```

**Pythia:**
```bash
# Direct execution
python email_worker.py

# Or using CLI
pythia run email_worker.py

# With monitoring
pythia run email_worker.py --monitor
```

## 📋 Common Migration Patterns

### 1. Simple Task Migration

**Before (Celery):**
```python
@app.task
def process_order(order_id):
    order = Order.objects.get(id=order_id)
    order.status = 'processing'
    order.save()

    # Process order
    result = process_payment(order)

    order.status = 'completed'
    order.save()

    return {'order_id': order_id, 'status': 'completed'}
```

**After (Pythia):**
```python
class OrderProcessor(Worker):
    def __init__(self):
        self.source = RedisListConsumer(queue="order-processing")
        super().__init__()

    async def process(self, message):
        data = json.loads(message.body)
        order_id = data['order_id']

        # Same logic but async
        order = await Order.objects.aget(id=order_id)
        order.status = 'processing'
        await order.asave()

        result = await process_payment_async(order)

        order.status = 'completed'
        await order.asave()

        return {'order_id': order_id, 'status': 'completed'}
```

### 2. Periodic Tasks

**Before (Celery Beat):**
```python
from celery.schedules import crontab

@app.task
def cleanup_old_files():
    # Cleanup logic
    pass

app.conf.beat_schedule = {
    'cleanup-every-day': {
        'task': 'cleanup_old_files',
        'schedule': crontab(hour=2, minute=0),
    },
}
```

**After (Pythia Scheduled Worker):**
```python
from pythia.jobs import ScheduledWorker

class CleanupWorker(ScheduledWorker):
    schedule = "0 2 * * *"  # Same cron format

    async def process(self):
        # Same cleanup logic but async
        await cleanup_old_files_async()
```

### 3. Task Chains/Groups

**Before (Celery):**
```python
from celery import chain, group

# Chain: task1 -> task2 -> task3
workflow = chain(
    process_data.s(data),
    transform_data.s(),
    save_result.s()
)
result = workflow.apply_async()
```

**After (Pythia):**
```python
class WorkflowWorker(Worker):
    def __init__(self):
        self.source = RedisListConsumer(queue="workflow-tasks")
        self.output = RedisListProducer(queue="next-step")
        super().__init__()

    async def process(self, message):
        data = json.loads(message.body)

        # Process step by step
        step1_result = await self.process_data(data)
        step2_result = await self.transform_data(step1_result)
        final_result = await self.save_result(step2_result)

        # Or pipeline through multiple workers
        await self.output.send(json.dumps(step1_result))
```

### 4. Error Handling & Retries

**Before (Celery):**
```python
@app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def unreliable_task(self, data):
    try:
        # Risky operation
        return api_call(data)
    except TemporaryError as exc:
        raise self.retry(countdown=60, exc=exc)
```

**After (Pythia):**
```python
class ReliableWorker(Worker):
    def __init__(self):
        self.source = RedisListConsumer(queue="risky-tasks")
        super().__init__()

    async def process(self, message):
        data = json.loads(message.body)

        try:
            # Same risky operation
            return await api_call_async(data)
        except TemporaryError as e:
            self.logger.warning(f"Temporary failure: {e}")
            raise  # Worker handles retry automatically
        except PermanentError as e:
            self.logger.error(f"Permanent failure: {e}")
            return  # Don't retry
```

## ⚡ Benefits After Migration

### 1. **Native Async Support**
```python
# Celery (blocking)
def send_emails(email_list):
    for email in email_list:
        send_email(email)  # Blocks

# Pythia (concurrent)
async def send_emails(email_list):
    tasks = [send_email_async(email) for email in email_list]
    await asyncio.gather(*tasks)  # Concurrent!
```

### 2. **Better Type Safety**
```python
from pydantic import BaseModel

class OrderData(BaseModel):
    order_id: str
    customer_id: str
    amount: float

async def process(self, message):
    # Automatic validation!
    data = OrderData.parse_raw(message.body)
```

### 3. **Built-in Monitoring**
```bash
# Real-time monitoring
pythia monitor worker --worker order-processor

# Broker health
pythia monitor broker --broker-type redis
```

### 4. **Multiple Broker Support**
```python
# Easy to switch brokers
class MyWorker(Worker):
    def __init__(self):
        # Switch from Redis to Kafka easily
        # self.source = RedisListConsumer(queue="tasks")
        self.source = KafkaConsumer(topics=["tasks"])
```

## 📊 Performance Comparison

### Throughput
- **Celery**: ~500-1000 tasks/second (depends on task)
- **Pythia**: ~2000-5000 messages/second (async advantage)

### Memory Usage
- **Celery**: Higher due to process model
- **Pythia**: Lower with async single-process model

### Latency
- **Celery**: Higher (process creation overhead)
- **Pythia**: Lower (async event loop)

## 🛠️ Migration Tools

### 1. Celery Task Converter
```python
# Tool to convert Celery tasks to Pythia workers
def convert_celery_task(celery_func):
    class ConvertedWorker(Worker):
        def __init__(self):
            queue_name = f"{celery_func.__name__}-queue"
            self.source = RedisListConsumer(queue=queue_name)
            super().__init__()

        async def process(self, message):
            data = json.loads(message.body)
            # Convert sync function to async
            return await asyncio.to_thread(celery_func, **data)

    return ConvertedWorker
```

### 2. Message Bridge
```python
# Bridge to slowly migrate - run both systems
class CeleryBridge(Worker):
    """Forward Pythia messages to Celery tasks"""

    def __init__(self):
        self.source = RedisListConsumer(queue="bridge-tasks")
        super().__init__()

    async def process(self, message):
        data = json.loads(message.body)
        task_name = data.pop('task_name')

        # Send to Celery
        celery_app.send_task(task_name, kwargs=data)
```

## 📋 Migration Checklist

### Pre-Migration
- [ ] **Inventory tasks** - List all Celery tasks
- [ ] **Identify dependencies** - Database, APIs, etc.
- [ ] **Review task patterns** - Simple, periodic, chains
- [ ] **Check error handling** - Retry logic, dead letters
- [ ] **Document configuration** - Broker settings, queues

### During Migration
- [ ] **Set up Pythia environment** - Install, configure
- [ ] **Create worker classes** - Convert tasks to workers
- [ ] **Test in staging** - Verify functionality
- [ ] **Set up monitoring** - Logs, metrics, health checks
- [ ] **Update deployment** - Docker, systemd, etc.

### Post-Migration
- [ ] **Monitor performance** - Compare throughput, errors
- [ ] **Update documentation** - New patterns, deployment
- [ ] **Train team** - Pythia concepts, debugging
- [ ] **Optimize** - Tune batch sizes, concurrency
- [ ] **Remove old code** - Clean up Celery tasks

## 🚧 Common Gotchas

### 1. **Async/Await Everywhere**
```python
# Wrong - mixing sync/async
async def process(self, message):
    data = json.loads(message.body)
    result = some_sync_function(data)  # This blocks the event loop!

# Right - async all the way
async def process(self, message):
    data = json.loads(message.body)
    result = await some_async_function(data)
```

### 2. **Message Format Changes**
```python
# Celery automatically serializes arguments
send_email.delay('user@example.com', 'subject', 'body')

# Pythia needs explicit JSON
message = {
    'email': 'user@example.com',
    'subject': 'subject',
    'body': 'body'
}
await producer.send(json.dumps(message))
```

### 3. **Single Process vs Multi-Process**
- **Celery**: Multiple processes, shared nothing
- **Pythia**: Single process, shared memory (be careful with globals)

## 📚 Next Steps

1. **Start small** - Migrate one simple task first
2. **Run in parallel** - Keep Celery running while testing Pythia
3. **Monitor closely** - Compare performance and error rates
4. **Gradually migrate** - Move tasks one by one
5. **Optimize** - Tune Pythia configuration based on usage

## 🔗 Resources

- [Pythia Examples](../../examples/) - Real-world patterns
- [API Reference](../api/) - Complete API documentation
- [Performance Guide](../performance/) - Optimization tips
- [Deployment Guide](../deployment/) - Production setup

## 🤝 Need Help?

- **Migration support** - Open an issue with your specific use case
- **Performance questions** - Share your benchmarks
- **Custom patterns** - We can help design Pythia equivalents

Happy migrating! The async future awaits! 🚀
