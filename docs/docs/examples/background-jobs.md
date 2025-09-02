# Background Job Workers

Complete guide to implementing background job processing with Pythia's job system.

## Overview

Background job workers handle asynchronous task processing, scheduled jobs, and long-running operations outside of the main request/response cycle. Pythia's job system provides priority queues, retry logic, and comprehensive job management.

## Basic Job Worker

### Simple Email Job Processor

```python
import asyncio
import smtplib
from email.mime.text import MIMEText
from typing import Dict, Any
from pythia.jobs import BackgroundJobWorker, JobProcessor, Job, JobResult
from pythia.jobs.queue import MemoryJobQueue
from pythia.config import WorkerConfig

class EmailJobProcessor(JobProcessor):
    """Process email sending jobs"""

    def __init__(self, smtp_config: Dict[str, str]):
        self.smtp_config = smtp_config

    async def process(self, job: Job) -> JobResult:
        """Process email job"""
        try:
            # Extract email parameters from job
            to_email = job.kwargs.get('to')
            subject = job.kwargs.get('subject')
            body = job.kwargs.get('body')

            if not all([to_email, subject, body]):
                return JobResult(
                    success=False,
                    error="Missing required email parameters"
                )

            # Send email
            await self._send_email(to_email, subject, body)

            return JobResult(
                success=True,
                result={
                    "email_sent": True,
                    "recipient": to_email,
                    "subject": subject
                }
            )

        except Exception as e:
            return JobResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__
            )

    async def _send_email(self, to_email: str, subject: str, body: str):
        """Send email via SMTP"""
        # Create message
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.smtp_config['from_email']
        msg['To'] = to_email

        # Send via SMTP (in production, use aiosmtplib for async)
        # For demo purposes, we'll simulate the email send
        await asyncio.sleep(0.5)  # Simulate network delay

        print(f"📧 Email sent to {to_email}")
        print(f"   Subject: {subject}")
        print(f"   Body: {body[:100]}...")

# Usage example
async def run_email_jobs():
    # Create SMTP configuration
    smtp_config = {
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'from_email': 'noreply@example.com',
        'username': 'your_email@gmail.com',
        'password': 'your_password'
    }

    # Create job processor
    processor = EmailJobProcessor(smtp_config)

    # Create job worker
    worker = BackgroundJobWorker(
        queue=MemoryJobQueue(),
        processor=processor,
        max_concurrent_jobs=5,
        polling_interval=1.0
    )

    # Submit email jobs
    jobs = [
        {
            'name': 'welcome_email',
            'func': 'send_email',
            'kwargs': {
                'to': 'new_user@example.com',
                'subject': 'Welcome to Our Platform!',
                'body': 'Thank you for joining us. Get started with your first project.'
            }
        },
        {
            'name': 'password_reset',
            'func': 'send_email',
            'kwargs': {
                'to': 'user@example.com',
                'subject': 'Password Reset Request',
                'body': 'Click this link to reset your password: https://example.com/reset'
            }
        }
    ]

    # Submit jobs
    for job_data in jobs:
        await worker.submit_job(**job_data)

    # Start processing jobs
    print("🚀 Starting email job worker...")

    # Run for a limited time for demo
    job_task = asyncio.create_task(worker.run())
    await asyncio.sleep(10)  # Process for 10 seconds
    await worker.stop()

if __name__ == "__main__":
    asyncio.run(run_email_jobs())
```

## Advanced Job Processing

### Multi-Type Job Processor

```python
import asyncio
import json
import httpx
from typing import Dict, Any, Optional
from pythia.jobs import BackgroundJobWorker, JobProcessor, Job, JobResult, JobPriority
from pythia.jobs.queue import RedisJobQueue
from datetime import datetime, timedelta

class MultiTypeJobProcessor(JobProcessor):
    """Handle multiple types of jobs"""

    def __init__(self):
        self.http_client = httpx.AsyncClient(timeout=30.0)

    async def process(self, job: Job) -> JobResult:
        """Route job to appropriate handler"""
        job_type = job.name

        try:
            if job_type == 'send_email':
                return await self._handle_email_job(job)
            elif job_type == 'process_image':
                return await self._handle_image_job(job)
            elif job_type == 'sync_data':
                return await self._handle_sync_job(job)
            elif job_type == 'generate_report':
                return await self._handle_report_job(job)
            else:
                return JobResult(
                    success=False,
                    error=f"Unknown job type: {job_type}"
                )

        except Exception as e:
            return JobResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__
            )

    async def _handle_email_job(self, job: Job) -> JobResult:
        """Handle email sending job"""
        # Email logic here
        await asyncio.sleep(0.5)  # Simulate email send

        return JobResult(
            success=True,
            result={"email_sent": True, "recipient": job.kwargs.get('to')}
        )

    async def _handle_image_job(self, job: Job) -> JobResult:
        """Handle image processing job"""
        image_url = job.kwargs.get('image_url')
        operations = job.kwargs.get('operations', ['resize'])

        # Simulate image processing
        await asyncio.sleep(2.0)  # Image processing takes longer

        return JobResult(
            success=True,
            result={
                "image_processed": True,
                "image_url": image_url,
                "operations": operations,
                "output_url": f"https://cdn.example.com/processed/{job.id}.jpg"
            }
        )

    async def _handle_sync_job(self, job: Job) -> JobResult:
        """Handle data synchronization job"""
        source = job.kwargs.get('source')
        destination = job.kwargs.get('destination')

        # Simulate data sync via HTTP API
        async with self.http_client.get(f"https://api.{source}.com/data") as response:
            if response.status_code == 200:
                data = response.json()

                # Sync to destination
                sync_response = await self.http_client.post(
                    f"https://api.{destination}.com/sync",
                    json=data
                )

                return JobResult(
                    success=True,
                    result={
                        "synced": True,
                        "records_count": len(data.get('records', [])),
                        "source": source,
                        "destination": destination
                    }
                )

        return JobResult(
            success=False,
            error="Failed to sync data"
        )

    async def _handle_report_job(self, job: Job) -> JobResult:
        """Handle report generation job"""
        report_type = job.kwargs.get('type')
        date_range = job.kwargs.get('date_range')

        # Simulate report generation
        await asyncio.sleep(5.0)  # Reports take time to generate

        return JobResult(
            success=True,
            result={
                "report_generated": True,
                "report_type": report_type,
                "date_range": date_range,
                "download_url": f"https://reports.example.com/{job.id}.pdf"
            }
        )

# Advanced usage example
async def run_advanced_jobs():
    # Use Redis for job persistence
    redis_queue = RedisJobQueue(
        host="localhost",
        port=6379,
        db=0,
        queue_name="pythia_jobs"
    )

    processor = MultiTypeJobProcessor()

    worker = BackgroundJobWorker(
        queue=redis_queue,
        processor=processor,
        max_concurrent_jobs=10,
        polling_interval=0.5
    )

    # Submit various job types with different priorities
    jobs = [
        # High priority email
        {
            'name': 'send_email',
            'func': 'send_email',
            'priority': JobPriority.HIGH,
            'kwargs': {
                'to': 'urgent@example.com',
                'subject': 'Urgent: System Alert',
                'body': 'Critical system alert requiring immediate attention.'
            }
        },

        # Normal priority image processing
        {
            'name': 'process_image',
            'func': 'process_image',
            'priority': JobPriority.NORMAL,
            'kwargs': {
                'image_url': 'https://example.com/upload/image1.jpg',
                'operations': ['resize', 'compress', 'watermark']
            }
        },

        # Low priority data sync
        {
            'name': 'sync_data',
            'func': 'sync_data',
            'priority': JobPriority.LOW,
            'kwargs': {
                'source': 'shopify',
                'destination': 'warehouse'
            }
        },

        # Scheduled report generation
        {
            'name': 'generate_report',
            'func': 'generate_report',
            'scheduled_at': datetime.now() + timedelta(minutes=5),  # Run in 5 minutes
            'kwargs': {
                'type': 'monthly_sales',
                'date_range': '2024-01-01 to 2024-01-31'
            }
        }
    ]

    # Submit jobs
    submitted_jobs = []
    for job_data in jobs:
        job = await worker.submit_job(**job_data)
        submitted_jobs.append(job)
        print(f"📝 Submitted job: {job.name} (ID: {job.id})")

    # Start worker
    print("🚀 Starting multi-type job worker...")

    # Monitor jobs
    async def monitor_jobs():
        while True:
            stats = await worker.get_queue_stats()
            print(f"📊 Queue: {stats['queue_size']} | Active: {stats['active_jobs']} | Processed: {stats['worker_stats']['jobs_processed']}")

            # Check individual job status
            for job in submitted_jobs:
                status = await worker.get_job_status(job.id)
                if status:
                    print(f"   Job {job.name}: {status.value}")

            await asyncio.sleep(5)

    # Run worker and monitor concurrently
    monitor_task = asyncio.create_task(monitor_jobs())
    worker_task = asyncio.create_task(worker.run())

    # Run for demo period
    await asyncio.sleep(30)

    # Cleanup
    monitor_task.cancel()
    await worker.stop()

if __name__ == "__main__":
    asyncio.run(run_advanced_jobs())
```

## Scheduled Jobs

### Cron-Like Job Scheduling

```python
import asyncio
from datetime import datetime, timedelta
from pythia.jobs import BackgroundJobWorker, JobProcessor, Job, JobResult, JobPriority
from pythia.jobs.scheduler import JobScheduler

class ScheduledJobProcessor(JobProcessor):
    """Handle scheduled maintenance jobs"""

    async def process(self, job: Job) -> JobResult:
        """Process scheduled jobs"""
        job_type = job.name

        if job_type == 'cleanup_logs':
            return await self._cleanup_old_logs()
        elif job_type == 'backup_database':
            return await self._backup_database()
        elif job_type == 'send_daily_report':
            return await self._send_daily_report()
        elif job_type == 'health_check':
            return await self._perform_health_check()

        return JobResult(success=False, error=f"Unknown scheduled job: {job_type}")

    async def _cleanup_old_logs(self) -> JobResult:
        """Clean up log files older than 30 days"""
        # Simulate log cleanup
        await asyncio.sleep(2.0)

        deleted_files = 15
        freed_space = "2.5GB"

        return JobResult(
            success=True,
            result={
                "task": "log_cleanup",
                "deleted_files": deleted_files,
                "freed_space": freed_space
            }
        )

    async def _backup_database(self) -> JobResult:
        """Perform database backup"""
        # Simulate database backup
        await asyncio.sleep(10.0)

        backup_file = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"

        return JobResult(
            success=True,
            result={
                "task": "database_backup",
                "backup_file": backup_file,
                "size": "1.2GB"
            }
        )

    async def _send_daily_report(self) -> JobResult:
        """Send daily report to stakeholders"""
        # Simulate report generation and sending
        await asyncio.sleep(3.0)

        return JobResult(
            success=True,
            result={
                "task": "daily_report",
                "recipients": ["manager@example.com", "team@example.com"],
                "metrics_included": ["users", "revenue", "errors"]
            }
        )

    async def _perform_health_check(self) -> JobResult:
        """Perform system health check"""
        # Simulate health check
        await asyncio.sleep(1.0)

        return JobResult(
            success=True,
            result={
                "task": "health_check",
                "status": "healthy",
                "services_checked": ["database", "cache", "api", "queue"]
            }
        )

# Scheduled jobs example
async def run_scheduled_jobs():
    processor = ScheduledJobProcessor()

    worker = BackgroundJobWorker(
        processor=processor,
        max_concurrent_jobs=3,
        polling_interval=1.0
    )

    # Schedule recurring jobs
    now = datetime.now()

    scheduled_jobs = [
        # Health check every 5 minutes
        {
            'name': 'health_check',
            'func': 'health_check',
            'scheduled_at': now + timedelta(minutes=1),
            'recurring': True,
            'interval': timedelta(minutes=5)
        },

        # Log cleanup daily at 2 AM
        {
            'name': 'cleanup_logs',
            'func': 'cleanup_logs',
            'scheduled_at': now.replace(hour=2, minute=0, second=0) + timedelta(days=1),
            'recurring': True,
            'interval': timedelta(days=1)
        },

        # Database backup daily at 3 AM
        {
            'name': 'backup_database',
            'func': 'backup_database',
            'priority': JobPriority.HIGH,
            'scheduled_at': now.replace(hour=3, minute=0, second=0) + timedelta(days=1),
            'recurring': True,
            'interval': timedelta(days=1)
        },

        # Daily report at 9 AM
        {
            'name': 'send_daily_report',
            'func': 'send_daily_report',
            'scheduled_at': now.replace(hour=9, minute=0, second=0) + timedelta(days=1),
            'recurring': True,
            'interval': timedelta(days=1)
        }
    ]

    # Submit scheduled jobs
    for job_data in scheduled_jobs:
        job = await worker.submit_job(**job_data)
        print(f"📅 Scheduled job: {job.name} at {job.scheduled_at}")

    print("⏰ Starting scheduled job worker...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(run_scheduled_jobs())
```

## Job Monitoring & Management

### Job Status Dashboard

```python
import asyncio
import json
from datetime import datetime
from pythia.jobs import BackgroundJobWorker, JobStatus

class JobManager:
    """Manage and monitor background jobs"""

    def __init__(self, worker: BackgroundJobWorker):
        self.worker = worker

    async def get_job_summary(self) -> dict:
        """Get summary of all jobs"""
        stats = await self.worker.get_queue_stats()

        return {
            "timestamp": datetime.now().isoformat(),
            "queue_stats": stats,
            "worker_health": await self.worker.health_check()
        }

    async def list_jobs_by_status(self, status: JobStatus = None) -> list:
        """List jobs filtered by status"""
        # This would require extending the job queue interface
        # For demo purposes, we'll simulate
        jobs = [
            {
                "id": "job_001",
                "name": "send_email",
                "status": JobStatus.COMPLETED.value,
                "created_at": "2024-01-15T10:30:00Z",
                "completed_at": "2024-01-15T10:30:02Z"
            },
            {
                "id": "job_002",
                "name": "process_image",
                "status": JobStatus.RUNNING.value,
                "created_at": "2024-01-15T10:31:00Z",
                "started_at": "2024-01-15T10:31:01Z"
            },
            {
                "id": "job_003",
                "name": "generate_report",
                "status": JobStatus.PENDING.value,
                "created_at": "2024-01-15T10:32:00Z",
                "scheduled_at": "2024-01-15T11:00:00Z"
            }
        ]

        if status:
            jobs = [job for job in jobs if job["status"] == status.value]

        return jobs

    async def retry_failed_jobs(self) -> int:
        """Retry all failed jobs that can be retried"""
        failed_jobs = await self.list_jobs_by_status(JobStatus.FAILED)
        retried_count = 0

        for job_data in failed_jobs:
            success = await self.worker.retry_job(job_data["id"])
            if success:
                retried_count += 1

        return retried_count

    async def cancel_pending_jobs(self, job_names: list = None) -> int:
        """Cancel pending jobs, optionally filtered by name"""
        pending_jobs = await self.list_jobs_by_status(JobStatus.PENDING)
        cancelled_count = 0

        for job_data in pending_jobs:
            if job_names is None or job_data["name"] in job_names:
                success = await self.worker.cancel_job(job_data["id"])
                if success:
                    cancelled_count += 1

        return cancelled_count

    async def monitor_continuously(self, interval: int = 10):
        """Continuously monitor job worker"""
        print("📊 Starting continuous job monitoring...")

        while True:
            try:
                summary = await self.get_job_summary()

                print(f"\n--- Job Summary at {summary['timestamp']} ---")
                print(f"Queue Size: {summary['queue_stats']['queue_size']}")
                print(f"Active Jobs: {summary['queue_stats']['active_jobs']}")
                print(f"Available Slots: {summary['queue_stats']['available_slots']}")
                print(f"Jobs Processed: {summary['queue_stats']['worker_stats']['jobs_processed']}")
                print(f"Jobs Failed: {summary['queue_stats']['worker_stats']['jobs_failed']}")
                print(f"Worker Health: {'✅ Healthy' if summary['worker_health'] else '❌ Unhealthy'}")

                # Show jobs by status
                for status in [JobStatus.RUNNING, JobStatus.PENDING, JobStatus.FAILED]:
                    jobs = await self.list_jobs_by_status(status)
                    if jobs:
                        print(f"{status.value.upper()}: {len(jobs)} jobs")
                        for job in jobs[:3]:  # Show first 3
                            print(f"  - {job['name']} (ID: {job['id']})")

                await asyncio.sleep(interval)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Monitor error: {e}")
                await asyncio.sleep(interval)

# Usage example
async def run_job_monitoring():
    # Create worker
    processor = MultiTypeJobProcessor()
    worker = BackgroundJobWorker(processor=processor)

    # Create job manager
    manager = JobManager(worker)

    # Submit some test jobs
    test_jobs = [
        {'name': 'send_email', 'func': 'send_email', 'kwargs': {'to': 'test@example.com'}},
        {'name': 'process_image', 'func': 'process_image', 'kwargs': {'image_url': 'test.jpg'}},
        {'name': 'generate_report', 'func': 'generate_report', 'kwargs': {'type': 'daily'}},
    ]

    for job_data in test_jobs:
        await worker.submit_job(**job_data)

    # Start worker and monitoring
    worker_task = asyncio.create_task(worker.run())
    monitor_task = asyncio.create_task(manager.monitor_continuously())

    # Run for demo
    await asyncio.sleep(30)

    # Cleanup
    monitor_task.cancel()
    await worker.stop()

if __name__ == "__main__":
    asyncio.run(run_job_monitoring())
```

## Best Practices

### 1. Job Design

```python
# Good: Specific, focused jobs
class EmailJob(JobProcessor):
    async def process(self, job: Job) -> JobResult:
        # Single responsibility: send email
        pass

# Good: Idempotent jobs
class DataSyncJob(JobProcessor):
    async def process(self, job: Job) -> JobResult:
        # Check if sync already completed
        if await self._already_synced(job.kwargs['sync_id']):
            return JobResult(success=True, result="Already synced")
        # Proceed with sync...
```

### 2. Error Handling

```python
class RobustJobProcessor(JobProcessor):
    async def process(self, job: Job) -> JobResult:
        try:
            # Job logic here
            result = await self._do_work(job)
            return JobResult(success=True, result=result)

        except TemporaryError as e:
            # Retryable error
            return JobResult(
                success=False,
                error=str(e),
                retryable=True
            )

        except PermanentError as e:
            # Non-retryable error
            return JobResult(
                success=False,
                error=str(e),
                retryable=False
            )
```

### 3. Resource Management

```python
class ResourceManagedProcessor(JobProcessor):
    def __init__(self):
        self.db_pool = None
        self.http_client = None

    async def startup(self):
        """Initialize resources"""
        self.db_pool = await create_db_pool()
        self.http_client = httpx.AsyncClient()

    async def shutdown(self):
        """Cleanup resources"""
        if self.db_pool:
            await self.db_pool.close()
        if self.http_client:
            await self.http_client.aclose()

    async def process(self, job: Job) -> JobResult:
        # Use managed resources
        async with self.db_pool.acquire() as conn:
            # Database operations
            pass
```

## Testing Background Jobs

```python
import pytest
from pythia.jobs import Job, JobResult, JobPriority
from pythia.jobs.queue import MemoryJobQueue

@pytest.mark.asyncio
class TestEmailJobProcessor:
    async def test_successful_email_job(self):
        """Test successful email processing"""
        processor = EmailJobProcessor({})

        job = Job(
            name="test_email",
            func="send_email",
            kwargs={
                "to": "test@example.com",
                "subject": "Test",
                "body": "Test body"
            }
        )

        result = await processor.process(job)

        assert result.success is True
        assert result.result["email_sent"] is True
        assert result.result["recipient"] == "test@example.com"

    async def test_missing_email_parameters(self):
        """Test job with missing parameters"""
        processor = EmailJobProcessor({})

        job = Job(
            name="test_email",
            func="send_email",
            kwargs={"to": "test@example.com"}  # Missing subject and body
        )

        result = await processor.process(job)

        assert result.success is False
        assert "Missing required email parameters" in result.error

@pytest.mark.asyncio
async def test_job_queue_operations():
    """Test job queue operations"""
    queue = MemoryJobQueue()

    # Create test job
    job = Job(
        name="test_job",
        func="test_func",
        priority=JobPriority.HIGH
    )

    # Test put/get
    await queue.put(job)
    assert await queue.size() == 1

    retrieved_job = await queue.get()
    assert retrieved_job.id == job.id
    assert retrieved_job.priority == JobPriority.HIGH
```

## Next Steps

- [HTTP Workers](http-workers.md) - HTTP polling and webhook workers
- [Message Workers](../brokers/redis.md) - Message-based worker patterns
- [Worker Lifecycle](../user-guide/worker-lifecycle.md) - Understanding worker management
