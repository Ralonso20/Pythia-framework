"""
Background job worker for Pythia
"""

import asyncio
from typing import Optional, Dict, Any, List, Callable
from datetime import datetime

from ..core.worker import Worker
from ..core.message import Message
from ..logging import get_pythia_logger
from .job import Job, JobStatus, JobResult, JobPriority
from .queue import JobQueue, MemoryJobQueue
from .executor import JobExecutor, HybridJobExecutor


class JobProcessor:
    """Simple job processor interface"""

    async def process(self, job: Job) -> JobResult:
        """Process a job and return result"""
        # Default implementation - subclasses should override
        logger = get_pythia_logger(self.__class__.__name__)
        logger.warning(f"JobProcessor.process not implemented for job: {job.id}")

        return JobResult(
            success=False,
            result=None,
            error="JobProcessor.process method not implemented",
        )


class BackgroundJobWorker(Worker):
    """
    Background job processing worker

    Example:
        # Simple processor
        class EmailProcessor(JobProcessor):
            async def process(self, job: Job) -> JobResult:
                # Send email logic
                return JobResult(success=True, result="Email sent")

        worker = BackgroundJobWorker(
            queue=RedisJobQueue("email_jobs"),
            processor=EmailProcessor()
        )

        await worker.run()
    """

    def __init__(
        self,
        queue: Optional[JobQueue] = None,
        processor: Optional[JobProcessor] = None,
        executor: Optional[JobExecutor] = None,
        max_concurrent_jobs: int = 10,
        polling_interval: float = 1.0,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # Job processing components
        self.queue = queue or MemoryJobQueue()
        self.processor = processor
        self.executor = executor or HybridJobExecutor()

        # Configuration
        self.max_concurrent_jobs = max_concurrent_jobs
        self.polling_interval = polling_interval

        # Processing state
        self._active_jobs: Dict[str, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self._processing = False
        self._stats = {
            "jobs_processed": 0,
            "jobs_failed": 0,
            "jobs_retried": 0,
            "processing_time_total": 0.0,
        }

        self.logger = get_pythia_logger("BackgroundJobWorker")

    async def process(self, message: Message) -> None:
        """Process messages (not used for job worker)"""
        # Background job worker doesn't process traditional messages
        # It processes jobs from the queue directly
        pass

    async def run(self) -> None:
        """Start processing jobs from the queue"""
        self.logger.info("Starting background job worker")

        try:
            await self._startup()
            self._processing = True

            # Main processing loop
            while self._processing:
                try:
                    # Get next job with timeout
                    job = await self.queue.get(timeout=self.polling_interval)

                    if job:
                        # Process job concurrently
                        await self._process_job_async(job)

                    # Clean up completed jobs
                    await self._cleanup_completed_jobs()

                except Exception as e:
                    self.logger.error("Error in job processing loop", error=e)
                    await asyncio.sleep(1.0)

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error("Fatal error in job worker", error=e)
            raise
        finally:
            await self._shutdown()

    async def stop(self) -> None:
        """Stop processing jobs"""
        self.logger.info("Stopping background job worker")
        self._processing = False

        # Wait for active jobs to complete
        if self._active_jobs:
            self.logger.info(f"Waiting for {len(self._active_jobs)} active jobs to complete")
            await asyncio.gather(*self._active_jobs.values(), return_exceptions=True)

    async def submit_job(
        self,
        name: str,
        func: str,
        args: list = None,
        kwargs: dict = None,
        priority: JobPriority = JobPriority.NORMAL,
        scheduled_at: Optional[datetime] = None,
        **job_kwargs,
    ) -> Job:
        """Submit a new job to the queue"""

        job = Job(
            name=name,
            func=func,
            args=args or [],
            kwargs=kwargs or {},
            priority=priority,
            scheduled_at=scheduled_at,
            **job_kwargs,
        )

        await self.queue.put(job)

        self.logger.info(f"Job {job.id} submitted", job_name=name, priority=priority.value)
        return job

    async def submit_jobs(self, jobs: List[Job]) -> List[Job]:
        """Submit multiple jobs"""
        for job in jobs:
            await self.queue.put(job)

        self.logger.info(f"Submitted {len(jobs)} jobs to queue")
        return jobs

    async def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get status of a specific job"""
        job = await self.queue.get_job(job_id)
        return job.status if job else None

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending or running job"""
        # Cancel if currently running
        if job_id in self._active_jobs:
            task = self._active_jobs[job_id]
            task.cancel()
            del self._active_jobs[job_id]

            # Update job status
            job = await self.queue.get_job(job_id)
            if job:
                job.mark_cancelled()
                await self.queue.update_job(job)

            self.logger.info(f"Cancelled running job {job_id}")
            return True

        # Cancel if in queue
        job = await self.queue.get_job(job_id)
        if job and job.status in [JobStatus.PENDING, JobStatus.QUEUED]:
            job.mark_cancelled()
            await self.queue.update_job(job)
            self.logger.info(f"Cancelled queued job {job_id}")
            return True

        return False

    async def retry_job(self, job_id: str) -> bool:
        """Manually retry a failed job"""
        job = await self.queue.get_job(job_id)
        if not job:
            return False

        if job.status == JobStatus.FAILED and job.can_retry():
            job.mark_retry()
            job.scheduled_at = job.get_next_retry_at()
            await self.queue.update_job(job)

            self.logger.info(f"Job {job_id} scheduled for retry", retry_count=job.current_retry)
            return True

        return False

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue and processing statistics"""
        return {
            "queue_size": await self.queue.size(),
            "active_jobs": len(self._active_jobs),
            "max_concurrent": self.max_concurrent_jobs,
            "available_slots": self.max_concurrent_jobs - len(self._active_jobs),
            "worker_stats": self._stats.copy(),
            "queue_empty": await self.queue.empty(),
        }

    async def _process_job_async(self, job: Job) -> None:
        """Process a job asynchronously"""
        # Wait for available slot
        await self._semaphore.acquire()

        # Create processing task
        task = asyncio.create_task(self._process_single_job(job))
        self._active_jobs[job.id] = task

        # Don't wait for completion - let it run in background
        # Completion will be handled by cleanup

    async def _process_single_job(self, job: Job) -> None:
        """Process a single job"""
        start_time = datetime.now()

        try:
            # Mark job as started
            job.mark_started(self.config.worker_id)
            await self.queue.update_job(job)

            self.logger.info(f"Processing job {job.id}", job_name=job.name, func=job.func)

            # Execute the job
            if self.processor:
                # Use custom processor
                result = await self.processor.process(job)
            else:
                # Use executor
                result = await self.executor.execute(job)

            # Mark job as completed
            job.mark_completed(result)
            await self.queue.update_job(job)

            # Update stats
            execution_time = (datetime.now() - start_time).total_seconds()
            self._stats["processing_time_total"] += execution_time

            if result.success:
                self._stats["jobs_processed"] += 1
                self.logger.info(
                    f"Job {job.id} completed successfully",
                    job_name=job.name,
                    execution_time=execution_time,
                )
            else:
                self._stats["jobs_failed"] += 1
                self.logger.error(
                    f"Job {job.id} failed",
                    job_name=job.name,
                    error=result.error,
                    execution_time=execution_time,
                )

                # Handle retry
                await self._handle_job_retry(job)

        except Exception as e:
            # Handle unexpected errors
            self._stats["jobs_failed"] += 1
            execution_time = (datetime.now() - start_time).total_seconds()

            result = JobResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__,
                execution_time=execution_time,
            )

            job.mark_completed(result)
            await self.queue.update_job(job)

            self.logger.error(
                f"Unexpected error processing job {job.id}",
                job_name=job.name,
                error=str(e),
                execution_time=execution_time,
            )

            await self._handle_job_retry(job)

        finally:
            # Always release semaphore
            self._semaphore.release()

    async def _handle_job_retry(self, job: Job) -> None:
        """Handle job retry logic"""
        if job.can_retry():
            job.mark_retry()
            job.scheduled_at = job.get_next_retry_at()
            await self.queue.update_job(job)

            self._stats["jobs_retried"] += 1

            self.logger.info(
                f"Job {job.id} scheduled for retry {job.current_retry}/{job.max_retries}",
                job_name=job.name,
                retry_at=job.scheduled_at.isoformat(),
            )
        else:
            self.logger.warning(
                f"Job {job.id} failed permanently (max retries reached)",
                job_name=job.name,
                retries=job.current_retry,
            )

    async def _cleanup_completed_jobs(self) -> None:
        """Clean up completed job tasks"""
        completed_jobs = []

        for job_id, task in self._active_jobs.items():
            if task.done():
                completed_jobs.append(job_id)

                # Handle task exceptions
                if task.exception():
                    self.logger.error(
                        f"Task for job {job_id} failed with exception",
                        error=str(task.exception()),
                    )

        # Remove completed tasks
        for job_id in completed_jobs:
            del self._active_jobs[job_id]

    async def _startup(self) -> None:
        """Worker startup"""
        self.logger.info("Starting background job worker startup")

        # Initialize components
        if hasattr(self.queue, "connect"):
            await self.queue.connect()

        # Call parent startup
        await super().startup()

    async def _shutdown(self) -> None:
        """Worker shutdown"""
        self.logger.info("Starting background job worker shutdown")

        # Stop processing
        self._processing = False

        # Wait for active jobs
        if self._active_jobs:
            self.logger.info(f"Waiting for {len(self._active_jobs)} jobs to complete")
            await asyncio.gather(*self._active_jobs.values(), return_exceptions=True)

        # Close components
        await self.executor.close()
        await self.queue.close()

        # Call parent shutdown
        await super().shutdown()

        self.logger.info("Background job worker shutdown complete")

    async def health_check(self) -> bool:
        """Health check for job worker"""
        try:
            # Check queue connectivity
            queue_ok = await self.queue.size() >= 0

            # Check if processing is active
            processing_ok = self._processing

            # Check semaphore state
            semaphore_ok = self._semaphore._value >= 0

            return queue_ok and processing_ok and semaphore_ok

        except Exception:
            return False


# Convenience function for simple job submission
async def submit_job(
    name: str,
    func: str,
    args: list = None,
    kwargs: dict = None,
    queue: Optional[JobQueue] = None,
    **job_kwargs,
) -> Job:
    """Submit a job to a queue (convenience function)"""

    if queue is None:
        queue = MemoryJobQueue()

    job = Job(name=name, func=func, args=args or [], kwargs=kwargs or {}, **job_kwargs)

    await queue.put(job)
    return job


# Decorator for making functions job-compatible
def job_function(
    name: Optional[str] = None,
    priority: JobPriority = JobPriority.NORMAL,
    max_retries: int = 3,
    timeout: Optional[float] = None,
):
    """Decorator to mark functions as job-compatible"""

    def decorator(func: Callable) -> Callable:
        # Store job metadata on function
        func._job_name = name or func.__name__
        func._job_priority = priority
        func._job_max_retries = max_retries
        func._job_timeout = timeout

        # Create submission helper
        async def submit_to_queue(queue: JobQueue, *args, **kwargs) -> Job:
            """Submit this function as a job"""

            # Build function path
            func_path = f"{func.__module__}.{func.__name__}"

            job = Job(
                name=func._job_name,
                func=func_path,
                args=list(args),
                kwargs=kwargs,
                priority=func._job_priority,
                max_retries=func._job_max_retries,
                timeout=func._job_timeout,
            )

            await queue.put(job)
            return job

        func.submit_job = submit_to_queue
        return func

    return decorator
