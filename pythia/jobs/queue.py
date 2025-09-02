"""
Job queue implementations for Pythia
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from datetime import datetime

import redis.asyncio as redis

from .job import Job, JobStatus, JobPriority
from ..logging import get_pythia_logger


class JobQueue(ABC):
    """Abstract base class for job queues"""

    @abstractmethod
    async def put(self, job: Job) -> None:
        """Add a job to the queue"""
        pass

    @abstractmethod
    async def get(self, timeout: Optional[float] = None) -> Optional[Job]:
        """Get the next job from the queue"""
        pass

    @abstractmethod
    async def peek(self, count: int = 1) -> List[Job]:
        """Peek at jobs without removing them"""
        pass

    @abstractmethod
    async def size(self) -> int:
        """Get the number of jobs in the queue"""
        pass

    @abstractmethod
    async def empty(self) -> bool:
        """Check if the queue is empty"""
        pass

    @abstractmethod
    async def clear(self) -> int:
        """Clear all jobs from the queue"""
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get a specific job by ID"""
        pass

    @abstractmethod
    async def update_job(self, job: Job) -> None:
        """Update a job in the queue"""
        pass

    @abstractmethod
    async def delete_job(self, job_id: str) -> bool:
        """Delete a job from the queue"""
        pass

    @abstractmethod
    async def get_jobs_by_status(self, status: JobStatus) -> List[Job]:
        """Get jobs by status"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the queue connection"""
        pass


class MemoryJobQueue(JobQueue):
    """In-memory job queue implementation"""

    def __init__(self, name: str = "default"):
        self.name = name
        self.logger = get_pythia_logger(f"MemoryJobQueue[{name}]")

        # Priority queues for different priority levels
        self._queues: Dict[JobPriority, asyncio.Queue] = {
            JobPriority.CRITICAL: asyncio.Queue(),
            JobPriority.HIGH: asyncio.Queue(),
            JobPriority.NORMAL: asyncio.Queue(),
            JobPriority.LOW: asyncio.Queue(),
        }

        # Job storage
        self._jobs: Dict[str, Job] = {}

        # Status indexes
        self._status_index: Dict[JobStatus, set[str]] = {status: set() for status in JobStatus}

        # Scheduled jobs
        self._scheduled_jobs: List[Job] = []

        # Background task for scheduled jobs
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False

    async def put(self, job: Job) -> None:
        """Add a job to the queue"""
        self._jobs[job.id] = job
        self._status_index[job.status].add(job.id)

        if job.is_scheduled():
            self._scheduled_jobs.append(job)
            self._ensure_scheduler_running()
        else:
            job.status = JobStatus.QUEUED
            await self._queues[job.priority].put(job)

        self.logger.debug(
            f"Job {job.id} added to queue",
            job_name=job.name,
            priority=job.priority.value,
        )

    async def get(self, timeout: Optional[float] = None) -> Optional[Job]:
        """Get the next job from the queue (priority order)"""

        # Try each priority queue in order
        for priority in [
            JobPriority.CRITICAL,
            JobPriority.HIGH,
            JobPriority.NORMAL,
            JobPriority.LOW,
        ]:
            try:
                job = self._queues[priority].get_nowait()
                self._update_status_index(job, JobStatus.RUNNING)
                return job
            except asyncio.QueueEmpty:
                continue

        # If no jobs immediately available and timeout specified, wait
        if timeout and timeout > 0:
            # Create a task for each priority queue
            tasks = []
            for priority in [
                JobPriority.CRITICAL,
                JobPriority.HIGH,
                JobPriority.NORMAL,
                JobPriority.LOW,
            ]:
                task = asyncio.create_task(self._queues[priority].get())
                tasks.append((priority, task))

            try:
                # Wait for first job from any queue
                done, pending = await asyncio.wait(
                    [task for _, task in tasks],
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Cancel remaining tasks
                for _, task in tasks:
                    if task in pending:
                        task.cancel()

                if done:
                    job = done.pop().result()
                    self._update_status_index(job, JobStatus.RUNNING)
                    return job

            except Exception:
                # Cancel all tasks on error
                for _, task in tasks:
                    task.cancel()

        return None

    async def peek(self, count: int = 1) -> List[Job]:
        """Peek at jobs without removing them"""
        jobs = []

        # Get jobs from each priority queue
        for priority in [
            JobPriority.CRITICAL,
            JobPriority.HIGH,
            JobPriority.NORMAL,
            JobPriority.LOW,
        ]:
            queue = self._queues[priority]
            queue_jobs = []

            # Temporarily drain queue to peek
            while not queue.empty() and len(jobs + queue_jobs) < count:
                job = queue.get_nowait()
                queue_jobs.append(job)

            # Put jobs back
            for job in queue_jobs:
                await queue.put(job)

            jobs.extend(queue_jobs[: count - len(jobs)])

            if len(jobs) >= count:
                break

        return jobs

    async def size(self) -> int:
        """Get total number of jobs in all queues"""
        return sum(queue.qsize() for queue in self._queues.values())

    async def empty(self) -> bool:
        """Check if all queues are empty"""
        return await self.size() == 0

    async def clear(self) -> int:
        """Clear all jobs from all queues"""
        count = 0
        for queue in self._queues.values():
            while not queue.empty():
                queue.get_nowait()
                count += 1

        self._jobs.clear()
        for status_set in self._status_index.values():
            status_set.clear()
        self._scheduled_jobs.clear()

        return count

    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get a specific job by ID"""
        return self._jobs.get(job_id)

    async def update_job(self, job: Job) -> None:
        """Update a job"""
        if job.id in self._jobs:
            old_job = self._jobs[job.id]
            self._update_status_index(old_job, job.status)
            self._jobs[job.id] = job

    async def delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        job = self._jobs.get(job_id)
        if job:
            del self._jobs[job_id]
            self._status_index[job.status].discard(job_id)
            return True
        return False

    async def get_jobs_by_status(self, status: JobStatus) -> List[Job]:
        """Get jobs by status"""
        job_ids = self._status_index[status]
        return [self._jobs[job_id] for job_id in job_ids if job_id in self._jobs]

    async def close(self) -> None:
        """Close the queue"""
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        self.logger.info("Memory job queue closed")

    def _update_status_index(self, job: Job, new_status: JobStatus) -> None:
        """Update job status index"""
        old_status = job.status
        if old_status != new_status:
            self._status_index[old_status].discard(job.id)
            self._status_index[new_status].add(job.id)
            job.status = new_status

    def _ensure_scheduler_running(self) -> None:
        """Ensure scheduler task is running for scheduled jobs"""
        if not self._running:
            self._running = True
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def _scheduler_loop(self) -> None:
        """Background loop to process scheduled jobs"""
        while self._running:
            try:
                now = datetime.now()
                ready_jobs = []

                # Check which scheduled jobs are ready
                for job in self._scheduled_jobs[:]:  # Copy list to allow modification
                    if job.scheduled_at and job.scheduled_at <= now:
                        ready_jobs.append(job)
                        self._scheduled_jobs.remove(job)

                # Queue ready jobs
                for job in ready_jobs:
                    job.status = JobStatus.QUEUED
                    await self._queues[job.priority].put(job)
                    self.logger.debug(f"Scheduled job {job.id} queued", job_name=job.name)

                # Sleep for a short interval
                await asyncio.sleep(1.0)

            except Exception as e:
                self.logger.error("Error in scheduler loop", error=str(e))
                await asyncio.sleep(5.0)


class RedisJobQueue(JobQueue):
    """Redis-based job queue implementation"""

    def __init__(
        self,
        name: str = "default",
        redis_url: str = "redis://localhost:6379",
        key_prefix: str = "pythia:jobs:",
        **redis_kwargs,
    ):
        self.name = name
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.redis_kwargs = redis_kwargs

        self.logger = get_pythia_logger(f"RedisJobQueue[{name}]")

        # Redis keys
        self.queue_key = f"{key_prefix}queue:{name}"
        self.jobs_key = f"{key_prefix}jobs:{name}"
        self.scheduled_key = f"{key_prefix}scheduled:{name}"
        self.status_key = f"{key_prefix}status:{name}"

        # Redis connection
        self._redis: Optional[redis.Redis] = None

        # Background scheduler
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False

    async def _get_redis(self) -> redis.Redis:
        """Get Redis connection"""
        if not self._redis:
            self._redis = redis.from_url(self.redis_url, **self.redis_kwargs)
        return self._redis

    async def put(self, job: Job) -> None:
        """Add a job to the queue"""
        r = await self._get_redis()

        # Store job data
        await r.hset(self.jobs_key, job.id, job.to_json())

        # Update status index
        await r.sadd(f"{self.status_key}:{job.status.value}", job.id)

        if job.is_scheduled():
            # Add to scheduled jobs with timestamp as score
            timestamp = job.scheduled_at.timestamp()
            await r.zadd(self.scheduled_key, {job.id: timestamp})
            self._ensure_scheduler_running()
        else:
            # Add to priority queue
            job.status = JobStatus.QUEUED
            priority_score = self._get_priority_score(job.priority)
            await r.zadd(self.queue_key, {job.id: priority_score})

        self.logger.debug(f"Job {job.id} added to Redis queue", job_name=job.name)

    async def get(self, timeout: Optional[float] = None) -> Optional[Job]:
        """Get the next job from the queue"""
        r = await self._get_redis()

        # Get highest priority job (highest score)
        if timeout:
            # Blocking pop with timeout
            result = await r.bzpopmax(self.queue_key, timeout=timeout)
            if not result:
                return None
            _, job_id, _ = result
        else:
            # Non-blocking pop
            result = await r.zpopmax(self.queue_key, count=1)
            if not result:
                return None
            job_id, _ = result[0]

        # Get job data
        job_data = await r.hget(self.jobs_key, job_id)
        if not job_data:
            return None

        job = Job.from_json(job_data)

        # Update status
        await self._update_job_status(job, JobStatus.RUNNING)

        return job

    async def peek(self, count: int = 1) -> List[Job]:
        """Peek at jobs without removing them"""
        r = await self._get_redis()

        # Get job IDs from queue (highest priority first)
        job_ids = await r.zrevrange(self.queue_key, 0, count - 1)

        jobs = []
        for job_id in job_ids:
            job_data = await r.hget(self.jobs_key, job_id)
            if job_data:
                job = Job.from_json(job_data)
                jobs.append(job)

        return jobs

    async def size(self) -> int:
        """Get queue size"""
        r = await self._get_redis()
        return await r.zcard(self.queue_key)

    async def empty(self) -> bool:
        """Check if queue is empty"""
        return await self.size() == 0

    async def clear(self) -> int:
        """Clear all jobs"""
        r = await self._get_redis()

        # Get all job IDs
        all_jobs = await r.hgetall(self.jobs_key)
        count = len(all_jobs)

        # Delete all keys
        if count > 0:
            await r.delete(
                self.queue_key,
                self.jobs_key,
                self.scheduled_key,
            )

            # Clear status indexes
            for status in JobStatus:
                await r.delete(f"{self.status_key}:{status.value}")

        return count

    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get a specific job by ID"""
        r = await self._get_redis()
        job_data = await r.hget(self.jobs_key, job_id)
        if job_data:
            return Job.from_json(job_data)
        return None

    async def update_job(self, job: Job) -> None:
        """Update a job"""
        r = await self._get_redis()

        # Update job data
        await r.hset(self.jobs_key, job.id, job.to_json())

        # Note: Status index updates would need to track old status
        # This is a simplified implementation
        await r.sadd(f"{self.status_key}:{job.status.value}", job.id)

    async def delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        r = await self._get_redis()

        # Get job first to know its status
        job = await self.get_job(job_id)
        if not job:
            return False

        # Remove from all possible locations
        await r.hdel(self.jobs_key, job_id)
        await r.zrem(self.queue_key, job_id)
        await r.zrem(self.scheduled_key, job_id)
        await r.srem(f"{self.status_key}:{job.status.value}", job_id)

        return True

    async def get_jobs_by_status(self, status: JobStatus) -> List[Job]:
        """Get jobs by status"""
        r = await self._get_redis()

        job_ids = await r.smembers(f"{self.status_key}:{status.value}")
        jobs = []

        for job_id in job_ids:
            job_data = await r.hget(self.jobs_key, job_id)
            if job_data:
                job = Job.from_json(job_data)
                jobs.append(job)

        return jobs

    async def close(self) -> None:
        """Close Redis connection"""
        self._running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        if self._redis:
            await self._redis.close()
            self._redis = None

        self.logger.info("Redis job queue closed")

    def _get_priority_score(self, priority: JobPriority) -> float:
        """Get numeric score for priority (higher = higher priority)"""
        scores = {
            JobPriority.LOW: 1.0,
            JobPriority.NORMAL: 2.0,
            JobPriority.HIGH: 3.0,
            JobPriority.CRITICAL: 4.0,
        }
        return scores[priority]

    async def _update_job_status(self, job: Job, new_status: JobStatus) -> None:
        """Update job status in Redis"""
        r = await self._get_redis()

        old_status = job.status
        job.status = new_status

        # Update job data
        await r.hset(self.jobs_key, job.id, job.to_json())

        # Update status indexes
        if old_status != new_status:
            await r.srem(f"{self.status_key}:{old_status.value}", job.id)
            await r.sadd(f"{self.status_key}:{new_status.value}", job.id)

    def _ensure_scheduler_running(self) -> None:
        """Ensure scheduler task is running"""
        if not self._running:
            self._running = True
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def _scheduler_loop(self) -> None:
        """Process scheduled jobs"""
        while self._running:
            try:
                r = await self._get_redis()
                now = time.time()

                # Get ready jobs (score <= current timestamp)
                ready_jobs = await r.zrangebyscore(self.scheduled_key, 0, now, withscores=False)

                for job_id in ready_jobs:
                    # Get job data
                    job_data = await r.hget(self.jobs_key, job_id)
                    if not job_data:
                        continue

                    job = Job.from_json(job_data)

                    # Remove from scheduled and add to queue
                    await r.zrem(self.scheduled_key, job_id)

                    job.status = JobStatus.QUEUED
                    priority_score = self._get_priority_score(job.priority)
                    await r.zadd(self.queue_key, {job_id: priority_score})

                    # Update job status
                    await self._update_job_status(job, JobStatus.QUEUED)

                    self.logger.debug(f"Scheduled job {job_id} queued")

                await asyncio.sleep(1.0)

            except Exception as e:
                self.logger.error("Error in Redis scheduler loop", error=str(e))
                await asyncio.sleep(5.0)
