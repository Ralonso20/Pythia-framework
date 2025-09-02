"""
Scheduled task system for Pythia
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable, Union, List
from dataclasses import dataclass

from ..core.worker import Worker
from ..core.message import Message
from ..logging import get_pythia_logger
from .job import Job, JobPriority
from .queue import JobQueue, MemoryJobQueue


@dataclass
class Schedule:
    """Base class for schedules"""

    def get_next_run(self, last_run: Optional[datetime] = None) -> datetime:
        """Get next run time"""
        # Default implementation - run every hour
        if last_run is None:
            return datetime.now() + timedelta(hours=1)
        return last_run + timedelta(hours=1)

    def should_run(self, last_run: Optional[datetime] = None) -> bool:
        """Check if should run now"""
        return datetime.now() >= self.get_next_run(last_run)


@dataclass
class IntervalJob(Schedule):
    """Job that runs at regular intervals"""

    interval: Union[int, float, timedelta]
    start_time: Optional[datetime] = None

    def __post_init__(self):
        if isinstance(self.interval, (int, float)):
            self.interval = timedelta(seconds=self.interval)

    def get_next_run(self, last_run: Optional[datetime] = None) -> datetime:
        """Get next run time based on interval"""
        if last_run is None:
            return self.start_time or datetime.now()

        return last_run + self.interval

    def __str__(self) -> str:
        return f"Every {self.interval}"


@dataclass
class CronJob(Schedule):
    """Job that runs based on cron expression"""

    expression: str
    timezone: Optional[str] = None

    def __post_init__(self):
        # Import croniter if available
        try:
            from croniter import croniter

            self.croniter = croniter
        except ImportError:
            raise ImportError(
                "croniter is required for CronJob. Install with: pip install croniter"
            )

    def get_next_run(self, last_run: Optional[datetime] = None) -> datetime:
        """Get next run time from cron expression"""
        base = last_run or datetime.now()

        if self.timezone:
            import pytz

            tz = pytz.timezone(self.timezone)
            if base.tzinfo is None:
                base = tz.localize(base)

        cron = self.croniter(self.expression, base)
        return cron.get_next(datetime)

    def __str__(self) -> str:
        return f"Cron: {self.expression}"


class ScheduledTask:
    """A scheduled task configuration"""

    def __init__(
        self,
        name: str,
        func: Union[str, Callable],
        schedule: Schedule,
        args: list = None,
        kwargs: dict = None,
        priority: JobPriority = JobPriority.NORMAL,
        enabled: bool = True,
        max_retries: int = 3,
        timeout: Optional[float] = None,
        tags: List[str] = None,
        metadata: Dict[str, Any] = None,
    ):
        self.name = name
        self.func = func
        self.schedule = schedule
        self.args = args or []
        self.kwargs = kwargs or {}
        self.priority = priority
        self.enabled = enabled
        self.max_retries = max_retries
        self.timeout = timeout
        self.tags = tags or []
        self.metadata = metadata or {}

        # Runtime state
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.run_count = 0
        self.success_count = 0
        self.failure_count = 0

    def get_func_path(self) -> str:
        """Get function path for job execution"""
        if isinstance(self.func, str):
            return self.func
        else:
            return f"{self.func.__module__}.{self.func.__name__}"

    def should_run(self, now: Optional[datetime] = None) -> bool:
        """Check if task should run now"""
        if not self.enabled:
            return False

        if now is None:
            now = datetime.now()

        if self.next_run is None:
            self.next_run = self.schedule.get_next_run(self.last_run)

        return now >= self.next_run

    def create_job(self) -> Job:
        """Create a job for this scheduled task"""
        job = Job(
            name=f"scheduled:{self.name}",
            func=self.get_func_path(),
            args=self.args,
            kwargs=self.kwargs,
            priority=self.priority,
            max_retries=self.max_retries,
            timeout=self.timeout,
            tags=["scheduled"] + self.tags,
            metadata={
                "scheduled_task": self.name,
                "schedule": str(self.schedule),
                **self.metadata,
            },
        )
        return job

    def mark_run(self, success: bool = True) -> None:
        """Mark task as having been run"""
        now = datetime.now()
        self.last_run = now
        self.next_run = self.schedule.get_next_run(now)
        self.run_count += 1

        if success:
            self.success_count += 1
        else:
            self.failure_count += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get task statistics"""
        return {
            "name": self.name,
            "enabled": self.enabled,
            "schedule": str(self.schedule),
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": self.success_count / max(self.run_count, 1) * 100,
        }

    def enable(self) -> None:
        """Enable the task"""
        self.enabled = True

    def disable(self) -> None:
        """Disable the task"""
        self.enabled = False

    def __str__(self) -> str:
        return f"ScheduledTask({self.name}, {self.schedule}, enabled={self.enabled})"


class ScheduledWorker(Worker):
    """
    Worker that executes scheduled tasks

    Example:
        scheduler = ScheduledWorker(
            tasks=[
                ScheduledTask(
                    name="daily_report",
                    func="myapp.tasks.generate_report",
                    schedule=CronJob("0 9 * * *")  # Daily at 9 AM
                ),
                ScheduledTask(
                    name="health_check",
                    func="myapp.tasks.health_check",
                    schedule=IntervalJob(minutes=5)  # Every 5 minutes
                )
            ],
            job_queue=RedisJobQueue("scheduled_jobs")
        )

        await scheduler.run()
    """

    def __init__(
        self,
        tasks: List[ScheduledTask] = None,
        job_queue: Optional[JobQueue] = None,
        check_interval: float = 60.0,  # Check every minute
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.tasks: Dict[str, ScheduledTask] = {}
        self.job_queue = job_queue or MemoryJobQueue("scheduled")
        self.check_interval = check_interval

        # Add initial tasks
        if tasks:
            for task in tasks:
                self.add_task(task)

        # Scheduler state
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False

        self.logger = get_pythia_logger("ScheduledWorker")

    async def process(self, message: Message) -> None:
        """Process messages (not used for scheduled worker)"""
        # Scheduled worker doesn't process traditional messages
        pass

    async def run(self) -> None:
        """Start the scheduler"""
        self.logger.info(f"Starting scheduler with {len(self.tasks)} tasks")

        try:
            await self._startup()
            self._running = True

            # Start scheduler loop
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())

            # Wait for scheduler to complete
            await self._scheduler_task

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error("Fatal error in scheduler", error=e)
            raise
        finally:
            await self._shutdown()

    async def stop(self) -> None:
        """Stop the scheduler"""
        self.logger.info("Stopping scheduler")
        self._running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

    def add_task(self, task: ScheduledTask) -> None:
        """Add a scheduled task"""
        self.tasks[task.name] = task
        self.logger.info(f"Added scheduled task: {task.name}", schedule=str(task.schedule))

    def remove_task(self, name: str) -> bool:
        """Remove a scheduled task"""
        if name in self.tasks:
            del self.tasks[name]
            self.logger.info(f"Removed scheduled task: {name}")
            return True
        return False

    def get_task(self, name: str) -> Optional[ScheduledTask]:
        """Get a scheduled task by name"""
        return self.tasks.get(name)

    def enable_task(self, name: str) -> bool:
        """Enable a scheduled task"""
        task = self.tasks.get(name)
        if task:
            task.enable()
            self.logger.info(f"Enabled task: {name}")
            return True
        return False

    def disable_task(self, name: str) -> bool:
        """Disable a scheduled task"""
        task = self.tasks.get(name)
        if task:
            task.disable()
            self.logger.info(f"Disabled task: {name}")
            return True
        return False

    def get_task_stats(self) -> Dict[str, Any]:
        """Get statistics for all tasks"""
        return {name: task.get_stats() for name, task in self.tasks.items()}

    def get_next_runs(self) -> Dict[str, Optional[str]]:
        """Get next run times for all tasks"""
        return {
            name: task.next_run.isoformat() if task.next_run else None
            for name, task in self.tasks.items()
        }

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop"""
        self.logger.info("Scheduler loop started")

        while self._running:
            try:
                now = datetime.now()

                # Check each task
                for task in self.tasks.values():
                    try:
                        if task.should_run(now):
                            await self._execute_task(task)
                    except Exception as e:
                        self.logger.error(f"Error checking task {task.name}", error=str(e))

                # Sleep until next check
                await asyncio.sleep(self.check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in scheduler loop", error=str(e))
                await asyncio.sleep(self.check_interval)

        self.logger.info("Scheduler loop stopped")

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task"""
        self.logger.info(f"Executing scheduled task: {task.name}")

        try:
            # Create job for the task
            job = task.create_job()

            # Submit to job queue
            await self.job_queue.put(job)

            # Mark task as run (success assumed for now)
            task.mark_run(success=True)

            self.logger.debug(
                f"Task {task.name} scheduled for execution",
                job_id=job.id,
                next_run=task.next_run.isoformat() if task.next_run else None,
            )

        except Exception as e:
            task.mark_run(success=False)
            self.logger.error(f"Failed to execute task {task.name}", error=str(e))

    async def _startup(self) -> None:
        """Scheduler startup"""
        self.logger.info("Starting scheduler startup")

        # Initialize job queue if needed
        if hasattr(self.job_queue, "connect"):
            await self.job_queue.connect()

        # Calculate initial next run times
        now = datetime.now()
        for task in self.tasks.values():
            if task.next_run is None:
                task.next_run = task.schedule.get_next_run(now)

        await super().startup()

    async def _shutdown(self) -> None:
        """Scheduler shutdown"""
        self.logger.info("Starting scheduler shutdown")

        # Stop scheduler
        self._running = False

        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        # Close job queue
        await self.job_queue.close()

        await super().shutdown()

        self.logger.info("Scheduler shutdown complete")

    async def health_check(self) -> bool:
        """Health check for scheduler"""
        try:
            # Check if scheduler is running
            if not self._running:
                return False

            # Check if scheduler task is alive
            if self._scheduler_task and self._scheduler_task.done():
                return False

            # Check job queue
            queue_ok = await self.job_queue.size() >= 0

            return queue_ok

        except Exception:
            return False


# Convenience functions and decorators
def every(interval: Union[int, float, timedelta]) -> IntervalJob:
    """Create an interval schedule"""
    return IntervalJob(interval=interval)


def cron(expression: str, timezone: Optional[str] = None) -> CronJob:
    """Create a cron schedule"""
    return CronJob(expression=expression, timezone=timezone)


def scheduled_task(
    name: str,
    schedule: Schedule,
    priority: JobPriority = JobPriority.NORMAL,
    max_retries: int = 3,
    timeout: Optional[float] = None,
    enabled: bool = True,
    tags: List[str] = None,
):
    """Decorator to mark functions as scheduled tasks"""

    def decorator(func: Callable) -> Callable:
        # Create the scheduled task
        task = ScheduledTask(
            name=name,
            func=func,
            schedule=schedule,
            priority=priority,
            max_retries=max_retries,
            timeout=timeout,
            enabled=enabled,
            tags=tags or [],
        )

        # Store task on function
        func._scheduled_task = task

        return func

    return decorator


# Common schedule helpers
class CommonSchedules:
    """Common schedule patterns"""

    @staticmethod
    def hourly() -> CronJob:
        """Run every hour"""
        return cron("0 * * * *")

    @staticmethod
    def daily(hour: int = 0, minute: int = 0) -> CronJob:
        """Run daily at specified time"""
        return cron(f"{minute} {hour} * * *")

    @staticmethod
    def weekly(day: int = 1, hour: int = 0, minute: int = 0) -> CronJob:
        """Run weekly (day: 1=Monday, 7=Sunday)"""
        return cron(f"{minute} {hour} * * {day}")

    @staticmethod
    def monthly(day: int = 1, hour: int = 0, minute: int = 0) -> CronJob:
        """Run monthly on specified day"""
        return cron(f"{minute} {hour} {day} * *")

    @staticmethod
    def every_minutes(minutes: int) -> IntervalJob:
        """Run every N minutes"""
        return every(timedelta(minutes=minutes))

    @staticmethod
    def every_hours(hours: int) -> IntervalJob:
        """Run every N hours"""
        return every(timedelta(hours=hours))

    @staticmethod
    def every_days(days: int) -> IntervalJob:
        """Run every N days"""
        return every(timedelta(days=days))
