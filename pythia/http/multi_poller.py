"""
Multi-URL HTTP Poller for Pythia framework
"""

import asyncio
from typing import AsyncIterator, Dict, Any, Optional, List, Union
from datetime import datetime

from pythia.core.message import Message
from pythia.brokers.base import BaseConsumer
from pythia.config.http import PollerConfig
from pythia.logging import get_pythia_logger
from .poller import HTTPPoller


class MultiHTTPPoller(BaseConsumer):
    """
    HTTP Poller that can poll multiple URLs concurrently

    Example:
        poller = MultiHTTPPoller([
            {"url": "https://api1.com/data", "interval": 30},
            {"url": "https://api2.com/status", "interval": 60},
            {"url": "https://api3.com/events", "interval": 10, "method": "POST", "data": {"filter": "new"}},
        ])

        async for message in poller.consume():
            print(f"From {message.headers['http_url']}: {message.body}")
    """

    def __init__(
        self,
        endpoints: List[Dict[str, Any]],
        config: Optional[PollerConfig] = None,
        max_concurrent: int = 10,
        **kwargs,
    ):
        """
        Initialize multi-URL poller

        Args:
            endpoints: List of endpoint configurations
            config: Base configuration for all endpoints
            max_concurrent: Maximum concurrent pollers
            **kwargs: Additional configuration
        """
        super().__init__()

        self.endpoints = endpoints
        self.base_config = config
        self.max_concurrent = max_concurrent
        self.kwargs = kwargs

        self.logger = get_pythia_logger("MultiHTTPPoller")

        # Individual pollers
        self._pollers: List[HTTPPoller] = []
        self._polling_tasks: List[asyncio.Task] = []
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._stop_event = asyncio.Event()

    async def connect(self) -> None:
        """Initialize all pollers"""
        if self._pollers:
            return

        self.logger.info(f"Initializing {len(self.endpoints)} HTTP pollers")

        for i, endpoint_config in enumerate(self.endpoints):
            # Merge base config with endpoint-specific config
            poller_config = self._create_poller_config(endpoint_config)

            # Create individual poller
            poller = HTTPPoller(
                url=endpoint_config["url"],
                interval=endpoint_config.get("interval", 60),
                method=endpoint_config.get("method", "GET"),
                headers=endpoint_config.get("headers"),
                params=endpoint_config.get("params"),
                data_extractor=endpoint_config.get("data_extractor"),
                config=poller_config,
                **self.kwargs,
            )

            await poller.connect()
            self._pollers.append(poller)

        self.logger.info(f"Initialized {len(self._pollers)} pollers successfully")

    async def disconnect(self) -> None:
        """Disconnect all pollers"""
        self._stop_event.set()

        # Cancel all polling tasks
        for task in self._polling_tasks:
            if not task.done():
                task.cancel()

        if self._polling_tasks:
            await asyncio.gather(*self._polling_tasks, return_exceptions=True)
            self._polling_tasks.clear()

        # Disconnect all pollers
        for poller in self._pollers:
            try:
                await poller.disconnect()
            except Exception as e:
                self.logger.warning(f"Error disconnecting poller {poller.url}", error=str(e))

        self._pollers.clear()
        self.logger.info("All pollers disconnected")

    def is_connected(self) -> bool:
        """Check if any poller is connected"""
        return any(poller.is_connected() for poller in self._pollers)

    async def consume(self) -> AsyncIterator[Message]:
        """Consume messages from all pollers"""
        if not self._pollers:
            await self.connect()

        # Start all polling tasks
        await self._start_polling_tasks()

        self.logger.info(f"Starting consumption from {len(self._pollers)} pollers")

        try:
            while not self._stop_event.is_set():
                try:
                    # Get message from queue with timeout
                    message = await asyncio.wait_for(self._message_queue.get(), timeout=1.0)
                    yield message
                    self._message_queue.task_done()

                except asyncio.TimeoutError:
                    # Check if any tasks are still running
                    if all(task.done() for task in self._polling_tasks):
                        break
                    continue

        except Exception as e:
            self.logger.error("Error in consumption loop", error=str(e))
            raise
        finally:
            await self.disconnect()

    async def _start_polling_tasks(self) -> None:
        """Start all polling tasks"""
        semaphore = asyncio.Semaphore(self.max_concurrent)

        for poller in self._pollers:
            task = asyncio.create_task(self._poll_single(poller, semaphore))
            self._polling_tasks.append(task)

    async def _poll_single(self, poller: HTTPPoller, semaphore: asyncio.Semaphore) -> None:
        """Poll a single endpoint and queue messages"""
        async with semaphore:
            try:
                async for message in poller.consume():
                    if self._stop_event.is_set():
                        break
                    await self._message_queue.put(message)

            except Exception as e:
                self.logger.error(f"Error polling {poller.url}", error=str(e))
                # Continue polling other endpoints

    def _create_poller_config(self, endpoint_config: Dict[str, Any]) -> Optional[PollerConfig]:
        """Create poller configuration for an endpoint"""
        if not self.base_config:
            return None

        # Start with base config
        config_dict = self.base_config.model_dump()

        # Override with endpoint-specific config
        for key in ["url", "interval", "method", "headers", "params"]:
            if key in endpoint_config:
                config_dict[key] = endpoint_config[key]

        return PollerConfig(**config_dict)

    async def health_check(self) -> bool:
        """Check health of all pollers"""
        if not self._pollers:
            return False

        results = await asyncio.gather(
            *[poller.health_check() for poller in self._pollers], return_exceptions=True
        )

        # Return True if at least one poller is healthy
        return any(result is True for result in results)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all pollers"""
        return {
            "total_pollers": len(self._pollers),
            "active_tasks": len([t for t in self._polling_tasks if not t.done()]),
            "queue_size": self._message_queue.qsize(),
            "endpoints": [poller.get_poll_stats() for poller in self._pollers],
        }

    def add_endpoint(self, url: str, interval: int = 60, method: str = "GET", **config) -> None:
        """Add a new endpoint to poll (requires reconnection)"""
        endpoint_config = {"url": url, "interval": interval, "method": method, **config}
        self.endpoints.append(endpoint_config)

        # Note: Would need to restart polling to include new endpoint
        self.logger.info(f"Added endpoint {url} (requires restart to take effect)")

    def remove_endpoint(self, url: str) -> bool:
        """Remove an endpoint (requires reconnection)"""
        original_count = len(self.endpoints)
        self.endpoints = [ep for ep in self.endpoints if ep.get("url") != url]
        removed = len(self.endpoints) < original_count

        if removed:
            self.logger.info(f"Removed endpoint {url} (requires restart to take effect)")

        return removed


class ScheduledHTTPPoller(BaseConsumer):
    """
    HTTP Poller with advanced scheduling capabilities

    Example:
        # Poll different endpoints at different times
        poller = ScheduledHTTPPoller({
            "0 */5 * * *": "https://api.com/frequent-data",  # Every 5 minutes
            "0 0 * * *": "https://api.com/daily-report",      # Daily at midnight
            "0 0 * * 1": "https://api.com/weekly-summary",    # Weekly on Mondays
        })
    """

    def __init__(
        self,
        schedule: Dict[str, Union[str, Dict[str, Any]]],
        config: Optional[PollerConfig] = None,
        **kwargs,
    ):
        """
        Initialize scheduled poller

        Args:
            schedule: Dict of cron expressions to URLs or endpoint configs
            config: Base configuration
            **kwargs: Additional configuration
        """
        super().__init__()

        self.schedule = schedule
        self.base_config = config
        self.kwargs = kwargs

        self.logger = get_pythia_logger("ScheduledHTTPPoller")

        # Scheduler state
        self._scheduler_task: Optional[asyncio.Task] = None
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._stop_event = asyncio.Event()

        # Import cron parser if available
        try:
            from croniter import croniter

            self.croniter = croniter
        except ImportError:
            raise ImportError(
                "croniter is required for ScheduledHTTPPoller. "
                "Install it with: pip install croniter"
            )

    async def connect(self) -> None:
        """Initialize scheduler"""
        if self._scheduler_task:
            return

        self.logger.info(f"Initializing scheduler with {len(self.schedule)} scheduled polls")
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def disconnect(self) -> None:
        """Stop scheduler"""
        self._stop_event.set()

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
            self._scheduler_task = None

        self.logger.info("Scheduler stopped")

    def is_connected(self) -> bool:
        """Check if scheduler is running"""
        return self._scheduler_task is not None and not self._scheduler_task.done()

    async def consume(self) -> AsyncIterator[Message]:
        """Consume scheduled messages"""
        if not self.is_connected():
            await self.connect()

        self.logger.info("Starting scheduled consumption")

        try:
            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(self._message_queue.get(), timeout=1.0)
                    yield message
                    self._message_queue.task_done()

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            self.logger.error("Error in scheduled consumption", error=str(e))
            raise
        finally:
            await self.disconnect()

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop"""
        from datetime import datetime, timezone

        next_runs = {}

        # Initialize next run times for all schedules
        now = datetime.now(timezone.utc)
        for cron_expr in self.schedule.keys():
            cron = self.croniter(cron_expr, now)
            next_runs[cron_expr] = cron.get_next(datetime)

        while not self._stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)

                # Check which schedules need to run
                for cron_expr, next_run in next_runs.items():
                    if now >= next_run:
                        # Execute the scheduled poll
                        await self._execute_scheduled_poll(cron_expr)

                        # Calculate next run time
                        cron = self.croniter(cron_expr, now)
                        next_runs[cron_expr] = cron.get_next(datetime)

                # Sleep until next check (1 minute intervals)
                await asyncio.sleep(60)

            except Exception as e:
                self.logger.error("Error in scheduler loop", error=str(e))
                await asyncio.sleep(60)  # Continue after error

    async def _execute_scheduled_poll(self, cron_expr: str) -> None:
        """Execute a scheduled poll"""
        endpoint_config = self.schedule[cron_expr]

        if isinstance(endpoint_config, str):
            # Simple URL
            url = endpoint_config
            poll_config = {"url": url, "method": "GET"}
        else:
            # Full configuration
            poll_config = endpoint_config.copy()

        self.logger.info(f"Executing scheduled poll: {cron_expr}", url=poll_config.get("url"))

        try:
            # Create temporary poller for this execution
            poller = HTTPPoller(
                url=poll_config["url"],
                interval=1,  # Not used for one-time execution
                method=poll_config.get("method", "GET"),
                headers=poll_config.get("headers"),
                params=poll_config.get("params"),
                config=self.base_config,
                **self.kwargs,
            )

            await poller.connect()

            # Execute single poll
            messages = await poller._poll()

            # Add schedule info to messages
            for message in messages:
                message.headers["schedule_expression"] = cron_expr
                message.headers["scheduled_at"] = datetime.now().isoformat()
                await self._message_queue.put(message)

            await poller.disconnect()

            self.logger.debug(
                f"Scheduled poll completed: {cron_expr}",
                url=poll_config["url"],
                messages_count=len(messages),
            )

        except Exception as e:
            self.logger.error(
                f"Error executing scheduled poll: {cron_expr}",
                error=str(e),
                url=poll_config.get("url"),
            )

    async def health_check(self) -> bool:
        """Check scheduler health"""
        return self.is_connected()

    def get_next_runs(self) -> Dict[str, str]:
        """Get next run times for all schedules"""
        from datetime import datetime, timezone

        next_runs = {}
        now = datetime.now(timezone.utc)

        for cron_expr in self.schedule.keys():
            cron = self.croniter(cron_expr, now)
            next_runs[cron_expr] = cron.get_next(datetime).isoformat()

        return next_runs
