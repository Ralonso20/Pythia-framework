"""
Worker lifecycle management - handles startup, shutdown, and signals
"""

import asyncio
import signal
import sys
from typing import Any, Dict, List, Optional, Callable, Awaitable
from datetime import datetime
from enum import Enum

from ..config.base import WorkerConfig


class WorkerState(Enum):
    """Worker state enumeration"""

    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class LifecycleManager:
    """Manages the lifecycle of a Pythia worker"""

    def __init__(self, config: WorkerConfig):
        self.config = config
        self.state = WorkerState.INITIALIZING
        self.running = False
        self._shutdown_requested = False
        self._startup_time: Optional[datetime] = None
        self._shutdown_time: Optional[datetime] = None

        # Lifecycle hooks
        self._startup_hooks: List[Callable[[], Awaitable[None]]] = []
        self._shutdown_hooks: List[Callable[[], Awaitable[None]]] = []
        self._health_check_hooks: List[Callable[[], Awaitable[bool]]] = []

        # Signal handlers
        self._original_handlers = {}

    def add_startup_hook(self, hook: Callable[[], Awaitable[None]]) -> None:
        """Add a startup hook"""
        self._startup_hooks.append(hook)

    def add_shutdown_hook(self, hook: Callable[[], Awaitable[None]]) -> None:
        """Add a shutdown hook"""
        self._shutdown_hooks.append(hook)

    def add_health_check_hook(self, hook: Callable[[], Awaitable[bool]]) -> None:
        """Add a health check hook"""
        self._health_check_hooks.append(hook)

    def setup_signal_handlers(self) -> None:
        """Configure signal handlers for graceful shutdown"""
        if sys.platform == "win32":
            # Windows doesn't support SIGTERM
            signals = [signal.SIGINT]
        else:
            signals = [signal.SIGINT, signal.SIGTERM]

        for sig in signals:
            self._original_handlers[sig] = signal.signal(sig, self._signal_handler)

    def restore_signal_handlers(self) -> None:
        """Restore original signal handlers"""
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)
        self._original_handlers.clear()

    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals"""
        print(f"\n🛑 Received signal {signum}, initiating graceful shutdown...")
        self.request_shutdown()

    async def startup(self) -> None:
        """Execute startup sequence"""
        try:
            self.state = WorkerState.STARTING
            self._startup_time = datetime.utcnow()

            print(f"🚀 Starting worker: {self.config.worker_id}")

            # Execute startup hooks
            for hook in self._startup_hooks:
                await hook()

            self.running = True
            self.state = WorkerState.RUNNING

            print(f"✅ Worker started successfully: {self.config.worker_id}")

        except Exception as e:
            self.state = WorkerState.ERROR
            print(f"❌ Failed to start worker: {e}")
            raise

    async def shutdown(self) -> None:
        """Execute shutdown sequence"""
        if self.state in [WorkerState.STOPPING, WorkerState.STOPPED]:
            return

        try:
            self.state = WorkerState.STOPPING
            self._shutdown_time = datetime.utcnow()

            print(f"🛑 Shutting down worker: {self.config.worker_id}")

            # Execute shutdown hooks in reverse order
            for hook in reversed(self._shutdown_hooks):
                try:
                    await hook()
                except Exception as e:
                    print(f"⚠️  Error in shutdown hook: {e}")

            self.running = False
            self.state = WorkerState.STOPPED

            # Calculate uptime
            uptime = self._calculate_uptime()
            print(f"✅ Worker shutdown complete. Uptime: {uptime}")

        except Exception as e:
            self.state = WorkerState.ERROR
            print(f"❌ Error during shutdown: {e}")
            raise
        finally:
            self.restore_signal_handlers()

    def request_shutdown(self) -> None:
        """Request graceful shutdown"""
        if not self._shutdown_requested:
            self._shutdown_requested = True
            self.running = False
            print(f"🛑 Graceful shutdown requested for worker: {self.config.worker_id}")

    def is_shutdown_requested(self) -> bool:
        """Check if shutdown was requested"""
        return self._shutdown_requested

    def is_running(self) -> bool:
        """Check if worker is running"""
        return self.running and not self._shutdown_requested

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        health_status = {
            "worker_id": self.config.worker_id,
            "state": self.state.value,
            "running": self.running,
            "uptime_seconds": self._calculate_uptime_seconds(),
            "checks": {},
        }

        overall_healthy = True

        # Execute health check hooks
        for i, hook in enumerate(self._health_check_hooks):
            try:
                check_result = await hook()
                health_status["checks"][f"check_{i}"] = {
                    "healthy": check_result,
                    "timestamp": datetime.utcnow().isoformat(),
                }
                if not check_result:
                    overall_healthy = False
            except Exception as e:
                health_status["checks"][f"check_{i}"] = {
                    "healthy": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                overall_healthy = False

        health_status["healthy"] = overall_healthy and self.running
        return health_status

    def get_stats(self) -> Dict[str, Any]:
        """Get lifecycle statistics"""
        return {
            "worker_id": self.config.worker_id,
            "worker_name": self.config.worker_name,
            "state": self.state.value,
            "running": self.running,
            "shutdown_requested": self._shutdown_requested,
            "startup_time": self._startup_time.isoformat() if self._startup_time else None,
            "shutdown_time": self._shutdown_time.isoformat() if self._shutdown_time else None,
            "uptime_seconds": self._calculate_uptime_seconds(),
            "uptime_human": self._calculate_uptime(),
        }

    def _calculate_uptime_seconds(self) -> Optional[float]:
        """Calculate uptime in seconds"""
        if not self._startup_time:
            return None

        end_time = self._shutdown_time or datetime.utcnow()
        delta = end_time - self._startup_time
        return delta.total_seconds()

    def _calculate_uptime(self) -> str:
        """Calculate human-readable uptime"""
        uptime_seconds = self._calculate_uptime_seconds()
        if uptime_seconds is None:
            return "Unknown"

        hours, remainder = divmod(int(uptime_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)

        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"


class WorkerRunner:
    """High-level worker runner with lifecycle management"""

    def __init__(self, worker_instance, config: Optional[WorkerConfig] = None):
        self.worker = worker_instance
        self.config = config or WorkerConfig()
        self.lifecycle = LifecycleManager(self.config)
        self._setup_lifecycle_hooks()

    def _setup_lifecycle_hooks(self):
        """Setup default lifecycle hooks"""

        async def startup_hook():
            if hasattr(self.worker, "startup"):
                await self.worker.startup()

        async def shutdown_hook():
            if hasattr(self.worker, "shutdown"):
                await self.worker.shutdown()

        async def health_check_hook():
            if hasattr(self.worker, "health_check"):
                return await self.worker.health_check()
            return True

        self.lifecycle.add_startup_hook(startup_hook)
        self.lifecycle.add_shutdown_hook(shutdown_hook)
        self.lifecycle.add_health_check_hook(health_check_hook)

    async def run(self) -> None:
        """Run the worker with full lifecycle management"""

        # Setup signal handlers
        self.lifecycle.setup_signal_handlers()

        try:
            # Startup sequence
            await self.lifecycle.startup()

            # Main execution loop
            if hasattr(self.worker, "run"):
                # Worker has its own run method
                while self.lifecycle.is_running():
                    try:
                        await self.worker.run()
                        if not hasattr(self.worker, "_continuous") or not self.worker._continuous:
                            break
                    except KeyboardInterrupt:
                        self.lifecycle.request_shutdown()
                        break
                    except Exception as e:
                        print(f"❌ Error in worker execution: {e}")
                        if (
                            not self.config.max_retries
                            or self.worker.retry_count >= self.config.max_retries
                        ):
                            break
                        await asyncio.sleep(self.config.retry_delay)
            else:
                # Worker doesn't have run method, just wait for shutdown
                while self.lifecycle.is_running():
                    await asyncio.sleep(1)

        except KeyboardInterrupt:
            print("\n🛑 Interrupted by user")
        except Exception as e:
            print(f"❌ Worker failed: {e}")
            raise
        finally:
            # Shutdown sequence
            await self.lifecycle.shutdown()

    def run_sync(self) -> None:
        """Run the worker synchronously"""
        try:
            asyncio.run(self.run())
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            sys.exit(1)
