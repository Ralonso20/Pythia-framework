"""
Job execution engines for Pythia
"""

import asyncio
import importlib
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Any, Callable, Dict, Optional

from .job import Job, JobResult
from ..logging import get_pythia_logger


class JobExecutor(ABC):
    """Abstract base class for job executors"""

    @abstractmethod
    async def execute(self, job: Job) -> JobResult:
        """Execute a job and return the result"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the executor and cleanup resources"""
        pass


class AsyncJobExecutor(JobExecutor):
    """Executor for async functions"""

    def __init__(self, timeout: Optional[float] = None):
        self.timeout = timeout
        self.logger = get_pythia_logger("AsyncJobExecutor")

    async def execute(self, job: Job) -> JobResult:
        """Execute an async job"""
        start_time = time.time()

        try:
            # Import the function
            func = self._import_function(job.func)

            # Check if function is async
            if not asyncio.iscoroutinefunction(func):
                return JobResult(
                    success=False,
                    error="Function is not async",
                    error_type="InvalidFunction",
                )

            # Execute with timeout
            timeout = job.timeout or self.timeout

            if timeout:
                result = await asyncio.wait_for(func(*job.args, **job.kwargs), timeout=timeout)
            else:
                result = await func(*job.args, **job.kwargs)

            execution_time = time.time() - start_time

            return JobResult(success=True, result=result, execution_time=execution_time)

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            return JobResult(
                success=False,
                error="Job timed out",
                error_type="TimeoutError",
                execution_time=execution_time,
            )

        except Exception as e:
            execution_time = time.time() - start_time
            return JobResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__,
                execution_time=execution_time,
            )

    async def close(self) -> None:
        """Nothing to close for async executor"""
        pass

    def _import_function(self, func_path: str) -> Callable:
        """Import a function from a module path"""
        try:
            module_path, func_name = func_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, func_name)
        except (ValueError, ImportError, AttributeError) as e:
            raise ImportError(f"Could not import function {func_path}: {e}")


class ThreadPoolJobExecutor(JobExecutor):
    """Executor for sync functions using thread pool"""

    def __init__(self, max_workers: Optional[int] = None, timeout: Optional[float] = None):
        self.max_workers = max_workers
        self.timeout = timeout
        self.logger = get_pythia_logger("ThreadPoolJobExecutor")

        self._executor: Optional[ThreadPoolExecutor] = None

    @property
    def executor(self) -> ThreadPoolExecutor:
        """Get thread pool executor (lazy initialization)"""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        return self._executor

    async def execute(self, job: Job) -> JobResult:
        """Execute a sync job in thread pool"""
        start_time = time.time()

        try:
            # Import the function
            func = self._import_function(job.func)

            # Check if function is sync
            if asyncio.iscoroutinefunction(func):
                return JobResult(
                    success=False,
                    error="Function is async, use AsyncJobExecutor",
                    error_type="InvalidFunction",
                )

            # Execute in thread pool with timeout
            timeout = job.timeout or self.timeout

            loop = asyncio.get_event_loop()
            future = loop.run_in_executor(self.executor, lambda: func(*job.args, **job.kwargs))

            if timeout:
                result = await asyncio.wait_for(future, timeout=timeout)
            else:
                result = await future

            execution_time = time.time() - start_time

            return JobResult(success=True, result=result, execution_time=execution_time)

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            return JobResult(
                success=False,
                error="Job timed out",
                error_type="TimeoutError",
                execution_time=execution_time,
            )

        except Exception as e:
            execution_time = time.time() - start_time
            return JobResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__,
                execution_time=execution_time,
            )

    async def close(self) -> None:
        """Shutdown thread pool"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
            self.logger.info("Thread pool executor closed")

    def _import_function(self, func_path: str) -> Callable:
        """Import a function from a module path"""
        try:
            module_path, func_name = func_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, func_name)
        except (ValueError, ImportError, AttributeError) as e:
            raise ImportError(f"Could not import function {func_path}: {e}")


class ProcessPoolJobExecutor(JobExecutor):
    """Executor for CPU-intensive sync functions using process pool"""

    def __init__(self, max_workers: Optional[int] = None, timeout: Optional[float] = None):
        self.max_workers = max_workers
        self.timeout = timeout
        self.logger = get_pythia_logger("ProcessPoolJobExecutor")

        self._executor: Optional[ProcessPoolExecutor] = None

    @property
    def executor(self) -> ProcessPoolExecutor:
        """Get process pool executor (lazy initialization)"""
        if self._executor is None:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self._executor

    async def execute(self, job: Job) -> JobResult:
        """Execute a job in process pool"""
        start_time = time.time()

        try:
            # Validate function path (process pool needs importable functions)
            self._validate_function_path(job.func)

            # Execute in process pool
            timeout = job.timeout or self.timeout

            loop = asyncio.get_event_loop()
            future = loop.run_in_executor(
                self.executor, self._execute_in_process, job.func, job.args, job.kwargs
            )

            if timeout:
                result = await asyncio.wait_for(future, timeout=timeout)
            else:
                result = await future

            execution_time = time.time() - start_time

            return JobResult(success=True, result=result, execution_time=execution_time)

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            return JobResult(
                success=False,
                error="Job timed out",
                error_type="TimeoutError",
                execution_time=execution_time,
            )

        except Exception as e:
            execution_time = time.time() - start_time
            return JobResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__,
                execution_time=execution_time,
            )

    async def close(self) -> None:
        """Shutdown process pool"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
            self.logger.info("Process pool executor closed")

    def _validate_function_path(self, func_path: str) -> None:
        """Validate function path for process pool execution"""
        try:
            module_path, func_name = func_path.rsplit(".", 1)
            # Try to import to validate
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)

            # Check if it's async (not supported in process pool)
            if asyncio.iscoroutinefunction(func):
                raise ValueError("Async functions not supported in process pool")

        except (ValueError, ImportError, AttributeError) as e:
            raise ImportError(f"Could not validate function {func_path}: {e}")

    @staticmethod
    def _execute_in_process(func_path: str, args: list, kwargs: dict) -> Any:
        """Execute function in separate process (static method for pickling)"""
        try:
            module_path, func_name = func_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)
            return func(*args, **kwargs)
        except Exception as e:
            raise RuntimeError(f"Process execution failed: {e}")


class HybridJobExecutor(JobExecutor):
    """
    Hybrid executor that automatically chooses the right execution method
    based on function type and job configuration
    """

    def __init__(
        self,
        thread_pool_workers: Optional[int] = None,
        process_pool_workers: Optional[int] = None,
        default_timeout: Optional[float] = None,
    ):
        self.default_timeout = default_timeout
        self.logger = get_pythia_logger("HybridJobExecutor")

        # Initialize sub-executors
        self._async_executor = AsyncJobExecutor(timeout=default_timeout)
        self._thread_executor = ThreadPoolJobExecutor(
            max_workers=thread_pool_workers, timeout=default_timeout
        )
        self._process_executor = ProcessPoolJobExecutor(
            max_workers=process_pool_workers, timeout=default_timeout
        )

        # Function cache to avoid repeated imports
        self._function_cache: Dict[str, Callable] = {}

    async def execute(self, job: Job) -> JobResult:
        """Execute job using the most appropriate executor"""

        try:
            # Determine execution method
            executor = await self._choose_executor(job)

            self.logger.debug(
                f"Executing job {job.id} with {executor.__class__.__name__}",
                job_name=job.name,
                func=job.func,
            )

            # Execute the job
            return await executor.execute(job)

        except Exception as e:
            return JobResult(success=False, error=str(e), error_type=type(e).__name__)

    async def _choose_executor(self, job: Job) -> JobExecutor:
        """Choose the appropriate executor for a job"""

        # Check job metadata for executor hint
        executor_hint = job.get_metadata("executor")
        if executor_hint == "async":
            return self._async_executor
        elif executor_hint == "thread":
            return self._thread_executor
        elif executor_hint == "process":
            return self._process_executor

        # Analyze function to determine executor
        try:
            func = self._get_function(job.func)

            # Async functions use async executor
            if asyncio.iscoroutinefunction(func):
                return self._async_executor

            # Check for CPU-intensive hint
            if job.has_tag("cpu-intensive") or job.get_metadata("cpu_intensive"):
                return self._process_executor

            # Default to thread executor for sync functions
            return self._thread_executor

        except Exception as e:
            self.logger.warning(
                f"Could not analyze function {job.func}, defaulting to thread executor",
                error=str(e),
            )
            return self._thread_executor

    def _get_function(self, func_path: str) -> Callable:
        """Get function from cache or import it"""
        if func_path not in self._function_cache:
            try:
                module_path, func_name = func_path.rsplit(".", 1)
                module = importlib.import_module(module_path)
                func = getattr(module, func_name)
                self._function_cache[func_path] = func
            except (ValueError, ImportError, AttributeError) as e:
                raise ImportError(f"Could not import function {func_path}: {e}")

        return self._function_cache[func_path]

    async def close(self) -> None:
        """Close all sub-executors"""
        await self._async_executor.close()
        await self._thread_executor.close()
        await self._process_executor.close()

        self._function_cache.clear()
        self.logger.info("Hybrid executor closed")

    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics"""
        return {
            "cached_functions": len(self._function_cache),
            "thread_pool_active": self._thread_executor._executor is not None,
            "process_pool_active": self._process_executor._executor is not None,
        }
