"""
Job data structures and status management
"""

import json
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Job status enumeration"""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class JobPriority(str, Enum):
    """Job priority levels"""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class JobResult:
    """Job execution result"""

    success: bool
    result: Any = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    execution_time: Optional[float] = None
    output: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "success": self.success,
            "result": self.result,
            "error": self.error,
            "error_type": self.error_type,
            "execution_time": self.execution_time,
            "output": self.output,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobResult":
        """Create from dictionary"""
        return cls(**data)


class Job(BaseModel):
    """
    Job definition for background processing

    Example:
        job = Job(
            name="send_email",
            func="myapp.tasks.send_email",
            args=["user@example.com"],
            kwargs={"subject": "Welcome!"},
            priority=JobPriority.HIGH,
            max_retries=3,
        )
    """

    # Job identification
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Job name")
    description: Optional[str] = Field(default=None, description="Job description")

    # Job execution
    func: str = Field(description="Function to execute (module.function)")
    args: list = Field(default_factory=list, description="Positional arguments")
    kwargs: Dict[str, Any] = Field(default_factory=dict, description="Keyword arguments")

    # Job metadata
    priority: JobPriority = Field(default=JobPriority.NORMAL, description="Job priority")
    tags: list[str] = Field(default_factory=list, description="Job tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    # Scheduling and timing
    scheduled_at: Optional[datetime] = Field(default=None, description="When to run the job")
    expires_at: Optional[datetime] = Field(default=None, description="When the job expires")
    timeout: Optional[float] = Field(default=None, description="Job timeout in seconds")

    # Retry configuration
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: float = Field(default=1.0, description="Delay between retries")
    retry_backoff: float = Field(default=2.0, description="Exponential backoff multiplier")
    retry_jitter: bool = Field(default=True, description="Add jitter to retry delays")

    # Job status and tracking
    status: JobStatus = Field(default=JobStatus.PENDING, description="Current status")
    created_at: datetime = Field(default_factory=datetime.now, description="Creation time")
    started_at: Optional[datetime] = Field(default=None, description="Start time")
    completed_at: Optional[datetime] = Field(default=None, description="Completion time")

    # Results and errors
    current_retry: int = Field(default=0, description="Current retry attempt")
    last_error: Optional[str] = Field(default=None, description="Last error message")
    result: Optional[JobResult] = Field(default=None, description="Job result")

    # Worker tracking
    worker_id: Optional[str] = Field(default=None, description="ID of processing worker")
    queue_name: Optional[str] = Field(default="default", description="Queue name")

    def is_scheduled(self) -> bool:
        """Check if job is scheduled for future execution"""
        return self.scheduled_at is not None and self.scheduled_at > datetime.now()

    def is_expired(self) -> bool:
        """Check if job has expired"""
        return self.expires_at is not None and self.expires_at < datetime.now()

    def can_retry(self) -> bool:
        """Check if job can be retried"""
        return self.current_retry < self.max_retries and self.status == JobStatus.FAILED

    def get_next_retry_at(self) -> datetime:
        """Calculate next retry time"""
        if not self.can_retry():
            raise ValueError("Job cannot be retried")

        delay = self.retry_delay * (self.retry_backoff**self.current_retry)

        # Add jitter to prevent thundering herd
        if self.retry_jitter:
            import random

            delay *= random.uniform(0.5, 1.5)

        return datetime.now() + timedelta(seconds=delay)

    def mark_started(self, worker_id: str) -> None:
        """Mark job as started"""
        self.status = JobStatus.RUNNING
        self.started_at = datetime.now()
        self.worker_id = worker_id

    def mark_completed(self, result: JobResult) -> None:
        """Mark job as completed"""
        self.status = JobStatus.COMPLETED if result.success else JobStatus.FAILED
        self.completed_at = datetime.now()
        self.result = result

        if not result.success and result.error:
            self.last_error = result.error

    def mark_retry(self) -> None:
        """Mark job for retry"""
        self.current_retry += 1
        self.status = JobStatus.RETRYING
        # Don't reset worker_id - keep track of which worker is handling retries

    def mark_cancelled(self) -> None:
        """Mark job as cancelled"""
        self.status = JobStatus.CANCELLED
        self.completed_at = datetime.now()

    def mark_expired(self) -> None:
        """Mark job as expired"""
        self.status = JobStatus.EXPIRED
        self.completed_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for serialization"""
        data = self.model_dump()

        # Convert datetime objects to ISO format
        for field_name in [
            "created_at",
            "started_at",
            "completed_at",
            "scheduled_at",
            "expires_at",
        ]:
            if data.get(field_name):
                data[field_name] = data[field_name].isoformat()

        # Convert result to dict
        if data.get("result") and isinstance(data["result"], JobResult):
            data["result"] = data["result"].to_dict()

        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """Create job from dictionary"""
        # Parse datetime fields
        for field_name in [
            "created_at",
            "started_at",
            "completed_at",
            "scheduled_at",
            "expires_at",
        ]:
            if data.get(field_name) and isinstance(data[field_name], str):
                data[field_name] = datetime.fromisoformat(data[field_name])

        # Parse result
        if data.get("result") and isinstance(data["result"], dict):
            data["result"] = JobResult.from_dict(data["result"])

        return cls(**data)

    def to_json(self) -> str:
        """Serialize job to JSON"""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_json(cls, json_str: str) -> "Job":
        """Deserialize job from JSON"""
        data = json.loads(json_str)
        return cls.from_dict(data)

    def get_execution_time(self) -> Optional[float]:
        """Get job execution time in seconds"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def get_queue_time(self) -> Optional[float]:
        """Get time spent in queue before execution"""
        if self.created_at and self.started_at:
            return (self.started_at - self.created_at).total_seconds()
        return None

    def add_tag(self, tag: str) -> None:
        """Add a tag to the job"""
        if tag not in self.tags:
            self.tags.append(tag)

    def remove_tag(self, tag: str) -> None:
        """Remove a tag from the job"""
        if tag in self.tags:
            self.tags.remove(tag)

    def has_tag(self, tag: str) -> bool:
        """Check if job has a specific tag"""
        return tag in self.tags

    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata value"""
        self.metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value"""
        return self.metadata.get(key, default)

    def __str__(self) -> str:
        return f"Job({self.id}, {self.name}, {self.status.value})"

    def __repr__(self) -> str:
        return (
            f"Job(id={self.id!r}, name={self.name!r}, "
            f"status={self.status.value!r}, priority={self.priority.value!r})"
        )
