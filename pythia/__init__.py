"""
🐍 Pythia - Python Worker Framework

A modern library for creating efficient and scalable workers in Python
"""

from pythia.core.worker import Worker
from pythia.core.message import Message
from pythia.http import WebhookClient, HTTPPoller, MultiHTTPPoller, ScheduledHTTPPoller
from pythia.jobs import (
    BackgroundJobWorker,
    ScheduledWorker,
    Job,
    JobStatus,
    JobPriority,
    submit_job,
    job_function,
    scheduled_task,
    every,
    cron,
    with_retry,
    CommonRetryPolicies,
)
from pythia.config import (
    WorkerConfig,
    auto_detect_config,
    BrokerFactory,
    BrokerSwitcher,
    create_consumer,
    create_producer,
)
from pythia.brokers.database import (
    DatabaseWorker,
    CDCWorker,
    SyncWorker,
    DatabaseChange,
    ChangeType,
)

__version__ = "0.1.0"
__all__ = [
    # Core framework
    "Worker",
    "Message",
    "WorkerConfig",
    "auto_detect_config",
    # HTTP utilities
    "WebhookClient",
    "HTTPPoller",
    "MultiHTTPPoller",
    "ScheduledHTTPPoller",
    # Job system
    "BackgroundJobWorker",
    "ScheduledWorker",
    "Job",
    "JobStatus",
    "JobPriority",
    "submit_job",
    "job_function",
    "scheduled_task",
    "every",
    "cron",
    "with_retry",
    "CommonRetryPolicies",
    # Broker utilities
    "BrokerFactory",
    "BrokerSwitcher",
    "create_consumer",
    "create_producer",
    # Database workers
    "DatabaseWorker",
    "CDCWorker",
    "SyncWorker",
    "DatabaseChange",
    "ChangeType",
]
