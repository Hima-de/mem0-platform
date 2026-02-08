"""
Celery Task Queue Configuration for Production.

Provides distributed task processing for:
- Memory embedding generation
- Importance score decay
- Analytics aggregation
- Cleanup jobs
- Snapshot compression
"""

import os
import logging
from celery import Celery
from celery.schedules import crontab
from kombu import Queue

logger = logging.getLogger(__name__)

# Celery configuration
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/2")
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TIMEZONE = "UTC"
CELERY_ENABLE_UTC = True
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 300  # 5 minutes
CELERY_TASK_SOFT_TIME_LIMIT = 240  # 4 minutes
CELERY_WORKER_PREFETCH_MULTIPLIER = 4
CELERY_WORKER_CONCURRENCY = int(os.getenv("CELERY_CONCURRENCY", 8))

# Create Celery app
celery_app = Celery(
    "mem0_platform",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=[
        "tasks.memory",
        "tasks.embeddings",
        "tasks.analytics",
        "tasks.cleanup",
        "tasks.snapshots",
    ],
)

# Celery configuration
celery_app.conf.update(
    broker_url=CELERY_BROKER_URL,
    result_backend=CELERY_RESULT_BACKEND,
    task_serializer=CELERY_TASK_SERIALIZER,
    result_serializer=CELERY_RESULT_SERIALIZER,
    accept_content=CELERY_ACCEPT_CONTENT,
    timezone=CELERY_TIMEZONE,
    enable_utc=CELERY_ENABLE_UTC,
    task_track_started=CELERY_TASK_TRACKED,
    task_time_limit=CELERY_TASK_TIME_LIMIT,
    task_soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    worker_prefetch_multiplier=CELERY_WORKER_PREFETCH_MULTIPLIER,
    worker_concurrency=CELERY_WORKER_CONCURRENCY,
    task_queues={
        "default": {
            "exchange": "default",
            "routing_key": "default",
            "queue_arguments": {"x-max-priority": 10},
        },
        "high_priority": {
            "exchange": "high_priority",
            "routing_key": "high_priority",
            "queue_arguments": {"x-max-priority": 10},
        },
        "embeddings": {
            "exchange": "embeddings",
            "routing_key": "embeddings",
            "queue_arguments": {"x-max-priority": 10},
        },
        "cleanup": {
            "exchange": "cleanup",
            "routing_key": "cleanup",
            "queue_arguments": {"x-max-priority": 1},
        },
        "analytics": {
            "exchange": "analytics",
            "routing_key": "analytics",
        },
    },
    task_routes={
        "tasks.embeddings.*": {"queue": "embeddings"},
        "tasks.cleanup.*": {"queue": "cleanup"},
        "tasks.analytics.*": {"queue": "analytics"},
        "tasks.memory.generate_embedding": {"queue": "high_priority"},
        "tasks.memory.decay_importance": {"queue": "cleanup"},
    },
    task_defaults={
        "default_priority": 5,
        "high_priority_priority": 9,
    },
    beat_schedule={
        "decay-importance-daily": {
            "task": "tasks.memory.decay_importance",
            "schedule": crontab(hour=2, minute=0),
            "options": {"queue": "cleanup"},
        },
        "cleanup-expired-sessions": {
            "task": "tasks.cleanup.cleanup_expired_sessions",
            "schedule": crontab(hour=3, minute=0),
            "options": {"queue": "cleanup"},
        },
        "aggregate-analytics-hourly": {
            "task": "tasks.analytics.aggregate_hourly",
            "schedule": crontab(minute=5),
            "options": {"queue": "analytics"},
        },
        "archive-old-memories": {
            "task": "tasks.cleanup.archive_old_memories",
            "schedule": crontab(hour=4, minute=0),
            "options": {"queue": "cleanup"},
        },
        "refresh-user-quotas": {
            "task": "tasks.analytics.refresh_user_quotas",
            "schedule": crontab(hour=0, minute=0),
            "options": {"queue": "cleanup"},
        },
    },
)


# Health check endpoint
@celery_app.route("/health")
def health_check():
    """Health check endpoint for Celery."""
    return {"status": "healthy"}


if __name__ == "__main__":
    celery_app.start()
