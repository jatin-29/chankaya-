"""
Celery application for durable paper-processing jobs.
"""

from celery import Celery
from celery.signals import worker_ready

from app.core.config import (
    BACKGROUND_TASKS_SYNC,
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    CELERY_TASK_DEFAULT_QUEUE,
    CELERY_TASK_SOFT_TIME_LIMIT,
    CELERY_TASK_TIME_LIMIT,
)
from app.db.database import create_tables

app = Celery("question_paper_extraction")

app.conf.update(
    broker_url=CELERY_BROKER_URL,
    result_backend=CELERY_RESULT_BACKEND,
    task_default_queue=CELERY_TASK_DEFAULT_QUEUE,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    task_time_limit=CELERY_TASK_TIME_LIMIT,
    broker_connection_retry_on_startup=True,
    task_always_eager=BACKGROUND_TASKS_SYNC,
    task_eager_propagates=True,
)

# Explicit import list — autodiscover_tasks(["app.tasks"]) looks for
# app.tasks.tasks (missing); register paper_tasks directly instead.
app.conf.imports = ("app.tasks.paper_tasks",)


@worker_ready.connect
def _init_worker_db(**_kwargs) -> None:
    create_tables()
