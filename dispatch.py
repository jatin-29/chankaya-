"""
Dispatch helpers — route tasks synchronously or through Celery based on config.
"""

from __future__ import annotations

import logging
from typing import Any

from celery import group
from celery.result import GroupResult

from app.core import config

logger = logging.getLogger("app.dispatch")


def dispatch_task(task, *args, task_id: str | None = None, **kwargs) -> Any:
    """Run a task in-process (sync mode) or enqueue to Celery."""
    if config.BACKGROUND_TASKS_SYNC:
        return task.apply(args=args, kwargs=kwargs)
    return task.apply_async(args=args, kwargs=kwargs, task_id=task_id)


def _forget_group_result(group_result: GroupResult) -> None:
    """Drop result-backend entries so completed task metadata does not linger."""
    try:
        for child in list(getattr(group_result, "results", None) or []):
            try:
                child.forget()
            except Exception as exc:
                logger.debug(
                    f"Could not forget child result {getattr(child, 'id', '?')}: {exc}"
                )
        group_result.forget()
    except Exception as exc:
        logger.warning(f"Could not clean up Celery group results: {exc}")


def run_task_group(group_sig: group, timeout: int | None = None) -> list[Any]:
    """
    Execute a Celery group and return collected results in submission order.
    In sync mode runs eagerly in-process; in celery mode waits on the group.
    Always forgets backend result metadata after collection.
    """
    if config.BACKGROUND_TASKS_SYNC:
        group_result = group_sig.apply()
    else:
        group_result = group_sig.apply_async()

    try:
        return group_result.get(
            timeout=timeout,
            propagate=True,
            disable_sync_subtasks=False,
        )
    finally:
        _forget_group_result(group_result)
