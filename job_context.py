"""
Per-job context for Celery-safe tracing and token accounting.

Uses a contextvar for the active job_id within a task/thread and a shared
registry for Langfuse parent traces keyed by job_id.
"""

from __future__ import annotations

import threading
from contextvars import ContextVar
from typing import Any, Optional

_job_id_var: ContextVar[Optional[str]] = ContextVar("job_id", default=None)

_registry_lock = threading.Lock()
_job_registry: dict[str, dict[str, Any]] = {}


def set_job_context(job_id: str) -> None:
    _job_id_var.set(job_id)


def get_job_context() -> Optional[str]:
    return _job_id_var.get()


def register_job_trace(
    job_id: str,
    *,
    trace: Any,
    request_id: str,
    user_name: str,
) -> None:
    with _registry_lock:
        _job_registry[job_id] = {
            "trace": trace,
            "request_id": request_id,
            "user_name": user_name,
        }


def get_job_trace_entry(job_id: Optional[str] = None) -> Optional[dict[str, Any]]:
    jid = job_id or get_job_context()
    if not jid:
        return None
    with _registry_lock:
        return _job_registry.get(jid)


def clear_job_trace(job_id: Optional[str] = None) -> None:
    jid = job_id or get_job_context()
    if not jid:
        return
    with _registry_lock:
        _job_registry.pop(jid, None)


def clear_job_context() -> None:
    _job_id_var.set(None)
