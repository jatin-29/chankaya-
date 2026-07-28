"""
Celery tasks for paper extraction pipeline stages.
"""

from __future__ import annotations

import logging

from app.core.celery_app import app
from app.core.job_context import set_job_context
from app.db import database as db
from app.models.schemas import UploadPaperRequest
from app.pipeline.answer_generator import (
    generate_answers_for_batch,
    normalize_answer_list,
)
from app.pipeline.question_extractor import run_phase2_extract_one
from app.services.paper_worker import process_paper

logger = logging.getLogger("app.paper_tasks")


@app.task(
    name="paper.process_paper",
    bind=True,
    autoretry_for=(),
    max_retries=0,
)
def process_paper_task(self, job_id: str) -> None:
    """Load job payload from SQLite and run the full pipeline."""
    job = db.get_job(job_id)
    if job is None:
        raise ValueError(f"Job '{job_id}' not found.")

    req = UploadPaperRequest.model_validate(job["request_payload"])
    logger.info(f"[job:{job_id}] Celery task started (task_id={self.request.id})")
    process_paper(job_id, req)


@app.task(
    name="paper.process_chunk_p2",
    autoretry_for=(),
    max_retries=0,
)
def process_chunk_p2_task(job_id: str, chunk: dict, config: dict) -> dict:
    """Run Phase 2 (question extraction) for a single chunk. Phase 3 is fanned out separately."""
    set_job_context(job_id)
    return run_phase2_extract_one(chunk, config)


# Keep legacy name registered so in-flight queued messages don't explode workers.
@app.task(
    name="paper.process_chunk_p2_p3",
    autoretry_for=(),
    max_retries=0,
)
def process_chunk_p2_p3_task(job_id: str, chunk: dict, config: dict) -> dict:
    """Deprecated alias — Phase 2 only (Phase 3 runs at parent level)."""
    return process_chunk_p2_task(job_id, chunk, config)


@app.task(
    name="paper.process_answer_batch",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    autoretry_for=(),
)
def process_answer_batch_task(
    self,
    job_id: str,
    questions_batch: list,
    config: dict,
    chunk_index,
    qtype: str,
    batch_num: int,
    num_batches: int,
) -> dict:
    """
    Generate answers for one type-based question batch.

    Retries up to 3 times on failure. After retries are exhausted, returns a
    failed payload instead of raising so sibling batches can still complete.
    """
    set_job_context(job_id)
    label = (
        f"Phase 3 - Chunk {chunk_index} - Answer Generation "
        f"[{qtype}] batch {batch_num}/{num_batches}"
    )
    try:
        answers = generate_answers_for_batch(
            questions_batch,
            config,
            chunk_index,
            qtype,
            batch_num,
            num_batches,
        )
        answers = normalize_answer_list(answers)
        return {
            "status": "ok",
            "answers": answers,
            "chunk_index": chunk_index,
            "qtype": qtype,
            "batch_num": batch_num,
            "num_batches": num_batches,
            "error": None,
        }
    except Exception as exc:
        attempt = self.request.retries + 1
        max_attempts = (self.max_retries or 0) + 1
        if self.request.retries < (self.max_retries or 0):
            logger.warning(
                f"[{label}] Failed on attempt {attempt}/{max_attempts}: {exc} — retrying..."
            )
            raise self.retry(exc=exc, countdown=min(2 ** self.request.retries, 30))

        logger.error(
            f"[{label}] Failed after {max_attempts} attempt(s): {exc} — "
            "continuing without this batch."
        )
        return {
            "status": "failed",
            "answers": [],
            "chunk_index": chunk_index,
            "qtype": qtype,
            "batch_num": batch_num,
            "num_batches": num_batches,
            "error": f"{type(exc).__name__}: {exc}",
        }
