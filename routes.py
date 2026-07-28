"""
API routes for the paper extraction service.
"""

import logging
import uuid

from fastapi import APIRouter, HTTPException
from kombu.exceptions import OperationalError

from app.db import database as db
from app.models.schemas import JobStatusResponse, UploadPaperRequest
from app.tasks.dispatch import dispatch_task
from app.tasks.paper_tasks import process_paper_task

logger = logging.getLogger("app.routes")
router = APIRouter()


@router.post("/upload-paper/", status_code=202)
def upload_paper(payload: UploadPaperRequest):
    """
    Accept a paper submission and kick off the full background pipeline:
    OCR -> image upload -> Phase 1 (relevance+chunk) -> Phase 2 (extract)
    -> Phase 3 (answers) -> merge -> auto-repair -> create-paper/.
    Returns immediately with a job_id for polling.
    """
    if not payload.question_paper_url:
        raise HTTPException(
            status_code=400,
            detail="question_paper_url must contain at least one entry.",
        )

    job_id = str(uuid.uuid4())
    logger.info(f"[job:{job_id}] Received upload-paper request for exam_id={payload.exam_id}")

    db.insert_job(job_id, payload.exam_id, payload.model_dump())

    try:
        dispatch_task(process_paper_task, job_id, task_id=job_id)
    except OperationalError as exc:
        logger.error(f"[job:{job_id}] Failed to enqueue Celery task: {exc}")
        db.update_job_failed(job_id, f"Failed to enqueue background task: {exc}")
        raise HTTPException(
            status_code=503,
            detail="Background task broker unavailable. Job marked failed.",
        ) from exc
    except Exception as exc:
        logger.error(f"[job:{job_id}] Failed to dispatch background task: {exc}")
        db.update_job_failed(job_id, f"Failed to dispatch background task: {exc}")
        raise HTTPException(
            status_code=503,
            detail="Failed to start background processing. Job marked failed.",
        ) from exc

    return {"job_id": job_id, "status": "processing", "exam_id": payload.exam_id}


@router.get("/job-status/{job_id}", response_model=JobStatusResponse)
def job_status(job_id: str):
    """
    Return the current status of a single job. Read-only — does not
    trigger or affect processing in any way.
    Status values: processing | ocr | uploading_images | extracting | saving | done | failed
    """
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return job
