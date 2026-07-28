"""
Central configuration — constants, file paths, and environment-variable keys.
All modules import from here; nothing is hardcoded elsewhere.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).parent.parent.parent  # project root
APP_DIR = Path(__file__).parent.parent  # app/
DATA_DIR = Path(os.getenv("DATA_DIR", str(ROOT_DIR / "data")))
PROMPTS_DIR = APP_DIR / "pipeline" / "prompts"

# Local prompt fallbacks (used only when Langfuse fetch fails)
SPLITTER_PROMPT_FILE = str(PROMPTS_DIR / "relevance_and_chunking.md")
EXTRACTOR_PROMPT_FILE = str(PROMPTS_DIR / "question_extractor.md")
ANSWERER_PROMPT_FILE = str(PROMPTS_DIR / "answer_generator.md")

# Langfuse prompt names live in app.pipeline.langfuse_prompts (single source of truth)

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

MODEL = "mistral-large-latest"
TEMPERATURE = 0.0
MAX_TOKENS = 16000

# ---------------------------------------------------------------------------
# Pipeline batching / parallelism
# ---------------------------------------------------------------------------

MAX_QUESTIONS_PER_CHUNK = 10
MAX_PARALLEL_CHUNK_CALLS = 3

# Phase 3 (answer generation) batch size, PER QUESTION TYPE — replaces the
# old single fixed MAX_QUESTIONS_PER_ANSWER_CALL. MCQ answers are short (just
# a letter), so many can go in one call without hurting quality; LA (Long
# Answer) answers are multi-paragraph, so batching too many together risks
# quality loss / truncation against MAX_TOKENS. Team-decided ranges:
# MCQ 6-7, SA 3-4, LA 1-2. VSA and "_default" are reasonable extrapolations
# (VSA answers are also short) — adjust if testing shows otherwise.
ANSWER_BATCH_SIZE_BY_TYPE = {
    "MCQ": 7,
    "VSA": 5,
    "SA": 4,
    "LA": 2,
    "_default": 4,  # fallback for any unrecognized/missing question_type
}

# ---------------------------------------------------------------------------
# Retries / jitter
# ---------------------------------------------------------------------------

RATE_LIMIT_MAX_RETRIES = 6
RATE_LIMIT_BASE_DELAY = 2.0
RATE_LIMIT_MAX_DELAY = 60.0
INTER_CALL_JITTER = (0.3, 1.0)
JSON_PARSE_MAX_ATTEMPTS = 3  # re-call LLM when response is invalid JSON (5 was compounding latency)
JSON_RETRY_TEMPERATURE = 0.2  # slight entropy so retries are not identical

# ---------------------------------------------------------------------------
# Domain defaults
# ---------------------------------------------------------------------------

DIFFICULTY_TO_MARKS_FALLBACK = {"Easy": 1, "Medium": 2, "Hard": 3}

MARKS_BREAKDOWN_ENABLED_TEXT = (
    "ENABLED — MANDATORY. For every SUBJECTIVE question or sub-part/alternative "
    "with marks >= 2, you MUST include a marking_scheme field inside its answer_data."
)

MARKS_BREAKDOWN_DISABLED_TEXT = (
    "DISABLED — Do NOT include a marking_scheme field in any answer_data object."
)

# ---------------------------------------------------------------------------
# Downstream API + pricing
# ---------------------------------------------------------------------------

CREATE_PAPER_URL = "https://stageapi.aichanakya.in/v0/edu-multiagent/create-paper/"

MISTRAL_INPUT_PRICE_PER_M_TOKENS = 0.50   # Mistral Large 3 (correct current pricing)
MISTRAL_OUTPUT_PRICE_PER_M_TOKENS = 1.50

# ---------------------------------------------------------------------------
# Background tasks / Celery
# ---------------------------------------------------------------------------

# sync  — run tasks in-process (local dev, no worker required)
# celery — enqueue to Redis (development / staging / production)
BACKGROUND_TASK_EXECUTION = os.getenv("BACKGROUND_TASK_EXECUTION", "sync").strip().lower()
BACKGROUND_TASKS_SYNC = BACKGROUND_TASK_EXECUTION == "sync"

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/2")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
CELERY_TASK_DEFAULT_QUEUE = os.getenv("CELERY_TASK_DEFAULT_QUEUE", "questionpaperprocessing")
CELERY_TASK_SOFT_TIME_LIMIT = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "3600"))
CELERY_TASK_TIME_LIMIT = int(os.getenv("CELERY_TASK_TIME_LIMIT", "3900"))
CELERY_WORKER_CONCURRENCY = int(
    os.getenv("CELERY_WORKER_CONCURRENCY", str(MAX_PARALLEL_CHUNK_CALLS))
)