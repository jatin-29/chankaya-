# Question Paper Extraction Service

FastAPI service that converts question-paper PDFs into structured exam JSON
(questions, options, answers, marking schemes) using Mistral OCR + Mistral
Large, and pushes the result into the Chanakya AI platform.

---

## What it does

1. Accepts a PDF (via URL) + exam metadata on `POST /upload-paper/`
2. Returns a `job_id` immediately and processes everything in the background
3. Pipeline: OCR → image upload → relevance check + chunking → question
   extraction → answer generation → merge + auto-repair → push to platform
4. Progress can be checked anytime via `GET /job-status/{job_id}`

---

## Requirements

- Python 3.12+
- A Mistral API key (OCR + Large model access)
- An internal upload token for the image bucket
- An internal API token for the `create-paper/` platform endpoint

---

## Setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
```

Create a `.env` file in the project root (same folder as `main.py`) — **never
commit this file**, it's already in `.gitignore`.

### `.env` keys

| Key | Used for | Where it's used |
|---|---|---|
| `MISTRAL_API_KEY` | OCR (PDF → Markdown) and the 3-phase extraction pipeline (Mistral Large) | `app/services/ocr_service.py`, `app/pipeline/*.py` |
| `UPLOAD_API_TOKEN` | Uploading extracted question images to the internal file bucket | `app/services/upload_service.py` |
| `INTERNAL_API_TOKEN` | Authenticating the final `create-paper/` call that saves results into the platform (sent as the `x-access-token` header) | `app/services/paper_worker.py` |
| `BACKGROUND_TASK_EXECUTION` | `sync` (local, no worker) or `celery` (development/staging/production) | `app/core/config.py`, `app/tasks/dispatch.py` |
| `CELERY_BROKER_URL` | Redis broker URL when using Celery mode | `app/core/celery_app.py` |
| `CELERY_RESULT_BACKEND` | Celery result backend (defaults to broker URL) | `app/core/celery_app.py` |
| `CELERY_TASK_DEFAULT_QUEUE` | Queue name (default: `questionpaperprocessing`) | `app/core/celery_app.py` |

Example `.env`:

```
MISTRAL_API_KEY=your_mistral_key_here
UPLOAD_API_TOKEN=your_upload_token_here
INTERNAL_API_TOKEN=your_internal_token_here
BACKGROUND_TASK_EXECUTION=sync
```

For deployed environments (development, staging, production):

```
BACKGROUND_TASK_EXECUTION=celery
CELERY_BROKER_URL=redis://redis:6379/2
CELERY_RESULT_BACKEND=redis://redis:6379/2
CELERY_TASK_DEFAULT_QUEUE=questionpaperprocessing
```

---

## Dependencies (`requirements.txt`)

```
annotated-doc==0.0.4
annotated-types==0.7.0
anyio==4.14.1
certifi==2026.6.17
charset-normalizer==3.4.8
click==8.4.2
colorama==0.4.6
eval_type_backport==0.4.0
fastapi==0.139.0
h11==0.16.0
httpcore==1.0.9
httptools==0.8.0
httpx==0.28.1
idna==3.18
importlib_metadata==8.7.1
jsonpath-python==1.1.6
mistralai==2.6.0
opentelemetry-api==1.39.1
opentelemetry-semantic-conventions==0.60b1
pydantic==2.13.4
pydantic_core==2.46.4
python-dateutil==2.9.0.post0
python-dotenv==1.2.2
PyYAML==6.0.3
requests==2.34.2
six==1.17.0
starlette==1.3.1
typing-inspection==0.4.2
typing_extensions==4.16.0
urllib3==2.7.0
uvicorn==0.50.2
watchfiles==1.2.0
websockets==16.0
zipp==4.1.0
```

Key packages worth knowing:

| Package | Why it's here |
|---|---|
| `fastapi` | The web framework itself |
| `uvicorn` | The ASGI server that actually runs the app |
| `mistralai` | Official client for Mistral OCR + Mistral Large calls |
| `pydantic` | Powers request/response validation (`app/models/schemas.py`) |
| `python-dotenv` | Loads `.env` file contents into environment variables |
| `requests` | Used for downloading PDFs and calling the upload/create-paper APIs |
| `watchfiles` | Powers `--reload` auto-restart during local development |

---

## Running locally

**API only (sync mode — no Celery worker required):**

```bash
export BACKGROUND_TASK_EXECUTION=sync
uvicorn main:app --reload
```

**API + Celery worker (optional, mirrors deployed behavior):**

Terminal 1:

```bash
export BACKGROUND_TASK_EXECUTION=celery
export CELERY_BROKER_URL=redis://localhost:6379/2
export CELERY_RESULT_BACKEND=redis://localhost:6379/2
uvicorn main:app --reload
```

Terminal 2:

```bash
export BACKGROUND_TASK_EXECUTION=celery
export CELERY_BROKER_URL=redis://localhost:6379/2
export CELERY_RESULT_BACKEND=redis://localhost:6379/2
celery -A app.core.celery_app worker -Q questionpaperprocessing -l info -P threads --concurrency=3
```

- App: `http://127.0.0.1:8000`
- Interactive docs (Swagger UI): `http://127.0.0.1:8000/docs`

In Docker Compose, the `question-paper-extraction` service runs the API and
`question-paper-processing` runs the Celery worker (same image, shared
`/app/data` volume for SQLite job status).

---

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/upload-paper/` | Submit a PDF + exam metadata, returns a `job_id` |
| `GET` | `/job-status/{job_id}` | Check status/result of a single job |
| `GET` | `/jobs/exam/{exam_id}` | List all jobs submitted for a given exam |

Job status values: `processing` → `ocr` → `uploading_images` → `extracting`
→ `saving` → `done` / `failed`

---

## External services this app talks to

| Service | Purpose | Configured in |
|---|---|---|
| Mistral OCR (`mistral-ocr-latest`) | PDF → Markdown + image extraction | `app/services/ocr_service.py` |
| Mistral Large (`mistral-large-latest`) | Question extraction + answer generation | `app/core/config.py` (`MODEL`) |
| `https://stageapi.aichanakya.in/v0/internal/upload-file/` | Image bucket upload | `app/services/upload_service.py` |
| `https://stageapi.aichanakya.in/v0/edu-multiagent/create-paper/` | Saves the final structured paper into the platform | `app/core/config.py` (`CREATE_PAPER_URL`) |

---

## Project structure

```
main.py                      → app entry point, creates DB table on startup
requirements.txt             → Python dependencies (see above)
app/
├── api/routes.py             → the 3 endpoints above
├── core/config.py            → all constants (model name, URLs, tunables)
├── db/database.py            → SQLite-backed job tracking
├── models/schemas.py         → request/response validation
├── services/
│   ├── ocr_service.py         → PDF → Markdown + images
│   ├── upload_service.py      → image upload to bucket
│   ├── paper_worker.py        → orchestrates the full pipeline
│   └── ...
├── tasks/
│   ├── dispatch.py            → sync/celery dispatch helpers
│   └── paper_tasks.py         → Celery task definitions
└── pipeline/
    ├── relevance_and_chunking.py  → Phase 1
    ├── question_extractor.py      → Phase 2
    ├── answer_generator.py        → Phase 3
    ├── postprocess.py             → merge + auto-repair
    └── prompts/                   → the 3 prompt templates used above
```

---

## Known limitations

- `INTERNAL_API_TOKEN` must be set for the final save-to-platform step to
  succeed; all earlier pipeline steps work without it.
- Not yet deployed anywhere — local development only.