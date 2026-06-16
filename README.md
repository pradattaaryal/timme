# Oxylab — Job Acquisition Pipeline

A Python service that scrapes Google Jobs for a list of stores, aggregates results, uploads to Google Drive, and sends notifications via email and Slack.

## Quick Links

| Resource | URL |
|----------|-----|
| OpenAPI docs | `http://127.0.0.1:8000/docs` |
| Health check | `GET http://127.0.0.1:8000/` |
| Jaeger tracing | `http://localhost:16686` (Docker only) |

---

## Project Structure

```
oxylab/
├── src/
│   ├── main.py                          # FastAPI application entry
│   ├── celery_app.py                    # Celery worker + acquisition task
│   ├── dependencies.py                  # Dependency injection
│   ├── config/
│   │   └── settings.py                  # Environment-driven configuration
│   ├── models/                          # Typed request/response & pipeline DTOs
│   ├── routers/                         # HTTP endpoints
│   ├── application/                     # Ports & services layer
│   ├── infrastructure/                  # Providers (SerpAPI, Oxylabs, retry)
│   ├── services/                        # Business logic (acquisition, retry, upload, email, slack)
│   └── telemetry/                       # OpenTelemetry tracing
├── data/
│   ├── input/                           # Stores CSV/Excel
│   ├── output/                          # Result CSV & summary Excel
│   └── logs/                            # Checkpoints, validation, error logs
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API server | FastAPI + uvicorn |
| Background workers | Celery |
| Message broker | Redis |
| Data processing | pandas |
| Job scraping | Oxylabs Realtime API / SerpAPI |
| Cloud storage | Google Drive API |
| Notifications | SMTP email + Slack webhooks |
| Tracing | OpenTelemetry + Jaeger |
| Deployment | Docker Compose |

---

## Installation

### 1. Clone & Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy `.env.example` (or edit `.env`) and set your credentials:

```bash
# Job provider: oxylabs or serpapi
JOB_PROVIDER=oxylabs
JOB_PROVIDERS=oxylabs

# SerpAPI (if JOB_PROVIDER=serpapi)
SERPAPI_KEY=your_serpapi_key

# Oxylabs Realtime API (if JOB_PROVIDER=oxylabs)
OXYLABS_CREDENTIALS="username:password"
OXYLABS_MAX_WORKERS=15
OXYLABS_MAX_CONCURRENT_PER_KEY=7
OXYLABS_MAX_CONCURRENT_TOTAL=15

# Google Drive upload
GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON=path/to/service-account.json
GOOGLE_DRIVE_FOLDER_ID=your_folder_id
GOOGLE_DRIVE_USE_TEMP_URL=false

# Email notifications
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_SECURITY=SSL
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-app-password
EMAIL_FROM=your-email@gmail.com
EMAIL_RECIPIENTS=recipient@example.com
EMAIL_CSV_LINK_URL=https://drive.google.com/drive/u/5/folders/your_folder_id

# Slack notifications

# Batch processing
ACQUISITION_BATCH_SIZE=50
MAX_WORKERS=50
MAX_RETRIES=3
ACQUISITION_ERROR_RETRY_ENABLED=true

# OpenTelemetry tracing (optional)
OTEL_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

> **Security:** Keep `SMTP_PASSWORD`, `OXYLABS_CREDENTIALS`, `SERPAPI_KEY`, and Google service account JSON out of version control.

---

## Running the Application

### Option A: Docker Compose (recommended)

Starts FastAPI, Celery worker, Redis, and Jaeger in containers:

```bash
docker compose up --build
```

### Option B: Local

Terminal 1 — FastAPI server:

```bash
uvicorn src.main:app --reload
```

Terminal 2 — Celery worker:

```bash
celery -A src.celery_app worker --loglevel=info
```

---

## API Endpoints

### Health Check

```
GET /
```

### Fetch Jobs (ad-hoc)

```
GET /jobs?query=software+engineer&location=Tokyo&limit=10&enrich=false
```

### Trigger Acquisition (full pipeline)

```
POST /acquisition/run
Content-Type: application/json

{
  "input_path": "data/input/stores.csv",
  "pages": 1,
  "limit_per_query": 20,
  "resume_from_checkpoint": false,
  "retry_dead_queue": false
}
```

**Response:**

```json
{
  "task_id": "abc123-def456"
}
```

---

## Acquisition Pipeline Flow

```
POST /acquisition/run
  └── Celery: run_acquisition_task
        ├── Load & validate stores CSV/Excel
        ├── Batch parallel fetch per store (Oxylabs / SerpAPI)
        ├── Checkpoint + incremental CSV export
        ├── Retry failed (エラー) records (optional)
        ├── Merge retry results into final CSV
        ├── Upload final CSV to Google Drive
        └── Notify via email + Slack
```

### Pipeline Steps

| Step | Description |
|------|-------------|
| 1. Validate | Load stores CSV, require `store_name` + `city_ward_name`, deduplicate |
| 2. Fetch | Parallel scrape Google Jobs per store, multiple query variants |
| 3. Checkpoint | Save progress after each record — resume on interruption |
| 4. Retry | Re-fetch stores with `error` status before final export |
| 5. Export | Write timestamped CSV to `data/output/` |
| 6. Upload | Push CSV to Google Drive (or use temp URL placeholder) |
| 7. Notify | Email + Slack completion report |

---

## Slack Notifications

Notifications are sent via Slack webhook at three points:

| Trigger | Message |
|---------|---------|
| **Error** | Any record fails during a batch — includes batch ID and failed query snippets |
| **Progress** | At 9am, 2pm, and 5pm JST (only when a run is active) — shows % processed, batch count, success/error breakdown |
| **Completion** | After pipeline finishes — total records, success rate, API usage, output path |

To enable, set `SLACK_WEBHOOK_URLl` in `.env`. Create a webhook URL in **Slack → Settings → Manage apps → Incoming Webhooks → Activate**.

---

## Output Files

| File | Location | Description |
|------|----------|-------------|
| `google_jobs_{timestamp}.csv` | `data/output/` | Final merged results (UTF-8 BOM) |
| `google_jobs_{timestamp}_partial.csv` | `data/output/` | Incremental export during processing |
| `summary_{timestamp}.xlsx` | `data/output/` | Execution summary, media counts, error list |
| `validation_{timestamp}.csv` | `data/logs/` | Input validation log |
| `errors_{timestamp}.csv` | `data/logs/` | Per-store processing outcomes |
| `checkpoint_{run_key}.json` | `data/logs/` | Resumable checkpoint for crash recovery |
| `execution_report_{timestamp}.xlsx` | `data/logs/` | Per-run metadata index |

### CSV Columns

**Input:** `store_name`, `city_ward_name`, `Store_code`, `business_type`, `corporate_number`

**Result:** `query_string`, `Acquisition_date_and_time`, `has_job_listing`, `job_count`, `Indeed_listed`, `Baitoru_listed`, `MynaviBaito_listed`, `other_media_count`, `job_title`, `job_url`, `job_type`, `status`

**Status values:** `成功` (success), `結果なし` (no result), `エラー` (error)

---

## Error Retry

After the main pipeline exports the partial CSV, stores with `エラー` status are retried:

1. Extract errored records from the partial CSV
2. Sanitize by `取引先ID` (store code) — deduplicate and remove empty IDs
3. Re-fetch using the same query variants and provider chain
4. Merge retried results into the final CSV (idempotent — same store code replaces error rows)

Enable/disable in `.env`:

```bash
ACQUISITION_ERROR_RETRY_ENABLED=true   # or false to skip
BATCH_ERROR_RETRY_PASSES=0             # intra-batch error retries (0 = disabled)
```

---

## Configuration Reference

See `.env` for all options. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `JOB_PROVIDER` | `serpapi` | Active job source (`serpapi` / `oxylabs`) |
| `ACQUISITION_BATCH_SIZE` | `50` | Records per parallel batch |
| `MAX_WORKERS` | `10` | Thread pool size |
| `BATCH_DELAY_SECONDS` | `0` | Pause between batches |
| `MAX_RETRIES` | `3` | HTTP retries per provider call |
| `MAX_QUERY_ATTEMPTS` | `5` | Query variant attempts per store |
| `GOOGLE_DRIVE_USE_TEMP_URL` | `false` | Skip real Drive upload, use placeholder URL |
| `SLACK_WEBHOOK_URLl` | — | Slack Incoming Webhook URL |
| `SMTP_HOST` | — | SMTP relay hostname |
| `OTEL_ENABLED` | `true` | Enable OpenTelemetry tracing |

---

## Development

### Run tests (when available)

```bash
pytest
```

### Check linting

```bash
ruff check src/
mypy src/
```

### Debug tracing

Open Jaeger UI at `http://localhost:16686` to inspect spans for acquisition runs, batch processing, and individual scrape requests.

---

## Architecture

The codebase follows a ports-and-adapters (hexagonal) architecture:

- **Routers** — thin HTTP controllers, no business logic
- **Services** — domain logic: validation, fetching, aggregation, retry, merge, upload, notifications
- **Infrastructure** — HTTP providers (Oxylabs, SerpAPI), rate limiting, credential pools
- **Application** — port interfaces (`JobProvider`) and facade service
- **Models** — typed request/response and pipeline DTOs
