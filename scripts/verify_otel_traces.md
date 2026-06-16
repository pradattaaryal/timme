# OpenTelemetry: distributed traces for batch + parallel scraping

## Enable

```env
OTEL_ENABLED=true
OTEL_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318
OTEL_INSTRUMENT_REDIS=false
OTEL_SERVICE_NAME_API=oxylab-api
OTEL_SERVICE_NAME_CELERY=oxylab-celery-worker
```

`OTEL_INSTRUMENT_REDIS=false` avoids orphan **PUBLISH / SET / EVALSHA** root traces. Celery + HTTP still traced.

Restart after changes:

```bash
docker compose up -d --build jaeger fastapi celery_worker
```

## Expected trace tree (one acquisition run)

```
POST /acquisition/run                    (oxylab-api, FastAPI)
  └── run_acquisition_task               (Celery auto-instrumentation)
        └── oxylab.pipeline.acquisition
              ├── oxylab.batch.process          (batch_id, batch_size, task_id, …)
              │     ├── oxylab.batch.parallel_scrape
              │     │     ├── oxylab.record.process   ← parallel (overlapping times)
              │     │     │     ├── oxylab.scrape.request
              │     │     │     └── oxylab.parse.results
              │     │     └── oxylab.record.process
              │     ├── oxylab.persistence.checkpoint
              │     └── oxylab.export.csv.partial
              ├── oxylab.batch.process (batch 2…)
              ├── oxylab.export.csv.final
              ├── oxylab.drive.upload
              └── oxylab.notify.email
```

## Attributes to filter in Jaeger

| Attribute | Meaning |
|-----------|---------|
| `celery.task_id` | Celery task UUID |
| `acquisition.run_key` | Run fingerprint |
| `acquisition.batch_id` | e.g. `abc123-batch-0001` |
| `acquisition.batch_size` | Records in this batch |
| `acquisition.batch.processing_time_s` | Batch wall time |
| `acquisition.parallel.worker_count` | Thread pool size |
| `celery.retry_count` | Celery retry attempt |

## Verify parallel execution

1. Open a trace for `oxylab.batch.parallel_scrape`.
2. Expand children `oxylab.record.process`.
3. **Overlapping** start/end times ⇒ parallel scraping is working.

## Verify batching

Multiple sibling `oxylab.batch.process` spans under one `oxylab.pipeline.acquisition` ⇒ batch loop is active.

## Logs

Worker logs include:

- `trace_pipeline_start` / `trace_pipeline_end`
- `trace_batch_start` / `trace_batch_end` with `batch_id` and `elapsed_s`

## Console exporter (no Jaeger)

```env
OTEL_EXPORTER=console
```

Read `docker compose logs -f celery_worker`.
