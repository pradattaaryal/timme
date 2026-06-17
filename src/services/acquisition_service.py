from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.application.ports.job_provider import FetchJobParams
from src.config.settings import settings
from src.dependencies import get_job_service
from src.services import acquisition_aggregation as _aggregation
from src.services import acquisition_persistence as _persistence
from src.services import acquisition_queries as _queries
from src.services import acquisition_reporting as _reporting
from src.services import acquisition_validation as _validation
from src.services.acquisition_partial_csv import IncrementalPartialCsvWriter
from src.services.error_retry_orchestrator import ErrorRetryOrchestrator
from src.services.google_drive_upload import upload_local_file_to_drive
from src.services.smtp_notification import send_drive_csv_link_email
from src.services.slack_notification import (
    send_error_notice,
    send_completion_notice,
    send_progress_notice,
    send_interval_progress,
)
from src.telemetry.acquisition_tracing import (
    BatchTraceContext,
    TraceContext,
    make_batch_id,
    set_batch_outcome,
    span_batch_process,
    span_checkpoint_save,
    span_csv_final_export,
    span_csv_partial_export,
    span_drive_upload,
    span_email_notify,
    span_parallel_scrape,
    span_parse_results,
    span_pipeline_acquisition,
    span_record_process,
    span_scrape_request,
)
from src.telemetry.context import attach_parent_context, submit_with_context


REQUIRED_INPUT_COLUMNS = {"store_name", "city_ward_name"}
OPTIONAL_INPUT_COLUMNS = {"business_type", "corporate_number", "store_name_jp", "city_ward_name_jp"}

MEDIA_NAME_RULES = {
    "indeed": "Indeed",
    "baitoru": "Baitoru",
    "baito.mynavi": "Mynavi Baito",
    "townwork": "Townwork",
    "froma": "FromA Navi",
    "recruit": "Recruit",
    "engage": "Engage",
}

BRAND_JP_ALIASES = {
    "familymart": "ファミリーマート",
    "lawson": "ローソン",
    "seven-eleven": "セブンイレブン",
    "7-eleven": "セブンイレブン",
    "mcdonald": "マクドナルド",
}

STATUS_JA_MAP = {
    "success": "成功",
    "no_result": "結果なし",
    "error": "エラー",
    "valid": "有効",
    "skipped": "スキップ",
}

VALIDATION_REASON_JA_MAP = {
    "blank_or_invalid_value": "空欄または不正な値",
    "garbled": "文字化け",
    "duplicate": "重複",
    "skipped": "スキップ",
}

SUMMARY_METRIC_JA_MAP = {
    "total_input_records": "入力件数",
    "valid_records": "有効件数",
    "skipped_records": "スキップ件数",
    "success_records": "成功件数",
    "no_result_records": "結果なし件数",
    "error_records": "エラー件数",
    "api_requests": "APIリクエスト数",
}

logger = logging.getLogger(__name__)

EXECUTION_REPORT_COLUMNS: list[str] = [
    "timestamp",
    "run_key",
    "Acquisition_date_and_time",
    "input_path",
    "pages",
    "limit_per_query",
    "output_csv",
    "output_csv_drive_file_id",
    "output_csv_drive_url",
    "summary_excel",
    "validation_log",
    "processing_log",
    "checkpoint_path",
    "dead_queue_path",
    "total_input_records",
    "valid_records",
    "skipped_records",
    "success_rate",
    "success_records",
    "no_result_records",
    "error_records",
    "api_requests",
    "job_providers",
    "min_interval_seconds",
]


@dataclass
class ValidationResult:
    valid_records: list[dict[str, Any]]
    skipped_records: list[dict[str, Any]]


@dataclass
class RecordProcessingResult:
    rows: list[dict[str, Any]]
    processing_log: dict[str, Any]
    error: dict[str, Any] | None
    requests_made: int
    status: str


@dataclass
class CheckpointState:
    run_key: str
    source_path: str
    pages: int
    limit_per_query: int
    fetched_at: str
    processed_indices: set[int]
    rows: list[dict[str, Any]]
    processing_logs: list[dict[str, Any]]
    errors: list[dict[str, Any]]
    requests_made: int


def _load_store_file(input_path: str) -> pd.DataFrame:
    return _validation.load_store_file(input_path)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Backward-compatible wrapper (kept for callers/tests).
    return df.rename(columns={col: str(col).strip().lower().replace(" ", "_") for col in df.columns})


def _build_query(city_ward_name: str, store_name: str, char_limit: int) -> str:
    return _queries.build_query(city_ward_name=city_ward_name, store_name=store_name, char_limit=char_limit)


def _brand_jp_alias(store_name: str) -> str | None:
    # Kept for backward compatibility; actual alias lookup lives in acquisition_queries.
    lower = store_name.lower()
    for token, alias in BRAND_JP_ALIASES.items():
        if token in lower:
            return alias
    return None


def _build_query_variants(
    city_ward_name: str,
    store_name: str,
    char_limit: int,
    city_ward_name_jp: str | None = None,
    store_name_jp: str | None = None,
    preferred_suffixes: list[str] | None = None,
) -> list[str]:
    return _queries.build_query_variants(
        city_ward_name=city_ward_name,
        store_name=store_name,
        char_limit=char_limit,
        city_ward_name_jp=city_ward_name_jp,
        store_name_jp=store_name_jp,
        preferred_suffixes=preferred_suffixes,
    )


def _normalize_media_name(url: str | None) -> str | None:
    return _reporting.normalize_media_name(url)


def _apply_priority_media_listing_flags(
    row: dict[str, Any],
    *,
    jobs: list[dict[str, Any]] | None = None,
) -> None:
    """Sync priority listing columns from job URL(s); also refreshes checkpoint rows at export."""
    if jobs:
        media_names = [_aggregation.listing_media_name(j, _normalize_media_name) for j in jobs]
    else:
        job_count = int(row.get("job_count") or 0)
        if job_count <= 0:
            row["Indeed_listed"] = ""
            row["Baitoru_listed"] = ""
            row["MynaviBaito_listed"] = ""
            row["other_media_count"] = 0
            return
        media_names = [
            _aggregation.listing_media_name({"url": row.get("job_url")}, _normalize_media_name)
        ]

    row["Indeed_listed"] = "○" if any(m == "Indeed" for m in media_names) else ""
    row["Baitoru_listed"] = "○" if any(m == "Baitoru" for m in media_names) else ""
    row["MynaviBaito_listed"] = "○" if any(m == "Mynavi Baito" for m in media_names) else ""
    row["other_media_count"] = sum(1 for m in media_names if m not in _aggregation.PRIORITY_MEDIA)


def _status_to_ja(status: str | None) -> str | None:
    return _reporting.status_to_ja(status)


def _validation_reason_to_ja(reason: str | None) -> str | None:
    return _reporting.validation_reason_to_ja(reason)


def _apply_store_code_column_name(df: pd.DataFrame) -> pd.DataFrame:
    return _reporting.apply_store_code_column_name(df)


def _output_row_with_input(record: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    return _reporting.output_row_with_input(record, row)


def _format_iso8601_datetime(value: str | datetime | None) -> str:
    return _reporting.format_iso8601_datetime(value)


def _validate_records(df: pd.DataFrame) -> ValidationResult:
    validated = _validation.validate_records(df)
    return ValidationResult(valid_records=validated.valid_records, skipped_records=validated.skipped_records)


def _build_validation_log_records(
    df: pd.DataFrame,
    validation: ValidationResult,
) -> list[dict[str, Any]]:
    return _validation.build_validation_log_records(
        df,
        _validation.ValidationResult(
            valid_records=validation.valid_records,
            skipped_records=validation.skipped_records,
        ),
    )


def _process_record(
    record: dict[str, Any],
    *,
    pages: int,
    limit_per_query: int,
    fetched_at: str,
) -> RecordProcessingResult:
    base_query = _build_query(
        city_ward_name=record["city_ward_name"],
        store_name=record["store_name"],
        char_limit=settings.QUERY_CHAR_LIMIT,
    )
    preferred_suffixes: list[str] | None = ["求人"] if record.get("_force_job_suffix") else None
    query_variants = _build_query_variants(
        city_ward_name=record["city_ward_name"],
        store_name=record["store_name"],
        char_limit=settings.QUERY_CHAR_LIMIT,
        city_ward_name_jp=record.get("city_ward_name_jp"),
        store_name_jp=record.get("store_name_jp"),
        preferred_suffixes=preferred_suffixes,
    )
    max_query_attempts = max(1, settings.MAX_QUERY_ATTEMPTS)
    query_variants = query_variants[:max_query_attempts]

    # When the user requests a single page with a limit of 1, they typically expect
    # exactly one query attempt per input record. Without this cap, we may try many
    # suffix/alias/JP variants, which can keep Celery tasks in STARTED for a long time.
    if pages == 1 and limit_per_query == 1:
        query_variants = query_variants[:1]

    attempted_queries: list[str] = []
    requests_made = 0
    try:
        jobs: list[dict[str, Any]] = []
        used_query = base_query
        for variant in query_variants:
            requests_made += 1
            attempted_queries.append(variant)
            with span_scrape_request(query=variant, location=record["city_ward_name"]):
                jobs = get_job_service().get_jobs(
                    FetchJobParams(
                        limit=max(1, limit_per_query * max(1, pages)),
                        query=variant,
                        location=record["city_ward_name"],
                        retries=settings.MAX_RETRIES,
                        generate_variants=False,
                        enrich=True,
                    )
                )
            if jobs:
                used_query = variant
                break

        if not jobs:
            attempted_for_output = attempted_queries[0] if attempted_queries else base_query
            return RecordProcessingResult(
                rows=[
                    _output_row_with_input(
                        record,
                        {
                            "store_name": record["store_name"],
                            "query_string": attempted_for_output,
                            "fetched_at": fetched_at,
                            "has_job_listing": False,
                            "job_count": 0,
                            "Indeed_listed": "",
                            "Baitoru_listed": "",
                            "MynaviBaito_listed": "",
                            "other_media_count": 0,
                            "job_title": _aggregation.normalize_job_title(None),
                            "job_url": None,
                            "job_type": None,
                            "status": "no_result",
                        },
                    )
                ],
                processing_log={
                    "store_name": record["store_name"],
                    "city_ward_name": record["city_ward_name"],
                    "status": "no_result",
                    "attempted_query_count": len(attempted_queries),
                    "attempted_queries": " | ".join(attempted_queries),
                    "matched_query": None,
                    "jobs_found": 0,
                    "error": "",
                },
                error=None,
                requests_made=requests_made,
                status="no_result",
            )

        # Aggregate to one row per store; preserve API result order for representative fields (no re-sort).
        job_count = len(jobs)
        with span_parse_results(jobs_found=job_count):
            result_row = _aggregation.aggregate_jobs_to_output_row(
                jobs,
                store_name=record["store_name"],
                query_string=used_query,
                fetched_at=fetched_at,
                normalize_media_name=_normalize_media_name,
            )
            _apply_priority_media_listing_flags(result_row, jobs=jobs)
        return RecordProcessingResult(
            rows=[_output_row_with_input(record, result_row)],
            processing_log={
                "store_name": record["store_name"],
                "city_ward_name": record["city_ward_name"],
                "status": "success",
                "attempted_query_count": len(attempted_queries),
                "attempted_queries": " | ".join(attempted_queries),
                "matched_query": used_query,
                "jobs_found": job_count,
                "error": "",
            },
            error=None,
            requests_made=requests_made,
            status="success",
        )
    except Exception as exc:  # noqa: BLE001
        return RecordProcessingResult(
            rows=[
                _output_row_with_input(
                    record,
                    {
                        "store_name": record["store_name"],
                        "query_string": base_query,
                        "fetched_at": fetched_at,
                        "has_job_listing": False,
                        "job_count": 0,
                        "Indeed_listed": "",
                        "Baitoru_listed": "",
                        "MynaviBaito_listed": "",
                        "other_media_count": 0,
                        "job_title": _aggregation.normalize_job_title(None),
                        "job_url": None,
                        "job_type": None,
                        "status": "error",
                    },
                )
            ],
            processing_log={
                "store_name": record["store_name"],
                "city_ward_name": record["city_ward_name"],
                "status": "error",
                "attempted_query_count": len(attempted_queries),
                "attempted_queries": " | ".join(attempted_queries),
                "matched_query": None,
                "jobs_found": 0,
                "error": str(exc),
            },
            error={
                "store_name": record["store_name"],
                "city_ward_name": record["city_ward_name"],
                "error": str(exc),
            },
            requests_made=requests_made,
            status="error",
        )


def _build_run_key(source_path: str, pages: int, limit_per_query: int) -> str:
    raw = f"{Path(source_path).resolve()}|{pages}|{limit_per_query}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _checkpoint_path(log_dir: Path, run_key: str) -> Path:
    return _persistence.checkpoint_path(log_dir, run_key)


def _dead_queue_path(log_dir: Path, run_key: str) -> Path:
    return _persistence.dead_queue_path(log_dir, run_key)


def _load_checkpoint(path: Path) -> CheckpointState | None:
    loaded = _persistence.load_checkpoint(path)
    if loaded is None:
        return None
    return CheckpointState(
        run_key=loaded.run_key,
        source_path=loaded.source_path,
        pages=loaded.pages,
        limit_per_query=loaded.limit_per_query,
        fetched_at=loaded.fetched_at,
        processed_indices=loaded.processed_indices,
        rows=loaded.rows,
        processing_logs=loaded.processing_logs,
        errors=loaded.errors,
        requests_made=loaded.requests_made,
    )


def _save_checkpoint(path: Path, state: CheckpointState) -> None:
    _persistence.save_checkpoint(
        path,
        _persistence.CheckpointState(
            run_key=state.run_key,
            source_path=state.source_path,
            pages=state.pages,
            limit_per_query=state.limit_per_query,
            fetched_at=state.fetched_at,
            processed_indices=state.processed_indices,
            rows=state.rows,
            processing_logs=state.processing_logs,
            errors=state.errors,
            requests_made=state.requests_made,
        ),
    )


def _append_dead_queue(dead_queue_file: Path, index: int, record: dict[str, Any], error: str) -> None:
    _persistence.append_dead_queue(dead_queue_file, index, record, error)


def _load_dead_queue(dead_queue_file: Path) -> dict[int, dict[str, Any]]:
    return _persistence.load_dead_queue(dead_queue_file)


def _rewrite_dead_queue(dead_queue_file: Path, entries: dict[int, dict[str, Any]]) -> None:
    _persistence.rewrite_dead_queue(dead_queue_file, entries)


def _write_execution_report_excels(log_dir: Path, payload: dict[str, Any]) -> Path:
    return _reporting.write_execution_report_excels(log_dir, payload)


def _export_ready_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=["store_code", "Store code"], errors="ignore")


def _partial_export_result_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    if "status" in out.columns:
        out["status"] = out["status"].map(_status_to_ja)
    return _reporting.prepare_result_export_df(out)


def _final_export_result_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return _reporting.prepare_result_export_df(df)


def _export_rows_for_partial_csv(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chunk = _partial_export_result_df(rows)
    if chunk.empty:
        return []
    # Match pandas to_csv: NaN -> empty cell (stdlib csv writes the literal "nan" otherwise).
    return chunk.where(pd.notna(chunk), None).to_dict(orient="records")


def _worker_pool_size(pool_for_n: int) -> int:
    pool_for_n = max(1, pool_for_n)
    configured_workers = max(1, settings.MAX_WORKERS)
    worker_count = min(configured_workers, pool_for_n)
    oxylabs_enabled = any(p.strip().lower() == "oxylabs" for p in settings.job_provider_names)
    if oxylabs_enabled:
        worker_count = min(worker_count, max(1, settings.OXYLABS_MAX_WORKERS))
    return max(1, worker_count)


def _purge_state_for_indices(
    state: CheckpointState,
    validation: ValidationResult,
    indices: set[int],
) -> None:
    if not indices:
        return
    keys: set[tuple[Any, Any]] = set()
    for i in indices:
        if i < 0 or i >= len(validation.valid_records):
            continue
        rec = validation.valid_records[i]
        keys.add((rec.get("store_name"), rec.get("city_ward_name")))
    state.rows = [r for r in state.rows if (r.get("store_name"), r.get("city_ward_name")) not in keys]
    state.processing_logs = [
        log for log in state.processing_logs if (log.get("store_name"), log.get("city_ward_name")) not in keys
    ]
    state.errors = [e for e in state.errors if (e.get("store_name"), e.get("city_ward_name")) not in keys]
    for i in indices:
        state.processed_indices.discard(i)


def _ingest_process_result(
    *,
    state: CheckpointState,
    idx: int,
    result: RecordProcessingResult,
    dead_queue_file: Path,
    dead_entries: dict[int, dict[str, Any]],
    validation: ValidationResult,
) -> None:
    state.processed_indices.add(idx)
    state.requests_made += result.requests_made
    state.processing_logs.append(result.processing_log)
    state.rows.extend(result.rows)
    if result.error:
        state.errors.append(result.error)
        _append_dead_queue(
            dead_queue_file=dead_queue_file,
            index=idx,
            record=validation.valid_records[idx],
            error=str(result.error.get("error") or "unknown_error"),
        )
    elif idx in dead_entries:
        dead_entries.pop(idx, None)


_progress_hours_fired: set[str] = set()

# Module-level lock and thread for the interval progress reporter.
_interval_lock = threading.Lock()
_interval_thread: threading.Thread | None = None
_interval_stop_event: threading.Event | None = None
_interval_run_key: str = ""
_interval_total: int = 0
_interval_status: list[dict[str, int]] = []


def _interval_progress_worker() -> None:
    """Background thread that posts Slack progress every 15 seconds until stopped."""
    global _interval_stop_event, _interval_run_key, _interval_total, _interval_status
    stop = _interval_stop_event
    run_key = _interval_run_key
    total = _interval_total

    while True:
        # Wait up to 15 s for the stop event
        stop.wait(timeout=15)
        if stop.is_set():
            break

        # Read current status snapshot under lock
        with _interval_lock:
            counts: dict[str, int] = {}
            if _interval_status:
                counts = dict(_interval_status[0])
            processed = counts.get("success", 0) + counts.get("no_result", 0) + counts.get("error", 0)

        try:
            send_interval_progress(
                run_key=run_key,
                total=total,
                processed=processed,
                status_counts=counts,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Interval progress Slack notification failed (ignored)")


def _start_interval_progress(run_key: str, total: int, state: CheckpointState | None = None) -> None:
    """Start the background interval progress thread."""
    global _interval_stop_event, _interval_thread, _interval_run_key, _interval_total, _interval_status

    # If a previous thread is still running, stop it first.
    stop_previous_interval_thread()

    _interval_run_key = run_key
    _interval_total = total
    _interval_stop_event = threading.Event()

    with _interval_lock:
        _interval_status.clear()
        if state is not None:
            ok = sum(1 for log in state.processing_logs if log.get("status") == "success")
            no = sum(1 for log in state.processing_logs if log.get("status") == "no_result")
            err = sum(1 for log in state.processing_logs if log.get("status") == "error")
            _interval_status.append({"success": ok, "no_result": no, "error": err})
        else:
            _interval_status.append({"success": 0, "no_result": 0, "error": 0})

    _interval_thread = threading.Thread(target=_interval_progress_worker, daemon=True, name="slack-interval-progress")
    _interval_thread.start()


def _stop_interval_progress() -> None:
    """Signal the background thread to stop and join it."""
    global _interval_thread

    if _interval_stop_event:
        _interval_stop_event.set()
    t = _interval_thread
    if t is not None:
        t.join(timeout=20)
        _interval_thread = None


def _update_interval_status(status_counts: dict[str, int]) -> None:
    """Thread-safe update of the latest status counts (read by the interval thread)."""
    with _interval_lock:
        if _interval_status:
            _interval_status[0] = status_counts
        else:
            _interval_status.append(status_counts)


def _update_interval_status_from_state(state: CheckpointState) -> None:
    """Read counts from CheckpointState and update the interval thread snapshot."""
    ok = sum(1 for log in state.processing_logs if log.get("status") == "success")
    no = sum(1 for log in state.processing_logs if log.get("status") == "no_result")
    err = sum(1 for log in state.processing_logs if log.get("status") == "error")
    _update_interval_status({"success": ok, "no_result": no, "error": err})


def _interval_progress_active() -> bool:
    t = _interval_thread
    return t is not None and t.is_alive()


def stop_previous_interval_thread() -> None:
    """Convenience wrapper to stop any previously running interval thread."""
    global _interval_stop_event, _interval_thread, _interval_status
    if _interval_stop_event:
        _interval_stop_event.set()
    t = _interval_thread
    if t is not None:
        t.join(timeout=20)
        _interval_thread = None
    with _interval_lock:
        _interval_status.clear()


def _send_batch_progress(
    *,
    run_key: str,
    total: int,
    processed: int,
    ok_n: int,
    no_n: int,
    err_n: int,
    batch_num: int,
    total_batches: int,
) -> None:
    """Send Slack progress notice at 9am/2pm/5pm JST if a run is active."""
    from src.services.slack_notification import send_progress_notice

    # Japan is UTC+9, use fixed offset to avoid pytz dependency
    import datetime as _dt

    class JST(_dt.tzinfo):
        _offset = _dt.timedelta(hours=9)

        def utcoffset(self, dt):
            return self._offset

        def tzname(self, dt):
            return "JST"

        def dst(self, dt):
            return _dt.timedelta(0)

    jst = JST()
    now_jst = datetime.now(tz=jst)
    current_hour = now_jst.hour
    progress_hours = {9, 14, 17}
    if current_hour not in progress_hours:
        return
    hour_key = f"{run_key}-{current_hour}"
    if hour_key in _progress_hours_fired:
        return
    _progress_hours_fired.add(hour_key)

    pct = round((processed / total) * 100, 1) if total else 0
    time_str = now_jst.strftime("%Y-%m-%d %H:%M JST")

    lines = [
        f"acquisition_progress",
        f"run_key: `{run_key}`",
        f"time: {time_str}",
        f"batch: {batch_num}/{total_batches}",
        "",
        f"*Progress: {pct}%",
        f"Processed {processed}/{total} stores",
        "",
    ]
    parts = []
    if ok_n > 0:
        parts.append(f"success: {ok_n}")
    if no_n > 0:
        parts.append(f"no_result: {no_n}")
    if err_n > 0:
        parts.append(f"error: {err_n}")
    if parts:
        lines.append("Status: " + " | ".join(parts))

    try:
        send_progress_notice(
            run_key=run_key,
            total=total,
            processed=processed,
            status_counts={"success": ok_n, "no_result": no_n, "error": err_n},
        )
    except Exception:  # noqa: BLE001
        logger.exception("Slack progress notification failed (ignored)")


def run_monthly_acquisition(
    input_path: str | None = None,
    pages: int = 1,
    limit_per_query: int = 20,
    resume_from_checkpoint: bool = True,
    retry_dead_queue: bool = True,
    *,
    celery_task_id: str | None = None,
    celery_retry_count: int = 0,
) -> dict[str, Any]:
    normalized_input_path = (input_path or "").strip()
    if normalized_input_path in {"", "string", "null", "none"}:
        source_path = settings.INPUT_PATH
    else:
        source_path = normalized_input_path
    output_dir = Path(settings.OUTPUT_DIR)
    log_dir = Path(settings.LOG_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    df = _load_store_file(source_path)
    validation = _validate_records(df)

    run_key = _build_run_key(str(source_path), pages, limit_per_query)
    trace_ctx = TraceContext(
        task_id=celery_task_id or f"local-{run_key}",
        run_key=run_key,
        retry_count=celery_retry_count,
    )
    checkpoint_file = _checkpoint_path(log_dir, run_key)
    dead_queue_file = _dead_queue_path(log_dir, run_key)

    checkpoint = _load_checkpoint(checkpoint_file) if resume_from_checkpoint else None
    if checkpoint and (
        checkpoint.run_key != run_key
        or checkpoint.source_path != str(Path(source_path).resolve())
        or checkpoint.pages != pages
        or checkpoint.limit_per_query != limit_per_query
    ):
        checkpoint = None

    if checkpoint:
        state = checkpoint
    else:
        state = CheckpointState(
            run_key=run_key,
            source_path=str(Path(source_path).resolve()),
            pages=pages,
            limit_per_query=limit_per_query,
            fetched_at=_format_iso8601_datetime(datetime.now(timezone.utc)),
            processed_indices=set(),
            rows=[],
            processing_logs=[],
            errors=[],
            requests_made=0,
        )

    pending_indices = [idx for idx in range(len(validation.valid_records)) if idx not in state.processed_indices]

    dead_entries = _load_dead_queue(dead_queue_file) if retry_dead_queue else {}
    retry_indices = [idx for idx in sorted(dead_entries.keys()) if idx in pending_indices]
    remaining_indices = [idx for idx in pending_indices if idx not in set(retry_indices)]
    processing_order = retry_indices + remaining_indices

    export_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    partial_csv_path = output_dir / f"google_jobs_{export_stamp}_partial.csv"
    partial_csv_writer = IncrementalPartialCsvWriter(
        partial_csv_path,
        transform_rows=_export_rows_for_partial_csv,
    )
    if state.rows:
        partial_csv_writer.rebuild(state.rows)
        partial_csv_writer.mark_indices_written(set(state.processed_indices))
    batch_size_cfg = max(1, settings.ACQUISITION_BATCH_SIZE)

    with span_pipeline_acquisition(
        trace_ctx,
        record_count=len(validation.valid_records),
        batch_size=batch_size_cfg,
    ):
        if processing_order:
            batch_size = batch_size_cfg
            total_batches = (len(processing_order) + batch_size - 1) // batch_size

            def _process_record_for_index(batch_ctx: BatchTraceContext, idx: int) -> RecordProcessingResult:
                rec = validation.valid_records[idx]
                with span_record_process(
                    batch_ctx,
                    record_index=idx,
                    store_name=str(rec.get("store_name") or ""),
                    city=str(rec.get("city_ward_name") or ""),
                ):
                    return _process_record(
                        rec,
                        pages=pages,
                        limit_per_query=limit_per_query,
                        fetched_at=state.fetched_at,
                    )

            def _run_slice(
                batch_ctx: BatchTraceContext,
                indices: list[int],
                *,
                is_retry: bool = False,
                retry_pass: int = 0,
                partial_writer: IncrementalPartialCsvWriter = partial_csv_writer,
            ) -> dict[int, RecordProcessingResult]:
                by_idx: dict[int, RecordProcessingResult] = {}
                wc = _worker_pool_size(len(indices))
                batch_ctx_retry = BatchTraceContext(
                    trace=batch_ctx.trace,
                    batch_id=batch_ctx.batch_id,
                    batch_num=batch_ctx.batch_num,
                    total_batches=batch_ctx.total_batches,
                    batch_size=batch_ctx.batch_size,
                    worker_count=wc,
                    retry_pass=retry_pass,
                )
                parent_ctx = attach_parent_context()
                with span_parallel_scrape(batch_ctx_retry, slice_size=len(indices), is_retry=is_retry):
                    with ThreadPoolExecutor(max_workers=wc) as executor:
                        future_map = {
                            submit_with_context(
                                executor,
                                parent_ctx,
                                _process_record_for_index,
                                batch_ctx_retry,
                                idx,
                            ): idx
                            for idx in indices
                        }
                        for future in as_completed(future_map):
                            idx = future_map[future]
                            result = future.result()
                            by_idx[idx] = result
                            _ingest_process_result(
                                state=state,
                                idx=idx,
                                result=result,
                                dead_queue_file=dead_queue_file,
                                dead_entries=dead_entries,
                                validation=validation,
                            )
                            if _interval_progress_active():
                                _update_interval_status_from_state(state)
                            with span_checkpoint_save(record_index=idx):
                                _save_checkpoint(checkpoint_file, state)
                            with span_csv_partial_export(
                                trace_ctx,
                                batch_id=batch_ctx.batch_id,
                                row_count=len(result.rows),
                                path=str(partial_writer.path),
                            ):
                                partial_writer.append_record(idx, result.rows)
                return by_idx

            for batch_start in range(0, len(processing_order), batch_size):
                batch_indices = processing_order[batch_start : batch_start + batch_size]
                batch_num = batch_start // batch_size + 1
                batch_id = make_batch_id(run_key, batch_num)
                wc_batch = _worker_pool_size(len(batch_indices))
                batch_ctx = BatchTraceContext(
                    trace=trace_ctx,
                    batch_id=batch_id,
                    batch_num=batch_num,
                    total_batches=total_batches,
                    batch_size=len(batch_indices),
                    worker_count=wc_batch,
                )

                # Start interval progress thread on first batch
                if batch_start == 0 and len(validation.valid_records) > 0:
                    _start_interval_progress(run_key, len(validation.valid_records), state)

                with span_batch_process(batch_ctx) as batch_span:
                    by_idx = _run_slice(batch_ctx, batch_indices, is_retry=False, retry_pass=0)
                    for retry_pass in range(1, settings.BATCH_ERROR_RETRY_PASSES + 1):
                        failed = {i for i in batch_indices if by_idx.get(i) and by_idx[i].status == "error"}
                        if not failed:
                            break
                        _purge_state_for_indices(state, validation, failed)
                        partial_csv_writer.rebuild(state.rows)
                        partial_csv_writer.mark_indices_written(set(state.processed_indices))
                        if _interval_progress_active():
                            _update_interval_status_from_state(state)
                        with span_checkpoint_save(record_index=-1):
                            _save_checkpoint(checkpoint_file, state)
                        retry_by = _run_slice(
                            batch_ctx,
                            sorted(failed),
                            is_retry=True,
                            retry_pass=retry_pass,
                        )
                        by_idx.update(retry_by)

                    ok_n = sum(1 for i in batch_indices if by_idx.get(i) and by_idx[i].status == "success")
                    no_n = sum(1 for i in batch_indices if by_idx.get(i) and by_idx[i].status == "no_result")
                    err_n = sum(1 for i in batch_indices if by_idx.get(i) and by_idx[i].status == "error")
                    failed_queries: list[str] = []
                    for i in batch_indices:
                        r = by_idx.get(i)
                        if not r or r.status != "error":
                            continue
                        pl = r.processing_log or {}
                        hint = str(pl.get("matched_query") or pl.get("attempted_queries") or "")
                        if hint:
                            failed_queries.append(hint[:500])

                    elapsed = batch_ctx.elapsed_s()
                    set_batch_outcome(
                        batch_span,
                        ok=ok_n,
                        no_result=no_n,
                        error=err_n,
                        processing_time_s=elapsed,
                    )
                    logger.info(
                        "acquisition_batch run_key=%s batch_id=%s batch=%s/%s size=%s elapsed_s=%.2f ok=%s no_result=%s error=%s",
                        run_key,
                        batch_id,
                        batch_num,
                        total_batches,
                        len(batch_indices),
                        elapsed,
                        ok_n,
                        no_n,
                        err_n,
                    )
                    if failed_queries:
                        logger.info(
                            "acquisition_batch_errors batch_id=%s sample_queries=%s",
                            batch_id,
                            failed_queries[:15],
                        )

                    # Slack error notice when any record in this batch failed
                    if err_n > 0 and failed_queries:
                        try:
                            send_error_notice(
                                run_key=run_key,
                                batch_id=batch_id,
                                failed_stores=failed_queries[:5],
                                error_msg=f"{err_n} store(s) failed in batch {batch_num}/{total_batches}",
                            )
                        except Exception:  # noqa: BLE001
                            logger.exception("Slack error notification failed (ignored)")

                    # Slack progress notice at scheduled hours (9am, 2pm, 5pm)
                    if len(validation.valid_records) > 0:
                        _send_batch_progress(
                            run_key=run_key,
                            total=len(validation.valid_records),
                            processed=len(state.processed_indices),
                            ok_n=ok_n,
                            no_n=no_n,
                            err_n=err_n,
                            batch_num=batch_num,
                            total_batches=total_batches,
                        )

                    # Update status snapshot for the interval progress thread
                    if len(validation.valid_records) > 0:
                        _update_interval_status_from_state(state)

                if settings.BATCH_DELAY_SECONDS > 0 and batch_start + batch_size < len(processing_order):
                    time.sleep(settings.BATCH_DELAY_SECONDS)

        if retry_dead_queue:
            _rewrite_dead_queue(dead_queue_file, dead_entries)

        rows: list[dict[str, Any]] = state.rows
        for row in rows:
            if int(row.get("job_count") or 0) > 0:
                _apply_priority_media_listing_flags(row)
        processing_logs: list[dict[str, Any]] = state.processing_logs
        requests_made = state.requests_made
        store_success_count = sum(1 for log in processing_logs if log.get("status") == "success")
        store_no_result_count = sum(1 for log in processing_logs if log.get("status") == "no_result")
        store_error_count = sum(1 for log in processing_logs if log.get("status") == "error")

        stamp = export_stamp
        output_csv_path = output_dir / f"google_jobs_{stamp}.csv"
        validation_log_path = log_dir / f"validation_{stamp}.csv"
        error_log_path = log_dir / f"errors_{stamp}.csv"
        summary_path = output_dir / f"summary_{stamp}.xlsx"

        result_df = pd.DataFrame(rows)
        validation_log_df = pd.DataFrame(_build_validation_log_records(df, validation))
        processing_log_df = pd.DataFrame(processing_logs)
        error_list_df = pd.DataFrame(state.errors)

        if "status" in result_df.columns:
            result_df["status"] = result_df["status"].map(_status_to_ja)
        if "validation_status" in validation_log_df.columns:
            validation_log_df["validation_status"] = validation_log_df["validation_status"].map(_status_to_ja)
        if "validation_reason" in validation_log_df.columns:
            validation_log_df["validation_reason"] = validation_log_df["validation_reason"].map(_validation_reason_to_ja)
        if "status" in processing_log_df.columns:
            processing_log_df["status"] = processing_log_df["status"].map(_status_to_ja)

        export_result_df = _final_export_result_df(result_df)
        export_validation_log_df = _apply_store_code_column_name(_export_ready_columns(validation_log_df))
        export_processing_log_df = _apply_store_code_column_name(_export_ready_columns(processing_log_df))
        export_error_list_df = _apply_store_code_column_name(_export_ready_columns(error_list_df))

        error_retry_outcome = ErrorRetryOrchestrator(_process_record).run(
            partial_csv_path=str(partial_csv_path),
            pages=pages,
            limit_per_query=limit_per_query,
            fetched_at=state.fetched_at,
            trace_ctx=trace_ctx,
            run_key=run_key,
        ) if partial_csv_path.exists() else None

        if error_retry_outcome and error_retry_outcome.merged_export_df is not None:
            export_result_df = error_retry_outcome.merged_export_df
            with span_csv_partial_export(
                trace_ctx,
                batch_id=f"{run_key}-retry-merge",
                row_count=len(export_result_df),
                path=str(partial_csv_path),
            ):
                export_result_df.to_csv(partial_csv_path, index=False, encoding="utf-8-sig")

            requests_made += error_retry_outcome.extra_requests_made
            if error_retry_outcome.retry_processing_logs:
                processing_logs.extend(error_retry_outcome.retry_processing_logs)
                processing_log_df = pd.DataFrame(processing_logs)
                if "status" in processing_log_df.columns:
                    processing_log_df["status"] = processing_log_df["status"].map(_status_to_ja)
                export_processing_log_df = _apply_store_code_column_name(
                    _export_ready_columns(processing_log_df)
                )
            if error_retry_outcome.retry_errors:
                state.errors.extend(error_retry_outcome.retry_errors)
                error_list_df = pd.DataFrame(state.errors)
                export_error_list_df = _apply_store_code_column_name(_export_ready_columns(error_list_df))
            if error_retry_outcome.merge:
                store_success_count = error_retry_outcome.merge.final_success_count
                store_no_result_count = error_retry_outcome.merge.final_no_result_count
                store_error_count = error_retry_outcome.merge.final_error_count
            result_df = export_result_df.copy()

        with span_csv_final_export(
            trace_ctx,
            row_count=len(export_result_df),
            path=str(output_csv_path),
        ):
            export_result_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

        output_csv_drive_file_id = None
        output_csv_drive_url = ""

        if settings.GOOGLE_DRIVE_USE_TEMP_URL:
            output_csv_drive_url = (settings.GOOGLE_DRIVE_TEMP_URL or "").strip() or (
                f"https://temp.invalid/acquisition-csv?export={stamp}&file={output_csv_path.name}"
            )
        else:
            with span_drive_upload(trace_ctx, file_name=output_csv_path.name):
                drive_upload = upload_local_file_to_drive(
                    output_csv_path,
                    service_account_json=settings.GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON,
                    parent_folder_id=settings.GOOGLE_DRIVE_FOLDER_ID,
                )
            output_csv_drive_file_id = drive_upload.file_id if drive_upload else None
            output_csv_drive_url = drive_upload.url if drive_upload else ""
            notify_url = (settings.EMAIL_CSV_LINK_URL or "").strip()
            if drive_upload and notify_url:
                try:
                    with span_email_notify(trace_ctx, recipient_hint=settings.EMAIL_RECIPIENTS):
                        ok = send_drive_csv_link_email(
                            csv_filename=output_csv_path.name,
                            drive_url=notify_url,
                            is_temporary_placeholder=False,
                        )
                    if not ok:
                        logger.warning(
                            "Post-upload notify email was not accepted by SMTP (acquisition still completed)."
                        )
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Post-upload notify email failed (ignored; acquisition completed successfully)."
                    )
        export_validation_log_df.to_csv(validation_log_path, index=False, encoding="utf-8-sig")
        export_processing_log_df.to_csv(error_log_path, index=False, encoding="utf-8-sig")

        media_counter = Counter()
        if not result_df.empty:
            if "Indeed_listed" in result_df.columns:
                media_counter["Indeed"] += int((result_df["Indeed_listed"] == "○").sum())
            if "Baitoru_listed" in result_df.columns:
                media_counter["Baitoru"] += int((result_df["Baitoru_listed"] == "○").sum())
            if "MynaviBaito_listed" in result_df.columns:
                media_counter["Mynavi Baito"] += int((result_df["MynaviBaito_listed"] == "○").sum())
            if "other_media_count" in result_df.columns:
                other_total = int(result_df["other_media_count"].fillna(0).astype(int).sum())
                if other_total:
                    media_counter["Other"] += other_total

        success_rate = 0.0
        if validation.valid_records:
            success_rate = round((store_success_count / len(validation.valid_records)) * 100, 2)

        # F-10 Execution Summary Report (Excel)
        # - Total number of transactions: treated as "valid (processable) records"
        # - Success rate: success_records / valid_records
        # - Number of publications by medium: derived from result rows' media_name
        # - Error list: true error entries captured during processing
        # - API usage: total API requests made
        execution_summary_df = pd.DataFrame(
            [
                ("total_transactions", len(validation.valid_records)),
                ("success_records", store_success_count),
                ("no_result_records", store_no_result_count),
                ("error_records", store_error_count),
                ("success_rate_percent", success_rate),
                ("api_requests", requests_made),
                ("total_input_records", len(df)),
                ("skipped_records", len(validation.skipped_records)),
            ],
            columns=["metric", "value"],
        )
        metric_label_map = {
            "total_transactions": "総トランザクション数",
            "success_rate_percent": "成功率(%)",
        }
        execution_summary_df["metric"] = execution_summary_df["metric"].map(
            lambda m: metric_label_map.get(str(m), SUMMARY_METRIC_JA_MAP.get(str(m), str(m)))
        )
        media_df = pd.DataFrame(list(media_counter.items()), columns=["media_name", "count"])

        with pd.ExcelWriter(summary_path, engine="openpyxl") as writer:
            execution_summary_df.to_excel(writer, sheet_name="execution_summary", index=False)
            media_df.to_excel(writer, sheet_name="publications_by_medium", index=False)
            export_error_list_df.to_excel(writer, sheet_name="error_list", index=False)
            pd.DataFrame(
                [
                    {"metric": "api_requests", "value": requests_made},
                    {"metric": "job_providers", "value": ", ".join(settings.job_provider_names)},
                    {"metric": "min_interval_seconds", "value": settings.JOB_PROVIDER_MIN_INTERVAL_SECONDS},
                ]
            ).to_excel(writer, sheet_name="api_usage", index=False)

            # Keep legacy sheets (non-breaking for existing users of summary_*.xlsx)
            export_processing_log_df.to_excel(writer, sheet_name="processing_log", index=False)

        partial_output_csv = str(partial_csv_path) if processing_order and partial_csv_path.exists() else ""

        # Log report locations to disk and standard logger.
        report_payload = {
            "run_key": run_key,
            "Acquisition_date_and_time": state.fetched_at,
            "input_path": str(source_path),
            "pages": pages,
            "limit_per_query": limit_per_query,
            "output_csv": str(output_csv_path),
            "output_csv_drive_file_id": output_csv_drive_file_id or "",
            "output_csv_drive_url": output_csv_drive_url or "",
            "summary_excel": str(summary_path),
            "validation_log": str(validation_log_path),
            "processing_log": str(error_log_path),
            "checkpoint_path": str(checkpoint_file),
            "dead_queue_path": str(dead_queue_file),
            "total_input_records": len(df),
            "valid_records": len(validation.valid_records),
            "skipped_records": len(validation.skipped_records),
            "success_rate": success_rate,
            "success_records": store_success_count,
            "no_result_records": store_no_result_count,
            "error_records": store_error_count,
            "api_requests": requests_made,
            "job_providers": ", ".join(settings.job_provider_names),
            "min_interval_seconds": settings.JOB_PROVIDER_MIN_INTERVAL_SECONDS,
        }
        per_exec_excel = _write_execution_report_excels(log_dir, report_payload)
        logger.info("Execution summary report written: %s", summary_path)
        logger.info("Execution report Excel written: %s", per_exec_excel)
        if partial_output_csv:
            logger.info("Partial batch CSV (incremental export): %s", partial_output_csv)

        # Stop interval progress thread before sending final completion notice
        _stop_interval_progress()

        # Slack completion notice
        try:
            send_completion_notice(
                run_key=run_key,
                total_records=len(validation.valid_records),
                success=store_success_count,
                no_result=store_no_result_count,
                error=store_error_count,
                success_rate=success_rate,
                api_requests=requests_made,
                output_csv=output_csv_path.name,
                drive_url=output_csv_drive_url,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Slack completion notification failed (ignored)")

        return {
            "input_path": str(source_path),
            "output_csv": str(output_csv_path),
            "partial_output_csv": partial_output_csv,
            "output_csv_drive_file_id": output_csv_drive_file_id,
            "output_csv_drive_url": output_csv_drive_url or None,
            "summary_excel": str(summary_path),
            "validation_log": str(validation_log_path),
            "error_log": str(error_log_path),
            "checkpoint_path": str(checkpoint_file),
            "dead_queue_path": str(dead_queue_file),
            "total_input_records": len(df),
            "valid_records": len(validation.valid_records),
            "skipped_records": len(validation.skipped_records),
            "success_rate": success_rate,
            "success_records": store_success_count,
            "no_result_records": store_no_result_count,
            "error_records": store_error_count,
            "api_requests": requests_made,
            "error_retry": (
                error_retry_outcome.merge.audit
                if error_retry_outcome and error_retry_outcome.merge
                else (
                    {
                        "skipped_reason": error_retry_outcome.skipped_reason,
                        "extraction_error_count": error_retry_outcome.extraction_error_count,
                    }
                    if error_retry_outcome
                    else {"skipped_reason": "no_partial_csv"}
                )
            ),
        }
