from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


INPUT_EXPORT_COLUMNS: list[str] = ["企業名", "事業所名", "市区郡", "取引先ID"]

RESULT_EXPORT_TAIL_COLUMNS: list[str] = [
    "query_string",
    "Acquisition_date_and_time",
    "has_job_listing",
    "job_count",
    "Indeed_listed",
    "Baitoru_listed",
    "MynaviBaito_listed",
    "other_media_count",
    "job_title",
    "job_url",
    "job_type",
    "status",
]


MEDIA_NAME_RULES = {
    "indeed": "Indeed",
    "baitoru": "Baitoru",
    "baito.mynavi": "Mynavi Baito",
    "townwork": "Townwork",
    "froma": "FromA Navi",
    "recruit": "Recruit",
    "engage": "Engage",
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


def normalize_media_name(url: str | None) -> str | None:
    if not url:
        return None
    lower = url.lower()
    if "baito.mynavi" in lower:
        return "Mynavi Baito"
    for token, canonical in MEDIA_NAME_RULES.items():
        if token in lower:
            return canonical
    return None


def status_to_ja(status: str | None) -> str | None:
    if not status:
        return status
    return STATUS_JA_MAP.get(status, status)


def validation_reason_to_ja(reason: str | None) -> str | None:
    if not reason:
        return reason
    return VALIDATION_REASON_JA_MAP.get(reason, reason)


def input_columns_from_record(record: dict[str, Any]) -> dict[str, str]:
    """Map canonical input fields to JP export headers."""
    company = record.get("company_name")
    store_code = record.get("store_code") or record.get("_customer_ref")
    return {
        "企業名": "" if company is None else str(company).strip(),
        "事業所名": str(record.get("store_name") or "").strip(),
        "市区郡": str(record.get("city_ward_name") or "").strip(),
        "取引先ID": "" if store_code is None else str(store_code).strip(),
    }


def output_row_with_input(record: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    """Prepend input columns and normalize acquisition timestamp to ISO8601 UTC."""
    merged = {**input_columns_from_record(record), **row}
    if "fetched_at" in merged:
        merged["fetched_at"] = format_iso8601_datetime(merged.get("fetched_at"))
    return merged


def format_iso8601_datetime(value: str | datetime | None) -> str:
    """UTC ISO8601 with Z suffix (e.g. 2026-05-22T16:20:50.185Z)."""
    if value is None or (isinstance(value, str) and not str(value).strip()):
        dt = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    text = dt.isoformat(timespec="milliseconds")
    return text.replace("+00:00", "Z")


def apply_store_code_column_name(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns for CSV/Excel export (no store_code in the published schema)."""
    renames: dict[str, str] = {}
    if "fetched_at" in df.columns:
        renames["fetched_at"] = "Acquisition_date_and_time"
    return df.rename(columns=renames)


def prepare_result_export_df(df: pd.DataFrame) -> pd.DataFrame:
    """Drop internal columns, rename timestamps, format ISO8601, and order export columns."""
    if df.empty:
        return df
    out = df.drop(
        columns=["store_code", "Store code", "store_name", "company_name", "city_ward_name"],
        errors="ignore",
    )
    out = apply_store_code_column_name(out)
    if "Acquisition_date_and_time" in out.columns:
        out["Acquisition_date_and_time"] = out["Acquisition_date_and_time"].map(format_iso8601_datetime)
    prefix = [c for c in INPUT_EXPORT_COLUMNS if c in out.columns]
    preferred_tail = [c for c in RESULT_EXPORT_TAIL_COLUMNS if c in out.columns]
    remainder = [c for c in out.columns if c not in prefix and c not in preferred_tail]
    return out[prefix + preferred_tail + remainder]


def write_execution_report_excels(log_dir: Path, payload: dict[str, Any]) -> Path:
    """
    Write a clean Excel report for each execution and also append to an index workbook.
    This is intentionally independent of Python logging config so it always lands on disk.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = format_iso8601_datetime(datetime.now(timezone.utc))
    record: dict[str, Any] = {"timestamp": stamp, **payload}
    if "fetched_at" in record:
        record["Acquisition_date_and_time"] = format_iso8601_datetime(record.pop("fetched_at"))
    elif "Acquisition_date_and_time" in record:
        record["Acquisition_date_and_time"] = format_iso8601_datetime(record.get("Acquisition_date_and_time"))

    # Per-execution report (new file per run)
    compact = datetime.now().strftime("%Y%m%d_%H%M%S")
    per_exec_path = log_dir / f"execution_report_{compact}.xlsx"
    per_exec_df = pd.DataFrame([{col: record.get(col, "") for col in EXECUTION_REPORT_COLUMNS}])
    with pd.ExcelWriter(per_exec_path, engine="openpyxl") as writer:
        per_exec_df.to_excel(writer, sheet_name="execution_report", index=False)

    # Index report (append-only, stable schema)
    index_path = log_dir / "execution_reports.xlsx"
    legacy_path = log_dir / f"execution_reports_legacy_{compact}.xlsx"
    new_row_df = pd.DataFrame([{col: record.get(col, "") for col in EXECUTION_REPORT_COLUMNS}])

    if index_path.exists():
        try:
            existing_df = pd.read_excel(index_path, sheet_name="execution_reports")
            if list(existing_df.columns) != EXECUTION_REPORT_COLUMNS:
                # Unexpected schema -> move aside and start fresh.
                try:
                    index_path.replace(legacy_path)
                    existing_df = pd.DataFrame(columns=EXECUTION_REPORT_COLUMNS)
                except OSError:
                    index_path = log_dir / f"execution_reports_{compact}.xlsx"
                    existing_df = pd.DataFrame(columns=EXECUTION_REPORT_COLUMNS)
        except Exception:  # noqa: BLE001
            # Corrupted or unreadable workbook -> move aside and start fresh.
            try:
                index_path.replace(legacy_path)
            except OSError:
                index_path = log_dir / f"execution_reports_{compact}.xlsx"
            existing_df = pd.DataFrame(columns=EXECUTION_REPORT_COLUMNS)
    else:
        existing_df = pd.DataFrame(columns=EXECUTION_REPORT_COLUMNS)

    combined = pd.concat([existing_df, new_row_df], ignore_index=True)
    with pd.ExcelWriter(index_path, engine="openpyxl") as writer:
        combined.to_excel(writer, sheet_name="execution_reports", index=False)

    return per_exec_path

