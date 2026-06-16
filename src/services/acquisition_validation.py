from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_INPUT_COLUMNS = {"store_name", "city_ward_name"}

# Centralized input mapping (old headers + new JP headers).
# Canonical internal names are kept stable to avoid changing any downstream behavior.
INPUT_FIELD_ALIASES: dict[str, set[str]] = {
    # canonical -> possible headers (after normalization OR original)
    # `_normalize_columns` lowercases ASCII; 取引先ID becomes 取引先id — include both.
    "store_code": {"store_code", "storecode", "Store_code", "取引先ID", "取引先id"},
    "store_name": {"store_name", "storename", "事業所名"},
    "city_ward_name": {"city_ward_name", "citywardname", "市区郡"},
    "business_type": {"business_type", "businesstype", "事業所種別"},
    "corporate_number": {"corporate_number", "corporatenumber", "法人番号"},
    "store_name_jp": {"store_name_jp", "事業所名_jp"},
    "city_ward_name_jp": {"city_ward_name_jp", "市区郡_jp"},
    # Optional, not used in the current flow but mapped for completeness/back-compat.
    "company_name": {"company_name", "企業名"},
    # Do not alias 取引先ID here — it maps to store_code only to avoid clobbering in alias_to_canonical.
    "account_id": {"account_id"},
}


def _detect_new_jp_schema(columns: list[str]) -> bool:
    col_set = {str(c) for c in columns}
    required_jp = {"事業所名", "市区郡"}
    return bool(required_jp.issubset(col_set))


def apply_input_field_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename any known input headers to canonical internal names.
    Keeps unknown columns untouched.
    """
    # Build inverse lookup: alias -> canonical
    alias_to_canonical: dict[str, str] = {}
    for canonical, aliases in INPUT_FIELD_ALIASES.items():
        for alias in aliases:
            alias_to_canonical[str(alias)] = canonical

    rename_map: dict[str, str] = {}
    for col in df.columns:
        key = str(col)
        canonical = alias_to_canonical.get(key)
        if canonical:
            rename_map[col] = canonical

    mapped = df.rename(columns=rename_map)

    # Back-compat: treat new schema identifiers as existing internal keys.
    if "store_code" not in mapped.columns and "account_id" in mapped.columns:
        mapped["store_code"] = mapped["account_id"]

    # Mark source schema for downstream query-generation tweaks (no API changes).
    mapped.attrs["source_schema"] = "jp" if _detect_new_jp_schema(list(df.columns)) else "legacy"
    return mapped


@dataclass
class ValidationResult:
    valid_records: list[dict[str, Any]]
    skipped_records: list[dict[str, Any]]


def load_store_file(input_path: str) -> pd.DataFrame:
    path = Path(input_path)
    if not path.exists():
        # Backward/forward compatible fallback for common filename changes.
        # Keeps existing behavior when the configured path exists.
        candidates: list[Path] = []
        if path.name.lower() == "stores.csv":
            candidates.append(path.with_name("store.csv"))
        elif path.name.lower() == "store.csv":
            candidates.append(path.with_name("stores.csv"))

        for candidate in candidates:
            if candidate.exists():
                path = candidate
                break
        else:
            raise FileNotFoundError(f"Input file was not found: {input_path}")

    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        # Priority: UTF-8 then Shift-JIS
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="shift_jis")
    normalized = _normalize_columns(df)
    mapped = apply_input_field_mapping(normalized)
    return mapped.dropna(how="all").reset_index(drop=True)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = {}
    for col in df.columns:
        slug = str(col).strip().lower().replace(" ", "_")
        normalized[col] = slug
    df = df.rename(columns=normalized)
    return df


def validate_records(df: pd.DataFrame) -> ValidationResult:
    missing = REQUIRED_INPUT_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Required columns are missing: {sorted(missing)}")

    skipped: list[dict[str, Any]] = []
    valid: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str]] = set()

    force_job_suffix = str(df.attrs.get("source_schema") or "").lower() == "jp"

    for row_index, row in df.iterrows():
        store_name = str(row.get("store_name", "")).strip()
        city_ward = str(row.get("city_ward_name", "")).strip()
        customer_ref = str(row.get("store_code", "")).strip() or None
        company_name = str(row.get("company_name", "")).strip() or None
        if company_name and company_name.lower() == "nan":
            company_name = None

        if not store_name or not city_ward or store_name.lower() == "nan" or city_ward.lower() == "nan":
            skipped.append(
                {
                    "row_index": int(row_index),
                    "store_name": store_name,
                    "city_ward_name": city_ward,
                    "reason": "blank_or_invalid_value",
                }
            )
            continue

        # Same branch name + ward can appear for different companies; use optional ID when present.
        ref_key = (customer_ref or "").strip().lower()
        dedupe_key = (ref_key, store_name.lower(), city_ward.lower())
        if dedupe_key in seen_keys:
            skipped.append(
                {
                    "row_index": int(row_index),
                    "store_name": store_name,
                    "city_ward_name": city_ward,
                    "reason": "duplicate",
                }
            )
            continue

        seen_keys.add(dedupe_key)
        record: dict[str, Any] = {
            "_customer_ref": customer_ref,
            "store_code": customer_ref,
            "company_name": company_name,
            "store_name": store_name,
            "city_ward_name": city_ward,
            "business_type": row.get("business_type"),
            "corporate_number": row.get("corporate_number"),
            "store_name_jp": str(row.get("store_name_jp", "")).strip() or None,
            "city_ward_name_jp": str(row.get("city_ward_name_jp", "")).strip() or None,
        }
        if force_job_suffix:
            record["_force_job_suffix"] = True
        valid.append(record)

    return ValidationResult(valid_records=valid, skipped_records=skipped)


def build_validation_log_records(
    df: pd.DataFrame,
    validation: ValidationResult,
) -> list[dict[str, Any]]:
    skipped_by_row_index: dict[int, str] = {}
    for record in validation.skipped_records:
        ri = record.get("row_index")
        if isinstance(ri, int):
            skipped_by_row_index[ri] = str(record.get("reason", "")).strip() or "skipped"

    rows: list[dict[str, Any]] = []
    for row_index, raw_row in df.iterrows():
        store_name = str(raw_row.get("store_name", "")).strip()
        city_ward_name = str(raw_row.get("city_ward_name", "")).strip()
        reason = skipped_by_row_index.get(int(row_index), "")
        rows.append(
            {
                "store_name": store_name,
                "city_ward_name": city_ward_name,
                "business_type": raw_row.get("business_type"),
                "corporate_number": raw_row.get("corporate_number"),
                "store_name_jp": str(raw_row.get("store_name_jp", "")).strip() or None,
                "city_ward_name_jp": str(raw_row.get("city_ward_name_jp", "")).strip() or None,
                "validation_status": "skipped" if reason else "valid",
                "validation_reason": reason or "",
            }
        )
    return rows

