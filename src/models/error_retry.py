from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Japanese export column names used in google_jobs CSV output.
ERROR_RETRY_INPUT_COLUMNS: tuple[str, ...] = ("企業名", "事業所名", "市区郡", "取引先ID")

# Status value in exported CSV (Japanese).
ERROR_STATUS_JA = "エラー"


@dataclass(frozen=True)
class ErrorCsvRow:
    """A single error row extracted from the output CSV (JP column schema)."""

    company_name: str
    store_name: str
    city_ward_name: str
    customer_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "企業名": self.company_name,
            "事業所名": self.store_name,
            "市区郡": self.city_ward_name,
            "取引先ID": self.customer_id,
        }


@dataclass
class CsvErrorExtractionResult:
    """Outcome of reading an output CSV and filtering error-status rows."""

    source_path: str
    total_rows: int
    error_rows: list[ErrorCsvRow] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return len(self.error_rows)


@dataclass
class SanitizedErrorRecord:
    """Deduplicated, trimmed error record ready for retry acquisition."""

    company_name: str
    store_name: str
    city_ward_name: str
    customer_id: str

    def to_internal_record(self) -> dict[str, Any]:
        """Convert to the canonical record dict used by _process_record."""
        return {
            "store_code": self.customer_id,
            "_customer_ref": self.customer_id,
            "company_name": self.company_name or None,
            "store_name": self.store_name,
            "city_ward_name": self.city_ward_name,
            "_force_job_suffix": True,
        }

    def to_dict(self) -> dict[str, str]:
        return {
            "企業名": self.company_name,
            "事業所名": self.store_name,
            "市区郡": self.city_ward_name,
            "取引先ID": self.customer_id,
        }


@dataclass
class SanitizationResult:
    """Outcome of sanitizing extracted error rows."""

    input_count: int
    records: list[SanitizedErrorRecord] = field(default_factory=list)
    skipped_empty_id: int = 0
    duplicates_removed: int = 0

    @property
    def output_count(self) -> int:
        return len(self.records)


@dataclass
class RetryRecordResult:
    """Processing outcome for a single retried record."""

    customer_id: str
    status: str  # success | no_result | error
    rows: list[dict[str, Any]]
    processing_log: dict[str, Any]
    error: dict[str, Any] | None
    requests_made: int


@dataclass
class RetryAcquisitionResult:
    """Outcome of the post-export error retry acquisition pass."""

    retried_count: int
    success_count: int
    no_result_count: int
    error_count: int
    requests_made: int
    results: list[RetryRecordResult] = field(default_factory=list)

    @property
    def failed_count(self) -> int:
        return self.error_count


@dataclass
class MergeResult:
    """Outcome of merging original export data with retry results."""

    merged_df_rows: int
    removed_error_rows: int
    appended_retry_rows: int
    final_error_count: int
    final_success_count: int
    final_no_result_count: int
    audit: dict[str, Any] = field(default_factory=dict)
