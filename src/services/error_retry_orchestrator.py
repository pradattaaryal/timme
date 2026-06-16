from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.config.settings import settings
from src.models.error_retry import MergeResult, RetryAcquisitionResult, SanitizationResult
from src.services.csv_error_extraction import CsvErrorExtractionService
from src.services.error_record_sanitization import ErrorRecordSanitizationService
from src.services.result_merging import ResultMergingService
from src.services.retry_acquisition import RetryAcquisitionService
from src.telemetry.acquisition_tracing import TraceContext

logger = logging.getLogger(__name__)

RecordProcessor = Callable[..., Any]


@dataclass
class ErrorRetryPipelineResult:
    """Full outcome of the post-export error retry pipeline."""

    enabled: bool
    skipped_reason: str | None
    extraction_error_count: int
    sanitization: SanitizationResult | None
    retry: RetryAcquisitionResult | None
    merge: MergeResult | None
    merged_export_df: pd.DataFrame | None
    extra_requests_made: int = 0
    retry_processing_logs: list[dict[str, Any]] | None = None
    retry_errors: list[dict[str, Any]] | None = None


class ErrorRetryOrchestrator:
    """
    Thin coordinator: extract errors from CSV → sanitize → retry → merge.
  Business rules live in the dedicated services; this class only sequences them.
    """

    def __init__(self, process_record: RecordProcessor) -> None:
        self._extraction = CsvErrorExtractionService()
        self._sanitization = ErrorRecordSanitizationService()
        self._retry = RetryAcquisitionService(process_record)
        self._merging = ResultMergingService()

    def run(
        self,
        *,
        partial_csv_path: str,
        pages: int,
        limit_per_query: int,
        fetched_at: str,
        trace_ctx: TraceContext,
        run_key: str,
    ) -> ErrorRetryPipelineResult:
        if not settings.ACQUISITION_ERROR_RETRY_ENABLED:
            logger.info("error_retry_pipeline skipped reason=disabled")
            return ErrorRetryPipelineResult(
                enabled=False,
                skipped_reason="disabled",
                extraction_error_count=0,
                sanitization=None,
                retry=None,
                merge=None,
                merged_export_df=None,
            )

        partial_df = self._extraction.load_dataframe(partial_csv_path)
        extraction = self._extraction.extract_from_dataframe(
            partial_df, source_path=partial_csv_path
        )
        if not extraction.error_rows:
            logger.info("error_retry_pipeline skipped reason=no_error_rows")
            return ErrorRetryPipelineResult(
                enabled=True,
                skipped_reason="no_error_rows",
                extraction_error_count=0,
                sanitization=None,
                retry=None,
                merge=None,
                merged_export_df=None,
            )

        sanitization = self._sanitization.sanitize(extraction.error_rows)
        if not sanitization.records:
            logger.info("error_retry_pipeline skipped reason=no_sanitized_records")
            return ErrorRetryPipelineResult(
                enabled=True,
                skipped_reason="no_sanitized_records",
                extraction_error_count=extraction.error_count,
                sanitization=sanitization,
                retry=None,
                merge=None,
                merged_export_df=None,
            )

        retry = self._retry.run(
            sanitization.records,
            pages=pages,
            limit_per_query=limit_per_query,
            fetched_at=fetched_at,
            trace_ctx=trace_ctx,
            run_key=run_key,
        )

        retried_ids = {rec.customer_id for rec in sanitization.records}
        merged_df, merge = self._merging.merge(
            partial_df,
            retry,
            retried_customer_ids=retried_ids,
        )

        retry_logs = [r.processing_log for r in retry.results]
        retry_errors = [r.error for r in retry.results if r.error]

        audit = {
            **merge.audit,
            "total_error_records_found": extraction.error_count,
        }
        merge.audit = audit

        logger.info(
            "error_retry_pipeline_audit total_error_records_found=%s total_records_retried=%s "
            "successful_retries=%s failed_retries=%s final_output_count=%s",
            audit.get("total_error_records_found"),
            audit.get("total_records_retried"),
            audit.get("successful_retries"),
            audit.get("failed_retries"),
            audit.get("final_output_count"),
        )

        return ErrorRetryPipelineResult(
            enabled=True,
            skipped_reason=None,
            extraction_error_count=extraction.error_count,
            sanitization=sanitization,
            retry=retry,
            merge=merge,
            merged_export_df=merged_df,
            extra_requests_made=retry.requests_made,
            retry_processing_logs=retry_logs,
            retry_errors=retry_errors,
        )
