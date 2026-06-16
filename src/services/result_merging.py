from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.models.error_retry import ERROR_STATUS_JA, MergeResult, RetryAcquisitionResult
from src.services import acquisition_reporting as reporting

logger = logging.getLogger(__name__)

STATUS_COLUMN = "status"
CUSTOMER_ID_COLUMN = "取引先ID"

SUCCESS_STATUS_JA = reporting.STATUS_JA_MAP["success"]
NO_RESULT_STATUS_JA = reporting.STATUS_JA_MAP["no_result"]


class ResultMergingService:
    """Merges retry acquisition results back into the exported dataset."""

    def merge(
        self,
        original_df: pd.DataFrame,
        retry_result: RetryAcquisitionResult,
        *,
        retried_customer_ids: set[str],
    ) -> tuple[pd.DataFrame, MergeResult]:
        if original_df.empty:
            merged = self._build_retry_export_df(retry_result)
            merged = reporting.prepare_result_export_df(merged)
            audit = self._build_audit(
                total_records_retried=retry_result.retried_count,
                successful_retries=retry_result.success_count,
                failed_retries=retry_result.failed_count,
                final_output_count=len(merged),
            )
            return merged, MergeResult(
                merged_df_rows=len(merged),
                removed_error_rows=0,
                appended_retry_rows=len(merged),
                final_error_count=retry_result.error_count,
                final_success_count=retry_result.success_count,
                final_no_result_count=retry_result.no_result_count,
                audit=audit,
            )

        working = original_df.copy()
        removed_error_rows = 0

        if retried_customer_ids and STATUS_COLUMN in working.columns and CUSTOMER_ID_COLUMN in working.columns:
            status_series = working[STATUS_COLUMN].fillna("").astype(str).str.strip()
            id_series = working[CUSTOMER_ID_COLUMN].fillna("").astype(str).str.strip()
            remove_mask = (status_series == ERROR_STATUS_JA) & id_series.isin(retried_customer_ids)
            removed_error_rows = int(remove_mask.sum())
            working = working.loc[~remove_mask].copy()

        retry_export_df = self._build_retry_export_df(retry_result)
        if not retry_export_df.empty:
            working = pd.concat([working, retry_export_df], ignore_index=True)

        working = self._dedupe_by_customer_id(working)
        working = reporting.prepare_result_export_df(working)

        final_status = (
            working[STATUS_COLUMN].fillna("").astype(str).str.strip()
            if STATUS_COLUMN in working.columns
            else pd.Series(dtype=str)
        )
        final_success = int((final_status == SUCCESS_STATUS_JA).sum()) if len(final_status) else 0
        final_no_result = int((final_status == NO_RESULT_STATUS_JA).sum()) if len(final_status) else 0
        final_error = int((final_status == ERROR_STATUS_JA).sum()) if len(final_status) else 0

        audit = self._build_audit(
            total_records_retried=retry_result.retried_count,
            successful_retries=retry_result.success_count,
            failed_retries=retry_result.failed_count,
            final_output_count=len(working),
            removed_error_rows=removed_error_rows,
        )
        logger.info(
            "result_merging removed_error_rows=%s appended_retry_rows=%s final_rows=%s "
            "final_success=%s final_no_result=%s final_error=%s",
            removed_error_rows,
            len(retry_export_df),
            len(working),
            final_success,
            final_no_result,
            final_error,
        )
        return working, MergeResult(
            merged_df_rows=len(working),
            removed_error_rows=removed_error_rows,
            appended_retry_rows=len(retry_export_df),
            final_error_count=final_error,
            final_success_count=final_success,
            final_no_result_count=final_no_result,
            audit=audit,
        )

    def _build_retry_export_df(self, retry_result: RetryAcquisitionResult) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for record_result in retry_result.results:
            rows.extend(record_result.rows)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "status" in df.columns:
            df["status"] = df["status"].map(reporting.status_to_ja)
        return df

    def _dedupe_by_customer_id(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or CUSTOMER_ID_COLUMN not in df.columns:
            return df
        id_series = df[CUSTOMER_ID_COLUMN].fillna("").astype(str).str.strip()
        has_id = id_series != ""
        with_id = df.loc[has_id].copy()
        without_id = df.loc[~has_id].copy()
        if with_id.empty:
            return df
        # Keep the last occurrence (retry results are appended after originals).
        with_id = with_id.assign(_customer_id_key=id_series[has_id])
        with_id = with_id.drop_duplicates(subset=["_customer_id_key"], keep="last")
        with_id = with_id.drop(columns=["_customer_id_key"])
        return pd.concat([with_id, without_id], ignore_index=True)

    @staticmethod
    def _build_audit(
        *,
        total_records_retried: int,
        successful_retries: int,
        failed_retries: int,
        final_output_count: int,
        removed_error_rows: int = 0,
    ) -> dict[str, Any]:
        return {
            "removed_error_rows": removed_error_rows,
            "total_records_retried": total_records_retried,
            "successful_retries": successful_retries,
            "failed_retries": failed_retries,
            "final_output_count": final_output_count,
        }
