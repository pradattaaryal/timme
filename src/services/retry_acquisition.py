from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from src.config.settings import settings
from src.models.error_retry import RetryAcquisitionResult, RetryRecordResult, SanitizedErrorRecord
from src.telemetry.acquisition_tracing import (
    BatchTraceContext,
    TraceContext,
    make_batch_id,
    set_batch_outcome,
    span_batch_process,
    span_parallel_scrape,
    span_record_process,
)
from src.telemetry.context import attach_parent_context, submit_with_context

logger = logging.getLogger(__name__)

RecordProcessor = Callable[..., Any]


def _worker_pool_size(pool_for_n: int) -> int:
    pool_for_n = max(1, pool_for_n)
    configured_workers = max(1, settings.MAX_WORKERS)
    worker_count = min(configured_workers, pool_for_n)
    oxylabs_enabled = any(p.strip().lower() == "oxylabs" for p in settings.job_provider_names)
    if oxylabs_enabled:
        worker_count = min(worker_count, max(1, settings.OXYLABS_MAX_WORKERS))
    return max(1, worker_count)


class RetryAcquisitionService:
    """
    Re-runs acquisition for sanitized error records using the same batching,
    concurrency, and per-record processing as the main pipeline.
    """

    def __init__(self, process_record: RecordProcessor) -> None:
        self._process_record = process_record

    def run(
        self,
        records: list[SanitizedErrorRecord],
        *,
        pages: int,
        limit_per_query: int,
        fetched_at: str,
        trace_ctx: TraceContext,
        run_key: str,
    ) -> RetryAcquisitionResult:
        if not records:
            return RetryAcquisitionResult(
                retried_count=0,
                success_count=0,
                no_result_count=0,
                error_count=0,
                requests_made=0,
            )

        batch_size = max(1, settings.ACQUISITION_BATCH_SIZE)
        total_batches = (len(records) + batch_size - 1) // batch_size
        all_results: list[RetryRecordResult] = []
        total_requests = 0

        for batch_start in range(0, len(records), batch_size):
            batch_records = records[batch_start : batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            batch_id = make_batch_id(f"{run_key}-retry", batch_num)
            wc_batch = _worker_pool_size(len(batch_records))
            batch_ctx = BatchTraceContext(
                trace=trace_ctx,
                batch_id=batch_id,
                batch_num=batch_num,
                total_batches=total_batches,
                batch_size=len(batch_records),
                worker_count=wc_batch,
            )

            with span_batch_process(batch_ctx) as batch_span:
                batch_results = self._run_batch_slice(
                    batch_ctx=batch_ctx,
                    records=batch_records,
                    pages=pages,
                    limit_per_query=limit_per_query,
                    fetched_at=fetched_at,
                )
                all_results.extend(batch_results)
                total_requests += sum(r.requests_made for r in batch_results)

                ok_n = sum(1 for r in batch_results if r.status == "success")
                no_n = sum(1 for r in batch_results if r.status == "no_result")
                err_n = sum(1 for r in batch_results if r.status == "error")
                elapsed = batch_ctx.elapsed_s()
                set_batch_outcome(
                    batch_span,
                    ok=ok_n,
                    no_result=no_n,
                    error=err_n,
                    processing_time_s=elapsed,
                )
                logger.info(
                    "retry_acquisition_batch run_key=%s batch_id=%s batch=%s/%s size=%s "
                    "elapsed_s=%.2f ok=%s no_result=%s error=%s",
                    run_key,
                    batch_id,
                    batch_num,
                    total_batches,
                    len(batch_records),
                    elapsed,
                    ok_n,
                    no_n,
                    err_n,
                )

            if settings.BATCH_DELAY_SECONDS > 0 and batch_start + batch_size < len(records):
                time.sleep(settings.BATCH_DELAY_SECONDS)

        success_count = sum(1 for r in all_results if r.status == "success")
        no_result_count = sum(1 for r in all_results if r.status == "no_result")
        error_count = sum(1 for r in all_results if r.status == "error")

        return RetryAcquisitionResult(
            retried_count=len(records),
            success_count=success_count,
            no_result_count=no_result_count,
            error_count=error_count,
            requests_made=total_requests,
            results=all_results,
        )

    def _run_batch_slice(
        self,
        *,
        batch_ctx: BatchTraceContext,
        records: list[SanitizedErrorRecord],
        pages: int,
        limit_per_query: int,
        fetched_at: str,
    ) -> list[RetryRecordResult]:
        results: list[RetryRecordResult] = []
        wc = _worker_pool_size(len(records))
        parent_ctx = attach_parent_context()

        def _process_one(rec: SanitizedErrorRecord) -> RetryRecordResult:
            internal = rec.to_internal_record()
            with span_record_process(
                batch_ctx,
                record_index=-1,
                store_name=rec.store_name,
                city=rec.city_ward_name,
            ):
                outcome = self._process_record(
                    internal,
                    pages=pages,
                    limit_per_query=limit_per_query,
                    fetched_at=fetched_at,
                )
            return RetryRecordResult(
                customer_id=rec.customer_id,
                status=outcome.status,
                rows=outcome.rows,
                processing_log=outcome.processing_log,
                error=outcome.error,
                requests_made=outcome.requests_made,
            )

        with span_parallel_scrape(batch_ctx, slice_size=len(records), is_retry=True):
            with ThreadPoolExecutor(max_workers=wc) as executor:
                future_map = {
                    submit_with_context(executor, parent_ctx, _process_one, rec): rec
                    for rec in records
                }
                for future in as_completed(future_map):
                    results.append(future.result())
        return results
