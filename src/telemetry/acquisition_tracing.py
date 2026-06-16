from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from src.telemetry.setup import flush_traces, get_tracer, is_telemetry_enabled

logger = logging.getLogger(__name__)
_TRACER_NAME = "oxylab.pipeline"


@dataclass(frozen=True)
class TraceContext:
    task_id: str
    run_key: str
    retry_count: int = 0

    def base_attributes(self) -> dict[str, str | int]:
        return {
            "celery.task_id": self.task_id,
            "acquisition.run_key": self.run_key,
            "celery.retry_count": self.retry_count,
        }


@dataclass
class BatchTraceContext:
    trace: TraceContext
    batch_id: str
    batch_num: int
    total_batches: int
    batch_size: int
    worker_count: int
    retry_pass: int = 0
    _t0: float = field(default_factory=time.perf_counter, repr=False)

    def attrs(self) -> dict[str, str | int]:
        base = self.trace.base_attributes()
        base.update(
            {
                "acquisition.batch_id": self.batch_id,
                "acquisition.batch_num": self.batch_num,
                "acquisition.batch_total": self.total_batches,
                "acquisition.batch_size": self.batch_size,
                "acquisition.parallel.worker_count": self.worker_count,
                "acquisition.batch.retry_pass": self.retry_pass,
            }
        )
        return base

    def elapsed_s(self) -> float:
        return time.perf_counter() - self._t0


def make_batch_id(run_key: str, batch_num: int) -> str:
    return f"{run_key}-batch-{batch_num:04d}"


@contextmanager
def span_pipeline_acquisition(ctx: TraceContext, *, record_count: int, batch_size: int) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    attrs = ctx.base_attributes()
    attrs["acquisition.record_count"] = record_count
    attrs["acquisition.configured_batch_size"] = batch_size
    with tracer.start_as_current_span("oxylab.pipeline.acquisition", attributes=attrs) as span:
        logger.info(
            "trace_pipeline_start task_id=%s run_key=%s records=%s batch_size=%s retry_count=%s",
            ctx.task_id,
            ctx.run_key,
            record_count,
            batch_size,
            ctx.retry_count,
        )
        try:
            yield
            span.set_status(Status(StatusCode.OK))
        except Exception as exc:  # noqa: BLE001
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            logger.info(
                "trace_pipeline_end task_id=%s run_key=%s",
                ctx.task_id,
                ctx.run_key,
            )
            flush_traces()


@contextmanager
def span_batch_process(batch: BatchTraceContext) -> Iterator[Any]:
    if not is_telemetry_enabled():
        yield None
        return
    tracer = get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span("oxylab.batch.process", attributes=batch.attrs()) as span:
        logger.info(
            "trace_batch_start batch_id=%s task_id=%s num=%s/%s size=%s workers=%s retry_pass=%s",
            batch.batch_id,
            batch.trace.task_id,
            batch.batch_num,
            batch.total_batches,
            batch.batch_size,
            batch.worker_count,
            batch.retry_pass,
        )
        try:
            yield span
            span.set_attribute("acquisition.batch.processing_time_s", round(batch.elapsed_s(), 3))
            span.set_status(Status(StatusCode.OK))
        except Exception as exc:  # noqa: BLE001
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            logger.info(
                "trace_batch_end batch_id=%s elapsed_s=%.3f",
                batch.batch_id,
                batch.elapsed_s(),
            )


@contextmanager
def span_parallel_scrape(batch: BatchTraceContext, *, slice_size: int, is_retry: bool) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    attrs = batch.attrs()
    attrs["acquisition.parallel.slice_size"] = slice_size
    attrs["acquisition.parallel.is_retry"] = is_retry
    with tracer.start_as_current_span("oxylab.batch.parallel_scrape", attributes=attrs):
        yield


@contextmanager
def span_record_process(
    batch: BatchTraceContext,
    *,
    record_index: int,
    store_name: str,
    city: str,
) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    attrs = batch.attrs()
    attrs["acquisition.record.index"] = record_index
    attrs["acquisition.store_name"] = store_name[:200]
    attrs["acquisition.city_ward_name"] = city[:200]
    with tracer.start_as_current_span("oxylab.record.process", attributes=attrs):
        yield


@contextmanager
def span_scrape_request(*, query: str, location: str) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span(
        "oxylab.scrape.request",
        attributes={
            "scrape.query": query[:300],
            "scrape.location": location[:200],
        },
    ):
        yield


@contextmanager
def span_parse_results(*, jobs_found: int) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span(
        "oxylab.parse.results",
        attributes={"parse.jobs_found": jobs_found},
    ):
        yield


@contextmanager
def span_checkpoint_save(*, record_index: int) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span(
        "oxylab.persistence.checkpoint",
        attributes={"acquisition.record.index": record_index},
    ):
        yield


@contextmanager
def span_csv_partial_export(ctx: TraceContext, *, batch_id: str, row_count: int, path: str) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    attrs = ctx.base_attributes()
    attrs["acquisition.batch_id"] = batch_id
    attrs["export.row_count"] = row_count
    attrs["export.path"] = path
    with tracer.start_as_current_span("oxylab.export.csv.partial", attributes=attrs):
        yield


@contextmanager
def span_csv_final_export(ctx: TraceContext, *, row_count: int, path: str) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    attrs = ctx.base_attributes()
    attrs["export.row_count"] = row_count
    attrs["export.path"] = path
    with tracer.start_as_current_span("oxylab.export.csv.final", attributes=attrs):
        yield


@contextmanager
def span_drive_upload(ctx: TraceContext, *, file_name: str) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    attrs = ctx.base_attributes()
    attrs["drive.file_name"] = file_name
    with tracer.start_as_current_span("oxylab.drive.upload", attributes=attrs):
        yield


@contextmanager
def span_email_notify(ctx: TraceContext, *, recipient_hint: str) -> Iterator[None]:
    if not is_telemetry_enabled():
        yield
        return
    tracer = get_tracer(_TRACER_NAME)
    attrs = ctx.base_attributes()
    attrs["notify.recipient_hint"] = recipient_hint[:200]
    with tracer.start_as_current_span("oxylab.notify.email", attributes=attrs):
        yield


def set_batch_outcome(
    span: Any,
    *,
    ok: int,
    no_result: int,
    error: int,
    processing_time_s: float,
) -> None:
    if span is None or not is_telemetry_enabled():
        return
    span.set_attribute("acquisition.batch.ok", ok)
    span.set_attribute("acquisition.batch.no_result", no_result)
    span.set_attribute("acquisition.batch.error", error)
    span.set_attribute("acquisition.batch.processing_time_s", round(processing_time_s, 3))


def current_span_add_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    if not is_telemetry_enabled():
        return
    span = trace.get_current_span()
    if span.is_recording():
        span.add_event(name, attributes=attributes or {})
