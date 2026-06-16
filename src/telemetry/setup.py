from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

logger = logging.getLogger(__name__)

_configured = False
_configured_pid: int | None = None
_propagator_configured = False


def is_telemetry_enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


def ensure_propagator_configured() -> None:
    """W3C trace context must be set before inject/extract (API publish path)."""
    global _propagator_configured
    if _propagator_configured or not is_telemetry_enabled():
        return
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    set_global_textmap(TraceContextTextMapPropagator())
    _propagator_configured = True


def configure_telemetry(*, service_name: str, instrument_celery: bool = True) -> bool:
    """
    Configure OpenTelemetry once per process (API or Celery worker child).
    Redis instrumentation is off by default to avoid orphan PUBLISH/SET root spans in Jaeger.
    """
    global _configured, _configured_pid
    pid = os.getpid()
    if _configured and _configured_pid == pid:
        return True
    if not is_telemetry_enabled():
        logger.info("OpenTelemetry disabled (OTEL_ENABLED is not true)")
        return False

    ensure_propagator_configured()

    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.namespace": os.getenv("OTEL_SERVICE_NAMESPACE", "oxylab"),
        }
    )
    provider = TracerProvider(resource=resource)

    exporter_kind = (os.getenv("OTEL_EXPORTER", "console") or "console").strip().lower()
    if exporter_kind == "otlp":
        endpoint = (os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or "http://localhost:4318").strip()
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            exporter = OTLPSpanExporter(endpoint=f"{endpoint.rstrip('/')}/v1/traces")
            provider.add_span_processor(BatchSpanProcessor(exporter))
            logger.info("OpenTelemetry OTLP exporter: %s", endpoint)
        except Exception:  # noqa: BLE001
            logger.exception("OTLP exporter failed; falling back to console")
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    else:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        logger.info("OpenTelemetry console span exporter enabled")

    trace.set_tracer_provider(provider)
    _instrument_libraries(instrument_celery=instrument_celery)
    _configured = True
    _configured_pid = pid
    logger.info("OpenTelemetry configured for service.name=%s pid=%s", service_name, pid)
    return True


def _instrument_libraries(*, instrument_celery: bool) -> None:
    if instrument_celery:
        try:
            from opentelemetry.instrumentation.celery import CeleryInstrumentor

            CeleryInstrumentor().instrument()
            logger.info("Celery OpenTelemetry instrumentation enabled")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to instrument Celery")

    if _env_bool("OTEL_INSTRUMENT_REDIS", False):
        try:
            from opentelemetry.instrumentation.redis import RedisInstrumentor

            RedisInstrumentor().instrument()
            logger.info("Redis OpenTelemetry instrumentation enabled")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to instrument Redis")
    else:
        logger.info("Redis OpenTelemetry instrumentation disabled (set OTEL_INSTRUMENT_REDIS=true to enable)")

    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor

        RequestsInstrumentor().instrument()
    except Exception:  # noqa: BLE001
        logger.exception("Failed to instrument requests")


def instrument_fastapi(app: object) -> None:
    if not is_telemetry_enabled():
        return
    ensure_propagator_configured()
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001
        logger.exception("Failed to instrument FastAPI")


def get_tracer(name: str) -> "Tracer":
    from opentelemetry import trace

    return trace.get_tracer(name)


def flush_traces(timeout_millis: int = 5000) -> None:
    if not is_telemetry_enabled():
        return
    from opentelemetry import trace

    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush(timeout_millis=timeout_millis)
