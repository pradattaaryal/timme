from __future__ import annotations

from typing import Any

from opentelemetry.propagate import inject

from src.telemetry.setup import is_telemetry_enabled


def build_celery_publish_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """
    Inject W3C trace context into Celery message headers so the worker continues the same trace.
  """
    headers: dict[str, str] = dict(extra or {})
    if is_telemetry_enabled():
        inject(headers)
    return headers


def enqueue_acquisition_task(task: Any, **kwargs: Any) -> Any:
    """apply_async with trace propagation headers."""
    headers = build_celery_publish_headers()
    return task.apply_async(kwargs=kwargs, headers=headers)
