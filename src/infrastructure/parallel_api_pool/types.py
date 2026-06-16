from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")

LoggerFn = Callable[[str, dict[str, Any] | None], None]


def _default_logger(level: str, payload: dict[str, Any] | None) -> None:
    logging.getLogger("parallel_api_pool").log(
        logging.INFO if level == "info" else logging.WARNING,
        "%s %s",
        level,
        payload or {},
    )


default_logger: dict[str, LoggerFn] = {
    "info": lambda event, fields=None: _default_logger("info", {"event": event, **(fields or {})}),
    "warn": lambda event, fields=None: _default_logger("warn", {"event": event, **(fields or {})}),
}


@dataclass(slots=True)
class TaskOutcome(Generic[T]):
    ok: bool
    value: T | None
    task_index: int
    key_index: int
    key_id: str
    attempts: int
    error: str | None = None


@dataclass(slots=True)
class BatchSummary:
    total: int
    succeeded: int
    failed: int
    duration_ms: float


@dataclass(slots=True)
class BatchResult(Generic[T]):
    results: list[TaskOutcome[T]]
    summary: BatchSummary
