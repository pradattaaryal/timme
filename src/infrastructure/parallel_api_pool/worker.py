from __future__ import annotations

import threading
from typing import Any, Callable, Generic, TypeVar

from src.infrastructure.parallel_api_pool.rate_limiter import MinIntervalRateLimiter
from src.infrastructure.parallel_api_pool.retry import RetryOptions, with_retry
from src.infrastructure.parallel_api_pool.types import TaskOutcome, default_logger

T = TypeVar("T")
TaskFn = Callable[[str], T]


class KeyWorker(Generic[T]):
    """One worker = one API key + serial execution + dedicated rate limiter."""

    def __init__(
        self,
        *,
        key_index: int,
        key_id: str,
        api_key: str,
        min_interval_seconds: float,
        retry: RetryOptions,
        logger: dict[str, Callable[[str, dict[str, Any] | None], None]] | None = None,
    ) -> None:
        self.key_index = key_index
        self.key_id = key_id
        self.api_key = api_key
        self._limiter = MinIntervalRateLimiter(min_interval_seconds)
        self._retry = retry
        self._logger = logger or default_logger
        self._lock = threading.Lock()
        self._pending_jobs = 0

    def get_pending_jobs(self) -> int:
        with self._lock:
            return self._pending_jobs

    def run_task(self, task_index: int, task: TaskFn[T]) -> TaskOutcome[T]:
        with self._lock:
            self._pending_jobs += 1
        try:
            self._limiter.acquire()
            self._logger["info"](
                "request_start",
                {
                    "key_id": self.key_id,
                    "key_index": self.key_index,
                    "task_index": task_index,
                },
            )

            def _run() -> T:
                return task(self.api_key)

            try:
                value, attempts = with_retry(_run, self._retry)
            except BaseException as exc:
                self._logger["warn"](
                    "request_failed",
                    {
                        "key_id": self.key_id,
                        "key_index": self.key_index,
                        "task_index": task_index,
                        "error": str(exc),
                    },
                )
                return TaskOutcome(
                    ok=False,
                    value=None,
                    error=str(exc),
                    task_index=task_index,
                    key_index=self.key_index,
                    key_id=self.key_id,
                    attempts=self._retry.max_retries + 1,
                )

            self._logger["info"](
                "request_success",
                {
                    "key_id": self.key_id,
                    "key_index": self.key_index,
                    "task_index": task_index,
                    "attempts": attempts,
                },
            )
            return TaskOutcome(
                ok=True,
                value=value,
                task_index=task_index,
                key_index=self.key_index,
                key_id=self.key_id,
                attempts=attempts,
            )
        finally:
            with self._lock:
                self._pending_jobs -= 1
