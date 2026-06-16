from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar

from src.infrastructure.parallel_api_pool.dispatcher import DispatchStrategy, pick_worker_index
from src.infrastructure.parallel_api_pool.retry import RetryOptions
from src.infrastructure.parallel_api_pool.types import BatchResult, BatchSummary, TaskOutcome, default_logger
from src.infrastructure.parallel_api_pool.worker import KeyWorker, TaskFn

T = TypeVar("T")


@dataclass(slots=True)
class PoolOptions:
    max_workers: int = 10
    min_interval_seconds_per_key: float = 0.0
    strategy: DispatchStrategy = "round_robin"
    retry: RetryOptions | None = None


class ApiKeyPool(Generic[T]):
    """
    Pool of KeyWorkers — one dedicated API key per worker slot, each with its own rate limiter.
    execute_batch never raises for individual task failures; inspect results[].ok.
    """

    def __init__(
        self,
        api_keys: list[str],
        *,
        opts: PoolOptions | None = None,
        logger: dict[str, Callable[[str, dict[str, Any] | None], None]] | None = None,
    ) -> None:
        keys = [k.strip() for k in api_keys if k and k.strip()]
        if not keys:
            raise ValueError("ApiKeyPool requires at least one non-empty API key")

        options = opts or PoolOptions()
        max_workers = min(max(1, options.max_workers), len(keys))
        self._strategy = options.strategy
        self._logger = logger or default_logger
        retry = options.retry or RetryOptions()

        self._workers: list[KeyWorker[T]] = []
        for key_index in range(max_workers):
            key_id = f"key-{key_index}"
            self._workers.append(
                KeyWorker(
                    key_index=key_index,
                    key_id=key_id,
                    api_key=keys[key_index % len(keys)],
                    min_interval_seconds=options.min_interval_seconds_per_key,
                    retry=retry,
                    logger=self._logger,
                )
            )

        self._logger["info"](
            "pool_initialized",
            {
                "worker_count": len(self._workers),
                "strategy": self._strategy,
                "min_interval_seconds_per_key": options.min_interval_seconds_per_key,
            },
        )

    def get_worker_count(self) -> int:
        return len(self._workers)

    def execute_batch(self, tasks: list[TaskFn[T]]) -> BatchResult[T]:
        started = time.perf_counter()
        worker_count = len(self._workers)
        results: list[TaskOutcome[T] | None] = [None] * len(tasks)

        def _run_one(task_index: int, task: TaskFn[T]) -> TaskOutcome[T]:
            wi = pick_worker_index(self._strategy, task_index, worker_count, self._workers)
            return self._workers[wi].run_task(task_index, task)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run_one, i, task): i for i, task in enumerate(tasks)
            }
            for future in as_completed(future_map):
                task_index = future_map[future]
                results[task_index] = future.result()

        ordered = sorted(
            (r for r in results if r is not None),
            key=lambda x: x.task_index,
        )
        succeeded = sum(1 for r in ordered if r.ok)
        failed = len(ordered) - succeeded
        duration_ms = (time.perf_counter() - started) * 1000.0

        return BatchResult(
            results=ordered,
            summary=BatchSummary(
                total=len(ordered),
                succeeded=succeeded,
                failed=failed,
                duration_ms=duration_ms,
            ),
        )
