from __future__ import annotations

from typing import Literal, Protocol

DispatchStrategy = Literal["round_robin", "least_queued"]


class _WorkerQueueView(Protocol):
    def get_pending_jobs(self) -> int: ...


def pick_worker_index(
    strategy: DispatchStrategy,
    task_index: int,
    worker_count: int,
    workers: list[_WorkerQueueView],
) -> int:
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    if strategy == "round_robin":
        return task_index % worker_count
    best = 0
    best_depth = workers[0].get_pending_jobs()
    for i in range(1, worker_count):
        depth = workers[i].get_pending_jobs()
        if depth < best_depth:
            best_depth = depth
            best = i
    return best
