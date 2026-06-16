from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import ParamSpec, TypeVar

from opentelemetry import context as otel_context

P = ParamSpec("P")
R = TypeVar("R")


def attach_parent_context() -> object:
    """Capture current OTel context for use in another thread."""
    return otel_context.get_current()


def run_in_attached_context(ctx: object, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    token = otel_context.attach(ctx)  # type: ignore[arg-type]
    try:
        return fn(*args, **kwargs)
    finally:
        otel_context.detach(token)


def submit_with_context(
    executor: ThreadPoolExecutor,
    ctx: object,
    fn: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> Future[R]:
    return executor.submit(run_in_attached_context, ctx, fn, *args, **kwargs)
