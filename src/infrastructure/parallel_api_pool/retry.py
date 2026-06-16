from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

T = TypeVar("T")


def _default_is_retryable(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate limit" in text


@dataclass(slots=True)
class RetryOptions:
    max_retries: int = 2
    base_delay_seconds: float = 0.2
    max_delay_seconds: float = 15.0
    jitter_ratio: float = 0.1
    is_retryable: Callable[[BaseException], bool] = _default_is_retryable


def _backoff_delay(attempt: int, opts: RetryOptions) -> float:
    delay = min(opts.max_delay_seconds, opts.base_delay_seconds * (2**attempt))
    jitter = delay * opts.jitter_ratio * random.random()
    return delay + jitter


def with_retry(fn: Callable[[], T], opts: RetryOptions) -> tuple[T, int]:
    attempts = 0
    last_exc: BaseException | None = None
    for attempt in range(opts.max_retries + 1):
        attempts = attempt + 1
        try:
            return fn(), attempts
        except BaseException as exc:
            last_exc = exc
            if attempt >= opts.max_retries or not opts.is_retryable(exc):
                raise
            time.sleep(_backoff_delay(attempt, opts))
    raise RuntimeError("with_retry exhausted without result") from last_exc
