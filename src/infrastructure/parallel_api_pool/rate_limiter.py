from __future__ import annotations

import threading
import time


class MinIntervalRateLimiter:
    """Per-API-key spacing between successive operations (thread-safe)."""

    def __init__(self, min_interval_seconds: float) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be non-negative")
        self._min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._last_call_monotonic = 0.0

    def acquire(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call_monotonic
            wait = self._min_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            self._last_call_monotonic = time.monotonic()
