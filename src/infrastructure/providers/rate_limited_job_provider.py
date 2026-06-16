from __future__ import annotations

import threading
import time
from typing import Any

from src.application.ports.job_provider import FetchJobParams, JobProvider


class RateLimitedJobProvider:
    def __init__(self, provider: JobProvider, min_interval_seconds: float) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be non-negative")
        self._provider = provider
        self._min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._last_call_monotonic = 0.0

    def fetch_jobs(self, params: FetchJobParams) -> list[dict[str, Any]]:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call_monotonic
            wait = self._min_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            result = self._provider.fetch_jobs(params)
            self._last_call_monotonic = time.monotonic()
            return result
