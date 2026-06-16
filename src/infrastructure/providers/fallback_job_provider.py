from __future__ import annotations

import logging
from typing import Any

from src.application.ports.job_provider import FetchJobParams, JobProvider

logger = logging.getLogger(__name__)


class FallbackJobProvider:
    def __init__(self, providers: list[JobProvider]) -> None:
        if not providers:
            raise ValueError("FallbackJobProvider requires at least one provider")
        self._providers = providers

    def fetch_jobs(self, params: FetchJobParams) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        provider_errors: list[str] = []
        for provider in self._providers:
            try:
                return provider.fetch_jobs(params)
            except Exception as exc:
                logger.warning("Job provider failed, trying next: %s", exc)
                last_error = exc
                provider_errors.append(f"{provider.__class__.__name__}: {exc}")
        detail = " | ".join(provider_errors) if provider_errors else "unknown provider error"
        raise RuntimeError(f"All job providers failed: {detail}") from last_error
