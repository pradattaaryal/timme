from __future__ import annotations

from typing import Annotated

from fastapi import HTTPException, Query

from src.application.ports.job_provider import JobProvider
from src.application.services.job_service import JobService
from src.config.settings import settings
from src.infrastructure.factories.job_provider_factory import JobProviderFactory
from src.infrastructure.providers.rate_limited_job_provider import RateLimitedJobProvider

_default_job_service: JobService | None = None
_named_job_services: dict[str, JobService] = {}


def reset_job_service_cache() -> None:
    """Drop cached JobService instances (Celery fork-safe re-init)."""
    global _default_job_service
    _default_job_service = None
    _named_job_services.clear()


def _wrap_rate_limited(provider: JobProvider) -> JobProvider:
    if settings.JOB_PROVIDER_MIN_INTERVAL_SECONDS > 0:
        return RateLimitedJobProvider(provider, settings.JOB_PROVIDER_MIN_INTERVAL_SECONDS)
    return provider


def get_job_service(
    provider: Annotated[
        str | None,
        Query(
            description="Override job source for this request: serpapi, google, oxylabs. "
            "Omit to use JOB_PROVIDER / JOB_PROVIDERS from settings.",
        ),
    ] = None,
) -> JobService:
    global _default_job_service
    name = (provider or "").strip().lower()
    if not name:
        if _default_job_service is None:
            _default_job_service = JobService(JobProviderFactory.create(settings))
        return _default_job_service
    if name not in _named_job_services:
        try:
            inner = _wrap_rate_limited(JobProviderFactory._create_named(settings, name))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        _named_job_services[name] = JobService(inner)
    return _named_job_services[name]
