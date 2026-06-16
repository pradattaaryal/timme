from __future__ import annotations

from typing import TYPE_CHECKING

from src.application.ports.job_provider import JobProvider
from src.infrastructure.providers.fallback_job_provider import FallbackJobProvider
from src.infrastructure.providers.rate_limited_job_provider import RateLimitedJobProvider
from src.infrastructure.parallel_api_pool.credentials import get_round_robin_pool
from src.infrastructure.providers.oxylabs_job_provider import OxylabsJobProvider
from src.infrastructure.providers.serpapi_provider import SerpApiJobProvider

if TYPE_CHECKING:
    from src.config.settings import Settings


class JobProviderFactory:
    @staticmethod
    def _create_named(settings: Settings, name: str) -> JobProvider:
        key = name.strip().lower()
        if key == "serpapi":
            return SerpApiJobProvider(settings)
        if key == "google":
            return SerpApiJobProvider(settings)
        if key == "oxylabs":
            return OxylabsJobProvider(settings, credential_pool=get_round_robin_pool(settings))
        raise ValueError(
            f"Invalid job provider '{name}'. Use 'serpapi', 'google', 'oxylabs', or a comma list in JOB_PROVIDERS."
        )

    @staticmethod
    def create(settings: Settings) -> JobProvider:
        names = settings.job_provider_names
        if len(names) > 1:
            inner: JobProvider = FallbackJobProvider(
                [JobProviderFactory._create_named(settings, n) for n in names]
            )
        else:
            inner = JobProviderFactory._create_named(settings, names[0])

        if settings.JOB_PROVIDER_MIN_INTERVAL_SECONDS > 0:
            return RateLimitedJobProvider(inner, settings.JOB_PROVIDER_MIN_INTERVAL_SECONDS)
        return inner
