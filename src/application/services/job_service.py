from __future__ import annotations

from typing import Any

from src.application.ports.job_provider import FetchJobParams, JobProvider
from src.config.settings import settings


class JobService:
    def __init__(self, job_provider: JobProvider) -> None:
        self._job_provider = job_provider

    def get_jobs(self, params: FetchJobParams) -> list[dict[str, Any]]:
        query = (params.query or settings.DEFAULT_QUERY).strip()
        location = (params.location or settings.DEFAULT_LOCATION).strip()
        normalized_params = FetchJobParams(
            limit=params.limit,
            query=query,
            location=location or None,
            retries=params.retries,
            generate_variants=params.generate_variants,
            enrich=params.enrich,
        )
        return self._job_provider.fetch_jobs(normalized_params)
