from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Any

import requests

from src.application.ports.job_provider import FetchJobParams

if TYPE_CHECKING:
    from src.config.settings import Settings

logger = logging.getLogger(__name__)


def _is_no_result_error(message: str | None) -> bool:
    if not message:
        return False
    normalized = message.lower()
    return "hasn't returned any results" in normalized or "no results" in normalized


def _normalize_serpapi_location(location: str | None) -> str | None:
    if not location:
        return None
    s = location.strip()
    if not s:
        return None

    work = s
    had_ward = bool(re.search(r"\bward\s*$", work, flags=re.I))
    if had_ward:
        work = re.sub(r"\s*,?\s*ward\s*$", "", work, flags=re.I).strip()

    m_city = re.match(r"^(.+?)\s+City\s+.+$", work, flags=re.I)
    if m_city:
        metro = m_city.group(1).strip()
        if metro:
            return f"{metro}, Japan"

    if had_ward and work:
        return f"{work}, Japan"

    return work


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value:
            return value
    return None


def _extract_minimum_qualifications(text: str) -> str | None:
    if not text:
        return None
    match = re.search(
        r"Minimum qualifications?\s*[:\-]?\s*([^.;|]+)",
        text,
        flags=re.I,
    )
    if match:
        return match.group(1).strip()
    return None


def _extract_serpapi_jobs(data: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    raw_jobs = data.get("jobs_results", []) or []
    jobs: list[dict[str, Any]] = []
    for job in raw_jobs[:limit]:
        description = _first_non_empty(
            job.get("description"),
            job.get("snippet"),
        ) or ""
        jobs.append(
            {
                "title": _first_non_empty(job.get("title"), job.get("job_title")),
                "company": _first_non_empty(job.get("company_name"), job.get("company")),
                "location": _first_non_empty(job.get("location"), job.get("job_location")),
                "minimum_qualifications": _extract_minimum_qualifications(description),
                "url": _first_non_empty(
                    job.get("related_links", [{}])[0].get("link")
                    if isinstance(job.get("related_links"), list) and job.get("related_links")
                    else None,
                    job.get("link"),
                    job.get("apply_options", [{}])[0].get("link")
                    if isinstance(job.get("apply_options"), list) and job.get("apply_options")
                    else None,
                ),
                "posted_date": _first_non_empty(
                    job.get("detected_extensions", {}).get("posted_at"),
                    job.get("posted_at"),
                ),
            }
        )
    return jobs


class SerpApiJobProvider:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def fetch_jobs(self, params: FetchJobParams) -> list[dict[str, Any]]:
        return self._fetch_jobs_serpapi(
            limit=params.limit,
            query=params.query,
            location=params.location,
            retries=params.retries,
        )

    def _fetch_jobs_serpapi(
        self,
        *,
        limit: int,
        query: str | None,
        location: str | None,
        retries: int,
    ) -> list[dict[str, Any]]:
        if not self._settings.SERPAPI_KEY:
            raise RuntimeError("SERPAPI_KEY is missing in environment")

        location = _normalize_serpapi_location(location)

        base_params: dict[str, Any] = {
            "engine": "google_jobs",
            "q": query,
            "hl": self._settings.DEFAULT_LANGUAGE,
            "google_domain": f"google.{self._settings.DEFAULT_DOMAIN}",
            "api_key": self._settings.SERPAPI_KEY,
        }
        if self._settings.DEFAULT_COUNTRY:
            base_params["gl"] = self._settings.DEFAULT_COUNTRY
        params_dict = {**base_params, "location": location} if location else {**base_params}

        last_data: dict[str, Any] | None = None
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(
                    self._settings.SERPAPI_URL,
                    params=params_dict,
                    timeout=60,
                )
                response.raise_for_status()
                data = response.json()
                last_data = data
                jobs = _extract_serpapi_jobs(data, limit)
                if jobs:
                    return jobs
                if data.get("error"):
                    if _is_no_result_error(str(data.get("error"))):
                        return []
                    logger.info(
                        "SerpAPI returned no results for params location=%s q=%s",
                        params_dict.get("location"),
                        params_dict.get("q"),
                    )
                    break
                return jobs
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                response_preview = ""
                if exc.response is not None and exc.response.text:
                    response_preview = exc.response.text[:300]
                logger.warning(
                    "SerpAPI attempt %s failed: status=%s body=%s",
                    attempt,
                    status,
                    response_preview,
                )
                if status is not None and 500 <= status < 600:
                    time.sleep(2)
                    continue
                raise
            except requests.RequestException as exc:
                logger.warning("SerpAPI attempt %s: Network error %s", attempt, exc)
                time.sleep(2)
        if isinstance(last_data, dict) and last_data.get("error"):
            if _is_no_result_error(str(last_data.get("error"))):
                return []
            logger.warning("SerpAPI final no-result message: %s", last_data.get("error"))
        raise RuntimeError("Failed to fetch jobs from SerpAPI after retries")
