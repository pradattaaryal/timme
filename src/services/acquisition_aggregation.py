from __future__ import annotations

from typing import Any, Callable

from src.services.job_listing_normalization import (
    job_url_from_record,
    normalize_job_title,
    pick_representative_job,
    resolve_listing_url,
)

PRIORITY_MEDIA = frozenset({"Indeed", "Baitoru", "Mynavi Baito"})

_JOB_URL_KEYS = ("url", "link", "URL")


def _job_url_candidates(job: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in _JOB_URL_KEYS:
        raw = job.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            out.append(text)
    return out


def _is_mynavi_baito_url(url: str) -> bool:
    return "baito.mynavi" in url.lower()


def listing_media_name(job: dict[str, Any], normalize_media_name: Callable[[str | None], str | None]) -> str | None:
    """Per-listing media label; must match pre-aggregation fetch semantics (URL rules + company fallback)."""
    for url in _job_url_candidates(job):
        if _is_mynavi_baito_url(url):
            return "Mynavi Baito"
    for url in _job_url_candidates(job):
        name = normalize_media_name(url)
        if name:
            return name
    return job.get("company")


def aggregate_jobs_to_output_row(
    jobs: list[dict[str, Any]],
    *,
    store_name: str,
    query_string: str,
    fetched_at: str,
    normalize_media_name: Callable[[str | None], str | None],
) -> dict[str, Any]:
    """
    Spec v1.1: one output row per store. Representative fields use the best listing (direct URL + real title).
    """
    job_count = len(jobs)
    has_job_listing = job_count > 0
    media_names: list[str | None] = [listing_media_name(j, normalize_media_name) for j in jobs]

    indeed_listed = "○" if any(m == "Indeed" for m in media_names) else ""
    baitoru_listed = "○" if any(m == "Baitoru" for m in media_names) else ""
    mynavi_baito_listed = "○" if any(m == "Mynavi Baito" for m in media_names) else ""
    other_media_count = sum(1 for m in media_names if m not in PRIORITY_MEDIA)

    representative = pick_representative_job(jobs)
    rep_url = resolve_listing_url(job_url_from_record(representative)) if representative else None
    if rep_url is None and representative:
        rep_url = representative.get("url")
    return {
        "store_name": store_name,
        "query_string": query_string,
        "fetched_at": fetched_at,
        "has_job_listing": has_job_listing,
        "job_count": job_count,
        "Indeed_listed": indeed_listed,
        "Baitoru_listed": baitoru_listed,
        "MynaviBaito_listed": mynavi_baito_listed,
        "other_media_count": other_media_count,
        "job_title": normalize_job_title(representative.get("title") if representative else None),
        "job_url": rep_url,
        "job_type": representative.get("job_type") if representative else None,
        "status": "success",
    }
