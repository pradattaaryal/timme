from __future__ import annotations

import math
import re
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

_URL_IN_TITLE_RE = re.compile(r"(?:https?://|www\.)[^\s]+", re.IGNORECASE)
_BARE_DOMAIN_TITLE_RE = re.compile(r"^[a-z0-9][-a-z0-9]*\.[a-z]{2,}(/|$)", re.IGNORECASE)
_RECRUIT_SITE_TITLE_RE = re.compile(r"採用(?:情報)?サイト|採用ページ", re.IGNORECASE)

GENERIC_JOB_TITLE_TOKENS = frozenset(
    {
        "indeed",
        "indeed.com",
        "baitoru",
        "バイトル",
        "タウンワーク",
        "townwork",
        "はたらいく",
        "froma",
        "求人ボックス",
        "hrmos",
        "engage",
        "recruit",
        "wantedly",
        "ジョブカン",
        "jobcan",
        "タレントパレット",
    }
)

_JOB_URL_KEYS = ("url", "link", "URL")


def unwrap_google_redirect_url(url: str) -> str:
    """Extract destination URL from Google /url?...&url=https://... wrappers."""
    u = (url or "").strip()
    if not u:
        return u
    lower = u.lower()
    is_google_wrapper = u.startswith("/url?") or (
        "google." in lower and "/url?" in lower
    )
    if not is_google_wrapper:
        return u

    parsed = urlparse(u if "://" in u else f"https://www.google.com{u}")
    for key in ("url", "q"):
        values = parse_qs(parsed.query).get(key) or []
        for raw in values:
            target = unquote(str(raw).strip())
            if target.startswith(("http://", "https://")):
                return target
    return u


def resolve_listing_url(url: str | None) -> str | None:
    """Return a direct http(s) job URL, unwrapping Google redirects when possible."""
    if url is None:
        return None
    u = unwrap_google_redirect_url(str(url).strip())
    if not u:
        return None
    if u.startswith("//"):
        u = f"https:{u}"
    if u.startswith(("http://", "https://")):
        return u
    return None


def href_qualifies_as_job_link(href: str) -> bool:
    """True when href is a Google Jobs apply link (not accidental /jobs/ in query text)."""
    h = (href or "").strip()
    if not h:
        return False
    if "google_jobs_apply" in h.lower():
        return True

    candidate = unwrap_google_redirect_url(h)
    parsed = urlparse(candidate if "://" in candidate else f"https://invalid.local{candidate}")
    return "/jobs/" in (parsed.path or "").lower()


def is_generic_job_title(text: str | None) -> bool:
    """Board/site labels that are not actual job posting titles."""
    s = (text or "").strip()
    if not s:
        return True
    if s.lower() in GENERIC_JOB_TITLE_TOKENS:
        return True
    if _RECRUIT_SITE_TITLE_RE.search(s):
        return True
    return False


def _title_looks_like_url(value: str) -> bool:
    text = value.strip()
    if not text:
        return True
    lower = text.lower()
    if lower.startswith(("http://", "https://", "www.")):
        return True
    if _URL_IN_TITLE_RE.search(text):
        return True
    return bool(_BARE_DOMAIN_TITLE_RE.match(lower))


def normalize_job_title(raw: Any) -> float | str:
    """Return a display title, or NaN when missing, URL-like, or a platform/site label."""
    if raw is None:
        return math.nan
    text = str(raw).strip()
    if not text or text.lower() == "nan":
        return math.nan
    if _title_looks_like_url(text) or is_generic_job_title(text):
        return math.nan
    return text


def job_url_from_record(job: dict[str, Any]) -> str | None:
    for key in _JOB_URL_KEYS:
        raw = job.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            return text
    return None


def pick_representative_job(jobs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Prefer a listing with a direct https URL and a non-generic title (API order as tie-break)."""
    if not jobs:
        return None

    best: dict[str, Any] | None = None
    best_score = -1
    for idx, job in enumerate(jobs):
        url_ok = 1 if resolve_listing_url(job_url_from_record(job)) else 0
        title_val = normalize_job_title(job.get("title"))
        title_ok = 0 if isinstance(title_val, float) and math.isnan(title_val) else 1
        score = url_ok * 2 + title_ok
        if score > best_score or (score == best_score and best is None):
            best = job
            best_score = score
    return best or jobs[0]
