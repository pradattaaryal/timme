from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus
from html import unescape

import requests
from bs4 import BeautifulSoup

from src.application.ports.job_provider import FetchJobParams
from src.services.job_listing_normalization import (
    href_qualifies_as_job_link,
    is_generic_job_title,
    resolve_listing_url,
)
from src.infrastructure.parallel_api_pool.credentials import (
    RoundRobinCredentialPool,
    get_active_credential,
    get_round_robin_pool,
)

if TYPE_CHECKING:
    from src.config.settings import Settings

logger = logging.getLogger(__name__)

# Detects CJK scripts (JP/CN/KR) after fixing common UTF-8-as-latin1 mojibake.
_CJK_CHAR_RE = re.compile(r"[\u3040-\u30ff\u3400-\u9fff\u3000-\u303f\uac00-\ud7af]")


def _repair_utf8_mojibake(text: str | None) -> str:
    """
    Reverse 'double encoding': UTF-8 bytes were wrongly decoded as latin-1/cp1252.
    Produces strings like 'ï¼ï¼³ï¼¥ï¼‰å¯Œå£«...' instead of Japanese.
    """
    if not text:
        return str(text or "")
    s = str(text)
    if not s.strip():
        return s

    def _cjk_count(value: str) -> int:
        return len(_CJK_CHAR_RE.findall(value))

    best = s
    best_cjk = _cjk_count(s)
    for encoding in ("latin-1", "cp1252"):
        try:
            repaired = s.encode(encoding, errors="strict").decode("utf-8", errors="strict")
        except UnicodeError:
            continue
        cjk = _cjk_count(repaired)
        # Prefer a repair that yields clear CJK text; avoid flipping clean ASCII/UTF-8 strings.
        if cjk >= 2 and cjk > best_cjk:
            best = repaired
            best_cjk = cjk
    return best


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value:
            return value
    return None


def _normalize_http_url(url: str) -> str | None:
    return resolve_listing_url(url)


def _extract_minimum_qualifications(text: str) -> str | None:
    if not text:
        return None
    match = re.search(
        r"Minimum qualifications?\s*[:\-]?\s*([^.;|]+)",
        text,
        flags=re.I,
    )
    return match.group(1).strip() if match else None


def _extract_posted_date(text: str) -> str | None:
    s = (text or "").strip()
    if not s:
        return None

    m = re.search(r"\b(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago\b", s, flags=re.I)
    if m:
        return m.group(0).strip()

    if re.search(r"\btoday\b", s, flags=re.I):
        return "today"

    if re.search(r"\byesterday\b", s, flags=re.I):
        return "yesterday"

    m = re.search(r"(\d+)\s*(分前|時間前|日前|週間前|ヶ月前|か月前|年前)", s)
    return m.group(0).strip() if m else None


def _extract_salary(text: str) -> str | None:
    s = (text or "").strip()
    if not s:
        return None

    salary_pattern = re.compile(
        r"((?:JPY|YEN|¥)\s?\d[\d,]*(?:\s?[-~to]+\s?(?:JPY|YEN|¥)?\s?\d[\d,]*)?\s?(?:/h|/hour|per hour|hourly|/month|per month|/year|per year)?)|(\d[\d,]*(?:\s?[-~to]+\s?\d[\d,]*)?\s?(?:jpy|yen)\b)",
        flags=re.I,
    )
    m = salary_pattern.search(s)
    return m.group(0).strip() if m else None


def _extract_job_type(text: str) -> str | None:
    s = _repair_utf8_mojibake((text or "").strip()).lower()

    if not s:
        return None

    patterns: list[tuple[str, str]] = [

        # FULL TIME
        (
            r"\bfull[_\s-]?time\b|"
            r"\bfulltime\b|"
            r"正社員|"
            r"正職員|"
            r"常勤|"
            r"フルタイム",
            "Full-time",
        ),

        # PART TIME
        (
            r"\bpart[_\s-]?time\b|"
            r"\bparttime\b|"
            r"パート|"
            r"アルバイト|"
            r"アルバイト・パート|"
            r"非常勤|"
            r"時短",
            "Part-time",
        ),

        # CONTRACT (exclude 業務委託 → Freelance)
        (
            r"\bcontract(?:or)?\b|"
            r"契約社員|"
            r"契約職員|"
            r"嘱託",
            "Contract",
        ),

        # TEMPORARY
        (
            r"\btemporary\b|"
            r"\btemp\b|"
            r"\bseasonal\b|"
            r"派遣|"
            r"派遣社員|"
            r"派遣スタッフ|"
            r"短期",
            "Temporary",
        ),

        # INTERNSHIP
        (
            r"\bintern(?:ship)?\b|"
            r"\bintern\b|"
            r"インターン|"
            r"新卒",
            "Internship",
        ),

        # FREELANCE / 業務委託
        (
            r"\bfreelance\b|"
            r"業務委託|"
            r"業務委託契約",
            "Freelance",
        ),

        # PERMANENT (English labels)
        (
            r"\bpermanent\b|"
            r"\bregular\b",
            "Permanent",
        ),

        # REMOTE
        (
            r"\bremote\b|"
            r"在宅|"
            r"リモート|"
            r"テレワーク",
            "Remote",
        ),

        # HYBRID
        (
            r"\bhybrid\b|"
            r"ハイブリッド",
            "Hybrid",
        ),
    ]

    for pattern, normalized in patterns:
        if re.search(pattern, s, flags=re.I):
            return normalized

    return None


def _extract_job_type_from_scripts(soup: BeautifulSoup) -> str | None:
    """
    Extract employment type from JavaScript hydration data embedded in <script> tags.

    Targets:
    - Next.js __NEXT_DATA__ blobs
    - React hydration / window.INITIAL_STATE
    - Apollo state / embedded app JSON
    - Generic JSON fields (employmentType, jobType, etc.)
    - Japanese label patterns (雇用形態, 勤務形態)

    This covers sites like Indeed JP, Townwork, Rikunabi, and Toranet where
    the employment type is NOT in visible HTML but lives inside script payloads.
    """
    script_texts: list[str] = []
    for script in soup.find_all("script"):
        text = script.string or script.get_text(" ", strip=True)
        if text:
            script_texts.append(text)

    if not script_texts:
        return None

    combined = _repair_utf8_mojibake("\n".join(script_texts))

    # Ordered from most-specific / highest-confidence to most-generic.
    patterns: list[str] = [
        # Standard JSON-LD / schema.org fields
        r'"employmentType"\s*:\s*"([^"]+)"',
        r'"employment_type"\s*:\s*"([^"]+)"',
        r'"jobType"\s*:\s*"([^"]+)"',
        r'"job_type"\s*:\s*"([^"]+)"',
        # Next.js / React hydration variants
        r'"workStyle"\s*:\s*"([^"]+)"',
        r'"contractType"\s*:\s*"([^"]+)"',
        r'"workType"\s*:\s*"([^"]+)"',
        r'"employType"\s*:\s*"([^"]+)"',
        r'"positionType"\s*:\s*"([^"]+)"',
        # Rikunabi / Toranet / Japanese portals
        r'"koyoKeitai"\s*:\s*"([^"]+)"',        # 雇用形態 (romaji key)
        r'"kinmuKeitai"\s*:\s*"([^"]+)"',        # 勤務形態 (romaji key)
        # Japanese visible-label patterns that may appear inside JSON strings
        r'雇用形態[:：]\s*([^\s",\\]+)',
        r'勤務形態[:：]\s*([^\s",\\]+)',
        # Catch-all: any key ending in "type" whose value looks like a job type
        r'"[^"]*[Tt]ype[^"]*"\s*:\s*"((?:full.?time|part.?time|contract|temporary|intern|freelance|正社員|パート|アルバイト|契約|派遣|インターン)[^"]*)"',
    ]

    for pattern in patterns:
        m = re.search(pattern, combined, flags=re.I)
        if not m:
            continue
        extracted = m.group(1).strip()
        normalized = _extract_job_type(extracted)
        if normalized:
            logger.debug(
                "Script extraction matched pattern %r → %r → %r",
                pattern,
                _safe_ascii_for_logging(extracted),
                normalized,
            )
            return normalized

    return None


def _infer_job_type_from_url(url: str) -> str | None:
    """
    Infer employment type from domain / URL path conventions used by Japanese job sites.

    Many portals encode job type in the URL structure (e.g. /fulltime/, /part/),
    or the domain itself is exclusively one type (e.g. baitoru = part-time/baito).
    This is a fast, zero-request fallback that handles the most common cases.
    """
    s = (url or "").lower()
    if not s:
        return None

    # ── Domain-level signals ────────────────────────────────────────────────
    domain_rules: list[tuple[str, str]] = [
        # Part-time / baito portals
        ("baitoru", "Part-time"),
        ("gaku-baito", "Part-time"),
        ("baito.com", "Part-time"),
        ("townwork", "Part-time"),        # Townwork is primarily part-time/baito
        ("an-jinfo", "Part-time"),
        ("shiftwork", "Part-time"),
        # Full-time / career portals
        ("rikunabi", "Full-time"),
        ("mynavi", "Full-time"),
        ("doda.jp", "Full-time"),
        ("type.jp", "Full-time"),
        ("en-japan", "Full-time"),
        ("toranet", "Contract"),           # Toranet specialises in contract/派遣
        ("haken", "Temporary"),            # 'haken' (派遣) in domain
    ]
    for fragment, job_type in domain_rules:
        if fragment in s:
            return job_type

    # ── Path-level signals ──────────────────────────────────────────────────
    path_rules: list[tuple[str, str]] = [
        (r"/fulltime/", "Full-time"),
        (r"/full-time/", "Full-time"),
        (r"/seishain/", "Full-time"),       # 正社員
        (r"/part/", "Part-time"),
        (r"/parttime/", "Part-time"),
        (r"/part-time/", "Part-time"),
        (r"/baito/", "Part-time"),
        (r"/arbeit/", "Part-time"),
        (r"/haken/", "Temporary"),          # 派遣
        (r"/contract/", "Contract"),
        (r"/keiyaku/", "Contract"),         # 契約
        (r"/intern/", "Internship"),
        (r"/internship/", "Internship"),
        (r"/freelance/", "Freelance"),
        (r"/gyomu-itaku/", "Freelance"),    # 業務委託
    ]
    for pattern, job_type in path_rules:
        if re.search(pattern, s):
            return job_type

    return None


def _iter_jsonld_nodes(payload: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        nodes.append(payload)
        for key in ("@graph", "graph", "itemListElement", "mainEntity"):
            nested = payload.get(key)
            if isinstance(nested, list):
                for item in nested:
                    nodes.extend(_iter_jsonld_nodes(item))
            elif isinstance(nested, dict):
                nodes.extend(_iter_jsonld_nodes(nested))
    elif isinstance(payload, list):
        for item in payload:
            nodes.extend(_iter_jsonld_nodes(item))
    return nodes


def _empty_page_enrichment() -> dict[str, str | None]:
    return {
        "title": None,
        "company": None,
        "location": None,
        "salary": None,
        "description": None,
        "posted_date": None,
        "job_type": None,
        "minimum_qualifications": None,
    }


def _strip_html_to_text(html_fragment: str | None, *, max_chars: int = 12000) -> str | None:
    if not html_fragment or not str(html_fragment).strip():
        return None
    text = BeautifulSoup(str(html_fragment), "html.parser").get_text(" ", strip=True)
    text = _repair_utf8_mojibake(re.sub(r"\s+", " ", text).strip())
    if not text:
        return None
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "…"
    return text


def _jsonld_job_location_to_string(job_location: Any) -> str | None:
    if job_location is None:
        return None
    if isinstance(job_location, str):
        s = job_location.strip()
        return s or None
    if isinstance(job_location, dict):
        raw_type = job_location.get("@type")
        type_l = str(raw_type).lower() if raw_type is not None else ""
        if "place" in type_l and isinstance(job_location.get("address"), (dict, str)):
            return _jsonld_job_location_to_string(job_location.get("address"))
        parts = [
            job_location.get("streetAddress"),
            job_location.get("addressLocality"),
            job_location.get("addressRegion"),
            job_location.get("postalCode"),
            job_location.get("addressCountry"),
        ]
        joined = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
        if joined:
            return joined
        name = job_location.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(job_location, list):
        for item in job_location:
            s = _jsonld_job_location_to_string(item)
            if s:
                return s
    return None


def _jsonld_hiring_org_to_string(org: Any) -> str | None:
    if org is None:
        return None
    if isinstance(org, str):
        s = org.strip()
        return s or None
    if isinstance(org, dict):
        name = _first_non_empty(org.get("name"), org.get("legalName"))
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(org, list):
        for item in org:
            s = _jsonld_hiring_org_to_string(item)
            if s:
                return s
    return None


def _jsonld_base_salary_to_string(base_salary: Any) -> str | None:
    if base_salary is None:
        return None
    if isinstance(base_salary, str):
        s = base_salary.strip()
        return s or None
    if isinstance(base_salary, (int, float)):
        return str(base_salary)
    if isinstance(base_salary, dict):
        raw_type = str(base_salary.get("@type") or "").lower()
        currency = str(base_salary.get("currency") or base_salary.get("currencyCode") or "").strip()
        if "monetaryamount" in raw_type:
            val = base_salary.get("value")
            if isinstance(val, dict):
                return _jsonld_base_salary_to_string(val)
            if val is not None and str(val).strip():
                unit = str(base_salary.get("unitText") or "").strip()
                cur = currency or "JPY"
                return f"{cur} {val}{(' ' + unit) if unit else ''}".strip()
        min_v = base_salary.get("minValue")
        max_v = base_salary.get("maxValue")
        value = base_salary.get("value")
        cur = currency or "JPY"
        if min_v is not None and max_v is not None and str(min_v) != str(max_v):
            return f"{cur} {min_v} - {max_v}".strip()
        if value is not None:
            return f"{cur} {value}".strip()
        if min_v is not None:
            return f"{cur} {min_v}".strip()
        txt = base_salary.get("text") or base_salary.get("name")
        if isinstance(txt, str) and txt.strip():
            return txt.strip()
    if isinstance(base_salary, list):
        parts = [_jsonld_base_salary_to_string(x) for x in base_salary]
        parts = [p for p in parts if p]
        return " | ".join(parts) if parts else None
    return None


def _jobposting_node_to_flat(node: dict[str, Any]) -> dict[str, str | None]:
    title = _first_non_empty(node.get("title"), node.get("name"))
    title = str(title).strip() if title else None

    company = _jsonld_hiring_org_to_string(node.get("hiringOrganization") or node.get("hiringorganization"))

    location = _jsonld_job_location_to_string(
        _first_non_empty(node.get("jobLocation"), node.get("joblocation"), node.get("applicantLocationRequirements"))
    )

    salary = _jsonld_base_salary_to_string(_first_non_empty(node.get("baseSalary"), node.get("basesalary")))
    if not salary:
        salary = _jsonld_base_salary_to_string(node.get("estimatedSalary"))

    desc_raw = _first_non_empty(node.get("description"), node.get("responsibilities"))
    description = _strip_html_to_text(str(desc_raw)) if desc_raw else None

    raw_employment = _first_non_empty(node.get("employmentType"), node.get("employment_type"))
    if isinstance(raw_employment, list):
        employment_text = " | ".join(str(x).strip() for x in raw_employment if str(x).strip())
    else:
        employment_text = str(raw_employment or "").strip()
    job_type = _extract_job_type(_repair_utf8_mojibake(employment_text)) if employment_text else None

    raw_date = _first_non_empty(node.get("datePosted"), node.get("dateposted"), node.get("validThrough"))
    posted_date = str(raw_date).strip() if raw_date else None

    out = {
        "title": title,
        "company": company,
        "location": location,
        "salary": salary,
        "description": description,
        "posted_date": posted_date,
        "job_type": job_type,
        "minimum_qualifications": _extract_minimum_qualifications(description or "") or None,
    }
    if not any(out.values()):
        return _empty_page_enrichment()
    return out


def _richness(meta: dict[str, str | None]) -> int:
    return sum(1 for v in meta.values() if v)


def _merge_enrichment_layers(*layers: dict[str, str | None]) -> dict[str, str | None]:
    merged = _empty_page_enrichment()
    keys = list(merged.keys())
    for layer in layers:
        for k in keys:
            if merged.get(k):
                continue
            val = layer.get(k) if layer else None
            if val:
                merged[k] = val
    return merged


def _extract_jobposting_metadata_from_jsonld(soup: BeautifulSoup) -> dict[str, str | None]:
    best = _empty_page_enrichment()
    scripts = soup.find_all("script", attrs={"type": re.compile(r"application/ld\+json", flags=re.I)})
    for script in scripts:
        raw_json = script.string or script.get_text(strip=True)
        if not raw_json:
            continue
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            continue

        for node in _iter_jsonld_nodes(parsed):
            if not isinstance(node, dict):
                continue
            raw_type = node.get("@type")
            if isinstance(raw_type, list):
                type_names = {str(t).strip().lower() for t in raw_type}
            else:
                type_names = {str(raw_type).strip().lower()} if raw_type is not None else set()
            if "jobposting" not in type_names:
                continue
            flat = _jobposting_node_to_flat(node)
            if _richness(flat) > _richness(best):
                best = flat
    return best


def _is_indeed_japan_job_url(url: str) -> bool:
    u = (url or "").lower()
    if "jp.indeed.com" in u or "indeed.co.jp" in u:
        return True
    if "indeed.com" not in u:
        return False
    if "hl=ja" in u or "gl=jp" in u or "locale=ja" in u or "locale=ja_jp" in u:
        return True
    if "jk=" in u or "/viewjob" in u or "/rc/clk" in u or "/pagead/clk" in u:
        if "co.jp" in u or "jp.indeed" in u:
            return True
    return False


def _indeed_candidate_score(d: dict[str, Any]) -> int:
    keys = set(d.keys())
    score = 0
    if keys.intersection({"jobTitle", "jobTitleHtml", "jobTitleLanguage", "displayTitle"}):
        score += 4
    if keys.intersection({"jobCompany", "companyName", "employerName", "company", "employer"}):
        score += 2
    if keys.intersection({"formattedLocation", "jobLocation", "jobLocationCity", "location", "city", "state"}):
        score += 2
    if keys.intersection({"jobDescription", "description", "sanitizedJobDescription", "jobDescriptionHtml"}):
        score += 2
    if keys.intersection({"salaryText", "formattedCompensation", "compensation", "baseSalary", "salarySnippet"}):
        score += 1
    if keys.intersection({"datePublished", "datePosted", "createDate", "postedDate", "formattedDate"}):
        score += 1
    if keys.intersection({"employmentType", "jobTypes", "formattedEmploymentStatus", "remoteWorkModel"}):
        score += 1
    return score


def _indeed_flatten_record(d: dict[str, Any]) -> dict[str, str | None]:
    def pick_from(mapping: dict[str, Any], *keys: str) -> str | None:
        for k in keys:
            if k not in mapping or mapping[k] is None:
                continue
            v = mapping[k]
            if isinstance(v, str) and v.strip():
                return _repair_utf8_mojibake(v.strip())
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                s = str(v).strip()
                if s:
                    return s
        return None

    def pick_str(*keys: str) -> str | None:
        return pick_from(d, *keys)

    title = pick_str("jobTitle", "jobTitleHtml", "displayTitle", "title", "positionTitle")
    company = pick_str("jobCompany", "companyName", "employerName", "company", "employer", "companyLabel")

    location = pick_str(
        "formattedLocation",
        "jobLocationText",
        "formattedLocationFull",
        "location",
        "city",
    )
    if not location and isinstance(d.get("jobLocation"), dict):
        jl = d["jobLocation"]
        if isinstance(jl, dict):
            location = pick_from(
                jl,
                "formattedLocation",
                "jobLocationText",
                "city",
                "state",
                "country",
                "label",
            ) or _jsonld_job_location_to_string(jl)

    salary = pick_str("salaryText", "formattedCompensation", "salarySnippet")
    if not salary and isinstance(d.get("compensation"), dict):
        comp = d["compensation"]
        salary = pick_from(comp, "formattedText", "text", "label", "displayText", "baseSalary")
        if not salary and isinstance(comp.get("baseSalary"), (dict, str, int, float)):
            salary = _jsonld_base_salary_to_string(comp.get("baseSalary"))
    if not salary and d.get("baseSalary") is not None:
        salary = _jsonld_base_salary_to_string(d.get("baseSalary"))

    desc_raw = d.get("jobDescription") or d.get("description") or d.get("sanitizedJobDescription") or d.get("jobDescriptionHtml")
    description = _strip_html_to_text(str(desc_raw)) if desc_raw else None

    posted = pick_str("datePublished", "datePosted", "createDate", "postedDate", "formattedDate", "date")

    emp = d.get("employmentType") or d.get("formattedEmploymentStatus") or d.get("remoteWorkModel")
    job_type: str | None = None
    if isinstance(emp, list):
        emp_text = " | ".join(str(x).strip() for x in emp if str(x).strip())
        job_type = _extract_job_type(emp_text)
    elif isinstance(emp, str) and emp.strip():
        job_type = _extract_job_type(emp)
    jt = d.get("jobTypes")
    if not job_type and isinstance(jt, list):
        job_type = _extract_job_type(" | ".join(str(x) for x in jt if str(x).strip()))

    minimum = _extract_minimum_qualifications(description or "") or None

    return {
        "title": title,
        "company": company,
        "location": location,
        "salary": salary,
        "description": description,
        "posted_date": posted,
        "job_type": job_type,
        "minimum_qualifications": minimum,
    }


def _indeed_walk_best_dict(obj: Any, best: list[tuple[int, dict[str, Any]]]) -> None:
    if isinstance(obj, dict):
        score = _indeed_candidate_score(obj)
        if score >= 4:
            best.append((score, obj))
        for v in obj.values():
            _indeed_walk_best_dict(v, best)
    elif isinstance(obj, list):
        for item in obj:
            _indeed_walk_best_dict(item, best)


def _parse_json_from_script_tag(soup: BeautifulSoup, script_id: str) -> Any | None:
    node = soup.find("script", id=script_id)
    if node is None:
        return None
    raw = node.string or node.get_text(strip=True)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _extract_json_object_after(html: str, needle: str) -> dict[str, Any] | None:
    pos = html.find(needle)
    if pos == -1:
        return None
    brace = html.find("{", pos)
    if brace == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    quote = ""
    for i in range(brace, len(html)):
        ch = html[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
                quote = ""
            continue
        if ch in "\"'":
            in_str = True
            quote = ch
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                blob = html[brace : i + 1]
                try:
                    return json.loads(blob)
                except json.JSONDecodeError:
                    return None
    return None


def _extract_indeed_jp_structured_metadata(soup: BeautifulSoup, html: str) -> dict[str, str | None]:
    candidates: list[tuple[int, dict[str, Any]]] = []

    next_data = _parse_json_from_script_tag(soup, "__NEXT_DATA__")
    if next_data is not None:
        _indeed_walk_best_dict(next_data, candidates)

    for marker in ("window._initialData", "window.__INITIAL_DATA__", "window.__PRELOADED_STATE__"):
        blob = _extract_json_object_after(html, marker)
        if isinstance(blob, dict):
            _indeed_walk_best_dict(blob, candidates)

    for script in soup.find_all("script"):
        if script.get("id") == "__NEXT_DATA__":
            continue
        txt = script.string or script.get_text(" ", strip=True)
        if not txt or len(txt) < 80:
            continue
        if "jobTitle" not in txt and "jobCompany" not in txt:
            continue
        if "{" not in txt:
            continue
        # Heuristic: try whole script as JSON (some pages embed pure JSON)
        try:
            parsed = json.loads(txt)
            _indeed_walk_best_dict(parsed, candidates)
        except json.JSONDecodeError:
            continue

    if not candidates:
        return _empty_page_enrichment()

    candidates.sort(key=lambda t: t[0], reverse=True)
    best_flat = _empty_page_enrichment()
    for _, rec in candidates[:12]:
        flat = _indeed_flatten_record(rec)
        best_flat = _merge_enrichment_layers(best_flat, flat)
        if _richness(best_flat) >= 6:
            break
    return best_flat


def _extract_card_title(text: str) -> str | None:
    s = (text or "").strip()
    if not s:
        return None

    generic_tokens = {
        "indeed",
        "indeed.com",
        "baitoru",
        "バイトル",
        "タウンワーク",
        "townwork",
        "はたらいく",
        "froma",
    }
    normalized = s.lower()
    if normalized in generic_tokens:
        return None
    if len(s) < 3:
        return None
    if is_generic_job_title(s):
        return None
    return s


def _looks_generic_title(text: str | None) -> bool:
    return is_generic_job_title(text)


def _safe_ascii_for_logging(value: Any) -> Any:
    """
    Prevent UnicodeEncodeError in some Windows console/logging setups.
    Only affects log output, not business logic.
    """
    if isinstance(value, str):
        try:
            value.encode("cp1252")
            return value
        except UnicodeEncodeError:
            return value.encode("unicode_escape").decode("ascii", errors="replace")
    return value


class OxylabsJobProvider:
    def __init__(
        self,
        settings: Settings,
        *,
        credential_pool: RoundRobinCredentialPool | None = None,
    ) -> None:
        self._settings = settings
        self._credential_pool = credential_pool if credential_pool is not None else get_round_robin_pool(
            settings
        )
        self._enrich_max_workers = max(1, settings.OXYLABS_ENRICH_MAX_WORKERS)

    def _resolve_credential_pool(self) -> RoundRobinCredentialPool | None:
        if self._credential_pool is not None:
            return self._credential_pool
        pool = get_round_robin_pool(self._settings)
        if pool is not None:
            self._credential_pool = pool
        return self._credential_pool

    def _ensure_credentials_configured(self) -> None:
        if self._resolve_credential_pool() is not None:
            return
        user = self._settings.OXYLABS_USERNAME
        password = self._settings.OXYLABS_PASSWORD
        if user and password:
            return
        raise RuntimeError("OXYLABS credentials missing")

    def _auth_for_request(self) -> tuple[str, str]:
        active = get_active_credential()
        if active is not None:
            return active.username, active.password
        user = self._settings.OXYLABS_USERNAME
        password = self._settings.OXYLABS_PASSWORD
        if user and password:
            return user, password
        raise RuntimeError(
            "Oxylabs API call without an active credential; use credential pool borrow()"
        )

    def _post_oxylabs(
        self,
        payload: dict[str, Any],
        *,
        timeout: int = 120,
    ) -> requests.Response:
        pool = self._resolve_credential_pool()
        if pool is not None:
            with pool.borrow():
                return self._post_oxylabs_inner(payload, timeout=timeout)
        return self._post_oxylabs_inner(payload, timeout=timeout)

    def _post_oxylabs_inner(
        self,
        payload: dict[str, Any],
        *,
        timeout: int = 120,
    ) -> requests.Response:
        user, password = self._auth_for_request()
        response = requests.post(
            self._settings.OXYLABS_URL,
            json=payload,
            auth=(user, password),
            timeout=timeout,
        )
        response.raise_for_status()
        return response

    def fetch_jobs(self, params: FetchJobParams) -> list[dict[str, Any]]:
        return self._fetch_jobs_oxylabs(
            limit=params.limit,
            query=params.query,
            location=params.location,
            retries=params.retries,
            enrich=params.enrich,
        )

    def _fetch_jobs_oxylabs(
        self,
        *,
        limit: int,
        query: str | None,
        location: str | None,
        retries: int,
        enrich: bool = False,
    ) -> list[dict[str, Any]]:

        self._ensure_credentials_configured()

        query_str = (query or "").strip()
        hl = self._settings.DEFAULT_LANGUAGE
        gl = self._settings.DEFAULT_COUNTRY
        domain = (self._settings.DEFAULT_DOMAIN or "com").strip().lower()
        if not re.fullmatch(r"[a-z.]+", domain):
            domain = "com"

        # Google Jobs listings are served behind `ibp=htl;jobs`.
        # Use the configured Google domain (e.g. co.jp) to reduce consent-page responses.
        google_url = (
            f"https://www.google.{domain}/search"
            f"?q={quote_plus(query_str)}&ibp=htl;jobs&hl={hl}&gl={gl}"
        )

        payload: dict[str, Any] = {
            "source": "google",
            "url": google_url,
            "user_agent_type": "desktop",
            "render": "html",
            "parse": False,
        }

        if location:
            # Keep caller-provided location as-is. Forcing ", Japan" breaks
            # non-Japan lookups (e.g. "Warsaw Poland" -> "Warsaw Poland, Japan").
            payload["geo_location"] = location.strip()

        last_exc: Exception | None = None

        for attempt in range(1, max(1, retries) + 1):
            try:
                response = self._post_oxylabs(payload, timeout=120)
                raw_html = self._html_from_oxylabs_response(response.json())
                if raw_html:
                    jobs = self._jobs_from_rendered_html(raw_html, location=location)
                    standardized = self._standardize_jobs(jobs, limit, enrich=enrich)
                    if standardized:
                        return standardized

                # Successful response but no jobs. Retrying the exact same request is
                # unlikely to help and can make Celery tasks appear "stuck".
                logger.info("No valid jobs found for query: %s", _safe_ascii_for_logging(query))
                return []

            except requests.RequestException as exc:
                last_exc = exc
                logger.warning("Attempt %s failed: %s", attempt, exc)
                time.sleep(2)

        if last_exc:
            raise RuntimeError("Failed after retries") from last_exc

        return []

    @staticmethod
    def _html_from_oxylabs_response(data: dict[str, Any]) -> str | None:
        results = data.get("results") or []
        if not results:
            return None
        content = results[0].get("content")
        return str(content) if content else None

    def _jobs_from_rendered_html(self, html: str, *, location: str | None) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "").strip()
            if not href:
                continue
            if not href_qualifies_as_job_link(href):
                continue

            normalized_url = resolve_listing_url(unescape(href))
            if not normalized_url:
                continue
            if normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)

            # Google commonly stores company/source in the title attribute.
            company_raw = str(anchor.get("title") or "").strip()
            company = re.sub(r"^(?:直接応募|応募)\s*[:：]\s*", "", company_raw).strip() or None

            card = anchor.find_parent("div", class_=re.compile(r"(sMn82b|MQUd2b|iFjolb)"))
            if card is None:
                card = anchor.find_parent("li")
            if card is None:
                card = anchor.find_parent("div")

            card_text = ""
            card_title = None
            if card is not None:
                card_text = str(card.get_text(" ", strip=True) or "").strip()
                title_node = card.select_one("h3.QJPWVe, h3")
                if title_node is not None:
                    card_title = _extract_card_title(str(title_node.get_text(strip=True) or ""))

            anchor_title = _extract_card_title(str(anchor.get_text(" ", strip=True) or "").strip())
            extracted_posted_date = _extract_posted_date(card_text)
            extracted_job_type = _extract_job_type(card_text)

            jobs.append(
                {
                    "job_title": card_title or anchor_title,
                    "company_name": company,
                    "location": location,
                    "URL": normalized_url,
                    "posted_date_raw": extracted_posted_date,
                    "job_type_raw": extracted_job_type,
                    "card_text": card_text,
                }
            )

        return jobs

    def _fetch_provider_page_html_universal(self, url: str) -> str | None:
        try:
            self._ensure_credentials_configured()
        except RuntimeError:
            return None

        payload: dict[str, Any] = {
            "source": "universal",
            "url": url,
            "render": "html",
            "parse": False,
            "user_agent_type": "desktop",
        }
        try:
            response = self._post_oxylabs(payload, timeout=120)
            data = response.json()
        except requests.RequestException:
            return None

        results = data.get("results") or []
        if not results:
            return None
        content = results[0].get("content")
        if not content:
            return None
        if isinstance(content, dict):
            nested = content.get("content")
            if isinstance(nested, str) and nested.strip():
                return nested
            return None
        return str(content)

    def _enrich_job_fields_from_url(self, url: str) -> dict[str, str | None]:
        empty = _empty_page_enrichment()
        url_norm = _normalize_http_url(url)
        if not url_norm:
            return empty

        html = self._fetch_provider_page_html_universal(url_norm)
        if not html or not html.strip():
            return empty

        soup = BeautifulSoup(html, "html.parser")

        jsonld_layer = _extract_jobposting_metadata_from_jsonld(soup)
        indeed_layer = (
            _extract_indeed_jp_structured_metadata(soup, html)
            if _is_indeed_japan_job_url(url_norm)
            else empty
        )
        structured = _merge_enrichment_layers(jsonld_layer, indeed_layer)

        og_title = soup.select_one("meta[property='og:title']")
        title_text = (
            str(og_title.get("content") or "").strip()
            if og_title is not None
            else str((soup.title.string if soup.title else "") or "").strip()
        )
        title_text = re.sub(r"\s*[-|｜]\s*(Indeed|バイトル|タウンワーク|はたらいく).*$", "", title_text, flags=re.I).strip()
        title_text = _repair_utf8_mojibake(title_text) or None
        if _looks_generic_title(title_text):
            title_text = None

        og_desc = soup.select_one("meta[property='og:description']")
        og_description = (
            str(og_desc.get("content") or "").strip()
            if og_desc is not None and og_desc.get("content")
            else None
        )

        script_job_type = _extract_job_type_from_scripts(soup)
        body_text = _repair_utf8_mojibake(soup.get_text(" ", strip=True))

        title = _first_non_empty(structured.get("title"), title_text)
        description = _first_non_empty(
            structured.get("description"),
            _strip_html_to_text(og_description) if og_description else None,
        )
        minimum_qualifications = _first_non_empty(
            structured.get("minimum_qualifications"),
            _extract_minimum_qualifications(description or ""),
            _extract_minimum_qualifications(body_text),
        )

        return {
            "title": title,
            "company": structured.get("company"),
            "location": structured.get("location"),
            "salary": structured.get("salary"),
            "description": description,
            "posted_date": _first_non_empty(
                structured.get("posted_date"),
                _extract_posted_date(body_text),
            ),
            "job_type": _first_non_empty(
                structured.get("job_type"),
                script_job_type,
                _extract_job_type(body_text),
            ),
            "minimum_qualifications": minimum_qualifications,
        }

    def _normalize_minimum_qualifications(self, raw: Any) -> str | None:
        if isinstance(raw, list):
            cleaned = [
                _repair_utf8_mojibake(str(x).strip())
                for x in raw
                if str(x).strip()
            ]
            if cleaned:
                return " | ".join(cleaned)
            return None
        if raw:
            return _repair_utf8_mojibake(str(raw).strip())
        return None

    def _normalize_job_type_raw(self, raw: Any) -> str | None:
        if isinstance(raw, list):
            cleaned_job_types = [
                _repair_utf8_mojibake(str(x).strip())
                for x in raw
                if str(x).strip()
            ]
            if cleaned_job_types:
                return " | ".join(cleaned_job_types)
            return None
        if raw:
            return _repair_utf8_mojibake(str(raw).strip())
        return None

    def _standardize_jobs(
        self,
        raw_jobs: list[dict[str, Any]],
        limit: int,
        *,
        enrich: bool = False,
    ) -> list[dict[str, Any]]:

        jobs: list[dict[str, Any]] = []
        pending: list[tuple[dict[str, Any], str | None]] = []
        enrich_targets: list[tuple[int, str]] = []
        enrich_cap = max(1, limit) if enrich else 0

        for item in raw_jobs:
            url_candidate = _first_non_empty(
                item.get("url"),
                item.get("link"),
                item.get("URL"),
            )
            provider_apply_url = (
                _normalize_http_url(str(url_candidate))
                if isinstance(url_candidate, str)
                else None
            )
            pending_idx = len(pending)
            pending.append((item, provider_apply_url))
            if (
                enrich
                and provider_apply_url
                and len(enrich_targets) < enrich_cap
            ):
                enrich_targets.append((pending_idx, provider_apply_url))

        enriched_map: dict[int, dict[str, str | None]] = {}
        if enrich_targets:
            pool = self._resolve_credential_pool()
            concurrent_slots = (
                pool.max_concurrent_requests if pool is not None else len(enrich_targets)
            )
            enrich_workers = max(
                1,
                min(len(enrich_targets), self._enrich_max_workers, concurrent_slots),
            )
            with ThreadPoolExecutor(max_workers=enrich_workers) as enrich_ex:
                future_to_idx = {
                    enrich_ex.submit(self._enrich_job_fields_from_url, url): idx
                    for idx, url in enrich_targets
                }
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        enriched_map[idx] = future.result()
                    except Exception:
                        enriched_map[idx] = _empty_page_enrichment()

        for pending_idx, (item, provider_apply_url) in enumerate(pending):
            enriched = enriched_map.get(pending_idx, _empty_page_enrichment())
            title_seed = _first_non_empty(item.get("title"), item.get("job_title"))

            title = _first_non_empty(enriched.get("title"), title_seed)
            if not title:
                for candidate in (
                    item.get("company_name"),
                    item.get("company"),
                    item.get("posted_via"),
                ):
                    text = str(candidate or "").strip()
                    if text and not _looks_generic_title(text):
                        title = text
                        break
            if not title:
                continue

            title = _repair_utf8_mojibake(title)

            desc = _repair_utf8_mojibake(
                str(
                    _first_non_empty(
                        enriched.get("description"),
                        item.get("description"),
                        item.get("desc"),
                    )
                    or ""
                )
            )
            url_text = (
                resolve_listing_url(
                    _first_non_empty(item.get("url"), item.get("link"), item.get("URL"))
                )
                or ""
            )
            posted_date_raw = _repair_utf8_mojibake(str(item.get("posted_date_raw") or "").strip())
            job_type_raw = item.get("job_type_raw")
            minimum_qualifications_raw = item.get("minimum_qualifications")
            base_mq = self._normalize_minimum_qualifications(minimum_qualifications_raw)
            minimum_qualifications = _first_non_empty(
                enriched.get("minimum_qualifications"),
                base_mq,
                _extract_minimum_qualifications(desc),
            )
            job_type_text = self._normalize_job_type_raw(job_type_raw)

            posted_date = _first_non_empty(
                enriched.get("posted_date"),
                item.get("posted_date"),
                item.get("date"),
                item.get("posted_at"),
                posted_date_raw,
                _extract_posted_date(desc),
                _extract_posted_date(title),
                _extract_posted_date(url_text),
            )

            salary = _first_non_empty(
                enriched.get("salary"),
                item.get("salary"),
                item.get("pay"),
                _extract_salary(desc),
                _extract_salary(title),
                _extract_salary(url_text),
            )

            job_type = _first_non_empty(
                enriched.get("job_type"),
                item.get("job_type"),
                item.get("employment_type"),
                item.get("employmentType"),
                job_type_text,
                _infer_job_type_from_url(url_text),
                _extract_job_type(desc),
                _extract_job_type(title),
                _extract_job_type(url_text),
            )

            company_val = _first_non_empty(
                enriched.get("company"),
                item.get("company_name"),
                item.get("company"),
                item.get("posted_via"),
            )
            location_val = _first_non_empty(
                enriched.get("location"),
                item.get("location"),
                item.get("job_location"),
            )

            jobs.append(
                {
                    "title": title,
                    "company": _repair_utf8_mojibake(str(company_val or "N/A")),
                    "location": _repair_utf8_mojibake(str(location_val or "N/A")),
                    "minimum_qualifications": _repair_utf8_mojibake(
                        minimum_qualifications or _extract_minimum_qualifications(desc) or ""
                    )
                    or None,
                    "url": url_text or None,
                    "job_type": job_type,
                    "posted_date": posted_date,
                    "salary": salary,
                }
            )
            if len(jobs) >= max(1, limit):
                break

        return jobs