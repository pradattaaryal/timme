from __future__ import annotations

import re

from src.config.settings import settings


BRAND_JP_ALIASES = {
    "familymart": "ファミリーマート",
    "lawson": "ローソン",
    "seven-eleven": "セブンイレブン",
    "7-eleven": "セブンイレブン",
    "mcdonald": "マクドナルド",
}


def build_query(city_ward_name: str, store_name: str, char_limit: int) -> str:
    effective_char_limit = min(150, max(1, char_limit))
    # Prefer store name first, then ward/city.
    # This better matches common job intent query patterns in Google Jobs.
    query = f"{store_name} {city_ward_name}".strip()
    if len(query) <= effective_char_limit:
        return query
    return query[:effective_char_limit].strip()


def _brand_jp_alias(store_name: str) -> str | None:
    lower = store_name.lower()
    for token, alias in BRAND_JP_ALIASES.items():
        if token in lower:
            return alias
    return None


def build_query_variants(
    city_ward_name: str,
    store_name: str,
    char_limit: int,
    city_ward_name_jp: str | None = None,
    store_name_jp: str | None = None,
    preferred_suffixes: list[str] | None = None,
) -> list[str]:
    effective_char_limit = min(150, max(1, char_limit))
    suffixes = [s.strip() for s in settings.QUERY_SUFFIXES.split(",") if s.strip()]
    if preferred_suffixes:
        # Keep caller-provided suffixes first, without duplicating existing ones.
        seen = {s.lower() for s in suffixes}
        prefix = [s for s in preferred_suffixes if s and str(s).strip() and str(s).strip().lower() not in seen]
        suffixes = [*prefix, *suffixes]
    brand_alias = _brand_jp_alias(store_name)

    raw_base = f"{store_name} {city_ward_name}".strip()
    raw_store_only = store_name.strip()

    raw_brand_base_city_first = f"{city_ward_name} {brand_alias}".strip() if brand_alias else ""
    raw_brand_base_store_first = f"{brand_alias} {city_ward_name}".strip() if brand_alias else ""

    def truncate_base(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        if len(s) <= effective_char_limit:
            return s
        return s[:effective_char_limit].strip()

    def prefix_with_suffix(prefix_raw: str, suffix: str) -> str:
        """
        Build: "<prefix> <suffix>" while preserving the full suffix under max length.
        This avoids truncating away job-intent terms like "求人"/"アルバイト".
        """
        prefix_raw = (prefix_raw or "").strip()
        suffix = (suffix or "").strip()
        if not suffix:
            return truncate_base(prefix_raw)
        if not prefix_raw:
            return suffix[:effective_char_limit].strip()

        max_prefix_len = effective_char_limit - len(suffix) - 1  # space
        if max_prefix_len <= 0:
            return suffix[:effective_char_limit].strip()

        prefix = prefix_raw[:max_prefix_len].strip()
        candidate = f"{prefix} {suffix}".strip()
        return candidate[:effective_char_limit].strip()

    candidates: list[str] = []

    # Prefer Japanese-form queries early when JP columns are available.
    if city_ward_name_jp and store_name_jp:
        jp_raw_base = f"{store_name_jp} {city_ward_name_jp}".strip()
        for suffix in suffixes:
            candidates.append(prefix_with_suffix(jp_raw_base, suffix))
    elif city_ward_name_jp and brand_alias:
        jp_brand_base = f"{brand_alias} {city_ward_name_jp}".strip()
        for suffix in suffixes:
            candidates.append(prefix_with_suffix(jp_brand_base, suffix))

    # Try job-intent queries first (suffix appended at end).
    for suffix in suffixes:
        candidates.append(prefix_with_suffix(raw_base, suffix))
    for suffix in suffixes:
        candidates.append(prefix_with_suffix(raw_store_only, suffix))

    # Brand alias variants (JP names).
    if brand_alias:
        for suffix in suffixes:
            candidates.append(prefix_with_suffix(raw_brand_base_city_first, suffix))
        for suffix in suffixes:
            candidates.append(prefix_with_suffix(raw_brand_base_store_first, suffix))

    # Fallback: non-intent base query last.
    base_only = truncate_base(raw_base)
    if base_only:
        candidates.append(base_only)

    # Preserve order but deduplicate.
    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = (candidate or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)

    return unique

