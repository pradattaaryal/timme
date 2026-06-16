"""Slack webhook notifications for acquisition pipeline progress, errors, and completion."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from src.config.settings import settings

logger = logging.getLogger(__name__)

_WEBHOOK_URL = (settings.SLACK_WEBHOOK_URL or "").strip()
_WEBHOOK_TIMEOUT = 10  # seconds


def _should_send() -> bool:
    return bool(_WEBHOOK_URL)


def _post_slack(payload: dict[str, Any]) -> bool:
    """POST to Slack webhook, return True on success."""
    if not _should_send():
        return False
    try:
        resp = requests.post(_WEBHOOK_URL, json=payload, timeout=_WEBHOOK_TIMEOUT)
        if resp.status_code == 200:
            return True
        logger.warning(
            "Slack webhook returned status %s: %s",
            resp.status_code,
            resp.text[:300] or resp.reason,
        )
        return False
    except requests.RequestException:
        logger.exception("Slack webhook request failed")
        return False


def _format_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def send_slack_message(text: str) -> bool:
    """Send an arbitrary plain-text message to Slack via webhook."""
    payload = {"text": text}
    return _post_slack(payload)


def send_progress_notice(
    *,
    run_key: str,
    total: int,
    processed: int,
    status_counts: dict[str, int],
) -> bool:
    """Progress report at scheduled hours (9am, 2pm, 5pm)."""
    pct = round((processed / total) * 100, 1) if total else 0
    now_jp = datetime.now(timezone.utc).astimezone()
    time_str = now_jp.strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"acquisition_progress",
        f"run_key: `{run_key}`",
        f"time: {time_str}",
        "",
        f"*Progress: {pct}%",
        f"Processed {processed}/{total} stores",
        "",
    ]
    if status_counts:
        parts = []
        for key, label in [("success", ":large_green_circle:"), ("no_result", ":white_circle:"), ("error", ":red_circle:")]:
            if key in status_counts and status_counts[key] > 0:
                parts.append(f"{label} {label}: {status_counts[key]}")
        if parts:
            lines.append("Status: " + " | ".join(parts))

    return send_slack_message("\n".join(lines))


def send_error_notice(
    *,
    run_key: str,
    batch_id: str,
    failed_stores: list[str] | None = None,
    error_msg: str = "",
) -> bool:
    """Error alert when a batch encounters errors."""
    lines = [
        ":rotating_light: *acquisition_error*",
        f"run_key: `{run_key}`",
        f"batch_id: `{batch_id}`",
        "",
    ]
    if error_msg:
        lines.append(f"Error: {error_msg[:500]}")
    if failed_stores:
        lines.append("")
        lines.append("Failed queries (up to 5):")
        for i, q in enumerate(failed_stores, 1):
            lines.append(f"{i}. {q[:200]}")
    return send_slack_message("\n".join(lines))


def send_completion_notice(
    *,
    run_key: str,
    total_records: int,
    success: int,
    no_result: int,
    error: int,
    success_rate: float,
    api_requests: int,
    output_csv: str = "",
    drive_url: str = "",
) -> bool:
    """Completion summary after pipeline finishes."""
    overall_status = ":large_green_circle: completed" if error == 0 else ":red_circle: completed with errors"
    time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f":white_check_mark: *acquisition_complete* ({overall_status})",
        f"time: {time_str}",
        f"run_key: `{run_key}`",
        "",
        f"Results: {total_records} total | {success} success | {no_result} no_result | {error} error",
        f"Success rate: {success_rate}%",
        f"API requests: {api_requests}",
    ]
    if drive_url:
        lines.append(f"[Google Drive]({drive_url})")
    elif output_csv:
        lines.append(f"Output: `{output_csv}`")
    return send_slack_message("\n".join(lines))
