"""Slack webhook notifications for acquisition pipeline progress, errors, and completion."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from src.config.settings import settings

logger = logging.getLogger(__name__)

_WEBHOOK_URL = (settings.SLACK_WEBHOOK_URL or "").strip()
_WEBHOOK_TIMEOUT = 10  # seconds
JST = timezone(timedelta(hours=9))
SCHEDULED_PROGRESS_HOURS_JST = {9, 14, 17}

STATUS_JA_LABELS = {
    "success": "成功",
    "no_result": "結果なし",
    "error": "エラー",
}


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


def send_slack_message(text: str, *, mrkdwn: bool = True) -> bool:
    """Send a message to Slack via webhook (mrkdwn enabled by default)."""
    payload: dict[str, Any] = {"text": text}
    if mrkdwn:
        payload["mrkdwn"] = True
    return _post_slack(payload)


def _format_status_counts_line(status_counts: dict[str, int]) -> str | None:
    parts = []
    for key in ("success", "no_result", "error"):
        count = status_counts.get(key, 0)
        if count > 0:
            parts.append(f"{STATUS_JA_LABELS[key]}: {count}")
    if not parts:
        return None
    return "ステータス: " + " | ".join(parts)


def _format_live_progress_message(
    *,
    run_key: str,
    total: int,
    processed: int,
    status_counts: dict[str, int],
    notice_label: str = "",
) -> str:
    pct = round((processed / total) * 100, 1) if total else 0
    time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    progress_line = f"*Progress: {pct}%*"
    if notice_label:
        progress_line = f"{progress_line} {notice_label}"

    lines = [
        "acquisition_progress",
        f"run_key: `{run_key}`",
        f"time: {time_str}",
        progress_line,
        f"Processed {processed}/{total} stores",
        "",
    ]
    if status_counts:
        status_line = _format_status_counts_line(status_counts)
        if status_line:
            lines.append(status_line)

    return "\n".join(lines)


def send_live_progress_notice(
    *,
    run_key: str,
    total: int,
    processed: int,
    status_counts: dict[str, int],
    notice_label: str = "",
) -> bool:
    """Live acquisition progress (shared by interval and scheduled JST updates)."""
    return send_slack_message(
        _format_live_progress_message(
            run_key=run_key,
            total=total,
            processed=processed,
            status_counts=status_counts,
            notice_label=notice_label,
        )
    )


def send_progress_notice(
    *,
    run_key: str,
    total: int,
    processed: int,
    status_counts: dict[str, int],
) -> bool:
    """Scheduled progress report at 9am, 2pm, or 5pm JST (live run only)."""
    now_jst = datetime.now(JST)
    label = f"(scheduled update: {now_jst.strftime('%H:%M')} JST)"
    return send_live_progress_notice(
        run_key=run_key,
        total=total,
        processed=processed,
        status_counts=status_counts,
        notice_label=label,
    )


def send_interval_progress(
    *,
    run_key: str,
    total: int,
    processed: int,
    status_counts: dict[str, int],
) -> bool:
    """Periodic progress report (sent every 15 s during a live acquisition)."""
    return send_live_progress_notice(
        run_key=run_key,
        total=total,
        processed=processed,
        status_counts=status_counts,
        notice_label="(auto-update every 15 s)",
    )


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
    overall_status = "完了" if error == 0 else "完了（エラーあり）"
    time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f":white_check_mark: *acquisition_complete* ({overall_status})",
        f"time: {time_str}",
        f"run_key: `{run_key}`",
        "",
        (
            f"結果: {total_records} 件 | "
            f"{STATUS_JA_LABELS['success']}: {success} | "
            f"{STATUS_JA_LABELS['no_result']}: {no_result} | "
            f"{STATUS_JA_LABELS['error']}: {error}"
        ),
        f"成功率: {success_rate}%",
        f"APIリクエスト数: {api_requests}",
    ]
    link = (drive_url or "").strip()
    if link:
        label = output_csv or "Google Drive CSV"
        lines.append(f"Google Drive: <{link}|{label}>")
        lines.append(link)
    elif output_csv:
        lines.append(f"出力ファイル: `{output_csv}`（Driveアップロードなし）")
    return send_slack_message("\n".join(lines))
