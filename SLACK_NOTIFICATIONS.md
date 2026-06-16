# Slack Notification Feature

## Overview

Added Slack webhook notifications to the acquisition pipeline for real-time team awareness. Notifications are sent at three points:

- **Error alerts** — when any store fails during a batch
- **Progress reports** — at 9am, 2pm, and 5pm JST (only when a run is active)
- **Completion summary** — after the pipeline finishes

## What Changed

### Files Added

| File | Purpose |
|------|---------|
| `src/services/slack_notification.py` | Slack webhook client with typed message builders |

### Files Modified

| File | Change |
|------|--------|
| `.env` | Added `SLACK_WEBHOOK_URLl=` placeholder |
| `src/config/settings.py` | Added `SLACK_WEBHOOK_URLl` setting |
| `src/services/acquisition_service.py` | Added 3 notification hooks into the batch loop and pipeline completion |

## Architecture

```
.env: SLACK_WEBHOOK_URLl
  └── settings.py: SLACK_WEBHOOK_URLl
        └── slack_notification.py
              ├── send_error_notice()       — called on batch errors
              ├── send_progress_notice()    — called at 9/14/17 JST
              └── send_completion_notice()  — called on pipeline finish
                    └── HTTP POST → Slack Incoming Webhook → #channel
```

All notifications are fire-and-forget. If the webhook call fails, a warning is logged and the pipeline continues without interruption.

## Setup

### 1. Create a Slack Webhook URL

1. Go to your Slack workspace
2. Navigate to **Settings → Manage apps → Incoming Webhooks**
3. Click **Activate Incoming Webhooks** (if not already on)
4. Click **Add New Webhook to Workspace**
5. Select the channel you want notifications in
6. Copy the generated webhook URL

### 2. Add to `.env`

```bash
# =========================
# Slack Notifications
# =========================
 ```

If left empty or unset, all Slack notifications are silently skipped.

## Notification Details

### Error Alert

Sent immediately when a batch has stores with `status=error`.

```
:rotating_light: acquisition_error
run_key: `abc123`
batch_id: `abc123-batch-0001`

Error: 2 store(s) failed in batch 1/3
```

Includes up to 5 sample failed query strings to help diagnose the issue.

### Progress Report

Sent after each batch completes, but only if the current hour matches 9am, 2pm, or 5pm (JST). Each hour slot per run is sent at most once to avoid duplicate messages.

```
acquisition_progress
run_key: `abc123`
time: 2026-06-16 14:03 JST
batch: 3/5

*Progress: 60.0%*
Processed 300/500 stores

Status: success: 200 | no_result: 80 | error: 20
```

### Completion Summary

Sent after the full pipeline finishes (after CSV export, Drive upload, and email notification).

```
:white_check_mark: acquisition_complete (completed with errors)
time: 2026-06-16 14:10 JST
run_key: `abc123`

Results: 500 total | 400 success | 50 no_result | 50 error
Success rate: 80.0%
API requests: 2500
Output: `google_jobs_20260616_141000.csv`
```

## Implementation Details

### `slack_notification.py` API

| Function | Parameters | Returns |
|----------|-----------|---------|
| `send_slack_message(text)` | Plain text | `bool` |
| `send_progress_notice(run_key, total, processed, status_counts)` | Progress data | `bool` |
| `send_error_notice(run_key, batch_id, failed_stores, error_msg)` | Error data | `bool` |
| `send_completion_notice(run_key, total_records, success, no_result, error, success_rate, api_requests, output_csv)` | Summary data | `bool` |

### `acquisition_service.py` Hooks

**Hook 1 — Error (after each batch):**

Located at line ~810, inside the batch processing loop. Triggered when `err_n > 0` (any store in the batch ended with `status=error`).

```python
if err_n > 0 and failed_queries:
    send_error_notice(
        run_key=run_key,
        batch_id=batch_id,
        failed_stores=failed_queries[:5],
        error_msg=f"{err_n} store(s) failed in batch {batch_num}/{total_batches}",
    )
```

**Hook 2 — Progress (after each batch):**

Located at line ~822, inside the batch processing loop. Checks the current JST hour — sends only at 9, 14, or 17. Tracks which hour-slots have been sent per run to prevent duplicates.

```python
_send_batch_progress(
    run_key=run_key, total=len(validation.valid_records),
    processed=len(state.processed_indices),
    ok_n=ok_n, no_n=no_n, err_n=err_n,
    batch_num=batch_num, total_batches=total_batches,
)
```

**Hook 3 — Completion (after pipeline finishes):**

Located at line ~1056, after all CSV export, Drive upload, and email notification is done.

```python
send_completion_notice(
    run_key=run_key,
    total_records=len(validation.valid_records),
    success=store_success_count,
    no_result=store_no_result_count,
    error=store_error_count,
    success_rate=success_rate,
    api_requests=requests_made,
    output_csv=str(output_csv_path),
)
```

### Progress Deduplication

A module-level set `_progress_hours_fired` tracks which `run_key-hour` combinations have already triggered a notification. This ensures that if a run spans multiple hours (e.g. from 2pm to 5pm), only one progress message is sent per hour slot.

```python
_progress_hours_fired: set[str] = set()
# ...
hour_key = f"{run_key}-{current_hour}"
if hour_key in _progress_hours_fired:
    return  # skip duplicate
```

## Error Handling

All Slack calls are wrapped in `try/except` blocks that catch any exception and log it with `logger.exception()`. The pipeline never halts or returns an error due to Slack failures.

```python
try:
    send_completion_notice(...)
except Exception:
    logger.exception("Slack completion notification failed (ignored)")
```

## No New Dependencies

The Slack service uses the existing `requests` library (already in `requirements.txt`), so no new pip packages are needed.
