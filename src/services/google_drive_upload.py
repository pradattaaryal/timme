from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_service_account_json_path(raw: str) -> Path:
    trimmed = raw.strip()
    if not trimmed:
        return Path("")
    p = Path(trimmed)
    if p.is_file():
        return p.resolve()
    alt = (_project_root() / trimmed).resolve()
    if alt.is_file():
        return alt
    return p


@dataclass(frozen=True)
class DriveUploadResult:
    """Result of a successful Google Drive file create."""

    file_id: str
    web_view_link: str

    @property
    def url(self) -> str:
        """Browser URL for the uploaded file (API link or standard file URL)."""
        if self.web_view_link.strip():
            return self.web_view_link.strip()
        return f"https://drive.google.com/file/d/{self.file_id}/view"


def _read_service_account_email(sa_path: Path) -> str:
    try:
        data = json.loads(sa_path.read_text(encoding="utf-8"))
        return str(data.get("client_email") or "<missing client_email in JSON>")
    except (OSError, json.JSONDecodeError):
        return "<could not read client_email>"


def _http_error_detail(exc: Any) -> str:
    try:
        from googleapiclient.errors import HttpError

        if isinstance(exc, HttpError):
            content = getattr(exc, "content", None) or b""
            if isinstance(content, bytes):
                return content.decode("utf-8", errors="replace")[:2000]
            return str(content)[:2000]
    except ImportError:
        pass
    return str(exc)


def upload_local_file_to_drive(
    file_path: Path,
    *,
    service_account_json: str,
    parent_folder_id: str = "",
) -> DriveUploadResult | None:
    """
    Upload a local file to Google Drive using a service account (Drive API v3).
    Returns ``DriveUploadResult`` with ``file_id`` and ``url``, or None if upload was skipped or failed.

    Setup checklist:
      1. Set GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON in .env (path to JSON; relative paths resolve from cwd or repo root)
      2. Set GOOGLE_DRIVE_FOLDER_ID to a folder shared with the service account as Editor
      3. Ensure google-api-python-client and google-auth are installed
    """
    sa_json_str = (service_account_json or "").strip()
    if not sa_json_str:
        logger.warning(
            "Google Drive upload skipped: GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON is not set in .env"
        )
        return None

    sa_path = _resolve_service_account_json_path(sa_json_str)
    if not sa_path.is_file():
        logger.error(
            "Google Drive upload skipped: service account JSON not found (tried %s and repo-relative).",
            Path(sa_json_str),
        )
        return None

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaFileUpload
    except ImportError as exc:
        logger.error(
            "Google Drive upload skipped: required library not installed (%s). "
            "Add google-api-python-client and google-auth to requirements.txt.",
            exc,
        )
        return None

    sa_email = _read_service_account_email(sa_path)
    folder = parent_folder_id.strip()

    if not folder:
        logger.warning(
            "Google Drive: GOOGLE_DRIVE_FOLDER_ID is empty. "
            "The file may upload only to the service account drive (not visible under your personal My Drive). "
            "Share a folder with %s as Editor and set GOOGLE_DRIVE_FOLDER_ID.",
            sa_email,
        )

    file_path = Path(file_path)
    if not file_path.is_file():
        logger.error(
            "Google Drive upload skipped: output file not found at '%s'.",
            file_path,
        )
        return None

    try:
        creds = service_account.Credentials.from_service_account_file(
            str(sa_path),
            scopes=[DRIVE_SCOPE],
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        logger.info(
            "Google Drive: uploading '%s' as service account %s to folder '%s'",
            file_path.name,
            sa_email,
            folder or "(service account root — set GOOGLE_DRIVE_FOLDER_ID)",
        )

        body: dict[str, Any] = {"name": file_path.name}
        if folder:
            body["parents"] = [folder]

        media = MediaFileUpload(str(file_path), mimetype="text/csv", resumable=True)
        created = (
            service.files()
            .create(
                body=body,
                media_body=media,
                fields="id, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        file_id = created.get("id")
        link = (created.get("webViewLink") or "").strip()
        if not file_id:
            logger.error("Google Drive API returned no file id for '%s'.", file_path.name)
            return None

        result = DriveUploadResult(file_id=file_id, web_view_link=link)
        logger.info(
            "Google Drive upload succeeded: name=%s id=%s url=%s",
            file_path.name,
            result.file_id,
            result.url,
        )
        return result

    except HttpError as exc:
        detail = _http_error_detail(exc)
        status = getattr(getattr(exc, "resp", None), "status", "?")
        logger.error(
            "Google Drive API HTTP %s while uploading '%s'.\n"
            "  Service account : %s\n"
            "  Folder ID       : %s\n"
            "  API response: %s",
            status,
            file_path.name,
            sa_email,
            folder or "(empty)",
            detail,
        )
        return None

    except Exception:  # noqa: BLE001
        logger.exception(
            "Google Drive upload failed (non-HTTP) for '%s' using service account '%s'.",
            file_path,
            sa_email,
        )
        return None
