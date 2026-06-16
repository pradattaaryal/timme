from __future__ import annotations

import logging
import smtplib
import ssl
from email.message import EmailMessage

from src.config.settings import settings

logger = logging.getLogger(__name__)


def _smtp_password_normalized(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.replace(" ", "")


def _parse_recipients(raw: str) -> list[str]:
    return [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]


def _use_ssl(security_mode: str, port: int) -> bool:
    return security_mode == "SSL" or (security_mode != "NONE" and port == 465)


def _use_starttls(security_mode: str, port: int) -> bool:
    if security_mode == "TLS":
        return port != 465
    if security_mode == "NONE":
        return False
    return settings.SMTP_USE_TLS and port != 465


def send_drive_csv_link_email(
    *,
    csv_filename: str,
    drive_url: str,
    is_temporary_placeholder: bool = False,
) -> bool:
    """
    Send the CSV link using SMTP_* / EMAIL_* from .env.
    Returns True if the message was handed off to SMTP successfully.
    """
    host = (settings.SMTP_HOST or "").strip()
    recipients = _parse_recipients(settings.EMAIL_RECIPIENTS or "")
    if not host or not recipients:
        logger.warning(
            "Drive CSV email skipped: set SMTP_HOST and EMAIL_RECIPIENTS in .env (host=%r, recipients=%r).",
            host or "(empty)",
            settings.EMAIL_RECIPIENTS or "(empty)",
        )
        return False

    username = (settings.SMTP_USERNAME or "").strip()
    password = _smtp_password_normalized(settings.SMTP_PASSWORD)
    if username and not password:
        logger.warning(
            "Drive CSV email skipped: SMTP_USERNAME is set but SMTP_PASSWORD is empty."
        )
        return False

    use_auth = bool(username and password)
    mail_from = (settings.EMAIL_FROM or "").strip() or username or "acquisition@localhost"
    subject = f"{settings.EMAIL_DRIVE_SUBJECT_PREFIX}: {csv_filename}"
    body = (
        "The acquisition job finished writing the results CSV.\n\n"
        f"File: {csv_filename}\n"
        f"Link: {drive_url}\n"
    )
    if is_temporary_placeholder:
        body += (
            "\nNote: This is a temporary placeholder URL (not Google Drive). "
            "Set GOOGLE_DRIVE_USE_TEMP_URL=false when you want real uploads.\n"
        )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    port = settings.SMTP_PORT
    security_mode = settings.smtp_security_mode
    use_ssl = _use_ssl(security_mode, port)
    use_starttls = _use_starttls(security_mode, port)
    ctx = ssl.create_default_context()

    logger.info(
        "Drive CSV email: sending via %s:%s to %s (security=%s, auth=%s)",
        host,
        port,
        ", ".join(recipients),
        "SSL" if use_ssl else ("STARTTLS" if use_starttls else "none"),
        "yes" if use_auth else "no",
    )

    try:
        if use_ssl:
            with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as smtp:
                if use_auth:
                    smtp.login(username, password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=30) as smtp:
                smtp.ehlo()
                if use_starttls:
                    smtp.starttls(context=ctx)
                    smtp.ehlo()
                if use_auth:
                    smtp.login(username, password)
                smtp.send_message(msg)
    except smtplib.SMTPAuthenticationError:
        logger.exception(
            "Drive CSV email failed: SMTP authentication rejected for user %r on %s:%s.",
            username,
            host,
            port,
        )
        return False
    except smtplib.SMTPConnectError:
        logger.exception(
            "Drive CSV email failed: could not connect to SMTP server %s:%s.",
            host,
            port,
        )
        return False
    except smtplib.SMTPException:
        logger.exception(
            "Drive CSV email failed: SMTP error while sending to %s via %s:%s.",
            ", ".join(recipients),
            host,
            port,
        )
        return False
    except OSError:
        logger.exception(
            "Drive CSV email failed: network error reaching SMTP server %s:%s.",
            host,
            port,
        )
        return False

    logger.info("Drive CSV link emailed to %s", ", ".join(recipients))
    return True
