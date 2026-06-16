import os
from dotenv import load_dotenv

load_dotenv()


def _env_strip(name: str, default: str = "") -> str:
    raw = os.getenv(name, default)
    if raw is None:
        return ""
    v = str(raw).strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        return v[1:-1].strip()
    return v


def _parse_provider_names(job_providers: str, job_provider: str) -> list[str]:
    if job_providers.strip():
        names = [p.strip().lower() for p in job_providers.split(",") if p.strip()]
        if names:
            return names
    single = job_provider.strip().lower()
    return [single] if single else ["serpapi"]


class Settings:
    JOB_PROVIDER: str = os.getenv("JOB_PROVIDER", "serpapi")
    JOB_PROVIDERS: str = os.getenv("JOB_PROVIDERS", "")
    JOB_PROVIDER_MIN_INTERVAL_SECONDS: float = float(os.getenv("JOB_PROVIDER_MIN_INTERVAL_SECONDS", "0"))

    SERPAPI_KEY: str | None = os.getenv("SERPAPI_KEY")
    SERPAPI_URL: str = os.getenv("SERPAPI_URL", "https://serpapi.com/search.json")

    OXYLABS_USERNAME: str | None = (
        os.getenv("OXYLABS_USERNAME")
        or os.getenv("OXYLABS_USER")
        or os.getenv("OXYLABS_API_USER")
    )
    OXYLABS_PASSWORD: str | None = (
        os.getenv("OXYLABS_PASSWORD")
        or os.getenv("OXYLABS_PASS")
        or os.getenv("OXYLABS_API_PASSWORD")
    )
    # Comma-separated user:password pairs for multi-key concurrency (see parallel_api_pool).
    OXYLABS_CREDENTIALS: str = os.getenv("OXYLABS_CREDENTIALS", "")
    OXYLABS_URL: str = os.getenv("OXYLABS_URL", "https://realtime.oxylabs.io/v1/queries")
    OXYLABS_ENRICH_MAX_WORKERS: int = max(
        1,
        int(os.getenv("OXYLABS_ENRICH_MAX_WORKERS", "3")),
    )
    # Concurrent in-flight Oxylabs HTTP calls allowed per API key (round-robin across keys).
    OXYLABS_MAX_CONCURRENT_PER_KEY: int = max(
        1,
        int(os.getenv("OXYLABS_MAX_CONCURRENT_PER_KEY", "10")),
    )
    API_POOL_MAX_WORKERS: int = int(
        os.getenv("API_POOL_MAX_WORKERS", os.getenv("OXYLABS_MAX_WORKERS", "10"))
    )
    API_POOL_MIN_INTERVAL_SECONDS_PER_KEY: float = float(
        os.getenv("API_POOL_MIN_INTERVAL_SECONDS_PER_KEY", "0")
    )
    API_POOL_DISPATCH_STRATEGY: str = os.getenv("API_POOL_DISPATCH_STRATEGY", "round_robin")
    DEFAULT_QUERY: str = os.getenv("DEFAULT_QUERY", "software engineer")
    DEFAULT_LOCATION: str = os.getenv("DEFAULT_LOCATION", "Kathmandu")
    DEFAULT_DOMAIN: str = os.getenv("DEFAULT_DOMAIN", "co.jp")
    DEFAULT_LANGUAGE: str = os.getenv("DEFAULT_LANGUAGE", "ja")
    DEFAULT_COUNTRY: str = os.getenv("DEFAULT_COUNTRY", "jp")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    # Records processed per batch slice (parallel within batch). Falls back to BATCH_SIZE when unset.
    ACQUISITION_BATCH_SIZE: int = max(
        1,
        int((os.getenv("ACQUISITION_BATCH_SIZE") or os.getenv("BATCH_SIZE") or "50").strip() or "50"),
    )
    # Sleep between batches (seconds) to reduce API rate pressure.
    BATCH_DELAY_SECONDS: float = float(os.getenv("BATCH_DELAY_SECONDS", "0"))
    # Extra passes over indices that ended with status=error within a batch (0 = disabled).
    BATCH_ERROR_RETRY_PASSES: int = max(0, int(os.getenv("BATCH_ERROR_RETRY_PASSES", "0")))
    # Post-export retry pass: re-fetch records with エラー status before Drive upload.
    ACQUISITION_ERROR_RETRY_ENABLED: bool = os.getenv(
        "ACQUISITION_ERROR_RETRY_ENABLED", "true"
    ).strip().lower() not in ("0", "false", "no", "off")
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "10"))
    # ThreadPool workers per acquisition batch (may exceed key count; pool shares keys with concurrency slots).
    OXYLABS_MAX_WORKERS: int = int(os.getenv("OXYLABS_MAX_WORKERS", "10"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    MAX_QUERY_ATTEMPTS: int = int(os.getenv("MAX_QUERY_ATTEMPTS", "5"))
    QUERY_CHAR_LIMIT: int = int(os.getenv("QUERY_CHAR_LIMIT", "150"))
    QUERY_SUFFIXES: str = os.getenv("QUERY_SUFFIXES", "求人,アルバイト,jobs,hiring")
    INPUT_PATH: str = os.getenv("INPUT_PATH", "data/input/stores.csv")
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "data/output")
    LOG_DIR: str = os.getenv("LOG_DIR", "data/logs")

    # Share a Drive folder (Editor) with the service account email from the JSON so uploads appear in your Drive.
    GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON: str = os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON", "")
    GOOGLE_DRIVE_FOLDER_ID: str = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
    # When true, skip Drive upload and email a placeholder URL (GOOGLE_DRIVE_TEMP_URL or auto-generated).
    GOOGLE_DRIVE_USE_TEMP_URL: bool = os.getenv("GOOGLE_DRIVE_USE_TEMP_URL", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    GOOGLE_DRIVE_TEMP_URL: str = os.getenv("GOOGLE_DRIVE_TEMP_URL", "")

    # SMTP: used to email the Google Drive link after CSV upload (Brevo/Sendinblue, Gmail, etc.).
    SMTP_HOST: str = os.getenv("SMTP_HOST", "")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME: str = os.getenv("SMTP_USERNAME") or os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str | None = os.getenv("SMTP_PASSWORD")
    SMTP_SECURITY: str = _env_strip("SMTP_SECURITY", "TLS").upper()
    SMTP_USE_TLS: bool = os.getenv("SMTP_USE_TLS", "true").strip().lower() not in ("0", "false", "no", "off")
    EMAIL_FROM: str = os.getenv("EMAIL_FROM", "")
    EMAIL_RECIPIENTS: str = os.getenv("EMAIL_RECIPIENTS", "")
    EMAIL_DRIVE_SUBJECT_PREFIX: str = os.getenv("EMAIL_DRIVE_SUBJECT_PREFIX", "Google Drive - acquisition CSV")
    # Sent by email only after a successful Drive upload (ignored when GOOGLE_DRIVE_USE_TEMP_URL=true).
    EMAIL_CSV_LINK_URL: str = _env_strip("EMAIL_CSV_LINK_URL")
    SLACK_WEBHOOK_URL: str = _env_strip("SLACK_WEBHOOK_URL")

    @property
    def smtp_security_mode(self) -> str:
        """TLS (STARTTLS), SSL (implicit), or NONE."""
        mode = (self.SMTP_SECURITY or "").strip().upper()
        if mode in ("TLS", "SSL", "NONE"):
            return mode
        return "TLS" if self.SMTP_USE_TLS else "NONE"

    @property
    def job_provider_names(self) -> list[str]:
        return _parse_provider_names(self.JOB_PROVIDERS, self.JOB_PROVIDER)


settings = Settings()