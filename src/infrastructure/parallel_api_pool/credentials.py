from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator

from src.infrastructure.parallel_api_pool.pool import ApiKeyPool, PoolOptions

if TYPE_CHECKING:
    from src.config.settings import Settings

logger = logging.getLogger(__name__)

_active_credential: ContextVar["ApiCredential | None"] = ContextVar(
    "oxylabs_active_credential",
    default=None,
)


@dataclass(frozen=True, slots=True)
class ApiCredential:
    key_id: str
    key_index: int
    username: str
    password: str

    @property
    def pool_token(self) -> str:
        return f"{self.username}:{self.password}"


def set_active_credential(credential: ApiCredential) -> Token["ApiCredential | None"]:
    return _active_credential.set(credential)


def reset_active_credential(token: Token["ApiCredential | None"]) -> None:
    _active_credential.reset(token)


def get_active_credential() -> ApiCredential | None:
    return _active_credential.get()


def parse_oxylabs_credentials(settings: Settings) -> list[ApiCredential]:
    raw = (getattr(settings, "OXYLABS_CREDENTIALS", None) or "").strip()
    pairs: list[tuple[str, str]] = []

    if raw:
        for chunk in raw.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            if ":" not in chunk:
                logger.warning(
                    "Skipping invalid OXYLABS_CREDENTIALS entry (missing ':'): %s",
                    chunk[:20],
                )
                continue
            user, _, passwd = chunk.partition(":")
            user = user.strip()
            passwd = passwd.strip()
            if not user or not passwd:
                continue
            pairs.append((user, passwd))

    if not pairs:
        user = settings.OXYLABS_USERNAME
        password = settings.OXYLABS_PASSWORD
        if user and password:
            pairs.append((user.strip(), password.strip()))

    return [
        ApiCredential(
            key_id=f"key-{i}",
            key_index=i,
            username=u,
            password=p,
        )
        for i, (u, p) in enumerate(pairs)
    ]


class RoundRobinCredentialPool:
    """
    Thread-safe credential pool with round-robin key selection.

    Each API key may serve multiple concurrent requests (bounded by max_concurrent_per_key).
    """

    def __init__(
        self,
        credentials: list[ApiCredential],
        *,
        max_concurrent_per_key: int = 1,
    ) -> None:
        if not credentials:
            raise ValueError("RoundRobinCredentialPool requires at least one credential")
        if max_concurrent_per_key < 1:
            raise ValueError("max_concurrent_per_key must be at least 1")

        self._credentials = list(credentials)
        self._max_concurrent_per_key = max_concurrent_per_key
        self._slots: list[threading.Semaphore] = [
            threading.Semaphore(max_concurrent_per_key) for _ in credentials
        ]
        self._rr_lock = threading.Lock()
        self._next_index = 0

    @property
    def credential_count(self) -> int:
        return len(self._credentials)

    @property
    def max_concurrent_per_key(self) -> int:
        return self._max_concurrent_per_key

    @property
    def max_concurrent_requests(self) -> int:
        return len(self._credentials) * self._max_concurrent_per_key

    def _advance_round_robin(self, idx: int) -> None:
        with self._rr_lock:
            self._next_index = (idx + 1) % len(self._credentials)

    def _round_robin_start(self) -> int:
        with self._rr_lock:
            return self._next_index

    def _acquire(self, timeout: float | None = None) -> ApiCredential:
        deadline = None if timeout is None else (time.monotonic() + timeout)
        key_count = len(self._credentials)

        while True:
            start = self._round_robin_start()

            for offset in range(key_count):
                idx = (start + offset) % key_count
                if self._slots[idx].acquire(blocking=False):
                    self._advance_round_robin(idx)
                    return self._credentials[idx]

            wait_idx = start
            if timeout is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for an available Oxylabs credential slot")
                acquired = self._slots[wait_idx].acquire(timeout=remaining)
            else:
                acquired = self._slots[wait_idx].acquire(blocking=True)

            if acquired:
                self._advance_round_robin(wait_idx)
                return self._credentials[wait_idx]

            if timeout is not None and time.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for an available Oxylabs credential slot")

    def _release(self, credential: ApiCredential) -> None:
        self._slots[credential.key_index].release()

    @contextmanager
    def borrow(self, *, timeout: float | None = None) -> Iterator[ApiCredential]:
        credential = self._acquire(timeout=timeout)
        token = set_active_credential(credential)
        try:
            yield credential
        finally:
            reset_active_credential(token)
            self._release(credential)


_shared_round_robin_pool: RoundRobinCredentialPool | None = None
_shared_round_robin_lock = threading.Lock()


def get_round_robin_pool(settings: Settings) -> RoundRobinCredentialPool | None:
    global _shared_round_robin_pool
    credentials = parse_oxylabs_credentials(settings)
    if not credentials:
        return None
    max_per_key = max(1, int(getattr(settings, "OXYLABS_MAX_CONCURRENT_PER_KEY", 1) or 1))
    with _shared_round_robin_lock:
        if _shared_round_robin_pool is None:
            _shared_round_robin_pool = RoundRobinCredentialPool(
                credentials,
                max_concurrent_per_key=max_per_key,
            )
            logger.info(
                "Oxylabs credential pool: keys=%s slots_per_key=%s max_concurrent=%s",
                len(credentials),
                max_per_key,
                _shared_round_robin_pool.max_concurrent_requests,
            )
        return _shared_round_robin_pool


def reset_round_robin_pool() -> None:
    """Clear cached pool (call from Celery worker_process_init after fork)."""
    global _shared_round_robin_pool
    with _shared_round_robin_lock:
        _shared_round_robin_pool = None


def build_oxylabs_pool_from_settings(settings: Settings) -> ApiKeyPool | None:
    credentials = parse_oxylabs_credentials(settings)
    if len(credentials) < 1:
        return None

    max_workers = min(
        max(1, int(getattr(settings, "API_POOL_MAX_WORKERS", settings.OXYLABS_MAX_WORKERS) or 10)),
        len(credentials),
    )
    min_interval = float(
        getattr(settings, "API_POOL_MIN_INTERVAL_SECONDS_PER_KEY", 0) or 0
    )
    strategy = getattr(settings, "API_POOL_DISPATCH_STRATEGY", "round_robin") or "round_robin"
    if strategy not in ("round_robin", "least_queued"):
        strategy = "round_robin"

    pool_keys = [c.pool_token for c in credentials[:max_workers]]
    return ApiKeyPool(
        pool_keys,
        opts=PoolOptions(
            max_workers=max_workers,
            min_interval_seconds_per_key=min_interval,
            strategy=strategy,
        ),
    )
