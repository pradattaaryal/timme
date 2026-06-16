from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class FetchJobParams:
    limit: int = 10
    query: str | None = None
    location: str | None = None
    retries: int = 3
    generate_variants: bool = True
    enrich: bool = False


class JobProvider(Protocol):
    def fetch_jobs(self, params: FetchJobParams) -> list[dict[str, Any]]:
         ...
