from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


@dataclass
class CheckpointState:
    run_key: str
    source_path: str
    pages: int
    limit_per_query: int
    fetched_at: str
    processed_indices: set[int]
    rows: list[dict[str, Any]]
    processing_logs: list[dict[str, Any]]
    errors: list[dict[str, Any]]
    requests_made: int


def checkpoint_path(log_dir: Path, run_key: str) -> Path:
    checkpoint_dir = log_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    return checkpoint_dir / f"acquisition_{run_key}.json"


def dead_queue_path(log_dir: Path, run_key: str) -> Path:
    dead_dir = log_dir / "dead_queue"
    dead_dir.mkdir(parents=True, exist_ok=True)
    return dead_dir / f"dead_queue_{run_key}.jsonl"


def load_checkpoint(path: Path) -> CheckpointState | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    return CheckpointState(
        run_key=str(data.get("run_key") or ""),
        source_path=str(data.get("source_path") or ""),
        pages=int(data.get("pages") or 1),
        limit_per_query=int(data.get("limit_per_query") or 20),
        fetched_at=str(data.get("fetched_at") or datetime.now(timezone.utc).isoformat()),
        processed_indices={int(v) for v in (data.get("processed_indices") or [])},
        rows=list(data.get("rows") or []),
        processing_logs=list(data.get("processing_logs") or []),
        errors=list(data.get("errors") or []),
        requests_made=int(data.get("requests_made") or 0),
    )


def save_checkpoint(path: Path, state: CheckpointState) -> None:
    payload = {
        "run_key": state.run_key,
        "source_path": state.source_path,
        "pages": state.pages,
        "limit_per_query": state.limit_per_query,
        "fetched_at": state.fetched_at,
        "processed_indices": sorted(state.processed_indices),
        "rows": state.rows,
        "processing_logs": state.processing_logs,
        "errors": state.errors,
        "requests_made": state.requests_made,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def append_dead_queue(dead_queue_file: Path, index: int, record: dict[str, Any], error: str) -> None:
    payload = {"index": index, "record": record, "error": error}
    with dead_queue_file.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_dead_queue(dead_queue_file: Path) -> dict[int, dict[str, Any]]:
    if not dead_queue_file.exists():
        return {}
    entries: dict[int, dict[str, Any]] = {}
    with dead_queue_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            idx = parsed.get("index")
            rec = parsed.get("record")
            if isinstance(idx, int) and isinstance(rec, dict):
                entries[idx] = rec
    return entries


def rewrite_dead_queue(dead_queue_file: Path, entries: dict[int, dict[str, Any]]) -> None:
    with dead_queue_file.open("w", encoding="utf-8") as fp:
        for idx, record in sorted(entries.items(), key=lambda item: item[0]):
            fp.write(json.dumps({"index": idx, "record": record}, ensure_ascii=False) + "\n")

