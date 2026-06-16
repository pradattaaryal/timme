from __future__ import annotations

import csv
import os
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any


RowTransform = Callable[[list[dict[str, Any]]], list[dict[str, Any]]]


class IncrementalPartialCsvWriter:
    """Thread-safe append-only partial CSV; one store's rows flushed after each fetch."""

    def __init__(
        self,
        path: Path,
        *,
        transform_rows: RowTransform,
    ) -> None:
        self._path = path
        self._transform_rows = transform_rows
        self._lock = threading.Lock()
        self._written_indices: set[int] = set()
        self._fieldnames: list[str] | None = None
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def mark_indices_written(self, indices: set[int]) -> None:
        with self._lock:
            self._written_indices = set(indices)

    def append_record(self, record_index: int, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        export_rows = self._transform_rows(rows)
        if not export_rows:
            return
        with self._lock:
            if record_index in self._written_indices:
                return
            self._append_rows_locked(export_rows)
            self._written_indices.add(record_index)

    def rebuild(self, rows: list[dict[str, Any]]) -> None:
        """Replace partial CSV from checkpoint state (e.g. after error retry purge)."""
        export_rows = self._transform_rows(rows)
        with self._lock:
            self._fieldnames = None
            self._write_all_locked(export_rows)

    def _append_rows_locked(self, export_rows: list[dict[str, Any]]) -> None:
        write_header = not self._path.exists() or self._path.stat().st_size == 0
        self._ensure_fieldnames(export_rows)
        with self._path.open("a", newline="", encoding="utf-8-sig") as fp:
            writer = csv.DictWriter(
                fp,
                fieldnames=self._fieldnames,
                extrasaction="ignore",
            )
            if write_header:
                writer.writeheader()
            for row in export_rows:
                writer.writerow({k: row.get(k, "") for k in self._fieldnames or []})
            self._flush(fp)

    def _write_all_locked(self, export_rows: list[dict[str, Any]]) -> None:
        self._ensure_fieldnames(export_rows)
        with self._path.open("w", newline="", encoding="utf-8-sig") as fp:
            if not export_rows or not self._fieldnames:
                self._flush(fp)
                return
            writer = csv.DictWriter(
                fp,
                fieldnames=self._fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()
            for row in export_rows:
                writer.writerow({k: row.get(k, "") for k in self._fieldnames})
            self._flush(fp)

    def _ensure_fieldnames(self, export_rows: list[dict[str, Any]]) -> None:
        if self._fieldnames or not export_rows:
            return
        self._fieldnames = list(export_rows[0].keys())

    @staticmethod
    def _flush(fp: Any) -> None:
        fp.flush()
        try:
            os.fsync(fp.fileno())
        except OSError:
            pass
