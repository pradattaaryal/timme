from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.models.error_retry import (
    ERROR_RETRY_INPUT_COLUMNS,
    ERROR_STATUS_JA,
    CsvErrorExtractionResult,
    ErrorCsvRow,
)

logger = logging.getLogger(__name__)

STATUS_COLUMN = "status"
CUSTOMER_ID_COLUMN = "取引先ID"


class CsvErrorExtractionService:
    """Reads a partial or final google_jobs CSV and extracts rows with error status."""

    def load_dataframe(self, csv_path: str | Path) -> pd.DataFrame:
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV not found for error extraction: {path}")
        return pd.read_csv(path, encoding="utf-8-sig", dtype=str)

    def extract_from_path(self, csv_path: str | Path) -> CsvErrorExtractionResult:
        path = Path(csv_path)
        df = self.load_dataframe(path)
        return self.extract_from_dataframe(df, source_path=str(path))

    def extract_from_dataframe(
        self, df: pd.DataFrame, *, source_path: str = ""
    ) -> CsvErrorExtractionResult:
        total_rows = len(df)
        if total_rows == 0:
            logger.info("csv_error_extraction path=%s total_rows=0 error_rows=0", source_path)
            return CsvErrorExtractionResult(source_path=source_path, total_rows=0)

        if STATUS_COLUMN not in df.columns:
            logger.warning(
                "csv_error_extraction path=%s missing status column; no error rows extracted",
                source_path,
            )
            return CsvErrorExtractionResult(source_path=source_path, total_rows=total_rows)

        missing_cols = [c for c in ERROR_RETRY_INPUT_COLUMNS if c not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Output CSV is missing required columns for error retry: {missing_cols}"
            )

        error_mask = df[STATUS_COLUMN].fillna("").astype(str).str.strip() == ERROR_STATUS_JA
        error_df = df.loc[error_mask, list(ERROR_RETRY_INPUT_COLUMNS)]

        error_rows: list[ErrorCsvRow] = []
        for _, row in error_df.iterrows():
            error_rows.append(
                ErrorCsvRow(
                    company_name=str(row.get("企業名") or "").strip(),
                    store_name=str(row.get("事業所名") or "").strip(),
                    city_ward_name=str(row.get("市区郡") or "").strip(),
                    customer_id=str(row.get(CUSTOMER_ID_COLUMN) or "").strip(),
                )
            )

        logger.info(
            "csv_error_extraction path=%s total_rows=%s error_rows=%s",
            source_path,
            total_rows,
            len(error_rows),
        )
        return CsvErrorExtractionResult(
            source_path=source_path,
            total_rows=total_rows,
            error_rows=error_rows,
        )
