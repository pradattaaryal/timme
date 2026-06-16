from __future__ import annotations

import logging

from src.models.error_retry import ErrorCsvRow, SanitizationResult, SanitizedErrorRecord

logger = logging.getLogger(__name__)


class ErrorRecordSanitizationService:
    """Trims, deduplicates, and filters error records before retry acquisition."""

    def sanitize(self, error_rows: list[ErrorCsvRow]) -> SanitizationResult:
        input_count = len(error_rows)
        skipped_empty_id = 0
        duplicates_removed = 0
        seen_ids: set[str] = set()
        records: list[SanitizedErrorRecord] = []

        for row in error_rows:
            company = row.company_name.strip()
            store = row.store_name.strip()
            city = row.city_ward_name.strip()
            customer_id = row.customer_id.strip()

            if not customer_id:
                skipped_empty_id += 1
                continue

            if customer_id in seen_ids:
                duplicates_removed += 1
                continue

            seen_ids.add(customer_id)
            records.append(
                SanitizedErrorRecord(
                    company_name=company,
                    store_name=store,
                    city_ward_name=city,
                    customer_id=customer_id,
                )
            )

        logger.info(
            "error_record_sanitization input=%s output=%s skipped_empty_id=%s duplicates_removed=%s",
            input_count,
            len(records),
            skipped_empty_id,
            duplicates_removed,
        )
        return SanitizationResult(
            input_count=input_count,
            records=records,
            skipped_empty_id=skipped_empty_id,
            duplicates_removed=duplicates_removed,
        )
