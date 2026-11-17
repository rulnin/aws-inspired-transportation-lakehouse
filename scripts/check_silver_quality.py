"""Reconcile Bronze source rows with Silver and quarantine in Trino."""

from __future__ import annotations

import os

from trino_silver_smoke import _query

from lakehouse.common.logging import configure_logging
from lakehouse.config.settings import load_settings
from lakehouse.ingestion.verification import assert_silver_reconciles, verify_months


def main() -> None:
    configure_logging(os.environ.get("LAKEHOUSE_LOG_LEVEL", "INFO").upper())
    months = load_settings().source_months
    source_rows = verify_months()
    month_list = ", ".join(f"'{month}'" for month in months)
    endpoint = os.environ.get("TRINO_ENDPOINT_URL", "http://127.0.0.1:8080")
    counts_query = f"""
        SELECT source_month, layer, count(*)
        FROM (
            SELECT source_month, 'silver' AS layer
            FROM iceberg.silver.yellow_taxi_trips
            WHERE source_month IN ({month_list})
            UNION ALL
            SELECT source_month, 'quarantine' AS layer
            FROM iceberg.silver.yellow_taxi_quarantine
            WHERE source_month IN ({month_list})
        ) classified
        GROUP BY source_month, layer
    """
    counts = {(row[0], row[1]): int(row[2]) for row in _query(endpoint, counts_query)}
    for month in months:
        valid_rows = counts.get((month, "silver"), 0)
        rejected_rows = counts.get((month, "quarantine"), 0)
        assert_silver_reconciles(month, source_rows[month], valid_rows, rejected_rows)

    lineage_query = f"""
        SELECT count_if(
            source_url IS NULL OR ingested_at IS NULL OR pipeline_run_id IS NULL
        )
        FROM iceberg.silver.yellow_taxi_trips
        WHERE source_month IN ({month_list})
    """
    if int(_query(endpoint, lineage_query)[0][0]):
        raise RuntimeError("Silver rows are missing Bronze lineage")

    reasons_query = f"""
        SELECT count_if(rejection_reasons IS NULL OR cardinality(rejection_reasons) = 0)
        FROM iceberg.silver.yellow_taxi_quarantine
        WHERE source_month IN ({month_list})
    """
    if int(_query(endpoint, reasons_query)[0][0]):
        raise RuntimeError("Quarantine rows are missing rejection reasons")

    for month in months:
        print(
            f"{month}: bronze={source_rows[month]}, "
            f"silver={counts.get((month, 'silver'), 0)}, "
            f"quarantine={counts.get((month, 'quarantine'), 0)}"
        )


if __name__ == "__main__":
    main()
