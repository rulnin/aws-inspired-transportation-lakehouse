"""Verify Trino can query the Silver and quarantine Iceberg tables."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _read_response(url: str, *, method: str, body: bytes | None = None) -> dict:
    request = Request(
        url,
        data=body,
        method=method,
        headers={"X-Trino-User": "silver-smoke", "Content-Type": "text/plain; charset=utf-8"},
    )
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def _query(endpoint: str, statement: str) -> list[list]:
    response = _read_response(
        f"{endpoint}/v1/statement", method="POST", body=statement.encode("utf-8")
    )
    rows = list(response.get("data", []))
    deadline = time.monotonic() + 180
    while response.get("nextUri"):
        if time.monotonic() >= deadline:
            raise TimeoutError("Trino Silver query did not finish within 180 seconds")
        response = _read_response(response["nextUri"], method="GET")
        if "error" in response:
            raise RuntimeError(f"Trino Silver query failed: {response['error']}")
        rows.extend(response.get("data", []))
    if "error" in response:
        raise RuntimeError(f"Trino Silver query failed: {response['error']}")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", action="append", help="YYYY-MM month to verify; repeatable")
    args = parser.parse_args()
    months = args.month or os.environ.get("LAKEHOUSE_SOURCE_MONTHS", "2024-01,2024-02").split(",")
    months = [month.strip() for month in months]
    if not months or any(not _MONTH_PATTERN.fullmatch(month) for month in months):
        raise ValueError("Months must be valid YYYY-MM values")
    endpoint = os.environ.get("TRINO_ENDPOINT_URL", "http://127.0.0.1:8080")
    month_list = ", ".join(f"'{month}'" for month in months)
    statement = f"""
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
        ORDER BY source_month, layer
    """
    ready_deadline = time.monotonic() + 120
    while True:
        try:
            _read_response(f"{endpoint}/v1/info", method="GET")
            break
        except (HTTPError, URLError):
            if time.monotonic() >= ready_deadline:
                raise TimeoutError("Trino did not become ready within 120 seconds") from None
            time.sleep(3)
    rows = _query(endpoint, statement)
    counts = {(row[0], row[1]): int(row[2]) for row in rows}
    missing = [
        month
        for month in months
        if sum(value for (key, _), value in counts.items() if key == month) == 0
    ]
    if missing:
        raise RuntimeError(f"Trino returned no Silver or quarantine rows for: {missing}")
    lineage_sql = f"""
        SELECT source_month, count_if(
            source_url IS NULL OR ingested_at IS NULL OR pipeline_run_id IS NULL
        )
        FROM iceberg.silver.yellow_taxi_trips
        WHERE source_month IN ({month_list})
        GROUP BY source_month
    """
    missing_lineage = {row[0]: int(row[1]) for row in _query(endpoint, lineage_sql)}
    if any(missing_lineage.get(month, 0) for month in months):
        raise RuntimeError(f"Silver rows are missing Bronze lineage: {missing_lineage}")
    rejection_sql = f"""
        SELECT source_month, count_if(
            rejection_reasons IS NULL OR cardinality(rejection_reasons) = 0
        )
        FROM iceberg.silver.yellow_taxi_quarantine
        WHERE source_month IN ({month_list})
        GROUP BY source_month
    """
    missing_reasons = {row[0]: int(row[1]) for row in _query(endpoint, rejection_sql)}
    if any(missing_reasons.get(month, 0) for month in months):
        raise RuntimeError(f"Quarantine rows are missing rejection reasons: {missing_reasons}")
    for month in months:
        print(
            f"{month}: silver={counts.get((month, 'silver'), 0)} "
            f"quarantine={counts.get((month, 'quarantine'), 0)}"
        )


if __name__ == "__main__":
    main()
