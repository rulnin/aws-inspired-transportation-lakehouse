"""Verify Gold aggregates are queryable and reconcile to Silver in Trino."""

from __future__ import annotations

import argparse
import math
import os
import re

from trino_silver_smoke import _query

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _assert_close(actual, expected, label: str) -> None:
    if actual is None or expected is None:
        if actual is not None or expected is not None:
            raise RuntimeError(f"{label} differs: Gold={actual}, Silver={expected}")
        return
    if not math.isclose(float(actual), float(expected), rel_tol=1e-10, abs_tol=1e-6):
        raise RuntimeError(f"{label} differs: Gold={actual}, Silver={expected}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", action="append", help="YYYY-MM month to verify; repeatable")
    args = parser.parse_args()
    months = args.month or os.environ.get("LAKEHOUSE_SOURCE_MONTHS", "2024-01,2024-02").split(",")
    months = [month.strip() for month in months]
    if not months or any(not _MONTH_PATTERN.fullmatch(month) for month in months):
        raise ValueError("Months must be valid YYYY-MM values")
    month_list = ", ".join(f"'{month}'" for month in months)
    endpoint = os.environ.get("TRINO_ENDPOINT_URL", "http://127.0.0.1:8080")

    monthly_sql = f"""
        WITH expected AS (
            SELECT source_month, count(*) AS trip_count,
                   coalesce(sum(total_amount), 0e0) AS total_revenue,
                   avg(trip_distance) AS average_trip_distance,
                   avg(fare_amount) AS average_fare,
                   avg(tip_amount) AS average_tip
            FROM iceberg.silver.yellow_taxi_trips
            WHERE source_month IN ({month_list})
            GROUP BY source_month
        )
        SELECT g.month, g.trip_count, expected.trip_count,
               g.total_revenue, expected.total_revenue,
               g.average_trip_distance, expected.average_trip_distance,
               g.average_fare, expected.average_fare,
               g.average_tip, expected.average_tip
        FROM iceberg.gold.monthly_trip_summary g
        JOIN expected ON expected.source_month = g.month
        ORDER BY g.month
    """
    monthly_rows = _query(endpoint, monthly_sql)
    monthly = {row[0]: row for row in monthly_rows}
    if set(monthly) != set(months):
        missing = sorted(set(months) - set(monthly))
        raise RuntimeError(f"Gold monthly summary is missing selected months: {missing}")
    for month, row in monthly.items():
        if int(row[1]) != int(row[2]):
            raise RuntimeError(
                f"Monthly trip_count does not reconcile for {month}: {row[1]} != {row[2]}"
            )
        metrics = ((3, "revenue"), (5, "average distance"), (7, "average fare"), (9, "average tip"))
        for index, label in metrics:
            _assert_close(row[index], row[index + 1], f"{month} {label}")

    hourly_sql = f"""
        SELECT source_month, sum(trip_count)
        FROM iceberg.gold.hourly_trip_demand
        WHERE source_month IN ({month_list})
        GROUP BY source_month
    """
    hourly_counts = {row[0]: int(row[1]) for row in _query(endpoint, hourly_sql)}
    zone_sql = f"""
        SELECT source_month, sum(trip_count), coalesce(sum(total_revenue), 0e0)
        FROM iceberg.gold.pickup_zone_summary
        WHERE source_month IN ({month_list})
        GROUP BY source_month
    """
    zone_counts = {row[0]: row[1:] for row in _query(endpoint, zone_sql)}
    for month, row in monthly.items():
        if hourly_counts.get(month, 0) != int(row[2]):
            raise RuntimeError(f"Hourly trip_count does not reconcile for {month}")
        zone_row = zone_counts.get(month)
        if zone_row is None or int(zone_row[0]) != int(row[2]):
            raise RuntimeError(f"Pickup-zone trip_count does not reconcile for {month}")
        _assert_close(zone_row[1], row[4], f"{month} pickup-zone revenue")

    zone_label_sql = """
        SELECT pickup_location_id, pickup_zone, borough
        FROM iceberg.gold.pickup_zone_summary
        WHERE source_month = '2024-01' AND pickup_location_id = 1
    """
    label = _query(endpoint, zone_label_sql)
    if not label or label[0][1:] != ["Newark Airport", "EWR"]:
        raise RuntimeError(f"Taxi Zone Lookup join returned an unexpected label: {label}")
    for month, row in monthly.items():
        print(
            f"{month}: trips={row[1]}, revenue={float(row[3]):.2f}, "
            f"avg_distance={float(row[5]):.4f}, avg_fare={float(row[7]):.2f}, "
            f"avg_tip={float(row[9]):.2f}"
        )
    print("Gold hourly and pickup-zone totals reconcile to Silver; TLC zone lookup is joined")


if __name__ == "__main__":
    main()
