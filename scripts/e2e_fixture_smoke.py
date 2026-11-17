"""Run and verify a tiny Bronze-to-Gold fixture through Trino, twice."""

from __future__ import annotations

import argparse
import json
import os
import re
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession

from lakehouse.processing.gold import process_month as process_gold_month
from lakehouse.processing.silver import process_month as process_silver_month

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _trino_query(statement: str) -> list[list[object]]:
    endpoint = "http://trino:8080/v1/statement"
    request = Request(
        endpoint,
        data=statement.encode(),
        method="POST",
        headers={"X-Trino-User": "e2e-fixture", "Content-Type": "text/plain"},
    )
    with urlopen(request, timeout=30) as response:
        result = json.load(response)
    rows = list(result.get("data", []))
    while result.get("nextUri"):
        with urlopen(result["nextUri"], timeout=30) as response:
            result = json.load(response)
        if "error" in result:
            raise RuntimeError(f"Trino fixture query failed: {result['error']}")
        rows.extend(result.get("data", []))
    return rows


def _assert_trino_results(source_month: str) -> None:
    silver = _trino_query(
        "SELECT count(*) FROM iceberg.silver.yellow_taxi_trips "
        f"WHERE source_month = '{source_month}'"
    )
    rejected = _trino_query(
        "SELECT count(*), count_if(contains(rejection_reasons, 'dropoff_before_pickup')) "
        "FROM iceberg.silver.yellow_taxi_quarantine "
        f"WHERE source_month = '{source_month}'"
    )
    monthly = _trino_query(
        "SELECT trip_count, total_revenue FROM iceberg.gold.monthly_trip_summary "
        f"WHERE month = '{source_month}'"
    )
    hourly = _trino_query(
        "SELECT sum(trip_count) FROM iceberg.gold.hourly_trip_demand "
        f"WHERE source_month = '{source_month}'"
    )
    zones = _trino_query(
        "SELECT sum(trip_count) FROM iceberg.gold.pickup_zone_summary "
        f"WHERE source_month = '{source_month}'"
    )
    expected = (silver, rejected, monthly, hourly, zones)
    if expected != ([[3]], [[1, 1]], [[3, 35.0]], [[3]], [[3]]):
        raise AssertionError(f"Trino fixture outputs did not reconcile: {expected!r}")


def _clear_test_partitions(spark: SparkSession, source_month: str) -> None:
    tables = (
        ("lakehouse.silver.yellow_taxi_trips", "source_month"),
        ("lakehouse.silver.yellow_taxi_quarantine", "source_month"),
        ("lakehouse.gold.monthly_trip_summary", "month"),
        ("lakehouse.gold.hourly_trip_demand", "source_month"),
        ("lakehouse.gold.pickup_zone_summary", "source_month"),
    )
    for table, partition in tables:
        if spark.catalog.tableExists(table):
            spark.sql(f"DELETE FROM {table} WHERE {partition} = '{source_month}'")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--run-id", default="phase8-fixture-run")
    args = parser.parse_args()
    if not MONTH_PATTERN.fullmatch(args.month):
        parser.error("month must use YYYY-MM")

    spark = SparkSession.builder.appName("transportation-lakehouse-e2e-fixture").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    quarantine_table = "lakehouse.silver.yellow_taxi_quarantine"
    try:
        _clear_test_partitions(spark, args.month)

        zones = spark.createDataFrame(
            [
                (1, "Fixture Zone One", "Fixture Borough"),
                (2, "Fixture Zone Two", "Fixture Borough"),
                (264, "Other", "Fixture Borough"),
            ],
            ["pickup_location_id", "pickup_zone", "borough"],
        )
        for attempt in range(2):
            counts = process_silver_month(spark, bucket, args.month)
            if (counts.input_rows, counts.silver_rows, counts.quarantined_rows) != (4, 3, 1):
                raise AssertionError(f"Unexpected Silver counts: {counts}")
            reasons = {
                reason
                for row in spark.table(quarantine_table)
                .filter(f"source_month = '{args.month}'")
                .select("rejection_reasons")
                .collect()
                for reason in row.rejection_reasons
            }
            if "dropoff_before_pickup" not in reasons:
                raise AssertionError(f"Malformed fixture row was not quarantined: {reasons}")

            gold_counts = process_gold_month(
                spark,
                args.month,
                zones,
                pipeline_run_id=f"{args.run_id}-{attempt}",
            )
            if gold_counts.silver_rows != 3 or gold_counts.monthly_rows != 1:
                raise AssertionError(f"Unexpected Gold counts: {gold_counts}")
            _assert_trino_results(args.month)

        print("Tiny Bronze-to-Gold fixture passed twice with stable Trino results")
    finally:
        try:
            _clear_test_partitions(spark, args.month)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
