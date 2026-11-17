"""Exercise Silver casts and quality classification with a tiny Spark fixture."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from lakehouse.processing.silver import classify_source


def main() -> None:
    spark = (
        SparkSession.builder.appName("silver-fixture-smoke")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = spark.createDataFrame(
            [
                ("2024-01-01 10:00:00", "2024-01-01 10:10:00", "1.2", 1, 2, 1, 5.0),
                ("2024-01-02 10:00:00", "2024-01-02 10:10:00", "0", 1, 2, None, -2.0),
                ("2024-01-02 11:00:00", "2024-01-02 11:10:00", "0", 264, 2, 1, 5.0),
                ("2024-01-02 12:00:00", "2024-01-02 12:10:00", "0", 265, 2, 1, 5.0),
                (None, "2024-01-03 10:10:00", "1", 1, 2, 1, 5.0),
                ("2024-01-04 10:10:00", "2024-01-04 10:00:00", "1", 1, 2, 1, 5.0),
                ("2024-01-05 10:00:00", "2024-01-05 10:10:00", "-1", 1, 2, 1, 5.0),
                ("2024-01-06 10:00:00", "2024-01-06 10:10:00", "1", 1, 2, -1, 5.0),
                ("2024-01-07 10:00:00", "2024-01-07 10:10:00", "1", 266, 2, 1, 5.0),
                ("2024-01-08 10:00:00", "2024-01-08 10:10:00", "unknown", 1, 2, 1, 5.0),
            ],
            [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "trip_distance",
                "PULocationID",
                "DOLocationID",
                "passenger_count",
                "fare_amount",
            ],
        )
        manifest = {
            "source_url": "https://example.invalid/fixture.parquet",
            "retrieved_at": datetime(2024, 2, 1, tzinfo=timezone.utc).isoformat(),  # noqa: UP017
            "pipeline_run_id": "fixture-run",
        }
        result = classify_source(
            source,
            source_month="2024-01",
            source_file="fixture.parquet",
            manifest=manifest,
        )
        valid = result.valid.collect()
        rejected = result.rejected.collect()
        assert len(valid) == 4
        assert len(rejected) == 6
        assert valid[0].pickup_datetime is not None
        assert {row.pu_location_id for row in valid} >= {264, 265}
        warning_row = next(row for row in valid if row.fare_amount == -2.0)
        assert "negative_fare_amount" in warning_row.quality_warnings
        reasons = {reason for row in rejected for reason in row.rejection_reasons}
        assert {
            "missing_pickup_datetime",
            "dropoff_before_pickup",
            "negative_trip_distance",
            "negative_passenger_count",
            "out_of_range_pu_location_id",
            "unparseable_trip_distance",
        } <= reasons
        assert all(row.airport_fee is None for row in valid)
        print("Silver fixture classification passed: 4 valid, 6 quarantined, 1 retained warning")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
