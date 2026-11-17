"""Exercise Gold grains and business formulas with a tiny Spark fixture."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.processing.gold import (
    hourly_trip_demand,
    monthly_trip_summary,
    pickup_zone_summary,
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("gold-fixture-smoke")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver = spark.createDataFrame(
            [
                ("2024-01", datetime(2024, 1, 10, 10, 15), 1, 1.0, 8.0, 2.0, 10.0),
                ("2024-01", datetime(2024, 1, 10, 10, 45), 1, 3.0, 12.0, 4.0, 20.0),
                ("2024-01", datetime(2024, 1, 10, 11, 10), 264, 0.0, 4.0, None, 5.0),
                ("2024-02", datetime(2024, 2, 1, 0, 10), 265, 2.0, 10.0, 1.0, 12.0),
            ],
            [
                "source_month",
                "pickup_datetime",
                "pu_location_id",
                "trip_distance",
                "fare_amount",
                "tip_amount",
                "total_amount",
            ],
        )
        zones = spark.createDataFrame(
            [
                (1, "Newark Airport", "EWR"),
                (264, "Unknown", "N/A"),
                (265, "Outside of NYC", "N/A"),
            ],
            ["pickup_location_id", "pickup_zone", "borough"],
        )
        monthly = {row.month: row for row in monthly_trip_summary(silver).collect()}
        january = monthly["2024-01"]
        assert january.trip_count == 3
        assert january.total_revenue == 35.0
        assert abs(january.average_trip_distance - (4.0 / 3.0)) < 1e-9
        assert january.average_fare == 8.0
        assert january.average_tip == 3.0

        hourly = hourly_trip_demand(silver).collect()
        demand = next(
            row for row in hourly if row.source_month == "2024-01" and row.pickup_hour == 10
        )
        assert demand.pickup_date.isoformat() == "2024-01-10"
        assert demand.pickup_location_id == 1
        assert demand.trip_count == 2
        assert demand.average_trip_distance == 2.0

        pickup_zones = pickup_zone_summary(silver, zones).collect()
        zone_one = next(
            row
            for row in pickup_zones
            if row.source_month == "2024-01" and row.pickup_location_id == 1
        )
        assert zone_one.pickup_zone == "Newark Airport"
        assert zone_one.borough == "EWR"
        assert zone_one.trip_count == 2
        assert zone_one.total_revenue == 30.0
        assert sum(row.trip_count for row in hourly) == silver.count()
        assert sum(row.trip_count for row in pickup_zones) == silver.count()
        revenue = silver.agg(F.sum("total_amount")).first()[0]
        assert sum(row.total_revenue for row in pickup_zones) == revenue
        print("Gold fixture passed: monthly, hourly, and pickup-zone metrics reconcile")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
