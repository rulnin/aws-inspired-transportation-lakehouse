"""Gold aggregates with explicit grains and source-month replacement."""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)
MONTHLY_TABLE = "lakehouse.gold.monthly_trip_summary"
HOURLY_TABLE = "lakehouse.gold.hourly_trip_demand"
ZONE_TABLE = "lakehouse.gold.pickup_zone_summary"


@dataclass(frozen=True, slots=True)
class GoldCounts:
    """Input and output row counts for one processed source month."""

    silver_rows: int
    monthly_rows: int
    hourly_rows: int
    zone_rows: int


def monthly_trip_summary(silver: DataFrame) -> DataFrame:
    """Aggregate one row per source month from valid Silver trips."""
    return (
        silver.groupBy("source_month")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.coalesce(F.sum("total_amount"), F.lit(0.0)).alias("total_revenue"),
            F.avg("trip_distance").alias("average_trip_distance"),
            F.avg("fare_amount").alias("average_fare"),
            F.avg("tip_amount").alias("average_tip"),
        )
        .withColumnRenamed("source_month", "month")
        .select(
            "month",
            "trip_count",
            "total_revenue",
            "average_trip_distance",
            "average_fare",
            "average_tip",
        )
    )


def hourly_trip_demand(silver: DataFrame) -> DataFrame:
    """Aggregate trips by source month, pickup date/hour, and pickup location."""
    return silver.groupBy(
        "source_month",
        F.to_date("pickup_datetime").alias("pickup_date"),
        F.hour("pickup_datetime").alias("pickup_hour"),
        F.col("pu_location_id").alias("pickup_location_id"),
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.avg("trip_distance").alias("average_trip_distance"),
    )


def pickup_zone_summary(silver: DataFrame, zones: DataFrame) -> DataFrame:
    """Aggregate by source month and pickup ID, retaining IDs without a lookup match."""
    totals = silver.groupBy("source_month", "pu_location_id").agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.coalesce(F.sum("total_amount"), F.lit(0.0)).alias("total_revenue"),
    )
    return totals.join(zones, totals.pu_location_id == zones.pickup_location_id, "left").select(
        totals.source_month,
        totals.pu_location_id.alias("pickup_location_id"),
        zones.pickup_zone,
        zones.borough,
        totals.trip_count,
        totals.total_revenue,
    )


def load_zone_lookup(spark: SparkSession, bucket: str) -> DataFrame:
    """Read and validate the official TLC zone lookup stored in MinIO."""
    uri = f"s3a://{bucket}/reference/taxi_zone_lookup.csv"
    source = spark.read.option("header", True).option("mode", "FAILFAST").csv(uri)
    required = {"LocationID", "Borough", "Zone"}
    missing = required - set(source.columns)
    if missing:
        raise ValueError(f"Taxi Zone Lookup is missing columns: {sorted(missing)}")
    zones = source.select(
        F.expr("try_cast(LocationID AS int)").alias("pickup_location_id"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("borough"),
    )
    invalid = (
        zones.filter(
            F.col("pickup_location_id").isNull()
            | F.col("pickup_zone").isNull()
            | F.col("borough").isNull()
        )
        .limit(1)
        .count()
    )
    if invalid:
        raise ValueError("Taxi Zone Lookup contains a missing or invalid ID, zone, or borough")
    duplicate = (
        zones.groupBy("pickup_location_id").count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicate:
        raise ValueError("Taxi Zone Lookup has duplicate location IDs")
    if not zones.take(1):
        raise ValueError("Taxi Zone Lookup is empty")
    return zones


def _ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    definitions = (
        (
            MONTHLY_TABLE,
            "month string, trip_count bigint, total_revenue double, "
            "average_trip_distance double, average_fare double, average_tip double",
            "month",
        ),
        (
            HOURLY_TABLE,
            "source_month string, pickup_date date, pickup_hour int, pickup_location_id int, "
            "trip_count bigint, average_trip_distance double",
            "source_month",
        ),
        (
            ZONE_TABLE,
            "source_month string, pickup_location_id int, pickup_zone string, borough string, "
            "trip_count bigint, total_revenue double",
            "source_month",
        ),
    )
    for table, schema, partition in definitions:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ({schema}) USING iceberg "
            f"PARTITIONED BY ({partition}) "
            "TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')"
        )


def _same_optional_number(actual: Any, expected: Any) -> bool:
    if actual is None or expected is None:
        return actual is None and expected is None
    return math.isclose(float(actual), float(expected), rel_tol=1e-10, abs_tol=1e-6)


def _assert_reconciled(
    source_stats: Any,
    monthly: DataFrame,
    hourly: DataFrame,
    zones: DataFrame,
) -> None:
    monthly_row = monthly.first()
    hourly_row = hourly.agg(F.sum("trip_count").alias("trip_count")).first()
    zone_row = zones.agg(
        F.sum("trip_count").alias("trip_count"),
        F.sum("total_revenue").alias("total_revenue"),
    ).first()
    silver_rows = int(source_stats.trip_count)
    if monthly_row is None or int(monthly_row.trip_count) != silver_rows:
        raise ValueError("Monthly Gold trip_count does not reconcile to Silver")
    if int(hourly_row.trip_count or 0) != silver_rows:
        raise ValueError("Hourly Gold trip_count does not reconcile to Silver")
    if int(zone_row.trip_count or 0) != silver_rows:
        raise ValueError("Pickup-zone Gold trip_count does not reconcile to Silver")

    if not _same_optional_number(monthly_row.total_revenue, source_stats.total_revenue):
        raise ValueError("Monthly Gold revenue does not reconcile to Silver")
    if not _same_optional_number(zone_row.total_revenue, source_stats.total_revenue):
        raise ValueError("Pickup-zone Gold revenue does not reconcile to Silver")
    for gold_field, silver_field in (
        ("average_trip_distance", "average_trip_distance"),
        ("average_fare", "average_fare"),
        ("average_tip", "average_tip"),
    ):
        if not _same_optional_number(getattr(monthly_row, gold_field), source_stats[silver_field]):
            raise ValueError(f"Monthly Gold {gold_field} does not reconcile to Silver")


def process_month(
    spark: SparkSession,
    source_month: str,
    zones: DataFrame,
    *,
    pipeline_run_id: str,
) -> GoldCounts:
    """Build, reconcile, and idempotently replace Gold outputs for one month."""
    started = time.monotonic()
    silver = (
        spark.table("lakehouse.silver.yellow_taxi_trips")
        .filter(F.col("source_month") == source_month)
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        source_stats = silver.agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.coalesce(F.sum("total_amount"), F.lit(0.0)).alias("total_revenue"),
            F.avg("trip_distance").alias("average_trip_distance"),
            F.avg("fare_amount").alias("average_fare"),
            F.avg("tip_amount").alias("average_tip"),
        ).first()
        monthly = monthly_trip_summary(silver)
        hourly = hourly_trip_demand(silver)
        zone_summary = pickup_zone_summary(silver, zones)
        _assert_reconciled(source_stats, monthly, hourly, zone_summary)

        silver_rows = int(source_stats.trip_count)
        monthly_rows = monthly.count()
        hourly_rows = hourly.count()
        zone_rows = zone_summary.count()
        _ensure_tables(spark)
        monthly.writeTo(MONTHLY_TABLE).overwrite(F.col("month") == F.lit(source_month))
        hourly.writeTo(HOURLY_TABLE).overwrite(F.col("source_month") == F.lit(source_month))
        zone_summary.writeTo(ZONE_TABLE).overwrite(F.col("source_month") == F.lit(source_month))
    finally:
        silver.unpersist()

    LOGGER.info(
        "Gold month completed",
        extra={
            "source_month": source_month,
            "pipeline_run_id": pipeline_run_id,
            "component": "gold_processing",
            "status": "success",
            "input_count": silver_rows,
            "output_count": monthly_rows + hourly_rows + zone_rows,
            "monthly_rows": monthly_rows,
            "hourly_rows": hourly_rows,
            "zone_rows": zone_rows,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        },
    )
    return GoldCounts(silver_rows, monthly_rows, hourly_rows, zone_rows)
