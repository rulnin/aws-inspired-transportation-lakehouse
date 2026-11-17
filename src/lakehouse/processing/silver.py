"""Normalize Bronze taxi data, classify quality, and replace monthly Iceberg data."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.processing.schema import (
    CANONICAL_FIELDS,
    LOCATION_ID_MAX,
    LOCATION_ID_MIN,
    MONEY_FIELDS,
    resolve_source_columns,
)

LOGGER = logging.getLogger(__name__)
VALID_TABLE = "lakehouse.silver.yellow_taxi_trips"
QUARANTINE_TABLE = "lakehouse.silver.yellow_taxi_quarantine"


@dataclass(frozen=True, slots=True)
class ClassifiedRows:
    """Silver and quarantine projections for one source frame."""

    valid: DataFrame
    rejected: DataFrame
    classified: DataFrame


@dataclass(frozen=True, slots=True)
class QualityCounts:
    """Counts emitted for a processed month."""

    input_rows: int
    silver_rows: int
    quarantined_rows: int
    warning_rows: int


def classify_source(
    source: DataFrame,
    *,
    source_month: str,
    source_file: str,
    manifest: dict[str, Any],
) -> ClassifiedRows:
    """Cast and classify rows while preserving malformed or rejected values as reasons."""
    columns = resolve_source_columns(source.columns)
    raw = source.select(
        *[
            F.col(columns[field.name]).alias(f"_raw_{field.name}")
            if columns[field.name] is not None
            else F.lit(None).alias(f"_raw_{field.name}")
            for field in CANONICAL_FIELDS
        ]
    )

    typed = raw.select(
        *[F.col(f"_raw_{field.name}") for field in CANONICAL_FIELDS],
        *[
            F.expr(f"try_cast(`_raw_{field.name}` AS {field.spark_type})").alias(field.name)
            for field in CANONICAL_FIELDS
        ],
    )
    enriched = typed
    enriched = (
        enriched.withColumn("source_month", F.lit(source_month))
        .withColumn("source_year", F.lit(int(source_month[:4])).cast("int"))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("source_url", F.lit(manifest["source_url"]))
        .withColumn("ingested_at", F.to_timestamp(F.lit(manifest["retrieved_at"])))
        .withColumn("pipeline_run_id", F.lit(manifest["pipeline_run_id"]))
        .withColumn("processed_at", F.current_timestamp())
    )

    rejection_checks = []
    for field in CANONICAL_FIELDS:
        raw_value = F.col(f"_raw_{field.name}")
        value = F.col(field.name)
        if field.required:
            rejection_checks.append(F.when(raw_value.isNull(), F.lit(f"missing_{field.name}")))
        if field.spark_type != "string":
            rejection_checks.append(
                F.when(raw_value.isNotNull() & value.isNull(), F.lit(f"unparseable_{field.name}"))
            )
        if field.spark_type == "int":
            parsed_number = F.expr(f"try_cast(`_raw_{field.name}` AS double)")
            rejection_checks.append(
                F.when(
                    parsed_number.isNotNull() & (parsed_number != value.cast("double")),
                    F.lit(f"non_integral_{field.name}"),
                )
            )

    rejection_checks.extend(
        [
            F.when(
                F.col("dropoff_datetime") < F.col("pickup_datetime"),
                F.lit("dropoff_before_pickup"),
            ),
            F.when(F.col("trip_distance") < 0, F.lit("negative_trip_distance")),
            F.when(
                F.col("passenger_count").isNotNull() & (F.col("passenger_count") < 0),
                F.lit("negative_passenger_count"),
            ),
        ]
    )
    for field_name in ("pu_location_id", "do_location_id"):
        rejection_checks.append(
            F.when(
                F.col(field_name).isNotNull()
                & ~F.col(field_name).between(LOCATION_ID_MIN, LOCATION_ID_MAX),
                F.lit(f"out_of_range_{field_name}"),
            )
        )

    warnings = [F.when(F.col(name) < 0, F.lit(f"negative_{name}")) for name in MONEY_FIELDS]
    classified = enriched.withColumn(
        "rejection_reasons",
        F.filter(F.array(*rejection_checks), lambda reason: reason.isNotNull()),
    ).withColumn(
        "quality_warnings",
        F.filter(F.array(*warnings), lambda warning: warning.isNotNull()),
    )
    result_columns = ["source_month", "source_year"] + [field.name for field in CANONICAL_FIELDS]
    result_columns += [
        "source_file",
        "source_url",
        "ingested_at",
        "pipeline_run_id",
        "processed_at",
        "quality_warnings",
    ]
    valid = classified.filter(F.size("rejection_reasons") == 0).select(*result_columns)
    rejected = classified.filter(F.size("rejection_reasons") > 0).select(
        *result_columns, "rejection_reasons"
    )
    return ClassifiedRows(valid=valid, rejected=rejected, classified=classified)


def _table_schema(*, include_reasons: bool = False) -> str:
    fields = ["source_month string", "source_year int"]
    fields.extend(f"{field.name} {field.spark_type}" for field in CANONICAL_FIELDS)
    fields.extend(
        [
            "source_file string",
            "source_url string",
            "ingested_at timestamp",
            "pipeline_run_id string",
            "processed_at timestamp",
            "quality_warnings array<string>",
        ]
    )
    if include_reasons:
        fields.append("rejection_reasons array<string>")
    return ", ".join(fields)


def _ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    for table, include_reasons in ((VALID_TABLE, False), (QUARANTINE_TABLE, True)):
        schema = _table_schema(include_reasons=include_reasons)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ({schema}) "
            "USING iceberg PARTITIONED BY (source_month) "
            "TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')"
        )


def _read_manifest(spark: SparkSession, manifest_uri: str) -> dict[str, Any]:
    row = spark.read.json(manifest_uri).first()
    if row is None:
        raise ValueError(f"Bronze manifest is empty: {manifest_uri}")
    manifest = row.asDict(recursive=True)
    missing = {"source_url", "retrieved_at", "pipeline_run_id"} - manifest.keys()
    if missing:
        raise ValueError(f"Bronze manifest {manifest_uri} is missing fields: {sorted(missing)}")
    return manifest


def process_month(spark: SparkSession, bucket: str, source_month: str) -> QualityCounts:
    """Read a Bronze month, classify it, and replace that month in both tables."""
    year, month = source_month.split("-")
    prefix = f"raw/yellow_taxi/year={year}/month={month}/yellow_tripdata_{source_month}"
    source_uri = f"s3a://{bucket}/{prefix}.parquet"
    manifest_uri = f"s3a://{bucket}/{prefix}.manifest.json"
    manifest = _read_manifest(spark, manifest_uri)
    started = time.monotonic()
    classified = classify_source(
        spark.read.parquet(source_uri),
        source_month=source_month,
        source_file=source_uri,
        manifest=manifest,
    )
    frame = classified.classified.persist(StorageLevel.DISK_ONLY)
    try:
        counts_row = frame.agg(
            F.count(F.lit(1)).alias("input_rows"),
            F.sum(F.when(F.size("rejection_reasons") == 0, 1).otherwise(0)).alias("silver_rows"),
            F.sum(F.when(F.size("rejection_reasons") > 0, 1).otherwise(0)).alias(
                "quarantined_rows"
            ),
            F.sum(F.when(F.size("quality_warnings") > 0, 1).otherwise(0)).alias("warning_rows"),
        ).first()
        counts = QualityCounts(
            input_rows=int(counts_row["input_rows"] or 0),
            silver_rows=int(counts_row["silver_rows"] or 0),
            quarantined_rows=int(counts_row["quarantined_rows"] or 0),
            warning_rows=int(counts_row["warning_rows"] or 0),
        )

        _ensure_tables(spark)
        partition_filter = F.col("source_month") == F.lit(source_month)
        classified.valid.writeTo(VALID_TABLE).overwrite(partition_filter)
        classified.rejected.writeTo(QUARANTINE_TABLE).overwrite(partition_filter)
    finally:
        frame.unpersist()

    LOGGER.info(
        "Silver month completed",
        extra={
            "source_month": source_month,
            "pipeline_run_id": manifest["pipeline_run_id"],
            "component": "silver_processing",
            "status": "success",
            "input_count": counts.input_rows,
            "output_count": counts.silver_rows,
            "rejected_count": counts.quarantined_rows,
            "warning_count": counts.warning_rows,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        },
    )
    return counts
