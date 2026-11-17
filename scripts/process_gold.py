"""Build reconciled Gold aggregate tables from selected Silver months."""

from __future__ import annotations

import argparse
import logging
import os
import re
import uuid

from pyspark.sql import SparkSession

from lakehouse.common.logging import configure_logging
from lakehouse.common.metrics import send_gauge
from lakehouse.config.settings import load_settings
from lakehouse.processing.gold import load_zone_lookup, process_month

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month", action="append", help="YYYY-MM; repeat to override configured months"
    )
    parser.add_argument("--pipeline-run-id", default=uuid.uuid4().hex)
    args = parser.parse_args()
    settings = load_settings()
    configure_logging(settings.log_level)
    months = tuple(args.month) if args.month else settings.source_months
    invalid = [month for month in months if not _MONTH_PATTERN.fullmatch(month)]
    if invalid:
        parser.error("months must use YYYY-MM with a valid month: " + ", ".join(invalid))
    if len(set(months)) != len(months):
        parser.error("months must not contain duplicates")

    spark = SparkSession.builder.appName("transportation-lakehouse-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
        zones = load_zone_lookup(spark, bucket)
        logger = logging.getLogger("lakehouse.processing.gold")
        logger.info(
            "Starting Gold batch",
            extra={
                "pipeline_run_id": args.pipeline_run_id,
                "component": "gold_processing",
                "status": "started",
                "input_count": len(months),
            },
        )
        for source_month in months:
            counts = process_month(
                spark,
                source_month,
                zones,
                pipeline_run_id=args.pipeline_run_id,
            )
            send_gauge(
                "gold_output_rows",
                counts.monthly_rows + counts.hourly_rows + counts.zone_rows,
                source_month=source_month,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
