"""Normalize configured Bronze months into valid and quarantined Silver Iceberg tables."""

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
from lakehouse.processing.silver import process_month

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month", action="append", help="YYYY-MM; repeat to override configured months"
    )
    parser.add_argument("--processing-run-id", default=uuid.uuid4().hex)
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_level)
    months = tuple(args.month) if args.month else settings.source_months
    invalid_months = [month for month in months if not _MONTH_PATTERN.fullmatch(month)]
    if invalid_months:
        parser.error("months must use YYYY-MM with a valid month: " + ", ".join(invalid_months))
    if len(set(months)) != len(months):
        parser.error("months must not contain duplicates")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    spark = SparkSession.builder.appName("transportation-lakehouse-silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        logger = logging.getLogger("lakehouse.processing.silver")
        logger.info(
            "Starting Silver batch",
            extra={
                "pipeline_run_id": args.processing_run_id,
                "component": "silver_processing",
                "status": "started",
                "input_count": len(months),
            },
        )
        for source_month in months:
            counts = process_month(spark, bucket, source_month)
            send_gauge("silver_input_rows", counts.input_rows, source_month=source_month)
            send_gauge("silver_valid_rows", counts.silver_rows, source_month=source_month)
            send_gauge("silver_rejected_rows", counts.quarantined_rows, source_month=source_month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
