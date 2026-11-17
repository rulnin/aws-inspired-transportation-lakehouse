"""Ingest the configured official TLC months into the local Bronze bucket."""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
import uuid
from pathlib import Path

import boto3
from botocore.config import Config

from lakehouse.common.logging import configure_logging
from lakehouse.config.settings import load_settings
from lakehouse.ingestion.bronze import land_in_bronze
from lakehouse.ingestion.source import download_month


def _s3_client():
    endpoint = os.environ.get("MINIO_ENDPOINT_URL", "http://127.0.0.1:9000")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month", action="append", help="YYYY-MM; repeat to override configured months"
    )
    parser.add_argument(
        "--run-id", default=uuid.uuid4().hex, help="Pipeline run ID (default: generated UUID)"
    )
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger("lakehouse.ingestion")
    months = tuple(args.month) if args.month else settings.source_months
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    client = _s3_client()
    existing = {entry["Name"] for entry in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)

    with tempfile.TemporaryDirectory(prefix="tlc-bronze-") as workdir:
        for month in months:
            destination = Path(workdir) / f"yellow_tripdata_{month}.parquet"
            downloaded = download_month(month, destination)
            result = land_in_bronze(client, bucket, downloaded, pipeline_run_id=args.run_id)
            logger.info(
                "Bronze month completed",
                extra={
                    "source_month": month,
                    "pipeline_run_id": args.run_id,
                    "component": "bronze_ingestion",
                    "status": result.status,
                    "input_count": 1,
                    "uploaded_count": int(result.status == "uploaded"),
                    "skipped_count": int(result.status == "skipped"),
                    "size_bytes": result.size_bytes,
                    "elapsed_seconds": round(result.elapsed_seconds, 3),
                    "download_elapsed_seconds": round(downloaded.elapsed_seconds, 3),
                    "object_key": result.object_key,
                },
            )


if __name__ == "__main__":
    main()
