"""Verify Bronze object bytes, manifests, and Parquet metadata."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path

import boto3
import pyarrow.parquet as parquet
from botocore.config import Config

from lakehouse.config.settings import load_settings
from lakehouse.ingestion.bronze import manifest_key, object_key

LOGGER = logging.getLogger(__name__)


def assert_silver_reconciles(
    source_month: str, source_rows: int, silver_rows: int, rejected_rows: int
) -> None:
    """Fail when valid and quarantined output counts do not cover a Bronze month."""
    if silver_rows + rejected_rows != source_rows:
        raise ValueError(
            f"Silver plus quarantine does not reconcile to Bronze for {source_month}: "
            f"{silver_rows} + {rejected_rows} != {source_rows}"
        )


def verify_months() -> dict[str, int]:
    """Verify each selected Bronze object and return its Parquet row count."""
    endpoint = os.environ.get("MINIO_ENDPOINT_URL", "http://127.0.0.1:9000")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    results: dict[str, int] = {}
    for month in load_settings().source_months:
        manifest_response = client.get_object(Bucket=bucket, Key=manifest_key(month))
        manifest = json.loads(manifest_response["Body"].read())
        manifest_response["Body"].close()
        if manifest.get("source_month") != month:
            raise ValueError(f"Bronze manifest month does not match {month}")
        required = {"source_url", "retrieved_at", "pipeline_run_id", "size_bytes", "sha256"}
        if required - manifest.keys():
            raise ValueError(f"Bronze manifest for {month} is missing required fields")

        response = client.get_object(Bucket=bucket, Key=object_key(month))
        digest = hashlib.sha256()
        size_bytes = 0
        with tempfile.TemporaryDirectory(prefix="bronze-verify-") as workdir:
            path = Path(workdir) / f"yellow_tripdata_{month}.parquet"
            with path.open("wb") as target:
                while block := response["Body"].read(1024 * 1024):
                    target.write(block)
                    digest.update(block)
                    size_bytes += len(block)
            response["Body"].close()
            if size_bytes != int(manifest["size_bytes"]):
                raise ValueError(f"Bronze object size does not match manifest for {month}")
            if digest.hexdigest() != manifest["sha256"]:
                raise ValueError(f"Bronze object SHA-256 does not match manifest for {month}")
            rows = parquet.ParquetFile(path).metadata.num_rows

        results[month] = rows
        LOGGER.info(
            "Bronze object verified",
            extra={
                "component": "bronze_verification",
                "status": "success",
                "pipeline_run_id": os.environ.get("PIPELINE_RUN_ID"),
                "source_month": month,
                "input_count": rows,
                "size_bytes": size_bytes,
                "object_key": object_key(month),
            },
        )
    return results
