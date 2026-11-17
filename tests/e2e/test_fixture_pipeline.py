"""Opt-in Bronze-to-Gold fixture test across MinIO, Spark, Iceberg, and Trino."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import boto3
import pyarrow as arrow
import pyarrow.parquet as parquet
import pytest
from botocore.config import Config

from lakehouse.ingestion.bronze import manifest_key, object_key
from lakehouse.ingestion.verification import verify_months

ROOT = Path(__file__).resolve().parents[2]
SOURCE_MONTH = "2099-01"
pytestmark = pytest.mark.e2e


def _source_bytes() -> bytes:
    table = arrow.table(
        {
            "tpep_pickup_datetime": arrow.array(
                [
                    datetime(2099, 1, 10, 10, 15),
                    datetime(2099, 1, 10, 10, 45),
                    datetime(2099, 1, 10, 11, 10),
                    datetime(2099, 1, 10, 12, 0),
                ],
                type=arrow.timestamp("us"),
            ),
            "tpep_dropoff_datetime": arrow.array(
                [
                    datetime(2099, 1, 10, 10, 25),
                    datetime(2099, 1, 10, 10, 55),
                    datetime(2099, 1, 10, 11, 20),
                    datetime(2099, 1, 10, 11, 59),
                ],
                type=arrow.timestamp("us"),
            ),
            "trip_distance": [1.0, 3.0, 0.0, -1.0],
            "PULocationID": [1, 1, 264, 2],
            "DOLocationID": [2, 1, 265, 1],
            "fare_amount": [8.0, 12.0, 4.0, 5.0],
            "tip_amount": [2.0, 4.0, None, 1.0],
            "total_amount": [10.0, 20.0, 5.0, 6.0],
        }
    )
    stream = io.BytesIO()
    parquet.write_table(table, stream)
    return stream.getvalue()


def test_tiny_fixture_pipeline_and_same_month_reprocessing() -> None:
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
    existing_buckets = {item["Name"] for item in client.list_buckets()["Buckets"]}
    if bucket not in existing_buckets:
        client.create_bucket(Bucket=bucket)
    payload = _source_bytes()
    run_id = "phase8-e2e-fixture"
    manifest = {
        "source_url": "https://example.invalid/fixture/yellow_tripdata_2099-01.parquet",
        "source_month": SOURCE_MONTH,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "pipeline_run_id": run_id,
    }
    key = object_key(SOURCE_MONTH)
    meta_key = manifest_key(SOURCE_MONTH)
    client.put_object(Bucket=bucket, Key=key, Body=payload)
    client.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(manifest).encode(),
        ContentType="application/json",
    )
    try:
        os.environ["LAKEHOUSE_SOURCE_MONTHS"] = SOURCE_MONTH
        assert verify_months() == {SOURCE_MONTH: 4}
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                ".env",
                "--profile",
                "core",
                "--profile",
                "spark",
                "run",
                "--rm",
                "spark",
                "/workspace/scripts/e2e_fixture_smoke.py",
                "--month",
                SOURCE_MONTH,
                "--run-id",
                run_id,
            ],
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        assert "passed twice with stable Trino results" in result.stdout
    finally:
        client.delete_object(Bucket=bucket, Key=key)
        client.delete_object(Bucket=bucket, Key=meta_key)
