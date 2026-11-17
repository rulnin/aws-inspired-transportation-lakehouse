"""Download the official TLC Taxi Zone Lookup CSV into MinIO reference storage."""

from __future__ import annotations

import csv
import hashlib
import logging
import os
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from lakehouse.common.logging import configure_logging

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OBJECT_KEY = "reference/taxi_zone_lookup.csv"
REQUIRED_COLUMNS = {"LocationID", "Borough", "Zone", "service_zone"}
LOGGER = logging.getLogger(__name__)


def _validate_csv(path: Path) -> int:
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"TLC Taxi Zone Lookup is missing columns: {sorted(missing)}")
        location_ids: set[int] = set()
        for row in reader:
            location_id = int(row["LocationID"])
            if location_id in location_ids:
                raise ValueError(f"TLC Taxi Zone Lookup has duplicate LocationID {location_id}")
            if not row["Zone"] or not row["Borough"]:
                raise ValueError(f"TLC Taxi Zone Lookup has an unlabeled LocationID {location_id}")
            location_ids.add(location_id)
    if not location_ids:
        raise ValueError("TLC Taxi Zone Lookup is empty")
    return len(location_ids)


def main() -> None:
    configure_logging(os.environ.get("LAKEHOUSE_LOG_LEVEL", "INFO").upper())
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
    started = time.monotonic()
    digest = hashlib.sha256()
    size_bytes = 0
    expected_size: int | None = None
    with tempfile.TemporaryDirectory(prefix="tlc-zone-lookup-") as workdir:
        path = Path(workdir) / "taxi_zone_lookup.csv"
        try:
            with urlopen(SOURCE_URL, timeout=30) as response, path.open("wb") as target:
                status = getattr(response, "status", 200)
                if not 200 <= status < 300:
                    raise RuntimeError(f"TLC returned HTTP {status} for the Taxi Zone Lookup")
                length = response.headers.get("Content-Length")
                expected_size = int(length) if length else None
                while block := response.read(64 * 1024):
                    target.write(block)
                    digest.update(block)
                    size_bytes += len(block)
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            raise RuntimeError(
                f"Could not download the official Taxi Zone Lookup: {error}"
            ) from error

        if size_bytes == 0 or (expected_size is not None and size_bytes != expected_size):
            raise RuntimeError(
                "Incomplete Taxi Zone Lookup download: "
                f"expected {expected_size}, received {size_bytes}"
            )
        row_count = _validate_csv(path)
        sha256 = digest.hexdigest()
        try:
            metadata = client.head_object(Bucket=bucket, Key=OBJECT_KEY).get("Metadata", {})
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
            metadata = {}

        if metadata.get("sha256") == sha256:
            status = "skipped"
        else:
            with path.open("rb") as body:
                client.upload_fileobj(
                    body,
                    bucket,
                    OBJECT_KEY,
                    ExtraArgs={
                        "ContentType": "text/csv",
                        "Metadata": {"sha256": sha256, "source-url": SOURCE_URL},
                    },
                )
            status = "uploaded"

    LOGGER.info(
        "Taxi Zone Lookup reference completed",
        extra={
            "component": "reference_ingestion",
            "status": status,
            "input_count": row_count,
            "size_bytes": size_bytes,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "object_key": OBJECT_KEY,
        },
    )


if __name__ == "__main__":
    main()
