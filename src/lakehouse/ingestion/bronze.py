"""Idempotent S3-compatible Bronze landing and manifest creation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from lakehouse.ingestion.source import DownloadResult

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BronzeResult:
    """Outcome for one source month."""

    source_month: str
    object_key: str
    manifest_key: str
    status: str
    size_bytes: int
    elapsed_seconds: float


def object_key(source_month: str) -> str:
    """Return the canonical raw object key for a source month."""
    year, month = source_month.split("-")
    return f"raw/yellow_taxi/year={year}/month={month}/yellow_tripdata_{source_month}.parquet"


def manifest_key(source_month: str) -> str:
    """Return the adjacent JSON manifest key."""
    return object_key(source_month).removesuffix(".parquet") + ".manifest.json"


def build_manifest(
    download: DownloadResult,
    *,
    pipeline_run_id: str,
    retrieved_at: datetime | None = None,
) -> dict[str, Any]:
    """Build a stable, serializable record of the ingested source file."""
    timestamp = retrieved_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("retrieved_at must include a timezone")
    return {
        "source_url": download.source_url,
        "source_month": download.source_month,
        "retrieved_at": timestamp.astimezone(UTC).isoformat(),
        "size_bytes": download.size_bytes,
        "sha256": download.sha256,
        "pipeline_run_id": pipeline_run_id,
    }


def land_in_bronze(
    client: Any,
    bucket: str,
    download: DownloadResult,
    *,
    pipeline_run_id: str,
) -> BronzeResult:
    """Upload raw bytes and manifest, skipping identical already-landed data."""
    started = datetime.now(UTC)
    key = object_key(download.source_month)
    metadata_key = manifest_key(download.source_month)
    manifest = build_manifest(download, pipeline_run_id=pipeline_run_id)
    existing_manifest = _existing_manifest(client, bucket, metadata_key)

    if existing_manifest and _same_source(existing_manifest, manifest):
        try:
            client.head_object(Bucket=bucket, Key=key)
        except Exception as error:
            if not _is_missing(error):
                raise
        else:
            elapsed = (datetime.now(UTC) - started).total_seconds()
            LOGGER.info(
                "Bronze source already current",
                extra={
                    "source_month": download.source_month,
                    "component": "bronze_ingestion",
                    "status": "skipped",
                },
            )
            return BronzeResult(
                download.source_month, key, metadata_key, "skipped", download.size_bytes, elapsed
            )

    with download.path.open("rb") as body:
        client.upload_fileobj(
            body,
            bucket,
            key,
            ExtraArgs={"Metadata": {"sha256": download.sha256}},
        )
    client.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )
    elapsed = (datetime.now(UTC) - started).total_seconds()
    LOGGER.info(
        "Bronze source landed",
        extra={
            "source_month": download.source_month,
            "component": "bronze_ingestion",
            "status": "uploaded",
            "size_bytes": download.size_bytes,
            "elapsed_seconds": round(elapsed, 3),
        },
    )
    return BronzeResult(
        download.source_month, key, metadata_key, "uploaded", download.size_bytes, elapsed
    )


def _existing_manifest(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as error:
        if _is_missing(error):
            return None
        raise
    body = response["Body"]
    try:
        return json.loads(body.read())
    finally:
        body.close()


def _same_source(existing: dict[str, Any], current: dict[str, Any]) -> bool:
    return all(
        existing.get(field) == current[field] for field in ("sha256", "size_bytes", "source_url")
    )


def _is_missing(error: Exception) -> bool:
    response = getattr(error, "response", {})
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    return code in {"404", "NoSuchKey", "NotFound"}
