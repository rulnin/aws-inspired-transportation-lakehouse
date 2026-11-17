"""Unit tests for Bronze object and manifest verification."""

from __future__ import annotations

import hashlib
import io
import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as parquet
import pytest

from lakehouse.ingestion import verification
from lakehouse.ingestion.bronze import manifest_key, object_key


class _FakeS3:
    def __init__(self, manifest: dict, parquet_bytes: bytes) -> None:
        self.objects = {
            manifest_key("2024-01"): json.dumps(manifest).encode(),
            object_key("2024-01"): parquet_bytes,
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        del Bucket
        return {"Body": io.BytesIO(self.objects[Key])}


def _bronze_fixture(tmp_path, monkeypatch, *, valid_digest: bool = True) -> _FakeS3:
    parquet_path = tmp_path / "yellow_tripdata_2024-01.parquet"
    parquet.write_table(pa.table({"trip_id": [1, 2, 3]}), parquet_path)
    content = parquet_path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    manifest = {
        "source_url": "https://example.test/yellow_tripdata_2024-01.parquet",
        "source_month": "2024-01",
        "retrieved_at": "2024-01-01T00:00:00+00:00",
        "pipeline_run_id": "test-run",
        "size_bytes": len(content),
        "sha256": digest if valid_digest else "incorrect",
    }
    fake_s3 = _FakeS3(manifest, content)
    monkeypatch.setattr(verification.boto3, "client", lambda *args, **kwargs: fake_s3)
    monkeypatch.setattr(
        verification,
        "load_settings",
        lambda: SimpleNamespace(source_months=("2024-01",)),
    )
    monkeypatch.setenv("MINIO_ROOT_USER", "test-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test-password")
    return fake_s3


def test_verify_months_checks_manifest_and_parquet_footer(tmp_path, monkeypatch):
    _bronze_fixture(tmp_path, monkeypatch)

    assert verification.verify_months() == {"2024-01": 3}


def test_verify_months_rejects_checksum_mismatch(tmp_path, monkeypatch):
    _bronze_fixture(tmp_path, monkeypatch, valid_digest=False)

    with pytest.raises(ValueError, match="SHA-256"):
        verification.verify_months()
