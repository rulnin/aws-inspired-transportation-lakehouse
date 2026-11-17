"""Unit coverage for source download and Bronze idempotency."""

from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError

import pyarrow as arrow
import pyarrow.parquet as parquet
import pytest

from lakehouse.ingestion.bronze import build_manifest, land_in_bronze, manifest_key, object_key
from lakehouse.ingestion.source import DownloadError, DownloadResult, download_month, source_url


def parquet_bytes(value: int = 1) -> bytes:
    buffer = io.BytesIO()
    parquet.write_table(arrow.table({"trip_id": [value]}), buffer)
    return buffer.getvalue()


class HttpResponse(io.BytesIO):
    status = 200

    def __init__(self, content: bytes, content_length: int | None = None):
        super().__init__(content)
        self.headers = {} if content_length is None else {"Content-Length": str(content_length)}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.uploads = 0

    def get_object(self, *, Bucket: str, Key: str):
        del Bucket
        if Key not in self.objects:
            raise FakeClientError("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, *, Bucket: str, Key: str):
        del Bucket
        if Key not in self.objects:
            raise FakeClientError("404")

    def upload_fileobj(self, body, bucket: str, key: str, ExtraArgs=None):
        del bucket, ExtraArgs
        self.objects[key] = body.read()
        self.uploads += 1

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str):
        del Bucket, ContentType
        self.objects[Key] = Body


class FakeClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def test_source_url_is_official_and_month_checked():
    assert source_url("2024-01") == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )
    with pytest.raises(ValueError):
        source_url("2024-13")


def test_download_streams_validates_and_hashes_fixture(tmp_path: Path):
    payload = parquet_bytes()
    destination = tmp_path / "nested" / "source.parquet"
    result = download_month(
        "2024-01",
        destination,
        opener=lambda _url, timeout: HttpResponse(payload, len(payload)),
        sleep=lambda _delay: pytest.fail("unexpected retry"),
    )
    assert destination.read_bytes() == payload
    assert result.size_bytes == len(payload)
    assert result.sha256 == hashlib.sha256(payload).hexdigest()
    assert result.source_url == source_url("2024-01")


def test_transient_http_failure_retries_with_exponential_backoff(tmp_path: Path):
    payload = parquet_bytes()
    calls = 0
    delays = []

    def opener(_url, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise HTTPError(_url, 503, "busy", {}, None)
        return HttpResponse(payload)

    result = download_month(
        "2024-01", tmp_path / "source.parquet", opener=opener, sleep=delays.append
    )
    assert result.size_bytes == len(payload)
    assert calls == 2
    assert delays == [1.0]


def test_incomplete_or_invalid_parquet_is_never_published(tmp_path: Path):
    destination = tmp_path / "source.parquet"
    with pytest.raises(DownloadError, match="Incomplete"):
        download_month(
            "2024-01",
            destination,
            opener=lambda _url, timeout: HttpResponse(parquet_bytes(), 999999),
            sleep=lambda _delay: None,
        )
    assert not destination.exists()
    assert not list(tmp_path.glob("*.part"))

    with pytest.raises(DownloadError, match="empty file"):
        download_month(
            "2024-01",
            destination,
            opener=lambda _url, timeout: HttpResponse(b""),
            sleep=lambda _delay: None,
        )
    assert not destination.exists()

    with pytest.raises(DownloadError, match="not readable Parquet"):
        download_month(
            "2024-01",
            destination,
            opener=lambda _url, timeout: HttpResponse(b"not parquet"),
            sleep=lambda _delay: None,
        )
    assert not destination.exists()


def test_truncated_response_retries_before_succeeding(tmp_path: Path):
    payload = parquet_bytes()
    calls = 0

    def opener(_url, timeout):
        nonlocal calls
        calls += 1
        return HttpResponse(payload, len(payload) + 1 if calls == 1 else len(payload))

    delays = []
    result = download_month(
        "2024-01", tmp_path / "source.parquet", opener=opener, sleep=delays.append
    )
    assert result.size_bytes == len(payload)
    assert calls == 2
    assert delays == [1.0]


def test_permanent_http_error_does_not_retry(tmp_path: Path):
    calls = 0

    def opener(url, timeout):
        nonlocal calls
        calls += 1
        raise HTTPError(url, 404, "missing", {}, None)

    with pytest.raises(DownloadError, match="HTTP 404"):
        download_month(
            "2024-01", tmp_path / "source.parquet", opener=opener, sleep=lambda _delay: None
        )
    assert calls == 1


def test_manifest_and_bronze_upload_are_idempotent(tmp_path: Path):
    payload = parquet_bytes()
    path = tmp_path / "source.parquet"
    path.write_bytes(payload)
    download = DownloadResult(
        "2024-01", source_url("2024-01"), path, len(payload), hashlib.sha256(payload).hexdigest()
    )
    timestamp = datetime(2024, 2, 1, tzinfo=UTC)
    manifest = build_manifest(download, pipeline_run_id="run-1", retrieved_at=timestamp)
    assert manifest["retrieved_at"] == timestamp.isoformat()
    assert manifest["pipeline_run_id"] == "run-1"

    client = FakeS3()
    first = land_in_bronze(client, "lakehouse", download, pipeline_run_id="run-1")
    second = land_in_bronze(client, "lakehouse", download, pipeline_run_id="run-2")
    assert first.status == "uploaded"
    assert second.status == "skipped"
    assert client.uploads == 1
    assert client.objects[object_key("2024-01")] == payload
    assert json.loads(client.objects[manifest_key("2024-01")])["pipeline_run_id"] == "run-1"


def test_changed_source_replaces_month_object_and_manifest(tmp_path: Path):
    client = FakeS3()
    for value, run_id in ((1, "run-1"), (2, "run-2")):
        payload = parquet_bytes(value)
        path = tmp_path / f"{value}.parquet"
        path.write_bytes(payload)
        item = DownloadResult(
            "2024-01",
            source_url("2024-01"),
            path,
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        )
        result = land_in_bronze(client, "lakehouse", item, pipeline_run_id=run_id)
        assert result.status == "uploaded"
    assert client.uploads == 2
    assert client.objects[object_key("2024-01")] == parquet_bytes(2)
    assert json.loads(client.objects[manifest_key("2024-01")])["pipeline_run_id"] == "run-2"
