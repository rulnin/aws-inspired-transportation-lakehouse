"""Streaming access to the official TLC Yellow Taxi monthly files."""

from __future__ import annotations

import hashlib
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import pyarrow.parquet as parquet
from pyarrow import ArrowException

LOGGER = logging.getLogger(__name__)
_MONTH_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
_SOURCE_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


class DownloadError(RuntimeError):
    """Raised when an official source file cannot be safely downloaded."""


class IncompleteDownloadError(DownloadError):
    """Raised for a truncated response that is safe to retry."""


@dataclass(frozen=True, slots=True)
class DownloadResult:
    """Verified local source file metadata."""

    source_month: str
    source_url: str
    path: Path
    size_bytes: int
    sha256: str
    elapsed_seconds: float = 0.0


def source_url(source_month: str) -> str:
    """Build the official TLC URL for a selected YYYY-MM month."""
    match = _MONTH_PATTERN.fullmatch(source_month)
    if not match:
        raise ValueError(f"Invalid source month {source_month!r}; expected YYYY-MM")
    year, month = match.groups()
    return f"{_SOURCE_BASE_URL}/yellow_tripdata_{year}-{month}.parquet"


def download_month(
    source_month: str,
    destination: Path,
    *,
    attempts: int = 4,
    backoff_seconds: float = 1.0,
    opener: Callable[..., object] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
) -> DownloadResult:
    """Stream, checksum, and validate one source file with bounded retries."""
    url = source_url(source_month)
    if attempts < 1 or backoff_seconds < 0:
        raise ValueError("attempts must be positive and backoff_seconds non-negative")

    started = time.monotonic()
    for attempt in range(attempts):
        try:
            result = replace(
                _download_once(source_month, url, destination, opener),
                elapsed_seconds=time.monotonic() - started,
            )
            LOGGER.info(
                "TLC source downloaded",
                extra={
                    "source_month": source_month,
                    "component": "bronze_ingestion",
                    "status": "downloaded",
                    "size_bytes": result.size_bytes,
                    "elapsed_seconds": round(result.elapsed_seconds, 3),
                },
            )
            return result
        except IncompleteDownloadError as error:
            retryable = True
            message = str(error)
        except HTTPError as error:
            retryable = error.code in _RETRYABLE_STATUS
            message = f"TLC returned HTTP {error.code} for {source_month}"
        except (URLError, TimeoutError, OSError) as error:
            retryable = True
            message = f"Network error downloading {source_month}: {error}"

        if not retryable or attempt + 1 == attempts:
            raise DownloadError(message) from None
        delay = backoff_seconds * (2**attempt)
        LOGGER.warning(
            "TLC download failed; retrying",
            extra={
                "source_month": source_month,
                "component": "bronze_ingestion",
                "status": "retry",
            },
        )
        sleep(delay)
    raise AssertionError("unreachable")


def _download_once(
    source_month: str,
    url: str,
    destination: Path,
    opener: Callable[..., object],
) -> DownloadResult:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.part")
    digest = hashlib.sha256()
    size_bytes = 0
    expected_size: int | None = None
    try:
        with opener(url, timeout=60) as response, temporary.open("wb") as target:  # type: ignore[attr-defined]
            status = getattr(response, "status", 200)
            if status < 200 or status >= 300:
                raise DownloadError(f"TLC returned HTTP {status} for {source_month}")
            headers = getattr(response, "headers", {})
            expected_size_value = headers.get("Content-Length")
            expected_size = int(expected_size_value) if expected_size_value else None
            while block := response.read(1024 * 1024):
                target.write(block)
                digest.update(block)
                size_bytes += len(block)
        if size_bytes == 0:
            raise DownloadError(f"TLC returned an empty file for {source_month}")
        if expected_size is not None and size_bytes != expected_size:
            raise IncompleteDownloadError(
                f"Incomplete TLC download for {source_month}: expected {expected_size} bytes, "
                f"received {size_bytes}"
            )
        try:
            parquet.ParquetFile(temporary)
        except (ArrowException, OSError, ValueError) as error:
            raise DownloadError(
                f"Downloaded file for {source_month} is not readable Parquet"
            ) from error
        temporary.replace(destination)
        return DownloadResult(source_month, url, destination, size_bytes, digest.hexdigest())
    finally:
        temporary.unlink(missing_ok=True)
