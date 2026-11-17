"""Verify Trino can read the Iceberg table created by the Spark smoke test."""

from __future__ import annotations

import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _read_response(url: str, *, method: str, body: bytes | None = None) -> dict:
    request = Request(
        url,
        data=body,
        method=method,
        headers={"X-Trino-User": "infra-smoke", "Content-Type": "text/plain; charset=utf-8"},
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def main() -> None:
    endpoint = os.environ.get("TRINO_ENDPOINT_URL", "http://127.0.0.1:8080")
    statement = "SELECT payload, expected_value FROM iceberg.infra_smoke.spark_trino_check"
    ready_deadline = time.monotonic() + 120
    while True:
        try:
            _read_response(f"{endpoint}/v1/info", method="GET")
            break
        except (HTTPError, URLError):
            if time.monotonic() >= ready_deadline:
                raise TimeoutError("Trino did not become ready within 120 seconds") from None
            time.sleep(3)

    result = _read_response(
        f"{endpoint}/v1/statement", method="POST", body=statement.encode("utf-8")
    )
    rows = list(result.get("data", []))
    deadline = time.monotonic() + 60
    while result.get("nextUri"):
        if time.monotonic() >= deadline:
            raise TimeoutError("Trino query did not finish within 60 seconds")
        result = _read_response(result["nextUri"], method="GET")
        if "error" in result:
            raise RuntimeError(f"Trino query failed: {result['error']}")
        rows.extend(result.get("data", []))

    if rows != [["spark-to-trino-ok", 42]]:
        raise RuntimeError(f"Trino returned unexpected rows: {rows!r}")
    print("Trino read the Iceberg row written by Spark")


if __name__ == "__main__":
    try:
        main()
    except (HTTPError, URLError) as exc:
        raise SystemExit(f"Cannot query Trino; is the core profile healthy? {exc}") from exc
