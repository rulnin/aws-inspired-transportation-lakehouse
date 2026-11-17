"""Best-effort StatsD gauges for pipeline row counts."""

from __future__ import annotations

import logging
import os
import re
import socket

LOGGER = logging.getLogger(__name__)
_SAFE_VALUE = re.compile(r"[^a-zA-Z0-9_-]")


def send_gauge(name: str, value: int | float, *, source_month: str) -> None:
    """Send a pipeline gauge when a StatsD endpoint is configured.

    Metrics are supplementary: a local exporter outage must not fail data processing.
    """
    host = os.environ.get("LAKEHOUSE_STATSD_HOST")
    if not host:
        return
    port = int(os.environ.get("LAKEHOUSE_STATSD_PORT", "9125"))
    safe_name = _SAFE_VALUE.sub("_", name)
    safe_month = _SAFE_VALUE.sub("_", source_month)
    payload = f"lakehouse.{safe_name}:{value}|g|#source_month:{safe_month}".encode()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
            client.sendto(payload, (host, port))
    except OSError as error:
        LOGGER.warning("Unable to emit pipeline metric %s: %s", name, error)
