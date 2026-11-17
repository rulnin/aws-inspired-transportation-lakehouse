"""Tests for optional pipeline metrics emission."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakehouse.common.metrics import send_gauge


def test_gauge_emits_a_tagged_statsd_datagram(monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_STATSD_HOST", "statsd-exporter")
    monkeypatch.setenv("LAKEHOUSE_STATSD_PORT", "9125")
    client = MagicMock()
    client.__enter__.return_value = client
    with patch("lakehouse.common.metrics.socket.socket", return_value=client):
        send_gauge("silver_valid_rows", 42, source_month="2024-01")

    client.sendto.assert_called_once_with(
        b"lakehouse.silver_valid_rows:42|g|#source_month:2024-01",
        ("statsd-exporter", 9125),
    )


def test_gauge_is_a_noop_when_metrics_are_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("LAKEHOUSE_STATSD_HOST", raising=False)
    with patch("lakehouse.common.metrics.socket.socket") as socket_factory:
        send_gauge("silver_valid_rows", 42, source_month="2024-01")

    socket_factory.assert_not_called()


def test_exporter_unavailable_does_not_fail_pipeline(monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_STATSD_HOST", "statsd-exporter")
    with patch("lakehouse.common.metrics.socket.socket", side_effect=OSError("offline")):
        send_gauge("silver_valid_rows", 42, source_month="2024-01")
