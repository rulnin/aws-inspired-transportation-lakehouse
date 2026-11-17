"""Unit tests for the Silver/quarantine reconciliation gate."""

import pytest

from lakehouse.ingestion.verification import assert_silver_reconciles


def test_silver_and_quarantine_cover_every_bronze_row() -> None:
    assert_silver_reconciles("2024-01", source_rows=10, silver_rows=8, rejected_rows=2)


def test_reconciliation_failure_reports_month_and_counts() -> None:
    with pytest.raises(ValueError, match=r"2024-01: 8 \+ 1 != 10"):
        assert_silver_reconciles("2024-01", source_rows=10, silver_rows=8, rejected_rows=1)
