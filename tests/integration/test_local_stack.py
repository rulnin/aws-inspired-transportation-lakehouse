"""Opt-in interoperability checks against the local Compose stack."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.integration


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, check=True, text=True, capture_output=True)


def test_minio_put_get_round_trip() -> None:
    result = _run([sys.executable, "scripts/minio_smoke.py"])
    assert "object round trip passed" in result.stdout


def test_spark_iceberg_table_is_read_by_trino() -> None:
    _run(
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
            "/workspace/scripts/iceberg_interop_smoke.py",
        ]
    )
    result = _run([sys.executable, "scripts/trino_smoke.py"])
    assert "Trino read the Iceberg row" in result.stdout


def test_airflow_dag_imports_and_exposes_expected_stages() -> None:
    compose = [
        "docker",
        "compose",
        "--env-file",
        ".env",
        "--profile",
        "core",
        "--profile",
        "airflow",
        "exec",
        "-T",
        "airflow-scheduler",
    ]
    errors = _run([*compose, "airflow", "dags", "list-import-errors", "--output", "json"])
    last_line = errors.stdout.strip().splitlines()[-1]
    assert json.loads(last_line) == []

    tasks = _run([*compose, "airflow", "tasks", "list", "yellow_taxi_bronze_to_gold"])
    output = "\n".join([tasks.stdout, tasks.stderr])
    for task_id in ("verify_bronze", "process_silver", "check_silver_quality", "process_gold"):
        assert task_id in output


def test_spark_silver_quality_fixture() -> None:
    _run(
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
            "/workspace/scripts/silver_fixture_smoke.py",
        ]
    )
