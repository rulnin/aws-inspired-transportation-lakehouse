"""Orchestrate an idempotent TLC Bronze-to-Gold batch in local Compose."""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.observability import stats

LOGGER = logging.getLogger(__name__)
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
PROJECT_ROOT = Path("/opt/airflow/project")
DEFAULT_MONTHS = [
    month.strip()
    for month in os.environ.get("LAKEHOUSE_SOURCE_MONTHS", "2024-01,2024-02").split(",")
]
SPARK_IMAGE = os.environ["LAKEHOUSE_SPARK_IMAGE"]
WORKSPACE_SOURCE = os.environ["LAKEHOUSE_WORKSPACE"]
SPARK_IVY_VOLUME = os.environ["LAKEHOUSE_SPARK_IVY_VOLUME"]
DOCKER_NETWORK = os.environ["LAKEHOUSE_DOCKER_NETWORK"]


def _months_from_context() -> list[str]:
    months = get_current_context()["params"]["source_months"]
    if not months or any(not MONTH_PATTERN.fullmatch(month) for month in months):
        raise ValueError("source_months must contain valid YYYY-MM values")
    if len(months) != len(set(months)):
        raise ValueError("source_months must not contain duplicates")
    return months


def _log_task_failure(context: dict) -> None:
    task_instance = context.get("ti")
    error = context.get("exception")
    LOGGER.error(
        "Lakehouse task failed",
        exc_info=(type(error), error, error.__traceback__) if error else None,
        extra={
            "component": "airflow_orchestration",
            "status": "failed",
            "pipeline_run_id": context.get("run_id"),
            "source_months": context.get("params", {}).get("source_months"),
            "task_id": getattr(task_instance, "task_id", None),
            "dag_id": getattr(task_instance, "dag_id", None),
        },
    )


def _run_script(script_name: str, *arguments: str) -> None:
    context = get_current_context()
    months = _months_from_context()
    environment = os.environ.copy()
    environment.update(
        {
            "LAKEHOUSE_SOURCE_MONTHS": ",".join(months),
            "PIPELINE_RUN_ID": context["run_id"],
            "PYTHONPATH": f"{PROJECT_ROOT / 'src'}:{PROJECT_ROOT / 'scripts'}",
        }
    )
    command = [sys.executable, str(PROJECT_ROOT / "scripts" / script_name), *arguments]
    LOGGER.info(
        "Starting orchestration stage",
        extra={
            "component": "airflow_orchestration",
            "status": "started",
            "pipeline_run_id": context["run_id"],
            "source_months": months,
            "script": script_name,
        },
    )
    subprocess.run(command, check=True, env=environment)


def _spark_operator(task_id: str, script_name: str, run_id_argument: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        entrypoint=["/bin/bash", "/workspace/docker/spark/entrypoint.sh"],
        command=[f"/workspace/scripts/{script_name}", run_id_argument, "{{ run_id }}"],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=[
            {
                "source": WORKSPACE_SOURCE,
                "target": "/workspace",
                "type": "bind",
                "read_only": True,
            },
            {
                "source": str(Path(WORKSPACE_SOURCE) / "docker/spark/ivysettings.xml"),
                "target": "/opt/lakehouse/ivysettings.xml",
                "type": "bind",
                "read_only": True,
            },
            {
                "source": SPARK_IVY_VOLUME,
                "target": "/tmp/.ivy2",
                "type": "volume",
            },
        ],
        environment={
            "LAKEHOUSE_SOURCE_MONTHS": "{{ params.source_months | join(',') }}",
            "LAKEHOUSE_LOG_LEVEL": os.environ.get("LAKEHOUSE_LOG_LEVEL", "INFO"),
            "MINIO_BUCKET": os.environ.get("MINIO_BUCKET", "lakehouse"),
            "LAKEHOUSE_STATSD_HOST": "statsd-exporter",
            "LAKEHOUSE_STATSD_PORT": "9125",
            "PIPELINE_RUN_ID": "{{ run_id }}",
            "PYTHONPATH": "/workspace/src",
        },
        private_environment={
            "MINIO_ROOT_USER": os.environ["MINIO_ROOT_USER"],
            "MINIO_ROOT_PASSWORD": os.environ["MINIO_ROOT_PASSWORD"],
        },
        mem_limit="3g",
        cpus=2,
        mount_tmp_dir=False,
        auto_remove="force",
        execution_timeout=timedelta(minutes=30),
        retries=2,
        retry_delay=timedelta(seconds=30),
        on_failure_callback=_log_task_failure,
        labels={"app": "transportation-lakehouse", "task": task_id},
    )


@dag(
    dag_id="yellow_taxi_bronze_to_gold",
    description="Validate and process selected official TLC Yellow Taxi months.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": _log_task_failure,
    },
    params={
        "source_months": Param(
            DEFAULT_MONTHS,
            type="array",
            minItems=1,
            maxItems=12,
            items={"type": "string", "pattern": r"^\d{4}-(0[1-9]|1[0-2])$"},
            description="TLC source months to ingest and process.",
        ),
        "failure_mode": Param(
            "none",
            type="string",
            enum=["none", "retry_once", "always"],
            description="Controlled retry probe for local orchestration verification.",
        ),
    },
    tags=["transportation-lakehouse", "batch", "tlc"],
)
def yellow_taxi_bronze_to_gold():
    @task(retries=1, retry_delay=timedelta(seconds=5), on_failure_callback=_log_task_failure)
    def retry_probe() -> None:
        context = get_current_context()
        mode = context["params"]["failure_mode"]
        attempt = context["ti"].try_number
        LOGGER.info(
            "Retry probe attempt",
            extra={
                "component": "airflow_orchestration",
                "status": "started",
                "pipeline_run_id": context["run_id"],
                "attempt": attempt,
                "failure_mode": mode,
            },
        )
        if mode == "always" or (mode == "retry_once" and attempt == 1):
            raise RuntimeError(f"Controlled Airflow retry probe failure on attempt {attempt}")

    @task(on_failure_callback=_log_task_failure)
    def validate_official_sources() -> None:
        context = get_current_context()
        months = _months_from_context()
        for month in months:
            year, month_number = month.split("-")
            url = (
                "https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"yellow_tripdata_{year}-{month_number}.parquet"
            )
            request = Request(url, method="HEAD")
            with urlopen(request, timeout=30) as response:
                if not 200 <= response.status < 300:
                    raise RuntimeError(
                        f"TLC source check failed for {month}: HTTP {response.status}"
                    )
                content_length = response.headers.get("Content-Length")
                if content_length is not None and int(content_length) <= 0:
                    raise RuntimeError(f"TLC source check found an empty file for {month}")
            LOGGER.info(
                "Official TLC source validated",
                extra={
                    "component": "source_validation",
                    "status": "success",
                    "pipeline_run_id": context["run_id"],
                    "source_month": month,
                    "size_bytes": int(content_length) if content_length else None,
                },
            )

    @task(on_failure_callback=_log_task_failure)
    def ingest_bronze() -> None:
        run_id = get_current_context()["run_id"]
        _run_script("ingest_bronze.py", "--run-id", run_id)

    @task(on_failure_callback=_log_task_failure)
    def verify_bronze() -> None:
        _run_script("verify_bronze.py")

    @task(on_failure_callback=_log_task_failure)
    def check_silver_quality() -> None:
        _run_script("check_silver_quality.py")

    @task(on_failure_callback=_log_task_failure)
    def verify_gold() -> None:
        _run_script("trino_gold_smoke.py")
        stats.gauge("pipeline.last_success_timestamp_seconds", time.time())

    probe = retry_probe()
    sources = validate_official_sources()
    bronze = ingest_bronze()
    bronze_check = verify_bronze()
    silver = _spark_operator("process_silver", "process_silver.py", "--processing-run-id")
    quality = check_silver_quality()
    gold = _spark_operator("process_gold", "process_gold.py", "--pipeline-run-id")
    final_check = verify_gold()

    probe >> sources >> bronze >> bronze_check >> silver >> quality >> gold >> final_check


yellow_taxi_bronze_to_gold()
