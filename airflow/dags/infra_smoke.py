"""Minimal DAG used to verify the local Airflow executor and UI."""

from datetime import UTC, datetime

from airflow.sdk import dag, task


@dag(
    dag_id="infra_smoke",
    description="Checks that the local Airflow scheduler can run a task.",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["infrastructure", "smoke-test"],
)
def infra_smoke() -> None:
    @task
    def confirm_local_executor() -> None:
        """Emit a useful marker in task logs for the infrastructure smoke run."""
        print("Airflow infrastructure smoke task completed")

    confirm_local_executor()


infra_smoke()
