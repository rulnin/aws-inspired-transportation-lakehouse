# Operations runbook

## Start and stop

Prepare the ignored local `.env` file from `.env.example`, then start the data services,
Spark dependency cache, and Airflow:

```bash
cp .env.example .env
make infra-airflow-up
```

The Airflow UI is at `http://127.0.0.1:8081`. Use the local login configured in `.env`.
The Airflow scheduler launches short lived Spark containers through the Docker provider.
This local-only setup mounts the Docker socket into the scheduler, which grants access to
the local Docker daemon. Keep Airflow bound to loopback and do not expose its socket or UI.

Stop the stack without deleting data with `make infra-down`. `make infra-clean` removes
named volumes as well, including Bronze, Iceberg, Airflow metadata, logs, and Spark caches.

## Run and backfill selected months

The `yellow_taxi_bronze_to_gold` DAG has no automatic schedule. Trigger it from the UI or
CLI and provide `source_months` as an array of `YYYY-MM` strings. Months default to
`LAKEHOUSE_SOURCE_MONTHS` in `.env`:

```bash
docker compose --profile core --profile spark --profile airflow exec airflow-scheduler \
  airflow dags trigger \
  --conf '{"source_months":["2024-01","2024-02"],"failure_mode":"none"}' \
  yellow_taxi_bronze_to_gold
```

To backfill or reprocess a month, trigger the same DAG with that month in `source_months`.
Bronze skips an identical object after download/checksum validation. Silver replaces the
month in valid and quarantine tables, and Gold replaces its month partitions. A completed
month can therefore be selected again without multiplying rows.

The stages run in order: retry probe, official source validation, Bronze ingestion, Bronze
checksum/Parquet verification, Silver processing, Silver/quarantine reconciliation and
quality checks, Gold processing, and Trino verification. Each task log includes the Airflow
run ID and the selected source month(s). Spark task output is streamed into the associated
Airflow task log.

## Failure handling

Tasks retry twice with a 30-second delay and a 30-minute execution timeout; the retry probe
uses a shorter delay to make retry behavior easy to verify. Failures after retries invoke a
callback that records DAG, task, run ID, and month context. Inspect the failed task's Airflow
log, correct the source/service/configuration issue, then rerun the same selected months.

For a local retry test, set `failure_mode` to `retry_once`. The probe fails on its first
attempt and succeeds on its retry before the pipeline continues. To verify terminal failure
visibility without starting data work, set `failure_mode` to `always`; the probe exhausts
its retry and the DAG run ends failed. Normal runs should use `failure_mode: "none"`.

Useful checks:

```bash
docker compose --profile core --profile spark --profile airflow ps
docker compose --profile core --profile spark --profile airflow logs --tail=100 airflow-scheduler
docker compose --profile core --profile spark --profile airflow logs --tail=100 minio trino hive-metastore
```

The [local infrastructure guide](local-infrastructure.md) covers service health and resource
limits. Re-run `bash scripts/airflow_smoke.sh` to validate DAG import, end-to-end processing,
month reprocessing, retry behavior, and visible failure behavior.

## Metrics and dashboards

Start the full stack with `make infra-monitoring-up`. Open Grafana at
`http://127.0.0.1:3001`, sign in with the `.env` credentials, and select the provisioned
**Transportation Lakehouse Overview** dashboard. Prometheus is available at
`http://127.0.0.1:9090`; its **Status → Targets** page shows scrape health. The dashboard
service panel probes MinIO, Trino, Airflow, Prometheus, Grafana, both PostgreSQL instances,
and Hive Metastore. A failed probe is `0`; a successful probe is `1`.

Prometheus scrapes MinIO's v3 endpoint with public metrics enabled for the isolated Compose
network. Airflow 3.4 emits its supported StatsD metrics to the StatsD exporter. Silver and
Gold Spark task containers send row-count gauges through the same exporter. The UDP emitter
is best-effort: metrics delivery errors are logged as warnings and do not fail processing.

Metric definitions:

- `airflow_ti_successes` and `airflow_ti_failures`: cumulative Airflow task
  instance outcomes; the dashboard shows their one-hour increase.
- `airflow_dagrun_duration_success_sum` / `_count`: Airflow successful DAG duration samples
  in seconds; the dashboard displays their mean.
- `airflow_pipeline_last_success_timestamp_seconds`: Unix timestamp set after the final
  Trino verification task succeeds.
- `lakehouse_silver_input_rows`: all source records classified for a month.
- `lakehouse_silver_valid_rows`: rows written to Silver after quality classification.
- `lakehouse_silver_rejected_rows`: rows written to quarantine.
- `lakehouse_gold_output_rows`: total output rows written across monthly, hourly, and pickup
  zone Gold tables for the month.
- `minio_cluster_usage_buckets_total_bytes`: MinIO's asynchronous bucket usage scan, in bytes.
- `probe_success`: current HTTP or TCP readiness probe outcome.

Row gauges are labeled by `source_month`; they represent the latest observed run for each
month, not a run-history table. Airflow's StatsD metrics do not include pipeline run ID or
source month labels; use the structured Airflow and Spark task logs to correlate a specific
run. MinIO bucket usage is updated by MinIO's scanner and can lag recent writes. The local
single-node MinIO metrics profile does not represent distributed cluster capacity.

## Restart and pipeline recovery

To verify/recover after a MinIO service interruption while preserving data:

```bash
docker compose --profile core --profile spark --profile airflow --profile monitoring restart minio
docker compose --profile core --profile spark --profile airflow --profile monitoring ps
bash scripts/airflow_smoke.sh
```

Wait for MinIO's Compose health status to become `healthy` before rerunning a DAG. The smoke
script validates Airflow imports, processes configured month(s), reprocesses the first month,
and checks retry and terminal failure visibility. Review the dashboard's MinIO/service probes
and the DAG task logs after the run. For an actual pipeline failure, inspect the failed task
log, resolve the underlying service or data error, then trigger the same `source_months` again;
Bronze, Silver, and Gold month writes are idempotent. These checks preserve named volumes;
do not use `docker compose down -v` during restart recovery.
