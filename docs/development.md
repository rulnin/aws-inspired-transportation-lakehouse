# Developer guide

## Read the implementation

The Python package contains the data logic. Scripts adapt that logic into runnable jobs;
Airflow connects the jobs and handles orchestration. Spark and Airflow dependencies live
in their container runtimes, so the host environment does not need either installed.

Follow one source month through these files:

| Step | Entry point | Implementation |
|---|---|---|
| Download and land Bronze | `scripts/ingest_bronze.py` | `src/lakehouse/ingestion/source.py` handles downloads and retries; `bronze.py` handles object keys, checksums, and manifests |
| Verify landed data | `scripts/verify_bronze.py` | `src/lakehouse/ingestion/verification.py` verifies objects against manifests and reads Parquet metadata |
| Normalize and classify Silver | `scripts/process_silver.py` | `src/lakehouse/processing/schema.py` defines the contract; `silver.py` classifies and writes valid and quarantined rows |
| Check Silver quality | `scripts/check_silver_quality.py` | Reconciles Bronze, Silver, and quarantine counts through Trino |
| Load reference data | `scripts/ingest_taxi_zones.py` | Downloads, validates, and lands the zone lookup used by Gold |
| Aggregate Gold | `scripts/process_gold.py` | `src/lakehouse/processing/gold.py` defines aggregations, reconciliation, and month replacement |
| Verify query results | `scripts/trino_gold_smoke.py` | Queries Gold and reconciles it with Silver |
| Orchestrate stages | `airflow/dags/yellow_taxi_pipeline.py` | Defines stage order, parameters, retries, and Spark container execution |

Shared settings are in `src/lakehouse/config/settings.py`. JSON logging and the best-effort
StatsD emitter are in `src/lakehouse/common/`. Example analytical SQL lives in `sql/trino/`.

Bronze preserves source bytes, but a changed upstream file replaces the canonical monthly
object and manifest. Silver and Gold replace selected month partitions. Iceberg commits
are per table; the pipeline does not provide a transaction spanning all tables. See the
[data quality guide](data-quality.md) and [runbook](operations-runbook.md) before changing
rerun or recovery behavior.

## Set up and check a change

From the repository root, install the locked Python environment:

```bash
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run pytest
uv run python -m compileall -q airflow/dags
```

These checks run without Docker or `.env`. Default pytest discovery covers `tests/unit/`.
DAG compilation checks syntax; importing and executing a DAG requires the Airflow runtime.
GitHub Actions runs these same checks. Use `uv run ruff format .` to apply formatting.

For changes to transformations or service integration, prepare `.env` as described in the
[quick start](../README.md#quick-start), then run the relevant service tests:

```bash
bash scripts/run_service_tests.sh integration
bash scripts/run_service_tests.sh e2e
```

The equivalent Make targets are `make integration-test` and `make e2e-test`.
The runner starts services in the current Compose project and stops them on exit, preserving
named volumes. Stop an active demo before running it; this is not a separate test deployment.

| Test scope | What it verifies |
|---|---|
| `tests/unit/` | Settings, download/retry behavior, Bronze manifests and verification, schema resolution, quality checks, logging, and metrics |
| `tests/integration/` | MinIO round trip, Spark/Iceberg/Trino interoperability, Airflow DAG imports and stage presence, and Silver quality fixtures |
| `tests/e2e/` | Four fixture rows through Bronze, Silver, quarantine, Gold, and Trino; two processing passes verify stable results |

Additional focused Spark fixtures are available with `make silver-fixture` and
`make gold-fixture`. For a full official-data orchestration demo, use
`bash scripts/airflow_smoke.sh` after starting the Airflow stack. That script also exercises
reprocessing and controlled retry/failure behavior; it takes longer than the fixture tests.

## Keep changes understandable

Keep reusable data logic in `src/lakehouse/`, command-line setup in `scripts/`, and task
coordination in `airflow/dags/`. Update the data dictionary, quality rules, or metric
definitions when their contracts change. Document architecture tradeoffs in `docs/decisions/`.

Commit `.env.example` when configuration requirements change. Keep `.env`, credentials,
raw data, service volumes, and generated logs out of Git. Review `git diff` and run the
checks appropriate to the changed behavior before submitting a pull request.
