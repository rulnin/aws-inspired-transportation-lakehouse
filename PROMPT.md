# Master Prompt — Build the AWS-Inspired Transportation Data Lakehouse

---

## Role

Act as a senior Data Engineer and pragmatic platform engineer. Help build a production-style **local AWS-inspired Transportation Data Lakehouse** using the NYC TLC Yellow Taxi public dataset.

You are responsible for architecture, implementation, tests, documentation, and operational quality. Work incrementally. Do not generate a large unverified codebase in one pass.

## Project goal

Build a reproducible local data platform with:

- MinIO for S3-compatible object storage.
- Apache Spark in local mode for data processing.
- Apache Iceberg for Silver and Gold lakehouse tables.
- Hive Metastore backed by PostgreSQL as the shared catalog.
- Trino for SQL analytics.
- Apache Airflow for orchestration.
- Prometheus and Grafana for monitoring.
- Python, `uv`, pytest, Ruff, Docker Compose, and GitHub Actions.

The platform ingests official NYC TLC Yellow Taxi Parquet files, stores immutable source files in Bronze, creates cleaned Silver Iceberg tables, produces analytics-ready Gold tables, and exposes them through Trino.

**This is a local AWS-inspired implementation. Do not create AWS resources, incur cloud charges, or describe this as actual AWS experience.** The AWS service mapping is conceptual:

- MinIO ↔ Amazon S3
- Spark ↔ AWS Glue ETL / Amazon EMR processing
- Hive Metastore ↔ AWS Glue Data Catalog concept
- Trino ↔ Amazon Athena concept
- Airflow ↔ Amazon MWAA concept
- Prometheus/Grafana ↔ monitoring concept similar to CloudWatch

These are not exact service equivalents.

## Dataset

Official source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Use Yellow Taxi monthly Parquet files from the official TLC source. Start with two configurable consecutive months (January and February 2024 may be used as an initial example only after verifying the files exist and inspecting their schemas). Add the official Taxi Zone Lookup CSV only if it is useful and its source is verified.

Rules:

1. Never assume the Parquet schema. Inspect actual files first.
2. Support optional columns and schema differences between months.
3. Do not download the entire TLC history.
4. Do not commit source Parquet files or other large data files to Git.
5. Preserve raw source files unchanged in Bronze.
6. Include source attribution and link to the official TLC documentation.
7. Do not infer personal identities or attempt re-identification.

## Development environment and constraints

The expected environment is Windows with WSL2 Ubuntu, VS Code, Docker Desktop with WSL integration, Git, Python, and `uv`.

Before implementation:

- Inspect the current working directory and existing files.
- Check Docker/Compose, Python, `uv`, available CPU, RAM, and disk.
- Do not assume a drive letter, username, home directory, or absolute machine path.
- Keep all project work inside the opened repository.
- Use Docker Compose for infrastructure; do not require users to install PostgreSQL, MinIO, Spark, Trino, Hive Metastore, or Airflow directly on Windows.
- Use a Python virtual environment managed by `uv`.
- Keep Spark in local mode and use conservative resource settings.
- Provide Compose profiles or staged startup commands so all services do not have to run simultaneously.
- Avoid unnecessarily heavy services and dependencies.
- Never alter another repository, including the user's existing E-Commerce Data Platform project.

## Required working method

### Work phase by phase

Use `PROJECT_PLAN.md` as the source of truth.

1. Begin with Phase 0 only.
2. Inspect the environment and verify the data source/schema.
3. Create or update the project files for the current phase only.
4. Run relevant checks and tests.
5. Update the checkboxes in `PROJECT_PLAN.md` only after the task has actually passed.
6. Update `docs/PROGRESS.md` with:
   - Current phase.
   - Completed work.
   - Commands executed.
   - Test results.
   - Known issues.
   - Next tasks.
7. Summarize changed files and test evidence.
8. Stop at the end of the phase and ask before starting the next phase.

Do not mark a task complete merely because code was written. It is complete only when the stated acceptance criteria are verified.

### Before adding dependencies or services

- Explain why the dependency/service is needed.
- Check whether an existing dependency can perform the task.
- Verify compatibility across Spark, Iceberg, Hadoop AWS/S3A, Hive Metastore, Trino, and Java.
- Pin compatible versions after a working integration test.
- Do not independently upgrade one component if it breaks the tested stack.
- Prefer official documentation and supported container images.

### Coding standards

- Use clear, modular Python packages.
- Add type hints to public functions.
- Use structured logging with useful context.
- Separate configuration from code.
- Validate configuration at startup.
- Use environment variables for credentials.
- Include `.env.example`; never commit `.env`.
- Add meaningful error handling and actionable error messages.
- Avoid broad exception swallowing.
- Avoid hard-coded local paths and secrets.
- Avoid premature abstractions and unnecessary frameworks.
- Keep functions testable and deterministic where possible.
- Use Ruff for formatting/linting and pytest for tests.
- Do not add fake metrics, fake test results, or fabricated performance claims.

## Architecture requirements

### Bronze

- Download official monthly Yellow Taxi Parquet files.
- Validate response status, file size, checksum, and Parquet readability.
- Upload unchanged objects to MinIO.
- Use partition-like object key conventions such as:
  `raw/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet`
- Record a manifest with source URL, month, ingestion timestamp, file size, SHA-256, and pipeline run ID.
- Make ingestion idempotent.
- Incomplete downloads must not be treated as successful.

### Silver

- Read Bronze with Spark.
- Normalize source column names and types to a documented canonical schema.
- Handle optional/renamed columns explicitly.
- Add lineage columns such as source year/month, ingestion timestamp, and pipeline run ID.
- Apply documented quality checks.
- Write valid records to an Iceberg Silver table.
- Write invalid records and reason codes to a quarantine path/table.
- Make month-level reprocessing idempotent.
- Do not silently drop records or silently coerce invalid values.

Initial quality rules to evaluate and document:

- Pickup/dropoff timestamps are parseable and non-null.
- Dropoff is not earlier than pickup.
- Trip distance is non-negative.
- Passenger count is non-negative when present.
- Location IDs follow the documented expected domain where applicable.
- Monetary fields are parseable.
- Negative or unusual financial values must be handled through explicit domain rules, not arbitrary blanket deletion.

### Gold

Create documented analytics tables, at minimum:

1. `gold.monthly_trip_summary`
   - month, trip_count, total_revenue, average_trip_distance, average_fare, average_tip
2. `gold.hourly_trip_demand`
   - pickup_date, pickup_hour, pickup_location_id, trip_count, average_trip_distance

Optional:

3. `gold.pickup_zone_summary`
   - pickup_location_id, pickup_zone, borough, trip_count, total_revenue

Document table grain, metric formulas, filters, null handling, and limitations. Add reconciliation checks between Silver and Gold.

### Catalog and query

- Use Hive Metastore backed by PostgreSQL as the initial shared catalog.
- Configure Spark and Trino to use the same catalog.
- Store Iceberg data files in MinIO.
- Prove interoperability with a small integration test: Spark writes an Iceberg table and Trino reads it.
- Do not add Project Nessie until the baseline catalog and end-to-end flow are stable.

### Orchestration

Airflow DAG should have understandable task boundaries:

1. Validate source/configuration.
2. Ingest source month(s) to Bronze.
3. Verify Bronze objects/manifests.
4. Process Bronze to Silver.
5. Run Silver quality checks.
6. Build Gold tables.
7. Run final reconciliation/verification.

Configure retries, retry delays, timeouts, failure callbacks, and run IDs. Retrying must be safe because the underlying tasks are idempotent. Do not put secrets in DAG source code.

### Monitoring

Use Prometheus and Grafana for real, useful signals. Start small:

- Service health.
- Airflow DAG/task success and failure.
- Task/pipeline duration.
- Input, valid, rejected, and output row counts.
- MinIO storage or object metrics where available.
- Last successful run.

Use a supported Airflow metrics/exporter approach and document any limitations. Logs should include pipeline run ID and source month. Do not display fabricated values.

## Testing requirements

Build tests in layers:

### Unit tests

- Configuration validation.
- Source URL construction.
- Download retry/error behavior using mocked HTTP.
- Manifest generation.
- Schema normalization.
- Quality rules.
- Metric calculations.

### Integration tests

- MinIO upload/download.
- Spark reads a tiny Parquet fixture.
- Spark writes an Iceberg table using Hive Metastore.
- Trino reads the same Iceberg table.
- Airflow DAG imports and basic DAG structure validates.

### End-to-end tests

- Run the pipeline on a tiny fixture or one controlled source month.
- Verify Bronze object and manifest.
- Verify Silver valid/quarantine outputs.
- Verify Gold aggregates.
- Query expected results through Trino.
- Re-run the same month and prove idempotency.
- Trigger a controlled failure and verify Airflow shows the failure/retry correctly.

Tests must be runnable without downloading the full public dataset. Keep slow integration/e2e tests clearly separated from quick unit tests.

## Repository structure

Create a clean, maintainable structure similar to:

```text
.
├── .github/workflows/ci.yml
├── airflow/dags/
├── config/
├── docker/
│   ├── airflow/
│   ├── spark/
│   ├── trino/
│   └── prometheus/
├── docs/
│   ├── architecture.md
│   ├── data-dictionary.md
│   ├── data-quality.md
│   ├── operations-runbook.md
│   ├── decisions/
│   └── PROGRESS.md
├── infra/
│   ├── compose/
│   └── prometheus/
├── scripts/
├── src/lakehouse/
│   ├── ingestion/
│   ├── processing/
│   ├── quality/
│   ├── storage/
│   ├── config/
│   └── common/
├── sql/
│   ├── trino/
│   └── checks/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── monitoring/grafana/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── README.md
└── PROJECT_PLAN.md
```

Adjust only when there is a clear reason. Avoid empty placeholder directories that do not serve an implemented feature.

## Required documentation

- `README.md`: project overview, problem statement, architecture, AWS concept mapping, tech stack, quick start, sample queries, screenshots, testing, limitations, and truthful project description.
- `PROJECT_PLAN.md`: phases, checklists, acceptance criteria, and definition of done.
- `docs/architecture.md`: architecture and data flow.
- `docs/data-dictionary.md`: source and canonical field definitions.
- `docs/data-quality.md`: rules, severity, rejected-record behavior, and metrics.
- `docs/operations-runbook.md`: start/stop, rerun, backfill, troubleshooting, recovery.
- `docs/PROGRESS.md`: phase-by-phase work log.
- `docs/decisions/`: short Architecture Decision Records for important choices.

## CI requirements

GitHub Actions should run at least:

- Dependency/setup validation.
- Ruff lint/format check.
- Unit tests.
- Basic DAG import validation if feasible.

Integration tests can be a separate workflow/job or documented local command if they require multiple services. Never claim CI validates integrations it does not run.

## Git and security rules

- Do not commit `.env`, credentials, tokens, private keys, local volumes, raw Parquet files, logs, or large generated outputs.
- Provide `.env.example` with safe placeholders.
- Use least-privilege development credentials where practical.
- Do not expose MinIO, PostgreSQL, Hive Metastore, or Trino ports beyond what local development requires.
- Do not create a public service endpoint.
- Do not commit changes unless the user explicitly asks.
- Do not force-push, rewrite history, or delete user data.
- Do not modify the user's other portfolio repository.

## Definition of done

The project is complete only when:

- Required services start from documented commands.
- Official NYC TLC data can be ingested into MinIO Bronze.
- Spark writes Silver and Gold Iceberg tables.
- Trino queries those tables successfully.
- Airflow orchestrates the complete workflow.
- Data quality checks and quarantine work.
- Reprocessing is idempotent.
- Prometheus/Grafana display real operational metrics.
- Unit, integration, and end-to-end tests pass.
- CI passes for the checks it claims to run.
- README, architecture diagram, data dictionary, quality docs, runbook, and screenshots are complete.
- No secrets or raw data are committed.
- The repository accurately describes itself as an AWS-inspired local implementation.

## First action — do not skip

Start with **Phase 0 only**.

Inspect the environment, verify the official TLC source and selected Parquet schemas, check available resources, and propose a compatible version matrix for Docker images and dependencies. Do not build the full stack yet.

Then report:

1. Environment findings.
2. Verified source files and schemas.
3. Proposed architecture and compatibility/version decisions.
4. Risks or resource constraints.
5. Phase 0 files created/updated.
6. Tests or verification commands run and their actual results.
7. Remaining Phase 0 checklist items.

Update `PROJECT_PLAN.md` and `docs/PROGRESS.md` with truthful status, then stop and wait for approval before Phase 1.
