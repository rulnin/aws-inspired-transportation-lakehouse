# Progress

## Current phase: Phase 9 — Documentation and portfolio release

### Phase 0 review

Phase 0 acceptance criteria are met based on the documented WSL2/resource inspection, official TLC source availability checks, and schema inspection for both selected months. Docker Desktop integration remains unconfirmed, and the proposed multi-service compatibility matrix remains a candidate to validate during infrastructure work. These are recorded caveats rather than blockers to Phase 1.

### Phase 1 completed

- Kept the existing initialized Git repository and added `.gitignore` rules for credentials, Python artifacts, local data, service state, and Parquet files. No commit was created.
- Added `pyproject.toml`, pinned resolved dependencies in `uv.lock`, and created a Python 3.12 `.venv` with `uv sync`.
- Configured Ruff lint/format rules and pytest discovery.
- Added `.env.example` with non-secret defaults.
- Added Make targets for sync, lint, format, test, and check.
- Added an initial README with project scope, setup commands, and truthful implementation status.
- Added environment-based settings validation for selected `YYYY-MM` months and log levels.
- Added JSON logging conventions with optional pipeline run ID, source month, component, and status fields.
- Added unit tests for settings validation and structured log formatting.

### Commands and verification

- `uv sync`: passed; created `.venv` with CPython 3.12.16, installed this project, pytest 8.4.2, Ruff 0.14.14, and generated `uv.lock`.
- `uv sync --locked`: passed.
- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed; 7 files already formatted.
- `uv run pytest`: passed; 12 unit tests, including rejection of malformed month-list entries.
- `git check-ignore -v .env sample.parquet data/sample.parquet logs/pipeline.log`: confirmed credential, raw Parquet, local data, and log paths are ignored.
- `make check`: could not run because GNU Make is not installed in this host. Equivalent lint, format, and test commands ran directly through `uv` and passed.

### Known issues / obstacles

- GNU Make is absent, so the Makefile targets are present but not executable in this environment until Make is installed. The underlying `uv` commands pass.
- The repository is initialized but has no commits; work remains uncommitted as required.
- The data pipeline remains future-phase work.

### Remaining Phase 1 checklist

- No implementation item remains open. Makefile target invocation is the only environment-specific verification limitation; target commands were verified directly.

### Phase 1 review before Phase 2

- Phase 1 was reviewed before Phase 2: its checklist and acceptance criteria are complete. `uv sync --locked`, Ruff, pytest (12 passed), ignore checks, and direct equivalents for Make targets were previously verified. GNU Make remains unavailable in this host; Docker Desktop integration remains unconfirmed as documented in Phase 0.

### Phase 2 completed

- Added staged Compose profiles for MinIO, PostgreSQL-backed Hive Metastore, Trino, local Spark, Airflow 3, and independent Prometheus/Grafana services. Added persistent volumes, health checks, resource limits, restart policies, loopback-only ports, and digest-pinned service images.
- Added a Hive Metastore image with the compatible PostgreSQL driver and Hadoop S3 filesystem support for Iceberg warehouse validation. Spark uses Iceberg `S3FileIO`; Trino uses native S3. Airflow uses a small derived image with pinned PostgreSQL sync/async drivers and an initialized SimpleAuthManager password file.
- Added MinIO put/get, Spark Iceberg write/read, Trino readback, and Airflow LocalExecutor DAG smoke checks. Updated README, architecture, operations notes, and the infrastructure ADR.
- Phase 1 acceptance was reviewed before starting: all Phase 1 checklist items and acceptance criteria were met. GNU Make remains unavailable, so Make target invocations were not run; equivalent `uv` commands passed. Docker Desktop integration remains unconfirmed from the available Docker server details.

### Phase 2 verification

- `docker compose config --quiet`: passed.
- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed.
- `uv run pytest`: 12 passed.
- `bash -n scripts/airflow_smoke.sh scripts/spark/entrypoint.sh`: passed.
- MinIO: container health check passed; `scripts/minio_smoke.py` stored and retrieved a test object.
- Core services: PostgreSQL, Hive Metastore, and Trino reported healthy. Hive schema initialization and Thrift reachability passed.
- Spark/Trino integration: Spark wrote and read the tiny Iceberg row in MinIO; `scripts/trino_smoke.py` read the same row through the shared Hive catalog.
- Airflow: API health response showed healthy metadata DB, scheduler, triggerer, and DAG processor. `scripts/airflow_smoke.sh` completed `infra_smoke` successfully under LocalExecutor.
- Monitoring: Prometheus and Grafana reported healthy independently; Prometheus `/-/healthy` and Grafana `/api/health` returned healthy responses.
- Tracked configs interpolate secrets from ignored `.env`; image references are digest pinned. `.env` is ignored and contains only local placeholders.

### Phase 2 obstacles and resolutions

- Slow registry image downloads delayed service validation but completed.
- Live checks exposed and fixed the Hive JDBC jar selection/permissions, S3 filesystem configuration and credentials, repeatable schema initialization, Trino JVM flags and node environment value, Spark's writable Ivy cache, Airflow PostgreSQL driver packages and volume permissions, and Airflow 3's execution API URL.
- A truncated Iceberg Maven artifact transfer failed once; Spark now uses Maven Central's canonical endpoint and the rerun passed. One Airflow DAG attempt also timed out during API startup; the smoke script now waits for API health, and subsequent LocalExecutor runs passed.
- Port 3000 was already occupied locally. Grafana is published on 3001; the existing service was left untouched.
- The MinIO server image is a community mirror of an upstream release because the official public image endpoint was unavailable. Its upstream-binary claim is not independently attested and is documented as a local-development supply-chain caveat.

### Next tasks

- Phase 2 was reviewed before Phase 3: all Phase 2 checklist items and acceptance criteria are complete. Compose configuration, Ruff, pytest (12 passed), MinIO round trip, Spark/Trino Iceberg interoperability, Airflow LocalExecutor DAG, and independent monitoring health checks passed. GNU Make invocation remained unavailable; direct `uv` equivalents passed.

### Phase 3 completed

- Added configurable month selection through `LAKEHOUSE_SOURCE_MONTHS` and repeatable `--month` CLI options. Generated URLs use only the official TLC CloudFront trip-data host.
- Added a streaming downloader with bounded exponential backoff for transient network failures and HTTP 408/429/5xx statuses. It checks HTTP success, non-empty size and Content-Length when supplied, incrementally computes SHA-256, and verifies the Parquet footer/schema with PyArrow before accepting the local file.
- Added MinIO landing at `raw/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet` with an adjacent JSON manifest. Identical source URL/checksum/size is skipped. Changed bytes for a month replace the canonical object and then its manifest. The source object is uploaded directly from its validated local file without transformations.
- Added structured per-month logging for run ID, source month, input/upload/skip counts, bytes, download and upload durations, status, and object key. Added `make ingest-bronze` and README operating instructions.
- Added PyArrow as a runtime dependency because boto3 and the standard library cannot validate Parquet structure. PyArrow is used to read file metadata/footer, not load trip rows.
- Reviewed previous-phase output before starting: Phase 2 acceptance criteria were all checked and its live integration evidence was recorded above. No Phase 2 implementation work was repeated.

### Phase 3 verification

- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed.
- `uv run pytest`: 20 passed, including 8 ingestion tests with tiny generated Parquet fixtures and mocked HTTP/S3 behavior.
- Unit tests verified official URL construction, streaming/checksum, transient retry/backoff including a truncated response, permanent 404 behavior, incomplete content-length rejection, unreadable Parquet rejection, deterministic manifest fields, identical-file skip, and changed-source replacement.
- `docker compose --env-file .env --profile core up -d minio`: MinIO became healthy.
- `uv run python scripts/ingest_bronze.py` with the configured January/February 2024 months: both official files passed validation and uploaded. Source HEAD responses reported 200 and lengths 49,961,641 bytes (January) and 50,349,284 bytes (February).
- Read both objects back from MinIO: bytes matched each manifest’s size and SHA-256; both object files remained readable Parquet; each manifest included month and run ID.
- Re-ran the same ingestion command: both months reported `skipped`, with zero duplicate uploads.
- The live command streams to a temporary directory and cleans local files at exit. Raw files remain only in the ignored MinIO volume, not in the repository.

### Phase 3 known issues and next tasks

- No Phase 3 checklist or acceptance item remains open. Phase 4 is the next planned phase (Spark Silver processing), but work stops here pending approval.
- The local MinIO service used for acceptance verification has been stopped after readback; the Phase 3 objects remain in its named volume. `docker compose down` stops services and keeps the volume; `make infra-clean` would remove it.

### Phase 4 review before implementation

- Phase 3 was reviewed before Phase 4. All Phase 3 checklist items and acceptance criteria were complete: the January and February 2024 official objects and manifests were verified by MinIO readback, and a second ingestion run skipped both identical files. No Bronze implementation was repeated.
- Started the existing core services and inspected Parquet metadata directly from the Bronze objects. January contained 2,964,624 rows and February 3,007,526. Both had the same 19 source columns, including `Airport_fee`; other months still use explicit optional/alias handling.

### Phase 4 completed

- Added a canonical snake_case Yellow Taxi schema with explicit source aliases, required/optional fields, and typed handling for missing optional columns. The Spark job reads the Bronze month and adjacent manifest through S3A, takes lineage timestamp/run ID from that manifest, and normalizes the source with `try_cast`.
- Added Spark/Hadoop S3A configuration using Hadoop `hadoop-aws` 3.3.4, matching the Spark image's Hadoop client 3.3.4. The existing Spark 3.5.3 and Iceberg 1.6.1 stack was retained. An initialized named Ivy volume caches Maven artifacts for Spark runs.
- Added `silver.yellow_taxi_trips` and `silver.yellow_taxi_quarantine` Iceberg tables in the shared Hive catalog. Both are partitioned by `source_month`; month-filtered overwrites replace the current month. Quarantine rows retain canonical fields, lineage, and rejection reason arrays.
- Added quality rules and reason codes for required timestamps/columns, timestamp order, numeric cast failures, fractional integer IDs, negative distance/passenger count, and location codes outside 1–265. The selected data also contains location values 264 and 265, so they are retained as source codes. Negative monetary values are kept in Silver and surfaced in `quality_warnings` rather than discarded.
- Added schema unit tests, a Spark fixture smoke for transformations and rule behavior, and a Trino smoke query that checks both tables, lineage, and rejection reasons. Added data dictionary, data-quality documentation, README commands, and Make targets.

### Phase 4 verification

- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed.
- `uv run pytest`: 25 passed (including five Silver schema-resolution tests).
- `docker compose --env-file .env config --quiet`: passed; `bash -n docker/spark/entrypoint.sh`: passed.
- `docker compose --profile core --profile spark run --rm spark /workspace/scripts/silver_fixture_smoke.py`: passed. Ten fixture rows yielded four valid and six quarantined rows; malformed fields and rule-specific reasons were asserted, optional airport fee became null, IDs 264/265 remained valid, and a negative fare remained valid with a warning.
- `docker compose --profile core --profile spark run --rm spark /workspace/scripts/process_silver.py --month 2024-01 --month 2024-02`: passed against the actual Bronze objects. January: 2,964,568 Silver / 56 quarantine; February: 3,007,519 Silver / 7 quarantine. These reconcile to the inspected source row counts. The job also counted warning rows and logged month timing.
- `uv run python scripts/trino_silver_smoke.py`: passed. Trino read both Iceberg tables for both months, found no missing Bronze lineage, and found no quarantine rows without rejection reasons. A direct check confirmed selected-file location IDs 264/265 are present and retained; 50,933 occurrences of 264 and 26,125 of 265 were observed across Silver pickup/dropoff values.
- Re-ran the same Spark processing command and Trino smoke. Per-month Silver/quarantine counts remained exactly the same, verifying no duplicate rows after reprocessing.
- `uv sync --locked`: unchanged since Phase 3 and was not needed; Phase 4 adds no host Python dependencies.

### Phase 4 obstacles and next tasks

- The Spark image uses Python 3.10 while the host uses Python 3.12. The shared logger and Spark fixture were kept compatible with both after the first fixture run exposed `datetime.UTC` incompatibility. A PySpark import path mismatch was also corrected to the Spark 3.5 API.
- The first Spark invocation downloaded about 347 MB of Maven artifacts and exposed permissions on a named cache volume. Added a small root-owned cache initializer and persistent volume; subsequent Spark runs reused the cached artifacts. No Spark/Iceberg/Hive/Trino versions were upgraded.
- No Phase 4 checklist or acceptance item remained open before Phase 5 began. The Silver tables and previous phase's Bronze inputs were reused without repeating Silver work.

### Phase 5 completed

- Reviewed `PROJECT_PLAN.md`, this progress record, and the Phase 4 implementation before beginning. Phase 4 acceptance was satisfied: January and February Silver/quarantine counts were reconciled to Bronze, Trino read both tables, rule fixtures passed, and rerunning Silver preserved counts.
- Defined monthly trip volume, source-reported revenue, average distance/fare/tip, hourly pickup demand, and pickup-zone measures with explicit grains and limitations in `docs/gold-metrics.md`.
- Added a streaming, checksummed ingest for the official TLC Taxi Zone Lookup CSV at `reference/taxi_zone_lookup.csv`. The downloaded CSV passed schema/duplicate/label validation and contains 265 unique rows.
- Added Spark transformations and Iceberg tables `gold.monthly_trip_summary`, `gold.hourly_trip_demand`, and `gold.pickup_zone_summary`. Tables are partitioned for source-month replacement. Zone aggregation retains unmatched location IDs with null labels.
- Before publishing each month, the job reconciles monthly/hourly/zone counts with Silver, monthly and zone revenue with Silver, and monthly averages with Silver. Added a local Spark fixture, Trino reconciliation smoke, three sample Trino queries, Make targets, and README sample results.
- Phase 4 was checked before work. No Phase 4 code or output was repeated; Gold consumed existing Silver tables.

### Phase 5 verification

- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed (29 files formatted).
- `uv run pytest`: 25 passed.
- `docker compose --env-file .env config --quiet`: passed.
- `docker compose --env-file .env --profile core up -d`: core services became healthy.
- `uv run python scripts/ingest_taxi_zones.py`: passed; validated and uploaded official lookup (265 rows, 12,331 bytes).
- `docker compose --env-file .env --profile core --profile spark run --rm spark /workspace/scripts/gold_fixture_smoke.py`: passed; fixture asserted formulas, grains, lookup labels, and row/revenue reconciliation.
- `docker compose --env-file .env --profile core --profile spark run --rm spark /workspace/scripts/process_gold.py --month 2024-01 --month 2024-02`: passed. January: 2,964,568 input trips, 1 monthly row, 77,540 hourly rows, and 260 pickup rows. February: 3,007,519 input trips, 1 monthly row, 74,386 hourly rows, and 254 pickup rows. Reconciliation passed before writes.
- `uv run python scripts/trino_gold_smoke.py --month 2024-01 --month 2024-02`: passed both before and after rerunning the Spark Gold job. Counts and measures remained stable; monthly revenue was $79,454,508.28 (January) and $80,073,431.28 (February). Trino also confirmed zone ID 1 maps to Newark Airport / EWR.
- README records representative metrics: Jan average distance 3.6521, fare $18.17, tip $3.34; Feb average distance 3.8609, fare $18.05, tip $3.30.

### Phase 5 obstacles and next tasks

- No unresolved Phase 5 issue remains. Live Spark processing took roughly 50–71 seconds per source month on this local setup because the full Silver months are scanned and reconciled before output.
- Phase 5 checklist and acceptance criteria are complete. Phase 6 (Airflow orchestration) is next; stop here pending approval.
- Core services were stopped after integration verification. Named data volumes with Bronze, Silver, Gold, lookup, and Spark cache data remain. `make infra-clean` removes those volumes.

### Phase 6 review before implementation

- Reviewed `PROJECT_PLAN.md`, this progress record, and the Phase 5 implementation. Phase 5 acceptance remained satisfied: all three Gold tables were queryable through Trino, reconciled to Silver, and returned the documented January/February results. Reprocessing Gold preserved counts and measures. No Gold transformation was repeated.

### Phase 6 completed

- Added the `yellow_taxi_bronze_to_gold` DAG with explicit tasks for a controlled retry probe, official TLC URL validation, Bronze ingestion, Bronze checksum/Parquet verification, Silver processing, Silver/quarantine reconciliation, Gold processing, and final Trino verification.
- Added a validated runtime `source_months` Airflow parameter, defaulted from `LAKEHOUSE_SOURCE_MONTHS`. Airflow task environments provide endpoints, bucket, log level, and credentials. Credentials are passed to Spark jobs through the Docker provider's private environment.
- Added bounded retries, retry delays, 30-minute task timeouts, failure callbacks with task/run/month context, and one-at-a-time local execution. The DAG logs source month and run ID for each stage.
- Added `scripts/verify_bronze.py`, `src/lakehouse/ingestion/verification.py`, and `scripts/check_silver_quality.py` to verify manifest/SHA/Parquet integrity and reconcile Bronze rows to valid Silver plus quarantine with lineage and rejection-reason checks.
- Added Docker provider and pinned Bronze-verification dependencies to the Airflow image. The scheduler launches isolated jobs in the existing Spark image using a local Docker socket, workspace bind mount, and persistent Spark Ivy volume. Startup detects the host Docker socket's group ID instead of assuming a machine-specific value.
- Added `scripts/airflow_smoke.sh`, a Bronze verification unit test, and `docs/operations-runbook.md` with startup, backfill, reprocessing, retry, failure, and recovery procedures. Updated README and Make targets for local orchestration.

### Phase 6 verification

- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed (34 Python files formatted).
- `uv run pytest`: 27 passed, including two Bronze integrity verification tests.
- `bash -n scripts/airflow_smoke.sh`: passed.
- `docker compose --env-file .env --profile core --profile spark --profile airflow config --quiet`: passed.
- Built the Airflow image with pinned Airflow 3.4.0, Docker provider 4.6.1, boto3 1.44.3, and PyArrow 24.0.0. Started core, Spark, and Airflow profiles; all required services became healthy. The scheduler successfully connected to Docker after Compose received the detected socket group ID.
- `airflow dags list-import-errors --output json`: returned `[]`; `airflow dags list` included `yellow_taxi_bronze_to_gold` and `infra_smoke`.
- Full DAG run `phase6-retry_once-20261116T065348Z-31108`: succeeded for January and February 2024 through final Trino verification. Bronze objects were checksum/Parquet verified; Silver processing reported January 2,964,568 valid/56 quarantined and February 3,007,519 valid/7 quarantined; Bronze-to-Silver quality reconciliation passed; Gold completed for both months; final Trino metric checks passed.
- The retry-enabled full run `phase6-retry_once-20261116T070015Z-25164` also completed successfully. Its controlled probe failed visibly on attempt 1, retried, and succeeded on attempt 2. Airflow logs included the failure detail, task ID, and DAG run ID.
- Same-month reprocessing run `phase6-none-20261116T070403Z-11283` completed successfully for January. Post-run Trino checks still returned 2,964,568 valid Silver rows, 56 quarantine rows, and Gold metrics of $79,454,508.28 revenue, 3.6521 average distance, $18.17 average fare, and $3.34 average tip.
- Terminal failure run `phase6-always-20261116T070715Z-11197` failed at `retry_probe` after two attempts; downstream tasks were `upstream_failed`. The task log recorded both attempts and the failure callback identified the failing task and run ID.
- The first live orchestration attempts exposed a Docker socket group mismatch, an over-escaped Jinja run ID, and a host bind path for the Spark Ivy settings file. These were corrected; subsequent full and smoke DAG runs passed. No unresolved Phase 6 obstacles remain.
- GNU Make is not installed on this host, so `make infra-airflow-up` could not be invoked directly. The equivalent Compose startup command with the detected socket group ID was run successfully.

### Phase 6 next tasks

- Phase 6 checklist and acceptance criteria are complete. Phase 7 (observability and operational readiness) is next; stop here pending approval.
- Core, Spark, and Airflow services were stopped after verification. Named volumes with Bronze, Silver, Gold, Airflow metadata/logs, reference data, and Spark cache remain. `make infra-clean` removes those volumes.

### Phase 7 review before implementation

- Reviewed `PROJECT_PLAN.md`, `PROMPT.md`, this progress record, and the Phase 6 implementation. All Phase 6 checklist items and acceptance criteria were marked complete. Prior live evidence covered full two-month DAG processing, successful retry, safe reprocessing, and a visible terminal failure; no orchestration or transformation work was repeated.
- Phase 7 gaps were the Prometheus self-scrape only, no Airflow exporter, no scrapeable service probes, and no provisioned dashboard. Structured JSON logs and container health checks already existed and were retained.

### Phase 7 completed

- Enabled MinIO's Prometheus v3 endpoint for the internal Compose network. Prometheus now scrapes MinIO storage metrics and the Airflow/Prometheus exporters.
- Added the Airflow-supported StatsD integration (`apache-airflow[statsd]`) and a digest-pinned Prometheus StatsD exporter. Airflow exposes task outcomes and task/DAG duration metrics. The DAG emits a last-success timestamp after final Trino verification.
- Added a digest-pinned Blackbox Exporter and explicit HTTP/TCP probe modules for MinIO, Trino, Airflow API (including its scheduler/metadata health status), both PostgreSQL services, Hive Metastore, Prometheus, and Grafana. Compose health checks remain available as container-level detail.
- Added a best-effort UDP StatsD gauge helper. Silver emits input, valid, and rejected row counts; Gold emits output rows. Delivery failure logs a warning and does not interrupt data processing. Month is a bounded label; run ID remains in structured logs to avoid high-cardinality metric labels.
- Provisioned the Transportation Lakehouse Overview Grafana dashboard with service status, Airflow task outcomes and duration, Silver/Gold row counts, last successful run, and MinIO bucket storage. Added metric definitions, known gaps, failure handling, and restart recovery instructions to the runbook.
- Updated the monitoring startup target and README/local-infrastructure guide. Exporter images are version and digest pinned.

### Phase 7 verification

- Reviewed Phase 6 acceptance before work; all Phase 6 checklist/acceptance items remained complete.
- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed (36 Python files formatted).
- `uv run pytest`: 30 passed, including new tests for tagged gauge output, no-op without a configured exporter, and best-effort behavior on exporter failure.
- `python3 -m json.tool monitoring/grafana/dashboards/lakehouse-overview.json`: passed.
- `docker compose --env-file .env --profile core --profile spark --profile airflow --profile monitoring config --quiet`: passed.
- `bash -n scripts/airflow_smoke.sh` and `git diff --check`: passed.
- Started the full Compose stack with the detected Docker socket group. All services became healthy; Prometheus had 8/8 successful HTTP/TCP probes and successful scrapes for MinIO, Airflow StatsD exporter, Blackbox Exporter, and Prometheus.
- Live Prometheus queries returned real task success/failure counters, successful DAG duration, last-success timestamp, Silver input/valid/rejected gauges, Gold output gauges, and MinIO storage usage. For January 2024 the gauges reported 2,964,624 input, 2,964,568 valid, 56 rejected, and 77,801 combined Gold output rows. MinIO reported 481,915,265 bytes at the verification scrape.
- Grafana API confirmed the provisioned dashboard and all 6 panels. The panel expressions reference the live Prometheus series.
- Restarted MinIO; its Compose health returned to `healthy` while named-volume data remained available. A one-month end-to-end retry run `phase6-retry_once-20261117T022044Z-7016` succeeded through final Trino verification after restart. Same-month rerun `phase6-none-20261117T022439Z-10785` also succeeded. The smoke's controlled terminal failure run `phase6-always-20261117T022801Z-11908` ended failed as expected; `scripts/airflow_smoke.sh` exited successfully overall.
- A first recovery attempt failed to launch Spark because the Airflow scheduler was recreated without the host Docker socket's supplementary group. The detected group was reapplied and the full recovery/reprocessing/failure smoke passed. The first Blackbox startup also revealed the exporter image requires an explicit config path; adding the minimal HTTP/TCP configuration resolved it. No unresolved Phase 7 blockers remain.
- GNU Make remains unavailable on this host, so the monitoring startup target could not be invoked directly; its exact Compose command (with dynamic Docker socket group) ran successfully.

### Phase 7 next tasks

- Phase 7 checklist and acceptance criteria are complete. Phase 8 (testing, CI, and performance checks) is the next planned phase; stop here pending approval.
- The full Compose stack is left running with its verified volumes and dashboard available for review. `make infra-down` stops services and preserves data; `make infra-clean` removes named volumes.

### Phase 8 review before implementation

- Reviewed `PROJECT_PLAN.md`, `PROMPT.md`, this progress record, and the completed Phase 7 implementation. Phase 7 checklist and acceptance criteria were all complete: live MinIO/Airflow metrics, 8/8 health probes, six real Grafana panels, structured run-context logs, and successful service restart/recovery were verified. No monitoring work was repeated.
- Existing unit tests already covered settings, ingestion retries/idempotency, Bronze checksums/manifests, and source-schema normalization. Existing Spark fixtures covered Silver classification and Gold formulas. Gaps were CI, a controlled test entry point, empty-input and explicit reconciliation-failure unit cases, cross-service E2E coverage, and recorded reproducible performance measurements.

### Phase 8 completed

- Added empty-download coverage and a directly testable `assert_silver_reconciles` quality gate with success and failure cases.
- Added opt-in integration tests for MinIO round-trip, Spark writing Iceberg through Hive Metastore and Trino reading it, Airflow DAG imports/stage structure, and Silver quality-rule fixture behavior.
- Added a four-row E2E fixture with missing optional source columns, three valid records, one invalid/quarantined record, Bronze manifest/SHA/Parquet verification, Silver and Gold Iceberg writes, Trino assertions, and a second processing pass to prove idempotent counts. Fixture rows/objects and their Iceberg partitions are cleaned after the test.
- Added `scripts/phase8_test.sh`, `make integration-test`, and `make e2e-test` for opt-in Compose startup/readiness/test/teardown. The wrapper preserves named data volumes. Default `uv run pytest` remains service-free and quick.
- Added `.github/workflows/ci.yml` for locked dependency setup, Ruff, DAG syntax compilation, and unit tests. Service integrations remain local and opt-in.
- Added `docs/performance.md` with stage-specific commands, measured January/February row counts and elapsed times from existing Airflow task logs, resource context, and limitations. Updated README test/performance instructions and project status.

### Phase 8 verification

- `uv sync --locked`: passed.
- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed (40 Python files formatted).
- `uv run python -m compileall -q airflow/dags`: passed.
- `uv run pytest`: 32 passed.
- CI-equivalent clean-room check: copied the project into a temporary clean directory without `.git`, `.env`, or existing virtual environments; `uv sync --locked`, Ruff lint, Ruff format check, DAG syntax compilation, and all 32 unit tests passed there. No public TLC data or existing service state was needed.
- `bash -n scripts/phase8_test.sh scripts/airflow_smoke.sh`, Compose config validation, Git diff whitespace check, and GitHub workflow YAML parse: passed.
- `bash scripts/phase8_test.sh integration`: 4 passed in 61.38 seconds against the local services. MinIO put/get, Spark→Iceberg→Hive Metastore→Trino interoperability, Airflow DAG import and task boundaries, and Silver Spark quality fixtures all passed.
- `bash scripts/phase8_test.sh e2e`: 1 passed in 50.63 seconds. The four-row fixture produced 3 Silver rows and 1 quarantined row; Gold and Trino counts/revenue reconciled identically on both processing passes. The test verified and removed its Bronze objects and cleared its Iceberg test-month partition.
- Existing Airflow logs provided measured timings: Silver Jan 66.943 s for 2,964,624 rows (2,964,568 output, 56 rejected), Silver Feb 45.794 s for 3,007,526 rows (3,007,519 output, 7 rejected), Gold Jan 38.167 s for 2,964,568 input and 77,801 output rows, and Gold Feb 28.476 s for 3,007,519 input and 74,641 output rows. These are single local observations; reproduction commands and limits are documented.
- One integration-test assertion initially used an unsupported Airflow 3.4 `--output json` option; it now checks the supported text task-list output. Initial clean-room checks also caught import ordering and formatting issues in the new opt-in tests; those were fixed before the successful clean-room run. No unresolved Phase 8 blockers remain.
- GNU Make is unavailable on the host, so the two Make wrappers were exercised through their underlying `bash scripts/phase8_test.sh` commands.
- The GitHub-hosted workflow has not been dispatched from this local workspace. Its exact setup/lint/format/syntax/unit steps passed from a temporary clean project copy.

### Phase 8 next tasks

- Phase 8 checklist and acceptance criteria are complete. Phase 9 documentation and portfolio release is next; stop here pending approval.
- All Compose services are stopped after testing; Docker named volumes remain. `make infra-clean` removes them.

### Phase 9 review before implementation

- Reviewed `PROJECT_PLAN.md`, `PROMPT.md`, README, architecture, data dictionary, quality guide, operations runbook, sample SQL, and the Phase 8 results. All Phase 8 checklist items and acceptance criteria remained complete. Detailed data and operations documents already existed; Phase 9 reused them rather than duplicating their content.
- Phase 9 gaps were a concise recruiter-oriented README, setup instructions that account for the Airflow Docker socket group, screenshots of the actual services/results, explicit limitations and costs, and a repository hygiene audit. The repository had no commits, so the requested release required an initial commit after auditing its contents.

### Phase 9 completed

- Reworked README to explain the problem, architecture and AWS concept mapping, stack, local setup/demo, sample SQL/results, links to contracts and runbook, limitations, costs, attribution, and the local-only scope.
- Reused the existing Mermaid architecture diagram, data dictionary, quality rules, operations/troubleshooting runbook, and three Trino SQL examples. Added the real verified query output to the README.
- Captured screenshots from the live local Airflow DAG run, MinIO Bronze bucket, Grafana dashboard, and Trino Gold query results in `docs/screenshots/`.
- Fixed the README Compose startup example to pass the detected Docker socket group ID, as required for Airflow's Docker-launched Spark tasks.
- Screenshot review found that the Grafana dashboard's datasource UID did not match the provisioned datasource UID. Pinned the provisioning UID to `prometheus`; also converted the Unix-seconds timestamp to milliseconds for Grafana's ISO date display. The dashboard now queries live Prometheus data.
- Audited the release contents: `.env` is ignored, `.env.example` contains only local placeholder values, no raw Parquet/large generated files are present, and no private absolute machine paths were found.

### Phase 9 verification

- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: passed (40 Python files formatted).
- `uv run pytest`: 32 passed.
- `uv run python -m compileall -q airflow/dags`: passed.
- Compose config validation, shell syntax checks, JSON parse of the Grafana dashboard, Git whitespace check, and README local-link checks: passed.
- Reproduced the README's stack startup with a detected Docker socket group and ran the official-data Airflow pipeline. Bronze verification, Silver processing and reconciliation, Gold processing, and final Trino verification passed for January/February 2024 (`phase6-retry_once-20261117T033743Z-27756`). A one-month reprocessing run also succeeded (`phase6-none-20261117T034237Z-28693`).
- `uv run python scripts/trino_gold_smoke.py --month 2024-01 --month 2024-02`: passed; counts and measures reconciled to Silver and zone lookup results matched.
- Grafana datasource API returned UID `prometheus`; Prometheus returned eight service probes, all with value `1`. Dashboard panels display service state, Airflow task outcomes/duration, Silver/Gold row counts, MinIO storage, and last-success time.
- Screenshots were captured with headless Chromium from the service UIs and a Trino query executed against the shared Iceberg Gold table. The displayed January/February values match the verified SQL output above.
- Triggered `phase9-documentation-failure-check-20261117` with `failure_mode=always`; Airflow reported the expected failed DAG run at the controlled retry probe.
- An initial demo launch omitted the Docker socket group and failed at Spark container creation. The documented command was corrected and the full pipeline and reprocessing passed after scheduler recreation with the detected group.
- The audited release contents contain no `.env`, credentials, raw Parquet, oversized generated files, or private machine paths. The Phase 9 request explicitly includes the versioned local release tag.
- The Phase 9 request explicitly includes the versioned release tag. After all checks passed, created the initial local release commit and tagged it `v0.1.0`; the tag was not pushed.

### Phase 9 next tasks

- README, architecture mapping, data dictionary, quality rules, runbook, sample SQL, real screenshots, limitations, costs, and the local `v0.1.0` tag are complete. Phase 9's acceptance criteria are met.
- No subsequent phase is started; stop here and await the user's direction.
- All Compose services are stopped after the final dashboard screenshot; named data volumes remain. `make infra-clean` removes them.
