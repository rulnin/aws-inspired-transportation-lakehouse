# Performance and resource notes

## Reproducing measurements

The jobs emit `elapsed_seconds` and row counts in their structured completion logs. To
measure Silver and Gold independently, first start the core and Spark services and ensure
the selected Bronze months and Taxi Zone Lookup are already in MinIO:

```bash
docker compose --profile core up -d --build
docker compose --profile core --profile spark run --rm spark \
  /workspace/scripts/process_silver.py --month 2024-01 --month 2024-02 \
  --processing-run-id local-perf-silver
docker compose --profile core --profile spark run --rm spark \
  /workspace/scripts/process_gold.py --month 2024-01 --month 2024-02 \
  --pipeline-run-id local-perf-gold
```

Read each job's JSON completion log for the exact `elapsed_seconds`, `input_count`,
`output_count`, and `rejected_count` fields. Silver elapsed time covers reading Bronze,
classification/counting, and Iceberg writes; Gold elapsed time covers reading Silver,
aggregation/reconciliation, and Iceberg writes. These timers exclude service startup and
Bronze downloading. Gold `output_count` is the sum of monthly, hourly, and pickup-zone rows.

## Recorded local sample

Airflow task logs from the November 16, 2026 local run `phase6-retry_once-20261116T065348Z-31108`
recorded the following for the full 2024 months:

| Stage | Source month | Input rows | Output rows | Rejected rows | Elapsed seconds |
|---|---|---:|---:|---:|---:|
| Silver | 2024-01 | 2,964,624 | 2,964,568 | 56 | 66.943 |
| Silver | 2024-02 | 3,007,526 | 3,007,519 | 7 | 45.794 |
| Gold | 2024-01 | 2,964,568 | 77,801 | — | 38.167 |
| Gold | 2024-02 | 3,007,519 | 74,641 | — | 28.476 |

These are observed single-run timings, not a benchmark or service-level guarantee. Cached
JVM artifacts, Docker state, storage cache, host load, and disk speed affect the results.
The local host was documented with approximately 11 GiB of RAM. Compose limits individual
services, including 3 GiB for the Spark container; the complete Airflow/monitoring stack
uses more memory than staged core-only processing.

## Limits

- Spark runs in local mode with two worker threads, so this measures one host and does not
  predict distributed-cluster throughput.
- Silver and Gold replace one source-month partition and reconcile full month counts; this
  prioritizes correctness and repeatability over minimizing scans.
- MinIO is a single local service on a named Docker volume. Storage and object metrics are
  not a proxy for production S3 capacity or network behavior.
- The tiny E2E fixture verifies correctness and idempotency. Its duration is not presented
  as a production performance result.
