# ADR 0001: Phase 0 scope and compatibility baseline

- **Status:** Proposed
- **Date:** 2026-11-15

## Context

The project prompt requires a discovery-only first phase before building services or application code. We need a small, verifiable starting dataset and a candidate stack suitable for a resource-limited local environment.

## Decisions

1. Use January and February 2024 Yellow Taxi Parquet as the initial consecutive-month probe. Both files are linked from the official TLC data page and served from its `d37ci6vzurychx.cloudfront.net/trip-data/` source path.
2. Keep raw objects unchanged in MinIO Bronze; write separate manifests and process into Iceberg Silver/Gold with Spark local mode.
3. Use Hive Metastore backed by PostgreSQL as the initial shared catalog and Trino as the SQL client.
4. Treat the listed component versions as candidates only. Phase 2 must validate the actual shared catalog, S3-compatible storage, and Spark-to-Trino read path before pins are treated as tested.
5. Use Python 3.12 managed by `uv` because host Python 3.14.4 is not the project runtime proposal; do not require host Java.

## Evidence and consequences

The official TLC page lists both selected monthly Yellow Taxi files. HTTP HEAD returned 200 for each (49,961,641 bytes for January and 50,349,284 bytes for February). DuckDB's Parquet metadata reader reported identical schemas for both files: 19 nullable fields, including `Airport_fee` (DOUBLE), the timestamp columns, IDs, distance, passenger count, and fare components. The source schema is not presumed fixed for all months; schema normalization remains explicit.

The host is WSL2 Ubuntu 26.04.1 with 16 logical CPUs, approximately 11 GiB RAM, and 938 GiB free at the inspected mount. Docker Engine 29.9.3 and Compose 5.7.0 respond, but available details do not prove that the engine is Docker Desktop. Python 3.14.4 and `uv` 0.13.7 are installed; Java is absent. The available machine memory argues for staged Compose profiles and conservative Spark settings.

The proposed stack has not yet passed an interoperability test. Before pinning the integration versions, verify current upstream compatibility and exercise Spark writes, Hive Metastore registration, MinIO storage, and Trino reads together.
