# ADR 0002: Local Compose infrastructure baseline

- **Status:** Accepted for local development, subject to smoke-check results
- **Date:** 2026-11-15

## Context

Phase 2 needs a runnable local MinIO, PostgreSQL-backed Hive Metastore, Spark local-mode runtime, Trino, Airflow, Prometheus, and Grafana setup. The host has about 11 GiB RAM, so the services must be staged. The Phase 0 candidate versions also need a concrete interoperability test.

## Decisions

1. Use Compose profiles `core`, `spark`, `airflow`, and `monitoring` to stage service startup. Bind UI/API ports to loopback; keep PostgreSQL and Hive Metastore internal to Compose.
2. Keep Spark `3.5.3`, Iceberg runtime/AWS S3 FileIO `1.6.1`, Trino `451`, Hive Metastore `3.1.3`, PostgreSQL `16`, and Airflow `3.4.0` as the integration baseline. The exact set is retained because Spark/Trino/HMS interoperability will be exercised as one stack; it is not inferred from individual version compatibility alone. Airflow 3.4.0 replaces the Phase 0 candidate 2.10.3, which reached end of life before this phase.
3. Configure Iceberg's Spark catalog to use Hive Metastore and Iceberg `S3FileIO`; configure Trino's Iceberg connector to use the same metastore and native S3 filesystem. This keeps S3 credentials in container environment variables and avoids adding Hadoop S3A jars to the runtime.
4. Use separate PostgreSQL databases/volumes for Hive and Airflow metadata.
5. Use Boto3 for a direct, temporary MinIO object put/get check. The SDK will also serve the planned Bronze ingestion implementation.
6. Use Prometheus and Grafana in their own profile; initially scrape Prometheus itself. Real MinIO/Airflow pipeline metric integration belongs to the monitoring phase.
7. The upstream MinIO public container repository is unavailable in this environment. Use a digest-pinned community mirror that claims to republish the unmodified final upstream server release, disclose that it is not independently attested, and restrict use to local development.

## Consequences

The stacks can be run in stages to fit the inspected host. The core integration test needs MinIO, Hive Metastore, Trino, and the one-shot Spark runtime available together. Airflow and monitoring can be tested separately. Local passwords and keys live in ignored `.env`; no tracked Compose file contains credentials. Validation confirmed the stack using Spark's Iceberg write/read smoke, Trino's readback, Airflow's LocalExecutor DAG smoke, and independent monitoring health checks.

The MinIO mirror is a supply-chain caveat. If that image becomes unavailable or fails provenance review, users will need to provide a trusted internal mirror or replace the local S3-compatible server and update the documented mapping. The current settings are not a production deployment profile.
