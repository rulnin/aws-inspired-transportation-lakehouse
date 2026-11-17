# Local infrastructure

## Profiles and service URLs

All published ports bind to `127.0.0.1`. Hive Metastore and Airflow PostgreSQL use the internal Compose network. Hive PostgreSQL is also available at `127.0.0.1:5433` for local inspection. This database stores catalog metadata; query trip data through Trino.

| Profile | Services | Host access |
|---|---|---|
| `core` | MinIO, PostgreSQL for Hive, Hive Metastore, Trino | MinIO API `http://127.0.0.1:9000`, MinIO Console `http://127.0.0.1:9001`, Trino `http://127.0.0.1:8080` |
| `spark` | One-shot Spark local-mode runtime | No host port; submit with `docker compose --profile core --profile spark run --rm spark` |
| `airflow` | Airflow PostgreSQL, API server, scheduler, DAG processor, triggerer, LocalExecutor initializer | Airflow UI `http://127.0.0.1:8081` |
| `monitoring` | Prometheus, Grafana, blackbox exporter | Prometheus `http://127.0.0.1:9090`, Grafana `http://127.0.0.1:3001` |
| `airflow` / `spark` / `monitoring` | StatsD exporter | Internal only; receives Airflow and pipeline metrics |

## Start and stop

1. Copy `.env.example` to `.env` and replace the local-only passwords and Airflow keys. `.env` is ignored by Git.
2. Validate Compose expansion with `docker compose config --quiet`.
3. Start the core profile using `docker compose --profile core up -d --build`.
4. Run `make infra-smoke-minio`, `make infra-smoke-spark`, and `make infra-smoke-trino` in that order.
5. Stop core services with `docker compose down`. Named volumes persist unless explicitly removed.
6. Start the observable Airflow stack with `make infra-monitoring-up`. On an approximately 11 GiB host, monitor memory because this runs the complete stack together.

`docker compose down -v` deletes the project's persisted databases and object-store data. Use it only when a full local reset is intended.

## Health checks and verification

- PostgreSQL uses `pg_isready`; the Hive Metastore checks its Thrift listener; Trino checks `/v1/info`.
- MinIO checks its live health endpoint. Airflow API server and scheduler have HTTP and job-heartbeat health checks.
- Prometheus and Grafana have container health checks. Prometheus probes service readiness over HTTP/TCP and scrapes MinIO, Airflow's StatsD exporter, and itself.
- `scripts/minio_smoke.py` creates the `lakehouse` bucket if missing, writes a unique temporary object, reads the bytes back, and removes the object.
- `scripts/iceberg_interop_smoke.py` writes a tiny table through Spark, and `scripts/trino_smoke.py` verifies that Trino reads the same values through the shared Hive Metastore and MinIO warehouse.
- `airflow/dags/infra_smoke.py` is a minimal local-executor check, not a pipeline DAG. `scripts/airflow_smoke.sh` triggers it and waits for success.

## Resource guidance

Compose applies conservative per-container memory limits. The core profile should fit the inspected local host when run alone; Spark is a one-shot local process. The complete observable profile runs all services together and uses more memory than isolated profiles.

## Image and compatibility notes

- Spark `3.5.3` uses Java 17 and Scala 2.12, with Iceberg Spark runtime and AWS S3 FileIO artifacts both pinned to `1.6.1`. The Spark runtime includes Hadoop S3A `hadoop-aws` `3.3.4`, matching the Spark image's Hadoop client `3.3.4`, for reading Bronze Parquet directly from MinIO.
- Hive Metastore `3.1.3` uses PostgreSQL 16. The image is built with the PostgreSQL JDBC 42.7.4 driver; the JAR is fetched from Maven Central with a SHA-256 check during the image build.
- Trino `451` uses the Iceberg connector, Hive Metastore Thrift, and native S3 filesystem settings for the MinIO endpoint.
- Airflow `3.4.0` uses Python 3.12 and LocalExecutor with its own PostgreSQL database. Its SimpleAuthManager credentials are initialized from ignored `.env` values.
- The documented Spark–Iceberg–HMS–MinIO–Trino smoke checks passed on this Compose stack. This is a local development baseline, not a production compatibility guarantee.
- The Hive Metastore image removes the bundled, incompatible PostgreSQL JDBC driver and adds a checksum-pinned driver plus the image's Hadoop AWS and AWS SDK jars. A Hadoop filesystem alias lets Hive validate the Iceberg `s3://` warehouse path; credentials and MinIO endpoint settings come from `.env`.
- The Airflow 3.4.0 image adds pinned `psycopg2-binary` and `asyncpg` packages for its PostgreSQL metadata engine. The API server URL is shared with task workers for the Airflow 3 execution API. LocalExecutor parallelism is limited to two tasks for the inspected host.
- PostgreSQL, Prometheus, and Grafana image references are digest pinned. The selected MinIO server remains a digest-pinned community mirror with the provenance caveat below.
- The MinIO service image is pinned to a mirror release and digest in Compose. MinIO's Docker Hub repository and its `mc` client are archived, and its official download endpoints returned HTTP 410 during Phase 2. The selected mirror claims to publish an unmodified upstream image, but the claim is not independently attested. Keep this stack local-only and review the mirror/image source before reuse.

The Python project uses `boto3` for the MinIO S3 API smoke check and Bronze uploads. Spark reads Bronze with Hadoop S3A and writes Iceberg with Iceberg's S3 FileIO. No MinIO, PostgreSQL, Spark, Hive, Trino, Airflow, Prometheus, or Grafana software is installed directly on the host.
