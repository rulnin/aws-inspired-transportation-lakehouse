#!/usr/bin/env bash
set -euo pipefail

scope="${1:-}"
if [[ "$scope" != integration && "$scope" != e2e ]]; then
  echo "Usage: bash scripts/run_service_tests.sh <integration|e2e>" >&2
  exit 2
fi
if [[ ! -f .env ]]; then
  echo "Copy .env.example to .env before running service tests" >&2
  exit 2
fi

set -a
# Compose service credentials are local-only values from the ignored .env file.
source .env
set +a
export DOCKER_SOCKET_GID="${DOCKER_SOCKET_GID:-$(stat -c '%g' /var/run/docker.sock)}"
compose=(docker compose --env-file .env --profile core --profile spark)
pytest_args=(tests/e2e)

if [[ "$scope" == integration ]]; then
  compose+=(--profile airflow)
  pytest_args=(tests/integration)
  services=(minio hive-postgres hive-metastore trino airflow-postgres airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer statsd-exporter)
else
  services=(minio hive-postgres hive-metastore trino)
fi

cleanup() {
  "${compose[@]}" down
}
trap cleanup EXIT

"${compose[@]}" up -d --build "${services[@]}"

if [[ "$scope" == integration ]]; then
  for _ in $(seq 1 60); do
    if curl --fail --silent http://127.0.0.1:8081/api/v2/monitor/health >/dev/null; then
      break
    fi
    sleep 2
  done
  if ! curl --fail --silent http://127.0.0.1:8081/api/v2/monitor/health >/dev/null; then
    echo "Airflow API server did not become healthy within 120 seconds" >&2
    exit 1
  fi
fi

uv run pytest "${pytest_args[@]}"
