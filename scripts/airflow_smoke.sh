#!/usr/bin/env bash
set -euo pipefail

requested_months="${LAKEHOUSE_SOURCE_MONTHS:-}"
if [[ -f .env ]]; then
  set -a
  # Local Compose configuration is stored in the ignored .env file.
  source .env
  set +a
fi

export LAKEHOUSE_SOURCE_MONTHS="${requested_months:-${LAKEHOUSE_SOURCE_MONTHS:-2024-01,2024-02}}"
export DOCKER_SOCKET_GID="${DOCKER_SOCKET_GID:-$(stat -c '%g' /var/run/docker.sock)}"
service=(docker compose --profile core --profile spark --profile airflow exec -T airflow-scheduler)
api_health_url="http://127.0.0.1:8081/api/v2/monitor/health"
dag_id="yellow_taxi_bronze_to_gold"

for _ in $(seq 1 60); do
  if curl --fail --silent "$api_health_url" >/dev/null; then
    break
  fi
  sleep 2
done
if ! curl --fail --silent "$api_health_url" >/dev/null; then
  echo "Airflow API server did not become healthy ($api_health_url)" >&2
  exit 1
fi

import_errors=$("${service[@]}" airflow dags list-import-errors --output json 2>/dev/null | tail -n 1)
if [[ "$import_errors" != "[]" ]]; then
  echo "Airflow reported DAG import errors: $import_errors" >&2
  exit 1
fi

make_conf() {
  SOURCE_MONTHS="$1" FAILURE_MODE="$2" python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "source_months": os.environ["SOURCE_MONTHS"].split(","),
            "failure_mode": os.environ["FAILURE_MODE"],
        }
    )
)
PY
}

trigger_run() {
  local mode="$1"
  local months="$2"
  local run_id="phase6-${mode}-$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
  local conf
  conf=$(make_conf "$months" "$mode")
  "${service[@]}" airflow dags trigger -r "$run_id" -c "$conf" "$dag_id" >/dev/null
  printf '%s\n' "$run_id"
}

wait_for_state() {
  local run_id="$1"
  local expected="$2"
  local timeout_seconds="$3"
  local state=""
  for ((elapsed = 0; elapsed < timeout_seconds; elapsed += 2)); do
    state=$("${service[@]}" airflow dags state "$dag_id" "$run_id" 2>/dev/null \
      | tail -n 1 | cut -d ',' -f 1 | tr -d '\r')
    if [[ "$state" == "$expected" ]]; then
      echo "DAG $dag_id reached $state ($run_id)"
      return 0
    fi
    if [[ "$state" == "failed" && "$expected" != "failed" ]]; then
      echo "DAG $dag_id failed unexpectedly ($run_id)" >&2
      return 1
    fi
    sleep 2
  done
  echo "Timed out waiting for DAG $dag_id to reach $expected; last state: $state ($run_id)" >&2
  return 1
}

months_json=$(printf '%s' "$LAKEHOUSE_SOURCE_MONTHS" | tr -d ' ')
full_run=$(trigger_run retry_once "$months_json")
wait_for_state "$full_run" success 1800

first_month=${LAKEHOUSE_SOURCE_MONTHS%%,*}
rerun=$(trigger_run none "$first_month")
wait_for_state "$rerun" success 900

failure=$(trigger_run always "$first_month")
wait_for_state "$failure" failed 120

echo "Phase 6 orchestration smoke passed: end-to-end, retry, idempotent rerun, and visible failure"
