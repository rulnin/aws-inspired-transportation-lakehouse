.PHONY: sync lint format test check integration-test e2e-test infra-config infra-up infra-down infra-clean infra-airflow-up infra-monitoring-up infra-smoke-minio infra-smoke-spark infra-smoke-trino airflow-smoke ingest-bronze silver-fixture silver-run silver-verify zones-ingest gold-fixture gold-run gold-verify

sync:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

test:
	uv run pytest

integration-test:
	bash scripts/phase8_test.sh integration

e2e-test:
	bash scripts/phase8_test.sh e2e

check: lint test

infra-config:
	docker compose config --quiet

infra-up:
	docker compose --profile core up -d --build

infra-down:
	docker compose down

infra-clean:
	docker compose down --volumes --remove-orphans

infra-airflow-up:
	DOCKER_SOCKET_GID=$$(stat -c '%g' /var/run/docker.sock) docker compose --profile core --profile spark --profile airflow up -d --build

infra-monitoring-up:
	docker compose --profile core --profile spark --profile airflow --profile monitoring up -d --build

infra-smoke-minio:
	set -a; . ./.env; set +a; uv run python scripts/minio_smoke.py

infra-smoke-spark:
	docker compose --profile core --profile spark run --rm spark

infra-smoke-trino:
	uv run python scripts/trino_smoke.py

airflow-smoke:
	bash scripts/airflow_smoke.sh

ingest-bronze:
	set -a; . ./.env; set +a; uv run python scripts/ingest_bronze.py

silver-fixture:
	docker compose --profile core --profile spark run --rm spark /workspace/scripts/silver_fixture_smoke.py

silver-run:
	docker compose --profile core --profile spark run --rm spark /workspace/scripts/process_silver.py

silver-verify:
	uv run python scripts/trino_silver_smoke.py

zones-ingest:
	set -a; . ./.env; set +a; uv run python scripts/ingest_taxi_zones.py

gold-fixture:
	docker compose --profile core --profile spark run --rm spark /workspace/scripts/gold_fixture_smoke.py

gold-run:
	docker compose --profile core --profile spark run --rm spark /workspace/scripts/process_gold.py

gold-verify:
	uv run python scripts/trino_gold_smoke.py
