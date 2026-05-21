.DEFAULT_GOAL := help
SHELL         := /bin/bash
.SHELLFLAGS   := -euo pipefail -c

# Load .env if it exists
ifneq (,$(wildcard .env))
  include .env
  export
endif

# ── Helpers ───────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort

# ── Environment ───────────────────────────────────────────────────────────────
.PHONY: install
install: ## Install all dependencies via Poetry
	poetry install

.PHONY: env
env: ## Copy .env.example → .env (skips if .env already exists)
	@test -f .env && echo ".env already exists — skipping" || cp .env.example .env

# ── Ingestion ─────────────────────────────────────────────────────────────────
.PHONY: ingest
ingest: ## Run all ingestion pipelines
	poetry run python -m ingestion.run --all

.PHONY: ingest-%
ingest-%: ## Run a single ingestion pipeline  (e.g. make ingest-claims)
	poetry run python -m ingestion.run --source $*

.PHONY: ingest-cms
ingest-cms: ## Ingest CMS HCPCS data for DEFAULT_YEAR (override: make ingest-cms YEAR=2021)
	poetry run python ingestion/cms_ingest.py --year $(or $(YEAR),2022) --log-level INFO

# ── dbt ───────────────────────────────────────────────────────────────────────
.PHONY: dbt-run
dbt-run: ## Run all dbt models
	poetry run dbt run --project-dir dbt_project --profiles-dir dbt_project

.PHONY: dbt-test
dbt-test: ## Run dbt tests
	poetry run dbt test --project-dir dbt_project --profiles-dir dbt_project

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs
	poetry run dbt docs generate --project-dir dbt_project --profiles-dir dbt_project
	poetry run dbt docs serve  --project-dir dbt_project --profiles-dir dbt_project

.PHONY: dbt-fresh
dbt-fresh: ## Full refresh of all incremental models
	poetry run dbt run --full-refresh --project-dir dbt_project --profiles-dir dbt_project

# ── Great Expectations ────────────────────────────────────────────────────────
.PHONY: ge-check
ge-check: ## Run daily GE checkpoint; exits 1 on failure (Airflow-safe)
	poetry run python scripts/run_ge_checkpoint.py

.PHONY: ge-check-year
ge-check-year: ## Validate a single CMS year: make ge-check-year YEAR=2022
	poetry run python scripts/run_ge_checkpoint.py --year $(YEAR)

.PHONY: ge-check-verbose
ge-check-verbose: ## Run checkpoint with COMPLETE result format (shows all failing rows)
	poetry run python scripts/run_ge_checkpoint.py --result-format COMPLETE

.PHONY: ge-docs
ge-docs: ## Build GE Data Docs and print the local path
	poetry run great_expectations docs build --directory $(GE_ROOT_DIR)

# ── Testing ───────────────────────────────────────────────────────────────────
.PHONY: test
test: ## Run Python unit tests with coverage
	poetry run pytest

.PHONY: lint
lint: ## Lint and format check with ruff
	poetry run ruff check .
	poetry run ruff format --check .

.PHONY: fmt
fmt: ## Auto-format with ruff
	poetry run ruff format .
	poetry run ruff check --fix .

.PHONY: typecheck
typecheck: ## Run mypy type checks
	poetry run mypy ingestion scripts

# ── Airflow (Docker) ──────────────────────────────────────────────────────────
.PHONY: airflow-build
airflow-build: ## Build the custom Airflow Docker image
	docker-compose build

.PHONY: airflow-init
airflow-init: ## Run one-shot DB migration + admin user creation
	docker-compose up airflow-init

.PHONY: airflow-up
airflow-up: ## Start Airflow webserver + scheduler in the background
	docker-compose up -d airflow-webserver airflow-scheduler

.PHONY: airflow-down
airflow-down: ## Stop all Airflow containers (volumes preserved)
	docker-compose down

.PHONY: airflow-logs
airflow-logs: ## Tail scheduler logs
	docker-compose logs -f airflow-scheduler

.PHONY: airflow-trigger
airflow-trigger: ## Trigger the daily pipeline: make airflow-trigger YEAR=2022
	docker-compose exec airflow-scheduler \
		airflow dags trigger healthcare_dw_daily \
		--conf '{"year": $(or $(YEAR),2022)}'

# ── Snowflake ─────────────────────────────────────────────────────────────────
.PHONY: create-raw-tables
create-raw-tables: ## Create RAW schema tables in Snowflake (safe to re-run)
	poetry run python scripts/create_raw_tables.py

.PHONY: create-raw-tables-dry
create-raw-tables-dry: ## Preview CREATE TABLE DDL without connecting to Snowflake
	poetry run python scripts/create_raw_tables.py --dry-run

.PHONY: snowflake-setup
snowflake-setup: ## Run Snowflake DDL setup script (requires SNOWSQL_* env vars)
	snowsql \
		--accountname "$(SNOWFLAKE_ACCOUNT)" \
		--username    "$(SNOWFLAKE_USER)"    \
		--password    "$(SNOWFLAKE_PASSWORD)"\
		--rolename    SYSADMIN               \
		-f scripts/snowflake_setup.sql

# ── CI shortcut ───────────────────────────────────────────────────────────────
.PHONY: ci
ci: lint typecheck test dbt-test ge-check ## Run full CI pipeline locally
