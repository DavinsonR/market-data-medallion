# market-data-medallion — developer entrypoints.
#
# psql note: any PostgreSQL 16+ client works. On the author's machine the client
# lives in a conda env, so migrations run as:
#   make db-migrate PSQL=~/anaconda3/envs/pg/bin/psql

PY           := .venv/bin/python
PYTEST       := .venv/bin/pytest
RUFF         := .venv/bin/ruff
DBT          := .venv/bin/dbt
PSQL         ?= psql

# Honor .env (same file the Python entrypoints load) so psql/dbt targets agree with them.
-include .env
DATABASE_URL ?= postgresql://mdm@localhost:5433/mdm
export DATABASE_URL

.PHONY: help setup db-migrate ingest dbt-build dbt-freshness backtest export run test lint

help: ## List available targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*##"} {printf "  %-12s %s\n", $$1, $$2}'

setup: ## Create .venv and install all dependencies (uv preferred, pip fallback)
	@if command -v uv >/dev/null 2>&1; then uv sync --all-extras; \
	else python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"; fi

db-migrate: ## Apply db/migrations/*.sql in order via psql (see psql note above)
	@for f in db/migrations/*.sql; do \
		echo "applying $$f"; \
		$(PSQL) "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f "$$f"; \
	done

ingest: ## Incremental fetch of new candles into bronze (watermark-based)
	$(PY) -m pipeline.bronze

dbt-build: ## Build and test the silver + gold dbt models (local defaults; cloud runs go through `make run`)
	$(DBT) build --profiles-dir dbt --project-dir dbt

dbt-freshness: ## Check bronze source freshness thresholds (warn 36h / error 72h)
	$(DBT) source freshness --profiles-dir dbt --project-dir dbt

backtest: ## Run all configured strategies against the gold indicator mart
	$(PY) -m pipeline.backtest

export: ## Write exports/trading_sim.json for the portfolio site
	$(PY) -m pipeline.export

run: ## Full daily flow (Prefect 3): ingest -> dbt build -> backtests -> export
	$(PY) -m pipeline.flows

test: ## Run the pytest suite
	$(PYTEST)

lint: ## Ruff static checks
	$(RUFF) check .
