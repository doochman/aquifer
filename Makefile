# Convenience targets for macOS and Linux. Everything delegates to ./lakehouse, which is the real interface.
# Windows: use lakehouse.cmd directly (no make needed).
.DEFAULT_GOAL := help

.PHONY: help quickstart up down status demo sql duckdb dbt stream flink notebook backup logs check reset doctor lint test

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

quickstart: ## First run: configure, start, load the demo, open the home page
	./lakehouse quickstart

up: ## Start the stack
	./lakehouse up

down: ## Stop the stack (data is kept)
	./lakehouse down

status: ## Probe every endpoint
	./lakehouse status

demo: ## Load the sample shop, run dbt, replay events, seed the dashboards
	./lakehouse demo

sql: ## Open a Trino shell
	./lakehouse sql

duckdb: ## Open a DuckDB shell on the Iceberg tables
	./lakehouse duckdb

dbt: ## Run dbt build for the shop project
	./lakehouse dbt build --exclude tag:streaming

stream: ## Send 200 order events through Kafka and Flink into Iceberg
	./lakehouse stream --events 200

flink: ## Show Flink jobs
	./lakehouse flink

notebook: ## Start JupyterLab and open it
	./lakehouse up --with notebook && ./lakehouse open notebook

backup: ## Archive all data into backups/
	./lakehouse backup

logs: ## Follow all logs
	./lakehouse logs

check: ## Full acceptance run
	./lakehouse check

doctor: ## Check Docker, memory and ports
	./lakehouse doctor

reset: ## Delete all data (asks first)
	./lakehouse reset

lint: ## Validate the compose model
	./lakehouse lint

test: ## Unit tests (no Docker needed)
	./lakehouse test
