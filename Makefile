# Linux/macOS/WSL entry points. On Windows without make, use .\dev.ps1 <target>.
SHELL := bash
COMPOSE := docker compose
PY := $(if $(wildcard .venv/Scripts/python.exe),.venv/Scripts/python.exe,.venv/bin/python)
JMX_VERSION := 1.0.1
JMX_JAR := connect/jmx-exporter/jmx_prometheus_javaagent.jar

.PHONY: help env venv up down reset ps logs register topics es-bootstrap seed test verify \
        bench-single bench-pertx bench-rate probe report pgbench monitoring jmx-download \
        rebuild chaos dashboard

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

env: ## create .env from .env.example if missing
	@test -f .env || cp .env.example .env

venv: ## host virtualenv for tests and benchmark scripts
	python -m venv .venv
	$(PY) -m pip install -r consumer/requirements-dev.txt -r benchmark/requirements.txt

up: env ## start core stack (PG, Kafka, Connect, ES, Kibana, UI, consumer) and register connectors
	$(COMPOSE) up -d --build
	$(COMPOSE) ps

down: ## stop containers, keep volumes
	$(COMPOSE) --profile monitoring --profile bench down

reset: ## stop and DELETE all volumes (PG data, Kafka log, ES indices)
	$(COMPOSE) --profile monitoring --profile bench down -v

ps:
	$(COMPOSE) ps

logs: ## follow consumer + connect logs
	$(COMPOSE) logs -f --tail=100 consumer kafka-connect

register: ## (re)register connectors after editing connect/*.json
	$(COMPOSE) run --rm connect-init

topics: ## (re)create Kafka topics
	$(COMPOSE) run --rm kafka-init

es-bootstrap: ## (re)install ES templates and v1 indices/aliases
	$(COMPOSE) run --rm es-init

seed: ## 100k products + 10k orders
	$(PY) benchmark/seed.py --products 100000 --orders 10000

test: ## consumer unit tests
	cd consumer && ../$(PY) -m pytest

verify: ## compare PostgreSQL and Elasticsearch
	$(PY) benchmark/verify_consistency.py --wait 120

bench-single: ## 10k updates in one transaction
	$(PY) benchmark/load_update.py single-tx --count 10000

bench-pertx: ## 10k updates in 10k transactions
	$(PY) benchmark/load_update.py per-tx --count 10000

bench-rate: ## 500 updates/s for 20 s
	$(PY) benchmark/load_update.py rate --rate 500 --duration 20

probe: ## commit -> searchable latency probes
	$(PY) benchmark/measure_lag.py probe --count 200

report: ## per-stage lag for the most recent load run
	$(PY) benchmark/measure_lag.py report --run "$$(ls -t results/*-load-*.json | head -1)"

pgbench: ## pgbench baseline vs CDC vs paused connector
	$(COMPOSE) --profile bench up -d postgres-baseline
	$(PY) benchmark/seed.py --port-var POSTGRES_BASELINE_PORT --orders 0
	bash benchmark/pgbench/compare.sh 60 8

jmx-download: ## fetch the JMX exporter javaagent for Debezium metrics
	curl -fL -o $(JMX_JAR) https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$(JMX_VERSION)/jmx_prometheus_javaagent-$(JMX_VERSION).jar

monitoring: ## Prometheus + Grafana + exporters
	$(COMPOSE) --profile monitoring up -d

dashboard: ## regenerate the Grafana dashboard JSON
	$(PY) monitoring/grafana/build_dashboard.py

rebuild: ## replay topic into a new index version: make rebuild V=2
	bash benchmark/chaos/08-rebuild-index.sh $(or $(V),2)

chaos: ## run one chaos scenario: make chaos N=01
	bash $$(ls benchmark/chaos/$(N)-*.sh)
