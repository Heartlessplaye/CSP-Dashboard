export HOST_UID := $(shell id -u)
export HOST_GID := $(shell id -g)

COMPOSE := docker compose

.PHONY: help dirs build test up demo down logs bench clean-data

help:
	@echo "make build       Build runtime and dev images"
	@echo "make test        Run the pytest suite in a container"
	@echo "make up          Start processor + dashboard (http://localhost:8000)"
	@echo "make demo        Start processor + dashboard + synthetic load generator"
	@echo "make logs        Follow container logs"
	@echo "make bench       Run the throughput / lag benchmark in a container"
	@echo "make down        Stop everything"
	@echo "make clean-data  Remove generated input/output/state data"

dirs:
	@mkdir -p input output state

build:
	$(COMPOSE) --profile test --profile demo build

test:
	$(COMPOSE) --profile test run --rm test

up: dirs
	$(COMPOSE) up -d --build processor dashboard

demo: dirs
	$(COMPOSE) --profile demo up -d --build

logs:
	$(COMPOSE) --profile demo logs -f

bench:
	$(COMPOSE) --profile test run --rm test python tools/bench.py

down:
	$(COMPOSE) --profile demo --profile test down

clean-data:
	find input output state -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
