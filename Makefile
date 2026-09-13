.PHONY: test lint fmt doctor

PY ?= python3
HERMES_AGENT_PATH ?= $(HOME)/.hermes/hermes-agent

test:
	HERMES_AGENT_PATH=$(HERMES_AGENT_PATH) $(PY) -m pytest tests/ -q --timeout=30

lint:
	$(PY) -m ruff check .

fmt:
	$(PY) -m ruff check --fix .

doctor:
	hermes plugins doctor . --ci
