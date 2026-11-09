.DEFAULT_GOAL := help
.PHONY: help install env up up-demo down logs migrate run-api run-relay run-consumer lint format typecheck test test-unit check

TEST_DATABASE_URL ?= postgresql+asyncpg://payments:payments@localhost:5432/payments_test

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

install: ## Install dependencies and git hooks
	uv sync
	uv run pre-commit install

env: ## Create .env from the example if it does not exist
	@test -f .env || cp .env.example .env

up: env ## Build and start the whole stack
	docker compose up -d --build

up-demo: env ## Start the stack plus a local webhook receiver on :8080
	docker compose --profile demo up -d --build

down: ## Stop the stack
	docker compose --profile demo down

logs: ## Follow logs of the application services
	docker compose logs -f api outbox-relay consumer

migrate: ## Apply migrations to DATABASE_URL
	uv run alembic upgrade head

run-api: ## Run the API locally (needs postgres)
	uv run uvicorn payments.main:app --reload

run-relay: ## Run the outbox relay locally
	uv run python -m payments.messaging.outbox_relay

run-consumer: ## Run the consumer locally
	uv run faststream run payments.messaging.consumer:app

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

format: ## Fix lint issues and format
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## Static type checks
	uv run mypy

test: ## All tests (needs postgres: `docker compose up -d postgres`)
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) uv run pytest --cov

test-unit: ## Tests that need no database
	uv run pytest -m "not integration"

check: lint typecheck test ## Everything CI runs
