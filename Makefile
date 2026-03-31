.PHONY: setup up down restart logs init shell backfill test deploy-prep clean

# Environment variables
include .env
export

setup:
	@echo "Setting up project directories..."
	@mkdir -p dags logs plugins scripts config data
	@echo "Initializing Airflow database and users..."
	@docker compose up airflow-init

up:
	@echo "Starting Airflow locally..."
	@docker compose up -d

down:
	@echo "Stopping Airflow..."
	@docker compose down

restart: down up

logs:
	@docker compose logs -f

init: setup up
	@echo "Airflow is starting up. Visit http://localhost:8080 (admin/admin)"

shell:
	@docker compose exec airflow-webserver bash

backfill:
	@echo "Triggering initial backfill job. This might take roughly 10-15 minutes due to API rate limits..."
	@docker compose exec airflow-webserver airflow dags trigger vn30_backfill_dag

test:
	@echo "Running validation tests..."
	@docker compose exec airflow-webserver pytest /opt/airflow/tests -v

deploy-prep:
	@echo "Preparing Streamlit deployment..."
	@echo "Check streamlit_app/requirements.txt and .streamlit/secrets.toml before committing!"

clean:
	@echo "Cleaning up generated files and containers..."
	@docker compose down -v
	@rm -rf logs/*
