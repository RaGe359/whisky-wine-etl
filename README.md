# Whisky & Wine ETL Project

This project uses Docker to set up an Airflow pipeline to compare alternative investments.

## Goals

- Scrape whisky auction listings from https://whiskyauctioneer.com/
- Analyze wine dataset taken from Kaggle API
- Orchestrate with Airflow
- Use Postgres for storage

## Structure

- `dags/`: Airflow DAGs
- `scripts/`: Python scripts
- `data/`: Input/exported data
- `.env`: Docker environment config

## Setup

```bash
docker compose up airflow-init
docker compose up -d
