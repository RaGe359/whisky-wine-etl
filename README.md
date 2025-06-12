# Whisky & Wine ETL Project

This project uses Docker to set up an Airflow pipeline to compare alternative investments.

## Goals

- Scrape whisky auction listings; link - https://whiskyauctioneer.com/
- Analyze wine dataset; Taken from Kaggle instead of an API; link - https://www.kaggle.com/datasets/zynicide/wine-reviews
- Use Postgres for storage
- Orchestrate with Airflow

## Structure

- `dags/`: Airflow DAGs
- `scripts/`: Python scripts
- `data/`: Input/exported data
- `.env`: Docker environment config

## Setup

```bash
docker compose up airflow-init
docker compose up -d
