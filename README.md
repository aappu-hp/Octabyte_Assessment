# Airflow + Yahoo Finance Pipeline

This repository provides a Dockerized local development environment for an Apache Airflow-based pipeline that fetches intraday stock data (via `yfinance`) and persists it into PostgreSQL.

## Overview

The pipeline:

* fetches stock prices from Yahoo Finance using the `yfinance` library,
* stores them in a Postgres table with idempotent upserts,
* is orchestrated by Apache Airflow (DAG scheduled hourly),
* runs inside Docker Compose containers (Postgres, pgAdmin, Airflow webserver & scheduler).

## Features

* **Docker Compose setup** with Postgres, pgAdmin, and Airflow services.
* **Custom Airflow image** with required dependencies.
* **Stock fetch script** (`dags/scripts/fetch_and_store.py`) that:

  * Fetches data via `yfinance`.
  * Connects to Postgres with SQLAlchemy.
  * Creates `stock_prices` table if missing.
  * Performs upserts to avoid duplicates.
* **Airflow DAG** (`dags/yahoo_stock_dag.py`) that:

  * Runs hourly.
  * Reads stock tickers from the `TICKERS` environment variable.
  * Executes the fetch script using a PythonOperator.

## Project structure

```
.
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── README.md
├── dags/
│   ├── yahoo_stock_dag.py
│   └── scripts/
│       ├── __init__.py
│       └── fetch_and_store.py
```

## Environment variables

* `POSTGRES_USER` — DB username (default: `postgres`)
* `POSTGRES_PASSWORD` — DB password (default: `postgres`)
* `POSTGRES_DB` — DB name (default: `stocks_db`)
* `TICKERS` — comma-separated tickers for the DAG (e.g. `AAPL,MSFT`)
* `TARGET_TABLE` — target table name in Postgres (default: `stock_prices`)

## Getting started

### Prerequisites

* Docker & Docker Compose installed
* At least \~2GB free space

### Setup

1. Copy the environment example:

   ```bash
   cp .env.example .env
   ```

2. Build and start services:

   ```bash
   docker compose build --no-cache
   docker compose up -d
   ```

3. Confirm services are running:

   ```bash
   docker compose ps
   ```

4. Access the UIs:

   * Airflow: [http://localhost:8080](http://localhost:8080)
   * pgAdmin: [http://localhost:5050](http://localhost:5050)

## Running the script manually

### From host (with venv)

```bash
python -m dags.scripts.fetch_and_store AAPL MSFT
```

### From inside Airflow container

```bash
docker compose exec airflow-webserver bash -lc "python /opt/airflow/dags/scripts/fetch_and_store.py AAPL MSFT"
```

## Running with Airflow

### Trigger DAG manually

```bash
docker compose exec airflow-webserver airflow dags trigger yahoo_stock_dag
```

### List DAG runs

```bash
docker compose exec airflow-webserver airflow dags list-runs -d yahoo_stock_dag
```

### Show tasks

```bash
docker compose exec airflow-webserver airflow tasks list yahoo_stock_dag
```

### Run a task manually

```bash
docker compose exec airflow-webserver airflow tasks run yahoo_stock_dag fetch_and_store_task 2025-08-26T14:20:30+00:00 --local --subdir /opt/airflow/dags/yahoo_stock_dag.py
```

## Database verification

Connect to Postgres inside the container:

```bash
docker compose exec postgres psql -U postgres -d stocks_db -c "\d stock_prices"
docker compose exec postgres psql -U postgres -d stocks_db -c "SELECT ticker, COUNT(*) FROM stock_prices GROUP BY ticker;"
docker compose exec postgres psql -U postgres -d stocks_db -c "SELECT * FROM stock_prices ORDER BY ts DESC LIMIT 20;"
```

Or use pgAdmin at [http://localhost:5050](http://localhost:5050).

## Logs

Task logs are stored inside the Airflow container under:

```
/opt/airflow/logs/dag_id=yahoo_stock_dag/run_id=<run_id>/task_id=fetch_and_store_task/
```

Example:

```bash
docker compose exec airflow-webserver cat "/opt/airflow/logs/dag_id=yahoo_stock_dag/run_id=manual__2025-08-26T14:20:30+00:00/task_id=fetch_and_store_task/attempt=1.log"
```

## Troubleshooting

* If tasks are stuck in `queued` state, check:

  ```bash
  docker compose logs -f airflow-scheduler
  ```

* If running commands on Windows PowerShell, avoid using `\` line continuations. Put the command on a single line.

* To clear task runs:

  ```bash
  docker compose exec airflow-webserver airflow tasks clear yahoo_stock_dag --start-date 2025-08-23 --end-date 2025-08-27 --yes
  ```

* To inspect environment variables inside container:

  ```bash
  docker compose exec airflow-webserver bash -lc "printenv | grep -E 'TICKERS|POSTGRES'"
  ```

## Recap

* Dockerized Airflow + Postgres setup.
* Script to fetch stock data and upsert to Postgres.
* Hourly Airflow DAG wired to run the script.
* Verified data persistence with psql and pgAdmin.
* Logging and troubleshooting steps included.
