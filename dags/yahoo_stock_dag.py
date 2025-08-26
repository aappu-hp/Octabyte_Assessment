"""
yahoo_stock_dag.py

Airflow DAG that runs hourly and calls fetch_and_store from dags/scripts/fetch_and_store.py.
Tickers are read from TICKERS environment variable (comma-separated).
"""

from __future__ import annotations
import os
import logging
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "yahoo_stock_dag"
SCHEDULE = "@hourly"
START_DATE = pendulum.now("UTC").subtract(days=1)
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_fetch(**context) -> dict:
    """Wrapper that runs fetch_and_store for tickers in TICKERS env var."""

    tickers_env = os.getenv("TICKERS", "")
    if not tickers_env:
        raise ValueError("TICKERS environment variable not set")

    tickers = [t.strip() for t in tickers_env.split(",") if t.strip()]
    if not tickers:
        raise ValueError("No tickers parsed from TICKERS env variable")

    logging.info("Running fetch_and_store for tickers=%s", tickers)

    from dags.scripts.fetch_and_store import fetch_and_store

    result = fetch_and_store(tickers_list=tickers, table_name=os.getenv("TARGET_TABLE", "stock_prices"))
    logging.info("fetch_and_store result: %s", result)

    errors = result.get("errors") if isinstance(result, dict) else None
    if errors:
        raise Exception(f"fetch_and_store returned errors: {errors}")

    return result


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Hourly DAG to fetch intraday stock data from Yahoo Finance",
    schedule_interval=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    tags=["stocks", "yfinance"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_store_task",
        python_callable=_run_fetch,
        provide_context=True,
    )

    fetch_task
