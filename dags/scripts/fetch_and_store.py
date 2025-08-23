"""
dags/scripts/fetch_and_store.py

Fetch stock data from Yahoo Finance (yfinance), normalize it, ensure the Postgres
table exists, and upsert rows using INSERT ... ON CONFLICT.

Safe to import (no side-effects on import). Execution and CLI behavior live in
the `if __name__ == "__main__":` block so you can run:

    uv run -m dags.scripts.fetch_and_store AAPL MSFT
    python -m dags.scripts.fetch_and_store AAPL MSFT
"""

from __future__ import annotations
import os
import sys
import time
import logging
from typing import List, Optional, Dict, Any
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# Module-level logger; configured when run from __main__
logger = logging.getLogger("fetch_and_store")

# Defaults (overridable via env)
DEFAULT_FETCH_ATTEMPTS = 3
DEFAULT_FETCH_BACKOFF_SECONDS = 2.0
DEFAULT_DB_CONNECT_RETRIES = 3
DEFAULT_DB_CONNECT_BACKOFF = 2.0


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def get_db_url() -> str:
    POSTGRES_HOST = get_env("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = int(get_env("POSTGRES_PORT", "5432"))
    POSTGRES_DB = get_env("POSTGRES_DB", "stocks_db")
    POSTGRES_USER = get_env("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def get_engine(
    db_connect_retries: int = DEFAULT_DB_CONNECT_RETRIES,
    db_connect_backoff: float = DEFAULT_DB_CONNECT_BACKOFF,
) -> Engine:
    """Create SQLAlchemy engine with retries for transient DB startup issues."""
    url = get_db_url()
    last_exc: Optional[Exception] = None
    for attempt in range(1, db_connect_retries + 1):
        try:
            engine = create_engine(url, pool_pre_ping=True)
            # quick connection test
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Connected to Postgres (attempt %d/%d)", attempt, db_connect_retries)
            return engine
        except OperationalError as e:
            last_exc = e
            wait = db_connect_backoff * attempt
            logger.warning("DB connection attempt %d failed: %s — retrying in %.1fs", attempt, e, wait)
            time.sleep(wait)
    logger.exception("Failed to connect to Postgres after %d attempts", db_connect_retries)
    raise last_exc


def ensure_table(engine: Engine, table_name: str) -> None:
    """Create the stock table if it does not exist."""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      id SERIAL PRIMARY KEY,
      ticker VARCHAR(16) NOT NULL,
      ts TIMESTAMP NOT NULL,
      open NUMERIC,
      high NUMERIC,
      low NUMERIC,
      close NUMERIC,
      volume BIGINT,
      created_at TIMESTAMP DEFAULT now(),
      UNIQUE (ticker, ts)
    );
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info("Ensured table '%s' exists", table_name)
    except SQLAlchemyError:
        logger.exception("Error ensuring table exists: %s", table_name)
        raise


def _normalize_history_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize yfinance history DataFrame to columns: open, high, low, close, volume."""
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    df = raw_df.copy()
    df = df.rename(columns={
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"
    })
    df.columns = [c.lower() for c in df.columns]
    keep_cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep_cols]
    df = df.dropna(how="all")
    return df


def fetch_latest_for_ticker(
    ticker: str,
    fetch_attempts: int = DEFAULT_FETCH_ATTEMPTS,
    fetch_backoff_seconds: float = DEFAULT_FETCH_BACKOFF_SECONDS,
) -> pd.DataFrame:
    """Fetch the most recent bars for `ticker` (intraday first, fallback to daily)."""
    if not ticker or not isinstance(ticker, str):
        raise ValueError("Invalid ticker supplied")

    ticker = ticker.strip().upper()
    last_exc: Optional[Exception] = None
    for attempt in range(1, fetch_attempts + 1):
        try:
            tk = yf.Ticker(ticker)
            logger.info("Fetching intraday for %s (attempt %d/%d)", ticker, attempt, fetch_attempts)
            df = None
            try:
                df = tk.history(period="1d", interval="5m", actions=False)
                if df is None or df.empty:
                    logger.info("Intraday empty for %s; fetching daily", ticker)
                    df = tk.history(period="7d", interval="1d", actions=False)
            except Exception as e:
                logger.warning("yfinance intraday fetch error for %s: %s", ticker, e)
                df = tk.history(period="7d", interval="1d", actions=False)

            df = _normalize_history_df(df)
            if df.empty:
                raise RuntimeError(f"No data available for ticker {ticker}")

            logger.info("Fetched %d rows for %s", len(df), ticker)
            return df

        except Exception as e:
            last_exc = e
            backoff = fetch_backoff_seconds * attempt
            logger.warning("Fetch attempt %d for %s failed: %s — retry in %.1fs", attempt, ticker, e, backoff)
            time.sleep(backoff)

    logger.exception("All fetch attempts failed for %s", ticker)
    raise last_exc


def upsert_rows(engine: Engine, table_name: str, ticker: str, df: pd.DataFrame) -> int:
    """Upsert the rows from df into the Postgres table using ON CONFLICT."""
    if df is None or df.empty:
        logger.info("No data to upsert for %s", ticker)
        return 0

    rows = []
    for ts, row in df.iterrows():
        try:
            ts_val = pd.to_datetime(ts).to_pydatetime()
            rows.append({
                "ticker": ticker,
                "ts": ts_val,
                "open": None if pd.isna(row.get("open")) else float(row.get("open")),
                "high": None if pd.isna(row.get("high")) else float(row.get("high")),
                "low": None if pd.isna(row.get("low")) else float(row.get("low")),
                "close": None if pd.isna(row.get("close")) else float(row.get("close")),
                "volume": None if pd.isna(row.get("volume")) else int(row.get("volume")),
            })
        except Exception:
            logger.exception("Skipping invalid row for %s at index %s", ticker, ts)

    if not rows:
        logger.info("No valid rows extracted for %s", ticker)
        return 0

    insert_sql = f"""
    INSERT INTO {table_name} (ticker, ts, open, high, low, close, volume)
    VALUES (:ticker, :ts, :open, :high, :low, :close, :volume)
    ON CONFLICT (ticker, ts) DO UPDATE
      SET open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          volume = EXCLUDED.volume;
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(insert_sql), rows)
        logger.info("Upserted %d rows for %s", len(rows), ticker)
        return len(rows)
    except SQLAlchemyError:
        logger.exception("Database error during upsert for %s", ticker)
        raise


def fetch_and_store(
    tickers: List[str],
    table_name: Optional[str] = None,
    fetch_attempts: Optional[int] = None,
    fetch_backoff_seconds: Optional[float] = None,
    db_connect_retries: Optional[int] = None,
    db_connect_backoff: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Main entrypoint for Airflow DAG or direct invocation.
    Returns a summary dict with per-ticker counts/errors.
    """
    table_name = table_name or get_env("STOCK_TABLE") or "stock_prices"
    fetch_attempts = fetch_attempts or int(get_env("FETCH_ATTEMPTS", DEFAULT_FETCH_ATTEMPTS))
    fetch_backoff_seconds = float(get_env("FETCH_BACKOFF_SECONDS", fetch_backoff_seconds or DEFAULT_FETCH_BACKOFF_SECONDS))
    db_connect_retries = int(get_env("DB_CONNECT_RETRIES", db_connect_retries or DEFAULT_DB_CONNECT_RETRIES))
    db_connect_backoff = float(get_env("DB_CONNECT_BACKOFF", db_connect_backoff or DEFAULT_DB_CONNECT_BACKOFF))

    logger.info("Starting fetch_and_store for tickers=%s", tickers)
    summary: Dict[str, Any] = {"success": {}, "errors": {}}

    engine = get_engine(db_connect_retries=db_connect_retries, db_connect_backoff=db_connect_backoff)
    ensure_table(engine, table_name)

    for ticker in tickers:
        ticker = ticker.strip().upper()
        try:
            df = fetch_latest_for_ticker(ticker, fetch_attempts, fetch_backoff_seconds)
            count = upsert_rows(engine, table_name, ticker, df)
            summary["success"][ticker] = count
        except Exception as e:
            logger.exception("Failed processing ticker %s", ticker)
            summary["errors"][ticker] = str(e)
            continue

    logger.info("Completed fetch_and_store: %s", summary)
    return summary


# -----------------------------------------------------------------------------
# Runtime / CLI entrypoint — everything below runs only when executed directly
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Local-only imports
    from dotenv import load_dotenv
    import argparse

    # Load .env for local runs (if present)
    load_dotenv()

    # Configure basic logging for CLI runs (avoid double handlers)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(get_env("LOG_LEVEL", "INFO"))

    # CLI parsing
    parser = argparse.ArgumentParser(
        description="Fetch stock data from Yahoo Finance and upsert to Postgres."
    )
    parser.add_argument("tickers", nargs="*", help="List of tickers, e.g. AAPL MSFT. If omitted, uses TICKERS env var.")
    parser.add_argument("--table", "-t", help="Table name to upsert into (overrides STOCK_TABLE env var).")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but don't write to DB (for quick tests).")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel("DEBUG")

    # Determine tickers: CLI args first, then env var
    if args.tickers:
        tickers_list = [t.strip().upper() for t in args.tickers if t.strip()]
    else:
        env_tickers = get_env("TICKERS", "")
        if not env_tickers:
            logger.error("No tickers provided on CLI and TICKERS env var is empty.")
            parser.print_help()
            sys.exit(1)
        tickers_list = [t.strip().upper() for t in env_tickers.split(",") if t.strip()]

    table_name = args.table or get_env("STOCK_TABLE", "stock_prices")

    # If dry-run: fetch for each ticker and print counts without DB writes
    if args.dry_run:
        logger.info("Running in dry-run mode. Will fetch but not write to DB.")
        for t in tickers_list:
            try:
                df = fetch_latest_for_ticker(t)
                logger.info("Ticker %s fetched %d rows (dry-run)", t, len(df))
            except Exception:
                logger.exception("Dry-run fetch failed for %s", t)
        logger.info("Dry-run complete.")
        sys.exit(0)

    # Non-dry run: call fetch_and_store and print summary
    try:
        result = fetch_and_store(tickers_list, table_name=table_name)
        logger.info("Result: %s", result)
        # Exit code 0 means success, but if there were per-ticker errors, exit 2
        if result.get("errors"):
            logger.warning("Some tickers failed: %s", list(result["errors"].keys()))
            sys.exit(2)
        sys.exit(0)
    except Exception as e:
        logger.exception("Script failed: %s", e)
        sys.exit(1)
