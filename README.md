# Airflow + Yahoo Finance pipeline (Part 1 - scaffold)

This repository provides a Dockerized local development scaffold for an Airflow-based pipeline that will fetch stock data (Yahoo Finance) and persist it to PostgreSQL.

## What Part 1 gives you
- Docker Compose setup with:
  - Postgres (database for pipeline data)
  - Airflow (webserver + scheduler) running from a custom image that includes Python dependencies
- `dags/` and `dags/scripts/` directories where DAGs and scripts will live (Part 2)
- `.env.example` for local environment variables

## Prerequisites
- Docker & Docker Compose installed
- At least ~2GB free space (images and packages)
- Network access (Airflow containers will fetch packages / and later yfinance will call Yahoo)

## Quickstart (local)
1. Copy the environment example:
   ```bash
   cp .env.example .env

