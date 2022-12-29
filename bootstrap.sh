#! /bin/bash

# Alpha Vantage API key
API_KEY="7ZYTGG8N0LLVQ9FJ"
API_INT="1min"
PG_HOST="host.docker.internal"
PG_USER="postgres"
PG_PASS="postgres"
PG_PORT="5430"
PG_DB="stocks"
TICKERS="AAPL GOOGL MSFT IBM NVDA ORCL INTC QCOM AMD TSM"

echo "Create and run docker containers"
docker-compose -f docker/docker-compose.yaml up

echo "Create Airflow variables"
docker exec postgres_airflow-worker_1 airflow variables set apikey "$API_KEY"
docker exec postgres_airflow-worker_1 airflow variables set apiint "$API_INT"
docker exec postgres_airflow-worker_1 airflow variables set tickers "$TICKERS"
docker exec postgres_airflow-worker_1 airflow connections add "pg_conn" --conn-uri "postgres://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB"

echo "Copying DAGs"
cp scripts/* docker/dags
