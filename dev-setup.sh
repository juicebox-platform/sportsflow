#!/bin/bash

uv sync
source .venv/bin/activate

# local airflow env vars
export AIRFLOW_HOME=$(pwd)/airflow-local
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW_ENV=local

# run standalone for dev
airflow standalone
