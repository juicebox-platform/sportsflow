FROM apache/airflow:3.0.2-python3.12

USER root
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root src/ /tmp/sportsflow/src/
COPY --chown=airflow:root pyproject.toml /tmp/sportsflow/

RUN pip install /tmp/sportsflow/ && rm -rf /tmp/sportsflow/

WORKDIR /opt/airflow
