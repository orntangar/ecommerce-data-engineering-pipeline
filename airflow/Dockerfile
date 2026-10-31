
FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*


USER airflow

RUN pip install --no-cache-dir "psycopg[binary]>=3.1,<4"

RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir \
       "dbt-core==1.12.6" "dbt-postgres==1.11.0" \
    && ln -sf /opt/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
