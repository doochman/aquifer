ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11

# Core image: Airflow with the Trino client, S3 access and an isolated dbt (Trino adapter).
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS base

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

RUN pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
        "trino==0.339.0" \
        "boto3==1.43.75"

RUN python -m venv /home/airflow/.dbt-venv \
        && /home/airflow/.dbt-venv/bin/pip install --no-cache-dir \
                "dbt-core==1.10.6" \
                "dbt-trino==1.10.6" \
                "trino==0.339.0"

# Reference extension image: adds FLUID Forge and the Customer Profile 360 guards.
# Only built when compose.reference.yaml selects `target: reference`.
FROM base AS reference

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

RUN pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
        "data-product-forge[local]==0.18.1"

COPY --from=guards --chown=airflow:root pyproject.toml /tmp/customer-profile-guards/pyproject.toml
COPY --from=guards --chown=airflow:root scripts/source_evidence.py /tmp/customer-profile-guards/scripts/source_evidence.py
RUN pip install --no-cache-dir --no-deps /tmp/customer-profile-guards
