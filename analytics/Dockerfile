FROM apache/superset:4.1.1

USER root
# The upstream image ships neither a PostgreSQL driver nor a Trino dialect.
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10" "trino[sqlalchemy]==0.339.0"
COPY --chown=superset:superset superset_config.py /app/pythonpath/superset_config.py
COPY --chown=superset:superset bootstrap.py verify.py /app/analytics/
USER superset
