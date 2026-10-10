FROM python:3.12-slim

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN pip install \
        "jupyterlab==4.3.5" \
        "pandas==2.2.3" \
        "matplotlib==3.10.0" \
        "pyarrow==19.0.0" \
        "trino==0.339.0" \
        "duckdb==1.3.2" \
        "pyiceberg[pyarrow]==0.9.1"

# DuckDB extensions are baked in (world-readable) so the notebooks work offline for any container user.
RUN python -c "import duckdb; c = duckdb.connect(); c.sql(\"SET extension_directory='/opt/duckdb/extensions'\"); c.sql('INSTALL httpfs'); c.sql('INSTALL iceberg'); c.sql('INSTALL avro')" \
    && chmod -R a+rX /opt/duckdb

COPY aquifer_helpers.py /opt/aquifer/aquifer_helpers.py
ENV PYTHONPATH=/opt/aquifer \
    HOME=/tmp/home \
    JUPYTER_RUNTIME_DIR=/tmp/jupyter/runtime \
    JUPYTER_DATA_DIR=/tmp/jupyter/data \
    JUPYTER_CONFIG_DIR=/tmp/jupyter/config \
    IPYTHONDIR=/tmp/ipython \
    MPLCONFIGDIR=/tmp/matplotlib
WORKDIR /work
CMD ["sh", "-c", "mkdir -p /tmp/home && exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=\"$NOTEBOOK_TOKEN\" --ServerApp.root_dir=/work --ServerApp.terminals_enabled=False"]
