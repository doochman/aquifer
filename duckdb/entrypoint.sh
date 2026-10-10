#!/bin/sh
# Renders the DuckDB start-up script from the environment, then idles so `./lakehouse duckdb` can exec into it.
set -eu
umask 077
sed -e "s|__MINIO_USER__|${MINIO_ROOT_USER}|g" -e "s|__MINIO_PASSWORD__|${MINIO_ROOT_PASSWORD}|g" \
  /opt/aquifer/duckdb/init.sql.template > /home/duck/init.sql
exec sleep infinity
