#!/bin/sh
# Submits the streaming job once. Safe to run on every `up`: it does nothing when the job already runs.
set -eu

JOB="aquifer-orders-to-iceberg"
REST="http://flink-jobmanager:8081"

until curl -fsS "$REST/overview" > /dev/null 2>&1; do
  echo "waiting for the Flink JobManager ..."
  sleep 2
done

if curl -fsS "$REST/jobs/overview" | tr '{' '\n' | grep -q "\"name\":\"$JOB\".*\"state\":\"RUNNING\""; then
  echo "job $JOB is already running"
  exit 0
fi

# The single quotes are intentional: envsubst receives the variable names to replace, the shell must not expand them.
# shellcheck disable=SC2016
envsubst '${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}' < /opt/aquifer/flink/sql/orders_stream.sql > /tmp/orders_stream.sql
/opt/flink/bin/sql-client.sh -f /tmp/orders_stream.sql
echo "submitted $JOB"
