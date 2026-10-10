FROM debian:bookworm-slim

ARG DUCKDB_VERSION=1.3.2
ARG TARGETARCH

# The CLI is a single static binary; extensions are baked in so the shell works offline.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
         amd64) asset=linux-amd64 ;; \
         arm64) asset=linux-arm64 ;; \
         *) echo "unsupported architecture ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-${asset}.zip" \
    && unzip -q /tmp/duckdb.zip -d /usr/local/bin \
    && rm /tmp/duckdb.zip \
    && chmod 0755 /usr/local/bin/duckdb

RUN useradd --create-home --uid 10001 duck
USER duck
ENV HOME=/home/duck
RUN duckdb -c "INSTALL httpfs; INSTALL iceberg; INSTALL avro;"

COPY --chown=duck:duck entrypoint.sh init.sql.template /opt/aquifer/duckdb/
ENTRYPOINT ["sh", "/opt/aquifer/duckdb/entrypoint.sh"]
