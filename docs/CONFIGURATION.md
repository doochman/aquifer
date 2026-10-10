# Configuration

Settings come from three layers; later layers win:

1. **`pattern.yaml`** (committed): preset, optional profiles, extensions, port and resource defaults for everyone.
2. **`.env`** (yours, never committed): created by `./lakehouse init` with generated credentials and any port that had
   to move. Docker Compose reads it too.
3. **Environment variables** of the shell that runs the command.

> A variable such as `COMPOSE_PROJECT_NAME` or `HOST_TRINO_PORT` that is still exported in your shell from an earlier
> session overrides `.env`. If the launcher talks to the wrong stack, run `env | grep -E 'COMPOSE|HOST_'`.

`./lakehouse pattern` prints the effective non-secret result.

## pattern.yaml

```yaml
version: 1
pattern: local-lakehouse
preset: small            # small | medium
profiles: [analytics, streaming]   # analytics | streaming | observability | spark | notebook
extensions: []           # reference
ports: {}                # per service, for example {grafana: 8391}
resources: {}            # for example {trino_memory_mb: 4096}
```

| Preset | Trino container / heap | Trino CPUs | Airflow scheduler | Airflow parallelism | Suggested Docker memory |
| --- | --- | --- | --- | --- | --- |
| `small` | 3072 / 2048 MB | 2 | 3072 MB | 2 | 8 GB |
| `medium` | 6144 / 4096 MB | 4 | 4096 MB | 4 | 16 GB |

The renderer validates everything (unique ports 1024-65535, enough JVM headroom, scheduler memory for the worker budget)
and generates `.pattern/` (git-ignored). Design notes: [patterns/README.md](../patterns/README.md).

## Profiles

| Profile | Adds | Start |
| --- | --- | --- |
| *(core, always)* | PostgreSQL, MinIO, Iceberg REST, Trino, DuckDB, Redpanda + Console, Airflow (with dbt Core) | `./lakehouse up` |
| `analytics` | Apache Superset 4.1.1 (about 0.5 GB used, 2 GB cap) | default; or `up --with analytics` |
| `streaming` | Apache Flink 1.20: JobManager, TaskManager and the job submitter, plus the Kafka topic `shop.orders` (about 1.1 GB used) | default; or `up --with streaming` |
| `observability` | Prometheus 2.51, blackbox exporter, Grafana 11.1 (readiness and Flink job health) | `up --with observability` |
| `spark` | Spark 3.5.3 local worker for `spark-check` | `up --with spark` |
| `notebook` | JupyterLab 4 with pandas, PyIceberg, DuckDB and helpers (`query()`, `duck()`, `catalog()`); files live in `notebooks/`; login by token (shown by `info`) | `up --with notebook`, `open notebook` |

`--with` is per run; put a profile in `pattern.yaml` to make it permanent, or set `COMPOSE_PROFILES` in `.env`.

## .env reference

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMPOSE_PROJECT_NAME` | `aquifer` | Names containers and volumes; change it to run a second copy |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `localadmin` / generated | MinIO console and S3 keys (also used by Trino and the catalog) |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | `admin` / generated | Airflow UI |
| `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` | `admin` / generated | Superset login |
| `SUPERSET_SECRET_KEY` | generated | Superset session signing |
| `AIRFLOW_UID` | `50000` | User id of the Airflow containers |
| `HOST_<NAME>_PORT` | see below | Host port per service (`TRINO`, `AIRFLOW`, `CONSOLE`, `MINIO_API`, `MINIO_CONSOLE`, `REGISTRY`, `KAFKA`, `SUPERSET`, `FLINK`, `NOTEBOOK`, `PROMETHEUS`, `GRAFANA`, `SHOWCASE`) |
| `TRINO_MEMORY_LIMIT`, `TRINO_CPUS` | from preset | Override Trino limits (`4g`, `2`) |
| `AIRFLOW_SCHEDULER_MEMORY`, `AIRFLOW_PARALLELISM` | from preset | Override Airflow limits |
| `SUPERSET_MEMORY_LIMIT` | `2g` | Superset container cap |
| `NOTEBOOK_TOKEN` | generated | Login token for JupyterLab |
| `NOTEBOOK_UID` | your user id | Runs the notebook container as you, so `notebooks/` stays writable |
| `NOTEBOOK_MEMORY_LIMIT` | `2g` | JupyterLab container cap |
| `COMPOSE_PROFILES` | from `pattern.yaml` | Comma separated profiles |
| `LAKEHOUSE_EXTENSIONS` | from `pattern.yaml` | Comma separated extensions |

Default ports (moved automatically by `init` when taken): Trino 8093, Airflow 8095, Redpanda Console 8094, MinIO API 9000,
MinIO console 9001, Schema Registry 18081, Kafka 19092, Superset 8280, Flink 8283, Notebook 8284, Prometheus 8290, Grafana 8291.

Passwords are generated once. Changing one in `.env` later does not rotate data that already exists in a volume (for example
the Airflow login stored in its metadata); `./lakehouse reset` gives a clean start. `./lakehouse init --force` writes a
fresh `.env` and keeps the previous one as `.env.bak-<timestamp>`.

Internal service-to-service passwords (PostgreSQL roles used by Airflow, Superset and the catalog) are fixed lab values
inside the Docker network; they are not published to the host.

## Analytics access model

Superset is seeded with one connection, **Lakehouse (Trino, general)**, a read-only connection that identifies itself as
`superset-analyst`. Data manipulation, CTAS and file upload are disabled. Anyone who can sign in to Superset can query
everything that identity can read, because per-user impersonation is not enabled. Trino itself has no authentication here.
Treat the whole stack as single-user.

## Observability

`up --with observability` adds Prometheus (7 days or 1 GB retention) and Grafana with an anonymous read-only viewer on
loopback. The dashboard `http://localhost:8291/d/local-lakehouse` shows service readiness, probe latency and history. Readiness
probes reject Trino's `starting: true` and unhealthy Airflow components even when HTTP returns 200. Five alert rules
exist (readiness or scrape failure, stale or empty data products, quality violations); no notification receiver is
configured, so they are visible in Prometheus only.

## Streaming

The `streaming` profile runs a bounded Spark 3.5.3 structured-streaming check (`./lakehouse spark-check`): non-root user,
1 GB driver heap, 3 GB cap and a durable checkpoint volume. It appends five rows per run and asserts checkpoint recovery
and unique ids. It does **not** yet read Redpanda or write Iceberg; Kafka and Iceberg connector dependencies, a governed
event contract, quarantine, replay and late-event semantics are still required for that.

## Resource limits

Trino has a fixed heap, container cap and CPU limit instead of sizing itself to the whole Docker VM; Airflow's API server
and DAG processor are capped at 1 GB and the scheduler at the preset value; every other service has its own cap. Logs
rotate at 10 MB with three files. These are laptop budgets, not capacity guarantees.
