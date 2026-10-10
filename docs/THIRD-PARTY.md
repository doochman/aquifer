# Third-party software

AQUIFER's own files (the launcher, Compose files, SQL, DAGs, dashboards seeds, scripts and documentation) are MIT licensed, see
[LICENSE](../LICENSE). AQUIFER does **not** redistribute the software below: Docker pulls the images and `pip` installs the
packages on your machine at run time, under their own licenses. Check each license before you redistribute images, host the
stack for others or build a product on it.

| Component | Used as | License (verify against the version you run) |
| --- | --- | --- |
| Trino | SQL engine | Apache-2.0 |
| Apache Iceberg REST catalog (`tabulario/iceberg-rest`) | Table catalog | Apache-2.0 |
| Apache Airflow | Orchestration | Apache-2.0 |
| Apache Superset | BI | Apache-2.0 |
| Apache Spark | Optional streaming check | Apache-2.0 |
| Apache Flink | Stream processing (`streaming` profile) and its Iceberg, Kafka and Hadoop client jars | Apache-2.0 |
| DuckDB | Embedded SQL engine (CLI binary and `httpfs`/`iceberg` extensions) | MIT |
| dbt-core, dbt-trino | Transformations inside the Airflow image | Apache-2.0 |
| PostgreSQL | Metadata store | PostgreSQL License |
| Prometheus, blackbox exporter | Optional monitoring | Apache-2.0 |
| MinIO | S3-compatible object storage | GNU AGPL-3.0 |
| Grafana OSS | Optional dashboards | GNU AGPL-3.0 |
| Redpanda, Redpanda Console | Kafka-compatible broker and UI | Redpanda Community License / Business Source License (free for non-competing use; read the terms) |
| JupyterLab, pandas, NumPy, matplotlib | Optional notebook profile | BSD-3-Clause / PSF-style (matplotlib) |
| PyIceberg, PyArrow | Optional notebook profile | Apache-2.0 |
| PyYAML | Launcher dependency | MIT |
| FLUID Forge (`reference` extension only) | Data-product tooling | see its package metadata |

Notes:

- AGPL components run as separate containers and are only reached over the network; AQUIFER does not link to them or modify them.
- The Redpanda licenses restrict offering Redpanda itself as a competing hosted service. Running it locally for development is
  their intended use. If that matters for you, swap it for another Kafka-compatible broker in `compose.yaml`.
- Trademarks belong to their owners; AQUIFER is not affiliated with or endorsed by any of these projects.
