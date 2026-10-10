# Extensions

The core stack in `compose.yaml` depends on nothing outside this folder. An **extension** is an extra Compose file
(`compose.<name>.yaml`) that `./lakehouse` layers on top when you enable it:

```yaml
# pattern.yaml
extensions: [reference]
```

or `LAKEHOUSE_EXTENSIONS=reference` in `.env`. The launcher then passes `--file compose.yaml --file compose.reference.yaml
--file .pattern/compose.yaml` to Docker Compose. `./lakehouse lint` validates every combination, and `./lakehouse doctor`
checks the extension's prerequisites.

## Writing your own extension

1. Create `compose.<name>.yaml` next to `compose.yaml`. Compose merges it by key: add environment variables, volumes (merged
   by container path), `depends_on`, new services and volumes. Relative paths resolve from this folder.
2. Register the name in `EXTENSIONS` in `patterns/render.py` and add a case to `patterns/test_pattern.py`.
3. Mount extra DAGs at `/opt/airflow/dags/extensions/<name>` (the empty folder `airflow/dags/extensions/reference` shows
   the pattern: the mount point must exist on the host because the DAG folder is mounted read-only).
4. Ask for extra buckets with `EXTRA_BUCKETS` on the `storage-init` service; they are created with versioning on.
5. Put anything that is not generic (company names, sibling repositories, external volumes) in the extension, never in
   `compose.yaml`. A unit test fails if the core file reaches outside the directory.

## reference: Customer Profile 360 data-product reference

A contract-led deployment of a synthetic *Customer Profile 360* data product from the Telekom data-products repository,
built with FLUID Forge 0.18.1 and a dedicated Jenkins job. It is not needed for anything else in this project.

What it adds:

- the `reference` build target of the Airflow image (FLUID Forge, the profile guards package),
- a Trino file-based access policy (`reference/trino/access-control.properties`) fed by the Jenkins deployment volume, which
  denies undeclared readers of `iceberg.silver.customer_profile_360`,
- a read-only **Customer analytics reader (Trino, governed)** connection and the *Customer Profile 360 - executive view*
  dashboard in Superset,
- the **Customer Profile 360 showcase** service on port 8200,
- the `customer-profile-360` bucket (versioned),
- the `pattern-check` acceptance command (FLUID contract, bundle, plan/apply and dbt reconciliation of a second, synthetic
  inventory-balance product).

Prerequisites (checked by `doctor`):

- `../customer-profile-360-reference` and `../telco-data-products` checked out next to this folder,
- Docker Compose 2.23+ (volume subpaths),
- the external Docker volume `tdp-jenkins_sm_jenkins_home` (override with `JENKINS_REFERENCE_VOLUME`) holding an initialised
  `customer-profile-deployment` directory with `dags`, `policy` and `locks`.

Enable and run:

```sh
# pattern.yaml: extensions: [reference]
./lakehouse doctor
./lakehouse up
./lakehouse pattern-check
```

### Jenkins reference integration

The reference runtime uses Forge 0.18.1 and its public loader and native apply-hook surface; the verified migration is in
`../customer-profile-360-reference/UPGRADE-0.18.1.md`. The dedicated job is
`http://localhost:8196/job/customer-profile-360-local-reference/`; all eleven stages execute, and Forge handles artifact
generation, publication and product-scoped Airflow synchronisation. See `../customer-profile-360-reference/README.md` for the
stage table and job setup.

The stack reads only the `customer-profile-deployment` subdirectories of the Jenkins volume. On first setup run this in the
copied reference directory inside Jenkins, with `CUSTOMER_PROFILE_CANONICAL_CONTRACT` set as documented in the reference:

```sh
python3 scripts/prepare_pipeline_inputs.py
fluid bundle runtime/customer_profile_360/contract.fluid.yaml --env local --out runtime/bundle.tgz
mkdir -p /var/jenkins_home/customer-profile-deployment/dags
python3 scripts/local_runtime.py policy
```

The initialisation needs the pinned `dbt-trino` dependency in the executing Python environment; later builds install it in
their own workspace virtualenv. Forge scheduler sync needs `rsync` on the Jenkins agent. The generated Airflow DAG stays
paused until you enable it.

Trino reloads the product's access rules every second. They deny undeclared readers of the profile table and apply declared
consumer conditions; other tables stay open. User headers are not authenticated, and cloud or regional privacy policies are
not fully modelled. Airflow and the showcase read the atomic `current` release pointer; Jenkins keeps previous clean releases
with file hashes and build provenance. The showcase cannot write the profile table.

### Upgrading from earlier versions of this stack

Before the extension split the reference was part of the core file and the default project name was `tdp-lakehouse`.

- Add `extensions: [reference]` to `pattern.yaml` (or `LAKEHOUSE_EXTENSIONS=reference` to `.env`) to keep the showcase,
  the governed Superset connection and the Jenkins mounts.
- Keep your existing `COMPOSE_PROJECT_NAME` in `.env`; volumes and data are untouched because they are named after it.
  New installs default to `aquifer`.
- Airflow images are now `aquifer-airflow:3.3.2` (core) and `...-reference`; `./lakehouse up` rebuilds them.
- The Jenkins-delivered DAGs are mounted at `/opt/airflow/dags/extensions/reference` instead of `/opt/airflow/dags/jenkins`.
- `./up.sh <flag>` still works and forwards to `./lakehouse`.
