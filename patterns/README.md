# Local Lakehouse Pattern v1

This is a tested localhost implementation pattern, not a new FLUID dialect or
a production certification. Platform configuration is in `../pattern.yaml`;
product facts remain in FLUID contracts and overlays.

## Three Ownership Layers

| Layer | Source of truth |
| --- | --- |
| Platform | Versioned `pattern.yaml`, service definitions and monitoring templates |
| Product | FLUID identity, schema, bindings, builds, selectors and schedule |
| Execution | Referenced dbt project, models and domain-specific tests |

The renderer does not duplicate product schema, ownership or consent semantics.
Forge remains the native build runner and artifact generator.

## Configure and Inspect

Host rendering needs Python 3.8+ and PyYAML. `./lakehouse` installs PyYAML into a
private `.pattern/venv` on first use, so no global installation or interpreter
change is needed. To run the unit tests yourself, install it with
`python -m pip install -r patterns/requirements.txt`. Docker acceptance uses the
existing pinned Airflow image.

```sh
./lakehouse pattern
./lakehouse doctor
./lakehouse pattern-check
```

Run from the local-stack directory. `pattern` shows the effective non-secret
configuration, including `.env` overrides, without restarting anything.
`pattern-check` needs the `reference` extension (see `docs/EXTENSIONS.md`); it uses the running Airflow image and its pinned dbt adapter to
strictly validate the contract and bundle, generate managed dbt tests and standard
artifacts, validate their manifest, plan/apply, and run native dbt reconciliation.
A separately labeled supplemental probe checks the missing native Iceberg storage
capability. Unsupported native storage is reported as unsupported, not as passed.
It writes only `iceberg.demo.inventory_balance` and is intended for synthetic
acceptance, not arbitrary production products.

```yaml
version: 1
pattern: local-lakehouse
preset: small
profiles: [analytics]
extensions: []
ports:
  grafana: 8291
resources: {}
```

Supported presets:

| Setting | Small | Medium |
| --- | --- | --- |
| Trino container / heap | 3072 / 2048 MB | 6144 / 4096 MB |
| Trino CPU | 2 | 4 |
| Airflow scheduler memory | 3072 MB | 4096 MB |
| Default task parallelism | 2 | 4 |

The small preset has live execution evidence. Medium has configuration and
budget-coherence tests, not load/capacity certification. Resource fields can be
overridden explicitly; invalid names, duplicate ports, unsafe heap headroom,
and excessive concurrency fail before Docker actions. Scheduler budgeting
reserves 1024 MB plus 512 MB per worker; this is a conservative recipe, not a
guarantee for arbitrary jobs.

The launcher renders `.pattern/defaults.env`, `.pattern/compose.yaml`, a JVM
configuration and a resolved report. These are deterministic generated files,
ignored by source control. The JVM file and container budget come from the same
preset. A deterministic fingerprint changes Compose's service definition when
the JVM/resource settings change, so a subsequent explicit `./lakehouse up` recreates
Trino rather than silently retaining an old mounted-file configuration.
`.env` takes precedence; effective overrides are also validated. Secrets
must stay outside `pattern.yaml` and are never included in the resolved report.

## Acceptance Evidence

- Customer Profile: 41 columns, synthetic snapshots, seed dependency, consent
  gates and incremental native dbt build.
- Inventory example: 3 columns, 3 synthetic rows, no consent/PII/seed dependency,
  native table build and 4 dbt data tests.
- The same contract-derived target verifier checks both catalogs/schemas/tables;
  the inventory result is `iceberg.demo.inventory_balance`, not a customer alias.
- Pattern tests cover deterministic rendering, presets and effective overrides.
- SQL identifiers are validated and metadata filters are parameterized.

## Forge-First Ownership

| Behavior | Owner and verified interface |
| --- | --- |
| Contract/overlay validation | `fluid validate`; no replacement schema validator |
| Bundle/integrity of native artifacts | `fluid bundle`, bundle validation, `fluid validate-artifacts` |
| dbt quality generation | `fluid generate dbt-tests` from FLUID `contract.dq.rules` |
| Standards fanout | `fluid generate artifacts` for ODCS/ODPS |
| Planning and build execution | Native `fluid plan` and plan-bound `fluid apply` |
| dbt schema/run evidence | `fluid verify --reconcile-dbt --strict --fail-on-warning` |
| CI defaults | Native generator flags for publish, scheduler and destination |
| Scheduling and catalog publication | Native generated DAGs, `schedule-sync`, and `publish` |
| Local service ports/resource profiles | Compose and the platform renderer, not a product DSL |
| Live Iceberg storage check | Temporary supplemental Trino bridge: native verify reports unsupported |
| Customer-specific file policy | Temporary local bridge: native compilation selects the wrong provider |
| Read-only release/freshness preflight | Platform input handling, not a replacement build engine |

The inventory model's business logic remains a referenced hybrid-reference dbt
artifact. Its test schema is managed by Forge. The acceptance command generates
tests only with explicit `--generate-dbt-tests`, in a temporary copy; native Forge
refuses to overwrite an unmanaged file. Do not bypass that protection or put a
second legacy quality block into the contract.

The native `fluid test` probe was also executed: Forge 0.17.0 treats this local
Iceberg binding as a file and fails with `File 'unknown' does not exist`. That
command cannot presently replace the storage bridge. Native IaC generation lists
cloud/confluent targets, not this shared Docker lakehouse recipe. Native `config`
sets provider/project/region context; it does not configure these services or
their joint JVM/worker budgets. These observed gaps justify the remaining platform
code; they do not justify a second workflow engine.

Supplemental schema checking is deliberately scalar-only and retains this
reference's NUMERIC-to-double compatibility choice. Arrays are not silently
accepted as strings. It is not a complete replacement for native provider type,
nullability, precision, key, policy, or table-property verification. Retire each
supplement when the official provider passes the same positive/negative tests.

## Deliberate Limits

Version 1 supports a single local Iceberg expose for live schema verification.
The shared verifier is currently supplied from the reference's `local_runtime.py`
through the acceptance command's explicit `--verifier` interface. Extracting it
into a distributable platform package is future work, not hidden duplication.
The existing stack still uses the reference's initialized Jenkins deployment
volume; this does not yet provide independent, clean-clone onboarding for every
product. The customer explorer, policy mapping, source-evidence thresholds and
release layout are not claimed to be generic.

The pattern adds no default production IAM, four-hour freshness rule, synthetic
source waiver, real-source connector, or automatic Spark ingestion. Those are
product/platform decisions requiring explicit evidence. Native Iceberg verification
and policy gaps are still covered by the separate Forge requirements proposal.