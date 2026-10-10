# Production Readiness

Status: **NO-GO for real customer data or production exposure**. A successful
local acceptance test is not production approval. This single-host stack can
exercise production patterns, but cannot demonstrate resilience across fault
domains. Do not expose its ports through a tunnel, reverse proxy, LAN binding,
or shared host without first establishing authenticated access.

## Verified Controls

- Published ports bind to loopback; platform and query resource budgets are bounded.
- Airflow, Trino, the showcase and Prometheus drop all Linux capabilities and
  prohibit gaining privileges. Blackbox also prohibits gaining privileges.
  Initialization services retain their necessary permissions.
- Startup and status fail closed on Trino `starting` and on unhealthy or missing
  Airflow database, scheduler or DAG-processor health. Requests have a three-second timeout.
- Native Airflow smoke execution writes and reads Iceberg through the catalog,
  MinIO and Trino after the privilege-hardening rollout.
- Catalog recovery uses native PostgreSQL tools, not a new orchestration engine.
  FLUID Forge retains ownership of product validation, builds and scheduler sync.

## Catalog Restore Drill

```sh
./lakehouse catalog-restore-check
```

Run while catalog writers are quiescent. The drill exports the live `iceberg`
database using `pg_dump`, restores it with `pg_restore --exit-on-error` into
an ephemeral PostgreSQL instance, and compares every field of the Iceberg
table-registration rows. Empty catalogs fail rather than giving vacuous success.
Concurrent drills using the same stack directory are rejected.

The restore instance has no network, no published ports and tmpfs-only database
storage. Its trust authentication is restricted to this disposable instance.
The temporary dump directory is private (umask 077). The dump, restore container
and lock are removed on normal exit or command failure. After a forced kill,
verify that no drill is running, then remove its stale lock/container/private
temporary directory explicitly. Never use `--reset` as cleanup.

This proves restoration of catalog metadata, not a recoverable lakehouse. It does
not preserve object-store data, PostgreSQL roles/ACLs, Airflow state, broker logs,
Jenkins releases, credentials, or encryption keys. There is no retained backup
artifact, off-host copy, measured RPO/RTO, or disaster-recovery claim.

## Local TLS And Identity Acceptance

```sh
./lakehouse security-check
```

Start the normal stack and deploy the synthetic profile first. This opt-in check
uses an additional 3 GB-capped Trino process on loopback port 8493 (override
`HOST_TRINO_SECURE_PORT` if needed). It does not change port 8093, existing clients
or generated Forge jobs. It stops the candidate after success or failure.

The candidate uses native Trino password-file authentication, bcrypt cost 10,
explicit no-impersonation rules, read-only catalog permissions, HTTPS-only service
and internal communication, a random shared secret, and a private local CA.
Completed SDK queries prove trusted reads and denied impersonation/writes.
Negative checks also reject missing/incorrect passwords and an untrusted CA.
The unauthenticated `/v1/info` endpoint is public by Trino design; authentication
is tested on protected SQL endpoints, not inferred from a health response.

Keys/passwords are generated outside the workspace at
`$HOME/.local/share/local-lakehouse-security/<project>` with private permissions.
Only runtime key material, the public CA and password hashes are copied into
the candidate's read-only Docker volume. The CA signing key is not mounted in
Trino. The verification client receives only its CA and two test identities.
Passwords are not printed, committed or passed as command-line arguments.
Host and Docker administrators remain trusted. This is not a corporate CA or
external secret manager.

The server certificate lasts 90 days; preparation refuses certificates expiring
within 24 hours. Renewal and credential rotation require explicit operations;
there is no silent rotation of live secrets. Do not delete the private directory
to rotate credentials while clients still use it. Custom `LOCAL_SECURITY_ROOT`
locations must be outside the workspace. Keep this directory in protected backup.

**Rollout is not complete:** the default Trino endpoint still uses HTTP and
unauthenticated headers. dbt, Forge-generated Jenkins jobs, Airflow, showcase and
monitoring clients must be migrated and tested together before retiring it.
MinIO/catalog/broker links and identities remain separate unfinished gates.

## Analytics Profile (Superset)

The `analytics` profile (`./lakehouse up --with analytics`, on by default in `pattern.yaml`) is a convenience for
exploring synthetic data and carries the same NO-GO status:

- Superset authenticates its own users with a local database login and a
  predictable admin password; there is no identity provider, SSO or MFA.
- Both Trino connections use a fixed identity over plaintext HTTP. Per-user
  impersonation is off, so Superset roles, not Trino access rules, decide who
  can query which dataset. Anyone who can sign in can use the governed
  connection to read `customer_profile_360`, which the file rules otherwise deny
  to undeclared readers. Do not point it at real customer data.
- The Superset secret key is a lab default. Metadata lives in the shared lab
  PostgreSQL, is not part of the catalog restore drill and has no backup.
- Caching is a local filesystem cache; there is no worker, alerting or report
  delivery, and `--analytics-check` proves query execution through the API, not
  browser behaviour at scale.

## Streaming, transformation and embedded engines

The `streaming` profile (Flink), dbt Core and DuckDB are verified by `./lakehouse check` on a laptop and carry the same NO-GO
status:

- Flink runs a single session cluster without high availability. Checkpoints live in the container, so a JobManager restart
  loses job state; the job is resubmitted and resumes from the committed Kafka offsets. Delivery to Iceberg is at-least-once, bad
  records are dropped without a dead-letter queue, and the Flink UI and REST API have no authentication (loopback only).
- Kafka (Redpanda) has no authentication or encryption, and the MinIO and Iceberg catalog credentials are passed to Flink and
  DuckDB through the container environment.
- dbt connects to Trino as an unauthenticated user over HTTP; its tests are data-quality checks, not governance controls.
- DuckDB receives the object-store keys in a generated start-up file inside its container (mode 600).

## Release Gates

| Gate | Required Evidence | Current State |
| --- | --- | --- |
| Identity | Trino, UI and API identities verified against an owned identity provider; negative impersonation tests | Local TLS candidate passes native auth/impersonation tests; default endpoint and dashboards remain unauthenticated |
| Secrets | External secret ownership, file/provider injection, least-privilege service accounts and tested rotation | Predictable lab defaults; root MinIO credentials reused |
| Transport | TLS with certificate validation for clients and service links; failed/untrusted certificates rejected | Candidate TLS/query rejection verified; default clients and storage links remain plaintext |
| Isolation | Explicit network boundaries, deny-by-default access, authenticated broker/catalog/object store | Flat local network; Kafka has no authentication |
| Recovery | Consistent catalog plus object snapshot, immutable encrypted off-host backups and clean-room query restoration within agreed RPO/RTO | Catalog-only restore verified; other recovery unproven |
| Availability | Multiple independent fault domains and tested failure/maintenance behavior | All state on one Docker host |
| Operations | Delivered alert, acknowledgement, escalation, incident and restore runbooks with named owners | Metrics and alert rules; no notification receiver |
| Supply Chain | Supported component versions, digest pins, vulnerability policy, signed native Forge artifacts and verified trust roots | Version tags and corruption hashes, not signer authenticity |
| Capacity | Representative load/soak results, concurrency limits, disk pressure and resource-exhaustion behavior within SLOs | Synthetic smoke data; no capacity certification |
| Data Authority | Real source freshness/revocation/order tests and governed privacy enforcement | Synthetic evidence only |

## Safe Migration Order

1. Agree deployment fault domains, identity provider, alert ownership and RPO/RTO.
2. Prove a retained, off-host catalog-and-object backup with clean-room restoration.
3. Introduce scoped storage identities, external secrets and rotation. Rotate
   persisted database roles explicitly; changing Compose variables is insufficient.
4. Introduce TLS and authenticated Trino identities, then update and test dbt,
   Forge execution, Airflow, Jenkins, showcase and monitoring clients together.
5. Restrict networks and broker/catalog access; retain negative authorization tests.
6. Deliver alerts, enforce trusted native Forge signatures, and test fault-domain
   failures and realistic load before approving production deployment.

Keep canonical contracts unchanged. Use local overlays for local bindings and
existing Forge capabilities for product lifecycle responsibilities. Do not add
parallel custom build, policy, signing or scheduling engines.