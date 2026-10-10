# Security policy

AQUIFER is a **local development stack**: it binds to `127.0.0.1`, generates per-machine passwords and is not hardened for
production or real customer data (see [PRODUCTION-READINESS.md](PRODUCTION-READINESS.md)).

## Reporting a vulnerability

Please do not open a public issue for a security problem. Use the repository's private vulnerability reporting
(GitHub: Security > Report a vulnerability) or contact the maintainers privately, and include:

- what you found and the impact,
- the version or commit and the output of `./lakehouse doctor`,
- steps to reproduce.

We aim to acknowledge reports within a few working days.

## In scope

- The launcher (`lakehouse.py`, `setup.*`), Compose files, Dockerfiles, seeded dashboards and scripts in this repository,
  for example credential leaks, unsafe defaults that expose services beyond loopback, or command injection.

## Out of scope

- Vulnerabilities in upstream projects (Trino, Airflow, Flink, Superset, MinIO, Redpanda, ...): report those upstream.
- The documented non-goals: no TLS, no authentication on Trino, no backups.
