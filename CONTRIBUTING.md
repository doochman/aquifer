# Contributing to AQUIFER

Thanks for helping. AQUIFER is a developer platform: changes should keep `./lakehouse quickstart` working on macOS,
Windows and Linux with nothing but Docker and Python installed.

## Ground rules

- The core (`compose.yaml`) must stay standalone: no sibling repositories, no external volumes, no company names. Put
  such things in an extension (`compose.<name>.yaml`, see [docs/EXTENSIONS.md](docs/EXTENSIONS.md)). A unit test enforces this.
- The launcher (`lakehouse.py`) uses the standard library only, supports Python 3.8+, prints ASCII only (Windows consoles)
  and never needs `bash`, `lsof` or other Unix tools. Anything that does must say so and be optional.
- Never print or log secrets by default; generated credentials live in the git-ignored `.env`.
- Pin image and package versions. Explain version bumps in the pull request.
- Keep local-only credentials out of anything that looks production-ready, and keep
  [PRODUCTION-READINESS.md](PRODUCTION-READINESS.md) honest when you change security-relevant behaviour.

## Develop

```sh
make test                 # unit tests, no Docker (Windows: lakehouse.cmd test)
./lakehouse lint          # validate the compose model for every extension combination
./lakehouse quickstart    # bring the stack up
./lakehouse check         # endpoints, round trip, dbt tests, DuckDB, Kafka to Flink to Iceberg, every chart
./lakehouse reset --yes
```

Test in a scratch copy with its own project name when you already run the stack:

```sh
rsync -a --exclude .env --exclude .pattern ./ /tmp/lakehouse-test/ && cd /tmp/lakehouse-test && ./lakehouse quickstart --no-open
```

(`init` moves ports that are taken, so both copies can run side by side.)

## Typical changes

| Change | Where |
| --- | --- |
| New launcher command | `build_parser()` and a `cmd_*` function in `lakehouse.py`, a test in `tests/test_lakehouse.py`, a row in the README table |
| New service | `compose.yaml` with a memory cap, `cap_drop`, loopback port, a probe in `SERVICES` in `lakehouse.py`, a row in `docs/ARCHITECTURE.md` |
| New demo SQL | `demo/sql/` (statements split on `;`, no semicolons inside strings), update `DEMO_SQL` and the statement-count test |
| New dbt model or test | `dbt/models/`; `./lakehouse dbt build` and `./lakehouse check` run them |
| New Flink job | SQL in `flink/sql/`, submission in `flink/submit.sh`, document it in `docs/PIPELINES.md` |
| New dashboard chart | `analytics/bootstrap.py`; `./lakehouse check` runs every chart through Trino |
| New preset or setting | `patterns/render.py` plus `patterns/test_pattern.py` |

## Pull requests

CI runs the unit tests on Linux, macOS and Windows (Python 3.8 and 3.12), validates the compose model and runs the full
stack on Linux. Please describe what you ran locally and on which platform; for Windows changes say whether you ran them on
a real machine.

## License

By contributing you agree that your contribution is licensed under the [MIT License](LICENSE) that covers this project.
Third-party software that AQUIFER orchestrates keeps its own licenses, see [docs/THIRD-PARTY.md](docs/THIRD-PARTY.md).
