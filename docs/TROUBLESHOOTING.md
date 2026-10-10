# Troubleshooting

Start with:

```sh
./lakehouse doctor        # Docker, memory, ports, configuration, with a fix per problem
./lakehouse status        # which endpoint is down
./lakehouse logs <service>
```

When asking for help attach the output of `doctor` and the last 100 lines of the failing service's logs.

## Setup

| Symptom | Cause and fix |
| --- | --- |
| `Python 3.8 or newer is required` | macOS: `xcode-select --install` or `brew install python`. Ubuntu: `sudo apt install python3`. Windows: `winget install -e --id Python.Python.3.12`, then a new terminal |
| `Could not create a Python virtual environment` | Debian/Ubuntu split the module out: `sudo apt install python3-venv`. Or install PyYAML yourself: `python3 -m pip install --user -r patterns/requirements.txt` |
| `Installing PyYAML failed` | No route to PyPI. Set `PIP_INDEX_URL` to your mirror or configure the proxy, then rerun |
| `Docker is not installed` / `daemon is not reachable` | Install Docker (`./setup.sh`), start Docker Desktop, or on Linux `sudo systemctl start docker` |
| Linux: `permission denied ... /var/run/docker.sock` | `sudo usermod -aG docker "$USER"`, then log out and in again |
| `Docker Compose v2.20+ is required` | Update Docker Desktop, or install `docker-compose-v2` / `docker-compose-plugin`. The old `docker-compose` v1 binary is not supported |
| Windows: `lakehouse` opens a file dialog or is not found | Use `lakehouse.cmd` (in PowerShell `.\lakehouse.cmd`). `setup.cmd` bypasses the script execution policy for that one run |
| `exec ./lakehouse: no such file or directory` or `\r` errors | The script got CRLF line endings. Re-clone (`.gitattributes` fixes this) or run `git config core.autocrlf false` and checkout again |

## Starting

| Symptom | Cause and fix |
| --- | --- |
| `port 8093 is taken by ...` | Another program or container uses it. Set `HOST_TRINO_PORT=<free port>` in `.env`, or `./lakehouse init --force` to regenerate with free ports. The message names the owner when it is a container |
| Docker has less than 7 GB | Docker Desktop > Settings > Resources > Memory (Windows: `.wslconfig`). Or stop other stacks |
| A service restarts in a loop, container shows `Exited (137)` | Out of memory. Raise Docker memory or lower limits (`TRINO_MEMORY_LIMIT`, `AIRFLOW_SCHEDULER_MEMORY`) |
| First start is slow | It pulls several GB of images and builds two local images. `./lakehouse up` streams progress; later starts are fast |
| `Airflow scheduler or DAG processor did not become healthy` | `./lakehouse logs airflow-scheduler`. Usually memory or a leftover volume from another version: `./lakehouse reset` |
| `Superset is ready` but the dashboard is missing | The demo was not run yet: `./lakehouse demo`. Check seeding output for errors, then `./lakehouse logs superset-init` |
| `no matching manifest for linux/arm64` | An image without Arm support. All default images are multi-arch; report which one |
| Build fails fetching `raw.githubusercontent.com` or PyPI | Proxy or offline network, see [INSTALL.md](INSTALL.md#corporate-networks-and-proxies) |

## Running

| Symptom | Cause and fix |
| --- | --- |
| The launcher talks to the wrong stack | A variable like `COMPOSE_PROJECT_NAME` or `HOST_*` is exported in your shell and overrides `.env`. `unset` it |
| `Cannot reach Trino at http://127.0.0.1:...` | Stack is down or the port changed: `./lakehouse status`, `./lakehouse info` |
| `SCHEMA_NOT_FOUND: Schema 'shop'` | Demo not loaded: `./lakehouse demo` |
| Trino query fails with `Query exceeded per-node memory` | Raise Trino memory (`TRINO_MEMORY_LIMIT=4g`) or use `preset: medium` |
| Airflow login rejected after editing `.env` | The password is stored in the Airflow volume on first start. Use the old password or `./lakehouse reset` |
| Superset charts spin forever | Trino is busy or restarting; `./lakehouse status`. SQL Lab shows the exact error |
| `smoke` times out | The DAG was not discovered yet (the DAG processor needs a minute after start). Rerun; see `./lakehouse logs airflow-dag-processor` |
| `Flink job aquifer-orders-to-iceberg is not running` | `./lakehouse logs flink-submit` shows why the submission failed (often Redpanda or the Iceberg catalog was not ready). Run `./lakehouse up` again; submission is idempotent |
| `stream` says events did not arrive in time | The job is running but the sink commits on checkpoints (every 10 s). Check `./lakehouse logs flink-taskmanager`; `./lakehouse flink` must show state RUNNING |
| Events in Kafka but `shop.order_events` is empty | The job was not running while you produced. Events wait in Kafka and are read once the job runs (it resumes from committed offsets) |
| `dbt build failed` | The full log is printed above the error. Fix the model, then `./lakehouse dbt build`. Models read the raw `shop` tables, so run `./lakehouse demo` first |
| `dbt` says the `shop.order_events` source is missing | Run without the streaming profile or exclude it: `./lakehouse dbt build --exclude tag:streaming` (the default) |
| `./lakehouse duckdb` fails to attach the catalog | Iceberg REST or MinIO is not up yet; `./lakehouse status`, then retry. `./lakehouse logs duckdb` shows the init script result |
| Flink or Redpanda containers exit with code 137 | Out of memory. Raise Docker memory, or drop the streaming profile in `pattern.yaml` (`profiles: [analytics]`) |

## Platform notes

- **macOS**: Docker Desktop's file sharing must include the project folder (the default includes your home directory). If a
  VPN breaks `localhost` forwarding, restart Docker Desktop.
- **Windows**: keep the repository on the Windows or WSL file system, not on a network drive. Antivirus scanning of the
  WSL VM disk slows first start. Behind a proxy configure Docker Desktop's proxy settings.
- **Linux**: rootless Docker and Podman are untested. If you run SELinux in enforcing mode (Fedora, RHEL) the bind-mounted
  configuration folders may need the `:z` label; open an issue with your findings.

## Start over

```sh
./lakehouse reset --yes       # delete this project's containers and volumes
./lakehouse init --force      # optional: new .env (the old one is kept as .env.bak-<time>)
./lakehouse quickstart
```

`docker system df` shows what Docker uses on your disk; `docker image prune` reclaims unused images.
