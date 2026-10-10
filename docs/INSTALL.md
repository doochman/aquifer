# Installation

You need two things: **Docker with Compose v2** and **Python 3.8 or newer**. Python only runs the small launcher
(`lakehouse.py`); everything else runs in containers. `setup.sh` (macOS, Linux) and `setup.cmd` (Windows) check both and
print exactly what is missing. Add `--install` (`-Install` on Windows) to let them install it, with a confirmation before
every step.

## Platform status

| Platform | Status |
| --- | --- |
| macOS, Apple Silicon | Verified end to end: quickstart, demo, dashboards, `check`, restore drill |
| Linux (Ubuntu 24.04) | Launcher unit tests and `setup.sh` verified in an Ubuntu 24.04 container; the full stack runs in CI on `ubuntu-latest` |
| Windows 10/11 | Launcher unit tests run in CI on `windows-latest`; `setup.ps1` and `lakehouse.cmd` have not been run on a physical Windows machine by the maintainers yet, so please report problems |

## macOS

```sh
./setup.sh              # check
./setup.sh --install    # install Docker Desktop and Python with Homebrew (asks first)
```

Manual route: install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Colima or OrbStack, any
engine that provides `docker compose`), start it, and make sure `python3 --version` prints 3.8 or newer
(`xcode-select --install` provides one). In Docker Desktop open **Settings > Resources** and give it at least 8 GB.

## Ubuntu and Debian

```sh
./setup.sh              # check
./setup.sh --install    # runs the apt commands below after asking
```

Equivalent manual commands:

```sh
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 python3 python3-venv
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"      # then log out and back in
```

- `python3-venv` is split out on Debian-based systems; the launcher needs it once to install PyYAML into `.pattern/venv`.
- If `docker-compose-v2` is not available for your release (Ubuntu 20.04, older Debian), install Docker from
  [Docker's apt repository](https://docs.docker.com/engine/install/ubuntu/), which includes the Compose plugin.
- Snap-packaged Docker (`/snap/bin/docker`) runs under AppArmor, which can refuse to start any container that sets
  `no-new-privileges`. `./lakehouse up` and `./lakehouse doctor` detect this and write `.pattern/compose.compat.yaml`, which
  drops only that option on this machine (needs Compose 2.24.4+). Docker's apt packages above avoid the problem entirely.
- Other distributions: install Docker Engine plus the Compose plugin and Python 3.8+ with your package manager.

## Windows 10 and 11

```bat
setup.cmd               :: check
setup.cmd -Install      :: install Docker Desktop and Python with winget (asks first)
```

1. Install **Docker Desktop** and keep the **WSL 2** backend selected. If it asks for WSL 2, run
   `wsl --install` in an administrator PowerShell and restart. A restart cannot be automated safely, so the script
   explains it instead.
2. Install Python 3.8+ from python.org or `winget install -e --id Python.Python.3.12`. Choose "Add python.exe to PATH"
   or use the `py` launcher.
3. Open a new terminal in the project folder and run `lakehouse.cmd quickstart` (works in cmd.exe and PowerShell; in
   PowerShell write `.\lakehouse.cmd`).

Tips:

- Clone the repository with Git for Windows. `.gitattributes` forces LF line endings for scripts that run inside Linux
  containers; do not re-save them with CRLF.
- By default WSL 2 may take half of your RAM. Raise or lower it in `%UserProfile%\.wslconfig`:

  ```ini
  [wsl2]
  memory=10GB
  processors=6
  ```

  then `wsl --shutdown` and restart Docker Desktop.
- Running the launcher from inside a WSL distribution (`./lakehouse ...`) is equally supported and needs the same
  Docker Desktop WSL integration enabled for that distribution. `security-check` needs bash, openssl and htpasswd, so
  use WSL for it.

## Verify

```sh
./lakehouse doctor
```

It checks Python, Docker, Compose version, Docker memory and CPUs, free disk, the Compose model and every host port, and
suggests a fix for each problem. Then:

```sh
./lakehouse quickstart
```

## Corporate networks and proxies

- The launcher never routes `localhost` traffic through a proxy.
- Docker image pulls and the image builds (`pip install` inside Airflow and Superset images) need access to Docker Hub,
  PyPI and `raw.githubusercontent.com` (Airflow constraint files). Configure the proxy in Docker Desktop
  (**Settings > Resources > Proxies**) or the Docker daemon.
- The first launcher run downloads PyYAML from PyPI. Behind a pip mirror set `PIP_INDEX_URL` in your shell first.
- Air-gapped: build the images on a connected machine, `docker save` them, `docker load` them offline and run
  `./lakehouse up --no-build`.

## Alternatives

- **Podman**: `docker compose` can talk to Podman's Docker-compatible socket; this is not tested here.
- **GitHub Codespaces / Dev Containers**: `.devcontainer/devcontainer.json` provides Docker-in-Docker; choose a machine
  with 4 cores and 16 GB.
- **Remote Docker host**: the stack binds to the Docker host's loopback; use SSH port forwarding to reach it.

## Uninstall

```sh
./lakehouse reset --yes      # remove containers and all data volumes of this project
docker image prune           # optional: reclaim image space
```

Then delete the folder. Nothing is installed outside Docker and the project directory (`.pattern/venv` lives inside it).
