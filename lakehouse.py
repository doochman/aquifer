#!/usr/bin/env python3
"""AQUIFER launcher: the open-source, on-premise data stack on your machine. One cross-platform entry point for macOS, Linux and Windows.

Standard library only (Python 3.8+). It wraps Docker Compose, so everything it does is
visible with `./lakehouse compose ...`; nothing is hidden state. Run `./lakehouse --help`.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import platform
import re
import secrets
import shutil
import socket
import string
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.error
import urllib.request
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parent
PATTERN_FILE = ROOT / "pattern.yaml"
GENERATED = ROOT / ".pattern"
ENV_FILE = ROOT / ".env"
DEFAULT_PROJECT = "aquifer"
VERSION = "1.0.0"
BANNER = r"""
     _      ___   _   _  ___  _____  _____  ____
    / \    / _ \ | | | ||_ _||  ___|| ____||  _ \
   / _ \  | | | || | | | | | | |_   |  _|  | |_) |
  / ___ \ | |_| || |_| | | | |  _|  | |___ |  _ <
 /_/   \_\ \__\_\ \___/ |___||_|    |_____||_| \_\
"""
ALL_PROFILES = ("analytics", "observability", "streaming", "spark", "notebook", "recovery", "security-validation")
OPTIONAL_PROFILES = ("analytics", "observability", "streaming", "spark", "notebook")
MIN_COMPOSE = (2, 20)
MIN_COMPOSE_REFERENCE = (2, 23)
MIN_DOCKER_MEMORY_GB = {"small": 7.0, "medium": 14.0}
DEMO_SQL = ("01_seed.sql",)
DBT_BIN = "/home/airflow/.dbt-venv/bin/dbt"
FLINK_JOB = "aquifer-orders-to-iceberg"
ORDER_TOPIC = "shop.orders"
DUCKDB_INIT = "/home/duck/init.sql"
PORT_LABELS = {"trino": "Trino", "airflow": "Airflow", "console": "Redpanda Console", "minio_api": "MinIO API", "minio_console": "MinIO console",
               "registry": "Schema Registry", "kafka": "Kafka", "superset": "Superset", "prometheus": "Prometheus", "grafana": "Grafana",
               "showcase": "Showcase", "flink": "Flink", "notebook": "JupyterLab"}
SUPERSET_DASHBOARD = "/superset/dashboard/shop-overview/"
SUPERSET_STREAMING = "/superset/dashboard/streaming-live/"

LEGACY_FLAGS = {
    "--doctor", "--status", "--logs", "--smoke", "--down", "--reset", "--pattern", "--pattern-check",
    "--observability", "--analytics", "--analytics-check", "--spark-check", "--catalog-restore-check",
    "--security-check", "--up", "--help",
}


class LakehouseError(Exception):
    """A problem the user can fix; printed without a traceback."""


# --------------------------------------------------------------------------- output

def _enable_ansi() -> bool:
    if os.environ.get("NO_COLOR") or not sys.stdout.isatty():
        return False
    if os.name == "nt":
        os.system("")  # switches the Windows console into VT mode
    return True


COLOR = _enable_ansi()
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")
    sys.stderr.reconfigure(errors="replace")


def paint(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if COLOR else text


def say(message: str = "") -> None:
    print(message, flush=True)


def ok(message: str) -> None:
    say(f"  {paint('[ ok ]', '32')} {message}")


def warn(message: str) -> None:
    say(f"  {paint('[warn]', '33')} {message}")


def bad(message: str) -> None:
    say(f"  {paint('[FAIL]', '31')} {message}")


def heading(message: str) -> None:
    say(f"\n{paint(message, '1')}")


def hint(message: str) -> None:
    say(textwrap.indent(textwrap.fill(message, 92), "         "))


# --------------------------------------------------------------------------- pure helpers

def parse_env(text: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        values[key] = value
    return values


def read_env(path: Path) -> Dict[str, str]:
    return parse_env(path.read_text(encoding="utf-8")) if path.is_file() else {}


def generate_secret(length: int = 24) -> str:
    # Letters and digits only: safe in shells, URLs and YAML without quoting.
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def parse_version(text: str) -> Tuple[int, ...]:
    match = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", text)
    return tuple(int(part) for part in match.groups() if part is not None) if match else (0,)


def port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.5)
        return probe.connect_ex((host, port)) == 0


def pick_free_ports(defaults: Dict[str, int], busy=None) -> Dict[str, int]:
    """Keep each default port when it is free; otherwise take the next free, unassigned one."""
    busy = busy or port_in_use
    chosen: Dict[str, int] = {}
    taken = set()
    for name, port in defaults.items():
        candidate = port
        while candidate in taken or busy(candidate):
            candidate += 1
            if candidate > 65535:
                raise LakehouseError(f"No free port found at or above {port} for {name}")
        chosen[name] = candidate
        taken.add(candidate)
    return chosen


def split_sql(script: str) -> List[str]:
    """Split a script on top-level semicolons, honouring quotes and comments."""
    statements: List[str] = []
    current: List[str] = []
    index, length = 0, len(script)
    quote = ""
    while index < length:
        char = script[index]
        pair = script[index:index + 2]
        if quote:
            current.append(char)
            if char == quote:
                if script[index + 1:index + 2] == quote:  # doubled quote escapes itself
                    current.append(quote)
                    index += 1
                else:
                    quote = ""
        elif pair == "--":
            newline = script.find("\n", index)
            index = length if newline == -1 else newline
            continue
        elif pair == "/*":
            end = script.find("*/", index + 2)
            index = length if end == -1 else end + 1
        elif char in "'\"":
            quote = char
            current.append(char)
        elif char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        index += 1
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def format_table(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    cells = [[("NULL" if value is None else str(value)) for value in row] for row in rows]
    widths = [max([len(str(column))] + [len(row[i]) for row in cells]) for i, column in enumerate(columns)]
    def line(values: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[i]) for i, value in enumerate(values)).rstrip()
    rule = "-+-".join("-" * width for width in widths)
    return "\n".join([line([str(c) for c in columns]), rule] + [line(row) for row in cells])


_INT = re.compile(r"^[+-]?\d+$")
_FLOAT = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?$")
_RESERVED = {"all", "and", "as", "by", "case", "cast", "create", "cross", "current_date", "delete", "distinct", "drop", "else", "end",
             "except", "exists", "false", "from", "full", "group", "having", "in", "inner", "insert", "intersect", "into", "is",
             "join", "left", "like", "natural", "not", "null", "on", "or", "order", "outer", "right", "select", "table", "then",
             "true", "union", "using", "values", "when", "where", "with"}


def sanitize_identifier(name: str, taken: Sequence[str] = ()) -> str:
    """Lower-case snake_case that is safe unquoted in Trino and unique among `taken`."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower() or "column"
    if cleaned[0].isdigit() or cleaned in _RESERVED:
        cleaned = "c_" + cleaned
    candidate, counter = cleaned, 2
    while candidate in taken:
        candidate, counter = f"{cleaned}_{counter}", counter + 1
    return candidate


def infer_column_type(values: Iterable[Optional[str]]) -> str:
    """Narrowest Trino type that holds every non-empty value; anything unclear stays VARCHAR."""
    seen = [value for value in values if value not in (None, "")]
    if not seen:
        return "VARCHAR"
    if all(value.lower() in ("true", "false") for value in seen):
        return "BOOLEAN"
    if all(_INT.match(value) and (len(value.lstrip("+-")) == 1 or not value.lstrip("+-").startswith("0")) and abs(int(value)) < 2 ** 63 for value in seen):
        return "BIGINT"
    if all(_is_float(value) for value in seen):
        return "DOUBLE"
    if all(_DATE.match(value) and _valid_date(value) for value in seen):
        return "DATE"
    if all(_TIMESTAMP.match(value) and _valid_date(value[:10]) for value in seen):
        return "TIMESTAMP(6)"
    return "VARCHAR"


def _is_float(value: str) -> bool:
    if not _FLOAT.match(value):
        return False
    integer_part = re.split(r"[.eE]", value.lstrip("+-"))[0]
    if len(integer_part) > 1 and integer_part.startswith("0"):  # zip codes and ids keep their leading zero
        return False
    return math.isfinite(float(value))


def _valid_date(text: str) -> bool:
    try:
        datetime.strptime(text, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def sql_literal(value: Optional[str], column_type: str) -> str:
    if value in (None, ""):
        return "NULL"
    if column_type == "BOOLEAN":
        return value.lower()
    if column_type == "BIGINT":
        return str(int(value))
    if column_type == "DOUBLE":
        return repr(float(value))
    if column_type == "DATE":
        return f"DATE '{value}'"
    if column_type.startswith("TIMESTAMP"):
        stamp = value.replace("T", " ")
        head, _, fraction = stamp.partition(".")
        return f"TIMESTAMP '{head}.{fraction.ljust(6, '0')}'" if fraction else f"TIMESTAMP '{head}.000000'"
    return "'" + value.replace("'", "''") + "'"


def read_rows(path: Path, delimiter: Optional[str] = None) -> Tuple[List[str], List[List[Optional[str]]]]:
    """Header and string rows from CSV/TSV (or JSON lines); values stay strings so types are inferred once, later."""
    suffix = path.suffix.lower()
    if suffix in (".jsonl", ".ndjson"):
        records = [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
        header: List[str] = []
        for record in records:
            header += [key for key in record if key not in header]
        scalar = lambda value: None if value is None else (json.dumps(value) if isinstance(value, (dict, list)) else (str(value).lower() if isinstance(value, bool) else str(value)))  # noqa: E731
        return header, [[scalar(record.get(key)) for key in header] for record in records]
    chosen = delimiter or ("\t" if suffix in (".tsv", ".tab") else ",")
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle, delimiter=chosen)
        try:
            header = next(reader)
        except StopIteration:
            raise LakehouseError(f"{path.name} is empty")
        rows = [[(cell if cell != "" else None) for cell in (row + [""] * (len(header) - len(row)))[:len(header)]] for row in reader if row]
    return header, rows


def table_ddl(table: str, columns: Sequence[Tuple[str, str]]) -> str:
    body = ",\n  ".join(f"{name} {column_type}" for name, column_type in columns)
    return f"CREATE TABLE {table} (\n  {body}\n) WITH (format = 'PARQUET')"


def insert_batches(table: str, columns: Sequence[Tuple[str, str]], rows: Sequence[Sequence[Optional[str]]], max_chars: int = 300_000, max_rows: int = 2000) -> Iterable[str]:
    """INSERT statements sized to stay well under Trino's query-length limit."""
    prefix = f"INSERT INTO {table} VALUES\n"
    chunk: List[str] = []
    size = len(prefix)
    for row in rows:
        literal = "(" + ", ".join(sql_literal(value, column_type) for value, (_, column_type) in zip(row, columns)) + ")"
        if chunk and (len(chunk) >= max_rows or size + len(literal) > max_chars):
            yield prefix + ",\n".join(chunk)
            chunk, size = [], len(prefix)
        chunk.append(literal)
        size += len(literal) + 2
    if chunk:
        yield prefix + ",\n".join(chunk)


def normalise_argv(argv: Sequence[str]) -> List[str]:
    """Accept the historical `./up.sh --doctor` spelling alongside `./lakehouse doctor`."""
    args = list(argv)
    if args and args[0] in LEGACY_FLAGS and args[0] not in ("--help",):
        args[0] = args[0][2:]
    return args


def human_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m {secs:02d}s" if minutes else f"{secs}s"


def describe_statement(statement: str) -> str:
    match = re.match(r"CREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)", statement, re.IGNORECASE)
    return f"{match.group(1).lower()} {match.group(2)}" if match else re.sub(r"\s+", " ", statement)[:60]


# --------------------------------------------------------------------------- HTTP and Trino

_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # never route localhost via a corporate proxy


def http_get(url: str, timeout: float = 3.0) -> Tuple[int, bytes]:
    try:
        with _OPENER.open(url, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, b""
    except (urllib.error.URLError, OSError, ValueError):
        return 0, b""


def url_ready(url: str) -> bool:
    status, body = http_get(url)
    if status != 200:
        return False
    try:
        if url.endswith("/v1/info"):
            return json.loads(body).get("starting") is False
        if url.endswith("/api/v2/monitor/health"):
            data = json.loads(body)
            return all(data.get(key, {}).get("status") == "healthy" for key in ("metadatabase", "scheduler", "dag_processor"))
    except ValueError:
        return False
    return True


class TrinoError(LakehouseError):
    pass


class Trino:
    """Minimal Trino HTTP client so `sql` and `demo` need no driver and no docker exec."""

    def __init__(self, port: int, user: str = "lakehouse", catalog: str = "iceberg", host: str = "127.0.0.1"):
        self.base = f"http://{host}:{port}"
        self.headers = {"X-Trino-User": user, "X-Trino-Catalog": catalog, "X-Trino-Source": "lakehouse-cli"}

    def execute(self, sql: str, timeout: float = 600.0) -> Tuple[List[str], List[list]]:
        request = urllib.request.Request(f"{self.base}/v1/statement", data=sql.encode("utf-8"), headers=self.headers, method="POST")
        columns: List[str] = []
        rows: List[list] = []
        deadline = time.monotonic() + timeout
        payload = self._send(request)
        while True:
            if "error" in payload:
                error = payload["error"]
                raise TrinoError(f"{error.get('errorName', 'ERROR')}: {error.get('message', 'query failed')}")
            if payload.get("columns") and not columns:
                columns = [column["name"] for column in payload["columns"]]
            rows.extend(payload.get("data", []))
            next_uri = payload.get("nextUri")
            if not next_uri:
                return columns, rows
            if time.monotonic() > deadline:
                raise TrinoError("Query timed out")
            if not payload.get("data"):
                time.sleep(0.15)
            payload = self._send(urllib.request.Request(next_uri, headers=self.headers))

    def _send(self, request: urllib.request.Request) -> dict:
        try:
            with _OPENER.open(request, timeout=60) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            raise TrinoError(f"Trino answered HTTP {error.code}: {error.read()[:200]!r}")
        except (urllib.error.URLError, OSError) as error:
            raise TrinoError(f"Cannot reach Trino at {self.base} ({error}). Is the stack running? Try ./lakehouse up")


# --------------------------------------------------------------------------- service catalogue

class Service:
    def __init__(self, key: str, label: str, port: str, path: str, probe: str, profile: str = "", extension: str = "",
                 login: Optional[Tuple[str, str]] = None, note: str = "", probe_port: str = "", token_env: str = ""):
        self.key, self.label, self.port, self.path, self.probe = key, label, port, path, probe
        self.profile, self.extension, self.login, self.note = profile, extension, login, note
        self.token_env = token_env
        self.probe_port = probe_port or port


SERVICES = [
    Service("superset", "Superset (charts, dashboards)", "superset", "/", "/health", profile="analytics",
            login=("SUPERSET_ADMIN_USER", "SUPERSET_ADMIN_PASSWORD")),
    Service("airflow", "Airflow (orchestration)", "airflow", "/", "/api/v2/monitor/health",
            login=("AIRFLOW_ADMIN_USER", "AIRFLOW_ADMIN_PASSWORD")),
    Service("trino", "Trino (SQL engine)", "trino", "/ui/", "/v1/info", note="UI sign-in: any user name, no password"),
    Service("console", "Redpanda Console (Kafka)", "console", "/", "/"),
    Service("flink", "Flink (stream processing)", "flink", "/", "/overview", profile="streaming"),
    Service("notebook", "JupyterLab (notebooks)", "notebook", "/lab", "/api", profile="notebook", token_env="NOTEBOOK_TOKEN"),
    Service("minio", "MinIO (object storage)", "minio_console", "/", "/minio/health/ready", probe_port="minio_api",
            login=("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")),
    Service("registry", "Schema Registry", "registry", "/subjects", "/subjects"),
    Service("grafana", "Grafana (platform dashboards)", "grafana", "/d/local-lakehouse", "/api/health", profile="observability"),
    Service("prometheus", "Prometheus", "prometheus", "/", "/-/ready", profile="observability"),
    Service("showcase", "Customer Profile 360 showcase", "showcase", "/", "/api/health", extension="reference"),
]


# --------------------------------------------------------------------------- the stack

class Stack:
    def __init__(self) -> None:
        self.env: Dict[str, str] = {}
        self.resolved: dict = {}
        self.effective: dict = {}
        self.project = DEFAULT_PROJECT

    # -- configuration ------------------------------------------------------------------
    @classmethod
    def load(cls, auto_init: bool = True) -> "Stack":
        if auto_init and not ENV_FILE.exists():
            init_env(quiet=False)
        stack = cls()
        stack.refresh()
        return stack

    def refresh(self) -> None:
        render = load_renderer()
        self.resolved = render.render(PATTERN_FILE)
        self.env = {**read_env(GENERATED / "defaults.env"), **read_env(ENV_FILE), **os.environ}
        try:
            self.effective = render.effective(self.resolved, self.env)
        except ValueError as error:
            raise LakehouseError(f"Invalid configuration: {error}")
        self.project = self.env.get("COMPOSE_PROJECT_NAME") or DEFAULT_PROJECT
        self.write_prometheus_targets()

    def write_prometheus_targets(self) -> None:
        """Prometheus reads these files, so only services that should be running are probed and scraped."""
        readiness = ["http://trino:8080/v1/info", "http://minio:9000/minio/health/ready",
                     "http://airflow-api-server:8080/api/v2/monitor/health", "http://redpanda:8081/subjects"]
        if "analytics" in self.profiles:
            readiness.append("http://superset:8088/health")
        streaming = "streaming" in self.profiles
        if streaming:
            readiness.append("http://flink-jobmanager:8081/overview")
        reference = "reference" in self.extensions
        if reference:
            readiness.append("http://profile-showcase:8088/api/summary")
        directory = GENERATED / "targets"
        directory.mkdir(parents=True, exist_ok=True)
        for name, targets in (("readiness", readiness), ("flink", ["flink-jobmanager:9249", "flink-taskmanager:9249"] if streaming else []),
                              ("customer-profile", ["profile-showcase:8088"] if reference else [])):
            (directory / f"{name}.json").write_text(json.dumps([{"targets": targets}], indent=2) + "\n", encoding="utf-8")

    @property
    def extensions(self) -> List[str]:
        return list(self.effective.get("extensions", []))

    @property
    def profiles(self) -> List[str]:
        return list(self.effective.get("profiles", []))

    def port(self, name: str) -> int:
        return int(self.effective["ports"][name])

    def url(self, service: Service, probe: bool = False, token: bool = False) -> str:
        port = self.port(service.probe_port if probe else service.port)
        base = f"http://localhost:{port}" + (service.probe if probe else service.path)
        return f"{base}?token={self.env.get(service.token_env, '')}" if token and service.token_env else base

    def probe_url(self, service: Service) -> str:
        return f"http://127.0.0.1:{self.port(service.probe_port)}{service.probe}"

    def compose_files(self) -> List[Path]:
        files = [ROOT / "compose.yaml"]
        files += [ROOT / f"compose.{name}.yaml" for name in self.extensions]
        files.append(GENERATED / "compose.yaml")
        return files

    def compose_prefix(self, profiles: Iterable[str] = ()) -> List[str]:
        command = ["docker", "compose", "--project-name", self.project]
        for path in self.compose_files():
            command += ["--file", str(path)]
        for path in (GENERATED / "defaults.env", ENV_FILE):
            if path.is_file():
                command += ["--env-file", str(path)]
        for profile in dict.fromkeys(profiles):
            command += ["--profile", profile]
        return command

    def compose(self, *args: str, profiles: Optional[Iterable[str]] = None, check: bool = True,
                capture: bool = False, stdin=None, input_text: Optional[str] = None) -> subprocess.CompletedProcess:
        selected = self.profiles if profiles is None else profiles
        command = self.compose_prefix(selected) + list(args)
        return subprocess.run(command, cwd=str(ROOT), check=check, stdin=stdin, input=input_text,
                              stdout=subprocess.PIPE if capture else None,
                              stderr=subprocess.STDOUT if capture else None, universal_newlines=True if (capture or input_text) else None)

    def compose_out(self, *args: str, profiles: Optional[Iterable[str]] = None) -> str:
        result = self.compose(*args, profiles=profiles, check=False, capture=True)
        return (result.stdout or "").strip()

    def running_services(self) -> List[str]:
        out = self.compose_out("ps", "--status", "running", "--services", profiles=ALL_PROFILES)
        return [line.strip() for line in out.splitlines() if line.strip() and " " not in line.strip()]

    def active_services(self) -> List[Service]:
        """Services that should answer: everything in the configured profiles and extensions."""
        return [s for s in SERVICES if (not s.profile or s.profile in self.profiles) and (not s.extension or s.extension in self.extensions)]

    def trino(self, user: str = "lakehouse") -> Trino:
        return Trino(self.port("trino"), user=user)


def load_renderer():
    ensure_yaml()
    sys.path.insert(0, str(ROOT / "patterns"))
    import render  # type: ignore
    return render


def ensure_yaml() -> None:
    """PyYAML is the one third-party dependency; install it into a private venv when missing."""
    try:
        import yaml  # noqa: F401
        return
    except ImportError:
        pass
    venv = GENERATED / "venv"
    interpreter = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not interpreter.exists():
        say("Preparing a private Python environment for the launcher (one time, needs internet) ...")
        GENERATED.mkdir(exist_ok=True)
        created = subprocess.run([sys.executable, "-m", "venv", str(venv)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        if created.returncode != 0:
            shutil.rmtree(venv, ignore_errors=True)
            raise LakehouseError(
                "Could not create a Python virtual environment.\n"
                "  Debian/Ubuntu: sudo apt install python3-venv\n"
                "  Or install PyYAML yourself: python3 -m pip install --user -r patterns/requirements.txt\n" + created.stdout[-300:])
        installed = subprocess.run([str(interpreter), "-m", "pip", "install", "--quiet", "--disable-pip-version-check", "-r", str(ROOT / "patterns" / "requirements.txt")],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        if installed.returncode != 0:
            shutil.rmtree(venv, ignore_errors=True)
            raise LakehouseError("Installing PyYAML failed (is there internet access / a pip proxy?):\n" + installed.stdout[-400:])
    for site in list(venv.glob("lib/python*/site-packages")) + list(venv.glob("Lib/site-packages")):
        sys.path.insert(0, str(site))
    try:
        import yaml  # noqa: F401
    except ImportError:
        shutil.rmtree(venv, ignore_errors=True)
        raise LakehouseError("The private environment is unusable; it was removed. Run the command again.")


# --------------------------------------------------------------------------- docker preflight

def docker_hint() -> str:
    system = platform.system()
    if system == "Darwin":
        return "Start Docker Desktop (or Colima/OrbStack) and wait until it reports 'running'."
    if system == "Windows":
        return "Start Docker Desktop (WSL 2 backend) and wait until the whale icon stops animating."
    return "Start the daemon (sudo systemctl start docker) and make sure your user may use it: sudo usermod -aG docker $USER, then log in again."


def docker_info() -> Optional[dict]:
    if not shutil.which("docker"):
        return None
    result = subprocess.run(["docker", "info", "--format", "{{json .}}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode != 0 and not result.stdout.strip():
        return {}
    try:
        return json.loads(result.stdout)
    except ValueError:
        return {}


def compose_version() -> Tuple[int, ...]:
    result = subprocess.run(["docker", "compose", "version", "--short"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    return parse_version(result.stdout) if result.returncode == 0 else (0,)


def require_docker(stack: Optional[Stack] = None) -> dict:
    if not shutil.which("docker"):
        raise LakehouseError("Docker is not installed. Run ./setup.sh (macOS/Linux) or setup.cmd (Windows), or see docs/INSTALL.md.")
    version = compose_version()
    minimum = MIN_COMPOSE_REFERENCE if stack and "reference" in stack.extensions else MIN_COMPOSE
    if version < minimum:
        found = ".".join(map(str, version)) if version != (0,) else "missing"
        raise LakehouseError(f"Docker Compose v{'.'.join(map(str, minimum))}+ is required (found: {found}). Update Docker Desktop, or install the docker-compose-v2 / docker-compose-plugin package.")
    info = docker_info()
    if not info:
        raise LakehouseError("The Docker daemon is not reachable. " + docker_hint())
    return info


def port_owner(port: int, project: str) -> Tuple[str, bool]:
    """Return (container names publishing the port, True when they all belong to this project)."""
    def names(extra: Sequence[str]) -> List[str]:
        result = subprocess.run(["docker", "ps", "--filter", f"publish={port}", *extra, "--format", "{{.Names}}"],
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
        return [line for line in result.stdout.splitlines() if line]
    everyone = names([])
    ours = names(["--filter", f"label=com.docker.compose.project={project}"])
    return ", ".join(everyone), bool(everyone) and sorted(everyone) == sorted(ours)


# --------------------------------------------------------------------------- init (.env)

ENV_TEMPLATE = """\
# AQUIFER configuration. Generated by `./lakehouse init`; edit freely, never commit.
# These credentials protect a loopback-only development stack. Do not reuse them anywhere else.
# Changing a password here does not rotate data that already exists in the volumes
# (Airflow, MinIO, Superset); use `./lakehouse reset` for a clean start.

COMPOSE_PROJECT_NAME={project}

MINIO_ROOT_USER=localadmin
MINIO_ROOT_PASSWORD={minio}
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD={airflow}
SUPERSET_ADMIN_USER=admin
SUPERSET_ADMIN_PASSWORD={superset}
SUPERSET_SECRET_KEY={secret}
AIRFLOW_UID=50000
NOTEBOOK_TOKEN={notebook}
NOTEBOOK_UID={uid}

# --- Host ports -----------------------------------------------------------------------------
# Written below only where the default was already taken on this machine when init ran.
{ports}
# --- Optional switches (uncomment to override pattern.yaml) ------------------------------------
# COMPOSE_PROFILES=analytics,streaming,observability,spark,notebook
# LAKEHOUSE_EXTENSIONS=reference
# TRINO_MEMORY_LIMIT=4g
# AIRFLOW_SCHEDULER_MEMORY=4g
"""


def init_env(force: bool = False, quiet: bool = False, project: str = DEFAULT_PROJECT, check_ports: bool = True) -> Path:
    if ENV_FILE.exists() and not force:
        if not quiet:
            say(f".env already exists; leaving it untouched ({ENV_FILE})")
        return ENV_FILE
    render = load_renderer()
    defaults = dict(render.resolve(load_pattern(render))["ports"])
    chosen = pick_free_ports(defaults) if check_ports else dict(defaults)
    moved = {name: port for name, port in chosen.items() if port != defaults[name]}
    lines = "\n".join(f"HOST_{name.upper()}_PORT={port}" for name, port in sorted(moved.items())) or "# (all default ports are free)"
    if force and ENV_FILE.exists():
        backup = ENV_FILE.with_name(f".env.bak-{datetime.now().strftime('%Y%m%d%H%M%S')}")
        ENV_FILE.replace(backup)
        say(f"Previous .env saved as {backup.name}")
    content = ENV_TEMPLATE.format(project=project, minio=generate_secret(), airflow=generate_secret(), superset=generate_secret(),
                                  notebook=generate_secret(), uid=os.getuid() if hasattr(os, "getuid") else 1000,
                                  secret=generate_secret(42), ports=lines)
    ENV_FILE.write_text(content, encoding="utf-8")
    try:
        os.chmod(ENV_FILE, 0o600)
    except OSError:
        pass
    say(f"Created .env with generated credentials ({ENV_FILE.name}).")
    for name, port in sorted(moved.items()):
        say(f"  default port {defaults[name]} for {name} is busy on this machine; using {port}")
    return ENV_FILE


def load_pattern(render) -> dict:
    import yaml
    return yaml.safe_load(PATTERN_FILE.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- waiting and summaries

def wait_for(url: str, label: str, timeout: int = 300, stack: Optional[Stack] = None, logs_for: str = "") -> None:
    start = time.monotonic()
    frames = "|/-\\"
    tick = 0
    interactive = sys.stdout.isatty()
    while True:
        if url_ready(url):
            say(f"\r  {paint('[ ok ]', '32')} {label} is ready ({human_duration(time.monotonic() - start)})".ljust(80) if interactive else f"  [ ok ] {label} is ready")
            return
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            say()
            bad(f"{label} did not become ready within {human_duration(timeout)} ({url})")
            if stack and logs_for:
                stack.compose("logs", "--tail=40", logs_for, check=False)
            raise LakehouseError(f"{label} is not ready. Inspect with: ./lakehouse logs {logs_for or ''}".strip())
        if interactive:
            sys.stdout.write(f"\r  {frames[tick % 4]} waiting for {label} ... {human_duration(elapsed)}   ")
            sys.stdout.flush()
        elif tick % 15 == 0:
            say(f"  waiting for {label} ...")
        tick += 1
        time.sleep(1)


def wait_for_airflow(stack: Stack) -> None:
    wait_for(stack.probe_url(next(s for s in SERVICES if s.key == "airflow")), "Airflow API server", stack=stack, logs_for="airflow-api-server")
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        scheduler = stack.compose("exec", "-T", "airflow-scheduler", "airflow", "jobs", "check", "--job-type", "SchedulerJob", "--local", check=False, capture=True)
        processor = stack.compose("exec", "-T", "airflow-dag-processor", "airflow", "jobs", "check", "--job-type", "DagProcessorJob", "--local", check=False, capture=True)
        if scheduler.returncode == 0 and processor.returncode == 0:
            ok("Airflow scheduler and DAG processor are healthy")
            return
        time.sleep(3)
    raise LakehouseError("Airflow scheduler or DAG processor did not become healthy. Inspect: ./lakehouse logs airflow-scheduler")


def credential_lines(stack: Stack, show_secrets: bool) -> List[str]:
    lines = []
    for service in stack.active_services():
        if service.login:
            user = stack.env.get(service.login[0], "")
            password = stack.env.get(service.login[1], "")
            shown = password if show_secrets else f"(see {service.login[1]} in .env)"
            lines.append(f"  {service.label:<34} user {user}   password {shown}")
        elif service.token_env:
            shown = stack.env.get(service.token_env, "") if show_secrets else f"(see {service.token_env} in .env)"
            lines.append(f"  {service.label:<34} token {shown}")
    return lines


def print_endpoints(stack: Stack, show_secrets: bool = False) -> None:
    for service in stack.active_services():
        say(f"  {service.label:<34} {stack.url(service)}")
    say(f"  {'Kafka (from the host)':<34} localhost:{stack.port('kafka')}")
    say(f"  {'DuckDB / dbt shells':<34} ./lakehouse duckdb   ./lakehouse dbt")
    say()
    for line in credential_lines(stack, show_secrets):
        say(line)
    if not show_secrets:
        say("  (reveal passwords with: ./lakehouse info --show-secrets)")


def write_home(stack: Stack) -> Path:
    """A local landing page with live status dots, generated with the configured ports."""
    cards = []
    for service in stack.active_services():
        login = ""
        if service.login:
            login = f"<small>user <code>{html.escape(stack.env.get(service.login[0], ''))}</code> - password in <code>.env</code></small>"
        elif service.note:
            login = f"<small>{html.escape(service.note)}</small>"
        cards.append(
            f'<a class="card" href="{html.escape(stack.url(service, token=True))}" target="_blank" rel="noopener"><span class="dot" data-probe="{html.escape(stack.probe_url(service))}"></span>'
            f"<b>{html.escape(service.label)}</b><span>{html.escape(stack.url(service))}</span>{login}</a>")
    try_it = ("<li><code>./lakehouse demo</code> loads the Acme Outdoor shop, runs dbt and seeds the dashboards</li>"
              "<li><code>./lakehouse sql</code> opens a Trino shell on <code>iceberg.shop</code>; <code>./lakehouse duckdb</code> queries the same tables with DuckDB</li>"
              "<li><code>./lakehouse stream --events 500</code> sends orders through Kafka and Flink into Iceberg; watch the streaming dashboard</li>"
              "<li><code>./lakehouse dbt build</code> rebuilds the marts; the Airflow DAG <code>shop_marts</code> does the same on a schedule</li>")
    page = HOME_TEMPLATE.replace("{{CARDS}}", "\n".join(cards)).replace("{{TRY}}", try_it).replace("{{PROJECT}}", html.escape(stack.project))
    GENERATED.mkdir(exist_ok=True)
    target = GENERATED / "home.html"
    target.write_text(page, encoding="utf-8")
    return target


HOME_TEMPLATE = """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AQUIFER - open data stack</title><style>
:root{color-scheme:dark}body{margin:0;font:16px/1.5 system-ui,-apple-system,Segoe UI,sans-serif;background:#0d1117;color:#e6edf3}
main{max-width:980px;margin:0 auto;padding:48px 24px}h1{font-size:2.4rem;margin:0;display:flex;align-items:center;gap:14px}p.lead{color:#9da7b3;margin:.4rem 0 2rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:14px}
.card{display:flex;flex-direction:column;gap:2px;padding:16px 18px;border:1px solid #2b3440;border-radius:10px;background:#151b23;color:inherit;text-decoration:none}
.card:hover{border-color:#58a6ff}.card span{color:#8b98a5;font-size:.85rem}.card small{color:#6e7a86;margin-top:4px}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:#6e7a86;margin-right:8px}.up{background:#3fb950}.down{background:#f85149}
h2{margin:2.2rem 0 .6rem;font-size:1.1rem}code{background:#1f2630;padding:1px 6px;border-radius:5px}li{margin:.3rem 0}footer{margin-top:2.5rem;color:#6e7a86;font-size:.85rem}
</style></head><body><main><h1><svg width="44" height="44" viewBox="0 0 64 64" aria-hidden="true"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#22d3ee"/><stop offset="1" stop-color="#2563eb"/></linearGradient></defs><rect width="64" height="64" rx="14" fill="url(#g)"/><path d="M10 40 L24 20 L33 31 L40 24 L54 40 Z" fill="#0b1220" opacity=".35"/><path d="M8 44 q6 -5 12 0 t12 0 t12 0 t12 0" fill="none" stroke="#fff" stroke-width="3" stroke-linecap="round"/><path d="M8 52 q6 -5 12 0 t12 0 t12 0 t12 0" fill="none" stroke="#fff" stroke-width="3" stroke-linecap="round" opacity=".6"/></svg>AQUIFER</h1><p class="lead">The open-source, on-premise data stack. Project <code>{{PROJECT}}</code>. Green dot: the service answers.</p>
<div class="grid">{{CARDS}}</div><h2>Try this</h2><ul>{{TRY}}</ul>
<footer>Development stack bound to localhost. Not for production or real customer data.</footer></main>
<script>document.querySelectorAll('[data-probe]').forEach(function(d){fetch(d.dataset.probe,{mode:'no-cors'}).then(function(){d.classList.add('up')}).catch(function(){d.classList.add('down')})})</script>
</body></html>
"""


# --------------------------------------------------------------------------- commands

def cmd_init(args) -> int:
    init_env(force=args.force, project=args.project)
    return 0


def run_checks(stack: Stack, strict_ports: bool = True) -> int:
    failures = warnings = 0

    def record(level: str, message: str, advice: str = "") -> None:
        nonlocal failures, warnings
        {"ok": ok, "warn": warn, "fail": bad}[level](message)
        if advice:
            hint(advice)
        failures += level == "fail"
        warnings += level == "warn"

    heading("System")
    record("ok", f"{platform.system()} {platform.machine()}, Python {platform.python_version()}")
    if sys.version_info < (3, 8):
        record("fail", "Python 3.8 or newer is required")
    if shutil.disk_usage(str(ROOT)).free < 15 * 1024 ** 3:
        record("warn", "Less than 15 GB free disk space; images and volumes need room", "Free some space or prune with: docker system prune")

    heading("Docker")
    info = None
    if not shutil.which("docker"):
        record("fail", "Docker is not installed", "Run ./setup.sh (macOS/Linux) or setup.cmd (Windows), or read docs/INSTALL.md.")
    else:
        version = compose_version()
        minimum = MIN_COMPOSE_REFERENCE if "reference" in stack.extensions else MIN_COMPOSE
        info = docker_info()
        if not info:
            record("fail", "Docker daemon is not reachable", docker_hint())
        else:
            record("ok", f"Docker Engine {info.get('ServerVersion', '?')} ({info.get('OperatingSystem', '?')})")
        if version >= minimum:
            record("ok", f"Docker Compose {'.'.join(map(str, version))}")
        else:
            record("fail", f"Docker Compose v{'.'.join(map(str, minimum))}+ required, found {'.'.join(map(str, version)) if version != (0,) else 'none'}",
                   "Update Docker Desktop or install docker-compose-v2 / docker-compose-plugin.")
        if info:
            memory = info.get("MemTotal", 0) / 1024 ** 3
            need = MIN_DOCKER_MEMORY_GB.get(stack.effective.get("preset", "small"), 7.0)
            if memory < need:
                record("warn", f"Docker has {memory:.1f} GB memory; the '{stack.effective.get('preset')}' preset wants about {need:.0f} GB",
                       "Docker Desktop: Settings > Resources > Memory. Or stop other stacks. Lower Trino/Airflow memory in pattern.yaml.")
            else:
                record("ok", f"Docker memory {memory:.1f} GB, {info.get('NCPU', '?')} CPUs")

    heading("Configuration")
    record("ok", f"Preset {stack.effective['preset']}, profiles {stack.profiles or 'none'}, extensions {stack.extensions or 'none'}")
    if info:
        result = stack.compose("config", "--quiet", profiles=ALL_PROFILES, check=False, capture=True)
        if result.returncode == 0:
            record("ok", "Compose model is valid")
        else:
            record("fail", "Compose model is invalid", (result.stdout or "")[-400:])
    if "reference" in stack.extensions:
        parent = ROOT.parent
        for sibling in ("customer-profile-360-reference", "telco-data-products"):
            if (parent / sibling).is_dir():
                record("ok", f"Reference extension: ../{sibling} found")
            else:
                record("fail", f"Reference extension needs ../{sibling}", "Clone it next to this folder or remove 'reference' from extensions (see docs/EXTENSIONS.md).")
        volume = stack.env.get("JENKINS_REFERENCE_VOLUME", "tdp-jenkins_sm_jenkins_home")
        if info and subprocess.run(["docker", "volume", "inspect", volume], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
            record("fail", f"Reference extension needs the Docker volume '{volume}'", "Start the local Jenkins stack and initialise the reference deployment (docs/EXTENSIONS.md).")

    heading("Ports (127.0.0.1)")
    names = PORT_LABELS
    needed = ["trino", "airflow", "console", "minio_api", "minio_console", "registry", "kafka"]
    if "analytics" in stack.profiles:
        needed.append("superset")
    if "observability" in stack.profiles:
        needed += ["prometheus", "grafana"]
    if "streaming" in stack.profiles:
        needed.append("flink")
    if "notebook" in stack.profiles:
        needed.append("notebook")
    if "reference" in stack.extensions:
        needed.append("showcase")
    busy = 0
    for name in needed:
        port = stack.port(name)
        if not port_in_use(port):
            record("ok", f"{names[name]} port {port} is free")
            continue
        owner, ours = port_owner(port, stack.project) if info else ("", False)
        if ours:
            record("ok", f"{names[name]} port {port} is used by this stack ({owner})")
        else:
            busy += 1
            who = f"container {owner}" if owner else "another program"
            record("fail" if strict_ports else "warn", f"{names[name]} port {port} is taken by {who}",
                   f"Set HOST_{name.upper()}_PORT=<free port> in .env, or run: ./lakehouse init --force to pick free ports automatically.")

    heading("Result")
    if failures:
        bad(f"{failures} problem(s) must be fixed first")
    elif warnings:
        warn(f"Ready with {warnings} warning(s)")
    else:
        ok("Everything looks good. Run ./lakehouse up")
    return 1 if failures else 0


def cmd_doctor(args) -> int:
    stack = Stack.load()
    return run_checks(stack)


def demo_loaded(stack: Stack) -> bool:
    try:
        stack.trino().execute("SELECT 1 FROM iceberg.shop.daily_revenue LIMIT 1")
        return True
    except LakehouseError:
        return False


def cmd_up(args) -> int:
    stack = Stack.load()
    extra = [name for chunk in (args.with_ or []) for name in re.split(r"[,\s]+", chunk) if name]
    unknown = [name for name in extra if name not in OPTIONAL_PROFILES]
    if unknown:
        raise LakehouseError(f"Unknown profile(s): {', '.join(unknown)}. Choose from: {', '.join(OPTIONAL_PROFILES)}")
    stack.effective["profiles"] = list(dict.fromkeys(stack.profiles + extra))
    stack.write_prometheus_targets()
    require_docker(stack)
    started = time.monotonic()
    if not stack.running_services():
        heading("Preflight")
        if run_checks(stack) != 0:
            raise LakehouseError("Fix the problems above and run ./lakehouse up again.")
    heading("Starting the stack (the first run builds images and can take several minutes)")
    build = [] if args.no_build else ["--build"]
    stack.compose("up", "--detach", *build, "--remove-orphans")
    heading("Waiting for services")
    for service in stack.active_services():
        if service.key == "airflow":
            wait_for_airflow(stack)
        elif service.key == "superset":
            wait_for(stack.probe_url(service), service.label, timeout=600, stack=stack, logs_for="superset-init")
        else:
            wait_for(stack.probe_url(service), service.label, stack=stack, logs_for=service.key)
    if "streaming" in stack.profiles:
        wait_for_flink_job(stack)
    home = write_home(stack)
    heading(f"AQUIFER is ready ({human_duration(time.monotonic() - started)})")
    print_endpoints(stack)
    say(f"\n  Home page: {home}")
    if args.demo:
        cmd_demo(argparse.Namespace(skip_dashboards=False), stack=stack)
    elif demo_loaded(stack):
        if "analytics" in stack.profiles:
            say(f"\n  Demo data is loaded. Dashboard: http://localhost:{stack.port('superset')}{SUPERSET_DASHBOARD}")
        else:
            say("\n  Demo data is loaded. Try: ./lakehouse sql \"SELECT * FROM shop.daily_revenue LIMIT 5\"")
    else:
        say("\n  Next: ./lakehouse demo   (loads a sample shop dataset and a ready-made dashboard)")
    if args.open:
        webbrowser.open(home.as_uri())
    return 0


def cmd_quickstart(args) -> int:
    say(BANNER)
    say("Quickstart: configure, start, load the demo and open the home page.")
    return cmd_up(argparse.Namespace(with_=args.with_, demo=True, open=not args.no_open, no_build=False))


def cmd_down(args) -> int:
    stack = Stack.load(auto_init=False)
    require_docker(stack)
    stack.compose("down", "--remove-orphans", profiles=ALL_PROFILES)
    say("Stopped. Data is preserved in Docker volumes; ./lakehouse up resumes it.")
    return 0


def cmd_restart(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    stack.compose("restart", *([args.service] if args.service else []), profiles=ALL_PROFILES)
    return 0


def cmd_status(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    stack.compose("ps", profiles=ALL_PROFILES)
    say("\nService endpoints")
    failures = 0
    running = set(stack.running_services())
    for service in SERVICES:
        configured = (not service.profile or service.profile in stack.profiles or service.key in running) and (not service.extension or service.extension in stack.extensions)
        if not configured:
            continue
        up = url_ready(stack.probe_url(service))
        failures += not up
        say(f"  {service.label:<34} {'ready' if up else 'DOWN ':<6} {stack.url(service)}")
    return 1 if failures else 0


def cmd_info(args) -> int:
    stack = Stack.load()
    heading("Endpoints")
    print_endpoints(stack, show_secrets=args.show_secrets)
    say(f"\n  Project {stack.project}; preset {stack.effective['preset']}; profiles {', '.join(stack.profiles) or 'none'}; extensions {', '.join(stack.extensions) or 'none'}")
    return 0


def connection_guide(stack: Stack, show_secrets: bool = False) -> str:
    trino, minio, kafka, registry = stack.port("trino"), stack.port("minio_api"), stack.port("kafka"), stack.port("registry")
    secret = stack.env.get("MINIO_ROOT_PASSWORD", "") if show_secrets else "<MINIO_ROOT_PASSWORD from .env>"
    user = stack.env.get("MINIO_ROOT_USER", "localadmin")
    return textwrap.dedent(f"""\
        Trino (SQL): any tool that speaks Trino, no password on this local stack
          JDBC URL     jdbc:trino://localhost:{trino}/iceberg      (DBeaver, DataGrip, IntelliJ: user = any name)
          Python       pip install trino
                       from trino.dbapi import connect
                       cur = connect(host="localhost", port={trino}, user="me", catalog="iceberg", schema="shop").cursor()
                       cur.execute("SELECT * FROM daily_revenue LIMIT 5"); print(cur.fetchall())
          dbt          pip install dbt-trino, then in profiles.yml:
                       aquifer: {{target: dev, outputs: {{dev: {{type: trino, method: none, user: me, host: localhost, port: {trino}, database: iceberg, schema: shop, threads: 2}}}}}}
          Shell        ./lakehouse sql

        Object storage (MinIO, S3 API)
          Endpoint     http://localhost:{minio}      path-style addressing, region us-east-1
          Access key   {user}
          Secret key   {secret}
          AWS CLI      AWS_ACCESS_KEY_ID={user} AWS_SECRET_ACCESS_KEY=... aws --endpoint-url http://localhost:{minio} s3 ls s3://warehouse

        Kafka (Redpanda)
          Bootstrap    localhost:{kafka}
          Registry     http://localhost:{registry}
          CLI          ./lakehouse compose exec redpanda rpk topic list

        Inside the Docker network use trino:8080, minio:9000, redpanda:9092, iceberg-rest:8181 (the catalog is not published to the host).""")


def cmd_connect(args) -> int:
    stack = Stack.load()
    heading("Connect your tools")
    say(textwrap.indent(connection_guide(stack, args.show_secrets), "  "))
    return 0


def cmd_open(args) -> int:
    stack = Stack.load()
    target = args.service or "home"
    if target == "home":
        url = write_home(stack).as_uri()
    elif target == "dashboard":
        url = f"http://localhost:{stack.port('superset')}{SUPERSET_DASHBOARD}"
    else:
        match = next((s for s in SERVICES if s.key == target), None)
        if not match:
            raise LakehouseError("Unknown service. Choose: home, dashboard, " + ", ".join(s.key for s in SERVICES))
        url = stack.url(match, token=True)
    say(f"Opening {url}")
    if not webbrowser.open(url):
        say("No browser could be launched; open the address above yourself.")
    return 0


def cmd_sql(args) -> int:
    stack = Stack.load()
    query = args.query
    if args.file:
        query = Path(args.file).read_text(encoding="utf-8")
    elif not query and not sys.stdin.isatty():
        query = sys.stdin.read()
    if not query:
        require_docker(stack)
        say("Trino shell (iceberg catalog). Try: SHOW SCHEMAS;  SELECT * FROM shop.daily_revenue LIMIT 5;  Exit with Ctrl-D.")
        return stack.compose("exec", "trino", "trino", "--user", "lakehouse", "--catalog", "iceberg", check=False).returncode
    client = stack.trino()
    for statement in split_sql(query):
        columns, rows = client.execute(statement)
        if args.format == "json":
            say(json.dumps([dict(zip(columns, row)) for row in rows], default=str, indent=2))
        elif args.format == "csv":
            say(",".join(columns))
            for row in rows:
                say(",".join("" if value is None else str(value) for value in row))
        elif columns:
            say(format_table(columns, rows))
            say(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")
        else:
            say("OK")
    return 0


def cmd_logs(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    return stack.compose("logs", "--follow", "--tail=100", *([args.service] if args.service else []), profiles=ALL_PROFILES, check=False).returncode


def cmd_compose(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    return stack.compose(*args.args, profiles=ALL_PROFILES, check=False).returncode


def confirm(prompt: str, answer: str) -> bool:
    try:
        return input(f"{prompt} Type '{answer}' to continue: ").strip() == answer
    except EOFError:
        return False


def cmd_reset(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    if not args.yes and not confirm("This permanently deletes all lakehouse data (volumes) of this project.", "reset"):
        say("Aborted.")
        return 1
    stack.compose("down", "--volumes", "--remove-orphans", profiles=ALL_PROFILES)
    say("All containers and volumes of this project were removed.")
    return 0


def run_dbt(stack: Stack, dbt_args: Sequence[str], check: bool = True) -> subprocess.CompletedProcess:
    """dbt runs inside the Airflow image (isolated venv); the project is the ./dbt folder."""
    return stack.compose("exec", "-T", "airflow-scheduler", DBT_BIN, *dbt_args, profiles=ALL_PROFILES, check=check)


def run_dbt_quiet(stack: Stack, dbt_args: Sequence[str], label: str) -> bool:
    """Runs dbt, prints its summary line, and dumps the full log only when something failed."""
    started = time.monotonic()
    result = stack.compose("exec", "-T", "airflow-scheduler", DBT_BIN, *dbt_args, profiles=ALL_PROFILES, check=False, capture=True)
    output = result.stdout or ""
    summary = next((line.split("  ", 1)[-1] for line in reversed(output.splitlines()) if "Done. PASS=" in line), "")
    if result.returncode == 0:
        say(f"    ok {label}: {summary or 'passed'} ({time.monotonic() - started:.1f}s)")
        return True
    say(output)
    bad(f"{label} failed")
    return False


def flink_jobs(stack: Stack) -> List[dict]:
    status, body = http_get(f"http://127.0.0.1:{stack.port('flink')}/jobs/overview")
    if status != 200:
        return []
    try:
        return list(json.loads(body).get("jobs", []))
    except ValueError:
        return []


def flink_job_running(stack: Stack, name: str = FLINK_JOB) -> bool:
    return any(job.get("name") == name and job.get("state") == "RUNNING" for job in flink_jobs(stack))


def wait_for_flink_job(stack: Stack, timeout: int = 300) -> None:
    start = time.monotonic()
    while not flink_job_running(stack):
        if time.monotonic() - start > timeout:
            stack.compose("logs", "--tail=40", "flink-submit", check=False)
            raise LakehouseError(f"Flink job {FLINK_JOB} is not running. Inspect: ./lakehouse logs flink-submit")
        time.sleep(2)
    ok(f"Flink job {FLINK_JOB} is running")


def make_events(count: int, rng, spread_hours: float = 0.0, now: Optional[datetime] = None) -> List[str]:
    """Synthetic order events as JSON lines for the Kafka topic shop.orders."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    lines = []
    for _ in range(count):
        quantity = rng.randint(1, 4)
        moment = now - timedelta(seconds=rng.random() * spread_hours * 3600)
        lines.append(json.dumps({
            "event_id": "%016x" % rng.getrandbits(64),
            "order_id": rng.randint(1_000_000, 9_999_999),
            "customer_id": rng.randint(1, 5000),
            "product_id": rng.randint(1, 48),
            "quantity": quantity,
            "amount": round(quantity * rng.uniform(9, 120), 2),
            "status": rng.choices(["completed", "refunded", "cancelled"], weights=[90, 5, 5])[0],
            "event_time": moment.isoformat(timespec="milliseconds"),
        }))
    return lines


def order_event_count(stack: Stack) -> int:
    try:
        return int(stack.trino().execute("SELECT count(*) FROM iceberg.shop.order_events")[1][0][0])
    except (LakehouseError, IndexError, ValueError):
        return 0


def produce_events(stack: Stack, lines: Sequence[str], rate: float = 0.0) -> None:
    command = stack.compose_prefix(ALL_PROFILES) + ["exec", "-T", "redpanda", "rpk", "topic", "produce", ORDER_TOPIC]
    process = subprocess.Popen(command, cwd=str(ROOT), stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, universal_newlines=True)
    try:
        for index, line in enumerate(lines):
            process.stdin.write(line + "\n")
            if rate > 0 and (index + 1) % max(1, int(rate)) == 0:
                process.stdin.flush()
                time.sleep(1)
        process.stdin.close()
        error = process.stderr.read()
        if process.wait() != 0:
            raise LakehouseError(f"Producing to Kafka failed: {error.strip()[:300]}")
    except BrokenPipeError:
        raise LakehouseError(f"Producing to Kafka failed: {process.stderr.read().strip()[:300]}")


def stream_events(stack: Stack, count: int, spread_hours: float = 0.0, rate: float = 0.0, wait: bool = True, seed: Optional[int] = None) -> int:
    import random
    if "redpanda" not in stack.running_services():
        raise LakehouseError("Redpanda is not running. Start the stack first: ./lakehouse up")
    before = order_event_count(stack)
    say(f"Producing {count} order events to Kafka topic {ORDER_TOPIC}" + (f" at about {rate:g}/s" if rate else "") + " ...")
    produce_events(stack, make_events(count, random.Random(seed), spread_hours), rate)
    if not wait:
        return 0
    if not flink_job_running(stack):
        warn(f"The Flink job {FLINK_JOB} is not running; events wait in Kafka. Start the streaming profile: ./lakehouse up --with streaming")
        return 1
    say("Waiting for Flink to land them in Iceberg (checkpoint every 10 s) ...")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        landed = order_event_count(stack) - before
        if landed >= count:
            ok(f"{landed} events landed in iceberg.shop.order_events")
            return 0
        time.sleep(3)
    bad(f"Only {max(0, order_event_count(stack) - before)} of {count} events arrived in time. Inspect: ./lakehouse logs flink-taskmanager")
    return 1


def cmd_demo(args, stack: Optional[Stack] = None) -> int:
    stack = stack or Stack.load()
    require_docker(stack)
    heading("Demo data: Acme Outdoor web shop (synthetic)")
    wait_for(stack.probe_url(next(s for s in SERVICES if s.key == "trino")), "Trino", timeout=240, stack=stack, logs_for="trino")
    client = stack.trino()
    for name in DEMO_SQL:
        path = ROOT / "demo" / "sql" / name
        statements = split_sql(path.read_text(encoding="utf-8"))
        say(f"  {name}: {len(statements)} statements (raw tables)")
        for statement in statements:
            label = describe_statement(statement)
            started = time.monotonic()
            client.execute(statement)
            say(f"    ok {label} ({time.monotonic() - started:.1f}s)")
    say("  dbt build: staging, marts and tests")
    if "airflow-scheduler" not in stack.running_services():
        raise LakehouseError("Airflow is not running; dbt runs inside it. Start the stack first: ./lakehouse up")
    if not run_dbt_quiet(stack, ["build", "--exclude", "tag:streaming"], "dbt build"):
        raise LakehouseError("dbt build failed; see the output above. Re-run with: ./lakehouse dbt build")
    streaming = "streaming" in stack.profiles and flink_job_running(stack)
    if streaming:
        say("  Flink: replaying 600 synthetic order events from the last 6 hours through Kafka")
        try:
            client.execute("DELETE FROM iceberg.shop.order_events")  # keeps `demo` repeatable: the replay is deterministic
        except LakehouseError:
            pass
        stream_events(stack, 600, spread_hours=6, seed=42)
        run_dbt_quiet(stack, ["build", "--select", "tag:streaming"], "dbt build (streaming mart)")
    _, rows = client.execute(
        "SELECT 'customers', count(*) FROM iceberg.shop.customers UNION ALL SELECT 'orders', count(*) FROM iceberg.shop.orders "
        "UNION ALL SELECT 'daily_revenue', count(*) FROM iceberg.shop.daily_revenue UNION ALL SELECT 'customer_value', count(*) FROM iceberg.shop.customer_value")
    say("\n" + format_table(["table", "rows"], rows))
    if "analytics" in stack.profiles and not args.skip_dashboards:
        if "superset" in stack.running_services():
            say("\nSeeding Superset (connection, datasets, charts, dashboards) ...")
            result = stack.compose("exec", "-T", "superset", "python", "/app/analytics/bootstrap.py", check=False)
            if result.returncode == 0:
                say(f"Dashboard: http://localhost:{stack.port('superset')}{SUPERSET_DASHBOARD}")
                if streaming:
                    say(f"Streaming: http://localhost:{stack.port('superset')}{SUPERSET_STREAMING}")
            else:
                warn("Superset seeding failed; see ./lakehouse logs superset")
        else:
            say("\nSuperset is not running; start it with ./lakehouse up --with analytics, then run ./lakehouse demo again.")
    say("\nTry it:  ./lakehouse sql \"SELECT country, round(sum(net_revenue)) AS revenue FROM shop.daily_revenue GROUP BY 1 ORDER BY 2 DESC\"")
    say("         ./lakehouse duckdb \"SELECT count(*) FROM lake.shop.orders\"     ./lakehouse stream --events 100")
    return 0


def cmd_dbt(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    dbt_args = args.args or ["build", "--exclude", "tag:streaming"]
    return run_dbt(stack, dbt_args, check=False).returncode


def cmd_duckdb(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    if "duckdb" not in stack.running_services():
        raise LakehouseError("The duckdb container is not running. Start the stack first: ./lakehouse up")
    base = ["exec"] + (["-T"] if args.query else []) + ["duckdb", "duckdb", "-init", DUCKDB_INIT]
    if args.query:
        flags = {"csv": ["-csv"], "json": ["-json"], "table": []}[args.format]
        return stack.compose(*base, *flags, "-c", args.query, profiles=ALL_PROFILES, check=False).returncode
    say("DuckDB shell with the Iceberg catalog attached as `lake` (try: SHOW ALL TABLES;  SELECT * FROM lake.shop.daily_revenue LIMIT 5;). Exit with Ctrl-D.")
    return stack.compose(*base, profiles=ALL_PROFILES, check=False).returncode


def cmd_flink(args) -> int:
    stack = Stack.load()
    if args.action == "sql":
        require_docker(stack)
        say("Flink SQL client. Try: SHOW JOBS;  Exit with Ctrl-D.")
        return stack.compose("exec", "flink-jobmanager", "/opt/flink/bin/sql-client.sh", profiles=ALL_PROFILES, check=False).returncode
    jobs = flink_jobs(stack)
    if not jobs:
        say("No Flink jobs found (is the streaming profile running? ./lakehouse up --with streaming)")
        return 1
    rows = [[job.get("name"), job.get("state"), job.get("jid"), datetime.fromtimestamp(job.get("start-time", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S")] for job in jobs]
    say(format_table(["job", "state", "id", "started"], rows))
    say(f"\nWeb UI: http://localhost:{stack.port('flink')}")
    return 0 if flink_job_running(stack) else 1


def cmd_stream(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    return stream_events(stack, args.events, spread_hours=args.spread_hours, rate=args.rate, wait=not args.no_wait)


BACKUP_VOLUMES = ("postgres_data", "minio_data", "redpanda_data", "airflow_home", "superset_home")
BACKUP_MEMBER = re.compile(r"^volumes/([a-z_]+)\.tar\.gz$")


def volume_exists(name: str) -> bool:
    return subprocess.run(["docker", "volume", "inspect", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def build_manifest(project: str, volumes: Sequence[str], moment: Optional[datetime] = None) -> dict:
    return {"format": 1, "tool": "aquifer", "version": VERSION, "project": project, "volumes": list(volumes),
            "created": (moment or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")}


def bundle_backup(target: Path, manifest: dict, volume_archives: Dict[str, Path], env_file: Optional[Path]) -> None:
    import tarfile
    target.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(target, "w") as bundle:
        manifest_path = target.with_suffix(".manifest.tmp")
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        try:
            bundle.add(manifest_path, arcname="manifest.json")
        finally:
            manifest_path.unlink()
        for name, archive in volume_archives.items():
            bundle.add(archive, arcname=f"volumes/{name}.tar.gz")
        if env_file and env_file.is_file():
            bundle.add(env_file, arcname="env")
    try:
        os.chmod(target, 0o600)
    except OSError:
        pass


def read_backup(source: Path, workdir: Path) -> Tuple[dict, Dict[str, Path], Optional[Path]]:
    """Extract only the members we wrote ourselves, by exact name, so a crafted archive cannot write elsewhere."""
    import tarfile
    archives: Dict[str, Path] = {}
    env_path: Optional[Path] = None
    with tarfile.open(source, "r") as bundle:
        names = bundle.getnames()
        if "manifest.json" not in names:
            raise LakehouseError(f"{source.name} is not an AQUIFER backup (no manifest.json)")
        member = bundle.extractfile("manifest.json")
        manifest = json.loads(member.read().decode("utf-8")) if member else {}
        if manifest.get("tool") != "aquifer" or manifest.get("format") != 1:
            raise LakehouseError("Unsupported backup format")
        for name in names:
            match = BACKUP_MEMBER.match(name)
            if match and match.group(1) in BACKUP_VOLUMES:
                destination = workdir / f"{match.group(1)}.tar.gz"
                handle = bundle.extractfile(name)
                with open(destination, "wb") as out:
                    shutil.copyfileobj(handle, out)
                archives[match.group(1)] = destination
            elif name == "env":
                env_path = workdir / "env"
                handle = bundle.extractfile(name)
                with open(env_path, "wb") as out:
                    shutil.copyfileobj(handle, out)
    return manifest, archives, env_path


def cmd_backup(args) -> int:
    """Stops the stack for a consistent copy, archives the data volumes into one file, then starts it again."""
    stack = Stack.load()
    require_docker(stack)
    volumes = [name for name in BACKUP_VOLUMES if volume_exists(f"{stack.project}_{name}")]
    if not volumes:
        raise LakehouseError(f"No data volumes found for project '{stack.project}'. Start the stack first: ./lakehouse up")
    was_running = bool(stack.running_services())
    if was_running and not args.yes and not confirm("The stack is stopped for a consistent backup and restarted afterwards.", "backup"):
        say("Aborted.")
        return 1
    target = Path(args.file).expanduser() if args.file else ROOT / "backups" / f"{stack.project}-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.tar"
    if was_running:
        stack.compose("down", "--remove-orphans", profiles=ALL_PROFILES)
    GENERATED.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="backup-", dir=str(GENERATED)))  # inside the project: Docker is sure to share it
    try:
        archives: Dict[str, Path] = {}
        for name in volumes:
            say(f"  archiving {name} ...")
            subprocess.run(["docker", "run", "--rm", "-v", f"{stack.project}_{name}:/source:ro", "-v", f"{work}:/backup", "alpine:3",
                            "sh", "-c", f"tar czf /backup/{name}.tar.gz -C /source ."], check=True)
            archives[name] = work / f"{name}.tar.gz"
        bundle_backup(target, build_manifest(stack.project, volumes), archives, ENV_FILE)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    ok(f"Backup written: {target} ({target.stat().st_size / 1024 ** 2:.1f} MB)")
    say("  It contains your data and .env (credentials): keep it private. Restore with: ./lakehouse restore " + str(target))
    if was_running and not args.stay_down:
        cmd_up(argparse.Namespace(with_=None, demo=False, open=False, no_build=True))
    return 0


def cmd_restore(args) -> int:
    """Replaces the project's data volumes with those from a backup file."""
    stack = Stack.load()
    require_docker(stack)
    source = Path(args.file).expanduser()
    if not source.is_file():
        raise LakehouseError(f"Backup not found: {source}")
    GENERATED.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="restore-", dir=str(GENERATED)))
    try:
        manifest, archives, env_path = read_backup(source, work)
        if not archives:
            raise LakehouseError("The backup contains no data volumes")
        say(f"Backup of project '{manifest.get('project')}' from {manifest.get('created')}: {', '.join(archives)}")
        if not args.yes and not confirm(f"This REPLACES all data of project '{stack.project}'.", "restore"):
            say("Aborted.")
            return 1
        stack.compose("down", "--remove-orphans", profiles=ALL_PROFILES)
        for name, archive in archives.items():
            volume = f"{stack.project}_{name}"
            if volume_exists(volume):
                subprocess.run(["docker", "volume", "rm", volume], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["docker", "volume", "create", "--label", f"com.docker.compose.project={stack.project}",
                            "--label", f"com.docker.compose.volume={name}", volume], check=True, stdout=subprocess.DEVNULL)
            say(f"  restoring {name} ...")
            subprocess.run(["docker", "run", "--rm", "-v", f"{volume}:/target", "-v", f"{work}:/backup:ro", "alpine:3",
                            "sh", "-c", f"cd /target && tar xzf /backup/{archive.name}"], check=True)
        if args.with_env and env_path:
            if ENV_FILE.exists():
                ENV_FILE.replace(ENV_FILE.with_name(f".env.bak-{datetime.now().strftime('%Y%m%d%H%M%S')}"))
            shutil.copyfile(env_path, ENV_FILE)
            say("  .env restored from the backup (the previous one was kept as .env.bak-*)")
        elif env_path:
            warn("Passwords stored inside the volumes (Airflow, Superset) come from the backup's .env. If logins fail, restore with --with-env.")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    ok("Data restored. Start the stack with: ./lakehouse up")
    return 0


def cmd_load(args) -> int:
    """Load a CSV, TSV or JSON-lines file into a new Iceberg table, with inferred column types."""
    stack = Stack.load()
    path = Path(args.file).expanduser()
    if not path.is_file():
        raise LakehouseError(f"File not found: {path}")
    header, rows = read_rows(path, args.delimiter)
    if not rows:
        raise LakehouseError(f"{path.name} has a header but no rows")
    if len(rows) > 1_000_000:
        warn(f"{len(rows):,} rows is a lot for INSERT-based loading; consider writing Parquet to MinIO instead (docs/PIPELINES.md)")
    names: List[str] = []
    for column in header:
        names.append(sanitize_identifier(column, names))
    columns = [(name, infer_column_type(row[index] for row in rows)) for index, name in enumerate(names)]
    default_name = sanitize_identifier(path.stem)
    schema, _, table_name = (args.table or f"data.{default_name}").rpartition(".")
    schema = schema or "data"
    if not (re.fullmatch(r"[a-z][a-z0-9_]*", schema) and re.fullmatch(r"[a-z][a-z0-9_]*", table_name)):
        raise LakehouseError("Use --table schema.name with lower-case letters, digits and underscores")
    target = f"iceberg.{schema}.{table_name}"
    client = stack.trino()
    heading(f"Loading {path.name} into {target}")
    say("  columns: " + ", ".join(f"{name} {column_type}" for name, column_type in columns))
    client.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema} WITH (location = 's3://warehouse/{schema}/')")
    exists = bool(client.execute(f"SHOW TABLES FROM iceberg.{schema} LIKE '{table_name}'")[1])
    if exists and not args.replace:
        raise LakehouseError(f"{target} already exists. Use --replace to drop and reload it, or choose another --table.")
    if exists:
        client.execute(f"DROP TABLE {target}")
    client.execute(table_ddl(target, columns))
    started = time.monotonic()
    batches = 0
    for statement in insert_batches(target, columns, rows):
        client.execute(statement)
        batches += 1
    _, counted = client.execute(f"SELECT count(*) FROM {target}")
    ok(f"{counted[0][0]:,} rows loaded in {batches} batch(es), {time.monotonic() - started:.1f}s")
    say(f"\n  Query it:   ./lakehouse sql \"SELECT * FROM {schema}.{table_name} LIMIT 10\"")
    say(f"              ./lakehouse duckdb \"SELECT * FROM lake.{schema}.{table_name} LIMIT 10\"")
    say(f"  Chart it:   Superset > Datasets > + Dataset > connection 'Lakehouse (Trino, general)', schema {schema}, table {table_name}")
    return 0


def cmd_test(args) -> int:
    """Unit tests of the launcher and the pattern renderer; no Docker needed."""
    ensure_yaml()
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if ".pattern" in path)
    failed = 0
    for directory, extra in ((ROOT, ["discover", "-s", "tests"]), (ROOT / "patterns", ["test_pattern"])):
        failed += subprocess.call([sys.executable, "-m", "unittest", *extra, "-v" if args.verbose else "-q"], cwd=str(directory), env=env) != 0
    return 1 if failed else 0


def cmd_smoke(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    say("Waiting for the iceberg_smoke DAG to be discovered")
    for attempt in range(60):
        listed = stack.compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "list", "--output", "plain", check=False, capture=True)
        if "iceberg_smoke" in (listed.stdout or ""):
            break
        time.sleep(2)
    else:
        raise LakehouseError("Airflow has not discovered iceberg_smoke. Inspect: ./lakehouse logs airflow-dag-processor")
    stack.compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "unpause", "iceberg_smoke", capture=True)
    run_id = "local_smoke_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stack.compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "trigger", "--run-id", run_id, "iceberg_smoke", capture=True)
    say("Waiting for the Iceberg round trip (Airflow -> Trino -> Iceberg -> MinIO)")
    for attempt in range(120):
        state = stack.compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "state", "iceberg_smoke", run_id, check=False, capture=True)
        last = ((state.stdout or "").strip().splitlines() or [""])[-1].strip().lower()
        if last == "success":
            ok(f"Run {run_id} wrote one Iceberg row and read it back through Trino")
            return 0
        if last == "failed":
            stack.compose("exec", "-T", "airflow-scheduler", "airflow", "tasks", "states-for-dag-run", "iceberg_smoke", run_id, check=False)
            raise LakehouseError("The smoke run failed. Inspect: ./lakehouse logs airflow-scheduler")
        time.sleep(2)
    raise LakehouseError("The smoke run timed out. Inspect: ./lakehouse logs airflow-scheduler")


def cmd_check(args) -> int:
    """Acceptance run used by CI: endpoints, Iceberg round trip, demo tables, analytics API."""
    stack = Stack.load()
    require_docker(stack)
    results = []
    heading("Endpoints")
    results.append(cmd_status(args) == 0)
    heading("Iceberg round trip")
    try:
        results.append(cmd_smoke(args) == 0)
    except LakehouseError as error:
        bad(str(error))
        results.append(False)
    heading("Demo data")
    demo_ready = False
    try:
        _, rows = stack.trino().execute("SELECT count(*) FROM iceberg.shop.orders")
        ok(f"iceberg.shop.orders has {rows[0][0]} rows")
        results.append(rows[0][0] > 0)
        demo_ready = rows[0][0] > 0
    except LakehouseError:
        warn("Demo data not loaded (run ./lakehouse demo); skipping")
    running = stack.running_services()
    if demo_ready and "airflow-scheduler" in running:
        heading("dbt tests")
        results.append(run_dbt_quiet(stack, ["test", "--exclude", "tag:streaming"], "dbt test"))
    if "duckdb" in running:
        heading("DuckDB reads the same Iceberg tables")
        probe = stack.compose("exec", "-T", "duckdb", "duckdb", "-init", DUCKDB_INIT, "-csv", "-c", "SELECT count(*) FROM lake.shop.orders", profiles=ALL_PROFILES, check=False, capture=True)
        passed = probe.returncode == 0 and "30000" in (probe.stdout or "")
        (ok if passed else bad)("DuckDB counted the orders through the Iceberg REST catalog" if passed else "DuckDB could not read iceberg.shop.orders: " + (probe.stdout or "")[-200:])
        results.append(passed or not demo_ready)
    if "flink-jobmanager" in running:
        heading("Streaming: Kafka -> Flink -> Iceberg")
        if flink_job_running(stack):
            results.append(stream_events(stack, 100) == 0)
        else:
            bad(f"Flink job {FLINK_JOB} is not running")
            results.append(False)
    if "superset" in running:
        heading("Analytics")
        results.append(cmd_analytics_check(args) == 0)
    return 0 if all(results) else 1


def cmd_analytics(args) -> int:
    stack = Stack.load()
    stack.effective["profiles"] = list(dict.fromkeys(stack.profiles + ["analytics"]))
    require_docker(stack)
    stack.compose("up", "--detach", "--build", "--wait", "--wait-timeout", "600", "superset")
    service = next(s for s in SERVICES if s.key == "superset")
    say(f"Superset: {stack.url(service)}  (user {stack.env.get('SUPERSET_ADMIN_USER', 'admin')}, password in .env)")
    return 0


def cmd_observability(args) -> int:
    stack = Stack.load()
    stack.effective["profiles"] = list(dict.fromkeys(stack.profiles + ["observability"]))
    require_docker(stack)
    stack.compose("up", "--detach", "prometheus", "blackbox", "grafana")
    say(f"Prometheus: http://localhost:{stack.port('prometheus')}\nGrafana: http://localhost:{stack.port('grafana')}/d/local-lakehouse")
    return 0


def cmd_analytics_check(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    return stack.compose("exec", "-T", "superset", "python", "/app/analytics/verify.py", profiles=ALL_PROFILES, check=False).returncode


def cmd_spark_check(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    stack.compose("up", "--detach", "spark-storage-init", profiles=["spark"])
    return stack.compose("run", "--rm", "spark", profiles=["spark"], check=False).returncode


def cmd_pattern(args) -> int:
    stack = Stack.load()
    say(json.dumps(stack.effective, indent=2))
    say("\nThese non-secret settings include .env overrides.")
    return 0


def cmd_pattern_check(args) -> int:
    stack = Stack.load()
    if "reference" not in stack.extensions:
        raise LakehouseError("pattern-check runs the FLUID acceptance example and needs the 'reference' extension (docs/EXTENSIONS.md).")
    require_docker(stack)
    return stack.compose("exec", "-T", "airflow-dag-processor", "python", "/opt/aquifer/pattern/accept.py",
                         "/opt/aquifer/pattern/examples/inventory-balance", "--verifier",
                         "/opt/customer-profile-360/scripts/local_runtime.py", "--generate-dbt-tests", check=False).returncode


def cmd_lint(args) -> int:
    """Validate the compose model for every extension and profile combination (used by CI)."""
    stack = Stack.load()
    require_docker(stack)
    failures = 0
    for extensions in ([], ["reference"]):
        stack.effective["extensions"] = extensions
        result = stack.compose("config", "--quiet", profiles=ALL_PROFILES, check=False, capture=True)
        label = "core" + (" + " + ", ".join(extensions) if extensions else "")
        if result.returncode == 0:
            ok(f"compose model valid: {label}")
        else:
            bad(f"compose model invalid: {label}\n{result.stdout}")
            failures += 1
    return 1 if failures else 0


def cmd_catalog_restore_check(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    lock = ROOT / ".catalog-restore.lock"
    try:
        lock.mkdir()
    except FileExistsError:
        raise LakehouseError("Another catalog restore drill is running. If an earlier run was killed, remove .catalog-restore.lock after verifying it stopped.")
    sql = ("SELECT row_to_json(registration)::text FROM (SELECT * FROM iceberg_tables "
           "ORDER BY catalog_name, table_namespace, table_name) registration")
    try:
        baseline = stack.compose("exec", "-T", "postgres", "psql", "-U", "platform", "-d", "iceberg", "-Atc", sql, capture=True).stdout.strip()
        if not baseline:
            raise LakehouseError("Create at least one Iceberg table first (./lakehouse demo), then run the restore drill.")
        with tempfile.TemporaryDirectory(prefix="catalog-restore-") as scratch:
            dump = Path(scratch) / "catalog.dump"
            with open(dump, "wb") as handle:
                subprocess.run(stack.compose_prefix(stack.profiles) + ["exec", "-T", "postgres", "pg_dump", "-U", "platform", "-d", "iceberg", "--format=custom", "--no-owner", "--no-acl"],
                               cwd=str(ROOT), stdout=handle, check=True)
            stack.compose("up", "--detach", "--wait", "--wait-timeout", "60", "catalog-restore-db", profiles=["recovery"])
            try:
                with open(dump, "rb") as handle:
                    subprocess.run(stack.compose_prefix(["recovery"]) + ["exec", "-T", "catalog-restore-db", "pg_restore", "-U", "restore_drill", "-d", "restore_drill", "--exit-on-error", "--no-owner", "--no-acl"],
                                   cwd=str(ROOT), stdin=handle, check=True)
                restored = stack.compose("exec", "-T", "catalog-restore-db", "psql", "-U", "restore_drill", "-d", "restore_drill", "-Atc", sql, profiles=["recovery"], capture=True).stdout.strip()
            finally:
                stack.compose("rm", "--stop", "--force", "catalog-restore-db", profiles=["recovery"], check=False, capture=True)
        if baseline != restored:
            raise LakehouseError("Catalog restore verification failed; registration rows differ, or a live writer changed the catalog during the drill.")
        ok("Catalog restore passed: all registration fields match in an isolated, network-disabled instance.")
        say("Scope: PostgreSQL catalog metadata only; object-store data and disaster recovery are not proven.")
        return 0
    finally:
        shutil.rmtree(lock, ignore_errors=True)


def cmd_security_check(args) -> int:
    stack = Stack.load()
    require_docker(stack)
    if os.name == "nt" or not shutil.which("bash"):
        raise LakehouseError("security-check needs bash, openssl and htpasswd: run it on macOS, Linux or inside WSL.")
    env = {**os.environ, "COMPOSE_PROJECT_NAME": stack.project}
    subprocess.run(["bash", str(ROOT / "security" / "prepare.sh")], cwd=str(ROOT), env=env, check=True)
    credentials = Path(os.environ.get("LOCAL_SECURITY_ROOT", str(Path.home() / ".local/share/local-lakehouse-security"))) / stack.project
    try:
        stack.compose("up", "--detach", "--no-deps", "--wait", "--wait-timeout", "150", "trino-secure", profiles=["security-validation"])
        subprocess.run(["docker", "run", "--rm", "--network", f"{stack.project}_default", "--user", f"{os.getuid()}:0", "--cap-drop", "ALL",
                        "--security-opt", "no-new-privileges:true", "--env", "PYTHONUSERBASE=/home/airflow/.local", "--entrypoint", "python",
                        "-v", f"{credentials}/ca.crt:/credentials/ca.crt:ro",
                        "-v", f"{credentials}/customer-profile-reference.password:/credentials/customer-profile-reference.password:ro",
                        "-v", f"{credentials}/showcase.password:/credentials/showcase.password:ro",
                        "-v", f"{ROOT}/security/verify_trino.py:/verify_trino.py:ro",
                        "aquifer-airflow:3.3.2", "/verify_trino.py"], check=True)
    finally:
        stack.compose("stop", "trino-secure", profiles=["security-validation"], check=False, capture=True)
    return 0


# --------------------------------------------------------------------------- argument parsing

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakehouse", formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=BANNER + "\nThe open-source, on-premise data stack: Airflow, Trino, Iceberg, Redpanda and Superset in one command.",
                                     epilog=textwrap.dedent("""\
                                         first time:   ./lakehouse quickstart
                                         day to day:   ./lakehouse up | status | sql | logs | down
                                         wire a tool:  ./lakehouse connect
                                         clean slate:  ./lakehouse reset
                                         Windows:      lakehouse.cmd quickstart
                                     """))
    parser.add_argument("--version", action="version", version=f"AQUIFER {VERSION}")
    sub = parser.add_subparsers(dest="command", metavar="command")

    def add(name, func, help_text, aliases=()):
        p = sub.add_parser(name, help=help_text, aliases=list(aliases))
        p.set_defaults(func=func)
        return p

    add("quickstart", cmd_quickstart, "configure, start, load the demo and open the home page").add_argument("--no-open", action="store_true", help="do not open a browser")
    sub.choices["quickstart"].add_argument("--with", dest="with_", action="append", metavar="PROFILES", help="also start: analytics, observability, streaming, spark, notebook")
    add("init", cmd_init, "create .env with generated passwords and free ports")
    sub.choices["init"].add_argument("--force", action="store_true", help="replace an existing .env (a backup is kept)")
    sub.choices["init"].add_argument("--project", default=DEFAULT_PROJECT, help="Compose project name")
    add("doctor", cmd_doctor, "check Docker, memory, ports and configuration")
    up = add("up", cmd_up, "build and start the stack, wait until it is ready")
    up.add_argument("--with", dest="with_", action="append", metavar="PROFILES", help="also start: analytics, observability, streaming, spark, notebook (comma separated)")
    up.add_argument("--demo", action="store_true", help="load the demo dataset and dashboard afterwards")
    up.add_argument("--open", action="store_true", help="open the home page when ready")
    up.add_argument("--no-build", action="store_true", help="skip rebuilding images")
    add("down", cmd_down, "stop containers, keep data")
    restart = add("restart", cmd_restart, "restart all services or one")
    restart.add_argument("service", nargs="?")
    add("status", cmd_status, "show containers and probe every endpoint (non-zero when something is down)", aliases=["ps"])
    info = add("info", cmd_info, "print URLs and logins", aliases=["urls"])
    info.add_argument("--show-secrets", action="store_true", help="print passwords from .env")
    connect = add("connect", cmd_connect, "connection snippets for DBeaver, Python, dbt, S3 and Kafka tools", aliases=["connections"])
    connect.add_argument("--show-secrets", action="store_true", help="print the MinIO secret key from .env")
    op = add("open", cmd_open, "open the home page, the dashboard or one service in the browser")
    op.add_argument("service", nargs="?", help="home, dashboard, superset, airflow, trino, ...")
    sql = add("sql", cmd_sql, "run SQL on Trino (no argument: interactive shell)")
    sql.add_argument("query", nargs="?")
    sql.add_argument("-f", "--file", help="run statements from a file")
    sql.add_argument("--format", choices=["table", "csv", "json"], default="table")
    logs = add("logs", cmd_logs, "follow logs of all services or one")
    logs.add_argument("service", nargs="?")
    add("demo", cmd_demo, "load the sample shop, run dbt, replay events through Flink, seed the dashboards").add_argument("--skip-dashboards", action="store_true")
    dbt = add("dbt", cmd_dbt, "run dbt in the Airflow image (default: build the shop project); e.g. dbt test, dbt run -s daily_revenue")
    dbt.add_argument("args", nargs=argparse.REMAINDER)
    duck = add("duckdb", cmd_duckdb, "DuckDB shell with the Iceberg catalog attached as `lake`; or run one query")
    duck.add_argument("query", nargs="?")
    duck.add_argument("--format", choices=["table", "csv", "json"], default="table")
    fl = add("flink", cmd_flink, "show Flink jobs, or `flink sql` for the SQL client")
    fl.add_argument("action", nargs="?", choices=["status", "sql"], default="status")
    st = add("stream", cmd_stream, "produce synthetic order events to Kafka and wait until Flink lands them in Iceberg")
    st.add_argument("--events", type=int, default=200, help="number of events (default 200)")
    st.add_argument("--rate", type=float, default=0.0, help="events per second; 0 sends them at once")
    st.add_argument("--spread-hours", type=float, default=0.0, help="spread event times over the last N hours")
    st.add_argument("--no-wait", action="store_true", help="do not wait for the events to arrive in Iceberg")
    bk = add("backup", cmd_backup, "archive all data volumes (and .env) into one file; the stack is stopped briefly")
    bk.add_argument("file", nargs="?", help="output file (default: backups/<project>-backup-<time>.tar)")
    bk.add_argument("--yes", action="store_true", help="do not ask before stopping the stack")
    bk.add_argument("--stay-down", action="store_true", help="leave the stack stopped afterwards")
    rs = add("restore", cmd_restore, "replace this project's data with a backup file")
    rs.add_argument("file")
    rs.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    rs.add_argument("--with-env", action="store_true", help="also restore the backup's .env (credentials stored inside the volumes match it)")
    ld = add("load", cmd_load, "load a CSV, TSV or JSON-lines file into an Iceberg table (types are inferred)")
    ld.add_argument("file")
    ld.add_argument("--table", help="target as schema.name (default: data.<file name>)")
    ld.add_argument("--delimiter", help="column delimiter for CSV-like files (default: comma, tab for .tsv)")
    ld.add_argument("--replace", action="store_true", help="drop and reload the table if it exists")
    tst = add("test", cmd_test, "run the unit tests (no Docker needed)")
    tst.add_argument("-v", "--verbose", action="store_true")
    add("smoke", cmd_smoke, "Airflow -> Trino -> Iceberg -> MinIO round trip")
    add("check", cmd_check, "full acceptance run (used by CI)")
    reset = add("reset", cmd_reset, "delete all containers and data of this project")
    reset.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    compose = add("compose", cmd_compose, "run any docker compose command with this project's files")
    compose.add_argument("args", nargs=argparse.REMAINDER)
    add("lint", cmd_lint, "validate the compose model for all extension combinations")
    add("pattern", cmd_pattern, "print the effective non-secret configuration")
    add("pattern-check", cmd_pattern_check, "FLUID acceptance example (reference extension)")
    add("analytics", cmd_analytics, "start only Superset")
    add("analytics-check", cmd_analytics_check, "run every seeded chart through Trino")
    add("observability", cmd_observability, "start Prometheus and Grafana")
    add("spark-check", cmd_spark_check, "bounded Spark structured streaming check")
    add("catalog-restore-check", cmd_catalog_restore_check, "restore the Iceberg catalog into an isolated database")
    add("security-check", cmd_security_check, "TLS and password authentication candidate (macOS, Linux, WSL)")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args_list = normalise_argv(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if not args_list or args_list == ["--help"]:
        parser.print_help()
        return 0
    args = parser.parse_args(args_list)
    if not getattr(args, "func", None):
        parser.print_help()
        return 0
    try:
        return int(args.func(args) or 0)
    except LakehouseError as error:
        say(f"\n{paint('Error:', '31')} {error}")
        return 1
    except subprocess.CalledProcessError as error:
        say(f"\n{paint('Error:', '31')} command failed ({error.returncode}): {' '.join(map(str, error.cmd))[:300]}")
        return error.returncode or 1
    except KeyboardInterrupt:
        say("\nInterrupted.")
        return 130
    except BrokenPipeError:
        try:
            sys.stdout = open(os.devnull, "w")
        except OSError:
            pass
        return 0


if __name__ == "__main__":
    sys.exit(main())
