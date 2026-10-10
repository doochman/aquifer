from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path

import yaml


PRESETS = {
    "small": {"trino_memory_mb": 3072, "trino_heap_mb": 2048, "trino_cpus": 2, "airflow_scheduler_memory_mb": 3072, "airflow_parallelism": 2},
    "medium": {"trino_memory_mb": 6144, "trino_heap_mb": 4096, "trino_cpus": 4, "airflow_scheduler_memory_mb": 4096, "airflow_parallelism": 4},
}
PORTS = {"trino": 8093, "airflow": 8095, "console": 8094, "minio_api": 9000, "minio_console": 9001, "kafka": 19092, "registry": 18081, "showcase": 8200, "prometheus": 8290, "grafana": 8291, "superset": 8280, "flink": 8283, "notebook": 8284}
PROFILES = {"observability", "streaming", "analytics", "spark", "notebook"}
EXTENSIONS = {"reference"}


def effective(resolved: dict, environment: dict) -> dict:
    result = {**resolved, "ports": dict(resolved["ports"]), "resources": dict(resolved["resources"])}
    if "LAKEHOUSE_EXTENSIONS" in environment:
        result["extensions"] = [name for name in re.split(r"[,\s]+", environment["LAKEHOUSE_EXTENSIONS"]) if name]
        if any(name not in EXTENSIONS for name in result["extensions"]) or len(set(result["extensions"])) != len(result["extensions"]):
            raise ValueError("Effective extensions are unsupported or duplicated")
    if "COMPOSE_PROFILES" in environment:
        result["profiles"] = [profile for profile in environment["COMPOSE_PROFILES"].split(",") if profile]
        if any(profile not in PROFILES for profile in result["profiles"]) or len(set(result["profiles"])) != len(result["profiles"]):
            raise ValueError("Effective Compose profiles are unsupported or duplicated")
    for name in result["ports"]:
        result["ports"][name] = int(environment.get(f"HOST_{name.upper()}_PORT", result["ports"][name]))
    if any(not 1024 <= value <= 65535 for value in result["ports"].values()) or len(set(result["ports"].values())) != len(result["ports"]):
        raise ValueError("Effective environment ports are invalid or duplicated")
    for variable, resource in (("TRINO_MEMORY_LIMIT", "trino_memory_mb"), ("AIRFLOW_SCHEDULER_MEMORY", "airflow_scheduler_memory_mb")):
        memory = environment.get(variable)
        if memory:
            match = re.fullmatch(r"([0-9]+)([mMgG])", memory)
            if not match:
                raise ValueError(f"Use an integer m/g {variable}, or declare memory in pattern.yaml")
            result["resources"][resource] = int(match[1]) * (1024 if match[2].lower() == "g" else 1)
    for name, variable in (("trino_cpus", "TRINO_CPUS"), ("airflow_parallelism", "AIRFLOW_PARALLELISM")):
        result["resources"][name] = int(environment.get(variable, result["resources"][name]))
    if result["resources"]["trino_memory_mb"] < result["resources"]["trino_heap_mb"] + 1024:
        raise ValueError("Effective TRINO_MEMORY_LIMIT leaves insufficient JVM/container headroom")
    if not 1 <= result["resources"]["trino_cpus"] <= 16 or not 1 <= result["resources"]["airflow_parallelism"] <= 8:
        raise ValueError("Effective CPU/parallelism exceeds local-lab limits")
    if result["resources"]["airflow_scheduler_memory_mb"] < 1024 + 512 * result["resources"]["airflow_parallelism"]:
        raise ValueError("Effective scheduler memory is below the pattern's worker budget")
    return result


def resolve(document: dict) -> dict:
    allowed = {"version", "pattern", "preset", "profiles", "extensions", "ports", "resources"}
    if not isinstance(document, dict) or set(document) - allowed:
        raise ValueError("Unknown platform configuration fields")
    if type(document.get("version")) is not int or document.get("version") != 1 or document.get("pattern") != "local-lakehouse":
        raise ValueError("Expected version 1 of local-lakehouse")
    preset = document.get("preset", "small")
    if preset not in PRESETS:
        raise ValueError("Unknown resource preset")
    profiles = document.get("profiles", [])
    if not isinstance(profiles, list) or any(profile not in PROFILES for profile in profiles) or len(profiles) != len(set(profiles)):
        raise ValueError("Unsupported or duplicate Compose profiles")
    extensions = document.get("extensions", [])
    if not isinstance(extensions, list) or any(name not in EXTENSIONS for name in extensions) or len(extensions) != len(set(extensions)):
        raise ValueError("Unsupported or duplicate extensions")
    ports = {**PORTS}
    resources = {**PRESETS[preset]}
    for name, defaults in (("ports", ports), ("resources", resources)):
        overrides = document.get(name, {})
        if not isinstance(overrides, dict) or set(overrides) - set(defaults):
            raise ValueError(f"Unknown {name} override")
        defaults.update(overrides)
    if any(type(value) is not int or not 1024 <= value <= 65535 for value in ports.values()) or len(set(ports.values())) != len(ports):
        raise ValueError("Ports must be unique integers between 1024 and 65535")
    if any(type(value) is not int or value <= 0 for value in resources.values()):
        raise ValueError("Resources must be positive integers")
    if resources["trino_heap_mb"] < 1536 or resources["trino_memory_mb"] < resources["trino_heap_mb"] + 1024:
        raise ValueError("Trino needs at least 1536 MB heap and 1024 MB container headroom")
    if resources["airflow_parallelism"] > 8 or resources["trino_cpus"] > 16:
        raise ValueError("Resource request exceeds the local-lab pattern boundary")
    if resources["airflow_scheduler_memory_mb"] < 1024 + 512 * resources["airflow_parallelism"]:
        raise ValueError("Scheduler memory is below the pattern's worker budget")
    return {"version": 1, "pattern": "local-lakehouse", "preset": preset, "profiles": profiles, "extensions": extensions, "ports": ports, "resources": resources}


def render(config: Path) -> dict:
    resolved = resolve(yaml.safe_load(config.read_text()))
    directory = config.parent / ".pattern"
    directory.mkdir(exist_ok=True)
    resources = resolved["resources"]
    values = {f"HOST_{name.upper()}_PORT": str(value) for name, value in resolved["ports"].items()}
    values.update({"TRINO_MEMORY_LIMIT": f"{resources['trino_memory_mb']}m", "TRINO_CPUS": str(resources["trino_cpus"]), "AIRFLOW_SCHEDULER_MEMORY": f"{resources['airflow_scheduler_memory_mb']}m", "AIRFLOW_PARALLELISM": str(resources["airflow_parallelism"]), "COMPOSE_PROFILES": ",".join(resolved["profiles"])})
    jvm = config.parent / "trino/jvm.config"
    lines = jvm.read_text().splitlines()
    lines = [f"-Xmx{resources['trino_heap_mb']}M" if line.startswith("-Xmx") else line for line in lines]
    fingerprint = hashlib.sha256(("\n".join(lines) + json.dumps(resources, sort_keys=True)).encode()).hexdigest()
    files = {
        "defaults.env": "".join(f"{name}={value}\n" for name, value in sorted(values.items())),
        "trino-jvm.config": "\n".join(lines) + "\n",
        "compose.yaml": yaml.safe_dump({"services": {"trino": {"environment": {"LOCAL_STACK_PATTERN_DIGEST": fingerprint}, "volumes": ["./.pattern/trino-jvm.config:/etc/trino/jvm.config:ro"]}}}, sort_keys=True),
        "resolved.json": json.dumps(resolved, indent=2) + "\n",
    }
    for name, content in files.items():
        temporary = directory / f"{name}.tmp"
        temporary.write_text(content)
        temporary.replace(directory / name)
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    parser.add_argument("--effective", action="store_true")
    args = parser.parse_args()
    resolved = resolve(yaml.safe_load(args.config.read_text())) if args.effective else render(args.config)
    print(json.dumps(effective(resolved, os.environ) if args.effective else resolved, indent=2))


if __name__ == "__main__":
    main()