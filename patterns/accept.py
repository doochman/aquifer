from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("product", type=Path)
    parser.add_argument("--verifier", type=Path, required=True)
    parser.add_argument("--generate-dbt-tests", action="store_true", help="Generate the example's dbt test schema from FLUID DQ rules in the temporary workspace")
    args = parser.parse_args()
    product = args.product.resolve(strict=True)
    if not (product / "contract.fluid.yaml").is_file():
        raise ValueError("Product directory must contain a FLUID contract")
    environment = {**os.environ, "DBT_EXECUTABLE": os.getenv("DBT_EXECUTABLE", "/home/airflow/.dbt-venv/bin/dbt"), "LOCAL_PRODUCT_USER": "local-pattern-acceptance"}
    with tempfile.TemporaryDirectory(prefix="local-pattern-acceptance-") as temporary:
        workspace = Path(temporary) / "product"
        shutil.copytree(product, workspace, ignore=shutil.ignore_patterns("target", "logs", ".fluid", "__pycache__"))
        runtime = workspace / "runtime"
        runtime.mkdir()
        commands = [
            ["fluid", "validate", "contract.fluid.yaml", "--strict"],
            ["fluid", "bundle", "contract.fluid.yaml", "--out", "runtime/bundle.tgz"],
            ["fluid", "validate", "runtime/bundle.tgz", "--strict"],
            ["fluid", "generate", "artifacts", "runtime/bundle.tgz", "--contract-path", "contract.fluid.yaml", "--out", "runtime/artifacts", "--emit", "odcs,odps-bitol"],
            ["fluid", "validate-artifacts", "runtime/artifacts", "--manifest", "runtime/artifacts/MANIFEST.json", "--report", "runtime/artifact-validation.json"],
            ["fluid", "plan", "runtime/bundle.tgz", "--mode", "amend-and-build", "--out", "runtime/plan.json"],
            ["fluid", "apply", "runtime/plan.json", "--bundle", "runtime/bundle.tgz", "--mode", "amend-and-build", "--yes", "--fail-fast"],
            ["fluid", "verify", "contract.fluid.yaml", "--strict", "--fail-on-warning", "--reconcile-dbt", "--out", "runtime/forge-verification.json"],
            [sys.executable, str(args.verifier.resolve(strict=True)), "diff"],
        ]
        if args.generate_dbt_tests:
            commands.insert(1, ["fluid", "generate", "dbt-tests", "contract.fluid.yaml", "--dbt-tests-key", "data_tests", "--out", "dbt/models/schema.yml"])
        for command in commands:
            subprocess.run(command, cwd=workspace, env=environment, check=True)
        evidence = json.loads((runtime / "local-diff-report.json").read_text())
        if evidence["status"] != "matched":
            raise RuntimeError("Native build did not produce its declared target")
        print(json.dumps({"pattern_acceptance": "passed", "product": product.name, "native_checks": ["contract_and_bundle_validation", "artifact_generation_and_validation", "plan_bound_apply", "dbt_reconciliation_and_run_probes"], "supplemental_check": {"reason": "Forge 0.17.0 has no native Iceberg storage verifier", "result": evidence}, "native_verification": json.loads((runtime / "forge-verification.json").read_text())}, indent=2))


if __name__ == "__main__":
    main()