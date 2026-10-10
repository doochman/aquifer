import tempfile
import unittest
from pathlib import Path

import render


class PatternTests(unittest.TestCase):
    def config(self) -> dict:
        return {"version": 1, "pattern": "local-lakehouse", "preset": "small"}

    def test_small_and_medium_budgets_are_coherent(self) -> None:
        for preset in ("small", "medium"):
            configuration = self.config()
            configuration["preset"] = preset
            resolved = render.resolve(configuration)
            self.assertGreaterEqual(resolved["resources"]["trino_memory_mb"] - resolved["resources"]["trino_heap_mb"], 1024)

    def test_rejects_unknown_fields_and_conflicting_ports(self) -> None:
        for invalid in ({"version": True}, {"secret": "not-allowed"}, {"ports": {"grafana": 8093}}, {"resources": {"trino_memory_mb": 1024}}, {"resources": {"airflow_parallelism": True}}):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    render.resolve({**self.config(), **invalid})

    def test_render_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "trino").mkdir()
            (root / "trino/jvm.config").write_text("-server\n-Xmx2G\n")
            (root / "pattern.yaml").write_text("version: 1\npattern: local-lakehouse\npreset: medium\n")
            render.render(root / "pattern.yaml")
            before = {path.name: path.read_bytes() for path in (root / ".pattern").iterdir()}
            render.render(root / "pattern.yaml")
            self.assertEqual(before, {path.name: path.read_bytes() for path in (root / ".pattern").iterdir()})
            self.assertIn("-Xmx4096M", (root / ".pattern/trino-jvm.config").read_text())
            (root / "pattern.yaml").write_text("version: 1\npattern: local-lakehouse\npreset: small\n")
            render.render(root / "pattern.yaml")
            self.assertNotEqual(before["compose.yaml"], (root / ".pattern/compose.yaml").read_bytes())

    def test_effective_overrides_preserve_ports_and_guard_memory(self) -> None:
        resolved = render.resolve(self.config())
        effective = render.effective(resolved, {"HOST_AIRFLOW_PORT": "8195", "TRINO_MEMORY_LIMIT": "4g", "COMPOSE_PROFILES": "observability,streaming"})
        self.assertEqual(effective["ports"]["airflow"], 8195)
        self.assertEqual(effective["resources"]["trino_memory_mb"], 4096)
        self.assertEqual(effective["profiles"], ["observability", "streaming"])
        self.assertEqual(render.effective(resolved, {"COMPOSE_PROFILES": "analytics"})["profiles"], ["analytics"])
        self.assertEqual(render.effective(resolved, {"HOST_SUPERSET_PORT": "8380"})["ports"]["superset"], 8380)
        for environment in ({"TRINO_MEMORY_LIMIT": "2g"}, {"HOST_AIRFLOW_PORT": "8093"}, {"HOST_SUPERSET_PORT": "8291"}, {"AIRFLOW_PARALLELISM": "99"}, {"AIRFLOW_SCHEDULER_MEMORY": "1g"}, {"COMPOSE_PROFILES": "unknown"}):
            with self.subTest(environment=environment):
                with self.assertRaises(ValueError):
                    render.effective(resolved, environment)
    def test_extensions_are_validated_and_overridable(self) -> None:
        resolved = render.resolve({**self.config(), "extensions": ["reference"]})
        self.assertEqual(resolved["extensions"], ["reference"])
        self.assertEqual(render.resolve(self.config())["extensions"], [])
        self.assertEqual(render.effective(resolved, {"LAKEHOUSE_EXTENSIONS": ""})["extensions"], [])
        self.assertEqual(render.effective(render.resolve(self.config()), {"LAKEHOUSE_EXTENSIONS": "reference"})["extensions"], ["reference"])
        for invalid in (["unknown"], ["reference", "reference"], "reference"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    render.resolve({**self.config(), "extensions": invalid})
        with self.assertRaises(ValueError):
            render.effective(resolved, {"LAKEHOUSE_EXTENSIONS": "unknown"})


if __name__ == "__main__":
    unittest.main()