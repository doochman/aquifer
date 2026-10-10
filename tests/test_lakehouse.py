"""Unit tests for the launcher. They need no Docker and run on macOS, Linux and Windows."""
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "patterns"))

import lakehouse as L  # noqa: E402

try:
    import yaml  # noqa: F401
    HAVE_YAML = True
except ImportError:
    HAVE_YAML = False


class ParsingTests(unittest.TestCase):
    def test_parse_env_handles_comments_quotes_and_export(self):
        text = "# comment\nA=1\nexport B = two \nC=\"quoted value\"\nD='single'\nE=plain # trailing\n\nBROKEN\n"
        self.assertEqual(L.parse_env(text), {"A": "1", "B": "two", "C": "quoted value", "D": "single", "E": "plain"})

    def test_parse_env_keeps_equals_in_values(self):
        self.assertEqual(L.parse_env("URL=http://x/?a=b\n"), {"URL": "http://x/?a=b"})

    def test_secrets_are_shell_safe_and_unique(self):
        first, second = L.generate_secret(), L.generate_secret()
        self.assertEqual(len(first), 24)
        self.assertTrue(first.isalnum())
        self.assertNotEqual(first, second)

    def test_parse_version(self):
        self.assertEqual(L.parse_version("v2.31.0-desktop.2"), (2, 31, 0))
        self.assertEqual(L.parse_version("2.20"), (2, 20))
        self.assertEqual(L.parse_version("nothing"), (0,))
        self.assertLess(L.parse_version("2.19.1"), L.MIN_COMPOSE)

    def test_normalise_argv_accepts_legacy_flags(self):
        self.assertEqual(L.normalise_argv(["--doctor"]), ["doctor"])
        self.assertEqual(L.normalise_argv(["--reset", "--yes"]), ["reset", "--yes"])
        self.assertEqual(L.normalise_argv(["--logs", "trino"]), ["logs", "trino"])
        self.assertEqual(L.normalise_argv(["up", "--demo"]), ["up", "--demo"])
        self.assertEqual(L.normalise_argv(["--help"]), ["--help"])

    def test_human_duration(self):
        self.assertEqual(L.human_duration(9), "9s")
        self.assertEqual(L.human_duration(125), "2m 05s")

    def test_every_pattern_port_has_a_label_and_every_profile_is_known(self):
        import render
        self.assertEqual(set(render.PORTS), set(L.PORT_LABELS))
        self.assertEqual(set(L.OPTIONAL_PROFILES), set(render.PROFILES))

    def test_make_events_are_valid_json_with_expected_fields(self):
        import random
        from datetime import datetime
        lines = L.make_events(50, random.Random(1), spread_hours=2, now=datetime(2026, 1, 1, 12, 0, 0))
        self.assertEqual(len(lines), 50)
        first = json.loads(lines[0])
        self.assertEqual(set(first), {"event_id", "order_id", "customer_id", "product_id", "quantity", "amount", "status", "event_time"})
        self.assertLessEqual(first["event_time"], "2026-01-01T12:00:00.000")
        self.assertGreaterEqual(first["event_time"], "2026-01-01T10:00:00.000")
        self.assertEqual(lines, L.make_events(50, random.Random(1), spread_hours=2, now=datetime(2026, 1, 1, 12, 0, 0)))

    def test_every_legacy_flag_maps_to_a_command(self):
        parser = L.build_parser()
        commands = set(parser._subparsers._group_actions[0].choices)
        for flag in L.LEGACY_FLAGS - {"--help"}:
            self.assertIn(flag[2:], commands, flag)


class SqlTests(unittest.TestCase):
    def test_split_sql_ignores_comments_and_quoted_semicolons(self):
        script = "-- header; not a statement\nSELECT 'a;b' AS x;\n/* block; comment */ SELECT 2;\n\nSELECT \"q;q\" ; SELECT 'it''s;'"
        self.assertEqual(L.split_sql(script), ["SELECT 'a;b' AS x", "SELECT 2", 'SELECT "q;q"', "SELECT 'it''s;'"])

    def test_demo_scripts_split_into_expected_statements(self):
        counts = {}
        for name in L.DEMO_SQL:
            statements = L.split_sql((ROOT / "demo" / "sql" / name).read_text(encoding="utf-8"))
            counts[name] = len(statements)
            for statement in statements:
                self.assertRegex(statement, r"^CREATE ", name)
        self.assertEqual(counts, {"01_seed.sql": 4})

    def test_describe_statement(self):
        self.assertEqual(L.describe_statement("CREATE OR REPLACE TABLE iceberg.shop.orders WITH (x) AS SELECT 1"), "table iceberg.shop.orders")
        self.assertEqual(L.describe_statement("CREATE SCHEMA IF NOT EXISTS iceberg.shop"), "schema iceberg.shop")

    def test_format_table(self):
        out = L.format_table(["a", "bb"], [[1, None], [22, "x"]])
        self.assertEqual(out.splitlines(), ["a  | bb", "---+-----", "1  | NULL", "22 | x"])


class LoadTests(unittest.TestCase):
    def test_sanitize_identifier(self):
        self.assertEqual(L.sanitize_identifier("Order Date (UTC)"), "order_date_utc")
        self.assertEqual(L.sanitize_identifier("2nd"), "c_2nd")
        self.assertEqual(L.sanitize_identifier("select"), "c_select")
        self.assertEqual(L.sanitize_identifier("!!!"), "column")
        self.assertEqual(L.sanitize_identifier("a", ["a", "a_2"]), "a_3")

    def test_infer_column_type(self):
        infer = L.infer_column_type
        self.assertEqual(infer(["1", "-2", "30"]), "BIGINT")
        self.assertEqual(infer(["1", "2.5", ""]), "DOUBLE")
        self.assertEqual(infer(["1e3", "0.5"]), "DOUBLE")
        self.assertEqual(infer(["true", "FALSE", None]), "BOOLEAN")
        self.assertEqual(infer(["2026-01-31", "2026-02-01"]), "DATE")
        self.assertEqual(infer(["2026-02-30"]), "VARCHAR")
        self.assertEqual(infer(["2026-01-31 10:00:00", "2026-01-31T10:00:00.123"]), "TIMESTAMP(6)")
        self.assertEqual(infer(["01234", "99999"]), "VARCHAR")
        self.assertEqual(infer(["nan", "1"]), "VARCHAR")
        self.assertEqual(infer(["99999999999999999999"]), "DOUBLE")
        self.assertEqual(infer(["a", "1"]), "VARCHAR")
        self.assertEqual(infer([None, ""]), "VARCHAR")

    def test_sql_literal_escapes_and_formats(self):
        self.assertEqual(L.sql_literal("it's", "VARCHAR"), "'it''s'")
        self.assertEqual(L.sql_literal("", "BIGINT"), "NULL")
        self.assertEqual(L.sql_literal("007", "VARCHAR"), "'007'")
        self.assertEqual(L.sql_literal("2.50", "DOUBLE"), "2.5")
        self.assertEqual(L.sql_literal("TRUE", "BOOLEAN"), "true")
        self.assertEqual(L.sql_literal("2026-01-31", "DATE"), "DATE '2026-01-31'")
        self.assertEqual(L.sql_literal("2026-01-31T10:00:00.5", "TIMESTAMP(6)"), "TIMESTAMP '2026-01-31 10:00:00.500000'")
        self.assertEqual(L.sql_literal("2026-01-31 10:00:00", "TIMESTAMP(6)"), "TIMESTAMP '2026-01-31 10:00:00.000000'")

    def test_read_rows_csv_tsv_and_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.csv").write_text("\ufeffid,name,note\n1,Ann,\"hello, world\"\n2,Bob\n\n", encoding="utf-8")
            header, rows = L.read_rows(root / "a.csv")
            self.assertEqual(header, ["id", "name", "note"])
            self.assertEqual(rows, [["1", "Ann", "hello, world"], ["2", "Bob", None]])
            (root / "b.tsv").write_text("x\ty\n1\t2\n", encoding="utf-8")
            self.assertEqual(L.read_rows(root / "b.tsv")[1], [["1", "2"]])
            (root / "c.jsonl").write_text('{"id": 1, "ok": true}\n{"id": 2, "extra": {"k": 1}}\n', encoding="utf-8")
            header, rows = L.read_rows(root / "c.jsonl")
            self.assertEqual(header, ["id", "ok", "extra"])
            self.assertEqual(rows, [["1", "true", None], ["2", None, '{"k": 1}']])
            (root / "empty.csv").write_text("", encoding="utf-8")
            with self.assertRaises(L.LakehouseError):
                L.read_rows(root / "empty.csv")

    def test_ddl_and_batching(self):
        columns = [("id", "BIGINT"), ("name", "VARCHAR")]
        self.assertIn("id BIGINT,\n  name VARCHAR", L.table_ddl("iceberg.data.t", columns))
        rows = [[str(i), "x" * 10] for i in range(25)]
        statements = list(L.insert_batches("iceberg.data.t", columns, rows, max_rows=10))
        self.assertEqual(len(statements), 3)
        self.assertTrue(all(statement.startswith("INSERT INTO iceberg.data.t VALUES") for statement in statements))
        self.assertEqual(sum(statement.count("(") for statement in statements), 25)
        by_size = list(L.insert_batches("iceberg.data.t", columns, rows, max_chars=120))
        self.assertGreater(len(by_size), 3)
        self.assertEqual(list(L.insert_batches("iceberg.data.t", columns, [])), [])


class BackupTests(unittest.TestCase):
    def test_bundle_and_read_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "postgres_data.tar.gz").write_bytes(b"pg")
            (root / "minio_data.tar.gz").write_bytes(b"minio")
            (root / ".env").write_text("A=1\n")
            target = root / "out" / "b.tar"
            manifest = L.build_manifest("demo", ["postgres_data", "minio_data"])
            L.bundle_backup(target, manifest, {"postgres_data": root / "postgres_data.tar.gz", "minio_data": root / "minio_data.tar.gz"}, root / ".env")
            work = root / "work"
            work.mkdir()
            read_manifest, archives, env_path = L.read_backup(target, work)
            self.assertEqual(read_manifest["project"], "demo")
            self.assertEqual(sorted(archives), ["minio_data", "postgres_data"])
            self.assertEqual(archives["minio_data"].read_bytes(), b"minio")
            self.assertEqual(env_path.read_text(), "A=1\n")

    def test_read_backup_rejects_foreign_archives_and_unknown_members(self):
        import io
        import tarfile
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            foreign = root / "foreign.tar"
            with tarfile.open(foreign, "w") as bundle:
                data = b"x"
                info = tarfile.TarInfo("other.txt")
                info.size = len(data)
                bundle.addfile(info, io.BytesIO(data))
            with self.assertRaises(L.LakehouseError):
                L.read_backup(foreign, root)
            crafted = root / "crafted.tar"
            manifest = json.dumps(L.build_manifest("demo", [])).encode()
            with tarfile.open(crafted, "w") as bundle:
                for name, data in (("manifest.json", manifest), ("volumes/../../evil.tar.gz", b"e"), ("volumes/unknown_data.tar.gz", b"u")):
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    bundle.addfile(info, io.BytesIO(data))
            work = root / "w"
            work.mkdir()
            _, archives, env_path = L.read_backup(crafted, work)
            self.assertEqual(archives, {})
            self.assertIsNone(env_path)
            self.assertEqual(list(work.iterdir()), [])

    def test_backup_volumes_are_real_compose_volumes(self):
        text = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        for name in L.BACKUP_VOLUMES:
            self.assertIn(f"\n  {name}:", text, name)


class PortTests(unittest.TestCase):
    def test_pick_free_ports_moves_busy_and_keeps_unique(self):
        busy = {8093, 8094}
        chosen = L.pick_free_ports({"trino": 8093, "airflow": 8094, "console": 8095}, busy=lambda p: p in busy)
        self.assertEqual(chosen, {"trino": 8095, "airflow": 8096, "console": 8097})

    def test_pick_free_ports_keeps_free_defaults(self):
        self.assertEqual(L.pick_free_ports({"a": 9000, "b": 9001}, busy=lambda p: False), {"a": 9000, "b": 9001})


class _FakeTrino(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _reply(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        sql = self.rfile.read(int(self.headers["Content-Length"])).decode()
        self.server.seen.append((sql, self.headers.get("X-Trino-User")))
        base = f"http://127.0.0.1:{self.server.server_port}"
        if "boom" in sql:
            self._reply({"error": {"errorName": "SYNTAX_ERROR", "message": "nope"}})
        else:
            self._reply({"nextUri": base + "/page/1"})

    def do_GET(self):
        base = f"http://127.0.0.1:{self.server.server_port}"
        if self.path.endswith("/1"):
            self._reply({"columns": [{"name": "n"}], "nextUri": base + "/page/2"})
        elif self.path.endswith("/2"):
            self._reply({"data": [[1], [2]], "nextUri": base + "/page/3"})
        else:
            self._reply({"data": [[3]]})


class TrinoClientTests(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), _FakeTrino)
        self.server.seen = []
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)

    def test_follows_next_uri_and_collects_rows(self):
        client = L.Trino(self.server.server_port, user="tester")
        columns, rows = client.execute("SELECT n")
        self.assertEqual(columns, ["n"])
        self.assertEqual(rows, [[1], [2], [3]])
        self.assertEqual(self.server.seen, [("SELECT n", "tester")])

    def test_surfaces_query_errors(self):
        with self.assertRaisesRegex(L.TrinoError, "SYNTAX_ERROR: nope"):
            L.Trino(self.server.server_port).execute("boom")

    def test_unreachable_server_gives_actionable_message(self):
        with self.assertRaisesRegex(L.TrinoError, "Is the stack running"):
            L.Trino(1).execute("SELECT 1")

    def test_url_ready_rejects_starting_trino(self):
        class Starting(_FakeTrino):
            def do_GET(self):
                self._reply({"starting": True})
        server = HTTPServer(("127.0.0.1", 0), Starting)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.assertFalse(L.url_ready(f"http://127.0.0.1:{server.server_port}/v1/info"))


@unittest.skipUnless(HAVE_YAML, "PyYAML not installed")
class StackTests(unittest.TestCase):
    def sandbox(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "trino").mkdir()
        (root / "trino" / "jvm.config").write_text("-server\n-Xmx2G\n")
        (root / "pattern.yaml").write_text("version: 1\npattern: local-lakehouse\npreset: small\nprofiles: [analytics]\nextensions: [reference]\n")
        patches = [
            mock.patch.object(L, "ROOT", root), mock.patch.object(L, "PATTERN_FILE", root / "pattern.yaml"),
            mock.patch.object(L, "GENERATED", root / ".pattern"), mock.patch.object(L, "ENV_FILE", root / ".env"),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        return root

    def test_init_creates_env_once_with_generated_secrets(self):
        root = self.sandbox()
        with mock.patch.object(L, "port_in_use", lambda port, host="127.0.0.1": port == 8093):
            L.init_env(quiet=True)
        values = L.parse_env((root / ".env").read_text())
        self.assertEqual(values["COMPOSE_PROJECT_NAME"], "aquifer")
        self.assertTrue(values["AIRFLOW_ADMIN_PASSWORD"].isalnum())
        self.assertNotEqual(values["AIRFLOW_ADMIN_PASSWORD"], values["MINIO_ROOT_PASSWORD"])
        self.assertNotEqual(values["HOST_TRINO_PORT"], "8093")
        first = (root / ".env").read_text()
        L.init_env(quiet=True)
        self.assertEqual((root / ".env").read_text(), first)

    def test_stack_assembles_extension_files_and_profiles(self):
        root = self.sandbox()
        (root / ".env").write_text("COMPOSE_PROJECT_NAME=demo\nHOST_AIRFLOW_PORT=8999\n")
        with mock.patch.dict("os.environ", {}, clear=False):
            for name in ("COMPOSE_PROFILES", "LAKEHOUSE_EXTENSIONS", "HOST_AIRFLOW_PORT", "COMPOSE_PROJECT_NAME"):
                L.os.environ.pop(name, None)
            stack = L.Stack.load(auto_init=False)
        self.assertEqual(stack.project, "demo")
        self.assertEqual(stack.port("airflow"), 8999)
        self.assertEqual([path.name for path in stack.compose_files()], ["compose.yaml", "compose.reference.yaml", "compose.yaml"])
        command = stack.compose_prefix(["analytics"])
        self.assertEqual(command[:4], ["docker", "compose", "--project-name", "demo"])
        self.assertIn("analytics", command)
        self.assertEqual(command.count("--env-file"), 2)

    def test_environment_overrides_pattern_file(self):
        self.sandbox()
        with mock.patch.dict("os.environ", {"LAKEHOUSE_EXTENSIONS": "", "COMPOSE_PROFILES": "observability"}):
            stack = L.Stack.load(auto_init=False)
        self.assertEqual(stack.extensions, [])
        self.assertEqual(stack.profiles, ["observability"])

    def test_connection_guide_uses_effective_ports_and_hides_secrets(self):
        self.sandbox()
        L.ENV_FILE.write_text("MINIO_ROOT_PASSWORD=s3cretvalue\nHOST_TRINO_PORT=8777\n")
        with mock.patch.dict("os.environ", {}, clear=False):
            for name in ("HOST_TRINO_PORT", "MINIO_ROOT_PASSWORD", "COMPOSE_PROFILES", "LAKEHOUSE_EXTENSIONS"):
                L.os.environ.pop(name, None)
            stack = L.Stack.load(auto_init=False)
        self.assertIn("jdbc:trino://localhost:8777/iceberg", L.connection_guide(stack))
        self.assertNotIn("s3cretvalue", L.connection_guide(stack))
        self.assertIn("s3cretvalue", L.connection_guide(stack, show_secrets=True))

    def test_prometheus_targets_follow_profiles_and_extensions(self):
        root = self.sandbox()
        with mock.patch.dict("os.environ", {"COMPOSE_PROFILES": "analytics,streaming", "LAKEHOUSE_EXTENSIONS": ""}):
            L.Stack.load(auto_init=False)
        readiness = json.loads((root / ".pattern/targets/readiness.json").read_text())[0]["targets"]
        self.assertIn("http://flink-jobmanager:8081/overview", readiness)
        self.assertIn("http://superset:8088/health", readiness)
        self.assertNotIn("http://profile-showcase:8088/api/summary", readiness)
        self.assertEqual(json.loads((root / ".pattern/targets/flink.json").read_text())[0]["targets"], ["flink-jobmanager:9249", "flink-taskmanager:9249"])
        self.assertEqual(json.loads((root / ".pattern/targets/customer-profile.json").read_text())[0]["targets"], [])
        with mock.patch.dict("os.environ", {"COMPOSE_PROFILES": "", "LAKEHOUSE_EXTENSIONS": "reference"}):
            L.Stack.load(auto_init=False)
        self.assertEqual(json.loads((root / ".pattern/targets/flink.json").read_text())[0]["targets"], [])
        self.assertIn("http://profile-showcase:8088/api/summary", json.loads((root / ".pattern/targets/readiness.json").read_text())[0]["targets"])

    def test_home_page_lists_active_services(self):
        root = self.sandbox()
        with mock.patch.dict("os.environ", {"LAKEHOUSE_EXTENSIONS": ""}):
            stack = L.Stack.load(auto_init=False)
            page = L.write_home(stack).read_text(encoding="utf-8")
        self.assertIn("Superset", page)
        self.assertNotIn("Customer Profile 360", page)
        self.assertNotIn("{{", page)


class ComposeModelTests(unittest.TestCase):
    """Static guards that keep the core stack standalone."""

    def test_core_compose_does_not_reach_outside_the_directory(self):
        text = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        self.assertNotIn("../", text)
        self.assertNotIn("external: true", text)
        self.assertNotIn("customer-profile", text)

    def test_reference_extension_is_the_only_place_for_sibling_repositories(self):
        text = (ROOT / "compose.reference.yaml").read_text(encoding="utf-8")
        self.assertIn("../customer-profile-360-reference", text)
        self.assertIn("external: true", text)


if __name__ == "__main__":
    unittest.main()
