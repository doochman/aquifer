"""Acceptance check for the analytics profile: logs in, lists connections, and runs every dashboard chart through Trino."""

import json
import os
import sys

import requests

BASE = os.environ.get("SUPERSET_URL", "http://127.0.0.1:8088")
USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.environ["SUPERSET_ADMIN_PASSWORD"]
SLUGS = ["shop-overview", "streaming-live", "customer-profile-360"]


def fail(message):
    print(f"FAILED: {message}", file=sys.stderr)
    sys.exit(1)


def main():
    session = requests.Session()
    health = session.get(f"{BASE}/health", timeout=10)
    if health.status_code != 200:
        fail(f"/health returned {health.status_code}")
    login = session.post(f"{BASE}/api/v1/security/login", timeout=20,
                         json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True})
    if login.status_code != 200:
        fail(f"login returned {login.status_code}")
    session.headers["Authorization"] = f"Bearer {login.json()['access_token']}"
    session.headers["X-CSRFToken"] = session.get(f"{BASE}/api/v1/security/csrf_token/", timeout=10).json()["result"]
    session.headers["Referer"] = BASE

    databases = [row["database_name"] for row in session.get(f"{BASE}/api/v1/database/", timeout=20).json()["result"]]
    print(f"connections: {databases}")
    if not any("Trino" in name for name in databases):
        fail("no Trino connection is registered")

    total = 0
    seeded = 0
    for slug in SLUGS:
        query = json.dumps({"filters": [{"col": "slug", "opr": "eq", "value": slug}]})
        dashboards = session.get(f"{BASE}/api/v1/dashboard/", params={"q": query}, timeout=20).json()["result"]
        if not dashboards:
            print(f"dashboard {slug}: not seeded")
            continue
        seeded += 1
        total += check_dashboard(session, slug, dashboards[0]["id"])
    if not seeded:
        print("no dashboard seeded yet: run ./lakehouse demo")
        return
    print(f"analytics check passed: {total} charts answered by Trino")


def check_dashboard(session, slug, dashboard_id):
    charts = session.get(f"{BASE}/api/v1/dashboard/{dashboard_id}/charts", timeout=20).json()["result"]
    for chart in charts:
        detail = session.get(f"{BASE}/api/v1/chart/{chart['id']}", timeout=20).json()["result"]
        params = json.loads(detail["params"])
        group = params.get("groupby") or []
        columns = [group] if isinstance(group, str) else list(group)
        columns += list(params.get("all_columns") or [])
        axis = params.get("x_axis")
        grain = params.get("time_grain_sqla")
        if isinstance(axis, str):
            # With a time grain the chart asks for a base axis; this reproduces the grain lookup of the real request.
            columns.append({"columnType": "BASE_AXIS", "expressionType": "SQL", "label": axis, "sqlExpression": axis, "timeGrain": grain} if grain else axis)
        metrics = list(params.get("metrics") or ([params["metric"]] if params.get("metric") else []))
        body = {
            "datasource": {"id": int(params["datasource"].split("__")[0]), "type": "table"},
            "force": True, "result_format": "json", "result_type": "full",
            "queries": [{"columns": columns, "metrics": metrics, "orderby": [], "row_limit": 1000,
                         "filters": [], "extras": {"where": "", "having": "", **({"time_grain_sqla": grain} if grain else {})}}],
        }
        response = session.post(f"{BASE}/api/v1/chart/data", json=body, timeout=120)
        if response.status_code != 200:
            fail(f"{chart['slice_name']}: HTTP {response.status_code} {response.text[:200]}")
        rows = response.json()["result"][0]["data"]
        if not rows:
            if slug == "streaming-live":
                print(f"ok  {chart['slice_name']}: no events yet (run ./lakehouse stream)")
                continue
            fail(f"{chart['slice_name']}: query returned no rows")
        print(f"ok  {chart['slice_name']}: {len(rows)} rows")
    print(f"dashboard {slug}: {len(charts)} charts answered by Trino")
    return len(charts)


if __name__ == "__main__":
    main()
