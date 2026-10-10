"""Idempotent seed for the local analytics workspace.

Always creates the general Trino connection. When the demo shop tables exist it adds the
"Acme Outdoor" dataset, charts, dashboard and saved queries. With the `reference` extension
(SUPERSET_GOVERNED_PRINCIPAL set) it also seeds the governed Customer Profile 360 dashboard.
Objects are matched by name or slug and updated in place, so reruns are safe.
"""

import json
import os
import sys
import time

from sqlalchemy import text

from superset.app import create_app

GENERAL_DB = "Lakehouse (Trino, general)"
GOVERNED_DB = "Customer analytics reader (Trino, governed)"
GOVERNED_PRINCIPAL = os.environ.get("SUPERSET_GOVERNED_PRINCIPAL", "")

SHOP_SLUG = "shop-overview"
SHOP_TITLE = "Acme Outdoor - sales overview"
SHOP_PROBE = "iceberg.shop.daily_revenue"

STREAM_SLUG = "streaming-live"
STREAM_TITLE = "AQUIFER - streaming pipeline (Kafka, Flink, Iceberg)"
STREAM_PROBE = "iceberg.shop.order_events"
STREAM_METRICS = {
    "events": ("Events", "COUNT(*)"),
    "revenue": ("Order value (EUR)", "SUM(amount)"),
    "avg_basket": ("Average order value (EUR)", "AVG(amount)"),
}

PROFILE = "iceberg.silver.customer_profile_360"
PROFILE_SLUG = "customer-profile-360"
PROFILE_TITLE = "Customer Profile 360 - executive view"
PROFILE_LATEST_SQL = f"SELECT * FROM {PROFILE} WHERE data_as_of_date = (SELECT max(data_as_of_date) FROM {PROFILE})"

SHOP_DATASETS = {
    "daily_revenue": {
        "main_dttm_col": "order_date",
        "metrics": {
            "net_revenue": ("Net revenue (EUR)", "SUM(net_revenue)"),
            "orders": ("Orders", "SUM(orders)"),
            "units": ("Units sold", "SUM(units)"),
            "aov": ("Net revenue per order (EUR)", "SUM(net_revenue) / NULLIF(SUM(orders), 0)"),
            "refund_rate": ("Refund rate", "SUM(refunded_revenue) / NULLIF(SUM(gross_revenue), 0)"),
        },
    },
    "customer_value": {
        "metrics": {
            "customers": ("Customers", "COUNT(*)"),
            "avg_ltv": ("Average lifetime value (EUR)", "AVG(net_revenue)"),
            "active_share": ("Active share", "AVG(CASE WHEN lifecycle_status = 'active' THEN 1.0 ELSE 0.0 END)"),
            "opt_in_rate": ("Marketing opt-in rate", "AVG(CASE WHEN marketing_opt_in THEN 1.0 ELSE 0.0 END)"),
        },
    },
    "monthly_cohorts": {
        "metrics": {"avg_retention": ("Average retention", "AVG(retention)")},
    },
}

SHOP_QUERIES = [
    ("Shop: net revenue by country and category",
     "Aggregation over the daily_revenue mart.",
     "SELECT country, category,\n       round(sum(net_revenue)) AS net_revenue_eur,\n       sum(orders) AS orders\nFROM iceberg.shop.daily_revenue\nGROUP BY country, category\nORDER BY net_revenue_eur DESC"),
    ("Shop: month-over-month revenue growth",
     "A window function over a monthly aggregation.",
     "WITH monthly AS (\n  SELECT date_trunc('month', order_date) AS month, sum(net_revenue) AS revenue\n  FROM iceberg.shop.daily_revenue\n  GROUP BY 1\n)\nSELECT month, round(revenue) AS revenue,\n       round(100.0 * (revenue / lag(revenue) OVER (ORDER BY month) - 1), 1) AS growth_pct\nFROM monthly\nORDER BY month"),
    ("Shop: top 20 customers by lifetime value",
     "Join the mart back to the raw customers table.",
     "SELECT v.customer_id, c.country, c.acquisition_channel, v.orders, round(v.net_revenue, 2) AS lifetime_value\nFROM iceberg.shop.customer_value v\nJOIN iceberg.shop.customers c USING (customer_id)\nORDER BY v.net_revenue DESC\nLIMIT 20"),
    ("Shop: at-risk customers who accept marketing",
     "An activation list: lifecycle status plus consent.",
     "SELECT country, value_tier, count(*) AS customers\nFROM iceberg.shop.customer_value\nWHERE lifecycle_status = 'at_risk' AND marketing_opt_in\nGROUP BY country, value_tier\nORDER BY customers DESC"),
]

PROFILE_METRICS = {
    "customers": ("Customers", "COUNT(DISTINCT customer_id)"),
    "avg_arpu_eur": ("Average monthly ARPU (EUR)", "AVG(monthly_arpu_eur)"),
    "avg_ltv_eur": ("Average lifetime value (EUR)", "AVG(lifetime_value_eur)"),
    "avg_churn_risk": ("Average churn risk", "AVG(churn_risk_score)"),
    "avg_tenure_months": ("Average tenure (months)", "AVG(tenure_months)"),
    "email_consent_rate": ("Marketing e-mail consent rate", "AVG(CASE WHEN consent_marketing_email THEN 1.0 ELSE 0.0 END)"),
    "suppressed_rate": ("Suppressed rate", "AVG(CASE WHEN is_suppressed THEN 1.0 ELSE 0.0 END)"),
}

PROFILE_QUERIES = [
    ("Profile 360: ARPU and lifetime value by value tier",
     "Aggregation over the latest snapshot.",
     f"SELECT value_tier,\n       count(*) AS customers,\n       round(avg(monthly_arpu_eur), 2) AS avg_arpu_eur,\n       round(avg(lifetime_value_eur), 2) AS avg_ltv_eur\nFROM {PROFILE}\nWHERE data_as_of_date = (SELECT max(data_as_of_date) FROM {PROFILE})\nGROUP BY value_tier\nORDER BY value_tier"),
    ("Profile 360: churn deciles by segment",
     "Where the churn-prone customers sit.",
     f"SELECT segment_code, churn_decile, count(*) AS customers\nFROM {PROFILE}\nWHERE data_as_of_date = (SELECT max(data_as_of_date) FROM {PROFILE})\nGROUP BY segment_code, churn_decile\nORDER BY segment_code, churn_decile"),
    ("Profile 360: who may be contacted by e-mail",
     "Consent and suppression are activation gates, not optional context.",
     f"SELECT consent_marketing_email AS has_consent,\n       is_suppressed,\n       count(*) AS customers\nFROM {PROFILE}\nWHERE data_as_of_date = (SELECT max(data_as_of_date) FROM {PROFILE})\nGROUP BY 1, 2\nORDER BY 1 DESC, 2"),
    ("Profile 360: snapshot history",
     "One row per snapshot day; never mix a snapshot day with an event timestamp.",
     f"SELECT data_as_of_date,\n       count(*) AS customers,\n       round(avg(churn_risk_score), 4) AS avg_churn_risk\nFROM {PROFILE}\nGROUP BY data_as_of_date\nORDER BY data_as_of_date"),
]


def log(message):
    print(f"[analytics] {message}", flush=True)


def wait_for_trino(database, attempts=60):
    for attempt in range(attempts):
        try:
            with database.get_sqla_engine() as engine:
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
            return True
        except Exception as error:  # noqa: BLE001 - any failure means "not ready yet"
            if attempt == attempts - 1:
                log(f"Trino did not answer: {error}")
            time.sleep(2)
    return False


def table_available(database, table):
    try:
        with database.get_sqla_engine() as engine:
            with engine.connect() as connection:
                connection.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
        return True
    except Exception as error:  # noqa: BLE001
        log(f"{table} is not queryable yet ({str(error).splitlines()[0][:110]})")
        return False


def upsert_database(db, Database, name, uri, extra):
    database = db.session.query(Database).filter_by(database_name=name).one_or_none()
    if database is None:
        database = Database(database_name=name)
        db.session.add(database)
    database.set_sqlalchemy_uri(uri)
    database.expose_in_sqllab = True
    database.allow_dml = False
    database.allow_ctas = False
    database.allow_cvas = False
    database.allow_file_upload = False
    database.impersonate_user = False
    database.extra = json.dumps(extra)
    db.session.commit()
    return database


def upsert_dataset(db, SqlaTable, SqlMetric, database, schema, name, metrics, sql=None, main_dttm_col=None):
    dataset = db.session.query(SqlaTable).filter_by(database_id=database.id, schema=schema, table_name=name).one_or_none()
    if dataset is None:
        dataset = SqlaTable(table_name=name, schema=schema, database=database)
        db.session.add(dataset)
    dataset.sql = sql
    db.session.commit()
    dataset.fetch_metadata()
    known = {metric.metric_name: metric for metric in dataset.metrics}
    for metric_name, (verbose, expression) in metrics.items():
        metric = known.get(metric_name)
        if metric is None:
            dataset.metrics.append(SqlMetric(metric_name=metric_name, verbose_name=verbose, expression=expression, table=dataset))
        else:
            metric.verbose_name, metric.expression = verbose, expression
    if main_dttm_col:
        dataset.main_dttm_col = main_dttm_col
        for column in dataset.columns:
            if column.column_name == main_dttm_col:
                column.is_dttm = True
    db.session.commit()
    return dataset


def upsert_chart(db, Slice, dataset, owner, name, viz_type, params):
    form_data = {"datasource": f"{dataset.id}__table", "viz_type": viz_type, "adhoc_filters": [], "time_range": "No filter", **params}
    chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
    if chart is None:
        chart = Slice(slice_name=name)
        db.session.add(chart)
    chart.viz_type = viz_type
    chart.datasource_type = "table"
    chart.datasource_id = dataset.id
    chart.datasource_name = dataset.datasource_name
    chart.params = json.dumps(form_data)
    chart.query_context = None
    chart.owners = [owner]
    db.session.commit()
    return chart


def position_json(title, rows):
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }
    for index, row in enumerate(rows, start=1):
        row_id = f"ROW-{index}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart, width, *rest in row:
            chart_id = f"CHART-{chart.id}"
            layout[row_id]["children"].append(chart_id)
            layout[chart_id] = {
                "type": "CHART", "id": chart_id, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": chart.id, "sliceName": chart.slice_name, "width": width, "height": rest[0] if rest else 50},
            }
    return json.dumps(layout)


def upsert_dashboard(db, Dashboard, admin, slug, title, rows):
    dashboard = db.session.query(Dashboard).filter_by(slug=slug).one_or_none()
    if dashboard is None:
        dashboard = Dashboard(slug=slug)
        db.session.add(dashboard)
    dashboard.dashboard_title = title
    dashboard.published = True
    dashboard.slices = [chart for row in rows for chart, *_ in row]
    dashboard.owners = [admin]
    dashboard.position_json = position_json(title, rows)
    dashboard.json_metadata = json.dumps({
        "native_filter_configuration": [], "color_scheme": "", "refresh_frequency": 0,
        "expanded_slices": {}, "timed_refresh_immune_slices": [], "label_colors": {},
    })
    db.session.commit()
    return dashboard


def upsert_queries(db, SavedQuery, admin, database, schema, queries):
    for label, description, sql in queries:
        saved = db.session.query(SavedQuery).filter_by(label=label).one_or_none()
        if saved is None:
            saved = SavedQuery(label=label)
            db.session.add(saved)
        saved.db_id, saved.schema, saved.description, saved.sql = database.id, schema, description, sql
        saved.user_id = admin.id
    db.session.commit()


def seed_shop(ctx, general):
    db, SqlaTable, SqlMetric, Slice, Dashboard, SavedQuery, admin = ctx
    datasets = {
        name: upsert_dataset(db, SqlaTable, SqlMetric, general, "shop", name, spec["metrics"], main_dttm_col=spec.get("main_dttm_col"))
        for name, spec in SHOP_DATASETS.items()
    }
    revenue, customers, cohorts = datasets["daily_revenue"], datasets["customer_value"], datasets["monthly_cohorts"]

    def chart(dataset, name, viz_type, **params):
        return upsert_chart(db, Slice, dataset, admin, name, viz_type, params)

    kpi_revenue = chart(revenue, "Net revenue (EUR)", "big_number_total", metric="net_revenue", y_axis_format="SMART_NUMBER")
    kpi_orders = chart(revenue, "Orders", "big_number_total", metric="orders", y_axis_format="SMART_NUMBER")
    kpi_aov = chart(revenue, "Net revenue per order (EUR)", "big_number_total", metric="aov", y_axis_format=",.2f")
    kpi_customers = chart(customers, "Customers", "big_number_total", metric="customers", y_axis_format="SMART_NUMBER")
    monthly = chart(revenue, "Net revenue by month (completed months)", "echarts_timeseries_line",
                    x_axis="order_date", time_grain_sqla="P1M", metrics=["net_revenue"], groupby=[], row_limit=1000,
                    adhoc_filters=[{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": "order_date < date_trunc('month', current_date)"}])
    categories = chart(revenue, "Net revenue by category", "pie",
                       groupby=["category"], metric="net_revenue", donut=True, show_labels=True,
                       label_type="key_percent", row_limit=100)
    countries = chart(revenue, "Net revenue by country", "echarts_timeseries_bar",
                      x_axis="country", metrics=["net_revenue"], groupby=[], orientation="vertical",
                      row_limit=100, x_axis_sort="net_revenue", x_axis_sort_asc=False)
    tiers = chart(customers, "Customers by value tier and lifecycle", "echarts_timeseries_bar",
                  x_axis="value_tier", metrics=["customers"], groupby=["lifecycle_status"], stack=True,
                  orientation="vertical", row_limit=100)
    channels = chart(customers, "Acquisition channels", "table",
                     query_mode="aggregate", groupby=["acquisition_channel"],
                     metrics=["customers", "avg_ltv", "active_share", "opt_in_rate"],
                     all_columns=[], order_desc=True, row_limit=100, include_search=False, show_cell_bars=False)
    retention = chart(cohorts, "Cohort retention", "heatmap_v2",
                      x_axis="month_label", groupby="cohort", metric="avg_retention",
                      normalize_across="heatmap", show_values=False, show_legend=True,
                      linear_color_scheme="blue_white_yellow", y_axis_format=",.0%", row_limit=1000)

    rows = [
        [(kpi_revenue, 3, 22), (kpi_orders, 3, 22), (kpi_aov, 3, 22), (kpi_customers, 3, 22)],
        [(monthly, 8), (categories, 4)],
        [(countries, 6), (tiers, 6)],
        [(channels, 5), (retention, 7)],
    ]
    upsert_dashboard(db, Dashboard, admin, SHOP_SLUG, SHOP_TITLE, rows)
    upsert_queries(db, SavedQuery, admin, general, "shop", SHOP_QUERIES)
    log(f"Dashboard ready at /superset/dashboard/{SHOP_SLUG}/ with {sum(len(r) for r in rows)} charts and {len(SHOP_QUERIES)} saved queries")


def seed_stream(ctx, general):
    db, SqlaTable, SqlMetric, Slice, Dashboard, SavedQuery, admin = ctx
    events = upsert_dataset(db, SqlaTable, SqlMetric, general, "shop", "order_events", STREAM_METRICS, main_dttm_col="event_time")

    def chart(name, viz_type, **params):
        return upsert_chart(db, Slice, events, admin, name, viz_type, params)

    total = chart("Events landed by Flink", "big_number_total", metric="events", y_axis_format="SMART_NUMBER")
    value = chart("Order value streamed (EUR)", "big_number_total", metric="revenue", y_axis_format="SMART_NUMBER")
    basket = chart("Average order value (EUR)", "big_number_total", metric="avg_basket", y_axis_format=",.2f")
    over_time = chart("Events over time", "echarts_timeseries_line", x_axis="event_time", time_grain_sqla="PT1M",
                      metrics=["events"], groupby=[], row_limit=1000)
    by_status = chart("Events by status", "pie", groupby=["status"], metric="events", donut=True, show_labels=True,
                      label_type="key_percent", row_limit=100)
    latest = chart("Latest events", "table", query_mode="raw",
                   all_columns=["event_time", "order_id", "customer_id", "quantity", "amount", "status", "kafka_partition", "kafka_offset"],
                   order_by_cols=['["event_time", false]'], row_limit=15, include_search=False, show_cell_bars=False)
    rows = [[(total, 4, 22), (value, 4, 22), (basket, 4, 22)], [(over_time, 8), (by_status, 4)], [(latest, 12)]]
    upsert_dashboard(db, Dashboard, admin, STREAM_SLUG, STREAM_TITLE, rows)
    log(f"Dashboard ready at /superset/dashboard/{STREAM_SLUG}/ with {sum(len(r) for r in rows)} charts")


def seed_profile(ctx, governed):
    db, SqlaTable, SqlMetric, Slice, Dashboard, SavedQuery, admin = ctx
    latest = upsert_dataset(db, SqlaTable, SqlMetric, governed, "silver", "customer_profile_360_latest", PROFILE_METRICS, sql=PROFILE_LATEST_SQL)
    history = upsert_dataset(db, SqlaTable, SqlMetric, governed, "silver", "customer_profile_360", PROFILE_METRICS, main_dttm_col="data_as_of_date")

    def chart(dataset, name, viz_type, **params):
        return upsert_chart(db, Slice, dataset, admin, name, viz_type, params)

    customers = chart(latest, "Customers in the latest snapshot", "big_number_total", metric="customers", y_axis_format="SMART_NUMBER")
    arpu = chart(latest, "Average monthly ARPU (EUR)", "big_number_total", metric="avg_arpu_eur", y_axis_format=",.2f")
    segments = chart(latest, "Customers by segment", "pie", groupby=["segment_code"], metric="customers", donut=True,
                     show_labels=True, label_type="key_value_percent", row_limit=100)
    deciles = chart(latest, "Churn risk by decile", "echarts_timeseries_bar", x_axis="churn_decile", metrics=["customers"],
                    groupby=[], orientation="vertical", row_limit=100, x_axis_sort_asc=True)
    consent = chart(latest, "Marketing e-mail consent rate by segment", "echarts_timeseries_bar", x_axis="segment_code",
                    metrics=["email_consent_rate"], groupby=[], orientation="vertical", row_limit=100, y_axis_format=",.0%")
    tiers = chart(latest, "Value tier economics", "table", query_mode="aggregate", groupby=["value_tier"],
                  metrics=["customers", "avg_arpu_eur", "avg_ltv_eur", "avg_churn_risk"], all_columns=[],
                  order_desc=True, row_limit=100, include_search=False)
    trend = chart(history, "Average churn risk by snapshot day", "echarts_timeseries_line", x_axis="data_as_of_date",
                  time_grain_sqla="P1D", metrics=["avg_churn_risk"], groupby=[], row_limit=1000)
    rows = [[(customers, 3), (arpu, 3), (segments, 6)], [(deciles, 6), (consent, 6)], [(tiers, 6), (trend, 6)]]
    upsert_dashboard(db, Dashboard, admin, PROFILE_SLUG, PROFILE_TITLE, rows)
    upsert_queries(db, SavedQuery, admin, governed, "silver", PROFILE_QUERIES)
    log(f"Dashboard ready at /superset/dashboard/{PROFILE_SLUG}/ with {len(rows)} rows and {len(PROFILE_QUERIES)} saved queries")


def main():
    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.connectors.sqla.models import SqlaTable, SqlMetric
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice
        from superset.models.sql_lab import SavedQuery

        admin = security_manager.find_user(os.environ.get("SUPERSET_ADMIN_USER", "admin"))
        ctx = (db, SqlaTable, SqlMetric, Slice, Dashboard, SavedQuery, admin)
        general = upsert_database(db, Database, GENERAL_DB, "trino://superset-analyst@trino:8080/iceberg", {})
        log(f"Connection ready: {GENERAL_DB!r}")
        if not wait_for_trino(general):
            return 1

        if table_available(general, SHOP_PROBE):
            seed_shop(ctx, general)
        else:
            log("Demo tables not found: run ./lakehouse demo to load them and seed the dashboard.")

        if table_available(general, STREAM_PROBE):
            seed_stream(ctx, general)
        else:
            log("Streaming table not found yet: it appears once the Flink job has run (./lakehouse up --with streaming).")

        if GOVERNED_PRINCIPAL:
            governed = upsert_database(
                db, Database, GOVERNED_DB, "trino://customer-analytics@trino:8080/iceberg",
                {"engine_params": {"connect_args": {"user": GOVERNED_PRINCIPAL}}},
            )
            log(f"Connection ready: {GOVERNED_DB!r}")
            if table_available(governed, PROFILE):
                seed_profile(ctx, governed)
            else:
                log("Customer Profile 360 not deployed yet; rerun ./lakehouse demo or restart analytics after the reference build.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
