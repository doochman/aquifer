"""Local-lab Superset settings. Secrets come from the environment; nothing here is production-grade."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_URI"]

ROW_LIMIT = 50_000
SQL_MAX_ROW = 100_000
SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_TIMEOUT = 120

# Never let a BI user point Superset at the metadata store or a local file.
PREVENT_UNSAFE_DB_CONNECTIONS = True

# The stack binds to loopback only; Talisman's browser policy would break the local HTTP origin.
TALISMAN_ENABLED = False

_CACHE = {
    "CACHE_TYPE": "FileSystemCache",
    "CACHE_DIR": "/app/superset_home/cache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "meta_"}
DATA_CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "data_"}
FILTER_STATE_CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "filter_"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "explore_"}

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
