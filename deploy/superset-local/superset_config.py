import os


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "local-dev-secret-key")
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
BABEL_DEFAULT_LOCALE = os.getenv("SUPERSET_DEFAULT_LOCALE", "ru")
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "ru": {"flag": "ru", "name": "Russian"},
}

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.getenv('DATABASE_USER', 'superset')}:"
    f"{os.getenv('DATABASE_PASSWORD', 'superset')}@"
    f"{os.getenv('DATABASE_HOST', 'db')}:"
    f"{os.getenv('DATABASE_PORT', '5432')}/"
    f"{os.getenv('DATABASE_DB', 'superset')}"
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_cache_",
    "CACHE_REDIS_URL": REDIS_URL,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": REDIS_URL,
}

RESULTS_BACKEND = CACHE_CONFIG

class CeleryConfig:
    broker_url = REDIS_URL
    imports = ("superset.sql_lab",)
    result_backend = REDIS_URL
    task_acks_late = True
    worker_prefetch_multiplier = 1
    task_annotations = {"sql_lab.get_sql_results": {"rate_limit": "100/s"}}


CELERY_CONFIG = CeleryConfig
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": True,
    "EMBEDDED_SUPERSET": False,
}

ROW_LIMIT = 100000
SQL_MAX_ROW = 100000

PUBLIC_ROLE_LIKE_GAMMA = env_bool("PUBLIC_ROLE_LIKE_GAMMA", False)
