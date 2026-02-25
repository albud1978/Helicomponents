#!/usr/bin/env python3
import os
import sys

from superset import db
from superset.app import create_app


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def build_clickhouse_uri() -> str:
    host = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
    port = os.getenv("CLICKHOUSE_PORT", "9000")
    database = os.getenv("CLICKHOUSE_DATABASE", "default")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    secure = env_bool("CLICKHOUSE_SECURE", False)
    proto_opts = "secure=true" if secure else "secure=false"
    return f"clickhousedb://{user}:{password}@{host}:{port}/{database}?{proto_opts}"


def ensure_database() -> None:
    from superset.models.core import Database

    database_name = os.getenv("SUPERSET_CH_DATABASE_NAME", "clickhouse_default")
    sqlalchemy_uri = build_clickhouse_uri()

    existing = db.session.query(Database).filter_by(database_name=database_name).one_or_none()
    if existing is None:
        existing = Database(database_name=database_name, sqlalchemy_uri=sqlalchemy_uri)
        print(f"[rbac] Creating database '{database_name}'")
    else:
        print(f"[rbac] Updating database '{database_name}'")
        existing.sqlalchemy_uri = sqlalchemy_uri

    existing.expose_in_sqllab = True
    existing.allow_run_async = False
    existing.allow_ctas = False
    existing.allow_cvas = False
    existing.allow_dml = False
    db.session.add(existing)
    db.session.commit()


def clone_role_permissions(app, target_role: str, source_role: str) -> None:
    sm = app.appbuilder.sm
    src = sm.find_role(source_role)
    if src is None:
        raise RuntimeError(f"Source role '{source_role}' not found")

    dst = sm.find_role(target_role)
    if dst is None:
        dst = sm.add_role(target_role)
        print(f"[rbac] Created role '{target_role}'")

    existing = {(p.permission.name, p.view_menu.name) for p in dst.permissions}
    for perm_view in src.permissions:
        key = (perm_view.permission.name, perm_view.view_menu.name)
        if key in existing:
            continue
        sm.add_permission_role(dst, perm_view)

    db.session.commit()
    print(f"[rbac] Synced permissions {source_role} -> {target_role}")


def main() -> int:
    app = create_app()
    with app.app_context():
        ensure_database()
        clone_role_permissions(app, "bi_viewer", "Gamma")
        clone_role_permissions(app, "bi_editor", "Alpha")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[rbac] ERROR: {exc}", file=sys.stderr)
        raise
