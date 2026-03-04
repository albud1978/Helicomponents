#!/usr/bin/env python3
"""Git-oriented Superset dashboard bundle sync.

Workflow:
1) export bundle from running Superset into repository directory (YAML files).
2) commit/push to Git.
3) pull on another machine.
4) import bundle into target Superset.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


@dataclass
class SupersetSession:
    base_url: str
    token: str
    csrf_token: str
    session: requests.Session

    @property
    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @property
    def write_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-CSRFToken": self.csrf_token,
            "Referer": f"{self.base_url}/",
        }


def _build_base_url(raw: str) -> str:
    return raw.rstrip("/")


def _require_non_empty(name: str, value: str | None) -> str:
    if value and value.strip():
        return value.strip()
    raise RuntimeError(
        f"Missing required parameter: {name}. "
        f"Pass it explicitly or set corresponding SUPERSET_API_* env var."
    )


def _login(
    base_url: str,
    username: str,
    password: str,
    provider: str = "db",
    timeout_sec: int = 30,
) -> SupersetSession:
    session = requests.Session()
    login = session.post(
        f"{base_url}/api/v1/security/login",
        json={
            "username": username,
            "password": password,
            "provider": provider,
            "refresh": True,
        },
        timeout=timeout_sec,
    )
    login.raise_for_status()
    token = login.json().get("access_token")
    if not token:
        raise RuntimeError("Login succeeded without access_token")

    csrf = session.get(
        f"{base_url}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout_sec,
    )
    csrf.raise_for_status()
    csrf_token = csrf.json().get("result")
    if not csrf_token:
        raise RuntimeError("Failed to obtain CSRF token")

    return SupersetSession(
        base_url=base_url,
        token=token,
        csrf_token=csrf_token,
        session=session,
    )


def _parse_dashboard_ids(raw: str) -> list[int]:
    values = [x.strip() for x in raw.split(",") if x.strip()]
    ids = [int(x) for x in values]
    if not ids:
        raise ValueError("No dashboard ids provided")
    return ids


def _safe_recreate_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def export_dashboard_bundle(
    ss: SupersetSession,
    dashboard_ids: list[int],
    output_dir: Path,
    timeout_sec: int = 120,
) -> None:
    r = ss.session.get(
        f"{ss.base_url}/api/v1/dashboard/export/",
        params={"q": json.dumps(dashboard_ids)},
        headers=ss.write_headers,
        timeout=timeout_sec,
    )
    r.raise_for_status()
    ctype = (r.headers.get("content-type") or "").lower()
    if "zip" not in ctype and not r.content.startswith(b"PK"):
        raise RuntimeError(f"Unexpected export response content-type: {ctype}")

    _safe_recreate_dir(output_dir)
    with tempfile.TemporaryDirectory(prefix="superset_export_") as temp_dir:
        extract_dir = Path(temp_dir)
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(extract_dir)

        children = [p for p in extract_dir.iterdir()]
        payload_root = children[0] if len(children) == 1 and children[0].is_dir() else extract_dir
        normalized_root = output_dir / "dashboard_export_bundle"
        normalized_root.mkdir(parents=True, exist_ok=True)

        for src in sorted(payload_root.iterdir()):
            dst = normalized_root / src.name
            if src.is_dir():
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)

    meta = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source": ss.base_url,
        "dashboard_ids": dashboard_ids,
        "file_count": len([p for p in output_dir.rglob("*") if p.is_file()]),
    }
    (output_dir / "_export_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _zip_directory(input_dir: Path, out_zip: Path) -> None:
    with zipfile.ZipFile(out_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(input_dir.rglob("*")):
            if not file_path.is_file():
                continue
            rel = file_path.relative_to(input_dir).as_posix()
            zf.write(file_path, arcname=rel)


def import_dashboard_bundle(
    ss: SupersetSession,
    bundle_dir: Path,
    overwrite: bool,
    passwords_json: str | None = None,
    ssh_tunnel_passwords_json: str | None = None,
    ssh_tunnel_private_key_passwords_json: str | None = None,
    ssh_tunnel_private_keys_json: str | None = None,
    timeout_sec: int = 180,
) -> dict[str, Any]:
    if not bundle_dir.exists():
        raise FileNotFoundError(f"Bundle dir does not exist: {bundle_dir}")

    with tempfile.TemporaryDirectory(prefix="superset_bundle_") as temp_dir:
        zip_path = Path(temp_dir) / "dashboard_bundle.zip"
        _zip_directory(bundle_dir, zip_path)
        with zip_path.open("rb") as f:
            files = {"formData": ("dashboard_bundle.zip", f, "application/zip")}
            data: dict[str, str] = {"overwrite": "true" if overwrite else "false"}
            if passwords_json:
                data["passwords"] = passwords_json
            if ssh_tunnel_passwords_json:
                data["ssh_tunnel_passwords"] = ssh_tunnel_passwords_json
            if ssh_tunnel_private_key_passwords_json:
                data["ssh_tunnel_private_key_passwords"] = ssh_tunnel_private_key_passwords_json
            if ssh_tunnel_private_keys_json:
                data["ssh_tunnel_private_keys"] = ssh_tunnel_private_keys_json

            r = ss.session.post(
                f"{ss.base_url}/api/v1/dashboard/import/",
                headers=ss.write_headers,
                files=files,
                data=data,
                timeout=timeout_sec,
            )
            r.raise_for_status()
            try:
                return r.json()
            except ValueError:
                return {
                    "status_code": r.status_code,
                    "message": r.text.strip() or "OK",
                }


def _read_optional_text(path: str | None) -> str | None:
    if not path:
        return None
    p = Path(path)
    return p.read_text(encoding="utf-8")


def cmd_export(args: argparse.Namespace) -> int:
    base_url = _build_base_url(args.base_url)
    ss = _login(base_url, args.username, args.password, provider=args.provider)
    dashboard_ids = _parse_dashboard_ids(args.dashboard_ids)
    out_dir = Path(args.output_dir)
    export_dashboard_bundle(ss, dashboard_ids, out_dir, timeout_sec=args.timeout_sec)
    print(f"export OK: {out_dir}")
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    base_url = _build_base_url(args.base_url)
    ss = _login(base_url, args.username, args.password, provider=args.provider)
    result = import_dashboard_bundle(
        ss=ss,
        bundle_dir=Path(args.bundle_dir),
        overwrite=args.overwrite,
        passwords_json=_read_optional_text(args.passwords_file),
        ssh_tunnel_passwords_json=_read_optional_text(args.ssh_tunnel_passwords_file),
        ssh_tunnel_private_key_passwords_json=_read_optional_text(
            args.ssh_tunnel_private_key_passwords_file
        ),
        ssh_tunnel_private_keys_json=_read_optional_text(args.ssh_tunnel_private_keys_file),
        timeout_sec=args.timeout_sec,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    default_base_url = os.getenv("SUPERSET_API_BASE_URL")
    default_username = os.getenv("SUPERSET_API_USERNAME")
    default_password = os.getenv("SUPERSET_API_PASSWORD")
    default_provider = os.getenv("SUPERSET_API_PROVIDER", "db")
    default_timeout = int(os.getenv("SUPERSET_API_TIMEOUT_SEC", "120"))

    p = argparse.ArgumentParser(description="Superset dashboard bundle sync via Git.")
    p.add_argument("--base-url", default=default_base_url)
    p.add_argument("--username", default=default_username)
    p.add_argument("--password", default=default_password)
    p.add_argument("--provider", default=default_provider)
    p.add_argument("--timeout-sec", type=int, default=default_timeout)

    sub = p.add_subparsers(dest="cmd", required=True)

    p_export = sub.add_parser("export", help="Export dashboard bundle into repository directory.")
    p_export.add_argument("--dashboard-ids", default="1")
    p_export.add_argument(
        "--output-dir",
        default="deploy/bi-as-code/superset/bundles/dashboard_1",
    )
    p_export.set_defaults(func=cmd_export)

    p_import = sub.add_parser("import", help="Import dashboard bundle from repository directory.")
    p_import.add_argument(
        "--bundle-dir",
        default="deploy/bi-as-code/superset/bundles/dashboard_1",
    )
    p_import.add_argument("--overwrite", action="store_true")
    p_import.add_argument("--passwords-file")
    p_import.add_argument("--ssh-tunnel-passwords-file")
    p_import.add_argument("--ssh-tunnel-private-key-passwords-file")
    p_import.add_argument("--ssh-tunnel-private-keys-file")
    p_import.set_defaults(func=cmd_import)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.base_url = _require_non_empty("--base-url / SUPERSET_API_BASE_URL", args.base_url)
    args.username = _require_non_empty("--username / SUPERSET_API_USERNAME", args.username)
    args.password = _require_non_empty("--password / SUPERSET_API_PASSWORD", args.password)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
