# BI as Code (Sandbox + Prod Handoff)

## Scope Freeze (current stage)
- Active scope: `personal sandbox -> corporate sandbox`.
- **Single transfer mode: Mode B (repo-only) only.**
- Exact clone mode is out of active process and is not used by default.
- Production deploy is **not** executed by project agents at this stage.
- Production is treated as a handoff target for corporate admins.
- Any clone/deploy action to corporate sandbox is executed **only** by explicit user command.

## Roles (strict BI model)
- `orchestrator`: plans phases, controls gates, assembles handoff.
- `analyst-sql-graph`: validates KPI semantics and aggregation correctness.
- `coder-general`: maintains BI artifacts and API applier in this directory.
- `governance-compliance`: policy/risk verdict before preview handoff.
- `docs-curator`: keeps runbooks/changelog/doc sync.
- `corp BI admins` (external): execute production deployment by approved runbook.

## Directory layout
- `contracts/` - semantic and brandbook contracts.
- `domains/` - domain-level BI declarations.
- `superset/datasets/` - dataset manifests.
- `superset/charts/` - chart manifests.
- `superset/dashboards/` - dashboard manifests and layout metadata.
- `scripts/` - dry-run/apply/rollback tools.
- `release/` - handoff artifacts and templates for preview/prod.

## Operational gates
1. Scope and role matrix approved.
2. `semantic_contract` is defined.
3. `brandbook_contract` is provided and mapped.
4. Explicit command to deploy/clone into corporate sandbox is received.
5. Production handoff package is complete and validated (without prod apply).

## Safety rules
- No secrets in Git. Use environment variables or secured secret stores.
- All artifacts are idempotent and reviewable in Git before apply.
- Runtime emergency patches must be backported into source artifacts.

## Instance isolation model

Each machine runs a **fully isolated** Superset instance:
- Separate Docker containers and named volumes (no shared state between machines).
- Superset metadata DB (PostgreSQL) is local to each machine.
- **Git bundle is the only state transfer mechanism** between instances.
- Starting Superset on machine B from the same `docker-compose.yml` does NOT affect machine A in any way.

### What "same content" means
After bootstrap, a fresh instance has **no dashboards**. To replicate content from another machine:
1. Pull the latest Git state (`git pull`).
2. Run `import --overwrite` (see section below).

Content is in sync only after explicit import. Without import, instances diverge independently.

### Running two instances on the same machine
Possible, but requires three isolation parameters:

| Parameter | Default | Override |
|---|---|---|
| Container names | `superset-local`, `superset-db-local`, `superset-redis-local` | Remove `container_name` from compose or rename |
| Port | `8088` | Set `SUPERSET_PORT=8089` in `.env` |
| Compose project | directory name | `docker compose -p superset2 -f ...` |

Example (second instance on port 8089):
```bash
SUPERSET_PORT=8089 docker compose -p superset2 \
  -f "deploy/superset-local/docker-compose.yml" up -d --build
```

## Scope Freeze: ONLY Mode B (repo-only)

> **The only supported migration mode is Mode B (repo-only via Git bundle + plugin image build from source).**
> Exact-clone mode (container snapshot) is archived and not used in regular workflow.
> New agents must NOT use exact-clone scripts unless explicitly instructed by the user.

## Git migration mode (regular A <-> B sync)

### What is now stored in Git for migration
- `deploy/superset-local/` (Docker compose + init/config/scripts), except secrets in `.env`.
- `deploy/bi-as-code/contracts/` (semantic + brandbook contracts).
- `deploy/bi-as-code/superset/` (BI manifests and exported bundle directory).
- `deploy/bi-as-code/scripts/superset_git_sync.py` (export/import controller via Superset API).
- `superset-frontend/plugins/plugin-chart-echarts6-gantt/` (custom chart source for local plugin image build).
- `.cursor/hooks/code_edit_audit.log` is versioned for project traceability.
- `.cursor/hooks/user_comm_audit.log` is local-only and not synchronized via Git.

### Mandatory onboarding for a new agent (read order)
1) `README.md` (section `BI (Superset)` and migration pointers).
2) `deploy/bi-as-code/README.md` (this file, full runbook).
3) `.cursor/rules/00_global_always.mdc` and `.cursor/rules/90_multiagent_workflow.mdc` (governance and workflow).
4) Latest audit entries:
   - `.cursor/hooks/code_edit_audit.log` (in Git)
   - `.cursor/hooks/user_comm_audit.log` (local machine only)
5) Current BI artifact state in Git:
   - `deploy/bi-as-code/superset/bundles/dashboard_1/`

If these five points are read, a new agent has enough context to deploy and continue regular A <-> B synchronization.

### One-time bootstrap on any machine after `git pull`
1) Prepare local env for Superset:
```bash
cp "deploy/superset-local/.env.example" "deploy/superset-local/.env"
# fill real secrets in deploy/superset-local/.env (do not commit)
```

2) Start local Superset:
```bash
docker compose -f "deploy/superset-local/docker-compose.yml" up -d --build
```

3) Health check:
```bash
curl -s -o /dev/null -w "%{http_code}\n" "http://127.0.0.1:8088/health"
```

### WSL2 notes (if machine is Windows + WSL)
- Run all commands from Linux side (WSL shell), not from PowerShell.
- Keep repo in Linux filesystem (e.g. `/home/<user>/...`) for faster Docker bind-mounts.
- Ensure Docker Desktop integration with WSL distro is enabled.
- `127.0.0.1:8088` is valid from WSL and Windows browser in standard Docker Desktop setup.
- If corporate policies remap networking, keep `SUPERSET_PORT` configurable in `deploy/superset-local/.env`.

### Repo-only mode (no cloud registry, local build on each machine)
Use this mode when both machines have Docker Desktop/WSL and you want full reproducibility only via Git.

1) Build custom Superset image with `echarts6_gantt` plugin from repository source:
```bash
bash "deploy/superset-local/scripts/build_superset_with_plugin.sh"
```

2) Start stack with plugin override:
```bash
bash "deploy/superset-local/start_local_plugin.sh"
```

3) Verify chart type is registered:
```bash
python - <<'PY'
import requests
base="http://127.0.0.1:8088"
s=requests.Session()
t=s.post(base+"/api/v1/security/login",json={"username":"admin","password":"admin","provider":"db","refresh":True},timeout=30).json()["access_token"]
h={"Authorization":f"Bearer {t}"}
r=s.get(base+"/api/v1/chart/",headers=h,params={"q":"(page:0,page_size:1)"},timeout=30)
print("api_ok", r.status_code)
PY
```
If dashboard still says `Item with key "echarts6_gantt" is not registered`, run hard refresh (`Ctrl+Shift+R`) and ensure startup used `docker-compose.plugin.yml`.
This guarantees plugin registration and chart rendering. Visual fine-tuning from one-off runtime asset patches must be reapplied only if explicitly required.

### ~~Exact 1:1 clone mode~~ (ARCHIVED — do not use)
> **ARCHIVED. Not part of regular workflow. Scripts kept for reference only.**
> Use Mode B (repo-only) instead. See `Scope Freeze` section above.

#### Source machine (export exact clone)
```bash
bash "deploy/superset-local/scripts/export_exact_superset_clone.sh"
```

Default output:
- `output/superset_exact_clone_<UTC>/superset-image.tar`
- `output/superset_exact_clone_<UTC>/superset_meta.dump`
- `output/superset_exact_clone_<UTC>/superset_home.tar.gz`
- `output/superset_exact_clone_<UTC>/image_ref.txt`

Transfer that output directory to the target machine by any secure channel.

#### Target machine (import exact clone)
```bash
bash "deploy/superset-local/scripts/import_exact_superset_clone.sh" \
  "output/superset_exact_clone_<UTC>"
```

What import does:
1) `docker load` image tar.
2) Recreates Superset metadata DB from dump.
3) Starts compose with plugin override using imported image.
4) Restores `superset_home` archive (if present).
5) Waits for health check.

#### Notes and limits (archived)
- Scripts are kept for reference; not used in standard workflow.
- Keep `.env` local and never commit secrets.
### Export current BI state to Git (machine A)
```bash
python "deploy/bi-as-code/scripts/superset_git_sync.py" \
  --base-url "http://127.0.0.1:8088" \
  --username "admin" \
  --password "admin" \
  export \
  --dashboard-ids "1" \
  --output-dir "deploy/bi-as-code/superset/bundles/dashboard_1"
```

Then:
```bash
git add deploy/bi-as-code/superset/bundles/dashboard_1
git commit -m "sync superset dashboard bundle"
git push
```

### Import BI state from Git (machine B)
```bash
git pull
bash "deploy/superset-local/start_local_plugin.sh"
python "deploy/bi-as-code/scripts/superset_git_sync.py" \
  --base-url "http://127.0.0.1:8088" \
  --username "admin" \
  --password "admin" \
  import \
  --bundle-dir "deploy/bi-as-code/superset/bundles/dashboard_1" \
  --overwrite
```

### Regular back-and-forth workflow
- A changed BI in UI -> export -> commit/push.
- B pulls -> import --overwrite.
- B changed BI in UI -> export -> commit/push.
- A pulls -> import --overwrite.

### New agent post-pull checklist (mandatory, Mode B only)
1) Read this file fully. Confirm scope: local sandbox only, Mode B.
2) Verify plugin source exists:
   - `superset-frontend/plugins/plugin-chart-echarts6-gantt/package.json`
   - `superset-frontend/plugins/plugin-chart-echarts6-gantt/src/plugin/Echarts6Gantt.tsx`
3) Create `.env` from `.env.example`. Set `CLICKHOUSE_PORT=8123` (HTTP, for `clickhousedb://`).
4) Run `bash deploy/superset-local/start_local_plugin.sh` (not base `start_local.sh`).
5) Health check: `curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8088/health` -> `200`.
6) Import bundle and check dashboard `1` + chart `5`.
7) If import requires DB password map, use key `databases/clickhouse.yaml`.
8) If `echarts6_gantt is not registered` then startup used wrong compose path; repeat step 4.

### Critical notes
- `.env` is local-only and ignored; never commit secrets.
- `deploy/superset-local/scripts/export_exact_superset_clone.sh` and `import_exact_superset_clone.sh` are kept in repository, but not part of active Mode B workflow.
- `code_edit_audit.log` is versioned by design for traceability:
```bash
git add ".cursor/hooks/code_edit_audit.log"
```
- If dashboard bundle contains database YAML requiring passwords, pass JSON maps to import:
  - `--passwords-file`
  - `--ssh-tunnel-passwords-file`
  - `--ssh-tunnel-private-key-passwords-file`
  - `--ssh-tunnel-private-keys-file`
- Important for `--passwords-file`: key must be the path inside bundle ZIP, not database display name.
  - For current bundle, use key: `"databases/clickhouse.yaml"`.
  - Example:
```bash
cat > /tmp/superset_passwords.json <<'EOF'
{"databases/clickhouse.yaml":"REPLACE_WITH_REAL_PASSWORD"}
EOF

python "deploy/bi-as-code/scripts/superset_git_sync.py" \
  --base-url "http://127.0.0.1:8088" \
  --username "admin" \
  --password "admin" \
  import \
  --bundle-dir "deploy/bi-as-code/superset/bundles/dashboard_1" \
  --overwrite \
  --passwords-file "/tmp/superset_passwords.json"
```
- If UI still shows old assets, restart container and hard refresh:
```bash
docker restart superset-local
# browser: Ctrl+Shift+R
```
