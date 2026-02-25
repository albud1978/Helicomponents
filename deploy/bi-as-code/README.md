# BI as Code (Sandbox + Prod Handoff)

## Scope Freeze (current stage)
- Active scope: `personal sandbox -> corporate sandbox`.
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

## Git migration mode (regular A <-> B sync)

### What is now stored in Git for migration
- `deploy/superset-local/` (Docker compose + init/config/scripts), except secrets in `.env`.
- `deploy/bi-as-code/contracts/` (semantic + brandbook contracts).
- `deploy/bi-as-code/superset/` (BI manifests and exported bundle directory).
- `deploy/bi-as-code/scripts/superset_git_sync.py` (export/import controller via Superset API).
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

### Critical notes
- `.env` is local-only and ignored; never commit secrets.
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
