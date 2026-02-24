# Runbook: Production Deploy (Admin Handoff Template)

## 1) Input package
- `deployment_manifest.yaml` (release steps)
- `env_overrides.yaml` (environment mapping, no secrets)
- `acceptance_report.md` (from corporate sandbox)
- `brandbook_conformance_report.yaml`
- `governance_verdict` reference

## 2) Preconditions
- Preview sign-off is approved.
- Governance verdict is `approve`.
- Admin has production credentials and rollback access.
- Maintenance window and communication channel are confirmed.

## 3) Commands (example flow)
```bash
# 1. Dry-run manifest validation
python "deploy/bi-as-code/scripts/bi_release_ctl.py" \
  --manifest "deploy/bi-as-code/release/deployment_manifest.yaml" \
  dry-run

# 2. Apply (admin-owned execution)
python "deploy/bi-as-code/scripts/bi_release_ctl.py" \
  --manifest "deploy/bi-as-code/release/deployment_manifest.yaml" \
  apply --confirm DEPLOY_TO_CORP_SANDBOX

# 3. Rollback planning (if required)
python "deploy/bi-as-code/scripts/bi_release_ctl.py" rollback
```

## 4) Verification checklist
- Dashboards are accessible for target roles.
- Key charts render and return non-error states.
- Date formats, labels, and status naming match contracts.
- Filter scopes and cross-filter behavior are valid.

## 5) Rollback policy
- Trigger rollback on critical rendering/data regressions.
- Execute reverse-step rollback plan from last apply state.
- Re-run smoke checks after rollback.

## 6) Completion record
- Deployment start/end timestamps.
- Executor (admin) and reviewer.
- Result: success/rollback.
- Incident notes and next actions.
