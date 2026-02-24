# Runbook: Corporate Sandbox Deploy

## Preconditions
- You received explicit user command to deploy/clone into corporate sandbox.
- Release artifacts are committed and reviewed in Git.
- `semantic_contract` is valid.
- Corporate sandbox credentials are configured in environment (not in Git).

## Commands

### 1) Dry-run
```bash
python "deploy/bi-as-code/scripts/bi_release_ctl.py" \
  --manifest "deploy/bi-as-code/release/templates/deployment_manifest.template.yaml" \
  dry-run
```

### 2) Apply (explicit confirmation required)
```bash
python "deploy/bi-as-code/scripts/bi_release_ctl.py" \
  --manifest "deploy/bi-as-code/release/templates/deployment_manifest.template.yaml" \
  apply --confirm DEPLOY_TO_CORP_SANDBOX
```

### 3) Rollback plan (reverse order preview)
```bash
python "deploy/bi-as-code/scripts/bi_release_ctl.py" rollback
```

## What this stage does now
- Validates and prints release intent.
- Writes deterministic apply state for rollback planning.
- Does **not** execute live Superset API mutations yet (safe-mode controller).

## Promotion policy
- Corporate sandbox execution is on-demand by explicit command only.
- Production deployment is excluded from this runbook and handled by admin handoff package.
