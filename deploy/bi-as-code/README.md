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
