# Acceptance Report (Corporate Sandbox)

## Release meta
- release_id: `REPLACE_ME`
- source_commit: `REPLACE_ME`
- environment: `corp-sandbox`
- validated_at_utc: `YYYY-MM-DDTHH:MM:SSZ`
- validator_role: `REPLACE_ME`

## Scope covered
- datasets:
  - `REPLACE_ME`
- charts:
  - `REPLACE_ME`
- dashboards:
  - `REPLACE_ME`

## Checks
- API smoke:
  - [ ] charts readable via API
  - [ ] dashboards readable via API
- Render smoke:
  - [ ] key histogram tooltip/legend check
  - [ ] key gantt tooltip/date/label check
- Filter behavior:
  - [ ] native filter scope valid
  - [ ] no unwanted cross-filter coupling
- Contracts:
  - [ ] semantic_contract conformance
  - [ ] brandbook_contract conformance (or approved exceptions)

## Result
- status: `PASS | FAIL | PASS_WITH_EXCEPTIONS`
- critical_issues: `0`
- non_critical_issues: `0`

## Exceptions (if any)
- rule_id:
- justification:
- approver:
- expiration_date:

## Recommendation
- Promote to production handoff: `YES | NO`
