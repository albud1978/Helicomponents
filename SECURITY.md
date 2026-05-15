# Security policy

## Supported versions

Active development happens on the `master` branch.

Older branches and historical archives are not maintained for security fixes.

## Reporting a vulnerability

This repository is an internal development tool and research artifact.

If you discover a vulnerability:

- Open a private security advisory through the GitHub Security tab:
  Settings -> Security -> Advisories -> New draft.
- Or contact the maintainer directly. Use `CODEOWNERS`, if present, or the
  recent commit history to identify the maintainer.

Do not open a public issue for security-sensitive disclosures.

## Security model

### Scope (in scope)

- Multi-agent governance discipline:
  `.cursor/rules/**`, `.cursor/hooks/**`, `.cursor/agents/**`.
- Agent KG integrity:
  `config/agent_kg.json` and archive records.
- ETL and simulation data flow:
  read-only access to production AMOS is the design intent.
- Audit trail integrity:
  hash-chain verification for audit logs through `tools/audit_verify.py`.

### Out of scope

- Production deployment of a multi-agent service. The current architecture has
  no network-exposed agentic service, so AGPL v3 section 13 is not triggered.
  See `THIRD_PARTY.md`.
- Third-party dependency CVEs. See `THIRD_PARTY.md` and
  `deploy/sbom/sbom.cdx.json`.
- Cursor SaaS data handling. See Cursor Terms of Service; repository code can
  be sent to LLM providers during agent invocations.
- Production AMOS database security. That boundary is managed by the Utair
  infrastructure team.

## License-related security considerations

### FLAME GPU 2 (`pyflamegpu`)

- License: AGPL v3 plus commercial dual licensing.
- Current state: AGPL v3 section 13 is not triggered by the internal-only
  architecture.
- Critical future-state boundary: if simulation becomes a network-exposed
  service, stop and choose between AGPL source availability obligations and a
  commercial license.
- Reference: `THIRD_PARTY.md`.

### Neo4j Community Server (Docker)

- License: GPL v3.
- Current use: separate process via `bolt://localhost:7687`; no source linking
  to Neo4j Server.
- Reference: `THIRD_PARTY.md`.

### CUDA 13.0 Toolkit

- License: proprietary NVIDIA commercial EULA.
- Current use: internal development on NVIDIA hardware.
- Do not distribute CUDA toolkit binaries as project artifacts.

## Hardening guidelines

### Secrets

- Do not commit `.env` files to Git. Use `.env.example` as a template only.
- Store ClickHouse credentials, Neo4j passwords, and similar secrets only in
  local environment files.
- `THIRD_PARTY.md` and `SECURITY.md` must not contain concrete secrets.

### Audit

- Run `tools/audit_verify.py` to verify audit-log hash-chain integrity.
- Use `tools/audit_summarize.py` for sanitized weekly summaries in
  `docs/audit_summaries/` when the audit workflow requires them.

### CI checks

- `.github/workflows/quality.yml` validates JSON files, Python syntax, and
  selected invariant/TEMP drift signals.
- Hygiene findings are reported as non-blocking because stale workflow cleanup
  remains human-in-the-loop.

## Updates

This document was last reviewed on 15-05-2026
(`W_compliance_remediation_2026_05_15`).
