# Security Policy

## Threat model (what Wavicle defends)

- **Wire**: TLS termination (1.2 minimum, 1.3 negotiated); plaintext fallback is a
  boot-time fatal, never silent. Clients must pin or CA-verify in production —
  self-signed certs are staging-only.
- **Auth**: multi-user ACL file (Redis-style, `chmod 0600` — passwords are
  cleartext at rest in the file, SHA256 in memory). Constant-time password
  compare. Unknown tokens fail the boot. Off-by-default users.
- **Injection**: table/column identifiers allowlisted before SQL interpolation
  (PG + MySQL); wrong keys fail closed, never interpolated.
- **Availability attacks**: RESP parser caps (10k args, 1MB arg); 10k connection
  cap with explicit error; per-table metric labels capped at 64 series.
- **Supply chain**: no Redis dependency of any kind; CDC via `pgx/pglogrepl`.

## What is NOT defended (say it before an auditor does)

No RBAC/SSO, no audit log, no at-rest encryption (rely on PG + disk encryption),
no rate limiting, no SOC2. Single shared `requirepass` mode exists only for
backward compatibility — pilot uses the ACL file. The slot is never auto-dropped:
availability of PG's disk is a human decision (runbook procedure).

## Reporting

Security issues: email **tanmayjoddar17@gmail.com** with "[wavicle-security]" in
the subject. Do not file public issues for credentials, bypasses, or
data-exposure bugs. Response: best-effort acknowledgment within 3–5 business
days with a fix-or-plan (solo maintainer alongside internship and exams — a
modest SLA kept beats an ambitious one missed). Supported: latest `master` and
the newest `v*` tag only. Latest scan: `docc/VULN_SCAN.md` (2026-09-29, clean).

## Verified posture (evidence, not adjectives)

ACL parser + enforcement tests (`internal/auth/`), all-keys auth incl. TX
(`server.go:allKeys`, queue+exec re-check), fail-fast boot proofs (bad ACL/cert
= exit 1, verified live 2026-09-28), chaos suite (`tests/chaos/`), Tier-1
checklist items 11–14.
