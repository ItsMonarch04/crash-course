# Security policy

Crash Course is an educational lab tool, not a production database. The real
listener intentionally has no authentication, authorization, or TLS and binds
to loopback by default. Do not expose it to an untrusted network — there is no
configuration that makes doing so safe.

## Reporting a vulnerability

Report suspected vulnerabilities privately through this repository's
**Security → Report a vulnerability** flow. Do not open a public issue,
discussion, or pull request with exploit details before I have had a chance to
assess the report.

Include the affected build or commit, the command and seed that reproduce it,
and whether data loss or code execution is possible. Reproducible simulator
seeds may be shared publicly after triage.
