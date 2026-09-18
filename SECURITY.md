# Security policy

## Supported releases

Security fixes are made against the current published release line. Upgrade to
the latest release before reporting a suspected issue where possible.

## Reporting a vulnerability

Please report vulnerabilities privately to the repository maintainer or your
organization’s designated security contact. Include the affected version,
reproduction steps, impact, and any relevant logs with credentials and
customer data removed. Do not open a public issue for a suspected active
authorization bypass.

The maintainer will acknowledge reports, assess severity, coordinate a fix,
and agree on disclosure timing with the reporter. Deployment-specific concerns
(TLS certificates, reverse proxy exposure, backups, identity-provider setup,
and incident response) must also be sent to the deployment operator.

## Production baseline

The server is designed to fail closed when `ENV=production` lacks its required
host configuration: trusted HTTPS origin and hosts, proxy CIDRs, CSP,
authenticated TLS Redis rate limiting/cache, request limits, and a JWT key
ring with issuer/audience validation. Every externally reachable dataset needs
an explicit server-owned row security policy; client filter, table, history,
or Flight ticket fields are not authorization authority.

Run the [production security runbook](docs/operations/production-security-runbook.md)
before public deployment. Its deployment verifier and CI evidence are useful
controls, not substitutes for certificate management, independent penetration
testing, restore testing, key-management governance, or an incident-response
program.

The assurance baseline is OWASP ASVS v5.0.0 Level 2. The
[certification matrix](security/certification-matrix.json) identifies which
controls have repository-local evidence and which require fresh operator
attachments. Validate a release manifest with
`python scripts/verify_security_release_evidence.py --matrix security/certification-matrix.json --evidence <protected-manifest> --expected-tag <tag> --expected-commit <sha> --expected-environment <protected-environment> --expected-deployment-origin <https-origin>`;
the tool binds completeness and freshness to that immutable release without
claiming that external deployment work occurred.
