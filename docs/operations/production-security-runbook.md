# Production security runbook

This runbook is the launch gate for a public Dash, React, Streamlit, HTTP, or
Arrow Flight deployment. The library validates configuration and supplies host
middleware, but it cannot obtain certificates, operate a Redis service, run an
independent penetration test, restore a customer backup, or manage an
organization's incident response.

## Before a production start

The release owner must set `ENV=production` and provide a valid HTTPS
`PIVOT_PUBLIC_ORIGIN`, `PIVOT_TRUSTED_HOSTS`, `PIVOT_TRUSTED_PROXY_NETWORKS`,
`PIVOT_CSP`, an authenticated `PIVOT_RATE_LIMIT_REDIS_URL`, and positive rate
and request limits. Configure an authenticated `rediss://` cache URL with
certificate validation. Production JWTs require `PIVOT_JWT_KEYS_JSON`,
`PIVOT_JWT_ACTIVE_KID`, `JWT_ISSUER`, `JWT_AUDIENCE`, `exp`, `iss`, `aud`, and
`kid`. Configure a server-owned dataset policy with explicit `rowSecurity` for
every externally reachable table; use `mode: global` only intentionally.

`ENV`, `APP_ENV`, `FLASK_ENV`, and `DASH_ENV` are one production decision: any
non-empty values must agree after `prod`/`production` and `dev`/`local` alias
normalization. Production HMAC JWT verification keys require at least 32 UTF-8
bytes. Arrow Flight authentication cannot be disabled in production, even when
a local-development environment flag is present. Production Flight must use a
`grpc+tls://` location and provide non-empty `tls_certificates` to the PyArrow
server; plaintext `grpc://` is not a production fallback. Configure the
certificate/key material through the deployment secret manager, not a package
artifact.

Deploy the reviewed [Nginx reference configuration](../../deploy/nginx/serverside-pivot.conf)
or an equivalent managed edge. The ASGI service must be reachable only from
the configured proxy network. Use a per-principal Flight resolver and table
authorizer; `require_auth=False` is local development only.

ASGI deployments reject both declared and chunked oversized bodies before an
endpoint runs. Dash's Flask host installs the matching request/header/CSRF
boundary through `register_pivot_app`; WSGI hosts must still retain the edge
body limit because the reverse proxy is the authoritative streamed-body gate.

## Release evidence

The release owner attaches these to the release record:

1. `python scripts/verify_production_deployment.py --base-url https://pivot.example.com --output security-deployment.json` output.
2. The CI `security-evidence` artifact: dependency audit, Bandit report, and CycloneDX SBOM.
3. The current local certification probe output, with no production URL, data, or credential included.
4. A review record proving only trusted proxy CIDRs can reach the application port, TLS certificate validity, and CSP compatibility for Dash/React/Streamlit hosts.
5. A signed operational approval from the release owner and security owner.

The assurance baseline is **OWASP ASVS v5.0.0 Level 2**. The repository's
[control matrix](../../security/certification-matrix.json) separates code-backed
checks from operator-owned controls. For every release, copy the example
manifest to a protected evidence directory, add only the current approved
attachments, and validate the manifest without putting sensitive evidence in
the repository. The release job runs on the protected `pypi-release`
environment and requires its environment-scoped `PIVOT_RELEASE_EVIDENCE_PATH`
to name that mounted manifest. It deliberately rejects the checked-in example
and cannot publish without current evidence and an exact protected release
binding.

`security/release-evidence.example.json` intentionally fails against the full
matrix: it has no operator attachments and is a schema template, not a release
attestation. Before validation, replace every placeholder with the immutable
tag and source SHA being released, the protected environment name, and its
credential-free HTTPS origin. The `scope.matrixSha256` must be the SHA-256 of
the raw current matrix file and `scope.controlIds` must list every current
matrix and operational-control ID exactly once. The validator checks these
bindings, attachment paths, hashes, owners, expiry, and completeness but does
not inspect or claim to have performed the attached work.

The `pypi-release` GitHub environment must define all three protected
variables below; do not source them from the tag, workflow input, or an
unprotected repository file:

- `PIVOT_RELEASE_EVIDENCE_PATH` — a mounted, protected manifest (never the example).
- `PIVOT_RELEASE_ENVIRONMENT` — the exact environment named by the manifest.
- `PIVOT_RELEASE_DEPLOYMENT_ORIGIN` — the exact public `https://host` origin named by the manifest.

The release job supplies the immutable `GITHUB_REF_NAME` and `GITHUB_SHA`. Run
the same binding check locally with explicit values before approving a release:

```bash
python scripts/verify_security_release_evidence.py \
  --matrix security/certification-matrix.json \
  --evidence /protected/release-evidence.json \
  --repository-root . \
  --expected-tag vX.Y.Z \
  --expected-commit "$(git rev-parse HEAD)" \
  --expected-environment production \
  --expected-deployment-origin https://pivot.example.com
```

Before it can publish, the protected release job also runs
`verify_production_deployment.py` against `PIVOT_RELEASE_DEPLOYMENT_ORIGIN` and
uploads its JSON result with the release evidence. This is an edge-behaviour
check, not evidence of certificate custody, private ingress review, IdP
configuration, restoration, incident exercise, or an independent penetration
test; those still require the named current attachments.

The release workflow creates pip-audit, npm audit (`--omit=dev`), Bandit, and
CycloneDX reports before publication. A skipped or malformed Python audit entry
fails; JavaScript production findings have no exception path and fail. Python
dependency findings pass only when
`security/dependency-exceptions.json` contains an exact package/version/id,
an accountable owner and ticket, a rationale, and a strictly future expiry;
run the same gate locally with:

```bash
python scripts/ci/verify_dependency_audit.py \
  --audit security/reports/pip-audit.json \
  --exceptions security/dependency-exceptions.json

python scripts/ci/verify_npm_audit.py \
  --audit security/reports/npm-audit.json
```

The matrix's ASVS controls are exact references to the vendored released
v5.0.0 requirement catalog. Penetration testing, backup restoration, and
incident/sign-off are listed separately as operational controls because ASVS
does not define them as one-for-one application verification requirements;
they are still mandatory protected release evidence.

## Recurring controls and owners

| Control | Owner | Cadence | Evidence |
| --- | --- | --- | --- |
| Certificate renewal and TLS scan | Platform | Before expiry and quarterly | Certificate monitor, HTTPS verifier output |
| CSP, frame embedding, CSRF, and rate-limit review | Application security | Each release | Browser/network test and configuration diff |
| JWT key rotation | Identity team | Every 90 days and after any suspected exposure | Active/previous `kid` rollout record; previous key removal confirmation |
| Dependency triage and SBOM review | Maintainer | Weekly and every release | CI reports and tracked remediation decision |
| Dataset/row policy review | Data owner | Each new dataset and quarterly | Policy review showing roles, operations, and `rowSecurity` mapping |
| External penetration test | Independent security provider | Annually and before major exposure changes | Signed report and remediation tracking |
| Incident tabletop and response review | Security owner | Semiannually | Exercise notes, contacts, and follow-up actions |
| Encrypted backup restore | Operations | Quarterly | Timed restore evidence and data-integrity check |

## Incident response

Treat an authorization bypass, credential exposure, cache-deserialization
finding, or unexpected cross-tenant result as a security incident. Restrict
access at the edge, preserve relevant audit and deployment logs, rotate
credentials, notify the incident owner, and follow the organization’s legal
and customer-notification process. Do not use browser error text or client
supplied identifiers as a security authority.

## Reporting

Report suspected vulnerabilities privately to the repository maintainer or the
organization’s security contact. Include affected version, reproduction steps,
and impact. Do not include production credentials or customer records in a
public issue.

## What this repository has not verified

This repository provides code, tests, and evidence templates only. TLS
issuance, external network configuration, penetration testing, backup
restoration, and organization-specific incident-response execution remain
`not_verified_locally` until an operator attaches the evidence above.
