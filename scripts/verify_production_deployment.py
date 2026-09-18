"""Verify the externally observable HTTP security contract of a deployment.

This verifier deliberately has no product dependency: run it from a release
runner with a public HTTPS URL. It verifies edge behaviour; it does not claim
certificate issuance, penetration testing, recovery testing, or incident
readiness on behalf of an operator.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urlunparse
from urllib.request import HTTPRedirectHandler, Request, build_opener


_REQUIRED_HEADERS = frozenset(
    {
        "strict-transport-security",
        "content-security-policy",
        "x-content-type-options",
        "x-frame-options",
        "referrer-policy",
        "permissions-policy",
    }
)
_REDIRECT_CODES = {301, 302, 307, 308}
_MIN_HSTS_MAX_AGE = 63_072_000
_SAFE_REFERRER_POLICIES = frozenset(
    {"no-referrer", "same-origin", "strict-origin", "strict-origin-when-cross-origin"}
)
_REQUIRED_CSP_DIRECTIVES = {
    "base-uri": "'self'",
    "frame-ancestors": "'none'",
    "object-src": "'none'",
}
_REQUIRED_PERMISSIONS_DIRECTIVES = {
    "geolocation": "()",
    "microphone": "()",
    "camera": "()",
}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):  # noqa: N802
        return None


def _response_for(url: str, *, follow_redirects: bool) -> tuple[int, dict[str, str], str]:
    opener = build_opener() if follow_redirects else build_opener(_NoRedirect())
    request = Request(url, headers={"User-Agent": "serverside-pivot-security-verifier/1"})
    try:
        with opener.open(request, timeout=10) as response:
            return int(response.status), dict(response.headers.items()), str(response.geturl())
    except HTTPError as error:
        return int(error.code), dict(error.headers.items()), str(error.geturl())
    except URLError as error:
        raise RuntimeError(f"Unable to reach deployment: {error.reason}") from error


def _base_url(value: str) -> str:
    parsed = urlparse(str(value or "").strip())
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("--base-url must be a credential-free https:// origin")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("--base-url must be an origin without a path, query string, or fragment")
    return urlunparse(("https", parsed.netloc, "", "", "", ""))


def _csp_directives(value: str) -> dict[str, str]:
    """Normalize semicolon-separated CSP directives."""
    parsed: dict[str, str] = {}
    for raw_directive in str(value or "").split(";"):
        directive = raw_directive.strip()
        if not directive:
            continue
        name, _, argument = directive.partition(" ")
        normalized_name = name.strip().lower()
        if normalized_name and normalized_name not in parsed:
            parsed[normalized_name] = argument.strip().lower()
    return parsed


def _hsts_error(value: str) -> str | None:
    directives: dict[str, str] = {}
    for raw_directive in str(value or "").split(";"):
        name, separator, argument = raw_directive.strip().partition("=")
        normalized_name = name.strip().lower()
        if normalized_name and normalized_name not in directives:
            directives[normalized_name] = argument.strip() if separator else ""
    raw_max_age = directives.get("max-age", "")
    try:
        max_age = int(raw_max_age)
    except ValueError:
        return "HSTS must define a numeric max-age."
    if max_age < _MIN_HSTS_MAX_AGE:
        return f"HSTS max-age must be at least {_MIN_HSTS_MAX_AGE}."
    if "includesubdomains" not in directives:
        return "HSTS must include includeSubDomains."
    if "preload" not in directives:
        return "HSTS must include preload."
    return None


def _csp_errors(value: str) -> list[str]:
    directives = _csp_directives(value)
    errors: list[str] = []
    for directive, expected in _REQUIRED_CSP_DIRECTIVES.items():
        if directives.get(directive) != expected:
            errors.append(
                f"Content-Security-Policy {directive} must be exactly {expected}."
            )
    return errors


def _permissions_policy_errors(value: str) -> list[str]:
    directives: dict[str, str] = {}
    for raw_directive in str(value or "").split(","):
        name, separator, argument = raw_directive.strip().partition("=")
        if separator and name.strip() and name.strip().lower() not in directives:
            directives[name.strip().lower()] = argument.strip().lower()
    return [
        f"Permissions-Policy {directive} must be {expected}."
        for directive, expected in _REQUIRED_PERMISSIONS_DIRECTIVES.items()
        if directives.get(directive) != expected
    ]


def _security_header_errors(headers: dict[str, str]) -> list[str]:
    errors: list[str] = []
    for name in _REQUIRED_HEADERS:
        if not headers.get(name, ""):
            errors.append(f"missing required response header: {name}")
    if errors:
        return errors

    hsts_error = _hsts_error(headers["strict-transport-security"])
    if hsts_error:
        errors.append(hsts_error)
    errors.extend(_csp_errors(headers["content-security-policy"]))
    if headers["x-content-type-options"].strip().lower() != "nosniff":
        errors.append("X-Content-Type-Options must be nosniff.")
    if headers["x-frame-options"].strip().upper() != "DENY":
        errors.append("X-Frame-Options must be DENY.")
    if headers["referrer-policy"].strip().lower() not in _SAFE_REFERRER_POLICIES:
        errors.append("Referrer-Policy must use a restrictive policy.")
    errors.extend(_permissions_policy_errors(headers["permissions-policy"]))
    return errors


def verify(base_url: str, health_path: str) -> dict[str, Any]:
    origin = _base_url(base_url)
    if not health_path.startswith("/"):
        raise ValueError("--health-path must begin with '/'")
    target = f"{origin}{health_path}"
    errors: list[str] = []
    try:
        https_status, headers, final_url = _response_for(target, follow_redirects=True)
    except RuntimeError as error:
        return {"ok": False, "target": target, "errors": [str(error)]}
    if not 200 <= https_status < 400:
        errors.append(f"HTTPS endpoint returned {https_status}")
    final = urlparse(final_url)
    configured = urlparse(origin)
    if final.scheme != "https":
        errors.append("HTTPS request did not finish on HTTPS")
    elif final.netloc.lower() != configured.netloc.lower():
        errors.append("HTTPS request did not finish on the configured HTTPS origin")
    normalized_headers = {str(key).lower(): str(value) for key, value in headers.items()}
    errors.extend(_security_header_errors(normalized_headers))

    parsed = urlparse(origin)
    http_target = urlunparse(("http", parsed.netloc, f"{parsed.path}{health_path}", "", "", ""))
    try:
        http_status, http_headers, _http_final = _response_for(http_target, follow_redirects=False)
        location = str(http_headers.get("Location") or http_headers.get("location") or "")
        location_parsed = urlparse(location)
        if http_status not in _REDIRECT_CODES or location_parsed.scheme != "https":
            errors.append("HTTP endpoint must redirect to an https:// URL")
        elif location_parsed.netloc.lower() != parsed.netloc.lower():
            errors.append("HTTP endpoint must redirect to the configured HTTPS origin")
    except RuntimeError as error:
        errors.append(f"HTTP redirect check failed: {error}")

    return {
        "ok": not errors,
        "target": target,
        "https_status": https_status,
        "headers_checked": sorted(_REQUIRED_HEADERS),
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="Public credential-free HTTPS origin")
    parser.add_argument("--health-path", default="/health", help="Health endpoint path (default: /health)")
    parser.add_argument("--output", help="Optional path for the JSON evidence document")
    args = parser.parse_args(argv)
    try:
        result = verify(args.base_url, args.health_path)
    except ValueError as error:
        parser.error(str(error))
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8", newline="\n") as output:
            output.write(encoded)
    sys.stdout.write(encoded)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
