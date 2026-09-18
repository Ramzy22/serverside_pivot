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


_REQUIRED_HEADERS = {
    "strict-transport-security": "max-age",
    "content-security-policy": "frame-ancestors",
    "x-content-type-options": "nosniff",
    "referrer-policy": None,
}
_REDIRECT_CODES = {301, 302, 307, 308}


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
    if parsed.query or parsed.fragment:
        raise ValueError("--base-url must not include a query string or fragment")
    return urlunparse(("https", parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


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
    if urlparse(final_url).scheme != "https":
        errors.append("HTTPS request did not finish on HTTPS")
    normalized_headers = {str(key).lower(): str(value) for key, value in headers.items()}
    for name, required_value in _REQUIRED_HEADERS.items():
        value = normalized_headers.get(name, "")
        if not value:
            errors.append(f"missing required response header: {name}")
        elif required_value and required_value.lower() not in value.lower():
            errors.append(f"response header {name} lacks required value {required_value!r}")

    parsed = urlparse(origin)
    http_target = urlunparse(("http", parsed.netloc, f"{parsed.path}{health_path}", "", "", ""))
    try:
        http_status, http_headers, _http_final = _response_for(http_target, follow_redirects=False)
        location = str(http_headers.get("Location") or http_headers.get("location") or "")
        if http_status not in _REDIRECT_CODES or not location.startswith("https://"):
            errors.append("HTTP endpoint must redirect to an https:// URL")
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
