"""The host-neutral Pivot Runtime Protocol V1 codec.

``schemaVersion: 1`` is the only runtime protocol version.  Dash is one
transport adapter for it; HTTP, standalone React, and Streamlit use the same
envelope.  Historical Dash prop forms are normalized only by the Dash adapter
entry point below and are never accepted by the canonical HTTP transport.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Optional
from ..resource_governor import ResourceExhaustedError
from .export_jobs import ExportJobSnapshot

try:  # Keep import errors actionable for library consumers.
    from jsonschema import Draft202012Validator
except ImportError:  # pragma: no cover - dependency declared by the package
    Draft202012Validator = None  # type: ignore[assignment]


RUNTIME_PROTOCOL_NAME = "pivot-runtime"
RUNTIME_SCHEMA_VERSION = 1
RUNTIME_COMMAND_KINDS = frozenset(
    {
        "data",
        "filter_options",
        "chart",
        "detail",
        "drill",
        "update",
        "transaction",
        "export",
    }
)
_COMMAND_ALIASES = {"batch_update": "update", "cell_updates": "update"}


class RuntimeContractError(ValueError):
    """A caller-visible V1 envelope validation failure."""

    def __init__(self, message: str, *, code: str = "invalid_runtime_request") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class NormalizedRuntimeRequest:
    """Callback-ready request plus compatibility metadata."""

    kind: str
    request_id: Optional[str]
    payload: Dict[str, Any]
    trigger_prop: str
    schema_version: Optional[int]
    legacy: bool
    error: Optional[RuntimeContractError] = None

    def as_callback_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "kind": self.kind,
            "request_id": self.request_id,
            "payload": self.payload,
            "trigger_prop": self.trigger_prop,
            "schema_version": self.schema_version,
            "legacy": self.legacy,
        }
        if self.error is not None:
            payload["error"] = str(self.error)
            payload["error_code"] = self.error.code
        return payload


def _schema_validator(schema_filename: str):
    if Draft202012Validator is None:
        raise RuntimeContractError(
            "Runtime contract validation requires the jsonschema package.",
            code="runtime_contract_validator_unavailable",
        )
    schema_path = Path(__file__).with_name(schema_filename)
    with schema_path.open(encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    return Draft202012Validator(schema)


_REQUEST_VALIDATOR = None
_RESPONSE_VALIDATOR = None


def validate_runtime_request_v1(value: Any) -> Dict[str, Any]:
    """Return a deep-copy of a valid V1 envelope or raise a stable error."""
    if not isinstance(value, dict):
        raise RuntimeContractError("Runtime request must be an object.")
    global _REQUEST_VALIDATOR
    if _REQUEST_VALIDATOR is None:
        _REQUEST_VALIDATOR = _schema_validator("runtime-contract.schema.json")
    errors = sorted(
        _REQUEST_VALIDATOR.iter_errors(value), key=lambda error: list(error.path)
    )
    if errors:
        error = errors[0]
        path = ".".join(str(segment) for segment in error.path) or "request"
        raise RuntimeContractError(f"Invalid runtime request at {path}: {error.message}")
    return deepcopy(value)


def validate_runtime_response_v1(value: Any) -> Dict[str, Any]:
    """Return a deep-copy of a valid canonical V1 response envelope."""
    if not isinstance(value, dict):
        raise RuntimeContractError(
            "Runtime response must be an object.", code="invalid_runtime_response"
        )
    global _RESPONSE_VALIDATOR
    if _RESPONSE_VALIDATOR is None:
        _RESPONSE_VALIDATOR = _schema_validator("runtime-response.schema.json")
    errors = sorted(
        _RESPONSE_VALIDATOR.iter_errors(value), key=lambda error: list(error.path)
    )
    if errors:
        error = errors[0]
        path = ".".join(str(segment) for segment in error.path) or "response"
        raise RuntimeContractError(
            f"Invalid runtime response at {path}: {error.message}",
            code="invalid_runtime_response",
        )
    return deepcopy(value)


def _normalize_id(value: Any) -> str:
    return str(value or "pivot-grid")


def _trigger_prop(pivot_id: Any, kind: str) -> str:
    normalized_id = _normalize_id(pivot_id)
    return {
        "filter_options": f"{normalized_id}.filterRequest",
        "chart": f"{normalized_id}.chartRequest",
        "drill": f"{normalized_id}.drillThrough",
        "detail": f"{normalized_id}.detailRequest",
        "update": f"{normalized_id}.cellUpdates",
        "export": f"{normalized_id}.runtimeRequest",
        "transaction": f"{normalized_id}.runtimeRequest",
    }.get(kind, f"{normalized_id}.viewport")


def _coerce_kind(value: Any) -> str:
    normalized = str(value or "data").strip().lower() or "data"
    return _COMMAND_ALIASES.get(normalized, normalized)


def _request_id(value: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    for key in ("requestId", "request_id", "request_signature", "windowSeq", "window_seq", "version"):
        candidate = value.get(key)
        if candidate is None:
            candidate = payload.get(key)
        if candidate is not None:
            return str(candidate)
    return None


def _legacy_payload(value: Dict[str, Any]) -> Dict[str, Any]:
    payload = value.get("payload")
    if isinstance(payload, dict):
        return deepcopy(payload)
    envelope_keys = {
        "schemaVersion", "kind", "requestId", "request_id", "table",
        "sessionId", "session_id", "clientInstance", "client_instance",
        "stateEpoch", "state_epoch", "windowSeq", "window_seq",
        "abortGeneration", "abort_generation", "cacheKey", "cache_key", "state",
    }
    return {key: deepcopy(item) for key, item in value.items() if key not in envelope_keys}


def _merge_v1_metadata_into_payload(envelope: Dict[str, Any]) -> Dict[str, Any]:
    payload = deepcopy(envelope["payload"])
    state = envelope.get("state")
    if isinstance(state, dict):
        # Existing services consume this spelling; the codec keeps it out of
        # every other runtime layer.
        payload["state_override"] = deepcopy(state)
    metadata = {
        "table": envelope.get("table"),
        "session_id": envelope.get("sessionId"),
        "client_instance": envelope.get("clientInstance"),
        "state_epoch": envelope.get("stateEpoch"),
        "window_seq": envelope.get("windowSeq"),
        "abort_generation": envelope.get("abortGeneration"),
        "cache_key": envelope.get("cacheKey"),
        "requestId": envelope.get("requestId"),
    }
    for key, item in metadata.items():
        if item is not None:
            payload[key] = item
    return payload


def normalize_runtime_request(*, pivot_id: Any, runtime_request: Any) -> Dict[str, Any]:
    """Normalize V1 and historical transport forms for Dash callbacks."""
    if not isinstance(runtime_request, dict):
        return NormalizedRuntimeRequest(
            kind="data",
            request_id=None,
            payload={},
            trigger_prop=_trigger_prop(pivot_id, "data"),
            schema_version=None,
            legacy=True,
        ).as_callback_dict()

    if runtime_request.get("schemaVersion") == RUNTIME_SCHEMA_VERSION:
        try:
            envelope = validate_runtime_request_v1(runtime_request)
            kind = str(envelope["kind"])
            return NormalizedRuntimeRequest(
                kind=kind,
                request_id=str(envelope["requestId"]),
                payload=_merge_v1_metadata_into_payload(envelope),
                trigger_prop=_trigger_prop(pivot_id, kind),
                schema_version=RUNTIME_SCHEMA_VERSION,
                legacy=False,
            ).as_callback_dict()
        except RuntimeContractError as error:
            return NormalizedRuntimeRequest(
                kind="data",
                request_id=None,
                payload={},
                trigger_prop=_trigger_prop(pivot_id, "data"),
                schema_version=RUNTIME_SCHEMA_VERSION,
                legacy=False,
                error=error,
            ).as_callback_dict()

    if "schemaVersion" in runtime_request:
        error = RuntimeContractError(
            f"Unsupported runtime schema version {runtime_request.get('schemaVersion')!r}.",
            code="unsupported_runtime_schema_version",
        )
        return NormalizedRuntimeRequest(
            kind="data",
            request_id=None,
            payload={},
            trigger_prop=_trigger_prop(pivot_id, "data"),
            schema_version=None,
            legacy=False,
            error=error,
        ).as_callback_dict()

    payload = _legacy_payload(runtime_request)
    kind = _coerce_kind(runtime_request.get("kind"))
    return NormalizedRuntimeRequest(
        kind=kind,
        request_id=_request_id(runtime_request, payload),
        payload=payload,
        trigger_prop=_trigger_prop(pivot_id, kind),
        schema_version=None,
        legacy=True,
    ).as_callback_dict()


def build_runtime_response(
    *,
    kind: str,
    request_id: Optional[str],
    status: str,
    payload: Dict[str, Any],
    table: Optional[str] = None,
    session_id: Optional[str] = None,
    client_instance: Optional[str] = None,
    state_epoch: Optional[int] = None,
    window_seq: Optional[int] = None,
    message: Optional[str] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a canonical, host-neutral V1 response envelope.

    Legacy snake-case aliases belong exclusively to the Dash adapter.  Keeping
    them out of this builder ensures HTTP and future hosts observe one exact
    protocol shape.
    """
    response: Dict[str, Any] = {
        "schemaVersion": RUNTIME_SCHEMA_VERSION,
        "kind": kind,
        "status": status,
        "payload": payload if isinstance(payload, dict) else {},
    }
    metadata = {
        "requestId": request_id,
        "table": table,
        "sessionId": session_id,
        "clientInstance": client_instance,
        "stateEpoch": state_epoch,
        "windowSeq": window_seq,
    }
    response.update({key: value for key, value in metadata.items() if value is not None})
    if message:
        response["message"] = message
    if isinstance(profile, dict) and profile:
        response["profile"] = profile
    return response


def build_export_job_response(
    snapshot: ExportJobSnapshot, *, request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Encode only public observations; identity/accounting stay server-side."""
    return build_runtime_response(
        kind="export", request_id=request_id, status="export_job",
        payload=snapshot.to_payload(),
    )


def build_resource_exhausted_response(
    error: ResourceExhaustedError,
    *,
    kind: str,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Expose only stable, retryable admission metadata, never internal reasons."""
    return build_runtime_response(
        kind=kind,
        request_id=request_id,
        status="error",
        payload={
            "code": error.code,
            "resourceKind": error.resource_kind.value,
            "retryAfterMs": min(60_000, max(1, int(error.retry_after_ms))),
        },
    )
