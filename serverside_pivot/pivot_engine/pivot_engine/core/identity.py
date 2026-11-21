"""Typed, collision-free row identity (REQ-ID-001, REQ-ID-002).

Moved from ``streaming`` so the core owns identity; ``streaming`` re-exports it.
"""

from __future__ import annotations

import base64
import json
import math
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Sequence, Tuple


class ExactCDCStateError(ValueError):
    """Raised when an event cannot be represented in exact aggregate state."""


GRAND_TOTAL_CANONICAL_PATH = "[]"
GRAND_TOTAL_DISPLAY_PATH = "__grand_total__"


class CanonicalScalarCodec:
    """Versioned, type-preserving codec for supported pivot scalar values."""

    version = 1

    @classmethod
    def encode(cls, value: Any) -> str:
        encoded_value: Any
        if value is None:
            type_name, encoded_value = "none", None
        elif isinstance(value, bool):
            type_name, encoded_value = "bool", value
        elif isinstance(value, int):
            type_name, encoded_value = "int", str(value)
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ExactCDCStateError("CDC scalar floats must be finite.")
            type_name, encoded_value = "float", repr(value)
        elif isinstance(value, Decimal):
            type_name, encoded_value = "decimal", str(value)
        elif isinstance(value, str):
            type_name, encoded_value = "str", value
        elif isinstance(value, bytes):
            type_name = "bytes"
            encoded_value = base64.b64encode(value).decode("ascii")
        elif isinstance(value, datetime):
            type_name, encoded_value = "datetime", value.isoformat()
        elif isinstance(value, date):
            type_name, encoded_value = "date", value.isoformat()
        else:
            raise ExactCDCStateError(
                "CDC scalar type is not supported for exact aggregation."
            )
        return json.dumps(
            {"version": cls.version, "type": type_name, "value": encoded_value},
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _payload(cls, encoded: str) -> Dict[str, Any]:
        try:
            payload = json.loads(encoded)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExactCDCStateError("CDC scalar payload is invalid.") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("version") != cls.version
            or not isinstance(payload.get("type"), str)
            or "value" not in payload
        ):
            raise ExactCDCStateError("CDC scalar payload has an unsupported format.")
        return payload

    @classmethod
    def decode(cls, encoded: str) -> Any:
        payload = cls._payload(encoded)
        type_name = payload["type"]
        value = payload["value"]
        try:
            if type_name == "none" and value is None:
                return None
            if type_name == "bool" and isinstance(value, bool):
                return value
            if type_name == "int" and isinstance(value, str):
                return int(value)
            if type_name == "float" and isinstance(value, str):
                decoded = float(value)
                if not math.isfinite(decoded):
                    raise ValueError("non-finite float")
                return decoded
            if type_name == "decimal" and isinstance(value, str):
                return Decimal(value)
            if type_name == "str" and isinstance(value, str):
                return value
            if type_name == "bytes" and isinstance(value, str):
                return base64.b64decode(value.encode("ascii"), validate=True)
            if type_name == "date" and isinstance(value, str):
                return date.fromisoformat(value)
            if type_name == "datetime" and isinstance(value, str):
                return datetime.fromisoformat(value)
        except (ValueError, InvalidOperation, TypeError) as exc:
            raise ExactCDCStateError("CDC scalar payload value is invalid.") from exc
        raise ExactCDCStateError("CDC scalar payload type is invalid.")

    @classmethod
    def order_key(cls, encoded: str) -> tuple[Any, ...]:
        payload = cls._payload(encoded)
        type_name = payload["type"]
        value = cls.decode(encoded)
        if type_name in {"int", "float", "decimal"}:
            return ("number", Decimal(str(value)))
        if type_name == "none":
            return ("none", "")
        if type_name == "bool":
            return ("bool", int(value))
        if type_name == "bytes":
            return ("bytes", value)
        if type_name in {"date", "datetime"}:
            return ("temporal", value.isoformat())
        return (type_name, value)


class CanonicalPathCodec:
    """Collision-free typed canonical encoding for multi-level hierarchy paths."""

    @staticmethod
    def encode(values: Sequence[Any]) -> str:
        if not values:
            return GRAND_TOTAL_CANONICAL_PATH
        encoded_items = [CanonicalScalarCodec.encode(v) for v in values]
        return json.dumps(encoded_items, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def decode(canonical_path: str) -> Tuple[Any, ...]:
        if canonical_path in {GRAND_TOTAL_CANONICAL_PATH, GRAND_TOTAL_DISPLAY_PATH, ""}:
            return ()
        raw = json.loads(canonical_path)
        if not isinstance(raw, list):
            raise ValueError("Canonical path must be a JSON array.")
        return tuple(CanonicalScalarCodec.decode(item) for item in raw)

    @staticmethod
    def display_path(values: Sequence[Any]) -> str:
        """Backward-compatible display path while internal keys use `encode()`."""
        if not values:
            return GRAND_TOTAL_DISPLAY_PATH
        return "|||".join("" if v is None else str(v) for v in values)

    @staticmethod
    def prefix_paths(values: Sequence[Any]) -> List[str]:
        """Return canonical paths from depth 0 (Grand Total) through full tuple."""
        paths = [GRAND_TOTAL_CANONICAL_PATH]
        for depth in range(1, len(values) + 1):
            paths.append(CanonicalPathCodec.encode(values[:depth]))
        return paths
