"""Pure, authorization-aware ordering of pivot column identities.

The layout identity is separate from the current cell key: labels and viewport
positions can change without changing the saved preference. A server can apply
the same resolver to a viewport, clipboard document, or export job.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Iterable, Sequence


MAX_LAYOUT_MANIFEST_IDS = 20_000
MAX_LAYOUT_MANIFEST_BYTES = 512 * 1024


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


@dataclass(frozen=True)
class PivotColumnIdentity:
    """A value column identified by its source path and measure, not its label."""

    pivot_path: tuple[tuple[str, Any], ...]
    measure_key: str
    cell_id: str
    label: str = field(default="", compare=False)
    is_row_total: bool = False

    @property
    def group_id(self) -> str:
        if self.is_row_total:
            return "total"
        digest = hashlib.sha256(_canonical_json(self.pivot_path)).hexdigest()
        return f"g:{digest}"

    @property
    def layout_id(self) -> str:
        digest = hashlib.sha256(_canonical_json({
            "path": self.pivot_path,
            "measure": self.measure_key,
            "rowTotal": self.is_row_total,
        })).hexdigest()
        return f"{'t' if self.is_row_total else 'p'}:{digest}"


@dataclass(frozen=True)
class ResolvedColumnLayout:
    columns: tuple[PivotColumnIdentity, ...]
    revision: str


def _normalize_manifest(saved_ids: Sequence[str]) -> tuple[str, ...]:
    if len(saved_ids) > MAX_LAYOUT_MANIFEST_IDS:
        raise ValueError("layout manifest count exceeds limit")
    if len(_canonical_json(saved_ids)) > MAX_LAYOUT_MANIFEST_BYTES:
        raise ValueError("layout manifest bytes exceed limit")
    seen: set[str] = set()
    normalized: list[str] = []
    for identity in saved_ids:
        if not isinstance(identity, str) or not identity:
            raise ValueError("layout manifest contains an invalid identity")
        if identity not in seen:
            normalized.append(identity)
            seen.add(identity)
    return tuple(normalized)


def resolve_column_layout(
    default_columns: Iterable[PivotColumnIdentity],
    saved_ids: Sequence[str],
    *,
    allowed_ids: set[str] | None = None,
    structural_key: str = "",
) -> ResolvedColumnLayout:
    """Reconcile a saved leaf order with currently authorized, discovered columns.

    First occurrence of a saved group's leaf sets the group rank. Leaves remain
    contiguous within that group; unseen leaves and groups append in default
    order. Absent saved identities retain their rank in ``saved_ids`` but do not
    occupy output slots. Totals always follow ordinary pivot groups.
    """
    manifest = _normalize_manifest(saved_ids)
    available: dict[str, PivotColumnIdentity] = {}
    default_group_order: list[str] = []
    default_leaves: dict[str, list[str]] = {}
    for column in default_columns:
        identity = column.layout_id
        if allowed_ids is not None and identity not in allowed_ids:
            continue
        if identity in available:
            raise ValueError("duplicate current pivot column identity")
        available[identity] = column
        group = column.group_id
        if group not in default_leaves:
            default_group_order.append(group)
            default_leaves[group] = []
        default_leaves[group].append(identity)

    ordered_groups: list[str] = []
    ordered_leaves: dict[str, list[str]] = {}
    for identity in manifest:
        column = available.get(identity)
        if column is None:
            continue
        group = column.group_id
        if group not in ordered_leaves:
            ordered_groups.append(group)
            ordered_leaves[group] = []
        ordered_leaves[group].append(identity)
    for group in default_group_order:
        if group not in ordered_leaves:
            ordered_groups.append(group)
            ordered_leaves[group] = []
        existing = set(ordered_leaves[group])
        ordered_leaves[group].extend(
            identity for identity in default_leaves[group] if identity not in existing
        )
    ordered_groups = [group for group in ordered_groups if group != "total"] + (
        ["total"] if "total" in ordered_leaves else []
    )
    columns = tuple(
        available[identity]
        for group in ordered_groups
        for identity in ordered_leaves[group]
    )
    revision = hashlib.sha256(_canonical_json({
        "contract": "pivot-layout-v1",
        "structural": structural_key,
        "saved": manifest,
        "resolved": [column.layout_id for column in columns],
    })).hexdigest()
    return ResolvedColumnLayout(columns, revision)
