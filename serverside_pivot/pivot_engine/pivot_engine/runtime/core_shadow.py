"""Routing between the legacy engines and the semantic core, and shadow diffs.

``PIVOT_CORE`` selects the engine for requests the core supports:

* ``off``: legacy only;
* ``shadow``: legacy answers, the core runs too, and every difference is
  classified and written to ``PIVOT_CORE_SHADOW_DIR`` (default
  ``artifacts/shadow``) as JSON lines;
* ``on``: the core answers.

Requests the core cannot express (``CoreUnsupported``) are always served by
legacy; the reasons are counted so coverage is measurable.  A diff is either
attributed to an audited legacy finding (``legacy:<id>``) or ``unclassified``,
which is a core bug until proven otherwise (M2 gate: zero unclassified).
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import math
import os
import pathlib
import threading
from collections import Counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

__all__ = ["STATS", "classify", "compare_views", "core_mode", "record_shadow", "record_unsupported"]

_LOG = logging.getLogger(__name__)
_LOCK = threading.Lock()
STATS: Counter = Counter()
UNSUPPORTED: Counter = Counter()

_HOLISTIC_OR_EXTREMA = {"avg", "mean", "min", "max", "count_distinct", "distinct_count", "median", "stddev",
                        "variance", "percentile", "weighted_avg", "wavg", "weighted_mean"}


def core_mode() -> str:
    mode = os.environ.get("PIVOT_CORE", "off").strip().lower()
    return mode if mode in {"off", "shadow", "on"} else "off"


def _shadow_dir() -> pathlib.Path:
    return pathlib.Path(os.environ.get("PIVOT_CORE_SHADOW_DIR", "artifacts/shadow"))


# --------------------------------------------------------------------- normalisation


def _path(row: Mapping[str, Any]) -> Optional[Tuple[Any, ...]]:
    """Identity of a legacy or core row; ``None`` means the grand total."""
    if row.get("_isTotal") and row.get("_path") == "__grand_total__":
        return None
    return tuple(row.get(f) for f in row.get("_pathFields") or [])


def _same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) and math.isnan(a):
        return False
    try:
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    except (TypeError, ValueError):
        return a == b


def compare_views(legacy: Mapping[str, Any], core: Mapping[str, Any], measures: Sequence[str]) -> List[Dict[str, Any]]:
    """Differences between two ``run_hierarchy_view`` results (rows, order, values, totals)."""
    diffs: List[Dict[str, Any]] = []
    legacy_rows = [r for r in legacy.get("rows") or [] if _path(r) is not None]
    core_rows = [r for r in core.get("rows") or [] if _path(r) is not None]
    legacy_gts = [r for r in legacy.get("rows") or [] if _path(r) is None]
    lp, cp = [_path(r) for r in legacy_rows], [_path(r) for r in core_rows]
    if lp != cp:
        diffs.append({"kind": "rows", "legacy_only": [list(p) for p in lp if p not in cp],
                      "core_only": [list(p) for p in cp if p not in lp],
                      "order_only": sorted(map(repr, lp)) == sorted(map(repr, cp))})
    core_by_path = {_path(r): r for r in core_rows}
    for row in legacy_rows:
        other = core_by_path.get(_path(row))
        if other is None:
            continue
        bad = {m: [row.get(m), other.get(m)] for m in measures if not _same(row.get(m), other.get(m))}
        if bad:
            diffs.append({"kind": "values", "path": list(_path(row) or ()), "depth": len(_path(row) or ()),
                          "measures": bad})
    lgt, cgt = legacy.get("grand_total_row"), core.get("grand_total_row")
    if (lgt is None) != (cgt is None):
        diffs.append({"kind": "grand_total_presence", "legacy": lgt is not None, "core": cgt is not None})
    elif lgt is not None:
        bad = {m: [lgt.get(m), cgt.get(m)] for m in measures if not _same(lgt.get(m), cgt.get(m))}
        if bad:
            diffs.append({"kind": "grand_total_values", "measures": bad})
    if legacy_gts:
        diffs.append({"kind": "grand_total_in_rows", "count": len(legacy_gts)})
    if legacy.get("total_rows") is not None and legacy.get("total_rows") != core.get("total_rows"):
        diffs.append({"kind": "total_rows", "legacy": legacy.get("total_rows"), "core": core.get("total_rows")})
    return diffs


# --------------------------------------------------------------------- classification


def _has_null_dimension(rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(v is None for r in rows for v in (_path(r) or ()))


def classify(diff: Mapping[str, Any], *, spec_summary: Mapping[str, Any], core_rows: Sequence[Mapping[str, Any]],
             legacy: Mapping[str, Any]) -> str:
    """Attribute one diff to an audited legacy finding, or ``unclassified``."""
    has_null = _has_null_dimension(core_rows) or bool(spec_summary.get("data_has_null_dimension"))
    aggs = {str(a).lower() for a in spec_summary.get("aggs", ())}
    measure_filtered = bool(spec_summary.get("measure_filters"))
    kind = diff["kind"]
    if has_null and kind in {"rows", "grand_total_in_rows", "grand_total_values", "grand_total_presence",
                             "total_rows"}:
        legacy_only = diff.get("legacy_only") or []
        core_only = diff.get("core_only") or []
        if any(None in p for p in core_only) or kind != "rows" or not legacy_only:
            return "legacy:F-NULL-GT" if kind != "rows" or any(len(p) == 1 for p in core_only) else "legacy:F-NULL-LEAF"
    if measure_filtered and kind in {"values", "grand_total_values", "rows", "total_rows"}:
        return "legacy:F-TOT-MF-PARENT"
    if kind == "values" and diff.get("depth", 0) < spec_summary.get("depth", 0) and aggs & _HOLISTIC_OR_EXTREMA:
        return "legacy:F-TOT-FALLBACK"
    if kind == "rows" and spec_summary.get("has_case_variants"):
        return "legacy:F-CASE"
    if kind == "values" and spec_summary.get("has_case_insensitive_membership"):
        return "legacy:F-CASE"
    return "unclassified"


def record_shadow(spec_summary: Mapping[str, Any], legacy: Mapping[str, Any], core: Mapping[str, Any],
                  measures: Sequence[str]) -> List[Dict[str, Any]]:
    diffs = compare_views(legacy, core, measures)
    for d in diffs:
        d["class"] = classify(d, spec_summary=spec_summary, core_rows=core.get("rows") or [], legacy=legacy)
    with _LOCK:
        STATS["shadow_requests"] += 1
        STATS["shadow_identical" if not diffs else "shadow_differing"] += 1
        for d in diffs:
            STATS[d["class"]] += 1
    if diffs:
        entry = {"at": _dt.datetime.now(_dt.timezone.utc).isoformat(), "spec": dict(spec_summary), "diffs": diffs}
        try:
            out = _shadow_dir()
            out.mkdir(parents=True, exist_ok=True)
            with _LOCK, open(out / f"shadow-{_dt.date.today().isoformat()}.jsonl", "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, default=str) + "\n")
        except OSError:
            _LOG.warning("could not write the core shadow report", exc_info=True)
        if any(d["class"] == "unclassified" for d in diffs):
            _LOG.warning("core shadow: unclassified difference for %s", spec_summary.get("table"))
    return diffs


def record_unsupported(reasons: Sequence[str]) -> None:
    with _LOCK:
        STATS["unsupported_requests"] += 1
        for reason in reasons:
            UNSUPPORTED[reason] += 1


def summarize_spec(spec: Any, query_filters_have_case_insensitive_in: bool = False) -> Dict[str, Any]:
    rows = list(getattr(spec, "rows", []) or [])
    aliases = {m.alias for m in getattr(spec, "measures", []) or []}

    def fields(f: Mapping[str, Any]) -> List[str]:
        return [x for c in f.get("conditions") or [] for x in fields(c)] if "conditions" in f else [f.get("field")]

    measure_filters = [f for f in getattr(spec, "filters", []) or [] if set(fields(f)) & aliases]

    def ci_membership(f: Mapping[str, Any]) -> bool:
        if "conditions" in f:
            return any(ci_membership(c) for c in f.get("conditions") or [])
        return f.get("op") in {"in", "not in", "not_in"} and not f.get("caseSensitive", False)

    return {
        "table": getattr(spec, "table", None),
        "rows": rows,
        "depth": len(rows),
        "aggs": [m.agg for m in getattr(spec, "measures", []) or []],
        "measures": sorted(aliases),
        "measure_filters": len(measure_filters),
        "has_case_insensitive_membership": any(ci_membership(f) for f in getattr(spec, "filters", []) or []),
    }
