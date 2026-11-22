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


GRAND_TOTAL = ("<grand total>",)


def _is_grand_total(row: Mapping[str, Any]) -> bool:
    return bool(row.get("_isTotal")) and (row.get("_path") == "__grand_total__" or row.get("_id") == "Grand Total")


def _path(row: Mapping[str, Any], rows: Sequence[str]) -> Tuple[Any, ...]:
    """Typed identity of a row; legacy fast paths omit ``_pathFields``, so fall back to depth."""
    if _is_grand_total(row):
        return GRAND_TOTAL
    fields = row.get("_pathFields")
    if not fields:
        fields = list(rows[: int(row.get("depth") or 0) + 1])
    return tuple(row.get(f) for f in fields)


def _value_keys(a: Mapping[str, Any], b: Mapping[str, Any], rows: Sequence[str]) -> List[str]:
    return sorted(k for k in set(a) | set(b) if not k.startswith("_") and k != "depth" and k not in rows)


def _same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) and math.isnan(a):
        return False
    try:
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    except (TypeError, ValueError):
        return bool(a == b)


def _grand_total_of(view: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    gt = view.get("grand_total_row")
    if gt:
        return gt
    return next((r for r in view.get("rows") or [] if _is_grand_total(r)), None)


def compare_views(legacy: Mapping[str, Any], core: Mapping[str, Any], rows: Sequence[str], *,
                  grand_total_requested: bool = True) -> List[Dict[str, Any]]:
    """Differences between two ``run_hierarchy_view`` results.

    Nodes are compared by identity, order and every value column.  The grand
    total is compared by value wherever both sides carry it (legacy fast paths
    return it outside ``rows`` and sometimes unrequested; the response layer
    places it), and its absence counts only when it was requested.
    """
    diffs: List[Dict[str, Any]] = []
    legacy_rows = [r for r in legacy.get("rows") or [] if not _is_grand_total(r)]
    core_rows = [r for r in core.get("rows") or [] if not _is_grand_total(r)]
    lp, cp = [_path(r, rows) for r in legacy_rows], [_path(r, rows) for r in core_rows]
    if lp != cp:
        diffs.append({"kind": "rows", "legacy_only": [list(p) for p in lp if p not in cp],
                      "core_only": [list(p) for p in cp if p not in lp],
                      "order_only": sorted(map(repr, lp)) == sorted(map(repr, cp))})
    core_by_path = {p: r for p, r in zip(cp, core_rows, strict=True)}
    for path, row in zip(lp, legacy_rows, strict=True):
        other = core_by_path.get(path)
        if other is None:
            continue
        bad = {k: [row.get(k), other.get(k)] for k in _value_keys(row, other, rows)
               if not _same(row.get(k), other.get(k))}
        if bad:
            diffs.append({"kind": "values", "path": list(path), "depth": len(path), "measures": bad})
    lgt, cgt = _grand_total_of(legacy), _grand_total_of(core)
    if lgt is not None and cgt is not None:
        bad = {k: [lgt.get(k), cgt.get(k)] for k in _value_keys(lgt, cgt, rows) if not _same(lgt.get(k), cgt.get(k))}
        if bad:
            diffs.append({"kind": "grand_total_values", "measures": bad})
    elif grand_total_requested and (lgt is None) != (cgt is None):
        diffs.append({"kind": "grand_total_presence", "legacy": lgt is not None, "core": cgt is not None})
    if legacy.get("total_rows") is not None and legacy.get("total_rows") != core.get("total_rows"):
        diffs.append({"kind": "total_rows", "legacy": legacy.get("total_rows"), "core": core.get("total_rows")})
    return diffs


# --------------------------------------------------------------------- classification


def _has_null_dimension(rows: Sequence[Mapping[str, Any]], dims: Sequence[str]) -> bool:
    return any(v is None for r in rows if not _is_grand_total(r) for v in _path(r, dims))


def classify(diff: Mapping[str, Any], *, spec_summary: Mapping[str, Any], core_rows: Sequence[Mapping[str, Any]],
             legacy: Mapping[str, Any]) -> str:
    """Attribute one diff to an audited legacy finding, or ``unclassified``."""
    has_null = _has_null_dimension(core_rows, spec_summary.get("rows") or []) or bool(
        spec_summary.get("data_has_null_dimension"))
    aggs = {str(a).lower() for a in spec_summary.get("aggs", ())}
    measure_filtered = bool(spec_summary.get("measure_filters"))
    kind = diff["kind"]
    if has_null and kind in {"rows", "values", "grand_total_values", "grand_total_presence", "total_rows"}:
        legacy_only = diff.get("legacy_only") or []
        core_only = diff.get("core_only") or []
        if any(None in p for p in core_only) or kind != "rows" or not legacy_only:
            return "legacy:F-NULL-GT" if kind != "rows" or any(len(p) == 1 for p in core_only) else "legacy:F-NULL-LEAF"
    if measure_filtered and kind in {"values", "grand_total_values", "rows", "total_rows"}:
        return "legacy:F-TOT-MF-PARENT"
    windows = spec_summary.get("windows") or {}
    if kind in {"values", "grand_total_values"} and set(diff.get("measures") or ()) <= set(windows):
        funcs = {windows[k] for k in diff.get("measures") or ()}
        if funcs <= {"rank", "dense_rank"}:
            return "legacy:F-RANK"
        if funcs <= {"moving_avg", "running_avg"}:
            return "legacy:F-MOVING-AVG"
    if kind == "values" and diff.get("depth", 0) < spec_summary.get("depth", 0) and aggs & _HOLISTIC_OR_EXTREMA:
        return "legacy:F-TOT-FALLBACK"
    if kind == "rows" and spec_summary.get("has_case_variants"):
        return "legacy:F-CASE"
    if kind == "values" and spec_summary.get("has_case_insensitive_membership"):
        return "legacy:F-CASE"
    return "unclassified"


def capture_enabled() -> bool:
    """Opt-in: shadow entries then carry the scoped dataset (local referee use only)."""
    return os.environ.get("PIVOT_CORE_SHADOW_CAPTURE", "").strip() == "1"


def record_shadow(spec_summary: Mapping[str, Any], legacy: Mapping[str, Any],
                  core: Mapping[str, Any], capture: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
    diffs = compare_views(legacy, core, spec_summary.get("rows") or [],
                          grand_total_requested=bool(spec_summary.get("include_grand_total", True)))
    for d in diffs:
        d["class"] = classify(d, spec_summary=spec_summary, core_rows=core.get("rows") or [], legacy=legacy)
    _mark_skew(spec_summary, diffs)
    with _LOCK:
        STATS["shadow_requests"] += 1
        STATS["shadow_identical" if not diffs else "shadow_differing"] += 1
        for d in diffs:
            STATS[d["class"]] += 1
    if diffs:
        entry = {"at": _dt.datetime.now(_dt.timezone.utc).isoformat(), "spec": dict(spec_summary), "diffs": diffs}
        if capture:
            entry["capture"] = dict(capture)
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


def _canonical_record(rec: Mapping[str, Any]) -> str:
    def norm(v: Any) -> Any:
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, float) and v.is_integer():
            return int(v)
        return v

    return json.dumps({k: norm(v) for k, v in sorted(rec.items())}, default=str, sort_keys=True)


def compare_drill(legacy: Mapping[str, Any], core: Mapping[str, Any], *, ordered: bool) -> List[Dict[str, Any]]:
    """Drill-through differences: total and the page's records (as a multiset unless ordered)."""
    diffs: List[Dict[str, Any]] = []
    if legacy.get("total_rows") != core.get("total_rows"):
        diffs.append({"kind": "drill_total", "legacy": legacy.get("total_rows"), "core": core.get("total_rows")})
    lr = [_canonical_record(r) for r in legacy.get("rows") or []]
    cr = [_canonical_record(r) for r in core.get("rows") or []]
    if (lr != cr) if ordered else (sorted(lr) != sorted(cr)):
        diffs.append({"kind": "drill_rows", "legacy_count": len(lr), "core_count": len(cr),
                      "order_only": sorted(lr) == sorted(cr)})
    return diffs


def record_drill_shadow(summary: Mapping[str, Any], legacy: Mapping[str, Any],
                        core: Mapping[str, Any]) -> List[Dict[str, Any]]:
    diffs = compare_drill(legacy, core, ordered=bool(summary.get("sorted")))
    for d in diffs:
        # Unsorted legacy pages have no defined order, so a page can hold other records (F-DRILL-ORDER).
        unordered_page = d["kind"] == "drill_rows" and not summary.get("sorted") and not any(
            x["kind"] == "drill_total" for x in diffs)
        d["class"] = ("legacy:F-DRILL-FIELDS" if summary.get("row_fields_ignored")
                      else "legacy:F-DRILL-ORDER" if unordered_page else "unclassified")
    _record("drill", summary, diffs)
    return diffs


def _mark_skew(summary: Mapping[str, Any], diffs: List[Dict[str, Any]]) -> None:
    """A write committed between the two reads: the pair is not comparable (never a verdict)."""
    if summary.get("concurrent_write"):
        for d in diffs:
            d["class"] = "skew:concurrent-write"


def _record(kind: str, summary: Mapping[str, Any], diffs: List[Dict[str, Any]]) -> None:
    _mark_skew(summary, diffs)
    with _LOCK:
        STATS[f"{kind}_shadow_requests"] += 1
        STATS[f"{kind}_shadow_identical" if not diffs else f"{kind}_shadow_differing"] += 1
        for d in diffs:
            STATS[d["class"]] += 1
    if not diffs:
        return
    entry = {"at": _dt.datetime.now(_dt.timezone.utc).isoformat(), "request": kind, "spec": dict(summary),
             "diffs": diffs}
    try:
        out = _shadow_dir()
        out.mkdir(parents=True, exist_ok=True)
        with _LOCK, open(out / f"shadow-{_dt.date.today().isoformat()}.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, default=str) + "\n")
    except OSError:
        _LOG.warning("could not write the core shadow report", exc_info=True)
    if any(d["class"] == "unclassified" for d in diffs):
        _LOG.warning("core shadow: unclassified %s difference for %s", kind, summary.get("table"))


def record_unique_shadow(summary: Mapping[str, Any], legacy: Tuple[List[Any], int],
                         core: Tuple[List[Any], int]) -> List[Dict[str, Any]]:
    def norm(values: List[Any]) -> List[Any]:
        return [None if isinstance(v, float) and math.isnan(v) else v for v in values]

    diffs: List[Dict[str, Any]] = []
    if legacy[1] != core[1]:
        diffs.append({"kind": "unique_total", "legacy": legacy[1], "core": core[1]})
    lv, cv = norm(list(legacy[0])), norm(list(core[0]))
    if lv != cv:
        same_set = sorted(map(repr, lv)) == sorted(map(repr, cv))
        diffs.append({"kind": "unique_values", "legacy": lv[:20], "core": cv[:20], "order_only": same_set})
    for d in diffs:
        # Legacy leaves NULL placement to the backend and pandas turns NULL into NaN (F-NULL-NAN).
        nan = any(isinstance(v, float) and math.isnan(v) for v in legacy[0])
        d["class"] = "legacy:F-NULL-NAN" if nan else "unclassified"
    _record("unique", summary, diffs)
    return diffs


def record_served(kind: str) -> None:
    with _LOCK:
        STATS[f"core_served_{kind}"] += 1


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
        "windows": {m.alias: str(m.window_func).lower() for m in getattr(spec, "measures", []) or []
                    if getattr(m, "window_func", None)},
        "has_case_insensitive_membership": any(ci_membership(f) for f in getattr(spec, "filters", []) or []),
    }
