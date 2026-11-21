"""Exact sufficient-statistics measure engine, canonical path codec, and diff math.

This module owns Component 3A of the streaming and analytical diff architecture:
- `CanonicalPathCodec`: typed canonical JSON-array row identities (immune to
  embedded `|||`, NULL vs "null", and int vs str collisions)
- `CanonicalDecimalWire`: exact authoritative Decimal wire representation
  (`{"__decimal__": "...", "scale": N}`) with zero float coercion for decimals
- `SufficientStatsProjector`: single-pass grouped Ibis projections for
  `__ss_row_count`, `sum`, `count`, `avg`, valid-masked `wavg` (both numerator
  AND denominator gated on `x IS NOT NULL AND w IS NOT NULL`), and `min`/`max`
- `ExactMeasureEngine`: sufficient-statistics transitions, `CrossNodeDependencyGraph`
  for `% of Grand Total` / `% of Row` invalidation, and exact extensive vs.
  intensive removed-group variance rules
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import json
import math
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from enum import StrEnum
except ImportError:  # Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        __str__ = str.__str__
        __format__ = str.__format__

import ibis
from ibis.expr.api import Table as IbisTable

from ..planner.expression_parser import SafeExpressionParser
from ..types.pivot_spec import Measure
from .exact_state import CanonicalScalarCodec

from ..core.identity import GRAND_TOTAL_CANONICAL_PATH, GRAND_TOTAL_DISPLAY_PATH  # noqa: E402
ROW_TOTAL_COL_KEY = "__RowTotal__"

_BRACKET_REF_RE = re.compile(r"\[([^\]]+)\]")
_EXTENSIVE_AGGS = frozenset({"sum", "count"})
_ALGEBRAIC_AGGS = frozenset({"sum", "count", "avg", "weighted_avg", "wavg", "weighted_mean"})
_EXTREMA_AGGS = frozenset({"min", "max"})
_WAVG_AGGS = frozenset({"weighted_avg", "wavg", "weighted_mean"})
_FORMULA_AGGS = frozenset({"formula"})
_HOLISTIC_AGGS = frozenset(
    {"count_distinct", "distinct_count", "median", "percentile", "stddev", "variance", "string_agg", "array_agg", "first", "last"}
)


class MeasureAlgebraClass(StrEnum):
    ADDITIVE_ALGEBRAIC = "additive_algebraic"
    EXTREMA_MONOTONIC = "extrema_monotonic"
    POST_AGG_FORMULA = "post_agg_formula"
    HOLISTIC_SQL_ONLY = "holistic_sql_only"


class DependencyScope(StrEnum):
    SCOPE_SELF = "scope_self"
    SCOPE_ROW_TOTAL = "scope_row_total"
    SCOPE_GRAND_TOTAL = "scope_grand_total"
    SCOPE_GLOBAL_RANK = "scope_global_rank"


from ..core.identity import CanonicalPathCodec  # noqa: E402  (moved to the core)


def normalize_numeric(value: Any) -> Any:
    """Normalize NaN -> None and -0.0 -> 0.0 while preserving exact Decimal/int."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            return None
        return Decimal(0) if value == 0 else value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return 0.0 if value == 0.0 else value
    return value


def _decimal_scale(value: Decimal) -> int:
    exp = value.as_tuple().exponent
    if isinstance(exp, int) and exp < 0:
        return abs(exp)
    return 0


def encode_wire_value(value: Any) -> Any:
    """Encode scalar for JSON wire transport preserving authoritative Decimal exactness.

    Decimals serialize as `{"__decimal__": "<exact string>", "scale": <int>}`.
    No float is emitted as an authoritative decimal value.
    """
    normalized = normalize_numeric(value)
    if normalized is None:
        return None
    if isinstance(normalized, Decimal):
        return {
            "__decimal__": format(normalized, "f"),
            "scale": _decimal_scale(normalized),
        }
    return normalized


def decode_wire_value(value: Any) -> Any:
    """Decode wire value back into exact Python Decimal / int / float / str / None."""
    if isinstance(value, Mapping) and "__decimal__" in value:
        return Decimal(str(value["__decimal__"]))
    return normalize_numeric(value)


def values_equal(left: Any, right: Any, *, float_tol: float = 1e-12) -> bool:
    """Exact equality for None/Decimal/int/str and tolerance-aware check for floats."""
    a = normalize_numeric(left)
    b = normalize_numeric(right)
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, Decimal) or isinstance(b, Decimal):
        try:
            da = a if isinstance(a, Decimal) else Decimal(str(a))
            db = b if isinstance(b, Decimal) else Decimal(str(b))
            return da == db
        except (InvalidOperation, ValueError):
            return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
        return math.isclose(float(a), float(b), rel_tol=float_tol, abs_tol=float_tol)
    return a == b


def _add_exact(a: Any, b: Any) -> Any:
    if isinstance(a, Decimal) or isinstance(b, Decimal):
        da = a if isinstance(a, Decimal) else Decimal(str(a))
        db = b if isinstance(b, Decimal) else Decimal(str(b))
        return da + db
    return a + b


def _sub_exact(a: Any, b: Any) -> Any:
    if isinstance(a, Decimal) or isinstance(b, Decimal):
        da = a if isinstance(a, Decimal) else Decimal(str(a))
        db = b if isinstance(b, Decimal) else Decimal(str(b))
        return da - db
    return a - b


def _mul_exact(a: Any, b: Any) -> Any:
    if isinstance(a, Decimal) or isinstance(b, Decimal):
        da = a if isinstance(a, Decimal) else Decimal(str(a))
        db = b if isinstance(b, Decimal) else Decimal(str(b))
        return da * db
    return a * b


def _div_exact(a: Any, b: Any) -> Any:
    if isinstance(a, Decimal) or isinstance(b, Decimal):
        da = a if isinstance(a, Decimal) else Decimal(str(a))
        db = b if isinstance(b, Decimal) else Decimal(str(b))
        if db == 0:
            return None
        return da / db
    if b == 0:
        return None
    return a / b


def _is_zero(value: Any) -> bool:
    v = normalize_numeric(value)
    if v is None or isinstance(v, bool):
        return False
    if isinstance(v, Decimal):
        return v == 0
    if isinstance(v, (int, float)):
        return v == 0
    return False


def _zero_like(reference: Any) -> Any:
    if isinstance(reference, Decimal):
        return Decimal(0)
    if isinstance(reference, int) and not isinstance(reference, bool):
        return 0
    return 0.0


@dataclass(frozen=True)
class MeasureDiffResult:
    """Authoritative variance tuple for one cell under measure-specific diff rules."""

    current: Any
    baseline: Any
    delta: Any
    pct_delta: Optional[float]
    delta_unit: str = "abs"  # "abs" | "pp"
    changed: bool = False

    def to_wire_dict(self) -> Dict[str, Any]:
        return {
            "current": encode_wire_value(self.current),
            "baseline": encode_wire_value(self.baseline),
            "delta": encode_wire_value(self.delta),
            "pctDelta": self.pct_delta,
            "deltaUnit": self.delta_unit,
            "changed": self.changed,
        }


def compute_measure_diff(
    current: Any,
    baseline: Any,
    *,
    agg: str,
    diff_status: str = "modified",
    value_format: Optional[str] = None,
) -> MeasureDiffResult:
    """Compute exact variance obeying extensive vs. intensive removed-group rules.

    Contract:
    - Removed group (`diff_status == "removed"`):
      - SUM / COUNT: current=0, delta=-baseline, pctDelta=-1.0 (if baseline != 0 else None)
      - AVG / WAVG / MIN / MAX / Formula: current=None, delta=None, pctDelta=None
    - Added group (`diff_status == "added"`):
      - SUM / COUNT: baseline=0 (display None), delta=current, pctDelta=None
      - AVG / WAVG / MIN / MAX / Formula: baseline=None, delta=None, pctDelta=None
    - 0 -> 0: delta=0, pctDelta=None (undefined denominator; never fake 0.0%)
    - 0 -> X (X != 0): delta=X, pctDelta=None
    - Percent-formatted measure: delta_unit="pp"
    """
    norm_agg = str(agg or "sum").strip().lower()
    is_extensive = norm_agg in _EXTENSIVE_AGGS
    delta_unit = "pp" if str(value_format or "").strip().lower() == "percent" else "abs"
    cur = normalize_numeric(current)
    base = normalize_numeric(baseline)

    if diff_status == "removed":
        if is_extensive:
            effective_cur = _zero_like(base) if base is not None else 0
            if base is None:
                return MeasureDiffResult(
                    current=effective_cur,
                    baseline=None,
                    delta=None,
                    pct_delta=None,
                    delta_unit=delta_unit,
                    changed=False,
                )
            delta = -base
            pct = -1.0 if not _is_zero(base) else None
            return MeasureDiffResult(
                current=effective_cur,
                baseline=base,
                delta=delta,
                pct_delta=pct,
                delta_unit=delta_unit,
                changed=not _is_zero(base),
            )
        return MeasureDiffResult(
            current=None,
            baseline=base,
            delta=None,
            pct_delta=None,
            delta_unit=delta_unit,
            changed=base is not None,
        )

    if diff_status == "added":
        if is_extensive:
            return MeasureDiffResult(
                current=cur,
                baseline=None,
                delta=cur,
                pct_delta=None,
                delta_unit=delta_unit,
                changed=cur is not None and not _is_zero(cur),
            )
        return MeasureDiffResult(
            current=cur,
            baseline=None,
            delta=None,
            pct_delta=None,
            delta_unit=delta_unit,
            changed=cur is not None,
        )

    if cur is None or base is None:
        return MeasureDiffResult(
            current=cur,
            baseline=base,
            delta=None,
            pct_delta=None,
            delta_unit=delta_unit,
            changed=not values_equal(cur, base),
        )

    if not isinstance(cur, (int, float, Decimal)) or not isinstance(base, (int, float, Decimal)):
        return MeasureDiffResult(
            current=cur,
            baseline=base,
            delta=None,
            pct_delta=None,
            delta_unit=delta_unit,
            changed=not values_equal(cur, base),
        )

    delta = _sub_exact(cur, base)
    if _is_zero(base):
        # 0 -> 0 and 0 -> X both have undefined relative denominator.
        pct_delta = None
    else:
        pct_raw = _div_exact(delta, abs(base))
        pct_delta = None if pct_raw is None else float(pct_raw)

    return MeasureDiffResult(
        current=cur,
        baseline=base,
        delta=delta,
        pct_delta=pct_delta,
        delta_unit=delta_unit,
        changed=not values_equal(cur, base),
    )


def classify_measure(measure: Measure) -> Tuple[MeasureAlgebraClass, DependencyScope]:
    """Classify a Measure into its algebraic class and cross-node dependency scope."""
    agg = str(getattr(measure, "agg", None) or "sum").strip().lower()
    window_fn = str(getattr(measure, "window_func", None) or "").strip().lower()
    formula_expr = getattr(measure, "formula", None) or getattr(measure, "expression", None)

    scope = DependencyScope.SCOPE_SELF
    if window_fn in {"percent_of_grand_total", "percent_of_col"}:
        scope = DependencyScope.SCOPE_GRAND_TOTAL
    elif window_fn == "percent_of_row":
        scope = DependencyScope.SCOPE_ROW_TOTAL
    elif window_fn in {"rank", "dense_rank", "running_sum", "cumulative_sum"}:
        scope = DependencyScope.SCOPE_GLOBAL_RANK

    if agg in _FORMULA_AGGS or formula_expr:
        return MeasureAlgebraClass.POST_AGG_FORMULA, scope
    if scope == DependencyScope.SCOPE_GLOBAL_RANK:
        return MeasureAlgebraClass.HOLISTIC_SQL_ONLY, scope
    if agg in _ALGEBRAIC_AGGS:
        return MeasureAlgebraClass.ADDITIVE_ALGEBRAIC, scope
    if agg in _EXTREMA_AGGS:
        return MeasureAlgebraClass.EXTREMA_MONOTONIC, scope
    return MeasureAlgebraClass.HOLISTIC_SQL_ONLY, scope


class SufficientStatsProjector:
    """Projects single-pass grouped SQL columns required to initialize exact state."""

    ROW_COUNT_COL = "__ss_row_count"

    @staticmethod
    def nn_col(alias: str) -> str:
        return f"__ss_nn__{alias}"

    @staticmethod
    def sum_col(alias: str) -> str:
        return f"__ss_sum__{alias}"

    @staticmethod
    def wsum_col(alias: str) -> str:
        return f"__ss_wsum__{alias}"

    @staticmethod
    def weight_col(alias: str) -> str:
        return f"__ss_weight__{alias}"

    @classmethod
    def build_projections(
        cls,
        table: IbisTable,
        measures: Sequence[Measure],
        *,
        expression_builder: Any = None,
    ) -> List[Any]:
        """Return single-pass Ibis aggregate expressions including sufficient stats."""
        projections: List[Any] = [table.count().name(cls.ROW_COUNT_COL)]
        for measure in measures:
            alias = measure.alias
            agg = str(measure.agg or "sum").strip().lower()
            if agg in _FORMULA_AGGS:
                continue
            col = table[measure.field] if measure.field and measure.field in table.columns else None
            if measure.filter_condition and col is not None and expression_builder is not None:
                f_expr = expression_builder.build_filter_expression(table, [measure.filter_condition])
                if f_expr is not None:
                    col = col.where(f_expr)

            if agg == "count":
                expr = col.count() if col is not None else table.count()
                projections.append(expr.name(alias))
            elif agg == "sum":
                assert col is not None
                projections.append(col.sum().name(alias))
                projections.append(col.count().name(cls.nn_col(alias)))
            elif agg == "avg":
                assert col is not None
                projections.append(col.mean().name(alias))
                projections.append(col.sum().name(cls.sum_col(alias)))
                projections.append(col.count().name(cls.nn_col(alias)))
            elif agg in _WAVG_AGGS:
                assert col is not None and measure.weighted_field
                w_col = table[measure.weighted_field]
                if measure.filter_condition and expression_builder is not None:
                    f_expr = expression_builder.build_filter_expression(table, [measure.filter_condition])
                    if f_expr is not None:
                        w_col = w_col.where(f_expr)
                # Correction 1A: BOTH numerator AND denominator MUST share valid_mask!
                valid_mask = col.notnull() & w_col.notnull()
                prod_expr = col * w_col
                col_dtype = col.type()
                w_dtype = w_col.type()
                if (hasattr(col_dtype, "is_decimal") and col_dtype.is_decimal()) or (
                    hasattr(w_dtype, "is_decimal") and w_dtype.is_decimal()
                ):
                    s1 = int(getattr(col_dtype, "scale", 0) or 0)
                    s2 = int(getattr(w_dtype, "scale", 0) or 0)
                    target_scale = min(18, s1 + s2)
                    prod_expr = prod_expr.cast(ibis.dtype(f"decimal(38, {target_scale})"))
                masked_xw = ibis.ifelse(valid_mask, prod_expr, ibis.null())
                masked_w = ibis.ifelse(valid_mask, w_col, ibis.null())
                masked_nn = ibis.ifelse(valid_mask, ibis.literal(1), ibis.literal(0))
                wsum_expr = masked_xw.sum()
                weight_expr = masked_w.sum()
                projections.append((wsum_expr / weight_expr.nullif(0)).name(alias))
                projections.append(wsum_expr.name(cls.wsum_col(alias)))
                projections.append(weight_expr.name(cls.weight_col(alias)))
                projections.append(masked_nn.sum().name(cls.nn_col(alias)))
            elif agg == "min":
                # Correction 2.7: Single-pass grouped SQL (no nested aggregate).
                assert col is not None
                projections.append(col.min().name(alias))
                projections.append(col.count().name(cls.nn_col(alias)))
            elif agg == "max":
                assert col is not None
                projections.append(col.max().name(alias))
                projections.append(col.count().name(cls.nn_col(alias)))
            else:
                if expression_builder is not None:
                    projections.append(expression_builder.build_measure_aggregation(table, measure))
        return projections


@dataclass
class SufficientStatCell:
    """Exact mutable sufficient-statistics state for one (group, col_key, measure)."""

    agg: str
    total: Any = 0
    nn_count: int = 0
    weight_sum: Any = 0
    extremum: Any = None
    initialized: bool = True

    def clone(self) -> "SufficientStatCell":
        return SufficientStatCell(
            agg=self.agg,
            total=self.total,
            nn_count=self.nn_count,
            weight_sum=self.weight_sum,
            extremum=self.extremum,
            initialized=self.initialized,
        )

    def value(self) -> Any:
        if not self.initialized:
            return None
        if self.agg == "count":
            return self.nn_count
        if self.agg == "sum":
            return normalize_numeric(self.total) if self.nn_count > 0 else None
        if self.agg == "avg":
            if self.nn_count <= 0:
                return None
            return normalize_numeric(_div_exact(self.total, self.nn_count))
        if self.agg in _WAVG_AGGS:
            if self.nn_count <= 0 or _is_zero(self.weight_sum):
                return None
            return normalize_numeric(_div_exact(self.total, self.weight_sum))
        if self.agg in _EXTREMA_AGGS:
            return normalize_numeric(self.extremum) if self.nn_count > 0 else None
        return None

    def apply_row_delta(
        self,
        *,
        old_val: Any = None,
        new_val: Any = None,
        old_weight: Any = None,
        new_weight: Any = None,
        has_old: bool = False,
        has_new: bool = False,
        count_all_rows: bool = False,
    ) -> bool:
        """Apply one row transition. Returns True if Tier B SQL requery is needed."""
        if not self.initialized:
            return True

        old_v = normalize_numeric(old_val) if has_old else None
        new_v = normalize_numeric(new_val) if has_new else None

        if self.agg == "count":
            if has_old and (count_all_rows or old_v is not None):
                self.nn_count = max(0, self.nn_count - 1)
            if has_new and (count_all_rows or new_v is not None):
                self.nn_count += 1
            return False

        if self.agg in {"sum", "avg"}:
            if has_old and old_v is not None:
                self.total = _sub_exact(self.total, old_v) if self.nn_count > 1 else _zero_like(old_v)
                self.nn_count = max(0, self.nn_count - 1)
            if has_new and new_v is not None:
                self.total = new_v if self.nn_count == 0 else _add_exact(self.total, new_v)
                self.nn_count += 1
            return False

        if self.agg in _WAVG_AGGS:
            old_w = normalize_numeric(old_weight) if has_old else None
            new_w = normalize_numeric(new_weight) if has_new else None
            # Correction 1A: Exact same validity predicate (x IS NOT NULL AND w IS NOT NULL).
            old_valid = has_old and old_v is not None and old_w is not None
            new_valid = has_new and new_v is not None and new_w is not None
            if old_valid:
                contrib = _mul_exact(old_v, old_w)
                if self.nn_count <= 1:
                    self.total = _zero_like(contrib)
                    self.weight_sum = _zero_like(old_w)
                    self.nn_count = 0
                else:
                    self.total = _sub_exact(self.total, contrib)
                    self.weight_sum = _sub_exact(self.weight_sum, old_w)
                    self.nn_count -= 1
            if new_valid:
                contrib = _mul_exact(new_v, new_w)
                if self.nn_count == 0:
                    self.total = contrib
                    self.weight_sum = new_w
                    self.nn_count = 1
                else:
                    self.total = _add_exact(self.total, contrib)
                    self.weight_sum = _add_exact(self.weight_sum, new_w)
                    self.nn_count += 1
            return False

        if self.agg == "min":
            if has_old and old_v is not None:
                self.nn_count = max(0, self.nn_count - 1)
                if self.extremum is not None and values_equal(old_v, self.extremum):
                    # Extremum removed or modified: must trigger Tier B SQL unless replaced by <= value.
                    if not (has_new and new_v is not None and new_v <= self.extremum):
                        self.initialized = False
                        return True
            if has_new and new_v is not None:
                self.nn_count += 1
                if self.extremum is None or new_v < self.extremum:
                    self.extremum = new_v
            if self.nn_count == 0:
                self.extremum = None
            return False

        if self.agg == "max":
            if has_old and old_v is not None:
                self.nn_count = max(0, self.nn_count - 1)
                if self.extremum is not None and values_equal(old_v, self.extremum):
                    if not (has_new and new_v is not None and new_v >= self.extremum):
                        self.initialized = False
                        return True
            if has_new and new_v is not None:
                self.nn_count += 1
                if self.extremum is None or new_v > self.extremum:
                    self.extremum = new_v
            if self.nn_count == 0:
                self.extremum = None
            return False

        return True


@dataclass
class GroupSufficientStats:
    """Complete sufficient-statistics record for one logical hierarchy group node."""

    canonical_path: str
    path_values: Tuple[Any, ...]
    applied_revision: int
    row_count: int = 0
    # Keyed by (col_key, measure_alias) where col_key == "" for unpivoted measures.
    cells: Dict[Tuple[str, str], SufficientStatCell] = field(default_factory=dict)
    initialized: bool = True

    def display_path(self) -> str:
        return CanonicalPathCodec.display_path(self.path_values)

    def clone(self) -> "GroupSufficientStats":
        return GroupSufficientStats(
            canonical_path=self.canonical_path,
            path_values=self.path_values,
            applied_revision=self.applied_revision,
            row_count=self.row_count,
            cells={k: v.clone() for k, v in self.cells.items()},
            initialized=self.initialized,
        )


def _formula_identifier(raw_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", str(raw_name).strip())


class CrossNodeDependencyGraph:
    """Classifies formulas and window functions to propagate cross-node invalidations."""

    def __init__(self, measures: Sequence[Measure]) -> None:
        self.measures: List[Measure] = list(measures)
        self.by_alias: Dict[str, Measure] = {m.alias: m for m in self.measures}
        self.algebra_class: Dict[str, MeasureAlgebraClass] = {}
        self.scope: Dict[str, DependencyScope] = {}
        self.formula_deps: Dict[str, Set[str]] = {}
        self.formula_order: List[str] = []
        self.has_grand_total_dependents: bool = False
        self.has_row_total_dependents: bool = False
        self.has_global_rank_dependents: bool = False
        self._parser = SafeExpressionParser()

        for m in self.measures:
            alg_cls, dep_scope = classify_measure(m)
            self.algebra_class[m.alias] = alg_cls
            self.scope[m.alias] = dep_scope
            if dep_scope == DependencyScope.SCOPE_GRAND_TOTAL:
                self.has_grand_total_dependents = True
            elif dep_scope == DependencyScope.SCOPE_ROW_TOTAL:
                self.has_row_total_dependents = True
            elif dep_scope == DependencyScope.SCOPE_GLOBAL_RANK:
                self.has_global_rank_dependents = True
            if alg_cls == MeasureAlgebraClass.POST_AGG_FORMULA:
                raw_expr = str(getattr(m, "formula", None) or getattr(m, "expression", None) or "")
                refs = set(_BRACKET_REF_RE.findall(raw_expr))
                self.formula_deps[m.alias] = refs

        # Topological sort of formulas.
        visited: Set[str] = set()
        visiting: Set[str] = set()

        def visit(alias: str) -> None:
            if alias in visited or alias in visiting:
                return
            visiting.add(alias)
            for dep in self.formula_deps.get(alias, ()):
                if dep in self.formula_deps:
                    visit(dep)
            visiting.discard(alias)
            visited.add(alias)
            if alias in self.formula_deps:
                self.formula_order.append(alias)

        for m in self.measures:
            if m.alias in self.formula_deps:
                visit(m.alias)

    def evaluate_formula(self, alias: str, scalar_map: Mapping[str, Any]) -> Any:
        measure = self.by_alias.get(alias)
        if measure is None:
            return None
        raw_expr = str(getattr(measure, "formula", None) or getattr(measure, "expression", None) or "").strip()
        if not raw_expr:
            return None

        context: Dict[str, Any] = {}
        for key, val in scalar_map.items():
            safe_key = _formula_identifier(key)
            context[safe_key] = val
            # Also bind by underlying field name if unambiguous.
            m = self.by_alias.get(key)
            if m is not None and m.field:
                context.setdefault(_formula_identifier(m.field), val)

        def _replace_ref(match: re.Match[str]) -> str:
            token = match.group(1).strip()
            safe = _formula_identifier(token)
            if safe not in context:
                # Try matching measure alias or field case-insensitively.
                lowered = token.lower()
                for m in self.measures:
                    if m.alias.lower() == lowered or (m.field and m.field.lower() == lowered):
                        context[safe] = scalar_map.get(m.alias)
                        break
            return safe

        compiled = _BRACKET_REF_RE.sub(_replace_ref, raw_expr)
        # If any referenced variable is None (SQL NULL propagation), return None.
        for ref in _BRACKET_REF_RE.findall(raw_expr):
            if context.get(_formula_identifier(ref)) is None:
                return None
        try:
            res = self._parser.evaluate(compiled, context)
            return normalize_numeric(res)
        except (ValueError, ZeroDivisionError, ArithmeticError):
            return None


class ExactMeasureEngine:
    """Component 3A: Initializes, updates, and evaluates sufficient-stats group states."""

    def __init__(self, measures: Sequence[Measure]) -> None:
        self.measures: List[Measure] = list(measures)
        self.graph = CrossNodeDependencyGraph(self.measures)
        self.base_measures: List[Measure] = [
            m
            for m in self.measures
            if self.graph.algebra_class[m.alias] != MeasureAlgebraClass.POST_AGG_FORMULA
        ]
        self.supports_incremental: bool = all(
            self.graph.algebra_class[m.alias] != MeasureAlgebraClass.HOLISTIC_SQL_ONLY
            for m in self.measures
        )

    def init_group_from_projected_row(
        self,
        row: Mapping[str, Any],
        *,
        path_values: Tuple[Any, ...],
        source_revision: int,
        col_key: str = "",
        existing: Optional[GroupSufficientStats] = None,
    ) -> GroupSufficientStats:
        """Initialize a group strictly from SufficientStatsProjector output columns."""
        canonical_path = CanonicalPathCodec.encode(path_values)
        group = existing or GroupSufficientStats(
            canonical_path=canonical_path,
            path_values=path_values,
            applied_revision=source_revision,
            row_count=0,
            initialized=True,
        )
        if SufficientStatsProjector.ROW_COUNT_COL not in row:
            # Never initialize from display scalars without sufficient stats!
            group.initialized = False
            return group

        row_cnt = int(row.get(SufficientStatsProjector.ROW_COUNT_COL) or 0)
        if col_key == "":
            group.row_count = row_cnt
        else:
            group.row_count += row_cnt

        for measure in self.base_measures:
            alias = measure.alias
            agg = str(measure.agg or "sum").strip().lower()
            if agg == "count":
                cell = SufficientStatCell(
                    agg="count",
                    nn_count=int(row.get(alias) or 0),
                    initialized=True,
                )
            elif agg == "sum":
                nn_col = SufficientStatsProjector.nn_col(alias)
                if nn_col not in row:
                    cell = SufficientStatCell(agg="sum", initialized=False)
                else:
                    nn = int(row.get(nn_col) or 0)
                    tot = normalize_numeric(row.get(alias))
                    cell = SufficientStatCell(
                        agg="sum",
                        total=0 if tot is None else tot,
                        nn_count=nn,
                        initialized=True,
                    )
            elif agg == "avg":
                sum_col = SufficientStatsProjector.sum_col(alias)
                nn_col = SufficientStatsProjector.nn_col(alias)
                if sum_col not in row or nn_col not in row:
                    cell = SufficientStatCell(agg="avg", initialized=False)
                else:
                    nn = int(row.get(nn_col) or 0)
                    tot = normalize_numeric(row.get(sum_col))
                    cell = SufficientStatCell(
                        agg="avg",
                        total=0 if tot is None else tot,
                        nn_count=nn,
                        initialized=True,
                    )
            elif agg in _WAVG_AGGS:
                wsum_col = SufficientStatsProjector.wsum_col(alias)
                w_col = SufficientStatsProjector.weight_col(alias)
                nn_col = SufficientStatsProjector.nn_col(alias)
                if wsum_col not in row or w_col not in row or nn_col not in row:
                    cell = SufficientStatCell(agg=agg, initialized=False)
                else:
                    nn = int(row.get(nn_col) or 0)
                    wsum = normalize_numeric(row.get(wsum_col))
                    w = normalize_numeric(row.get(w_col))
                    cell = SufficientStatCell(
                        agg=agg,
                        total=0 if wsum is None else wsum,
                        weight_sum=0 if w is None else w,
                        nn_count=nn,
                        initialized=True,
                    )
            elif agg in _EXTREMA_AGGS:
                nn_col = SufficientStatsProjector.nn_col(alias)
                if nn_col not in row:
                    cell = SufficientStatCell(agg=agg, initialized=False)
                else:
                    cell = SufficientStatCell(
                        agg=agg,
                        extremum=normalize_numeric(row.get(alias)),
                        nn_count=int(row.get(nn_col) or 0),
                        initialized=True,
                    )
            else:
                cell = SufficientStatCell(agg=agg, initialized=False)
            group.cells[(col_key, alias)] = cell
        group.applied_revision = source_revision
        return group

    def apply_row_transition(
        self,
        group: GroupSufficientStats,
        *,
        col_key: str,
        old_row: Optional[Mapping[str, Any]],
        new_row: Optional[Mapping[str, Any]],
        source_revision: int,
    ) -> Tuple[bool, Set[Tuple[str, str]]]:
        """Apply one row transition to `group`.

        Returns `(needs_tier_b_requery, dirty_cell_keys)`.
        Enforces Blocker 1A: if `source_revision <= group.applied_revision`,
        the transition is idempotently skipped!
        """
        if source_revision <= group.applied_revision:
            return False, set()
        if not group.initialized:
            return True, set()

        has_old = old_row is not None
        has_new = new_row is not None
        if has_old and not has_new:
            group.row_count = max(0, group.row_count - 1)
        elif has_new and not has_old:
            group.row_count += 1

        needs_tier_b = False
        dirty_cells: Set[Tuple[str, str]] = set()

        for measure in self.base_measures:
            alias = measure.alias
            agg = str(measure.agg or "sum").strip().lower()
            key = (col_key, alias)
            cell = group.cells.get(key)
            if cell is None:
                cell = SufficientStatCell(agg=agg, initialized=True)
                group.cells[key] = cell

            prev_val = cell.value()
            old_val = old_row.get(measure.field) if (has_old and measure.field) else None
            new_val = new_row.get(measure.field) if (has_new and measure.field) else None
            old_w = (
                old_row.get(measure.weighted_field)
                if (has_old and measure.weighted_field)
                else None
            )
            new_w = (
                new_row.get(measure.weighted_field)
                if (has_new and measure.weighted_field)
                else None
            )
            invalidated = cell.apply_row_delta(
                old_val=old_val,
                new_val=new_val,
                old_weight=old_w,
                new_weight=new_w,
                has_old=has_old,
                has_new=has_new,
                count_all_rows=(measure.field is None or measure.field == ""),
            )
            if invalidated:
                needs_tier_b = True
                dirty_cells.add(key)
            elif not values_equal(prev_val, cell.value()):
                dirty_cells.add(key)

        return needs_tier_b, dirty_cells

    def materialize_row_scalars(
        self,
        group: GroupSufficientStats,
        *,
        col_keys: Sequence[str],
        grand_total_group: Optional[GroupSufficientStats] = None,
    ) -> Dict[str, Any]:
        """Materialize scalar values (base measures, formulas, and window ratios)."""
        result: Dict[str, Any] = {}
        active_col_keys = list(col_keys) if col_keys else [""]

        # Precompute Grand Total base values for SCOPE_GRAND_TOTAL window ratios.
        gt_base: Dict[Tuple[str, str], Any] = {}
        if grand_total_group is not None:
            for ck in active_col_keys:
                for m in self.base_measures:
                    cell = grand_total_group.cells.get((ck, m.alias))
                    gt_base[(ck, m.alias)] = cell.value() if cell is not None else None

        # Precompute row totals across pivot columns for SCOPE_ROW_TOTAL ratios.
        row_totals: Dict[str, Any] = {}
        if len(active_col_keys) > 1 or (len(active_col_keys) == 1 and active_col_keys[0] != ""):
            for m in self.base_measures:
                agg = str(m.agg or "sum").strip().lower()
                if agg in _EXTENSIVE_AGGS:
                    total_val: Any = None
                    for ck in active_col_keys:
                        c = group.cells.get((ck, m.alias))
                        v = c.value() if c is not None else None
                        if v is not None:
                            total_val = v if total_val is None else _add_exact(total_val, v)
                    row_totals[m.alias] = total_val

        for ck in active_col_keys:
            prefix = f"{ck}_" if ck else ""
            local_scalars: Dict[str, Any] = {}

            # 1. Base measures
            for m in self.base_measures:
                cell = group.cells.get((ck, m.alias))
                raw_val = cell.value() if cell is not None else None
                scope = self.graph.scope[m.alias]
                if scope == DependencyScope.SCOPE_GRAND_TOTAL:
                    denom = gt_base.get((ck, m.alias))
                    if raw_val is None or denom is None or _is_zero(denom):
                        val = None
                    else:
                        val = normalize_numeric(_div_exact(raw_val, denom))
                elif scope == DependencyScope.SCOPE_ROW_TOTAL:
                    denom = row_totals.get(m.alias)
                    if raw_val is None or denom is None or _is_zero(denom):
                        val = None
                    else:
                        val = normalize_numeric(_div_exact(raw_val, denom))
                else:
                    val = raw_val
                local_scalars[m.alias] = val
                result[f"{prefix}{m.alias}"] = val

            # 2. Topological AST formulas
            for f_alias in self.graph.formula_order:
                f_val = self.graph.evaluate_formula(f_alias, local_scalars)
                local_scalars[f_alias] = f_val
                result[f"{prefix}{f_alias}"] = f_val

        return result
