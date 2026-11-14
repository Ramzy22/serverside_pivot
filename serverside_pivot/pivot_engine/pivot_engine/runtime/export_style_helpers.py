"""Pure export styling, header resolution, and numeric display helpers."""

from __future__ import annotations

from decimal import Decimal
import math
import re
from typing import Any, Dict, List, Optional, Tuple

from .models import PivotViewState, first_present


def export_key_aliases(key: str) -> List[str]:
    text = str(key or "")
    aliases = [text]
    if text == "_id":
        aliases.append("hierarchy")
    elif text == "hierarchy":
        aliases.append("_id")
    return aliases


def display_export_header(key: str, state: PivotViewState) -> str:
    if key == "_id":
        row_fields = state.row_fields or []
        return "/".join(str(field).replace("_", " ").title() for field in row_fields) or "Row"
    return str(key).replace("_", " ").title()


def export_header_for_key(
    key: str,
    header_map: Dict[str, str],
    state: PivotViewState,
    style_profile: Dict[str, Any],
) -> str:
    labels = style_profile.get("headerLabels") if isinstance(style_profile, dict) else None
    if isinstance(labels, dict):
        for alias in export_key_aliases(key):
            label = labels.get(alias)
            if label is not None:
                return str(label)
    return header_map.get(key, display_export_header(key, state))


def export_column_width(key: str, style_profile: Dict[str, Any]) -> Optional[int]:
    widths = style_profile.get("columnWidths") if isinstance(style_profile, dict) else None
    if not isinstance(widths, dict):
        return None
    for alias in export_key_aliases(key):
        try:
            width = int(float(widths.get(alias)))
        except (TypeError, ValueError):
            continue
        if width > 0:
            return min(max(width, 24), 1200)
    return None


def normalize_export_header_style(raw_style: Any) -> Dict[str, Any]:
    if isinstance(raw_style, str):
        return {"background": raw_style} if raw_style.strip() else {}
    if not isinstance(raw_style, dict):
        return {}
    style_keys = {
        "background",
        "backgroundColor",
        "color",
        "border",
        "borderColor",
        "borderTop",
        "borderRight",
        "borderBottom",
        "borderLeft",
        "boxShadow",
        "fontWeight",
        "fontStyle",
        "textDecoration",
        "textTransform",
        "letterSpacing",
    }
    source: Dict[str, Any] = dict(raw_style)
    nested_style = source.get("style")
    if isinstance(nested_style, dict):
        source.update(nested_style)
    style = {
        key: value
        for key, value in source.items()
        if key in style_keys and value not in (None, False, "")
    }
    background = first_present(
        source,
        "background",
        "backgroundColor",
        "bg",
        "fill",
        "headerBg",
        "headerBackground",
        "headerBackgroundColor",
        "headerColor",
    )
    if background not in (None, False, ""):
        style["background"] = background
        style.pop("backgroundColor", None)
    color = first_present(
        source,
        "color",
        "textColor",
        "foreground",
        "fg",
        "headerText",
        "headerTextColor",
    )
    if color not in (None, False, ""):
        style["color"] = color
    return style


def export_header_formatting_style(key: str, header_label: str, style_profile: Dict[str, Any]) -> Dict[str, Any]:
    formatting = style_profile.get("headerFormatting") if isinstance(style_profile, dict) else None
    if not formatting:
        return {}
    candidates = set()
    for alias in export_key_aliases(key):
        if alias:
            candidates.add(str(alias))
            candidates.add(str(alias).lower())
    if header_label:
        candidates.add(str(header_label))
        candidates.add(str(header_label).lower())
    style: Dict[str, Any] = {}

    def merge(candidate: Any) -> None:
        style.update(normalize_export_header_style(candidate))

    def matches_rule(rule: Dict[str, Any]) -> bool:
        if not isinstance(rule, dict):
            return False
        if rule.get("all") or rule.get("default") or rule.get("column") == "*" or rule.get("id") == "*":
            return True
        targets: List[Any] = []
        for prop in ("column", "columnId", "id", "field", "header", "headerLabel", "label", "group", "groupValue"):
            value = rule.get(prop)
            if value is not None:
                targets.append(value)
        for prop in ("columns", "ids"):
            value = rule.get(prop)
            if isinstance(value, list):
                targets.extend(value)
        if not targets:
            return True
        return any(str(target) in candidates or str(target).lower() in candidates for target in targets if target is not None)

    if isinstance(formatting, list):
        for rule in formatting:
            if matches_rule(rule):
                merge(rule)
        return style

    if not isinstance(formatting, dict):
        return style

    for prop in ("default", "defaultStyle", "all", "*"):
        merge(formatting.get(prop))

    column_map = first_present(formatting, "columns", "byColumn", "columnStyles")
    if isinstance(column_map, dict):
        for candidate in candidates:
            merge(column_map.get(candidate))

    rules = first_present(formatting, "rules", "headerRules", default=[])
    if isinstance(rules, list):
        for rule in rules:
            if matches_rule(rule):
                merge(rule)

    style_config_keys = {
        "background",
        "backgroundColor",
        "color",
        "bg",
        "fill",
        "foreground",
        "fg",
        "textColor",
        "headerBg",
        "headerBackground",
        "headerBackgroundColor",
        "headerColor",
        "headerText",
        "headerTextColor",
        "style",
    }
    if any(key_name in formatting for key_name in style_config_keys):
        merge(formatting)

    reserved = {"default", "defaultStyle", "all", "*", "columns", "byColumn", "columnStyles", "rules", "headerRules"}
    if not isinstance(column_map, dict):
        for candidate in candidates:
            if candidate not in reserved:
                merge(formatting.get(candidate))

    return style


def css_property_name(name: str) -> str:
    return re.sub(r"([A-Z])", lambda match: "-" + match.group(1).lower(), str(name or ""))


def style_to_css(style: Dict[str, Any]) -> str:
    if not isinstance(style, dict):
        return ""
    unitless = {"fontWeight", "lineHeight", "opacity", "zIndex"}
    parts: List[str] = []
    for prop, raw_value in style.items():
        if raw_value is None or raw_value is False or raw_value == "":
            continue
        if isinstance(raw_value, (int, float)) and math.isfinite(float(raw_value)) and prop not in unitless:
            value = f"{raw_value}px"
        else:
            value = str(raw_value)
        parts.append(f"{css_property_name(str(prop))}:{value}")
    return ";".join(parts)


def round_half_away(value: float, decimals: int) -> float:
    d = max(0, min(20, int(decimals)))
    factor = 10 ** d
    rounded = math.floor(abs(value) * factor + 0.5 + 1e-12) / factor
    if rounded == 0:
        return 0.0
    return -rounded if value < 0 else rounded


def resolve_compact_export_spec(value: float, decimal_places: Optional[int]) -> Tuple[float, str, int]:
    abs_val = abs(value)
    divisor = 1.0
    suffix = ""
    if abs_val >= 1e9:
        divisor = 1e9
        suffix = "B"
    elif abs_val >= 1e6:
        divisor = 1e6
        suffix = "M"
    elif abs_val >= 1e3:
        divisor = 1e3
        suffix = "K"
    scaled = value / divisor
    places = max(0, min(20, int(decimal_places))) if decimal_places is not None else (1 if divisor > 1.0 else 0)
    rounded = round_half_away(scaled, places)
    if divisor == 1.0 and abs(rounded) >= 1000.0:
        return resolve_compact_export_spec(rounded, decimal_places)
    return rounded, suffix, places


def resolve_export_numeric_display(
    value: Any,
    fmt: Any,
    decimal_places: Any,
    value_scale: Any = None,
) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    try:
        scale_num = float(value_scale) if value_scale is not None else 1.0
    except (TypeError, ValueError):
        scale_num = 1.0
    if math.isfinite(scale_num) and scale_num not in (0.0, 1.0):
        numeric = numeric / scale_num
    if numeric == 0:
        numeric = 0.0
    fmt_text = str(fmt or "").strip()
    decimals: Optional[int]
    try:
        decimals = int(decimal_places) if decimal_places is not None else None
    except (TypeError, ValueError):
        decimals = None
    if fmt_text == "compact" or fmt_text.startswith("compact:"):
        compact_dec = decimals
        if compact_dec is None and fmt_text.startswith("compact:"):
            try:
                compact_dec = int(fmt_text.split(":", 1)[1])
            except (IndexError, TypeError, ValueError):
                compact_dec = None
        rounded, _, _ = resolve_compact_export_spec(numeric, compact_dec)
        return 0.0 if rounded == 0 else rounded
    if fmt_text == "percent":
        dec = decimals if decimals is not None else 2
        pct_rounded = round_half_away(numeric * 100.0, dec)
        return 0.0 if pct_rounded == 0 else pct_rounded / 100.0
    if fmt_text == "scientific":
        return 0.0 if numeric == 0 else numeric
    if fmt_text in {"currency", "accounting"} or fmt_text.startswith(("currency:", "accounting:", "fixed")):
        if fmt_text.startswith("fixed:") and decimals is None:
            try:
                decimals = int(fmt_text.split(":", 1)[1])
            except (IndexError, TypeError, ValueError):
                decimals = 2
        dec = decimals if decimals is not None else 2
        return round_half_away(numeric, dec)
    default_dec = decimals if decimals is not None else 3
    checked = round_half_away(numeric, default_dec)
    if checked == 0:
        return 0.0
    return checked if decimals is not None else (0.0 if numeric == 0 else numeric)


def format_export_number(
    value: float,
    fmt: Any,
    decimal_places: Any,
    group_separator: Any,
    value_scale: Any = None,
    unit_suffix: Any = None,
) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(numeric):
        return ""
    try:
        scale_num = float(value_scale) if value_scale is not None else 1.0
    except (TypeError, ValueError):
        scale_num = 1.0
    if math.isfinite(scale_num) and scale_num not in (0.0, 1.0):
        numeric = numeric / scale_num
    if numeric == 0:
        numeric = 0.0

    fmt_text = str(fmt or "").strip()
    decimals: Optional[int]
    try:
        decimals = int(decimal_places) if decimal_places is not None else None
    except (TypeError, ValueError):
        decimals = None
    if fmt_text.startswith("fixed:") and decimals is None:
        try:
            decimals = int(fmt_text.split(":", 1)[1])
        except (IndexError, TypeError, ValueError):
            decimals = 2
    if decimals is None and fmt_text != "compact" and not fmt_text.startswith("compact:"):
        decimals = 2 if fmt_text in {"currency", "accounting", "percent", "scientific"} or fmt_text.startswith(("currency:", "accounting:", "fixed")) else 0

    separator_map = {
        "comma": ",",
        # HTML/copy exports must not contain non-breaking or narrow
        # non-breaking spaces: both are surprising when pasted into text
        # tools and can break equality checks or numeric parsing.
        "space": " ",
        "thin_space": " ",
        "apostrophe": "'",
        "none": "",
    }
    separator = separator_map.get(str(group_separator or "thin_space"), " ")
    suffix_str = str(unit_suffix) if isinstance(unit_suffix, str) else ""

    def grouped(number: float, places: int) -> str:
        rounded = round_half_away(number, places)
        if rounded == 0:
            rounded = 0.0
        formatted = f"{rounded:,.{max(0, places)}f}"
        return formatted.replace(",", separator)

    if fmt_text == "compact" or fmt_text.startswith("compact:"):
        compact_dec = decimals
        if compact_dec is None and fmt_text.startswith("compact:"):
            try:
                compact_dec = int(fmt_text.split(":", 1)[1])
            except (IndexError, TypeError, ValueError):
                compact_dec = None
        rounded, compact_suffix, places = resolve_compact_export_spec(numeric, compact_dec)
        if decimals is None and compact_suffix:
            text = f"{rounded:,.{places}f}".replace(",", separator)
            if "." in text:
                text = text.rstrip("0").rstrip(".")
        else:
            text = grouped(rounded, places)
        return f"{text}{compact_suffix}{suffix_str}"

    if fmt_text == "percent":
        pct_val = round_half_away(numeric * 100.0, decimals or 0)
        return f"{grouped(pct_val, decimals or 0)}%{suffix_str}"
    if fmt_text == "scientific":
        if abs(numeric) < 0.5 * (10 ** (-max(0, decimals or 0))) and numeric != 0 and abs(numeric) < 1e-12:
            numeric = 0.0
        return f"{numeric:.{max(0, decimals or 0)}e}{suffix_str}"
    if fmt_text == "currency" or fmt_text.startswith("currency:"):
        symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
        rounded = round_half_away(numeric, decimals or 0)
        sign = "-" if rounded < 0 else ""
        return f"{sign}{symbol}{grouped(abs(rounded), decimals or 0)}{suffix_str}"
    if fmt_text == "accounting" or fmt_text.startswith("accounting:"):
        symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
        rounded = round_half_away(numeric, decimals or 0)
        formatted = f"{symbol}{grouped(abs(rounded), decimals or 0)}{suffix_str}"
        return f"({formatted})" if rounded < 0 else formatted
    return f"{grouped(numeric, decimals or 0)}{suffix_str}"


def matching_value_config_for_export(
    key: str,
    state: Optional[PivotViewState],
    style_profile: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    profile = style_profile if isinstance(style_profile, dict) else {}
    state_configs = getattr(state, "val_configs", None) if state is not None else None
    configs = profile.get("valConfigs") if isinstance(profile.get("valConfigs"), list) else state_configs
    normalized_key = str(key or "").lower()
    for config in configs or []:
        if not isinstance(config, dict):
            continue
        field = str(config.get("field") or "").lower()
        agg = str(config.get("agg") or "sum").lower()
        if not field:
            continue
        if agg == "formula":
            if normalized_key == field or normalized_key.endswith(f"_{field}"):
                return config
        else:
            suffix = f"{field}_{agg}"
            if normalized_key == field or normalized_key == suffix or normalized_key.endswith(f"_{suffix}") or f"_{field}_" in normalized_key:
                return config
    return None


def formatted_export_value(
    key: str,
    value: Any,
    row: Dict[str, Any],
    state: PivotViewState,
    style_profile: Dict[str, Any],
) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if key == "_id":
        return str(value)
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        decimal_overrides = style_profile.get("columnDecimalOverrides") if isinstance(style_profile, dict) else None
        format_overrides = style_profile.get("columnFormatOverrides") if isinstance(style_profile, dict) else None
        group_overrides = style_profile.get("columnGroupSeparatorOverrides") if isinstance(style_profile, dict) else None
        decimal_places = style_profile.get("decimalPlaces") if isinstance(style_profile, dict) else None
        default_format = style_profile.get("defaultValueFormat") if isinstance(style_profile, dict) else None
        group_separator = style_profile.get("numberGroupSeparator") if isinstance(style_profile, dict) else None
        config = matching_value_config_for_export(key, state, style_profile)
        for alias in export_key_aliases(key):
            if isinstance(decimal_overrides, dict) and alias in decimal_overrides:
                decimal_places = decimal_overrides[alias]
            if isinstance(format_overrides, dict) and alias in format_overrides:
                default_format = format_overrides[alias]
            if isinstance(group_overrides, dict) and alias in group_overrides:
                group_separator = group_overrides[alias]
        if (not default_format) and isinstance(config, dict):
            default_format = config.get("format") or default_format
        value_scale = (
            config.get("valueScale")
            if isinstance(config, dict) and config.get("valueScale") is not None
            else (style_profile.get("valueScale") if isinstance(style_profile, dict) else None)
        )
        unit_suffix = (
            config.get("unitSuffix")
            if isinstance(config, dict) and config.get("unitSuffix") is not None
            else (style_profile.get("unitSuffix") if isinstance(style_profile, dict) else None)
        )
        return format_export_number(
            float(value),
            default_format,
            decimal_places,
            group_separator,
            value_scale=value_scale,
            unit_suffix=unit_suffix,
        )
    if isinstance(value, (list, tuple)):
        return f"{len(value)} items"
    if isinstance(value, dict):
        return str(value)
    return str(value)


def export_condition_matches(condition: Any, left_value: float, right_value: Any) -> bool:
    try:
        compare_value = float(right_value)
    except (TypeError, ValueError):
        return False
    if condition == ">":
        return left_value > compare_value
    if condition == "<":
        return left_value < compare_value
    if condition == ">=":
        return left_value >= compare_value
    if condition == "<=":
        return left_value <= compare_value
    if condition in {"==", "="}:
        return left_value == compare_value
    return False


def conditional_export_style(key: str, value: Any, row: Dict[str, Any], style_profile: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(style_profile, dict):
        return {}
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return {}
    if not math.isfinite(numeric):
        return {}
    style: Dict[str, Any] = {}
    rules = style_profile.get("conditionalFormatting")
    if isinstance(rules, list):
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            column = rule.get("column")
            if column and str(column) not in export_key_aliases(key):
                continue
            if export_condition_matches(rule.get("condition"), numeric, rule.get("value")):
                rule_style = rule.get("style")
                if isinstance(rule_style, dict):
                    style.update(rule_style)

    color_mode = str(style_profile.get("colorScaleMode") or "off")
    if color_mode == "off" or row.get("_isTotal") or row.get("_path") == "__grand_total__" or row.get("_id") == "Grand Total":
        return style
    stats_payload = style_profile.get("colorScaleStats")
    stats = None
    if isinstance(stats_payload, dict):
        if color_mode == "col" and isinstance(stats_payload.get("byCol"), dict):
            for alias in export_key_aliases(key):
                if alias in stats_payload["byCol"]:
                    stats = stats_payload["byCol"][alias]
                    break
        elif color_mode == "table":
            stats = stats_payload.get("table")
    if not isinstance(stats, dict):
        return style
    try:
        min_value = float(stats.get("min"))
        max_value = float(stats.get("max"))
    except (TypeError, ValueError):
        return style
    if not math.isfinite(min_value) or not math.isfinite(max_value) or min_value == max_value:
        return style
    palettes = {
        "redGreen": {"low": (248, 105, 107), "high": (99, 190, 123)},
        "greenRed": {"low": (99, 190, 123), "high": (248, 105, 107)},
        "blueWhite": {"low": (65, 105, 225), "high": (255, 255, 255)},
        "yellowBlue": {"low": (255, 220, 0), "high": (30, 90, 200)},
        "orangePurp": {"low": (255, 140, 0), "high": (120, 50, 200)},
    }
    palette = palettes.get(str(style_profile.get("colorPalette") or "redGreen"), palettes["redGreen"])
    if min_value < 0 < max_value:
        pos = 0.5 * (numeric - min_value) / (0 - min_value) if numeric <= 0 else 0.5 + 0.5 * numeric / max_value
    else:
        pos = (numeric - min_value) / (max_value - min_value)
    clamped = max(0.0, min(1.0, pos))
    distance = abs(clamped - 0.5) * 2
    alpha = 0.06 + distance * 0.76
    rgb = palette["low"] if clamped <= 0.5 else palette["high"]
    luminance = (0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]) / 255
    style["background"] = f"rgba({rgb[0]},{rgb[1]},{rgb[2]},{alpha:.3f})"
    if alpha > 0.5:
        style["color"] = "#111827" if luminance > 0.45 else "#ffffff"
    return style


def resolve_depth_token(token_val: Any, depth: int) -> Any:
    if isinstance(token_val, list) and token_val:
        idx = min(max(0, depth), len(token_val) - 1)
        return token_val[idx]
    return token_val


def export_cell_style(
    key: str,
    value: Any,
    row: Dict[str, Any],
    header_label: str,
    style_profile: Optional[Dict[str, Any]],
    state: Optional[PivotViewState] = None,
) -> Dict[str, Any]:
    style_profile = style_profile if isinstance(style_profile, dict) else {}
    theme = style_profile.get("theme") if isinstance(style_profile.get("theme"), dict) else {}
    font_family = style_profile.get("fontFamily") or theme.get("fontFamily") or "'Inter', ui-sans-serif, system-ui, sans-serif"
    font_size = style_profile.get("fontSize") or (f"{theme.get('fontSize')}px" if isinstance(theme.get("fontSize"), (int, float)) else theme.get("fontSize")) or "13px"
    row_height = style_profile.get("rowHeight") or theme.get("rowHeight")
    is_hierarchy = key == "_id"
    is_total_row = bool(row.get("_isTotal") or row.get("__isGrandTotal__") or row.get("_path") == "__grand_total__" or row.get("_id") == "Grand Total")
    is_total_col = not is_hierarchy and str(header_label or "").startswith("Grand Total")
    try:
        depth = max(0, int(row.get("depth") or 0))
    except (TypeError, ValueError):
        depth = 0
    has_children = bool(row.get("_has_children") or (isinstance(row.get("subRows"), list) and len(row.get("subRows")) > 0))
    is_leaf_row = not is_total_row and not has_children
    group_bg_val = resolve_depth_token(theme.get("groupBg"), depth)
    group_text_val = resolve_depth_token(theme.get("groupText"), depth)
    group_weight_val = resolve_depth_token(theme.get("groupFontWeight"), depth)
    background = (
        theme.get("totalBgStrong") or theme.get("totalBg") or theme.get("select") or theme.get("background") or "#fff"
        if is_total_row
        else theme.get("totalBg") or theme.get("select") or theme.get("background") or "#fff"
        if is_total_col
        else group_bg_val
        if (not is_leaf_row and group_bg_val)
        else theme.get("leafBg")
        if (is_leaf_row and theme.get("leafBg"))
        else theme.get("hierarchyBg") or theme.get("surfaceInset") or theme.get("surfaceBg") or theme.get("background") or "#fff"
        if is_hierarchy
        else theme.get("surfaceBg") or theme.get("background") or "#fff"
    )
    report_format = row.get("_reportFormat") if isinstance(row.get("_reportFormat"), dict) else {}
    row_id = str(
        row.get("_path")
        or ("__grand_total__" if row.get("_isTotal") or row.get("__isGrandTotal__") or row.get("_id") == "Grand Total" else None)
        or row.get("id")
        or row.get("_id")
        or ""
    )
    cell_rules = style_profile.get("cellFormatRules") if isinstance(style_profile.get("cellFormatRules"), dict) else {}
    cell_format: Dict[str, Any] = {}
    if row_id and isinstance(cell_rules, dict):
        for alias in export_key_aliases(key):
            candidate = cell_rules.get(f"{row_id}:::{alias}")
            if isinstance(candidate, dict):
                cell_format = candidate
                break
    row_border_color = theme.get("rowBorder") or theme.get("border") or "#d1d5db"
    border = (
        f"{max(1, int(float(report_format.get('borderWidth') or 1)))}px {report_format.get('borderStyle')} {report_format.get('borderColor') or row_border_color}"
        if report_format.get("borderStyle")
        else f"1px solid {row_border_color}"
    )
    decimal_places = style_profile.get("decimalPlaces") if isinstance(style_profile, dict) else None
    default_format = style_profile.get("defaultValueFormat") if isinstance(style_profile, dict) else None
    config = matching_value_config_for_export(key, state, style_profile) if state is not None else None
    decimal_overrides = style_profile.get("columnDecimalOverrides") if isinstance(style_profile, dict) else None
    format_overrides = style_profile.get("columnFormatOverrides") if isinstance(style_profile, dict) else None
    for alias in export_key_aliases(key):
        if isinstance(decimal_overrides, dict) and alias in decimal_overrides:
            decimal_places = decimal_overrides[alias]
        if isinstance(format_overrides, dict) and alias in format_overrides:
            default_format = format_overrides[alias]
    if (not default_format) and isinstance(config, dict):
        default_format = config.get("format") or default_format
    value_scale = (
        config.get("valueScale")
        if isinstance(config, dict) and config.get("valueScale") is not None
        else (style_profile.get("valueScale") if isinstance(style_profile, dict) else None)
    )
    display_num = (
        resolve_export_numeric_display(value, default_format, decimal_places, value_scale)
        if (not is_hierarchy and isinstance(value, (int, float, Decimal)) and not isinstance(value, bool))
        else None
    )
    default_text_color = (
        theme.get("totalText") or theme.get("text")
        if is_total_row
        else group_text_val
        if (not is_leaf_row and group_text_val)
        else theme.get("text")
        if is_hierarchy
        else theme.get("textSec") or theme.get("text") or "#111827"
    )
    sign_color = (
        theme.get("negativeText")
        if (display_num is not None and display_num < 0 and theme.get("negativeText"))
        else (
            theme.get("positiveText")
            if (display_num is not None and display_num > 0 and theme.get("positiveText"))
            else default_text_color
        )
    )
    resolved_weight = (
        700 if (cell_format.get("bold") or report_format.get("bold"))
        else theme.get("totalFontWeight", 700) if is_total_row
        else 600 if is_total_col
        else group_weight_val if (not is_leaf_row and group_weight_val is not None)
        else theme.get("leafFontWeight", 400) if is_leaf_row
        else 400
    )
    tree_indent = theme.get("treeIndent") if isinstance(theme.get("treeIndent"), (int, float)) else 24
    style = {
        "fontFamily": font_family,
        "fontSize": font_size,
        "height": row_height,
        "color": cell_format.get("color") or sign_color,
        "background": cell_format.get("bg") or report_format.get("rowColor") or background,
        "border": border,
        "padding": f"4px 10px 4px {max(10, int(10 + (depth * tree_indent)))}px" if is_hierarchy else "4px 10px",
        "textAlign": "left" if is_hierarchy else "right",
        "whiteSpace": "pre" if is_hierarchy else "nowrap",
        "fontVariantNumeric": None if is_hierarchy else "tabular-nums",
        "fontWeight": resolved_weight,
        "fontStyle": "italic" if cell_format.get("italic") else None,
    }
    style.update(conditional_export_style(key, value, row, style_profile))
    if cell_format.get("bg"):
        style["background"] = cell_format.get("bg")
    if cell_format.get("color"):
        style["color"] = cell_format.get("color")
    return style


def export_header_style(
    key: str,
    header_label: str,
    style_profile: Optional[Dict[str, Any]],
    state: Optional[PivotViewState] = None,
) -> Dict[str, Any]:
    style_profile = style_profile if isinstance(style_profile, dict) else {}
    theme = style_profile.get("theme") if isinstance(style_profile.get("theme"), dict) else {}
    width = export_column_width(key, style_profile)
    is_total = str(header_label or "").startswith("Grand Total")
    style = {
        "fontFamily": style_profile.get("fontFamily") or theme.get("fontFamily") or "'Inter', ui-sans-serif, system-ui, sans-serif",
        "fontSize": (
            f"{theme.get('headerFontSize')}px"
            if isinstance(theme.get("headerFontSize"), (int, float))
            else (theme.get("headerFontSize") or style_profile.get("fontSize") or "13px")
        ),
        "fontWeight": theme.get("headerFontWeight", 700),
        "color": theme.get("totalText") or theme.get("text") if is_total else theme.get("headerText") or theme.get("text") or "#111827",
        "background": (
            theme.get("totalBgStrong") or theme.get("totalBg") or theme.get("select") or "#e5e7eb"
            if is_total
            else theme.get("headerBg") or theme.get("headerSubtleBg") or theme.get("surfaceBg") or "#f3f4f6"
        ),
        "border": f"1px solid {theme.get('headerBorderBottom') or theme.get('border') or '#d1d5db'}",
        "padding": "6px 10px",
        "textAlign": "center",
        "verticalAlign": "middle",
        "whiteSpace": "nowrap",
        "width": f"{width}px" if width else None,
        "minWidth": f"{width}px" if width else None,
    }
    if state is not None:
        measure_config = matching_value_config_for_export(key, state, style_profile)
        if isinstance(measure_config, dict):
            style.update(normalize_export_header_style(measure_config.get("headerStyle")))
            style.update(normalize_export_header_style(measure_config))
    style.update(export_header_formatting_style(key, header_label, style_profile))
    return style
