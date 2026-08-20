"""Export document formatting and file writers for pivot runtime responses."""

from __future__ import annotations

import csv
from decimal import Decimal
import html
import math
import os
import tempfile
import zipfile
from typing import Any, Dict, List, Optional

from ..tanstack_adapter import TanStackResponse
from .models import PivotViewState, first_present


class RuntimeExportService:
    """Build and write table export payloads without routing requests."""

    @staticmethod
    def _build_export_payload(
        response: TanStackResponse,
        state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        request_payload = export_request if isinstance(export_request, dict) else {}
        export_fmt = str(request_payload.get("format") or "csv").strip().lower()
        if export_fmt == "xls":
            export_fmt = "xlsx"
        if export_fmt not in {"csv", "tsv", "html", "xlsx"}:
            export_fmt = "csv"
        delimiter = "\t" if export_fmt == "tsv" else ","
        ext = {"tsv": "tsv", "html": "html", "xlsx": "xlsx"}.get(export_fmt, "csv")
        content_type = {
            "tsv": "text/tab-separated-values",
            "html": "text/html",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }.get(export_fmt, "text/csv")
        part_label = request_payload.get("partLabel")
        requested_filename = request_payload.get("filename")
        if isinstance(requested_filename, str) and requested_filename.strip():
            filename = requested_filename.strip()
        else:
            filename = (
                f"pivot_export_{part_label}.{ext}"
                if part_label
                else f"pivot_export.{ext}"
            )
        include_headers = bool(
            first_present(
                request_payload, "includeHeaders", "include_headers", default=True
            )
        )

        rows = [row for row in (response.data or []) if isinstance(row, dict)]
        field_order = RuntimeExportService._resolve_export_field_order(
            rows,
            response.columns or [],
            request_payload.get("colIds"),
            state,
        )
        header_map = RuntimeExportService._build_export_header_map(
            response.columns or [], state
        )
        style_profile = (
            request_payload.get("style")
            if isinstance(request_payload.get("style"), dict)
            else (
                request_payload.get("exportStyle")
                if isinstance(request_payload.get("exportStyle"), dict)
                else {}
            )
        )

        value_keys = {key for key in field_order if key != "_id"}

        def _cell(key: str, value: Any) -> Any:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return 0 if key in value_keys else ""
            return value

        fd, content_path = tempfile.mkstemp(prefix="pivot_export_", suffix=f".{ext}")
        try:
            if export_fmt == "xlsx":
                os.close(fd)
                RuntimeExportService._write_xlsx_export(
                    content_path,
                    rows=rows,
                    field_order=field_order,
                    header_map=header_map,
                    state=state,
                    style_profile=style_profile,
                    include_headers=include_headers,
                )
            else:
                with os.fdopen(fd, "w", encoding="utf-8", newline="") as buf:
                    if export_fmt == "html":
                        RuntimeExportService._write_styled_html_export(
                            buf,
                            rows=rows,
                            field_order=field_order,
                            header_map=header_map,
                            state=state,
                            style_profile=style_profile,
                            include_headers=include_headers,
                        )
                    else:
                        writer = csv.writer(buf, delimiter=delimiter)
                        if include_headers and field_order:
                            writer.writerow(
                                [
                                    RuntimeExportService._export_header_for_key(
                                        key, header_map, state, style_profile
                                    )
                                    for key in field_order
                                ]
                            )
                        for row in rows:
                            writer.writerow(
                                [_cell(key, row.get(key)) for key in field_order]
                            )
            content_length = os.path.getsize(content_path)
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.remove(content_path)
            except OSError:
                pass
            raise

        return {
            "contentPath": content_path,
            "contentLength": content_length,
            "contentType": f"{content_type};charset=utf-8",
            "format": export_fmt,
            "filename": filename,
            "rows": len(rows),
            "columns": len(field_order),
            "partId": request_payload.get("partId"),
        }

    @staticmethod
    def _write_xlsx_export(
        content_path: str,
        *,
        rows: List[Dict[str, Any]],
        field_order: List[str],
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
        include_headers: bool,
    ) -> None:
        def _col_name(index: int) -> str:
            name = ""
            current = index + 1
            while current:
                current, remainder = divmod(current - 1, 26)
                name = chr(65 + remainder) + name
            return name

        def _xml_text(value: Any) -> str:
            return html.escape("" if value is None else str(value), quote=True)

        def _is_number(value: Any) -> bool:
            return (
                isinstance(value, (int, float, Decimal))
                and not isinstance(value, bool)
                and math.isfinite(float(value))
            )

        def _cell_xml(row_index: int, col_index: int, value: Any) -> str:
            ref = f"{_col_name(col_index)}{row_index}"
            if value is None or (isinstance(value, float) and math.isnan(value)):
                value = ""
            if _is_number(value):
                return f'<c r="{ref}"><v>{html.escape(str(value), quote=True)}</v></c>'
            return f'<c r="{ref}" t="inlineStr"><is><t>{_xml_text(value)}</t></is></c>'

        sheet_rows: List[str] = []
        excel_row = 1
        if include_headers and field_order:
            headers = [
                RuntimeExportService._export_header_for_key(
                    key, header_map, state, style_profile
                )
                for key in field_order
            ]
            cells = "".join(
                _cell_xml(excel_row, col_index, header)
                for col_index, header in enumerate(headers)
            )
            sheet_rows.append(f'<row r="{excel_row}">{cells}</row>')
            excel_row += 1
        for row in rows:
            values = []
            for key in field_order:
                values.append(
                    RuntimeExportService._formatted_export_value(
                        key, row.get(key), row, state, style_profile
                    )
                )
            cells = "".join(
                _cell_xml(excel_row, col_index, value)
                for col_index, value in enumerate(values)
            )
            sheet_rows.append(f'<row r="{excel_row}">{cells}</row>')
            excel_row += 1

        worksheet_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheetData>{"".join(sheet_rows)}</sheetData>'
            "</worksheet>"
        )
        workbook_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="Pivot" sheetId="1" r:id="rId1"/></sheets>'
            "</workbook>"
        )
        content_types_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            "</Types>"
        )
        root_rels_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            "</Relationships>"
        )
        workbook_rels_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            "</Relationships>"
        )

        with zipfile.ZipFile(
            content_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as workbook:
            workbook.writestr("[Content_Types].xml", content_types_xml)
            workbook.writestr("_rels/.rels", root_rels_xml)
            workbook.writestr("xl/workbook.xml", workbook_xml)
            workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
            workbook.writestr("xl/worksheets/sheet1.xml", worksheet_xml)

    @staticmethod
    def _export_key_aliases(key: str) -> List[str]:
        text = str(key or "")
        aliases = [text]
        if text == "_id":
            aliases.append("hierarchy")
        elif text == "hierarchy":
            aliases.append("_id")
        return aliases

    @staticmethod
    def _export_header_for_key(
        key: str,
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
    ) -> str:
        labels = (
            style_profile.get("headerLabels")
            if isinstance(style_profile, dict)
            else None
        )
        if isinstance(labels, dict):
            for alias in RuntimeExportService._export_key_aliases(key):
                label = labels.get(alias)
                if label is not None:
                    return str(label)
        return header_map.get(
            key, RuntimeExportService._display_export_header(key, state)
        )

    @staticmethod
    def _export_column_width(key: str, style_profile: Dict[str, Any]) -> Optional[int]:
        widths = (
            style_profile.get("columnWidths")
            if isinstance(style_profile, dict)
            else None
        )
        if not isinstance(widths, dict):
            return None
        for alias in RuntimeExportService._export_key_aliases(key):
            try:
                width = int(float(widths.get(alias)))
            except (TypeError, ValueError):
                continue
            if width > 0:
                return min(max(width, 24), 1200)
        return None

    @staticmethod
    def _normalize_export_header_style(raw_style: Any) -> Dict[str, Any]:
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

    @staticmethod
    def _export_header_formatting_style(
        key: str, header_label: str, style_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        formatting = (
            style_profile.get("headerFormatting")
            if isinstance(style_profile, dict)
            else None
        )
        if not formatting:
            return {}
        candidates = set()
        for alias in RuntimeExportService._export_key_aliases(key):
            if alias:
                candidates.add(str(alias))
                candidates.add(str(alias).lower())
        if header_label:
            candidates.add(str(header_label))
            candidates.add(str(header_label).lower())
        style: Dict[str, Any] = {}

        def merge(candidate: Any) -> None:
            style.update(RuntimeExportService._normalize_export_header_style(candidate))

        def matches_rule(rule: Dict[str, Any]) -> bool:
            if not isinstance(rule, dict):
                return False
            if (
                rule.get("all")
                or rule.get("default")
                or rule.get("column") == "*"
                or rule.get("id") == "*"
            ):
                return True
            targets: List[Any] = []
            for prop in (
                "column",
                "columnId",
                "id",
                "field",
                "header",
                "headerLabel",
                "label",
                "group",
                "groupValue",
            ):
                value = rule.get(prop)
                if value is not None:
                    targets.append(value)
            for prop in ("columns", "ids"):
                value = rule.get(prop)
                if isinstance(value, list):
                    targets.extend(value)
            if not targets:
                return True
            return any(
                str(target) in candidates or str(target).lower() in candidates
                for target in targets
                if target is not None
            )

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

        reserved = {
            "default",
            "defaultStyle",
            "all",
            "*",
            "columns",
            "byColumn",
            "columnStyles",
            "rules",
            "headerRules",
        }
        if not isinstance(column_map, dict):
            for candidate in candidates:
                if candidate not in reserved:
                    merge(formatting.get(candidate))

        return style

    @staticmethod
    def _css_property_name(name: str) -> str:
        return re.sub(
            r"([A-Z])", lambda match: "-" + match.group(1).lower(), str(name or "")
        )

    @staticmethod
    def _style_to_css(style: Dict[str, Any]) -> str:
        if not isinstance(style, dict):
            return ""
        unitless = {"fontWeight", "lineHeight", "opacity", "zIndex"}
        parts: List[str] = []
        for prop, raw_value in style.items():
            if raw_value is None or raw_value is False or raw_value == "":
                continue
            if (
                isinstance(raw_value, (int, float))
                and math.isfinite(float(raw_value))
                and prop not in unitless
            ):
                value = f"{raw_value}px"
            else:
                value = str(raw_value)
            parts.append(
                f"{RuntimeExportService._css_property_name(str(prop))}:{value}"
            )
        return ";".join(parts)

    @staticmethod
    def _format_export_number(
        value: float, fmt: Any, decimal_places: Any, group_separator: Any
    ) -> str:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)
        if not math.isfinite(numeric):
            return ""

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
        if decimals is None:
            decimals = (
                2
                if fmt_text in {"currency", "accounting", "percent", "scientific"}
                or fmt_text.startswith(("currency:", "accounting:", "fixed"))
                else 0
            )

        separator_map = {
            "comma": ",",
            "space": "\u00a0",
            "thin_space": "\u202f",
            "apostrophe": "'",
            "none": "",
        }
        separator = separator_map.get(str(group_separator or "thin_space"), "\u202f")

        def grouped(number: float, places: int) -> str:
            formatted = f"{number:,.{max(0, places)}f}"
            return formatted.replace(",", separator)

        if fmt_text == "percent":
            return f"{grouped(numeric * 100, decimals)}%"
        if fmt_text == "scientific":
            return f"{numeric:.{max(0, decimals)}e}"
        if fmt_text == "currency" or fmt_text.startswith("currency:"):
            symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
            sign = "-" if numeric < 0 else ""
            return f"{sign}{symbol}{grouped(abs(numeric), decimals)}"
        if fmt_text == "accounting" or fmt_text.startswith("accounting:"):
            symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
            formatted = f"{symbol}{grouped(abs(numeric), decimals)}"
            return f"({formatted})" if numeric < 0 else formatted
        return grouped(numeric, decimals)

    @staticmethod
    def _matching_value_config_for_export(
        key: str, state: PivotViewState, style_profile: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        configs = (
            style_profile.get("valConfigs")
            if isinstance(style_profile.get("valConfigs"), list)
            else state.val_configs
        )
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
                if (
                    normalized_key == field
                    or normalized_key == suffix
                    or normalized_key.endswith(f"_{suffix}")
                    or f"_{field}_" in normalized_key
                ):
                    return config
        return None

    @staticmethod
    def _formatted_export_value(
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
            decimal_overrides = (
                style_profile.get("columnDecimalOverrides")
                if isinstance(style_profile, dict)
                else None
            )
            format_overrides = (
                style_profile.get("columnFormatOverrides")
                if isinstance(style_profile, dict)
                else None
            )
            group_overrides = (
                style_profile.get("columnGroupSeparatorOverrides")
                if isinstance(style_profile, dict)
                else None
            )
            decimal_places = (
                style_profile.get("decimalPlaces")
                if isinstance(style_profile, dict)
                else None
            )
            default_format = (
                style_profile.get("defaultValueFormat")
                if isinstance(style_profile, dict)
                else None
            )
            group_separator = (
                style_profile.get("numberGroupSeparator")
                if isinstance(style_profile, dict)
                else None
            )
            config = RuntimeExportService._matching_value_config_for_export(
                key, state, style_profile
            )
            for alias in RuntimeExportService._export_key_aliases(key):
                if isinstance(decimal_overrides, dict) and alias in decimal_overrides:
                    decimal_places = decimal_overrides[alias]
                if isinstance(format_overrides, dict) and alias in format_overrides:
                    default_format = format_overrides[alias]
                if isinstance(group_overrides, dict) and alias in group_overrides:
                    group_separator = group_overrides[alias]
            if (not default_format) and isinstance(config, dict):
                default_format = config.get("format") or default_format
            return RuntimeExportService._format_export_number(
                float(value), default_format, decimal_places, group_separator
            )
        if isinstance(value, (list, tuple)):
            return f"{len(value)} items"
        if isinstance(value, dict):
            return str(value)
        return str(value)

    @staticmethod
    def _export_condition_matches(
        condition: Any, left_value: float, right_value: Any
    ) -> bool:
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

    @staticmethod
    def _conditional_export_style(
        key: str, value: Any, row: Dict[str, Any], style_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
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
                if column and str(
                    column
                ) not in RuntimeExportService._export_key_aliases(key):
                    continue
                if RuntimeExportService._export_condition_matches(
                    rule.get("condition"), numeric, rule.get("value")
                ):
                    rule_style = rule.get("style")
                    if isinstance(rule_style, dict):
                        style.update(rule_style)

        color_mode = str(style_profile.get("colorScaleMode") or "off")
        if (
            color_mode == "off"
            or row.get("_isTotal")
            or row.get("_path") == "__grand_total__"
            or row.get("_id") == "Grand Total"
        ):
            return style
        stats_payload = style_profile.get("colorScaleStats")
        stats = None
        if isinstance(stats_payload, dict):
            if color_mode == "col" and isinstance(stats_payload.get("byCol"), dict):
                for alias in RuntimeExportService._export_key_aliases(key):
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
        if (
            not math.isfinite(min_value)
            or not math.isfinite(max_value)
            or min_value == max_value
        ):
            return style
        palettes = {
            "redGreen": {"low": (248, 105, 107), "high": (99, 190, 123)},
            "greenRed": {"low": (99, 190, 123), "high": (248, 105, 107)},
            "blueWhite": {"low": (65, 105, 225), "high": (255, 255, 255)},
            "yellowBlue": {"low": (255, 220, 0), "high": (30, 90, 200)},
            "orangePurp": {"low": (255, 140, 0), "high": (120, 50, 200)},
        }
        palette = palettes.get(
            str(style_profile.get("colorPalette") or "redGreen"), palettes["redGreen"]
        )
        if min_value < 0 < max_value:
            pos = (
                0.5 * (numeric - min_value) / (0 - min_value)
                if numeric <= 0
                else 0.5 + 0.5 * numeric / max_value
            )
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

    @staticmethod
    def _export_cell_style(
        key: str,
        value: Any,
        row: Dict[str, Any],
        header_label: str,
        style_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        theme = (
            style_profile.get("theme")
            if isinstance(style_profile.get("theme"), dict)
            else {}
        )
        font_family = (
            style_profile.get("fontFamily")
            or "'Inter', ui-sans-serif, system-ui, sans-serif"
        )
        font_size = style_profile.get("fontSize") or "13px"
        row_height = style_profile.get("rowHeight")
        is_hierarchy = key == "_id"
        is_total_row = bool(
            row.get("_isTotal")
            or row.get("__isGrandTotal__")
            or row.get("_path") == "__grand_total__"
            or row.get("_id") == "Grand Total"
        )
        is_total_col = str(header_label or "").startswith("Grand Total")
        background = (
            theme.get("totalBgStrong")
            or theme.get("totalBg")
            or theme.get("select")
            or theme.get("background")
            or "#fff"
            if is_total_row
            else (
                theme.get("totalBg")
                or theme.get("select")
                or theme.get("background")
                or "#fff"
                if is_total_col
                else (
                    theme.get("hierarchyBg")
                    or theme.get("surfaceInset")
                    or theme.get("surfaceBg")
                    or theme.get("background")
                    or "#fff"
                    if is_hierarchy
                    else theme.get("surfaceBg") or theme.get("background") or "#fff"
                )
            )
        )
        try:
            depth = max(0, int(row.get("depth") or 0))
        except (TypeError, ValueError):
            depth = 0
        report_format = (
            row.get("_reportFormat")
            if isinstance(row.get("_reportFormat"), dict)
            else {}
        )
        row_id = str(
            row.get("_path")
            or (
                "__grand_total__"
                if row.get("_isTotal")
                or row.get("__isGrandTotal__")
                or row.get("_id") == "Grand Total"
                else None
            )
            or row.get("id")
            or row.get("_id")
            or ""
        )
        cell_rules = (
            style_profile.get("cellFormatRules")
            if isinstance(style_profile.get("cellFormatRules"), dict)
            else {}
        )
        cell_format: Dict[str, Any] = {}
        if row_id and isinstance(cell_rules, dict):
            for alias in RuntimeExportService._export_key_aliases(key):
                candidate = cell_rules.get(f"{row_id}:::{alias}")
                if isinstance(candidate, dict):
                    cell_format = candidate
                    break
        border = (
            f"{max(1, int(float(report_format.get('borderWidth') or 1)))}px {report_format.get('borderStyle')} {report_format.get('borderColor') or theme.get('border') or '#d1d5db'}"
            if report_format.get("borderStyle")
            else f"1px solid {theme.get('border') or '#d1d5db'}"
        )
        style = {
            "fontFamily": font_family,
            "fontSize": font_size,
            "height": row_height,
            "color": cell_format.get("color")
            or (
                theme.get("totalText") or theme.get("text")
                if is_total_row
                else (
                    theme.get("text")
                    if is_hierarchy
                    else theme.get("textSec") or theme.get("text") or "#111827"
                )
            ),
            "background": cell_format.get("bg")
            or report_format.get("rowColor")
            or background,
            "border": border,
            "padding": (
                f"4px 10px 4px {max(10, 10 + (depth * 24))}px"
                if is_hierarchy
                else "4px 10px"
            ),
            "textAlign": "left" if is_hierarchy else "right",
            "whiteSpace": "pre" if is_hierarchy else "nowrap",
            "fontVariantNumeric": None if is_hierarchy else "tabular-nums",
            "fontWeight": (
                700
                if (
                    cell_format.get("bold") or is_total_row or report_format.get("bold")
                )
                else 600 if is_total_col else 400
            ),
            "fontStyle": "italic" if cell_format.get("italic") else None,
        }
        style.update(
            RuntimeExportService._conditional_export_style(
                key, value, row, style_profile
            )
        )
        if cell_format.get("bg"):
            style["background"] = cell_format.get("bg")
        if cell_format.get("color"):
            style["color"] = cell_format.get("color")
        return style

    @staticmethod
    def _export_header_style(
        key: str,
        header_label: str,
        style_profile: Dict[str, Any],
        state: Optional[PivotViewState] = None,
    ) -> Dict[str, Any]:
        theme = (
            style_profile.get("theme")
            if isinstance(style_profile.get("theme"), dict)
            else {}
        )
        width = RuntimeExportService._export_column_width(key, style_profile)
        is_total = str(header_label or "").startswith("Grand Total")
        style = {
            "fontFamily": style_profile.get("fontFamily")
            or "'Inter', ui-sans-serif, system-ui, sans-serif",
            "fontSize": style_profile.get("fontSize") or "13px",
            "fontWeight": 700,
            "color": (
                theme.get("totalText") or theme.get("text")
                if is_total
                else theme.get("headerText") or theme.get("text") or "#111827"
            ),
            "background": (
                theme.get("totalBgStrong")
                or theme.get("totalBg")
                or theme.get("select")
                or "#e5e7eb"
                if is_total
                else theme.get("headerBg")
                or theme.get("headerSubtleBg")
                or theme.get("surfaceBg")
                or "#f3f4f6"
            ),
            "border": f"1px solid {theme.get('border') or '#d1d5db'}",
            "padding": "6px 10px",
            "textAlign": "center",
            "verticalAlign": "middle",
            "whiteSpace": "nowrap",
            "width": f"{width}px" if width else None,
            "minWidth": f"{width}px" if width else None,
        }
        if state is not None:
            measure_config = RuntimeExportService._matching_value_config_for_export(
                key, state, style_profile
            )
            if isinstance(measure_config, dict):
                style.update(
                    RuntimeExportService._normalize_export_header_style(
                        measure_config.get("headerStyle")
                    )
                )
                style.update(
                    RuntimeExportService._normalize_export_header_style(measure_config)
                )
        style.update(
            RuntimeExportService._export_header_formatting_style(
                key, header_label, style_profile
            )
        )
        return style

    @staticmethod
    def _write_styled_html_export(
        buf: Any,
        *,
        rows: List[Dict[str, Any]],
        field_order: List[str],
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
        include_headers: bool,
    ) -> None:
        font_family = (
            style_profile.get("fontFamily") if isinstance(style_profile, dict) else None
        )
        font_size = (
            style_profile.get("fontSize") if isinstance(style_profile, dict) else None
        )
        table_style = RuntimeExportService._style_to_css(
            {
                "borderCollapse": "collapse",
                "borderSpacing": 0,
                "fontFamily": font_family
                or "'Inter', ui-sans-serif, system-ui, sans-serif",
                "fontSize": font_size or "13px",
            }
        )
        buf.write('<!DOCTYPE html><html><head><meta charset="utf-8" />')
        buf.write(
            "<style>table.pivot-export-table{border-collapse:collapse;border-spacing:0}</style>"
        )
        buf.write("</head><body>")
        buf.write(
            f'<table class="pivot-export-table" style="{html.escape(table_style, quote=True)}">'
        )
        if include_headers and field_order:
            buf.write("<tr>")
            for key in field_order:
                header_label = RuntimeExportService._export_header_for_key(
                    key, header_map, state, style_profile
                )
                css = RuntimeExportService._style_to_css(
                    RuntimeExportService._export_header_style(
                        key, header_label, style_profile, state
                    )
                )
                buf.write(
                    f'<th style="{html.escape(css, quote=True)}">{html.escape(header_label)}</th>'
                )
            buf.write("</tr>")

        for row in rows:
            buf.write("<tr>")
            for key in field_order:
                raw_value = row.get(key)
                if raw_value is None and key == "_id":
                    raw_value = row.get("hierarchy")
                header_label = RuntimeExportService._export_header_for_key(
                    key, header_map, state, style_profile
                )
                display_value = RuntimeExportService._formatted_export_value(
                    key, raw_value, row, state, style_profile
                )
                css = RuntimeExportService._style_to_css(
                    RuntimeExportService._export_cell_style(
                        key, raw_value, row, header_label, style_profile
                    )
                )
                buf.write(
                    f'<td style="{html.escape(css, quote=True)}">{html.escape(display_value)}</td>'
                )
            buf.write("</tr>")
        buf.write("</table></body></html>")

    @staticmethod
    def _resolve_export_field_order(
        rows: List[Dict[str, Any]],
        columns: List[Dict[str, Any]],
        col_ids: Any,
        state: Optional[PivotViewState] = None,
    ) -> List[str]:
        def _field_from_column_id(column_id: Any) -> Optional[str]:
            if column_id is None:
                return None
            text = str(column_id)
            return "_id" if text == "hierarchy" else text

        if isinstance(col_ids, list) and col_ids:
            ordered: List[str] = []
            seen = set()
            for column_id in col_ids:
                field = _field_from_column_id(column_id)
                if field and field not in seen:
                    ordered.append(field)
                    seen.add(field)
            return ordered

        dimension_fields = set()
        if state is not None:
            dimension_fields.update(
                str(field) for field in (state.row_fields or []) if field is not None
            )
            dimension_fields.update(
                str(field) for field in (state.col_fields or []) if field is not None
            )

        skip = {
            "_path",
            "_isTotal",
            "_has_children",
            "depth",
            "__virtualIndex",
            "__colPending",
            "_parentPath",
            "_isGrandTotal",
            "__isGrandTotal__",
        } | dimension_fields
        column_fields: List[str] = []
        for column in columns or []:
            if not isinstance(column, dict) or column.get("_isImplicitFormulaRef"):
                continue
            column_id = column.get("id") or column.get("accessorKey")
            field = _field_from_column_id(column_id)
            if (
                field
                and field not in skip
                and not str(field).startswith("_")
                and field not in column_fields
            ):
                column_fields.append(field)
        if column_fields:
            return (
                ["_id"]
                if any("_id" in row for row in rows) and "_id" not in column_fields
                else []
            ) + column_fields

        if not rows:
            return []
        first = rows[0]
        return [key for key in first if key == "_id"] + [
            key
            for key in first
            if key not in skip and key != "_id" and not key.startswith("_")
        ]

    @staticmethod
    def _build_export_header_map(
        columns: List[Dict[str, Any]],
        state: PivotViewState,
    ) -> Dict[str, str]:
        header_map: Dict[str, str] = {}
        for column in columns or []:
            if not isinstance(column, dict):
                continue
            column_id = column.get("id") or column.get("accessorKey")
            if column_id is None:
                continue
            field = "_id" if str(column_id) == "hierarchy" else str(column_id)
            header = column.get("header") or column.get("name") or column.get("label")
            if header is not None:
                header_map[field] = str(header)

        agg_labels = {
            "sum": "Sum",
            "avg": "Avg",
            "count": "Cnt",
            "min": "Min",
            "max": "Max",
            "weighted_avg": "Weighted Avg",
        }
        for config in state.val_configs or []:
            if not isinstance(config, dict):
                continue
            field = config.get("field", "")
            agg = config.get("agg", "sum")
            key = f"{field}_{agg}" if agg != "formula" else field
            if key and key not in header_map:
                label = (
                    config.get("label")
                    or f"{str(field).replace('_', ' ').title()} ({agg_labels.get(agg, agg)})"
                )
                header_map[key] = str(label)
        return header_map

    @staticmethod
    def _display_export_header(key: str, state: PivotViewState) -> str:
        if key == "_id":
            row_fields = state.row_fields or []
            return (
                "/".join(str(field).replace("_", " ").title() for field in row_fields)
                or "Row"
            )
        return str(key).replace("_", " ").title()
