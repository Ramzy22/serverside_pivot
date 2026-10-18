"""Export document formatting and file writers for pivot runtime responses."""

from __future__ import annotations

import csv
from decimal import Decimal
import html
import math
import os
import re
import tempfile
import zipfile
from typing import Any, AsyncIterable, Dict, Iterable, List, Optional, Tuple

from ..tanstack_adapter import TanStackResponse
from ..resource_governor import ResourceLease
from ..export_limits import (
    DEFAULT_EXPORT_MAX_BYTES,
    DEFAULT_EXPORT_MAX_ROWS,
    ExportLimitError,
    ExportLimits,
)
from .models import PivotViewState, first_present
# Re-export writer controls for existing runner/router import paths.
from .bounded_export import (
    ExportCancelledError,
    ExportWriteControl,
    _DiskExportRows,
    _export_checkpoint,
    _export_tempfile,
    build_paged_export as _build_paged_export,
    build_streamed_export as _build_streamed_export,
    current_export_write,
)


class RuntimeExportService:
    """Build and write table export payloads without routing requests."""

    def __init__(
        self,
        *,
        max_rows: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> None:
        self.limits = ExportLimits.from_environment(
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
        self.max_rows = self.limits.max_rows
        self.max_bytes = self.limits.max_bytes
    @staticmethod
    def normalize_export_format(value: object) -> str:
        if not isinstance(value, (str, type(None))): raise ValueError(f"Unsupported export format: {value}")
        export_fmt = "csv" if value is None else value.strip().lower()
        export_fmt = "xlsx" if export_fmt == "xls" else export_fmt
        if export_fmt not in {"csv", "tsv", "html", "xlsx"}: raise ValueError(f"Unsupported export format: {value}")
        return export_fmt
    def build_export_payload(
        self,
        response: TanStackResponse,
        state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Write an export and enforce the configured row and byte budgets."""
        response_rows = getattr(response, "data", None)
        if isinstance(response_rows, (list, _DiskExportRows)):
            # Reject a materialized allRows/hierarchy response before a derived
            # list or temporary document is allocated.
            self.limits.ensure_rows(len(response_rows))
        _export_checkpoint()
        payload = self._build_export_payload(response, state, export_request)
        rows = int(payload.get("rows") or 0)
        content_length = int(payload.get("contentLength") or 0)
        if rows <= self.max_rows and content_length <= self.max_bytes:
            return payload

        content_path = payload.get("contentPath")
        if isinstance(content_path, str):
            try:
                os.remove(content_path)
            except OSError:
                pass
        if rows > self.max_rows:
            self.limits.ensure_rows(rows)
        self.limits.ensure_bytes(content_length)

    async def build_paged_export(
        self,
        pages: AsyncIterable[TanStackResponse],
        state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Spool bounded response pages and reuse the document writers."""
        return await _build_paged_export(self, pages, state, export_request)

    async def build_streamed_export(
        self,
        batches: Iterable[Any] | AsyncIterable[Any],
        state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
        *,
        columns: Optional[List[Dict[str, Any]]] = None,
        export_lease: Optional[ResourceLease] = None,
    ) -> Dict[str, Any]:
        """Write native record batches without retaining the full result."""
        return await _build_streamed_export(
            self, batches, state, export_request,
            columns=columns, export_lease=export_lease,
        )

    @staticmethod
    def _build_export_payload(
        response: TanStackResponse,
        state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        request_payload = export_request if isinstance(export_request, dict) else {}
        export_fmt = RuntimeExportService.normalize_export_format(request_payload.get("format"))
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
            filename = f"pivot_export_{part_label}.{ext}" if part_label else f"pivot_export.{ext}"
        include_headers = bool(first_present(request_payload, "includeHeaders", "include_headers", default=True))

        # The TanStack response already owns an in-memory row window.  Reuse
        # that list for the common hierarchy/outline export path rather than
        # allocating a second full list before writing the file. Tabular
        # projection and subtotal filtering intentionally create derived rows
        # only when their presentation contract requires it.
        response_rows = response.data if isinstance(response.data, (list, _DiskExportRows)) else []
        rows = (
            response_rows
            if isinstance(response_rows, _DiskExportRows) or all(isinstance(row, dict) for row in response_rows)
            else [row for row in response_rows if isinstance(row, dict)]
        )
        is_tabular_without_subtotals = (
            str(getattr(state, "layout_mode", "") or "").strip().lower() == "tabular"
            and not bool(getattr(state, "show_subtotals", False))
        )
        if is_tabular_without_subtotals and not isinstance(rows, _DiskExportRows):
            # The normal tabular query is flat and contains no subtotal rows.
            # Retain this export-side guard for responses produced by custom
            # adapters or stale hierarchy caches: the toggle must be honored
            # at the final output boundary as well.
            rows = [
                row for row in rows
                if not bool(row.get("_isTabularSubtotal") or row.get("_isSubtotalFooter"))
            ]
        # The hierarchy column is a presentation-only shorthand.  In tabular
        # mode the grid instead renders one physical column per row field and
        # repeats the ancestor labels on every leaf row.  Prepare the same row
        # shape before resolving fields so CSV, HTML/copy, and XLSX all export
        # what the user sees rather than a single indented hierarchy column.
        if not isinstance(rows, _DiskExportRows):
            rows = RuntimeExportService._project_export_rows_for_layout(rows, state)
        field_order = RuntimeExportService._resolve_export_field_order(
            rows,
            response.columns or [],
            request_payload.get("colIds"),
            state,
        )
        header_map = RuntimeExportService._build_export_header_map(response.columns or [], state)
        style_profile = (
            request_payload.get("style")
            if isinstance(request_payload.get("style"), dict)
            else request_payload.get("exportStyle")
            if isinstance(request_payload.get("exportStyle"), dict)
            else {}
        )

        def _cell(key: str, value: Any) -> Any:
            if value is None or (
                isinstance(value, (float, Decimal))
                and not math.isfinite(float(value))
            ):
                return ""
            if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
                # A leading apostrophe is Excel's established literal-text
                # marker and prevents CSV values from executing as formulas.
                return "'" + value
            return value

        fd, content_path = _export_tempfile(prefix="pivot_export_", suffix=f".{ext}")
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
                        # Excel uses the UTF-8 BOM to detect non-ASCII CSV/TSV
                        # text reliably instead of falling back to a local code page.
                        buf.write("\ufeff")
                        writer = csv.writer(buf, delimiter=delimiter)
                        if include_headers and field_order:
                            writer.writerow([
                                _cell(key, RuntimeExportService._export_header_for_key(key, header_map, state, style_profile))
                                for key in field_order
                            ])
                        for row_index, row in enumerate(rows, 1):
                            _export_checkpoint()
                            writer.writerow([_cell(key, row.get(key)) for key in field_order])
                            if row_index % 128 == 0:
                                buf.flush()
                                _export_checkpoint(row_index, os.path.getsize(content_path))
            content_length = os.path.getsize(content_path)
            _export_checkpoint(len(rows), content_length)
        except BaseException:
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
            "contentType": (
                f"{content_type};charset=utf-8"
                if export_fmt in {"csv", "tsv", "html"}
                else content_type
            ),
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
        spreadsheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

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
            return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool) and math.isfinite(float(value))

        def _css_number(value: Any, default: float) -> float:
            match = re.search(r"-?\d+(?:\.\d+)?", str(value or ""))
            return float(match.group(0)) if match else default

        def _argb(value: Any, default: str = "FF000000") -> str:
            text = str(value or "").strip()
            hex_match = re.fullmatch(r"#([0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})", text)
            if hex_match:
                digits = hex_match.group(1)
                if len(digits) == 3:
                    digits = "".join(char * 2 for char in digits)
                if len(digits) == 6:
                    digits = "FF" + digits
                return digits.upper()
            rgb_match = re.match(
                r"rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*([\d.]+))?\s*\)",
                text,
                re.IGNORECASE,
            )
            if rgb_match:
                red, green, blue = (max(0, min(255, int(rgb_match.group(i)))) for i in range(1, 4))
                alpha = 255
                if rgb_match.group(4) is not None:
                    alpha = max(0, min(255, round(float(rgb_match.group(4)) * 255)))
                return f"{alpha:02X}{red:02X}{green:02X}{blue:02X}"
            return default

        fonts: List[str] = [
            '<font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>'
        ]
        fills: List[str] = [
            '<fill><patternFill patternType="none"/></fill>',
            '<fill><patternFill patternType="gray125"/></fill>',
        ]
        borders: List[str] = [
            '<border><left/><right/><top/><bottom/><diagonal/></border>'
        ]
        num_fmts: List[Tuple[int, str]] = []
        cell_xfs: List[str] = [
            '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        ]
        font_ids: Dict[Tuple[Any, ...], int] = {}
        fill_ids: Dict[str, int] = {}
        border_ids: Dict[Tuple[Any, ...], int] = {}
        num_fmt_ids: Dict[str, int] = {}
        xf_ids: Dict[Tuple[Any, ...], int] = {}

        def _font_id(style: Dict[str, Any]) -> int:
            family = str(style.get("fontFamily") or "Calibri").split(",", 1)[0].strip(" '\"") or "Calibri"
            size = max(1.0, _css_number(style.get("fontSize"), 11.0) * 0.75)
            weight = str(style.get("fontWeight") or "").lower()
            bold = weight in {"bold", "bolder"} or (_css_number(weight, 0) >= 600)
            italic = str(style.get("fontStyle") or "").lower() == "italic"
            underline = "underline" in str(style.get("textDecoration") or "").lower()
            color = _argb(style.get("color"), "FF000000")
            key = (family, round(size, 2), bold, italic, underline, color)
            if key in font_ids:
                return font_ids[key]
            xml = ["<font>"]
            if bold:
                xml.append("<b/>")
            if italic:
                xml.append("<i/>")
            if underline:
                xml.append("<u/>")
            xml.extend(
                [
                    f'<sz val="{size:g}"/>',
                    f'<color rgb="{color}"/>',
                    f'<name val="{_xml_text(family)}"/>',
                    '<family val="2"/>',
                    "</font>",
                ]
            )
            font_ids[key] = len(fonts)
            fonts.append("".join(xml))
            return font_ids[key]

        def _fill_id(style: Dict[str, Any]) -> int:
            color = _argb(style.get("background") or style.get("backgroundColor"), "FFFFFFFF")
            if color in fill_ids:
                return fill_ids[color]
            fill_ids[color] = len(fills)
            fills.append(
                '<fill><patternFill patternType="solid">'
                f'<fgColor rgb="{color}"/><bgColor indexed="64"/>'
                '</patternFill></fill>'
            )
            return fill_ids[color]

        def _border_id(style: Dict[str, Any]) -> int:
            generic = style.get("border") or ""
            sides = []
            for side in ("left", "right", "top", "bottom"):
                raw = style.get(f"border{side.title()}") or generic
                text = str(raw or "")
                width = max(0.0, _css_number(text, 0.0))
                if "double" in text:
                    line_style = "double"
                elif "dotted" in text:
                    line_style = "dotted"
                elif "dashed" in text:
                    line_style = "dashed"
                elif width >= 3:
                    line_style = "thick"
                elif width >= 2:
                    line_style = "medium"
                elif width > 0 or text:
                    line_style = "thin"
                else:
                    line_style = None
                color_match = re.search(r"#[0-9a-fA-F]{3,8}|rgba?\([^)]*\)", text)
                color = _argb(color_match.group(0) if color_match else style.get("borderColor"), "FFD1D5DB")
                sides.append((line_style, color))
            key = tuple(sides)
            if key in border_ids:
                return border_ids[key]
            xml = ["<border>"]
            for side, (line_style, color) in zip(("left", "right", "top", "bottom"), sides):
                if line_style:
                    xml.append(f'<{side} style="{line_style}"><color rgb="{color}"/></{side}>')
                else:
                    xml.append(f"<{side}/>")
            xml.extend(["<diagonal/>", "</border>"])
            border_ids[key] = len(borders)
            borders.append("".join(xml))
            return border_ids[key]

        def _number_format_code(key: str) -> Optional[str]:
            decimal_overrides = style_profile.get("columnDecimalOverrides") if isinstance(style_profile, dict) else None
            format_overrides = style_profile.get("columnFormatOverrides") if isinstance(style_profile, dict) else None
            decimal_places = style_profile.get("decimalPlaces") if isinstance(style_profile, dict) else None
            value_format = style_profile.get("defaultValueFormat") if isinstance(style_profile, dict) else None
            config = RuntimeExportService._matching_value_config_for_export(key, state, style_profile)
            for alias in RuntimeExportService._export_key_aliases(key):
                if isinstance(decimal_overrides, dict) and alias in decimal_overrides:
                    decimal_places = decimal_overrides[alias]
                if isinstance(format_overrides, dict) and alias in format_overrides:
                    value_format = format_overrides[alias]
            if not value_format and isinstance(config, dict):
                value_format = config.get("format")
            fmt_text = str(value_format or "").strip()
            if decimal_places is None and not fmt_text:
                return None
            try:
                decimals = max(0, int(decimal_places)) if decimal_places is not None else None
            except (TypeError, ValueError):
                decimals = None
            if fmt_text.startswith("fixed:") and decimals is None:
                try:
                    decimals = max(0, int(fmt_text.split(":", 1)[1]))
                except (IndexError, TypeError, ValueError):
                    decimals = 2
            if decimals is None:
                decimals = 2 if fmt_text in {"currency", "accounting", "percent", "scientific"} or fmt_text.startswith(("currency:", "accounting:")) else 0
            decimal_suffix = "." + ("0" * decimals) if decimals else ""
            if fmt_text == "percent":
                return f"0{decimal_suffix}%"
            if fmt_text == "scientific":
                return f"0{decimal_suffix}E+00"
            if fmt_text == "currency" or fmt_text.startswith("currency:"):
                symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
                prefix = f'"{symbol.replace(chr(34), chr(34) * 2)}"'
                return f"{prefix}#,##0{decimal_suffix};[Red]-{prefix}#,##0{decimal_suffix}"
            if fmt_text == "accounting" or fmt_text.startswith("accounting:"):
                symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
                prefix = f'"{symbol.replace(chr(34), chr(34) * 2)}"'
                return f"{prefix}#,##0{decimal_suffix};[Red]({prefix}#,##0{decimal_suffix})"
            return f"#,##0{decimal_suffix}"

        def _num_fmt_id(code: Optional[str]) -> int:
            if not code:
                return 0
            if code in num_fmt_ids:
                return num_fmt_ids[code]
            fmt_id = 164 + len(num_fmts)
            num_fmt_ids[code] = fmt_id
            num_fmts.append((fmt_id, code))
            return fmt_id

        def _xf_id(style: Dict[str, Any], number_format: Optional[str] = None) -> int:
            font_id = _font_id(style)
            fill_id = _fill_id(style)
            border_id = _border_id(style)
            num_fmt_id = _num_fmt_id(number_format)
            horizontal = str(style.get("textAlign") or "").lower()
            if horizontal not in {"left", "center", "right", "fill", "justify"}:
                horizontal = "general"
            vertical = str(style.get("verticalAlign") or "").lower()
            if vertical not in {"top", "center", "bottom", "justify"}:
                vertical = "center"
            wrap = str(style.get("whiteSpace") or "").lower() not in {"nowrap", "pre"}
            key = (font_id, fill_id, border_id, num_fmt_id, horizontal, vertical, wrap)
            if key in xf_ids:
                return xf_ids[key]
            alignment = (
                f'<alignment horizontal="{horizontal}" vertical="{vertical}"'
                + (' wrapText="1"' if wrap else "")
                + "/>"
            )
            xml = (
                f'<xf numFmtId="{num_fmt_id}" fontId="{font_id}" fillId="{fill_id}" borderId="{border_id}" xfId="0" '
                'applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"'
                + (' applyNumberFormat="1"' if num_fmt_id else "")
                + f">{alignment}</xf>"
            )
            xf_ids[key] = len(cell_xfs)
            cell_xfs.append(xml)
            return xf_ids[key]

        def _cell_xml(row_index: int, col_index: int, value: Any, style_id: int) -> str:
            ref = f"{_col_name(col_index)}{row_index}"
            style_attr = f' s="{style_id}"' if style_id else ""
            if value is None or (
                isinstance(value, (float, Decimal))
                and not math.isfinite(float(value))
            ):
                return f'<c r="{ref}"{style_attr}/>'
            if _is_number(value):
                return f'<c r="{ref}"{style_attr}><v>{html.escape(str(value), quote=True)}</v></c>'
            if isinstance(value, bool):
                return f'<c r="{ref}"{style_attr} t="b"><v>{1 if value else 0}</v></c>'
            text_value = str(value)
            if text_value.startswith(("=", "+", "-", "@")):
                text_value = "'" + text_value
            return f'<c r="{ref}"{style_attr} t="inlineStr"><is><t>{_xml_text(text_value)}</t></is></c>'

        def _export_field_from_column_id(column_id: Any) -> str:
            return "_id" if str(column_id) == "hierarchy" else str(column_id)

        def _merged_header_rows() -> List[List[Dict[str, Any]]]:
            """Normalize client header ancestry into safe worksheet merge cells."""
            raw_rows = style_profile.get("mergedHeaderRows") if isinstance(style_profile, dict) else None
            if not isinstance(raw_rows, list) or not raw_rows:
                return []
            field_indexes = {field: index for index, field in enumerate(field_order)}
            normalized: List[List[Dict[str, Any]]] = []
            total_rows = len(raw_rows)
            for row_index, raw_row in enumerate(raw_rows):
                if not isinstance(raw_row, list):
                    return []
                cells: List[Dict[str, Any]] = []
                for raw_cell in raw_row:
                    if not isinstance(raw_cell, dict):
                        continue
                    raw_column_ids = raw_cell.get("columnIds")
                    if not isinstance(raw_column_ids, list):
                        raw_column_ids = [raw_cell.get("styleColumnId")]
                    indexes = sorted({
                        field_indexes[field]
                        for column_id in raw_column_ids
                        for field in [_export_field_from_column_id(column_id)]
                        if field in field_indexes
                    })
                    if not indexes:
                        continue
                    start, end = indexes[0], indexes[-1]
                    # A merge cannot skip a field that was selected for this
                    # export part. Falling back to a flat header is safer than
                    # emitting a misleading, non-contiguous group.
                    if indexes != list(range(start, end + 1)):
                        return []
                    try:
                        row_span = int(raw_cell.get("rowSpan") or 1)
                    except (TypeError, ValueError):
                        row_span = 1
                    row_span = max(1, min(row_span, total_rows - row_index))
                    style_key = _export_field_from_column_id(raw_cell.get("styleColumnId") or raw_column_ids[0])
                    label = raw_cell.get("label")
                    cells.append({
                        "label": str(label) if label not in (None, "") else RuntimeExportService._export_header_for_key(
                            field_order[start], header_map, state, style_profile
                        ),
                        "start": start,
                        "end": end,
                        "rowSpan": row_span,
                        "styleKey": style_key if style_key in field_indexes else field_order[start],
                    })
                if not cells:
                    return []
                normalized.append(sorted(cells, key=lambda cell: cell["start"]))
            return normalized

        # Spool worksheet rows to disk; styles and merges remain small metadata.
        control = current_export_write.get()
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8", newline="",
                                    dir=control.directory if control else None) as sheet_rows:
            merge_ranges: List[str] = []
            excel_row = 1
            if include_headers and field_order:
                merged_header_rows = _merged_header_rows()
                if merged_header_rows:
                    for header_row_index, header_row in enumerate(merged_header_rows):
                        cells = "".join(
                            _cell_xml(
                                excel_row,
                                cell["start"],
                                cell["label"],
                                _xf_id(RuntimeExportService._export_header_style(
                                    cell["styleKey"], cell["label"], style_profile, state
                                )),
                            )
                            for cell in header_row
                        )
                        sheet_rows.write(f'<row r="{excel_row}" customHeight="1" ht="24">{cells}</row>')
                        for cell in header_row:
                            if cell["end"] > cell["start"] or cell["rowSpan"] > 1:
                                merge_ranges.append(
                                    f'{_col_name(cell["start"])}{excel_row}:{_col_name(cell["end"])}{excel_row + cell["rowSpan"] - 1}'
                                )
                        excel_row += 1
                else:
                    headers = [RuntimeExportService._export_header_for_key(key, header_map, state, style_profile) for key in field_order]
                    cells = "".join(
                        _cell_xml(
                            excel_row,
                            col_index,
                            header,
                            _xf_id(RuntimeExportService._export_header_style(key, header, style_profile, state)),
                        )
                        for col_index, (key, header) in enumerate(zip(field_order, headers))
                    )
                    sheet_rows.write(f'<row r="{excel_row}" customHeight="1" ht="24">{cells}</row>')
                    excel_row += 1
            for row in rows:
                _export_checkpoint()
                cells = []
                for col_index, key in enumerate(field_order):
                    raw_value = row.get(key)
                    header_label = RuntimeExportService._export_header_for_key(key, header_map, state, style_profile)
                    style = RuntimeExportService._export_cell_style(key, raw_value, row, header_label, style_profile)
                    number_format = _number_format_code(key) if _is_number(raw_value) else None
                    cells.append(_cell_xml(excel_row, col_index, raw_value, _xf_id(style, number_format)))
                row_height = max(1.0, _css_number(style_profile.get("rowHeight"), 20.0) * 0.75)
                sheet_rows.write(f'<row r="{excel_row}" customHeight="1" ht="{row_height:g}">{"".join(cells)}</row>')
                excel_row += 1

            column_xml = []
            for index, key in enumerate(field_order, start=1):
                pixel_width = RuntimeExportService._export_column_width(key, style_profile)
                if pixel_width:
                    excel_width = max(1.0, min(255.0, (pixel_width - 5) / 7))
                    column_xml.append(f'<col min="{index}" max="{index}" width="{excel_width:.2f}" customWidth="1"/>')

            worksheet_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                f'{"<cols>" + "".join(column_xml) + "</cols>" if column_xml else ""}'
                '<sheetData>__PIVOT_ROWS__</sheetData>'
                + (f'<mergeCells count="{len(merge_ranges)}">' + "".join(
                    f'<mergeCell ref="{merge_range}"/>' for merge_range in merge_ranges
                ) + '</mergeCells>' if merge_ranges else '')
                + '</worksheet>'
            )
            num_fmts_xml = (
                f'<numFmts count="{len(num_fmts)}">'
                + "".join(f'<numFmt numFmtId="{fmt_id}" formatCode="{_xml_text(code)}"/>' for fmt_id, code in num_fmts)
                + "</numFmts>"
                if num_fmts
                else ""
            )
            styles_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                f'<styleSheet xmlns="{spreadsheet_ns}">'
                f'{num_fmts_xml}'
                f'<fonts count="{len(fonts)}">{"".join(fonts)}</fonts>'
                f'<fills count="{len(fills)}">{"".join(fills)}</fills>'
                f'<borders count="{len(borders)}">{"".join(borders)}</borders>'
                '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
                f'<cellXfs count="{len(cell_xfs)}">{"".join(cell_xfs)}</cellXfs>'
                '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
                '<dxfs count="0"/>'
                '</styleSheet>'
            )
            workbook_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                '<sheets><sheet name="Pivot" sheetId="1" r:id="rId1"/></sheets>'
                '</workbook>'
            )
            content_types_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
                '</Types>'
            )
            root_rels_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
                '</Relationships>'
            )
            workbook_rels_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
                '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
                '</Relationships>'
            )

            with zipfile.ZipFile(content_path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
                _export_checkpoint()
                workbook.writestr("[Content_Types].xml", content_types_xml)
                workbook.writestr("_rels/.rels", root_rels_xml)
                workbook.writestr("xl/workbook.xml", workbook_xml)
                workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
                before_rows, after_rows = worksheet_xml.split("__PIVOT_ROWS__")
                sheet_rows.seek(0)
                with workbook.open("xl/worksheets/sheet1.xml", "w") as worksheet:
                    worksheet.write(before_rows.encode("utf-8"))
                    while chunk := sheet_rows.read(64 * 1024):
                        _export_checkpoint()
                        worksheet.write(chunk.encode("utf-8"))
                    worksheet.write(after_rows.encode("utf-8"))
                workbook.writestr("xl/styles.xml", styles_xml)

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
        labels = style_profile.get("headerLabels") if isinstance(style_profile, dict) else None
        if isinstance(labels, dict):
            for alias in RuntimeExportService._export_key_aliases(key):
                label = labels.get(alias)
                if label is not None:
                    return str(label)
        return header_map.get(key, RuntimeExportService._display_export_header(key, state))

    @staticmethod
    def _export_column_width(key: str, style_profile: Dict[str, Any]) -> Optional[int]:
        widths = style_profile.get("columnWidths") if isinstance(style_profile, dict) else None
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
    def _export_header_formatting_style(key: str, header_label: str, style_profile: Dict[str, Any]) -> Dict[str, Any]:
        formatting = style_profile.get("headerFormatting") if isinstance(style_profile, dict) else None
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

    @staticmethod
    def _css_property_name(name: str) -> str:
        return re.sub(r"([A-Z])", lambda match: "-" + match.group(1).lower(), str(name or ""))

    @staticmethod
    def _style_to_css(style: Dict[str, Any]) -> str:
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
            parts.append(f"{RuntimeExportService._css_property_name(str(prop))}:{value}")
        return ";".join(parts)

    @staticmethod
    def _format_export_number(value: float, fmt: Any, decimal_places: Any, group_separator: Any) -> str:
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
    def _matching_value_config_for_export(key: str, state: PivotViewState, style_profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        configs = style_profile.get("valConfigs") if isinstance(style_profile.get("valConfigs"), list) else state.val_configs
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
            decimal_overrides = style_profile.get("columnDecimalOverrides") if isinstance(style_profile, dict) else None
            format_overrides = style_profile.get("columnFormatOverrides") if isinstance(style_profile, dict) else None
            group_overrides = style_profile.get("columnGroupSeparatorOverrides") if isinstance(style_profile, dict) else None
            decimal_places = style_profile.get("decimalPlaces") if isinstance(style_profile, dict) else None
            default_format = style_profile.get("defaultValueFormat") if isinstance(style_profile, dict) else None
            group_separator = style_profile.get("numberGroupSeparator") if isinstance(style_profile, dict) else None
            config = RuntimeExportService._matching_value_config_for_export(key, state, style_profile)
            for alias in RuntimeExportService._export_key_aliases(key):
                if isinstance(decimal_overrides, dict) and alias in decimal_overrides:
                    decimal_places = decimal_overrides[alias]
                if isinstance(format_overrides, dict) and alias in format_overrides:
                    default_format = format_overrides[alias]
                if isinstance(group_overrides, dict) and alias in group_overrides:
                    group_separator = group_overrides[alias]
            if (not default_format) and isinstance(config, dict):
                default_format = config.get("format") or default_format
            return RuntimeExportService._format_export_number(float(value), default_format, decimal_places, group_separator)
        if isinstance(value, (list, tuple)):
            return f"{len(value)} items"
        if isinstance(value, dict):
            return str(value)
        return str(value)

    @staticmethod
    def _export_condition_matches(condition: Any, left_value: float, right_value: Any) -> bool:
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
    def _conditional_export_style(key: str, value: Any, row: Dict[str, Any], style_profile: Dict[str, Any]) -> Dict[str, Any]:
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
                if column and str(column) not in RuntimeExportService._export_key_aliases(key):
                    continue
                if RuntimeExportService._export_condition_matches(rule.get("condition"), numeric, rule.get("value")):
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

    @staticmethod
    def _export_cell_style(
        key: str,
        value: Any,
        row: Dict[str, Any],
        header_label: str,
        style_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        theme = style_profile.get("theme") if isinstance(style_profile.get("theme"), dict) else {}
        font_family = style_profile.get("fontFamily") or "'Inter', ui-sans-serif, system-ui, sans-serif"
        font_size = style_profile.get("fontSize") or "13px"
        row_height = style_profile.get("rowHeight")
        is_hierarchy = key == "_id"
        is_total_row = bool(row.get("_isTotal") or row.get("__isGrandTotal__") or row.get("_path") == "__grand_total__" or row.get("_id") == "Grand Total")
        is_total_col = str(header_label or "").startswith("Grand Total")
        background = (
            theme.get("totalBgStrong") or theme.get("totalBg") or theme.get("select") or theme.get("background") or "#fff"
            if is_total_row
            else theme.get("totalBg") or theme.get("select") or theme.get("background") or "#fff"
            if is_total_col
            else theme.get("hierarchyBg") or theme.get("surfaceInset") or theme.get("surfaceBg") or theme.get("background") or "#fff"
            if is_hierarchy
            else theme.get("surfaceBg") or theme.get("background") or "#fff"
        )
        try:
            depth = max(0, int(row.get("depth") or 0))
        except (TypeError, ValueError):
            depth = 0
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
            "color": cell_format.get("color") or (theme.get("totalText") or theme.get("text") if is_total_row else theme.get("text") if is_hierarchy else theme.get("textSec") or theme.get("text") or "#111827"),
            "background": cell_format.get("bg") or report_format.get("rowColor") or background,
            "border": border,
            "padding": f"4px 10px 4px {max(10, 10 + (depth * 24))}px" if is_hierarchy else "4px 10px",
            "textAlign": "left" if is_hierarchy else "right",
            "whiteSpace": "pre" if is_hierarchy else "nowrap",
            "fontVariantNumeric": None if is_hierarchy else "tabular-nums",
            "fontWeight": 700 if (cell_format.get("bold") or is_total_row or report_format.get("bold")) else 600 if is_total_col else 400,
            "fontStyle": "italic" if cell_format.get("italic") else None,
        }
        style.update(RuntimeExportService._conditional_export_style(key, value, row, style_profile))
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
        theme = style_profile.get("theme") if isinstance(style_profile.get("theme"), dict) else {}
        width = RuntimeExportService._export_column_width(key, style_profile)
        is_total = str(header_label or "").startswith("Grand Total")
        style = {
            "fontFamily": style_profile.get("fontFamily") or "'Inter', ui-sans-serif, system-ui, sans-serif",
            "fontSize": style_profile.get("fontSize") or "13px",
            "fontWeight": 700,
            "color": theme.get("totalText") or theme.get("text") if is_total else theme.get("headerText") or theme.get("text") or "#111827",
            "background": (
                theme.get("totalBgStrong") or theme.get("totalBg") or theme.get("select") or "#e5e7eb"
                if is_total
                else theme.get("headerBg") or theme.get("headerSubtleBg") or theme.get("surfaceBg") or "#f3f4f6"
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
            measure_config = RuntimeExportService._matching_value_config_for_export(key, state, style_profile)
            if isinstance(measure_config, dict):
                style.update(RuntimeExportService._normalize_export_header_style(measure_config.get("headerStyle")))
                style.update(RuntimeExportService._normalize_export_header_style(measure_config))
        style.update(RuntimeExportService._export_header_formatting_style(key, header_label, style_profile))
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
        font_family = style_profile.get("fontFamily") if isinstance(style_profile, dict) else None
        font_size = style_profile.get("fontSize") if isinstance(style_profile, dict) else None
        table_style = RuntimeExportService._style_to_css({
            "borderCollapse": "collapse",
            "borderSpacing": 0,
            "fontFamily": font_family or "'Inter', ui-sans-serif, system-ui, sans-serif",
            "fontSize": font_size or "13px",
        })
        buf.write("<!DOCTYPE html><html><head><meta charset=\"utf-8\" />")
        buf.write("<style>table.pivot-export-table{border-collapse:collapse;border-spacing:0}</style>")
        buf.write("</head><body>")
        buf.write(f"<table class=\"pivot-export-table\" style=\"{html.escape(table_style, quote=True)}\">")
        if include_headers and field_order:
            # Clipboard consumers do not all infer a header row from bare
            # ``<th>`` cells in the first table row.  Keep the header in an
            # explicit table head so rich pastes and the text/plain fallback
            # agree on the first, labelled row.
            buf.write("<thead><tr>")
            for key in field_order:
                header_label = RuntimeExportService._export_header_for_key(key, header_map, state, style_profile)
                css = RuntimeExportService._style_to_css(RuntimeExportService._export_header_style(key, header_label, style_profile, state))
                buf.write(f"<th style=\"{html.escape(css, quote=True)}\">{html.escape(header_label)}</th>")
            buf.write("</tr></thead>")

        buf.write("<tbody>")
        for row in rows:
            buf.write("<tr>")
            for key in field_order:
                raw_value = row.get(key)
                if raw_value is None and key == "_id":
                    raw_value = row.get("hierarchy")
                header_label = RuntimeExportService._export_header_for_key(key, header_map, state, style_profile)
                display_value = RuntimeExportService._formatted_export_value(key, raw_value, row, state, style_profile)
                css = RuntimeExportService._style_to_css(RuntimeExportService._export_cell_style(key, raw_value, row, header_label, style_profile))
                buf.write(f"<td style=\"{html.escape(css, quote=True)}\">{html.escape(display_value)}</td>")
            buf.write("</tr>")
        buf.write("</tbody></table></body></html>")

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

        is_tabular_layout = str(getattr(state, "layout_mode", "") or "").strip().lower() == "tabular"
        tabular_row_fields = [
            str(field)
            for field in (getattr(state, "row_fields", None) or [])
            if field is not None and str(field)
        ] if is_tabular_layout and state is not None else []

        if isinstance(col_ids, list) and col_ids:
            ordered: List[str] = []
            seen = set()
            for column_id in col_ids:
                field = _field_from_column_id(column_id)
                # Older server-side clients can still send the hierarchy
                # placeholder.  Expand it into tabular's real row columns.
                if is_tabular_layout and field == "_id":
                    for row_field in tabular_row_fields:
                        if row_field not in seen:
                            ordered.append(row_field)
                            seen.add(row_field)
                    continue
                if field and field not in seen:
                    ordered.append(field)
                    seen.add(field)
            return ordered

        dimension_fields = set()
        if state is not None:
            dimension_fields.update(str(field) for field in (state.row_fields or []) if field is not None)
            dimension_fields.update(str(field) for field in (state.col_fields or []) if field is not None)

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
            if field and field not in skip and not str(field).startswith("_") and field not in column_fields:
                column_fields.append(field)
        if column_fields:
            if is_tabular_layout:
                return tabular_row_fields + [
                    field for field in column_fields if field not in tabular_row_fields
                ]
            return (["_id"] if any("_id" in row for row in rows) and "_id" not in column_fields else []) + column_fields

        if not rows:
            return []
        first = rows[0]
        if is_tabular_layout:
            return tabular_row_fields + [
                key for key in first
                if key not in skip and key != "_id" and not key.startswith("_") and key not in tabular_row_fields
            ]
        return (
            [key for key in first if key == "_id"]
            + [key for key in first if key not in skip and key != "_id" and not key.startswith("_")]
        )

    @staticmethod
    def _project_export_rows_for_layout(
        rows: List[Dict[str, Any]],
        state: PivotViewState,
    ) -> List[Dict[str, Any]]:
        """Return export rows shaped like the active layout's visible grid."""
        if str(getattr(state, "layout_mode", "") or "").strip().lower() != "tabular":
            return rows

        row_fields = [str(field) for field in (state.row_fields or []) if field is not None and str(field)]
        if not row_fields:
            return rows

        def _is_blank(value: Any) -> bool:
            return value is None or value == "" or (
                isinstance(value, (float, Decimal)) and not math.isfinite(float(value))
            )

        projected_rows: List[Dict[str, Any]] = []
        for source_row in rows:
            row = dict(source_row)
            is_total = bool(
                row.get("_isTotal")
                or row.get("__isGrandTotal__")
                or row.get("_path") == "__grand_total__"
                or row.get("_id") == "Grand Total"
            )
            if is_total:
                # Tabular grid shows the grand-total label only in its first
                # dimension column, with the remaining dimensions empty.
                if _is_blank(row.get(row_fields[0])):
                    row[row_fields[0]] = row.get("_id") or "Grand Total"
                for field in row_fields[1:]:
                    row[field] = None
                projected_rows.append(row)
                continue

            path_parts = (
                str(row.get("_path")).split("|||")
                if isinstance(row.get("_path"), str) and row.get("_path")
                else []
            )
            path_fields = row.get("_pathFields") if isinstance(row.get("_pathFields"), list) else []
            for index, field in enumerate(row_fields):
                if not _is_blank(row.get(field)):
                    continue
                path_index = path_fields.index(field) if field in path_fields else index
                if 0 <= path_index < len(path_parts):
                    row[field] = path_parts[path_index]
                elif index == 0 and not _is_blank(row.get("_id")):
                    # Some lightweight/custom adapters expose only the
                    # hierarchy display value.  It is the first tabular
                    # dimension when no full row path is available.
                    row[field] = row.get("_id")
            projected_rows.append(row)
        return projected_rows

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
                label = config.get("label") or f"{str(field).replace('_', ' ').title()} ({agg_labels.get(agg, agg)})"
                header_map[key] = str(label)
        return header_map

    @staticmethod
    def _display_export_header(key: str, state: PivotViewState) -> str:
        if key == "_id":
            row_fields = state.row_fields or []
            return "/".join(str(field).replace("_", " ").title() for field in row_fields) or "Row"
        return str(key).replace("_", " ").title()
