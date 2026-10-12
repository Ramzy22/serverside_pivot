import { T as nt, U as rt, W as ot, X as z, Y as lt, j as n } from "./index-DyLbfkYK.js";
import { R as d } from "./charts-DthW1Q8W.js";
function st({ modal: S, theme: e }) {
  const a = Array.isArray(S && S.points) ? S.points : [], v = S && S.sparklineConfig ? S.sparklineConfig : {}, s = ["line", "area", "column", "bar"].includes(v.type) ? v.type : "line", h = 640, i = 300, o = 46, [R, X] = d.useState(null), [Y, j] = d.useState(null);
  d.useEffect(() => {
    X(null), j(null);
  }, [a.length, s]);
  const x = d.useMemo(() => {
    const t = Math.max(0, a.length - 1);
    if (!Y || a.length === 0) return { start: 0, end: t };
    const r = Math.max(0, Math.min(t, Math.floor(Number(Y.start) || 0))), c = Math.max(r, Math.min(t, Math.floor(Number(Y.end) || t)));
    return { start: r, end: c };
  }, [a.length, Y]), f = d.useMemo(
    () => a.slice(x.start, x.end + 1),
    [x, a]
  ), H = f.length, M = a.length > 4, $ = M && H > 4, k = M && (x.start > 0 || x.end < a.length - 1), y = d.useMemo(
    () => nt({ points: f, type: s, width: h, height: i, padding: o }),
    [f, s]
  ), I = rt(f), b = d.useMemo(
    () => ot(I, s),
    [I, s]
  ), m = d.useMemo(() => s === "bar" ? y.bars.map((t, r) => ({
    ...t,
    pointIndex: r,
    fullIndex: x.start + r,
    markerX: Number(t.value) >= 0 ? t.x + t.width : t.x,
    markerY: t.y + t.height / 2
  })) : s === "column" ? y.bars.map((t, r) => ({
    ...t,
    pointIndex: r,
    fullIndex: x.start + r,
    markerX: t.x + t.width / 2,
    markerY: Number(t.value) >= 0 ? t.y : t.y + t.height
  })) : y.points.map((t, r) => ({
    ...t,
    pointIndex: r,
    fullIndex: x.start + r,
    markerX: t.x,
    markerY: t.y
  })), [y.bars, y.points, x.start, s]), B = d.useMemo(() => Array.from({ length: 5 }, (r, c) => {
    const u = c / 4;
    return {
      value: b.max - (b.max - b.min) * u,
      y: o + (i - o * 2) * u
    };
  }), [i, o, b]), _ = d.useMemo(() => {
    if (!Array.isArray(f) || f.length === 0) return [];
    const t = f.length - 1;
    return Array.from(/* @__PURE__ */ new Set([
      0,
      Math.floor(t * 0.25),
      Math.floor(t * 0.5),
      Math.floor(t * 0.75),
      t
    ])).map((c) => {
      const u = f[c], p = m.find((g) => g.pointIndex === c);
      return !u || !p ? null : { index: c, label: u.label, x: p.markerX };
    }).filter(Boolean);
  }, [m, f]), A = v.color || e.primary || "#2563EB", U = v.positiveColor || (e.isDark ? "#86EFAC" : "#2F855A"), q = v.negativeColor || (e.isDark ? "#FDA4AF" : "#B91C1C"), J = I.length ? z(Math.min(...I)) : "", K = I.length ? z(Math.max(...I)) : "", T = f[0], V = f[f.length - 1], P = b.min < 0 && b.max > 0 ? lt(0, b.min, b.max, o, i - o) : null, l = R !== null && m[R] ? m[R] : null, E = 172, Z = 54, L = l ? Math.max(10, Math.min(h - E - 10, l.markerX + (l.markerX > h * 0.66 ? -E - 14 : 14))) : 0, N = l ? Math.max(10, Math.min(i - Z - 10, l.markerY + (l.markerY > i * 0.55 ? -Z - 14 : 14))) : 0, Q = d.useCallback((t) => {
    if (!m.length) return;
    const r = t.currentTarget.getBoundingClientRect(), c = (t.clientX - r.left) / Math.max(r.width, 1) * h, u = (t.clientY - r.top) / Math.max(r.height, 1) * i;
    let p = 0, g = Number.POSITIVE_INFINITY;
    m.forEach((W, C) => {
      const w = c - W.markerX, F = u - W.markerY, G = s === "bar" ? w * w + F * F : Math.abs(w) + Math.abs(F) * 0.08;
      G < g && (g = G, p = C);
    }), X(p);
  }, [m, i, s, h]), O = d.useCallback((t, r) => {
    if (a.length <= 0) {
      j(null);
      return;
    }
    const c = a.length - 1, u = Math.min(4, a.length), p = Math.max(0, Math.min(c, Math.floor(t))), g = Math.max(p, Math.min(c, Math.floor(r)));
    if (g - p + 1 >= a.length) {
      j(null);
      return;
    }
    if (g - p + 1 < u) {
      const W = Math.round((p + g) / 2), C = Math.floor(u / 2), w = Math.max(0, Math.min(c - u + 1, W - C));
      j({ start: w, end: w + u - 1 });
      return;
    }
    j({ start: p, end: g });
  }, [a.length]), D = d.useCallback((t) => {
    if (!M) return;
    const r = x.start, c = x.end, u = c - r + 1, p = Math.min(4, a.length), g = Math.max(p, Math.min(a.length, Math.round(u * t))), W = l ? l.fullIndex : Math.round((r + c) / 2), C = Math.max(0, Math.min(a.length - g, Math.round(W - g / 2)));
    O(C, C + g - 1);
  }, [l, M, x, a.length, O]), tt = d.useCallback(() => {
    X(null), j(null);
  }, []), et = d.useCallback((t) => {
    M && (t.preventDefault(), D(t.deltaY < 0 ? 0.68 : 1.42));
  }, [M, D]);
  return /* @__PURE__ */ n.jsxs(
    "div",
    {
      "data-pivot-sparkline-detail-graph": "true",
      style: {
        flex: "1 1 430px",
        minWidth: "300px",
        padding: "14px",
        borderRight: `1px solid ${e.border}`,
        background: e.headerSubtleBg || e.background
      },
      children: [
        M && /* @__PURE__ */ n.jsxs(
          "div",
          {
            "data-pivot-sparkline-zoom-controls": "true",
            style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px", marginBottom: "8px" },
            children: [
              /* @__PURE__ */ n.jsx("span", { style: { fontSize: "11px", fontWeight: 700, color: e.textSec }, children: k ? `${x.start + 1}-${x.end + 1} of ${a.length}` : `${a.length} points` }),
              /* @__PURE__ */ n.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center" }, children: [
                /* @__PURE__ */ n.jsx(
                  "button",
                  {
                    type: "button",
                    "data-pivot-sparkline-zoom-in": "true",
                    disabled: !$,
                    onClick: () => D(0.62),
                    style: { border: `1px solid ${e.border}`, background: $ ? e.background : e.headerSubtleBg, color: $ ? e.text : e.textSec, borderRadius: "6px", padding: "4px 8px", fontSize: "11px", fontWeight: 700, cursor: $ ? "pointer" : "default", opacity: $ ? 1 : 0.55 },
                    children: "Zoom In"
                  }
                ),
                /* @__PURE__ */ n.jsx(
                  "button",
                  {
                    type: "button",
                    "data-pivot-sparkline-zoom-out": "true",
                    disabled: !k,
                    onClick: () => D(1.62),
                    style: { border: `1px solid ${e.border}`, background: k ? e.background : e.headerSubtleBg, color: k ? e.text : e.textSec, borderRadius: "6px", padding: "4px 8px", fontSize: "11px", fontWeight: 700, cursor: k ? "pointer" : "default", opacity: k ? 1 : 0.55 },
                    children: "Zoom Out"
                  }
                ),
                /* @__PURE__ */ n.jsx(
                  "button",
                  {
                    type: "button",
                    "data-pivot-sparkline-zoom-reset": "true",
                    disabled: !k,
                    onClick: tt,
                    style: { border: `1px solid ${e.border}`, background: k ? e.background : e.headerSubtleBg, color: k ? e.text : e.textSec, borderRadius: "6px", padding: "4px 8px", fontSize: "11px", fontWeight: 700, cursor: k ? "pointer" : "default", opacity: k ? 1 : 0.55 },
                    children: "Reset"
                  }
                )
              ] })
            ]
          }
        ),
        /* @__PURE__ */ n.jsxs(
          "svg",
          {
            "data-pivot-sparkline-detail-svg": "true",
            viewBox: `0 0 ${h} ${i}`,
            "aria-label": "Sparkline detail chart",
            onMouseMove: Q,
            onMouseLeave: () => X(null),
            onWheel: et,
            style: { display: "block", width: "100%", height: "320px", cursor: m.length ? "crosshair" : "default" },
            children: [
              /* @__PURE__ */ n.jsx("rect", { x: "0", y: "0", width: h, height: i, fill: e.background || "#fff", opacity: "0.72" }),
              B.map((t, r) => /* @__PURE__ */ n.jsxs("g", { children: [
                /* @__PURE__ */ n.jsx("line", { x1: o, y1: t.y, x2: h - o, y2: t.y, stroke: e.border, strokeWidth: "1", opacity: r === B.length - 1 ? 0.9 : 0.45, strokeDasharray: r === B.length - 1 ? "" : "4 6" }),
                /* @__PURE__ */ n.jsx("text", { x: o - 8, y: t.y + 4, fill: e.textSec, fontSize: "10", textAnchor: "end", children: z(t.value) })
              ] }, `y-tick-${r}`)),
              _.map((t) => /* @__PURE__ */ n.jsxs("g", { children: [
                /* @__PURE__ */ n.jsx("line", { x1: t.x, y1: o, x2: t.x, y2: i - o, stroke: e.border, strokeWidth: "1", opacity: "0.28", strokeDasharray: "3 8" }),
                /* @__PURE__ */ n.jsx("text", { x: t.x, y: i - 12, fill: e.textSec, fontSize: "10", textAnchor: "middle", children: String(t.label || "").slice(0, 18) })
              ] }, `x-tick-${t.index}`)),
              P !== null ? /* @__PURE__ */ n.jsx("line", { x1: o, y1: P, x2: h - o, y2: P, stroke: e.textSec, strokeWidth: "1.2", opacity: "0.5" }) : null,
              /* @__PURE__ */ n.jsx("line", { x1: o, y1: i - o, x2: h - o, y2: i - o, stroke: e.border, strokeWidth: "1.2" }),
              /* @__PURE__ */ n.jsx("line", { x1: o, y1: o, x2: o, y2: i - o, stroke: e.border, strokeWidth: "1.2" }),
              s === "area" && y.areaPath ? /* @__PURE__ */ n.jsx("path", { d: y.areaPath, fill: A, opacity: v.areaOpacity || 0.14 }) : null,
              s === "line" || s === "area" ? /* @__PURE__ */ n.jsxs(n.Fragment, { children: [
                y.linePath ? /* @__PURE__ */ n.jsx("path", { d: y.linePath, fill: "none", stroke: A, strokeWidth: "3", strokeLinecap: "round", strokeLinejoin: "round" }) : null,
                m.map((t, r) => /* @__PURE__ */ n.jsx(
                  "circle",
                  {
                    "data-pivot-sparkline-detail-point": "true",
                    cx: t.markerX,
                    cy: t.markerY,
                    r: l && l.pointIndex === t.pointIndex ? "5.4" : "3.6",
                    fill: A,
                    stroke: e.background,
                    strokeWidth: "1.4",
                    children: /* @__PURE__ */ n.jsx("title", { children: `${t.label}: ${z(t.value, 6)}` })
                  },
                  `${t.label || "point"}-${r}`
                ))
              ] }) : null,
              s === "column" || s === "bar" ? m.map((t, r) => /* @__PURE__ */ n.jsx(
                "rect",
                {
                  x: t.x,
                  y: t.y,
                  width: t.width,
                  height: t.height,
                  rx: "2",
                  ry: "2",
                  fill: t.positive ? U : q,
                  opacity: l && l.fullIndex === t.fullIndex ? 1 : 0.88,
                  children: /* @__PURE__ */ n.jsx("title", { children: `${t.label}: ${z(t.value, 6)}` })
                },
                `${t.label || "bar"}-${r}`
              )) : null,
              l ? /* @__PURE__ */ n.jsxs("g", { "data-pivot-sparkline-tooltip": "true", pointerEvents: "none", children: [
                s !== "bar" ? /* @__PURE__ */ n.jsx("line", { x1: l.markerX, y1: o, x2: l.markerX, y2: i - o, stroke: A, strokeWidth: "1", opacity: "0.55", strokeDasharray: "5 5" }) : null,
                /* @__PURE__ */ n.jsx("circle", { cx: l.markerX, cy: l.markerY, r: "6.4", fill: A, stroke: e.background, strokeWidth: "2" }),
                /* @__PURE__ */ n.jsx("rect", { x: L, y: N, width: E, height: Z, rx: "7", fill: e.background || "#fff", stroke: e.border, strokeWidth: "1.2", opacity: "0.98" }),
                /* @__PURE__ */ n.jsx("text", { x: L + 10, y: N + 18, fill: e.textSec, fontSize: "10", fontWeight: "700", children: String(l.label || l.pointIndex + 1).slice(0, 28) }),
                /* @__PURE__ */ n.jsx("text", { x: L + 10, y: N + 38, fill: e.text, fontSize: "15", fontWeight: "800", children: z(l.value, 6) })
              ] }) : null,
              y.maxValue !== null ? /* @__PURE__ */ n.jsxs("text", { x: h - o, y: "22", fill: e.textSec, fontSize: "11", fontWeight: "700", textAnchor: "end", children: [
                "Max ",
                K
              ] }) : null,
              y.minValue !== null ? /* @__PURE__ */ n.jsxs("text", { x: h - o, y: i - 28, fill: e.textSec, fontSize: "11", fontWeight: "700", textAnchor: "end", children: [
                "Min ",
                J
              ] }) : null,
              /* @__PURE__ */ n.jsx("rect", { "data-pivot-sparkline-hover-target": "true", x: "0", y: "0", width: h, height: i, fill: "transparent" })
            ]
          }
        ),
        /* @__PURE__ */ n.jsxs("div", { style: { display: "flex", justifyContent: "space-between", gap: "10px", marginTop: "6px", fontSize: "11px", color: e.textSec }, children: [
          /* @__PURE__ */ n.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: T ? T.label : "" }),
          /* @__PURE__ */ n.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", textAlign: "right" }, children: V ? V.label : "" })
        ] })
      ]
    }
  );
}
export {
  st as SparklineDetailGraph
};
