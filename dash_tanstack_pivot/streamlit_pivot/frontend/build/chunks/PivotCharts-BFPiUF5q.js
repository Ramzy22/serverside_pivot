import { j as e, C as rr, r as Yr, c as rn, a as wr, n as It, e as Ct, t as he, l as nn, g as Ae, b as sn, d as Lr, f as on, p as ir, h as Er, i as Rr, k as ur, m as Dr, o as At, M as Nr, q as Pr, s as Mr, u as an, I as ln, v as cn, w as dn, x as Vr, y as _r, z as tr, A as un, B as xn, V as pn, D as Xr, E as Gr, F as fn, G as xr, H as hn, J as gn, K as bn, L as yn, N as Ir, O as vn, P as mn, Q as jn, R as Hr, S as kn } from "./index-Bu1c7lM2.js";
import { a as v, R as xt } from "./charts-DthW1Q8W.js";
const Ar = [
  { value: "solid", label: "Solid", dash: "none" },
  { value: "dashed", label: "Dashed", dash: "8 4" },
  { value: "dotted", label: "Dotted", dash: "2 4" },
  { value: "dot-dash", label: "Dot-Dash", dash: "8 4 2 4" }
], pr = (t) => {
  const n = Ar.find((i) => i.value === t);
  return n && n.dash !== "none" ? n.dash : void 0;
}, Cr = [
  { value: "vertical", label: "Vertical", x1: "0", y1: "0", x2: "0", y2: "1" },
  { value: "horizontal", label: "Horizontal", x1: "0", y1: "0", x2: "1", y2: "0" },
  { value: "diagonal", label: "Diagonal", x1: "0", y1: "0", x2: "1", y2: "1" }
], Sn = (t, n) => {
  const i = (Number(n) || 70) / 100;
  return [
    { offset: "0%", stopOpacity: String(Math.min(1, i + 0.2)) },
    { offset: "100%", stopOpacity: String(i) }
  ];
}, wn = ["bottom", "top", "none"], Mn = ({ legendPosition: t, onChange: n, theme: i }) => {
  const a = (r, g) => ({
    border: `1px solid ${r === t ? i.primary : i.border}`,
    background: r === t ? i.select : i.headerSubtleBg || i.hover,
    color: r === t ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "5px 10px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: wn.map((r) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => n(r), style: a(r), children: r.charAt(0).toUpperCase() + r.slice(1) }, r)) });
}, Cn = [
  { value: "none", label: "Natural" },
  { value: "value_desc", label: "Val ↓" },
  { value: "value_asc", label: "Val ↑" },
  { value: "alpha_asc", label: "A→Z" },
  { value: "alpha_desc", label: "Z→A" }
], An = (t, n) => {
  if (!Array.isArray(t) || n === "none" || !n) return t;
  const i = [...t];
  if (n === "alpha_asc") return i.sort((r, g) => String(r.name || "").localeCompare(String(g.name || "")));
  if (n === "alpha_desc") return i.sort((r, g) => String(g.name || "").localeCompare(String(r.name || "")));
  const a = (r) => (r.values || []).reduce((g, S) => g + (Number.isFinite(Number(S)) ? Number(S) : 0), 0);
  return n === "value_desc" ? i.sort((r, g) => a(g) - a(r)) : n === "value_asc" ? i.sort((r, g) => a(r) - a(g)) : i;
}, $n = ({ model: t, maxCategories: n, maxSeries: i, theme: a }) => {
  const r = Array.isArray(t && t.categories) ? t.categories.length : 0, g = Array.isArray(t && t.series) ? t.series.length : 0, S = r >= n * 0.9, A = g >= i * 0.9, B = r >= n, N = g >= i;
  if (!S && !A) return null;
  const E = (Y) => ({
    display: "inline-flex",
    alignItems: "center",
    gap: "4px",
    padding: "3px 8px",
    borderRadius: a.radiusSm || "8px",
    fontSize: "10px",
    fontWeight: 700,
    background: Y ? (a.danger || "#ef4444") + "22" : (a.warning || "#f59e0b") + "22",
    color: Y ? a.danger || "#ef4444" : a.warning || "#ca8a04",
    border: `1px solid ${Y ? a.danger || "#ef4444" : a.warning || "#f59e0b"}44`
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "5px", flexWrap: "wrap" }, children: [
    S && /* @__PURE__ */ e.jsxs("span", { style: E(B), children: [
      B ? "⚠ " : "○ ",
      r,
      "/",
      n,
      " categories"
    ] }),
    A && /* @__PURE__ */ e.jsxs("span", { style: E(N), children: [
      N ? "⚠ " : "○ ",
      g,
      "/",
      i,
      " series"
    ] })
  ] });
}, Wn = ({ series: t, lineDashStyles: n, onChange: i, seriesColors: a, paletteColors: r, getColorForIndex: g, theme: S, compactInputStyle: A }) => !Array.isArray(t) || t.length === 0 ? null : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: t.slice(0, 16).map((B, N) => {
  const E = a && a[B.name] || g(N, S, r), Y = n && n[B.name] || "solid";
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, children: [
    /* @__PURE__ */ e.jsx("span", { style: { width: "10px", height: "10px", borderRadius: "50%", background: E, flexShrink: 0 } }),
    /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: S.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: B.name || `Series ${N + 1}` }),
    /* @__PURE__ */ e.jsx(
      "select",
      {
        value: Y,
        onChange: (te) => i({ ...n, [B.name]: te.target.value }),
        style: { ...A, width: "82px", flexShrink: 0 },
        children: Ar.map((te) => /* @__PURE__ */ e.jsx("option", { value: te.value, children: te.label }, te.value))
      }
    )
  ] }, B.name || N);
}) }), zn = ({ customColors: t, onChange: n, theme: i, compactInputStyle: a }) => {
  const r = Array.isArray(t) && t.length > 0 ? t : ["#2563EB", "#F97316", "#0F766E", "#7C3AED", "#DC2626", "#0891B2"], g = (B, N) => {
    const E = [...r];
    E[B] = N, n(E);
  }, S = () => n([...r, "#888888"]), A = (B) => n(r.filter((N, E) => E !== B));
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", alignItems: "center" }, children: [
    r.map((B, N) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", alignItems: "center", gap: "2px" }, children: [
      /* @__PURE__ */ e.jsx(
        "input",
        {
          type: "color",
          value: B,
          onChange: (E) => g(N, E.target.value),
          style: { width: "28px", height: "28px", padding: 0, border: `1px solid ${i.border}`, borderRadius: "4px", cursor: "pointer", background: "none" },
          title: `Color ${N + 1}`
        }
      ),
      r.length > 2 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => A(N), style: { border: "none", background: "none", color: i.textSec, cursor: "pointer", fontSize: "9px", padding: 0, lineHeight: 1 }, children: "✕" })
    ] }, N)),
    r.length < 12 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: S, style: { ...a, padding: "5px 8px", flexShrink: 0 }, children: "+" })
  ] }) });
}, ze = 16, Bn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "7", width: "4", height: "8", fill: t, opacity: "0.85", rx: "0.5" })
] }), Ln = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polyline", { points: "1,13 5,7 9,10 15,3", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "1", cy: "13", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "7", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "9", cy: "10", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "15", cy: "3", r: "1.4", fill: t })
] }), En = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L8,1 A7,7 0 0,1 15,8 Z", fill: t, opacity: "0.9" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L15,8 A7,7 0 0,1 6,15 Z", fill: t, opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L6,15 A7,7 1 1,1 8,1 Z", fill: t, opacity: "0.3" })
] }), Rn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("circle", { cx: "3", cy: "12", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "7", cy: "5", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "12", cy: "9", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "13", cy: "3", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "11", r: "1.8", fill: t, opacity: "0.85" })
] }), Dn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "8", width: "4", height: "7", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("polyline", { points: "1,9 5,4 9,7 15,2", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" })
] }), Nn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "4", width: "3", height: "7", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "5", y: "8", width: "3", height: "3", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "5", width: "3", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "13", y: "10", width: "3", height: "1", fill: t, opacity: "0.4", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("line", { x1: "0", y1: "15", x2: "16", y2: "15", stroke: t, strokeWidth: "0.7", opacity: "0.35" })
] }), Pn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "2", y: "6", width: "12", height: "5", fill: "none", stroke: t, strokeWidth: "1.2", rx: "1", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "8", y2: "11", stroke: t, strokeWidth: "1.2", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "5", y1: "3", x2: "5", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "11", y1: "3", x2: "11", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "3", x2: "6", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "3", x2: "12", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "13", x2: "6", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "13", x2: "12", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" })
] }), In = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polygon", { points: "8,2 14,6 12,13 4,13 2,6", fill: "none", stroke: t, strokeWidth: "1", opacity: "0.35" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "8,5 11,7.5 9.5,11 6.5,11 5,7.5", fill: t, opacity: "0.65" })
] }), Hn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "1", width: "4", height: "4", fill: t, opacity: "0.9", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "1", width: "4", height: "4", fill: t, opacity: "0.4", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "1", width: "4", height: "4", fill: t, opacity: "0.7", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "6", width: "4", height: "4", fill: t, opacity: "0.3", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "6", width: "4", height: "4", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "6", width: "4", height: "4", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "11", width: "4", height: "4", fill: t, opacity: "0.6", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "11", width: "4", height: "4", fill: t, opacity: "0.95", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "11", width: "4", height: "4", fill: t, opacity: "0.35", rx: "0.5" })
] }), Fn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "5", height: "6", fill: t, opacity: "0.65", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "7", y: "4", width: "5", height: "11", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "1,9 4,5 9,5 7,9", fill: t, opacity: "0.45" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "7,4 12,4 12,9 7,9", fill: t, opacity: "0.55" })
] }), On = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "4", y: "1", width: "8", height: "5", fill: t, opacity: "0.85", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "4", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "12", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" })
] }), Fr = [
  { name: "Bar", Icon: Bn, types: [{ value: "bar", label: "Bar / Column" }] },
  { name: "Line / Area", Icon: Ln, types: [{ value: "line", label: "Line" }, { value: "area", label: "Area" }, { value: "sparkline", label: "Sparkline" }] },
  { name: "Pie / Donut", Icon: En, types: [{ value: "pie", label: "Pie" }, { value: "donut", label: "Donut" }] },
  { name: "Scatter", Icon: Rn, types: [{ value: "scatter", label: "Scatter" }, { value: "bubble", label: "Bubble" }] },
  { name: "Combo", Icon: Dn, types: [{ value: "combo", label: "Combo" }] },
  { name: "Waterfall", Icon: Nn, types: [{ value: "waterfall", label: "Waterfall" }, { value: "range", label: "Range" }] },
  { name: "Statistical", Icon: Pn, types: [{ value: "histogram", label: "Histogram" }, { value: "boxplot", label: "Box Plot" }, { value: "funnel", label: "Funnel" }] },
  { name: "Radial", Icon: In, types: [{ value: "radar", label: "Radar" }, { value: "nightingale", label: "Nightingale" }] },
  { name: "Heatmap", Icon: Hn, types: [{ value: "heatmap", label: "Heatmap" }] },
  { name: "3D", Icon: Fn, types: [{ value: "bar3d", label: "3D Bar" }, { value: "line3d", label: "3D Line" }, { value: "scatter3d", label: "3D Scatter" }] },
  { name: "Hierarchical", Icon: On, types: [{ value: "icicle", label: "Icicle" }, { value: "sunburst", label: "Sunburst" }, { value: "sankey", label: "Sankey" }], hierarchyOnly: !0 }
], Yn = ({ chartType: t, onChange: n, theme: i, includeHierarchyCharts: a = !0 }) => {
  const [r, g] = v.useState(null), S = a ? Fr : Fr.filter((A) => !A.hierarchyOnly);
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "3px" }, children: S.map(({ name: A, Icon: B, types: N }) => {
    const E = N.some((_) => _.value === t), Y = N.find((_) => _.value === t), te = N.length === 1, Q = r === A || E, s = E ? i.primary : i.textSec || i.text;
    return /* @__PURE__ */ e.jsxs("div", { children: [
      /* @__PURE__ */ e.jsxs(
        "button",
        {
          type: "button",
          "aria-expanded": te ? void 0 : Q,
          onClick: () => {
            te ? n(N[0].value) : g(Q && !E ? null : A);
          },
          style: {
            width: "100%",
            display: "flex",
            alignItems: "center",
            gap: "7px",
            padding: "5px 8px",
            border: `1px solid ${E ? i.primary : i.border}`,
            background: E ? i.select : i.headerSubtleBg || i.hover,
            color: E ? i.primary : i.text,
            borderRadius: i.radiusSm || "8px",
            fontSize: "11px",
            fontWeight: E ? 700 : 600,
            cursor: "pointer",
            textAlign: "left"
          },
          children: [
            /* @__PURE__ */ e.jsx(B, { color: s }),
            /* @__PURE__ */ e.jsx("span", { style: { flex: 1 }, children: Y ? Y.label : A }),
            !te && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "8px", color: s, transform: Q ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.12s", flexShrink: 0 }, children: "▼" })
          ]
        }
      ),
      !te && Q && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "3px", padding: "4px 0 2px 24px" }, children: N.map((_) => /* @__PURE__ */ e.jsx(
        "button",
        {
          type: "button",
          "aria-pressed": t === _.value,
          onClick: () => {
            n(_.value), g(null);
          },
          style: {
            border: `1px solid ${t === _.value ? i.primary : i.border}`,
            background: t === _.value ? i.select : i.headerSubtleBg || i.hover,
            color: t === _.value ? i.primary : i.text,
            borderRadius: i.radiusSm || "8px",
            padding: "4px 9px",
            fontSize: "10px",
            fontWeight: t === _.value ? 700 : 600,
            cursor: "pointer"
          },
          children: _.label
        },
        _.value
      )) })
    ] }, A);
  }) });
}, Vn = ({ line: t, onUpdate: n, onRemove: i, exportButtonStyle: a, compactInputStyle: r, theme: g }) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center", flexWrap: "wrap" }, children: [
  /* @__PURE__ */ e.jsx(
    "button",
    {
      type: "button",
      onClick: () => n({ ...t, orient: t.orient === "v" ? "h" : "v" }),
      style: { ...a, padding: "4px 7px", flexShrink: 0, fontWeight: 800, color: t.orient === "v" ? g.primary : g.textSec },
      title: t.orient === "v" ? "Vertical (click for horizontal)" : "Horizontal (click for vertical)",
      children: t.orient === "v" ? "|" : "—"
    }
  ),
  /* @__PURE__ */ e.jsx(
    "input",
    {
      type: "number",
      value: t.value,
      onChange: (S) => n({ ...t, value: S.target.value }),
      placeholder: t.orient === "v" ? "Cat index" : "Value",
      style: { ...r, width: "60px", flexShrink: 0 }
    }
  ),
  /* @__PURE__ */ e.jsx(
    "input",
    {
      type: "text",
      value: t.label,
      onChange: (S) => n({ ...t, label: S.target.value }),
      placeholder: "Label",
      style: { ...r, flex: 1, minWidth: 0 }
    }
  ),
  /* @__PURE__ */ e.jsx(
    "input",
    {
      type: "color",
      value: t.color || g.primary || "#2563EB",
      onChange: (S) => n({ ...t, color: S.target.value }),
      style: { width: "28px", height: "28px", padding: 0, border: `1px solid ${g.border}`, borderRadius: "4px", cursor: "pointer", background: "none", flexShrink: 0 },
      title: "Line color"
    }
  ),
  /* @__PURE__ */ e.jsx(
    "select",
    {
      value: t.dash || "dashed",
      onChange: (S) => n({ ...t, dash: S.target.value }),
      style: { ...r, width: "74px", flexShrink: 0 },
      title: "Line style",
      children: Ar.map((S) => /* @__PURE__ */ e.jsx("option", { value: S.value, children: S.label }, S.value))
    }
  ),
  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: i, style: { ...a, padding: "4px 7px", color: g.danger || "#ef4444", flexShrink: 0 }, children: "✕" })
] }), Or = (t, n = 430, i, a) => {
  const r = Number(t), g = Number(n), S = Number.isFinite(r) ? r : Number.isFinite(g) ? g : 430;
  return Math.max(i, Math.min(a, Math.floor(S)));
}, _n = (t) => {
  const n = t.map((E, Y) => [Y, Number(E)]).filter(([, E]) => Number.isFinite(E));
  if (n.length < 2) return null;
  const i = n.length, a = n.reduce((E, [Y]) => E + Y, 0), r = n.reduce((E, [, Y]) => E + Y, 0), g = n.reduce((E, [Y, te]) => E + Y * te, 0), S = n.reduce((E, [Y]) => E + Y * Y, 0), A = i * S - a * a;
  if (A === 0) return null;
  const B = (i * g - a * r) / A, N = (r - B * a) / i;
  return t.map((E, Y) => B * Y + N);
}, Xn = (t, n) => {
  const i = Math.max(2, Math.floor(Number(n) || 3));
  return t.map((a, r) => {
    const g = t.slice(Math.max(0, r - i + 1), r + 1).map(Number).filter(Number.isFinite);
    return g.length > 0 ? g.reduce((S, A) => S + A, 0) / g.length : null;
  });
}, Tr = (t) => Array.isArray(t && t.series) && t.series.length > 1, hr = (t, n) => {
  if (typeof window > "u" || !t) return;
  const i = window.URL.createObjectURL(t), a = document.createElement("a");
  a.href = i, a.download = n, document.body.appendChild(a), a.click(), document.body.removeChild(a), window.URL.revokeObjectURL(i);
}, gr = (t = {}) => ({
  title: String(t.title || "").trim(),
  legendPosition: t.legendPosition === "top" ? "top" : "bottom",
  legendItems: Array.isArray(t.legendItems) ? t.legendItems.filter((n) => n && n.label) : [],
  background: t.background || "#ffffff",
  textColor: t.textColor || "#111827",
  secondaryTextColor: t.secondaryTextColor || "#4b5563"
}), Gn = (t, n, i, a) => {
  const r = t ? Array.from(t.querySelectorAll('[data-chart-export-legend-item="true"]')).map((g) => ({
    label: g.getAttribute("data-chart-export-label") || g.textContent || "",
    color: g.getAttribute("data-chart-export-color") || a.primary || "#2563eb",
    hidden: g.getAttribute("data-chart-export-hidden") === "true"
  })).filter((g) => g.label) : [];
  return gr({
    title: n,
    legendPosition: i,
    legendItems: r,
    background: a.chartBg || a.surfaceBg || a.background || "#ffffff",
    textColor: a.text || "#111827",
    secondaryTextColor: a.textSec || a.text || "#4b5563"
  });
}, Ur = (t, n, i) => {
  const a = [];
  let r = [], g = 0;
  return t.forEach((S) => {
    const A = Math.max(48, i(S.label) + 30);
    r.length > 0 && g + A > Math.max(80, n - 24) && (a.push(r), r = [], g = 0), r.push({ ...S, width: A }), g += A;
  }), r.length > 0 && a.push(r), a;
}, fr = (t) => {
  const n = String(t ?? ""), i = typeof t == "string" && /^[=+\-@]/.test(n) ? `'${n}` : n;
  return i.includes(",") || i.includes('"') || i.includes(`
`) ? `"${i.replace(/"/g, '""')}"` : i;
}, Tn = (t, n, i) => {
  if (!t) return;
  const a = Array.isArray(t.categories) ? t.categories : [], r = Array.isArray(t.series) ? t.series : [];
  if (a.length === 0 || r.length === 0) return;
  const g = ["Category", ...r.map((E) => E.name)].map(fr).join(","), S = a.map((E, Y) => [E, ...r.map((te) => te.values[Y] ?? "")].map(fr).join(",")), A = gr(i), B = A.legendItems.length > 0 ? A.legendItems.map((E) => E.label) : r.map((E) => E.name).filter(Boolean), N = [];
  A.title && N.push(["Title", A.title].map(fr).join(",")), B.length > 0 && N.push(["Legend", ...B].map(fr).join(",")), N.length > 0 && N.push(""), hr(new Blob([`\uFEFF${[...N, g, ...S].join(`
`)}`], { type: "text/csv;charset=utf-8" }), `${n}.csv`);
}, Un = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), Kn = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d"]), Zn = xt.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-2d" */
  "./ECharts2dRenderer-BMX7Vq0g.js"
)), qn = xt.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-3d" */
  "./ECharts3dRenderer-OAqR7x-J.js"
)), Jt = ({ chartType: t, echartsRef: n, option: i }) => {
  const a = Kn.has(t) ? qn : Zn;
  return /* @__PURE__ */ e.jsx(xt.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { "aria-label": "Loading chart", style: { width: "100%", height: "100%" } }), children: /* @__PURE__ */ e.jsx(a, { echartsRef: n, option: i }) });
}, we = ({ message: t, theme: n, chartHeight: i = rr }) => /* @__PURE__ */ e.jsx("div", { style: {
  minHeight: `${i}px`,
  border: `1px dashed ${n.border}`,
  borderRadius: n.radius || "14px",
  background: n.headerSubtleBg || n.hover,
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  padding: "16px",
  color: n.textSec,
  fontSize: "12px",
  textAlign: "center"
}, children: t }), Qn = ({ model: t, theme: n }) => {
  const i = v.useMemo(() => {
    const r = [];
    return (t && t.series ? t.series : []).forEach((g) => {
      (g.values || []).forEach((S) => {
        typeof S == "number" && Number.isFinite(S) && r.push(S);
      });
    }), r.length === 0 && t && Array.isArray(t.icicleNodes) && t.icicleNodes.forEach((g) => {
      const S = Number(g && g.value);
      Number.isFinite(S) && r.push(S);
    }), r.length === 0 ? null : {
      categories: Array.isArray(t.categories) ? t.categories.length : 0,
      seriesCount: Array.isArray(t.series) ? t.series.length : 0,
      hierarchyNodes: Array.isArray(t.icicleNodes) ? t.icicleNodes.length : 0,
      min: Math.min(...r),
      max: Math.max(...r)
    };
  }, [t]);
  if (!i) return null;
  const a = {
    border: `1px solid ${n.border}`,
    background: n.headerSubtleBg || n.hover,
    color: n.textSec,
    borderRadius: n.radiusSm || "8px",
    padding: "5px 8px",
    fontSize: "11px",
    fontWeight: 700
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      i.categories,
      " categories"
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      i.seriesCount,
      " series"
    ] }),
    i.hierarchyNodes > 0 ? /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      i.hierarchyNodes,
      " nodes"
    ] }) : null,
    /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      "Min ",
      Dr(i.min)
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      "Max ",
      Dr(i.max)
    ] })
  ] });
}, jt = ({ items: t, theme: n, hiddenSet: i, onToggle: a, position: r = "bottom", onElementSelect: g }) => {
  if (r === "none" || !Array.isArray(t) || t.length === 0) return null;
  const S = typeof a == "function" && i;
  return /* @__PURE__ */ e.jsx("div", { "data-chart-export-legend": "true", style: {
    display: "flex",
    gap: "10px",
    flexWrap: "wrap",
    alignItems: "center",
    justifyContent: "flex-start",
    order: r === "top" ? -1 : 1,
    padding: r === "top" ? "0 0 6px 0" : "6px 0 0 0"
  }, children: t.map((A) => {
    const B = S && i.has(A.label);
    return /* @__PURE__ */ e.jsxs(
      "div",
      {
        "data-chart-export-legend-item": "true",
        "data-chart-export-label": A.label,
        "data-chart-export-color": B ? n.border : A.color,
        "data-chart-export-hidden": B ? "true" : "false",
        onClick: () => {
          S && a(A.label), typeof g == "function" && g("legend", A.label);
        },
        role: "button",
        tabIndex: 0,
        onKeyDown: (N) => {
          (N.key === "Enter" || N.key === " ") && (N.preventDefault(), S && a(A.label), typeof g == "function" && g("legend", A.label));
        },
        "aria-pressed": S ? !B : void 0,
        style: {
          display: "flex",
          alignItems: "center",
          gap: "6px",
          fontSize: "11px",
          color: n.textSec,
          cursor: "pointer",
          opacity: B ? 0.35 : 1,
          textDecoration: B ? "line-through" : "none",
          userSelect: "none"
        },
        children: [
          /* @__PURE__ */ e.jsx("span", { style: { width: "10px", height: "10px", borderRadius: "999px", background: B ? n.border : A.color, display: "inline-block" } }),
          /* @__PURE__ */ e.jsx("span", { children: A.label })
        ]
      },
      A.label
    );
  }) });
}, Jn = (t, n, i, a = 10) => {
  const g = (Array.isArray(t) ? t.map((X) => Number.isFinite(Number(X)) ? Number(X) : null) : []).map((X, O) => X === null ? null : { value: X, index: O }).filter(Boolean);
  if (g.length === 0)
    return {
      linePath: "",
      areaPath: "",
      currentPoint: null,
      minPoint: null,
      maxPoint: null,
      minValue: null,
      maxValue: null,
      firstValue: null,
      lastValue: null
    };
  const S = Math.min(...g.map((X) => X.value)), A = Math.max(...g.map((X) => X.value)), B = Math.max(1, A - S), N = g.length > 1 ? (n - a * 2) / (g.length - 1) : 0, E = g.map((X, O) => {
    const G = a + O * N, K = (X.value - S) / B, U = i - a - K * Math.max(1, i - a * 2);
    return {
      ...X,
      x: G,
      y: U
    };
  }), Y = E.map((X, O) => `${O === 0 ? "M" : "L"} ${X.x.toFixed(2)} ${X.y.toFixed(2)}`).join(" "), te = E.length > 1 ? `${Y} L ${E[E.length - 1].x.toFixed(2)} ${(i - a).toFixed(2)} L ${E[0].x.toFixed(2)} ${(i - a).toFixed(2)} Z` : "", Q = E.find((X) => X.value === S) || E[0], s = E.find((X) => X.value === A) || E[0], _ = E[E.length - 1];
  return {
    linePath: Y,
    areaPath: te,
    currentPoint: _,
    minPoint: Q,
    maxPoint: s,
    minValue: S,
    maxValue: A,
    firstValue: E[0] ? E[0].value : null,
    lastValue: _ ? _.value : null
  };
}, es = ({
  model: t,
  theme: n,
  chartHeight: i,
  paletteColors: a,
  valueFormat: r,
  referenceLines: g = []
}) => {
  const S = Array.isArray(t && t.series) ? t.series : [], A = Array.isArray(t && t.categories) ? t.categories : [];
  if (S.length === 0)
    return /* @__PURE__ */ e.jsx(we, { message: t && t.emptyMessage ? t.emptyMessage : "No sparkline data available.", theme: n, chartHeight: i });
  const B = 110, N = 240, E = 64;
  return /* @__PURE__ */ e.jsx(
    "div",
    {
      "data-chart-sparkline-board": "true",
      style: {
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))",
        gap: "10px",
        minHeight: 0,
        maxHeight: `${i}px`,
        overflowY: "auto",
        paddingRight: "2px"
      },
      children: S.map((Y, te) => {
        const Q = Ae(te, n, a), s = Jn(Y.values || [], N, E, 10), _ = Array.isArray(Y.values) && Y.values.length > 1 ? Number(Y.values[Y.values.length - 2]) : null, X = s.lastValue, O = Number.isFinite(_) && Number.isFinite(X) ? X - _ : null, G = O === null ? n.textSec : O >= 0 ? n.isDark ? "#7DD3A7" : "#2E7D5B" : n.isDark ? "#F7A3A3" : "#B45353";
        return /* @__PURE__ */ e.jsxs(
          "div",
          {
            "data-chart-sparkline-card": Y.id || Y.name || `spark-${te}`,
            style: {
              minHeight: `${B}px`,
              border: `1px solid ${n.border}`,
              borderRadius: n.radiusSm || "10px",
              background: n.surfaceBg || n.background || "#fff",
              boxShadow: n.shadowInset || "none",
              padding: "10px 12px",
              display: "flex",
              flexDirection: "column",
              gap: "8px"
            },
            children: [
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px" }, children: [
                /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0 }, children: [
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: n.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: Y.name || `Series ${te + 1}` }),
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: n.textSec }, children: A.length > 0 ? `${A.length} points` : "Trend" })
                ] }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", alignItems: "flex-end", gap: "2px", flexShrink: 0 }, children: [
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: X === null ? "—" : At(X, r) }),
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: G }, children: O === null ? "No delta" : `${O >= 0 ? "+" : ""}${At(O, r)}` })
                ] })
              ] }),
              /* @__PURE__ */ e.jsxs(
                "svg",
                {
                  viewBox: `0 0 ${N} ${E}`,
                  style: { width: "100%", height: "64px", display: "block" },
                  "aria-label": `${Y.name || `Series ${te + 1}`} sparkline`,
                  children: [
                    s.areaPath ? /* @__PURE__ */ e.jsx("path", { d: s.areaPath, fill: Q, opacity: "0.12" }) : null,
                    s.linePath ? /* @__PURE__ */ e.jsx(
                      "path",
                      {
                        d: s.linePath,
                        fill: "none",
                        stroke: Q,
                        strokeWidth: "2.5",
                        strokeLinecap: "round",
                        strokeLinejoin: "round"
                      }
                    ) : null,
                    s.minPoint ? /* @__PURE__ */ e.jsx("circle", { cx: s.minPoint.x, cy: s.minPoint.y, r: "2.6", fill: n.surfaceBg || n.background || "#fff", stroke: Q, strokeWidth: "1.4", opacity: "0.7" }) : null,
                    s.maxPoint ? /* @__PURE__ */ e.jsx("circle", { cx: s.maxPoint.x, cy: s.maxPoint.y, r: "2.8", fill: n.surfaceBg || n.background || "#fff", stroke: Q, strokeWidth: "1.6", opacity: "0.9" }) : null,
                    s.currentPoint ? /* @__PURE__ */ e.jsx("circle", { cx: s.currentPoint.x, cy: s.currentPoint.y, r: "3.2", fill: Q }) : null,
                    g.map((K, U) => {
                      const ge = Number(K.value);
                      if (!Number.isFinite(ge) || K.orient === "v" || s.minValue === null || s.maxValue === null) return null;
                      const ae = s.maxValue - s.minValue || 1, de = E - 10 - (ge - s.minValue) / ae * (E - 20);
                      if (de < 0 || de > E) return null;
                      const xe = K.color || n.primary;
                      return /* @__PURE__ */ e.jsxs("g", { children: [
                        /* @__PURE__ */ e.jsx("line", { x1: 10, y1: de, x2: N - 10, y2: de, stroke: xe, strokeWidth: "1", strokeDasharray: "4 2", style: { pointerEvents: "none" } }),
                        K.label ? /* @__PURE__ */ e.jsx("text", { x: N - 11, y: de - 2, textAnchor: "end", fontSize: "8", fontWeight: "700", fill: xe, style: { pointerEvents: "none" }, children: K.label }) : null
                      ] }, `spark-rl-${U}`);
                    })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", fontSize: "10px", color: n.textSec }, children: [
                /* @__PURE__ */ e.jsx("span", { children: A[0] ? he(String(A[0]), 14) : "Start" }),
                /* @__PURE__ */ e.jsxs("span", { children: [
                  "Min ",
                  s.minValue === null ? "—" : At(s.minValue, r),
                  " · ",
                  "Max ",
                  s.maxValue === null ? "—" : At(s.maxValue, r)
                ] }),
                /* @__PURE__ */ e.jsx("span", { children: A.length > 0 ? he(String(A[A.length - 1]), 14) : "End" })
              ] })
            ]
          },
          Y.id || Y.name || `spark-${te}`
        );
      })
    }
  );
}, ts = ({
  model: t,
  chartType: n,
  barLayout: i,
  axisMode: a,
  theme: r,
  chartHeight: g = rr,
  showLegend: S = !0,
  showDataLabels: A = !1,
  onCategoryActivate: B = null,
  svgRef: N = null,
  echartsRef: E = null,
  colorPalette: Y = "default",
  valueFormat: te = "auto",
  yAxisTitle: Q = "",
  yAxisMin: s = "",
  yAxisMax: _ = "",
  referenceLines: X = [],
  labelAngle: O = 0,
  chartTitle: G = "",
  onTitleChange: K = null,
  tickCount: U = 5,
  markerShape: ge = "circle",
  markerSize: ae = 4,
  showGradient: de = !1,
  seriesColors: xe = {},
  showAnimations: nt = !1,
  showCalloutLabels: ye = !1,
  subtitle: st = "",
  onReferenceLinesChange: _e = null,
  showTrendline: Ht = !1,
  trendlineMode: ot = "linear",
  trendlinePeriod: qe = 3,
  // new props
  legendPosition: Oe = "bottom",
  labelPosition: Me = "outside",
  decimalPlaces: _t = -1,
  gradientDirection: nr = "vertical",
  gradientOpacity: et = 70,
  trendlineColor: Xt = "",
  trendlineWidth: Gt = 2,
  seriesSort: Qe = "none",
  lineDashStyles: pt = {},
  yAxisMinRight: Tt = "",
  yAxisMaxRight: W = "",
  customPaletteColors: $t = [],
  onElementSelect: ue = null
}) => {
  const je = Y === "custom" && $t.length >= 2 ? $t : Yr(Y), P = (o) => {
    if (_t >= 0 && typeof o == "number" && Number.isFinite(o)) {
      const c = At(o, te);
      if (!/[KMB%$]/.test(c) || te === "number")
        return o.toFixed(_t);
    }
    return At(o, te);
  };
  v.useMemo(() => !t || !Array.isArray(t.series) || Qe === "none" ? t : { ...t, series: An(t.series, Qe) }, [t, Qe]);
  const Ie = n === "bar" && i === "stacked" && Array.isArray(t.stackedGroups) && t.stackedGroups.length > 0, Wt = n === "bar" && i === "stacked" && !Ie && rn(t), ft = n === "area" && i === "stacked" && Tr(t), it = n === "bar" && a === "horizontal", De = i === "stacked100", ke = Number.isFinite(Number(O)) ? Number(O) : 0, $ = g, ve = (o, c) => xe && xe[o] || Ae(c, r, je), zt = (o, c, l, u, p = {}) => {
    if (ge === "none") return null;
    const f = ae || 4, j = r.surfaceBg || r.background || "#fff", h = { fill: l, stroke: j, strokeWidth: "1.5", opacity: "0.82", ...u, ...p };
    switch (ge) {
      case "square":
        return /* @__PURE__ */ e.jsx("rect", { x: o - f, y: c - f, width: f * 2, height: f * 2, rx: "2", ...h });
      case "diamond":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${c - f} ${o + f},${c} ${o},${c + f} ${o - f},${c}`, ...h });
      case "cross":
        return /* @__PURE__ */ e.jsx("path", { d: `M${o - f},${c} L${o + f},${c} M${o},${c - f} L${o},${c + f}`, fill: "none", stroke: l, strokeWidth: "2.5", strokeLinecap: "round", ...u, ...p });
      case "triangle":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${c - f} ${o + f},${c + f * 0.85} ${o - f},${c + f * 0.85}`, ...h });
      default:
        return /* @__PURE__ */ e.jsx("circle", { cx: o, cy: c, r: f, ...h });
    }
  }, me = v.useMemo(() => !De || !t || !Array.isArray(t.categories) ? null : t.categories.map((o, c) => {
    let l = 0;
    return (t.series || []).forEach((u) => {
      const p = u.values && u.values[c];
      typeof p == "number" && Number.isFinite(p) && (l += Math.abs(p));
    }), l || 1;
  }), [De, t]), ht = Array.isArray(t && t.icicleNodes) ? t.icicleNodes : [], at = Math.max(1, Number(t && t.icicleDepth) || 1), He = v.useRef(null), [kt, sr] = v.useState(wr), T = Math.max(wr, Number.isFinite(kt) ? Math.floor(kt) : wr);
  v.useEffect(() => {
    const o = He.current;
    if (!o) return;
    const c = () => {
      const u = o.clientWidth;
      u > 0 && sr(u);
    };
    if (c(), typeof ResizeObserver > "u") return;
    const l = new ResizeObserver(c);
    return l.observe(o), () => l.disconnect();
  }, []);
  const [J, Bt] = v.useState(/* @__PURE__ */ new Set()), Ke = v.useCallback((o) => {
    Bt((c) => {
      const l = new Set(c);
      return l.has(o) ? l.delete(o) : l.add(o), l;
    });
  }, []), [$e, Xe] = v.useState(null), Ge = v.useCallback((o) => {
    const c = o.target, l = c.getAttribute("data-tip-cat");
    if (!l) {
      Xe((f) => f && null);
      return;
    }
    const p = o.currentTarget.getBoundingClientRect();
    Xe({
      x: o.clientX - p.left,
      y: o.clientY - p.top,
      category: l,
      series: c.getAttribute("data-tip-ser") || "",
      value: c.getAttribute("data-tip-val") || "",
      color: c.getAttribute("data-tip-color") || ""
    });
  }, []), gt = v.useCallback(() => Xe(null), []), lt = v.useCallback((o) => {
    if (!ue) return;
    let c = o.target;
    for (; c && c !== o.currentTarget; ) {
      const l = c.getAttribute && c.getAttribute("data-chart-element-type");
      if (l) {
        ue(l, c.getAttribute("data-chart-element-id") || "");
        return;
      }
      c = c.parentElement;
    }
  }, [ue]), Ne = $e ? /* @__PURE__ */ e.jsxs("div", { style: {
    position: "absolute",
    left: $e.x + 14 + 160 > T ? Math.max(0, $e.x - 174) : $e.x + 14,
    top: $e.y - 10 < 0 ? $e.y + 14 : $e.y - 10,
    pointerEvents: "none",
    zIndex: 10,
    background: r.surfaceBg || r.background || "#fff",
    border: `1px solid ${r.border}`,
    borderRadius: r.radiusSm || "8px",
    padding: "6px 10px",
    fontSize: "11px",
    fontWeight: 600,
    color: r.text,
    boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
    whiteSpace: "nowrap"
  }, children: [
    /* @__PURE__ */ e.jsx("div", { style: { fontWeight: 800, marginBottom: $e.series ? "2px" : 0 }, children: $e.category }),
    $e.series ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, children: [
      $e.color ? /* @__PURE__ */ e.jsx("span", { style: { width: "8px", height: "8px", borderRadius: "50%", background: $e.color, flexShrink: 0 } }) : null,
      /* @__PURE__ */ e.jsxs("span", { children: [
        $e.series,
        ": ",
        $e.value
      ] })
    ] }) : $e.value ? /* @__PURE__ */ e.jsx("div", { children: $e.value }) : null
  ] }) : null, [Le, Ft] = v.useState(1), [tt, Ee] = v.useState({ x: 0, y: 0 }), Ze = v.useRef(null), [bt, yt] = v.useState(null), rt = v.useCallback((o) => {
    o.preventDefault(), Ft((c) => {
      const l = o.deltaY < 0 ? c * 1.15 : c / 1.15, u = Math.min(10, Math.max(1, l));
      return u === 1 && Ee({ x: 0, y: 0 }), u;
    });
  }, []), Te = v.useCallback((o) => {
    Le <= 1 || o.button !== 0 || (Ze.current = { startX: o.clientX, startY: o.clientY, startPan: { ...tt } });
  }, [Le, tt]), ct = v.useCallback((o) => {
    if (!Ze.current) return;
    const c = o.clientX - Ze.current.startX, l = o.clientY - Ze.current.startY;
    Ee({ x: Ze.current.startPan.x + c, y: Ze.current.startPan.y + l });
  }, []), Fe = v.useCallback(() => {
    Ze.current = null;
  }, []), Lt = v.useCallback(() => {
    Ft(1), Ee({ x: 0, y: 0 });
  }, []), dt = v.useCallback((o) => {
    Ge(o), ct(o);
    const c = N && N.current;
    if (c && typeof c.getScreenCTM == "function") {
      const l = c.getScreenCTM();
      if (l) {
        const u = c.createSVGPoint();
        u.x = o.clientX, u.y = o.clientY;
        const p = u.matrixTransform(l.inverse());
        yt({ x: p.x, y: p.y });
      }
    }
  }, [Ge, ct]), St = v.useCallback(() => {
    gt(), Fe(), yt(null);
  }, [gt, Fe]), We = {
    onMouseMove: dt,
    onMouseLeave: St,
    onWheel: rt,
    onMouseDown: Te,
    onMouseUp: Fe,
    onDoubleClick: Lt
  }, ee = Le > 1 ? `scale(${Le}) translate(${tt.x / Le}px, ${tt.y / Le}px)` : void 0, Be = Le > 1 ? { overflow: "hidden", cursor: "grab" } : void 0, Ue = Le > 1 ? { transform: ee, transformOrigin: "center center" } : void 0, Se = bt && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsxs("svg", { style: { position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none", zIndex: 5 }, viewBox: `0 0 ${T} ${$}`, preserveAspectRatio: "xMidYMid meet", children: [
    /* @__PURE__ */ e.jsx("line", { x1: bt.x, y1: 0, x2: bt.x, y2: $, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" }),
    /* @__PURE__ */ e.jsx("line", { x1: 0, y1: bt.y, x2: T, y2: bt.y, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" })
  ] }) : null, Ye = Le > 1.05 ? /* @__PURE__ */ e.jsxs("div", { style: { position: "absolute", top: 4, right: 4, zIndex: 12, background: r.surfaceBg || "#fff", border: `1px solid ${r.border}`, borderRadius: r.radiusSm || "8px", padding: "2px 7px", fontSize: "10px", fontWeight: 700, color: r.textSec, pointerEvents: "auto", cursor: "pointer" }, onClick: Lt, title: "Reset zoom (or double-click chart)", children: [
    Math.round(Le * 100),
    "% ✕"
  ] }) : null, [Je, or] = v.useState(!1), [vt, ar] = v.useState(""), Et = v.useRef(null), [Ut, Kt] = v.useState(null), Zt = v.useRef(null);
  v.useEffect(() => {
    const o = (l) => {
      if (!Zt.current) return;
      const { startX: u, startY: p, startPos: f, containerRect: j } = Zt.current, h = l.clientX - u, w = l.clientY - p, k = Math.max(0, Math.min(j.width - 40, f.x + h)), b = Math.max(0, Math.min(j.height - 20, f.y + w));
      Kt({ x: k, y: b });
    }, c = () => {
      Zt.current = null;
    };
    return window.addEventListener("mousemove", o), window.addEventListener("mouseup", c), () => {
      window.removeEventListener("mousemove", o), window.removeEventListener("mouseup", c);
    };
  }, []);
  const Rt = (o) => {
    if (Je) return;
    o.preventDefault();
    const c = o.currentTarget.parentElement;
    if (!c) return;
    const l = c.getBoundingClientRect(), u = Ut || {
      x: l.width / 2 - 40,
      y: 6
    };
    Zt.current = {
      startX: o.clientX,
      startY: o.clientY,
      startPos: u,
      containerRect: l
    };
  }, br = () => {
    typeof K == "function" && (ar(G || ""), or(!0));
  }, Dt = () => {
    typeof K == "function" && K(vt), or(!1);
  };
  v.useEffect(() => {
    Je && Et.current && Et.current.focus();
  }, [Je]);
  const yr = Ut ? { left: `${Ut.x}px`, top: `${Ut.y}px` } : { left: "50%", top: "6px", transform: "translateX(-50%)" }, Re = G || Je ? /* @__PURE__ */ e.jsx(
    "div",
    {
      onMouseDown: Rt,
      onDoubleClick: br,
      onClick: ue && !Je ? () => ue("title", "") : void 0,
      "data-chart-element-type": "title",
      style: {
        position: "absolute",
        ...yr,
        zIndex: 6,
        cursor: Je ? "text" : ue ? "pointer" : "grab",
        userSelect: Je ? "auto" : "none"
      },
      children: Je ? /* @__PURE__ */ e.jsx(
        "input",
        {
          ref: Et,
          type: "text",
          value: vt,
          onChange: (o) => ar(o.target.value),
          onBlur: Dt,
          onKeyDown: (o) => {
            o.key === "Enter" && Dt(), o.key === "Escape" && or(!1);
          },
          onMouseDown: (o) => o.stopPropagation(),
          style: {
            fontSize: "13px",
            fontWeight: 700,
            color: r.text,
            background: r.surfaceBg || r.background || "#fff",
            border: `1.5px solid ${r.primary}`,
            borderRadius: r.radiusSm || "6px",
            padding: "2px 10px",
            outline: "none",
            minWidth: "80px"
          }
        }
      ) : /* @__PURE__ */ e.jsx("span", { style: {
        fontSize: "13px",
        fontWeight: 700,
        color: r.text,
        background: `${r.surfaceBg || r.background || "#fff"}cc`,
        padding: "2px 10px",
        borderRadius: r.radiusSm || "6px",
        whiteSpace: "nowrap",
        display: "inline-block"
      }, children: G })
    }
  ) : null, x = v.useMemo(() => {
    if (n === "heatmap" || n === "icicle" || n === "sunburst" || n === "sankey" || n === "pie" || n === "donut" || n === "scatter" || n === "waterfall" || n === "combo" || n === "sparkline" || n === "bubble" || n === "radar" || n === "funnel" || n === "histogram" || Un.has(n)) return null;
    const o = Ie ? (t.stackedGroups || []).reduce((L, le) => {
      let oe = 0, re = 0;
      return (le.segments || []).forEach((D) => {
        const ne = D && typeof D.value == "number" && Number.isFinite(D.value) ? D.value : null;
        ne !== null && (ne >= 0 ? oe += ne : re += ne);
      }), L.push(oe, re), L;
    }, []) : Wt ? (t.categories || []).reduce((L, le, oe) => {
      let re = 0, D = 0;
      return (t.series || []).forEach((ne) => {
        const ce = ne && Array.isArray(ne.values) ? ne.values[oe] : null;
        typeof ce != "number" || !Number.isFinite(ce) || (ce >= 0 ? re += ce : D += ce);
      }), L.push(re, D), L;
    }, []) : ft ? t.categories.reduce((L, le, oe) => {
      let re = 0, D = 0;
      return (t.series || []).forEach((ne) => {
        const ce = ne && Array.isArray(ne.values) ? ne.values[oe] : null;
        typeof ce != "number" || !Number.isFinite(ce) || (ce >= 0 ? re += ce : D += ce);
      }), L.push(re, D), L;
    }, []) : (t.series || []).reduce((L, le) => ((le.values || []).forEach((oe) => {
      typeof oe == "number" && Number.isFinite(oe) && L.push(oe);
    }), L), []);
    if (o.length === 0) return null;
    const c = !Ie && n === "bar" && Array.isArray(t.categoryBands) && t.categoryBands.length === t.categories.length && t.categoryBands.some((L) => L && L.outerLabel);
    let l = Math.min(...o, 0), u = Math.max(...o, 0);
    if (De)
      l = 0, u = 100;
    else {
      if (l === u) {
        const L = Math.abs(u || 1) * 0.15 || 1;
        l -= L, u += L;
      }
      Number.isFinite(Number(s)) && s !== "" && (l = Number(s)), Number.isFinite(Number(_)) && _ !== "" && (u = Number(_)), l >= u && (u = l + 1);
    }
    const f = It(l, u, 5).map((L) => De ? `${Math.round(L)}%` : P(L)).reduce((L, le) => Math.max(L, Ct(le)), 0), j = Ie ? (t.stackedGroups || []).map((L) => he(L && L.label, 22)) : c ? (t.categoryBands || []).map((L) => he(L && L.innerLabel, 18)) : (t.categories || []).map((L) => he(L, 22)), h = c ? (t.categoryBands || []).map((L) => he(L && L.outerLabel, 18)) : [], w = j.reduce((L, le) => Math.max(L, Ct(le)), 0), k = h.reduce((L, le) => Math.max(L, Ct(le, 11)), 0), b = it ? Math.max(
      c ? 176 : 132,
      c ? k + w + 38 : w + 22
    ) : Math.max(66, f + 18), I = ke === 0 ? 40 : ke <= 30 ? 62 : ke <= 45 ? 80 : ke <= 60 ? 98 : 118, F = it ? { top: 24, right: 18, bottom: 42, left: b } : { top: 24, right: 18, bottom: c ? 118 : I, left: b }, M = T - F.left - F.right, m = $ - F.top - F.bottom, z = n === "bar" ? 0 : Math.min(Math.max(0, l), u), R = u - l || 1, H = it ? null : ((L) => F.top + (u - L) / R * m), V = it ? ((L) => F.left + (L - l) / R * M) : null, ie = H ? H(z) : null, y = V ? V(z) : null, Z = Math.max(t.categories.length, 1), se = (it ? m : M) / Z, C = Math.max(10, se * 0.72);
    return {
      margin: F,
      plotWidth: M,
      plotHeight: m,
      baselineY: ie,
      baselineX: y,
      scaleY: H,
      scaleX: V,
      step: se,
      groupWidth: C,
      minValue: l,
      maxValue: u,
      hasHierarchicalBands: c,
      horizontalBarMode: it
    };
  }, [a, t, n, ft, Ie, Wt, it, $, T, De, s, _, ke, P]), Nt = v.useMemo(() => {
    if (!x || x.horizontalBarMode || x.hasHierarchicalBands)
      return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const o = Array.isArray(t && t.categories) ? t.categories : [];
    if (o.length <= 1) return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const c = x.step < 42 ? 12 : x.step < 70 ? 16 : 20, l = o.reduce((k, b) => Math.max(k, Ct(he(b, c), 11)), 0), u = Math.abs(ke) * (Math.PI / 180), p = ke === 0 ? l : Math.abs(Math.cos(u)) * l + Math.abs(Math.sin(u)) * 12, f = Math.max(1, Math.ceil((p + 12) / Math.max(x.step, 1)));
    if (f === 1) return { visibleIndexes: null, maxChars: c, stride: f };
    const j = /* @__PURE__ */ new Set();
    for (let k = 0; k < o.length; k += f) j.add(k);
    const h = o.length - 1, w = Math.floor(h / f) * f;
    return h - w >= Math.ceil(f * 0.6) && j.add(h), { visibleIndexes: j, maxChars: c, stride: f };
  }, [ke, x, t]);
  if (n === "sparkline")
    return /* @__PURE__ */ e.jsxs(
      "div",
      {
        ref: He,
        "data-chart-sparkline-surface": "true",
        style: {
          position: "relative",
          minHeight: `${$}px`,
          height: `${$}px`,
          minWidth: 0
        },
        children: [
          /* @__PURE__ */ e.jsx(
            es,
            {
              model: t,
              theme: r,
              chartHeight: $,
              paletteColors: je,
              valueFormat: te,
              referenceLines: X
            }
          ),
          Re
        ]
      }
    );
  if (n === "icicle") {
    if (ht.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Icicle charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: $ });
    const o = $ / at, c = nn(ht, 0, T, 0, o, []);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
      /* @__PURE__ */ e.jsx(
        "svg",
        {
          ref: N,
          viewBox: `0 0 ${T} ${$}`,
          preserveAspectRatio: "xMidYMid meet",
          style: {
            width: "100%",
            height: "auto",
            display: "block",
            maxWidth: "100%"
          },
          role: "img",
          "aria-label": t.title || "Icicle chart",
          children: c.map((l, u) => {
            const p = Ae(Math.max(0, l.depth - 1), r, je), f = l.width >= 54;
            return /* @__PURE__ */ e.jsxs(
              "g",
              {
                onClick: () => {
                  typeof B == "function" && B(l);
                },
                style: typeof B == "function" ? { cursor: "pointer" } : void 0,
                children: [
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: l.x,
                      y: l.y,
                      width: Math.max(1, l.width - 1),
                      height: Math.max(20, l.height - 2),
                      rx: "4",
                      fill: p,
                      opacity: "0.86",
                      stroke: r.surfaceBg || r.background || "#fff",
                      strokeWidth: "1",
                      "data-tip-cat": l.label || "",
                      "data-tip-val": typeof l.value == "number" ? P(l.value) : "",
                      "data-tip-color": p
                    }
                  ),
                  f ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: l.x + 8,
                      y: l.y + Math.min(l.height - 10, 22),
                      textAnchor: "start",
                      fontSize: "11",
                      fontWeight: "700",
                      fill: "#fff",
                      style: { pointerEvents: "none" },
                      children: he(l.label, Math.max(6, Math.floor(l.width / 9)))
                    }
                  ) : null
                ]
              },
              `icicle-node-${l.rowPath || u}`
            );
          })
        }
      ),
      Re,
      Se,
      Ne,
      Ye
    ] }) });
  }
  if (n === "sunburst") {
    if (ht.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Sunburst charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: $ });
    const o = Math.min(T, $) / 2 - 18, c = T / 2, l = $ / 2, u = o / Math.max(at, 1), p = sn(ht, -Math.PI / 2, Math.PI * 3 / 2, 1, u, []);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
      /* @__PURE__ */ e.jsx(
        "svg",
        {
          ref: N,
          viewBox: `0 0 ${T} ${$}`,
          preserveAspectRatio: "xMidYMid meet",
          style: {
            width: "100%",
            height: "auto",
            display: "block",
            maxWidth: "100%"
          },
          role: "img",
          "aria-label": t.title || "Sunburst chart",
          children: p.map((f, j) => {
            const h = Ae(Math.max(0, f.depth - 1), r, je), w = Lr(c, l, f.innerRadius, f.outerRadius, f.startAngle, f.endAngle), k = f.startAngle + (f.endAngle - f.startAngle) / 2, b = f.innerRadius + (f.outerRadius - f.innerRadius) / 2, I = ir(c, l, b, k), F = f.endAngle - f.startAngle, M = F > 0.2 && f.outerRadius - f.innerRadius >= 18;
            return /* @__PURE__ */ e.jsxs(
              "g",
              {
                onClick: () => {
                  typeof B == "function" && B(f);
                },
                style: typeof B == "function" ? { cursor: "pointer" } : void 0,
                children: [
                  /* @__PURE__ */ e.jsx(
                    "path",
                    {
                      d: w,
                      fill: h,
                      opacity: "0.88",
                      stroke: r.surfaceBg || r.background || "#fff",
                      strokeWidth: "1",
                      "data-tip-cat": f.label || "",
                      "data-tip-val": typeof f.value == "number" ? P(f.value) : "",
                      "data-tip-color": h
                    }
                  ),
                  M ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: I.x,
                      y: I.y,
                      textAnchor: "middle",
                      fontSize: "10",
                      fontWeight: "700",
                      fill: "#fff",
                      style: { pointerEvents: "none" },
                      children: he(f.label, Math.max(5, Math.floor(F * 24)))
                    }
                  ) : null
                ]
              },
              `sunburst-node-${f.rowPath || j}`
            );
          })
        }
      ),
      Re,
      Se,
      Ne,
      Ye
    ] }) });
  }
  if (n === "sankey") {
    const o = Array.isArray(t && t.sankeyNodes) ? t.sankeyNodes : [], c = Array.isArray(t && t.sankeyLinks) ? t.sankeyLinks : [];
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Sankey charts need expanded hierarchical rows with child flows.", theme: r, chartHeight: $ });
    const l = on(o, c, T, $), u = l.nodes.length > 0 ? Math.max(...l.nodes.map((p) => p.x)) : 0;
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
      /* @__PURE__ */ e.jsxs(
        "svg",
        {
          ref: N,
          viewBox: `0 0 ${T} ${$}`,
          preserveAspectRatio: "xMidYMid meet",
          style: {
            width: "100%",
            height: "auto",
            display: "block",
            maxWidth: "100%",
            overflow: "visible"
          },
          role: "img",
          "aria-label": t.title || "Sankey chart",
          children: [
            l.links.map((p, f) => {
              const j = p.sourceNode.x + p.sourceNode.width, h = p.targetNode.x, w = j + (h - j) / 2, k = [
                `M ${j} ${p.sourceY}`,
                `C ${w} ${p.sourceY}, ${w} ${p.targetY}, ${h} ${p.targetY}`
              ].join(" ");
              return /* @__PURE__ */ e.jsx(
                "path",
                {
                  d: k,
                  fill: "none",
                  stroke: r.primary,
                  strokeOpacity: "0.22",
                  strokeWidth: p.thickness,
                  "data-tip-cat": `${p.sourceNode.label || ""} → ${p.targetNode.label || ""}`,
                  "data-tip-val": typeof p.value == "number" ? P(p.value) : ""
                },
                `sankey-link-${p.source}-${p.target}-${f}`
              );
            }),
            l.nodes.map((p, f) => {
              const j = Ae(Math.max(0, p.depth - 1), r, je);
              return /* @__PURE__ */ e.jsxs(
                "g",
                {
                  onClick: () => {
                    typeof B == "function" && B(p);
                  },
                  style: typeof B == "function" ? { cursor: "pointer" } : void 0,
                  children: [
                    /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: p.x,
                        y: p.y,
                        width: p.width,
                        height: p.height,
                        rx: "4",
                        fill: j,
                        opacity: "0.92",
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "1",
                        "data-tip-cat": p.label || "",
                        "data-tip-val": typeof p.value == "number" ? P(p.value) : "",
                        "data-tip-color": j
                      }
                    ),
                    /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: p.x === u ? p.x - 6 : p.x + p.width + 6,
                        y: p.y + Math.min(p.height / 2 + 4, p.height - 4),
                        textAnchor: p.x === u ? "end" : "start",
                        fontSize: "11",
                        fill: r.text,
                        fontWeight: "700",
                        style: { pointerEvents: "none" },
                        children: he(p.label, 24)
                      }
                    )
                  ]
                },
                `sankey-node-${p.rowPath || f}`
              );
            })
          ]
        }
      ),
      Re,
      Se,
      Ne,
      Ye
    ] }) });
  }
  if (n === "pie" || n === "donut") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, c = Array.isArray(t && t.categories) ? t.categories : [];
    if (!o || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Pie charts need categories and at least one numeric measure.", theme: r, chartHeight: $ });
    const l = c.map((M, m) => {
      const z = o.values[m], R = typeof z == "number" && Number.isFinite(z) ? z : 0;
      return { category: M, value: Math.abs(R), originalValue: R, index: m };
    }).filter((M) => M.value > 0 && !J.has(M.category)), u = l.reduce((M, m) => M + m.value, 0);
    if (u === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "All values are zero — nothing to display.", theme: r, chartHeight: $ });
    const p = n === "donut", f = Math.min(T, $) / 2 - 24, j = p ? f * 0.52 : 0, h = T / 2, w = $ / 2, k = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    let b = -Math.PI / 2;
    const I = l.map((M) => {
      const m = M.value / u, z = m * Math.PI * 2, R = b, H = b + z;
      return b = H, { ...M, startAngle: R, endAngle: H, fraction: m };
    }), F = I.map((M) => ({
      label: `${M.category} (${(M.fraction * 100).toFixed(1)}%)`,
      color: Ae(M.index, r, je)
    }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: N,
            viewBox: `0 0 ${T} ${$}`,
            preserveAspectRatio: "xMidYMid meet",
            style: {
              width: "100%",
              height: "auto",
              display: "block",
              maxWidth: "100%"
            },
            role: "img",
            "aria-label": t.title || (p ? "Donut chart" : "Pie chart"),
            children: [
              I.map((M) => {
                const m = Ae(M.index, r, je), z = Lr(h, w, j, f, M.startAngle, M.endAngle), R = M.startAngle + (M.endAngle - M.startAngle) / 2, H = p ? j + (f - j) / 2 : f * 0.62, V = ir(h, w, H, R), ie = M.endAngle - M.startAngle > 0.3, y = ir(h, w, f + (ye ? 26 : 14), R), Z = M.endAngle - M.startAngle > 0.15, se = k[M.index] || null;
                return /* @__PURE__ */ e.jsxs(
                  "g",
                  {
                    onClick: () => {
                      typeof B == "function" && se && B(se);
                    },
                    style: typeof B == "function" && se ? { cursor: "pointer" } : void 0,
                    children: [
                      /* @__PURE__ */ e.jsx(
                        "path",
                        {
                          d: z,
                          fill: m,
                          opacity: "0.9",
                          stroke: r.surfaceBg || r.background || "#fff",
                          strokeWidth: "2",
                          "data-tip-cat": M.category,
                          "data-tip-ser": o.name,
                          "data-tip-val": `${P(M.originalValue)} (${(M.fraction * 100).toFixed(1)}%)`,
                          "data-tip-color": m
                        }
                      ),
                      ie ? /* @__PURE__ */ e.jsxs(
                        "text",
                        {
                          x: V.x,
                          y: V.y,
                          textAnchor: "middle",
                          dominantBaseline: "central",
                          fontSize: "10",
                          fontWeight: "700",
                          fill: "#fff",
                          style: { pointerEvents: "none" },
                          children: [
                            (M.fraction * 100).toFixed(1),
                            "%"
                          ]
                        }
                      ) : null,
                      (A || ye) && Z ? /* @__PURE__ */ e.jsxs("g", { children: [
                        ye ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx(
                            "line",
                            {
                              x1: ir(h, w, f * 0.97, R).x,
                              y1: ir(h, w, f * 0.97, R).y,
                              x2: y.x,
                              y2: y.y,
                              stroke: m,
                              strokeWidth: "1",
                              style: { pointerEvents: "none" }
                            }
                          ),
                          /* @__PURE__ */ e.jsx(
                            "text",
                            {
                              x: R > Math.PI / 2 && R < 3 * Math.PI / 2 ? y.x - 4 : y.x + 4,
                              y: y.y + 4,
                              textAnchor: R > Math.PI / 2 && R < 3 * Math.PI / 2 ? "end" : "start",
                              fontSize: "9",
                              fontWeight: "700",
                              fill: r.textSec,
                              style: { pointerEvents: "none" },
                              children: he(M.category, 14)
                            }
                          )
                        ] }) : null,
                        A ? /* @__PURE__ */ e.jsx(
                          "text",
                          {
                            x: y.x,
                            y: ye ? y.y + 14 : y.y,
                            textAnchor: R > Math.PI / 2 && R < 3 * Math.PI / 2 ? "end" : "start",
                            dominantBaseline: "central",
                            fontSize: "9",
                            fontWeight: "700",
                            fill: r.textSec,
                            style: { pointerEvents: "none" },
                            children: P(M.originalValue)
                          }
                        ) : null
                      ] }) : null
                    ]
                  },
                  `pie-slice-${M.index}`
                );
              }),
              p ? /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: h,
                  y: w,
                  textAnchor: "middle",
                  dominantBaseline: "central",
                  fontSize: "16",
                  fontWeight: "800",
                  fill: r.text,
                  style: { pointerEvents: "none" },
                  children: P(u)
                }
              ) : null
            ]
          }
        ),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: F, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (n === "bubble") {
    const o = Array.isArray(t && t.series) ? t.series.filter((D) => !J.has(D.name)) : [], c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "Bubble charts need at least two measures (X and Y). A third measure sets bubble size.", theme: r, chartHeight: $ });
    const u = o[0], p = o[1], f = o[2] || null, j = (u.values || []).filter((D) => typeof D == "number" && Number.isFinite(D)), h = (p.values || []).filter((D) => typeof D == "number" && Number.isFinite(D));
    if (j.length === 0 || h.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Not enough numeric data for a bubble chart.", theme: r, chartHeight: $ });
    let w = Math.min(...j), k = Math.max(...j), b = Math.min(...h), I = Math.max(...h);
    if (w === k) {
      const D = Math.abs(k || 1) * 0.15 || 1;
      w -= D, k += D;
    }
    if (b === I) {
      const D = Math.abs(I || 1) * 0.15 || 1;
      b -= D, I += D;
    }
    const F = (k - w) * 0.08, M = (I - b) * 0.08;
    w -= F, k += F, b -= M, I += M;
    const m = f ? (f.values || []).filter((D) => typeof D == "number" && Number.isFinite(D)) : [], z = m.length ? Math.min(...m) : 0, H = (m.length ? Math.max(...m) : 1) - z || 1, V = (D) => {
      if (!f) return 7;
      const ne = f.values && f.values[D];
      return typeof ne != "number" || !Number.isFinite(ne) ? 7 : 4 + (ne - z) / H * 20;
    }, y = { top: 24, right: 20, bottom: 64, left: Math.max(52, Ct(P(I), 11) + 16) }, Z = T - y.left - y.right, se = $ - y.top - y.bottom, C = (D) => y.left + (D - w) / (k - w) * Z, L = (D) => y.top + (I - D) / (I - b) * se, le = It(w, k, 5), oe = It(b, I, 5), re = ve(p.name, 0);
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: N, viewBox: `0 0 ${T} ${$}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", overflow: "visible", ...Ue }, children: [
          oe.map((D, ne) => /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx("line", { x1: y.left, y1: L(D), x2: T - y.right, y2: L(D), stroke: r.border, strokeDasharray: "3 4" }),
            /* @__PURE__ */ e.jsx("text", { x: y.left - 8, y: L(D) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: P(D) })
          ] }, `y-${ne}`)),
          le.map((D, ne) => /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx("line", { x1: C(D), y1: y.top, x2: C(D), y2: $ - y.bottom, stroke: r.border, strokeDasharray: "3 4" }),
            /* @__PURE__ */ e.jsx("text", { x: C(D), y: $ - y.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, children: P(D) })
          ] }, `x-${ne}`)),
          /* @__PURE__ */ e.jsx("line", { x1: y.left, y1: $ - y.bottom, x2: T - y.right, y2: $ - y.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("line", { x1: y.left, y1: y.top, x2: y.left, y2: $ - y.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("text", { x: T / 2, y: $ - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, children: u.name }),
          Q ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (y.top + $ - y.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(y.top + $ - y.bottom) / 2})`, children: Q }) : null,
          (u.values || []).map((D, ne) => {
            const ce = p.values[ne];
            if (typeof D != "number" || !Number.isFinite(D) || typeof ce != "number" || !Number.isFinite(ce)) return null;
            const Pe = V(ne), mt = c[ne] || `Point ${ne + 1}`, Yt = f ? ` | ${f.name}: ${P(f.values[ne])}` : "", cr = { "data-tip-cat": mt, "data-tip-ser": `${p.name} (X:${P(D)})${Yt}`, "data-tip-val": P(ce), "data-tip-color": re }, Pt = l[ne];
            return /* @__PURE__ */ e.jsx(
              "circle",
              {
                cx: C(D),
                cy: L(ce),
                r: Pe,
                fill: re,
                opacity: "0.72",
                stroke: r.surfaceBg || r.background || "#fff",
                strokeWidth: "1.5",
                style: typeof B == "function" && Pt ? { cursor: "pointer" } : void 0,
                onClick: typeof B == "function" && Pt ? () => B(Pt) : void 0,
                ...cr
              },
              `bubble-${ne}`
            );
          }),
          A && (u.values || []).map((D, ne) => {
            const ce = p.values[ne];
            if (typeof D != "number" || !Number.isFinite(D) || typeof ce != "number" || !Number.isFinite(ce)) return null;
            const Pe = V(ne);
            return /* @__PURE__ */ e.jsx("text", { x: C(D), y: L(ce) - Pe - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: c[ne] || P(ce) }, `bubble-lbl-${ne}`);
          })
        ] }),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: [{ label: `${u.name} vs ${p.name}${f ? ` (size: ${f.name})` : ""}`, color: re }], theme: r, hiddenSet: J, onToggle: Ke, position: Oe }) : null
    ] });
  }
  if (n === "radar") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((M) => !J.has(M.name)) : [];
    if (o.length < 3)
      return /* @__PURE__ */ e.jsx(we, { message: "Radar charts need at least 3 categories (spokes).", theme: r, chartHeight: $ });
    if (c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Radar charts need at least one measure series.", theme: r, chartHeight: $ });
    const l = c.flatMap((M) => (M.values || []).filter((m) => typeof m == "number" && Number.isFinite(m))), u = l.length ? Math.max(...l, 0) : 1, p = 60, f = T / 2, j = $ / 2, h = Math.min(f, j) - p, w = o.length, k = 2 * Math.PI / w, b = (M, m) => {
      const z = M * k - Math.PI / 2, R = h * (m / (u || 1));
      return { x: f + R * Math.cos(z), y: j + R * Math.sin(z) };
    }, I = (M) => {
      const m = M * k - Math.PI / 2, z = h + 18;
      return { x: f + z * Math.cos(m), y: j + z * Math.sin(m) };
    }, F = c.map((M, m) => ({ label: M.name, color: ve(M.name, m) }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: N, viewBox: `0 0 ${T} ${$}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: [
          [0.25, 0.5, 0.75, 1].map((M) => {
            const m = o.map((z, R) => b(R, u * M));
            return /* @__PURE__ */ e.jsx("polygon", { points: m.map((z) => `${z.x},${z.y}`).join(" "), fill: "none", stroke: r.border, strokeWidth: "1" }, `grid-${M}`);
          }),
          o.map((M, m) => {
            const z = b(m, u);
            return /* @__PURE__ */ e.jsx("line", { x1: f, y1: j, x2: z.x, y2: z.y, stroke: r.border, strokeWidth: "1" }, `spoke-${m}`);
          }),
          o.map((M, m) => {
            const z = I(m), R = Math.abs(z.x - f) < 10 ? "middle" : z.x < f ? "end" : "start";
            return /* @__PURE__ */ e.jsx("text", { x: z.x, y: z.y + 4, textAnchor: R, fontSize: "10", fill: r.textSec, children: he(M, 14) }, `spoke-lbl-${m}`);
          }),
          c.map((M, m) => {
            const z = ve(M.name, m), R = o.map((V, ie) => {
              const y = M.values && M.values[ie];
              return b(ie, typeof y == "number" && Number.isFinite(y) ? y : 0);
            }), H = R.map((V) => `${V.x},${V.y}`).join(" ");
            return /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx("polygon", { points: H, fill: z, fillOpacity: "0.18", stroke: z, strokeWidth: "2", style: { pointerEvents: "none" } }),
              R.map((V, ie) => {
                const y = M.values && M.values[ie], Z = { "data-tip-cat": o[ie], "data-tip-ser": M.name, "data-tip-val": P(y), "data-tip-color": z };
                return /* @__PURE__ */ e.jsx("circle", { cx: V.x, cy: V.y, r: "4", fill: z, stroke: r.surfaceBg || "#fff", strokeWidth: "1.5", ...Z }, `radar-dot-${m}-${ie}`);
              })
            ] }, `radar-${m}`);
          }),
          [0.5, 1].map((M) => /* @__PURE__ */ e.jsx("text", { x: f + 4, y: j - h * M + 4, fontSize: "9", fill: r.textSec, children: P(u * M) }, `scale-${M}`))
        ] }),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: F, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (n === "funnel") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!c || o.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Funnel charts need categories and at least one measure.", theme: r, chartHeight: $ });
    const l = o.map((k, b) => {
      const I = c.values && c.values[b];
      return typeof I == "number" && Number.isFinite(I) ? Math.abs(I) : 0;
    }), u = Math.max(...l, 1), p = { top: 24, right: 20, bottom: 24, left: 20 }, f = T - p.left - p.right, h = ($ - p.top - p.bottom) / o.length, w = o.map((k, b) => ({ label: k, color: ve(k, b) }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsx("svg", { ref: N, viewBox: `0 0 ${T} ${$}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: o.map((k, b) => {
          const I = l[b] / u, F = b + 1 < o.length ? l[b + 1] / u : l[b] / u * 0.5, M = f * I, m = f * F, z = T / 2, R = p.top + b * h, H = ve(k, b), V = [
            `${z - M / 2},${R}`,
            `${z + M / 2},${R}`,
            `${z + m / 2},${R + h - 1}`,
            `${z - m / 2},${R + h - 1}`
          ].join(" ");
          return /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx(
              "polygon",
              {
                points: V,
                fill: H,
                opacity: "0.88",
                stroke: r.surfaceBg || "#fff",
                strokeWidth: "1.5",
                "data-tip-cat": k,
                "data-tip-ser": c.name,
                "data-tip-val": P(l[b]),
                "data-tip-color": H
              }
            ),
            /* @__PURE__ */ e.jsxs("text", { x: z, y: R + h / 2 + 4, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: "#fff", style: { pointerEvents: "none" }, children: [
              he(k, Math.max(4, Math.floor(M / 7))),
              " ",
              A ? `— ${P(l[b])}` : ""
            ] })
          ] }, `funnel-${b}`);
        }) }),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: w, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (n === "histogram") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!o)
      return /* @__PURE__ */ e.jsx(we, { message: "Histogram needs at least one numeric measure.", theme: r, chartHeight: $ });
    const c = (o.values || []).filter((R) => typeof R == "number" && Number.isFinite(R));
    if (c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "No numeric values for histogram.", theme: r, chartHeight: $ });
    const l = Math.max(4, Math.min(20, Math.ceil(1 + Math.log2(c.length)))), u = Math.min(...c), f = (Math.max(...c) - u) / l || 1, j = Array.from({ length: l }, (R, H) => ({
      low: u + H * f,
      high: u + (H + 1) * f,
      count: 0
    }));
    c.forEach((R) => {
      const H = Math.min(Math.floor((R - u) / f), l - 1);
      j[H].count++;
    });
    const h = Math.max(...j.map((R) => R.count), 1), k = { top: 24, right: 16, bottom: 44, left: Math.max(46, Ct(String(h), 11) + 16) }, b = T - k.left - k.right, I = $ - k.top - k.bottom, F = b / l, M = (R) => k.top + (h - R) / h * I, m = ve(o.name, 0), z = It(0, h, U || 5);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
      /* @__PURE__ */ e.jsxs("svg", { ref: N, viewBox: `0 0 ${T} ${$}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: [
        z.map((R, H) => /* @__PURE__ */ e.jsxs("g", { children: [
          /* @__PURE__ */ e.jsx("line", { x1: k.left, y1: M(R), x2: T - k.right, y2: M(R), stroke: r.border, strokeDasharray: "3 4" }),
          /* @__PURE__ */ e.jsx("text", { x: k.left - 8, y: M(R) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: R })
        ] }, `htick-${H}`)),
        /* @__PURE__ */ e.jsx("line", { x1: k.left, y1: k.top + I, x2: T - k.right, y2: k.top + I, stroke: r.textSec, strokeWidth: "1" }),
        j.map((R, H) => {
          const V = k.left + H * F, ie = M(R.count), y = Math.max(1, k.top + I - ie);
          return /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx(
              "rect",
              {
                x: V,
                y: ie,
                width: Math.max(1, F - 1),
                height: y,
                fill: m,
                opacity: "0.88",
                "data-tip-cat": `${P(R.low)}–${P(R.high)}`,
                "data-tip-ser": "Count",
                "data-tip-val": String(R.count),
                "data-tip-color": m
              }
            ),
            A && R.count > 0 ? /* @__PURE__ */ e.jsx("text", { x: V + F / 2, y: ie - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: R.count }) : null
          ] }, `hbar-${H}`);
        }),
        j.filter((R, H) => H % Math.max(1, Math.floor(l / 6)) === 0 || H === l - 1).map((R, H) => /* @__PURE__ */ e.jsx("text", { x: k.left + j.indexOf(R) * F + F / 2, y: k.top + I + 18, textAnchor: "middle", fontSize: "10", fill: r.textSec, children: P(R.low) }, `hlbl-${H}`))
      ] }),
      Re,
      Se,
      Ne,
      Ye
    ] }) });
  }
  if (n === "boxplot") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((k) => !J.has(k.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", f = r.gridLine || r.border || "#eee";
    if (c.length < 5)
      return /* @__PURE__ */ e.jsx(we, { message: "Box Plot needs 5 measures: Min, Q1, Median, Q3, Max.", theme: r, chartHeight: $ });
    const j = o.map((k, b) => [
      c[0].values[b] ?? 0,
      c[1].values[b] ?? 0,
      c[2].values[b] ?? 0,
      c[3].values[b] ?? 0,
      c[4].values[b] ?? 0
    ]), h = ve(c[2].name, 0), w = {
      backgroundColor: l,
      tooltip: { trigger: "axis", formatter: (k) => {
        const b = k[0];
        return `${b.name}<br/>Min: ${P(b.data[1])}<br/>Q1: ${P(b.data[2])}<br/>Median: ${P(b.data[3])}<br/>Q3: ${P(b.data[4])}<br/>Max: ${P(b.data[5])}`;
      } },
      xAxis: { type: "category", data: o.map((k) => he(String(k), 14)), axisLabel: { color: u, fontSize: 10 }, axisLine: { lineStyle: { color: p } } },
      yAxis: { type: "value", axisLabel: { color: u, fontSize: 10, formatter: P }, axisLine: { lineStyle: { color: p } }, splitLine: { lineStyle: { color: f } }, name: Q || "", nameTextStyle: { color: u } },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      series: [{ type: "boxplot", data: j, itemStyle: { color: h, borderColor: h } }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: g, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(Jt, { chartType: n, echartsRef: E, option: w }) });
  }
  if (n === "nightingale") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((j) => !J.has(j.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333";
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Nightingale charts need categories and at least one measure.", theme: r, chartHeight: $ });
    const p = o.map((j, h) => {
      const w = c[0] && c[0].values ? c[0].values[h] ?? 0 : 0;
      return { name: String(j), value: w, itemStyle: { color: ve(String(j), h) } };
    }), f = {
      backgroundColor: l,
      tooltip: { trigger: "item", formatter: (j) => `${j.name}: ${P(j.value)}` },
      legend: { show: S, textStyle: { color: u }, type: "scroll" },
      series: [{
        type: "bar",
        coordinateSystem: "polar",
        data: p,
        label: { show: A, position: "outside", formatter: (j) => P(j.value), color: u, fontSize: 10 }
      }],
      polar: { radius: ["10%", "75%"] },
      angleAxis: { type: "category", data: o.map((j) => he(String(j), 12)), axisLabel: { color: u, fontSize: 9 } },
      radiusAxis: { axisLabel: { color: u, fontSize: 9, formatter: P } }
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: g, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(Jt, { chartType: n, echartsRef: E, option: f }) });
  }
  if (n === "heatmap") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((H) => !J.has(H.name)) : [];
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Heatmap needs categories and at least one measure.", theme: r, chartHeight: $ });
    const l = c.flatMap((H) => (H.values || []).filter((V) => typeof V == "number" && Number.isFinite(V))), u = l.length ? Math.min(...l) : 0, f = (l.length ? Math.max(...l) : 1) - u || 1, j = (H) => {
      const V = Math.max(0, Math.min(1, (H - u) / f)), ie = Math.round(59 + V * 180), y = Math.round(130 - V * 62), Z = Math.round(246 - V * 178);
      return `rgb(${ie},${y},${Z})`;
    }, h = 2, w = Math.min(140, Math.max(60, c.reduce((H, V) => Math.max(H, Ct(V.name, 11)), 0) + 10)), k = 32, b = T, I = $, F = b - w - 8, M = I - k - 8, m = Math.max(20, F / o.length), z = Math.max(20, M / c.length), R = c.map((H, V) => ({ label: H.name, color: Ae(V, r, je) }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: N,
            viewBox: `0 0 ${b} ${I}`,
            preserveAspectRatio: "xMidYMid meet",
            style: { width: "100%", height: "auto", display: "block", maxWidth: "100%", ...Ue },
            role: "img",
            "aria-label": t.title || "Heatmap",
            children: [
              o.map((H, V) => /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: w + V * m + m / 2,
                  y: k - 6,
                  textAnchor: "middle",
                  fontSize: "10",
                  fill: r.textSec,
                  style: { pointerEvents: "none" },
                  children: he(H, Math.max(4, Math.floor(m / 6.5)))
                },
                `hm-col-${V}`
              )),
              c.map((H, V) => /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: w - 6,
                  y: k + V * z + z / 2 + 4,
                  textAnchor: "end",
                  fontSize: "11",
                  fill: r.textSec,
                  style: { pointerEvents: "none" },
                  children: he(H.name, Math.max(6, Math.floor(w / 6.5)))
                },
                `hm-row-${V}`
              )),
              c.map(
                (H, V) => o.map((ie, y) => {
                  const Z = H.values && H.values[y];
                  if (Z == null) return null;
                  const se = j(Z), C = w + y * m, L = k + V * z;
                  return /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: C + h,
                      y: L + h,
                      width: Math.max(1, m - h * 2),
                      height: Math.max(1, z - h * 2),
                      rx: "3",
                      fill: se,
                      "data-tip-cat": ie,
                      "data-tip-ser": H.name,
                      "data-tip-val": P(Z),
                      "data-tip-color": se
                    },
                    `hm-cell-${V}-${y}`
                  );
                })
              ),
              A && c.map(
                (H, V) => o.map((ie, y) => {
                  const Z = H.values && H.values[y];
                  if (Z == null || m < 36 || z < 18) return null;
                  const C = Math.max(0, Math.min(1, (Z - u) / f)) > 0.5 ? "#fff" : r.text;
                  return /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: w + y * m + m / 2,
                      y: k + V * z + z / 2 + 4,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: C,
                      style: { pointerEvents: "none" },
                      children: P(Z)
                    },
                    `hm-label-${V}-${y}`
                  );
                })
              )
            ]
          }
        ),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: R, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (n === "bar3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((h) => !J.has(h.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", f = [];
    c.forEach((h, w) => {
      o.forEach((k, b) => {
        const I = h.values[b];
        I != null && f.push([b, w, I]);
      });
    });
    const j = {
      backgroundColor: l,
      legend: {
        show: S,
        data: c.map((h) => h.name),
        textStyle: { color: u },
        type: "scroll"
      },
      tooltip: {
        formatter: (h) => {
          const w = o[h.value[0]] || "", k = c[h.value[1]] ? c[h.value[1]].name : "";
          return `${w} / ${k}: <b>${P(h.value[2])}</b>`;
        }
      },
      grid3D: {
        boxWidth: 200,
        boxDepth: 80,
        viewControl: { projection: "perspective", autoRotate: !1 },
        light: { main: { intensity: 1.2, shadow: !1 }, ambient: { intensity: 0.4 } }
      },
      xAxis3D: {
        type: "category",
        data: o.map((h) => he(String(h), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } },
        name: ""
      },
      yAxis3D: {
        type: "category",
        data: c.map((h) => he(String(h.name), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      zAxis3D: {
        type: "value",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        name: Q || "",
        nameTextStyle: { color: u }
      },
      series: [{
        type: "bar3D",
        data: f.map((h) => ({
          value: h,
          itemStyle: { color: Ae(h[1], r, je), opacity: 0.9 }
        })),
        shading: "lambert",
        label: {
          show: A,
          formatter: (h) => P(h.value[2]),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { label: { show: !0 }, itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: g, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(Jt, { chartType: n, echartsRef: E, option: j }) });
  }
  if (n === "line3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((h) => !J.has(h.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", f = c.map((h, w) => ({
      type: "line3D",
      data: o.map((k, b) => {
        const I = h.values[b];
        return [b, w, I ?? 0];
      }),
      lineStyle: { color: Ae(w, r, je), width: 3, opacity: 0.9 },
      name: h.name
    })), j = {
      backgroundColor: l,
      legend: { show: S, data: c.map((h) => h.name), textStyle: { color: u }, type: "scroll" },
      tooltip: {
        formatter: (h) => {
          const w = o[h.value[0]] || "", k = c[h.value[1]] ? c[h.value[1]].name : "";
          return `${w} / ${k}: <b>${P(h.value[2])}</b>`;
        }
      },
      grid3D: {
        boxWidth: 200,
        boxDepth: 80,
        viewControl: { projection: "perspective", autoRotate: !1 },
        light: { main: { intensity: 1.2, shadow: !1 }, ambient: { intensity: 0.4 } }
      },
      xAxis3D: {
        type: "category",
        data: o.map((h) => he(String(h), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      yAxis3D: {
        type: "category",
        data: c.map((h) => he(String(h.name), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      zAxis3D: {
        type: "value",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        name: Q || "",
        nameTextStyle: { color: u }
      },
      series: f
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: g, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(Jt, { chartType: n, echartsRef: E, option: j }) });
  }
  if (n === "scatter3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((b) => !J.has(b.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc";
    if (c.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "3D Scatter needs at least two numeric measures (X and Y).", theme: r, chartHeight: $ });
    const f = c[0], j = c[1], h = c[2] || null, w = o.map((b, I) => ({
      value: [f.values[I] ?? 0, j.values[I] ?? 0, h ? h.values[I] ?? 0 : I],
      name: String(b)
    })), k = {
      backgroundColor: l,
      tooltip: {
        formatter: (b) => `${o[b.dataIndex] || ""}<br/>X=${P(b.value[0])}, Y=${P(b.value[1])}, Z=${P(b.value[2])}`
      },
      grid3D: {
        boxWidth: 200,
        boxDepth: 80,
        viewControl: { projection: "perspective", autoRotate: !1 },
        light: { main: { intensity: 1.2, shadow: !1 }, ambient: { intensity: 0.4 } }
      },
      xAxis3D: {
        type: "value",
        name: f.name,
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        nameTextStyle: { color: u }
      },
      yAxis3D: {
        type: "value",
        name: j.name,
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        nameTextStyle: { color: u }
      },
      zAxis3D: {
        type: "value",
        name: h ? h.name : "Index",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        nameTextStyle: { color: u }
      },
      series: [{
        type: "scatter3D",
        data: w,
        symbolSize: 8,
        itemStyle: { color: Ae(0, r, je), opacity: 0.85 },
        label: {
          show: A,
          formatter: (b) => String(o[b.dataIndex] || ""),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: g, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(Jt, { chartType: n, echartsRef: E, option: k }) });
  }
  if (n === "range") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((b) => !J.has(b.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", f = r.gridLine || r.border || "#eee";
    if (c.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "Range chart needs at least two measures (low and high values).", theme: r, chartHeight: $ });
    const j = [];
    for (let b = 0; b + 1 < c.length; b += 2)
      j.push({ low: c[b], high: c[b + 1] });
    const h = [];
    j.forEach((b, I) => {
      const F = Ae(I, r, je);
      h.push({
        type: "bar",
        name: `_base_${I}`,
        data: o.map((M, m) => Math.min(b.low.values[m] ?? 0, b.high.values[m] ?? 0)),
        stack: `range_${I}`,
        itemStyle: { opacity: 0 },
        emphasis: { disabled: !0 },
        silent: !0,
        legendHoverLink: !1
      }), h.push({
        type: "bar",
        name: `${b.low.name} – ${b.high.name}`,
        data: o.map((M, m) => Math.abs((b.high.values[m] ?? 0) - (b.low.values[m] ?? 0))),
        stack: `range_${I}`,
        itemStyle: { color: F, opacity: 0.9 },
        label: {
          show: A,
          position: "inside",
          formatter: (M) => {
            const m = M.dataIndex, z = b.low.values[m] ?? 0, R = b.high.values[m] ?? 0;
            return `${P(Math.min(z, R))}–${P(Math.max(z, R))}`;
          },
          color: u,
          fontSize: 10
        },
        tooltip: {
          formatter: (M) => {
            const m = M.dataIndex;
            return `${o[m]}<br/>${b.low.name}: ${P(b.low.values[m] ?? 0)}<br/>${b.high.name}: ${P(b.high.values[m] ?? 0)}`;
          }
        }
      });
    });
    const w = j.map((b, I) => ({ label: `${b.low.name} – ${b.high.name}`, color: Ae(I, r, je) })), k = {
      backgroundColor: l,
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: o.map((b) => he(String(b), 14)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } },
        splitLine: { show: !1 }
      },
      yAxis: {
        type: "value",
        name: Q || "",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        splitLine: { lineStyle: { color: f } },
        nameTextStyle: { color: u }
      },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      series: h
    };
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: g, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(Jt, { chartType: n, echartsRef: E, option: k }) }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: w, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (n === "scatter") {
    const o = Array.isArray(t && t.series) ? t.series.filter((C) => !J.has(C.name)) : [], c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "Scatter charts need at least two numeric measures (X and Y).", theme: r, chartHeight: $ });
    const u = o[0], p = o.slice(1), f = (u.values || []).filter((C) => typeof C == "number" && Number.isFinite(C)), j = p.flatMap((C) => (C.values || []).filter((L) => typeof L == "number" && Number.isFinite(L)));
    if (f.length === 0 || j.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Not enough numeric data for a scatter chart.", theme: r, chartHeight: $ });
    let h = Math.min(...f), w = Math.max(...f), k = Math.min(...j), b = Math.max(...j);
    if (h === w) {
      const C = Math.abs(w || 1) * 0.15 || 1;
      h -= C, w += C;
    }
    if (k === b) {
      const C = Math.abs(b || 1) * 0.15 || 1;
      k -= C, b += C;
    }
    const I = (w - h) * 0.06, F = (b - k) * 0.06;
    h -= I, w += I, k -= F, b += F;
    const m = { top: 24, right: 20, bottom: 64, left: Math.max(52, Ct(P(b), 11) + 16) }, z = T - m.left - m.right, R = $ - m.top - m.bottom, H = (C) => m.left + (C - h) / (w - h) * z, V = (C) => m.top + (b - C) / (b - k) * R, ie = It(h, w, 5), y = It(k, b, 5), Z = p.map((C, L) => ({ label: C.name, color: ve(C.name, L) })), se = typeof B == "function";
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { style: { position: "relative", width: "100%" }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: N,
            viewBox: `0 0 ${T} ${$}`,
            width: "100%",
            preserveAspectRatio: "xMidYMid meet",
            style: { display: "block", overflow: "visible" },
            children: [
              y.map((C, L) => /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: m.left, y1: V(C), x2: T - m.right, y2: V(C), stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: m.left - 8, y: V(C) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(C) })
              ] }, `y-tick-${L}`)),
              ie.map((C, L) => /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: H(C), y1: m.top, x2: H(C), y2: $ - m.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: H(C), y: $ - m.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(C) })
              ] }, `x-tick-${L}`)),
              /* @__PURE__ */ e.jsx("line", { x1: m.left, y1: $ - m.bottom, x2: T - m.right, y2: $ - m.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("line", { x1: m.left, y1: m.top, x2: m.left, y2: $ - m.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("text", { x: T / 2, y: $ - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: u.name }),
              Q ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (m.top + $ - m.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(m.top + $ - m.bottom) / 2})`, style: { pointerEvents: "none" }, children: Q }) : null,
              p.map((C, L) => {
                const le = ve(C.name, L);
                return (C.values || []).map((oe, re) => {
                  const D = u.values[re];
                  if (typeof D != "number" || !Number.isFinite(D) || typeof oe != "number" || !Number.isFinite(oe)) return null;
                  const ne = H(D), ce = V(oe), Pe = c[re] || `Point ${re + 1}`, mt = l[re], Yt = {
                    "data-tip-cat": Pe,
                    "data-tip-ser": `${C.name} (${u.name}=${P(D)})`,
                    "data-tip-val": P(oe),
                    "data-tip-color": le
                  };
                  return xt.cloneElement(zt(ne, ce, le, Yt, {
                    style: se && mt ? { cursor: "pointer" } : void 0,
                    onClick: se && mt ? () => B(mt) : void 0
                  }), { key: `scatter-${L}-${re}` });
                });
              }),
              A ? p.map((C, L) => {
                const le = ve(C.name, L);
                return (C.values || []).map((oe, re) => {
                  const D = u.values[re];
                  return typeof D != "number" || !Number.isFinite(D) || typeof oe != "number" || !Number.isFinite(oe) ? null : /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: H(D),
                      y: V(oe) - 8,
                      textAnchor: "middle",
                      fontSize: "10",
                      fontWeight: "700",
                      fill: le,
                      style: { pointerEvents: "none" },
                      children: P(oe)
                    },
                    `scatter-label-${L}-${re}`
                  );
                });
              }) : null
            ]
          }
        ),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: Z, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (n === "waterfall") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (!o || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Waterfall charts need categories and at least one numeric measure.", theme: r, chartHeight: $ });
    const u = c.map((C, L) => {
      const le = o.values[L];
      return typeof le == "number" && Number.isFinite(le) ? le : 0;
    }), p = [];
    let f = 0;
    for (let C = 0; C < u.length; C++) {
      const L = f;
      f += u[C], p.push({ label: c[C], start: L, end: f, increment: u[C], type: u[C] >= 0 ? "positive" : "negative" });
    }
    p.push({ label: "Total", start: 0, end: f, increment: f, type: "total" });
    const j = p.map((C) => C.start).concat(p.map((C) => C.end));
    let h = Math.min(...j, 0), w = Math.max(...j, 0);
    if (h === w) {
      const C = Math.abs(w || 1) * 0.15 || 1;
      h -= C, w += C;
    }
    const k = (w - h) * 0.08;
    h -= k, w += k;
    const b = It(h, w, 5), F = { top: 24, right: 20, bottom: 84, left: Math.max(52, Ct(P(w), 11) + 16) }, M = T - F.left - F.right, m = $ - F.top - F.bottom, z = (C) => F.top + (w - C) / (w - h) * m, R = M / p.length, H = Math.max(14, Math.min(60, R * 0.6)), V = "#059669", ie = "#DC2626", y = Ae(0, r, je), Z = typeof B == "function", se = [
      { label: "Increase", color: V },
      { label: "Decrease", color: ie },
      { label: "Total", color: y }
    ];
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: N,
            viewBox: `0 0 ${T} ${$}`,
            width: "100%",
            preserveAspectRatio: "xMidYMid meet",
            style: { display: "block", overflow: "visible" },
            children: [
              b.map((C, L) => /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: F.left, y1: z(C), x2: T - F.right, y2: z(C), stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: F.left - 8, y: z(C) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(C) })
              ] }, `wf-tick-${L}`)),
              /* @__PURE__ */ e.jsx("line", { x1: F.left, y1: z(0), x2: T - F.right, y2: z(0), stroke: r.textSec, strokeOpacity: "0.5" }),
              Q ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (F.top + $ - F.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(F.top + $ - F.bottom) / 2})`, style: { pointerEvents: "none" }, children: Q }) : null,
              p.map((C, L) => {
                const le = F.left + L * R + R / 2, oe = le - H / 2, re = z(Math.max(C.start, C.end)), D = z(Math.min(C.start, C.end)), ne = Math.max(1, D - re), ce = C.type === "total" ? y : C.type === "positive" ? V : ie, Pe = L < l.length ? l[L] : null;
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  L > 0 ? /* @__PURE__ */ e.jsx(
                    "line",
                    {
                      x1: F.left + (L - 1) * R + R / 2 + H / 2,
                      y1: L === p.length - 1 ? z(C.end) : z(C.start),
                      x2: oe,
                      y2: L === p.length - 1 ? z(C.end) : z(C.start),
                      stroke: r.textSec,
                      strokeDasharray: "2 2",
                      strokeOpacity: "0.5"
                    }
                  ) : null,
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: oe,
                      y: re,
                      width: H,
                      height: ne,
                      rx: "3",
                      fill: ce,
                      opacity: "0.9",
                      "data-tip-cat": C.label,
                      "data-tip-ser": C.type === "total" ? "Total" : (C.increment >= 0 ? "+" : "") + P(C.increment),
                      "data-tip-val": P(C.end),
                      "data-tip-color": ce,
                      style: Z && Pe ? { cursor: "pointer" } : void 0,
                      onClick: Z && Pe ? () => B(Pe) : void 0
                    }
                  ),
                  A ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: le,
                      y: re - 5,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: C.type === "total" ? P(C.end) : (C.increment >= 0 ? "+" : "") + P(C.increment)
                    }
                  ) : null,
                  /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: le,
                      y: $ - F.bottom + 16,
                      textAnchor: "middle",
                      fontSize: "10",
                      fontWeight: "600",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: he(C.label, 12)
                    }
                  )
                ] }, `wf-bar-${L}`);
              })
            ]
          }
        ),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: se, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (n === "combo") {
    const o = (Array.isArray(t && t.layers) ? t.layers : []).filter((y) => y && !y.hidden && !J.has(y.name) && Array.isArray(y.values) && y.values.some((Z) => Z !== null));
    if (o.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: t.emptyMessage || "No combo layers are configured.", theme: r, chartHeight: $ });
    const c = o.some((y) => y.axis === "right"), l = { top: 24, right: c ? 76 : 20, bottom: 96, left: 72 }, u = Math.max(1, Array.isArray(t.categories) ? t.categories.length : 0), p = Math.max(1, (T - l.left - l.right) / u), f = Math.max(22, Math.min(72, p * 0.72)), j = o.filter((y) => y.type === "bar"), h = o.filter((y) => y.type === "area"), w = o.filter((y) => y.type === "line"), k = Er(
      o.filter((y) => y.axis !== "right").flatMap((y) => y.values),
      $,
      l
    ), b = c ? Er(
      o.filter((y) => y.axis === "right").flatMap((y) => y.values),
      $,
      l
    ) : k, I = Array.isArray(t.categoryTargets) ? t.categoryTargets : [], F = o.map((y, Z) => ({
      label: y.name,
      color: y.color || Ae(Z, r, je)
    })), M = (y) => y.axis === "right" ? b.scaleY : k.scaleY, m = (y) => y.axis === "right" ? b.baselineY : k.baselineY, z = (y) => (Z) => At(Z, y.valueFormat || te || "auto"), R = o.find((y) => y.axis === "right"), H = R ? z(R) : P, V = 4, ie = Math.max(8, (f - (Math.max(j.length, 1) - 1) * V) / Math.max(j.length, 1));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: N,
            viewBox: `0 0 ${T} ${$}`,
            preserveAspectRatio: "xMidYMid meet",
            style: {
              width: "100%",
              height: "auto",
              display: "block",
              maxWidth: "100%",
              ...Ue
            },
            role: "img",
            "aria-label": t.title || "Combo chart",
            children: [
              k.tickValues.map((y, Z) => {
                const se = k.scaleY(y);
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx(
                    "line",
                    {
                      x1: l.left,
                      x2: T - l.right,
                      y1: se,
                      y2: se,
                      stroke: r.border,
                      strokeOpacity: "0.7",
                      strokeDasharray: "4 4"
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: l.left - 10,
                      y: se + 4,
                      textAnchor: "end",
                      fontSize: "11",
                      fill: r.textSec,
                      children: P(y)
                    }
                  )
                ] }, `combo-left-tick-${Z}`);
              }),
              c ? b.tickValues.map((y, Z) => {
                const se = b.scaleY(y);
                return /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: T - l.right + 10,
                    y: se + 4,
                    textAnchor: "start",
                    fontSize: "11",
                    fill: r.textSec,
                    children: H(y)
                  },
                  `combo-right-tick-${Z}`
                );
              }) : null,
              Q ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (l.top + $ - l.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(l.top + $ - l.bottom) / 2})`, style: { pointerEvents: "none" }, children: Q }) : null,
              /* @__PURE__ */ e.jsx(
                "line",
                {
                  x1: l.left,
                  x2: T - l.right,
                  y1: k.baselineY,
                  y2: k.baselineY,
                  stroke: r.textSec,
                  strokeOpacity: "0.45"
                }
              ),
              j.map((y, Z) => t.categories.map((se, C) => {
                const L = y.values[C];
                if (L == null) return null;
                const le = l.left + C * p + (p - f) / 2, oe = M(y), re = m(y), D = oe(L), ne = y.color || Ae(Z, r, je), ce = le + Z * (ie + V), Pe = z(y);
                return /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: ce,
                      y: Math.min(D, re),
                      width: ie,
                      height: Math.max(1, Math.abs(re - D)),
                      rx: "3",
                      fill: ne,
                      opacity: "0.9",
                      "data-tip-cat": se,
                      "data-tip-ser": y.name,
                      "data-tip-val": Pe(L),
                      "data-tip-color": ne
                    }
                  ),
                  A ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: ce + ie / 2,
                      y: L >= 0 ? Math.min(D, re) - 4 : Math.max(D, re) + 12,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: Pe(L)
                    }
                  ) : null
                ] }, `${y.id}-${se}-${Z}`);
              })),
              h.map((y, Z) => {
                const se = t.categories.map((re, D) => {
                  const ne = y.values[D];
                  if (ne == null) return null;
                  const ce = l.left + D * p + p / 2, Pe = M(y);
                  return {
                    x: ce,
                    y: Pe(ne),
                    baseY: m(y),
                    rawValue: ne
                  };
                }), C = Rr(se), L = ur(se), le = y.color || Ae(j.length + Z, r, je), oe = z(y);
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  C ? /* @__PURE__ */ e.jsx("path", { d: C, fill: le, opacity: "0.16", style: { pointerEvents: "none" } }) : null,
                  L ? /* @__PURE__ */ e.jsx("path", { d: L, fill: "none", stroke: le, strokeWidth: "2.5", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: pr(pt[y.name]), style: { pointerEvents: "none" } }) : null,
                  se.map((re, D) => re ? /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    /* @__PURE__ */ e.jsx(
                      "circle",
                      {
                        cx: re.x,
                        cy: re.y,
                        r: "4",
                        fill: le,
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "2",
                        "data-tip-cat": t.categories[D],
                        "data-tip-ser": y.name,
                        "data-tip-val": oe(re.rawValue),
                        "data-tip-color": le
                      }
                    ),
                    A ? /* @__PURE__ */ e.jsx("text", { x: re.x, y: re.y - 8, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: oe(re.rawValue) }) : null
                  ] }, `${y.id}-area-pt-${D}`) : null)
                ] }, `combo-area-${y.id}`);
              }),
              w.map((y, Z) => {
                const se = t.categories.map((oe, re) => {
                  const D = y.values[re];
                  return D == null ? null : {
                    x: l.left + re * p + p / 2,
                    y: M(y)(D),
                    rawValue: D
                  };
                }), C = ur(se), L = y.color || Ae(j.length + h.length + Z, r, je), le = z(y);
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  C ? /* @__PURE__ */ e.jsx("path", { d: C, fill: "none", stroke: L, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: pr(pt[y.name]), style: { pointerEvents: "none" } }) : null,
                  se.map((oe, re) => oe ? /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    /* @__PURE__ */ e.jsx(
                      "circle",
                      {
                        cx: oe.x,
                        cy: oe.y,
                        r: "4",
                        fill: L,
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "2",
                        "data-tip-cat": t.categories[re],
                        "data-tip-ser": y.name,
                        "data-tip-val": le(oe.rawValue),
                        "data-tip-color": L
                      }
                    ),
                    A ? /* @__PURE__ */ e.jsx("text", { x: oe.x, y: oe.y - 8, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: le(oe.rawValue) }) : null
                  ] }, `${y.id}-point-${re}`) : null)
                ] }, `combo-line-${y.id}`);
              }),
              t.categories.map((y, Z) => {
                const se = l.left + Z * p + p / 2;
                return /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: se,
                    y: $ - l.bottom + 28,
                    textAnchor: "middle",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(y, 18)
                  },
                  `combo-label-${Z}`
                );
              }),
              typeof B == "function" ? t.categories.map((y, Z) => {
                const se = I[Z];
                return se ? /* @__PURE__ */ e.jsx(
                  "rect",
                  {
                    x: l.left + Z * p,
                    y: l.top,
                    width: p,
                    height: $ - l.top - l.bottom,
                    fill: "transparent",
                    style: { cursor: "pointer" },
                    onClick: () => B(se)
                  },
                  `combo-hit-${Z}`
                ) : null;
              }) : null
            ]
          }
        ),
        Re,
        Se,
        Ne,
        Ye
      ] }),
      S ? /* @__PURE__ */ e.jsx(jt, { items: F, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
    ] });
  }
  if (!x)
    return /* @__PURE__ */ e.jsx(we, { message: "No numeric data available for this chart.", theme: r, chartHeight: $ });
  const vr = It(x.minValue, x.maxValue, U || 5), wt = Ie ? Array.from(new Set(
    (t.stackedGroups || []).reduce((o, c) => ((c.segments || []).forEach((l) => {
      l && l.label && o.push(l.label);
    }), o), [])
  )) : [], lr = wt.reduce((o, c, l) => (o[c] = Ae(l, r, je), o), {}), Mt = Ie ? wt.map((o) => ({ label: o, color: lr[o] || Ae(0, r, je) })) : (t.series || []).map((o, c) => ({
    label: o.name,
    color: ve(o.name, c)
  })), qt = x.hasHierarchicalBands ? t.categoryBands.reduce((o, c, l) => {
    const u = o[o.length - 1];
    return u && u.groupKey === c.groupKey ? (u.endIndex = l, o) : (o.push({
      groupKey: c.groupKey,
      outerLabel: c.outerLabel,
      startIndex: l,
      endIndex: l
    }), o);
  }, []) : [], Ot = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [], mr = typeof B == "function" && Ot.length === t.categories.length;
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
    /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
      /* @__PURE__ */ e.jsxs(
        "svg",
        {
          ref: N,
          viewBox: `0 0 ${T} ${$}`,
          preserveAspectRatio: "xMidYMid meet",
          style: {
            width: "100%",
            height: "auto",
            display: "block",
            maxWidth: "100%",
            ...Ue
          },
          role: "img",
          "aria-label": t.title,
          onClick: ue ? lt : void 0,
          children: [
            de ? /* @__PURE__ */ e.jsx("defs", { children: (t.series || []).map((o, c) => {
              const l = ve(o.name, c), u = Cr.find((f) => f.value === nr) || Cr[0], p = Sn(l, et);
              return /* @__PURE__ */ e.jsxs("linearGradient", { id: `pg-grad-${c}`, x1: u.x1, y1: u.y1, x2: u.x2, y2: u.y2, children: [
                /* @__PURE__ */ e.jsx("stop", { offset: p[0].offset, stopColor: l, stopOpacity: p[0].stopOpacity }),
                /* @__PURE__ */ e.jsx("stop", { offset: p[1].offset, stopColor: l, stopOpacity: p[1].stopOpacity })
              ] }, `grad-${c}`);
            }) }) : null,
            vr.map((o, c) => {
              const l = x.scaleY ? x.scaleY(o) : null, u = x.scaleX ? x.scaleX(o) : null, p = De ? `${Math.round(o)}%` : P(o);
              return /* @__PURE__ */ e.jsx("g", { children: x.horizontalBarMode ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                /* @__PURE__ */ e.jsx("line", { x1: u, y1: x.margin.top, x2: u, y2: $ - x.margin.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: u, y: $ - x.margin.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: ue ? { cursor: "pointer" } : void 0, children: p })
              ] }) : /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: l, x2: T - x.margin.right, y2: l, stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: x.margin.left - 8, y: l + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "y", style: ue ? { cursor: "pointer" } : void 0, children: p })
              ] }) }, `tick-${c}`);
            }),
            /* @__PURE__ */ e.jsx(
              "line",
              {
                x1: x.horizontalBarMode ? x.baselineX : x.margin.left,
                y1: x.horizontalBarMode ? x.margin.top : x.baselineY,
                x2: x.horizontalBarMode ? x.baselineX : T - x.margin.right,
                y2: x.horizontalBarMode ? $ - x.margin.bottom : x.baselineY,
                stroke: r.textSec,
                strokeWidth: "1"
              }
            ),
            Q ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (x.margin.top + $ - x.margin.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(x.margin.top + $ - x.margin.bottom) / 2})`, style: { pointerEvents: "none" }, children: Q }) : null,
            Array.isArray(X) && X.length > 0 && X.map((o, c) => {
              const l = Number(o.value);
              if (!Number.isFinite(l)) return null;
              const u = o.color || r.primary, p = o.orient === "v", f = pr(o.dash || "dashed") || "6 3";
              if (p) {
                const h = x.margin.left + l * x.step;
                return h < x.margin.left - 2 || h > T - x.margin.right + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: h, y1: x.margin.top, x2: h, y2: $ - x.margin.bottom, stroke: u, strokeWidth: "1.5", strokeDasharray: f, style: { pointerEvents: "none" } }),
                  o.label ? /* @__PURE__ */ e.jsx("text", { x: h + 4, y: x.margin.top + 12, textAnchor: "start", fontSize: "10", fontWeight: "700", fill: u, style: { pointerEvents: "none" }, children: o.label }) : null
                ] }, `ref-line-${c}`);
              }
              if (!x.scaleY) return null;
              const j = x.scaleY(l);
              return j < x.margin.top - 2 || j > $ - x.margin.bottom + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: j, x2: T - x.margin.right, y2: j, stroke: u, strokeWidth: "1.5", strokeDasharray: f, style: { pointerEvents: "none" } }),
                o.label ? /* @__PURE__ */ e.jsx("text", { x: T - x.margin.right - 4, y: j - 4, textAnchor: "end", fontSize: "10", fontWeight: "700", fill: u, style: { pointerEvents: "none" }, children: o.label }) : null,
                typeof _e == "function" ? /* @__PURE__ */ e.jsx(
                  "circle",
                  {
                    cx: x.margin.left + 22,
                    cy: j,
                    r: 6,
                    fill: u,
                    stroke: "white",
                    strokeWidth: "1.5",
                    style: { cursor: "ns-resize" },
                    onMouseDown: (h) => {
                      h.preventDefault(), h.stopPropagation();
                      const w = h.clientY, k = l, { plotHeight: b, maxValue: I, minValue: F } = x, M = I - F;
                      if (M <= 0) return;
                      const m = (R) => {
                        const H = k - (R.clientY - w) / b * M;
                        _e((V) => V.map(
                          (ie, y) => y === c ? { ...ie, value: String(Math.round(H * 1e3) / 1e3) } : ie
                        ));
                      }, z = () => {
                        window.removeEventListener("mousemove", m), window.removeEventListener("mouseup", z);
                      };
                      window.addEventListener("mousemove", m), window.addEventListener("mouseup", z);
                    }
                  }
                ) : null
              ] }, `ref-line-${c}`);
            }),
            Ht && !x.horizontalBarMode && (n === "bar" || n === "line" || n === "area" || n === "scatter") && Array.isArray(t.series) && t.series.length > 0 && x.scaleY ? t.series.filter((c) => !J.has(c.name)).map((c, l) => {
              const u = c.values || [], p = ot === "moving_avg" ? Xn(u, qe) : _n(u);
              if (!p) return null;
              const f = p.map((k, b) => Number.isFinite(k) ? { x: x.margin.left + b * x.step + x.step / 2, y: x.scaleY(k) } : null), j = ur(f);
              if (!j) return null;
              const h = ve(c.name, l), w = Xt || h;
              return /* @__PURE__ */ e.jsx(
                "path",
                {
                  d: j,
                  fill: "none",
                  stroke: w,
                  strokeWidth: String(Gt || 2),
                  strokeDasharray: "8 4",
                  strokeLinecap: "round",
                  opacity: "0.78",
                  style: { pointerEvents: "none" }
                },
                `trendline-${l}`
              );
            }) : null,
            x.hasHierarchicalBands && qt.map((o, c) => {
              if (c === qt.length - 1) return null;
              const l = x.horizontalBarMode ? x.margin.top + (o.endIndex + 1) * x.step : x.margin.left + (o.endIndex + 1) * x.step;
              return /* @__PURE__ */ e.jsx(
                "line",
                {
                  x1: x.horizontalBarMode ? x.margin.left : l,
                  y1: x.horizontalBarMode ? l : x.margin.top,
                  x2: x.horizontalBarMode ? T - x.margin.right : l,
                  y2: x.horizontalBarMode ? l : $ - x.margin.bottom + 58,
                  stroke: r.border,
                  strokeDasharray: "4 4"
                },
                `group-divider-${o.groupKey}-${c}`
              );
            }),
            Ie && (t.stackedGroups || []).map((o, c) => {
              const l = x.horizontalBarMode ? x.margin.top + c * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + c * x.step + (x.step - x.groupWidth) / 2, u = Math.max(14, x.groupWidth * 0.64);
              let p = 0, f = 0;
              return /* @__PURE__ */ e.jsxs("g", { children: [
                (o.segments || []).map((j) => {
                  const h = j && typeof j.value == "number" && Number.isFinite(j.value) ? j.value : null;
                  if (h === null) return null;
                  const w = h >= 0 ? p : f, k = w + h;
                  h >= 0 ? p = k : f = k;
                  const b = lr[j.label] || Ae(0, r, je);
                  if (x.horizontalBarMode) {
                    const z = x.scaleX(w), R = x.scaleX(k);
                    return /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: Math.min(z, R),
                        y: l,
                        width: Math.max(1, Math.abs(R - z)),
                        height: u,
                        rx: "3",
                        fill: b,
                        opacity: "0.94",
                        "data-tip-cat": o.label,
                        "data-tip-ser": j.label,
                        "data-tip-val": P(h),
                        "data-tip-color": b
                      },
                      `${o.key}-${j.path || j.label}`
                    );
                  }
                  const I = x.scaleY(w), F = x.scaleY(k), M = Math.min(I, F), m = Math.max(1, Math.abs(F - I));
                  return /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: l,
                      y: M,
                      width: u,
                      height: m,
                      rx: "3",
                      fill: b,
                      opacity: "0.94",
                      "data-tip-cat": o.label,
                      "data-tip-ser": j.label,
                      "data-tip-val": P(h),
                      "data-tip-color": b
                    },
                    `${o.key}-${j.path || j.label}`
                  );
                }),
                x.horizontalBarMode ? /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: x.margin.left - 10,
                    y: l + u / 2 + 4,
                    textAnchor: "end",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(o.label, 22)
                  }
                ) : /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: l + u / 2,
                    y: $ - x.margin.bottom + 30,
                    textAnchor: "middle",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(o.label, 20)
                  }
                )
              ] }, `stacked-group-${o.key}`);
            }),
            !Ie && n === "bar" && t.categories.map((o, c) => {
              const l = x.horizontalBarMode ? x.margin.top + c * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + c * x.step + (x.step - x.groupWidth) / 2, u = Wt || De ? Math.max(14, x.groupWidth * 0.64) : Math.max(6, x.groupWidth / Math.max(t.series.length, 1) - 4);
              let p = 0, f = 0;
              return /* @__PURE__ */ e.jsx("g", { children: t.series.map((j, h) => {
                if (J.has(j.name)) return null;
                const w = j.values[c];
                if (w == null) return null;
                const k = ve(j.name, h), b = de ? `url(#pg-grad-${h})` : k;
                if (Wt || De) {
                  const m = De && me ? me[c] : null, z = De && m ? w / m * 100 : w, R = z >= 0 ? p : f, H = R + z;
                  z >= 0 ? p = H : f = H;
                  const V = De ? `${(Math.abs(w / m) * 100).toFixed(1)}% (${P(w)})` : P(w), ie = { "data-tip-cat": o, "data-tip-ser": j.name, "data-tip-val": V, "data-tip-color": k };
                  if (x.horizontalBarMode) {
                    const se = x.scaleX(R), C = x.scaleX(H);
                    return /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: Math.min(se, C),
                        y: l,
                        width: Math.max(1, Math.abs(C - se)),
                        height: u,
                        rx: "3",
                        fill: b,
                        opacity: "0.92",
                        style: { transition: nt ? "width 0.35s ease, x 0.35s ease" : void 0 },
                        ...ie
                      },
                      `${j.name}-${o}`
                    );
                  }
                  const y = x.scaleY(R), Z = x.scaleY(H);
                  return /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: l,
                      y: Math.min(y, Z),
                      width: u,
                      height: Math.max(1, Math.abs(Z - y)),
                      rx: "3",
                      fill: b,
                      opacity: "0.92",
                      style: { transition: nt ? "height 0.35s ease, y 0.35s ease" : void 0 },
                      ...ie
                    },
                    `${j.name}-${o}`
                  );
                }
                if (x.horizontalBarMode) {
                  const m = x.scaleX(w), z = l + h * (u + 4);
                  return /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: Math.min(m, x.baselineX),
                        y: z,
                        width: Math.max(1, Math.abs(x.baselineX - m)),
                        height: u,
                        rx: "3",
                        fill: b,
                        opacity: "0.9",
                        style: { transition: nt ? "width 0.35s ease, x 0.35s ease" : void 0 },
                        "data-tip-cat": o,
                        "data-tip-ser": j.name,
                        "data-tip-val": P(w),
                        "data-tip-color": k
                      }
                    ),
                    A ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: w >= 0 ? Math.max(m, x.baselineX) + 4 : Math.min(m, x.baselineX) - 4,
                        y: z + u / 2 + 3,
                        textAnchor: w >= 0 ? "start" : "end",
                        fontSize: "9",
                        fontWeight: "700",
                        fill: r.textSec,
                        style: { pointerEvents: "none" },
                        children: P(w)
                      }
                    ) : null
                  ] }, `${j.name}-${o}`);
                }
                const I = x.scaleY(w), F = Math.max(1, Math.abs(x.baselineY - I)), M = l + h * (u + 4);
                return /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: M,
                      y: Math.min(I, x.baselineY),
                      width: u,
                      height: F,
                      rx: "3",
                      fill: b,
                      opacity: "0.9",
                      style: { transition: nt ? "height 0.35s ease, y 0.35s ease" : void 0, cursor: ue ? "pointer" : void 0 },
                      "data-tip-cat": o,
                      "data-tip-ser": j.name,
                      "data-tip-val": P(w),
                      "data-tip-color": k,
                      "data-chart-element-type": "series",
                      "data-chart-element-id": j.name
                    }
                  ),
                  A ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: M + u / 2,
                      y: w >= 0 ? Math.min(I, x.baselineY) - 4 : Math.max(I, x.baselineY) + 12,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: P(w)
                    }
                  ) : null
                ] }, `${j.name}-${o}`);
              }) }, `bar-group-${o}`);
            }),
            (n === "line" || n === "area") && (() => {
              const o = t.categories.map(() => 0), c = t.categories.map(() => 0);
              return t.series.map((l, u) => {
                if (J.has(l.name)) return null;
                const p = ve(l.name, u), f = t.categories.map((w, k) => {
                  const b = l.values[k];
                  if (b == null) return null;
                  const I = x.margin.left + k * x.step + x.step / 2;
                  if (ft || De) {
                    const F = De && me ? me[k] : null, M = De && F ? b / F * 100 : b, m = M >= 0 ? o[k] : c[k], z = m + M;
                    return M >= 0 ? o[k] = z : c[k] = z, {
                      x: I,
                      y: x.scaleY(z),
                      baseY: x.scaleY(m),
                      rawValue: b
                    };
                  }
                  return {
                    x: I,
                    y: x.scaleY(b),
                    baseY: x.baselineY,
                    rawValue: b
                  };
                }), j = ur(f), h = n === "area" ? Rr(f) : "";
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  n === "area" && h && /* @__PURE__ */ e.jsx("path", { d: h, fill: de ? `url(#pg-grad-${u})` : p, opacity: ft || De ? "0.46" : "0.16", style: { pointerEvents: "none" } }),
                  j && /* @__PURE__ */ e.jsx("path", { d: j, fill: "none", stroke: p, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: pr(pt[l.name]), style: { pointerEvents: "none" } }),
                  f.map((w, k) => w ? /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    xt.cloneElement(zt(w.x, w.y, p, {
                      "data-tip-cat": t.categories[k],
                      "data-tip-ser": l.name,
                      "data-tip-val": P(w.rawValue),
                      "data-tip-color": p
                    }), { key: `marker-${u}-${k}` }),
                    A ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: w.x,
                        y: w.y - 8,
                        textAnchor: "middle",
                        fontSize: "9",
                        fontWeight: "700",
                        fill: r.textSec,
                        style: { pointerEvents: "none" },
                        children: P(w.rawValue)
                      }
                    ) : null
                  ] }, `${l.name}-${k}`) : null)
                ] }, `${l.name}-${n}`);
              });
            })(),
            mr && t.categories.map((o, c) => {
              const l = Ot[c];
              return l ? x.horizontalBarMode ? /* @__PURE__ */ e.jsx(
                "rect",
                {
                  x: x.margin.left,
                  y: x.margin.top + c * x.step,
                  width: T - x.margin.left - x.margin.right,
                  height: x.step,
                  fill: "transparent",
                  style: { cursor: "pointer" },
                  onClick: () => B(l)
                },
                `category-hit-horizontal-${c}`
              ) : /* @__PURE__ */ e.jsx(
                "rect",
                {
                  x: x.margin.left + c * x.step,
                  y: x.margin.top,
                  width: x.step,
                  height: $ - x.margin.top - x.margin.bottom,
                  fill: "transparent",
                  style: { cursor: "pointer" },
                  onClick: () => B(l)
                },
                `category-hit-vertical-${c}`
              ) : null;
            }),
            Ie ? null : x.horizontalBarMode ? x.hasHierarchicalBands ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              qt.map((o) => {
                const c = x.margin.top + o.startIndex * x.step, l = x.margin.top + (o.endIndex + 1) * x.step, u = (c + l) / 2;
                return /* @__PURE__ */ e.jsx("g", { children: /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: 10,
                    y: u + 4,
                    textAnchor: "start",
                    fontSize: "11",
                    fontWeight: "700",
                    fill: r.text,
                    children: he(o.outerLabel, 18)
                  }
                ) }, `outer-label-horizontal-${o.groupKey}`);
              }),
              t.categoryBands.map((o, c) => {
                const l = x.margin.top + c * x.step + x.step / 2, u = o && o.innerLabel ? String(o.innerLabel) : "";
                return u ? /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: x.margin.left - 10,
                    y: l + 4,
                    textAnchor: "end",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(u, 18)
                  },
                  `inner-label-horizontal-${c}`
                ) : null;
              })
            ] }) : t.categories.map((o, c) => {
              const l = x.margin.top + c * x.step + x.step / 2;
              return /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: x.margin.left - 10,
                  y: l + 4,
                  textAnchor: "end",
                  fontSize: "11",
                  fill: r.textSec,
                  children: he(o, 22)
                },
                `label-horizontal-${c}`
              );
            }) : x.hasHierarchicalBands ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              qt.map((o) => {
                const c = x.margin.left + o.startIndex * x.step, l = x.margin.left + (o.endIndex + 1) * x.step, u = (c + l) / 2;
                return /* @__PURE__ */ e.jsx("g", { children: /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: u,
                    y: $ - x.margin.bottom + 68,
                    textAnchor: "middle",
                    fontSize: "11",
                    fontWeight: "700",
                    fill: r.text,
                    children: he(o.outerLabel, 22)
                  }
                ) }, `outer-label-${o.groupKey}`);
              }),
              t.categoryBands.map((o, c) => {
                const l = x.margin.left + c * x.step + x.step / 2, u = o && o.innerLabel ? String(o.innerLabel) : "";
                return /* @__PURE__ */ e.jsx("g", { children: u ? /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: l,
                    y: $ - x.margin.bottom + 30,
                    textAnchor: "middle",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(u, 18)
                  }
                ) : null }, `inner-label-${c}`);
              })
            ] }) : t.categories.map((o, c) => {
              if (Nt.visibleIndexes && !Nt.visibleIndexes.has(c)) return null;
              const l = x.margin.left + c * x.step + x.step / 2;
              return /* @__PURE__ */ e.jsx("g", { transform: `translate(${l}, ${$ - x.margin.bottom + 18}) rotate(${ke})`, children: /* @__PURE__ */ e.jsxs("text", { textAnchor: ke === 0 ? "middle" : "start", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: ue ? { cursor: "pointer" } : void 0, children: [
                /* @__PURE__ */ e.jsx("title", { children: String(o) }),
                he(o, Nt.maxChars)
              ] }) }, `label-${c}`);
            })
          ]
        }
      ),
      Re,
      Ne
    ] }),
    S ? /* @__PURE__ */ e.jsx(jt, { items: Mt, theme: r, hiddenSet: J, onToggle: Ke, position: Oe, onElementSelect: ue }) : null
  ] });
}, rs = ({ chartType: t, barLayout: n, onChange: i, canStack: a, theme: r }) => {
  const g = (S, A = !1) => ({
    border: `1px solid ${S === n ? r.primary : r.border}`,
    background: S === n ? r.select : r.headerSubtleBg || r.hover,
    color: S === n ? r.primary : r.text,
    borderRadius: r.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: A ? "not-allowed" : "pointer",
    opacity: A ? 0.45 : 1
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => i("grouped"), style: g("grouped"), children: t === "area" ? "Normal" : "Grouped" }),
    /* @__PURE__ */ e.jsx(
      "button",
      {
        onClick: () => {
          a && i("stacked");
        },
        style: g("stacked", !a),
        title: a ? t === "area" ? "Stack area series" : "Stack series or visible child rows" : t === "area" ? "Stacked area needs at least two visible series" : "Stacked bars need multiple visible series or visible child groups",
        children: "Stacked"
      }
    ),
    /* @__PURE__ */ e.jsx(
      "button",
      {
        onClick: () => {
          a && i("stacked100");
        },
        style: g("stacked100", !a),
        title: a ? "Normalize each category to 100%" : "Needs multiple series",
        children: "100%"
      }
    )
  ] });
}, ns = ({ orientation: t, onChange: n, theme: i }) => {
  const a = (r) => ({
    border: `1px solid ${r === t ? i.primary : i.border}`,
    background: r === t ? i.select : i.headerSubtleBg || i.hover,
    color: r === t ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("rows"), style: a("rows"), children: "By Rows" }),
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("columns"), style: a("columns"), children: "By Columns" })
  ] });
}, ss = ({ axisMode: t, onChange: n, theme: i }) => {
  const a = (r) => ({
    border: `1px solid ${r === t ? i.primary : i.border}`,
    background: r === t ? i.select : i.headerSubtleBg || i.hover,
    color: r === t ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("vertical"), style: a("vertical"), children: "Vertical" }),
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("horizontal"), style: a("horizontal"), children: "Horizontal" })
  ] });
}, os = ({ level: t, onChange: n, maxLevel: i, theme: a }) => {
  if (!i || i <= 1) return null;
  const r = (g) => ({
    border: `1px solid ${g === t ? a.primary : a.border}`,
    background: g === t ? a.select : a.headerSubtleBg || a.hover,
    color: g === t ? a.primary : a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer",
    minWidth: "34px"
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("all"), style: r("all"), children: "All" }),
    Array.from({ length: i }, (g, S) => S + 1).map((g) => /* @__PURE__ */ e.jsx("button", { onClick: () => n(g), style: r(g), children: g }, g))
  ] });
}, is = ({ title: t, subtitle: n, note: i, theme: a }) => !t && !n && !i ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px" }, children: [
  t ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: a.text }, children: t }) : null,
  n ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: n }) : null,
  i ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: i }) : null
] }), fe = ({ label: t, theme: n, children: i, controlMinWidth: a = "180px" }) => /* @__PURE__ */ e.jsxs("div", { style: {
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: "10px",
  flexWrap: "wrap"
}, children: [
  /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "3px", flex: "1 1 180px", minWidth: "132px" }, children: /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", fontWeight: 700, color: n.text }, children: t }) }),
  /* @__PURE__ */ e.jsx("div", { style: {
    display: "flex",
    justifyContent: "flex-end",
    alignItems: "flex-start",
    flex: "1 1 210px",
    minWidth: a
  }, children: i })
] }), as = ({
  rowLimit: t,
  onRowLimitChange: n,
  columnLimit: i,
  onColumnLimitChange: a,
  theme: r
}) => {
  const g = {
    width: "58px",
    border: `1px solid ${r.border}`,
    background: r.surfaceBg || r.background || "#fff",
    color: r.text,
    borderRadius: r.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700
  }, S = {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    color: r.textSec,
    fontSize: "11px",
    fontWeight: 700
  }, A = (B, N) => (E) => {
    B && B(kn(E.target.value, N));
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsxs("label", { style: S, children: [
      "Rows",
      /* @__PURE__ */ e.jsx(
        "input",
        {
          type: "number",
          min: "1",
          step: "1",
          value: t,
          onChange: A(n, Gr),
          style: g
        }
      )
    ] }),
    /* @__PURE__ */ e.jsxs("label", { style: S, children: [
      "Cols",
      /* @__PURE__ */ e.jsx(
        "input",
        {
          type: "number",
          min: "1",
          step: "1",
          value: i,
          onChange: A(a, Xr),
          style: g
        }
      )
    ] })
  ] });
}, ls = ({
  layers: t,
  onChange: n,
  availableColumns: i,
  theme: a
}) => {
  const r = Mr(i), g = bn(t, r), [S, A] = v.useState(""), B = v.useMemo(
    () => yn(r),
    [r]
  ), N = v.useMemo(
    () => new Map(B.map((G) => [G.id, G])),
    [B]
  ), E = S.trim().toLowerCase(), Y = v.useMemo(
    () => E ? B.filter((G) => G.searchText.includes(E)) : B,
    [B, E]
  ), te = v.useMemo(() => {
    const G = /* @__PURE__ */ new Map();
    return Y.forEach((K) => {
      G.has(K.groupLabel) || G.set(K.groupLabel, []), G.get(K.groupLabel).push(K);
    }), Array.from(G.entries()).map(([K, U]) => ({ label: K, options: U }));
  }, [Y]), Q = {
    border: `1px solid ${a.border}`,
    background: a.surfaceBg || a.background || "#fff",
    color: a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700
  }, s = (G, K) => {
    typeof n == "function" && n(g.map((U) => U.id === G ? { ...U, ...K || {} } : U));
  }, _ = (G, K) => {
    if (typeof n != "function") return;
    const U = g.findIndex((xe) => xe.id === G), ge = U + K;
    if (U < 0 || ge < 0 || ge >= g.length) return;
    const ae = [...g], [de] = ae.splice(U, 1);
    ae.splice(ge, 0, de), n(ae);
  }, X = (G) => {
    if (typeof n != "function") return;
    const K = g.filter((U) => U.id !== G);
    n(K.length > 0 ? K : vn(r, 1));
  }, O = () => {
    if (typeof n != "function") return;
    const G = new Set(g.map((ge) => ge.columnId)), K = r.find((ge) => !G.has(ge.id)) || r[0];
    if (!K) return;
    const U = mn({
      id: `layer-${g.length + 1}`,
      type: Hr[g.length % Hr.length],
      columnId: K.id,
      axis: g.length === 1 ? "right" : "left",
      name: jn(K)
    }, r, g.length);
    n([...g, U]);
  };
  return r.length === 0 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: "No visible numeric/value columns are available for combo layers." }) : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "8px", width: "100%" }, children: [
    /* @__PURE__ */ e.jsxs("div", { style: {
      display: "flex",
      flexWrap: "wrap",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "8px",
      padding: "10px",
      border: `1px solid ${a.border}`,
      borderRadius: a.radiusSm || "8px",
      background: a.headerSubtleBg || a.hover
    }, children: [
      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "3px", flex: "1 1 240px", minWidth: "220px" }, children: [
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", fontWeight: 800, color: a.text }, children: "Measure Search" }),
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: a.textSec }, children: "Search across all available chart measures and grouped pivot columns." })
      ] }),
      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", flex: "1 1 320px", minWidth: "240px", justifyContent: "flex-end" }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "search",
            value: S,
            onChange: (G) => A(G.target.value),
            placeholder: "Search measures, groups, or column ids",
            style: { ...Q, width: "100%", maxWidth: "360px" }
          }
        ),
        /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", fontWeight: 700, color: a.textSec, whiteSpace: "nowrap" }, children: [
          Y.length,
          " / ",
          B.length
        ] })
      ] })
    ] }),
    Y.length === 0 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec, padding: "2px 2px 0 2px" }, children: "No measures match the current search. Clear the search to see the full catalog." }) : null,
    g.map((G, K) => {
      const U = N.get(G.columnId) || null, ge = te.some((ae) => ae.options.some((de) => de.id === G.columnId));
      return /* @__PURE__ */ e.jsxs(
        "div",
        {
          style: {
            display: "flex",
            flexDirection: "column",
            gap: "10px",
            width: "100%",
            padding: "12px",
            border: `1px solid ${a.border}`,
            borderRadius: a.radiusSm || "8px",
            background: a.surfaceBg || a.background || "#fff",
            boxShadow: a.shadowInset || "none"
          },
          children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px", minWidth: "180px" }, children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", fontWeight: 800, color: a.text }, children: `Layer ${K + 1}` }),
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: U ? U.fullLabel : G.name || "Select a measure" }),
                U ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: a.textSec }, children: `${U.groupLabel} | ${U.id}` }) : null
              ] }),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => _(G.id, -1),
                    disabled: K === 0,
                    style: {
                      border: `1px solid ${a.border}`,
                      background: a.headerSubtleBg || a.hover,
                      color: a.textSec,
                      borderRadius: a.radiusSm || "8px",
                      padding: "6px 8px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: K === 0 ? "not-allowed" : "pointer",
                      opacity: K === 0 ? 0.45 : 1
                    },
                    children: "Up"
                  }
                ),
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => _(G.id, 1),
                    disabled: K === g.length - 1,
                    style: {
                      border: `1px solid ${a.border}`,
                      background: a.headerSubtleBg || a.hover,
                      color: a.textSec,
                      borderRadius: a.radiusSm || "8px",
                      padding: "6px 8px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: K === g.length - 1 ? "not-allowed" : "pointer",
                      opacity: K === g.length - 1 ? 0.45 : 1
                    },
                    children: "Down"
                  }
                ),
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => X(G.id),
                    style: {
                      border: `1px solid ${a.border}`,
                      background: a.headerSubtleBg || a.hover,
                      color: a.textSec,
                      borderRadius: a.radiusSm || "8px",
                      padding: "6px 8px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: "pointer"
                    },
                    children: "Remove"
                  }
                )
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: {
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
              gap: "8px",
              alignItems: "start",
              width: "100%"
            }, children: [
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", minWidth: 0 }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Measure" }),
                /* @__PURE__ */ e.jsxs(
                  "select",
                  {
                    value: G.columnId || "",
                    onChange: (ae) => {
                      const de = N.get(ae.target.value) || null;
                      s(G.id, {
                        columnId: ae.target.value,
                        name: de ? de.fullLabel : G.name
                      });
                    },
                    style: Q,
                    children: [
                      !ge && U ? /* @__PURE__ */ e.jsx("optgroup", { label: "Current Layer Measure", children: /* @__PURE__ */ e.jsx("option", { value: U.id, children: U.fullLabel }) }) : null,
                      te.map((ae) => /* @__PURE__ */ e.jsx("optgroup", { label: `${ae.label} (${ae.options.length})`, children: ae.options.map((de) => /* @__PURE__ */ e.jsx("option", { value: de.id, children: de.leafLabel }, de.id)) }, ae.label))
                    ]
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Layer Type" }),
                /* @__PURE__ */ e.jsxs(
                  "select",
                  {
                    value: G.type,
                    onChange: (ae) => s(G.id, { type: ae.target.value }),
                    style: Q,
                    children: [
                      /* @__PURE__ */ e.jsx("option", { value: "bar", children: "Bar" }),
                      /* @__PURE__ */ e.jsx("option", { value: "line", children: "Line" }),
                      /* @__PURE__ */ e.jsx("option", { value: "area", children: "Area" })
                    ]
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Axis" }),
                /* @__PURE__ */ e.jsxs(
                  "select",
                  {
                    value: G.axis,
                    onChange: (ae) => s(G.id, { axis: ae.target.value }),
                    style: Q,
                    children: [
                      /* @__PURE__ */ e.jsx("option", { value: "left", children: "Left Axis" }),
                      /* @__PURE__ */ e.jsx("option", { value: "right", children: "Right Axis" })
                    ]
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Format" }),
                /* @__PURE__ */ e.jsxs(
                  "select",
                  {
                    value: G.valueFormat || "auto",
                    onChange: (ae) => s(G.id, { valueFormat: ae.target.value }),
                    style: Q,
                    children: [
                      /* @__PURE__ */ e.jsx("option", { value: "auto", children: "Auto" }),
                      /* @__PURE__ */ e.jsx("option", { value: "number", children: "Number" }),
                      /* @__PURE__ */ e.jsx("option", { value: "compact", children: "Compact (K/M)" }),
                      /* @__PURE__ */ e.jsx("option", { value: "percent", children: "Percent" }),
                      /* @__PURE__ */ e.jsx("option", { value: "currency", children: "Currency ($)" })
                    ]
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Color" }),
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "color",
                    value: G.color || "#6366f1",
                    onChange: (ae) => s(G.id, { color: ae.target.value }),
                    style: { width: "100%", height: "34px", padding: "2px 4px", border: `1px solid ${a.border}`, borderRadius: a.radiusSm || "8px", cursor: "pointer", background: "none" },
                    title: "Override color for this layer"
                  }
                )
              ] })
            ] })
          ]
        },
        G.id
      );
    }),
    /* @__PURE__ */ e.jsx("div", { children: /* @__PURE__ */ e.jsx(
      "button",
      {
        type: "button",
        onClick: O,
        style: {
          border: `1px solid ${a.primary}`,
          background: a.select,
          color: a.primary,
          borderRadius: a.radiusSm || "8px",
          padding: "6px 10px",
          fontSize: "11px",
          fontWeight: 700,
          cursor: "pointer"
        },
        children: "Add Layer"
      }
    ) })
  ] });
}, cs = ({ sortMode: t, onChange: n, theme: i }) => {
  const a = [
    { value: "natural", label: "Natural" },
    { value: "value_desc", label: "Value Desc" },
    { value: "value_asc", label: "Value Asc" },
    { value: "label_asc", label: "Label A-Z" },
    { value: "label_desc", label: "Label Z-A" }
  ];
  return /* @__PURE__ */ e.jsx(
    "select",
    {
      value: t,
      onChange: (r) => n(r.target.value),
      style: {
        border: `1px solid ${i.border}`,
        background: i.surfaceBg || i.background || "#fff",
        color: i.text,
        borderRadius: i.radiusSm || "8px",
        padding: "6px 8px",
        fontSize: "11px",
        fontWeight: 700,
        minWidth: "132px"
      },
      title: "Chart ordering",
      children: a.map((r) => /* @__PURE__ */ e.jsx("option", { value: r.value, children: r.label }, r.value))
    }
  );
}, ds = ({ interactionMode: t, onChange: n, theme: i }) => {
  const a = (r) => ({
    border: `1px solid ${r === t ? i.primary : i.border}`,
    background: r === t ? i.select : i.headerSubtleBg || i.hover,
    color: r === t ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("focus"), style: a("focus"), title: "Jump the grid to the matching row or column", children: "Jump to Grid" }),
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("filter"), style: a("filter"), title: "Apply the clicked chart item as a pivot filter", children: "Filter Grid" }),
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("event"), style: a("event"), title: "Send the chart click to Dash only and leave the grid unchanged", children: "No Grid Change" })
  ] });
}, us = ({ scope: t, onChange: n, theme: i }) => {
  const a = (r) => ({
    border: `1px solid ${r === t ? i.primary : i.border}`,
    background: r === t ? i.select : i.headerSubtleBg || i.hover,
    color: r === t ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("viewport"), style: a("viewport"), children: "Viewport" }),
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("root"), style: a("root"), children: "From Start" })
  ] });
}, er = "http://www.w3.org/2000/svg", $r = (t, n) => {
  if (!t || typeof document > "u") return null;
  const i = gr(n), a = t.cloneNode(!0), r = (a.getAttribute("viewBox") || "").split(/[\s,]+/).map(Number), g = r.length === 4 && r[2] > 0 ? r[2] : t.clientWidth || 800, S = r.length === 4 && r[3] > 0 ? r[3] : t.clientHeight || 400, A = i.title ? 42 : 0, B = Ur(
    i.legendItems,
    g,
    (X) => String(X || "").length * 6.4
  ), N = B.length > 0 ? B.length * 22 + 14 : 0, E = i.legendPosition === "top" ? N : 0, Y = A + E, te = A + S + N, Q = Array.from(a.childNodes), s = document.createElementNS(er, "g");
  s.setAttribute("transform", `translate(0 ${Y})`), Q.forEach((X) => s.appendChild(X)), a.appendChild(s), a.setAttribute("viewBox", `0 0 ${g} ${te}`), a.setAttribute("width", String(g)), a.setAttribute("height", String(te)), a.setAttribute("xmlns", er);
  const _ = document.createElementNS(er, "rect");
  if (_.setAttribute("x", "0"), _.setAttribute("y", "0"), _.setAttribute("width", String(g)), _.setAttribute("height", String(te)), _.setAttribute("fill", i.background), a.insertBefore(_, s), i.title) {
    const X = document.createElementNS(er, "text");
    X.setAttribute("x", "16"), X.setAttribute("y", "27"), X.setAttribute("fill", i.textColor), X.setAttribute("font-size", "16"), X.setAttribute("font-weight", "700"), X.setAttribute("font-family", "Inter, Arial, sans-serif"), X.textContent = i.title, a.appendChild(X);
  }
  if (B.length > 0) {
    const X = i.legendPosition === "top" ? A + 16 : A + S + 18;
    B.forEach((O, G) => {
      let K = 16;
      O.forEach((U) => {
        const ge = X + G * 22, ae = document.createElementNS(er, "circle");
        ae.setAttribute("cx", String(K + 5)), ae.setAttribute("cy", String(ge - 4)), ae.setAttribute("r", "5"), ae.setAttribute("fill", U.hidden ? i.secondaryTextColor : U.color || "#2563eb"), ae.setAttribute("opacity", U.hidden ? "0.35" : "1"), a.appendChild(ae);
        const de = document.createElementNS(er, "text");
        de.setAttribute("x", String(K + 16)), de.setAttribute("y", String(ge)), de.setAttribute("fill", i.secondaryTextColor), de.setAttribute("font-size", "11"), de.setAttribute("font-family", "Inter, Arial, sans-serif"), de.setAttribute("opacity", U.hidden ? "0.45" : "1"), de.textContent = U.label, a.appendChild(de), K += U.width;
      });
    });
  }
  return a;
}, xs = async (t, n, i) => {
  if (!t || typeof window > "u") return;
  const a = $r(t, i);
  if (!a) return;
  const g = new XMLSerializer().serializeToString(a), S = new Blob([g], { type: "image/svg+xml;charset=utf-8" });
  hr(S, `${n}.svg`);
}, Kr = (t, n = 2) => new Promise((i) => {
  if (!t) {
    i(null);
    return;
  }
  const r = new XMLSerializer().serializeToString(t), g = t.getAttribute("viewBox");
  let S = t.clientWidth || 800, A = t.clientHeight || 400;
  if (g) {
    const Q = g.split(/[\s,]+/).map(Number);
    Q.length === 4 && Q[2] > 0 && Q[3] > 0 && (S = Q[2], A = Q[3]);
  }
  const B = document.createElement("canvas");
  B.width = S * n, B.height = A * n;
  const N = B.getContext("2d");
  N.fillStyle = "#ffffff", N.fillRect(0, 0, B.width, B.height);
  const E = new Image(), Y = new Blob([r], { type: "image/svg+xml;charset=utf-8" }), te = URL.createObjectURL(Y);
  E.onload = () => {
    N.drawImage(E, 0, 0, B.width, B.height), URL.revokeObjectURL(te), i(B);
  }, E.onerror = () => {
    URL.revokeObjectURL(te), i(null);
  }, E.src = te;
}), ps = async (t, n, i) => {
  const a = await Kr($r(t, i));
  a && a.toBlob((r) => {
    r && hr(r, `${n}.png`);
  }, "image/png");
}, fs = async (t, n) => {
  if (typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  const i = await Kr($r(t, n));
  if (!i) return !1;
  try {
    const a = await new Promise((r) => i.toBlob(r, "image/png"));
    return a ? (await navigator.clipboard.write([new ClipboardItem({ "image/png": a })]), !0) : !1;
  } catch {
    return !1;
  }
}, ut = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), hs = (t) => new Promise((n) => {
  const i = new Image();
  i.onload = () => n(i), i.onerror = () => n(null), i.src = t;
}), Zr = async (t, n) => {
  const i = t && t.current && t.current.getEchartsInstance();
  if (!i) return null;
  const a = gr(n), r = i.getDataURL({ type: "png", pixelRatio: 2 }), g = await hs(r);
  if (!g) return null;
  const S = Math.max(1, g.naturalWidth / Math.max(1, i.getWidth())), A = a.title ? Math.round(42 * S) : 0, N = document.createElement("canvas").getContext("2d");
  N.font = "11px Inter, Arial, sans-serif";
  const E = Ur(
    a.legendItems,
    i.getWidth(),
    (X) => N.measureText(String(X || "")).width
  ), Y = E.length > 0 ? Math.round((E.length * 22 + 14) * S) : 0, te = a.legendPosition === "top" ? Y : 0, Q = A + te, s = document.createElement("canvas");
  s.width = g.naturalWidth, s.height = A + g.naturalHeight + Y;
  const _ = s.getContext("2d");
  if (_.fillStyle = a.background, _.fillRect(0, 0, s.width, s.height), _.drawImage(g, 0, Q), a.title && (_.fillStyle = a.textColor, _.font = `700 ${Math.round(16 * S)}px Inter, Arial, sans-serif`, _.textBaseline = "alphabetic", _.fillText(a.title, Math.round(16 * S), Math.round(27 * S))), E.length > 0) {
    const X = a.legendPosition === "top" ? A + Math.round(16 * S) : A + g.naturalHeight + Math.round(18 * S);
    _.font = `${Math.round(11 * S)}px Inter, Arial, sans-serif`, E.forEach((O, G) => {
      let K = Math.round(16 * S);
      O.forEach((U) => {
        const ge = X + Math.round(G * 22 * S);
        _.globalAlpha = U.hidden ? 0.35 : 1, _.fillStyle = U.hidden ? a.secondaryTextColor : U.color || "#2563eb", _.beginPath(), _.arc(K + Math.round(5 * S), ge - Math.round(4 * S), Math.round(5 * S), 0, Math.PI * 2), _.fill(), _.globalAlpha = U.hidden ? 0.45 : 1, _.fillStyle = a.secondaryTextColor, _.fillText(U.label, K + Math.round(16 * S), ge), _.globalAlpha = 1, K += Math.round(U.width * S);
      });
    });
  }
  return s;
}, gs = async (t, n, i) => {
  const a = await Zr(t, i);
  a && a.toBlob((r) => {
    r && hr(r, `${n}.png`);
  }, "image/png");
}, bs = async (t, n) => {
  if (!(t && t.current && t.current.getEchartsInstance()) || typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  try {
    const a = await Zr(t, n);
    if (!a) return !1;
    const r = await new Promise((g) => a.toBlob(g, "image/png"));
    return r ? (await navigator.clipboard.write([new ClipboardItem({ "image/png": r })]), !0) : !1;
  } catch {
    return !1;
  }
}, qr = ({
  model: t,
  chartType: n,
  chartLayers: i,
  onChartLayersChange: a,
  availableColumns: r,
  barLayout: g,
  axisMode: S,
  onChartTypeChange: A,
  onBarLayoutChange: B,
  onAxisModeChange: N,
  orientation: E,
  onOrientationChange: Y,
  hierarchyLevel: te,
  onHierarchyLevelChange: Q,
  theme: s,
  title: _,
  onTitleChange: X,
  subtitle: O,
  note: G,
  chartHeight: K,
  onChartHeightChange: U,
  rowLimit: ge,
  onRowLimitChange: ae,
  columnLimit: de,
  onColumnLimitChange: xe,
  sortMode: nt,
  onSortModeChange: ye,
  interactionMode: st,
  onInteractionModeChange: _e,
  serverScope: Ht,
  onServerScopeChange: ot,
  showServerScope: qe = !1,
  floating: Oe = !1,
  immersiveMode: Me = !1,
  onCategoryActivate: _t,
  allowHierarchyCharts: nr = !0,
  locked: et = !1,
  onToggleLock: Xt,
  configOpen: Gt,
  onConfigChange: Qe,
  settingsPanelWidth: pt,
  onSettingsPanelWidthChange: Tt,
  initialSettings: W = {},
  onSettingsChange: $t
}) => {
  const ue = n === "combo", je = n === "sparkline", P = ue ? !1 : n === "area" ? Tr(t) : Vr(t), Ie = _ !== void 0 ? _ : t && t.title, Wt = O !== void 0 ? O : t && t.subtitle, ft = G !== void 0 ? G : et ? Oe ? "Locked to the current chart request and floating frame" : "Locked to the current chart request" : t && t.note, [it, De] = v.useState(!1), ke = typeof Gt == "boolean" ? Gt : it, $ = typeof Qe == "function" ? Qe : De, [ve, zt] = v.useState(() => (W == null ? void 0 : W.showDataLabels) ?? !1), [me, ht] = v.useState(() => (W == null ? void 0 : W.colorPalette) ?? "default"), [at, He] = v.useState(() => (W == null ? void 0 : W.valueFormat) ?? "auto"), [kt, sr] = v.useState(() => (W == null ? void 0 : W.yAxisTitle) ?? ""), [T, J] = v.useState(() => (W == null ? void 0 : W.yAxisMin) ?? ""), [Bt, Ke] = v.useState(() => (W == null ? void 0 : W.yAxisMax) ?? ""), [$e, Xe] = v.useState(() => (W == null ? void 0 : W.referenceLines) ?? []), [Ge, gt] = v.useState({ value: "", label: "", orient: "h" }), [lt, Ne] = v.useState(() => (W == null ? void 0 : W.labelAngle) ?? 0), [Le, Ft] = v.useState(() => (W == null ? void 0 : W.chartSubtitle) ?? ""), [tt, Ee] = v.useState(() => (W == null ? void 0 : W.tickCount) ?? 5), [Ze, bt] = v.useState(() => (W == null ? void 0 : W.markerShape) ?? "circle"), [yt, rt] = v.useState(() => (W == null ? void 0 : W.markerSize) ?? 4), [Te, ct] = v.useState(() => (W == null ? void 0 : W.showGradient) ?? !1), [Fe, Lt] = v.useState(() => (W == null ? void 0 : W.seriesColors) ?? {}), [dt, St] = v.useState(() => (W == null ? void 0 : W.showAnimations) ?? !0), [We, ee] = v.useState(() => (W == null ? void 0 : W.showCalloutLabels) ?? !1), [Be, Ue] = v.useState(() => (W == null ? void 0 : W.showTrendline) ?? !1), [Se, Ye] = v.useState(() => (W == null ? void 0 : W.trendlineMode) ?? "linear"), [Je, or] = v.useState(() => (W == null ? void 0 : W.trendlinePeriod) ?? 3), [vt, ar] = v.useState(() => (W == null ? void 0 : W.legendPosition) ?? "bottom"), [Et, Ut] = v.useState(() => (W == null ? void 0 : W.labelPosition) ?? "outside"), [Kt, Zt] = v.useState(() => (W == null ? void 0 : W.decimalPlaces) ?? -1), [Rt, br] = v.useState(() => (W == null ? void 0 : W.gradientDirection) ?? "vertical"), [Dt, yr] = v.useState(() => (W == null ? void 0 : W.gradientOpacity) ?? 70), [Re, x] = v.useState(() => (W == null ? void 0 : W.trendlineColor) ?? ""), [Nt, vr] = v.useState(() => (W == null ? void 0 : W.trendlineWidth) ?? 2), [wt, lr] = v.useState(() => (W == null ? void 0 : W.seriesSort) ?? "none"), [Mt, qt] = v.useState(() => (W == null ? void 0 : W.customPaletteColors) ?? []), [Ot, mr] = v.useState(() => (W == null ? void 0 : W.lineDashStyles) ?? {}), [o, c] = v.useState(() => (W == null ? void 0 : W.yAxisMinRight) ?? ""), [l, u] = v.useState(() => (W == null ? void 0 : W.yAxisMaxRight) ?? ""), [p, f] = v.useState("chart"), [j, h] = v.useState(!0), [w, k] = v.useState(!0), b = v.useCallback((d) => {
    const pe = { series: "style", axis: "axes", legend: "labels", title: "data" }[d];
    pe && ($(!0), f(pe));
  }, [$]), I = Yr(me), [F, M] = v.useState(String(K || rr)), m = v.useRef(null), z = v.useRef(null), R = v.useRef(null), [H, V] = v.useState(null), [ie, y] = v.useState(!1), Z = v.useRef(null), [se, C] = v.useState(null), [L, le] = v.useState(348), oe = Number.isFinite(Number(pt)) ? Math.max(300, Math.min(680, Math.floor(Number(pt)))) : L, re = typeof Tt == "function" ? Tt : le, D = !Me, ce = Me && se ? Math.max(180, se - (Me && !je && vt !== "none" ? 44 : 0) - 44) : Math.max(180, Number.isFinite(Number(K)) ? Number(K) : rr), Pe = v.useRef(null), mt = v.useRef(null), Yt = v.useRef(null), cr = t && t.maxHierarchyLevel || 1, Pt = typeof Q == "function" && cr > 1, Wr = {
    width: "32px",
    height: "32px",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    border: `1px solid ${ke ? s.primary : s.border}`,
    background: ke ? s.select : s.headerSubtleBg || s.hover,
    color: ke ? s.primary : s.textSec,
    borderRadius: s.radiusSm || "8px",
    cursor: "pointer",
    flexShrink: 0
  }, Qr = {
    ...Wr,
    border: `1px solid ${et ? s.primary : s.border}`,
    background: et ? s.select : s.headerSubtleBg || s.hover,
    color: et ? s.primary : s.textSec
  }, Vt = {
    border: `1px solid ${s.border}`,
    background: s.headerSubtleBg || s.hover,
    color: s.textSec,
    borderRadius: s.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }, Ce = {
    border: `1px solid ${s.border}`,
    background: s.surfaceBg || s.background || "#fff",
    color: s.text,
    borderRadius: s.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 600,
    minWidth: "68px"
  }, Jr = v.useCallback((d) => {
    if (d === "trend") {
      A("line"), typeof Y == "function" && Y("rows"), typeof ye == "function" && ye("natural");
      return;
    }
    if (d === "compare") {
      A("bar"), typeof B == "function" && B("grouped"), typeof N == "function" && N("vertical");
      return;
    }
    if (d === "composition") {
      A(Pt ? "sunburst" : "donut");
      return;
    }
    if (d === "flow") {
      A("waterfall");
      return;
    }
    d === "sparkline" && A("sparkline");
  }, [
    N,
    B,
    A,
    Y,
    ye,
    Pt
  ]);
  v.useEffect(() => {
    !Me || !ke || $(!1);
  }, [Me, ke]), v.useEffect(() => {
    if (!H) return;
    const d = setTimeout(() => V(null), 2500);
    return () => clearTimeout(d);
  }, [H]), v.useEffect(() => {
    if (!ie) return;
    const d = (pe) => {
      Z.current && !Z.current.contains(pe.target) && y(!1);
    }, q = (pe) => {
      pe.key === "Escape" && y(!1);
    };
    return document.addEventListener("mousedown", d), document.addEventListener("keydown", q), () => {
      document.removeEventListener("mousedown", d), document.removeEventListener("keydown", q);
    };
  }, [ie]);
  const dr = v.useCallback(() => Gn(
    Yt.current,
    Ie,
    vt,
    s
  ), [vt, Ie, s]), en = v.useCallback(async (d) => {
    y(!1);
    const q = dr();
    if (d === "svg") {
      ut.has(n) || await xs(Pe.current, "pivot-chart", q);
      return;
    }
    if (d === "png") {
      ut.has(n) ? await gs(mt, "pivot-chart", q) : await ps(Pe.current, "pivot-chart", q);
      return;
    }
    d === "csv" && Tn(t, "pivot-chart", q);
  }, [n, dr, t]), tn = v.useCallback(async () => {
    const d = dr(), q = ut.has(n) ? await bs(mt, d) : await fs(Pe.current, d);
    V(q ? { tone: "success", text: "Copied chart" } : { tone: "error", text: "Copy failed" });
  }, [n, dr]);
  v.useEffect(() => {
    if (!Me) {
      C(null);
      return;
    }
    const d = R.current;
    if (!d) return;
    const q = () => {
      const be = d.clientHeight;
      be > 0 && C(be);
    };
    if (q(), typeof ResizeObserver > "u") return;
    const pe = new ResizeObserver(q);
    return pe.observe(d), () => pe.disconnect();
  }, [Me]);
  const jr = v.useRef(U);
  v.useEffect(() => {
    jr.current = U;
  }), v.useEffect(() => {
    const d = (pe) => {
      if (!m.current || typeof jr.current != "function") return;
      const be = m.current, Ve = Math.max(180, be.startHeight + (pe.clientY - be.startY));
      jr.current(Ve);
    }, q = () => {
      m.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", q), window.addEventListener("blur", q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", q), window.removeEventListener("blur", q);
    };
  }, []), v.useEffect(() => {
    const d = (pe) => {
      if (!z.current) return;
      const be = z.current, Ve = be.direction === "left" ? be.startX - pe.clientX : pe.clientX - be.startX, Qt = Math.max(300, Math.min(680, be.startWidth + Ve));
      re(Qt);
    }, q = () => {
      z.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", q), window.addEventListener("blur", q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", q), window.removeEventListener("blur", q);
    };
  }, []), v.useEffect(() => {
    const d = Yt.current;
    if (!d || typeof ResizeObserver > "u") return;
    let q = d.clientWidth;
    const pe = new ResizeObserver(() => {
      var Ve, Qt;
      const be = d.clientWidth;
      if (be !== q) {
        q = be;
        const Br = (Qt = (Ve = mt.current) == null ? void 0 : Ve.getEchartsInstance) == null ? void 0 : Qt.call(Ve);
        Br && Br.resize();
      }
    });
    return pe.observe(d), () => pe.disconnect();
  }, []), v.useEffect(() => {
    M(String(ce));
  }, [ce]);
  const kr = v.useRef($t);
  v.useEffect(() => {
    kr.current = $t;
  });
  const zr = v.useRef(!0);
  v.useEffect(() => {
    if (zr.current) {
      zr.current = !1;
      return;
    }
    typeof kr.current == "function" && kr.current({
      showDataLabels: ve,
      colorPalette: me,
      valueFormat: at,
      yAxisTitle: kt,
      yAxisMin: T,
      yAxisMax: Bt,
      referenceLines: $e,
      labelAngle: lt,
      chartSubtitle: Le,
      tickCount: tt,
      markerShape: Ze,
      markerSize: yt,
      showGradient: Te,
      seriesColors: Fe,
      showAnimations: dt,
      showCalloutLabels: We,
      showTrendline: Be,
      trendlineMode: Se,
      trendlinePeriod: Je,
      legendPosition: vt,
      labelPosition: Et,
      decimalPlaces: Kt,
      gradientDirection: Rt,
      gradientOpacity: Dt,
      trendlineColor: Re,
      trendlineWidth: Nt,
      seriesSort: wt,
      customPaletteColors: Mt,
      lineDashStyles: Ot,
      yAxisMinRight: o,
      yAxisMaxRight: l
    });
  }, [
    ve,
    me,
    at,
    kt,
    T,
    Bt,
    $e,
    lt,
    Le,
    tt,
    Ze,
    yt,
    Te,
    Fe,
    dt,
    We,
    Be,
    Se,
    Je,
    vt,
    Et,
    Kt,
    Rt,
    Dt,
    Re,
    Nt,
    wt,
    Mt,
    Ot,
    o,
    l
  ]);
  const Sr = v.useMemo(() => {
    if (!t || !Array.isArray(t.series)) return null;
    let d = 1 / 0, q = -1 / 0;
    return t.series.forEach((pe) => {
      (pe.values || []).forEach((be) => {
        typeof be == "number" && Number.isFinite(be) && (d = Math.min(d, be), q = Math.max(q, be));
      });
    }), Number.isFinite(d) && d !== q ? { min: d, max: q } : null;
  }, [t]);
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      ref: R,
      style: {
        display: "flex",
        flexDirection: "column",
        gap: D ? "14px" : "10px",
        flex: "1 1 auto",
        minHeight: 0,
        overflow: "hidden"
      },
      children: [
        D || Me ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
          /* @__PURE__ */ e.jsx(
            is,
            {
              title: Me ? Ie : null,
              subtitle: Wt,
              note: ft,
              theme: s
            }
          ),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap", justifyContent: "flex-end" }, children: [
            /* @__PURE__ */ e.jsxs("div", { ref: Z, style: { position: "relative" }, children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => y((d) => !d),
                  style: Vt,
                  title: "Export chart",
                  "aria-haspopup": "menu",
                  "aria-expanded": ie,
                  children: /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "5px" }, children: [
                    "Export ",
                    /* @__PURE__ */ e.jsx(tr.ChevronDown, {})
                  ] })
                }
              ),
              ie ? /* @__PURE__ */ e.jsx(
                "div",
                {
                  role: "menu",
                  "aria-label": "Chart export formats",
                  style: {
                    position: "absolute",
                    top: "calc(100% + 6px)",
                    right: 0,
                    zIndex: 80,
                    minWidth: "176px",
                    padding: "6px",
                    border: `1px solid ${s.border}`,
                    borderRadius: s.radiusSm || "8px",
                    background: s.surfaceBg || s.background || "#fff",
                    boxShadow: s.shadowMd || "0 12px 28px rgba(15,23,42,0.18)"
                  },
                  children: [
                    { format: "svg", label: "SVG image", hint: "Title + legend included", disabled: ut.has(n) },
                    { format: "png", label: "PNG image", hint: "Title + legend included", disabled: !1 },
                    { format: "csv", label: "CSV data", hint: "Title + legend metadata", disabled: !1 }
                  ].map((d) => /* @__PURE__ */ e.jsxs(
                    "button",
                    {
                      type: "button",
                      role: "menuitem",
                      disabled: d.disabled,
                      onClick: () => en(d.format),
                      title: d.disabled ? "SVG export is unavailable for this chart type" : void 0,
                      style: {
                        width: "100%",
                        display: "flex",
                        flexDirection: "column",
                        alignItems: "flex-start",
                        gap: "2px",
                        border: "none",
                        borderRadius: s.radiusSm || "7px",
                        background: "transparent",
                        color: d.disabled ? s.textSec : s.text,
                        opacity: d.disabled ? 0.45 : 1,
                        padding: "8px 9px",
                        cursor: d.disabled ? "not-allowed" : "pointer",
                        textAlign: "left"
                      },
                      onMouseEnter: (q) => {
                        d.disabled || (q.currentTarget.style.background = s.hover);
                      },
                      onMouseLeave: (q) => {
                        q.currentTarget.style.background = "transparent";
                      },
                      children: [
                        /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", fontWeight: 800 }, children: d.label }),
                        /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: s.textSec }, children: d.hint })
                      ]
                    },
                    d.format
                  ))
                }
              ) : null
            ] }),
            /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: tn,
                style: Vt,
                title: "Copy chart with title and legend",
                children: "Copy"
              }
            ),
            H ? /* @__PURE__ */ e.jsx(
              "span",
              {
                role: "status",
                style: {
                  fontSize: "10px",
                  fontWeight: 800,
                  color: H.tone === "success" ? s.primary || "#047857" : "#B91C1C",
                  border: `1px solid ${H.tone === "success" ? `${s.primary || "#047857"}55` : "#FCA5A5"}`,
                  borderRadius: "999px",
                  padding: "3px 7px",
                  background: H.tone === "success" ? `${s.primary || "#047857"}10` : "#FEF2F2"
                },
                children: H.text
              }
            ) : null,
            typeof Xt == "function" ? /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                onClick: Xt,
                style: { ...Qr, width: "auto", padding: "0 10px", gap: "6px", fontSize: "11px", fontWeight: 800 },
                title: et ? "Data is fixed to the saved selection. Click to resume live pivot updates." : "Keep this chart fixed to the current data selection while the pivot changes.",
                "aria-label": et ? "Unlock chart data and resume live updates" : "Lock chart to the current data selection",
                "aria-pressed": et,
                children: [
                  /* @__PURE__ */ e.jsx(tr.Lock, {}),
                  /* @__PURE__ */ e.jsx("span", { children: et ? "Data locked" : "Lock data" })
                ]
              }
            ) : null,
            D ? /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onMouseDown: (d) => d.stopPropagation(),
                onClick: (d) => {
                  d.stopPropagation(), $((q) => !q);
                },
                "data-chart-settings-toggle": "true",
                style: Wr,
                title: ke ? "Hide chart settings" : "Show chart settings",
                "aria-label": ke ? "Hide chart settings" : "Show chart settings",
                children: /* @__PURE__ */ e.jsx(tr.Settings, {})
              }
            ) : null
          ] })
        ] }) : null,
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", minHeight: 0, gap: 0, overflow: "hidden", flex: "1 1 auto" }, children: [
          /* @__PURE__ */ e.jsxs(
            "div",
            {
              ref: Yt,
              "data-chart-surface-main": "true",
              "data-chart-surface-scroll": "true",
              style: {
                display: "flex",
                flexDirection: "column",
                minHeight: 0,
                minWidth: 0,
                flex: "1 1 auto",
                overflowY: "auto",
                overflowX: "hidden",
                overscrollBehavior: "contain",
                paddingRight: ke ? "4px" : "0"
              },
              children: [
                t ? (!Array.isArray(t.series) || t.series.length === 0) && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsx(we, { message: t.emptyMessage || "No chart data available.", theme: s, chartHeight: ce }) : /* @__PURE__ */ e.jsx(ts, { model: t, chartType: n, barLayout: g, axisMode: S, theme: s, chartHeight: ce, showLegend: !je, showDataLabels: ve, onCategoryActivate: _t, svgRef: Pe, echartsRef: mt, colorPalette: me, valueFormat: at, yAxisTitle: kt, yAxisMin: T, yAxisMax: Bt, referenceLines: $e, onReferenceLinesChange: Xe, labelAngle: lt, chartTitle: Me ? "" : Ie, onTitleChange: X, subtitle: Le, tickCount: tt, markerShape: Ze, markerSize: yt, showGradient: Te, seriesColors: Fe, showAnimations: dt, showCalloutLabels: We, showTrendline: Be, trendlineMode: Se, trendlinePeriod: Je, legendPosition: vt, labelPosition: Et, decimalPlaces: Kt, gradientDirection: Rt, gradientOpacity: Dt, trendlineColor: Re, trendlineWidth: Nt, seriesSort: wt, lineDashStyles: Ot, yAxisMinRight: o, yAxisMaxRight: l, customPaletteColors: Mt, onElementSelect: b }) : /* @__PURE__ */ e.jsx(we, { message: "No chart data available.", theme: s, chartHeight: ce }),
                Le ? /* @__PURE__ */ e.jsx("div", { style: { textAlign: "center", fontSize: "11px", color: s.textSec, padding: "4px 8px 0", fontStyle: "italic" }, children: Le }) : null,
                !Me && typeof U == "function" ? /* @__PURE__ */ e.jsxs(
                  "div",
                  {
                    onMouseDown: (d) => {
                      d.preventDefault(), m.current = {
                        startY: d.clientY,
                        startHeight: ce
                      };
                    },
                    style: {
                      height: "22px",
                      cursor: "ns-resize",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      gap: "8px",
                      marginTop: "6px",
                      color: s.textSec,
                      fontSize: "11px",
                      fontWeight: 700
                    },
                    title: "Drag to resize the graph height",
                    children: [
                      /* @__PURE__ */ e.jsx("span", { children: "Graph Height" }),
                      /* @__PURE__ */ e.jsx("div", { style: { width: "72px", height: "5px", borderRadius: "999px", background: s.border, opacity: 0.95 } })
                    ]
                  }
                ) : null
              ]
            }
          ),
          D && ke ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
            /* @__PURE__ */ e.jsx(
              "div",
              {
                "data-chart-settings-resizer": "true",
                onMouseDown: (d) => {
                  d.preventDefault(), z.current = {
                    startX: d.clientX,
                    startWidth: oe,
                    direction: "left"
                  };
                },
                style: {
                  width: "8px",
                  cursor: "col-resize",
                  background: "transparent",
                  position: "relative",
                  flexShrink: 0
                },
                title: "Resize chart settings",
                children: /* @__PURE__ */ e.jsx("div", { style: {
                  position: "absolute",
                  top: "50%",
                  left: "50%",
                  transform: "translate(-50%, -50%)",
                  width: "3px",
                  height: "78px",
                  borderRadius: "999px",
                  background: s.border,
                  opacity: 0.92
                } })
              }
            ),
            /* @__PURE__ */ e.jsxs(
              "aside",
              {
                "data-chart-settings-pane": "true",
                style: {
                  width: `${oe}px`,
                  minWidth: "300px",
                  maxWidth: "680px",
                  flexShrink: 0,
                  minHeight: 0,
                  overflow: "hidden",
                  borderLeft: `1px solid ${s.border}`,
                  background: s.sidebarBg || s.surfaceBg || s.background,
                  display: "flex",
                  flexDirection: "column"
                },
                children: [
                  /* @__PURE__ */ e.jsxs("div", { style: {
                    display: "flex",
                    alignItems: "stretch",
                    borderBottom: `1px solid ${s.border}`,
                    background: s.sidebarBg || s.surfaceBg || s.background,
                    flexShrink: 0
                  }, children: [
                    [
                      { id: "chart", label: "Chart" },
                      { id: "data", label: "Data" },
                      { id: "axes", label: "Axes" },
                      { id: "style", label: "Style" },
                      { id: "labels", label: "Labels" }
                    ].map((d) => /* @__PURE__ */ e.jsx(
                      "button",
                      {
                        type: "button",
                        "data-chart-settings-tab": d.id,
                        onClick: () => f(d.id),
                        style: {
                          flex: 1,
                          padding: "9px 4px 7px",
                          fontSize: "11px",
                          fontWeight: p === d.id ? 800 : 600,
                          color: p === d.id ? s.primary : s.textSec,
                          background: "none",
                          border: "none",
                          borderBottom: `2px solid ${p === d.id ? s.primary : "transparent"}`,
                          cursor: "pointer",
                          letterSpacing: "0.03em"
                        },
                        children: d.label
                      },
                      d.id
                    )),
                    /* @__PURE__ */ e.jsx(
                      "button",
                      {
                        type: "button",
                        onClick: () => $(!1),
                        style: {
                          padding: "9px 12px",
                          background: "none",
                          border: "none",
                          borderBottom: "2px solid transparent",
                          color: s.textSec,
                          cursor: "pointer",
                          fontSize: "14px",
                          lineHeight: 1
                        },
                        title: "Close settings",
                        children: "✕"
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs(
                    "div",
                    {
                      "data-chart-settings-scroll": "true",
                      style: {
                        overflowY: "auto",
                        overflowX: "hidden",
                        overscrollBehavior: "contain",
                        padding: "12px",
                        display: "flex",
                        flexDirection: "column",
                        gap: "8px",
                        flex: 1,
                        minHeight: 0,
                        background: s.sidebarBg || s.surfaceBg || s.background
                      },
                      children: [
                        p === "chart" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => h((d) => !d), style: { display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", borderBottom: `1px solid ${s.border}`, padding: "4px 0", cursor: "pointer", marginBottom: j ? "6px" : 0 }, children: [
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec }, children: "Chart Type" }),
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: s.textSec, transform: j ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.15s" }, children: "▾" })
                          ] }),
                          j && /* @__PURE__ */ e.jsx(Yn, { chartType: n, onChange: A, theme: s, includeHierarchyCharts: nr }),
                          /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => k((d) => !d), style: { display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", borderBottom: `1px solid ${s.border}`, padding: "4px 0", cursor: "pointer", marginTop: "8px", marginBottom: w ? "6px" : 0 }, children: [
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec }, children: "Parameters" }),
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: s.textSec, transform: w ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.15s" }, children: "▾" })
                          ] }),
                          w && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !ut.has(n) && !ue ? /* @__PURE__ */ e.jsx(fe, { label: "Source", theme: s, children: /* @__PURE__ */ e.jsx(ns, { orientation: E, onChange: Y, theme: s }) }) : null,
                            n === "bar" ? /* @__PURE__ */ e.jsx(fe, { label: "Direction", theme: s, children: /* @__PURE__ */ e.jsx(ss, { axisMode: S, onChange: N, theme: s }) }) : null,
                            (n === "bar" || n === "area") && !ue && /* @__PURE__ */ e.jsx(fe, { label: "Layout", theme: s, children: /* @__PURE__ */ e.jsx(rs, { chartType: n, barLayout: g, onChange: B, canStack: P, theme: s }) }),
                            Pt ? /* @__PURE__ */ e.jsx(fe, { label: "Level", theme: s, children: /* @__PURE__ */ e.jsx(os, { level: te, onChange: Q, maxLevel: cr, theme: s }) }) : null,
                            ue ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Layers" }),
                              /* @__PURE__ */ e.jsx(ls, { layers: i, onChange: a, availableColumns: r, theme: s })
                            ] }) : null,
                            !(n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !ut.has(n) && !ue) && n !== "bar" && !((n === "bar" || n === "area") && !ue) && !Pt && !ue ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: s.textSec, padding: "4px 0" }, children: "No parameters for this chart type." }) : null
                          ] })
                        ] }),
                        p === "data" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "General" }),
                          typeof X == "function" ? /* @__PURE__ */ e.jsx(fe, { label: "Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: Ie || "", onChange: (d) => X(d.target.value), placeholder: "Chart title", style: { ...Ce, width: "100%" } }) }) : null,
                          /* @__PURE__ */ e.jsx(fe, { label: "Subtitle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: Le, onChange: (d) => Ft(d.target.value), placeholder: "Chart subtitle", style: { ...Ce, width: "100%" } }) }),
                          typeof U == "function" ? /* @__PURE__ */ e.jsx(fe, { label: "Height", theme: s, controlMinWidth: "96px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "180", step: "20", value: F, onChange: (d) => M(d.target.value), onBlur: () => {
                            typeof U == "function" && U(F);
                          }, onKeyDown: (d) => {
                            d.key === "Enter" && typeof U == "function" && U(F);
                          }, style: Ce }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Data & Actions" }),
                          /* @__PURE__ */ e.jsx(fe, { label: "Limits", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(as, { rowLimit: ge, onRowLimitChange: ae, columnLimit: de, onColumnLimitChange: xe, theme: s }) }),
                          typeof ye == "function" ? /* @__PURE__ */ e.jsx(fe, { label: "Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(cs, { sortMode: nt, onChange: ye, theme: s }) }) : null,
                          typeof ye == "function" ? /* @__PURE__ */ e.jsx(fe, { label: "Series Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px", flexWrap: "wrap" }, children: Cn.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => lr(d.value), style: { border: `1px solid ${wt === d.value ? s.primary : s.border}`, background: wt === d.value ? s.select : s.headerSubtleBg || s.hover, color: wt === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }) : null,
                          typeof _e == "function" ? /* @__PURE__ */ e.jsx(fe, { label: "Click", theme: s, children: /* @__PURE__ */ e.jsx(ds, { interactionMode: st, onChange: _e, theme: s }) }) : null,
                          qe && typeof ot == "function" ? /* @__PURE__ */ e.jsx(fe, { label: "Scope", theme: s, children: /* @__PURE__ */ e.jsx(us, { scope: Ht, onChange: ot, theme: s }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Presets" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "6px" }, children: [
                            { key: "trend", label: "Trend" },
                            { key: "compare", label: "Compare" },
                            { key: "composition", label: "Composition" },
                            { key: "flow", label: "Flow" },
                            { key: "sparkline", label: "Sparkline" }
                          ].map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", "data-chart-preset": d.key, onClick: () => Jr(d.key), style: { ...Vt, textAlign: "left" }, children: d.label }, d.key)) })
                        ] }),
                        p === "axes" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(fe, { label: "Y-Axis Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: kt, onChange: (d) => sr(d.target.value), placeholder: "Y-axis title", style: { ...Ce, width: "100%" } }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(fe, { label: "Y Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: T, onChange: (d) => J(d.target.value), placeholder: "Min", style: { ...Ce, width: "50%" } }),
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: Bt, onChange: (d) => Ke(d.target.value), placeholder: "Max", style: { ...Ce, width: "50%" } })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(fe, { label: "Y Ticks", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "20", value: tt, onChange: (d) => Ee(Math.max(2, Math.min(20, Number(d.target.value) || 5))), style: { ...Ce, width: "80px" } }) }),
                            /* @__PURE__ */ e.jsx(fe, { label: "X Angle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center", flexWrap: "wrap" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", min: "0", max: "90", value: lt, onChange: (d) => Ne(Math.max(0, Math.min(90, Number(d.target.value) || 0))), style: { ...Ce, width: "58px" }, title: "X-axis label angle (0–90°)" }),
                              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.textSec }, children: "°" }),
                              [0, 30, 45, 90].map((d) => /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => Ne(d), style: { ...Ce, padding: "4px 6px", background: lt === d ? s.select : s.headerSubtleBg || s.hover, color: lt === d ? s.primary : s.text, border: `1px solid ${lt === d ? s.primary : s.border}` }, children: [
                                d,
                                "°"
                              ] }, d))
                            ] }) })
                          ] }) : null,
                          ue ? /* @__PURE__ */ e.jsx(fe, { label: "Right Axis Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: o, onChange: (d) => c(d.target.value), placeholder: "Min", style: { ...Ce, width: "50%" } }),
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: l, onChange: (d) => u(d.target.value), placeholder: "Max", style: { ...Ce, width: "50%" } })
                          ] }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && !ut.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Reference Lines" }),
                            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: [
                              $e.map((d, q) => /* @__PURE__ */ e.jsx(Vn, { line: d, onUpdate: (pe) => Xe((be) => be.map((Ve, Qt) => Qt === q ? pe : Ve)), onRemove: () => Xe((pe) => pe.filter((be, Ve) => Ve !== q)), exportButtonStyle: Vt, compactInputStyle: Ce, theme: s }, d.id)),
                              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => gt((d) => ({ ...d, orient: d.orient === "v" ? "h" : "v" })), style: { ...Vt, padding: "4px 7px", flexShrink: 0, fontWeight: 800, color: Ge.orient === "v" ? s.primary : s.textSec }, children: Ge.orient === "v" ? "|" : "—" }),
                                /* @__PURE__ */ e.jsx("input", { type: "number", value: Ge.value, onChange: (d) => gt((q) => ({ ...q, value: d.target.value })), placeholder: Ge.orient === "v" ? "Cat index" : "Value", style: { ...Ce, width: "60px", flexShrink: 0 } }),
                                /* @__PURE__ */ e.jsx("input", { type: "text", value: Ge.label, onChange: (d) => gt((q) => ({ ...q, label: d.target.value })), placeholder: "Label", style: { ...Ce, flex: 1, minWidth: 0 } }),
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => {
                                  Ge.value !== "" && (Xe((d) => [...d, { id: Date.now(), value: Ge.value, label: Ge.label, orient: Ge.orient, color: "", dash: "dashed" }]), gt({ value: "", label: "", orient: Ge.orient }));
                                }, style: { ...Vt, padding: "4px 10px", flexShrink: 0 }, children: "+" })
                              ] }),
                              Sr ? /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: s.textSec }, children: [
                                "Data range: ",
                                At(Sr.min, at),
                                " — ",
                                At(Sr.max, at)
                              ] }) : null
                            ] })
                          ] }) : null
                        ] }),
                        p === "style" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}` }, children: "Colors" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" }, children: [...un, ...Mt.length >= 2 ? ["custom"] : []].map((d) => {
                            const q = me === d, pe = d === "custom" ? Mt : xn[d];
                            return /* @__PURE__ */ e.jsxs(
                              "button",
                              {
                                type: "button",
                                onClick: () => ht(d),
                                title: d.charAt(0).toUpperCase() + d.slice(1),
                                style: { display: "flex", alignItems: "center", gap: "3px", padding: "3px 6px", border: `1.5px solid ${q ? s.primary : s.border}`, background: q ? s.select : s.headerSubtleBg || s.hover, borderRadius: s.radiusSm || "8px", cursor: "pointer" },
                                children: [
                                  (pe || []).slice(0, 4).map((be, Ve) => /* @__PURE__ */ e.jsx("span", { style: { width: "8px", height: "8px", borderRadius: "50%", background: be, flexShrink: 0 } }, Ve)),
                                  /* @__PURE__ */ e.jsx("span", { style: { fontSize: "9px", fontWeight: q ? 700 : 600, color: q ? s.primary : s.textSec, marginLeft: "1px", textTransform: "capitalize" }, children: d === "a11y" ? "A11y" : d })
                                ]
                              },
                              d
                            );
                          }) }),
                          Array.isArray(t && t.series) && t.series.length > 0 ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px" }, children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: s.textSec, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "2px" }, children: "Override per series" }),
                            t.series.slice(0, 20).map((d, q) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "5px" }, children: [
                              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "2px", flexShrink: 0 }, children: I.slice(0, 6).map((pe, be) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Lt((Ve) => ({ ...Ve, [d.name]: pe })), style: { width: "14px", height: "14px", borderRadius: "50%", background: pe, border: `2px solid ${(Fe[d.name] || Ae(q, s, I)) === pe ? s.text : "transparent"}`, cursor: "pointer", padding: 0, flexShrink: 0 }, title: pe }, be)) }),
                              /* @__PURE__ */ e.jsx("input", { type: "color", value: Fe[d.name] || Ae(q, s, I), onChange: (pe) => Lt((be) => ({ ...be, [d.name]: pe.target.value })), style: { width: "24px", height: "24px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none", flexShrink: 0 }, title: `Custom color for ${d.name}` }),
                              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: d.name || `Series ${q + 1}` }),
                              Fe[d.name] ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Lt((pe) => {
                                const be = { ...pe };
                                return delete be[d.name], be;
                              }), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 2px" }, title: "Reset to palette", children: "↺" }) : null
                            ] }, d.name || q)),
                            t.series.length > 20 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: s.textSec }, children: `…${t.series.length - 20} more` }) : null
                          ] }) : null,
                          /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${s.border}`, paddingTop: "8px" }, children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: s.textSec, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "6px" }, children: "Custom Palette" }),
                            /* @__PURE__ */ e.jsx(zn, { customColors: Mt, onChange: qt, theme: s, compactInputStyle: Ce }),
                            Mt.length >= 2 ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ht("custom"), style: { ...Vt, marginTop: "6px", background: me === "custom" ? s.select : void 0, color: me === "custom" ? s.primary : void 0, border: `1px solid ${me === "custom" ? s.primary : s.border}` }, children: me === "custom" ? "Custom active" : "Use custom palette" }) : null
                          ] }),
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Effects" }),
                          /* @__PURE__ */ e.jsx(fe, { label: "Animate", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => St((d) => !d), style: { border: `1px solid ${dt ? s.primary : s.border}`, background: dt ? s.select : s.headerSubtleBg || s.hover, color: dt ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: dt ? "On" : "Off" }) }),
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(fe, { label: "Gradient", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ct((d) => !d), style: { border: `1px solid ${Te ? s.primary : s.border}`, background: Te ? s.select : s.headerSubtleBg || s.hover, color: Te ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: Te ? "On" : "Off" }) }),
                            Te ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(fe, { label: "Grad Dir", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: Cr.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => br(d.value), style: { border: `1px solid ${Rt === d.value ? s.primary : s.border}`, background: Rt === d.value ? s.select : s.headerSubtleBg || s.hover, color: Rt === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }),
                              /* @__PURE__ */ e.jsx(fe, { label: "Grad Opacity", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "range", min: "10", max: "100", step: "5", value: Dt, onChange: (d) => yr(Number(d.target.value)), style: { flex: 1 } }),
                                /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "11px", color: s.textSec, minWidth: "30px" }, children: [
                                  Dt,
                                  "%"
                                ] })
                              ] }) })
                            ] }) : null
                          ] }) : null,
                          n === "line" || n === "scatter" || n === "bubble" || n === "area" || n === "radar" || ue ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Markers" }),
                            /* @__PURE__ */ e.jsx(fe, { label: "Shape", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: Ze, onChange: (d) => bt(d.target.value), style: { ...Ce, width: "100%" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: "none", children: "None (line only)" }),
                              /* @__PURE__ */ e.jsx("option", { value: "circle", children: "Circle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "square", children: "Square" }),
                              /* @__PURE__ */ e.jsx("option", { value: "diamond", children: "Diamond" }),
                              /* @__PURE__ */ e.jsx("option", { value: "triangle", children: "Triangle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "cross", children: "Cross" })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(fe, { label: "Size", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs("select", { value: yt, onChange: (d) => rt(Number(d.target.value)), style: { ...Ce, width: "80px" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: 3, children: "Small" }),
                              /* @__PURE__ */ e.jsx("option", { value: 5, children: "Medium" }),
                              /* @__PURE__ */ e.jsx("option", { value: 7, children: "Large" }),
                              /* @__PURE__ */ e.jsx("option", { value: 10, children: "XLarge" })
                            ] }) })
                          ] }) : null,
                          (n === "line" || n === "area" || ue) && Array.isArray(t && t.series) && t.series.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Line Style" }),
                            /* @__PURE__ */ e.jsx(Wn, { series: t.series, lineDashStyles: Ot, onChange: mr, seriesColors: Fe, paletteColors: I, getColorForIndex: Ae, theme: s, compactInputStyle: Ce })
                          ] }) : null,
                          n === "bar" || n === "line" || n === "area" || n === "scatter" || ue ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Trendline" }),
                            /* @__PURE__ */ e.jsx(fe, { label: "Show", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Ue((d) => !d), style: { border: `1px solid ${Be ? s.primary : s.border}`, background: Be ? s.select : s.headerSubtleBg || s.hover, color: Be ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: Be ? "On" : "Off" }) }),
                            Be ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(fe, { label: "Type", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: Se, onChange: (d) => Ye(d.target.value), style: { ...Ce, width: "100%" }, children: [
                                /* @__PURE__ */ e.jsx("option", { value: "linear", children: "Linear Regression" }),
                                /* @__PURE__ */ e.jsx("option", { value: "moving_avg", children: "Moving Average" }),
                                /* @__PURE__ */ e.jsx("option", { value: "exponential", children: "Exponential Smoothing" })
                              ] }) }),
                              Se === "moving_avg" ? /* @__PURE__ */ e.jsx(fe, { label: "Period", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "50", value: Je, onChange: (d) => or(Math.max(2, Math.min(50, Number(d.target.value) || 3))), style: { ...Ce, width: "80px" } }) }) : null,
                              /* @__PURE__ */ e.jsxs(fe, { label: "Color", theme: s, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "color", value: Re || "#888888", onChange: (d) => x(d.target.value), style: { width: "32px", height: "28px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }, title: "Trendline color" }),
                                Re ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => x(""), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 4px" }, title: "Reset to auto", children: "↺" }) : null
                              ] }),
                              /* @__PURE__ */ e.jsx(fe, { label: "Width", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "1", max: "8", value: Nt, onChange: (d) => vr(Math.max(1, Math.min(8, Number(d.target.value) || 2))), style: { ...Ce, width: "80px" } }) })
                            ] }) : null
                          ] }) : null
                        ] }),
                        p === "labels" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(fe, { label: "Data Labels", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => zt((d) => !d), style: { border: `1px solid ${ve ? s.primary : s.border}`, background: ve ? s.select : s.headerSubtleBg || s.hover, color: ve ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: ve ? "On" : "Off" }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" ? /* @__PURE__ */ e.jsx(fe, { label: "Format", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("select", { value: at, onChange: (d) => He(d.target.value), style: { ...Ce, width: "100%" }, title: "Value format for axis labels", children: pn.map((d) => /* @__PURE__ */ e.jsx("option", { value: d.value, children: d.label }, d.value)) }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsx(fe, { label: "Decimals", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: Kt, onChange: (d) => Zt(Number(d.target.value)), style: { ...Ce, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: -1, children: "Auto" }),
                            /* @__PURE__ */ e.jsx("option", { value: 0, children: "0  (e.g. 1,234)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 1, children: "1  (e.g. 1.2)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 2, children: "2  (e.g. 1.23)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 3, children: "3  (e.g. 1.234)" })
                          ] }) }) : null,
                          ve && n === "bar" ? /* @__PURE__ */ e.jsx(fe, { label: "Position", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: Et, onChange: (d) => Ut(d.target.value), style: { ...Ce, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: "outside", children: "Outside" }),
                            /* @__PURE__ */ e.jsx("option", { value: "inside", children: "Inside top" }),
                            /* @__PURE__ */ e.jsx("option", { value: "center", children: "Center" })
                          ] }) }) : null,
                          n === "pie" || n === "donut" ? /* @__PURE__ */ e.jsx(fe, { label: "Callouts", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ee((d) => !d), style: { border: `1px solid ${We ? s.primary : s.border}`, background: We ? s.select : s.headerSubtleBg || s.hover, color: We ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: We ? "On" : "Off" }) }) : null,
                          n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsx(fe, { label: "Legend", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(Mn, { legendPosition: vt, onChange: ar, theme: s }) }) : null
                        ] })
                      ]
                    }
                  )
                ]
              }
            )
          ] }) : null
        ] }),
        D ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap", alignItems: "center" }, children: [
          /* @__PURE__ */ e.jsx(Qn, { model: t, theme: s }),
          /* @__PURE__ */ e.jsx($n, { model: t, maxCategories: Gr, maxSeries: Xr, theme: s })
        ] }) : null
      ]
    }
  );
}, ys = ({
  onClose: t,
  source: n,
  onSourceChange: i,
  chartType: a,
  onChartTypeChange: r,
  chartLayers: g = [],
  onChartLayersChange: S,
  availableColumns: A = [],
  barLayout: B,
  onBarLayoutChange: N,
  axisMode: E,
  onAxisModeChange: Y,
  orientation: te,
  onOrientationChange: Q,
  hierarchyLevel: s,
  onHierarchyLevelChange: _,
  model: X,
  theme: O,
  width: G,
  chartTitle: K,
  onChartTitleChange: U,
  chartHeight: ge,
  onChartHeightChange: ae,
  onResizeStart: de,
  floating: xe = !1,
  onToggleFloating: nt,
  floatingRect: ye = null,
  onFloatingDragStart: st,
  onFloatingResizeStart: _e,
  rowLimit: Ht,
  onRowLimitChange: ot,
  columnLimit: qe,
  onColumnLimitChange: Oe,
  sortMode: Me,
  onSortModeChange: _t,
  interactionMode: nr,
  onInteractionModeChange: et,
  serverScope: Xt,
  onServerScopeChange: Gt,
  chartDefinitions: Qe = [],
  activeChartId: pt = null,
  onActiveChartChange: Tt,
  onCreateChart: W,
  onDuplicateChart: $t,
  onDeleteChart: ue,
  onRenameChart: je,
  onCategoryActivate: P,
  allowHierarchyCharts: Ie = !0,
  showServerScope: Wt = !1,
  locked: ft = !1,
  onToggleLock: it,
  dockPosition: De = "right",
  onDockPositionChange: ke,
  immersiveMode: $,
  onImmersiveModeChange: ve,
  onSettingsWidthBudgetChange: zt,
  standalone: me = !1,
  showResizeHandle: ht = !0,
  title: at = "Chart Panel",
  showDefinitionManager: He = !0,
  initialSettings: kt = {},
  onSettingsChange: sr,
  initialSettingsPaneOpen: T = !1
}) => {
  fn("PivotChartPanel", pt);
  const [J, Bt] = v.useState(!1), [Ke, $e] = v.useState(!1), [Xe, Ge] = v.useState(T), [gt, lt] = v.useState(348), Ne = v.useRef(null), Le = v.useRef(Or(
    G,
    430,
    xr,
    Ir
  )), Ft = v.useRef(zt), tt = v.useRef(null), Ee = typeof $ == "boolean" ? $ : Ke, Ze = () => {
    const ee = !Ee;
    if (typeof ve == "function") {
      ve(ee);
      return;
    }
    $e(ee);
  }, bt = xe && ft, yt = (ee) => ({
    border: `1px solid ${ee === n ? O.primary : O.border}`,
    background: ee === n ? O.select : O.headerSubtleBg || O.hover,
    color: ee === n ? O.primary : O.text,
    borderRadius: O.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }), rt = (ee = !1) => ({
    border: `1px solid ${ee ? O.primary : O.border}`,
    background: ee ? O.select : O.headerSubtleBg || O.hover,
    color: ee ? O.primary : O.text,
    borderRadius: O.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "6px",
    minWidth: "32px"
  }), Te = (ee) => ({
    ...rt(De === ee),
    minWidth: "56px",
    padding: "6px 10px"
  }), ct = !Ee && Xe ? gt + 9 : 0, Fe = Or(
    G,
    430,
    xr,
    Ir
  ), Lt = ye && Number.isFinite(Number(ye.width)) ? Math.max(xr, Math.floor(Number(ye.width))) : Fe, dt = ye && Number.isFinite(Number(ye.height)) ? Math.max(hn, Math.floor(Number(ye.height))) : Math.max(gn, (Number(ge) || rr) + _r);
  v.useEffect(() => {
    Ft.current = zt;
  }, [zt]);
  const St = v.useCallback((ee, Be = {}) => {
    const Ue = Number(ee), Se = Number.isFinite(Ue) ? Math.max(320, Math.ceil(Ue)) : null;
    if (!Be.force && tt.current === Se) return;
    tt.current = Se;
    const Ye = Ft.current;
    typeof Ye == "function" && Ye(Se);
  }, []), We = J ? {
    position: "fixed",
    inset: "16px",
    zIndex: 10040,
    display: "flex",
    minWidth: 0,
    minHeight: 0
  } : xe ? {
    position: "absolute",
    left: `${ye && ye.left || 24}px`,
    top: `${ye && ye.top || 24}px`,
    width: `${Lt + ct}px`,
    height: `${dt}px`,
    zIndex: 240,
    display: "flex",
    minWidth: 0,
    minHeight: 0
  } : me ? {
    display: "flex",
    flexGrow: 1,
    flexShrink: 1,
    flexBasis: "auto",
    width: "100%",
    height: "100%",
    minWidth: 0,
    minHeight: 0,
    overflow: "hidden"
  } : {
    display: "flex",
    width: `${Fe + ct}px`,
    minWidth: `${Fe + ct}px`,
    maxWidth: `${Fe + ct}px`,
    flexShrink: 0
  };
  return v.useEffect(() => {
    if (!me || xe || J) return;
    const ee = Ne.current;
    if (!ee) return;
    const Be = () => {
      const Se = ee.clientWidth;
      Se > 0 && !Xe && (Le.current = Math.max(xr, Se));
    };
    if (Be(), typeof ResizeObserver > "u") return;
    const Ue = new ResizeObserver(Be);
    return Ue.observe(ee), () => Ue.disconnect();
  }, [xe, J, Xe, me]), v.useEffect(() => {
    if (!me || xe || J || Ee || !Xe) {
      St(null);
      return;
    }
    const ee = Le.current || Fe;
    St(ee + ct);
  }, [
    Fe,
    St,
    Ee,
    xe,
    J,
    Xe,
    me,
    ct
  ]), v.useEffect(() => () => {
    St(null, { force: !0 });
  }, [St]), /* @__PURE__ */ e.jsxs("div", { style: We, children: [
    !J && !xe && !me && ht ? /* @__PURE__ */ e.jsx(
      "div",
      {
        onMouseDown: (ee) => {
          ee.preventDefault(), de && de();
        },
        style: {
          width: "8px",
          cursor: "col-resize",
          background: "transparent",
          position: "relative",
          flexShrink: 0
        },
        title: "Resize chart panel",
        children: /* @__PURE__ */ e.jsx("div", { style: {
          position: "absolute",
          top: "50%",
          left: "50%",
          transform: "translate(-50%, -50%)",
          width: "3px",
          height: "72px",
          borderRadius: "999px",
          background: O.border,
          opacity: 0.9
        } })
      }
    ) : null,
    /* @__PURE__ */ e.jsxs("aside", { ref: Ne, style: {
      width: J || xe || me ? "100%" : `calc(100% - ${ht ? 8 : 0}px)`,
      height: J || xe || me ? "100%" : "auto",
      minWidth: 0,
      minHeight: 0,
      borderLeft: me || xe || J ? "none" : `1px solid ${O.border}`,
      border: xe || J ? `1px solid ${O.border}` : "none",
      borderRadius: xe || J ? O.radius || "16px" : 0,
      background: O.sidebarBg || O.surfaceBg || O.background,
      padding: J ? "12px" : "14px",
      overflow: "hidden",
      boxShadow: xe || J ? O.shadowMd || "0 18px 40px rgba(15,23,42,0.24)" : me ? "none" : `-10px 0 24px ${O.pinnedBoundaryShadow || "rgba(15,23,42,0.12)"}`,
      display: "flex",
      flexDirection: "column",
      gap: Ee ? "8px" : "0",
      position: "relative"
    }, children: [
      /* @__PURE__ */ e.jsxs(
        "div",
        {
          onMouseDown: (ee) => {
            xe && !J && !bt && typeof st == "function" && st(ee);
          },
          style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px", marginBottom: Ee ? "0" : "12px", cursor: xe && !J && !bt ? "move" : "default" },
          children: [
            Ee ? /* @__PURE__ */ e.jsx("div", {}) : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "4px", minWidth: 0 }, children: /* @__PURE__ */ e.jsx("div", { style: { fontSize: "15px", fontWeight: 800, color: O.text }, children: at }) }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, onMouseDown: (ee) => ee.stopPropagation(), children: [
              typeof nt == "function" ? /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: nt,
                  style: rt(xe),
                  title: xe ? "Dock chart panel" : "Float chart over table",
                  "aria-label": xe ? "Dock chart panel" : "Float chart over table",
                  children: xe ? "Dock" : "Float"
                }
              ) : null,
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: Ze,
                  style: rt(Ee),
                  title: Ee ? "Exit immersive mode" : "Enter immersive mode",
                  "aria-label": Ee ? "Exit immersive mode" : "Enter immersive mode",
                  children: "Immersive"
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => Bt((ee) => !ee),
                  style: rt(J),
                  title: J ? "Exit full screen" : "Open chart full screen",
                  "aria-label": J ? "Exit full screen" : "Open chart full screen",
                  children: J ? /* @__PURE__ */ e.jsx(tr.FullscreenExit, {}) : /* @__PURE__ */ e.jsx(tr.Fullscreen, {})
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: t,
                  style: rt(!1),
                  title: me ? "Close chart pane" : "Close chart panel",
                  "aria-label": me ? "Close chart pane" : "Close chart panel",
                  children: J ? /* @__PURE__ */ e.jsx(tr.Close, {}) : "Close"
                }
              )
            ] })
          ]
        }
      ),
      !Ee && He ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", marginBottom: "10px", flexWrap: "wrap", alignItems: "center" }, children: [
        Array.isArray(Qe) && Qe.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
          /* @__PURE__ */ e.jsx(
            "select",
            {
              value: pt || "",
              onChange: (ee) => {
                typeof Tt == "function" && Tt(ee.target.value);
              },
              style: {
                border: `1px solid ${O.border}`,
                background: O.surfaceBg || O.background || "#fff",
                color: O.text,
                borderRadius: O.radiusSm || "8px",
                padding: "6px 8px",
                fontSize: "11px",
                fontWeight: 700,
                minWidth: "160px"
              },
              children: Qe.map((ee) => /* @__PURE__ */ e.jsx("option", { value: ee.id, children: ee.name || ee.id }, ee.id))
            }
          ),
          /* @__PURE__ */ e.jsx(
            "input",
            {
              type: "text",
              value: (Qe.find((ee) => ee.id === pt) || {}).name || "",
              onChange: (ee) => {
                typeof je == "function" && je(ee.target.value);
              },
              placeholder: "Chart name",
              style: {
                border: `1px solid ${O.border}`,
                background: O.surfaceBg || O.background || "#fff",
                color: O.text,
                borderRadius: O.radiusSm || "8px",
                padding: "6px 8px",
                fontSize: "11px",
                fontWeight: 700,
                minWidth: "180px"
              }
            }
          )
        ] }) : null,
        typeof W == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: rt(!1), onClick: W, children: "New" }) : null,
        typeof $t == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: rt(!1), onClick: $t, children: "Duplicate" }) : null,
        typeof ue == "function" ? /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            style: rt(!1),
            onClick: ue,
            disabled: !Array.isArray(Qe) || Qe.length <= 1,
            children: "Delete"
          }
        ) : null
      ] }) : null,
      !Ee && me && !xe && typeof ke == "function" ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "10px", flexWrap: "wrap", alignItems: "center" }, children: [
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("left"), style: Te("left"), title: "Dock chart pane to the left", children: "Left" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("right"), style: Te("right"), title: "Dock chart pane to the right", children: "Right" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("top"), style: Te("top"), title: "Dock chart pane to the top", children: "Top" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("bottom"), style: Te("bottom"), title: "Dock chart pane to the bottom", children: "Bottom" })
      ] }) : null,
      Ee ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "12px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ e.jsx("button", { style: yt("pivot"), onClick: () => i("pivot"), children: "Pivot View" }),
        /* @__PURE__ */ e.jsx("button", { style: yt("selection"), onClick: () => i("selection"), children: "Selection" })
      ] }),
      /* @__PURE__ */ e.jsx(
        qr,
        {
          model: X,
          chartType: a,
          chartLayers: g,
          onChartLayersChange: S,
          availableColumns: A,
          barLayout: B,
          axisMode: E,
          onChartTypeChange: r,
          onBarLayoutChange: N,
          onAxisModeChange: Y,
          orientation: te,
          onOrientationChange: Q,
          hierarchyLevel: s,
          onHierarchyLevelChange: _,
          rowLimit: Ht,
          onRowLimitChange: ot,
          columnLimit: qe,
          onColumnLimitChange: Oe,
          title: K,
          onTitleChange: U,
          chartHeight: ge,
          onChartHeightChange: ae,
          sortMode: Me,
          onSortModeChange: _t,
          interactionMode: nr,
          onInteractionModeChange: et,
          serverScope: Xt,
          onServerScopeChange: Gt,
          showServerScope: Wt,
          theme: O,
          immersiveMode: Ee,
          onCategoryActivate: P,
          allowHierarchyCharts: Ie,
          floating: xe,
          locked: ft,
          onToggleLock: it,
          configOpen: Xe,
          onConfigChange: Ge,
          settingsPanelWidth: gt,
          onSettingsPanelWidthChange: lt,
          initialSettings: kt,
          onSettingsChange: sr
        }
      ),
      xe && !J && !ft ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (ee) => typeof _e == "function" && _e("right", ee),
            style: { position: "absolute", top: 0, right: 0, width: "10px", height: "100%", cursor: "ew-resize" }
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (ee) => typeof _e == "function" && _e("bottom", ee),
            style: { position: "absolute", left: 0, bottom: 0, width: "100%", height: "10px", cursor: "ns-resize" }
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (ee) => typeof _e == "function" && _e("corner", ee),
            style: { position: "absolute", right: 0, bottom: 0, width: "18px", height: "18px", cursor: "nwse-resize" }
          }
        )
      ] }) : null
    ] })
  ] });
}, js = (t) => t.open ? /* @__PURE__ */ e.jsx(ys, { ...t }) : null, ks = ({ chartState: t, onClose: n, theme: i, position: a = "right", onPositionChange: r }) => {
  const [g, S] = v.useState(t && t.chartType ? t.chartType : "bar"), [A, B] = v.useState(t && t.barLayout ? t.barLayout : "grouped"), [N, E] = v.useState(t && t.axisMode ? t.axisMode : "vertical"), [Y, te] = v.useState(t && t.defaultOrientation ? t.defaultOrientation : "rows"), [Q, s] = v.useState(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), [_, X] = v.useState(t && t.rowLimit ? t.rowLimit : Nr), [O, G] = v.useState(t && t.columnLimit ? t.columnLimit : Pr), [K, U] = v.useState(t && t.sortMode ? t.sortMode : "natural"), [ge, ae] = v.useState(t && Array.isArray(t.chartLayers) ? t.chartLayers : []), [de, xe] = v.useState(null);
  v.useEffect(() => {
    S(t && t.chartType ? t.chartType : "bar"), B(t && t.barLayout ? t.barLayout : "grouped"), E(t && t.axisMode ? t.axisMode : "vertical"), te(t && t.defaultOrientation ? t.defaultOrientation : "rows"), s(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), X(t && t.rowLimit ? t.rowLimit : Nr), G(t && t.columnLimit ? t.columnLimit : Pr), U(t && t.sortMode ? t.sortMode : "natural"), ae(t && Array.isArray(t.chartLayers) ? t.chartLayers : []);
  }, [t]);
  const nt = v.useMemo(() => {
    if (!t || !Array.isArray(t.visibleCols)) return [];
    if (!(t.selectionMap && t.visibleRows))
      return Mr(t.visibleCols);
    const qe = an(t.selectionMap, t.visibleRows, t.visibleCols);
    if (!qe) return [];
    const Oe = new Set(
      (qe.colIndexes || []).map((Me) => t.visibleCols[Me]).filter((Me) => Me && Me.id && !ln.has(Me.id)).map((Me) => Me.id)
    );
    return Mr(t.visibleCols).filter((Me) => Oe.has(Me.id));
  }, [t]), ye = v.useMemo(() => t ? t.selectionMap && t.visibleRows && t.visibleCols ? g === "combo" ? cn(t.selectionMap, t.visibleRows, t.visibleCols, {
    hierarchyLevel: Q,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: _,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: K,
    layers: ge
  }) : dn(t.selectionMap, t.visibleRows, t.visibleCols, {
    orientation: Y,
    hierarchyLevel: Q,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: _,
    maxColumns: O,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: K
  }) : t.model : null, [ge, t, g, Q, Y, _, O, K]);
  if (v.useEffect(() => {
    if (A !== "stacked") return;
    if (g === "combo") {
      B("grouped");
      return;
    }
    (g === "area" ? ye && Array.isArray(ye.series) && ye.series.length > 1 : Vr(ye)) || B("grouped");
  }, [ye, A, g]), !t) return null;
  const st = a === "top" || a === "bottom", _e = Math.max(420, Number.isFinite(Number(de)) ? Math.floor(Number(de)) : 420), Ht = Math.max(360, Number(t.chartHeight || rr) + _r), ot = (qe, Oe) => ({
    border: `1px solid ${qe === a ? i.primary : i.border}`,
    background: qe === a ? i.select : i.headerSubtleBg || i.hover,
    color: qe === a ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "5px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsx(
    "aside",
    {
      "data-chart-modal-position": a,
      style: {
        display: "flex",
        flexDirection: "column",
        width: st ? "100%" : `${_e}px`,
        minWidth: st ? 0 : `${Math.max(320, _e)}px`,
        maxWidth: st ? "100%" : "70%",
        height: st ? `${Ht}px` : void 0,
        minHeight: st ? "320px" : 0,
        flexShrink: 0,
        overflow: "hidden",
        borderLeft: a === "right" ? `1px solid ${i.border}` : "none",
        borderRight: a === "left" ? `1px solid ${i.border}` : "none",
        borderTop: a === "bottom" ? `1px solid ${i.border}` : "none",
        borderBottom: a === "top" ? `1px solid ${i.border}` : "none",
        background: i.sidebarBg || i.surfaceBg || i.background
      },
      children: /* @__PURE__ */ e.jsxs("div", { style: { padding: "12px 14px", overflow: "hidden", flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: "12px" }, children: [
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }, children: [
          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: i.text }, children: t.title || "Range Chart" }),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px" }, children: [
            typeof r == "function" ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => r("left"),
                  style: ot("left"),
                  title: "Dock chart pane to the left",
                  "aria-label": "Dock left",
                  children: "Left"
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => r("right"),
                  style: ot("right"),
                  title: "Dock chart pane to the right",
                  "aria-label": "Dock right",
                  children: "Right"
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => r("top"),
                  style: ot("top"),
                  title: "Dock chart pane to the top",
                  "aria-label": "Dock top",
                  children: "Top"
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => r("bottom"),
                  style: ot("bottom"),
                  title: "Dock chart pane to the bottom",
                  "aria-label": "Dock bottom",
                  children: "Bottom"
                }
              )
            ] }) : null,
            /* @__PURE__ */ e.jsx(
              "button",
              {
                onClick: n,
                style: {
                  border: `1px solid ${i.border}`,
                  background: i.headerSubtleBg || i.hover,
                  color: i.text,
                  borderRadius: i.radiusSm || "8px",
                  padding: "5px 10px",
                  fontSize: "11px",
                  fontWeight: 700,
                  cursor: "pointer"
                },
                children: "Close"
              }
            )
          ] })
        ] }),
        /* @__PURE__ */ e.jsx(
          qr,
          {
            model: ye,
            chartType: g,
            chartLayers: ge,
            onChartLayersChange: ae,
            availableColumns: nt,
            barLayout: A,
            axisMode: N,
            onChartTypeChange: S,
            onBarLayoutChange: B,
            onAxisModeChange: E,
            orientation: Y,
            onOrientationChange: te,
            hierarchyLevel: Q,
            onHierarchyLevelChange: s,
            title: t.title || "Range Chart",
            rowLimit: _,
            onRowLimitChange: X,
            columnLimit: O,
            onColumnLimitChange: G,
            sortMode: K,
            onSortModeChange: U,
            theme: i,
            allowHierarchyCharts: !(t.selectionMap && t.visibleRows && t.visibleCols),
            onSettingsWidthBudgetChange: xe
          }
        )
      ] })
    }
  );
};
export {
  ks as PivotChartModal,
  js as PivotChartPanel
};
