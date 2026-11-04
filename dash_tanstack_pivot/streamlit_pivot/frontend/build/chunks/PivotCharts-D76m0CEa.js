import { j as e, C as nr, r as Xr, c as on, a as Ar, n as It, e as At, t as he, l as an, g as Ce, b as ln, d as Dr, f as cn, p as ar, h as Nr, i as Pr, k as xr, m as Ir, o as Ct, M as Hr, q as Fr, s as Cr, u as dn, I as un, v as xn, w as pn, x as Gr, y as Tr, z as rr, A as fn, B as hn, D as gn, V as yn, E as Ur, F as Kr, G as bn, H as pr, J as vn, K as mn, L as jn, N as kn, O as Or, P as wn, Q as Sn, R as Mn, S as Yr, T as An } from "./index-CVsYjN62.js";
import { a as v, R as xt } from "./charts-DthW1Q8W.js";
function Cn(t, n, i) {
  const f = Math.min(n.width, Math.max(0, i.width - 16)), m = Math.max(0, i.height - 8 - t.bottom - 6), M = Math.max(0, t.top - 6 - 8), W = n.height > m && M > m, R = W ? M : m, $ = Math.min(n.height, R);
  return {
    left: Math.max(8, Math.min(t.right - f, i.width - 8 - f)),
    top: W ? t.top - 6 - $ : t.bottom + 6,
    maxWidth: Math.max(0, i.width - 16),
    maxHeight: R
  };
}
function $n(t, n) {
  var W;
  const i = v.useRef(null), a = v.useRef(null), [r, f] = v.useState(null), m = (W = i.current) == null ? void 0 : W.ownerDocument, M = (m == null ? void 0 : m.fullscreenElement) || (m == null ? void 0 : m.body);
  return v.useLayoutEffect(() => {
    if (!t || !i.current || !a.current) return;
    const R = i.current, $ = a.current, O = R.ownerDocument, J = O.defaultView, K = () => f(Cn(
      R.getBoundingClientRect(),
      { width: $.offsetWidth, height: $.scrollHeight + 2 },
      { width: O.documentElement.clientWidth, height: O.documentElement.clientHeight }
    )), s = () => Array.from($.querySelectorAll('[role="menuitem"]:not(:disabled)')), _ = (F = !1) => {
      n(!1), f(null), F && R.focus();
    }, T = (F) => {
      var de;
      const U = ((de = F.composedPath) == null ? void 0 : de.call(F)) || [];
      [R, $].some((ne) => U.includes(ne) || ne.contains(F.target)) || _();
    }, V = (F) => {
      var U, de;
      if (F.key === "Escape")
        F.preventDefault(), F.stopPropagation(), _(!0);
      else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(F.key)) {
        if (!$.contains(F.target) && !((U = F.composedPath) != null && U.call(F).includes(R))) return;
        F.preventDefault();
        const ne = s(), ae = ne.indexOf(O.activeElement), ue = F.key === "Home" ? 0 : F.key === "End" ? ne.length - 1 : (ae + (F.key === "ArrowUp" ? -1 : 1) + ne.length) % ne.length;
        (de = ne[ue]) == null || de.focus();
      } else F.key === "Tab" && _(!0);
    };
    K(), O.addEventListener("mousedown", T, !0), O.addEventListener("keydown", V, !0), O.addEventListener("scroll", K, !0), J.addEventListener("resize", K);
    const Y = typeof J.ResizeObserver == "function" ? new J.ResizeObserver(K) : null;
    return Y == null || Y.observe(R), Y == null || Y.observe($), () => {
      O.removeEventListener("mousedown", T, !0), O.removeEventListener("keydown", V, !0), O.removeEventListener("scroll", K, !0), J.removeEventListener("resize", K), Y == null || Y.disconnect();
    };
  }, [t, M, n]), v.useLayoutEffect(() => {
    var R, $;
    t && r && (($ = (R = a.current) == null ? void 0 : R.querySelector('[role="menuitem"]:not(:disabled)')) == null || $.focus());
  }, [t, !!r]), { anchorRef: i, menuRef: a, position: r, portalTarget: M };
}
const Wr = [
  { value: "solid", label: "Solid", dash: "none" },
  { value: "dashed", label: "Dashed", dash: "8 4" },
  { value: "dotted", label: "Dotted", dash: "2 4" },
  { value: "dot-dash", label: "Dot-Dash", dash: "8 4 2 4" }
], fr = (t) => {
  const n = Wr.find((i) => i.value === t);
  return n && n.dash !== "none" ? n.dash : void 0;
}, $r = [
  { value: "vertical", label: "Vertical", x1: "0", y1: "0", x2: "0", y2: "1" },
  { value: "horizontal", label: "Horizontal", x1: "0", y1: "0", x2: "1", y2: "0" },
  { value: "diagonal", label: "Diagonal", x1: "0", y1: "0", x2: "1", y2: "1" }
], Wn = (t, n) => {
  const i = (Number(n) || 70) / 100;
  return [
    { offset: "0%", stopOpacity: String(Math.min(1, i + 0.2)) },
    { offset: "100%", stopOpacity: String(i) }
  ];
}, zn = ["bottom", "top", "none"], Bn = ({ legendPosition: t, onChange: n, theme: i }) => {
  const a = (r, f) => ({
    border: `1px solid ${r === t ? i.primary : i.border}`,
    background: r === t ? i.select : i.headerSubtleBg || i.hover,
    color: r === t ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "5px 10px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: zn.map((r) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => n(r), style: a(r), children: r.charAt(0).toUpperCase() + r.slice(1) }, r)) });
}, Ln = [
  { value: "none", label: "Natural" },
  { value: "value_desc", label: "Val ↓" },
  { value: "value_asc", label: "Val ↑" },
  { value: "alpha_asc", label: "A→Z" },
  { value: "alpha_desc", label: "Z→A" }
], En = (t, n) => {
  if (!Array.isArray(t) || n === "none" || !n) return t;
  const i = [...t];
  if (n === "alpha_asc") return i.sort((r, f) => String(r.name || "").localeCompare(String(f.name || "")));
  if (n === "alpha_desc") return i.sort((r, f) => String(f.name || "").localeCompare(String(r.name || "")));
  const a = (r) => (r.values || []).reduce((f, m) => f + (Number.isFinite(Number(m)) ? Number(m) : 0), 0);
  return n === "value_desc" ? i.sort((r, f) => a(f) - a(r)) : n === "value_asc" ? i.sort((r, f) => a(r) - a(f)) : i;
}, Rn = ({ model: t, maxCategories: n, maxSeries: i, theme: a }) => {
  const r = Array.isArray(t && t.categories) ? t.categories.length : 0, f = Array.isArray(t && t.series) ? t.series.length : 0, m = r >= n * 0.9, M = f >= i * 0.9, W = r >= n, R = f >= i;
  if (!m && !M) return null;
  const $ = (O) => ({
    display: "inline-flex",
    alignItems: "center",
    gap: "4px",
    padding: "3px 8px",
    borderRadius: a.radiusSm || "8px",
    fontSize: "10px",
    fontWeight: 700,
    background: O ? (a.danger || "#ef4444") + "22" : (a.warning || "#f59e0b") + "22",
    color: O ? a.danger || "#ef4444" : a.warning || "#ca8a04",
    border: `1px solid ${O ? a.danger || "#ef4444" : a.warning || "#f59e0b"}44`
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "5px", flexWrap: "wrap" }, children: [
    m && /* @__PURE__ */ e.jsxs("span", { style: $(W), children: [
      W ? "⚠ " : "○ ",
      r,
      "/",
      n,
      " categories"
    ] }),
    M && /* @__PURE__ */ e.jsxs("span", { style: $(R), children: [
      R ? "⚠ " : "○ ",
      f,
      "/",
      i,
      " series"
    ] })
  ] });
}, Dn = ({ series: t, lineDashStyles: n, onChange: i, seriesColors: a, paletteColors: r, getColorForIndex: f, theme: m, compactInputStyle: M }) => !Array.isArray(t) || t.length === 0 ? null : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: t.slice(0, 16).map((W, R) => {
  const $ = a && a[W.name] || f(R, m, r), O = n && n[W.name] || "solid";
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, children: [
    /* @__PURE__ */ e.jsx("span", { style: { width: "10px", height: "10px", borderRadius: "50%", background: $, flexShrink: 0 } }),
    /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: m.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: W.name || `Series ${R + 1}` }),
    /* @__PURE__ */ e.jsx(
      "select",
      {
        value: O,
        onChange: (J) => i({ ...n, [W.name]: J.target.value }),
        style: { ...M, width: "82px", flexShrink: 0 },
        children: Wr.map((J) => /* @__PURE__ */ e.jsx("option", { value: J.value, children: J.label }, J.value))
      }
    )
  ] }, W.name || R);
}) }), Nn = ({ customColors: t, onChange: n, theme: i, compactInputStyle: a }) => {
  const r = Array.isArray(t) && t.length > 0 ? t : ["#2563EB", "#F97316", "#0F766E", "#7C3AED", "#DC2626", "#0891B2"], f = (W, R) => {
    const $ = [...r];
    $[W] = R, n($);
  }, m = () => n([...r, "#888888"]), M = (W) => n(r.filter((R, $) => $ !== W));
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", alignItems: "center" }, children: [
    r.map((W, R) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", alignItems: "center", gap: "2px" }, children: [
      /* @__PURE__ */ e.jsx(
        "input",
        {
          type: "color",
          value: W,
          onChange: ($) => f(R, $.target.value),
          style: { width: "28px", height: "28px", padding: 0, border: `1px solid ${i.border}`, borderRadius: "4px", cursor: "pointer", background: "none" },
          title: `Color ${R + 1}`
        }
      ),
      r.length > 2 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => M(R), style: { border: "none", background: "none", color: i.textSec, cursor: "pointer", fontSize: "9px", padding: 0, lineHeight: 1 }, children: "✕" })
    ] }, R)),
    r.length < 12 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: m, style: { ...a, padding: "5px 8px", flexShrink: 0 }, children: "+" })
  ] }) });
}, ze = 16, Pn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "7", width: "4", height: "8", fill: t, opacity: "0.85", rx: "0.5" })
] }), In = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polyline", { points: "1,13 5,7 9,10 15,3", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "1", cy: "13", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "7", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "9", cy: "10", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "15", cy: "3", r: "1.4", fill: t })
] }), Hn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L8,1 A7,7 0 0,1 15,8 Z", fill: t, opacity: "0.9" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L15,8 A7,7 0 0,1 6,15 Z", fill: t, opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L6,15 A7,7 1 1,1 8,1 Z", fill: t, opacity: "0.3" })
] }), Fn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("circle", { cx: "3", cy: "12", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "7", cy: "5", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "12", cy: "9", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "13", cy: "3", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "11", r: "1.8", fill: t, opacity: "0.85" })
] }), On = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "8", width: "4", height: "7", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("polyline", { points: "1,9 5,4 9,7 15,2", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" })
] }), Yn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "4", width: "3", height: "7", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "5", y: "8", width: "3", height: "3", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "5", width: "3", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "13", y: "10", width: "3", height: "1", fill: t, opacity: "0.4", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("line", { x1: "0", y1: "15", x2: "16", y2: "15", stroke: t, strokeWidth: "0.7", opacity: "0.35" })
] }), Vn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "2", y: "6", width: "12", height: "5", fill: "none", stroke: t, strokeWidth: "1.2", rx: "1", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "8", y2: "11", stroke: t, strokeWidth: "1.2", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "5", y1: "3", x2: "5", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "11", y1: "3", x2: "11", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "3", x2: "6", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "3", x2: "12", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "13", x2: "6", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "13", x2: "12", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" })
] }), _n = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polygon", { points: "8,2 14,6 12,13 4,13 2,6", fill: "none", stroke: t, strokeWidth: "1", opacity: "0.35" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "8,5 11,7.5 9.5,11 6.5,11 5,7.5", fill: t, opacity: "0.65" })
] }), Xn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "1", width: "4", height: "4", fill: t, opacity: "0.9", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "1", width: "4", height: "4", fill: t, opacity: "0.4", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "1", width: "4", height: "4", fill: t, opacity: "0.7", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "6", width: "4", height: "4", fill: t, opacity: "0.3", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "6", width: "4", height: "4", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "6", width: "4", height: "4", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "11", width: "4", height: "4", fill: t, opacity: "0.6", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "11", width: "4", height: "4", fill: t, opacity: "0.95", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "11", width: "4", height: "4", fill: t, opacity: "0.35", rx: "0.5" })
] }), Gn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "5", height: "6", fill: t, opacity: "0.65", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "7", y: "4", width: "5", height: "11", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "1,9 4,5 9,5 7,9", fill: t, opacity: "0.45" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "7,4 12,4 12,9 7,9", fill: t, opacity: "0.55" })
] }), Tn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "4", y: "1", width: "8", height: "5", fill: t, opacity: "0.85", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "4", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "12", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" })
] }), Vr = [
  { name: "Bar", Icon: Pn, types: [{ value: "bar", label: "Bar / Column" }] },
  { name: "Line / Area", Icon: In, types: [{ value: "line", label: "Line" }, { value: "area", label: "Area" }, { value: "sparkline", label: "Sparkline" }] },
  { name: "Pie / Donut", Icon: Hn, types: [{ value: "pie", label: "Pie" }, { value: "donut", label: "Donut" }] },
  { name: "Scatter", Icon: Fn, types: [{ value: "scatter", label: "Scatter" }, { value: "bubble", label: "Bubble" }] },
  { name: "Combo", Icon: On, types: [{ value: "combo", label: "Combo" }] },
  { name: "Waterfall", Icon: Yn, types: [{ value: "waterfall", label: "Waterfall" }, { value: "range", label: "Range" }] },
  { name: "Statistical", Icon: Vn, types: [{ value: "histogram", label: "Histogram" }, { value: "boxplot", label: "Box Plot" }, { value: "funnel", label: "Funnel" }] },
  { name: "Radial", Icon: _n, types: [{ value: "radar", label: "Radar" }, { value: "nightingale", label: "Nightingale" }] },
  { name: "Heatmap", Icon: Xn, types: [{ value: "heatmap", label: "Heatmap" }] },
  { name: "3D", Icon: Gn, types: [{ value: "bar3d", label: "3D Bar" }, { value: "line3d", label: "3D Line" }, { value: "scatter3d", label: "3D Scatter" }] },
  { name: "Hierarchical", Icon: Tn, types: [{ value: "icicle", label: "Icicle" }, { value: "sunburst", label: "Sunburst" }, { value: "sankey", label: "Sankey" }], hierarchyOnly: !0 }
], Un = ({ chartType: t, onChange: n, theme: i, includeHierarchyCharts: a = !0 }) => {
  const [r, f] = v.useState(null), m = a ? Vr : Vr.filter((M) => !M.hierarchyOnly);
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "3px" }, children: m.map(({ name: M, Icon: W, types: R }) => {
    const $ = R.some((_) => _.value === t), O = R.find((_) => _.value === t), J = R.length === 1, K = r === M || $, s = $ ? i.primary : i.textSec || i.text;
    return /* @__PURE__ */ e.jsxs("div", { children: [
      /* @__PURE__ */ e.jsxs(
        "button",
        {
          type: "button",
          "aria-expanded": J ? void 0 : K,
          onClick: () => {
            J ? n(R[0].value) : f(K && !$ ? null : M);
          },
          style: {
            width: "100%",
            display: "flex",
            alignItems: "center",
            gap: "7px",
            padding: "5px 8px",
            border: `1px solid ${$ ? i.primary : i.border}`,
            background: $ ? i.select : i.headerSubtleBg || i.hover,
            color: $ ? i.primary : i.text,
            borderRadius: i.radiusSm || "8px",
            fontSize: "11px",
            fontWeight: $ ? 700 : 600,
            cursor: "pointer",
            textAlign: "left"
          },
          children: [
            /* @__PURE__ */ e.jsx(W, { color: s }),
            /* @__PURE__ */ e.jsx("span", { style: { flex: 1 }, children: O ? O.label : M }),
            !J && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "8px", color: s, transform: K ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.12s", flexShrink: 0 }, children: "▼" })
          ]
        }
      ),
      !J && K && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "3px", padding: "4px 0 2px 24px" }, children: R.map((_) => /* @__PURE__ */ e.jsx(
        "button",
        {
          type: "button",
          "aria-pressed": t === _.value,
          onClick: () => {
            n(_.value), f(null);
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
    ] }, M);
  }) });
}, Kn = ({ line: t, onUpdate: n, onRemove: i, exportButtonStyle: a, compactInputStyle: r, theme: f }) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center", flexWrap: "wrap" }, children: [
  /* @__PURE__ */ e.jsx(
    "button",
    {
      type: "button",
      onClick: () => n({ ...t, orient: t.orient === "v" ? "h" : "v" }),
      style: { ...a, padding: "4px 7px", flexShrink: 0, fontWeight: 800, color: t.orient === "v" ? f.primary : f.textSec },
      title: t.orient === "v" ? "Vertical (click for horizontal)" : "Horizontal (click for vertical)",
      children: t.orient === "v" ? "|" : "—"
    }
  ),
  /* @__PURE__ */ e.jsx(
    "input",
    {
      type: "number",
      value: t.value,
      onChange: (m) => n({ ...t, value: m.target.value }),
      placeholder: t.orient === "v" ? "Cat index" : "Value",
      style: { ...r, width: "60px", flexShrink: 0 }
    }
  ),
  /* @__PURE__ */ e.jsx(
    "input",
    {
      type: "text",
      value: t.label,
      onChange: (m) => n({ ...t, label: m.target.value }),
      placeholder: "Label",
      style: { ...r, flex: 1, minWidth: 0 }
    }
  ),
  /* @__PURE__ */ e.jsx(
    "input",
    {
      type: "color",
      value: t.color || f.primary || "#2563EB",
      onChange: (m) => n({ ...t, color: m.target.value }),
      style: { width: "28px", height: "28px", padding: 0, border: `1px solid ${f.border}`, borderRadius: "4px", cursor: "pointer", background: "none", flexShrink: 0 },
      title: "Line color"
    }
  ),
  /* @__PURE__ */ e.jsx(
    "select",
    {
      value: t.dash || "dashed",
      onChange: (m) => n({ ...t, dash: m.target.value }),
      style: { ...r, width: "74px", flexShrink: 0 },
      title: "Line style",
      children: Wr.map((m) => /* @__PURE__ */ e.jsx("option", { value: m.value, children: m.label }, m.value))
    }
  ),
  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: i, style: { ...a, padding: "4px 7px", color: f.danger || "#ef4444", flexShrink: 0 }, children: "✕" })
] }), _r = (t, n = 430, i, a) => {
  const r = Number(t), f = Number(n), m = Number.isFinite(r) ? r : Number.isFinite(f) ? f : 430;
  return Math.max(i, Math.min(a, Math.floor(m)));
}, Zn = (t) => {
  const n = t.map(($, O) => [O, Number($)]).filter(([, $]) => Number.isFinite($));
  if (n.length < 2) return null;
  const i = n.length, a = n.reduce(($, [O]) => $ + O, 0), r = n.reduce(($, [, O]) => $ + O, 0), f = n.reduce(($, [O, J]) => $ + O * J, 0), m = n.reduce(($, [O]) => $ + O * O, 0), M = i * m - a * a;
  if (M === 0) return null;
  const W = (i * f - a * r) / M, R = (r - W * a) / i;
  return t.map(($, O) => W * O + R);
}, qn = (t, n) => {
  const i = Math.max(2, Math.floor(Number(n) || 3));
  return t.map((a, r) => {
    const f = t.slice(Math.max(0, r - i + 1), r + 1).map(Number).filter(Number.isFinite);
    return f.length > 0 ? f.reduce((m, M) => m + M, 0) / f.length : null;
  });
}, Zr = (t) => Array.isArray(t && t.series) && t.series.length > 1, gr = (t, n) => {
  if (typeof window > "u" || !t) return;
  const i = window.URL.createObjectURL(t), a = document.createElement("a");
  a.href = i, a.download = n, document.body.appendChild(a), a.click(), document.body.removeChild(a), window.URL.revokeObjectURL(i);
}, yr = (t = {}) => ({
  title: String(t.title || "").trim(),
  legendPosition: t.legendPosition === "top" ? "top" : "bottom",
  legendItems: Array.isArray(t.legendItems) ? t.legendItems.filter((n) => n && n.label) : [],
  background: t.background || "#ffffff",
  textColor: t.textColor || "#111827",
  secondaryTextColor: t.secondaryTextColor || "#4b5563"
}), Qn = (t, n, i, a) => {
  const r = t ? Array.from(t.querySelectorAll('[data-chart-export-legend-item="true"]')).map((f) => ({
    label: f.getAttribute("data-chart-export-label") || f.textContent || "",
    color: f.getAttribute("data-chart-export-color") || a.primary || "#2563eb",
    hidden: f.getAttribute("data-chart-export-hidden") === "true"
  })).filter((f) => f.label) : [];
  return yr({
    title: n,
    legendPosition: i,
    legendItems: r,
    background: a.chartBg || a.surfaceBg || a.background || "#ffffff",
    textColor: a.text || "#111827",
    secondaryTextColor: a.textSec || a.text || "#4b5563"
  });
}, qr = (t, n, i) => {
  const a = [];
  let r = [], f = 0;
  return t.forEach((m) => {
    const M = Math.max(48, i(m.label) + 30);
    r.length > 0 && f + M > Math.max(80, n - 24) && (a.push(r), r = [], f = 0), r.push({ ...m, width: M }), f += M;
  }), r.length > 0 && a.push(r), a;
}, hr = (t) => {
  const n = String(t ?? ""), i = typeof t == "string" && /^[=+\-@]/.test(n) ? `'${n}` : n;
  return i.includes(",") || i.includes('"') || i.includes(`
`) ? `"${i.replace(/"/g, '""')}"` : i;
}, Jn = (t, n, i) => {
  if (!t) return;
  const a = Array.isArray(t.categories) ? t.categories : [], r = Array.isArray(t.series) ? t.series : [];
  if (a.length === 0 || r.length === 0) return;
  const f = ["Category", ...r.map(($) => $.name)].map(hr).join(","), m = a.map(($, O) => [$, ...r.map((J) => J.values[O] ?? "")].map(hr).join(",")), M = yr(i), W = M.legendItems.length > 0 ? M.legendItems.map(($) => $.label) : r.map(($) => $.name).filter(Boolean), R = [];
  M.title && R.push(["Title", M.title].map(hr).join(",")), W.length > 0 && R.push(["Legend", ...W].map(hr).join(",")), R.length > 0 && R.push(""), gr(new Blob([`\uFEFF${[...R, f, ...m].join(`
`)}`], { type: "text/csv;charset=utf-8" }), `${n}.csv`);
}, es = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), ts = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d"]), rs = xt.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-2d" */
  "./ECharts2dRenderer-DB1VaBAJ.js"
)), ns = xt.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-3d" */
  "./ECharts3dRenderer-DCkX0AIG.js"
)), er = ({ chartType: t, echartsRef: n, option: i }) => {
  const a = ts.has(t) ? ns : rs;
  return /* @__PURE__ */ e.jsx(xt.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { "aria-label": "Loading chart", style: { width: "100%", height: "100%" } }), children: /* @__PURE__ */ e.jsx(a, { echartsRef: n, option: i }) });
}, Se = ({ message: t, theme: n, chartHeight: i = nr }) => /* @__PURE__ */ e.jsx("div", { style: {
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
}, children: t }), ss = ({ model: t, theme: n }) => {
  const i = v.useMemo(() => {
    const r = [];
    return (t && t.series ? t.series : []).forEach((f) => {
      (f.values || []).forEach((m) => {
        typeof m == "number" && Number.isFinite(m) && r.push(m);
      });
    }), r.length === 0 && t && Array.isArray(t.icicleNodes) && t.icicleNodes.forEach((f) => {
      const m = Number(f && f.value);
      Number.isFinite(m) && r.push(m);
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
      Ir(i.min)
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      "Max ",
      Ir(i.max)
    ] })
  ] });
}, jt = ({ items: t, theme: n, hiddenSet: i, onToggle: a, position: r = "bottom", onElementSelect: f }) => {
  if (r === "none" || !Array.isArray(t) || t.length === 0) return null;
  const m = typeof a == "function" && i;
  return /* @__PURE__ */ e.jsx("div", { "data-chart-export-legend": "true", style: {
    display: "flex",
    gap: "10px",
    flexWrap: "wrap",
    alignItems: "center",
    justifyContent: "flex-start",
    order: r === "top" ? -1 : 1,
    padding: r === "top" ? "0 0 6px 0" : "6px 0 0 0"
  }, children: t.map((M) => {
    const W = m && i.has(M.label);
    return /* @__PURE__ */ e.jsxs(
      "div",
      {
        "data-chart-export-legend-item": "true",
        "data-chart-export-label": M.label,
        "data-chart-export-color": W ? n.border : M.color,
        "data-chart-export-hidden": W ? "true" : "false",
        onClick: () => {
          m && a(M.label), typeof f == "function" && f("legend", M.label);
        },
        role: "button",
        tabIndex: 0,
        onKeyDown: (R) => {
          (R.key === "Enter" || R.key === " ") && (R.preventDefault(), m && a(M.label), typeof f == "function" && f("legend", M.label));
        },
        "aria-pressed": m ? !W : void 0,
        style: {
          display: "flex",
          alignItems: "center",
          gap: "6px",
          fontSize: "11px",
          color: n.textSec,
          cursor: "pointer",
          opacity: W ? 0.35 : 1,
          textDecoration: W ? "line-through" : "none",
          userSelect: "none"
        },
        children: [
          /* @__PURE__ */ e.jsx("span", { style: { width: "10px", height: "10px", borderRadius: "999px", background: W ? n.border : M.color, display: "inline-block" } }),
          /* @__PURE__ */ e.jsx("span", { children: M.label })
        ]
      },
      M.label
    );
  }) });
}, os = (t, n, i, a = 10) => {
  const f = (Array.isArray(t) ? t.map((T) => Number.isFinite(Number(T)) ? Number(T) : null) : []).map((T, V) => T === null ? null : { value: T, index: V }).filter(Boolean);
  if (f.length === 0)
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
  const m = Math.min(...f.map((T) => T.value)), M = Math.max(...f.map((T) => T.value)), W = Math.max(1, M - m), R = f.length > 1 ? (n - a * 2) / (f.length - 1) : 0, $ = f.map((T, V) => {
    const Y = a + V * R, F = (T.value - m) / W, U = i - a - F * Math.max(1, i - a * 2);
    return {
      ...T,
      x: Y,
      y: U
    };
  }), O = $.map((T, V) => `${V === 0 ? "M" : "L"} ${T.x.toFixed(2)} ${T.y.toFixed(2)}`).join(" "), J = $.length > 1 ? `${O} L ${$[$.length - 1].x.toFixed(2)} ${(i - a).toFixed(2)} L ${$[0].x.toFixed(2)} ${(i - a).toFixed(2)} Z` : "", K = $.find((T) => T.value === m) || $[0], s = $.find((T) => T.value === M) || $[0], _ = $[$.length - 1];
  return {
    linePath: O,
    areaPath: J,
    currentPoint: _,
    minPoint: K,
    maxPoint: s,
    minValue: m,
    maxValue: M,
    firstValue: $[0] ? $[0].value : null,
    lastValue: _ ? _.value : null
  };
}, is = ({
  model: t,
  theme: n,
  chartHeight: i,
  paletteColors: a,
  valueFormat: r,
  referenceLines: f = []
}) => {
  const m = Array.isArray(t && t.series) ? t.series : [], M = Array.isArray(t && t.categories) ? t.categories : [];
  if (m.length === 0)
    return /* @__PURE__ */ e.jsx(Se, { message: t && t.emptyMessage ? t.emptyMessage : "No sparkline data available.", theme: n, chartHeight: i });
  const W = 110, R = 240, $ = 64;
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
      children: m.map((O, J) => {
        const K = Ce(J, n, a), s = os(O.values || [], R, $, 10), _ = Array.isArray(O.values) && O.values.length > 1 ? Number(O.values[O.values.length - 2]) : null, T = s.lastValue, V = Number.isFinite(_) && Number.isFinite(T) ? T - _ : null, Y = V === null ? n.textSec : V >= 0 ? n.isDark ? "#7DD3A7" : "#2E7D5B" : n.isDark ? "#F7A3A3" : "#B45353";
        return /* @__PURE__ */ e.jsxs(
          "div",
          {
            "data-chart-sparkline-card": O.id || O.name || `spark-${J}`,
            style: {
              minHeight: `${W}px`,
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
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: n.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: O.name || `Series ${J + 1}` }),
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: n.textSec }, children: M.length > 0 ? `${M.length} points` : "Trend" })
                ] }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", alignItems: "flex-end", gap: "2px", flexShrink: 0 }, children: [
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: T === null ? "—" : Ct(T, r) }),
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: Y }, children: V === null ? "No delta" : `${V >= 0 ? "+" : ""}${Ct(V, r)}` })
                ] })
              ] }),
              /* @__PURE__ */ e.jsxs(
                "svg",
                {
                  viewBox: `0 0 ${R} ${$}`,
                  style: { width: "100%", height: "64px", display: "block" },
                  "aria-label": `${O.name || `Series ${J + 1}`} sparkline`,
                  children: [
                    s.areaPath ? /* @__PURE__ */ e.jsx("path", { d: s.areaPath, fill: K, opacity: "0.12" }) : null,
                    s.linePath ? /* @__PURE__ */ e.jsx(
                      "path",
                      {
                        d: s.linePath,
                        fill: "none",
                        stroke: K,
                        strokeWidth: "2.5",
                        strokeLinecap: "round",
                        strokeLinejoin: "round"
                      }
                    ) : null,
                    s.minPoint ? /* @__PURE__ */ e.jsx("circle", { cx: s.minPoint.x, cy: s.minPoint.y, r: "2.6", fill: n.surfaceBg || n.background || "#fff", stroke: K, strokeWidth: "1.4", opacity: "0.7" }) : null,
                    s.maxPoint ? /* @__PURE__ */ e.jsx("circle", { cx: s.maxPoint.x, cy: s.maxPoint.y, r: "2.8", fill: n.surfaceBg || n.background || "#fff", stroke: K, strokeWidth: "1.6", opacity: "0.9" }) : null,
                    s.currentPoint ? /* @__PURE__ */ e.jsx("circle", { cx: s.currentPoint.x, cy: s.currentPoint.y, r: "3.2", fill: K }) : null,
                    f.map((F, U) => {
                      const de = Number(F.value);
                      if (!Number.isFinite(de) || F.orient === "v" || s.minValue === null || s.maxValue === null) return null;
                      const ne = s.maxValue - s.minValue || 1, ae = $ - 10 - (de - s.minValue) / ne * ($ - 20);
                      if (ae < 0 || ae > $) return null;
                      const ue = F.color || n.primary;
                      return /* @__PURE__ */ e.jsxs("g", { children: [
                        /* @__PURE__ */ e.jsx("line", { x1: 10, y1: ae, x2: R - 10, y2: ae, stroke: ue, strokeWidth: "1", strokeDasharray: "4 2", style: { pointerEvents: "none" } }),
                        F.label ? /* @__PURE__ */ e.jsx("text", { x: R - 11, y: ae - 2, textAnchor: "end", fontSize: "8", fontWeight: "700", fill: ue, style: { pointerEvents: "none" }, children: F.label }) : null
                      ] }, `spark-rl-${U}`);
                    })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", fontSize: "10px", color: n.textSec }, children: [
                /* @__PURE__ */ e.jsx("span", { children: M[0] ? he(String(M[0]), 14) : "Start" }),
                /* @__PURE__ */ e.jsxs("span", { children: [
                  "Min ",
                  s.minValue === null ? "—" : Ct(s.minValue, r),
                  " · ",
                  "Max ",
                  s.maxValue === null ? "—" : Ct(s.maxValue, r)
                ] }),
                /* @__PURE__ */ e.jsx("span", { children: M.length > 0 ? he(String(M[M.length - 1]), 14) : "End" })
              ] })
            ]
          },
          O.id || O.name || `spark-${J}`
        );
      })
    }
  );
}, as = ({
  model: t,
  chartType: n,
  barLayout: i,
  axisMode: a,
  theme: r,
  chartHeight: f = nr,
  showLegend: m = !0,
  showDataLabels: M = !1,
  onCategoryActivate: W = null,
  svgRef: R = null,
  echartsRef: $ = null,
  colorPalette: O = "default",
  valueFormat: J = "auto",
  yAxisTitle: K = "",
  yAxisMin: s = "",
  yAxisMax: _ = "",
  referenceLines: T = [],
  labelAngle: V = 0,
  chartTitle: Y = "",
  onTitleChange: F = null,
  tickCount: U = 5,
  markerShape: de = "circle",
  markerSize: ne = 4,
  showGradient: ae = !1,
  seriesColors: ue = {},
  showAnimations: nt = !1,
  showCalloutLabels: be = !1,
  subtitle: st = "",
  onReferenceLinesChange: _e = null,
  showTrendline: Ht = !1,
  trendlineMode: ot = "linear",
  trendlinePeriod: qe = 3,
  // new props
  legendPosition: Oe = "bottom",
  labelPosition: Me = "outside",
  decimalPlaces: Vt = -1,
  gradientDirection: sr = "vertical",
  gradientOpacity: et = 70,
  trendlineColor: _t = "",
  trendlineWidth: Xt = 2,
  seriesSort: Qe = "none",
  lineDashStyles: pt = {},
  yAxisMinRight: Gt = "",
  yAxisMaxRight: B = "",
  customPaletteColors: $t = [],
  onElementSelect: xe = null
}) => {
  const je = O === "custom" && $t.length >= 2 ? $t : Xr(O), P = (o) => {
    if (Vt >= 0 && typeof o == "number" && Number.isFinite(o)) {
      const c = Ct(o, J);
      if (!/[KMB%$]/.test(c) || J === "number")
        return o.toFixed(Vt);
    }
    return Ct(o, J);
  };
  v.useMemo(() => !t || !Array.isArray(t.series) || Qe === "none" ? t : { ...t, series: En(t.series, Qe) }, [t, Qe]);
  const Ie = n === "bar" && i === "stacked" && Array.isArray(t.stackedGroups) && t.stackedGroups.length > 0, Wt = n === "bar" && i === "stacked" && !Ie && on(t), ft = n === "area" && i === "stacked" && Zr(t), it = n === "bar" && a === "horizontal", Ne = i === "stacked100", ke = Number.isFinite(Number(V)) ? Number(V) : 0, z = f, ve = (o, c) => ue && ue[o] || Ce(c, r, je), zt = (o, c, l, u, p = {}) => {
    if (de === "none") return null;
    const h = ne || 4, k = r.surfaceBg || r.background || "#fff", g = { fill: l, stroke: k, strokeWidth: "1.5", opacity: "0.82", ...u, ...p };
    switch (de) {
      case "square":
        return /* @__PURE__ */ e.jsx("rect", { x: o - h, y: c - h, width: h * 2, height: h * 2, rx: "2", ...g });
      case "diamond":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${c - h} ${o + h},${c} ${o},${c + h} ${o - h},${c}`, ...g });
      case "cross":
        return /* @__PURE__ */ e.jsx("path", { d: `M${o - h},${c} L${o + h},${c} M${o},${c - h} L${o},${c + h}`, fill: "none", stroke: l, strokeWidth: "2.5", strokeLinecap: "round", ...u, ...p });
      case "triangle":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${c - h} ${o + h},${c + h * 0.85} ${o - h},${c + h * 0.85}`, ...g });
      default:
        return /* @__PURE__ */ e.jsx("circle", { cx: o, cy: c, r: h, ...g });
    }
  }, me = v.useMemo(() => !Ne || !t || !Array.isArray(t.categories) ? null : t.categories.map((o, c) => {
    let l = 0;
    return (t.series || []).forEach((u) => {
      const p = u.values && u.values[c];
      typeof p == "number" && Number.isFinite(p) && (l += Math.abs(p));
    }), l || 1;
  }), [Ne, t]), ht = Array.isArray(t && t.icicleNodes) ? t.icicleNodes : [], at = Math.max(1, Number(t && t.icicleDepth) || 1), He = v.useRef(null), [kt, or] = v.useState(Ar), Z = Math.max(Ar, Number.isFinite(kt) ? Math.floor(kt) : Ar);
  v.useEffect(() => {
    const o = He.current;
    if (!o) return;
    const c = () => {
      const u = o.clientWidth;
      u > 0 && or(u);
    };
    if (c(), typeof ResizeObserver > "u") return;
    const l = new ResizeObserver(c);
    return l.observe(o), () => l.disconnect();
  }, []);
  const [ee, Bt] = v.useState(/* @__PURE__ */ new Set()), Ke = v.useCallback((o) => {
    Bt((c) => {
      const l = new Set(c);
      return l.has(o) ? l.delete(o) : l.add(o), l;
    });
  }, []), [$e, Xe] = v.useState(null), Ge = v.useCallback((o) => {
    const c = o.target, l = c.getAttribute("data-tip-cat");
    if (!l) {
      Xe((h) => h && null);
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
    if (!xe) return;
    let c = o.target;
    for (; c && c !== o.currentTarget; ) {
      const l = c.getAttribute && c.getAttribute("data-chart-element-type");
      if (l) {
        xe(l, c.getAttribute("data-chart-element-id") || "");
        return;
      }
      c = c.parentElement;
    }
  }, [xe]), Pe = $e ? /* @__PURE__ */ e.jsxs("div", { style: {
    position: "absolute",
    left: $e.x + 14 + 160 > Z ? Math.max(0, $e.x - 174) : $e.x + 14,
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
  ] }) : null, [Le, Ft] = v.useState(1), [tt, Ee] = v.useState({ x: 0, y: 0 }), Ze = v.useRef(null), [yt, bt] = v.useState(null), rt = v.useCallback((o) => {
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
    const c = R && R.current;
    if (c && typeof c.getScreenCTM == "function") {
      const l = c.getScreenCTM();
      if (l) {
        const u = c.createSVGPoint();
        u.x = o.clientX, u.y = o.clientY;
        const p = u.matrixTransform(l.inverse());
        bt({ x: p.x, y: p.y });
      }
    }
  }, [Ge, ct]), wt = v.useCallback(() => {
    gt(), Fe(), bt(null);
  }, [gt, Fe]), We = {
    onMouseMove: dt,
    onMouseLeave: wt,
    onWheel: rt,
    onMouseDown: Te,
    onMouseUp: Fe,
    onDoubleClick: Lt
  }, te = Le > 1 ? `scale(${Le}) translate(${tt.x / Le}px, ${tt.y / Le}px)` : void 0, Be = Le > 1 ? { overflow: "hidden", cursor: "grab" } : void 0, Ue = Le > 1 ? { transform: te, transformOrigin: "center center" } : void 0, we = yt && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsxs("svg", { style: { position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none", zIndex: 5 }, viewBox: `0 0 ${Z} ${z}`, preserveAspectRatio: "xMidYMid meet", children: [
    /* @__PURE__ */ e.jsx("line", { x1: yt.x, y1: 0, x2: yt.x, y2: z, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" }),
    /* @__PURE__ */ e.jsx("line", { x1: 0, y1: yt.y, x2: Z, y2: yt.y, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" })
  ] }) : null, Ye = Le > 1.05 ? /* @__PURE__ */ e.jsxs("div", { style: { position: "absolute", top: 4, right: 4, zIndex: 12, background: r.surfaceBg || "#fff", border: `1px solid ${r.border}`, borderRadius: r.radiusSm || "8px", padding: "2px 7px", fontSize: "10px", fontWeight: 700, color: r.textSec, pointerEvents: "auto", cursor: "pointer" }, onClick: Lt, title: "Reset zoom (or double-click chart)", children: [
    Math.round(Le * 100),
    "% ✕"
  ] }) : null, [Je, ir] = v.useState(!1), [vt, lr] = v.useState(""), Et = v.useRef(null), [Tt, Ut] = v.useState(null), Kt = v.useRef(null);
  v.useEffect(() => {
    const o = (l) => {
      if (!Kt.current) return;
      const { startX: u, startY: p, startPos: h, containerRect: k } = Kt.current, g = l.clientX - u, S = l.clientY - p, w = Math.max(0, Math.min(k.width - 40, h.x + g)), y = Math.max(0, Math.min(k.height - 20, h.y + S));
      Ut({ x: w, y });
    }, c = () => {
      Kt.current = null;
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
    const l = c.getBoundingClientRect(), u = Tt || {
      x: l.width / 2 - 40,
      y: 6
    };
    Kt.current = {
      startX: o.clientX,
      startY: o.clientY,
      startPos: u,
      containerRect: l
    };
  }, br = () => {
    typeof F == "function" && (lr(Y || ""), ir(!0));
  }, Dt = () => {
    typeof F == "function" && F(vt), ir(!1);
  };
  v.useEffect(() => {
    Je && Et.current && Et.current.focus();
  }, [Je]);
  const vr = Tt ? { left: `${Tt.x}px`, top: `${Tt.y}px` } : { left: "50%", top: "6px", transform: "translateX(-50%)" }, Re = Y || Je ? /* @__PURE__ */ e.jsx(
    "div",
    {
      onMouseDown: Rt,
      onDoubleClick: br,
      onClick: xe && !Je ? () => xe("title", "") : void 0,
      "data-chart-element-type": "title",
      style: {
        position: "absolute",
        ...vr,
        zIndex: 6,
        cursor: Je ? "text" : xe ? "pointer" : "grab",
        userSelect: Je ? "auto" : "none"
      },
      children: Je ? /* @__PURE__ */ e.jsx(
        "input",
        {
          ref: Et,
          type: "text",
          value: vt,
          onChange: (o) => lr(o.target.value),
          onBlur: Dt,
          onKeyDown: (o) => {
            o.key === "Enter" && Dt(), o.key === "Escape" && ir(!1);
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
      }, children: Y })
    }
  ) : null, x = v.useMemo(() => {
    if (n === "heatmap" || n === "icicle" || n === "sunburst" || n === "sankey" || n === "pie" || n === "donut" || n === "scatter" || n === "waterfall" || n === "combo" || n === "sparkline" || n === "bubble" || n === "radar" || n === "funnel" || n === "histogram" || es.has(n)) return null;
    const o = Ie ? (t.stackedGroups || []).reduce((E, ce) => {
      let ie = 0, se = 0;
      return (ce.segments || []).forEach((N) => {
        const re = N && typeof N.value == "number" && Number.isFinite(N.value) ? N.value : null;
        re !== null && (re >= 0 ? ie += re : se += re);
      }), E.push(ie, se), E;
    }, []) : Wt ? (t.categories || []).reduce((E, ce, ie) => {
      let se = 0, N = 0;
      return (t.series || []).forEach((re) => {
        const fe = re && Array.isArray(re.values) ? re.values[ie] : null;
        typeof fe != "number" || !Number.isFinite(fe) || (fe >= 0 ? se += fe : N += fe);
      }), E.push(se, N), E;
    }, []) : ft ? t.categories.reduce((E, ce, ie) => {
      let se = 0, N = 0;
      return (t.series || []).forEach((re) => {
        const fe = re && Array.isArray(re.values) ? re.values[ie] : null;
        typeof fe != "number" || !Number.isFinite(fe) || (fe >= 0 ? se += fe : N += fe);
      }), E.push(se, N), E;
    }, []) : (t.series || []).reduce((E, ce) => ((ce.values || []).forEach((ie) => {
      typeof ie == "number" && Number.isFinite(ie) && E.push(ie);
    }), E), []);
    if (o.length === 0) return null;
    const c = !Ie && n === "bar" && Array.isArray(t.categoryBands) && t.categoryBands.length === t.categories.length && t.categoryBands.some((E) => E && E.outerLabel);
    let l = Math.min(...o, 0), u = Math.max(...o, 0);
    if (Ne)
      l = 0, u = 100;
    else {
      if (l === u) {
        const E = Math.abs(u || 1) * 0.15 || 1;
        l -= E, u += E;
      }
      Number.isFinite(Number(s)) && s !== "" && (l = Number(s)), Number.isFinite(Number(_)) && _ !== "" && (u = Number(_)), l >= u && (u = l + 1);
    }
    const h = It(l, u, 5).map((E) => Ne ? `${Math.round(E)}%` : P(E)).reduce((E, ce) => Math.max(E, At(ce)), 0), k = Ie ? (t.stackedGroups || []).map((E) => he(E && E.label, 22)) : c ? (t.categoryBands || []).map((E) => he(E && E.innerLabel, 18)) : (t.categories || []).map((E) => he(E, 22)), g = c ? (t.categoryBands || []).map((E) => he(E && E.outerLabel, 18)) : [], S = k.reduce((E, ce) => Math.max(E, At(ce)), 0), w = g.reduce((E, ce) => Math.max(E, At(ce, 11)), 0), y = it ? Math.max(
      c ? 176 : 132,
      c ? w + S + 38 : S + 22
    ) : Math.max(66, h + 18), I = ke === 0 ? 40 : ke <= 30 ? 62 : ke <= 45 ? 80 : ke <= 60 ? 98 : 118, X = it ? { top: 24, right: 18, bottom: 42, left: y } : { top: 24, right: 18, bottom: c ? 118 : I, left: y }, A = Z - X.left - X.right, j = z - X.top - X.bottom, L = n === "bar" ? 0 : Math.min(Math.max(0, l), u), D = u - l || 1, H = it ? null : ((E) => X.top + (u - E) / D * j), G = it ? ((E) => X.left + (E - l) / D * A) : null, le = H ? H(L) : null, b = G ? G(L) : null, q = Math.max(t.categories.length, 1), oe = (it ? j : A) / q, C = Math.max(10, oe * 0.72);
    return {
      margin: X,
      plotWidth: A,
      plotHeight: j,
      baselineY: le,
      baselineX: b,
      scaleY: H,
      scaleX: G,
      step: oe,
      groupWidth: C,
      minValue: l,
      maxValue: u,
      hasHierarchicalBands: c,
      horizontalBarMode: it
    };
  }, [a, t, n, ft, Ie, Wt, it, z, Z, Ne, s, _, ke, P]), Nt = v.useMemo(() => {
    if (!x || x.horizontalBarMode || x.hasHierarchicalBands)
      return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const o = Array.isArray(t && t.categories) ? t.categories : [];
    if (o.length <= 1) return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const c = x.step < 42 ? 12 : x.step < 70 ? 16 : 20, l = o.reduce((w, y) => Math.max(w, At(he(y, c), 11)), 0), u = Math.abs(ke) * (Math.PI / 180), p = ke === 0 ? l : Math.abs(Math.cos(u)) * l + Math.abs(Math.sin(u)) * 12, h = Math.max(1, Math.ceil((p + 12) / Math.max(x.step, 1)));
    if (h === 1) return { visibleIndexes: null, maxChars: c, stride: h };
    const k = /* @__PURE__ */ new Set();
    for (let w = 0; w < o.length; w += h) k.add(w);
    const g = o.length - 1, S = Math.floor(g / h) * h;
    return g - S >= Math.ceil(h * 0.6) && k.add(g), { visibleIndexes: k, maxChars: c, stride: h };
  }, [ke, x, t]);
  if (n === "sparkline")
    return /* @__PURE__ */ e.jsxs(
      "div",
      {
        ref: He,
        "data-chart-sparkline-surface": "true",
        style: {
          position: "relative",
          minHeight: `${z}px`,
          height: `${z}px`,
          minWidth: 0
        },
        children: [
          /* @__PURE__ */ e.jsx(
            is,
            {
              model: t,
              theme: r,
              chartHeight: z,
              paletteColors: je,
              valueFormat: J,
              referenceLines: T
            }
          ),
          Re
        ]
      }
    );
  if (n === "icicle") {
    if (ht.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Icicle charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: z });
    const o = z / at, c = an(ht, 0, Z, 0, o, []);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
      /* @__PURE__ */ e.jsx(
        "svg",
        {
          ref: R,
          viewBox: `0 0 ${Z} ${z}`,
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
            const p = Ce(Math.max(0, l.depth - 1), r, je), h = l.width >= 54;
            return /* @__PURE__ */ e.jsxs(
              "g",
              {
                onClick: () => {
                  typeof W == "function" && W(l);
                },
                style: typeof W == "function" ? { cursor: "pointer" } : void 0,
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
                  h ? /* @__PURE__ */ e.jsx(
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
      we,
      Pe,
      Ye
    ] }) });
  }
  if (n === "sunburst") {
    if (ht.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Sunburst charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: z });
    const o = Math.min(Z, z) / 2 - 18, c = Z / 2, l = z / 2, u = o / Math.max(at, 1), p = ln(ht, -Math.PI / 2, Math.PI * 3 / 2, 1, u, []);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
      /* @__PURE__ */ e.jsx(
        "svg",
        {
          ref: R,
          viewBox: `0 0 ${Z} ${z}`,
          preserveAspectRatio: "xMidYMid meet",
          style: {
            width: "100%",
            height: "auto",
            display: "block",
            maxWidth: "100%"
          },
          role: "img",
          "aria-label": t.title || "Sunburst chart",
          children: p.map((h, k) => {
            const g = Ce(Math.max(0, h.depth - 1), r, je), S = Dr(c, l, h.innerRadius, h.outerRadius, h.startAngle, h.endAngle), w = h.startAngle + (h.endAngle - h.startAngle) / 2, y = h.innerRadius + (h.outerRadius - h.innerRadius) / 2, I = ar(c, l, y, w), X = h.endAngle - h.startAngle, A = X > 0.2 && h.outerRadius - h.innerRadius >= 18;
            return /* @__PURE__ */ e.jsxs(
              "g",
              {
                onClick: () => {
                  typeof W == "function" && W(h);
                },
                style: typeof W == "function" ? { cursor: "pointer" } : void 0,
                children: [
                  /* @__PURE__ */ e.jsx(
                    "path",
                    {
                      d: S,
                      fill: g,
                      opacity: "0.88",
                      stroke: r.surfaceBg || r.background || "#fff",
                      strokeWidth: "1",
                      "data-tip-cat": h.label || "",
                      "data-tip-val": typeof h.value == "number" ? P(h.value) : "",
                      "data-tip-color": g
                    }
                  ),
                  A ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: I.x,
                      y: I.y,
                      textAnchor: "middle",
                      fontSize: "10",
                      fontWeight: "700",
                      fill: "#fff",
                      style: { pointerEvents: "none" },
                      children: he(h.label, Math.max(5, Math.floor(X * 24)))
                    }
                  ) : null
                ]
              },
              `sunburst-node-${h.rowPath || k}`
            );
          })
        }
      ),
      Re,
      we,
      Pe,
      Ye
    ] }) });
  }
  if (n === "sankey") {
    const o = Array.isArray(t && t.sankeyNodes) ? t.sankeyNodes : [], c = Array.isArray(t && t.sankeyLinks) ? t.sankeyLinks : [];
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Sankey charts need expanded hierarchical rows with child flows.", theme: r, chartHeight: z });
    const l = cn(o, c, Z, z), u = l.nodes.length > 0 ? Math.max(...l.nodes.map((p) => p.x)) : 0;
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
      /* @__PURE__ */ e.jsxs(
        "svg",
        {
          ref: R,
          viewBox: `0 0 ${Z} ${z}`,
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
            l.links.map((p, h) => {
              const k = p.sourceNode.x + p.sourceNode.width, g = p.targetNode.x, S = k + (g - k) / 2, w = [
                `M ${k} ${p.sourceY}`,
                `C ${S} ${p.sourceY}, ${S} ${p.targetY}, ${g} ${p.targetY}`
              ].join(" ");
              return /* @__PURE__ */ e.jsx(
                "path",
                {
                  d: w,
                  fill: "none",
                  stroke: r.primary,
                  strokeOpacity: "0.22",
                  strokeWidth: p.thickness,
                  "data-tip-cat": `${p.sourceNode.label || ""} → ${p.targetNode.label || ""}`,
                  "data-tip-val": typeof p.value == "number" ? P(p.value) : ""
                },
                `sankey-link-${p.source}-${p.target}-${h}`
              );
            }),
            l.nodes.map((p, h) => {
              const k = Ce(Math.max(0, p.depth - 1), r, je);
              return /* @__PURE__ */ e.jsxs(
                "g",
                {
                  onClick: () => {
                    typeof W == "function" && W(p);
                  },
                  style: typeof W == "function" ? { cursor: "pointer" } : void 0,
                  children: [
                    /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: p.x,
                        y: p.y,
                        width: p.width,
                        height: p.height,
                        rx: "4",
                        fill: k,
                        opacity: "0.92",
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "1",
                        "data-tip-cat": p.label || "",
                        "data-tip-val": typeof p.value == "number" ? P(p.value) : "",
                        "data-tip-color": k
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
                `sankey-node-${p.rowPath || h}`
              );
            })
          ]
        }
      ),
      Re,
      we,
      Pe,
      Ye
    ] }) });
  }
  if (n === "pie" || n === "donut") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, c = Array.isArray(t && t.categories) ? t.categories : [];
    if (!o || c.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Pie charts need categories and at least one numeric measure.", theme: r, chartHeight: z });
    const l = c.map((A, j) => {
      const L = o.values[j], D = typeof L == "number" && Number.isFinite(L) ? L : 0;
      return { category: A, value: Math.abs(D), originalValue: D, index: j };
    }).filter((A) => A.value > 0 && !ee.has(A.category)), u = l.reduce((A, j) => A + j.value, 0);
    if (u === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "All values are zero — nothing to display.", theme: r, chartHeight: z });
    const p = n === "donut", h = Math.min(Z, z) / 2 - 24, k = p ? h * 0.52 : 0, g = Z / 2, S = z / 2, w = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    let y = -Math.PI / 2;
    const I = l.map((A) => {
      const j = A.value / u, L = j * Math.PI * 2, D = y, H = y + L;
      return y = H, { ...A, startAngle: D, endAngle: H, fraction: j };
    }), X = I.map((A) => ({
      label: `${A.category} (${(A.fraction * 100).toFixed(1)}%)`,
      color: Ce(A.index, r, je)
    }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: R,
            viewBox: `0 0 ${Z} ${z}`,
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
              I.map((A) => {
                const j = Ce(A.index, r, je), L = Dr(g, S, k, h, A.startAngle, A.endAngle), D = A.startAngle + (A.endAngle - A.startAngle) / 2, H = p ? k + (h - k) / 2 : h * 0.62, G = ar(g, S, H, D), le = A.endAngle - A.startAngle > 0.3, b = ar(g, S, h + (be ? 26 : 14), D), q = A.endAngle - A.startAngle > 0.15, oe = w[A.index] || null;
                return /* @__PURE__ */ e.jsxs(
                  "g",
                  {
                    onClick: () => {
                      typeof W == "function" && oe && W(oe);
                    },
                    style: typeof W == "function" && oe ? { cursor: "pointer" } : void 0,
                    children: [
                      /* @__PURE__ */ e.jsx(
                        "path",
                        {
                          d: L,
                          fill: j,
                          opacity: "0.9",
                          stroke: r.surfaceBg || r.background || "#fff",
                          strokeWidth: "2",
                          "data-tip-cat": A.category,
                          "data-tip-ser": o.name,
                          "data-tip-val": `${P(A.originalValue)} (${(A.fraction * 100).toFixed(1)}%)`,
                          "data-tip-color": j
                        }
                      ),
                      le ? /* @__PURE__ */ e.jsxs(
                        "text",
                        {
                          x: G.x,
                          y: G.y,
                          textAnchor: "middle",
                          dominantBaseline: "central",
                          fontSize: "10",
                          fontWeight: "700",
                          fill: "#fff",
                          style: { pointerEvents: "none" },
                          children: [
                            (A.fraction * 100).toFixed(1),
                            "%"
                          ]
                        }
                      ) : null,
                      (M || be) && q ? /* @__PURE__ */ e.jsxs("g", { children: [
                        be ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx(
                            "line",
                            {
                              x1: ar(g, S, h * 0.97, D).x,
                              y1: ar(g, S, h * 0.97, D).y,
                              x2: b.x,
                              y2: b.y,
                              stroke: j,
                              strokeWidth: "1",
                              style: { pointerEvents: "none" }
                            }
                          ),
                          /* @__PURE__ */ e.jsx(
                            "text",
                            {
                              x: D > Math.PI / 2 && D < 3 * Math.PI / 2 ? b.x - 4 : b.x + 4,
                              y: b.y + 4,
                              textAnchor: D > Math.PI / 2 && D < 3 * Math.PI / 2 ? "end" : "start",
                              fontSize: "9",
                              fontWeight: "700",
                              fill: r.textSec,
                              style: { pointerEvents: "none" },
                              children: he(A.category, 14)
                            }
                          )
                        ] }) : null,
                        M ? /* @__PURE__ */ e.jsx(
                          "text",
                          {
                            x: b.x,
                            y: be ? b.y + 14 : b.y,
                            textAnchor: D > Math.PI / 2 && D < 3 * Math.PI / 2 ? "end" : "start",
                            dominantBaseline: "central",
                            fontSize: "9",
                            fontWeight: "700",
                            fill: r.textSec,
                            style: { pointerEvents: "none" },
                            children: P(A.originalValue)
                          }
                        ) : null
                      ] }) : null
                    ]
                  },
                  `pie-slice-${A.index}`
                );
              }),
              p ? /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: g,
                  y: S,
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
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: X, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "bubble") {
    const o = Array.isArray(t && t.series) ? t.series.filter((N) => !ee.has(N.name)) : [], c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "Bubble charts need at least two measures (X and Y). A third measure sets bubble size.", theme: r, chartHeight: z });
    const u = o[0], p = o[1], h = o[2] || null, k = (u.values || []).filter((N) => typeof N == "number" && Number.isFinite(N)), g = (p.values || []).filter((N) => typeof N == "number" && Number.isFinite(N));
    if (k.length === 0 || g.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Not enough numeric data for a bubble chart.", theme: r, chartHeight: z });
    let S = Math.min(...k), w = Math.max(...k), y = Math.min(...g), I = Math.max(...g);
    if (S === w) {
      const N = Math.abs(w || 1) * 0.15 || 1;
      S -= N, w += N;
    }
    if (y === I) {
      const N = Math.abs(I || 1) * 0.15 || 1;
      y -= N, I += N;
    }
    const X = (w - S) * 0.08, A = (I - y) * 0.08;
    S -= X, w += X, y -= A, I += A;
    const j = h ? (h.values || []).filter((N) => typeof N == "number" && Number.isFinite(N)) : [], L = j.length ? Math.min(...j) : 0, H = (j.length ? Math.max(...j) : 1) - L || 1, G = (N) => {
      if (!h) return 7;
      const re = h.values && h.values[N];
      return typeof re != "number" || !Number.isFinite(re) ? 7 : 4 + (re - L) / H * 20;
    }, b = { top: 24, right: 20, bottom: 64, left: Math.max(52, At(P(I), 11) + 16) }, q = Z - b.left - b.right, oe = z - b.top - b.bottom, C = (N) => b.left + (N - S) / (w - S) * q, E = (N) => b.top + (I - N) / (I - y) * oe, ce = It(S, w, 5), ie = It(y, I, 5), se = ve(p.name, 0);
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", overflow: "visible", ...Ue }, children: [
          ie.map((N, re) => /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx("line", { x1: b.left, y1: E(N), x2: Z - b.right, y2: E(N), stroke: r.border, strokeDasharray: "3 4" }),
            /* @__PURE__ */ e.jsx("text", { x: b.left - 8, y: E(N) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: P(N) })
          ] }, `y-${re}`)),
          ce.map((N, re) => /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx("line", { x1: C(N), y1: b.top, x2: C(N), y2: z - b.bottom, stroke: r.border, strokeDasharray: "3 4" }),
            /* @__PURE__ */ e.jsx("text", { x: C(N), y: z - b.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, children: P(N) })
          ] }, `x-${re}`)),
          /* @__PURE__ */ e.jsx("line", { x1: b.left, y1: z - b.bottom, x2: Z - b.right, y2: z - b.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("line", { x1: b.left, y1: b.top, x2: b.left, y2: z - b.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("text", { x: Z / 2, y: z - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, children: u.name }),
          K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (b.top + z - b.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(b.top + z - b.bottom) / 2})`, children: K }) : null,
          (u.values || []).map((N, re) => {
            const fe = p.values[re];
            if (typeof N != "number" || !Number.isFinite(N) || typeof fe != "number" || !Number.isFinite(fe)) return null;
            const De = G(re), qt = c[re] || `Point ${re + 1}`, mt = h ? ` | ${h.name}: ${P(h.values[re])}` : "", Qt = { "data-tip-cat": qt, "data-tip-ser": `${p.name} (X:${P(N)})${mt}`, "data-tip-val": P(fe), "data-tip-color": se }, Pt = l[re];
            return /* @__PURE__ */ e.jsx(
              "circle",
              {
                cx: C(N),
                cy: E(fe),
                r: De,
                fill: se,
                opacity: "0.72",
                stroke: r.surfaceBg || r.background || "#fff",
                strokeWidth: "1.5",
                style: typeof W == "function" && Pt ? { cursor: "pointer" } : void 0,
                onClick: typeof W == "function" && Pt ? () => W(Pt) : void 0,
                ...Qt
              },
              `bubble-${re}`
            );
          }),
          M && (u.values || []).map((N, re) => {
            const fe = p.values[re];
            if (typeof N != "number" || !Number.isFinite(N) || typeof fe != "number" || !Number.isFinite(fe)) return null;
            const De = G(re);
            return /* @__PURE__ */ e.jsx("text", { x: C(N), y: E(fe) - De - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: c[re] || P(fe) }, `bubble-lbl-${re}`);
          })
        ] }),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: [{ label: `${u.name} vs ${p.name}${h ? ` (size: ${h.name})` : ""}`, color: se }], theme: r, hiddenSet: ee, onToggle: Ke, position: Oe }) : null
    ] });
  }
  if (n === "radar") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((A) => !ee.has(A.name)) : [];
    if (o.length < 3)
      return /* @__PURE__ */ e.jsx(Se, { message: "Radar charts need at least 3 categories (spokes).", theme: r, chartHeight: z });
    if (c.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Radar charts need at least one measure series.", theme: r, chartHeight: z });
    const l = c.flatMap((A) => (A.values || []).filter((j) => typeof j == "number" && Number.isFinite(j))), u = l.length ? Math.max(...l, 0) : 1, p = 60, h = Z / 2, k = z / 2, g = Math.min(h, k) - p, S = o.length, w = 2 * Math.PI / S, y = (A, j) => {
      const L = A * w - Math.PI / 2, D = g * (j / (u || 1));
      return { x: h + D * Math.cos(L), y: k + D * Math.sin(L) };
    }, I = (A) => {
      const j = A * w - Math.PI / 2, L = g + 18;
      return { x: h + L * Math.cos(j), y: k + L * Math.sin(j) };
    }, X = c.map((A, j) => ({ label: A.name, color: ve(A.name, j) }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: [
          [0.25, 0.5, 0.75, 1].map((A) => {
            const j = o.map((L, D) => y(D, u * A));
            return /* @__PURE__ */ e.jsx("polygon", { points: j.map((L) => `${L.x},${L.y}`).join(" "), fill: "none", stroke: r.border, strokeWidth: "1" }, `grid-${A}`);
          }),
          o.map((A, j) => {
            const L = y(j, u);
            return /* @__PURE__ */ e.jsx("line", { x1: h, y1: k, x2: L.x, y2: L.y, stroke: r.border, strokeWidth: "1" }, `spoke-${j}`);
          }),
          o.map((A, j) => {
            const L = I(j), D = Math.abs(L.x - h) < 10 ? "middle" : L.x < h ? "end" : "start";
            return /* @__PURE__ */ e.jsx("text", { x: L.x, y: L.y + 4, textAnchor: D, fontSize: "10", fill: r.textSec, children: he(A, 14) }, `spoke-lbl-${j}`);
          }),
          c.map((A, j) => {
            const L = ve(A.name, j), D = o.map((G, le) => {
              const b = A.values && A.values[le];
              return y(le, typeof b == "number" && Number.isFinite(b) ? b : 0);
            }), H = D.map((G) => `${G.x},${G.y}`).join(" ");
            return /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx("polygon", { points: H, fill: L, fillOpacity: "0.18", stroke: L, strokeWidth: "2", style: { pointerEvents: "none" } }),
              D.map((G, le) => {
                const b = A.values && A.values[le], q = { "data-tip-cat": o[le], "data-tip-ser": A.name, "data-tip-val": P(b), "data-tip-color": L };
                return /* @__PURE__ */ e.jsx("circle", { cx: G.x, cy: G.y, r: "4", fill: L, stroke: r.surfaceBg || "#fff", strokeWidth: "1.5", ...q }, `radar-dot-${j}-${le}`);
              })
            ] }, `radar-${j}`);
          }),
          [0.5, 1].map((A) => /* @__PURE__ */ e.jsx("text", { x: h + 4, y: k - g * A + 4, fontSize: "9", fill: r.textSec, children: P(u * A) }, `scale-${A}`))
        ] }),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: X, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "funnel") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!c || o.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Funnel charts need categories and at least one measure.", theme: r, chartHeight: z });
    const l = o.map((w, y) => {
      const I = c.values && c.values[y];
      return typeof I == "number" && Number.isFinite(I) ? Math.abs(I) : 0;
    }), u = Math.max(...l, 1), p = { top: 24, right: 20, bottom: 24, left: 20 }, h = Z - p.left - p.right, g = (z - p.top - p.bottom) / o.length, S = o.map((w, y) => ({ label: w, color: ve(w, y) }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsx("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: o.map((w, y) => {
          const I = l[y] / u, X = y + 1 < o.length ? l[y + 1] / u : l[y] / u * 0.5, A = h * I, j = h * X, L = Z / 2, D = p.top + y * g, H = ve(w, y), G = [
            `${L - A / 2},${D}`,
            `${L + A / 2},${D}`,
            `${L + j / 2},${D + g - 1}`,
            `${L - j / 2},${D + g - 1}`
          ].join(" ");
          return /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx(
              "polygon",
              {
                points: G,
                fill: H,
                opacity: "0.88",
                stroke: r.surfaceBg || "#fff",
                strokeWidth: "1.5",
                "data-tip-cat": w,
                "data-tip-ser": c.name,
                "data-tip-val": P(l[y]),
                "data-tip-color": H
              }
            ),
            /* @__PURE__ */ e.jsxs("text", { x: L, y: D + g / 2 + 4, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: "#fff", style: { pointerEvents: "none" }, children: [
              he(w, Math.max(4, Math.floor(A / 7))),
              " ",
              M ? `— ${P(l[y])}` : ""
            ] })
          ] }, `funnel-${y}`);
        }) }),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: S, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "histogram") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!o)
      return /* @__PURE__ */ e.jsx(Se, { message: "Histogram needs at least one numeric measure.", theme: r, chartHeight: z });
    const c = (o.values || []).filter((D) => typeof D == "number" && Number.isFinite(D));
    if (c.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "No numeric values for histogram.", theme: r, chartHeight: z });
    const l = Math.max(4, Math.min(20, Math.ceil(1 + Math.log2(c.length)))), u = Math.min(...c), h = (Math.max(...c) - u) / l || 1, k = Array.from({ length: l }, (D, H) => ({
      low: u + H * h,
      high: u + (H + 1) * h,
      count: 0
    }));
    c.forEach((D) => {
      const H = Math.min(Math.floor((D - u) / h), l - 1);
      k[H].count++;
    });
    const g = Math.max(...k.map((D) => D.count), 1), w = { top: 24, right: 16, bottom: 44, left: Math.max(46, At(String(g), 11) + 16) }, y = Z - w.left - w.right, I = z - w.top - w.bottom, X = y / l, A = (D) => w.top + (g - D) / g * I, j = ve(o.name, 0), L = It(0, g, U || 5);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
      /* @__PURE__ */ e.jsxs("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: [
        L.map((D, H) => /* @__PURE__ */ e.jsxs("g", { children: [
          /* @__PURE__ */ e.jsx("line", { x1: w.left, y1: A(D), x2: Z - w.right, y2: A(D), stroke: r.border, strokeDasharray: "3 4" }),
          /* @__PURE__ */ e.jsx("text", { x: w.left - 8, y: A(D) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: D })
        ] }, `htick-${H}`)),
        /* @__PURE__ */ e.jsx("line", { x1: w.left, y1: w.top + I, x2: Z - w.right, y2: w.top + I, stroke: r.textSec, strokeWidth: "1" }),
        k.map((D, H) => {
          const G = w.left + H * X, le = A(D.count), b = Math.max(1, w.top + I - le);
          return /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx(
              "rect",
              {
                x: G,
                y: le,
                width: Math.max(1, X - 1),
                height: b,
                fill: j,
                opacity: "0.88",
                "data-tip-cat": `${P(D.low)}–${P(D.high)}`,
                "data-tip-ser": "Count",
                "data-tip-val": String(D.count),
                "data-tip-color": j
              }
            ),
            M && D.count > 0 ? /* @__PURE__ */ e.jsx("text", { x: G + X / 2, y: le - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: D.count }) : null
          ] }, `hbar-${H}`);
        }),
        k.filter((D, H) => H % Math.max(1, Math.floor(l / 6)) === 0 || H === l - 1).map((D, H) => /* @__PURE__ */ e.jsx("text", { x: w.left + k.indexOf(D) * X + X / 2, y: w.top + I + 18, textAnchor: "middle", fontSize: "10", fill: r.textSec, children: P(D.low) }, `hlbl-${H}`))
      ] }),
      Re,
      we,
      Pe,
      Ye
    ] }) });
  }
  if (n === "boxplot") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((w) => !ee.has(w.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", h = r.gridLine || r.border || "#eee";
    if (c.length < 5)
      return /* @__PURE__ */ e.jsx(Se, { message: "Box Plot needs 5 measures: Min, Q1, Median, Q3, Max.", theme: r, chartHeight: z });
    const k = o.map((w, y) => [
      c[0].values[y] ?? 0,
      c[1].values[y] ?? 0,
      c[2].values[y] ?? 0,
      c[3].values[y] ?? 0,
      c[4].values[y] ?? 0
    ]), g = ve(c[2].name, 0), S = {
      backgroundColor: l,
      tooltip: { trigger: "axis", formatter: (w) => {
        const y = w[0];
        return `${y.name}<br/>Min: ${P(y.data[1])}<br/>Q1: ${P(y.data[2])}<br/>Median: ${P(y.data[3])}<br/>Q3: ${P(y.data[4])}<br/>Max: ${P(y.data[5])}`;
      } },
      xAxis: { type: "category", data: o.map((w) => he(String(w), 14)), axisLabel: { color: u, fontSize: 10 }, axisLine: { lineStyle: { color: p } } },
      yAxis: { type: "value", axisLabel: { color: u, fontSize: 10, formatter: P }, axisLine: { lineStyle: { color: p } }, splitLine: { lineStyle: { color: h } }, name: K || "", nameTextStyle: { color: u } },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      series: [{ type: "boxplot", data: k, itemStyle: { color: g, borderColor: g } }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: f, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: S }) });
  }
  if (n === "nightingale") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((k) => !ee.has(k.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333";
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Nightingale charts need categories and at least one measure.", theme: r, chartHeight: z });
    const p = o.map((k, g) => {
      const S = c[0] && c[0].values ? c[0].values[g] ?? 0 : 0;
      return { name: String(k), value: S, itemStyle: { color: ve(String(k), g) } };
    }), h = {
      backgroundColor: l,
      tooltip: { trigger: "item", formatter: (k) => `${k.name}: ${P(k.value)}` },
      legend: { show: m, textStyle: { color: u }, type: "scroll" },
      series: [{
        type: "bar",
        coordinateSystem: "polar",
        data: p,
        label: { show: M, position: "outside", formatter: (k) => P(k.value), color: u, fontSize: 10 }
      }],
      polar: { radius: ["10%", "75%"] },
      angleAxis: { type: "category", data: o.map((k) => he(String(k), 12)), axisLabel: { color: u, fontSize: 9 } },
      radiusAxis: { axisLabel: { color: u, fontSize: 9, formatter: P } }
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: f, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: h }) });
  }
  if (n === "heatmap") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((H) => !ee.has(H.name)) : [];
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Heatmap needs categories and at least one measure.", theme: r, chartHeight: z });
    const l = c.flatMap((H) => (H.values || []).filter((G) => typeof G == "number" && Number.isFinite(G))), u = l.length ? Math.min(...l) : 0, h = (l.length ? Math.max(...l) : 1) - u || 1, k = (H) => {
      const G = Math.max(0, Math.min(1, (H - u) / h)), le = Math.round(59 + G * 180), b = Math.round(130 - G * 62), q = Math.round(246 - G * 178);
      return `rgb(${le},${b},${q})`;
    }, g = 2, S = Math.min(140, Math.max(60, c.reduce((H, G) => Math.max(H, At(G.name, 11)), 0) + 10)), w = 32, y = Z, I = z, X = y - S - 8, A = I - w - 8, j = Math.max(20, X / o.length), L = Math.max(20, A / c.length), D = c.map((H, G) => ({ label: H.name, color: Ce(G, r, je) }));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: R,
            viewBox: `0 0 ${y} ${I}`,
            preserveAspectRatio: "xMidYMid meet",
            style: { width: "100%", height: "auto", display: "block", maxWidth: "100%", ...Ue },
            role: "img",
            "aria-label": t.title || "Heatmap",
            children: [
              o.map((H, G) => /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: S + G * j + j / 2,
                  y: w - 6,
                  textAnchor: "middle",
                  fontSize: "10",
                  fill: r.textSec,
                  style: { pointerEvents: "none" },
                  children: he(H, Math.max(4, Math.floor(j / 6.5)))
                },
                `hm-col-${G}`
              )),
              c.map((H, G) => /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: S - 6,
                  y: w + G * L + L / 2 + 4,
                  textAnchor: "end",
                  fontSize: "11",
                  fill: r.textSec,
                  style: { pointerEvents: "none" },
                  children: he(H.name, Math.max(6, Math.floor(S / 6.5)))
                },
                `hm-row-${G}`
              )),
              c.map(
                (H, G) => o.map((le, b) => {
                  const q = H.values && H.values[b];
                  if (q == null) return null;
                  const oe = k(q), C = S + b * j, E = w + G * L;
                  return /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: C + g,
                      y: E + g,
                      width: Math.max(1, j - g * 2),
                      height: Math.max(1, L - g * 2),
                      rx: "3",
                      fill: oe,
                      "data-tip-cat": le,
                      "data-tip-ser": H.name,
                      "data-tip-val": P(q),
                      "data-tip-color": oe
                    },
                    `hm-cell-${G}-${b}`
                  );
                })
              ),
              M && c.map(
                (H, G) => o.map((le, b) => {
                  const q = H.values && H.values[b];
                  if (q == null || j < 36 || L < 18) return null;
                  const C = Math.max(0, Math.min(1, (q - u) / h)) > 0.5 ? "#fff" : r.text;
                  return /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: S + b * j + j / 2,
                      y: w + G * L + L / 2 + 4,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: C,
                      style: { pointerEvents: "none" },
                      children: P(q)
                    },
                    `hm-label-${G}-${b}`
                  );
                })
              )
            ]
          }
        ),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: D, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "bar3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((g) => !ee.has(g.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", h = [];
    c.forEach((g, S) => {
      o.forEach((w, y) => {
        const I = g.values[y];
        I != null && h.push([y, S, I]);
      });
    });
    const k = {
      backgroundColor: l,
      legend: {
        show: m,
        data: c.map((g) => g.name),
        textStyle: { color: u },
        type: "scroll"
      },
      tooltip: {
        formatter: (g) => {
          const S = o[g.value[0]] || "", w = c[g.value[1]] ? c[g.value[1]].name : "";
          return `${S} / ${w}: <b>${P(g.value[2])}</b>`;
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
        data: o.map((g) => he(String(g), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } },
        name: ""
      },
      yAxis3D: {
        type: "category",
        data: c.map((g) => he(String(g.name), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      zAxis3D: {
        type: "value",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        name: K || "",
        nameTextStyle: { color: u }
      },
      series: [{
        type: "bar3D",
        data: h.map((g) => ({
          value: g,
          itemStyle: { color: Ce(g[1], r, je), opacity: 0.9 }
        })),
        shading: "lambert",
        label: {
          show: M,
          formatter: (g) => P(g.value[2]),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { label: { show: !0 }, itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: f, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: k }) });
  }
  if (n === "line3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((g) => !ee.has(g.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", h = c.map((g, S) => ({
      type: "line3D",
      data: o.map((w, y) => {
        const I = g.values[y];
        return [y, S, I ?? 0];
      }),
      lineStyle: { color: Ce(S, r, je), width: 3, opacity: 0.9 },
      name: g.name
    })), k = {
      backgroundColor: l,
      legend: { show: m, data: c.map((g) => g.name), textStyle: { color: u }, type: "scroll" },
      tooltip: {
        formatter: (g) => {
          const S = o[g.value[0]] || "", w = c[g.value[1]] ? c[g.value[1]].name : "";
          return `${S} / ${w}: <b>${P(g.value[2])}</b>`;
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
        data: o.map((g) => he(String(g), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      yAxis3D: {
        type: "category",
        data: c.map((g) => he(String(g.name), 12)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      zAxis3D: {
        type: "value",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        name: K || "",
        nameTextStyle: { color: u }
      },
      series: h
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: f, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: k }) });
  }
  if (n === "scatter3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((y) => !ee.has(y.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc";
    if (c.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "3D Scatter needs at least two numeric measures (X and Y).", theme: r, chartHeight: z });
    const h = c[0], k = c[1], g = c[2] || null, S = o.map((y, I) => ({
      value: [h.values[I] ?? 0, k.values[I] ?? 0, g ? g.values[I] ?? 0 : I],
      name: String(y)
    })), w = {
      backgroundColor: l,
      tooltip: {
        formatter: (y) => `${o[y.dataIndex] || ""}<br/>X=${P(y.value[0])}, Y=${P(y.value[1])}, Z=${P(y.value[2])}`
      },
      grid3D: {
        boxWidth: 200,
        boxDepth: 80,
        viewControl: { projection: "perspective", autoRotate: !1 },
        light: { main: { intensity: 1.2, shadow: !1 }, ambient: { intensity: 0.4 } }
      },
      xAxis3D: {
        type: "value",
        name: h.name,
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        nameTextStyle: { color: u }
      },
      yAxis3D: {
        type: "value",
        name: k.name,
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        nameTextStyle: { color: u }
      },
      zAxis3D: {
        type: "value",
        name: g ? g.name : "Index",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        nameTextStyle: { color: u }
      },
      series: [{
        type: "scatter3D",
        data: S,
        symbolSize: 8,
        itemStyle: { color: Ce(0, r, je), opacity: 0.85 },
        label: {
          show: M,
          formatter: (y) => String(o[y.dataIndex] || ""),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: f, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: w }) });
  }
  if (n === "range") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter((y) => !ee.has(y.name)) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", h = r.gridLine || r.border || "#eee";
    if (c.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "Range chart needs at least two measures (low and high values).", theme: r, chartHeight: z });
    const k = [];
    for (let y = 0; y + 1 < c.length; y += 2)
      k.push({ low: c[y], high: c[y + 1] });
    const g = [];
    k.forEach((y, I) => {
      const X = Ce(I, r, je);
      g.push({
        type: "bar",
        name: `_base_${I}`,
        data: o.map((A, j) => Math.min(y.low.values[j] ?? 0, y.high.values[j] ?? 0)),
        stack: `range_${I}`,
        itemStyle: { opacity: 0 },
        emphasis: { disabled: !0 },
        silent: !0,
        legendHoverLink: !1
      }), g.push({
        type: "bar",
        name: `${y.low.name} – ${y.high.name}`,
        data: o.map((A, j) => Math.abs((y.high.values[j] ?? 0) - (y.low.values[j] ?? 0))),
        stack: `range_${I}`,
        itemStyle: { color: X, opacity: 0.9 },
        label: {
          show: M,
          position: "inside",
          formatter: (A) => {
            const j = A.dataIndex, L = y.low.values[j] ?? 0, D = y.high.values[j] ?? 0;
            return `${P(Math.min(L, D))}–${P(Math.max(L, D))}`;
          },
          color: u,
          fontSize: 10
        },
        tooltip: {
          formatter: (A) => {
            const j = A.dataIndex;
            return `${o[j]}<br/>${y.low.name}: ${P(y.low.values[j] ?? 0)}<br/>${y.high.name}: ${P(y.high.values[j] ?? 0)}`;
          }
        }
      });
    });
    const S = k.map((y, I) => ({ label: `${y.low.name} – ${y.high.name}`, color: Ce(I, r, je) })), w = {
      backgroundColor: l,
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: o.map((y) => he(String(y), 14)),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } },
        splitLine: { show: !1 }
      },
      yAxis: {
        type: "value",
        name: K || "",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        splitLine: { lineStyle: { color: h } },
        nameTextStyle: { color: u }
      },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      series: g
    };
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: f, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: w }) }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: S, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "scatter") {
    const o = Array.isArray(t && t.series) ? t.series.filter((C) => !ee.has(C.name)) : [], c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "Scatter charts need at least two numeric measures (X and Y).", theme: r, chartHeight: z });
    const u = o[0], p = o.slice(1), h = (u.values || []).filter((C) => typeof C == "number" && Number.isFinite(C)), k = p.flatMap((C) => (C.values || []).filter((E) => typeof E == "number" && Number.isFinite(E)));
    if (h.length === 0 || k.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Not enough numeric data for a scatter chart.", theme: r, chartHeight: z });
    let g = Math.min(...h), S = Math.max(...h), w = Math.min(...k), y = Math.max(...k);
    if (g === S) {
      const C = Math.abs(S || 1) * 0.15 || 1;
      g -= C, S += C;
    }
    if (w === y) {
      const C = Math.abs(y || 1) * 0.15 || 1;
      w -= C, y += C;
    }
    const I = (S - g) * 0.06, X = (y - w) * 0.06;
    g -= I, S += I, w -= X, y += X;
    const j = { top: 24, right: 20, bottom: 64, left: Math.max(52, At(P(y), 11) + 16) }, L = Z - j.left - j.right, D = z - j.top - j.bottom, H = (C) => j.left + (C - g) / (S - g) * L, G = (C) => j.top + (y - C) / (y - w) * D, le = It(g, S, 5), b = It(w, y, 5), q = p.map((C, E) => ({ label: C.name, color: ve(C.name, E) })), oe = typeof W == "function";
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { style: { position: "relative", width: "100%" }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: R,
            viewBox: `0 0 ${Z} ${z}`,
            width: "100%",
            preserveAspectRatio: "xMidYMid meet",
            style: { display: "block", overflow: "visible" },
            children: [
              b.map((C, E) => /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: G(C), x2: Z - j.right, y2: G(C), stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: j.left - 8, y: G(C) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(C) })
              ] }, `y-tick-${E}`)),
              le.map((C, E) => /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: H(C), y1: j.top, x2: H(C), y2: z - j.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: H(C), y: z - j.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(C) })
              ] }, `x-tick-${E}`)),
              /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: z - j.bottom, x2: Z - j.right, y2: z - j.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: j.top, x2: j.left, y2: z - j.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("text", { x: Z / 2, y: z - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: u.name }),
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (j.top + z - j.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(j.top + z - j.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              p.map((C, E) => {
                const ce = ve(C.name, E);
                return (C.values || []).map((ie, se) => {
                  const N = u.values[se];
                  if (typeof N != "number" || !Number.isFinite(N) || typeof ie != "number" || !Number.isFinite(ie)) return null;
                  const re = H(N), fe = G(ie), De = c[se] || `Point ${se + 1}`, qt = l[se], mt = {
                    "data-tip-cat": De,
                    "data-tip-ser": `${C.name} (${u.name}=${P(N)})`,
                    "data-tip-val": P(ie),
                    "data-tip-color": ce
                  };
                  return xt.cloneElement(zt(re, fe, ce, mt, {
                    style: oe && qt ? { cursor: "pointer" } : void 0,
                    onClick: oe && qt ? () => W(qt) : void 0
                  }), { key: `scatter-${E}-${se}` });
                });
              }),
              M ? p.map((C, E) => {
                const ce = ve(C.name, E);
                return (C.values || []).map((ie, se) => {
                  const N = u.values[se];
                  return typeof N != "number" || !Number.isFinite(N) || typeof ie != "number" || !Number.isFinite(ie) ? null : /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: H(N),
                      y: G(ie) - 8,
                      textAnchor: "middle",
                      fontSize: "10",
                      fontWeight: "700",
                      fill: ce,
                      style: { pointerEvents: "none" },
                      children: P(ie)
                    },
                    `scatter-label-${E}-${se}`
                  );
                });
              }) : null
            ]
          }
        ),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: q, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "waterfall") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (!o || c.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Waterfall charts need categories and at least one numeric measure.", theme: r, chartHeight: z });
    const u = c.map((C, E) => {
      const ce = o.values[E];
      return typeof ce == "number" && Number.isFinite(ce) ? ce : 0;
    }), p = [];
    let h = 0;
    for (let C = 0; C < u.length; C++) {
      const E = h;
      h += u[C], p.push({ label: c[C], start: E, end: h, increment: u[C], type: u[C] >= 0 ? "positive" : "negative" });
    }
    p.push({ label: "Total", start: 0, end: h, increment: h, type: "total" });
    const k = p.map((C) => C.start).concat(p.map((C) => C.end));
    let g = Math.min(...k, 0), S = Math.max(...k, 0);
    if (g === S) {
      const C = Math.abs(S || 1) * 0.15 || 1;
      g -= C, S += C;
    }
    const w = (S - g) * 0.08;
    g -= w, S += w;
    const y = It(g, S, 5), X = { top: 24, right: 20, bottom: 84, left: Math.max(52, At(P(S), 11) + 16) }, A = Z - X.left - X.right, j = z - X.top - X.bottom, L = (C) => X.top + (S - C) / (S - g) * j, D = A / p.length, H = Math.max(14, Math.min(60, D * 0.6)), G = "#059669", le = "#DC2626", b = Ce(0, r, je), q = typeof W == "function", oe = [
      { label: "Increase", color: G },
      { label: "Decrease", color: le },
      { label: "Total", color: b }
    ];
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative" }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: R,
            viewBox: `0 0 ${Z} ${z}`,
            width: "100%",
            preserveAspectRatio: "xMidYMid meet",
            style: { display: "block", overflow: "visible" },
            children: [
              y.map((C, E) => /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: X.left, y1: L(C), x2: Z - X.right, y2: L(C), stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: X.left - 8, y: L(C) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(C) })
              ] }, `wf-tick-${E}`)),
              /* @__PURE__ */ e.jsx("line", { x1: X.left, y1: L(0), x2: Z - X.right, y2: L(0), stroke: r.textSec, strokeOpacity: "0.5" }),
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (X.top + z - X.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(X.top + z - X.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              p.map((C, E) => {
                const ce = X.left + E * D + D / 2, ie = ce - H / 2, se = L(Math.max(C.start, C.end)), N = L(Math.min(C.start, C.end)), re = Math.max(1, N - se), fe = C.type === "total" ? b : C.type === "positive" ? G : le, De = E < l.length ? l[E] : null;
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  E > 0 ? /* @__PURE__ */ e.jsx(
                    "line",
                    {
                      x1: X.left + (E - 1) * D + D / 2 + H / 2,
                      y1: E === p.length - 1 ? L(C.end) : L(C.start),
                      x2: ie,
                      y2: E === p.length - 1 ? L(C.end) : L(C.start),
                      stroke: r.textSec,
                      strokeDasharray: "2 2",
                      strokeOpacity: "0.5"
                    }
                  ) : null,
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: ie,
                      y: se,
                      width: H,
                      height: re,
                      rx: "3",
                      fill: fe,
                      opacity: "0.9",
                      "data-tip-cat": C.label,
                      "data-tip-ser": C.type === "total" ? "Total" : (C.increment >= 0 ? "+" : "") + P(C.increment),
                      "data-tip-val": P(C.end),
                      "data-tip-color": fe,
                      style: q && De ? { cursor: "pointer" } : void 0,
                      onClick: q && De ? () => W(De) : void 0
                    }
                  ),
                  M ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: ce,
                      y: se - 5,
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
                      x: ce,
                      y: z - X.bottom + 16,
                      textAnchor: "middle",
                      fontSize: "10",
                      fontWeight: "600",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: he(C.label, 12)
                    }
                  )
                ] }, `wf-bar-${E}`);
              })
            ]
          }
        ),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: oe, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "combo") {
    const o = (Array.isArray(t && t.layers) ? t.layers : []).filter((b) => b && !b.hidden && !ee.has(b.name) && Array.isArray(b.values) && b.values.some((q) => q !== null));
    if (o.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: t.emptyMessage || "No combo layers are configured.", theme: r, chartHeight: z });
    const c = o.some((b) => b.axis === "right"), l = { top: 24, right: c ? 76 : 20, bottom: 96, left: 72 }, u = Math.max(1, Array.isArray(t.categories) ? t.categories.length : 0), p = Math.max(1, (Z - l.left - l.right) / u), h = Math.max(22, Math.min(72, p * 0.72)), k = o.filter((b) => b.type === "bar"), g = o.filter((b) => b.type === "area"), S = o.filter((b) => b.type === "line"), w = Nr(
      o.filter((b) => b.axis !== "right").flatMap((b) => b.values),
      z,
      l
    ), y = c ? Nr(
      o.filter((b) => b.axis === "right").flatMap((b) => b.values),
      z,
      l
    ) : w, I = Array.isArray(t.categoryTargets) ? t.categoryTargets : [], X = o.map((b, q) => ({
      label: b.name,
      color: b.color || Ce(q, r, je)
    })), A = (b) => b.axis === "right" ? y.scaleY : w.scaleY, j = (b) => b.axis === "right" ? y.baselineY : w.baselineY, L = (b) => (q) => Ct(q, b.valueFormat || J || "auto"), D = o.find((b) => b.axis === "right"), H = D ? L(D) : P, G = 4, le = Math.max(8, (h - (Math.max(k.length, 1) - 1) * G) / Math.max(k.length, 1));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: R,
            viewBox: `0 0 ${Z} ${z}`,
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
              w.tickValues.map((b, q) => {
                const oe = w.scaleY(b);
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx(
                    "line",
                    {
                      x1: l.left,
                      x2: Z - l.right,
                      y1: oe,
                      y2: oe,
                      stroke: r.border,
                      strokeOpacity: "0.7",
                      strokeDasharray: "4 4"
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: l.left - 10,
                      y: oe + 4,
                      textAnchor: "end",
                      fontSize: "11",
                      fill: r.textSec,
                      children: P(b)
                    }
                  )
                ] }, `combo-left-tick-${q}`);
              }),
              c ? y.tickValues.map((b, q) => {
                const oe = y.scaleY(b);
                return /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: Z - l.right + 10,
                    y: oe + 4,
                    textAnchor: "start",
                    fontSize: "11",
                    fill: r.textSec,
                    children: H(b)
                  },
                  `combo-right-tick-${q}`
                );
              }) : null,
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (l.top + z - l.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(l.top + z - l.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              /* @__PURE__ */ e.jsx(
                "line",
                {
                  x1: l.left,
                  x2: Z - l.right,
                  y1: w.baselineY,
                  y2: w.baselineY,
                  stroke: r.textSec,
                  strokeOpacity: "0.45"
                }
              ),
              k.map((b, q) => t.categories.map((oe, C) => {
                const E = b.values[C];
                if (E == null) return null;
                const ce = l.left + C * p + (p - h) / 2, ie = A(b), se = j(b), N = ie(E), re = b.color || Ce(q, r, je), fe = ce + q * (le + G), De = L(b);
                return /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: fe,
                      y: Math.min(N, se),
                      width: le,
                      height: Math.max(1, Math.abs(se - N)),
                      rx: "3",
                      fill: re,
                      opacity: "0.9",
                      "data-tip-cat": oe,
                      "data-tip-ser": b.name,
                      "data-tip-val": De(E),
                      "data-tip-color": re
                    }
                  ),
                  M ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: fe + le / 2,
                      y: E >= 0 ? Math.min(N, se) - 4 : Math.max(N, se) + 12,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: De(E)
                    }
                  ) : null
                ] }, `${b.id}-${oe}-${q}`);
              })),
              g.map((b, q) => {
                const oe = t.categories.map((se, N) => {
                  const re = b.values[N];
                  if (re == null) return null;
                  const fe = l.left + N * p + p / 2, De = A(b);
                  return {
                    x: fe,
                    y: De(re),
                    baseY: j(b),
                    rawValue: re
                  };
                }), C = Pr(oe), E = xr(oe), ce = b.color || Ce(k.length + q, r, je), ie = L(b);
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  C ? /* @__PURE__ */ e.jsx("path", { d: C, fill: ce, opacity: "0.16", style: { pointerEvents: "none" } }) : null,
                  E ? /* @__PURE__ */ e.jsx("path", { d: E, fill: "none", stroke: ce, strokeWidth: "2.5", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: fr(pt[b.name]), style: { pointerEvents: "none" } }) : null,
                  oe.map((se, N) => se ? /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    /* @__PURE__ */ e.jsx(
                      "circle",
                      {
                        cx: se.x,
                        cy: se.y,
                        r: "4",
                        fill: ce,
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "2",
                        "data-tip-cat": t.categories[N],
                        "data-tip-ser": b.name,
                        "data-tip-val": ie(se.rawValue),
                        "data-tip-color": ce
                      }
                    ),
                    M ? /* @__PURE__ */ e.jsx("text", { x: se.x, y: se.y - 8, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: ie(se.rawValue) }) : null
                  ] }, `${b.id}-area-pt-${N}`) : null)
                ] }, `combo-area-${b.id}`);
              }),
              S.map((b, q) => {
                const oe = t.categories.map((ie, se) => {
                  const N = b.values[se];
                  return N == null ? null : {
                    x: l.left + se * p + p / 2,
                    y: A(b)(N),
                    rawValue: N
                  };
                }), C = xr(oe), E = b.color || Ce(k.length + g.length + q, r, je), ce = L(b);
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  C ? /* @__PURE__ */ e.jsx("path", { d: C, fill: "none", stroke: E, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: fr(pt[b.name]), style: { pointerEvents: "none" } }) : null,
                  oe.map((ie, se) => ie ? /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    /* @__PURE__ */ e.jsx(
                      "circle",
                      {
                        cx: ie.x,
                        cy: ie.y,
                        r: "4",
                        fill: E,
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "2",
                        "data-tip-cat": t.categories[se],
                        "data-tip-ser": b.name,
                        "data-tip-val": ce(ie.rawValue),
                        "data-tip-color": E
                      }
                    ),
                    M ? /* @__PURE__ */ e.jsx("text", { x: ie.x, y: ie.y - 8, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: ce(ie.rawValue) }) : null
                  ] }, `${b.id}-point-${se}`) : null)
                ] }, `combo-line-${b.id}`);
              }),
              t.categories.map((b, q) => {
                const oe = l.left + q * p + p / 2;
                return /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: oe,
                    y: z - l.bottom + 28,
                    textAnchor: "middle",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(b, 18)
                  },
                  `combo-label-${q}`
                );
              }),
              typeof W == "function" ? t.categories.map((b, q) => {
                const oe = I[q];
                return oe ? /* @__PURE__ */ e.jsx(
                  "rect",
                  {
                    x: l.left + q * p,
                    y: l.top,
                    width: p,
                    height: z - l.top - l.bottom,
                    fill: "transparent",
                    style: { cursor: "pointer" },
                    onClick: () => W(oe)
                  },
                  `combo-hit-${q}`
                ) : null;
              }) : null
            ]
          }
        ),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: X, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (!x)
    return /* @__PURE__ */ e.jsx(Se, { message: "No numeric data available for this chart.", theme: r, chartHeight: z });
  const mr = It(x.minValue, x.maxValue, U || 5), St = Ie ? Array.from(new Set(
    (t.stackedGroups || []).reduce((o, c) => ((c.segments || []).forEach((l) => {
      l && l.label && o.push(l.label);
    }), o), [])
  )) : [], cr = St.reduce((o, c, l) => (o[c] = Ce(l, r, je), o), {}), Mt = Ie ? St.map((o) => ({ label: o, color: cr[o] || Ce(0, r, je) })) : (t.series || []).map((o, c) => ({
    label: o.name,
    color: ve(o.name, c)
  })), Zt = x.hasHierarchicalBands ? t.categoryBands.reduce((o, c, l) => {
    const u = o[o.length - 1];
    return u && u.groupKey === c.groupKey ? (u.endIndex = l, o) : (o.push({
      groupKey: c.groupKey,
      outerLabel: c.outerLabel,
      startIndex: l,
      endIndex: l
    }), o);
  }, []) : [], Ot = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [], jr = typeof W == "function" && Ot.length === t.categories.length;
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
    /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Be }, ...We, children: [
      /* @__PURE__ */ e.jsxs(
        "svg",
        {
          ref: R,
          viewBox: `0 0 ${Z} ${z}`,
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
          onClick: xe ? lt : void 0,
          children: [
            ae ? /* @__PURE__ */ e.jsx("defs", { children: (t.series || []).map((o, c) => {
              const l = ve(o.name, c), u = $r.find((h) => h.value === sr) || $r[0], p = Wn(l, et);
              return /* @__PURE__ */ e.jsxs("linearGradient", { id: `pg-grad-${c}`, x1: u.x1, y1: u.y1, x2: u.x2, y2: u.y2, children: [
                /* @__PURE__ */ e.jsx("stop", { offset: p[0].offset, stopColor: l, stopOpacity: p[0].stopOpacity }),
                /* @__PURE__ */ e.jsx("stop", { offset: p[1].offset, stopColor: l, stopOpacity: p[1].stopOpacity })
              ] }, `grad-${c}`);
            }) }) : null,
            mr.map((o, c) => {
              const l = x.scaleY ? x.scaleY(o) : null, u = x.scaleX ? x.scaleX(o) : null, p = Ne ? `${Math.round(o)}%` : P(o);
              return /* @__PURE__ */ e.jsx("g", { children: x.horizontalBarMode ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                /* @__PURE__ */ e.jsx("line", { x1: u, y1: x.margin.top, x2: u, y2: z - x.margin.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: u, y: z - x.margin.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: xe ? { cursor: "pointer" } : void 0, children: p })
              ] }) : /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: l, x2: Z - x.margin.right, y2: l, stroke: r.border, strokeDasharray: "3 4" }),
                /* @__PURE__ */ e.jsx("text", { x: x.margin.left - 8, y: l + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "y", style: xe ? { cursor: "pointer" } : void 0, children: p })
              ] }) }, `tick-${c}`);
            }),
            /* @__PURE__ */ e.jsx(
              "line",
              {
                x1: x.horizontalBarMode ? x.baselineX : x.margin.left,
                y1: x.horizontalBarMode ? x.margin.top : x.baselineY,
                x2: x.horizontalBarMode ? x.baselineX : Z - x.margin.right,
                y2: x.horizontalBarMode ? z - x.margin.bottom : x.baselineY,
                stroke: r.textSec,
                strokeWidth: "1"
              }
            ),
            K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (x.margin.top + z - x.margin.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(x.margin.top + z - x.margin.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
            Array.isArray(T) && T.length > 0 && T.map((o, c) => {
              const l = Number(o.value);
              if (!Number.isFinite(l)) return null;
              const u = o.color || r.primary, p = o.orient === "v", h = fr(o.dash || "dashed") || "6 3";
              if (p) {
                const g = x.margin.left + l * x.step;
                return g < x.margin.left - 2 || g > Z - x.margin.right + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: g, y1: x.margin.top, x2: g, y2: z - x.margin.bottom, stroke: u, strokeWidth: "1.5", strokeDasharray: h, style: { pointerEvents: "none" } }),
                  o.label ? /* @__PURE__ */ e.jsx("text", { x: g + 4, y: x.margin.top + 12, textAnchor: "start", fontSize: "10", fontWeight: "700", fill: u, style: { pointerEvents: "none" }, children: o.label }) : null
                ] }, `ref-line-${c}`);
              }
              if (!x.scaleY) return null;
              const k = x.scaleY(l);
              return k < x.margin.top - 2 || k > z - x.margin.bottom + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: k, x2: Z - x.margin.right, y2: k, stroke: u, strokeWidth: "1.5", strokeDasharray: h, style: { pointerEvents: "none" } }),
                o.label ? /* @__PURE__ */ e.jsx("text", { x: Z - x.margin.right - 4, y: k - 4, textAnchor: "end", fontSize: "10", fontWeight: "700", fill: u, style: { pointerEvents: "none" }, children: o.label }) : null,
                typeof _e == "function" ? /* @__PURE__ */ e.jsx(
                  "circle",
                  {
                    cx: x.margin.left + 22,
                    cy: k,
                    r: 6,
                    fill: u,
                    stroke: "white",
                    strokeWidth: "1.5",
                    style: { cursor: "ns-resize" },
                    onMouseDown: (g) => {
                      g.preventDefault(), g.stopPropagation();
                      const S = g.clientY, w = l, { plotHeight: y, maxValue: I, minValue: X } = x, A = I - X;
                      if (A <= 0) return;
                      const j = (D) => {
                        const H = w - (D.clientY - S) / y * A;
                        _e((G) => G.map(
                          (le, b) => b === c ? { ...le, value: String(Math.round(H * 1e3) / 1e3) } : le
                        ));
                      }, L = () => {
                        window.removeEventListener("mousemove", j), window.removeEventListener("mouseup", L);
                      };
                      window.addEventListener("mousemove", j), window.addEventListener("mouseup", L);
                    }
                  }
                ) : null
              ] }, `ref-line-${c}`);
            }),
            Ht && !x.horizontalBarMode && (n === "bar" || n === "line" || n === "area" || n === "scatter") && Array.isArray(t.series) && t.series.length > 0 && x.scaleY ? t.series.filter((c) => !ee.has(c.name)).map((c, l) => {
              const u = c.values || [], p = ot === "moving_avg" ? qn(u, qe) : Zn(u);
              if (!p) return null;
              const h = p.map((w, y) => Number.isFinite(w) ? { x: x.margin.left + y * x.step + x.step / 2, y: x.scaleY(w) } : null), k = xr(h);
              if (!k) return null;
              const g = ve(c.name, l), S = _t || g;
              return /* @__PURE__ */ e.jsx(
                "path",
                {
                  d: k,
                  fill: "none",
                  stroke: S,
                  strokeWidth: String(Xt || 2),
                  strokeDasharray: "8 4",
                  strokeLinecap: "round",
                  opacity: "0.78",
                  style: { pointerEvents: "none" }
                },
                `trendline-${l}`
              );
            }) : null,
            x.hasHierarchicalBands && Zt.map((o, c) => {
              if (c === Zt.length - 1) return null;
              const l = x.horizontalBarMode ? x.margin.top + (o.endIndex + 1) * x.step : x.margin.left + (o.endIndex + 1) * x.step;
              return /* @__PURE__ */ e.jsx(
                "line",
                {
                  x1: x.horizontalBarMode ? x.margin.left : l,
                  y1: x.horizontalBarMode ? l : x.margin.top,
                  x2: x.horizontalBarMode ? Z - x.margin.right : l,
                  y2: x.horizontalBarMode ? l : z - x.margin.bottom + 58,
                  stroke: r.border,
                  strokeDasharray: "4 4"
                },
                `group-divider-${o.groupKey}-${c}`
              );
            }),
            Ie && (t.stackedGroups || []).map((o, c) => {
              const l = x.horizontalBarMode ? x.margin.top + c * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + c * x.step + (x.step - x.groupWidth) / 2, u = Math.max(14, x.groupWidth * 0.64);
              let p = 0, h = 0;
              return /* @__PURE__ */ e.jsxs("g", { children: [
                (o.segments || []).map((k) => {
                  const g = k && typeof k.value == "number" && Number.isFinite(k.value) ? k.value : null;
                  if (g === null) return null;
                  const S = g >= 0 ? p : h, w = S + g;
                  g >= 0 ? p = w : h = w;
                  const y = cr[k.label] || Ce(0, r, je);
                  if (x.horizontalBarMode) {
                    const L = x.scaleX(S), D = x.scaleX(w);
                    return /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: Math.min(L, D),
                        y: l,
                        width: Math.max(1, Math.abs(D - L)),
                        height: u,
                        rx: "3",
                        fill: y,
                        opacity: "0.94",
                        "data-tip-cat": o.label,
                        "data-tip-ser": k.label,
                        "data-tip-val": P(g),
                        "data-tip-color": y
                      },
                      `${o.key}-${k.path || k.label}`
                    );
                  }
                  const I = x.scaleY(S), X = x.scaleY(w), A = Math.min(I, X), j = Math.max(1, Math.abs(X - I));
                  return /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: l,
                      y: A,
                      width: u,
                      height: j,
                      rx: "3",
                      fill: y,
                      opacity: "0.94",
                      "data-tip-cat": o.label,
                      "data-tip-ser": k.label,
                      "data-tip-val": P(g),
                      "data-tip-color": y
                    },
                    `${o.key}-${k.path || k.label}`
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
                    y: z - x.margin.bottom + 30,
                    textAnchor: "middle",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(o.label, 20)
                  }
                )
              ] }, `stacked-group-${o.key}`);
            }),
            !Ie && n === "bar" && t.categories.map((o, c) => {
              const l = x.horizontalBarMode ? x.margin.top + c * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + c * x.step + (x.step - x.groupWidth) / 2, u = Wt || Ne ? Math.max(14, x.groupWidth * 0.64) : Math.max(6, x.groupWidth / Math.max(t.series.length, 1) - 4);
              let p = 0, h = 0;
              return /* @__PURE__ */ e.jsx("g", { children: t.series.map((k, g) => {
                if (ee.has(k.name)) return null;
                const S = k.values[c];
                if (S == null) return null;
                const w = ve(k.name, g), y = ae ? `url(#pg-grad-${g})` : w;
                if (Wt || Ne) {
                  const j = Ne && me ? me[c] : null, L = Ne && j ? S / j * 100 : S, D = L >= 0 ? p : h, H = D + L;
                  L >= 0 ? p = H : h = H;
                  const G = Ne ? `${(Math.abs(S / j) * 100).toFixed(1)}% (${P(S)})` : P(S), le = { "data-tip-cat": o, "data-tip-ser": k.name, "data-tip-val": G, "data-tip-color": w };
                  if (x.horizontalBarMode) {
                    const oe = x.scaleX(D), C = x.scaleX(H);
                    return /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: Math.min(oe, C),
                        y: l,
                        width: Math.max(1, Math.abs(C - oe)),
                        height: u,
                        rx: "3",
                        fill: y,
                        opacity: "0.92",
                        style: { transition: nt ? "width 0.35s ease, x 0.35s ease" : void 0 },
                        ...le
                      },
                      `${k.name}-${o}`
                    );
                  }
                  const b = x.scaleY(D), q = x.scaleY(H);
                  return /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: l,
                      y: Math.min(b, q),
                      width: u,
                      height: Math.max(1, Math.abs(q - b)),
                      rx: "3",
                      fill: y,
                      opacity: "0.92",
                      style: { transition: nt ? "height 0.35s ease, y 0.35s ease" : void 0 },
                      ...le
                    },
                    `${k.name}-${o}`
                  );
                }
                if (x.horizontalBarMode) {
                  const j = x.scaleX(S), L = l + g * (u + 4);
                  return /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: Math.min(j, x.baselineX),
                        y: L,
                        width: Math.max(1, Math.abs(x.baselineX - j)),
                        height: u,
                        rx: "3",
                        fill: y,
                        opacity: "0.9",
                        style: { transition: nt ? "width 0.35s ease, x 0.35s ease" : void 0 },
                        "data-tip-cat": o,
                        "data-tip-ser": k.name,
                        "data-tip-val": P(S),
                        "data-tip-color": w
                      }
                    ),
                    M ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: S >= 0 ? Math.max(j, x.baselineX) + 4 : Math.min(j, x.baselineX) - 4,
                        y: L + u / 2 + 3,
                        textAnchor: S >= 0 ? "start" : "end",
                        fontSize: "9",
                        fontWeight: "700",
                        fill: r.textSec,
                        style: { pointerEvents: "none" },
                        children: P(S)
                      }
                    ) : null
                  ] }, `${k.name}-${o}`);
                }
                const I = x.scaleY(S), X = Math.max(1, Math.abs(x.baselineY - I)), A = l + g * (u + 4);
                return /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: A,
                      y: Math.min(I, x.baselineY),
                      width: u,
                      height: X,
                      rx: "3",
                      fill: y,
                      opacity: "0.9",
                      style: { transition: nt ? "height 0.35s ease, y 0.35s ease" : void 0, cursor: xe ? "pointer" : void 0 },
                      "data-tip-cat": o,
                      "data-tip-ser": k.name,
                      "data-tip-val": P(S),
                      "data-tip-color": w,
                      "data-chart-element-type": "series",
                      "data-chart-element-id": k.name
                    }
                  ),
                  M ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: A + u / 2,
                      y: S >= 0 ? Math.min(I, x.baselineY) - 4 : Math.max(I, x.baselineY) + 12,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: P(S)
                    }
                  ) : null
                ] }, `${k.name}-${o}`);
              }) }, `bar-group-${o}`);
            }),
            (n === "line" || n === "area") && (() => {
              const o = t.categories.map(() => 0), c = t.categories.map(() => 0);
              return t.series.map((l, u) => {
                if (ee.has(l.name)) return null;
                const p = ve(l.name, u), h = t.categories.map((S, w) => {
                  const y = l.values[w];
                  if (y == null) return null;
                  const I = x.margin.left + w * x.step + x.step / 2;
                  if (ft || Ne) {
                    const X = Ne && me ? me[w] : null, A = Ne && X ? y / X * 100 : y, j = A >= 0 ? o[w] : c[w], L = j + A;
                    return A >= 0 ? o[w] = L : c[w] = L, {
                      x: I,
                      y: x.scaleY(L),
                      baseY: x.scaleY(j),
                      rawValue: y
                    };
                  }
                  return {
                    x: I,
                    y: x.scaleY(y),
                    baseY: x.baselineY,
                    rawValue: y
                  };
                }), k = xr(h), g = n === "area" ? Pr(h) : "";
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  n === "area" && g && /* @__PURE__ */ e.jsx("path", { d: g, fill: ae ? `url(#pg-grad-${u})` : p, opacity: ft || Ne ? "0.46" : "0.16", style: { pointerEvents: "none" } }),
                  k && /* @__PURE__ */ e.jsx("path", { d: k, fill: "none", stroke: p, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: fr(pt[l.name]), style: { pointerEvents: "none" } }),
                  h.map((S, w) => S ? /* @__PURE__ */ e.jsxs(xt.Fragment, { children: [
                    xt.cloneElement(zt(S.x, S.y, p, {
                      "data-tip-cat": t.categories[w],
                      "data-tip-ser": l.name,
                      "data-tip-val": P(S.rawValue),
                      "data-tip-color": p
                    }), { key: `marker-${u}-${w}` }),
                    M ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: S.x,
                        y: S.y - 8,
                        textAnchor: "middle",
                        fontSize: "9",
                        fontWeight: "700",
                        fill: r.textSec,
                        style: { pointerEvents: "none" },
                        children: P(S.rawValue)
                      }
                    ) : null
                  ] }, `${l.name}-${w}`) : null)
                ] }, `${l.name}-${n}`);
              });
            })(),
            jr && t.categories.map((o, c) => {
              const l = Ot[c];
              return l ? x.horizontalBarMode ? /* @__PURE__ */ e.jsx(
                "rect",
                {
                  x: x.margin.left,
                  y: x.margin.top + c * x.step,
                  width: Z - x.margin.left - x.margin.right,
                  height: x.step,
                  fill: "transparent",
                  style: { cursor: "pointer" },
                  onClick: () => W(l)
                },
                `category-hit-horizontal-${c}`
              ) : /* @__PURE__ */ e.jsx(
                "rect",
                {
                  x: x.margin.left + c * x.step,
                  y: x.margin.top,
                  width: x.step,
                  height: z - x.margin.top - x.margin.bottom,
                  fill: "transparent",
                  style: { cursor: "pointer" },
                  onClick: () => W(l)
                },
                `category-hit-vertical-${c}`
              ) : null;
            }),
            Ie ? null : x.horizontalBarMode ? x.hasHierarchicalBands ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              Zt.map((o) => {
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
              Zt.map((o) => {
                const c = x.margin.left + o.startIndex * x.step, l = x.margin.left + (o.endIndex + 1) * x.step, u = (c + l) / 2;
                return /* @__PURE__ */ e.jsx("g", { children: /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: u,
                    y: z - x.margin.bottom + 68,
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
                    y: z - x.margin.bottom + 30,
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
              return /* @__PURE__ */ e.jsx("g", { transform: `translate(${l}, ${z - x.margin.bottom + 18}) rotate(${ke})`, children: /* @__PURE__ */ e.jsxs("text", { textAnchor: ke === 0 ? "middle" : "start", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: xe ? { cursor: "pointer" } : void 0, children: [
                /* @__PURE__ */ e.jsx("title", { children: String(o) }),
                he(o, Nt.maxChars)
              ] }) }, `label-${c}`);
            })
          ]
        }
      ),
      Re,
      Pe
    ] }),
    m ? /* @__PURE__ */ e.jsx(jt, { items: Mt, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
  ] });
}, ls = ({ chartType: t, barLayout: n, onChange: i, canStack: a, theme: r }) => {
  const f = (m, M = !1) => ({
    border: `1px solid ${m === n ? r.primary : r.border}`,
    background: m === n ? r.select : r.headerSubtleBg || r.hover,
    color: m === n ? r.primary : r.text,
    borderRadius: r.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: M ? "not-allowed" : "pointer",
    opacity: M ? 0.45 : 1
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => i("grouped"), style: f("grouped"), children: t === "area" ? "Normal" : "Grouped" }),
    /* @__PURE__ */ e.jsx(
      "button",
      {
        onClick: () => {
          a && i("stacked");
        },
        style: f("stacked", !a),
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
        style: f("stacked100", !a),
        title: a ? "Normalize each category to 100%" : "Needs multiple series",
        children: "100%"
      }
    )
  ] });
}, cs = ({ orientation: t, onChange: n, theme: i }) => {
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
}, ds = ({ axisMode: t, onChange: n, theme: i }) => {
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
}, us = ({ level: t, onChange: n, maxLevel: i, theme: a }) => {
  if (!i || i <= 1) return null;
  const r = (f) => ({
    border: `1px solid ${f === t ? a.primary : a.border}`,
    background: f === t ? a.select : a.headerSubtleBg || a.hover,
    color: f === t ? a.primary : a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer",
    minWidth: "34px"
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("all"), style: r("all"), children: "All" }),
    Array.from({ length: i }, (f, m) => m + 1).map((f) => /* @__PURE__ */ e.jsx("button", { onClick: () => n(f), style: r(f), children: f }, f))
  ] });
}, xs = ({ title: t, subtitle: n, note: i, theme: a }) => !t && !n && !i ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px" }, children: [
  t ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: a.text }, children: t }) : null,
  n ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: n }) : null,
  i ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: i }) : null
] }), pe = ({ label: t, theme: n, children: i, controlMinWidth: a = "180px" }) => /* @__PURE__ */ e.jsxs("div", { style: {
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
] }), ps = ({
  rowLimit: t,
  onRowLimitChange: n,
  columnLimit: i,
  onColumnLimitChange: a,
  theme: r
}) => {
  const f = {
    width: "58px",
    border: `1px solid ${r.border}`,
    background: r.surfaceBg || r.background || "#fff",
    color: r.text,
    borderRadius: r.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700
  }, m = {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    color: r.textSec,
    fontSize: "11px",
    fontWeight: 700
  }, M = (W, R) => ($) => {
    W && W(An($.target.value, R));
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsxs("label", { style: m, children: [
      "Rows",
      /* @__PURE__ */ e.jsx(
        "input",
        {
          type: "number",
          min: "1",
          step: "1",
          value: t,
          onChange: M(n, Kr),
          style: f
        }
      )
    ] }),
    /* @__PURE__ */ e.jsxs("label", { style: m, children: [
      "Cols",
      /* @__PURE__ */ e.jsx(
        "input",
        {
          type: "number",
          min: "1",
          step: "1",
          value: i,
          onChange: M(a, Ur),
          style: f
        }
      )
    ] })
  ] });
}, fs = ({
  layers: t,
  onChange: n,
  availableColumns: i,
  theme: a
}) => {
  const r = Cr(i), f = jn(t, r), [m, M] = v.useState(""), W = v.useMemo(
    () => kn(r),
    [r]
  ), R = v.useMemo(
    () => new Map(W.map((Y) => [Y.id, Y])),
    [W]
  ), $ = m.trim().toLowerCase(), O = v.useMemo(
    () => $ ? W.filter((Y) => Y.searchText.includes($)) : W,
    [W, $]
  ), J = v.useMemo(() => {
    const Y = /* @__PURE__ */ new Map();
    return O.forEach((F) => {
      Y.has(F.groupLabel) || Y.set(F.groupLabel, []), Y.get(F.groupLabel).push(F);
    }), Array.from(Y.entries()).map(([F, U]) => ({ label: F, options: U }));
  }, [O]), K = {
    border: `1px solid ${a.border}`,
    background: a.surfaceBg || a.background || "#fff",
    color: a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700
  }, s = (Y, F) => {
    typeof n == "function" && n(f.map((U) => U.id === Y ? { ...U, ...F || {} } : U));
  }, _ = (Y, F) => {
    if (typeof n != "function") return;
    const U = f.findIndex((ue) => ue.id === Y), de = U + F;
    if (U < 0 || de < 0 || de >= f.length) return;
    const ne = [...f], [ae] = ne.splice(U, 1);
    ne.splice(de, 0, ae), n(ne);
  }, T = (Y) => {
    if (typeof n != "function") return;
    const F = f.filter((U) => U.id !== Y);
    n(F.length > 0 ? F : wn(r, 1));
  }, V = () => {
    if (typeof n != "function") return;
    const Y = new Set(f.map((de) => de.columnId)), F = r.find((de) => !Y.has(de.id)) || r[0];
    if (!F) return;
    const U = Sn({
      id: `layer-${f.length + 1}`,
      type: Yr[f.length % Yr.length],
      columnId: F.id,
      axis: f.length === 1 ? "right" : "left",
      name: Mn(F)
    }, r, f.length);
    n([...f, U]);
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
            value: m,
            onChange: (Y) => M(Y.target.value),
            placeholder: "Search measures, groups, or column ids",
            style: { ...K, width: "100%", maxWidth: "360px" }
          }
        ),
        /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", fontWeight: 700, color: a.textSec, whiteSpace: "nowrap" }, children: [
          O.length,
          " / ",
          W.length
        ] })
      ] })
    ] }),
    O.length === 0 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec, padding: "2px 2px 0 2px" }, children: "No measures match the current search. Clear the search to see the full catalog." }) : null,
    f.map((Y, F) => {
      const U = R.get(Y.columnId) || null, de = J.some((ne) => ne.options.some((ae) => ae.id === Y.columnId));
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
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", fontWeight: 800, color: a.text }, children: `Layer ${F + 1}` }),
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: U ? U.fullLabel : Y.name || "Select a measure" }),
                U ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: a.textSec }, children: `${U.groupLabel} | ${U.id}` }) : null
              ] }),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => _(Y.id, -1),
                    disabled: F === 0,
                    style: {
                      border: `1px solid ${a.border}`,
                      background: a.headerSubtleBg || a.hover,
                      color: a.textSec,
                      borderRadius: a.radiusSm || "8px",
                      padding: "6px 8px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: F === 0 ? "not-allowed" : "pointer",
                      opacity: F === 0 ? 0.45 : 1
                    },
                    children: "Up"
                  }
                ),
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => _(Y.id, 1),
                    disabled: F === f.length - 1,
                    style: {
                      border: `1px solid ${a.border}`,
                      background: a.headerSubtleBg || a.hover,
                      color: a.textSec,
                      borderRadius: a.radiusSm || "8px",
                      padding: "6px 8px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: F === f.length - 1 ? "not-allowed" : "pointer",
                      opacity: F === f.length - 1 ? 0.45 : 1
                    },
                    children: "Down"
                  }
                ),
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => T(Y.id),
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
                    value: Y.columnId || "",
                    onChange: (ne) => {
                      const ae = R.get(ne.target.value) || null;
                      s(Y.id, {
                        columnId: ne.target.value,
                        name: ae ? ae.fullLabel : Y.name
                      });
                    },
                    style: K,
                    children: [
                      !de && U ? /* @__PURE__ */ e.jsx("optgroup", { label: "Current Layer Measure", children: /* @__PURE__ */ e.jsx("option", { value: U.id, children: U.fullLabel }) }) : null,
                      J.map((ne) => /* @__PURE__ */ e.jsx("optgroup", { label: `${ne.label} (${ne.options.length})`, children: ne.options.map((ae) => /* @__PURE__ */ e.jsx("option", { value: ae.id, children: ae.leafLabel }, ae.id)) }, ne.label))
                    ]
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Layer Type" }),
                /* @__PURE__ */ e.jsxs(
                  "select",
                  {
                    value: Y.type,
                    onChange: (ne) => s(Y.id, { type: ne.target.value }),
                    style: K,
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
                    value: Y.axis,
                    onChange: (ne) => s(Y.id, { axis: ne.target.value }),
                    style: K,
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
                    value: Y.valueFormat || "auto",
                    onChange: (ne) => s(Y.id, { valueFormat: ne.target.value }),
                    style: K,
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
                    value: Y.color || "#6366f1",
                    onChange: (ne) => s(Y.id, { color: ne.target.value }),
                    style: { width: "100%", height: "34px", padding: "2px 4px", border: `1px solid ${a.border}`, borderRadius: a.radiusSm || "8px", cursor: "pointer", background: "none" },
                    title: "Override color for this layer"
                  }
                )
              ] })
            ] })
          ]
        },
        Y.id
      );
    }),
    /* @__PURE__ */ e.jsx("div", { children: /* @__PURE__ */ e.jsx(
      "button",
      {
        type: "button",
        onClick: V,
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
}, hs = ({ sortMode: t, onChange: n, theme: i }) => {
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
}, gs = ({ interactionMode: t, onChange: n, theme: i }) => {
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
}, ys = ({ scope: t, onChange: n, theme: i }) => {
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
}, tr = "http://www.w3.org/2000/svg", zr = (t, n) => {
  if (!t || typeof document > "u") return null;
  const i = yr(n), a = t.cloneNode(!0), r = (a.getAttribute("viewBox") || "").split(/[\s,]+/).map(Number), f = r.length === 4 && r[2] > 0 ? r[2] : t.clientWidth || 800, m = r.length === 4 && r[3] > 0 ? r[3] : t.clientHeight || 400, M = i.title ? 42 : 0, W = qr(
    i.legendItems,
    f,
    (T) => String(T || "").length * 6.4
  ), R = W.length > 0 ? W.length * 22 + 14 : 0, $ = i.legendPosition === "top" ? R : 0, O = M + $, J = M + m + R, K = Array.from(a.childNodes), s = document.createElementNS(tr, "g");
  s.setAttribute("transform", `translate(0 ${O})`), K.forEach((T) => s.appendChild(T)), a.appendChild(s), a.setAttribute("viewBox", `0 0 ${f} ${J}`), a.setAttribute("width", String(f)), a.setAttribute("height", String(J)), a.setAttribute("xmlns", tr);
  const _ = document.createElementNS(tr, "rect");
  if (_.setAttribute("x", "0"), _.setAttribute("y", "0"), _.setAttribute("width", String(f)), _.setAttribute("height", String(J)), _.setAttribute("fill", i.background), a.insertBefore(_, s), i.title) {
    const T = document.createElementNS(tr, "text");
    T.setAttribute("x", "16"), T.setAttribute("y", "27"), T.setAttribute("fill", i.textColor), T.setAttribute("font-size", "16"), T.setAttribute("font-weight", "700"), T.setAttribute("font-family", "Inter, Arial, sans-serif"), T.textContent = i.title, a.appendChild(T);
  }
  if (W.length > 0) {
    const T = i.legendPosition === "top" ? M + 16 : M + m + 18;
    W.forEach((V, Y) => {
      let F = 16;
      V.forEach((U) => {
        const de = T + Y * 22, ne = document.createElementNS(tr, "circle");
        ne.setAttribute("cx", String(F + 5)), ne.setAttribute("cy", String(de - 4)), ne.setAttribute("r", "5"), ne.setAttribute("fill", U.hidden ? i.secondaryTextColor : U.color || "#2563eb"), ne.setAttribute("opacity", U.hidden ? "0.35" : "1"), a.appendChild(ne);
        const ae = document.createElementNS(tr, "text");
        ae.setAttribute("x", String(F + 16)), ae.setAttribute("y", String(de)), ae.setAttribute("fill", i.secondaryTextColor), ae.setAttribute("font-size", "11"), ae.setAttribute("font-family", "Inter, Arial, sans-serif"), ae.setAttribute("opacity", U.hidden ? "0.45" : "1"), ae.textContent = U.label, a.appendChild(ae), F += U.width;
      });
    });
  }
  return a;
}, bs = async (t, n, i) => {
  if (!t || typeof window > "u") return;
  const a = zr(t, i);
  if (!a) return;
  const f = new XMLSerializer().serializeToString(a), m = new Blob([f], { type: "image/svg+xml;charset=utf-8" });
  gr(m, `${n}.svg`);
}, Qr = (t, n = 2) => new Promise((i) => {
  if (!t) {
    i(null);
    return;
  }
  const r = new XMLSerializer().serializeToString(t), f = t.getAttribute("viewBox");
  let m = t.clientWidth || 800, M = t.clientHeight || 400;
  if (f) {
    const K = f.split(/[\s,]+/).map(Number);
    K.length === 4 && K[2] > 0 && K[3] > 0 && (m = K[2], M = K[3]);
  }
  const W = document.createElement("canvas");
  W.width = m * n, W.height = M * n;
  const R = W.getContext("2d");
  R.fillStyle = "#ffffff", R.fillRect(0, 0, W.width, W.height);
  const $ = new Image(), O = new Blob([r], { type: "image/svg+xml;charset=utf-8" }), J = URL.createObjectURL(O);
  $.onload = () => {
    R.drawImage($, 0, 0, W.width, W.height), URL.revokeObjectURL(J), i(W);
  }, $.onerror = () => {
    URL.revokeObjectURL(J), i(null);
  }, $.src = J;
}), vs = async (t, n, i) => {
  const a = await Qr(zr(t, i));
  a && a.toBlob((r) => {
    r && gr(r, `${n}.png`);
  }, "image/png");
}, ms = async (t, n) => {
  if (typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  const i = await Qr(zr(t, n));
  if (!i) return !1;
  try {
    const a = await new Promise((r) => i.toBlob(r, "image/png"));
    return a ? (await navigator.clipboard.write([new ClipboardItem({ "image/png": a })]), !0) : !1;
  } catch {
    return !1;
  }
}, ut = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), js = (t) => new Promise((n) => {
  const i = new Image();
  i.onload = () => n(i), i.onerror = () => n(null), i.src = t;
}), Jr = async (t, n) => {
  const i = t && t.current && t.current.getEchartsInstance();
  if (!i) return null;
  const a = yr(n), r = i.getDataURL({ type: "png", pixelRatio: 2 }), f = await js(r);
  if (!f) return null;
  const m = Math.max(1, f.naturalWidth / Math.max(1, i.getWidth())), M = a.title ? Math.round(42 * m) : 0, R = document.createElement("canvas").getContext("2d");
  R.font = "11px Inter, Arial, sans-serif";
  const $ = qr(
    a.legendItems,
    i.getWidth(),
    (T) => R.measureText(String(T || "")).width
  ), O = $.length > 0 ? Math.round(($.length * 22 + 14) * m) : 0, J = a.legendPosition === "top" ? O : 0, K = M + J, s = document.createElement("canvas");
  s.width = f.naturalWidth, s.height = M + f.naturalHeight + O;
  const _ = s.getContext("2d");
  if (_.fillStyle = a.background, _.fillRect(0, 0, s.width, s.height), _.drawImage(f, 0, K), a.title && (_.fillStyle = a.textColor, _.font = `700 ${Math.round(16 * m)}px Inter, Arial, sans-serif`, _.textBaseline = "alphabetic", _.fillText(a.title, Math.round(16 * m), Math.round(27 * m))), $.length > 0) {
    const T = a.legendPosition === "top" ? M + Math.round(16 * m) : M + f.naturalHeight + Math.round(18 * m);
    _.font = `${Math.round(11 * m)}px Inter, Arial, sans-serif`, $.forEach((V, Y) => {
      let F = Math.round(16 * m);
      V.forEach((U) => {
        const de = T + Math.round(Y * 22 * m);
        _.globalAlpha = U.hidden ? 0.35 : 1, _.fillStyle = U.hidden ? a.secondaryTextColor : U.color || "#2563eb", _.beginPath(), _.arc(F + Math.round(5 * m), de - Math.round(4 * m), Math.round(5 * m), 0, Math.PI * 2), _.fill(), _.globalAlpha = U.hidden ? 0.45 : 1, _.fillStyle = a.secondaryTextColor, _.fillText(U.label, F + Math.round(16 * m), de), _.globalAlpha = 1, F += Math.round(U.width * m);
      });
    });
  }
  return s;
}, ks = async (t, n, i) => {
  const a = await Jr(t, i);
  a && a.toBlob((r) => {
    r && gr(r, `${n}.png`);
  }, "image/png");
}, ws = async (t, n) => {
  if (!(t && t.current && t.current.getEchartsInstance()) || typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  try {
    const a = await Jr(t, n);
    if (!a) return !1;
    const r = await new Promise((f) => a.toBlob(f, "image/png"));
    return r ? (await navigator.clipboard.write([new ClipboardItem({ "image/png": r })]), !0) : !1;
  } catch {
    return !1;
  }
}, en = ({
  model: t,
  chartType: n,
  chartLayers: i,
  onChartLayersChange: a,
  availableColumns: r,
  barLayout: f,
  axisMode: m,
  onChartTypeChange: M,
  onBarLayoutChange: W,
  onAxisModeChange: R,
  orientation: $,
  onOrientationChange: O,
  hierarchyLevel: J,
  onHierarchyLevelChange: K,
  theme: s,
  title: _,
  onTitleChange: T,
  subtitle: V,
  note: Y,
  chartHeight: F,
  onChartHeightChange: U,
  rowLimit: de,
  onRowLimitChange: ne,
  columnLimit: ae,
  onColumnLimitChange: ue,
  sortMode: nt,
  onSortModeChange: be,
  interactionMode: st,
  onInteractionModeChange: _e,
  serverScope: Ht,
  onServerScopeChange: ot,
  showServerScope: qe = !1,
  floating: Oe = !1,
  immersiveMode: Me = !1,
  onCategoryActivate: Vt,
  allowHierarchyCharts: sr = !0,
  locked: et = !1,
  onToggleLock: _t,
  configOpen: Xt,
  onConfigChange: Qe,
  settingsPanelWidth: pt,
  onSettingsPanelWidthChange: Gt,
  initialSettings: B = {},
  onSettingsChange: $t
}) => {
  const xe = n === "combo", je = n === "sparkline", P = xe ? !1 : n === "area" ? Zr(t) : Gr(t), Ie = _ !== void 0 ? _ : t && t.title, Wt = V !== void 0 ? V : t && t.subtitle, ft = Y !== void 0 ? Y : et ? Oe ? "Locked to the current chart request and floating frame" : "Locked to the current chart request" : t && t.note, [it, Ne] = v.useState(!1), ke = typeof Xt == "boolean" ? Xt : it, z = typeof Qe == "function" ? Qe : Ne, [ve, zt] = v.useState(() => (B == null ? void 0 : B.showDataLabels) ?? !1), [me, ht] = v.useState(() => (B == null ? void 0 : B.colorPalette) ?? "default"), [at, He] = v.useState(() => (B == null ? void 0 : B.valueFormat) ?? "auto"), [kt, or] = v.useState(() => (B == null ? void 0 : B.yAxisTitle) ?? ""), [Z, ee] = v.useState(() => (B == null ? void 0 : B.yAxisMin) ?? ""), [Bt, Ke] = v.useState(() => (B == null ? void 0 : B.yAxisMax) ?? ""), [$e, Xe] = v.useState(() => (B == null ? void 0 : B.referenceLines) ?? []), [Ge, gt] = v.useState({ value: "", label: "", orient: "h" }), [lt, Pe] = v.useState(() => (B == null ? void 0 : B.labelAngle) ?? 0), [Le, Ft] = v.useState(() => (B == null ? void 0 : B.chartSubtitle) ?? ""), [tt, Ee] = v.useState(() => (B == null ? void 0 : B.tickCount) ?? 5), [Ze, yt] = v.useState(() => (B == null ? void 0 : B.markerShape) ?? "circle"), [bt, rt] = v.useState(() => (B == null ? void 0 : B.markerSize) ?? 4), [Te, ct] = v.useState(() => (B == null ? void 0 : B.showGradient) ?? !1), [Fe, Lt] = v.useState(() => (B == null ? void 0 : B.seriesColors) ?? {}), [dt, wt] = v.useState(() => (B == null ? void 0 : B.showAnimations) ?? !0), [We, te] = v.useState(() => (B == null ? void 0 : B.showCalloutLabels) ?? !1), [Be, Ue] = v.useState(() => (B == null ? void 0 : B.showTrendline) ?? !1), [we, Ye] = v.useState(() => (B == null ? void 0 : B.trendlineMode) ?? "linear"), [Je, ir] = v.useState(() => (B == null ? void 0 : B.trendlinePeriod) ?? 3), [vt, lr] = v.useState(() => (B == null ? void 0 : B.legendPosition) ?? "bottom"), [Et, Tt] = v.useState(() => (B == null ? void 0 : B.labelPosition) ?? "outside"), [Ut, Kt] = v.useState(() => (B == null ? void 0 : B.decimalPlaces) ?? -1), [Rt, br] = v.useState(() => (B == null ? void 0 : B.gradientDirection) ?? "vertical"), [Dt, vr] = v.useState(() => (B == null ? void 0 : B.gradientOpacity) ?? 70), [Re, x] = v.useState(() => (B == null ? void 0 : B.trendlineColor) ?? ""), [Nt, mr] = v.useState(() => (B == null ? void 0 : B.trendlineWidth) ?? 2), [St, cr] = v.useState(() => (B == null ? void 0 : B.seriesSort) ?? "none"), [Mt, Zt] = v.useState(() => (B == null ? void 0 : B.customPaletteColors) ?? []), [Ot, jr] = v.useState(() => (B == null ? void 0 : B.lineDashStyles) ?? {}), [o, c] = v.useState(() => (B == null ? void 0 : B.yAxisMinRight) ?? ""), [l, u] = v.useState(() => (B == null ? void 0 : B.yAxisMaxRight) ?? ""), [p, h] = v.useState("chart"), [k, g] = v.useState(!0), [S, w] = v.useState(!0), y = v.useCallback((d) => {
    const ge = { series: "style", axis: "axes", legend: "labels", title: "data" }[d];
    ge && (z(!0), h(ge));
  }, [z]), I = Xr(me), [X, A] = v.useState(String(F || nr)), j = v.useRef(null), L = v.useRef(null), D = v.useRef(null), [H, G] = v.useState(null), [le, b] = v.useState(!1), { anchorRef: q, menuRef: oe, position: C, portalTarget: E } = $n(le, b), [ce, ie] = v.useState(null), [se, N] = v.useState(348), re = Number.isFinite(Number(pt)) ? Math.max(300, Math.min(680, Math.floor(Number(pt)))) : se, fe = typeof Gt == "function" ? Gt : N, De = !Me, mt = Me && ce ? Math.max(180, ce - (Me && !je && vt !== "none" ? 44 : 0) - 44) : Math.max(180, Number.isFinite(Number(F)) ? Number(F) : nr), Qt = v.useRef(null), Pt = v.useRef(null), kr = v.useRef(null), Br = t && t.maxHierarchyLevel || 1, dr = typeof K == "function" && Br > 1, Lr = {
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
  }, tn = {
    ...Lr,
    border: `1px solid ${et ? s.primary : s.border}`,
    background: et ? s.select : s.headerSubtleBg || s.hover,
    color: et ? s.primary : s.textSec
  }, Yt = {
    border: `1px solid ${s.border}`,
    background: s.headerSubtleBg || s.hover,
    color: s.textSec,
    borderRadius: s.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }, Ae = {
    border: `1px solid ${s.border}`,
    background: s.surfaceBg || s.background || "#fff",
    color: s.text,
    borderRadius: s.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 600,
    minWidth: "68px"
  }, rn = v.useCallback((d) => {
    if (d === "trend") {
      M("line"), typeof O == "function" && O("rows"), typeof be == "function" && be("natural");
      return;
    }
    if (d === "compare") {
      M("bar"), typeof W == "function" && W("grouped"), typeof R == "function" && R("vertical");
      return;
    }
    if (d === "composition") {
      M(dr ? "sunburst" : "donut");
      return;
    }
    if (d === "flow") {
      M("waterfall");
      return;
    }
    d === "sparkline" && M("sparkline");
  }, [
    R,
    W,
    M,
    O,
    be,
    dr
  ]);
  v.useEffect(() => {
    !Me || !ke || z(!1);
  }, [Me, ke]), v.useEffect(() => {
    if (!H) return;
    const d = setTimeout(() => G(null), 2500);
    return () => clearTimeout(d);
  }, [H]);
  const ur = v.useCallback(() => Qn(
    kr.current,
    Ie,
    vt,
    s
  ), [vt, Ie, s]), nn = v.useCallback(async (d) => {
    b(!1);
    const Q = ur();
    if (d === "svg") {
      ut.has(n) || await bs(Qt.current, "pivot-chart", Q);
      return;
    }
    if (d === "png") {
      ut.has(n) ? await ks(Pt, "pivot-chart", Q) : await vs(Qt.current, "pivot-chart", Q);
      return;
    }
    d === "csv" && Jn(t, "pivot-chart", Q);
  }, [n, ur, t]), sn = v.useCallback(async () => {
    const d = ur(), Q = ut.has(n) ? await ws(Pt, d) : await ms(Qt.current, d);
    G(Q ? { tone: "success", text: "Copied chart" } : { tone: "error", text: "Copy failed" });
  }, [n, ur]);
  v.useEffect(() => {
    if (!Me) {
      ie(null);
      return;
    }
    const d = D.current;
    if (!d) return;
    const Q = () => {
      const ye = d.clientHeight;
      ye > 0 && ie(ye);
    };
    if (Q(), typeof ResizeObserver > "u") return;
    const ge = new ResizeObserver(Q);
    return ge.observe(d), () => ge.disconnect();
  }, [Me]);
  const wr = v.useRef(U);
  v.useEffect(() => {
    wr.current = U;
  }), v.useEffect(() => {
    const d = (ge) => {
      if (!j.current || typeof wr.current != "function") return;
      const ye = j.current, Ve = Math.max(180, ye.startHeight + (ge.clientY - ye.startY));
      wr.current(Ve);
    }, Q = () => {
      j.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", Q), window.addEventListener("blur", Q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", Q), window.removeEventListener("blur", Q);
    };
  }, []), v.useEffect(() => {
    const d = (ge) => {
      if (!L.current) return;
      const ye = L.current, Ve = ye.direction === "left" ? ye.startX - ge.clientX : ge.clientX - ye.startX, Jt = Math.max(300, Math.min(680, ye.startWidth + Ve));
      fe(Jt);
    }, Q = () => {
      L.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", Q), window.addEventListener("blur", Q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", Q), window.removeEventListener("blur", Q);
    };
  }, []), v.useEffect(() => {
    const d = kr.current;
    if (!d || typeof ResizeObserver > "u") return;
    let Q = d.clientWidth;
    const ge = new ResizeObserver(() => {
      var Ve, Jt;
      const ye = d.clientWidth;
      if (ye !== Q) {
        Q = ye;
        const Rr = (Jt = (Ve = Pt.current) == null ? void 0 : Ve.getEchartsInstance) == null ? void 0 : Jt.call(Ve);
        Rr && Rr.resize();
      }
    });
    return ge.observe(d), () => ge.disconnect();
  }, []), v.useEffect(() => {
    A(String(mt));
  }, [mt]);
  const Sr = v.useRef($t);
  v.useEffect(() => {
    Sr.current = $t;
  });
  const Er = v.useRef(!0);
  v.useEffect(() => {
    if (Er.current) {
      Er.current = !1;
      return;
    }
    typeof Sr.current == "function" && Sr.current({
      showDataLabels: ve,
      colorPalette: me,
      valueFormat: at,
      yAxisTitle: kt,
      yAxisMin: Z,
      yAxisMax: Bt,
      referenceLines: $e,
      labelAngle: lt,
      chartSubtitle: Le,
      tickCount: tt,
      markerShape: Ze,
      markerSize: bt,
      showGradient: Te,
      seriesColors: Fe,
      showAnimations: dt,
      showCalloutLabels: We,
      showTrendline: Be,
      trendlineMode: we,
      trendlinePeriod: Je,
      legendPosition: vt,
      labelPosition: Et,
      decimalPlaces: Ut,
      gradientDirection: Rt,
      gradientOpacity: Dt,
      trendlineColor: Re,
      trendlineWidth: Nt,
      seriesSort: St,
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
    Z,
    Bt,
    $e,
    lt,
    Le,
    tt,
    Ze,
    bt,
    Te,
    Fe,
    dt,
    We,
    Be,
    we,
    Je,
    vt,
    Et,
    Ut,
    Rt,
    Dt,
    Re,
    Nt,
    St,
    Mt,
    Ot,
    o,
    l
  ]);
  const Mr = v.useMemo(() => {
    if (!t || !Array.isArray(t.series)) return null;
    let d = 1 / 0, Q = -1 / 0;
    return t.series.forEach((ge) => {
      (ge.values || []).forEach((ye) => {
        typeof ye == "number" && Number.isFinite(ye) && (d = Math.min(d, ye), Q = Math.max(Q, ye));
      });
    }), Number.isFinite(d) && d !== Q ? { min: d, max: Q } : null;
  }, [t]);
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      ref: D,
      style: {
        display: "flex",
        flexDirection: "column",
        gap: De ? "14px" : "10px",
        flex: "1 1 auto",
        minHeight: 0,
        overflow: "hidden"
      },
      children: [
        De || Me ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
          /* @__PURE__ */ e.jsx(
            xs,
            {
              title: Me ? Ie : null,
              subtitle: Wt,
              note: ft,
              theme: s
            }
          ),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap", justifyContent: "flex-end" }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { position: "relative" }, children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  ref: q,
                  onClick: () => b((d) => !d),
                  onKeyDown: (d) => {
                    (d.key === "ArrowDown" || d.key === "ArrowUp") && (d.preventDefault(), b(!0));
                  },
                  style: Yt,
                  title: "Export chart",
                  "aria-haspopup": "menu",
                  "aria-expanded": le,
                  children: /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "5px" }, children: [
                    "Export ",
                    /* @__PURE__ */ e.jsx(rr.ChevronDown, {})
                  ] })
                }
              ),
              le && E ? fn.createPortal(
                /* @__PURE__ */ e.jsx(
                  "div",
                  {
                    ref: oe,
                    role: "menu",
                    "aria-label": "Chart export formats",
                    style: {
                      position: "fixed",
                      ...C,
                      visibility: C ? "visible" : "hidden",
                      zIndex: 10060,
                      width: "190px",
                      boxSizing: "border-box",
                      overflowY: "auto",
                      fontFamily: "inherit",
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
                        onClick: () => {
                          var Q;
                          (Q = q.current) == null || Q.focus(), nn(d.format);
                        },
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
                        onMouseEnter: (Q) => {
                          d.disabled || (Q.currentTarget.style.background = s.hover);
                        },
                        onMouseLeave: (Q) => {
                          Q.currentTarget.style.background = "transparent";
                        },
                        children: [
                          /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", fontWeight: 800 }, children: d.label }),
                          /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: s.textSec }, children: d.hint })
                        ]
                      },
                      d.format
                    ))
                  }
                ),
                E
              ) : null
            ] }),
            /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: sn,
                style: Yt,
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
            typeof _t == "function" ? /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                onClick: _t,
                style: { ...tn, width: "auto", padding: "0 10px", gap: "6px", fontSize: "11px", fontWeight: 800 },
                title: et ? "Data is fixed to the saved selection. Click to resume live pivot updates." : "Keep this chart fixed to the current data selection while the pivot changes.",
                "aria-label": et ? "Unlock chart data and resume live updates" : "Lock chart to the current data selection",
                "aria-pressed": et,
                children: [
                  /* @__PURE__ */ e.jsx(rr.Lock, {}),
                  /* @__PURE__ */ e.jsx("span", { children: et ? "Data locked" : "Lock data" })
                ]
              }
            ) : null,
            De ? /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onMouseDown: (d) => d.stopPropagation(),
                onClick: (d) => {
                  d.stopPropagation(), z((Q) => !Q);
                },
                "data-chart-settings-toggle": "true",
                style: Lr,
                title: ke ? "Hide chart settings" : "Show chart settings",
                "aria-label": ke ? "Hide chart settings" : "Show chart settings",
                children: /* @__PURE__ */ e.jsx(rr.Settings, {})
              }
            ) : null
          ] })
        ] }) : null,
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", minHeight: 0, gap: 0, overflow: "hidden", flex: "1 1 auto" }, children: [
          /* @__PURE__ */ e.jsxs(
            "div",
            {
              ref: kr,
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
                t ? (!Array.isArray(t.series) || t.series.length === 0) && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsx(Se, { message: t.emptyMessage || "No chart data available.", theme: s, chartHeight: mt }) : /* @__PURE__ */ e.jsx(as, { model: t, chartType: n, barLayout: f, axisMode: m, theme: s, chartHeight: mt, showLegend: !je, showDataLabels: ve, onCategoryActivate: Vt, svgRef: Qt, echartsRef: Pt, colorPalette: me, valueFormat: at, yAxisTitle: kt, yAxisMin: Z, yAxisMax: Bt, referenceLines: $e, onReferenceLinesChange: Xe, labelAngle: lt, chartTitle: Me ? "" : Ie, onTitleChange: T, subtitle: Le, tickCount: tt, markerShape: Ze, markerSize: bt, showGradient: Te, seriesColors: Fe, showAnimations: dt, showCalloutLabels: We, showTrendline: Be, trendlineMode: we, trendlinePeriod: Je, legendPosition: vt, labelPosition: Et, decimalPlaces: Ut, gradientDirection: Rt, gradientOpacity: Dt, trendlineColor: Re, trendlineWidth: Nt, seriesSort: St, lineDashStyles: Ot, yAxisMinRight: o, yAxisMaxRight: l, customPaletteColors: Mt, onElementSelect: y }) : /* @__PURE__ */ e.jsx(Se, { message: "No chart data available.", theme: s, chartHeight: mt }),
                Le ? /* @__PURE__ */ e.jsx("div", { style: { textAlign: "center", fontSize: "11px", color: s.textSec, padding: "4px 8px 0", fontStyle: "italic" }, children: Le }) : null,
                !Me && typeof U == "function" ? /* @__PURE__ */ e.jsxs(
                  "div",
                  {
                    onMouseDown: (d) => {
                      d.preventDefault(), j.current = {
                        startY: d.clientY,
                        startHeight: mt
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
          De && ke ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
            /* @__PURE__ */ e.jsx(
              "div",
              {
                "data-chart-settings-resizer": "true",
                onMouseDown: (d) => {
                  d.preventDefault(), L.current = {
                    startX: d.clientX,
                    startWidth: re,
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
                  width: `${re}px`,
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
                        onClick: () => h(d.id),
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
                        onClick: () => z(!1),
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
                          /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => g((d) => !d), style: { display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", borderBottom: `1px solid ${s.border}`, padding: "4px 0", cursor: "pointer", marginBottom: k ? "6px" : 0 }, children: [
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec }, children: "Chart Type" }),
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: s.textSec, transform: k ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.15s" }, children: "▾" })
                          ] }),
                          k && /* @__PURE__ */ e.jsx(Un, { chartType: n, onChange: M, theme: s, includeHierarchyCharts: sr }),
                          /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => w((d) => !d), style: { display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", borderBottom: `1px solid ${s.border}`, padding: "4px 0", cursor: "pointer", marginTop: "8px", marginBottom: S ? "6px" : 0 }, children: [
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec }, children: "Parameters" }),
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: s.textSec, transform: S ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.15s" }, children: "▾" })
                          ] }),
                          S && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !ut.has(n) && !xe ? /* @__PURE__ */ e.jsx(pe, { label: "Source", theme: s, children: /* @__PURE__ */ e.jsx(cs, { orientation: $, onChange: O, theme: s }) }) : null,
                            n === "bar" ? /* @__PURE__ */ e.jsx(pe, { label: "Direction", theme: s, children: /* @__PURE__ */ e.jsx(ds, { axisMode: m, onChange: R, theme: s }) }) : null,
                            (n === "bar" || n === "area") && !xe && /* @__PURE__ */ e.jsx(pe, { label: "Layout", theme: s, children: /* @__PURE__ */ e.jsx(ls, { chartType: n, barLayout: f, onChange: W, canStack: P, theme: s }) }),
                            dr ? /* @__PURE__ */ e.jsx(pe, { label: "Level", theme: s, children: /* @__PURE__ */ e.jsx(us, { level: J, onChange: K, maxLevel: Br, theme: s }) }) : null,
                            xe ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Layers" }),
                              /* @__PURE__ */ e.jsx(fs, { layers: i, onChange: a, availableColumns: r, theme: s })
                            ] }) : null,
                            !(n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !ut.has(n) && !xe) && n !== "bar" && !((n === "bar" || n === "area") && !xe) && !dr && !xe ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: s.textSec, padding: "4px 0" }, children: "No parameters for this chart type." }) : null
                          ] })
                        ] }),
                        p === "data" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "General" }),
                          typeof T == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: Ie || "", onChange: (d) => T(d.target.value), placeholder: "Chart title", style: { ...Ae, width: "100%" } }) }) : null,
                          /* @__PURE__ */ e.jsx(pe, { label: "Subtitle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: Le, onChange: (d) => Ft(d.target.value), placeholder: "Chart subtitle", style: { ...Ae, width: "100%" } }) }),
                          typeof U == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Height", theme: s, controlMinWidth: "96px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "180", step: "20", value: X, onChange: (d) => A(d.target.value), onBlur: () => {
                            typeof U == "function" && U(X);
                          }, onKeyDown: (d) => {
                            d.key === "Enter" && typeof U == "function" && U(X);
                          }, style: Ae }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Data & Actions" }),
                          /* @__PURE__ */ e.jsx(pe, { label: "Limits", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(ps, { rowLimit: de, onRowLimitChange: ne, columnLimit: ae, onColumnLimitChange: ue, theme: s }) }),
                          typeof be == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(hs, { sortMode: nt, onChange: be, theme: s }) }) : null,
                          typeof be == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Series Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px", flexWrap: "wrap" }, children: Ln.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => cr(d.value), style: { border: `1px solid ${St === d.value ? s.primary : s.border}`, background: St === d.value ? s.select : s.headerSubtleBg || s.hover, color: St === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }) : null,
                          typeof _e == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Click", theme: s, children: /* @__PURE__ */ e.jsx(gs, { interactionMode: st, onChange: _e, theme: s }) }) : null,
                          qe && typeof ot == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Scope", theme: s, children: /* @__PURE__ */ e.jsx(ys, { scope: Ht, onChange: ot, theme: s }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Presets" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "6px" }, children: [
                            { key: "trend", label: "Trend" },
                            { key: "compare", label: "Compare" },
                            { key: "composition", label: "Composition" },
                            { key: "flow", label: "Flow" },
                            { key: "sparkline", label: "Sparkline" }
                          ].map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", "data-chart-preset": d.key, onClick: () => rn(d.key), style: { ...Yt, textAlign: "left" }, children: d.label }, d.key)) })
                        ] }),
                        p === "axes" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(pe, { label: "Y-Axis Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: kt, onChange: (d) => or(d.target.value), placeholder: "Y-axis title", style: { ...Ae, width: "100%" } }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(pe, { label: "Y Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: Z, onChange: (d) => ee(d.target.value), placeholder: "Min", style: { ...Ae, width: "50%" } }),
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: Bt, onChange: (d) => Ke(d.target.value), placeholder: "Max", style: { ...Ae, width: "50%" } })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Y Ticks", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "20", value: tt, onChange: (d) => Ee(Math.max(2, Math.min(20, Number(d.target.value) || 5))), style: { ...Ae, width: "80px" } }) }),
                            /* @__PURE__ */ e.jsx(pe, { label: "X Angle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center", flexWrap: "wrap" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", min: "0", max: "90", value: lt, onChange: (d) => Pe(Math.max(0, Math.min(90, Number(d.target.value) || 0))), style: { ...Ae, width: "58px" }, title: "X-axis label angle (0–90°)" }),
                              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.textSec }, children: "°" }),
                              [0, 30, 45, 90].map((d) => /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => Pe(d), style: { ...Ae, padding: "4px 6px", background: lt === d ? s.select : s.headerSubtleBg || s.hover, color: lt === d ? s.primary : s.text, border: `1px solid ${lt === d ? s.primary : s.border}` }, children: [
                                d,
                                "°"
                              ] }, d))
                            ] }) })
                          ] }) : null,
                          xe ? /* @__PURE__ */ e.jsx(pe, { label: "Right Axis Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: o, onChange: (d) => c(d.target.value), placeholder: "Min", style: { ...Ae, width: "50%" } }),
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: l, onChange: (d) => u(d.target.value), placeholder: "Max", style: { ...Ae, width: "50%" } })
                          ] }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && !ut.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Reference Lines" }),
                            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: [
                              $e.map((d, Q) => /* @__PURE__ */ e.jsx(Kn, { line: d, onUpdate: (ge) => Xe((ye) => ye.map((Ve, Jt) => Jt === Q ? ge : Ve)), onRemove: () => Xe((ge) => ge.filter((ye, Ve) => Ve !== Q)), exportButtonStyle: Yt, compactInputStyle: Ae, theme: s }, d.id)),
                              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => gt((d) => ({ ...d, orient: d.orient === "v" ? "h" : "v" })), style: { ...Yt, padding: "4px 7px", flexShrink: 0, fontWeight: 800, color: Ge.orient === "v" ? s.primary : s.textSec }, children: Ge.orient === "v" ? "|" : "—" }),
                                /* @__PURE__ */ e.jsx("input", { type: "number", value: Ge.value, onChange: (d) => gt((Q) => ({ ...Q, value: d.target.value })), placeholder: Ge.orient === "v" ? "Cat index" : "Value", style: { ...Ae, width: "60px", flexShrink: 0 } }),
                                /* @__PURE__ */ e.jsx("input", { type: "text", value: Ge.label, onChange: (d) => gt((Q) => ({ ...Q, label: d.target.value })), placeholder: "Label", style: { ...Ae, flex: 1, minWidth: 0 } }),
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => {
                                  Ge.value !== "" && (Xe((d) => [...d, { id: Date.now(), value: Ge.value, label: Ge.label, orient: Ge.orient, color: "", dash: "dashed" }]), gt({ value: "", label: "", orient: Ge.orient }));
                                }, style: { ...Yt, padding: "4px 10px", flexShrink: 0 }, children: "+" })
                              ] }),
                              Mr ? /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: s.textSec }, children: [
                                "Data range: ",
                                Ct(Mr.min, at),
                                " — ",
                                Ct(Mr.max, at)
                              ] }) : null
                            ] })
                          ] }) : null
                        ] }),
                        p === "style" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}` }, children: "Colors" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" }, children: [...hn, ...Mt.length >= 2 ? ["custom"] : []].map((d) => {
                            const Q = me === d, ge = d === "custom" ? Mt : gn[d];
                            return /* @__PURE__ */ e.jsxs(
                              "button",
                              {
                                type: "button",
                                onClick: () => ht(d),
                                title: d.charAt(0).toUpperCase() + d.slice(1),
                                style: { display: "flex", alignItems: "center", gap: "3px", padding: "3px 6px", border: `1.5px solid ${Q ? s.primary : s.border}`, background: Q ? s.select : s.headerSubtleBg || s.hover, borderRadius: s.radiusSm || "8px", cursor: "pointer" },
                                children: [
                                  (ge || []).slice(0, 4).map((ye, Ve) => /* @__PURE__ */ e.jsx("span", { style: { width: "8px", height: "8px", borderRadius: "50%", background: ye, flexShrink: 0 } }, Ve)),
                                  /* @__PURE__ */ e.jsx("span", { style: { fontSize: "9px", fontWeight: Q ? 700 : 600, color: Q ? s.primary : s.textSec, marginLeft: "1px", textTransform: "capitalize" }, children: d === "a11y" ? "A11y" : d })
                                ]
                              },
                              d
                            );
                          }) }),
                          Array.isArray(t && t.series) && t.series.length > 0 ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px" }, children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: s.textSec, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "2px" }, children: "Override per series" }),
                            t.series.slice(0, 20).map((d, Q) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "5px" }, children: [
                              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "2px", flexShrink: 0 }, children: I.slice(0, 6).map((ge, ye) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Lt((Ve) => ({ ...Ve, [d.name]: ge })), style: { width: "14px", height: "14px", borderRadius: "50%", background: ge, border: `2px solid ${(Fe[d.name] || Ce(Q, s, I)) === ge ? s.text : "transparent"}`, cursor: "pointer", padding: 0, flexShrink: 0 }, title: ge }, ye)) }),
                              /* @__PURE__ */ e.jsx("input", { type: "color", value: Fe[d.name] || Ce(Q, s, I), onChange: (ge) => Lt((ye) => ({ ...ye, [d.name]: ge.target.value })), style: { width: "24px", height: "24px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none", flexShrink: 0 }, title: `Custom color for ${d.name}` }),
                              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: d.name || `Series ${Q + 1}` }),
                              Fe[d.name] ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Lt((ge) => {
                                const ye = { ...ge };
                                return delete ye[d.name], ye;
                              }), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 2px" }, title: "Reset to palette", children: "↺" }) : null
                            ] }, d.name || Q)),
                            t.series.length > 20 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: s.textSec }, children: `…${t.series.length - 20} more` }) : null
                          ] }) : null,
                          /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${s.border}`, paddingTop: "8px" }, children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: s.textSec, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "6px" }, children: "Custom Palette" }),
                            /* @__PURE__ */ e.jsx(Nn, { customColors: Mt, onChange: Zt, theme: s, compactInputStyle: Ae }),
                            Mt.length >= 2 ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ht("custom"), style: { ...Yt, marginTop: "6px", background: me === "custom" ? s.select : void 0, color: me === "custom" ? s.primary : void 0, border: `1px solid ${me === "custom" ? s.primary : s.border}` }, children: me === "custom" ? "Custom active" : "Use custom palette" }) : null
                          ] }),
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Effects" }),
                          /* @__PURE__ */ e.jsx(pe, { label: "Animate", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => wt((d) => !d), style: { border: `1px solid ${dt ? s.primary : s.border}`, background: dt ? s.select : s.headerSubtleBg || s.hover, color: dt ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: dt ? "On" : "Off" }) }),
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(pe, { label: "Gradient", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ct((d) => !d), style: { border: `1px solid ${Te ? s.primary : s.border}`, background: Te ? s.select : s.headerSubtleBg || s.hover, color: Te ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: Te ? "On" : "Off" }) }),
                            Te ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(pe, { label: "Grad Dir", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: $r.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => br(d.value), style: { border: `1px solid ${Rt === d.value ? s.primary : s.border}`, background: Rt === d.value ? s.select : s.headerSubtleBg || s.hover, color: Rt === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }),
                              /* @__PURE__ */ e.jsx(pe, { label: "Grad Opacity", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "range", min: "10", max: "100", step: "5", value: Dt, onChange: (d) => vr(Number(d.target.value)), style: { flex: 1 } }),
                                /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "11px", color: s.textSec, minWidth: "30px" }, children: [
                                  Dt,
                                  "%"
                                ] })
                              ] }) })
                            ] }) : null
                          ] }) : null,
                          n === "line" || n === "scatter" || n === "bubble" || n === "area" || n === "radar" || xe ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Markers" }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Shape", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: Ze, onChange: (d) => yt(d.target.value), style: { ...Ae, width: "100%" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: "none", children: "None (line only)" }),
                              /* @__PURE__ */ e.jsx("option", { value: "circle", children: "Circle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "square", children: "Square" }),
                              /* @__PURE__ */ e.jsx("option", { value: "diamond", children: "Diamond" }),
                              /* @__PURE__ */ e.jsx("option", { value: "triangle", children: "Triangle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "cross", children: "Cross" })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Size", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs("select", { value: bt, onChange: (d) => rt(Number(d.target.value)), style: { ...Ae, width: "80px" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: 3, children: "Small" }),
                              /* @__PURE__ */ e.jsx("option", { value: 5, children: "Medium" }),
                              /* @__PURE__ */ e.jsx("option", { value: 7, children: "Large" }),
                              /* @__PURE__ */ e.jsx("option", { value: 10, children: "XLarge" })
                            ] }) })
                          ] }) : null,
                          (n === "line" || n === "area" || xe) && Array.isArray(t && t.series) && t.series.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Line Style" }),
                            /* @__PURE__ */ e.jsx(Dn, { series: t.series, lineDashStyles: Ot, onChange: jr, seriesColors: Fe, paletteColors: I, getColorForIndex: Ce, theme: s, compactInputStyle: Ae })
                          ] }) : null,
                          n === "bar" || n === "line" || n === "area" || n === "scatter" || xe ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Trendline" }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Show", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Ue((d) => !d), style: { border: `1px solid ${Be ? s.primary : s.border}`, background: Be ? s.select : s.headerSubtleBg || s.hover, color: Be ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: Be ? "On" : "Off" }) }),
                            Be ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(pe, { label: "Type", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: we, onChange: (d) => Ye(d.target.value), style: { ...Ae, width: "100%" }, children: [
                                /* @__PURE__ */ e.jsx("option", { value: "linear", children: "Linear Regression" }),
                                /* @__PURE__ */ e.jsx("option", { value: "moving_avg", children: "Moving Average" }),
                                /* @__PURE__ */ e.jsx("option", { value: "exponential", children: "Exponential Smoothing" })
                              ] }) }),
                              we === "moving_avg" ? /* @__PURE__ */ e.jsx(pe, { label: "Period", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "50", value: Je, onChange: (d) => ir(Math.max(2, Math.min(50, Number(d.target.value) || 3))), style: { ...Ae, width: "80px" } }) }) : null,
                              /* @__PURE__ */ e.jsxs(pe, { label: "Color", theme: s, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "color", value: Re || "#888888", onChange: (d) => x(d.target.value), style: { width: "32px", height: "28px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }, title: "Trendline color" }),
                                Re ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => x(""), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 4px" }, title: "Reset to auto", children: "↺" }) : null
                              ] }),
                              /* @__PURE__ */ e.jsx(pe, { label: "Width", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "1", max: "8", value: Nt, onChange: (d) => mr(Math.max(1, Math.min(8, Number(d.target.value) || 2))), style: { ...Ae, width: "80px" } }) })
                            ] }) : null
                          ] }) : null
                        ] }),
                        p === "labels" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(pe, { label: "Data Labels", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => zt((d) => !d), style: { border: `1px solid ${ve ? s.primary : s.border}`, background: ve ? s.select : s.headerSubtleBg || s.hover, color: ve ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: ve ? "On" : "Off" }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" ? /* @__PURE__ */ e.jsx(pe, { label: "Format", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("select", { value: at, onChange: (d) => He(d.target.value), style: { ...Ae, width: "100%" }, title: "Value format for axis labels", children: yn.map((d) => /* @__PURE__ */ e.jsx("option", { value: d.value, children: d.label }, d.value)) }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsx(pe, { label: "Decimals", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: Ut, onChange: (d) => Kt(Number(d.target.value)), style: { ...Ae, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: -1, children: "Auto" }),
                            /* @__PURE__ */ e.jsx("option", { value: 0, children: "0  (e.g. 1,234)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 1, children: "1  (e.g. 1.2)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 2, children: "2  (e.g. 1.23)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 3, children: "3  (e.g. 1.234)" })
                          ] }) }) : null,
                          ve && n === "bar" ? /* @__PURE__ */ e.jsx(pe, { label: "Position", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("select", { value: Et, onChange: (d) => Tt(d.target.value), style: { ...Ae, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: "outside", children: "Outside" }),
                            /* @__PURE__ */ e.jsx("option", { value: "inside", children: "Inside top" }),
                            /* @__PURE__ */ e.jsx("option", { value: "center", children: "Center" })
                          ] }) }) : null,
                          n === "pie" || n === "donut" ? /* @__PURE__ */ e.jsx(pe, { label: "Callouts", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => te((d) => !d), style: { border: `1px solid ${We ? s.primary : s.border}`, background: We ? s.select : s.headerSubtleBg || s.hover, color: We ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: We ? "On" : "Off" }) }) : null,
                          n !== "sparkline" && !ut.has(n) ? /* @__PURE__ */ e.jsx(pe, { label: "Legend", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(Bn, { legendPosition: vt, onChange: lr, theme: s }) }) : null
                        ] })
                      ]
                    }
                  )
                ]
              }
            )
          ] }) : null
        ] }),
        De ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap", alignItems: "center" }, children: [
          /* @__PURE__ */ e.jsx(ss, { model: t, theme: s }),
          /* @__PURE__ */ e.jsx(Rn, { model: t, maxCategories: Kr, maxSeries: Ur, theme: s })
        ] }) : null
      ]
    }
  );
}, Ss = ({
  onClose: t,
  source: n,
  onSourceChange: i,
  chartType: a,
  onChartTypeChange: r,
  chartLayers: f = [],
  onChartLayersChange: m,
  availableColumns: M = [],
  barLayout: W,
  onBarLayoutChange: R,
  axisMode: $,
  onAxisModeChange: O,
  orientation: J,
  onOrientationChange: K,
  hierarchyLevel: s,
  onHierarchyLevelChange: _,
  model: T,
  theme: V,
  width: Y,
  chartTitle: F,
  onChartTitleChange: U,
  chartHeight: de,
  onChartHeightChange: ne,
  onResizeStart: ae,
  floating: ue = !1,
  onToggleFloating: nt,
  floatingRect: be = null,
  onFloatingDragStart: st,
  onFloatingResizeStart: _e,
  rowLimit: Ht,
  onRowLimitChange: ot,
  columnLimit: qe,
  onColumnLimitChange: Oe,
  sortMode: Me,
  onSortModeChange: Vt,
  interactionMode: sr,
  onInteractionModeChange: et,
  serverScope: _t,
  onServerScopeChange: Xt,
  chartDefinitions: Qe = [],
  activeChartId: pt = null,
  onActiveChartChange: Gt,
  onCreateChart: B,
  onDuplicateChart: $t,
  onDeleteChart: xe,
  onRenameChart: je,
  onCategoryActivate: P,
  allowHierarchyCharts: Ie = !0,
  showServerScope: Wt = !1,
  locked: ft = !1,
  onToggleLock: it,
  dockPosition: Ne = "right",
  onDockPositionChange: ke,
  immersiveMode: z,
  onImmersiveModeChange: ve,
  onSettingsWidthBudgetChange: zt,
  standalone: me = !1,
  showResizeHandle: ht = !0,
  title: at = "Chart Panel",
  showDefinitionManager: He = !0,
  initialSettings: kt = {},
  onSettingsChange: or,
  initialSettingsPaneOpen: Z = !1
}) => {
  bn("PivotChartPanel", pt);
  const [ee, Bt] = v.useState(!1), [Ke, $e] = v.useState(!1), [Xe, Ge] = v.useState(Z), [gt, lt] = v.useState(348), Pe = v.useRef(null), Le = v.useRef(_r(
    Y,
    430,
    pr,
    Or
  )), Ft = v.useRef(zt), tt = v.useRef(null), Ee = typeof z == "boolean" ? z : Ke, Ze = () => {
    const te = !Ee;
    if (typeof ve == "function") {
      ve(te);
      return;
    }
    $e(te);
  }, yt = ue && ft, bt = (te) => ({
    border: `1px solid ${te === n ? V.primary : V.border}`,
    background: te === n ? V.select : V.headerSubtleBg || V.hover,
    color: te === n ? V.primary : V.text,
    borderRadius: V.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }), rt = (te = !1) => ({
    border: `1px solid ${te ? V.primary : V.border}`,
    background: te ? V.select : V.headerSubtleBg || V.hover,
    color: te ? V.primary : V.text,
    borderRadius: V.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "6px",
    minWidth: "32px"
  }), Te = (te) => ({
    ...rt(Ne === te),
    minWidth: "56px",
    padding: "6px 10px"
  }), ct = !Ee && Xe ? gt + 9 : 0, Fe = _r(
    Y,
    430,
    pr,
    Or
  ), Lt = be && Number.isFinite(Number(be.width)) ? Math.max(pr, Math.floor(Number(be.width))) : Fe, dt = be && Number.isFinite(Number(be.height)) ? Math.max(vn, Math.floor(Number(be.height))) : Math.max(mn, (Number(de) || nr) + Tr);
  v.useEffect(() => {
    Ft.current = zt;
  }, [zt]);
  const wt = v.useCallback((te, Be = {}) => {
    const Ue = Number(te), we = Number.isFinite(Ue) ? Math.max(320, Math.ceil(Ue)) : null;
    if (!Be.force && tt.current === we) return;
    tt.current = we;
    const Ye = Ft.current;
    typeof Ye == "function" && Ye(we);
  }, []), We = ee ? {
    position: "fixed",
    inset: "16px",
    zIndex: 10040,
    display: "flex",
    minWidth: 0,
    minHeight: 0
  } : ue ? {
    position: "absolute",
    left: `${be && be.left || 24}px`,
    top: `${be && be.top || 24}px`,
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
    if (!me || ue || ee) return;
    const te = Pe.current;
    if (!te) return;
    const Be = () => {
      const we = te.clientWidth;
      we > 0 && !Xe && (Le.current = Math.max(pr, we));
    };
    if (Be(), typeof ResizeObserver > "u") return;
    const Ue = new ResizeObserver(Be);
    return Ue.observe(te), () => Ue.disconnect();
  }, [ue, ee, Xe, me]), v.useEffect(() => {
    if (!me || ue || ee || Ee || !Xe) {
      wt(null);
      return;
    }
    const te = Le.current || Fe;
    wt(te + ct);
  }, [
    Fe,
    wt,
    Ee,
    ue,
    ee,
    Xe,
    me,
    ct
  ]), v.useEffect(() => () => {
    wt(null, { force: !0 });
  }, [wt]), /* @__PURE__ */ e.jsxs("div", { style: We, children: [
    !ee && !ue && !me && ht ? /* @__PURE__ */ e.jsx(
      "div",
      {
        onMouseDown: (te) => {
          te.preventDefault(), ae && ae();
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
          background: V.border,
          opacity: 0.9
        } })
      }
    ) : null,
    /* @__PURE__ */ e.jsxs("aside", { ref: Pe, style: {
      width: ee || ue || me ? "100%" : `calc(100% - ${ht ? 8 : 0}px)`,
      height: ee || ue || me ? "100%" : "auto",
      minWidth: 0,
      minHeight: 0,
      borderLeft: me || ue || ee ? "none" : `1px solid ${V.border}`,
      border: ue || ee ? `1px solid ${V.border}` : "none",
      borderRadius: ue || ee ? V.radius || "16px" : 0,
      background: V.sidebarBg || V.surfaceBg || V.background,
      padding: ee ? "12px" : "14px",
      overflow: "hidden",
      boxShadow: ue || ee ? V.shadowMd || "0 18px 40px rgba(15,23,42,0.24)" : me ? "none" : `-10px 0 24px ${V.pinnedBoundaryShadow || "rgba(15,23,42,0.12)"}`,
      display: "flex",
      flexDirection: "column",
      gap: Ee ? "8px" : "0",
      position: "relative"
    }, children: [
      /* @__PURE__ */ e.jsxs(
        "div",
        {
          onMouseDown: (te) => {
            ue && !ee && !yt && typeof st == "function" && st(te);
          },
          style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px", marginBottom: Ee ? "0" : "12px", cursor: ue && !ee && !yt ? "move" : "default" },
          children: [
            Ee ? /* @__PURE__ */ e.jsx("div", {}) : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "4px", minWidth: 0 }, children: /* @__PURE__ */ e.jsx("div", { style: { fontSize: "15px", fontWeight: 800, color: V.text }, children: at }) }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, onMouseDown: (te) => te.stopPropagation(), children: [
              typeof nt == "function" ? /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: nt,
                  style: rt(ue),
                  title: ue ? "Dock chart panel" : "Float chart over table",
                  "aria-label": ue ? "Dock chart panel" : "Float chart over table",
                  children: ue ? "Dock" : "Float"
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
                  onClick: () => Bt((te) => !te),
                  style: rt(ee),
                  title: ee ? "Exit full screen" : "Open chart full screen",
                  "aria-label": ee ? "Exit full screen" : "Open chart full screen",
                  children: ee ? /* @__PURE__ */ e.jsx(rr.FullscreenExit, {}) : /* @__PURE__ */ e.jsx(rr.Fullscreen, {})
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: t,
                  style: rt(!1),
                  title: me ? "Close chart pane" : "Close chart panel",
                  "aria-label": me ? "Close chart pane" : "Close chart panel",
                  children: ee ? /* @__PURE__ */ e.jsx(rr.Close, {}) : "Close"
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
              onChange: (te) => {
                typeof Gt == "function" && Gt(te.target.value);
              },
              style: {
                border: `1px solid ${V.border}`,
                background: V.surfaceBg || V.background || "#fff",
                color: V.text,
                borderRadius: V.radiusSm || "8px",
                padding: "6px 8px",
                fontSize: "11px",
                fontWeight: 700,
                minWidth: "160px"
              },
              children: Qe.map((te) => /* @__PURE__ */ e.jsx("option", { value: te.id, children: te.name || te.id }, te.id))
            }
          ),
          /* @__PURE__ */ e.jsx(
            "input",
            {
              type: "text",
              value: (Qe.find((te) => te.id === pt) || {}).name || "",
              onChange: (te) => {
                typeof je == "function" && je(te.target.value);
              },
              placeholder: "Chart name",
              style: {
                border: `1px solid ${V.border}`,
                background: V.surfaceBg || V.background || "#fff",
                color: V.text,
                borderRadius: V.radiusSm || "8px",
                padding: "6px 8px",
                fontSize: "11px",
                fontWeight: 700,
                minWidth: "180px"
              }
            }
          )
        ] }) : null,
        typeof B == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: rt(!1), onClick: B, children: "New" }) : null,
        typeof $t == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: rt(!1), onClick: $t, children: "Duplicate" }) : null,
        typeof xe == "function" ? /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            style: rt(!1),
            onClick: xe,
            disabled: !Array.isArray(Qe) || Qe.length <= 1,
            children: "Delete"
          }
        ) : null
      ] }) : null,
      !Ee && me && !ue && typeof ke == "function" ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "10px", flexWrap: "wrap", alignItems: "center" }, children: [
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("left"), style: Te("left"), title: "Dock chart pane to the left", children: "Left" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("right"), style: Te("right"), title: "Dock chart pane to the right", children: "Right" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("top"), style: Te("top"), title: "Dock chart pane to the top", children: "Top" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("bottom"), style: Te("bottom"), title: "Dock chart pane to the bottom", children: "Bottom" })
      ] }) : null,
      Ee ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "12px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ e.jsx("button", { style: bt("pivot"), onClick: () => i("pivot"), children: "Pivot View" }),
        /* @__PURE__ */ e.jsx("button", { style: bt("selection"), onClick: () => i("selection"), children: "Selection" })
      ] }),
      /* @__PURE__ */ e.jsx(
        en,
        {
          model: T,
          chartType: a,
          chartLayers: f,
          onChartLayersChange: m,
          availableColumns: M,
          barLayout: W,
          axisMode: $,
          onChartTypeChange: r,
          onBarLayoutChange: R,
          onAxisModeChange: O,
          orientation: J,
          onOrientationChange: K,
          hierarchyLevel: s,
          onHierarchyLevelChange: _,
          rowLimit: Ht,
          onRowLimitChange: ot,
          columnLimit: qe,
          onColumnLimitChange: Oe,
          title: F,
          onTitleChange: U,
          chartHeight: de,
          onChartHeightChange: ne,
          sortMode: Me,
          onSortModeChange: Vt,
          interactionMode: sr,
          onInteractionModeChange: et,
          serverScope: _t,
          onServerScopeChange: Xt,
          showServerScope: Wt,
          theme: V,
          immersiveMode: Ee,
          onCategoryActivate: P,
          allowHierarchyCharts: Ie,
          floating: ue,
          locked: ft,
          onToggleLock: it,
          configOpen: Xe,
          onConfigChange: Ge,
          settingsPanelWidth: gt,
          onSettingsPanelWidthChange: lt,
          initialSettings: kt,
          onSettingsChange: or
        }
      ),
      ue && !ee && !ft ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (te) => typeof _e == "function" && _e("right", te),
            style: { position: "absolute", top: 0, right: 0, width: "10px", height: "100%", cursor: "ew-resize" }
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (te) => typeof _e == "function" && _e("bottom", te),
            style: { position: "absolute", left: 0, bottom: 0, width: "100%", height: "10px", cursor: "ns-resize" }
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (te) => typeof _e == "function" && _e("corner", te),
            style: { position: "absolute", right: 0, bottom: 0, width: "18px", height: "18px", cursor: "nwse-resize" }
          }
        )
      ] }) : null
    ] })
  ] });
}, Cs = (t) => t.open ? /* @__PURE__ */ e.jsx(Ss, { ...t }) : null, $s = ({ chartState: t, onClose: n, theme: i, position: a = "right", onPositionChange: r }) => {
  const [f, m] = v.useState(t && t.chartType ? t.chartType : "bar"), [M, W] = v.useState(t && t.barLayout ? t.barLayout : "grouped"), [R, $] = v.useState(t && t.axisMode ? t.axisMode : "vertical"), [O, J] = v.useState(t && t.defaultOrientation ? t.defaultOrientation : "rows"), [K, s] = v.useState(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), [_, T] = v.useState(t && t.rowLimit ? t.rowLimit : Hr), [V, Y] = v.useState(t && t.columnLimit ? t.columnLimit : Fr), [F, U] = v.useState(t && t.sortMode ? t.sortMode : "natural"), [de, ne] = v.useState(t && Array.isArray(t.chartLayers) ? t.chartLayers : []), [ae, ue] = v.useState(null);
  v.useEffect(() => {
    m(t && t.chartType ? t.chartType : "bar"), W(t && t.barLayout ? t.barLayout : "grouped"), $(t && t.axisMode ? t.axisMode : "vertical"), J(t && t.defaultOrientation ? t.defaultOrientation : "rows"), s(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), T(t && t.rowLimit ? t.rowLimit : Hr), Y(t && t.columnLimit ? t.columnLimit : Fr), U(t && t.sortMode ? t.sortMode : "natural"), ne(t && Array.isArray(t.chartLayers) ? t.chartLayers : []);
  }, [t]);
  const nt = v.useMemo(() => {
    if (!t || !Array.isArray(t.visibleCols)) return [];
    if (!(t.selectionMap && t.visibleRows))
      return Cr(t.visibleCols);
    const qe = dn(t.selectionMap, t.visibleRows, t.visibleCols);
    if (!qe) return [];
    const Oe = new Set(
      (qe.colIndexes || []).map((Me) => t.visibleCols[Me]).filter((Me) => Me && Me.id && !un.has(Me.id)).map((Me) => Me.id)
    );
    return Cr(t.visibleCols).filter((Me) => Oe.has(Me.id));
  }, [t]), be = v.useMemo(() => t ? t.selectionMap && t.visibleRows && t.visibleCols ? f === "combo" ? xn(t.selectionMap, t.visibleRows, t.visibleCols, {
    hierarchyLevel: K,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: _,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: F,
    layers: de
  }) : pn(t.selectionMap, t.visibleRows, t.visibleCols, {
    orientation: O,
    hierarchyLevel: K,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: _,
    maxColumns: V,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: F
  }) : t.model : null, [de, t, f, K, O, _, V, F]);
  if (v.useEffect(() => {
    if (M !== "stacked") return;
    if (f === "combo") {
      W("grouped");
      return;
    }
    (f === "area" ? be && Array.isArray(be.series) && be.series.length > 1 : Gr(be)) || W("grouped");
  }, [be, M, f]), !t) return null;
  const st = a === "top" || a === "bottom", _e = Math.max(420, Number.isFinite(Number(ae)) ? Math.floor(Number(ae)) : 420), Ht = Math.max(360, Number(t.chartHeight || nr) + Tr), ot = (qe, Oe) => ({
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
          en,
          {
            model: be,
            chartType: f,
            chartLayers: de,
            onChartLayersChange: ne,
            availableColumns: nt,
            barLayout: M,
            axisMode: R,
            onChartTypeChange: m,
            onBarLayoutChange: W,
            onAxisModeChange: $,
            orientation: O,
            onOrientationChange: J,
            hierarchyLevel: K,
            onHierarchyLevelChange: s,
            title: t.title || "Range Chart",
            rowLimit: _,
            onRowLimitChange: T,
            columnLimit: V,
            onColumnLimitChange: Y,
            sortMode: F,
            onSortModeChange: U,
            theme: i,
            allowHierarchyCharts: !(t.selectionMap && t.visibleRows && t.visibleCols),
            onSettingsWidthBudgetChange: ue
          }
        )
      ] })
    }
  );
};
export {
  $s as PivotChartModal,
  Cs as PivotChartPanel
};
