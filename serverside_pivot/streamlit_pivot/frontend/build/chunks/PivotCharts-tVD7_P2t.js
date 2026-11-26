import { j as e, I as tt, T as qe, C as sr, r as Tr, c as an, a as Ar, n as Ft, e as $t, t as he, l as ln, g as Ae, b as cn, d as Nr, f as dn, p as lr, h as Pr, i as Ir, k as pr, m as Hr, o as Wt, M as Fr, q as Or, s as $r, u as un, v as xn, w as pn, x as fn, y as Gr, z as Ur, A as hn, B as gn, D as yn, V as bn, E as Kr, F as Zr, G as vn, H as fr, J as mn, K as jn, L as kn, N as wn, O as Yr, P as Sn, Q as Mn, R as Cn, S as Vr, U as An } from "./index-BvjmMxCj.js";
import { a as v, R as ft } from "./charts-DthW1Q8W.js";
function $n(t, n, i) {
  const h = Math.min(n.width, Math.max(0, i.width - 16)), m = Math.max(0, i.height - 8 - t.bottom - 6), M = Math.max(0, t.top - 6 - 8), W = n.height > m && M > m, R = W ? M : m, $ = Math.min(n.height, R);
  return {
    left: Math.max(8, Math.min(t.right - h, i.width - 8 - h)),
    top: W ? t.top - 6 - $ : t.bottom + 6,
    maxWidth: Math.max(0, i.width - 16),
    maxHeight: R
  };
}
function Wn(t, n) {
  var W;
  const i = v.useRef(null), a = v.useRef(null), [r, h] = v.useState(null), m = (W = i.current) == null ? void 0 : W.ownerDocument, M = (m == null ? void 0 : m.fullscreenElement) || (m == null ? void 0 : m.body);
  return v.useLayoutEffect(() => {
    if (!t || !i.current || !a.current) return;
    const R = i.current, $ = a.current, O = R.ownerDocument, J = O.defaultView, K = () => h($n(
      R.getBoundingClientRect(),
      { width: $.offsetWidth, height: $.scrollHeight + 2 },
      { width: O.documentElement.clientWidth, height: O.documentElement.clientHeight }
    )), s = () => Array.from($.querySelectorAll('[role="menuitem"]:not(:disabled)')), _ = (F = !1) => {
      n(!1), h(null), F && R.focus();
    }, G = (F) => {
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
    K(), O.addEventListener("mousedown", G, !0), O.addEventListener("keydown", V, !0), O.addEventListener("scroll", K, !0), J.addEventListener("resize", K);
    const Y = typeof J.ResizeObserver == "function" ? new J.ResizeObserver(K) : null;
    return Y == null || Y.observe(R), Y == null || Y.observe($), () => {
      O.removeEventListener("mousedown", G, !0), O.removeEventListener("keydown", V, !0), O.removeEventListener("scroll", K, !0), J.removeEventListener("resize", K), Y == null || Y.disconnect();
    };
  }, [t, M, n]), v.useLayoutEffect(() => {
    var R, $;
    t && r && (($ = (R = a.current) == null ? void 0 : R.querySelector('[role="menuitem"]:not(:disabled)')) == null || $.focus());
  }, [t, !!r]), { anchorRef: i, menuRef: a, position: r, portalTarget: M };
}
const zr = [
  { value: "solid", label: "Solid", dash: "none" },
  { value: "dashed", label: "Dashed", dash: "8 4" },
  { value: "dotted", label: "Dotted", dash: "2 4" },
  { value: "dot-dash", label: "Dot-Dash", dash: "8 4 2 4" }
], hr = (t) => {
  const n = zr.find((i) => i.value === t);
  return n && n.dash !== "none" ? n.dash : void 0;
}, Wr = [
  { value: "vertical", label: "Vertical", x1: "0", y1: "0", x2: "0", y2: "1" },
  { value: "horizontal", label: "Horizontal", x1: "0", y1: "0", x2: "1", y2: "0" },
  { value: "diagonal", label: "Diagonal", x1: "0", y1: "0", x2: "1", y2: "1" }
], zn = (t, n) => {
  const i = (Number(n) || 70) / 100;
  return [
    { offset: "0%", stopOpacity: String(Math.min(1, i + 0.2)) },
    { offset: "100%", stopOpacity: String(i) }
  ];
}, Ln = ["bottom", "top", "none"], Bn = ({ legendPosition: t, onChange: n, theme: i }) => {
  const a = (r, h) => ({
    border: `1px solid ${r === t ? i.primary : i.border}`,
    background: r === t ? i.select : i.headerSubtleBg || i.hover,
    color: r === t ? i.primary : i.text,
    borderRadius: i.radiusSm || "8px",
    padding: "5px 10px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  });
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: Ln.map((r) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => n(r), style: a(r), children: r.charAt(0).toUpperCase() + r.slice(1) }, r)) });
}, En = [
  { value: "none", label: "Natural" },
  { value: "value_desc", label: "Val ↓" },
  { value: "value_asc", label: "Val ↑" },
  { value: "alpha_asc", label: "A→Z" },
  { value: "alpha_desc", label: "Z→A" }
], Rn = (t, n) => {
  if (!Array.isArray(t) || n === "none" || !n) return t;
  const i = [...t];
  if (n === "alpha_asc") return i.sort((r, h) => String(r.name || "").localeCompare(String(h.name || "")));
  if (n === "alpha_desc") return i.sort((r, h) => String(h.name || "").localeCompare(String(r.name || "")));
  const a = (r) => (r.values || []).reduce(
    /** @param {any} [acc] @param {any} [v] @returns {any} */
    (h, m) => h + (Number.isFinite(Number(m)) ? Number(m) : 0),
    0
  );
  return n === "value_desc" ? i.sort((r, h) => a(h) - a(r)) : n === "value_asc" ? i.sort((r, h) => a(r) - a(h)) : i;
}, Dn = ({ model: t, maxCategories: n, maxSeries: i, theme: a }) => {
  const r = Array.isArray(t && t.categories) ? t.categories.length : 0, h = Array.isArray(t && t.series) ? t.series.length : 0, m = r >= n * 0.9, M = h >= i * 0.9, W = r >= n, R = h >= i;
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
      h,
      "/",
      i,
      " series"
    ] })
  ] });
}, Nn = ({ series: t, lineDashStyles: n, onChange: i, seriesColors: a, paletteColors: r, getColorForIndex: h, theme: m, compactInputStyle: M }) => !Array.isArray(t) || t.length === 0 ? null : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: t.slice(0, 16).map((W, R) => {
  const $ = a && a[W.name] || h(R, m, r), O = n && n[W.name] || "solid";
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, children: [
    /* @__PURE__ */ e.jsx("span", { style: { width: "10px", height: "10px", borderRadius: "50%", background: $, flexShrink: 0 } }),
    /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: m.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: W.name || `Series ${R + 1}` }),
    /* @__PURE__ */ e.jsx(
      qe,
      {
        size: "sm",
        "aria-label": `${W.name || "Series"} dash style`,
        value: O,
        onChange: (J) => i({ ...n, [W.name]: J.target.value }),
        style: { ...M, width: "88px", flexShrink: 0 },
        children: zr.map((J) => /* @__PURE__ */ e.jsx("option", { value: J.value, children: J.label }, J.value))
      }
    )
  ] }, W.name || R);
}) }), Pn = ({ customColors: t, onChange: n, theme: i, compactInputStyle: a }) => {
  const r = Array.isArray(t) && t.length > 0 ? t : ["#2563EB", "#F97316", "#0F766E", "#7C3AED", "#DC2626", "#0891B2"], h = (W, R) => {
    const $ = [...r];
    $[W] = R, n($);
  }, m = () => n([...r, "#888888"]), M = (W) => n(r.filter(
    /** @param {any} [_] @param {any} [i] @returns {any} */
    (R, $) => $ !== W
  ));
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", alignItems: "center" }, children: [
    r.map(
      /** @param {any} [hex] @param {any} [i] @returns {any} */
      (W, R) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", alignItems: "center", gap: "2px" }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "color",
            value: W,
            onChange: ($) => h(R, $.target.value),
            style: { width: "28px", height: "28px", padding: 0, border: `1px solid ${i.border}`, borderRadius: "4px", cursor: "pointer", background: "none" },
            title: `Color ${R + 1}`
          }
        ),
        r.length > 2 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => M(R), title: "Remove color", style: { border: "none", background: "none", color: i.textSec, cursor: "pointer", padding: 0, lineHeight: 1, display: "inline-flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", transform: "scale(0.72)" }, children: /* @__PURE__ */ e.jsx(tt.Close, {}) }) })
      ] }, R)
    ),
    r.length < 12 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: m, style: { ...a, padding: "5px 8px", flexShrink: 0 }, children: "+" })
  ] }) });
}, ze = 16, In = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "7", width: "4", height: "8", fill: t, opacity: "0.85", rx: "0.5" })
] }), Hn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polyline", { points: "1,13 5,7 9,10 15,3", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "1", cy: "13", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "7", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "9", cy: "10", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "15", cy: "3", r: "1.4", fill: t })
] }), Fn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L8,1 A7,7 0 0,1 15,8 Z", fill: t, opacity: "0.9" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L15,8 A7,7 0 0,1 6,15 Z", fill: t, opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L6,15 A7,7 1 1,1 8,1 Z", fill: t, opacity: "0.3" })
] }), On = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("circle", { cx: "3", cy: "12", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "7", cy: "5", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "12", cy: "9", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "13", cy: "3", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "11", r: "1.8", fill: t, opacity: "0.85" })
] }), Yn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "8", width: "4", height: "7", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("polyline", { points: "1,9 5,4 9,7 15,2", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" })
] }), Vn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "4", width: "3", height: "7", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "5", y: "8", width: "3", height: "3", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "5", width: "3", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "13", y: "10", width: "3", height: "1", fill: t, opacity: "0.4", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("line", { x1: "0", y1: "15", x2: "16", y2: "15", stroke: t, strokeWidth: "0.7", opacity: "0.35" })
] }), _n = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "2", y: "6", width: "12", height: "5", fill: "none", stroke: t, strokeWidth: "1.2", rx: "1", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "8", y2: "11", stroke: t, strokeWidth: "1.2", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "5", y1: "3", x2: "5", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "11", y1: "3", x2: "11", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "3", x2: "6", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "3", x2: "12", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "13", x2: "6", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "13", x2: "12", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" })
] }), Xn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polygon", { points: "8,2 14,6 12,13 4,13 2,6", fill: "none", stroke: t, strokeWidth: "1", opacity: "0.35" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "8,5 11,7.5 9.5,11 6.5,11 5,7.5", fill: t, opacity: "0.65" })
] }), Tn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
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
] }), Un = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: ze, height: ze, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "4", y: "1", width: "8", height: "5", fill: t, opacity: "0.85", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "4", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "12", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" })
] }), _r = [
  { name: "Bar", Icon: In, types: [{ value: "bar", label: "Bar / Column" }] },
  { name: "Line / Area", Icon: Hn, types: [{ value: "line", label: "Line" }, { value: "area", label: "Area" }, { value: "sparkline", label: "Sparkline" }] },
  { name: "Pie / Donut", Icon: Fn, types: [{ value: "pie", label: "Pie" }, { value: "donut", label: "Donut" }] },
  { name: "Scatter", Icon: On, types: [{ value: "scatter", label: "Scatter" }, { value: "bubble", label: "Bubble" }] },
  { name: "Combo", Icon: Yn, types: [{ value: "combo", label: "Combo" }] },
  { name: "Waterfall", Icon: Vn, types: [{ value: "waterfall", label: "Waterfall" }, { value: "range", label: "Range" }] },
  { name: "Statistical", Icon: _n, types: [{ value: "histogram", label: "Histogram" }, { value: "boxplot", label: "Box Plot" }, { value: "funnel", label: "Funnel" }] },
  { name: "Radial", Icon: Xn, types: [{ value: "radar", label: "Radar" }, { value: "nightingale", label: "Nightingale" }] },
  { name: "Heatmap", Icon: Tn, types: [{ value: "heatmap", label: "Heatmap" }] },
  { name: "3D", Icon: Gn, types: [{ value: "bar3d", label: "3D Bar" }, { value: "line3d", label: "3D Line" }, { value: "scatter3d", label: "3D Scatter" }] },
  { name: "Hierarchical", Icon: Un, types: [{ value: "icicle", label: "Icicle" }, { value: "sunburst", label: "Sunburst" }, { value: "sankey", label: "Sankey" }], hierarchyOnly: !0 }
], Kn = ({ chartType: t, onChange: n, theme: i, includeHierarchyCharts: a = !0 }) => {
  const [r, h] = v.useState(null), m = a ? _r : _r.filter((M) => !M.hierarchyOnly);
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "3px" }, children: m.map(
    /** @param {any} p0 @returns {any} */
    ({ name: M, Icon: W, types: R }) => {
      const $ = R.some(
        /** @param {any} [t] @returns {any} */
        (_) => _.value === t
      ), O = R.find(
        /** @param {any} [t] @returns {any} */
        (_) => _.value === t
      ), J = R.length === 1, K = r === M || $, s = $ ? i.primary : i.textSec || i.text;
      return /* @__PURE__ */ e.jsxs("div", { children: [
        /* @__PURE__ */ e.jsxs(
          "button",
          {
            type: "button",
            "aria-expanded": J ? void 0 : K,
            onClick: () => {
              J ? n(R[0].value) : h(K && !$ ? null : M);
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
              !J && /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", color: s, transform: K ? "scale(0.8) rotate(0deg)" : "scale(0.8) rotate(-90deg)", transition: "transform 0.15s", flexShrink: 0 }, children: /* @__PURE__ */ e.jsx(tt.ChevronDown, {}) })
            ]
          }
        ),
        !J && K && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "3px", padding: "4px 0 2px 24px" }, children: R.map(
          /** @param {any} [t] @returns {any} */
          (_) => /* @__PURE__ */ e.jsx(
            "button",
            {
              type: "button",
              "aria-pressed": t === _.value,
              onClick: () => {
                n(_.value), h(null);
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
          )
        ) })
      ] }, M);
    }
  ) });
}, Zn = ({ line: t, onUpdate: n, onRemove: i, exportButtonStyle: a, compactInputStyle: r, theme: h }) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center", flexWrap: "wrap" }, children: [
  /* @__PURE__ */ e.jsx(
    "button",
    {
      type: "button",
      onClick: () => n({ ...t, orient: t.orient === "v" ? "h" : "v" }),
      style: { ...a, padding: "4px 7px", flexShrink: 0, fontWeight: 800, color: t.orient === "v" ? h.primary : h.textSec },
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
      value: t.color || h.primary || "#2563EB",
      onChange: (m) => n({ ...t, color: m.target.value }),
      style: { width: "28px", height: "28px", padding: 0, border: `1px solid ${h.border}`, borderRadius: "4px", cursor: "pointer", background: "none", flexShrink: 0 },
      title: "Line color"
    }
  ),
  /* @__PURE__ */ e.jsx(
    qe,
    {
      size: "sm",
      value: t.dash || "dashed",
      onChange: (m) => n({ ...t, dash: m.target.value }),
      style: { ...r, width: "80px", flexShrink: 0 },
      title: "Line style",
      children: zr.map((m) => /* @__PURE__ */ e.jsx("option", { value: m.value, children: m.label }, m.value))
    }
  ),
  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: i, title: "Remove reference line", style: { ...a, padding: "4px 7px", color: h.danger || "#ef4444", flexShrink: 0, display: "inline-flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", transform: "scale(0.8)" }, children: /* @__PURE__ */ e.jsx(tt.Close, {}) }) })
] }), Xr = (t, n = 430, i, a) => {
  const r = Number(t), h = Number(n), m = Number.isFinite(r) ? r : Number.isFinite(h) ? h : 430;
  return Math.max(i, Math.min(a, Math.floor(m)));
}, qn = (t) => {
  const n = t.map(
    /** @param {any} [value] @param {any} [index] @returns {any} */
    ($, O) => [O, Number($)]
  ).filter(
    /** @param {any} p0 @returns {any} */
    ([, $]) => Number.isFinite($)
  );
  if (n.length < 2) return null;
  const i = n.length, a = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [O]) => $ + O,
    0
  ), r = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [, O]) => $ + O,
    0
  ), h = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [O, J]) => $ + O * J,
    0
  ), m = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [O]) => $ + O * O,
    0
  ), M = i * m - a * a;
  if (M === 0) return null;
  const W = (i * h - a * r) / M, R = (r - W * a) / i;
  return t.map(
    /** @param {any} [_] @param {any} [index] @returns {any} */
    ($, O) => W * O + R
  );
}, Qn = (t, n) => {
  const i = Math.max(2, Math.floor(Number(n) || 3));
  return t.map(
    /** @param {any} [_] @param {any} [index] @returns {any} */
    (a, r) => {
      const h = t.slice(Math.max(0, r - i + 1), r + 1).map(Number).filter(Number.isFinite);
      return h.length > 0 ? h.reduce(
        /** @param {any} [sum] @param {any} [value] @returns {any} */
        (m, M) => m + M,
        0
      ) / h.length : null;
    }
  );
}, qr = (t) => Array.isArray(t && t.series) && t.series.length > 1, yr = (t, n) => {
  if (typeof window > "u" || !t) return;
  const i = window.URL.createObjectURL(t), a = document.createElement("a");
  a.href = i, a.download = n, document.body.appendChild(a), a.click(), document.body.removeChild(a), window.URL.revokeObjectURL(i);
}, br = (t = {}) => ({
  title: String(t.title || "").trim(),
  legendPosition: t.legendPosition === "top" ? "top" : "bottom",
  legendItems: Array.isArray(t.legendItems) ? t.legendItems.filter(
    /** @param {any} [item] @returns {any} */
    (n) => n && n.label
  ) : [],
  background: t.background || "#ffffff",
  textColor: t.textColor || "#111827",
  secondaryTextColor: t.secondaryTextColor || "#4b5563"
}), Jn = (t, n, i, a) => {
  const r = t ? Array.from(t.querySelectorAll('[data-chart-export-legend-item="true"]')).map((h) => ({
    label: h.getAttribute("data-chart-export-label") || h.textContent || "",
    color: h.getAttribute("data-chart-export-color") || a.primary || "#2563eb",
    hidden: h.getAttribute("data-chart-export-hidden") === "true"
  })).filter((h) => h.label) : [];
  return br({
    title: n,
    legendPosition: i,
    legendItems: r,
    background: a.chartBg || a.surfaceBg || a.background || "#ffffff",
    textColor: a.text || "#111827",
    secondaryTextColor: a.textSec || a.text || "#4b5563"
  });
}, Qr = (t, n, i) => {
  const a = [];
  let r = [], h = 0;
  return t.forEach(
    /** @param {any} [item] @returns {any} */
    (m) => {
      const M = Math.max(48, i(m.label) + 30);
      r.length > 0 && h + M > Math.max(80, n - 24) && (a.push(r), r = [], h = 0), r.push({ ...m, width: M }), h += M;
    }
  ), r.length > 0 && a.push(r), a;
}, gr = (t) => {
  const n = String(t ?? ""), i = typeof t == "string" && /^[=+\-@]/.test(n) ? `'${n}` : n;
  return i.includes(",") || i.includes('"') || i.includes(`
`) ? `"${i.replace(/"/g, '""')}"` : i;
}, es = (t, n, i) => {
  if (!t) return;
  const a = Array.isArray(t.categories) ? t.categories : [], r = Array.isArray(t.series) ? t.series : [];
  if (a.length === 0 || r.length === 0) return;
  const h = ["Category", ...r.map(
    /** @param {any} [seriesItem] @returns {any} */
    ($) => $.name
  )].map(gr).join(","), m = a.map(
    /** @param {any} [category] @param {any} [index] @returns {any} */
    ($, O) => [$, ...r.map(
      /** @param {any} [seriesItem] @returns {any} */
      (J) => J.values[O] ?? ""
    )].map(gr).join(",")
  ), M = br(i), W = M.legendItems.length > 0 ? M.legendItems.map(
    /** @param {any} [item] @returns {any} */
    ($) => $.label
  ) : r.map(
    /** @param {any} [item] @returns {any} */
    ($) => $.name
  ).filter(Boolean), R = [];
  M.title && R.push(["Title", M.title].map(gr).join(",")), W.length > 0 && R.push(["Legend", ...W].map(gr).join(",")), R.length > 0 && R.push(""), yr(new Blob([`\uFEFF${[...R, h, ...m].join(`
`)}`], { type: "text/csv;charset=utf-8" }), `${n}.csv`);
}, ts = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), rs = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d"]), ns = ft.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-2d" */
  "./ECharts2dRenderer-M8bFFwgN.js"
)), ss = ft.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-3d" */
  "./ECharts3dRenderer-CcTkusgr.js"
)), rr = ({ chartType: t, echartsRef: n, option: i }) => {
  const a = rs.has(t) ? ss : ns;
  return /* @__PURE__ */ e.jsx(ft.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { "aria-label": "Loading chart", style: { width: "100%", height: "100%" } }), children: /* @__PURE__ */ e.jsx(a, { echartsRef: n, option: i }) });
}, Se = ({ message: t, theme: n, chartHeight: i = sr }) => /* @__PURE__ */ e.jsx("div", { style: {
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
}, children: t }), os = ({ model: t, theme: n }) => {
  const i = v.useMemo(() => {
    const r = [];
    return (t && t.series ? t.series : []).forEach(
      /** @param {any} [series] @returns {any} */
      (h) => {
        (h.values || []).forEach(
          /** @param {any} [value] @returns {any} */
          (m) => {
            typeof m == "number" && Number.isFinite(m) && r.push(m);
          }
        );
      }
    ), r.length === 0 && t && Array.isArray(t.icicleNodes) && t.icicleNodes.forEach(
      /** @param {any} [node] @returns {any} */
      (h) => {
        const m = Number(h && h.value);
        Number.isFinite(m) && r.push(m);
      }
    ), r.length === 0 ? null : {
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
      Hr(i.min)
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      "Max ",
      Hr(i.max)
    ] })
  ] });
}, wt = ({ items: t, theme: n, hiddenSet: i, onToggle: a, position: r = "bottom", onElementSelect: h }) => {
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
          m && a(M.label), typeof h == "function" && h("legend", M.label);
        },
        role: "button",
        tabIndex: 0,
        onKeyDown: (R) => {
          (R.key === "Enter" || R.key === " ") && (R.preventDefault(), m && a(M.label), typeof h == "function" && h("legend", M.label));
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
}, is = (t, n, i, a = 10) => {
  const h = (Array.isArray(t) ? t.map((G) => Number.isFinite(Number(G)) ? Number(G) : null) : []).map(
    /** @param {any} [value] @param {any} [index] @returns {any} */
    (G, V) => G === null ? null : { value: G, index: V }
  ).filter(Boolean);
  if (h.length === 0)
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
  const m = Math.min(...h.map(
    /** @param {any} [point] @returns {any} */
    (G) => G.value
  )), M = Math.max(...h.map(
    /** @param {any} [point] @returns {any} */
    (G) => G.value
  )), W = Math.max(1, M - m), R = h.length > 1 ? (n - a * 2) / (h.length - 1) : 0, $ = h.map(
    /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
    (G, V) => {
      const Y = a + V * R, F = (G.value - m) / W, U = i - a - F * Math.max(1, i - a * 2);
      return {
        ...G,
        x: Y,
        y: U
      };
    }
  ), O = $.map(
    /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
    (G, V) => `${V === 0 ? "M" : "L"} ${G.x.toFixed(2)} ${G.y.toFixed(2)}`
  ).join(" "), J = $.length > 1 ? `${O} L ${$[$.length - 1].x.toFixed(2)} ${(i - a).toFixed(2)} L ${$[0].x.toFixed(2)} ${(i - a).toFixed(2)} Z` : "", K = $.find(
    /** @param {any} [point] @returns {any} */
    (G) => G.value === m
  ) || $[0], s = $.find(
    /** @param {any} [point] @returns {any} */
    (G) => G.value === M
  ) || $[0], _ = $[$.length - 1];
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
}, as = ({
  model: t,
  theme: n,
  chartHeight: i,
  paletteColors: a,
  valueFormat: r,
  referenceLines: h = []
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
      children: m.map(
        /** @param {any} [series] @param {any} [index] @returns {any} */
        (O, J) => {
          const K = Ae(J, n, a), s = is(O.values || [], R, $, 10), _ = Array.isArray(O.values) && O.values.length > 1 ? Number(O.values[O.values.length - 2]) : null, G = s.lastValue, V = Number.isFinite(_) && Number.isFinite(G) ? G - _ : null, Y = V === null ? n.textSec : V >= 0 ? n.isDark ? "#7DD3A7" : "#2E7D5B" : n.isDark ? "#F7A3A3" : "#B45353";
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
                    /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: G === null ? "—" : Wt(G, r) }),
                    /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: Y }, children: V === null ? "No delta" : `${V >= 0 ? "+" : ""}${Wt(V, r)}` })
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
                      h.map(
                        /** @param {any} [rl] @param {any} [rli] @returns {any} */
                        (F, U) => {
                          const de = Number(F.value);
                          if (!Number.isFinite(de) || F.orient === "v" || s.minValue === null || s.maxValue === null) return null;
                          const ne = s.maxValue - s.minValue || 1, ae = $ - 10 - (de - s.minValue) / ne * ($ - 20);
                          if (ae < 0 || ae > $) return null;
                          const ue = F.color || n.primary;
                          return /* @__PURE__ */ e.jsxs("g", { children: [
                            /* @__PURE__ */ e.jsx("line", { x1: 10, y1: ae, x2: R - 10, y2: ae, stroke: ue, strokeWidth: "1", strokeDasharray: "4 2", style: { pointerEvents: "none" } }),
                            F.label ? /* @__PURE__ */ e.jsx("text", { x: R - 11, y: ae - 2, textAnchor: "end", fontSize: "8", fontWeight: "700", fill: ue, style: { pointerEvents: "none" }, children: F.label }) : null
                          ] }, `spark-rl-${U}`);
                        }
                      )
                    ]
                  }
                ),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", fontSize: "10px", color: n.textSec }, children: [
                  /* @__PURE__ */ e.jsx("span", { children: M[0] ? he(String(M[0]), 14) : "Start" }),
                  /* @__PURE__ */ e.jsxs("span", { children: [
                    "Min ",
                    s.minValue === null ? "—" : Wt(s.minValue, r),
                    " · ",
                    "Max ",
                    s.maxValue === null ? "—" : Wt(s.maxValue, r)
                  ] }),
                  /* @__PURE__ */ e.jsx("span", { children: M.length > 0 ? he(String(M[M.length - 1]), 14) : "End" })
                ] })
              ]
            },
            O.id || O.name || `spark-${J}`
          );
        }
      )
    }
  );
}, ls = ({
  model: t,
  chartType: n,
  barLayout: i,
  axisMode: a,
  theme: r,
  chartHeight: h = sr,
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
  referenceLines: G = [],
  labelAngle: V = 0,
  chartTitle: Y = "",
  onTitleChange: F = null,
  tickCount: U = 5,
  markerShape: de = "circle",
  markerSize: ne = 4,
  showGradient: ae = !1,
  seriesColors: ue = {},
  showAnimations: ot = !1,
  showCalloutLabels: be = !1,
  subtitle: it = "",
  onReferenceLinesChange: _e = null,
  showTrendline: Ot = !1,
  trendlineMode: at = "linear",
  trendlinePeriod: Qe = 3,
  // new props
  legendPosition: Oe = "bottom",
  labelPosition: Me = "outside",
  decimalPlaces: Xt = -1,
  gradientDirection: or = "vertical",
  gradientOpacity: rt = 70,
  trendlineColor: Tt = "",
  trendlineWidth: Gt = 2,
  seriesSort: Je = "none",
  lineDashStyles: ht = {},
  yAxisMinRight: Ut = "",
  yAxisMaxRight: L = "",
  customPaletteColors: zt = [],
  onElementSelect: xe = null
}) => {
  const je = O === "custom" && zt.length >= 2 ? zt : Tr(O), P = (o) => {
    if (Xt >= 0 && typeof o == "number" && Number.isFinite(o)) {
      const l = Wt(o, J);
      if (!/[KMB%$]/.test(l) || J === "number")
        return o.toFixed(Xt);
    }
    return Wt(o, J);
  };
  v.useMemo(() => !t || !Array.isArray(t.series) || Je === "none" ? t : { ...t, series: Rn(t.series, Je) }, [t, Je]);
  const Ie = n === "bar" && i === "stacked" && Array.isArray(t.stackedGroups) && t.stackedGroups.length > 0, Lt = n === "bar" && i === "stacked" && !Ie && an(t), gt = n === "area" && i === "stacked" && qr(t), lt = n === "bar" && a === "horizontal", Ne = i === "stacked100", ke = Number.isFinite(Number(V)) ? Number(V) : 0, z = h, ve = (o, l) => ue && ue[o] || Ae(l, r, je), Bt = (o, l, c, u, p = {}) => {
    if (de === "none") return null;
    const g = ne || 4, k = r.surfaceBg || r.background || "#fff", f = { fill: c, stroke: k, strokeWidth: "1.5", opacity: "0.82", ...u, ...p };
    switch (de) {
      case "square":
        return /* @__PURE__ */ e.jsx("rect", { x: o - g, y: l - g, width: g * 2, height: g * 2, rx: "2", ...f });
      case "diamond":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${l - g} ${o + g},${l} ${o},${l + g} ${o - g},${l}`, ...f });
      case "cross":
        return /* @__PURE__ */ e.jsx("path", { d: `M${o - g},${l} L${o + g},${l} M${o},${l - g} L${o},${l + g}`, fill: "none", stroke: c, strokeWidth: "2.5", strokeLinecap: "round", ...u, ...p });
      case "triangle":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${l - g} ${o + g},${l + g * 0.85} ${o - g},${l + g * 0.85}`, ...f });
      default:
        return /* @__PURE__ */ e.jsx("circle", { cx: o, cy: l, r: g, ...f });
    }
  }, me = v.useMemo(() => !Ne || !t || !Array.isArray(t.categories) ? null : t.categories.map(
    /** @param {any} [_] @param {any} [catIndex] @returns {any} */
    (o, l) => {
      let c = 0;
      return (t.series || []).forEach(
        /** @param {any} [s] @returns {any} */
        (u) => {
          const p = u.values && u.values[l];
          typeof p == "number" && Number.isFinite(p) && (c += Math.abs(p));
        }
      ), c || 1;
    }
  ), [Ne, t]), yt = Array.isArray(t && t.icicleNodes) ? t.icicleNodes : [], ct = Math.max(1, Number(t && t.icicleDepth) || 1), He = v.useRef(null), [St, ir] = v.useState(Ar), Z = Math.max(Ar, Number.isFinite(St) ? Math.floor(St) : Ar);
  v.useEffect(() => {
    const o = He.current;
    if (!o) return;
    const l = () => {
      const u = o.clientWidth;
      u > 0 && ir(u);
    };
    if (l(), typeof ResizeObserver > "u") return;
    const c = new ResizeObserver(l);
    return c.observe(o), () => c.disconnect();
  }, []);
  const [ee, Et] = v.useState(/* @__PURE__ */ new Set()), Ke = v.useCallback((o) => {
    Et(
      /** @param {any} [prev] @returns {any} */
      (l) => {
        const c = new Set(l);
        return c.has(o) ? c.delete(o) : c.add(o), c;
      }
    );
  }, []), [$e, Xe] = v.useState(null), Te = v.useCallback((o) => {
    const l = o.target, c = l.getAttribute("data-tip-cat");
    if (!c) {
      Xe(
        /** @param {any} [prev] @returns {any} */
        (g) => g && null
      );
      return;
    }
    const p = o.currentTarget.getBoundingClientRect();
    Xe({
      x: o.clientX - p.left,
      y: o.clientY - p.top,
      category: c,
      series: l.getAttribute("data-tip-ser") || "",
      value: l.getAttribute("data-tip-val") || "",
      color: l.getAttribute("data-tip-color") || ""
    });
  }, []), bt = v.useCallback(() => Xe(null), []), dt = v.useCallback((o) => {
    if (!xe) return;
    let l = o.target;
    for (; l && l !== o.currentTarget; ) {
      const c = l.getAttribute && l.getAttribute("data-chart-element-type");
      if (c) {
        xe(c, l.getAttribute("data-chart-element-id") || "");
        return;
      }
      l = l.parentElement;
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
  ] }) : null, [Be, Yt] = v.useState(1), [nt, Ee] = v.useState({ x: 0, y: 0 }), Ze = v.useRef(null), [vt, mt] = v.useState(null), st = v.useCallback((o) => {
    o.preventDefault(), Yt(
      /** @param {any} [prev] @returns {any} */
      (l) => {
        const c = o.deltaY < 0 ? l * 1.15 : l / 1.15, u = Math.min(10, Math.max(1, c));
        return u === 1 && Ee({ x: 0, y: 0 }), u;
      }
    );
  }, []), Ge = v.useCallback((o) => {
    Be <= 1 || o.button !== 0 || (Ze.current = { startX: o.clientX, startY: o.clientY, startPan: { ...nt } });
  }, [Be, nt]), ut = v.useCallback((o) => {
    if (!Ze.current) return;
    const l = o.clientX - Ze.current.startX, c = o.clientY - Ze.current.startY;
    Ee({ x: Ze.current.startPan.x + l, y: Ze.current.startPan.y + c });
  }, []), Fe = v.useCallback(() => {
    Ze.current = null;
  }, []), Rt = v.useCallback(() => {
    Yt(1), Ee({ x: 0, y: 0 });
  }, []), xt = v.useCallback((o) => {
    Te(o), ut(o);
    const l = R && R.current;
    if (l && typeof l.getScreenCTM == "function") {
      const c = l.getScreenCTM();
      if (c) {
        const u = l.createSVGPoint();
        u.x = o.clientX, u.y = o.clientY;
        const p = u.matrixTransform(c.inverse());
        mt({ x: p.x, y: p.y });
      }
    }
  }, [Te, ut]), Mt = v.useCallback(() => {
    bt(), Fe(), mt(null);
  }, [bt, Fe]), We = {
    onMouseMove: xt,
    onMouseLeave: Mt,
    onWheel: st,
    onMouseDown: Ge,
    onMouseUp: Fe,
    onDoubleClick: Rt
  }, te = Be > 1 ? `scale(${Be}) translate(${nt.x / Be}px, ${nt.y / Be}px)` : void 0, Le = Be > 1 ? { overflow: "hidden", cursor: "grab" } : void 0, Ue = Be > 1 ? { transform: te, transformOrigin: "center center" } : void 0, we = vt && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsxs("svg", { style: { position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none", zIndex: 5 }, viewBox: `0 0 ${Z} ${z}`, preserveAspectRatio: "xMidYMid meet", children: [
    /* @__PURE__ */ e.jsx("line", { x1: vt.x, y1: 0, x2: vt.x, y2: z, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" }),
    /* @__PURE__ */ e.jsx("line", { x1: 0, y1: vt.y, x2: Z, y2: vt.y, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" })
  ] }) : null, Ye = Be > 1.05 ? /* @__PURE__ */ e.jsx("div", { style: { position: "absolute", top: 4, right: 4, zIndex: 12, background: r.surfaceBg || "#fff", border: `1px solid ${r.border}`, borderRadius: r.radiusSm || "8px", padding: "2px 7px", fontSize: "10px", fontWeight: 700, color: r.textSec, pointerEvents: "auto", cursor: "pointer" }, onClick: Rt, title: "Reset zoom (or double-click chart)", children: /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "4px" }, children: [
    /* @__PURE__ */ e.jsxs("span", { children: [
      Math.round(Be * 100),
      "%"
    ] }),
    /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", transform: "scale(0.75)" }, children: /* @__PURE__ */ e.jsx(tt.Close, {}) })
  ] }) }) : null, [et, ar] = v.useState(!1), [jt, cr] = v.useState(""), Dt = v.useRef(null), [Kt, Zt] = v.useState(null), qt = v.useRef(null);
  v.useEffect(() => {
    const o = (c) => {
      if (!qt.current) return;
      const { startX: u, startY: p, startPos: g, containerRect: k } = qt.current, f = c.clientX - u, S = c.clientY - p, w = Math.max(0, Math.min(k.width - 40, g.x + f)), y = Math.max(0, Math.min(k.height - 20, g.y + S));
      Zt({ x: w, y });
    }, l = () => {
      qt.current = null;
    };
    return window.addEventListener("mousemove", o), window.addEventListener("mouseup", l), () => {
      window.removeEventListener("mousemove", o), window.removeEventListener("mouseup", l);
    };
  }, []);
  const Nt = (o) => {
    if (et) return;
    o.preventDefault();
    const l = o.currentTarget.parentElement;
    if (!l) return;
    const c = l.getBoundingClientRect(), u = Kt || {
      x: c.width / 2 - 40,
      y: 6
    };
    qt.current = {
      startX: o.clientX,
      startY: o.clientY,
      startPos: u,
      containerRect: c
    };
  }, vr = () => {
    typeof F == "function" && (cr(Y || ""), ar(!0));
  }, Pt = () => {
    typeof F == "function" && F(jt), ar(!1);
  };
  v.useEffect(() => {
    et && Dt.current && Dt.current.focus();
  }, [et]);
  const mr = Kt ? { left: `${Kt.x}px`, top: `${Kt.y}px` } : { left: "50%", top: "6px", transform: "translateX(-50%)" }, Re = Y || et ? /* @__PURE__ */ e.jsx(
    "div",
    {
      onMouseDown: Nt,
      onDoubleClick: vr,
      onClick: xe && !et ? () => xe("title", "") : void 0,
      "data-chart-element-type": "title",
      style: {
        position: "absolute",
        ...mr,
        zIndex: 6,
        cursor: et ? "text" : xe ? "pointer" : "grab",
        userSelect: et ? "auto" : "none"
      },
      children: et ? /* @__PURE__ */ e.jsx(
        "input",
        {
          ref: Dt,
          type: "text",
          value: jt,
          onChange: (o) => cr(o.target.value),
          onBlur: Pt,
          onKeyDown: (o) => {
            o.key === "Enter" && Pt(), o.key === "Escape" && ar(!1);
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
    if (n === "heatmap" || n === "icicle" || n === "sunburst" || n === "sankey" || n === "pie" || n === "donut" || n === "scatter" || n === "waterfall" || n === "combo" || n === "sparkline" || n === "bubble" || n === "radar" || n === "funnel" || n === "histogram" || ts.has(n)) return null;
    const o = Ie ? (t.stackedGroups || []).reduce(
      /** @param {any} [acc] @param {any} [group] @returns {any} */
      (E, ce) => {
        let ie = 0, se = 0;
        return (ce.segments || []).forEach(
          /** @param {any} [segment] @returns {any} */
          (N) => {
            const re = N && typeof N.value == "number" && Number.isFinite(N.value) ? N.value : null;
            re !== null && (re >= 0 ? ie += re : se += re);
          }
        ), E.push(ie, se), E;
      },
      []
    ) : Lt ? (t.categories || []).reduce(
      /** @param {any} [acc] @param {any} [_] @param {any} [categoryIndex] @returns {any} */
      (E, ce, ie) => {
        let se = 0, N = 0;
        return (t.series || []).forEach(
          /** @param {any} [series] @returns {any} */
          (re) => {
            const fe = re && Array.isArray(re.values) ? re.values[ie] : null;
            typeof fe != "number" || !Number.isFinite(fe) || (fe >= 0 ? se += fe : N += fe);
          }
        ), E.push(se, N), E;
      },
      []
    ) : gt ? t.categories.reduce(
      /** @param {any} [acc] @param {any} [_] @param {any} [categoryIndex] @returns {any} */
      (E, ce, ie) => {
        let se = 0, N = 0;
        return (t.series || []).forEach(
          /** @param {any} [series] @returns {any} */
          (re) => {
            const fe = re && Array.isArray(re.values) ? re.values[ie] : null;
            typeof fe != "number" || !Number.isFinite(fe) || (fe >= 0 ? se += fe : N += fe);
          }
        ), E.push(se, N), E;
      },
      []
    ) : (t.series || []).reduce(
      /** @param {any} [acc] @param {any} [series] @returns {any} */
      (E, ce) => ((ce.values || []).forEach(
        /** @param {any} [value] @returns {any} */
        (ie) => {
          typeof ie == "number" && Number.isFinite(ie) && E.push(ie);
        }
      ), E),
      []
    );
    if (o.length === 0) return null;
    const l = !Ie && n === "bar" && Array.isArray(t.categoryBands) && t.categoryBands.length === t.categories.length && t.categoryBands.some(
      /** @param {any} [band] @returns {any} */
      (E) => E && E.outerLabel
    );
    let c = Math.min(...o, 0), u = Math.max(...o, 0);
    if (Ne)
      c = 0, u = 100;
    else {
      if (c === u) {
        const E = Math.abs(u || 1) * 0.15 || 1;
        c -= E, u += E;
      }
      Number.isFinite(Number(s)) && s !== "" && (c = Number(s)), Number.isFinite(Number(_)) && _ !== "" && (u = Number(_)), c >= u && (u = c + 1);
    }
    const g = Ft(c, u, 5).map(
      /** @param {any} [v] @returns {any} */
      (E) => Ne ? `${Math.round(E)}%` : P(E)
    ).reduce(
      /** @param {any} [maxWidth] @param {any} [label] @returns {any} */
      (E, ce) => Math.max(E, $t(ce)),
      0
    ), k = Ie ? (t.stackedGroups || []).map(
      /** @param {any} [group] @returns {any} */
      (E) => he(E && E.label, 22)
    ) : l ? (t.categoryBands || []).map(
      /** @param {any} [band] @returns {any} */
      (E) => he(E && E.innerLabel, 18)
    ) : (t.categories || []).map(
      /** @param {any} [category] @returns {any} */
      (E) => he(E, 22)
    ), f = l ? (t.categoryBands || []).map(
      /** @param {any} [band] @returns {any} */
      (E) => he(E && E.outerLabel, 18)
    ) : [], S = k.reduce(
      /** @param {any} [maxWidth] @param {any} [label] @returns {any} */
      (E, ce) => Math.max(E, $t(ce)),
      0
    ), w = f.reduce(
      /** @param {any} [maxWidth] @param {any} [label] @returns {any} */
      (E, ce) => Math.max(E, $t(ce, 11)),
      0
    ), y = lt ? Math.max(
      l ? 176 : 132,
      l ? w + S + 38 : S + 22
    ) : Math.max(66, g + 18), I = ke === 0 ? 40 : ke <= 30 ? 62 : ke <= 45 ? 80 : ke <= 60 ? 98 : 118, X = lt ? { top: 24, right: 18, bottom: 42, left: y } : { top: 24, right: 18, bottom: l ? 118 : I, left: y }, C = Z - X.left - X.right, j = z - X.top - X.bottom, B = n === "bar" ? 0 : Math.min(Math.max(0, c), u), D = u - c || 1, H = lt ? null : (
      /** @param {any} [value] @returns {any} */
      ((E) => X.top + (u - E) / D * j)
    ), T = lt ? (
      /** @param {any} [value] @returns {any} */
      ((E) => X.left + (E - c) / D * C)
    ) : null, le = H ? H(B) : null, b = T ? T(B) : null, q = Math.max(t.categories.length, 1), oe = (lt ? j : C) / q, A = Math.max(10, oe * 0.72);
    return {
      margin: X,
      plotWidth: C,
      plotHeight: j,
      baselineY: le,
      baselineX: b,
      scaleY: H,
      scaleX: T,
      step: oe,
      groupWidth: A,
      minValue: c,
      maxValue: u,
      hasHierarchicalBands: l,
      horizontalBarMode: lt
    };
  }, [a, t, n, gt, Ie, Lt, lt, z, Z, Ne, s, _, ke, P]), It = v.useMemo(() => {
    if (!x || x.horizontalBarMode || x.hasHierarchicalBands)
      return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const o = Array.isArray(t && t.categories) ? t.categories : [];
    if (o.length <= 1) return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const l = x.step < 42 ? 12 : x.step < 70 ? 16 : 20, c = o.reduce(
      /** @param {any} [width] @param {any} [category] @returns {any} */
      (w, y) => Math.max(w, $t(he(y, l), 11)),
      0
    ), u = Math.abs(ke) * (Math.PI / 180), p = ke === 0 ? c : Math.abs(Math.cos(u)) * c + Math.abs(Math.sin(u)) * 12, g = Math.max(1, Math.ceil((p + 12) / Math.max(x.step, 1)));
    if (g === 1) return { visibleIndexes: null, maxChars: l, stride: g };
    const k = /* @__PURE__ */ new Set();
    for (let w = 0; w < o.length; w += g) k.add(w);
    const f = o.length - 1, S = Math.floor(f / g) * g;
    return f - S >= Math.ceil(g * 0.6) && k.add(f), { visibleIndexes: k, maxChars: l, stride: g };
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
            as,
            {
              model: t,
              theme: r,
              chartHeight: z,
              paletteColors: je,
              valueFormat: J,
              referenceLines: G
            }
          ),
          Re
        ]
      }
    );
  if (n === "icicle") {
    if (yt.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Icicle charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: z });
    const o = z / ct, l = ln(yt, 0, Z, 0, o, []);
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
          children: l.map(
            /** @param {any} [node] @param {any} [index] @returns {any} */
            (c, u) => {
              const p = Ae(Math.max(0, c.depth - 1), r, je), g = c.width >= 54;
              return /* @__PURE__ */ e.jsxs(
                "g",
                {
                  onClick: () => {
                    typeof W == "function" && W(c);
                  },
                  style: typeof W == "function" ? { cursor: "pointer" } : void 0,
                  children: [
                    /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: c.x,
                        y: c.y,
                        width: Math.max(1, c.width - 1),
                        height: Math.max(20, c.height - 2),
                        rx: "4",
                        fill: p,
                        opacity: "0.86",
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "1",
                        "data-tip-cat": c.label || "",
                        "data-tip-val": typeof c.value == "number" ? P(c.value) : "",
                        "data-tip-color": p
                      }
                    ),
                    g ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: c.x + 8,
                        y: c.y + Math.min(c.height - 10, 22),
                        textAnchor: "start",
                        fontSize: "11",
                        fontWeight: "700",
                        fill: "#fff",
                        style: { pointerEvents: "none" },
                        children: he(c.label, Math.max(6, Math.floor(c.width / 9)))
                      }
                    ) : null
                  ]
                },
                `icicle-node-${c.rowPath || u}`
              );
            }
          )
        }
      ),
      Re,
      we,
      Pe,
      Ye
    ] }) });
  }
  if (n === "sunburst") {
    if (yt.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Sunburst charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: z });
    const o = Math.min(Z, z) / 2 - 18, l = Z / 2, c = z / 2, u = o / Math.max(ct, 1), p = cn(yt, -Math.PI / 2, Math.PI * 3 / 2, 1, u, []);
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
          children: p.map(
            /** @param {any} [node] @param {any} [index] @returns {any} */
            (g, k) => {
              const f = Ae(Math.max(0, g.depth - 1), r, je), S = Nr(l, c, g.innerRadius, g.outerRadius, g.startAngle, g.endAngle), w = g.startAngle + (g.endAngle - g.startAngle) / 2, y = g.innerRadius + (g.outerRadius - g.innerRadius) / 2, I = lr(l, c, y, w), X = g.endAngle - g.startAngle, C = X > 0.2 && g.outerRadius - g.innerRadius >= 18;
              return /* @__PURE__ */ e.jsxs(
                "g",
                {
                  onClick: () => {
                    typeof W == "function" && W(g);
                  },
                  style: typeof W == "function" ? { cursor: "pointer" } : void 0,
                  children: [
                    /* @__PURE__ */ e.jsx(
                      "path",
                      {
                        d: S,
                        fill: f,
                        opacity: "0.88",
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "1",
                        "data-tip-cat": g.label || "",
                        "data-tip-val": typeof g.value == "number" ? P(g.value) : "",
                        "data-tip-color": f
                      }
                    ),
                    C ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: I.x,
                        y: I.y,
                        textAnchor: "middle",
                        fontSize: "10",
                        fontWeight: "700",
                        fill: "#fff",
                        style: { pointerEvents: "none" },
                        children: he(g.label, Math.max(5, Math.floor(X * 24)))
                      }
                    ) : null
                  ]
                },
                `sunburst-node-${g.rowPath || k}`
              );
            }
          )
        }
      ),
      Re,
      we,
      Pe,
      Ye
    ] }) });
  }
  if (n === "sankey") {
    const o = Array.isArray(t && t.sankeyNodes) ? t.sankeyNodes : [], l = Array.isArray(t && t.sankeyLinks) ? t.sankeyLinks : [];
    if (o.length === 0 || l.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Sankey charts need expanded hierarchical rows with child flows.", theme: r, chartHeight: z });
    const c = dn(o, l, Z, z), u = c.nodes.length > 0 ? Math.max(...c.nodes.map(
      /** @param {any} [n] @returns {any} */
      (p) => p.x
    )) : 0;
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
            c.links.map(
              /** @param {any} [link] @param {any} [index] @returns {any} */
              (p, g) => {
                const k = p.sourceNode.x + p.sourceNode.width, f = p.targetNode.x, S = k + (f - k) / 2, w = [
                  `M ${k} ${p.sourceY}`,
                  `C ${S} ${p.sourceY}, ${S} ${p.targetY}, ${f} ${p.targetY}`
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
                  `sankey-link-${p.source}-${p.target}-${g}`
                );
              }
            ),
            c.nodes.map(
              /** @param {any} [node] @param {any} [index] @returns {any} */
              (p, g) => {
                const k = Ae(Math.max(0, p.depth - 1), r, je);
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
                  `sankey-node-${p.rowPath || g}`
                );
              }
            )
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
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, l = Array.isArray(t && t.categories) ? t.categories : [];
    if (!o || l.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Pie charts need categories and at least one numeric measure.", theme: r, chartHeight: z });
    const c = l.map(
      /** @param {any} [category] @param {any} [index] @returns {any} */
      (C, j) => {
        const B = o.values[j], D = typeof B == "number" && Number.isFinite(B) ? B : 0;
        return { category: C, value: Math.abs(D), originalValue: D, index: j };
      }
    ).filter(
      /** @param {any} [slice] @returns {any} */
      (C) => C.value > 0 && !ee.has(C.category)
    ), u = c.reduce(
      /** @param {any} [sum] @param {any} [slice] @returns {any} */
      (C, j) => C + j.value,
      0
    );
    if (u === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "All values are zero — nothing to display.", theme: r, chartHeight: z });
    const p = n === "donut", g = Math.min(Z, z) / 2 - 24, k = p ? g * 0.52 : 0, f = Z / 2, S = z / 2, w = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    let y = -Math.PI / 2;
    const I = c.map(
      /** @param {any} [slice] @returns {any} */
      (C) => {
        const j = C.value / u, B = j * Math.PI * 2, D = y, H = y + B;
        return y = H, { ...C, startAngle: D, endAngle: H, fraction: j };
      }
    ), X = I.map(
      /** @param {any} [slice] @returns {any} */
      (C) => ({
        label: `${C.category} (${(C.fraction * 100).toFixed(1)}%)`,
        color: Ae(C.index, r, je)
      })
    );
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
              I.map(
                /** @param {any} [slice] @returns {any} */
                (C) => {
                  const j = Ae(C.index, r, je), B = Nr(f, S, k, g, C.startAngle, C.endAngle), D = C.startAngle + (C.endAngle - C.startAngle) / 2, H = p ? k + (g - k) / 2 : g * 0.62, T = lr(f, S, H, D), le = C.endAngle - C.startAngle > 0.3, b = lr(f, S, g + (be ? 26 : 14), D), q = C.endAngle - C.startAngle > 0.15, oe = w[C.index] || null;
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
                            d: B,
                            fill: j,
                            opacity: "0.9",
                            stroke: r.surfaceBg || r.background || "#fff",
                            strokeWidth: "2",
                            "data-tip-cat": C.category,
                            "data-tip-ser": o.name,
                            "data-tip-val": `${P(C.originalValue)} (${(C.fraction * 100).toFixed(1)}%)`,
                            "data-tip-color": j
                          }
                        ),
                        le ? /* @__PURE__ */ e.jsxs(
                          "text",
                          {
                            x: T.x,
                            y: T.y,
                            textAnchor: "middle",
                            dominantBaseline: "central",
                            fontSize: "10",
                            fontWeight: "700",
                            fill: "#fff",
                            style: { pointerEvents: "none" },
                            children: [
                              (C.fraction * 100).toFixed(1),
                              "%"
                            ]
                          }
                        ) : null,
                        (M || be) && q ? /* @__PURE__ */ e.jsxs("g", { children: [
                          be ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(
                              "line",
                              {
                                x1: lr(f, S, g * 0.97, D).x,
                                y1: lr(f, S, g * 0.97, D).y,
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
                                children: he(C.category, 14)
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
                              children: P(C.originalValue)
                            }
                          ) : null
                        ] }) : null
                      ]
                    },
                    `pie-slice-${C.index}`
                  );
                }
              ),
              p ? /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: f,
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
      m ? /* @__PURE__ */ e.jsx(wt, { items: X, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "bubble") {
    const o = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (N) => !ee.has(N.name)
    ) : [], l = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "Bubble charts need at least two measures (X and Y). A third measure sets bubble size.", theme: r, chartHeight: z });
    const u = o[0], p = o[1], g = o[2] || null, k = (u.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (N) => typeof N == "number" && Number.isFinite(N)
    ), f = (p.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (N) => typeof N == "number" && Number.isFinite(N)
    );
    if (k.length === 0 || f.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Not enough numeric data for a bubble chart.", theme: r, chartHeight: z });
    let S = Math.min(...k), w = Math.max(...k), y = Math.min(...f), I = Math.max(...f);
    if (S === w) {
      const N = Math.abs(w || 1) * 0.15 || 1;
      S -= N, w += N;
    }
    if (y === I) {
      const N = Math.abs(I || 1) * 0.15 || 1;
      y -= N, I += N;
    }
    const X = (w - S) * 0.08, C = (I - y) * 0.08;
    S -= X, w += X, y -= C, I += C;
    const j = g ? (g.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (N) => typeof N == "number" && Number.isFinite(N)
    ) : [], B = j.length ? Math.min(...j) : 0, H = (j.length ? Math.max(...j) : 1) - B || 1, T = (N) => {
      if (!g) return 7;
      const re = g.values && g.values[N];
      return typeof re != "number" || !Number.isFinite(re) ? 7 : 4 + (re - B) / H * 20;
    }, b = { top: 24, right: 20, bottom: 64, left: Math.max(52, $t(P(I), 11) + 16) }, q = Z - b.left - b.right, oe = z - b.top - b.bottom, A = (N) => b.left + (N - S) / (w - S) * q, E = (N) => b.top + (I - N) / (I - y) * oe, ce = Ft(S, w, 5), ie = Ft(y, I, 5), se = ve(p.name, 0);
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Le }, ...We, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", overflow: "visible", ...Ue }, children: [
          ie.map(
            /** @param {any} [tick] @param {any} [i] @returns {any} */
            (N, re) => /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx("line", { x1: b.left, y1: E(N), x2: Z - b.right, y2: E(N), stroke: r.border, strokeDasharray: "3 4" }),
              /* @__PURE__ */ e.jsx("text", { x: b.left - 8, y: E(N) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: P(N) })
            ] }, `y-${re}`)
          ),
          ce.map(
            /** @param {any} [tick] @param {any} [i] @returns {any} */
            (N, re) => /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx("line", { x1: A(N), y1: b.top, x2: A(N), y2: z - b.bottom, stroke: r.border, strokeDasharray: "3 4" }),
              /* @__PURE__ */ e.jsx("text", { x: A(N), y: z - b.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, children: P(N) })
            ] }, `x-${re}`)
          ),
          /* @__PURE__ */ e.jsx("line", { x1: b.left, y1: z - b.bottom, x2: Z - b.right, y2: z - b.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("line", { x1: b.left, y1: b.top, x2: b.left, y2: z - b.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("text", { x: Z / 2, y: z - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, children: u.name }),
          K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (b.top + z - b.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(b.top + z - b.bottom) / 2})`, children: K }) : null,
          (u.values || []).map(
            /** @param {any} [xVal] @param {any} [i] @returns {any} */
            (N, re) => {
              const fe = p.values[re];
              if (typeof N != "number" || !Number.isFinite(N) || typeof fe != "number" || !Number.isFinite(fe)) return null;
              const De = T(re), Jt = l[re] || `Point ${re + 1}`, kt = g ? ` | ${g.name}: ${P(g.values[re])}` : "", er = { "data-tip-cat": Jt, "data-tip-ser": `${p.name} (X:${P(N)})${kt}`, "data-tip-val": P(fe), "data-tip-color": se }, Ht = c[re];
              return /* @__PURE__ */ e.jsx(
                "circle",
                {
                  cx: A(N),
                  cy: E(fe),
                  r: De,
                  fill: se,
                  opacity: "0.72",
                  stroke: r.surfaceBg || r.background || "#fff",
                  strokeWidth: "1.5",
                  style: typeof W == "function" && Ht ? { cursor: "pointer" } : void 0,
                  onClick: typeof W == "function" && Ht ? () => W(Ht) : void 0,
                  ...er
                },
                `bubble-${re}`
              );
            }
          ),
          M && (u.values || []).map(
            /** @param {any} [xVal] @param {any} [i] @returns {any} */
            (N, re) => {
              const fe = p.values[re];
              if (typeof N != "number" || !Number.isFinite(N) || typeof fe != "number" || !Number.isFinite(fe)) return null;
              const De = T(re);
              return /* @__PURE__ */ e.jsx("text", { x: A(N), y: E(fe) - De - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: l[re] || P(fe) }, `bubble-lbl-${re}`);
            }
          )
        ] }),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(wt, { items: [{ label: `${u.name} vs ${p.name}${g ? ` (size: ${g.name})` : ""}`, color: se }], theme: r, hiddenSet: ee, onToggle: Ke, position: Oe }) : null
    ] });
  }
  if (n === "radar") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (C) => !ee.has(C.name)
    ) : [];
    if (o.length < 3)
      return /* @__PURE__ */ e.jsx(Se, { message: "Radar charts need at least 3 categories (spokes).", theme: r, chartHeight: z });
    if (l.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Radar charts need at least one measure series.", theme: r, chartHeight: z });
    const c = l.flatMap(
      /** @param {any} [s] @returns {any} */
      (C) => (C.values || []).filter(
        /** @param {any} [v] @returns {any} */
        (j) => typeof j == "number" && Number.isFinite(j)
      )
    ), u = c.length ? Math.max(...c, 0) : 1, p = 60, g = Z / 2, k = z / 2, f = Math.min(g, k) - p, S = o.length, w = 2 * Math.PI / S, y = (C, j) => {
      const B = C * w - Math.PI / 2, D = f * (j / (u || 1));
      return { x: g + D * Math.cos(B), y: k + D * Math.sin(B) };
    }, I = (C) => {
      const j = C * w - Math.PI / 2, B = f + 18;
      return { x: g + B * Math.cos(j), y: k + B * Math.sin(j) };
    }, X = l.map(
      /** @param {any} [s] @param {any} [i] @returns {any} */
      (C, j) => ({ label: C.name, color: ve(C.name, j) })
    );
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Le }, ...We, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: [
          [0.25, 0.5, 0.75, 1].map((C) => {
            const j = o.map(
              /** @param {any} [_] @param {any} [i] @returns {any} */
              (B, D) => y(D, u * C)
            );
            return /* @__PURE__ */ e.jsx("polygon", { points: j.map(
              /** @param {any} [p] @returns {any} */
              (B) => `${B.x},${B.y}`
            ).join(" "), fill: "none", stroke: r.border, strokeWidth: "1" }, `grid-${C}`);
          }),
          o.map(
            /** @param {any} [_] @param {any} [i] @returns {any} */
            (C, j) => {
              const B = y(j, u);
              return /* @__PURE__ */ e.jsx("line", { x1: g, y1: k, x2: B.x, y2: B.y, stroke: r.border, strokeWidth: "1" }, `spoke-${j}`);
            }
          ),
          o.map(
            /** @param {any} [cat] @param {any} [i] @returns {any} */
            (C, j) => {
              const B = I(j), D = Math.abs(B.x - g) < 10 ? "middle" : B.x < g ? "end" : "start";
              return /* @__PURE__ */ e.jsx("text", { x: B.x, y: B.y + 4, textAnchor: D, fontSize: "10", fill: r.textSec, children: he(C, 14) }, `spoke-lbl-${j}`);
            }
          ),
          l.map(
            /** @param {any} [ser] @param {any} [si] @returns {any} */
            (C, j) => {
              const B = ve(C.name, j), D = o.map(
                /** @param {any} [_] @param {any} [i] @returns {any} */
                (T, le) => {
                  const b = C.values && C.values[le];
                  return y(le, typeof b == "number" && Number.isFinite(b) ? b : 0);
                }
              ), H = D.map(
                /** @param {any} [p] @returns {any} */
                (T) => `${T.x},${T.y}`
              ).join(" ");
              return /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("polygon", { points: H, fill: B, fillOpacity: "0.18", stroke: B, strokeWidth: "2", style: { pointerEvents: "none" } }),
                D.map(
                  /** @param {any} [p] @param {any} [i] @returns {any} */
                  (T, le) => {
                    const b = C.values && C.values[le], q = { "data-tip-cat": o[le], "data-tip-ser": C.name, "data-tip-val": P(b), "data-tip-color": B };
                    return /* @__PURE__ */ e.jsx("circle", { cx: T.x, cy: T.y, r: "4", fill: B, stroke: r.surfaceBg || "#fff", strokeWidth: "1.5", ...q }, `radar-dot-${j}-${le}`);
                  }
                )
              ] }, `radar-${j}`);
            }
          ),
          [0.5, 1].map((C) => /* @__PURE__ */ e.jsx("text", { x: g + 4, y: k - f * C + 4, fontSize: "9", fill: r.textSec, children: P(u * C) }, `scale-${C}`))
        ] }),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(wt, { items: X, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "funnel") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!l || o.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Funnel charts need categories and at least one measure.", theme: r, chartHeight: z });
    const c = o.map(
      /** @param {any} [_] @param {any} [i] @returns {any} */
      (w, y) => {
        const I = l.values && l.values[y];
        return typeof I == "number" && Number.isFinite(I) ? Math.abs(I) : 0;
      }
    ), u = Math.max(...c, 1), p = { top: 24, right: 20, bottom: 24, left: 20 }, g = Z - p.left - p.right, f = (z - p.top - p.bottom) / o.length, S = o.map(
      /** @param {any} [cat] @param {any} [i] @returns {any} */
      (w, y) => ({ label: w, color: ve(w, y) })
    );
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Le }, ...We, children: [
        /* @__PURE__ */ e.jsx("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: o.map(
          /** @param {any} [cat] @param {any} [i] @returns {any} */
          (w, y) => {
            const I = c[y] / u, X = y + 1 < o.length ? c[y + 1] / u : c[y] / u * 0.5, C = g * I, j = g * X, B = Z / 2, D = p.top + y * f, H = ve(w, y), T = [
              `${B - C / 2},${D}`,
              `${B + C / 2},${D}`,
              `${B + j / 2},${D + f - 1}`,
              `${B - j / 2},${D + f - 1}`
            ].join(" ");
            return /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx(
                "polygon",
                {
                  points: T,
                  fill: H,
                  opacity: "0.88",
                  stroke: r.surfaceBg || "#fff",
                  strokeWidth: "1.5",
                  "data-tip-cat": w,
                  "data-tip-ser": l.name,
                  "data-tip-val": P(c[y]),
                  "data-tip-color": H
                }
              ),
              /* @__PURE__ */ e.jsxs("text", { x: B, y: D + f / 2 + 4, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: "#fff", style: { pointerEvents: "none" }, children: [
                he(w, Math.max(4, Math.floor(C / 7))),
                " ",
                M ? `— ${P(c[y])}` : ""
              ] })
            ] }, `funnel-${y}`);
          }
        ) }),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(wt, { items: S, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "histogram") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!o)
      return /* @__PURE__ */ e.jsx(Se, { message: "Histogram needs at least one numeric measure.", theme: r, chartHeight: z });
    const l = (o.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (D) => typeof D == "number" && Number.isFinite(D)
    );
    if (l.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "No numeric values for histogram.", theme: r, chartHeight: z });
    const c = Math.max(4, Math.min(20, Math.ceil(1 + Math.log2(l.length)))), u = Math.min(...l), g = (Math.max(...l) - u) / c || 1, k = Array.from({ length: c }, (D, H) => ({
      low: u + H * g,
      high: u + (H + 1) * g,
      count: 0
    }));
    l.forEach(
      /** @param {any} [v] @returns {any} */
      (D) => {
        const H = Math.min(Math.floor((D - u) / g), c - 1);
        k[H].count++;
      }
    );
    const f = Math.max(...k.map(
      /** @param {any} [b] @returns {any} */
      (D) => D.count
    ), 1), w = { top: 24, right: 16, bottom: 44, left: Math.max(46, $t(String(f), 11) + 16) }, y = Z - w.left - w.right, I = z - w.top - w.bottom, X = y / c, C = (D) => w.top + (f - D) / f * I, j = ve(o.name, 0), B = Ft(0, f, U || 5);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Le }, ...We, children: [
      /* @__PURE__ */ e.jsxs("svg", { ref: R, viewBox: `0 0 ${Z} ${z}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...Ue }, children: [
        B.map(
          /** @param {any} [tick] @param {any} [i] @returns {any} */
          (D, H) => /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx("line", { x1: w.left, y1: C(D), x2: Z - w.right, y2: C(D), stroke: r.border, strokeDasharray: "3 4" }),
            /* @__PURE__ */ e.jsx("text", { x: w.left - 8, y: C(D) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: D })
          ] }, `htick-${H}`)
        ),
        /* @__PURE__ */ e.jsx("line", { x1: w.left, y1: w.top + I, x2: Z - w.right, y2: w.top + I, stroke: r.textSec, strokeWidth: "1" }),
        k.map(
          /** @param {any} [bin] @param {any} [i] @returns {any} */
          (D, H) => {
            const T = w.left + H * X, le = C(D.count), b = Math.max(1, w.top + I - le);
            return /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx(
                "rect",
                {
                  x: T,
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
              M && D.count > 0 ? /* @__PURE__ */ e.jsx("text", { x: T + X / 2, y: le - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: D.count }) : null
            ] }, `hbar-${H}`);
          }
        ),
        k.filter(
          /** @param {any} [_] @param {any} [i] @returns {any} */
          (D, H) => H % Math.max(1, Math.floor(c / 6)) === 0 || H === c - 1
        ).map(
          /** @param {any} [bin] @param {any} [i] @returns {any} */
          (D, H) => /* @__PURE__ */ e.jsx("text", { x: w.left + k.indexOf(D) * X + X / 2, y: w.top + I + 18, textAnchor: "middle", fontSize: "10", fill: r.textSec, children: P(D.low) }, `hlbl-${H}`)
        )
      ] }),
      Re,
      we,
      Pe,
      Ye
    ] }) });
  }
  if (n === "boxplot") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (w) => !ee.has(w.name)
    ) : [], c = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = r.gridLine || r.border || "#eee";
    if (l.length < 5)
      return /* @__PURE__ */ e.jsx(Se, { message: "Box Plot needs 5 measures: Min, Q1, Median, Q3, Max.", theme: r, chartHeight: z });
    const k = o.map(
      /** @param {any} [_] @param {any} [ci] @returns {any} */
      (w, y) => [
        l[0].values[y] ?? 0,
        l[1].values[y] ?? 0,
        l[2].values[y] ?? 0,
        l[3].values[y] ?? 0,
        l[4].values[y] ?? 0
      ]
    ), f = ve(l[2].name, 0), S = {
      backgroundColor: c,
      tooltip: {
        trigger: "axis",
        /** @param {any} [params] @returns {any} */
        formatter: (
          /** @param {any} [params] @returns {any} */
          ((w) => {
            const y = w[0];
            return `${y.name}<br/>Min: ${P(y.data[1])}<br/>Q1: ${P(y.data[2])}<br/>Median: ${P(y.data[3])}<br/>Q3: ${P(y.data[4])}<br/>Max: ${P(y.data[5])}`;
          })
        )
      },
      xAxis: { type: "category", data: o.map(
        /** @param {any} [c] @returns {any} */
        (w) => he(String(w), 14)
      ), axisLabel: { color: u, fontSize: 10 }, axisLine: { lineStyle: { color: p } } },
      yAxis: { type: "value", axisLabel: { color: u, fontSize: 10, formatter: P }, axisLine: { lineStyle: { color: p } }, splitLine: { lineStyle: { color: g } }, name: K || "", nameTextStyle: { color: u } },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      series: [{ type: "boxplot", data: k, itemStyle: { color: f, borderColor: f } }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: c, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(rr, { chartType: n, echartsRef: $, option: S }) });
  }
  if (n === "nightingale") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (k) => !ee.has(k.name)
    ) : [], c = r.chartBg || r.surface || "#fff", u = r.text || "#333";
    if (o.length === 0 || l.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Nightingale charts need categories and at least one measure.", theme: r, chartHeight: z });
    const p = o.map(
      /** @param {any} [cat] @param {any} [ci] @returns {any} */
      (k, f) => {
        const S = l[0] && l[0].values ? l[0].values[f] ?? 0 : 0;
        return { name: String(k), value: S, itemStyle: { color: ve(String(k), f) } };
      }
    ), g = {
      backgroundColor: c,
      tooltip: {
        trigger: "item",
        /** @param {any} [p] @returns {any} */
        formatter: (
          /** @param {any} [p] @returns {any} */
          ((k) => `${k.name}: ${P(k.value)}`)
        )
      },
      legend: { show: m, textStyle: { color: u }, type: "scroll" },
      series: [{
        type: "bar",
        coordinateSystem: "polar",
        data: p,
        label: {
          show: M,
          position: "outside",
          /** @param {any} [p] @returns {any} */
          formatter: (
            /** @param {any} [p] @returns {any} */
            ((k) => P(k.value))
          ),
          color: u,
          fontSize: 10
        }
      }],
      polar: { radius: ["10%", "75%"] },
      angleAxis: { type: "category", data: o.map(
        /** @param {any} [c] @returns {any} */
        (k) => he(String(k), 12)
      ), axisLabel: { color: u, fontSize: 9 } },
      radiusAxis: { axisLabel: { color: u, fontSize: 9, formatter: P } }
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: c, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(rr, { chartType: n, echartsRef: $, option: g }) });
  }
  if (n === "heatmap") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (H) => !ee.has(H.name)
    ) : [];
    if (o.length === 0 || l.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Heatmap needs categories and at least one measure.", theme: r, chartHeight: z });
    const c = l.flatMap(
      /** @param {any} [s] @returns {any} */
      (H) => (H.values || []).filter(
        /** @param {any} [v] @returns {any} */
        (T) => typeof T == "number" && Number.isFinite(T)
      )
    ), u = c.length ? Math.min(...c) : 0, g = (c.length ? Math.max(...c) : 1) - u || 1, k = (H) => {
      const T = Math.max(0, Math.min(1, (H - u) / g)), le = Math.round(59 + T * 180), b = Math.round(130 - T * 62), q = Math.round(246 - T * 178);
      return `rgb(${le},${b},${q})`;
    }, f = 2, S = Math.min(140, Math.max(60, l.reduce(
      /** @param {any} [m] @param {any} [s] @returns {any} */
      (H, T) => Math.max(H, $t(T.name, 11)),
      0
    ) + 10)), w = 32, y = Z, I = z, X = y - S - 8, C = I - w - 8, j = Math.max(20, X / o.length), B = Math.max(20, C / l.length), D = l.map(
      /** @param {any} [s] @param {any} [i] @returns {any} */
      (H, T) => ({ label: H.name, color: Ae(T, r, je) })
    );
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Le }, ...We, children: [
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
              o.map(
                /** @param {any} [cat] @param {any} [ci] @returns {any} */
                (H, T) => /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: S + T * j + j / 2,
                    y: w - 6,
                    textAnchor: "middle",
                    fontSize: "10",
                    fill: r.textSec,
                    style: { pointerEvents: "none" },
                    children: he(H, Math.max(4, Math.floor(j / 6.5)))
                  },
                  `hm-col-${T}`
                )
              ),
              l.map(
                /** @param {any} [ser] @param {any} [si] @returns {any} */
                (H, T) => /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: S - 6,
                    y: w + T * B + B / 2 + 4,
                    textAnchor: "end",
                    fontSize: "11",
                    fill: r.textSec,
                    style: { pointerEvents: "none" },
                    children: he(H.name, Math.max(6, Math.floor(S / 6.5)))
                  },
                  `hm-row-${T}`
                )
              ),
              l.map(
                /** @param {any} [ser] @param {any} [si] @returns {any} */
                (H, T) => o.map(
                  /** @param {any} [cat] @param {any} [ci] @returns {any} */
                  (le, b) => {
                    const q = H.values && H.values[b];
                    if (q == null) return null;
                    const oe = k(q), A = S + b * j, E = w + T * B;
                    return /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: A + f,
                        y: E + f,
                        width: Math.max(1, j - f * 2),
                        height: Math.max(1, B - f * 2),
                        rx: "3",
                        fill: oe,
                        "data-tip-cat": le,
                        "data-tip-ser": H.name,
                        "data-tip-val": P(q),
                        "data-tip-color": oe
                      },
                      `hm-cell-${T}-${b}`
                    );
                  }
                )
              ),
              M && l.map(
                /** @param {any} [ser] @param {any} [si] @returns {any} */
                (H, T) => o.map(
                  /** @param {any} [cat] @param {any} [ci] @returns {any} */
                  (le, b) => {
                    const q = H.values && H.values[b];
                    if (q == null || j < 36 || B < 18) return null;
                    const A = Math.max(0, Math.min(1, (q - u) / g)) > 0.5 ? "#fff" : r.text;
                    return /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: S + b * j + j / 2,
                        y: w + T * B + B / 2 + 4,
                        textAnchor: "middle",
                        fontSize: "9",
                        fontWeight: "700",
                        fill: A,
                        style: { pointerEvents: "none" },
                        children: P(q)
                      },
                      `hm-label-${T}-${b}`
                    );
                  }
                )
              )
            ]
          }
        ),
        Re,
        we,
        Pe,
        Ye
      ] }),
      m ? /* @__PURE__ */ e.jsx(wt, { items: D, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "bar3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (f) => !ee.has(f.name)
    ) : [], c = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = [];
    l.forEach(
      /** @param {any} [ser] @param {any} [si] @returns {any} */
      (f, S) => {
        o.forEach(
          /** @param {any} [cat] @param {any} [ci] @returns {any} */
          (w, y) => {
            const I = f.values[y];
            I != null && g.push([y, S, I]);
          }
        );
      }
    );
    const k = {
      backgroundColor: c,
      legend: {
        show: m,
        data: l.map(
          /** @param {any} [s] @returns {any} */
          (f) => f.name
        ),
        textStyle: { color: u },
        type: "scroll"
      },
      tooltip: {
        /** @param {any} [params] @returns {any} */
        formatter: (f) => {
          const S = o[f.value[0]] || "", w = l[f.value[1]] ? l[f.value[1]].name : "";
          return `${S} / ${w}: <b>${P(f.value[2])}</b>`;
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
        data: o.map(
          /** @param {any} [c] @returns {any} */
          (f) => he(String(f), 12)
        ),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } },
        name: ""
      },
      yAxis3D: {
        type: "category",
        data: l.map(
          /** @param {any} [s] @returns {any} */
          (f) => he(String(f.name), 12)
        ),
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
        data: g.map((f) => ({
          value: f,
          itemStyle: { color: Ae(f[1], r, je), opacity: 0.9 }
        })),
        shading: "lambert",
        label: {
          show: M,
          /** @param {any} [p] @returns {any} */
          formatter: (f) => P(f.value[2]),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { label: { show: !0 }, itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: c, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(rr, { chartType: n, echartsRef: $, option: k }) });
  }
  if (n === "line3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (f) => !ee.has(f.name)
    ) : [], c = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = l.map(
      /** @param {any} [ser] @param {any} [si] @returns {any} */
      (f, S) => ({
        type: "line3D",
        data: o.map(
          /** @param {any} [_] @param {any} [ci] @returns {any} */
          (w, y) => {
            const I = f.values[y];
            return [y, S, I ?? 0];
          }
        ),
        lineStyle: { color: Ae(S, r, je), width: 3, opacity: 0.9 },
        name: f.name
      })
    ), k = {
      backgroundColor: c,
      legend: { show: m, data: l.map(
        /** @param {any} [s] @returns {any} */
        (f) => f.name
      ), textStyle: { color: u }, type: "scroll" },
      tooltip: {
        /** @param {any} [params] @returns {any} */
        formatter: (f) => {
          const S = o[f.value[0]] || "", w = l[f.value[1]] ? l[f.value[1]].name : "";
          return `${S} / ${w}: <b>${P(f.value[2])}</b>`;
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
        data: o.map(
          /** @param {any} [c] @returns {any} */
          (f) => he(String(f), 12)
        ),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      yAxis3D: {
        type: "category",
        data: l.map(
          /** @param {any} [s] @returns {any} */
          (f) => he(String(f.name), 12)
        ),
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
      series: g
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: c, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(rr, { chartType: n, echartsRef: $, option: k }) });
  }
  if (n === "scatter3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (y) => !ee.has(y.name)
    ) : [], c = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc";
    if (l.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "3D Scatter needs at least two numeric measures (X and Y).", theme: r, chartHeight: z });
    const g = l[0], k = l[1], f = l[2] || null, S = o.map(
      /** @param {any} [cat] @param {any} [ci] @returns {any} */
      (y, I) => ({
        value: [g.values[I] ?? 0, k.values[I] ?? 0, f ? f.values[I] ?? 0 : I],
        name: String(y)
      })
    ), w = {
      backgroundColor: c,
      tooltip: {
        /** @param {any} [params] @returns {any} */
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
        name: g.name,
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
        name: f ? f.name : "Index",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        nameTextStyle: { color: u }
      },
      series: [{
        type: "scatter3D",
        data: S,
        symbolSize: 8,
        itemStyle: { color: Ae(0, r, je), opacity: 0.85 },
        label: {
          show: M,
          /** @param {any} [params] @returns {any} */
          formatter: (y) => String(o[y.dataIndex] || ""),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: c, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(rr, { chartType: n, echartsRef: $, option: w }) });
  }
  if (n === "range") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (y) => !ee.has(y.name)
    ) : [], c = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = r.gridLine || r.border || "#eee";
    if (l.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "Range chart needs at least two measures (low and high values).", theme: r, chartHeight: z });
    const k = [];
    for (let y = 0; y + 1 < l.length; y += 2)
      k.push({ low: l[y], high: l[y + 1] });
    const f = [];
    k.forEach((y, I) => {
      const X = Ae(I, r, je);
      f.push({
        type: "bar",
        name: `_base_${I}`,
        data: o.map(
          /** @param {any} [_] @param {any} [ci] @returns {any} */
          (C, j) => Math.min(y.low.values[j] ?? 0, y.high.values[j] ?? 0)
        ),
        stack: `range_${I}`,
        itemStyle: { opacity: 0 },
        emphasis: { disabled: !0 },
        silent: !0,
        legendHoverLink: !1
      }), f.push({
        type: "bar",
        name: `${y.low.name} – ${y.high.name}`,
        data: o.map(
          /** @param {any} [_] @param {any} [ci] @returns {any} */
          (C, j) => Math.abs((y.high.values[j] ?? 0) - (y.low.values[j] ?? 0))
        ),
        stack: `range_${I}`,
        itemStyle: { color: X, opacity: 0.9 },
        label: {
          show: M,
          position: "inside",
          /** @param {any} [params] @returns {any} */
          formatter: (C) => {
            const j = C.dataIndex, B = y.low.values[j] ?? 0, D = y.high.values[j] ?? 0;
            return `${P(Math.min(B, D))}–${P(Math.max(B, D))}`;
          },
          color: u,
          fontSize: 10
        },
        tooltip: {
          /** @param {any} [params] @returns {any} */
          formatter: (C) => {
            const j = C.dataIndex;
            return `${o[j]}<br/>${y.low.name}: ${P(y.low.values[j] ?? 0)}<br/>${y.high.name}: ${P(y.high.values[j] ?? 0)}`;
          }
        }
      });
    });
    const S = k.map((y, I) => ({ label: `${y.low.name} – ${y.high.name}`, color: Ae(I, r, je) })), w = {
      backgroundColor: c,
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: o.map(
          /** @param {any} [c] @returns {any} */
          (y) => he(String(y), 14)
        ),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } },
        splitLine: { show: !1 }
      },
      yAxis: {
        type: "value",
        name: K || "",
        axisLabel: { color: u, fontSize: 10, formatter: P },
        axisLine: { lineStyle: { color: p } },
        splitLine: { lineStyle: { color: g } },
        nameTextStyle: { color: u }
      },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      series: f
    };
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: c, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(rr, { chartType: n, echartsRef: $, option: w }) }),
      m ? /* @__PURE__ */ e.jsx(wt, { items: S, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "scatter") {
    const o = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (A) => !ee.has(A.name)
    ) : [], l = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(Se, { message: "Scatter charts need at least two numeric measures (X and Y).", theme: r, chartHeight: z });
    const u = o[0], p = o.slice(1), g = (u.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (A) => typeof A == "number" && Number.isFinite(A)
    ), k = p.flatMap(
      /** @param {any} [s] @returns {any} */
      (A) => (A.values || []).filter(
        /** @param {any} [v] @returns {any} */
        (E) => typeof E == "number" && Number.isFinite(E)
      )
    );
    if (g.length === 0 || k.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Not enough numeric data for a scatter chart.", theme: r, chartHeight: z });
    let f = Math.min(...g), S = Math.max(...g), w = Math.min(...k), y = Math.max(...k);
    if (f === S) {
      const A = Math.abs(S || 1) * 0.15 || 1;
      f -= A, S += A;
    }
    if (w === y) {
      const A = Math.abs(y || 1) * 0.15 || 1;
      w -= A, y += A;
    }
    const I = (S - f) * 0.06, X = (y - w) * 0.06;
    f -= I, S += I, w -= X, y += X;
    const j = { top: 24, right: 20, bottom: 64, left: Math.max(52, $t(P(y), 11) + 16) }, B = Z - j.left - j.right, D = z - j.top - j.bottom, H = (A) => j.left + (A - f) / (S - f) * B, T = (A) => j.top + (y - A) / (y - w) * D, le = Ft(f, S, 5), b = Ft(w, y, 5), q = p.map(
      /** @param {any} [s] @param {any} [i] @returns {any} */
      (A, E) => ({ label: A.name, color: ve(A.name, E) })
    ), oe = typeof W == "function";
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
              b.map(
                /** @param {any} [tick] @param {any} [i] @returns {any} */
                (A, E) => /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: T(A), x2: Z - j.right, y2: T(A), stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: j.left - 8, y: T(A) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(A) })
                ] }, `y-tick-${E}`)
              ),
              le.map(
                /** @param {any} [tick] @param {any} [i] @returns {any} */
                (A, E) => /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: H(A), y1: j.top, x2: H(A), y2: z - j.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: H(A), y: z - j.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(A) })
                ] }, `x-tick-${E}`)
              ),
              /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: z - j.bottom, x2: Z - j.right, y2: z - j.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: j.top, x2: j.left, y2: z - j.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("text", { x: Z / 2, y: z - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: u.name }),
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (j.top + z - j.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(j.top + z - j.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              p.map(
                /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                (A, E) => {
                  const ce = ve(A.name, E);
                  return (A.values || []).map(
                    /** @param {any} [yVal] @param {any} [pointIndex] @returns {any} */
                    (ie, se) => {
                      const N = u.values[se];
                      if (typeof N != "number" || !Number.isFinite(N) || typeof ie != "number" || !Number.isFinite(ie)) return null;
                      const re = H(N), fe = T(ie), De = l[se] || `Point ${se + 1}`, Jt = c[se], kt = {
                        "data-tip-cat": De,
                        "data-tip-ser": `${A.name} (${u.name}=${P(N)})`,
                        "data-tip-val": P(ie),
                        "data-tip-color": ce
                      };
                      return ft.cloneElement(Bt(re, fe, ce, kt, {
                        style: oe && Jt ? { cursor: "pointer" } : void 0,
                        onClick: oe && Jt ? () => W(Jt) : void 0
                      }), { key: `scatter-${E}-${se}` });
                    }
                  );
                }
              ),
              M ? p.map(
                /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                (A, E) => {
                  const ce = ve(A.name, E);
                  return (A.values || []).map(
                    /** @param {any} [yVal] @param {any} [pointIndex] @returns {any} */
                    (ie, se) => {
                      const N = u.values[se];
                      return typeof N != "number" || !Number.isFinite(N) || typeof ie != "number" || !Number.isFinite(ie) ? null : /* @__PURE__ */ e.jsx(
                        "text",
                        {
                          x: H(N),
                          y: T(ie) - 8,
                          textAnchor: "middle",
                          fontSize: "10",
                          fontWeight: "700",
                          fill: ce,
                          style: { pointerEvents: "none" },
                          children: P(ie)
                        },
                        `scatter-label-${E}-${se}`
                      );
                    }
                  );
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
      m ? /* @__PURE__ */ e.jsx(wt, { items: q, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "waterfall") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, l = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (!o || l.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: "Waterfall charts need categories and at least one numeric measure.", theme: r, chartHeight: z });
    const u = l.map(
      /** @param {any} [_] @param {any} [i] @returns {any} */
      (A, E) => {
        const ce = o.values[E];
        return typeof ce == "number" && Number.isFinite(ce) ? ce : 0;
      }
    ), p = [];
    let g = 0;
    for (let A = 0; A < u.length; A++) {
      const E = g;
      g += u[A], p.push({ label: l[A], start: E, end: g, increment: u[A], type: u[A] >= 0 ? "positive" : "negative" });
    }
    p.push({ label: "Total", start: 0, end: g, increment: g, type: "total" });
    const k = p.map((A) => A.start).concat(p.map((A) => A.end));
    let f = Math.min(...k, 0), S = Math.max(...k, 0);
    if (f === S) {
      const A = Math.abs(S || 1) * 0.15 || 1;
      f -= A, S += A;
    }
    const w = (S - f) * 0.08;
    f -= w, S += w;
    const y = Ft(f, S, 5), X = { top: 24, right: 20, bottom: 84, left: Math.max(52, $t(P(S), 11) + 16) }, C = Z - X.left - X.right, j = z - X.top - X.bottom, B = (A) => X.top + (S - A) / (S - f) * j, D = C / p.length, H = Math.max(14, Math.min(60, D * 0.6)), T = "#059669", le = "#DC2626", b = Ae(0, r, je), q = typeof W == "function", oe = [
      { label: "Increase", color: T },
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
              y.map(
                /** @param {any} [tick] @param {any} [i] @returns {any} */
                (A, E) => /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: X.left, y1: B(A), x2: Z - X.right, y2: B(A), stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: X.left - 8, y: B(A) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(A) })
                ] }, `wf-tick-${E}`)
              ),
              /* @__PURE__ */ e.jsx("line", { x1: X.left, y1: B(0), x2: Z - X.right, y2: B(0), stroke: r.textSec, strokeOpacity: "0.5" }),
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (X.top + z - X.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(X.top + z - X.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              p.map((A, E) => {
                const ce = X.left + E * D + D / 2, ie = ce - H / 2, se = B(Math.max(A.start, A.end)), N = B(Math.min(A.start, A.end)), re = Math.max(1, N - se), fe = A.type === "total" ? b : A.type === "positive" ? T : le, De = E < c.length ? c[E] : null;
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  E > 0 ? /* @__PURE__ */ e.jsx(
                    "line",
                    {
                      x1: X.left + (E - 1) * D + D / 2 + H / 2,
                      y1: E === p.length - 1 ? B(A.end) : B(A.start),
                      x2: ie,
                      y2: E === p.length - 1 ? B(A.end) : B(A.start),
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
                      "data-tip-cat": A.label,
                      "data-tip-ser": A.type === "total" ? "Total" : (A.increment >= 0 ? "+" : "") + P(A.increment),
                      "data-tip-val": P(A.end),
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
                      children: A.type === "total" ? P(A.end) : (A.increment >= 0 ? "+" : "") + P(A.increment)
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
                      children: he(A.label, 12)
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
      m ? /* @__PURE__ */ e.jsx(wt, { items: oe, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (n === "combo") {
    const o = (Array.isArray(t && t.layers) ? t.layers : []).filter(
      /** @param {any} [layer] @returns {any} */
      (b) => b && !b.hidden && !ee.has(b.name) && Array.isArray(b.values) && b.values.some(
        /** @param {any} [value] @returns {any} */
        (q) => q !== null
      )
    );
    if (o.length === 0)
      return /* @__PURE__ */ e.jsx(Se, { message: t.emptyMessage || "No combo layers are configured.", theme: r, chartHeight: z });
    const l = o.some(
      /** @param {any} [layer] @returns {any} */
      (b) => b.axis === "right"
    ), c = { top: 24, right: l ? 76 : 20, bottom: 96, left: 72 }, u = Math.max(1, Array.isArray(t.categories) ? t.categories.length : 0), p = Math.max(1, (Z - c.left - c.right) / u), g = Math.max(22, Math.min(72, p * 0.72)), k = o.filter(
      /** @param {any} [layer] @returns {any} */
      (b) => b.type === "bar"
    ), f = o.filter(
      /** @param {any} [layer] @returns {any} */
      (b) => b.type === "area"
    ), S = o.filter(
      /** @param {any} [layer] @returns {any} */
      (b) => b.type === "line"
    ), w = Pr(
      o.filter(
        /** @param {any} [layer] @returns {any} */
        (b) => b.axis !== "right"
      ).flatMap(
        /** @param {any} [layer] @returns {any} */
        (b) => b.values
      ),
      z,
      c
    ), y = l ? Pr(
      o.filter(
        /** @param {any} [layer] @returns {any} */
        (b) => b.axis === "right"
      ).flatMap(
        /** @param {any} [layer] @returns {any} */
        (b) => b.values
      ),
      z,
      c
    ) : w, I = Array.isArray(t.categoryTargets) ? t.categoryTargets : [], X = o.map(
      /** @param {any} [layer] @param {any} [index] @returns {any} */
      (b, q) => ({
        label: b.name,
        color: b.color || Ae(q, r, je)
      })
    ), C = (b) => b.axis === "right" ? y.scaleY : w.scaleY, j = (b) => b.axis === "right" ? y.baselineY : w.baselineY, B = (b) => (
      /** @param {any} [v] @returns {any} */
      ((q) => Wt(q, b.valueFormat || J || "auto"))
    ), D = o.find(
      /** @param {any} [layer] @returns {any} */
      (b) => b.axis === "right"
    ), H = D ? B(D) : P, T = 4, le = Math.max(8, (g - (Math.max(k.length, 1) - 1) * T) / Math.max(k.length, 1));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Le }, ...We, children: [
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
              w.tickValues.map(
                /** @param {any} [tickValue] @param {any} [index] @returns {any} */
                (b, q) => {
                  const oe = w.scaleY(b);
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    /* @__PURE__ */ e.jsx(
                      "line",
                      {
                        x1: c.left,
                        x2: Z - c.right,
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
                        x: c.left - 10,
                        y: oe + 4,
                        textAnchor: "end",
                        fontSize: "11",
                        fill: r.textSec,
                        children: P(b)
                      }
                    )
                  ] }, `combo-left-tick-${q}`);
                }
              ),
              l ? y.tickValues.map(
                /** @param {any} [tickValue] @param {any} [index] @returns {any} */
                (b, q) => {
                  const oe = y.scaleY(b);
                  return /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: Z - c.right + 10,
                      y: oe + 4,
                      textAnchor: "start",
                      fontSize: "11",
                      fill: r.textSec,
                      children: H(b)
                    },
                    `combo-right-tick-${q}`
                  );
                }
              ) : null,
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (c.top + z - c.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(c.top + z - c.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              /* @__PURE__ */ e.jsx(
                "line",
                {
                  x1: c.left,
                  x2: Z - c.right,
                  y1: w.baselineY,
                  y2: w.baselineY,
                  stroke: r.textSec,
                  strokeOpacity: "0.45"
                }
              ),
              k.map(
                /** @param {any} [layer] @param {any} [barLayerIndex] @returns {any} */
                (b, q) => t.categories.map(
                  /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
                  (oe, A) => {
                    const E = b.values[A];
                    if (E == null) return null;
                    const ce = c.left + A * p + (p - g) / 2, ie = C(b), se = j(b), N = ie(E), re = b.color || Ae(q, r, je), fe = ce + q * (le + T), De = B(b);
                    return /* @__PURE__ */ e.jsxs(ft.Fragment, { children: [
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
                  }
                )
              ),
              f.map(
                /** @param {any} [layer] @param {any} [layerIndex] @returns {any} */
                (b, q) => {
                  const oe = t.categories.map(
                    /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                    (se, N) => {
                      const re = b.values[N];
                      if (re == null) return null;
                      const fe = c.left + N * p + p / 2, De = C(b);
                      return {
                        x: fe,
                        y: De(re),
                        baseY: j(b),
                        rawValue: re
                      };
                    }
                  ), A = Ir(oe), E = pr(oe), ce = b.color || Ae(k.length + q, r, je), ie = B(b);
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    A ? /* @__PURE__ */ e.jsx("path", { d: A, fill: ce, opacity: "0.16", style: { pointerEvents: "none" } }) : null,
                    E ? /* @__PURE__ */ e.jsx("path", { d: E, fill: "none", stroke: ce, strokeWidth: "2.5", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: hr(ht[b.name]), style: { pointerEvents: "none" } }) : null,
                    oe.map(
                      /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
                      (se, N) => se ? /* @__PURE__ */ e.jsxs(ft.Fragment, { children: [
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
                      ] }, `${b.id}-area-pt-${N}`) : null
                    )
                  ] }, `combo-area-${b.id}`);
                }
              ),
              S.map(
                /** @param {any} [layer] @param {any} [layerIndex] @returns {any} */
                (b, q) => {
                  const oe = t.categories.map(
                    /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                    (ie, se) => {
                      const N = b.values[se];
                      return N == null ? null : {
                        x: c.left + se * p + p / 2,
                        y: C(b)(N),
                        rawValue: N
                      };
                    }
                  ), A = pr(oe), E = b.color || Ae(k.length + f.length + q, r, je), ce = B(b);
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    A ? /* @__PURE__ */ e.jsx("path", { d: A, fill: "none", stroke: E, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: hr(ht[b.name]), style: { pointerEvents: "none" } }) : null,
                    oe.map(
                      /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
                      (ie, se) => ie ? /* @__PURE__ */ e.jsxs(ft.Fragment, { children: [
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
                      ] }, `${b.id}-point-${se}`) : null
                    )
                  ] }, `combo-line-${b.id}`);
                }
              ),
              t.categories.map(
                /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
                (b, q) => {
                  const oe = c.left + q * p + p / 2;
                  return /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: oe,
                      y: z - c.bottom + 28,
                      textAnchor: "middle",
                      fontSize: "11",
                      fill: r.textSec,
                      children: he(b, 18)
                    },
                    `combo-label-${q}`
                  );
                }
              ),
              typeof W == "function" ? t.categories.map(
                /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                (b, q) => {
                  const oe = I[q];
                  return oe ? /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: c.left + q * p,
                      y: c.top,
                      width: p,
                      height: z - c.top - c.bottom,
                      fill: "transparent",
                      style: { cursor: "pointer" },
                      onClick: () => W(oe)
                    },
                    `combo-hit-${q}`
                  ) : null;
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
      m ? /* @__PURE__ */ e.jsx(wt, { items: X, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
    ] });
  }
  if (!x)
    return /* @__PURE__ */ e.jsx(Se, { message: "No numeric data available for this chart.", theme: r, chartHeight: z });
  const jr = Ft(x.minValue, x.maxValue, U || 5), Ct = Ie ? Array.from(new Set(
    (t.stackedGroups || []).reduce(
      /** @param {any} [labels] @param {any} [group] @returns {any} */
      (o, l) => ((l.segments || []).forEach(
        /** @param {any} [segment] @returns {any} */
        (c) => {
          c && c.label && o.push(c.label);
        }
      ), o),
      []
    )
  )) : [], dr = Ct.reduce(
    /** @param {any} [acc] @param {any} [label] @param {any} [index] @returns {any} */
    (o, l, c) => (o[l] = Ae(c, r, je), o),
    {}
  ), At = Ie ? Ct.map(
    /** @param {any} [label] @returns {any} */
    (o) => ({ label: o, color: dr[o] || Ae(0, r, je) })
  ) : (t.series || []).map(
    /** @param {any} [series] @param {any} [index] @returns {any} */
    (o, l) => ({
      label: o.name,
      color: ve(o.name, l)
    })
  ), Qt = x.hasHierarchicalBands ? t.categoryBands.reduce(
    /** @param {any} [groups] @param {any} [band] @param {any} [index] @returns {any} */
    (o, l, c) => {
      const u = o[o.length - 1];
      return u && u.groupKey === l.groupKey ? (u.endIndex = c, o) : (o.push({
        groupKey: l.groupKey,
        outerLabel: l.outerLabel,
        startIndex: c,
        endIndex: c
      }), o);
    },
    []
  ) : [], Vt = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [], kr = typeof W == "function" && Vt.length === t.categories.length;
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
    /* @__PURE__ */ e.jsxs("div", { ref: He, style: { width: "100%", minWidth: 0, position: "relative", ...Le }, ...We, children: [
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
          onClick: xe ? dt : void 0,
          children: [
            ae ? /* @__PURE__ */ e.jsx("defs", { children: (t.series || []).map(
              /** @param {any} [ser] @param {any} [si] @returns {any} */
              (o, l) => {
                const c = ve(o.name, l), u = Wr.find((g) => g.value === or) || Wr[0], p = zn(c, rt);
                return /* @__PURE__ */ e.jsxs("linearGradient", { id: `pg-grad-${l}`, x1: u.x1, y1: u.y1, x2: u.x2, y2: u.y2, children: [
                  /* @__PURE__ */ e.jsx("stop", { offset: p[0].offset, stopColor: c, stopOpacity: p[0].stopOpacity }),
                  /* @__PURE__ */ e.jsx("stop", { offset: p[1].offset, stopColor: c, stopOpacity: p[1].stopOpacity })
                ] }, `grad-${l}`);
              }
            ) }) : null,
            jr.map(
              /** @param {any} [tickValue] @param {any} [index] @returns {any} */
              (o, l) => {
                const c = x.scaleY ? x.scaleY(o) : null, u = x.scaleX ? x.scaleX(o) : null, p = Ne ? `${Math.round(o)}%` : P(o);
                return /* @__PURE__ */ e.jsx("g", { children: x.horizontalBarMode ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: u, y1: x.margin.top, x2: u, y2: z - x.margin.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: u, y: z - x.margin.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: xe ? { cursor: "pointer" } : void 0, children: p })
                ] }) : /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: c, x2: Z - x.margin.right, y2: c, stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: x.margin.left - 8, y: c + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "y", style: xe ? { cursor: "pointer" } : void 0, children: p })
                ] }) }, `tick-${l}`);
              }
            ),
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
            Array.isArray(G) && G.length > 0 && G.map((o, l) => {
              const c = Number(o.value);
              if (!Number.isFinite(c)) return null;
              const u = o.color || r.primary, p = o.orient === "v", g = hr(o.dash || "dashed") || "6 3";
              if (p) {
                const f = x.margin.left + c * x.step;
                return f < x.margin.left - 2 || f > Z - x.margin.right + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: f, y1: x.margin.top, x2: f, y2: z - x.margin.bottom, stroke: u, strokeWidth: "1.5", strokeDasharray: g, style: { pointerEvents: "none" } }),
                  o.label ? /* @__PURE__ */ e.jsx("text", { x: f + 4, y: x.margin.top + 12, textAnchor: "start", fontSize: "10", fontWeight: "700", fill: u, style: { pointerEvents: "none" }, children: o.label }) : null
                ] }, `ref-line-${l}`);
              }
              if (!x.scaleY) return null;
              const k = x.scaleY(c);
              return k < x.margin.top - 2 || k > z - x.margin.bottom + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: k, x2: Z - x.margin.right, y2: k, stroke: u, strokeWidth: "1.5", strokeDasharray: g, style: { pointerEvents: "none" } }),
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
                    onMouseDown: (f) => {
                      f.preventDefault(), f.stopPropagation();
                      const S = f.clientY, w = c, { plotHeight: y, maxValue: I, minValue: X } = x, C = I - X;
                      if (C <= 0) return;
                      const j = (D) => {
                        const H = w - (D.clientY - S) / y * C;
                        _e(
                          /** @param {any} [prev] @returns {any} */
                          (T) => T.map(
                            /** @param {any} [l] @param {any} [j] @returns {any} */
                            (le, b) => b === l ? { ...le, value: String(Math.round(H * 1e3) / 1e3) } : le
                          )
                        );
                      }, B = () => {
                        window.removeEventListener("mousemove", j), window.removeEventListener("mouseup", B);
                      };
                      window.addEventListener("mousemove", j), window.addEventListener("mouseup", B);
                    }
                  }
                ) : null
              ] }, `ref-line-${l}`);
            }),
            Ot && !x.horizontalBarMode && (n === "bar" || n === "line" || n === "area" || n === "scatter") && Array.isArray(t.series) && t.series.length > 0 && x.scaleY ? t.series.filter(
              /** @param {any} [s] @returns {any} */
              (l) => !ee.has(l.name)
            ).map(
              /** @param {any} [ser] @param {any} [tsi] @returns {any} */
              (l, c) => {
                const u = l.values || [], p = at === "moving_avg" ? Qn(u, Qe) : qn(u);
                if (!p) return null;
                const g = p.map(
                  /** @param {any} [v] @param {any} [i] @returns {any} */
                  (w, y) => Number.isFinite(w) ? { x: x.margin.left + y * x.step + x.step / 2, y: x.scaleY(w) } : null
                ), k = pr(g);
                if (!k) return null;
                const f = ve(l.name, c), S = Tt || f;
                return /* @__PURE__ */ e.jsx(
                  "path",
                  {
                    d: k,
                    fill: "none",
                    stroke: S,
                    strokeWidth: String(Gt || 2),
                    strokeDasharray: "8 4",
                    strokeLinecap: "round",
                    opacity: "0.78",
                    style: { pointerEvents: "none" }
                  },
                  `trendline-${c}`
                );
              }
            ) : null,
            x.hasHierarchicalBands && Qt.map(
              /** @param {any} [group] @param {any} [groupIndex] @returns {any} */
              (o, l) => {
                if (l === Qt.length - 1) return null;
                const c = x.horizontalBarMode ? x.margin.top + (o.endIndex + 1) * x.step : x.margin.left + (o.endIndex + 1) * x.step;
                return /* @__PURE__ */ e.jsx(
                  "line",
                  {
                    x1: x.horizontalBarMode ? x.margin.left : c,
                    y1: x.horizontalBarMode ? c : x.margin.top,
                    x2: x.horizontalBarMode ? Z - x.margin.right : c,
                    y2: x.horizontalBarMode ? c : z - x.margin.bottom + 58,
                    stroke: r.border,
                    strokeDasharray: "4 4"
                  },
                  `group-divider-${o.groupKey}-${l}`
                );
              }
            ),
            Ie && (t.stackedGroups || []).map(
              /** @param {any} [group] @param {any} [groupIndex] @returns {any} */
              (o, l) => {
                const c = x.horizontalBarMode ? x.margin.top + l * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + l * x.step + (x.step - x.groupWidth) / 2, u = Math.max(14, x.groupWidth * 0.64);
                let p = 0, g = 0;
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  (o.segments || []).map(
                    /** @param {any} [segment] @returns {any} */
                    (k) => {
                      const f = k && typeof k.value == "number" && Number.isFinite(k.value) ? k.value : null;
                      if (f === null) return null;
                      const S = f >= 0 ? p : g, w = S + f;
                      f >= 0 ? p = w : g = w;
                      const y = dr[k.label] || Ae(0, r, je);
                      if (x.horizontalBarMode) {
                        const B = x.scaleX(S), D = x.scaleX(w);
                        return /* @__PURE__ */ e.jsx(
                          "rect",
                          {
                            x: Math.min(B, D),
                            y: c,
                            width: Math.max(1, Math.abs(D - B)),
                            height: u,
                            rx: "3",
                            fill: y,
                            opacity: "0.94",
                            "data-tip-cat": o.label,
                            "data-tip-ser": k.label,
                            "data-tip-val": P(f),
                            "data-tip-color": y
                          },
                          `${o.key}-${k.path || k.label}`
                        );
                      }
                      const I = x.scaleY(S), X = x.scaleY(w), C = Math.min(I, X), j = Math.max(1, Math.abs(X - I));
                      return /* @__PURE__ */ e.jsx(
                        "rect",
                        {
                          x: c,
                          y: C,
                          width: u,
                          height: j,
                          rx: "3",
                          fill: y,
                          opacity: "0.94",
                          "data-tip-cat": o.label,
                          "data-tip-ser": k.label,
                          "data-tip-val": P(f),
                          "data-tip-color": y
                        },
                        `${o.key}-${k.path || k.label}`
                      );
                    }
                  ),
                  x.horizontalBarMode ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: x.margin.left - 10,
                      y: c + u / 2 + 4,
                      textAnchor: "end",
                      fontSize: "11",
                      fill: r.textSec,
                      children: he(o.label, 22)
                    }
                  ) : /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: c + u / 2,
                      y: z - x.margin.bottom + 30,
                      textAnchor: "middle",
                      fontSize: "11",
                      fill: r.textSec,
                      children: he(o.label, 20)
                    }
                  )
                ] }, `stacked-group-${o.key}`);
              }
            ),
            !Ie && n === "bar" && t.categories.map(
              /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
              (o, l) => {
                const c = x.horizontalBarMode ? x.margin.top + l * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + l * x.step + (x.step - x.groupWidth) / 2, u = Lt || Ne ? Math.max(14, x.groupWidth * 0.64) : Math.max(6, x.groupWidth / Math.max(t.series.length, 1) - 4);
                let p = 0, g = 0;
                return /* @__PURE__ */ e.jsx("g", { children: t.series.map(
                  /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                  (k, f) => {
                    if (ee.has(k.name)) return null;
                    const S = k.values[l];
                    if (S == null) return null;
                    const w = ve(k.name, f), y = ae ? `url(#pg-grad-${f})` : w;
                    if (Lt || Ne) {
                      const j = Ne && me ? me[l] : null, B = Ne && j ? S / j * 100 : S, D = B >= 0 ? p : g, H = D + B;
                      B >= 0 ? p = H : g = H;
                      const T = Ne ? `${(Math.abs(S / j) * 100).toFixed(1)}% (${P(S)})` : P(S), le = { "data-tip-cat": o, "data-tip-ser": k.name, "data-tip-val": T, "data-tip-color": w };
                      if (x.horizontalBarMode) {
                        const oe = x.scaleX(D), A = x.scaleX(H);
                        return /* @__PURE__ */ e.jsx(
                          "rect",
                          {
                            x: Math.min(oe, A),
                            y: c,
                            width: Math.max(1, Math.abs(A - oe)),
                            height: u,
                            rx: "3",
                            fill: y,
                            opacity: "0.92",
                            style: { transition: ot ? "width 0.35s ease, x 0.35s ease" : void 0 },
                            ...le
                          },
                          `${k.name}-${f}-${o}-${l}`
                        );
                      }
                      const b = x.scaleY(D), q = x.scaleY(H);
                      return /* @__PURE__ */ e.jsx(
                        "rect",
                        {
                          x: c,
                          y: Math.min(b, q),
                          width: u,
                          height: Math.max(1, Math.abs(q - b)),
                          rx: "3",
                          fill: y,
                          opacity: "0.92",
                          style: { transition: ot ? "height 0.35s ease, y 0.35s ease" : void 0 },
                          ...le
                        },
                        `${k.name}-${f}-${o}-${l}`
                      );
                    }
                    if (x.horizontalBarMode) {
                      const j = x.scaleX(S), B = c + f * (u + 4);
                      return /* @__PURE__ */ e.jsxs(ft.Fragment, { children: [
                        /* @__PURE__ */ e.jsx(
                          "rect",
                          {
                            x: Math.min(j, x.baselineX),
                            y: B,
                            width: Math.max(1, Math.abs(x.baselineX - j)),
                            height: u,
                            rx: "3",
                            fill: y,
                            opacity: "0.9",
                            style: { transition: ot ? "width 0.35s ease, x 0.35s ease" : void 0 },
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
                            y: B + u / 2 + 3,
                            textAnchor: S >= 0 ? "start" : "end",
                            fontSize: "9",
                            fontWeight: "700",
                            fill: r.textSec,
                            style: { pointerEvents: "none" },
                            children: P(S)
                          }
                        ) : null
                      ] }, `${k.name}-${f}-${o}-${l}`);
                    }
                    const I = x.scaleY(S), X = Math.max(1, Math.abs(x.baselineY - I)), C = c + f * (u + 4);
                    return /* @__PURE__ */ e.jsxs(ft.Fragment, { children: [
                      /* @__PURE__ */ e.jsx(
                        "rect",
                        {
                          x: C,
                          y: Math.min(I, x.baselineY),
                          width: u,
                          height: X,
                          rx: "3",
                          fill: y,
                          opacity: "0.9",
                          style: { transition: ot ? "height 0.35s ease, y 0.35s ease" : void 0, cursor: xe ? "pointer" : void 0 },
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
                          x: C + u / 2,
                          y: S >= 0 ? Math.min(I, x.baselineY) - 4 : Math.max(I, x.baselineY) + 12,
                          textAnchor: "middle",
                          fontSize: "9",
                          fontWeight: "700",
                          fill: r.textSec,
                          style: { pointerEvents: "none" },
                          children: P(S)
                        }
                      ) : null
                    ] }, `${k.name}-${f}-${o}-${l}`);
                  }
                ) }, `bar-group-${o}-${l}`);
              }
            ),
            (n === "line" || n === "area") && (() => {
              const o = t.categories.map(() => 0), l = t.categories.map(() => 0);
              return t.series.map(
                /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                (c, u) => {
                  if (ee.has(c.name)) return null;
                  const p = ve(c.name, u), g = t.categories.map(
                    /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                    (S, w) => {
                      const y = c.values[w];
                      if (y == null) return null;
                      const I = x.margin.left + w * x.step + x.step / 2;
                      if (gt || Ne) {
                        const X = Ne && me ? me[w] : null, C = Ne && X ? y / X * 100 : y, j = C >= 0 ? o[w] : l[w], B = j + C;
                        return C >= 0 ? o[w] = B : l[w] = B, {
                          x: I,
                          y: x.scaleY(B),
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
                    }
                  ), k = pr(g), f = n === "area" ? Ir(g) : "";
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    n === "area" && f && /* @__PURE__ */ e.jsx("path", { d: f, fill: ae ? `url(#pg-grad-${u})` : p, opacity: gt || Ne ? "0.46" : "0.16", style: { pointerEvents: "none" } }),
                    k && /* @__PURE__ */ e.jsx("path", { d: k, fill: "none", stroke: p, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: hr(ht[c.name]), style: { pointerEvents: "none" } }),
                    g.map(
                      /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
                      (S, w) => S ? /* @__PURE__ */ e.jsxs(ft.Fragment, { children: [
                        ft.cloneElement(Bt(S.x, S.y, p, {
                          "data-tip-cat": t.categories[w],
                          "data-tip-ser": c.name,
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
                      ] }, `${c.name}-${w}`) : null
                    )
                  ] }, `${c.name}-${n}`);
                }
              );
            })(),
            kr && t.categories.map(
              /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
              (o, l) => {
                const c = Vt[l];
                return c ? x.horizontalBarMode ? /* @__PURE__ */ e.jsx(
                  "rect",
                  {
                    x: x.margin.left,
                    y: x.margin.top + l * x.step,
                    width: Z - x.margin.left - x.margin.right,
                    height: x.step,
                    fill: "transparent",
                    style: { cursor: "pointer" },
                    onClick: () => W(c)
                  },
                  `category-hit-horizontal-${l}`
                ) : /* @__PURE__ */ e.jsx(
                  "rect",
                  {
                    x: x.margin.left + l * x.step,
                    y: x.margin.top,
                    width: x.step,
                    height: z - x.margin.top - x.margin.bottom,
                    fill: "transparent",
                    style: { cursor: "pointer" },
                    onClick: () => W(c)
                  },
                  `category-hit-vertical-${l}`
                ) : null;
              }
            ),
            Ie ? null : x.horizontalBarMode ? x.hasHierarchicalBands ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              Qt.map(
                /** @param {any} [group] @returns {any} */
                (o) => {
                  const l = x.margin.top + o.startIndex * x.step, c = x.margin.top + (o.endIndex + 1) * x.step, u = (l + c) / 2;
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
                }
              ),
              t.categoryBands.map(
                /** @param {any} [band] @param {any} [categoryIndex] @returns {any} */
                (o, l) => {
                  const c = x.margin.top + l * x.step + x.step / 2, u = o && o.innerLabel ? String(o.innerLabel) : "";
                  return u ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: x.margin.left - 10,
                      y: c + 4,
                      textAnchor: "end",
                      fontSize: "11",
                      fill: r.textSec,
                      children: he(u, 18)
                    },
                    `inner-label-horizontal-${l}`
                  ) : null;
                }
              )
            ] }) : t.categories.map(
              /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
              (o, l) => {
                const c = x.margin.top + l * x.step + x.step / 2;
                return /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: x.margin.left - 10,
                    y: c + 4,
                    textAnchor: "end",
                    fontSize: "11",
                    fill: r.textSec,
                    children: he(o, 22)
                  },
                  `label-horizontal-${l}`
                );
              }
            ) : x.hasHierarchicalBands ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              Qt.map(
                /** @param {any} [group] @returns {any} */
                (o) => {
                  const l = x.margin.left + o.startIndex * x.step, c = x.margin.left + (o.endIndex + 1) * x.step, u = (l + c) / 2;
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
                }
              ),
              t.categoryBands.map(
                /** @param {any} [band] @param {any} [categoryIndex] @returns {any} */
                (o, l) => {
                  const c = x.margin.left + l * x.step + x.step / 2, u = o && o.innerLabel ? String(o.innerLabel) : "";
                  return /* @__PURE__ */ e.jsx("g", { children: u ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: c,
                      y: z - x.margin.bottom + 30,
                      textAnchor: "middle",
                      fontSize: "11",
                      fill: r.textSec,
                      children: he(u, 18)
                    }
                  ) : null }, `inner-label-${l}`);
                }
              )
            ] }) : t.categories.map(
              /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
              (o, l) => {
                if (It.visibleIndexes && !It.visibleIndexes.has(l)) return null;
                const c = x.margin.left + l * x.step + x.step / 2;
                return /* @__PURE__ */ e.jsx("g", { transform: `translate(${c}, ${z - x.margin.bottom + 18}) rotate(${ke})`, children: /* @__PURE__ */ e.jsxs("text", { textAnchor: ke === 0 ? "middle" : "start", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: xe ? { cursor: "pointer" } : void 0, children: [
                  /* @__PURE__ */ e.jsx("title", { children: String(o) }),
                  he(o, It.maxChars)
                ] }) }, `label-${l}`);
              }
            )
          ]
        }
      ),
      Re,
      Pe
    ] }),
    m ? /* @__PURE__ */ e.jsx(wt, { items: At, theme: r, hiddenSet: ee, onToggle: Ke, position: Oe, onElementSelect: xe }) : null
  ] });
}, cs = ({ chartType: t, barLayout: n, onChange: i, canStack: a, theme: r }) => {
  const h = (m, M = !1) => ({
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
    /* @__PURE__ */ e.jsx("button", { onClick: () => i("grouped"), style: h("grouped"), children: t === "area" ? "Normal" : "Grouped" }),
    /* @__PURE__ */ e.jsx(
      "button",
      {
        onClick: () => {
          a && i("stacked");
        },
        style: h("stacked", !a),
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
        style: h("stacked100", !a),
        title: a ? "Normalize each category to 100%" : "Needs multiple series",
        children: "100%"
      }
    )
  ] });
}, ds = ({ orientation: t, onChange: n, theme: i }) => {
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
}, us = ({ axisMode: t, onChange: n, theme: i }) => {
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
}, xs = ({ level: t, onChange: n, maxLevel: i, theme: a }) => {
  if (!i || i <= 1) return null;
  const r = (h) => ({
    border: `1px solid ${h === t ? a.primary : a.border}`,
    background: h === t ? a.select : a.headerSubtleBg || a.hover,
    color: h === t ? a.primary : a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer",
    minWidth: "34px"
  });
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { onClick: () => n("all"), style: r("all"), children: "All" }),
    Array.from({ length: i }, (h, m) => m + 1).map((h) => /* @__PURE__ */ e.jsx("button", { onClick: () => n(h), style: r(h), children: h }, h))
  ] });
}, ps = ({ title: t, subtitle: n, note: i, theme: a }) => !t && !n && !i ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px" }, children: [
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
] }), fs = ({
  rowLimit: t,
  onRowLimitChange: n,
  columnLimit: i,
  onColumnLimitChange: a,
  theme: r
}) => {
  const h = {
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
  }, M = (W, R) => (
    /** @param {any} [event] @returns {any} */
    (($) => {
      W && W(An($.target.value, R));
    })
  );
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
          onChange: M(n, Zr),
          style: h
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
          onChange: M(a, Kr),
          style: h
        }
      )
    ] })
  ] });
}, hs = ({
  layers: t,
  onChange: n,
  availableColumns: i,
  theme: a
}) => {
  const r = $r(i), h = kn(t, r), [m, M] = v.useState(""), W = v.useMemo(
    () => wn(r),
    [r]
  ), R = v.useMemo(
    () => new Map(W.map(
      /** @param {any} [option] @returns {any} */
      (Y) => [Y.id, Y]
    )),
    [W]
  ), $ = m.trim().toLowerCase(), O = v.useMemo(
    () => $ ? W.filter(
      /** @param {any} [option] @returns {any} */
      (Y) => Y.searchText.includes($)
    ) : W,
    [W, $]
  ), J = v.useMemo(() => {
    const Y = /* @__PURE__ */ new Map();
    return O.forEach(
      /** @param {any} [option] @returns {any} */
      (F) => {
        Y.has(F.groupLabel) || Y.set(F.groupLabel, []), Y.get(F.groupLabel).push(F);
      }
    ), Array.from(Y.entries()).map(([F, U]) => ({ label: F, options: U }));
  }, [O]), K = {
    border: `1px solid ${a.border}`,
    background: a.surfaceBg || a.background || "#fff",
    color: a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700
  }, s = (Y, F) => {
    typeof n == "function" && n(h.map(
      /** @param {any} [layer] @returns {any} */
      (U) => U.id === Y ? { ...U, ...F || {} } : U
    ));
  }, _ = (Y, F) => {
    if (typeof n != "function") return;
    const U = h.findIndex(
      /** @param {any} [layer] @returns {any} */
      (ue) => ue.id === Y
    ), de = U + F;
    if (U < 0 || de < 0 || de >= h.length) return;
    const ne = [...h], [ae] = ne.splice(U, 1);
    ne.splice(de, 0, ae), n(ne);
  }, G = (Y) => {
    if (typeof n != "function") return;
    const F = h.filter(
      /** @param {any} [layer] @returns {any} */
      (U) => U.id !== Y
    );
    n(F.length > 0 ? F : Sn(r, 1));
  }, V = () => {
    if (typeof n != "function") return;
    const Y = new Set(h.map(
      /** @param {any} [layer] @returns {any} */
      (de) => de.columnId
    )), F = r.find(
      /** @param {any} [column] @returns {any} */
      (de) => !Y.has(de.id)
    ) || r[0];
    if (!F) return;
    const U = Mn({
      id: `layer-${h.length + 1}`,
      type: Vr[h.length % Vr.length],
      columnId: F.id,
      axis: h.length === 1 ? "right" : "left",
      name: Cn(F)
    }, r, h.length);
    n([...h, U]);
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
    h.map(
      /** @param {any} [layer] @param {any} [layerIndex] @returns {any} */
      (Y, F) => {
        const U = R.get(Y.columnId) || null, de = J.some(
          /** @param {any} [group] @returns {any} */
          (ne) => ne.options.some(
            /** @param {any} [option] @returns {any} */
            (ae) => ae.id === Y.columnId
          )
        );
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
                      disabled: F === h.length - 1,
                      style: {
                        border: `1px solid ${a.border}`,
                        background: a.headerSubtleBg || a.hover,
                        color: a.textSec,
                        borderRadius: a.radiusSm || "8px",
                        padding: "6px 8px",
                        fontSize: "11px",
                        fontWeight: 700,
                        cursor: F === h.length - 1 ? "not-allowed" : "pointer",
                        opacity: F === h.length - 1 ? 0.45 : 1
                      },
                      children: "Down"
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => G(Y.id),
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
                    qe,
                    {
                      size: "sm",
                      "aria-label": "Measure",
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
                        J.map(
                          /** @param {any} [group] @returns {any} */
                          (ne) => /* @__PURE__ */ e.jsx("optgroup", { label: `${ne.label} (${ne.options.length})`, children: ne.options.map(
                            /** @param {any} [option] @returns {any} */
                            (ae) => /* @__PURE__ */ e.jsx("option", { value: ae.id, children: ae.leafLabel }, ae.id)
                          ) }, ne.label)
                        )
                      ]
                    }
                  )
                ] }),
                /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: [
                  /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Layer Type" }),
                  /* @__PURE__ */ e.jsxs(
                    qe,
                    {
                      size: "sm",
                      "aria-label": "Layer Type",
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
                    qe,
                    {
                      size: "sm",
                      "aria-label": "Axis",
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
                    qe,
                    {
                      size: "sm",
                      "aria-label": "Format",
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
      }
    ),
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
}, gs = ({ sortMode: t, onChange: n, theme: i }) => {
  const a = [
    { value: "natural", label: "Natural" },
    { value: "value_desc", label: "Value Desc" },
    { value: "value_asc", label: "Value Asc" },
    { value: "label_asc", label: "Label A-Z" },
    { value: "label_desc", label: "Label Z-A" }
  ];
  return /* @__PURE__ */ e.jsx(
    qe,
    {
      size: "sm",
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
}, ys = ({ interactionMode: t, onChange: n, theme: i }) => {
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
}, bs = ({ scope: t, onChange: n, theme: i }) => {
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
}, nr = "http://www.w3.org/2000/svg", Lr = (t, n) => {
  if (!t || typeof document > "u") return null;
  const i = br(n), a = t.cloneNode(!0), r = (a.getAttribute("viewBox") || "").split(/[\s,]+/).map(Number), h = r.length === 4 && r[2] > 0 ? r[2] : t.clientWidth || 800, m = r.length === 4 && r[3] > 0 ? r[3] : t.clientHeight || 400, M = i.title ? 42 : 0, W = Qr(
    i.legendItems,
    h,
    /** @param {any} [label] @returns {any} */
    (G) => String(G || "").length * 6.4
  ), R = W.length > 0 ? W.length * 22 + 14 : 0, $ = i.legendPosition === "top" ? R : 0, O = M + $, J = M + m + R, K = Array.from(a.childNodes), s = document.createElementNS(nr, "g");
  s.setAttribute("transform", `translate(0 ${O})`), K.forEach(
    /** @param {any} [child] @returns {any} */
    (G) => s.appendChild(G)
  ), a.appendChild(s), a.setAttribute("viewBox", `0 0 ${h} ${J}`), a.setAttribute("width", String(h)), a.setAttribute("height", String(J)), a.setAttribute("xmlns", nr);
  const _ = document.createElementNS(nr, "rect");
  if (_.setAttribute("x", "0"), _.setAttribute("y", "0"), _.setAttribute("width", String(h)), _.setAttribute("height", String(J)), _.setAttribute("fill", i.background), a.insertBefore(_, s), i.title) {
    const G = document.createElementNS(nr, "text");
    G.setAttribute("x", "16"), G.setAttribute("y", "27"), G.setAttribute("fill", i.textColor), G.setAttribute("font-size", "16"), G.setAttribute("font-weight", "700"), G.setAttribute("font-family", "Inter, Arial, sans-serif"), G.textContent = i.title, a.appendChild(G);
  }
  if (W.length > 0) {
    const G = i.legendPosition === "top" ? M + 16 : M + m + 18;
    W.forEach(
      /** @param {any} [row] @param {any} [rowIndex] @returns {any} */
      (V, Y) => {
        let F = 16;
        V.forEach(
          /** @param {any} [item] @returns {any} */
          (U) => {
            const de = G + Y * 22, ne = document.createElementNS(nr, "circle");
            ne.setAttribute("cx", String(F + 5)), ne.setAttribute("cy", String(de - 4)), ne.setAttribute("r", "5"), ne.setAttribute("fill", U.hidden ? i.secondaryTextColor : U.color || "#2563eb"), ne.setAttribute("opacity", U.hidden ? "0.35" : "1"), a.appendChild(ne);
            const ae = document.createElementNS(nr, "text");
            ae.setAttribute("x", String(F + 16)), ae.setAttribute("y", String(de)), ae.setAttribute("fill", i.secondaryTextColor), ae.setAttribute("font-size", "11"), ae.setAttribute("font-family", "Inter, Arial, sans-serif"), ae.setAttribute("opacity", U.hidden ? "0.45" : "1"), ae.textContent = U.label, a.appendChild(ae), F += U.width;
          }
        );
      }
    );
  }
  return a;
}, vs = async (t, n, i) => {
  if (!t || typeof window > "u") return;
  const a = Lr(t, i);
  if (!a) return;
  const h = new XMLSerializer().serializeToString(a), m = new Blob([h], { type: "image/svg+xml;charset=utf-8" });
  yr(m, `${n}.svg`);
}, Jr = (t, n = 2) => new Promise((i) => {
  if (!t) {
    i(null);
    return;
  }
  const r = new XMLSerializer().serializeToString(t), h = t.getAttribute("viewBox");
  let m = t.clientWidth || 800, M = t.clientHeight || 400;
  if (h) {
    const K = h.split(/[\s,]+/).map(Number);
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
}), ms = async (t, n, i) => {
  const a = await Jr(Lr(t, i));
  a && a.toBlob(
    /** @param {any} [blob] @returns {any} */
    (r) => {
      r && yr(r, `${n}.png`);
    },
    "image/png"
  );
}, js = async (t, n) => {
  if (typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  const i = await Jr(Lr(t, n));
  if (!i) return !1;
  try {
    const a = await new Promise((r) => i.toBlob(r, "image/png"));
    return a ? (await navigator.clipboard.write([new ClipboardItem({ "image/png": a })]), !0) : !1;
  } catch {
    return !1;
  }
}, pt = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), ks = (t) => new Promise((n) => {
  const i = new Image();
  i.onload = () => n(i), i.onerror = () => n(null), i.src = t;
}), en = async (t, n) => {
  const i = t && t.current && t.current.getEchartsInstance();
  if (!i) return null;
  const a = br(n), r = i.getDataURL({ type: "png", pixelRatio: 2 }), h = await ks(r);
  if (!h) return null;
  const m = Math.max(1, h.naturalWidth / Math.max(1, i.getWidth())), M = a.title ? Math.round(42 * m) : 0, R = document.createElement("canvas").getContext("2d");
  R.font = "11px Inter, Arial, sans-serif";
  const $ = Qr(
    a.legendItems,
    i.getWidth(),
    /** @param {any} [label] @returns {any} */
    (G) => R.measureText(String(G || "")).width
  ), O = $.length > 0 ? Math.round(($.length * 22 + 14) * m) : 0, J = a.legendPosition === "top" ? O : 0, K = M + J, s = document.createElement("canvas");
  s.width = h.naturalWidth, s.height = M + h.naturalHeight + O;
  const _ = s.getContext("2d");
  if (_.fillStyle = a.background, _.fillRect(0, 0, s.width, s.height), _.drawImage(h, 0, K), a.title && (_.fillStyle = a.textColor, _.font = `700 ${Math.round(16 * m)}px Inter, Arial, sans-serif`, _.textBaseline = "alphabetic", _.fillText(a.title, Math.round(16 * m), Math.round(27 * m))), $.length > 0) {
    const G = a.legendPosition === "top" ? M + Math.round(16 * m) : M + h.naturalHeight + Math.round(18 * m);
    _.font = `${Math.round(11 * m)}px Inter, Arial, sans-serif`, $.forEach(
      /** @param {any} [row] @param {any} [rowIndex] @returns {any} */
      (V, Y) => {
        let F = Math.round(16 * m);
        V.forEach(
          /** @param {any} [item] @returns {any} */
          (U) => {
            const de = G + Math.round(Y * 22 * m);
            _.globalAlpha = U.hidden ? 0.35 : 1, _.fillStyle = U.hidden ? a.secondaryTextColor : U.color || "#2563eb", _.beginPath(), _.arc(F + Math.round(5 * m), de - Math.round(4 * m), Math.round(5 * m), 0, Math.PI * 2), _.fill(), _.globalAlpha = U.hidden ? 0.45 : 1, _.fillStyle = a.secondaryTextColor, _.fillText(U.label, F + Math.round(16 * m), de), _.globalAlpha = 1, F += Math.round(U.width * m);
          }
        );
      }
    );
  }
  return s;
}, ws = async (t, n, i) => {
  const a = await en(t, i);
  a && a.toBlob(
    /** @param {any} [blob] @returns {any} */
    (r) => {
      r && yr(r, `${n}.png`);
    },
    "image/png"
  );
}, Ss = async (t, n) => {
  if (!(t && t.current && t.current.getEchartsInstance()) || typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  try {
    const a = await en(t, n);
    if (!a) return !1;
    const r = await new Promise((h) => a.toBlob(h, "image/png"));
    return r ? (await navigator.clipboard.write([new ClipboardItem({ "image/png": r })]), !0) : !1;
  } catch {
    return !1;
  }
}, tn = ({
  model: t,
  chartType: n,
  chartLayers: i,
  onChartLayersChange: a,
  availableColumns: r,
  barLayout: h,
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
  onTitleChange: G,
  subtitle: V,
  note: Y,
  chartHeight: F,
  onChartHeightChange: U,
  rowLimit: de,
  onRowLimitChange: ne,
  columnLimit: ae,
  onColumnLimitChange: ue,
  sortMode: ot,
  onSortModeChange: be,
  interactionMode: it,
  onInteractionModeChange: _e,
  serverScope: Ot,
  onServerScopeChange: at,
  showServerScope: Qe = !1,
  floating: Oe = !1,
  immersiveMode: Me = !1,
  onCategoryActivate: Xt,
  allowHierarchyCharts: or = !0,
  locked: rt = !1,
  onToggleLock: Tt,
  configOpen: Gt,
  onConfigChange: Je,
  settingsPanelWidth: ht,
  onSettingsPanelWidthChange: Ut,
  initialSettings: L = {},
  onSettingsChange: zt
}) => {
  const xe = n === "combo", je = n === "sparkline", P = xe ? !1 : n === "area" ? qr(t) : Gr(t), Ie = _ !== void 0 ? _ : t && t.title, Lt = V !== void 0 ? V : t && t.subtitle, gt = Y !== void 0 ? Y : rt ? Oe ? "Locked to the current chart request and floating frame" : "Locked to the current chart request" : t && t.note, [lt, Ne] = v.useState(!1), ke = typeof Gt == "boolean" ? Gt : lt, z = typeof Je == "function" ? Je : Ne, [ve, Bt] = v.useState(() => (L == null ? void 0 : L.showDataLabels) ?? !1), [me, yt] = v.useState(() => (L == null ? void 0 : L.colorPalette) ?? "default"), [ct, He] = v.useState(() => (L == null ? void 0 : L.valueFormat) ?? "auto"), [St, ir] = v.useState(() => (L == null ? void 0 : L.yAxisTitle) ?? ""), [Z, ee] = v.useState(() => (L == null ? void 0 : L.yAxisMin) ?? ""), [Et, Ke] = v.useState(() => (L == null ? void 0 : L.yAxisMax) ?? ""), [$e, Xe] = v.useState(() => (L == null ? void 0 : L.referenceLines) ?? []), [Te, bt] = v.useState({ value: "", label: "", orient: "h" }), [dt, Pe] = v.useState(() => (L == null ? void 0 : L.labelAngle) ?? 0), [Be, Yt] = v.useState(() => (L == null ? void 0 : L.chartSubtitle) ?? ""), [nt, Ee] = v.useState(() => (L == null ? void 0 : L.tickCount) ?? 5), [Ze, vt] = v.useState(() => (L == null ? void 0 : L.markerShape) ?? "circle"), [mt, st] = v.useState(() => (L == null ? void 0 : L.markerSize) ?? 4), [Ge, ut] = v.useState(() => (L == null ? void 0 : L.showGradient) ?? !1), [Fe, Rt] = v.useState(() => (L == null ? void 0 : L.seriesColors) ?? {}), [xt, Mt] = v.useState(() => (L == null ? void 0 : L.showAnimations) ?? !0), [We, te] = v.useState(() => (L == null ? void 0 : L.showCalloutLabels) ?? !1), [Le, Ue] = v.useState(() => (L == null ? void 0 : L.showTrendline) ?? !1), [we, Ye] = v.useState(() => (L == null ? void 0 : L.trendlineMode) ?? "linear"), [et, ar] = v.useState(() => (L == null ? void 0 : L.trendlinePeriod) ?? 3), [jt, cr] = v.useState(() => (L == null ? void 0 : L.legendPosition) ?? "bottom"), [Dt, Kt] = v.useState(() => (L == null ? void 0 : L.labelPosition) ?? "outside"), [Zt, qt] = v.useState(() => (L == null ? void 0 : L.decimalPlaces) ?? -1), [Nt, vr] = v.useState(() => (L == null ? void 0 : L.gradientDirection) ?? "vertical"), [Pt, mr] = v.useState(() => (L == null ? void 0 : L.gradientOpacity) ?? 70), [Re, x] = v.useState(() => (L == null ? void 0 : L.trendlineColor) ?? ""), [It, jr] = v.useState(() => (L == null ? void 0 : L.trendlineWidth) ?? 2), [Ct, dr] = v.useState(() => (L == null ? void 0 : L.seriesSort) ?? "none"), [At, Qt] = v.useState(() => (L == null ? void 0 : L.customPaletteColors) ?? []), [Vt, kr] = v.useState(() => (L == null ? void 0 : L.lineDashStyles) ?? {}), [o, l] = v.useState(() => (L == null ? void 0 : L.yAxisMinRight) ?? ""), [c, u] = v.useState(() => (L == null ? void 0 : L.yAxisMaxRight) ?? ""), [p, g] = v.useState("chart"), [k, f] = v.useState(!0), [S, w] = v.useState(!0), y = v.useCallback((d) => {
    const ge = { series: "style", axis: "axes", legend: "labels", title: "data" }[d];
    ge && (z(!0), g(ge));
  }, [z]), I = Tr(me), [X, C] = v.useState(String(F || sr)), j = v.useRef(null), B = v.useRef(null), D = v.useRef(null), [H, T] = v.useState(null), [le, b] = v.useState(!1), { anchorRef: q, menuRef: oe, position: A, portalTarget: E } = Wn(le, b), [ce, ie] = v.useState(null), [se, N] = v.useState(348), re = Number.isFinite(Number(ht)) ? Math.max(300, Math.min(680, Math.floor(Number(ht)))) : se, fe = typeof Ut == "function" ? Ut : N, De = !Me, kt = Me && ce ? Math.max(180, ce - (Me && !je && jt !== "none" ? 44 : 0) - 44) : Math.max(180, Number.isFinite(Number(F)) ? Number(F) : sr), er = v.useRef(null), Ht = v.useRef(null), wr = v.useRef(null), Br = t && t.maxHierarchyLevel || 1, ur = typeof K == "function" && Br > 1, Er = {
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
  }, rn = {
    ...Er,
    border: `1px solid ${rt ? s.primary : s.border}`,
    background: rt ? s.select : s.headerSubtleBg || s.hover,
    color: rt ? s.primary : s.textSec
  }, _t = {
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
  }, nn = v.useCallback((d) => {
    if (d === "trend") {
      M("line"), typeof O == "function" && O("rows"), typeof be == "function" && be("natural");
      return;
    }
    if (d === "compare") {
      M("bar"), typeof W == "function" && W("grouped"), typeof R == "function" && R("vertical");
      return;
    }
    if (d === "composition") {
      M(ur ? "sunburst" : "donut");
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
    ur
  ]);
  v.useEffect(() => {
    !Me || !ke || z(!1);
  }, [Me, ke]), v.useEffect(() => {
    if (!H) return;
    const d = setTimeout(() => T(null), 2500);
    return () => clearTimeout(d);
  }, [H]);
  const xr = v.useCallback(() => Jn(
    wr.current,
    Ie,
    jt,
    s
  ), [jt, Ie, s]), sn = v.useCallback(async (d) => {
    b(!1);
    const Q = xr();
    if (d === "svg") {
      pt.has(n) || await vs(er.current, "pivot-chart", Q);
      return;
    }
    if (d === "png") {
      pt.has(n) ? await ws(Ht, "pivot-chart", Q) : await ms(er.current, "pivot-chart", Q);
      return;
    }
    d === "csv" && es(t, "pivot-chart", Q);
  }, [n, xr, t]), on = v.useCallback(async () => {
    const d = xr(), Q = pt.has(n) ? await Ss(Ht, d) : await js(er.current, d);
    T(Q ? { tone: "success", text: "Copied chart" } : { tone: "error", text: "Copy failed" });
  }, [n, xr]);
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
  const Sr = v.useRef(U);
  v.useEffect(() => {
    Sr.current = U;
  }), v.useEffect(() => {
    const d = (ge) => {
      if (!j.current || typeof Sr.current != "function") return;
      const ye = j.current, Ve = Math.max(180, ye.startHeight + (ge.clientY - ye.startY));
      Sr.current(Ve);
    }, Q = () => {
      j.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", Q), window.addEventListener("blur", Q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", Q), window.removeEventListener("blur", Q);
    };
  }, []), v.useEffect(() => {
    const d = (ge) => {
      if (!B.current) return;
      const ye = B.current, Ve = ye.direction === "left" ? ye.startX - ge.clientX : ge.clientX - ye.startX, tr = Math.max(300, Math.min(680, ye.startWidth + Ve));
      fe(tr);
    }, Q = () => {
      B.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", Q), window.addEventListener("blur", Q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", Q), window.removeEventListener("blur", Q);
    };
  }, []), v.useEffect(() => {
    const d = wr.current;
    if (!d || typeof ResizeObserver > "u") return;
    let Q = d.clientWidth;
    const ge = new ResizeObserver(() => {
      var Ve, tr;
      const ye = d.clientWidth;
      if (ye !== Q) {
        Q = ye;
        const Dr = (tr = (Ve = Ht.current) == null ? void 0 : Ve.getEchartsInstance) == null ? void 0 : tr.call(Ve);
        Dr && Dr.resize();
      }
    });
    return ge.observe(d), () => ge.disconnect();
  }, []), v.useEffect(() => {
    C(String(kt));
  }, [kt]);
  const Mr = v.useRef(zt);
  v.useEffect(() => {
    Mr.current = zt;
  });
  const Rr = v.useRef(!0);
  v.useEffect(() => {
    if (Rr.current) {
      Rr.current = !1;
      return;
    }
    typeof Mr.current == "function" && Mr.current({
      showDataLabels: ve,
      colorPalette: me,
      valueFormat: ct,
      yAxisTitle: St,
      yAxisMin: Z,
      yAxisMax: Et,
      referenceLines: $e,
      labelAngle: dt,
      chartSubtitle: Be,
      tickCount: nt,
      markerShape: Ze,
      markerSize: mt,
      showGradient: Ge,
      seriesColors: Fe,
      showAnimations: xt,
      showCalloutLabels: We,
      showTrendline: Le,
      trendlineMode: we,
      trendlinePeriod: et,
      legendPosition: jt,
      labelPosition: Dt,
      decimalPlaces: Zt,
      gradientDirection: Nt,
      gradientOpacity: Pt,
      trendlineColor: Re,
      trendlineWidth: It,
      seriesSort: Ct,
      customPaletteColors: At,
      lineDashStyles: Vt,
      yAxisMinRight: o,
      yAxisMaxRight: c
    });
  }, [
    ve,
    me,
    ct,
    St,
    Z,
    Et,
    $e,
    dt,
    Be,
    nt,
    Ze,
    mt,
    Ge,
    Fe,
    xt,
    We,
    Le,
    we,
    et,
    jt,
    Dt,
    Zt,
    Nt,
    Pt,
    Re,
    It,
    Ct,
    At,
    Vt,
    o,
    c
  ]);
  const Cr = v.useMemo(() => {
    if (!t || !Array.isArray(t.series)) return null;
    let d = 1 / 0, Q = -1 / 0;
    return t.series.forEach(
      /** @param {any} [ser] @returns {any} */
      (ge) => {
        (ge.values || []).forEach(
          /** @param {any} [v] @returns {any} */
          (ye) => {
            typeof ye == "number" && Number.isFinite(ye) && (d = Math.min(d, ye), Q = Math.max(Q, ye));
          }
        );
      }
    ), Number.isFinite(d) && d !== Q ? { min: d, max: Q } : null;
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
            ps,
            {
              title: Me ? Ie : null,
              subtitle: Lt,
              note: gt,
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
                  onClick: () => b(
                    /** @param {any} [isOpen] @returns {any} */
                    (d) => !d
                  ),
                  onKeyDown: (d) => {
                    (d.key === "ArrowDown" || d.key === "ArrowUp") && (d.preventDefault(), b(!0));
                  },
                  style: _t,
                  title: "Export chart",
                  "aria-haspopup": "menu",
                  "aria-expanded": le,
                  children: /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "5px" }, children: [
                    "Export ",
                    /* @__PURE__ */ e.jsx(tt.ChevronDown, {})
                  ] })
                }
              ),
              le && E ? hn.createPortal(
                /* @__PURE__ */ e.jsx(
                  "div",
                  {
                    ref: oe,
                    "data-pivot-workspace-root": "true",
                    className: "sp-menu-surface",
                    role: "menu",
                    "aria-label": "Chart export formats",
                    style: {
                      position: "fixed",
                      ...A,
                      visibility: A ? "visible" : "hidden",
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
                      { format: "svg", label: "SVG image", hint: "Title + legend included", disabled: pt.has(n) },
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
                          (Q = q.current) == null || Q.focus(), sn(d.format);
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
                onClick: on,
                style: _t,
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
            typeof Tt == "function" ? /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                onClick: Tt,
                style: { ...rn, width: "auto", padding: "0 10px", gap: "6px", fontSize: "11px", fontWeight: 800 },
                title: rt ? "Data is fixed to the saved selection. Click to resume live pivot updates." : "Keep this chart fixed to the current data selection while the pivot changes.",
                "aria-label": rt ? "Unlock chart data and resume live updates" : "Lock chart to the current data selection",
                "aria-pressed": rt,
                children: [
                  /* @__PURE__ */ e.jsx(tt.Lock, {}),
                  /* @__PURE__ */ e.jsx("span", { children: rt ? "Data locked" : "Lock data" })
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
                style: Er,
                title: ke ? "Hide chart settings" : "Show chart settings",
                "aria-label": ke ? "Hide chart settings" : "Show chart settings",
                children: /* @__PURE__ */ e.jsx(tt.Settings, {})
              }
            ) : null
          ] })
        ] }) : null,
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", minHeight: 0, gap: 0, overflow: "hidden", flex: "1 1 auto" }, children: [
          /* @__PURE__ */ e.jsxs(
            "div",
            {
              ref: wr,
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
                t ? (!Array.isArray(t.series) || t.series.length === 0) && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsx(Se, { message: t.emptyMessage || "No chart data available.", theme: s, chartHeight: kt }) : /* @__PURE__ */ e.jsx(ls, { model: t, chartType: n, barLayout: h, axisMode: m, theme: s, chartHeight: kt, showLegend: !je, showDataLabels: ve, onCategoryActivate: Xt, svgRef: er, echartsRef: Ht, colorPalette: me, valueFormat: ct, yAxisTitle: St, yAxisMin: Z, yAxisMax: Et, referenceLines: $e, onReferenceLinesChange: Xe, labelAngle: dt, chartTitle: Me ? "" : Ie, onTitleChange: G, subtitle: Be, tickCount: nt, markerShape: Ze, markerSize: mt, showGradient: Ge, seriesColors: Fe, showAnimations: xt, showCalloutLabels: We, showTrendline: Le, trendlineMode: we, trendlinePeriod: et, legendPosition: jt, labelPosition: Dt, decimalPlaces: Zt, gradientDirection: Nt, gradientOpacity: Pt, trendlineColor: Re, trendlineWidth: It, seriesSort: Ct, lineDashStyles: Vt, yAxisMinRight: o, yAxisMaxRight: c, customPaletteColors: At, onElementSelect: y }) : /* @__PURE__ */ e.jsx(Se, { message: "No chart data available.", theme: s, chartHeight: kt }),
                Be ? /* @__PURE__ */ e.jsx("div", { style: { textAlign: "center", fontSize: "11px", color: s.textSec, padding: "4px 8px 0", fontStyle: "italic" }, children: Be }) : null,
                !Me && typeof U == "function" ? /* @__PURE__ */ e.jsxs(
                  "div",
                  {
                    onMouseDown: (d) => {
                      d.preventDefault(), j.current = {
                        startY: d.clientY,
                        startHeight: kt
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
                  d.preventDefault(), B.current = {
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
                        onClick: () => g(d.id),
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
                        children: /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(tt.Close, {}) })
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
                          /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => f(
                            /** @param {any} [o] @returns {any} */
                            (d) => !d
                          ), style: { display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", borderBottom: `1px solid ${s.border}`, padding: "4px 0", cursor: "pointer", marginBottom: k ? "6px" : 0 }, children: [
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec }, children: "Chart Type" }),
                            /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", color: s.textSec, transform: k ? "scale(0.85) rotate(0deg)" : "scale(0.85) rotate(-90deg)", transition: "transform 0.15s" }, children: /* @__PURE__ */ e.jsx(tt.ChevronDown, {}) })
                          ] }),
                          k && /* @__PURE__ */ e.jsx(Kn, { chartType: n, onChange: M, theme: s, includeHierarchyCharts: or }),
                          /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => w(
                            /** @param {any} [o] @returns {any} */
                            (d) => !d
                          ), style: { display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", borderBottom: `1px solid ${s.border}`, padding: "4px 0", cursor: "pointer", marginTop: "8px", marginBottom: S ? "6px" : 0 }, children: [
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec }, children: "Parameters" }),
                            /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", color: s.textSec, transform: S ? "scale(0.85) rotate(0deg)" : "scale(0.85) rotate(-90deg)", transition: "transform 0.15s" }, children: /* @__PURE__ */ e.jsx(tt.ChevronDown, {}) })
                          ] }),
                          S && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !pt.has(n) && !xe ? /* @__PURE__ */ e.jsx(pe, { label: "Source", theme: s, children: /* @__PURE__ */ e.jsx(ds, { orientation: $, onChange: O, theme: s }) }) : null,
                            n === "bar" ? /* @__PURE__ */ e.jsx(pe, { label: "Direction", theme: s, children: /* @__PURE__ */ e.jsx(us, { axisMode: m, onChange: R, theme: s }) }) : null,
                            (n === "bar" || n === "area") && !xe && /* @__PURE__ */ e.jsx(pe, { label: "Layout", theme: s, children: /* @__PURE__ */ e.jsx(cs, { chartType: n, barLayout: h, onChange: W, canStack: P, theme: s }) }),
                            ur ? /* @__PURE__ */ e.jsx(pe, { label: "Level", theme: s, children: /* @__PURE__ */ e.jsx(xs, { level: J, onChange: K, maxLevel: Br, theme: s }) }) : null,
                            xe ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Layers" }),
                              /* @__PURE__ */ e.jsx(hs, { layers: i, onChange: a, availableColumns: r, theme: s })
                            ] }) : null,
                            !(n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !pt.has(n) && !xe) && n !== "bar" && !((n === "bar" || n === "area") && !xe) && !ur && !xe ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: s.textSec, padding: "4px 0" }, children: "No parameters for this chart type." }) : null
                          ] })
                        ] }),
                        p === "data" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "General" }),
                          typeof G == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: Ie || "", onChange: (d) => G(d.target.value), placeholder: "Chart title", style: { ...Ce, width: "100%" } }) }) : null,
                          /* @__PURE__ */ e.jsx(pe, { label: "Subtitle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: Be, onChange: (d) => Yt(d.target.value), placeholder: "Chart subtitle", style: { ...Ce, width: "100%" } }) }),
                          typeof U == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Height", theme: s, controlMinWidth: "96px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "180", step: "20", value: X, onChange: (d) => C(d.target.value), onBlur: () => {
                            typeof U == "function" && U(X);
                          }, onKeyDown: (d) => {
                            d.key === "Enter" && typeof U == "function" && U(X);
                          }, style: Ce }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Data & Actions" }),
                          /* @__PURE__ */ e.jsx(pe, { label: "Limits", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(fs, { rowLimit: de, onRowLimitChange: ne, columnLimit: ae, onColumnLimitChange: ue, theme: s }) }),
                          typeof be == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(gs, { sortMode: ot, onChange: be, theme: s }) }) : null,
                          typeof be == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Series Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px", flexWrap: "wrap" }, children: En.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => dr(d.value), style: { border: `1px solid ${Ct === d.value ? s.primary : s.border}`, background: Ct === d.value ? s.select : s.headerSubtleBg || s.hover, color: Ct === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }) : null,
                          typeof _e == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Click", theme: s, children: /* @__PURE__ */ e.jsx(ys, { interactionMode: it, onChange: _e, theme: s }) }) : null,
                          Qe && typeof at == "function" ? /* @__PURE__ */ e.jsx(pe, { label: "Scope", theme: s, children: /* @__PURE__ */ e.jsx(bs, { scope: Ot, onChange: at, theme: s }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Presets" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "6px" }, children: [
                            { key: "trend", label: "Trend" },
                            { key: "compare", label: "Compare" },
                            { key: "composition", label: "Composition" },
                            { key: "flow", label: "Flow" },
                            { key: "sparkline", label: "Sparkline" }
                          ].map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", "data-chart-preset": d.key, onClick: () => nn(d.key), style: { ..._t, textAlign: "left" }, children: d.label }, d.key)) })
                        ] }),
                        p === "axes" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(pe, { label: "Y-Axis Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: St, onChange: (d) => ir(d.target.value), placeholder: "Y-axis title", style: { ...Ce, width: "100%" } }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !pt.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(pe, { label: "Y Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: Z, onChange: (d) => ee(d.target.value), placeholder: "Min", style: { ...Ce, width: "50%" } }),
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: Et, onChange: (d) => Ke(d.target.value), placeholder: "Max", style: { ...Ce, width: "50%" } })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Y Ticks", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "20", value: nt, onChange: (d) => Ee(Math.max(2, Math.min(20, Number(d.target.value) || 5))), style: { ...Ce, width: "80px" } }) }),
                            /* @__PURE__ */ e.jsx(pe, { label: "X Angle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center", flexWrap: "wrap" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", min: "0", max: "90", value: dt, onChange: (d) => Pe(Math.max(0, Math.min(90, Number(d.target.value) || 0))), style: { ...Ce, width: "58px" }, title: "X-axis label angle (0–90°)" }),
                              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.textSec }, children: "°" }),
                              [0, 30, 45, 90].map((d) => /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => Pe(d), style: { ...Ce, padding: "4px 6px", background: dt === d ? s.select : s.headerSubtleBg || s.hover, color: dt === d ? s.primary : s.text, border: `1px solid ${dt === d ? s.primary : s.border}` }, children: [
                                d,
                                "°"
                              ] }, d))
                            ] }) })
                          ] }) : null,
                          xe ? /* @__PURE__ */ e.jsx(pe, { label: "Right Axis Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: o, onChange: (d) => l(d.target.value), placeholder: "Min", style: { ...Ce, width: "50%" } }),
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: c, onChange: (d) => u(d.target.value), placeholder: "Max", style: { ...Ce, width: "50%" } })
                          ] }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && !pt.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Reference Lines" }),
                            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: [
                              $e.map(
                                /** @param {any} [line] @param {any} [lineIdx] @returns {any} */
                                (d, Q) => /* @__PURE__ */ e.jsx(Zn, { line: d, onUpdate: (
                                  /** @param {any} [updated] @returns {any} */
                                  ((ge) => Xe(
                                    /** @param {any} [prev] @returns {any} */
                                    (ye) => ye.map(
                                      /** @param {any} [l] @param {any} [j] @returns {any} */
                                      (Ve, tr) => tr === Q ? ge : Ve
                                    )
                                  ))
                                ), onRemove: () => Xe(
                                  /** @param {any} [prev] @returns {any} */
                                  (ge) => ge.filter(
                                    /** @param {any} [_] @param {any} [j] @returns {any} */
                                    (ye, Ve) => Ve !== Q
                                  )
                                ), exportButtonStyle: _t, compactInputStyle: Ce, theme: s }, d.id)
                              ),
                              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => bt(
                                  /** @param {any} [d] @returns {any} */
                                  (d) => ({ ...d, orient: d.orient === "v" ? "h" : "v" })
                                ), style: { ..._t, padding: "4px 7px", flexShrink: 0, fontWeight: 800, color: Te.orient === "v" ? s.primary : s.textSec }, children: Te.orient === "v" ? "|" : "—" }),
                                /* @__PURE__ */ e.jsx("input", { type: "number", value: Te.value, onChange: (d) => bt(
                                  /** @param {any} [d] @returns {any} */
                                  (Q) => ({ ...Q, value: d.target.value })
                                ), placeholder: Te.orient === "v" ? "Cat index" : "Value", style: { ...Ce, width: "60px", flexShrink: 0 } }),
                                /* @__PURE__ */ e.jsx("input", { type: "text", value: Te.label, onChange: (d) => bt(
                                  /** @param {any} [d] @returns {any} */
                                  (Q) => ({ ...Q, label: d.target.value })
                                ), placeholder: "Label", style: { ...Ce, flex: 1, minWidth: 0 } }),
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => {
                                  Te.value !== "" && (Xe(
                                    /** @param {any} [prev] @returns {any} */
                                    (d) => [...d, { id: Date.now(), value: Te.value, label: Te.label, orient: Te.orient, color: "", dash: "dashed" }]
                                  ), bt({ value: "", label: "", orient: Te.orient }));
                                }, style: { ..._t, padding: "4px 10px", flexShrink: 0 }, children: "+" })
                              ] }),
                              Cr ? /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: s.textSec }, children: [
                                "Data range: ",
                                Wt(Cr.min, ct),
                                " — ",
                                Wt(Cr.max, ct)
                              ] }) : null
                            ] })
                          ] }) : null
                        ] }),
                        p === "style" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}` }, children: "Colors" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" }, children: [...gn, ...At.length >= 2 ? ["custom"] : []].map((d) => {
                            const Q = me === d, ge = d === "custom" ? At : yn[d];
                            return /* @__PURE__ */ e.jsxs(
                              "button",
                              {
                                type: "button",
                                onClick: () => yt(d),
                                title: d.charAt(0).toUpperCase() + d.slice(1),
                                style: { display: "flex", alignItems: "center", gap: "3px", padding: "3px 6px", border: `1.5px solid ${Q ? s.primary : s.border}`, background: Q ? s.select : s.headerSubtleBg || s.hover, borderRadius: s.radiusSm || "8px", cursor: "pointer" },
                                children: [
                                  (ge || []).slice(0, 4).map(
                                    /** @param {any} [hex] @param {any} [i] @returns {any} */
                                    (ye, Ve) => /* @__PURE__ */ e.jsx("span", { style: { width: "8px", height: "8px", borderRadius: "50%", background: ye, flexShrink: 0 } }, Ve)
                                  ),
                                  /* @__PURE__ */ e.jsx("span", { style: { fontSize: "9px", fontWeight: Q ? 700 : 600, color: Q ? s.primary : s.textSec, marginLeft: "1px", textTransform: "capitalize" }, children: d === "a11y" ? "A11y" : d })
                                ]
                              },
                              d
                            );
                          }) }),
                          Array.isArray(t && t.series) && t.series.length > 0 ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px" }, children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: s.textSec, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "2px" }, children: "Override per series" }),
                            t.series.slice(0, 20).map(
                              /** @param {any} [ser] @param {any} [si] @returns {any} */
                              (d, Q) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "5px" }, children: [
                                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "2px", flexShrink: 0 }, children: I.slice(0, 6).map(
                                  /** @param {any} [hex] @param {any} [pi] @returns {any} */
                                  (ge, ye) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Rt(
                                    /** @param {any} [prev] @returns {any} */
                                    (Ve) => ({ ...Ve, [d.name]: ge })
                                  ), style: { width: "14px", height: "14px", borderRadius: "50%", background: ge, border: `2px solid ${(Fe[d.name] || Ae(Q, s, I)) === ge ? s.text : "transparent"}`, cursor: "pointer", padding: 0, flexShrink: 0 }, title: ge }, ye)
                                ) }),
                                /* @__PURE__ */ e.jsx("input", { type: "color", value: Fe[d.name] || Ae(Q, s, I), onChange: (ge) => Rt(
                                  /** @param {any} [prev] @returns {any} */
                                  (ye) => ({ ...ye, [d.name]: ge.target.value })
                                ), style: { width: "24px", height: "24px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none", flexShrink: 0 }, title: `Custom color for ${d.name}` }),
                                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: d.name || `Series ${Q + 1}` }),
                                Fe[d.name] ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Rt(
                                  /** @param {any} [prev] @returns {any} */
                                  (ge) => {
                                    const ye = { ...ge };
                                    return delete ye[d.name], ye;
                                  }
                                ), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 2px" }, title: "Reset to palette", children: "↺" }) : null
                              ] }, d.name || Q)
                            ),
                            t.series.length > 20 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: s.textSec }, children: `…${t.series.length - 20} more` }) : null
                          ] }) : null,
                          /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${s.border}`, paddingTop: "8px" }, children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: s.textSec, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "6px" }, children: "Custom Palette" }),
                            /* @__PURE__ */ e.jsx(Pn, { customColors: At, onChange: Qt, theme: s, compactInputStyle: Ce }),
                            At.length >= 2 ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => yt("custom"), style: { ..._t, marginTop: "6px", background: me === "custom" ? s.select : void 0, color: me === "custom" ? s.primary : void 0, border: `1px solid ${me === "custom" ? s.primary : s.border}` }, children: me === "custom" ? "Custom active" : "Use custom palette" }) : null
                          ] }),
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Effects" }),
                          /* @__PURE__ */ e.jsx(pe, { label: "Animate", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Mt(
                            /** @param {any} [v] @returns {any} */
                            (d) => !d
                          ), style: { border: `1px solid ${xt ? s.primary : s.border}`, background: xt ? s.select : s.headerSubtleBg || s.hover, color: xt ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: xt ? "On" : "Off" }) }),
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !pt.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(pe, { label: "Gradient", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ut(
                              /** @param {any} [v] @returns {any} */
                              (d) => !d
                            ), style: { border: `1px solid ${Ge ? s.primary : s.border}`, background: Ge ? s.select : s.headerSubtleBg || s.hover, color: Ge ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: Ge ? "On" : "Off" }) }),
                            Ge ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(pe, { label: "Grad Dir", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: Wr.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => vr(d.value), style: { border: `1px solid ${Nt === d.value ? s.primary : s.border}`, background: Nt === d.value ? s.select : s.headerSubtleBg || s.hover, color: Nt === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }),
                              /* @__PURE__ */ e.jsx(pe, { label: "Grad Opacity", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "range", min: "10", max: "100", step: "5", value: Pt, onChange: (d) => mr(Number(d.target.value)), style: { flex: 1 } }),
                                /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "11px", color: s.textSec, minWidth: "30px" }, children: [
                                  Pt,
                                  "%"
                                ] })
                              ] }) })
                            ] }) : null
                          ] }) : null,
                          n === "line" || n === "scatter" || n === "bubble" || n === "area" || n === "radar" || xe ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Markers" }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Shape", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(qe, { size: "sm", "aria-label": "Marker shape", value: Ze, onChange: (d) => vt(d.target.value), style: { ...Ce, width: "100%" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: "none", children: "None (line only)" }),
                              /* @__PURE__ */ e.jsx("option", { value: "circle", children: "Circle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "square", children: "Square" }),
                              /* @__PURE__ */ e.jsx("option", { value: "diamond", children: "Diamond" }),
                              /* @__PURE__ */ e.jsx("option", { value: "triangle", children: "Triangle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "cross", children: "Cross" })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Size", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs(qe, { size: "sm", "aria-label": "Marker size", value: mt, onChange: (d) => st(Number(d.target.value)), style: { ...Ce, width: "92px" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: 3, children: "Small" }),
                              /* @__PURE__ */ e.jsx("option", { value: 5, children: "Medium" }),
                              /* @__PURE__ */ e.jsx("option", { value: 7, children: "Large" }),
                              /* @__PURE__ */ e.jsx("option", { value: 10, children: "XLarge" })
                            ] }) })
                          ] }) : null,
                          (n === "line" || n === "area" || xe) && Array.isArray(t && t.series) && t.series.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Line Style" }),
                            /* @__PURE__ */ e.jsx(Nn, { series: t.series, lineDashStyles: Vt, onChange: kr, seriesColors: Fe, paletteColors: I, getColorForIndex: Ae, theme: s, compactInputStyle: Ce })
                          ] }) : null,
                          n === "bar" || n === "line" || n === "area" || n === "scatter" || xe ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Trendline" }),
                            /* @__PURE__ */ e.jsx(pe, { label: "Show", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Ue(
                              /** @param {any} [v] @returns {any} */
                              (d) => !d
                            ), style: { border: `1px solid ${Le ? s.primary : s.border}`, background: Le ? s.select : s.headerSubtleBg || s.hover, color: Le ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: Le ? "On" : "Off" }) }),
                            Le ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(pe, { label: "Type", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(qe, { size: "sm", "aria-label": "Trendline type", value: we, onChange: (d) => Ye(d.target.value), style: { ...Ce, width: "100%" }, children: [
                                /* @__PURE__ */ e.jsx("option", { value: "linear", children: "Linear Regression" }),
                                /* @__PURE__ */ e.jsx("option", { value: "moving_avg", children: "Moving Average" }),
                                /* @__PURE__ */ e.jsx("option", { value: "exponential", children: "Exponential Smoothing" })
                              ] }) }),
                              we === "moving_avg" ? /* @__PURE__ */ e.jsx(pe, { label: "Period", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "50", value: et, onChange: (d) => ar(Math.max(2, Math.min(50, Number(d.target.value) || 3))), style: { ...Ce, width: "80px" } }) }) : null,
                              /* @__PURE__ */ e.jsxs(pe, { label: "Color", theme: s, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "color", value: Re || "#888888", onChange: (d) => x(d.target.value), style: { width: "32px", height: "28px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }, title: "Trendline color" }),
                                Re ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => x(""), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 4px" }, title: "Reset to auto", children: "↺" }) : null
                              ] }),
                              /* @__PURE__ */ e.jsx(pe, { label: "Width", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "1", max: "8", value: It, onChange: (d) => jr(Math.max(1, Math.min(8, Number(d.target.value) || 2))), style: { ...Ce, width: "80px" } }) })
                            ] }) : null
                          ] }) : null
                        ] }),
                        p === "labels" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(pe, { label: "Data Labels", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Bt(
                            /** @param {any} [v] @returns {any} */
                            (d) => !d
                          ), style: { border: `1px solid ${ve ? s.primary : s.border}`, background: ve ? s.select : s.headerSubtleBg || s.hover, color: ve ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: ve ? "On" : "Off" }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" ? /* @__PURE__ */ e.jsx(pe, { label: "Format", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(qe, { size: "sm", value: ct, onChange: (d) => He(d.target.value), style: { ...Ce, width: "100%" }, title: "Value format for axis labels", children: bn.map((d) => /* @__PURE__ */ e.jsx("option", { value: d.value, children: d.label }, d.value)) }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" && !pt.has(n) ? /* @__PURE__ */ e.jsx(pe, { label: "Decimals", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(qe, { size: "sm", "aria-label": "Decimals", value: Zt, onChange: (d) => qt(Number(d.target.value)), style: { ...Ce, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: -1, children: "Auto" }),
                            /* @__PURE__ */ e.jsx("option", { value: 0, children: "0  (e.g. 1,234)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 1, children: "1  (e.g. 1.2)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 2, children: "2  (e.g. 1.23)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 3, children: "3  (e.g. 1.234)" })
                          ] }) }) : null,
                          ve && n === "bar" ? /* @__PURE__ */ e.jsx(pe, { label: "Position", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(qe, { size: "sm", "aria-label": "Label position", value: Dt, onChange: (d) => Kt(d.target.value), style: { ...Ce, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: "outside", children: "Outside" }),
                            /* @__PURE__ */ e.jsx("option", { value: "inside", children: "Inside top" }),
                            /* @__PURE__ */ e.jsx("option", { value: "center", children: "Center" })
                          ] }) }) : null,
                          n === "pie" || n === "donut" ? /* @__PURE__ */ e.jsx(pe, { label: "Callouts", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => te(
                            /** @param {any} [v] @returns {any} */
                            (d) => !d
                          ), style: { border: `1px solid ${We ? s.primary : s.border}`, background: We ? s.select : s.headerSubtleBg || s.hover, color: We ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: We ? "On" : "Off" }) }) : null,
                          n !== "sparkline" && !pt.has(n) ? /* @__PURE__ */ e.jsx(pe, { label: "Legend", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(Bn, { legendPosition: jt, onChange: cr, theme: s }) }) : null
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
          /* @__PURE__ */ e.jsx(os, { model: t, theme: s }),
          /* @__PURE__ */ e.jsx(Dn, { model: t, maxCategories: Zr, maxSeries: Kr, theme: s })
        ] }) : null
      ]
    }
  );
}, Ms = ({
  onClose: t,
  source: n,
  onSourceChange: i,
  chartType: a,
  onChartTypeChange: r,
  chartLayers: h = [],
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
  model: G,
  theme: V,
  width: Y,
  chartTitle: F,
  onChartTitleChange: U,
  chartHeight: de,
  onChartHeightChange: ne,
  onResizeStart: ae,
  floating: ue = !1,
  onToggleFloating: ot,
  floatingRect: be = null,
  onFloatingDragStart: it,
  onFloatingResizeStart: _e,
  rowLimit: Ot,
  onRowLimitChange: at,
  columnLimit: Qe,
  onColumnLimitChange: Oe,
  sortMode: Me,
  onSortModeChange: Xt,
  interactionMode: or,
  onInteractionModeChange: rt,
  serverScope: Tt,
  onServerScopeChange: Gt,
  chartDefinitions: Je = [],
  activeChartId: ht = null,
  onActiveChartChange: Ut,
  onCreateChart: L,
  onDuplicateChart: zt,
  onDeleteChart: xe,
  onRenameChart: je,
  onCategoryActivate: P,
  allowHierarchyCharts: Ie = !0,
  showServerScope: Lt = !1,
  locked: gt = !1,
  onToggleLock: lt,
  dockPosition: Ne = "right",
  onDockPositionChange: ke,
  immersiveMode: z,
  onImmersiveModeChange: ve,
  onSettingsWidthBudgetChange: Bt,
  standalone: me = !1,
  showResizeHandle: yt = !0,
  title: ct = "Chart Panel",
  showDefinitionManager: He = !0,
  initialSettings: St = {},
  onSettingsChange: ir,
  initialSettingsPaneOpen: Z = !1
}) => {
  vn("PivotChartPanel", ht);
  const [ee, Et] = v.useState(!1), [Ke, $e] = v.useState(!1), [Xe, Te] = v.useState(Z), [bt, dt] = v.useState(348), Pe = v.useRef(null), Be = v.useRef(Xr(
    Y,
    430,
    fr,
    Yr
  )), Yt = v.useRef(Bt), nt = v.useRef(null), Ee = typeof z == "boolean" ? z : Ke, Ze = () => {
    const te = !Ee;
    if (typeof ve == "function") {
      ve(te);
      return;
    }
    $e(te);
  }, vt = ue && gt, mt = (te) => ({
    border: `1px solid ${te === n ? V.primary : V.border}`,
    background: te === n ? V.select : V.headerSubtleBg || V.hover,
    color: te === n ? V.primary : V.text,
    borderRadius: V.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }), st = (te = !1) => ({
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
  }), Ge = (te) => ({
    ...st(Ne === te),
    minWidth: "56px",
    padding: "6px 10px"
  }), ut = !Ee && Xe ? bt + 9 : 0, Fe = Xr(
    Y,
    430,
    fr,
    Yr
  ), Rt = be && Number.isFinite(Number(be.width)) ? Math.max(fr, Math.floor(Number(be.width))) : Fe, xt = be && Number.isFinite(Number(be.height)) ? Math.max(mn, Math.floor(Number(be.height))) : Math.max(jn, (Number(de) || sr) + Ur);
  v.useEffect(() => {
    Yt.current = Bt;
  }, [Bt]);
  const Mt = v.useCallback((te, Le = {}) => {
    const Ue = Number(te), we = Number.isFinite(Ue) ? Math.max(320, Math.ceil(Ue)) : null;
    if (!Le.force && nt.current === we) return;
    nt.current = we;
    const Ye = Yt.current;
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
    width: `${Rt + ut}px`,
    height: `${xt}px`,
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
    width: `${Fe + ut}px`,
    minWidth: `${Fe + ut}px`,
    maxWidth: `${Fe + ut}px`,
    flexShrink: 0
  };
  return v.useEffect(() => {
    if (!me || ue || ee) return;
    const te = Pe.current;
    if (!te) return;
    const Le = () => {
      const we = te.clientWidth;
      we > 0 && !Xe && (Be.current = Math.max(fr, we));
    };
    if (Le(), typeof ResizeObserver > "u") return;
    const Ue = new ResizeObserver(Le);
    return Ue.observe(te), () => Ue.disconnect();
  }, [ue, ee, Xe, me]), v.useEffect(() => {
    if (!me || ue || ee || Ee || !Xe) {
      Mt(null);
      return;
    }
    const te = Be.current || Fe;
    Mt(te + ut);
  }, [
    Fe,
    Mt,
    Ee,
    ue,
    ee,
    Xe,
    me,
    ut
  ]), v.useEffect(() => () => {
    Mt(null, { force: !0 });
  }, [Mt]), /* @__PURE__ */ e.jsxs("div", { style: We, children: [
    !ee && !ue && !me && yt ? /* @__PURE__ */ e.jsx(
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
      width: ee || ue || me ? "100%" : `calc(100% - ${yt ? 8 : 0}px)`,
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
            ue && !ee && !vt && typeof it == "function" && it(te);
          },
          style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px", marginBottom: Ee ? "0" : "12px", cursor: ue && !ee && !vt ? "move" : "default" },
          children: [
            Ee ? /* @__PURE__ */ e.jsx("div", {}) : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "4px", minWidth: 0 }, children: /* @__PURE__ */ e.jsx("div", { style: { fontSize: "15px", fontWeight: 800, color: V.text }, children: ct }) }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, onMouseDown: (te) => te.stopPropagation(), children: [
              typeof ot == "function" ? /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: ot,
                  style: st(ue),
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
                  style: st(Ee),
                  title: Ee ? "Exit immersive mode" : "Enter immersive mode",
                  "aria-label": Ee ? "Exit immersive mode" : "Enter immersive mode",
                  children: "Immersive"
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => Et(
                    /** @param {any} [isActive] @returns {any} */
                    (te) => !te
                  ),
                  style: st(ee),
                  title: ee ? "Exit full screen" : "Open chart full screen",
                  "aria-label": ee ? "Exit full screen" : "Open chart full screen",
                  children: ee ? /* @__PURE__ */ e.jsx(tt.FullscreenExit, {}) : /* @__PURE__ */ e.jsx(tt.Fullscreen, {})
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: t,
                  style: st(!1),
                  title: me ? "Close chart pane" : "Close chart panel",
                  "aria-label": me ? "Close chart pane" : "Close chart panel",
                  children: ee ? /* @__PURE__ */ e.jsx(tt.Close, {}) : "Close"
                }
              )
            ] })
          ]
        }
      ),
      !Ee && He ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", marginBottom: "10px", flexWrap: "wrap", alignItems: "center" }, children: [
        Array.isArray(Je) && Je.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
          /* @__PURE__ */ e.jsx(
            qe,
            {
              size: "sm",
              "aria-label": "Active chart",
              value: ht || "",
              onChange: (te) => {
                typeof Ut == "function" && Ut(te.target.value);
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
              children: Je.map((te) => /* @__PURE__ */ e.jsx("option", { value: te.id, children: te.name || te.id }, te.id))
            }
          ),
          /* @__PURE__ */ e.jsx(
            "input",
            {
              type: "text",
              value: (Je.find((te) => te.id === ht) || {}).name || "",
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
        typeof L == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: st(!1), onClick: L, children: "New" }) : null,
        typeof zt == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: st(!1), onClick: zt, children: "Duplicate" }) : null,
        typeof xe == "function" ? /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            style: st(!1),
            onClick: xe,
            disabled: !Array.isArray(Je) || Je.length <= 1,
            children: "Delete"
          }
        ) : null
      ] }) : null,
      !Ee && me && !ue && typeof ke == "function" ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "10px", flexWrap: "wrap", alignItems: "center" }, children: [
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("left"), style: Ge("left"), title: "Dock chart pane to the left", children: "Left" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("right"), style: Ge("right"), title: "Dock chart pane to the right", children: "Right" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("top"), style: Ge("top"), title: "Dock chart pane to the top", children: "Top" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ke("bottom"), style: Ge("bottom"), title: "Dock chart pane to the bottom", children: "Bottom" })
      ] }) : null,
      Ee ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "12px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ e.jsx("button", { style: mt("pivot"), onClick: () => i("pivot"), children: "Pivot View" }),
        /* @__PURE__ */ e.jsx("button", { style: mt("selection"), onClick: () => i("selection"), children: "Selection" })
      ] }),
      /* @__PURE__ */ e.jsx(
        tn,
        {
          model: G,
          chartType: a,
          chartLayers: h,
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
          rowLimit: Ot,
          onRowLimitChange: at,
          columnLimit: Qe,
          onColumnLimitChange: Oe,
          title: F,
          onTitleChange: U,
          chartHeight: de,
          onChartHeightChange: ne,
          sortMode: Me,
          onSortModeChange: Xt,
          interactionMode: or,
          onInteractionModeChange: rt,
          serverScope: Tt,
          onServerScopeChange: Gt,
          showServerScope: Lt,
          theme: V,
          immersiveMode: Ee,
          onCategoryActivate: P,
          allowHierarchyCharts: Ie,
          floating: ue,
          locked: gt,
          onToggleLock: lt,
          configOpen: Xe,
          onConfigChange: Te,
          settingsPanelWidth: bt,
          onSettingsPanelWidthChange: dt,
          initialSettings: St,
          onSettingsChange: ir
        }
      ),
      ue && !ee && !gt ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
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
}, $s = (t) => t.open ? /* @__PURE__ */ e.jsx(Ms, { ...t }) : null, Ws = ({ chartState: t, onClose: n, theme: i, position: a = "right", onPositionChange: r }) => {
  const [h, m] = v.useState(t && t.chartType ? t.chartType : "bar"), [M, W] = v.useState(t && t.barLayout ? t.barLayout : "grouped"), [R, $] = v.useState(t && t.axisMode ? t.axisMode : "vertical"), [O, J] = v.useState(t && t.defaultOrientation ? t.defaultOrientation : "rows"), [K, s] = v.useState(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), [_, G] = v.useState(t && t.rowLimit ? t.rowLimit : Fr), [V, Y] = v.useState(t && t.columnLimit ? t.columnLimit : Or), [F, U] = v.useState(t && t.sortMode ? t.sortMode : "natural"), [de, ne] = v.useState(t && Array.isArray(t.chartLayers) ? t.chartLayers : []), [ae, ue] = v.useState(null);
  v.useEffect(() => {
    m(t && t.chartType ? t.chartType : "bar"), W(t && t.barLayout ? t.barLayout : "grouped"), $(t && t.axisMode ? t.axisMode : "vertical"), J(t && t.defaultOrientation ? t.defaultOrientation : "rows"), s(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), G(t && t.rowLimit ? t.rowLimit : Fr), Y(t && t.columnLimit ? t.columnLimit : Or), U(t && t.sortMode ? t.sortMode : "natural"), ne(t && Array.isArray(t.chartLayers) ? t.chartLayers : []);
  }, [t]);
  const ot = v.useMemo(() => {
    if (!t || !Array.isArray(t.visibleCols)) return [];
    if (!(t.selectionMap && t.visibleRows))
      return $r(t.visibleCols);
    const Qe = un(t.selectionMap, t.visibleRows, t.visibleCols);
    if (!Qe) return [];
    const Oe = new Set(
      (Qe.colIndexes || []).map(
        /** @param {any} [index] @returns {any} */
        (Me) => t.visibleCols[Me]
      ).filter(
        /** @param {any} [column] @returns {any} */
        (Me) => Me && Me.id && !xn.has(Me.id)
      ).map(
        /** @param {any} [column] @returns {any} */
        (Me) => Me.id
      )
    );
    return $r(t.visibleCols).filter(
      /** @param {any} [column] @returns {any} */
      (Me) => Oe.has(Me.id)
    );
  }, [t]), be = v.useMemo(() => t ? t.selectionMap && t.visibleRows && t.visibleCols ? h === "combo" ? pn(t.selectionMap, t.visibleRows, t.visibleCols, {
    hierarchyLevel: K,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: _,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: F,
    layers: de
  }) : fn(t.selectionMap, t.visibleRows, t.visibleCols, {
    orientation: O,
    hierarchyLevel: K,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: _,
    maxColumns: V,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: F
  }) : t.model : null, [de, t, h, K, O, _, V, F]);
  if (v.useEffect(() => {
    if (M !== "stacked") return;
    if (h === "combo") {
      W("grouped");
      return;
    }
    (h === "area" ? be && Array.isArray(be.series) && be.series.length > 1 : Gr(be)) || W("grouped");
  }, [be, M, h]), !t) return null;
  const it = a === "top" || a === "bottom", _e = Math.max(420, Number.isFinite(Number(ae)) ? Math.floor(Number(ae)) : 420), Ot = Math.max(360, Number(t.chartHeight || sr) + Ur), at = (Qe, Oe) => ({
    border: `1px solid ${Qe === a ? i.primary : i.border}`,
    background: Qe === a ? i.select : i.headerSubtleBg || i.hover,
    color: Qe === a ? i.primary : i.text,
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
        width: it ? "100%" : `${_e}px`,
        minWidth: it ? 0 : `${Math.max(320, _e)}px`,
        maxWidth: it ? "100%" : "70%",
        height: it ? `${Ot}px` : void 0,
        minHeight: it ? "320px" : 0,
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
                  style: at("left"),
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
                  style: at("right"),
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
                  style: at("top"),
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
                  style: at("bottom"),
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
          tn,
          {
            model: be,
            chartType: h,
            chartLayers: de,
            onChartLayersChange: ne,
            availableColumns: ot,
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
            onRowLimitChange: G,
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
  Ws as PivotChartModal,
  $s as PivotChartPanel
};
