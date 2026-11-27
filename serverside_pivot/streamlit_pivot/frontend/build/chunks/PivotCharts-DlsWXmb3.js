import { j as e, I as st, T as tt, C as rr, r as Gr, c as on, a as $r, n as Ht, e as $t, t as pe, l as an, g as Ce, b as ln, d as Pr, f as cn, p as cr, h as Ir, i as Hr, k as fr, m as Fr, o as Wt, M as Or, q as Yr, s as Wr, u as dn, v as un, w as xn, x as pn, y as Ur, z as Kr, A as fn, B as hn, D as gn, V as bn, E as yn, F as hr, G as vn, H as mn, J as jn, K as kn, L as Vr, N as wn, O as Sn, P as Mn, Q as _r, R as Cn, S as An, U as $n } from "./index-BO6HjsL-.js";
import { a as v, R as pt } from "./charts-DthW1Q8W.js";
function Wn(t, n, i) {
  const h = Math.min(n.width, Math.max(0, i.width - 16)), m = Math.max(0, i.height - 8 - t.bottom - 6), C = Math.max(0, t.top - 6 - 8), S = n.height > m && C > m, D = S ? C : m, $ = Math.min(n.height, D);
  return {
    left: Math.max(8, Math.min(t.right - h, i.width - 8 - h)),
    top: S ? t.top - 6 - $ : t.bottom + 6,
    maxWidth: Math.max(0, i.width - 16),
    maxHeight: D
  };
}
function zn(t, n) {
  var S;
  const i = v.useRef(null), a = v.useRef(null), [r, h] = v.useState(null), m = (S = i.current) == null ? void 0 : S.ownerDocument, C = (m == null ? void 0 : m.fullscreenElement) || (m == null ? void 0 : m.body);
  return v.useLayoutEffect(() => {
    if (!t || !i.current || !a.current) return;
    const D = i.current, $ = a.current, Y = D.ownerDocument, J = Y.defaultView, K = () => h(Wn(
      D.getBoundingClientRect(),
      { width: $.offsetWidth, height: $.scrollHeight + 2 },
      { width: Y.documentElement.clientWidth, height: Y.documentElement.clientHeight }
    )), s = () => Array.from($.querySelectorAll('[role="menuitem"]:not(:disabled)')), V = (F = !1) => {
      n(!1), h(null), F && D.focus();
    }, T = (F) => {
      var ce;
      const G = ((ce = F.composedPath) == null ? void 0 : ce.call(F)) || [];
      [D, $].some((ee) => G.includes(ee) || ee.contains(F.target)) || V();
    }, xe = (F) => {
      var G, ce;
      if (F.key === "Escape")
        F.preventDefault(), F.stopPropagation(), V(!0);
      else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(F.key)) {
        if (!$.contains(F.target) && !((G = F.composedPath) != null && G.call(F).includes(D))) return;
        F.preventDefault();
        const ee = s(), ae = ee.indexOf(Y.activeElement), Ve = F.key === "Home" ? 0 : F.key === "End" ? ee.length - 1 : (ae + (F.key === "ArrowUp" ? -1 : 1) + ee.length) % ee.length;
        (ce = ee[Ve]) == null || ce.focus();
      } else F.key === "Tab" && V(!0);
    };
    K(), Y.addEventListener("mousedown", T, !0), Y.addEventListener("keydown", xe, !0), Y.addEventListener("scroll", K, !0), J.addEventListener("resize", K);
    const z = typeof J.ResizeObserver == "function" ? new J.ResizeObserver(K) : null;
    return z == null || z.observe(D), z == null || z.observe($), () => {
      Y.removeEventListener("mousedown", T, !0), Y.removeEventListener("keydown", xe, !0), Y.removeEventListener("scroll", K, !0), J.removeEventListener("resize", K), z == null || z.disconnect();
    };
  }, [t, C, n]), v.useLayoutEffect(() => {
    var D, $;
    t && r && (($ = (D = a.current) == null ? void 0 : D.querySelector('[role="menuitem"]:not(:disabled)')) == null || $.focus());
  }, [t, !!r]), { anchorRef: i, menuRef: a, position: r, portalTarget: C };
}
const Lr = [
  { value: "solid", label: "Solid", dash: "none" },
  { value: "dashed", label: "Dashed", dash: "8 4" },
  { value: "dotted", label: "Dotted", dash: "2 4" },
  { value: "dot-dash", label: "Dot-Dash", dash: "8 4 2 4" }
], gr = (t) => {
  const n = Lr.find((i) => i.value === t);
  return n && n.dash !== "none" ? n.dash : void 0;
}, zr = [
  { value: "vertical", label: "Vertical", x1: "0", y1: "0", x2: "0", y2: "1" },
  { value: "horizontal", label: "Horizontal", x1: "0", y1: "0", x2: "1", y2: "0" },
  { value: "diagonal", label: "Diagonal", x1: "0", y1: "0", x2: "1", y2: "1" }
], Ln = (t, n) => {
  const i = (Number(n) || 70) / 100;
  return [
    { offset: "0%", stopOpacity: String(Math.min(1, i + 0.2)) },
    { offset: "100%", stopOpacity: String(i) }
  ];
}, Bn = ["bottom", "top", "none"], En = ({ legendPosition: t, onChange: n, theme: i }) => {
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
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: Bn.map((r) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => n(r), style: a(r), children: r.charAt(0).toUpperCase() + r.slice(1) }, r)) });
}, Rn = [
  { value: "none", label: "Natural" },
  { value: "value_desc", label: "Val ↓" },
  { value: "value_asc", label: "Val ↑" },
  { value: "alpha_asc", label: "A→Z" },
  { value: "alpha_desc", label: "Z→A" }
], Dn = (t, n) => {
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
}, Nn = ({ series: t, lineDashStyles: n, onChange: i, seriesColors: a, paletteColors: r, getColorForIndex: h, theme: m, compactInputStyle: C }) => !Array.isArray(t) || t.length === 0 ? null : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: t.slice(0, 16).map((S, D) => {
  const $ = a && a[S.name] || h(D, m, r), Y = n && n[S.name] || "solid";
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, children: [
    /* @__PURE__ */ e.jsx("span", { style: { width: "10px", height: "10px", borderRadius: "50%", background: $, flexShrink: 0 } }),
    /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: m.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: S.name || `Series ${D + 1}` }),
    /* @__PURE__ */ e.jsx(
      tt,
      {
        size: "sm",
        "aria-label": `${S.name || "Series"} dash style`,
        value: Y,
        onChange: (J) => i({ ...n, [S.name]: J.target.value }),
        style: { ...C, width: "88px", flexShrink: 0 },
        children: Lr.map((J) => /* @__PURE__ */ e.jsx("option", { value: J.value, children: J.label }, J.value))
      }
    )
  ] }, S.name || D);
}) }), Pn = ({ customColors: t, onChange: n, theme: i, compactInputStyle: a }) => {
  const r = Array.isArray(t) && t.length > 0 ? t : ["#2563EB", "#F97316", "#0F766E", "#7C3AED", "#DC2626", "#0891B2"], h = (S, D) => {
    const $ = [...r];
    $[S] = D, n($);
  }, m = () => n([...r, "#888888"]), C = (S) => n(r.filter(
    /** @param {any} [_] @param {any} [i] @returns {any} */
    (D, $) => $ !== S
  ));
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", alignItems: "center" }, children: [
    r.map(
      /** @param {any} [hex] @param {any} [i] @returns {any} */
      (S, D) => /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", alignItems: "center", gap: "2px" }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "color",
            value: S,
            onChange: ($) => h(D, $.target.value),
            style: { width: "28px", height: "28px", padding: 0, border: `1px solid ${i.border}`, borderRadius: "4px", cursor: "pointer", background: "none" },
            title: `Color ${D + 1}`
          }
        ),
        r.length > 2 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => C(D), title: "Remove color", style: { border: "none", background: "none", color: i.textSec, cursor: "pointer", padding: 0, lineHeight: 1, display: "inline-flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", transform: "scale(0.72)" }, children: /* @__PURE__ */ e.jsx(st.Close, {}) }) })
      ] }, D)
    ),
    r.length < 12 && /* @__PURE__ */ e.jsx("button", { type: "button", onClick: m, style: { ...a, padding: "5px 8px", flexShrink: 0 }, children: "+" })
  ] }) });
}, $e = 16, In = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "7", width: "4", height: "8", fill: t, opacity: "0.85", rx: "0.5" })
] }), Hn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polyline", { points: "1,13 5,7 9,10 15,3", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "1", cy: "13", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "7", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "9", cy: "10", r: "1.4", fill: t }),
  /* @__PURE__ */ e.jsx("circle", { cx: "15", cy: "3", r: "1.4", fill: t })
] }), Fn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L8,1 A7,7 0 0,1 15,8 Z", fill: t, opacity: "0.9" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L15,8 A7,7 0 0,1 6,15 Z", fill: t, opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("path", { d: "M8,8 L6,15 A7,7 1 1,1 8,1 Z", fill: t, opacity: "0.3" })
] }), On = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("circle", { cx: "3", cy: "12", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "7", cy: "5", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "12", cy: "9", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "13", cy: "3", r: "1.8", fill: t, opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("circle", { cx: "5", cy: "11", r: "1.8", fill: t, opacity: "0.85" })
] }), Yn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "4", height: "6", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "5", width: "4", height: "10", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "8", width: "4", height: "7", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("polyline", { points: "1,9 5,4 9,7 15,2", stroke: t, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" })
] }), Vn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "4", width: "3", height: "7", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "5", y: "8", width: "3", height: "3", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "5", width: "3", height: "6", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "13", y: "10", width: "3", height: "1", fill: t, opacity: "0.4", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("line", { x1: "0", y1: "15", x2: "16", y2: "15", stroke: t, strokeWidth: "0.7", opacity: "0.35" })
] }), _n = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "2", y: "6", width: "12", height: "5", fill: "none", stroke: t, strokeWidth: "1.2", rx: "1", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "8", y2: "11", stroke: t, strokeWidth: "1.2", opacity: "0.85" }),
  /* @__PURE__ */ e.jsx("line", { x1: "5", y1: "3", x2: "5", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "11", y1: "3", x2: "11", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "3", x2: "6", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "3", x2: "12", y2: "3", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "4", y1: "13", x2: "6", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" }),
  /* @__PURE__ */ e.jsx("line", { x1: "10", y1: "13", x2: "12", y2: "13", stroke: t, strokeWidth: "1.1", opacity: "0.55" })
] }), Xn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("polygon", { points: "8,2 14,6 12,13 4,13 2,6", fill: "none", stroke: t, strokeWidth: "1", opacity: "0.35" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "8,5 11,7.5 9.5,11 6.5,11 5,7.5", fill: t, opacity: "0.65" })
] }), Tn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "1", width: "4", height: "4", fill: t, opacity: "0.9", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "1", width: "4", height: "4", fill: t, opacity: "0.4", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "1", width: "4", height: "4", fill: t, opacity: "0.7", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "6", width: "4", height: "4", fill: t, opacity: "0.3", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "6", width: "4", height: "4", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "6", width: "4", height: "4", fill: t, opacity: "0.5", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "11", width: "4", height: "4", fill: t, opacity: "0.6", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "6", y: "11", width: "4", height: "4", fill: t, opacity: "0.95", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "11", y: "11", width: "4", height: "4", fill: t, opacity: "0.35", rx: "0.5" })
] }), Gn = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "9", width: "5", height: "6", fill: t, opacity: "0.65", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("rect", { x: "7", y: "4", width: "5", height: "11", fill: t, opacity: "0.85", rx: "0.5" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "1,9 4,5 9,5 7,9", fill: t, opacity: "0.45" }),
  /* @__PURE__ */ e.jsx("polygon", { points: "7,4 12,4 12,9 7,9", fill: t, opacity: "0.55" })
] }), Un = ({ color: t }) => /* @__PURE__ */ e.jsxs("svg", { width: $e, height: $e, viewBox: "0 0 16 16", fill: "none", children: [
  /* @__PURE__ */ e.jsx("rect", { x: "4", y: "1", width: "8", height: "5", fill: t, opacity: "0.85", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "1", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("rect", { x: "9", y: "10", width: "6", height: "5", fill: t, opacity: "0.6", rx: "1" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "4", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" }),
  /* @__PURE__ */ e.jsx("line", { x1: "8", y1: "6", x2: "12", y2: "10", stroke: t, strokeWidth: "1", opacity: "0.45" })
] }), Xr = [
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
  const [r, h] = v.useState(null), m = a ? Xr : Xr.filter((C) => !C.hierarchyOnly);
  return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "3px" }, children: m.map(
    /** @param {any} p0 @returns {any} */
    ({ name: C, Icon: S, types: D }) => {
      const $ = D.some(
        /** @param {any} [t] @returns {any} */
        (V) => V.value === t
      ), Y = D.find(
        /** @param {any} [t] @returns {any} */
        (V) => V.value === t
      ), J = D.length === 1, K = r === C || $, s = $ ? i.primary : i.textSec || i.text;
      return /* @__PURE__ */ e.jsxs("div", { children: [
        /* @__PURE__ */ e.jsxs(
          "button",
          {
            type: "button",
            "aria-expanded": J ? void 0 : K,
            onClick: () => {
              J ? n(D[0].value) : h(K && !$ ? null : C);
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
              /* @__PURE__ */ e.jsx(S, { color: s }),
              /* @__PURE__ */ e.jsx("span", { style: { flex: 1 }, children: Y ? Y.label : C }),
              !J && /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", color: s, transform: K ? "scale(0.8) rotate(0deg)" : "scale(0.8) rotate(-90deg)", transition: "transform 0.15s", flexShrink: 0 }, children: /* @__PURE__ */ e.jsx(st.ChevronDown, {}) })
            ]
          }
        ),
        !J && K && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "3px", padding: "4px 0 2px 24px" }, children: D.map(
          /** @param {any} [t] @returns {any} */
          (V) => /* @__PURE__ */ e.jsx(
            "button",
            {
              type: "button",
              "aria-pressed": t === V.value,
              onClick: () => {
                n(V.value), h(null);
              },
              style: {
                border: `1px solid ${t === V.value ? i.primary : i.border}`,
                background: t === V.value ? i.select : i.headerSubtleBg || i.hover,
                color: t === V.value ? i.primary : i.text,
                borderRadius: i.radiusSm || "8px",
                padding: "4px 9px",
                fontSize: "10px",
                fontWeight: t === V.value ? 700 : 600,
                cursor: "pointer"
              },
              children: V.label
            },
            V.value
          )
        ) })
      ] }, C);
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
    tt,
    {
      size: "sm",
      value: t.dash || "dashed",
      onChange: (m) => n({ ...t, dash: m.target.value }),
      style: { ...r, width: "80px", flexShrink: 0 },
      title: "Line style",
      children: Lr.map((m) => /* @__PURE__ */ e.jsx("option", { value: m.value, children: m.label }, m.value))
    }
  ),
  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: i, title: "Remove reference line", style: { ...a, padding: "4px 7px", color: h.danger || "#ef4444", flexShrink: 0, display: "inline-flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", transform: "scale(0.8)" }, children: /* @__PURE__ */ e.jsx(st.Close, {}) }) })
] }), Tr = (t, n = 430, i, a) => {
  const r = Number(t), h = Number(n), m = Number.isFinite(r) ? r : Number.isFinite(h) ? h : 430;
  return Math.max(i, Math.min(a, Math.floor(m)));
}, qn = (t) => {
  const n = t.map(
    /** @param {any} [value] @param {any} [index] @returns {any} */
    ($, Y) => [Y, Number($)]
  ).filter(
    /** @param {any} p0 @returns {any} */
    ([, $]) => Number.isFinite($)
  );
  if (n.length < 2) return null;
  const i = n.length, a = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [Y]) => $ + Y,
    0
  ), r = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [, Y]) => $ + Y,
    0
  ), h = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [Y, J]) => $ + Y * J,
    0
  ), m = n.reduce(
    /** @param {any} sum @param {any} p1 @returns {any} */
    ($, [Y]) => $ + Y * Y,
    0
  ), C = i * m - a * a;
  if (C === 0) return null;
  const S = (i * h - a * r) / C, D = (r - S * a) / i;
  return t.map(
    /** @param {any} [_] @param {any} [index] @returns {any} */
    ($, Y) => S * Y + D
  );
}, Qn = (t, n) => {
  const i = Math.max(2, Math.floor(Number(n) || 3));
  return t.map(
    /** @param {any} [_] @param {any} [index] @returns {any} */
    (a, r) => {
      const h = t.slice(Math.max(0, r - i + 1), r + 1).map(Number).filter(Number.isFinite);
      return h.length > 0 ? h.reduce(
        /** @param {any} [sum] @param {any} [value] @returns {any} */
        (m, C) => m + C,
        0
      ) / h.length : null;
    }
  );
}, Zr = (t) => Array.isArray(t && t.series) && t.series.length > 1, yr = (t, n) => {
  if (typeof window > "u" || !t) return;
  const i = window.URL.createObjectURL(t), a = document.createElement("a");
  a.href = i, a.download = n, document.body.appendChild(a), a.click(), document.body.removeChild(a), window.URL.revokeObjectURL(i);
}, vr = (t = {}) => ({
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
  return vr({
    title: n,
    legendPosition: i,
    legendItems: r,
    background: a.chartBg || a.surfaceBg || a.background || "#ffffff",
    textColor: a.text || "#111827",
    secondaryTextColor: a.textSec || a.text || "#4b5563"
  });
}, qr = (t, n, i) => {
  const a = [];
  let r = [], h = 0;
  return t.forEach(
    /** @param {any} [item] @returns {any} */
    (m) => {
      const C = Math.max(48, i(m.label) + 30);
      r.length > 0 && h + C > Math.max(80, n - 24) && (a.push(r), r = [], h = 0), r.push({ ...m, width: C }), h += C;
    }
  ), r.length > 0 && a.push(r), a;
}, br = (t) => {
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
  )].map(br).join(","), m = a.map(
    /** @param {any} [category] @param {any} [index] @returns {any} */
    ($, Y) => [$, ...r.map(
      /** @param {any} [seriesItem] @returns {any} */
      (J) => J.values[Y] ?? ""
    )].map(br).join(",")
  ), C = vr(i), S = C.legendItems.length > 0 ? C.legendItems.map(
    /** @param {any} [item] @returns {any} */
    ($) => $.label
  ) : r.map(
    /** @param {any} [item] @returns {any} */
    ($) => $.name
  ).filter(Boolean), D = [];
  C.title && D.push(["Title", C.title].map(br).join(",")), S.length > 0 && D.push(["Legend", ...S].map(br).join(",")), D.length > 0 && D.push(""), yr(new Blob([`\uFEFF${[...D, h, ...m].join(`
`)}`], { type: "text/csv;charset=utf-8" }), `${n}.csv`);
}, ts = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), rs = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d"]), ns = pt.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-2d" */
  "./ECharts2dRenderer-COJoXbIS.js"
)), ss = pt.lazy(() => import(
  /* webpackChunkName: "pivot-echarts-3d" */
  "./ECharts3dRenderer-DVRmvI2n.js"
)), er = ({ chartType: t, echartsRef: n, option: i }) => {
  const a = rs.has(t) ? ss : ns;
  return /* @__PURE__ */ e.jsx(pt.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { "aria-label": "Loading chart", style: { width: "100%", height: "100%" } }), children: /* @__PURE__ */ e.jsx(a, { echartsRef: n, option: i }) });
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
      Fr(i.min)
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: a, children: [
      "Max ",
      Fr(i.max)
    ] })
  ] });
}, jt = ({ items: t, theme: n, hiddenSet: i, onToggle: a, position: r = "bottom", onElementSelect: h }) => {
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
  }, children: t.map((C) => {
    const S = m && i.has(C.label);
    return /* @__PURE__ */ e.jsxs(
      "div",
      {
        "data-chart-export-legend-item": "true",
        "data-chart-export-label": C.label,
        "data-chart-export-color": S ? n.border : C.color,
        "data-chart-export-hidden": S ? "true" : "false",
        onClick: () => {
          m && a(C.label), typeof h == "function" && h("legend", C.label);
        },
        role: "button",
        tabIndex: 0,
        onKeyDown: (D) => {
          (D.key === "Enter" || D.key === " ") && (D.preventDefault(), m && a(C.label), typeof h == "function" && h("legend", C.label));
        },
        "aria-pressed": m ? !S : void 0,
        style: {
          display: "flex",
          alignItems: "center",
          gap: "6px",
          fontSize: "11px",
          color: n.textSec,
          cursor: "pointer",
          opacity: S ? 0.35 : 1,
          textDecoration: S ? "line-through" : "none",
          userSelect: "none"
        },
        children: [
          /* @__PURE__ */ e.jsx("span", { style: { width: "10px", height: "10px", borderRadius: "999px", background: S ? n.border : C.color, display: "inline-block" } }),
          /* @__PURE__ */ e.jsx("span", { children: C.label })
        ]
      },
      C.label
    );
  }) });
}, is = (t, n, i, a = 10) => {
  const h = (Array.isArray(t) ? t.map((T) => Number.isFinite(Number(T)) ? Number(T) : null) : []).map(
    /** @param {any} [value] @param {any} [index] @returns {any} */
    (T, xe) => T === null ? null : { value: T, index: xe }
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
    (T) => T.value
  )), C = Math.max(...h.map(
    /** @param {any} [point] @returns {any} */
    (T) => T.value
  )), S = Math.max(1, C - m), D = h.length > 1 ? (n - a * 2) / (h.length - 1) : 0, $ = h.map(
    /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
    (T, xe) => {
      const z = a + xe * D, F = (T.value - m) / S, G = i - a - F * Math.max(1, i - a * 2);
      return {
        ...T,
        x: z,
        y: G
      };
    }
  ), Y = $.map(
    /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
    (T, xe) => `${xe === 0 ? "M" : "L"} ${T.x.toFixed(2)} ${T.y.toFixed(2)}`
  ).join(" "), J = $.length > 1 ? `${Y} L ${$[$.length - 1].x.toFixed(2)} ${(i - a).toFixed(2)} L ${$[0].x.toFixed(2)} ${(i - a).toFixed(2)} Z` : "", K = $.find(
    /** @param {any} [point] @returns {any} */
    (T) => T.value === m
  ) || $[0], s = $.find(
    /** @param {any} [point] @returns {any} */
    (T) => T.value === C
  ) || $[0], V = $[$.length - 1];
  return {
    linePath: Y,
    areaPath: J,
    currentPoint: V,
    minPoint: K,
    maxPoint: s,
    minValue: m,
    maxValue: C,
    firstValue: $[0] ? $[0].value : null,
    lastValue: V ? V.value : null
  };
}, as = ({
  model: t,
  theme: n,
  chartHeight: i,
  paletteColors: a,
  valueFormat: r,
  referenceLines: h = []
}) => {
  const m = Array.isArray(t && t.series) ? t.series : [], C = Array.isArray(t && t.categories) ? t.categories : [];
  if (m.length === 0)
    return /* @__PURE__ */ e.jsx(we, { message: t && t.emptyMessage ? t.emptyMessage : "No sparkline data available.", theme: n, chartHeight: i });
  const S = 110, D = 240, $ = 64;
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
        (Y, J) => {
          const K = Ce(J, n, a), s = is(Y.values || [], D, $, 10), V = Array.isArray(Y.values) && Y.values.length > 1 ? Number(Y.values[Y.values.length - 2]) : null, T = s.lastValue, xe = Number.isFinite(V) && Number.isFinite(T) ? T - V : null, z = xe === null ? n.textSec : xe >= 0 ? n.isDark ? "#7DD3A7" : "#2E7D5B" : n.isDark ? "#F7A3A3" : "#B45353";
          return /* @__PURE__ */ e.jsxs(
            "div",
            {
              "data-chart-sparkline-card": Y.id || Y.name || `spark-${J}`,
              style: {
                minHeight: `${S}px`,
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
                    /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: n.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: Y.name || `Series ${J + 1}` }),
                    /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: n.textSec }, children: C.length > 0 ? `${C.length} points` : "Trend" })
                  ] }),
                  /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", alignItems: "flex-end", gap: "2px", flexShrink: 0 }, children: [
                    /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: T === null ? "—" : Wt(T, r) }),
                    /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: z }, children: xe === null ? "No delta" : `${xe >= 0 ? "+" : ""}${Wt(xe, r)}` })
                  ] })
                ] }),
                /* @__PURE__ */ e.jsxs(
                  "svg",
                  {
                    viewBox: `0 0 ${D} ${$}`,
                    style: { width: "100%", height: "64px", display: "block" },
                    "aria-label": `${Y.name || `Series ${J + 1}`} sparkline`,
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
                        (F, G) => {
                          const ce = Number(F.value);
                          if (!Number.isFinite(ce) || F.orient === "v" || s.minValue === null || s.maxValue === null) return null;
                          const ee = s.maxValue - s.minValue || 1, ae = $ - 10 - (ce - s.minValue) / ee * ($ - 20);
                          if (ae < 0 || ae > $) return null;
                          const Ve = F.color || n.primary;
                          return /* @__PURE__ */ e.jsxs("g", { children: [
                            /* @__PURE__ */ e.jsx("line", { x1: 10, y1: ae, x2: D - 10, y2: ae, stroke: Ve, strokeWidth: "1", strokeDasharray: "4 2", style: { pointerEvents: "none" } }),
                            F.label ? /* @__PURE__ */ e.jsx("text", { x: D - 11, y: ae - 2, textAnchor: "end", fontSize: "8", fontWeight: "700", fill: Ve, style: { pointerEvents: "none" }, children: F.label }) : null
                          ] }, `spark-rl-${G}`);
                        }
                      )
                    ]
                  }
                ),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", fontSize: "10px", color: n.textSec }, children: [
                  /* @__PURE__ */ e.jsx("span", { children: C[0] ? pe(String(C[0]), 14) : "Start" }),
                  /* @__PURE__ */ e.jsxs("span", { children: [
                    "Min ",
                    s.minValue === null ? "—" : Wt(s.minValue, r),
                    " · ",
                    "Max ",
                    s.maxValue === null ? "—" : Wt(s.maxValue, r)
                  ] }),
                  /* @__PURE__ */ e.jsx("span", { children: C.length > 0 ? pe(String(C[C.length - 1]), 14) : "End" })
                ] })
              ]
            },
            Y.id || Y.name || `spark-${J}`
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
  chartHeight: h = rr,
  showLegend: m = !0,
  showDataLabels: C = !1,
  onCategoryActivate: S = null,
  svgRef: D = null,
  echartsRef: $ = null,
  colorPalette: Y = "default",
  valueFormat: J = "auto",
  yAxisTitle: K = "",
  yAxisMin: s = "",
  yAxisMax: V = "",
  referenceLines: T = [],
  labelAngle: xe = 0,
  chartTitle: z = "",
  onTitleChange: F = null,
  tickCount: G = 5,
  markerShape: ce = "circle",
  markerSize: ee = 4,
  showGradient: ae = !1,
  seriesColors: Ve = {},
  showAnimations: fe = !1,
  showCalloutLabels: We = !1,
  subtitle: Ee = "",
  onReferenceLinesChange: ft = null,
  showTrendline: rt = !1,
  trendlineMode: ht = "linear",
  trendlinePeriod: Ze = 3,
  // new props
  legendPosition: _e = "bottom",
  labelPosition: qe = "outside",
  decimalPlaces: Xe = -1,
  gradientDirection: nr = "vertical",
  gradientOpacity: sr = 70,
  trendlineColor: ot = "",
  trendlineWidth: Xt = 2,
  seriesSort: zt = "none",
  lineDashStyles: Lt = {},
  yAxisMinRight: gt = "",
  yAxisMaxRight: Ft = "",
  customPaletteColors: B = [],
  onElementSelect: ke = null
}) => {
  const le = Y === "custom" && B.length >= 2 ? B : Gr(Y), P = (o) => {
    if (Xe >= 0 && typeof o == "number" && Number.isFinite(o)) {
      const c = Wt(o, J);
      if (!/[KMB%$]/.test(c) || J === "number")
        return o.toFixed(Xe);
    }
    return Wt(o, J);
  };
  v.useMemo(() => !t || !Array.isArray(t.series) || zt === "none" ? t : { ...t, series: Dn(t.series, zt) }, [t, zt]);
  const Ke = n === "bar" && i === "stacked" && Array.isArray(t.stackedGroups) && t.stackedGroups.length > 0, it = n === "bar" && i === "stacked" && !Ke && on(t), Bt = n === "area" && i === "stacked" && Zr(t), dt = n === "bar" && a === "horizontal", ze = i === "stacked100", Qe = Number.isFinite(Number(xe)) ? Number(xe) : 0, L = h, Te = /* @__PURE__ */ new Map(), ye = (o, c) => {
    if (Ve && Ve[o]) return Ve[o];
    const l = String(o);
    return Te.has(l) || Te.set(l, Number.isFinite(c) ? c : Te.size), Ce(
      /** @type {number} */
      Te.get(l),
      r,
      le
    );
  }, Ot = (o, c, l, u, p = {}) => {
    if (ce === "none") return null;
    const g = ee || 4, k = r.surfaceBg || r.background || "#fff", f = { fill: l, stroke: k, strokeWidth: "1.5", opacity: "0.82", ...u, ...p };
    switch (ce) {
      case "square":
        return /* @__PURE__ */ e.jsx("rect", { x: o - g, y: c - g, width: g * 2, height: g * 2, rx: "2", ...f });
      case "diamond":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${c - g} ${o + g},${c} ${o},${c + g} ${o - g},${c}`, ...f });
      case "cross":
        return /* @__PURE__ */ e.jsx("path", { d: `M${o - g},${c} L${o + g},${c} M${o},${c - g} L${o},${c + g}`, fill: "none", stroke: l, strokeWidth: "2.5", strokeLinecap: "round", ...u, ...p });
      case "triangle":
        return /* @__PURE__ */ e.jsx("polygon", { points: `${o},${c - g} ${o + g},${c + g * 0.85} ${o - g},${c + g * 0.85}`, ...f });
      default:
        return /* @__PURE__ */ e.jsx("circle", { cx: o, cy: c, r: g, ...f });
    }
  }, He = v.useMemo(() => !ze || !t || !Array.isArray(t.categories) ? null : t.categories.map(
    /** @param {any} [_] @param {any} [catIndex] @returns {any} */
    (o, c) => {
      let l = 0;
      return (t.series || []).forEach(
        /** @param {any} [s] @returns {any} */
        (u) => {
          const p = u.values && u.values[c];
          typeof p == "number" && Number.isFinite(p) && (l += Math.abs(p));
        }
      ), l || 1;
    }
  ), [ze, t]), Le = Array.isArray(t && t.icicleNodes) ? t.icicleNodes : [], at = Math.max(1, Number(t && t.icicleDepth) || 1), Fe = v.useRef(null), [kt, or] = v.useState($r), Z = Math.max($r, Number.isFinite(kt) ? Math.floor(kt) : $r);
  v.useEffect(() => {
    const o = Fe.current;
    if (!o) return;
    const c = () => {
      const u = o.clientWidth;
      u > 0 && or(u);
    };
    if (c(), typeof ResizeObserver > "u") return;
    const l = new ResizeObserver(c);
    return l.observe(o), () => l.disconnect();
  }, []);
  const [be, ve] = v.useState(/* @__PURE__ */ new Set()), Je = v.useCallback((o) => {
    ve(
      /** @param {any} [prev] @returns {any} */
      (c) => {
        const l = new Set(c);
        return l.has(o) ? l.delete(o) : l.add(o), l;
      }
    );
  }, []), [Ae, bt] = v.useState(null), Re = v.useCallback((o) => {
    const c = o.target, l = c.getAttribute("data-tip-cat");
    if (!l) {
      bt(
        /** @param {any} [prev] @returns {any} */
        (g) => g && null
      );
      return;
    }
    const p = o.currentTarget.getBoundingClientRect();
    bt({
      x: o.clientX - p.left,
      y: o.clientY - p.top,
      category: l,
      series: c.getAttribute("data-tip-ser") || "",
      value: c.getAttribute("data-tip-val") || "",
      color: c.getAttribute("data-tip-color") || ""
    });
  }, []), wt = v.useCallback(() => bt(null), []), lt = v.useCallback((o) => {
    if (!ke) return;
    let c = o.target;
    for (; c && c !== o.currentTarget; ) {
      const l = c.getAttribute && c.getAttribute("data-chart-element-type");
      if (l) {
        ke(l, c.getAttribute("data-chart-element-id") || "");
        return;
      }
      c = c.parentElement;
    }
  }, [ke]), Oe = Ae ? /* @__PURE__ */ e.jsxs("div", { style: {
    position: "absolute",
    left: Ae.x + 14 + 160 > Z ? Math.max(0, Ae.x - 174) : Ae.x + 14,
    top: Ae.y - 10 < 0 ? Ae.y + 14 : Ae.y - 10,
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
    /* @__PURE__ */ e.jsx("div", { style: { fontWeight: 800, marginBottom: Ae.series ? "2px" : 0 }, children: Ae.category }),
    Ae.series ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, children: [
      Ae.color ? /* @__PURE__ */ e.jsx("span", { style: { width: "8px", height: "8px", borderRadius: "50%", background: Ae.color, flexShrink: 0 } }) : null,
      /* @__PURE__ */ e.jsxs("span", { children: [
        Ae.series,
        ": ",
        Ae.value
      ] })
    ] }) : Ae.value ? /* @__PURE__ */ e.jsx("div", { children: Ae.value }) : null
  ] }) : null, [De, Yt] = v.useState(1), [ct, Et] = v.useState({ x: 0, y: 0 }), me = v.useRef(null), [St, yt] = v.useState(null), Tt = v.useCallback((o) => {
    o.preventDefault(), Yt(
      /** @param {any} [prev] @returns {any} */
      (c) => {
        const l = o.deltaY < 0 ? c * 1.15 : c / 1.15, u = Math.min(10, Math.max(1, l));
        return u === 1 && Et({ x: 0, y: 0 }), u;
      }
    );
  }, []), Ie = v.useCallback((o) => {
    De <= 1 || o.button !== 0 || (me.current = { startX: o.clientX, startY: o.clientY, startPan: { ...ct } });
  }, [De, ct]), Mt = v.useCallback((o) => {
    if (!me.current) return;
    const c = o.clientX - me.current.startX, l = o.clientY - me.current.startY;
    Et({ x: me.current.startPan.x + c, y: me.current.startPan.y + l });
  }, []), Ye = v.useCallback(() => {
    me.current = null;
  }, []), nt = v.useCallback(() => {
    Yt(1), Et({ x: 0, y: 0 });
  }, []), ut = v.useCallback((o) => {
    Re(o), Mt(o);
    const c = D && D.current;
    if (c && typeof c.getScreenCTM == "function") {
      const l = c.getScreenCTM();
      if (l) {
        const u = c.createSVGPoint();
        u.x = o.clientX, u.y = o.clientY;
        const p = u.matrixTransform(l.inverse());
        yt({ x: p.x, y: p.y });
      }
    }
  }, [Re, Mt]), ir = v.useCallback(() => {
    wt(), Ye(), yt(null);
  }, [wt, Ye]), je = {
    onMouseMove: ut,
    onMouseLeave: ir,
    onWheel: Tt,
    onMouseDown: Ie,
    onMouseUp: Ye,
    onDoubleClick: nt
  }, ar = De > 1 ? `scale(${De}) translate(${ct.x / De}px, ${ct.y / De}px)` : void 0, U = De > 1 ? { overflow: "hidden", cursor: "grab" } : void 0, et = De > 1 ? { transform: ar, transformOrigin: "center center" } : void 0, Se = St && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsxs("svg", { style: { position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none", zIndex: 5 }, viewBox: `0 0 ${Z} ${L}`, preserveAspectRatio: "xMidYMid meet", children: [
    /* @__PURE__ */ e.jsx("line", { x1: St.x, y1: 0, x2: St.x, y2: L, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" }),
    /* @__PURE__ */ e.jsx("line", { x1: 0, y1: St.y, x2: Z, y2: St.y, stroke: r.textSec, strokeOpacity: "0.3", strokeDasharray: "4 3" })
  ] }) : null, Be = De > 1.05 ? /* @__PURE__ */ e.jsx("div", { style: { position: "absolute", top: 4, right: 4, zIndex: 12, background: r.surfaceBg || "#fff", border: `1px solid ${r.border}`, borderRadius: r.radiusSm || "8px", padding: "2px 7px", fontSize: "10px", fontWeight: 700, color: r.textSec, pointerEvents: "auto", cursor: "pointer" }, onClick: nt, title: "Reset zoom (or double-click chart)", children: /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "4px" }, children: [
    /* @__PURE__ */ e.jsxs("span", { children: [
      Math.round(De * 100),
      "%"
    ] }),
    /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", transform: "scale(0.75)" }, children: /* @__PURE__ */ e.jsx(st.Close, {}) })
  ] }) }) : null, [Ge, lr] = v.useState(!1), [vt, dr] = v.useState(""), Rt = v.useRef(null), [Gt, Ut] = v.useState(null), Kt = v.useRef(null);
  v.useEffect(() => {
    const o = (l) => {
      if (!Kt.current) return;
      const { startX: u, startY: p, startPos: g, containerRect: k } = Kt.current, f = l.clientX - u, M = l.clientY - p, w = Math.max(0, Math.min(k.width - 40, g.x + f)), b = Math.max(0, Math.min(k.height - 20, g.y + M));
      Ut({ x: w, y: b });
    }, c = () => {
      Kt.current = null;
    };
    return window.addEventListener("mousemove", o), window.addEventListener("mouseup", c), () => {
      window.removeEventListener("mousemove", o), window.removeEventListener("mouseup", c);
    };
  }, []);
  const Dt = (o) => {
    if (Ge) return;
    o.preventDefault();
    const c = o.currentTarget.parentElement;
    if (!c) return;
    const l = c.getBoundingClientRect(), u = Gt || {
      x: l.width / 2 - 40,
      y: 6
    };
    Kt.current = {
      startX: o.clientX,
      startY: o.clientY,
      startPos: u,
      containerRect: l
    };
  }, mr = () => {
    typeof F == "function" && (dr(z || ""), lr(!0));
  }, Nt = () => {
    typeof F == "function" && F(vt), lr(!1);
  };
  v.useEffect(() => {
    Ge && Rt.current && Rt.current.focus();
  }, [Ge]);
  const jr = Gt ? { left: `${Gt.x}px`, top: `${Gt.y}px` } : { left: "50%", top: "6px", transform: "translateX(-50%)" }, Ne = z || Ge ? /* @__PURE__ */ e.jsx(
    "div",
    {
      onMouseDown: Dt,
      onDoubleClick: mr,
      onClick: ke && !Ge ? () => ke("title", "") : void 0,
      "data-chart-element-type": "title",
      style: {
        position: "absolute",
        ...jr,
        zIndex: 6,
        cursor: Ge ? "text" : ke ? "pointer" : "grab",
        userSelect: Ge ? "auto" : "none"
      },
      children: Ge ? /* @__PURE__ */ e.jsx(
        "input",
        {
          ref: Rt,
          type: "text",
          value: vt,
          onChange: (o) => dr(o.target.value),
          onBlur: Nt,
          onKeyDown: (o) => {
            o.key === "Enter" && Nt(), o.key === "Escape" && lr(!1);
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
      }, children: z })
    }
  ) : null, x = v.useMemo(() => {
    if (n === "heatmap" || n === "icicle" || n === "sunburst" || n === "sankey" || n === "pie" || n === "donut" || n === "scatter" || n === "waterfall" || n === "combo" || n === "sparkline" || n === "bubble" || n === "radar" || n === "funnel" || n === "histogram" || ts.has(n)) return null;
    const o = Ke ? (t.stackedGroups || []).reduce(
      /** @param {any} [acc] @param {any} [group] @returns {any} */
      (R, ie) => {
        let se = 0, re = 0;
        return (ie.segments || []).forEach(
          /** @param {any} [segment] @returns {any} */
          (I) => {
            const te = I && typeof I.value == "number" && Number.isFinite(I.value) ? I.value : null;
            te !== null && (te >= 0 ? se += te : re += te);
          }
        ), R.push(se, re), R;
      },
      []
    ) : it ? (t.categories || []).reduce(
      /** @param {any} [acc] @param {any} [_] @param {any} [categoryIndex] @returns {any} */
      (R, ie, se) => {
        let re = 0, I = 0;
        return (t.series || []).forEach(
          /** @param {any} [series] @returns {any} */
          (te) => {
            const ue = te && Array.isArray(te.values) ? te.values[se] : null;
            typeof ue != "number" || !Number.isFinite(ue) || (ue >= 0 ? re += ue : I += ue);
          }
        ), R.push(re, I), R;
      },
      []
    ) : Bt ? t.categories.reduce(
      /** @param {any} [acc] @param {any} [_] @param {any} [categoryIndex] @returns {any} */
      (R, ie, se) => {
        let re = 0, I = 0;
        return (t.series || []).forEach(
          /** @param {any} [series] @returns {any} */
          (te) => {
            const ue = te && Array.isArray(te.values) ? te.values[se] : null;
            typeof ue != "number" || !Number.isFinite(ue) || (ue >= 0 ? re += ue : I += ue);
          }
        ), R.push(re, I), R;
      },
      []
    ) : (t.series || []).reduce(
      /** @param {any} [acc] @param {any} [series] @returns {any} */
      (R, ie) => ((ie.values || []).forEach(
        /** @param {any} [value] @returns {any} */
        (se) => {
          typeof se == "number" && Number.isFinite(se) && R.push(se);
        }
      ), R),
      []
    );
    if (o.length === 0) return null;
    const c = !Ke && n === "bar" && Array.isArray(t.categoryBands) && t.categoryBands.length === t.categories.length && t.categoryBands.some(
      /** @param {any} [band] @returns {any} */
      (R) => R && R.outerLabel
    );
    let l = Math.min(...o, 0), u = Math.max(...o, 0);
    if (ze)
      l = 0, u = 100;
    else {
      if (l === u) {
        const R = Math.abs(u || 1) * 0.15 || 1;
        l -= R, u += R;
      }
      Number.isFinite(Number(s)) && s !== "" && (l = Number(s)), Number.isFinite(Number(V)) && V !== "" && (u = Number(V)), l >= u && (u = l + 1);
    }
    const g = Ht(l, u, 5).map(
      /** @param {any} [v] @returns {any} */
      (R) => ze ? `${Math.round(R)}%` : P(R)
    ).reduce(
      /** @param {any} [maxWidth] @param {any} [label] @returns {any} */
      (R, ie) => Math.max(R, $t(ie)),
      0
    ), k = Ke ? (t.stackedGroups || []).map(
      /** @param {any} [group] @returns {any} */
      (R) => pe(R && R.label, 22)
    ) : c ? (t.categoryBands || []).map(
      /** @param {any} [band] @returns {any} */
      (R) => pe(R && R.innerLabel, 18)
    ) : (t.categories || []).map(
      /** @param {any} [category] @returns {any} */
      (R) => pe(R, 22)
    ), f = c ? (t.categoryBands || []).map(
      /** @param {any} [band] @returns {any} */
      (R) => pe(R && R.outerLabel, 18)
    ) : [], M = k.reduce(
      /** @param {any} [maxWidth] @param {any} [label] @returns {any} */
      (R, ie) => Math.max(R, $t(ie)),
      0
    ), w = f.reduce(
      /** @param {any} [maxWidth] @param {any} [label] @returns {any} */
      (R, ie) => Math.max(R, $t(ie, 11)),
      0
    ), b = dt ? Math.max(
      c ? 176 : 132,
      c ? w + M + 38 : M + 22
    ) : Math.max(66, g + 18), H = Qe === 0 ? 40 : Qe <= 30 ? 62 : Qe <= 45 ? 80 : Qe <= 60 ? 98 : 118, _ = dt ? { top: 24, right: 18, bottom: 42, left: b } : { top: 24, right: 18, bottom: c ? 118 : H, left: b }, A = Z - _.left - _.right, j = L - _.top - _.bottom, E = n === "bar" ? 0 : Math.min(Math.max(0, l), u), N = u - l || 1, O = dt ? null : (
      /** @param {any} [value] @returns {any} */
      ((R) => _.top + (u - R) / N * j)
    ), X = dt ? (
      /** @param {any} [value] @returns {any} */
      ((R) => _.left + (R - l) / N * A)
    ) : null, oe = O ? O(E) : null, y = X ? X(E) : null, q = Math.max(t.categories.length, 1), ne = (dt ? j : A) / q, W = Math.max(10, ne * 0.72);
    return {
      margin: _,
      plotWidth: A,
      plotHeight: j,
      baselineY: oe,
      baselineX: y,
      scaleY: O,
      scaleX: X,
      step: ne,
      groupWidth: W,
      minValue: l,
      maxValue: u,
      hasHierarchicalBands: c,
      horizontalBarMode: dt
    };
  }, [a, t, n, Bt, Ke, it, dt, L, Z, ze, s, V, Qe, P]), Pt = v.useMemo(() => {
    if (!x || x.horizontalBarMode || x.hasHierarchicalBands)
      return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const o = Array.isArray(t && t.categories) ? t.categories : [];
    if (o.length <= 1) return { visibleIndexes: null, maxChars: 20, stride: 1 };
    const c = x.step < 42 ? 12 : x.step < 70 ? 16 : 20, l = o.reduce(
      /** @param {any} [width] @param {any} [category] @returns {any} */
      (w, b) => Math.max(w, $t(pe(b, c), 11)),
      0
    ), u = Math.abs(Qe) * (Math.PI / 180), p = Qe === 0 ? l : Math.abs(Math.cos(u)) * l + Math.abs(Math.sin(u)) * 12, g = Math.max(1, Math.ceil((p + 12) / Math.max(x.step, 1)));
    if (g === 1) return { visibleIndexes: null, maxChars: c, stride: g };
    const k = /* @__PURE__ */ new Set();
    for (let w = 0; w < o.length; w += g) k.add(w);
    const f = o.length - 1, M = Math.floor(f / g) * g;
    return f - M >= Math.ceil(g * 0.6) && k.add(f), { visibleIndexes: k, maxChars: c, stride: g };
  }, [Qe, x, t]);
  if (n === "sparkline")
    return /* @__PURE__ */ e.jsxs(
      "div",
      {
        ref: Fe,
        "data-chart-sparkline-surface": "true",
        style: {
          position: "relative",
          minHeight: `${L}px`,
          height: `${L}px`,
          minWidth: 0
        },
        children: [
          /* @__PURE__ */ e.jsx(
            as,
            {
              model: t,
              theme: r,
              chartHeight: L,
              paletteColors: le,
              valueFormat: J,
              referenceLines: T
            }
          ),
          Ne
        ]
      }
    );
  if (n === "icicle") {
    if (Le.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Icicle charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: L });
    const o = L / at, c = an(Le, 0, Z, 0, o, []);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative" }, ...je, children: [
      /* @__PURE__ */ e.jsx(
        "svg",
        {
          ref: D,
          viewBox: `0 0 ${Z} ${L}`,
          preserveAspectRatio: "xMidYMid meet",
          style: {
            width: "100%",
            height: "auto",
            display: "block",
            maxWidth: "100%"
          },
          role: "img",
          "aria-label": t.title || "Icicle chart",
          children: c.map(
            /** @param {any} [node] @param {any} [index] @returns {any} */
            (l, u) => {
              const p = Ce(Math.max(0, l.depth - 1), r, le), g = l.width >= 54;
              return /* @__PURE__ */ e.jsxs(
                "g",
                {
                  onClick: () => {
                    typeof S == "function" && S(l);
                  },
                  style: typeof S == "function" ? { cursor: "pointer" } : void 0,
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
                    g ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: l.x + 8,
                        y: l.y + Math.min(l.height - 10, 22),
                        textAnchor: "start",
                        fontSize: "11",
                        fontWeight: "700",
                        fill: "#fff",
                        style: { pointerEvents: "none" },
                        children: pe(l.label, Math.max(6, Math.floor(l.width / 9)))
                      }
                    ) : null
                  ]
                },
                `icicle-node-${l.rowPath || u}`
              );
            }
          )
        }
      ),
      Ne,
      Se,
      Oe,
      Be
    ] }) });
  }
  if (n === "sunburst") {
    if (Le.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Sunburst charts need hierarchical rows and a numeric measure.", theme: r, chartHeight: L });
    const o = Math.min(Z, L) / 2 - 18, c = Z / 2, l = L / 2, u = o / Math.max(at, 1), p = ln(Le, -Math.PI / 2, Math.PI * 3 / 2, 1, u, []);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative" }, ...je, children: [
      /* @__PURE__ */ e.jsx(
        "svg",
        {
          ref: D,
          viewBox: `0 0 ${Z} ${L}`,
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
              const f = Ce(Math.max(0, g.depth - 1), r, le), M = Pr(c, l, g.innerRadius, g.outerRadius, g.startAngle, g.endAngle), w = g.startAngle + (g.endAngle - g.startAngle) / 2, b = g.innerRadius + (g.outerRadius - g.innerRadius) / 2, H = cr(c, l, b, w), _ = g.endAngle - g.startAngle, A = _ > 0.2 && g.outerRadius - g.innerRadius >= 18;
              return /* @__PURE__ */ e.jsxs(
                "g",
                {
                  onClick: () => {
                    typeof S == "function" && S(g);
                  },
                  style: typeof S == "function" ? { cursor: "pointer" } : void 0,
                  children: [
                    /* @__PURE__ */ e.jsx(
                      "path",
                      {
                        d: M,
                        fill: f,
                        opacity: "0.88",
                        stroke: r.surfaceBg || r.background || "#fff",
                        strokeWidth: "1",
                        "data-tip-cat": g.label || "",
                        "data-tip-val": typeof g.value == "number" ? P(g.value) : "",
                        "data-tip-color": f
                      }
                    ),
                    A ? /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: H.x,
                        y: H.y,
                        textAnchor: "middle",
                        fontSize: "10",
                        fontWeight: "700",
                        fill: "#fff",
                        style: { pointerEvents: "none" },
                        children: pe(g.label, Math.max(5, Math.floor(_ * 24)))
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
      Ne,
      Se,
      Oe,
      Be
    ] }) });
  }
  if (n === "sankey") {
    const o = Array.isArray(t && t.sankeyNodes) ? t.sankeyNodes : [], c = Array.isArray(t && t.sankeyLinks) ? t.sankeyLinks : [];
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Sankey charts need expanded hierarchical rows with child flows.", theme: r, chartHeight: L });
    const l = cn(o, c, Z, L), u = l.nodes.length > 0 ? Math.max(...l.nodes.map(
      /** @param {any} [n] @returns {any} */
      (p) => p.x
    )) : 0;
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative" }, ...je, children: [
      /* @__PURE__ */ e.jsxs(
        "svg",
        {
          ref: D,
          viewBox: `0 0 ${Z} ${L}`,
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
            l.links.map(
              /** @param {any} [link] @param {any} [index] @returns {any} */
              (p, g) => {
                const k = p.sourceNode.x + p.sourceNode.width, f = p.targetNode.x, M = k + (f - k) / 2, w = [
                  `M ${k} ${p.sourceY}`,
                  `C ${M} ${p.sourceY}, ${M} ${p.targetY}, ${f} ${p.targetY}`
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
            l.nodes.map(
              /** @param {any} [node] @param {any} [index] @returns {any} */
              (p, g) => {
                const k = Ce(Math.max(0, p.depth - 1), r, le);
                return /* @__PURE__ */ e.jsxs(
                  "g",
                  {
                    onClick: () => {
                      typeof S == "function" && S(p);
                    },
                    style: typeof S == "function" ? { cursor: "pointer" } : void 0,
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
                          children: pe(p.label, 24)
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
      Ne,
      Se,
      Oe,
      Be
    ] }) });
  }
  if (n === "pie" || n === "donut") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, c = Array.isArray(t && t.categories) ? t.categories : [];
    if (!o || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Pie charts need categories and at least one numeric measure.", theme: r, chartHeight: L });
    const l = c.map(
      /** @param {any} [category] @param {any} [index] @returns {any} */
      (A, j) => {
        const E = o.values[j], N = typeof E == "number" && Number.isFinite(E) ? E : 0;
        return { category: A, value: Math.abs(N), originalValue: N, index: j };
      }
    ).filter(
      /** @param {any} [slice] @returns {any} */
      (A) => A.value > 0 && !be.has(A.category)
    ), u = l.reduce(
      /** @param {any} [sum] @param {any} [slice] @returns {any} */
      (A, j) => A + j.value,
      0
    );
    if (u === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "All values are zero — nothing to display.", theme: r, chartHeight: L });
    const p = n === "donut", g = Math.min(Z, L) / 2 - 24, k = p ? g * 0.52 : 0, f = Z / 2, M = L / 2, w = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    let b = -Math.PI / 2;
    const H = l.map(
      /** @param {any} [slice] @returns {any} */
      (A) => {
        const j = A.value / u, E = j * Math.PI * 2, N = b, O = b + E;
        return b = O, { ...A, startAngle: N, endAngle: O, fraction: j };
      }
    ), _ = H.map(
      /** @param {any} [slice] @returns {any} */
      (A) => ({
        label: `${A.category} (${(A.fraction * 100).toFixed(1)}%)`,
        color: Ce(A.index, r, le)
      })
    );
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative" }, ...je, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: D,
            viewBox: `0 0 ${Z} ${L}`,
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
              H.map(
                /** @param {any} [slice] @returns {any} */
                (A) => {
                  const j = Ce(A.index, r, le), E = Pr(f, M, k, g, A.startAngle, A.endAngle), N = A.startAngle + (A.endAngle - A.startAngle) / 2, O = p ? k + (g - k) / 2 : g * 0.62, X = cr(f, M, O, N), oe = A.endAngle - A.startAngle > 0.3, y = cr(f, M, g + (We ? 26 : 14), N), q = A.endAngle - A.startAngle > 0.15, ne = w[A.index] || null;
                  return /* @__PURE__ */ e.jsxs(
                    "g",
                    {
                      onClick: () => {
                        typeof S == "function" && ne && S(ne);
                      },
                      style: typeof S == "function" && ne ? { cursor: "pointer" } : void 0,
                      children: [
                        /* @__PURE__ */ e.jsx(
                          "path",
                          {
                            d: E,
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
                        oe ? /* @__PURE__ */ e.jsxs(
                          "text",
                          {
                            x: X.x,
                            y: X.y,
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
                        (C || We) && q ? /* @__PURE__ */ e.jsxs("g", { children: [
                          We ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(
                              "line",
                              {
                                x1: cr(f, M, g * 0.97, N).x,
                                y1: cr(f, M, g * 0.97, N).y,
                                x2: y.x,
                                y2: y.y,
                                stroke: j,
                                strokeWidth: "1",
                                style: { pointerEvents: "none" }
                              }
                            ),
                            /* @__PURE__ */ e.jsx(
                              "text",
                              {
                                x: N > Math.PI / 2 && N < 3 * Math.PI / 2 ? y.x - 4 : y.x + 4,
                                y: y.y + 4,
                                textAnchor: N > Math.PI / 2 && N < 3 * Math.PI / 2 ? "end" : "start",
                                fontSize: "9",
                                fontWeight: "700",
                                fill: r.textSec,
                                style: { pointerEvents: "none" },
                                children: pe(A.category, 14)
                              }
                            )
                          ] }) : null,
                          C ? /* @__PURE__ */ e.jsx(
                            "text",
                            {
                              x: y.x,
                              y: We ? y.y + 14 : y.y,
                              textAnchor: N > Math.PI / 2 && N < 3 * Math.PI / 2 ? "end" : "start",
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
                }
              ),
              p ? /* @__PURE__ */ e.jsx(
                "text",
                {
                  x: f,
                  y: M,
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
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: _, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (n === "bubble") {
    const o = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (I) => !be.has(I.name)
    ) : [], c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "Bubble charts need at least two measures (X and Y). A third measure sets bubble size.", theme: r, chartHeight: L });
    const u = o[0], p = o[1], g = o[2] || null, k = (u.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (I) => typeof I == "number" && Number.isFinite(I)
    ), f = (p.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (I) => typeof I == "number" && Number.isFinite(I)
    );
    if (k.length === 0 || f.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Not enough numeric data for a bubble chart.", theme: r, chartHeight: L });
    let M = Math.min(...k), w = Math.max(...k), b = Math.min(...f), H = Math.max(...f);
    if (M === w) {
      const I = Math.abs(w || 1) * 0.15 || 1;
      M -= I, w += I;
    }
    if (b === H) {
      const I = Math.abs(H || 1) * 0.15 || 1;
      b -= I, H += I;
    }
    const _ = (w - M) * 0.08, A = (H - b) * 0.08;
    M -= _, w += _, b -= A, H += A;
    const j = g ? (g.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (I) => typeof I == "number" && Number.isFinite(I)
    ) : [], E = j.length ? Math.min(...j) : 0, O = (j.length ? Math.max(...j) : 1) - E || 1, X = (I) => {
      if (!g) return 7;
      const te = g.values && g.values[I];
      return typeof te != "number" || !Number.isFinite(te) ? 7 : 4 + (te - E) / O * 20;
    }, y = { top: 24, right: 20, bottom: 64, left: Math.max(52, $t(P(H), 11) + 16) }, q = Z - y.left - y.right, ne = L - y.top - y.bottom, W = (I) => y.left + (I - M) / (w - M) * q, R = (I) => y.top + (H - I) / (H - b) * ne, ie = Ht(M, w, 5), se = Ht(b, H, 5), re = ye(p.name, 0);
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative", ...U }, ...je, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: D, viewBox: `0 0 ${Z} ${L}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", overflow: "visible", ...et }, children: [
          se.map(
            /** @param {any} [tick] @param {any} [i] @returns {any} */
            (I, te) => /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx("line", { x1: y.left, y1: R(I), x2: Z - y.right, y2: R(I), stroke: r.border, strokeDasharray: "3 4" }),
              /* @__PURE__ */ e.jsx("text", { x: y.left - 8, y: R(I) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: P(I) })
            ] }, `y-${te}`)
          ),
          ie.map(
            /** @param {any} [tick] @param {any} [i] @returns {any} */
            (I, te) => /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx("line", { x1: W(I), y1: y.top, x2: W(I), y2: L - y.bottom, stroke: r.border, strokeDasharray: "3 4" }),
              /* @__PURE__ */ e.jsx("text", { x: W(I), y: L - y.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, children: P(I) })
            ] }, `x-${te}`)
          ),
          /* @__PURE__ */ e.jsx("line", { x1: y.left, y1: L - y.bottom, x2: Z - y.right, y2: L - y.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("line", { x1: y.left, y1: y.top, x2: y.left, y2: L - y.bottom, stroke: r.border }),
          /* @__PURE__ */ e.jsx("text", { x: Z / 2, y: L - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, children: u.name }),
          K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (y.top + L - y.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(y.top + L - y.bottom) / 2})`, children: K }) : null,
          (u.values || []).map(
            /** @param {any} [xVal] @param {any} [i] @returns {any} */
            (I, te) => {
              const ue = p.values[te];
              if (typeof I != "number" || !Number.isFinite(I) || typeof ue != "number" || !Number.isFinite(ue)) return null;
              const Pe = X(te), qt = c[te] || `Point ${te + 1}`, mt = g ? ` | ${g.name}: ${P(g.values[te])}` : "", Qt = { "data-tip-cat": qt, "data-tip-ser": `${p.name} (X:${P(I)})${mt}`, "data-tip-val": P(ue), "data-tip-color": re }, It = l[te];
              return /* @__PURE__ */ e.jsx(
                "circle",
                {
                  cx: W(I),
                  cy: R(ue),
                  r: Pe,
                  fill: re,
                  opacity: "0.72",
                  stroke: r.surfaceBg || r.background || "#fff",
                  strokeWidth: "1.5",
                  style: typeof S == "function" && It ? { cursor: "pointer" } : void 0,
                  onClick: typeof S == "function" && It ? () => S(It) : void 0,
                  ...Qt
                },
                `bubble-${te}`
              );
            }
          ),
          C && (u.values || []).map(
            /** @param {any} [xVal] @param {any} [i] @returns {any} */
            (I, te) => {
              const ue = p.values[te];
              if (typeof I != "number" || !Number.isFinite(I) || typeof ue != "number" || !Number.isFinite(ue)) return null;
              const Pe = X(te);
              return /* @__PURE__ */ e.jsx("text", { x: W(I), y: R(ue) - Pe - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: c[te] || P(ue) }, `bubble-lbl-${te}`);
            }
          )
        ] }),
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: [{ label: `${u.name} vs ${p.name}${g ? ` (size: ${g.name})` : ""}`, color: re }], theme: r, hiddenSet: be, onToggle: Je, position: _e }) : null
    ] });
  }
  if (n === "radar") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (A) => !be.has(A.name)
    ) : [];
    if (o.length < 3)
      return /* @__PURE__ */ e.jsx(we, { message: "Radar charts need at least 3 categories (spokes).", theme: r, chartHeight: L });
    if (c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Radar charts need at least one measure series.", theme: r, chartHeight: L });
    const l = c.flatMap(
      /** @param {any} [s] @returns {any} */
      (A) => (A.values || []).filter(
        /** @param {any} [v] @returns {any} */
        (j) => typeof j == "number" && Number.isFinite(j)
      )
    ), u = l.length ? Math.max(...l, 0) : 1, p = 60, g = Z / 2, k = L / 2, f = Math.min(g, k) - p, M = o.length, w = 2 * Math.PI / M, b = (A, j) => {
      const E = A * w - Math.PI / 2, N = f * (j / (u || 1));
      return { x: g + N * Math.cos(E), y: k + N * Math.sin(E) };
    }, H = (A) => {
      const j = A * w - Math.PI / 2, E = f + 18;
      return { x: g + E * Math.cos(j), y: k + E * Math.sin(j) };
    }, _ = c.map(
      /** @param {any} [s] @param {any} [i] @returns {any} */
      (A, j) => ({ label: A.name, color: ye(A.name, j) })
    );
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative", ...U }, ...je, children: [
        /* @__PURE__ */ e.jsxs("svg", { ref: D, viewBox: `0 0 ${Z} ${L}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...et }, children: [
          [0.25, 0.5, 0.75, 1].map((A) => {
            const j = o.map(
              /** @param {any} [_] @param {any} [i] @returns {any} */
              (E, N) => b(N, u * A)
            );
            return /* @__PURE__ */ e.jsx("polygon", { points: j.map(
              /** @param {any} [p] @returns {any} */
              (E) => `${E.x},${E.y}`
            ).join(" "), fill: "none", stroke: r.border, strokeWidth: "1" }, `grid-${A}`);
          }),
          o.map(
            /** @param {any} [_] @param {any} [i] @returns {any} */
            (A, j) => {
              const E = b(j, u);
              return /* @__PURE__ */ e.jsx("line", { x1: g, y1: k, x2: E.x, y2: E.y, stroke: r.border, strokeWidth: "1" }, `spoke-${j}`);
            }
          ),
          o.map(
            /** @param {any} [cat] @param {any} [i] @returns {any} */
            (A, j) => {
              const E = H(j), N = Math.abs(E.x - g) < 10 ? "middle" : E.x < g ? "end" : "start";
              return /* @__PURE__ */ e.jsx("text", { x: E.x, y: E.y + 4, textAnchor: N, fontSize: "10", fill: r.textSec, children: pe(A, 14) }, `spoke-lbl-${j}`);
            }
          ),
          c.map(
            /** @param {any} [ser] @param {any} [si] @returns {any} */
            (A, j) => {
              const E = ye(A.name, j), N = o.map(
                /** @param {any} [_] @param {any} [i] @returns {any} */
                (X, oe) => {
                  const y = A.values && A.values[oe];
                  return b(oe, typeof y == "number" && Number.isFinite(y) ? y : 0);
                }
              ), O = N.map(
                /** @param {any} [p] @returns {any} */
                (X) => `${X.x},${X.y}`
              ).join(" ");
              return /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("polygon", { points: O, fill: E, fillOpacity: "0.18", stroke: E, strokeWidth: "2", style: { pointerEvents: "none" } }),
                N.map(
                  /** @param {any} [p] @param {any} [i] @returns {any} */
                  (X, oe) => {
                    const y = A.values && A.values[oe], q = { "data-tip-cat": o[oe], "data-tip-ser": A.name, "data-tip-val": P(y), "data-tip-color": E };
                    return /* @__PURE__ */ e.jsx("circle", { cx: X.x, cy: X.y, r: "4", fill: E, stroke: r.surfaceBg || "#fff", strokeWidth: "1.5", ...q }, `radar-dot-${j}-${oe}`);
                  }
                )
              ] }, `radar-${j}`);
            }
          ),
          [0.5, 1].map((A) => /* @__PURE__ */ e.jsx("text", { x: g + 4, y: k - f * A + 4, fontSize: "9", fill: r.textSec, children: P(u * A) }, `scale-${A}`))
        ] }),
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: _, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (n === "funnel") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!c || o.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Funnel charts need categories and at least one measure.", theme: r, chartHeight: L });
    const l = o.map(
      /** @param {any} [_] @param {any} [i] @returns {any} */
      (w, b) => {
        const H = c.values && c.values[b];
        return typeof H == "number" && Number.isFinite(H) ? Math.abs(H) : 0;
      }
    ), u = Math.max(...l, 1), p = { top: 24, right: 20, bottom: 24, left: 20 }, g = Z - p.left - p.right, f = (L - p.top - p.bottom) / o.length, M = o.map(
      /** @param {any} [cat] @param {any} [i] @returns {any} */
      (w, b) => ({ label: w, color: ye(w, b) })
    );
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative", ...U }, ...je, children: [
        /* @__PURE__ */ e.jsx("svg", { ref: D, viewBox: `0 0 ${Z} ${L}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...et }, children: o.map(
          /** @param {any} [cat] @param {any} [i] @returns {any} */
          (w, b) => {
            const H = l[b] / u, _ = b + 1 < o.length ? l[b + 1] / u : l[b] / u * 0.5, A = g * H, j = g * _, E = Z / 2, N = p.top + b * f, O = ye(w, b), X = [
              `${E - A / 2},${N}`,
              `${E + A / 2},${N}`,
              `${E + j / 2},${N + f - 1}`,
              `${E - j / 2},${N + f - 1}`
            ].join(" ");
            return /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx(
                "polygon",
                {
                  points: X,
                  fill: O,
                  opacity: "0.88",
                  stroke: r.surfaceBg || "#fff",
                  strokeWidth: "1.5",
                  "data-tip-cat": w,
                  "data-tip-ser": c.name,
                  "data-tip-val": P(l[b]),
                  "data-tip-color": O
                }
              ),
              /* @__PURE__ */ e.jsxs("text", { x: E, y: N + f / 2 + 4, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: "#fff", style: { pointerEvents: "none" }, children: [
                pe(w, Math.max(4, Math.floor(A / 7))),
                " ",
                C ? `— ${P(l[b])}` : ""
              ] })
            ] }, `funnel-${b}`);
          }
        ) }),
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: M, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (n === "histogram") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null;
    if (!o)
      return /* @__PURE__ */ e.jsx(we, { message: "Histogram needs at least one numeric measure.", theme: r, chartHeight: L });
    const c = (o.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (N) => typeof N == "number" && Number.isFinite(N)
    );
    if (c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "No numeric values for histogram.", theme: r, chartHeight: L });
    const l = Math.max(4, Math.min(20, Math.ceil(1 + Math.log2(c.length)))), u = Math.min(...c), g = (Math.max(...c) - u) / l || 1, k = Array.from({ length: l }, (N, O) => ({
      low: u + O * g,
      high: u + (O + 1) * g,
      count: 0
    }));
    c.forEach(
      /** @param {any} [v] @returns {any} */
      (N) => {
        const O = Math.min(Math.floor((N - u) / g), l - 1);
        k[O].count++;
      }
    );
    const f = Math.max(...k.map(
      /** @param {any} [b] @returns {any} */
      (N) => N.count
    ), 1), w = { top: 24, right: 16, bottom: 44, left: Math.max(46, $t(String(f), 11) + 16) }, b = Z - w.left - w.right, H = L - w.top - w.bottom, _ = b / l, A = (N) => w.top + (f - N) / f * H, j = ye(o.name, 0), E = Ht(0, f, G || 5);
    return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative", ...U }, ...je, children: [
      /* @__PURE__ */ e.jsxs("svg", { ref: D, viewBox: `0 0 ${Z} ${L}`, width: "100%", preserveAspectRatio: "xMidYMid meet", style: { display: "block", ...et }, children: [
        E.map(
          /** @param {any} [tick] @param {any} [i] @returns {any} */
          (N, O) => /* @__PURE__ */ e.jsxs("g", { children: [
            /* @__PURE__ */ e.jsx("line", { x1: w.left, y1: A(N), x2: Z - w.right, y2: A(N), stroke: r.border, strokeDasharray: "3 4" }),
            /* @__PURE__ */ e.jsx("text", { x: w.left - 8, y: A(N) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, children: N })
          ] }, `htick-${O}`)
        ),
        /* @__PURE__ */ e.jsx("line", { x1: w.left, y1: w.top + H, x2: Z - w.right, y2: w.top + H, stroke: r.textSec, strokeWidth: "1" }),
        k.map(
          /** @param {any} [bin] @param {any} [i] @returns {any} */
          (N, O) => {
            const X = w.left + O * _, oe = A(N.count), y = Math.max(1, w.top + H - oe);
            return /* @__PURE__ */ e.jsxs("g", { children: [
              /* @__PURE__ */ e.jsx(
                "rect",
                {
                  x: X,
                  y: oe,
                  width: Math.max(1, _ - 1),
                  height: y,
                  fill: j,
                  opacity: "0.88",
                  "data-tip-cat": `${P(N.low)}–${P(N.high)}`,
                  "data-tip-ser": "Count",
                  "data-tip-val": String(N.count),
                  "data-tip-color": j
                }
              ),
              C && N.count > 0 ? /* @__PURE__ */ e.jsx("text", { x: X + _ / 2, y: oe - 4, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: N.count }) : null
            ] }, `hbar-${O}`);
          }
        ),
        k.filter(
          /** @param {any} [_] @param {any} [i] @returns {any} */
          (N, O) => O % Math.max(1, Math.floor(l / 6)) === 0 || O === l - 1
        ).map(
          /** @param {any} [bin] @param {any} [i] @returns {any} */
          (N, O) => /* @__PURE__ */ e.jsx("text", { x: w.left + k.indexOf(N) * _ + _ / 2, y: w.top + H + 18, textAnchor: "middle", fontSize: "10", fill: r.textSec, children: P(N.low) }, `hlbl-${O}`)
        )
      ] }),
      Ne,
      Se,
      Oe,
      Be
    ] }) });
  }
  if (n === "boxplot") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (w) => !be.has(w.name)
    ) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = r.gridLine || r.border || "#eee";
    if (c.length < 5)
      return /* @__PURE__ */ e.jsx(we, { message: "Box Plot needs 5 measures: Min, Q1, Median, Q3, Max.", theme: r, chartHeight: L });
    const k = o.map(
      /** @param {any} [_] @param {any} [ci] @returns {any} */
      (w, b) => [
        c[0].values[b] ?? 0,
        c[1].values[b] ?? 0,
        c[2].values[b] ?? 0,
        c[3].values[b] ?? 0,
        c[4].values[b] ?? 0
      ]
    ), f = ye(c[2].name, 0), M = {
      backgroundColor: l,
      tooltip: {
        trigger: "axis",
        /** @param {any} [params] @returns {any} */
        formatter: (
          /** @param {any} [params] @returns {any} */
          ((w) => {
            const b = w[0];
            return `${b.name}<br/>Min: ${P(b.data[1])}<br/>Q1: ${P(b.data[2])}<br/>Median: ${P(b.data[3])}<br/>Q3: ${P(b.data[4])}<br/>Max: ${P(b.data[5])}`;
          })
        )
      },
      xAxis: { type: "category", data: o.map(
        /** @param {any} [c] @returns {any} */
        (w) => pe(String(w), 14)
      ), axisLabel: { color: u, fontSize: 10 }, axisLine: { lineStyle: { color: p } } },
      yAxis: { type: "value", axisLabel: { color: u, fontSize: 10, formatter: P }, axisLine: { lineStyle: { color: p } }, splitLine: { lineStyle: { color: g } }, name: K || "", nameTextStyle: { color: u } },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      series: [{ type: "boxplot", data: k, itemStyle: { color: f, borderColor: f } }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: M }) });
  }
  if (n === "nightingale") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (k) => !be.has(k.name)
    ) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333";
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Nightingale charts need categories and at least one measure.", theme: r, chartHeight: L });
    const p = o.map(
      /** @param {any} [cat] @param {any} [ci] @returns {any} */
      (k, f) => {
        const M = c[0] && c[0].values ? c[0].values[f] ?? 0 : 0;
        return { name: String(k), value: M, itemStyle: { color: ye(String(k), f) } };
      }
    ), g = {
      backgroundColor: l,
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
          show: C,
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
        (k) => pe(String(k), 12)
      ), axisLabel: { color: u, fontSize: 9 } },
      radiusAxis: { axisLabel: { color: u, fontSize: 9, formatter: P } }
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: g }) });
  }
  if (n === "heatmap") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (O) => !be.has(O.name)
    ) : [];
    if (o.length === 0 || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Heatmap needs categories and at least one measure.", theme: r, chartHeight: L });
    const l = c.flatMap(
      /** @param {any} [s] @returns {any} */
      (O) => (O.values || []).filter(
        /** @param {any} [v] @returns {any} */
        (X) => typeof X == "number" && Number.isFinite(X)
      )
    ), u = l.length ? Math.min(...l) : 0, g = (l.length ? Math.max(...l) : 1) - u || 1, k = (O) => {
      const X = Math.max(0, Math.min(1, (O - u) / g)), oe = Math.round(59 + X * 180), y = Math.round(130 - X * 62), q = Math.round(246 - X * 178);
      return `rgb(${oe},${y},${q})`;
    }, f = 2, M = Math.min(140, Math.max(60, c.reduce(
      /** @param {any} [m] @param {any} [s] @returns {any} */
      (O, X) => Math.max(O, $t(X.name, 11)),
      0
    ) + 10)), w = 32, b = Z, H = L, _ = b - M - 8, A = H - w - 8, j = Math.max(20, _ / o.length), E = Math.max(20, A / c.length), N = c.map(
      /** @param {any} [s] @param {any} [i] @returns {any} */
      (O, X) => ({ label: O.name, color: Ce(X, r, le) })
    );
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative", ...U }, ...je, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: D,
            viewBox: `0 0 ${b} ${H}`,
            preserveAspectRatio: "xMidYMid meet",
            style: { width: "100%", height: "auto", display: "block", maxWidth: "100%", ...et },
            role: "img",
            "aria-label": t.title || "Heatmap",
            children: [
              o.map(
                /** @param {any} [cat] @param {any} [ci] @returns {any} */
                (O, X) => /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: M + X * j + j / 2,
                    y: w - 6,
                    textAnchor: "middle",
                    fontSize: "10",
                    fill: r.textSec,
                    style: { pointerEvents: "none" },
                    children: pe(O, Math.max(4, Math.floor(j / 6.5)))
                  },
                  `hm-col-${X}`
                )
              ),
              c.map(
                /** @param {any} [ser] @param {any} [si] @returns {any} */
                (O, X) => /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: M - 6,
                    y: w + X * E + E / 2 + 4,
                    textAnchor: "end",
                    fontSize: "11",
                    fill: r.textSec,
                    style: { pointerEvents: "none" },
                    children: pe(O.name, Math.max(6, Math.floor(M / 6.5)))
                  },
                  `hm-row-${X}`
                )
              ),
              c.map(
                /** @param {any} [ser] @param {any} [si] @returns {any} */
                (O, X) => o.map(
                  /** @param {any} [cat] @param {any} [ci] @returns {any} */
                  (oe, y) => {
                    const q = O.values && O.values[y];
                    if (q == null) return null;
                    const ne = k(q), W = M + y * j, R = w + X * E;
                    return /* @__PURE__ */ e.jsx(
                      "rect",
                      {
                        x: W + f,
                        y: R + f,
                        width: Math.max(1, j - f * 2),
                        height: Math.max(1, E - f * 2),
                        rx: "3",
                        fill: ne,
                        "data-tip-cat": oe,
                        "data-tip-ser": O.name,
                        "data-tip-val": P(q),
                        "data-tip-color": ne
                      },
                      `hm-cell-${X}-${y}`
                    );
                  }
                )
              ),
              C && c.map(
                /** @param {any} [ser] @param {any} [si] @returns {any} */
                (O, X) => o.map(
                  /** @param {any} [cat] @param {any} [ci] @returns {any} */
                  (oe, y) => {
                    const q = O.values && O.values[y];
                    if (q == null || j < 36 || E < 18) return null;
                    const W = Math.max(0, Math.min(1, (q - u) / g)) > 0.5 ? "#fff" : r.text;
                    return /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: M + y * j + j / 2,
                        y: w + X * E + E / 2 + 4,
                        textAnchor: "middle",
                        fontSize: "9",
                        fontWeight: "700",
                        fill: W,
                        style: { pointerEvents: "none" },
                        children: P(q)
                      },
                      `hm-label-${X}-${y}`
                    );
                  }
                )
              )
            ]
          }
        ),
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: N, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (n === "bar3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (f) => !be.has(f.name)
    ) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = [];
    c.forEach(
      /** @param {any} [ser] @param {any} [si] @returns {any} */
      (f, M) => {
        o.forEach(
          /** @param {any} [cat] @param {any} [ci] @returns {any} */
          (w, b) => {
            const H = f.values[b];
            H != null && g.push([b, M, H]);
          }
        );
      }
    );
    const k = {
      backgroundColor: l,
      legend: {
        show: m,
        data: c.map(
          /** @param {any} [s] @returns {any} */
          (f) => f.name
        ),
        textStyle: { color: u },
        type: "scroll"
      },
      tooltip: {
        /** @param {any} [params] @returns {any} */
        formatter: (f) => {
          const M = o[f.value[0]] || "", w = c[f.value[1]] ? c[f.value[1]].name : "";
          return `${M} / ${w}: <b>${P(f.value[2])}</b>`;
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
          (f) => pe(String(f), 12)
        ),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } },
        name: ""
      },
      yAxis3D: {
        type: "category",
        data: c.map(
          /** @param {any} [s] @returns {any} */
          (f) => pe(String(f.name), 12)
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
          itemStyle: { color: Ce(f[1], r, le), opacity: 0.9 }
        })),
        shading: "lambert",
        label: {
          show: C,
          /** @param {any} [p] @returns {any} */
          formatter: (f) => P(f.value[2]),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { label: { show: !0 }, itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: k }) });
  }
  if (n === "line3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (f) => !be.has(f.name)
    ) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = c.map(
      /** @param {any} [ser] @param {any} [si] @returns {any} */
      (f, M) => ({
        type: "line3D",
        data: o.map(
          /** @param {any} [_] @param {any} [ci] @returns {any} */
          (w, b) => {
            const H = f.values[b];
            return [b, M, H ?? 0];
          }
        ),
        lineStyle: { color: Ce(M, r, le), width: 3, opacity: 0.9 },
        name: f.name
      })
    ), k = {
      backgroundColor: l,
      legend: { show: m, data: c.map(
        /** @param {any} [s] @returns {any} */
        (f) => f.name
      ), textStyle: { color: u }, type: "scroll" },
      tooltip: {
        /** @param {any} [params] @returns {any} */
        formatter: (f) => {
          const M = o[f.value[0]] || "", w = c[f.value[1]] ? c[f.value[1]].name : "";
          return `${M} / ${w}: <b>${P(f.value[2])}</b>`;
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
          (f) => pe(String(f), 12)
        ),
        axisLabel: { color: u, fontSize: 10 },
        axisLine: { lineStyle: { color: p } }
      },
      yAxis3D: {
        type: "category",
        data: c.map(
          /** @param {any} [s] @returns {any} */
          (f) => pe(String(f.name), 12)
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
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: k }) });
  }
  if (n === "scatter3d") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (b) => !be.has(b.name)
    ) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc";
    if (c.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "3D Scatter needs at least two numeric measures (X and Y).", theme: r, chartHeight: L });
    const g = c[0], k = c[1], f = c[2] || null, M = o.map(
      /** @param {any} [cat] @param {any} [ci] @returns {any} */
      (b, H) => ({
        value: [g.values[H] ?? 0, k.values[H] ?? 0, f ? f.values[H] ?? 0 : H],
        name: String(b)
      })
    ), w = {
      backgroundColor: l,
      tooltip: {
        /** @param {any} [params] @returns {any} */
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
        data: M,
        symbolSize: 8,
        itemStyle: { color: Ce(0, r, le), opacity: 0.85 },
        label: {
          show: C,
          /** @param {any} [params] @returns {any} */
          formatter: (b) => String(o[b.dataIndex] || ""),
          textStyle: { color: u, fontSize: 10 }
        },
        emphasis: { itemStyle: { opacity: 1 } }
      }]
    };
    return /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: w }) });
  }
  if (n === "range") {
    const o = Array.isArray(t && t.categories) ? t.categories : [], c = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (b) => !be.has(b.name)
    ) : [], l = r.chartBg || r.surface || "#fff", u = r.text || "#333", p = r.border || "#ccc", g = r.gridLine || r.border || "#eee";
    if (c.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "Range chart needs at least two measures (low and high values).", theme: r, chartHeight: L });
    const k = [];
    for (let b = 0; b + 1 < c.length; b += 2)
      k.push({ low: c[b], high: c[b + 1] });
    const f = [];
    k.forEach((b, H) => {
      const _ = Ce(H, r, le);
      f.push({
        type: "bar",
        name: `_base_${H}`,
        data: o.map(
          /** @param {any} [_] @param {any} [ci] @returns {any} */
          (A, j) => Math.min(b.low.values[j] ?? 0, b.high.values[j] ?? 0)
        ),
        stack: `range_${H}`,
        itemStyle: { opacity: 0 },
        emphasis: { disabled: !0 },
        silent: !0,
        legendHoverLink: !1
      }), f.push({
        type: "bar",
        name: `${b.low.name} – ${b.high.name}`,
        data: o.map(
          /** @param {any} [_] @param {any} [ci] @returns {any} */
          (A, j) => Math.abs((b.high.values[j] ?? 0) - (b.low.values[j] ?? 0))
        ),
        stack: `range_${H}`,
        itemStyle: { color: _, opacity: 0.9 },
        label: {
          show: C,
          position: "inside",
          /** @param {any} [params] @returns {any} */
          formatter: (A) => {
            const j = A.dataIndex, E = b.low.values[j] ?? 0, N = b.high.values[j] ?? 0;
            return `${P(Math.min(E, N))}–${P(Math.max(E, N))}`;
          },
          color: u,
          fontSize: 10
        },
        tooltip: {
          /** @param {any} [params] @returns {any} */
          formatter: (A) => {
            const j = A.dataIndex;
            return `${o[j]}<br/>${b.low.name}: ${P(b.low.values[j] ?? 0)}<br/>${b.high.name}: ${P(b.high.values[j] ?? 0)}`;
          }
        }
      });
    });
    const M = k.map((b, H) => ({ label: `${b.low.name} – ${b.high.name}`, color: Ce(H, r, le) })), w = {
      backgroundColor: l,
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: o.map(
          /** @param {any} [c] @returns {any} */
          (b) => pe(String(b), 14)
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
      /* @__PURE__ */ e.jsx("div", { style: { width: "100%", height: h, background: l, borderRadius: r.radiusSm || "8px", overflow: "hidden" }, children: /* @__PURE__ */ e.jsx(er, { chartType: n, echartsRef: $, option: w }) }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: M, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (n === "scatter") {
    const o = Array.isArray(t && t.series) ? t.series.filter(
      /** @param {any} [s] @returns {any} */
      (W) => !be.has(W.name)
    ) : [], c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (o.length < 2)
      return /* @__PURE__ */ e.jsx(we, { message: "Scatter charts need at least two numeric measures (X and Y).", theme: r, chartHeight: L });
    const u = o[0], p = o.slice(1), g = (u.values || []).filter(
      /** @param {any} [v] @returns {any} */
      (W) => typeof W == "number" && Number.isFinite(W)
    ), k = p.flatMap(
      /** @param {any} [s] @returns {any} */
      (W) => (W.values || []).filter(
        /** @param {any} [v] @returns {any} */
        (R) => typeof R == "number" && Number.isFinite(R)
      )
    );
    if (g.length === 0 || k.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Not enough numeric data for a scatter chart.", theme: r, chartHeight: L });
    let f = Math.min(...g), M = Math.max(...g), w = Math.min(...k), b = Math.max(...k);
    if (f === M) {
      const W = Math.abs(M || 1) * 0.15 || 1;
      f -= W, M += W;
    }
    if (w === b) {
      const W = Math.abs(b || 1) * 0.15 || 1;
      w -= W, b += W;
    }
    const H = (M - f) * 0.06, _ = (b - w) * 0.06;
    f -= H, M += H, w -= _, b += _;
    const j = { top: 24, right: 20, bottom: 64, left: Math.max(52, $t(P(b), 11) + 16) }, E = Z - j.left - j.right, N = L - j.top - j.bottom, O = (W) => j.left + (W - f) / (M - f) * E, X = (W) => j.top + (b - W) / (b - w) * N, oe = Ht(f, M, 5), y = Ht(w, b, 5), q = p.map(
      /** @param {any} [s] @param {any} [i] @returns {any} */
      (W, R) => ({ label: W.name, color: ye(W.name, R) })
    ), ne = typeof S == "function";
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { style: { position: "relative", width: "100%" }, ...je, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: D,
            viewBox: `0 0 ${Z} ${L}`,
            width: "100%",
            preserveAspectRatio: "xMidYMid meet",
            style: { display: "block", overflow: "visible" },
            children: [
              y.map(
                /** @param {any} [tick] @param {any} [i] @returns {any} */
                (W, R) => /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: X(W), x2: Z - j.right, y2: X(W), stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: j.left - 8, y: X(W) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(W) })
                ] }, `y-tick-${R}`)
              ),
              oe.map(
                /** @param {any} [tick] @param {any} [i] @returns {any} */
                (W, R) => /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: O(W), y1: j.top, x2: O(W), y2: L - j.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: O(W), y: L - j.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(W) })
                ] }, `x-tick-${R}`)
              ),
              /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: L - j.bottom, x2: Z - j.right, y2: L - j.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("line", { x1: j.left, y1: j.top, x2: j.left, y2: L - j.bottom, stroke: r.border }),
              /* @__PURE__ */ e.jsx("text", { x: Z / 2, y: L - 8, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: u.name }),
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (j.top + L - j.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(j.top + L - j.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              p.map(
                /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                (W, R) => {
                  const ie = ye(W.name, R);
                  return (W.values || []).map(
                    /** @param {any} [yVal] @param {any} [pointIndex] @returns {any} */
                    (se, re) => {
                      const I = u.values[re];
                      if (typeof I != "number" || !Number.isFinite(I) || typeof se != "number" || !Number.isFinite(se)) return null;
                      const te = O(I), ue = X(se), Pe = c[re] || `Point ${re + 1}`, qt = l[re], mt = {
                        "data-tip-cat": Pe,
                        "data-tip-ser": `${W.name} (${u.name}=${P(I)})`,
                        "data-tip-val": P(se),
                        "data-tip-color": ie
                      };
                      return pt.cloneElement(Ot(te, ue, ie, mt, {
                        style: ne && qt ? { cursor: "pointer" } : void 0,
                        onClick: ne && qt ? () => S(qt) : void 0
                      }), { key: `scatter-${R}-${re}` });
                    }
                  );
                }
              ),
              C ? p.map(
                /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                (W, R) => {
                  const ie = ye(W.name, R);
                  return (W.values || []).map(
                    /** @param {any} [yVal] @param {any} [pointIndex] @returns {any} */
                    (se, re) => {
                      const I = u.values[re];
                      return typeof I != "number" || !Number.isFinite(I) || typeof se != "number" || !Number.isFinite(se) ? null : /* @__PURE__ */ e.jsx(
                        "text",
                        {
                          x: O(I),
                          y: X(se) - 8,
                          textAnchor: "middle",
                          fontSize: "10",
                          fontWeight: "700",
                          fill: ie,
                          style: { pointerEvents: "none" },
                          children: P(se)
                        },
                        `scatter-label-${R}-${re}`
                      );
                    }
                  );
                }
              ) : null
            ]
          }
        ),
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: q, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (n === "waterfall") {
    const o = Array.isArray(t && t.series) && t.series.length > 0 ? t.series[0] : null, c = Array.isArray(t && t.categories) ? t.categories : [], l = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [];
    if (!o || c.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: "Waterfall charts need categories and at least one numeric measure.", theme: r, chartHeight: L });
    const u = c.map(
      /** @param {any} [_] @param {any} [i] @returns {any} */
      (W, R) => {
        const ie = o.values[R];
        return typeof ie == "number" && Number.isFinite(ie) ? ie : 0;
      }
    ), p = [];
    let g = 0;
    for (let W = 0; W < u.length; W++) {
      const R = g;
      g += u[W], p.push({ label: c[W], start: R, end: g, increment: u[W], type: u[W] >= 0 ? "positive" : "negative" });
    }
    p.push({ label: "Total", start: 0, end: g, increment: g, type: "total" });
    const k = p.map((W) => W.start).concat(p.map((W) => W.end));
    let f = Math.min(...k, 0), M = Math.max(...k, 0);
    if (f === M) {
      const W = Math.abs(M || 1) * 0.15 || 1;
      f -= W, M += W;
    }
    const w = (M - f) * 0.08;
    f -= w, M += w;
    const b = Ht(f, M, 5), _ = { top: 24, right: 20, bottom: 84, left: Math.max(52, $t(P(M), 11) + 16) }, A = Z - _.left - _.right, j = L - _.top - _.bottom, E = (W) => _.top + (M - W) / (M - f) * j, N = A / p.length, O = Math.max(14, Math.min(60, N * 0.6)), X = "#059669", oe = "#DC2626", y = Ce(0, r, le), q = typeof S == "function", ne = [
      { label: "Increase", color: X },
      { label: "Decrease", color: oe },
      { label: "Total", color: y }
    ];
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative" }, ...je, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: D,
            viewBox: `0 0 ${Z} ${L}`,
            width: "100%",
            preserveAspectRatio: "xMidYMid meet",
            style: { display: "block", overflow: "visible" },
            children: [
              b.map(
                /** @param {any} [tick] @param {any} [i] @returns {any} */
                (W, R) => /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: _.left, y1: E(W), x2: Z - _.right, y2: E(W), stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: _.left - 8, y: E(W) + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, style: { pointerEvents: "none" }, children: P(W) })
                ] }, `wf-tick-${R}`)
              ),
              /* @__PURE__ */ e.jsx("line", { x1: _.left, y1: E(0), x2: Z - _.right, y2: E(0), stroke: r.textSec, strokeOpacity: "0.5" }),
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (_.top + L - _.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(_.top + L - _.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
              p.map((W, R) => {
                const ie = _.left + R * N + N / 2, se = ie - O / 2, re = E(Math.max(W.start, W.end)), I = E(Math.min(W.start, W.end)), te = Math.max(1, I - re), ue = W.type === "total" ? y : W.type === "positive" ? X : oe, Pe = R < l.length ? l[R] : null;
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  R > 0 ? /* @__PURE__ */ e.jsx(
                    "line",
                    {
                      x1: _.left + (R - 1) * N + N / 2 + O / 2,
                      y1: R === p.length - 1 ? E(W.end) : E(W.start),
                      x2: se,
                      y2: R === p.length - 1 ? E(W.end) : E(W.start),
                      stroke: r.textSec,
                      strokeDasharray: "2 2",
                      strokeOpacity: "0.5"
                    }
                  ) : null,
                  /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: se,
                      y: re,
                      width: O,
                      height: te,
                      rx: "3",
                      fill: ue,
                      opacity: "0.9",
                      "data-tip-cat": W.label,
                      "data-tip-ser": W.type === "total" ? "Total" : (W.increment >= 0 ? "+" : "") + P(W.increment),
                      "data-tip-val": P(W.end),
                      "data-tip-color": ue,
                      style: q && Pe ? { cursor: "pointer" } : void 0,
                      onClick: q && Pe ? () => S(Pe) : void 0
                    }
                  ),
                  C ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: ie,
                      y: re - 5,
                      textAnchor: "middle",
                      fontSize: "9",
                      fontWeight: "700",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: W.type === "total" ? P(W.end) : (W.increment >= 0 ? "+" : "") + P(W.increment)
                    }
                  ) : null,
                  /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: ie,
                      y: L - _.bottom + 16,
                      textAnchor: "middle",
                      fontSize: "10",
                      fontWeight: "600",
                      fill: r.textSec,
                      style: { pointerEvents: "none" },
                      children: pe(W.label, 12)
                    }
                  )
                ] }, `wf-bar-${R}`);
              })
            ]
          }
        ),
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: ne, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (n === "combo") {
    const o = (Array.isArray(t && t.layers) ? t.layers : []).filter(
      /** @param {any} [layer] @returns {any} */
      (y) => y && !y.hidden && !be.has(y.name) && Array.isArray(y.values) && y.values.some(
        /** @param {any} [value] @returns {any} */
        (q) => q !== null
      )
    );
    if (o.length === 0)
      return /* @__PURE__ */ e.jsx(we, { message: t.emptyMessage || "No combo layers are configured.", theme: r, chartHeight: L });
    const c = o.some(
      /** @param {any} [layer] @returns {any} */
      (y) => y.axis === "right"
    ), l = { top: 24, right: c ? 76 : 20, bottom: 96, left: 72 }, u = Math.max(1, Array.isArray(t.categories) ? t.categories.length : 0), p = Math.max(1, (Z - l.left - l.right) / u), g = Math.max(22, Math.min(72, p * 0.72)), k = o.filter(
      /** @param {any} [layer] @returns {any} */
      (y) => y.type === "bar"
    ), f = o.filter(
      /** @param {any} [layer] @returns {any} */
      (y) => y.type === "area"
    ), M = o.filter(
      /** @param {any} [layer] @returns {any} */
      (y) => y.type === "line"
    ), w = Ir(
      o.filter(
        /** @param {any} [layer] @returns {any} */
        (y) => y.axis !== "right"
      ).flatMap(
        /** @param {any} [layer] @returns {any} */
        (y) => y.values
      ),
      L,
      l
    ), b = c ? Ir(
      o.filter(
        /** @param {any} [layer] @returns {any} */
        (y) => y.axis === "right"
      ).flatMap(
        /** @param {any} [layer] @returns {any} */
        (y) => y.values
      ),
      L,
      l
    ) : w, H = Array.isArray(t.categoryTargets) ? t.categoryTargets : [], _ = o.map(
      /** @param {any} [layer] @param {any} [index] @returns {any} */
      (y, q) => ({
        label: y.name,
        color: y.color || Ce(q, r, le)
      })
    ), A = (y) => y.axis === "right" ? b.scaleY : w.scaleY, j = (y) => y.axis === "right" ? b.baselineY : w.baselineY, E = (y) => (
      /** @param {any} [v] @returns {any} */
      ((q) => Wt(q, y.valueFormat || J || "auto"))
    ), N = o.find(
      /** @param {any} [layer] @returns {any} */
      (y) => y.axis === "right"
    ), O = N ? E(N) : P, X = 4, oe = Math.max(8, (g - (Math.max(k.length, 1) - 1) * X) / Math.max(k.length, 1));
    return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative", ...U }, ...je, children: [
        /* @__PURE__ */ e.jsxs(
          "svg",
          {
            ref: D,
            viewBox: `0 0 ${Z} ${L}`,
            preserveAspectRatio: "xMidYMid meet",
            style: {
              width: "100%",
              height: "auto",
              display: "block",
              maxWidth: "100%",
              ...et
            },
            role: "img",
            "aria-label": t.title || "Combo chart",
            children: [
              w.tickValues.map(
                /** @param {any} [tickValue] @param {any} [index] @returns {any} */
                (y, q) => {
                  const ne = w.scaleY(y);
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    /* @__PURE__ */ e.jsx(
                      "line",
                      {
                        x1: l.left,
                        x2: Z - l.right,
                        y1: ne,
                        y2: ne,
                        stroke: r.border,
                        strokeOpacity: "0.7",
                        strokeDasharray: "4 4"
                      }
                    ),
                    /* @__PURE__ */ e.jsx(
                      "text",
                      {
                        x: l.left - 10,
                        y: ne + 4,
                        textAnchor: "end",
                        fontSize: "11",
                        fill: r.textSec,
                        children: P(y)
                      }
                    )
                  ] }, `combo-left-tick-${q}`);
                }
              ),
              c ? b.tickValues.map(
                /** @param {any} [tickValue] @param {any} [index] @returns {any} */
                (y, q) => {
                  const ne = b.scaleY(y);
                  return /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: Z - l.right + 10,
                      y: ne + 4,
                      textAnchor: "start",
                      fontSize: "11",
                      fill: r.textSec,
                      children: O(y)
                    },
                    `combo-right-tick-${q}`
                  );
                }
              ) : null,
              K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (l.top + L - l.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(l.top + L - l.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
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
              k.map(
                /** @param {any} [layer] @param {any} [barLayerIndex] @returns {any} */
                (y, q) => t.categories.map(
                  /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
                  (ne, W) => {
                    const R = y.values[W];
                    if (R == null) return null;
                    const ie = l.left + W * p + (p - g) / 2, se = A(y), re = j(y), I = se(R), te = y.color || Ce(q, r, le), ue = ie + q * (oe + X), Pe = E(y);
                    return /* @__PURE__ */ e.jsxs(pt.Fragment, { children: [
                      /* @__PURE__ */ e.jsx(
                        "rect",
                        {
                          x: ue,
                          y: Math.min(I, re),
                          width: oe,
                          height: Math.max(1, Math.abs(re - I)),
                          rx: "3",
                          fill: te,
                          opacity: "0.9",
                          "data-tip-cat": ne,
                          "data-tip-ser": y.name,
                          "data-tip-val": Pe(R),
                          "data-tip-color": te
                        }
                      ),
                      C ? /* @__PURE__ */ e.jsx(
                        "text",
                        {
                          x: ue + oe / 2,
                          y: R >= 0 ? Math.min(I, re) - 4 : Math.max(I, re) + 12,
                          textAnchor: "middle",
                          fontSize: "9",
                          fontWeight: "700",
                          fill: r.textSec,
                          style: { pointerEvents: "none" },
                          children: Pe(R)
                        }
                      ) : null
                    ] }, `${y.id}-${ne}-${q}`);
                  }
                )
              ),
              f.map(
                /** @param {any} [layer] @param {any} [layerIndex] @returns {any} */
                (y, q) => {
                  const ne = t.categories.map(
                    /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                    (re, I) => {
                      const te = y.values[I];
                      if (te == null) return null;
                      const ue = l.left + I * p + p / 2, Pe = A(y);
                      return {
                        x: ue,
                        y: Pe(te),
                        baseY: j(y),
                        rawValue: te
                      };
                    }
                  ), W = Hr(ne), R = fr(ne), ie = y.color || Ce(k.length + q, r, le), se = E(y);
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    W ? /* @__PURE__ */ e.jsx("path", { d: W, fill: ie, opacity: "0.16", style: { pointerEvents: "none" } }) : null,
                    R ? /* @__PURE__ */ e.jsx("path", { d: R, fill: "none", stroke: ie, strokeWidth: "2.5", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: gr(Lt[y.name]), style: { pointerEvents: "none" } }) : null,
                    ne.map(
                      /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
                      (re, I) => re ? /* @__PURE__ */ e.jsxs(pt.Fragment, { children: [
                        /* @__PURE__ */ e.jsx(
                          "circle",
                          {
                            cx: re.x,
                            cy: re.y,
                            r: "4",
                            fill: ie,
                            stroke: r.surfaceBg || r.background || "#fff",
                            strokeWidth: "2",
                            "data-tip-cat": t.categories[I],
                            "data-tip-ser": y.name,
                            "data-tip-val": se(re.rawValue),
                            "data-tip-color": ie
                          }
                        ),
                        C ? /* @__PURE__ */ e.jsx("text", { x: re.x, y: re.y - 8, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: se(re.rawValue) }) : null
                      ] }, `${y.id}-area-pt-${I}`) : null
                    )
                  ] }, `combo-area-${y.id}`);
                }
              ),
              M.map(
                /** @param {any} [layer] @param {any} [layerIndex] @returns {any} */
                (y, q) => {
                  const ne = t.categories.map(
                    /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                    (se, re) => {
                      const I = y.values[re];
                      return I == null ? null : {
                        x: l.left + re * p + p / 2,
                        y: A(y)(I),
                        rawValue: I
                      };
                    }
                  ), W = fr(ne), R = y.color || Ce(k.length + f.length + q, r, le), ie = E(y);
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    W ? /* @__PURE__ */ e.jsx("path", { d: W, fill: "none", stroke: R, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: gr(Lt[y.name]), style: { pointerEvents: "none" } }) : null,
                    ne.map(
                      /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
                      (se, re) => se ? /* @__PURE__ */ e.jsxs(pt.Fragment, { children: [
                        /* @__PURE__ */ e.jsx(
                          "circle",
                          {
                            cx: se.x,
                            cy: se.y,
                            r: "4",
                            fill: R,
                            stroke: r.surfaceBg || r.background || "#fff",
                            strokeWidth: "2",
                            "data-tip-cat": t.categories[re],
                            "data-tip-ser": y.name,
                            "data-tip-val": ie(se.rawValue),
                            "data-tip-color": R
                          }
                        ),
                        C ? /* @__PURE__ */ e.jsx("text", { x: se.x, y: se.y - 8, textAnchor: "middle", fontSize: "9", fontWeight: "700", fill: r.textSec, style: { pointerEvents: "none" }, children: ie(se.rawValue) }) : null
                      ] }, `${y.id}-point-${re}`) : null
                    )
                  ] }, `combo-line-${y.id}`);
                }
              ),
              t.categories.map(
                /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
                (y, q) => {
                  const ne = l.left + q * p + p / 2;
                  return /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: ne,
                      y: L - l.bottom + 28,
                      textAnchor: "middle",
                      fontSize: "11",
                      fill: r.textSec,
                      children: pe(y, 18)
                    },
                    `combo-label-${q}`
                  );
                }
              ),
              typeof S == "function" ? t.categories.map(
                /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                (y, q) => {
                  const ne = H[q];
                  return ne ? /* @__PURE__ */ e.jsx(
                    "rect",
                    {
                      x: l.left + q * p,
                      y: l.top,
                      width: p,
                      height: L - l.top - l.bottom,
                      fill: "transparent",
                      style: { cursor: "pointer" },
                      onClick: () => S(ne)
                    },
                    `combo-hit-${q}`
                  ) : null;
                }
              ) : null
            ]
          }
        ),
        Ne,
        Se,
        Oe,
        Be
      ] }),
      m ? /* @__PURE__ */ e.jsx(jt, { items: _, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
    ] });
  }
  if (!x)
    return /* @__PURE__ */ e.jsx(we, { message: "No numeric data available for this chart.", theme: r, chartHeight: L });
  const kr = Ht(x.minValue, x.maxValue, G || 5), Ct = Ke ? Array.from(new Set(
    (t.stackedGroups || []).reduce(
      /** @param {any} [labels] @param {any} [group] @returns {any} */
      (o, c) => ((c.segments || []).forEach(
        /** @param {any} [segment] @returns {any} */
        (l) => {
          l && l.label && o.push(l.label);
        }
      ), o),
      []
    )
  )) : [], ur = Ct.reduce(
    /** @param {any} [acc] @param {any} [label] @param {any} [index] @returns {any} */
    (o, c, l) => (o[c] = Ce(l, r, le), o),
    {}
  ), At = Ke ? Ct.map(
    /** @param {any} [label] @returns {any} */
    (o) => ({ label: o, color: ur[o] || Ce(0, r, le) })
  ) : (t.series || []).map(
    /** @param {any} [series] @param {any} [index] @returns {any} */
    (o, c) => ({
      label: o.name,
      color: ye(o.name, c)
    })
  ), Zt = x.hasHierarchicalBands ? t.categoryBands.reduce(
    /** @param {any} [groups] @param {any} [band] @param {any} [index] @returns {any} */
    (o, c, l) => {
      const u = o[o.length - 1];
      return u && u.groupKey === c.groupKey ? (u.endIndex = l, o) : (o.push({
        groupKey: c.groupKey,
        outerLabel: c.outerLabel,
        startIndex: l,
        endIndex: l
      }), o);
    },
    []
  ) : [], Vt = Array.isArray(t && t.categoryTargets) ? t.categoryTargets : [], wr = typeof S == "function" && Vt.length === t.categories.length;
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
    /* @__PURE__ */ e.jsxs("div", { ref: Fe, style: { width: "100%", minWidth: 0, position: "relative", ...U }, ...je, children: [
      /* @__PURE__ */ e.jsxs(
        "svg",
        {
          ref: D,
          viewBox: `0 0 ${Z} ${L}`,
          preserveAspectRatio: "xMidYMid meet",
          style: {
            width: "100%",
            height: "auto",
            display: "block",
            maxWidth: "100%",
            ...et
          },
          role: "img",
          "aria-label": t.title,
          onClick: ke ? lt : void 0,
          children: [
            ae ? /* @__PURE__ */ e.jsx("defs", { children: (t.series || []).map(
              /** @param {any} [ser] @param {any} [si] @returns {any} */
              (o, c) => {
                const l = ye(o.name, c), u = zr.find((g) => g.value === nr) || zr[0], p = Ln(l, sr);
                return /* @__PURE__ */ e.jsxs("linearGradient", { id: `pg-grad-${c}`, x1: u.x1, y1: u.y1, x2: u.x2, y2: u.y2, children: [
                  /* @__PURE__ */ e.jsx("stop", { offset: p[0].offset, stopColor: l, stopOpacity: p[0].stopOpacity }),
                  /* @__PURE__ */ e.jsx("stop", { offset: p[1].offset, stopColor: l, stopOpacity: p[1].stopOpacity })
                ] }, `grad-${c}`);
              }
            ) }) : null,
            kr.map(
              /** @param {any} [tickValue] @param {any} [index] @returns {any} */
              (o, c) => {
                const l = x.scaleY ? x.scaleY(o) : null, u = x.scaleX ? x.scaleX(o) : null, p = ze ? `${Math.round(o)}%` : P(o);
                return /* @__PURE__ */ e.jsx("g", { children: x.horizontalBarMode ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: u, y1: x.margin.top, x2: u, y2: L - x.margin.bottom, stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: u, y: L - x.margin.bottom + 18, textAnchor: "middle", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: ke ? { cursor: "pointer" } : void 0, children: p })
                ] }) : /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: l, x2: Z - x.margin.right, y2: l, stroke: r.border, strokeDasharray: "3 4" }),
                  /* @__PURE__ */ e.jsx("text", { x: x.margin.left - 8, y: l + 4, textAnchor: "end", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "y", style: ke ? { cursor: "pointer" } : void 0, children: p })
                ] }) }, `tick-${c}`);
              }
            ),
            /* @__PURE__ */ e.jsx(
              "line",
              {
                x1: x.horizontalBarMode ? x.baselineX : x.margin.left,
                y1: x.horizontalBarMode ? x.margin.top : x.baselineY,
                x2: x.horizontalBarMode ? x.baselineX : Z - x.margin.right,
                y2: x.horizontalBarMode ? L - x.margin.bottom : x.baselineY,
                stroke: r.textSec,
                strokeWidth: "1"
              }
            ),
            K ? /* @__PURE__ */ e.jsx("text", { x: 14, y: (x.margin.top + L - x.margin.bottom) / 2, textAnchor: "middle", fontSize: "11", fontWeight: "700", fill: r.textSec, transform: `rotate(-90, 14, ${(x.margin.top + L - x.margin.bottom) / 2})`, style: { pointerEvents: "none" }, children: K }) : null,
            Array.isArray(T) && T.length > 0 && T.map((o, c) => {
              const l = Number(o.value);
              if (!Number.isFinite(l)) return null;
              const u = o.color || r.primary, p = o.orient === "v", g = gr(o.dash || "dashed") || "6 3";
              if (p) {
                const f = x.margin.left + l * x.step;
                return f < x.margin.left - 2 || f > Z - x.margin.right + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                  /* @__PURE__ */ e.jsx("line", { x1: f, y1: x.margin.top, x2: f, y2: L - x.margin.bottom, stroke: u, strokeWidth: "1.5", strokeDasharray: g, style: { pointerEvents: "none" } }),
                  o.label ? /* @__PURE__ */ e.jsx("text", { x: f + 4, y: x.margin.top + 12, textAnchor: "start", fontSize: "10", fontWeight: "700", fill: u, style: { pointerEvents: "none" }, children: o.label }) : null
                ] }, `ref-line-${c}`);
              }
              if (!x.scaleY) return null;
              const k = x.scaleY(l);
              return k < x.margin.top - 2 || k > L - x.margin.bottom + 2 ? null : /* @__PURE__ */ e.jsxs("g", { children: [
                /* @__PURE__ */ e.jsx("line", { x1: x.margin.left, y1: k, x2: Z - x.margin.right, y2: k, stroke: u, strokeWidth: "1.5", strokeDasharray: g, style: { pointerEvents: "none" } }),
                o.label ? /* @__PURE__ */ e.jsx("text", { x: Z - x.margin.right - 4, y: k - 4, textAnchor: "end", fontSize: "10", fontWeight: "700", fill: u, style: { pointerEvents: "none" }, children: o.label }) : null,
                typeof ft == "function" ? /* @__PURE__ */ e.jsx(
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
                      const M = f.clientY, w = l, { plotHeight: b, maxValue: H, minValue: _ } = x, A = H - _;
                      if (A <= 0) return;
                      const j = (N) => {
                        const O = w - (N.clientY - M) / b * A;
                        ft(
                          /** @param {any} [prev] @returns {any} */
                          (X) => X.map(
                            /** @param {any} [l] @param {any} [j] @returns {any} */
                            (oe, y) => y === c ? { ...oe, value: String(Math.round(O * 1e3) / 1e3) } : oe
                          )
                        );
                      }, E = () => {
                        window.removeEventListener("mousemove", j), window.removeEventListener("mouseup", E);
                      };
                      window.addEventListener("mousemove", j), window.addEventListener("mouseup", E);
                    }
                  }
                ) : null
              ] }, `ref-line-${c}`);
            }),
            rt && !x.horizontalBarMode && (n === "bar" || n === "line" || n === "area" || n === "scatter") && Array.isArray(t.series) && t.series.length > 0 && x.scaleY ? t.series.filter(
              /** @param {any} [s] @returns {any} */
              (c) => !be.has(c.name)
            ).map(
              /** @param {any} [ser] @param {any} [tsi] @returns {any} */
              (c, l) => {
                const u = c.values || [], p = ht === "moving_avg" ? Qn(u, Ze) : qn(u);
                if (!p) return null;
                const g = p.map(
                  /** @param {any} [v] @param {any} [i] @returns {any} */
                  (w, b) => Number.isFinite(w) ? { x: x.margin.left + b * x.step + x.step / 2, y: x.scaleY(w) } : null
                ), k = fr(g);
                if (!k) return null;
                const f = ye(c.name, l), M = ot || f;
                return /* @__PURE__ */ e.jsx(
                  "path",
                  {
                    d: k,
                    fill: "none",
                    stroke: M,
                    strokeWidth: String(Xt || 2),
                    strokeDasharray: "8 4",
                    strokeLinecap: "round",
                    opacity: "0.78",
                    style: { pointerEvents: "none" }
                  },
                  `trendline-${l}`
                );
              }
            ) : null,
            x.hasHierarchicalBands && Zt.map(
              /** @param {any} [group] @param {any} [groupIndex] @returns {any} */
              (o, c) => {
                if (c === Zt.length - 1) return null;
                const l = x.horizontalBarMode ? x.margin.top + (o.endIndex + 1) * x.step : x.margin.left + (o.endIndex + 1) * x.step;
                return /* @__PURE__ */ e.jsx(
                  "line",
                  {
                    x1: x.horizontalBarMode ? x.margin.left : l,
                    y1: x.horizontalBarMode ? l : x.margin.top,
                    x2: x.horizontalBarMode ? Z - x.margin.right : l,
                    y2: x.horizontalBarMode ? l : L - x.margin.bottom + 58,
                    stroke: r.border,
                    strokeDasharray: "4 4"
                  },
                  `group-divider-${o.groupKey}-${c}`
                );
              }
            ),
            Ke && (t.stackedGroups || []).map(
              /** @param {any} [group] @param {any} [groupIndex] @returns {any} */
              (o, c) => {
                const l = x.horizontalBarMode ? x.margin.top + c * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + c * x.step + (x.step - x.groupWidth) / 2, u = Math.max(14, x.groupWidth * 0.64);
                let p = 0, g = 0;
                return /* @__PURE__ */ e.jsxs("g", { children: [
                  (o.segments || []).map(
                    /** @param {any} [segment] @returns {any} */
                    (k) => {
                      const f = k && typeof k.value == "number" && Number.isFinite(k.value) ? k.value : null;
                      if (f === null) return null;
                      const M = f >= 0 ? p : g, w = M + f;
                      f >= 0 ? p = w : g = w;
                      const b = ur[k.label] || Ce(0, r, le);
                      if (x.horizontalBarMode) {
                        const E = x.scaleX(M), N = x.scaleX(w);
                        return /* @__PURE__ */ e.jsx(
                          "rect",
                          {
                            x: Math.min(E, N),
                            y: l,
                            width: Math.max(1, Math.abs(N - E)),
                            height: u,
                            rx: "3",
                            fill: b,
                            opacity: "0.94",
                            "data-tip-cat": o.label,
                            "data-tip-ser": k.label,
                            "data-tip-val": P(f),
                            "data-tip-color": b
                          },
                          `${o.key}-${k.path || k.label}`
                        );
                      }
                      const H = x.scaleY(M), _ = x.scaleY(w), A = Math.min(H, _), j = Math.max(1, Math.abs(_ - H));
                      return /* @__PURE__ */ e.jsx(
                        "rect",
                        {
                          x: l,
                          y: A,
                          width: u,
                          height: j,
                          rx: "3",
                          fill: b,
                          opacity: "0.94",
                          "data-tip-cat": o.label,
                          "data-tip-ser": k.label,
                          "data-tip-val": P(f),
                          "data-tip-color": b
                        },
                        `${o.key}-${k.path || k.label}`
                      );
                    }
                  ),
                  x.horizontalBarMode ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: x.margin.left - 10,
                      y: l + u / 2 + 4,
                      textAnchor: "end",
                      fontSize: "11",
                      fill: r.textSec,
                      children: pe(o.label, 22)
                    }
                  ) : /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: l + u / 2,
                      y: L - x.margin.bottom + 30,
                      textAnchor: "middle",
                      fontSize: "11",
                      fill: r.textSec,
                      children: pe(o.label, 20)
                    }
                  )
                ] }, `stacked-group-${o.key}`);
              }
            ),
            !Ke && n === "bar" && t.categories.map(
              /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
              (o, c) => {
                const l = x.horizontalBarMode ? x.margin.top + c * x.step + (x.step - x.groupWidth) / 2 : x.margin.left + c * x.step + (x.step - x.groupWidth) / 2, u = it || ze ? Math.max(14, x.groupWidth * 0.64) : Math.max(6, x.groupWidth / Math.max(t.series.length, 1) - 4);
                let p = 0, g = 0;
                return /* @__PURE__ */ e.jsx("g", { children: t.series.map(
                  /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                  (k, f) => {
                    if (be.has(k.name)) return null;
                    const M = k.values[c];
                    if (M == null) return null;
                    const w = ye(k.name, f), b = ae ? `url(#pg-grad-${f})` : w;
                    if (it || ze) {
                      const j = ze && He ? He[c] : null, E = ze && j ? M / j * 100 : M, N = E >= 0 ? p : g, O = N + E;
                      E >= 0 ? p = O : g = O;
                      const X = ze ? `${(Math.abs(M / j) * 100).toFixed(1)}% (${P(M)})` : P(M), oe = { "data-tip-cat": o, "data-tip-ser": k.name, "data-tip-val": X, "data-tip-color": w };
                      if (x.horizontalBarMode) {
                        const ne = x.scaleX(N), W = x.scaleX(O);
                        return /* @__PURE__ */ e.jsx(
                          "rect",
                          {
                            x: Math.min(ne, W),
                            y: l,
                            width: Math.max(1, Math.abs(W - ne)),
                            height: u,
                            rx: "3",
                            fill: b,
                            opacity: "0.92",
                            style: { transition: fe ? "width 0.35s ease, x 0.35s ease" : void 0 },
                            ...oe
                          },
                          `${k.name}-${f}-${o}-${c}`
                        );
                      }
                      const y = x.scaleY(N), q = x.scaleY(O);
                      return /* @__PURE__ */ e.jsx(
                        "rect",
                        {
                          x: l,
                          y: Math.min(y, q),
                          width: u,
                          height: Math.max(1, Math.abs(q - y)),
                          rx: "3",
                          fill: b,
                          opacity: "0.92",
                          style: { transition: fe ? "height 0.35s ease, y 0.35s ease" : void 0 },
                          ...oe
                        },
                        `${k.name}-${f}-${o}-${c}`
                      );
                    }
                    if (x.horizontalBarMode) {
                      const j = x.scaleX(M), E = l + f * (u + 4);
                      return /* @__PURE__ */ e.jsxs(pt.Fragment, { children: [
                        /* @__PURE__ */ e.jsx(
                          "rect",
                          {
                            x: Math.min(j, x.baselineX),
                            y: E,
                            width: Math.max(1, Math.abs(x.baselineX - j)),
                            height: u,
                            rx: "3",
                            fill: b,
                            opacity: "0.9",
                            style: { transition: fe ? "width 0.35s ease, x 0.35s ease" : void 0 },
                            "data-tip-cat": o,
                            "data-tip-ser": k.name,
                            "data-tip-val": P(M),
                            "data-tip-color": w
                          }
                        ),
                        C ? /* @__PURE__ */ e.jsx(
                          "text",
                          {
                            x: M >= 0 ? Math.max(j, x.baselineX) + 4 : Math.min(j, x.baselineX) - 4,
                            y: E + u / 2 + 3,
                            textAnchor: M >= 0 ? "start" : "end",
                            fontSize: "9",
                            fontWeight: "700",
                            fill: r.textSec,
                            style: { pointerEvents: "none" },
                            children: P(M)
                          }
                        ) : null
                      ] }, `${k.name}-${f}-${o}-${c}`);
                    }
                    const H = x.scaleY(M), _ = Math.max(1, Math.abs(x.baselineY - H)), A = l + f * (u + 4);
                    return /* @__PURE__ */ e.jsxs(pt.Fragment, { children: [
                      /* @__PURE__ */ e.jsx(
                        "rect",
                        {
                          x: A,
                          y: Math.min(H, x.baselineY),
                          width: u,
                          height: _,
                          rx: "3",
                          fill: b,
                          opacity: "0.9",
                          style: { transition: fe ? "height 0.35s ease, y 0.35s ease" : void 0, cursor: ke ? "pointer" : void 0 },
                          "data-tip-cat": o,
                          "data-tip-ser": k.name,
                          "data-tip-val": P(M),
                          "data-tip-color": w,
                          "data-chart-element-type": "series",
                          "data-chart-element-id": k.name
                        }
                      ),
                      C ? /* @__PURE__ */ e.jsx(
                        "text",
                        {
                          x: A + u / 2,
                          y: M >= 0 ? Math.min(H, x.baselineY) - 4 : Math.max(H, x.baselineY) + 12,
                          textAnchor: "middle",
                          fontSize: "9",
                          fontWeight: "700",
                          fill: r.textSec,
                          style: { pointerEvents: "none" },
                          children: P(M)
                        }
                      ) : null
                    ] }, `${k.name}-${f}-${o}-${c}`);
                  }
                ) }, `bar-group-${o}-${c}`);
              }
            ),
            (n === "line" || n === "area") && (() => {
              const o = t.categories.map(() => 0), c = t.categories.map(() => 0);
              return t.series.map(
                /** @param {any} [series] @param {any} [seriesIndex] @returns {any} */
                (l, u) => {
                  if (be.has(l.name)) return null;
                  const p = ye(l.name, u), g = t.categories.map(
                    /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
                    (M, w) => {
                      const b = l.values[w];
                      if (b == null) return null;
                      const H = x.margin.left + w * x.step + x.step / 2;
                      if (Bt || ze) {
                        const _ = ze && He ? He[w] : null, A = ze && _ ? b / _ * 100 : b, j = A >= 0 ? o[w] : c[w], E = j + A;
                        return A >= 0 ? o[w] = E : c[w] = E, {
                          x: H,
                          y: x.scaleY(E),
                          baseY: x.scaleY(j),
                          rawValue: b
                        };
                      }
                      return {
                        x: H,
                        y: x.scaleY(b),
                        baseY: x.baselineY,
                        rawValue: b
                      };
                    }
                  ), k = fr(g), f = n === "area" ? Hr(g) : "";
                  return /* @__PURE__ */ e.jsxs("g", { children: [
                    n === "area" && f && /* @__PURE__ */ e.jsx("path", { d: f, fill: ae ? `url(#pg-grad-${u})` : p, opacity: Bt || ze ? "0.46" : "0.16", style: { pointerEvents: "none" } }),
                    k && /* @__PURE__ */ e.jsx("path", { d: k, fill: "none", stroke: p, strokeWidth: "3", strokeLinejoin: "round", strokeLinecap: "round", strokeDasharray: gr(Lt[l.name]), style: { pointerEvents: "none" } }),
                    g.map(
                      /** @param {any} [point] @param {any} [pointIndex] @returns {any} */
                      (M, w) => M ? /* @__PURE__ */ e.jsxs(pt.Fragment, { children: [
                        pt.cloneElement(Ot(M.x, M.y, p, {
                          "data-tip-cat": t.categories[w],
                          "data-tip-ser": l.name,
                          "data-tip-val": P(M.rawValue),
                          "data-tip-color": p
                        }), { key: `marker-${u}-${w}` }),
                        C ? /* @__PURE__ */ e.jsx(
                          "text",
                          {
                            x: M.x,
                            y: M.y - 8,
                            textAnchor: "middle",
                            fontSize: "9",
                            fontWeight: "700",
                            fill: r.textSec,
                            style: { pointerEvents: "none" },
                            children: P(M.rawValue)
                          }
                        ) : null
                      ] }, `${l.name}-${w}`) : null
                    )
                  ] }, `${l.name}-${n}`);
                }
              );
            })(),
            wr && t.categories.map(
              /** @param {any} [_] @param {any} [categoryIndex] @returns {any} */
              (o, c) => {
                const l = Vt[c];
                return l ? x.horizontalBarMode ? /* @__PURE__ */ e.jsx(
                  "rect",
                  {
                    x: x.margin.left,
                    y: x.margin.top + c * x.step,
                    width: Z - x.margin.left - x.margin.right,
                    height: x.step,
                    fill: "transparent",
                    style: { cursor: "pointer" },
                    onClick: () => S(l)
                  },
                  `category-hit-horizontal-${c}`
                ) : /* @__PURE__ */ e.jsx(
                  "rect",
                  {
                    x: x.margin.left + c * x.step,
                    y: x.margin.top,
                    width: x.step,
                    height: L - x.margin.top - x.margin.bottom,
                    fill: "transparent",
                    style: { cursor: "pointer" },
                    onClick: () => S(l)
                  },
                  `category-hit-vertical-${c}`
                ) : null;
              }
            ),
            Ke ? null : x.horizontalBarMode ? x.hasHierarchicalBands ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              Zt.map(
                /** @param {any} [group] @returns {any} */
                (o) => {
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
                      children: pe(o.outerLabel, 18)
                    }
                  ) }, `outer-label-horizontal-${o.groupKey}`);
                }
              ),
              t.categoryBands.map(
                /** @param {any} [band] @param {any} [categoryIndex] @returns {any} */
                (o, c) => {
                  const l = x.margin.top + c * x.step + x.step / 2, u = o && o.innerLabel ? String(o.innerLabel) : "";
                  return u ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: x.margin.left - 10,
                      y: l + 4,
                      textAnchor: "end",
                      fontSize: "11",
                      fill: r.textSec,
                      children: pe(u, 18)
                    },
                    `inner-label-horizontal-${c}`
                  ) : null;
                }
              )
            ] }) : t.categories.map(
              /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
              (o, c) => {
                const l = x.margin.top + c * x.step + x.step / 2;
                return /* @__PURE__ */ e.jsx(
                  "text",
                  {
                    x: x.margin.left - 10,
                    y: l + 4,
                    textAnchor: "end",
                    fontSize: "11",
                    fill: r.textSec,
                    children: pe(o, 22)
                  },
                  `label-horizontal-${c}`
                );
              }
            ) : x.hasHierarchicalBands ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              Zt.map(
                /** @param {any} [group] @returns {any} */
                (o) => {
                  const c = x.margin.left + o.startIndex * x.step, l = x.margin.left + (o.endIndex + 1) * x.step, u = (c + l) / 2;
                  return /* @__PURE__ */ e.jsx("g", { children: /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: u,
                      y: L - x.margin.bottom + 68,
                      textAnchor: "middle",
                      fontSize: "11",
                      fontWeight: "700",
                      fill: r.text,
                      children: pe(o.outerLabel, 22)
                    }
                  ) }, `outer-label-${o.groupKey}`);
                }
              ),
              t.categoryBands.map(
                /** @param {any} [band] @param {any} [categoryIndex] @returns {any} */
                (o, c) => {
                  const l = x.margin.left + c * x.step + x.step / 2, u = o && o.innerLabel ? String(o.innerLabel) : "";
                  return /* @__PURE__ */ e.jsx("g", { children: u ? /* @__PURE__ */ e.jsx(
                    "text",
                    {
                      x: l,
                      y: L - x.margin.bottom + 30,
                      textAnchor: "middle",
                      fontSize: "11",
                      fill: r.textSec,
                      children: pe(u, 18)
                    }
                  ) : null }, `inner-label-${c}`);
                }
              )
            ] }) : t.categories.map(
              /** @param {any} [category] @param {any} [categoryIndex] @returns {any} */
              (o, c) => {
                if (Pt.visibleIndexes && !Pt.visibleIndexes.has(c)) return null;
                const l = x.margin.left + c * x.step + x.step / 2;
                return /* @__PURE__ */ e.jsx("g", { transform: `translate(${l}, ${L - x.margin.bottom + 18}) rotate(${Qe})`, children: /* @__PURE__ */ e.jsxs("text", { textAnchor: Qe === 0 ? "middle" : "start", fontSize: "11", fill: r.textSec, "data-chart-element-type": "axis", "data-chart-element-id": "x", style: ke ? { cursor: "pointer" } : void 0, children: [
                  /* @__PURE__ */ e.jsx("title", { children: String(o) }),
                  pe(o, Pt.maxChars)
                ] }) }, `label-${c}`);
              }
            )
          ]
        }
      ),
      Ne,
      Oe
    ] }),
    m ? /* @__PURE__ */ e.jsx(jt, { items: At, theme: r, hiddenSet: be, onToggle: Je, position: _e, onElementSelect: ke }) : null
  ] });
}, cs = ({ chartType: t, barLayout: n, onChange: i, canStack: a, theme: r }) => {
  const h = (m, C = !1) => ({
    border: `1px solid ${m === n ? r.primary : r.border}`,
    background: m === n ? r.select : r.headerSubtleBg || r.hover,
    color: m === n ? r.primary : r.text,
    borderRadius: r.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: C ? "not-allowed" : "pointer",
    opacity: C ? 0.45 : 1
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
}, ps = ({ available: t, active: n, onChange: i, theme: a }) => {
  const r = new Set(n || []), h = (t || []).every(
    /** @param {any} key */
    (S) => r.has(S)
  ), m = (S) => ({
    border: `1px solid ${S ? a.primary : a.border}`,
    background: S ? a.select : a.headerSubtleBg || a.hover,
    color: S ? a.primary : a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "5px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }), C = (S) => {
    const D = r.has(S) ? (n || []).filter(
      /** @param {any} entry */
      ($) => $ !== S
    ) : (t || []).filter(
      /** @param {any} entry */
      ($) => $ === S || r.has($)
    );
    D.length !== 0 && i(D.length === (t || []).length ? "all" : D);
  };
  return /* @__PURE__ */ e.jsxs("div", { role: "group", "aria-label": "Chart measures", style: { display: "flex", gap: "5px", flexWrap: "wrap" }, children: [
    /* @__PURE__ */ e.jsx("button", { type: "button", "aria-pressed": h, onClick: () => i("all"), style: m(h), children: "All" }),
    (t || []).map(
      /** @param {any} key */
      (S) => /* @__PURE__ */ e.jsx("button", { type: "button", "aria-pressed": r.has(S), onClick: () => C(S), style: m(r.has(S) && !h), children: S }, S)
    )
  ] });
}, fs = ({ title: t, subtitle: n, note: i, theme: a }) => !t && !n && !i ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "4px" }, children: [
  t ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: a.text }, children: t }) : null,
  n ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: n }) : null,
  i ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: i }) : null
] }), de = ({ label: t, theme: n, children: i, controlMinWidth: a = "180px" }) => /* @__PURE__ */ e.jsxs("div", { style: {
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
] }), hs = ({
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
  }, C = (S, D) => (
    /** @param {any} [event] @returns {any} */
    (($) => {
      S && S(Cn($.target.value, D));
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
          onChange: C(n, An),
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
          onChange: C(a, $n),
          style: h
        }
      )
    ] })
  ] });
}, gs = ({
  layers: t,
  onChange: n,
  availableColumns: i,
  theme: a
}) => {
  const r = Wr(i), h = jn(t, r), [m, C] = v.useState(""), S = v.useMemo(
    () => kn(r),
    [r]
  ), D = v.useMemo(
    () => new Map(S.map(
      /** @param {any} [option] @returns {any} */
      (z) => [z.id, z]
    )),
    [S]
  ), $ = m.trim().toLowerCase(), Y = v.useMemo(
    () => $ ? S.filter(
      /** @param {any} [option] @returns {any} */
      (z) => z.searchText.includes($)
    ) : S,
    [S, $]
  ), J = v.useMemo(() => {
    const z = /* @__PURE__ */ new Map();
    return Y.forEach(
      /** @param {any} [option] @returns {any} */
      (F) => {
        z.has(F.groupLabel) || z.set(F.groupLabel, []), z.get(F.groupLabel).push(F);
      }
    ), Array.from(z.entries()).map(([F, G]) => ({ label: F, options: G }));
  }, [Y]), K = {
    border: `1px solid ${a.border}`,
    background: a.surfaceBg || a.background || "#fff",
    color: a.text,
    borderRadius: a.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700
  }, s = (z, F) => {
    typeof n == "function" && n(h.map(
      /** @param {any} [layer] @returns {any} */
      (G) => G.id === z ? { ...G, ...F || {} } : G
    ));
  }, V = (z, F) => {
    if (typeof n != "function") return;
    const G = h.findIndex(
      /** @param {any} [layer] @returns {any} */
      (Ve) => Ve.id === z
    ), ce = G + F;
    if (G < 0 || ce < 0 || ce >= h.length) return;
    const ee = [...h], [ae] = ee.splice(G, 1);
    ee.splice(ce, 0, ae), n(ee);
  }, T = (z) => {
    if (typeof n != "function") return;
    const F = h.filter(
      /** @param {any} [layer] @returns {any} */
      (G) => G.id !== z
    );
    n(F.length > 0 ? F : wn(r, 1));
  }, xe = () => {
    if (typeof n != "function") return;
    const z = new Set(h.map(
      /** @param {any} [layer] @returns {any} */
      (ce) => ce.columnId
    )), F = r.find(
      /** @param {any} [column] @returns {any} */
      (ce) => !z.has(ce.id)
    ) || r[0];
    if (!F) return;
    const G = Sn({
      id: `layer-${h.length + 1}`,
      type: _r[h.length % _r.length],
      columnId: F.id,
      axis: h.length === 1 ? "right" : "left",
      name: Mn(F)
    }, r, h.length);
    n([...h, G]);
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
            onChange: (z) => C(z.target.value),
            placeholder: "Search measures, groups, or column ids",
            style: { ...K, width: "100%", maxWidth: "360px" }
          }
        ),
        /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", fontWeight: 700, color: a.textSec, whiteSpace: "nowrap" }, children: [
          Y.length,
          " / ",
          S.length
        ] })
      ] })
    ] }),
    Y.length === 0 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec, padding: "2px 2px 0 2px" }, children: "No measures match the current search. Clear the search to see the full catalog." }) : null,
    h.map(
      /** @param {any} [layer] @param {any} [layerIndex] @returns {any} */
      (z, F) => {
        const G = D.get(z.columnId) || null, ce = J.some(
          /** @param {any} [group] @returns {any} */
          (ee) => ee.options.some(
            /** @param {any} [option] @returns {any} */
            (ae) => ae.id === z.columnId
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
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: a.textSec }, children: G ? G.fullLabel : z.name || "Select a measure" }),
                  G ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: a.textSec }, children: `${G.groupLabel} | ${G.id}` }) : null
                ] }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => V(z.id, -1),
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
                      onClick: () => V(z.id, 1),
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
                      onClick: () => T(z.id),
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
                    tt,
                    {
                      size: "sm",
                      "aria-label": "Measure",
                      value: z.columnId || "",
                      onChange: (ee) => {
                        const ae = D.get(ee.target.value) || null;
                        s(z.id, {
                          columnId: ee.target.value,
                          name: ae ? ae.fullLabel : z.name
                        });
                      },
                      style: K,
                      children: [
                        !ce && G ? /* @__PURE__ */ e.jsx("optgroup", { label: "Current Layer Measure", children: /* @__PURE__ */ e.jsx("option", { value: G.id, children: G.fullLabel }) }) : null,
                        J.map(
                          /** @param {any} [group] @returns {any} */
                          (ee) => /* @__PURE__ */ e.jsx("optgroup", { label: `${ee.label} (${ee.options.length})`, children: ee.options.map(
                            /** @param {any} [option] @returns {any} */
                            (ae) => /* @__PURE__ */ e.jsx("option", { value: ae.id, children: ae.leafLabel }, ae.id)
                          ) }, ee.label)
                        )
                      ]
                    }
                  )
                ] }),
                /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px" }, children: [
                  /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.06em", textTransform: "uppercase", color: a.textSec }, children: "Layer Type" }),
                  /* @__PURE__ */ e.jsxs(
                    tt,
                    {
                      size: "sm",
                      "aria-label": "Layer Type",
                      value: z.type,
                      onChange: (ee) => s(z.id, { type: ee.target.value }),
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
                    tt,
                    {
                      size: "sm",
                      "aria-label": "Axis",
                      value: z.axis,
                      onChange: (ee) => s(z.id, { axis: ee.target.value }),
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
                    tt,
                    {
                      size: "sm",
                      "aria-label": "Format",
                      value: z.valueFormat || "auto",
                      onChange: (ee) => s(z.id, { valueFormat: ee.target.value }),
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
                      value: z.color || "#6366f1",
                      onChange: (ee) => s(z.id, { color: ee.target.value }),
                      style: { width: "100%", height: "34px", padding: "2px 4px", border: `1px solid ${a.border}`, borderRadius: a.radiusSm || "8px", cursor: "pointer", background: "none" },
                      title: "Override color for this layer"
                    }
                  )
                ] })
              ] })
            ]
          },
          z.id
        );
      }
    ),
    /* @__PURE__ */ e.jsx("div", { children: /* @__PURE__ */ e.jsx(
      "button",
      {
        type: "button",
        onClick: xe,
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
}, bs = ({ sortMode: t, onChange: n, theme: i }) => {
  const a = [
    { value: "natural", label: "Natural" },
    { value: "value_desc", label: "Value Desc" },
    { value: "value_asc", label: "Value Asc" },
    { value: "label_asc", label: "Label A-Z" },
    { value: "label_desc", label: "Label Z-A" }
  ];
  return /* @__PURE__ */ e.jsx(
    tt,
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
}, vs = ({ scope: t, onChange: n, theme: i }) => {
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
}, tr = "http://www.w3.org/2000/svg", Br = (t, n) => {
  if (!t || typeof document > "u") return null;
  const i = vr(n), a = t.cloneNode(!0), r = (a.getAttribute("viewBox") || "").split(/[\s,]+/).map(Number), h = r.length === 4 && r[2] > 0 ? r[2] : t.clientWidth || 800, m = r.length === 4 && r[3] > 0 ? r[3] : t.clientHeight || 400, C = i.title ? 42 : 0, S = qr(
    i.legendItems,
    h,
    /** @param {any} [label] @returns {any} */
    (T) => String(T || "").length * 6.4
  ), D = S.length > 0 ? S.length * 22 + 14 : 0, $ = i.legendPosition === "top" ? D : 0, Y = C + $, J = C + m + D, K = Array.from(a.childNodes), s = document.createElementNS(tr, "g");
  s.setAttribute("transform", `translate(0 ${Y})`), K.forEach(
    /** @param {any} [child] @returns {any} */
    (T) => s.appendChild(T)
  ), a.appendChild(s), a.setAttribute("viewBox", `0 0 ${h} ${J}`), a.setAttribute("width", String(h)), a.setAttribute("height", String(J)), a.setAttribute("xmlns", tr);
  const V = document.createElementNS(tr, "rect");
  if (V.setAttribute("x", "0"), V.setAttribute("y", "0"), V.setAttribute("width", String(h)), V.setAttribute("height", String(J)), V.setAttribute("fill", i.background), a.insertBefore(V, s), i.title) {
    const T = document.createElementNS(tr, "text");
    T.setAttribute("x", "16"), T.setAttribute("y", "27"), T.setAttribute("fill", i.textColor), T.setAttribute("font-size", "16"), T.setAttribute("font-weight", "700"), T.setAttribute("font-family", "Inter, Arial, sans-serif"), T.textContent = i.title, a.appendChild(T);
  }
  if (S.length > 0) {
    const T = i.legendPosition === "top" ? C + 16 : C + m + 18;
    S.forEach(
      /** @param {any} [row] @param {any} [rowIndex] @returns {any} */
      (xe, z) => {
        let F = 16;
        xe.forEach(
          /** @param {any} [item] @returns {any} */
          (G) => {
            const ce = T + z * 22, ee = document.createElementNS(tr, "circle");
            ee.setAttribute("cx", String(F + 5)), ee.setAttribute("cy", String(ce - 4)), ee.setAttribute("r", "5"), ee.setAttribute("fill", G.hidden ? i.secondaryTextColor : G.color || "#2563eb"), ee.setAttribute("opacity", G.hidden ? "0.35" : "1"), a.appendChild(ee);
            const ae = document.createElementNS(tr, "text");
            ae.setAttribute("x", String(F + 16)), ae.setAttribute("y", String(ce)), ae.setAttribute("fill", i.secondaryTextColor), ae.setAttribute("font-size", "11"), ae.setAttribute("font-family", "Inter, Arial, sans-serif"), ae.setAttribute("opacity", G.hidden ? "0.45" : "1"), ae.textContent = G.label, a.appendChild(ae), F += G.width;
          }
        );
      }
    );
  }
  return a;
}, ms = async (t, n, i) => {
  if (!t || typeof window > "u") return;
  const a = Br(t, i);
  if (!a) return;
  const h = new XMLSerializer().serializeToString(a), m = new Blob([h], { type: "image/svg+xml;charset=utf-8" });
  yr(m, `${n}.svg`);
}, Qr = (t, n = 2) => new Promise((i) => {
  if (!t) {
    i(null);
    return;
  }
  const r = new XMLSerializer().serializeToString(t), h = t.getAttribute("viewBox");
  let m = t.clientWidth || 800, C = t.clientHeight || 400;
  if (h) {
    const K = h.split(/[\s,]+/).map(Number);
    K.length === 4 && K[2] > 0 && K[3] > 0 && (m = K[2], C = K[3]);
  }
  const S = document.createElement("canvas");
  S.width = m * n, S.height = C * n;
  const D = S.getContext("2d");
  D.fillStyle = "#ffffff", D.fillRect(0, 0, S.width, S.height);
  const $ = new Image(), Y = new Blob([r], { type: "image/svg+xml;charset=utf-8" }), J = URL.createObjectURL(Y);
  $.onload = () => {
    D.drawImage($, 0, 0, S.width, S.height), URL.revokeObjectURL(J), i(S);
  }, $.onerror = () => {
    URL.revokeObjectURL(J), i(null);
  }, $.src = J;
}), js = async (t, n, i) => {
  const a = await Qr(Br(t, i));
  a && a.toBlob(
    /** @param {any} [blob] @returns {any} */
    (r) => {
      r && yr(r, `${n}.png`);
    },
    "image/png"
  );
}, ks = async (t, n) => {
  if (typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  const i = await Qr(Br(t, n));
  if (!i) return !1;
  try {
    const a = await new Promise((r) => i.toBlob(r, "image/png"));
    return a ? (await navigator.clipboard.write([new ClipboardItem({ "image/png": a })]), !0) : !1;
  } catch {
    return !1;
  }
}, xt = /* @__PURE__ */ new Set(["bar3d", "line3d", "scatter3d", "range", "boxplot", "nightingale"]), ws = (t) => new Promise((n) => {
  const i = new Image();
  i.onload = () => n(i), i.onerror = () => n(null), i.src = t;
}), Jr = async (t, n) => {
  const i = t && t.current && t.current.getEchartsInstance();
  if (!i) return null;
  const a = vr(n), r = i.getDataURL({ type: "png", pixelRatio: 2 }), h = await ws(r);
  if (!h) return null;
  const m = Math.max(1, h.naturalWidth / Math.max(1, i.getWidth())), C = a.title ? Math.round(42 * m) : 0, D = document.createElement("canvas").getContext("2d");
  D.font = "11px Inter, Arial, sans-serif";
  const $ = qr(
    a.legendItems,
    i.getWidth(),
    /** @param {any} [label] @returns {any} */
    (T) => D.measureText(String(T || "")).width
  ), Y = $.length > 0 ? Math.round(($.length * 22 + 14) * m) : 0, J = a.legendPosition === "top" ? Y : 0, K = C + J, s = document.createElement("canvas");
  s.width = h.naturalWidth, s.height = C + h.naturalHeight + Y;
  const V = s.getContext("2d");
  if (V.fillStyle = a.background, V.fillRect(0, 0, s.width, s.height), V.drawImage(h, 0, K), a.title && (V.fillStyle = a.textColor, V.font = `700 ${Math.round(16 * m)}px Inter, Arial, sans-serif`, V.textBaseline = "alphabetic", V.fillText(a.title, Math.round(16 * m), Math.round(27 * m))), $.length > 0) {
    const T = a.legendPosition === "top" ? C + Math.round(16 * m) : C + h.naturalHeight + Math.round(18 * m);
    V.font = `${Math.round(11 * m)}px Inter, Arial, sans-serif`, $.forEach(
      /** @param {any} [row] @param {any} [rowIndex] @returns {any} */
      (xe, z) => {
        let F = Math.round(16 * m);
        xe.forEach(
          /** @param {any} [item] @returns {any} */
          (G) => {
            const ce = T + Math.round(z * 22 * m);
            V.globalAlpha = G.hidden ? 0.35 : 1, V.fillStyle = G.hidden ? a.secondaryTextColor : G.color || "#2563eb", V.beginPath(), V.arc(F + Math.round(5 * m), ce - Math.round(4 * m), Math.round(5 * m), 0, Math.PI * 2), V.fill(), V.globalAlpha = G.hidden ? 0.45 : 1, V.fillStyle = a.secondaryTextColor, V.fillText(G.label, F + Math.round(16 * m), ce), V.globalAlpha = 1, F += Math.round(G.width * m);
          }
        );
      }
    );
  }
  return s;
}, Ss = async (t, n, i) => {
  const a = await Jr(t, i);
  a && a.toBlob(
    /** @param {any} [blob] @returns {any} */
    (r) => {
      r && yr(r, `${n}.png`);
    },
    "image/png"
  );
}, Ms = async (t, n) => {
  if (!(t && t.current && t.current.getEchartsInstance()) || typeof navigator > "u" || !navigator.clipboard || !navigator.clipboard.write) return !1;
  try {
    const a = await Jr(t, n);
    if (!a) return !1;
    const r = await new Promise((h) => a.toBlob(h, "image/png"));
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
  barLayout: h,
  axisMode: m,
  onChartTypeChange: C,
  onBarLayoutChange: S,
  onAxisModeChange: D,
  orientation: $,
  onOrientationChange: Y,
  hierarchyLevel: J,
  onHierarchyLevelChange: K,
  theme: s,
  title: V,
  onTitleChange: T,
  subtitle: xe,
  note: z,
  chartHeight: F,
  onChartHeightChange: G,
  rowLimit: ce,
  onRowLimitChange: ee,
  columnLimit: ae,
  onColumnLimitChange: Ve,
  sortMode: fe,
  onSortModeChange: We,
  onMeasureIdsChange: Ee,
  interactionMode: ft,
  onInteractionModeChange: rt,
  serverScope: ht,
  onServerScopeChange: Ze,
  showServerScope: _e = !1,
  floating: qe = !1,
  immersiveMode: Xe = !1,
  onCategoryActivate: nr,
  allowHierarchyCharts: sr = !0,
  locked: ot = !1,
  onToggleLock: Xt,
  configOpen: zt,
  onConfigChange: Lt,
  settingsPanelWidth: gt,
  onSettingsPanelWidthChange: Ft,
  initialSettings: B = {},
  onSettingsChange: ke
}) => {
  const le = n === "combo", P = n === "sparkline", Ke = le ? !1 : n === "area" ? Zr(t) : Ur(t), it = V !== void 0 ? V : t && t.title, Bt = xe !== void 0 ? xe : t && t.subtitle, dt = z !== void 0 ? z : ot ? qe ? "Locked to the current chart request and floating frame" : "Locked to the current chart request" : t && t.note, [ze, Qe] = v.useState(!1), L = typeof zt == "boolean" ? zt : ze, Te = typeof Lt == "function" ? Lt : Qe, [ye, Ot] = v.useState(() => (B == null ? void 0 : B.showDataLabels) ?? !1), [He, Le] = v.useState(() => (B == null ? void 0 : B.colorPalette) ?? "default"), [at, Fe] = v.useState(() => (B == null ? void 0 : B.valueFormat) ?? "auto"), [kt, or] = v.useState(() => (B == null ? void 0 : B.yAxisTitle) ?? ""), [Z, be] = v.useState(() => (B == null ? void 0 : B.yAxisMin) ?? ""), [ve, Je] = v.useState(() => (B == null ? void 0 : B.yAxisMax) ?? ""), [Ae, bt] = v.useState(() => (B == null ? void 0 : B.referenceLines) ?? []), [Re, wt] = v.useState({ value: "", label: "", orient: "h" }), [lt, Oe] = v.useState(() => (B == null ? void 0 : B.labelAngle) ?? 0), [De, Yt] = v.useState(() => (B == null ? void 0 : B.chartSubtitle) ?? ""), [ct, Et] = v.useState(() => (B == null ? void 0 : B.tickCount) ?? 5), [me, St] = v.useState(() => (B == null ? void 0 : B.markerShape) ?? "circle"), [yt, Tt] = v.useState(() => (B == null ? void 0 : B.markerSize) ?? 4), [Ie, Mt] = v.useState(() => (B == null ? void 0 : B.showGradient) ?? !1), [Ye, nt] = v.useState(() => (B == null ? void 0 : B.seriesColors) ?? {}), [ut, ir] = v.useState(() => (B == null ? void 0 : B.showAnimations) ?? !0), [je, ar] = v.useState(() => (B == null ? void 0 : B.showCalloutLabels) ?? !1), [U, et] = v.useState(() => (B == null ? void 0 : B.showTrendline) ?? !1), [Se, Be] = v.useState(() => (B == null ? void 0 : B.trendlineMode) ?? "linear"), [Ge, lr] = v.useState(() => (B == null ? void 0 : B.trendlinePeriod) ?? 3), [vt, dr] = v.useState(() => (B == null ? void 0 : B.legendPosition) ?? "bottom"), [Rt, Gt] = v.useState(() => (B == null ? void 0 : B.labelPosition) ?? "outside"), [Ut, Kt] = v.useState(() => (B == null ? void 0 : B.decimalPlaces) ?? -1), [Dt, mr] = v.useState(() => (B == null ? void 0 : B.gradientDirection) ?? "vertical"), [Nt, jr] = v.useState(() => (B == null ? void 0 : B.gradientOpacity) ?? 70), [Ne, x] = v.useState(() => (B == null ? void 0 : B.trendlineColor) ?? ""), [Pt, kr] = v.useState(() => (B == null ? void 0 : B.trendlineWidth) ?? 2), [Ct, ur] = v.useState(() => (B == null ? void 0 : B.seriesSort) ?? "none"), [At, Zt] = v.useState(() => (B == null ? void 0 : B.customPaletteColors) ?? []), [Vt, wr] = v.useState(() => (B == null ? void 0 : B.lineDashStyles) ?? {}), [o, c] = v.useState(() => (B == null ? void 0 : B.yAxisMinRight) ?? ""), [l, u] = v.useState(() => (B == null ? void 0 : B.yAxisMaxRight) ?? ""), [p, g] = v.useState("chart"), [k, f] = v.useState(!0), [M, w] = v.useState(!0), b = v.useCallback((d) => {
    const he = { series: "style", axis: "axes", legend: "labels", title: "data" }[d];
    he && (Te(!0), g(he));
  }, [Te]), H = Gr(He), [_, A] = v.useState(String(F || rr)), j = v.useRef(null), E = v.useRef(null), N = v.useRef(null), [O, X] = v.useState(null), [oe, y] = v.useState(!1), { anchorRef: q, menuRef: ne, position: W, portalTarget: R } = zn(oe, y), [ie, se] = v.useState(null), [re, I] = v.useState(348), te = Number.isFinite(Number(gt)) ? Math.max(300, Math.min(680, Math.floor(Number(gt)))) : re, ue = typeof Ft == "function" ? Ft : I, Pe = !Xe, mt = Xe && ie ? Math.max(180, ie - (Xe && !P && vt !== "none" ? 44 : 0) - 44) : Math.max(180, Number.isFinite(Number(F)) ? Number(F) : rr), Qt = v.useRef(null), It = v.useRef(null), Sr = v.useRef(null), Er = t && t.maxHierarchyLevel || 1, xr = typeof K == "function" && Er > 1, Rr = {
    width: "32px",
    height: "32px",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    border: `1px solid ${L ? s.primary : s.border}`,
    background: L ? s.select : s.headerSubtleBg || s.hover,
    color: L ? s.primary : s.textSec,
    borderRadius: s.radiusSm || "8px",
    cursor: "pointer",
    flexShrink: 0
  }, tn = {
    ...Rr,
    border: `1px solid ${ot ? s.primary : s.border}`,
    background: ot ? s.select : s.headerSubtleBg || s.hover,
    color: ot ? s.primary : s.textSec
  }, _t = {
    border: `1px solid ${s.border}`,
    background: s.headerSubtleBg || s.hover,
    color: s.textSec,
    borderRadius: s.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }, Me = {
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
      C("line"), typeof Y == "function" && Y("rows"), typeof We == "function" && We("natural");
      return;
    }
    if (d === "compare") {
      C("bar"), typeof S == "function" && S("grouped"), typeof D == "function" && D("vertical");
      return;
    }
    if (d === "composition") {
      C(xr ? "sunburst" : "donut");
      return;
    }
    if (d === "flow") {
      C("waterfall");
      return;
    }
    d === "sparkline" && C("sparkline");
  }, [
    D,
    S,
    C,
    Y,
    We,
    xr
  ]);
  v.useEffect(() => {
    !Xe || !L || Te(!1);
  }, [Xe, L]), v.useEffect(() => {
    if (!O) return;
    const d = setTimeout(() => X(null), 2500);
    return () => clearTimeout(d);
  }, [O]);
  const pr = v.useCallback(() => Jn(
    Sr.current,
    it,
    vt,
    s
  ), [vt, it, s]), nn = v.useCallback(async (d) => {
    y(!1);
    const Q = pr();
    if (d === "svg") {
      xt.has(n) || await ms(Qt.current, "pivot-chart", Q);
      return;
    }
    if (d === "png") {
      xt.has(n) ? await Ss(It, "pivot-chart", Q) : await js(Qt.current, "pivot-chart", Q);
      return;
    }
    d === "csv" && es(t, "pivot-chart", Q);
  }, [n, pr, t]), sn = v.useCallback(async () => {
    const d = pr(), Q = xt.has(n) ? await Ms(It, d) : await ks(Qt.current, d);
    X(Q ? { tone: "success", text: "Copied chart" } : { tone: "error", text: "Copy failed" });
  }, [n, pr]);
  v.useEffect(() => {
    if (!Xe) {
      se(null);
      return;
    }
    const d = N.current;
    if (!d) return;
    const Q = () => {
      const ge = d.clientHeight;
      ge > 0 && se(ge);
    };
    if (Q(), typeof ResizeObserver > "u") return;
    const he = new ResizeObserver(Q);
    return he.observe(d), () => he.disconnect();
  }, [Xe]);
  const Mr = v.useRef(G);
  v.useEffect(() => {
    Mr.current = G;
  }), v.useEffect(() => {
    const d = (he) => {
      if (!j.current || typeof Mr.current != "function") return;
      const ge = j.current, Ue = Math.max(180, ge.startHeight + (he.clientY - ge.startY));
      Mr.current(Ue);
    }, Q = () => {
      j.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", Q), window.addEventListener("blur", Q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", Q), window.removeEventListener("blur", Q);
    };
  }, []), v.useEffect(() => {
    const d = (he) => {
      if (!E.current) return;
      const ge = E.current, Ue = ge.direction === "left" ? ge.startX - he.clientX : he.clientX - ge.startX, Jt = Math.max(300, Math.min(680, ge.startWidth + Ue));
      ue(Jt);
    }, Q = () => {
      E.current = null;
    };
    return window.addEventListener("mousemove", d), window.addEventListener("mouseup", Q), window.addEventListener("blur", Q), () => {
      window.removeEventListener("mousemove", d), window.removeEventListener("mouseup", Q), window.removeEventListener("blur", Q);
    };
  }, []), v.useEffect(() => {
    const d = Sr.current;
    if (!d || typeof ResizeObserver > "u") return;
    let Q = d.clientWidth;
    const he = new ResizeObserver(() => {
      var Ue, Jt;
      const ge = d.clientWidth;
      if (ge !== Q) {
        Q = ge;
        const Nr = (Jt = (Ue = It.current) == null ? void 0 : Ue.getEchartsInstance) == null ? void 0 : Jt.call(Ue);
        Nr && Nr.resize();
      }
    });
    return he.observe(d), () => he.disconnect();
  }, []), v.useEffect(() => {
    A(String(mt));
  }, [mt]);
  const Cr = v.useRef(ke);
  v.useEffect(() => {
    Cr.current = ke;
  });
  const Dr = v.useRef(!0);
  v.useEffect(() => {
    if (Dr.current) {
      Dr.current = !1;
      return;
    }
    typeof Cr.current == "function" && Cr.current({
      showDataLabels: ye,
      colorPalette: He,
      valueFormat: at,
      yAxisTitle: kt,
      yAxisMin: Z,
      yAxisMax: ve,
      referenceLines: Ae,
      labelAngle: lt,
      chartSubtitle: De,
      tickCount: ct,
      markerShape: me,
      markerSize: yt,
      showGradient: Ie,
      seriesColors: Ye,
      showAnimations: ut,
      showCalloutLabels: je,
      showTrendline: U,
      trendlineMode: Se,
      trendlinePeriod: Ge,
      legendPosition: vt,
      labelPosition: Rt,
      decimalPlaces: Ut,
      gradientDirection: Dt,
      gradientOpacity: Nt,
      trendlineColor: Ne,
      trendlineWidth: Pt,
      seriesSort: Ct,
      customPaletteColors: At,
      lineDashStyles: Vt,
      yAxisMinRight: o,
      yAxisMaxRight: l
    });
  }, [
    ye,
    He,
    at,
    kt,
    Z,
    ve,
    Ae,
    lt,
    De,
    ct,
    me,
    yt,
    Ie,
    Ye,
    ut,
    je,
    U,
    Se,
    Ge,
    vt,
    Rt,
    Ut,
    Dt,
    Nt,
    Ne,
    Pt,
    Ct,
    At,
    Vt,
    o,
    l
  ]);
  const Ar = v.useMemo(() => {
    if (!t || !Array.isArray(t.series)) return null;
    let d = 1 / 0, Q = -1 / 0;
    return t.series.forEach(
      /** @param {any} [ser] @returns {any} */
      (he) => {
        (he.values || []).forEach(
          /** @param {any} [v] @returns {any} */
          (ge) => {
            typeof ge == "number" && Number.isFinite(ge) && (d = Math.min(d, ge), Q = Math.max(Q, ge));
          }
        );
      }
    ), Number.isFinite(d) && d !== Q ? { min: d, max: Q } : null;
  }, [t]);
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      ref: N,
      style: {
        display: "flex",
        flexDirection: "column",
        gap: Pe ? "14px" : "10px",
        flex: "1 1 auto",
        minHeight: 0,
        overflow: "hidden"
      },
      children: [
        Pe || Xe ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
          /* @__PURE__ */ e.jsx(
            fs,
            {
              title: Xe ? it : null,
              subtitle: Bt,
              note: dt,
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
                  onClick: () => y(
                    /** @param {any} [isOpen] @returns {any} */
                    (d) => !d
                  ),
                  onKeyDown: (d) => {
                    (d.key === "ArrowDown" || d.key === "ArrowUp") && (d.preventDefault(), y(!0));
                  },
                  style: _t,
                  title: "Export chart",
                  "aria-haspopup": "menu",
                  "aria-expanded": oe,
                  children: /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "5px" }, children: [
                    "Export ",
                    /* @__PURE__ */ e.jsx(st.ChevronDown, {})
                  ] })
                }
              ),
              oe && R ? fn.createPortal(
                /* @__PURE__ */ e.jsx(
                  "div",
                  {
                    ref: ne,
                    "data-pivot-workspace-root": "true",
                    className: "sp-menu-surface",
                    role: "menu",
                    "aria-label": "Chart export formats",
                    style: {
                      position: "fixed",
                      ...W,
                      visibility: W ? "visible" : "hidden",
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
                      { format: "svg", label: "SVG image", hint: "Title + legend included", disabled: xt.has(n) },
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
                R
              ) : null
            ] }),
            /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: sn,
                style: _t,
                title: "Copy chart with title and legend",
                children: "Copy"
              }
            ),
            O ? /* @__PURE__ */ e.jsx(
              "span",
              {
                role: "status",
                style: {
                  fontSize: "10px",
                  fontWeight: 800,
                  color: O.tone === "success" ? s.primary || "#047857" : "#B91C1C",
                  border: `1px solid ${O.tone === "success" ? `${s.primary || "#047857"}55` : "#FCA5A5"}`,
                  borderRadius: "999px",
                  padding: "3px 7px",
                  background: O.tone === "success" ? `${s.primary || "#047857"}10` : "#FEF2F2"
                },
                children: O.text
              }
            ) : null,
            typeof Xt == "function" ? /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                onClick: Xt,
                style: { ...tn, width: "auto", padding: "0 10px", gap: "6px", fontSize: "11px", fontWeight: 800 },
                title: ot ? "Data is fixed to the saved selection. Click to resume live pivot updates." : "Keep this chart fixed to the current data selection while the pivot changes.",
                "aria-label": ot ? "Unlock chart data and resume live updates" : "Lock chart to the current data selection",
                "aria-pressed": ot,
                children: [
                  /* @__PURE__ */ e.jsx(st.Lock, {}),
                  /* @__PURE__ */ e.jsx("span", { children: ot ? "Data locked" : "Lock data" })
                ]
              }
            ) : null,
            Pe ? /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onMouseDown: (d) => d.stopPropagation(),
                onClick: (d) => {
                  d.stopPropagation(), Te((Q) => !Q);
                },
                "data-chart-settings-toggle": "true",
                style: Rr,
                title: L ? "Hide chart settings" : "Show chart settings",
                "aria-label": L ? "Hide chart settings" : "Show chart settings",
                children: /* @__PURE__ */ e.jsx(st.Settings, {})
              }
            ) : null
          ] })
        ] }) : null,
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", minHeight: 0, gap: 0, overflow: "hidden", flex: "1 1 auto" }, children: [
          /* @__PURE__ */ e.jsxs(
            "div",
            {
              ref: Sr,
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
                paddingRight: L ? "4px" : "0"
              },
              children: [
                t ? (!Array.isArray(t.series) || t.series.length === 0) && n !== "icicle" && n !== "sunburst" && n !== "sankey" ? /* @__PURE__ */ e.jsx(we, { message: t.emptyMessage || "No chart data available.", theme: s, chartHeight: mt }) : /* @__PURE__ */ e.jsx(ls, { model: t, chartType: n, barLayout: h, axisMode: m, theme: s, chartHeight: mt, showLegend: !P, showDataLabels: ye, onCategoryActivate: nr, svgRef: Qt, echartsRef: It, colorPalette: He, valueFormat: at, yAxisTitle: kt, yAxisMin: Z, yAxisMax: ve, referenceLines: Ae, onReferenceLinesChange: bt, labelAngle: lt, chartTitle: Xe ? "" : it, onTitleChange: T, subtitle: De, tickCount: ct, markerShape: me, markerSize: yt, showGradient: Ie, seriesColors: Ye, showAnimations: ut, showCalloutLabels: je, showTrendline: U, trendlineMode: Se, trendlinePeriod: Ge, legendPosition: vt, labelPosition: Rt, decimalPlaces: Ut, gradientDirection: Dt, gradientOpacity: Nt, trendlineColor: Ne, trendlineWidth: Pt, seriesSort: Ct, lineDashStyles: Vt, yAxisMinRight: o, yAxisMaxRight: l, customPaletteColors: At, onElementSelect: b }) : /* @__PURE__ */ e.jsx(we, { message: "No chart data available.", theme: s, chartHeight: mt }),
                De ? /* @__PURE__ */ e.jsx("div", { style: { textAlign: "center", fontSize: "11px", color: s.textSec, padding: "4px 8px 0", fontStyle: "italic" }, children: De }) : null,
                !Xe && typeof G == "function" ? /* @__PURE__ */ e.jsx(
                  "div",
                  {
                    onMouseDown: (d) => {
                      d.preventDefault(), j.current = {
                        startY: d.clientY,
                        startHeight: mt
                      };
                    },
                    role: "separator",
                    "aria-orientation": "horizontal",
                    "aria-label": "Resize chart height",
                    title: "Drag to resize the chart height",
                    style: {
                      height: "12px",
                      cursor: "ns-resize",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      marginTop: "2px"
                    },
                    children: /* @__PURE__ */ e.jsx("div", { style: { width: "40px", height: "4px", borderRadius: "999px", background: s.border, opacity: 0.95 } })
                  }
                ) : null
              ]
            }
          ),
          Pe && L ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
            /* @__PURE__ */ e.jsx(
              "div",
              {
                "data-chart-settings-resizer": "true",
                onMouseDown: (d) => {
                  d.preventDefault(), E.current = {
                    startX: d.clientX,
                    startWidth: te,
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
                  width: `${te}px`,
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
                        onClick: () => Te(!1),
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
                        children: /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(st.Close, {}) })
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
                            /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", color: s.textSec, transform: k ? "scale(0.85) rotate(0deg)" : "scale(0.85) rotate(-90deg)", transition: "transform 0.15s" }, children: /* @__PURE__ */ e.jsx(st.ChevronDown, {}) })
                          ] }),
                          k && /* @__PURE__ */ e.jsx(Kn, { chartType: n, onChange: C, theme: s, includeHierarchyCharts: sr }),
                          /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => w(
                            /** @param {any} [o] @returns {any} */
                            (d) => !d
                          ), style: { display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", borderBottom: `1px solid ${s.border}`, padding: "4px 0", cursor: "pointer", marginTop: "8px", marginBottom: M ? "6px" : 0 }, children: [
                            /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec }, children: "Parameters" }),
                            /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center", color: s.textSec, transform: M ? "scale(0.85) rotate(0deg)" : "scale(0.85) rotate(-90deg)", transition: "transform 0.15s" }, children: /* @__PURE__ */ e.jsx(st.ChevronDown, {}) })
                          ] }),
                          M && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !xt.has(n) && !le ? /* @__PURE__ */ e.jsx(de, { label: "Source", theme: s, children: /* @__PURE__ */ e.jsx(ds, { orientation: $, onChange: Y, theme: s }) }) : null,
                            n === "bar" ? /* @__PURE__ */ e.jsx(de, { label: "Direction", theme: s, children: /* @__PURE__ */ e.jsx(us, { axisMode: m, onChange: D, theme: s }) }) : null,
                            (n === "bar" || n === "area") && !le && /* @__PURE__ */ e.jsx(de, { label: "Layout", theme: s, children: /* @__PURE__ */ e.jsx(cs, { chartType: n, barLayout: h, onChange: S, canStack: Ke, theme: s }) }),
                            xr ? /* @__PURE__ */ e.jsx(de, { label: "Level", theme: s, children: /* @__PURE__ */ e.jsx(xs, { level: J, onChange: K, maxLevel: Er, theme: s }) }) : null,
                            le ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Layers" }),
                              /* @__PURE__ */ e.jsx(gs, { layers: i, onChange: a, availableColumns: r, theme: s })
                            ] }) : null,
                            !(n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "scatter" && n !== "waterfall" && !xt.has(n) && !le) && n !== "bar" && !((n === "bar" || n === "area") && !le) && !xr && !le ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: s.textSec, padding: "4px 0" }, children: "No parameters for this chart type." }) : null
                          ] })
                        ] }),
                        p === "data" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "General" }),
                          typeof T == "function" ? /* @__PURE__ */ e.jsx(de, { label: "Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: it || "", onChange: (d) => T(d.target.value), placeholder: "Chart title", style: { ...Me, width: "100%" } }) }) : null,
                          /* @__PURE__ */ e.jsx(de, { label: "Subtitle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: De, onChange: (d) => Yt(d.target.value), placeholder: "Chart subtitle", style: { ...Me, width: "100%" } }) }),
                          typeof G == "function" ? /* @__PURE__ */ e.jsx(de, { label: "Height", theme: s, controlMinWidth: "96px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "180", step: "20", value: _, onChange: (d) => A(d.target.value), onBlur: () => {
                            typeof G == "function" && G(_);
                          }, onKeyDown: (d) => {
                            d.key === "Enter" && typeof G == "function" && G(_);
                          }, style: Me }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Data & Actions" }),
                          typeof Ee == "function" && t && Array.isArray(t.availableMeasures) && t.availableMeasures.length > 1 ? /* @__PURE__ */ e.jsx(de, { label: "Measures", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(
                            ps,
                            {
                              available: t.availableMeasures,
                              active: Array.isArray(t.activeMeasures) ? t.activeMeasures : t.availableMeasures,
                              onChange: Ee,
                              theme: s
                            }
                          ) }) : null,
                          /* @__PURE__ */ e.jsx(de, { label: "Limits", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(hs, { rowLimit: ce, onRowLimitChange: ee, columnLimit: ae, onColumnLimitChange: Ve, theme: s }) }),
                          typeof We == "function" ? /* @__PURE__ */ e.jsx(de, { label: "Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(bs, { sortMode: fe, onChange: We, theme: s }) }) : null,
                          typeof We == "function" ? /* @__PURE__ */ e.jsx(de, { label: "Series Sort", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px", flexWrap: "wrap" }, children: Rn.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ur(d.value), style: { border: `1px solid ${Ct === d.value ? s.primary : s.border}`, background: Ct === d.value ? s.select : s.headerSubtleBg || s.hover, color: Ct === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }) : null,
                          typeof rt == "function" ? /* @__PURE__ */ e.jsx(de, { label: "Click", theme: s, children: /* @__PURE__ */ e.jsx(ys, { interactionMode: ft, onChange: rt, theme: s }) }) : null,
                          _e && typeof Ze == "function" ? /* @__PURE__ */ e.jsx(de, { label: "Scope", theme: s, children: /* @__PURE__ */ e.jsx(vs, { scope: ht, onChange: Ze, theme: s }) }) : null,
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Presets" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "6px" }, children: [
                            { key: "trend", label: "Trend" },
                            { key: "compare", label: "Compare" },
                            { key: "composition", label: "Composition" },
                            { key: "flow", label: "Flow" },
                            { key: "sparkline", label: "Sparkline" }
                          ].map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", "data-chart-preset": d.key, onClick: () => rn(d.key), style: { ..._t, textAlign: "left" }, children: d.label }, d.key)) })
                        ] }),
                        p === "axes" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(de, { label: "Y-Axis Title", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("input", { type: "text", value: kt, onChange: (d) => or(d.target.value), placeholder: "Y-axis title", style: { ...Me, width: "100%" } }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !xt.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(de, { label: "Y Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: Z, onChange: (d) => be(d.target.value), placeholder: "Min", style: { ...Me, width: "50%" } }),
                              /* @__PURE__ */ e.jsx("input", { type: "number", value: ve, onChange: (d) => Je(d.target.value), placeholder: "Max", style: { ...Me, width: "50%" } })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(de, { label: "Y Ticks", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "20", value: ct, onChange: (d) => Et(Math.max(2, Math.min(20, Number(d.target.value) || 5))), style: { ...Me, width: "80px" } }) }),
                            /* @__PURE__ */ e.jsx(de, { label: "X Angle", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center", flexWrap: "wrap" }, children: [
                              /* @__PURE__ */ e.jsx("input", { type: "number", min: "0", max: "90", value: lt, onChange: (d) => Oe(Math.max(0, Math.min(90, Number(d.target.value) || 0))), style: { ...Me, width: "58px" }, title: "X-axis label angle (0–90°)" }),
                              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.textSec }, children: "°" }),
                              [0, 30, 45, 90].map((d) => /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => Oe(d), style: { ...Me, padding: "4px 6px", background: lt === d ? s.select : s.headerSubtleBg || s.hover, color: lt === d ? s.primary : s.text, border: `1px solid ${lt === d ? s.primary : s.border}` }, children: [
                                d,
                                "°"
                              ] }, d))
                            ] }) })
                          ] }) : null,
                          le ? /* @__PURE__ */ e.jsx(de, { label: "Right Axis Range", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: o, onChange: (d) => c(d.target.value), placeholder: "Min", style: { ...Me, width: "50%" } }),
                            /* @__PURE__ */ e.jsx("input", { type: "number", value: l, onChange: (d) => u(d.target.value), placeholder: "Max", style: { ...Me, width: "50%" } })
                          ] }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && !xt.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Reference Lines" }),
                            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: [
                              Ae.map(
                                /** @param {any} [line] @param {any} [lineIdx] @returns {any} */
                                (d, Q) => /* @__PURE__ */ e.jsx(Zn, { line: d, onUpdate: (
                                  /** @param {any} [updated] @returns {any} */
                                  ((he) => bt(
                                    /** @param {any} [prev] @returns {any} */
                                    (ge) => ge.map(
                                      /** @param {any} [l] @param {any} [j] @returns {any} */
                                      (Ue, Jt) => Jt === Q ? he : Ue
                                    )
                                  ))
                                ), onRemove: () => bt(
                                  /** @param {any} [prev] @returns {any} */
                                  (he) => he.filter(
                                    /** @param {any} [_] @param {any} [j] @returns {any} */
                                    (ge, Ue) => Ue !== Q
                                  )
                                ), exportButtonStyle: _t, compactInputStyle: Me, theme: s }, d.id)
                              ),
                              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => wt(
                                  /** @param {any} [d] @returns {any} */
                                  (d) => ({ ...d, orient: d.orient === "v" ? "h" : "v" })
                                ), style: { ..._t, padding: "4px 7px", flexShrink: 0, fontWeight: 800, color: Re.orient === "v" ? s.primary : s.textSec }, children: Re.orient === "v" ? "|" : "—" }),
                                /* @__PURE__ */ e.jsx("input", { type: "number", value: Re.value, onChange: (d) => wt(
                                  /** @param {any} [d] @returns {any} */
                                  (Q) => ({ ...Q, value: d.target.value })
                                ), placeholder: Re.orient === "v" ? "Cat index" : "Value", style: { ...Me, width: "60px", flexShrink: 0 } }),
                                /* @__PURE__ */ e.jsx("input", { type: "text", value: Re.label, onChange: (d) => wt(
                                  /** @param {any} [d] @returns {any} */
                                  (Q) => ({ ...Q, label: d.target.value })
                                ), placeholder: "Label", style: { ...Me, flex: 1, minWidth: 0 } }),
                                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => {
                                  Re.value !== "" && (bt(
                                    /** @param {any} [prev] @returns {any} */
                                    (d) => [...d, { id: Date.now(), value: Re.value, label: Re.label, orient: Re.orient, color: "", dash: "dashed" }]
                                  ), wt({ value: "", label: "", orient: Re.orient }));
                                }, style: { ..._t, padding: "4px 10px", flexShrink: 0 }, children: "+" })
                              ] }),
                              Ar ? /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: s.textSec }, children: [
                                "Data range: ",
                                Wt(Ar.min, at),
                                " — ",
                                Wt(Ar.max, at)
                              ] }) : null
                            ] })
                          ] }) : null
                        ] }),
                        p === "style" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}` }, children: "Colors" }),
                          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" }, children: [...hn, ...At.length >= 2 ? ["custom"] : []].map((d) => {
                            const Q = He === d, he = d === "custom" ? At : gn[d];
                            return /* @__PURE__ */ e.jsxs(
                              "button",
                              {
                                type: "button",
                                onClick: () => Le(d),
                                title: d.charAt(0).toUpperCase() + d.slice(1),
                                style: { display: "flex", alignItems: "center", gap: "3px", padding: "3px 6px", border: `1.5px solid ${Q ? s.primary : s.border}`, background: Q ? s.select : s.headerSubtleBg || s.hover, borderRadius: s.radiusSm || "8px", cursor: "pointer" },
                                children: [
                                  (he || []).slice(0, 4).map(
                                    /** @param {any} [hex] @param {any} [i] @returns {any} */
                                    (ge, Ue) => /* @__PURE__ */ e.jsx("span", { style: { width: "8px", height: "8px", borderRadius: "50%", background: ge, flexShrink: 0 } }, Ue)
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
                                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "2px", flexShrink: 0 }, children: H.slice(0, 6).map(
                                  /** @param {any} [hex] @param {any} [pi] @returns {any} */
                                  (he, ge) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => nt(
                                    /** @param {any} [prev] @returns {any} */
                                    (Ue) => ({ ...Ue, [d.name]: he })
                                  ), style: { width: "14px", height: "14px", borderRadius: "50%", background: he, border: `2px solid ${(Ye[d.name] || Ce(Q, s, H)) === he ? s.text : "transparent"}`, cursor: "pointer", padding: 0, flexShrink: 0 }, title: he }, ge)
                                ) }),
                                /* @__PURE__ */ e.jsx("input", { type: "color", value: Ye[d.name] || Ce(Q, s, H), onChange: (he) => nt(
                                  /** @param {any} [prev] @returns {any} */
                                  (ge) => ({ ...ge, [d.name]: he.target.value })
                                ), style: { width: "24px", height: "24px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none", flexShrink: 0 }, title: `Custom color for ${d.name}` }),
                                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: s.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: d.name || `Series ${Q + 1}` }),
                                Ye[d.name] ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => nt(
                                  /** @param {any} [prev] @returns {any} */
                                  (he) => {
                                    const ge = { ...he };
                                    return delete ge[d.name], ge;
                                  }
                                ), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 2px" }, title: "Reset to palette", children: "↺" }) : null
                              ] }, d.name || Q)
                            ),
                            t.series.length > 20 ? /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: s.textSec }, children: `…${t.series.length - 20} more` }) : null
                          ] }) : null,
                          /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${s.border}`, paddingTop: "8px" }, children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 700, color: s.textSec, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "6px" }, children: "Custom Palette" }),
                            /* @__PURE__ */ e.jsx(Pn, { customColors: At, onChange: Zt, theme: s, compactInputStyle: Me }),
                            At.length >= 2 ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Le("custom"), style: { ..._t, marginTop: "6px", background: He === "custom" ? s.select : void 0, color: He === "custom" ? s.primary : void 0, border: `1px solid ${He === "custom" ? s.primary : s.border}` }, children: He === "custom" ? "Custom active" : "Use custom palette" }) : null
                          ] }),
                          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Effects" }),
                          /* @__PURE__ */ e.jsx(de, { label: "Animate", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ir(
                            /** @param {any} [v] @returns {any} */
                            (d) => !d
                          ), style: { border: `1px solid ${ut ? s.primary : s.border}`, background: ut ? s.select : s.headerSubtleBg || s.hover, color: ut ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: ut ? "On" : "Off" }) }),
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" && n !== "sparkline" && !xt.has(n) ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx(de, { label: "Gradient", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Mt(
                              /** @param {any} [v] @returns {any} */
                              (d) => !d
                            ), style: { border: `1px solid ${Ie ? s.primary : s.border}`, background: Ie ? s.select : s.headerSubtleBg || s.hover, color: Ie ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: Ie ? "On" : "Off" }) }),
                            Ie ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(de, { label: "Grad Dir", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "5px" }, children: zr.map((d) => /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => mr(d.value), style: { border: `1px solid ${Dt === d.value ? s.primary : s.border}`, background: Dt === d.value ? s.select : s.headerSubtleBg || s.hover, color: Dt === d.value ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "5px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: d.label }, d.value)) }) }),
                              /* @__PURE__ */ e.jsx(de, { label: "Grad Opacity", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center" }, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "range", min: "10", max: "100", step: "5", value: Nt, onChange: (d) => jr(Number(d.target.value)), style: { flex: 1 } }),
                                /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "11px", color: s.textSec, minWidth: "30px" }, children: [
                                  Nt,
                                  "%"
                                ] })
                              ] }) })
                            ] }) : null
                          ] }) : null,
                          n === "line" || n === "scatter" || n === "bubble" || n === "area" || n === "radar" || le ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Markers" }),
                            /* @__PURE__ */ e.jsx(de, { label: "Shape", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(tt, { size: "sm", "aria-label": "Marker shape", value: me, onChange: (d) => St(d.target.value), style: { ...Me, width: "100%" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: "none", children: "None (line only)" }),
                              /* @__PURE__ */ e.jsx("option", { value: "circle", children: "Circle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "square", children: "Square" }),
                              /* @__PURE__ */ e.jsx("option", { value: "diamond", children: "Diamond" }),
                              /* @__PURE__ */ e.jsx("option", { value: "triangle", children: "Triangle" }),
                              /* @__PURE__ */ e.jsx("option", { value: "cross", children: "Cross" })
                            ] }) }),
                            /* @__PURE__ */ e.jsx(de, { label: "Size", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsxs(tt, { size: "sm", "aria-label": "Marker size", value: yt, onChange: (d) => Tt(Number(d.target.value)), style: { ...Me, width: "92px" }, children: [
                              /* @__PURE__ */ e.jsx("option", { value: 3, children: "Small" }),
                              /* @__PURE__ */ e.jsx("option", { value: 5, children: "Medium" }),
                              /* @__PURE__ */ e.jsx("option", { value: 7, children: "Large" }),
                              /* @__PURE__ */ e.jsx("option", { value: 10, children: "XLarge" })
                            ] }) })
                          ] }) : null,
                          (n === "line" || n === "area" || le) && Array.isArray(t && t.series) && t.series.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Line Style" }),
                            /* @__PURE__ */ e.jsx(Nn, { series: t.series, lineDashStyles: Vt, onChange: wr, seriesColors: Ye, paletteColors: H, getColorForIndex: Ce, theme: s, compactInputStyle: Me })
                          ] }) : null,
                          n === "bar" || n === "line" || n === "area" || n === "scatter" || le ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                            /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", fontWeight: 800, letterSpacing: "0.08em", textTransform: "uppercase", color: s.textSec, paddingBottom: "4px", borderBottom: `1px solid ${s.border}`, marginTop: "4px" }, children: "Trendline" }),
                            /* @__PURE__ */ e.jsx(de, { label: "Show", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => et(
                              /** @param {any} [v] @returns {any} */
                              (d) => !d
                            ), style: { border: `1px solid ${U ? s.primary : s.border}`, background: U ? s.select : s.headerSubtleBg || s.hover, color: U ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: U ? "On" : "Off" }) }),
                            U ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                              /* @__PURE__ */ e.jsx(de, { label: "Type", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(tt, { size: "sm", "aria-label": "Trendline type", value: Se, onChange: (d) => Be(d.target.value), style: { ...Me, width: "100%" }, children: [
                                /* @__PURE__ */ e.jsx("option", { value: "linear", children: "Linear Regression" }),
                                /* @__PURE__ */ e.jsx("option", { value: "moving_avg", children: "Moving Average" }),
                                /* @__PURE__ */ e.jsx("option", { value: "exponential", children: "Exponential Smoothing" })
                              ] }) }),
                              Se === "moving_avg" ? /* @__PURE__ */ e.jsx(de, { label: "Period", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "2", max: "50", value: Ge, onChange: (d) => lr(Math.max(2, Math.min(50, Number(d.target.value) || 3))), style: { ...Me, width: "80px" } }) }) : null,
                              /* @__PURE__ */ e.jsxs(de, { label: "Color", theme: s, children: [
                                /* @__PURE__ */ e.jsx("input", { type: "color", value: Ne || "#888888", onChange: (d) => x(d.target.value), style: { width: "32px", height: "28px", padding: 0, border: `1px solid ${s.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }, title: "Trendline color" }),
                                Ne ? /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => x(""), style: { border: "none", background: "none", color: s.textSec, cursor: "pointer", fontSize: "10px", padding: "0 4px" }, title: "Reset to auto", children: "↺" }) : null
                              ] }),
                              /* @__PURE__ */ e.jsx(de, { label: "Width", theme: s, controlMinWidth: "80px", children: /* @__PURE__ */ e.jsx("input", { type: "number", min: "1", max: "8", value: Pt, onChange: (d) => kr(Math.max(1, Math.min(8, Number(d.target.value) || 2))), style: { ...Me, width: "80px" } }) })
                            ] }) : null
                          ] }) : null
                        ] }),
                        p === "labels" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                          n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" ? /* @__PURE__ */ e.jsx(de, { label: "Data Labels", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Ot(
                            /** @param {any} [v] @returns {any} */
                            (d) => !d
                          ), style: { border: `1px solid ${ye ? s.primary : s.border}`, background: ye ? s.select : s.headerSubtleBg || s.hover, color: ye ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: ye ? "On" : "Off" }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "pie" && n !== "donut" ? /* @__PURE__ */ e.jsx(de, { label: "Format", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(tt, { size: "sm", value: at, onChange: (d) => Fe(d.target.value), style: { ...Me, width: "100%" }, title: "Value format for axis labels", children: bn.map((d) => /* @__PURE__ */ e.jsx("option", { value: d.value, children: d.label }, d.value)) }) }) : null,
                          n !== "heatmap" && n !== "icicle" && n !== "sunburst" && n !== "sankey" && n !== "sparkline" && !xt.has(n) ? /* @__PURE__ */ e.jsx(de, { label: "Decimals", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(tt, { size: "sm", "aria-label": "Decimals", value: Ut, onChange: (d) => Kt(Number(d.target.value)), style: { ...Me, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: -1, children: "Auto" }),
                            /* @__PURE__ */ e.jsx("option", { value: 0, children: "0  (e.g. 1,234)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 1, children: "1  (e.g. 1.2)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 2, children: "2  (e.g. 1.23)" }),
                            /* @__PURE__ */ e.jsx("option", { value: 3, children: "3  (e.g. 1.234)" })
                          ] }) }) : null,
                          ye && n === "bar" ? /* @__PURE__ */ e.jsx(de, { label: "Position", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsxs(tt, { size: "sm", "aria-label": "Label position", value: Rt, onChange: (d) => Gt(d.target.value), style: { ...Me, width: "100%" }, children: [
                            /* @__PURE__ */ e.jsx("option", { value: "outside", children: "Outside" }),
                            /* @__PURE__ */ e.jsx("option", { value: "inside", children: "Inside top" }),
                            /* @__PURE__ */ e.jsx("option", { value: "center", children: "Center" })
                          ] }) }) : null,
                          n === "pie" || n === "donut" ? /* @__PURE__ */ e.jsx(de, { label: "Callouts", theme: s, children: /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => ar(
                            /** @param {any} [v] @returns {any} */
                            (d) => !d
                          ), style: { border: `1px solid ${je ? s.primary : s.border}`, background: je ? s.select : s.headerSubtleBg || s.hover, color: je ? s.primary : s.text, borderRadius: s.radiusSm || "8px", padding: "6px 8px", fontSize: "11px", fontWeight: 700, cursor: "pointer" }, children: je ? "On" : "Off" }) }) : null,
                          n !== "sparkline" && !xt.has(n) ? /* @__PURE__ */ e.jsx(de, { label: "Legend", theme: s, controlMinWidth: "100%", children: /* @__PURE__ */ e.jsx(En, { legendPosition: vt, onChange: dr, theme: s }) }) : null
                        ] })
                      ]
                    }
                  )
                ]
              }
            )
          ] }) : null
        ] }),
        Pe ? /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(os, { model: t, theme: s }) }) : null
      ]
    }
  );
}, Cs = ({
  onClose: t,
  variant: n = "pane",
  source: i,
  onSourceChange: a,
  chartType: r,
  onChartTypeChange: h,
  chartLayers: m = [],
  onChartLayersChange: C,
  availableColumns: S = [],
  barLayout: D,
  onBarLayoutChange: $,
  axisMode: Y,
  onAxisModeChange: J,
  orientation: K,
  onOrientationChange: s,
  hierarchyLevel: V,
  onHierarchyLevelChange: T,
  model: xe,
  theme: z,
  width: F,
  chartTitle: G,
  onChartTitleChange: ce,
  chartHeight: ee,
  onChartHeightChange: ae,
  onResizeStart: Ve,
  floating: fe = !1,
  onToggleFloating: We,
  floatingRect: Ee = null,
  onFloatingDragStart: ft,
  onFloatingResizeStart: rt,
  rowLimit: ht,
  onRowLimitChange: Ze,
  columnLimit: _e,
  onColumnLimitChange: qe,
  sortMode: Xe,
  onSortModeChange: nr,
  onMeasureIdsChange: sr,
  interactionMode: ot,
  onInteractionModeChange: Xt,
  serverScope: zt,
  onServerScopeChange: Lt,
  chartDefinitions: gt = [],
  activeChartId: Ft = null,
  onActiveChartChange: B,
  onCreateChart: ke,
  onDuplicateChart: le,
  onDeleteChart: P,
  onRenameChart: Ke,
  onCategoryActivate: it,
  allowHierarchyCharts: Bt = !0,
  showServerScope: dt = !1,
  locked: ze = !1,
  onToggleLock: Qe,
  dockPosition: L = "right",
  onDockPositionChange: Te,
  immersiveMode: ye,
  onImmersiveModeChange: Ot,
  onSettingsWidthBudgetChange: He,
  standalone: Le = !1,
  showResizeHandle: at = !0,
  title: Fe = "Chart Panel",
  showDefinitionManager: kt = !0,
  initialSettings: or = {},
  onSettingsChange: Z,
  initialSettingsPaneOpen: be = !1
}) => {
  yn("PivotChartPanel", Ft);
  const [ve, Je] = v.useState(!1), [Ae, bt] = v.useState(!1), [Re, wt] = v.useState(be), [lt, Oe] = v.useState(348), De = v.useRef(null), Yt = v.useRef(Tr(
    F,
    430,
    hr,
    Vr
  )), ct = v.useRef(He), Et = v.useRef(null), me = typeof ye == "boolean" ? ye : Ae, St = () => {
    const U = !me;
    if (typeof Ot == "function") {
      Ot(U);
      return;
    }
    bt(U);
  }, yt = fe && ze, Tt = (U) => ({
    border: `1px solid ${U === i ? z.primary : z.border}`,
    background: U === i ? z.select : z.headerSubtleBg || z.hover,
    color: U === i ? z.primary : z.text,
    borderRadius: z.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer"
  }), Ie = (U = !1) => ({
    border: `1px solid ${U ? z.primary : z.border}`,
    background: U ? z.select : z.headerSubtleBg || z.hover,
    color: U ? z.primary : z.text,
    borderRadius: z.radiusSm || "8px",
    padding: "6px 8px",
    fontSize: "11px",
    fontWeight: 700,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "6px",
    minWidth: "32px"
  }), Mt = (U) => ({
    ...Ie(L === U),
    minWidth: "56px",
    padding: "6px 10px"
  }), Ye = !me && Re ? lt + 9 : 0, nt = Tr(
    F,
    430,
    hr,
    Vr
  ), ut = Ee && Number.isFinite(Number(Ee.width)) ? Math.max(hr, Math.floor(Number(Ee.width))) : nt, ir = Ee && Number.isFinite(Number(Ee.height)) ? Math.max(vn, Math.floor(Number(Ee.height))) : Math.max(mn, (Number(ee) || rr) + Kr);
  v.useEffect(() => {
    ct.current = He;
  }, [He]);
  const je = v.useCallback((U, et = {}) => {
    const Se = Number(U), Be = Number.isFinite(Se) ? Math.max(320, Math.ceil(Se)) : null;
    if (!et.force && Et.current === Be) return;
    Et.current = Be;
    const Ge = ct.current;
    typeof Ge == "function" && Ge(Be);
  }, []), ar = ve ? {
    position: "fixed",
    inset: "16px",
    zIndex: 10040,
    display: "flex",
    minWidth: 0,
    minHeight: 0
  } : fe ? {
    position: "absolute",
    left: `${Ee && Ee.left || 24}px`,
    top: `${Ee && Ee.top || 24}px`,
    width: `${ut + Ye}px`,
    height: `${ir}px`,
    zIndex: 240,
    display: "flex",
    minWidth: 0,
    minHeight: 0
  } : Le ? {
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
    width: `${nt + Ye}px`,
    minWidth: `${nt + Ye}px`,
    maxWidth: `${nt + Ye}px`,
    flexShrink: 0
  };
  return v.useEffect(() => {
    if (!Le || fe || ve) return;
    const U = De.current;
    if (!U) return;
    const et = () => {
      const Be = U.clientWidth;
      Be > 0 && !Re && (Yt.current = Math.max(hr, Be));
    };
    if (et(), typeof ResizeObserver > "u") return;
    const Se = new ResizeObserver(et);
    return Se.observe(U), () => Se.disconnect();
  }, [fe, ve, Re, Le]), v.useEffect(() => {
    if (!Le || fe || ve || me || !Re) {
      je(null);
      return;
    }
    const U = Yt.current || nt;
    je(U + Ye);
  }, [
    nt,
    je,
    me,
    fe,
    ve,
    Re,
    Le,
    Ye
  ]), v.useEffect(() => () => {
    je(null, { force: !0 });
  }, [je]), /* @__PURE__ */ e.jsxs("div", { style: ar, children: [
    !ve && !fe && !Le && at ? /* @__PURE__ */ e.jsx(
      "div",
      {
        onMouseDown: (U) => {
          U.preventDefault(), Ve && Ve();
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
          background: z.border,
          opacity: 0.9
        } })
      }
    ) : null,
    /* @__PURE__ */ e.jsxs("aside", { ref: De, style: {
      width: ve || fe || Le ? "100%" : `calc(100% - ${at ? 8 : 0}px)`,
      height: ve || fe || Le ? "100%" : "auto",
      minWidth: 0,
      minHeight: 0,
      borderLeft: Le || fe || ve ? "none" : `1px solid ${z.border}`,
      border: fe || ve ? `1px solid ${z.border}` : "none",
      borderRadius: fe || ve ? z.radius || "16px" : 0,
      background: z.sidebarBg || z.surfaceBg || z.background,
      padding: ve ? "12px" : "14px",
      overflow: "hidden",
      boxShadow: fe || ve ? z.shadowMd || "0 18px 40px rgba(15,23,42,0.24)" : Le ? "none" : `-10px 0 24px ${z.pinnedBoundaryShadow || "rgba(15,23,42,0.12)"}`,
      display: "flex",
      flexDirection: "column",
      gap: me ? "8px" : "0",
      position: "relative"
    }, children: [
      n === "preview" ? null : /* @__PURE__ */ e.jsxs(
        "div",
        {
          onMouseDown: (U) => {
            fe && !ve && !yt && typeof ft == "function" && ft(U);
          },
          style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px", marginBottom: me ? "0" : "12px", cursor: fe && !ve && !yt ? "move" : "default" },
          children: [
            me ? /* @__PURE__ */ e.jsx("div", {}) : /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "4px", minWidth: 0 }, children: /* @__PURE__ */ e.jsx("div", { style: { fontSize: "15px", fontWeight: 800, color: z.text }, children: Fe }) }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px" }, onMouseDown: (U) => U.stopPropagation(), children: [
              typeof We == "function" ? /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: We,
                  style: Ie(fe),
                  title: fe ? "Dock chart panel" : "Float chart over table",
                  "aria-label": fe ? "Dock chart panel" : "Float chart over table",
                  children: fe ? "Dock" : "Float"
                }
              ) : null,
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: St,
                  style: Ie(me),
                  title: me ? "Exit immersive mode" : "Enter immersive mode",
                  "aria-label": me ? "Exit immersive mode" : "Enter immersive mode",
                  children: "Immersive"
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => Je(
                    /** @param {any} [isActive] @returns {any} */
                    (U) => !U
                  ),
                  style: Ie(ve),
                  title: ve ? "Exit full screen" : "Open chart full screen",
                  "aria-label": ve ? "Exit full screen" : "Open chart full screen",
                  children: ve ? /* @__PURE__ */ e.jsx(st.FullscreenExit, {}) : /* @__PURE__ */ e.jsx(st.Fullscreen, {})
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: t,
                  style: Ie(!1),
                  title: Le ? "Close chart pane" : "Close chart panel",
                  "aria-label": Le ? "Close chart pane" : "Close chart panel",
                  children: ve ? /* @__PURE__ */ e.jsx(st.Close, {}) : "Close"
                }
              )
            ] })
          ]
        }
      ),
      !me && kt ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", marginBottom: "10px", flexWrap: "wrap", alignItems: "center" }, children: [
        Array.isArray(gt) && gt.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
          /* @__PURE__ */ e.jsx(
            tt,
            {
              size: "sm",
              "aria-label": "Active chart",
              value: Ft || "",
              onChange: (U) => {
                typeof B == "function" && B(U.target.value);
              },
              style: {
                border: `1px solid ${z.border}`,
                background: z.surfaceBg || z.background || "#fff",
                color: z.text,
                borderRadius: z.radiusSm || "8px",
                padding: "6px 8px",
                fontSize: "11px",
                fontWeight: 700,
                minWidth: "160px"
              },
              children: gt.map((U) => /* @__PURE__ */ e.jsx("option", { value: U.id, children: U.name || U.id }, U.id))
            }
          ),
          /* @__PURE__ */ e.jsx(
            "input",
            {
              type: "text",
              value: (gt.find((U) => U.id === Ft) || {}).name || "",
              onChange: (U) => {
                typeof Ke == "function" && Ke(U.target.value);
              },
              placeholder: "Chart name",
              style: {
                border: `1px solid ${z.border}`,
                background: z.surfaceBg || z.background || "#fff",
                color: z.text,
                borderRadius: z.radiusSm || "8px",
                padding: "6px 8px",
                fontSize: "11px",
                fontWeight: 700,
                minWidth: "180px"
              }
            }
          )
        ] }) : null,
        typeof ke == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: Ie(!1), onClick: ke, children: "New" }) : null,
        typeof le == "function" ? /* @__PURE__ */ e.jsx("button", { type: "button", style: Ie(!1), onClick: le, children: "Duplicate" }) : null,
        typeof P == "function" ? /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            style: Ie(!1),
            onClick: P,
            disabled: !Array.isArray(gt) || gt.length <= 1,
            children: "Delete"
          }
        ) : null
      ] }) : null,
      !me && Le && !fe && typeof Te == "function" ? /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "10px", flexWrap: "wrap", alignItems: "center" }, children: [
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Te("left"), style: Mt("left"), title: "Dock chart pane to the left", children: "Left" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Te("right"), style: Mt("right"), title: "Dock chart pane to the right", children: "Right" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Te("top"), style: Mt("top"), title: "Dock chart pane to the top", children: "Top" }),
        /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => Te("bottom"), style: Mt("bottom"), title: "Dock chart pane to the bottom", children: "Bottom" })
      ] }) : null,
      me ? null : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", marginBottom: "12px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ e.jsx("button", { style: Tt("pivot"), onClick: () => a("pivot"), children: "Pivot View" }),
        /* @__PURE__ */ e.jsx("button", { style: Tt("selection"), onClick: () => a("selection"), children: "Selection" })
      ] }),
      /* @__PURE__ */ e.jsx(
        en,
        {
          model: xe,
          chartType: r,
          chartLayers: m,
          onChartLayersChange: C,
          availableColumns: S,
          barLayout: D,
          axisMode: Y,
          onChartTypeChange: h,
          onBarLayoutChange: $,
          onAxisModeChange: J,
          orientation: K,
          onOrientationChange: s,
          hierarchyLevel: V,
          onHierarchyLevelChange: T,
          rowLimit: ht,
          onRowLimitChange: Ze,
          columnLimit: _e,
          onColumnLimitChange: qe,
          title: G,
          onTitleChange: ce,
          chartHeight: ee,
          onChartHeightChange: ae,
          sortMode: Xe,
          onSortModeChange: nr,
          onMeasureIdsChange: sr,
          interactionMode: ot,
          onInteractionModeChange: Xt,
          serverScope: zt,
          onServerScopeChange: Lt,
          showServerScope: dt,
          theme: z,
          immersiveMode: me,
          onCategoryActivate: it,
          allowHierarchyCharts: Bt,
          floating: fe,
          locked: ze,
          onToggleLock: Qe,
          configOpen: Re,
          onConfigChange: wt,
          settingsPanelWidth: lt,
          onSettingsPanelWidthChange: Oe,
          initialSettings: or,
          onSettingsChange: Z
        }
      ),
      fe && !ve && !ze ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (U) => typeof rt == "function" && rt("right", U),
            style: { position: "absolute", top: 0, right: 0, width: "10px", height: "100%", cursor: "ew-resize" }
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (U) => typeof rt == "function" && rt("bottom", U),
            style: { position: "absolute", left: 0, bottom: 0, width: "100%", height: "10px", cursor: "ns-resize" }
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (U) => typeof rt == "function" && rt("corner", U),
            style: { position: "absolute", right: 0, bottom: 0, width: "18px", height: "18px", cursor: "nwse-resize" }
          }
        )
      ] }) : null
    ] })
  ] });
}, Ws = (t) => t.open ? /* @__PURE__ */ e.jsx(Cs, { ...t }) : null, zs = ({ chartState: t, onClose: n, theme: i, position: a = "right", onPositionChange: r }) => {
  const [h, m] = v.useState(t && t.chartType ? t.chartType : "bar"), [C, S] = v.useState(t && t.barLayout ? t.barLayout : "grouped"), [D, $] = v.useState(t && t.axisMode ? t.axisMode : "vertical"), [Y, J] = v.useState(t && t.defaultOrientation ? t.defaultOrientation : "rows"), [K, s] = v.useState(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), [V, T] = v.useState(t && t.rowLimit ? t.rowLimit : Or), [xe, z] = v.useState(t && t.columnLimit ? t.columnLimit : Yr), [F, G] = v.useState(t && t.sortMode ? t.sortMode : "natural"), [ce, ee] = v.useState(t && Array.isArray(t.chartLayers) ? t.chartLayers : []), [ae, Ve] = v.useState(null);
  v.useEffect(() => {
    m(t && t.chartType ? t.chartType : "bar"), S(t && t.barLayout ? t.barLayout : "grouped"), $(t && t.axisMode ? t.axisMode : "vertical"), J(t && t.defaultOrientation ? t.defaultOrientation : "rows"), s(t && t.defaultHierarchyLevel !== void 0 ? t.defaultHierarchyLevel : "all"), T(t && t.rowLimit ? t.rowLimit : Or), z(t && t.columnLimit ? t.columnLimit : Yr), G(t && t.sortMode ? t.sortMode : "natural"), ee(t && Array.isArray(t.chartLayers) ? t.chartLayers : []);
  }, [t]);
  const fe = v.useMemo(() => {
    if (!t || !Array.isArray(t.visibleCols)) return [];
    if (!(t.selectionMap && t.visibleRows))
      return Wr(t.visibleCols);
    const Ze = dn(t.selectionMap, t.visibleRows, t.visibleCols);
    if (!Ze) return [];
    const _e = new Set(
      (Ze.colIndexes || []).map(
        /** @param {any} [index] @returns {any} */
        (qe) => t.visibleCols[qe]
      ).filter(
        /** @param {any} [column] @returns {any} */
        (qe) => qe && qe.id && !un.has(qe.id)
      ).map(
        /** @param {any} [column] @returns {any} */
        (qe) => qe.id
      )
    );
    return Wr(t.visibleCols).filter(
      /** @param {any} [column] @returns {any} */
      (qe) => _e.has(qe.id)
    );
  }, [t]), We = v.useMemo(() => t ? t.selectionMap && t.visibleRows && t.visibleCols ? h === "combo" ? xn(t.selectionMap, t.visibleRows, t.visibleCols, {
    hierarchyLevel: K,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: V,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: F,
    layers: ce
  }) : pn(t.selectionMap, t.visibleRows, t.visibleCols, {
    orientation: Y,
    hierarchyLevel: K,
    maxHierarchyLevel: t.maxHierarchyLevel || 1,
    maxRows: V,
    maxColumns: xe,
    rowFields: t.rowFields || [],
    colFields: t.colFields || [],
    sortMode: F
  }) : t.model : null, [ce, t, h, K, Y, V, xe, F]);
  if (v.useEffect(() => {
    if (C !== "stacked") return;
    if (h === "combo") {
      S("grouped");
      return;
    }
    (h === "area" ? We && Array.isArray(We.series) && We.series.length > 1 : Ur(We)) || S("grouped");
  }, [We, C, h]), !t) return null;
  const Ee = a === "top" || a === "bottom", ft = Math.max(420, Number.isFinite(Number(ae)) ? Math.floor(Number(ae)) : 420), rt = Math.max(360, Number(t.chartHeight || rr) + Kr), ht = (Ze, _e) => ({
    border: `1px solid ${Ze === a ? i.primary : i.border}`,
    background: Ze === a ? i.select : i.headerSubtleBg || i.hover,
    color: Ze === a ? i.primary : i.text,
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
        width: Ee ? "100%" : `${ft}px`,
        minWidth: Ee ? 0 : `${Math.max(320, ft)}px`,
        maxWidth: Ee ? "100%" : "70%",
        height: Ee ? `${rt}px` : void 0,
        minHeight: Ee ? "320px" : 0,
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
                  style: ht("left"),
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
                  style: ht("right"),
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
                  style: ht("top"),
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
                  style: ht("bottom"),
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
            model: We,
            chartType: h,
            chartLayers: ce,
            onChartLayersChange: ee,
            availableColumns: fe,
            barLayout: C,
            axisMode: D,
            onChartTypeChange: m,
            onBarLayoutChange: S,
            onAxisModeChange: $,
            orientation: Y,
            onOrientationChange: J,
            hierarchyLevel: K,
            onHierarchyLevelChange: s,
            title: t.title || "Range Chart",
            rowLimit: V,
            onRowLimitChange: T,
            columnLimit: xe,
            onColumnLimitChange: z,
            sortMode: F,
            onSortModeChange: G,
            theme: i,
            allowHierarchyCharts: !(t.selectionMap && t.visibleRows && t.visibleCols),
            onSettingsWidthBudgetChange: Ve
          }
        )
      ] })
    }
  );
};
export {
  zs as PivotChartModal,
  Ws as PivotChartPanel
};
