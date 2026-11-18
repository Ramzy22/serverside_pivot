import { j as e, I as E, $ as ke, a0 as Ot, a1 as Ht, a2 as wr, a3 as ir, a4 as Nt, a5 as Ut, a6 as Vt, a7 as He, a8 as sr, a9 as dr, G as Kt, aa as Yt, ab as Zt, ac as Gt, ad as Je, ae as ut, af as Xt, ag as qr, T as ne, ah as qt, ai as Jt, aj as Qt, ak as Jr, al as en, am as rn } from "./index-DqztQvIY.js";
import { a as de, R as u } from "./charts-DthW1Q8W.js";
const tn = ({ column: t, theme: n, styles: i, onFilter: o, currentFilter: l, options: x, optionMeta: f, onSearchOptions: k, onLoadMoreOptions: y, onOpen: F }) => {
  const [s, b] = de.useState(!1), W = l && (l.conditions || l.value), P = () => b(!1), M = () => {
    const $ = !s;
    b($), $ && typeof F == "function" && F(t.id);
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column" }, children: [
    /* @__PURE__ */ e.jsxs(
      "div",
      {
        style: {
          ...i.columnItem,
          cursor: "pointer",
          background: s ? n.select : "transparent",
          borderLeft: W ? `3px solid ${n.primary}` : "3px solid transparent"
        },
        onClick: M,
        children: [
          /* @__PURE__ */ e.jsx("span", { style: { marginRight: "8px", opacity: 0.7, display: "flex" }, children: s ? /* @__PURE__ */ e.jsx(E.ChevronDown, {}) : /* @__PURE__ */ e.jsx(E.ChevronRight, {}) }),
          /* @__PURE__ */ e.jsxs("span", { style: { flex: 1, fontWeight: 500, display: "flex", alignItems: "center", gap: "6px" }, children: [
            W && /* @__PURE__ */ e.jsx(E.Filter, { style: { fontSize: "12px", color: n.primary } }),
            ke(typeof t.header == "string" ? t.header : t.columnDef && typeof t.columnDef.header == "string" ? t.columnDef.header : t.id)
          ] })
        ]
      }
    ),
    s && /* @__PURE__ */ e.jsx("div", { style: { padding: "8px", borderBottom: `1px solid ${n.border}44` }, children: /* @__PURE__ */ e.jsx(
      Ot,
      {
        column: t,
        onFilter: o,
        currentFilter: l,
        options: x,
        optionMeta: f,
        onSearchOptions: k,
        onLoadMoreOptions: y,
        theme: n,
        onClose: P
      }
    ) })
  ] });
}, kr = ({ title: t, children: n, items: i, renderItem: o, theme: l, styles: x, initialExpanded: f = !0, count: k, onDrop: y, sectionId: F }) => {
  const [s, b] = de.useState(f), [W, P] = de.useState(f), [M, $] = de.useState(f), [h, R] = de.useState(!1), A = de.useRef(null);
  de.useEffect(() => {
    if (s) {
      P(!0);
      const C = setTimeout(() => $(!0), 220);
      return () => clearTimeout(C);
    }
    $(!1);
    const w = setTimeout(() => P(!1), 220);
    return () => clearTimeout(w);
  }, [s]);
  const T = Ht({
    count: i ? i.length : 0,
    getScrollElement: () => A.current,
    estimateSize: () => 32,
    overscan: 10,
    enabled: !!i && (s || W)
  }), U = (w) => {
    w.preventDefault(), R(!0);
  }, D = () => {
    R(!1);
  }, O = (w) => {
    w.preventDefault(), R(!1);
    const C = w.dataTransfer.getData("text/plain");
    y && C && y(C, F);
  }, a = (w) => {
    (w.key === "Enter" || w.key === " ") && (w.preventDefault(), b(
      /** @param {any} [prev] @returns {any} */
      (C) => !C
    ));
  };
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      "data-is-over": h ? "true" : void 0,
      style: {
        ...x.toolPanelSection,
        flex: s ? "0 1 auto" : "0 0 auto",
        minHeight: s ? "40px" : "32px",
        overflow: "hidden",
        // Prevent overlap
        borderRadius: h ? "8px" : void 0,
        background: h ? l.select || "rgba(37,99,235,0.08)" : void 0,
        boxShadow: h ? `inset 0 0 0 1.5px ${l.primary}` : void 0,
        transition: "background-color 140ms ease, box-shadow 140ms ease"
      },
      onDragOver: U,
      onDragLeave: D,
      onDrop: O,
      children: [
        /* @__PURE__ */ e.jsxs(
          "div",
          {
            role: "button",
            tabIndex: 0,
            "aria-expanded": s,
            className: "sp-btn sp-btn-ghost",
            style: { ...x.toolPanelSectionHeader, borderRadius: "6px" },
            onClick: () => b(!s),
            onKeyDown: a,
            children: [
              /* @__PURE__ */ e.jsx("span", { style: {
                marginRight: "8px",
                opacity: 0.72,
                display: "inline-flex",
                alignItems: "center",
                transform: s ? "rotate(90deg)" : "rotate(0deg)",
                transition: "transform 160ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))"
              }, children: /* @__PURE__ */ e.jsx(E.ChevronRight, {}) }),
              /* @__PURE__ */ e.jsx("span", { style: { flex: 1 }, children: t }),
              k !== void 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", opacity: 0.7, background: l.hover, padding: "2px 7px", borderRadius: "10px", fontVariantNumeric: "tabular-nums" }, children: k })
            ]
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-disclosure-grid",
            "data-expanded": s ? "true" : "false",
            style: { pointerEvents: s ? "auto" : "none" },
            children: /* @__PURE__ */ e.jsx(
              "div",
              {
                ref: A,
                className: "sp-disclosure-inner",
                style: {
                  ...x.toolPanelList,
                  maxHeight: "300px",
                  overflowY: s && M ? "auto" : "hidden"
                },
                children: i ? /* @__PURE__ */ e.jsx("div", { style: {
                  height: `${T.getTotalSize()}px`,
                  width: "100%",
                  position: "relative"
                }, children: T.getVirtualItems().map(
                  /** @param {any} [virtualRow] @returns {any} */
                  (w) => /* @__PURE__ */ e.jsx(
                    "div",
                    {
                      style: {
                        position: "absolute",
                        top: 0,
                        left: 0,
                        width: "100%",
                        height: `${w.size}px`,
                        transform: `translateY(${w.start}px)`
                      },
                      children: o(i[w.index], w.index)
                    },
                    w.index
                  )
                ) }) : n
              }
            )
          }
        )
      ]
    }
  );
}, pt = ({ column: t, level: n, theme: i, styles: o, handlePinColumn: l, colSearch: x, selectedCols: f, setSelectedCols: k, onDrop: y, sectionId: F }) => {
  const [s, b] = de.useState(n < 1), [W, P] = de.useState(!1), [M, $] = de.useState(!1), h = de.useRef(null), R = t.columns && t.columns.length > 0, A = t.columnDef.header;
  let T = typeof A == "string" ? A : t.id;
  typeof T == "string" && T.startsWith("group_") && (T = t.id.replace("group_", "").split("|||").pop() || T), t.headerVal && (T = t.headerVal), T = ke(T);
  const U = t.getIsPinned(), D = t.getIsVisible(), O = R ? wr(t) : [t.id], a = O.length > 0 && O.every((v) => f.has(v)), w = !a && O.some((v) => f.has(v));
  if (de.useEffect(() => {
    h.current && (h.current.indeterminate = w);
  }, [w]), x && !T.toLowerCase().includes(x.toLowerCase()))
    if (R) {
      const v = (V) => {
        const G = V.columnDef.header;
        return (typeof G == "string" ? G : V.id).toLowerCase().includes(x.toLowerCase()) ? !0 : V.columns ? V.columns.some(v) : !1;
      };
      if (!v(t)) return null;
    } else
      return null;
  const C = (v) => {
    v.stopPropagation();
    const V = new Set(f), G = R ? wr(t) : [t.id];
    G.every(
      /** @param {any} [id] @returns {any} */
      (Q) => V.has(Q)
    ) ? G.forEach(
      /** @param {any} [id] @returns {any} */
      (Q) => V.delete(Q)
    ) : G.forEach(
      /** @param {any} [id] @returns {any} */
      (Q) => V.add(Q)
    ), k(V);
  }, B = (v) => {
    if (v.stopPropagation(), R) {
      const V = Nt(t), G = V.some(
        /** @param {any} [c] @returns {any} */
        (K) => !K.getIsVisible()
      );
      V.forEach(
        /** @param {any} [c] @returns {any} */
        (K) => K.toggleVisibility(G)
      );
    } else
      t.toggleVisibility();
  }, _ = (v, V) => {
    v.stopPropagation(), l(t.id, V);
  }, N = (v) => {
    $(!0), v.dataTransfer.setData("text/plain", t.id), v.dataTransfer.effectAllowed = "move";
  }, L = () => {
    $(!1);
  }, g = (v) => {
    v.preventDefault(), v.stopPropagation();
    const V = v.dataTransfer.getData("text/plain");
    V && y && V !== t.id && y(V, F, t.id);
  }, z = (v) => {
    v.key === "Enter" ? B(v) : v.key === " " ? (v.preventDefault(), C(v)) : v.altKey && v.key === "ArrowLeft" ? (v.preventDefault(), l(t.id, "left")) : v.altKey && v.key === "ArrowRight" ? (v.preventDefault(), l(t.id, "right")) : v.altKey && v.key === "ArrowDown" && (v.preventDefault(), l(t.id, !1));
  };
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      style: { display: "flex", flexDirection: "column", opacity: M ? 0.5 : 1 },
      draggable: !R,
      onDragStart: N,
      onDragEnd: L,
      onDragOver: (v) => v.preventDefault(),
      onDrop: g,
      role: "treeitem",
      "aria-selected": a || w,
      "aria-expanded": s,
      children: [
        /* @__PURE__ */ e.jsxs(
          "div",
          {
            style: {
              ...o.columnItem,
              paddingLeft: `${n * 12 + 8}px`,
              // Reduced indentation step
              background: a || w ? i.select : W ? i.hover : "transparent",
              borderLeft: U ? `3px solid ${i.primary}` : "3px solid transparent"
            },
            onMouseEnter: (v) => !(a || w) && P(!0),
            onMouseLeave: (v) => !(a || w) && P(!1),
            tabIndex: 0,
            onKeyDown: z,
            children: [
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  ref: h,
                  type: "checkbox",
                  checked: a,
                  onChange: C,
                  onClick: (v) => v.stopPropagation(),
                  style: { margin: 0, cursor: "pointer", pointerEvents: "auto" },
                  tabIndex: -1
                }
              ),
              !R && /* @__PURE__ */ e.jsx("span", { style: { cursor: "grab", display: "flex", opacity: 0.7, marginRight: "4px" }, children: /* @__PURE__ */ e.jsx(E.DragIndicator, {}) }),
              R ? /* @__PURE__ */ e.jsx(
                "span",
                {
                  role: "button",
                  tabIndex: 0,
                  "aria-expanded": s,
                  "aria-label": s ? `Collapse ${T}` : `Expand ${T}`,
                  onClick: () => b(!s),
                  onKeyDown: (v) => {
                    (v.key === "Enter" || v.key === " ") && (v.preventDefault(), b(!s));
                  },
                  style: { cursor: "pointer", display: "flex", opacity: 0.7, marginRight: "4px" },
                  children: s ? /* @__PURE__ */ e.jsx(E.ChevronDown, {}) : /* @__PURE__ */ e.jsx(E.ChevronRight, {})
                }
              ) : /* @__PURE__ */ e.jsx("span", { style: { width: "20px" } }),
              /* @__PURE__ */ e.jsx(
                "span",
                {
                  onClick: B,
                  style: {
                    cursor: "pointer",
                    display: "flex",
                    color: D ? i.primary : i.textSec,
                    opacity: D ? 1 : 0.5,
                    marginRight: "4px"
                  },
                  title: D ? "Hide Column" : "Show Column",
                  "aria-label": D ? "Hide Column" : "Show Column",
                  children: D ? /* @__PURE__ */ e.jsx(E.Visibility, {}) : /* @__PURE__ */ e.jsx(E.VisibilityOff, {})
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "span",
                {
                  style: {
                    flex: 1,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    fontWeight: R ? 600 : 400,
                    cursor: R ? "pointer" : "default",
                    color: D ? i.text : i.textSec,
                    opacity: D ? 1 : 0.6,
                    display: "flex",
                    alignItems: "center"
                  },
                  onClick: () => R && b(!s),
                  title: T,
                  children: [
                    R && /* @__PURE__ */ e.jsx(E.Group, { style: { marginRight: "6px", fontSize: "14px", opacity: 0.8 } }),
                    T
                  ]
                }
              ),
              !R && /* @__PURE__ */ e.jsxs("div", { className: "pin-controls", children: [
                /* @__PURE__ */ e.jsx("button", { onClick: (v) => _(v, "left"), title: "Pin Column Left", "aria-label": "Pin Column Left", children: /* @__PURE__ */ e.jsx(E.PinLeft, {}) }),
                /* @__PURE__ */ e.jsx("button", { onClick: (v) => _(v, !1), title: "Unpin Column", "aria-label": "Unpin Column", children: /* @__PURE__ */ e.jsx(E.Unpin, {}) }),
                /* @__PURE__ */ e.jsx("button", { onClick: (v) => _(v, "right"), title: "Pin Column Right", "aria-label": "Pin Column Right", children: /* @__PURE__ */ e.jsx(E.PinRight, {}) })
              ] })
            ]
          }
        ),
        R && s && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: t.columns.filter(
          /** @param {any} [child] @returns {any} */
          (v) => ir(v, F)
        ).map(
          /** @param {any} [child] @returns {any} */
          (v) => /* @__PURE__ */ e.jsx(
            pt,
            {
              column: v,
              level: n + 1,
              theme: i,
              styles: o,
              handlePinColumn: l,
              colSearch: x,
              selectedCols: f,
              setSelectedCols: k,
              onDrop: y,
              sectionId: F
            },
            v.id
          )
        ) })
      ]
    }
  );
}, xt = /\b[A-Za-z_][A-Za-z0-9_]*\b/g, ft = /^[A-Za-z_][A-Za-z0-9_]*$/, cr = /\[([^\]]+)\]/g, nn = /* @__PURE__ */ new Set(["abs", "round", "ceil", "floor", "max", "min", "int", "float"]), ln = /* @__PURE__ */ new Set(["True", "False", "None", "and", "or", "not", "if", "else", "in", "is"]), Ee = {
  type: "line",
  metric: "last",
  showCurrentValue: !0,
  showDelta: !0,
  compact: !1,
  hideColumns: !1,
  placement: "before",
  source: "pivot",
  displayMode: "trend"
};
function gt(t) {
  return String(t || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function Me(t, n = "formula") {
  const o = String(t || "").trim().toLowerCase().replace(/\s+/g, "").replace(/[^a-z0-9_]/g, "") || String(n || "formula").toLowerCase().replace(/[^a-z0-9_]/g, "") || "formula";
  return /^[a-z_]/.test(o) ? o : `f_${o}`;
}
function J(t) {
  return !t || typeof t != "object" ? "" : String(
    t.formulaRef || Me(t.label || "", t.field || "formula") || t.field || ""
  ).trim();
}
function Ne(t) {
  const n = String(t || "").trim().toLowerCase();
  return ["columns", "display", "displayed", "displayed_columns", "rendered", "rendered_columns"].includes(n) ? "columns" : "measures";
}
function ze(t) {
  return Ne(t) === "columns";
}
const on = [
  { value: "eq", label: "Equals" },
  { value: "ne", label: "Does not equal" },
  { value: "contains", label: "Contains" },
  { value: "starts_with", label: "Starts with" },
  { value: "ends_with", label: "Ends with" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "in", label: "In list" },
  { value: "not_in", label: "Not in list" },
  { value: "is_null", label: "Is empty" },
  { value: "is_not_null", label: "Is not empty" }
], Qr = /* @__PURE__ */ new Set(["in", "not_in"]), an = /* @__PURE__ */ new Set(["is_null", "is_not_null"]);
function yt(t) {
  return { field: t && t[0] || "", operator: "eq", value: "", values: [], caseSensitive: !1 };
}
function bt(t, n = 0) {
  return {
    id: `rule-${Date.now().toString(36)}-${n}`,
    label: n === 0 ? "Group A" : `Group ${n + 1}`,
    condition: { op: "AND", clauses: [yt(t)] }
  };
}
function et(t, n = null) {
  if (n) {
    const o = He([n])[0] || n;
    return { ...JSON.parse(JSON.stringify(o)), __editingField: o.field, __editingId: o.id };
  }
  const i = sr(`category_${Date.now().toString(36)}`);
  return {
    id: i,
    field: dr(i),
    name: "New Category",
    fallbackLabel: "Other",
    order: "creation",
    rules: [bt(t, 0)],
    __isNew: !0
  };
}
function sn(t, n) {
  if (!n) return "string";
  const i = Array.isArray(t) ? t.slice(0, 250) : [];
  for (let o = 0; o < i.length; o += 1) {
    const l = i[o];
    if (!l || typeof l != "object") continue;
    const x = l[n];
    if (!(x == null || x === "")) {
      if (typeof x == "number" && Number.isFinite(x)) return "number";
      if (x instanceof Date && !Number.isNaN(x.getTime())) return "date";
      if (typeof x == "string") {
        const f = x.trim();
        if (!f) continue;
        if (Number.isFinite(Number(f))) return "number";
        const k = Date.parse(f);
        return !Number.isNaN(k) && /[-/T:]/.test(f) ? "date" : "string";
      }
    }
  }
  return "string";
}
const ar = (t, n, i) => (Array.isArray(n) ? n : []).filter((o) => sn(t, o) === i);
function dn(t, n) {
  const o = ar(n, t, "date")[0] || t[0] || "", l = sr(`${o || "date"}_month`);
  return {
    id: l,
    field: dr(l),
    kind: "date_group",
    name: o ? `${ke(o)} Month` : "Date Month",
    sourceField: o,
    granularity: "month",
    fiscalYearStartMonth: 1,
    fallbackLabel: "No date",
    __isNew: !0
  };
}
function cn(t, n) {
  const o = ar(n, t, "number")[0] || t[0] || "", l = sr(`${o || "number"}_bins`);
  return {
    id: l,
    field: dr(l),
    kind: "number_group",
    name: o ? `${ke(o)} Bins` : "Number Bins",
    sourceField: o,
    binSize: 10,
    minValue: "",
    maxValue: "",
    precision: 0,
    fallbackLabel: "No value",
    __isNew: !0
  };
}
const rt = (t) => String(t || "").split(",").map((n) => n.trim()).filter(Boolean), tt = (t) => String(t || "").trim().replace(/\s+/g, " ").toLowerCase(), ht = (t) => t && (t.__editingField || t.field) ? t.__editingField || t.field : "";
function un(t, n) {
  const i = Array.isArray(t) ? t : [], o = ht(n);
  if (!o) return i;
  const l = i.findIndex(
    /** @param {any} [dimension] @returns {any} */
    (x) => x && x.field === o
  );
  return l >= 0 ? i.slice(0, l) : i;
}
function nt(t) {
  const n = [];
  return (t && Array.isArray(t.rules) ? t.rules : []).forEach(
    /** @param {any} [rule] @returns {any} */
    (i) => {
      (i && i.condition && Array.isArray(i.condition.clauses) ? i.condition.clauses : []).forEach(
        /** @param {any} [clause] @returns {any} */
        (l) => {
          l && l.field && n.push(l.field);
        }
      );
    }
  ), Array.from(new Set(n));
}
function pn(t, n) {
  const i = (Array.isArray(t) ? t : []).filter((o) => o && typeof o == "object").filter((o) => !o._isTotal && o._path !== "__grand_total__" && o._path !== "__color_scale_stats__");
  return Ut(i, n || []);
}
function xn(t, n) {
  return !Array.isArray(t) || t.length === 0 || !Array.isArray(n) || n.length === 0 ? !1 : n.every((i) => t.some((o) => o && Object.prototype.hasOwnProperty.call(o, i)));
}
const fn = (t, n) => (n && Array.isArray(n.rules) ? n.rules : []).map(
  /** @param {any} [rule] @returns {any} */
  (i) => ({
    label: i && i.label ? i.label : "Rule",
    count: t.reduce(
      /** @param {any} [matches] @param {any} [row] @returns {any} */
      (o, l) => Vt(l, i.condition) ? o + 1 : o,
      0
    )
  })
), gn = u.lazy(() => import(
  /* webpackChunkName: "pivot-report-editor" */
  "./ReportEditor-6WpIXBNG.js"
).then((t) => ({
  default: t.ReportEditor
}))), yn = (t) => /* @__PURE__ */ e.jsx(u.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { role: "status", style: { padding: "16px" }, children: "Loading report builder…" }), children: /* @__PURE__ */ e.jsx(gn, { ...t }) });
function lt({
  availableFields: t,
  customDimensions: n,
  setCustomDimensions: i,
  data: o,
  theme: l,
  showNotification: x,
  getFieldLabel: f,
  onDeleteField: k
}) {
  const y = u.useMemo(
    () => (t || []).filter(
      /** @param {any} [field] @returns {any} */
      (a) => a && !ut(a)
    ),
    [t]
  ), F = u.useMemo(
    () => He(n || []),
    [n]
  ), [s, b] = u.useState(null), W = u.useMemo(
    () => un(F, s),
    [s, F]
  ), P = u.useMemo(() => {
    const a = /* @__PURE__ */ new Set(), w = [];
    return y.forEach(
      /** @param {any} [field] @returns {any} */
      (C) => {
        C && !a.has(C) && (a.add(C), w.push(C));
      }
    ), W.forEach(
      /** @param {any} [dimension] @returns {any} */
      (C) => {
        C && C.field && !a.has(C.field) && (a.add(C.field), w.push(C.field));
      }
    ), w;
  }, [W, y]), M = u.useMemo(
    () => {
      const a = ar(o, y, "date");
      return a.length > 0 ? a : y;
    },
    [y, o]
  ), $ = u.useMemo(
    () => {
      const a = ar(o, y, "number");
      return a.length > 0 ? a : y;
    },
    [y, o]
  ), h = u.useMemo(
    () => pn(o, W),
    [W, o]
  ), R = u.useMemo(() => ({
    border: `1px solid ${l.border}`,
    background: l.background,
    color: l.text,
    borderRadius: "8px",
    fontSize: "12px",
    padding: "7px 8px",
    outline: "none",
    minWidth: 0
  }), [l.background, l.border, l.text]), A = u.useCallback((a = "default") => ({
    border: a === "primary" ? `1px solid ${l.primary}` : `1px solid ${l.border}`,
    background: a === "primary" ? l.primary || "#2563EB" : l.hover || "#F8FAFC",
    color: a === "primary" ? "#fff" : l.text,
    borderRadius: "8px",
    fontSize: "11px",
    fontWeight: 700,
    padding: "6px 9px",
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: "5px"
  }), [l.border, l.hover, l.primary, l.text]);
  u.useEffect(() => {
    if (!s) return;
    const a = (w) => {
      w.key === "Escape" && (w.preventDefault(), b(null));
    };
    return window.addEventListener("keydown", a), () => window.removeEventListener("keydown", a);
  }, [s]);
  const T = u.useCallback((a, w) => {
    b(
      /** @param {any} [current] @returns {any} */
      (C) => {
        if (!C) return C;
        const B = [...C.rules || []];
        return B[a] = typeof w == "function" ? w(B[a]) : w, { ...C, rules: B };
      }
    );
  }, []), U = u.useCallback((a, w, C) => {
    T(
      a,
      /** @param {any} [rule] @returns {any} */
      (B) => {
        const _ = B.condition || { op: "AND", clauses: [] }, N = [..._.clauses || []];
        return N[w] = { ...N[w], ...C }, { ...B, condition: { ..._, clauses: N } };
      }
    );
  }, [T]), D = u.useCallback(() => {
    const a = He([s])[0];
    if (!a || !a.name) {
      x && x("A grouped field needs a name.", "warning");
      return;
    }
    if (a.kind === "category" && (!a.rules || a.rules.length === 0)) {
      x && x("A category needs at least one valid rule.", "warning");
      return;
    }
    if ((a.kind === "date_group" || a.kind === "number_group") && !a.sourceField) {
      x && x("Choose the source field to group.", "warning");
      return;
    }
    const w = ht(s), C = tt(a.name);
    if (F.some(
      /** @param {any} [dimension] @returns {any} */
      (_) => _ && _.field !== w && tt(_.name) === C
    )) {
      x && x(`A category named "${a.name}" already exists.`, "warning");
      return;
    }
    if (a.kind === "category") {
      const _ = new Set(P);
      if (nt(a).find(
        /** @param {any} [field] @returns {any} */
        (g) => !_.has(g)
      )) {
        x && x("A category can only use base fields or categories defined before it.", "warning");
        return;
      }
      const L = nt(a);
      if (xn(h, L)) {
        const g = fn(h, a).find(
          /** @param {any} [summary] @returns {any} */
          (z) => z.count === 0
        );
        if (g) {
          x && x(`"${g.label}" does not match any current row. Adjust the condition before applying.`, "warning");
          return;
        }
      }
    }
    if (a.kind === "date_group" && !M.includes(a.sourceField)) {
      x && x("Choose a date field for date grouping.", "warning");
      return;
    }
    if (a.kind === "number_group") {
      if (!$.includes(a.sourceField)) {
        x && x("Choose a numeric field for number grouping.", "warning");
        return;
      }
      if (!Number.isFinite(Number(a.binSize)) || Number(a.binSize) <= 0) {
        x && x("Number bins need a positive bin size.", "warning");
        return;
      }
    }
    i(
      /** @param {any} [previous] @returns {any} */
      (_) => [...He(_ || []).filter(
        /** @param {any} [dimension] @returns {any} */
        (g) => g.id !== a.id && g.field !== a.field && g.field !== w
      ), a]
    ), b(null), x && x(`Applied ${a.name}.`, "success");
  }, [P, M, s, F, $, i, x, h]), O = u.useCallback((a) => {
    const w = He([a])[0];
    w && (i(
      /** @param {any} [previous] @returns {any} */
      (C) => He(C || []).filter(
        /** @param {any} [item] @returns {any} */
        (B) => B.field !== w.field
      )
    ), typeof k == "function" && k(w.field), s && s.field === w.field && b(null), x && x(`Removed ${w.name}.`, "success"));
  }, [s, k, i, x]);
  return /* @__PURE__ */ e.jsxs("div", { style: {
    border: `1px solid ${l.border}`,
    background: l.headerBg || l.surfaceInset || l.background,
    borderRadius: "12px",
    padding: "10px",
    marginBottom: "14px",
    display: "flex",
    flexDirection: "column",
    gap: "10px"
  }, children: [
    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { children: [
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 800 }, children: "Custom Categories" }),
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec, marginTop: "2px" }, children: "Named filter rules that become fields." })
      ] }),
      /* @__PURE__ */ e.jsx(
        "button",
        {
          type: "button",
          onClick: () => b(et(P)),
          style: { ...A("primary"), whiteSpace: "nowrap", flexShrink: 0 },
          children: "+ Category"
        }
      )
    ] }),
    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
      /* @__PURE__ */ e.jsxs(
        "button",
        {
          type: "button",
          onClick: () => b(dn(y, o)),
          disabled: M.length === 0,
          style: { ...A(), opacity: M.length === 0 ? 0.5 : 1, cursor: M.length === 0 ? "not-allowed" : "pointer" },
          title: M.length === 0 ? "No date fields detected" : "Create date buckets",
          children: [
            /* @__PURE__ */ e.jsx(E.Group, {}),
            " Date Group"
          ]
        }
      ),
      /* @__PURE__ */ e.jsxs(
        "button",
        {
          type: "button",
          onClick: () => b(cn(y, o)),
          disabled: $.length === 0,
          style: { ...A(), opacity: $.length === 0 ? 0.5 : 1, cursor: $.length === 0 ? "not-allowed" : "pointer" },
          title: $.length === 0 ? "No numeric fields detected" : "Create numeric bins",
          children: [
            /* @__PURE__ */ e.jsx(E.NumberFormat, {}),
            " Number Bins"
          ]
        }
      )
    ] }),
    F.length > 0 && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: F.map(
      /** @param {any} [dimension] @returns {any} */
      (a) => {
        const w = a.kind || "category", C = w === "date_group" ? `${f(a.sourceField)} by ${a.granularity}` : w === "number_group" ? `${f(a.sourceField)} every ${a.binSize}` : `${a.rules.length} rule${a.rules.length === 1 ? "" : "s"} | fallback: ${a.fallbackLabel}`;
        return /* @__PURE__ */ e.jsxs(
          "div",
          {
            style: {
              border: `1px solid ${l.border}`,
              background: l.background,
              borderRadius: "10px",
              padding: "8px",
              display: "flex",
              alignItems: "center",
              gap: "8px"
            },
            children: [
              w === "number_group" ? /* @__PURE__ */ e.jsx(E.NumberFormat, {}) : /* @__PURE__ */ e.jsx(E.Branch, {}),
              /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0, flex: 1 }, children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: l.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: a.name }),
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: C })
              ] }),
              /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => b(et(y, a)), style: A(), children: [
                /* @__PURE__ */ e.jsx(E.Edit, {}),
                " Edit"
              ] }),
              /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => O(a), style: { ...A(), color: "#B91C1C" }, children: /* @__PURE__ */ e.jsx(E.Delete, {}) })
            ]
          },
          a.field
        );
      }
    ) }),
    s && /* @__PURE__ */ e.jsx(
      "div",
      {
        role: "presentation",
        onMouseDown: (a) => {
          a.target === a.currentTarget && b(null);
        },
        style: {
          position: "fixed",
          inset: 0,
          zIndex: 1e4,
          background: "rgba(15, 23, 42, 0.38)",
          backdropFilter: "blur(4px)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "24px"
        },
        children: /* @__PURE__ */ e.jsxs(
          "div",
          {
            role: "dialog",
            "aria-modal": "true",
            "aria-label": s.kind === "date_group" || s.kind === "number_group" ? s.__editingField ? "Edit grouped field" : "Add grouped field" : s.__editingField ? "Edit custom category" : "Add custom category",
            style: {
              width: "min(780px, calc(100vw - 32px))",
              maxHeight: "calc(100vh - 48px)",
              border: `1px solid ${l.primary || "#2563EB"}55`,
              background: l.background,
              color: l.text,
              borderRadius: "16px",
              boxShadow: "0 24px 80px rgba(15, 23, 42, 0.28)",
              overflow: "hidden"
            },
            children: [
              /* @__PURE__ */ e.jsxs("div", { style: {
                padding: "14px 16px",
                borderBottom: `1px solid ${l.border}`,
                background: l.headerBg || l.surfaceInset || l.background,
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: "12px"
              }, children: [
                /* @__PURE__ */ e.jsxs("div", { children: [
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "13px", fontWeight: 900, color: l.text }, children: s.kind === "date_group" || s.kind === "number_group" ? s.__editingField ? "Edit Grouped Field" : "Add Grouped Field" : s.__editingField ? "Edit Custom Category" : "Add Custom Category" }),
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec, marginTop: "2px" }, children: s.kind === "date_group" ? "Create calendar buckets from a date field." : s.kind === "number_group" ? "Create fixed-width numeric bins from a number field." : "Rules can use base fields and categories defined before this one." })
                ] }),
                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => b(null), style: { ...A(), padding: "6px 8px" }, "aria-label": "Close category editor", children: /* @__PURE__ */ e.jsx(E.Close, {}) })
              ] }),
              /* @__PURE__ */ e.jsxs("div", { style: {
                padding: "14px",
                overflowY: "auto",
                maxHeight: "calc(100vh - 128px)",
                display: "flex",
                flexDirection: "column",
                gap: "10px"
              }, children: [
                /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 120px 150px", gap: "8px" }, children: [
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Category Name",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        value: s.name || "",
                        onChange: (a) => {
                          const w = a.target.value, C = s.id || sr(w);
                          b({ ...s, name: w, id: C, field: s.field || dr(C) });
                        },
                        style: R,
                        placeholder: "Example: Priority Segment"
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Else",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        value: s.fallbackLabel || "",
                        onChange: (a) => b({ ...s, fallbackLabel: a.target.value }),
                        style: R,
                        placeholder: "Other"
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Member Order",
                    /* @__PURE__ */ e.jsxs(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Member Order",
                        value: s.order || "creation",
                        onChange: (a) => b({ ...s, order: a.target.value }),
                        style: { ...R, width: "100%" },
                        children: [
                          /* @__PURE__ */ e.jsx("option", { value: "creation", children: "Creation Order" }),
                          /* @__PURE__ */ e.jsx("option", { value: "alpha", children: "Alphabetical (A→Z)" }),
                          /* @__PURE__ */ e.jsx("option", { value: "alpha_desc", children: "Alphabetical (Z→A)" }),
                          /* @__PURE__ */ e.jsx("option", { value: "numeric", children: "Numeric (0→9)" }),
                          /* @__PURE__ */ e.jsx("option", { value: "numeric_desc", children: "Numeric (9→0)" })
                        ]
                      }
                    )
                  ] })
                ] }),
                s.kind === "date_group" && /* @__PURE__ */ e.jsxs("div", { style: {
                  border: `1px solid ${l.border}`,
                  borderRadius: "10px",
                  padding: "10px",
                  background: l.hover || "#F8FAFC",
                  display: "grid",
                  gridTemplateColumns: "1.4fr 1fr 1fr",
                  gap: "8px"
                }, children: [
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Date Field",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Date Field",
                        value: s.sourceField || "",
                        onChange: (a) => b({
                          ...s,
                          sourceField: a.target.value,
                          name: s.__isNew ? `${f(a.target.value)} ${ke(s.granularity || "month")}` : s.name
                        }),
                        style: { ...R, width: "100%" },
                        children: M.map(
                          /** @param {any} [field] @returns {any} */
                          (a) => /* @__PURE__ */ e.jsx("option", { value: a, children: f(a) }, a)
                        )
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Date Part",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Date Part",
                        value: s.granularity || "month",
                        onChange: (a) => b({ ...s, granularity: a.target.value }),
                        style: { ...R, width: "100%" },
                        children: Array.from(Jt).map((a) => /* @__PURE__ */ e.jsx("option", { value: a, children: ke(a) }, a))
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Fiscal Start",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Fiscal Start",
                        value: s.fiscalYearStartMonth || 1,
                        onChange: (a) => b({ ...s, fiscalYearStartMonth: Number(a.target.value) }),
                        style: { ...R, width: "100%" },
                        children: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"].map((a, w) => /* @__PURE__ */ e.jsx("option", { value: w + 1, children: a }, a))
                      }
                    )
                  ] })
                ] }),
                s.kind === "number_group" && /* @__PURE__ */ e.jsxs("div", { style: {
                  border: `1px solid ${l.border}`,
                  borderRadius: "10px",
                  padding: "10px",
                  background: l.hover || "#F8FAFC",
                  display: "grid",
                  gridTemplateColumns: "1.4fr 0.8fr 0.8fr 0.8fr 0.7fr",
                  gap: "8px"
                }, children: [
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Number Field",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Number Field",
                        value: s.sourceField || "",
                        onChange: (a) => b({
                          ...s,
                          sourceField: a.target.value,
                          name: s.__isNew ? `${f(a.target.value)} Bins` : s.name
                        }),
                        style: { ...R, width: "100%" },
                        children: $.map(
                          /** @param {any} [field] @returns {any} */
                          (a) => /* @__PURE__ */ e.jsx("option", { value: a, children: f(a) }, a)
                        )
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Bin Size",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: s.binSize === void 0 ? "" : s.binSize,
                        onChange: (a) => b({ ...s, binSize: a.target.value }),
                        style: R
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Min",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: s.minValue === null || s.minValue === void 0 ? "" : s.minValue,
                        onChange: (a) => b({ ...s, minValue: a.target.value }),
                        style: R
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Max",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: s.maxValue === null || s.maxValue === void 0 ? "" : s.maxValue,
                        onChange: (a) => b({ ...s, maxValue: a.target.value }),
                        style: R
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Decimals",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        min: "0",
                        max: "8",
                        value: s.precision === null || s.precision === void 0 ? "" : s.precision,
                        onChange: (a) => b({ ...s, precision: a.target.value }),
                        style: R
                      }
                    )
                  ] })
                ] }),
                (!s.kind || s.kind === "category") && (s.rules || []).map(
                  /** @param {any} [rule] @param {any} [ruleIndex] @returns {any} */
                  (a, w) => {
                    const C = a.condition || { op: "AND", clauses: [] }, B = C.clauses || [];
                    return /* @__PURE__ */ e.jsxs("div", { style: {
                      border: `1px solid ${l.border}`,
                      borderRadius: "10px",
                      padding: "8px",
                      background: l.hover || "#F8FAFC",
                      display: "flex",
                      flexDirection: "column",
                      gap: "8px"
                    }, children: [
                      /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 78px 28px", gap: "8px", alignItems: "center" }, children: [
                        /* @__PURE__ */ e.jsx(
                          "input",
                          {
                            value: a.label || "",
                            onChange: (_) => T(w, { ...a, label: _.target.value }),
                            style: R,
                            placeholder: "Category value"
                          }
                        ),
                        /* @__PURE__ */ e.jsxs(
                          ne,
                          {
                            size: "sm",
                            "aria-label": "Rule condition combiner",
                            value: C.op || "AND",
                            onChange: (_) => T(w, { ...a, condition: { ...C, op: _.target.value } }),
                            style: R,
                            children: [
                              /* @__PURE__ */ e.jsx("option", { value: "AND", children: "AND" }),
                              /* @__PURE__ */ e.jsx("option", { value: "OR", children: "OR" })
                            ]
                          }
                        ),
                        /* @__PURE__ */ e.jsx(
                          "button",
                          {
                            type: "button",
                            onClick: () => b({ ...s, rules: (s.rules || []).filter(
                              /** @param {any} [_] @param {any} [index] @returns {any} */
                              (_, N) => N !== w
                            ) }),
                            style: { ...A(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                            title: "Remove rule",
                            children: /* @__PURE__ */ e.jsx(E.Close, {})
                          }
                        )
                      ] }),
                      B.map(
                        /** @param {any} [clause] @param {any} [clauseIndex] @returns {any} */
                        (_, N) => {
                          const L = an.has(_.operator), g = Qr.has(_.operator);
                          return /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: L ? "1fr 130px 28px" : "1fr 130px 1fr 28px", gap: "6px", alignItems: "center" }, children: [
                            /* @__PURE__ */ e.jsx(
                              ne,
                              {
                                size: "sm",
                                "aria-label": "Clause field",
                                value: _.field || "",
                                onChange: (z) => U(w, N, { field: z.target.value }),
                                style: { ...R, width: "100%" },
                                children: P.map(
                                  /** @param {any} [field] @returns {any} */
                                  (z) => /* @__PURE__ */ e.jsx("option", { value: z, children: f(z) }, z)
                                )
                              }
                            ),
                            /* @__PURE__ */ e.jsx(
                              ne,
                              {
                                size: "sm",
                                "aria-label": "Clause operator",
                                value: _.operator || "eq",
                                onChange: (z) => {
                                  const v = z.target.value;
                                  U(w, N, {
                                    operator: v,
                                    values: Qr.has(v) ? _.values && _.values.length ? _.values : rt(_.value) : _.values
                                  });
                                },
                                style: { ...R, width: "100%" },
                                children: on.map((z) => /* @__PURE__ */ e.jsx("option", { value: z.value, children: z.label }, z.value))
                              }
                            ),
                            !L && /* @__PURE__ */ e.jsx(
                              "input",
                              {
                                value: g ? (_.values || []).join(", ") : _.value || "",
                                onChange: (z) => U(w, N, g ? { values: rt(z.target.value), value: z.target.value } : { value: z.target.value }),
                                style: R,
                                placeholder: g ? "A, B, C" : "Value"
                              }
                            ),
                            /* @__PURE__ */ e.jsx(
                              "button",
                              {
                                type: "button",
                                onClick: () => T(w, {
                                  ...a,
                                  condition: {
                                    ...C,
                                    clauses: B.filter(
                                      /** @param {any} [_] @param {any} [index] @returns {any} */
                                      (z, v) => v !== N
                                    )
                                  }
                                }),
                                style: { ...A(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                                title: "Remove condition",
                                children: /* @__PURE__ */ e.jsx(E.Close, {})
                              }
                            )
                          ] }, `${a.id || w}:${N}`);
                        }
                      ),
                      /* @__PURE__ */ e.jsx(
                        "button",
                        {
                          type: "button",
                          onClick: () => T(w, {
                            ...a,
                            condition: {
                              ...C,
                              clauses: [...B, yt(P)]
                            }
                          }),
                          style: { ...A(), alignSelf: "flex-start" },
                          children: "+ Condition"
                        }
                      )
                    ] }, a.id || w);
                  }
                ),
                (!s.kind || s.kind === "category") && /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => b({
                      ...s,
                      rules: [...s.rules || [], bt(P, (s.rules || []).length)]
                    }),
                    style: { ...A(), alignSelf: "flex-start" },
                    children: "+ Rule"
                  }
                ),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "flex-end", gap: "8px" }, children: [
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => b(null), style: A(), children: "Cancel" }),
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: D, style: A("primary"), children: s.kind === "date_group" || s.kind === "number_group" ? "Apply Group" : "Apply Category" })
                ] })
              ] })
            ]
          }
        )
      }
    )
  ] });
}
function mt(t) {
  const n = [], i = /* @__PURE__ */ new Set();
  return String(t || "").replace(cr, (o, l) => {
    const x = String(l || "").trim();
    return x && !i.has(x) && (n.push(x), i.add(x)), "";
  }), n;
}
function ot(t, n = []) {
  const i = String(t || ""), o = [], l = /* @__PURE__ */ new Set(), x = (y) => {
    const F = String(y || "").trim();
    !F || l.has(F) || (o.push(F), l.add(F));
  };
  mt(i).forEach(x);
  let f = i.replace(cr, " ");
  return (Array.isArray(n) ? n : Array.from(n || [])).map((y) => String(y || "").trim()).filter((y) => y && !ft.test(y)).sort((y, F) => F.length - y.length).forEach(
    /** @param {any} [reference] @returns {any} */
    (y) => {
      const F = new RegExp(`(^|[^A-Za-z0-9_])${gt(y)}(?=$|[^A-Za-z0-9_])`, "g");
      f = f.replace(
        F,
        /** @param {any} [match] @param {any} [prefix] @returns {any} */
        (s, b) => (x(y), `${b} `)
      );
    }
  ), f.replace(
    xt,
    /** @param {any} [token] @param {any} [offset] @returns {any} */
    (y, F) => {
      const s = f.slice(F + y.length).trimStart();
      return ln.has(y) || nn.has(y) && s.startsWith("(") || x(y), y;
    }
  ), o;
}
function se(t) {
  const n = String(t || "").trim();
  return n ? `[${n}]` : "";
}
function Fe(t) {
  const n = String(t || "").trim();
  return n ? ft.test(n) ? n : `[${n}]` : "";
}
function bn(t, n, i = "formula") {
  const o = Me(t, i), l = new Set(
    (Array.isArray(n) ? n : []).map((k) => k ? k.agg === "formula" ? J(k) : k.field : null).filter(Boolean).map((k) => String(k).toLowerCase())
  );
  if (!l.has(o)) return o;
  let x = 2, f = `${o}${x}`;
  for (; l.has(f); )
    x += 1, f = `${o}${x}`;
  return f;
}
function hn(t, n, i) {
  if (!t || !n || n === i) return t;
  const o = Fe(i), l = [];
  let f = String(t).replace(cr, (k, y) => String(y || "").trim() === n ? o : (l.push(k), `@@${l.length - 1}@@`)).replace(new RegExp(`\\b${gt(n)}\\b`, "g"), o);
  return l.forEach((k, y) => {
    f = f.replace(`@@${y}@@`, k);
  }), f;
}
function Cr(t, n, i) {
  t(
    /** @param {any} [prev] @returns {any} */
    (o) => {
      const l = [...o];
      return l[n] ? (l[n] = i(l[n]), l) : o;
    }
  );
}
function it(t) {
  return !t || typeof t.closest != "function" ? !1 : !!t.closest('button, input, select, textarea, [contenteditable="true"]');
}
function mn(t) {
  if (!t || typeof t != "object") return "";
  const n = t.field === void 0 || t.field === null ? "" : String(t.field), i = t.agg === void 0 || t.agg === null ? "" : String(t.agg);
  return `${n}::${i}`;
}
function Rr(t) {
  const n = rn(t);
  return n ? {
    ...Ee,
    ...n,
    header: n.header || "",
    color: n.color || "",
    positiveColor: n.positiveColor || "",
    negativeColor: n.negativeColor || ""
  } : null;
}
function vn(t) {
  const n = {
    ...Ee,
    ...t || {}
  }, i = {
    type: n.type || Ee.type,
    metric: n.metric || Ee.metric,
    showCurrentValue: n.showCurrentValue !== !1,
    showDelta: n.showDelta !== !1,
    compact: !!n.compact,
    hideColumns: !!n.hideColumns,
    placement: "before",
    source: n.source === "field" ? "field" : "pivot",
    displayMode: n.displayMode === "value" ? "value" : "trend"
  };
  return ["header", "color", "positiveColor", "negativeColor"].forEach((o) => {
    const l = typeof n[o] == "string" ? n[o].trim() : "";
    l && (i[o] = l);
  }), Number.isFinite(Number(n.areaOpacity)) && (i.areaOpacity = Math.max(0.02, Math.min(0.45, Number(n.areaOpacity)))), i;
}
function vt(t, n = []) {
  const i = String(t && t.agg || "").trim().toLowerCase(), o = Array.isArray(n) && n.length > 0, l = i === "array_agg", x = i !== "array_agg" && o;
  return {
    canUseFieldSeries: l,
    canUsePivotSeries: x,
    canUseAny: l || x,
    defaultSource: l ? "field" : x ? "pivot" : null
  };
}
function Ar(t, n) {
  return !n || !n.canUseAny ? !1 : t === "field" ? n.canUseFieldSeries : n.canUsePivotSeries;
}
function St(t, n) {
  const i = vn(t);
  return !n || !n.canUseAny ? null : Ar(i.source, n) ? i : {
    ...i,
    source: n.defaultSource
  };
}
function Dr(t) {
  if (!Array.isArray(t) || t.length === 0) return "";
  const n = t.map((i) => i && Fe(i.field)).filter(Boolean);
  return n.length === 0 ? "" : n.length === 1 ? `${n[0]} * 100` : `${n[0]} - ${n[1]}`;
}
function at(t) {
  const n = (Array.isArray(t) ? t : []).map((i) => i && i.id).filter(Boolean);
  return n.length === 0 ? "" : n.length === 1 ? `${se(n[0])} * 100` : `${se(n[0])} - ${se(n[1])}`;
}
function Sn(t) {
  const n = new Set(
    (Array.isArray(t) ? t : []).map((o) => o && o.field).filter(Boolean)
  );
  let i = 1;
  for (; n.has(`formula_${i}`); ) i += 1;
  return `formula_${i}`;
}
function jn(t, n, i = null, o = null, l = "measures", x = [], f = []) {
  const k = String(t || "").trim(), y = ze(l);
  if (!k)
    return {
      tone: "warning",
      text: y ? "Enter a formula using bracketed rendered columns. Example: [Cash Equity_day_pnl_sum] - [ETF_day_pnl_sum]" : "Enter a formula using value fields. Example: sales - [day pnl]"
    };
  let F = 0;
  for (const D of k)
    if (D === "(" && (F += 1), D === ")" && (F -= 1), F < 0)
      return { tone: "warning", text: "Parentheses are unbalanced." };
  if (F !== 0)
    return { tone: "warning", text: "Parentheses are unbalanced." };
  if (y) {
    const D = Array.isArray(x) ? x : [], O = new Set(
      D.map(
        /** @param {any} [option] @returns {any} */
        (B) => B && B.id
      ).filter(Boolean)
    ), a = mt(k);
    if (a.length === 0)
      return {
        tone: "warning",
        text: "Insert at least one rendered column reference like [column_id]."
      };
    const w = k.replace(cr, ""), C = Array.from(new Set(w.match(xt) || []));
    if (C.length > 0)
      return {
        tone: "warning",
        text: `Rendered columns must use brackets: ${C.join(", ")}`
      };
    if (O.size > 0) {
      const B = a.filter(
        /** @param {any} [columnId] @returns {any} */
        (_) => !O.has(_)
      );
      if (B.length > 0)
        return {
          tone: "warning",
          text: `Unknown rendered columns: ${B.join(", ")}`
        };
    }
    return i && a.includes(i) ? {
      tone: "warning",
      text: "A displayed-column formula cannot reference its own output column."
    } : {
      tone: "success",
      text: `Using rendered columns: ${a.join(", ")}`
    };
  }
  const s = Array.isArray(n) ? n.filter((D) => D && D.field) : [];
  if (s.length === 0)
    return {
      tone: "warning",
      text: "Add at least one value or formula reference before creating formulas."
    };
  const b = s.filter(
    /** @param {any} [config] @returns {any} */
    (D) => D.agg === "formula"
  ), W = /* @__PURE__ */ new Map();
  b.forEach(
    /** @param {any} [config] @returns {any} */
    (D) => {
      const O = D.field === i ? Me(o || J(D), D.field) : J(D);
      O && (W.set(O, O), W.set(O.toLowerCase(), O));
    }
  );
  const P = s.filter(
    /** @param {any} [config] @returns {any} */
    (D) => D.agg !== "formula"
  ).map(
    /** @param {any} [config] @returns {any} */
    (D) => D.field
  ).filter(Boolean), M = new Set(P), $ = /* @__PURE__ */ new Set([...P, ...b.map(
    /** @param {any} [config] @returns {any} */
    (D) => D.field === i ? Me(o || J(D), D.field) : J(D)
  ).filter(Boolean), ...Array.isArray(f) ? f : []]), h = Array.from($), R = ot(k, h), A = R.map(
    /** @param {any} [token] @returns {any} */
    (D) => W.get(D) || W.get(D.toLowerCase()) || D
  ), T = A.filter(
    /** @param {any} [token] @returns {any} */
    (D) => !$.has(D) && !M.has(D)
  );
  if (T.length > 0)
    return {
      tone: "warning",
      text: `Unknown fields: ${T.join(", ")}`
    };
  const U = i ? Me(o || i, i) : null;
  if (i && U && s.some(
    /** @param {any} [config] @returns {any} */
    (O) => {
      if (!O || O.field === i) return !1;
      const a = O.agg === "formula" ? J(O) : O.field;
      return String(a || "").toLowerCase() === U.toLowerCase();
    }
  ))
    return {
      tone: "warning",
      text: `Reference key "${U}" is already in use.`
    };
  if (U && A.includes(U))
    return {
      tone: "warning",
      text: "A formula cannot reference itself."
    };
  if (i && U) {
    const D = {};
    b.forEach(
      /** @param {any} [config] @returns {any} */
      (C) => {
        const B = C.field === i ? U : J(C), _ = C.field === i ? k : String(C.formula || ""), N = ot(_, h).map(
          /** @param {any} [token] @returns {any} */
          (L) => W.get(L) || W.get(L.toLowerCase()) || L
        ).filter(
          /** @param {any} [token] @returns {any} */
          (L) => L !== B && b.some(
            /** @param {any} [candidate] @returns {any} */
            (g) => J(g) === L || g.field === i && U === L
          )
        );
        D[B] = N;
      }
    );
    const O = /* @__PURE__ */ new Set(), a = /* @__PURE__ */ new Set(), w = (C) => {
      if (O.has(C)) return !0;
      if (a.has(C)) return !1;
      O.add(C);
      const B = D[C] || [];
      for (const _ of B)
        if (w(_)) return !0;
      return O.delete(C), a.add(C), !1;
    };
    if (w(U))
      return {
        tone: "warning",
        text: "Circular formula reference detected."
      };
  }
  return R.length === 0 ? {
    tone: "warning",
    text: "Insert at least one measure or formula reference."
  } : {
    tone: "success",
    text: `Using ${A.join(", ")}`
  };
}
function kn(t) {
  if (!Array.isArray(t) || t.length === 0) return [];
  const n = t.map((i) => i && Fe(i.field)).filter(Boolean);
  return n.length === 0 ? [] : n.length === 1 ? [{ label: "Scale x100", value: `${n[0]} * 100` }] : [
    { label: "Difference", value: `${n[0]} - ${n[1]}` },
    { label: "Ratio %", value: `(${n[0]} / ${n[1]}) * 100` },
    { label: "Margin %", value: `((${n[0]} - ${n[1]}) / ${n[0]}) * 100` }
  ];
}
function wn(t) {
  const n = (Array.isArray(t) ? t : []).map((i) => i && i.id).filter(Boolean);
  return n.length === 0 ? [] : n.length === 1 ? [{ label: "Scale x100", value: `${se(n[0])} * 100` }] : [
    { label: "Difference", value: `${se(n[0])} - ${se(n[1])}` },
    { label: "Ratio %", value: `(${se(n[0])} / ${se(n[1])}) * 100` },
    { label: "Spread %", value: `((${se(n[0])} - ${se(n[1])}) / ${se(n[0])}) * 100` }
  ];
}
function Cn(t, n) {
  const i = {
    muted: {
      text: t.textSec,
      border: t.border,
      background: t.headerSubtleBg || t.background
    },
    success: {
      text: t.primary,
      border: `${t.primary}33`,
      background: `${t.primary}10`
    },
    warning: {
      text: "#B45309",
      border: "#F59E0B55",
      background: "#F59E0B12"
    }
  };
  return i[n] || i.muted;
}
function Rn({ item: t, idx: n, setValConfigs: i, theme: o, colFields: l = [], onClose: x }) {
  const f = u.useMemo(
    () => Rr(t && t.sparkline) || { ...Ee },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [t && t.sparkline]
  ), k = u.useMemo(
    () => vt(t, l),
    [l, t]
  ), y = u.useMemo(() => {
    const h = [];
    return k.canUsePivotSeries && h.push({ value: "pivot", label: "Linked pivot" }), k.canUseFieldSeries && h.push({ value: "field", label: "Series field" }), h;
  }, [k]), F = Ar(f.source || "pivot", k) ? f.source || "pivot" : k.defaultSource, s = u.useCallback((h) => {
    Cr(
      i,
      n,
      /** @param {any} [current] @returns {any} */
      (R) => {
        const A = Rr(R.sparkline) || { ...Ee }, T = St({ ...A, ...h }, k);
        return T ? {
          ...R,
          sparkline: T
        } : R;
      }
    );
  }, [k, n, i]), b = u.useCallback((h) => h.stopPropagation(), []), W = [
    { value: "line", label: "Line" },
    { value: "area", label: "Area" },
    { value: "column", label: "Column" },
    { value: "bar", label: "Bar" }
  ], P = {
    background: o.background,
    border: `1px solid ${o.border}`,
    borderRadius: "5px",
    color: o.text,
    fontSize: "11px",
    padding: "4px 7px",
    outline: "none",
    width: "100%",
    boxSizing: "border-box"
  }, M = {
    fontSize: "10px",
    fontWeight: 700,
    letterSpacing: "0.06em",
    color: o.textSec,
    textTransform: "uppercase",
    marginBottom: "6px",
    display: "block"
  }, $ = {
    marginBottom: "14px"
  };
  return /* @__PURE__ */ e.jsx(
    "div",
    {
      style: { position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", zIndex: 9999, display: "flex", alignItems: "center", justifyContent: "center" },
      onMouseDown: b,
      onClick: x,
      children: /* @__PURE__ */ e.jsxs(
        "div",
        {
          onClick: b,
          style: {
            background: o.background,
            border: `1px solid ${o.border}`,
            borderRadius: "12px",
            boxShadow: "0 8px 32px rgba(0,0,0,0.22)",
            width: "320px",
            maxHeight: "80vh",
            overflow: "auto",
            padding: "18px 20px 16px",
            fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif"
          },
          children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "16px" }, children: [
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "13px", fontWeight: 700, color: o.text }, children: "Sparkline Column" }),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: x,
                  style: { background: "none", border: "none", cursor: "pointer", color: o.textSec, fontSize: "18px", lineHeight: 1, padding: "2px 4px" },
                  children: "×"
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Data source" }),
              y.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: y.map(
                  /** @param {any} p0 @returns {any} */
                  ({ value: h, label: R }) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => s({ source: h }),
                      style: {
                        flex: 1,
                        padding: "4px 0",
                        fontSize: "11px",
                        fontWeight: 600,
                        borderRadius: "6px",
                        cursor: "pointer",
                        border: `1px solid ${F === h ? o.primary : o.border}`,
                        background: F === h ? `${o.primary}18` : o.headerSubtleBg || o.background,
                        color: F === h ? o.primary : o.textSec
                      },
                      children: R
                    },
                    h
                  )
                ) }),
                /* @__PURE__ */ e.jsx("div", { style: { marginTop: "5px", fontSize: "10px", color: o.textSec, lineHeight: 1.4 }, children: F === "pivot" ? "Trend values are collected across the pivot columns. Source value cells stay numeric unless hidden." : "Each cell already contains the full data series for its row." })
              ] }) : /* @__PURE__ */ e.jsx(
                "div",
                {
                  "data-value-sparkline-unavailable": "true",
                  style: {
                    padding: "7px 8px",
                    borderRadius: "6px",
                    border: `1px solid ${o.warning || "#F59E0B"}55`,
                    background: `${o.warning || "#F59E0B"}12`,
                    color: o.text,
                    fontSize: "10px",
                    lineHeight: 1.45
                  },
                  children: "Add a column field for linked trends, or use the Series aggregation for field data."
                }
              )
            ] }),
            F === "field" && /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Cell display" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: [
                { value: "trend", label: "Trendline" },
                { value: "value", label: "Value" }
              ].map(({ value: h, label: R }) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => s({ displayMode: h }),
                  style: {
                    flex: 1,
                    padding: "4px 0",
                    fontSize: "11px",
                    fontWeight: 600,
                    borderRadius: "6px",
                    cursor: "pointer",
                    border: `1px solid ${(f.displayMode || "trend") === h ? o.primary : o.border}`,
                    background: (f.displayMode || "trend") === h ? `${o.primary}18` : o.headerSubtleBg || o.background,
                    color: (f.displayMode || "trend") === h ? o.primary : o.textSec
                  },
                  children: R
                },
                h
              )) }),
              /* @__PURE__ */ e.jsx("div", { style: { marginTop: "5px", fontSize: "10px", color: o.textSec, lineHeight: 1.4 }, children: (f.displayMode || "trend") === "value" ? "Show the selected summary metric from the series." : "Draw the series in the cell." })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Chart type" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: W.map(({ value: h, label: R }) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => s({ type: h }),
                  style: {
                    flex: 1,
                    padding: "4px 0",
                    fontSize: "11px",
                    fontWeight: 600,
                    borderRadius: "6px",
                    cursor: "pointer",
                    border: `1px solid ${f.type === h ? o.primary : o.border}`,
                    background: f.type === h ? `${o.primary}18` : o.headerSubtleBg || o.background,
                    color: f.type === h ? o.primary : o.textSec
                  },
                  children: R
                },
                h
              )) })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Summary metric" }),
              /* @__PURE__ */ e.jsxs(
                ne,
                {
                  size: "sm",
                  "aria-label": "Summary metric",
                  value: f.metric,
                  onChange: (h) => s({ metric: h.target.value }),
                  style: { ...P, cursor: "pointer", width: "100%" },
                  children: [
                    /* @__PURE__ */ e.jsx("option", { value: "last", children: "Last value" }),
                    /* @__PURE__ */ e.jsx("option", { value: "first", children: "First value" }),
                    /* @__PURE__ */ e.jsx("option", { value: "min", children: "Minimum" }),
                    /* @__PURE__ */ e.jsx("option", { value: "max", children: "Maximum" }),
                    /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Average" }),
                    /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                    /* @__PURE__ */ e.jsx("option", { value: "delta", children: "Delta (last − first)" })
                  ]
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Display" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px" }, children: [
                { key: "showCurrentValue", label: "Value label" },
                { key: "showDelta", label: "Delta label" },
                { key: "compact", label: "Compact cell" },
                { key: "hideColumns", label: "Hide source values" }
              ].map(({ key: h, label: R }) => /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "5px", fontSize: "11px", color: o.text, cursor: "pointer", userSelect: "none" }, children: [
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "checkbox",
                    checked: !!f[h],
                    onChange: (A) => s({ [h]: A.target.checked }),
                    style: { accentColor: o.primary, width: "13px", height: "13px", cursor: "pointer" }
                  }
                ),
                R
              ] }, h)) }),
              f.hideColumns && /* @__PURE__ */ e.jsx("div", { style: { marginTop: "6px", fontSize: "10px", color: o.textSec, background: `${o.primary}10`, border: `1px solid ${o.primary}30`, borderRadius: "6px", padding: "5px 8px", lineHeight: 1.4 }, children: "Source value columns are hidden; the trend column remains." }),
              !f.hideColumns && F === "pivot" && /* @__PURE__ */ e.jsx("div", { style: { marginTop: "6px", fontSize: "10px", color: o.textSec, background: o.headerSubtleBg || o.background, border: `1px solid ${o.border}`, borderRadius: "6px", padding: "5px 8px", lineHeight: 1.4 }, children: "Source value columns stay numeric; the trend column is added beside them." })
            ] }),
            F === "pivot" && /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Position" }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: o.textSec, lineHeight: 1.4, background: o.headerSubtleBg || o.background, border: `1px solid ${o.border}`, borderRadius: "6px", padding: "6px 8px" }, children: "Trend columns are placed before the matching value columns." })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Colors" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: [
                { key: "color", label: "Line / fill color", always: !0 },
                { key: "positiveColor", label: "Positive bar color", always: !1 },
                { key: "negativeColor", label: "Negative bar color", always: !1 }
              ].filter(({ always: h }) => h || f.type === "column" || f.type === "bar").map(({ key: h, label: R }) => /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: o.textSec, marginBottom: "3px", display: "block" }, children: R }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "5px", alignItems: "center" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "color",
                      value: f[h] || (h === "negativeColor" ? "#B91C1C" : h === "positiveColor" ? "#2F855A" : "#2563EB"),
                      onChange: (A) => s({ [h]: A.target.value }),
                      style: { width: "28px", height: "26px", padding: "1px 2px", border: `1px solid ${o.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "text",
                      value: f[h] || "",
                      onChange: (A) => s({ [h]: A.target.value }),
                      placeholder: "Auto",
                      style: { ...P, flex: 1 }
                    }
                  ),
                  f[h] && /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => s({ [h]: "" }),
                      style: { background: "none", border: "none", cursor: "pointer", color: o.textSec, fontSize: "14px", lineHeight: 1, padding: "2px" },
                      title: "Reset to auto",
                      children: "×"
                    }
                  )
                ] })
              ] }, h)) })
            ] }),
            f.type === "area" && /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Area fill opacity" }),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px" }, children: [
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "range",
                    min: "0.02",
                    max: "0.45",
                    step: "0.01",
                    value: f.areaOpacity != null ? f.areaOpacity : 0.14,
                    onChange: (h) => s({ areaOpacity: Number(h.target.value) }),
                    style: { flex: 1, accentColor: o.primary }
                  }
                ),
                /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "11px", color: o.textSec, minWidth: "32px", textAlign: "right" }, children: [
                  Math.round((f.areaOpacity != null ? f.areaOpacity : 0.14) * 100),
                  "%"
                ] })
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Column header label" }),
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  type: "text",
                  value: f.header || "",
                  onChange: (h) => s({ header: h.target.value }),
                  placeholder: "Auto (e.g. Sales Trend)",
                  style: P
                }
              )
            ] }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", justifyContent: "flex-end" }, children: /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: x,
                style: {
                  background: o.primary,
                  color: "#fff",
                  border: "none",
                  borderRadius: "7px",
                  padding: "6px 18px",
                  fontSize: "12px",
                  fontWeight: 700,
                  cursor: "pointer"
                },
                children: "Done"
              }
            ) })
          ]
        }
      )
    }
  );
}
function jt({ item: t, idx: n, setValConfigs: i, theme: o, colFields: l = [], fixedSparklineValueKeySet: x = /* @__PURE__ */ new Set(), showNotification: f }) {
  const [k, y] = u.useState(!1), F = Rr(t && t.sparkline), s = mn(t), b = !!(s && x && x.has(s)), W = u.useMemo(
    () => vt(t, l),
    [l, t]
  ), P = !!F, M = !!(F && Ar(F.source || "pivot", W)), $ = u.useCallback((T) => T.stopPropagation(), []), h = u.useCallback(() => {
    let T = !1;
    return Cr(
      i,
      n,
      /** @param {any} [current] @returns {any} */
      (U) => {
        const D = St(
          U.sparkline || Ee,
          W
        );
        return D ? (T = !0, { ...U, sparkline: D }) : U;
      }
    ), T;
  }, [W, n, i]), R = u.useCallback((T) => {
    if (T.stopPropagation(), P) {
      if (b) {
        f && f("This trend is defined by the app. You can adjust its settings, but not remove it here.", "info");
        return;
      }
      Cr(
        i,
        n,
        /** @param {any} [current] @returns {any} */
        (U) => ({ ...U, sparkline: !1 })
      ), f && f("Removed trend column.", "info");
    } else W.canUseAny && h() ? (y(!0), f && f("Trend column enabled.", "success")) : f && f("Trend needs a column field, or a Series aggregation for field data.", "warning");
  }, [W.canUseAny, P, h, n, b, i, f]), A = u.useCallback((T) => {
    if (T.stopPropagation(), !W.canUseAny) {
      f && f("Trend settings are not available for this value selection.", "warning");
      return;
    }
    h(), y(!0);
  }, [W.canUseAny, h, f]);
  return P ? /* @__PURE__ */ e.jsxs(
    "div",
    {
      "data-value-sparkline-controls": "true",
      "data-value-sparkline-available": W.canUseAny ? "true" : "false",
      "data-value-sparkline-enabled": M ? "true" : "false",
      onMouseDown: $,
      onClick: $,
      style: { marginTop: "6px", paddingTop: "6px", borderTop: `1px solid ${o.border}`, width: "100%", display: "flex", alignItems: "center", gap: "6px" },
      children: [
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "data-value-sparkline-settings": "true",
            onClick: A,
            style: {
              border: `1px solid ${o.border}`,
              background: o.headerSubtleBg || o.background,
              color: o.textSec,
              borderRadius: "6px",
              cursor: "pointer",
              padding: "3px 8px",
              fontSize: "10px",
              lineHeight: "16px",
              fontWeight: 600,
              flexShrink: 0
            },
            title: "Configure sparkline",
            children: "Settings"
          }
        ),
        b && /* @__PURE__ */ e.jsx(
          "span",
          {
            "data-value-sparkline-fixed": "true",
            style: { fontSize: "10px", color: o.textSec, flexShrink: 0 },
            title: "This trend is defined by the app",
            children: "App trend"
          }
        ),
        !b && /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "data-value-sparkline-toggle": "true",
            onClick: R,
            style: {
              border: `1px solid ${o.border}`,
              background: o.background,
              color: o.textSec,
              borderRadius: "6px",
              cursor: "pointer",
              padding: "3px 8px",
              fontSize: "10px",
              lineHeight: "16px",
              fontWeight: 600,
              flexShrink: 0
            },
            title: "Remove trend",
            children: "Remove Trend"
          }
        ),
        M && !k && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: o.primary, flex: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: [
          (t.sparkline && t.sparkline.source) === "field" ? `series ${(t.sparkline && t.sparkline.displayMode) === "value" ? "value" : "trend"}` : "linked pivot",
          " | ",
          ["line", "area", "column", "bar"].includes(t.sparkline && t.sparkline.type) ? t.sparkline.type : "line",
          " | ",
          t.sparkline && t.sparkline.metric || "last"
        ] }),
        k && /* @__PURE__ */ e.jsx(
          Rn,
          {
            item: t,
            idx: n,
            setValConfigs: i,
            theme: o,
            colFields: l,
            onClose: () => y(!1)
          }
        )
      ]
    }
  ) : null;
}
function st({ item: t, idx: n, selected: i, onSelect: o, onRemove: l, setValConfigs: x, theme: f, styles: k, dropLine: y, zoneId: F, valueSelectStyle: s, colFields: b = [], fixedSparklineValueKeySet: W = /* @__PURE__ */ new Set(), showNotification: P, serverError: M = null }) {
  const $ = i ? `${f.primary}77` : M ? "#F59E0B88" : f.border, h = i ? f.headerBg || `${f.primary}0f` : f.background || "transparent", R = String(t.formula || "").trim() || "Empty formula";
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      onClick: () => o(t.field),
      style: {
        ...k.chip,
        border: `1px solid ${$}`,
        background: h,
        borderLeft: i ? `3px solid ${f.primary}` : "3px solid transparent",
        boxShadow: "none",
        cursor: "pointer",
        flexWrap: "wrap",
        alignItems: "flex-start"
      },
      title: "Select formula",
      children: [
        y && y.zone === F && y.idx === n && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...k.dropLine, top: -2 } }),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
          /* @__PURE__ */ e.jsx(E.DragIndicator, {}),
          /* @__PURE__ */ e.jsx("span", { style: {
            fontSize: "10px",
            fontWeight: 700,
            color: f.primary,
            background: `${f.primary}18`,
            borderRadius: "999px",
            padding: "3px 7px",
            letterSpacing: "0.5px",
            flexShrink: 0
          }, children: "fx" }),
          /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0, flex: 1 }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0 }, children: [
              /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 600, display: "block", minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: t.label || "Formula" }),
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: f.textSec, fontFamily: "monospace", flexShrink: 0 }, children: J(t) }),
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "9px", color: f.textSec, border: `1px solid ${f.border}`, borderRadius: "999px", padding: "1px 6px", flexShrink: 0 }, children: ze(t.formulaScope) ? "columns" : "values" })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: {
              marginTop: "2px",
              fontSize: "10px",
              lineHeight: 1.35,
              color: f.textSec,
              fontFamily: "monospace",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis"
            }, children: [
              "=",
              R
            ] }),
            M && /* @__PURE__ */ e.jsxs("div", { style: { marginTop: "2px", fontSize: "9px", color: "#B45309" }, children: [
              "⚠ ",
              Dn[M] || M
            ] })
          ] })
        ] }),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", marginLeft: "8px", alignItems: "center", flexShrink: 0 }, children: [
          i && /* @__PURE__ */ e.jsx("span", { style: {
            fontSize: "9px",
            color: f.primary,
            background: `${f.primary}14`,
            borderRadius: "999px",
            padding: "2px 6px",
            flexShrink: 0
          }, children: "Active" }),
          /* @__PURE__ */ e.jsx(
            "span",
            {
              onClick: (A) => {
                A.stopPropagation(), l(n);
              },
              style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" },
              children: /* @__PURE__ */ e.jsx(E.Close, {})
            }
          )
        ] }),
        /* @__PURE__ */ e.jsx(
          jt,
          {
            item: t,
            idx: n,
            setValConfigs: x,
            theme: f,
            valueSelectStyle: s,
            colFields: b,
            fixedSparklineValueKeySet: W,
            showNotification: P
          }
        ),
        y && y.zone === F && y.idx === n + 1 && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...k.dropLine, bottom: -2 } })
      ]
    }
  );
}
const Dn = {
  self_reference: "Formula references itself (server)",
  circular_reference: "Circular dependency with another formula (server)",
  expression_error: "Expression could not be evaluated (server)",
  missing_reference: "Unknown field reference (server)"
};
function An({ item: t, idx: n, valConfigs: i, setValConfigs: o, theme: l, onClose: x, onRemove: f, displayedColumnOptions: k = [], availableRawFields: y = [], serverError: F = null }) {
  const s = u.useRef(null), [b, W] = u.useState({
    label: t.label || "",
    formula: t.formula || "",
    formulaRef: J(t),
    formulaScope: Ne(t.formulaScope)
  }), P = u.useRef(t);
  P.current = t, u.useEffect(() => {
    const g = P.current;
    W({
      label: g.label || "",
      formula: g.formula || "",
      formulaRef: J(g),
      formulaScope: Ne(g.formulaScope)
    });
  }, [t.field]), u.useEffect(() => {
    if (!s.current) return;
    s.current.focus();
    const g = s.current.value.length;
    s.current.setSelectionRange(g, g);
  }, [t.field]);
  const M = u.useMemo(
    () => i.filter(
      /** @param {any} [config] @returns {any} */
      (g) => g && g.field
    ),
    [i]
  ), $ = u.useMemo(
    () => M.filter(
      /** @param {any} [config] @returns {any} */
      (g) => g.agg !== "formula"
    ),
    [M]
  ), h = u.useMemo(
    () => M.filter(
      /** @param {any} [config] @returns {any} */
      (g) => g.agg === "formula" && g.field !== t.field
    ),
    [t.field, M]
  ), R = u.useMemo(
    () => (Array.isArray(k) ? k : []).filter((g) => g && g.id && g.id !== t.field),
    [k, t.field]
  ), A = u.useMemo(
    () => {
      const g = new Set(M.map(
        /** @param {any} [c] @returns {any} */
        (z) => z.field
      ).filter(Boolean));
      return (Array.isArray(y) ? y : []).filter((z) => !g.has(z));
    },
    [y, M]
  ), T = u.useMemo(
    () => jn(
      b.formula,
      M,
      t.field,
      b.formulaRef,
      b.formulaScope,
      R,
      A
    ),
    [R, b.formula, b.formulaRef, b.formulaScope, t.field, M, A]
  ), U = u.useMemo(
    () => Cn(l, T.tone),
    [T.tone, l]
  ), D = u.useMemo(
    () => ze(b.formulaScope) ? wn(R) : kn($),
    [R, b.formulaScope, $]
  ), O = u.useMemo(
    () => ze(b.formulaScope) ? at(R) || "[column_id] - [other_column_id]" : Dr($) || `${Fe(b.formulaRef || t.field)} * 100`,
    [R, b.formulaRef, b.formulaScope, t.field, $]
  ), a = b.label !== (t.label || "") || b.formula !== (t.formula || "") || b.formulaRef !== J(t) || b.formulaScope !== Ne(t.formulaScope), w = u.useCallback(() => {
    const g = Me(b.formulaRef, t.field);
    o(
      /** @param {any} [prev] @returns {any} */
      (z) => {
        const v = [...z], V = v[n];
        if (!V) return z;
        const G = J(V);
        let K = g;
        const Q = new Set(
          v.filter((Y, ee) => Y && ee !== n).map((Y) => Y.agg === "formula" ? J(Y) : Y.field).filter(Boolean).map((Y) => String(Y).toLowerCase())
        );
        if (Q.has(K.toLowerCase())) {
          let Y = 2, ee = `${K}${Y}`;
          for (; Q.has(ee.toLowerCase()); )
            Y += 1, ee = `${K}${Y}`;
          K = ee;
        }
        if (v[n] = {
          ...V,
          label: b.label,
          formula: b.formula,
          formulaRef: K,
          formulaScope: b.formulaScope
        }, G && G !== K)
          for (let Y = 0; Y < v.length; Y += 1) {
            if (Y === n) continue;
            const ee = v[Y];
            !ee || ee.agg !== "formula" || !ee.formula || (v[Y] = {
              ...ee,
              formula: hn(ee.formula, G, K)
            });
          }
        return v;
      }
    );
  }, [b.formula, b.formulaRef, b.formulaScope, b.label, n, t.field, o]), C = u.useCallback(() => {
    W({
      label: t.label || "",
      formula: t.formula || "",
      formulaRef: J(t),
      formulaScope: Ne(t.formulaScope)
    }), x();
  }, [t.field, t.formula, t.formulaRef, t.formulaScope, t.label, x]), B = u.useCallback((g) => {
    const z = s.current;
    if (!z) return;
    const v = b.formula || "", V = z.selectionStart ?? v.length, G = z.selectionEnd ?? v.length, K = `${v.slice(0, V)}${g}${v.slice(G)}`;
    W(
      /** @param {any} [prev] @returns {any} */
      (Y) => ({ ...Y, formula: K })
    );
    const Q = V + g.length;
    setTimeout(() => {
      s.current && (s.current.focus(), s.current.setSelectionRange(Q, Q));
    }, 0);
  }, [b.formula]), _ = u.useCallback(() => {
    w(), x();
  }, [w, x]), N = {
    border: `1px solid ${l.border}`,
    background: l.background,
    color: l.text,
    borderRadius: "999px",
    cursor: "pointer",
    padding: "3px 9px",
    fontSize: "10px",
    lineHeight: "16px"
  }, L = {
    border: `1px solid ${l.border}`,
    background: l.headerSubtleBg || l.background,
    color: l.text,
    borderRadius: "6px",
    cursor: "pointer",
    padding: "3px 8px",
    fontSize: "10px",
    lineHeight: "16px",
    fontFamily: "monospace"
  };
  return /* @__PURE__ */ e.jsx(
    "div",
    {
      onClick: C,
      style: {
        position: "fixed",
        inset: 0,
        zIndex: 10006,
        background: "rgba(2, 6, 23, 0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "24px"
      },
      children: /* @__PURE__ */ e.jsxs(
        "div",
        {
          onClick: (g) => g.stopPropagation(),
          onKeyDown: (g) => {
            g.key === "Escape" && C(), (g.ctrlKey || g.metaKey) && g.key === "Enter" && _();
          },
          style: {
            width: "min(760px, 92vw)",
            maxHeight: "80vh",
            overflowY: "auto",
            background: l.surfaceBg || l.background || "#fff",
            border: `1px solid ${l.border}`,
            borderRadius: l.radius || "16px",
            boxShadow: l.shadowMd || "0 18px 40px rgba(0,0,0,0.28)",
            padding: "18px"
          },
          children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px", marginBottom: "14px" }, children: [
              /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "15px", fontWeight: 800, color: l.text }, children: "Formula Editor" }),
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec }, children: "Use a stable reference key for formulas. Renaming the key updates dependent formulas." })
              ] }),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: C,
                  style: {
                    border: `1px solid ${l.border}`,
                    background: l.headerSubtleBg || l.background,
                    color: l.text,
                    borderRadius: "8px",
                    padding: "7px 10px",
                    fontSize: "11px",
                    fontWeight: 700,
                    cursor: "pointer"
                  },
                  children: "Close"
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "12px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", flex: 1, minWidth: "180px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px" }, children: "Label" }),
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    value: b.label,
                    onChange: (g) => W(
                      /** @param {any} [prev] @returns {any} */
                      (z) => ({ ...z, label: g.target.value })
                    ),
                    placeholder: "Formula label",
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.background,
                      color: l.text,
                      borderRadius: "8px",
                      padding: "8px 10px",
                      fontSize: "12px",
                      fontWeight: 600,
                      outline: "none"
                    }
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { minWidth: "180px", flex: "0 0 auto", display: "flex", flexDirection: "column", gap: "4px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px" }, children: "Reference key" }),
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    value: b.formulaRef,
                    onChange: (g) => W(
                      /** @param {any} [prev] @returns {any} */
                      (z) => ({ ...z, formulaRef: Me(g.target.value, t.field) })
                    ),
                    placeholder: "formula1",
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.headerSubtleBg || l.background,
                      color: l.text,
                      borderRadius: "8px",
                      padding: "8px 10px",
                      fontSize: "12px",
                      fontFamily: "monospace",
                      outline: "none"
                    }
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { minWidth: "180px", flex: "0 0 auto", display: "flex", flexDirection: "column", gap: "4px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px" }, children: "Formula type" }),
                /* @__PURE__ */ e.jsxs(
                  ne,
                  {
                    size: "sm",
                    "aria-label": "Formula type",
                    value: b.formulaScope,
                    onChange: (g) => {
                      const z = Ne(g.target.value);
                      W(
                        /** @param {any} [prev] @returns {any} */
                        (v) => ({
                          ...v,
                          formulaScope: z,
                          formula: v.formula || (z === "columns" ? at(R) : Dr($))
                        })
                      );
                    },
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.headerSubtleBg || l.background,
                      color: l.text,
                      borderRadius: "8px",
                      padding: "8px 10px",
                      fontSize: "12px",
                      outline: "none",
                      width: "100%"
                    },
                    children: [
                      /* @__PURE__ */ e.jsx("option", { value: "measures", children: "Value formula" }),
                      /* @__PURE__ */ e.jsx("option", { value: "columns", children: "Displayed columns" })
                    ]
                  }
                )
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "12px" }, children: [
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "4px" }, children: "Formula" }),
              /* @__PURE__ */ e.jsx(
                "textarea",
                {
                  ref: s,
                  value: b.formula,
                  onChange: (g) => W(
                    /** @param {any} [prev] @returns {any} */
                    (z) => ({ ...z, formula: g.target.value })
                  ),
                  placeholder: O,
                  rows: 4,
                  style: {
                    width: "100%",
                    boxSizing: "border-box",
                    resize: "vertical",
                    border: `1px solid ${l.border}`,
                    background: l.background,
                    color: l.text,
                    borderRadius: "10px",
                    fontFamily: "monospace",
                    fontSize: "12px",
                    lineHeight: 1.5,
                    padding: "10px 12px",
                    outline: "none"
                  }
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              /* @__PURE__ */ e.jsx("div", { style: {
                padding: "5px 9px",
                borderRadius: "999px",
                border: `1px solid ${U.border}`,
                background: U.background,
                color: U.text,
                fontSize: "10px",
                lineHeight: "14px"
              }, children: T.text }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: "Ctrl/Cmd + Enter applies" })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              D.map(
                /** @param {any} [template] @returns {any} */
                (g) => /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => W(
                      /** @param {any} [prev] @returns {any} */
                      (z) => ({ ...z, formula: g.value })
                    ),
                    style: N,
                    children: g.label
                  },
                  g.label
                )
              ),
              ["+", "-", "*", "/", "(", ")", "* 100"].map((g) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onMouseDown: (z) => z.preventDefault(),
                  onClick: () => B(g),
                  style: L,
                  children: g
                },
                g
              ))
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr", gap: "12px", marginBottom: "14px" }, children: [
              ze(b.formulaScope) ? /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Rendered Columns" }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
                  R.map(
                    /** @param {any} [column] @returns {any} */
                    (g) => /* @__PURE__ */ e.jsx(
                      "button",
                      {
                        type: "button",
                        onMouseDown: (z) => z.preventDefault(),
                        onClick: () => B(se(g.id)),
                        style: N,
                        title: `Insert ${g.id}`,
                        children: g.label || g.id
                      },
                      g.id
                    )
                  ),
                  R.length === 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: l.textSec }, children: "Render the pivot once, or type a bracketed column id directly." })
                ] })
              ] }) : /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Measures" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: $.map(
                  /** @param {any} [config] @returns {any} */
                  (g) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(Fe(g.field)),
                      style: N,
                      title: `Insert ${g.field}`,
                      children: g.field
                    },
                    g.field
                  )
                ) })
              ] }),
              !ze(b.formulaScope) && h.length > 0 && /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Formula References" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: h.map(
                  /** @param {any} [config] @returns {any} */
                  (g) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(Fe(J(g))),
                      style: N,
                      title: `${J(g)}${g.label ? ` (${g.label})` : ""}`,
                      children: J(g)
                    },
                    g.field
                  )
                ) })
              ] }),
              !ze(b.formulaScope) && A.length > 0 && /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Data Fields (auto-aggregated as sum)" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: A.map(
                  /** @param {any} [f] @returns {any} */
                  (g) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(Fe(g)),
                      style: { ...N, borderColor: `${l.primary}66`, color: l.primary, background: `${l.primary}0d` },
                      title: `Insert: ${g}`,
                      children: ke(g)
                    },
                    g
                  )
                ) })
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "space-between", gap: "8px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => {
                    f(n), x();
                  },
                  style: {
                    border: `1px solid ${l.border}`,
                    background: l.background,
                    color: l.textSec,
                    borderRadius: "8px",
                    padding: "8px 12px",
                    fontSize: "11px",
                    fontWeight: 700,
                    cursor: "pointer"
                  },
                  children: "Delete Formula"
                }
              ),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap" }, children: [
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: C,
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.background,
                      color: l.textSec,
                      borderRadius: "8px",
                      padding: "8px 12px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: "pointer"
                    },
                    children: "Cancel"
                  }
                ),
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: _,
                    disabled: !a,
                    style: {
                      border: `1px solid ${a ? `${l.primary}66` : l.border}`,
                      background: a ? `${l.primary}14` : l.background,
                      color: a ? l.primary : l.textSec,
                      borderRadius: "8px",
                      padding: "8px 12px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: a ? "pointer" : "default",
                      opacity: a ? 1 : 0.7
                    },
                    children: "Apply"
                  }
                )
              ] })
            ] })
          ]
        }
      )
    }
  );
}
function dt({
  panelId: t,
  panelLabel: n,
  size: i,
  onSizeChange: o,
  getSidebarWidth: l = null,
  onSidebarWidthChange: x = null,
  theme: f,
  outerStyle: k = {},
  contentStyle: y = {},
  children: F
}) {
  const s = u.useRef(null), b = u.useRef(null), W = u.useRef(null), P = u.useRef(null), M = u.useRef(null), $ = u.useRef(null), h = u.useRef(null), [R, A] = u.useState(null), [T, U] = u.useState(null), D = u.useMemo(() => Qt(t), [t]), O = u.useMemo(
    () => Jr(t, i),
    [t, i]
  ), a = u.useRef(O);
  a.current = O;
  const w = u.useMemo(() => R ? Jr(t, {
    width: R.width !== void 0 ? R.width : O.width,
    height: R.height !== void 0 ? R.height : O.height
  }) : O, [R, t, O]), C = u.useCallback((L = !0) => {
    if (b.current) {
      const g = b.current;
      b.current = null, g(L);
    }
  }, []);
  u.useEffect(() => (() => C(!1)), [C]);
  const B = u.useCallback((L, g) => {
    if (!s.current || typeof o != "function") return;
    L.preventDefault(), L.stopPropagation(), C(!1);
    const z = s.current.getBoundingClientRect(), v = s.current.parentElement ? s.current.parentElement.getBoundingClientRect() : z, V = typeof l == "function" ? Number(l()) : NaN, G = Number.isFinite(V) ? Math.max(200, Math.min(600, Math.round(V))) : null, K = G !== null ? Math.max(0, Math.round(G - v.width)) : 0, Q = typeof x == "function" && G !== null;
    G !== null && (a.current.width === null || $.current === null) && ($.current = G, h.current = K);
    const Y = G !== null ? $.current !== null ? Math.min(G, $.current) : G : null, ee = h.current !== null ? h.current : K, $r = Q ? Math.max(D.minWidth, Math.min(D.maxWidth, 600 - ee)) : Math.max(D.minWidth, Math.min(D.maxWidth, Math.floor(v.width))), le = {
      startX: L.clientX,
      startY: L.clientY,
      startWidth: z.width,
      startHeight: z.height,
      startParentWidth: Math.floor(v.width),
      startSidebarWidth: G,
      baseSidebarWidth: Y,
      chromeWidth: ee,
      canExpandSidebar: Q,
      maxWidth: $r,
      initialWidthProp: a.current.width
    }, we = document.body.style.cursor, _e = document.body.style.userSelect;
    document.body.style.cursor = g === "both" ? "nwse-resize" : g === "width" ? "ew-resize" : "ns-resize", document.body.style.userSelect = "none", P.current = null, M.current = null, U(g);
    const Ce = () => {
      W.current = null, P.current && A(P.current), M.current !== null && typeof x == "function" && (x(M.current), M.current = null);
    }, fe = (ge) => {
      const ye = ge.clientX - le.startX, ve = ge.clientY - le.startY, Ue = {};
      if (g === "width" || g === "both" && (le.initialWidthProp !== null || Math.abs(ye) >= 3)) {
        const Se = Math.max(D.minWidth, Math.min(le.maxWidth, Math.round(le.startWidth + ye)));
        if (Ue.width = Se, s.current && (s.current.style.width = `${Se}px`), le.canExpandSidebar) {
          const ur = le.baseSidebarWidth !== null ? le.baseSidebarWidth : le.startSidebarWidth, pr = Math.max(
            ur,
            Math.min(600, Se + le.chromeWidth)
          );
          M.current = pr;
        }
      }
      if (g === "height" || g === "both") {
        const Se = Math.max(D.minHeight, Math.min(D.maxHeight, Math.round(le.startHeight + ve)));
        Ue.height = Se, s.current && (s.current.style.height = `${Se}px`);
      }
      Object.keys(Ue).length !== 0 && (P.current = Ue, typeof window < "u" && typeof window.requestAnimationFrame == "function" ? W.current === null && (W.current = window.requestAnimationFrame(Ce)) : Ce());
    }, Re = (ge = !0) => {
      W.current !== null && typeof window < "u" && typeof window.cancelAnimationFrame == "function" && (window.cancelAnimationFrame(W.current), W.current = null), document.body.style.cursor = we, document.body.style.userSelect = _e, window.removeEventListener("mousemove", fe), window.removeEventListener("mouseup", ce), window.removeEventListener("blur", ce);
      const ye = P.current, ve = M.current;
      P.current = null, M.current = null, U(null), A(null), ge ? (ve !== null && typeof x == "function" && x(ve), ye && o(t, ye)) : s.current && (s.current.style.width = a.current.width === null ? "100%" : `${a.current.width}px`, s.current.style.height = `${a.current.height}px`);
    }, ce = () => C(!0);
    b.current = Re, window.addEventListener("mousemove", fe), window.addEventListener("mouseup", ce), window.addEventListener("blur", ce);
  }, [
    l,
    D.maxHeight,
    D.maxWidth,
    D.minHeight,
    D.minWidth,
    x,
    o,
    t,
    C
  ]), _ = u.useCallback((L, g) => {
    g && (g.preventDefault(), g.stopPropagation()), C(!1), typeof o == "function" && (L === "width" ? (o(t, { width: null }), $.current !== null && typeof x == "function" && (x($.current), $.current = null, h.current = null)) : L === "height" ? o(t, { height: null }) : (o(t, { width: null, height: null }), $.current !== null && typeof x == "function" && (x($.current), $.current = null, h.current = null)));
  }, [x, o, t, C]), N = `${f.primary}55`;
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      ref: s,
      style: {
        position: "relative",
        flexShrink: 0,
        width: w.width === null ? "100%" : `${w.width}px`,
        maxWidth: "100%",
        height: `${w.height}px`,
        minHeight: `${D.minHeight}px`,
        maxHeight: `${D.maxHeight}px`,
        boxSizing: "border-box",
        overflow: "hidden",
        ...k
      },
      children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            style: {
              width: "100%",
              height: "100%",
              overflow: "auto",
              boxSizing: "border-box",
              ...y
            },
            children: F
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": T === "width" ? "true" : void 0,
            onMouseDown: (L) => B(L, "width"),
            onDoubleClick: (L) => _("width", L),
            style: {
              position: "absolute",
              top: 0,
              right: 0,
              bottom: "12px",
              width: "10px",
              cursor: "ew-resize",
              zIndex: 4,
              touchAction: "none",
              display: "flex",
              alignItems: "center",
              justifyContent: "flex-end",
              paddingRight: "2px"
            },
            title: `Resize ${n} width`,
            children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "2px", height: "24px", borderRadius: "999px", background: N, opacity: 0.45, pointerEvents: "none" } })
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": T === "height" ? "true" : void 0,
            onMouseDown: (L) => B(L, "height"),
            onDoubleClick: (L) => _("height", L),
            style: {
              position: "absolute",
              left: 0,
              right: "12px",
              bottom: 0,
              height: "10px",
              cursor: "ns-resize",
              zIndex: 4,
              touchAction: "none",
              display: "flex",
              alignItems: "flex-end",
              justifyContent: "center",
              paddingBottom: "2px"
            },
            title: `Resize ${n} height`,
            children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "28px", height: "2px", borderRadius: "999px", background: N, opacity: 0.45, pointerEvents: "none" } })
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": T === "both" ? "true" : void 0,
            onMouseDown: (L) => B(L, "both"),
            onDoubleClick: (L) => _("both", L),
            style: {
              position: "absolute",
              right: 0,
              bottom: 0,
              width: "14px",
              height: "14px",
              cursor: "nwse-resize",
              zIndex: 5,
              touchAction: "none"
            },
            title: `Resize ${n}`,
            children: /* @__PURE__ */ e.jsx(
              "div",
              {
                className: "sp-resize-pill",
                "aria-hidden": "true",
                style: {
                  position: "absolute",
                  right: "4px",
                  bottom: "4px",
                  width: "3px",
                  height: "3px",
                  borderRadius: "999px",
                  background: N,
                  opacity: 0.55,
                  boxShadow: `-4px 0 0 ${N}, 0 -4px 0 ${N}, -4px -4px 0 ${N}`,
                  pointerEvents: "none"
                }
              }
            )
          }
        )
      ]
    }
  );
}
function ct({ measureAxis: t, setMeasureAxis: n, theme: i, valueSelectStyle: o, regularValueCount: l }) {
  const x = u.useMemo(() => Je(t), [t]), f = u.useCallback((y) => {
    typeof n == "function" && n(
      /** @param {any} [previous] @returns {any} */
      (F) => ({
        ...Je(F),
        ...typeof y == "function" ? y(Je(F)) : y
      })
    );
  }, [n]), k = {
    ...o,
    width: "112px",
    padding: "3px 6px",
    fontSize: "10px"
  };
  return /* @__PURE__ */ e.jsxs("div", { style: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    flexWrap: "wrap",
    padding: "6px",
    border: `1px solid ${i.border}`,
    borderRadius: i.radiusSm || "6px",
    background: i.surfaceInset || i.headerSubtleBg || i.background
  }, children: [
    /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: i.textSec, fontWeight: 700, textTransform: "uppercase" }, children: "Values as" }),
    /* @__PURE__ */ e.jsxs(
      ne,
      {
        size: "sm",
        value: x.placement,
        onChange: (y) => f({ placement: y.target.value }),
        style: { ...o, width: "104px" },
        title: "Values placement",
        children: [
          /* @__PURE__ */ e.jsx("option", { value: "none", children: "Columns" }),
          /* @__PURE__ */ e.jsx("option", { value: "rows", children: "Rows" }),
          /* @__PURE__ */ e.jsx("option", { value: "columns", children: "Column Axis" })
        ]
      }
    ),
    x.placement !== "none" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsx(
        "input",
        {
          value: x.labelField,
          onChange: (y) => f({ labelField: y.target.value }),
          style: k,
          title: "Measure label field"
        }
      ),
      /* @__PURE__ */ e.jsx(
        "input",
        {
          value: x.valueField,
          onChange: (y) => f({ valueField: y.target.value }),
          style: { ...k, width: "82px" },
          title: "Measure value field"
        }
      ),
      /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "4px", fontSize: "10px", color: i.textSec }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "checkbox",
            checked: x.suppressEmptyMembers,
            onChange: (y) => f({ suppressEmptyMembers: y.target.checked })
          }
        ),
        "Empty"
      ] }),
      /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "4px", fontSize: "10px", color: i.textSec }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "checkbox",
            checked: x.suppressZeroMembers,
            onChange: (y) => f({ suppressZeroMembers: y.target.checked })
          }
        ),
        "Zero"
      ] })
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: i.textSec }, children: [
      l,
      " selected"
    ] })
  ] });
}
const En = u.memo(
  /** @param {any} p0 @returns {any} */
  function({
    sidebarTab: n,
    setSidebarTab: i,
    rowFields: o,
    setRowFields: l,
    colFields: x,
    setColFields: f,
    valConfigs: k,
    setValConfigs: y,
    measureAxis: F = {},
    setMeasureAxis: s = null,
    columnVisibility: b,
    setColumnVisibility: W,
    columnPinning: P,
    setColumnPinning: M,
    availableFields: $,
    table: h,
    pinningPresets: R,
    showNotification: A,
    filterAnchorEl: T,
    setFilterAnchorEl: U,
    colSearch: D,
    setColSearch: O,
    colTypeFilter: a,
    setColTypeFilter: w,
    selectedCols: C,
    setSelectedCols: B,
    dropLine: _,
    onDragStart: N,
    onDragOver: L,
    onDrop: g,
    onDragEnd: z,
    onKeyboardFieldDrop: v,
    handleHeaderFilter: V,
    handleFilterClick: G,
    requestFilterOptions: K,
    handleExpandAllRows: Q,
    handlePinColumn: Y,
    toggleAllColumnsPinned: ee,
    activeFilterCol: $r,
    closeFilterPopover: le,
    filterOptions: we,
    filterOptionMeta: _e = {},
    clientFilterOptionMap: Ce = {},
    data: fe,
    sidebarWidth: Re,
    setSidebarWidth: ce,
    fieldPanelSizes: ge,
    setFieldPanelSizes: ye,
    fixedSparklineValueKeys: ve = [],
    sparklineFields: Ue = null,
    displayedColumnOptions: Se = [],
    reportEditorSelection: ur = null,
    showReportConfigColumn: pr = !0,
    setShowReportConfigColumn: kt = null,
    formulaErrors: We = {},
    serverSide: Qe = !1
  }) {
    Kt("SidebarPanel");
    const { theme: d, styles: Z } = Yt(), {
      filters: ue,
      setFilters: xr,
      pivotMode: wt,
      reportDef: Ct,
      setReportDef: Rt,
      savedReports: Dt,
      setSavedReports: At,
      activeReportId: $t,
      setActiveReportId: zt,
      customDimensions: Ve,
      setCustomDimensions: zr
    } = Zt(), [er, Fr] = u.useState({ columnId: null, anchorEl: null }), [Te, Ie] = u.useState(null), [be, De] = u.useState(null), [Ae, fr] = u.useState("report"), rr = u.useRef(null), [Ft, tr] = u.useState(null), pe = u.useRef(null), Er = u.useRef(0), Ke = u.useRef(null), he = u.useRef(null), [q, gr] = u.useState(null), [yr, Mr] = u.useState(""), br = u.useMemo(
      () => new Set(Array.isArray(ve) ? ve : []),
      [ve]
    ), re = u.useCallback(
      (r) => Gt(r, Ve),
      [Ve]
    ), _r = u.useMemo(
      () => Je(F),
      [F]
    ), je = _r.labelField || "Measure Name", Be = u.useMemo(
      () => ($ || []).filter(
        /** @param {any} [field] @returns {any} */
        (r) => r
      ),
      [$]
    ), te = u.useMemo(
      () => Be.filter(
        /** @param {any} [field] @returns {any} */
        (r) => r && !ut(r)
      ),
      [Be]
    ), Ye = Array.isArray(k) && k.length > 0 && je && !Be.includes(je), Le = u.useMemo(
      () => Ye ? [je, ...Be] : Be,
      [Be, je, Ye]
    ), Ze = yr.trim().toLowerCase(), nr = u.useCallback((r) => {
      if (!Ze) return !0;
      const p = Ye && r === je ? "measure name measure names metric metrics value values" : "";
      return String(r || "").toLowerCase().includes(Ze) || String(re(r) || "").toLowerCase().includes(Ze) || p.includes(Ze);
    }, [re, je, Ze, Ye]), hr = u.useMemo(
      () => Le.length > 8 ? Le.filter(nr) : Le,
      [Le, nr]
    ), mr = u.useMemo(
      () => te.length > 8 ? te.filter(nr) : te,
      [te, nr]
    ), Wr = u.useCallback((r) => {
      l(
        /** @param {any} [prev] @returns {any} */
        (p) => (p || []).filter(
          /** @param {any} [item] @returns {any} */
          (c) => c !== r
        )
      ), f(
        /** @param {any} [prev] @returns {any} */
        (p) => (p || []).filter(
          /** @param {any} [item] @returns {any} */
          (c) => c !== r
        )
      ), y(
        /** @param {any} [prev] @returns {any} */
        (p) => (p || []).filter(
          /** @param {any} [item] @returns {any} */
          (c) => !(c && c.field === r)
        )
      ), xr(
        /** @param {any} [prev] @returns {any} */
        (p) => {
          if (!p || !Object.prototype.hasOwnProperty.call(p, r)) return p;
          const c = { ...p };
          return delete c[r], c;
        }
      );
    }, [f, xr, l, y]), Et = u.useCallback((r) => {
      const p = pe.current;
      if (!p) return;
      const c = 60, m = 12, S = p.getBoundingClientRect(), I = r.clientY, X = I - S.top, j = S.bottom - I;
      let H = 0;
      if (X < c && X >= 0 ? H = -1 : j < c && j >= 0 && (H = 1), H === 0) {
        he.current && (cancelAnimationFrame(he.current), he.current = null);
        return;
      }
      const oe = () => {
        pe.current && (pe.current.scrollTop += H * m, he.current = requestAnimationFrame(oe));
      };
      he.current || (he.current = requestAnimationFrame(oe));
    }, []), Tr = u.useCallback(() => {
      he.current && (cancelAnimationFrame(he.current), he.current = null);
    }, []), xe = u.useMemo(
      () => k.filter(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg === "formula"
      ),
      [k]
    ), $e = u.useMemo(
      () => xe.length,
      [xe]
    ), Pe = u.useMemo(
      () => k.filter(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg !== "formula"
      ).length,
      [k]
    ), Mt = u.useMemo(() => [
      { id: "rows", label: "Rows", icon: /* @__PURE__ */ e.jsx(E.List, {}) },
      { id: "cols", label: "Columns", icon: /* @__PURE__ */ e.jsx(E.Columns, {}) },
      { id: "vals", label: "Values", icon: /* @__PURE__ */ e.jsx(E.Sigma, {}) },
      { id: "filter", label: "Filters", icon: /* @__PURE__ */ e.jsx(E.Filter, {}) }
    ], []), Ge = u.useMemo(() => ({
      filter: Object.keys(ue || {}).filter((r) => r !== "global"),
      rows: o || [],
      cols: x || [],
      vals: k || []
    }), [x, ue, o, k]), lr = u.useCallback(() => {
      q && A && A("Keyboard move cancelled.", "info"), gr(null);
    }, [q, A]), Ir = u.useCallback((r, p, c, m) => {
      if (typeof v != "function") return;
      gr({ field: r, zone: p, idx: c, label: m }), A && A(`Picked up ${m}. Focus a target field or zone and press Enter.`, "info");
    }, [v, A]), or = u.useCallback((r, p = null) => {
      if (!q || typeof v != "function") return !1;
      const c = Ge[r] || [], m = Number(p), S = Number.isFinite(m) ? Math.max(0, Math.min(Math.floor(m), c.length)) : c.length, I = v({
        field: q.field,
        srcZone: q.zone,
        srcIdx: q.idx,
        targetZone: r,
        targetIdx: S,
        source: "layout:keyboard-drop"
      });
      return I !== !1 && (A && A(`Moved ${q.label}.`, "success"), gr(null)), I !== !1;
    }, [q, v, A, Ge]), Br = u.useCallback((r, p, c, m, S) => {
      if (!it(r.target)) {
        if (r.key === "Escape") {
          r.preventDefault(), r.stopPropagation(), lr();
          return;
        }
        r.key !== "Enter" && r.key !== " " || (r.preventDefault(), r.stopPropagation(), q ? or(c, m) : Ir(p, c, m, S));
      }
    }, [lr, or, q, Ir]), _t = u.useCallback((r, p) => {
      if (!it(r.target)) {
        if (r.key === "Escape") {
          r.preventDefault(), r.stopPropagation(), lr();
          return;
        }
        !q || r.key !== "Enter" && r.key !== " " || (r.preventDefault(), r.stopPropagation(), or(p, (Ge[p] || []).length));
      }
    }, [lr, or, q, Ge]), Wt = u.useCallback((r) => {
      r.preventDefault();
      const p = r.clientX, c = Re || 288;
      Ke.current = { startX: p, startWidth: c };
      const m = document.body.style.cursor, S = document.body.style.userSelect;
      document.body.style.cursor = "col-resize", document.body.style.userSelect = "none";
      const I = (j) => {
        if (!Ke.current) return;
        const H = j.clientX - Ke.current.startX, oe = Math.max(200, Math.min(600, Ke.current.startWidth + H));
        ce(oe);
      }, X = () => {
        Ke.current = null, document.body.style.cursor = m, document.body.style.userSelect = S, window.removeEventListener("mousemove", I), window.removeEventListener("mouseup", X), window.removeEventListener("blur", X);
      };
      window.addEventListener("mousemove", I), window.addEventListener("mouseup", X), window.addEventListener("blur", X);
    }, [Re, ce]), Lr = u.useCallback(() => pe.current ? pe.current.getBoundingClientRect().width : Re || 288, [Re]), Pr = u.useCallback((r) => {
      if (typeof ce != "function") return;
      const p = Math.max(200, Math.min(600, Math.round(r)));
      ce(
        /** @param {any} [prev] @returns {any} */
        (c) => c === p ? c : p
      );
    }, [ce]), Or = u.useCallback((r, p) => {
      typeof ye == "function" && ye(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = Xt(c, r, p), S = c && c[r] ? c[r] : null, I = m[r];
          return S && S.width === I.width && S.height === I.height ? c : m;
        }
      );
    }, [ye]), Hr = Z.dropZone && Z.dropZone.padding || "8px", Nr = u.useMemo(() => {
      const r = { ...Z.dropZone || {} };
      return delete r.minHeight, delete r.maxHeight, delete r.overflowY, delete r.padding, delete r.border, delete r.boxShadow, r;
    }, [Z.dropZone]), Ur = u.useCallback((r) => {
      if (we && we[r]) return we[r];
      if (Ce && Ce[r]) return Ce[r];
      if (!h || !h.getColumn) return [];
      if (!h.getColumn(r)) return [];
      const c = /* @__PURE__ */ new Set();
      return (h.getCoreRowModel ? h.getCoreRowModel().rows : []).forEach(
        /** @param {any} [row] @returns {any} */
        (S) => {
          const I = S.getValue(r);
          I != null && I !== "" && c.add(I);
        }
      ), Array.from(c).sort((S, I) => String(S).localeCompare(String(I), void 0, { numeric: !0, sensitivity: "base" }));
    }, [Ce, we, h]), me = u.useMemo(() => ({
      border: `1px solid ${d.border}`,
      background: d.headerSubtleBg || d.surfaceInset || d.background,
      color: d.text,
      cursor: "pointer",
      borderRadius: d.radiusSm || "8px",
      fontSize: "9px",
      lineHeight: 1.2,
      padding: "1px 4px",
      minHeight: "20px",
      outline: "none"
    }), [d.background, d.border, d.headerSubtleBg, d.radiusSm, d.surfaceInset, d.text]), Vr = u.useCallback((r, p) => {
      y(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = [...c], S = m[r];
          if (!S) return c;
          if (qr(p) && !S.weightField) {
            const I = Array.isArray(te) && te.length > 0 ? te.find((H) => H !== S.field) || te[0] : "", X = typeof window < "u" ? window.prompt(
              `Weight field for weighted average of ${ke(S.field)}`,
              S.weightField || I || ""
            ) : S.weightField || I || "", j = typeof X == "string" ? X.trim() : "";
            return j ? Array.isArray(te) && te.length > 0 && !te.includes(j) ? (A && A(`Unknown weight field: ${j}`, "warning"), c) : (m[r] = { ...S, agg: p, weightField: j }, m) : (A && A("Weighted average requires a weight field.", "warning"), c);
          }
          return m[r] = { ...S, agg: p }, m;
        }
      );
    }, [te, y, A]), Tt = u.useCallback((r, p) => {
      if (!p) {
        A && A("Weighted average requires a weight field.", "warning");
        return;
      }
      y(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = [...c], S = m[r];
          return S ? (m[r] = { ...S, weightField: p }, m) : c;
        }
      );
    }, [y, A]), It = u.useCallback((r, p) => {
      y(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = [...c], S = m[r];
          return S ? (m[r] = {
            ...S,
            windowFn: p === "none" ? null : p
          }, m) : c;
        }
      );
    }, [y]), Kr = u.useCallback(() => {
      const r = Sn(k), p = k.filter(
        /** @param {any} [config] @returns {any} */
        (I) => I && I.agg !== "formula"
      ), c = `Formula ${$e + 1}`, m = Dr(p), S = bn(c, k, r);
      Ie(r), De(r), y(
        /** @param {any} [prev] @returns {any} */
        (I) => [
          ...I,
          {
            field: r,
            agg: "formula",
            label: c,
            formula: m,
            formulaRef: S,
            formulaScope: "measures"
          }
        ]
      ), A && (p.length === 0 ? A("Added a formula column. Add at least one regular value measure to reference in it.", "warning") : A(`Added ${c}.`, "success"));
    }, [$e, De, y, Ie, A, k]), Bt = (r, p) => {
      r.stopPropagation(), pe.current && (Er.current = pe.current.scrollTop), Fr(
        /** @param {any} [prev] @returns {any} */
        (c) => c.columnId === p ? { columnId: null, anchorEl: null } : { columnId: p, anchorEl: r.currentTarget }
      ), K && K(p);
    }, Lt = () => Fr({ columnId: null, anchorEl: null });
    u.useEffect(() => {
      pe.current && er.columnId && (pe.current.scrollTop = Er.current);
    }, [we, er.columnId]), u.useEffect(() => {
      if (xe.length === 0) {
        Te !== null && Ie(null), be !== null && De(null);
        return;
      }
      xe.some(
        /** @param {any} [config] @returns {any} */
        (p) => p.field === Te
      ) || Ie(xe[xe.length - 1].field), be && !xe.some(
        /** @param {any} [config] @returns {any} */
        (p) => p.field === be
      ) && De(null);
    }, [Te, xe, be]);
    const vr = u.useMemo(
      () => k.findIndex(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg === "formula" && r.field === be
      ),
      [be, k]
    ), Sr = vr >= 0 ? k[vr] : null, Yr = u.useCallback((r) => {
      Ie(r), De(r);
    }, []), Xe = u.useCallback((r) => {
      const p = k[r] && k[r].agg === "formula" ? k[r].field : null;
      if (y(
        /** @param {any} [prev] @returns {any} */
        (c) => c.filter(
          /** @param {any} [_] @param {any} [index] @returns {any} */
          (m, S) => S !== r
        )
      ), p && p === Te) {
        const c = xe.find(
          /** @param {any} [config] @returns {any} */
          (m) => m.field !== p
        );
        Ie(c ? c.field : null);
      }
      p && p === be && De(null);
    }, [Te, xe, be, y, k]), Oe = wt === "report", Zr = Math.max(Re || 288, Oe ? 380 : 0), Gr = (r, p) => {
      if (r <= 8) return null;
      const c = yr.trim().length > 0;
      return /* @__PURE__ */ e.jsxs("div", { style: {
        display: "flex",
        alignItems: "center",
        gap: "6px",
        margin: "0 2px 8px",
        padding: "5px 8px",
        border: `1px solid ${d.border}`,
        borderRadius: d.radiusSm || "8px",
        background: d.headerSubtleBg || d.surfaceInset || d.background
      }, children: [
        /* @__PURE__ */ e.jsx(E.Search, { style: { flexShrink: 0, color: d.textSec } }),
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "search",
            "aria-label": "Search available fields",
            placeholder: `Search ${r} fields...`,
            value: yr,
            onChange: (m) => Mr(m.target.value),
            style: {
              flex: 1,
              minWidth: 0,
              border: "none",
              outline: "none",
              background: "transparent",
              color: d.text,
              fontSize: "12px"
            }
          }
        ),
        c && /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "aria-label": "Clear available field search",
            title: "Clear search",
            onClick: () => Mr(""),
            style: {
              border: "none",
              background: "transparent",
              color: d.textSec,
              cursor: "pointer",
              padding: 0,
              display: "flex",
              alignItems: "center"
            },
            children: /* @__PURE__ */ e.jsx(E.Close, {})
          }
        ),
        c && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec, whiteSpace: "nowrap" }, children: [
          p,
          "/",
          r
        ] })
      ] });
    };
    return /* @__PURE__ */ e.jsxs("div", { style: { position: "relative", display: "flex", flexShrink: 0 }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: pe, style: { ...Z.sidebar, width: `${Zr}px`, minWidth: `${Zr}px` }, role: "complementary", "aria-label": "Tool Panel", onDragOver: Et, onDragLeave: Tr, onDrop: Tr, children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            id: "pivot-sidebar-keyboard-dnd-help",
            style: { position: "absolute", width: 1, height: 1, padding: 0, margin: -1, overflow: "hidden", clip: "rect(0 0 0 0)", whiteSpace: "nowrap", border: 0 },
            children: "Press Enter or Space on a field to pick it up. Focus a target field or zone and press Enter or Space to drop it. Press Escape to cancel."
          }
        ),
        Oe ? /* @__PURE__ */ e.jsxs("div", { role: "tablist", "aria-label": "Report Panel Tabs", style: { position: "relative", display: "grid", gridTemplateColumns: "repeat(3, 1fr)", borderBottom: `1px solid ${d.border}`, marginBottom: "16px" }, children: [
          [
            { id: "report", label: "Builder", icon: /* @__PURE__ */ e.jsx(E.Report, {}) },
            { id: "fields", label: "Fields", icon: /* @__PURE__ */ e.jsx(E.Sigma, {}) },
            { id: "columns", label: "Columns", icon: /* @__PURE__ */ e.jsx(E.Columns, {}) }
          ].map((r, p, c) => /* @__PURE__ */ e.jsxs(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": Ae === r.id,
              className: "sp-tab",
              onClick: () => fr(r.id),
              onKeyDown: (m) => {
                if (m.key === "Enter" || m.key === " ")
                  m.preventDefault(), fr(r.id);
                else if (m.key === "ArrowRight" || m.key === "ArrowLeft") {
                  m.preventDefault();
                  const S = m.key === "ArrowRight" ? 1 : -1, I = c[(p + S + c.length) % c.length];
                  I && fr(I.id);
                }
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                justifyContent: "center",
                fontWeight: Ae === r.id ? 600 : 400,
                color: Ae === r.id ? d.primary : d.textSec,
                display: "flex",
                alignItems: "center",
                gap: "5px",
                fontSize: "13px"
              },
              children: [
                r.icon,
                r.label
              ]
            },
            r.id
          )),
          /* @__PURE__ */ e.jsx(
            "div",
            {
              "aria-hidden": "true",
              style: {
                position: "absolute",
                bottom: "-1px",
                left: 0,
                width: "33.3333%",
                height: "2px",
                borderRadius: "2px 2px 0 0",
                background: d.primary,
                transform: `translateX(${Math.max(0, ["report", "fields", "columns"].indexOf(Ae)) * 100}%)`,
                transition: "transform 200ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))",
                pointerEvents: "none"
              }
            }
          )
        ] }) : /* @__PURE__ */ e.jsxs("div", { role: "tablist", "aria-label": "Tool Panel Tabs", style: { position: "relative", display: "grid", gridTemplateColumns: "repeat(3, 1fr)", borderBottom: `1px solid ${d.border}`, marginBottom: "16px" }, children: [
          /* @__PURE__ */ e.jsx(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": n === "fields",
              className: "sp-tab",
              onClick: () => i("fields"),
              onKeyDown: (r) => {
                r.key === "Enter" || r.key === " " ? (r.preventDefault(), i("fields")) : r.key === "ArrowRight" ? (r.preventDefault(), i("filters")) : r.key === "ArrowLeft" && (r.preventDefault(), i("columns"));
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                textAlign: "center",
                fontWeight: n === "fields" ? 600 : 400,
                color: n === "fields" ? d.primary : d.textSec
              },
              children: "Fields"
            }
          ),
          /* @__PURE__ */ e.jsxs(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": n === "filters",
              className: "sp-tab",
              onClick: () => i("filters"),
              onKeyDown: (r) => {
                r.key === "Enter" || r.key === " " ? (r.preventDefault(), i("filters")) : r.key === "ArrowRight" ? (r.preventDefault(), i("columns")) : r.key === "ArrowLeft" && (r.preventDefault(), i("fields"));
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                fontWeight: n === "filters" ? 600 : 400,
                color: n === "filters" ? d.primary : d.textSec,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "6px"
              },
              children: [
                "Filters",
                Object.keys(ue).length > 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "6px", height: "6px", borderRadius: "50%", background: "#d32f2f" } })
              ]
            }
          ),
          /* @__PURE__ */ e.jsx(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": n === "columns",
              className: "sp-tab",
              onClick: () => i("columns"),
              onKeyDown: (r) => {
                r.key === "Enter" || r.key === " " ? (r.preventDefault(), i("columns")) : r.key === "ArrowRight" ? (r.preventDefault(), i("fields")) : r.key === "ArrowLeft" && (r.preventDefault(), i("filters"));
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                textAlign: "center",
                fontWeight: n === "columns" ? 600 : 400,
                color: n === "columns" ? d.primary : d.textSec
              },
              children: "Columns"
            }
          ),
          /* @__PURE__ */ e.jsx(
            "div",
            {
              "aria-hidden": "true",
              style: {
                position: "absolute",
                bottom: "-1px",
                left: 0,
                width: "33.3333%",
                height: "2px",
                borderRadius: "2px 2px 0 0",
                background: d.primary,
                transform: `translateX(${Math.max(0, ["fields", "filters", "columns"].indexOf(n)) * 100}%)`,
                transition: "transform 200ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))",
                pointerEvents: "none"
              }
            }
          )
        ] }),
        Oe && Ae === "report" ? /* @__PURE__ */ e.jsx("div", { style: { flex: 1, overflowY: "auto", padding: "0 12px 12px" }, children: /* @__PURE__ */ e.jsx(
          yn,
          {
            reportDef: Ct,
            setReportDef: Rt,
            availableFields: $,
            customDimensions: Ve,
            theme: d,
            styles: Z,
            data: fe,
            valConfigs: k,
            colFields: x,
            setColFields: f,
            setValConfigs: y,
            savedReports: Dt,
            setSavedReports: At,
            activeReportId: $t,
            setActiveReportId: zt,
            showNotification: A,
            requestedSelection: ur,
            showReportConfigColumn: pr,
            setShowReportConfigColumn: kt
          }
        ) }) : Oe && Ae === "fields" ? /* @__PURE__ */ e.jsxs("div", { style: { padding: "0 4px" }, children: [
          /* @__PURE__ */ e.jsx(
            lt,
            {
              availableFields: $,
              customDimensions: Ve,
              setCustomDimensions: zr,
              data: fe,
              theme: d,
              showNotification: A,
              getFieldLabel: re,
              onDeleteField: Wr
            }
          ),
          /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: d.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px", padding: "0 2px" }, children: [
            /* @__PURE__ */ e.jsx(E.Database, { style: { verticalAlign: "middle", marginRight: "4px" } }),
            "Available Fields — click to add"
          ] }),
          Gr(te.length, mr.length),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", marginBottom: "14px" }, children: [
            mr.map(
              /** @param {any} [f] @returns {any} */
              (r) => {
                const p = k.some(
                  /** @param {any} [c] @returns {any} */
                  (c) => c && c.field === r && c.agg !== "formula"
                );
                return /* @__PURE__ */ e.jsxs(
                  "div",
                  {
                    onClick: () => {
                      p || y(
                        /** @param {any} [prev] @returns {any} */
                        (c) => [...c, { field: r, agg: "sum", label: re(r) }]
                      );
                    },
                    style: {
                      padding: "3px 9px",
                      fontSize: "11px",
                      lineHeight: 1.4,
                      fontWeight: 500,
                      borderRadius: "999px",
                      border: `1px solid ${p || d.isDark ? d.border : "#D7DDEA"}`,
                      background: p ? d.select || d.headerBg : d.isDark ? d.background : "#ffffff",
                      color: p ? d.primary : d.text,
                      cursor: p ? "default" : "pointer",
                      opacity: p ? 0.6 : 1,
                      userSelect: "none"
                    },
                    title: p ? `${re(r)} already in values` : `Add ${re(r)} to values`,
                    children: [
                      re(r),
                      p ? " ✓" : ""
                    ]
                  },
                  r
                );
              }
            ),
            te.length > 8 && mr.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${d.border}`, paddingTop: "10px", marginBottom: "8px" }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 2px 8px 2px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsx("button", { type: "button", onClick: Kr, style: { border: `1px solid ${d.primary}66`, background: `${d.primary}12`, color: d.primary, borderRadius: "999px", cursor: "pointer", padding: "4px 10px", fontSize: "11px", fontWeight: 700 }, children: "+ Add Formula" }),
              /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
                Pe,
                " value",
                Pe === 1 ? "" : "s",
                " · ",
                $e,
                " formula",
                $e === 1 ? "" : "s"
              ] })
            ] }),
            /* @__PURE__ */ e.jsx(
              ct,
              {
                measureAxis: F,
                setMeasureAxis: s,
                theme: d,
                valueSelectStyle: me,
                regularValueCount: Pe
              }
            )
          ] }),
          k.map(
            /** @param {any} [item] @param {any} [idx] @returns {any} */
            (r, p) => {
              const c = r && r.agg === "formula";
              if (!r) return null;
              const m = Ft === p;
              return /* @__PURE__ */ e.jsx(
                "div",
                {
                  draggable: !0,
                  onDragStart: () => {
                    rr.current = p;
                  },
                  onDragOver: (S) => {
                    S.preventDefault(), tr(p);
                  },
                  onDragLeave: () => tr(null),
                  onDrop: (S) => {
                    S.preventDefault();
                    const I = rr.current;
                    tr(null), rr.current = null, !(I === null || I === p) && y(
                      /** @param {any} [prev] @returns {any} */
                      (X) => {
                        const j = [...X], [H] = j.splice(I, 1);
                        return j.splice(p, 0, H), j;
                      }
                    );
                  },
                  onDragEnd: () => {
                    rr.current = null, tr(null);
                  },
                  style: { marginBottom: "6px", borderTop: m ? `2px solid ${d.primary}` : "2px solid transparent", cursor: "grab" },
                  children: c ? /* @__PURE__ */ e.jsx(st, { item: r, idx: p, selected: r.field === be, onSelect: Yr, onRemove: Xe, setValConfigs: y, theme: d, styles: Z, dropLine: null, zoneId: "vals", valueSelectStyle: me, colFields: x, fixedSparklineValueKeySet: br, showNotification: A, serverError: We && We[r.field] || null }) : /* @__PURE__ */ e.jsxs("div", { style: { ...Z.chip, flexWrap: "wrap", alignItems: "flex-start" }, children: [
                    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                      /* @__PURE__ */ e.jsx(E.DragIndicator, { style: { flexShrink: 0, opacity: 0.4 } }),
                      /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: re(r.field) })
                    ] }),
                    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                      /* @__PURE__ */ e.jsxs(ne, { size: "sm", "aria-label": "Aggregation", value: r.agg, onClick: (S) => S.stopPropagation(), onChange: (S) => Vr(p, S.target.value), style: { ...me, width: "78px" }, children: [
                        /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                        /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Avg" }),
                        /* @__PURE__ */ e.jsx("option", { value: "count", children: "Cnt" }),
                        /* @__PURE__ */ e.jsx("option", { value: "min", children: "Min" }),
                        /* @__PURE__ */ e.jsx("option", { value: "max", children: "Max" }),
                        /* @__PURE__ */ e.jsx("option", { value: "weighted_avg", children: "WAvg" })
                      ] }),
                      /* @__PURE__ */ e.jsx("span", { onClick: () => Xe(p), style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(E.Close, {}) })
                    ] })
                  ] })
                },
                r.field || p
              );
            }
          ),
          k.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { opacity: 0.5, fontSize: "11px", padding: "12px", textAlign: "center" }, children: "No value fields. Click a field above or add a formula." })
        ] }) : !Oe && n === "filters" ? /* @__PURE__ */ e.jsxs("div", { style: { flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", padding: "8px" }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "10px", display: "flex", alignItems: "center", background: d.background, borderRadius: "6px", padding: "4px 8px", border: `1px solid ${d.border}` }, children: [
            /* @__PURE__ */ e.jsx(E.Search, {}),
            /* @__PURE__ */ e.jsx(
              "input",
              {
                placeholder: "Search columns...",
                value: D,
                onChange: (r) => O(r.target.value),
                style: { border: "none", background: "transparent", marginLeft: "10px", outline: "none", width: "100%", color: d.text, fontSize: "13px" }
              }
            )
          ] }),
          (() => {
            const c = $.map(
              /** @param {any} [field] @returns {any} */
              (m) => {
                const S = h.getColumn(m);
                return S || { id: m, header: re(m), columnDef: { header: re(m) } };
              }
            ).filter(
              /** @param {any} [col] @returns {any} */
              (m) => {
                const S = m.columnDef && typeof m.columnDef.header == "string" ? m.columnDef.header : typeof m.header == "string" ? m.header : m.id;
                return String(S).toLowerCase().includes(D.toLowerCase()) || m.id.toLowerCase().includes(D.toLowerCase());
              }
            );
            return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: c.map(
              /** @param {any} [col] @returns {any} */
              (m) => /* @__PURE__ */ e.jsx(
                tn,
                {
                  column: m,
                  theme: d,
                  styles: Z,
                  onFilter: (
                    /** @param {any} [val] @returns {any} */
                    ((S) => V(m.id, S))
                  ),
                  currentFilter: ue[m.id],
                  options: Ur(m.id),
                  optionMeta: _e && _e[m.id],
                  onSearchOptions: Qe ? (
                    /** @param {any} [search] @returns {any} */
                    ((S) => K && K(m.id, { search: S, offset: 0 }))
                  ) : void 0,
                  onLoadMoreOptions: Qe ? (
                    /** @param {any} [search] @param {any} [offset] @returns {any} */
                    ((S, I) => K && K(m.id, { search: S, offset: I }))
                  ) : void 0,
                  onOpen: K
                },
                m.id
              )
            ) });
          })()
        ] }) : (Oe ? Ae !== "columns" : n !== "columns") ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
          n === "fields" && /* @__PURE__ */ e.jsxs("div", { style: { flexShrink: 0 }, children: [
            /* @__PURE__ */ e.jsx(
              lt,
              {
                availableFields: $,
                customDimensions: Ve,
                setCustomDimensions: zr,
                data: fe,
                theme: d,
                showNotification: A,
                getFieldLabel: re,
                onDeleteField: Wr
              }
            ),
            /* @__PURE__ */ e.jsxs("div", { style: Z.sectionTitleSm, children: [
              /* @__PURE__ */ e.jsx(E.Database, {}),
              " Available Fields"
            ] }),
            Gr(Le.length, hr.length),
            /* @__PURE__ */ e.jsxs(
              dt,
              {
                panelId: "availableFields",
                panelLabel: "Available Fields",
                size: ge && ge.availableFields,
                onSizeChange: Or,
                getSidebarWidth: Lr,
                onSidebarWidthChange: Pr,
                theme: d,
                outerStyle: Nr,
                contentStyle: {
                  display: "flex",
                  flexWrap: "wrap",
                  gap: "4px",
                  alignContent: "flex-start",
                  padding: "2px 12px 12px 2px"
                },
                children: [
                  hr.map(
                    /** @param {any} [f] @returns {any} */
                    (r) => {
                      const p = Ye && r === je, c = p ? "Measure Names" : re(r), m = p ? "measureAxis" : "pool";
                      return /* @__PURE__ */ e.jsxs(
                        "div",
                        {
                          draggable: !0,
                          "data-sidebar-field-chip": p ? "measure-axis-label" : "available",
                          role: "button",
                          tabIndex: 0,
                          "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                          "aria-grabbed": q && q.zone === m && q.field === r ? "true" : "false",
                          "aria-label": `Move ${c}`,
                          onDragStart: (S) => N(S, r, m),
                          onDragEnd: z,
                          onKeyDown: (S) => Br(S, r, m, -1, c),
                          title: p ? "Drag to Rows or Columns to show measure names" : `Move ${re(r)}`,
                          style: {
                            padding: "4px 8px",
                            fontSize: "11px",
                            lineHeight: 1.2,
                            fontWeight: p ? 700 : 500,
                            fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif",
                            borderRadius: d.radiusSm || d.radius || "10px",
                            border: p ? `1px dashed ${d.primary}` : `1px solid ${d.isDark ? d.border : "#D7DDEA"}`,
                            background: p ? `${d.primary}12` : d.isDark ? d.background : d.surfaceInset || "#ffffff",
                            color: p ? d.primary : d.isDark ? d.text : "#334155",
                            display: "inline-flex",
                            alignItems: "center",
                            gap: "4px",
                            boxShadow: d.isDark ? "none" : d.shadowInset || "0 1px 2px rgba(0,0,0,0.04)",
                            cursor: "grab",
                            userSelect: "none",
                            minHeight: "24px"
                          },
                          children: [
                            p && /* @__PURE__ */ e.jsx(E.Sigma, {}),
                            c
                          ]
                        },
                        p ? `__measure_axis__${r}` : r
                      );
                    }
                  ),
                  Le.length > 8 && hr.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
                ]
              }
            )
          ] }),
          Mt.map(
            /** @param {any} [zone] @returns {any} */
            (r) => {
              const p = Ge[r.id] || [];
              return /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "20px", flexShrink: 0 }, children: [
                /* @__PURE__ */ e.jsxs("div", { style: Z.sectionTitle, children: [
                  r.icon,
                  r.label
                ] }),
                r.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 4px 8px 4px", flexWrap: "wrap" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: Kr,
                      style: {
                        border: `1px solid ${d.primary}66`,
                        background: `${d.primary}12`,
                        color: d.primary,
                        borderRadius: "999px",
                        cursor: "pointer",
                        padding: "4px 10px",
                        fontSize: "11px",
                        fontWeight: 700
                      },
                      children: "+ Add Formula"
                    }
                  ),
                  /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
                    Pe,
                    " value measure",
                    Pe === 1 ? "" : "s",
                    " available"
                  ] }),
                  $e > 0 && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
                    $e,
                    " formula",
                    $e === 1 ? "" : "s"
                  ] }),
                  /* @__PURE__ */ e.jsx(
                    ct,
                    {
                      measureAxis: F,
                      setMeasureAxis: s,
                      theme: d,
                      valueSelectStyle: me,
                      regularValueCount: Pe
                    }
                  )
                ] }),
                r.id === "rows" && o.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", padding: "0 4px 6px 4px" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      title: "Expand all hierarchy levels",
                      onClick: () => Q(!0),
                      style: { flex: 1, border: `1px solid ${d.primary}`, background: d.select, cursor: "pointer", padding: "3px 6px", fontSize: "11px", color: d.primary, borderRadius: "4px", fontWeight: 600 },
                      children: "+ Expand All"
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      title: "Collapse all rows",
                      onClick: () => Q(!1),
                      style: { flex: 1, border: `1px solid ${d.border}`, background: d.background, cursor: "pointer", padding: "3px 6px", fontSize: "11px", color: d.textSec, borderRadius: "4px" },
                      children: "- Collapse All"
                    }
                  )
                ] }),
                /* @__PURE__ */ e.jsx(
                  dt,
                  {
                    panelId: r.id,
                    panelLabel: r.label,
                    size: ge && ge[r.id],
                    onSizeChange: Or,
                    getSidebarWidth: Lr,
                    onSidebarWidthChange: Pr,
                    theme: d,
                    outerStyle: Nr,
                    contentStyle: { padding: `${Hr} 12px 12px ${Hr}` },
                    children: /* @__PURE__ */ e.jsxs(
                      "div",
                      {
                        "data-sidebar-drop-zone": r.id,
                        role: "group",
                        tabIndex: 0,
                        "aria-label": `${r.label} drop zone`,
                        "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                        onKeyDown: (c) => _t(c, r.id),
                        onDragOver: (c) => {
                          c.target === c.currentTarget ? L(c, r.id, p.length) : (c.preventDefault(), c.dataTransfer && (c.dataTransfer.dropEffect = "move"));
                        },
                        onDrop: (c) => g(c, r.id),
                        children: [
                          p.map(
                            /** @param {any} [item] @param {any} [idx] @returns {any} */
                            (c, m) => {
                              const S = r.id === "vals" ? c.field : c, I = r.id === "vals" ? c.agg === "formula" ? c.label || "Formula" : re(c.field) : re(c), X = (r.id === "rows" || r.id === "cols") && S === je && _r.placement !== "none";
                              return /* @__PURE__ */ e.jsxs(
                                "div",
                                {
                                  draggable: !0,
                                  "data-sidebar-field-chip": "zone",
                                  role: "button",
                                  tabIndex: 0,
                                  "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                                  "aria-grabbed": q && q.zone === r.id && q.idx === m ? "true" : "false",
                                  "aria-label": `Move ${I}`,
                                  onKeyDown: (j) => Br(j, c, r.id, m, I),
                                  onDragStart: (j) => N(j, c, r.id, m),
                                  onDragOver: (j) => L(j, r.id, m),
                                  onDragEnd: z,
                                  children: [
                                    r.id === "vals" && c.agg === "formula" ? /* @__PURE__ */ e.jsx(
                                      st,
                                      {
                                        item: c,
                                        idx: m,
                                        selected: c.field === Te,
                                        onSelect: Yr,
                                        onRemove: Xe,
                                        setValConfigs: y,
                                        theme: d,
                                        styles: Z,
                                        dropLine: _,
                                        zoneId: r.id,
                                        valueSelectStyle: me,
                                        colFields: x,
                                        fixedSparklineValueKeySet: br,
                                        showNotification: A,
                                        serverError: We && We[c.field] || null
                                      }
                                    ) : /* @__PURE__ */ e.jsxs("div", { style: {
                                      ...Z.chip,
                                      ...r.id === "vals" ? { flexWrap: "wrap", alignItems: "flex-start" } : {}
                                    }, children: [
                                      _ && _.zone === r.id && _.idx === m && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...Z.dropLine, top: -2 } }),
                                      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                                        /* @__PURE__ */ e.jsx(E.DragIndicator, {}),
                                        /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, display: "block", minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: I })
                                      ] }),
                                      r.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                                        /* @__PURE__ */ e.jsxs(ne, { size: "sm", value: c.agg, onClick: (j) => j.stopPropagation(), onChange: (j) => Vr(m, j.target.value), style: { ...me, width: "78px" }, title: "Aggregation", children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Avg" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "count", children: "Cnt" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "min", children: "Min" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "max", children: "Max" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "weighted_avg", children: "WAvg" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "array_agg", children: "Series" })
                                        ] }),
                                        qr(c.agg) && /* @__PURE__ */ e.jsxs(ne, { size: "sm", value: c.weightField || "", onClick: (j) => j.stopPropagation(), onChange: (j) => Tt(m, j.target.value), style: { ...me, width: "96px" }, title: "Weight field", children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "", disabled: !0, children: "Weight" }),
                                          te.map(
                                            /** @param {any} [fieldName] @returns {any} */
                                            (j) => /* @__PURE__ */ e.jsx("option", { value: j, children: re(j) }, j)
                                          )
                                        ] }),
                                        /* @__PURE__ */ e.jsxs(ne, { size: "sm", "aria-label": "Window calculation", value: c.windowFn || "none", onClick: (j) => j.stopPropagation(), onChange: (j) => It(m, j.target.value), style: { ...me, width: "68px" }, children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "none", children: "Norm" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_row", children: "%Row" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_col", children: "%Col" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_grand_total", children: "%Tot" })
                                        ] })
                                      ] }),
                                      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", marginLeft: "auto", alignItems: "center" }, children: [
                                        r.id === "filter" && /* @__PURE__ */ e.jsx("div", { onClick: (j) => Bt(j, S), style: { cursor: "pointer", display: "flex", alignItems: "center", padding: "2px", borderRadius: "4px" }, children: /* @__PURE__ */ e.jsx(E.Filter, {}) }),
                                        /* @__PURE__ */ e.jsx("span", { onClick: () => {
                                          if (X && typeof s == "function") {
                                            s(
                                              /** @param {any} [previous] @returns {any} */
                                              (j) => ({
                                                ...Je(j),
                                                placement: "none"
                                              })
                                            ), r.id === "rows" && l(
                                              /** @param {any} [p] @returns {any} */
                                              (j) => Array.isArray(j) ? j.filter(
                                                /** @param {any} [x] @returns {any} */
                                                (H) => H !== S
                                              ) : j
                                            ), r.id === "cols" && f(
                                              /** @param {any} [p] @returns {any} */
                                              (j) => Array.isArray(j) ? j.filter(
                                                /** @param {any} [x] @returns {any} */
                                                (H) => H !== S
                                              ) : j
                                            );
                                            return;
                                          }
                                          if (r.id === "filter") {
                                            const j = { ...ue };
                                            delete j[S], xr(j);
                                          }
                                          r.id === "rows" && l(
                                            /** @param {any} [p] @returns {any} */
                                            (j) => j.filter(
                                              /** @param {any} [x] @returns {any} */
                                              (H) => H !== S
                                            )
                                          ), r.id === "cols" && f(
                                            /** @param {any} [p] @returns {any} */
                                            (j) => j.filter(
                                              /** @param {any} [x] @returns {any} */
                                              (H) => H !== S
                                            )
                                          ), r.id === "vals" && Xe(m);
                                        }, className: "sp-chip-remove", style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(E.Close, {}) })
                                      ] }),
                                      r.id === "filter" && er.columnId === S && /* @__PURE__ */ e.jsx(
                                        qt,
                                        {
                                          column: { header: I, id: S },
                                          anchorEl: er.anchorEl,
                                          onClose: Lt,
                                          onFilter: (
                                            /** @param {any} [filterValue] @returns {any} */
                                            ((j) => V(S, j))
                                          ),
                                          currentFilter: ue[S],
                                          options: Ur(S),
                                          optionMeta: _e && _e[S],
                                          onSearchOptions: Qe ? (
                                            /** @param {any} [search] @returns {any} */
                                            ((j) => K && K(S, { search: j, offset: 0 }))
                                          ) : void 0,
                                          onLoadMoreOptions: Qe ? (
                                            /** @param {any} [search] @param {any} [offset] @returns {any} */
                                            ((j, H) => K && K(S, { search: j, offset: H }))
                                          ) : void 0,
                                          theme: d
                                        }
                                      ),
                                      r.id === "vals" && /* @__PURE__ */ e.jsx(
                                        jt,
                                        {
                                          item: c,
                                          idx: m,
                                          setValConfigs: y,
                                          theme: d,
                                          valueSelectStyle: me,
                                          colFields: x,
                                          fixedSparklineValueKeySet: br,
                                          showNotification: A
                                        }
                                      ),
                                      _ && _.zone === r.id && _.idx === m + 1 && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...Z.dropLine, bottom: -2 } })
                                    ] }),
                                    r.id === "filter" && ue[S] && ue[S].conditions && /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: d.primary, padding: "0 8px 4px 8px", marginTop: "-4px" }, children: ue[S].conditions.map(
                                      /** @param {any} [c] @returns {any} */
                                      (j) => `${j.type}: ${j.value}${j.caseSensitive ? " (Match Case)" : ""}`
                                    ).join(` ${ue[S].operator} `) })
                                  ]
                                },
                                m
                              );
                            }
                          ),
                          p.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { opacity: 0.5, fontSize: "11px", padding: "8px", textAlign: "center", pointerEvents: "none" }, children: "Drag fields here" }),
                          /* @__PURE__ */ e.jsx("div", { style: { height: 20 }, onDragOver: (c) => L(c, r.id, p.length), onDrop: (c) => g(c, r.id) })
                        ]
                      }
                    )
                  }
                )
              ] }, r.id);
            }
          )
        ] }) : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "16px", height: "100%", overflow: "hidden" }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px", padding: "10px", background: d.headerSubtleBg || d.headerBg, border: `1px solid ${d.border}`, borderRadius: "10px" }, children: [
            /* @__PURE__ */ e.jsxs("div", { className: "sp-search-shell", style: { display: "flex", alignItems: "center", background: d.surfaceBg || d.background, borderRadius: "8px", padding: "7px 10px", border: `1px solid ${d.border}` }, children: [
              /* @__PURE__ */ e.jsx(E.Search, {}),
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  placeholder: "Search columns...",
                  value: D,
                  onChange: (r) => O(r.target.value),
                  style: {
                    border: "none",
                    background: "transparent",
                    marginLeft: "8px",
                    outline: "none",
                    width: "100%",
                    color: d.text,
                    fontSize: "13px",
                    fontWeight: 500
                  }
                }
              ),
              D && /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-ghost",
                  "aria-label": "Clear column search",
                  onClick: () => O(""),
                  style: {
                    cursor: "pointer",
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    border: "none",
                    color: d.textSec,
                    padding: "4px",
                    borderRadius: "6px",
                    background: d.hover
                  },
                  children: /* @__PURE__ */ e.jsx(E.Close, {})
                }
              )
            ] }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
              { value: "all", label: "All", Icon: E.TypeAll },
              { value: "number", label: "Numbers", Icon: E.TypeNumber },
              { value: "string", label: "Text", Icon: E.TypeText },
              { value: "date", label: "Dates", Icon: E.TypeDate }
            ].map((r) => {
              const p = a === r.value, c = r.Icon;
              return /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: `sp-btn ${p ? "sp-btn-primary" : "sp-btn-subtle"}`,
                  onClick: () => w(r.value),
                  style: {
                    padding: "6px 10px",
                    borderRadius: "8px",
                    border: `1px solid ${p ? d.primary : d.border}`,
                    background: p ? d.primary : d.surfaceBg || d.background,
                    color: p ? "#fff" : d.text,
                    cursor: "pointer",
                    fontSize: "11px",
                    fontWeight: 600,
                    display: "inline-flex",
                    alignItems: "center",
                    gap: "5px",
                    boxShadow: p ? "0 2px 6px rgba(0,0,0,0.12)" : "none"
                  },
                  children: [
                    /* @__PURE__ */ e.jsx(c, {}),
                    /* @__PURE__ */ e.jsx("span", { children: r.label })
                  ]
                },
                r.value
              );
            }) })
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", padding: "0 8px" }, children: [
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                onClick: () => h.toggleAllColumnsVisible(!0),
                style: {
                  ...Z.btn,
                  padding: "8px 12px",
                  fontSize: "11px",
                  flex: 1,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                  transition: "all 0.2s"
                },
                onMouseEnter: (r) => r.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                onMouseLeave: (r) => r.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
                children: [
                  /* @__PURE__ */ e.jsx(E.Visibility, { style: { fontSize: "14px" } }),
                  /* @__PURE__ */ e.jsx("span", { children: "Show All" })
                ]
              }
            ),
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                onClick: () => h.toggleAllColumnsVisible(!1),
                style: {
                  ...Z.btn,
                  padding: "8px 12px",
                  fontSize: "11px",
                  flex: 1,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                  transition: "all 0.2s"
                },
                onMouseEnter: (r) => r.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                onMouseLeave: (r) => r.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
                children: [
                  /* @__PURE__ */ e.jsx(E.VisibilityOff, { style: { fontSize: "14px" } }),
                  /* @__PURE__ */ e.jsx("span", { children: "Hide All" })
                ]
              }
            ),
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                onClick: () => ee(!1),
                style: {
                  ...Z.btn,
                  padding: "8px 12px",
                  fontSize: "11px",
                  flex: 1,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                  transition: "all 0.2s"
                },
                onMouseEnter: (r) => r.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                onMouseLeave: (r) => r.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
                children: [
                  /* @__PURE__ */ e.jsx(E.Unpin, { style: { fontSize: "14px" } }),
                  /* @__PURE__ */ e.jsx("span", { children: "Unpin All" })
                ]
              }
            )
          ] }),
          C.size > 0 && /* @__PURE__ */ e.jsxs("div", { style: {
            display: "flex",
            flexDirection: "column",
            gap: "8px",
            background: `linear-gradient(135deg, ${d.select}ee, ${d.select}dd)`,
            padding: "12px",
            borderRadius: "8px",
            border: `2px solid ${d.primary}66`,
            boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
            margin: "0 8px"
          }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "center" }, children: [
              /* @__PURE__ */ e.jsxs("span", { style: {
                fontSize: "12px",
                fontWeight: 700,
                padding: "4px 10px",
                color: d.primary,
                background: d.background,
                borderRadius: "6px",
                boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
              }, children: [
                C.size,
                " Column",
                C.size > 1 ? "s" : "",
                " Selected"
              ] }),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: () => B(/* @__PURE__ */ new Set()),
                  style: {
                    border: "none",
                    background: d.background,
                    cursor: "pointer",
                    display: "flex",
                    color: d.textSec,
                    padding: "4px",
                    borderRadius: "4px",
                    transition: "all 0.2s"
                  },
                  onMouseEnter: (r) => r.currentTarget.style.background = d.hover,
                  onMouseLeave: (r) => r.currentTarget.style.background = d.background,
                  children: /* @__PURE__ */ e.jsx(E.Close, {})
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  onClick: () => {
                    Array.from(C).forEach((r) => Y(r, "left")), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...Z.btn,
                    padding: "6px 12px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                    transition: "all 0.2s"
                  },
                  onMouseEnter: (r) => r.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                  onMouseLeave: (r) => r.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
                  children: [
                    /* @__PURE__ */ e.jsx(E.PinLeft, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Pin Left" })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  onClick: () => {
                    Array.from(C).forEach((r) => Y(r, !1)), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...Z.btn,
                    padding: "6px 12px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                    transition: "all 0.2s"
                  },
                  onMouseEnter: (r) => r.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                  onMouseLeave: (r) => r.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
                  children: [
                    /* @__PURE__ */ e.jsx(E.Unpin, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Unpin" })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  onClick: () => {
                    Array.from(C).forEach((r) => Y(r, "right")), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...Z.btn,
                    padding: "6px 12px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                    transition: "all 0.2s"
                  },
                  onMouseEnter: (r) => r.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                  onMouseLeave: (r) => r.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
                  children: [
                    /* @__PURE__ */ e.jsx(E.PinRight, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Pin Right" })
                  ]
                }
              )
            ] })
          ] }),
          /* @__PURE__ */ e.jsx("div", { style: { flex: 1, overflowY: "auto", display: "flex", flexDirection: "column" }, children: (() => {
            const p = h.getAllColumns().filter(
              /** @param {any} [c] @returns {any} */
              (j) => !j.parent && j.id !== "no_data"
            ).filter(
              /** @param {any} [col] @returns {any} */
              (j) => {
                if (a === "all") return !0;
                let H = "string";
                if (k.find(
                  /** @param {any} [v] @returns {any} */
                  (ie) => j.id.includes(ie.field)
                ))
                  H = "number";
                else if (fe && fe.length > 0) {
                  const ie = fe[0];
                  let ae = ie[j.id];
                  if (ae === void 0 && j.accessorFn)
                    try {
                      ae = j.accessorFn(ie);
                    } catch {
                    }
                  typeof ae == "number" ? H = "number" : ae instanceof Date ? H = "date" : typeof ae == "string" && (!isNaN(Number(ae)) && ae.trim() !== "" ? H = "number" : !isNaN(Date.parse(ae)) && ae.includes("-") && (H = "date"));
                }
                return H === a;
              }
            ), c = p.filter(
              /** @param {any} [c] @returns {any} */
              (j) => ir(j, "left")
            ), m = p.filter(
              /** @param {any} [c] @returns {any} */
              (j) => ir(j, "right")
            ), S = p.filter(
              /** @param {any} [c] @returns {any} */
              (j) => ir(j, "unpinned")
            ), I = (j, H) => j.map(
              /** @param {any} [column] @returns {any} */
              (oe) => /* @__PURE__ */ e.jsx(
                pt,
                {
                  column: oe,
                  level: 0,
                  theme: d,
                  styles: Z,
                  handlePinColumn: Y,
                  colSearch: D,
                  selectedCols: C,
                  setSelectedCols: B,
                  onDrop: X,
                  sectionId: H
                },
                oe.id
              )
            ), X = (j, H, oe) => {
              let ie;
              const ae = P || { left: [], right: [] };
              if (oe) {
                const qe = H === "left" ? ae.left : H === "right" ? ae.right : null;
                if (qe) {
                  if (ie = qe.indexOf(oe), ie === -1) {
                    const jr = h.getColumn(oe);
                    if (jr && en(jr)) {
                      const Xr = wr(jr).find(
                        /** @param {any} [id] @returns {any} */
                        (Pt) => qe.includes(Pt)
                      );
                      Xr && (ie = qe.indexOf(Xr));
                    }
                  }
                  ie === -1 && (ie = void 0);
                }
              }
              H === "left" ? Y(j, "left", ie) : H === "right" ? Y(j, "right", ie) : Y(j, !1);
            };
            return /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              c.length > 0 && /* @__PURE__ */ e.jsx(
                kr,
                {
                  title: "Pinned Left",
                  count: c.length,
                  theme: d,
                  styles: Z,
                  sectionId: "left",
                  onDrop: X,
                  children: I(c, "left")
                }
              ),
              m.length > 0 && /* @__PURE__ */ e.jsx(
                kr,
                {
                  title: "Pinned Right",
                  count: m.length,
                  theme: d,
                  styles: Z,
                  sectionId: "right",
                  onDrop: X,
                  children: I(m, "right")
                }
              ),
              /* @__PURE__ */ e.jsx(
                kr,
                {
                  title: "Columns",
                  count: S.length,
                  theme: d,
                  styles: Z,
                  sectionId: "unpinned",
                  onDrop: X,
                  children: I(S, "unpinned")
                }
              )
            ] });
          })() }),
          R && R.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { padding: "8px", borderTop: `1px solid ${d.border}` }, children: [
            /* @__PURE__ */ e.jsx("div", { style: Z.sectionTitle, children: "Pinning Presets" }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px", flexWrap: "wrap" }, children: R.map(
              /** @param {any} [preset] @param {any} [i] @returns {any} */
              (r, p) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: () => {
                    M(r.config), A(`Applied preset: ${r.name}`);
                  },
                  style: { ...Z.btn, fontSize: "11px", background: d.headerBg },
                  children: r.name
                },
                p
              )
            ) })
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "11px", color: d.textSec, fontStyle: "italic", padding: "8px", background: d.headerBg, borderTop: `1px solid ${d.border}44` }, children: [
            /* @__PURE__ */ e.jsx(E.Lock, { style: { verticalAlign: "middle", marginRight: "4px", opacity: 0.5 } }),
            "Drag columns or use pin icons to freeze areas."
          ] })
        ] })
      ] }),
      Sr && /* @__PURE__ */ e.jsx(
        An,
        {
          item: Sr,
          idx: vr,
          valConfigs: k,
          setValConfigs: y,
          theme: d,
          onClose: () => De(null),
          onRemove: Xe,
          displayedColumnOptions: Se,
          availableRawFields: te,
          serverError: We && We[Sr.field] || null
        }
      ),
      /* @__PURE__ */ e.jsx(
        "div",
        {
          className: "sp-resize-handle",
          onMouseDown: Wt,
          title: "Drag to resize panel",
          style: { position: "absolute", right: -2, top: 0, bottom: 0, width: "9px", cursor: "col-resize", zIndex: 10, background: "transparent", display: "flex", alignItems: "center", justifyContent: "center" },
          children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "2px", height: "100%", borderRadius: "999px", background: d.border, opacity: 0.55, pointerEvents: "none" } })
        }
      )
    ] });
  }
);
export {
  En as SidebarPanel
};
