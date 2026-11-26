import { j as e, I as E, a1 as ke, a2 as Or, a3 as Nr, a4 as wt, a5 as it, a6 as Hr, a7 as Ur, a8 as Vr, a9 as Ne, aa as st, ab as dt, G as Kr, ac as Yr, ad as Zr, ae as Gr, af as Je, ag as ur, ah as Xr, ai as qt, T as ne, aj as qr, ak as Jr, al as Qr, am as Jt, an as en, ao as tn } from "./index-BXM8DLHY.js";
import { a as de, R as u } from "./charts-DthW1Q8W.js";
const rn = ({ column: t, theme: n, styles: i, onFilter: o, currentFilter: l, options: x, optionMeta: f, onSearchOptions: j, onLoadMoreOptions: y, onOpen: F }) => {
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
      Or,
      {
        column: t,
        onFilter: o,
        currentFilter: l,
        options: x,
        optionMeta: f,
        onSearchOptions: j,
        onLoadMoreOptions: y,
        theme: n,
        onClose: P
      }
    ) })
  ] });
}, kt = ({ title: t, children: n, items: i, renderItem: o, theme: l, styles: x, initialExpanded: f = !0, count: j, onDrop: y, sectionId: F }) => {
  const [s, b] = de.useState(f), [W, P] = de.useState(f), [M, $] = de.useState(f), [h, C] = de.useState(!1), D = de.useRef(null);
  de.useEffect(() => {
    if (s) {
      P(!0);
      const w = setTimeout(() => $(!0), 220);
      return () => clearTimeout(w);
    }
    $(!1);
    const k = setTimeout(() => P(!1), 220);
    return () => clearTimeout(k);
  }, [s]);
  const I = Nr({
    count: i ? i.length : 0,
    getScrollElement: () => D.current,
    estimateSize: () => 32,
    overscan: 10,
    enabled: !!i && (s || W)
  }), N = (k) => {
    k.preventDefault(), C(!0);
  }, R = () => {
    C(!1);
  }, O = (k) => {
    k.preventDefault(), C(!1);
    const w = k.dataTransfer.getData("text/plain");
    y && w && y(w, F);
  }, a = (k) => {
    (k.key === "Enter" || k.key === " ") && (k.preventDefault(), b(
      /** @param {any} [prev] @returns {any} */
      (w) => !w
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
      onDragOver: N,
      onDragLeave: R,
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
              j !== void 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", opacity: 0.7, background: l.hover, padding: "2px 7px", borderRadius: "10px", fontVariantNumeric: "tabular-nums" }, children: j })
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
                ref: D,
                className: "sp-disclosure-inner",
                style: {
                  ...x.toolPanelList,
                  maxHeight: "300px",
                  overflowY: s && M ? "auto" : "hidden"
                },
                children: i ? /* @__PURE__ */ e.jsx("div", { style: {
                  height: `${I.getTotalSize()}px`,
                  width: "100%",
                  position: "relative"
                }, children: I.getVirtualItems().map(
                  /** @param {any} [virtualRow] @returns {any} */
                  (k) => /* @__PURE__ */ e.jsx(
                    "div",
                    {
                      style: {
                        position: "absolute",
                        top: 0,
                        left: 0,
                        width: "100%",
                        height: `${k.size}px`,
                        transform: `translateY(${k.start}px)`
                      },
                      children: o(i[k.index], k.index)
                    },
                    k.index
                  )
                ) }) : n
              }
            )
          }
        )
      ]
    }
  );
}, pr = ({ column: t, level: n, theme: i, styles: o, handlePinColumn: l, colSearch: x, selectedCols: f, setSelectedCols: j, onDrop: y, sectionId: F }) => {
  const [s, b] = de.useState(n < 1), [W, P] = de.useState(!1), [M, $] = de.useState(!1), h = de.useRef(null), C = t.columns && t.columns.length > 0, D = t.columnDef.header;
  let I = typeof D == "string" ? D : t.id;
  typeof I == "string" && I.startsWith("group_") && (I = t.id.replace("group_", "").split("|||").pop() || I), t.headerVal && (I = t.headerVal), I = ke(I);
  const N = t.getIsPinned(), R = t.getIsVisible(), O = C ? wt(t) : [t.id], a = O.length > 0 && O.every((A) => f.has(A)), k = !a && O.some((A) => f.has(A));
  if (de.useEffect(() => {
    h.current && (h.current.indeterminate = k);
  }, [k]), x && !I.toLowerCase().includes(x.toLowerCase()))
    if (C) {
      const A = (H) => {
        const Z = H.columnDef.header;
        return (typeof Z == "string" ? Z : H.id).toLowerCase().includes(x.toLowerCase()) ? !0 : H.columns ? H.columns.some(A) : !1;
      };
      if (!A(t)) return null;
    } else
      return null;
  const w = (A) => {
    A.stopPropagation();
    const H = new Set(f), Z = C ? wt(t) : [t.id];
    Z.every(
      /** @param {any} [id] @returns {any} */
      (U) => H.has(U)
    ) ? Z.forEach(
      /** @param {any} [id] @returns {any} */
      (U) => H.delete(U)
    ) : Z.forEach(
      /** @param {any} [id] @returns {any} */
      (U) => H.add(U)
    ), j(H);
  }, B = (A) => {
    if (A.stopPropagation(), C) {
      const H = Hr(t), Z = H.some(
        /** @param {any} [c] @returns {any} */
        (Q) => !Q.getIsVisible()
      );
      H.forEach(
        /** @param {any} [c] @returns {any} */
        (Q) => Q.toggleVisibility(Z)
      );
    } else
      t.toggleVisibility();
  }, _ = (A, H) => {
    A.stopPropagation(), l(t.id, H);
  }, Y = (A) => {
    $(!0), A.dataTransfer.setData("text/plain", t.id), A.dataTransfer.effectAllowed = "move";
  }, L = () => {
    $(!1);
  }, g = (A) => {
    A.preventDefault(), A.stopPropagation();
    const H = A.dataTransfer.getData("text/plain");
    H && y && H !== t.id && y(H, F, t.id);
  }, z = (A) => {
    A.key === "Enter" ? B(A) : A.key === " " ? (A.preventDefault(), w(A)) : A.altKey && A.key === "ArrowLeft" ? (A.preventDefault(), l(t.id, "left")) : A.altKey && A.key === "ArrowRight" ? (A.preventDefault(), l(t.id, "right")) : A.altKey && A.key === "ArrowDown" && (A.preventDefault(), l(t.id, !1));
  }, V = (A) => ({
    width: "20px",
    height: "20px",
    padding: "2px",
    background: A ? i.primary : "transparent",
    border: "none",
    cursor: "pointer",
    borderRadius: "4px",
    color: A ? "#fff" : i.textSec,
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    opacity: A ? 1 : 0.72,
    transition: "background-color var(--sp-dur-fast, 140ms) ease, color var(--sp-dur-fast, 140ms) ease, opacity var(--sp-dur-fast, 140ms) ease"
  });
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      style: { display: "flex", flexDirection: "column", opacity: M ? 0.5 : 1 },
      draggable: !C,
      onDragStart: Y,
      onDragEnd: L,
      onDragOver: (A) => A.preventDefault(),
      onDrop: g,
      role: "treeitem",
      "aria-selected": a || k,
      "aria-expanded": s,
      children: [
        /* @__PURE__ */ e.jsxs(
          "div",
          {
            className: "sp-col-tree-row",
            style: {
              ...o.columnItem,
              gap: "5px",
              paddingRight: "6px",
              paddingLeft: `${n * 12 + 6}px`,
              minWidth: 0,
              background: a || k ? i.select : W ? i.hover : "transparent",
              borderLeft: N ? `3px solid ${i.primary}` : "3px solid transparent"
            },
            onMouseEnter: () => !(a || k) && P(!0),
            onMouseLeave: () => !(a || k) && P(!1),
            tabIndex: 0,
            onKeyDown: z,
            children: [
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  ref: h,
                  type: "checkbox",
                  checked: a,
                  onChange: w,
                  onClick: (A) => A.stopPropagation(),
                  style: { margin: 0, cursor: "pointer", pointerEvents: "auto", flexShrink: 0 },
                  tabIndex: -1
                }
              ),
              !C && /* @__PURE__ */ e.jsx("span", { style: { cursor: "grab", display: "inline-flex", opacity: 0.55, flexShrink: 0 }, children: /* @__PURE__ */ e.jsx(E.DragIndicator, {}) }),
              C && /* @__PURE__ */ e.jsx(
                "span",
                {
                  role: "button",
                  tabIndex: 0,
                  "aria-expanded": s,
                  "aria-label": s ? `Collapse ${I}` : `Expand ${I}`,
                  onClick: () => b(!s),
                  onKeyDown: (A) => {
                    (A.key === "Enter" || A.key === " ") && (A.preventDefault(), b(!s));
                  },
                  style: { cursor: "pointer", display: "inline-flex", opacity: 0.75, flexShrink: 0 },
                  children: s ? /* @__PURE__ */ e.jsx(E.ChevronDown, {}) : /* @__PURE__ */ e.jsx(E.ChevronRight, {})
                }
              ),
              /* @__PURE__ */ e.jsx(
                "span",
                {
                  onClick: B,
                  style: {
                    cursor: "pointer",
                    display: "inline-flex",
                    color: R ? i.primary : i.textSec,
                    opacity: R ? 1 : 0.5,
                    flexShrink: 0
                  },
                  title: R ? "Hide Column" : "Show Column",
                  "aria-label": R ? "Hide Column" : "Show Column",
                  children: R ? /* @__PURE__ */ e.jsx(E.Visibility, {}) : /* @__PURE__ */ e.jsx(E.VisibilityOff, {})
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "span",
                {
                  style: {
                    flex: "1 1 0%",
                    minWidth: 0,
                    overflow: "hidden",
                    fontWeight: C ? 600 : 400,
                    cursor: C ? "pointer" : "default",
                    color: R ? i.text : i.textSec,
                    opacity: R ? 1 : 0.6,
                    display: "flex",
                    alignItems: "center",
                    gap: "4px"
                  },
                  onClick: () => C && b(!s),
                  title: I,
                  children: [
                    C && /* @__PURE__ */ e.jsx(E.Group, { style: { fontSize: "13px", opacity: 0.8, flexShrink: 0 } }),
                    /* @__PURE__ */ e.jsx("span", { style: { minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: I })
                  ]
                }
              ),
              !C && /* @__PURE__ */ e.jsxs("div", { className: "pin-controls sp-col-pin-controls", "data-pinned": N || void 0, children: [
                /* @__PURE__ */ e.jsx("button", { type: "button", style: V(N === "left"), onClick: (A) => _(A, "left"), title: "Pin Column Left", "aria-label": "Pin Column Left", children: /* @__PURE__ */ e.jsx(E.PinLeft, {}) }),
                /* @__PURE__ */ e.jsx("button", { type: "button", style: V(!N), onClick: (A) => _(A, !1), title: "Unpin Column", "aria-label": "Unpin Column", children: /* @__PURE__ */ e.jsx(E.Unpin, {}) }),
                /* @__PURE__ */ e.jsx("button", { type: "button", style: V(N === "right"), onClick: (A) => _(A, "right"), title: "Pin Column Right", "aria-label": "Pin Column Right", children: /* @__PURE__ */ e.jsx(E.PinRight, {}) })
              ] })
            ]
          }
        ),
        C && s && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: t.columns.filter(
          /** @param {any} [child] @returns {any} */
          (A) => it(A, F)
        ).map(
          /** @param {any} [child] @returns {any} */
          (A) => /* @__PURE__ */ e.jsx(
            pr,
            {
              column: A,
              level: n + 1,
              theme: i,
              styles: o,
              handlePinColumn: l,
              colSearch: x,
              selectedCols: f,
              setSelectedCols: j,
              onDrop: y,
              sectionId: F
            },
            A.id
          )
        ) })
      ]
    }
  );
}, xr = /\b[A-Za-z_][A-Za-z0-9_]*\b/g, fr = /^[A-Za-z_][A-Za-z0-9_]*$/, ct = /\[([^\]]+)\]/g, nn = /* @__PURE__ */ new Set(["abs", "round", "ceil", "floor", "max", "min", "int", "float"]), ln = /* @__PURE__ */ new Set(["True", "False", "None", "and", "or", "not", "if", "else", "in", "is"]), Ee = {
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
function gr(t) {
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
function He(t) {
  const n = String(t || "").trim().toLowerCase();
  return ["columns", "display", "displayed", "displayed_columns", "rendered", "rendered_columns"].includes(n) ? "columns" : "measures";
}
function ze(t) {
  return He(t) === "columns";
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
], Qt = /* @__PURE__ */ new Set(["in", "not_in"]), an = /* @__PURE__ */ new Set(["is_null", "is_not_null"]);
function yr(t) {
  return { field: t && t[0] || "", operator: "eq", value: "", values: [], caseSensitive: !1 };
}
function br(t, n = 0) {
  return {
    id: `rule-${Date.now().toString(36)}-${n}`,
    label: n === 0 ? "Group A" : `Group ${n + 1}`,
    condition: { op: "AND", clauses: [yr(t)] }
  };
}
function er(t, n = null) {
  if (n) {
    const o = Ne([n])[0] || n;
    return { ...JSON.parse(JSON.stringify(o)), __editingField: o.field, __editingId: o.id };
  }
  const i = st(`category_${Date.now().toString(36)}`);
  return {
    id: i,
    field: dt(i),
    name: "New Category",
    fallbackLabel: "Other",
    order: "creation",
    rules: [br(t, 0)],
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
        const j = Date.parse(f);
        return !Number.isNaN(j) && /[-/T:]/.test(f) ? "date" : "string";
      }
    }
  }
  return "string";
}
const at = (t, n, i) => (Array.isArray(n) ? n : []).filter((o) => sn(t, o) === i);
function dn(t, n) {
  const o = at(n, t, "date")[0] || t[0] || "", l = st(`${o || "date"}_month`);
  return {
    id: l,
    field: dt(l),
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
  const o = at(n, t, "number")[0] || t[0] || "", l = st(`${o || "number"}_bins`);
  return {
    id: l,
    field: dt(l),
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
const tr = (t) => String(t || "").split(",").map((n) => n.trim()).filter(Boolean), rr = (t) => String(t || "").trim().replace(/\s+/g, " ").toLowerCase(), hr = (t) => t && (t.__editingField || t.field) ? t.__editingField || t.field : "";
function un(t, n) {
  const i = Array.isArray(t) ? t : [], o = hr(n);
  if (!o) return i;
  const l = i.findIndex(
    /** @param {any} [dimension] @returns {any} */
    (x) => x && x.field === o
  );
  return l >= 0 ? i.slice(0, l) : i;
}
function nr(t) {
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
  return Ur(i, n || []);
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
      (o, l) => Vr(l, i.condition) ? o + 1 : o,
      0
    )
  })
), gn = u.lazy(() => import(
  /* webpackChunkName: "pivot-report-editor" */
  "./ReportEditor-DKIDJ3cz.js"
).then((t) => ({
  default: t.ReportEditor
}))), yn = (t) => /* @__PURE__ */ e.jsx(u.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { role: "status", style: { padding: "16px" }, children: "Loading report builder…" }), children: /* @__PURE__ */ e.jsx(gn, { ...t }) });
function lr({
  availableFields: t,
  customDimensions: n,
  setCustomDimensions: i,
  data: o,
  theme: l,
  showNotification: x,
  getFieldLabel: f,
  onDeleteField: j
}) {
  const y = u.useMemo(
    () => (t || []).filter(
      /** @param {any} [field] @returns {any} */
      (a) => a && !ur(a)
    ),
    [t]
  ), F = u.useMemo(
    () => Ne(n || []),
    [n]
  ), [s, b] = u.useState(null), W = u.useMemo(
    () => un(F, s),
    [s, F]
  ), P = u.useMemo(() => {
    const a = /* @__PURE__ */ new Set(), k = [];
    return y.forEach(
      /** @param {any} [field] @returns {any} */
      (w) => {
        w && !a.has(w) && (a.add(w), k.push(w));
      }
    ), W.forEach(
      /** @param {any} [dimension] @returns {any} */
      (w) => {
        w && w.field && !a.has(w.field) && (a.add(w.field), k.push(w.field));
      }
    ), k;
  }, [W, y]), M = u.useMemo(
    () => {
      const a = at(o, y, "date");
      return a.length > 0 ? a : y;
    },
    [y, o]
  ), $ = u.useMemo(
    () => {
      const a = at(o, y, "number");
      return a.length > 0 ? a : y;
    },
    [y, o]
  ), h = u.useMemo(
    () => pn(o, W),
    [W, o]
  ), C = u.useMemo(() => ({
    border: `1px solid ${l.border}`,
    background: l.background,
    color: l.text,
    borderRadius: "8px",
    fontSize: "12px",
    padding: "7px 8px",
    outline: "none",
    minWidth: 0
  }), [l.background, l.border, l.text]), D = u.useCallback((a = "default") => ({
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
    const a = (k) => {
      k.key === "Escape" && (k.preventDefault(), b(null));
    };
    return window.addEventListener("keydown", a), () => window.removeEventListener("keydown", a);
  }, [s]);
  const I = u.useCallback((a, k) => {
    b(
      /** @param {any} [current] @returns {any} */
      (w) => {
        if (!w) return w;
        const B = [...w.rules || []];
        return B[a] = typeof k == "function" ? k(B[a]) : k, { ...w, rules: B };
      }
    );
  }, []), N = u.useCallback((a, k, w) => {
    I(
      a,
      /** @param {any} [rule] @returns {any} */
      (B) => {
        const _ = B.condition || { op: "AND", clauses: [] }, Y = [..._.clauses || []];
        return Y[k] = { ...Y[k], ...w }, { ...B, condition: { ..._, clauses: Y } };
      }
    );
  }, [I]), R = u.useCallback(() => {
    const a = Ne([s])[0];
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
    const k = hr(s), w = rr(a.name);
    if (F.some(
      /** @param {any} [dimension] @returns {any} */
      (_) => _ && _.field !== k && rr(_.name) === w
    )) {
      x && x(`A category named "${a.name}" already exists.`, "warning");
      return;
    }
    if (a.kind === "category") {
      const _ = new Set(P);
      if (nr(a).find(
        /** @param {any} [field] @returns {any} */
        (g) => !_.has(g)
      )) {
        x && x("A category can only use base fields or categories defined before it.", "warning");
        return;
      }
      const L = nr(a);
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
      (_) => [...Ne(_ || []).filter(
        /** @param {any} [dimension] @returns {any} */
        (g) => g.id !== a.id && g.field !== a.field && g.field !== k
      ), a]
    ), b(null), x && x(`Applied ${a.name}.`, "success");
  }, [P, M, s, F, $, i, x, h]), O = u.useCallback((a) => {
    const k = Ne([a])[0];
    k && (i(
      /** @param {any} [previous] @returns {any} */
      (w) => Ne(w || []).filter(
        /** @param {any} [item] @returns {any} */
        (B) => B.field !== k.field
      )
    ), typeof j == "function" && j(k.field), s && s.field === k.field && b(null), x && x(`Removed ${k.name}.`, "success"));
  }, [s, j, i, x]);
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
          onClick: () => b(er(P)),
          style: { ...D("primary"), whiteSpace: "nowrap", flexShrink: 0 },
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
          style: { ...D(), opacity: M.length === 0 ? 0.5 : 1, cursor: M.length === 0 ? "not-allowed" : "pointer" },
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
          style: { ...D(), opacity: $.length === 0 ? 0.5 : 1, cursor: $.length === 0 ? "not-allowed" : "pointer" },
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
        const k = a.kind || "category", w = k === "date_group" ? `${f(a.sourceField)} by ${a.granularity}` : k === "number_group" ? `${f(a.sourceField)} every ${a.binSize}` : `${a.rules.length} rule${a.rules.length === 1 ? "" : "s"} | fallback: ${a.fallbackLabel}`;
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
              k === "number_group" ? /* @__PURE__ */ e.jsx(E.NumberFormat, {}) : /* @__PURE__ */ e.jsx(E.Branch, {}),
              /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0, flex: 1 }, children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: l.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: a.name }),
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: w })
              ] }),
              /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => b(er(y, a)), style: D(), children: [
                /* @__PURE__ */ e.jsx(E.Edit, {}),
                " Edit"
              ] }),
              /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => O(a), style: { ...D(), color: "#B91C1C" }, children: /* @__PURE__ */ e.jsx(E.Delete, {}) })
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
                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => b(null), style: { ...D(), padding: "6px 8px" }, "aria-label": "Close category editor", children: /* @__PURE__ */ e.jsx(E.Close, {}) })
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
                          const k = a.target.value, w = s.id || st(k);
                          b({ ...s, name: k, id: w, field: s.field || dt(w) });
                        },
                        style: C,
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
                        style: C,
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
                        style: { ...C, width: "100%" },
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
                        style: { ...C, width: "100%" },
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
                        style: { ...C, width: "100%" },
                        children: Array.from(Jr).map((a) => /* @__PURE__ */ e.jsx("option", { value: a, children: ke(a) }, a))
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
                        style: { ...C, width: "100%" },
                        children: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"].map((a, k) => /* @__PURE__ */ e.jsx("option", { value: k + 1, children: a }, a))
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
                        style: { ...C, width: "100%" },
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
                        style: C
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
                        style: C
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
                        style: C
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
                        style: C
                      }
                    )
                  ] })
                ] }),
                (!s.kind || s.kind === "category") && (s.rules || []).map(
                  /** @param {any} [rule] @param {any} [ruleIndex] @returns {any} */
                  (a, k) => {
                    const w = a.condition || { op: "AND", clauses: [] }, B = w.clauses || [];
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
                            onChange: (_) => I(k, { ...a, label: _.target.value }),
                            style: C,
                            placeholder: "Category value"
                          }
                        ),
                        /* @__PURE__ */ e.jsxs(
                          ne,
                          {
                            size: "sm",
                            "aria-label": "Rule condition combiner",
                            value: w.op || "AND",
                            onChange: (_) => I(k, { ...a, condition: { ...w, op: _.target.value } }),
                            style: C,
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
                              (_, Y) => Y !== k
                            ) }),
                            style: { ...D(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                            title: "Remove rule",
                            children: /* @__PURE__ */ e.jsx(E.Close, {})
                          }
                        )
                      ] }),
                      B.map(
                        /** @param {any} [clause] @param {any} [clauseIndex] @returns {any} */
                        (_, Y) => {
                          const L = an.has(_.operator), g = Qt.has(_.operator);
                          return /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: L ? "1fr 130px 28px" : "1fr 130px 1fr 28px", gap: "6px", alignItems: "center" }, children: [
                            /* @__PURE__ */ e.jsx(
                              ne,
                              {
                                size: "sm",
                                "aria-label": "Clause field",
                                value: _.field || "",
                                onChange: (z) => N(k, Y, { field: z.target.value }),
                                style: { ...C, width: "100%" },
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
                                  const V = z.target.value;
                                  N(k, Y, {
                                    operator: V,
                                    values: Qt.has(V) ? _.values && _.values.length ? _.values : tr(_.value) : _.values
                                  });
                                },
                                style: { ...C, width: "100%" },
                                children: on.map((z) => /* @__PURE__ */ e.jsx("option", { value: z.value, children: z.label }, z.value))
                              }
                            ),
                            !L && /* @__PURE__ */ e.jsx(
                              "input",
                              {
                                value: g ? (_.values || []).join(", ") : _.value || "",
                                onChange: (z) => N(k, Y, g ? { values: tr(z.target.value), value: z.target.value } : { value: z.target.value }),
                                style: C,
                                placeholder: g ? "A, B, C" : "Value"
                              }
                            ),
                            /* @__PURE__ */ e.jsx(
                              "button",
                              {
                                type: "button",
                                onClick: () => I(k, {
                                  ...a,
                                  condition: {
                                    ...w,
                                    clauses: B.filter(
                                      /** @param {any} [_] @param {any} [index] @returns {any} */
                                      (z, V) => V !== Y
                                    )
                                  }
                                }),
                                style: { ...D(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                                title: "Remove condition",
                                children: /* @__PURE__ */ e.jsx(E.Close, {})
                              }
                            )
                          ] }, `${a.id || k}:${Y}`);
                        }
                      ),
                      /* @__PURE__ */ e.jsx(
                        "button",
                        {
                          type: "button",
                          onClick: () => I(k, {
                            ...a,
                            condition: {
                              ...w,
                              clauses: [...B, yr(P)]
                            }
                          }),
                          style: { ...D(), alignSelf: "flex-start" },
                          children: "+ Condition"
                        }
                      )
                    ] }, a.id || k);
                  }
                ),
                (!s.kind || s.kind === "category") && /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => b({
                      ...s,
                      rules: [...s.rules || [], br(P, (s.rules || []).length)]
                    }),
                    style: { ...D(), alignSelf: "flex-start" },
                    children: "+ Rule"
                  }
                ),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "flex-end", gap: "8px" }, children: [
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => b(null), style: D(), children: "Cancel" }),
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: R, style: D("primary"), children: s.kind === "date_group" || s.kind === "number_group" ? "Apply Group" : "Apply Category" })
                ] })
              ] })
            ]
          }
        )
      }
    )
  ] });
}
function mr(t) {
  const n = [], i = /* @__PURE__ */ new Set();
  return String(t || "").replace(ct, (o, l) => {
    const x = String(l || "").trim();
    return x && !i.has(x) && (n.push(x), i.add(x)), "";
  }), n;
}
function or(t, n = []) {
  const i = String(t || ""), o = [], l = /* @__PURE__ */ new Set(), x = (y) => {
    const F = String(y || "").trim();
    !F || l.has(F) || (o.push(F), l.add(F));
  };
  mr(i).forEach(x);
  let f = i.replace(ct, " ");
  return (Array.isArray(n) ? n : Array.from(n || [])).map((y) => String(y || "").trim()).filter((y) => y && !fr.test(y)).sort((y, F) => F.length - y.length).forEach(
    /** @param {any} [reference] @returns {any} */
    (y) => {
      const F = new RegExp(`(^|[^A-Za-z0-9_])${gr(y)}(?=$|[^A-Za-z0-9_])`, "g");
      f = f.replace(
        F,
        /** @param {any} [match] @param {any} [prefix] @returns {any} */
        (s, b) => (x(y), `${b} `)
      );
    }
  ), f.replace(
    xr,
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
  return n ? fr.test(n) ? n : `[${n}]` : "";
}
function bn(t, n, i = "formula") {
  const o = Me(t, i), l = new Set(
    (Array.isArray(n) ? n : []).map((j) => j ? j.agg === "formula" ? J(j) : j.field : null).filter(Boolean).map((j) => String(j).toLowerCase())
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
  let f = String(t).replace(ct, (j, y) => String(y || "").trim() === n ? o : (l.push(j), `@@${l.length - 1}@@`)).replace(new RegExp(`\\b${gr(n)}\\b`, "g"), o);
  return l.forEach((j, y) => {
    f = f.replace(`@@${y}@@`, j);
  }), f;
}
function Ct(t, n, i) {
  t(
    /** @param {any} [prev] @returns {any} */
    (o) => {
      const l = [...o];
      return l[n] ? (l[n] = i(l[n]), l) : o;
    }
  );
}
function ir(t) {
  return !t || typeof t.closest != "function" ? !1 : !!t.closest('button, input, select, textarea, [contenteditable="true"]');
}
function mn(t) {
  if (!t || typeof t != "object") return "";
  const n = t.field === void 0 || t.field === null ? "" : String(t.field), i = t.agg === void 0 || t.agg === null ? "" : String(t.agg);
  return `${n}::${i}`;
}
function Rt(t) {
  const n = tn(t);
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
function vr(t, n = []) {
  const i = String(t && t.agg || "").trim().toLowerCase(), o = Array.isArray(n) && n.length > 0, l = i === "array_agg", x = i !== "array_agg" && o;
  return {
    canUseFieldSeries: l,
    canUsePivotSeries: x,
    canUseAny: l || x,
    defaultSource: l ? "field" : x ? "pivot" : null
  };
}
function At(t, n) {
  return !n || !n.canUseAny ? !1 : t === "field" ? n.canUseFieldSeries : n.canUsePivotSeries;
}
function Sr(t, n) {
  const i = vn(t);
  return !n || !n.canUseAny ? null : At(i.source, n) ? i : {
    ...i,
    source: n.defaultSource
  };
}
function Dt(t) {
  if (!Array.isArray(t) || t.length === 0) return "";
  const n = t.map((i) => i && Fe(i.field)).filter(Boolean);
  return n.length === 0 ? "" : n.length === 1 ? `${n[0]} * 100` : `${n[0]} - ${n[1]}`;
}
function ar(t) {
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
  const j = String(t || "").trim(), y = ze(l);
  if (!j)
    return {
      tone: "warning",
      text: y ? "Enter a formula using bracketed rendered columns. Example: [Cash Equity_day_pnl_sum] - [ETF_day_pnl_sum]" : "Enter a formula using value fields. Example: sales - [day pnl]"
    };
  let F = 0;
  for (const R of j)
    if (R === "(" && (F += 1), R === ")" && (F -= 1), F < 0)
      return { tone: "warning", text: "Parentheses are unbalanced." };
  if (F !== 0)
    return { tone: "warning", text: "Parentheses are unbalanced." };
  if (y) {
    const R = Array.isArray(x) ? x : [], O = new Set(
      R.map(
        /** @param {any} [option] @returns {any} */
        (B) => B && B.id
      ).filter(Boolean)
    ), a = mr(j);
    if (a.length === 0)
      return {
        tone: "warning",
        text: "Insert at least one rendered column reference like [column_id]."
      };
    const k = j.replace(ct, ""), w = Array.from(new Set(k.match(xr) || []));
    if (w.length > 0)
      return {
        tone: "warning",
        text: `Rendered columns must use brackets: ${w.join(", ")}`
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
  const s = Array.isArray(n) ? n.filter((R) => R && R.field) : [];
  if (s.length === 0)
    return {
      tone: "warning",
      text: "Add at least one value or formula reference before creating formulas."
    };
  const b = s.filter(
    /** @param {any} [config] @returns {any} */
    (R) => R.agg === "formula"
  ), W = /* @__PURE__ */ new Map();
  b.forEach(
    /** @param {any} [config] @returns {any} */
    (R) => {
      const O = R.field === i ? Me(o || J(R), R.field) : J(R);
      O && (W.set(O, O), W.set(O.toLowerCase(), O));
    }
  );
  const P = s.filter(
    /** @param {any} [config] @returns {any} */
    (R) => R.agg !== "formula"
  ).map(
    /** @param {any} [config] @returns {any} */
    (R) => R.field
  ).filter(Boolean), M = new Set(P), $ = /* @__PURE__ */ new Set([...P, ...b.map(
    /** @param {any} [config] @returns {any} */
    (R) => R.field === i ? Me(o || J(R), R.field) : J(R)
  ).filter(Boolean), ...Array.isArray(f) ? f : []]), h = Array.from($), C = or(j, h), D = C.map(
    /** @param {any} [token] @returns {any} */
    (R) => W.get(R) || W.get(R.toLowerCase()) || R
  ), I = D.filter(
    /** @param {any} [token] @returns {any} */
    (R) => !$.has(R) && !M.has(R)
  );
  if (I.length > 0)
    return {
      tone: "warning",
      text: `Unknown fields: ${I.join(", ")}`
    };
  const N = i ? Me(o || i, i) : null;
  if (i && N && s.some(
    /** @param {any} [config] @returns {any} */
    (O) => {
      if (!O || O.field === i) return !1;
      const a = O.agg === "formula" ? J(O) : O.field;
      return String(a || "").toLowerCase() === N.toLowerCase();
    }
  ))
    return {
      tone: "warning",
      text: `Reference key "${N}" is already in use.`
    };
  if (N && D.includes(N))
    return {
      tone: "warning",
      text: "A formula cannot reference itself."
    };
  if (i && N) {
    const R = {};
    b.forEach(
      /** @param {any} [config] @returns {any} */
      (w) => {
        const B = w.field === i ? N : J(w), _ = w.field === i ? j : String(w.formula || ""), Y = or(_, h).map(
          /** @param {any} [token] @returns {any} */
          (L) => W.get(L) || W.get(L.toLowerCase()) || L
        ).filter(
          /** @param {any} [token] @returns {any} */
          (L) => L !== B && b.some(
            /** @param {any} [candidate] @returns {any} */
            (g) => J(g) === L || g.field === i && N === L
          )
        );
        R[B] = Y;
      }
    );
    const O = /* @__PURE__ */ new Set(), a = /* @__PURE__ */ new Set(), k = (w) => {
      if (O.has(w)) return !0;
      if (a.has(w)) return !1;
      O.add(w);
      const B = R[w] || [];
      for (const _ of B)
        if (k(_)) return !0;
      return O.delete(w), a.add(w), !1;
    };
    if (k(N))
      return {
        tone: "warning",
        text: "Circular formula reference detected."
      };
  }
  return C.length === 0 ? {
    tone: "warning",
    text: "Insert at least one measure or formula reference."
  } : {
    tone: "success",
    text: `Using ${D.join(", ")}`
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
    () => Rt(t && t.sparkline) || { ...Ee },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [t && t.sparkline]
  ), j = u.useMemo(
    () => vr(t, l),
    [l, t]
  ), y = u.useMemo(() => {
    const h = [];
    return j.canUsePivotSeries && h.push({ value: "pivot", label: "Linked pivot" }), j.canUseFieldSeries && h.push({ value: "field", label: "Series field" }), h;
  }, [j]), F = At(f.source || "pivot", j) ? f.source || "pivot" : j.defaultSource, s = u.useCallback((h) => {
    Ct(
      i,
      n,
      /** @param {any} [current] @returns {any} */
      (C) => {
        const D = Rt(C.sparkline) || { ...Ee }, I = Sr({ ...D, ...h }, j);
        return I ? {
          ...C,
          sparkline: I
        } : C;
      }
    );
  }, [j, n, i]), b = u.useCallback((h) => h.stopPropagation(), []), W = [
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
                  ({ value: h, label: C }) => /* @__PURE__ */ e.jsx(
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
                      children: C
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
              ].map(({ value: h, label: C }) => /* @__PURE__ */ e.jsx(
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
                  children: C
                },
                h
              )) }),
              /* @__PURE__ */ e.jsx("div", { style: { marginTop: "5px", fontSize: "10px", color: o.textSec, lineHeight: 1.4 }, children: (f.displayMode || "trend") === "value" ? "Show the selected summary metric from the series." : "Draw the series in the cell." })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Chart type" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: W.map(({ value: h, label: C }) => /* @__PURE__ */ e.jsx(
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
                  children: C
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
              ].map(({ key: h, label: C }) => /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "5px", fontSize: "11px", color: o.text, cursor: "pointer", userSelect: "none" }, children: [
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "checkbox",
                    checked: !!f[h],
                    onChange: (D) => s({ [h]: D.target.checked }),
                    style: { accentColor: o.primary, width: "13px", height: "13px", cursor: "pointer" }
                  }
                ),
                C
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
              ].filter(({ always: h }) => h || f.type === "column" || f.type === "bar").map(({ key: h, label: C }) => /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: o.textSec, marginBottom: "3px", display: "block" }, children: C }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "5px", alignItems: "center" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "color",
                      value: f[h] || (h === "negativeColor" ? "#B91C1C" : h === "positiveColor" ? "#2F855A" : "#2563EB"),
                      onChange: (D) => s({ [h]: D.target.value }),
                      style: { width: "28px", height: "26px", padding: "1px 2px", border: `1px solid ${o.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "text",
                      value: f[h] || "",
                      onChange: (D) => s({ [h]: D.target.value }),
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
function jr({ item: t, idx: n, setValConfigs: i, theme: o, colFields: l = [], fixedSparklineValueKeySet: x = /* @__PURE__ */ new Set(), showNotification: f }) {
  const [j, y] = u.useState(!1), F = Rt(t && t.sparkline), s = mn(t), b = !!(s && x && x.has(s)), W = u.useMemo(
    () => vr(t, l),
    [l, t]
  ), P = !!F, M = !!(F && At(F.source || "pivot", W)), $ = u.useCallback((I) => I.stopPropagation(), []), h = u.useCallback(() => {
    let I = !1;
    return Ct(
      i,
      n,
      /** @param {any} [current] @returns {any} */
      (N) => {
        const R = Sr(
          N.sparkline || Ee,
          W
        );
        return R ? (I = !0, { ...N, sparkline: R }) : N;
      }
    ), I;
  }, [W, n, i]), C = u.useCallback((I) => {
    if (I.stopPropagation(), P) {
      if (b) {
        f && f("This trend is defined by the app. You can adjust its settings, but not remove it here.", "info");
        return;
      }
      Ct(
        i,
        n,
        /** @param {any} [current] @returns {any} */
        (N) => ({ ...N, sparkline: !1 })
      ), f && f("Removed trend column.", "info");
    } else W.canUseAny && h() ? (y(!0), f && f("Trend column enabled.", "success")) : f && f("Trend needs a column field, or a Series aggregation for field data.", "warning");
  }, [W.canUseAny, P, h, n, b, i, f]), D = u.useCallback((I) => {
    if (I.stopPropagation(), !W.canUseAny) {
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
            onClick: D,
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
            onClick: C,
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
        M && !j && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: o.primary, flex: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: [
          (t.sparkline && t.sparkline.source) === "field" ? `series ${(t.sparkline && t.sparkline.displayMode) === "value" ? "value" : "trend"}` : "linked pivot",
          " | ",
          ["line", "area", "column", "bar"].includes(t.sparkline && t.sparkline.type) ? t.sparkline.type : "line",
          " | ",
          t.sparkline && t.sparkline.metric || "last"
        ] }),
        j && /* @__PURE__ */ e.jsx(
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
function sr({ item: t, idx: n, selected: i, onSelect: o, onRemove: l, setValConfigs: x, theme: f, styles: j, dropLine: y, zoneId: F, valueSelectStyle: s, colFields: b = [], fixedSparklineValueKeySet: W = /* @__PURE__ */ new Set(), showNotification: P, serverError: M = null }) {
  const $ = i ? `${f.primary}77` : M ? "#F59E0B88" : f.border, h = i ? f.headerBg || `${f.primary}0f` : f.background || "transparent", C = String(t.formula || "").trim() || "Empty formula";
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      onClick: () => o(t.field),
      style: {
        ...j.chip,
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
        y && y.zone === F && y.idx === n && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...j.dropLine, top: -2 } }),
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
              C
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
              onClick: (D) => {
                D.stopPropagation(), l(n);
              },
              style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" },
              children: /* @__PURE__ */ e.jsx(E.Close, {})
            }
          )
        ] }),
        /* @__PURE__ */ e.jsx(
          jr,
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
        y && y.zone === F && y.idx === n + 1 && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...j.dropLine, bottom: -2 } })
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
function An({ item: t, idx: n, valConfigs: i, setValConfigs: o, theme: l, onClose: x, onRemove: f, displayedColumnOptions: j = [], availableRawFields: y = [], serverError: F = null }) {
  const s = u.useRef(null), [b, W] = u.useState({
    label: t.label || "",
    formula: t.formula || "",
    formulaRef: J(t),
    formulaScope: He(t.formulaScope)
  }), P = u.useRef(t);
  P.current = t, u.useEffect(() => {
    const g = P.current;
    W({
      label: g.label || "",
      formula: g.formula || "",
      formulaRef: J(g),
      formulaScope: He(g.formulaScope)
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
  ), C = u.useMemo(
    () => (Array.isArray(j) ? j : []).filter((g) => g && g.id && g.id !== t.field),
    [j, t.field]
  ), D = u.useMemo(
    () => {
      const g = new Set(M.map(
        /** @param {any} [c] @returns {any} */
        (z) => z.field
      ).filter(Boolean));
      return (Array.isArray(y) ? y : []).filter((z) => !g.has(z));
    },
    [y, M]
  ), I = u.useMemo(
    () => jn(
      b.formula,
      M,
      t.field,
      b.formulaRef,
      b.formulaScope,
      C,
      D
    ),
    [C, b.formula, b.formulaRef, b.formulaScope, t.field, M, D]
  ), N = u.useMemo(
    () => Cn(l, I.tone),
    [I.tone, l]
  ), R = u.useMemo(
    () => ze(b.formulaScope) ? wn(C) : kn($),
    [C, b.formulaScope, $]
  ), O = u.useMemo(
    () => ze(b.formulaScope) ? ar(C) || "[column_id] - [other_column_id]" : Dt($) || `${Fe(b.formulaRef || t.field)} * 100`,
    [C, b.formulaRef, b.formulaScope, t.field, $]
  ), a = b.label !== (t.label || "") || b.formula !== (t.formula || "") || b.formulaRef !== J(t) || b.formulaScope !== He(t.formulaScope), k = u.useCallback(() => {
    const g = Me(b.formulaRef, t.field);
    o(
      /** @param {any} [prev] @returns {any} */
      (z) => {
        const V = [...z], A = V[n];
        if (!A) return z;
        const H = J(A);
        let Z = g;
        const Q = new Set(
          V.filter((U, ee) => U && ee !== n).map((U) => U.agg === "formula" ? J(U) : U.field).filter(Boolean).map((U) => String(U).toLowerCase())
        );
        if (Q.has(Z.toLowerCase())) {
          let U = 2, ee = `${Z}${U}`;
          for (; Q.has(ee.toLowerCase()); )
            U += 1, ee = `${Z}${U}`;
          Z = ee;
        }
        if (V[n] = {
          ...A,
          label: b.label,
          formula: b.formula,
          formulaRef: Z,
          formulaScope: b.formulaScope
        }, H && H !== Z)
          for (let U = 0; U < V.length; U += 1) {
            if (U === n) continue;
            const ee = V[U];
            !ee || ee.agg !== "formula" || !ee.formula || (V[U] = {
              ...ee,
              formula: hn(ee.formula, H, Z)
            });
          }
        return V;
      }
    );
  }, [b.formula, b.formulaRef, b.formulaScope, b.label, n, t.field, o]), w = u.useCallback(() => {
    W({
      label: t.label || "",
      formula: t.formula || "",
      formulaRef: J(t),
      formulaScope: He(t.formulaScope)
    }), x();
  }, [t.field, t.formula, t.formulaRef, t.formulaScope, t.label, x]), B = u.useCallback((g) => {
    const z = s.current;
    if (!z) return;
    const V = b.formula || "", A = z.selectionStart ?? V.length, H = z.selectionEnd ?? V.length, Z = `${V.slice(0, A)}${g}${V.slice(H)}`;
    W(
      /** @param {any} [prev] @returns {any} */
      (U) => ({ ...U, formula: Z })
    );
    const Q = A + g.length;
    setTimeout(() => {
      s.current && (s.current.focus(), s.current.setSelectionRange(Q, Q));
    }, 0);
  }, [b.formula]), _ = u.useCallback(() => {
    k(), x();
  }, [k, x]), Y = {
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
      onClick: w,
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
            g.key === "Escape" && w(), (g.ctrlKey || g.metaKey) && g.key === "Enter" && _();
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
                  onClick: w,
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
                      const z = He(g.target.value);
                      W(
                        /** @param {any} [prev] @returns {any} */
                        (V) => ({
                          ...V,
                          formulaScope: z,
                          formula: V.formula || (z === "columns" ? ar(C) : Dt($))
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
                border: `1px solid ${N.border}`,
                background: N.background,
                color: N.text,
                fontSize: "10px",
                lineHeight: "14px"
              }, children: I.text }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: "Ctrl/Cmd + Enter applies" })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              R.map(
                /** @param {any} [template] @returns {any} */
                (g) => /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => W(
                      /** @param {any} [prev] @returns {any} */
                      (z) => ({ ...z, formula: g.value })
                    ),
                    style: Y,
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
                  C.map(
                    /** @param {any} [column] @returns {any} */
                    (g) => /* @__PURE__ */ e.jsx(
                      "button",
                      {
                        type: "button",
                        onMouseDown: (z) => z.preventDefault(),
                        onClick: () => B(se(g.id)),
                        style: Y,
                        title: `Insert ${g.id}`,
                        children: g.label || g.id
                      },
                      g.id
                    )
                  ),
                  C.length === 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: l.textSec }, children: "Render the pivot once, or type a bracketed column id directly." })
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
                      style: Y,
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
                      style: Y,
                      title: `${J(g)}${g.label ? ` (${g.label})` : ""}`,
                      children: J(g)
                    },
                    g.field
                  )
                ) })
              ] }),
              !ze(b.formulaScope) && D.length > 0 && /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Data Fields (auto-aggregated as sum)" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: D.map(
                  /** @param {any} [f] @returns {any} */
                  (g) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(Fe(g)),
                      style: { ...Y, borderColor: `${l.primary}66`, color: l.primary, background: `${l.primary}0d` },
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
                    onClick: w,
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
function dr({
  panelId: t,
  panelLabel: n,
  size: i,
  onSizeChange: o,
  getSidebarWidth: l = null,
  onSidebarWidthChange: x = null,
  theme: f,
  outerStyle: j = {},
  contentStyle: y = {},
  children: F
}) {
  const s = u.useRef(null), b = u.useRef(null), W = u.useRef(null), P = u.useRef(null), M = u.useRef(null), $ = u.useRef(null), h = u.useRef(null), [C, D] = u.useState(null), [I, N] = u.useState(null), R = u.useMemo(() => Qr(t), [t]), O = u.useMemo(
    () => Jt(t, i),
    [t, i]
  ), a = u.useRef(O);
  a.current = O;
  const k = u.useMemo(() => C ? Jt(t, {
    width: C.width !== void 0 ? C.width : O.width,
    height: C.height !== void 0 ? C.height : O.height
  }) : O, [C, t, O]), w = u.useCallback((L = !0) => {
    if (b.current) {
      const g = b.current;
      b.current = null, g(L);
    }
  }, []);
  u.useEffect(() => (() => w(!1)), [w]);
  const B = u.useCallback((L, g) => {
    if (!s.current || typeof o != "function") return;
    L.preventDefault(), L.stopPropagation(), w(!1);
    const z = s.current.getBoundingClientRect(), V = s.current.parentElement ? s.current.parentElement.getBoundingClientRect() : z, A = typeof l == "function" ? Number(l()) : NaN, H = Number.isFinite(A) ? Math.max(200, Math.min(600, Math.round(A))) : null, Z = H !== null ? Math.max(0, Math.round(H - V.width)) : 0, Q = typeof x == "function" && H !== null;
    H !== null && (a.current.width === null || $.current === null) && ($.current = H, h.current = Z);
    const U = H !== null ? $.current !== null ? Math.min(H, $.current) : H : null, ee = h.current !== null ? h.current : Z, $t = Q ? Math.max(R.minWidth, Math.min(R.maxWidth, 600 - ee)) : Math.max(R.minWidth, Math.min(R.maxWidth, Math.floor(V.width))), le = {
      startX: L.clientX,
      startY: L.clientY,
      startWidth: z.width,
      startHeight: z.height,
      startParentWidth: Math.floor(V.width),
      startSidebarWidth: H,
      baseSidebarWidth: U,
      chromeWidth: ee,
      canExpandSidebar: Q,
      maxWidth: $t,
      initialWidthProp: a.current.width
    }, we = document.body.style.cursor, _e = document.body.style.userSelect;
    document.body.style.cursor = g === "both" ? "nwse-resize" : g === "width" ? "ew-resize" : "ns-resize", document.body.style.userSelect = "none", P.current = null, M.current = null, N(g);
    const Ce = () => {
      W.current = null, P.current && D(P.current), M.current !== null && typeof x == "function" && (x(M.current), M.current = null);
    }, fe = (ge) => {
      const ye = ge.clientX - le.startX, ve = ge.clientY - le.startY, Ue = {};
      if (g === "width" || g === "both" && (le.initialWidthProp !== null || Math.abs(ye) >= 3)) {
        const Se = Math.max(R.minWidth, Math.min(le.maxWidth, Math.round(le.startWidth + ye)));
        if (Ue.width = Se, s.current && (s.current.style.width = `${Se}px`), le.canExpandSidebar) {
          const ut = le.baseSidebarWidth !== null ? le.baseSidebarWidth : le.startSidebarWidth, pt = Math.max(
            ut,
            Math.min(600, Se + le.chromeWidth)
          );
          M.current = pt;
        }
      }
      if (g === "height" || g === "both") {
        const Se = Math.max(R.minHeight, Math.min(R.maxHeight, Math.round(le.startHeight + ve)));
        Ue.height = Se, s.current && (s.current.style.height = `${Se}px`);
      }
      Object.keys(Ue).length !== 0 && (P.current = Ue, typeof window < "u" && typeof window.requestAnimationFrame == "function" ? W.current === null && (W.current = window.requestAnimationFrame(Ce)) : Ce());
    }, Re = (ge = !0) => {
      W.current !== null && typeof window < "u" && typeof window.cancelAnimationFrame == "function" && (window.cancelAnimationFrame(W.current), W.current = null), document.body.style.cursor = we, document.body.style.userSelect = _e, window.removeEventListener("mousemove", fe), window.removeEventListener("mouseup", ce), window.removeEventListener("blur", ce);
      const ye = P.current, ve = M.current;
      P.current = null, M.current = null, N(null), D(null), ge ? (ve !== null && typeof x == "function" && x(ve), ye && o(t, ye)) : s.current && (s.current.style.width = a.current.width === null ? "100%" : `${a.current.width}px`, s.current.style.height = `${a.current.height}px`);
    }, ce = () => w(!0);
    b.current = Re, window.addEventListener("mousemove", fe), window.addEventListener("mouseup", ce), window.addEventListener("blur", ce);
  }, [
    l,
    R.maxHeight,
    R.maxWidth,
    R.minHeight,
    R.minWidth,
    x,
    o,
    t,
    w
  ]), _ = u.useCallback((L, g) => {
    g && (g.preventDefault(), g.stopPropagation()), w(!1), typeof o == "function" && (L === "width" ? (o(t, { width: null }), $.current !== null && typeof x == "function" && (x($.current), $.current = null, h.current = null)) : L === "height" ? o(t, { height: null }) : (o(t, { width: null, height: null }), $.current !== null && typeof x == "function" && (x($.current), $.current = null, h.current = null)));
  }, [x, o, t, w]), Y = `${f.primary}55`;
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      ref: s,
      style: {
        position: "relative",
        flexShrink: 0,
        width: k.width === null ? "100%" : `${k.width}px`,
        maxWidth: "100%",
        height: `${k.height}px`,
        minHeight: `${R.minHeight}px`,
        maxHeight: `${R.maxHeight}px`,
        boxSizing: "border-box",
        overflow: "hidden",
        ...j
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
            "data-resizing": I === "width" ? "true" : void 0,
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
            children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "2px", height: "24px", borderRadius: "999px", background: Y, opacity: 0.45, pointerEvents: "none" } })
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": I === "height" ? "true" : void 0,
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
            children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "28px", height: "2px", borderRadius: "999px", background: Y, opacity: 0.45, pointerEvents: "none" } })
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": I === "both" ? "true" : void 0,
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
                  background: Y,
                  opacity: 0.55,
                  boxShadow: `-4px 0 0 ${Y}, 0 -4px 0 ${Y}, -4px -4px 0 ${Y}`,
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
function cr({ measureAxis: t, setMeasureAxis: n, theme: i, valueSelectStyle: o, regularValueCount: l }) {
  const x = u.useMemo(() => Je(t), [t]), f = u.useCallback((y) => {
    typeof n == "function" && n(
      /** @param {any} [previous] @returns {any} */
      (F) => ({
        ...Je(F),
        ...typeof y == "function" ? y(Je(F)) : y
      })
    );
  }, [n]), j = {
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
          style: j,
          title: "Measure label field"
        }
      ),
      /* @__PURE__ */ e.jsx(
        "input",
        {
          value: x.valueField,
          onChange: (y) => f({ valueField: y.target.value }),
          style: { ...j, width: "82px" },
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
    valConfigs: j,
    setValConfigs: y,
    measureAxis: F = {},
    setMeasureAxis: s = null,
    columnVisibility: b,
    setColumnVisibility: W,
    columnPinning: P,
    setColumnPinning: M,
    availableFields: $,
    table: h,
    pinningPresets: C,
    showNotification: D,
    filterAnchorEl: I,
    setFilterAnchorEl: N,
    colSearch: R,
    setColSearch: O,
    colTypeFilter: a,
    setColTypeFilter: k,
    selectedCols: w,
    setSelectedCols: B,
    dropLine: _,
    onDragStart: Y,
    onDragOver: L,
    onDrop: g,
    onDragEnd: z,
    onKeyboardFieldDrop: V,
    handleHeaderFilter: A,
    handleFilterClick: H,
    requestFilterOptions: Z,
    handleExpandAllRows: Q,
    handlePinColumn: U,
    toggleAllColumnsPinned: ee,
    activeFilterCol: $t,
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
    reportEditorSelection: ut = null,
    showReportConfigColumn: pt = !0,
    setShowReportConfigColumn: kr = null,
    formulaErrors: We = {},
    serverSide: Qe = !1
  }) {
    Kr("SidebarPanel");
    const { theme: d, styles: G } = Yr(), {
      filters: ue,
      setFilters: xt,
      pivotMode: wr,
      reportDef: Cr,
      setReportDef: Rr,
      savedReports: Dr,
      setSavedReports: Ar,
      activeReportId: $r,
      setActiveReportId: zr,
      customDimensions: Ve,
      setCustomDimensions: zt
    } = Zr(), [et, Ft] = u.useState({ columnId: null, anchorEl: null }), [Ie, Te] = u.useState(null), [be, De] = u.useState(null), [Ae, ft] = u.useState("report"), tt = u.useRef(null), [Fr, rt] = u.useState(null), pe = u.useRef(null), Et = u.useRef(0), Ke = u.useRef(null), he = u.useRef(null), [q, gt] = u.useState(null), [yt, Mt] = u.useState(""), bt = u.useMemo(
      () => new Set(Array.isArray(ve) ? ve : []),
      [ve]
    ), te = u.useCallback(
      (r) => Gr(r, Ve),
      [Ve]
    ), _t = u.useMemo(
      () => Je(F),
      [F]
    ), je = _t.labelField || "Measure Name", Be = u.useMemo(
      () => ($ || []).filter(
        /** @param {any} [field] @returns {any} */
        (r) => r
      ),
      [$]
    ), re = u.useMemo(
      () => Be.filter(
        /** @param {any} [field] @returns {any} */
        (r) => r && !ur(r)
      ),
      [Be]
    ), Ye = Array.isArray(j) && j.length > 0 && je && !Be.includes(je), Le = u.useMemo(
      () => Ye ? [je, ...Be] : Be,
      [Be, je, Ye]
    ), Ze = yt.trim().toLowerCase(), nt = u.useCallback((r) => {
      if (!Ze) return !0;
      const p = Ye && r === je ? "measure name measure names metric metrics value values" : "";
      return String(r || "").toLowerCase().includes(Ze) || String(te(r) || "").toLowerCase().includes(Ze) || p.includes(Ze);
    }, [te, je, Ze, Ye]), ht = u.useMemo(
      () => Le.length > 8 ? Le.filter(nt) : Le,
      [Le, nt]
    ), mt = u.useMemo(
      () => re.length > 8 ? re.filter(nt) : re,
      [re, nt]
    ), Wt = u.useCallback((r) => {
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
      ), xt(
        /** @param {any} [prev] @returns {any} */
        (p) => {
          if (!p || !Object.prototype.hasOwnProperty.call(p, r)) return p;
          const c = { ...p };
          return delete c[r], c;
        }
      );
    }, [f, xt, l, y]), Er = u.useCallback((r) => {
      const p = pe.current;
      if (!p) return;
      const c = 60, m = 12, v = p.getBoundingClientRect(), T = r.clientY, X = T - v.top, S = v.bottom - T;
      let K = 0;
      if (X < c && X >= 0 ? K = -1 : S < c && S >= 0 && (K = 1), K === 0) {
        he.current && (cancelAnimationFrame(he.current), he.current = null);
        return;
      }
      const oe = () => {
        pe.current && (pe.current.scrollTop += K * m, he.current = requestAnimationFrame(oe));
      };
      he.current || (he.current = requestAnimationFrame(oe));
    }, []), It = u.useCallback(() => {
      he.current && (cancelAnimationFrame(he.current), he.current = null);
    }, []), xe = u.useMemo(
      () => j.filter(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg === "formula"
      ),
      [j]
    ), $e = u.useMemo(
      () => xe.length,
      [xe]
    ), Pe = u.useMemo(
      () => j.filter(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg !== "formula"
      ).length,
      [j]
    ), Mr = u.useMemo(() => [
      { id: "rows", label: "Rows", icon: /* @__PURE__ */ e.jsx(E.List, {}) },
      { id: "cols", label: "Columns", icon: /* @__PURE__ */ e.jsx(E.Columns, {}) },
      { id: "vals", label: "Values", icon: /* @__PURE__ */ e.jsx(E.Sigma, {}) },
      { id: "filter", label: "Filters", icon: /* @__PURE__ */ e.jsx(E.Filter, {}) }
    ], []), Ge = u.useMemo(() => ({
      filter: Object.keys(ue || {}).filter((r) => r !== "global"),
      rows: o || [],
      cols: x || [],
      vals: j || []
    }), [x, ue, o, j]), lt = u.useCallback(() => {
      q && D && D("Keyboard move cancelled.", "info"), gt(null);
    }, [q, D]), Tt = u.useCallback((r, p, c, m) => {
      if (typeof V != "function") return;
      gt({ field: r, zone: p, idx: c, label: m }), D && D(`Picked up ${m}. Focus a target field or zone and press Enter.`, "info");
    }, [V, D]), ot = u.useCallback((r, p = null) => {
      if (!q || typeof V != "function") return !1;
      const c = Ge[r] || [], m = Number(p), v = Number.isFinite(m) ? Math.max(0, Math.min(Math.floor(m), c.length)) : c.length, T = V({
        field: q.field,
        srcZone: q.zone,
        srcIdx: q.idx,
        targetZone: r,
        targetIdx: v,
        source: "layout:keyboard-drop"
      });
      return T !== !1 && (D && D(`Moved ${q.label}.`, "success"), gt(null)), T !== !1;
    }, [q, V, D, Ge]), Bt = u.useCallback((r, p, c, m, v) => {
      if (!ir(r.target)) {
        if (r.key === "Escape") {
          r.preventDefault(), r.stopPropagation(), lt();
          return;
        }
        r.key !== "Enter" && r.key !== " " || (r.preventDefault(), r.stopPropagation(), q ? ot(c, m) : Tt(p, c, m, v));
      }
    }, [lt, ot, q, Tt]), _r = u.useCallback((r, p) => {
      if (!ir(r.target)) {
        if (r.key === "Escape") {
          r.preventDefault(), r.stopPropagation(), lt();
          return;
        }
        !q || r.key !== "Enter" && r.key !== " " || (r.preventDefault(), r.stopPropagation(), ot(p, (Ge[p] || []).length));
      }
    }, [lt, ot, q, Ge]), Wr = u.useCallback((r) => {
      r.preventDefault();
      const p = r.clientX, c = Re || 288;
      Ke.current = { startX: p, startWidth: c };
      const m = document.body.style.cursor, v = document.body.style.userSelect;
      document.body.style.cursor = "col-resize", document.body.style.userSelect = "none";
      const T = (S) => {
        if (!Ke.current) return;
        const K = S.clientX - Ke.current.startX, oe = Math.max(200, Math.min(600, Ke.current.startWidth + K));
        ce(oe);
      }, X = () => {
        Ke.current = null, document.body.style.cursor = m, document.body.style.userSelect = v, window.removeEventListener("mousemove", T), window.removeEventListener("mouseup", X), window.removeEventListener("blur", X);
      };
      window.addEventListener("mousemove", T), window.addEventListener("mouseup", X), window.addEventListener("blur", X);
    }, [Re, ce]), Lt = u.useCallback(() => pe.current ? pe.current.getBoundingClientRect().width : Re || 288, [Re]), Pt = u.useCallback((r) => {
      if (typeof ce != "function") return;
      const p = Math.max(200, Math.min(600, Math.round(r)));
      ce(
        /** @param {any} [prev] @returns {any} */
        (c) => c === p ? c : p
      );
    }, [ce]), Ot = u.useCallback((r, p) => {
      typeof ye == "function" && ye(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = Xr(c, r, p), v = c && c[r] ? c[r] : null, T = m[r];
          return v && v.width === T.width && v.height === T.height ? c : m;
        }
      );
    }, [ye]), Nt = G.dropZone && G.dropZone.padding || "8px", Ht = u.useMemo(() => {
      const r = { ...G.dropZone || {} };
      return delete r.minHeight, delete r.maxHeight, delete r.overflowY, delete r.padding, delete r.border, delete r.boxShadow, r;
    }, [G.dropZone]), Ut = u.useCallback((r) => {
      if (we && we[r]) return we[r];
      if (Ce && Ce[r]) return Ce[r];
      if (!h || !h.getColumn) return [];
      if (!h.getColumn(r)) return [];
      const c = /* @__PURE__ */ new Set();
      return (h.getCoreRowModel ? h.getCoreRowModel().rows : []).forEach(
        /** @param {any} [row] @returns {any} */
        (v) => {
          const T = v.getValue(r);
          T != null && T !== "" && c.add(T);
        }
      ), Array.from(c).sort((v, T) => String(v).localeCompare(String(T), void 0, { numeric: !0, sensitivity: "base" }));
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
    }), [d.background, d.border, d.headerSubtleBg, d.radiusSm, d.surfaceInset, d.text]), Vt = u.useCallback((r, p) => {
      y(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = [...c], v = m[r];
          if (!v) return c;
          if (qt(p) && !v.weightField) {
            const T = Array.isArray(re) && re.length > 0 ? re.find((K) => K !== v.field) || re[0] : "", X = typeof window < "u" ? window.prompt(
              `Weight field for weighted average of ${ke(v.field)}`,
              v.weightField || T || ""
            ) : v.weightField || T || "", S = typeof X == "string" ? X.trim() : "";
            return S ? Array.isArray(re) && re.length > 0 && !re.includes(S) ? (D && D(`Unknown weight field: ${S}`, "warning"), c) : (m[r] = { ...v, agg: p, weightField: S }, m) : (D && D("Weighted average requires a weight field.", "warning"), c);
          }
          return m[r] = { ...v, agg: p }, m;
        }
      );
    }, [re, y, D]), Ir = u.useCallback((r, p) => {
      if (!p) {
        D && D("Weighted average requires a weight field.", "warning");
        return;
      }
      y(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = [...c], v = m[r];
          return v ? (m[r] = { ...v, weightField: p }, m) : c;
        }
      );
    }, [y, D]), Tr = u.useCallback((r, p) => {
      y(
        /** @param {any} [prev] @returns {any} */
        (c) => {
          const m = [...c], v = m[r];
          return v ? (m[r] = {
            ...v,
            windowFn: p === "none" ? null : p
          }, m) : c;
        }
      );
    }, [y]), Kt = u.useCallback(() => {
      const r = Sn(j), p = j.filter(
        /** @param {any} [config] @returns {any} */
        (T) => T && T.agg !== "formula"
      ), c = `Formula ${$e + 1}`, m = Dt(p), v = bn(c, j, r);
      Te(r), De(r), y(
        /** @param {any} [prev] @returns {any} */
        (T) => [
          ...T,
          {
            field: r,
            agg: "formula",
            label: c,
            formula: m,
            formulaRef: v,
            formulaScope: "measures"
          }
        ]
      ), D && (p.length === 0 ? D("Added a formula column. Add at least one regular value measure to reference in it.", "warning") : D(`Added ${c}.`, "success"));
    }, [$e, De, y, Te, D, j]), Br = (r, p) => {
      r.stopPropagation(), pe.current && (Et.current = pe.current.scrollTop), Ft(
        /** @param {any} [prev] @returns {any} */
        (c) => c.columnId === p ? { columnId: null, anchorEl: null } : { columnId: p, anchorEl: r.currentTarget }
      ), Z && Z(p);
    }, Lr = () => Ft({ columnId: null, anchorEl: null });
    u.useEffect(() => {
      pe.current && et.columnId && (pe.current.scrollTop = Et.current);
    }, [we, et.columnId]), u.useEffect(() => {
      if (xe.length === 0) {
        Ie !== null && Te(null), be !== null && De(null);
        return;
      }
      xe.some(
        /** @param {any} [config] @returns {any} */
        (p) => p.field === Ie
      ) || Te(xe[xe.length - 1].field), be && !xe.some(
        /** @param {any} [config] @returns {any} */
        (p) => p.field === be
      ) && De(null);
    }, [Ie, xe, be]);
    const vt = u.useMemo(
      () => j.findIndex(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg === "formula" && r.field === be
      ),
      [be, j]
    ), St = vt >= 0 ? j[vt] : null, Yt = u.useCallback((r) => {
      Te(r), De(r);
    }, []), Xe = u.useCallback((r) => {
      const p = j[r] && j[r].agg === "formula" ? j[r].field : null;
      if (y(
        /** @param {any} [prev] @returns {any} */
        (c) => c.filter(
          /** @param {any} [_] @param {any} [index] @returns {any} */
          (m, v) => v !== r
        )
      ), p && p === Ie) {
        const c = xe.find(
          /** @param {any} [config] @returns {any} */
          (m) => m.field !== p
        );
        Te(c ? c.field : null);
      }
      p && p === be && De(null);
    }, [Ie, xe, be, y, j]), Oe = wr === "report", Zt = Math.max(Re || 288, Oe ? 380 : 0), Gt = (r, p) => {
      if (r <= 8) return null;
      const c = yt.trim().length > 0;
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
            value: yt,
            onChange: (m) => Mt(m.target.value),
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
            onClick: () => Mt(""),
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
      /* @__PURE__ */ e.jsxs("div", { ref: pe, style: { ...G.sidebar, width: `${Zt}px`, minWidth: `${Zt}px` }, role: "complementary", "aria-label": "Tool Panel", onDragOver: Er, onDragLeave: It, onDrop: It, children: [
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
              onClick: () => ft(r.id),
              onKeyDown: (m) => {
                if (m.key === "Enter" || m.key === " ")
                  m.preventDefault(), ft(r.id);
                else if (m.key === "ArrowRight" || m.key === "ArrowLeft") {
                  m.preventDefault();
                  const v = m.key === "ArrowRight" ? 1 : -1, T = c[(p + v + c.length) % c.length];
                  T && ft(T.id);
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
            reportDef: Cr,
            setReportDef: Rr,
            availableFields: $,
            customDimensions: Ve,
            theme: d,
            styles: G,
            data: fe,
            valConfigs: j,
            colFields: x,
            setColFields: f,
            setValConfigs: y,
            savedReports: Dr,
            setSavedReports: Ar,
            activeReportId: $r,
            setActiveReportId: zr,
            showNotification: D,
            requestedSelection: ut,
            showReportConfigColumn: pt,
            setShowReportConfigColumn: kr
          }
        ) }) : Oe && Ae === "fields" ? /* @__PURE__ */ e.jsxs("div", { style: { padding: "0 4px" }, children: [
          /* @__PURE__ */ e.jsx(
            lr,
            {
              availableFields: $,
              customDimensions: Ve,
              setCustomDimensions: zt,
              data: fe,
              theme: d,
              showNotification: D,
              getFieldLabel: te,
              onDeleteField: Wt
            }
          ),
          /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: d.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px", padding: "0 2px" }, children: [
            /* @__PURE__ */ e.jsx(E.Database, { style: { verticalAlign: "middle", marginRight: "4px" } }),
            "Available Fields — click to add"
          ] }),
          Gt(re.length, mt.length),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", marginBottom: "14px" }, children: [
            mt.map(
              /** @param {any} [f] @returns {any} */
              (r) => {
                const p = j.some(
                  /** @param {any} [c] @returns {any} */
                  (c) => c && c.field === r && c.agg !== "formula"
                );
                return /* @__PURE__ */ e.jsxs(
                  "div",
                  {
                    onClick: () => {
                      p || y(
                        /** @param {any} [prev] @returns {any} */
                        (c) => [...c, { field: r, agg: "sum", label: te(r) }]
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
                    title: p ? `${te(r)} already in values` : `Add ${te(r)} to values`,
                    children: [
                      te(r),
                      p ? " ✓" : ""
                    ]
                  },
                  r
                );
              }
            ),
            re.length > 8 && mt.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${d.border}`, paddingTop: "10px", marginBottom: "8px" }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 2px 8px 2px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsx("button", { type: "button", onClick: Kt, style: { border: `1px solid ${d.primary}66`, background: `${d.primary}12`, color: d.primary, borderRadius: "999px", cursor: "pointer", padding: "4px 10px", fontSize: "11px", fontWeight: 700 }, children: "+ Add Formula" }),
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
              cr,
              {
                measureAxis: F,
                setMeasureAxis: s,
                theme: d,
                valueSelectStyle: me,
                regularValueCount: Pe
              }
            )
          ] }),
          j.map(
            /** @param {any} [item] @param {any} [idx] @returns {any} */
            (r, p) => {
              const c = r && r.agg === "formula";
              if (!r) return null;
              const m = Fr === p;
              return /* @__PURE__ */ e.jsx(
                "div",
                {
                  draggable: !0,
                  onDragStart: () => {
                    tt.current = p;
                  },
                  onDragOver: (v) => {
                    v.preventDefault(), rt(p);
                  },
                  onDragLeave: () => rt(null),
                  onDrop: (v) => {
                    v.preventDefault();
                    const T = tt.current;
                    rt(null), tt.current = null, !(T === null || T === p) && y(
                      /** @param {any} [prev] @returns {any} */
                      (X) => {
                        const S = [...X], [K] = S.splice(T, 1);
                        return S.splice(p, 0, K), S;
                      }
                    );
                  },
                  onDragEnd: () => {
                    tt.current = null, rt(null);
                  },
                  style: { marginBottom: "6px", borderTop: m ? `2px solid ${d.primary}` : "2px solid transparent", cursor: "grab" },
                  children: c ? /* @__PURE__ */ e.jsx(sr, { item: r, idx: p, selected: r.field === be, onSelect: Yt, onRemove: Xe, setValConfigs: y, theme: d, styles: G, dropLine: null, zoneId: "vals", valueSelectStyle: me, colFields: x, fixedSparklineValueKeySet: bt, showNotification: D, serverError: We && We[r.field] || null }) : /* @__PURE__ */ e.jsxs("div", { style: { ...G.chip, flexWrap: "wrap", alignItems: "flex-start" }, children: [
                    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                      /* @__PURE__ */ e.jsx(E.DragIndicator, { style: { flexShrink: 0, opacity: 0.4 } }),
                      /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: te(r.field) })
                    ] }),
                    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                      /* @__PURE__ */ e.jsxs(ne, { size: "sm", "aria-label": "Aggregation", value: r.agg, onClick: (v) => v.stopPropagation(), onChange: (v) => Vt(p, v.target.value), style: { ...me, width: "78px" }, children: [
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
          j.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { opacity: 0.5, fontSize: "11px", padding: "12px", textAlign: "center" }, children: "No value fields. Click a field above or add a formula." })
        ] }) : !Oe && n === "filters" ? /* @__PURE__ */ e.jsxs("div", { style: { flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", padding: "8px" }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "10px", display: "flex", alignItems: "center", background: d.background, borderRadius: "6px", padding: "4px 8px", border: `1px solid ${d.border}` }, children: [
            /* @__PURE__ */ e.jsx(E.Search, {}),
            /* @__PURE__ */ e.jsx(
              "input",
              {
                placeholder: "Search columns...",
                value: R,
                onChange: (r) => O(r.target.value),
                style: { border: "none", background: "transparent", marginLeft: "10px", outline: "none", width: "100%", color: d.text, fontSize: "13px" }
              }
            )
          ] }),
          (() => {
            const c = $.map(
              /** @param {any} [field] @returns {any} */
              (m) => {
                const v = h.getColumn(m);
                return v || { id: m, header: te(m), columnDef: { header: te(m) } };
              }
            ).filter(
              /** @param {any} [col] @returns {any} */
              (m) => {
                const v = m.columnDef && typeof m.columnDef.header == "string" ? m.columnDef.header : typeof m.header == "string" ? m.header : m.id;
                return String(v).toLowerCase().includes(R.toLowerCase()) || m.id.toLowerCase().includes(R.toLowerCase());
              }
            );
            return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: c.map(
              /** @param {any} [col] @returns {any} */
              (m) => /* @__PURE__ */ e.jsx(
                rn,
                {
                  column: m,
                  theme: d,
                  styles: G,
                  onFilter: (
                    /** @param {any} [val] @returns {any} */
                    ((v) => A(m.id, v))
                  ),
                  currentFilter: ue[m.id],
                  options: Ut(m.id),
                  optionMeta: _e && _e[m.id],
                  onSearchOptions: Qe ? (
                    /** @param {any} [search] @returns {any} */
                    ((v) => Z && Z(m.id, { search: v, offset: 0 }))
                  ) : void 0,
                  onLoadMoreOptions: Qe ? (
                    /** @param {any} [search] @param {any} [offset] @returns {any} */
                    ((v, T) => Z && Z(m.id, { search: v, offset: T }))
                  ) : void 0,
                  onOpen: Z
                },
                m.id
              )
            ) });
          })()
        ] }) : (Oe ? Ae !== "columns" : n !== "columns") ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
          n === "fields" && /* @__PURE__ */ e.jsxs("div", { style: { flexShrink: 0 }, children: [
            /* @__PURE__ */ e.jsx(
              lr,
              {
                availableFields: $,
                customDimensions: Ve,
                setCustomDimensions: zt,
                data: fe,
                theme: d,
                showNotification: D,
                getFieldLabel: te,
                onDeleteField: Wt
              }
            ),
            /* @__PURE__ */ e.jsxs("div", { style: G.sectionTitleSm, children: [
              /* @__PURE__ */ e.jsx(E.Database, {}),
              " Available Fields"
            ] }),
            Gt(Le.length, ht.length),
            /* @__PURE__ */ e.jsxs(
              dr,
              {
                panelId: "availableFields",
                panelLabel: "Available Fields",
                size: ge && ge.availableFields,
                onSizeChange: Ot,
                getSidebarWidth: Lt,
                onSidebarWidthChange: Pt,
                theme: d,
                outerStyle: Ht,
                contentStyle: {
                  display: "flex",
                  flexWrap: "wrap",
                  gap: "4px",
                  alignContent: "flex-start",
                  padding: "2px 12px 12px 2px"
                },
                children: [
                  ht.map(
                    /** @param {any} [f] @returns {any} */
                    (r) => {
                      const p = Ye && r === je, c = p ? "Measure Names" : te(r), m = p ? "measureAxis" : "pool";
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
                          onDragStart: (v) => Y(v, r, m),
                          onDragEnd: z,
                          onKeyDown: (v) => Bt(v, r, m, -1, c),
                          title: p ? "Drag to Rows or Columns to show measure names" : `Move ${te(r)}`,
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
                  Le.length > 8 && ht.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
                ]
              }
            )
          ] }),
          Mr.map(
            /** @param {any} [zone] @returns {any} */
            (r) => {
              const p = Ge[r.id] || [];
              return /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "20px", flexShrink: 0 }, children: [
                /* @__PURE__ */ e.jsxs("div", { style: G.sectionTitle, children: [
                  r.icon,
                  r.label
                ] }),
                r.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 4px 8px 4px", flexWrap: "wrap" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: Kt,
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
                    cr,
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
                  dr,
                  {
                    panelId: r.id,
                    panelLabel: r.label,
                    size: ge && ge[r.id],
                    onSizeChange: Ot,
                    getSidebarWidth: Lt,
                    onSidebarWidthChange: Pt,
                    theme: d,
                    outerStyle: Ht,
                    contentStyle: { padding: `${Nt} 12px 12px ${Nt}` },
                    children: /* @__PURE__ */ e.jsxs(
                      "div",
                      {
                        "data-sidebar-drop-zone": r.id,
                        role: "group",
                        tabIndex: 0,
                        "aria-label": `${r.label} drop zone`,
                        "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                        onKeyDown: (c) => _r(c, r.id),
                        onDragOver: (c) => {
                          c.target === c.currentTarget ? L(c, r.id, p.length) : (c.preventDefault(), c.dataTransfer && (c.dataTransfer.dropEffect = "move"));
                        },
                        onDrop: (c) => g(c, r.id),
                        children: [
                          p.map(
                            /** @param {any} [item] @param {any} [idx] @returns {any} */
                            (c, m) => {
                              const v = r.id === "vals" ? c.field : c, T = r.id === "vals" ? c.agg === "formula" ? c.label || "Formula" : te(c.field) : te(c), X = (r.id === "rows" || r.id === "cols") && v === je && _t.placement !== "none";
                              return /* @__PURE__ */ e.jsxs(
                                "div",
                                {
                                  draggable: !0,
                                  "data-sidebar-field-chip": "zone",
                                  role: "button",
                                  tabIndex: 0,
                                  "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                                  "aria-grabbed": q && q.zone === r.id && q.idx === m ? "true" : "false",
                                  "aria-label": `Move ${T}`,
                                  onKeyDown: (S) => Bt(S, c, r.id, m, T),
                                  onDragStart: (S) => Y(S, c, r.id, m),
                                  onDragOver: (S) => L(S, r.id, m),
                                  onDragEnd: z,
                                  children: [
                                    r.id === "vals" && c.agg === "formula" ? /* @__PURE__ */ e.jsx(
                                      sr,
                                      {
                                        item: c,
                                        idx: m,
                                        selected: c.field === Ie,
                                        onSelect: Yt,
                                        onRemove: Xe,
                                        setValConfigs: y,
                                        theme: d,
                                        styles: G,
                                        dropLine: _,
                                        zoneId: r.id,
                                        valueSelectStyle: me,
                                        colFields: x,
                                        fixedSparklineValueKeySet: bt,
                                        showNotification: D,
                                        serverError: We && We[c.field] || null
                                      }
                                    ) : /* @__PURE__ */ e.jsxs("div", { style: {
                                      ...G.chip,
                                      ...r.id === "vals" ? { flexWrap: "wrap", alignItems: "flex-start" } : {}
                                    }, children: [
                                      _ && _.zone === r.id && _.idx === m && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...G.dropLine, top: -2 } }),
                                      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                                        /* @__PURE__ */ e.jsx(E.DragIndicator, {}),
                                        /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, display: "block", minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: T })
                                      ] }),
                                      r.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                                        /* @__PURE__ */ e.jsxs(ne, { size: "sm", value: c.agg, onClick: (S) => S.stopPropagation(), onChange: (S) => Vt(m, S.target.value), style: { ...me, width: "78px" }, title: "Aggregation", children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Avg" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "count", children: "Cnt" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "min", children: "Min" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "max", children: "Max" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "weighted_avg", children: "WAvg" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "array_agg", children: "Series" })
                                        ] }),
                                        qt(c.agg) && /* @__PURE__ */ e.jsxs(ne, { size: "sm", value: c.weightField || "", onClick: (S) => S.stopPropagation(), onChange: (S) => Ir(m, S.target.value), style: { ...me, width: "96px" }, title: "Weight field", children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "", disabled: !0, children: "Weight" }),
                                          re.map(
                                            /** @param {any} [fieldName] @returns {any} */
                                            (S) => /* @__PURE__ */ e.jsx("option", { value: S, children: te(S) }, S)
                                          )
                                        ] }),
                                        /* @__PURE__ */ e.jsxs(ne, { size: "sm", "aria-label": "Window calculation", value: c.windowFn || "none", onClick: (S) => S.stopPropagation(), onChange: (S) => Tr(m, S.target.value), style: { ...me, width: "68px" }, children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "none", children: "Norm" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_row", children: "%Row" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_col", children: "%Col" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_grand_total", children: "%Tot" })
                                        ] })
                                      ] }),
                                      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", marginLeft: "auto", alignItems: "center" }, children: [
                                        r.id === "filter" && /* @__PURE__ */ e.jsx("div", { onClick: (S) => Br(S, v), style: { cursor: "pointer", display: "flex", alignItems: "center", padding: "2px", borderRadius: "4px" }, children: /* @__PURE__ */ e.jsx(E.Filter, {}) }),
                                        /* @__PURE__ */ e.jsx("span", { onClick: () => {
                                          if (X && typeof s == "function") {
                                            s(
                                              /** @param {any} [previous] @returns {any} */
                                              (S) => ({
                                                ...Je(S),
                                                placement: "none"
                                              })
                                            ), r.id === "rows" && l(
                                              /** @param {any} [p] @returns {any} */
                                              (S) => Array.isArray(S) ? S.filter(
                                                /** @param {any} [x] @returns {any} */
                                                (K) => K !== v
                                              ) : S
                                            ), r.id === "cols" && f(
                                              /** @param {any} [p] @returns {any} */
                                              (S) => Array.isArray(S) ? S.filter(
                                                /** @param {any} [x] @returns {any} */
                                                (K) => K !== v
                                              ) : S
                                            );
                                            return;
                                          }
                                          if (r.id === "filter") {
                                            const S = { ...ue };
                                            delete S[v], xt(S);
                                          }
                                          r.id === "rows" && l(
                                            /** @param {any} [p] @returns {any} */
                                            (S) => S.filter(
                                              /** @param {any} [x] @returns {any} */
                                              (K) => K !== v
                                            )
                                          ), r.id === "cols" && f(
                                            /** @param {any} [p] @returns {any} */
                                            (S) => S.filter(
                                              /** @param {any} [x] @returns {any} */
                                              (K) => K !== v
                                            )
                                          ), r.id === "vals" && Xe(m);
                                        }, className: "sp-chip-remove", style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(E.Close, {}) })
                                      ] }),
                                      r.id === "filter" && et.columnId === v && /* @__PURE__ */ e.jsx(
                                        qr,
                                        {
                                          column: { header: T, id: v },
                                          anchorEl: et.anchorEl,
                                          onClose: Lr,
                                          onFilter: (
                                            /** @param {any} [filterValue] @returns {any} */
                                            ((S) => A(v, S))
                                          ),
                                          currentFilter: ue[v],
                                          options: Ut(v),
                                          optionMeta: _e && _e[v],
                                          onSearchOptions: Qe ? (
                                            /** @param {any} [search] @returns {any} */
                                            ((S) => Z && Z(v, { search: S, offset: 0 }))
                                          ) : void 0,
                                          onLoadMoreOptions: Qe ? (
                                            /** @param {any} [search] @param {any} [offset] @returns {any} */
                                            ((S, K) => Z && Z(v, { search: S, offset: K }))
                                          ) : void 0,
                                          theme: d
                                        }
                                      ),
                                      r.id === "vals" && /* @__PURE__ */ e.jsx(
                                        jr,
                                        {
                                          item: c,
                                          idx: m,
                                          setValConfigs: y,
                                          theme: d,
                                          valueSelectStyle: me,
                                          colFields: x,
                                          fixedSparklineValueKeySet: bt,
                                          showNotification: D
                                        }
                                      ),
                                      _ && _.zone === r.id && _.idx === m + 1 && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...G.dropLine, bottom: -2 } })
                                    ] }),
                                    r.id === "filter" && ue[v] && ue[v].conditions && /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: d.primary, padding: "0 8px 4px 8px", marginTop: "-4px" }, children: ue[v].conditions.map(
                                      /** @param {any} [c] @returns {any} */
                                      (S) => `${S.type}: ${S.value}${S.caseSensitive ? " (Match Case)" : ""}`
                                    ).join(` ${ue[v].operator} `) })
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
                  value: R,
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
              R && /* @__PURE__ */ e.jsx(
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
                  onClick: () => k(r.value),
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
          /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "4px", padding: "0 8px" }, children: [
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                className: "sp-btn sp-btn-subtle",
                onClick: () => h.toggleAllColumnsVisible(!0),
                title: "Show All Columns",
                style: {
                  ...G.btn,
                  padding: "6px 4px",
                  fontSize: "11px",
                  gap: "4px",
                  minWidth: 0,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                },
                children: [
                  /* @__PURE__ */ e.jsx(E.Visibility, { style: { fontSize: "13px", flexShrink: 0 } }),
                  /* @__PURE__ */ e.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: "Show" })
                ]
              }
            ),
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                className: "sp-btn sp-btn-subtle",
                onClick: () => h.toggleAllColumnsVisible(!1),
                title: "Hide All Columns",
                style: {
                  ...G.btn,
                  padding: "6px 4px",
                  fontSize: "11px",
                  gap: "4px",
                  minWidth: 0,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                },
                children: [
                  /* @__PURE__ */ e.jsx(E.VisibilityOff, { style: { fontSize: "13px", flexShrink: 0 } }),
                  /* @__PURE__ */ e.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: "Hide" })
                ]
              }
            ),
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                className: "sp-btn sp-btn-subtle",
                onClick: () => ee(!1),
                title: "Unpin All Columns",
                style: {
                  ...G.btn,
                  padding: "6px 4px",
                  fontSize: "11px",
                  gap: "4px",
                  minWidth: 0,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                },
                children: [
                  /* @__PURE__ */ e.jsx(E.Unpin, { style: { fontSize: "13px", flexShrink: 0 } }),
                  /* @__PURE__ */ e.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: "Unpin" })
                ]
              }
            )
          ] }),
          w.size > 0 && /* @__PURE__ */ e.jsxs("div", { style: {
            display: "flex",
            flexDirection: "column",
            gap: "8px",
            background: d.select || "var(--sp-select)",
            padding: "10px",
            borderRadius: "var(--sp-radius-md, 8px)",
            border: "1px solid var(--sp-border-strong, var(--sp-primary))",
            boxShadow: "var(--sp-shadow-sm, 0 2px 8px rgba(0,0,0,0.1))",
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
                w.size,
                " Column",
                w.size > 1 ? "s" : "",
                " Selected"
              ] }),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-ghost",
                  onClick: () => B(/* @__PURE__ */ new Set()),
                  style: {
                    border: "none",
                    background: d.background,
                    cursor: "pointer",
                    display: "flex",
                    color: d.textSec,
                    padding: "4px",
                    borderRadius: "4px"
                  },
                  children: /* @__PURE__ */ e.jsx(E.Close, {})
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-subtle",
                  onClick: () => {
                    Array.from(w).forEach((r) => U(r, "left")), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...G.btn,
                    padding: "6px 8px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                  },
                  children: [
                    /* @__PURE__ */ e.jsx(E.PinLeft, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Pin Left" })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-subtle",
                  onClick: () => {
                    Array.from(w).forEach((r) => U(r, !1)), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...G.btn,
                    padding: "6px 8px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                  },
                  children: [
                    /* @__PURE__ */ e.jsx(E.Unpin, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Unpin" })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-subtle",
                  onClick: () => {
                    Array.from(w).forEach((r) => U(r, "right")), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...G.btn,
                    padding: "6px 8px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                  },
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
              (S) => !S.parent && S.id !== "no_data"
            ).filter(
              /** @param {any} [col] @returns {any} */
              (S) => {
                if (a === "all") return !0;
                let K = "string";
                if (j.find(
                  /** @param {any} [v] @returns {any} */
                  (ie) => S.id.includes(ie.field)
                ))
                  K = "number";
                else if (fe && fe.length > 0) {
                  const ie = fe[0];
                  let ae = ie[S.id];
                  if (ae === void 0 && S.accessorFn)
                    try {
                      ae = S.accessorFn(ie);
                    } catch {
                    }
                  typeof ae == "number" ? K = "number" : ae instanceof Date ? K = "date" : typeof ae == "string" && (!isNaN(Number(ae)) && ae.trim() !== "" ? K = "number" : !isNaN(Date.parse(ae)) && ae.includes("-") && (K = "date"));
                }
                return K === a;
              }
            ), c = p.filter(
              /** @param {any} [c] @returns {any} */
              (S) => it(S, "left")
            ), m = p.filter(
              /** @param {any} [c] @returns {any} */
              (S) => it(S, "right")
            ), v = p.filter(
              /** @param {any} [c] @returns {any} */
              (S) => it(S, "unpinned")
            ), T = (S, K) => S.map(
              /** @param {any} [column] @returns {any} */
              (oe) => /* @__PURE__ */ e.jsx(
                pr,
                {
                  column: oe,
                  level: 0,
                  theme: d,
                  styles: G,
                  handlePinColumn: U,
                  colSearch: R,
                  selectedCols: w,
                  setSelectedCols: B,
                  onDrop: X,
                  sectionId: K
                },
                oe.id
              )
            ), X = (S, K, oe) => {
              let ie;
              const ae = P || { left: [], right: [] };
              if (oe) {
                const qe = K === "left" ? ae.left : K === "right" ? ae.right : null;
                if (qe) {
                  if (ie = qe.indexOf(oe), ie === -1) {
                    const jt = h.getColumn(oe);
                    if (jt && en(jt)) {
                      const Xt = wt(jt).find(
                        /** @param {any} [id] @returns {any} */
                        (Pr) => qe.includes(Pr)
                      );
                      Xt && (ie = qe.indexOf(Xt));
                    }
                  }
                  ie === -1 && (ie = void 0);
                }
              }
              K === "left" ? U(S, "left", ie) : K === "right" ? U(S, "right", ie) : U(S, !1);
            };
            return /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              c.length > 0 && /* @__PURE__ */ e.jsx(
                kt,
                {
                  title: "Pinned Left",
                  count: c.length,
                  theme: d,
                  styles: G,
                  sectionId: "left",
                  onDrop: X,
                  children: T(c, "left")
                }
              ),
              m.length > 0 && /* @__PURE__ */ e.jsx(
                kt,
                {
                  title: "Pinned Right",
                  count: m.length,
                  theme: d,
                  styles: G,
                  sectionId: "right",
                  onDrop: X,
                  children: T(m, "right")
                }
              ),
              /* @__PURE__ */ e.jsx(
                kt,
                {
                  title: "Columns",
                  count: v.length,
                  theme: d,
                  styles: G,
                  sectionId: "unpinned",
                  onDrop: X,
                  children: T(v, "unpinned")
                }
              )
            ] });
          })() }),
          C && C.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { padding: "8px", borderTop: `1px solid ${d.border}` }, children: [
            /* @__PURE__ */ e.jsx("div", { style: G.sectionTitle, children: "Pinning Presets" }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px", flexWrap: "wrap" }, children: C.map(
              /** @param {any} [preset] @param {any} [i] @returns {any} */
              (r, p) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: () => {
                    M(r.config), D(`Applied preset: ${r.name}`);
                  },
                  style: { ...G.btn, fontSize: "11px", background: d.headerBg },
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
      St && /* @__PURE__ */ e.jsx(
        An,
        {
          item: St,
          idx: vt,
          valConfigs: j,
          setValConfigs: y,
          theme: d,
          onClose: () => De(null),
          onRemove: Xe,
          displayedColumnOptions: Se,
          availableRawFields: re,
          serverError: We && We[St.field] || null
        }
      ),
      /* @__PURE__ */ e.jsx(
        "div",
        {
          className: "sp-resize-handle",
          onMouseDown: Wr,
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
