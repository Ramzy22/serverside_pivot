import { j as e, z as E, _ as ge, $ as _t, a0 as Tt, a1 as yr, a2 as qe, a3 as Wt, a4 as Bt, a5 as It, a6 as ze, a7 as er, a8 as rr, G as Lt, a9 as Pt, aa as Ot, ab as Ht, ac as Pe, ad as et, ae as Ut, af as Or, ag as Vt, ah as Yt, ai as Zt, aj as Kt, ak as Gt, al as Nt } from "./index-CVsYjN62.js";
import { a as ae, R as f } from "./charts-DthW1Q8W.js";
const Xt = ({ column: r, theme: n, styles: s, onFilter: o, currentFilter: l, options: g, optionMeta: c, onSearchOptions: v, onLoadMoreOptions: u, onOpen: F }) => {
  const [i, y] = ae.useState(!1), T = l && (l.conditions || l.value), L = () => y(!1), _ = () => {
    const A = !i;
    y(A), A && typeof F == "function" && F(r.id);
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column" }, children: [
    /* @__PURE__ */ e.jsxs(
      "div",
      {
        style: {
          ...s.columnItem,
          cursor: "pointer",
          background: i ? n.select : "transparent",
          borderLeft: T ? `3px solid ${n.primary}` : "3px solid transparent"
        },
        onClick: _,
        children: [
          /* @__PURE__ */ e.jsx("span", { style: { marginRight: "8px", opacity: 0.7, display: "flex" }, children: i ? /* @__PURE__ */ e.jsx(E.ChevronDown, {}) : /* @__PURE__ */ e.jsx(E.ChevronRight, {}) }),
          /* @__PURE__ */ e.jsxs("span", { style: { flex: 1, fontWeight: 500, display: "flex", alignItems: "center", gap: "6px" }, children: [
            T && /* @__PURE__ */ e.jsx(E.Filter, { style: { fontSize: "12px", color: n.primary } }),
            ge(typeof r.header == "string" ? r.header : r.columnDef && typeof r.columnDef.header == "string" ? r.columnDef.header : r.id)
          ] })
        ]
      }
    ),
    i && /* @__PURE__ */ e.jsx("div", { style: { padding: "8px", borderBottom: `1px solid ${n.border}44` }, children: /* @__PURE__ */ e.jsx(
      _t,
      {
        column: r,
        onFilter: o,
        currentFilter: l,
        options: g,
        optionMeta: c,
        onSearchOptions: v,
        onLoadMoreOptions: u,
        theme: n,
        onClose: L
      }
    ) })
  ] });
}, gr = ({ title: r, children: n, items: s, renderItem: o, theme: l, styles: g, initialExpanded: c = !0, count: v, onDrop: u, sectionId: F }) => {
  const [i, y] = ae.useState(c), [T, L] = ae.useState(c ? 200 : 0), [_, A] = ae.useState(!1), h = ae.useRef(null);
  ae.useRef(null), ae.useEffect(() => {
    if (i && h.current) {
      const C = h.current.scrollHeight;
      L(Math.min(C, 300));
    } else
      L(0);
  }, [i, s, v]);
  const k = Tt({
    count: s ? s.length : 0,
    getScrollElement: () => h.current,
    estimateSize: () => 32,
    overscan: 10,
    enabled: !!s && i
  }), R = (C) => {
    C.preventDefault(), A(!0);
  }, W = () => {
    A(!1);
  }, O = (C) => {
    C.preventDefault(), A(!1);
    const P = C.dataTransfer.getData("text/plain");
    u && P && u(P, F);
  };
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      style: {
        ...g.toolPanelSection,
        flex: i ? "0 1 auto" : "0 0 auto",
        minHeight: i ? "40px" : "32px",
        overflow: "hidden"
        // Prevent overlap
      },
      onDragOver: R,
      onDragLeave: W,
      onDrop: O,
      children: [
        /* @__PURE__ */ e.jsxs("div", { style: g.toolPanelSectionHeader, onClick: () => y(!i), children: [
          /* @__PURE__ */ e.jsx("span", { style: { marginRight: "8px", opacity: 0.7, display: "flex" }, children: i ? /* @__PURE__ */ e.jsx(E.ChevronDown, {}) : /* @__PURE__ */ e.jsx(E.ChevronRight, {}) }),
          /* @__PURE__ */ e.jsx("span", { style: { flex: 1 }, children: r }),
          v !== void 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", opacity: 0.5, background: l.hover, padding: "2px 6px", borderRadius: "10px" }, children: v })
        ] }),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            ref: h,
            style: {
              ...g.toolPanelList,
              height: `${T}px`,
              overflowY: i ? "auto" : "hidden",
              transition: "height 0.3s ease",
              opacity: i ? 1 : 0
            },
            children: s ? /* @__PURE__ */ e.jsx("div", { style: {
              height: `${k.getTotalSize()}px`,
              width: "100%",
              position: "relative"
            }, children: k.getVirtualItems().map((C) => /* @__PURE__ */ e.jsx(
              "div",
              {
                style: {
                  position: "absolute",
                  top: 0,
                  left: 0,
                  width: "100%",
                  height: `${C.size}px`,
                  transform: `translateY(${C.start}px)`
                },
                children: o(s[C.index], C.index)
              },
              C.index
            )) }) : n
          }
        )
      ]
    }
  );
}, rt = ({ column: r, level: n, theme: s, styles: o, handlePinColumn: l, colSearch: g, selectedCols: c, setSelectedCols: v, onDrop: u, sectionId: F }) => {
  const [i, y] = ae.useState(n < 1), [T, L] = ae.useState(!1), [_, A] = ae.useState(!1), h = ae.useRef(null), k = r.columns && r.columns.length > 0, R = r.columnDef.header;
  let W = typeof R == "string" ? R : r.id;
  typeof W == "string" && W.startsWith("group_") && (W = r.id.replace("group_", "").split("|||").pop() || W), r.headerVal && (W = r.headerVal), W = ge(W);
  const O = r.getIsPinned(), C = r.getIsVisible(), P = k ? yr(r) : [r.id], a = P.length > 0 && P.every((S) => c.has(S)), $ = !a && P.some((S) => c.has(S));
  if (ae.useEffect(() => {
    h.current && (h.current.indeterminate = $);
  }, [$]), g && !W.toLowerCase().includes(g.toLowerCase()))
    if (k) {
      const S = (U) => {
        const ee = U.columnDef.header;
        return (typeof ee == "string" ? ee : U.id).toLowerCase().includes(g.toLowerCase()) ? !0 : U.columns ? U.columns.some(S) : !1;
      };
      if (!S(r)) return null;
    } else
      return null;
  const D = (S) => {
    S.stopPropagation();
    const U = new Set(c), ee = k ? yr(r) : [r.id];
    ee.every((re) => U.has(re)) ? ee.forEach((re) => U.delete(re)) : ee.forEach((re) => U.add(re)), v(U);
  }, B = (S) => {
    if (S.stopPropagation(), k) {
      const U = Wt(r), ee = U.some((Y) => !Y.getIsVisible());
      U.forEach((Y) => Y.toggleVisibility(ee));
    } else
      r.toggleVisibility();
  }, M = (S, U) => {
    S.stopPropagation(), l(r.id, U);
  }, H = (S) => {
    A(!0), S.dataTransfer.setData("text/plain", r.id), S.dataTransfer.effectAllowed = "move";
  }, G = () => {
    A(!1);
  }, b = (S) => {
    S.preventDefault(), S.stopPropagation();
    const U = S.dataTransfer.getData("text/plain");
    U && u && U !== r.id && u(U, F, r.id);
  }, z = (S) => {
    S.key === "Enter" ? B(S) : S.key === " " ? (S.preventDefault(), D(S)) : S.altKey && S.key === "ArrowLeft" ? (S.preventDefault(), l(r.id, "left")) : S.altKey && S.key === "ArrowRight" ? (S.preventDefault(), l(r.id, "right")) : S.altKey && S.key === "ArrowDown" && (S.preventDefault(), l(r.id, !1));
  };
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      style: { display: "flex", flexDirection: "column", opacity: _ ? 0.5 : 1 },
      draggable: !k,
      onDragStart: H,
      onDragEnd: G,
      onDragOver: (S) => S.preventDefault(),
      onDrop: b,
      role: "treeitem",
      "aria-selected": a || $,
      "aria-expanded": i,
      children: [
        /* @__PURE__ */ e.jsxs(
          "div",
          {
            style: {
              ...o.columnItem,
              paddingLeft: `${n * 12 + 8}px`,
              // Reduced indentation step
              background: a || $ ? s.select : T ? s.hover : "transparent",
              borderLeft: O ? `3px solid ${s.primary}` : "3px solid transparent"
            },
            onMouseEnter: (S) => !(a || $) && L(!0),
            onMouseLeave: (S) => !(a || $) && L(!1),
            tabIndex: 0,
            onKeyDown: z,
            children: [
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  ref: h,
                  type: "checkbox",
                  checked: a,
                  onChange: D,
                  onClick: (S) => S.stopPropagation(),
                  style: { margin: 0, cursor: "pointer", pointerEvents: "auto" },
                  tabIndex: -1
                }
              ),
              !k && /* @__PURE__ */ e.jsx("span", { style: { cursor: "grab", display: "flex", opacity: 0.7, marginRight: "4px" }, children: /* @__PURE__ */ e.jsx(E.DragIndicator, {}) }),
              k ? /* @__PURE__ */ e.jsx(
                "span",
                {
                  role: "button",
                  tabIndex: 0,
                  "aria-expanded": i,
                  "aria-label": i ? `Collapse ${W}` : `Expand ${W}`,
                  onClick: () => y(!i),
                  onKeyDown: (S) => {
                    (S.key === "Enter" || S.key === " ") && (S.preventDefault(), y(!i));
                  },
                  style: { cursor: "pointer", display: "flex", opacity: 0.7, marginRight: "4px" },
                  children: i ? /* @__PURE__ */ e.jsx(E.ChevronDown, {}) : /* @__PURE__ */ e.jsx(E.ChevronRight, {})
                }
              ) : /* @__PURE__ */ e.jsx("span", { style: { width: "20px" } }),
              /* @__PURE__ */ e.jsx(
                "span",
                {
                  onClick: B,
                  style: {
                    cursor: "pointer",
                    display: "flex",
                    color: C ? s.primary : s.textSec,
                    opacity: C ? 1 : 0.5,
                    marginRight: "4px"
                  },
                  title: C ? "Hide Column" : "Show Column",
                  "aria-label": C ? "Hide Column" : "Show Column",
                  children: C ? /* @__PURE__ */ e.jsx(E.Visibility, {}) : /* @__PURE__ */ e.jsx(E.VisibilityOff, {})
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
                    fontWeight: k ? 600 : 400,
                    cursor: k ? "pointer" : "default",
                    color: C ? s.text : s.textSec,
                    opacity: C ? 1 : 0.6,
                    display: "flex",
                    alignItems: "center"
                  },
                  onClick: () => k && y(!i),
                  title: W,
                  children: [
                    k && /* @__PURE__ */ e.jsx(E.Group, { style: { marginRight: "6px", fontSize: "14px", opacity: 0.8 } }),
                    W
                  ]
                }
              ),
              !k && /* @__PURE__ */ e.jsxs("div", { className: "pin-controls", children: [
                /* @__PURE__ */ e.jsx("button", { onClick: (S) => M(S, "left"), title: "Pin Column Left", "aria-label": "Pin Column Left", children: /* @__PURE__ */ e.jsx(E.PinLeft, {}) }),
                /* @__PURE__ */ e.jsx("button", { onClick: (S) => M(S, !1), title: "Unpin Column", "aria-label": "Unpin Column", children: /* @__PURE__ */ e.jsx(E.Unpin, {}) }),
                /* @__PURE__ */ e.jsx("button", { onClick: (S) => M(S, "right"), title: "Pin Column Right", "aria-label": "Pin Column Right", children: /* @__PURE__ */ e.jsx(E.PinRight, {}) })
              ] })
            ]
          }
        ),
        k && i && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: r.columns.filter((S) => qe(S, F)).map((S) => /* @__PURE__ */ e.jsx(
          rt,
          {
            column: S,
            level: n + 1,
            theme: s,
            styles: o,
            handlePinColumn: l,
            colSearch: g,
            selectedCols: c,
            setSelectedCols: v,
            onDrop: u,
            sectionId: F
          },
          S.id
        )) })
      ]
    }
  );
}, tt = /\b[A-Za-z_][A-Za-z0-9_]*\b/g, nt = /^[A-Za-z_][A-Za-z0-9_]*$/, tr = /\[([^\]]+)\]/g, Jt = /* @__PURE__ */ new Set(["abs", "round", "ceil", "floor", "max", "min", "int", "float"]), qt = /* @__PURE__ */ new Set(["True", "False", "None", "and", "or", "not", "if", "else", "in", "is"]), ve = {
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
function lt(r) {
  return String(r || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function Se(r, n = "formula") {
  const o = String(r || "").trim().toLowerCase().replace(/\s+/g, "").replace(/[^a-z0-9_]/g, "") || String(n || "formula").toLowerCase().replace(/[^a-z0-9_]/g, "") || "formula";
  return /^[a-z_]/.test(o) ? o : `f_${o}`;
}
function X(r) {
  return !r || typeof r != "object" ? "" : String(
    r.formulaRef || Se(r.label || "", r.field || "formula") || r.field || ""
  ).trim();
}
function Ee(r) {
  const n = String(r || "").trim().toLowerCase();
  return ["columns", "display", "displayed", "displayed_columns", "rendered", "rendered_columns"].includes(n) ? "columns" : "measures";
}
function he(r) {
  return Ee(r) === "columns";
}
const Qt = [
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
], Hr = /* @__PURE__ */ new Set(["in", "not_in"]), en = /* @__PURE__ */ new Set(["is_null", "is_not_null"]);
function ot(r) {
  return { field: r && r[0] || "", operator: "eq", value: "", values: [], caseSensitive: !1 };
}
function at(r, n = 0) {
  return {
    id: `rule-${Date.now().toString(36)}-${n}`,
    label: n === 0 ? "Group A" : `Group ${n + 1}`,
    condition: { op: "AND", clauses: [ot(r)] }
  };
}
function Ur(r, n = null) {
  if (n) {
    const o = ze([n])[0] || n;
    return { ...JSON.parse(JSON.stringify(o)), __editingField: o.field, __editingId: o.id };
  }
  const s = er(`category_${Date.now().toString(36)}`);
  return {
    id: s,
    field: rr(s),
    name: "New Category",
    fallbackLabel: "Other",
    order: "creation",
    rules: [at(r, 0)],
    __isNew: !0
  };
}
function rn(r, n) {
  if (!n) return "string";
  const s = Array.isArray(r) ? r.slice(0, 250) : [];
  for (let o = 0; o < s.length; o += 1) {
    const l = s[o];
    if (!l || typeof l != "object") continue;
    const g = l[n];
    if (!(g == null || g === "")) {
      if (typeof g == "number" && Number.isFinite(g)) return "number";
      if (g instanceof Date && !Number.isNaN(g.getTime())) return "date";
      if (typeof g == "string") {
        const c = g.trim();
        if (!c) continue;
        if (Number.isFinite(Number(c))) return "number";
        const v = Date.parse(c);
        return !Number.isNaN(v) && /[-/T:]/.test(c) ? "date" : "string";
      }
    }
  }
  return "string";
}
const Qe = (r, n, s) => (Array.isArray(n) ? n : []).filter((o) => rn(r, o) === s);
function tn(r, n) {
  const o = Qe(n, r, "date")[0] || r[0] || "", l = er(`${o || "date"}_month`);
  return {
    id: l,
    field: rr(l),
    kind: "date_group",
    name: o ? `${ge(o)} Month` : "Date Month",
    sourceField: o,
    granularity: "month",
    fiscalYearStartMonth: 1,
    fallbackLabel: "No date",
    __isNew: !0
  };
}
function nn(r, n) {
  const o = Qe(n, r, "number")[0] || r[0] || "", l = er(`${o || "number"}_bins`);
  return {
    id: l,
    field: rr(l),
    kind: "number_group",
    name: o ? `${ge(o)} Bins` : "Number Bins",
    sourceField: o,
    binSize: 10,
    minValue: "",
    maxValue: "",
    precision: 0,
    fallbackLabel: "No value",
    __isNew: !0
  };
}
const Vr = (r) => String(r || "").split(",").map((n) => n.trim()).filter(Boolean), Yr = (r) => String(r || "").trim().replace(/\s+/g, " ").toLowerCase(), it = (r) => r && (r.__editingField || r.field) ? r.__editingField || r.field : "";
function ln(r, n) {
  const s = Array.isArray(r) ? r : [], o = it(n);
  if (!o) return s;
  const l = s.findIndex((g) => g && g.field === o);
  return l >= 0 ? s.slice(0, l) : s;
}
function Zr(r) {
  const n = [];
  return (r && Array.isArray(r.rules) ? r.rules : []).forEach((s) => {
    (s && s.condition && Array.isArray(s.condition.clauses) ? s.condition.clauses : []).forEach((l) => {
      l && l.field && n.push(l.field);
    });
  }), Array.from(new Set(n));
}
function on(r, n) {
  const s = (Array.isArray(r) ? r : []).filter((o) => o && typeof o == "object").filter((o) => !o._isTotal && o._path !== "__grand_total__" && o._path !== "__color_scale_stats__");
  return Bt(s, n || []);
}
function an(r, n) {
  return !Array.isArray(r) || r.length === 0 || !Array.isArray(n) || n.length === 0 ? !1 : n.every((s) => r.some((o) => o && Object.prototype.hasOwnProperty.call(o, s)));
}
const sn = (r, n) => (n && Array.isArray(n.rules) ? n.rules : []).map((s) => ({
  label: s && s.label ? s.label : "Rule",
  count: r.reduce((o, l) => It(l, s.condition) ? o + 1 : o, 0)
})), dn = f.lazy(() => import(
  /* webpackChunkName: "pivot-report-editor" */
  "./ReportEditor-CKghiKsh.js"
).then((r) => ({
  default: r.ReportEditor
}))), cn = (r) => /* @__PURE__ */ e.jsx(f.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { role: "status", style: { padding: "16px" }, children: "Loading report builder…" }), children: /* @__PURE__ */ e.jsx(dn, { ...r }) });
function Kr({
  availableFields: r,
  customDimensions: n,
  setCustomDimensions: s,
  data: o,
  theme: l,
  showNotification: g,
  getFieldLabel: c,
  onDeleteField: v
}) {
  const u = f.useMemo(
    () => (r || []).filter((a) => a && !et(a)),
    [r]
  ), F = f.useMemo(
    () => ze(n || []),
    [n]
  ), [i, y] = f.useState(null), T = f.useMemo(
    () => ln(F, i),
    [i, F]
  ), L = f.useMemo(() => {
    const a = /* @__PURE__ */ new Set(), $ = [];
    return u.forEach((D) => {
      D && !a.has(D) && (a.add(D), $.push(D));
    }), T.forEach((D) => {
      D && D.field && !a.has(D.field) && (a.add(D.field), $.push(D.field));
    }), $;
  }, [T, u]), _ = f.useMemo(
    () => {
      const a = Qe(o, u, "date");
      return a.length > 0 ? a : u;
    },
    [u, o]
  ), A = f.useMemo(
    () => {
      const a = Qe(o, u, "number");
      return a.length > 0 ? a : u;
    },
    [u, o]
  ), h = f.useMemo(
    () => on(o, T),
    [T, o]
  ), k = f.useMemo(() => ({
    border: `1px solid ${l.border}`,
    background: l.background,
    color: l.text,
    borderRadius: "8px",
    fontSize: "12px",
    padding: "7px 8px",
    outline: "none",
    minWidth: 0
  }), [l.background, l.border, l.text]), R = f.useCallback((a = "default") => ({
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
  f.useEffect(() => {
    if (!i) return;
    const a = ($) => {
      $.key === "Escape" && ($.preventDefault(), y(null));
    };
    return window.addEventListener("keydown", a), () => window.removeEventListener("keydown", a);
  }, [i]);
  const W = f.useCallback((a, $) => {
    y((D) => {
      if (!D) return D;
      const B = [...D.rules || []];
      return B[a] = typeof $ == "function" ? $(B[a]) : $, { ...D, rules: B };
    });
  }, []), O = f.useCallback((a, $, D) => {
    W(a, (B) => {
      const M = B.condition || { op: "AND", clauses: [] }, H = [...M.clauses || []];
      return H[$] = { ...H[$], ...D }, { ...B, condition: { ...M, clauses: H } };
    });
  }, [W]), C = f.useCallback(() => {
    const a = ze([i])[0];
    if (!a || !a.name) {
      g && g("A grouped field needs a name.", "warning");
      return;
    }
    if (a.kind === "category" && (!a.rules || a.rules.length === 0)) {
      g && g("A category needs at least one valid rule.", "warning");
      return;
    }
    if ((a.kind === "date_group" || a.kind === "number_group") && !a.sourceField) {
      g && g("Choose the source field to group.", "warning");
      return;
    }
    const $ = it(i), D = Yr(a.name);
    if (F.some((M) => M && M.field !== $ && Yr(M.name) === D)) {
      g && g(`A category named "${a.name}" already exists.`, "warning");
      return;
    }
    if (a.kind === "category") {
      const M = new Set(L);
      if (Zr(a).find((b) => !M.has(b))) {
        g && g("A category can only use base fields or categories defined before it.", "warning");
        return;
      }
      const G = Zr(a);
      if (an(h, G)) {
        const b = sn(h, a).find((z) => z.count === 0);
        if (b) {
          g && g(`"${b.label}" does not match any current row. Adjust the condition before applying.`, "warning");
          return;
        }
      }
    }
    if (a.kind === "date_group" && !_.includes(a.sourceField)) {
      g && g("Choose a date field for date grouping.", "warning");
      return;
    }
    if (a.kind === "number_group") {
      if (!A.includes(a.sourceField)) {
        g && g("Choose a numeric field for number grouping.", "warning");
        return;
      }
      if (!Number.isFinite(Number(a.binSize)) || Number(a.binSize) <= 0) {
        g && g("Number bins need a positive bin size.", "warning");
        return;
      }
    }
    s((M) => [...ze(M || []).filter((b) => b.id !== a.id && b.field !== a.field && b.field !== $), a]), y(null), g && g(`Applied ${a.name}.`, "success");
  }, [L, _, i, F, A, s, g, h]), P = f.useCallback((a) => {
    const $ = ze([a])[0];
    $ && (s((D) => ze(D || []).filter((B) => B.field !== $.field)), typeof v == "function" && v($.field), i && i.field === $.field && y(null), g && g(`Removed ${$.name}.`, "success"));
  }, [i, v, s, g]);
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
          onClick: () => y(Ur(L)),
          style: R("primary"),
          children: "+ Category"
        }
      )
    ] }),
    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
      /* @__PURE__ */ e.jsxs(
        "button",
        {
          type: "button",
          onClick: () => y(tn(u, o)),
          disabled: _.length === 0,
          style: { ...R(), opacity: _.length === 0 ? 0.5 : 1, cursor: _.length === 0 ? "not-allowed" : "pointer" },
          title: _.length === 0 ? "No date fields detected" : "Create date buckets",
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
          onClick: () => y(nn(u, o)),
          disabled: A.length === 0,
          style: { ...R(), opacity: A.length === 0 ? 0.5 : 1, cursor: A.length === 0 ? "not-allowed" : "pointer" },
          title: A.length === 0 ? "No numeric fields detected" : "Create numeric bins",
          children: [
            /* @__PURE__ */ e.jsx(E.NumberFormat, {}),
            " Number Bins"
          ]
        }
      )
    ] }),
    F.length > 0 && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: F.map((a) => {
      const $ = a.kind || "category", D = $ === "date_group" ? `${c(a.sourceField)} by ${a.granularity}` : $ === "number_group" ? `${c(a.sourceField)} every ${a.binSize}` : `${a.rules.length} rule${a.rules.length === 1 ? "" : "s"} | fallback: ${a.fallbackLabel}`;
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
            $ === "number_group" ? /* @__PURE__ */ e.jsx(E.NumberFormat, {}) : /* @__PURE__ */ e.jsx(E.Branch, {}),
            /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0, flex: 1 }, children: [
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: l.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: a.name }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: D })
            ] }),
            /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => y(Ur(u, a)), style: R(), children: [
              /* @__PURE__ */ e.jsx(E.Edit, {}),
              " Edit"
            ] }),
            /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => P(a), style: { ...R(), color: "#B91C1C" }, children: /* @__PURE__ */ e.jsx(E.Delete, {}) })
          ]
        },
        a.field
      );
    }) }),
    i && /* @__PURE__ */ e.jsx(
      "div",
      {
        role: "presentation",
        onMouseDown: (a) => {
          a.target === a.currentTarget && y(null);
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
            "aria-label": i.kind === "date_group" || i.kind === "number_group" ? i.__editingField ? "Edit grouped field" : "Add grouped field" : i.__editingField ? "Edit custom category" : "Add custom category",
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
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "13px", fontWeight: 900, color: l.text }, children: i.kind === "date_group" || i.kind === "number_group" ? i.__editingField ? "Edit Grouped Field" : "Add Grouped Field" : i.__editingField ? "Edit Custom Category" : "Add Custom Category" }),
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec, marginTop: "2px" }, children: i.kind === "date_group" ? "Create calendar buckets from a date field." : i.kind === "number_group" ? "Create fixed-width numeric bins from a number field." : "Rules can use base fields and categories defined before this one." })
                ] }),
                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => y(null), style: { ...R(), padding: "6px 8px" }, "aria-label": "Close category editor", children: /* @__PURE__ */ e.jsx(E.Close, {}) })
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
                        value: i.name || "",
                        onChange: (a) => {
                          const $ = a.target.value, D = i.id || er($);
                          y({ ...i, name: $, id: D, field: i.field || rr(D) });
                        },
                        style: k,
                        placeholder: "Example: Priority Segment"
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Else",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        value: i.fallbackLabel || "",
                        onChange: (a) => y({ ...i, fallbackLabel: a.target.value }),
                        style: k,
                        placeholder: "Other"
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Member Order",
                    /* @__PURE__ */ e.jsxs(
                      "select",
                      {
                        value: i.order || "creation",
                        onChange: (a) => y({ ...i, order: a.target.value }),
                        style: k,
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
                i.kind === "date_group" && /* @__PURE__ */ e.jsxs("div", { style: {
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
                      "select",
                      {
                        value: i.sourceField || "",
                        onChange: (a) => y({
                          ...i,
                          sourceField: a.target.value,
                          name: i.__isNew ? `${c(a.target.value)} ${ge(i.granularity || "month")}` : i.name
                        }),
                        style: k,
                        children: _.map((a) => /* @__PURE__ */ e.jsx("option", { value: a, children: c(a) }, a))
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Date Part",
                    /* @__PURE__ */ e.jsx(
                      "select",
                      {
                        value: i.granularity || "month",
                        onChange: (a) => y({ ...i, granularity: a.target.value }),
                        style: k,
                        children: Array.from(Yt).map((a) => /* @__PURE__ */ e.jsx("option", { value: a, children: ge(a) }, a))
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Fiscal Start",
                    /* @__PURE__ */ e.jsx(
                      "select",
                      {
                        value: i.fiscalYearStartMonth || 1,
                        onChange: (a) => y({ ...i, fiscalYearStartMonth: Number(a.target.value) }),
                        style: k,
                        children: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"].map((a, $) => /* @__PURE__ */ e.jsx("option", { value: $ + 1, children: a }, a))
                      }
                    )
                  ] })
                ] }),
                i.kind === "number_group" && /* @__PURE__ */ e.jsxs("div", { style: {
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
                      "select",
                      {
                        value: i.sourceField || "",
                        onChange: (a) => y({
                          ...i,
                          sourceField: a.target.value,
                          name: i.__isNew ? `${c(a.target.value)} Bins` : i.name
                        }),
                        style: k,
                        children: A.map((a) => /* @__PURE__ */ e.jsx("option", { value: a, children: c(a) }, a))
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Bin Size",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: i.binSize === void 0 ? "" : i.binSize,
                        onChange: (a) => y({ ...i, binSize: a.target.value }),
                        style: k
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Min",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: i.minValue === null || i.minValue === void 0 ? "" : i.minValue,
                        onChange: (a) => y({ ...i, minValue: a.target.value }),
                        style: k
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Max",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: i.maxValue === null || i.maxValue === void 0 ? "" : i.maxValue,
                        onChange: (a) => y({ ...i, maxValue: a.target.value }),
                        style: k
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
                        value: i.precision === null || i.precision === void 0 ? "" : i.precision,
                        onChange: (a) => y({ ...i, precision: a.target.value }),
                        style: k
                      }
                    )
                  ] })
                ] }),
                (!i.kind || i.kind === "category") && (i.rules || []).map((a, $) => {
                  const D = a.condition || { op: "AND", clauses: [] }, B = D.clauses || [];
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
                          onChange: (M) => W($, { ...a, label: M.target.value }),
                          style: k,
                          placeholder: "Category value"
                        }
                      ),
                      /* @__PURE__ */ e.jsxs(
                        "select",
                        {
                          value: D.op || "AND",
                          onChange: (M) => W($, { ...a, condition: { ...D, op: M.target.value } }),
                          style: k,
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
                          onClick: () => y({ ...i, rules: (i.rules || []).filter((M, H) => H !== $) }),
                          style: { ...R(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                          title: "Remove rule",
                          children: /* @__PURE__ */ e.jsx(E.Close, {})
                        }
                      )
                    ] }),
                    B.map((M, H) => {
                      const G = en.has(M.operator), b = Hr.has(M.operator);
                      return /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: G ? "1fr 130px 28px" : "1fr 130px 1fr 28px", gap: "6px", alignItems: "center" }, children: [
                        /* @__PURE__ */ e.jsx(
                          "select",
                          {
                            value: M.field || "",
                            onChange: (z) => O($, H, { field: z.target.value }),
                            style: k,
                            children: L.map((z) => /* @__PURE__ */ e.jsx("option", { value: z, children: c(z) }, z))
                          }
                        ),
                        /* @__PURE__ */ e.jsx(
                          "select",
                          {
                            value: M.operator || "eq",
                            onChange: (z) => {
                              const S = z.target.value;
                              O($, H, {
                                operator: S,
                                values: Hr.has(S) ? M.values && M.values.length ? M.values : Vr(M.value) : M.values
                              });
                            },
                            style: k,
                            children: Qt.map((z) => /* @__PURE__ */ e.jsx("option", { value: z.value, children: z.label }, z.value))
                          }
                        ),
                        !G && /* @__PURE__ */ e.jsx(
                          "input",
                          {
                            value: b ? (M.values || []).join(", ") : M.value || "",
                            onChange: (z) => O($, H, b ? { values: Vr(z.target.value), value: z.target.value } : { value: z.target.value }),
                            style: k,
                            placeholder: b ? "A, B, C" : "Value"
                          }
                        ),
                        /* @__PURE__ */ e.jsx(
                          "button",
                          {
                            type: "button",
                            onClick: () => W($, {
                              ...a,
                              condition: {
                                ...D,
                                clauses: B.filter((z, S) => S !== H)
                              }
                            }),
                            style: { ...R(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                            title: "Remove condition",
                            children: /* @__PURE__ */ e.jsx(E.Close, {})
                          }
                        )
                      ] }, `${a.id || $}:${H}`);
                    }),
                    /* @__PURE__ */ e.jsx(
                      "button",
                      {
                        type: "button",
                        onClick: () => W($, {
                          ...a,
                          condition: {
                            ...D,
                            clauses: [...B, ot(L)]
                          }
                        }),
                        style: { ...R(), alignSelf: "flex-start" },
                        children: "+ Condition"
                      }
                    )
                  ] }, a.id || $);
                }),
                (!i.kind || i.kind === "category") && /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => y({
                      ...i,
                      rules: [...i.rules || [], at(L, (i.rules || []).length)]
                    }),
                    style: { ...R(), alignSelf: "flex-start" },
                    children: "+ Rule"
                  }
                ),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "flex-end", gap: "8px" }, children: [
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => y(null), style: R(), children: "Cancel" }),
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: C, style: R("primary"), children: i.kind === "date_group" || i.kind === "number_group" ? "Apply Group" : "Apply Category" })
                ] })
              ] })
            ]
          }
        )
      }
    )
  ] });
}
function st(r) {
  const n = [], s = /* @__PURE__ */ new Set();
  return String(r || "").replace(tr, (o, l) => {
    const g = String(l || "").trim();
    return g && !s.has(g) && (n.push(g), s.add(g)), "";
  }), n;
}
function Gr(r, n = []) {
  const s = String(r || ""), o = [], l = /* @__PURE__ */ new Set(), g = (u) => {
    const F = String(u || "").trim();
    !F || l.has(F) || (o.push(F), l.add(F));
  };
  st(s).forEach(g);
  let c = s.replace(tr, " ");
  return (Array.isArray(n) ? n : Array.from(n || [])).map((u) => String(u || "").trim()).filter((u) => u && !nt.test(u)).sort((u, F) => F.length - u.length).forEach((u) => {
    const F = new RegExp(`(^|[^A-Za-z0-9_])${lt(u)}(?=$|[^A-Za-z0-9_])`, "g");
    c = c.replace(F, (i, y) => (g(u), `${y} `));
  }), c.replace(tt, (u, F) => {
    const i = c.slice(F + u.length).trimStart();
    return qt.has(u) || Jt.has(u) && i.startsWith("(") || g(u), u;
  }), o;
}
function oe(r) {
  const n = String(r || "").trim();
  return n ? `[${n}]` : "";
}
function me(r) {
  const n = String(r || "").trim();
  return n ? nt.test(n) ? n : `[${n}]` : "";
}
function pn(r, n, s = "formula") {
  const o = Se(r, s), l = new Set(
    (Array.isArray(n) ? n : []).map((v) => v ? v.agg === "formula" ? X(v) : v.field : null).filter(Boolean).map((v) => String(v).toLowerCase())
  );
  if (!l.has(o)) return o;
  let g = 2, c = `${o}${g}`;
  for (; l.has(c); )
    g += 1, c = `${o}${g}`;
  return c;
}
function un(r, n, s) {
  if (!r || !n || n === s) return r;
  const o = me(s), l = [];
  let c = String(r).replace(tr, (v, u) => String(u || "").trim() === n ? o : (l.push(v), `@@${l.length - 1}@@`)).replace(new RegExp(`\\b${lt(n)}\\b`, "g"), o);
  return l.forEach((v, u) => {
    c = c.replace(`@@${u}@@`, v);
  }), c;
}
function br(r, n, s) {
  r((o) => {
    const l = [...o];
    return l[n] ? (l[n] = s(l[n]), l) : o;
  });
}
function Nr(r) {
  return !r || typeof r.closest != "function" ? !1 : !!r.closest('button, input, select, textarea, [contenteditable="true"]');
}
function xn(r) {
  if (!r || typeof r != "object") return "";
  const n = r.field === void 0 || r.field === null ? "" : String(r.field), s = r.agg === void 0 || r.agg === null ? "" : String(r.agg);
  return `${n}::${s}`;
}
function hr(r) {
  const n = Nt(r);
  return n ? {
    ...ve,
    ...n,
    header: n.header || "",
    color: n.color || "",
    positiveColor: n.positiveColor || "",
    negativeColor: n.negativeColor || ""
  } : null;
}
function fn(r) {
  const n = {
    ...ve,
    ...r || {}
  }, s = {
    type: n.type || ve.type,
    metric: n.metric || ve.metric,
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
    l && (s[o] = l);
  }), Number.isFinite(Number(n.areaOpacity)) && (s.areaOpacity = Math.max(0.02, Math.min(0.45, Number(n.areaOpacity)))), s;
}
function dt(r, n = []) {
  const s = String(r && r.agg || "").trim().toLowerCase(), o = Array.isArray(n) && n.length > 0, l = s === "array_agg", g = s !== "array_agg" && o;
  return {
    canUseFieldSeries: l,
    canUsePivotSeries: g,
    canUseAny: l || g,
    defaultSource: l ? "field" : g ? "pivot" : null
  };
}
function vr(r, n) {
  return !n || !n.canUseAny ? !1 : r === "field" ? n.canUseFieldSeries : n.canUsePivotSeries;
}
function ct(r, n) {
  const s = fn(r);
  return !n || !n.canUseAny ? null : vr(s.source, n) ? s : {
    ...s,
    source: n.defaultSource
  };
}
function mr(r) {
  if (!Array.isArray(r) || r.length === 0) return "";
  const n = r.map((s) => s && me(s.field)).filter(Boolean);
  return n.length === 0 ? "" : n.length === 1 ? `${n[0]} * 100` : `${n[0]} - ${n[1]}`;
}
function Xr(r) {
  const n = (Array.isArray(r) ? r : []).map((s) => s && s.id).filter(Boolean);
  return n.length === 0 ? "" : n.length === 1 ? `${oe(n[0])} * 100` : `${oe(n[0])} - ${oe(n[1])}`;
}
function gn(r) {
  const n = new Set(
    (Array.isArray(r) ? r : []).map((o) => o && o.field).filter(Boolean)
  );
  let s = 1;
  for (; n.has(`formula_${s}`); ) s += 1;
  return `formula_${s}`;
}
function yn(r, n, s = null, o = null, l = "measures", g = [], c = []) {
  const v = String(r || "").trim(), u = he(l);
  if (!v)
    return {
      tone: "warning",
      text: u ? "Enter a formula using bracketed rendered columns. Example: [Cash Equity_day_pnl_sum] - [ETF_day_pnl_sum]" : "Enter a formula using value fields. Example: sales - [day pnl]"
    };
  let F = 0;
  for (const C of v)
    if (C === "(" && (F += 1), C === ")" && (F -= 1), F < 0)
      return { tone: "warning", text: "Parentheses are unbalanced." };
  if (F !== 0)
    return { tone: "warning", text: "Parentheses are unbalanced." };
  if (u) {
    const C = Array.isArray(g) ? g : [], P = new Set(
      C.map((B) => B && B.id).filter(Boolean)
    ), a = st(v);
    if (a.length === 0)
      return {
        tone: "warning",
        text: "Insert at least one rendered column reference like [column_id]."
      };
    const $ = v.replace(tr, ""), D = Array.from(new Set($.match(tt) || []));
    if (D.length > 0)
      return {
        tone: "warning",
        text: `Rendered columns must use brackets: ${D.join(", ")}`
      };
    if (P.size > 0) {
      const B = a.filter((M) => !P.has(M));
      if (B.length > 0)
        return {
          tone: "warning",
          text: `Unknown rendered columns: ${B.join(", ")}`
        };
    }
    return s && a.includes(s) ? {
      tone: "warning",
      text: "A displayed-column formula cannot reference its own output column."
    } : {
      tone: "success",
      text: `Using rendered columns: ${a.join(", ")}`
    };
  }
  const i = Array.isArray(n) ? n.filter((C) => C && C.field) : [];
  if (i.length === 0)
    return {
      tone: "warning",
      text: "Add at least one value or formula reference before creating formulas."
    };
  const y = i.filter((C) => C.agg === "formula"), T = /* @__PURE__ */ new Map();
  y.forEach((C) => {
    const P = C.field === s ? Se(o || X(C), C.field) : X(C);
    P && (T.set(P, P), T.set(P.toLowerCase(), P));
  });
  const L = i.filter((C) => C.agg !== "formula").map((C) => C.field).filter(Boolean), _ = new Set(L), A = /* @__PURE__ */ new Set([...L, ...y.map((C) => C.field === s ? Se(o || X(C), C.field) : X(C)).filter(Boolean), ...Array.isArray(c) ? c : []]), h = Array.from(A), k = Gr(v, h), R = k.map((C) => T.get(C) || T.get(C.toLowerCase()) || C), W = R.filter((C) => !A.has(C) && !_.has(C));
  if (W.length > 0)
    return {
      tone: "warning",
      text: `Unknown fields: ${W.join(", ")}`
    };
  const O = s ? Se(o || s, s) : null;
  if (s && O && i.some((P) => {
    if (!P || P.field === s) return !1;
    const a = P.agg === "formula" ? X(P) : P.field;
    return String(a || "").toLowerCase() === O.toLowerCase();
  }))
    return {
      tone: "warning",
      text: `Reference key "${O}" is already in use.`
    };
  if (O && R.includes(O))
    return {
      tone: "warning",
      text: "A formula cannot reference itself."
    };
  if (s && O) {
    const C = {};
    y.forEach((D) => {
      const B = D.field === s ? O : X(D), M = D.field === s ? v : String(D.formula || ""), H = Gr(M, h).map((G) => T.get(G) || T.get(G.toLowerCase()) || G).filter((G) => G !== B && y.some((b) => X(b) === G || b.field === s && O === G));
      C[B] = H;
    });
    const P = /* @__PURE__ */ new Set(), a = /* @__PURE__ */ new Set(), $ = (D) => {
      if (P.has(D)) return !0;
      if (a.has(D)) return !1;
      P.add(D);
      const B = C[D] || [];
      for (const M of B)
        if ($(M)) return !0;
      return P.delete(D), a.add(D), !1;
    };
    if ($(O))
      return {
        tone: "warning",
        text: "Circular formula reference detected."
      };
  }
  return k.length === 0 ? {
    tone: "warning",
    text: "Insert at least one measure or formula reference."
  } : {
    tone: "success",
    text: `Using ${R.join(", ")}`
  };
}
function bn(r) {
  if (!Array.isArray(r) || r.length === 0) return [];
  const n = r.map((s) => s && me(s.field)).filter(Boolean);
  return n.length === 0 ? [] : n.length === 1 ? [{ label: "Scale x100", value: `${n[0]} * 100` }] : [
    { label: "Difference", value: `${n[0]} - ${n[1]}` },
    { label: "Ratio %", value: `(${n[0]} / ${n[1]}) * 100` },
    { label: "Margin %", value: `((${n[0]} - ${n[1]}) / ${n[0]}) * 100` }
  ];
}
function hn(r) {
  const n = (Array.isArray(r) ? r : []).map((s) => s && s.id).filter(Boolean);
  return n.length === 0 ? [] : n.length === 1 ? [{ label: "Scale x100", value: `${oe(n[0])} * 100` }] : [
    { label: "Difference", value: `${oe(n[0])} - ${oe(n[1])}` },
    { label: "Ratio %", value: `(${oe(n[0])} / ${oe(n[1])}) * 100` },
    { label: "Spread %", value: `((${oe(n[0])} - ${oe(n[1])}) / ${oe(n[0])}) * 100` }
  ];
}
function mn(r, n) {
  const s = {
    muted: {
      text: r.textSec,
      border: r.border,
      background: r.headerSubtleBg || r.background
    },
    success: {
      text: r.primary,
      border: `${r.primary}33`,
      background: `${r.primary}10`
    },
    warning: {
      text: "#B45309",
      border: "#F59E0B55",
      background: "#F59E0B12"
    }
  };
  return s[n] || s.muted;
}
function vn({ item: r, idx: n, setValConfigs: s, theme: o, colFields: l = [], onClose: g }) {
  const c = f.useMemo(
    () => hr(r && r.sparkline) || { ...ve },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [r && r.sparkline]
  ), v = f.useMemo(
    () => dt(r, l),
    [l, r]
  ), u = f.useMemo(() => {
    const h = [];
    return v.canUsePivotSeries && h.push({ value: "pivot", label: "Linked pivot" }), v.canUseFieldSeries && h.push({ value: "field", label: "Series field" }), h;
  }, [v]), F = vr(c.source || "pivot", v) ? c.source || "pivot" : v.defaultSource, i = f.useCallback((h) => {
    br(s, n, (k) => {
      const R = hr(k.sparkline) || { ...ve }, W = ct({ ...R, ...h }, v);
      return W ? {
        ...k,
        sparkline: W
      } : k;
    });
  }, [v, n, s]), y = f.useCallback((h) => h.stopPropagation(), []), T = [
    { value: "line", label: "Line" },
    { value: "area", label: "Area" },
    { value: "column", label: "Column" },
    { value: "bar", label: "Bar" }
  ], L = {
    background: o.background,
    border: `1px solid ${o.border}`,
    borderRadius: "5px",
    color: o.text,
    fontSize: "11px",
    padding: "4px 7px",
    outline: "none",
    width: "100%",
    boxSizing: "border-box"
  }, _ = {
    fontSize: "10px",
    fontWeight: 700,
    letterSpacing: "0.06em",
    color: o.textSec,
    textTransform: "uppercase",
    marginBottom: "6px",
    display: "block"
  }, A = {
    marginBottom: "14px"
  };
  return /* @__PURE__ */ e.jsx(
    "div",
    {
      style: { position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", zIndex: 9999, display: "flex", alignItems: "center", justifyContent: "center" },
      onMouseDown: y,
      onClick: g,
      children: /* @__PURE__ */ e.jsxs(
        "div",
        {
          onClick: y,
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
                  onClick: g,
                  style: { background: "none", border: "none", cursor: "pointer", color: o.textSec, fontSize: "18px", lineHeight: 1, padding: "2px 4px" },
                  children: "×"
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Data source" }),
              u.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: u.map(({ value: h, label: k }) => /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => i({ source: h }),
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
                    children: k
                  },
                  h
                )) }),
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
            F === "field" && /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Cell display" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: [
                { value: "trend", label: "Trendline" },
                { value: "value", label: "Value" }
              ].map(({ value: h, label: k }) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => i({ displayMode: h }),
                  style: {
                    flex: 1,
                    padding: "4px 0",
                    fontSize: "11px",
                    fontWeight: 600,
                    borderRadius: "6px",
                    cursor: "pointer",
                    border: `1px solid ${(c.displayMode || "trend") === h ? o.primary : o.border}`,
                    background: (c.displayMode || "trend") === h ? `${o.primary}18` : o.headerSubtleBg || o.background,
                    color: (c.displayMode || "trend") === h ? o.primary : o.textSec
                  },
                  children: k
                },
                h
              )) }),
              /* @__PURE__ */ e.jsx("div", { style: { marginTop: "5px", fontSize: "10px", color: o.textSec, lineHeight: 1.4 }, children: (c.displayMode || "trend") === "value" ? "Show the selected summary metric from the series." : "Draw the series in the cell." })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Chart type" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: T.map(({ value: h, label: k }) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => i({ type: h }),
                  style: {
                    flex: 1,
                    padding: "4px 0",
                    fontSize: "11px",
                    fontWeight: 600,
                    borderRadius: "6px",
                    cursor: "pointer",
                    border: `1px solid ${c.type === h ? o.primary : o.border}`,
                    background: c.type === h ? `${o.primary}18` : o.headerSubtleBg || o.background,
                    color: c.type === h ? o.primary : o.textSec
                  },
                  children: k
                },
                h
              )) })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Summary metric" }),
              /* @__PURE__ */ e.jsxs(
                "select",
                {
                  value: c.metric,
                  onChange: (h) => i({ metric: h.target.value }),
                  style: { ...L, cursor: "pointer" },
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
            /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Display" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px" }, children: [
                { key: "showCurrentValue", label: "Value label" },
                { key: "showDelta", label: "Delta label" },
                { key: "compact", label: "Compact cell" },
                { key: "hideColumns", label: "Hide source values" }
              ].map(({ key: h, label: k }) => /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "5px", fontSize: "11px", color: o.text, cursor: "pointer", userSelect: "none" }, children: [
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "checkbox",
                    checked: !!c[h],
                    onChange: (R) => i({ [h]: R.target.checked }),
                    style: { accentColor: o.primary, width: "13px", height: "13px", cursor: "pointer" }
                  }
                ),
                k
              ] }, h)) }),
              c.hideColumns && /* @__PURE__ */ e.jsx("div", { style: { marginTop: "6px", fontSize: "10px", color: o.textSec, background: `${o.primary}10`, border: `1px solid ${o.primary}30`, borderRadius: "6px", padding: "5px 8px", lineHeight: 1.4 }, children: "Source value columns are hidden; the trend column remains." }),
              !c.hideColumns && F === "pivot" && /* @__PURE__ */ e.jsx("div", { style: { marginTop: "6px", fontSize: "10px", color: o.textSec, background: o.headerSubtleBg || o.background, border: `1px solid ${o.border}`, borderRadius: "6px", padding: "5px 8px", lineHeight: 1.4 }, children: "Source value columns stay numeric; the trend column is added beside them." })
            ] }),
            F === "pivot" && /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Position" }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: o.textSec, lineHeight: 1.4, background: o.headerSubtleBg || o.background, border: `1px solid ${o.border}`, borderRadius: "6px", padding: "6px 8px" }, children: "Trend columns are placed before the matching value columns." })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Colors" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: [
                { key: "color", label: "Line / fill color", always: !0 },
                { key: "positiveColor", label: "Positive bar color", always: !1 },
                { key: "negativeColor", label: "Negative bar color", always: !1 }
              ].filter(({ always: h }) => h || c.type === "column" || c.type === "bar").map(({ key: h, label: k }) => /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: o.textSec, marginBottom: "3px", display: "block" }, children: k }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "5px", alignItems: "center" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "color",
                      value: c[h] || (h === "negativeColor" ? "#B91C1C" : h === "positiveColor" ? "#2F855A" : "#2563EB"),
                      onChange: (R) => i({ [h]: R.target.value }),
                      style: { width: "28px", height: "26px", padding: "1px 2px", border: `1px solid ${o.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "text",
                      value: c[h] || "",
                      onChange: (R) => i({ [h]: R.target.value }),
                      placeholder: "Auto",
                      style: { ...L, flex: 1 }
                    }
                  ),
                  c[h] && /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => i({ [h]: "" }),
                      style: { background: "none", border: "none", cursor: "pointer", color: o.textSec, fontSize: "14px", lineHeight: 1, padding: "2px" },
                      title: "Reset to auto",
                      children: "×"
                    }
                  )
                ] })
              ] }, h)) })
            ] }),
            c.type === "area" && /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Area fill opacity" }),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px" }, children: [
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "range",
                    min: "0.02",
                    max: "0.45",
                    step: "0.01",
                    value: c.areaOpacity != null ? c.areaOpacity : 0.14,
                    onChange: (h) => i({ areaOpacity: Number(h.target.value) }),
                    style: { flex: 1, accentColor: o.primary }
                  }
                ),
                /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "11px", color: o.textSec, minWidth: "32px", textAlign: "right" }, children: [
                  Math.round((c.areaOpacity != null ? c.areaOpacity : 0.14) * 100),
                  "%"
                ] })
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: A, children: [
              /* @__PURE__ */ e.jsx("span", { style: _, children: "Column header label" }),
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  type: "text",
                  value: c.header || "",
                  onChange: (h) => i({ header: h.target.value }),
                  placeholder: "Auto (e.g. Sales Trend)",
                  style: L
                }
              )
            ] }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", justifyContent: "flex-end" }, children: /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: g,
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
function pt({ item: r, idx: n, setValConfigs: s, theme: o, colFields: l = [], fixedSparklineValueKeySet: g = /* @__PURE__ */ new Set(), showNotification: c }) {
  const [v, u] = f.useState(!1), F = hr(r && r.sparkline), i = xn(r), y = !!(i && g && g.has(i)), T = f.useMemo(
    () => dt(r, l),
    [l, r]
  ), L = !!F, _ = !!(F && vr(F.source || "pivot", T)), A = f.useCallback((W) => W.stopPropagation(), []), h = f.useCallback(() => {
    let W = !1;
    return br(s, n, (O) => {
      const C = ct(
        O.sparkline || ve,
        T
      );
      return C ? (W = !0, { ...O, sparkline: C }) : O;
    }), W;
  }, [T, n, s]), k = f.useCallback((W) => {
    if (W.stopPropagation(), L) {
      if (y) {
        c && c("This trend is defined by the app. You can adjust its settings, but not remove it here.", "info");
        return;
      }
      br(s, n, (O) => ({ ...O, sparkline: !1 })), c && c("Removed trend column.", "info");
    } else T.canUseAny && h() ? (u(!0), c && c("Trend column enabled.", "success")) : c && c("Trend needs a column field, or a Series aggregation for field data.", "warning");
  }, [T.canUseAny, L, h, n, y, s, c]), R = f.useCallback((W) => {
    if (W.stopPropagation(), !T.canUseAny) {
      c && c("Trend settings are not available for this value selection.", "warning");
      return;
    }
    h(), u(!0);
  }, [T.canUseAny, h, c]);
  return L ? /* @__PURE__ */ e.jsxs(
    "div",
    {
      "data-value-sparkline-controls": "true",
      "data-value-sparkline-available": T.canUseAny ? "true" : "false",
      "data-value-sparkline-enabled": _ ? "true" : "false",
      onMouseDown: A,
      onClick: A,
      style: { marginTop: "6px", paddingTop: "6px", borderTop: `1px solid ${o.border}`, width: "100%", display: "flex", alignItems: "center", gap: "6px" },
      children: [
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "data-value-sparkline-settings": "true",
            onClick: R,
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
        y && /* @__PURE__ */ e.jsx(
          "span",
          {
            "data-value-sparkline-fixed": "true",
            style: { fontSize: "10px", color: o.textSec, flexShrink: 0 },
            title: "This trend is defined by the app",
            children: "App trend"
          }
        ),
        !y && /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "data-value-sparkline-toggle": "true",
            onClick: k,
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
        _ && !v && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: o.primary, flex: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: [
          (r.sparkline && r.sparkline.source) === "field" ? `series ${(r.sparkline && r.sparkline.displayMode) === "value" ? "value" : "trend"}` : "linked pivot",
          " | ",
          ["line", "area", "column", "bar"].includes(r.sparkline && r.sparkline.type) ? r.sparkline.type : "line",
          " | ",
          r.sparkline && r.sparkline.metric || "last"
        ] }),
        v && /* @__PURE__ */ e.jsx(
          vn,
          {
            item: r,
            idx: n,
            setValConfigs: s,
            theme: o,
            colFields: l,
            onClose: () => u(!1)
          }
        )
      ]
    }
  ) : null;
}
function Jr({ item: r, idx: n, selected: s, onSelect: o, onRemove: l, setValConfigs: g, theme: c, styles: v, dropLine: u, zoneId: F, valueSelectStyle: i, colFields: y = [], fixedSparklineValueKeySet: T = /* @__PURE__ */ new Set(), showNotification: L, serverError: _ = null }) {
  const A = s ? `${c.primary}77` : _ ? "#F59E0B88" : c.border, h = s ? c.headerBg || `${c.primary}0f` : c.background || "transparent", k = String(r.formula || "").trim() || "Empty formula";
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      onClick: () => o(r.field),
      style: {
        ...v.chip,
        border: `1px solid ${A}`,
        background: h,
        borderLeft: s ? `3px solid ${c.primary}` : "3px solid transparent",
        boxShadow: "none",
        cursor: "pointer",
        flexWrap: "wrap",
        alignItems: "flex-start"
      },
      title: "Select formula",
      children: [
        u && u.zone === F && u.idx === n && /* @__PURE__ */ e.jsx("div", { style: { ...v.dropLine, top: -2 } }),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
          /* @__PURE__ */ e.jsx(E.DragIndicator, {}),
          /* @__PURE__ */ e.jsx("span", { style: {
            fontSize: "10px",
            fontWeight: 700,
            color: c.primary,
            background: `${c.primary}18`,
            borderRadius: "999px",
            padding: "3px 7px",
            letterSpacing: "0.5px",
            flexShrink: 0
          }, children: "fx" }),
          /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0, flex: 1 }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0 }, children: [
              /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 600, display: "block", minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: r.label || "Formula" }),
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: c.textSec, fontFamily: "monospace", flexShrink: 0 }, children: X(r) }),
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "9px", color: c.textSec, border: `1px solid ${c.border}`, borderRadius: "999px", padding: "1px 6px", flexShrink: 0 }, children: he(r.formulaScope) ? "columns" : "values" })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: {
              marginTop: "2px",
              fontSize: "10px",
              lineHeight: 1.35,
              color: c.textSec,
              fontFamily: "monospace",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis"
            }, children: [
              "=",
              k
            ] }),
            _ && /* @__PURE__ */ e.jsxs("div", { style: { marginTop: "2px", fontSize: "9px", color: "#B45309" }, children: [
              "⚠ ",
              Sn[_] || _
            ] })
          ] })
        ] }),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", marginLeft: "8px", alignItems: "center", flexShrink: 0 }, children: [
          s && /* @__PURE__ */ e.jsx("span", { style: {
            fontSize: "9px",
            color: c.primary,
            background: `${c.primary}14`,
            borderRadius: "999px",
            padding: "2px 6px",
            flexShrink: 0
          }, children: "Active" }),
          /* @__PURE__ */ e.jsx(
            "span",
            {
              onClick: (R) => {
                R.stopPropagation(), l(n);
              },
              style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" },
              children: /* @__PURE__ */ e.jsx(E.Close, {})
            }
          )
        ] }),
        /* @__PURE__ */ e.jsx(
          pt,
          {
            item: r,
            idx: n,
            setValConfigs: g,
            theme: c,
            valueSelectStyle: i,
            colFields: y,
            fixedSparklineValueKeySet: T,
            showNotification: L
          }
        ),
        u && u.zone === F && u.idx === n + 1 && /* @__PURE__ */ e.jsx("div", { style: { ...v.dropLine, bottom: -2 } })
      ]
    }
  );
}
const Sn = {
  self_reference: "Formula references itself (server)",
  circular_reference: "Circular dependency with another formula (server)",
  expression_error: "Expression could not be evaluated (server)",
  missing_reference: "Unknown field reference (server)"
};
function jn({ item: r, idx: n, valConfigs: s, setValConfigs: o, theme: l, onClose: g, onRemove: c, displayedColumnOptions: v = [], availableRawFields: u = [], serverError: F = null }) {
  const i = f.useRef(null), [y, T] = f.useState({
    label: r.label || "",
    formula: r.formula || "",
    formulaRef: X(r),
    formulaScope: Ee(r.formulaScope)
  }), L = f.useRef(r);
  L.current = r, f.useEffect(() => {
    const b = L.current;
    T({
      label: b.label || "",
      formula: b.formula || "",
      formulaRef: X(b),
      formulaScope: Ee(b.formulaScope)
    });
  }, [r.field]), f.useEffect(() => {
    if (!i.current) return;
    i.current.focus();
    const b = i.current.value.length;
    i.current.setSelectionRange(b, b);
  }, [r.field]);
  const _ = f.useMemo(
    () => s.filter((b) => b && b.field),
    [s]
  ), A = f.useMemo(
    () => _.filter((b) => b.agg !== "formula"),
    [_]
  ), h = f.useMemo(
    () => _.filter((b) => b.agg === "formula" && b.field !== r.field),
    [r.field, _]
  ), k = f.useMemo(
    () => (Array.isArray(v) ? v : []).filter((b) => b && b.id && b.id !== r.field),
    [v, r.field]
  ), R = f.useMemo(
    () => {
      const b = new Set(_.map((z) => z.field).filter(Boolean));
      return (Array.isArray(u) ? u : []).filter((z) => !b.has(z));
    },
    [u, _]
  ), W = f.useMemo(
    () => yn(
      y.formula,
      _,
      r.field,
      y.formulaRef,
      y.formulaScope,
      k,
      R
    ),
    [k, y.formula, y.formulaRef, y.formulaScope, r.field, _, R]
  ), O = f.useMemo(
    () => mn(l, W.tone),
    [W.tone, l]
  ), C = f.useMemo(
    () => he(y.formulaScope) ? hn(k) : bn(A),
    [k, y.formulaScope, A]
  ), P = f.useMemo(
    () => he(y.formulaScope) ? Xr(k) || "[column_id] - [other_column_id]" : mr(A) || `${me(y.formulaRef || r.field)} * 100`,
    [k, y.formulaRef, y.formulaScope, r.field, A]
  ), a = y.label !== (r.label || "") || y.formula !== (r.formula || "") || y.formulaRef !== X(r) || y.formulaScope !== Ee(r.formulaScope), $ = f.useCallback(() => {
    const b = Se(y.formulaRef, r.field);
    o((z) => {
      const S = [...z], U = S[n];
      if (!U) return z;
      const ee = X(U);
      let Y = b;
      const re = new Set(
        S.filter((Z, te) => Z && te !== n).map((Z) => Z.agg === "formula" ? X(Z) : Z.field).filter(Boolean).map((Z) => String(Z).toLowerCase())
      );
      if (re.has(Y.toLowerCase())) {
        let Z = 2, te = `${Y}${Z}`;
        for (; re.has(te.toLowerCase()); )
          Z += 1, te = `${Y}${Z}`;
        Y = te;
      }
      if (S[n] = {
        ...U,
        label: y.label,
        formula: y.formula,
        formulaRef: Y,
        formulaScope: y.formulaScope
      }, ee && ee !== Y)
        for (let Z = 0; Z < S.length; Z += 1) {
          if (Z === n) continue;
          const te = S[Z];
          !te || te.agg !== "formula" || !te.formula || (S[Z] = {
            ...te,
            formula: un(te.formula, ee, Y)
          });
        }
      return S;
    });
  }, [y.formula, y.formulaRef, y.formulaScope, y.label, n, r.field, o]), D = f.useCallback(() => {
    T({
      label: r.label || "",
      formula: r.formula || "",
      formulaRef: X(r),
      formulaScope: Ee(r.formulaScope)
    }), g();
  }, [r.field, r.formula, r.formulaRef, r.formulaScope, r.label, g]), B = f.useCallback((b) => {
    const z = i.current;
    if (!z) return;
    const S = y.formula || "", U = z.selectionStart ?? S.length, ee = z.selectionEnd ?? S.length, Y = `${S.slice(0, U)}${b}${S.slice(ee)}`;
    T((Z) => ({ ...Z, formula: Y }));
    const re = U + b.length;
    setTimeout(() => {
      i.current && (i.current.focus(), i.current.setSelectionRange(re, re));
    }, 0);
  }, [y.formula]), M = f.useCallback(() => {
    $(), g();
  }, [$, g]), H = {
    border: `1px solid ${l.border}`,
    background: l.background,
    color: l.text,
    borderRadius: "999px",
    cursor: "pointer",
    padding: "3px 9px",
    fontSize: "10px",
    lineHeight: "16px"
  }, G = {
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
      onClick: D,
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
          onClick: (b) => b.stopPropagation(),
          onKeyDown: (b) => {
            b.key === "Escape" && D(), (b.ctrlKey || b.metaKey) && b.key === "Enter" && M();
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
                  onClick: D,
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
                    value: y.label,
                    onChange: (b) => T((z) => ({ ...z, label: b.target.value })),
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
                    value: y.formulaRef,
                    onChange: (b) => T((z) => ({ ...z, formulaRef: Se(b.target.value, r.field) })),
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
                  "select",
                  {
                    value: y.formulaScope,
                    onChange: (b) => {
                      const z = Ee(b.target.value);
                      T((S) => ({
                        ...S,
                        formulaScope: z,
                        formula: S.formula || (z === "columns" ? Xr(k) : mr(A))
                      }));
                    },
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.headerSubtleBg || l.background,
                      color: l.text,
                      borderRadius: "8px",
                      padding: "8px 10px",
                      fontSize: "12px",
                      outline: "none"
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
                  ref: i,
                  value: y.formula,
                  onChange: (b) => T((z) => ({ ...z, formula: b.target.value })),
                  placeholder: P,
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
                border: `1px solid ${O.border}`,
                background: O.background,
                color: O.text,
                fontSize: "10px",
                lineHeight: "14px"
              }, children: W.text }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: "Ctrl/Cmd + Enter applies" })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              C.map((b) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => T((z) => ({ ...z, formula: b.value })),
                  style: H,
                  children: b.label
                },
                b.label
              )),
              ["+", "-", "*", "/", "(", ")", "* 100"].map((b) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onMouseDown: (z) => z.preventDefault(),
                  onClick: () => B(b),
                  style: G,
                  children: b
                },
                b
              ))
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr", gap: "12px", marginBottom: "14px" }, children: [
              he(y.formulaScope) ? /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Rendered Columns" }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
                  k.map((b) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(oe(b.id)),
                      style: H,
                      title: `Insert ${b.id}`,
                      children: b.label || b.id
                    },
                    b.id
                  )),
                  k.length === 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: l.textSec }, children: "Render the pivot once, or type a bracketed column id directly." })
                ] })
              ] }) : /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Measures" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: A.map((b) => /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onMouseDown: (z) => z.preventDefault(),
                    onClick: () => B(me(b.field)),
                    style: H,
                    title: `Insert ${b.field}`,
                    children: b.field
                  },
                  b.field
                )) })
              ] }),
              !he(y.formulaScope) && h.length > 0 && /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Formula References" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: h.map((b) => /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onMouseDown: (z) => z.preventDefault(),
                    onClick: () => B(me(X(b))),
                    style: H,
                    title: `${X(b)}${b.label ? ` (${b.label})` : ""}`,
                    children: X(b)
                  },
                  b.field
                )) })
              ] }),
              !he(y.formulaScope) && R.length > 0 && /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Data Fields (auto-aggregated as sum)" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: R.map((b) => /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onMouseDown: (z) => z.preventDefault(),
                    onClick: () => B(me(b)),
                    style: { ...H, borderColor: `${l.primary}66`, color: l.primary, background: `${l.primary}0d` },
                    title: `Insert: ${b}`,
                    children: ge(b)
                  },
                  b
                )) })
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "space-between", gap: "8px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => {
                    c(n), g();
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
                    onClick: D,
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
                    onClick: M,
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
function qr({
  panelId: r,
  panelLabel: n,
  size: s,
  onSizeChange: o,
  theme: l,
  outerStyle: g = {},
  contentStyle: c = {},
  children: v
}) {
  const u = f.useRef(null), F = f.useRef(null), i = f.useMemo(() => Zt(r), [r]), y = f.useMemo(
    () => Kt(r, s),
    [r, s]
  ), T = f.useCallback(() => {
    if (F.current) {
      const A = F.current;
      F.current = null, A();
    }
  }, []);
  f.useEffect(() => (() => T()), [T]);
  const L = f.useCallback((A, h) => {
    if (!u.current || typeof o != "function") return;
    A.preventDefault(), A.stopPropagation(), T();
    const k = u.current.getBoundingClientRect(), R = u.current.parentElement ? u.current.parentElement.getBoundingClientRect() : k, W = {
      startX: A.clientX,
      startY: A.clientY,
      startWidth: k.width,
      startHeight: k.height,
      maxWidth: Math.max(i.minWidth, Math.min(i.maxWidth, Math.floor(R.width)))
    }, O = document.body.style.cursor, C = document.body.style.userSelect;
    document.body.style.cursor = h === "both" ? "nwse-resize" : h === "width" ? "ew-resize" : "ns-resize", document.body.style.userSelect = "none";
    const P = (D) => {
      const B = D.clientX - W.startX, M = D.clientY - W.startY, H = h === "height" ? y.width : Math.max(i.minWidth, Math.min(W.maxWidth, Math.round(W.startWidth + B))), G = h === "width" ? y.height : Math.max(i.minHeight, Math.min(i.maxHeight, Math.round(W.startHeight + M)));
      o(r, { width: H, height: G });
    }, a = () => {
      document.body.style.cursor = O, document.body.style.userSelect = C, window.removeEventListener("mousemove", P), window.removeEventListener("mouseup", $);
    }, $ = () => T();
    F.current = a, window.addEventListener("mousemove", P), window.addEventListener("mouseup", $);
  }, [
    i.maxHeight,
    i.maxWidth,
    i.minHeight,
    i.minWidth,
    o,
    r,
    y.height,
    y.width,
    T
  ]), _ = `${l.primary}55`;
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      ref: u,
      style: {
        position: "relative",
        width: y.width === null ? "100%" : `${y.width}px`,
        maxWidth: "100%",
        height: `${y.height}px`,
        minHeight: `${i.minHeight}px`,
        maxHeight: `${i.maxHeight}px`,
        boxSizing: "border-box",
        overflow: "hidden",
        ...g
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
              ...c
            },
            children: v
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (A) => L(A, "width"),
            style: {
              position: "absolute",
              top: 0,
              right: 0,
              bottom: "12px",
              width: "12px",
              cursor: "ew-resize",
              zIndex: 4,
              touchAction: "none"
            },
            title: `Resize ${n} width`
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (A) => L(A, "height"),
            style: {
              position: "absolute",
              left: 0,
              right: "12px",
              bottom: 0,
              height: "12px",
              cursor: "ns-resize",
              zIndex: 4,
              touchAction: "none"
            },
            title: `Resize ${n} height`
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onMouseDown: (A) => L(A, "both"),
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
                style: {
                  position: "absolute",
                  right: "4px",
                  bottom: "4px",
                  width: "3px",
                  height: "3px",
                  borderRadius: "999px",
                  background: _,
                  boxShadow: `-4px 0 0 ${_}, 0 -4px 0 ${_}, -4px -4px 0 ${_}`
                }
              }
            )
          }
        )
      ]
    }
  );
}
function Qr({ measureAxis: r, setMeasureAxis: n, theme: s, valueSelectStyle: o, regularValueCount: l }) {
  const g = f.useMemo(() => Pe(r), [r]), c = f.useCallback((u) => {
    typeof n == "function" && n((F) => ({
      ...Pe(F),
      ...typeof u == "function" ? u(Pe(F)) : u
    }));
  }, [n]), v = {
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
    border: `1px solid ${s.border}`,
    borderRadius: s.radiusSm || "6px",
    background: s.surfaceInset || s.headerSubtleBg || s.background
  }, children: [
    /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: s.textSec, fontWeight: 700, textTransform: "uppercase" }, children: "Values as" }),
    /* @__PURE__ */ e.jsxs(
      "select",
      {
        value: g.placement,
        onChange: (u) => c({ placement: u.target.value }),
        style: { ...o, width: "98px" },
        title: "Values placement",
        children: [
          /* @__PURE__ */ e.jsx("option", { value: "none", children: "Columns" }),
          /* @__PURE__ */ e.jsx("option", { value: "rows", children: "Rows" }),
          /* @__PURE__ */ e.jsx("option", { value: "columns", children: "Column Axis" })
        ]
      }
    ),
    g.placement !== "none" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsx(
        "input",
        {
          value: g.labelField,
          onChange: (u) => c({ labelField: u.target.value }),
          style: v,
          title: "Measure label field"
        }
      ),
      /* @__PURE__ */ e.jsx(
        "input",
        {
          value: g.valueField,
          onChange: (u) => c({ valueField: u.target.value }),
          style: { ...v, width: "82px" },
          title: "Measure value field"
        }
      ),
      /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "4px", fontSize: "10px", color: s.textSec }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "checkbox",
            checked: g.suppressEmptyMembers,
            onChange: (u) => c({ suppressEmptyMembers: u.target.checked })
          }
        ),
        "Empty"
      ] }),
      /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "4px", fontSize: "10px", color: s.textSec }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "checkbox",
            checked: g.suppressZeroMembers,
            onChange: (u) => c({ suppressZeroMembers: u.target.checked })
          }
        ),
        "Zero"
      ] })
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: s.textSec }, children: [
      l,
      " selected"
    ] })
  ] });
}
const $n = f.memo(function({
  sidebarTab: n,
  setSidebarTab: s,
  rowFields: o,
  setRowFields: l,
  colFields: g,
  setColFields: c,
  valConfigs: v,
  setValConfigs: u,
  measureAxis: F = {},
  setMeasureAxis: i = null,
  columnVisibility: y,
  setColumnVisibility: T,
  columnPinning: L,
  setColumnPinning: _,
  availableFields: A,
  table: h,
  pinningPresets: k,
  showNotification: R,
  filterAnchorEl: W,
  setFilterAnchorEl: O,
  colSearch: C,
  setColSearch: P,
  colTypeFilter: a,
  setColTypeFilter: $,
  selectedCols: D,
  setSelectedCols: B,
  dropLine: M,
  onDragStart: H,
  onDragOver: G,
  onDrop: b,
  onDragEnd: z,
  onKeyboardFieldDrop: S,
  handleHeaderFilter: U,
  handleFilterClick: ee,
  requestFilterOptions: Y,
  handleExpandAllRows: re,
  handlePinColumn: Z,
  toggleAllColumnsPinned: te,
  activeFilterCol: kn,
  closeFilterPopover: Cn,
  filterOptions: Me,
  filterOptionMeta: Oe = {},
  clientFilterOptionMap: He = {},
  data: je,
  sidebarWidth: nr,
  setSidebarWidth: Sr,
  fieldPanelSizes: Ue,
  setFieldPanelSizes: lr,
  fixedSparklineValueKeys: or = [],
  sparklineFields: wn = null,
  displayedColumnOptions: ut = [],
  reportEditorSelection: xt = null,
  showReportConfigColumn: ft = !0,
  setShowReportConfigColumn: gt = null,
  formulaErrors: ke = {},
  serverSide: Ve = !1
}) {
  Lt("SidebarPanel");
  const { theme: d, styles: K } = Pt(), {
    filters: ie,
    setFilters: ar,
    pivotMode: yt,
    reportDef: bt,
    setReportDef: ht,
    savedReports: mt,
    setSavedReports: vt,
    activeReportId: St,
    setActiveReportId: jt,
    customDimensions: _e,
    setCustomDimensions: jr
  } = Ot(), [Ye, kr] = f.useState({ columnId: null, anchorEl: null }), [Ce, we] = f.useState(null), [ce, ye] = f.useState(null), [Re, kt] = f.useState("report"), Ze = f.useRef(null), [Ct, Ke] = f.useState(null), xe = f.useRef(null), Cr = f.useRef(0), Ge = f.useRef(null), pe = f.useRef(null), [N, ir] = f.useState(null), [sr, wr] = f.useState(""), dr = f.useMemo(
    () => new Set(Array.isArray(or) ? or : []),
    [or]
  ), J = f.useCallback(
    (t) => Ht(t, _e),
    [_e]
  ), Rr = f.useMemo(
    () => Pe(F),
    [F]
  ), fe = Rr.labelField || "Measure Name", De = f.useMemo(
    () => (A || []).filter((t) => t),
    [A]
  ), q = f.useMemo(
    () => De.filter((t) => t && !et(t)),
    [De]
  ), Te = Array.isArray(v) && v.length > 0 && fe && !De.includes(fe), Ae = f.useMemo(
    () => Te ? [fe, ...De] : De,
    [De, fe, Te]
  ), We = sr.trim().toLowerCase(), Ne = f.useCallback((t) => {
    if (!We) return !0;
    const x = Te && t === fe ? "measure name measure names metric metrics value values" : "";
    return String(t || "").toLowerCase().includes(We) || String(J(t) || "").toLowerCase().includes(We) || x.includes(We);
  }, [J, fe, We, Te]), cr = f.useMemo(
    () => Ae.length > 8 ? Ae.filter(Ne) : Ae,
    [Ae, Ne]
  ), pr = f.useMemo(
    () => q.length > 8 ? q.filter(Ne) : q,
    [q, Ne]
  ), Dr = f.useCallback((t) => {
    l((x) => (x || []).filter((p) => p !== t)), c((x) => (x || []).filter((p) => p !== t)), u((x) => (x || []).filter((p) => !(p && p.field === t))), ar((x) => {
      if (!x || !Object.prototype.hasOwnProperty.call(x, t)) return x;
      const p = { ...x };
      return delete p[t], p;
    });
  }, [c, ar, l, u]), wt = f.useCallback((t) => {
    const x = xe.current;
    if (!x) return;
    const p = 60, m = 12, j = x.getBoundingClientRect(), I = t.clientY, Q = I - j.top, w = j.bottom - I;
    let V = 0;
    if (Q < p && Q >= 0 ? V = -1 : w < p && w >= 0 && (V = 1), V === 0) {
      pe.current && (cancelAnimationFrame(pe.current), pe.current = null);
      return;
    }
    const de = () => {
      xe.current && (xe.current.scrollTop += V * m, pe.current = requestAnimationFrame(de));
    };
    pe.current || (pe.current = requestAnimationFrame(de));
  }, []), Ar = f.useCallback(() => {
    pe.current && (cancelAnimationFrame(pe.current), pe.current = null);
  }, []), se = f.useMemo(
    () => v.filter((t) => t && t.agg === "formula"),
    [v]
  ), be = f.useMemo(
    () => se.length,
    [se]
  ), $e = f.useMemo(
    () => v.filter((t) => t && t.agg !== "formula").length,
    [v]
  ), Rt = f.useMemo(() => [
    { id: "rows", label: "Rows", icon: /* @__PURE__ */ e.jsx(E.List, {}) },
    { id: "cols", label: "Columns", icon: /* @__PURE__ */ e.jsx(E.Columns, {}) },
    { id: "vals", label: "Values", icon: /* @__PURE__ */ e.jsx(E.Sigma, {}) },
    { id: "filter", label: "Filters", icon: /* @__PURE__ */ e.jsx(E.Filter, {}) }
  ], []), Be = f.useMemo(() => ({
    filter: Object.keys(ie || {}).filter((t) => t !== "global"),
    rows: o || [],
    cols: g || [],
    vals: v || []
  }), [g, ie, o, v]), Xe = f.useCallback(() => {
    N && R && R("Keyboard move cancelled.", "info"), ir(null);
  }, [N, R]), $r = f.useCallback((t, x, p, m) => {
    if (typeof S != "function") return;
    ir({ field: t, zone: x, idx: p, label: m }), R && R(`Picked up ${m}. Focus a target field or zone and press Enter.`, "info");
  }, [S, R]), Je = f.useCallback((t, x = null) => {
    if (!N || typeof S != "function") return !1;
    const p = Be[t] || [], m = Number(x), j = Number.isFinite(m) ? Math.max(0, Math.min(Math.floor(m), p.length)) : p.length, I = S({
      field: N.field,
      srcZone: N.zone,
      srcIdx: N.idx,
      targetZone: t,
      targetIdx: j,
      source: "layout:keyboard-drop"
    });
    return I !== !1 && (R && R(`Moved ${N.label}.`, "success"), ir(null)), I !== !1;
  }, [N, S, R, Be]), Fr = f.useCallback((t, x, p, m, j) => {
    if (!Nr(t.target)) {
      if (t.key === "Escape") {
        t.preventDefault(), t.stopPropagation(), Xe();
        return;
      }
      t.key !== "Enter" && t.key !== " " || (t.preventDefault(), t.stopPropagation(), N ? Je(p, m) : $r(x, p, m, j));
    }
  }, [Xe, Je, N, $r]), Dt = f.useCallback((t, x) => {
    if (!Nr(t.target)) {
      if (t.key === "Escape") {
        t.preventDefault(), t.stopPropagation(), Xe();
        return;
      }
      !N || t.key !== "Enter" && t.key !== " " || (t.preventDefault(), t.stopPropagation(), Je(x, (Be[x] || []).length));
    }
  }, [Xe, Je, N, Be]), At = f.useCallback((t) => {
    t.preventDefault();
    const x = t.clientX, p = nr || 288;
    Ge.current = { startX: x, startWidth: p };
    const m = (I) => {
      const Q = I.clientX - Ge.current.startX, w = Math.max(200, Math.min(520, Ge.current.startWidth + Q));
      Sr(w);
    }, j = () => {
      Ge.current = null, window.removeEventListener("mousemove", m), window.removeEventListener("mouseup", j);
    };
    window.addEventListener("mousemove", m), window.addEventListener("mouseup", j);
  }, [nr, Sr]), zr = f.useCallback((t, x) => {
    typeof lr == "function" && lr((p) => {
      const m = Ut(p, t, x), j = p && p[t] ? p[t] : null, I = m[t];
      return j && j.width === I.width && j.height === I.height ? p : m;
    });
  }, [lr]), Er = K.dropZone && K.dropZone.padding || "8px", Mr = f.useMemo(() => {
    const t = { ...K.dropZone || {} };
    return delete t.minHeight, delete t.maxHeight, delete t.overflowY, delete t.padding, delete t.border, delete t.boxShadow, t;
  }, [K.dropZone]), _r = f.useCallback((t) => {
    if (Me && Me[t]) return Me[t];
    if (He && He[t]) return He[t];
    if (!h || !h.getColumn) return [];
    if (!h.getColumn(t)) return [];
    const p = /* @__PURE__ */ new Set();
    return (h.getCoreRowModel ? h.getCoreRowModel().rows : []).forEach((j) => {
      const I = j.getValue(t);
      I != null && I !== "" && p.add(I);
    }), Array.from(p).sort((j, I) => String(j).localeCompare(String(I), void 0, { numeric: !0, sensitivity: "base" }));
  }, [He, Me, h]), ue = f.useMemo(() => ({
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
  }), [d.background, d.border, d.headerSubtleBg, d.radiusSm, d.surfaceInset, d.text]), Tr = f.useCallback((t, x) => {
    u((p) => {
      const m = [...p], j = m[t];
      if (!j) return p;
      if (Or(x) && !j.weightField) {
        const I = Array.isArray(q) && q.length > 0 ? q.find((V) => V !== j.field) || q[0] : "", Q = typeof window < "u" ? window.prompt(
          `Weight field for weighted average of ${ge(j.field)}`,
          j.weightField || I || ""
        ) : j.weightField || I || "", w = typeof Q == "string" ? Q.trim() : "";
        return w ? Array.isArray(q) && q.length > 0 && !q.includes(w) ? (R && R(`Unknown weight field: ${w}`, "warning"), p) : (m[t] = { ...j, agg: x, weightField: w }, m) : (R && R("Weighted average requires a weight field.", "warning"), p);
      }
      return m[t] = { ...j, agg: x }, m;
    });
  }, [q, u, R]), $t = f.useCallback((t, x) => {
    if (!x) {
      R && R("Weighted average requires a weight field.", "warning");
      return;
    }
    u((p) => {
      const m = [...p], j = m[t];
      return j ? (m[t] = { ...j, weightField: x }, m) : p;
    });
  }, [u, R]), Ft = f.useCallback((t, x) => {
    u((p) => {
      const m = [...p], j = m[t];
      return j ? (m[t] = {
        ...j,
        windowFn: x === "none" ? null : x
      }, m) : p;
    });
  }, [u]), Wr = f.useCallback(() => {
    const t = gn(v), x = v.filter((I) => I && I.agg !== "formula"), p = `Formula ${be + 1}`, m = mr(x), j = pn(p, v, t);
    we(t), ye(t), u((I) => [
      ...I,
      {
        field: t,
        agg: "formula",
        label: p,
        formula: m,
        formulaRef: j,
        formulaScope: "measures"
      }
    ]), R && (x.length === 0 ? R("Added a formula column. Add at least one regular value measure to reference in it.", "warning") : R(`Added ${p}.`, "success"));
  }, [be, ye, u, we, R, v]), zt = (t, x) => {
    t.stopPropagation(), xe.current && (Cr.current = xe.current.scrollTop), kr((p) => p.columnId === x ? { columnId: null, anchorEl: null } : { columnId: x, anchorEl: t.currentTarget }), Y && Y(x);
  }, Et = () => kr({ columnId: null, anchorEl: null });
  f.useEffect(() => {
    xe.current && Ye.columnId && (xe.current.scrollTop = Cr.current);
  }, [Me, Ye.columnId]), f.useEffect(() => {
    if (se.length === 0) {
      Ce !== null && we(null), ce !== null && ye(null);
      return;
    }
    se.some((x) => x.field === Ce) || we(se[se.length - 1].field), ce && !se.some((x) => x.field === ce) && ye(null);
  }, [Ce, se, ce]);
  const ur = f.useMemo(
    () => v.findIndex((t) => t && t.agg === "formula" && t.field === ce),
    [ce, v]
  ), xr = ur >= 0 ? v[ur] : null, Br = f.useCallback((t) => {
    we(t), ye(t);
  }, []), Ie = f.useCallback((t) => {
    const x = v[t] && v[t].agg === "formula" ? v[t].field : null;
    if (u((p) => p.filter((m, j) => j !== t)), x && x === Ce) {
      const p = se.find((m) => m.field !== x);
      we(p ? p.field : null);
    }
    x && x === ce && ye(null);
  }, [Ce, se, ce, u, v]), Fe = yt === "report", Ir = Math.max(nr || 288, Fe ? 380 : 0), Lr = (t, x) => {
    if (t <= 8) return null;
    const p = sr.trim().length > 0;
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
          placeholder: `Search ${t} fields...`,
          value: sr,
          onChange: (m) => wr(m.target.value),
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
      p && /* @__PURE__ */ e.jsx(
        "button",
        {
          type: "button",
          "aria-label": "Clear available field search",
          title: "Clear search",
          onClick: () => wr(""),
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
      p && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec, whiteSpace: "nowrap" }, children: [
        x,
        "/",
        t
      ] })
    ] });
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { position: "relative", display: "flex", flexShrink: 0 }, children: [
    /* @__PURE__ */ e.jsxs("div", { ref: xe, style: { ...K.sidebar, width: `${Ir}px`, minWidth: `${Ir}px` }, role: "complementary", "aria-label": "Tool Panel", onDragOver: wt, onDragLeave: Ar, onDrop: Ar, children: [
      /* @__PURE__ */ e.jsx(
        "div",
        {
          id: "pivot-sidebar-keyboard-dnd-help",
          style: { position: "absolute", width: 1, height: 1, padding: 0, margin: -1, overflow: "hidden", clip: "rect(0 0 0 0)", whiteSpace: "nowrap", border: 0 },
          children: "Press Enter or Space on a field to pick it up. Focus a target field or zone and press Enter or Space to drop it. Press Escape to cancel."
        }
      ),
      Fe ? /* @__PURE__ */ e.jsx("div", { style: { display: "flex", borderBottom: `1px solid ${d.border}`, marginBottom: "16px" }, children: [
        { id: "report", label: "Builder", icon: /* @__PURE__ */ e.jsx(E.Report, {}) },
        { id: "fields", label: "Fields", icon: /* @__PURE__ */ e.jsx(E.Sigma, {}) },
        { id: "columns", label: "Columns", icon: /* @__PURE__ */ e.jsx(E.Columns, {}) }
      ].map((t) => /* @__PURE__ */ e.jsxs(
        "div",
        {
          onClick: () => kt(t.id),
          style: {
            padding: "8px 14px",
            cursor: "pointer",
            borderBottom: Re === t.id ? `2px solid ${d.primary}` : "2px solid transparent",
            fontWeight: Re === t.id ? 600 : 400,
            color: Re === t.id ? d.primary : d.textSec,
            display: "flex",
            alignItems: "center",
            gap: "5px",
            fontSize: "13px"
          },
          children: [
            t.icon,
            t.label
          ]
        },
        t.id
      )) }) : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", borderBottom: `1px solid ${d.border}`, marginBottom: "16px" }, children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onClick: () => s("fields"),
            style: {
              padding: "8px 16px",
              cursor: "pointer",
              borderBottom: n === "fields" ? `2px solid ${d.primary}` : "none",
              fontWeight: n === "fields" ? 600 : 400,
              color: n === "fields" ? d.primary : d.textSec
            },
            children: "Fields"
          }
        ),
        /* @__PURE__ */ e.jsxs(
          "div",
          {
            onClick: () => s("filters"),
            style: {
              padding: "8px 16px",
              cursor: "pointer",
              borderBottom: n === "filters" ? `2px solid ${d.primary}` : "none",
              fontWeight: n === "filters" ? 600 : 400,
              color: n === "filters" ? d.primary : d.textSec,
              display: "flex",
              alignItems: "center",
              gap: "6px"
            },
            children: [
              "Filters",
              Object.keys(ie).length > 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "6px", height: "6px", borderRadius: "50%", background: "#d32f2f" } })
            ]
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            onClick: () => s("columns"),
            style: {
              padding: "8px 16px",
              cursor: "pointer",
              borderBottom: n === "columns" ? `2px solid ${d.primary}` : "none",
              fontWeight: n === "columns" ? 600 : 400,
              color: n === "columns" ? d.primary : d.textSec
            },
            children: "Columns"
          }
        )
      ] }),
      Fe && Re === "report" ? /* @__PURE__ */ e.jsx("div", { style: { flex: 1, overflowY: "auto", padding: "0 12px 12px" }, children: /* @__PURE__ */ e.jsx(
        cn,
        {
          reportDef: bt,
          setReportDef: ht,
          availableFields: A,
          customDimensions: _e,
          theme: d,
          styles: K,
          data: je,
          valConfigs: v,
          colFields: g,
          setColFields: c,
          setValConfigs: u,
          savedReports: mt,
          setSavedReports: vt,
          activeReportId: St,
          setActiveReportId: jt,
          showNotification: R,
          requestedSelection: xt,
          showReportConfigColumn: ft,
          setShowReportConfigColumn: gt
        }
      ) }) : Fe && Re === "fields" ? /* @__PURE__ */ e.jsxs("div", { style: { padding: "0 4px" }, children: [
        /* @__PURE__ */ e.jsx(
          Kr,
          {
            availableFields: A,
            customDimensions: _e,
            setCustomDimensions: jr,
            data: je,
            theme: d,
            showNotification: R,
            getFieldLabel: J,
            onDeleteField: Dr
          }
        ),
        /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: d.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px", padding: "0 2px" }, children: [
          /* @__PURE__ */ e.jsx(E.Database, { style: { verticalAlign: "middle", marginRight: "4px" } }),
          "Available Fields — click to add"
        ] }),
        Lr(q.length, pr.length),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", marginBottom: "14px" }, children: [
          pr.map((t) => {
            const x = v.some((p) => p && p.field === t && p.agg !== "formula");
            return /* @__PURE__ */ e.jsxs(
              "div",
              {
                onClick: () => {
                  x || u((p) => [...p, { field: t, agg: "sum", label: J(t) }]);
                },
                style: {
                  padding: "3px 9px",
                  fontSize: "11px",
                  lineHeight: 1.4,
                  fontWeight: 500,
                  borderRadius: "999px",
                  border: `1px solid ${x || d.isDark ? d.border : "#D7DDEA"}`,
                  background: x ? d.select || d.headerBg : d.isDark ? d.background : "#ffffff",
                  color: x ? d.primary : d.text,
                  cursor: x ? "default" : "pointer",
                  opacity: x ? 0.6 : 1,
                  userSelect: "none"
                },
                title: x ? `${J(t)} already in values` : `Add ${J(t)} to values`,
                children: [
                  J(t),
                  x ? " ✓" : ""
                ]
              },
              t
            );
          }),
          q.length > 8 && pr.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
        ] }),
        /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${d.border}`, paddingTop: "10px", marginBottom: "8px" }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 2px 8px 2px", flexWrap: "wrap" }, children: [
            /* @__PURE__ */ e.jsx("button", { type: "button", onClick: Wr, style: { border: `1px solid ${d.primary}66`, background: `${d.primary}12`, color: d.primary, borderRadius: "999px", cursor: "pointer", padding: "4px 10px", fontSize: "11px", fontWeight: 700 }, children: "+ Add Formula" }),
            /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
              $e,
              " value",
              $e === 1 ? "" : "s",
              " · ",
              be,
              " formula",
              be === 1 ? "" : "s"
            ] })
          ] }),
          /* @__PURE__ */ e.jsx(
            Qr,
            {
              measureAxis: F,
              setMeasureAxis: i,
              theme: d,
              valueSelectStyle: ue,
              regularValueCount: $e
            }
          )
        ] }),
        v.map((t, x) => {
          const p = t && t.agg === "formula";
          if (!t) return null;
          const m = Ct === x;
          return /* @__PURE__ */ e.jsx(
            "div",
            {
              draggable: !0,
              onDragStart: () => {
                Ze.current = x;
              },
              onDragOver: (j) => {
                j.preventDefault(), Ke(x);
              },
              onDragLeave: () => Ke(null),
              onDrop: (j) => {
                j.preventDefault();
                const I = Ze.current;
                Ke(null), Ze.current = null, !(I === null || I === x) && u((Q) => {
                  const w = [...Q], [V] = w.splice(I, 1);
                  return w.splice(x, 0, V), w;
                });
              },
              onDragEnd: () => {
                Ze.current = null, Ke(null);
              },
              style: { marginBottom: "6px", borderTop: m ? `2px solid ${d.primary}` : "2px solid transparent", cursor: "grab" },
              children: p ? /* @__PURE__ */ e.jsx(Jr, { item: t, idx: x, selected: t.field === ce, onSelect: Br, onRemove: Ie, setValConfigs: u, theme: d, styles: K, dropLine: null, zoneId: "vals", valueSelectStyle: ue, colFields: g, fixedSparklineValueKeySet: dr, showNotification: R, serverError: ke && ke[t.field] || null }) : /* @__PURE__ */ e.jsxs("div", { style: { ...K.chip, flexWrap: "wrap", alignItems: "flex-start" }, children: [
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                  /* @__PURE__ */ e.jsx(E.DragIndicator, { style: { flexShrink: 0, opacity: 0.4 } }),
                  /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: J(t.field) })
                ] }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                  /* @__PURE__ */ e.jsxs("select", { value: t.agg, onChange: (j) => Tr(x, j.target.value), style: { ...ue, width: "74px" }, children: [
                    /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                    /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Avg" }),
                    /* @__PURE__ */ e.jsx("option", { value: "count", children: "Cnt" }),
                    /* @__PURE__ */ e.jsx("option", { value: "min", children: "Min" }),
                    /* @__PURE__ */ e.jsx("option", { value: "max", children: "Max" }),
                    /* @__PURE__ */ e.jsx("option", { value: "weighted_avg", children: "WAvg" })
                  ] }),
                  /* @__PURE__ */ e.jsx("span", { onClick: () => Ie(x), style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(E.Close, {}) })
                ] })
              ] })
            },
            t.field || x
          );
        }),
        v.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { opacity: 0.5, fontSize: "11px", padding: "12px", textAlign: "center" }, children: "No value fields. Click a field above or add a formula." })
      ] }) : !Fe && n === "filters" ? /* @__PURE__ */ e.jsxs("div", { style: { flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", padding: "8px" }, children: [
        /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "10px", display: "flex", alignItems: "center", background: d.background, borderRadius: "6px", padding: "4px 8px", border: `1px solid ${d.border}` }, children: [
          /* @__PURE__ */ e.jsx(E.Search, {}),
          /* @__PURE__ */ e.jsx(
            "input",
            {
              placeholder: "Search columns...",
              value: C,
              onChange: (t) => P(t.target.value),
              style: { border: "none", background: "transparent", marginLeft: "10px", outline: "none", width: "100%", color: d.text, fontSize: "13px" }
            }
          )
        ] }),
        (() => {
          const p = A.map((m) => {
            const j = h.getColumn(m);
            return j || { id: m, header: J(m), columnDef: { header: J(m) } };
          }).filter((m) => {
            const j = m.columnDef && typeof m.columnDef.header == "string" ? m.columnDef.header : typeof m.header == "string" ? m.header : m.id;
            return String(j).toLowerCase().includes(C.toLowerCase()) || m.id.toLowerCase().includes(C.toLowerCase());
          });
          return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: p.map((m) => /* @__PURE__ */ e.jsx(
            Xt,
            {
              column: m,
              theme: d,
              styles: K,
              onFilter: (j) => U(m.id, j),
              currentFilter: ie[m.id],
              options: _r(m.id),
              optionMeta: Oe && Oe[m.id],
              onSearchOptions: Ve ? (j) => Y && Y(m.id, { search: j, offset: 0 }) : void 0,
              onLoadMoreOptions: Ve ? (j, I) => Y && Y(m.id, { search: j, offset: I }) : void 0,
              onOpen: Y
            },
            m.id
          )) });
        })()
      ] }) : (Fe ? Re !== "columns" : n !== "columns") ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
        n === "fields" && /* @__PURE__ */ e.jsxs("div", { children: [
          /* @__PURE__ */ e.jsx(
            Kr,
            {
              availableFields: A,
              customDimensions: _e,
              setCustomDimensions: jr,
              data: je,
              theme: d,
              showNotification: R,
              getFieldLabel: J,
              onDeleteField: Dr
            }
          ),
          /* @__PURE__ */ e.jsxs("div", { style: K.sectionTitleSm, children: [
            /* @__PURE__ */ e.jsx(E.Database, {}),
            " Available Fields"
          ] }),
          Lr(Ae.length, cr.length),
          /* @__PURE__ */ e.jsxs(
            qr,
            {
              panelId: "availableFields",
              panelLabel: "Available Fields",
              size: Ue && Ue.availableFields,
              onSizeChange: zr,
              theme: d,
              outerStyle: Mr,
              contentStyle: {
                display: "flex",
                flexWrap: "wrap",
                gap: "4px",
                alignContent: "flex-start",
                padding: "2px 12px 12px 2px"
              },
              children: [
                cr.map((t) => {
                  const x = Te && t === fe, p = x ? "Measure Names" : J(t), m = x ? "measureAxis" : "pool";
                  return /* @__PURE__ */ e.jsxs(
                    "div",
                    {
                      draggable: !0,
                      "data-sidebar-field-chip": x ? "measure-axis-label" : "available",
                      role: "button",
                      tabIndex: 0,
                      "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                      "aria-grabbed": N && N.zone === m && N.field === t ? "true" : "false",
                      "aria-label": `Move ${p}`,
                      onDragStart: (j) => H(j, t, m),
                      onDragEnd: z,
                      onKeyDown: (j) => Fr(j, t, m, -1, p),
                      title: x ? "Drag to Rows or Columns to show measure names" : `Move ${J(t)}`,
                      style: {
                        padding: "4px 8px",
                        fontSize: "11px",
                        lineHeight: 1.2,
                        fontWeight: x ? 700 : 500,
                        fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif",
                        borderRadius: d.radiusSm || d.radius || "10px",
                        border: x ? `1px dashed ${d.primary}` : `1px solid ${d.isDark ? d.border : "#D7DDEA"}`,
                        background: x ? `${d.primary}12` : d.isDark ? d.background : d.surfaceInset || "#ffffff",
                        color: x ? d.primary : d.isDark ? d.text : "#334155",
                        display: "inline-flex",
                        alignItems: "center",
                        gap: "4px",
                        boxShadow: d.isDark ? "none" : d.shadowInset || "0 1px 2px rgba(0,0,0,0.04)",
                        cursor: "grab",
                        userSelect: "none",
                        minHeight: "24px"
                      },
                      children: [
                        x && /* @__PURE__ */ e.jsx(E.Sigma, {}),
                        p
                      ]
                    },
                    x ? `__measure_axis__${t}` : t
                  );
                }),
                Ae.length > 8 && cr.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
              ]
            }
          )
        ] }),
        Rt.map((t) => {
          const x = Be[t.id] || [];
          return /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "20px" }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: K.sectionTitle, children: [
              t.icon,
              t.label
            ] }),
            t.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 4px 8px 4px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: Wr,
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
                $e,
                " value measure",
                $e === 1 ? "" : "s",
                " available"
              ] }),
              be > 0 && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
                be,
                " formula",
                be === 1 ? "" : "s"
              ] }),
              /* @__PURE__ */ e.jsx(
                Qr,
                {
                  measureAxis: F,
                  setMeasureAxis: i,
                  theme: d,
                  valueSelectStyle: ue,
                  regularValueCount: $e
                }
              )
            ] }),
            t.id === "rows" && o.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", padding: "0 4px 6px 4px" }, children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  title: "Expand all hierarchy levels",
                  onClick: () => re(!0),
                  style: { flex: 1, border: `1px solid ${d.primary}`, background: d.select, cursor: "pointer", padding: "3px 6px", fontSize: "11px", color: d.primary, borderRadius: "4px", fontWeight: 600 },
                  children: "+ Expand All"
                }
              ),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  title: "Collapse all rows",
                  onClick: () => re(!1),
                  style: { flex: 1, border: `1px solid ${d.border}`, background: d.background, cursor: "pointer", padding: "3px 6px", fontSize: "11px", color: d.textSec, borderRadius: "4px" },
                  children: "- Collapse All"
                }
              )
            ] }),
            /* @__PURE__ */ e.jsx(
              qr,
              {
                panelId: t.id,
                panelLabel: t.label,
                size: Ue && Ue[t.id],
                onSizeChange: zr,
                theme: d,
                outerStyle: Mr,
                contentStyle: { padding: `${Er} 12px 12px ${Er}` },
                children: /* @__PURE__ */ e.jsxs(
                  "div",
                  {
                    "data-sidebar-drop-zone": t.id,
                    role: "group",
                    tabIndex: 0,
                    "aria-label": `${t.label} drop zone`,
                    "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                    onKeyDown: (p) => Dt(p, t.id),
                    onDragOver: (p) => {
                      p.target === p.currentTarget ? G(p, t.id, x.length) : (p.preventDefault(), p.dataTransfer && (p.dataTransfer.dropEffect = "move"));
                    },
                    onDrop: (p) => b(p, t.id),
                    children: [
                      x.map((p, m) => {
                        const j = t.id === "vals" ? p.field : p, I = t.id === "vals" ? p.agg === "formula" ? p.label || "Formula" : J(p.field) : J(p), Q = (t.id === "rows" || t.id === "cols") && j === fe && Rr.placement !== "none";
                        return /* @__PURE__ */ e.jsxs(
                          "div",
                          {
                            draggable: !0,
                            "data-sidebar-field-chip": "zone",
                            role: "button",
                            tabIndex: 0,
                            "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                            "aria-grabbed": N && N.zone === t.id && N.idx === m ? "true" : "false",
                            "aria-label": `Move ${I}`,
                            onKeyDown: (w) => Fr(w, p, t.id, m, I),
                            onDragStart: (w) => H(w, p, t.id, m),
                            onDragOver: (w) => G(w, t.id, m),
                            onDragEnd: z,
                            children: [
                              t.id === "vals" && p.agg === "formula" ? /* @__PURE__ */ e.jsx(
                                Jr,
                                {
                                  item: p,
                                  idx: m,
                                  selected: p.field === Ce,
                                  onSelect: Br,
                                  onRemove: Ie,
                                  setValConfigs: u,
                                  theme: d,
                                  styles: K,
                                  dropLine: M,
                                  zoneId: t.id,
                                  valueSelectStyle: ue,
                                  colFields: g,
                                  fixedSparklineValueKeySet: dr,
                                  showNotification: R,
                                  serverError: ke && ke[p.field] || null
                                }
                              ) : /* @__PURE__ */ e.jsxs("div", { style: {
                                ...K.chip,
                                ...t.id === "vals" ? { flexWrap: "wrap", alignItems: "flex-start" } : {}
                              }, children: [
                                M && M.zone === t.id && M.idx === m && /* @__PURE__ */ e.jsx("div", { style: { ...K.dropLine, top: -2 } }),
                                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                                  /* @__PURE__ */ e.jsx(E.DragIndicator, {}),
                                  /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, display: "block", minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: I })
                                ] }),
                                t.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                                  /* @__PURE__ */ e.jsxs("select", { value: p.agg, onChange: (w) => Tr(m, w.target.value), style: { ...ue, width: "74px" }, title: "Aggregation", children: [
                                    /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Avg" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "count", children: "Cnt" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "min", children: "Min" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "max", children: "Max" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "weighted_avg", children: "WAvg" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "array_agg", children: "Series" })
                                  ] }),
                                  Or(p.agg) && /* @__PURE__ */ e.jsxs("select", { value: p.weightField || "", onChange: (w) => $t(m, w.target.value), style: { ...ue, width: "92px" }, title: "Weight field", children: [
                                    /* @__PURE__ */ e.jsx("option", { value: "", disabled: !0, children: "Weight" }),
                                    q.map((w) => /* @__PURE__ */ e.jsx("option", { value: w, children: J(w) }, w))
                                  ] }),
                                  /* @__PURE__ */ e.jsxs("select", { value: p.windowFn || "none", onChange: (w) => Ft(m, w.target.value), style: { ...ue, width: "56px" }, children: [
                                    /* @__PURE__ */ e.jsx("option", { value: "none", children: "Norm" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "percent_of_row", children: "%Row" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "percent_of_col", children: "%Col" }),
                                    /* @__PURE__ */ e.jsx("option", { value: "percent_of_grand_total", children: "%Tot" })
                                  ] })
                                ] }),
                                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", marginLeft: "auto", alignItems: "center" }, children: [
                                  t.id === "filter" && /* @__PURE__ */ e.jsx("div", { onClick: (w) => zt(w, j), style: { cursor: "pointer", display: "flex", alignItems: "center", padding: "2px", borderRadius: "4px" }, children: /* @__PURE__ */ e.jsx(E.Filter, {}) }),
                                  /* @__PURE__ */ e.jsx("span", { onClick: () => {
                                    if (Q && typeof i == "function") {
                                      i((w) => ({
                                        ...Pe(w),
                                        placement: "none"
                                      }));
                                      return;
                                    }
                                    if (t.id === "filter") {
                                      const w = { ...ie };
                                      delete w[j], ar(w);
                                    }
                                    t.id === "rows" && l((w) => w.filter((V) => V !== j)), t.id === "cols" && c((w) => w.filter((V) => V !== j)), t.id === "vals" && Ie(m);
                                  }, style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(E.Close, {}) })
                                ] }),
                                t.id === "filter" && Ye.columnId === j && /* @__PURE__ */ e.jsx(
                                  Vt,
                                  {
                                    column: { header: I, id: j },
                                    anchorEl: Ye.anchorEl,
                                    onClose: Et,
                                    onFilter: (w) => U(j, w),
                                    currentFilter: ie[j],
                                    options: _r(j),
                                    optionMeta: Oe && Oe[j],
                                    onSearchOptions: Ve ? (w) => Y && Y(j, { search: w, offset: 0 }) : void 0,
                                    onLoadMoreOptions: Ve ? (w, V) => Y && Y(j, { search: w, offset: V }) : void 0,
                                    theme: d
                                  }
                                ),
                                t.id === "vals" && /* @__PURE__ */ e.jsx(
                                  pt,
                                  {
                                    item: p,
                                    idx: m,
                                    setValConfigs: u,
                                    theme: d,
                                    valueSelectStyle: ue,
                                    colFields: g,
                                    fixedSparklineValueKeySet: dr,
                                    showNotification: R
                                  }
                                ),
                                M && M.zone === t.id && M.idx === m + 1 && /* @__PURE__ */ e.jsx("div", { style: { ...K.dropLine, bottom: -2 } })
                              ] }),
                              t.id === "filter" && ie[j] && ie[j].conditions && /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: d.primary, padding: "0 8px 4px 8px", marginTop: "-4px" }, children: ie[j].conditions.map((w) => `${w.type}: ${w.value}${w.caseSensitive ? " (Match Case)" : ""}`).join(` ${ie[j].operator} `) })
                            ]
                          },
                          m
                        );
                      }),
                      x.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { opacity: 0.5, fontSize: "11px", padding: "8px", textAlign: "center", pointerEvents: "none" }, children: "Drag fields here" }),
                      /* @__PURE__ */ e.jsx("div", { style: { height: 20 }, onDragOver: (p) => G(p, t.id, x.length), onDrop: (p) => b(p, t.id) })
                    ]
                  }
                )
              }
            )
          ] }, t.id);
        })
      ] }) : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "16px", height: "100%", overflow: "hidden" }, children: [
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px", padding: "8px", background: d.headerBg, borderRadius: "8px", boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", background: d.background, borderRadius: "6px", padding: "8px 12px", border: `2px solid ${d.border}`, transition: "border-color 0.2s" }, children: [
            /* @__PURE__ */ e.jsx(E.Search, {}),
            /* @__PURE__ */ e.jsx(
              "input",
              {
                placeholder: "Search columns...",
                value: C,
                onChange: (t) => P(t.target.value),
                style: {
                  border: "none",
                  background: "transparent",
                  marginLeft: "10px",
                  outline: "none",
                  width: "100%",
                  color: d.text,
                  fontSize: "13px",
                  fontWeight: 500
                }
              }
            ),
            C && /* @__PURE__ */ e.jsx(
              "span",
              {
                onClick: () => P(""),
                style: {
                  cursor: "pointer",
                  display: "flex",
                  padding: "4px",
                  borderRadius: "4px",
                  background: d.hover,
                  transition: "background 0.2s"
                },
                onMouseEnter: (t) => t.currentTarget.style.background = d.select,
                onMouseLeave: (t) => t.currentTarget.style.background = d.hover,
                children: /* @__PURE__ */ e.jsx(E.Close, {})
              }
            )
          ] }),
          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
            { value: "all", label: "All", icon: "📊" },
            { value: "number", label: "Numbers", icon: "🔢" },
            { value: "string", label: "Text", icon: "📝" },
            { value: "date", label: "Dates", icon: "📅" }
          ].map((t) => /* @__PURE__ */ e.jsxs(
            "button",
            {
              onClick: () => $(t.value),
              style: {
                padding: "6px 12px",
                borderRadius: "6px",
                border: "none",
                background: a === t.value ? d.primary : d.background,
                color: a === t.value ? "#fff" : d.text,
                cursor: "pointer",
                fontSize: "11px",
                fontWeight: 600,
                display: "flex",
                alignItems: "center",
                gap: "4px",
                transition: "all 0.2s",
                boxShadow: a === t.value ? "0 2px 4px rgba(0,0,0,0.1)" : "none"
              },
              onMouseEnter: (x) => {
                a !== t.value && (x.currentTarget.style.background = d.hover);
              },
              onMouseLeave: (x) => {
                a !== t.value && (x.currentTarget.style.background = d.background);
              },
              children: [
                /* @__PURE__ */ e.jsx("span", { children: t.icon }),
                /* @__PURE__ */ e.jsx("span", { children: t.label })
              ]
            },
            t.value
          )) })
        ] }),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", padding: "0 8px" }, children: [
          /* @__PURE__ */ e.jsxs(
            "button",
            {
              onClick: () => h.toggleAllColumnsVisible(!0),
              style: {
                ...K.btn,
                padding: "8px 12px",
                fontSize: "11px",
                flex: 1,
                justifyContent: "center",
                background: d.background,
                fontWeight: 600,
                boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                transition: "all 0.2s"
              },
              onMouseEnter: (t) => t.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
              onMouseLeave: (t) => t.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
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
                ...K.btn,
                padding: "8px 12px",
                fontSize: "11px",
                flex: 1,
                justifyContent: "center",
                background: d.background,
                fontWeight: 600,
                boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                transition: "all 0.2s"
              },
              onMouseEnter: (t) => t.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
              onMouseLeave: (t) => t.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
              children: [
                /* @__PURE__ */ e.jsx(E.VisibilityOff, { style: { fontSize: "14px" } }),
                /* @__PURE__ */ e.jsx("span", { children: "Hide All" })
              ]
            }
          ),
          /* @__PURE__ */ e.jsxs(
            "button",
            {
              onClick: () => te(!1),
              style: {
                ...K.btn,
                padding: "8px 12px",
                fontSize: "11px",
                flex: 1,
                justifyContent: "center",
                background: d.background,
                fontWeight: 600,
                boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                transition: "all 0.2s"
              },
              onMouseEnter: (t) => t.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
              onMouseLeave: (t) => t.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
              children: [
                /* @__PURE__ */ e.jsx(E.Unpin, { style: { fontSize: "14px" } }),
                /* @__PURE__ */ e.jsx("span", { children: "Unpin All" })
              ]
            }
          )
        ] }),
        D.size > 0 && /* @__PURE__ */ e.jsxs("div", { style: {
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
              D.size,
              " Column",
              D.size > 1 ? "s" : "",
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
                onMouseEnter: (t) => t.currentTarget.style.background = d.hover,
                onMouseLeave: (t) => t.currentTarget.style.background = d.background,
                children: /* @__PURE__ */ e.jsx(E.Close, {})
              }
            )
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                onClick: () => {
                  Array.from(D).forEach((t) => Z(t, "left")), B(/* @__PURE__ */ new Set());
                },
                style: {
                  ...K.btn,
                  padding: "6px 12px",
                  fontSize: "11px",
                  background: d.background,
                  flex: 1,
                  justifyContent: "center",
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                  transition: "all 0.2s"
                },
                onMouseEnter: (t) => t.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                onMouseLeave: (t) => t.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
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
                  Array.from(D).forEach((t) => Z(t, !1)), B(/* @__PURE__ */ new Set());
                },
                style: {
                  ...K.btn,
                  padding: "6px 12px",
                  fontSize: "11px",
                  background: d.background,
                  flex: 1,
                  justifyContent: "center",
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                  transition: "all 0.2s"
                },
                onMouseEnter: (t) => t.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                onMouseLeave: (t) => t.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
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
                  Array.from(D).forEach((t) => Z(t, "right")), B(/* @__PURE__ */ new Set());
                },
                style: {
                  ...K.btn,
                  padding: "6px 12px",
                  fontSize: "11px",
                  background: d.background,
                  flex: 1,
                  justifyContent: "center",
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
                  transition: "all 0.2s"
                },
                onMouseEnter: (t) => t.currentTarget.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
                onMouseLeave: (t) => t.currentTarget.style.boxShadow = "0 1px 2px rgba(0,0,0,0.05)",
                children: [
                  /* @__PURE__ */ e.jsx(E.PinRight, {}),
                  /* @__PURE__ */ e.jsx("span", { children: "Pin Right" })
                ]
              }
            )
          ] })
        ] }),
        /* @__PURE__ */ e.jsx("div", { style: { flex: 1, overflowY: "auto", display: "flex", flexDirection: "column" }, children: (() => {
          const x = h.getAllColumns().filter((w) => !w.parent && w.id !== "no_data").filter((w) => {
            if (a === "all") return !0;
            let V = "string";
            if (v.find((ne) => w.id.includes(ne.field)))
              V = "number";
            else if (je && je.length > 0) {
              const ne = je[0];
              let le = ne[w.id];
              if (le === void 0 && w.accessorFn)
                try {
                  le = w.accessorFn(ne);
                } catch {
                }
              typeof le == "number" ? V = "number" : le instanceof Date ? V = "date" : typeof le == "string" && (!isNaN(Number(le)) && le.trim() !== "" ? V = "number" : !isNaN(Date.parse(le)) && le.includes("-") && (V = "date"));
            }
            return V === a;
          }), p = x.filter((w) => qe(w, "left")), m = x.filter((w) => qe(w, "right")), j = x.filter((w) => qe(w, "unpinned")), I = (w, V) => w.map((de) => /* @__PURE__ */ e.jsx(
            rt,
            {
              column: de,
              level: 0,
              theme: d,
              styles: K,
              handlePinColumn: Z,
              colSearch: C,
              selectedCols: D,
              setSelectedCols: B,
              onDrop: Q,
              sectionId: V
            },
            de.id
          )), Q = (w, V, de) => {
            let ne;
            const le = L || { left: [], right: [] };
            if (de) {
              const Le = V === "left" ? le.left : V === "right" ? le.right : null;
              if (Le) {
                if (ne = Le.indexOf(de), ne === -1) {
                  const fr = h.getColumn(de);
                  if (fr && Gt(fr)) {
                    const Pr = yr(fr).find((Mt) => Le.includes(Mt));
                    Pr && (ne = Le.indexOf(Pr));
                  }
                }
                ne === -1 && (ne = void 0);
              }
            }
            V === "left" ? Z(w, "left", ne) : V === "right" ? Z(w, "right", ne) : Z(w, !1);
          };
          return /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
            p.length > 0 && /* @__PURE__ */ e.jsx(
              gr,
              {
                title: "Pinned Left",
                count: p.length,
                theme: d,
                styles: K,
                sectionId: "left",
                onDrop: Q,
                children: I(p, "left")
              }
            ),
            m.length > 0 && /* @__PURE__ */ e.jsx(
              gr,
              {
                title: "Pinned Right",
                count: m.length,
                theme: d,
                styles: K,
                sectionId: "right",
                onDrop: Q,
                children: I(m, "right")
              }
            ),
            /* @__PURE__ */ e.jsx(
              gr,
              {
                title: "Columns",
                count: j.length,
                theme: d,
                styles: K,
                sectionId: "unpinned",
                onDrop: Q,
                children: I(j, "unpinned")
              }
            )
          ] });
        })() }),
        k && k.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { padding: "8px", borderTop: `1px solid ${d.border}` }, children: [
          /* @__PURE__ */ e.jsx("div", { style: K.sectionTitle, children: "Pinning Presets" }),
          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px", flexWrap: "wrap" }, children: k.map((t, x) => /* @__PURE__ */ e.jsx(
            "button",
            {
              onClick: () => {
                _(t.config), R(`Applied preset: ${t.name}`);
              },
              style: { ...K.btn, fontSize: "11px", background: d.headerBg },
              children: t.name
            },
            x
          )) })
        ] }),
        /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "11px", color: d.textSec, fontStyle: "italic", padding: "8px", background: d.headerBg, borderTop: `1px solid ${d.border}44` }, children: [
          /* @__PURE__ */ e.jsx(E.Lock, { style: { verticalAlign: "middle", marginRight: "4px", opacity: 0.5 } }),
          "Drag columns or use pin icons to freeze areas."
        ] })
      ] })
    ] }),
    xr && /* @__PURE__ */ e.jsx(
      jn,
      {
        item: xr,
        idx: ur,
        valConfigs: v,
        setValConfigs: u,
        theme: d,
        onClose: () => ye(null),
        onRemove: Ie,
        displayedColumnOptions: ut,
        availableRawFields: q,
        serverError: ke && ke[xr.field] || null
      }
    ),
    /* @__PURE__ */ e.jsx(
      "div",
      {
        onMouseDown: At,
        style: {
          position: "absolute",
          right: 0,
          top: 0,
          bottom: 0,
          width: "5px",
          cursor: "col-resize",
          zIndex: 10,
          background: "transparent"
        },
        title: "Drag to resize panel",
        children: /* @__PURE__ */ e.jsx("div", { style: {
          position: "absolute",
          right: 0,
          top: 0,
          bottom: 0,
          width: "2px",
          background: d.border,
          transition: "background 0.15s"
        } })
      }
    )
  ] });
});
export {
  $n as SidebarPanel
};
