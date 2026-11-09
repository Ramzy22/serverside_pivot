import { ab as bl, am as gl, j as l, z as j, an as vl, ad as jl, _ as T, ao as Sl } from "./index-B7dueOw0.js";
import { a as x } from "./charts-DthW1Q8W.js";
const re = () => `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`, Ke = [
  { value: "eq", label: "Equals" },
  { value: "not_eq", label: "Not equals" },
  { value: "in", label: "In list" },
  { value: "not_in", label: "Not in list" },
  { value: "contains", label: "Contains" },
  { value: "not_contains", label: "Does not contain" },
  { value: "gt", label: "Greater than" },
  { value: "gte", label: "Greater or equal" },
  { value: "lt", label: "Less than" },
  { value: "lte", label: "Less or equal" }
], ae = /* @__PURE__ */ new Set(["in", "not_in"]), Cl = {
  eq: "=",
  not_eq: "!=",
  in: "in",
  not_in: "not in",
  contains: "contains",
  not_contains: "not contains",
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<="
}, wl = /* @__PURE__ */ new Set([
  "depth",
  "_id",
  "_path",
  "_levelLabel",
  "_levelField",
  "_pathFields",
  "_has_children",
  "_is_expanded",
  "__col_schema"
]), W = "default", ke = "branch:", Ge = ["", "solid", "dashed", "dotted", "double"], Ye = (e = "") => ({ _id: re(), field: e, operator: "eq", value: "", values: [] }), De = () => ({ op: "AND", clauses: [] }), Al = () => ({ _id: re(), label: "", condition: De(), child: null }), ge = (e = {}) => {
  const t = e && typeof e == "object" ? e : {}, n = Number(t.indent), i = Number(t.borderWidth), r = typeof t.borderStyle == "string" && Ge.includes(t.borderStyle) ? t.borderStyle : "";
  return {
    indent: Number.isFinite(n) && n >= 0 ? Math.floor(n) : "",
    bold: t.bold === !0,
    showSubtotal: t.showSubtotal !== !1,
    rowColor: typeof t.rowColor == "string" ? t.rowColor : "",
    labelPrefix: typeof t.labelPrefix == "string" ? t.labelPrefix : "",
    labelSuffix: typeof t.labelSuffix == "string" ? t.labelSuffix : "",
    numberFormat: typeof t.numberFormat == "string" ? t.numberFormat : "",
    borderStyle: r,
    borderColor: typeof t.borderColor == "string" ? t.borderColor : "",
    borderWidth: Number.isFinite(i) && i > 0 ? Math.floor(i) : ""
  };
}, K = (e = "") => ({
  _id: re(),
  field: e,
  label: "",
  topN: null,
  sortBy: null,
  sortDir: "desc",
  sortAbs: !1,
  filters: null,
  format: ge(),
  defaultChild: null,
  branches: []
}), kl = (e) => !e || typeof e.field != "string" || !e.field ? null : {
  _id: typeof e._id == "string" ? e._id : re(),
  field: e.field,
  operator: Ke.some((t) => t.value === e.operator) ? e.operator : "eq",
  value: e.value !== void 0 && e.value !== null ? String(e.value) : "",
  values: Array.isArray(e.values) ? e.values.map(String).filter(Boolean) : []
}, Re = (e) => ({
  op: e && e.op === "OR" ? "OR" : "AND",
  clauses: Array.isArray(e && e.clauses) ? e.clauses.map(kl).filter(Boolean) : []
}), me = (e) => !e || typeof e != "object" ? null : {
  _id: typeof e._id == "string" ? e._id : re(),
  label: typeof e.label == "string" ? e.label : "",
  condition: Re(e.condition),
  sourceRowPath: Array.isArray(e.sourceRowPath) ? e.sourceRowPath.map(String).filter(Boolean) : [],
  sourcePathFields: Array.isArray(e.sourcePathFields) ? e.sourcePathFields.map(String).filter(Boolean) : [],
  child: e.child ? M(e.child) : null
};
function M(e) {
  if (!e || typeof e != "object") return null;
  let t = Array.isArray(e.branches) ? e.branches.map(me).filter(Boolean) : [];
  t.length === 0 && e.childrenByValue && typeof e.childrenByValue == "object" && (t = Object.entries(e.childrenByValue).filter(([i]) => i).map(([i, r]) => me({
    label: i,
    condition: { op: "AND", clauses: [{ field: e.field || "", operator: "eq", value: i, values: [] }] },
    child: r
  })).filter(Boolean));
  const n = e.filters && typeof e.filters == "object" ? Re(e.filters) : null;
  return {
    _id: typeof e._id == "string" ? e._id : re(),
    field: typeof e.field == "string" ? e.field : "",
    label: typeof e.label == "string" ? e.label : "",
    topN: Number.isFinite(Number(e.topN)) && Number(e.topN) > 0 ? Math.floor(Number(e.topN)) : null,
    sortBy: typeof e.sortBy == "string" && e.sortBy.trim() ? e.sortBy.trim() : null,
    sortDir: e.sortDir === "asc" ? "asc" : "desc",
    sortAbs: e.sortAbs === !0,
    filters: n && n.clauses.length > 0 ? n : null,
    format: ge(e.format),
    defaultChild: e.defaultChild ? M(e.defaultChild) : null,
    branches: t
  };
}
const Ne = (e) => `${ke}${e}`, je = (e) => {
  if (typeof e != "string" || !e.startsWith(ke)) return -1;
  const t = parseInt(e.slice(ke.length), 10);
  return Number.isFinite(t) ? t : -1;
}, ml = (e, t) => {
  const n = new Set(
    (t || []).filter((i) => i && i.field && i.agg).map((i) => i.agg === "formula" ? i.field : `${i.field}_${i.agg}`)
  );
  return (e || []).filter((i) => i && typeof i == "string" && !wl.has(i) && (!i.startsWith("_") || jl(i)) && !n.has(i));
}, $l = (e) => {
  const t = [
    { value: "", label: "(default)" },
    { value: "__field__", label: "Field value (A→Z / Z→A)" }
  ];
  return (e || []).forEach((n) => {
    if (!n || !n.field || !n.agg) return;
    const i = n.agg === "formula" ? n.field : `${n.field}_${n.agg}`, r = n.agg === "formula" ? n.label || T(n.field) : `${T(n.field)} (${Sl(n.agg, n.weightField)})`;
    t.push({ value: i, label: r });
  }), t;
}, Se = (e) => !e || !e.clauses || e.clauses.length === 0 ? "No filter" : e.clauses.map((t) => {
  const n = T(t.field), i = Cl[t.operator] || t.operator;
  if (ae.has(t.operator)) {
    const r = t.values.slice(0, 3), o = t.values.length > 3 ? ` +${t.values.length - 3}` : "";
    return `${n} ${i} [${r.join(", ")}${o}]`;
  }
  return `${n} ${i} ${t.value || "..."}`;
}).join(` ${e.op} `), Ze = (e, t) => {
  if (!e) return "";
  const n = (t || []).find((i) => i.value === e);
  return n ? n.label : T(e);
}, Xe = (e) => e ? e.label && e.label.trim() ? e.label.trim() : e.field ? T(e.field) : "Select field" : "Select field", Fl = (e) => !e || !e.field ? !1 : ae.has(e.operator) ? Array.isArray(e.values) && e.values.length > 0 : e.value !== void 0 && e.value !== null && String(e.value).trim() !== "", $e = (e) => !!(e && Array.isArray(e.clauses) && e.clauses.length > 0) && e.clauses.every(Fl), He = (e, t) => {
  if (!e || !t || typeof t != "object") return !1;
  const n = t[e.field], i = n == null ? "" : String(n), r = e.value === void 0 || e.value === null ? "" : String(e.value), o = Array.isArray(e.values) ? e.values.map(String) : [], a = Number(i), f = Number(r);
  switch (e.operator) {
    case "not_eq":
      return i !== r;
    case "in":
      return o.includes(i);
    case "not_in":
      return !o.includes(i);
    case "contains":
      return i.toLowerCase().includes(r.toLowerCase());
    case "not_contains":
      return !i.toLowerCase().includes(r.toLowerCase());
    case "gt":
      return Number.isFinite(a) && Number.isFinite(f) && a > f;
    case "gte":
      return Number.isFinite(a) && Number.isFinite(f) && a >= f;
    case "lt":
      return Number.isFinite(a) && Number.isFinite(f) && a < f;
    case "lte":
      return Number.isFinite(a) && Number.isFinite(f) && a <= f;
    case "eq":
    default:
      return i === r;
  }
}, Ve = (e, t) => {
  if (!$e(e) || !Array.isArray(t) || t.length === 0) return !0;
  const n = t.filter((r) => r && typeof r == "object" && !r._isTotal && !r._isOther);
  return n.some((r) => e.clauses.every((o) => Object.prototype.hasOwnProperty.call(r, o.field))) ? n.some((r) => e.op === "OR" ? e.clauses.some((o) => He(o, r)) : e.clauses.every((o) => He(o, r))) : !0;
}, Bl = (e, t) => {
  if (!Array.isArray(e) || e.length === 0 || !Array.isArray(t) || t.length === 0) return !0;
  const n = e.map(String).join("|||");
  return t.some((i) => i && typeof i == "object" && i._path === n);
}, qe = (e, {
  allFields: t = [],
  valConfigs: n = [],
  data: i = []
} = {}) => {
  const r = [], o = [], a = (n || []).some((h) => h && h.field && h.agg), f = new Set(Array.isArray(t) ? t : []), S = /* @__PURE__ */ new Map(), b = Array.isArray(i) ? i : [], B = (h, k) => {
    const d = h.trim().toLowerCase();
    if (d) {
      if (S.has(d)) {
        r.push(`Duplicate header "${h}" at ${k}; already used at ${S.get(d)}.`);
        return;
      }
      S.set(d, k);
    }
  }, N = (h, k, d = /* @__PURE__ */ new Set()) => {
    if (!h) return;
    const C = h._id || `${k}:${h.field || ""}`;
    if (d.has(C)) {
      r.push(`Circular row override detected at ${k}.`);
      return;
    }
    const v = new Set(d);
    v.add(C), h.field ? f.size > 0 && !f.has(h.field) && r.push(`Field "${T(h.field)}" at ${k} is no longer available.`) : r.push(`Missing field at ${k}.`), B(Xe(h), k), h.topN && !a && r.push(`Top-N at ${k} needs at least one measure selected.`), h.filters && !$e(h.filters) ? r.push(`Level filter at ${k} has incomplete rules.`) : h.filters && !Ve(h.filters, b) && r.push(`Level filter at ${k} does not match any loaded rows.`), h.defaultChild && N(h.defaultChild, `${k} > Default children`, v), (h.branches || []).forEach((u, D) => {
      const R = u.label && u.label.trim() ? u.label.trim() : `Row override ${D + 1}`, y = `${k} > ${R}`;
      $e(u.condition) ? Ve(u.condition, b) || r.push(`Row override "${R}" does not match any loaded rows.`) : r.push(`Row override "${R}" needs a complete row rule.`), Array.isArray(u.sourcePathFields) && u.sourcePathFields.length > 0 && u.sourcePathFields[u.sourcePathFields.length - 1] !== h.field && r.push(`Row override "${R}" was created for another level and no longer points to this row path.`), Bl(u.sourceRowPath, b) || o.push(`Row override "${R}" points to a row path that is not in the current loaded rows.`), u.child && N(u.child, `${y} > Custom children`, v), u.child || o.push(`Row override "${R}" has no Custom children; it only renames the matching row.`);
    });
  };
  return e ? N(e, "Level 1") : r.push("Report needs at least Level 1."), { errors: r, warnings: o, valid: r.length === 0 };
}, Dl = (e) => !e || !e.branchBadge ? `Level ${e ? e.levelNumber : 1}` : e.branchDepth === 0 ? `Override children ${e.branchBadge}` : `Override child level ${e.branchBadge}.${e.branchDepth + 1}`, te = (e, t) => {
  let n = e;
  for (let i = 0; n && i < t.length; i += 1) {
    const r = t[i];
    if (r === W)
      n = n.defaultChild || null;
    else {
      const o = je(r);
      n = o >= 0 && n.branches[o] ? n.branches[o].child : null;
    }
  }
  return n || null;
}, Ae = (e, t, n) => {
  const i = te(e, t);
  return i && i.branches[n] ? i.branches[n] : null;
}, O = (e, t, n) => {
  if (!e) return e;
  const i = (r, o) => {
    if (!r) return r;
    if (o >= t.length) return M(n(r));
    const a = t[o];
    if (a === W)
      return M({
        ...r,
        defaultChild: i(r.defaultChild, o + 1)
      });
    const f = je(a);
    return f < 0 || f >= r.branches.length ? r : M({
      ...r,
      branches: r.branches.map((S, b) => b === f ? { ...S, child: i(S.child, o + 1) } : S)
    });
  };
  return i(e, 0);
}, be = (e, t, n, i) => e && O(e, t, (r) => n < 0 || n >= r.branches.length ? r : {
  ...r,
  branches: r.branches.map((o, a) => a === n ? me(i(o)) : o)
}), Rl = (e) => {
  if (!e) return [];
  const t = [], n = (i, r) => {
    i && (t.push({
      kind: "node",
      node: i,
      nodePath: r.nodePath,
      depth: r.depth,
      levelNumber: r.levelNumber,
      branchBadge: r.branchBadge,
      branchDepth: r.branchDepth
    }), i.branches.forEach((o, a) => {
      const f = `${r.levelNumber}.${a + 1}`;
      t.push({
        kind: "branch",
        branch: o,
        nodePath: r.nodePath,
        branchIndex: a,
        depth: r.depth + 1,
        badge: f
      }), o.child && n(o.child, {
        nodePath: [...r.nodePath, Ne(a)],
        depth: r.depth + 2,
        levelNumber: r.levelNumber + 1,
        branchBadge: f,
        branchDepth: 0
      });
    }), i.defaultChild && n(i.defaultChild, {
      nodePath: [...r.nodePath, W],
      depth: r.depth + 1,
      levelNumber: r.levelNumber + 1,
      branchBadge: r.branchBadge,
      branchDepth: r.branchBadge ? r.branchDepth + 1 : 0
    }));
  };
  return n(e, {
    nodePath: [],
    depth: 0,
    levelNumber: 1,
    branchBadge: null,
    branchDepth: 0
  }), t;
}, ee = (e = []) => e.join(">"), Ce = (e = [], t = []) => ee(e) === ee(t), Z = (e) => e == null ? "" : String(e), G = (e) => e ? e.type === "node" ? `node:${ee(e.path)}` : e.type === "branch" ? `branch:${ee(e.nodePath)}:${e.branchIndex}` : e.type === "rowPath" ? `rowPath:${ee(e.nodePath)}:${e.field}:${Z(e.value)}` : "" : "", ve = (e, t) => (e || []).find((n) => n.kind === "node" && Ce(n.nodePath, t)) || null, Qe = (e, t, n) => (e || []).find((i) => i.kind === "branch" && Ce(i.nodePath, t) && i.branchIndex === n) || null, Je = (e, t, n) => {
  if (!e || !t) return !1;
  const i = Z(n);
  return (e.condition && Array.isArray(e.condition.clauses) ? e.condition.clauses : []).some((o) => !o || o.field !== t ? !1 : o.operator === "eq" || o.operator === "=" ? Z(o.value) === i : o.operator === "in" ? Array.isArray(o.values) && o.values.map(Z).includes(i) : !1);
}, Nl = (e, t, n, i = "") => ({
  ...Al(),
  label: Z(n || t),
  condition: {
    op: "AND",
    clauses: [{
      ...Ye(e),
      operator: "eq",
      value: Z(t),
      values: []
    }]
  },
  child: K(i)
}), Ie = (e) => {
  if (!e) return null;
  try {
    return M(JSON.parse(JSON.stringify(e)));
  } catch {
    return M(e);
  }
}, Fe = (e) => Array.from({ length: Math.max(0, e) }, () => W), Pe = (e, t = 1, n = []) => {
  const i = [];
  let r = e, o = 0, a = Array.isArray(n) ? [...n] : [];
  for (; r; )
    i.push({
      node: r,
      path: [...a],
      index: o,
      levelNumber: t + o
    }), r = r.defaultChild || null, a = [...a, W], o += 1;
  return i;
}, Il = (e) => e ? { ...Ie(e), defaultChild: null } : null, Pl = (e) => {
  if (!Array.isArray(e) || e.length === 0) return null;
  let t = null;
  for (let n = e.length - 1; n >= 0; n -= 1)
    t = M({
      ...Il(e[n]),
      defaultChild: t
    });
  return t;
}, Ue = (e, t, n) => {
  const i = Pe(e).map((a) => a.node);
  if (t < 0 || n < 0 || t >= i.length || n >= i.length || t === n)
    return e;
  const r = [...i], [o] = r.splice(t, 1);
  return r.splice(n, 0, o), Pl(r);
}, El = (e) => {
  const t = Ie(e);
  return !t || !t.field ? null : t;
}, Ll = (e, t) => t ? t.type === "node" ? ve(e, t.path) : t.type === "branch" ? Qe(e, t.nodePath, t.branchIndex) : null : null, Ol = (e, t) => {
  if (!t) return [];
  const n = [], i = (a) => {
    const f = ve(e, a);
    f && n.push(f);
  }, r = (a, f) => {
    const S = Qe(e, a, f);
    S && n.push(S);
  }, o = (a) => {
    if (i([]), !Array.isArray(a) || a.length === 0) return;
    let f = [];
    a.forEach((S) => {
      if (S === W) {
        f = [...f, S], i(f);
        return;
      }
      const b = je(S);
      b >= 0 && (r(f, b), f = [...f, S], i(f));
    });
  };
  return t.type === "node" ? (o(t.path), n) : (o(t.nodePath), r(t.nodePath, t.branchIndex), n);
}, _l = (e, t) => {
  if (!t) return [];
  if (t.type === "node") {
    const i = [], r = ve(e, [...t.path, W]);
    r && i.push(r);
    const o = (e || []).filter((a) => a.kind === "branch" && Ce(a.nodePath, t.path)).sort((a, f) => a.branchIndex - f.branchIndex);
    return [...i, ...o];
  }
  const n = ve(e, [...t.nodePath, Ne(t.branchIndex)]);
  return n ? [n] : [];
}, H = (e, t = {}) => ({
  border: `1px solid ${e.border}`,
  borderRadius: "10px",
  padding: "8px 10px",
  fontSize: "12px",
  lineHeight: 1.4,
  background: e.background || "#fff",
  color: e.text,
  outline: "none",
  width: "100%",
  ...t
}), z = (e, t = !1, n = !1) => ({
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  gap: "6px",
  background: n ? "#FEF2F2" : t ? `${e.primary || "#2563EB"}12` : e.hover || "#F9FAFB",
  border: `1px solid ${n ? "#FECACA" : t ? `${e.primary || "#2563EB"}45` : e.border}`,
  borderRadius: "9px",
  padding: "7px 11px",
  fontSize: "11px",
  fontWeight: 700,
  color: n ? "#B91C1C" : t ? e.primary || "#2563EB" : e.textSec,
  cursor: "pointer"
}), E = (e) => ({
  fontSize: "9px",
  fontWeight: 800,
  textTransform: "uppercase",
  letterSpacing: "0.08em",
  color: e.textSec,
  display: "flex",
  alignItems: "center",
  gap: "6px",
  marginBottom: "10px"
}), de = (e) => ({
  border: `1px solid ${e.border}`,
  borderRadius: "14px",
  background: e.surfaceBg || e.background || "#fff",
  padding: "14px",
  display: "flex",
  flexDirection: "column",
  gap: "12px"
}), Wl = (e) => ({
  borderTop: `2px solid ${e.textSec || "#334155"}`,
  paddingTop: "14px",
  display: "flex",
  flexDirection: "column",
  gap: "12px"
}), el = (e, t, n) => ({
  width: `calc(100% - ${n * 14}px)`,
  marginLeft: `${n * 14}px`,
  border: `1px solid ${t ? e.primary || "#2563EB" : e.border}`,
  borderRadius: "12px",
  background: t ? e.select || `${e.primary || "#2563EB"}14` : e.surfaceBg || e.background || "#fff",
  padding: "12px",
  textAlign: "left",
  cursor: "pointer",
  display: "flex",
  flexDirection: "column",
  gap: "7px"
});
function Y({ children: e, theme: t, tone: n = "neutral" }) {
  const i = n === "primary" ? { background: `${t.primary || "#2563EB"}12`, border: `${t.primary || "#2563EB"}30`, color: t.primary || "#2563EB" } : { background: t.hover || "#F3F4F6", border: t.border, color: t.textSec };
  return /* @__PURE__ */ l.jsx(
    "span",
    {
      style: {
        display: "inline-flex",
        alignItems: "center",
        gap: "4px",
        padding: "3px 7px",
        borderRadius: "999px",
        fontSize: "10px",
        fontWeight: 700,
        border: `1px solid ${i.border}`,
        background: i.background,
        color: i.color
      },
      children: e
    }
  );
}
function le({
  theme: e,
  title: t,
  description: n,
  icon: i,
  action: r = null,
  sectionId: o,
  children: a
}) {
  return /* @__PURE__ */ l.jsxs("section", { "data-report-section": o || t, style: Wl(e), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", gap: "8px", minWidth: 0 }, children: [
        /* @__PURE__ */ l.jsx("div", { style: { color: e.textSec, display: "flex", alignItems: "center", paddingTop: "1px", flexShrink: 0 }, children: i }),
        /* @__PURE__ */ l.jsxs("div", { style: { minWidth: 0 }, children: [
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "13px", fontWeight: 900, color: e.text }, children: t }),
          n && /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: e.textSec, lineHeight: 1.45, marginTop: "2px" }, children: n })
        ] })
      ] }),
      r && /* @__PURE__ */ l.jsx("div", { style: { flexShrink: 0 }, children: r })
    ] }),
    /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: a })
  ] });
}
function Be({ condition: e, allFields: t, getFieldLabel: n = T, onSave: i, onClose: r, theme: o, title: a, description: f }) {
  const [S, b] = x.useState(() => Re(e)), B = H(o), N = x.useCallback(() => {
    b((d) => ({ ...d, clauses: [...d.clauses, Ye(t[0] || "")] }));
  }, [t]), h = x.useCallback((d, C) => {
    b((v) => ({
      ...v,
      clauses: v.clauses.map((u, D) => D === d ? { ...u, ...C } : u)
    }));
  }, []), k = x.useCallback((d) => {
    b((C) => ({
      ...C,
      clauses: C.clauses.filter((v, u) => u !== d)
    }));
  }, []);
  return /* @__PURE__ */ l.jsx(
    "div",
    {
      style: {
        position: "fixed",
        inset: 0,
        zIndex: 9999,
        background: "rgba(15, 23, 42, 0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center"
      },
      onClick: r,
      children: /* @__PURE__ */ l.jsxs(
        "div",
        {
          style: {
            ...de(o),
            width: "520px",
            maxWidth: "95vw",
            maxHeight: "85vh",
            overflowY: "auto",
            boxShadow: "0 24px 64px rgba(15, 23, 42, 0.22)"
          },
          onClick: (d) => d.stopPropagation(),
          children: [
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px" }, children: [
              /* @__PURE__ */ l.jsxs("div", { children: [
                /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: o.text }, children: a || "Edit row override rule" }),
                /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: o.textSec, marginTop: "2px" }, children: f || "Add one or more clauses to decide when this row override should be used." })
              ] }),
              /* @__PURE__ */ l.jsx("button", { type: "button", onClick: r, style: { background: "none", border: "none", cursor: "pointer", color: o.textSec }, children: /* @__PURE__ */ l.jsx(j.Close, {}) })
            ] }),
            S.clauses.length > 1 && /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ l.jsx("span", { style: { fontSize: "11px", color: o.textSec, fontWeight: 700 }, children: "Match:" }),
              ["AND", "OR"].map((d) => /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => b((C) => ({ ...C, op: d })),
                  style: {
                    padding: "5px 12px",
                    borderRadius: "999px",
                    fontSize: "11px",
                    fontWeight: 800,
                    border: "none",
                    cursor: "pointer",
                    background: S.op === d ? o.primary || "#2563EB" : o.hover || "#F3F4F6",
                    color: S.op === d ? "#fff" : o.textSec
                  },
                  children: d
                },
                d
              ))
            ] }),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: [
              S.clauses.length === 0 && /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: o.textSec, fontStyle: "italic" }, children: "No clauses means this row override matches every row at this level." }),
              S.clauses.map((d, C) => /* @__PURE__ */ l.jsxs(
                "div",
                {
                  style: {
                    border: `1px solid ${o.border}`,
                    borderRadius: "12px",
                    background: o.hover || "#F8FAFC",
                    padding: "10px",
                    display: "flex",
                    gap: "8px",
                    alignItems: "flex-start"
                  },
                  children: [
                    /* @__PURE__ */ l.jsx("div", { style: { width: "32px", paddingTop: "6px", textAlign: "center", fontSize: "10px", fontWeight: 800, color: o.textSec }, children: C > 0 ? S.op : "" }),
                    /* @__PURE__ */ l.jsxs("div", { style: { flex: 1, display: "flex", flexDirection: "column", gap: "8px" }, children: [
                      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap" }, children: [
                        /* @__PURE__ */ l.jsx(
                          "select",
                          {
                            value: d.field,
                            onChange: (v) => h(C, { field: v.target.value }),
                            style: { ...B, flex: 1, minWidth: "140px" },
                            children: t.map((v) => /* @__PURE__ */ l.jsx("option", { value: v, children: n(v) }, v))
                          }
                        ),
                        /* @__PURE__ */ l.jsx(
                          "select",
                          {
                            value: d.operator,
                            onChange: (v) => {
                              const u = v.target.value, D = ae.has(u), R = ae.has(d.operator);
                              h(C, {
                                operator: u,
                                values: D ? R ? d.values : d.value ? [d.value] : [] : d.values,
                                value: D ? d.value : d.values[0] || d.value || ""
                              });
                            },
                            style: { ...B, width: "160px", flexShrink: 0 },
                            children: Ke.map((v) => /* @__PURE__ */ l.jsx("option", { value: v.value, children: v.label }, v.value))
                          }
                        )
                      ] }),
                      ae.has(d.operator) ? /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: [
                        d.values.length > 0 && /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" }, children: d.values.map((v, u) => /* @__PURE__ */ l.jsxs(Y, { theme: o, tone: "primary", children: [
                          /* @__PURE__ */ l.jsx("span", { children: v }),
                          /* @__PURE__ */ l.jsx(
                            "button",
                            {
                              type: "button",
                              onClick: () => h(C, {
                                values: d.values.filter((D, R) => R !== u)
                              }),
                              style: { background: "none", border: "none", padding: 0, cursor: "pointer", color: "inherit", lineHeight: 1 },
                              children: "x"
                            }
                          )
                        ] }, `${d._id}-${u}`)) }),
                        /* @__PURE__ */ l.jsx(
                          "input",
                          {
                            placeholder: "Type a value and press Enter",
                            style: B,
                            onKeyDown: (v) => {
                              const u = v.target.value.trim();
                              v.key === "Enter" && u && (d.values.includes(u) || h(C, { values: [...d.values, u] }), v.target.value = "", v.preventDefault());
                            }
                          }
                        )
                      ] }) : /* @__PURE__ */ l.jsx(
                        "input",
                        {
                          value: d.value,
                          onChange: (v) => h(C, { value: v.target.value }),
                          placeholder: "Value",
                          style: B
                        }
                      )
                    ] }),
                    /* @__PURE__ */ l.jsx(
                      "button",
                      {
                        type: "button",
                        onClick: () => k(C),
                        style: { background: "none", border: "none", cursor: "pointer", color: "#B91C1C", padding: "6px" },
                        children: /* @__PURE__ */ l.jsx(j.Delete, {})
                      }
                    )
                  ]
                },
                d._id
              ))
            ] }),
            /* @__PURE__ */ l.jsxs(
              "button",
              {
                type: "button",
                onClick: N,
                style: {
                  ...z(o),
                  width: "100%",
                  borderStyle: "dashed"
                },
                children: [
                  /* @__PURE__ */ l.jsx(j.Add, {}),
                  " Add clause"
                ]
              }
            ),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", justifyContent: "flex-end", gap: "8px", borderTop: `1px solid ${o.border}`, paddingTop: "12px" }, children: [
              /* @__PURE__ */ l.jsx("button", { type: "button", onClick: r, style: z(o), children: "Cancel" }),
              /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => {
                    i(S), r();
                  },
                  style: {
                    background: o.primary || "#2563EB",
                    border: "none",
                    borderRadius: "9px",
                    padding: "8px 14px",
                    fontSize: "12px",
                    fontWeight: 800,
                    color: "#fff",
                    cursor: "pointer"
                  },
                  children: "Apply"
                }
              )
            ] })
          ]
        }
      )
    }
  );
}
function ll({ value: e, onChange: t, theme: n }) {
  const i = ge(e), r = H(n), o = (a) => t(ge({ ...i, ...a }));
  return /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsx("div", { style: E(n), children: "Presentation" }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px" }, children: [
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Indent px",
        /* @__PURE__ */ l.jsx("input", { type: "number", min: "0", value: i.indent, onChange: (a) => o({ indent: a.target.value }), placeholder: "Auto", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Row color",
        /* @__PURE__ */ l.jsx("input", { value: i.rowColor, onChange: (a) => o({ rowColor: a.target.value }), placeholder: "#F8FAFC", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Label prefix",
        /* @__PURE__ */ l.jsx("input", { value: i.labelPrefix, onChange: (a) => o({ labelPrefix: a.target.value }), placeholder: "Optional", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Label suffix",
        /* @__PURE__ */ l.jsx("input", { value: i.labelSuffix, onChange: (a) => o({ labelSuffix: a.target.value }), placeholder: "Optional", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec, gridColumn: "1 / -1" }, children: [
        "Number format override",
        /* @__PURE__ */ l.jsx("input", { value: i.numberFormat, onChange: (a) => o({ numberFormat: a.target.value }), placeholder: "fixed:2, percent, currency:$", style: r })
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "8px" }, children: [
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Border",
        /* @__PURE__ */ l.jsx("select", { value: i.borderStyle, onChange: (a) => o({ borderStyle: a.target.value }), style: r, children: Ge.map((a) => /* @__PURE__ */ l.jsx("option", { value: a, children: a || "None" }, a || "none")) })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Border color",
        /* @__PURE__ */ l.jsx("input", { value: i.borderColor, onChange: (a) => o({ borderColor: a.target.value }), placeholder: "#CBD5E1", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Width",
        /* @__PURE__ */ l.jsx("input", { type: "number", min: "1", value: i.borderWidth, onChange: (a) => o({ borderWidth: a.target.value }), placeholder: "1", style: r })
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "14px", flexWrap: "wrap" }, children: [
      /* @__PURE__ */ l.jsxs("label", { style: { display: "inline-flex", alignItems: "center", gap: "7px", fontSize: "12px", color: n.text, fontWeight: 700 }, children: [
        /* @__PURE__ */ l.jsx("input", { type: "checkbox", checked: i.bold, onChange: (a) => o({ bold: a.target.checked }) }),
        "Bold row"
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "inline-flex", alignItems: "center", gap: "7px", fontSize: "12px", color: n.text, fontWeight: 700 }, children: [
        /* @__PURE__ */ l.jsx("input", { type: "checkbox", checked: i.showSubtotal, onChange: (a) => o({ showSubtotal: a.target.checked }) }),
        "Show subtotal/other rows"
      ] })
    ] })
  ] });
}
function zl({ validation: e, theme: t }) {
  const n = e && Array.isArray(e.errors) ? e.errors : [], i = e && Array.isArray(e.warnings) ? e.warnings : [];
  return n.length === 0 && i.length === 0 ? null : /* @__PURE__ */ l.jsxs("div", { "data-report-validation": "true", style: { border: `1px solid ${n.length > 0 ? "#FCA5A5" : "#FDE68A"}`, borderRadius: "12px", padding: "10px 12px", background: n.length > 0 ? "#FEF2F2" : "#FFFBEB", color: n.length > 0 ? "#991B1B" : "#92400E", fontSize: "11px", lineHeight: 1.45, display: "flex", flexDirection: "column", gap: "6px" }, children: [
    n.length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: { fontWeight: 900, marginBottom: "4px" }, children: "Fix before applying or saving" }),
      n.map((r) => /* @__PURE__ */ l.jsx("div", { children: r }, `err:${r}`))
    ] }),
    i.length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: { fontWeight: 900, marginBottom: "4px" }, children: "Warnings" }),
      i.map((r) => /* @__PURE__ */ l.jsx("div", { children: r }, `warn:${r}`))
    ] })
  ] });
}
function Tl({ debug: e, theme: t }) {
  if (!e || typeof e != "object") return null;
  const n = [
    ["Matched level", e.matchedLevel || e.levelLabel || ""],
    ["Field", e.levelField || ""],
    ["Row path", Array.isArray(e.pathValues) ? e.pathValues.join(" > ") : e.rowPath || ""],
    ["Children source", e.childrenSource || "Default children"],
    ["Rule", e.matchedCondition || ""]
  ].filter(([, i]) => i);
  return n.length === 0 ? null : /* @__PURE__ */ l.jsxs("div", { "data-report-debug-inspector": "true", style: { border: `1px solid ${t.border}`, borderRadius: "12px", padding: "10px", background: t.hover || "#F8FAFC", display: "flex", flexDirection: "column", gap: "7px" }, children: [
    /* @__PURE__ */ l.jsx("div", { style: E(t), children: "Why Is This Row Here?" }),
    n.map(([i, r]) => /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", justifyContent: "space-between", gap: "10px", fontSize: "11px" }, children: [
      /* @__PURE__ */ l.jsx("span", { style: { color: t.textSec, fontWeight: 800 }, children: i }),
      /* @__PURE__ */ l.jsx("span", { style: { color: t.text, textAlign: "right", overflowWrap: "anywhere" }, children: r })
    ] }, i))
  ] });
}
function tl({
  title: e,
  summary: t,
  theme: n,
  defaultOpen: i = !1,
  children: r
}) {
  const [o, a] = x.useState(i);
  return /* @__PURE__ */ l.jsxs(
    "div",
    {
      "data-report-level-collapsible": "true",
      "data-report-level-collapsed": o ? "false" : "true",
      style: {
        border: `1px solid ${n.border}`,
        borderRadius: "12px",
        background: n.surfaceBg || n.background || "#fff",
        overflow: "hidden"
      },
      children: [
        /* @__PURE__ */ l.jsxs(
          "button",
          {
            type: "button",
            onClick: () => a((f) => !f),
            style: {
              width: "100%",
              border: "none",
              background: o ? n.hover || "#F8FAFC" : "transparent",
              color: n.text,
              cursor: "pointer",
              padding: "10px 12px",
              display: "flex",
              alignItems: "center",
              gap: "8px",
              textAlign: "left"
            },
            "aria-expanded": o,
            children: [
              /* @__PURE__ */ l.jsx("span", { style: { display: "inline-flex", color: n.textSec, flexShrink: 0 }, children: o ? /* @__PURE__ */ l.jsx(j.ChevronDown, {}) : /* @__PURE__ */ l.jsx(j.ChevronRight, {}) }),
              /* @__PURE__ */ l.jsxs("span", { style: { flex: 1, minWidth: 0 }, children: [
                /* @__PURE__ */ l.jsx("span", { style: { display: "block", fontSize: "12px", fontWeight: 900, color: n.text }, children: e }),
                t && /* @__PURE__ */ l.jsx("span", { style: { display: "block", fontSize: "11px", color: n.textSec, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginTop: "2px" }, children: t })
              ] })
            ]
          }
        ),
        o && /* @__PURE__ */ l.jsx("div", { style: { borderTop: `1px solid ${n.border}`, padding: "12px", display: "flex", flexDirection: "column", gap: "12px" }, children: r })
      ]
    }
  );
}
function Ml({ item: e, selected: t, theme: n, sortByOptions: i, getFieldLabel: r = T, onSelect: o }) {
  const a = e.node, f = Dl(e), S = a.field ? r(a.field) : "Select field", b = Xe(a), B = Ze(a.sortBy, i);
  return /* @__PURE__ */ l.jsx("button", { type: "button", onClick: o, style: el(n, t, e.depth), children: /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsxs("div", { style: { minWidth: 0, display: "flex", flexDirection: "column", gap: "7px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, tone: e.branchBadge ? "primary" : "neutral", children: f }),
        /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: S })
      ] }),
      /* @__PURE__ */ l.jsxs("div", { style: { fontSize: "11px", color: n.textSec }, children: [
        "Header: ",
        b
      ] }),
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, children: a.topN ? `Top ${a.topN}` : "All rows" }),
        /* @__PURE__ */ l.jsx(Y, { theme: n, children: B ? `Sort ${a.sortDir}${a.sortAbs ? " |abs|" : ""} by ${B}` : "Default sort" }),
        a.defaultChild && /* @__PURE__ */ l.jsx(Y, { theme: n, children: "Has next level" }),
        a.branches.length > 0 && /* @__PURE__ */ l.jsxs(Y, { theme: n, tone: "primary", children: [
          a.branches.length,
          " row override",
          a.branches.length === 1 ? "" : "s"
        ] })
      ] })
    ] }),
    /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(j.ChevronRight, {}) })
  ] }) });
}
function Hl({ item: e, selected: t, theme: n, onSelect: i }) {
  const r = e.branch, o = r.label && r.label.trim() ? r.label.trim() : "Display original label";
  return /* @__PURE__ */ l.jsx("button", { type: "button", onClick: i, style: el(n, t, e.depth), children: /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsxs("div", { style: { minWidth: 0, display: "flex", flexDirection: "column", gap: "7px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, tone: "primary", children: `Row override ${e.badge}` }),
        /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: o })
      ] }),
      /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: n.textSec, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: Se(r.condition) }),
      /* @__PURE__ */ l.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: /* @__PURE__ */ l.jsx(Y, { theme: n, children: r.child ? "Custom children" : "Default children" }) })
    ] }),
    /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(j.ChevronRight, {}) })
  ] }) });
}
function rl({
  item: e,
  selected: t,
  theme: n,
  sortByOptions: i,
  getFieldLabel: r = T,
  onSelect: o,
  onDragStart: a,
  onDragOver: f,
  onDrop: S
}) {
  const b = e.node, B = b.field ? r(b.field) : "Select field", N = b.label && b.label.trim() ? b.label.trim() : B, h = Ze(b.sortBy, i);
  return /* @__PURE__ */ l.jsxs(
    "button",
    {
      type: "button",
      draggable: !0,
      onDragStart: a,
      onDragOver: f,
      onDrop: S,
      onClick: o,
      style: {
        width: "100%",
        border: `1px solid ${t ? n.primary || "#2563EB" : n.border}`,
        borderRadius: "12px",
        background: t ? n.select || `${n.primary || "#2563EB"}14` : n.surfaceBg || n.background || "#fff",
        padding: "10px 12px",
        textAlign: "left",
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: "10px"
      },
      children: [
        /* @__PURE__ */ l.jsx("span", { style: { cursor: "grab", display: "inline-flex" }, children: /* @__PURE__ */ l.jsx(j.DragIndicator, {}) }),
        /* @__PURE__ */ l.jsxs("div", { style: { flex: 1, minWidth: 0 }, children: [
          /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "7px", minWidth: 0 }, children: [
            /* @__PURE__ */ l.jsxs("span", { style: { fontSize: "11px", fontWeight: 800, color: n.textSec, flexShrink: 0 }, children: [
              "Level ",
              e.levelNumber
            ] }),
            /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: N })
          ] }),
          /* @__PURE__ */ l.jsxs("div", { style: { fontSize: "11px", color: n.textSec, marginTop: "3px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: [
            B,
            h ? ` · ${b.sortDir === "asc" ? "Asc" : "Desc"}${b.sortAbs ? " |abs|" : ""} by ${h}` : "",
            b.topN ? ` · Top ${b.topN}` : "",
            b.filters && b.filters.clauses && b.filters.clauses.length > 0 ? ` · ${b.filters.clauses.length} filter${b.filters.clauses.length === 1 ? "" : "s"}` : ""
          ] })
        ] }),
        /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(j.ChevronRight, {}) })
      ]
    }
  );
}
function Vl({
  item: e,
  node: t,
  allFields: n,
  getFieldLabel: i = T,
  sortByOptions: r,
  theme: o,
  onUpdate: a,
  onApply: f,
  onRemove: S,
  onEditFilter: b,
  titleLabel: B = "Title",
  canRemove: N = !0
}) {
  const h = H(o), k = e && e.levelNumber ? `Level ${e.levelNumber}` : "Level", d = !!(t.filters && t.filters.clauses && t.filters.clauses.length > 0), v = [
    t.field ? i(t.field) : "Select field",
    t.label && t.label.trim() ? `Title: ${t.label.trim()}` : null,
    t.topN ? `Top ${t.topN}` : null,
    d ? `${t.filters.clauses.length} filter${t.filters.clauses.length === 1 ? "" : "s"}` : null
  ].filter(Boolean);
  return /* @__PURE__ */ l.jsxs("div", { style: de(o), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsx("div", { children: /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: o.text }, children: k }) }),
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexShrink: 0 }, children: [
        /* @__PURE__ */ l.jsx(
          "button",
          {
            type: "button",
            onClick: f,
            style: {
              background: o.primary || "#2563EB",
              border: "none",
              borderRadius: "9px",
              padding: "8px 14px",
              fontSize: "12px",
              fontWeight: 800,
              color: "#fff",
              cursor: "pointer"
            },
            children: "Apply"
          }
        ),
        N && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: S, style: z(o, !1, !0), children: [
          /* @__PURE__ */ l.jsx(j.Delete, {}),
          " ",
          e && e.path && e.path.length === 0 ? "Clear report" : "Remove level"
        ] })
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs(
      tl,
      {
        title: `${k} settings`,
        summary: v.join(" · "),
        theme: o,
        defaultOpen: !1,
        children: [
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: E(o), children: "Break Down By" }),
            /* @__PURE__ */ l.jsxs(
              "select",
              {
                value: t.field,
                onChange: (u) => a({ ...t, field: u.target.value }),
                style: h,
                children: [
                  /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                  n.map((u) => /* @__PURE__ */ l.jsx("option", { value: u, children: i(u) }, u))
                ]
              }
            )
          ] }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: E(o), children: B }),
            /* @__PURE__ */ l.jsx(
              "input",
              {
                value: t.label,
                onChange: (u) => a({ ...t, label: u.target.value }),
                placeholder: t.field ? i(t.field) : "Label for this level",
                style: h
              }
            )
          ] }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: E(o), children: "Ranking And Sort" }),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "82px 1fr 42px 42px", gap: "8px", alignItems: "stretch" }, children: [
              /* @__PURE__ */ l.jsx(
                "input",
                {
                  type: "number",
                  min: "1",
                  value: t.topN || "",
                  onChange: (u) => {
                    const D = u.target.value === "" ? null : parseInt(u.target.value, 10);
                    a({ ...t, topN: D && D > 0 ? D : null });
                  },
                  placeholder: "All",
                  style: { ...h, textAlign: "center" }
                }
              ),
              /* @__PURE__ */ l.jsx(
                "select",
                {
                  value: t.sortBy || "",
                  onChange: (u) => a({ ...t, sortBy: u.target.value || null }),
                  style: h,
                  children: r.map((u) => /* @__PURE__ */ l.jsx("option", { value: u.value, children: u.label }, u.value))
                }
              ),
              /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => a({ ...t, sortAbs: !t.sortAbs }),
                  style: {
                    border: `1px solid ${t.sortAbs ? o.primary || "#2563EB" : o.border}`,
                    borderRadius: "10px",
                    background: t.sortAbs ? `${o.primary || "#2563EB"}18` : o.hover || "#F3F4F6",
                    color: t.sortAbs ? o.primary || "#2563EB" : o.text,
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    fontSize: "11px",
                    fontWeight: 700
                  },
                  title: "Sort by absolute value (ignores sign)",
                  children: "|a|"
                }
              ),
              /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => a({ ...t, sortDir: t.sortDir === "asc" ? "desc" : "asc" }),
                  style: {
                    border: `1px solid ${o.border}`,
                    borderRadius: "10px",
                    background: o.hover || "#F3F4F6",
                    color: o.text,
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center"
                  },
                  title: t.sortDir === "asc" ? "Ascending" : "Descending",
                  children: t.sortDir === "asc" ? /* @__PURE__ */ l.jsx(j.SortAsc, {}) : /* @__PURE__ */ l.jsx(j.SortDesc, {})
                }
              )
            ] })
          ] }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: E(o), children: "Level Filter" }),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", alignItems: "stretch" }, children: [
              /* @__PURE__ */ l.jsxs(
                "button",
                {
                  type: "button",
                  onClick: b,
                  style: H(o, {
                    flex: 1,
                    display: "flex",
                    alignItems: "center",
                    gap: "8px",
                    textAlign: "left",
                    cursor: "pointer",
                    background: d ? `${o.primary || "#2563EB"}08` : o.hover || "#F8FAFC",
                    borderColor: d ? `${o.primary || "#2563EB"}45` : void 0
                  }),
                  children: [
                    /* @__PURE__ */ l.jsx(j.Filter, {}),
                    /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }, children: d ? Se(t.filters) : "No filter (show all rows)" })
                  ]
                }
              ),
              d && /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => a({ ...t, filters: null }),
                  style: {
                    border: `1px solid ${o.border}`,
                    borderRadius: "10px",
                    background: o.hover || "#F3F4F6",
                    color: "#B91C1C",
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    padding: "0 10px",
                    flexShrink: 0
                  },
                  title: "Clear filter",
                  children: /* @__PURE__ */ l.jsx(j.Close, {})
                }
              )
            ] }),
            /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: o.textSec, marginTop: "6px", lineHeight: 1.4 }, children: `Rows that don't match the filter are grouped into "Others" to keep totals intact.` })
          ] }),
          /* @__PURE__ */ l.jsx(
            ll,
            {
              value: t.format,
              theme: o,
              onChange: (u) => a({ ...t, format: u })
            }
          )
        ]
      }
    )
  ] });
}
function ql({
  draft: e,
  allFields: t,
  getFieldLabel: n = T,
  sortByOptions: i,
  theme: r,
  onChange: o,
  onApply: a,
  onCancel: f
}) {
  const [S, b] = x.useState(null), [B, N] = x.useState(!1), h = e && e.child ? e.child : K(""), k = Array.isArray(e && e.selectedPath) ? e.selectedPath : [], d = te(h, k) || h, C = !!(d.filters && d.filters.clauses && d.filters.clauses.length > 0), v = Pe(h, e.childLevelNumber || 2), u = Math.max(0, k.length), D = (e && Number.isFinite(Number(e.childLevelNumber)) ? Number(e.childLevelNumber) : 2) + k.length, R = H(r), y = [
    d.field ? n(d.field) : "Select field",
    e.title && e.title.trim() ? `Title: ${e.title.trim()}` : null,
    d.topN ? `Top ${d.topN}` : null,
    C ? `${d.filters.clauses.length} filter${d.filters.clauses.length === 1 ? "" : "s"}` : null
  ].filter(Boolean), A = (c) => {
    o({
      ...e,
      child: O(h, k, () => c)
    });
  }, V = (c) => {
    o({
      ...e,
      selectedPath: Fe(c)
    });
  }, ce = () => {
    const c = [...k, W];
    o({
      ...e,
      child: O(h, k, ($) => ({
        ...$,
        defaultChild: $.defaultChild || K("")
      })),
      selectedPath: c
    });
  }, ne = () => {
    o({
      ...e,
      child: O(h, k, (c) => ({
        ...c,
        defaultChild: null
      }))
    });
  }, pe = (c, $) => {
    c === null || c === $ || (o({
      ...e,
      child: Ue(h, c, $),
      selectedPath: Fe($)
    }), b(null));
  };
  return /* @__PURE__ */ l.jsxs(l.Fragment, { children: [
    /* @__PURE__ */ l.jsxs("div", { style: de(r), children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
        /* @__PURE__ */ l.jsxs("div", { children: [
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: r.text }, children: e.rowLabel || "Selected row" }),
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: r.textSec, marginTop: "2px" }, children: "Override applies to this row only." })
        ] }),
        /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexShrink: 0 }, children: [
          /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: f, style: z(r), children: [
            /* @__PURE__ */ l.jsx(j.Close, {}),
            " Close"
          ] }),
          /* @__PURE__ */ l.jsx(
            "button",
            {
              type: "button",
              onClick: a,
              style: {
                background: r.primary || "#2563EB",
                border: "none",
                borderRadius: "9px",
                padding: "8px 14px",
                fontSize: "12px",
                fontWeight: 800,
                color: "#fff",
                cursor: "pointer"
              },
              children: "Apply"
            }
          )
        ] })
      ] }),
      /* @__PURE__ */ l.jsxs("div", { children: [
        /* @__PURE__ */ l.jsx("div", { style: E(r), children: "Level" }),
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: r.text }, children: `Level ${D}` })
      ] }),
      /* @__PURE__ */ l.jsx(Tl, { debug: e.request && e.request.debug, theme: r }),
      v.length > 1 && /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: v.map((c) => /* @__PURE__ */ l.jsx(
        rl,
        {
          item: c,
          selected: u === c.index,
          theme: r,
          sortByOptions: i,
          getFieldLabel: n,
          onSelect: () => V(c.index),
          onDragStart: ($) => {
            b(c.index), $.dataTransfer.effectAllowed = "move";
          },
          onDragOver: ($) => $.preventDefault(),
          onDrop: ($) => {
            $.preventDefault(), pe(S, c.index);
          }
        },
        `draft-level:${c.index}:${c.node._id}`
      )) }),
      /* @__PURE__ */ l.jsxs(
        tl,
        {
          title: `Level ${D} settings`,
          summary: y.join(" · "),
          theme: r,
          defaultOpen: !1,
          children: [
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: E(r), children: "Title" }),
              /* @__PURE__ */ l.jsx(
                "input",
                {
                  value: e.title,
                  onChange: (c) => o({ ...e, title: c.target.value }),
                  placeholder: e.rowLabel || "Row title",
                  style: R
                }
              )
            ] }),
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: E(r), children: "Break Down By" }),
              /* @__PURE__ */ l.jsxs(
                "select",
                {
                  value: d.field,
                  onChange: (c) => A({ ...d, field: c.target.value }),
                  style: R,
                  children: [
                    /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                    t.map((c) => /* @__PURE__ */ l.jsx("option", { value: c, children: n(c) }, c))
                  ]
                }
              )
            ] }),
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: E(r), children: "Ranking And Sort" }),
              /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "82px 1fr 42px 42px", gap: "8px", alignItems: "stretch" }, children: [
                /* @__PURE__ */ l.jsx(
                  "input",
                  {
                    type: "number",
                    min: "1",
                    value: d.topN || "",
                    onChange: (c) => {
                      const $ = c.target.value === "" ? null : parseInt(c.target.value, 10);
                      A({ ...d, topN: $ && $ > 0 ? $ : null });
                    },
                    placeholder: "All",
                    style: { ...R, textAlign: "center" }
                  }
                ),
                /* @__PURE__ */ l.jsx(
                  "select",
                  {
                    value: d.sortBy || "",
                    onChange: (c) => A({ ...d, sortBy: c.target.value || null }),
                    style: R,
                    children: i.map((c) => /* @__PURE__ */ l.jsx("option", { value: c.value, children: c.label }, c.value))
                  }
                ),
                /* @__PURE__ */ l.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => A({ ...d, sortAbs: !d.sortAbs }),
                    style: {
                      border: `1px solid ${d.sortAbs ? r.primary || "#2563EB" : r.border}`,
                      borderRadius: "10px",
                      background: d.sortAbs ? `${r.primary || "#2563EB"}18` : r.hover || "#F3F4F6",
                      color: d.sortAbs ? r.primary || "#2563EB" : r.text,
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontSize: "11px",
                      fontWeight: 700
                    },
                    title: "Sort by absolute value (ignores sign)",
                    children: "|a|"
                  }
                ),
                /* @__PURE__ */ l.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => A({ ...d, sortDir: d.sortDir === "asc" ? "desc" : "asc" }),
                    style: {
                      border: `1px solid ${r.border}`,
                      borderRadius: "10px",
                      background: r.hover || "#F3F4F6",
                      color: r.text,
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center"
                    },
                    title: d.sortDir === "asc" ? "Ascending" : "Descending",
                    children: d.sortDir === "asc" ? /* @__PURE__ */ l.jsx(j.SortAsc, {}) : /* @__PURE__ */ l.jsx(j.SortDesc, {})
                  }
                )
              ] })
            ] }),
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: E(r), children: "Filter (this row's children only)" }),
              /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", alignItems: "stretch" }, children: [
                /* @__PURE__ */ l.jsxs(
                  "button",
                  {
                    type: "button",
                    onClick: () => N(!0),
                    style: H(r, {
                      flex: 1,
                      display: "flex",
                      alignItems: "center",
                      gap: "8px",
                      textAlign: "left",
                      cursor: "pointer",
                      background: C ? `${r.primary || "#2563EB"}08` : r.hover || "#F8FAFC",
                      borderColor: C ? `${r.primary || "#2563EB"}45` : void 0
                    }),
                    children: [
                      /* @__PURE__ */ l.jsx(j.Filter, {}),
                      /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }, children: C ? Se(d.filters) : "No filter (show all children)" })
                    ]
                  }
                ),
                C && /* @__PURE__ */ l.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => A({ ...d, filters: null }),
                    style: {
                      border: `1px solid ${r.border}`,
                      borderRadius: "10px",
                      background: r.hover || "#F3F4F6",
                      color: "#B91C1C",
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      padding: "0 10px",
                      flexShrink: 0
                    },
                    title: "Clear filter",
                    children: /* @__PURE__ */ l.jsx(j.Close, {})
                  }
                )
              ] }),
              /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: r.textSec, marginTop: "6px", lineHeight: 1.4 }, children: 'Filtered-out children are grouped into "Others" to keep totals intact.' })
            ] }),
            /* @__PURE__ */ l.jsx(
              ll,
              {
                value: d.format,
                theme: r,
                onChange: (c) => A({ ...d, format: c })
              }
            ),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px" }, children: [
              /* @__PURE__ */ l.jsxs(
                "button",
                {
                  type: "button",
                  onClick: ce,
                  disabled: !d.field,
                  style: { ...z(r, !!d.defaultChild), opacity: d.field ? 1 : 0.5 },
                  children: [
                    /* @__PURE__ */ l.jsx(j.Add, {}),
                    " ",
                    d.defaultChild ? "Open next level" : "Add next level"
                  ]
                }
              ),
              d.defaultChild && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: ne, style: z(r, !1, !0), children: [
                /* @__PURE__ */ l.jsx(j.Delete, {}),
                " Remove next level"
              ] })
            ] })
          ]
        }
      )
    ] }),
    B && /* @__PURE__ */ l.jsx(
      Be,
      {
        title: `Filter children of "${e.rowLabel || "this row"}"`,
        description: "Only children matching this filter are shown. Filtered-out rows are grouped into 'Others' to keep totals intact.",
        condition: d.filters || De(),
        allFields: t,
        getFieldLabel: n,
        theme: r,
        onSave: (c) => {
          A({
            ...d,
            filters: c.clauses.length > 0 ? c : null
          });
        },
        onClose: () => N(!1)
      }
    )
  ] });
}
function Jl({
  item: e,
  branch: t,
  theme: n,
  onUpdate: i,
  onEditCondition: r,
  onOpenCustomChildren: o,
  onRemoveCustomChildren: a,
  onRemove: f
}) {
  const S = H(n), b = Se(t.condition);
  return /* @__PURE__ */ l.jsxs("div", { style: de(n), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { children: [
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: n.text }, children: `Row override ${e.badge}` }),
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: n.textSec, marginTop: "2px" }, children: "Use a row override when matching rows need a different label or child breakdown." })
      ] }),
      /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: f, style: z(n, !1, !0), children: [
        /* @__PURE__ */ l.jsx(j.Delete, {}),
        " Remove override"
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: E(n), children: "Display As" }),
      /* @__PURE__ */ l.jsx(
        "input",
        {
          value: t.label,
          onChange: (B) => i({ ...t, label: B.target.value }),
          placeholder: "Label to show for matching rows",
          style: S
        }
      )
    ] }),
    /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: E(n), children: "Rule" }),
      /* @__PURE__ */ l.jsxs(
        "button",
        {
          type: "button",
          onClick: r,
          title: b,
          style: {
            ...H(n),
            display: "flex",
            alignItems: "center",
            gap: "8px",
            textAlign: "left",
            cursor: "pointer",
            background: n.hover || "#F8FAFC"
          },
          children: [
            /* @__PURE__ */ l.jsx(j.Filter, {}),
            /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: b })
          ]
        }
      )
    ] }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px" }, children: [
      /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: o, style: z(n, !!t.child), children: [
        /* @__PURE__ */ l.jsx(j.Add, {}),
        " ",
        t.child ? "Open Custom children" : "Add Custom children"
      ] }),
      t.child && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: a, style: z(n, !1, !0), children: [
        /* @__PURE__ */ l.jsx(j.Delete, {}),
        " Remove Custom children"
      ] })
    ] }),
    /* @__PURE__ */ l.jsx(
      "div",
      {
        style: {
          borderRadius: "10px",
          padding: "10px 12px",
          background: n.hover || "#F8FAFC",
          color: n.textSec,
          fontSize: "11px",
          lineHeight: 1.5
        },
        children: "Override applies to this row only. Rows without this override continue with Default children."
      }
    )
  ] });
}
function Yl({
  reportDef: e,
  setReportDef: t,
  availableFields: n,
  customDimensions: i = [],
  theme: r,
  data: o = [],
  valConfigs: a,
  colFields: f,
  setColFields: S,
  setValConfigs: b,
  savedReports: B,
  setSavedReports: N,
  activeReportId: h,
  setActiveReportId: k,
  showNotification: d,
  requestedSelection: C = null,
  showReportConfigColumn: v = !0,
  setShowReportConfigColumn: u
}) {
  const [D, R] = x.useState(""), [y, A] = x.useState(null), [V, ce] = x.useState(null), [ne, pe] = x.useState(null), [c, $] = x.useState(null), [nl, Ee] = x.useState(null), ue = x.useRef(null), ie = x.useRef(!1), Le = x.useRef(null), g = x.useMemo(
    () => e && e.root ? M(e.root) : null,
    [e]
  ), _ = x.useMemo(
    () => ml(n, a),
    [n, a]
  ), X = x.useCallback(
    (s) => bl(s, i),
    [i]
  ), xe = x.useMemo(
    () => $l(a),
    [a]
  ), q = x.useMemo(
    () => qe(g, { allFields: _, valConfigs: a, data: o }),
    [g, _, a, o]
  ), F = x.useCallback((s) => {
    t((p) => {
      const w = p && typeof p == "object" ? p : {}, m = w.root ? M(w.root) : null, P = typeof s == "function" ? s(m) : s;
      return { ...w, root: P };
    });
  }, [t]), Q = x.useMemo(() => Rl(g), [g]), I = x.useMemo(() => Pe(g), [g]), se = x.useMemo(
    () => Ll(Q, y),
    [Q, y]
  );
  x.useMemo(
    () => Ol(Q, y),
    [Q, y]
  ), x.useMemo(
    () => _l(Q, y),
    [Q, y]
  );
  const U = x.useMemo(() => {
    if (!g || !y) return null;
    if (y.type === "node") {
      const s = te(g, y.path);
      return s ? { type: "node", node: s } : null;
    }
    if (y.type === "branch") {
      const s = Ae(g, y.nodePath, y.branchIndex);
      return s ? { type: "branch", branch: s } : null;
    }
    return null;
  }, [g, y]), we = x.useMemo(() => !V || !g ? null : Ae(g, V.nodePath, V.branchIndex), [V, g]), Oe = x.useCallback((s) => {
    if (!s || s.type !== "rowPath" || !g) return !1;
    const p = Array.isArray(s.nodePath) ? s.nodePath : [], w = te(g, p);
    if (!w) return !1;
    const m = Array.isArray(w.branches) ? w.branches : [], P = m.findIndex((oe) => Je(oe, s.field, s.value)), L = P >= 0 ? m[P] : null, he = L && L.child ? L.child : w.defaultChild || K(""), J = Z(s.label || s.value);
    return $({
      request: s,
      nodePath: p,
      branchIndex: P,
      rowLabel: J,
      title: L && L.label ? L.label : J,
      child: Ie(he) || K(""),
      selectedPath: [],
      childLevelNumber: Number.isFinite(Number(s.levelNumber)) ? Number(s.levelNumber) + 1 : 2
    }), A({ type: "node", path: p }), !0;
  }, [g]);
  x.useEffect(() => {
    if (!g) {
      y !== null && A(null);
      return;
    }
    if (y) {
      if (y.type === "node") {
        te(g, y.path) || A({ type: "node", path: [] });
        return;
      }
      Ae(g, y.nodePath, y.branchIndex) || A({ type: "node", path: y.nodePath });
    }
  }, [g, y]);
  const fe = x.useCallback(() => {
    const s = ue.current;
    !s || typeof s.scrollIntoView != "function" || window.requestAnimationFrame(() => {
      s.scrollIntoView({ block: "start", behavior: "smooth" });
    });
  }, []);
  x.useEffect(() => {
    if (C && Le.current !== C) {
      if (Le.current = C, ie.current = !0, C.type === "rowPath") {
        Oe(C) || (ie.current = !1);
        return;
      }
      if (G(C) === G(y)) {
        ie.current = !1, fe();
        return;
      }
      A(C);
    }
  }, [Oe, C, fe, y]), x.useEffect(() => {
    ie.current && (ie.current = !1, fe());
  }, [fe, y]);
  const _e = x.useCallback((s, p) => {
    F((w) => O(w, s, () => p));
  }, [F]), We = x.useCallback((s, p, w) => {
    F((m) => be(m, s, p, () => w));
  }, [F]), il = x.useCallback((s) => {
    const p = K(s);
    F(p), $(null), A({ type: "node", path: [] });
  }, [F]);
  x.useCallback((s, p) => {
    p || F((w) => O(w, s, (m) => ({
      ...m,
      defaultChild: K("")
    }))), A({ type: "node", path: [...s, W] });
  }, [F]), x.useCallback((s) => {
    F((p) => O(p, s, (w) => ({
      ...w,
      defaultChild: null
    }))), A({ type: "node", path: s });
  }, [F]);
  const sl = x.useCallback((s, p, w) => {
    w || F((m) => be(m, s, p, (P) => ({
      ...P,
      child: K(_[0] || "")
    }))), A({ type: "node", path: [...s, Ne(p)] });
  }, [_, F]), ol = x.useCallback((s, p) => {
    F((w) => be(w, s, p, (m) => ({
      ...m,
      child: null
    }))), A({ type: "branch", nodePath: s, branchIndex: p });
  }, [F]), al = x.useCallback((s) => {
    if (s.length === 0) {
      F(null), $(null), A(null);
      return;
    }
    const p = s.slice(0, -1), w = s[s.length - 1];
    if (w === W) {
      F((P) => O(P, p, (L) => ({
        ...L,
        defaultChild: null
      }))), A({ type: "node", path: p });
      return;
    }
    const m = je(w);
    m >= 0 && (F((P) => be(P, p, m, (L) => ({
      ...L,
      child: null
    }))), A({ type: "branch", nodePath: p, branchIndex: m }));
  }, [F]), dl = x.useCallback((s, p) => {
    F((w) => O(w, s, (m) => ({
      ...m,
      branches: m.branches.filter((P, L) => L !== p)
    }))), A({ type: "node", path: s });
  }, [F]), cl = x.useCallback((s) => {
    $(null), A({ type: "node", path: s });
  }, []), ze = x.useCallback((s) => {
    if (s) {
      if ($(null), s.kind === "branch") {
        A({ type: "branch", nodePath: s.nodePath, branchIndex: s.branchIndex });
        return;
      }
      A({ type: "node", path: s.nodePath });
    }
  }, []), pl = x.useCallback(() => {
    if (!g || !I.length) return;
    const s = I[I.length - 1];
    if (!s || !s.node || !s.node.field) return;
    const p = [...s.path, W];
    $(null), F((w) => O(w, s.path, (m) => ({
      ...m,
      defaultChild: m.defaultChild || K("")
    }))), A({ type: "node", path: p });
  }, [I, g, F]), ul = x.useCallback((s, p) => {
    s === null || s === p || (F((w) => Ue(w, s, p)), A({ type: "node", path: Fe(p) }), Ee(null));
  }, [F]), xl = x.useCallback(() => {
    $(null), A(null);
  }, []), fl = x.useCallback(() => {
    if (!q.valid) {
      d && d(q.errors[0] || "Fix report validation errors before applying.", "error");
      return;
    }
    A(null);
  }, [q, d]), hl = x.useCallback(() => {
    if (!c || !c.request) return;
    const s = c.request, p = Array.isArray(c.nodePath) ? c.nodePath : [], w = El(c.child), m = {
      ...Nl(s.field, s.value, c.title || c.rowLabel, ""),
      label: Z(c.title || c.rowLabel || s.value),
      sourceRowPath: Array.isArray(s.pathValues) ? s.pathValues.map(String).filter(Boolean) : [],
      sourcePathFields: Array.isArray(s.pathFields) ? s.pathFields.map(String).filter(Boolean) : [],
      child: w
    }, P = (J) => {
      const oe = Array.isArray(J.branches) ? J.branches : [], Me = oe.findIndex((ye) => Je(ye, s.field, s.value));
      return Me >= 0 ? {
        ...J,
        branches: oe.map((ye, yl) => yl === Me ? { ...m, _id: ye._id || m._id } : ye)
      } : {
        ...J,
        branches: [...oe, m]
      };
    }, L = O(g, p, P), he = qe(L, { allFields: _, valConfigs: a, data: o });
    if (!he.valid) {
      d && d(he.errors[0] || "Fix report validation errors before applying.", "error");
      return;
    }
    F((J) => O(J, p, P)), $(null), A(null), d && d(`Applied settings for ${c.rowLabel || "row"}`, "info");
  }, [_, o, g, c, F, d, a]), Te = x.useCallback(() => {
    if (!g || !g.field) return;
    if (!q.valid) {
      d && d(q.errors[0] || "Fix report validation errors before saving.", "error");
      return;
    }
    const s = `report-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`, p = D.trim() || `Report ${(B || []).length + 1}`;
    N([
      ...B || [],
      {
        id: s,
        name: p,
        reportDef: JSON.parse(JSON.stringify(e)),
        layout: gl(f, a),
        createdAt: (/* @__PURE__ */ new Date()).toISOString()
      }
    ]), k(s), R(""), d && d(`Report "${p}" saved`, "info");
  }, [f, a, e, D, q, g, B, k, N, d]);
  return /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "18px", padding: "2px 0 8px" }, children: [
    (B || []).length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsxs("div", { style: E(r), children: [
        /* @__PURE__ */ l.jsx(j.Save, {}),
        " Saved Reports"
      ] }),
      /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: (B || []).map((s) => /* @__PURE__ */ l.jsxs(
        "div",
        {
          onClick: () => {
            vl(s, { setReportDef: t, setActiveReportId: k, setColFields: S, setValConfigs: b }), $(null), A({ type: "node", path: [] });
          },
          style: {
            display: "flex",
            alignItems: "center",
            gap: "8px",
            padding: "8px 10px",
            borderRadius: "10px",
            cursor: "pointer",
            background: s.id === h ? r.select || `${r.primary || "#2563EB"}14` : r.hover || "#F9FAFB",
            border: `1px solid ${s.id === h ? r.primary || "#2563EB" : r.border}`
          },
          children: [
            /* @__PURE__ */ l.jsx(j.Report, {}),
            /* @__PURE__ */ l.jsx("div", { style: { flex: 1, minWidth: 0, fontSize: "12px", fontWeight: 700, color: r.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: s.name }),
            /* @__PURE__ */ l.jsx(
              "button",
              {
                type: "button",
                onClick: (p) => {
                  p.stopPropagation(), N((w) => (w || []).filter((m) => m.id !== s.id)), h === s.id && k(null);
                },
                style: { background: "none", border: "none", cursor: "pointer", color: "#B91C1C", padding: "2px", flexShrink: 0 },
                children: /* @__PURE__ */ l.jsx(j.Delete, {})
              }
            )
          ]
        },
        s.id
      )) })
    ] }),
    /* @__PURE__ */ l.jsx(
      le,
      {
        theme: r,
        sectionId: "report-levels",
        title: "Report Levels",
        description: "Default hierarchy applied to every row unless a row override changes its children.",
        icon: /* @__PURE__ */ l.jsx(j.ReportLevel, {}),
        action: g && /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end" }, children: [
          typeof u == "function" && /* @__PURE__ */ l.jsxs(
            "button",
            {
              type: "button",
              onClick: () => u((s) => s === !1),
              style: z(r, v !== !1, !1),
              title: v !== !1 ? "Hide row gear column" : "Show row gear column",
              children: [
                /* @__PURE__ */ l.jsx(j.Settings, {}),
                " ",
                v !== !1 ? "Hide row gears" : "Show row gears"
              ]
            }
          ),
          /* @__PURE__ */ l.jsxs(
            "button",
            {
              type: "button",
              onClick: pl,
              disabled: !I.length || !I[I.length - 1].node.field,
              style: {
                ...z(r, !1, !1),
                opacity: !I.length || !I[I.length - 1].node.field ? 0.5 : 1,
                pointerEvents: !I.length || !I[I.length - 1].node.field ? "none" : "auto"
              },
              children: [
                /* @__PURE__ */ l.jsx(j.Add, {}),
                " Add next level"
              ]
            }
          )
        ] }),
        children: g ? /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: I.map((s) => /* @__PURE__ */ l.jsx(
          rl,
          {
            item: s,
            selected: !c && G(y) === G({ type: "node", path: s.path }),
            theme: r,
            sortByOptions: xe,
            getFieldLabel: X,
            onSelect: () => cl(s.path),
            onDragStart: (p) => {
              Ee(s.index), p.dataTransfer.effectAllowed = "move";
            },
            onDragOver: (p) => p.preventDefault(),
            onDrop: (p) => {
              p.preventDefault(), ul(nl, s.index);
            }
          },
          `default-level:${s.index}:${s.node._id}`
        )) }) : /* @__PURE__ */ l.jsxs("div", { style: de(r), children: [
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: r.text }, children: "Level 1" }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: E(r), children: "Break Down By" }),
            /* @__PURE__ */ l.jsxs(
              "select",
              {
                value: "",
                onChange: (s) => {
                  s.target.value && il(s.target.value);
                },
                style: H(r),
                children: [
                  /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                  _.map((s) => /* @__PURE__ */ l.jsx("option", { value: s, children: X(s) }, s))
                ]
              }
            )
          ] })
        ] })
      }
    ),
    g && /* @__PURE__ */ l.jsx(
      le,
      {
        theme: r,
        sectionId: "report-outline",
        title: "Report Outline",
        description: "Default levels plus every row override, shown as a separate map of Default children.",
        icon: /* @__PURE__ */ l.jsx(j.Report, {}),
        children: /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: Q.map((s) => s.kind === "branch" ? /* @__PURE__ */ l.jsx(
          Hl,
          {
            item: s,
            selected: !c && G(y) === G({
              type: "branch",
              nodePath: s.nodePath,
              branchIndex: s.branchIndex
            }),
            theme: r,
            onSelect: () => ze(s)
          },
          `outline-branch:${ee(s.nodePath)}:${s.branchIndex}:${s.branch._id}`
        ) : /* @__PURE__ */ l.jsx(
          Ml,
          {
            item: s,
            selected: !c && G(y) === G({ type: "node", path: s.nodePath }),
            theme: r,
            sortByOptions: xe,
            getFieldLabel: X,
            onSelect: () => ze(s)
          },
          `outline-node:${ee(s.nodePath)}:${s.node._id}`
        )) })
      }
    ),
    g && c && /* @__PURE__ */ l.jsx("div", { ref: ue, children: /* @__PURE__ */ l.jsx(
      le,
      {
        theme: r,
        sectionId: "selected-row",
        title: "Selected Row",
        description: "Configure Custom children for the row gear you clicked.",
        icon: /* @__PURE__ */ l.jsx(j.Settings, {}),
        children: /* @__PURE__ */ l.jsx(
          ql,
          {
            draft: c,
            allFields: _,
            getFieldLabel: X,
            sortByOptions: xe,
            theme: r,
            onChange: $,
            onApply: hl,
            onCancel: xl
          }
        )
      }
    ) }),
    g && !c && y && U && U.type === "node" && /* @__PURE__ */ l.jsx("div", { ref: ue, children: /* @__PURE__ */ l.jsx(
      le,
      {
        theme: r,
        sectionId: "selected-level",
        title: "Selected Level",
        description: "Edit the selected default level or override child level.",
        icon: /* @__PURE__ */ l.jsx(j.Edit, {}),
        children: /* @__PURE__ */ l.jsx(
          Vl,
          {
            item: I.find((s) => Ce(s.path, y.path)) || (se ? { ...se, path: se.nodePath } : { path: y.path, levelNumber: y.path.length + 1 }),
            node: U.node,
            allFields: _,
            getFieldLabel: X,
            sortByOptions: xe,
            theme: r,
            onUpdate: (s) => _e(y.path, s),
            onApply: fl,
            onRemove: () => al(y.path),
            onEditFilter: () => pe(y.path),
            titleLabel: "Title"
          }
        )
      }
    ) }),
    g && !c && y && U && U.type === "branch" && se && /* @__PURE__ */ l.jsx("div", { ref: ue, children: /* @__PURE__ */ l.jsx(
      le,
      {
        theme: r,
        sectionId: "row-override",
        title: "Row Override",
        description: "Override applies to this row only. Configure its rule and Custom children.",
        icon: /* @__PURE__ */ l.jsx(j.Settings, {}),
        children: /* @__PURE__ */ l.jsx(
          Jl,
          {
            item: se,
            branch: U.branch,
            theme: r,
            onUpdate: (s) => We(y.nodePath, y.branchIndex, s),
            onEditCondition: () => ce({ nodePath: y.nodePath, branchIndex: y.branchIndex }),
            onOpenCustomChildren: () => sl(y.nodePath, y.branchIndex, !!U.branch.child),
            onRemoveCustomChildren: () => ol(y.nodePath, y.branchIndex),
            onRemove: () => dl(y.nodePath, y.branchIndex)
          }
        )
      }
    ) }),
    /* @__PURE__ */ l.jsxs(
      le,
      {
        theme: r,
        sectionId: "save-report",
        title: "Save Report",
        description: "Store the current report levels and row overrides.",
        icon: /* @__PURE__ */ l.jsx(j.Save, {}),
        children: [
          /* @__PURE__ */ l.jsx(zl, { validation: q, theme: r }),
          /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px" }, children: [
            /* @__PURE__ */ l.jsx(
              "input",
              {
                value: D,
                onChange: (s) => R(s.target.value),
                onKeyDown: (s) => {
                  s.key === "Enter" && Te();
                },
                placeholder: "Report name...",
                style: { ...H(r), flex: 1 }
              }
            ),
            /* @__PURE__ */ l.jsxs(
              "button",
              {
                type: "button",
                onClick: Te,
                disabled: !g || !g.field || !q.valid,
                style: {
                  display: "inline-flex",
                  alignItems: "center",
                  gap: "6px",
                  background: r.primary || "#2563EB",
                  border: "none",
                  borderRadius: "9px",
                  padding: "8px 14px",
                  fontSize: "12px",
                  fontWeight: 800,
                  color: "#fff",
                  cursor: "pointer",
                  opacity: !g || !g.field || !q.valid ? 0.5 : 1
                },
                children: [
                  /* @__PURE__ */ l.jsx(j.Save, {}),
                  " Save"
                ]
              }
            )
          ] })
        ]
      }
    ),
    V && we && /* @__PURE__ */ l.jsx(
      Be,
      {
        condition: we.condition,
        allFields: _,
        getFieldLabel: X,
        theme: r,
        onSave: (s) => {
          We(V.nodePath, V.branchIndex, {
            ...we,
            condition: s
          });
        },
        onClose: () => ce(null)
      }
    ),
    ne && (() => {
      const s = te(g, ne);
      return s ? /* @__PURE__ */ l.jsx(
        Be,
        {
          title: "Edit level filter",
          description: "Rows that don't match are grouped into 'Others' to keep totals intact. Applies to this level and all its children.",
          condition: s.filters || De(),
          allFields: _,
          getFieldLabel: X,
          theme: r,
          onSave: (p) => {
            _e(ne, {
              ...s,
              filters: p.clauses.length > 0 ? p : null
            });
          },
          onClose: () => pe(null)
        }
      ) : null;
    })()
  ] });
}
export {
  Yl as ReportEditor
};
