import { ac as gl, an as vl, j as l, I as S, ao as jl, T as X, ae as Sl, $ as T, ap as Cl } from "./index-D4WX8QbF.js";
import { a as x } from "./charts-DthW1Q8W.js";
const ne = () => `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`, Ge = [
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
], de = /* @__PURE__ */ new Set(["in", "not_in"]), wl = {
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
}, kl = /* @__PURE__ */ new Set([
  "depth",
  "_id",
  "_path",
  "_levelLabel",
  "_levelField",
  "_pathFields",
  "_has_children",
  "_is_expanded",
  "__col_schema"
]), W = "default", me = "branch:", Ye = ["", "solid", "dashed", "dotted", "double"], Ze = (e = "") => ({ _id: ne(), field: e, operator: "eq", value: "", values: [] }), Re = () => ({ op: "AND", clauses: [] }), Al = () => ({ _id: ne(), label: "", condition: Re(), child: null }), ve = (e = {}) => {
  const t = e && typeof e == "object" ? e : {}, n = Number(t.indent), i = Number(t.borderWidth), r = typeof t.borderStyle == "string" && Ye.includes(t.borderStyle) ? t.borderStyle : "";
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
}, J = (e = "") => ({
  _id: ne(),
  field: e,
  label: "",
  topN: null,
  sortBy: null,
  sortDir: "desc",
  sortAbs: !1,
  filters: null,
  format: ve(),
  defaultChild: null,
  branches: []
}), ml = (e) => !e || typeof e.field != "string" || !e.field ? null : {
  _id: typeof e._id == "string" ? e._id : ne(),
  field: e.field,
  operator: Ge.some((t) => t.value === e.operator) ? e.operator : "eq",
  value: e.value !== void 0 && e.value !== null ? String(e.value) : "",
  values: Array.isArray(e.values) ? e.values.map(String).filter(Boolean) : []
}, Ne = (e) => ({
  op: e && e.op === "OR" ? "OR" : "AND",
  clauses: Array.isArray(e && e.clauses) ? e.clauses.map(ml).filter(Boolean) : []
}), $e = (e) => !e || typeof e != "object" ? null : {
  _id: typeof e._id == "string" ? e._id : ne(),
  label: typeof e.label == "string" ? e.label : "",
  condition: Ne(e.condition),
  sourceRowPath: Array.isArray(e.sourceRowPath) ? e.sourceRowPath.map(String).filter(Boolean) : [],
  sourcePathFields: Array.isArray(e.sourcePathFields) ? e.sourcePathFields.map(String).filter(Boolean) : [],
  child: e.child ? M(e.child) : null
};
function M(e) {
  if (!e || typeof e != "object") return null;
  let t = Array.isArray(e.branches) ? e.branches.map($e).filter(Boolean) : [];
  t.length === 0 && e.childrenByValue && typeof e.childrenByValue == "object" && (t = Object.entries(e.childrenByValue).filter(([i]) => i).map(([i, r]) => $e({
    label: i,
    condition: { op: "AND", clauses: [{ field: e.field || "", operator: "eq", value: i, values: [] }] },
    child: r
  })).filter(Boolean));
  const n = e.filters && typeof e.filters == "object" ? Ne(e.filters) : null;
  return {
    _id: typeof e._id == "string" ? e._id : ne(),
    field: typeof e.field == "string" ? e.field : "",
    label: typeof e.label == "string" ? e.label : "",
    topN: Number.isFinite(Number(e.topN)) && Number(e.topN) > 0 ? Math.floor(Number(e.topN)) : null,
    sortBy: typeof e.sortBy == "string" && e.sortBy.trim() ? e.sortBy.trim() : null,
    sortDir: e.sortDir === "asc" ? "asc" : "desc",
    sortAbs: e.sortAbs === !0,
    filters: n && n.clauses.length > 0 ? n : null,
    format: ve(e.format),
    defaultChild: e.defaultChild ? M(e.defaultChild) : null,
    branches: t
  };
}
const Ie = (e) => `${me}${e}`, Se = (e) => {
  if (typeof e != "string" || !e.startsWith(me)) return -1;
  const t = parseInt(e.slice(me.length), 10);
  return Number.isFinite(t) ? t : -1;
}, $l = (e, t) => {
  const n = new Set(
    (t || []).filter(
      /** @param {any} [config] @returns {any} */
      (i) => i && i.field && i.agg
    ).map(
      /** @param {any} [config] @returns {any} */
      (i) => i.agg === "formula" ? i.field : `${i.field}_${i.agg}`
    )
  );
  return (e || []).filter(
    /** @param {any} [field] @returns {any} */
    (i) => i && typeof i == "string" && !kl.has(i) && (!i.startsWith("_") || Sl(i)) && !n.has(i)
  );
}, Fl = (e) => {
  const t = [
    { value: "", label: "(default)" },
    { value: "__field__", label: "Field value (A→Z / Z→A)" }
  ];
  return (e || []).forEach(
    /** @param {any} [config] @returns {any} */
    (n) => {
      if (!n || !n.field || !n.agg) return;
      const i = n.agg === "formula" ? n.field : `${n.field}_${n.agg}`, r = n.agg === "formula" ? n.label || T(n.field) : `${T(n.field)} (${Cl(n.agg, n.weightField)})`;
      t.push({ value: i, label: r });
    }
  ), t;
}, Ce = (e) => !e || !e.clauses || e.clauses.length === 0 ? "No filter" : e.clauses.map(
  /** @param {any} [clause] @returns {any} */
  (t) => {
    const n = T(t.field), i = wl[t.operator] || t.operator;
    if (de.has(t.operator)) {
      const r = t.values.slice(0, 3), o = t.values.length > 3 ? ` +${t.values.length - 3}` : "";
      return `${n} ${i} [${r.join(", ")}${o}]`;
    }
    return `${n} ${i} ${t.value || "..."}`;
  }
).join(` ${e.op} `), Xe = (e, t) => {
  if (!e) return "";
  const n = (t || []).find(
    /** @param {any} [item] @returns {any} */
    (i) => i.value === e
  );
  return n ? n.label : T(e);
}, Qe = (e) => e ? e.label && e.label.trim() ? e.label.trim() : e.field ? T(e.field) : "Select field" : "Select field", Bl = (e) => !e || !e.field ? !1 : de.has(e.operator) ? Array.isArray(e.values) && e.values.length > 0 : e.value !== void 0 && e.value !== null && String(e.value).trim() !== "", Fe = (e) => !!(e && Array.isArray(e.clauses) && e.clauses.length > 0) && e.clauses.every(Bl), Ve = (e, t) => {
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
}, qe = (e, t) => {
  if (!Fe(e) || !Array.isArray(t) || t.length === 0) return !0;
  const n = t.filter((r) => r && typeof r == "object" && !r._isTotal && !r._isOther);
  return n.some(
    /** @param {any} [row] @returns {any} */
    (r) => e.clauses.every(
      /** @param {any} [clause] @returns {any} */
      (o) => Object.prototype.hasOwnProperty.call(r, o.field)
    )
  ) ? n.some(
    /** @param {any} [row] @returns {any} */
    (r) => e.op === "OR" ? e.clauses.some(
      /** @param {any} [clause] @returns {any} */
      (o) => Ve(o, r)
    ) : e.clauses.every(
      /** @param {any} [clause] @returns {any} */
      (o) => Ve(o, r)
    )
  ) : !0;
}, Dl = (e, t) => {
  if (!Array.isArray(e) || e.length === 0 || !Array.isArray(t) || t.length === 0) return !0;
  const n = e.map(String).join("|||");
  return t.some((i) => i && typeof i == "object" && i._path === n);
}, Ke = (e, {
  allFields: t = [],
  valConfigs: n = [],
  data: i = []
} = {}) => {
  const r = [], o = [], a = (n || []).some(
    /** @param {any} [config] @returns {any} */
    (h) => h && h.field && h.agg
  ), f = new Set(Array.isArray(t) ? t : []), C = /* @__PURE__ */ new Map(), b = Array.isArray(i) ? i : [], B = (h, A) => {
    const d = h.trim().toLowerCase();
    if (d) {
      if (C.has(d)) {
        r.push(`Duplicate header "${h}" at ${A}; already used at ${C.get(d)}.`);
        return;
      }
      C.set(d, A);
    }
  }, N = (h, A, d = /* @__PURE__ */ new Set()) => {
    if (!h) return;
    const v = h._id || `${A}:${h.field || ""}`;
    if (d.has(v)) {
      r.push(`Circular row override detected at ${A}.`);
      return;
    }
    const j = new Set(d);
    j.add(v), h.field ? f.size > 0 && !f.has(h.field) && r.push(`Field "${T(h.field)}" at ${A} is no longer available.`) : r.push(`Missing field at ${A}.`), B(Qe(h), A), h.topN && !a && r.push(`Top-N at ${A} needs at least one measure selected.`), h.filters && !Fe(h.filters) ? r.push(`Level filter at ${A} has incomplete rules.`) : h.filters && !qe(h.filters, b) && r.push(`Level filter at ${A} does not match any loaded rows.`), h.defaultChild && N(h.defaultChild, `${A} > Default children`, j), (h.branches || []).forEach(
      /** @param {any} [branch] @param {any} [index] @returns {any} */
      (u, D) => {
        const R = u.label && u.label.trim() ? u.label.trim() : `Row override ${D + 1}`, y = `${A} > ${R}`;
        Fe(u.condition) ? qe(u.condition, b) || r.push(`Row override "${R}" does not match any loaded rows.`) : r.push(`Row override "${R}" needs a complete row rule.`), Array.isArray(u.sourcePathFields) && u.sourcePathFields.length > 0 && u.sourcePathFields[u.sourcePathFields.length - 1] !== h.field && r.push(`Row override "${R}" was created for another level and no longer points to this row path.`), Dl(u.sourceRowPath, b) || o.push(`Row override "${R}" points to a row path that is not in the current loaded rows.`), u.child && N(u.child, `${y} > Custom children`, j), u.child || o.push(`Row override "${R}" has no Custom children; it only renames the matching row.`);
      }
    );
  };
  return e ? N(e, "Level 1") : r.push("Report needs at least Level 1."), { errors: r, warnings: o, valid: r.length === 0 };
}, Rl = (e) => !e || !e.branchBadge ? `Level ${e ? e.levelNumber : 1}` : e.branchDepth === 0 ? `Override children ${e.branchBadge}` : `Override child level ${e.branchBadge}.${e.branchDepth + 1}`, re = (e, t) => {
  let n = e;
  for (let i = 0; n && i < t.length; i += 1) {
    const r = t[i];
    if (r === W)
      n = n.defaultChild || null;
    else {
      const o = Se(r);
      n = o >= 0 && n.branches[o] ? n.branches[o].child : null;
    }
  }
  return n || null;
}, Ae = (e, t, n) => {
  const i = re(e, t);
  return i && i.branches[n] ? i.branches[n] : null;
}, z = (e, t, n) => {
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
    const f = Se(a);
    return f < 0 || f >= r.branches.length ? r : M({
      ...r,
      branches: r.branches.map(
        /** @param {any} [branch] @param {any} [index] @returns {any} */
        (C, b) => b === f ? { ...C, child: i(C.child, o + 1) } : C
      )
    });
  };
  return i(e, 0);
}, ge = (e, t, n, i) => e && z(
  e,
  t,
  /** @param {any} [node] @returns {any} */
  (r) => n < 0 || n >= r.branches.length ? r : {
    ...r,
    branches: r.branches.map(
      /** @param {any} [branch] @param {any} [index] @returns {any} */
      (o, a) => a === n ? $e(i(o)) : o
    )
  }
), Nl = (e) => {
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
    }), i.branches.forEach(
      /** @param {any} [branch] @param {any} [index] @returns {any} */
      (o, a) => {
        const f = `${r.levelNumber}.${a + 1}`;
        t.push({
          kind: "branch",
          branch: o,
          nodePath: r.nodePath,
          branchIndex: a,
          depth: r.depth + 1,
          badge: f
        }), o.child && n(o.child, {
          nodePath: [...r.nodePath, Ie(a)],
          depth: r.depth + 2,
          levelNumber: r.levelNumber + 1,
          branchBadge: f,
          branchDepth: 0
        });
      }
    ), i.defaultChild && n(i.defaultChild, {
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
}, le = (e = []) => e.join(">"), we = (e = [], t = []) => le(e) === le(t), Z = (e) => e == null ? "" : String(e), G = (e) => e ? e.type === "node" ? `node:${le(e.path)}` : e.type === "branch" ? `branch:${le(e.nodePath)}:${e.branchIndex}` : e.type === "rowPath" ? `rowPath:${le(e.nodePath)}:${e.field}:${Z(e.value)}` : "" : "", je = (e, t) => (e || []).find(
  /** @param {any} [item] @returns {any} */
  (n) => n.kind === "node" && we(n.nodePath, t)
) || null, Ue = (e, t, n) => (e || []).find(
  /** @param {any} [item] @returns {any} */
  (i) => i.kind === "branch" && we(i.nodePath, t) && i.branchIndex === n
) || null, Je = (e, t, n) => {
  if (!e || !t) return !1;
  const i = Z(n);
  return (e.condition && Array.isArray(e.condition.clauses) ? e.condition.clauses : []).some(
    /** @param {any} [clause] @returns {any} */
    (o) => !o || o.field !== t ? !1 : o.operator === "eq" || o.operator === "=" ? Z(o.value) === i : o.operator === "in" ? Array.isArray(o.values) && o.values.map(Z).includes(i) : !1
  );
}, Il = (e, t, n, i = "") => ({
  ...Al(),
  label: Z(n || t),
  condition: {
    op: "AND",
    clauses: [{
      ...Ze(e),
      operator: "eq",
      value: Z(t),
      values: []
    }]
  },
  child: J(i)
}), Pe = (e) => {
  if (!e) return null;
  try {
    return M(JSON.parse(JSON.stringify(e)));
  } catch {
    return M(e);
  }
}, Be = (e) => Array.from({ length: Math.max(0, e) }, () => W), Ee = (e, t = 1, n = []) => {
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
}, Pl = (e) => e ? { ...Pe(e), defaultChild: null } : null, El = (e) => {
  if (!Array.isArray(e) || e.length === 0) return null;
  let t = null;
  for (let n = e.length - 1; n >= 0; n -= 1)
    t = M({
      ...Pl(e[n]),
      defaultChild: t
    });
  return t;
}, el = (e, t, n) => {
  const i = Ee(e).map(
    /** @param {any} [item] @returns {any} */
    (a) => a.node
  );
  if (t < 0 || n < 0 || t >= i.length || n >= i.length || t === n)
    return e;
  const r = [...i], [o] = r.splice(t, 1);
  return r.splice(n, 0, o), El(r);
}, Ll = (e) => {
  const t = Pe(e);
  return !t || !t.field ? null : t;
}, zl = (e, t) => t ? t.type === "node" ? je(e, t.path) : t.type === "branch" ? Ue(e, t.nodePath, t.branchIndex) : null : null, Ol = (e, t) => {
  if (!t) return [];
  const n = [], i = (a) => {
    const f = je(e, a);
    f && n.push(f);
  }, r = (a, f) => {
    const C = Ue(e, a, f);
    C && n.push(C);
  }, o = (a) => {
    if (i([]), !Array.isArray(a) || a.length === 0) return;
    let f = [];
    a.forEach((C) => {
      if (C === W) {
        f = [...f, C], i(f);
        return;
      }
      const b = Se(C);
      b >= 0 && (r(f, b), f = [...f, C], i(f));
    });
  };
  return t.type === "node" ? (o(t.path), n) : (o(t.nodePath), r(t.nodePath, t.branchIndex), n);
}, Wl = (e, t) => {
  if (!t) return [];
  if (t.type === "node") {
    const i = [], r = je(e, [...t.path, W]);
    r && i.push(r);
    const o = (e || []).filter(
      /** @param {any} [item] @returns {any} */
      (a) => a.kind === "branch" && we(a.nodePath, t.path)
    ).sort(
      /** @param {any} [left] @param {any} [right] @returns {any} */
      (a, f) => a.branchIndex - f.branchIndex
    );
    return [...i, ...o];
  }
  const n = je(e, [...t.nodePath, Ie(t.branchIndex)]);
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
}), _ = (e, t = !1, n = !1) => ({
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
}), ce = (e) => ({
  border: `1px solid ${e.border}`,
  borderRadius: "14px",
  background: e.surfaceBg || e.background || "#fff",
  padding: "14px",
  display: "flex",
  flexDirection: "column",
  gap: "12px"
}), _l = (e) => ({
  borderTop: `2px solid ${e.textSec || "#334155"}`,
  paddingTop: "14px",
  display: "flex",
  flexDirection: "column",
  gap: "12px"
}), ll = (e, t, n) => ({
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
function te({
  theme: e,
  title: t,
  description: n,
  icon: i,
  action: r = null,
  sectionId: o,
  children: a
}) {
  return /* @__PURE__ */ l.jsxs("section", { "data-report-section": o || t, style: _l(e), children: [
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
function De({ condition: e, allFields: t, getFieldLabel: n = T, onSave: i, onClose: r, theme: o, title: a, description: f }) {
  const [C, b] = x.useState(() => Ne(e)), B = H(o), N = x.useCallback(() => {
    b(
      /** @param {any} [prev] @returns {any} */
      (d) => ({ ...d, clauses: [...d.clauses, Ze(t[0] || "")] })
    );
  }, [t]), h = x.useCallback((d, v) => {
    b(
      /** @param {any} [prev] @returns {any} */
      (j) => ({
        ...j,
        clauses: j.clauses.map(
          /** @param {any} [clause] @param {any} [clauseIndex] @returns {any} */
          (u, D) => D === d ? { ...u, ...v } : u
        )
      })
    );
  }, []), A = x.useCallback((d) => {
    b(
      /** @param {any} [prev] @returns {any} */
      (v) => ({
        ...v,
        clauses: v.clauses.filter(
          /** @param {any} [_] @param {any} [clauseIndex] @returns {any} */
          (j, u) => u !== d
        )
      })
    );
  }, []);
  return x.useEffect(() => {
    if (typeof r != "function") return;
    const d = (v) => {
      v.key === "Escape" && !v.defaultPrevented && (v.preventDefault(), r());
    };
    return window.addEventListener("keydown", d), () => window.removeEventListener("keydown", d);
  }, [r]), /* @__PURE__ */ l.jsx(
    "div",
    {
      className: "sp-dialog-backdrop",
      style: {
        position: "fixed",
        inset: 0,
        zIndex: 9999,
        background: "rgba(15, 23, 42, 0.38)",
        backdropFilter: "blur(6px)",
        WebkitBackdropFilter: "blur(6px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center"
      },
      onClick: r,
      children: /* @__PURE__ */ l.jsxs(
        "div",
        {
          className: "sp-dialog-surface",
          role: "dialog",
          "aria-modal": "true",
          style: {
            ...ce(o),
            width: "520px",
            maxWidth: "95vw",
            maxHeight: "85vh",
            overflowY: "auto",
            boxShadow: o.shadow || "0 24px 64px rgba(15, 23, 42, 0.26)"
          },
          onClick: (d) => d.stopPropagation(),
          children: [
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px" }, children: [
              /* @__PURE__ */ l.jsxs("div", { children: [
                /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, letterSpacing: "-0.01em", color: o.text }, children: a || "Edit row override rule" }),
                /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: o.textSec, marginTop: "2px" }, children: f || "Add one or more clauses to decide when this row override should be used." })
              ] }),
              /* @__PURE__ */ l.jsx("button", { type: "button", className: "sp-btn sp-btn-ghost", onClick: r, style: { background: "transparent", border: "none", borderRadius: "6px", cursor: "pointer", color: o.textSec, padding: "4px", display: "inline-flex", alignItems: "center" }, children: /* @__PURE__ */ l.jsx(S.Close, {}) })
            ] }),
            C.clauses.length > 1 && /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ l.jsx("span", { style: { fontSize: "11px", color: o.textSec, fontWeight: 700 }, children: "Match:" }),
              ["AND", "OR"].map((d) => /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => b(
                    /** @param {any} [prev] @returns {any} */
                    (v) => ({ ...v, op: d })
                  ),
                  style: {
                    padding: "5px 12px",
                    borderRadius: "999px",
                    fontSize: "11px",
                    fontWeight: 800,
                    border: "none",
                    cursor: "pointer",
                    background: C.op === d ? o.primary || "#2563EB" : o.hover || "#F3F4F6",
                    color: C.op === d ? "#fff" : o.textSec
                  },
                  children: d
                },
                d
              ))
            ] }),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: [
              C.clauses.length === 0 && /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: o.textSec, fontStyle: "italic" }, children: "No clauses means this row override matches every row at this level." }),
              C.clauses.map(
                /** @param {any} [clause] @param {any} [index] @returns {any} */
                (d, v) => /* @__PURE__ */ l.jsxs(
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
                      /* @__PURE__ */ l.jsx("div", { style: { width: "32px", paddingTop: "6px", textAlign: "center", fontSize: "10px", fontWeight: 800, color: o.textSec }, children: v > 0 ? C.op : "" }),
                      /* @__PURE__ */ l.jsxs("div", { style: { flex: 1, display: "flex", flexDirection: "column", gap: "8px" }, children: [
                        /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap" }, children: [
                          /* @__PURE__ */ l.jsx(
                            X,
                            {
                              size: "sm",
                              "aria-label": "Filter field",
                              value: d.field,
                              onChange: (j) => h(v, { field: j.target.value }),
                              style: { ...B, flex: 1, minWidth: "140px" },
                              children: t.map(
                                /** @param {any} [field] @returns {any} */
                                (j) => /* @__PURE__ */ l.jsx("option", { value: j, children: n(j) }, j)
                              )
                            }
                          ),
                          /* @__PURE__ */ l.jsx(
                            X,
                            {
                              size: "sm",
                              "aria-label": "Filter operator",
                              value: d.operator,
                              onChange: (j) => {
                                const u = j.target.value, D = de.has(u), R = de.has(d.operator);
                                h(v, {
                                  operator: u,
                                  values: D ? R ? d.values : d.value ? [d.value] : [] : d.values,
                                  value: D ? d.value : d.values[0] || d.value || ""
                                });
                              },
                              style: { ...B, width: "160px", flexShrink: 0 },
                              children: Ge.map((j) => /* @__PURE__ */ l.jsx("option", { value: j.value, children: j.label }, j.value))
                            }
                          )
                        ] }),
                        de.has(d.operator) ? /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: [
                          d.values.length > 0 && /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" }, children: d.values.map(
                            /** @param {any} [value] @param {any} [valueIndex] @returns {any} */
                            (j, u) => /* @__PURE__ */ l.jsxs(Y, { theme: o, tone: "primary", children: [
                              /* @__PURE__ */ l.jsx("span", { children: j }),
                              /* @__PURE__ */ l.jsx(
                                "button",
                                {
                                  type: "button",
                                  onClick: () => h(v, {
                                    values: d.values.filter(
                                      /** @param {any} [_] @param {any} [currentIndex] @returns {any} */
                                      (D, R) => R !== u
                                    )
                                  }),
                                  style: { background: "none", border: "none", padding: 0, cursor: "pointer", color: "inherit", lineHeight: 1 },
                                  children: "x"
                                }
                              )
                            ] }, `${d._id}-${u}`)
                          ) }),
                          /* @__PURE__ */ l.jsx(
                            "input",
                            {
                              placeholder: "Type a value and press Enter",
                              style: B,
                              onKeyDown: (
                                /** @param {any} event */
                                ((j) => {
                                  const u = j.target.value.trim();
                                  j.key === "Enter" && u && (d.values.includes(u) || h(v, { values: [...d.values, u] }), j.target.value = "", j.preventDefault());
                                })
                              )
                            }
                          )
                        ] }) : /* @__PURE__ */ l.jsx(
                          "input",
                          {
                            value: d.value,
                            onChange: (j) => h(v, { value: j.target.value }),
                            placeholder: "Value",
                            style: B
                          }
                        )
                      ] }),
                      /* @__PURE__ */ l.jsx(
                        "button",
                        {
                          type: "button",
                          onClick: () => A(v),
                          style: { background: "none", border: "none", cursor: "pointer", color: "#B91C1C", padding: "6px" },
                          children: /* @__PURE__ */ l.jsx(S.Delete, {})
                        }
                      )
                    ]
                  },
                  d._id
                )
              )
            ] }),
            /* @__PURE__ */ l.jsxs(
              "button",
              {
                type: "button",
                onClick: N,
                style: {
                  ..._(o),
                  width: "100%",
                  borderStyle: "dashed"
                },
                children: [
                  /* @__PURE__ */ l.jsx(S.Add, {}),
                  " Add clause"
                ]
              }
            ),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", justifyContent: "flex-end", gap: "8px", borderTop: `1px solid ${o.border}`, paddingTop: "12px" }, children: [
              /* @__PURE__ */ l.jsx("button", { type: "button", onClick: r, style: _(o), children: "Cancel" }),
              /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => {
                    i(C), r();
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
function tl({ value: e, onChange: t, theme: n }) {
  const i = ve(e), r = H(n), o = (a) => t(ve({ ...i, ...a }));
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
        /* @__PURE__ */ l.jsx(X, { size: "sm", "aria-label": "Border", value: i.borderStyle, onChange: (a) => o({ borderStyle: a.target.value }), style: { ...r, width: "100%" }, children: Ye.map((a) => /* @__PURE__ */ l.jsx("option", { value: a, children: a || "None" }, a || "none")) })
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
function Tl({ validation: e, theme: t }) {
  const n = e && Array.isArray(e.errors) ? e.errors : [], i = e && Array.isArray(e.warnings) ? e.warnings : [];
  return n.length === 0 && i.length === 0 ? null : /* @__PURE__ */ l.jsxs("div", { "data-report-validation": "true", style: { border: `1px solid ${n.length > 0 ? "#FCA5A5" : "#FDE68A"}`, borderRadius: "12px", padding: "10px 12px", background: n.length > 0 ? "#FEF2F2" : "#FFFBEB", color: n.length > 0 ? "#991B1B" : "#92400E", fontSize: "11px", lineHeight: 1.45, display: "flex", flexDirection: "column", gap: "6px" }, children: [
    n.length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: { fontWeight: 900, marginBottom: "4px" }, children: "Fix before applying or saving" }),
      n.map(
        /** @param {any} [message] @returns {any} */
        (r) => /* @__PURE__ */ l.jsx("div", { children: r }, `err:${r}`)
      )
    ] }),
    i.length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: { fontWeight: 900, marginBottom: "4px" }, children: "Warnings" }),
      i.map(
        /** @param {any} [message] @returns {any} */
        (r) => /* @__PURE__ */ l.jsx("div", { children: r }, `warn:${r}`)
      )
    ] })
  ] });
}
function Ml({ debug: e, theme: t }) {
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
    n.map(
      /** @param {any} p0 @returns {any} */
      ([i, r]) => /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", justifyContent: "space-between", gap: "10px", fontSize: "11px" }, children: [
        /* @__PURE__ */ l.jsx("span", { style: { color: t.textSec, fontWeight: 800 }, children: i }),
        /* @__PURE__ */ l.jsx("span", { style: { color: t.text, textAlign: "right", overflowWrap: "anywhere" }, children: r })
      ] }, i)
    )
  ] });
}
function rl({
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
            onClick: () => a(
              /** @param {any} [current] @returns {any} */
              (f) => !f
            ),
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
              /* @__PURE__ */ l.jsx("span", { style: { display: "inline-flex", color: n.textSec, flexShrink: 0 }, children: o ? /* @__PURE__ */ l.jsx(S.ChevronDown, {}) : /* @__PURE__ */ l.jsx(S.ChevronRight, {}) }),
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
function Hl({ item: e, selected: t, theme: n, sortByOptions: i, getFieldLabel: r = T, onSelect: o }) {
  const a = e.node, f = Rl(e), C = a.field ? r(a.field) : "Select field", b = Qe(a), B = Xe(a.sortBy, i);
  return /* @__PURE__ */ l.jsx("button", { type: "button", onClick: o, style: ll(n, t, e.depth), children: /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsxs("div", { style: { minWidth: 0, display: "flex", flexDirection: "column", gap: "7px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, tone: e.branchBadge ? "primary" : "neutral", children: f }),
        /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: C })
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
    /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(S.ChevronRight, {}) })
  ] }) });
}
function Vl({ item: e, selected: t, theme: n, onSelect: i }) {
  const r = e.branch, o = r.label && r.label.trim() ? r.label.trim() : "Display original label";
  return /* @__PURE__ */ l.jsx("button", { type: "button", onClick: i, style: ll(n, t, e.depth), children: /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsxs("div", { style: { minWidth: 0, display: "flex", flexDirection: "column", gap: "7px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, tone: "primary", children: `Row override ${e.badge}` }),
        /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: o })
      ] }),
      /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: n.textSec, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: Ce(r.condition) }),
      /* @__PURE__ */ l.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: /* @__PURE__ */ l.jsx(Y, { theme: n, children: r.child ? "Custom children" : "Default children" }) })
    ] }),
    /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(S.ChevronRight, {}) })
  ] }) });
}
function nl({
  item: e,
  selected: t,
  theme: n,
  sortByOptions: i,
  getFieldLabel: r = T,
  onSelect: o,
  onDragStart: a,
  onDragOver: f,
  onDrop: C
}) {
  const b = e.node, B = b.field ? r(b.field) : "Select field", N = b.label && b.label.trim() ? b.label.trim() : B, h = Xe(b.sortBy, i);
  return /* @__PURE__ */ l.jsxs(
    "button",
    {
      type: "button",
      draggable: !0,
      onDragStart: a,
      onDragOver: f,
      onDrop: C,
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
        /* @__PURE__ */ l.jsx("span", { style: { cursor: "grab", display: "inline-flex" }, children: /* @__PURE__ */ l.jsx(S.DragIndicator, {}) }),
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
        /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(S.ChevronRight, {}) })
      ]
    }
  );
}
function ql({
  item: e,
  node: t,
  allFields: n,
  getFieldLabel: i = T,
  sortByOptions: r,
  theme: o,
  onUpdate: a,
  onApply: f,
  onRemove: C,
  onEditFilter: b,
  titleLabel: B = "Title",
  canRemove: N = !0
}) {
  const h = H(o), A = e && e.levelNumber ? `Level ${e.levelNumber}` : "Level", d = !!(t.filters && t.filters.clauses && t.filters.clauses.length > 0), j = [
    t.field ? i(t.field) : "Select field",
    t.label && t.label.trim() ? `Title: ${t.label.trim()}` : null,
    t.topN ? `Top ${t.topN}` : null,
    d ? `${t.filters.clauses.length} filter${t.filters.clauses.length === 1 ? "" : "s"}` : null
  ].filter(Boolean);
  return /* @__PURE__ */ l.jsxs("div", { style: ce(o), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsx("div", { children: /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: o.text }, children: A }) }),
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
        N && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: C, style: _(o, !1, !0), children: [
          /* @__PURE__ */ l.jsx(S.Delete, {}),
          " ",
          e && e.path && e.path.length === 0 ? "Clear report" : "Remove level"
        ] })
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs(
      rl,
      {
        title: `${A} settings`,
        summary: j.join(" · "),
        theme: o,
        defaultOpen: !1,
        children: [
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: E(o), children: "Break Down By" }),
            /* @__PURE__ */ l.jsxs(
              X,
              {
                size: "sm",
                "aria-label": "Break down by",
                value: t.field,
                onChange: (u) => a({ ...t, field: u.target.value }),
                style: { ...h, width: "100%" },
                children: [
                  /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                  n.map(
                    /** @param {any} [field] @returns {any} */
                    (u) => /* @__PURE__ */ l.jsx("option", { value: u, children: i(u) }, u)
                  )
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
                X,
                {
                  size: "sm",
                  "aria-label": "Sort by",
                  value: t.sortBy || "",
                  onChange: (u) => a({ ...t, sortBy: u.target.value || null }),
                  style: { ...h, width: "100%" },
                  children: r.map(
                    /** @param {any} [option] @returns {any} */
                    (u) => /* @__PURE__ */ l.jsx("option", { value: u.value, children: u.label }, u.value)
                  )
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
                  children: t.sortDir === "asc" ? /* @__PURE__ */ l.jsx(S.SortAsc, {}) : /* @__PURE__ */ l.jsx(S.SortDesc, {})
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
                    /* @__PURE__ */ l.jsx(S.Filter, {}),
                    /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }, children: d ? Ce(t.filters) : "No filter (show all rows)" })
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
                  children: /* @__PURE__ */ l.jsx(S.Close, {})
                }
              )
            ] }),
            /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: o.textSec, marginTop: "6px", lineHeight: 1.4 }, children: `Rows that don't match the filter are grouped into "Others" to keep totals intact.` })
          ] }),
          /* @__PURE__ */ l.jsx(
            tl,
            {
              value: t.format,
              theme: o,
              onChange: (
                /** @param {any} [format] @returns {any} */
                ((u) => a({ ...t, format: u }))
              )
            }
          )
        ]
      }
    )
  ] });
}
function Kl({
  draft: e,
  allFields: t,
  getFieldLabel: n = T,
  sortByOptions: i,
  theme: r,
  onChange: o,
  onApply: a,
  onCancel: f
}) {
  const [C, b] = x.useState(null), [B, N] = x.useState(!1), h = e && e.child ? e.child : J(""), A = Array.isArray(e && e.selectedPath) ? e.selectedPath : [], d = re(h, A) || h, v = !!(d.filters && d.filters.clauses && d.filters.clauses.length > 0), j = Ee(h, e.childLevelNumber || 2), u = Math.max(0, A.length), D = (e && Number.isFinite(Number(e.childLevelNumber)) ? Number(e.childLevelNumber) : 2) + A.length, R = H(r), y = [
    d.field ? n(d.field) : "Select field",
    e.title && e.title.trim() ? `Title: ${e.title.trim()}` : null,
    d.topN ? `Top ${d.topN}` : null,
    v ? `${d.filters.clauses.length} filter${d.filters.clauses.length === 1 ? "" : "s"}` : null
  ].filter(Boolean), k = (c) => {
    o({
      ...e,
      child: z(h, A, () => c)
    });
  }, V = (c) => {
    o({
      ...e,
      selectedPath: Be(c)
    });
  }, pe = () => {
    const c = [...A, W];
    o({
      ...e,
      child: z(
        h,
        A,
        /** @param {any} [node] @returns {any} */
        ($) => ({
          ...$,
          defaultChild: $.defaultChild || J("")
        })
      ),
      selectedPath: c
    });
  }, ie = () => {
    o({
      ...e,
      child: z(
        h,
        A,
        /** @param {any} [node] @returns {any} */
        (c) => ({
          ...c,
          defaultChild: null
        })
      )
    });
  }, ue = (c, $) => {
    c === null || c === $ || (o({
      ...e,
      child: el(h, c, $),
      selectedPath: Be($)
    }), b(null));
  };
  return /* @__PURE__ */ l.jsxs(l.Fragment, { children: [
    /* @__PURE__ */ l.jsxs("div", { style: ce(r), children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
        /* @__PURE__ */ l.jsxs("div", { children: [
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: r.text }, children: e.rowLabel || "Selected row" }),
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: r.textSec, marginTop: "2px" }, children: "Override applies to this row only." })
        ] }),
        /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexShrink: 0 }, children: [
          /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: f, style: _(r), children: [
            /* @__PURE__ */ l.jsx(S.Close, {}),
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
      /* @__PURE__ */ l.jsx(Ml, { debug: e.request && e.request.debug, theme: r }),
      j.length > 1 && /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: j.map(
        /** @param {any} [item] @returns {any} */
        (c) => /* @__PURE__ */ l.jsx(
          nl,
          {
            item: c,
            selected: u === c.index,
            theme: r,
            sortByOptions: i,
            getFieldLabel: n,
            onSelect: () => V(c.index),
            onDragStart: (
              /** @param {any} [event] @returns {any} */
              (($) => {
                b(c.index), $.dataTransfer.effectAllowed = "move";
              })
            ),
            onDragOver: (
              /** @param {any} [event] @returns {any} */
              (($) => $.preventDefault())
            ),
            onDrop: (
              /** @param {any} [event] @returns {any} */
              (($) => {
                $.preventDefault(), ue(C, c.index);
              })
            )
          },
          `draft-level:${c.index}:${c.node._id}`
        )
      ) }),
      /* @__PURE__ */ l.jsxs(
        rl,
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
                X,
                {
                  size: "sm",
                  "aria-label": "Break down by",
                  value: d.field,
                  onChange: (c) => k({ ...d, field: c.target.value }),
                  style: { ...R, width: "100%" },
                  children: [
                    /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                    t.map(
                      /** @param {any} [field] @returns {any} */
                      (c) => /* @__PURE__ */ l.jsx("option", { value: c, children: n(c) }, c)
                    )
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
                      k({ ...d, topN: $ && $ > 0 ? $ : null });
                    },
                    placeholder: "All",
                    style: { ...R, textAlign: "center" }
                  }
                ),
                /* @__PURE__ */ l.jsx(
                  X,
                  {
                    size: "sm",
                    "aria-label": "Sort by",
                    value: d.sortBy || "",
                    onChange: (c) => k({ ...d, sortBy: c.target.value || null }),
                    style: { ...R, width: "100%" },
                    children: i.map(
                      /** @param {any} [option] @returns {any} */
                      (c) => /* @__PURE__ */ l.jsx("option", { value: c.value, children: c.label }, c.value)
                    )
                  }
                ),
                /* @__PURE__ */ l.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => k({ ...d, sortAbs: !d.sortAbs }),
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
                    onClick: () => k({ ...d, sortDir: d.sortDir === "asc" ? "desc" : "asc" }),
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
                    children: d.sortDir === "asc" ? /* @__PURE__ */ l.jsx(S.SortAsc, {}) : /* @__PURE__ */ l.jsx(S.SortDesc, {})
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
                      background: v ? `${r.primary || "#2563EB"}08` : r.hover || "#F8FAFC",
                      borderColor: v ? `${r.primary || "#2563EB"}45` : void 0
                    }),
                    children: [
                      /* @__PURE__ */ l.jsx(S.Filter, {}),
                      /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }, children: v ? Ce(d.filters) : "No filter (show all children)" })
                    ]
                  }
                ),
                v && /* @__PURE__ */ l.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => k({ ...d, filters: null }),
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
                    children: /* @__PURE__ */ l.jsx(S.Close, {})
                  }
                )
              ] }),
              /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: r.textSec, marginTop: "6px", lineHeight: 1.4 }, children: 'Filtered-out children are grouped into "Others" to keep totals intact.' })
            ] }),
            /* @__PURE__ */ l.jsx(
              tl,
              {
                value: d.format,
                theme: r,
                onChange: (
                  /** @param {any} [format] @returns {any} */
                  ((c) => k({ ...d, format: c }))
                )
              }
            ),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px" }, children: [
              /* @__PURE__ */ l.jsxs(
                "button",
                {
                  type: "button",
                  onClick: pe,
                  disabled: !d.field,
                  style: { ..._(r, !!d.defaultChild), opacity: d.field ? 1 : 0.5 },
                  children: [
                    /* @__PURE__ */ l.jsx(S.Add, {}),
                    " ",
                    d.defaultChild ? "Open next level" : "Add next level"
                  ]
                }
              ),
              d.defaultChild && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: ie, style: _(r, !1, !0), children: [
                /* @__PURE__ */ l.jsx(S.Delete, {}),
                " Remove next level"
              ] })
            ] })
          ]
        }
      )
    ] }),
    B && /* @__PURE__ */ l.jsx(
      De,
      {
        title: `Filter children of "${e.rowLabel || "this row"}"`,
        description: "Only children matching this filter are shown. Filtered-out rows are grouped into 'Others' to keep totals intact.",
        condition: d.filters || Re(),
        allFields: t,
        getFieldLabel: n,
        theme: r,
        onSave: (
          /** @param {any} [nextCondition] @returns {any} */
          ((c) => {
            k({
              ...d,
              filters: c.clauses.length > 0 ? c : null
            });
          })
        ),
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
  const C = H(n), b = Ce(t.condition);
  return /* @__PURE__ */ l.jsxs("div", { style: ce(n), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { children: [
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: n.text }, children: `Row override ${e.badge}` }),
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: n.textSec, marginTop: "2px" }, children: "Use a row override when matching rows need a different label or child breakdown." })
      ] }),
      /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: f, style: _(n, !1, !0), children: [
        /* @__PURE__ */ l.jsx(S.Delete, {}),
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
          style: C
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
            /* @__PURE__ */ l.jsx(S.Filter, {}),
            /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: b })
          ]
        }
      )
    ] }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px" }, children: [
      /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: o, style: _(n, !!t.child), children: [
        /* @__PURE__ */ l.jsx(S.Add, {}),
        " ",
        t.child ? "Open Custom children" : "Add Custom children"
      ] }),
      t.child && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: a, style: _(n, !1, !0), children: [
        /* @__PURE__ */ l.jsx(S.Delete, {}),
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
function Zl({
  reportDef: e,
  setReportDef: t,
  availableFields: n,
  customDimensions: i = [],
  theme: r,
  data: o = [],
  valConfigs: a,
  colFields: f,
  setColFields: C,
  setValConfigs: b,
  savedReports: B,
  setSavedReports: N,
  activeReportId: h,
  setActiveReportId: A,
  showNotification: d,
  requestedSelection: v = null,
  showReportConfigColumn: j = !0,
  setShowReportConfigColumn: u
}) {
  const [D, R] = x.useState(""), [y, k] = x.useState(null), [V, pe] = x.useState(null), [ie, ue] = x.useState(null), [c, $] = x.useState(null), [il, Le] = x.useState(null), xe = x.useRef(null), se = x.useRef(!1), ze = x.useRef(null), g = x.useMemo(
    () => e && e.root ? M(e.root) : null,
    [e]
  ), O = x.useMemo(
    () => $l(n, a),
    [n, a]
  ), Q = x.useCallback(
    (s) => gl(s, i),
    [i]
  ), fe = x.useMemo(
    () => Fl(a),
    [a]
  ), q = x.useMemo(
    () => Ke(g, { allFields: O, valConfigs: a, data: o }),
    [g, O, a, o]
  ), F = x.useCallback((s) => {
    t(
      /** @param {any} [prev] @returns {any} */
      (p) => {
        const w = p && typeof p == "object" ? p : {}, m = w.root ? M(w.root) : null, P = typeof s == "function" ? s(m) : s;
        return { ...w, root: P };
      }
    );
  }, [t]), U = x.useMemo(() => Nl(g), [g]), I = x.useMemo(() => Ee(g), [g]), oe = x.useMemo(
    () => zl(U, y),
    [U, y]
  );
  x.useMemo(
    () => Ol(U, y),
    [U, y]
  ), x.useMemo(
    () => Wl(U, y),
    [U, y]
  );
  const ee = x.useMemo(() => {
    if (!g || !y) return null;
    if (y.type === "node") {
      const s = re(g, y.path);
      return s ? { type: "node", node: s } : null;
    }
    if (y.type === "branch") {
      const s = Ae(g, y.nodePath, y.branchIndex);
      return s ? { type: "branch", branch: s } : null;
    }
    return null;
  }, [g, y]), ke = x.useMemo(() => !V || !g ? null : Ae(g, V.nodePath, V.branchIndex), [V, g]), Oe = x.useCallback((s) => {
    if (!s || s.type !== "rowPath" || !g) return !1;
    const p = Array.isArray(s.nodePath) ? s.nodePath : [], w = re(g, p);
    if (!w) return !1;
    const m = Array.isArray(w.branches) ? w.branches : [], P = m.findIndex(
      /** @param {any} [branch] @returns {any} */
      (ae) => Je(ae, s.field, s.value)
    ), L = P >= 0 ? m[P] : null, ye = L && L.child ? L.child : w.defaultChild || J(""), K = Z(s.label || s.value);
    return $({
      request: s,
      nodePath: p,
      branchIndex: P,
      rowLabel: K,
      title: L && L.label ? L.label : K,
      child: Pe(ye) || J(""),
      selectedPath: [],
      childLevelNumber: Number.isFinite(Number(s.levelNumber)) ? Number(s.levelNumber) + 1 : 2
    }), k({ type: "node", path: p }), !0;
  }, [g]);
  x.useEffect(() => {
    if (!g) {
      y !== null && k(null);
      return;
    }
    if (y) {
      if (y.type === "node") {
        re(g, y.path) || k({ type: "node", path: [] });
        return;
      }
      Ae(g, y.nodePath, y.branchIndex) || k({ type: "node", path: y.nodePath });
    }
  }, [g, y]);
  const he = x.useCallback(() => {
    const s = xe.current;
    !s || typeof s.scrollIntoView != "function" || window.requestAnimationFrame(() => {
      s.scrollIntoView({ block: "start", behavior: "smooth" });
    });
  }, []);
  x.useEffect(() => {
    if (v && ze.current !== v) {
      if (ze.current = v, se.current = !0, v.type === "rowPath") {
        Oe(v) || (se.current = !1);
        return;
      }
      if (G(v) === G(y)) {
        se.current = !1, he();
        return;
      }
      k(v);
    }
  }, [Oe, v, he, y]), x.useEffect(() => {
    se.current && (se.current = !1, he());
  }, [he, y]);
  const We = x.useCallback((s, p) => {
    F(
      /** @param {any} [prevRoot] @returns {any} */
      (w) => z(w, s, () => p)
    );
  }, [F]), _e = x.useCallback((s, p, w) => {
    F(
      /** @param {any} [prevRoot] @returns {any} */
      (m) => ge(m, s, p, () => w)
    );
  }, [F]), sl = x.useCallback((s) => {
    const p = J(s);
    F(p), $(null), k({ type: "node", path: [] });
  }, [F]);
  x.useCallback((s, p) => {
    p || F(
      /** @param {any} [prevRoot] @returns {any} */
      (w) => z(
        w,
        s,
        /** @param {any} [node] @returns {any} */
        (m) => ({
          ...m,
          defaultChild: J("")
        })
      )
    ), k({ type: "node", path: [...s, W] });
  }, [F]), x.useCallback((s) => {
    F(
      /** @param {any} [prevRoot] @returns {any} */
      (p) => z(
        p,
        s,
        /** @param {any} [node] @returns {any} */
        (w) => ({
          ...w,
          defaultChild: null
        })
      )
    ), k({ type: "node", path: s });
  }, [F]);
  const ol = x.useCallback((s, p, w) => {
    w || F(
      /** @param {any} [prevRoot] @returns {any} */
      (m) => ge(
        m,
        s,
        p,
        /** @param {any} [branch] @returns {any} */
        (P) => ({
          ...P,
          child: J(O[0] || "")
        })
      )
    ), k({ type: "node", path: [...s, Ie(p)] });
  }, [O, F]), al = x.useCallback((s, p) => {
    F(
      /** @param {any} [prevRoot] @returns {any} */
      (w) => ge(
        w,
        s,
        p,
        /** @param {any} [branch] @returns {any} */
        (m) => ({
          ...m,
          child: null
        })
      )
    ), k({ type: "branch", nodePath: s, branchIndex: p });
  }, [F]), dl = x.useCallback((s) => {
    if (s.length === 0) {
      F(null), $(null), k(null);
      return;
    }
    const p = s.slice(0, -1), w = s[s.length - 1];
    if (w === W) {
      F(
        /** @param {any} [prevRoot] @returns {any} */
        (P) => z(
          P,
          p,
          /** @param {any} [node] @returns {any} */
          (L) => ({
            ...L,
            defaultChild: null
          })
        )
      ), k({ type: "node", path: p });
      return;
    }
    const m = Se(w);
    m >= 0 && (F(
      /** @param {any} [prevRoot] @returns {any} */
      (P) => ge(
        P,
        p,
        m,
        /** @param {any} [branch] @returns {any} */
        (L) => ({
          ...L,
          child: null
        })
      )
    ), k({ type: "branch", nodePath: p, branchIndex: m }));
  }, [F]), cl = x.useCallback((s, p) => {
    F(
      /** @param {any} [prevRoot] @returns {any} */
      (w) => z(
        w,
        s,
        /** @param {any} [node] @returns {any} */
        (m) => ({
          ...m,
          branches: m.branches.filter(
            /** @param {any} [_] @param {any} [index] @returns {any} */
            (P, L) => L !== p
          )
        })
      )
    ), k({ type: "node", path: s });
  }, [F]), pl = x.useCallback((s) => {
    $(null), k({ type: "node", path: s });
  }, []), Te = x.useCallback((s) => {
    if (s) {
      if ($(null), s.kind === "branch") {
        k({ type: "branch", nodePath: s.nodePath, branchIndex: s.branchIndex });
        return;
      }
      k({ type: "node", path: s.nodePath });
    }
  }, []), ul = x.useCallback(() => {
    if (!g || !I.length) return;
    const s = I[I.length - 1];
    if (!s || !s.node || !s.node.field) return;
    const p = [...s.path, W];
    $(null), F(
      /** @param {any} [prevRoot] @returns {any} */
      (w) => z(
        w,
        s.path,
        /** @param {any} [node] @returns {any} */
        (m) => ({
          ...m,
          defaultChild: m.defaultChild || J("")
        })
      )
    ), k({ type: "node", path: p });
  }, [I, g, F]), xl = x.useCallback((s, p) => {
    s === null || s === p || (F(
      /** @param {any} [prevRoot] @returns {any} */
      (w) => el(w, s, p)
    ), k({ type: "node", path: Be(p) }), Le(null));
  }, [F]), fl = x.useCallback(() => {
    $(null), k(null);
  }, []), hl = x.useCallback(() => {
    if (!q.valid) {
      d && d(q.errors[0] || "Fix report validation errors before applying.", "error");
      return;
    }
    k(null);
  }, [q, d]), yl = x.useCallback(() => {
    if (!c || !c.request) return;
    const s = c.request, p = Array.isArray(c.nodePath) ? c.nodePath : [], w = Ll(c.child), m = {
      ...Il(s.field, s.value, c.title || c.rowLabel, ""),
      label: Z(c.title || c.rowLabel || s.value),
      sourceRowPath: Array.isArray(s.pathValues) ? s.pathValues.map(String).filter(Boolean) : [],
      sourcePathFields: Array.isArray(s.pathFields) ? s.pathFields.map(String).filter(Boolean) : [],
      child: w
    }, P = (K) => {
      const ae = Array.isArray(K.branches) ? K.branches : [], He = ae.findIndex(
        /** @param {any} [branch] @returns {any} */
        (be) => Je(be, s.field, s.value)
      );
      return He >= 0 ? {
        ...K,
        branches: ae.map(
          /** @param {any} [branch] @param {any} [index] @returns {any} */
          (be, bl) => bl === He ? { ...m, _id: be._id || m._id } : be
        )
      } : {
        ...K,
        branches: [...ae, m]
      };
    }, L = z(g, p, P), ye = Ke(L, { allFields: O, valConfigs: a, data: o });
    if (!ye.valid) {
      d && d(ye.errors[0] || "Fix report validation errors before applying.", "error");
      return;
    }
    F(
      /** @param {any} [prevRoot] @returns {any} */
      (K) => z(K, p, P)
    ), $(null), k(null), d && d(`Applied settings for ${c.rowLabel || "row"}`, "info");
  }, [O, o, g, c, F, d, a]), Me = x.useCallback(() => {
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
        layout: vl(f, a),
        createdAt: (/* @__PURE__ */ new Date()).toISOString()
      }
    ]), A(s), R(""), d && d(`Report "${p}" saved`, "info");
  }, [f, a, e, D, q, g, B, A, N, d]);
  return /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "18px", padding: "2px 0 8px" }, children: [
    (B || []).length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsxs("div", { style: E(r), children: [
        /* @__PURE__ */ l.jsx(S.Save, {}),
        " Saved Reports"
      ] }),
      /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: (B || []).map(
        /** @param {any} [report] @returns {any} */
        (s) => /* @__PURE__ */ l.jsxs(
          "div",
          {
            onClick: () => {
              jl(s, { setReportDef: t, setActiveReportId: A, setColFields: C, setValConfigs: b }), $(null), k({ type: "node", path: [] });
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
              /* @__PURE__ */ l.jsx(S.Report, {}),
              /* @__PURE__ */ l.jsx("div", { style: { flex: 1, minWidth: 0, fontSize: "12px", fontWeight: 700, color: r.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: s.name }),
              /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: (p) => {
                    p.stopPropagation(), N(
                      /** @param {any} [prev] @returns {any} */
                      (w) => (w || []).filter(
                        /** @param {any} [current] @returns {any} */
                        (m) => m.id !== s.id
                      )
                    ), h === s.id && A(null);
                  },
                  style: { background: "none", border: "none", cursor: "pointer", color: "#B91C1C", padding: "2px", flexShrink: 0 },
                  children: /* @__PURE__ */ l.jsx(S.Delete, {})
                }
              )
            ]
          },
          s.id
        )
      ) })
    ] }),
    /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "report-levels",
        title: "Report Levels",
        description: "Default hierarchy applied to every row unless a row override changes its children.",
        icon: /* @__PURE__ */ l.jsx(S.ReportLevel, {}),
        action: g && /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end" }, children: [
          typeof u == "function" && /* @__PURE__ */ l.jsxs(
            "button",
            {
              type: "button",
              onClick: () => u((s) => s === !1),
              style: _(r, j !== !1, !1),
              title: j !== !1 ? "Hide row gear column" : "Show row gear column",
              children: [
                /* @__PURE__ */ l.jsx(S.Settings, {}),
                " ",
                j !== !1 ? "Hide row gears" : "Show row gears"
              ]
            }
          ),
          /* @__PURE__ */ l.jsxs(
            "button",
            {
              type: "button",
              onClick: ul,
              disabled: !I.length || !I[I.length - 1].node.field,
              style: {
                ..._(r, !1, !1),
                opacity: !I.length || !I[I.length - 1].node.field ? 0.5 : 1,
                pointerEvents: !I.length || !I[I.length - 1].node.field ? "none" : "auto"
              },
              children: [
                /* @__PURE__ */ l.jsx(S.Add, {}),
                " Add next level"
              ]
            }
          )
        ] }),
        children: g ? /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: I.map(
          /** @param {any} [item] @returns {any} */
          (s) => /* @__PURE__ */ l.jsx(
            nl,
            {
              item: s,
              selected: !c && G(y) === G({ type: "node", path: s.path }),
              theme: r,
              sortByOptions: fe,
              getFieldLabel: Q,
              onSelect: () => pl(s.path),
              onDragStart: (
                /** @param {any} [event] @returns {any} */
                ((p) => {
                  Le(s.index), p.dataTransfer.effectAllowed = "move";
                })
              ),
              onDragOver: (
                /** @param {any} [event] @returns {any} */
                ((p) => p.preventDefault())
              ),
              onDrop: (
                /** @param {any} [event] @returns {any} */
                ((p) => {
                  p.preventDefault(), xl(il, s.index);
                })
              )
            },
            `default-level:${s.index}:${s.node._id}`
          )
        ) }) : /* @__PURE__ */ l.jsxs("div", { style: ce(r), children: [
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: r.text }, children: "Level 1" }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: E(r), children: "Break Down By" }),
            /* @__PURE__ */ l.jsxs(
              X,
              {
                size: "sm",
                "aria-label": "Level 1 field",
                value: "",
                onChange: (s) => {
                  s.target.value && sl(s.target.value);
                },
                style: { ...H(r), width: "100%" },
                children: [
                  /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                  O.map(
                    /** @param {any} [field] @returns {any} */
                    (s) => /* @__PURE__ */ l.jsx("option", { value: s, children: Q(s) }, s)
                  )
                ]
              }
            )
          ] })
        ] })
      }
    ),
    g && /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "report-outline",
        title: "Report Outline",
        description: "Default levels plus every row override, shown as a separate map of Default children.",
        icon: /* @__PURE__ */ l.jsx(S.Report, {}),
        children: /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: U.map(
          /** @param {any} [item] @returns {any} */
          (s) => s.kind === "branch" ? /* @__PURE__ */ l.jsx(
            Vl,
            {
              item: s,
              selected: !c && G(y) === G({
                type: "branch",
                nodePath: s.nodePath,
                branchIndex: s.branchIndex
              }),
              theme: r,
              onSelect: () => Te(s)
            },
            `outline-branch:${le(s.nodePath)}:${s.branchIndex}:${s.branch._id}`
          ) : /* @__PURE__ */ l.jsx(
            Hl,
            {
              item: s,
              selected: !c && G(y) === G({ type: "node", path: s.nodePath }),
              theme: r,
              sortByOptions: fe,
              getFieldLabel: Q,
              onSelect: () => Te(s)
            },
            `outline-node:${le(s.nodePath)}:${s.node._id}`
          )
        ) })
      }
    ),
    g && c && /* @__PURE__ */ l.jsx("div", { ref: xe, children: /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "selected-row",
        title: "Selected Row",
        description: "Configure Custom children for the row gear you clicked.",
        icon: /* @__PURE__ */ l.jsx(S.Settings, {}),
        children: /* @__PURE__ */ l.jsx(
          Kl,
          {
            draft: c,
            allFields: O,
            getFieldLabel: Q,
            sortByOptions: fe,
            theme: r,
            onChange: $,
            onApply: yl,
            onCancel: fl
          }
        )
      }
    ) }),
    g && !c && y && ee && ee.type === "node" && /* @__PURE__ */ l.jsx("div", { ref: xe, children: /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "selected-level",
        title: "Selected Level",
        description: "Edit the selected default level or override child level.",
        icon: /* @__PURE__ */ l.jsx(S.Edit, {}),
        children: /* @__PURE__ */ l.jsx(
          ql,
          {
            item: I.find(
              /** @param {any} [item] @returns {any} */
              (s) => we(s.path, y.path)
            ) || (oe ? { ...oe, path: oe.nodePath } : { path: y.path, levelNumber: y.path.length + 1 }),
            node: ee.node,
            allFields: O,
            getFieldLabel: Q,
            sortByOptions: fe,
            theme: r,
            onUpdate: (
              /** @param {any} [nextNode] @returns {any} */
              ((s) => We(y.path, s))
            ),
            onApply: hl,
            onRemove: () => dl(y.path),
            onEditFilter: () => ue(y.path),
            titleLabel: "Title"
          }
        )
      }
    ) }),
    g && !c && y && ee && ee.type === "branch" && oe && /* @__PURE__ */ l.jsx("div", { ref: xe, children: /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "row-override",
        title: "Row Override",
        description: "Override applies to this row only. Configure its rule and Custom children.",
        icon: /* @__PURE__ */ l.jsx(S.Settings, {}),
        children: /* @__PURE__ */ l.jsx(
          Jl,
          {
            item: oe,
            branch: ee.branch,
            theme: r,
            onUpdate: (
              /** @param {any} [nextBranch] @returns {any} */
              ((s) => _e(y.nodePath, y.branchIndex, s))
            ),
            onEditCondition: () => pe({ nodePath: y.nodePath, branchIndex: y.branchIndex }),
            onOpenCustomChildren: () => ol(y.nodePath, y.branchIndex, !!ee.branch.child),
            onRemoveCustomChildren: () => al(y.nodePath, y.branchIndex),
            onRemove: () => cl(y.nodePath, y.branchIndex)
          }
        )
      }
    ) }),
    /* @__PURE__ */ l.jsxs(
      te,
      {
        theme: r,
        sectionId: "save-report",
        title: "Save Report",
        description: "Store the current report levels and row overrides.",
        icon: /* @__PURE__ */ l.jsx(S.Save, {}),
        children: [
          /* @__PURE__ */ l.jsx(Tl, { validation: q, theme: r }),
          /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px" }, children: [
            /* @__PURE__ */ l.jsx(
              "input",
              {
                value: D,
                onChange: (s) => R(s.target.value),
                onKeyDown: (s) => {
                  s.key === "Enter" && Me();
                },
                placeholder: "Report name...",
                style: { ...H(r), flex: 1 }
              }
            ),
            /* @__PURE__ */ l.jsxs(
              "button",
              {
                type: "button",
                onClick: Me,
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
                  /* @__PURE__ */ l.jsx(S.Save, {}),
                  " Save"
                ]
              }
            )
          ] })
        ]
      }
    ),
    V && ke && /* @__PURE__ */ l.jsx(
      De,
      {
        condition: ke.condition,
        allFields: O,
        getFieldLabel: Q,
        theme: r,
        onSave: (
          /** @param {any} [nextCondition] @returns {any} */
          ((s) => {
            _e(V.nodePath, V.branchIndex, {
              ...ke,
              condition: s
            });
          })
        ),
        onClose: () => pe(null)
      }
    ),
    ie && (() => {
      const s = re(g, ie);
      return s ? /* @__PURE__ */ l.jsx(
        De,
        {
          title: "Edit level filter",
          description: "Rows that don't match are grouped into 'Others' to keep totals intact. Applies to this level and all its children.",
          condition: s.filters || Re(),
          allFields: O,
          getFieldLabel: Q,
          theme: r,
          onSave: (
            /** @param {any} [nextCondition] @returns {any} */
            ((p) => {
              We(ie, {
                ...s,
                filters: p.clauses.length > 0 ? p : null
              });
            })
          ),
          onClose: () => ue(null)
        }
      ) : null;
    })()
  ] });
}
export {
  Zl as ReportEditor
};
