import { aa as fl, j as l, z as j, ac as hl, Z as M, al as yl } from "./index-DeulalCT.js";
import { a as f } from "./charts-DthW1Q8W.js";
const ne = () => `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`, Je = [
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
], ae = /* @__PURE__ */ new Set(["in", "not_in"]), bl = {
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
}, gl = /* @__PURE__ */ new Set([
  "depth",
  "_id",
  "_path",
  "_levelLabel",
  "_levelField",
  "_pathFields",
  "_has_children",
  "_is_expanded",
  "__col_schema"
]), z = "default", Ae = "branch:", qe = ["", "solid", "dashed", "dotted", "double"], Ke = (e = "") => ({ _id: ne(), field: e, operator: "eq", value: "", values: [] }), Be = () => ({ op: "AND", clauses: [] }), vl = () => ({ _id: ne(), label: "", condition: Be(), child: null }), ye = (e = {}) => {
  const t = e && typeof e == "object" ? e : {}, n = Number(t.indent), s = Number(t.borderWidth), r = typeof t.borderStyle == "string" && qe.includes(t.borderStyle) ? t.borderStyle : "";
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
    borderWidth: Number.isFinite(s) && s > 0 ? Math.floor(s) : ""
  };
}, K = (e = "") => ({
  _id: ne(),
  field: e,
  label: "",
  topN: null,
  sortBy: null,
  sortDir: "desc",
  sortAbs: !1,
  filters: null,
  format: ye(),
  defaultChild: null,
  branches: []
}), jl = (e) => !e || typeof e.field != "string" || !e.field ? null : {
  _id: typeof e._id == "string" ? e._id : ne(),
  field: e.field,
  operator: Je.some((t) => t.value === e.operator) ? e.operator : "eq",
  value: e.value !== void 0 && e.value !== null ? String(e.value) : "",
  values: Array.isArray(e.values) ? e.values.map(String).filter(Boolean) : []
}, De = (e) => ({
  op: e && e.op === "OR" ? "OR" : "AND",
  clauses: Array.isArray(e && e.clauses) ? e.clauses.map(jl).filter(Boolean) : []
}), ke = (e) => !e || typeof e != "object" ? null : {
  _id: typeof e._id == "string" ? e._id : ne(),
  label: typeof e.label == "string" ? e.label : "",
  condition: De(e.condition),
  sourceRowPath: Array.isArray(e.sourceRowPath) ? e.sourceRowPath.map(String).filter(Boolean) : [],
  sourcePathFields: Array.isArray(e.sourcePathFields) ? e.sourcePathFields.map(String).filter(Boolean) : [],
  child: e.child ? H(e.child) : null
};
function H(e) {
  if (!e || typeof e != "object") return null;
  let t = Array.isArray(e.branches) ? e.branches.map(ke).filter(Boolean) : [];
  t.length === 0 && e.childrenByValue && typeof e.childrenByValue == "object" && (t = Object.entries(e.childrenByValue).filter(([s]) => s).map(([s, r]) => ke({
    label: s,
    condition: { op: "AND", clauses: [{ field: e.field || "", operator: "eq", value: s, values: [] }] },
    child: r
  })).filter(Boolean));
  const n = e.filters && typeof e.filters == "object" ? De(e.filters) : null;
  return {
    _id: typeof e._id == "string" ? e._id : ne(),
    field: typeof e.field == "string" ? e.field : "",
    label: typeof e.label == "string" ? e.label : "",
    topN: Number.isFinite(Number(e.topN)) && Number(e.topN) > 0 ? Math.floor(Number(e.topN)) : null,
    sortBy: typeof e.sortBy == "string" && e.sortBy.trim() ? e.sortBy.trim() : null,
    sortDir: e.sortDir === "asc" ? "asc" : "desc",
    sortAbs: e.sortAbs === !0,
    filters: n && n.clauses.length > 0 ? n : null,
    format: ye(e.format),
    defaultChild: e.defaultChild ? H(e.defaultChild) : null,
    branches: t
  };
}
const Re = (e) => `${Ae}${e}`, ge = (e) => {
  if (typeof e != "string" || !e.startsWith(Ae)) return -1;
  const t = parseInt(e.slice(Ae.length), 10);
  return Number.isFinite(t) ? t : -1;
}, Sl = (e, t) => {
  const n = new Set(
    (t || []).filter((s) => s && s.field && s.agg).map((s) => s.agg === "formula" ? s.field : `${s.field}_${s.agg}`)
  );
  return (e || []).filter((s) => s && typeof s == "string" && !gl.has(s) && (!s.startsWith("_") || hl(s)) && !n.has(s));
}, Cl = (e) => {
  const t = [
    { value: "", label: "(default)" },
    { value: "__field__", label: "Field value (A→Z / Z→A)" }
  ];
  return (e || []).forEach((n) => {
    if (!n || !n.field || !n.agg) return;
    const s = n.agg === "formula" ? n.field : `${n.field}_${n.agg}`, r = n.agg === "formula" ? n.label || M(n.field) : `${M(n.field)} (${yl(n.agg, n.weightField)})`;
    t.push({ value: s, label: r });
  }), t;
}, ve = (e) => !e || !e.clauses || e.clauses.length === 0 ? "No filter" : e.clauses.map((t) => {
  const n = M(t.field), s = bl[t.operator] || t.operator;
  if (ae.has(t.operator)) {
    const r = t.values.slice(0, 3), o = t.values.length > 3 ? ` +${t.values.length - 3}` : "";
    return `${n} ${s} [${r.join(", ")}${o}]`;
  }
  return `${n} ${s} ${t.value || "..."}`;
}).join(` ${e.op} `), Ge = (e, t) => {
  if (!e) return "";
  const n = (t || []).find((s) => s.value === e);
  return n ? n.label : M(e);
}, Ye = (e) => e ? e.label && e.label.trim() ? e.label.trim() : e.field ? M(e.field) : "Select field" : "Select field", wl = (e) => !e || !e.field ? !1 : ae.has(e.operator) ? Array.isArray(e.values) && e.values.length > 0 : e.value !== void 0 && e.value !== null && String(e.value).trim() !== "", $e = (e) => !!(e && Array.isArray(e.clauses) && e.clauses.length > 0) && e.clauses.every(wl), Te = (e, t) => {
  if (!e || !t || typeof t != "object") return !1;
  const n = t[e.field], s = n == null ? "" : String(n), r = e.value === void 0 || e.value === null ? "" : String(e.value), o = Array.isArray(e.values) ? e.values.map(String) : [], a = Number(s), u = Number(r);
  switch (e.operator) {
    case "not_eq":
      return s !== r;
    case "in":
      return o.includes(s);
    case "not_in":
      return !o.includes(s);
    case "contains":
      return s.toLowerCase().includes(r.toLowerCase());
    case "not_contains":
      return !s.toLowerCase().includes(r.toLowerCase());
    case "gt":
      return Number.isFinite(a) && Number.isFinite(u) && a > u;
    case "gte":
      return Number.isFinite(a) && Number.isFinite(u) && a >= u;
    case "lt":
      return Number.isFinite(a) && Number.isFinite(u) && a < u;
    case "lte":
      return Number.isFinite(a) && Number.isFinite(u) && a <= u;
    case "eq":
    default:
      return s === r;
  }
}, Me = (e, t) => {
  if (!$e(e) || !Array.isArray(t) || t.length === 0) return !0;
  const n = t.filter((r) => r && typeof r == "object" && !r._isTotal && !r._isOther);
  return n.some((r) => e.clauses.every((o) => Object.prototype.hasOwnProperty.call(r, o.field))) ? n.some((r) => e.op === "OR" ? e.clauses.some((o) => Te(o, r)) : e.clauses.every((o) => Te(o, r))) : !0;
}, Al = (e, t) => {
  if (!Array.isArray(e) || e.length === 0 || !Array.isArray(t) || t.length === 0) return !0;
  const n = e.map(String).join("|||");
  return t.some((s) => s && typeof s == "object" && s._path === n);
}, He = (e, {
  allFields: t = [],
  valConfigs: n = [],
  data: s = []
} = {}) => {
  const r = [], o = [], a = (n || []).some((p) => p && p.field && p.agg), u = new Set(Array.isArray(t) ? t : []), v = /* @__PURE__ */ new Map(), y = Array.isArray(s) ? s : [], F = (p, w) => {
    const c = p.trim().toLowerCase();
    if (c) {
      if (v.has(c)) {
        r.push(`Duplicate header "${p}" at ${w}; already used at ${v.get(c)}.`);
        return;
      }
      v.set(c, w);
    }
  }, $ = (p, w, c = /* @__PURE__ */ new Set()) => {
    if (!p) return;
    const A = p._id || `${w}:${p.field || ""}`;
    if (c.has(A)) {
      r.push(`Circular row override detected at ${w}.`);
      return;
    }
    const S = new Set(c);
    S.add(A), p.field ? u.size > 0 && !u.has(p.field) && r.push(`Field "${M(p.field)}" at ${w} is no longer available.`) : r.push(`Missing field at ${w}.`), F(Ye(p), w), p.topN && !a && r.push(`Top-N at ${w} needs at least one measure selected.`), p.filters && !$e(p.filters) ? r.push(`Level filter at ${w} has incomplete rules.`) : p.filters && !Me(p.filters, y) && r.push(`Level filter at ${w} does not match any loaded rows.`), p.defaultChild && $(p.defaultChild, `${w} > Default children`, S), (p.branches || []).forEach((d, b) => {
      const B = d.label && d.label.trim() ? d.label.trim() : `Row override ${b + 1}`, le = `${w} > ${B}`;
      $e(d.condition) ? Me(d.condition, y) || r.push(`Row override "${B}" does not match any loaded rows.`) : r.push(`Row override "${B}" needs a complete row rule.`), Array.isArray(d.sourcePathFields) && d.sourcePathFields.length > 0 && d.sourcePathFields[d.sourcePathFields.length - 1] !== p.field && r.push(`Row override "${B}" was created for another level and no longer points to this row path.`), Al(d.sourceRowPath, y) || o.push(`Row override "${B}" points to a row path that is not in the current loaded rows.`), d.child && $(d.child, `${le} > Custom children`, S), d.child || o.push(`Row override "${B}" has no Custom children; it only renames the matching row.`);
    });
  };
  return e ? $(e, "Level 1") : r.push("Report needs at least Level 1."), { errors: r, warnings: o, valid: r.length === 0 };
}, kl = (e) => !e || !e.branchBadge ? `Level ${e ? e.levelNumber : 1}` : e.branchDepth === 0 ? `Override children ${e.branchBadge}` : `Override child level ${e.branchBadge}.${e.branchDepth + 1}`, re = (e, t) => {
  let n = e;
  for (let s = 0; n && s < t.length; s += 1) {
    const r = t[s];
    if (r === z)
      n = n.defaultChild || null;
    else {
      const o = ge(r);
      n = o >= 0 && n.branches[o] ? n.branches[o].child : null;
    }
  }
  return n || null;
}, we = (e, t, n) => {
  const s = re(e, t);
  return s && s.branches[n] ? s.branches[n] : null;
}, _ = (e, t, n) => {
  if (!e) return e;
  const s = (r, o) => {
    if (!r) return r;
    if (o >= t.length) return H(n(r));
    const a = t[o];
    if (a === z)
      return H({
        ...r,
        defaultChild: s(r.defaultChild, o + 1)
      });
    const u = ge(a);
    return u < 0 || u >= r.branches.length ? r : H({
      ...r,
      branches: r.branches.map((v, y) => y === u ? { ...v, child: s(v.child, o + 1) } : v)
    });
  };
  return s(e, 0);
}, he = (e, t, n, s) => e && _(e, t, (r) => n < 0 || n >= r.branches.length ? r : {
  ...r,
  branches: r.branches.map((o, a) => a === n ? ke(s(o)) : o)
}), $l = (e) => {
  if (!e) return [];
  const t = [], n = (s, r) => {
    s && (t.push({
      kind: "node",
      node: s,
      nodePath: r.nodePath,
      depth: r.depth,
      levelNumber: r.levelNumber,
      branchBadge: r.branchBadge,
      branchDepth: r.branchDepth
    }), s.branches.forEach((o, a) => {
      const u = `${r.levelNumber}.${a + 1}`;
      t.push({
        kind: "branch",
        branch: o,
        nodePath: r.nodePath,
        branchIndex: a,
        depth: r.depth + 1,
        badge: u
      }), o.child && n(o.child, {
        nodePath: [...r.nodePath, Re(a)],
        depth: r.depth + 2,
        levelNumber: r.levelNumber + 1,
        branchBadge: u,
        branchDepth: 0
      });
    }), s.defaultChild && n(s.defaultChild, {
      nodePath: [...r.nodePath, z],
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
}, ee = (e = []) => e.join(">"), je = (e = [], t = []) => ee(e) === ee(t), Z = (e) => e == null ? "" : String(e), G = (e) => e ? e.type === "node" ? `node:${ee(e.path)}` : e.type === "branch" ? `branch:${ee(e.nodePath)}:${e.branchIndex}` : e.type === "rowPath" ? `rowPath:${ee(e.nodePath)}:${e.field}:${Z(e.value)}` : "" : "", be = (e, t) => (e || []).find((n) => n.kind === "node" && je(n.nodePath, t)) || null, Ze = (e, t, n) => (e || []).find((s) => s.kind === "branch" && je(s.nodePath, t) && s.branchIndex === n) || null, Ve = (e, t, n) => {
  if (!e || !t) return !1;
  const s = Z(n);
  return (e.condition && Array.isArray(e.condition.clauses) ? e.condition.clauses : []).some((o) => !o || o.field !== t ? !1 : o.operator === "eq" || o.operator === "=" ? Z(o.value) === s : o.operator === "in" ? Array.isArray(o.values) && o.values.map(Z).includes(s) : !1);
}, ml = (e, t, n, s = "") => ({
  ...vl(),
  label: Z(n || t),
  condition: {
    op: "AND",
    clauses: [{
      ...Ke(e),
      operator: "eq",
      value: Z(t),
      values: []
    }]
  },
  child: K(s)
}), Ne = (e) => {
  if (!e) return null;
  try {
    return H(JSON.parse(JSON.stringify(e)));
  } catch {
    return H(e);
  }
}, me = (e) => Array.from({ length: Math.max(0, e) }, () => z), Ie = (e, t = 1, n = []) => {
  const s = [];
  let r = e, o = 0, a = Array.isArray(n) ? [...n] : [];
  for (; r; )
    s.push({
      node: r,
      path: [...a],
      index: o,
      levelNumber: t + o
    }), r = r.defaultChild || null, a = [...a, z], o += 1;
  return s;
}, Fl = (e) => e ? { ...Ne(e), defaultChild: null } : null, Bl = (e) => {
  if (!Array.isArray(e) || e.length === 0) return null;
  let t = null;
  for (let n = e.length - 1; n >= 0; n -= 1)
    t = H({
      ...Fl(e[n]),
      defaultChild: t
    });
  return t;
}, Xe = (e, t, n) => {
  const s = Ie(e).map((a) => a.node);
  if (t < 0 || n < 0 || t >= s.length || n >= s.length || t === n)
    return e;
  const r = [...s], [o] = r.splice(t, 1);
  return r.splice(n, 0, o), Bl(r);
}, Dl = (e) => {
  const t = Ne(e);
  return !t || !t.field ? null : t;
}, Rl = (e, t) => t ? t.type === "node" ? be(e, t.path) : t.type === "branch" ? Ze(e, t.nodePath, t.branchIndex) : null : null, Nl = (e, t) => {
  if (!t) return [];
  const n = [], s = (a) => {
    const u = be(e, a);
    u && n.push(u);
  }, r = (a, u) => {
    const v = Ze(e, a, u);
    v && n.push(v);
  }, o = (a) => {
    if (s([]), !Array.isArray(a) || a.length === 0) return;
    let u = [];
    a.forEach((v) => {
      if (v === z) {
        u = [...u, v], s(u);
        return;
      }
      const y = ge(v);
      y >= 0 && (r(u, y), u = [...u, v], s(u));
    });
  };
  return t.type === "node" ? (o(t.path), n) : (o(t.nodePath), r(t.nodePath, t.branchIndex), n);
}, Il = (e, t) => {
  if (!t) return [];
  if (t.type === "node") {
    const s = [], r = be(e, [...t.path, z]);
    r && s.push(r);
    const o = (e || []).filter((a) => a.kind === "branch" && je(a.nodePath, t.path)).sort((a, u) => a.branchIndex - u.branchIndex);
    return [...s, ...o];
  }
  const n = be(e, [...t.nodePath, Re(t.branchIndex)]);
  return n ? [n] : [];
}, V = (e, t = {}) => ({
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
}), T = (e, t = !1, n = !1) => ({
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
}), P = (e) => ({
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
}), Pl = (e) => ({
  borderTop: `2px solid ${e.textSec || "#334155"}`,
  paddingTop: "14px",
  display: "flex",
  flexDirection: "column",
  gap: "12px"
}), Qe = (e, t, n) => ({
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
  const s = n === "primary" ? { background: `${t.primary || "#2563EB"}12`, border: `${t.primary || "#2563EB"}30`, color: t.primary || "#2563EB" } : { background: t.hover || "#F3F4F6", border: t.border, color: t.textSec };
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
        border: `1px solid ${s.border}`,
        background: s.background,
        color: s.color
      },
      children: e
    }
  );
}
function te({
  theme: e,
  title: t,
  description: n,
  icon: s,
  action: r = null,
  sectionId: o,
  children: a
}) {
  return /* @__PURE__ */ l.jsxs("section", { "data-report-section": o || t, style: Pl(e), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", gap: "8px", minWidth: 0 }, children: [
        /* @__PURE__ */ l.jsx("div", { style: { color: e.textSec, display: "flex", alignItems: "center", paddingTop: "1px", flexShrink: 0 }, children: s }),
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
function Fe({ condition: e, allFields: t, getFieldLabel: n = M, onSave: s, onClose: r, theme: o, title: a, description: u }) {
  const [v, y] = f.useState(() => De(e)), F = V(o), $ = f.useCallback(() => {
    y((c) => ({ ...c, clauses: [...c.clauses, Ke(t[0] || "")] }));
  }, [t]), p = f.useCallback((c, A) => {
    y((S) => ({
      ...S,
      clauses: S.clauses.map((d, b) => b === c ? { ...d, ...A } : d)
    }));
  }, []), w = f.useCallback((c) => {
    y((A) => ({
      ...A,
      clauses: A.clauses.filter((S, d) => d !== c)
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
          onClick: (c) => c.stopPropagation(),
          children: [
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px" }, children: [
              /* @__PURE__ */ l.jsxs("div", { children: [
                /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: o.text }, children: a || "Edit row override rule" }),
                /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: o.textSec, marginTop: "2px" }, children: u || "Add one or more clauses to decide when this row override should be used." })
              ] }),
              /* @__PURE__ */ l.jsx("button", { type: "button", onClick: r, style: { background: "none", border: "none", cursor: "pointer", color: o.textSec }, children: /* @__PURE__ */ l.jsx(j.Close, {}) })
            ] }),
            v.clauses.length > 1 && /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "6px", alignItems: "center", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ l.jsx("span", { style: { fontSize: "11px", color: o.textSec, fontWeight: 700 }, children: "Match:" }),
              ["AND", "OR"].map((c) => /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => y((A) => ({ ...A, op: c })),
                  style: {
                    padding: "5px 12px",
                    borderRadius: "999px",
                    fontSize: "11px",
                    fontWeight: 800,
                    border: "none",
                    cursor: "pointer",
                    background: v.op === c ? o.primary || "#2563EB" : o.hover || "#F3F4F6",
                    color: v.op === c ? "#fff" : o.textSec
                  },
                  children: c
                },
                c
              ))
            ] }),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: [
              v.clauses.length === 0 && /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: o.textSec, fontStyle: "italic" }, children: "No clauses means this row override matches every row at this level." }),
              v.clauses.map((c, A) => /* @__PURE__ */ l.jsxs(
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
                    /* @__PURE__ */ l.jsx("div", { style: { width: "32px", paddingTop: "6px", textAlign: "center", fontSize: "10px", fontWeight: 800, color: o.textSec }, children: A > 0 ? v.op : "" }),
                    /* @__PURE__ */ l.jsxs("div", { style: { flex: 1, display: "flex", flexDirection: "column", gap: "8px" }, children: [
                      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap" }, children: [
                        /* @__PURE__ */ l.jsx(
                          "select",
                          {
                            value: c.field,
                            onChange: (S) => p(A, { field: S.target.value }),
                            style: { ...F, flex: 1, minWidth: "140px" },
                            children: t.map((S) => /* @__PURE__ */ l.jsx("option", { value: S, children: n(S) }, S))
                          }
                        ),
                        /* @__PURE__ */ l.jsx(
                          "select",
                          {
                            value: c.operator,
                            onChange: (S) => {
                              const d = S.target.value, b = ae.has(d), B = ae.has(c.operator);
                              p(A, {
                                operator: d,
                                values: b ? B ? c.values : c.value ? [c.value] : [] : c.values,
                                value: b ? c.value : c.values[0] || c.value || ""
                              });
                            },
                            style: { ...F, width: "160px", flexShrink: 0 },
                            children: Je.map((S) => /* @__PURE__ */ l.jsx("option", { value: S.value, children: S.label }, S.value))
                          }
                        )
                      ] }),
                      ae.has(c.operator) ? /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: [
                        c.values.length > 0 && /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" }, children: c.values.map((S, d) => /* @__PURE__ */ l.jsxs(Y, { theme: o, tone: "primary", children: [
                          /* @__PURE__ */ l.jsx("span", { children: S }),
                          /* @__PURE__ */ l.jsx(
                            "button",
                            {
                              type: "button",
                              onClick: () => p(A, {
                                values: c.values.filter((b, B) => B !== d)
                              }),
                              style: { background: "none", border: "none", padding: 0, cursor: "pointer", color: "inherit", lineHeight: 1 },
                              children: "x"
                            }
                          )
                        ] }, `${c._id}-${d}`)) }),
                        /* @__PURE__ */ l.jsx(
                          "input",
                          {
                            placeholder: "Type a value and press Enter",
                            style: F,
                            onKeyDown: (S) => {
                              const d = S.target.value.trim();
                              S.key === "Enter" && d && (c.values.includes(d) || p(A, { values: [...c.values, d] }), S.target.value = "", S.preventDefault());
                            }
                          }
                        )
                      ] }) : /* @__PURE__ */ l.jsx(
                        "input",
                        {
                          value: c.value,
                          onChange: (S) => p(A, { value: S.target.value }),
                          placeholder: "Value",
                          style: F
                        }
                      )
                    ] }),
                    /* @__PURE__ */ l.jsx(
                      "button",
                      {
                        type: "button",
                        onClick: () => w(A),
                        style: { background: "none", border: "none", cursor: "pointer", color: "#B91C1C", padding: "6px" },
                        children: /* @__PURE__ */ l.jsx(j.Delete, {})
                      }
                    )
                  ]
                },
                c._id
              ))
            ] }),
            /* @__PURE__ */ l.jsxs(
              "button",
              {
                type: "button",
                onClick: $,
                style: {
                  ...T(o),
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
              /* @__PURE__ */ l.jsx("button", { type: "button", onClick: r, style: T(o), children: "Cancel" }),
              /* @__PURE__ */ l.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => {
                    s(v), r();
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
function Ue({ value: e, onChange: t, theme: n }) {
  const s = ye(e), r = V(n), o = (a) => t(ye({ ...s, ...a }));
  return /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsx("div", { style: P(n), children: "Presentation" }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px" }, children: [
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Indent px",
        /* @__PURE__ */ l.jsx("input", { type: "number", min: "0", value: s.indent, onChange: (a) => o({ indent: a.target.value }), placeholder: "Auto", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Row color",
        /* @__PURE__ */ l.jsx("input", { value: s.rowColor, onChange: (a) => o({ rowColor: a.target.value }), placeholder: "#F8FAFC", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Label prefix",
        /* @__PURE__ */ l.jsx("input", { value: s.labelPrefix, onChange: (a) => o({ labelPrefix: a.target.value }), placeholder: "Optional", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Label suffix",
        /* @__PURE__ */ l.jsx("input", { value: s.labelSuffix, onChange: (a) => o({ labelSuffix: a.target.value }), placeholder: "Optional", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec, gridColumn: "1 / -1" }, children: [
        "Number format override",
        /* @__PURE__ */ l.jsx("input", { value: s.numberFormat, onChange: (a) => o({ numberFormat: a.target.value }), placeholder: "fixed:2, percent, currency:$", style: r })
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "8px" }, children: [
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Border",
        /* @__PURE__ */ l.jsx("select", { value: s.borderStyle, onChange: (a) => o({ borderStyle: a.target.value }), style: r, children: qe.map((a) => /* @__PURE__ */ l.jsx("option", { value: a, children: a || "None" }, a || "none")) })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Border color",
        /* @__PURE__ */ l.jsx("input", { value: s.borderColor, onChange: (a) => o({ borderColor: a.target.value }), placeholder: "#CBD5E1", style: r })
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "5px", fontSize: "11px", color: n.textSec }, children: [
        "Width",
        /* @__PURE__ */ l.jsx("input", { type: "number", min: "1", value: s.borderWidth, onChange: (a) => o({ borderWidth: a.target.value }), placeholder: "1", style: r })
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "14px", flexWrap: "wrap" }, children: [
      /* @__PURE__ */ l.jsxs("label", { style: { display: "inline-flex", alignItems: "center", gap: "7px", fontSize: "12px", color: n.text, fontWeight: 700 }, children: [
        /* @__PURE__ */ l.jsx("input", { type: "checkbox", checked: s.bold, onChange: (a) => o({ bold: a.target.checked }) }),
        "Bold row"
      ] }),
      /* @__PURE__ */ l.jsxs("label", { style: { display: "inline-flex", alignItems: "center", gap: "7px", fontSize: "12px", color: n.text, fontWeight: 700 }, children: [
        /* @__PURE__ */ l.jsx("input", { type: "checkbox", checked: s.showSubtotal, onChange: (a) => o({ showSubtotal: a.target.checked }) }),
        "Show subtotal/other rows"
      ] })
    ] })
  ] });
}
function El({ validation: e, theme: t }) {
  const n = e && Array.isArray(e.errors) ? e.errors : [], s = e && Array.isArray(e.warnings) ? e.warnings : [];
  return n.length === 0 && s.length === 0 ? null : /* @__PURE__ */ l.jsxs("div", { "data-report-validation": "true", style: { border: `1px solid ${n.length > 0 ? "#FCA5A5" : "#FDE68A"}`, borderRadius: "12px", padding: "10px 12px", background: n.length > 0 ? "#FEF2F2" : "#FFFBEB", color: n.length > 0 ? "#991B1B" : "#92400E", fontSize: "11px", lineHeight: 1.45, display: "flex", flexDirection: "column", gap: "6px" }, children: [
    n.length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: { fontWeight: 900, marginBottom: "4px" }, children: "Fix before applying or saving" }),
      n.map((r) => /* @__PURE__ */ l.jsx("div", { children: r }, `err:${r}`))
    ] }),
    s.length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: { fontWeight: 900, marginBottom: "4px" }, children: "Warnings" }),
      s.map((r) => /* @__PURE__ */ l.jsx("div", { children: r }, `warn:${r}`))
    ] })
  ] });
}
function Ll({ debug: e, theme: t }) {
  if (!e || typeof e != "object") return null;
  const n = [
    ["Matched level", e.matchedLevel || e.levelLabel || ""],
    ["Field", e.levelField || ""],
    ["Row path", Array.isArray(e.pathValues) ? e.pathValues.join(" > ") : e.rowPath || ""],
    ["Children source", e.childrenSource || "Default children"],
    ["Rule", e.matchedCondition || ""]
  ].filter(([, s]) => s);
  return n.length === 0 ? null : /* @__PURE__ */ l.jsxs("div", { "data-report-debug-inspector": "true", style: { border: `1px solid ${t.border}`, borderRadius: "12px", padding: "10px", background: t.hover || "#F8FAFC", display: "flex", flexDirection: "column", gap: "7px" }, children: [
    /* @__PURE__ */ l.jsx("div", { style: P(t), children: "Why Is This Row Here?" }),
    n.map(([s, r]) => /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", justifyContent: "space-between", gap: "10px", fontSize: "11px" }, children: [
      /* @__PURE__ */ l.jsx("span", { style: { color: t.textSec, fontWeight: 800 }, children: s }),
      /* @__PURE__ */ l.jsx("span", { style: { color: t.text, textAlign: "right", overflowWrap: "anywhere" }, children: r })
    ] }, s))
  ] });
}
function el({
  title: e,
  summary: t,
  theme: n,
  defaultOpen: s = !1,
  children: r
}) {
  const [o, a] = f.useState(s);
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
            onClick: () => a((u) => !u),
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
function Ol({ item: e, selected: t, theme: n, sortByOptions: s, getFieldLabel: r = M, onSelect: o }) {
  const a = e.node, u = kl(e), v = a.field ? r(a.field) : "Select field", y = Ye(a), F = Ge(a.sortBy, s);
  return /* @__PURE__ */ l.jsx("button", { type: "button", onClick: o, style: Qe(n, t, e.depth), children: /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsxs("div", { style: { minWidth: 0, display: "flex", flexDirection: "column", gap: "7px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, tone: e.branchBadge ? "primary" : "neutral", children: u }),
        /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: v })
      ] }),
      /* @__PURE__ */ l.jsxs("div", { style: { fontSize: "11px", color: n.textSec }, children: [
        "Header: ",
        y
      ] }),
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, children: a.topN ? `Top ${a.topN}` : "All rows" }),
        /* @__PURE__ */ l.jsx(Y, { theme: n, children: F ? `Sort ${a.sortDir}${a.sortAbs ? " |abs|" : ""} by ${F}` : "Default sort" }),
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
function _l({ item: e, selected: t, theme: n, onSelect: s }) {
  const r = e.branch, o = r.label && r.label.trim() ? r.label.trim() : "Display original label";
  return /* @__PURE__ */ l.jsx("button", { type: "button", onClick: s, style: Qe(n, t, e.depth), children: /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "10px" }, children: [
    /* @__PURE__ */ l.jsxs("div", { style: { minWidth: 0, display: "flex", flexDirection: "column", gap: "7px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }, children: [
        /* @__PURE__ */ l.jsx(Y, { theme: n, tone: "primary", children: `Row override ${e.badge}` }),
        /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text }, children: o })
      ] }),
      /* @__PURE__ */ l.jsx("div", { style: { fontSize: "11px", color: n.textSec, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: ve(r.condition) }),
      /* @__PURE__ */ l.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: /* @__PURE__ */ l.jsx(Y, { theme: n, children: r.child ? "Custom children" : "Default children" }) })
    ] }),
    /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(j.ChevronRight, {}) })
  ] }) });
}
function ll({
  item: e,
  selected: t,
  theme: n,
  sortByOptions: s,
  getFieldLabel: r = M,
  onSelect: o,
  onDragStart: a,
  onDragOver: u,
  onDrop: v
}) {
  const y = e.node, F = y.field ? r(y.field) : "Select field", $ = y.label && y.label.trim() ? y.label.trim() : F, p = Ge(y.sortBy, s);
  return /* @__PURE__ */ l.jsxs(
    "button",
    {
      type: "button",
      draggable: !0,
      onDragStart: a,
      onDragOver: u,
      onDrop: v,
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
            /* @__PURE__ */ l.jsx("span", { style: { fontSize: "12px", fontWeight: 800, color: n.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: $ })
          ] }),
          /* @__PURE__ */ l.jsxs("div", { style: { fontSize: "11px", color: n.textSec, marginTop: "3px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: [
            F,
            p ? ` · ${y.sortDir === "asc" ? "Asc" : "Desc"}${y.sortAbs ? " |abs|" : ""} by ${p}` : "",
            y.topN ? ` · Top ${y.topN}` : "",
            y.filters && y.filters.clauses && y.filters.clauses.length > 0 ? ` · ${y.filters.clauses.length} filter${y.filters.clauses.length === 1 ? "" : "s"}` : ""
          ] })
        ] }),
        /* @__PURE__ */ l.jsx("span", { style: { color: t ? n.primary || "#2563EB" : n.textSec, flexShrink: 0 }, children: /* @__PURE__ */ l.jsx(j.ChevronRight, {}) })
      ]
    }
  );
}
function Wl({
  item: e,
  node: t,
  allFields: n,
  getFieldLabel: s = M,
  sortByOptions: r,
  theme: o,
  onUpdate: a,
  onApply: u,
  onRemove: v,
  onEditFilter: y,
  titleLabel: F = "Title",
  canRemove: $ = !0
}) {
  const p = V(o), w = e && e.levelNumber ? `Level ${e.levelNumber}` : "Level", c = !!(t.filters && t.filters.clauses && t.filters.clauses.length > 0), S = [
    t.field ? s(t.field) : "Select field",
    t.label && t.label.trim() ? `Title: ${t.label.trim()}` : null,
    t.topN ? `Top ${t.topN}` : null,
    c ? `${t.filters.clauses.length} filter${t.filters.clauses.length === 1 ? "" : "s"}` : null
  ].filter(Boolean);
  return /* @__PURE__ */ l.jsxs("div", { style: de(o), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsx("div", { children: /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: o.text }, children: w }) }),
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexShrink: 0 }, children: [
        /* @__PURE__ */ l.jsx(
          "button",
          {
            type: "button",
            onClick: u,
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
        $ && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: v, style: T(o, !1, !0), children: [
          /* @__PURE__ */ l.jsx(j.Delete, {}),
          " ",
          e && e.path && e.path.length === 0 ? "Clear report" : "Remove level"
        ] })
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs(
      el,
      {
        title: `${w} settings`,
        summary: S.join(" · "),
        theme: o,
        defaultOpen: !1,
        children: [
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: P(o), children: "Break Down By" }),
            /* @__PURE__ */ l.jsxs(
              "select",
              {
                value: t.field,
                onChange: (d) => a({ ...t, field: d.target.value }),
                style: p,
                children: [
                  /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                  n.map((d) => /* @__PURE__ */ l.jsx("option", { value: d, children: s(d) }, d))
                ]
              }
            )
          ] }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: P(o), children: F }),
            /* @__PURE__ */ l.jsx(
              "input",
              {
                value: t.label,
                onChange: (d) => a({ ...t, label: d.target.value }),
                placeholder: t.field ? s(t.field) : "Label for this level",
                style: p
              }
            )
          ] }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: P(o), children: "Ranking And Sort" }),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "82px 1fr 42px 42px", gap: "8px", alignItems: "stretch" }, children: [
              /* @__PURE__ */ l.jsx(
                "input",
                {
                  type: "number",
                  min: "1",
                  value: t.topN || "",
                  onChange: (d) => {
                    const b = d.target.value === "" ? null : parseInt(d.target.value, 10);
                    a({ ...t, topN: b && b > 0 ? b : null });
                  },
                  placeholder: "All",
                  style: { ...p, textAlign: "center" }
                }
              ),
              /* @__PURE__ */ l.jsx(
                "select",
                {
                  value: t.sortBy || "",
                  onChange: (d) => a({ ...t, sortBy: d.target.value || null }),
                  style: p,
                  children: r.map((d) => /* @__PURE__ */ l.jsx("option", { value: d.value, children: d.label }, d.value))
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
            /* @__PURE__ */ l.jsx("div", { style: P(o), children: "Level Filter" }),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", alignItems: "stretch" }, children: [
              /* @__PURE__ */ l.jsxs(
                "button",
                {
                  type: "button",
                  onClick: y,
                  style: V(o, {
                    flex: 1,
                    display: "flex",
                    alignItems: "center",
                    gap: "8px",
                    textAlign: "left",
                    cursor: "pointer",
                    background: c ? `${o.primary || "#2563EB"}08` : o.hover || "#F8FAFC",
                    borderColor: c ? `${o.primary || "#2563EB"}45` : void 0
                  }),
                  children: [
                    /* @__PURE__ */ l.jsx(j.Filter, {}),
                    /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }, children: c ? ve(t.filters) : "No filter (show all rows)" })
                  ]
                }
              ),
              c && /* @__PURE__ */ l.jsx(
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
            Ue,
            {
              value: t.format,
              theme: o,
              onChange: (d) => a({ ...t, format: d })
            }
          )
        ]
      }
    )
  ] });
}
function zl({
  draft: e,
  allFields: t,
  getFieldLabel: n = M,
  sortByOptions: s,
  theme: r,
  onChange: o,
  onApply: a,
  onCancel: u
}) {
  const [v, y] = f.useState(null), [F, $] = f.useState(!1), p = e && e.child ? e.child : K(""), w = Array.isArray(e && e.selectedPath) ? e.selectedPath : [], c = re(p, w) || p, A = !!(c.filters && c.filters.clauses && c.filters.clauses.length > 0), S = Ie(p, e.childLevelNumber || 2), d = Math.max(0, w.length), b = (e && Number.isFinite(Number(e.childLevelNumber)) ? Number(e.childLevelNumber) : 2) + w.length, B = V(r), le = [
    c.field ? n(c.field) : "Select field",
    e.title && e.title.trim() ? `Title: ${e.title.trim()}` : null,
    c.topN ? `Top ${c.topN}` : null,
    A ? `${c.filters.clauses.length} filter${c.filters.clauses.length === 1 ? "" : "s"}` : null
  ].filter(Boolean), L = (h) => {
    o({
      ...e,
      child: _(p, w, () => h)
    });
  }, ce = (h) => {
    o({
      ...e,
      selectedPath: me(h)
    });
  }, D = () => {
    const h = [...w, z];
    o({
      ...e,
      child: _(p, w, (R) => ({
        ...R,
        defaultChild: R.defaultChild || K("")
      })),
      selectedPath: h
    });
  }, O = () => {
    o({
      ...e,
      child: _(p, w, (h) => ({
        ...h,
        defaultChild: null
      }))
    });
  }, Se = (h, R) => {
    h === null || h === R || (o({
      ...e,
      child: Xe(p, h, R),
      selectedPath: me(R)
    }), y(null));
  };
  return /* @__PURE__ */ l.jsxs(l.Fragment, { children: [
    /* @__PURE__ */ l.jsxs("div", { style: de(r), children: [
      /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
        /* @__PURE__ */ l.jsxs("div", { children: [
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: r.text }, children: e.rowLabel || "Selected row" }),
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: r.textSec, marginTop: "2px" }, children: "Override applies to this row only." })
        ] }),
        /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexShrink: 0 }, children: [
          /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: u, style: T(r), children: [
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
        /* @__PURE__ */ l.jsx("div", { style: P(r), children: "Level" }),
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: r.text }, children: `Level ${b}` })
      ] }),
      /* @__PURE__ */ l.jsx(Ll, { debug: e.request && e.request.debug, theme: r }),
      S.length > 1 && /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: S.map((h) => /* @__PURE__ */ l.jsx(
        ll,
        {
          item: h,
          selected: d === h.index,
          theme: r,
          sortByOptions: s,
          getFieldLabel: n,
          onSelect: () => ce(h.index),
          onDragStart: (R) => {
            y(h.index), R.dataTransfer.effectAllowed = "move";
          },
          onDragOver: (R) => R.preventDefault(),
          onDrop: (R) => {
            R.preventDefault(), Se(v, h.index);
          }
        },
        `draft-level:${h.index}:${h.node._id}`
      )) }),
      /* @__PURE__ */ l.jsxs(
        el,
        {
          title: `Level ${b} settings`,
          summary: le.join(" · "),
          theme: r,
          defaultOpen: !1,
          children: [
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: P(r), children: "Title" }),
              /* @__PURE__ */ l.jsx(
                "input",
                {
                  value: e.title,
                  onChange: (h) => o({ ...e, title: h.target.value }),
                  placeholder: e.rowLabel || "Row title",
                  style: B
                }
              )
            ] }),
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: P(r), children: "Break Down By" }),
              /* @__PURE__ */ l.jsxs(
                "select",
                {
                  value: c.field,
                  onChange: (h) => L({ ...c, field: h.target.value }),
                  style: B,
                  children: [
                    /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                    t.map((h) => /* @__PURE__ */ l.jsx("option", { value: h, children: n(h) }, h))
                  ]
                }
              )
            ] }),
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: P(r), children: "Ranking And Sort" }),
              /* @__PURE__ */ l.jsxs("div", { style: { display: "grid", gridTemplateColumns: "82px 1fr 42px 42px", gap: "8px", alignItems: "stretch" }, children: [
                /* @__PURE__ */ l.jsx(
                  "input",
                  {
                    type: "number",
                    min: "1",
                    value: c.topN || "",
                    onChange: (h) => {
                      const R = h.target.value === "" ? null : parseInt(h.target.value, 10);
                      L({ ...c, topN: R && R > 0 ? R : null });
                    },
                    placeholder: "All",
                    style: { ...B, textAlign: "center" }
                  }
                ),
                /* @__PURE__ */ l.jsx(
                  "select",
                  {
                    value: c.sortBy || "",
                    onChange: (h) => L({ ...c, sortBy: h.target.value || null }),
                    style: B,
                    children: s.map((h) => /* @__PURE__ */ l.jsx("option", { value: h.value, children: h.label }, h.value))
                  }
                ),
                /* @__PURE__ */ l.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => L({ ...c, sortAbs: !c.sortAbs }),
                    style: {
                      border: `1px solid ${c.sortAbs ? r.primary || "#2563EB" : r.border}`,
                      borderRadius: "10px",
                      background: c.sortAbs ? `${r.primary || "#2563EB"}18` : r.hover || "#F3F4F6",
                      color: c.sortAbs ? r.primary || "#2563EB" : r.text,
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
                    onClick: () => L({ ...c, sortDir: c.sortDir === "asc" ? "desc" : "asc" }),
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
                    title: c.sortDir === "asc" ? "Ascending" : "Descending",
                    children: c.sortDir === "asc" ? /* @__PURE__ */ l.jsx(j.SortAsc, {}) : /* @__PURE__ */ l.jsx(j.SortDesc, {})
                  }
                )
              ] })
            ] }),
            /* @__PURE__ */ l.jsxs("div", { children: [
              /* @__PURE__ */ l.jsx("div", { style: P(r), children: "Filter (this row's children only)" }),
              /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", alignItems: "stretch" }, children: [
                /* @__PURE__ */ l.jsxs(
                  "button",
                  {
                    type: "button",
                    onClick: () => $(!0),
                    style: V(r, {
                      flex: 1,
                      display: "flex",
                      alignItems: "center",
                      gap: "8px",
                      textAlign: "left",
                      cursor: "pointer",
                      background: A ? `${r.primary || "#2563EB"}08` : r.hover || "#F8FAFC",
                      borderColor: A ? `${r.primary || "#2563EB"}45` : void 0
                    }),
                    children: [
                      /* @__PURE__ */ l.jsx(j.Filter, {}),
                      /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }, children: A ? ve(c.filters) : "No filter (show all children)" })
                    ]
                  }
                ),
                A && /* @__PURE__ */ l.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => L({ ...c, filters: null }),
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
              Ue,
              {
                value: c.format,
                theme: r,
                onChange: (h) => L({ ...c, format: h })
              }
            ),
            /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px" }, children: [
              /* @__PURE__ */ l.jsxs(
                "button",
                {
                  type: "button",
                  onClick: D,
                  disabled: !c.field,
                  style: { ...T(r, !!c.defaultChild), opacity: c.field ? 1 : 0.5 },
                  children: [
                    /* @__PURE__ */ l.jsx(j.Add, {}),
                    " ",
                    c.defaultChild ? "Open next level" : "Add next level"
                  ]
                }
              ),
              c.defaultChild && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: O, style: T(r, !1, !0), children: [
                /* @__PURE__ */ l.jsx(j.Delete, {}),
                " Remove next level"
              ] })
            ] })
          ]
        }
      )
    ] }),
    F && /* @__PURE__ */ l.jsx(
      Fe,
      {
        title: `Filter children of "${e.rowLabel || "this row"}"`,
        description: "Only children matching this filter are shown. Filtered-out rows are grouped into 'Others' to keep totals intact.",
        condition: c.filters || Be(),
        allFields: t,
        getFieldLabel: n,
        theme: r,
        onSave: (h) => {
          L({
            ...c,
            filters: h.clauses.length > 0 ? h : null
          });
        },
        onClose: () => $(!1)
      }
    )
  ] });
}
function Tl({
  item: e,
  branch: t,
  theme: n,
  onUpdate: s,
  onEditCondition: r,
  onOpenCustomChildren: o,
  onRemoveCustomChildren: a,
  onRemove: u
}) {
  const v = V(n), y = ve(t.condition);
  return /* @__PURE__ */ l.jsxs("div", { style: de(n), children: [
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "12px" }, children: [
      /* @__PURE__ */ l.jsxs("div", { children: [
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: n.text }, children: `Row override ${e.badge}` }),
        /* @__PURE__ */ l.jsx("div", { style: { fontSize: "12px", color: n.textSec, marginTop: "2px" }, children: "Use a row override when matching rows need a different label or child breakdown." })
      ] }),
      /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: u, style: T(n, !1, !0), children: [
        /* @__PURE__ */ l.jsx(j.Delete, {}),
        " Remove override"
      ] })
    ] }),
    /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: P(n), children: "Display As" }),
      /* @__PURE__ */ l.jsx(
        "input",
        {
          value: t.label,
          onChange: (F) => s({ ...t, label: F.target.value }),
          placeholder: "Label to show for matching rows",
          style: v
        }
      )
    ] }),
    /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsx("div", { style: P(n), children: "Rule" }),
      /* @__PURE__ */ l.jsxs(
        "button",
        {
          type: "button",
          onClick: r,
          title: y,
          style: {
            ...V(n),
            display: "flex",
            alignItems: "center",
            gap: "8px",
            textAlign: "left",
            cursor: "pointer",
            background: n.hover || "#F8FAFC"
          },
          children: [
            /* @__PURE__ */ l.jsx(j.Filter, {}),
            /* @__PURE__ */ l.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: y })
          ]
        }
      )
    ] }),
    /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px" }, children: [
      /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: o, style: T(n, !!t.child), children: [
        /* @__PURE__ */ l.jsx(j.Add, {}),
        " ",
        t.child ? "Open Custom children" : "Add Custom children"
      ] }),
      t.child && /* @__PURE__ */ l.jsxs("button", { type: "button", onClick: a, style: T(n, !1, !0), children: [
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
function Vl({
  reportDef: e,
  setReportDef: t,
  availableFields: n,
  customDimensions: s = [],
  theme: r,
  data: o = [],
  valConfigs: a,
  savedReports: u,
  setSavedReports: v,
  activeReportId: y,
  setActiveReportId: F,
  showNotification: $,
  requestedSelection: p = null,
  showReportConfigColumn: w = !0,
  setShowReportConfigColumn: c
}) {
  const [A, S] = f.useState(""), [d, b] = f.useState(null), [B, le] = f.useState(null), [L, ce] = f.useState(null), [D, O] = f.useState(null), [Se, h] = f.useState(null), R = f.useRef(null), ie = f.useRef(!1), Pe = f.useRef(null), g = f.useMemo(
    () => e && e.root ? H(e.root) : null,
    [e]
  ), W = f.useMemo(
    () => Sl(n, a),
    [n, a]
  ), X = f.useCallback(
    (i) => fl(i, s),
    [s]
  ), pe = f.useMemo(
    () => Cl(a),
    [a]
  ), J = f.useMemo(
    () => He(g, { allFields: W, valConfigs: a, data: o }),
    [g, W, a, o]
  ), m = f.useCallback((i) => {
    t((x) => {
      const C = x && typeof x == "object" ? x : {}, k = C.root ? H(C.root) : null, I = typeof i == "function" ? i(k) : i;
      return { ...C, root: I };
    });
  }, [t]), Q = f.useMemo(() => $l(g), [g]), N = f.useMemo(() => Ie(g), [g]), se = f.useMemo(
    () => Rl(Q, d),
    [Q, d]
  );
  f.useMemo(
    () => Nl(Q, d),
    [Q, d]
  ), f.useMemo(
    () => Il(Q, d),
    [Q, d]
  );
  const U = f.useMemo(() => {
    if (!g || !d) return null;
    if (d.type === "node") {
      const i = re(g, d.path);
      return i ? { type: "node", node: i } : null;
    }
    if (d.type === "branch") {
      const i = we(g, d.nodePath, d.branchIndex);
      return i ? { type: "branch", branch: i } : null;
    }
    return null;
  }, [g, d]), Ce = f.useMemo(() => !B || !g ? null : we(g, B.nodePath, B.branchIndex), [B, g]), Ee = f.useCallback((i) => {
    if (!i || i.type !== "rowPath" || !g) return !1;
    const x = Array.isArray(i.nodePath) ? i.nodePath : [], C = re(g, x);
    if (!C) return !1;
    const k = Array.isArray(C.branches) ? C.branches : [], I = k.findIndex((oe) => Ve(oe, i.field, i.value)), E = I >= 0 ? k[I] : null, xe = E && E.child ? E.child : C.defaultChild || K(""), q = Z(i.label || i.value);
    return O({
      request: i,
      nodePath: x,
      branchIndex: I,
      rowLabel: q,
      title: E && E.label ? E.label : q,
      child: Ne(xe) || K(""),
      selectedPath: [],
      childLevelNumber: Number.isFinite(Number(i.levelNumber)) ? Number(i.levelNumber) + 1 : 2
    }), b({ type: "node", path: x }), !0;
  }, [g]);
  f.useEffect(() => {
    if (!g) {
      d !== null && b(null);
      return;
    }
    if (d) {
      if (d.type === "node") {
        re(g, d.path) || b({ type: "node", path: [] });
        return;
      }
      we(g, d.nodePath, d.branchIndex) || b({ type: "node", path: d.nodePath });
    }
  }, [g, d]);
  const ue = f.useCallback(() => {
    const i = R.current;
    !i || typeof i.scrollIntoView != "function" || window.requestAnimationFrame(() => {
      i.scrollIntoView({ block: "start", behavior: "smooth" });
    });
  }, []);
  f.useEffect(() => {
    if (p && Pe.current !== p) {
      if (Pe.current = p, ie.current = !0, p.type === "rowPath") {
        Ee(p) || (ie.current = !1);
        return;
      }
      if (G(p) === G(d)) {
        ie.current = !1, ue();
        return;
      }
      b(p);
    }
  }, [Ee, p, ue, d]), f.useEffect(() => {
    ie.current && (ie.current = !1, ue());
  }, [ue, d]);
  const Le = f.useCallback((i, x) => {
    m((C) => _(C, i, () => x));
  }, [m]), Oe = f.useCallback((i, x, C) => {
    m((k) => he(k, i, x, () => C));
  }, [m]), tl = f.useCallback((i) => {
    const x = K(i);
    m(x), O(null), b({ type: "node", path: [] });
  }, [m]);
  f.useCallback((i, x) => {
    x || m((C) => _(C, i, (k) => ({
      ...k,
      defaultChild: K("")
    }))), b({ type: "node", path: [...i, z] });
  }, [m]), f.useCallback((i) => {
    m((x) => _(x, i, (C) => ({
      ...C,
      defaultChild: null
    }))), b({ type: "node", path: i });
  }, [m]);
  const rl = f.useCallback((i, x, C) => {
    C || m((k) => he(k, i, x, (I) => ({
      ...I,
      child: K(W[0] || "")
    }))), b({ type: "node", path: [...i, Re(x)] });
  }, [W, m]), nl = f.useCallback((i, x) => {
    m((C) => he(C, i, x, (k) => ({
      ...k,
      child: null
    }))), b({ type: "branch", nodePath: i, branchIndex: x });
  }, [m]), il = f.useCallback((i) => {
    if (i.length === 0) {
      m(null), O(null), b(null);
      return;
    }
    const x = i.slice(0, -1), C = i[i.length - 1];
    if (C === z) {
      m((I) => _(I, x, (E) => ({
        ...E,
        defaultChild: null
      }))), b({ type: "node", path: x });
      return;
    }
    const k = ge(C);
    k >= 0 && (m((I) => he(I, x, k, (E) => ({
      ...E,
      child: null
    }))), b({ type: "branch", nodePath: x, branchIndex: k }));
  }, [m]), sl = f.useCallback((i, x) => {
    m((C) => _(C, i, (k) => ({
      ...k,
      branches: k.branches.filter((I, E) => E !== x)
    }))), b({ type: "node", path: i });
  }, [m]), ol = f.useCallback((i) => {
    O(null), b({ type: "node", path: i });
  }, []), _e = f.useCallback((i) => {
    if (i) {
      if (O(null), i.kind === "branch") {
        b({ type: "branch", nodePath: i.nodePath, branchIndex: i.branchIndex });
        return;
      }
      b({ type: "node", path: i.nodePath });
    }
  }, []), al = f.useCallback(() => {
    if (!g || !N.length) return;
    const i = N[N.length - 1];
    if (!i || !i.node || !i.node.field) return;
    const x = [...i.path, z];
    O(null), m((C) => _(C, i.path, (k) => ({
      ...k,
      defaultChild: k.defaultChild || K("")
    }))), b({ type: "node", path: x });
  }, [N, g, m]), dl = f.useCallback((i, x) => {
    i === null || i === x || (m((C) => Xe(C, i, x)), b({ type: "node", path: me(x) }), h(null));
  }, [m]), cl = f.useCallback(() => {
    O(null), b(null);
  }, []), pl = f.useCallback(() => {
    if (!J.valid) {
      $ && $(J.errors[0] || "Fix report validation errors before applying.", "error");
      return;
    }
    b(null);
  }, [J, $]), ul = f.useCallback(() => {
    if (!D || !D.request) return;
    const i = D.request, x = Array.isArray(D.nodePath) ? D.nodePath : [], C = Dl(D.child), k = {
      ...ml(i.field, i.value, D.title || D.rowLabel, ""),
      label: Z(D.title || D.rowLabel || i.value),
      sourceRowPath: Array.isArray(i.pathValues) ? i.pathValues.map(String).filter(Boolean) : [],
      sourcePathFields: Array.isArray(i.pathFields) ? i.pathFields.map(String).filter(Boolean) : [],
      child: C
    }, I = (q) => {
      const oe = Array.isArray(q.branches) ? q.branches : [], ze = oe.findIndex((fe) => Ve(fe, i.field, i.value));
      return ze >= 0 ? {
        ...q,
        branches: oe.map((fe, xl) => xl === ze ? { ...k, _id: fe._id || k._id } : fe)
      } : {
        ...q,
        branches: [...oe, k]
      };
    }, E = _(g, x, I), xe = He(E, { allFields: W, valConfigs: a, data: o });
    if (!xe.valid) {
      $ && $(xe.errors[0] || "Fix report validation errors before applying.", "error");
      return;
    }
    m((q) => _(q, x, I)), O(null), b(null), $ && $(`Applied settings for ${D.rowLabel || "row"}`, "info");
  }, [W, o, g, D, m, $, a]), We = f.useCallback(() => {
    if (!g || !g.field) return;
    if (!J.valid) {
      $ && $(J.errors[0] || "Fix report validation errors before saving.", "error");
      return;
    }
    const i = `report-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`, x = A.trim() || `Report ${(u || []).length + 1}`;
    v([
      ...u || [],
      {
        id: i,
        name: x,
        reportDef: JSON.parse(JSON.stringify(e)),
        createdAt: (/* @__PURE__ */ new Date()).toISOString()
      }
    ]), F(i), S(""), $ && $(`Report "${x}" saved`, "info");
  }, [e, A, J, g, u, F, v, $]);
  return /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "18px", padding: "2px 0 8px" }, children: [
    (u || []).length > 0 && /* @__PURE__ */ l.jsxs("div", { children: [
      /* @__PURE__ */ l.jsxs("div", { style: P(r), children: [
        /* @__PURE__ */ l.jsx(j.Save, {}),
        " Saved Reports"
      ] }),
      /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: (u || []).map((i) => /* @__PURE__ */ l.jsxs(
        "div",
        {
          onClick: () => {
            t(JSON.parse(JSON.stringify(i.reportDef))), F(i.id), O(null), b({ type: "node", path: [] });
          },
          style: {
            display: "flex",
            alignItems: "center",
            gap: "8px",
            padding: "8px 10px",
            borderRadius: "10px",
            cursor: "pointer",
            background: i.id === y ? r.select || `${r.primary || "#2563EB"}14` : r.hover || "#F9FAFB",
            border: `1px solid ${i.id === y ? r.primary || "#2563EB" : r.border}`
          },
          children: [
            /* @__PURE__ */ l.jsx(j.Report, {}),
            /* @__PURE__ */ l.jsx("div", { style: { flex: 1, minWidth: 0, fontSize: "12px", fontWeight: 700, color: r.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: i.name }),
            /* @__PURE__ */ l.jsx(
              "button",
              {
                type: "button",
                onClick: (x) => {
                  x.stopPropagation(), v((C) => (C || []).filter((k) => k.id !== i.id)), y === i.id && F(null);
                },
                style: { background: "none", border: "none", cursor: "pointer", color: "#B91C1C", padding: "2px", flexShrink: 0 },
                children: /* @__PURE__ */ l.jsx(j.Delete, {})
              }
            )
          ]
        },
        i.id
      )) })
    ] }),
    /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "report-levels",
        title: "Report Levels",
        description: "Default hierarchy applied to every row unless a row override changes its children.",
        icon: /* @__PURE__ */ l.jsx(j.ReportLevel, {}),
        action: g && /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end" }, children: [
          typeof c == "function" && /* @__PURE__ */ l.jsxs(
            "button",
            {
              type: "button",
              onClick: () => c((i) => i === !1),
              style: T(r, w !== !1, !1),
              title: w !== !1 ? "Hide row gear column" : "Show row gear column",
              children: [
                /* @__PURE__ */ l.jsx(j.Settings, {}),
                " ",
                w !== !1 ? "Hide row gears" : "Show row gears"
              ]
            }
          ),
          /* @__PURE__ */ l.jsxs(
            "button",
            {
              type: "button",
              onClick: al,
              disabled: !N.length || !N[N.length - 1].node.field,
              style: {
                ...T(r, !1, !1),
                opacity: !N.length || !N[N.length - 1].node.field ? 0.5 : 1,
                pointerEvents: !N.length || !N[N.length - 1].node.field ? "none" : "auto"
              },
              children: [
                /* @__PURE__ */ l.jsx(j.Add, {}),
                " Add next level"
              ]
            }
          )
        ] }),
        children: g ? /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: N.map((i) => /* @__PURE__ */ l.jsx(
          ll,
          {
            item: i,
            selected: !D && G(d) === G({ type: "node", path: i.path }),
            theme: r,
            sortByOptions: pe,
            getFieldLabel: X,
            onSelect: () => ol(i.path),
            onDragStart: (x) => {
              h(i.index), x.dataTransfer.effectAllowed = "move";
            },
            onDragOver: (x) => x.preventDefault(),
            onDrop: (x) => {
              x.preventDefault(), dl(Se, i.index);
            }
          },
          `default-level:${i.index}:${i.node._id}`
        )) }) : /* @__PURE__ */ l.jsxs("div", { style: de(r), children: [
          /* @__PURE__ */ l.jsx("div", { style: { fontSize: "14px", fontWeight: 800, color: r.text }, children: "Level 1" }),
          /* @__PURE__ */ l.jsxs("div", { children: [
            /* @__PURE__ */ l.jsx("div", { style: P(r), children: "Break Down By" }),
            /* @__PURE__ */ l.jsxs(
              "select",
              {
                value: "",
                onChange: (i) => {
                  i.target.value && tl(i.target.value);
                },
                style: V(r),
                children: [
                  /* @__PURE__ */ l.jsx("option", { value: "", children: "Select field..." }),
                  W.map((i) => /* @__PURE__ */ l.jsx("option", { value: i, children: X(i) }, i))
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
        icon: /* @__PURE__ */ l.jsx(j.Report, {}),
        children: /* @__PURE__ */ l.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "8px" }, children: Q.map((i) => i.kind === "branch" ? /* @__PURE__ */ l.jsx(
          _l,
          {
            item: i,
            selected: !D && G(d) === G({
              type: "branch",
              nodePath: i.nodePath,
              branchIndex: i.branchIndex
            }),
            theme: r,
            onSelect: () => _e(i)
          },
          `outline-branch:${ee(i.nodePath)}:${i.branchIndex}:${i.branch._id}`
        ) : /* @__PURE__ */ l.jsx(
          Ol,
          {
            item: i,
            selected: !D && G(d) === G({ type: "node", path: i.nodePath }),
            theme: r,
            sortByOptions: pe,
            getFieldLabel: X,
            onSelect: () => _e(i)
          },
          `outline-node:${ee(i.nodePath)}:${i.node._id}`
        )) })
      }
    ),
    g && D && /* @__PURE__ */ l.jsx("div", { ref: R, children: /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "selected-row",
        title: "Selected Row",
        description: "Configure Custom children for the row gear you clicked.",
        icon: /* @__PURE__ */ l.jsx(j.Settings, {}),
        children: /* @__PURE__ */ l.jsx(
          zl,
          {
            draft: D,
            allFields: W,
            getFieldLabel: X,
            sortByOptions: pe,
            theme: r,
            onChange: O,
            onApply: ul,
            onCancel: cl
          }
        )
      }
    ) }),
    g && !D && d && U && U.type === "node" && /* @__PURE__ */ l.jsx("div", { ref: R, children: /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "selected-level",
        title: "Selected Level",
        description: "Edit the selected default level or override child level.",
        icon: /* @__PURE__ */ l.jsx(j.Edit, {}),
        children: /* @__PURE__ */ l.jsx(
          Wl,
          {
            item: N.find((i) => je(i.path, d.path)) || (se ? { ...se, path: se.nodePath } : { path: d.path, levelNumber: d.path.length + 1 }),
            node: U.node,
            allFields: W,
            getFieldLabel: X,
            sortByOptions: pe,
            theme: r,
            onUpdate: (i) => Le(d.path, i),
            onApply: pl,
            onRemove: () => il(d.path),
            onEditFilter: () => ce(d.path),
            titleLabel: "Title"
          }
        )
      }
    ) }),
    g && !D && d && U && U.type === "branch" && se && /* @__PURE__ */ l.jsx("div", { ref: R, children: /* @__PURE__ */ l.jsx(
      te,
      {
        theme: r,
        sectionId: "row-override",
        title: "Row Override",
        description: "Override applies to this row only. Configure its rule and Custom children.",
        icon: /* @__PURE__ */ l.jsx(j.Settings, {}),
        children: /* @__PURE__ */ l.jsx(
          Tl,
          {
            item: se,
            branch: U.branch,
            theme: r,
            onUpdate: (i) => Oe(d.nodePath, d.branchIndex, i),
            onEditCondition: () => le({ nodePath: d.nodePath, branchIndex: d.branchIndex }),
            onOpenCustomChildren: () => rl(d.nodePath, d.branchIndex, !!U.branch.child),
            onRemoveCustomChildren: () => nl(d.nodePath, d.branchIndex),
            onRemove: () => sl(d.nodePath, d.branchIndex)
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
        icon: /* @__PURE__ */ l.jsx(j.Save, {}),
        children: [
          /* @__PURE__ */ l.jsx(El, { validation: J, theme: r }),
          /* @__PURE__ */ l.jsxs("div", { style: { display: "flex", gap: "8px" }, children: [
            /* @__PURE__ */ l.jsx(
              "input",
              {
                value: A,
                onChange: (i) => S(i.target.value),
                onKeyDown: (i) => {
                  i.key === "Enter" && We();
                },
                placeholder: "Report name...",
                style: { ...V(r), flex: 1 }
              }
            ),
            /* @__PURE__ */ l.jsxs(
              "button",
              {
                type: "button",
                onClick: We,
                disabled: !g || !g.field || !J.valid,
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
                  opacity: !g || !g.field || !J.valid ? 0.5 : 1
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
    B && Ce && /* @__PURE__ */ l.jsx(
      Fe,
      {
        condition: Ce.condition,
        allFields: W,
        getFieldLabel: X,
        theme: r,
        onSave: (i) => {
          Oe(B.nodePath, B.branchIndex, {
            ...Ce,
            condition: i
          });
        },
        onClose: () => le(null)
      }
    ),
    L && (() => {
      const i = re(g, L);
      return i ? /* @__PURE__ */ l.jsx(
        Fe,
        {
          title: "Edit level filter",
          description: "Rows that don't match are grouped into 'Others' to keep totals intact. Applies to this level and all its children.",
          condition: i.filters || Be(),
          allFields: W,
          getFieldLabel: X,
          theme: r,
          onSave: (x) => {
            Le(L, {
              ...i,
              filters: x.clauses.length > 0 ? x : null
            });
          },
          onClose: () => ce(null)
        }
      ) : null;
    })()
  ] });
}
export {
  Vl as ReportEditor
};
