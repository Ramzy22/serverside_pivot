class d extends Error {
  constructor(n) {
    super(`emitter does not support: ${n}`), this.name = "EmitterUnsupported";
  }
}
const D = "__depth", de = /* @__PURE__ */ new Set(["contains", "not_contains", "starts_with", "ends_with"]), ge = /* @__PURE__ */ new Set(["=", "!=", ...de]), Se = /* @__PURE__ */ new Set(["=", "!=", ">", ">=", "<", "<="]), Ne = /* @__PURE__ */ new Set(["integer", "floating", "decimal"]);
function u(e) {
  return `"${String(e).replace(/"/g, '""')}"`;
}
function V(e, n) {
  return `__c${e}__${n}`;
}
function _e(e) {
  const n = e.measures.map((t) => t.alias), i = (e.derived || []).map((t) => t.alias), o = [];
  return (e.columnMembers || []).forEach((t, s) => e.measures.forEach((a) => o.push(V(s, a.alias)))), [...n, ...i, ...o];
}
function we(e) {
  const n = [];
  return (e.columnMembers || []).forEach((i, o) => e.measures.forEach((t) => n.push(V(o, t.alias)))), [...e.measures.map((i) => i.alias), ...n];
}
function v(e, n) {
  if (e == null) return "NULL";
  if (typeof e == "boolean") return e ? "TRUE" : "FALSE";
  if (typeof e == "number") {
    if (!Number.isFinite(e)) throw new d(`non-finite number ${e}`);
    return n && n.family === "decimal" ? `CAST('${e}' AS ${n.sqlType})` : String(e);
  }
  if (typeof e == "bigint") return e.toString();
  if (typeof e == "string") {
    const i = `'${e.replace(/'/g, "''")}'`;
    return n && Ne.has(n.family) ? `CAST(${i} AS ${n.sqlType})` : i;
  }
  throw new d(`literal ${JSON.stringify(e)}`);
}
function H(e, n) {
  return e == null ? `CAST(NULL AS ${n.sqlType})` : `CAST(${v(e, n)} AS ${n.sqlType})`;
}
function J(e, n) {
  if ("conditions" in e) {
    const $ = String(e.op).toLowerCase();
    if ($ !== "and" && $ !== "or") throw new d(`filter group operator ${e.op}`);
    const r = (e.conditions || []).map((m) => J(m, n));
    if (r.length === 0) throw new d("empty filter group");
    return `(${r.join($ === "and" ? " AND " : " OR ")})`;
  }
  const i = String(e.field), o = n[i];
  if (!o) throw new d(`unknown column ${i}`);
  const t = u(i), s = String(e.op), a = e.caseSensitive !== !1, l = e.value;
  if (de.has(s)) {
    if (typeof l != "string") throw new d(`${s} needs a string`);
    const $ = a ? t : `lower(${t})`, r = v(a ? l : l.toLowerCase(), void 0), m = {
      contains: `contains(${$}, ${r})`,
      not_contains: `NOT contains(${$}, ${r})`,
      starts_with: `starts_with(${$}, ${r})`,
      ends_with: `ends_with(${$}, ${r})`
    }[s];
    return `(${t} IS NOT NULL AND COALESCE(${m}, FALSE))`;
  }
  if (s === "between") {
    const [$, r] = Array.isArray(l) ? l : [];
    return $ == null || r === null || r === void 0 ? "FALSE" : `(${t} IS NOT NULL AND ${t} >= ${v($, o)} AND ${t} <= ${v(r, o)})`;
  }
  if (Se.has(s))
    return l == null ? "FALSE" : !a && ge.has(s) && typeof l == "string" && o.family === "string" ? `(${t} IS NOT NULL AND lower(${t}) ${s} ${v(l.toLowerCase(), void 0)})` : `(${t} IS NOT NULL AND ${t} ${s} ${v(l, o)})`;
  if (s === "in" || s === "not_in") {
    const $ = Array.isArray(l) ? l : [l], r = $.filter((p) => p != null), m = r.length !== $.length, w = r.map((p) => v(p, o)).join(", ");
    if (s === "in") {
      const p = r.length ? `${t} IN (${w})` : "FALSE";
      return m ? `(${p} OR ${t} IS NULL)` : `(${p})`;
    }
    if (!r.length) return m ? `(${t} IS NOT NULL)` : "TRUE";
    const N = `COALESCE(${t} NOT IN (${w}), FALSE)`;
    return m ? `(${N} AND ${t} IS NOT NULL)` : `(${N} OR ${t} IS NULL)`;
  }
  if (s === "is_empty" || s === "is_not_empty") {
    const $ = n[e.field], r = $ && $.family === "string" ? `(${t} IS NULL OR trim(${t}) = '')` : `(${t} IS NULL)`;
    return s === "is_empty" ? r : `(NOT ${r})`;
  }
  if (s === "is_null") return `(${t} IS NULL)`;
  if (s === "is_not_null") return `(${t} IS NOT NULL)`;
  throw new d(`operator ${s}`);
}
function re(e, n, i) {
  const o = (a) => n ? `CASE WHEN ${n} THEN ${u(a)} END` : u(a);
  if (!e.field) {
    if (e.agg !== "count") throw new d(`${e.agg} without a field`);
    return n ? `COUNT(CASE WHEN ${n} THEN 1 END)` : "COUNT(*)";
  }
  const t = o(e.field), s = `CAST(${t} AS DOUBLE)`;
  switch (e.agg) {
    // Typed as Ibis types it: an integer sum is BIGINT (DuckDB would widen it to HUGEINT).
    case "sum":
      return i[e.field] && i[e.field].family === "integer" ? `CAST(SUM(${t}) AS BIGINT)` : `SUM(${t})`;
    case "count":
      return `COUNT(${t})`;
    case "avg":
      return `AVG(${t})`;
    case "min":
      return `MIN(${t})`;
    case "max":
      return `MAX(${t})`;
    case "count_distinct":
      return `COUNT(DISTINCT ${t})`;
    case "median":
      return `QUANTILE_CONT(${s}, 0.5)`;
    case "stddev":
      return `STDDEV_SAMP(${s})`;
    case "variance":
      return `VAR_SAMP(${s})`;
    case "percentile":
      if (typeof e.q != "number") throw new d("percentile without q");
      return `QUANTILE_CONT(${s}, ${e.q})`;
    case "weighted_avg": {
      if (!e.weight) throw new d("weighted_avg without a weight");
      const a = o(e.weight), l = `(${t}) IS NOT NULL AND (${a}) IS NOT NULL`, $ = `SUM(CASE WHEN ${l} THEN CAST(${t} AS DOUBLE) * CAST(${a} AS DOUBLE) END)`, r = `SUM(CASE WHEN ${l} THEN CAST(${a} AS DOUBLE) END)`;
      return `CASE WHEN COALESCE(${r}, 0) = 0 THEN NULL ELSE ${$} / ${r} END`;
    }
    case "first":
    case "last": {
      if (!e.orderBy) throw new d(`${e.agg} without an order field`);
      const a = o(e.orderBy);
      return `${e.agg.toUpperCase()}(${t} ORDER BY ${a} ASC, ${t} ASC) FILTER (WHERE (${t}) IS NOT NULL AND (${a}) IS NOT NULL)`;
    }
    case "array_agg": {
      if (!e.orderBy) return `ARRAY_AGG(${t} ORDER BY ${t} ASC) FILTER (WHERE (${t}) IS NOT NULL)`;
      const a = o(e.orderBy);
      return `ARRAY_AGG(${t} ORDER BY ${a} ASC, ${t} ASC) FILTER (WHERE (${t}) IS NOT NULL AND (${a}) IS NOT NULL)`;
    }
    default:
      throw new d(`aggregation ${e.agg}`);
  }
}
function Ee(e, n, i) {
  return e.map((o, t) => n[t] === null || n[t] === void 0 ? `${u(o)} IS NULL` : `${u(o)} IS NOT DISTINCT FROM ${H(n[t], i[o])}`).join(" AND ");
}
function z(e, n, i, o, t, s = null) {
  const a = ($) => z(e, $, i, o, t, s), l = n.args;
  switch (n.op) {
    case "null":
      return "CAST(NULL AS DOUBLE)";
    case "num":
      return `CAST(${Number(l[0])} AS DOUBLE)`;
    case "ref": {
      const $ = e.measures.find((r) => r.alias === l[0]);
      if (!$) throw new d(`formula over unknown measure ${String(l[0])}`);
      return `CAST(${K(e, $, i, o, t, s)} AS DOUBLE)`;
    }
    case "round":
      return `ROUND(${a(l[0])}, ${Number(l[1])})`;
    case "pow": {
      const $ = Number(l[1]);
      let r = a(l[0]);
      return Number.isInteger($) || (r = `(CASE WHEN ${r} < 0 THEN NULL ELSE ${r} END)`), $ < 0 && (r = `(CASE WHEN ${r} = 0 THEN NULL ELSE ${r} END)`), `POWER(${r}, CAST(${$} AS DOUBLE))`;
    }
    case "+":
    case "-":
    case "*":
      return `(${a(l[0])} ${n.op} ${a(l[1])})`;
    case "/":
      return `(CASE WHEN ${a(l[1])} = 0 THEN NULL ELSE ${a(l[0])} / ${a(l[1])} END)`;
    case "neg":
      return `(-${a(l[0])})`;
    case "abs":
      return `ABS(${a(l[0])})`;
    case "ceil":
      return `CEIL(${a(l[0])})`;
    case "floor":
      return `FLOOR(${a(l[0])})`;
    case "trunc":
      return `TRUNC(${a(l[0])})`;
    case "min":
    case "max": {
      const $ = l.map(a);
      return `(CASE WHEN ${$.map((r) => `${r} IS NULL`).join(" OR ")} THEN NULL ELSE ${n.op === "min" ? "LEAST" : "GREATEST"}(${$.join(", ")}) END)`;
    }
    default:
      throw new d(`formula operator ${n.op}`);
  }
}
function K(e, n, i, o, t, s = null) {
  if (n.agg === "formula") {
    const f = z(e, n.expr, i, o, t, s);
    return `(CASE WHEN isnan(${f}) OR isinf(${f}) THEN NULL ELSE ${f} END)`;
  }
  if (n.agg !== "member") return re(n, i, o);
  const a = e.unpivot;
  if (!a) throw new d("member aggregation without a measure axis");
  const $ = a.members.some((f) => f.agg === "array_agg") ? `${o[a.value].sqlType}[]` : o[a.value].sqlType, r = (e.columns || []).indexOf(a.label), m = s !== null && r >= 0 ? s[r] : void 0;
  if (m !== void 0 && (t = [...t, a.label]), !t.includes(a.label)) return `MIN(CAST(NULL AS ${$}))`;
  const w = { family: "string", sqlType: "VARCHAR" }, N = m === void 0 ? a.members : a.members.filter((f) => f.label === m);
  if (m !== void 0 && !N.length) return `MIN(CAST(NULL AS ${$}))`;
  const p = N.map((f) => {
    const g = `${u(a.label)} = ${H(f.label, w)}`, C = {
      alias: f.label,
      agg: String(f.agg),
      field: f.field,
      weight: f.weight,
      q: f.q,
      orderBy: f.orderBy
    }, y = i ? `(${g}) AND (${i})` : g;
    if (f.agg === "formula") {
      const A = z(e, f.expr, y, o, t, s);
      return [g, `CAST((CASE WHEN isnan(${A}) OR isinf(${A}) THEN NULL ELSE ${A} END) AS ${$})`];
    }
    return [g, `CAST(${re(C, y, o)} AS ${$})`];
  });
  return m !== void 0 ? p[0][1] : `CASE ${p.map(([f, g]) => `WHEN BOOL_OR(${f}) THEN ${g}`).join(" ")} ELSE CAST(NULL AS ${$}) END`;
}
const B = "[\\t\\n\\f\\r ]*", ae = `^${B}([0-9]+(?:\\.[0-9]+)?)${B}([A-Za-z])${B}$`, Ae = `^${B}([0-9]{4})-([0-9]{2})-([0-9]{2})${B}$`, le = `^${B}([A-Za-z]+)[\\t\\n\\f\\r ]+([0-9]{4})${B}$`, Le = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
function Te(e) {
  const n = (p, f) => `NULLIF(regexp_extract(${e}, '${p}', ${f}), '')`, i = `CAST(${n(ae, 1)} AS DOUBLE)`, t = `CASE ${`lower(${n(ae, 2)})`} WHEN 'd' THEN 1.0 WHEN 'w' THEN 7.0 WHEN 'm' THEN 30.4375 WHEN 'y' THEN 365.25 END`, [s, a, l] = [1, 2, 3].map((p) => `CAST(${n(Ae, p)} AS DOUBLE)`), $ = `((${s} - 1970.0) * 365.25 + (${a} - 1.0) * 30.4375 + ${l})`, r = `CAST(${n(le, 2)} AS DOUBLE)`, w = `CASE ${`substr(lower(${n(le, 1)}), 1, 3)`} ${Le.map((p, f) => `WHEN '${p}' THEN ${f + 1}.0`).join(" ")} END`, N = `((${r} - 1970.0) * 365.25 + (${w} - 1.0) * 30.4375)`;
  return `CAST(CASE WHEN ${i} IS NOT NULL AND (${t}) IS NOT NULL THEN ${i} * (${t}) WHEN ${s} IS NOT NULL THEN ${$} WHEN ${N} IS NOT NULL THEN ${N} END AS DOUBLE)`;
}
function ce(e, n, i, o) {
  const t = e.measures.filter((s) => !i || i.has(s.alias)).map((s) => `${K(e, s, null, n, o)} AS ${u(s.alias)}`);
  return i || (e.columnMembers || []).forEach((s, a) => {
    const l = Ee(e.columns || [], s, n);
    e.measures.forEach(($) => t.push(
      // a cell fixes its column members (REQ-DIM-006)
      `${K(e, $, l, n, o, s)} AS ${u(V(a, $.alias))}`
    ));
  }), t;
}
function $e(e) {
  return JSON.stringify(e);
}
function ye(e) {
  if (e.subtotals === "none")
    return [.../* @__PURE__ */ new Set([0, e.rows.length])].map((t) => ({ depth: t, keys: e.rows.slice(0, t), visibleParents: null }));
  const n = e.expanded === void 0 || e.expanded === null ? null : e.expanded.map((t) => [...t]), i = n ? new Set(n.map($e)) : null, o = [];
  for (let t = 0; t <= e.rows.length; t++) {
    let s = null;
    n && t > 1 && (s = n.filter((a) => a.length === t - 1 && Array.from({ length: t - 2 }, (l, $) => $ + 1).every((l) => i.has($e(a.slice(0, l))))), s.length === 0) || o.push({ depth: t, keys: e.rows.slice(0, t), visibleParents: s });
  }
  return o;
}
function G(e, n, i, o) {
  return i.map((t, s) => `${e}.${u(t)} IS NOT DISTINCT FROM ${n}.${u(i[s])}`).join(" AND ");
}
function fe(e, n) {
  return e == null ? n ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING" : e === 0 ? "CURRENT ROW" : e < 0 ? `${-e} PRECEDING` : `${e} FOLLOWING`;
}
function pe(e, n) {
  const i = { ...n.columns }, o = [];
  let t = u(n.table);
  (e.categories || []).forEach((l, $) => {
    if (i[l.name]) throw new d(`category ${l.name} collides with a column`);
    const r = [...l.rules.map((f) => f.label), l.fallback];
    let m;
    if (r.every((f) => typeof f == "string")) m = { family: "string", sqlType: "VARCHAR" };
    else if (r.every((f) => typeof f == "number" && Number.isInteger(f))) m = { family: "integer", sqlType: "BIGINT" };
    else if (r.every((f) => typeof f == "number")) m = { family: "floating", sqlType: "DOUBLE" };
    else throw new d(`category ${l.name} mixes label types`);
    const w = l.rules.map((f) => `WHEN ${J(f.when, i)} THEN ${H(f.label, m)}`), N = w.length ? `CASE ${w.join(" ")} ELSE ${H(l.fallback, m)} END` : H(l.fallback, m), p = `__cat${$}`;
    o.push(`${p} AS (SELECT *, ${N} AS ${u(l.name)} FROM ${t})`), t = p, i[l.name] = m;
  });
  const s = e.unpivot;
  if (s) {
    const l = [s.label, s.value, s.key];
    if (new Set(l).size !== 3 || l.some((f) => !f || i[f]))
      throw new d("measure axis columns collide with a column");
    const $ = s.members.filter((f) => f.field !== null && f.field !== void 0);
    $.forEach((f) => {
      if (!i[f.field]) throw new d(`unknown column ${f.field}`);
    });
    const r = new Set($.map((f) => i[f.field].family)), w = [...r].every((f) => f === "integer" || f === "floating" || f === "decimal") ? { family: "floating", sqlType: "DOUBLE" } : r.size === 1 ? i[$[0].field] : { family: "string", sqlType: "VARCHAR" }, N = { family: "string", sqlType: "VARCHAR" }, p = s.members.map((f) => {
      const g = f.field === null || f.field === void 0, C = g ? `CAST(NULL AS ${w.sqlType})` : `CAST(${u(f.field)} AS ${w.sqlType})`, y = s.dropNulls && !g ? ` WHERE ${u(f.field)} IS NOT NULL` : "";
      return `SELECT *, ${H(f.label, N)} AS ${u(s.label)}, ${C} AS ${u(s.value)}, CAST(${f.order} AS BIGINT) AS ${u(s.key)} FROM ${t}${y}`;
    });
    o.push(`__axis AS (${p.join(" UNION ALL ")})`), t = "__axis", i[s.label] = N, i[s.value] = w, i[s.key] = { family: "integer", sqlType: "BIGINT" };
  }
  const a = e.ragged;
  if (a) {
    a.fields.forEach((p) => {
      if (!i[p]) throw new d(`unknown column ${p}`);
    });
    const l = i[a.fields[0]], r = a.fields.every((p) => i[p].sqlType === l.sqlType) ? l : l.family === "integer" ? { family: "integer", sqlType: "BIGINT" } : l.family === "floating" ? { family: "floating", sqlType: "DOUBLE" } : l.family === "string" ? { family: "string", sqlType: "VARCHAR" } : (() => {
      throw new d("ragged hierarchy fields of different types");
    })(), m = a.fields.map((p) => {
      const f = u(p), g = [`${f} IS NOT NULL`];
      return i[p].family === "floating" && g.push(`NOT isnan(${f})`), a.missing ? a.missing.length && g.push(`${f} NOT IN (${a.missing.map((C) => H(C, i[p])).join(", ")})`) : i[p].family === "string" && g.push(`${f} <> ''`), `COALESCE(${g.join(" AND ")}, FALSE)`;
    }), w = m.map((p, f) => m.slice(0, f + 1).map((g) => `CAST(${g} AS BIGINT)`).join(" + ")), N = [];
    a.fields.forEach((p, f) => {
      const g = f + 1, C = a.fields.map((y, A) => `WHEN ${m[A]} AND ${w[A]} = ${g}`);
      N.push(`CASE ${C.map((y, A) => `${y} THEN CAST(${u(a.fields[A])} AS ${r.sqlType})`).join(" ")} END AS ${u(`__rag${g}`)}`), N.push(`CASE ${C.map((y, A) => `${y} THEN CAST(${A} AS BIGINT)`).join(" ")} END AS ${u(`__ragf${g}`)}`), i[`__rag${g}`] = r, i[`__ragf${g}`] = { family: "integer", sqlType: "BIGINT" };
    }), N.push(`${w[w.length - 1]} AS ${u("__ragn")}`), i.__ragn = { family: "integer", sqlType: "BIGINT" }, o.push(`__ragged AS (SELECT *, ${N.join(", ")} FROM ${t})`), t = "__ragged";
  }
  return { ctes: o, relation: t, columns: i };
}
function Oe(e, n) {
  const i = e.columns || [];
  if (!i.length) throw new d("column members without column dimensions");
  const o = pe(e, n);
  i.forEach(($) => {
    if (!o.columns[$]) throw new d(`unknown column ${$}`);
  });
  const t = (e.filters || []).map(($) => J($, o.columns)), s = i.map(u).join(", "), a = i.map(($) => `(${u($)} IS NULL) ASC, ${u($)} ASC`).join(", ");
  return `${o.ctes.length ? `WITH ${o.ctes.join(", ")} ` : ""}SELECT DISTINCT ${s} FROM ${o.relation}${t.length ? ` WHERE ${t.join(" AND ")}` : ""} ORDER BY ${a}`;
}
function Ce(e, n) {
  const i = pe(e, n), { columns: o } = i, t = e.rows, s = (c) => {
    if (!o[c]) throw new d(`unknown column ${c}`);
  };
  t.forEach(s), (e.columns || []).forEach(s);
  const a = [...i.ctes], l = (e.filters || []).map((c) => J(c, o));
  a.push(`__e0 AS (SELECT * FROM ${i.relation}${l.length ? ` WHERE ${l.join(" AND ")}` : ""})`);
  const $ = /* @__PURE__ */ new Map();
  for (const c of e.measureFilters || []) {
    const E = `${c.level}:${c.cells ? 1 : 0}`;
    $.set(E, [...$.get(E) || [], c]);
  }
  const r = [];
  [...$.keys()].sort((c, E) => {
    const [S, _] = c.split(":").map(Number), [h, L] = E.split(":").map(Number);
    return S - h || _ - L;
  }).forEach((c) => {
    const E = $.get(c), [S, _] = c.split(":").map(Number), h = t.slice(0, S), L = _ ? [...h, ...e.columns || []] : h, T = new Set(E.map((U) => U.measure)), M = E.map((U) => {
      if (!Se.has(U.op)) throw new d(`measure filter ${U.op}`);
      return U.value === null || U.value === void 0 ? "FALSE" : `${u(U.measure)} ${U.op} ${v(U.value, void 0)}`;
    }), I = `__mf${S}${_ ? "c" : ""}`, R = u(h[h.length - 1]), W = e.ragged ? `(SELECT * FROM __e0 WHERE ${R} IS NOT NULL)` : "__e0";
    a.push(`${I} AS (SELECT * FROM (SELECT ${L.map(u).join(", ")}, ${ce(e, o, T, L).join(", ")} FROM ${W} GROUP BY ${L.map(u).join(", ")}) WHERE ${M.join(" AND ")})`);
    const se = `EXISTS (SELECT 1 FROM ${I} g WHERE ${G("e", "g", L)})`;
    r.push(e.ragged ? `(e.${R} IS NULL OR ${se})` : se);
  }), a.push(`__e AS (SELECT * FROM __e0 e${r.length ? ` WHERE ${r.join(" AND ")}` : ""})`);
  const m = e.totals === "all_data" ? "__e0" : "__e", w = we(e), N = ye(e), p = {};
  N.forEach((c) => {
    const E = c.keys, S = [];
    if (e.ragged && E.length && S.push(`a.${u(E[E.length - 1])} IS NOT NULL`), e.totals === "all_data" && E.length && S.push(`EXISTS (SELECT 1 FROM __e v WHERE ${G("a", "v", E)})`), c.visibleParents) {
      const T = E.slice(0, -1), M = c.visibleParents.map((I) => `(${I.map((R, W) => H(R, o[T[W]])).join(", ")})`);
      a.push(`__vp${c.depth} (${T.map((I, R) => u(`k${R}`)).join(", ")}) AS (VALUES ${M.join(", ")})`), S.push(`EXISTS (SELECT 1 FROM __vp${c.depth} p WHERE ${T.map((I, R) => `a.${u(I)} IS NOT DISTINCT FROM p.${u(`k${R}`)}`).join(" AND ")})`);
    }
    const _ = `SELECT ${[...E.map(u), ...ce(e, o, void 0, E)].join(", ")} FROM ${m}${E.length ? ` GROUP BY ${E.map(u).join(", ")}` : ""}`, h = [
      ...t.map((T) => E.includes(T) ? `a.${u(T)}` : `CAST(NULL AS ${o[T].sqlType}) AS ${u(T)}`),
      ...w.map((T) => `a.${u(T)}`),
      `CAST(${c.depth} AS SMALLINT) AS ${D}`
    ], L = `__lvl${c.depth}`;
    p[c.depth] = L, a.push(`${L} AS (SELECT ${h.join(", ")} FROM (${_}) a${S.length ? ` WHERE ${S.join(" AND ")}` : ""})`);
  });
  const f = [...t, ...w, D].map(u).join(", ");
  a.push(`__tree AS (${N.map((c) => `SELECT ${f} FROM ${p[c.depth]}`).join(" UNION ALL ")})`);
  const g = e.sort || [], C = new Set(e.measures.map((c) => c.alias)), y = [...new Set(g.filter((c) => !t.includes(c.field) && C.has(c.field)).map((c) => c.field))];
  let A = "__tree";
  if (y.length) {
    const c = [], E = [];
    for (let S = 1; S <= t.length; S++) {
      const _ = p[S];
      y.forEach((h) => {
        const L = u(`__sort${S}_${h}`);
        E.push(_ ? `s${S}.${u(h)} AS ${L}` : `NULL AS ${L}`);
      }), _ && c.push(`LEFT JOIN ${_} s${S} ON ${G("u", `s${S}`, t.slice(0, S))}`);
    }
    a.push(`__sorted AS (SELECT u.*, ${E.join(", ")} FROM __tree u ${c.join(" ")})`), A = "__sorted";
  }
  const j = g.map((c, E) => ({ s: c, n: E })).filter(({ s: c }) => Array.isArray(c.member));
  if (j.length) {
    const c = [], E = [];
    for (let S = 1; S <= t.length; S++) {
      const _ = t.slice(0, S).map(u).join(", "), h = j.map(({ s: L, n: T }) => {
        const M = e.measures.find((W) => W.alias === L.field);
        if (!M) throw new d(`cell sort on unknown measure ${L.field}`);
        const I = u(`__cell${S}_${T}`);
        E.push(`c${S}.${I} AS ${I}`);
        const R = Ee(e.columns || [], L.member, o);
        return `${K(e, M, R, o, t.slice(0, S), L.member)} AS ${I}`;
      });
      c.push(`LEFT JOIN (SELECT ${_}, ${h.join(", ")} FROM ${m} GROUP BY ${_}) c${S} ON ${G("t", `c${S}`, t.slice(0, S))}`);
    }
    a.push(`__cells AS (SELECT t.*, ${E.join(", ")} FROM ${A} t ${c.join(" ")})`), A = "__cells";
  }
  const O = g.filter((c) => t.includes(c.field) && typeof c.semantic == "string");
  if (O.length) {
    const c = O.map((E) => `${Te(`t.${u(E.field)}`)} AS ${u(`__key${t.indexOf(E.field) + 1}`)}`);
    a.push(`__semantic AS (SELECT t.*, ${c.join(", ")} FROM ${A} t)`), A = "__semantic";
  }
  const F = g.filter((c) => t.includes(c.field) && typeof c.by == "string");
  if (F.length) {
    const c = [], E = [];
    F.forEach((S) => {
      const _ = t.indexOf(S.field) + 1, h = t.slice(0, _).map(u).join(", "), L = u(`__key${_}`);
      E.push(`k${_}.${L} AS ${L}`), c.push(`LEFT JOIN (SELECT ${h}, MIN(${u(S.by)}) AS ${L} FROM ${m} GROUP BY ${h}) k${_} ON ${G("t", `k${_}`, t.slice(0, _))}`);
    }), a.push(`__keyed AS (SELECT t.*, ${E.join(", ")} FROM ${A} t ${c.join(" ")})`), A = "__keyed";
  }
  const b = [];
  if (e.subtotals === "none") {
    const c = t.length;
    b.push(`(CASE WHEN ${D} = 0 THEN 1 ELSE 0 END) ASC`);
    const E = /* @__PURE__ */ new Set();
    g.forEach((S, _) => {
      let h;
      !t.includes(S.field) && Array.isArray(S.member) ? h = u(`__cell${c}_${_}`) : t.includes(S.field) ? typeof S.by == "string" || typeof S.semantic == "string" ? h = u(`__key${t.indexOf(S.field) + 1}`) : (h = u(S.field), S.absolute || E.add(S.field)) : h = u(`__sort${c}_${S.field}`), S.absolute && (h = `abs(${h})`), b.push(`(${h} IS NULL) ${S.nullsFirst ? "DESC" : "ASC"}`, `${h} ${S.desc ? "DESC" : "ASC"}`);
    }), t.forEach((S) => {
      E.has(S) || b.push(`(${u(S)} IS NULL) ASC`, `${u(S)} ASC`);
    });
  }
  e.subtotals !== "none" && t.forEach((c, E) => {
    const S = E + 1;
    b.push(`(CASE WHEN ${D} >= ${S} THEN 1 ELSE 0 END) ${e.subtotals === "after" ? "DESC" : "ASC"}`);
    let _ = !1;
    g.forEach((h, L) => {
      let T;
      if (!t.includes(h.field) && Array.isArray(h.member)) T = u(`__cell${S}_${L}`);
      else if (!t.includes(h.field)) T = u(`__sort${S}_${h.field}`);
      else if (h.field === c && (typeof h.by == "string" || typeof h.semantic == "string")) T = u(`__key${S}`);
      else if (h.field === c)
        T = u(c), _ = !0;
      else return;
      h.absolute && (T = `abs(${T})`), b.push(`(${T} IS NULL) ${h.nullsFirst ? "DESC" : "ASC"}`, `${T} ${h.desc ? "DESC" : "ASC"}`);
    }), _ || b.push(`(${u(c)} IS NULL) ASC`, `${u(c)} ASC`);
  });
  const k = e.derived || [], Q = [...t, ..._e(e), D].map(u).join(", "), P = e.window || {}, Z = Number(P.offset || 0), q = P.limit === null || P.limit === void 0 ? null : Number(P.limit), ee = `${q !== null ? ` LIMIT ${q}` : ""}${Z || q !== null ? ` OFFSET ${Z}` : ""}`, te = b.length ? b.join(", ") : "1";
  if (!k.length)
    return `WITH ${a.join(`,
`)}
SELECT ${Q} FROM ${A} ORDER BY ${te}${ee}`;
  const me = t.map((c, E) => `CASE WHEN ${D} > ${E + 1} THEN ${u(c)} ELSE NULL END AS ${u(`__p${E}`)}`);
  a.push(`__ordered AS (SELECT *, ROW_NUMBER() OVER (ORDER BY ${te}) AS __ord, ${[...me, "0 AS __one"].join(", ")} FROM ${A})`);
  const ne = [...new Set(k.filter((c) => c.func === "percent_of_total").map((c) => c.of))];
  let oe = "__ordered o";
  ne.length && (a.push(`__gt AS (SELECT ${ne.map((c) => `${u(c)} AS ${u(`__gt_${c}`)}`).join(", ")} FROM __ordered WHERE ${D} = 0)`), oe = "__ordered o CROSS JOIN __gt");
  const ie = [D, ...t.map((c, E) => u(`__p${E}`))].join(", "), he = k.map((c) => {
    const E = u(c.of);
    switch (c.func) {
      case "ratio": {
        const S = u(String(c.of2)), _ = c.nullValue === null || c.nullValue === void 0 ? "CAST(NULL AS DOUBLE)" : `CAST(${c.nullValue} AS DOUBLE)`;
        return `CASE WHEN ${S} IS NULL OR ${S} = 0 THEN ${_} ELSE CAST(${E} AS DOUBLE) / CAST(${S} AS DOUBLE) END`;
      }
      case "percent_of_total": {
        const S = u(`__gt_${c.of}`);
        return `CASE WHEN ${S} IS NULL OR ${S} = 0 THEN CAST(NULL AS DOUBLE) ELSE CAST(${E} AS DOUBLE) / CAST(${S} AS DOUBLE) END`;
      }
      case "rank":
      case "dense_rank": {
        const S = c.func === "rank" ? "RANK" : "DENSE_RANK", _ = c.desc === !1 ? "ASC" : "DESC";
        return `CASE WHEN ${E} IS NULL THEN NULL ELSE CAST(${S}() OVER (PARTITION BY ${ie} ORDER BY (${E} IS NULL) ASC, ${E} ${_}) AS BIGINT) END`;
      }
      case "cumulative":
      case "moving_avg": {
        const [S, _] = c.frame ? [c.frame[0], c.frame[1]] : [null, 0], h = `ROWS BETWEEN ${fe(S, !0)} AND ${fe(_, !1)}`;
        return `${c.func === "cumulative" ? `SUM(${E})` : `AVG(CAST(${E} AS DOUBLE))`} OVER (PARTITION BY ${ie} ORDER BY __ord ${h})`;
      }
      default:
        throw new d(`derived measure ${c.func}`);
    }
  });
  return a.push(`__derived AS (SELECT o.*, ${k.map((c, E) => `${he[E]} AS ${u(c.alias)}`).join(", ")} FROM ${oe})`), `WITH ${a.join(`,
`)}
SELECT ${Q} FROM __derived ORDER BY __ord${ee}`;
}
const be = {
  sum: "sum",
  avg: "avg",
  mean: "avg",
  min: "min",
  max: "max",
  count: "count",
  count_distinct: "count_distinct",
  distinct_count: "count_distinct",
  median: "median",
  stddev: "stddev",
  variance: "variance",
  percentile: "percentile",
  weighted_avg: "weighted_avg",
  wavg: "weighted_avg",
  weighted_mean: "weighted_avg",
  first: "first",
  last: "last",
  // REQ-AGG-008: need an order field
  array_agg: "array_agg"
  // REQ-AGG-009: ordered by its order field when given, else by value
}, Ie = {
  rank: "rank",
  dense_rank: "dense_rank",
  cumulative: "cumulative",
  running_avg: "moving_avg",
  moving_avg: "moving_avg",
  percent_of_total: "percent_of_total",
  percent_of_grand_total: "percent_of_total"
}, Re = {
  eq: "=",
  equals: "=",
  "=": "=",
  ne: "!=",
  not_eq: "!=",
  not_equals: "!=",
  "!=": "!=",
  gt: ">",
  ">": ">",
  gte: ">=",
  ">=": ">=",
  lt: "<",
  "<": "<",
  lte: "<=",
  "<=": "<=",
  in: "in",
  set: "in",
  not_in: "not_in",
  notin: "not_in",
  between: "between",
  contains: "contains",
  not_contains: "not_contains",
  notcontains: "not_contains",
  starts_with: "starts_with",
  startswith: "starts_with",
  ends_with: "ends_with",
  endswith: "ends_with",
  is_null: "is_null",
  isnull: "is_null",
  is_not_null: "is_not_null",
  isnotnull: "is_not_null"
}, Ue = "__null__";
function De(e) {
  if (!Number.isFinite(e)) throw new d("non-finite float in a path");
  if (e === 0) return Object.is(e, -0) ? "-0.0" : "0.0";
  const [n, i] = e.toExponential().split("e"), o = Number(i), t = n.startsWith("-"), s = n.replace("-", "").replace(".", "");
  let a;
  if (o >= -4 && o < 16)
    if (o >= 0) {
      const l = s.slice(0, o + 1).padEnd(o + 1, "0"), $ = s.slice(o + 1);
      a = `${l}.${$ || "0"}`;
    } else
      a = `0.${"0".repeat(-o - 1)}${s}`;
  else {
    const l = s.length > 1 ? `${s[0]}.${s.slice(1)}` : s, $ = Math.abs(o) < 10 ? `0${Math.abs(o)}` : String(Math.abs(o));
    a = `${l}e${o < 0 ? "-" : "+"}${$}`;
  }
  return t ? `-${a}` : a;
}
function ve(e, n) {
  let i, o;
  if (e == null)
    i = "none", o = null;
  else if (typeof e == "boolean")
    i = "bool", o = e;
  else if (typeof e == "bigint")
    i = "int", o = e.toString();
  else if (typeof e == "number")
    n === "integer" ? (i = "int", o = String(Math.trunc(e))) : (i = "float", o = De(e));
  else if (typeof e == "string")
    i = "str", o = e;
  else
    throw new d(`path value of type ${typeof e}`);
  return `{"type":${JSON.stringify(i)},"value":${JSON.stringify(o)},"version":1}`;
}
function He(e, n) {
  return `[${e.map((i, o) => JSON.stringify(ve(i, n[o]))).join(",")}]`;
}
function Y(e, n, i) {
  if (e == null || e === Ue) return null;
  if (n.family === "integer" || n.family === "floating" || n.family === "decimal") {
    if (typeof e == "number") return e;
    const o = typeof e == "string" && e.trim() !== "" ? Number(e) : NaN;
    if (!Number.isFinite(o)) throw new d(`${i}: non-numeric value for a numeric column`);
    if (n.family === "integer" && !Number.isInteger(o)) throw new d(`${i}: fractional value for an integer column`);
    return o;
  }
  if (n.family === "string") {
    if (typeof e != "string") throw new d(`${i}: non-text value for a text column`);
    return e;
  }
  if (n.family === "boolean" && typeof e == "boolean") return e;
  throw new d(`${i}: ${n.family} column`);
}
function Fe(e, n, i) {
  const o = String(n.type || "").trim().toLowerCase(), t = n.caseSensitive !== !1;
  if (o === "empty" || o === "blank" || o === "not_empty" || o === "not_blank") {
    if (i.family !== "string") throw new d(`${o} on a ${i.family} column`);
    const l = { op: "or", conditions: [{ field: e, op: "is_null" }, { field: e, op: "=", value: "" }] };
    return o.startsWith("not") ? { op: "and", conditions: [{ field: e, op: "is_not_null" }, { field: e, op: "!=", value: "" }] } : l;
  }
  const s = Re[o];
  if (!s) throw new d(`filter clause ${o}`);
  if (s === "is_null" || s === "is_not_null") return { field: e, op: s };
  if (s === "in" || s === "not_in") {
    const l = (Array.isArray(n.values) ? n.values : Array.isArray(n.value) ? n.value : [n.value]).map(($) => Y($, i, `filter ${e}`));
    return { field: e, op: s, value: l };
  }
  if (s === "between") {
    const l = Array.isArray(n.values) ? n.values : Array.isArray(n.value) ? n.value : [];
    if (l.length !== 2) throw new d("between needs two values");
    return { field: e, op: s, value: l.map(($) => Y($, i, `filter ${e}`)) };
  }
  if (s === "contains" || s === "not_contains" || s === "starts_with" || s === "ends_with") {
    if (i.family !== "string" || typeof n.value != "string") throw new d(`${s} on a non-text value`);
    return { field: e, op: s, value: n.value, caseSensitive: t };
  }
  const a = Y(n.value, i, `filter ${e}`);
  if (a === null) throw new d(`comparison ${s} with NULL on ${e}`);
  return { field: e, op: s, value: a, ...i.family === "string" ? { caseSensitive: t } : {} };
}
function Be(e, n) {
  const i = n[e.field];
  if (!i) throw new d(`filter on unknown column ${e.field}`);
  const o = e.clauses.map((t) => Fe(e.field, t, i));
  if (o.length === 0) throw new d("empty filter");
  return o.length === 1 ? o[0] : { op: e.operator === "OR" ? "or" : "and", conditions: o };
}
function ue(e, n, i) {
  const o = be[String(e.agg || "").toLowerCase()];
  if (!o) throw new d(`aggregation ${e.agg}`);
  if (!i[e.field]) throw new d(`measure on unknown column ${e.field}`);
  const t = { alias: n, agg: o, field: e.field };
  if (o === "weighted_avg") {
    if (!e.weightField || !i[e.weightField]) throw new d("weighted_avg without a known weight");
    t.weight = e.weightField;
  }
  if (o === "first" || o === "last") {
    if (!e.orderBy || !i[e.orderBy]) throw new d(`${o} without a known order field`);
    t.orderBy = e.orderBy;
  }
  if (o === "array_agg" && e.orderBy) {
    if (!i[e.orderBy]) throw new d("array_agg over an unknown order field");
    t.orderBy = e.orderBy;
  }
  if (o === "percentile") {
    const s = e.percentile === void 0 ? 0.5 : Number(e.percentile);
    if (!(s >= 0 && s <= 1)) throw new d("percentile outside [0, 1]");
    t.q = s;
  }
  return t;
}
function Me(e, n, i) {
  return e.map((o, t) => {
    const s = i[n[t]];
    if (o === "null") throw new d('ambiguous "null" label in an expanded path (send rowKey)');
    return Y(o, s, "expanded path");
  });
}
function xe(e, n) {
  if (e.kind !== "data") throw new d(`request kind ${e.kind}`);
  if (e.viewMode !== "pivot") throw new d(`view mode ${e.viewMode}`);
  if (e.layoutMode === "tabular") throw new d("tabular layout");
  if (e.showSubtotalFooters) throw new d("subtotal footers");
  if (e.groupAllowUnbalanced) throw new d("unbalanced grouping");
  if (e.customDimensions.length) throw new d("custom dimensions");
  if (e.measureAxis.placement === "rows") throw new d("measures on rows");
  const i = e.sortOptions || {};
  if (i.nulls && i.nulls !== "last" || i.columnSortBy || Object.values(i.fieldOptions || {}).some((r) => r && (r.sortType || r.sortKeyField || r.absoluteSort)) || Object.values(i.columnOptions || {}).some((r) => r && (r.sortType || r.sortKeyField || r.absoluteSort)))
    throw new d("sort options");
  e.rowFields.forEach((r) => {
    if (!n[r]) throw new d(`row field ${r}`);
  }), e.colFields.forEach((r) => {
    if (!n[r]) throw new d(`column field ${r}`);
  });
  const o = [], t = [];
  for (const r of e.valConfigs) {
    if (r.agg === "formula" || r.formula) throw new d("formula measures");
    const m = r.id || `${r.field}_${r.agg}`, w = r.windowFn ? Ie[String(r.windowFn).toLowerCase()] : void 0;
    if (r.windowFn && !w) throw new d(`window function ${r.windowFn}`);
    if (w) {
      if (e.colFields.length) throw new d("window measures with column pivot");
      o.push(ue(r, `__base_${m}`, n)), t.push({ alias: m, func: w, of: `__base_${m}` });
    } else
      o.push(ue(r, m, n));
  }
  if (!o.length) throw new d("no measures");
  const s = new Set(o.map((r) => r.alias)), a = [];
  for (const r of e.sorting) {
    if (r.sortType || r.sortKeyField) throw new d("sort type");
    if (e.rowFields.includes(r.id) || s.has(r.id)) {
      if (s.has(r.id) && e.colFields.length) throw new d("measure sort with column pivot");
      a.push({ field: r.id, desc: !!r.desc, ...r.absoluteSort ? { absolute: !0 } : {} });
    } else
      throw new d(`sort by ${r.id}`);
  }
  if (e.expandedAmbiguous) throw new d("expanded path with an empty or NULL member (send rowKey)");
  let l;
  if (e.expandedAll) {
    if (e.collapsedPaths.length) throw new d("collapsed paths under expand-all");
    l = null;
  } else
    l = e.expandedPaths.filter((r) => r.length > 0 && r.length < e.rowFields.length).map((r) => Me(r, e.rowFields, n));
  return { query: {
    dataset: e.table,
    rows: [...e.rowFields],
    measures: o,
    filters: e.filters.map((r) => Be(r, n)),
    sort: a,
    totals: "visible",
    expanded: l,
    columns: [...e.colFields],
    columnMembers: [],
    derived: t
  }, families: e.rowFields.map((r) => n[r].family) };
}
function je(e) {
  return e.map((n) => n == null ? "null" : String(n)).join("_");
}
function x(e) {
  if (typeof e == "bigint") {
    const n = Number(e);
    if (!Number.isSafeInteger(n)) throw new d("integer result beyond 2^53");
    return n;
  }
  return e;
}
function We(e) {
  return e.map((n) => n == null ? "" : String(n)).join("|||");
}
function Ge(e, n, i) {
  const { query: o, families: t } = n, s = o.rows, a = o.columnMembers || [], l = a.map(je);
  if (new Set(l).size !== l.length) throw new d("ambiguous pivot column labels");
  const $ = e.valConfigs.map((N) => N.id || `${N.field}_${N.agg}`), r = e.expandedAll ? null : new Set(e.expandedPaths.map((N) => N.join("|||"))), m = [];
  let w = null;
  for (const N of i) {
    const p = Number(x(N[D])), f = s.slice(0, p).map((O) => x(N[O])), g = {};
    if (a.length ? (a.forEach((O, F) => o.measures.forEach((b) => {
      g[`${l[F]}_${b.alias}`] = x(N[V(F, b.alias)]);
    })), o.measures.forEach((O) => {
      g[`__RowTotal__${O.alias}`] = x(N[O.alias]);
    })) : $.forEach((O) => {
      g[O] = x(N[O]);
    }), p === 0) {
      s.length === 0 && m.push({ ...g, _id: "All", _path: "All", _rowKey: "[]", depth: 0, _has_children: !1, _is_expanded: !1 }), w = {
        ...g,
        _id: "Grand Total",
        _path: "__grand_total__",
        _rowKey: "[]",
        depth: 0,
        _isTotal: !0,
        __isGrandTotal__: !0,
        _has_children: !1,
        _is_expanded: !1,
        ...s.length ? { [s[0]]: "Grand Total" } : {}
      };
      continue;
    }
    const C = We(f), y = p < s.length, A = f[p - 1], j = {
      ...g,
      _id: A == null ? "null" : String(A),
      _path: C,
      _rowKey: He(f, t),
      depth: p - 1,
      _has_children: y,
      _is_expanded: y && (r === null || r.has(f.map((O) => String(O)).join("|||")))
    };
    s.slice(0, p).forEach((O, F) => {
      j[O] = f[F];
    }), m.push(j);
  }
  return e.includeGrandTotal || e.showGrandTotal || (w = null), { rows: m, grandTotalRow: w, colCombinations: l };
}
function X(e) {
  const n = e;
  if (!n || typeof n.toArray != "function") throw new d("connection returned no Arrow table");
  return n.toArray().map((i) => {
    const o = i;
    return typeof o.toJSON == "function" ? o.toJSON() : i;
  });
}
const ke = [
  [/^(VARCHAR|TEXT|STRING|CHAR|BPCHAR)/, "string"],
  [/^(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|INT)/, "integer"],
  [/^(FLOAT|DOUBLE|REAL)/, "floating"],
  [/^DECIMAL/, "decimal"],
  [/^BOOL/, "boolean"],
  [/^DATE$/, "date"],
  [/^TIMESTAMP/, "timestamp"]
];
function Pe(e) {
  const n = e.toUpperCase(), i = ke.find(([o]) => o.test(n));
  return i ? i[1] : "other";
}
async function Ye(e, n) {
  const i = X(await e.query(`DESCRIBE ${'"' + n.replace(/"/g, '""') + '"'}`)), o = {};
  for (const t of i) {
    const s = String(t.column_type);
    o[String(t.column_name)] = { sqlType: s, family: Pe(s) };
  }
  return o;
}
async function Ke(e, n, i) {
  const o = await Ye(n, i), t = xe(e, o), s = { table: i, columns: o };
  if (t.query.columns && t.query.columns.length) {
    const l = X(await n.query(Oe(t.query, s)));
    t.query.columnMembers = l.map(($) => (t.query.columns || []).map((r) => x($[r])));
  }
  const a = Ce(t.query, s);
  return { ...Ge(e, t, X(await n.query(a))), sql: a };
}
export {
  d as EmitterUnsupported,
  He as canonicalPath,
  ve as canonicalScalar,
  Ye as describeColumns,
  Pe as familyOf,
  xe as planToQuery,
  De as pythonFloatRepr,
  Ke as runClientCore,
  Ge as shapeRows
};
