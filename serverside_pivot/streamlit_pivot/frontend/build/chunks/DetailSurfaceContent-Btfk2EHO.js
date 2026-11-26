import { j as e, I as a } from "./index-BXM8DLHY.js";
import { a as B } from "./charts-DthW1Q8W.js";
function R({
  detailState: i,
  onClose: S,
  onPageChange: n,
  onSort: o,
  onFilter: y,
  theme: r,
  compact: l = !1
}) {
  const [j, h] = B.useState("");
  if (B.useEffect(() => {
    h(i && typeof i.filterText == "string" ? i.filterText : "");
  }, [i && i.filterText]), !i) return null;
  const {
    loading: c,
    rows: d = [],
    columns: g = [],
    page: s = 0,
    pageSize: C = 100,
    totalRows: k = 0,
    sortCol: w = null,
    sortDir: v = "asc",
    title: $ = "Detail",
    rowPath: u = ""
  } = i, f = Array.isArray(g) && g.length > 0 ? g.map((t) => typeof t == "string" ? t : t.id).filter(Boolean) : d[0] ? Object.keys(d[0]).filter((t) => !t.startsWith("_")) : [], x = Math.max(1, Math.ceil((k || 0) / Math.max(1, C || 100))), z = (t) => {
    if (o) {
      if (w === t) {
        o(t, v === "asc" ? "desc" : "asc");
        return;
      }
      o(t, "asc");
    }
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }, children: [
    /* @__PURE__ */ e.jsxs("div", { style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "12px",
      padding: l ? "10px 12px" : "14px 16px",
      borderBottom: `1px solid ${r.border}`,
      background: r.headerBg
    }, children: [
      /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0 }, children: [
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: l ? "12px" : "13px", fontWeight: 700, color: r.text }, children: $ }),
        u && /* @__PURE__ */ e.jsx("div", { style: {
          fontSize: "11px",
          color: r.textSec,
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          maxWidth: l ? "320px" : "460px"
        }, children: u })
      ] }),
      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", flexShrink: 0 }, children: [
        /* @__PURE__ */ e.jsxs("div", { style: {
          display: "flex",
          alignItems: "center",
          gap: "6px",
          padding: "0 10px",
          border: `1px solid ${r.border}`,
          borderRadius: "999px",
          background: r.surfaceBg || r.background
        }, children: [
          /* @__PURE__ */ e.jsx(a.Search, {}),
          /* @__PURE__ */ e.jsx(
            "input",
            {
              type: "text",
              value: j,
              placeholder: "Filter rows...",
              onChange: (t) => h(t.target.value),
              onKeyDown: (t) => {
                t.key === "Enter" && y && y(j);
              },
              style: {
                border: "none",
                outline: "none",
                background: "transparent",
                color: r.text,
                fontSize: "12px",
                width: l ? "140px" : "180px",
                padding: "8px 0"
              }
            }
          )
        ] }),
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            onClick: S,
            style: {
              width: "28px",
              height: "28px",
              borderRadius: "999px",
              border: `1px solid ${r.border}`,
              background: r.surfaceBg || r.background,
              color: r.textSec,
              cursor: "pointer",
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center"
            },
            title: "Close detail",
            children: /* @__PURE__ */ e.jsx(a.Close, {})
          }
        )
      ] })
    ] }),
    /* @__PURE__ */ e.jsx("div", { style: { flex: 1, minHeight: 0, overflow: "auto", background: r.surfaceBg || r.background }, children: c ? /* @__PURE__ */ e.jsx("div", { style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      height: "100%",
      color: r.textSec,
      fontSize: "13px"
    }, children: "Loading detail..." }) : /* @__PURE__ */ e.jsxs("table", { style: { width: "100%", borderCollapse: "collapse", fontSize: "12px" }, children: [
      /* @__PURE__ */ e.jsx("thead", { children: /* @__PURE__ */ e.jsx("tr", { children: f.map(
        /** @param {any} [columnId] @returns {any} */
        (t) => /* @__PURE__ */ e.jsx(
          "th",
          {
            onClick: () => z(t),
            style: {
              position: "sticky",
              top: 0,
              zIndex: 1,
              textAlign: "left",
              padding: l ? "8px 10px" : "10px 12px",
              background: r.headerBg,
              color: r.text,
              borderBottom: `1px solid ${r.border}`,
              cursor: o ? "pointer" : "default",
              whiteSpace: "nowrap"
            },
            children: /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "4px" }, children: [
              /* @__PURE__ */ e.jsx("span", { children: t }),
              w === t ? /* @__PURE__ */ e.jsx("span", { style: { display: "inline-flex", alignItems: "center" }, children: v === "asc" ? /* @__PURE__ */ e.jsx(a.SortAsc, {}) : /* @__PURE__ */ e.jsx(a.SortDesc, {}) }) : null
            ] })
          },
          t
        )
      ) }) }),
      /* @__PURE__ */ e.jsx("tbody", { children: d.length === 0 ? /* @__PURE__ */ e.jsx("tr", { children: /* @__PURE__ */ e.jsx(
        "td",
        {
          colSpan: Math.max(f.length, 1),
          style: {
            padding: "18px 12px",
            textAlign: "center",
            color: r.textSec,
            borderBottom: `1px solid ${r.border}`
          },
          children: "No detail rows"
        }
      ) }) : d.map(
        /** @param {any} [row] @param {any} [index] @returns {any} */
        (t, b) => /* @__PURE__ */ e.jsx("tr", { style: {
          background: b % 2 === 0 ? r.surfaceBg || r.background : r.headerSubtleBg
        }, children: f.map(
          /** @param {any} [columnId] @returns {any} */
          (p) => /* @__PURE__ */ e.jsx(
            "td",
            {
              style: {
                padding: l ? "7px 10px" : "9px 12px",
                borderBottom: `1px solid ${r.border}`,
                color: r.text,
                whiteSpace: "nowrap"
              },
              children: t[p] !== void 0 && t[p] !== null ? String(t[p]) : ""
            },
            `${p}:${b}`
          )
        ) }, `${i.rowKey || u || "detail"}:${b}`)
      ) })
    ] }) }),
    /* @__PURE__ */ e.jsxs("div", { style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "12px",
      padding: l ? "10px 12px" : "12px 16px",
      borderTop: `1px solid ${r.border}`,
      background: r.headerBg,
      fontSize: "12px",
      color: r.textSec
    }, children: [
      /* @__PURE__ */ e.jsxs("span", { children: [
        "Page ",
        s + 1,
        " of ",
        x,
        " ",
        "(",
        k || 0,
        " rows)"
      ] }),
      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px" }, children: [
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            onClick: () => n && n(Math.max(s - 1, 0)),
            disabled: c || s <= 0,
            style: {
              padding: "6px 10px",
              borderRadius: "8px",
              border: `1px solid ${r.border}`,
              background: r.surfaceBg || r.background,
              color: r.text,
              cursor: s <= 0 ? "default" : "pointer",
              opacity: s <= 0 ? 0.45 : 1
            },
            children: "Previous"
          }
        ),
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            onClick: () => n && n(s + 1),
            disabled: c || s >= x - 1,
            style: {
              padding: "6px 10px",
              borderRadius: "8px",
              border: `1px solid ${r.border}`,
              background: r.surfaceBg || r.background,
              color: r.text,
              cursor: s >= x - 1 ? "default" : "pointer",
              opacity: s >= x - 1 ? 0.45 : 1
            },
            children: "Next"
          }
        )
      ] })
    ] })
  ] });
}
export {
  R as D
};
