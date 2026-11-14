import { j as e, z as v } from "./index-C8_65yWw.js";
import { a as B } from "./charts-DthW1Q8W.js";
function M({
  detailState: s,
  onClose: C,
  onPageChange: l,
  onSort: n,
  onFilter: b,
  theme: r,
  compact: o = !1
}) {
  const [y, j] = B.useState("");
  if (B.useEffect(() => {
    j(s && typeof s.filterText == "string" ? s.filterText : "");
  }, [s && s.filterText]), !s) return null;
  const {
    loading: a,
    rows: d = [],
    columns: c = [],
    page: i = 0,
    pageSize: S = 100,
    totalRows: h = 0,
    sortCol: k = null,
    sortDir: w = "asc",
    title: $ = "Detail",
    rowPath: g = ""
  } = s, u = Array.isArray(c) && c.length > 0 ? c.map((t) => typeof t == "string" ? t : t.id).filter(Boolean) : d[0] ? Object.keys(d[0]).filter((t) => !t.startsWith("_")) : [], x = Math.max(1, Math.ceil((h || 0) / Math.max(1, S || 100))), z = (t) => {
    if (n) {
      if (k === t) {
        n(t, w === "asc" ? "desc" : "asc");
        return;
      }
      n(t, "asc");
    }
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }, children: [
    /* @__PURE__ */ e.jsxs("div", { style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "12px",
      padding: o ? "10px 12px" : "14px 16px",
      borderBottom: `1px solid ${r.border}`,
      background: r.headerBg
    }, children: [
      /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0 }, children: [
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: o ? "12px" : "13px", fontWeight: 700, color: r.text }, children: $ }),
        g && /* @__PURE__ */ e.jsx("div", { style: {
          fontSize: "11px",
          color: r.textSec,
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          maxWidth: o ? "320px" : "460px"
        }, children: g })
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
          /* @__PURE__ */ e.jsx(v.Search, {}),
          /* @__PURE__ */ e.jsx(
            "input",
            {
              type: "text",
              value: y,
              placeholder: "Filter rows...",
              onChange: (t) => j(t.target.value),
              onKeyDown: (t) => {
                t.key === "Enter" && b && b(y);
              },
              style: {
                border: "none",
                outline: "none",
                background: "transparent",
                color: r.text,
                fontSize: "12px",
                width: o ? "140px" : "180px",
                padding: "8px 0"
              }
            }
          )
        ] }),
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            onClick: C,
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
            children: /* @__PURE__ */ e.jsx(v.Close, {})
          }
        )
      ] })
    ] }),
    /* @__PURE__ */ e.jsx("div", { style: { flex: 1, minHeight: 0, overflow: "auto", background: r.surfaceBg || r.background }, children: a ? /* @__PURE__ */ e.jsx("div", { style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      height: "100%",
      color: r.textSec,
      fontSize: "13px"
    }, children: "Loading detail..." }) : /* @__PURE__ */ e.jsxs("table", { style: { width: "100%", borderCollapse: "collapse", fontSize: "12px" }, children: [
      /* @__PURE__ */ e.jsx("thead", { children: /* @__PURE__ */ e.jsx("tr", { children: u.map(
        /** @param {any} [columnId] @returns {any} */
        (t) => /* @__PURE__ */ e.jsxs(
          "th",
          {
            onClick: () => z(t),
            style: {
              position: "sticky",
              top: 0,
              zIndex: 1,
              textAlign: "left",
              padding: o ? "8px 10px" : "10px 12px",
              background: r.headerBg,
              color: r.text,
              borderBottom: `1px solid ${r.border}`,
              cursor: n ? "pointer" : "default",
              whiteSpace: "nowrap"
            },
            children: [
              t,
              k === t ? w === "asc" ? " ▲" : " ▼" : ""
            ]
          },
          t
        )
      ) }) }),
      /* @__PURE__ */ e.jsx("tbody", { children: d.length === 0 ? /* @__PURE__ */ e.jsx("tr", { children: /* @__PURE__ */ e.jsx(
        "td",
        {
          colSpan: Math.max(u.length, 1),
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
        (t, f) => /* @__PURE__ */ e.jsx("tr", { style: {
          background: f % 2 === 0 ? r.surfaceBg || r.background : r.headerSubtleBg
        }, children: u.map(
          /** @param {any} [columnId] @returns {any} */
          (p) => /* @__PURE__ */ e.jsx(
            "td",
            {
              style: {
                padding: o ? "7px 10px" : "9px 12px",
                borderBottom: `1px solid ${r.border}`,
                color: r.text,
                whiteSpace: "nowrap"
              },
              children: t[p] !== void 0 && t[p] !== null ? String(t[p]) : ""
            },
            `${p}:${f}`
          )
        ) }, `${s.rowKey || g || "detail"}:${f}`)
      ) })
    ] }) }),
    /* @__PURE__ */ e.jsxs("div", { style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "12px",
      padding: o ? "10px 12px" : "12px 16px",
      borderTop: `1px solid ${r.border}`,
      background: r.headerBg,
      fontSize: "12px",
      color: r.textSec
    }, children: [
      /* @__PURE__ */ e.jsxs("span", { children: [
        "Page ",
        i + 1,
        " of ",
        x,
        " ",
        "(",
        h || 0,
        " rows)"
      ] }),
      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px" }, children: [
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            onClick: () => l && l(Math.max(i - 1, 0)),
            disabled: a || i <= 0,
            style: {
              padding: "6px 10px",
              borderRadius: "8px",
              border: `1px solid ${r.border}`,
              background: r.surfaceBg || r.background,
              color: r.text,
              cursor: i <= 0 ? "default" : "pointer",
              opacity: i <= 0 ? 0.45 : 1
            },
            children: "Previous"
          }
        ),
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            onClick: () => l && l(i + 1),
            disabled: a || i >= x - 1,
            style: {
              padding: "6px 10px",
              borderRadius: "8px",
              border: `1px solid ${r.border}`,
              background: r.surfaceBg || r.background,
              color: r.text,
              cursor: i >= x - 1 ? "default" : "pointer",
              opacity: i >= x - 1 ? 0.45 : 1
            },
            children: "Next"
          }
        )
      ] })
    ] })
  ] });
}
export {
  M as D
};
