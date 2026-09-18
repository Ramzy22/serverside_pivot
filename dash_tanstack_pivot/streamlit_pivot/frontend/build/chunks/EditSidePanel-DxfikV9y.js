import { j as e, z as g } from "./index-DeulalCT.js";
import "./charts-DthW1Q8W.js";
const p = (t, l = 4) => {
  if (t == null || t === "") return "--";
  const d = Number(t);
  return Number.isFinite(d) ? d.toLocaleString(void 0, { maximumFractionDigits: l }) : String(t);
};
function C({
  editedCells: t,
  propagationLog: l,
  onRevertCell: d,
  onRevertAll: f,
  onRevertSelected: b,
  onReapplyPropagation: h,
  onToggleDisplayMode: m,
  displayMode: y,
  onClose: k,
  theme: r,
  width: $ = 320,
  pendingPropagation: j,
  propagationMethod: s,
  onPropagationMethodChange: v,
  onConfirmPropagation: w,
  onCancelPropagation: z
}) {
  const c = j && j.length > 0, a = t && t.length > 0, W = a && t.some((i) => i.direct), B = !c && W && typeof h == "function";
  if (!a && !c) return null;
  const S = Math.max(260, Math.min(Number($) || 320, 480)), x = r.surfaceBg || r.background, o = r.border, n = r.primary || "#4F46E5";
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      "data-pivot-edit-panel": "true",
      style: {
        width: `${S}px`,
        minWidth: `${S}px`,
        borderLeft: `1px solid ${o}`,
        background: x,
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        overflow: "hidden",
        fontSize: "11px",
        color: r.text
      },
      children: [
        /* @__PURE__ */ e.jsxs("div", { style: {
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "6px 10px",
          borderBottom: `1px solid ${o}`,
          background: r.headerBg || x,
          flexShrink: 0
        }, children: [
          /* @__PURE__ */ e.jsxs("span", { style: { fontWeight: 700, fontSize: "12px", display: "flex", alignItems: "center", gap: "6px" }, children: [
            /* @__PURE__ */ e.jsx(g.Edit, {}),
            "Edit Inspector"
          ] }),
          /* @__PURE__ */ e.jsx(
            "button",
            {
              type: "button",
              onClick: k,
              style: {
                border: "none",
                background: "transparent",
                cursor: "pointer",
                color: r.textSec,
                padding: "2px",
                display: "flex"
              },
              children: /* @__PURE__ */ e.jsx(g.Close, {})
            }
          )
        ] }),
        a && /* @__PURE__ */ e.jsxs("div", { style: {
          display: "flex",
          gap: "6px",
          padding: "6px 10px",
          borderBottom: `1px solid ${o}`,
          flexShrink: 0,
          background: r.headerBg || x
        }, children: [
          /* @__PURE__ */ e.jsx(
            "button",
            {
              type: "button",
              onClick: m,
              style: {
                flex: 1,
                border: `1px solid ${o}`,
                borderRadius: "5px",
                background: y === "original" ? `${n}15` : "transparent",
                color: r.text,
                padding: "4px 8px",
                fontSize: "10px",
                fontWeight: 600,
                cursor: "pointer"
              },
              children: y === "original" ? "Show Edited" : "Show Original"
            }
          ),
          /* @__PURE__ */ e.jsxs(
            "button",
            {
              type: "button",
              onClick: () => {
                typeof b == "function" && t && t.length > 0 ? b(t) : typeof f == "function" && f();
              },
              style: {
                flex: 1,
                border: "1px solid rgba(220, 38, 38, 0.3)",
                borderRadius: "5px",
                background: "rgba(220, 38, 38, 0.08)",
                color: "#B91C1C",
                padding: "4px 8px",
                fontSize: "10px",
                fontWeight: 600,
                cursor: "pointer"
              },
              children: [
                "Revert Selected (",
                t ? t.length : 0,
                ")"
              ]
            }
          )
        ] }),
        c && /* @__PURE__ */ e.jsxs("div", { style: {
          padding: "8px 10px",
          borderBottom: `1px solid ${o}`,
          background: `${n}08`,
          flexShrink: 0
        }, children: [
          /* @__PURE__ */ e.jsx("div", { style: { fontWeight: 700, fontSize: "11px", marginBottom: "6px", color: r.text }, children: "Propagation Method" }),
          /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: r.textSec, marginBottom: "8px" }, children: "Editing a group row will distribute the change to child rows." }),
          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "4px", marginBottom: "8px" }, children: [
            { value: "equal", label: "Equal", desc: "Distribute change evenly across child rows" },
            { value: "proportional", label: "Proportional", desc: "Preserve current child ratios" }
          ].map((i) => /* @__PURE__ */ e.jsxs("label", { style: {
            display: "flex",
            alignItems: "center",
            gap: "6px",
            padding: "4px 8px",
            borderRadius: "4px",
            cursor: "pointer",
            background: s === i.value ? `${n}14` : "transparent",
            border: `1px solid ${s === i.value ? n + "40" : o}`
          }, children: [
            /* @__PURE__ */ e.jsx(
              "input",
              {
                type: "radio",
                name: "propagation-method",
                value: i.value,
                checked: s === i.value,
                onChange: () => v(i.value),
                style: { margin: 0, accentColor: n }
              }
            ),
            /* @__PURE__ */ e.jsxs("div", { children: [
              /* @__PURE__ */ e.jsx("div", { style: { fontWeight: 600, fontSize: "10px", color: r.text }, children: i.label }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "9px", color: r.textSec }, children: i.desc })
            ] })
          ] }, i.value)) }),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px" }, children: [
            /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: w,
                style: {
                  flex: 1,
                  border: `1px solid ${n}40`,
                  borderRadius: "5px",
                  background: `${n}18`,
                  color: n,
                  padding: "5px 8px",
                  fontSize: "10px",
                  fontWeight: 700,
                  cursor: "pointer"
                },
                children: "Apply Edit"
              }
            ),
            /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: z,
                style: {
                  flex: 1,
                  border: `1px solid ${o}`,
                  borderRadius: "5px",
                  background: "transparent",
                  color: r.textSec,
                  padding: "5px 8px",
                  fontSize: "10px",
                  fontWeight: 600,
                  cursor: "pointer"
                },
                children: "Cancel"
              }
            )
          ] })
        ] }),
        a && /* @__PURE__ */ e.jsx("div", { style: { flex: 1, overflowY: "auto", padding: "4px 0" }, children: t.map((i) => /* @__PURE__ */ e.jsxs(
          "div",
          {
            style: {
              padding: "5px 10px",
              borderBottom: `1px solid ${o}22`,
              display: "flex",
              flexDirection: "column",
              gap: "2px"
            },
            children: [
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontWeight: 600, color: r.text, fontSize: "10px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "60%" }, children: i.colId }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", alignItems: "center" }, children: [
                  i.propagated && /* @__PURE__ */ e.jsx("span", { style: {
                    fontSize: "9px",
                    padding: "1px 4px",
                    borderRadius: "3px",
                    background: `${n}18`,
                    color: n,
                    fontWeight: 600
                  }, children: "propagated" }),
                  i.direct && /* @__PURE__ */ e.jsx("span", { style: {
                    fontSize: "9px",
                    padding: "1px 4px",
                    borderRadius: "3px",
                    background: "rgba(16, 185, 129, 0.12)",
                    color: "#047857",
                    fontWeight: 600
                  }, children: "direct" }),
                  d && /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => d(i.rowId, i.colId),
                      title: "Revert this cell to original",
                      style: {
                        border: "none",
                        background: "transparent",
                        cursor: "pointer",
                        color: r.textSec,
                        padding: "1px",
                        fontSize: "10px",
                        display: "flex"
                      },
                      children: /* @__PURE__ */ e.jsx(g.Close, {})
                    }
                  )
                ] })
              ] }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "8px", fontSize: "10px", color: r.textSec }, children: /* @__PURE__ */ e.jsxs("span", { children: [
                "Row: ",
                /* @__PURE__ */ e.jsx("span", { style: { color: r.text, fontWeight: 500 }, children: String(i.rowId).split("|||").pop() })
              ] }) }),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "10px", fontSize: "10px" }, children: [
                /* @__PURE__ */ e.jsxs("span", { style: { color: r.textSec }, children: [
                  "Original: ",
                  /* @__PURE__ */ e.jsx("span", { style: { color: r.text }, children: p(i.originalValue) })
                ] }),
                i.currentValue !== void 0 && /* @__PURE__ */ e.jsxs("span", { style: { color: r.textSec }, children: [
                  "Now: ",
                  /* @__PURE__ */ e.jsx("span", { style: { color: n, fontWeight: 600 }, children: p(i.currentValue) })
                ] })
              ] })
            ]
          },
          `${i.rowId}:::${i.colId}`
        )) }),
        B && /* @__PURE__ */ e.jsxs("div", { style: {
          padding: "6px 10px",
          borderTop: `1px solid ${o}`,
          borderBottom: `1px solid ${o}`,
          background: `${n}06`,
          flexShrink: 0
        }, children: [
          /* @__PURE__ */ e.jsx("div", { style: { fontWeight: 700, fontSize: "10px", marginBottom: "4px", color: r.text }, children: "Change Propagation" }),
          /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "3px" }, children: [
            { value: "equal", label: "Equal" },
            { value: "proportional", label: "Proportional" }
          ].map((i) => /* @__PURE__ */ e.jsxs("label", { style: {
            display: "flex",
            alignItems: "center",
            gap: "5px",
            padding: "3px 6px",
            borderRadius: "3px",
            cursor: "pointer",
            fontSize: "10px",
            background: s === i.value ? `${n}14` : "transparent",
            border: `1px solid ${s === i.value ? n + "40" : o}`
          }, children: [
            /* @__PURE__ */ e.jsx(
              "input",
              {
                type: "radio",
                name: "reapply-method",
                value: i.value,
                checked: s === i.value,
                onChange: () => {
                  v(i.value), h(t.filter((u) => u.direct), i.value);
                },
                style: { margin: 0, accentColor: n }
              }
            ),
            /* @__PURE__ */ e.jsx("span", { style: { fontWeight: 600 }, children: i.label })
          ] }, i.value)) })
        ] }),
        l && l.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: {
          borderTop: `1px solid ${o}`,
          flexShrink: 0,
          maxHeight: "120px",
          overflowY: "auto"
        }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: {
            padding: "4px 10px",
            fontWeight: 700,
            fontSize: "10px",
            color: r.textSec,
            background: r.headerBg || x,
            borderBottom: `1px solid ${o}22`,
            position: "sticky",
            top: 0
          }, children: [
            "Propagation Log (",
            l.length,
            ")"
          ] }),
          l.slice().reverse().slice(0, 10).map((i, u) => /* @__PURE__ */ e.jsxs(
            "div",
            {
              style: {
                padding: "3px 10px",
                fontSize: "10px",
                display: "flex",
                gap: "6px",
                alignItems: "center",
                color: r.textSec,
                borderBottom: `1px solid ${o}11`
              },
              children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontWeight: 600, color: n }, children: i.strategy || "equal" }),
                /* @__PURE__ */ e.jsx("span", { children: i.targetColumn || "?" }),
                /* @__PURE__ */ e.jsxs("span", { children: [
                  p(i.fromValue, 2),
                  " → ",
                  p(i.toValue, 2)
                ] }),
                /* @__PURE__ */ e.jsxs("span", { children: [
                  "(",
                  i.updatedRowCount || 0,
                  "r)"
                ] })
              ]
            },
            u
          ))
        ] })
      ]
    }
  );
}
export {
  C as default
};
