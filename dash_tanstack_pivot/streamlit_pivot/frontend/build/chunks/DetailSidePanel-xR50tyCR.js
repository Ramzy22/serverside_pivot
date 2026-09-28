import { j as e } from "./index-Bu1c7lM2.js";
import "./charts-DthW1Q8W.js";
import { D as l } from "./DetailSurfaceContent-Dq9t2bwQ.js";
function m({
  detailState: i,
  onClose: n,
  onPageChange: t,
  onSort: a,
  onFilter: d,
  theme: r,
  width: o = 480
}) {
  return i ? /* @__PURE__ */ e.jsx("div", { style: {
    width: `${Math.max(320, Number(o) || 480)}px`,
    minWidth: `${Math.max(320, Number(o) || 480)}px`,
    maxWidth: "52vw",
    borderLeft: `1px solid ${r.border}`,
    background: r.surfaceBg || r.background,
    display: "flex",
    flexDirection: "column",
    minHeight: 0,
    overflow: "hidden"
  }, children: /* @__PURE__ */ e.jsx(
    l,
    {
      detailState: i,
      onClose: n,
      onPageChange: t,
      onSort: a,
      onFilter: d,
      theme: r
    }
  ) }) : null;
}
export {
  m as default
};
