import { j as t } from "./index-Bu1c7lM2.js";
import "./charts-DthW1Q8W.js";
import { D as u } from "./DetailSurfaceContent-Dq9t2bwQ.js";
function l({
  detailState: o,
  onClose: i,
  onPageChange: a,
  onSort: e,
  onFilter: n,
  theme: r,
  height: s = 320
}) {
  return o ? /* @__PURE__ */ t.jsx("div", { style: {
    position: "absolute",
    left: 0,
    right: 0,
    bottom: 0,
    height: `${Math.max(220, Number(s) || 320)}px`,
    borderTop: `1px solid ${r.border}`,
    background: r.surfaceBg || r.background,
    zIndex: 85,
    boxShadow: "0 -10px 24px rgba(15,23,42,0.12)",
    overflow: "hidden"
  }, children: /* @__PURE__ */ t.jsx(
    u,
    {
      detailState: o,
      onClose: i,
      onPageChange: a,
      onSort: e,
      onFilter: n,
      theme: r
    }
  ) }) : null;
}
export {
  l as default
};
