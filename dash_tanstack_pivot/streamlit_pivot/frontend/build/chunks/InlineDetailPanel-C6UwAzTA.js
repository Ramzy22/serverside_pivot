import { j as t } from "./index--Q_tEdOr.js";
import "./charts-DthW1Q8W.js";
import { D as u } from "./DetailSurfaceContent-gK5x2DRi.js";
function f({
  detailState: o,
  onClose: e,
  onPageChange: i,
  onSort: n,
  onFilter: a,
  theme: r,
  width: p,
  top: x,
  height: d = 280
}) {
  return o ? /* @__PURE__ */ t.jsx("div", { style: {
    position: "absolute",
    top: `${x}px`,
    left: 0,
    width: `${p}px`,
    height: `${Math.max(220, Number(d) || 280)}px`,
    zIndex: 95,
    border: `1px solid ${r.border}`,
    borderRadius: "14px",
    overflow: "hidden",
    background: r.surfaceBg || r.background,
    boxShadow: "0 18px 40px rgba(15,23,42,0.16)"
  }, children: /* @__PURE__ */ t.jsx(
    u,
    {
      detailState: o,
      onClose: e,
      onPageChange: i,
      onSort: n,
      onFilter: a,
      theme: r,
      compact: !0
    }
  ) }) : null;
}
export {
  f as default
};
