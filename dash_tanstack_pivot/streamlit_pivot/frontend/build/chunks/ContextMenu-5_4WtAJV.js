import { j as p } from "./index-DeulalCT.js";
import { a as o } from "./charts-DthW1Q8W.js";
const _ = 80, I = 220, H = ({ actions: e, theme: r, onClose: k, itemRefs: d, onItemMouseEnter: g, onKeyDown: S }) => /* @__PURE__ */ p.jsx(p.Fragment, { children: e.map((a, s) => {
  if (a === "separator")
    return /* @__PURE__ */ p.jsx(
      "div",
      {
        role: "presentation",
        style: { height: "1px", background: (r == null ? void 0 : r.border) || "#e0e0e0", margin: "4px 0" }
      },
      s
    );
  const f = !!a.disabled, x = Array.isArray(a.children) && a.children.length > 0;
  return /* @__PURE__ */ p.jsxs(
    "div",
    {
      ref: (l) => {
        d && (d.current[s] = l);
      },
      role: "menuitem",
      "aria-haspopup": x ? "menu" : void 0,
      "aria-disabled": f || void 0,
      tabIndex: f ? -1 : 0,
      title: a.title || (typeof a.label == "string" ? a.label : void 0),
      onMouseEnter: (l) => {
        l.currentTarget.style.backgroundColor = f ? "" : (r == null ? void 0 : r.hover) || "#f5f5f5", g && g(s, x, f);
      },
      onMouseLeave: (l) => {
        l.currentTarget.style.backgroundColor = "";
      },
      onClick: (l) => {
        if (f || x) {
          l.stopPropagation();
          return;
        }
        typeof a.onClick == "function" && a.onClick(), k();
      },
      onKeyDown: (l) => {
        f || (l.key === "Enter" || l.key === " " ? (l.preventDefault(), l.stopPropagation(), !x && typeof a.onClick == "function" && a.onClick(), x ? g && g(s, !0, !1) : k()) : S && S(l, s));
      },
      style: {
        padding: "7px 12px 7px 14px",
        cursor: f ? "not-allowed" : "pointer",
        display: "flex",
        alignItems: "center",
        gap: "8px",
        color: f ? (r == null ? void 0 : r.textSec) || "#757575" : (r == null ? void 0 : r.text) || "#111",
        opacity: f ? 0.48 : 1,
        userSelect: "none"
      },
      children: [
        a.icon && /* @__PURE__ */ p.jsx("span", { style: { color: (r == null ? void 0 : r.textSec) || "#757575", display: "flex", flexShrink: 0 }, children: a.icon }),
        /* @__PURE__ */ p.jsx("span", { style: { flex: 1 }, children: a.label }),
        x && /* @__PURE__ */ p.jsx("span", { style: { color: (r == null ? void 0 : r.textSec) || "#9ca3af", fontSize: "10px", marginLeft: "4px" }, children: "▶" })
      ]
    },
    s
  );
}) }), K = (e) => ({
  background: (e == null ? void 0 : e.surfaceBg) || (e == null ? void 0 : e.background) || "#fff",
  border: `1px solid ${(e == null ? void 0 : e.border) || "#ccc"}`,
  boxShadow: (e == null ? void 0 : e.shadowMd) || "0 4px 20px rgba(0,0,0,0.15)",
  padding: "6px 0",
  borderRadius: (e == null ? void 0 : e.radiusSm) || "8px",
  fontSize: "13px",
  minWidth: "190px",
  maxWidth: "300px",
  color: (e == null ? void 0 : e.text) || "#111"
}), G = ({ x: e, y: r, onClose: k, actions: d, theme: g }) => {
  const [S, a] = o.useState({ x: e, y: r }), [s, f] = o.useState(null), [x, l] = o.useState(null), T = o.useRef(null), P = o.useRef(null), E = o.useRef([]), A = o.useRef(null), v = o.useCallback((n, t = 1) => {
    const u = d.length;
    for (let i = 0; i < u; i++) {
      const y = (n + i * t + u * 2) % u, c = d[y];
      if (c && c !== "separator" && !c.disabled && E.current[y]) {
        E.current[y].focus();
        return;
      }
    }
  }, [d]);
  o.useEffect(() => {
    v(0, 1);
  }, [v]), o.useEffect(() => () => clearTimeout(A.current), []);
  const z = o.useCallback(() => {
    clearTimeout(A.current), A.current = setTimeout(() => {
      f(null), l(null);
    }, I);
  }, []), R = o.useCallback(() => {
    clearTimeout(A.current);
  }, []), L = o.useCallback((n) => {
    clearTimeout(A.current), A.current = setTimeout(() => {
      f(n);
      const t = E.current[n];
      t && l(t.getBoundingClientRect());
    }, s !== null ? 0 : _);
  }, [s]), F = o.useCallback((n, t, u) => {
    u || (t ? L(n) : (R(), f(null), l(null)));
  }, [L, R]), O = o.useCallback((n, t) => {
    if (n.key === "Escape")
      n.preventDefault(), k();
    else if (n.key === "ArrowDown")
      n.preventDefault(), v(t + 1, 1);
    else if (n.key === "ArrowUp")
      n.preventDefault(), v(t - 1, -1);
    else if (n.key === "Home")
      n.preventDefault(), v(0, 1);
    else if (n.key === "End")
      n.preventDefault(), v(d.length - 1, -1);
    else if (n.key === "ArrowRight" && s === null && Number.isFinite(t) && t >= 0) {
      const u = d[t];
      u && u !== "separator" && Array.isArray(u.children) && (n.preventDefault(), L(t));
    }
  }, [k, v, d, s, L]);
  o.useLayoutEffect(() => {
    const n = () => {
      var W;
      const t = window.innerWidth, u = window.innerHeight, i = (W = T.current) == null ? void 0 : W.getBoundingClientRect(), y = (i == null ? void 0 : i.width) || 200, c = (i == null ? void 0 : i.height) || d.length * 32 + 20, w = 8;
      let D = Math.max(w, Math.min(e, t - y - w)), Y = Math.max(w, Math.min(r, u - c - w));
      a({ x: D, y: Y });
    };
    return n(), window.addEventListener("resize", n), window.addEventListener("scroll", n, !0), () => {
      window.removeEventListener("resize", n), window.removeEventListener("scroll", n, !0);
    };
  }, [e, r, d.length]);
  const [B, N] = o.useState({ left: 0, top: 0 });
  o.useLayoutEffect(() => {
    if (!x || s === null) return;
    const n = P.current;
    if (!n) return;
    const t = window.innerWidth, u = window.innerHeight, i = n.offsetWidth || 200, y = n.offsetHeight || 200, c = 6;
    let w = x.right + 2;
    w + i > t - c && (w = x.left - i - 2);
    let D = x.top - 6;
    D + y > u - c && (D = u - y - c), N({ left: Math.max(c, w), top: Math.max(c, D) });
  }, [x, s]);
  const M = s !== null ? d[s] : null, b = M && Array.isArray(M.children) ? M.children : null, C = o.useRef([]), j = o.useCallback((n, t = 1) => {
    if (!b) return;
    const u = b.length;
    for (let i = 0; i < u; i++) {
      const y = (n + i * t + u * 2) % u, c = b[y];
      if (c && c !== "separator" && !c.disabled && C.current[y]) {
        C.current[y].focus();
        return;
      }
    }
  }, [b]);
  o.useEffect(() => {
    b && (C.current = C.current.slice(0, b.length));
  }, [b]);
  const U = o.useCallback((n, t) => {
    n.key === "Escape" || n.key === "ArrowLeft" ? (n.preventDefault(), n.stopPropagation(), f(null), l(null), s !== null && E.current[s] && E.current[s].focus()) : n.key === "ArrowDown" ? (n.preventDefault(), j(t + 1, 1)) : n.key === "ArrowUp" && (n.preventDefault(), j(t - 1, -1));
  }, [j, s]);
  return /* @__PURE__ */ p.jsxs(p.Fragment, { children: [
    /* @__PURE__ */ p.jsx(
      "div",
      {
        ref: T,
        role: "menu",
        style: { position: "fixed", top: S.y, left: S.x, zIndex: 10001, ...K(g) },
        onMouseLeave: z,
        onMouseEnter: R,
        children: /* @__PURE__ */ p.jsx(
          H,
          {
            actions: d,
            theme: g,
            onClose: k,
            itemRefs: E,
            onItemMouseEnter: F,
            onKeyDown: O
          }
        )
      }
    ),
    b && /* @__PURE__ */ p.jsx(
      "div",
      {
        ref: P,
        role: "menu",
        style: { position: "fixed", top: B.top, left: B.left, zIndex: 10002, ...K(g) },
        onMouseEnter: R,
        onMouseLeave: z,
        children: /* @__PURE__ */ p.jsx(
          H,
          {
            actions: b,
            theme: g,
            onClose: k,
            itemRefs: C,
            onKeyDown: U
          }
        )
      }
    )
  ] });
};
export {
  G as default
};
