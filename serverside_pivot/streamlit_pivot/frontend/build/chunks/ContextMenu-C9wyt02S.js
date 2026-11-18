import { j as f, I as h } from "./index-D4WX8QbF.js";
import { a as s } from "./charts-DthW1Q8W.js";
const Y = 80, _ = 220, F = ({ actions: n, theme: t, onClose: k, itemRefs: p, onItemMouseEnter: g, onKeyDown: A }) => /* @__PURE__ */ f.jsx(f.Fragment, { children: n.map(
  /** @param {any} [action] @param {any} [i] @returns {any} */
  (u, l) => {
    if (u === "separator")
      return /* @__PURE__ */ f.jsx(
        "div",
        {
          role: "presentation",
          style: { height: "1px", background: (t == null ? void 0 : t.border) || "#e0e0e0", margin: "4px 0" }
        },
        l
      );
    const d = !!u.disabled, x = Array.isArray(u.children) && u.children.length > 0;
    return /* @__PURE__ */ f.jsxs(
      "div",
      {
        ref: (c) => {
          p && (p.current[l] = c);
        },
        className: "sp-menu-item",
        role: "menuitem",
        "aria-haspopup": x ? "menu" : void 0,
        "aria-disabled": d || void 0,
        tabIndex: d ? -1 : 0,
        title: u.title || (typeof u.label == "string" ? u.label : void 0),
        onMouseEnter: () => {
          g && g(l, x, d);
        },
        onClick: (c) => {
          if (d || x) {
            c.stopPropagation();
            return;
          }
          typeof u.onClick == "function" && u.onClick(), k();
        },
        onKeyDown: (c) => {
          d || (c.key === "Enter" || c.key === " " ? (c.preventDefault(), c.stopPropagation(), !x && typeof u.onClick == "function" && u.onClick(), x ? g && g(l, !0, !1) : k()) : A && A(c, l));
        },
        style: {
          padding: "7px 10px",
          cursor: d ? "not-allowed" : "pointer",
          display: "flex",
          alignItems: "center",
          gap: "8px",
          color: d ? (t == null ? void 0 : t.textSec) || "#757575" : (t == null ? void 0 : t.text) || "#111",
          opacity: d ? 0.48 : 1,
          userSelect: "none"
        },
        children: [
          u.icon && /* @__PURE__ */ f.jsx("span", { style: { color: (t == null ? void 0 : t.textSec) || "#757575", display: "flex", flexShrink: 0 }, children: u.icon }),
          /* @__PURE__ */ f.jsx("span", { style: { flex: 1 }, children: u.label }),
          x && /* @__PURE__ */ f.jsx("span", { "aria-hidden": "true", style: { color: (t == null ? void 0 : t.textSec) || "#9ca3af", display: "inline-flex", alignItems: "center", marginLeft: "4px" }, children: h && h.ChevronRight ? /* @__PURE__ */ f.jsx(h.ChevronRight, {}) : ">" })
        ]
      },
      l
    );
  }
) }), N = (n) => ({
  background: `var(--sp-glass-bg, ${(n == null ? void 0 : n.surfaceBg) || (n == null ? void 0 : n.background) || "#fff"})`,
  backdropFilter: "blur(18px) saturate(180%)",
  WebkitBackdropFilter: "blur(18px) saturate(180%)",
  border: `1px solid ${(n == null ? void 0 : n.border) || "#ccc"}`,
  boxShadow: (n == null ? void 0 : n.shadow) || (n == null ? void 0 : n.shadowMd) || "0 14px 36px rgba(0,0,0,0.18)",
  padding: "5px",
  borderRadius: "10px",
  fontSize: "13px",
  minWidth: "190px",
  maxWidth: "300px",
  color: (n == null ? void 0 : n.text) || "#111"
}), q = ({ x: n, y: t, onClose: k, actions: p, theme: g }) => {
  const [A, u] = s.useState({ x: n, y: t }), [l, d] = s.useState(null), [x, c] = s.useState(null), P = s.useRef(null), T = s.useRef(null), E = s.useRef([]), R = s.useRef(null), v = s.useCallback((e, r = 1) => {
    const o = p.length;
    for (let i = 0; i < o; i++) {
      const b = (e + i * r + o * 2) % o, a = p[b];
      if (a && a !== "separator" && !a.disabled && E.current[b]) {
        E.current[b].focus();
        return;
      }
    }
  }, [p]);
  s.useEffect(() => {
    v(0, 1);
  }, [v]), s.useEffect(() => () => clearTimeout(R.current), []);
  const W = s.useCallback(() => {
    clearTimeout(R.current), R.current = setTimeout(() => {
      d(null), c(null);
    }, _);
  }, []), S = s.useCallback(() => {
    clearTimeout(R.current);
  }, []), j = s.useCallback((e) => {
    clearTimeout(R.current), R.current = setTimeout(() => {
      d(e);
      const r = E.current[e];
      r && c(r.getBoundingClientRect());
    }, l !== null ? 0 : Y);
  }, [l]), H = s.useCallback((e, r, o) => {
    o || (r ? j(e) : (S(), d(null), c(null)));
  }, [j, S]), I = s.useCallback((e, r) => {
    if (e.key === "Escape")
      e.preventDefault(), k();
    else if (e.key === "ArrowDown")
      e.preventDefault(), v(r + 1, 1);
    else if (e.key === "ArrowUp")
      e.preventDefault(), v(r - 1, -1);
    else if (e.key === "Home")
      e.preventDefault(), v(0, 1);
    else if (e.key === "End")
      e.preventDefault(), v(p.length - 1, -1);
    else if (e.key === "ArrowRight" && l === null && Number.isFinite(r) && r >= 0) {
      const o = p[r];
      o && o !== "separator" && Array.isArray(o.children) && (e.preventDefault(), j(r));
    }
  }, [k, v, p, l, j]);
  s.useLayoutEffect(() => {
    const e = () => {
      var B;
      const r = window.innerWidth, o = window.innerHeight, i = (B = P.current) == null ? void 0 : B.getBoundingClientRect(), b = (i == null ? void 0 : i.width) || 200, a = (i == null ? void 0 : i.height) || p.length * 32 + 20, w = 8;
      let D = Math.max(w, Math.min(n, r - b - w)), U = Math.max(w, Math.min(t, o - a - w));
      u({ x: D, y: U });
    };
    return e(), window.addEventListener("resize", e), window.addEventListener("scroll", e, !0), () => {
      window.removeEventListener("resize", e), window.removeEventListener("scroll", e, !0);
    };
  }, [n, t, p.length]);
  const [z, K] = s.useState({ left: 0, top: 0 });
  s.useLayoutEffect(() => {
    if (!x || l === null) return;
    const e = T.current;
    if (!e) return;
    const r = window.innerWidth, o = window.innerHeight, i = e.offsetWidth || 200, b = e.offsetHeight || 200, a = 6;
    let w = x.right + 2;
    w + i > r - a && (w = x.left - i - 2);
    let D = x.top - 6;
    D + b > o - a && (D = o - b - a), K({ left: Math.max(a, w), top: Math.max(a, D) });
  }, [x, l]);
  const L = l !== null ? p[l] : null, y = L && Array.isArray(L.children) ? L.children : null, C = s.useRef([]), M = s.useCallback((e, r = 1) => {
    if (!y) return;
    const o = y.length;
    for (let i = 0; i < o; i++) {
      const b = (e + i * r + o * 2) % o, a = y[b];
      if (a && a !== "separator" && !a.disabled && C.current[b]) {
        C.current[b].focus();
        return;
      }
    }
  }, [y]);
  s.useEffect(() => {
    y && (C.current = C.current.slice(0, y.length));
  }, [y]);
  const O = s.useCallback((e, r) => {
    e.key === "Escape" || e.key === "ArrowLeft" ? (e.preventDefault(), e.stopPropagation(), d(null), c(null), l !== null && E.current[l] && E.current[l].focus()) : e.key === "ArrowDown" ? (e.preventDefault(), M(r + 1, 1)) : e.key === "ArrowUp" && (e.preventDefault(), M(r - 1, -1));
  }, [M, l]);
  return /* @__PURE__ */ f.jsxs(f.Fragment, { children: [
    /* @__PURE__ */ f.jsx(
      "div",
      {
        ref: P,
        className: "sp-menu-surface",
        role: "menu",
        style: { position: "fixed", top: A.y, left: A.x, zIndex: 10001, ...N(g) },
        onMouseLeave: W,
        onMouseEnter: S,
        children: /* @__PURE__ */ f.jsx(
          F,
          {
            actions: p,
            theme: g,
            onClose: k,
            itemRefs: E,
            onItemMouseEnter: H,
            onKeyDown: I
          }
        )
      }
    ),
    y && /* @__PURE__ */ f.jsx(
      "div",
      {
        ref: T,
        className: "sp-menu-surface",
        role: "menu",
        style: { position: "fixed", top: z.top, left: z.left, zIndex: 10002, ...N(g) },
        onMouseEnter: S,
        onMouseLeave: W,
        children: /* @__PURE__ */ f.jsx(
          F,
          {
            actions: y,
            theme: g,
            onClose: k,
            itemRefs: C,
            onKeyDown: O
          }
        )
      }
    )
  ] });
};
export {
  q as default
};
