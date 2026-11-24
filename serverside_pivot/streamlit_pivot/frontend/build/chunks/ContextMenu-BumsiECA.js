import { W as m, j as d, X as U, I as W } from "./index-D68SRNaU.js";
import { a as r } from "./charts-DthW1Q8W.js";
const _ = 80, $ = 220, F = ({ actions: n, theme: l, onClose: y, itemRefs: f, onItemMouseEnter: v, onKeyDown: A }) => /* @__PURE__ */ d.jsx(d.Fragment, { children: n.map(
  /** @param {any} [action] @param {any} [i] @returns {any} */
  (i, o) => {
    if (i === "separator")
      return /* @__PURE__ */ d.jsx(
        "div",
        {
          role: "presentation",
          style: { height: "1px", background: (l == null ? void 0 : l.border) || "var(--sp-border, #e2e8f0)", margin: "4px 0" }
        },
        o
      );
    const p = !!i.disabled, x = Array.isArray(i.children) && i.children.length > 0;
    return /* @__PURE__ */ d.jsxs(
      "div",
      {
        ref: (c) => {
          f && (f.current[o] = c);
        },
        className: "sp-menu-item",
        role: "menuitem",
        "aria-haspopup": x ? "menu" : void 0,
        "aria-disabled": p || void 0,
        tabIndex: p ? -1 : 0,
        title: i.title || (typeof i.label == "string" ? i.label : void 0),
        onMouseEnter: () => {
          v && v(o, x, p);
        },
        onClick: (c) => {
          if (p || x) {
            c.stopPropagation();
            return;
          }
          typeof i.onClick == "function" && i.onClick(), y();
        },
        onKeyDown: (c) => {
          p || (c.key === "Enter" || c.key === " " ? (c.preventDefault(), c.stopPropagation(), !x && typeof i.onClick == "function" && i.onClick(), x ? v && v(o, !0, !1) : y()) : A && A(c, o));
        },
        style: {
          padding: "7px 10px",
          cursor: p ? "not-allowed" : "pointer",
          display: "flex",
          alignItems: "center",
          gap: "8px",
          color: p ? (l == null ? void 0 : l.textSec) || "var(--sp-text-sec, #64748b)" : (l == null ? void 0 : l.text) || "var(--sp-text, #0f172a)",
          opacity: p ? 0.48 : 1,
          userSelect: "none"
        },
        children: [
          i.icon && /* @__PURE__ */ d.jsx("span", { style: { color: (l == null ? void 0 : l.textSec) || "var(--sp-text-sec, #64748b)", display: "flex", flexShrink: 0 }, children: i.icon }),
          /* @__PURE__ */ d.jsx("span", { style: { flex: 1 }, children: i.label }),
          x && /* @__PURE__ */ d.jsx("span", { "aria-hidden": "true", style: { color: (l == null ? void 0 : l.textSec) || "var(--sp-text-sec, #64748b)", display: "inline-flex", alignItems: "center", marginLeft: "4px" }, children: W && W.ChevronRight ? /* @__PURE__ */ d.jsx(W.ChevronRight, {}) : ">" })
        ]
      },
      o
    );
  }
) }), H = (n) => ({
  background: `var(--sp-glass-bg, ${(n == null ? void 0 : n.surfaceBg) || (n == null ? void 0 : n.background) || "#fff"})`,
  backdropFilter: "blur(18px) saturate(180%)",
  WebkitBackdropFilter: "blur(18px) saturate(180%)",
  border: `1px solid ${(n == null ? void 0 : n.border) || "var(--sp-border, #cbd5e1)"}`,
  boxShadow: (n == null ? void 0 : n.shadow) || (n == null ? void 0 : n.shadowMd) || "0 14px 36px rgba(0,0,0,0.18)",
  padding: "5px",
  borderRadius: "var(--sp-radius-lg, 10px)",
  fontSize: "var(--sp-fs-base, 13px)",
  minWidth: "190px",
  maxWidth: "300px",
  maxHeight: "calc(100vh - 16px)",
  overflowY: "auto",
  pointerEvents: "auto",
  color: (n == null ? void 0 : n.text) || "var(--sp-text, #0f172a)"
}), G = ({ x: n, y: l, onClose: y, actions: f, theme: v }) => {
  const [A, i] = r.useState({ x: n, y: l }), [o, p] = r.useState(null), [x, c] = r.useState(null), j = r.useRef(null), L = r.useRef(null), E = r.useRef([]), R = r.useRef(null), k = r.useCallback((e, s = 1) => {
    const t = f.length;
    for (let a = 0; a < t; a++) {
      const b = (e + a * s + t * 2) % t, u = f[b];
      if (u && u !== "separator" && !u.disabled && E.current[b]) {
        E.current[b].focus();
        return;
      }
    }
  }, [f]);
  r.useEffect(() => {
    k(0, 1);
  }, [k]), r.useEffect(() => () => clearTimeout(R.current), []);
  const z = r.useCallback(() => {
    clearTimeout(R.current), R.current = setTimeout(() => {
      p(null), c(null);
    }, $);
  }, []), S = r.useCallback(() => {
    clearTimeout(R.current);
  }, []), M = r.useCallback((e) => {
    clearTimeout(R.current), R.current = setTimeout(() => {
      p(e);
      const s = E.current[e];
      s && c(s.getBoundingClientRect());
    }, o !== null ? 0 : _);
  }, [o]), I = r.useCallback((e, s, t) => {
    t || (s ? M(e) : (S(), p(null), c(null)));
  }, [M, S]), N = r.useCallback((e, s) => {
    if (e.key === "Escape")
      e.preventDefault(), y();
    else if (e.key === "ArrowDown")
      e.preventDefault(), k(s + 1, 1);
    else if (e.key === "ArrowUp")
      e.preventDefault(), k(s - 1, -1);
    else if (e.key === "Home")
      e.preventDefault(), k(0, 1);
    else if (e.key === "End")
      e.preventDefault(), k(f.length - 1, -1);
    else if (e.key === "ArrowRight" && o === null && Number.isFinite(s) && s >= 0) {
      const t = f[s];
      t && t !== "separator" && Array.isArray(t.children) && (e.preventDefault(), M(s));
    }
  }, [y, k, f, o, M]);
  r.useLayoutEffect(() => {
    const e = () => {
      var B;
      const s = window.innerWidth, t = window.innerHeight, a = (B = j.current) == null ? void 0 : B.getBoundingClientRect(), b = (a == null ? void 0 : a.width) || 210, u = Math.min((a == null ? void 0 : a.height) || f.length * 32 + 20, t - 16), w = 8, C = Math.max(w, Math.min(n, s - b - w)), Y = Math.max(w, Math.min(l, t - u - w));
      i({ x: C, y: Y });
    };
    return e(), window.addEventListener("resize", e), window.addEventListener("scroll", e, !0), () => {
      window.removeEventListener("resize", e), window.removeEventListener("scroll", e, !0);
    };
  }, [n, l, f.length]);
  const [T, K] = r.useState({ left: 0, top: 0 });
  r.useLayoutEffect(() => {
    if (!x || o === null) return;
    const e = L.current;
    if (!e) return;
    const s = window.innerWidth, t = window.innerHeight, a = e.offsetWidth || 200, b = Math.min(e.offsetHeight || 200, t - 16), u = 8;
    let w = x.right + 2;
    w + a > s - u && (w = x.left - a - 2);
    let C = x.top - 6;
    C + b > t - u && (C = t - b - u), K({ left: Math.max(u, w), top: Math.max(u, C) });
  }, [x, o]);
  const h = o !== null ? f[o] : null, g = h && Array.isArray(h.children) ? h.children : null, D = r.useRef([]), P = r.useCallback((e, s = 1) => {
    if (!g) return;
    const t = g.length;
    for (let a = 0; a < t; a++) {
      const b = (e + a * s + t * 2) % t, u = g[b];
      if (u && u !== "separator" && !u.disabled && D.current[b]) {
        D.current[b].focus();
        return;
      }
    }
  }, [g]);
  r.useEffect(() => {
    g && (D.current = D.current.slice(0, g.length));
  }, [g]);
  const O = r.useCallback((e, s) => {
    e.key === "Escape" || e.key === "ArrowLeft" ? (e.preventDefault(), e.stopPropagation(), p(null), c(null), o !== null && E.current[o] && E.current[o].focus()) : e.key === "ArrowDown" ? (e.preventDefault(), P(s + 1, 1)) : e.key === "ArrowUp" && (e.preventDefault(), P(s - 1, -1));
  }, [P, o]);
  return m({
    open: !0,
    onClose: y,
    refs: [j, L],
    restoreFocus: !0
  }), /* @__PURE__ */ d.jsxs(U, { theme: v, zIndex: 10001, children: [
    /* @__PURE__ */ d.jsx(
      "div",
      {
        ref: j,
        className: "sp-menu-surface",
        role: "menu",
        style: { position: "fixed", top: A.y, left: A.x, zIndex: 10001, ...H(v) },
        onMouseLeave: z,
        onMouseEnter: S,
        children: /* @__PURE__ */ d.jsx(
          F,
          {
            actions: f,
            theme: v,
            onClose: y,
            itemRefs: E,
            onItemMouseEnter: I,
            onKeyDown: N
          }
        )
      }
    ),
    g && /* @__PURE__ */ d.jsx(
      "div",
      {
        ref: L,
        className: "sp-menu-surface",
        role: "menu",
        style: { position: "fixed", top: T.top, left: T.left, zIndex: 10002, ...H(v) },
        onMouseEnter: S,
        onMouseLeave: z,
        children: /* @__PURE__ */ d.jsx(
          F,
          {
            actions: g,
            theme: v,
            onClose: y,
            itemRefs: D,
            onKeyDown: O
          }
        )
      }
    )
  ] });
};
export {
  G as default
};
