import { W as K, j as d, X as _, I as W } from "./index-vxH3rV1_.js";
import { a as s } from "./charts-DthW1Q8W.js";
const U = 80, Y = 220, G = 16, I = ({ actions: t, theme: o, onClose: g, itemRefs: c, onItemMouseEnter: b, onKeyDown: m }) => {
  const S = t.some(
    /** @param {any} [action] @returns {boolean} */
    (n) => !!(n && n !== "separator" && n.icon)
  );
  return /* @__PURE__ */ d.jsx(d.Fragment, { children: t.map(
    /** @param {any} [action] @param {any} [i] @returns {any} */
    (n, x) => {
      if (n === "separator")
        return /* @__PURE__ */ d.jsx(
          "div",
          {
            role: "presentation",
            style: { height: "1px", background: (o == null ? void 0 : o.border) || "var(--sp-border, #e2e8f0)", margin: "4px 0" }
          },
          x
        );
      const u = !!n.disabled, v = Array.isArray(n.children) && n.children.length > 0;
      return /* @__PURE__ */ d.jsxs(
        "div",
        {
          ref: (f) => {
            c && (c.current[x] = f);
          },
          className: "sp-menu-item",
          role: "menuitem",
          "aria-haspopup": v ? "menu" : void 0,
          "aria-disabled": u || void 0,
          tabIndex: u ? -1 : 0,
          title: n.title || (typeof n.label == "string" ? n.label : void 0),
          onMouseEnter: () => {
            b && b(x, v, u);
          },
          onClick: (f) => {
            if (u || v) {
              f.stopPropagation();
              return;
            }
            typeof n.onClick == "function" && n.onClick(), g();
          },
          onKeyDown: (f) => {
            u || (f.key === "Enter" || f.key === " " ? (f.preventDefault(), f.stopPropagation(), !v && typeof n.onClick == "function" && n.onClick(), v ? b && b(x, !0, !1) : g()) : m && m(f, x));
          },
          style: {
            padding: "7px 10px",
            cursor: u ? "not-allowed" : "pointer",
            display: "flex",
            alignItems: "center",
            gap: "8px",
            color: u ? (o == null ? void 0 : o.textSec) || "var(--sp-text-sec, #64748b)" : (o == null ? void 0 : o.text) || "var(--sp-text, #0f172a)",
            opacity: u ? 0.48 : 1,
            userSelect: "none"
          },
          children: [
            (n.icon || S) && /* @__PURE__ */ d.jsx("span", { "aria-hidden": "true", style: { color: (o == null ? void 0 : o.textSec) || "var(--sp-text-sec, #64748b)", display: "inline-flex", alignItems: "center", justifyContent: "center", width: G, flexShrink: 0 }, children: n.icon || null }),
            /* @__PURE__ */ d.jsx("span", { style: { flex: 1, minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: n.label }),
            v && /* @__PURE__ */ d.jsx("span", { "aria-hidden": "true", style: { color: (o == null ? void 0 : o.textSec) || "var(--sp-text-sec, #64748b)", display: "inline-flex", alignItems: "center", marginLeft: "4px" }, children: W && W.ChevronRight ? /* @__PURE__ */ d.jsx(W.ChevronRight, {}) : ">" })
          ]
        },
        x
      );
    }
  ) });
}, T = (t) => ({
  background: `var(--sp-glass-bg, ${(t == null ? void 0 : t.surfaceBg) || (t == null ? void 0 : t.background) || "#fff"})`,
  backdropFilter: "blur(18px) saturate(180%)",
  WebkitBackdropFilter: "blur(18px) saturate(180%)",
  border: `1px solid ${(t == null ? void 0 : t.border) || "var(--sp-border, #cbd5e1)"}`,
  boxShadow: (t == null ? void 0 : t.shadow) || (t == null ? void 0 : t.shadowMd) || "0 14px 36px rgba(0,0,0,0.18)",
  padding: "5px",
  borderRadius: "var(--sp-radius-lg, 10px)",
  fontSize: "var(--sp-fs-base, 13px)",
  minWidth: "200px",
  // Labels stay on one line; the menu widens to fit them, up to the viewport.
  maxWidth: "min(380px, calc(100vw - 16px))",
  maxHeight: "calc(100vh - 16px)",
  overflowY: "auto",
  pointerEvents: "auto",
  color: (t == null ? void 0 : t.text) || "var(--sp-text, #0f172a)"
}), q = ({ x: t, y: o, onClose: g, actions: c, theme: b }) => {
  const [m, S] = s.useState({ x: t, y: o }), [n, x] = s.useState(null), [u, v] = s.useState(null), f = s.useRef(null), j = s.useRef(null), E = s.useRef([]), R = s.useRef(null), k = s.useCallback((e, r = 1) => {
    const l = c.length;
    for (let i = 0; i < l; i++) {
      const p = (e + i * r + l * 2) % l, a = c[p];
      if (a && a !== "separator" && !a.disabled && E.current[p]) {
        E.current[p].focus();
        return;
      }
    }
  }, [c]);
  s.useEffect(() => {
    k(0, 1);
  }, [k]), s.useEffect(() => () => clearTimeout(R.current), []);
  const P = s.useCallback(() => {
    clearTimeout(R.current), R.current = setTimeout(() => {
      x(null), v(null);
    }, Y);
  }, []), h = s.useCallback(() => {
    clearTimeout(R.current);
  }, []), C = s.useCallback((e) => {
    clearTimeout(R.current), R.current = setTimeout(() => {
      x(e);
      const r = E.current[e];
      r && v(r.getBoundingClientRect());
    }, n !== null ? 0 : U);
  }, [n]), z = s.useCallback((e, r, l) => {
    l || (r ? C(e) : (h(), x(null), v(null)));
  }, [C, h]), N = s.useCallback((e, r) => {
    if (e.key === "Escape")
      e.preventDefault(), g();
    else if (e.key === "ArrowDown")
      e.preventDefault(), k(r + 1, 1);
    else if (e.key === "ArrowUp")
      e.preventDefault(), k(r - 1, -1);
    else if (e.key === "Home")
      e.preventDefault(), k(0, 1);
    else if (e.key === "End")
      e.preventDefault(), k(c.length - 1, -1);
    else if (e.key === "ArrowRight" && n === null && Number.isFinite(r) && r >= 0) {
      const l = c[r];
      l && l !== "separator" && Array.isArray(l.children) && (e.preventDefault(), C(r));
    }
  }, [g, k, c, n, C]);
  s.useLayoutEffect(() => {
    const e = () => {
      const r = document.documentElement.clientWidth || window.innerWidth, l = document.documentElement.clientHeight || window.innerHeight, i = f.current, p = i && i.offsetWidth || 210, a = Math.min(i && i.offsetHeight || c.length * 32 + 20, l - 16), y = 8, D = Math.max(y, Math.min(t, r - p - y)), F = Math.max(y, Math.min(o, l - a - y));
      S({ x: D, y: F });
    };
    return e(), window.addEventListener("resize", e), window.addEventListener("scroll", e, !0), () => {
      window.removeEventListener("resize", e), window.removeEventListener("scroll", e, !0);
    };
  }, [t, o, c.length]);
  const [H, O] = s.useState({ left: 0, top: 0 });
  s.useLayoutEffect(() => {
    if (!u || n === null) return;
    const e = j.current;
    if (!e) return;
    const r = document.documentElement.clientWidth || window.innerWidth, l = document.documentElement.clientHeight || window.innerHeight, i = e.offsetWidth || 200, p = Math.min(e.offsetHeight || 200, l - 16), a = 8;
    let y = u.right + 2;
    y + i > r - a && (y = u.left - i - 2);
    let D = u.top - 6;
    D + p > l - a && (D = l - p - a), O({ left: Math.max(a, y), top: Math.max(a, D) });
  }, [u, n]);
  const M = n !== null ? c[n] : null, w = M && Array.isArray(M.children) ? M.children : null, A = s.useRef([]), L = s.useCallback((e, r = 1) => {
    if (!w) return;
    const l = w.length;
    for (let i = 0; i < l; i++) {
      const p = (e + i * r + l * 2) % l, a = w[p];
      if (a && a !== "separator" && !a.disabled && A.current[p]) {
        A.current[p].focus();
        return;
      }
    }
  }, [w]);
  s.useEffect(() => {
    w && (A.current = A.current.slice(0, w.length));
  }, [w]);
  const B = s.useCallback((e, r) => {
    e.key === "Escape" || e.key === "ArrowLeft" ? (e.preventDefault(), e.stopPropagation(), x(null), v(null), n !== null && E.current[n] && E.current[n].focus()) : e.key === "ArrowDown" ? (e.preventDefault(), L(r + 1, 1)) : e.key === "ArrowUp" && (e.preventDefault(), L(r - 1, -1));
  }, [L, n]);
  return K({
    open: !0,
    onClose: g,
    refs: [f, j],
    restoreFocus: !0
  }), /* @__PURE__ */ d.jsxs(_, { theme: b, zIndex: 10001, children: [
    /* @__PURE__ */ d.jsx(
      "div",
      {
        ref: f,
        className: "sp-menu-surface",
        role: "menu",
        style: { position: "fixed", top: m.y, left: m.x, zIndex: 10001, ...T(b) },
        onMouseLeave: P,
        onMouseEnter: h,
        children: /* @__PURE__ */ d.jsx(
          I,
          {
            actions: c,
            theme: b,
            onClose: g,
            itemRefs: E,
            onItemMouseEnter: z,
            onKeyDown: N
          }
        )
      }
    ),
    w && /* @__PURE__ */ d.jsx(
      "div",
      {
        ref: j,
        className: "sp-menu-surface",
        role: "menu",
        style: { position: "fixed", top: H.top, left: H.left, zIndex: 10002, ...T(b) },
        onMouseEnter: h,
        onMouseLeave: P,
        children: /* @__PURE__ */ d.jsx(
          I,
          {
            actions: w,
            theme: b,
            onClose: g,
            itemRefs: A,
            onKeyDown: B
          }
        )
      }
    )
  ] });
};
export {
  q as default
};
