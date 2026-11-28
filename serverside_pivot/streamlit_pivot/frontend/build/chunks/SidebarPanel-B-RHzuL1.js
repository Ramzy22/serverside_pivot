import { j as e, I as F, a1 as ke, a2 as Ur, a3 as Vr, a4 as Ct, a5 as it, a6 as Kr, a7 as Yr, a8 as Gt, a9 as Zr, aa as Gr, ab as He, ac as st, ad as dt, E as Xr, ae as qr, af as Jr, ag as Qr, ah as Je, ai as sr, aj as en, ak as Xt, T as ne, al as tn, am as rn, an as nn, ao as ln } from "./index-vxH3rV1_.js";
import { a as de, R as c } from "./charts-DthW1Q8W.js";
const on = ({ column: t, theme: n, styles: i, onFilter: o, currentFilter: l, options: x, optionMeta: f, onSearchOptions: j, onLoadMoreOptions: y, onOpen: E }) => {
  const [s, h] = de.useState(!1), W = l && (l.conditions || l.value), P = () => h(!1), M = () => {
    const $ = !s;
    h($), $ && typeof E == "function" && E(t.id);
  };
  return /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column" }, children: [
    /* @__PURE__ */ e.jsxs(
      "div",
      {
        style: {
          ...i.columnItem,
          cursor: "pointer",
          background: s ? n.select : "transparent",
          borderLeft: W ? `3px solid ${n.primary}` : "3px solid transparent"
        },
        onClick: M,
        children: [
          /* @__PURE__ */ e.jsx("span", { style: { marginRight: "8px", opacity: 0.7, display: "flex" }, children: s ? /* @__PURE__ */ e.jsx(F.ChevronDown, {}) : /* @__PURE__ */ e.jsx(F.ChevronRight, {}) }),
          /* @__PURE__ */ e.jsxs("span", { style: { flex: 1, fontWeight: 500, display: "flex", alignItems: "center", gap: "6px" }, children: [
            W && /* @__PURE__ */ e.jsx(F.Filter, { style: { fontSize: "12px", color: n.primary } }),
            ke(typeof t.header == "string" ? t.header : t.columnDef && typeof t.columnDef.header == "string" ? t.columnDef.header : t.id)
          ] })
        ]
      }
    ),
    s && /* @__PURE__ */ e.jsx("div", { style: { padding: "8px", borderBottom: `1px solid ${n.border}44` }, children: /* @__PURE__ */ e.jsx(
      Ur,
      {
        column: t,
        onFilter: o,
        currentFilter: l,
        options: x,
        optionMeta: f,
        onSearchOptions: j,
        onLoadMoreOptions: y,
        theme: n,
        onClose: P
      }
    ) })
  ] });
}, wt = ({ title: t, children: n, items: i, renderItem: o, theme: l, styles: x, initialExpanded: f = !0, count: j, onDrop: y, sectionId: E }) => {
  const [s, h] = de.useState(f), [W, P] = de.useState(f), [M, $] = de.useState(f), [b, C] = de.useState(!1), A = de.useRef(null);
  de.useEffect(() => {
    if (s) {
      P(!0);
      const w = setTimeout(() => $(!0), 220);
      return () => clearTimeout(w);
    }
    $(!1);
    const k = setTimeout(() => P(!1), 220);
    return () => clearTimeout(k);
  }, [s]);
  const I = Vr({
    count: i ? i.length : 0,
    getScrollElement: () => A.current,
    estimateSize: () => 32,
    overscan: 10,
    enabled: !!i && (s || W)
  }), N = (k) => {
    k.preventDefault(), C(!0);
  }, R = () => {
    C(!1);
  }, O = (k) => {
    k.preventDefault(), C(!1);
    const w = k.dataTransfer.getData("text/plain");
    y && w && y(w, E);
  }, a = (k) => {
    (k.key === "Enter" || k.key === " ") && (k.preventDefault(), h(
      /** @param {any} [prev] @returns {any} */
      (w) => !w
    ));
  };
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      "data-is-over": b ? "true" : void 0,
      style: {
        ...x.toolPanelSection,
        flex: s ? "0 1 auto" : "0 0 auto",
        minHeight: s ? "40px" : "32px",
        overflow: "hidden",
        // Prevent overlap
        borderRadius: b ? "8px" : void 0,
        background: b ? l.select || "rgba(37,99,235,0.08)" : void 0,
        boxShadow: b ? `inset 0 0 0 1.5px ${l.primary}` : void 0,
        transition: "background-color 140ms ease, box-shadow 140ms ease"
      },
      onDragOver: N,
      onDragLeave: R,
      onDrop: O,
      children: [
        /* @__PURE__ */ e.jsxs(
          "div",
          {
            role: "button",
            tabIndex: 0,
            "aria-expanded": s,
            className: "sp-btn sp-btn-ghost",
            style: { ...x.toolPanelSectionHeader, borderRadius: "6px" },
            onClick: () => h(!s),
            onKeyDown: a,
            children: [
              /* @__PURE__ */ e.jsx("span", { style: {
                marginRight: "8px",
                opacity: 0.72,
                display: "inline-flex",
                alignItems: "center",
                transform: s ? "rotate(90deg)" : "rotate(0deg)",
                transition: "transform 160ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))"
              }, children: /* @__PURE__ */ e.jsx(F.ChevronRight, {}) }),
              /* @__PURE__ */ e.jsx("span", { style: { flex: 1 }, children: t }),
              j !== void 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", opacity: 0.7, background: l.hover, padding: "2px 7px", borderRadius: "10px", fontVariantNumeric: "tabular-nums" }, children: j })
            ]
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-disclosure-grid",
            "data-expanded": s ? "true" : "false",
            style: { pointerEvents: s ? "auto" : "none" },
            children: /* @__PURE__ */ e.jsx(
              "div",
              {
                ref: A,
                className: "sp-disclosure-inner",
                style: {
                  ...x.toolPanelList,
                  maxHeight: "300px",
                  overflowY: s && M ? "auto" : "hidden"
                },
                children: i ? /* @__PURE__ */ e.jsx("div", { style: {
                  height: `${I.getTotalSize()}px`,
                  width: "100%",
                  position: "relative"
                }, children: I.getVirtualItems().map(
                  /** @param {any} [virtualRow] @returns {any} */
                  (k) => /* @__PURE__ */ e.jsx(
                    "div",
                    {
                      style: {
                        position: "absolute",
                        top: 0,
                        left: 0,
                        width: "100%",
                        height: `${k.size}px`,
                        transform: `translateY(${k.start}px)`
                      },
                      children: o(i[k.index], k.index)
                    },
                    k.index
                  )
                ) }) : n
              }
            )
          }
        )
      ]
    }
  );
}, dr = ({ column: t, level: n, theme: i, styles: o, handlePinColumn: l, colSearch: x, selectedCols: f, setSelectedCols: j, onDrop: y, sectionId: E }) => {
  const [s, h] = de.useState(n < 1), [W, P] = de.useState(!1), [M, $] = de.useState(!1), b = de.useRef(null), C = t.columns && t.columns.length > 0, A = t.columnDef.header;
  let I = typeof A == "string" ? A : t.id;
  typeof I == "string" && I.startsWith("group_") && (I = t.id.replace("group_", "").split("|||").pop() || I), t.headerVal && (I = t.headerVal), I = ke(I);
  const N = t.getIsPinned(), R = t.getIsVisible(), O = C ? Ct(t) : [t.id], a = O.length > 0 && O.every((D) => f.has(D)), k = !a && O.some((D) => f.has(D));
  if (de.useEffect(() => {
    b.current && (b.current.indeterminate = k);
  }, [k]), x && !I.toLowerCase().includes(x.toLowerCase()))
    if (C) {
      const D = (H) => {
        const Z = H.columnDef.header;
        return (typeof Z == "string" ? Z : H.id).toLowerCase().includes(x.toLowerCase()) ? !0 : H.columns ? H.columns.some(D) : !1;
      };
      if (!D(t)) return null;
    } else
      return null;
  const w = (D) => {
    D.stopPropagation();
    const H = new Set(f), Z = C ? Ct(t) : [t.id];
    Z.every(
      /** @param {any} [id] @returns {any} */
      (U) => H.has(U)
    ) ? Z.forEach(
      /** @param {any} [id] @returns {any} */
      (U) => H.delete(U)
    ) : Z.forEach(
      /** @param {any} [id] @returns {any} */
      (U) => H.add(U)
    ), j(H);
  }, B = (D) => {
    if (D.stopPropagation(), C) {
      const H = Kr(t), Z = H.some(
        /** @param {any} [c] @returns {any} */
        (Q) => !Q.getIsVisible()
      );
      H.forEach(
        /** @param {any} [c] @returns {any} */
        (Q) => Q.toggleVisibility(Z)
      );
    } else
      t.toggleVisibility();
  }, _ = (D, H) => {
    D.stopPropagation(), l(t.id, H);
  }, Y = (D) => {
    $(!0), D.dataTransfer.setData("text/plain", t.id), D.dataTransfer.effectAllowed = "move";
  }, L = () => {
    $(!1);
  }, g = (D) => {
    D.preventDefault(), D.stopPropagation();
    const H = D.dataTransfer.getData("text/plain");
    H && y && H !== t.id && y(H, E, t.id);
  }, z = (D) => {
    D.key === "Enter" ? B(D) : D.key === " " ? (D.preventDefault(), w(D)) : D.altKey && D.key === "ArrowLeft" ? (D.preventDefault(), l(t.id, "left")) : D.altKey && D.key === "ArrowRight" ? (D.preventDefault(), l(t.id, "right")) : D.altKey && D.key === "ArrowDown" && (D.preventDefault(), l(t.id, !1));
  }, V = (D) => ({
    width: "20px",
    height: "20px",
    padding: "2px",
    background: D ? i.primary : "transparent",
    border: "none",
    cursor: "pointer",
    borderRadius: "4px",
    color: D ? "#fff" : i.textSec,
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    opacity: D ? 1 : 0.72,
    transition: "background-color var(--sp-dur-fast, 140ms) ease, color var(--sp-dur-fast, 140ms) ease, opacity var(--sp-dur-fast, 140ms) ease"
  });
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      style: { display: "flex", flexDirection: "column", opacity: M ? 0.5 : 1 },
      draggable: !C,
      onDragStart: Y,
      onDragEnd: L,
      onDragOver: (D) => D.preventDefault(),
      onDrop: g,
      role: "treeitem",
      "aria-selected": a || k,
      "aria-expanded": s,
      children: [
        /* @__PURE__ */ e.jsxs(
          "div",
          {
            className: "sp-col-tree-row",
            style: {
              ...o.columnItem,
              gap: "5px",
              paddingRight: "6px",
              paddingLeft: `${n * 12 + 6}px`,
              minWidth: 0,
              background: a || k ? i.select : W ? i.hover : "transparent",
              borderLeft: N ? `3px solid ${i.primary}` : "3px solid transparent"
            },
            onMouseEnter: () => !(a || k) && P(!0),
            onMouseLeave: () => !(a || k) && P(!1),
            tabIndex: 0,
            onKeyDown: z,
            children: [
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  ref: b,
                  type: "checkbox",
                  checked: a,
                  onChange: w,
                  onClick: (D) => D.stopPropagation(),
                  style: { margin: 0, cursor: "pointer", pointerEvents: "auto", flexShrink: 0 },
                  tabIndex: -1
                }
              ),
              !C && /* @__PURE__ */ e.jsx("span", { style: { cursor: "grab", display: "inline-flex", opacity: 0.55, flexShrink: 0 }, children: /* @__PURE__ */ e.jsx(F.DragIndicator, {}) }),
              C && /* @__PURE__ */ e.jsx(
                "span",
                {
                  role: "button",
                  tabIndex: 0,
                  "aria-expanded": s,
                  "aria-label": s ? `Collapse ${I}` : `Expand ${I}`,
                  onClick: () => h(!s),
                  onKeyDown: (D) => {
                    (D.key === "Enter" || D.key === " ") && (D.preventDefault(), h(!s));
                  },
                  style: { cursor: "pointer", display: "inline-flex", opacity: 0.75, flexShrink: 0 },
                  children: s ? /* @__PURE__ */ e.jsx(F.ChevronDown, {}) : /* @__PURE__ */ e.jsx(F.ChevronRight, {})
                }
              ),
              /* @__PURE__ */ e.jsx(
                "span",
                {
                  onClick: B,
                  style: {
                    cursor: "pointer",
                    display: "inline-flex",
                    color: R ? i.primary : i.textSec,
                    opacity: R ? 1 : 0.5,
                    flexShrink: 0
                  },
                  title: R ? "Hide Column" : "Show Column",
                  "aria-label": R ? "Hide Column" : "Show Column",
                  children: R ? /* @__PURE__ */ e.jsx(F.Visibility, {}) : /* @__PURE__ */ e.jsx(F.VisibilityOff, {})
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "span",
                {
                  style: {
                    flex: "1 1 0%",
                    minWidth: 0,
                    overflow: "hidden",
                    fontWeight: C ? 600 : 400,
                    cursor: C ? "pointer" : "default",
                    color: R ? i.text : i.textSec,
                    opacity: R ? 1 : 0.6,
                    display: "flex",
                    alignItems: "center",
                    gap: "4px"
                  },
                  onClick: () => C && h(!s),
                  title: I,
                  children: [
                    C && /* @__PURE__ */ e.jsx(F.Group, { style: { fontSize: "13px", opacity: 0.8, flexShrink: 0 } }),
                    /* @__PURE__ */ e.jsx("span", { style: { minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: I })
                  ]
                }
              ),
              !C && /* @__PURE__ */ e.jsxs("div", { className: "pin-controls sp-col-pin-controls", "data-pinned": N || void 0, children: [
                /* @__PURE__ */ e.jsx("button", { type: "button", style: V(N === "left"), onClick: (D) => _(D, "left"), title: "Pin Column Left", "aria-label": "Pin Column Left", children: /* @__PURE__ */ e.jsx(F.PinLeft, {}) }),
                /* @__PURE__ */ e.jsx("button", { type: "button", style: V(!N), onClick: (D) => _(D, !1), title: "Unpin Column", "aria-label": "Unpin Column", children: /* @__PURE__ */ e.jsx(F.Unpin, {}) }),
                /* @__PURE__ */ e.jsx("button", { type: "button", style: V(N === "right"), onClick: (D) => _(D, "right"), title: "Pin Column Right", "aria-label": "Pin Column Right", children: /* @__PURE__ */ e.jsx(F.PinRight, {}) })
              ] })
            ]
          }
        ),
        C && s && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: t.columns.filter(
          /** @param {any} [child] @returns {any} */
          (D) => it(D, E)
        ).map(
          /** @param {any} [child] @returns {any} */
          (D) => /* @__PURE__ */ e.jsx(
            dr,
            {
              column: D,
              level: n + 1,
              theme: i,
              styles: o,
              handlePinColumn: l,
              colSearch: x,
              selectedCols: f,
              setSelectedCols: j,
              onDrop: y,
              sectionId: E
            },
            D.id
          )
        ) })
      ]
    }
  );
};
function an({
  panelId: t,
  panelLabel: n,
  size: i,
  onSizeChange: o,
  getSidebarWidth: l = null,
  onSidebarWidthChange: x = null,
  theme: f,
  outerStyle: j = {},
  contentStyle: y = {},
  children: E
}) {
  const s = c.useRef(null), h = c.useRef(null), W = c.useRef(null), P = c.useRef(null), M = c.useRef(null), $ = c.useRef(null), b = c.useRef(null), [C, A] = c.useState(null), [I, N] = c.useState(null), R = c.useMemo(() => Yr(t), [t]), O = c.useMemo(
    () => Gt(t, i),
    [t, i]
  ), a = c.useRef(O);
  a.current = O;
  const k = c.useMemo(() => C ? Gt(t, {
    width: C.width !== void 0 ? C.width : O.width,
    height: C.height !== void 0 ? C.height : O.height
  }) : O, [C, t, O]), w = c.useCallback((L = !0) => {
    if (h.current) {
      const g = h.current;
      h.current = null, g(L);
    }
  }, []);
  c.useEffect(() => (() => w(!1)), [w]);
  const B = c.useCallback((L, g) => {
    if (!s.current || typeof o != "function") return;
    L.preventDefault(), L.stopPropagation(), w(!1);
    const z = s.current.getBoundingClientRect(), V = s.current.parentElement ? s.current.parentElement.getBoundingClientRect() : z, D = typeof l == "function" ? Number(l()) : NaN, H = Number.isFinite(D) ? Math.max(200, Math.min(600, Math.round(D))) : null, Z = H !== null ? Math.max(0, Math.round(H - V.width)) : 0, Q = typeof x == "function" && H !== null;
    H !== null && (a.current.width === null || $.current === null) && ($.current = H, b.current = Z);
    const U = H !== null ? $.current !== null ? Math.min(H, $.current) : H : null, ee = b.current !== null ? b.current : Z, zt = Q ? Math.max(R.minWidth, Math.min(R.maxWidth, 600 - ee)) : Math.max(R.minWidth, Math.min(R.maxWidth, Math.floor(V.width))), le = {
      startX: L.clientX,
      startY: L.clientY,
      startWidth: z.width,
      startHeight: z.height,
      startParentWidth: Math.floor(V.width),
      startSidebarWidth: H,
      baseSidebarWidth: U,
      chromeWidth: ee,
      canExpandSidebar: Q,
      maxWidth: zt,
      initialWidthProp: a.current.width
    }, we = document.body.style.cursor, We = document.body.style.userSelect;
    document.body.style.cursor = g === "both" ? "nwse-resize" : g === "width" ? "ew-resize" : "ns-resize", document.body.style.userSelect = "none", P.current = null, M.current = null, N(g);
    const Ce = () => {
      W.current = null, P.current && A(P.current), M.current !== null && typeof x == "function" && (x(M.current), M.current = null);
    }, fe = (Ae) => {
      const ge = Ae.clientX - le.startX, me = Ae.clientY - le.startY, Ve = {};
      if (g === "width" || g === "both" && (le.initialWidthProp !== null || Math.abs(ge) >= 3)) {
        const ve = Math.max(R.minWidth, Math.min(le.maxWidth, Math.round(le.startWidth + ge)));
        if (Ve.width = ve, s.current && (s.current.style.width = `${ve}px`), le.canExpandSidebar) {
          const pt = le.baseSidebarWidth !== null ? le.baseSidebarWidth : le.startSidebarWidth, ut = Math.max(
            pt,
            Math.min(600, ve + le.chromeWidth)
          );
          M.current = ut;
        }
      }
      if (g === "height" || g === "both") {
        const ve = Math.max(R.minHeight, Math.min(R.maxHeight, Math.round(le.startHeight + me)));
        Ve.height = ve, s.current && (s.current.style.height = `${ve}px`);
      }
      Object.keys(Ve).length !== 0 && (P.current = Ve, typeof window < "u" && typeof window.requestAnimationFrame == "function" ? W.current === null && (W.current = window.requestAnimationFrame(Ce)) : Ce());
    }, Re = (Ae = !0) => {
      W.current !== null && typeof window < "u" && typeof window.cancelAnimationFrame == "function" && (window.cancelAnimationFrame(W.current), W.current = null), document.body.style.cursor = we, document.body.style.userSelect = We, window.removeEventListener("mousemove", fe), window.removeEventListener("mouseup", ce), window.removeEventListener("blur", ce);
      const ge = P.current, me = M.current;
      P.current = null, M.current = null, N(null), A(null), Ae ? (me !== null && typeof x == "function" && x(me), ge && o(t, ge)) : s.current && (s.current.style.width = a.current.width === null ? "100%" : `${a.current.width}px`, s.current.style.height = `${a.current.height}px`);
    }, ce = () => w(!0);
    h.current = Re, window.addEventListener("mousemove", fe), window.addEventListener("mouseup", ce), window.addEventListener("blur", ce);
  }, [
    l,
    R.maxHeight,
    R.maxWidth,
    R.minHeight,
    R.minWidth,
    x,
    o,
    t,
    w
  ]), _ = c.useCallback((L, g) => {
    g && (g.preventDefault(), g.stopPropagation()), w(!1), typeof o == "function" && (L === "width" ? (o(t, { width: null }), $.current !== null && typeof x == "function" && (x($.current), $.current = null, b.current = null)) : L === "height" ? o(t, { height: null }) : (o(t, { width: null, height: null }), $.current !== null && typeof x == "function" && (x($.current), $.current = null, b.current = null)));
  }, [x, o, t, w]), Y = `${f.primary}55`;
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      ref: s,
      style: {
        position: "relative",
        flexShrink: 0,
        width: k.width === null ? "100%" : `${k.width}px`,
        maxWidth: "100%",
        height: `${k.height}px`,
        minHeight: `${R.minHeight}px`,
        maxHeight: `${R.maxHeight}px`,
        boxSizing: "border-box",
        overflow: "hidden",
        ...j
      },
      children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            style: {
              width: "100%",
              height: "100%",
              overflow: "auto",
              boxSizing: "border-box",
              ...y
            },
            children: E
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": I === "width" ? "true" : void 0,
            onMouseDown: (L) => B(L, "width"),
            onDoubleClick: (L) => _("width", L),
            style: {
              position: "absolute",
              top: 0,
              right: 0,
              bottom: "12px",
              width: "10px",
              cursor: "ew-resize",
              zIndex: 4,
              touchAction: "none",
              display: "flex",
              alignItems: "center",
              justifyContent: "flex-end",
              paddingRight: "2px"
            },
            title: `Resize ${n} width`,
            children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "2px", height: "24px", borderRadius: "999px", background: Y, opacity: 0.45, pointerEvents: "none" } })
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": I === "height" ? "true" : void 0,
            onMouseDown: (L) => B(L, "height"),
            onDoubleClick: (L) => _("height", L),
            style: {
              position: "absolute",
              left: 0,
              right: "12px",
              bottom: 0,
              height: "10px",
              cursor: "ns-resize",
              zIndex: 4,
              touchAction: "none",
              display: "flex",
              alignItems: "flex-end",
              justifyContent: "center",
              paddingBottom: "2px"
            },
            title: `Resize ${n} height`,
            children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "28px", height: "2px", borderRadius: "999px", background: Y, opacity: 0.45, pointerEvents: "none" } })
          }
        ),
        /* @__PURE__ */ e.jsx(
          "div",
          {
            className: "sp-resize-handle",
            "data-resizing": I === "both" ? "true" : void 0,
            onMouseDown: (L) => B(L, "both"),
            onDoubleClick: (L) => _("both", L),
            style: {
              position: "absolute",
              right: 0,
              bottom: 0,
              width: "14px",
              height: "14px",
              cursor: "nwse-resize",
              zIndex: 5,
              touchAction: "none"
            },
            title: `Resize ${n}`,
            children: /* @__PURE__ */ e.jsx(
              "div",
              {
                className: "sp-resize-pill",
                "aria-hidden": "true",
                style: {
                  position: "absolute",
                  right: "4px",
                  bottom: "4px",
                  width: "3px",
                  height: "3px",
                  borderRadius: "999px",
                  background: Y,
                  opacity: 0.55,
                  boxShadow: `-4px 0 0 ${Y}, 0 -4px 0 ${Y}, -4px -4px 0 ${Y}`,
                  pointerEvents: "none"
                }
              }
            )
          }
        )
      ]
    }
  );
}
const cr = /\b[A-Za-z_][A-Za-z0-9_]*\b/g, pr = /^[A-Za-z_][A-Za-z0-9_]*$/, ct = /\[([^\]]+)\]/g, sn = /* @__PURE__ */ new Set(["abs", "round", "ceil", "floor", "max", "min", "int", "float"]), dn = /* @__PURE__ */ new Set(["True", "False", "None", "and", "or", "not", "if", "else", "in", "is"]), Me = {
  type: "line",
  metric: "last",
  showCurrentValue: !0,
  showDelta: !0,
  compact: !1,
  hideColumns: !1,
  placement: "before",
  source: "pivot",
  displayMode: "trend"
};
function ur(t) {
  return String(t || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function _e(t, n = "formula") {
  const o = String(t || "").trim().toLowerCase().replace(/\s+/g, "").replace(/[^a-z0-9_]/g, "") || String(n || "formula").toLowerCase().replace(/[^a-z0-9_]/g, "") || "formula";
  return /^[a-z_]/.test(o) ? o : `f_${o}`;
}
function J(t) {
  return !t || typeof t != "object" ? "" : String(
    t.formulaRef || _e(t.label || "", t.field || "formula") || t.field || ""
  ).trim();
}
function Ue(t) {
  const n = String(t || "").trim().toLowerCase();
  return ["columns", "display", "displayed", "displayed_columns", "rendered", "rendered_columns"].includes(n) ? "columns" : "measures";
}
function Fe(t) {
  return Ue(t) === "columns";
}
const cn = [
  { value: "eq", label: "Equals" },
  { value: "ne", label: "Does not equal" },
  { value: "contains", label: "Contains" },
  { value: "starts_with", label: "Starts with" },
  { value: "ends_with", label: "Ends with" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "in", label: "In list" },
  { value: "not_in", label: "Not in list" },
  { value: "is_null", label: "Is empty" },
  { value: "is_not_null", label: "Is not empty" }
], qt = /* @__PURE__ */ new Set(["in", "not_in"]), pn = /* @__PURE__ */ new Set(["is_null", "is_not_null"]);
function xr(t) {
  return { field: t && t[0] || "", operator: "eq", value: "", values: [], caseSensitive: !1 };
}
function fr(t, n = 0) {
  return {
    id: `rule-${Date.now().toString(36)}-${n}`,
    label: n === 0 ? "Group A" : `Group ${n + 1}`,
    condition: { op: "AND", clauses: [xr(t)] }
  };
}
function Jt(t, n = null) {
  if (n) {
    const o = He([n])[0] || n;
    return { ...JSON.parse(JSON.stringify(o)), __editingField: o.field, __editingId: o.id };
  }
  const i = st(`category_${Date.now().toString(36)}`);
  return {
    id: i,
    field: dt(i),
    name: "New Category",
    fallbackLabel: "Other",
    order: "creation",
    rules: [fr(t, 0)],
    __isNew: !0
  };
}
function un(t, n) {
  if (!n) return "string";
  const i = Array.isArray(t) ? t.slice(0, 250) : [];
  for (let o = 0; o < i.length; o += 1) {
    const l = i[o];
    if (!l || typeof l != "object") continue;
    const x = l[n];
    if (!(x == null || x === "")) {
      if (typeof x == "number" && Number.isFinite(x)) return "number";
      if (x instanceof Date && !Number.isNaN(x.getTime())) return "date";
      if (typeof x == "string") {
        const f = x.trim();
        if (!f) continue;
        if (Number.isFinite(Number(f))) return "number";
        const j = Date.parse(f);
        return !Number.isNaN(j) && /[-/T:]/.test(f) ? "date" : "string";
      }
    }
  }
  return "string";
}
const at = (t, n, i) => (Array.isArray(n) ? n : []).filter((o) => un(t, o) === i);
function xn(t, n) {
  const o = at(n, t, "date")[0] || t[0] || "", l = st(`${o || "date"}_month`);
  return {
    id: l,
    field: dt(l),
    kind: "date_group",
    name: o ? `${ke(o)} Month` : "Date Month",
    sourceField: o,
    granularity: "month",
    fiscalYearStartMonth: 1,
    fallbackLabel: "No date",
    __isNew: !0
  };
}
function fn(t, n) {
  const o = at(n, t, "number")[0] || t[0] || "", l = st(`${o || "number"}_bins`);
  return {
    id: l,
    field: dt(l),
    kind: "number_group",
    name: o ? `${ke(o)} Bins` : "Number Bins",
    sourceField: o,
    binSize: 10,
    minValue: "",
    maxValue: "",
    precision: 0,
    fallbackLabel: "No value",
    __isNew: !0
  };
}
const Qt = (t) => String(t || "").split(",").map((n) => n.trim()).filter(Boolean), er = (t) => String(t || "").trim().replace(/\s+/g, " ").toLowerCase(), gr = (t) => t && (t.__editingField || t.field) ? t.__editingField || t.field : "";
function gn(t, n) {
  const i = Array.isArray(t) ? t : [], o = gr(n);
  if (!o) return i;
  const l = i.findIndex(
    /** @param {any} [dimension] @returns {any} */
    (x) => x && x.field === o
  );
  return l >= 0 ? i.slice(0, l) : i;
}
function tr(t) {
  const n = [];
  return (t && Array.isArray(t.rules) ? t.rules : []).forEach(
    /** @param {any} [rule] @returns {any} */
    (i) => {
      (i && i.condition && Array.isArray(i.condition.clauses) ? i.condition.clauses : []).forEach(
        /** @param {any} [clause] @returns {any} */
        (l) => {
          l && l.field && n.push(l.field);
        }
      );
    }
  ), Array.from(new Set(n));
}
function yn(t, n) {
  const i = (Array.isArray(t) ? t : []).filter((o) => o && typeof o == "object").filter((o) => !o._isTotal && o._path !== "__grand_total__" && o._path !== "__color_scale_stats__");
  return Zr(i, n || []);
}
function hn(t, n) {
  return !Array.isArray(t) || t.length === 0 || !Array.isArray(n) || n.length === 0 ? !1 : n.every((i) => t.some((o) => o && Object.prototype.hasOwnProperty.call(o, i)));
}
const bn = (t, n) => (n && Array.isArray(n.rules) ? n.rules : []).map(
  /** @param {any} [rule] @returns {any} */
  (i) => ({
    label: i && i.label ? i.label : "Rule",
    count: t.reduce(
      /** @param {any} [matches] @param {any} [row] @returns {any} */
      (o, l) => Gr(l, i.condition) ? o + 1 : o,
      0
    )
  })
), mn = c.lazy(() => import(
  /* webpackChunkName: "pivot-report-editor" */
  "./ReportEditor-BBeI-b4y.js"
).then((t) => ({
  default: t.ReportEditor
}))), vn = (t) => /* @__PURE__ */ e.jsx(c.Suspense, { fallback: /* @__PURE__ */ e.jsx("div", { role: "status", style: { padding: "16px" }, children: "Loading report builder…" }), children: /* @__PURE__ */ e.jsx(mn, { ...t }) });
function rr({
  availableFields: t,
  customDimensions: n,
  setCustomDimensions: i,
  data: o,
  theme: l,
  showNotification: x,
  getFieldLabel: f,
  onDeleteField: j
}) {
  const y = c.useMemo(
    () => (t || []).filter(
      /** @param {any} [field] @returns {any} */
      (a) => a && !sr(a)
    ),
    [t]
  ), E = c.useMemo(
    () => He(n || []),
    [n]
  ), [s, h] = c.useState(null), W = c.useMemo(
    () => gn(E, s),
    [s, E]
  ), P = c.useMemo(() => {
    const a = /* @__PURE__ */ new Set(), k = [];
    return y.forEach(
      /** @param {any} [field] @returns {any} */
      (w) => {
        w && !a.has(w) && (a.add(w), k.push(w));
      }
    ), W.forEach(
      /** @param {any} [dimension] @returns {any} */
      (w) => {
        w && w.field && !a.has(w.field) && (a.add(w.field), k.push(w.field));
      }
    ), k;
  }, [W, y]), M = c.useMemo(
    () => {
      const a = at(o, y, "date");
      return a.length > 0 ? a : y;
    },
    [y, o]
  ), $ = c.useMemo(
    () => {
      const a = at(o, y, "number");
      return a.length > 0 ? a : y;
    },
    [y, o]
  ), b = c.useMemo(
    () => yn(o, W),
    [W, o]
  ), C = c.useMemo(() => ({
    border: `1px solid ${l.border}`,
    background: l.background,
    color: l.text,
    borderRadius: "8px",
    fontSize: "12px",
    padding: "7px 8px",
    outline: "none",
    minWidth: 0
  }), [l.background, l.border, l.text]), A = c.useCallback((a = "default") => ({
    border: a === "primary" ? `1px solid ${l.primary}` : `1px solid ${l.border}`,
    background: a === "primary" ? l.primary || "#2563EB" : l.hover || "#F8FAFC",
    color: a === "primary" ? "#fff" : l.text,
    borderRadius: "8px",
    fontSize: "11px",
    fontWeight: 700,
    padding: "6px 9px",
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: "5px"
  }), [l.border, l.hover, l.primary, l.text]);
  c.useEffect(() => {
    if (!s) return;
    const a = (k) => {
      k.key === "Escape" && (k.preventDefault(), h(null));
    };
    return window.addEventListener("keydown", a), () => window.removeEventListener("keydown", a);
  }, [s]);
  const I = c.useCallback((a, k) => {
    h(
      /** @param {any} [current] @returns {any} */
      (w) => {
        if (!w) return w;
        const B = [...w.rules || []];
        return B[a] = typeof k == "function" ? k(B[a]) : k, { ...w, rules: B };
      }
    );
  }, []), N = c.useCallback((a, k, w) => {
    I(
      a,
      /** @param {any} [rule] @returns {any} */
      (B) => {
        const _ = B.condition || { op: "AND", clauses: [] }, Y = [..._.clauses || []];
        return Y[k] = { ...Y[k], ...w }, { ...B, condition: { ..._, clauses: Y } };
      }
    );
  }, [I]), R = c.useCallback(() => {
    const a = He([s])[0];
    if (!a || !a.name) {
      x && x("A grouped field needs a name.", "warning");
      return;
    }
    if (a.kind === "category" && (!a.rules || a.rules.length === 0)) {
      x && x("A category needs at least one valid rule.", "warning");
      return;
    }
    if ((a.kind === "date_group" || a.kind === "number_group") && !a.sourceField) {
      x && x("Choose the source field to group.", "warning");
      return;
    }
    const k = gr(s), w = er(a.name);
    if (E.some(
      /** @param {any} [dimension] @returns {any} */
      (_) => _ && _.field !== k && er(_.name) === w
    )) {
      x && x(`A category named "${a.name}" already exists.`, "warning");
      return;
    }
    if (a.kind === "category") {
      const _ = new Set(P);
      if (tr(a).find(
        /** @param {any} [field] @returns {any} */
        (g) => !_.has(g)
      )) {
        x && x("A category can only use base fields or categories defined before it.", "warning");
        return;
      }
      const L = tr(a);
      if (hn(b, L)) {
        const g = bn(b, a).find(
          /** @param {any} [summary] @returns {any} */
          (z) => z.count === 0
        );
        if (g) {
          x && x(`"${g.label}" does not match any current row. Adjust the condition before applying.`, "warning");
          return;
        }
      }
    }
    if (a.kind === "date_group" && !M.includes(a.sourceField)) {
      x && x("Choose a date field for date grouping.", "warning");
      return;
    }
    if (a.kind === "number_group") {
      if (!$.includes(a.sourceField)) {
        x && x("Choose a numeric field for number grouping.", "warning");
        return;
      }
      if (!Number.isFinite(Number(a.binSize)) || Number(a.binSize) <= 0) {
        x && x("Number bins need a positive bin size.", "warning");
        return;
      }
    }
    i(
      /** @param {any} [previous] @returns {any} */
      (_) => [...He(_ || []).filter(
        /** @param {any} [dimension] @returns {any} */
        (g) => g.id !== a.id && g.field !== a.field && g.field !== k
      ), a]
    ), h(null), x && x(`Applied ${a.name}.`, "success");
  }, [P, M, s, E, $, i, x, b]), O = c.useCallback((a) => {
    const k = He([a])[0];
    k && (i(
      /** @param {any} [previous] @returns {any} */
      (w) => He(w || []).filter(
        /** @param {any} [item] @returns {any} */
        (B) => B.field !== k.field
      )
    ), typeof j == "function" && j(k.field), s && s.field === k.field && h(null), x && x(`Removed ${k.name}.`, "success"));
  }, [s, j, i, x]);
  return /* @__PURE__ */ e.jsxs("div", { style: {
    border: `1px solid ${l.border}`,
    background: l.headerBg || l.surfaceInset || l.background,
    borderRadius: "12px",
    padding: "10px",
    marginBottom: "14px",
    display: "flex",
    flexDirection: "column",
    gap: "10px"
  }, children: [
    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }, children: [
      /* @__PURE__ */ e.jsxs("div", { children: [
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 800 }, children: "Custom Categories" }),
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec, marginTop: "2px" }, children: "Named filter rules that become fields." })
      ] }),
      /* @__PURE__ */ e.jsx(
        "button",
        {
          type: "button",
          onClick: () => h(Jt(P)),
          style: { ...A("primary"), whiteSpace: "nowrap", flexShrink: 0 },
          children: "+ Category"
        }
      )
    ] }),
    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
      /* @__PURE__ */ e.jsxs(
        "button",
        {
          type: "button",
          onClick: () => h(xn(y, o)),
          disabled: M.length === 0,
          style: { ...A(), opacity: M.length === 0 ? 0.5 : 1, cursor: M.length === 0 ? "not-allowed" : "pointer" },
          title: M.length === 0 ? "No date fields detected" : "Create date buckets",
          children: [
            /* @__PURE__ */ e.jsx(F.Group, {}),
            " Date Group"
          ]
        }
      ),
      /* @__PURE__ */ e.jsxs(
        "button",
        {
          type: "button",
          onClick: () => h(fn(y, o)),
          disabled: $.length === 0,
          style: { ...A(), opacity: $.length === 0 ? 0.5 : 1, cursor: $.length === 0 ? "not-allowed" : "pointer" },
          title: $.length === 0 ? "No numeric fields detected" : "Create numeric bins",
          children: [
            /* @__PURE__ */ e.jsx(F.NumberFormat, {}),
            " Number Bins"
          ]
        }
      )
    ] }),
    E.length > 0 && /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: E.map(
      /** @param {any} [dimension] @returns {any} */
      (a) => {
        const k = a.kind || "category", w = k === "date_group" ? `${f(a.sourceField)} by ${a.granularity}` : k === "number_group" ? `${f(a.sourceField)} every ${a.binSize}` : `${a.rules.length} rule${a.rules.length === 1 ? "" : "s"} | fallback: ${a.fallbackLabel}`;
        return /* @__PURE__ */ e.jsxs(
          "div",
          {
            style: {
              border: `1px solid ${l.border}`,
              background: l.background,
              borderRadius: "10px",
              padding: "8px",
              display: "flex",
              alignItems: "center",
              gap: "8px"
            },
            children: [
              k === "number_group" ? /* @__PURE__ */ e.jsx(F.NumberFormat, {}) : /* @__PURE__ */ e.jsx(F.Branch, {}),
              /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0, flex: 1 }, children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "12px", fontWeight: 800, color: l.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: a.name }),
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: w })
              ] }),
              /* @__PURE__ */ e.jsxs("button", { type: "button", onClick: () => h(Jt(y, a)), style: A(), children: [
                /* @__PURE__ */ e.jsx(F.Edit, {}),
                " Edit"
              ] }),
              /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => O(a), style: { ...A(), color: "#B91C1C" }, children: /* @__PURE__ */ e.jsx(F.Delete, {}) })
            ]
          },
          a.field
        );
      }
    ) }),
    s && /* @__PURE__ */ e.jsx(
      "div",
      {
        role: "presentation",
        onMouseDown: (a) => {
          a.target === a.currentTarget && h(null);
        },
        style: {
          position: "fixed",
          inset: 0,
          zIndex: 1e4,
          background: "rgba(15, 23, 42, 0.38)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "24px"
        },
        children: /* @__PURE__ */ e.jsxs(
          "div",
          {
            role: "dialog",
            "aria-modal": "true",
            "aria-label": s.kind === "date_group" || s.kind === "number_group" ? s.__editingField ? "Edit grouped field" : "Add grouped field" : s.__editingField ? "Edit custom category" : "Add custom category",
            style: {
              width: "min(780px, calc(100vw - 32px))",
              maxHeight: "calc(100vh - 48px)",
              border: `1px solid ${l.primary || "#2563EB"}55`,
              background: l.background,
              color: l.text,
              borderRadius: "16px",
              boxShadow: "0 24px 80px rgba(15, 23, 42, 0.28)",
              overflow: "hidden"
            },
            children: [
              /* @__PURE__ */ e.jsxs("div", { style: {
                padding: "14px 16px",
                borderBottom: `1px solid ${l.border}`,
                background: l.headerBg || l.surfaceInset || l.background,
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: "12px"
              }, children: [
                /* @__PURE__ */ e.jsxs("div", { children: [
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "13px", fontWeight: 900, color: l.text }, children: s.kind === "date_group" || s.kind === "number_group" ? s.__editingField ? "Edit Grouped Field" : "Add Grouped Field" : s.__editingField ? "Edit Custom Category" : "Add Custom Category" }),
                  /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec, marginTop: "2px" }, children: s.kind === "date_group" ? "Create calendar buckets from a date field." : s.kind === "number_group" ? "Create fixed-width numeric bins from a number field." : "Rules can use base fields and categories defined before this one." })
                ] }),
                /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => h(null), style: { ...A(), padding: "6px 8px" }, "aria-label": "Close category editor", children: /* @__PURE__ */ e.jsx(F.Close, {}) })
              ] }),
              /* @__PURE__ */ e.jsxs("div", { style: {
                padding: "14px",
                overflowY: "auto",
                maxHeight: "calc(100vh - 128px)",
                display: "flex",
                flexDirection: "column",
                gap: "10px"
              }, children: [
                /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 120px 150px", gap: "8px" }, children: [
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Category Name",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        value: s.name || "",
                        onChange: (a) => {
                          const k = a.target.value, w = s.id || st(k);
                          h({ ...s, name: k, id: w, field: s.field || dt(w) });
                        },
                        style: C,
                        placeholder: "Example: Priority Segment"
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Else",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        value: s.fallbackLabel || "",
                        onChange: (a) => h({ ...s, fallbackLabel: a.target.value }),
                        style: C,
                        placeholder: "Other"
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Member Order",
                    /* @__PURE__ */ e.jsxs(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Member Order",
                        value: s.order || "creation",
                        onChange: (a) => h({ ...s, order: a.target.value }),
                        style: { ...C, width: "100%" },
                        children: [
                          /* @__PURE__ */ e.jsx("option", { value: "creation", children: "Creation Order" }),
                          /* @__PURE__ */ e.jsx("option", { value: "alpha", children: "Alphabetical (A→Z)" }),
                          /* @__PURE__ */ e.jsx("option", { value: "alpha_desc", children: "Alphabetical (Z→A)" }),
                          /* @__PURE__ */ e.jsx("option", { value: "numeric", children: "Numeric (0→9)" }),
                          /* @__PURE__ */ e.jsx("option", { value: "numeric_desc", children: "Numeric (9→0)" })
                        ]
                      }
                    )
                  ] })
                ] }),
                s.kind === "date_group" && /* @__PURE__ */ e.jsxs("div", { style: {
                  border: `1px solid ${l.border}`,
                  borderRadius: "10px",
                  padding: "10px",
                  background: l.hover || "#F8FAFC",
                  display: "grid",
                  gridTemplateColumns: "1.4fr 1fr 1fr",
                  gap: "8px"
                }, children: [
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Date Field",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Date Field",
                        value: s.sourceField || "",
                        onChange: (a) => h({
                          ...s,
                          sourceField: a.target.value,
                          name: s.__isNew ? `${f(a.target.value)} ${ke(s.granularity || "month")}` : s.name
                        }),
                        style: { ...C, width: "100%" },
                        children: M.map(
                          /** @param {any} [field] @returns {any} */
                          (a) => /* @__PURE__ */ e.jsx("option", { value: a, children: f(a) }, a)
                        )
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Date Part",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Date Part",
                        value: s.granularity || "month",
                        onChange: (a) => h({ ...s, granularity: a.target.value }),
                        style: { ...C, width: "100%" },
                        children: Array.from(rn).map((a) => /* @__PURE__ */ e.jsx("option", { value: a, children: ke(a) }, a))
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Fiscal Start",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Fiscal Start",
                        value: s.fiscalYearStartMonth || 1,
                        onChange: (a) => h({ ...s, fiscalYearStartMonth: Number(a.target.value) }),
                        style: { ...C, width: "100%" },
                        children: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"].map((a, k) => /* @__PURE__ */ e.jsx("option", { value: k + 1, children: a }, a))
                      }
                    )
                  ] })
                ] }),
                s.kind === "number_group" && /* @__PURE__ */ e.jsxs("div", { style: {
                  border: `1px solid ${l.border}`,
                  borderRadius: "10px",
                  padding: "10px",
                  background: l.hover || "#F8FAFC",
                  display: "grid",
                  gridTemplateColumns: "1.4fr 0.8fr 0.8fr 0.8fr 0.7fr",
                  gap: "8px"
                }, children: [
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Number Field",
                    /* @__PURE__ */ e.jsx(
                      ne,
                      {
                        size: "sm",
                        "aria-label": "Number Field",
                        value: s.sourceField || "",
                        onChange: (a) => h({
                          ...s,
                          sourceField: a.target.value,
                          name: s.__isNew ? `${f(a.target.value)} Bins` : s.name
                        }),
                        style: { ...C, width: "100%" },
                        children: $.map(
                          /** @param {any} [field] @returns {any} */
                          (a) => /* @__PURE__ */ e.jsx("option", { value: a, children: f(a) }, a)
                        )
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Bin Size",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: s.binSize === void 0 ? "" : s.binSize,
                        onChange: (a) => h({ ...s, binSize: a.target.value }),
                        style: C
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Min",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: s.minValue === null || s.minValue === void 0 ? "" : s.minValue,
                        onChange: (a) => h({ ...s, minValue: a.target.value }),
                        style: C
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Max",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        value: s.maxValue === null || s.maxValue === void 0 ? "" : s.maxValue,
                        onChange: (a) => h({ ...s, maxValue: a.target.value }),
                        style: C
                      }
                    )
                  ] }),
                  /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", fontSize: "10px", fontWeight: 800, color: l.textSec, textTransform: "uppercase", letterSpacing: "0.4px" }, children: [
                    "Decimals",
                    /* @__PURE__ */ e.jsx(
                      "input",
                      {
                        type: "number",
                        min: "0",
                        max: "8",
                        value: s.precision === null || s.precision === void 0 ? "" : s.precision,
                        onChange: (a) => h({ ...s, precision: a.target.value }),
                        style: C
                      }
                    )
                  ] })
                ] }),
                (!s.kind || s.kind === "category") && (s.rules || []).map(
                  /** @param {any} [rule] @param {any} [ruleIndex] @returns {any} */
                  (a, k) => {
                    const w = a.condition || { op: "AND", clauses: [] }, B = w.clauses || [];
                    return /* @__PURE__ */ e.jsxs("div", { style: {
                      border: `1px solid ${l.border}`,
                      borderRadius: "10px",
                      padding: "8px",
                      background: l.hover || "#F8FAFC",
                      display: "flex",
                      flexDirection: "column",
                      gap: "8px"
                    }, children: [
                      /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr 78px 28px", gap: "8px", alignItems: "center" }, children: [
                        /* @__PURE__ */ e.jsx(
                          "input",
                          {
                            value: a.label || "",
                            onChange: (_) => I(k, { ...a, label: _.target.value }),
                            style: C,
                            placeholder: "Category value"
                          }
                        ),
                        /* @__PURE__ */ e.jsxs(
                          ne,
                          {
                            size: "sm",
                            "aria-label": "Rule condition combiner",
                            value: w.op || "AND",
                            onChange: (_) => I(k, { ...a, condition: { ...w, op: _.target.value } }),
                            style: C,
                            children: [
                              /* @__PURE__ */ e.jsx("option", { value: "AND", children: "AND" }),
                              /* @__PURE__ */ e.jsx("option", { value: "OR", children: "OR" })
                            ]
                          }
                        ),
                        /* @__PURE__ */ e.jsx(
                          "button",
                          {
                            type: "button",
                            onClick: () => h({ ...s, rules: (s.rules || []).filter(
                              /** @param {any} [_] @param {any} [index] @returns {any} */
                              (_, Y) => Y !== k
                            ) }),
                            style: { ...A(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                            title: "Remove rule",
                            children: /* @__PURE__ */ e.jsx(F.Close, {})
                          }
                        )
                      ] }),
                      B.map(
                        /** @param {any} [clause] @param {any} [clauseIndex] @returns {any} */
                        (_, Y) => {
                          const L = pn.has(_.operator), g = qt.has(_.operator);
                          return /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: L ? "1fr 130px 28px" : "1fr 130px 1fr 28px", gap: "6px", alignItems: "center" }, children: [
                            /* @__PURE__ */ e.jsx(
                              ne,
                              {
                                size: "sm",
                                "aria-label": "Clause field",
                                value: _.field || "",
                                onChange: (z) => N(k, Y, { field: z.target.value }),
                                style: { ...C, width: "100%" },
                                children: P.map(
                                  /** @param {any} [field] @returns {any} */
                                  (z) => /* @__PURE__ */ e.jsx("option", { value: z, children: f(z) }, z)
                                )
                              }
                            ),
                            /* @__PURE__ */ e.jsx(
                              ne,
                              {
                                size: "sm",
                                "aria-label": "Clause operator",
                                value: _.operator || "eq",
                                onChange: (z) => {
                                  const V = z.target.value;
                                  N(k, Y, {
                                    operator: V,
                                    values: qt.has(V) ? _.values && _.values.length ? _.values : Qt(_.value) : _.values
                                  });
                                },
                                style: { ...C, width: "100%" },
                                children: cn.map((z) => /* @__PURE__ */ e.jsx("option", { value: z.value, children: z.label }, z.value))
                              }
                            ),
                            !L && /* @__PURE__ */ e.jsx(
                              "input",
                              {
                                value: g ? (_.values || []).join(", ") : _.value || "",
                                onChange: (z) => N(k, Y, g ? { values: Qt(z.target.value), value: z.target.value } : { value: z.target.value }),
                                style: C,
                                placeholder: g ? "A, B, C" : "Value"
                              }
                            ),
                            /* @__PURE__ */ e.jsx(
                              "button",
                              {
                                type: "button",
                                onClick: () => I(k, {
                                  ...a,
                                  condition: {
                                    ...w,
                                    clauses: B.filter(
                                      /** @param {any} [_] @param {any} [index] @returns {any} */
                                      (z, V) => V !== Y
                                    )
                                  }
                                }),
                                style: { ...A(), padding: "6px", justifyContent: "center", color: "#B91C1C" },
                                title: "Remove condition",
                                children: /* @__PURE__ */ e.jsx(F.Close, {})
                              }
                            )
                          ] }, `${a.id || k}:${Y}`);
                        }
                      ),
                      /* @__PURE__ */ e.jsx(
                        "button",
                        {
                          type: "button",
                          onClick: () => I(k, {
                            ...a,
                            condition: {
                              ...w,
                              clauses: [...B, xr(P)]
                            }
                          }),
                          style: { ...A(), alignSelf: "flex-start" },
                          children: "+ Condition"
                        }
                      )
                    ] }, a.id || k);
                  }
                ),
                (!s.kind || s.kind === "category") && /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => h({
                      ...s,
                      rules: [...s.rules || [], fr(P, (s.rules || []).length)]
                    }),
                    style: { ...A(), alignSelf: "flex-start" },
                    children: "+ Rule"
                  }
                ),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "flex-end", gap: "8px" }, children: [
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: () => h(null), style: A(), children: "Cancel" }),
                  /* @__PURE__ */ e.jsx("button", { type: "button", onClick: R, style: A("primary"), children: s.kind === "date_group" || s.kind === "number_group" ? "Apply Group" : "Apply Category" })
                ] })
              ] })
            ]
          }
        )
      }
    )
  ] });
}
function yr(t) {
  const n = [], i = /* @__PURE__ */ new Set();
  return String(t || "").replace(ct, (o, l) => {
    const x = String(l || "").trim();
    return x && !i.has(x) && (n.push(x), i.add(x)), "";
  }), n;
}
function nr(t, n = []) {
  const i = String(t || ""), o = [], l = /* @__PURE__ */ new Set(), x = (y) => {
    const E = String(y || "").trim();
    !E || l.has(E) || (o.push(E), l.add(E));
  };
  yr(i).forEach(x);
  let f = i.replace(ct, " ");
  return (Array.isArray(n) ? n : Array.from(n || [])).map((y) => String(y || "").trim()).filter((y) => y && !pr.test(y)).sort((y, E) => E.length - y.length).forEach(
    /** @param {any} [reference] @returns {any} */
    (y) => {
      const E = new RegExp(`(^|[^A-Za-z0-9_])${ur(y)}(?=$|[^A-Za-z0-9_])`, "g");
      f = f.replace(
        E,
        /** @param {any} [match] @param {any} [prefix] @returns {any} */
        (s, h) => (x(y), `${h} `)
      );
    }
  ), f.replace(
    cr,
    /** @param {any} [token] @param {any} [offset] @returns {any} */
    (y, E) => {
      const s = f.slice(E + y.length).trimStart();
      return dn.has(y) || sn.has(y) && s.startsWith("(") || x(y), y;
    }
  ), o;
}
function se(t) {
  const n = String(t || "").trim();
  return n ? `[${n}]` : "";
}
function Ee(t) {
  const n = String(t || "").trim();
  return n ? pr.test(n) ? n : `[${n}]` : "";
}
function Sn(t, n, i = "formula") {
  const o = _e(t, i), l = new Set(
    (Array.isArray(n) ? n : []).map((j) => j ? j.agg === "formula" ? J(j) : j.field : null).filter(Boolean).map((j) => String(j).toLowerCase())
  );
  if (!l.has(o)) return o;
  let x = 2, f = `${o}${x}`;
  for (; l.has(f); )
    x += 1, f = `${o}${x}`;
  return f;
}
function jn(t, n, i) {
  if (!t || !n || n === i) return t;
  const o = Ee(i), l = [];
  let f = String(t).replace(ct, (j, y) => String(y || "").trim() === n ? o : (l.push(j), `@@${l.length - 1}@@`)).replace(new RegExp(`\\b${ur(n)}\\b`, "g"), o);
  return l.forEach((j, y) => {
    f = f.replace(`@@${y}@@`, j);
  }), f;
}
function Rt(t, n, i) {
  t(
    /** @param {any} [prev] @returns {any} */
    (o) => {
      const l = [...o];
      return l[n] ? (l[n] = i(l[n]), l) : o;
    }
  );
}
function lr(t) {
  return !t || typeof t.closest != "function" ? !1 : !!t.closest('button, input, select, textarea, [contenteditable="true"]');
}
function kn(t) {
  if (!t || typeof t != "object") return "";
  const n = t.field === void 0 || t.field === null ? "" : String(t.field), i = t.agg === void 0 || t.agg === null ? "" : String(t.agg);
  return `${n}::${i}`;
}
function At(t) {
  const n = ln(t);
  return n ? {
    ...Me,
    ...n,
    header: n.header || "",
    color: n.color || "",
    positiveColor: n.positiveColor || "",
    negativeColor: n.negativeColor || ""
  } : null;
}
function wn(t) {
  const n = {
    ...Me,
    ...t || {}
  }, i = {
    type: n.type || Me.type,
    metric: n.metric || Me.metric,
    showCurrentValue: n.showCurrentValue !== !1,
    showDelta: n.showDelta !== !1,
    compact: !!n.compact,
    hideColumns: !!n.hideColumns,
    placement: "before",
    source: n.source === "field" ? "field" : "pivot",
    displayMode: n.displayMode === "value" ? "value" : "trend"
  };
  return ["header", "color", "positiveColor", "negativeColor"].forEach((o) => {
    const l = typeof n[o] == "string" ? n[o].trim() : "";
    l && (i[o] = l);
  }), Number.isFinite(Number(n.areaOpacity)) && (i.areaOpacity = Math.max(0.02, Math.min(0.45, Number(n.areaOpacity)))), i;
}
function hr(t, n = []) {
  const i = String(t && t.agg || "").trim().toLowerCase(), o = Array.isArray(n) && n.length > 0, l = i === "array_agg", x = i !== "array_agg" && o;
  return {
    canUseFieldSeries: l,
    canUsePivotSeries: x,
    canUseAny: l || x,
    defaultSource: l ? "field" : x ? "pivot" : null
  };
}
function $t(t, n) {
  return !n || !n.canUseAny ? !1 : t === "field" ? n.canUseFieldSeries : n.canUsePivotSeries;
}
function br(t, n) {
  const i = wn(t);
  return !n || !n.canUseAny ? null : $t(i.source, n) ? i : {
    ...i,
    source: n.defaultSource
  };
}
function Dt(t) {
  if (!Array.isArray(t) || t.length === 0) return "";
  const n = t.map((i) => i && Ee(i.field)).filter(Boolean);
  return n.length === 0 ? "" : n.length === 1 ? `${n[0]} * 100` : `${n[0]} - ${n[1]}`;
}
function or(t) {
  const n = (Array.isArray(t) ? t : []).map((i) => i && i.id).filter(Boolean);
  return n.length === 0 ? "" : n.length === 1 ? `${se(n[0])} * 100` : `${se(n[0])} - ${se(n[1])}`;
}
function Cn(t) {
  const n = new Set(
    (Array.isArray(t) ? t : []).map((o) => o && o.field).filter(Boolean)
  );
  let i = 1;
  for (; n.has(`formula_${i}`); ) i += 1;
  return `formula_${i}`;
}
function Rn(t, n, i = null, o = null, l = "measures", x = [], f = []) {
  const j = String(t || "").trim(), y = Fe(l);
  if (!j)
    return {
      tone: "warning",
      text: y ? "Enter a formula using bracketed rendered columns. Example: [Cash Equity_day_pnl_sum] - [ETF_day_pnl_sum]" : "Enter a formula using value fields. Example: sales - [day pnl]"
    };
  let E = 0;
  for (const R of j)
    if (R === "(" && (E += 1), R === ")" && (E -= 1), E < 0)
      return { tone: "warning", text: "Parentheses are unbalanced." };
  if (E !== 0)
    return { tone: "warning", text: "Parentheses are unbalanced." };
  if (y) {
    const R = Array.isArray(x) ? x : [], O = new Set(
      R.map(
        /** @param {any} [option] @returns {any} */
        (B) => B && B.id
      ).filter(Boolean)
    ), a = yr(j);
    if (a.length === 0)
      return {
        tone: "warning",
        text: "Insert at least one rendered column reference like [column_id]."
      };
    const k = j.replace(ct, ""), w = Array.from(new Set(k.match(cr) || []));
    if (w.length > 0)
      return {
        tone: "warning",
        text: `Rendered columns must use brackets: ${w.join(", ")}`
      };
    if (O.size > 0) {
      const B = a.filter(
        /** @param {any} [columnId] @returns {any} */
        (_) => !O.has(_)
      );
      if (B.length > 0)
        return {
          tone: "warning",
          text: `Unknown rendered columns: ${B.join(", ")}`
        };
    }
    return i && a.includes(i) ? {
      tone: "warning",
      text: "A displayed-column formula cannot reference its own output column."
    } : {
      tone: "success",
      text: `Using rendered columns: ${a.join(", ")}`
    };
  }
  const s = Array.isArray(n) ? n.filter((R) => R && R.field) : [];
  if (s.length === 0)
    return {
      tone: "warning",
      text: "Add at least one value or formula reference before creating formulas."
    };
  const h = s.filter(
    /** @param {any} [config] @returns {any} */
    (R) => R.agg === "formula"
  ), W = /* @__PURE__ */ new Map();
  h.forEach(
    /** @param {any} [config] @returns {any} */
    (R) => {
      const O = R.field === i ? _e(o || J(R), R.field) : J(R);
      O && (W.set(O, O), W.set(O.toLowerCase(), O));
    }
  );
  const P = s.filter(
    /** @param {any} [config] @returns {any} */
    (R) => R.agg !== "formula"
  ).map(
    /** @param {any} [config] @returns {any} */
    (R) => R.field
  ).filter(Boolean), M = new Set(P), $ = /* @__PURE__ */ new Set([...P, ...h.map(
    /** @param {any} [config] @returns {any} */
    (R) => R.field === i ? _e(o || J(R), R.field) : J(R)
  ).filter(Boolean), ...Array.isArray(f) ? f : []]), b = Array.from($), C = nr(j, b), A = C.map(
    /** @param {any} [token] @returns {any} */
    (R) => W.get(R) || W.get(R.toLowerCase()) || R
  ), I = A.filter(
    /** @param {any} [token] @returns {any} */
    (R) => !$.has(R) && !M.has(R)
  );
  if (I.length > 0)
    return {
      tone: "warning",
      text: `Unknown fields: ${I.join(", ")}`
    };
  const N = i ? _e(o || i, i) : null;
  if (i && N && s.some(
    /** @param {any} [config] @returns {any} */
    (O) => {
      if (!O || O.field === i) return !1;
      const a = O.agg === "formula" ? J(O) : O.field;
      return String(a || "").toLowerCase() === N.toLowerCase();
    }
  ))
    return {
      tone: "warning",
      text: `Reference key "${N}" is already in use.`
    };
  if (N && A.includes(N))
    return {
      tone: "warning",
      text: "A formula cannot reference itself."
    };
  if (i && N) {
    const R = {};
    h.forEach(
      /** @param {any} [config] @returns {any} */
      (w) => {
        const B = w.field === i ? N : J(w), _ = w.field === i ? j : String(w.formula || ""), Y = nr(_, b).map(
          /** @param {any} [token] @returns {any} */
          (L) => W.get(L) || W.get(L.toLowerCase()) || L
        ).filter(
          /** @param {any} [token] @returns {any} */
          (L) => L !== B && h.some(
            /** @param {any} [candidate] @returns {any} */
            (g) => J(g) === L || g.field === i && N === L
          )
        );
        R[B] = Y;
      }
    );
    const O = /* @__PURE__ */ new Set(), a = /* @__PURE__ */ new Set(), k = (w) => {
      if (O.has(w)) return !0;
      if (a.has(w)) return !1;
      O.add(w);
      const B = R[w] || [];
      for (const _ of B)
        if (k(_)) return !0;
      return O.delete(w), a.add(w), !1;
    };
    if (k(N))
      return {
        tone: "warning",
        text: "Circular formula reference detected."
      };
  }
  return C.length === 0 ? {
    tone: "warning",
    text: "Insert at least one measure or formula reference."
  } : {
    tone: "success",
    text: `Using ${A.join(", ")}`
  };
}
function An(t) {
  if (!Array.isArray(t) || t.length === 0) return [];
  const n = t.map((i) => i && Ee(i.field)).filter(Boolean);
  return n.length === 0 ? [] : n.length === 1 ? [{ label: "Scale x100", value: `${n[0]} * 100` }] : [
    { label: "Difference", value: `${n[0]} - ${n[1]}` },
    { label: "Ratio %", value: `(${n[0]} / ${n[1]}) * 100` },
    { label: "Margin %", value: `((${n[0]} - ${n[1]}) / ${n[0]}) * 100` }
  ];
}
function Dn(t) {
  const n = (Array.isArray(t) ? t : []).map((i) => i && i.id).filter(Boolean);
  return n.length === 0 ? [] : n.length === 1 ? [{ label: "Scale x100", value: `${se(n[0])} * 100` }] : [
    { label: "Difference", value: `${se(n[0])} - ${se(n[1])}` },
    { label: "Ratio %", value: `(${se(n[0])} / ${se(n[1])}) * 100` },
    { label: "Spread %", value: `((${se(n[0])} - ${se(n[1])}) / ${se(n[0])}) * 100` }
  ];
}
function $n(t, n) {
  const i = {
    muted: {
      text: t.textSec,
      border: t.border,
      background: t.headerSubtleBg || t.background
    },
    success: {
      text: t.primary,
      border: `${t.primary}33`,
      background: `${t.primary}10`
    },
    warning: {
      text: "#B45309",
      border: "#F59E0B55",
      background: "#F59E0B12"
    }
  };
  return i[n] || i.muted;
}
function zn({ item: t, idx: n, setValConfigs: i, theme: o, colFields: l = [], onClose: x }) {
  const f = c.useMemo(
    () => At(t && t.sparkline) || { ...Me },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [t && t.sparkline]
  ), j = c.useMemo(
    () => hr(t, l),
    [l, t]
  ), y = c.useMemo(() => {
    const b = [];
    return j.canUsePivotSeries && b.push({ value: "pivot", label: "Linked pivot" }), j.canUseFieldSeries && b.push({ value: "field", label: "Series field" }), b;
  }, [j]), E = $t(f.source || "pivot", j) ? f.source || "pivot" : j.defaultSource, s = c.useCallback((b) => {
    Rt(
      i,
      n,
      /** @param {any} [current] @returns {any} */
      (C) => {
        const A = At(C.sparkline) || { ...Me }, I = br({ ...A, ...b }, j);
        return I ? {
          ...C,
          sparkline: I
        } : C;
      }
    );
  }, [j, n, i]), h = c.useCallback((b) => b.stopPropagation(), []), W = [
    { value: "line", label: "Line" },
    { value: "area", label: "Area" },
    { value: "column", label: "Column" },
    { value: "bar", label: "Bar" }
  ], P = {
    background: o.background,
    border: `1px solid ${o.border}`,
    borderRadius: "5px",
    color: o.text,
    fontSize: "11px",
    padding: "4px 7px",
    outline: "none",
    width: "100%",
    boxSizing: "border-box"
  }, M = {
    fontSize: "10px",
    fontWeight: 700,
    letterSpacing: "0.06em",
    color: o.textSec,
    textTransform: "uppercase",
    marginBottom: "6px",
    display: "block"
  }, $ = {
    marginBottom: "14px"
  };
  return /* @__PURE__ */ e.jsx(
    "div",
    {
      style: { position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", zIndex: 9999, display: "flex", alignItems: "center", justifyContent: "center" },
      onMouseDown: h,
      onClick: x,
      children: /* @__PURE__ */ e.jsxs(
        "div",
        {
          onClick: h,
          style: {
            background: o.background,
            border: `1px solid ${o.border}`,
            borderRadius: "12px",
            boxShadow: "0 8px 32px rgba(0,0,0,0.22)",
            width: "320px",
            maxHeight: "80vh",
            overflow: "auto",
            padding: "18px 20px 16px",
            fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif"
          },
          children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "16px" }, children: [
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "13px", fontWeight: 700, color: o.text }, children: "Sparkline Column" }),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: x,
                  style: { background: "none", border: "none", cursor: "pointer", color: o.textSec, fontSize: "18px", lineHeight: 1, padding: "2px 4px" },
                  children: "×"
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Data source" }),
              y.length > 0 ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: y.map(
                  /** @param {any} p0 @returns {any} */
                  ({ value: b, label: C }) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => s({ source: b }),
                      style: {
                        flex: 1,
                        padding: "4px 0",
                        fontSize: "11px",
                        fontWeight: 600,
                        borderRadius: "6px",
                        cursor: "pointer",
                        border: `1px solid ${E === b ? o.primary : o.border}`,
                        background: E === b ? `${o.primary}18` : o.headerSubtleBg || o.background,
                        color: E === b ? o.primary : o.textSec
                      },
                      children: C
                    },
                    b
                  )
                ) }),
                /* @__PURE__ */ e.jsx("div", { style: { marginTop: "5px", fontSize: "10px", color: o.textSec, lineHeight: 1.4 }, children: E === "pivot" ? "Trend values are collected across the pivot columns. Source value cells stay numeric unless hidden." : "Each cell already contains the full data series for its row." })
              ] }) : /* @__PURE__ */ e.jsx(
                "div",
                {
                  "data-value-sparkline-unavailable": "true",
                  style: {
                    padding: "7px 8px",
                    borderRadius: "6px",
                    border: `1px solid ${o.warning || "#F59E0B"}55`,
                    background: `${o.warning || "#F59E0B"}12`,
                    color: o.text,
                    fontSize: "10px",
                    lineHeight: 1.45
                  },
                  children: "Add a column field for linked trends, or use the Series aggregation for field data."
                }
              )
            ] }),
            E === "field" && /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Cell display" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: [
                { value: "trend", label: "Trendline" },
                { value: "value", label: "Value" }
              ].map(({ value: b, label: C }) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => s({ displayMode: b }),
                  style: {
                    flex: 1,
                    padding: "4px 0",
                    fontSize: "11px",
                    fontWeight: 600,
                    borderRadius: "6px",
                    cursor: "pointer",
                    border: `1px solid ${(f.displayMode || "trend") === b ? o.primary : o.border}`,
                    background: (f.displayMode || "trend") === b ? `${o.primary}18` : o.headerSubtleBg || o.background,
                    color: (f.displayMode || "trend") === b ? o.primary : o.textSec
                  },
                  children: C
                },
                b
              )) }),
              /* @__PURE__ */ e.jsx("div", { style: { marginTop: "5px", fontSize: "10px", color: o.textSec, lineHeight: 1.4 }, children: (f.displayMode || "trend") === "value" ? "Show the selected summary metric from the series." : "Draw the series in the cell." })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Chart type" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px" }, children: W.map(({ value: b, label: C }) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => s({ type: b }),
                  style: {
                    flex: 1,
                    padding: "4px 0",
                    fontSize: "11px",
                    fontWeight: 600,
                    borderRadius: "6px",
                    cursor: "pointer",
                    border: `1px solid ${f.type === b ? o.primary : o.border}`,
                    background: f.type === b ? `${o.primary}18` : o.headerSubtleBg || o.background,
                    color: f.type === b ? o.primary : o.textSec
                  },
                  children: C
                },
                b
              )) })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Summary metric" }),
              /* @__PURE__ */ e.jsxs(
                ne,
                {
                  size: "sm",
                  "aria-label": "Summary metric",
                  value: f.metric,
                  onChange: (b) => s({ metric: b.target.value }),
                  style: { ...P, cursor: "pointer", width: "100%" },
                  children: [
                    /* @__PURE__ */ e.jsx("option", { value: "last", children: "Last value" }),
                    /* @__PURE__ */ e.jsx("option", { value: "first", children: "First value" }),
                    /* @__PURE__ */ e.jsx("option", { value: "min", children: "Minimum" }),
                    /* @__PURE__ */ e.jsx("option", { value: "max", children: "Maximum" }),
                    /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Average" }),
                    /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                    /* @__PURE__ */ e.jsx("option", { value: "delta", children: "Delta (last − first)" })
                  ]
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Display" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px" }, children: [
                { key: "showCurrentValue", label: "Value label" },
                { key: "showDelta", label: "Delta label" },
                { key: "compact", label: "Compact cell" },
                { key: "hideColumns", label: "Hide source values" }
              ].map(({ key: b, label: C }) => /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "5px", fontSize: "11px", color: o.text, cursor: "pointer", userSelect: "none" }, children: [
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "checkbox",
                    checked: !!f[b],
                    onChange: (A) => s({ [b]: A.target.checked }),
                    style: { accentColor: o.primary, width: "13px", height: "13px", cursor: "pointer" }
                  }
                ),
                C
              ] }, b)) }),
              f.hideColumns && /* @__PURE__ */ e.jsx("div", { style: { marginTop: "6px", fontSize: "10px", color: o.textSec, background: `${o.primary}10`, border: `1px solid ${o.primary}30`, borderRadius: "6px", padding: "5px 8px", lineHeight: 1.4 }, children: "Source value columns are hidden; the trend column remains." }),
              !f.hideColumns && E === "pivot" && /* @__PURE__ */ e.jsx("div", { style: { marginTop: "6px", fontSize: "10px", color: o.textSec, background: o.headerSubtleBg || o.background, border: `1px solid ${o.border}`, borderRadius: "6px", padding: "5px 8px", lineHeight: 1.4 }, children: "Source value columns stay numeric; the trend column is added beside them." })
            ] }),
            E === "pivot" && /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Position" }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: o.textSec, lineHeight: 1.4, background: o.headerSubtleBg || o.background, border: `1px solid ${o.border}`, borderRadius: "6px", padding: "6px 8px" }, children: "Trend columns are placed before the matching value columns." })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Colors" }),
              /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column", gap: "6px" }, children: [
                { key: "color", label: "Line / fill color", always: !0 },
                { key: "positiveColor", label: "Positive bar color", always: !1 },
                { key: "negativeColor", label: "Negative bar color", always: !1 }
              ].filter(({ always: b }) => b || f.type === "column" || f.type === "bar").map(({ key: b, label: C }) => /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: o.textSec, marginBottom: "3px", display: "block" }, children: C }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "5px", alignItems: "center" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "color",
                      value: f[b] || (b === "negativeColor" ? "#B91C1C" : b === "positiveColor" ? "#2F855A" : "#2563EB"),
                      onChange: (A) => s({ [b]: A.target.value }),
                      style: { width: "28px", height: "26px", padding: "1px 2px", border: `1px solid ${o.border}`, borderRadius: "4px", cursor: "pointer", background: "none" }
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "input",
                    {
                      type: "text",
                      value: f[b] || "",
                      onChange: (A) => s({ [b]: A.target.value }),
                      placeholder: "Auto",
                      style: { ...P, flex: 1 }
                    }
                  ),
                  f[b] && /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: () => s({ [b]: "" }),
                      style: { background: "none", border: "none", cursor: "pointer", color: o.textSec, fontSize: "14px", lineHeight: 1, padding: "2px" },
                      title: "Reset to auto",
                      children: "×"
                    }
                  )
                ] })
              ] }, b)) })
            ] }),
            f.type === "area" && /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Area fill opacity" }),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px" }, children: [
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    type: "range",
                    min: "0.02",
                    max: "0.45",
                    step: "0.01",
                    value: f.areaOpacity != null ? f.areaOpacity : 0.14,
                    onChange: (b) => s({ areaOpacity: Number(b.target.value) }),
                    style: { flex: 1, accentColor: o.primary }
                  }
                ),
                /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "11px", color: o.textSec, minWidth: "32px", textAlign: "right" }, children: [
                  Math.round((f.areaOpacity != null ? f.areaOpacity : 0.14) * 100),
                  "%"
                ] })
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: $, children: [
              /* @__PURE__ */ e.jsx("span", { style: M, children: "Column header label" }),
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  type: "text",
                  value: f.header || "",
                  onChange: (b) => s({ header: b.target.value }),
                  placeholder: "Auto (e.g. Sales Trend)",
                  style: P
                }
              )
            ] }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", justifyContent: "flex-end" }, children: /* @__PURE__ */ e.jsx(
              "button",
              {
                type: "button",
                onClick: x,
                style: {
                  background: o.primary,
                  color: "#fff",
                  border: "none",
                  borderRadius: "7px",
                  padding: "6px 18px",
                  fontSize: "12px",
                  fontWeight: 700,
                  cursor: "pointer"
                },
                children: "Done"
              }
            ) })
          ]
        }
      )
    }
  );
}
function mr({ item: t, idx: n, setValConfigs: i, theme: o, colFields: l = [], fixedSparklineValueKeySet: x = /* @__PURE__ */ new Set(), showNotification: f }) {
  const [j, y] = c.useState(!1), E = At(t && t.sparkline), s = kn(t), h = !!(s && x && x.has(s)), W = c.useMemo(
    () => hr(t, l),
    [l, t]
  ), P = !!E, M = !!(E && $t(E.source || "pivot", W)), $ = c.useCallback((I) => I.stopPropagation(), []), b = c.useCallback(() => {
    let I = !1;
    return Rt(
      i,
      n,
      /** @param {any} [current] @returns {any} */
      (N) => {
        const R = br(
          N.sparkline || Me,
          W
        );
        return R ? (I = !0, { ...N, sparkline: R }) : N;
      }
    ), I;
  }, [W, n, i]), C = c.useCallback((I) => {
    if (I.stopPropagation(), P) {
      if (h) {
        f && f("This trend is defined by the app. You can adjust its settings, but not remove it here.", "info");
        return;
      }
      Rt(
        i,
        n,
        /** @param {any} [current] @returns {any} */
        (N) => ({ ...N, sparkline: !1 })
      ), f && f("Removed trend column.", "info");
    } else W.canUseAny && b() ? (y(!0), f && f("Trend column enabled.", "success")) : f && f("Trend needs a column field, or a Series aggregation for field data.", "warning");
  }, [W.canUseAny, P, b, n, h, i, f]), A = c.useCallback((I) => {
    if (I.stopPropagation(), !W.canUseAny) {
      f && f("Trend settings are not available for this value selection.", "warning");
      return;
    }
    b(), y(!0);
  }, [W.canUseAny, b, f]);
  return P ? /* @__PURE__ */ e.jsxs(
    "div",
    {
      "data-value-sparkline-controls": "true",
      "data-value-sparkline-available": W.canUseAny ? "true" : "false",
      "data-value-sparkline-enabled": M ? "true" : "false",
      onMouseDown: $,
      onClick: $,
      style: { marginTop: "6px", paddingTop: "6px", borderTop: `1px solid ${o.border}`, width: "100%", display: "flex", alignItems: "center", gap: "6px" },
      children: [
        /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "data-value-sparkline-settings": "true",
            onClick: A,
            style: {
              border: `1px solid ${o.border}`,
              background: o.headerSubtleBg || o.background,
              color: o.textSec,
              borderRadius: "6px",
              cursor: "pointer",
              padding: "3px 8px",
              fontSize: "10px",
              lineHeight: "16px",
              fontWeight: 600,
              flexShrink: 0
            },
            title: "Configure sparkline",
            children: "Settings"
          }
        ),
        h && /* @__PURE__ */ e.jsx(
          "span",
          {
            "data-value-sparkline-fixed": "true",
            style: { fontSize: "10px", color: o.textSec, flexShrink: 0 },
            title: "This trend is defined by the app",
            children: "App trend"
          }
        ),
        !h && /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "data-value-sparkline-toggle": "true",
            onClick: C,
            style: {
              border: `1px solid ${o.border}`,
              background: o.background,
              color: o.textSec,
              borderRadius: "6px",
              cursor: "pointer",
              padding: "3px 8px",
              fontSize: "10px",
              lineHeight: "16px",
              fontWeight: 600,
              flexShrink: 0
            },
            title: "Remove trend",
            children: "Remove Trend"
          }
        ),
        M && !j && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: o.primary, flex: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: [
          (t.sparkline && t.sparkline.source) === "field" ? `series ${(t.sparkline && t.sparkline.displayMode) === "value" ? "value" : "trend"}` : "linked pivot",
          " | ",
          ["line", "area", "column", "bar"].includes(t.sparkline && t.sparkline.type) ? t.sparkline.type : "line",
          " | ",
          t.sparkline && t.sparkline.metric || "last"
        ] }),
        j && /* @__PURE__ */ e.jsx(
          zn,
          {
            item: t,
            idx: n,
            setValConfigs: i,
            theme: o,
            colFields: l,
            onClose: () => y(!1)
          }
        )
      ]
    }
  ) : null;
}
function ir({ item: t, idx: n, selected: i, onSelect: o, onRemove: l, setValConfigs: x, theme: f, styles: j, dropLine: y, zoneId: E, valueSelectStyle: s, colFields: h = [], fixedSparklineValueKeySet: W = /* @__PURE__ */ new Set(), showNotification: P, serverError: M = null }) {
  const $ = i ? `${f.primary}77` : M ? "#F59E0B88" : f.border, b = i ? f.headerBg || `${f.primary}0f` : f.background || "transparent", C = String(t.formula || "").trim() || "Empty formula";
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      onClick: () => o(t.field),
      style: {
        ...j.chip,
        border: `1px solid ${$}`,
        background: b,
        borderLeft: i ? `3px solid ${f.primary}` : "3px solid transparent",
        boxShadow: "none",
        cursor: "pointer",
        flexWrap: "wrap",
        alignItems: "flex-start"
      },
      title: "Select formula",
      children: [
        y && y.zone === E && y.idx === n && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...j.dropLine, top: -2 } }),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
          /* @__PURE__ */ e.jsx(F.DragIndicator, {}),
          /* @__PURE__ */ e.jsx("span", { style: {
            fontSize: "10px",
            fontWeight: 700,
            color: f.primary,
            background: `${f.primary}18`,
            borderRadius: "999px",
            padding: "3px 7px",
            letterSpacing: "0.5px",
            flexShrink: 0
          }, children: "fx" }),
          /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0, flex: 1 }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0 }, children: [
              /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 600, display: "block", minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: t.label || "Formula" }),
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: f.textSec, fontFamily: "monospace", flexShrink: 0 }, children: J(t) }),
              /* @__PURE__ */ e.jsx("span", { style: { fontSize: "9px", color: f.textSec, border: `1px solid ${f.border}`, borderRadius: "999px", padding: "1px 6px", flexShrink: 0 }, children: Fe(t.formulaScope) ? "columns" : "values" })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: {
              marginTop: "2px",
              fontSize: "10px",
              lineHeight: 1.35,
              color: f.textSec,
              fontFamily: "monospace",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis"
            }, children: [
              "=",
              C
            ] }),
            M && /* @__PURE__ */ e.jsxs("div", { style: { marginTop: "2px", fontSize: "9px", color: "#B45309" }, children: [
              "⚠ ",
              Fn[M] || M
            ] })
          ] })
        ] }),
        /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", marginLeft: "8px", alignItems: "center", flexShrink: 0 }, children: [
          i && /* @__PURE__ */ e.jsx("span", { style: {
            fontSize: "9px",
            color: f.primary,
            background: `${f.primary}14`,
            borderRadius: "999px",
            padding: "2px 6px",
            flexShrink: 0
          }, children: "Active" }),
          /* @__PURE__ */ e.jsx(
            "span",
            {
              onClick: (A) => {
                A.stopPropagation(), l(n);
              },
              style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" },
              children: /* @__PURE__ */ e.jsx(F.Close, {})
            }
          )
        ] }),
        /* @__PURE__ */ e.jsx(
          mr,
          {
            item: t,
            idx: n,
            setValConfigs: x,
            theme: f,
            valueSelectStyle: s,
            colFields: h,
            fixedSparklineValueKeySet: W,
            showNotification: P
          }
        ),
        y && y.zone === E && y.idx === n + 1 && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...j.dropLine, bottom: -2 } })
      ]
    }
  );
}
const Fn = {
  self_reference: "Formula references itself (server)",
  circular_reference: "Circular dependency with another formula (server)",
  expression_error: "Expression could not be evaluated (server)",
  missing_reference: "Unknown field reference (server)"
};
function En({ item: t, idx: n, valConfigs: i, setValConfigs: o, theme: l, onClose: x, onRemove: f, displayedColumnOptions: j = [], availableRawFields: y = [], serverError: E = null }) {
  const s = c.useRef(null), [h, W] = c.useState({
    label: t.label || "",
    formula: t.formula || "",
    formulaRef: J(t),
    formulaScope: Ue(t.formulaScope)
  }), P = c.useRef(t);
  P.current = t, c.useEffect(() => {
    const g = P.current;
    W({
      label: g.label || "",
      formula: g.formula || "",
      formulaRef: J(g),
      formulaScope: Ue(g.formulaScope)
    });
  }, [t.field]), c.useEffect(() => {
    if (!s.current) return;
    s.current.focus();
    const g = s.current.value.length;
    s.current.setSelectionRange(g, g);
  }, [t.field]);
  const M = c.useMemo(
    () => i.filter(
      /** @param {any} [config] @returns {any} */
      (g) => g && g.field
    ),
    [i]
  ), $ = c.useMemo(
    () => M.filter(
      /** @param {any} [config] @returns {any} */
      (g) => g.agg !== "formula"
    ),
    [M]
  ), b = c.useMemo(
    () => M.filter(
      /** @param {any} [config] @returns {any} */
      (g) => g.agg === "formula" && g.field !== t.field
    ),
    [t.field, M]
  ), C = c.useMemo(
    () => (Array.isArray(j) ? j : []).filter((g) => g && g.id && g.id !== t.field),
    [j, t.field]
  ), A = c.useMemo(
    () => {
      const g = new Set(M.map(
        /** @param {any} [c] @returns {any} */
        (z) => z.field
      ).filter(Boolean));
      return (Array.isArray(y) ? y : []).filter((z) => !g.has(z));
    },
    [y, M]
  ), I = c.useMemo(
    () => Rn(
      h.formula,
      M,
      t.field,
      h.formulaRef,
      h.formulaScope,
      C,
      A
    ),
    [C, h.formula, h.formulaRef, h.formulaScope, t.field, M, A]
  ), N = c.useMemo(
    () => $n(l, I.tone),
    [I.tone, l]
  ), R = c.useMemo(
    () => Fe(h.formulaScope) ? Dn(C) : An($),
    [C, h.formulaScope, $]
  ), O = c.useMemo(
    () => Fe(h.formulaScope) ? or(C) || "[column_id] - [other_column_id]" : Dt($) || `${Ee(h.formulaRef || t.field)} * 100`,
    [C, h.formulaRef, h.formulaScope, t.field, $]
  ), a = h.label !== (t.label || "") || h.formula !== (t.formula || "") || h.formulaRef !== J(t) || h.formulaScope !== Ue(t.formulaScope), k = c.useCallback(() => {
    const g = _e(h.formulaRef, t.field);
    o(
      /** @param {any} [prev] @returns {any} */
      (z) => {
        const V = [...z], D = V[n];
        if (!D) return z;
        const H = J(D);
        let Z = g;
        const Q = new Set(
          V.filter((U, ee) => U && ee !== n).map((U) => U.agg === "formula" ? J(U) : U.field).filter(Boolean).map((U) => String(U).toLowerCase())
        );
        if (Q.has(Z.toLowerCase())) {
          let U = 2, ee = `${Z}${U}`;
          for (; Q.has(ee.toLowerCase()); )
            U += 1, ee = `${Z}${U}`;
          Z = ee;
        }
        if (V[n] = {
          ...D,
          label: h.label,
          formula: h.formula,
          formulaRef: Z,
          formulaScope: h.formulaScope
        }, H && H !== Z)
          for (let U = 0; U < V.length; U += 1) {
            if (U === n) continue;
            const ee = V[U];
            !ee || ee.agg !== "formula" || !ee.formula || (V[U] = {
              ...ee,
              formula: jn(ee.formula, H, Z)
            });
          }
        return V;
      }
    );
  }, [h.formula, h.formulaRef, h.formulaScope, h.label, n, t.field, o]), w = c.useCallback(() => {
    W({
      label: t.label || "",
      formula: t.formula || "",
      formulaRef: J(t),
      formulaScope: Ue(t.formulaScope)
    }), x();
  }, [t.field, t.formula, t.formulaRef, t.formulaScope, t.label, x]), B = c.useCallback((g) => {
    const z = s.current;
    if (!z) return;
    const V = h.formula || "", D = z.selectionStart ?? V.length, H = z.selectionEnd ?? V.length, Z = `${V.slice(0, D)}${g}${V.slice(H)}`;
    W(
      /** @param {any} [prev] @returns {any} */
      (U) => ({ ...U, formula: Z })
    );
    const Q = D + g.length;
    setTimeout(() => {
      s.current && (s.current.focus(), s.current.setSelectionRange(Q, Q));
    }, 0);
  }, [h.formula]), _ = c.useCallback(() => {
    k(), x();
  }, [k, x]), Y = {
    border: `1px solid ${l.border}`,
    background: l.background,
    color: l.text,
    borderRadius: "999px",
    cursor: "pointer",
    padding: "3px 9px",
    fontSize: "10px",
    lineHeight: "16px"
  }, L = {
    border: `1px solid ${l.border}`,
    background: l.headerSubtleBg || l.background,
    color: l.text,
    borderRadius: "6px",
    cursor: "pointer",
    padding: "3px 8px",
    fontSize: "10px",
    lineHeight: "16px",
    fontFamily: "monospace"
  };
  return /* @__PURE__ */ e.jsx(
    "div",
    {
      onClick: w,
      style: {
        position: "fixed",
        inset: 0,
        zIndex: 10006,
        background: "rgba(2, 6, 23, 0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "24px"
      },
      children: /* @__PURE__ */ e.jsxs(
        "div",
        {
          onClick: (g) => g.stopPropagation(),
          onKeyDown: (g) => {
            g.key === "Escape" && w(), (g.ctrlKey || g.metaKey) && g.key === "Enter" && _();
          },
          style: {
            width: "min(760px, 92vw)",
            maxHeight: "80vh",
            overflowY: "auto",
            background: l.surfaceBg || l.background || "#fff",
            border: `1px solid ${l.border}`,
            borderRadius: l.radius || "16px",
            boxShadow: l.shadowMd || "0 18px 40px rgba(0,0,0,0.28)",
            padding: "18px"
          },
          children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px", marginBottom: "14px" }, children: [
              /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "15px", fontWeight: 800, color: l.text }, children: "Formula Editor" }),
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "11px", color: l.textSec }, children: "Use a stable reference key for formulas. Renaming the key updates dependent formulas." })
              ] }),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: w,
                  style: {
                    border: `1px solid ${l.border}`,
                    background: l.headerSubtleBg || l.background,
                    color: l.text,
                    borderRadius: "8px",
                    padding: "7px 10px",
                    fontSize: "11px",
                    fontWeight: 700,
                    cursor: "pointer"
                  },
                  children: "Close"
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "12px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", flexDirection: "column", gap: "4px", flex: 1, minWidth: "180px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px" }, children: "Label" }),
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    value: h.label,
                    onChange: (g) => W(
                      /** @param {any} [prev] @returns {any} */
                      (z) => ({ ...z, label: g.target.value })
                    ),
                    placeholder: "Formula label",
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.background,
                      color: l.text,
                      borderRadius: "8px",
                      padding: "8px 10px",
                      fontSize: "12px",
                      fontWeight: 600,
                      outline: "none"
                    }
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { minWidth: "180px", flex: "0 0 auto", display: "flex", flexDirection: "column", gap: "4px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px" }, children: "Reference key" }),
                /* @__PURE__ */ e.jsx(
                  "input",
                  {
                    value: h.formulaRef,
                    onChange: (g) => W(
                      /** @param {any} [prev] @returns {any} */
                      (z) => ({ ...z, formulaRef: _e(g.target.value, t.field) })
                    ),
                    placeholder: "formula1",
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.headerSubtleBg || l.background,
                      color: l.text,
                      borderRadius: "8px",
                      padding: "8px 10px",
                      fontSize: "12px",
                      fontFamily: "monospace",
                      outline: "none"
                    }
                  }
                )
              ] }),
              /* @__PURE__ */ e.jsxs("label", { style: { minWidth: "180px", flex: "0 0 auto", display: "flex", flexDirection: "column", gap: "4px" }, children: [
                /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px" }, children: "Formula type" }),
                /* @__PURE__ */ e.jsxs(
                  ne,
                  {
                    size: "sm",
                    "aria-label": "Formula type",
                    value: h.formulaScope,
                    onChange: (g) => {
                      const z = Ue(g.target.value);
                      W(
                        /** @param {any} [prev] @returns {any} */
                        (V) => ({
                          ...V,
                          formulaScope: z,
                          formula: V.formula || (z === "columns" ? or(C) : Dt($))
                        })
                      );
                    },
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.headerSubtleBg || l.background,
                      color: l.text,
                      borderRadius: "8px",
                      padding: "8px 10px",
                      fontSize: "12px",
                      outline: "none",
                      width: "100%"
                    },
                    children: [
                      /* @__PURE__ */ e.jsx("option", { value: "measures", children: "Value formula" }),
                      /* @__PURE__ */ e.jsx("option", { value: "columns", children: "Displayed columns" })
                    ]
                  }
                )
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "12px" }, children: [
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "4px" }, children: "Formula" }),
              /* @__PURE__ */ e.jsx(
                "textarea",
                {
                  ref: s,
                  value: h.formula,
                  onChange: (g) => W(
                    /** @param {any} [prev] @returns {any} */
                    (z) => ({ ...z, formula: g.target.value })
                  ),
                  placeholder: O,
                  rows: 4,
                  style: {
                    width: "100%",
                    boxSizing: "border-box",
                    resize: "vertical",
                    border: `1px solid ${l.border}`,
                    background: l.background,
                    color: l.text,
                    borderRadius: "10px",
                    fontFamily: "monospace",
                    fontSize: "12px",
                    lineHeight: 1.5,
                    padding: "10px 12px",
                    outline: "none"
                  }
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              /* @__PURE__ */ e.jsx("div", { style: {
                padding: "5px 9px",
                borderRadius: "999px",
                border: `1px solid ${N.border}`,
                background: N.background,
                color: N.text,
                fontSize: "10px",
                lineHeight: "14px"
              }, children: I.text }),
              /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec }, children: "Ctrl/Cmd + Enter applies" })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap", marginBottom: "12px" }, children: [
              R.map(
                /** @param {any} [template] @returns {any} */
                (g) => /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: () => W(
                      /** @param {any} [prev] @returns {any} */
                      (z) => ({ ...z, formula: g.value })
                    ),
                    style: Y,
                    children: g.label
                  },
                  g.label
                )
              ),
              ["+", "-", "*", "/", "(", ")", "* 100"].map((g) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onMouseDown: (z) => z.preventDefault(),
                  onClick: () => B(g),
                  style: L,
                  children: g
                },
                g
              ))
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "1fr", gap: "12px", marginBottom: "14px" }, children: [
              Fe(h.formulaScope) ? /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Rendered Columns" }),
                /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
                  C.map(
                    /** @param {any} [column] @returns {any} */
                    (g) => /* @__PURE__ */ e.jsx(
                      "button",
                      {
                        type: "button",
                        onMouseDown: (z) => z.preventDefault(),
                        onClick: () => B(se(g.id)),
                        style: Y,
                        title: `Insert ${g.id}`,
                        children: g.label || g.id
                      },
                      g.id
                    )
                  ),
                  C.length === 0 && /* @__PURE__ */ e.jsx("span", { style: { fontSize: "11px", color: l.textSec }, children: "Render the pivot once, or type a bracketed column id directly." })
                ] })
              ] }) : /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Measures" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: $.map(
                  /** @param {any} [config] @returns {any} */
                  (g) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(Ee(g.field)),
                      style: Y,
                      title: `Insert ${g.field}`,
                      children: g.field
                    },
                    g.field
                  )
                ) })
              ] }),
              !Fe(h.formulaScope) && b.length > 0 && /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Formula References" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: b.map(
                  /** @param {any} [config] @returns {any} */
                  (g) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(Ee(J(g))),
                      style: Y,
                      title: `${J(g)}${g.label ? ` (${g.label})` : ""}`,
                      children: J(g)
                    },
                    g.field
                  )
                ) })
              ] }),
              !Fe(h.formulaScope) && A.length > 0 && /* @__PURE__ */ e.jsxs("div", { children: [
                /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: l.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }, children: "Data Fields (auto-aggregated as sum)" }),
                /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: A.map(
                  /** @param {any} [f] @returns {any} */
                  (g) => /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onMouseDown: (z) => z.preventDefault(),
                      onClick: () => B(Ee(g)),
                      style: { ...Y, borderColor: `${l.primary}66`, color: l.primary, background: `${l.primary}0d` },
                      title: `Insert: ${g}`,
                      children: ke(g)
                    },
                    g
                  )
                ) })
              ] })
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "space-between", gap: "8px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  onClick: () => {
                    f(n), x();
                  },
                  style: {
                    border: `1px solid ${l.border}`,
                    background: l.background,
                    color: l.textSec,
                    borderRadius: "8px",
                    padding: "8px 12px",
                    fontSize: "11px",
                    fontWeight: 700,
                    cursor: "pointer"
                  },
                  children: "Delete Formula"
                }
              ),
              /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "8px", flexWrap: "wrap" }, children: [
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: w,
                    style: {
                      border: `1px solid ${l.border}`,
                      background: l.background,
                      color: l.textSec,
                      borderRadius: "8px",
                      padding: "8px 12px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: "pointer"
                    },
                    children: "Cancel"
                  }
                ),
                /* @__PURE__ */ e.jsx(
                  "button",
                  {
                    type: "button",
                    onClick: _,
                    disabled: !a,
                    style: {
                      border: `1px solid ${a ? `${l.primary}66` : l.border}`,
                      background: a ? `${l.primary}14` : l.background,
                      color: a ? l.primary : l.textSec,
                      borderRadius: "8px",
                      padding: "8px 12px",
                      fontSize: "11px",
                      fontWeight: 700,
                      cursor: a ? "pointer" : "default",
                      opacity: a ? 1 : 0.7
                    },
                    children: "Apply"
                  }
                )
              ] })
            ] })
          ]
        }
      )
    }
  );
}
function ar({ measureAxis: t, setMeasureAxis: n, theme: i, valueSelectStyle: o, regularValueCount: l }) {
  const x = c.useMemo(() => Je(t), [t]), f = c.useCallback((y) => {
    typeof n == "function" && n(
      /** @param {any} [previous] @returns {any} */
      (E) => ({
        ...Je(E),
        ...typeof y == "function" ? y(Je(E)) : y
      })
    );
  }, [n]), j = {
    ...o,
    width: "112px",
    padding: "3px 6px",
    fontSize: "10px"
  };
  return /* @__PURE__ */ e.jsxs("div", { style: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    flexWrap: "wrap",
    padding: "6px",
    border: `1px solid ${i.border}`,
    borderRadius: i.radiusSm || "6px",
    background: i.surfaceInset || i.headerSubtleBg || i.background
  }, children: [
    /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", color: i.textSec, fontWeight: 700, textTransform: "uppercase" }, children: "Values as" }),
    /* @__PURE__ */ e.jsxs(
      ne,
      {
        size: "sm",
        value: x.placement,
        onChange: (y) => f({ placement: y.target.value }),
        style: { ...o, width: "104px" },
        title: "Values placement",
        children: [
          /* @__PURE__ */ e.jsx("option", { value: "none", children: "Columns" }),
          /* @__PURE__ */ e.jsx("option", { value: "rows", children: "Rows" }),
          /* @__PURE__ */ e.jsx("option", { value: "columns", children: "Column Axis" })
        ]
      }
    ),
    x.placement !== "none" && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsx(
        "input",
        {
          value: x.labelField,
          onChange: (y) => f({ labelField: y.target.value }),
          style: j,
          title: "Measure label field"
        }
      ),
      /* @__PURE__ */ e.jsx(
        "input",
        {
          value: x.valueField,
          onChange: (y) => f({ valueField: y.target.value }),
          style: { ...j, width: "82px" },
          title: "Measure value field"
        }
      ),
      /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "4px", fontSize: "10px", color: i.textSec }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "checkbox",
            checked: x.suppressEmptyMembers,
            onChange: (y) => f({ suppressEmptyMembers: y.target.checked })
          }
        ),
        "Empty"
      ] }),
      /* @__PURE__ */ e.jsxs("label", { style: { display: "flex", alignItems: "center", gap: "4px", fontSize: "10px", color: i.textSec }, children: [
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "checkbox",
            checked: x.suppressZeroMembers,
            onChange: (y) => f({ suppressZeroMembers: y.target.checked })
          }
        ),
        "Zero"
      ] })
    ] }),
    /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: i.textSec }, children: [
      l,
      " selected"
    ] })
  ] });
}
const vr = "serverside-pivot:sidebar:modeling-open";
function Mn() {
  try {
    return typeof window < "u" && window.localStorage ? window.localStorage.getItem(vr) === "true" : !1;
  } catch {
    return !1;
  }
}
function _n(t) {
  try {
    typeof window < "u" && window.localStorage && window.localStorage.setItem(vr, t ? "true" : "false");
  } catch {
  }
}
const Bn = c.memo(
  /** @param {any} p0 @returns {any} */
  function({
    sidebarTab: n,
    setSidebarTab: i,
    rowFields: o,
    setRowFields: l,
    colFields: x,
    setColFields: f,
    valConfigs: j,
    setValConfigs: y,
    measureAxis: E = {},
    setMeasureAxis: s = null,
    columnVisibility: h,
    setColumnVisibility: W,
    columnPinning: P,
    setColumnPinning: M,
    availableFields: $,
    table: b,
    pinningPresets: C,
    showNotification: A,
    filterAnchorEl: I,
    setFilterAnchorEl: N,
    colSearch: R,
    setColSearch: O,
    colTypeFilter: a,
    setColTypeFilter: k,
    selectedCols: w,
    setSelectedCols: B,
    dropLine: _,
    onDragStart: Y,
    onDragOver: L,
    onDrop: g,
    onDragEnd: z,
    onKeyboardFieldDrop: V,
    handleHeaderFilter: D,
    handleFilterClick: H,
    requestFilterOptions: Z,
    handleExpandAllRows: Q,
    handlePinColumn: U,
    toggleAllColumnsPinned: ee,
    activeFilterCol: zt,
    closeFilterPopover: le,
    filterOptions: we,
    filterOptionMeta: We = {},
    clientFilterOptionMap: Ce = {},
    data: fe,
    sidebarWidth: Re,
    setSidebarWidth: ce,
    fieldPanelSizes: Ae,
    setFieldPanelSizes: ge,
    fixedSparklineValueKeys: me = [],
    sparklineFields: Ve = null,
    displayedColumnOptions: ve = [],
    reportEditorSelection: pt = null,
    showReportConfigColumn: ut = !0,
    setShowReportConfigColumn: Sr = null,
    formulaErrors: Ie = {},
    serverSide: Qe = !1
  }) {
    Xr("SidebarPanel");
    const { theme: d, styles: G } = qr(), {
      filters: pe,
      setFilters: xt,
      pivotMode: jr,
      reportDef: kr,
      setReportDef: wr,
      savedReports: Cr,
      setSavedReports: Rr,
      activeReportId: Ar,
      setActiveReportId: Dr,
      customDimensions: Se,
      setCustomDimensions: Ft
    } = Jr(), [et, Et] = c.useState({ columnId: null, anchorEl: null }), [Te, Be] = c.useState(null), [ye, De] = c.useState(null), [$e, ft] = c.useState("report"), tt = c.useRef(null), [$r, rt] = c.useState(null), ue = c.useRef(null), Mt = c.useRef(0), Ke = c.useRef(null), he = c.useRef(null), [q, gt] = c.useState(null), [yt, _t] = c.useState(""), [ht, zr] = c.useState(() => Mn()), Fr = c.useCallback(() => {
      zr(
        /** @param {boolean} previous */
        (r) => {
          const u = !r;
          return _n(u), u;
        }
      );
    }, []), bt = c.useMemo(
      () => new Set(Array.isArray(me) ? me : []),
      [me]
    ), te = c.useCallback(
      (r) => Qr(r, Se),
      [Se]
    ), Wt = c.useMemo(
      () => Je(E),
      [E]
    ), je = Wt.labelField || "Measure Name", Le = c.useMemo(
      () => ($ || []).filter(
        /** @param {any} [field] @returns {any} */
        (r) => r
      ),
      [$]
    ), re = c.useMemo(
      () => Le.filter(
        /** @param {any} [field] @returns {any} */
        (r) => r && !sr(r)
      ),
      [Le]
    ), Ye = Array.isArray(j) && j.length > 0 && je && !Le.includes(je), Pe = c.useMemo(
      () => Ye ? [je, ...Le] : Le,
      [Le, je, Ye]
    ), Ze = yt.trim().toLowerCase(), nt = c.useCallback((r) => {
      if (!Ze) return !0;
      const u = Ye && r === je ? "measure name measure names metric metrics value values" : "";
      return String(r || "").toLowerCase().includes(Ze) || String(te(r) || "").toLowerCase().includes(Ze) || u.includes(Ze);
    }, [te, je, Ze, Ye]), mt = c.useMemo(
      () => Pe.length > 8 ? Pe.filter(nt) : Pe,
      [Pe, nt]
    ), vt = c.useMemo(
      () => re.length > 8 ? re.filter(nt) : re,
      [re, nt]
    ), It = c.useCallback((r) => {
      l(
        /** @param {any} [prev] @returns {any} */
        (u) => (u || []).filter(
          /** @param {any} [item] @returns {any} */
          (p) => p !== r
        )
      ), f(
        /** @param {any} [prev] @returns {any} */
        (u) => (u || []).filter(
          /** @param {any} [item] @returns {any} */
          (p) => p !== r
        )
      ), y(
        /** @param {any} [prev] @returns {any} */
        (u) => (u || []).filter(
          /** @param {any} [item] @returns {any} */
          (p) => !(p && p.field === r)
        )
      ), xt(
        /** @param {any} [prev] @returns {any} */
        (u) => {
          if (!u || !Object.prototype.hasOwnProperty.call(u, r)) return u;
          const p = { ...u };
          return delete p[r], p;
        }
      );
    }, [f, xt, l, y]), Er = c.useCallback((r) => {
      const u = ue.current;
      if (!u) return;
      const p = 60, m = 12, v = u.getBoundingClientRect(), T = r.clientY, X = T - v.top, S = v.bottom - T;
      let K = 0;
      if (X < p && X >= 0 ? K = -1 : S < p && S >= 0 && (K = 1), K === 0) {
        he.current && (cancelAnimationFrame(he.current), he.current = null);
        return;
      }
      const oe = () => {
        ue.current && (ue.current.scrollTop += K * m, he.current = requestAnimationFrame(oe));
      };
      he.current || (he.current = requestAnimationFrame(oe));
    }, []), Tt = c.useCallback(() => {
      he.current && (cancelAnimationFrame(he.current), he.current = null);
    }, []), xe = c.useMemo(
      () => j.filter(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg === "formula"
      ),
      [j]
    ), ze = c.useMemo(
      () => xe.length,
      [xe]
    ), Oe = c.useMemo(
      () => j.filter(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg !== "formula"
      ).length,
      [j]
    ), Mr = c.useMemo(() => [
      { id: "rows", label: "Rows", icon: /* @__PURE__ */ e.jsx(F.List, {}) },
      { id: "cols", label: "Columns", icon: /* @__PURE__ */ e.jsx(F.Columns, {}) },
      { id: "vals", label: "Values", icon: /* @__PURE__ */ e.jsx(F.Sigma, {}) },
      { id: "filter", label: "Filters", icon: /* @__PURE__ */ e.jsx(F.Filter, {}) }
    ], []), Ge = c.useMemo(() => ({
      filter: Object.keys(pe || {}).filter((r) => r !== "global"),
      rows: o || [],
      cols: x || [],
      vals: j || []
    }), [x, pe, o, j]), lt = c.useCallback(() => {
      q && A && A("Keyboard move cancelled.", "info"), gt(null);
    }, [q, A]), Bt = c.useCallback((r, u, p, m) => {
      if (typeof V != "function") return;
      gt({ field: r, zone: u, idx: p, label: m }), A && A(`Picked up ${m}. Focus a target field or zone and press Enter.`, "info");
    }, [V, A]), ot = c.useCallback((r, u = null) => {
      if (!q || typeof V != "function") return !1;
      const p = Ge[r] || [], m = Number(u), v = Number.isFinite(m) ? Math.max(0, Math.min(Math.floor(m), p.length)) : p.length, T = V({
        field: q.field,
        srcZone: q.zone,
        srcIdx: q.idx,
        targetZone: r,
        targetIdx: v,
        source: "layout:keyboard-drop"
      });
      return T !== !1 && (A && A(`Moved ${q.label}.`, "success"), gt(null)), T !== !1;
    }, [q, V, A, Ge]), Lt = c.useCallback((r, u, p, m, v) => {
      if (!lr(r.target)) {
        if (r.key === "Escape") {
          r.preventDefault(), r.stopPropagation(), lt();
          return;
        }
        r.key !== "Enter" && r.key !== " " || (r.preventDefault(), r.stopPropagation(), q ? ot(p, m) : Bt(u, p, m, v));
      }
    }, [lt, ot, q, Bt]), _r = c.useCallback((r, u) => {
      if (!lr(r.target)) {
        if (r.key === "Escape") {
          r.preventDefault(), r.stopPropagation(), lt();
          return;
        }
        !q || r.key !== "Enter" && r.key !== " " || (r.preventDefault(), r.stopPropagation(), ot(u, (Ge[u] || []).length));
      }
    }, [lt, ot, q, Ge]), Wr = c.useCallback((r) => {
      r.preventDefault();
      const u = r.clientX, p = Re || 288;
      Ke.current = { startX: u, startWidth: p };
      const m = document.body.style.cursor, v = document.body.style.userSelect;
      document.body.style.cursor = "col-resize", document.body.style.userSelect = "none";
      const T = (S) => {
        if (!Ke.current) return;
        const K = S.clientX - Ke.current.startX, oe = Math.max(200, Math.min(600, Ke.current.startWidth + K));
        ce(oe);
      }, X = () => {
        Ke.current = null, document.body.style.cursor = m, document.body.style.userSelect = v, window.removeEventListener("mousemove", T), window.removeEventListener("mouseup", X), window.removeEventListener("blur", X);
      };
      window.addEventListener("mousemove", T), window.addEventListener("mouseup", X), window.addEventListener("blur", X);
    }, [Re, ce]), Ir = c.useCallback(() => ue.current ? ue.current.getBoundingClientRect().width : Re || 288, [Re]), Tr = c.useCallback((r) => {
      if (typeof ce != "function") return;
      const u = Math.max(200, Math.min(600, Math.round(r)));
      ce(
        /** @param {any} [prev] @returns {any} */
        (p) => p === u ? p : u
      );
    }, [ce]), Br = c.useCallback((r, u) => {
      typeof ge == "function" && ge(
        /** @param {any} [prev] @returns {any} */
        (p) => {
          const m = en(p, r, u), v = p && p[r] ? p[r] : null, T = m[r];
          return v && v.width === T.width && v.height === T.height ? p : m;
        }
      );
    }, [ge]), Pt = G.dropZone && G.dropZone.padding || "8px", Ot = c.useMemo(() => {
      const r = { ...G.dropZone || {} };
      return delete r.minHeight, delete r.maxHeight, delete r.overflowY, delete r.padding, delete r.border, delete r.boxShadow, r;
    }, [G.dropZone]), Nt = c.useCallback((r) => {
      if (we && we[r]) return we[r];
      if (Ce && Ce[r]) return Ce[r];
      if (!b || !b.getColumn) return [];
      if (!b.getColumn(r)) return [];
      const p = /* @__PURE__ */ new Set();
      return (b.getCoreRowModel ? b.getCoreRowModel().rows : []).forEach(
        /** @param {any} [row] @returns {any} */
        (v) => {
          const T = v.getValue(r);
          T != null && T !== "" && p.add(T);
        }
      ), Array.from(p).sort((v, T) => String(v).localeCompare(String(T), void 0, { numeric: !0, sensitivity: "base" }));
    }, [Ce, we, b]), be = c.useMemo(() => ({
      border: `1px solid ${d.border}`,
      background: d.headerSubtleBg || d.surfaceInset || d.background,
      color: d.text,
      cursor: "pointer",
      borderRadius: d.radiusSm || "8px",
      fontSize: "9px",
      lineHeight: 1.2,
      padding: "1px 4px",
      minHeight: "20px",
      outline: "none"
    }), [d.background, d.border, d.headerSubtleBg, d.radiusSm, d.surfaceInset, d.text]), Ht = c.useCallback((r, u) => {
      y(
        /** @param {any} [prev] @returns {any} */
        (p) => {
          const m = [...p], v = m[r];
          if (!v) return p;
          if (Xt(u) && !v.weightField) {
            const T = Array.isArray(re) && re.length > 0 ? re.find((K) => K !== v.field) || re[0] : "", X = typeof window < "u" ? window.prompt(
              `Weight field for weighted average of ${ke(v.field)}`,
              v.weightField || T || ""
            ) : v.weightField || T || "", S = typeof X == "string" ? X.trim() : "";
            return S ? Array.isArray(re) && re.length > 0 && !re.includes(S) ? (A && A(`Unknown weight field: ${S}`, "warning"), p) : (m[r] = { ...v, agg: u, weightField: S }, m) : (A && A("Weighted average requires a weight field.", "warning"), p);
          }
          return m[r] = { ...v, agg: u }, m;
        }
      );
    }, [re, y, A]), Lr = c.useCallback((r, u) => {
      if (!u) {
        A && A("Weighted average requires a weight field.", "warning");
        return;
      }
      y(
        /** @param {any} [prev] @returns {any} */
        (p) => {
          const m = [...p], v = m[r];
          return v ? (m[r] = { ...v, weightField: u }, m) : p;
        }
      );
    }, [y, A]), Pr = c.useCallback((r, u) => {
      y(
        /** @param {any} [prev] @returns {any} */
        (p) => {
          const m = [...p], v = m[r];
          return v ? (m[r] = {
            ...v,
            windowFn: u === "none" ? null : u
          }, m) : p;
        }
      );
    }, [y]), Ut = c.useCallback(() => {
      const r = Cn(j), u = j.filter(
        /** @param {any} [config] @returns {any} */
        (T) => T && T.agg !== "formula"
      ), p = `Formula ${ze + 1}`, m = Dt(u), v = Sn(p, j, r);
      Be(r), De(r), y(
        /** @param {any} [prev] @returns {any} */
        (T) => [
          ...T,
          {
            field: r,
            agg: "formula",
            label: p,
            formula: m,
            formulaRef: v,
            formulaScope: "measures"
          }
        ]
      ), A && (u.length === 0 ? A("Added a formula column. Add at least one regular value measure to reference in it.", "warning") : A(`Added ${p}.`, "success"));
    }, [ze, De, y, Be, A, j]), Or = (r, u) => {
      r.stopPropagation(), ue.current && (Mt.current = ue.current.scrollTop), Et(
        /** @param {any} [prev] @returns {any} */
        (p) => p.columnId === u ? { columnId: null, anchorEl: null } : { columnId: u, anchorEl: r.currentTarget }
      ), Z && Z(u);
    }, Nr = () => Et({ columnId: null, anchorEl: null });
    c.useEffect(() => {
      ue.current && et.columnId && (ue.current.scrollTop = Mt.current);
    }, [we, et.columnId]), c.useEffect(() => {
      if (xe.length === 0) {
        Te !== null && Be(null), ye !== null && De(null);
        return;
      }
      xe.some(
        /** @param {any} [config] @returns {any} */
        (u) => u.field === Te
      ) || Be(xe[xe.length - 1].field), ye && !xe.some(
        /** @param {any} [config] @returns {any} */
        (u) => u.field === ye
      ) && De(null);
    }, [Te, xe, ye]);
    const St = c.useMemo(
      () => j.findIndex(
        /** @param {any} [config] @returns {any} */
        (r) => r && r.agg === "formula" && r.field === ye
      ),
      [ye, j]
    ), jt = St >= 0 ? j[St] : null, Vt = c.useCallback((r) => {
      Be(r), De(r);
    }, []), Xe = c.useCallback((r) => {
      const u = j[r] && j[r].agg === "formula" ? j[r].field : null;
      if (y(
        /** @param {any} [prev] @returns {any} */
        (p) => p.filter(
          /** @param {any} [_] @param {any} [index] @returns {any} */
          (m, v) => v !== r
        )
      ), u && u === Te) {
        const p = xe.find(
          /** @param {any} [config] @returns {any} */
          (m) => m.field !== u
        );
        Be(p ? p.field : null);
      }
      u && u === ye && De(null);
    }, [Te, xe, ye, y, j]), Ne = jr === "report", Kt = Math.max(Re || 288, Ne ? 380 : 0), Yt = (r, u) => {
      if (r <= 8) return null;
      const p = yt.trim().length > 0;
      return /* @__PURE__ */ e.jsxs("div", { style: {
        display: "flex",
        alignItems: "center",
        gap: "6px",
        margin: "0 2px 8px",
        padding: "5px 8px",
        border: `1px solid ${d.border}`,
        borderRadius: d.radiusSm || "8px",
        background: d.headerSubtleBg || d.surfaceInset || d.background
      }, children: [
        /* @__PURE__ */ e.jsx(F.Search, { style: { flexShrink: 0, color: d.textSec } }),
        /* @__PURE__ */ e.jsx(
          "input",
          {
            type: "search",
            "aria-label": "Search available fields",
            placeholder: `Search ${r} fields...`,
            value: yt,
            onChange: (m) => _t(m.target.value),
            style: {
              flex: 1,
              minWidth: 0,
              border: "none",
              outline: "none",
              background: "transparent",
              color: d.text,
              fontSize: "12px"
            }
          }
        ),
        p && /* @__PURE__ */ e.jsx(
          "button",
          {
            type: "button",
            "aria-label": "Clear available field search",
            title: "Clear search",
            onClick: () => _t(""),
            style: {
              border: "none",
              background: "transparent",
              color: d.textSec,
              cursor: "pointer",
              padding: 0,
              display: "flex",
              alignItems: "center"
            },
            children: /* @__PURE__ */ e.jsx(F.Close, {})
          }
        ),
        p && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec, whiteSpace: "nowrap" }, children: [
          u,
          "/",
          r
        ] })
      ] });
    };
    return /* @__PURE__ */ e.jsxs("div", { style: { position: "relative", display: "flex", flexShrink: 0 }, children: [
      /* @__PURE__ */ e.jsxs("div", { ref: ue, style: { ...G.sidebar, width: `${Kt}px`, minWidth: `${Kt}px` }, role: "complementary", "aria-label": "Tool Panel", onDragOver: Er, onDragLeave: Tt, onDrop: Tt, children: [
        /* @__PURE__ */ e.jsx(
          "div",
          {
            id: "pivot-sidebar-keyboard-dnd-help",
            style: { position: "absolute", width: 1, height: 1, padding: 0, margin: -1, overflow: "hidden", clip: "rect(0 0 0 0)", whiteSpace: "nowrap", border: 0 },
            children: "Press Enter or Space on a field to pick it up. Focus a target field or zone and press Enter or Space to drop it. Press Escape to cancel."
          }
        ),
        Ne ? /* @__PURE__ */ e.jsxs("div", { role: "tablist", "aria-label": "Report Panel Tabs", style: { position: "relative", display: "grid", gridTemplateColumns: "repeat(3, 1fr)", borderBottom: `1px solid ${d.border}`, marginBottom: "16px" }, children: [
          [
            { id: "report", label: "Builder", icon: /* @__PURE__ */ e.jsx(F.Report, {}) },
            { id: "fields", label: "Fields", icon: /* @__PURE__ */ e.jsx(F.Sigma, {}) },
            { id: "columns", label: "Columns", icon: /* @__PURE__ */ e.jsx(F.Columns, {}) }
          ].map((r, u, p) => /* @__PURE__ */ e.jsxs(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": $e === r.id,
              className: "sp-tab",
              onClick: () => ft(r.id),
              onKeyDown: (m) => {
                if (m.key === "Enter" || m.key === " ")
                  m.preventDefault(), ft(r.id);
                else if (m.key === "ArrowRight" || m.key === "ArrowLeft") {
                  m.preventDefault();
                  const v = m.key === "ArrowRight" ? 1 : -1, T = p[(u + v + p.length) % p.length];
                  T && ft(T.id);
                }
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                justifyContent: "center",
                fontWeight: $e === r.id ? 600 : 400,
                color: $e === r.id ? d.primary : d.textSec,
                display: "flex",
                alignItems: "center",
                gap: "5px",
                fontSize: "13px"
              },
              children: [
                r.icon,
                r.label
              ]
            },
            r.id
          )),
          /* @__PURE__ */ e.jsx(
            "div",
            {
              "aria-hidden": "true",
              style: {
                position: "absolute",
                bottom: "-1px",
                left: 0,
                width: "33.3333%",
                height: "2px",
                borderRadius: "2px 2px 0 0",
                background: d.primary,
                transform: `translateX(${Math.max(0, ["report", "fields", "columns"].indexOf($e)) * 100}%)`,
                transition: "transform 200ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))",
                pointerEvents: "none"
              }
            }
          )
        ] }) : /* @__PURE__ */ e.jsxs("div", { role: "tablist", "aria-label": "Tool Panel Tabs", style: { position: "relative", display: "grid", gridTemplateColumns: "repeat(3, 1fr)", borderBottom: `1px solid ${d.border}`, marginBottom: "16px" }, children: [
          /* @__PURE__ */ e.jsx(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": n === "fields",
              className: "sp-tab",
              onClick: () => i("fields"),
              onKeyDown: (r) => {
                r.key === "Enter" || r.key === " " ? (r.preventDefault(), i("fields")) : r.key === "ArrowRight" ? (r.preventDefault(), i("filters")) : r.key === "ArrowLeft" && (r.preventDefault(), i("columns"));
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                textAlign: "center",
                fontWeight: n === "fields" ? 600 : 400,
                color: n === "fields" ? d.primary : d.textSec
              },
              children: "Fields"
            }
          ),
          /* @__PURE__ */ e.jsxs(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": n === "filters",
              className: "sp-tab",
              onClick: () => i("filters"),
              onKeyDown: (r) => {
                r.key === "Enter" || r.key === " " ? (r.preventDefault(), i("filters")) : r.key === "ArrowRight" ? (r.preventDefault(), i("columns")) : r.key === "ArrowLeft" && (r.preventDefault(), i("fields"));
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                fontWeight: n === "filters" ? 600 : 400,
                color: n === "filters" ? d.primary : d.textSec,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "6px"
              },
              children: [
                "Filters",
                Object.keys(pe).length > 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "6px", height: "6px", borderRadius: "50%", background: "#d32f2f" } })
              ]
            }
          ),
          /* @__PURE__ */ e.jsx(
            "div",
            {
              role: "tab",
              tabIndex: 0,
              "aria-selected": n === "columns",
              className: "sp-tab",
              onClick: () => i("columns"),
              onKeyDown: (r) => {
                r.key === "Enter" || r.key === " " ? (r.preventDefault(), i("columns")) : r.key === "ArrowRight" ? (r.preventDefault(), i("fields")) : r.key === "ArrowLeft" && (r.preventDefault(), i("filters"));
              },
              style: {
                padding: "8px 10px",
                cursor: "pointer",
                textAlign: "center",
                fontWeight: n === "columns" ? 600 : 400,
                color: n === "columns" ? d.primary : d.textSec
              },
              children: "Columns"
            }
          ),
          /* @__PURE__ */ e.jsx(
            "div",
            {
              "aria-hidden": "true",
              style: {
                position: "absolute",
                bottom: "-1px",
                left: 0,
                width: "33.3333%",
                height: "2px",
                borderRadius: "2px 2px 0 0",
                background: d.primary,
                transform: `translateX(${Math.max(0, ["fields", "filters", "columns"].indexOf(n)) * 100}%)`,
                transition: "transform 200ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))",
                pointerEvents: "none"
              }
            }
          )
        ] }),
        Ne && $e === "report" ? /* @__PURE__ */ e.jsx("div", { style: { flex: 1, overflowY: "auto", padding: "0 12px 12px" }, children: /* @__PURE__ */ e.jsx(
          vn,
          {
            reportDef: kr,
            setReportDef: wr,
            availableFields: $,
            customDimensions: Se,
            theme: d,
            styles: G,
            data: fe,
            valConfigs: j,
            colFields: x,
            setColFields: f,
            setValConfigs: y,
            savedReports: Cr,
            setSavedReports: Rr,
            activeReportId: Ar,
            setActiveReportId: Dr,
            showNotification: A,
            requestedSelection: pt,
            showReportConfigColumn: ut,
            setShowReportConfigColumn: Sr
          }
        ) }) : Ne && $e === "fields" ? /* @__PURE__ */ e.jsxs("div", { style: { padding: "0 4px" }, children: [
          /* @__PURE__ */ e.jsx(
            rr,
            {
              availableFields: $,
              customDimensions: Se,
              setCustomDimensions: Ft,
              data: fe,
              theme: d,
              showNotification: A,
              getFieldLabel: te,
              onDeleteField: It
            }
          ),
          /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "10px", color: d.textSec, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px", padding: "0 2px" }, children: [
            /* @__PURE__ */ e.jsx(F.Database, { style: { verticalAlign: "middle", marginRight: "4px" } }),
            "Available Fields — click to add"
          ] }),
          Yt(re.length, vt.length),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: "4px", marginBottom: "14px" }, children: [
            vt.map(
              /** @param {any} [f] @returns {any} */
              (r) => {
                const u = j.some(
                  /** @param {any} [c] @returns {any} */
                  (p) => p && p.field === r && p.agg !== "formula"
                );
                return /* @__PURE__ */ e.jsxs(
                  "div",
                  {
                    onClick: () => {
                      u || y(
                        /** @param {any} [prev] @returns {any} */
                        (p) => [...p, { field: r, agg: "sum", label: te(r) }]
                      );
                    },
                    style: {
                      padding: "3px 9px",
                      fontSize: "11px",
                      lineHeight: 1.4,
                      fontWeight: 500,
                      borderRadius: "999px",
                      border: `1px solid ${u || d.isDark ? d.border : "#D7DDEA"}`,
                      background: u ? d.select || d.headerBg : d.isDark ? d.background : "#ffffff",
                      color: u ? d.primary : d.text,
                      cursor: u ? "default" : "pointer",
                      opacity: u ? 0.6 : 1,
                      userSelect: "none"
                    },
                    title: u ? `${te(r)} already in values` : `Add ${te(r)} to values`,
                    children: [
                      te(r),
                      u ? " ✓" : ""
                    ]
                  },
                  r
                );
              }
            ),
            re.length > 8 && vt.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { borderTop: `1px solid ${d.border}`, paddingTop: "10px", marginBottom: "8px" }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 2px 8px 2px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsx("button", { type: "button", onClick: Ut, style: { border: `1px solid ${d.primary}66`, background: `${d.primary}12`, color: d.primary, borderRadius: "999px", cursor: "pointer", padding: "4px 10px", fontSize: "11px", fontWeight: 700 }, children: "+ Add Formula" }),
              /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
                Oe,
                " value",
                Oe === 1 ? "" : "s",
                " · ",
                ze,
                " formula",
                ze === 1 ? "" : "s"
              ] })
            ] }),
            /* @__PURE__ */ e.jsx(
              ar,
              {
                measureAxis: E,
                setMeasureAxis: s,
                theme: d,
                valueSelectStyle: be,
                regularValueCount: Oe
              }
            )
          ] }),
          j.map(
            /** @param {any} [item] @param {any} [idx] @returns {any} */
            (r, u) => {
              const p = r && r.agg === "formula";
              if (!r) return null;
              const m = $r === u;
              return /* @__PURE__ */ e.jsx(
                "div",
                {
                  draggable: !0,
                  onDragStart: () => {
                    tt.current = u;
                  },
                  onDragOver: (v) => {
                    v.preventDefault(), rt(u);
                  },
                  onDragLeave: () => rt(null),
                  onDrop: (v) => {
                    v.preventDefault();
                    const T = tt.current;
                    rt(null), tt.current = null, !(T === null || T === u) && y(
                      /** @param {any} [prev] @returns {any} */
                      (X) => {
                        const S = [...X], [K] = S.splice(T, 1);
                        return S.splice(u, 0, K), S;
                      }
                    );
                  },
                  onDragEnd: () => {
                    tt.current = null, rt(null);
                  },
                  style: { marginBottom: "6px", borderTop: m ? `2px solid ${d.primary}` : "2px solid transparent", cursor: "grab" },
                  children: p ? /* @__PURE__ */ e.jsx(ir, { item: r, idx: u, selected: r.field === ye, onSelect: Vt, onRemove: Xe, setValConfigs: y, theme: d, styles: G, dropLine: null, zoneId: "vals", valueSelectStyle: be, colFields: x, fixedSparklineValueKeySet: bt, showNotification: A, serverError: Ie && Ie[r.field] || null }) : /* @__PURE__ */ e.jsxs("div", { style: { ...G.chip, flexWrap: "wrap", alignItems: "flex-start" }, children: [
                    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                      /* @__PURE__ */ e.jsx(F.DragIndicator, { style: { flexShrink: 0, opacity: 0.4 } }),
                      /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: te(r.field) })
                    ] }),
                    /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                      /* @__PURE__ */ e.jsxs(ne, { size: "sm", "aria-label": "Aggregation", value: r.agg, onClick: (v) => v.stopPropagation(), onChange: (v) => Ht(u, v.target.value), style: { ...be, width: "78px" }, children: [
                        /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                        /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Avg" }),
                        /* @__PURE__ */ e.jsx("option", { value: "count", children: "Cnt" }),
                        /* @__PURE__ */ e.jsx("option", { value: "min", children: "Min" }),
                        /* @__PURE__ */ e.jsx("option", { value: "max", children: "Max" }),
                        /* @__PURE__ */ e.jsx("option", { value: "weighted_avg", children: "WAvg" })
                      ] }),
                      /* @__PURE__ */ e.jsx("span", { onClick: () => Xe(u), style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(F.Close, {}) })
                    ] })
                  ] })
                },
                r.field || u
              );
            }
          ),
          j.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { opacity: 0.5, fontSize: "11px", padding: "12px", textAlign: "center" }, children: "No value fields. Click a field above or add a formula." })
        ] }) : !Ne && n === "filters" ? /* @__PURE__ */ e.jsxs("div", { style: { flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", padding: "8px" }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "10px", display: "flex", alignItems: "center", background: d.background, borderRadius: "6px", padding: "4px 8px", border: `1px solid ${d.border}` }, children: [
            /* @__PURE__ */ e.jsx(F.Search, {}),
            /* @__PURE__ */ e.jsx(
              "input",
              {
                placeholder: "Search columns...",
                value: R,
                onChange: (r) => O(r.target.value),
                style: { border: "none", background: "transparent", marginLeft: "10px", outline: "none", width: "100%", color: d.text, fontSize: "13px" }
              }
            )
          ] }),
          (() => {
            const p = $.map(
              /** @param {any} [field] @returns {any} */
              (m) => {
                const v = b.getColumn(m);
                return v || { id: m, header: te(m), columnDef: { header: te(m) } };
              }
            ).filter(
              /** @param {any} [col] @returns {any} */
              (m) => {
                const v = m.columnDef && typeof m.columnDef.header == "string" ? m.columnDef.header : typeof m.header == "string" ? m.header : m.id;
                return String(v).toLowerCase().includes(R.toLowerCase()) || m.id.toLowerCase().includes(R.toLowerCase());
              }
            );
            return /* @__PURE__ */ e.jsx("div", { style: { display: "flex", flexDirection: "column" }, children: p.map(
              /** @param {any} [col] @returns {any} */
              (m) => /* @__PURE__ */ e.jsx(
                on,
                {
                  column: m,
                  theme: d,
                  styles: G,
                  onFilter: (
                    /** @param {any} [val] @returns {any} */
                    ((v) => D(m.id, v))
                  ),
                  currentFilter: pe[m.id],
                  options: Nt(m.id),
                  optionMeta: We && We[m.id],
                  onSearchOptions: Qe ? (
                    /** @param {any} [search] @returns {any} */
                    ((v) => Z && Z(m.id, { search: v, offset: 0 }))
                  ) : void 0,
                  onLoadMoreOptions: Qe ? (
                    /** @param {any} [search] @param {any} [offset] @returns {any} */
                    ((v, T) => Z && Z(m.id, { search: v, offset: T }))
                  ) : void 0,
                  onOpen: Z
                },
                m.id
              )
            ) });
          })()
        ] }) : (Ne ? $e !== "columns" : n !== "columns") ? /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
          n === "fields" && /* @__PURE__ */ e.jsxs("div", { style: { flexShrink: 0, marginBottom: "16px" }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: G.sectionTitleSm, children: [
              /* @__PURE__ */ e.jsx(F.Database, {}),
              " Available Fields"
            ] }),
            Yt(Pe.length, mt.length),
            /* @__PURE__ */ e.jsxs(
              an,
              {
                panelId: "availableFields",
                panelLabel: "Available Fields",
                size: Ae && Ae.availableFields,
                onSizeChange: Br,
                getSidebarWidth: Ir,
                onSidebarWidthChange: Tr,
                theme: d,
                outerStyle: Ot,
                contentStyle: {
                  display: "flex",
                  flexDirection: "column",
                  gap: "1px",
                  padding: "2px 12px 12px 2px"
                },
                children: [
                  mt.map(
                    /** @param {any} [f] @returns {any} */
                    (r) => {
                      const u = Ye && r === je, p = u ? "Measure Names" : te(r), m = u ? "measureAxis" : "pool";
                      return /* @__PURE__ */ e.jsxs(
                        "div",
                        {
                          draggable: !0,
                          "data-sidebar-field-chip": u ? "measure-axis-label" : "available",
                          role: "button",
                          tabIndex: 0,
                          "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                          "aria-grabbed": q && q.zone === m && q.field === r ? "true" : "false",
                          "aria-label": `Move ${p}`,
                          onDragStart: (v) => Y(v, r, m),
                          onDragEnd: z,
                          onKeyDown: (v) => Lt(v, r, m, -1, p),
                          title: u ? "Drag to Rows or Columns to show measure names" : `Move ${te(r)}`,
                          className: "sp-field-row",
                          style: {
                            padding: "4px 8px",
                            fontSize: "12px",
                            lineHeight: 1.3,
                            fontWeight: u ? 700 : 500,
                            borderRadius: "6px",
                            border: u ? `1px dashed ${d.primary}` : "1px solid transparent",
                            background: u ? `${d.primary}12` : "transparent",
                            color: u ? d.primary : d.text,
                            display: "flex",
                            alignItems: "center",
                            gap: "8px",
                            cursor: "grab",
                            userSelect: "none",
                            minHeight: "26px",
                            whiteSpace: "nowrap",
                            overflow: "hidden"
                          },
                          children: [
                            /* @__PURE__ */ e.jsx("span", { "aria-hidden": "true", style: { display: "inline-flex", width: "14px", justifyContent: "center", color: u ? d.primary : d.textSec, flexShrink: 0 }, children: u ? /* @__PURE__ */ e.jsx(F.Sigma, {}) : /* @__PURE__ */ e.jsx(F.DragIndicator, {}) }),
                            /* @__PURE__ */ e.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis" }, children: p })
                          ]
                        },
                        u ? `__measure_axis__${r}` : r
                      );
                    }
                  ),
                  Pe.length > 8 && mt.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { width: "100%", padding: "8px 2px", fontSize: "11px", color: d.textSec }, children: "No fields match the current search." })
                ]
              }
            )
          ] }),
          Mr.map(
            /** @param {any} [zone] @returns {any} */
            (r) => {
              const u = Ge[r.id] || [];
              return /* @__PURE__ */ e.jsxs("div", { style: { marginBottom: "20px", flexShrink: 0 }, children: [
                /* @__PURE__ */ e.jsxs("div", { style: G.sectionTitle, children: [
                  r.icon,
                  r.label
                ] }),
                r.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "0 4px 8px 4px", flexWrap: "wrap" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      type: "button",
                      onClick: Ut,
                      style: {
                        border: `1px solid ${d.primary}66`,
                        background: `${d.primary}12`,
                        color: d.primary,
                        borderRadius: "999px",
                        cursor: "pointer",
                        padding: "4px 10px",
                        fontSize: "11px",
                        fontWeight: 700
                      },
                      children: "+ Add Formula"
                    }
                  ),
                  /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
                    Oe,
                    " value measure",
                    Oe === 1 ? "" : "s",
                    " available"
                  ] }),
                  ze > 0 && /* @__PURE__ */ e.jsxs("span", { style: { fontSize: "10px", color: d.textSec }, children: [
                    ze,
                    " formula",
                    ze === 1 ? "" : "s"
                  ] }),
                  /* @__PURE__ */ e.jsx(
                    ar,
                    {
                      measureAxis: E,
                      setMeasureAxis: s,
                      theme: d,
                      valueSelectStyle: be,
                      regularValueCount: Oe
                    }
                  )
                ] }),
                r.id === "rows" && o.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", padding: "0 4px 6px 4px" }, children: [
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      title: "Expand all hierarchy levels",
                      onClick: () => Q(!0),
                      style: { flex: 1, border: `1px solid ${d.primary}`, background: d.select, cursor: "pointer", padding: "3px 6px", fontSize: "11px", color: d.primary, borderRadius: "4px", fontWeight: 600 },
                      children: "+ Expand All"
                    }
                  ),
                  /* @__PURE__ */ e.jsx(
                    "button",
                    {
                      title: "Collapse all rows",
                      onClick: () => Q(!1),
                      style: { flex: 1, border: `1px solid ${d.border}`, background: d.background, cursor: "pointer", padding: "3px 6px", fontSize: "11px", color: d.textSec, borderRadius: "4px" },
                      children: "- Collapse All"
                    }
                  )
                ] }),
                /* @__PURE__ */ e.jsx(
                  "div",
                  {
                    "data-sidebar-zone-panel": r.id,
                    style: {
                      ...Ot,
                      boxSizing: "border-box",
                      maxHeight: "280px",
                      overflow: "auto",
                      padding: `${Pt} 12px 4px ${Pt}`
                    },
                    children: /* @__PURE__ */ e.jsxs(
                      "div",
                      {
                        "data-sidebar-drop-zone": r.id,
                        role: "group",
                        tabIndex: 0,
                        "aria-label": `${r.label} drop zone`,
                        "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                        onKeyDown: (p) => _r(p, r.id),
                        onDragOver: (p) => {
                          p.target === p.currentTarget ? L(p, r.id, u.length) : (p.preventDefault(), p.dataTransfer && (p.dataTransfer.dropEffect = "move"));
                        },
                        onDrop: (p) => g(p, r.id),
                        children: [
                          u.map(
                            /** @param {any} [item] @param {any} [idx] @returns {any} */
                            (p, m) => {
                              const v = r.id === "vals" ? p.field : p, T = r.id === "vals" ? p.agg === "formula" ? p.label || "Formula" : te(p.field) : te(p), X = (r.id === "rows" || r.id === "cols") && v === je && Wt.placement !== "none";
                              return /* @__PURE__ */ e.jsxs(
                                "div",
                                {
                                  draggable: !0,
                                  "data-sidebar-field-chip": "zone",
                                  role: "button",
                                  tabIndex: 0,
                                  "aria-describedby": "pivot-sidebar-keyboard-dnd-help",
                                  "aria-grabbed": q && q.zone === r.id && q.idx === m ? "true" : "false",
                                  "aria-label": `Move ${T}`,
                                  onKeyDown: (S) => Lt(S, p, r.id, m, T),
                                  onDragStart: (S) => Y(S, p, r.id, m),
                                  onDragOver: (S) => L(S, r.id, m),
                                  onDragEnd: z,
                                  children: [
                                    r.id === "vals" && p.agg === "formula" ? /* @__PURE__ */ e.jsx(
                                      ir,
                                      {
                                        item: p,
                                        idx: m,
                                        selected: p.field === Te,
                                        onSelect: Vt,
                                        onRemove: Xe,
                                        setValConfigs: y,
                                        theme: d,
                                        styles: G,
                                        dropLine: _,
                                        zoneId: r.id,
                                        valueSelectStyle: be,
                                        colFields: x,
                                        fixedSparklineValueKeySet: bt,
                                        showNotification: A,
                                        serverError: Ie && Ie[p.field] || null
                                      }
                                    ) : /* @__PURE__ */ e.jsxs("div", { style: {
                                      ...G.chip,
                                      ...r.id === "vals" ? { flexWrap: "wrap", alignItems: "flex-start" } : {}
                                    }, children: [
                                      _ && _.zone === r.id && _.idx === m && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...G.dropLine, top: -2 } }),
                                      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "8px", minWidth: 0, flex: 1 }, children: [
                                        /* @__PURE__ */ e.jsx(F.DragIndicator, {}),
                                        /* @__PURE__ */ e.jsx("b", { style: { fontWeight: 500, display: "block", minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }, children: T })
                                      ] }),
                                      r.id === "vals" && /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", alignItems: "center", gap: "4px", marginLeft: "8px", flexWrap: "nowrap", flexShrink: 0 }, children: [
                                        /* @__PURE__ */ e.jsxs(ne, { size: "sm", value: p.agg, onClick: (S) => S.stopPropagation(), onChange: (S) => Ht(m, S.target.value), style: { ...be, width: "78px" }, title: "Aggregation", children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "sum", children: "Sum" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "avg", children: "Avg" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "count", children: "Cnt" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "min", children: "Min" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "max", children: "Max" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "weighted_avg", children: "WAvg" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "array_agg", children: "Series" })
                                        ] }),
                                        Xt(p.agg) && /* @__PURE__ */ e.jsxs(ne, { size: "sm", value: p.weightField || "", onClick: (S) => S.stopPropagation(), onChange: (S) => Lr(m, S.target.value), style: { ...be, width: "96px" }, title: "Weight field", children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "", disabled: !0, children: "Weight" }),
                                          re.map(
                                            /** @param {any} [fieldName] @returns {any} */
                                            (S) => /* @__PURE__ */ e.jsx("option", { value: S, children: te(S) }, S)
                                          )
                                        ] }),
                                        /* @__PURE__ */ e.jsxs(ne, { size: "sm", "aria-label": "Window calculation", value: p.windowFn || "none", onClick: (S) => S.stopPropagation(), onChange: (S) => Pr(m, S.target.value), style: { ...be, width: "68px" }, children: [
                                          /* @__PURE__ */ e.jsx("option", { value: "none", children: "Value" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_row", children: "%Row" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_col", children: "%Col" }),
                                          /* @__PURE__ */ e.jsx("option", { value: "percent_of_grand_total", children: "%Tot" })
                                        ] })
                                      ] }),
                                      /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "4px", marginLeft: "auto", alignItems: "center" }, children: [
                                        r.id === "filter" && /* @__PURE__ */ e.jsx("div", { onClick: (S) => Or(S, v), style: { cursor: "pointer", display: "flex", alignItems: "center", padding: "2px", borderRadius: "4px" }, children: /* @__PURE__ */ e.jsx(F.Filter, {}) }),
                                        /* @__PURE__ */ e.jsx("span", { onClick: () => {
                                          if (X && typeof s == "function") {
                                            s(
                                              /** @param {any} [previous] @returns {any} */
                                              (S) => ({
                                                ...Je(S),
                                                placement: "none"
                                              })
                                            ), r.id === "rows" && l(
                                              /** @param {any} [p] @returns {any} */
                                              (S) => Array.isArray(S) ? S.filter(
                                                /** @param {any} [x] @returns {any} */
                                                (K) => K !== v
                                              ) : S
                                            ), r.id === "cols" && f(
                                              /** @param {any} [p] @returns {any} */
                                              (S) => Array.isArray(S) ? S.filter(
                                                /** @param {any} [x] @returns {any} */
                                                (K) => K !== v
                                              ) : S
                                            );
                                            return;
                                          }
                                          if (r.id === "filter") {
                                            const S = { ...pe };
                                            delete S[v], xt(S);
                                          }
                                          r.id === "rows" && l(
                                            /** @param {any} [p] @returns {any} */
                                            (S) => S.filter(
                                              /** @param {any} [x] @returns {any} */
                                              (K) => K !== v
                                            )
                                          ), r.id === "cols" && f(
                                            /** @param {any} [p] @returns {any} */
                                            (S) => S.filter(
                                              /** @param {any} [x] @returns {any} */
                                              (K) => K !== v
                                            )
                                          ), r.id === "vals" && Xe(m);
                                        }, className: "sp-chip-remove", style: { cursor: "pointer", color: "#9CA3AF", display: "flex", alignItems: "center" }, children: /* @__PURE__ */ e.jsx(F.Close, {}) })
                                      ] }),
                                      r.id === "filter" && et.columnId === v && /* @__PURE__ */ e.jsx(
                                        tn,
                                        {
                                          column: { header: T, id: v },
                                          anchorEl: et.anchorEl,
                                          onClose: Nr,
                                          onFilter: (
                                            /** @param {any} [filterValue] @returns {any} */
                                            ((S) => D(v, S))
                                          ),
                                          currentFilter: pe[v],
                                          options: Nt(v),
                                          optionMeta: We && We[v],
                                          onSearchOptions: Qe ? (
                                            /** @param {any} [search] @returns {any} */
                                            ((S) => Z && Z(v, { search: S, offset: 0 }))
                                          ) : void 0,
                                          onLoadMoreOptions: Qe ? (
                                            /** @param {any} [search] @param {any} [offset] @returns {any} */
                                            ((S, K) => Z && Z(v, { search: S, offset: K }))
                                          ) : void 0,
                                          theme: d
                                        }
                                      ),
                                      r.id === "vals" && /* @__PURE__ */ e.jsx(
                                        mr,
                                        {
                                          item: p,
                                          idx: m,
                                          setValConfigs: y,
                                          theme: d,
                                          valueSelectStyle: be,
                                          colFields: x,
                                          fixedSparklineValueKeySet: bt,
                                          showNotification: A
                                        }
                                      ),
                                      _ && _.zone === r.id && _.idx === m + 1 && /* @__PURE__ */ e.jsx("div", { className: "sp-drop-line", style: { ...G.dropLine, bottom: -2 } })
                                    ] }),
                                    r.id === "filter" && pe[v] && pe[v].conditions && /* @__PURE__ */ e.jsx("div", { style: { fontSize: "10px", color: d.primary, padding: "0 8px 4px 8px", marginTop: "-4px" }, children: pe[v].conditions.map(
                                      /** @param {any} [c] @returns {any} */
                                      (S) => `${S.type}: ${S.value}${S.caseSensitive ? " (Match Case)" : ""}`
                                    ).join(` ${pe[v].operator} `) })
                                  ]
                                },
                                m
                              );
                            }
                          ),
                          u.length === 0 && /* @__PURE__ */ e.jsx("div", { style: { opacity: 0.5, fontSize: "11px", padding: "8px", textAlign: "center", pointerEvents: "none" }, children: "Drag fields here" }),
                          /* @__PURE__ */ e.jsx("div", { style: { height: 20 }, onDragOver: (p) => L(p, r.id, u.length), onDrop: (p) => g(p, r.id) })
                        ]
                      }
                    )
                  }
                )
              ] }, r.id);
            }
          ),
          n === "fields" && /* @__PURE__ */ e.jsxs("div", { "data-sidebar-modeling": "true", style: { flexShrink: 0, marginBottom: "16px", borderTop: `1px solid ${d.border}`, paddingTop: "8px" }, children: [
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                "aria-expanded": ht,
                "aria-controls": "pivot-sidebar-modeling",
                onClick: Fr,
                style: { ...G.sectionTitleSm, display: "flex", alignItems: "center", width: "100%", border: "none", background: "transparent", cursor: "pointer", justifyContent: "space-between", padding: "4px 2px", color: d.textSec },
                children: [
                  /* @__PURE__ */ e.jsxs("span", { style: { display: "inline-flex", alignItems: "center", gap: "6px" }, children: [
                    /* @__PURE__ */ e.jsx(F.Settings, {}),
                    " Modeling",
                    Array.isArray(Se) && Se.length > 0 ? /* @__PURE__ */ e.jsx("span", { style: { fontSize: "10px", fontWeight: 700, color: d.primary, background: d.select, borderRadius: "999px", padding: "0 6px" }, children: Se.length }) : null
                  ] }),
                  /* @__PURE__ */ e.jsx("span", { "aria-hidden": "true", style: { display: "inline-flex", transform: ht ? "rotate(180deg)" : "none" }, children: /* @__PURE__ */ e.jsx(F.ChevronDown, {}) })
                ]
              }
            ),
            ht ? /* @__PURE__ */ e.jsx("div", { id: "pivot-sidebar-modeling", children: /* @__PURE__ */ e.jsx(
              rr,
              {
                availableFields: $,
                customDimensions: Se,
                setCustomDimensions: Ft,
                data: fe,
                theme: d,
                showNotification: A,
                getFieldLabel: te,
                onDeleteField: It
              }
            ) }) : null
          ] })
        ] }) : /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "16px", height: "100%", overflow: "hidden" }, children: [
          /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", flexDirection: "column", gap: "10px", padding: "10px", background: d.headerSubtleBg || d.headerBg, border: `1px solid ${d.border}`, borderRadius: "10px" }, children: [
            /* @__PURE__ */ e.jsxs("div", { className: "sp-search-shell", style: { display: "flex", alignItems: "center", background: d.surfaceBg || d.background, borderRadius: "8px", padding: "7px 10px", border: `1px solid ${d.border}` }, children: [
              /* @__PURE__ */ e.jsx(F.Search, {}),
              /* @__PURE__ */ e.jsx(
                "input",
                {
                  placeholder: "Search columns...",
                  value: R,
                  onChange: (r) => O(r.target.value),
                  style: {
                    border: "none",
                    background: "transparent",
                    marginLeft: "8px",
                    outline: "none",
                    width: "100%",
                    color: d.text,
                    fontSize: "13px",
                    fontWeight: 500
                  }
                }
              ),
              R && /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-ghost",
                  "aria-label": "Clear column search",
                  onClick: () => O(""),
                  style: {
                    cursor: "pointer",
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    border: "none",
                    color: d.textSec,
                    padding: "4px",
                    borderRadius: "6px",
                    background: d.hover
                  },
                  children: /* @__PURE__ */ e.jsx(F.Close, {})
                }
              )
            ] }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
              { value: "all", label: "All", Icon: F.TypeAll },
              { value: "number", label: "Numbers", Icon: F.TypeNumber },
              { value: "string", label: "Text", Icon: F.TypeText },
              { value: "date", label: "Dates", Icon: F.TypeDate }
            ].map((r) => {
              const u = a === r.value, p = r.Icon;
              return /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: `sp-btn ${u ? "sp-btn-primary" : "sp-btn-subtle"}`,
                  onClick: () => k(r.value),
                  style: {
                    padding: "6px 10px",
                    borderRadius: "8px",
                    border: `1px solid ${u ? d.primary : d.border}`,
                    background: u ? d.primary : d.surfaceBg || d.background,
                    color: u ? "#fff" : d.text,
                    cursor: "pointer",
                    fontSize: "11px",
                    fontWeight: 600,
                    display: "inline-flex",
                    alignItems: "center",
                    gap: "5px",
                    boxShadow: u ? "0 2px 6px rgba(0,0,0,0.12)" : "none"
                  },
                  children: [
                    /* @__PURE__ */ e.jsx(p, {}),
                    /* @__PURE__ */ e.jsx("span", { children: r.label })
                  ]
                },
                r.value
              );
            }) })
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "4px", padding: "0 8px" }, children: [
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                className: "sp-btn sp-btn-subtle",
                onClick: () => b.toggleAllColumnsVisible(!0),
                title: "Show All Columns",
                style: {
                  ...G.btn,
                  padding: "6px 4px",
                  fontSize: "11px",
                  gap: "4px",
                  minWidth: 0,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                },
                children: [
                  /* @__PURE__ */ e.jsx(F.Visibility, { style: { fontSize: "13px", flexShrink: 0 } }),
                  /* @__PURE__ */ e.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: "Show" })
                ]
              }
            ),
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                className: "sp-btn sp-btn-subtle",
                onClick: () => b.toggleAllColumnsVisible(!1),
                title: "Hide All Columns",
                style: {
                  ...G.btn,
                  padding: "6px 4px",
                  fontSize: "11px",
                  gap: "4px",
                  minWidth: 0,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                },
                children: [
                  /* @__PURE__ */ e.jsx(F.VisibilityOff, { style: { fontSize: "13px", flexShrink: 0 } }),
                  /* @__PURE__ */ e.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: "Hide" })
                ]
              }
            ),
            /* @__PURE__ */ e.jsxs(
              "button",
              {
                type: "button",
                className: "sp-btn sp-btn-subtle",
                onClick: () => ee(!1),
                title: "Unpin All Columns",
                style: {
                  ...G.btn,
                  padding: "6px 4px",
                  fontSize: "11px",
                  gap: "4px",
                  minWidth: 0,
                  justifyContent: "center",
                  background: d.background,
                  fontWeight: 600,
                  boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                },
                children: [
                  /* @__PURE__ */ e.jsx(F.Unpin, { style: { fontSize: "13px", flexShrink: 0 } }),
                  /* @__PURE__ */ e.jsx("span", { style: { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }, children: "Unpin" })
                ]
              }
            )
          ] }),
          w.size > 0 && /* @__PURE__ */ e.jsxs("div", { style: {
            display: "flex",
            flexDirection: "column",
            gap: "8px",
            background: d.select || "var(--sp-select)",
            padding: "10px",
            borderRadius: "var(--sp-radius-md, 8px)",
            border: "1px solid var(--sp-border-strong, var(--sp-primary))",
            boxShadow: "var(--sp-shadow-sm, 0 2px 8px rgba(0,0,0,0.1))",
            margin: "0 8px"
          }, children: [
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "center" }, children: [
              /* @__PURE__ */ e.jsxs("span", { style: {
                fontSize: "12px",
                fontWeight: 700,
                padding: "4px 10px",
                color: d.primary,
                background: d.background,
                borderRadius: "6px",
                boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
              }, children: [
                w.size,
                " Column",
                w.size > 1 ? "s" : "",
                " Selected"
              ] }),
              /* @__PURE__ */ e.jsx(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-ghost",
                  onClick: () => B(/* @__PURE__ */ new Set()),
                  style: {
                    border: "none",
                    background: d.background,
                    cursor: "pointer",
                    display: "flex",
                    color: d.textSec,
                    padding: "4px",
                    borderRadius: "4px"
                  },
                  children: /* @__PURE__ */ e.jsx(F.Close, {})
                }
              )
            ] }),
            /* @__PURE__ */ e.jsxs("div", { style: { display: "flex", gap: "6px", flexWrap: "wrap" }, children: [
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-subtle",
                  onClick: () => {
                    Array.from(w).forEach((r) => U(r, "left")), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...G.btn,
                    padding: "6px 8px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                  },
                  children: [
                    /* @__PURE__ */ e.jsx(F.PinLeft, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Pin Left" })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-subtle",
                  onClick: () => {
                    Array.from(w).forEach((r) => U(r, !1)), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...G.btn,
                    padding: "6px 8px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                  },
                  children: [
                    /* @__PURE__ */ e.jsx(F.Unpin, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Unpin" })
                  ]
                }
              ),
              /* @__PURE__ */ e.jsxs(
                "button",
                {
                  type: "button",
                  className: "sp-btn sp-btn-subtle",
                  onClick: () => {
                    Array.from(w).forEach((r) => U(r, "right")), B(/* @__PURE__ */ new Set());
                  },
                  style: {
                    ...G.btn,
                    padding: "6px 8px",
                    fontSize: "11px",
                    background: d.background,
                    flex: 1,
                    justifyContent: "center",
                    fontWeight: 600,
                    boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
                  },
                  children: [
                    /* @__PURE__ */ e.jsx(F.PinRight, {}),
                    /* @__PURE__ */ e.jsx("span", { children: "Pin Right" })
                  ]
                }
              )
            ] })
          ] }),
          /* @__PURE__ */ e.jsx("div", { style: { flex: 1, overflowY: "auto", display: "flex", flexDirection: "column" }, children: (() => {
            const u = b.getAllColumns().filter(
              /** @param {any} [c] @returns {any} */
              (S) => !S.parent && S.id !== "no_data"
            ).filter(
              /** @param {any} [col] @returns {any} */
              (S) => {
                if (a === "all") return !0;
                let K = "string";
                if (j.find(
                  /** @param {any} [v] @returns {any} */
                  (ie) => S.id.includes(ie.field)
                ))
                  K = "number";
                else if (fe && fe.length > 0) {
                  const ie = fe[0];
                  let ae = ie[S.id];
                  if (ae === void 0 && S.accessorFn)
                    try {
                      ae = S.accessorFn(ie);
                    } catch {
                    }
                  typeof ae == "number" ? K = "number" : ae instanceof Date ? K = "date" : typeof ae == "string" && (!isNaN(Number(ae)) && ae.trim() !== "" ? K = "number" : !isNaN(Date.parse(ae)) && ae.includes("-") && (K = "date"));
                }
                return K === a;
              }
            ), p = u.filter(
              /** @param {any} [c] @returns {any} */
              (S) => it(S, "left")
            ), m = u.filter(
              /** @param {any} [c] @returns {any} */
              (S) => it(S, "right")
            ), v = u.filter(
              /** @param {any} [c] @returns {any} */
              (S) => it(S, "unpinned")
            ), T = (S, K) => S.map(
              /** @param {any} [column] @returns {any} */
              (oe) => /* @__PURE__ */ e.jsx(
                dr,
                {
                  column: oe,
                  level: 0,
                  theme: d,
                  styles: G,
                  handlePinColumn: U,
                  colSearch: R,
                  selectedCols: w,
                  setSelectedCols: B,
                  onDrop: X,
                  sectionId: K
                },
                oe.id
              )
            ), X = (S, K, oe) => {
              let ie;
              const ae = P || { left: [], right: [] };
              if (oe) {
                const qe = K === "left" ? ae.left : K === "right" ? ae.right : null;
                if (qe) {
                  if (ie = qe.indexOf(oe), ie === -1) {
                    const kt = b.getColumn(oe);
                    if (kt && nn(kt)) {
                      const Zt = Ct(kt).find(
                        /** @param {any} [id] @returns {any} */
                        (Hr) => qe.includes(Hr)
                      );
                      Zt && (ie = qe.indexOf(Zt));
                    }
                  }
                  ie === -1 && (ie = void 0);
                }
              }
              K === "left" ? U(S, "left", ie) : K === "right" ? U(S, "right", ie) : U(S, !1);
            };
            return /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
              p.length > 0 && /* @__PURE__ */ e.jsx(
                wt,
                {
                  title: "Pinned Left",
                  count: p.length,
                  theme: d,
                  styles: G,
                  sectionId: "left",
                  onDrop: X,
                  children: T(p, "left")
                }
              ),
              m.length > 0 && /* @__PURE__ */ e.jsx(
                wt,
                {
                  title: "Pinned Right",
                  count: m.length,
                  theme: d,
                  styles: G,
                  sectionId: "right",
                  onDrop: X,
                  children: T(m, "right")
                }
              ),
              /* @__PURE__ */ e.jsx(
                wt,
                {
                  title: "Columns",
                  count: v.length,
                  theme: d,
                  styles: G,
                  sectionId: "unpinned",
                  onDrop: X,
                  children: T(v, "unpinned")
                }
              )
            ] });
          })() }),
          C && C.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { padding: "8px", borderTop: `1px solid ${d.border}` }, children: [
            /* @__PURE__ */ e.jsx("div", { style: G.sectionTitle, children: "Pinning Presets" }),
            /* @__PURE__ */ e.jsx("div", { style: { display: "flex", gap: "4px", flexWrap: "wrap" }, children: C.map(
              /** @param {any} [preset] @param {any} [i] @returns {any} */
              (r, u) => /* @__PURE__ */ e.jsx(
                "button",
                {
                  onClick: () => {
                    M(r.config), A(`Applied preset: ${r.name}`);
                  },
                  style: { ...G.btn, fontSize: "11px", background: d.headerBg },
                  children: r.name
                },
                u
              )
            ) })
          ] }),
          /* @__PURE__ */ e.jsxs("div", { style: { fontSize: "11px", color: d.textSec, fontStyle: "italic", padding: "8px", background: d.headerBg, borderTop: `1px solid ${d.border}44` }, children: [
            /* @__PURE__ */ e.jsx(F.Lock, { style: { verticalAlign: "middle", marginRight: "4px", opacity: 0.5 } }),
            "Drag columns or use pin icons to freeze areas."
          ] })
        ] })
      ] }),
      jt && /* @__PURE__ */ e.jsx(
        En,
        {
          item: jt,
          idx: St,
          valConfigs: j,
          setValConfigs: y,
          theme: d,
          onClose: () => De(null),
          onRemove: Xe,
          displayedColumnOptions: ve,
          availableRawFields: re,
          serverError: Ie && Ie[jt.field] || null
        }
      ),
      /* @__PURE__ */ e.jsx(
        "div",
        {
          className: "sp-resize-handle",
          onMouseDown: Wr,
          title: "Drag to resize panel",
          style: { position: "absolute", right: -2, top: 0, bottom: 0, width: "9px", cursor: "col-resize", zIndex: 10, background: "transparent", display: "flex", alignItems: "center", justifyContent: "center" },
          children: /* @__PURE__ */ e.jsx("span", { className: "sp-resize-pill", "aria-hidden": "true", style: { width: "2px", height: "100%", borderRadius: "999px", background: d.border, opacity: 0.55, pointerEvents: "none" } })
        }
      )
    ] });
  }
);
export {
  Bn as SidebarPanel
};
