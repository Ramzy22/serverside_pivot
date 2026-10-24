import { j as or } from "./index-DMKf03Q6.js";
import "./charts-DthW1Q8W.js";
import { r as Se, i as X, M as Le, U as va, a as N, S as gt, R as pt, b as Mt, c as da, _ as O, A as it, d as sr, e as ee, p as lr, f as ha, L as ur, g as ae, h as cr, j as vr, k as dr, l as fa, m as ga, n as Bt, o as pa, q as ma, s as Yt, t as F, C as kt, u as re, v as hr, w as ya, x as fr, y as ne, z as nt, B as at, D as ie, E as G, F as T, G as Rt, H as rt, I as Y, J as xa, K as oe, N as gr, O as pr, P as se, Q as Ce, T as Aa, V as mt, W as mr, X as yr, Y as _t, Z as dt, $ as Vt, a0 as ba, a1 as xr, a2 as le, a3 as Nt, a4 as Sa, a5 as Gt, a6 as Ar, a7 as br, a8 as Sr, a9 as La, aa as Ca, ab as Lr, ac as Cr, ad as Mr, ae as _r, af as ht, ag as ue, ah as Ma, ai as U, aj as et, ak as wr, al as Tr, am as _a, an as Pr, ao as Me, ap as Dr, aq as _e, ar as we, as as Ir, at as Xt, au as Or, av as Te, aw as Er, ax as Br, ay as kr, az as Pe, aA as Rr, aB as Nr, aC as Gr, aD as Wr, aE as Vr, aF as De, aG as Fr, aH as $t, aI as wa, aJ as Ta, aK as ce, aL as Lt, aM as Pa, aN as Da, aO as Ia, aP as zr, aQ as Ur, aR as Hr, aS as ve, aT as wt, aU as ft, aV as Yr, aW as Xr, aX as $r, aY as Kr, aZ as Oa, a_ as Zr, a$ as Ea, b0 as ut, b1 as Tt, b2 as Ie, b3 as Oe, b4 as qr, b5 as jr, b6 as Qr, b7 as Jr, b8 as de, b9 as Kt, ba as tn, bb as Pt, bc as en, bd as an, be as rn, bf as nn, bg as on, bh as Ba, bi as ka, bj as Wt, bk as ct, bl as Ee, bm as Ra, bn as Be, bo as sn, bp as ln, bq as he, br as Na, bs as un, bt as Ft, bu as Ga, bv as k, bw as Dt, bx as Wa, by as cn, bz as vn, bA as dn, bB as hn, bC as ke, bD as fn, bE as gn, bF as pn, bG as mn, bH as yn, bI as It, bJ as xn, bK as An, bL as bn, bM as J, bN as Sn, bO as Ln, bP as Cn, bQ as Mn, bR as _n, bS as wn, bT as Re, bU as Tn, bV as Va, bW as Pn, bX as Dn, bY as In, bZ as On, b_ as En, b$ as Bn, c0 as kn, c1 as Rn, c2 as Nn, c3 as Gn, c4 as Wn } from "./installCanvasRenderer-ui7Bv_08.js";
function Vn(a, t) {
  var e = a.mapDimensionsAll("defaultedLabel"), r = e.length;
  if (r === 1) {
    var n = Se(a, t, e[0]);
    return n != null ? n + "" : null;
  } else if (r) {
    for (var i = [], o = 0; o < e.length; o++)
      i.push(Se(a, t, e[o]));
    return i.join(" ");
  }
}
function Fn(a, t) {
  var e = a.mapDimensionsAll("defaultedLabel");
  if (!X(t))
    return t + "";
  for (var r = [], n = 0; n < e.length; n++) {
    var i = a.getDimensionIndex(e[n]);
    i >= 0 && r.push(t[i]);
  }
  return r.join(" ");
}
var zn = typeof Float32Array !== va ? Float32Array : void 0, Un = typeof Float64Array !== va ? Float64Array : void 0;
function zt(a) {
  return fe({
    ctor: zn
  }, a).arr;
}
function fe(a, t) {
  var e = a.arr, r = a.ctor;
  if (t > Le && (t = Le), !e || a.typed && e.length < t) {
    var n = void 0;
    if (r)
      try {
        n = new r(t), a.typed = !0, e && n.set(e);
      } catch {
      }
    if (!n && (n = [], a.typed = !1, e))
      for (var i = 0, o = e.length; i < o; i++)
        n[i] = e[i];
    a.arr = n;
  }
  return a;
}
function Hn(a, t, e, r, n) {
  var i = a.getArea(), o = i.x, s = i.y, l = i.width, c = i.height, u = e.get(["lineStyle", "width"]) || 0;
  o -= u / 2, s -= u / 2, l += u, c += u, l = Math.ceil(l), o !== Math.floor(o) && (o = Math.floor(o), l++);
  var v = new pt({
    shape: {
      x: o,
      y: s,
      width: l,
      height: c
    }
  });
  return v;
}
function Yn(a, t, e) {
  var r = a.getArea(), n = N(r.r0, 1), i = N(r.r, 1), o = new gt({
    shape: {
      cx: N(a.cx, 1),
      cy: N(a.cy, 1),
      r0: n,
      r: i,
      startAngle: r.startAngle,
      endAngle: r.endAngle,
      clockwise: r.clockwise
    }
  });
  return o;
}
function Fa(a, t, e, r, n) {
  if (a) {
    if (a.type === "polar")
      return Yn(a);
    if (a.type === "cartesian2d")
      return Hn(a, t, e);
  } else return null;
  return null;
}
var Zt = 0, qt = 1, jt = 2;
function Ne(a, t, e) {
  a ? t.setClipPath(e) : t.removeClipPath();
}
function Xn(a, t) {
  return a.type === t;
}
var $n = {
  average: function(a) {
    for (var t = 0, e = 0, r = 0; r < a.length; r++)
      isNaN(a[r]) || (t += a[r], e++);
    return e === 0 ? NaN : t / e;
  },
  sum: function(a) {
    for (var t = 0, e = 0; e < a.length; e++)
      t += a[e] || 0;
    return t;
  },
  max: function(a) {
    for (var t = -1 / 0, e = 0; e < a.length; e++)
      a[e] > t && (t = a[e]);
    return isFinite(t) ? t : NaN;
  },
  min: function(a) {
    for (var t = 1 / 0, e = 0; e < a.length; e++)
      a[e] < t && (t = a[e]);
    return isFinite(t) ? t : NaN;
  },
  // TODO
  // Median
  nearest: function(a) {
    return a[0];
  }
}, Kn = function(a) {
  return Math.round(a.length / 2);
};
function Zn(a) {
  return {
    seriesType: a,
    // FIXME:TS never used, so comment it
    // modifyOutputEnd: true,
    reset: function(t, e, r) {
      var n = t.getData(), i = t.get("sampling"), o = t.coordinateSystem, s = n.count();
      if (s > 10 && o.type === "cartesian2d" && i) {
        var l = o.getBaseAxis(), c = o.getOtherAxis(l), u = l.getExtent(), v = r.getDevicePixelRatio(), d = Math.abs(u[1] - u[0]) * (v || 1), h = Math.round(s / d);
        if (isFinite(h) && h > 1) {
          i === "lttb" ? t.setData(n.lttbDownSample(n.mapDimension(c.dim), 1 / h)) : i === "minmax" && t.setData(n.minmaxDownSample(n.mapDimension(c.dim), 1 / h));
          var f = void 0;
          Mt(i) ? f = $n[i] : da(i) && (f = i), f && t.setData(n.downSample(n.mapDimension(c.dim), 1 / h, f, Kn));
        }
      }
    }
  };
}
var qn = (
  /** @class */
  (function(a) {
    O(t, a);
    function t(e, r, n, i, o) {
      var s = a.call(this, e, r, n) || this;
      return s.index = 0, s.type = i || "value", s.position = o || "bottom", s;
    }
    return t.prototype.isHorizontal = function() {
      var e = this.position;
      return e === "top" || e === "bottom";
    }, t.prototype.getGlobalExtent = function(e) {
      var r = this.getExtent();
      return r[0] = this.toGlobalCoord(r[0]), r[1] = this.toGlobalCoord(r[1]), e && r[0] > r[1] && r.reverse(), r;
    }, t.prototype.pointToData = function(e, r) {
      return this.coordToData(this.toLocalCoord(e[this.dim === "x" ? 0 : 1]), r);
    }, t.prototype.setCategorySortInfo = function(e) {
      if (this.type !== "category")
        return !1;
      this.model.option.categorySortInfo = e, this.scale.setSortInfo(e);
    }, t;
  })(it)
);
function jn() {
  sr("liPosMinGap", Qn);
}
function Qn(a, t, e) {
  var r = ee(), n = e.serUids, i = e.liPosMinGap, o, s = t.axis, l = s.scale, c = l.needTransform(), u = l.getFilter ? l.getFilter() : null, v = lr(u);
  function d(x) {
    cr(a, t.sers, function(b) {
      var S = b.getRawData(), L = S.getDimensionIndex(S.mapDimension(s.dim));
      L >= 0 && x(L, b, S.getStore());
    });
  }
  var h = 0;
  if (d(function(x, b, S) {
    r.set(b.uid, 1), (!n || !n.hasKey(b.uid)) && (o = !0), h += S.count();
  }), (!n || n.keys().length !== r.keys().length) && (o = !0), !o && i != null) {
    t.liPosMinGap = i;
    return;
  }
  fe(tt, h);
  var f = 0;
  d(function(x, b, S) {
    for (var L = 0, A = S.count(); L < A; ++L) {
      var M = S.get(x, L);
      isFinite(M) && (!u || vr(v, M)) && (c && (M = l.transformIn(M, null)), tt.arr[f++] = M);
    }
  });
  var g = tt.typed ? tt.arr.subarray(0, f) : (tt.arr.length = f, tt.arr);
  tt.typed ? g.sort() : ha(g);
  for (var p = 1 / 0, y = 1; y < f; ++y) {
    var m = g[y] - g[y - 1];
    // - Different series normally have the same values (e.g., barA, barB, barC),
    //   which should be ignored.
    // - A single series with multiple same values is often not meaningful to
    //   create `bandWidth`, so it is also ignored.
    m > 0 && m < p && (p = m);
  }
  e.liPosMinGap = t.liPosMinGap = ae(p) ? p : f > 0 ? dr : ur, e.serUids = r;
}
var tt = fe(
  {
    ctor: Un
  },
  50
  // An arbitrary initial capability.
);
function ge(a) {
  return function(t, e) {
    var r = Bt(t, {
      fromStat: {
        key: a
      }
    });
    if (ae(r.w2))
      return [-r.w2 / 2, r.w2 / 2];
  };
}
function vt(a) {
  return a + fa;
}
function ot(a, t) {
  return a + fa + t;
}
function za(a) {
  return jn(), {
    // non-category scale do not use `liPosMinGap` to calculate `bandWidth`.
    liPosMinGap: !ga(a.scale)
  };
}
var K = "bar";
function Ua(a, t, e, r) {
  pa(a, {
    key: t,
    seriesType: e,
    coordSysType: r,
    getMetrics: za
  });
}
function Ha(a) {
  var t = a.scale.rawExtentInfo.makeRenderInfo().startValue;
  return t;
}
var Ya = {
  left: 0,
  right: 0,
  top: 0,
  bottom: 0
}, Ot = ["25%", "25%"], j = "cartesian2d", Jn = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      return a !== null && a.apply(this, arguments) || this;
    }
    return t.prototype.mergeDefaultAndTheme = function(e, r) {
      var n = ma(e.outerBounds);
      a.prototype.mergeDefaultAndTheme.apply(this, arguments), n && e.outerBounds && Yt(e.outerBounds, n);
    }, t.prototype.mergeOption = function(e, r) {
      a.prototype.mergeOption.apply(this, arguments), this.option.outerBounds && e.outerBounds && Yt(this.option.outerBounds, e.outerBounds);
    }, t.type = "grid", t.dependencies = ["xAxis", "yAxis"], t.layoutMode = "box", t.defaultOption = {
      show: !1,
      // zlevel: 0,
      z: 0,
      left: "15%",
      top: 65,
      right: "10%",
      bottom: 80,
      // If grid size contain label
      containLabel: !1,
      outerBoundsMode: "auto",
      outerBounds: Ya,
      outerBoundsContain: "all",
      outerBoundsClampWidth: Ot[0],
      outerBoundsClampHeight: Ot[1],
      // width: {totalWidth} - left - right,
      // height: {totalHeight} - top - bottom,
      backgroundColor: F.color.transparent,
      borderWidth: 1,
      borderColor: F.color.neutral30
    }, t;
  })(kt)
), ti = ne(), ei = "__ec_stack_";
function Xa(a) {
  return a.get("stack") || ei + a.seriesIndex;
}
function ai(a, t) {
  var e = ri(a, t);
  return e.columnMap = ni(e), e;
}
function ri(a, t) {
  var e = ot(t, j), r = [], n = Bt(a, {
    fromStat: {
      key: e
    },
    min: 1
  });
  return nt(a, e, function(i) {
    r.push({
      barWidth: G(i.get("barWidth"), n.w),
      barMaxWidth: G(i.get("barMaxWidth"), n.w),
      barMinWidth: G(
        // barMinWidth by default is 0.5 / 1 in cartesian. Because in value axis,
        // the auto-calculated bar width might be less than 0.5 / 1.
        i.get("barMinWidth") || ($a(i) ? 0.5 : 1),
        n.w
      ),
      barGap: i.get("barGap"),
      barCategoryGap: i.get("barCategoryGap"),
      defaultBarGap: i.get("defaultBarGap"),
      stackId: Xa(i)
    });
  }), {
    bandWidthResult: n,
    seriesInfo: r
  };
}
function ni(a) {
  var t = a.bandWidthResult.w, e = t, r = 0, n, i, o = [], s = {};
  T(a.seriesInfo, function(g, p) {
    p || (i = g.defaultBarGap || 0);
    var y = g.stackId;
    Rt(s, y) || r++;
    var m = s[y];
    m || (m = s[y] = {
      width: 0,
      maxWidth: 0
    }, o.push(y));
    var x = g.barWidth;
    x && !m.width && (m.width = x, x = rt(e, x), e -= x);
    var b = g.barMaxWidth;
    b && (m.maxWidth = b);
    var S = g.barMinWidth;
    S && (m.minWidth = S);
    var L = g.barGap;
    L != null && (i = L);
    var A = g.barCategoryGap;
    A != null && (n = A);
  }), n == null && (n = Y(35 - o.length * 4, 15) + "%");
  var l = G(n, t), c = G(i, 1), u = (e - l) / (r + (r - 1) * c);
  u = Y(u, 0), T(o, function(g) {
    var p = s[g], y = p.maxWidth, m = p.minWidth;
    if (p.width) {
      var x = p.width;
      y && (x = rt(x, y)), m && (x = Y(x, m)), p.width = x, e -= x + c * x, r--;
    } else {
      var x = u;
      y && y < x && (x = rt(y, e)), m && m > x && (x = m), x !== u && (p.width = x, e -= x + c * x, r--);
    }
  }), u = (e - l) / (r + (r - 1) * c), u = Y(u, 0);
  var v = 0, d;
  T(o, function(g) {
    var p = s[g];
    p.width || (p.width = u), d = p, v += p.width * (1 + c);
  }), d && (v -= d.width * c);
  var h = {}, f = -v / 2;
  return T(o, function(g) {
    var p = s[g];
    h[g] = h[g] || {
      bandWidth: t,
      offset: f,
      width: p.width
    }, f += p.width * (1 + c);
  }), h;
}
function ii(a) {
  return {
    seriesType: a,
    overallReset: function(t) {
      var e = ot(a, j);
      re(t, e, function(r) {
        var n = ai(r, a);
        nt(r, e, function(i) {
          var o = n.columnMap[Xa(i)];
          i.getData().setLayout({
            bandWidth: o.bandWidth,
            offset: o.offset,
            size: o.width
          });
        });
      });
    }
  };
}
function oi(a) {
  return {
    seriesType: a,
    plan: fr(),
    reset: function(t) {
      if (hr(t)) {
        var e = t.getData(), r = t.coordinateSystem, n = r.getBaseAxis(), i = r.getOtherAxis(n), o = e.getDimensionIndex(e.mapDimension(i.dim)), s = e.getDimensionIndex(e.mapDimension(n.dim)), l = t.get("showBackground", !0), c = e.mapDimension(i.dim), u = e.getCalculationInfo("stackResultDimension"), v = ya(e, c) && !!e.getCalculationInfo("stackedOnSeries"), d = i.isHorizontal(), h = i.toGlobalCoord(i.dataToCoord(Ha(i))), f = $a(t), g = t.get("barMinHeight") || 0, p = u && e.getDimensionIndex(u), y = e.getLayout("size"), m = e.getLayout("offset");
        return {
          progress: function(x, b) {
            for (var S = x.count, L = f && zt(S * 3), A = f && l && zt(S * 3), M = f && zt(S), P = r.master.getRect(), _ = d ? P.width : P.height, C, D = b.getStore(), w = 0; (C = x.next()) != null; ) {
              var B = D.get(v ? p : o, C), E = D.get(s, C), R = h, $ = void 0;
              v && ($ = +B - D.get(o, C));
              var I = void 0, z = void 0, H = void 0, V = void 0;
              if (d) {
                var W = r.dataToPoint([B, E]);
                v && (R = r.dataToPoint([$, E])[0]), I = R, z = W[1] + m, H = W[0] - R, V = y, at(H) < g && (H = (H < 0 ? -1 : 1) * g);
              } else {
                var W = r.dataToPoint([E, B]);
                v && (R = r.dataToPoint([E, $])[1]), I = W[0] + m, z = R, H = y, V = W[1] - R, at(V) < g && (V = (V <= 0 ? -1 : 1) * g);
              }
              f ? (L[w] = I, L[w + 1] = z, L[w + 2] = d ? H : V, A && (A[w] = d ? P.x : I, A[w + 1] = d ? z : P.y, A[w + 2] = _), M[C] = C) : b.setItemLayout(C, {
                x: I,
                y: z,
                width: H,
                height: V
              }), w += 3;
            }
            f && b.setLayout({
              largePoints: L,
              largeDataIndices: M,
              largeBackgroundPoints: A,
              valueAxisHorizontal: d
            });
          }
        };
      }
    }
  };
}
function $a(a) {
  return a.pipelineContext && a.pipelineContext.large;
}
function si(a) {
  return ge(ot(a, j));
}
function li(a) {
  ti(a, function() {
    function t(e) {
      var r = ot(e, j);
      Ua(a, r, e, j), ie(r, si(e));
    }
    t("bar"), t("pictorialBar");
  });
}
var Qt = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.prototype.getInitialData = function(e, r) {
      return xa(null, this, {
        useEncodeDefaulter: !0
      });
    }, t.prototype.getMarkerPosition = function(e, r, n) {
      var i = this.coordinateSystem;
      if (i && i.clampData) {
        var o = i.clampData(e), s = i.dataToPoint(o);
        if (n)
          T(i.getAxes(), function(d, h) {
            if (d.type === "category" && r != null) {
              var f = d.getTicksCoords(), g = d.getTickModel().get("alignWithLabel"), p = o[h], y = r[h] === "x1" || r[h] === "y1";
              if (y && !g && (p += 1), f.length < 2)
                return;
              if (f.length === 2) {
                s[h] = d.toGlobalCoord(d.getExtent()[y ? 1 : 0]);
                return;
              }
              for (var m = void 0, x = void 0, b = 1, S = 0; S < f.length; S++) {
                var L = f[S].coord, A = S === f.length - 1 ? f[S - 1].tickValue + b : f[S].tickValue;
                if (A === p) {
                  x = L;
                  break;
                } else if (A < p)
                  m = L;
                else if (m != null && A > p) {
                  x = (L + m) / 2;
                  break;
                }
                S === 1 && (b = A - f[0].tickValue);
              }
              x == null && (m ? m && (x = f[f.length - 1].coord) : x = f[0].coord), s[h] = d.toGlobalCoord(x);
            }
          });
        else {
          var l = this.getData(), c = l.getLayout("offset"), u = l.getLayout("size"), v = i.getBaseAxis().isHorizontal() ? 0 : 1;
          s[v] += c + u / 2;
        }
        return s;
      }
      return [NaN, NaN];
    }, t.prototype.__requireStartValue = function(e) {
      return this.getBaseAxis() !== e;
    }, t.type = "series.__base_bar__", t.defaultOption = {
      // zlevel: 0,
      z: 2,
      coordinateSystem: "cartesian2d",
      legendHoverLink: !0,
      // stack: null
      // Cartesian coordinate system
      // xAxisIndex: 0,
      // yAxisIndex: 0,
      barMinHeight: 0,
      barMinAngle: 0,
      // cursor: null,
      large: !1,
      largeThreshold: 400,
      progressive: 3e3,
      progressiveChunkMode: "mod",
      defaultBarGap: "10%"
    }, t;
  })(oe)
);
oe.registerClass(Qt);
var ui = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.prototype.getInitialData = function() {
      return xa(null, this, {
        useEncodeDefaulter: !0,
        createInvertedIndices: !!this.get("realtimeSort", !0) || null
      });
    }, t.prototype.getProgressive = function() {
      return this.get("large") ? this.get("progressive") : !1;
    }, t.prototype.__preparePipelineContext = function(e, r) {
      var n = gr(this, e, r);
      return n.progressiveRender && (n.large = !0), n;
    }, t.prototype.brushSelector = function(e, r, n) {
      return n.rect(r.getItemLayout(e));
    }, t.type = "series." + K, t.dependencies = ["grid", "polar"], t.defaultOption = pr(Qt.defaultOption, {
      // If clipped
      // Only available on cartesian2d
      clip: !0,
      roundCap: !1,
      showBackground: !1,
      backgroundStyle: {
        color: "rgba(180, 180, 180, 0.2)",
        borderColor: null,
        borderWidth: 0,
        borderType: "solid",
        borderRadius: 0,
        shadowBlur: 0,
        shadowColor: null,
        shadowOffsetX: 0,
        shadowOffsetY: 0,
        opacity: 1
      },
      select: {
        itemStyle: {
          borderColor: F.color.primary,
          borderWidth: 2
        }
      },
      realtimeSort: !1
    }), t;
  })(Qt)
), ci = (
  /** @class */
  /* @__PURE__ */ (function() {
    function a() {
      this.cx = 0, this.cy = 0, this.r0 = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0;
    }
    return a;
  })()
), Ge = (
  /** @class */
  (function(a) {
    O(t, a);
    function t(e) {
      var r = a.call(this, e) || this;
      return r.type = "sausage", r;
    }
    return t.prototype.getDefaultShape = function() {
      return new ci();
    }, t.prototype.buildPath = function(e, r) {
      var n = r.cx, i = r.cy, o = Math.max(r.r0 || 0, 0), s = Math.max(r.r, 0), l = (s - o) * 0.5, c = o + l, u = r.startAngle, v = r.endAngle, d = r.clockwise, h = Math.PI * 2, f = d ? v - u < h : u - v < h;
      f || (u = v - (d ? h : -h));
      var g = Math.cos(u), p = Math.sin(u), y = Math.cos(v), m = Math.sin(v);
      f ? (e.moveTo(g * o + n, p * o + i), e.arc(g * c + n, p * c + i, l, -Math.PI + u, u, !d)) : e.moveTo(g * s + n, p * s + i), e.arc(n, i, s, u, v, !d), e.arc(y * c + n, m * c + i, l, v - Math.PI * 2, v - Math.PI, !d), o !== 0 && e.arc(n, i, o, v, u, d);
    }, t;
  })(se)
);
function vi(a, t) {
  t = t || {};
  var e = t.isRoundCap;
  return function(r, n, i) {
    var o = n.position;
    if (!o || o instanceof Array)
      return Ce(r, n, i);
    var s = a(o), l = n.distance != null ? n.distance : 5, c = this.shape, u = c.cx, v = c.cy, d = c.r, h = c.r0, f = (d + h) / 2, g = c.startAngle, p = c.endAngle, y = (g + p) / 2, m = e ? Math.abs(d - h) / 2 : 0, x = Math.cos, b = Math.sin, S = u + d * x(g), L = v + d * b(g), A = "left", M = "top";
    switch (s) {
      case "startArc":
        S = u + (h - l) * x(y), L = v + (h - l) * b(y), A = "center", M = "top";
        break;
      case "insideStartArc":
        S = u + (h + l) * x(y), L = v + (h + l) * b(y), A = "center", M = "bottom";
        break;
      case "startAngle":
        S = u + f * x(g) + yt(g, l + m, !1), L = v + f * b(g) + xt(g, l + m, !1), A = "right", M = "middle";
        break;
      case "insideStartAngle":
        S = u + f * x(g) + yt(g, -l + m, !1), L = v + f * b(g) + xt(g, -l + m, !1), A = "left", M = "middle";
        break;
      case "middle":
        S = u + f * x(y), L = v + f * b(y), A = "center", M = "middle";
        break;
      case "endArc":
        S = u + (d + l) * x(y), L = v + (d + l) * b(y), A = "center", M = "bottom";
        break;
      case "insideEndArc":
        S = u + (d - l) * x(y), L = v + (d - l) * b(y), A = "center", M = "top";
        break;
      case "endAngle":
        S = u + f * x(p) + yt(p, l + m, !0), L = v + f * b(p) + xt(p, l + m, !0), A = "left", M = "middle";
        break;
      case "insideEndAngle":
        S = u + f * x(p) + yt(p, -l + m, !0), L = v + f * b(p) + xt(p, -l + m, !0), A = "right", M = "middle";
        break;
      default:
        return Ce(r, n, i);
    }
    return r = r || {}, r.x = S, r.y = L, r.align = A, r.verticalAlign = M, r;
  };
}
function di(a, t, e, r) {
  if (Aa(r)) {
    a.setTextConfig({
      rotation: r
    });
    return;
  } else if (X(t)) {
    a.setTextConfig({
      rotation: 0
    });
    return;
  }
  var n = a.shape, i = n.clockwise ? n.startAngle : n.endAngle, o = n.clockwise ? n.endAngle : n.startAngle, s = (i + o) / 2, l, c = e(t);
  switch (c) {
    case "startArc":
    case "insideStartArc":
    case "middle":
    case "insideEndArc":
    case "endArc":
      l = s;
      break;
    case "startAngle":
    case "insideStartAngle":
      l = i;
      break;
    case "endAngle":
    case "insideEndAngle":
      l = o;
      break;
    default:
      a.setTextConfig({
        rotation: 0
      });
      return;
  }
  var u = Math.PI * 1.5 - l;
  c === "middle" && u > Math.PI / 2 && u < Math.PI * 1.5 && (u -= Math.PI), a.setTextConfig({
    rotation: u
  });
}
function yt(a, t, e) {
  return t * Math.sin(a) * (e ? -1 : 1);
}
function xt(a, t, e) {
  return t * Math.cos(a) * (e ? 1 : -1);
}
function hi(a, t, e) {
  var r = a.get("borderRadius");
  if (r == null)
    return {
      cornerRadius: 0
    };
  X(r) || (r = [r, r, r, r]);
  var n = Math.abs(t.r || 0 - t.r0 || 0);
  return {
    cornerRadius: mt(r, function(i) {
      return mr(i, n);
    })
  };
}
var Ut = Math.max, Ht = Math.min, fi = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a.call(this) || this;
      return e.type = K, e._isFirstFrame = !0, e;
    }
    return t.prototype.render = function(e, r, n, i) {
      this._model = e, this._removeOnRenderedListener(n), this._updateDrawMode(e);
      var o = e.get("coordinateSystem");
      (o === "cartesian2d" || o === "polar") && (this._progressiveEls = null, this._isLargeDraw ? this._renderLarge(e, r, n) : this._renderNormal(e, r, n, i));
    }, t.prototype.incrementalPrepareRender = function(e) {
      this._clear(), this._updateDrawMode(e), this._updateLargeClip(e);
    }, t.prototype.incrementalRender = function(e, r) {
      this._progressiveEls = [], this._incrementalRenderLarge(e, r);
    }, t.prototype.eachRendered = function(e) {
      yr(this._progressiveEls || this.group, e);
    }, t.prototype._updateDrawMode = function(e) {
      var r = e.pipelineContext.large;
      (this._isLargeDraw == null || r !== this._isLargeDraw) && (this._isLargeDraw = r, this._clear());
    }, t.prototype._renderNormal = function(e, r, n, i) {
      var o = this.group, s = e.getData(), l = this._data, c = e.coordinateSystem, u = c.getBaseAxis(), v;
      c.type === "cartesian2d" ? v = u.isHorizontal() : c.type === "polar" && (v = u.dim === "angle");
      var d = e.isAnimationEnabled() ? e : null, h = gi(e, c);
      h && this._enableRealtimeSort(h, s, n);
      var f = e.get("clip", !0) || h, g = c.getArea();
      o.removeClipPath();
      var p = e.get("roundCap", !0), y = e.get("showBackground", !0), m = e.getModel("backgroundStyle"), x = m.get("borderRadius") || 0, b = [], S = this._backgroundEls, L = i && i.isInitSort, A = i && i.type === "changeAxisOrder";
      function M(C) {
        var D = At[c.type](s, C);
        if (!D)
          return null;
        var w = Si(c, v, D);
        return w.useStyle(m.getItemStyle()), c.type === "cartesian2d" ? w.setShape("r", x) : w.setShape("cornerRadius", x), b[C] = w, w;
      }
      s.diff(l).add(function(C) {
        var D = s.getItemModel(C), w = At[c.type](s, C, D);
        if (w && (y && M(C), !(!s.hasValue(C) || !Ue[c.type](w)))) {
          var B = !1;
          f && (B = We[c.type](g, w));
          var E = Ve[c.type](e, s, C, w, v, d, u.model, !1, p);
          h && (E.forceLabelAnimation = !0), He(E, s, C, D, w, e, v, c.type === "polar"), L ? E.attr({
            shape: w
          }) : h ? Fe(h, d, E, w, C, v, !1, !1) : _t(E, {
            shape: w
          }, e, C), s.setItemGraphicEl(C, E), o.add(E), E.ignore = B;
        }
      }).update(function(C, D) {
        var w = s.getItemModel(C), B = At[c.type](s, C, w);
        if (B) {
          if (y) {
            var E = void 0;
            S.length === 0 ? E = M(D) : (E = S[D], E.useStyle(m.getItemStyle()), c.type === "cartesian2d" ? E.setShape("r", x) : E.setShape("cornerRadius", x), b[C] = E);
            var R = At[c.type](s, C), $ = Za(v, R, c);
            dt(E, {
              shape: $
            }, d, C);
          }
          var I = l.getItemGraphicEl(D);
          if (!s.hasValue(C) || !Ue[c.type](B)) {
            o.remove(I);
            return;
          }
          var z = !1;
          f && (z = We[c.type](g, B), z && o.remove(I));
          var H = I && (I.type === "sector" && p || I.type === "sausage" && !p);
          if (H && (I && Vt(I, e, D), I = null), I ? ba(I) : I = Ve[c.type](e, s, C, B, v, d, u.model, !0, p), h && (I.forceLabelAnimation = !0), A) {
            var V = I.getTextContent();
            if (V) {
              var W = xr(V);
              W.prevValue != null && (W.prevValue = W.value);
            }
          } else
            He(I, s, C, w, B, e, v, c.type === "polar");
          L ? I.attr({
            shape: B
          }) : h ? Fe(h, d, I, B, C, v, !0, A) : dt(I, {
            shape: B
          }, e, C, null), s.setItemGraphicEl(C, I), I.ignore = z, o.add(I);
        }
      }).remove(function(C) {
        var D = l.getItemGraphicEl(C);
        D && Vt(D, e, C);
      }).execute();
      var P = this._backgroundGroup || (this._backgroundGroup = new le());
      P.removeAll();
      for (var _ = 0; _ < b.length; ++_)
        P.add(b[_]);
      o.add(P), this._backgroundEls = b, this._data = s;
    }, t.prototype._renderLarge = function(e, r, n) {
      this._clear(), Xe(e, this.group), this._updateLargeClip(e);
    }, t.prototype._incrementalRenderLarge = function(e, r) {
      this._removeBackground(), Xe(r, this.group, this._progressiveEls, !0);
    }, t.prototype._updateLargeClip = function(e) {
      var r = e.get("clip", !0) && Fa(e.coordinateSystem, !1, e), n = this.group;
      r ? n.setClipPath(r) : n.removeClipPath();
    }, t.prototype._enableRealtimeSort = function(e, r, n) {
      var i = this;
      if (r.count()) {
        var o = e.baseAxis;
        if (this._isFirstFrame)
          this._dispatchInitSort(r, e, n), this._isFirstFrame = !1;
        else {
          var s = function(l) {
            var c = r.getItemGraphicEl(l), u = c && c.shape;
            return u && // The result should be consistent with the initial sort by data value.
            // Do not support the case that both positive and negative exist.
            Math.abs(o.isHorizontal() ? u.height : u.width) || 0;
          };
          this._onRendered = function() {
            i._updateSortWithinSameData(r, s, o, n);
          }, n.getZr().on("rendered", this._onRendered);
        }
      }
    }, t.prototype._dataSort = function(e, r, n) {
      var i = [];
      return e.each(e.mapDimension(r.dim), function(o, s) {
        var l = n(s);
        l = l ?? NaN, i.push({
          dataIndex: s,
          mappedValue: l,
          ordinalNumber: o
        });
      }), i.sort(function(o, s) {
        return s.mappedValue - o.mappedValue;
      }), {
        ordinalNumbers: mt(i, function(o) {
          return o.ordinalNumber;
        })
      };
    }, t.prototype._isOrderChangedWithinSameData = function(e, r, n) {
      for (var i = n.scale, o = e.mapDimension(n.dim), s = Number.MAX_VALUE, l = 0, c = i.getOrdinalMeta().categories.length; l < c; ++l) {
        var u = e.rawIndexOf(o, i.getRawOrdinalNumber(l)), v = u < 0 ? Number.MIN_VALUE : r(e.indexOfRawIndex(u));
        if (v > s)
          return !0;
        s = v;
      }
      return !1;
    }, t.prototype._isOrderDifferentInView = function(e, r) {
      for (var n = r.scale, i = n.getExtent(), o = Math.max(0, i[0]), s = Math.min(i[1], n.getOrdinalMeta().categories.length - 1); o <= s; ++o)
        if (e.ordinalNumbers[o] !== n.getRawOrdinalNumber(o))
          return !0;
    }, t.prototype._updateSortWithinSameData = function(e, r, n, i) {
      if (this._isOrderChangedWithinSameData(e, r, n)) {
        var o = this._dataSort(e, n, r);
        this._isOrderDifferentInView(o, n) && (this._removeOnRenderedListener(i), i.dispatchAction({
          type: "changeAxisOrder",
          componentType: n.dim + "Axis",
          axisId: n.index,
          sortInfo: o
        }));
      }
    }, t.prototype._dispatchInitSort = function(e, r, n) {
      var i = r.baseAxis, o = this._dataSort(e, i, function(s) {
        return e.get(e.mapDimension(r.otherAxis.dim), s);
      });
      n.dispatchAction({
        type: "changeAxisOrder",
        componentType: i.dim + "Axis",
        isInitSort: !0,
        axisId: i.index,
        sortInfo: o
      });
    }, t.prototype.remove = function(e, r) {
      this._clear(this._model), this._removeOnRenderedListener(r);
    }, t.prototype.dispose = function(e, r) {
      this._removeOnRenderedListener(r);
    }, t.prototype._removeOnRenderedListener = function(e) {
      this._onRendered && (e.getZr().off("rendered", this._onRendered), this._onRendered = null);
    }, t.prototype._clear = function(e) {
      var r = this.group, n = this._data;
      e && e.isAnimationEnabled() && n && !this._isLargeDraw ? (this._removeBackground(), this._backgroundEls = [], n.eachItemGraphicEl(function(i) {
        Vt(i, e, Nt(i).dataIndex);
      })) : r.removeAll(), this._data = null, this._isFirstFrame = !0;
    }, t.prototype._removeBackground = function() {
      this.group.remove(this._backgroundGroup), this._backgroundGroup = null;
    }, t.type = K, t;
  })(Sa)
), We = {
  cartesian2d: function(a, t) {
    var e = t.width < 0 ? -1 : 1, r = t.height < 0 ? -1 : 1;
    e < 0 && (t.x += t.width, t.width = -t.width), r < 0 && (t.y += t.height, t.height = -t.height);
    var n = a.x + a.width, i = a.y + a.height, o = Ut(t.x, a.x), s = Ht(t.x + t.width, n), l = Ut(t.y, a.y), c = Ht(t.y + t.height, i), u = s < o, v = c < l;
    return t.x = u && o > n ? s : o, t.y = v && l > i ? c : l, t.width = u ? 0 : s - o, t.height = v ? 0 : c - l, e < 0 && (t.x += t.width, t.width = -t.width), r < 0 && (t.y += t.height, t.height = -t.height), u || v;
  },
  polar: function(a, t) {
    var e = t.r0 <= t.r ? 1 : -1;
    if (e < 0) {
      var r = t.r;
      t.r = t.r0, t.r0 = r;
    }
    var n = Ht(t.r, a.r), i = Ut(t.r0, a.r0);
    t.r = n, t.r0 = i;
    var o = n - i < 0;
    if (e < 0) {
      var r = t.r;
      t.r = t.r0, t.r0 = r;
    }
    return o;
  }
}, Ve = {
  cartesian2d: function(a, t, e, r, n, i, o, s, l) {
    var c = new pt({
      shape: Gt({}, r),
      z2: 1
    });
    if (c.__dataIndex = e, c.name = "item", i) {
      var u = c.shape, v = n ? "height" : "width";
      u[v] = 0;
    }
    return c;
  },
  polar: function(a, t, e, r, n, i, o, s, l) {
    var c = !n && l ? Ge : gt, u = new c({
      shape: r,
      z2: 1
    });
    u.name = "item";
    var v = Ka(n);
    if (u.calculateTextPosition = vi(v, {
      isRoundCap: c === Ge
    }), i) {
      var d = u.shape, h = n ? "r" : "endAngle", f = {};
      d[h] = n ? r.r0 : r.startAngle, f[h] = r[h], (s ? dt : _t)(u, {
        shape: f
        // __value: typeof dataValue === 'string' ? parseInt(dataValue, 10) : dataValue
      }, i);
    }
    return u;
  }
};
function gi(a, t) {
  var e = a.get("realtimeSort", !0), r = t.getBaseAxis();
  if (e && r.type === "category" && t.type === "cartesian2d")
    return {
      baseAxis: r,
      otherAxis: t.getOtherAxis(r)
    };
}
function Fe(a, t, e, r, n, i, o, s) {
  var l, c;
  i ? (c = {
    x: r.x,
    width: r.width
  }, l = {
    y: r.y,
    height: r.height
  }) : (c = {
    y: r.y,
    height: r.height
  }, l = {
    x: r.x,
    width: r.width
  }), s || (o ? dt : _t)(e, {
    shape: l
  }, t, n, null);
  var u = t ? a.baseAxis.model : null;
  (o ? dt : _t)(e, {
    shape: c
  }, u, n);
}
function ze(a, t) {
  for (var e = 0; e < t.length; e++)
    if (!isFinite(a[t[e]]))
      return !0;
  return !1;
}
var pi = ["x", "y", "width", "height"], mi = ["cx", "cy", "r", "startAngle", "endAngle"], Ue = {
  cartesian2d: function(a) {
    return !ze(a, pi);
  },
  polar: function(a) {
    return !ze(a, mi);
  }
}, At = {
  // itemModel is only used to get borderWidth, which is not needed
  // when calculating bar background layout.
  cartesian2d: function(a, t, e) {
    var r = a.getItemLayout(t);
    if (!r)
      return null;
    var n = e ? xi(e, r) : 0, i = r.width > 0 ? 1 : -1, o = r.height > 0 ? 1 : -1;
    return {
      x: r.x + i * n / 2,
      y: r.y + o * n / 2,
      width: r.width - i * n,
      height: r.height - o * n
    };
  },
  polar: function(a, t, e) {
    var r = a.getItemLayout(t);
    return {
      cx: r.cx,
      cy: r.cy,
      r0: r.r0,
      r: r.r,
      startAngle: r.startAngle,
      endAngle: r.endAngle,
      clockwise: r.clockwise
    };
  }
};
function yi(a) {
  return a.startAngle != null && a.endAngle != null && a.startAngle === a.endAngle;
}
function Ka(a) {
  return /* @__PURE__ */ (function(t) {
    var e = t ? "Arc" : "Angle";
    return function(r) {
      switch (r) {
        case "start":
        case "insideStart":
        case "end":
        case "insideEnd":
          return r + e;
        default:
          return r;
      }
    };
  })(a);
}
function He(a, t, e, r, n, i, o, s) {
  var l = t.getItemVisual(e, "style");
  if (s) {
    if (!i.get("roundCap")) {
      var u = a.shape, v = hi(r.getModel("itemStyle"), u);
      Gt(u, v), a.setShape(u);
    }
  } else {
    var c = r.get(["itemStyle", "borderRadius"]) || 0;
    a.setShape("r", c);
  }
  a.useStyle(l);
  var d = r.getShallow("cursor");
  d && a.attr("cursor", d);
  var h = s ? o ? n.r >= n.r0 ? "endArc" : "startArc" : n.endAngle >= n.startAngle ? "endAngle" : "startAngle" : o ? Li(n, i.coordinateSystem) : Ci(n, i.coordinateSystem), f = Ar(r);
  br(a, f, {
    labelFetcher: i,
    labelDataIndex: e,
    defaultText: Vn(i.getData(), e),
    inheritColor: l.fill,
    defaultOpacity: l.opacity,
    defaultOutsidePosition: h
  });
  var g = a.getTextContent();
  if (s && g) {
    var p = r.get(["label", "position"]);
    a.textConfig.inside = p === "middle" ? !0 : null, di(a, p === "outside" ? h : p, Ka(o), r.get(["label", "rotate"]));
  }
  Sr(g, f, i.getRawValue(e), function(m) {
    return Fn(t, m);
  });
  var y = r.getModel(["emphasis"]);
  La(a, y.get("focus"), y.get("blurScope"), y.get("disabled")), Ca(a, r), yi(n) && (a.style.fill = "none", a.style.stroke = "none", T(a.states, function(m) {
    m.style && (m.style.fill = m.style.stroke = "none");
  }));
}
function xi(a, t) {
  var e = a.get(["itemStyle", "borderColor"]);
  if (!e || e === "none")
    return 0;
  var r = a.get(["itemStyle", "borderWidth"]) || 0, n = isNaN(t.width) ? Number.MAX_VALUE : Math.abs(t.width), i = isNaN(t.height) ? Number.MAX_VALUE : Math.abs(t.height);
  return Math.min(r, n, i);
}
var Ai = (
  /** @class */
  /* @__PURE__ */ (function() {
    function a() {
    }
    return a;
  })()
), Ye = (
  /** @class */
  (function(a) {
    O(t, a);
    function t(e) {
      var r = a.call(this, e) || this;
      return r.type = "largeBar", r;
    }
    return t.prototype.getDefaultShape = function() {
      return new Ai();
    }, t.prototype.buildPath = function(e, r) {
      for (var n = r.points, i = this.baseDimIdx, o = 1 - this.baseDimIdx, s = [], l = [], c = this.barWidth, u = 0; u < n.length; u += 3)
        l[i] = c, l[o] = n[u + 2], s[i] = n[u + i], s[o] = n[u + o], e.rect(s[0], s[1], l[0], l[1]);
    }, t;
  })(se)
);
function Xe(a, t, e, r) {
  var n = a.getData(), i = n.getLayout("valueAxisHorizontal") ? 1 : 0, o = n.getLayout("largeDataIndices"), s = n.getLayout("size"), l = a.getModel("backgroundStyle"), c = n.getLayout("largeBackgroundPoints"), u = r ? Lr(a) : 0;
  if (c) {
    var v = new Ye({
      shape: {
        points: c
      },
      incremental: u,
      silent: !0,
      z2: 0
    });
    v.baseDimIdx = i, v.largeDataIndices = o, v.barWidth = s, v.useStyle(l.getItemStyle()), t.add(v), e && e.push(v);
  }
  var d = new Ye({
    shape: {
      points: n.getLayout("largePoints")
    },
    incremental: u,
    ignoreCoarsePointer: !0,
    z2: 1
  });
  d.baseDimIdx = i, d.largeDataIndices = o, d.barWidth = s, t.add(d), d.useStyle(n.getVisual("style")), d.style.stroke = null, Nt(d).seriesIndex = a.seriesIndex, a.get("silent") || (d.on("mousedown", $e), d.on("mousemove", $e)), e && e.push(d);
}
var $e = Cr(function(a) {
  var t = this, e = bi(t, a.offsetX, a.offsetY);
  Nt(t).dataIndex = e >= 0 ? e : null;
}, 30, !1);
function bi(a, t, e) {
  for (var r = a.baseDimIdx, n = 1 - r, i = a.shape.points, o = a.largeDataIndices, s = [], l = [], c = a.barWidth, u = 0, v = i.length / 3; u < v; u++) {
    var d = u * 3;
    if (l[r] = c, l[n] = i[d + 2], s[r] = i[d + r], s[n] = i[d + n], l[n] < 0 && (s[n] += l[n], l[n] = -l[n]), t >= s[0] && t <= s[0] + l[0] && e >= s[1] && e <= s[1] + l[1])
      return o[u];
  }
  return -1;
}
function Za(a, t, e) {
  if (Xn(e, "cartesian2d")) {
    var r = t, n = e.getArea();
    return {
      x: a ? r.x : n.x,
      y: a ? n.y : r.y,
      width: a ? r.width : n.width,
      height: a ? n.height : r.height
    };
  } else {
    var n = e.getArea(), i = t;
    return {
      cx: n.cx,
      cy: n.cy,
      r0: a ? n.r0 : i.r0,
      r: a ? n.r : i.r,
      startAngle: a ? i.startAngle : 0,
      endAngle: a ? i.endAngle : Math.PI * 2
    };
  }
}
function Si(a, t, e) {
  var r = a.type === "polar" ? gt : pt;
  return new r({
    shape: Za(t, e, a),
    silent: !0,
    z2: 0
  });
}
function Li(a, t) {
  if (a.height === 0) {
    var e = t.getOtherAxis(t.getBaseAxis());
    return e.inverse ? "bottom" : "top";
  }
  return a.height > 0 ? "bottom" : "top";
}
function Ci(a, t) {
  if (a.width === 0) {
    var e = t.getOtherAxis(t.getBaseAxis());
    return e.inverse ? "left" : "right";
  }
  return a.width >= 0 ? "right" : "left";
}
function Mi(a) {
  a.registerChartView(fi), a.registerSeriesModel(ui), a.registerLayout(a.PRIORITY.VISUAL.LAYOUT, ii(K)), a.registerLayout(a.PRIORITY.VISUAL.PROGRESSIVE_LAYOUT, oi(K)), a.registerProcessor(a.PRIORITY.PROCESSOR.STATISTIC, Zn(K)), a.registerAction({
    type: "changeAxisOrder",
    event: "changeAxisOrder",
    update: "update"
  }, function(t, e) {
    var r = t.componentType || "series";
    e.eachComponent({
      mainType: r,
      query: t
    }, function(n) {
      t.sortInfo && n.axis.setCategorySortInfo(t.sortInfo);
    });
  }), li(a);
}
function _i(a, t, e) {
  t = X(t) && {
    coordDimensions: t
  } || Gt({
    encodeDefine: a.getEncode()
  }, t);
  var r = a.getSource(), n = Mr(r, t).dimensions, i = new _r(n, a);
  return i.initData(r, e), i;
}
var Jt = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      return a !== null && a.apply(this, arguments) || this;
    }
    return t.prototype.getCoordSysModel = function() {
      return this.getReferringComponents("grid", ht).models[0];
    }, t.type = "cartesian2dAxis", t;
  })(kt)
);
ue(Jt, Ma);
var qa = {
  show: !0,
  // zlevel: 0,
  z: 0,
  // Inverse the axis.
  inverse: !1,
  // Axis name displayed.
  name: "",
  // 'start' | 'middle' | 'end'
  nameLocation: "end",
  // By degree. By default auto rotate by nameLocation.
  nameRotate: null,
  nameTruncate: {
    maxWidth: null,
    ellipsis: "...",
    placeholder: "."
  },
  // Use global text style by default.
  nameTextStyle: {
    // textMargin: never, // The default value will be specified based on `nameLocation`.
  },
  // The gap between axisName and axisLine.
  nameGap: 15,
  // Default `false` to support tooltip.
  silent: !1,
  // Default `false` to avoid legacy user event listener fail.
  triggerEvent: !1,
  tooltip: {
    show: !1
  },
  axisPointer: {},
  axisLine: {
    show: !0,
    onZero: "auto",
    onZeroAxisIndex: null,
    lineStyle: {
      color: F.color.axisLine,
      width: 1,
      type: "solid"
    },
    // The arrow at both ends the the axis.
    symbol: ["none", "none"],
    symbolSize: [10, 15],
    breakLine: !0
  },
  axisTick: {
    show: !0,
    // Whether axisTick is inside the grid or outside the grid.
    inside: !1,
    // The length of axisTick.
    length: 5,
    lineStyle: {
      width: 1
    }
  },
  axisLabel: {
    show: !0,
    // Whether axisLabel is inside the grid or outside the grid.
    inside: !1,
    rotate: 0,
    // true | false | null/undefined (auto)
    showMinLabel: null,
    // true | false | null/undefined (auto)
    showMaxLabel: null,
    margin: 8,
    // formatter: null,
    fontSize: 12,
    color: F.color.axisLabel,
    // In scenarios like axis labels, when labels text's progression direction matches the label
    // layout direction (e.g., when all letters are in a single line), extra start/end margin is
    // needed to prevent the text from appearing visually joined. In the other case, when lables
    // are stacked (e.g., having rotation or horizontal labels on yAxis), the layout needs to be
    // compact, so NO extra top/bottom margin should be applied.
    textMargin: [0, 3]
  },
  splitLine: {
    show: !0,
    showMinLine: !0,
    showMaxLine: !0,
    lineStyle: {
      color: F.color.axisSplitLine,
      width: 1,
      type: "solid"
    }
  },
  splitArea: {
    show: !1,
    areaStyle: {
      color: [F.color.backgroundTint, F.color.backgroundTransparent]
    }
  },
  breakArea: {
    show: !0,
    itemStyle: {
      color: F.color.neutral00,
      // Break border color should be darker than the splitLine
      // because it has opacity and should be more prominent
      borderColor: F.color.border,
      borderWidth: 1,
      borderType: [3, 3],
      opacity: 0.6
    },
    zigzagAmplitude: 4,
    zigzagMinSpan: 4,
    zigzagMaxSpan: 20,
    zigzagZ: 100,
    expandOnClick: !0
  },
  breakLabelLayout: {
    moveOverlap: "auto"
  }
}, wi = et({
  // The gap at both ends of the axis. For categoryAxis, boolean.
  boundaryGap: !0,
  // Set false to faster category collection.
  deduplication: null,
  jitter: 0,
  jitterOverlap: !0,
  jitterMargin: 2,
  // splitArea: {
  // show: false
  // },
  splitLine: {
    show: !1
  },
  axisTick: {
    // If tick is align with label when boundaryGap is true
    alignWithLabel: !1,
    interval: "auto",
    show: "auto"
  },
  axisLabel: {
    interval: "auto"
  }
}, qa), pe = et({
  boundaryGap: [0, 0],
  axisLine: {
    // Not shown when other axis is categoryAxis in cartesian
    show: "auto"
  },
  axisTick: {
    // Not shown when other axis is categoryAxis in cartesian
    show: "auto"
  },
  // TODO
  // min/max: [30, datamin, 60] or [20, datamin] or [datamin, 60]
  splitNumber: 5,
  minorTick: {
    // Minor tick, not available for cateogry axis.
    show: !1,
    // Split number of minor ticks. The value should be in range of (0, 100)
    splitNumber: 5,
    // Length of minor tick
    length: 3,
    // Line style
    lineStyle: {
      // Default to be same with axisTick
    }
  },
  minorSplitLine: {
    show: !1,
    lineStyle: {
      color: F.color.axisMinorSplitLine,
      width: 1
    }
  }
}, qa), Ti = et({
  splitNumber: 6,
  axisLabel: {
    // The default value of TimeScale is determined in `AxisBuilder`
    // showMinLabel: false,
    // showMaxLabel: false,
    rich: {
      primary: {
        fontWeight: "bold"
      }
    }
  },
  splitLine: {
    show: !1
  }
}, pe), Pi = U({
  logBase: 10
}, pe);
const Di = {
  category: wi,
  value: pe,
  time: Ti,
  log: Pi
};
function Et(a, t, e, r) {
  T(Pr, function(n, i) {
    var o = et(et({}, Di[i], !0), r, !0), s = (
      /** @class */
      (function(l) {
        O(c, l);
        function c() {
          var u = l !== null && l.apply(this, arguments) || this;
          return u.type = t + "Axis." + i, u;
        }
        return c.prototype.mergeDefaultAndTheme = function(u, v) {
          var d = wr(this), h = d ? ma(u) : {}, f = v.getTheme();
          et(u, f.get(i + "Axis")), et(u, this.getDefaultOption()), u.type = Ke(u), d && Yt(u, h, d);
        }, c.prototype.optionUpdated = function() {
          var u = this.option;
          u.type === "category" && (this.__ordinalMeta = Tr.createByAxisModel(this));
        }, c.prototype.getCategories = function(u) {
          var v = this.option;
          if (v.type === "category")
            return u ? v.data : this.__ordinalMeta.categories;
        }, c.prototype.getOrdinalMeta = function() {
          return this.__ordinalMeta;
        }, c.prototype.updateAxisBreaks = function(u) {
          var v = _a();
          return v ? v.updateModelAxisBreak(this, u) : {
            breaks: []
          };
        }, c.type = t + "Axis." + i, c.defaultOption = o, c;
      })(e)
    );
    a.registerComponentModel(s);
  }), a.registerSubTypeDefaulter(t + "Axis", Ke);
}
function Ke(a) {
  return a.type || (a.data ? "category" : "value");
}
var Ct = ["x", "y"];
function Ze(a) {
  return (a.type === "interval" || a.type === "time") && !Xt(a);
}
var Ii = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = j, e.dimensions = Ct, e;
    }
    return t.prototype.calcAffineTransform = function() {
      this._transform = this._invTransform = null;
      var e = this.getAxis("x").scale, r = this.getAxis("y").scale;
      if (!(!Ze(e) || !Ze(r))) {
        var n = Me(e, null), i = Me(r, null), o = this.dataToPoint([n[0], i[0]]), s = this.dataToPoint([n[1], i[1]]), l = n[1] - n[0], c = i[1] - i[0];
        if (!(!l || !c)) {
          var u = (s[0] - o[0]) / l, v = (s[1] - o[1]) / c, d = o[0] - n[0] * u, h = o[1] - i[0] * v, f = this._transform = [u, 0, 0, v, d, h];
          this._invTransform = Dr([], f);
        }
      }
    }, t.prototype.getBaseAxis = function() {
      return this.getAxesByScale("ordinal")[0] || this.getAxesByScale("time")[0] || this.getAxis("x");
    }, t.prototype.containPoint = function(e) {
      var r = this.getAxis("x"), n = this.getAxis("y");
      return r.contain(r.toLocalCoord(e[0])) && n.contain(n.toLocalCoord(e[1]));
    }, t.prototype.containData = function(e) {
      return this.getAxis("x").containData(e[0]) && this.getAxis("y").containData(e[1]);
    }, t.prototype.containZone = function(e, r) {
      var n = this.dataToPoint(e), i = this.dataToPoint(r), o = this.getArea(), s = new _e(n[0], n[1], i[0] - n[0], i[1] - n[1]);
      return o.intersect(s);
    }, t.prototype.dataToPoint = function(e, r, n) {
      n = n || [];
      var i = e[0], o = e[1];
      if (this._transform && i != null && isFinite(i) && o != null && isFinite(o))
        return we(n, e, this._transform);
      var s = this.getAxis("x"), l = this.getAxis("y");
      return n[0] = s.toGlobalCoord(s.dataToCoord(i, r)), n[1] = l.toGlobalCoord(l.dataToCoord(o, r)), n;
    }, t.prototype.clampData = function(e, r) {
      var n = this.getAxis("x").scale, i = this.getAxis("y").scale, o = n.getExtent(), s = i.getExtent(), l = n.parse(e[0]), c = i.parse(e[1]);
      return r = r || [], r[0] = Math.min(Math.max(Math.min(o[0], o[1]), l), Math.max(o[0], o[1])), r[1] = Math.min(Math.max(Math.min(s[0], s[1]), c), Math.max(s[0], s[1])), r;
    }, t.prototype.pointToData = function(e, r, n) {
      if (n = n || [], this._invTransform)
        return we(n, e, this._invTransform);
      var i = this.getAxis("x"), o = this.getAxis("y");
      return n[0] = i.coordToData(i.toLocalCoord(e[0]), r), n[1] = o.coordToData(o.toLocalCoord(e[1]), r), n;
    }, t.prototype.getOtherAxis = function(e) {
      return this.getAxis(e.dim === "x" ? "y" : "x");
    }, t.prototype.getArea = function(e) {
      e = e || 0;
      var r = this.getAxis("x").getGlobalExtent(), n = this.getAxis("y").getGlobalExtent(), i = Math.min(r[0], r[1]) - e, o = Math.min(n[0], n[1]) - e, s = Math.max(r[0], r[1]) - i + e, l = Math.max(n[0], n[1]) - o + e;
      return new _e(i, o, s, l);
    }, t;
  })(Ir)
);
function Oi(a, t) {
  var e = a.scale, r = a.model, n = Or(e, r, r.ecModel, a, null), i = Te(e), o = Te(t) ? t.intervalStub : t, s = i ? e.intervalStub : e, l = e.base, c = o.getTicks(), u = o.getTicks({
    expandToNicedExtent: !0
  }), v = c.length - 1, d, h, f;
  if (v === 1)
    d = h = 0, f = 1;
  else if (v === 2) {
    var g = at(c[0].value - c[1].value), p = at(c[1].value - c[2].value);
    d = h = 0, g === p ? f = 2 : (f = 1, g < p ? d = g / p : h = p / g);
  } else {
    var y = o.getConfig().interval;
    d = (1 - (c[0].value - u[0].value) / y) % 1, h = (1 - (u[v].value - c[v].value) / y) % 1, f = v - (d ? 1 : 0) - (h ? 1 : 0);
  }
  var m = n.zoomFixMM, x = m[0] || m[1], b = [n.fixMM[0] || x, n.fixMM[1] || x], S = e.getExtent(), L = s.getExtent(), A = Er(L, b), M, P, _, C, D, w;
  function B(W) {
    for (var st = 50, Q = 0; Q < st && !W(); Q++)
      _ = i ? _ * Y(l, 2) : Nr(_), C = Pe(_);
  }
  function E() {
    M = N(w - _ * d, C);
  }
  function R() {
    P = N(D + _ * h, C);
  }
  function $() {
    w = d ? N(M + _ * d, C) : M;
  }
  function I() {
    D = h ? N(P - _ * h, C) : P;
  }
  if (b[0] && b[1]) {
    M = A[0], P = A[1], _ = (P - M) / (f + d + h);
    var z = a.getExtent(), H = at(z[1] - z[0]);
    C = Gr([P, M], H, 0.5 / f), $(), I(), ae(C) && (_ = N(_, C));
  } else {
    var V = A[1] - A[0];
    _ = i ? Y(Br(V), 1) : kr(V / f, Wr), C = Pe(_), b[0] ? (M = A[0], B(function() {
      if ($(), D = N(w + _ * f, C), R(), P >= A[1])
        return !0;
    })) : b[1] ? (P = A[1], B(function() {
      if (I(), w = N(D - _ * f, C), E(), M <= A[0])
        return !0;
    })) : B(function() {
      w = N(Vr(A[0] / _) * _, C), D = N(De(A[1] / _) * _, C);
      var W = Fr((D - w) / _);
      if (W <= f) {
        var st = f - W, Q = void 0, be = n.incl0 || i;
        if (be && A[0] === 0)
          Q = [0, st];
        else if (be && A[1] === 0)
          Q = [st, 0];
        else {
          var lt = De(st / 2);
          Q = st % 2 === 0 ? [lt, lt] : M + P < A[0] + A[1] ? [lt, lt + 1] : [lt + 1, lt];
        }
        if (w = N(w - _ * Q[0], C), D = N(D + _ * Q[1], C), E(), R(), M <= A[0] && P >= A[1])
          return !0;
      }
    });
  }
  Rr(e, b, L, [M, P], S, {
    // NOTE: Even in LogScale, `interval` should not be in log space.
    interval: _,
    // Force ticks count, otherwise cumulative error may cause more unexpected ticks to be generated.
    // Though the overlapping tick labels may be auto-ignored, but probably unexpected, e.g., the min
    // tick label is ignored but the secondary min tick label is shown, which is unexpected when
    // `axis.min` is user-specified or dataZoom-specified.
    intervalCount: f,
    intervalPrecision: C,
    niceExtent: [w, D]
  });
}
var qe = [
  [3, 1],
  [0, 2]
  // xyIdx 1 => 'y'
], Ei = (
  /** @class */
  (function() {
    function a(t, e, r) {
      this.type = "grid", this._coordsMap = {}, this._coordsList = [], this._axesMap = {}, this._axesList = [], this.axisPointerEnabled = !0, this.dimensions = Ct, this._initCartesian(t, e, r), this.model = t;
    }
    return a.prototype.getRect = function() {
      return this._rect;
    }, a.prototype.update = function(t, e) {
      var r = this._axesMap;
      T(this._axesList, function(o) {
        $t(o, Kt);
        var s = o.scale;
        ga(s) && s.setSortInfo(o.model.get("categorySortInfo"));
      });
      function n(o) {
        for (var s = ve(o), l = [], c = s.length - 1; c >= 0; c--) {
          var u = o[+s[c]];
          u.__alignTo ? l.push(u) : wt(u);
        }
        T(l, function(v) {
          ki(v, v.__alignTo) ? wt(v) : Oi(v, v.__alignTo.scale);
        });
      }
      n(r.x), n(r.y);
      var i = {};
      T(r.x, function(o) {
        je(r, "y", o, i);
      }), T(r.y, function(o) {
        je(r, "x", o, i);
      }), this.resize(this.model, e);
    }, a.prototype.resize = function(t, e, r) {
      var n = wa(t, e), i = this._rect = Ta(t.getBoxLayoutParams(), n.refContainer), o = this._axesMap, s = this._coordsList, l = t.get("containLabel");
      if (ja(o, i), !r) {
        var c = Ni(i, s, o, l, e), u = void 0;
        if (l)
          u = ea(i.clone(), "axisLabel", null, i, o, c, n);
        else {
          var v = Gi(t, i, n), d = v.outerBoundsRect, h = v.parsedOuterBoundsContain, f = v.outerBoundsClamp;
          d && (u = ea(d, h, f, i, o, c, n));
        }
        Qa(i, o, de.determine, null, u, n), T(this._coordsList, function(g) {
          g.calcAffineTransform();
        });
      }
    }, a.prototype.getAxis = function(t, e) {
      var r = this._axesMap[t];
      if (r != null)
        return r[e || 0];
    }, a.prototype.getAxes = function() {
      return this._axesList.slice();
    }, a.prototype.getCartesian = function(t, e) {
      if (t != null && e != null) {
        var r = "x" + t + "y" + e;
        return this._coordsMap[r];
      }
      ce(t) && (e = t.yAxisIndex, t = t.xAxisIndex);
      for (var n = 0, i = this._coordsList; n < i.length; n++)
        if (i[n].getAxis("x").index === t || i[n].getAxis("y").index === e)
          return i[n];
    }, a.prototype.getCartesians = function() {
      return this._coordsList.slice();
    }, a.prototype.convertToPixel = function(t, e, r) {
      var n = this._findConvertTarget(e);
      return n.cartesian ? n.cartesian.dataToPoint(r) : n.axis ? n.axis.toGlobalCoord(n.axis.dataToCoord(r)) : null;
    }, a.prototype.convertFromPixel = function(t, e, r) {
      var n = this._findConvertTarget(e);
      return n.cartesian ? n.cartesian.pointToData(r) : n.axis ? n.axis.coordToData(n.axis.toLocalCoord(r)) : null;
    }, a.prototype._findConvertTarget = function(t) {
      var e = t.seriesModel, r = t.xAxisModel || e && e.getReferringComponents("xAxis", ht).models[0], n = t.yAxisModel || e && e.getReferringComponents("yAxis", ht).models[0], i = t.gridModel, o = this._coordsList, s, l;
      if (e)
        s = e.coordinateSystem, Lt(o, s) < 0 && (s = null);
      else if (r && n)
        s = this.getCartesian(r.componentIndex, n.componentIndex);
      else if (r)
        l = this.getAxis("x", r.componentIndex);
      else if (n)
        l = this.getAxis("y", n.componentIndex);
      else if (i) {
        var c = i.coordinateSystem;
        c === this && (s = this._coordsList[0]);
      }
      return {
        cartesian: s,
        axis: l
      };
    }, a.prototype.containPoint = function(t) {
      var e = this._coordsList[0];
      if (e)
        return e.containPoint(t);
    }, a.prototype._initCartesian = function(t, e, r) {
      var n = this, i = this, o = {
        left: !1,
        right: !1,
        top: !1,
        bottom: !1
      }, s = {
        x: {},
        y: {}
      }, l = {
        x: 0,
        y: 0
      };
      if (e.eachComponent("xAxis", c("x"), this), e.eachComponent("yAxis", c("y"), this), !l.x || !l.y) {
        this._axesMap = {}, this._axesList = [];
        return;
      }
      this._axesMap = s, T(s.x, function(u, v) {
        T(s.y, function(d, h) {
          var f = "x" + v + "y" + h, g = new Ii(f);
          g.master = n, g.model = t, n._coordsMap[f] = g, n._coordsList.push(g), g.addAxis(u), g.addAxis(d);
        });
      }), Je(s.x), Je(s.y);
      function c(u) {
        return function(v, d) {
          if (Bi(v, t)) {
            var h = v.get("position");
            u === "x" ? h !== "top" && h !== "bottom" && (h = o.bottom ? "top" : "bottom") : h !== "left" && h !== "right" && (h = o.left ? "right" : "left"), o[h] = !0;
            var f = Pa(v), g = new qn(u, Da(v, f, !0), [0, 0], f, h);
            g.onBand = Ia(g.scale, v), g.inverse = v.get("inverse"), v.axis = g, g.model = v, g.grid = i, g.index = d, i._axesList.push(g), s[u][d] = g, l[u]++;
          }
        };
      }
    }, a.prototype.getTooltipAxes = function(t) {
      var e = [], r = [];
      return T(this.getCartesians(), function(n) {
        var i = t != null && t !== "auto" ? n.getAxis(t) : n.getBaseAxis(), o = n.getOtherAxis(i);
        Lt(e, i) < 0 && e.push(i), Lt(r, o) < 0 && r.push(o);
      }), {
        baseAxes: e,
        otherAxes: r
      };
    }, a.create = function(t, e) {
      var r = [];
      return t.eachComponent("grid", function(n, i) {
        var o = new a(n, t, e);
        o.name = "grid_" + i, o.resize(n, e, !0), n.coordinateSystem = o, r.push(o), T(o._axesList, function(s) {
          zr(s, a.dimIdxMap);
        });
      }), t.eachSeries(function(n) {
        var i, o;
        Ur({
          targetModel: n,
          coordSysType: j,
          coordSysProvider: s
        });
        function s() {
          var l = tn(n), c = l.xAxisModel, u = l.yAxisModel;
          i = c.axis, o = u.axis;
          var v = c.getCoordSysModel(), d = v.coordinateSystem;
          return d.getCartesian(c.componentIndex, u.componentIndex);
        }
        i && o && (Pt(i, n, j), Pt(o, n, j));
      }, this), r;
    }, a.dimensions = Ct, a.dimIdxMap = Hr(Ct), a;
  })()
);
function Bi(a, t) {
  return a.getCoordSysModel() === t;
}
function je(a, t, e, r) {
  e.getAxesOnZeroOf = function() {
    return i ? [i] : [];
  };
  var n = a[t], i, o = e.model, s = o.get(["axisLine", "onZero"]), l = o.get(["axisLine", "onZeroAxisIndex"]);
  if (!s)
    return;
  if (l != null)
    Qe(s, n[l]) && (i = n[l]);
  else
    for (var c in n)
      if (Rt(n, c) && Qe(s, n[c]) && !r[u(n[c])]) {
        i = n[c];
        break;
      }
  i && (r[u(i)] = !0);
  function u(v) {
    return v.dim + "_" + v.index;
  }
}
function Qe(a, t) {
  if (!t)
    return !1;
  var e = t.scale, r = jr(e, 0), n = t && t.type !== "category" && t.type !== "time" && r !== Qr;
  return n && a === "auto" && Jr(t) && (n = !1), n;
}
function Je(a) {
  for (var t = ve(a), e, r = [], n = t.length - 1; n >= 0; n--) {
    var i = a[+t[n]];
    en(i.scale) && an(i.model, i.type, !0) == null && (i.model.get("alignTicks") && i.model.get("interval") == null ? r.push(i) : e = i);
  }
  e || (e = r.pop()), e && T(r, function(o) {
    o.__alignTo = e;
  });
}
function ki(a, t) {
  return Xt(a.scale) || Xt(t.scale) || t.scale.getTicks().length < 2;
}
function Ri(a, t) {
  var e = a.getExtent(), r = e[0] + e[1];
  a.toGlobalCoord = a.dim === "x" ? function(n) {
    return n + t;
  } : function(n) {
    return r - n + t;
  }, a.toLocalCoord = a.dim === "x" ? function(n) {
    return n - t;
  } : function(n) {
    return r - n + t;
  };
}
function ja(a, t) {
  T(a.x, function(e) {
    return ta(e, t.x, t.width);
  }), T(a.y, function(e) {
    return ta(e, t.y, t.height);
  });
}
function ta(a, t, e) {
  var r = [0, e], n = a.inverse ? 1 : 0;
  a.setExtent(r[n], r[1 - n]), Ri(a, t);
}
function ea(a, t, e, r, n, i, o) {
  Qa(r, n, de.estimate, t, !1, o);
  var s = [0, 0, 0, 0];
  c(0), c(1), u(r, 0, NaN), u(r, 1, NaN);
  var l = $r(s, function(d) {
    return d > 0;
  }) == null;
  return Kr(r, s, !0, !0, e), ja(n, r), l;
  function c(d) {
    T(n[ut[d]], function(h) {
      if (ft(h.model)) {
        var f = i.ensureRecord(h.model), g = f.labelInfoList;
        if (g)
          for (var p = 0; p < g.length; p++) {
            var y = g[p], m = h.scale.normalize(Oa(h.scale, Zr(y.label).labelInfo.tick));
            m = d === 1 ? 1 - m : m, u(y.rect, d, m), u(y.rect, 1 - d, NaN);
          }
        var x = f.nameLayout;
        if (x) {
          var m = Ea(f.nameLocation) ? 0.5 : NaN;
          u(x.rect, d, m), u(x.rect, 1 - d, NaN);
        }
      }
    });
  }
  function u(d, h, f) {
    var g = a[ut[h]] - d[ut[h]], p = d[Tt[h]] + d[ut[h]] - (a[Tt[h]] + a[ut[h]]);
    g = v(g, 1 - f), p = v(p, f);
    var y = qe[h][0], m = qe[h][1];
    s[y] = Y(s[y], g), s[m] = Y(s[m], p);
  }
  function v(d, h) {
    return d > 0 && !rn(h) && h > 1e-4 && (d /= h), d;
  }
}
function Ni(a, t, e, r, n) {
  var i = new Xr(Wi);
  return T(e, function(o) {
    return T(o, function(s) {
      if (ft(s.model)) {
        var l = !r;
        s.axisBuilder = Yr(a, t, s.model, n, i, l);
      }
    });
  }), i;
}
function Qa(a, t, e, r, n, i) {
  var o = e === de.determine;
  T(t, function(c) {
    return T(c, function(u) {
      ft(u.model) && (qr(u.axisBuilder, a, u.model), u.axisBuilder.build(o ? {
        axisTickLabelDetermine: !0
      } : {
        axisTickLabelEstimate: !0
      }, {
        noPxChange: n
      }));
    });
  });
  var s = {
    x: 0,
    y: 0
  };
  l(0), l(1);
  function l(c) {
    s[ut[1 - c]] = a[Tt[c]] <= i.refContainer[Tt[c]] * 0.5 ? 0 : 1 - c === 1 ? 2 : 1;
  }
  T(t, function(c, u) {
    return T(c, function(v) {
      ft(v.model) && ((r === "all" || o) && v.axisBuilder.build({
        axisName: !0
      }, {
        nameMarginLevel: s[u]
      }), o && v.axisBuilder.build({
        axisLine: !0
      }));
    });
  });
}
function Gi(a, t, e) {
  var r, n = a.get("outerBoundsMode", !0);
  n === "same" ? r = t.clone() : (n == null || n === "auto") && (r = Ta(a.get("outerBounds", !0) || Ya, e.refContainer));
  var i = a.get("outerBoundsContain", !0), o;
  i == null || i === "auto" || Lt(["all", "axisLabel"], i) < 0 ? o = "all" : o = i;
  var s = [Ie(Oe(a.get("outerBoundsClampWidth", !0), Ot[0]), t.width), Ie(Oe(a.get("outerBoundsClampHeight", !0), Ot[1]), t.height)];
  return {
    outerBoundsRect: r,
    parsedOuterBoundsContain: o,
    outerBoundsClamp: s
  };
}
var Wi = function(a, t, e, r, n, i) {
  var o = e.axis.dim === "x" ? "y" : "x";
  nn(a, t, e, r, n, i), Ea(a.nameLocation) || T(t.recordMap[o], function(s) {
    s && s.labelInfoList && s.dirVec && on(s.labelInfoList, s.dirVec, r, n);
  });
}, te = Ba();
function Vi(a, t, e, r) {
  var n = e.axis;
  if (!n.scale.isBlank()) {
    var i = e.getModel("splitArea"), o = i.getModel("areaStyle"), s = o.get("color"), l = r.coordinateSystem.getRect(), c = n.getTicksCoords({
      tickModel: i,
      breakTicks: "none",
      pruneByBreak: "preserve_extent_bound"
    });
    if (c.length) {
      var u = s.length, v = te(a).splitAreaColors, d = ee(), h = 0;
      if (v)
        for (var f = 0; f < c.length; f++) {
          var g = v.get(c[f].tickValue);
          if (g != null) {
            h = (g + (u - 1) * f) % u;
            break;
          }
        }
      var p = n.toGlobalCoord(c[0].coord), y = o.getAreaStyle();
      s = X(s) ? s : [s];
      for (var f = 1; f < c.length; f++) {
        var m = n.toGlobalCoord(c[f].coord), x = void 0, b = void 0, S = void 0, L = void 0;
        n.isHorizontal() ? (x = p, b = l.y, S = m - x, L = l.height, p = x + S) : (x = l.x, b = p, S = l.width, L = m - b, p = b + L);
        var A = c[f - 1].tickValue;
        A != null && d.set(A, h), t.add(new pt({
          anid: A != null ? "area_" + A : null,
          shape: {
            x,
            y: b,
            width: S,
            height: L
          },
          style: U({
            fill: s[h]
          }, y),
          autoBatch: !0,
          silent: !0
        })), h = (h + 1) % u;
      }
      te(a).splitAreaColors = d;
    }
  }
}
function Fi(a) {
  te(a).splitAreaColors = null;
}
var zi = ["splitArea", "splitLine", "minorSplitLine", "breakArea"], Ja = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e.axisPointerClass = "CartesianAxisPointer", e;
    }
    return t.prototype.render = function(e, r, n, i) {
      this.group.removeAll();
      var o = this._axisGroup;
      if (this._axisGroup = new le(), this.group.add(this._axisGroup), !!ft(e)) {
        this._axisGroup.add(e.axis.axisBuilder.group), T(zi, function(l) {
          e.get([l, "show"]) && Ui[l](this, this._axisGroup, e, e.getCoordSysModel(), n);
        }, this);
        var s = i && i.type === "changeAxisOrder" && i.isInitSort;
        s || ka(o, this._axisGroup, e), a.prototype.render.call(this, e, r, n, i);
      }
    }, t.prototype.remove = function() {
      Fi(this);
    }, t.type = "cartesianAxis", t;
  })(Wt)
), Ui = {
  splitLine: function(a, t, e, r, n) {
    var i = e.axis;
    if (!i.scale.isBlank()) {
      var o = e.getModel("splitLine"), s = o.getModel("lineStyle"), l = s.get("color"), c = o.get("showMinLine") !== !1, u = o.get("showMaxLine") !== !1;
      l = X(l) ? l : [l];
      for (var v = r.coordinateSystem.getRect(), d = i.isHorizontal(), h = 0, f = i.getTicksCoords({
        tickModel: o,
        breakTicks: "none",
        pruneByBreak: "preserve_extent_bound"
      }), g = [], p = [], y = s.getLineStyle(), m = 0; m < f.length; m++) {
        var x = i.toGlobalCoord(f[m].coord);
        if (!(m === 0 && !c || m === f.length - 1 && !u)) {
          var b = f[m].tickValue;
          d ? (g[0] = x, g[1] = v.y, p[0] = x, p[1] = v.y + v.height) : (g[0] = v.x, g[1] = x, p[0] = v.x + v.width, p[1] = x);
          var S = h++ % l.length, L = new ct({
            anid: b != null ? "line_" + b : null,
            autoBatch: !0,
            shape: {
              x1: g[0],
              y1: g[1],
              x2: p[0],
              y2: p[1]
            },
            style: U({
              stroke: l[S]
            }, y),
            silent: !0
          });
          Ee(L.shape, y.lineWidth), t.add(L);
        }
      }
    }
  },
  minorSplitLine: function(a, t, e, r, n) {
    var i = e.axis, o = e.getModel("minorSplitLine"), s = o.getModel("lineStyle"), l = r.coordinateSystem.getRect(), c = i.isHorizontal(), u = i.getMinorTicksCoords();
    if (u.length)
      for (var v = [], d = [], h = s.getLineStyle(), f = 0; f < u.length; f++)
        for (var g = 0; g < u[f].length; g++) {
          var p = i.toGlobalCoord(u[f][g].coord);
          c ? (v[0] = p, v[1] = l.y, d[0] = p, d[1] = l.y + l.height) : (v[0] = l.x, v[1] = p, d[0] = l.x + l.width, d[1] = p);
          var y = new ct({
            anid: "minor_line_" + u[f][g].tickValue,
            autoBatch: !0,
            shape: {
              x1: v[0],
              y1: v[1],
              x2: d[0],
              y2: d[1]
            },
            style: h,
            silent: !0
          });
          Ee(y.shape, h.lineWidth), t.add(y);
        }
  },
  splitArea: function(a, t, e, r, n) {
    Vi(a, t, e, r);
  },
  breakArea: function(a, t, e, r, n) {
    var i = _a(), o = e.axis.scale;
    i && o.type !== "ordinal" && i.rectCoordBuildBreakAxis(t, a, e, r.coordinateSystem.getRect(), n);
  }
}, tr = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "xAxis", t;
  })(Ja)
), Hi = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = tr.type, e;
    }
    return t.type = "yAxis", t;
  })(Ja)
), Yi = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = "grid", e;
    }
    return t.prototype.render = function(e, r) {
      this.group.removeAll(), e.get("show") && this.group.add(new pt({
        shape: e.coordinateSystem.getRect(),
        style: U({
          fill: e.get("backgroundColor")
        }, e.getItemStyle()),
        silent: !0,
        z2: -1
      }));
    }, t.type = "grid", t;
  })(Ra)
), aa = {
  // gridIndex: 0,
  // gridId: '',
  offset: 0
};
function Xi(a) {
  a.registerComponentView(Yi), a.registerComponentModel(Jn), a.registerCoordinateSystem("cartesian2d", Ei), Et(a, "x", Jt, aa), Et(a, "y", Jt, aa), a.registerComponentView(tr), a.registerComponentView(Hi), a.registerPreprocessor(function(t) {
    t.xAxis && t.yAxis && !t.grid && (t.grid = {});
  });
}
var $i = (
  /** @class */
  (function() {
    function a() {
    }
    return a.prototype._hasEncodeRule = function(t) {
      var e = this.getEncode();
      return e && e.get(t) != null;
    }, a.prototype.getInitialData = function(t, e) {
      var r, n = e.getComponent("xAxis", this.get("xAxisIndex")), i = e.getComponent("yAxis", this.get("yAxisIndex")), o = n.get("type"), s = i.get("type"), l, c = t.layout;
      o === "category" ? (c = "horizontal", r = n.getOrdinalMeta(), l = !this._hasEncodeRule("x")) : s === "category" && (c = "vertical", r = i.getOrdinalMeta(), l = !this._hasEncodeRule("y")), c || (c = s === "time" ? "vertical" : "horizontal"), this._layout = c;
      var u = ["x", "y"], v = c === "horizontal" ? 0 : 1, d = this._baseAxisDim = u[v], h = u[1 - v], f = [n, i], g = f[v].get("type"), p = f[1 - v].get("type"), y = t.data;
      if (y && l) {
        var m = [];
        T(y, function(S, L) {
          var A;
          X(S) ? (A = S.slice(), S.unshift(L)) : X(S.value) ? (A = Gt({}, S), A.value = A.value.slice(), S.value.unshift(L)) : A = S, m.push(A);
        }), t.data = m;
      }
      var x = this.defaultValueDimensions, b = [{
        name: d,
        type: Be(g),
        ordinalMeta: r,
        otherDims: {
          tooltip: !1,
          itemName: 0
        },
        dimsDef: ["base"]
      }, {
        name: h,
        type: Be(p),
        dimsDef: x.slice()
      }];
      return _i(this, {
        coordDimensions: b,
        dimensionsCount: x.length + 1,
        encodeDefaulter: sn(ln, b, this)
      });
    }, a.prototype.getBaseAxis = function() {
      var t = this._baseAxisDim;
      return this.ecModel.getComponent(t + "Axis", this.get(t + "AxisIndex")).axis;
    }, a.prototype.getWhiskerBoxesLayout = function() {
      return this._layout;
    }, a;
  })()
);
function ra(a, t) {
  for (var e = t.ends.length, r = 0, n = 0; n < e; n++)
    a.contain(t.ends[n][0], t.ends[n][1]) && r++;
  return r ? r < e ? qt : Zt : jt;
}
var Z = "boxplot", er = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e.defaultValueDimensions = [{
        name: "min",
        defaultTooltip: !0
      }, {
        name: "Q1",
        defaultTooltip: !0
      }, {
        name: "median",
        defaultTooltip: !0
      }, {
        name: "Q3",
        defaultTooltip: !0
      }, {
        name: "max",
        defaultTooltip: !0
      }], e.visualDrawType = "stroke", e;
    }
    return t.type = "series." + Z, t.dependencies = ["xAxis", "yAxis", "grid"], t.defaultOption = {
      // zlevel: 0,
      z: 2,
      coordinateSystem: "cartesian2d",
      legendHoverLink: !0,
      layout: null,
      clip: !0,
      boxWidth: [7, 50],
      itemStyle: {
        color: F.color.neutral00,
        borderWidth: 1
      },
      emphasis: {
        scale: !0,
        itemStyle: {
          borderWidth: 2,
          shadowBlur: 5,
          shadowOffsetX: 1,
          shadowOffsetY: 1,
          shadowColor: F.color.shadow
        }
      },
      animationDuration: 800
    }, t;
  })(oe)
);
ue(er, $i, !0);
var Ki = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = Z, e;
    }
    return t.prototype.render = function(e, r, n) {
      var i = e.getData(), o = this.group, s = this._data;
      this._data || o.removeAll();
      var l = e.getWhiskerBoxesLayout() === "horizontal" ? 1 : 0, c = e.get("clip", !0), u = e.coordinateSystem, v = u.getArea && u.getArea(), d = c && Fa(u, !1, e);
      i.diff(s).add(function(h) {
        if (i.hasValue(h)) {
          var f = i.getItemLayout(h), g = c ? ra(v, f) : Zt;
          if (g === jt)
            return;
          var p = na(f, i, h, l, !0);
          Ne(g === qt, p, d), i.setItemGraphicEl(h, p), o.add(p);
        }
      }).update(function(h, f) {
        var g = s.getItemGraphicEl(f);
        if (!i.hasValue(h)) {
          o.remove(g);
          return;
        }
        var p = i.getItemLayout(h), y = c ? ra(v, p) : Zt;
        if (y === jt) {
          o.remove(g);
          return;
        }
        g ? (ba(g), ar(p, g, i, h)) : g = na(p, i, h, l), Ne(y === qt, g, d), o.add(g), i.setItemGraphicEl(h, g);
      }).remove(function(h) {
        var f = s.getItemGraphicEl(h);
        f && o.remove(f);
      }).execute(), this._data = i;
    }, t.prototype.remove = function(e) {
      var r = this.group, n = this._data;
      this._data = null, n && n.eachItemGraphicEl(function(i) {
        i && r.remove(i);
      });
    }, t.type = Z, t;
  })(Sa)
), Zi = (
  /** @class */
  /* @__PURE__ */ (function() {
    function a() {
    }
    return a;
  })()
), qi = (
  /** @class */
  (function(a) {
    O(t, a);
    function t(e) {
      var r = a.call(this, e) || this;
      return r.type = "boxplotBoxPath", r;
    }
    return t.prototype.getDefaultShape = function() {
      return new Zi();
    }, t.prototype.buildPath = function(e, r) {
      var n = r.points, i = 0;
      for (e.moveTo(n[i][0], n[i][1]), i++; i < 4; i++)
        e.lineTo(n[i][0], n[i][1]);
      for (e.closePath(); i < n.length; i++)
        e.moveTo(n[i][0], n[i][1]), i++, e.lineTo(n[i][0], n[i][1]);
    }, t;
  })(se)
);
function na(a, t, e, r, n) {
  var i = a.ends, o = new qi({
    shape: {
      points: n ? ji(i, r, a) : i
    }
  });
  return ar(a, o, t, e, n), o;
}
function ar(a, t, e, r, n) {
  var i = e.hostModel, o = he[n ? "initProps" : "updateProps"];
  o(t, {
    shape: {
      points: a.ends
    }
  }, i, r), t.useStyle(e.getItemVisual(r, "style")), t.style.strokeNoScale = !0, t.z2 = 100;
  var s = e.getItemModel(r), l = s.getModel("emphasis");
  Ca(t, s), La(t, l.get("focus"), l.get("blurScope"), l.get("disabled"));
}
function ji(a, t, e) {
  return mt(a, function(r) {
    return r = r.slice(), r[t] = e.initBaseline, r;
  });
}
var Qi = ne(), Ji = Na(Z, to);
function to(a) {
  var t = vt(Z);
  re(a, t, function(e) {
    var r = un(e, t);
    if (r) {
      var n = eo(e, r);
      nt(e, t, function(i) {
        var o = i.seriesIndex;
        ao(i, n.boxOffsetList[o], n.boxWidthList[o]);
      });
    }
  });
}
function eo(a, t) {
  var e = [], r = [], n = [], i = Bt(a, {
    fromStat: {
      key: vt(Z)
    },
    min: 1
  }).w;
  nt(a, vt(Z), function(u) {
    var v = u.get("boxWidth");
    X(v) || (v = [v, v]), n[u.seriesIndex] = [G(v[0], i) || 0, G(v[1], i) || 0];
  });
  var o = i * 0.8 - 2, s = o / t * 0.3, l = (o - s * (t - 1)) / t, c = l / 2 - o / 2;
  return nt(a, vt(Z), function(u) {
    var v = u.seriesIndex;
    r[v] = c, c += s + l, e[v] = rt(Y(l, n[v][0]), n[v][1]);
  }), {
    boxOffsetList: r,
    boxWidthList: e
  };
}
function ao(a, t, e) {
  var r = a.coordinateSystem, n = a.getData(), i = e / 2, o = a.getWhiskerBoxesLayout() === "horizontal" ? 0 : 1, s = 1 - o, l = ["x", "y"], c = n.mapDimension(l[o]), u = n.mapDimensionsAll(l[s]);
  if (c == null || u.length < 5)
    return;
  for (var v = 0; v < n.count(); v++) {
    var d = n.get(c, v), h = x(d, u[2], v), f = x(d, u[0], v), g = x(d, u[1], v), p = x(d, u[3], v), y = x(d, u[4], v), m = [];
    b(m, g, !1), b(m, p, !0), m.push(f, g, y, p), S(m, f), S(m, y), S(m, h), n.setItemLayout(v, {
      initBaseline: h[s],
      ends: m
    });
  }
  function x(L, A, M) {
    var P = n.get(A, M), _ = [];
    _[o] = L, _[s] = P;
    var C;
    return isNaN(L) || isNaN(P) ? C = [NaN, NaN] : (C = r.dataToPoint(_), C[o] += t), C;
  }
  function b(L, A, M) {
    var P = A.slice(), _ = A.slice();
    P[o] += i, _[o] -= i, M ? L.push(P, _) : L.push(_, P);
  }
  function S(L, A) {
    var M = A.slice(), P = A.slice();
    M[o] -= i, P[o] += i, L.push(M, P);
  }
}
function ro(a) {
  Qi(a, function() {
    var t = vt(Z);
    pa(a, {
      key: t,
      seriesType: Z,
      getMetrics: za
    }), ie(t, ge(t));
  });
}
function no(a, t) {
  t = t || {};
  for (var e = [], r = [], n = t.boundIQR, i = n === "none" || n === 0, o = 0; o < a.length; o++) {
    var s = ha(a[o].slice()), l = Ft(s, 0.25), c = Ft(s, 0.5), u = Ft(s, 0.75), v = s[0], d = s[s.length - 1], h = (n ?? 1.5) * (u - l), f = i ? v : Math.max(v, l - h), g = i ? d : Math.min(d, u + h), p = t.itemNameFormatter, y = da(p) ? p({
      value: o
    }) : Mt(p) ? p.replace("{value}", o + "") : o + "";
    e.push([y, f, l, c, u, g]);
    for (var m = 0; m < s.length; m++) {
      var x = s[m];
      if (x < f || x > g) {
        var b = [y, x];
        r.push(b);
      }
    }
  }
  return {
    boxData: e,
    outliers: r
  };
}
var io = {
  type: "echarts:boxplot",
  transform: function(t) {
    var e = t.upstream;
    if (e.sourceFormat !== Ga) {
      var r = "";
      k(r);
    }
    var n = no(e.getRawData(), t.config);
    return [{
      dimensions: ["ItemName", "Low", "Q1", "Q2", "Q3", "High"],
      data: n.boxData
    }, {
      data: n.outliers
    }];
  }
};
function oo(a) {
  a.registerSeriesModel(er), a.registerChartView(Ki), a.registerLayout(Ji), a.registerTransform(io), ro(a);
}
function so(a) {
  Dt(Xi), Dt(Wa);
}
var lo = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      return a !== null && a.apply(this, arguments) || this;
    }
    return t.prototype.makeElOption = function(e, r, n, i, o) {
      var s = n.axis;
      s.dim === "angle" && (this.animationThreshold = Math.PI / 18);
      var l = s.polar, c = s.getExtent(), u = l.getOtherAxis(s).getExtent(), v = s.dataToCoord(r), d = i.get("type");
      if (d && d !== "none") {
        var h = cn(i), f = co[d](s, l, v, c, u, i.get("seriesDataIndices"), i.ecModel);
        f.style = h, e.graphicKey = f.type, e.pointer = f;
      }
      var g = i.get(["label", "margin"]), p = uo(r, n, i, l, g);
      vn(e, n, i, o, p);
    }, t;
  })(dn)
);
function uo(a, t, e, r, n) {
  var i = t.axis, o = i.dataToCoord(a), s = r.getAngleAxis().getExtent()[0];
  s = s / 180 * Math.PI;
  var l = r.getRadiusAxis().getExtent(), c, u, v;
  if (i.dim === "radius") {
    var d = xn();
    pn(d, d, s), mn(d, d, [r.cx, r.cy]), c = yn([o, -n], d);
    var h = t.getModel("axisLabel").get("rotate") || 0, f = It.innerTextLayout(s, h * Math.PI / 180, -1);
    u = f.textAlign, v = f.textVerticalAlign;
  } else {
    var g = l[1];
    c = r.coordToPoint([g + n, o]);
    var p = r.cx, y = r.cy;
    u = Math.abs(c[0] - p) / g < 0.3 ? "center" : c[0] > p ? "left" : "right", v = Math.abs(c[1] - y) / g < 0.3 ? "middle" : c[1] > y ? "top" : "bottom";
  }
  return {
    position: c,
    align: u,
    verticalAlign: v
  };
}
var co = {
  line: function(a, t, e, r, n) {
    return a.dim === "angle" ? {
      type: "Line",
      shape: gn(t.coordToPoint([n[0], e]), t.coordToPoint([n[1], e]))
    } : {
      type: "Circle",
      shape: {
        cx: t.cx,
        cy: t.cy,
        r: e
      }
    };
  },
  shadow: function(a, t, e, r, n, i, o) {
    var s = Math.PI / 180, l = hn(a, i, o), c;
    if (a.dim === "angle")
      c = ke(
        t.cx,
        t.cy,
        n[0],
        n[1],
        // In ECharts the screen y is negative if angle is positive,
        // opposite to zrender shape.
        // No need clamp.
        (-e - l / 2) * s,
        (-e + l / 2) * s
      );
    else {
      var u = fn(e, r, l), v = u[0], d = u[1];
      c = ke(t.cx, t.cy, v, d, 0, Math.PI * 2);
    }
    return {
      type: "Sector",
      shape: c
    };
  }
}, q = "polar", ia = q, vo = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.prototype.findAxisModel = function(e) {
      var r, n = this.ecModel;
      return n.eachComponent(e, function(i) {
        i.getCoordSysModel() === this && (r = i);
      }, this), r;
    }, t.type = q, t.dependencies = ["radiusAxis", "angleAxis"], t.defaultOption = {
      // zlevel: 0,
      z: 0,
      center: ["50%", "50%"],
      radius: "80%"
    }, t;
  })(kt)
), me = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      return a !== null && a.apply(this, arguments) || this;
    }
    return t.prototype.getCoordSysModel = function() {
      return this.getReferringComponents("polar", ht).models[0];
    }, t.type = "polarAxis", t;
  })(kt)
);
ue(me, Ma);
var ho = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "angleAxis", t;
  })(me)
), fo = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "radiusAxis", t;
  })(me)
), ye = (
  /** @class */
  (function(a) {
    O(t, a);
    function t(e, r) {
      return a.call(this, "radius", e, r) || this;
    }
    return t.prototype.pointToData = function(e, r) {
      return this.polar.pointToData(e, r)[this.dim === "radius" ? 0 : 1];
    }, t;
  })(it)
);
ye.prototype.dataToRadius = it.prototype.dataToCoord;
ye.prototype.radiusToData = it.prototype.coordToData;
var go = Ba(), xe = (
  /** @class */
  (function(a) {
    O(t, a);
    function t(e, r) {
      return a.call(this, "angle", e, r || [0, 360]) || this;
    }
    return t.prototype.pointToData = function(e, r) {
      return this.polar.pointToData(e, r)[this.dim === "radius" ? 0 : 1];
    }, t.prototype.calculateCategoryInterval = function() {
      var e = this, r = e.getLabelModel(), n = e.scale, i = n.getExtent(), o = n.count();
      if (i[1] - i[0] < 1)
        return 0;
      var s = i[0], l = e.dataToCoord(s + 1) - e.dataToCoord(s), c = Math.abs(l), u = An(s == null ? "" : s + "", r.getFont(), "center", "top"), v = Math.max(u.height, 7), d = v / c;
      isNaN(d) && (d = 1 / 0);
      var h = Math.max(0, Math.floor(d)), f = go(e.model), g = f.lastAutoInterval, p = f.lastTickCount;
      return g != null && p != null && Math.abs(g - h) <= 1 && Math.abs(p - o) <= 1 && g > h ? h = g : (f.lastTickCount = o, f.lastAutoInterval = h), h;
    }, t;
  })(it)
);
xe.prototype.dataToAngle = it.prototype.dataToCoord;
xe.prototype.angleToData = it.prototype.coordToData;
var rr = ["radius", "angle"], po = (
  /** @class */
  (function() {
    function a(t) {
      this.dimensions = rr, this.type = q, this.cx = 0, this.cy = 0, this._radiusAxis = new ye(), this._angleAxis = new xe(), this.axisPointerEnabled = !0, this.name = t || "", this._radiusAxis.polar = this._angleAxis.polar = this;
    }
    return a.prototype.containPoint = function(t) {
      var e = this.pointToCoord(t);
      return this._radiusAxis.contain(e[0]) && this._angleAxis.contain(e[1]);
    }, a.prototype.containData = function(t) {
      return this._radiusAxis.containData(t[0]) && this._angleAxis.containData(t[1]);
    }, a.prototype.getAxis = function(t) {
      var e = "_" + t + "Axis";
      return this[e];
    }, a.prototype.getAxes = function() {
      return [this._radiusAxis, this._angleAxis];
    }, a.prototype.getAxesByScale = function(t) {
      var e = [], r = this._angleAxis, n = this._radiusAxis;
      return r.scale.type === t && e.push(r), n.scale.type === t && e.push(n), e;
    }, a.prototype.getAngleAxis = function() {
      return this._angleAxis;
    }, a.prototype.getRadiusAxis = function() {
      return this._radiusAxis;
    }, a.prototype.getOtherAxis = function(t) {
      var e = this._angleAxis;
      return t === e ? this._radiusAxis : e;
    }, a.prototype.getBaseAxis = function() {
      return this.getAxesByScale("ordinal")[0] || this.getAxesByScale("time")[0] || this.getAngleAxis();
    }, a.prototype.getTooltipAxes = function(t) {
      var e = t != null && t !== "auto" ? this.getAxis(t) : this.getBaseAxis();
      return {
        baseAxes: [e],
        otherAxes: [this.getOtherAxis(e)]
      };
    }, a.prototype.dataToPoint = function(t, e, r) {
      return this.coordToPoint([
        // Must be the same order as polarDimensions
        this._radiusAxis.dataToRadius(t[0], e),
        this._angleAxis.dataToAngle(t[1], e)
      ], r);
    }, a.prototype.pointToData = function(t, e, r) {
      r = r || [];
      var n = this.pointToCoord(t);
      return r[0] = this._radiusAxis.radiusToData(n[0], e), r[1] = this._angleAxis.angleToData(n[1], e), r;
    }, a.prototype.pointToCoord = function(t) {
      var e = t[0] - this.cx, r = t[1] - this.cy, n = this.getAngleAxis(), i = n.getExtent(), o = Math.min(i[0], i[1]), s = Math.max(i[0], i[1]);
      n.inverse ? o = s - 360 : s = o + 360;
      var l = Math.sqrt(e * e + r * r);
      e /= l, r /= l;
      for (var c = Math.atan2(-r, e) / Math.PI * 180, u = c < o ? 1 : -1; c < o || c > s; )
        c += u * 360;
      return [l, c];
    }, a.prototype.coordToPoint = function(t, e) {
      e = e || [];
      var r = t[0], n = t[1] / 180 * Math.PI;
      return e[0] = Math.cos(n) * r + this.cx, e[1] = -Math.sin(n) * r + this.cy, e;
    }, a.prototype.getArea = function() {
      var t = this.getAngleAxis(), e = this.getRadiusAxis(), r = e.getExtent().slice();
      r[0] > r[1] && r.reverse();
      var n = t.getExtent(), i = Math.PI / 180, o = 1e-4;
      return {
        cx: this.cx,
        cy: this.cy,
        r0: r[0],
        r: r[1],
        startAngle: -n[0] * i,
        endAngle: -n[1] * i,
        clockwise: t.inverse,
        contain: function(s, l) {
          var c = s - this.cx, u = l - this.cy, v = c * c + u * u, d = this.r, h = this.r0;
          return d !== h && v - o <= d * d && v + o >= h * h;
        },
        // As the bounding box
        x: this.cx - r[1],
        y: this.cy - r[1],
        width: r[1] * 2,
        height: r[1] * 2
      };
    }, a.prototype.convertToPixel = function(t, e, r) {
      var n = oa(e);
      return n === this ? this.dataToPoint(r) : null;
    }, a.prototype.convertFromPixel = function(t, e, r) {
      var n = oa(e);
      return n === this ? this.pointToData(r) : null;
    }, a;
  })()
);
function oa(a) {
  var t = a.seriesModel, e = a.polarModel;
  return e && e.coordinateSystem || t && t.coordinateSystem;
}
function mo(a, t, e) {
  var r = t.get("center"), n = wa(t, e).refContainer;
  a.cx = G(r[0], n.width) + n.x, a.cy = G(r[1], n.height) + n.y;
  var i = a.getRadiusAxis(), o = Math.min(n.width, n.height) / 2, s = t.get("radius");
  s == null ? s = [0, "100%"] : X(s) || (s = [0, s]);
  var l = [G(s[0], o), G(s[1], o)];
  i.inverse ? i.setExtent(l[1], l[0]) : i.setExtent(l[0], l[1]);
}
function yo(a, t) {
  var e = this, r = e.getAngleAxis(), n = e.getRadiusAxis();
  if ($t(r, Kt), $t(n, Kt), wt(r), wt(n), r.type === "category" && !r.onBand) {
    var i = r.getExtent(), o = 360 / r.scale.count();
    r.inverse ? i[1] += o : i[1] -= o, r.setExtent(i[0], i[1]);
  }
}
function xo(a) {
  return a.mainType === "angleAxis";
}
function sa(a, t) {
  var e;
  if (a.type = Pa(t), a.scale = Da(t, a.type, !1), a.onBand = Ia(a.scale, t), a.inverse = t.get("inverse"), xo(t)) {
    a.inverse = a.inverse !== t.get("clockwise");
    var r = t.get("startAngle"), n = (e = t.get("endAngle")) !== null && e !== void 0 ? e : r + (a.inverse ? -360 : 360);
    a.setExtent(r, n);
  }
  t.axis = a, a.model = t;
}
var Ao = {
  dimensions: rr,
  create: function(a, t) {
    var e = [];
    return a.eachComponent(ia, function(r, n) {
      var i = new po(n + "");
      i.update = yo;
      var o = i.getRadiusAxis(), s = i.getAngleAxis(), l = r.findAxisModel("radiusAxis"), c = r.findAxisModel("angleAxis");
      sa(o, l), sa(s, c), mo(i, r, t), e.push(i), r.coordinateSystem = i, i.model = r;
    }), a.eachSeries(function(r) {
      if (r.get("coordinateSystem") === q) {
        var n = r.getReferringComponents(ia, ht).models[0], i = r.coordinateSystem = n.coordinateSystem;
        i && (Pt(i.getRadiusAxis(), r, q), Pt(i.getAngleAxis(), r, q));
      }
    }), e;
  }
}, bo = ["axisLine", "axisLabel", "axisTick", "minorTick", "splitLine", "minorSplitLine", "splitArea"];
function bt(a, t, e) {
  t[1] > t[0] && (t = t.slice().reverse());
  var r = a.coordToPoint([t[0], e]), n = a.coordToPoint([t[1], e]);
  return {
    x1: r[0],
    y1: r[1],
    x2: n[0],
    y2: n[1]
  };
}
function St(a) {
  var t = a.getRadiusAxis();
  return t.inverse ? 0 : 1;
}
function la(a) {
  var t = a[0], e = a[a.length - 1];
  t && e && Math.abs(Math.abs(t.coord - e.coord) - 360) < 1e-4 && a.pop();
}
var So = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e.axisPointerClass = "PolarAxisPointer", e;
    }
    return t.prototype.render = function(e, r) {
      if (this.group.removeAll(), !!e.get("show")) {
        var n = e.axis, i = n.polar, o = i.getRadiusAxis().getExtent(), s = n.getTicksCoords({
          breakTicks: "none"
        }), l = n.getMinorTicksCoords(), c = [];
        T(n.getViewLabels(), function(u) {
          if (!u.tick.offInterval) {
            u = bn(u);
            var v = n.scale;
            u.coord = n.dataToCoord(Oa(v, u.tick)), c.push(u);
          }
        }), la(c), la(s), T(bo, function(u) {
          e.get([u, "show"]) && (!n.scale.isBlank() || u === "axisLine") && Lo[u](this.group, e, i, s, l, o, c);
        }, this);
      }
    }, t.type = "angleAxis", t;
  })(Wt)
), Lo = {
  axisLine: function(a, t, e, r, n, i) {
    var o = t.getModel(["axisLine", "lineStyle"]), s = e.getAngleAxis(), l = Math.PI / 180, c = s.getExtent(), u = St(e), v = u ? 0 : 1, d, h = Math.abs(c[1] - c[0]) === 360 ? "Circle" : "Arc";
    i[v] === 0 ? d = new he[h]({
      shape: {
        cx: e.cx,
        cy: e.cy,
        r: i[u],
        startAngle: -c[0] * l,
        endAngle: -c[1] * l,
        clockwise: s.inverse
      },
      style: o.getLineStyle(),
      z2: 1,
      silent: !0
    }) : d = new _n({
      shape: {
        cx: e.cx,
        cy: e.cy,
        r: i[u],
        r0: i[v]
      },
      style: o.getLineStyle(),
      z2: 1,
      silent: !0
    }), d.style.fill = null, a.add(d);
  },
  axisTick: function(a, t, e, r, n, i) {
    var o = t.getModel("axisTick"), s = (o.get("inside") ? -1 : 1) * o.get("length"), l = i[St(e)], c = mt(r, function(u) {
      return new ct({
        shape: bt(e, [l, l + s], u.coord)
      });
    });
    a.add(J(c, {
      style: U(o.getModel("lineStyle").getLineStyle(), {
        stroke: t.get(["axisLine", "lineStyle", "color"])
      })
    }));
  },
  minorTick: function(a, t, e, r, n, i) {
    if (n.length) {
      for (var o = t.getModel("axisTick"), s = t.getModel("minorTick"), l = (o.get("inside") ? -1 : 1) * s.get("length"), c = i[St(e)], u = [], v = 0; v < n.length; v++)
        for (var d = 0; d < n[v].length; d++)
          u.push(new ct({
            shape: bt(e, [c, c + l], n[v][d].coord)
          }));
      a.add(J(u, {
        style: U(s.getModel("lineStyle").getLineStyle(), U(o.getLineStyle(), {
          stroke: t.get(["axisLine", "lineStyle", "color"])
        }))
      }));
    }
  },
  axisLabel: function(a, t, e, r, n, i, o) {
    var s = t.getCategories(!0), l = t.getModel("axisLabel"), c = l.get("margin"), u = t.get("triggerEvent");
    T(o, function(v, d) {
      var h = l, f = v.tick.value, g = i[St(e)], p = e.coordToPoint([g + c, v.coord]), y = e.cx, m = e.cy, x = Math.abs(p[0] - y) / g < 0.3 ? "center" : p[0] > y ? "left" : "right", b = Math.abs(p[1] - m) / g < 0.3 ? "middle" : p[1] > m ? "top" : "bottom";
      if (s && s[f]) {
        var S = s[f];
        ce(S) && S.textStyle && (h = new Sn(S.textStyle, l, l.ecModel));
      }
      var L = new Ln({
        silent: It.isLabelSilent(t),
        style: Cn(h, {
          x: p[0],
          y: p[1],
          fill: h.getTextColor() || t.get(["axisLine", "lineStyle", "color"]),
          text: v.formattedLabel,
          align: x,
          verticalAlign: b
        })
      });
      if (a.add(L), Mn({
        el: L,
        componentModel: t,
        itemName: v.formattedLabel,
        formatterParamsExtra: {
          isTruncated: function() {
            return L.isTruncated;
          },
          value: v.rawLabel,
          tickIndex: d
        }
      }), u) {
        var A = It.makeAxisEventDataBase(t);
        A.targetType = "axisLabel", A.value = v.rawLabel, Nt(L).eventData = A;
      }
    }, this);
  },
  splitLine: function(a, t, e, r, n, i) {
    var o = t.getModel("splitLine"), s = o.getModel("lineStyle"), l = s.get("color"), c = 0;
    l = l instanceof Array ? l : [l];
    for (var u = [], v = 0; v < r.length; v++) {
      var d = c++ % l.length;
      u[d] = u[d] || [], u[d].push(new ct({
        shape: bt(e, i, r[v].coord)
      }));
    }
    for (var v = 0; v < u.length; v++)
      a.add(J(u[v], {
        style: U({
          stroke: l[v % l.length]
        }, s.getLineStyle()),
        silent: !0,
        z: t.get("z")
      }));
  },
  minorSplitLine: function(a, t, e, r, n, i) {
    if (n.length) {
      for (var o = t.getModel("minorSplitLine"), s = o.getModel("lineStyle"), l = [], c = 0; c < n.length; c++)
        for (var u = 0; u < n[c].length; u++)
          l.push(new ct({
            shape: bt(e, i, n[c][u].coord)
          }));
      a.add(J(l, {
        style: s.getLineStyle(),
        silent: !0,
        z: t.get("z")
      }));
    }
  },
  splitArea: function(a, t, e, r, n, i) {
    if (r.length) {
      var o = t.getModel("splitArea"), s = o.getModel("areaStyle"), l = s.get("color"), c = 0;
      l = l instanceof Array ? l : [l];
      for (var u = [], v = Math.PI / 180, d = -r[0].coord * v, h = Math.min(i[0], i[1]), f = Math.max(i[0], i[1]), g = t.get("clockwise"), p = 1, y = r.length; p <= y; p++) {
        var m = p === y ? r[0].coord : r[p].coord, x = c++ % l.length;
        u[x] = u[x] || [], u[x].push(new gt({
          shape: {
            cx: e.cx,
            cy: e.cy,
            r0: h,
            r: f,
            startAngle: d,
            endAngle: -m * v,
            clockwise: g
          },
          silent: !0
        })), d = -m * v;
      }
      for (var p = 0; p < u.length; p++)
        a.add(J(u[p], {
          style: U({
            fill: l[p % l.length]
          }, s.getAreaStyle()),
          silent: !0
        }));
    }
  }
}, Co = ["splitLine", "splitArea", "minorSplitLine"], Mo = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e.axisPointerClass = "PolarAxisPointer", e;
    }
    return t.prototype.render = function(e, r, n) {
      if (this.group.removeAll(), !!e.get("show")) {
        var i = this._axisGroup, o = this._axisGroup = new le();
        this.group.add(o);
        var s = e.axis, l = s.polar, c = l.getAngleAxis(), u = s.getTicksCoords(), v = s.getMinorTicksCoords(), d = c.getExtent()[0], h = s.getExtent(), f = wo(l, e, d), g = new It(e, n, f);
        g.build(), o.add(g.group), ka(i, o, e), T(Co, function(p) {
          e.get([p, "show"]) && !s.scale.isBlank() && _o[p](this.group, e, l, d, h, u, v);
        }, this);
      }
    }, t.type = "radiusAxis", t;
  })(Wt)
), _o = {
  splitLine: function(a, t, e, r, n, i) {
    var o = t.getModel("splitLine"), s = o.getModel("lineStyle"), l = s.get("color"), c = 0, u = e.getAngleAxis(), v = Math.PI / 180, d = u.getExtent(), h = Math.abs(d[1] - d[0]) === 360 ? "Circle" : "Arc";
    l = l instanceof Array ? l : [l];
    for (var f = [], g = 0; g < i.length; g++) {
      var p = c++ % l.length;
      f[p] = f[p] || [], f[p].push(new he[h]({
        shape: {
          cx: e.cx,
          cy: e.cy,
          // ensure circle radius >= 0
          r: Math.max(i[g].coord, 0),
          startAngle: -d[0] * v,
          endAngle: -d[1] * v,
          clockwise: u.inverse
        }
      }));
    }
    for (var g = 0; g < f.length; g++)
      a.add(J(f[g], {
        style: U({
          stroke: l[g % l.length],
          fill: null
        }, s.getLineStyle()),
        silent: !0
      }));
  },
  minorSplitLine: function(a, t, e, r, n, i, o) {
    if (o.length) {
      for (var s = t.getModel("minorSplitLine"), l = s.getModel("lineStyle"), c = [], u = 0; u < o.length; u++)
        for (var v = 0; v < o[u].length; v++)
          c.push(new wn({
            shape: {
              cx: e.cx,
              cy: e.cy,
              r: o[u][v].coord
            }
          }));
      a.add(J(c, {
        style: U({
          fill: null
        }, l.getLineStyle()),
        silent: !0
      }));
    }
  },
  splitArea: function(a, t, e, r, n, i) {
    if (i.length) {
      var o = t.getModel("splitArea"), s = o.getModel("areaStyle"), l = s.get("color"), c = 0;
      l = l instanceof Array ? l : [l];
      for (var u = [], v = i[0].coord, d = 1; d < i.length; d++) {
        var h = c++ % l.length;
        u[h] = u[h] || [], u[h].push(new gt({
          shape: {
            cx: e.cx,
            cy: e.cy,
            r0: v,
            r: i[d].coord,
            startAngle: 0,
            endAngle: Math.PI * 2
          },
          silent: !0
        })), v = i[d].coord;
      }
      for (var d = 0; d < u.length; d++)
        a.add(J(u[d], {
          style: U({
            fill: l[d % l.length]
          }, s.getAreaStyle()),
          silent: !0
        }));
    }
  }
};
function wo(a, t, e) {
  return {
    position: [a.cx, a.cy],
    rotation: e / 180 * Math.PI,
    labelDirection: -1,
    tickDirection: -1,
    nameDirection: 1,
    labelRotate: t.getModel("axisLabel").get("rotate"),
    // Over splitLine and splitArea
    z2: 1
  };
}
var To = ne();
function nr(a) {
  return a.get("stack") || "__ec_stack_" + a.seriesIndex;
}
var Po = Na(K, Do);
function Do(a) {
  var t = ot(K, q);
  re(a, t, function(e) {
    var r = Oo(e, K), n = {};
    nt(e, t, function(i) {
      Io(e, i, r, n);
    });
  });
}
function Io(a, t, e, r) {
  var n = t.getData(), i = nr(t), o = e[i], s = o.offset, l = o.width, c = t.coordinateSystem, u = c.getOtherAxis(a), v = c.cx, d = c.cy, h = t.get("barMinHeight") || 0, f = t.get("barMinAngle") || 0;
  r[i] = r[i] || [];
  for (var g = n.mapDimension(u.dim), p = n.mapDimension(a.dim), y = ya(
    n,
    g
    /* , baseDim */
  ), m = a.dim !== "radius" || !t.get("roundCap", !0), x = u.dataToCoord(Ha(u)), b = 0, S = n.count(); b < S; b++) {
    var L = n.get(g, b), A = n.get(p, b), M = L >= 0 ? "p" : "n", P = x;
    y && (r[i][A] || (r[i][A] = {
      p: x,
      n: x
      // Negative stack
    }), P = r[i][A][M]);
    var _ = void 0, C = void 0, D = void 0, w = void 0;
    if (u.dim === "radius") {
      var B = u.dataToCoord(L) - x, E = a.dataToCoord(A);
      at(B) < h && (B = (B < 0 ? -1 : 1) * h), _ = P, C = P + B, D = E - s, w = D - l, y && (r[i][A][M] = C);
    } else {
      var R = u.dataToCoord(L, m) - x, $ = a.dataToCoord(A);
      at(R) < f && (R = (R < 0 ? -1 : 1) * f), _ = $ + s, C = _ + l, D = P, w = P + R, y && (r[i][A][M] = w);
    }
    n.setItemLayout(b, {
      cx: v,
      cy: d,
      r0: _,
      r: C,
      // Consider that positive angle is anti-clockwise,
      // while positive radian of sector is clockwise
      startAngle: -D * Re / 180,
      endAngle: -w * Re / 180,
      /**
       * Keep the same logic with bar in catesion: use end value to
       * control direction. Notice that if clockwise is true (by
       * default), the sector will always draw clockwisely, no matter
       * whether endAngle is greater or less than startAngle.
       */
      clockwise: D >= w
    });
  }
}
function Oo(a, t) {
  var e = ot(t, q), r = Bt(a, {
    fromStat: {
      key: e
    },
    min: 1
  }).w, n = r, i = 0, o = "20%", s = "30%", l = {};
  nt(a, e, function(p) {
    var y = nr(p);
    l[y] || i++, l[y] = l[y] || {
      width: 0,
      maxWidth: 0
    };
    var m = G(p.get("barWidth"), r), x = G(p.get("barMaxWidth"), r), b = p.get("barGap"), S = p.get("barCategoryGap");
    m && !l[y].width && (m = rt(n, m), l[y].width = m, n -= m), x && (l[y].maxWidth = x), b != null && (s = b), S != null && (o = S);
  });
  var c = {}, u = G(o, r), v = G(s, 1), d = (n - u) / (i + (i - 1) * v);
  d = Y(d, 0), T(l, function(p, y) {
    var m = p.maxWidth;
    m && m < d && (m = rt(m, n), p.width && (m = rt(m, p.width)), n -= m, p.width = m, i--);
  }), d = (n - u) / (i + (i - 1) * v), d = Y(d, 0);
  var h = 0, f;
  T(l, function(p, y) {
    p.width || (p.width = d), f = p, h += p.width * (1 + v);
  }), f && (h -= f.width * v);
  var g = -h / 2;
  return T(l, function(p, y) {
    c[y] = c[y] || {
      offset: g,
      width: p.width
    }, g += p.width * (1 + v);
  }), c;
}
function Eo(a, t) {
  To(a, function() {
    var e = ot(t, q);
    Ua(a, e, t, q), ie(e, ge(e));
  });
}
var Bo = {
  startAngle: 90,
  clockwise: !0,
  splitNumber: 12,
  // A round axis is not suitable for `containShape` in most cases.
  containShape: !1,
  axisLabel: {
    rotate: 0
  }
}, ko = {
  splitNumber: 5
}, Ro = (
  /** @class */
  (function(a) {
    O(t, a);
    function t() {
      var e = a !== null && a.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "polar", t;
  })(Ra)
);
function No(a) {
  Dt(Wa), Wt.registerAxisPointerClass("PolarAxisPointer", lo), a.registerCoordinateSystem("polar", Ao), a.registerComponentModel(vo), a.registerComponentView(Ro), Et(a, "angle", ho, Bo), Et(a, "radius", fo, ko), a.registerComponentView(So), a.registerComponentView(Mo), a.registerLayout(Po), Eo(a, K);
}
var ua = {
  value: "eq",
  // PENDING: not good for literal semantic?
  "<": "lt",
  "<=": "lte",
  ">": "gt",
  ">=": "gte",
  "=": "eq",
  "!=": "ne",
  "<>": "ne"
  // Might be misleading for sake of the difference between '==' and '===',
  // so don't support them.
  // '==': 'eq',
  // '===': 'seq',
  // '!==': 'sne'
  // PENDING: Whether support some common alias "ge", "le", "neq"?
  // ge: 'gte',
  // le: 'lte',
  // neq: 'ne',
}, Go = (
  /** @class */
  (function() {
    function a(t) {
      var e = this._condVal = Mt(t) ? new RegExp(t) : Dn(t) ? t : null;
      if (e == null) {
        var r = "";
        k(r);
      }
    }
    return a.prototype.evaluate = function(t) {
      var e = typeof t;
      return Mt(e) ? this._condVal.test(t) : Aa(e) ? this._condVal.test(t + "") : !1;
    }, a;
  })()
), Wo = (
  /** @class */
  (function() {
    function a() {
    }
    return a.prototype.evaluate = function() {
      return this.value;
    }, a;
  })()
), Vo = (
  /** @class */
  (function() {
    function a() {
    }
    return a.prototype.evaluate = function() {
      for (var t = this.children, e = 0; e < t.length; e++)
        if (!t[e].evaluate())
          return !1;
      return !0;
    }, a;
  })()
), Fo = (
  /** @class */
  (function() {
    function a() {
    }
    return a.prototype.evaluate = function() {
      for (var t = this.children, e = 0; e < t.length; e++)
        if (t[e].evaluate())
          return !0;
      return !1;
    }, a;
  })()
), zo = (
  /** @class */
  (function() {
    function a() {
    }
    return a.prototype.evaluate = function() {
      return !this.child.evaluate();
    }, a;
  })()
), Uo = (
  /** @class */
  (function() {
    function a() {
    }
    return a.prototype.evaluate = function() {
      for (var t = !!this.valueParser, e = this.getValue, r = e(this.valueGetterParam), n = t ? this.valueParser(r) : null, i = 0; i < this.subCondList.length; i++)
        if (!this.subCondList[i].evaluate(t ? n : r))
          return !1;
      return !0;
    }, a;
  })()
);
function Ae(a, t) {
  if (a === !0 || a === !1) {
    var e = new Wo();
    return e.value = a, e;
  }
  var r = "";
  return ir(a) || k(r), a.and ? ca("and", a, t) : a.or ? ca("or", a, t) : a.not ? Ho(a, t) : Yo(a, t);
}
function ca(a, t, e) {
  var r = t[a], n = "";
  X(r) || k(n), r.length || k(n);
  var i = a === "and" ? new Vo() : new Fo();
  return i.children = mt(r, function(o) {
    return Ae(o, e);
  }), i.children.length || k(n), i;
}
function Ho(a, t) {
  var e = a.not, r = "";
  ir(e) || k(r);
  var n = new zo();
  return n.child = Ae(e, t), n.child || k(r), n;
}
function Yo(a, t) {
  for (var e = "", r = t.prepareGetValue(a), n = [], i = ve(a), o = a.parser, s = o ? Va(o) : null, l = 0; l < i.length; l++) {
    var c = i[l];
    if (!(c === "parser" || t.valueGetterAttrMap.get(c))) {
      var u = Rt(ua, c) ? ua[c] : c, v = a[c], d = s ? s(v) : v, h = Pn(u, d) || u === "reg" && new Go(d);
      h || k(e), n.push(h);
    }
  }
  n.length || k(e);
  var f = new Uo();
  return f.valueGetterParam = r, f.valueParser = s, f.getValue = t.getValue, f.subCondList = n, f;
}
function ir(a) {
  return ce(a) && !Tn(a);
}
var Xo = (
  /** @class */
  (function() {
    function a(t, e) {
      this._cond = Ae(t, e);
    }
    return a.prototype.evaluate = function() {
      return this._cond.evaluate();
    }, a;
  })()
);
function $o(a, t) {
  return new Xo(a, t);
}
var Ko = {
  type: "echarts:filter",
  // PENDING: enhance to filter by index rather than create new data
  transform: function(a) {
    for (var t = a.upstream, e, r = $o(a.config, {
      valueGetterAttrMap: ee({
        dimension: !0
      }),
      prepareGetValue: function(s) {
        var l = "", c = s.dimension;
        Rt(s, "dimension") || k(l);
        var u = t.getDimensionInfo(c);
        return u || k(l), {
          dimIdx: u.index
        };
      },
      getValue: function(s) {
        return t.retrieveValueFromItem(e, s.dimIdx);
      }
    }), n = [], i = 0, o = t.count(); i < o; i++)
      e = t.getRawDataItem(i), r.evaluate() && n.push(e);
    return {
      data: n
    };
  }
}, Zo = {
  type: "echarts:sort",
  transform: function(a) {
    var t = a.upstream, e = a.config, r = "", n = En(e);
    n.length || k(r);
    var i = [];
    T(n, function(u) {
      var v = u.dimension, d = u.order, h = u.parser, f = u.incomparable;
      if (v == null && k(r), d !== "asc" && d !== "desc" && k(r), f && f !== "min" && f !== "max") {
        var g = "";
        k(g);
      }
      if (d !== "asc" && d !== "desc") {
        var p = "";
        k(p);
      }
      var y = t.getDimensionInfo(v);
      y || k(r);
      var m = h ? Va(h) : null;
      h && !m && k(r), i.push({
        dimIdx: y.index,
        parser: m,
        comparator: new In(d, f)
      });
    });
    var o = t.sourceFormat;
    o !== Ga && o !== On && k(r);
    for (var s = [], l = 0, c = t.count(); l < c; l++)
      s.push(t.getRawDataItem(l));
    return s.sort(function(u, v) {
      for (var d = 0; d < i.length; d++) {
        var h = i[d], f = t.retrieveValueFromItem(u, h.dimIdx), g = t.retrieveValueFromItem(v, h.dimIdx);
        h.parser && (f = h.parser(f), g = h.parser(g));
        var p = h.comparator.evaluate(f, g);
        if (p !== 0)
          return p;
      }
      return 0;
    }), {
      data: s
    };
  }
};
function qo(a) {
  a.registerTransform(Ko), a.registerTransform(Zo);
}
Dt([
  Mi,
  oo,
  Rn,
  so,
  Nn,
  No,
  Gn,
  qo,
  Wn
]);
function ts({ echartsRef: a, option: t }) {
  return /* @__PURE__ */ or.jsx(
    Bn,
    {
      ref: a,
      echarts: kn,
      option: t,
      style: { width: "100%", height: "100%" },
      notMerge: !0
    }
  );
}
export {
  ts as default
};
