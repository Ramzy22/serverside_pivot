import { j as Ke } from "./index-D582oiHq.js";
import "./charts-DthW1Q8W.js";
import { r as Ft, i as G, a as F, S as tt, R as et, b as vt, c as me, _ as L, d as Ae, e as w, f as _t, g as qe, P as Et, h as Ht, j as Se, m as U, p as Qe, t as Je, k as ct, u as q, s as be, l as tr, n as Yt, G as It, o as ft, C as Ce, q as rt, v as er, w as rr, x as ar, y as Me, z as we, A as ir, B as Rt, D as nr, E as or, F as sr, H as lr, I as pt, J as Q, K as kt, L as Le, M as O, N as H, O as ur, Q as vr, T as cr, U as hr, V as dr, W as Xt, X as $t, Y as gr, Z as Y, $ as fr, a0 as pr, a1 as xt, a2 as yr, a3 as xr, a4 as Bt, a5 as ut, a6 as Te, a7 as Zt, a8 as Ut, a9 as Mt, aa as De, ab as wt, ac as ht, ad as mr, ae as Pe, af as _e, ag as Ar, ah as J, ai as Ee, aj as yt, ak as Z, al as jt, am as Ie, an as Kt, ao as Sr, ap as Ot, aq as W, ar as br, as as mt, at as Re, au as E, av as dt, aw as ke, ax as Cr, ay as Mr, az as wr, aA as qt, aB as Lr, aC as Tr, aD as Dr, aE as Pr, aF as _r, aG as Er, aH as Ir, aI as V, aJ as Rr, aK as kr, aL as Br, aM as Or, aN as Gr, aO as Wr, aP as Nr, aQ as Vr, aR as Be, aS as Oe, aT as zr, aU as Fr, aV as Hr, aW as Yr, aX as Xr, aY as $r, aZ as Zr, a_ as Ur, a$ as jr, b0 as Kr, b1 as qr } from "./installCanvasRenderer-BAg3XhZs.js";
function Qr(r, t) {
  var e = r.mapDimensionsAll("defaultedLabel"), a = e.length;
  if (a === 1) {
    var i = Ft(r, t, e[0]);
    return i != null ? i + "" : null;
  } else if (a) {
    for (var n = [], o = 0; o < e.length; o++)
      n.push(Ft(r, t, e[o]));
    return n.join(" ");
  }
}
function Jr(r, t) {
  var e = r.mapDimensionsAll("defaultedLabel");
  if (!G(t))
    return t + "";
  for (var a = [], i = 0; i < e.length; i++) {
    var n = r.getDimensionIndex(e[i]);
    n >= 0 && a.push(t[n]);
  }
  return a.join(" ");
}
function ta(r, t, e, a, i) {
  var n = r.getArea(), o = n.x, s = n.y, v = n.width, u = n.height, l = e.get(["lineStyle", "width"]) || 0;
  o -= l / 2, s -= l / 2, v += l, u += l, v = Math.ceil(v), o !== Math.floor(o) && (o = Math.floor(o), v++);
  var c = new et({
    shape: {
      x: o,
      y: s,
      width: v,
      height: u
    }
  });
  return c;
}
function ea(r, t, e) {
  var a = r.getArea(), i = F(a.r0, 1), n = F(a.r, 1), o = new tt({
    shape: {
      cx: F(r.cx, 1),
      cy: F(r.cy, 1),
      r0: i,
      r: n,
      startAngle: a.startAngle,
      endAngle: a.endAngle,
      clockwise: a.clockwise
    }
  });
  return o;
}
function ra(r, t, e, a, i) {
  if (r) {
    if (r.type === "polar")
      return ea(r);
    if (r.type === "cartesian2d")
      return ta(r, t, e);
  } else return null;
  return null;
}
function Ge(r, t) {
  return r.type === t;
}
var aa = {
  average: function(r) {
    for (var t = 0, e = 0, a = 0; a < r.length; a++)
      isNaN(r[a]) || (t += r[a], e++);
    return e === 0 ? NaN : t / e;
  },
  sum: function(r) {
    for (var t = 0, e = 0; e < r.length; e++)
      t += r[e] || 0;
    return t;
  },
  max: function(r) {
    for (var t = -1 / 0, e = 0; e < r.length; e++)
      r[e] > t && (t = r[e]);
    return isFinite(t) ? t : NaN;
  },
  min: function(r) {
    for (var t = 1 / 0, e = 0; e < r.length; e++)
      r[e] < t && (t = r[e]);
    return isFinite(t) ? t : NaN;
  },
  // TODO
  // Median
  nearest: function(r) {
    return r[0];
  }
}, ia = function(r) {
  return Math.round(r.length / 2);
};
function na(r) {
  return {
    seriesType: r,
    // FIXME:TS never used, so comment it
    // modifyOutputEnd: true,
    reset: function(t, e, a) {
      var i = t.getData(), n = t.get("sampling"), o = t.coordinateSystem, s = i.count();
      if (s > 10 && o.type === "cartesian2d" && n) {
        var v = o.getBaseAxis(), u = o.getOtherAxis(v), l = v.getExtent(), c = a.getDevicePixelRatio(), d = Math.abs(l[1] - l[0]) * (c || 1), g = Math.round(s / d);
        if (isFinite(g) && g > 1) {
          n === "lttb" ? t.setData(i.lttbDownSample(i.mapDimension(u.dim), 1 / g)) : n === "minmax" && t.setData(i.minmaxDownSample(i.mapDimension(u.dim), 1 / g));
          var h = void 0;
          vt(n) ? h = aa[n] : me(n) && (h = n), h && t.setData(i.downSample(i.mapDimension(u.dim), 1 / g, h, ia));
        }
      }
    }
  };
}
var Lt = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.prototype.getInitialData = function(e, a) {
      return Ae(null, this, {
        useEncodeDefaulter: !0
      });
    }, t.prototype.getMarkerPosition = function(e, a, i) {
      var n = this.coordinateSystem;
      if (n && n.clampData) {
        var o = n.clampData(e), s = n.dataToPoint(o);
        if (i)
          w(n.getAxes(), function(d, g) {
            if (d.type === "category" && a != null) {
              var h = d.getTicksCoords(), f = d.getTickModel().get("alignWithLabel"), p = o[g], y = a[g] === "x1" || a[g] === "y1";
              if (y && !f && (p += 1), h.length < 2)
                return;
              if (h.length === 2) {
                s[g] = d.toGlobalCoord(d.getExtent()[y ? 1 : 0]);
                return;
              }
              for (var x = void 0, m = void 0, A = 1, S = 0; S < h.length; S++) {
                var b = h[S].coord, C = S === h.length - 1 ? h[S - 1].tickValue + A : h[S].tickValue;
                if (C === p) {
                  m = b;
                  break;
                } else if (C < p)
                  x = b;
                else if (x != null && C > p) {
                  m = (b + x) / 2;
                  break;
                }
                S === 1 && (A = C - h[0].tickValue);
              }
              m == null && (x ? x && (m = h[h.length - 1].coord) : m = h[0].coord), s[g] = d.toGlobalCoord(m);
            }
          });
        else {
          var v = this.getData(), u = v.getLayout("offset"), l = v.getLayout("size"), c = n.getBaseAxis().isHorizontal() ? 0 : 1;
          s[c] += u + l / 2;
        }
        return s;
      }
      return [NaN, NaN];
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
      progressiveChunkMode: "mod"
    }, t;
  })(_t)
);
_t.registerClass(Lt);
var oa = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.prototype.getInitialData = function() {
      return Ae(null, this, {
        useEncodeDefaulter: !0,
        createInvertedIndices: !!this.get("realtimeSort", !0) || null
      });
    }, t.prototype.getProgressive = function() {
      return this.get("large") ? this.get("progressive") : !1;
    }, t.prototype.getProgressiveThreshold = function() {
      var e = this.get("progressiveThreshold"), a = this.get("largeThreshold");
      return a > e && (e = a), e;
    }, t.prototype.brushSelector = function(e, a, i) {
      return i.rect(a.getItemLayout(e));
    }, t.type = "series.bar", t.dependencies = ["grid", "polar"], t.defaultOption = qe(Lt.defaultOption, {
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
          borderColor: "#212121"
        }
      },
      realtimeSort: !1
    }), t;
  })(Lt)
), sa = (
  /** @class */
  /* @__PURE__ */ (function() {
    function r() {
      this.cx = 0, this.cy = 0, this.r0 = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0;
    }
    return r;
  })()
), Qt = (
  /** @class */
  (function(r) {
    L(t, r);
    function t(e) {
      var a = r.call(this, e) || this;
      return a.type = "sausage", a;
    }
    return t.prototype.getDefaultShape = function() {
      return new sa();
    }, t.prototype.buildPath = function(e, a) {
      var i = a.cx, n = a.cy, o = Math.max(a.r0 || 0, 0), s = Math.max(a.r, 0), v = (s - o) * 0.5, u = o + v, l = a.startAngle, c = a.endAngle, d = a.clockwise, g = Math.PI * 2, h = d ? c - l < g : l - c < g;
      h || (l = c - (d ? g : -g));
      var f = Math.cos(l), p = Math.sin(l), y = Math.cos(c), x = Math.sin(c);
      h ? (e.moveTo(f * o + i, p * o + n), e.arc(f * u + i, p * u + n, v, -Math.PI + l, l, !d)) : e.moveTo(f * s + i, p * s + n), e.arc(i, n, s, l, c, !d), e.arc(y * u + i, x * u + n, v, c - Math.PI * 2, c - Math.PI, !d), o !== 0 && e.arc(i, n, o, c, l, d);
    }, t;
  })(Et)
);
function la(r, t) {
  t = t || {};
  var e = t.isRoundCap;
  return function(a, i, n) {
    var o = i.position;
    if (!o || o instanceof Array)
      return Ht(a, i, n);
    var s = r(o), v = i.distance != null ? i.distance : 5, u = this.shape, l = u.cx, c = u.cy, d = u.r, g = u.r0, h = (d + g) / 2, f = u.startAngle, p = u.endAngle, y = (f + p) / 2, x = e ? Math.abs(d - g) / 2 : 0, m = Math.cos, A = Math.sin, S = l + d * m(f), b = c + d * A(f), C = "left", D = "top";
    switch (s) {
      case "startArc":
        S = l + (g - v) * m(y), b = c + (g - v) * A(y), C = "center", D = "top";
        break;
      case "insideStartArc":
        S = l + (g + v) * m(y), b = c + (g + v) * A(y), C = "center", D = "bottom";
        break;
      case "startAngle":
        S = l + h * m(f) + it(f, v + x, !1), b = c + h * A(f) + nt(f, v + x, !1), C = "right", D = "middle";
        break;
      case "insideStartAngle":
        S = l + h * m(f) + it(f, -v + x, !1), b = c + h * A(f) + nt(f, -v + x, !1), C = "left", D = "middle";
        break;
      case "middle":
        S = l + h * m(y), b = c + h * A(y), C = "center", D = "middle";
        break;
      case "endArc":
        S = l + (d + v) * m(y), b = c + (d + v) * A(y), C = "center", D = "bottom";
        break;
      case "insideEndArc":
        S = l + (d - v) * m(y), b = c + (d - v) * A(y), C = "center", D = "top";
        break;
      case "endAngle":
        S = l + h * m(p) + it(p, v + x, !0), b = c + h * A(p) + nt(p, v + x, !0), C = "left", D = "middle";
        break;
      case "insideEndAngle":
        S = l + h * m(p) + it(p, -v + x, !0), b = c + h * A(p) + nt(p, -v + x, !0), C = "right", D = "middle";
        break;
      default:
        return Ht(a, i, n);
    }
    return a = a || {}, a.x = S, a.y = b, a.align = C, a.verticalAlign = D, a;
  };
}
function ua(r, t, e, a) {
  if (Se(a)) {
    r.setTextConfig({
      rotation: a
    });
    return;
  } else if (G(t)) {
    r.setTextConfig({
      rotation: 0
    });
    return;
  }
  var i = r.shape, n = i.clockwise ? i.startAngle : i.endAngle, o = i.clockwise ? i.endAngle : i.startAngle, s = (n + o) / 2, v, u = e(t);
  switch (u) {
    case "startArc":
    case "insideStartArc":
    case "middle":
    case "insideEndArc":
    case "endArc":
      v = s;
      break;
    case "startAngle":
    case "insideStartAngle":
      v = n;
      break;
    case "endAngle":
    case "insideEndAngle":
      v = o;
      break;
    default:
      r.setTextConfig({
        rotation: 0
      });
      return;
  }
  var l = Math.PI * 1.5 - v;
  u === "middle" && l > Math.PI / 2 && l < Math.PI * 1.5 && (l -= Math.PI), r.setTextConfig({
    rotation: l
  });
}
function it(r, t, e) {
  return t * Math.sin(r) * (e ? -1 : 1);
}
function nt(r, t, e) {
  return t * Math.cos(r) * (e ? 1 : -1);
}
function va(r, t, e) {
  var a = r.get("borderRadius");
  if (a == null)
    return {
      cornerRadius: 0
    };
  G(a) || (a = [a, a, a, a]);
  var i = Math.abs(t.r || 0 - t.r0 || 0);
  return {
    cornerRadius: U(a, function(n) {
      return Qe(n, i);
    })
  };
}
var At = Math.max, St = Math.min;
function ca(r, t) {
  var e = r.getArea && r.getArea();
  if (Ge(r, "cartesian2d")) {
    var a = r.getBaseAxis();
    if (a.type !== "category" || !a.onBand) {
      var i = t.getLayout("bandWidth");
      a.isHorizontal() ? (e.x -= i, e.width += i * 2) : (e.y -= i, e.height += i * 2);
    }
  }
  return e;
}
var ha = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r.call(this) || this;
      return e.type = t.type, e._isFirstFrame = !0, e;
    }
    return t.prototype.render = function(e, a, i, n) {
      this._model = e, this._removeOnRenderedListener(i), this._updateDrawMode(e);
      var o = e.get("coordinateSystem");
      (o === "cartesian2d" || o === "polar") && (this._progressiveEls = null, this._isLargeDraw ? this._renderLarge(e, a, i) : this._renderNormal(e, a, i, n));
    }, t.prototype.incrementalPrepareRender = function(e) {
      this._clear(), this._updateDrawMode(e), this._updateLargeClip(e);
    }, t.prototype.incrementalRender = function(e, a) {
      this._progressiveEls = [], this._incrementalRenderLarge(e, a);
    }, t.prototype.eachRendered = function(e) {
      Je(this._progressiveEls || this.group, e);
    }, t.prototype._updateDrawMode = function(e) {
      var a = e.pipelineContext.large;
      (this._isLargeDraw == null || a !== this._isLargeDraw) && (this._isLargeDraw = a, this._clear());
    }, t.prototype._renderNormal = function(e, a, i, n) {
      var o = this.group, s = e.getData(), v = this._data, u = e.coordinateSystem, l = u.getBaseAxis(), c;
      u.type === "cartesian2d" ? c = l.isHorizontal() : u.type === "polar" && (c = l.dim === "angle");
      var d = e.isAnimationEnabled() ? e : null, g = da(e, u);
      g && this._enableRealtimeSort(g, s, i);
      var h = e.get("clip", !0) || g, f = ca(u, s);
      o.removeClipPath();
      var p = e.get("roundCap", !0), y = e.get("showBackground", !0), x = e.getModel("backgroundStyle"), m = x.get("borderRadius") || 0, A = [], S = this._backgroundEls, b = n && n.isInitSort, C = n && n.type === "changeAxisOrder";
      function D(M) {
        var k = ot[u.type](s, M), T = Aa(u, c, k);
        return T.useStyle(x.getItemStyle()), u.type === "cartesian2d" ? T.setShape("r", m) : T.setShape("cornerRadius", m), A[M] = T, T;
      }
      s.diff(v).add(function(M) {
        var k = s.getItemModel(M), T = ot[u.type](s, M, k);
        if (y && D(M), !(!s.hasValue(M) || !ae[u.type](T))) {
          var B = !1;
          h && (B = Jt[u.type](f, T));
          var P = te[u.type](e, s, M, T, c, d, l.model, !1, p);
          g && (P.forceLabelAnimation = !0), ie(P, s, M, k, T, e, c, u.type === "polar"), b ? P.attr({
            shape: T
          }) : g ? ee(g, d, P, T, M, c, !1, !1) : ct(P, {
            shape: T
          }, e, M), s.setItemGraphicEl(M, P), o.add(P), P.ignore = B;
        }
      }).update(function(M, k) {
        var T = s.getItemModel(M), B = ot[u.type](s, M, T);
        if (y) {
          var P = void 0;
          S.length === 0 ? P = D(k) : (P = S[k], P.useStyle(x.getItemStyle()), u.type === "cartesian2d" ? P.setShape("r", m) : P.setShape("cornerRadius", m), A[M] = P);
          var X = ot[u.type](s, M), $ = Ne(c, X, u);
          q(P, {
            shape: $
          }, d, M);
        }
        var _ = v.getItemGraphicEl(k);
        if (!s.hasValue(M) || !ae[u.type](B)) {
          o.remove(_);
          return;
        }
        var N = !1;
        if (h && (N = Jt[u.type](f, B), N && o.remove(_)), _ ? be(_) : _ = te[u.type](e, s, M, B, c, d, l.model, !!_, p), g && (_.forceLabelAnimation = !0), C) {
          var z = _.getTextContent();
          if (z) {
            var j = tr(z);
            j.prevValue != null && (j.prevValue = j.value);
          }
        } else
          ie(_, s, M, T, B, e, c, u.type === "polar");
        b ? _.attr({
          shape: B
        }) : g ? ee(g, d, _, B, M, c, !0, C) : q(_, {
          shape: B
        }, e, M, null), s.setItemGraphicEl(M, _), _.ignore = N, o.add(_);
      }).remove(function(M) {
        var k = v.getItemGraphicEl(M);
        k && Yt(k, e, M);
      }).execute();
      var I = this._backgroundGroup || (this._backgroundGroup = new It());
      I.removeAll();
      for (var R = 0; R < A.length; ++R)
        I.add(A[R]);
      o.add(I), this._backgroundEls = A, this._data = s;
    }, t.prototype._renderLarge = function(e, a, i) {
      this._clear(), oe(e, this.group), this._updateLargeClip(e);
    }, t.prototype._incrementalRenderLarge = function(e, a) {
      this._removeBackground(), oe(a, this.group, this._progressiveEls, !0);
    }, t.prototype._updateLargeClip = function(e) {
      var a = e.get("clip", !0) && ra(e.coordinateSystem, !1, e), i = this.group;
      a ? i.setClipPath(a) : i.removeClipPath();
    }, t.prototype._enableRealtimeSort = function(e, a, i) {
      var n = this;
      if (a.count()) {
        var o = e.baseAxis;
        if (this._isFirstFrame)
          this._dispatchInitSort(a, e, i), this._isFirstFrame = !1;
        else {
          var s = function(v) {
            var u = a.getItemGraphicEl(v), l = u && u.shape;
            return l && // The result should be consistent with the initial sort by data value.
            // Do not support the case that both positive and negative exist.
            Math.abs(o.isHorizontal() ? l.height : l.width) || 0;
          };
          this._onRendered = function() {
            n._updateSortWithinSameData(a, s, o, i);
          }, i.getZr().on("rendered", this._onRendered);
        }
      }
    }, t.prototype._dataSort = function(e, a, i) {
      var n = [];
      return e.each(e.mapDimension(a.dim), function(o, s) {
        var v = i(s);
        v = v ?? NaN, n.push({
          dataIndex: s,
          mappedValue: v,
          ordinalNumber: o
        });
      }), n.sort(function(o, s) {
        return s.mappedValue - o.mappedValue;
      }), {
        ordinalNumbers: U(n, function(o) {
          return o.ordinalNumber;
        })
      };
    }, t.prototype._isOrderChangedWithinSameData = function(e, a, i) {
      for (var n = i.scale, o = e.mapDimension(i.dim), s = Number.MAX_VALUE, v = 0, u = n.getOrdinalMeta().categories.length; v < u; ++v) {
        var l = e.rawIndexOf(o, n.getRawOrdinalNumber(v)), c = l < 0 ? Number.MIN_VALUE : a(e.indexOfRawIndex(l));
        if (c > s)
          return !0;
        s = c;
      }
      return !1;
    }, t.prototype._isOrderDifferentInView = function(e, a) {
      for (var i = a.scale, n = i.getExtent(), o = Math.max(0, n[0]), s = Math.min(n[1], i.getOrdinalMeta().categories.length - 1); o <= s; ++o)
        if (e.ordinalNumbers[o] !== i.getRawOrdinalNumber(o))
          return !0;
    }, t.prototype._updateSortWithinSameData = function(e, a, i, n) {
      if (this._isOrderChangedWithinSameData(e, a, i)) {
        var o = this._dataSort(e, i, a);
        this._isOrderDifferentInView(o, i) && (this._removeOnRenderedListener(n), n.dispatchAction({
          type: "changeAxisOrder",
          componentType: i.dim + "Axis",
          axisId: i.index,
          sortInfo: o
        }));
      }
    }, t.prototype._dispatchInitSort = function(e, a, i) {
      var n = a.baseAxis, o = this._dataSort(e, n, function(s) {
        return e.get(e.mapDimension(a.otherAxis.dim), s);
      });
      i.dispatchAction({
        type: "changeAxisOrder",
        componentType: n.dim + "Axis",
        isInitSort: !0,
        axisId: n.index,
        sortInfo: o
      });
    }, t.prototype.remove = function(e, a) {
      this._clear(this._model), this._removeOnRenderedListener(a);
    }, t.prototype.dispose = function(e, a) {
      this._removeOnRenderedListener(a);
    }, t.prototype._removeOnRenderedListener = function(e) {
      this._onRendered && (e.getZr().off("rendered", this._onRendered), this._onRendered = null);
    }, t.prototype._clear = function(e) {
      var a = this.group, i = this._data;
      e && e.isAnimationEnabled() && i && !this._isLargeDraw ? (this._removeBackground(), this._backgroundEls = [], i.eachItemGraphicEl(function(n) {
        Yt(n, e, ft(n).dataIndex);
      })) : a.removeAll(), this._data = null, this._isFirstFrame = !0;
    }, t.prototype._removeBackground = function() {
      this.group.remove(this._backgroundGroup), this._backgroundGroup = null;
    }, t.type = "bar", t;
  })(Ce)
), Jt = {
  cartesian2d: function(r, t) {
    var e = t.width < 0 ? -1 : 1, a = t.height < 0 ? -1 : 1;
    e < 0 && (t.x += t.width, t.width = -t.width), a < 0 && (t.y += t.height, t.height = -t.height);
    var i = r.x + r.width, n = r.y + r.height, o = At(t.x, r.x), s = St(t.x + t.width, i), v = At(t.y, r.y), u = St(t.y + t.height, n), l = s < o, c = u < v;
    return t.x = l && o > i ? s : o, t.y = c && v > n ? u : v, t.width = l ? 0 : s - o, t.height = c ? 0 : u - v, e < 0 && (t.x += t.width, t.width = -t.width), a < 0 && (t.y += t.height, t.height = -t.height), l || c;
  },
  polar: function(r, t) {
    var e = t.r0 <= t.r ? 1 : -1;
    if (e < 0) {
      var a = t.r;
      t.r = t.r0, t.r0 = a;
    }
    var i = St(t.r, r.r), n = At(t.r0, r.r0);
    t.r = i, t.r0 = n;
    var o = i - n < 0;
    if (e < 0) {
      var a = t.r;
      t.r = t.r0, t.r0 = a;
    }
    return o;
  }
}, te = {
  cartesian2d: function(r, t, e, a, i, n, o, s, v) {
    var u = new et({
      shape: rt({}, a),
      z2: 1
    });
    if (u.__dataIndex = e, u.name = "item", n) {
      var l = u.shape, c = i ? "height" : "width";
      l[c] = 0;
    }
    return u;
  },
  polar: function(r, t, e, a, i, n, o, s, v) {
    var u = !i && v ? Qt : tt, l = new u({
      shape: a,
      z2: 1
    });
    l.name = "item";
    var c = We(i);
    if (l.calculateTextPosition = la(c, {
      isRoundCap: u === Qt
    }), n) {
      var d = l.shape, g = i ? "r" : "endAngle", h = {};
      d[g] = i ? a.r0 : a.startAngle, h[g] = a[g], (s ? q : ct)(l, {
        shape: h
        // __value: typeof dataValue === 'string' ? parseInt(dataValue, 10) : dataValue
      }, n);
    }
    return l;
  }
};
function da(r, t) {
  var e = r.get("realtimeSort", !0), a = t.getBaseAxis();
  if (e && a.type === "category" && t.type === "cartesian2d")
    return {
      baseAxis: a,
      otherAxis: t.getOtherAxis(a)
    };
}
function ee(r, t, e, a, i, n, o, s) {
  var v, u;
  n ? (u = {
    x: a.x,
    width: a.width
  }, v = {
    y: a.y,
    height: a.height
  }) : (u = {
    y: a.y,
    height: a.height
  }, v = {
    x: a.x,
    width: a.width
  }), s || (o ? q : ct)(e, {
    shape: v
  }, t, i, null);
  var l = t ? r.baseAxis.model : null;
  (o ? q : ct)(e, {
    shape: u
  }, l, i);
}
function re(r, t) {
  for (var e = 0; e < t.length; e++)
    if (!isFinite(r[t[e]]))
      return !0;
  return !1;
}
var ga = ["x", "y", "width", "height"], fa = ["cx", "cy", "r", "startAngle", "endAngle"], ae = {
  cartesian2d: function(r) {
    return !re(r, ga);
  },
  polar: function(r) {
    return !re(r, fa);
  }
}, ot = {
  // itemModel is only used to get borderWidth, which is not needed
  // when calculating bar background layout.
  cartesian2d: function(r, t, e) {
    var a = r.getItemLayout(t), i = e ? ya(e, a) : 0, n = a.width > 0 ? 1 : -1, o = a.height > 0 ? 1 : -1;
    return {
      x: a.x + n * i / 2,
      y: a.y + o * i / 2,
      width: a.width - n * i,
      height: a.height - o * i
    };
  },
  polar: function(r, t, e) {
    var a = r.getItemLayout(t);
    return {
      cx: a.cx,
      cy: a.cy,
      r0: a.r0,
      r: a.r,
      startAngle: a.startAngle,
      endAngle: a.endAngle,
      clockwise: a.clockwise
    };
  }
};
function pa(r) {
  return r.startAngle != null && r.endAngle != null && r.startAngle === r.endAngle;
}
function We(r) {
  return /* @__PURE__ */ (function(t) {
    var e = t ? "Arc" : "Angle";
    return function(a) {
      switch (a) {
        case "start":
        case "insideStart":
        case "end":
        case "insideEnd":
          return a + e;
        default:
          return a;
      }
    };
  })(r);
}
function ie(r, t, e, a, i, n, o, s) {
  var v = t.getItemVisual(e, "style");
  if (s) {
    if (!n.get("roundCap")) {
      var l = r.shape, c = va(a.getModel("itemStyle"), l);
      rt(l, c), r.setShape(l);
    }
  } else {
    var u = a.get(["itemStyle", "borderRadius"]) || 0;
    r.setShape("r", u);
  }
  r.useStyle(v);
  var d = a.getShallow("cursor");
  d && r.attr("cursor", d);
  var g = s ? o ? i.r >= i.r0 ? "endArc" : "startArc" : i.endAngle >= i.startAngle ? "endAngle" : "startAngle" : o ? i.height >= 0 ? "bottom" : "top" : i.width >= 0 ? "right" : "left", h = er(a);
  rr(r, h, {
    labelFetcher: n,
    labelDataIndex: e,
    defaultText: Qr(n.getData(), e),
    inheritColor: v.fill,
    defaultOpacity: v.opacity,
    defaultOutsidePosition: g
  });
  var f = r.getTextContent();
  if (s && f) {
    var p = a.get(["label", "position"]);
    r.textConfig.inside = p === "middle" ? !0 : null, ua(r, p === "outside" ? g : p, We(o), a.get(["label", "rotate"]));
  }
  ar(f, h, n.getRawValue(e), function(x) {
    return Jr(t, x);
  });
  var y = a.getModel(["emphasis"]);
  Me(r, y.get("focus"), y.get("blurScope"), y.get("disabled")), we(r, a), pa(i) && (r.style.fill = "none", r.style.stroke = "none", w(r.states, function(x) {
    x.style && (x.style.fill = x.style.stroke = "none");
  }));
}
function ya(r, t) {
  var e = r.get(["itemStyle", "borderColor"]);
  if (!e || e === "none")
    return 0;
  var a = r.get(["itemStyle", "borderWidth"]) || 0, i = isNaN(t.width) ? Number.MAX_VALUE : Math.abs(t.width), n = isNaN(t.height) ? Number.MAX_VALUE : Math.abs(t.height);
  return Math.min(a, i, n);
}
var xa = (
  /** @class */
  /* @__PURE__ */ (function() {
    function r() {
    }
    return r;
  })()
), ne = (
  /** @class */
  (function(r) {
    L(t, r);
    function t(e) {
      var a = r.call(this, e) || this;
      return a.type = "largeBar", a;
    }
    return t.prototype.getDefaultShape = function() {
      return new xa();
    }, t.prototype.buildPath = function(e, a) {
      for (var i = a.points, n = this.baseDimIdx, o = 1 - this.baseDimIdx, s = [], v = [], u = this.barWidth, l = 0; l < i.length; l += 3)
        v[n] = u, v[o] = i[l + 2], s[n] = i[l + n], s[o] = i[l + o], e.rect(s[0], s[1], v[0], v[1]);
    }, t;
  })(Et)
);
function oe(r, t, e, a) {
  var i = r.getData(), n = i.getLayout("valueAxisHorizontal") ? 1 : 0, o = i.getLayout("largeDataIndices"), s = i.getLayout("size"), v = r.getModel("backgroundStyle"), u = i.getLayout("largeBackgroundPoints");
  if (u) {
    var l = new ne({
      shape: {
        points: u
      },
      incremental: !!a,
      silent: !0,
      z2: 0
    });
    l.baseDimIdx = n, l.largeDataIndices = o, l.barWidth = s, l.useStyle(v.getItemStyle()), t.add(l), e && e.push(l);
  }
  var c = new ne({
    shape: {
      points: i.getLayout("largePoints")
    },
    incremental: !!a,
    ignoreCoarsePointer: !0,
    z2: 1
  });
  c.baseDimIdx = n, c.largeDataIndices = o, c.barWidth = s, t.add(c), c.useStyle(i.getVisual("style")), c.style.stroke = null, ft(c).seriesIndex = r.seriesIndex, r.get("silent") || (c.on("mousedown", se), c.on("mousemove", se)), e && e.push(c);
}
var se = ir(function(r) {
  var t = this, e = ma(t, r.offsetX, r.offsetY);
  ft(t).dataIndex = e >= 0 ? e : null;
}, 30, !1);
function ma(r, t, e) {
  for (var a = r.baseDimIdx, i = 1 - a, n = r.shape.points, o = r.largeDataIndices, s = [], v = [], u = r.barWidth, l = 0, c = n.length / 3; l < c; l++) {
    var d = l * 3;
    if (v[a] = u, v[i] = n[d + 2], s[a] = n[d + a], s[i] = n[d + i], v[i] < 0 && (s[i] += v[i], v[i] = -v[i]), t >= s[0] && t <= s[0] + v[0] && e >= s[1] && e <= s[1] + v[1])
      return o[l];
  }
  return -1;
}
function Ne(r, t, e) {
  if (Ge(e, "cartesian2d")) {
    var a = t, i = e.getArea();
    return {
      x: r ? a.x : i.x,
      y: r ? i.y : a.y,
      width: r ? a.width : i.width,
      height: r ? i.height : a.height
    };
  } else {
    var i = e.getArea(), n = t;
    return {
      cx: i.cx,
      cy: i.cy,
      r0: r ? i.r0 : n.r0,
      r: r ? i.r : n.r,
      startAngle: r ? n.startAngle : 0,
      endAngle: r ? n.endAngle : Math.PI * 2
    };
  }
}
function Aa(r, t, e) {
  var a = r.type === "polar" ? tt : et;
  return new a({
    shape: Ne(t, e, r),
    silent: !0,
    z2: 0
  });
}
function Sa(r) {
  r.registerChartView(ha), r.registerSeriesModel(oa), r.registerLayout(r.PRIORITY.VISUAL.LAYOUT, Rt(nr, "bar")), r.registerLayout(r.PRIORITY.VISUAL.PROGRESSIVE_LAYOUT, or("bar")), r.registerProcessor(r.PRIORITY.PROCESSOR.STATISTIC, na("bar")), r.registerAction({
    type: "changeAxisOrder",
    event: "changeAxisOrder",
    update: "update"
  }, function(t, e) {
    var a = t.componentType || "series";
    e.eachComponent({
      mainType: a,
      query: t
    }, function(i) {
      t.sortInfo && i.axis.setCategorySortInfo(t.sortInfo);
    });
  });
}
function ba(r, t, e) {
  t = G(t) && {
    coordDimensions: t
  } || rt({
    encodeDefine: r.getEncode()
  }, t);
  var a = r.getSource(), i = sr(a, t).dimensions, n = new lr(i, r);
  return n.initData(a, e), n;
}
var Ca = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.type = "grid", t.dependencies = ["xAxis", "yAxis"], t.layoutMode = "box", t.defaultOption = {
      show: !1,
      // zlevel: 0,
      z: 0,
      left: "10%",
      top: 60,
      right: "10%",
      bottom: 70,
      // If grid size contain label
      containLabel: !1,
      // width: {totalWidth} - left - right,
      // height: {totalHeight} - top - bottom,
      backgroundColor: "rgba(0,0,0,0)",
      borderWidth: 1,
      borderColor: "#ccc"
    }, t;
  })(pt)
), Tt = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.getCoordSysModel = function() {
      return this.getReferringComponents("grid", Q).models[0];
    }, t.type = "cartesian2dAxis", t;
  })(pt)
);
kt(Tt, Le);
var Ve = {
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
  nameTextStyle: {},
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
    onZero: !0,
    onZeroAxisIndex: null,
    lineStyle: {
      color: "#6E7079",
      width: 1,
      type: "solid"
    },
    // The arrow at both ends the the axis.
    symbol: ["none", "none"],
    symbolSize: [10, 15]
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
    fontSize: 12
  },
  splitLine: {
    show: !0,
    showMinLine: !0,
    showMaxLine: !0,
    lineStyle: {
      color: ["#E0E6F1"],
      width: 1,
      type: "solid"
    }
  },
  splitArea: {
    show: !1,
    areaStyle: {
      color: ["rgba(250,250,250,0.2)", "rgba(210,219,238,0.2)"]
    }
  }
}, Ma = H({
  // The gap at both ends of the axis. For categoryAxis, boolean.
  boundaryGap: !0,
  // Set false to faster category collection.
  deduplication: null,
  // splitArea: {
  // show: false
  // },
  splitLine: {
    show: !1
  },
  axisTick: {
    // If tick is align with label when boundaryGap is true
    alignWithLabel: !1,
    interval: "auto"
  },
  axisLabel: {
    interval: "auto"
  }
}, Ve), Gt = H({
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
      color: "#F4F7FD",
      width: 1
    }
  }
}, Ve), wa = H({
  splitNumber: 6,
  axisLabel: {
    // To eliminate labels that are not nice
    showMinLabel: !1,
    showMaxLabel: !1,
    rich: {
      primary: {
        fontWeight: "bold"
      }
    }
  },
  splitLine: {
    show: !1
  }
}, Gt), La = O({
  logBase: 10
}, Gt);
const Ta = {
  category: Ma,
  value: Gt,
  time: wa,
  log: La
};
var Da = {
  value: 1,
  category: 1,
  time: 1,
  log: 1
};
function gt(r, t, e, a) {
  w(Da, function(i, n) {
    var o = H(H({}, Ta[n], !0), a, !0), s = (
      /** @class */
      (function(v) {
        L(u, v);
        function u() {
          var l = v !== null && v.apply(this, arguments) || this;
          return l.type = t + "Axis." + n, l;
        }
        return u.prototype.mergeDefaultAndTheme = function(l, c) {
          var d = ur(this), g = d ? vr(l) : {}, h = c.getTheme();
          H(l, h.get(n + "Axis")), H(l, this.getDefaultOption()), l.type = le(l), d && cr(l, g, d);
        }, u.prototype.optionUpdated = function() {
          var l = this.option;
          l.type === "category" && (this.__ordinalMeta = hr.createByAxisModel(this));
        }, u.prototype.getCategories = function(l) {
          var c = this.option;
          if (c.type === "category")
            return l ? c.data : this.__ordinalMeta.categories;
        }, u.prototype.getOrdinalMeta = function() {
          return this.__ordinalMeta;
        }, u.type = t + "Axis." + n, u.defaultOption = o, u;
      })(e)
    );
    r.registerComponentModel(s);
  }), r.registerSubTypeDefaulter(t + "Axis", le);
}
function le(r) {
  return r.type || (r.data ? "category" : "value");
}
var Dt = ["x", "y"];
function ue(r) {
  return r.type === "interval" || r.type === "time";
}
var Pa = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = "cartesian2d", e.dimensions = Dt, e;
    }
    return t.prototype.calcAffineTransform = function() {
      this._transform = this._invTransform = null;
      var e = this.getAxis("x").scale, a = this.getAxis("y").scale;
      if (!(!ue(e) || !ue(a))) {
        var i = e.getExtent(), n = a.getExtent(), o = this.dataToPoint([i[0], n[0]]), s = this.dataToPoint([i[1], n[1]]), v = i[1] - i[0], u = n[1] - n[0];
        if (!(!v || !u)) {
          var l = (s[0] - o[0]) / v, c = (s[1] - o[1]) / u, d = o[0] - i[0] * l, g = o[1] - n[0] * c, h = this._transform = [l, 0, 0, c, d, g];
          this._invTransform = dr([], h);
        }
      }
    }, t.prototype.getBaseAxis = function() {
      return this.getAxesByScale("ordinal")[0] || this.getAxesByScale("time")[0] || this.getAxis("x");
    }, t.prototype.containPoint = function(e) {
      var a = this.getAxis("x"), i = this.getAxis("y");
      return a.contain(a.toLocalCoord(e[0])) && i.contain(i.toLocalCoord(e[1]));
    }, t.prototype.containData = function(e) {
      return this.getAxis("x").containData(e[0]) && this.getAxis("y").containData(e[1]);
    }, t.prototype.containZone = function(e, a) {
      var i = this.dataToPoint(e), n = this.dataToPoint(a), o = this.getArea(), s = new Xt(i[0], i[1], n[0] - i[0], n[1] - i[1]);
      return o.intersect(s);
    }, t.prototype.dataToPoint = function(e, a, i) {
      i = i || [];
      var n = e[0], o = e[1];
      if (this._transform && n != null && isFinite(n) && o != null && isFinite(o))
        return $t(i, e, this._transform);
      var s = this.getAxis("x"), v = this.getAxis("y");
      return i[0] = s.toGlobalCoord(s.dataToCoord(n, a)), i[1] = v.toGlobalCoord(v.dataToCoord(o, a)), i;
    }, t.prototype.clampData = function(e, a) {
      var i = this.getAxis("x").scale, n = this.getAxis("y").scale, o = i.getExtent(), s = n.getExtent(), v = i.parse(e[0]), u = n.parse(e[1]);
      return a = a || [], a[0] = Math.min(Math.max(Math.min(o[0], o[1]), v), Math.max(o[0], o[1])), a[1] = Math.min(Math.max(Math.min(s[0], s[1]), u), Math.max(s[0], s[1])), a;
    }, t.prototype.pointToData = function(e, a) {
      var i = [];
      if (this._invTransform)
        return $t(i, e, this._invTransform);
      var n = this.getAxis("x"), o = this.getAxis("y");
      return i[0] = n.coordToData(n.toLocalCoord(e[0]), a), i[1] = o.coordToData(o.toLocalCoord(e[1]), a), i;
    }, t.prototype.getOtherAxis = function(e) {
      return this.getAxis(e.dim === "x" ? "y" : "x");
    }, t.prototype.getArea = function(e) {
      e = e || 0;
      var a = this.getAxis("x").getGlobalExtent(), i = this.getAxis("y").getGlobalExtent(), n = Math.min(a[0], a[1]) - e, o = Math.min(i[0], i[1]) - e, s = Math.max(a[0], a[1]) - n + e, v = Math.max(i[0], i[1]) - o + e;
      return new Xt(n, o, s, v);
    }, t;
  })(gr)
), _a = (
  /** @class */
  (function(r) {
    L(t, r);
    function t(e, a, i, n, o) {
      var s = r.call(this, e, a, i) || this;
      return s.index = 0, s.type = n || "value", s.position = o || "bottom", s;
    }
    return t.prototype.isHorizontal = function() {
      var e = this.position;
      return e === "top" || e === "bottom";
    }, t.prototype.getGlobalExtent = function(e) {
      var a = this.getExtent();
      return a[0] = this.toGlobalCoord(a[0]), a[1] = this.toGlobalCoord(a[1]), e && a[0] > a[1] && a.reverse(), a;
    }, t.prototype.pointToData = function(e, a) {
      return this.coordToData(this.toLocalCoord(e[this.dim === "x" ? 0 : 1]), a);
    }, t.prototype.setCategorySortInfo = function(e) {
      if (this.type !== "category")
        return !1;
      this.model.option.categorySortInfo = e, this.scale.setSortInfo(e);
    }, t;
  })(Y)
), bt = Math.log;
function Ea(r, t, e) {
  var a = fr.prototype, i = a.getTicks.call(e), n = a.getTicks.call(e, !0), o = i.length - 1, s = a.getInterval.call(e), v = pr(r, t), u = v.extent, l = v.fixMin, c = v.fixMax;
  if (r.type === "log") {
    var d = bt(r.base);
    u = [bt(u[0]) / d, bt(u[1]) / d];
  }
  r.setExtent(u[0], u[1]), r.calcNiceExtent({
    splitNumber: o,
    fixMin: l,
    fixMax: c
  });
  var g = a.getExtent.call(r);
  l && (u[0] = g[0]), c && (u[1] = g[1]);
  var h = a.getInterval.call(r), f = u[0], p = u[1];
  if (l && c)
    h = (p - f) / o;
  else if (l)
    for (p = u[0] + h * o; p < u[1] && isFinite(p) && isFinite(u[1]); )
      h = xt(h), p = u[0] + h * o;
  else if (c)
    for (f = u[1] - h * o; f > u[0] && isFinite(f) && isFinite(u[0]); )
      h = xt(h), f = u[1] - h * o;
  else {
    var y = r.getTicks().length - 1;
    y > o && (h = xt(h));
    var x = h * o;
    p = Math.ceil(u[1] / h) * h, f = F(p - x), f < 0 && u[0] >= 0 ? (f = 0, p = F(x)) : p > 0 && u[1] <= 0 && (p = 0, f = -F(x));
  }
  var m = (i[0].value - n[0].value) / s, A = (i[o].value - n[o].value) / s;
  a.setExtent.call(r, f + h * m, p + h * A), a.setInterval.call(r, h), (m || A) && a.setNiceExtent.call(r, f + h, p - h);
}
var Ia = (
  /** @class */
  (function() {
    function r(t, e, a) {
      this.type = "grid", this._coordsMap = {}, this._coordsList = [], this._axesMap = {}, this._axesList = [], this.axisPointerEnabled = !0, this.dimensions = Dt, this._initCartesian(t, e, a), this.model = t;
    }
    return r.prototype.getRect = function() {
      return this._rect;
    }, r.prototype.update = function(t, e) {
      var a = this._axesMap;
      this._updateScale(t, this.model);
      function i(o) {
        var s, v = De(o), u = v.length;
        if (u) {
          for (var l = [], c = u - 1; c >= 0; c--) {
            var d = +v[c], g = o[d], h = g.model, f = g.scale;
            // Only value and log axis without interval support alignTicks.
            wt(f) && h.get("alignTicks") && h.get("interval") == null ? l.push(g) : (ht(f, h), wt(f) && (s = g));
          }
          l.length && (s || (s = l.pop(), ht(s.scale, s.model)), w(l, function(p) {
            Ea(p.scale, p.model, s.scale);
          }));
        }
      }
      i(a.x), i(a.y);
      var n = {};
      w(a.x, function(o) {
        ve(a, "y", o, n);
      }), w(a.y, function(o) {
        ve(a, "x", o, n);
      }), this.resize(this.model, e);
    }, r.prototype.resize = function(t, e, a) {
      var i = t.getBoxLayoutParams(), n = !a && t.get("containLabel"), o = yr(i, {
        width: e.getWidth(),
        height: e.getHeight()
      });
      this._rect = o;
      var s = this._axesList;
      v(), n && (w(s, function(u) {
        if (!u.model.get(["axisLabel", "inside"])) {
          var l = xr(u);
          if (l) {
            var c = u.isHorizontal() ? "height" : "width", d = u.model.get(["axisLabel", "margin"]);
            o[c] -= l[c] + d, u.position === "top" ? o.y += l.height + d : u.position === "left" && (o.x += l.width + d);
          }
        }
      }), v()), w(this._coordsList, function(u) {
        u.calcAffineTransform();
      });
      function v() {
        w(s, function(u) {
          var l = u.isHorizontal(), c = l ? [0, o.width] : [0, o.height], d = u.inverse ? 1 : 0;
          u.setExtent(c[d], c[1 - d]), Ra(u, l ? o.x : o.y);
        });
      }
    }, r.prototype.getAxis = function(t, e) {
      var a = this._axesMap[t];
      if (a != null)
        return a[e || 0];
    }, r.prototype.getAxes = function() {
      return this._axesList.slice();
    }, r.prototype.getCartesian = function(t, e) {
      if (t != null && e != null) {
        var a = "x" + t + "y" + e;
        return this._coordsMap[a];
      }
      Bt(t) && (e = t.yAxisIndex, t = t.xAxisIndex);
      for (var i = 0, n = this._coordsList; i < n.length; i++)
        if (n[i].getAxis("x").index === t || n[i].getAxis("y").index === e)
          return n[i];
    }, r.prototype.getCartesians = function() {
      return this._coordsList.slice();
    }, r.prototype.convertToPixel = function(t, e, a) {
      var i = this._findConvertTarget(e);
      return i.cartesian ? i.cartesian.dataToPoint(a) : i.axis ? i.axis.toGlobalCoord(i.axis.dataToCoord(a)) : null;
    }, r.prototype.convertFromPixel = function(t, e, a) {
      var i = this._findConvertTarget(e);
      return i.cartesian ? i.cartesian.pointToData(a) : i.axis ? i.axis.coordToData(i.axis.toLocalCoord(a)) : null;
    }, r.prototype._findConvertTarget = function(t) {
      var e = t.seriesModel, a = t.xAxisModel || e && e.getReferringComponents("xAxis", Q).models[0], i = t.yAxisModel || e && e.getReferringComponents("yAxis", Q).models[0], n = t.gridModel, o = this._coordsList, s, v;
      if (e)
        s = e.coordinateSystem, ut(o, s) < 0 && (s = null);
      else if (a && i)
        s = this.getCartesian(a.componentIndex, i.componentIndex);
      else if (a)
        v = this.getAxis("x", a.componentIndex);
      else if (i)
        v = this.getAxis("y", i.componentIndex);
      else if (n) {
        var u = n.coordinateSystem;
        u === this && (s = this._coordsList[0]);
      }
      return {
        cartesian: s,
        axis: v
      };
    }, r.prototype.containPoint = function(t) {
      var e = this._coordsList[0];
      if (e)
        return e.containPoint(t);
    }, r.prototype._initCartesian = function(t, e, a) {
      var i = this, n = this, o = {
        left: !1,
        right: !1,
        top: !1,
        bottom: !1
      }, s = {
        x: {},
        y: {}
      }, v = {
        x: 0,
        y: 0
      };
      if (e.eachComponent("xAxis", u("x"), this), e.eachComponent("yAxis", u("y"), this), !v.x || !v.y) {
        this._axesMap = {}, this._axesList = [];
        return;
      }
      this._axesMap = s, w(s.x, function(l, c) {
        w(s.y, function(d, g) {
          var h = "x" + c + "y" + g, f = new Pa(h);
          f.master = i, f.model = t, i._coordsMap[h] = f, i._coordsList.push(f), f.addAxis(l), f.addAxis(d);
        });
      });
      function u(l) {
        return function(c, d) {
          if (Ct(c, t)) {
            var g = c.get("position");
            l === "x" ? g !== "top" && g !== "bottom" && (g = o.bottom ? "top" : "bottom") : g !== "left" && g !== "right" && (g = o.left ? "right" : "left"), o[g] = !0;
            var h = new _a(l, Te(c), [0, 0], c.get("type"), g), f = h.type === "category";
            h.onBand = f && c.get("boundaryGap"), h.inverse = c.get("inverse"), c.axis = h, h.model = c, h.grid = n, h.index = d, n._axesList.push(h), s[l][d] = h, v[l]++;
          }
        };
      }
    }, r.prototype._updateScale = function(t, e) {
      w(this._axesList, function(i) {
        if (i.scale.setExtent(1 / 0, -1 / 0), i.type === "category") {
          var n = i.model.get("categorySortInfo");
          i.scale.setSortInfo(n);
        }
      }), t.eachSeries(function(i) {
        if (Zt(i)) {
          var n = Ut(i), o = n.xAxisModel, s = n.yAxisModel;
          if (!Ct(o, e) || !Ct(s, e))
            return;
          var v = this.getCartesian(o.componentIndex, s.componentIndex), u = i.getData(), l = v.getAxis("x"), c = v.getAxis("y");
          a(u, l), a(u, c);
        }
      }, this);
      function a(i, n) {
        w(Mt(i, n.dim), function(o) {
          n.scale.unionExtentFromData(i, o);
        });
      }
    }, r.prototype.getTooltipAxes = function(t) {
      var e = [], a = [];
      return w(this.getCartesians(), function(i) {
        var n = t != null && t !== "auto" ? i.getAxis(t) : i.getBaseAxis(), o = i.getOtherAxis(n);
        ut(e, n) < 0 && e.push(n), ut(a, o) < 0 && a.push(o);
      }), {
        baseAxes: e,
        otherAxes: a
      };
    }, r.create = function(t, e) {
      var a = [];
      return t.eachComponent("grid", function(i, n) {
        var o = new r(i, t, e);
        o.name = "grid_" + n, o.resize(i, e, !0), i.coordinateSystem = o, a.push(o);
      }), t.eachSeries(function(i) {
        if (Zt(i)) {
          var n = Ut(i), o = n.xAxisModel, s = n.yAxisModel, v = o.getCoordSysModel(), u = v.coordinateSystem;
          i.coordinateSystem = u.getCartesian(o.componentIndex, s.componentIndex);
        }
      }), a;
    }, r.dimensions = Dt, r;
  })()
);
function Ct(r, t) {
  return r.getCoordSysModel() === t;
}
function ve(r, t, e, a) {
  e.getAxesOnZeroOf = function() {
    return n ? [n] : [];
  };
  var i = r[t], n, o = e.model, s = o.get(["axisLine", "onZero"]), v = o.get(["axisLine", "onZeroAxisIndex"]);
  if (!s)
    return;
  if (v != null)
    ce(i[v]) && (n = i[v]);
  else
    for (var u in i)
      if (i.hasOwnProperty(u) && ce(i[u]) && !a[l(i[u])]) {
        n = i[u];
        break;
      }
  n && (a[l(n)] = !0);
  function l(c) {
    return c.dim + "_" + c.index;
  }
}
function ce(r) {
  return r && r.type !== "category" && r.type !== "time" && mr(r);
}
function Ra(r, t) {
  var e = r.getExtent(), a = e[0] + e[1];
  r.toGlobalCoord = r.dim === "x" ? function(i) {
    return i + t;
  } : function(i) {
    return a - i + t;
  }, r.toLocalCoord = r.dim === "x" ? function(i) {
    return i - t;
  } : function(i) {
    return a - i + t;
  };
}
var Pt = _e();
function ka(r, t, e, a) {
  var i = e.axis;
  if (!i.scale.isBlank()) {
    var n = e.getModel("splitArea"), o = n.getModel("areaStyle"), s = o.get("color"), v = a.coordinateSystem.getRect(), u = i.getTicksCoords({
      tickModel: n,
      clamp: !0
    });
    if (u.length) {
      var l = s.length, c = Pt(r).splitAreaColors, d = Pe(), g = 0;
      if (c)
        for (var h = 0; h < u.length; h++) {
          var f = c.get(u[h].tickValue);
          if (f != null) {
            g = (f + (l - 1) * h) % l;
            break;
          }
        }
      var p = i.toGlobalCoord(u[0].coord), y = o.getAreaStyle();
      s = G(s) ? s : [s];
      for (var h = 1; h < u.length; h++) {
        var x = i.toGlobalCoord(u[h].coord), m = void 0, A = void 0, S = void 0, b = void 0;
        i.isHorizontal() ? (m = p, A = v.y, S = x - m, b = v.height, p = m + S) : (m = v.x, A = p, S = v.width, b = x - A, p = A + b);
        var C = u[h - 1].tickValue;
        C != null && d.set(C, g), t.add(new et({
          anid: C != null ? "area_" + C : null,
          shape: {
            x: m,
            y: A,
            width: S,
            height: b
          },
          style: O({
            fill: s[g]
          }, y),
          autoBatch: !0,
          silent: !0
        })), g = (g + 1) % l;
      }
      Pt(r).splitAreaColors = d;
    }
  }
}
function Ba(r) {
  Pt(r).splitAreaColors = null;
}
var Oa = ["axisLine", "axisTickLabel", "axisName"], Ga = ["splitArea", "splitLine", "minorSplitLine"], ze = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e.axisPointerClass = "CartesianAxisPointer", e;
    }
    return t.prototype.render = function(e, a, i, n) {
      this.group.removeAll();
      var o = this._axisGroup;
      if (this._axisGroup = new It(), this.group.add(this._axisGroup), !!e.get("show")) {
        var s = e.getCoordSysModel(), v = Ar(s, e), u = new J(e, rt({
          handleAutoShown: function(c) {
            for (var d = s.coordinateSystem.getCartesians(), g = 0; g < d.length; g++)
              if (wt(d[g].getOtherAxis(e.axis).scale))
                return !0;
            return !1;
          }
        }, v));
        w(Oa, u.add, u), this._axisGroup.add(u.getGroup()), w(Ga, function(c) {
          e.get([c, "show"]) && Wa[c](this, this._axisGroup, e, s);
        }, this);
        var l = n && n.type === "changeAxisOrder" && n.isInitSort;
        l || Ee(o, this._axisGroup, e), r.prototype.render.call(this, e, a, i, n);
      }
    }, t.prototype.remove = function() {
      Ba(this);
    }, t.type = "cartesianAxis", t;
  })(yt)
), Wa = {
  splitLine: function(r, t, e, a) {
    var i = e.axis;
    if (!i.scale.isBlank()) {
      var n = e.getModel("splitLine"), o = n.getModel("lineStyle"), s = o.get("color"), v = n.get("showMinLine") !== !1, u = n.get("showMaxLine") !== !1;
      s = G(s) ? s : [s];
      for (var l = a.coordinateSystem.getRect(), c = i.isHorizontal(), d = 0, g = i.getTicksCoords({
        tickModel: n
      }), h = [], f = [], p = o.getLineStyle(), y = 0; y < g.length; y++) {
        var x = i.toGlobalCoord(g[y].coord);
        if (!(y === 0 && !v || y === g.length - 1 && !u)) {
          var m = g[y].tickValue;
          c ? (h[0] = x, h[1] = l.y, f[0] = x, f[1] = l.y + l.height) : (h[0] = l.x, h[1] = x, f[0] = l.x + l.width, f[1] = x);
          var A = d++ % s.length, S = new Z({
            anid: m != null ? "line_" + m : null,
            autoBatch: !0,
            shape: {
              x1: h[0],
              y1: h[1],
              x2: f[0],
              y2: f[1]
            },
            style: O({
              stroke: s[A]
            }, p),
            silent: !0
          });
          jt(S.shape, p.lineWidth), t.add(S);
        }
      }
    }
  },
  minorSplitLine: function(r, t, e, a) {
    var i = e.axis, n = e.getModel("minorSplitLine"), o = n.getModel("lineStyle"), s = a.coordinateSystem.getRect(), v = i.isHorizontal(), u = i.getMinorTicksCoords();
    if (u.length)
      for (var l = [], c = [], d = o.getLineStyle(), g = 0; g < u.length; g++)
        for (var h = 0; h < u[g].length; h++) {
          var f = i.toGlobalCoord(u[g][h].coord);
          v ? (l[0] = f, l[1] = s.y, c[0] = f, c[1] = s.y + s.height) : (l[0] = s.x, l[1] = f, c[0] = s.x + s.width, c[1] = f);
          var p = new Z({
            anid: "minor_line_" + u[g][h].tickValue,
            autoBatch: !0,
            shape: {
              x1: l[0],
              y1: l[1],
              x2: c[0],
              y2: c[1]
            },
            style: d,
            silent: !0
          });
          jt(p.shape, d.lineWidth), t.add(p);
        }
  },
  splitArea: function(r, t, e, a) {
    ka(r, t, e, a);
  }
}, Fe = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "xAxis", t;
  })(ze)
), Na = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = Fe.type, e;
    }
    return t.type = "yAxis", t;
  })(ze)
), Va = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = "grid", e;
    }
    return t.prototype.render = function(e, a) {
      this.group.removeAll(), e.get("show") && this.group.add(new et({
        shape: e.coordinateSystem.getRect(),
        style: O({
          fill: e.get("backgroundColor")
        }, e.getItemStyle()),
        silent: !0,
        z2: -1
      }));
    }, t.type = "grid", t;
  })(Ie)
), he = {
  // gridIndex: 0,
  // gridId: '',
  offset: 0
};
function za(r) {
  r.registerComponentView(Va), r.registerComponentModel(Ca), r.registerCoordinateSystem("cartesian2d", Ia), gt(r, "x", Tt, he), gt(r, "y", Tt, he), r.registerComponentView(Fe), r.registerComponentView(Na), r.registerPreprocessor(function(t) {
    t.xAxis && t.yAxis && !t.grid && (t.grid = {});
  });
}
var Fa = (
  /** @class */
  (function() {
    function r() {
    }
    return r.prototype._hasEncodeRule = function(t) {
      var e = this.getEncode();
      return e && e.get(t) != null;
    }, r.prototype.getInitialData = function(t, e) {
      var a, i = e.getComponent("xAxis", this.get("xAxisIndex")), n = e.getComponent("yAxis", this.get("yAxisIndex")), o = i.get("type"), s = n.get("type"), v;
      o === "category" ? (t.layout = "horizontal", a = i.getOrdinalMeta(), v = !this._hasEncodeRule("x")) : s === "category" ? (t.layout = "vertical", a = n.getOrdinalMeta(), v = !this._hasEncodeRule("y")) : t.layout = t.layout || "horizontal";
      var u = ["x", "y"], l = t.layout === "horizontal" ? 0 : 1, c = this._baseAxisDim = u[l], d = u[1 - l], g = [i, n], h = g[l].get("type"), f = g[1 - l].get("type"), p = t.data;
      if (p && v) {
        var y = [];
        w(p, function(A, S) {
          var b;
          G(A) ? (b = A.slice(), A.unshift(S)) : G(A.value) ? (b = rt({}, A), b.value = b.value.slice(), A.value.unshift(S)) : b = A, y.push(b);
        }), t.data = y;
      }
      var x = this.defaultValueDimensions, m = [{
        name: c,
        type: Kt(h),
        ordinalMeta: a,
        otherDims: {
          tooltip: !1,
          itemName: 0
        },
        dimsDef: ["base"]
      }, {
        name: d,
        type: Kt(f),
        dimsDef: x.slice()
      }];
      return ba(this, {
        coordDimensions: m,
        dimensionsCount: x.length + 1,
        encodeDefaulter: Rt(Sr, m, this)
      });
    }, r.prototype.getBaseAxis = function() {
      var t = this._baseAxisDim;
      return this.ecModel.getComponent(t + "Axis", this.get(t + "AxisIndex")).axis;
    }, r;
  })()
), He = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
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
    return t.type = "series.boxplot", t.dependencies = ["xAxis", "yAxis", "grid"], t.defaultOption = {
      // zlevel: 0,
      z: 2,
      coordinateSystem: "cartesian2d",
      legendHoverLink: !0,
      layout: null,
      boxWidth: [7, 50],
      itemStyle: {
        color: "#fff",
        borderWidth: 1
      },
      emphasis: {
        scale: !0,
        itemStyle: {
          borderWidth: 2,
          shadowBlur: 5,
          shadowOffsetX: 1,
          shadowOffsetY: 1,
          shadowColor: "rgba(0,0,0,0.2)"
        }
      },
      animationDuration: 800
    }, t;
  })(_t)
);
kt(He, Fa, !0);
var Ha = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.prototype.render = function(e, a, i) {
      var n = e.getData(), o = this.group, s = this._data;
      this._data || o.removeAll();
      var v = e.get("layout") === "horizontal" ? 1 : 0;
      n.diff(s).add(function(u) {
        if (n.hasValue(u)) {
          var l = n.getItemLayout(u), c = de(l, n, u, v, !0);
          n.setItemGraphicEl(u, c), o.add(c);
        }
      }).update(function(u, l) {
        var c = s.getItemGraphicEl(l);
        if (!n.hasValue(u)) {
          o.remove(c);
          return;
        }
        var d = n.getItemLayout(u);
        c ? (be(c), Ye(d, c, n, u)) : c = de(d, n, u, v), o.add(c), n.setItemGraphicEl(u, c);
      }).remove(function(u) {
        var l = s.getItemGraphicEl(u);
        l && o.remove(l);
      }).execute(), this._data = n;
    }, t.prototype.remove = function(e) {
      var a = this.group, i = this._data;
      this._data = null, i && i.eachItemGraphicEl(function(n) {
        n && a.remove(n);
      });
    }, t.type = "boxplot", t;
  })(Ce)
), Ya = (
  /** @class */
  /* @__PURE__ */ (function() {
    function r() {
    }
    return r;
  })()
), Xa = (
  /** @class */
  (function(r) {
    L(t, r);
    function t(e) {
      var a = r.call(this, e) || this;
      return a.type = "boxplotBoxPath", a;
    }
    return t.prototype.getDefaultShape = function() {
      return new Ya();
    }, t.prototype.buildPath = function(e, a) {
      var i = a.points, n = 0;
      for (e.moveTo(i[n][0], i[n][1]), n++; n < 4; n++)
        e.lineTo(i[n][0], i[n][1]);
      for (e.closePath(); n < i.length; n++)
        e.moveTo(i[n][0], i[n][1]), n++, e.lineTo(i[n][0], i[n][1]);
    }, t;
  })(Et)
);
function de(r, t, e, a, i) {
  var n = r.ends, o = new Xa({
    shape: {
      points: i ? $a(n, a, r) : n
    }
  });
  return Ye(r, o, t, e, i), o;
}
function Ye(r, t, e, a, i) {
  var n = e.hostModel, o = Ot[i ? "initProps" : "updateProps"];
  o(t, {
    shape: {
      points: r.ends
    }
  }, n, a), t.useStyle(e.getItemVisual(a, "style")), t.style.strokeNoScale = !0, t.z2 = 100;
  var s = e.getItemModel(a), v = s.getModel("emphasis");
  we(t, s), Me(t, v.get("focus"), v.get("blurScope"), v.get("disabled"));
}
function $a(r, t, e) {
  return U(r, function(a) {
    return a = a.slice(), a[t] = e.initBaseline, a;
  });
}
var K = w;
function Za(r) {
  var t = Ua(r);
  K(t, function(e) {
    var a = e.seriesModels;
    a.length && (ja(e), K(a, function(i, n) {
      Ka(i, e.boxOffsetList[n], e.boxWidthList[n]);
    }));
  });
}
function Ua(r) {
  var t = [], e = [];
  return r.eachSeriesByType("boxplot", function(a) {
    var i = a.getBaseAxis(), n = ut(e, i);
    n < 0 && (n = e.length, e[n] = i, t[n] = {
      axis: i,
      seriesModels: []
    }), t[n].seriesModels.push(a);
  }), t;
}
function ja(r) {
  var t = r.axis, e = r.seriesModels, a = e.length, i = r.boxWidthList = [], n = r.boxOffsetList = [], o = [], s;
  if (t.type === "category")
    s = t.getBandWidth();
  else {
    var v = 0;
    K(e, function(h) {
      v = Math.max(v, h.getData().count());
    });
    var u = t.getExtent();
    s = Math.abs(u[1] - u[0]) / v;
  }
  K(e, function(h) {
    var f = h.get("boxWidth");
    G(f) || (f = [f, f]), o.push([W(f[0], s) || 0, W(f[1], s) || 0]);
  });
  var l = s * 0.8 - 2, c = l / a * 0.3, d = (l - c * (a - 1)) / a, g = d / 2 - l / 2;
  K(e, function(h, f) {
    n.push(g), g += c + d, i.push(Math.min(Math.max(d, o[f][0]), o[f][1]));
  });
}
function Ka(r, t, e) {
  var a = r.coordinateSystem, i = r.getData(), n = e / 2, o = r.get("layout") === "horizontal" ? 0 : 1, s = 1 - o, v = ["x", "y"], u = i.mapDimension(v[o]), l = i.mapDimensionsAll(v[s]);
  if (u == null || l.length < 5)
    return;
  for (var c = 0; c < i.count(); c++) {
    var d = i.get(u, c), g = m(d, l[2], c), h = m(d, l[0], c), f = m(d, l[1], c), p = m(d, l[3], c), y = m(d, l[4], c), x = [];
    A(x, f, !1), A(x, p, !0), x.push(h, f, y, p), S(x, h), S(x, y), S(x, g), i.setItemLayout(c, {
      initBaseline: g[s],
      ends: x
    });
  }
  function m(b, C, D) {
    var I = i.get(C, D), R = [];
    R[o] = b, R[s] = I;
    var M;
    return isNaN(b) || isNaN(I) ? M = [NaN, NaN] : (M = a.dataToPoint(R), M[o] += t), M;
  }
  function A(b, C, D) {
    var I = C.slice(), R = C.slice();
    I[o] += n, R[o] -= n, D ? b.push(I, R) : b.push(R, I);
  }
  function S(b, C) {
    var D = C.slice(), I = C.slice();
    D[o] -= n, I[o] += n, b.push(D, I);
  }
}
function qa(r, t) {
  t = t || {};
  for (var e = [], a = [], i = t.boundIQR, n = i === "none" || i === 0, o = 0; o < r.length; o++) {
    var s = br(r[o].slice()), v = mt(s, 0.25), u = mt(s, 0.5), l = mt(s, 0.75), c = s[0], d = s[s.length - 1], g = (i ?? 1.5) * (l - v), h = n ? c : Math.max(c, v - g), f = n ? d : Math.min(d, l + g), p = t.itemNameFormatter, y = me(p) ? p({
      value: o
    }) : vt(p) ? p.replace("{value}", o + "") : o + "";
    e.push([y, h, v, u, l, f]);
    for (var x = 0; x < s.length; x++) {
      var m = s[x];
      if (m < h || m > f) {
        var A = [y, m];
        a.push(A);
      }
    }
  }
  return {
    boxData: e,
    outliers: a
  };
}
var Qa = {
  type: "echarts:boxplot",
  transform: function(t) {
    var e = t.upstream;
    if (e.sourceFormat !== Re) {
      var a = "";
      E(a);
    }
    var i = qa(e.getRawData(), t.config);
    return [{
      dimensions: ["ItemName", "Low", "Q1", "Q2", "Q3", "High"],
      data: i.boxData
    }, {
      data: i.outliers
    }];
  }
};
function Ja(r) {
  r.registerSeriesModel(He), r.registerChartView(Ha), r.registerLayout(Za), r.registerTransform(Qa);
}
function ti(r) {
  dt(za), dt(ke);
}
var ei = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.makeElOption = function(e, a, i, n, o) {
      var s = i.axis;
      s.dim === "angle" && (this.animationThreshold = Math.PI / 18);
      var v = s.polar, u = v.getOtherAxis(s), l = u.getExtent(), c = s.dataToCoord(a), d = n.get("type");
      if (d && d !== "none") {
        var g = Cr(n), h = ai[d](s, v, c, l);
        h.style = g, e.graphicKey = h.type, e.pointer = h;
      }
      var f = n.get(["label", "margin"]), p = ri(a, i, n, v, f);
      Mr(e, i, n, o, p);
    }, t;
  })(wr)
);
function ri(r, t, e, a, i) {
  var n = t.axis, o = n.dataToCoord(r), s = a.getAngleAxis().getExtent()[0];
  s = s / 180 * Math.PI;
  var v = a.getRadiusAxis().getExtent(), u, l, c;
  if (n.dim === "radius") {
    var d = _r();
    Tr(d, d, s), Dr(d, d, [a.cx, a.cy]), u = Pr([o, -i], d);
    var g = t.getModel("axisLabel").get("rotate") || 0, h = J.innerTextLayout(s, g * Math.PI / 180, -1);
    l = h.textAlign, c = h.textVerticalAlign;
  } else {
    var f = v[1];
    u = a.coordToPoint([f + i, o]);
    var p = a.cx, y = a.cy;
    l = Math.abs(u[0] - p) / f < 0.3 ? "center" : u[0] > p ? "left" : "right", c = Math.abs(u[1] - y) / f < 0.3 ? "middle" : u[1] > y ? "top" : "bottom";
  }
  return {
    position: u,
    align: l,
    verticalAlign: c
  };
}
var ai = {
  line: function(r, t, e, a) {
    return r.dim === "angle" ? {
      type: "Line",
      shape: Lr(t.coordToPoint([a[0], e]), t.coordToPoint([a[1], e]))
    } : {
      type: "Circle",
      shape: {
        cx: t.cx,
        cy: t.cy,
        r: e
      }
    };
  },
  shadow: function(r, t, e, a) {
    var i = Math.max(1, r.getBandWidth()), n = Math.PI / 180;
    return r.dim === "angle" ? {
      type: "Sector",
      shape: qt(
        t.cx,
        t.cy,
        a[0],
        a[1],
        // In ECharts y is negative if angle is positive
        (-e - i / 2) * n,
        (-e + i / 2) * n
      )
    } : {
      type: "Sector",
      shape: qt(t.cx, t.cy, e - i / 2, e + i / 2, 0, Math.PI * 2)
    };
  }
}, ii = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.prototype.findAxisModel = function(e) {
      var a, i = this.ecModel;
      return i.eachComponent(e, function(n) {
        n.getCoordSysModel() === this && (a = n);
      }, this), a;
    }, t.type = "polar", t.dependencies = ["radiusAxis", "angleAxis"], t.defaultOption = {
      // zlevel: 0,
      z: 0,
      center: ["50%", "50%"],
      radius: "80%"
    }, t;
  })(pt)
), Wt = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.getCoordSysModel = function() {
      return this.getReferringComponents("polar", Q).models[0];
    }, t.type = "polarAxis", t;
  })(pt)
);
kt(Wt, Le);
var ni = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "angleAxis", t;
  })(Wt)
), oi = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "radiusAxis", t;
  })(Wt)
), Nt = (
  /** @class */
  (function(r) {
    L(t, r);
    function t(e, a) {
      return r.call(this, "radius", e, a) || this;
    }
    return t.prototype.pointToData = function(e, a) {
      return this.polar.pointToData(e, a)[this.dim === "radius" ? 0 : 1];
    }, t;
  })(Y)
);
Nt.prototype.dataToRadius = Y.prototype.dataToCoord;
Nt.prototype.radiusToData = Y.prototype.coordToData;
var si = _e(), Vt = (
  /** @class */
  (function(r) {
    L(t, r);
    function t(e, a) {
      return r.call(this, "angle", e, a || [0, 360]) || this;
    }
    return t.prototype.pointToData = function(e, a) {
      return this.polar.pointToData(e, a)[this.dim === "radius" ? 0 : 1];
    }, t.prototype.calculateCategoryInterval = function() {
      var e = this, a = e.getLabelModel(), i = e.scale, n = i.getExtent(), o = i.count();
      if (n[1] - n[0] < 1)
        return 0;
      var s = n[0], v = e.dataToCoord(s + 1) - e.dataToCoord(s), u = Math.abs(v), l = Er(s == null ? "" : s + "", a.getFont(), "center", "top"), c = Math.max(l.height, 7), d = c / u;
      isNaN(d) && (d = 1 / 0);
      var g = Math.max(0, Math.floor(d)), h = si(e.model), f = h.lastAutoInterval, p = h.lastTickCount;
      return f != null && p != null && Math.abs(f - g) <= 1 && Math.abs(p - o) <= 1 && f > g ? g = f : (h.lastTickCount = o, h.lastAutoInterval = g), g;
    }, t;
  })(Y)
);
Vt.prototype.dataToAngle = Y.prototype.dataToCoord;
Vt.prototype.angleToData = Y.prototype.coordToData;
var Xe = ["radius", "angle"], li = (
  /** @class */
  (function() {
    function r(t) {
      this.dimensions = Xe, this.type = "polar", this.cx = 0, this.cy = 0, this._radiusAxis = new Nt(), this._angleAxis = new Vt(), this.axisPointerEnabled = !0, this.name = t || "", this._radiusAxis.polar = this._angleAxis.polar = this;
    }
    return r.prototype.containPoint = function(t) {
      var e = this.pointToCoord(t);
      return this._radiusAxis.contain(e[0]) && this._angleAxis.contain(e[1]);
    }, r.prototype.containData = function(t) {
      return this._radiusAxis.containData(t[0]) && this._angleAxis.containData(t[1]);
    }, r.prototype.getAxis = function(t) {
      var e = "_" + t + "Axis";
      return this[e];
    }, r.prototype.getAxes = function() {
      return [this._radiusAxis, this._angleAxis];
    }, r.prototype.getAxesByScale = function(t) {
      var e = [], a = this._angleAxis, i = this._radiusAxis;
      return a.scale.type === t && e.push(a), i.scale.type === t && e.push(i), e;
    }, r.prototype.getAngleAxis = function() {
      return this._angleAxis;
    }, r.prototype.getRadiusAxis = function() {
      return this._radiusAxis;
    }, r.prototype.getOtherAxis = function(t) {
      var e = this._angleAxis;
      return t === e ? this._radiusAxis : e;
    }, r.prototype.getBaseAxis = function() {
      return this.getAxesByScale("ordinal")[0] || this.getAxesByScale("time")[0] || this.getAngleAxis();
    }, r.prototype.getTooltipAxes = function(t) {
      var e = t != null && t !== "auto" ? this.getAxis(t) : this.getBaseAxis();
      return {
        baseAxes: [e],
        otherAxes: [this.getOtherAxis(e)]
      };
    }, r.prototype.dataToPoint = function(t, e) {
      return this.coordToPoint([this._radiusAxis.dataToRadius(t[0], e), this._angleAxis.dataToAngle(t[1], e)]);
    }, r.prototype.pointToData = function(t, e) {
      var a = this.pointToCoord(t);
      return [this._radiusAxis.radiusToData(a[0], e), this._angleAxis.angleToData(a[1], e)];
    }, r.prototype.pointToCoord = function(t) {
      var e = t[0] - this.cx, a = t[1] - this.cy, i = this.getAngleAxis(), n = i.getExtent(), o = Math.min(n[0], n[1]), s = Math.max(n[0], n[1]);
      i.inverse ? o = s - 360 : s = o + 360;
      var v = Math.sqrt(e * e + a * a);
      e /= v, a /= v;
      for (var u = Math.atan2(-a, e) / Math.PI * 180, l = u < o ? 1 : -1; u < o || u > s; )
        u += l * 360;
      return [v, u];
    }, r.prototype.coordToPoint = function(t) {
      var e = t[0], a = t[1] / 180 * Math.PI, i = Math.cos(a) * e + this.cx, n = -Math.sin(a) * e + this.cy;
      return [i, n];
    }, r.prototype.getArea = function() {
      var t = this.getAngleAxis(), e = this.getRadiusAxis(), a = e.getExtent().slice();
      a[0] > a[1] && a.reverse();
      var i = t.getExtent(), n = Math.PI / 180, o = 1e-4;
      return {
        cx: this.cx,
        cy: this.cy,
        r0: a[0],
        r: a[1],
        startAngle: -i[0] * n,
        endAngle: -i[1] * n,
        clockwise: t.inverse,
        contain: function(s, v) {
          var u = s - this.cx, l = v - this.cy, c = u * u + l * l, d = this.r, g = this.r0;
          return d !== g && c - o <= d * d && c + o >= g * g;
        }
      };
    }, r.prototype.convertToPixel = function(t, e, a) {
      var i = ge(e);
      return i === this ? this.dataToPoint(a) : null;
    }, r.prototype.convertFromPixel = function(t, e, a) {
      var i = ge(e);
      return i === this ? this.pointToData(a) : null;
    }, r;
  })()
);
function ge(r) {
  var t = r.seriesModel, e = r.polarModel;
  return e && e.coordinateSystem || t && t.coordinateSystem;
}
function ui(r, t, e) {
  var a = t.get("center"), i = e.getWidth(), n = e.getHeight();
  r.cx = W(a[0], i), r.cy = W(a[1], n);
  var o = r.getRadiusAxis(), s = Math.min(i, n) / 2, v = t.get("radius");
  v == null ? v = [0, "100%"] : G(v) || (v = [0, v]);
  var u = [W(v[0], s), W(v[1], s)];
  o.inverse ? o.setExtent(u[1], u[0]) : o.setExtent(u[0], u[1]);
}
function vi(r, t) {
  var e = this, a = e.getAngleAxis(), i = e.getRadiusAxis();
  if (a.scale.setExtent(1 / 0, -1 / 0), i.scale.setExtent(1 / 0, -1 / 0), r.eachSeries(function(s) {
    if (s.coordinateSystem === e) {
      var v = s.getData();
      w(Mt(v, "radius"), function(u) {
        i.scale.unionExtentFromData(v, u);
      }), w(Mt(v, "angle"), function(u) {
        a.scale.unionExtentFromData(v, u);
      });
    }
  }), ht(a.scale, a.model), ht(i.scale, i.model), a.type === "category" && !a.onBand) {
    var n = a.getExtent(), o = 360 / a.scale.count();
    a.inverse ? n[1] += o : n[1] -= o, a.setExtent(n[0], n[1]);
  }
}
function ci(r) {
  return r.mainType === "angleAxis";
}
function fe(r, t) {
  var e;
  if (r.type = t.get("type"), r.scale = Te(t), r.onBand = t.get("boundaryGap") && r.type === "category", r.inverse = t.get("inverse"), ci(t)) {
    r.inverse = r.inverse !== t.get("clockwise");
    var a = t.get("startAngle"), i = (e = t.get("endAngle")) !== null && e !== void 0 ? e : a + (r.inverse ? -360 : 360);
    r.setExtent(a, i);
  }
  t.axis = r, r.model = t;
}
var hi = {
  dimensions: Xe,
  create: function(r, t) {
    var e = [];
    return r.eachComponent("polar", function(a, i) {
      var n = new li(i + "");
      n.update = vi;
      var o = n.getRadiusAxis(), s = n.getAngleAxis(), v = a.findAxisModel("radiusAxis"), u = a.findAxisModel("angleAxis");
      fe(o, v), fe(s, u), ui(n, a, t), e.push(n), a.coordinateSystem = n, n.model = a;
    }), r.eachSeries(function(a) {
      if (a.get("coordinateSystem") === "polar") {
        var i = a.getReferringComponents("polar", Q).models[0];
        a.coordinateSystem = i.coordinateSystem;
      }
    }), e;
  }
}, di = ["axisLine", "axisLabel", "axisTick", "minorTick", "splitLine", "minorSplitLine", "splitArea"];
function st(r, t, e) {
  t[1] > t[0] && (t = t.slice().reverse());
  var a = r.coordToPoint([t[0], e]), i = r.coordToPoint([t[1], e]);
  return {
    x1: a[0],
    y1: a[1],
    x2: i[0],
    y2: i[1]
  };
}
function lt(r) {
  var t = r.getRadiusAxis();
  return t.inverse ? 0 : 1;
}
function pe(r) {
  var t = r[0], e = r[r.length - 1];
  t && e && Math.abs(Math.abs(t.coord - e.coord) - 360) < 1e-4 && r.pop();
}
var gi = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e.axisPointerClass = "PolarAxisPointer", e;
    }
    return t.prototype.render = function(e, a) {
      if (this.group.removeAll(), !!e.get("show")) {
        var i = e.axis, n = i.polar, o = n.getRadiusAxis().getExtent(), s = i.getTicksCoords(), v = i.getMinorTicksCoords(), u = U(i.getViewLabels(), function(l) {
          l = Ir(l);
          var c = i.scale, d = c.type === "ordinal" ? c.getRawOrdinalNumber(l.tickValue) : l.tickValue;
          return l.coord = i.dataToCoord(d), l;
        });
        pe(u), pe(s), w(di, function(l) {
          e.get([l, "show"]) && (!i.scale.isBlank() || l === "axisLine") && fi[l](this.group, e, n, s, v, o, u);
        }, this);
      }
    }, t.type = "angleAxis", t;
  })(yt)
), fi = {
  axisLine: function(r, t, e, a, i, n) {
    var o = t.getModel(["axisLine", "lineStyle"]), s = e.getAngleAxis(), v = Math.PI / 180, u = s.getExtent(), l = lt(e), c = l ? 0 : 1, d, g = Math.abs(u[1] - u[0]) === 360 ? "Circle" : "Arc";
    n[c] === 0 ? d = new Ot[g]({
      shape: {
        cx: e.cx,
        cy: e.cy,
        r: n[l],
        startAngle: -u[0] * v,
        endAngle: -u[1] * v,
        clockwise: s.inverse
      },
      style: o.getLineStyle(),
      z2: 1,
      silent: !0
    }) : d = new Or({
      shape: {
        cx: e.cx,
        cy: e.cy,
        r: n[l],
        r0: n[c]
      },
      style: o.getLineStyle(),
      z2: 1,
      silent: !0
    }), d.style.fill = null, r.add(d);
  },
  axisTick: function(r, t, e, a, i, n) {
    var o = t.getModel("axisTick"), s = (o.get("inside") ? -1 : 1) * o.get("length"), v = n[lt(e)], u = U(a, function(l) {
      return new Z({
        shape: st(e, [v, v + s], l.coord)
      });
    });
    r.add(V(u, {
      style: O(o.getModel("lineStyle").getLineStyle(), {
        stroke: t.get(["axisLine", "lineStyle", "color"])
      })
    }));
  },
  minorTick: function(r, t, e, a, i, n) {
    if (i.length) {
      for (var o = t.getModel("axisTick"), s = t.getModel("minorTick"), v = (o.get("inside") ? -1 : 1) * s.get("length"), u = n[lt(e)], l = [], c = 0; c < i.length; c++)
        for (var d = 0; d < i[c].length; d++)
          l.push(new Z({
            shape: st(e, [u, u + v], i[c][d].coord)
          }));
      r.add(V(l, {
        style: O(s.getModel("lineStyle").getLineStyle(), O(o.getLineStyle(), {
          stroke: t.get(["axisLine", "lineStyle", "color"])
        }))
      }));
    }
  },
  axisLabel: function(r, t, e, a, i, n, o) {
    var s = t.getCategories(!0), v = t.getModel("axisLabel"), u = v.get("margin"), l = t.get("triggerEvent");
    w(o, function(c, d) {
      var g = v, h = c.tickValue, f = n[lt(e)], p = e.coordToPoint([f + u, c.coord]), y = e.cx, x = e.cy, m = Math.abs(p[0] - y) / f < 0.3 ? "center" : p[0] > y ? "left" : "right", A = Math.abs(p[1] - x) / f < 0.3 ? "middle" : p[1] > x ? "top" : "bottom";
      if (s && s[h]) {
        var S = s[h];
        Bt(S) && S.textStyle && (g = new Rr(S.textStyle, v, v.ecModel));
      }
      var b = new kr({
        silent: J.isLabelSilent(t),
        style: Br(g, {
          x: p[0],
          y: p[1],
          fill: g.getTextColor() || t.get(["axisLine", "lineStyle", "color"]),
          text: c.formattedLabel,
          align: m,
          verticalAlign: A
        })
      });
      if (r.add(b), l) {
        var C = J.makeAxisEventDataBase(t);
        C.targetType = "axisLabel", C.value = c.rawLabel, ft(b).eventData = C;
      }
    }, this);
  },
  splitLine: function(r, t, e, a, i, n) {
    var o = t.getModel("splitLine"), s = o.getModel("lineStyle"), v = s.get("color"), u = 0;
    v = v instanceof Array ? v : [v];
    for (var l = [], c = 0; c < a.length; c++) {
      var d = u++ % v.length;
      l[d] = l[d] || [], l[d].push(new Z({
        shape: st(e, n, a[c].coord)
      }));
    }
    for (var c = 0; c < l.length; c++)
      r.add(V(l[c], {
        style: O({
          stroke: v[c % v.length]
        }, s.getLineStyle()),
        silent: !0,
        z: t.get("z")
      }));
  },
  minorSplitLine: function(r, t, e, a, i, n) {
    if (i.length) {
      for (var o = t.getModel("minorSplitLine"), s = o.getModel("lineStyle"), v = [], u = 0; u < i.length; u++)
        for (var l = 0; l < i[u].length; l++)
          v.push(new Z({
            shape: st(e, n, i[u][l].coord)
          }));
      r.add(V(v, {
        style: s.getLineStyle(),
        silent: !0,
        z: t.get("z")
      }));
    }
  },
  splitArea: function(r, t, e, a, i, n) {
    if (a.length) {
      var o = t.getModel("splitArea"), s = o.getModel("areaStyle"), v = s.get("color"), u = 0;
      v = v instanceof Array ? v : [v];
      for (var l = [], c = Math.PI / 180, d = -a[0].coord * c, g = Math.min(n[0], n[1]), h = Math.max(n[0], n[1]), f = t.get("clockwise"), p = 1, y = a.length; p <= y; p++) {
        var x = p === y ? a[0].coord : a[p].coord, m = u++ % v.length;
        l[m] = l[m] || [], l[m].push(new tt({
          shape: {
            cx: e.cx,
            cy: e.cy,
            r0: g,
            r: h,
            startAngle: d,
            endAngle: -x * c,
            clockwise: f
          },
          silent: !0
        })), d = -x * c;
      }
      for (var p = 0; p < l.length; p++)
        r.add(V(l[p], {
          style: O({
            fill: v[p % v.length]
          }, s.getAreaStyle()),
          silent: !0
        }));
    }
  }
}, pi = ["axisLine", "axisTickLabel", "axisName"], yi = ["splitLine", "splitArea", "minorSplitLine"], xi = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e.axisPointerClass = "PolarAxisPointer", e;
    }
    return t.prototype.render = function(e, a) {
      if (this.group.removeAll(), !!e.get("show")) {
        var i = this._axisGroup, n = this._axisGroup = new It();
        this.group.add(n);
        var o = e.axis, s = o.polar, v = s.getAngleAxis(), u = o.getTicksCoords(), l = o.getMinorTicksCoords(), c = v.getExtent()[0], d = o.getExtent(), g = Ai(s, e, c), h = new J(e, g);
        w(pi, h.add, h), n.add(h.getGroup()), Ee(i, n, e), w(yi, function(f) {
          e.get([f, "show"]) && !o.scale.isBlank() && mi[f](this.group, e, s, c, d, u, l);
        }, this);
      }
    }, t.type = "radiusAxis", t;
  })(yt)
), mi = {
  splitLine: function(r, t, e, a, i, n) {
    var o = t.getModel("splitLine"), s = o.getModel("lineStyle"), v = s.get("color"), u = 0, l = e.getAngleAxis(), c = Math.PI / 180, d = l.getExtent(), g = Math.abs(d[1] - d[0]) === 360 ? "Circle" : "Arc";
    v = v instanceof Array ? v : [v];
    for (var h = [], f = 0; f < n.length; f++) {
      var p = u++ % v.length;
      h[p] = h[p] || [], h[p].push(new Ot[g]({
        shape: {
          cx: e.cx,
          cy: e.cy,
          // ensure circle radius >= 0
          r: Math.max(n[f].coord, 0),
          startAngle: -d[0] * c,
          endAngle: -d[1] * c,
          clockwise: l.inverse
        }
      }));
    }
    for (var f = 0; f < h.length; f++)
      r.add(V(h[f], {
        style: O({
          stroke: v[f % v.length],
          fill: null
        }, s.getLineStyle()),
        silent: !0
      }));
  },
  minorSplitLine: function(r, t, e, a, i, n, o) {
    if (o.length) {
      for (var s = t.getModel("minorSplitLine"), v = s.getModel("lineStyle"), u = [], l = 0; l < o.length; l++)
        for (var c = 0; c < o[l].length; c++)
          u.push(new Gr({
            shape: {
              cx: e.cx,
              cy: e.cy,
              r: o[l][c].coord
            }
          }));
      r.add(V(u, {
        style: O({
          fill: null
        }, v.getLineStyle()),
        silent: !0
      }));
    }
  },
  splitArea: function(r, t, e, a, i, n) {
    if (n.length) {
      var o = t.getModel("splitArea"), s = o.getModel("areaStyle"), v = s.get("color"), u = 0;
      v = v instanceof Array ? v : [v];
      for (var l = [], c = n[0].coord, d = 1; d < n.length; d++) {
        var g = u++ % v.length;
        l[g] = l[g] || [], l[g].push(new tt({
          shape: {
            cx: e.cx,
            cy: e.cy,
            r0: c,
            r: n[d].coord,
            startAngle: 0,
            endAngle: Math.PI * 2
          },
          silent: !0
        })), c = n[d].coord;
      }
      for (var d = 0; d < l.length; d++)
        r.add(V(l[d], {
          style: O({
            fill: v[d % v.length]
          }, s.getAreaStyle()),
          silent: !0
        }));
    }
  }
};
function Ai(r, t, e) {
  return {
    position: [r.cx, r.cy],
    rotation: e / 180 * Math.PI,
    labelDirection: -1,
    tickDirection: -1,
    nameDirection: 1,
    labelRotate: t.getModel("axisLabel").get("rotate"),
    // Over splitLine and splitArea
    z2: 1
  };
}
function $e(r) {
  return r.get("stack") || "__ec_stack_" + r.seriesIndex;
}
function Ze(r, t) {
  return t.dim + r.model.componentIndex;
}
function Si(r, t, e) {
  var a = {}, i = bi(Wr(t.getSeriesByType(r), function(n) {
    return !t.isSeriesFiltered(n) && n.coordinateSystem && n.coordinateSystem.type === "polar";
  }));
  t.eachSeriesByType(r, function(n) {
    if (n.coordinateSystem.type === "polar") {
      var o = n.getData(), s = n.coordinateSystem, v = s.getBaseAxis(), u = Ze(s, v), l = $e(n), c = i[u][l], d = c.offset, g = c.width, h = s.getOtherAxis(v), f = n.coordinateSystem.cx, p = n.coordinateSystem.cy, y = n.get("barMinHeight") || 0, x = n.get("barMinAngle") || 0;
      a[l] = a[l] || [];
      for (var m = o.mapDimension(h.dim), A = o.mapDimension(v.dim), S = Nr(
        o,
        m
        /* , baseDim */
      ), b = v.dim !== "radius" || !n.get("roundCap", !0), C = h.model, D = C.get("startValue"), I = h.dataToCoord(D || 0), R = 0, M = o.count(); R < M; R++) {
        var k = o.get(m, R), T = o.get(A, R), B = k >= 0 ? "p" : "n", P = I;
        S && (a[l][T] || (a[l][T] = {
          p: I,
          n: I
          // Negative stack
        }), P = a[l][T][B]);
        var X = void 0, $ = void 0, _ = void 0, N = void 0;
        if (h.dim === "radius") {
          var z = h.dataToCoord(k) - I, j = v.dataToCoord(T);
          Math.abs(z) < y && (z = (z < 0 ? -1 : 1) * y), X = P, $ = P + z, _ = j - d, N = _ - g, S && (a[l][T][B] = $);
        } else {
          var at = h.dataToCoord(k, b) - I, je = v.dataToCoord(T);
          Math.abs(at) < x && (at = (at < 0 ? -1 : 1) * x), X = je + d, $ = X + g, _ = P, N = P + at, S && (a[l][T][B] = N);
        }
        o.setItemLayout(R, {
          cx: f,
          cy: p,
          r0: X,
          r: $,
          // Consider that positive angle is anti-clockwise,
          // while positive radian of sector is clockwise
          startAngle: -_ * Math.PI / 180,
          endAngle: -N * Math.PI / 180,
          /**
           * Keep the same logic with bar in catesion: use end value to
           * control direction. Notice that if clockwise is true (by
           * default), the sector will always draw clockwisely, no matter
           * whether endAngle is greater or less than startAngle.
           */
          clockwise: _ >= N
        });
      }
    }
  });
}
function bi(r) {
  var t = {};
  w(r, function(a, i) {
    var n = a.getData(), o = a.coordinateSystem, s = o.getBaseAxis(), v = Ze(o, s), u = s.getExtent(), l = s.type === "category" ? s.getBandWidth() : Math.abs(u[1] - u[0]) / n.count(), c = t[v] || {
      bandWidth: l,
      remainedWidth: l,
      autoWidthCount: 0,
      categoryGap: "20%",
      gap: "30%",
      stacks: {}
    }, d = c.stacks;
    t[v] = c;
    var g = $e(a);
    d[g] || c.autoWidthCount++, d[g] = d[g] || {
      width: 0,
      maxWidth: 0
    };
    var h = W(a.get("barWidth"), l), f = W(a.get("barMaxWidth"), l), p = a.get("barGap"), y = a.get("barCategoryGap");
    h && !d[g].width && (h = Math.min(c.remainedWidth, h), d[g].width = h, c.remainedWidth -= h), f && (d[g].maxWidth = f), p != null && (c.gap = p), y != null && (c.categoryGap = y);
  });
  var e = {};
  return w(t, function(a, i) {
    e[i] = {};
    var n = a.stacks, o = a.bandWidth, s = W(a.categoryGap, o), v = W(a.gap, 1), u = a.remainedWidth, l = a.autoWidthCount, c = (u - s) / (l + (l - 1) * v);
    c = Math.max(c, 0), w(n, function(f, p) {
      var y = f.maxWidth;
      y && y < c && (y = Math.min(y, u), f.width && (y = Math.min(y, f.width)), u -= y, f.width = y, l--);
    }), c = (u - s) / (l + (l - 1) * v), c = Math.max(c, 0);
    var d = 0, g;
    w(n, function(f, p) {
      f.width || (f.width = c), g = f, d += f.width * (1 + v);
    }), g && (d -= g.width * v);
    var h = -d / 2;
    w(n, function(f, p) {
      e[i][p] = e[i][p] || {
        offset: h,
        width: f.width
      }, h += f.width * (1 + v);
    });
  }), e;
}
var Ci = {
  startAngle: 90,
  clockwise: !0,
  splitNumber: 12,
  axisLabel: {
    rotate: 0
  }
}, Mi = {
  splitNumber: 5
}, wi = (
  /** @class */
  (function(r) {
    L(t, r);
    function t() {
      var e = r !== null && r.apply(this, arguments) || this;
      return e.type = t.type, e;
    }
    return t.type = "polar", t;
  })(Ie)
);
function Li(r) {
  dt(ke), yt.registerAxisPointerClass("PolarAxisPointer", ei), r.registerCoordinateSystem("polar", hi), r.registerComponentModel(ii), r.registerComponentView(wi), gt(r, "angle", ni, Ci), gt(r, "radius", oi, Mi), r.registerComponentView(gi), r.registerComponentView(xi), r.registerLayout(Rt(Si, "bar"));
}
var ye = {
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
}, Ti = (
  /** @class */
  (function() {
    function r(t) {
      var e = this._condVal = vt(t) ? new RegExp(t) : Fr(t) ? t : null;
      if (e == null) {
        var a = "";
        E(a);
      }
    }
    return r.prototype.evaluate = function(t) {
      var e = typeof t;
      return vt(e) ? this._condVal.test(t) : Se(e) ? this._condVal.test(t + "") : !1;
    }, r;
  })()
), Di = (
  /** @class */
  (function() {
    function r() {
    }
    return r.prototype.evaluate = function() {
      return this.value;
    }, r;
  })()
), Pi = (
  /** @class */
  (function() {
    function r() {
    }
    return r.prototype.evaluate = function() {
      for (var t = this.children, e = 0; e < t.length; e++)
        if (!t[e].evaluate())
          return !1;
      return !0;
    }, r;
  })()
), _i = (
  /** @class */
  (function() {
    function r() {
    }
    return r.prototype.evaluate = function() {
      for (var t = this.children, e = 0; e < t.length; e++)
        if (t[e].evaluate())
          return !0;
      return !1;
    }, r;
  })()
), Ei = (
  /** @class */
  (function() {
    function r() {
    }
    return r.prototype.evaluate = function() {
      return !this.child.evaluate();
    }, r;
  })()
), Ii = (
  /** @class */
  (function() {
    function r() {
    }
    return r.prototype.evaluate = function() {
      for (var t = !!this.valueParser, e = this.getValue, a = e(this.valueGetterParam), i = t ? this.valueParser(a) : null, n = 0; n < this.subCondList.length; n++)
        if (!this.subCondList[n].evaluate(t ? i : a))
          return !1;
      return !0;
    }, r;
  })()
);
function zt(r, t) {
  if (r === !0 || r === !1) {
    var e = new Di();
    return e.value = r, e;
  }
  var a = "";
  return Ue(r) || E(a), r.and ? xe("and", r, t) : r.or ? xe("or", r, t) : r.not ? Ri(r, t) : ki(r, t);
}
function xe(r, t, e) {
  var a = t[r], i = "";
  G(a) || E(i), a.length || E(i);
  var n = r === "and" ? new Pi() : new _i();
  return n.children = U(a, function(o) {
    return zt(o, e);
  }), n.children.length || E(i), n;
}
function Ri(r, t) {
  var e = r.not, a = "";
  Ue(e) || E(a);
  var i = new Ei();
  return i.child = zt(e, t), i.child || E(a), i;
}
function ki(r, t) {
  for (var e = "", a = t.prepareGetValue(r), i = [], n = De(r), o = r.parser, s = o ? Be(o) : null, v = 0; v < n.length; v++) {
    var u = n[v];
    if (!(u === "parser" || t.valueGetterAttrMap.get(u))) {
      var l = Oe(ye, u) ? ye[u] : u, c = r[u], d = s ? s(c) : c, g = zr(l, d) || l === "reg" && new Ti(d);
      g || E(e), i.push(g);
    }
  }
  i.length || E(e);
  var h = new Ii();
  return h.valueGetterParam = a, h.valueParser = s, h.getValue = t.getValue, h.subCondList = i, h;
}
function Ue(r) {
  return Bt(r) && !Vr(r);
}
var Bi = (
  /** @class */
  (function() {
    function r(t, e) {
      this._cond = zt(t, e);
    }
    return r.prototype.evaluate = function() {
      return this._cond.evaluate();
    }, r;
  })()
);
function Oi(r, t) {
  return new Bi(r, t);
}
var Gi = {
  type: "echarts:filter",
  // PENDING: enhance to filter by index rather than create new data
  transform: function(r) {
    for (var t = r.upstream, e, a = Oi(r.config, {
      valueGetterAttrMap: Pe({
        dimension: !0
      }),
      prepareGetValue: function(s) {
        var v = "", u = s.dimension;
        Oe(s, "dimension") || E(v);
        var l = t.getDimensionInfo(u);
        return l || E(v), {
          dimIdx: l.index
        };
      },
      getValue: function(s) {
        return t.retrieveValueFromItem(e, s.dimIdx);
      }
    }), i = [], n = 0, o = t.count(); n < o; n++)
      e = t.getRawDataItem(n), a.evaluate() && i.push(e);
    return {
      data: i
    };
  }
}, Wi = {
  type: "echarts:sort",
  transform: function(r) {
    var t = r.upstream, e = r.config, a = "", i = Xr(e);
    i.length || E(a);
    var n = [];
    w(i, function(l) {
      var c = l.dimension, d = l.order, g = l.parser, h = l.incomparable;
      if (c == null && E(a), d !== "asc" && d !== "desc" && E(a), h && h !== "min" && h !== "max") {
        var f = "";
        E(f);
      }
      if (d !== "asc" && d !== "desc") {
        var p = "";
        E(p);
      }
      var y = t.getDimensionInfo(c);
      y || E(a);
      var x = g ? Be(g) : null;
      g && !x && E(a), n.push({
        dimIdx: y.index,
        parser: x,
        comparator: new Hr(d, h)
      });
    });
    var o = t.sourceFormat;
    o !== Re && o !== Yr && E(a);
    for (var s = [], v = 0, u = t.count(); v < u; v++)
      s.push(t.getRawDataItem(v));
    return s.sort(function(l, c) {
      for (var d = 0; d < n.length; d++) {
        var g = n[d], h = t.retrieveValueFromItem(l, g.dimIdx), f = t.retrieveValueFromItem(c, g.dimIdx);
        g.parser && (h = g.parser(h), f = g.parser(f));
        var p = g.comparator.evaluate(h, f);
        if (p !== 0)
          return p;
      }
      return 0;
    }), {
      data: s
    };
  }
};
function Ni(r) {
  r.registerTransform(Gi), r.registerTransform(Wi);
}
dt([
  Sa,
  Ja,
  Ur,
  ti,
  jr,
  Li,
  Kr,
  Ni,
  qr
]);
function Hi({ echartsRef: r, option: t }) {
  return /* @__PURE__ */ Ke.jsx(
    $r,
    {
      ref: r,
      echarts: Zr,
      option: t,
      style: { width: "100%", height: "100%" },
      notMerge: !0
    }
  );
}
export {
  Hi as default
};
