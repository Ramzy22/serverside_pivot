import { j as Es } from "./index-I9cvqvRS.js";
import "./charts-DthW1Q8W.js";
import { c5 as jn, b3 as fn, c6 as Ss, ai as _t, c7 as Zn, ap as As, c8 as qe, P as Cs, c9 as Yn, ca as Ai, cb as Ms, cc as Ls, cd as Ds, ce as cr, bh as Cn, aq as Kn, cf as Ps, c as Qn, aS as Rs, a3 as $r, E as $n, F as rt, cg as Ns, ch as Ci, ci as Jn, cj as Os, ck as Is, cl as Bs, a1 as Fs, Y as ea, aL as hn, Z as ta, cm as Us, bw as mi, c4 as mo, c1 as zs, cn as Hs, co as _o, A as Mn, a4 as _i, C as Ln, bm as go, ae as yo, bN as xo, cp as Gs, K as gi, cq as Vs, cr as ks, cs as Ws, ct as Xs, cu as qs, cv as js, cw as Zs, cx as Ys, cy as Ks, cz as Qs, cA as $s, cB as Js, cC as el, cD as To, cE as tl, cF as rl, cG as il, cH as nl, cI as al, cJ as Dn, cK as ol, cL as sl, cM as ra, cN as ll, cO as ul, cP as fl, cQ as hl, cR as cl, cS as dl, cT as vl, cU as pl, cV as ml, cW as _l, cX as gl, cY as yl, cZ as xl, c_ as Tl, c$ as wl, d0 as bl, ac as El, d1 as Sl, d2 as Al, d3 as Cl, d4 as Ml, d5 as Ll, d6 as Dl, i as it, V as ia, d7 as wo, R as bo, d8 as Pn, d9 as Pl, da as Lr, db as na, aK as Eo, dc as Rl, dd as Nl, de as Ol, aj as lt, df as Il, bO as ni, bP as ai, dg as So, dh as Bl, di as Fl, as as Ao, dj as Co, aJ as Ul, dk as zl, dl as Hl, bL as Gl, al as Vl, bo as kl, b$ as Wl, c0 as Xl, c2 as ql, c3 as jl } from "./installCanvasRenderer-ui7Bv_08.js";
var ir = Math.PI * 2, xt = Ms.CMD, Zl = ["top", "right", "bottom", "left"];
function Yl(e, t, r, i, n) {
  var a = r.width, o = r.height;
  switch (e) {
    case "top":
      i.set(r.x + a / 2, r.y - t), n.set(0, -1);
      break;
    case "bottom":
      i.set(r.x + a / 2, r.y + o + t), n.set(0, 1);
      break;
    case "left":
      i.set(r.x - t, r.y + o / 2), n.set(-1, 0);
      break;
    case "right":
      i.set(r.x + a + t, r.y + o / 2), n.set(1, 0);
      break;
  }
}
function Kl(e, t, r, i, n, a, o, s, l) {
  o -= e, s -= t;
  var u = Math.sqrt(o * o + s * s);
  o /= u, s /= u;
  var f = o * r + e, h = s * r + t;
  if (Math.abs(i - n) % ir < 1e-4)
    return l[0] = f, l[1] = h, u - r;
  if (a) {
    var d = i;
    i = cr(n), n = cr(d);
  } else
    i = cr(i), n = cr(n);
  i > n && (n += ir);
  var c = Math.atan2(s, o);
  if (c < 0 && (c += ir), c >= i && c <= n || c + ir >= i && c + ir <= n)
    return l[0] = f, l[1] = h, u - r;
  var v = r * Math.cos(i) + e, p = r * Math.sin(i) + t, m = r * Math.cos(n) + e, _ = r * Math.sin(n) + t, x = (v - o) * (v - o) + (p - s) * (p - s), y = (m - o) * (m - o) + (_ - s) * (_ - s);
  return x < y ? (l[0] = v, l[1] = p, Math.sqrt(x)) : (l[0] = m, l[1] = _, Math.sqrt(y));
}
function cn(e, t, r, i, n, a, o, s) {
  var l = n - e, u = a - t, f = r - e, h = i - t, d = Math.sqrt(f * f + h * h);
  f /= d, h /= d;
  var c = l * f + u * h, v = c / d;
  s && (v = Math.min(Math.max(v, 0), 1)), v *= d;
  var p = o[0] = e + v * f, m = o[1] = t + v * h;
  return Math.sqrt((p - n) * (p - n) + (m - a) * (m - a));
}
function Mo(e, t, r, i, n, a, o) {
  r < 0 && (e = e + r, r = -r), i < 0 && (t = t + i, i = -i);
  var s = e + r, l = t + i, u = o[0] = Math.min(Math.max(n, e), s), f = o[1] = Math.min(Math.max(a, t), l);
  return Math.sqrt((u - n) * (u - n) + (f - a) * (f - a));
}
var Xe = [];
function Ql(e, t, r) {
  var i = Mo(t.x, t.y, t.width, t.height, e.x, e.y, Xe);
  return r.set(Xe[0], Xe[1]), i;
}
function $l(e, t, r) {
  for (var i = 0, n = 0, a = 0, o = 0, s, l, u = 1 / 0, f = t.data, h = e.x, d = e.y, c = 0; c < f.length; ) {
    var v = f[c++];
    c === 1 && (i = f[c], n = f[c + 1], a = i, o = n);
    var p = u;
    switch (v) {
      case xt.M:
        a = f[c++], o = f[c++], i = a, n = o;
        break;
      case xt.L:
        p = cn(i, n, f[c], f[c + 1], h, d, Xe, !0), i = f[c++], n = f[c++];
        break;
      case xt.C:
        p = Ds(i, n, f[c++], f[c++], f[c++], f[c++], f[c], f[c + 1], h, d, Xe), i = f[c++], n = f[c++];
        break;
      case xt.Q:
        p = Ls(i, n, f[c++], f[c++], f[c], f[c + 1], h, d, Xe), i = f[c++], n = f[c++];
        break;
      case xt.A:
        var m = f[c++], _ = f[c++], x = f[c++], y = f[c++], g = f[c++], b = f[c++];
        c += 1;
        var w = !!(1 - f[c++]);
        s = Math.cos(g) * x + m, l = Math.sin(g) * y + _, c <= 1 && (a = s, o = l);
        var C = (h - m) * y / x + m;
        p = Kl(m, _, y, g, g + b, w, C, d, Xe), i = Math.cos(g + b) * x + m, n = Math.sin(g + b) * y + _;
        break;
      case xt.R:
        a = i = f[c++], o = n = f[c++];
        var A = f[c++], R = f[c++];
        p = Mo(a, o, A, R, h, d, Xe);
        break;
      case xt.Z:
        p = cn(i, n, a, o, h, d, Xe, !0), i = a, n = o;
        break;
    }
    p < u && (u = p, r.set(Xe[0], Xe[1]));
  }
  return u;
}
var Mt = new qe(), Te = new qe(), Ue = new qe(), Wt = new qe(), nr = new qe();
function aa(e, t) {
  if (e) {
    var r = e.getTextGuideLine(), i = e.getTextContent();
    if (i && r) {
      var n = e.textGuideLineConfig || {}, a = [[0, 0], [0, 0], [0, 0]], o = n.candidates || Zl, s = i.getBoundingRect().clone();
      s.applyTransform(i.getComputedTransform());
      var l = 1 / 0, u = n.anchor, f = e.getComputedTransform(), h = f && As([], f), d = t.get("length2") || 0;
      u && Ue.copy(u);
      for (var c = 0; c < o.length; c++) {
        var v = o[c];
        Yl(v, 0, s, Mt, Wt), qe.scaleAndAdd(Te, Mt, Wt, d), Te.transform(h);
        var p = e.getBoundingRect(), m = u ? u.distance(Te) : e instanceof Cs ? $l(Te, e.path, Ue) : Ql(Te, p, Ue);
        m < l && (l = m, Te.transform(f), Ue.transform(f), Ue.toArray(a[0]), Te.toArray(a[1]), Mt.toArray(a[2]));
      }
      Jl(a, t.get("minTurnAngle")), r.setShape({
        points: a
      });
    }
  }
}
var oa = [], Tt = new qe();
function Jl(e, t) {
  if (t <= 180 && t > 0) {
    t = t / 180 * Math.PI, Mt.fromArray(e[0]), Te.fromArray(e[1]), Ue.fromArray(e[2]), qe.sub(Wt, Mt, Te), qe.sub(nr, Ue, Te);
    var r = Wt.len(), i = nr.len();
    if (!(r < 1e-3 || i < 1e-3)) {
      Wt.scale(1 / r), nr.scale(1 / i);
      var n = Wt.dot(nr), a = Math.cos(t);
      if (a < n) {
        var o = cn(Te.x, Te.y, Ue.x, Ue.y, Mt.x, Mt.y, oa, !1);
        Tt.fromArray(oa), Tt.scaleAndAdd(nr, o / Math.tan(Math.PI - t));
        var s = Ue.x !== Te.x ? (Tt.x - Te.x) / (Ue.x - Te.x) : (Tt.y - Te.y) / (Ue.y - Te.y);
        if (isNaN(s))
          return;
        s < 0 ? qe.copy(Tt, Te) : s > 1 && qe.copy(Tt, Ue), Tt.toArray(e[1]);
      }
    }
  }
}
function Mi(e, t, r, i) {
  var n = r === "normal", a = n ? e : e.ensureState(r);
  a.ignore = t;
  var o = i.get("smooth");
  o = o === !0 ? 0.3 : Math.max(+o, 0) || 0, a.shape = a.shape || {}, a.shape.smooth = o;
  var s = i.getModel("lineStyle").getLineStyle();
  n ? e.useStyle(s) : a.style = s;
}
function eu(e, t) {
  var r = t.smooth, i = t.points;
  if (i)
    if (e.moveTo(i[0][0], i[0][1]), r > 0 && i.length >= 3) {
      var n = Yn(i[0], i[1]), a = Yn(i[1], i[2]);
      if (!n || !a) {
        e.lineTo(i[1][0], i[1][1]), e.lineTo(i[2][0], i[2][1]);
        return;
      }
      var o = Math.min(n, a) * r, s = Ai([], i[1], i[0], o / n), l = Ai([], i[1], i[2], o / a), u = Ai([], s, l, 0.5);
      e.bezierCurveTo(s[0], s[1], s[0], s[1], u[0], u[1]), e.bezierCurveTo(l[0], l[1], l[0], l[1], i[2][0], i[2][1]);
    } else
      for (var f = 1; f < i.length; f++)
        e.lineTo(i[f][0], i[f][1]);
}
function tu(e, t, r) {
  var i = e.getTextGuideLine(), n = e.getTextContent();
  if (!n) {
    i && e.removeTextGuideLine();
    return;
  }
  for (var a = t.normal, o = a.get("show"), s = n.ignore, l = 0; l < jn.length; l++) {
    var u = jn[l], f = t[u], h = u === "normal";
    if (f) {
      var d = f.get("show"), c = h ? s : fn(n.states[u] && n.states[u].ignore, s);
      if (c || !fn(d, o)) {
        var v = h ? i : i && i.states[u];
        v && (v.ignore = !0), i && Mi(i, !0, u, f);
        continue;
      }
      i || (i = new Ss(), e.setTextGuideLine(i), !h && (s || !o) && Mi(i, !0, "normal", t.normal), e.stateProxy && (i.stateProxy = e.stateProxy)), Mi(i, !1, u, f);
    }
  }
  if (i) {
    _t(i.style, r), i.style.fill = null;
    var p = a.get("showAbove"), m = e.textGuideLineConfig = e.textGuideLineConfig || {};
    m.showAbove = p || !1, i.buildPath = eu;
  }
}
function ru(e, t) {
  t = t || "labelLine";
  for (var r = {
    normal: e.getModel(t)
  }, i = 0; i < Zn.length; i++) {
    var n = Zn[i];
    r[n] = e.getModel([n, t]);
  }
  return r;
}
function iu(e) {
  if (e) {
    for (var t = [], r = 0; r < e.length; r++)
      t.push(e[r].slice());
    return t;
  }
}
function nu(e, t) {
  var r = e.label, i = t && t.getTextGuideLine();
  return {
    dataIndex: e.dataIndex,
    dataType: e.dataType,
    seriesIndex: e.seriesModel.seriesIndex,
    text: e.label.style.text,
    rect: e.hostRect,
    labelRect: e.rect,
    // x: labelAttr.x,
    // y: labelAttr.y,
    align: r.style.align,
    verticalAlign: r.style.verticalAlign,
    labelLinePoints: iu(i && i.shape.points)
  };
}
var sa = ["align", "verticalAlign", "width", "height", "fontSize"], Me = new Ps(), Li = Cn(), au = Cn();
function Dr(e, t, r) {
  for (var i = 0; i < r.length; i++) {
    var n = r[i];
    t[n] != null && (e[n] = t[n]);
  }
}
var Pr = ["x", "y", "rotation"], ou = (
  /** @class */
  (function() {
    function e() {
      this._labelList = [], this._chartViewList = [];
    }
    return e.prototype.clearLabels = function() {
      this._labelList = [], this._chartViewList = [];
    }, e.prototype._addLabel = function(t, r, i, n, a) {
      var o = n.style, s = n.__hostTarget, l = s.textConfig || {}, u = n.getComputedTransform(), f = n.getBoundingRect().plain();
      Kn.applyTransform(f, f, u), u ? Me.setLocalTransform(u) : (Me.x = Me.y = Me.rotation = Me.originX = Me.originY = 0, Me.scaleX = Me.scaleY = 1), Me.rotation = cr(Me.rotation);
      var h = n.__hostTarget, d;
      if (h) {
        d = h.getBoundingRect().plain();
        var c = h.getComputedTransform();
        Kn.applyTransform(d, d, c);
      }
      var v = d && h.getTextGuideLine();
      this._labelList.push({
        label: n,
        labelLine: v,
        seriesModel: i,
        dataIndex: t,
        dataType: r,
        layoutOptionOrCb: a,
        layoutOption: null,
        rect: f,
        hostRect: d,
        // Label with lower priority will be hidden when overlapped
        // Use rect size as default priority
        priority: d ? d.width * d.height : 0,
        // Save default label attributes.
        // For restore if developers want get back to default value in callback.
        defaultAttr: {
          ignore: n.ignore,
          labelGuideIgnore: v && v.ignore,
          x: Me.x,
          y: Me.y,
          scaleX: Me.scaleX,
          scaleY: Me.scaleY,
          rotation: Me.rotation,
          style: {
            x: o.x,
            y: o.y,
            align: o.align,
            verticalAlign: o.verticalAlign,
            width: o.width,
            height: o.height,
            fontSize: o.fontSize
          },
          cursor: n.cursor,
          attachedPos: l.position,
          attachedRot: l.rotation
        }
      });
    }, e.prototype.addLabelsOfSeries = function(t) {
      var r = this;
      this._chartViewList.push(t);
      var i = t.__model, n = i.get("labelLayout");
      (Qn(n) || Rs(n).length) && t.group.traverse(function(a) {
        if (a.ignore)
          return !0;
        var o = a.getTextContent(), s = $r(a);
        o && !o.disableLabelLayout && r._addLabel(s.dataIndex, s.dataType, i, o, n);
      });
    }, e.prototype.updateLayoutConfig = function(t) {
      var r = t.getWidth(), i = t.getHeight();
      function n(y, g) {
        return function() {
          aa(y, g);
        };
      }
      for (var a = 0; a < this._labelList.length; a++) {
        var o = this._labelList[a], s = o.label, l = s.__hostTarget, u = o.defaultAttr, f = void 0;
        Qn(o.layoutOptionOrCb) ? f = o.layoutOptionOrCb(nu(o, l)) : f = o.layoutOptionOrCb, f = f || {}, o.layoutOption = f;
        var h = Math.PI / 180;
        l && l.setTextConfig({
          // Force to set local false.
          local: !1,
          // Ignore position and rotation config on the host el if x or y is changed.
          position: f.x != null || f.y != null ? null : u.attachedPos,
          // Ignore rotation config on the host el if rotation is changed.
          rotation: f.rotate != null ? f.rotate * h : u.attachedRot,
          offset: [f.dx || 0, f.dy || 0]
        });
        var d = !1;
        if (f.x != null ? (s.x = $n(f.x, r), s.setStyle("x", 0), d = !0) : (s.x = u.x, s.setStyle("x", u.style.x)), f.y != null ? (s.y = $n(f.y, i), s.setStyle("y", 0), d = !0) : (s.y = u.y, s.setStyle("y", u.style.y)), f.labelLinePoints) {
          var c = l.getTextGuideLine();
          c && (c.setShape({
            points: f.labelLinePoints
          }), d = !1);
        }
        var v = Li(s);
        v.needsUpdateLabelLine = d, s.rotation = f.rotate != null ? f.rotate * h : u.rotation, s.scaleX = u.scaleX, s.scaleY = u.scaleY;
        for (var p = 0; p < sa.length; p++) {
          var m = sa[p];
          s.setStyle(m, f[m] != null ? f[m] : u.style[m]);
        }
        if (f.draggable) {
          if (s.draggable = !0, s.cursor = "move", l) {
            var _ = o.seriesModel;
            if (o.dataIndex != null) {
              var x = o.seriesModel.getData(o.dataType);
              _ = x.getItemModel(o.dataIndex);
            }
            s.on("drag", n(l, _.getModel("labelLine")));
          }
        } else
          s.off("drag"), s.cursor = u.cursor;
      }
    }, e.prototype.layout = function(t) {
      var r = t.getWidth(), i = t.getHeight(), n = [];
      rt(this._labelList, function(l) {
        l.defaultAttr.ignore || n.push(Ns({}, l));
      });
      var a = Ci(n, function(l) {
        return l.layoutOption.moveOverlap === "shiftX";
      }), o = Ci(n, function(l) {
        return l.layoutOption.moveOverlap === "shiftY";
      });
      Jn(a, 0, 0, r), Jn(o, 1, 0, i);
      var s = Ci(n, function(l) {
        return l.layoutOption.hideOverlap;
      });
      Os(s), Is(s);
    }, e.prototype.processLabelsOverall = function() {
      var t = this;
      rt(this._chartViewList, function(r) {
        var i = r.__model, n = r.ignoreLabelLineUpdate, a = i.isAnimationEnabled();
        r.group.traverse(function(o) {
          if (o.ignore && !o.forceLabelAnimation)
            return !0;
          var s = !n, l = o.getTextContent();
          !s && l && (s = Li(l).needsUpdateLabelLine), s && t._updateLabelLine(o, i), a && t._animateLabels(o, i);
        });
      });
    }, e.prototype._updateLabelLine = function(t, r) {
      var i = t.getTextContent(), n = $r(t), a = n.dataIndex;
      if (i && a != null) {
        var o = r.getData(n.dataType), s = o.getItemModel(a), l = {}, u = o.getItemVisual(a, "style");
        if (u) {
          var f = o.getVisual("drawType");
          l.stroke = u[f];
        }
        var h = s.getModel("labelLine");
        tu(t, ru(s), l), aa(t, h);
      }
    }, e.prototype._animateLabels = function(t, r) {
      var i = t.getTextContent(), n = t.getTextGuideLine();
      if (i && (t.forceLabelAnimation || !i.ignore && !i.invisible && !t.disableLabelAnimation && !Bs(t))) {
        var a = Li(i), o = a.oldLayout, s = $r(t), l = s.dataIndex, u = {
          x: i.x,
          y: i.y,
          rotation: i.rotation
        }, f = r.getData(s.dataType);
        if (o) {
          i.attr(o);
          var d = t.prevStates;
          d && (hn(d, "select") >= 0 && i.attr(a.oldLayoutSelect), hn(d, "emphasis") >= 0 && i.attr(a.oldLayoutEmphasis)), ta(i, u, r, l);
        } else if (i.attr(u), !Fs(i).valueAnimation) {
          var h = fn(i.style.opacity, 1);
          i.style.opacity = 0, ea(i, {
            style: {
              opacity: h
            }
          }, r, l);
        }
        if (a.oldLayout = u, i.states.select) {
          var c = a.oldLayoutSelect = {};
          Dr(c, u, Pr), Dr(c, i.states.select, Pr);
        }
        if (i.states.emphasis) {
          var v = a.oldLayoutEmphasis = {};
          Dr(v, u, Pr), Dr(v, i.states.emphasis, Pr);
        }
        Us(i, l, f, r, r);
      }
      if (n && !n.ignore && !n.invisible) {
        var a = au(n), o = a.oldLayout, p = {
          points: n.shape.points
        };
        o ? (n.attr({
          shape: o
        }), ta(n, {
          shape: p
        }, r)) : (n.setShape(p), n.style.strokePercent = 0, ea(n, {
          style: {
            strokePercent: 1
          }
        }, r)), a.oldLayout = p;
      }
    }, e;
  })()
), Di = Cn();
function su(e) {
  e.registerUpdateLifecycle("series:beforeupdate", function(t, r, i) {
    var n = Di(r).labelManager;
    n || (n = Di(r).labelManager = new ou()), n.clearLabels();
  }), e.registerUpdateLifecycle("series:layoutlabels", function(t, r, i) {
    var n = Di(r).labelManager;
    rt(i.updatedSeries, function(a) {
      n.addLabelsOfSeries(r.getViewOfSeriesModel(a));
    }), n.updateLayoutConfig(r), n.layout(r), n.processLabelsOverall();
  });
}
mi([mo, zs]);
const lu = {
  init: function() {
    return _o.apply(null, arguments);
  }
};
mi(su);
const uu = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Axis: Mn,
  ChartView: _i,
  ComponentModel: Ln,
  ComponentView: go,
  List: yo,
  Model: xo,
  PRIORITY: Gs,
  SeriesModel: gi,
  color: Vs,
  connect: ks,
  dataTool: Ws,
  default: lu,
  dependencies: Xs,
  disConnect: qs,
  disconnect: js,
  dispose: Zs,
  env: Ys,
  extendChartView: Ks,
  extendComponentModel: Qs,
  extendComponentView: $s,
  extendSeriesModel: Js,
  format: el,
  getCoordinateSystemDimensions: To,
  getInstanceByDom: tl,
  getInstanceById: rl,
  getMap: il,
  graphic: nl,
  helper: al,
  init: _o,
  innerDrawElementOnCanvas: Dn,
  matrix: ol,
  number: sl,
  parseGeoJSON: ra,
  parseGeoJson: ra,
  registerAction: ll,
  registerCoordinateSystem: ul,
  registerCustomSeries: fl,
  registerLayout: hl,
  registerLoading: cl,
  registerLocale: dl,
  registerMap: vl,
  registerPostInit: Hs,
  registerPostUpdate: pl,
  registerPreprocessor: ml,
  registerProcessor: _l,
  registerTheme: gl,
  registerTransform: yl,
  registerUpdateLifecycle: xl,
  registerVisual: Tl,
  setCanvasCreator: wl,
  setPlatformAPI: bl,
  throttle: El,
  time: Sl,
  use: mi,
  util: Al,
  vector: Cl,
  version: Ml,
  zrUtil: Ll,
  zrender: Dl
}, Symbol.toStringTag, { value: "Module" }));
function la(e, t, r) {
  typeof t == "object" && (r = t, t = null);
  var i = this, n;
  if (!(e instanceof Function)) {
    n = [];
    for (var a in e)
      e.hasOwnProperty(a) && n.push(a);
  }
  var o = function(l) {
    if (i.apply(this, arguments), e instanceof Function ? ua(this, e.call(this, l)) : fu(this, e, n), this.constructor === o)
      for (var u = o.__initializers__, f = 0; f < u.length; f++)
        u[f].apply(this, arguments);
  };
  o.__super__ = i, i.__initializers__ ? o.__initializers__ = i.__initializers__.slice() : o.__initializers__ = [], t && o.__initializers__.push(t);
  var s = function() {
  };
  return s.prototype = i.prototype, o.prototype = new s(), o.prototype.constructor = o, ua(o.prototype, r), o.extend = i.extend, o.derive = i.extend, o;
}
function ua(e, t) {
  if (t)
    for (var r in t)
      t.hasOwnProperty(r) && (e[r] = t[r]);
}
function fu(e, t, r) {
  for (var i = 0; i < r.length; i++) {
    var n = r[i];
    e[n] = t[n];
  }
}
const hu = {
  extend: la,
  // DEPCRATED
  derive: la
};
function cu(e, t) {
  this.action = e, this.context = t;
}
var Rn = {
  /**
   * Trigger event
   * @param  {string} name
   */
  trigger: function(e) {
    if (this.hasOwnProperty("__handlers__") && this.__handlers__.hasOwnProperty(e)) {
      var t = this.__handlers__[e], r = t.length, i = -1, n = arguments;
      switch (n.length) {
        case 1:
          for (; ++i < r; )
            t[i].action.call(t[i].context);
          return;
        case 2:
          for (; ++i < r; )
            t[i].action.call(t[i].context, n[1]);
          return;
        case 3:
          for (; ++i < r; )
            t[i].action.call(t[i].context, n[1], n[2]);
          return;
        case 4:
          for (; ++i < r; )
            t[i].action.call(t[i].context, n[1], n[2], n[3]);
          return;
        case 5:
          for (; ++i < r; )
            t[i].action.call(t[i].context, n[1], n[2], n[3], n[4]);
          return;
        default:
          for (; ++i < r; )
            t[i].action.apply(t[i].context, Array.prototype.slice.call(n, 1));
          return;
      }
    }
  },
  /**
   * Register event handler
   * @param  {string} name
   * @param  {Function} action
   * @param  {Object} [context]
   * @chainable
   */
  on: function(e, t, r) {
    if (!(!e || !t)) {
      var i = this.__handlers__ || (this.__handlers__ = {});
      if (!i[e])
        i[e] = [];
      else if (this.has(e, t))
        return;
      var n = new cu(t, r || this);
      return i[e].push(n), this;
    }
  },
  /**
   * Register event, event will only be triggered once and then removed
   * @param  {string} name
   * @param  {Function} action
   * @param  {Object} [context]
   * @chainable
   */
  once: function(e, t, r) {
    if (!e || !t)
      return;
    var i = this;
    function n() {
      i.off(e, n), t.apply(this, arguments);
    }
    return this.on(e, n, r);
  },
  /**
   * Alias of once('before' + name)
   * @param  {string} name
   * @param  {Function} action
   * @param  {Object} [context]
   * @chainable
   */
  before: function(e, t, r) {
    if (!(!e || !t))
      return e = "before" + e, this.on(e, t, r);
  },
  /**
   * Alias of once('after' + name)
   * @param  {string} name
   * @param  {Function} action
   * @param  {Object} [context]
   * @chainable
   */
  after: function(e, t, r) {
    if (!(!e || !t))
      return e = "after" + e, this.on(e, t, r);
  },
  /**
   * Alias of on('success')
   * @param  {Function} action
   * @param  {Object} [context]
   * @chainable
   */
  success: function(e, t) {
    return this.once("success", e, t);
  },
  /**
   * Alias of on('error')
   * @param  {Function} action
   * @param  {Object} [context]
   * @chainable
   */
  error: function(e, t) {
    return this.once("error", e, t);
  },
  /**
   * Remove event listener
   * @param  {Function} action
   * @param  {Object} [context]
   * @chainable
   */
  off: function(e, t) {
    var r = this.__handlers__ || (this.__handlers__ = {});
    if (!t) {
      r[e] = [];
      return;
    }
    if (r[e]) {
      for (var i = r[e], n = [], a = 0; a < i.length; a++)
        t && i[a].action !== t && n.push(i[a]);
      r[e] = n;
    }
    return this;
  },
  /**
   * If registered the event handler
   * @param  {string}  name
   * @param  {Function}  action
   * @return {boolean}
   */
  has: function(e, t) {
    var r = this.__handlers__;
    if (!r || !r[e])
      return !1;
    for (var i = r[e], n = 0; n < i.length; n++)
      if (i[n].action === t)
        return !0;
  }
}, du = 0, vu = Array.prototype, pu = vu.forEach, he = {
  /**
   * Generate GUID
   * @return {number}
   * @memberOf clay.core.util
   */
  genGUID: function() {
    return ++du;
  },
  /**
   * Relative path to absolute path
   * @param  {string} path
   * @param  {string} basePath
   * @return {string}
   * @memberOf clay.core.util
   */
  relative2absolute: function(e, t) {
    if (!t || e.match(/^\//))
      return e;
    for (var r = e.split("/"), i = t.split("/"), n = r[0]; n === "." || n === ".."; )
      n === ".." && i.pop(), r.shift(), n = r[0];
    return i.join("/") + "/" + r.join("/");
  },
  /**
   * Extend target with source
   * @param  {Object} target
   * @param  {Object} source
   * @return {Object}
   * @memberOf clay.core.util
   */
  extend: function(e, t) {
    if (t)
      for (var r in t)
        t.hasOwnProperty(r) && (e[r] = t[r]);
    return e;
  },
  /**
   * Extend properties to target if not exist.
   * @param  {Object} target
   * @param  {Object} source
   * @return {Object}
   * @memberOf clay.core.util
   */
  defaults: function(e, t) {
    if (t)
      for (var r in t)
        e[r] === void 0 && (e[r] = t[r]);
    return e;
  },
  /**
   * Extend properties with a given property list to avoid for..in.. iteration.
   * @param  {Object} target
   * @param  {Object} source
   * @param  {Array.<string>} propList
   * @return {Object}
   * @memberOf clay.core.util
   */
  extendWithPropList: function(e, t, r) {
    if (t)
      for (var i = 0; i < r.length; i++) {
        var n = r[i];
        e[n] = t[n];
      }
    return e;
  },
  /**
   * Extend properties to target if not exist. With a given property list avoid for..in.. iteration.
   * @param  {Object} target
   * @param  {Object} source
   * @param  {Array.<string>} propList
   * @return {Object}
   * @memberOf clay.core.util
   */
  defaultsWithPropList: function(e, t, r) {
    if (t)
      for (var i = 0; i < r.length; i++) {
        var n = r[i];
        e[n] == null && (e[n] = t[n]);
      }
    return e;
  },
  /**
   * @param  {Object|Array} obj
   * @param  {Function} iterator
   * @param  {Object} [context]
   * @memberOf clay.core.util
   */
  each: function(e, t, r) {
    if (e && t)
      if (e.forEach && e.forEach === pu)
        e.forEach(t, r);
      else if (e.length === +e.length)
        for (var i = 0, n = e.length; i < n; i++)
          t.call(r, e[i], i, e);
      else
        for (var a in e)
          e.hasOwnProperty(a) && t.call(r, e[a], a, e);
  },
  /**
   * Is object
   * @param  {}  obj
   * @return {boolean}
   * @memberOf clay.core.util
   */
  isObject: function(e) {
    return e === Object(e);
  },
  /**
   * Is array ?
   * @param  {}  obj
   * @return {boolean}
   * @memberOf clay.core.util
   */
  isArray: function(e) {
    return Array.isArray(e);
  },
  /**
   * Is array like, which have a length property
   * @param  {}  obj
   * @return {boolean}
   * @memberOf clay.core.util
   */
  isArrayLike: function(e) {
    return e ? e.length === +e.length : !1;
  },
  /**
   * @param  {} obj
   * @return {}
   * @memberOf clay.core.util
   */
  clone: function(e) {
    if (he.isObject(e)) {
      if (he.isArray(e))
        return e.slice();
      if (he.isArrayLike(e)) {
        for (var t = new e.constructor(e.length), r = 0; r < e.length; r++)
          t[r] = e[r];
        return t;
      } else
        return he.extend({}, e);
    } else return e;
  }
}, Ae = function() {
  this.__uid__ = he.genGUID();
};
Ae.__initializers__ = [
  function(e) {
    he.extend(this, e);
  }
];
he.extend(Ae, hu);
he.extend(Ae.prototype, Rn);
var Jr = 1e-6, Ee = Array, Xt = Math.random, T = {};
T.create = function() {
  var e = new Ee(3);
  return e[0] = 0, e[1] = 0, e[2] = 0, e;
};
T.clone = function(e) {
  var t = new Ee(3);
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t;
};
T.fromValues = function(e, t, r) {
  var i = new Ee(3);
  return i[0] = e, i[1] = t, i[2] = r, i;
};
T.copy = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e;
};
T.set = function(e, t, r, i) {
  return e[0] = t, e[1] = r, e[2] = i, e;
};
T.add = function(e, t, r) {
  return e[0] = t[0] + r[0], e[1] = t[1] + r[1], e[2] = t[2] + r[2], e;
};
T.subtract = function(e, t, r) {
  return e[0] = t[0] - r[0], e[1] = t[1] - r[1], e[2] = t[2] - r[2], e;
};
T.sub = T.subtract;
T.multiply = function(e, t, r) {
  return e[0] = t[0] * r[0], e[1] = t[1] * r[1], e[2] = t[2] * r[2], e;
};
T.mul = T.multiply;
T.divide = function(e, t, r) {
  return e[0] = t[0] / r[0], e[1] = t[1] / r[1], e[2] = t[2] / r[2], e;
};
T.div = T.divide;
T.min = function(e, t, r) {
  return e[0] = Math.min(t[0], r[0]), e[1] = Math.min(t[1], r[1]), e[2] = Math.min(t[2], r[2]), e;
};
T.max = function(e, t, r) {
  return e[0] = Math.max(t[0], r[0]), e[1] = Math.max(t[1], r[1]), e[2] = Math.max(t[2], r[2]), e;
};
T.scale = function(e, t, r) {
  return e[0] = t[0] * r, e[1] = t[1] * r, e[2] = t[2] * r, e;
};
T.scaleAndAdd = function(e, t, r, i) {
  return e[0] = t[0] + r[0] * i, e[1] = t[1] + r[1] * i, e[2] = t[2] + r[2] * i, e;
};
T.distance = function(e, t) {
  var r = t[0] - e[0], i = t[1] - e[1], n = t[2] - e[2];
  return Math.sqrt(r * r + i * i + n * n);
};
T.dist = T.distance;
T.squaredDistance = function(e, t) {
  var r = t[0] - e[0], i = t[1] - e[1], n = t[2] - e[2];
  return r * r + i * i + n * n;
};
T.sqrDist = T.squaredDistance;
T.length = function(e) {
  var t = e[0], r = e[1], i = e[2];
  return Math.sqrt(t * t + r * r + i * i);
};
T.len = T.length;
T.squaredLength = function(e) {
  var t = e[0], r = e[1], i = e[2];
  return t * t + r * r + i * i;
};
T.sqrLen = T.squaredLength;
T.negate = function(e, t) {
  return e[0] = -t[0], e[1] = -t[1], e[2] = -t[2], e;
};
T.inverse = function(e, t) {
  return e[0] = 1 / t[0], e[1] = 1 / t[1], e[2] = 1 / t[2], e;
};
T.normalize = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = r * r + i * i + n * n;
  return a > 0 && (a = 1 / Math.sqrt(a), e[0] = t[0] * a, e[1] = t[1] * a, e[2] = t[2] * a), e;
};
T.dot = function(e, t) {
  return e[0] * t[0] + e[1] * t[1] + e[2] * t[2];
};
T.cross = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = r[0], s = r[1], l = r[2];
  return e[0] = n * l - a * s, e[1] = a * o - i * l, e[2] = i * s - n * o, e;
};
T.lerp = function(e, t, r, i) {
  var n = t[0], a = t[1], o = t[2];
  return e[0] = n + i * (r[0] - n), e[1] = a + i * (r[1] - a), e[2] = o + i * (r[2] - o), e;
};
T.random = function(e, t) {
  t = t || 1;
  var r = Xt() * 2 * Math.PI, i = Xt() * 2 - 1, n = Math.sqrt(1 - i * i) * t;
  return e[0] = Math.cos(r) * n, e[1] = Math.sin(r) * n, e[2] = i * t, e;
};
T.transformMat4 = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = r[3] * i + r[7] * n + r[11] * a + r[15];
  return o = o || 1, e[0] = (r[0] * i + r[4] * n + r[8] * a + r[12]) / o, e[1] = (r[1] * i + r[5] * n + r[9] * a + r[13]) / o, e[2] = (r[2] * i + r[6] * n + r[10] * a + r[14]) / o, e;
};
T.transformMat3 = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2];
  return e[0] = i * r[0] + n * r[3] + a * r[6], e[1] = i * r[1] + n * r[4] + a * r[7], e[2] = i * r[2] + n * r[5] + a * r[8], e;
};
T.transformQuat = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = r[0], s = r[1], l = r[2], u = r[3], f = u * i + s * a - l * n, h = u * n + l * i - o * a, d = u * a + o * n - s * i, c = -o * i - s * n - l * a;
  return e[0] = f * u + c * -o + h * -l - d * -s, e[1] = h * u + c * -s + d * -o - f * -l, e[2] = d * u + c * -l + f * -s - h * -o, e;
};
T.rotateX = function(e, t, r, i) {
  var n = [], a = [];
  return n[0] = t[0] - r[0], n[1] = t[1] - r[1], n[2] = t[2] - r[2], a[0] = n[0], a[1] = n[1] * Math.cos(i) - n[2] * Math.sin(i), a[2] = n[1] * Math.sin(i) + n[2] * Math.cos(i), e[0] = a[0] + r[0], e[1] = a[1] + r[1], e[2] = a[2] + r[2], e;
};
T.rotateY = function(e, t, r, i) {
  var n = [], a = [];
  return n[0] = t[0] - r[0], n[1] = t[1] - r[1], n[2] = t[2] - r[2], a[0] = n[2] * Math.sin(i) + n[0] * Math.cos(i), a[1] = n[1], a[2] = n[2] * Math.cos(i) - n[0] * Math.sin(i), e[0] = a[0] + r[0], e[1] = a[1] + r[1], e[2] = a[2] + r[2], e;
};
T.rotateZ = function(e, t, r, i) {
  var n = [], a = [];
  return n[0] = t[0] - r[0], n[1] = t[1] - r[1], n[2] = t[2] - r[2], a[0] = n[0] * Math.cos(i) - n[1] * Math.sin(i), a[1] = n[0] * Math.sin(i) + n[1] * Math.cos(i), a[2] = n[2], e[0] = a[0] + r[0], e[1] = a[1] + r[1], e[2] = a[2] + r[2], e;
};
T.forEach = (function() {
  var e = T.create();
  return function(t, r, i, n, a, o) {
    var s, l;
    for (r || (r = 3), i || (i = 0), n ? l = Math.min(n * r + i, t.length) : l = t.length, s = i; s < l; s += r)
      e[0] = t[s], e[1] = t[s + 1], e[2] = t[s + 2], a(e, e, o), t[s] = e[0], t[s + 1] = e[1], t[s + 2] = e[2];
    return t;
  };
})();
T.angle = function(e, t) {
  var r = T.fromValues(e[0], e[1], e[2]), i = T.fromValues(t[0], t[1], t[2]);
  T.normalize(r, r), T.normalize(i, i);
  var n = T.dot(r, i);
  return n > 1 ? 0 : Math.acos(n);
};
var S = function(e, t, r) {
  e = e || 0, t = t || 0, r = r || 0, this.array = T.fromValues(e, t, r), this._dirty = !0;
};
S.prototype = {
  constructor: S,
  /**
   * Add b to self
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  add: function(e) {
    return T.add(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Set x, y and z components
   * @param  {number}  x
   * @param  {number}  y
   * @param  {number}  z
   * @return {clay.Vector3}
   */
  set: function(e, t, r) {
    return this.array[0] = e, this.array[1] = t, this.array[2] = r, this._dirty = !0, this;
  },
  /**
   * Set x, y and z components from array
   * @param  {Float32Array|number[]} arr
   * @return {clay.Vector3}
   */
  setArray: function(e) {
    return this.array[0] = e[0], this.array[1] = e[1], this.array[2] = e[2], this._dirty = !0, this;
  },
  /**
   * Clone a new Vector3
   * @return {clay.Vector3}
   */
  clone: function() {
    return new S(this.x, this.y, this.z);
  },
  /**
   * Copy from b
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  copy: function(e) {
    return T.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Cross product of self and b, written to a Vector3 out
   * @param  {clay.Vector3} a
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  cross: function(e, t) {
    return T.cross(this.array, e.array, t.array), this._dirty = !0, this;
  },
  /**
   * Alias for distance
   * @param  {clay.Vector3} b
   * @return {number}
   */
  dist: function(e) {
    return T.dist(this.array, e.array);
  },
  /**
   * Distance between self and b
   * @param  {clay.Vector3} b
   * @return {number}
   */
  distance: function(e) {
    return T.distance(this.array, e.array);
  },
  /**
   * Alias for divide
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  div: function(e) {
    return T.div(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Divide self by b
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  divide: function(e) {
    return T.divide(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Dot product of self and b
   * @param  {clay.Vector3} b
   * @return {number}
   */
  dot: function(e) {
    return T.dot(this.array, e.array);
  },
  /**
   * Alias of length
   * @return {number}
   */
  len: function() {
    return T.len(this.array);
  },
  /**
   * Calculate the length
   * @return {number}
   */
  length: function() {
    return T.length(this.array);
  },
  /**
   * Linear interpolation between a and b
   * @param  {clay.Vector3} a
   * @param  {clay.Vector3} b
   * @param  {number}  t
   * @return {clay.Vector3}
   */
  lerp: function(e, t, r) {
    return T.lerp(this.array, e.array, t.array, r), this._dirty = !0, this;
  },
  /**
   * Minimum of self and b
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  min: function(e) {
    return T.min(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Maximum of self and b
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  max: function(e) {
    return T.max(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiply
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  mul: function(e) {
    return T.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Mutiply self and b
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  multiply: function(e) {
    return T.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Negate self
   * @return {clay.Vector3}
   */
  negate: function() {
    return T.negate(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Normalize self
   * @return {clay.Vector3}
   */
  normalize: function() {
    return T.normalize(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Generate random x, y, z components with a given scale
   * @param  {number} scale
   * @return {clay.Vector3}
   */
  random: function(e) {
    return T.random(this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale self
   * @param  {number}  scale
   * @return {clay.Vector3}
   */
  scale: function(e) {
    return T.scale(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale b and add to self
   * @param  {clay.Vector3} b
   * @param  {number}  scale
   * @return {clay.Vector3}
   */
  scaleAndAdd: function(e, t) {
    return T.scaleAndAdd(this.array, this.array, e.array, t), this._dirty = !0, this;
  },
  /**
   * Alias for squaredDistance
   * @param  {clay.Vector3} b
   * @return {number}
   */
  sqrDist: function(e) {
    return T.sqrDist(this.array, e.array);
  },
  /**
   * Squared distance between self and b
   * @param  {clay.Vector3} b
   * @return {number}
   */
  squaredDistance: function(e) {
    return T.squaredDistance(this.array, e.array);
  },
  /**
   * Alias for squaredLength
   * @return {number}
   */
  sqrLen: function() {
    return T.sqrLen(this.array);
  },
  /**
   * Squared length of self
   * @return {number}
   */
  squaredLength: function() {
    return T.squaredLength(this.array);
  },
  /**
   * Alias for subtract
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  sub: function(e) {
    return T.sub(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Subtract b from self
   * @param  {clay.Vector3} b
   * @return {clay.Vector3}
   */
  subtract: function(e) {
    return T.subtract(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Matrix3 m
   * @param  {clay.Matrix3} m
   * @return {clay.Vector3}
   */
  transformMat3: function(e) {
    return T.transformMat3(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Matrix4 m
   * @param  {clay.Matrix4} m
   * @return {clay.Vector3}
   */
  transformMat4: function(e) {
    return T.transformMat4(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Quaternion q
   * @param  {clay.Quaternion} q
   * @return {clay.Vector3}
   */
  transformQuat: function(e) {
    return T.transformQuat(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Trasnform self into projection space with m
   * @param  {clay.Matrix4} m
   * @return {clay.Vector3}
   */
  applyProjection: function(e) {
    var t = this.array;
    if (e = e.array, e[15] === 0) {
      var r = -1 / t[2];
      t[0] = e[0] * t[0] * r, t[1] = e[5] * t[1] * r, t[2] = (e[10] * t[2] + e[14]) * r;
    } else
      t[0] = e[0] * t[0] + e[12], t[1] = e[5] * t[1] + e[13], t[2] = e[10] * t[2] + e[14];
    return this._dirty = !0, this;
  },
  eulerFromQuat: function(e, t) {
    S.eulerFromQuat(this, e, t);
  },
  eulerFromMat3: function(e, t) {
    S.eulerFromMat3(this, e, t);
  },
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
var Rr = Object.defineProperty;
if (Rr) {
  var Pi = S.prototype;
  Rr(Pi, "x", {
    get: function() {
      return this.array[0];
    },
    set: function(e) {
      this.array[0] = e, this._dirty = !0;
    }
  }), Rr(Pi, "y", {
    get: function() {
      return this.array[1];
    },
    set: function(e) {
      this.array[1] = e, this._dirty = !0;
    }
  }), Rr(Pi, "z", {
    get: function() {
      return this.array[2];
    },
    set: function(e) {
      this.array[2] = e, this._dirty = !0;
    }
  });
}
S.add = function(e, t, r) {
  return T.add(e.array, t.array, r.array), e._dirty = !0, e;
};
S.set = function(e, t, r, i) {
  T.set(e.array, t, r, i), e._dirty = !0;
};
S.copy = function(e, t) {
  return T.copy(e.array, t.array), e._dirty = !0, e;
};
S.cross = function(e, t, r) {
  return T.cross(e.array, t.array, r.array), e._dirty = !0, e;
};
S.dist = function(e, t) {
  return T.distance(e.array, t.array);
};
S.distance = S.dist;
S.div = function(e, t, r) {
  return T.divide(e.array, t.array, r.array), e._dirty = !0, e;
};
S.divide = S.div;
S.dot = function(e, t) {
  return T.dot(e.array, t.array);
};
S.len = function(e) {
  return T.length(e.array);
};
S.lerp = function(e, t, r, i) {
  return T.lerp(e.array, t.array, r.array, i), e._dirty = !0, e;
};
S.min = function(e, t, r) {
  return T.min(e.array, t.array, r.array), e._dirty = !0, e;
};
S.max = function(e, t, r) {
  return T.max(e.array, t.array, r.array), e._dirty = !0, e;
};
S.mul = function(e, t, r) {
  return T.multiply(e.array, t.array, r.array), e._dirty = !0, e;
};
S.multiply = S.mul;
S.negate = function(e, t) {
  return T.negate(e.array, t.array), e._dirty = !0, e;
};
S.normalize = function(e, t) {
  return T.normalize(e.array, t.array), e._dirty = !0, e;
};
S.random = function(e, t) {
  return T.random(e.array, t), e._dirty = !0, e;
};
S.scale = function(e, t, r) {
  return T.scale(e.array, t.array, r), e._dirty = !0, e;
};
S.scaleAndAdd = function(e, t, r, i) {
  return T.scaleAndAdd(e.array, t.array, r.array, i), e._dirty = !0, e;
};
S.sqrDist = function(e, t) {
  return T.sqrDist(e.array, t.array);
};
S.squaredDistance = S.sqrDist;
S.sqrLen = function(e) {
  return T.sqrLen(e.array);
};
S.squaredLength = S.sqrLen;
S.sub = function(e, t, r) {
  return T.subtract(e.array, t.array, r.array), e._dirty = !0, e;
};
S.subtract = S.sub;
S.transformMat3 = function(e, t, r) {
  return T.transformMat3(e.array, t.array, r.array), e._dirty = !0, e;
};
S.transformMat4 = function(e, t, r) {
  return T.transformMat4(e.array, t.array, r.array), e._dirty = !0, e;
};
S.transformQuat = function(e, t, r) {
  return T.transformQuat(e.array, t.array, r.array), e._dirty = !0, e;
};
function He(e, t, r) {
  return e < t ? t : e > r ? r : e;
}
var te = Math.atan2, Ge = Math.asin, It = Math.abs;
S.eulerFromQuat = function(e, t, d) {
  e._dirty = !0, t = t.array;
  var i = e.array, n = t[0], a = t[1], o = t[2], s = t[3], l = n * n, u = a * a, f = o * o, h = s * s, d = (d || "XYZ").toUpperCase();
  switch (d) {
    case "XYZ":
      i[0] = te(2 * (n * s - a * o), h - l - u + f), i[1] = Ge(He(2 * (n * o + a * s), -1, 1)), i[2] = te(2 * (o * s - n * a), h + l - u - f);
      break;
    case "YXZ":
      i[0] = Ge(He(2 * (n * s - a * o), -1, 1)), i[1] = te(2 * (n * o + a * s), h - l - u + f), i[2] = te(2 * (n * a + o * s), h - l + u - f);
      break;
    case "ZXY":
      i[0] = Ge(He(2 * (n * s + a * o), -1, 1)), i[1] = te(2 * (a * s - o * n), h - l - u + f), i[2] = te(2 * (o * s - n * a), h - l + u - f);
      break;
    case "ZYX":
      i[0] = te(2 * (n * s + o * a), h - l - u + f), i[1] = Ge(He(2 * (a * s - n * o), -1, 1)), i[2] = te(2 * (n * a + o * s), h + l - u - f);
      break;
    case "YZX":
      i[0] = te(2 * (n * s - o * a), h - l + u - f), i[1] = te(2 * (a * s - n * o), h + l - u - f), i[2] = Ge(He(2 * (n * a + o * s), -1, 1));
      break;
    case "XZY":
      i[0] = te(2 * (n * s + a * o), h - l + u - f), i[1] = te(2 * (n * o + a * s), h + l - u - f), i[2] = Ge(He(2 * (o * s - n * a), -1, 1));
      break;
    default:
      console.warn("Unkown order: " + d);
  }
  return e;
};
S.eulerFromMat3 = function(e, t, v) {
  var i = t.array, n = i[0], a = i[3], o = i[6], s = i[1], l = i[4], u = i[7], f = i[2], h = i[5], d = i[8], c = e.array, v = (v || "XYZ").toUpperCase();
  switch (v) {
    case "XYZ":
      c[1] = Ge(He(o, -1, 1)), It(o) < 0.99999 ? (c[0] = te(-u, d), c[2] = te(-a, n)) : (c[0] = te(h, l), c[2] = 0);
      break;
    case "YXZ":
      c[0] = Ge(-He(u, -1, 1)), It(u) < 0.99999 ? (c[1] = te(o, d), c[2] = te(s, l)) : (c[1] = te(-f, n), c[2] = 0);
      break;
    case "ZXY":
      c[0] = Ge(He(h, -1, 1)), It(h) < 0.99999 ? (c[1] = te(-f, d), c[2] = te(-a, l)) : (c[1] = 0, c[2] = te(s, n));
      break;
    case "ZYX":
      c[1] = Ge(-He(f, -1, 1)), It(f) < 0.99999 ? (c[0] = te(h, d), c[2] = te(s, n)) : (c[0] = 0, c[2] = te(-a, l));
      break;
    case "YZX":
      c[2] = Ge(He(s, -1, 1)), It(s) < 0.99999 ? (c[0] = te(-u, l), c[1] = te(-f, n)) : (c[0] = 0, c[1] = te(o, d));
      break;
    case "XZY":
      c[2] = Ge(-He(a, -1, 1)), It(a) < 0.99999 ? (c[0] = te(h, l), c[1] = te(o, n)) : (c[0] = te(-u, d), c[1] = 0);
      break;
    default:
      console.warn("Unkown order: " + v);
  }
  return e._dirty = !0, e;
};
Object.defineProperties(S, {
  /**
   * @type {clay.Vector3}
   * @readOnly
   * @memberOf clay.Vector3
   */
  POSITIVE_X: {
    get: function() {
      return new S(1, 0, 0);
    }
  },
  /**
   * @type {clay.Vector3}
   * @readOnly
   * @memberOf clay.Vector3
   */
  NEGATIVE_X: {
    get: function() {
      return new S(-1, 0, 0);
    }
  },
  /**
   * @type {clay.Vector3}
   * @readOnly
   * @memberOf clay.Vector3
   */
  POSITIVE_Y: {
    get: function() {
      return new S(0, 1, 0);
    }
  },
  /**
   * @type {clay.Vector3}
   * @readOnly
   * @memberOf clay.Vector3
   */
  NEGATIVE_Y: {
    get: function() {
      return new S(0, -1, 0);
    }
  },
  /**
   * @type {clay.Vector3}
   * @readOnly
   * @memberOf clay.Vector3
   */
  POSITIVE_Z: {
    get: function() {
      return new S(0, 0, 1);
    }
  },
  /**
   * @type {clay.Vector3}
   * @readOnly
   */
  NEGATIVE_Z: {
    get: function() {
      return new S(0, 0, -1);
    }
  },
  /**
   * @type {clay.Vector3}
   * @readOnly
   * @memberOf clay.Vector3
   */
  UP: {
    get: function() {
      return new S(0, 1, 0);
    }
  },
  /**
   * @type {clay.Vector3}
   * @readOnly
   * @memberOf clay.Vector3
   */
  ZERO: {
    get: function() {
      return new S();
    }
  }
});
var L = {};
L.create = function() {
  var e = new Ee(4);
  return e[0] = 0, e[1] = 0, e[2] = 0, e[3] = 0, e;
};
L.clone = function(e) {
  var t = new Ee(4);
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t;
};
L.fromValues = function(e, t, r, i) {
  var n = new Ee(4);
  return n[0] = e, n[1] = t, n[2] = r, n[3] = i, n;
};
L.copy = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e;
};
L.set = function(e, t, r, i, n) {
  return e[0] = t, e[1] = r, e[2] = i, e[3] = n, e;
};
L.add = function(e, t, r) {
  return e[0] = t[0] + r[0], e[1] = t[1] + r[1], e[2] = t[2] + r[2], e[3] = t[3] + r[3], e;
};
L.subtract = function(e, t, r) {
  return e[0] = t[0] - r[0], e[1] = t[1] - r[1], e[2] = t[2] - r[2], e[3] = t[3] - r[3], e;
};
L.sub = L.subtract;
L.multiply = function(e, t, r) {
  return e[0] = t[0] * r[0], e[1] = t[1] * r[1], e[2] = t[2] * r[2], e[3] = t[3] * r[3], e;
};
L.mul = L.multiply;
L.divide = function(e, t, r) {
  return e[0] = t[0] / r[0], e[1] = t[1] / r[1], e[2] = t[2] / r[2], e[3] = t[3] / r[3], e;
};
L.div = L.divide;
L.min = function(e, t, r) {
  return e[0] = Math.min(t[0], r[0]), e[1] = Math.min(t[1], r[1]), e[2] = Math.min(t[2], r[2]), e[3] = Math.min(t[3], r[3]), e;
};
L.max = function(e, t, r) {
  return e[0] = Math.max(t[0], r[0]), e[1] = Math.max(t[1], r[1]), e[2] = Math.max(t[2], r[2]), e[3] = Math.max(t[3], r[3]), e;
};
L.scale = function(e, t, r) {
  return e[0] = t[0] * r, e[1] = t[1] * r, e[2] = t[2] * r, e[3] = t[3] * r, e;
};
L.scaleAndAdd = function(e, t, r, i) {
  return e[0] = t[0] + r[0] * i, e[1] = t[1] + r[1] * i, e[2] = t[2] + r[2] * i, e[3] = t[3] + r[3] * i, e;
};
L.distance = function(e, t) {
  var r = t[0] - e[0], i = t[1] - e[1], n = t[2] - e[2], a = t[3] - e[3];
  return Math.sqrt(r * r + i * i + n * n + a * a);
};
L.dist = L.distance;
L.squaredDistance = function(e, t) {
  var r = t[0] - e[0], i = t[1] - e[1], n = t[2] - e[2], a = t[3] - e[3];
  return r * r + i * i + n * n + a * a;
};
L.sqrDist = L.squaredDistance;
L.length = function(e) {
  var t = e[0], r = e[1], i = e[2], n = e[3];
  return Math.sqrt(t * t + r * r + i * i + n * n);
};
L.len = L.length;
L.squaredLength = function(e) {
  var t = e[0], r = e[1], i = e[2], n = e[3];
  return t * t + r * r + i * i + n * n;
};
L.sqrLen = L.squaredLength;
L.negate = function(e, t) {
  return e[0] = -t[0], e[1] = -t[1], e[2] = -t[2], e[3] = -t[3], e;
};
L.inverse = function(e, t) {
  return e[0] = 1 / t[0], e[1] = 1 / t[1], e[2] = 1 / t[2], e[3] = 1 / t[3], e;
};
L.normalize = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = r * r + i * i + n * n + a * a;
  return o > 0 && (o = 1 / Math.sqrt(o), e[0] = t[0] * o, e[1] = t[1] * o, e[2] = t[2] * o, e[3] = t[3] * o), e;
};
L.dot = function(e, t) {
  return e[0] * t[0] + e[1] * t[1] + e[2] * t[2] + e[3] * t[3];
};
L.lerp = function(e, t, r, i) {
  var n = t[0], a = t[1], o = t[2], s = t[3];
  return e[0] = n + i * (r[0] - n), e[1] = a + i * (r[1] - a), e[2] = o + i * (r[2] - o), e[3] = s + i * (r[3] - s), e;
};
L.random = function(e, t) {
  return t = t || 1, e[0] = Xt(), e[1] = Xt(), e[2] = Xt(), e[3] = Xt(), L.normalize(e, e), L.scale(e, e, t), e;
};
L.transformMat4 = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3];
  return e[0] = r[0] * i + r[4] * n + r[8] * a + r[12] * o, e[1] = r[1] * i + r[5] * n + r[9] * a + r[13] * o, e[2] = r[2] * i + r[6] * n + r[10] * a + r[14] * o, e[3] = r[3] * i + r[7] * n + r[11] * a + r[15] * o, e;
};
L.transformQuat = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = r[0], s = r[1], l = r[2], u = r[3], f = u * i + s * a - l * n, h = u * n + l * i - o * a, d = u * a + o * n - s * i, c = -o * i - s * n - l * a;
  return e[0] = f * u + c * -o + h * -l - d * -s, e[1] = h * u + c * -s + d * -o - f * -l, e[2] = d * u + c * -l + f * -s - h * -o, e;
};
L.forEach = (function() {
  var e = L.create();
  return function(t, r, i, n, a, o) {
    var s, l;
    for (r || (r = 4), i || (i = 0), n ? l = Math.min(n * r + i, t.length) : l = t.length, s = i; s < l; s += r)
      e[0] = t[s], e[1] = t[s + 1], e[2] = t[s + 2], e[3] = t[s + 3], a(e, e, o), t[s] = e[0], t[s + 1] = e[1], t[s + 2] = e[2], t[s + 3] = e[3];
    return t;
  };
})();
var V = {};
V.create = function() {
  var e = new Ee(9);
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 0, e[4] = 1, e[5] = 0, e[6] = 0, e[7] = 0, e[8] = 1, e;
};
V.fromMat4 = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[4], e[4] = t[5], e[5] = t[6], e[6] = t[8], e[7] = t[9], e[8] = t[10], e;
};
V.clone = function(e) {
  var t = new Ee(9);
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t[4] = e[4], t[5] = e[5], t[6] = e[6], t[7] = e[7], t[8] = e[8], t;
};
V.copy = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[4] = t[4], e[5] = t[5], e[6] = t[6], e[7] = t[7], e[8] = t[8], e;
};
V.identity = function(e) {
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 0, e[4] = 1, e[5] = 0, e[6] = 0, e[7] = 0, e[8] = 1, e;
};
V.transpose = function(e, t) {
  if (e === t) {
    var r = t[1], i = t[2], n = t[5];
    e[1] = t[3], e[2] = t[6], e[3] = r, e[5] = t[7], e[6] = i, e[7] = n;
  } else
    e[0] = t[0], e[1] = t[3], e[2] = t[6], e[3] = t[1], e[4] = t[4], e[5] = t[7], e[6] = t[2], e[7] = t[5], e[8] = t[8];
  return e;
};
V.invert = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = t[4], s = t[5], l = t[6], u = t[7], f = t[8], h = f * o - s * u, d = -f * a + s * l, c = u * a - o * l, v = r * h + i * d + n * c;
  return v ? (v = 1 / v, e[0] = h * v, e[1] = (-f * i + n * u) * v, e[2] = (s * i - n * o) * v, e[3] = d * v, e[4] = (f * r - n * l) * v, e[5] = (-s * r + n * a) * v, e[6] = c * v, e[7] = (-u * r + i * l) * v, e[8] = (o * r - i * a) * v, e) : null;
};
V.adjoint = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = t[4], s = t[5], l = t[6], u = t[7], f = t[8];
  return e[0] = o * f - s * u, e[1] = n * u - i * f, e[2] = i * s - n * o, e[3] = s * l - a * f, e[4] = r * f - n * l, e[5] = n * a - r * s, e[6] = a * u - o * l, e[7] = i * l - r * u, e[8] = r * o - i * a, e;
};
V.determinant = function(e) {
  var t = e[0], r = e[1], i = e[2], n = e[3], a = e[4], o = e[5], s = e[6], l = e[7], u = e[8];
  return t * (u * a - o * l) + r * (-u * n + o * s) + i * (l * n - a * s);
};
V.multiply = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = t[6], f = t[7], h = t[8], d = r[0], c = r[1], v = r[2], p = r[3], m = r[4], _ = r[5], x = r[6], y = r[7], g = r[8];
  return e[0] = d * i + c * o + v * u, e[1] = d * n + c * s + v * f, e[2] = d * a + c * l + v * h, e[3] = p * i + m * o + _ * u, e[4] = p * n + m * s + _ * f, e[5] = p * a + m * l + _ * h, e[6] = x * i + y * o + g * u, e[7] = x * n + y * s + g * f, e[8] = x * a + y * l + g * h, e;
};
V.mul = V.multiply;
V.translate = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = t[6], f = t[7], h = t[8], d = r[0], c = r[1];
  return e[0] = i, e[1] = n, e[2] = a, e[3] = o, e[4] = s, e[5] = l, e[6] = d * i + c * o + u, e[7] = d * n + c * s + f, e[8] = d * a + c * l + h, e;
};
V.rotate = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = t[6], f = t[7], h = t[8], d = Math.sin(r), c = Math.cos(r);
  return e[0] = c * i + d * o, e[1] = c * n + d * s, e[2] = c * a + d * l, e[3] = c * o - d * i, e[4] = c * s - d * n, e[5] = c * l - d * a, e[6] = u, e[7] = f, e[8] = h, e;
};
V.scale = function(e, t, r) {
  var i = r[0], n = r[1];
  return e[0] = i * t[0], e[1] = i * t[1], e[2] = i * t[2], e[3] = n * t[3], e[4] = n * t[4], e[5] = n * t[5], e[6] = t[6], e[7] = t[7], e[8] = t[8], e;
};
V.fromMat2d = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = 0, e[3] = t[2], e[4] = t[3], e[5] = 0, e[6] = t[4], e[7] = t[5], e[8] = 1, e;
};
V.fromQuat = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = r + r, s = i + i, l = n + n, u = r * o, f = i * o, h = i * s, d = n * o, c = n * s, v = n * l, p = a * o, m = a * s, _ = a * l;
  return e[0] = 1 - h - v, e[3] = f - _, e[6] = d + m, e[1] = f + _, e[4] = 1 - u - v, e[7] = c - p, e[2] = d - m, e[5] = c + p, e[8] = 1 - u - h, e;
};
V.normalFromMat4 = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = t[4], s = t[5], l = t[6], u = t[7], f = t[8], h = t[9], d = t[10], c = t[11], v = t[12], p = t[13], m = t[14], _ = t[15], x = r * s - i * o, y = r * l - n * o, g = r * u - a * o, b = i * l - n * s, w = i * u - a * s, C = n * u - a * l, A = f * p - h * v, R = f * m - d * v, H = f * _ - c * v, O = h * m - d * p, z = h * _ - c * p, G = d * _ - c * m, F = x * G - y * z + g * O + b * H - w * R + C * A;
  return F ? (F = 1 / F, e[0] = (s * G - l * z + u * O) * F, e[1] = (l * H - o * G - u * R) * F, e[2] = (o * z - s * H + u * A) * F, e[3] = (n * z - i * G - a * O) * F, e[4] = (r * G - n * H + a * R) * F, e[5] = (i * H - r * z - a * A) * F, e[6] = (p * C - m * w + _ * b) * F, e[7] = (m * g - v * C - _ * y) * F, e[8] = (v * w - p * g + _ * x) * F, e) : null;
};
V.frob = function(e) {
  return Math.sqrt(Math.pow(e[0], 2) + Math.pow(e[1], 2) + Math.pow(e[2], 2) + Math.pow(e[3], 2) + Math.pow(e[4], 2) + Math.pow(e[5], 2) + Math.pow(e[6], 2) + Math.pow(e[7], 2) + Math.pow(e[8], 2));
};
var B = {};
B.create = function() {
  var e = new Ee(4);
  return e[0] = 0, e[1] = 0, e[2] = 0, e[3] = 1, e;
};
B.rotationTo = (function() {
  var e = T.create(), t = T.fromValues(1, 0, 0), r = T.fromValues(0, 1, 0);
  return function(i, n, a) {
    var o = T.dot(n, a);
    return o < -0.999999 ? (T.cross(e, t, n), T.length(e) < 1e-6 && T.cross(e, r, n), T.normalize(e, e), B.setAxisAngle(i, e, Math.PI), i) : o > 0.999999 ? (i[0] = 0, i[1] = 0, i[2] = 0, i[3] = 1, i) : (T.cross(e, n, a), i[0] = e[0], i[1] = e[1], i[2] = e[2], i[3] = 1 + o, B.normalize(i, i));
  };
})();
B.setAxes = (function() {
  var e = V.create();
  return function(t, r, i, n) {
    return e[0] = i[0], e[3] = i[1], e[6] = i[2], e[1] = n[0], e[4] = n[1], e[7] = n[2], e[2] = -r[0], e[5] = -r[1], e[8] = -r[2], B.normalize(t, B.fromMat3(t, e));
  };
})();
B.clone = L.clone;
B.fromValues = L.fromValues;
B.copy = L.copy;
B.set = L.set;
B.identity = function(e) {
  return e[0] = 0, e[1] = 0, e[2] = 0, e[3] = 1, e;
};
B.setAxisAngle = function(e, t, r) {
  r = r * 0.5;
  var i = Math.sin(r);
  return e[0] = i * t[0], e[1] = i * t[1], e[2] = i * t[2], e[3] = Math.cos(r), e;
};
B.add = L.add;
B.multiply = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = r[0], l = r[1], u = r[2], f = r[3];
  return e[0] = i * f + o * s + n * u - a * l, e[1] = n * f + o * l + a * s - i * u, e[2] = a * f + o * u + i * l - n * s, e[3] = o * f - i * s - n * l - a * u, e;
};
B.mul = B.multiply;
B.scale = L.scale;
B.rotateX = function(e, t, r) {
  r *= 0.5;
  var i = t[0], n = t[1], a = t[2], o = t[3], s = Math.sin(r), l = Math.cos(r);
  return e[0] = i * l + o * s, e[1] = n * l + a * s, e[2] = a * l - n * s, e[3] = o * l - i * s, e;
};
B.rotateY = function(e, t, r) {
  r *= 0.5;
  var i = t[0], n = t[1], a = t[2], o = t[3], s = Math.sin(r), l = Math.cos(r);
  return e[0] = i * l - a * s, e[1] = n * l + o * s, e[2] = a * l + i * s, e[3] = o * l - n * s, e;
};
B.rotateZ = function(e, t, r) {
  r *= 0.5;
  var i = t[0], n = t[1], a = t[2], o = t[3], s = Math.sin(r), l = Math.cos(r);
  return e[0] = i * l + n * s, e[1] = n * l - i * s, e[2] = a * l + o * s, e[3] = o * l - a * s, e;
};
B.calculateW = function(e, t) {
  var r = t[0], i = t[1], n = t[2];
  return e[0] = r, e[1] = i, e[2] = n, e[3] = Math.sqrt(Math.abs(1 - r * r - i * i - n * n)), e;
};
B.dot = L.dot;
B.lerp = L.lerp;
B.slerp = function(e, t, r, i) {
  var n = t[0], a = t[1], o = t[2], s = t[3], l = r[0], u = r[1], f = r[2], h = r[3], d, c, v, p, m;
  return c = n * l + a * u + o * f + s * h, c < 0 && (c = -c, l = -l, u = -u, f = -f, h = -h), 1 - c > 1e-6 ? (d = Math.acos(c), v = Math.sin(d), p = Math.sin((1 - i) * d) / v, m = Math.sin(i * d) / v) : (p = 1 - i, m = i), e[0] = p * n + m * l, e[1] = p * a + m * u, e[2] = p * o + m * f, e[3] = p * s + m * h, e;
};
B.invert = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = r * r + i * i + n * n + a * a, s = o ? 1 / o : 0;
  return e[0] = -r * s, e[1] = -i * s, e[2] = -n * s, e[3] = a * s, e;
};
B.conjugate = function(e, t) {
  return e[0] = -t[0], e[1] = -t[1], e[2] = -t[2], e[3] = t[3], e;
};
B.length = L.length;
B.len = B.length;
B.squaredLength = L.squaredLength;
B.sqrLen = B.squaredLength;
B.normalize = L.normalize;
B.fromMat3 = function(e, t) {
  var r = t[0] + t[4] + t[8], i;
  if (r > 0)
    i = Math.sqrt(r + 1), e[3] = 0.5 * i, i = 0.5 / i, e[0] = (t[5] - t[7]) * i, e[1] = (t[6] - t[2]) * i, e[2] = (t[1] - t[3]) * i;
  else {
    var n = 0;
    t[4] > t[0] && (n = 1), t[8] > t[n * 3 + n] && (n = 2);
    var a = (n + 1) % 3, o = (n + 2) % 3;
    i = Math.sqrt(t[n * 3 + n] - t[a * 3 + a] - t[o * 3 + o] + 1), e[n] = 0.5 * i, i = 0.5 / i, e[3] = (t[a * 3 + o] - t[o * 3 + a]) * i, e[a] = (t[a * 3 + n] + t[n * 3 + a]) * i, e[o] = (t[o * 3 + n] + t[n * 3 + o]) * i;
  }
  return e;
};
var ee = function(e, t, r, i) {
  e = e || 0, t = t || 0, r = r || 0, i = i === void 0 ? 1 : i, this.array = B.fromValues(e, t, r, i), this._dirty = !0;
};
ee.prototype = {
  constructor: ee,
  /**
   * Add b to self
   * @param  {clay.Quaternion} b
   * @return {clay.Quaternion}
   */
  add: function(e) {
    return B.add(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Calculate the w component from x, y, z component
   * @return {clay.Quaternion}
   */
  calculateW: function() {
    return B.calculateW(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Set x, y and z components
   * @param  {number}  x
   * @param  {number}  y
   * @param  {number}  z
   * @param  {number}  w
   * @return {clay.Quaternion}
   */
  set: function(e, t, r, i) {
    return this.array[0] = e, this.array[1] = t, this.array[2] = r, this.array[3] = i, this._dirty = !0, this;
  },
  /**
   * Set x, y, z and w components from array
   * @param  {Float32Array|number[]} arr
   * @return {clay.Quaternion}
   */
  setArray: function(e) {
    return this.array[0] = e[0], this.array[1] = e[1], this.array[2] = e[2], this.array[3] = e[3], this._dirty = !0, this;
  },
  /**
   * Clone a new Quaternion
   * @return {clay.Quaternion}
   */
  clone: function() {
    return new ee(this.x, this.y, this.z, this.w);
  },
  /**
   * Calculates the conjugate of self If the quaternion is normalized,
   * this function is faster than invert and produces the same result.
   *
   * @return {clay.Quaternion}
   */
  conjugate: function() {
    return B.conjugate(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Copy from b
   * @param  {clay.Quaternion} b
   * @return {clay.Quaternion}
   */
  copy: function(e) {
    return B.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Dot product of self and b
   * @param  {clay.Quaternion} b
   * @return {number}
   */
  dot: function(e) {
    return B.dot(this.array, e.array);
  },
  /**
   * Set from the given 3x3 rotation matrix
   * @param  {clay.Matrix3} m
   * @return {clay.Quaternion}
   */
  fromMat3: function(e) {
    return B.fromMat3(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Set from the given 4x4 rotation matrix
   * The 4th column and 4th row will be droped
   * @param  {clay.Matrix4} m
   * @return {clay.Quaternion}
   */
  fromMat4: (function() {
    var e = V.create();
    return function(t) {
      return V.fromMat4(e, t.array), V.transpose(e, e), B.fromMat3(this.array, e), this._dirty = !0, this;
    };
  })(),
  /**
   * Set to identity quaternion
   * @return {clay.Quaternion}
   */
  identity: function() {
    return B.identity(this.array), this._dirty = !0, this;
  },
  /**
   * Invert self
   * @return {clay.Quaternion}
   */
  invert: function() {
    return B.invert(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Alias of length
   * @return {number}
   */
  len: function() {
    return B.len(this.array);
  },
  /**
   * Calculate the length
   * @return {number}
   */
  length: function() {
    return B.length(this.array);
  },
  /**
   * Linear interpolation between a and b
   * @param  {clay.Quaternion} a
   * @param  {clay.Quaternion} b
   * @param  {number}  t
   * @return {clay.Quaternion}
   */
  lerp: function(e, t, r) {
    return B.lerp(this.array, e.array, t.array, r), this._dirty = !0, this;
  },
  /**
   * Alias for multiply
   * @param  {clay.Quaternion} b
   * @return {clay.Quaternion}
   */
  mul: function(e) {
    return B.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiplyLeft
   * @param  {clay.Quaternion} a
   * @return {clay.Quaternion}
   */
  mulLeft: function(e) {
    return B.multiply(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Mutiply self and b
   * @param  {clay.Quaternion} b
   * @return {clay.Quaternion}
   */
  multiply: function(e) {
    return B.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Mutiply a and self
   * Quaternion mutiply is not commutative, so the result of mutiplyLeft is different with multiply.
   * @param  {clay.Quaternion} a
   * @return {clay.Quaternion}
   */
  multiplyLeft: function(e) {
    return B.multiply(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Normalize self
   * @return {clay.Quaternion}
   */
  normalize: function() {
    return B.normalize(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian about X axis
   * @param {number} rad
   * @return {clay.Quaternion}
   */
  rotateX: function(e) {
    return B.rotateX(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian about Y axis
   * @param {number} rad
   * @return {clay.Quaternion}
   */
  rotateY: function(e) {
    return B.rotateY(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian about Z axis
   * @param {number} rad
   * @return {clay.Quaternion}
   */
  rotateZ: function(e) {
    return B.rotateZ(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Sets self to represent the shortest rotation from Vector3 a to Vector3 b.
   * a and b needs to be normalized
   * @param  {clay.Vector3} a
   * @param  {clay.Vector3} b
   * @return {clay.Quaternion}
   */
  rotationTo: function(e, t) {
    return B.rotationTo(this.array, e.array, t.array), this._dirty = !0, this;
  },
  /**
   * Sets self with values corresponding to the given axes
   * @param {clay.Vector3} view
   * @param {clay.Vector3} right
   * @param {clay.Vector3} up
   * @return {clay.Quaternion}
   */
  setAxes: function(e, t, r) {
    return B.setAxes(this.array, e.array, t.array, r.array), this._dirty = !0, this;
  },
  /**
   * Sets self with a rotation axis and rotation angle
   * @param {clay.Vector3} axis
   * @param {number} rad
   * @return {clay.Quaternion}
   */
  setAxisAngle: function(e, t) {
    return B.setAxisAngle(this.array, e.array, t), this._dirty = !0, this;
  },
  /**
   * Perform spherical linear interpolation between a and b
   * @param  {clay.Quaternion} a
   * @param  {clay.Quaternion} b
   * @param  {number} t
   * @return {clay.Quaternion}
   */
  slerp: function(e, t, r) {
    return B.slerp(this.array, e.array, t.array, r), this._dirty = !0, this;
  },
  /**
   * Alias for squaredLength
   * @return {number}
   */
  sqrLen: function() {
    return B.sqrLen(this.array);
  },
  /**
   * Squared length of self
   * @return {number}
   */
  squaredLength: function() {
    return B.squaredLength(this.array);
  },
  /**
   * Set from euler
   * @param {clay.Vector3} v
   * @param {String} order
   */
  fromEuler: function(e, t) {
    return ee.fromEuler(this, e, t);
  },
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
var ar = Object.defineProperty;
if (ar) {
  var Nr = ee.prototype;
  ar(Nr, "x", {
    get: function() {
      return this.array[0];
    },
    set: function(e) {
      this.array[0] = e, this._dirty = !0;
    }
  }), ar(Nr, "y", {
    get: function() {
      return this.array[1];
    },
    set: function(e) {
      this.array[1] = e, this._dirty = !0;
    }
  }), ar(Nr, "z", {
    get: function() {
      return this.array[2];
    },
    set: function(e) {
      this.array[2] = e, this._dirty = !0;
    }
  }), ar(Nr, "w", {
    get: function() {
      return this.array[3];
    },
    set: function(e) {
      this.array[3] = e, this._dirty = !0;
    }
  });
}
ee.add = function(e, t, r) {
  return B.add(e.array, t.array, r.array), e._dirty = !0, e;
};
ee.set = function(e, t, r, i, n) {
  B.set(e.array, t, r, i, n), e._dirty = !0;
};
ee.copy = function(e, t) {
  return B.copy(e.array, t.array), e._dirty = !0, e;
};
ee.calculateW = function(e, t) {
  return B.calculateW(e.array, t.array), e._dirty = !0, e;
};
ee.conjugate = function(e, t) {
  return B.conjugate(e.array, t.array), e._dirty = !0, e;
};
ee.identity = function(e) {
  return B.identity(e.array), e._dirty = !0, e;
};
ee.invert = function(e, t) {
  return B.invert(e.array, t.array), e._dirty = !0, e;
};
ee.dot = function(e, t) {
  return B.dot(e.array, t.array);
};
ee.len = function(e) {
  return B.length(e.array);
};
ee.lerp = function(e, t, r, i) {
  return B.lerp(e.array, t.array, r.array, i), e._dirty = !0, e;
};
ee.slerp = function(e, t, r, i) {
  return B.slerp(e.array, t.array, r.array, i), e._dirty = !0, e;
};
ee.mul = function(e, t, r) {
  return B.multiply(e.array, t.array, r.array), e._dirty = !0, e;
};
ee.multiply = ee.mul;
ee.rotateX = function(e, t, r) {
  return B.rotateX(e.array, t.array, r), e._dirty = !0, e;
};
ee.rotateY = function(e, t, r) {
  return B.rotateY(e.array, t.array, r), e._dirty = !0, e;
};
ee.rotateZ = function(e, t, r) {
  return B.rotateZ(e.array, t.array, r), e._dirty = !0, e;
};
ee.setAxisAngle = function(e, t, r) {
  return B.setAxisAngle(e.array, t.array, r), e._dirty = !0, e;
};
ee.normalize = function(e, t) {
  return B.normalize(e.array, t.array), e._dirty = !0, e;
};
ee.sqrLen = function(e) {
  return B.sqrLen(e.array);
};
ee.squaredLength = ee.sqrLen;
ee.fromMat3 = function(e, t) {
  return B.fromMat3(e.array, t.array), e._dirty = !0, e;
};
ee.setAxes = function(e, t, r, i) {
  return B.setAxes(e.array, t.array, r.array, i.array), e._dirty = !0, e;
};
ee.rotationTo = function(e, t, r) {
  return B.rotationTo(e.array, t.array, r.array), e._dirty = !0, e;
};
ee.fromEuler = function(e, t, f) {
  e._dirty = !0, t = t.array;
  var i = e.array, n = Math.cos(t[0] / 2), a = Math.cos(t[1] / 2), o = Math.cos(t[2] / 2), s = Math.sin(t[0] / 2), l = Math.sin(t[1] / 2), u = Math.sin(t[2] / 2), f = (f || "XYZ").toUpperCase();
  switch (f) {
    case "XYZ":
      i[0] = s * a * o + n * l * u, i[1] = n * l * o - s * a * u, i[2] = n * a * u + s * l * o, i[3] = n * a * o - s * l * u;
      break;
    case "YXZ":
      i[0] = s * a * o + n * l * u, i[1] = n * l * o - s * a * u, i[2] = n * a * u - s * l * o, i[3] = n * a * o + s * l * u;
      break;
    case "ZXY":
      i[0] = s * a * o - n * l * u, i[1] = n * l * o + s * a * u, i[2] = n * a * u + s * l * o, i[3] = n * a * o - s * l * u;
      break;
    case "ZYX":
      i[0] = s * a * o - n * l * u, i[1] = n * l * o + s * a * u, i[2] = n * a * u - s * l * o, i[3] = n * a * o + s * l * u;
      break;
    case "YZX":
      i[0] = s * a * o + n * l * u, i[1] = n * l * o + s * a * u, i[2] = n * a * u - s * l * o, i[3] = n * a * o - s * l * u;
      break;
    case "XZY":
      i[0] = s * a * o - n * l * u, i[1] = n * l * o - s * a * u, i[2] = n * a * u + s * l * o, i[3] = n * a * o + s * l * u;
      break;
  }
};
var D = {};
D.create = function() {
  var e = new Ee(16);
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 0, e[4] = 0, e[5] = 1, e[6] = 0, e[7] = 0, e[8] = 0, e[9] = 0, e[10] = 1, e[11] = 0, e[12] = 0, e[13] = 0, e[14] = 0, e[15] = 1, e;
};
D.clone = function(e) {
  var t = new Ee(16);
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t[4] = e[4], t[5] = e[5], t[6] = e[6], t[7] = e[7], t[8] = e[8], t[9] = e[9], t[10] = e[10], t[11] = e[11], t[12] = e[12], t[13] = e[13], t[14] = e[14], t[15] = e[15], t;
};
D.copy = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[4] = t[4], e[5] = t[5], e[6] = t[6], e[7] = t[7], e[8] = t[8], e[9] = t[9], e[10] = t[10], e[11] = t[11], e[12] = t[12], e[13] = t[13], e[14] = t[14], e[15] = t[15], e;
};
D.identity = function(e) {
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 0, e[4] = 0, e[5] = 1, e[6] = 0, e[7] = 0, e[8] = 0, e[9] = 0, e[10] = 1, e[11] = 0, e[12] = 0, e[13] = 0, e[14] = 0, e[15] = 1, e;
};
D.transpose = function(e, t) {
  if (e === t) {
    var r = t[1], i = t[2], n = t[3], a = t[6], o = t[7], s = t[11];
    e[1] = t[4], e[2] = t[8], e[3] = t[12], e[4] = r, e[6] = t[9], e[7] = t[13], e[8] = i, e[9] = a, e[11] = t[14], e[12] = n, e[13] = o, e[14] = s;
  } else
    e[0] = t[0], e[1] = t[4], e[2] = t[8], e[3] = t[12], e[4] = t[1], e[5] = t[5], e[6] = t[9], e[7] = t[13], e[8] = t[2], e[9] = t[6], e[10] = t[10], e[11] = t[14], e[12] = t[3], e[13] = t[7], e[14] = t[11], e[15] = t[15];
  return e;
};
D.invert = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = t[4], s = t[5], l = t[6], u = t[7], f = t[8], h = t[9], d = t[10], c = t[11], v = t[12], p = t[13], m = t[14], _ = t[15], x = r * s - i * o, y = r * l - n * o, g = r * u - a * o, b = i * l - n * s, w = i * u - a * s, C = n * u - a * l, A = f * p - h * v, R = f * m - d * v, H = f * _ - c * v, O = h * m - d * p, z = h * _ - c * p, G = d * _ - c * m, F = x * G - y * z + g * O + b * H - w * R + C * A;
  return F ? (F = 1 / F, e[0] = (s * G - l * z + u * O) * F, e[1] = (n * z - i * G - a * O) * F, e[2] = (p * C - m * w + _ * b) * F, e[3] = (d * w - h * C - c * b) * F, e[4] = (l * H - o * G - u * R) * F, e[5] = (r * G - n * H + a * R) * F, e[6] = (m * g - v * C - _ * y) * F, e[7] = (f * C - d * g + c * y) * F, e[8] = (o * z - s * H + u * A) * F, e[9] = (i * H - r * z - a * A) * F, e[10] = (v * w - p * g + _ * x) * F, e[11] = (h * g - f * w - c * x) * F, e[12] = (s * R - o * O - l * A) * F, e[13] = (r * O - i * R + n * A) * F, e[14] = (p * y - v * b - m * x) * F, e[15] = (f * b - h * y + d * x) * F, e) : null;
};
D.adjoint = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = t[4], s = t[5], l = t[6], u = t[7], f = t[8], h = t[9], d = t[10], c = t[11], v = t[12], p = t[13], m = t[14], _ = t[15];
  return e[0] = s * (d * _ - c * m) - h * (l * _ - u * m) + p * (l * c - u * d), e[1] = -(i * (d * _ - c * m) - h * (n * _ - a * m) + p * (n * c - a * d)), e[2] = i * (l * _ - u * m) - s * (n * _ - a * m) + p * (n * u - a * l), e[3] = -(i * (l * c - u * d) - s * (n * c - a * d) + h * (n * u - a * l)), e[4] = -(o * (d * _ - c * m) - f * (l * _ - u * m) + v * (l * c - u * d)), e[5] = r * (d * _ - c * m) - f * (n * _ - a * m) + v * (n * c - a * d), e[6] = -(r * (l * _ - u * m) - o * (n * _ - a * m) + v * (n * u - a * l)), e[7] = r * (l * c - u * d) - o * (n * c - a * d) + f * (n * u - a * l), e[8] = o * (h * _ - c * p) - f * (s * _ - u * p) + v * (s * c - u * h), e[9] = -(r * (h * _ - c * p) - f * (i * _ - a * p) + v * (i * c - a * h)), e[10] = r * (s * _ - u * p) - o * (i * _ - a * p) + v * (i * u - a * s), e[11] = -(r * (s * c - u * h) - o * (i * c - a * h) + f * (i * u - a * s)), e[12] = -(o * (h * m - d * p) - f * (s * m - l * p) + v * (s * d - l * h)), e[13] = r * (h * m - d * p) - f * (i * m - n * p) + v * (i * d - n * h), e[14] = -(r * (s * m - l * p) - o * (i * m - n * p) + v * (i * l - n * s)), e[15] = r * (s * d - l * h) - o * (i * d - n * h) + f * (i * l - n * s), e;
};
D.determinant = function(e) {
  var t = e[0], r = e[1], i = e[2], n = e[3], a = e[4], o = e[5], s = e[6], l = e[7], u = e[8], f = e[9], h = e[10], d = e[11], c = e[12], v = e[13], p = e[14], m = e[15], _ = t * o - r * a, x = t * s - i * a, y = t * l - n * a, g = r * s - i * o, b = r * l - n * o, w = i * l - n * s, C = u * v - f * c, A = u * p - h * c, R = u * m - d * c, H = f * p - h * v, O = f * m - d * v, z = h * m - d * p;
  return _ * z - x * O + y * H + g * R - b * A + w * C;
};
D.multiply = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = t[6], f = t[7], h = t[8], d = t[9], c = t[10], v = t[11], p = t[12], m = t[13], _ = t[14], x = t[15], y = r[0], g = r[1], b = r[2], w = r[3];
  return e[0] = y * i + g * s + b * h + w * p, e[1] = y * n + g * l + b * d + w * m, e[2] = y * a + g * u + b * c + w * _, e[3] = y * o + g * f + b * v + w * x, y = r[4], g = r[5], b = r[6], w = r[7], e[4] = y * i + g * s + b * h + w * p, e[5] = y * n + g * l + b * d + w * m, e[6] = y * a + g * u + b * c + w * _, e[7] = y * o + g * f + b * v + w * x, y = r[8], g = r[9], b = r[10], w = r[11], e[8] = y * i + g * s + b * h + w * p, e[9] = y * n + g * l + b * d + w * m, e[10] = y * a + g * u + b * c + w * _, e[11] = y * o + g * f + b * v + w * x, y = r[12], g = r[13], b = r[14], w = r[15], e[12] = y * i + g * s + b * h + w * p, e[13] = y * n + g * l + b * d + w * m, e[14] = y * a + g * u + b * c + w * _, e[15] = y * o + g * f + b * v + w * x, e;
};
D.multiplyAffine = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[4], s = t[5], l = t[6], u = t[8], f = t[9], h = t[10], d = t[12], c = t[13], v = t[14], p = r[0], m = r[1], _ = r[2];
  return e[0] = p * i + m * o + _ * u, e[1] = p * n + m * s + _ * f, e[2] = p * a + m * l + _ * h, p = r[4], m = r[5], _ = r[6], e[4] = p * i + m * o + _ * u, e[5] = p * n + m * s + _ * f, e[6] = p * a + m * l + _ * h, p = r[8], m = r[9], _ = r[10], e[8] = p * i + m * o + _ * u, e[9] = p * n + m * s + _ * f, e[10] = p * a + m * l + _ * h, p = r[12], m = r[13], _ = r[14], e[12] = p * i + m * o + _ * u + d, e[13] = p * n + m * s + _ * f + c, e[14] = p * a + m * l + _ * h + v, e;
};
D.mul = D.multiply;
D.mulAffine = D.multiplyAffine;
D.translate = function(e, t, r) {
  var i = r[0], n = r[1], a = r[2], o, s, l, u, f, h, d, c, v, p, m, _;
  return t === e ? (e[12] = t[0] * i + t[4] * n + t[8] * a + t[12], e[13] = t[1] * i + t[5] * n + t[9] * a + t[13], e[14] = t[2] * i + t[6] * n + t[10] * a + t[14], e[15] = t[3] * i + t[7] * n + t[11] * a + t[15]) : (o = t[0], s = t[1], l = t[2], u = t[3], f = t[4], h = t[5], d = t[6], c = t[7], v = t[8], p = t[9], m = t[10], _ = t[11], e[0] = o, e[1] = s, e[2] = l, e[3] = u, e[4] = f, e[5] = h, e[6] = d, e[7] = c, e[8] = v, e[9] = p, e[10] = m, e[11] = _, e[12] = o * i + f * n + v * a + t[12], e[13] = s * i + h * n + p * a + t[13], e[14] = l * i + d * n + m * a + t[14], e[15] = u * i + c * n + _ * a + t[15]), e;
};
D.scale = function(e, t, r) {
  var i = r[0], n = r[1], a = r[2];
  return e[0] = t[0] * i, e[1] = t[1] * i, e[2] = t[2] * i, e[3] = t[3] * i, e[4] = t[4] * n, e[5] = t[5] * n, e[6] = t[6] * n, e[7] = t[7] * n, e[8] = t[8] * a, e[9] = t[9] * a, e[10] = t[10] * a, e[11] = t[11] * a, e[12] = t[12], e[13] = t[13], e[14] = t[14], e[15] = t[15], e;
};
D.rotate = function(e, t, r, i) {
  var n = i[0], a = i[1], o = i[2], s = Math.sqrt(n * n + a * a + o * o), l, u, f, h, d, c, v, p, m, _, x, y, g, b, w, C, A, R, H, O, z, G, F, W;
  return Math.abs(s) < Jr ? null : (s = 1 / s, n *= s, a *= s, o *= s, l = Math.sin(r), u = Math.cos(r), f = 1 - u, h = t[0], d = t[1], c = t[2], v = t[3], p = t[4], m = t[5], _ = t[6], x = t[7], y = t[8], g = t[9], b = t[10], w = t[11], C = n * n * f + u, A = a * n * f + o * l, R = o * n * f - a * l, H = n * a * f - o * l, O = a * a * f + u, z = o * a * f + n * l, G = n * o * f + a * l, F = a * o * f - n * l, W = o * o * f + u, e[0] = h * C + p * A + y * R, e[1] = d * C + m * A + g * R, e[2] = c * C + _ * A + b * R, e[3] = v * C + x * A + w * R, e[4] = h * H + p * O + y * z, e[5] = d * H + m * O + g * z, e[6] = c * H + _ * O + b * z, e[7] = v * H + x * O + w * z, e[8] = h * G + p * F + y * W, e[9] = d * G + m * F + g * W, e[10] = c * G + _ * F + b * W, e[11] = v * G + x * F + w * W, t !== e && (e[12] = t[12], e[13] = t[13], e[14] = t[14], e[15] = t[15]), e);
};
D.rotateX = function(e, t, r) {
  var i = Math.sin(r), n = Math.cos(r), a = t[4], o = t[5], s = t[6], l = t[7], u = t[8], f = t[9], h = t[10], d = t[11];
  return t !== e && (e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[12] = t[12], e[13] = t[13], e[14] = t[14], e[15] = t[15]), e[4] = a * n + u * i, e[5] = o * n + f * i, e[6] = s * n + h * i, e[7] = l * n + d * i, e[8] = u * n - a * i, e[9] = f * n - o * i, e[10] = h * n - s * i, e[11] = d * n - l * i, e;
};
D.rotateY = function(e, t, r) {
  var i = Math.sin(r), n = Math.cos(r), a = t[0], o = t[1], s = t[2], l = t[3], u = t[8], f = t[9], h = t[10], d = t[11];
  return t !== e && (e[4] = t[4], e[5] = t[5], e[6] = t[6], e[7] = t[7], e[12] = t[12], e[13] = t[13], e[14] = t[14], e[15] = t[15]), e[0] = a * n - u * i, e[1] = o * n - f * i, e[2] = s * n - h * i, e[3] = l * n - d * i, e[8] = a * i + u * n, e[9] = o * i + f * n, e[10] = s * i + h * n, e[11] = l * i + d * n, e;
};
D.rotateZ = function(e, t, r) {
  var i = Math.sin(r), n = Math.cos(r), a = t[0], o = t[1], s = t[2], l = t[3], u = t[4], f = t[5], h = t[6], d = t[7];
  return t !== e && (e[8] = t[8], e[9] = t[9], e[10] = t[10], e[11] = t[11], e[12] = t[12], e[13] = t[13], e[14] = t[14], e[15] = t[15]), e[0] = a * n + u * i, e[1] = o * n + f * i, e[2] = s * n + h * i, e[3] = l * n + d * i, e[4] = u * n - a * i, e[5] = f * n - o * i, e[6] = h * n - s * i, e[7] = d * n - l * i, e;
};
D.fromRotationTranslation = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = i + i, l = n + n, u = a + a, f = i * s, h = i * l, d = i * u, c = n * l, v = n * u, p = a * u, m = o * s, _ = o * l, x = o * u;
  return e[0] = 1 - (c + p), e[1] = h + x, e[2] = d - _, e[3] = 0, e[4] = h - x, e[5] = 1 - (f + p), e[6] = v + m, e[7] = 0, e[8] = d + _, e[9] = v - m, e[10] = 1 - (f + c), e[11] = 0, e[12] = r[0], e[13] = r[1], e[14] = r[2], e[15] = 1, e;
};
D.fromQuat = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = r + r, s = i + i, l = n + n, u = r * o, f = i * o, h = i * s, d = n * o, c = n * s, v = n * l, p = a * o, m = a * s, _ = a * l;
  return e[0] = 1 - h - v, e[1] = f + _, e[2] = d - m, e[3] = 0, e[4] = f - _, e[5] = 1 - u - v, e[6] = c + p, e[7] = 0, e[8] = d + m, e[9] = c - p, e[10] = 1 - u - h, e[11] = 0, e[12] = 0, e[13] = 0, e[14] = 0, e[15] = 1, e;
};
D.frustum = function(e, t, r, i, n, a, o) {
  var s = 1 / (r - t), l = 1 / (n - i), u = 1 / (a - o);
  return e[0] = a * 2 * s, e[1] = 0, e[2] = 0, e[3] = 0, e[4] = 0, e[5] = a * 2 * l, e[6] = 0, e[7] = 0, e[8] = (r + t) * s, e[9] = (n + i) * l, e[10] = (o + a) * u, e[11] = -1, e[12] = 0, e[13] = 0, e[14] = o * a * 2 * u, e[15] = 0, e;
};
D.perspective = function(e, t, r, i, n) {
  var a = 1 / Math.tan(t / 2), o = 1 / (i - n);
  return e[0] = a / r, e[1] = 0, e[2] = 0, e[3] = 0, e[4] = 0, e[5] = a, e[6] = 0, e[7] = 0, e[8] = 0, e[9] = 0, e[10] = (n + i) * o, e[11] = -1, e[12] = 0, e[13] = 0, e[14] = 2 * n * i * o, e[15] = 0, e;
};
D.ortho = function(e, t, r, i, n, a, o) {
  var s = 1 / (t - r), l = 1 / (i - n), u = 1 / (a - o);
  return e[0] = -2 * s, e[1] = 0, e[2] = 0, e[3] = 0, e[4] = 0, e[5] = -2 * l, e[6] = 0, e[7] = 0, e[8] = 0, e[9] = 0, e[10] = 2 * u, e[11] = 0, e[12] = (t + r) * s, e[13] = (n + i) * l, e[14] = (o + a) * u, e[15] = 1, e;
};
D.lookAt = function(e, t, r, i) {
  var n, a, o, s, l, u, f, h, d, c, v = t[0], p = t[1], m = t[2], _ = i[0], x = i[1], y = i[2], g = r[0], b = r[1], w = r[2];
  return Math.abs(v - g) < Jr && Math.abs(p - b) < Jr && Math.abs(m - w) < Jr ? D.identity(e) : (f = v - g, h = p - b, d = m - w, c = 1 / Math.sqrt(f * f + h * h + d * d), f *= c, h *= c, d *= c, n = x * d - y * h, a = y * f - _ * d, o = _ * h - x * f, c = Math.sqrt(n * n + a * a + o * o), c ? (c = 1 / c, n *= c, a *= c, o *= c) : (n = 0, a = 0, o = 0), s = h * o - d * a, l = d * n - f * o, u = f * a - h * n, c = Math.sqrt(s * s + l * l + u * u), c ? (c = 1 / c, s *= c, l *= c, u *= c) : (s = 0, l = 0, u = 0), e[0] = n, e[1] = s, e[2] = f, e[3] = 0, e[4] = a, e[5] = l, e[6] = h, e[7] = 0, e[8] = o, e[9] = u, e[10] = d, e[11] = 0, e[12] = -(n * v + a * p + o * m), e[13] = -(s * v + l * p + u * m), e[14] = -(f * v + h * p + d * m), e[15] = 1, e);
};
D.frob = function(e) {
  return Math.sqrt(Math.pow(e[0], 2) + Math.pow(e[1], 2) + Math.pow(e[2], 2) + Math.pow(e[3], 2) + Math.pow(e[4], 2) + Math.pow(e[5], 2) + Math.pow(e[6], 2) + Math.pow(e[7], 2) + Math.pow(e[8], 2) + Math.pow(e[9], 2) + Math.pow(e[10], 2) + Math.pow(e[11], 2) + Math.pow(e[12], 2) + Math.pow(e[13], 2) + Math.pow(e[14], 2) + Math.pow(e[15], 2));
};
var I = function() {
  this._axisX = new S(), this._axisY = new S(), this._axisZ = new S(), this.array = D.create(), this._dirty = !0;
};
I.prototype = {
  constructor: I,
  /**
   * Set components from array
   * @param  {Float32Array|number[]} arr
   */
  setArray: function(e) {
    for (var t = 0; t < this.array.length; t++)
      this.array[t] = e[t];
    return this._dirty = !0, this;
  },
  /**
   * Calculate the adjugate of self, in-place
   * @return {clay.Matrix4}
   */
  adjoint: function() {
    return D.adjoint(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Clone a new Matrix4
   * @return {clay.Matrix4}
   */
  clone: function() {
    return new I().copy(this);
  },
  /**
   * Copy from b
   * @param  {clay.Matrix4} b
   * @return {clay.Matrix4}
   */
  copy: function(e) {
    return D.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Calculate matrix determinant
   * @return {number}
   */
  determinant: function() {
    return D.determinant(this.array);
  },
  /**
   * Set upper 3x3 part from quaternion
   * @param  {clay.Quaternion} q
   * @return {clay.Matrix4}
   */
  fromQuat: function(e) {
    return D.fromQuat(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Set from a quaternion rotation and a vector translation
   * @param  {clay.Quaternion} q
   * @param  {clay.Vector3} v
   * @return {clay.Matrix4}
   */
  fromRotationTranslation: function(e, t) {
    return D.fromRotationTranslation(this.array, e.array, t.array), this._dirty = !0, this;
  },
  /**
   * Set from Matrix2d, it is used when converting a 2d shape to 3d space.
   * In 3d space it is equivalent to ranslate on xy plane and rotate about z axis
   * @param  {clay.Matrix2d} m2d
   * @return {clay.Matrix4}
   */
  fromMat2d: function(e) {
    return I.fromMat2d(this, e), this;
  },
  /**
   * Set from frustum bounds
   * @param  {number} left
   * @param  {number} right
   * @param  {number} bottom
   * @param  {number} top
   * @param  {number} near
   * @param  {number} far
   * @return {clay.Matrix4}
   */
  frustum: function(e, t, r, i, n, a) {
    return D.frustum(this.array, e, t, r, i, n, a), this._dirty = !0, this;
  },
  /**
   * Set to a identity matrix
   * @return {clay.Matrix4}
   */
  identity: function() {
    return D.identity(this.array), this._dirty = !0, this;
  },
  /**
   * Invert self
   * @return {clay.Matrix4}
   */
  invert: function() {
    return D.invert(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Set as a matrix with the given eye position, focal point, and up axis
   * @param  {clay.Vector3} eye
   * @param  {clay.Vector3} center
   * @param  {clay.Vector3} up
   * @return {clay.Matrix4}
   */
  lookAt: function(e, t, r) {
    return D.lookAt(this.array, e.array, t.array, r.array), this._dirty = !0, this;
  },
  /**
   * Alias for mutiply
   * @param  {clay.Matrix4} b
   * @return {clay.Matrix4}
   */
  mul: function(e) {
    return D.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiplyLeft
   * @param  {clay.Matrix4} a
   * @return {clay.Matrix4}
   */
  mulLeft: function(e) {
    return D.mul(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Multiply self and b
   * @param  {clay.Matrix4} b
   * @return {clay.Matrix4}
   */
  multiply: function(e) {
    return D.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Multiply a and self, a is on the left
   * @param  {clay.Matrix3} a
   * @return {clay.Matrix3}
   */
  multiplyLeft: function(e) {
    return D.multiply(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Set as a orthographic projection matrix
   * @param  {number} left
   * @param  {number} right
   * @param  {number} bottom
   * @param  {number} top
   * @param  {number} near
   * @param  {number} far
   * @return {clay.Matrix4}
   */
  ortho: function(e, t, r, i, n, a) {
    return D.ortho(this.array, e, t, r, i, n, a), this._dirty = !0, this;
  },
  /**
   * Set as a perspective projection matrix
   * @param  {number} fovy
   * @param  {number} aspect
   * @param  {number} near
   * @param  {number} far
   * @return {clay.Matrix4}
   */
  perspective: function(e, t, r, i) {
    return D.perspective(this.array, e, t, r, i), this._dirty = !0, this;
  },
  /**
   * Rotate self by rad about axis.
   * Equal to right-multiply a rotaion matrix
   * @param  {number}   rad
   * @param  {clay.Vector3} axis
   * @return {clay.Matrix4}
   */
  rotate: function(e, t) {
    return D.rotate(this.array, this.array, e, t.array), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian about X axis.
   * Equal to right-multiply a rotaion matrix
   * @param {number} rad
   * @return {clay.Matrix4}
   */
  rotateX: function(e) {
    return D.rotateX(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian about Y axis.
   * Equal to right-multiply a rotaion matrix
   * @param {number} rad
   * @return {clay.Matrix4}
   */
  rotateY: function(e) {
    return D.rotateY(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian about Z axis.
   * Equal to right-multiply a rotaion matrix
   * @param {number} rad
   * @return {clay.Matrix4}
   */
  rotateZ: function(e) {
    return D.rotateZ(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale self by s
   * Equal to right-multiply a scale matrix
   * @param  {clay.Vector3}  s
   * @return {clay.Matrix4}
   */
  scale: function(e) {
    return D.scale(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Translate self by v.
   * Equal to right-multiply a translate matrix
   * @param  {clay.Vector3}  v
   * @return {clay.Matrix4}
   */
  translate: function(e) {
    return D.translate(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transpose self, in-place.
   * @return {clay.Matrix2}
   */
  transpose: function() {
    return D.transpose(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Decompose a matrix to SRT
   * @param {clay.Vector3} [scale]
   * @param {clay.Quaternion} rotation
   * @param {clay.Vector} position
   * @see http://msdn.microsoft.com/en-us/library/microsoft.xna.framework.matrix.decompose.aspx
   */
  decomposeMatrix: (function() {
    var e = T.create(), t = T.create(), r = T.create(), i = V.create();
    return function(n, a, o) {
      var s = this.array;
      T.set(e, s[0], s[1], s[2]), T.set(t, s[4], s[5], s[6]), T.set(r, s[8], s[9], s[10]);
      var l = T.length(e), u = T.length(t), f = T.length(r), h = this.determinant();
      h < 0 && (l = -l), n && n.set(l, u, f), o.set(s[12], s[13], s[14]), V.fromMat4(i, s), i[0] /= l, i[1] /= l, i[2] /= l, i[3] /= u, i[4] /= u, i[5] /= u, i[6] /= f, i[7] /= f, i[8] /= f, B.fromMat3(a.array, i), B.normalize(a.array, a.array), a._dirty = !0, o._dirty = !0;
    };
  })(),
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
var Or = Object.defineProperty;
if (Or) {
  var Ri = I.prototype;
  Or(Ri, "z", {
    get: function() {
      var e = this.array;
      return this._axisZ.set(e[8], e[9], e[10]), this._axisZ;
    },
    set: function(e) {
      var t = this.array;
      e = e.array, t[8] = e[0], t[9] = e[1], t[10] = e[2], this._dirty = !0;
    }
  }), Or(Ri, "y", {
    get: function() {
      var e = this.array;
      return this._axisY.set(e[4], e[5], e[6]), this._axisY;
    },
    set: function(e) {
      var t = this.array;
      e = e.array, t[4] = e[0], t[5] = e[1], t[6] = e[2], this._dirty = !0;
    }
  }), Or(Ri, "x", {
    get: function() {
      var e = this.array;
      return this._axisX.set(e[0], e[1], e[2]), this._axisX;
    },
    set: function(e) {
      var t = this.array;
      e = e.array, t[0] = e[0], t[1] = e[1], t[2] = e[2], this._dirty = !0;
    }
  });
}
I.adjoint = function(e, t) {
  return D.adjoint(e.array, t.array), e._dirty = !0, e;
};
I.copy = function(e, t) {
  return D.copy(e.array, t.array), e._dirty = !0, e;
};
I.determinant = function(e) {
  return D.determinant(e.array);
};
I.identity = function(e) {
  return D.identity(e.array), e._dirty = !0, e;
};
I.ortho = function(e, t, r, i, n, a, o) {
  return D.ortho(e.array, t, r, i, n, a, o), e._dirty = !0, e;
};
I.perspective = function(e, t, r, i, n) {
  return D.perspective(e.array, t, r, i, n), e._dirty = !0, e;
};
I.lookAt = function(e, t, r, i) {
  return D.lookAt(e.array, t.array, r.array, i.array), e._dirty = !0, e;
};
I.invert = function(e, t) {
  return D.invert(e.array, t.array), e._dirty = !0, e;
};
I.mul = function(e, t, r) {
  return D.mul(e.array, t.array, r.array), e._dirty = !0, e;
};
I.multiply = I.mul;
I.fromQuat = function(e, t) {
  return D.fromQuat(e.array, t.array), e._dirty = !0, e;
};
I.fromRotationTranslation = function(e, t, r) {
  return D.fromRotationTranslation(e.array, t.array, r.array), e._dirty = !0, e;
};
I.fromMat2d = function(i, r) {
  i._dirty = !0;
  var r = r.array, i = i.array;
  return i[0] = r[0], i[4] = r[2], i[12] = r[4], i[1] = r[1], i[5] = r[3], i[13] = r[5], i;
};
I.rotate = function(e, t, r, i) {
  return D.rotate(e.array, t.array, r, i.array), e._dirty = !0, e;
};
I.rotateX = function(e, t, r) {
  return D.rotateX(e.array, t.array, r), e._dirty = !0, e;
};
I.rotateY = function(e, t, r) {
  return D.rotateY(e.array, t.array, r), e._dirty = !0, e;
};
I.rotateZ = function(e, t, r) {
  return D.rotateZ(e.array, t.array, r), e._dirty = !0, e;
};
I.scale = function(e, t, r) {
  return D.scale(e.array, t.array, r.array), e._dirty = !0, e;
};
I.transpose = function(e, t) {
  return D.transpose(e.array, t.array), e._dirty = !0, e;
};
I.translate = function(e, t, r) {
  return D.translate(e.array, t.array, r.array), e._dirty = !0, e;
};
var dt = T.set, Ir = T.copy, we = function(e, t) {
  this.min = e || new S(1 / 0, 1 / 0, 1 / 0), this.max = t || new S(-1 / 0, -1 / 0, -1 / 0), this.vertices = null;
};
we.prototype = {
  constructor: we,
  /**
   * Update min and max coords from a vertices array
   * @param  {array} vertices
   */
  updateFromVertices: function(e) {
    if (e.length > 0) {
      var t = this.min, r = this.max, i = t.array, n = r.array;
      Ir(i, e[0]), Ir(n, e[0]);
      for (var a = 1; a < e.length; a++) {
        var o = e[a];
        o[0] < i[0] && (i[0] = o[0]), o[1] < i[1] && (i[1] = o[1]), o[2] < i[2] && (i[2] = o[2]), o[0] > n[0] && (n[0] = o[0]), o[1] > n[1] && (n[1] = o[1]), o[2] > n[2] && (n[2] = o[2]);
      }
      t._dirty = !0, r._dirty = !0;
    }
  },
  /**
   * Union operation with another bounding box
   * @param  {clay.BoundingBox} bbox
   */
  union: function(e) {
    var t = this.min, r = this.max;
    return T.min(t.array, t.array, e.min.array), T.max(r.array, r.array, e.max.array), t._dirty = !0, r._dirty = !0, this;
  },
  /**
   * Intersection operation with another bounding box
   * @param  {clay.BoundingBox} bbox
   */
  intersection: function(e) {
    var t = this.min, r = this.max;
    return T.max(t.array, t.array, e.min.array), T.min(r.array, r.array, e.max.array), t._dirty = !0, r._dirty = !0, this;
  },
  /**
   * If intersect with another bounding box
   * @param  {clay.BoundingBox} bbox
   * @return {boolean}
   */
  intersectBoundingBox: function(e) {
    var t = this.min.array, r = this.max.array, i = e.min.array, n = e.max.array;
    return !(t[0] > n[0] || t[1] > n[1] || t[2] > n[2] || r[0] < i[0] || r[1] < i[1] || r[2] < i[2]);
  },
  /**
   * If contain another bounding box entirely
   * @param  {clay.BoundingBox} bbox
   * @return {boolean}
   */
  containBoundingBox: function(e) {
    var t = this.min.array, r = this.max.array, i = e.min.array, n = e.max.array;
    return t[0] <= i[0] && t[1] <= i[1] && t[2] <= i[2] && r[0] >= n[0] && r[1] >= n[1] && r[2] >= n[2];
  },
  /**
   * If contain point entirely
   * @param  {clay.Vector3} point
   * @return {boolean}
   */
  containPoint: function(e) {
    var t = this.min.array, r = this.max.array, i = e.array;
    return t[0] <= i[0] && t[1] <= i[1] && t[2] <= i[2] && r[0] >= i[0] && r[1] >= i[1] && r[2] >= i[2];
  },
  /**
   * If bounding box is finite
   */
  isFinite: function() {
    var e = this.min.array, t = this.max.array;
    return isFinite(e[0]) && isFinite(e[1]) && isFinite(e[2]) && isFinite(t[0]) && isFinite(t[1]) && isFinite(t[2]);
  },
  /**
   * Apply an affine transform matrix to the bounding box
   * @param  {clay.Matrix4} matrix
   */
  applyTransform: function(e) {
    this.transformFrom(this, e);
  },
  /**
   * Get from another bounding box and an affine transform matrix.
   * @param {clay.BoundingBox} source
   * @param {clay.Matrix4} matrix
   */
  transformFrom: (function() {
    var e = T.create(), t = T.create(), r = T.create(), i = T.create(), n = T.create(), a = T.create();
    return function(o, s) {
      var l = o.min.array, u = o.max.array, f = s.array;
      return e[0] = f[0] * l[0], e[1] = f[1] * l[0], e[2] = f[2] * l[0], t[0] = f[0] * u[0], t[1] = f[1] * u[0], t[2] = f[2] * u[0], r[0] = f[4] * l[1], r[1] = f[5] * l[1], r[2] = f[6] * l[1], i[0] = f[4] * u[1], i[1] = f[5] * u[1], i[2] = f[6] * u[1], n[0] = f[8] * l[2], n[1] = f[9] * l[2], n[2] = f[10] * l[2], a[0] = f[8] * u[2], a[1] = f[9] * u[2], a[2] = f[10] * u[2], l = this.min.array, u = this.max.array, l[0] = Math.min(e[0], t[0]) + Math.min(r[0], i[0]) + Math.min(n[0], a[0]) + f[12], l[1] = Math.min(e[1], t[1]) + Math.min(r[1], i[1]) + Math.min(n[1], a[1]) + f[13], l[2] = Math.min(e[2], t[2]) + Math.min(r[2], i[2]) + Math.min(n[2], a[2]) + f[14], u[0] = Math.max(e[0], t[0]) + Math.max(r[0], i[0]) + Math.max(n[0], a[0]) + f[12], u[1] = Math.max(e[1], t[1]) + Math.max(r[1], i[1]) + Math.max(n[1], a[1]) + f[13], u[2] = Math.max(e[2], t[2]) + Math.max(r[2], i[2]) + Math.max(n[2], a[2]) + f[14], this.min._dirty = !0, this.max._dirty = !0, this;
    };
  })(),
  /**
   * Apply a projection matrix to the bounding box
   * @param  {clay.Matrix4} matrix
   */
  applyProjection: function(e) {
    var t = this.min.array, r = this.max.array, i = e.array, n = t[0], a = t[1], o = t[2], s = r[0], l = r[1], u = t[2], f = r[0], h = r[1], d = r[2];
    if (i[15] === 1)
      t[0] = i[0] * n + i[12], t[1] = i[5] * a + i[13], r[2] = i[10] * o + i[14], r[0] = i[0] * f + i[12], r[1] = i[5] * h + i[13], t[2] = i[10] * d + i[14];
    else {
      var c = -1 / o;
      t[0] = i[0] * n * c, t[1] = i[5] * a * c, r[2] = (i[10] * o + i[14]) * c, c = -1 / u, r[0] = i[0] * s * c, r[1] = i[5] * l * c, c = -1 / d, t[2] = (i[10] * d + i[14]) * c;
    }
    return this.min._dirty = !0, this.max._dirty = !0, this;
  },
  updateVertices: function() {
    var e = this.vertices;
    if (!e) {
      e = [];
      for (var t = 0; t < 8; t++)
        e[t] = T.fromValues(0, 0, 0);
      this.vertices = e;
    }
    var r = this.min.array, i = this.max.array;
    return dt(e[0], r[0], r[1], r[2]), dt(e[1], r[0], i[1], r[2]), dt(e[2], i[0], r[1], r[2]), dt(e[3], i[0], i[1], r[2]), dt(e[4], r[0], r[1], i[2]), dt(e[5], r[0], i[1], i[2]), dt(e[6], i[0], r[1], i[2]), dt(e[7], i[0], i[1], i[2]), this;
  },
  /**
   * Copy values from another bounding box
   * @param  {clay.BoundingBox} bbox
   */
  copy: function(e) {
    var t = this.min, r = this.max;
    return Ir(t.array, e.min.array), Ir(r.array, e.max.array), t._dirty = !0, r._dirty = !0, this;
  },
  /**
   * Clone a new bounding box
   * @return {clay.BoundingBox}
   */
  clone: function() {
    var e = new we();
    return e.copy(this), e;
  }
};
var mu = 0, Je = Ae.extend(
  /** @lends clay.Node# */
  {
    /**
     * Scene node name
     * @type {string}
     */
    name: "",
    /**
     * Position relative to its parent node. aka translation.
     * @type {clay.Vector3}
     */
    position: null,
    /**
     * Rotation relative to its parent node. Represented by a quaternion
     * @type {clay.Quaternion}
     */
    rotation: null,
    /**
     * Scale relative to its parent node
     * @type {clay.Vector3}
     */
    scale: null,
    /**
     * Affine transform matrix relative to its root scene.
     * @type {clay.Matrix4}
     */
    worldTransform: null,
    /**
     * Affine transform matrix relative to its parent node.
     * Composited with position, rotation and scale.
     * @type {clay.Matrix4}
     */
    localTransform: null,
    /**
     * If the local transform is update from SRT(scale, rotation, translation, which is position here) each frame
     * @type {boolean}
     */
    autoUpdateLocalTransform: !0,
    /**
     * Parent of current scene node
     * @type {?clay.Node}
     * @private
     */
    _parent: null,
    /**
     * The root scene mounted. Null if it is a isolated node
     * @type {?clay.Scene}
     * @private
     */
    _scene: null,
    /**
     * @type {boolean}
     * @private
     */
    _needsUpdateWorldTransform: !0,
    /**
     * @type {boolean}
     * @private
     */
    _inIterating: !1,
    // Depth for transparent list sorting
    __depth: 0
  },
  function() {
    this.name || (this.name = (this.type || "NODE") + "_" + mu++), this.position || (this.position = new S()), this.rotation || (this.rotation = new ee()), this.scale || (this.scale = new S(1, 1, 1)), this.worldTransform = new I(), this.localTransform = new I(), this._children = [];
  },
  /**@lends clay.Node.prototype. */
  {
    /**
     * @type {?clay.Vector3}
     * @instance
     */
    target: null,
    /**
     * If node and its chilren invisible
     * @type {boolean}
     * @instance
     */
    invisible: !1,
    /**
     * If Node is a skinned mesh
     * @return {boolean}
     */
    isSkinnedMesh: function() {
      return !1;
    },
    /**
     * Return true if it is a renderable scene node, like Mesh and ParticleSystem
     * @return {boolean}
     */
    isRenderable: function() {
      return !1;
    },
    /**
     * Set the name of the scene node
     * @param {string} name
     */
    setName: function(e) {
      var t = this._scene;
      if (t) {
        var r = t._nodeRepository;
        delete r[this.name], r[e] = this;
      }
      this.name = e;
    },
    /**
     * Add a child node
     * @param {clay.Node} node
     */
    add: function(e) {
      var t = e._parent;
      if (t !== this) {
        t && t.remove(e), e._parent = this, this._children.push(e);
        var r = this._scene;
        r && r !== e.scene && e.traverse(this._addSelfToScene, this), e._needsUpdateWorldTransform = !0;
      }
    },
    /**
     * Remove the given child scene node
     * @param {clay.Node} node
     */
    remove: function(e) {
      var t = this._children, r = t.indexOf(e);
      r < 0 || (t.splice(r, 1), e._parent = null, this._scene && e.traverse(this._removeSelfFromScene, this));
    },
    /**
     * Remove all children
     */
    removeAll: function() {
      for (var e = this._children, t = 0; t < e.length; t++)
        e[t]._parent = null, this._scene && e[t].traverse(this._removeSelfFromScene, this);
      this._children = [];
    },
    /**
     * Get the scene mounted
     * @return {clay.Scene}
     */
    getScene: function() {
      return this._scene;
    },
    /**
     * Get parent node
     * @return {clay.Scene}
     */
    getParent: function() {
      return this._parent;
    },
    _removeSelfFromScene: function(e) {
      e._scene.removeFromScene(e), e._scene = null;
    },
    _addSelfToScene: function(e) {
      this._scene.addToScene(e), e._scene = this._scene;
    },
    /**
     * Return true if it is ancestor of the given scene node
     * @param {clay.Node} node
     */
    isAncestor: function(e) {
      for (var t = e._parent; t; ) {
        if (t === this)
          return !0;
        t = t._parent;
      }
      return !1;
    },
    /**
     * Get a new created array of all children nodes
     * @return {clay.Node[]}
     */
    children: function() {
      return this._children.slice();
    },
    /**
     * Get child scene node at given index.
     * @param {number} idx
     * @return {clay.Node}
     */
    childAt: function(e) {
      return this._children[e];
    },
    /**
     * Get first child with the given name
     * @param {string} name
     * @return {clay.Node}
     */
    getChildByName: function(e) {
      for (var t = this._children, r = 0; r < t.length; r++)
        if (t[r].name === e)
          return t[r];
    },
    /**
     * Get first descendant have the given name
     * @param {string} name
     * @return {clay.Node}
     */
    getDescendantByName: function(e) {
      for (var t = this._children, r = 0; r < t.length; r++) {
        var i = t[r];
        if (i.name === e)
          return i;
        var n = i.getDescendantByName(e);
        if (n)
          return n;
      }
    },
    /**
     * Query descendant node by path
     * @param {string} path
     * @return {clay.Node}
     * @example
     *  node.queryNode('root/parent/child');
     */
    queryNode: function(e) {
      if (e) {
        for (var t = e.split("/"), r = this, i = 0; i < t.length; i++) {
          var n = t[i];
          if (n) {
            for (var a = !1, o = r._children, s = 0; s < o.length; s++) {
              var l = o[s];
              if (l.name === n) {
                r = l, a = !0;
                break;
              }
            }
            if (!a)
              return;
          }
        }
        return r;
      }
    },
    /**
     * Get query path, relative to rootNode(default is scene)
     * @param {clay.Node} [rootNode]
     * @return {string}
     */
    getPath: function(e) {
      if (!this._parent)
        return "/";
      for (var t = this._parent, r = this.name; t._parent && (r = t.name + "/" + r, t._parent != e); )
        t = t._parent;
      return !t._parent && e ? null : r;
    },
    /**
     * Depth first traverse all its descendant scene nodes.
     *
     * **WARN** Don't do `add`, `remove` operation in the callback during traverse.
     * @param {Function} callback
     * @param {Node} [context]
     */
    traverse: function(e, t) {
      e.call(t, this);
      for (var r = this._children, i = 0, n = r.length; i < n; i++)
        r[i].traverse(e, t);
    },
    /**
     * Traverse all children nodes.
     *
     * **WARN** DON'T do `add`, `remove` operation in the callback during iteration.
     *
     * @param {Function} callback
     * @param {Node} [context]
     */
    eachChild: function(e, t) {
      for (var r = this._children, i = 0, n = r.length; i < n; i++) {
        var a = r[i];
        e.call(t, a, i);
      }
    },
    /**
     * Set the local transform and decompose to SRT
     * @param {clay.Matrix4} matrix
     */
    setLocalTransform: function(e) {
      D.copy(this.localTransform.array, e.array), this.decomposeLocalTransform();
    },
    /**
     * Decompose the local transform to SRT
     */
    decomposeLocalTransform: function(e) {
      var t = e ? null : this.scale;
      this.localTransform.decomposeMatrix(t, this.rotation, this.position);
    },
    /**
     * Set the world transform and decompose to SRT
     * @param {clay.Matrix4} matrix
     */
    setWorldTransform: function(e) {
      D.copy(this.worldTransform.array, e.array), this.decomposeWorldTransform();
    },
    /**
     * Decompose the world transform to SRT
     * @function
     */
    decomposeWorldTransform: (function() {
      var e = D.create();
      return function(t) {
        var r = this.localTransform, i = this.worldTransform;
        this._parent ? (D.invert(e, this._parent.worldTransform.array), D.multiply(r.array, e, i.array)) : D.copy(r.array, i.array);
        var n = t ? null : this.scale;
        r.decomposeMatrix(n, this.rotation, this.position);
      };
    })(),
    transformNeedsUpdate: function() {
      return this.position._dirty || this.rotation._dirty || this.scale._dirty;
    },
    /**
     * Update local transform from SRT
     * Notice that local transform will not be updated if _dirty mark of position, rotation, scale is all false
     */
    updateLocalTransform: function() {
      var e = this.position, t = this.rotation, r = this.scale;
      if (this.transformNeedsUpdate()) {
        var i = this.localTransform.array;
        D.fromRotationTranslation(i, t.array, e.array), D.scale(i, i, r.array), t._dirty = !1, r._dirty = !1, e._dirty = !1, this._needsUpdateWorldTransform = !0;
      }
    },
    /**
     * Update world transform, assume its parent world transform have been updated
     * @private
     */
    _updateWorldTransformTopDown: function() {
      var e = this.localTransform.array, t = this.worldTransform.array;
      this._parent ? D.multiplyAffine(
        t,
        this._parent.worldTransform.array,
        e
      ) : D.copy(t, e);
    },
    /**
     * Update world transform before whole scene is updated.
     */
    updateWorldTransform: function() {
      for (var e = this; e && e.getParent() && e.getParent().transformNeedsUpdate(); )
        e = e.getParent();
      e.update();
    },
    /**
     * Update local transform and world transform recursively
     * @param {boolean} forceUpdateWorld
     */
    update: function(e) {
      this.autoUpdateLocalTransform ? this.updateLocalTransform() : e = !0, (e || this._needsUpdateWorldTransform) && (this._updateWorldTransformTopDown(), e = !0, this._needsUpdateWorldTransform = !1);
      for (var t = this._children, r = 0, i = t.length; r < i; r++)
        t[r].update(e);
    },
    /**
     * Get bounding box of node
     * @param  {Function} [filter]
     * @param  {clay.BoundingBox} [out]
     * @return {clay.BoundingBox}
     */
    // TODO Skinning
    getBoundingBox: (function() {
      function e(n) {
        return !n.invisible && n.geometry;
      }
      var t = new we(), r = new I(), i = new I();
      return function(n, a) {
        return a = a || new we(), this._parent ? I.invert(i, this._parent.worldTransform) : I.identity(i), this.traverse(function(o) {
          o.geometry && o.geometry.boundingBox && (t.copy(o.geometry.boundingBox), I.multiply(r, i, o.worldTransform), t.applyTransform(r), a.union(t));
        }, this, e), a;
      };
    })(),
    /**
     * Get world position, extracted from world transform
     * @param  {clay.Vector3} [out]
     * @return {clay.Vector3}
     */
    getWorldPosition: function(e) {
      this.transformNeedsUpdate() && this.updateWorldTransform();
      var t = this.worldTransform.array;
      if (e) {
        var r = e.array;
        return r[0] = t[12], r[1] = t[13], r[2] = t[14], e;
      } else
        return new S(t[12], t[13], t[14]);
    },
    /**
     * Clone a new node
     * @return {Node}
     */
    clone: function() {
      var e = new this.constructor(), t = this._children;
      e.setName(this.name), e.position.copy(this.position), e.rotation.copy(this.rotation), e.scale.copy(this.scale);
      for (var r = 0; r < t.length; r++)
        e.add(t[r].clone());
      return e;
    },
    /**
     * Rotate the node around a axis by angle degrees, axis passes through point
     * @param {clay.Vector3} point Center point
     * @param {clay.Vector3} axis  Center axis
     * @param {number}       angle Rotation angle
     * @see http://docs.unity3d.com/Documentation/ScriptReference/Transform.RotateAround.html
     * @function
     */
    rotateAround: (function() {
      var e = new S(), t = new I();
      return function(r, i, n) {
        e.copy(this.position).subtract(r);
        var a = this.localTransform;
        a.identity(), a.translate(r), a.rotate(n, i), t.fromRotationTranslation(this.rotation, e), a.multiply(t), a.scale(this.scale), this.decomposeLocalTransform(), this._needsUpdateWorldTransform = !0;
      };
    })(),
    /**
     * @param {clay.Vector3} target
     * @param {clay.Vector3} [up]
     * @see http://www.opengl.org/sdk/docs/man2/xhtml/gluLookAt.xml
     * @function
     */
    lookAt: (function() {
      var e = new I();
      return function(t, r) {
        e.lookAt(this.position, t, r || this.localTransform.y).invert(), this.setLocalTransform(e), this.target = t;
      };
    })()
  }
);
const M = {
  /* ClearBufferMask */
  DEPTH_BUFFER_BIT: 256,
  STENCIL_BUFFER_BIT: 1024,
  COLOR_BUFFER_BIT: 16384,
  /* BeginMode */
  POINTS: 0,
  LINES: 1,
  LINE_LOOP: 2,
  LINE_STRIP: 3,
  TRIANGLES: 4,
  TRIANGLE_STRIP: 5,
  TRIANGLE_FAN: 6,
  STREAM_DRAW: 35040,
  STATIC_DRAW: 35044,
  DYNAMIC_DRAW: 35048,
  /* CullFaceMode */
  FRONT: 1028,
  BACK: 1029,
  FRONT_AND_BACK: 1032,
  /* FrontFaceDirection */
  CW: 2304,
  CCW: 2305,
  /* DataType */
  BYTE: 5120,
  UNSIGNED_BYTE: 5121,
  SHORT: 5122,
  UNSIGNED_SHORT: 5123,
  INT: 5124,
  UNSIGNED_INT: 5125,
  FLOAT: 5126,
  /* PixelFormat */
  DEPTH_COMPONENT: 6402,
  ALPHA: 6406,
  RGB: 6407,
  RGBA: 6408,
  LUMINANCE: 6409,
  LUMINANCE_ALPHA: 6410,
  /* TextureMagFilter */
  NEAREST: 9728,
  LINEAR: 9729,
  /* TextureMinFilter */
  /*      NEAREST */
  /*      LINEAR */
  NEAREST_MIPMAP_NEAREST: 9984,
  LINEAR_MIPMAP_NEAREST: 9985,
  NEAREST_MIPMAP_LINEAR: 9986,
  LINEAR_MIPMAP_LINEAR: 9987,
  /* TextureTarget */
  TEXTURE_2D: 3553,
  TEXTURE_CUBE_MAP: 34067,
  /* TextureWrapMode */
  REPEAT: 10497,
  CLAMP_TO_EDGE: 33071,
  MIRRORED_REPEAT: 33648,
  /* Framebuffer Object. */
  FRAMEBUFFER: 36160,
  RENDERBUFFER: 36161,
  DEPTH_STENCIL: 34041,
  COLOR_ATTACHMENT0: 36064,
  DEPTH_ATTACHMENT: 36096,
  STENCIL_ATTACHMENT: 36128,
  DEPTH_STENCIL_ATTACHMENT: 33306
};
var Re = Je.extend(
  /** @lends clay.Renderable# */
  {
    /**
     * @type {clay.Material}
     */
    material: null,
    /**
     * @type {clay.Geometry}
     */
    geometry: null,
    /**
     * @type {number}
     */
    mode: M.TRIANGLES,
    _renderInfo: null
  },
  /** @lends clay.Renderable.prototype */
  {
    __program: null,
    /**
     * Group of received light.
     */
    lightGroup: 0,
    /**
     * Render order, Nodes with smaller value renders before nodes with larger values.
     * @type {Number}
     */
    renderOrder: 0,
    /**
     * Used when mode is LINES, LINE_STRIP or LINE_LOOP
     * @type {number}
     */
    // lineWidth: 1,
    /**
     * If enable culling
     * @type {boolean}
     */
    culling: !0,
    /**
     * Specify which side of polygon will be culled.
     * Possible values:
     *  + {@link clay.Renderable.BACK}
     *  + {@link clay.Renderable.FRONT}
     *  + {@link clay.Renderable.FRONT_AND_BACK}
     * @see https://developer.mozilla.org/en-US/docs/Web/API/WebGLRenderingContext/cullFace
     * @type {number}
     */
    cullFace: M.BACK,
    /**
     * Specify which side is front face.
     * Possible values:
     *  + {@link clay.Renderable.CW}
     *  + {@link clay.Renderable.CCW}
     * @see https://developer.mozilla.org/en-US/docs/Web/API/WebGLRenderingContext/frontFace
     * @type {number}
     */
    frontFace: M.CCW,
    /**
     * If enable software frustum culling
     * @type {boolean}
     */
    frustumCulling: !0,
    /**
     * @type {boolean}
     */
    receiveShadow: !0,
    /**
     * @type {boolean}
     */
    castShadow: !0,
    /**
     * @type {boolean}
     */
    ignorePicking: !1,
    /**
     * @type {boolean}
     */
    ignorePreZ: !1,
    /**
     * @type {boolean}
     */
    ignoreGBuffer: !1,
    /**
     * @return {boolean}
     */
    isRenderable: function() {
      return this.geometry && this.material && this.material.shader && !this.invisible && this.geometry.vertexCount > 0;
    },
    /**
     * Before render hook
     * @type {Function}
     */
    beforeRender: function(e) {
    },
    /**
     * Before render hook
     * @type {Function}
     */
    afterRender: function(e, t) {
    },
    getBoundingBox: function(e, t) {
      return t = Je.prototype.getBoundingBox.call(this, e, t), this.geometry && this.geometry.boundingBox && t.union(this.geometry.boundingBox), t;
    },
    /**
     * Clone a new renderable
     * @function
     * @return {clay.Renderable}
     */
    clone: /* @__PURE__ */ (function() {
      var e = [
        "castShadow",
        "receiveShadow",
        "mode",
        "culling",
        "cullFace",
        "frontFace",
        "frustumCulling",
        "renderOrder",
        "lineWidth",
        "ignorePicking",
        "ignorePreZ",
        "ignoreGBuffer"
      ];
      return function() {
        var t = Je.prototype.clone.call(this);
        t.geometry = this.geometry, t.material = this.material;
        for (var r = 0; r < e.length; r++) {
          var i = e[r];
          t[i] !== this[i] && (t[i] = this[i]);
        }
        return t;
      };
    })()
  }
);
Re.POINTS = M.POINTS;
Re.LINES = M.LINES;
Re.LINE_LOOP = M.LINE_LOOP;
Re.LINE_STRIP = M.LINE_STRIP;
Re.TRIANGLES = M.TRIANGLES;
Re.TRIANGLE_STRIP = M.TRIANGLE_STRIP;
Re.TRIANGLE_FAN = M.TRIANGLE_FAN;
Re.BACK = M.BACK;
Re.FRONT = M.FRONT;
Re.FRONT_AND_BACK = M.FRONT_AND_BACK;
Re.CW = M.CW;
Re.CCW = M.CCW;
var Oe = Re.extend(
  /** @lends clay.Mesh# */
  {
    /**
     * Used when it is a skinned mesh
     * @type {clay.Skeleton}
     */
    skeleton: null,
    /**
     * Joints indices Meshes can share the one skeleton instance and each mesh can use one part of joints. Joints indices indicate the index of joint in the skeleton instance
     * @type {number[]}
     */
    joints: null
  },
  function() {
    this.joints || (this.joints = []);
  },
  {
    /**
     * Offset matrix used for multiple skinned mesh clone sharing one skeleton
     * @type {clay.Matrix4}
     */
    offsetMatrix: null,
    isInstancedMesh: function() {
      return !1;
    },
    isSkinnedMesh: function() {
      return !!(this.skeleton && this.joints && this.joints.length > 0);
    },
    clone: function() {
      var e = Re.prototype.clone.call(this);
      return e.skeleton = this.skeleton, this.joints && (e.joints = this.joints.slice()), e;
    }
  }
);
Oe.POINTS = M.POINTS;
Oe.LINES = M.LINES;
Oe.LINE_LOOP = M.LINE_LOOP;
Oe.LINE_STRIP = M.LINE_STRIP;
Oe.TRIANGLES = M.TRIANGLES;
Oe.TRIANGLE_STRIP = M.TRIANGLE_STRIP;
Oe.TRIANGLE_FAN = M.TRIANGLE_FAN;
Oe.BACK = M.BACK;
Oe.FRONT = M.FRONT;
Oe.FRONT_AND_BACK = M.FRONT_AND_BACK;
Oe.CW = M.CW;
Oe.CCW = M.CCW;
var fa = [
  "OES_texture_float",
  "OES_texture_half_float",
  "OES_texture_float_linear",
  "OES_texture_half_float_linear",
  "OES_standard_derivatives",
  "OES_vertex_array_object",
  "OES_element_index_uint",
  "WEBGL_compressed_texture_s3tc",
  "WEBGL_depth_texture",
  "EXT_texture_filter_anisotropic",
  "EXT_shader_texture_lod",
  "WEBGL_draw_buffers",
  "EXT_frag_depth",
  "EXT_sRGB",
  "ANGLE_instanced_arrays"
], ha = [
  "MAX_TEXTURE_SIZE",
  "MAX_CUBE_MAP_TEXTURE_SIZE"
];
function _u(e) {
  for (var t = {}, r = {}, i = 0; i < fa.length; i++) {
    var n = fa[i];
    o(n);
  }
  for (var i = 0; i < ha.length; i++) {
    var a = ha[i];
    r[a] = e.getParameter(e[a]);
  }
  this.getExtension = function(s) {
    return s in t || o(s), t[s];
  }, this.getParameter = function(s) {
    return r[s];
  };
  function o(s) {
    if (e.getExtension) {
      var l = e.getExtension(s);
      l || (l = e.getExtension("MOZ_" + s)), l || (l = e.getExtension("WEBKIT_" + s)), t[s] = l;
    }
  }
}
function gu(e) {
  var t = new XMLHttpRequest();
  t.open("get", e.url), t.responseType = e.responseType || "text", e.onprogress && (t.onprogress = function(r) {
    if (r.lengthComputable) {
      var i = r.loaded / r.total;
      e.onprogress(i, r.loaded, r.total);
    } else
      e.onprogress(null);
  }), t.onload = function(r) {
    t.status >= 400 ? e.onerror && e.onerror() : e.onload && e.onload(t.response);
  }, e.onerror && (t.onerror = e.onerror), t.send(null);
}
const yu = {
  get: gu
};
var Ni, Q = {};
Q.supportWebGL = function() {
  if (Ni == null)
    try {
      var e = document.createElement("canvas"), t = e.getContext("webgl") || e.getContext("experimental-webgl");
      if (!t)
        throw new Error();
    } catch {
      Ni = !1;
    }
  return Ni;
};
Q.Int8Array = typeof Int8Array > "u" ? Array : Int8Array;
Q.Uint8Array = typeof Uint8Array > "u" ? Array : Uint8Array;
Q.Uint16Array = typeof Uint16Array > "u" ? Array : Uint16Array;
Q.Uint32Array = typeof Uint32Array > "u" ? Array : Uint32Array;
Q.Int16Array = typeof Int16Array > "u" ? Array : Int16Array;
Q.Float32Array = typeof Float32Array > "u" ? Array : Float32Array;
Q.Float64Array = typeof Float64Array > "u" ? Array : Float64Array;
var Lt = {};
typeof window < "u" ? Lt = window : typeof global < "u" && (Lt = global);
Q.requestAnimationFrame = Lt.requestAnimationFrame || Lt.msRequestAnimationFrame || Lt.mozRequestAnimationFrame || Lt.webkitRequestAnimationFrame || function(e) {
  setTimeout(e, 16);
};
Q.createCanvas = function() {
  return document.createElement("canvas");
};
Q.createImage = function() {
  return new Lt.Image();
};
Q.request = {
  get: yu.get
};
Q.addEventListener = function(e, t, r, i) {
  e.addEventListener(t, r, i);
};
Q.removeEventListener = function(e, t, r) {
  e.removeEventListener(t, r);
};
var ge = function() {
  this.head = null, this.tail = null, this._length = 0;
};
ge.prototype.insert = function(e) {
  var t = new ge.Entry(e);
  return this.insertEntry(t), t;
};
ge.prototype.insertAt = function(e, t) {
  if (!(e < 0)) {
    for (var r = this.head, i = 0; r && i != e; )
      r = r.next, i++;
    if (r) {
      var n = new ge.Entry(t), a = r.prev;
      a ? (a.next = n, n.prev = a) : this.head = n, n.next = r, r.prev = n;
    } else
      this.insert(t);
  }
};
ge.prototype.insertBeforeEntry = function(e, t) {
  var r = new ge.Entry(e), i = t.prev;
  i ? (i.next = r, r.prev = i) : this.head = r, r.next = t, t.prev = r, this._length++;
};
ge.prototype.insertEntry = function(e) {
  this.head ? (this.tail.next = e, e.prev = this.tail, this.tail = e) : this.head = this.tail = e, this._length++;
};
ge.prototype.remove = function(e) {
  var t = e.prev, r = e.next;
  t ? t.next = r : this.head = r, r ? r.prev = t : this.tail = t, e.next = e.prev = null, this._length--;
};
ge.prototype.removeAt = function(e) {
  if (!(e < 0)) {
    for (var t = this.head, r = 0; t && r != e; )
      t = t.next, r++;
    if (t)
      return this.remove(t), t.value;
  }
};
ge.prototype.getHead = function() {
  if (this.head)
    return this.head.value;
};
ge.prototype.getTail = function() {
  if (this.tail)
    return this.tail.value;
};
ge.prototype.getAt = function(e) {
  if (!(e < 0)) {
    for (var t = this.head, r = 0; t && r != e; )
      t = t.next, r++;
    return t.value;
  }
};
ge.prototype.indexOf = function(e) {
  for (var t = this.head, r = 0; t; ) {
    if (t.value === e)
      return r;
    t = t.next, r++;
  }
};
ge.prototype.length = function() {
  return this._length;
};
ge.prototype.isEmpty = function() {
  return this._length === 0;
};
ge.prototype.forEach = function(e, t) {
  for (var r = this.head, i = 0, n = typeof t < "u"; r; )
    n ? e.call(t, r.value, i) : e(r.value, i), r = r.next, i++;
};
ge.prototype.clear = function() {
  this.tail = this.head = null, this._length = 0;
};
ge.Entry = function(e) {
  this.value = e, this.next = null, this.prev = null;
};
var Nt = function(e) {
  this._list = new ge(), this._map = {}, this._maxSize = e || 10;
};
Nt.prototype.setMaxSize = function(e) {
  this._maxSize = e;
};
Nt.prototype.put = function(e, t) {
  if (!this._map.hasOwnProperty(e)) {
    var r = this._list.length();
    if (r >= this._maxSize && r > 0) {
      var i = this._list.head;
      this._list.remove(i), delete this._map[i.key];
    }
    var n = this._list.insert(t);
    n.key = e, this._map[e] = n;
  }
};
Nt.prototype.get = function(e) {
  var t = this._map[e];
  if (this._map.hasOwnProperty(e))
    return t !== this._list.tail && (this._list.remove(t), this._list.insertEntry(t)), t.value;
};
Nt.prototype.remove = function(e) {
  var t = this._map[e];
  typeof t < "u" && (delete this._map[e], this._list.remove(t));
};
Nt.prototype.clear = function() {
  this._list.clear(), this._map = {};
};
var oe = {}, ca = {
  transparent: [0, 0, 0, 0],
  aliceblue: [240, 248, 255, 1],
  antiquewhite: [250, 235, 215, 1],
  aqua: [0, 255, 255, 1],
  aquamarine: [127, 255, 212, 1],
  azure: [240, 255, 255, 1],
  beige: [245, 245, 220, 1],
  bisque: [255, 228, 196, 1],
  black: [0, 0, 0, 1],
  blanchedalmond: [255, 235, 205, 1],
  blue: [0, 0, 255, 1],
  blueviolet: [138, 43, 226, 1],
  brown: [165, 42, 42, 1],
  burlywood: [222, 184, 135, 1],
  cadetblue: [95, 158, 160, 1],
  chartreuse: [127, 255, 0, 1],
  chocolate: [210, 105, 30, 1],
  coral: [255, 127, 80, 1],
  cornflowerblue: [100, 149, 237, 1],
  cornsilk: [255, 248, 220, 1],
  crimson: [220, 20, 60, 1],
  cyan: [0, 255, 255, 1],
  darkblue: [0, 0, 139, 1],
  darkcyan: [0, 139, 139, 1],
  darkgoldenrod: [184, 134, 11, 1],
  darkgray: [169, 169, 169, 1],
  darkgreen: [0, 100, 0, 1],
  darkgrey: [169, 169, 169, 1],
  darkkhaki: [189, 183, 107, 1],
  darkmagenta: [139, 0, 139, 1],
  darkolivegreen: [85, 107, 47, 1],
  darkorange: [255, 140, 0, 1],
  darkorchid: [153, 50, 204, 1],
  darkred: [139, 0, 0, 1],
  darksalmon: [233, 150, 122, 1],
  darkseagreen: [143, 188, 143, 1],
  darkslateblue: [72, 61, 139, 1],
  darkslategray: [47, 79, 79, 1],
  darkslategrey: [47, 79, 79, 1],
  darkturquoise: [0, 206, 209, 1],
  darkviolet: [148, 0, 211, 1],
  deeppink: [255, 20, 147, 1],
  deepskyblue: [0, 191, 255, 1],
  dimgray: [105, 105, 105, 1],
  dimgrey: [105, 105, 105, 1],
  dodgerblue: [30, 144, 255, 1],
  firebrick: [178, 34, 34, 1],
  floralwhite: [255, 250, 240, 1],
  forestgreen: [34, 139, 34, 1],
  fuchsia: [255, 0, 255, 1],
  gainsboro: [220, 220, 220, 1],
  ghostwhite: [248, 248, 255, 1],
  gold: [255, 215, 0, 1],
  goldenrod: [218, 165, 32, 1],
  gray: [128, 128, 128, 1],
  green: [0, 128, 0, 1],
  greenyellow: [173, 255, 47, 1],
  grey: [128, 128, 128, 1],
  honeydew: [240, 255, 240, 1],
  hotpink: [255, 105, 180, 1],
  indianred: [205, 92, 92, 1],
  indigo: [75, 0, 130, 1],
  ivory: [255, 255, 240, 1],
  khaki: [240, 230, 140, 1],
  lavender: [230, 230, 250, 1],
  lavenderblush: [255, 240, 245, 1],
  lawngreen: [124, 252, 0, 1],
  lemonchiffon: [255, 250, 205, 1],
  lightblue: [173, 216, 230, 1],
  lightcoral: [240, 128, 128, 1],
  lightcyan: [224, 255, 255, 1],
  lightgoldenrodyellow: [250, 250, 210, 1],
  lightgray: [211, 211, 211, 1],
  lightgreen: [144, 238, 144, 1],
  lightgrey: [211, 211, 211, 1],
  lightpink: [255, 182, 193, 1],
  lightsalmon: [255, 160, 122, 1],
  lightseagreen: [32, 178, 170, 1],
  lightskyblue: [135, 206, 250, 1],
  lightslategray: [119, 136, 153, 1],
  lightslategrey: [119, 136, 153, 1],
  lightsteelblue: [176, 196, 222, 1],
  lightyellow: [255, 255, 224, 1],
  lime: [0, 255, 0, 1],
  limegreen: [50, 205, 50, 1],
  linen: [250, 240, 230, 1],
  magenta: [255, 0, 255, 1],
  maroon: [128, 0, 0, 1],
  mediumaquamarine: [102, 205, 170, 1],
  mediumblue: [0, 0, 205, 1],
  mediumorchid: [186, 85, 211, 1],
  mediumpurple: [147, 112, 219, 1],
  mediumseagreen: [60, 179, 113, 1],
  mediumslateblue: [123, 104, 238, 1],
  mediumspringgreen: [0, 250, 154, 1],
  mediumturquoise: [72, 209, 204, 1],
  mediumvioletred: [199, 21, 133, 1],
  midnightblue: [25, 25, 112, 1],
  mintcream: [245, 255, 250, 1],
  mistyrose: [255, 228, 225, 1],
  moccasin: [255, 228, 181, 1],
  navajowhite: [255, 222, 173, 1],
  navy: [0, 0, 128, 1],
  oldlace: [253, 245, 230, 1],
  olive: [128, 128, 0, 1],
  olivedrab: [107, 142, 35, 1],
  orange: [255, 165, 0, 1],
  orangered: [255, 69, 0, 1],
  orchid: [218, 112, 214, 1],
  palegoldenrod: [238, 232, 170, 1],
  palegreen: [152, 251, 152, 1],
  paleturquoise: [175, 238, 238, 1],
  palevioletred: [219, 112, 147, 1],
  papayawhip: [255, 239, 213, 1],
  peachpuff: [255, 218, 185, 1],
  peru: [205, 133, 63, 1],
  pink: [255, 192, 203, 1],
  plum: [221, 160, 221, 1],
  powderblue: [176, 224, 230, 1],
  purple: [128, 0, 128, 1],
  red: [255, 0, 0, 1],
  rosybrown: [188, 143, 143, 1],
  royalblue: [65, 105, 225, 1],
  saddlebrown: [139, 69, 19, 1],
  salmon: [250, 128, 114, 1],
  sandybrown: [244, 164, 96, 1],
  seagreen: [46, 139, 87, 1],
  seashell: [255, 245, 238, 1],
  sienna: [160, 82, 45, 1],
  silver: [192, 192, 192, 1],
  skyblue: [135, 206, 235, 1],
  slateblue: [106, 90, 205, 1],
  slategray: [112, 128, 144, 1],
  slategrey: [112, 128, 144, 1],
  snow: [255, 250, 250, 1],
  springgreen: [0, 255, 127, 1],
  steelblue: [70, 130, 180, 1],
  tan: [210, 180, 140, 1],
  teal: [0, 128, 128, 1],
  thistle: [216, 191, 216, 1],
  tomato: [255, 99, 71, 1],
  turquoise: [64, 224, 208, 1],
  violet: [238, 130, 238, 1],
  wheat: [245, 222, 179, 1],
  white: [255, 255, 255, 1],
  whitesmoke: [245, 245, 245, 1],
  yellow: [255, 255, 0, 1],
  yellowgreen: [154, 205, 50, 1]
};
function je(e) {
  return e = Math.round(e), e < 0 ? 0 : e > 255 ? 255 : e;
}
function xu(e) {
  return e = Math.round(e), e < 0 ? 0 : e > 360 ? 360 : e;
}
function xr(e) {
  return e < 0 ? 0 : e > 1 ? 1 : e;
}
function Oi(e) {
  return e.length && e.charAt(e.length - 1) === "%" ? je(parseFloat(e) / 100 * 255) : je(parseInt(e, 10));
}
function jt(e) {
  return e.length && e.charAt(e.length - 1) === "%" ? xr(parseFloat(e) / 100) : xr(parseFloat(e));
}
function Ii(e, t, r) {
  return r < 0 ? r += 1 : r > 1 && (r -= 1), r * 6 < 1 ? e + (t - e) * r * 6 : r * 2 < 1 ? t : r * 3 < 2 ? e + (t - e) * (2 / 3 - r) * 6 : e;
}
function pt(e, t, r) {
  return e + (t - e) * r;
}
function We(e, t, r, i, n) {
  return e[0] = t, e[1] = r, e[2] = i, e[3] = n, e;
}
function dn(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e;
}
var Lo = new Nt(20), Br = null;
function Bt(e, t) {
  Br && dn(Br, t), Br = Lo.put(e, Br || t.slice());
}
oe.parse = function(e, t) {
  if (e) {
    t = t || [];
    var r = Lo.get(e);
    if (r)
      return dn(t, r);
    e = e + "";
    var i = e.replace(/ /g, "").toLowerCase();
    if (i in ca)
      return dn(t, ca[i]), Bt(e, t), t;
    if (i.charAt(0) === "#") {
      if (i.length === 4) {
        var n = parseInt(i.substr(1), 16);
        if (!(n >= 0 && n <= 4095)) {
          We(t, 0, 0, 0, 1);
          return;
        }
        return We(
          t,
          (n & 3840) >> 4 | (n & 3840) >> 8,
          n & 240 | (n & 240) >> 4,
          n & 15 | (n & 15) << 4,
          1
        ), Bt(e, t), t;
      } else if (i.length === 7) {
        var n = parseInt(i.substr(1), 16);
        if (!(n >= 0 && n <= 16777215)) {
          We(t, 0, 0, 0, 1);
          return;
        }
        return We(
          t,
          (n & 16711680) >> 16,
          (n & 65280) >> 8,
          n & 255,
          1
        ), Bt(e, t), t;
      }
      return;
    }
    var a = i.indexOf("("), o = i.indexOf(")");
    if (a !== -1 && o + 1 === i.length) {
      var s = i.substr(0, a), l = i.substr(a + 1, o - (a + 1)).split(","), u = 1;
      switch (s) {
        case "rgba":
          if (l.length !== 4) {
            We(t, 0, 0, 0, 1);
            return;
          }
          u = jt(l.pop());
        // jshint ignore:line
        // Fall through.
        case "rgb":
          if (l.length !== 3) {
            We(t, 0, 0, 0, 1);
            return;
          }
          return We(
            t,
            Oi(l[0]),
            Oi(l[1]),
            Oi(l[2]),
            u
          ), Bt(e, t), t;
        case "hsla":
          if (l.length !== 4) {
            We(t, 0, 0, 0, 1);
            return;
          }
          return l[3] = jt(l[3]), vn(l, t), Bt(e, t), t;
        case "hsl":
          if (l.length !== 3) {
            We(t, 0, 0, 0, 1);
            return;
          }
          return vn(l, t), Bt(e, t), t;
        default:
          return;
      }
    }
    We(t, 0, 0, 0, 1);
  }
};
oe.parseToFloat = function(e, t) {
  if (t = oe.parse(e, t), !!t)
    return t[0] /= 255, t[1] /= 255, t[2] /= 255, t;
};
function vn(e, t) {
  var r = (parseFloat(e[0]) % 360 + 360) % 360 / 360, i = jt(e[1]), n = jt(e[2]), a = n <= 0.5 ? n * (i + 1) : n + i - n * i, o = n * 2 - a;
  return t = t || [], We(
    t,
    je(Ii(o, a, r + 1 / 3) * 255),
    je(Ii(o, a, r) * 255),
    je(Ii(o, a, r - 1 / 3) * 255),
    1
  ), e.length === 4 && (t[3] = e[3]), t;
}
function Tu(e) {
  if (e) {
    var t = e[0] / 255, r = e[1] / 255, i = e[2] / 255, n = Math.min(t, r, i), a = Math.max(t, r, i), o = a - n, s = (a + n) / 2, l, u;
    if (o === 0)
      l = 0, u = 0;
    else {
      s < 0.5 ? u = o / (a + n) : u = o / (2 - a - n);
      var f = ((a - t) / 6 + o / 2) / o, h = ((a - r) / 6 + o / 2) / o, d = ((a - i) / 6 + o / 2) / o;
      t === a ? l = d - h : r === a ? l = 1 / 3 + f - d : i === a && (l = 2 / 3 + h - f), l < 0 && (l += 1), l > 1 && (l -= 1);
    }
    var c = [l * 360, u, s];
    return e[3] != null && c.push(e[3]), c;
  }
}
oe.lift = function(e, t) {
  var r = oe.parse(e);
  if (r) {
    for (var i = 0; i < 3; i++)
      t < 0 ? r[i] = r[i] * (1 - t) | 0 : r[i] = (255 - r[i]) * t + r[i] | 0;
    return oe.stringify(r, r.length === 4 ? "rgba" : "rgb");
  }
};
oe.toHex = function(e) {
  var t = oe.parse(e);
  if (t)
    return ((1 << 24) + (t[0] << 16) + (t[1] << 8) + +t[2]).toString(16).slice(1);
};
oe.fastLerp = function(e, t, r) {
  if (!(!(t && t.length) || !(e >= 0 && e <= 1))) {
    r = r || [];
    var i = e * (t.length - 1), n = Math.floor(i), a = Math.ceil(i), o = t[n], s = t[a], l = i - n;
    return r[0] = je(pt(o[0], s[0], l)), r[1] = je(pt(o[1], s[1], l)), r[2] = je(pt(o[2], s[2], l)), r[3] = xr(pt(o[3], s[3], l)), r;
  }
};
oe.fastMapToColor = oe.fastLerp;
oe.lerp = function(e, t, r) {
  if (!(!(t && t.length) || !(e >= 0 && e <= 1))) {
    var i = e * (t.length - 1), n = Math.floor(i), a = Math.ceil(i), o = oe.parse(t[n]), s = oe.parse(t[a]), l = i - n, u = oe.stringify(
      [
        je(pt(o[0], s[0], l)),
        je(pt(o[1], s[1], l)),
        je(pt(o[2], s[2], l)),
        xr(pt(o[3], s[3], l))
      ],
      "rgba"
    );
    return r ? {
      color: u,
      leftIndex: n,
      rightIndex: a,
      value: i
    } : u;
  }
};
oe.mapToColor = oe.lerp;
oe.modifyHSL = function(e, t, r, i) {
  if (e = oe.parse(e), e)
    return e = Tu(e), t != null && (e[0] = xu(t)), r != null && (e[1] = jt(r)), i != null && (e[2] = jt(i)), oe.stringify(vn(e), "rgba");
};
oe.modifyAlpha = function(e, t) {
  if (e = oe.parse(e), e && t != null)
    return e[3] = xr(t), oe.stringify(e, "rgba");
};
oe.stringify = function(e, t) {
  if (!(!e || !e.length)) {
    var r = e[0] + "," + e[1] + "," + e[2];
    return (t === "rgba" || t === "hsva" || t === "hsla") && (r += "," + e[3]), t + "(" + r + ")";
  }
};
var wu = oe.parseToFloat, Bi = {};
function da(e) {
  var t = Object.keys(e);
  t.sort();
  for (var r = [], i = 0; i < t.length; i++) {
    var n = t[i], a = e[n];
    a === null ? r.push(n) : r.push(n + " " + a.toString());
  }
  return r.join(`
`);
}
function bu(e, t, r) {
  r.sort();
  for (var i = [], n = 0; n < r.length; n++) {
    var a = r[n];
    i.push(a);
  }
  var o = da(e) + `
` + da(t) + `
` + i.join(`
`);
  if (Bi[o])
    return Bi[o];
  var s = he.genGUID();
  return Bi[o] = s, s;
}
var ft = Ae.extend(
  function() {
    return (
      /** @lends clay.Material# */
      {
        /**
         * @type {string}
         */
        name: "",
        /**
         * @type {Object}
         */
        // uniforms: null,
        /**
         * @type {clay.Shader}
         */
        // shader: null,
        /**
         * @type {boolean}
         */
        depthTest: !0,
        /**
         * @type {boolean}
         */
        depthMask: !0,
        /**
         * @type {boolean}
         */
        transparent: !1,
        /**
         * Blend func is a callback function when the material
         * have custom blending
         * The gl context will be the only argument passed in tho the
         * blend function
         * Detail of blend function in WebGL:
         * http://www.khronos.org/registry/gles/specs/2.0/es_full_spec_2.0.25.pdf
         *
         * Example :
         * function(_gl) {
         *  _gl.blendEquation(_gl.FUNC_ADD);
         *  _gl.blendFunc(_gl.SRC_ALPHA, _gl.ONE_MINUS_SRC_ALPHA);
         * }
         */
        blend: null,
        /**
         * If update texture status automatically.
         */
        autoUpdateTextureStatus: !0,
        uniforms: {},
        vertexDefines: {},
        fragmentDefines: {},
        _textureStatus: {},
        // shadowTransparentMap : null
        // PENDING enable the uniform that only used in shader.
        _enabledUniforms: null
      }
    );
  },
  function() {
    this.name || (this.name = "MATERIAL_" + this.__uid__), this.shader && this.attachShader(this.shader, !0);
  },
  /** @lends clay.Material.prototype */
  {
    precision: "highp",
    /**
     * Set material uniform
     * @example
     *  mat.setUniform('color', [1, 1, 1, 1]);
     * @param {string} symbol
     * @param {number|array|clay.Texture|ArrayBufferView} value
     */
    setUniform: function(e, t) {
      t === void 0 && console.warn('Uniform value "' + e + '" is undefined');
      var r = this.uniforms[e];
      r && (typeof t == "string" && (t = wu(t) || t), r.value = t, this.autoUpdateTextureStatus && r.type === "t" && (t ? this.enableTexture(e) : this.disableTexture(e)));
    },
    /**
     * @param {Object} obj
     */
    setUniforms: function(e) {
      for (var t in e) {
        var r = e[t];
        this.setUniform(t, r);
      }
    },
    /**
     * @param  {string}  symbol
     * @return {boolean}
     */
    isUniformEnabled: function(e) {
      return this._enabledUniforms.indexOf(e) >= 0;
    },
    getEnabledUniforms: function() {
      return this._enabledUniforms;
    },
    getTextureUniforms: function() {
      return this._textureUniforms;
    },
    /**
     * Alias of setUniform and setUniforms
     * @param {object|string} symbol
     * @param {number|array|clay.Texture|ArrayBufferView} [value]
     */
    set: function(e, t) {
      if (typeof e == "object")
        for (var r in e) {
          var i = e[r];
          this.setUniform(r, i);
        }
      else
        this.setUniform(e, t);
    },
    /**
     * Get uniform value
     * @param  {string} symbol
     * @return {number|array|clay.Texture|ArrayBufferView}
     */
    get: function(e) {
      var t = this.uniforms[e];
      if (t)
        return t.value;
    },
    /**
     * Attach a shader instance
     * @param  {clay.Shader} shader
     * @param  {boolean} keepStatus If try to keep uniform and texture
     */
    attachShader: function(e, t) {
      var r = this.uniforms;
      this.uniforms = e.createUniforms(), this.shader = e;
      var i = this.uniforms;
      this._enabledUniforms = Object.keys(i), this._enabledUniforms.sort(), this._textureUniforms = this._enabledUniforms.filter(function(u) {
        var f = this.uniforms[u].type;
        return f === "t" || f === "tv";
      }, this);
      var n = this.vertexDefines, a = this.fragmentDefines;
      if (this.vertexDefines = he.clone(e.vertexDefines), this.fragmentDefines = he.clone(e.fragmentDefines), t) {
        for (var o in r)
          i[o] && (i[o].value = r[o].value);
        he.defaults(this.vertexDefines, n), he.defaults(this.fragmentDefines, a);
      }
      var s = {};
      for (var l in e.textures)
        s[l] = {
          shaderType: e.textures[l].shaderType,
          type: e.textures[l].type,
          enabled: t && this._textureStatus[l] ? this._textureStatus[l].enabled : !1
        };
      this._textureStatus = s, this._programKey = "";
    },
    /**
     * Clone a new material and keep uniforms, shader will not be cloned
     * @return {clay.Material}
     */
    clone: function() {
      var e = new this.constructor({
        name: this.name,
        shader: this.shader
      });
      for (var t in this.uniforms)
        e.uniforms[t].value = this.uniforms[t].value;
      return e.depthTest = this.depthTest, e.depthMask = this.depthMask, e.transparent = this.transparent, e.blend = this.blend, e.vertexDefines = he.clone(this.vertexDefines), e.fragmentDefines = he.clone(this.fragmentDefines), e.enableTexture(this.getEnabledTextures()), e.precision = this.precision, e;
    },
    /**
     * Add a #define macro in shader code
     * @param  {string} shaderType Can be vertex, fragment or both
     * @param  {string} symbol
     * @param  {number} [val]
     */
    define: function(e, t, r) {
      var i = this.vertexDefines, n = this.fragmentDefines;
      e !== "vertex" && e !== "fragment" && e !== "both" && arguments.length < 3 && (r = t, t = e, e = "both"), r = r ?? null, (e === "vertex" || e === "both") && i[t] !== r && (i[t] = r, this._programKey = ""), (e === "fragment" || e === "both") && n[t] !== r && (n[t] = r, e !== "both" && (this._programKey = ""));
    },
    /**
     * Remove a #define macro in shader code
     * @param  {string} shaderType Can be vertex, fragment or both
     * @param  {string} symbol
     */
    undefine: function(e, t) {
      e !== "vertex" && e !== "fragment" && e !== "both" && arguments.length < 2 && (t = e, e = "both"), (e === "vertex" || e === "both") && this.isDefined("vertex", t) && (delete this.vertexDefines[t], this._programKey = ""), (e === "fragment" || e === "both") && this.isDefined("fragment", t) && (delete this.fragmentDefines[t], e !== "both" && (this._programKey = ""));
    },
    /**
     * If macro is defined in shader.
     * @param  {string} shaderType Can be vertex, fragment or both
     * @param  {string} symbol
     */
    isDefined: function(e, t) {
      switch (e) {
        case "vertex":
          return this.vertexDefines[t] !== void 0;
        case "fragment":
          return this.fragmentDefines[t] !== void 0;
      }
    },
    /**
     * Get macro value defined in shader.
     * @param  {string} shaderType Can be vertex, fragment or both
     * @param  {string} symbol
     */
    getDefine: function(e, t) {
      switch (e) {
        case "vertex":
          return this.vertexDefines[t];
        case "fragment":
          return this.fragmentDefines[t];
      }
    },
    /**
     * Enable a texture, actually it will add a #define macro in the shader code
     * For example, if texture symbol is diffuseMap, it will add a line `#define DIFFUSEMAP_ENABLED` in the shader code
     * @param  {string} symbol
     */
    enableTexture: function(e) {
      if (Array.isArray(e)) {
        for (var t = 0; t < e.length; t++)
          this.enableTexture(e[t]);
        return;
      }
      var r = this._textureStatus[e];
      if (r) {
        var i = r.enabled;
        i || (r.enabled = !0, this._programKey = "");
      }
    },
    /**
     * Enable all textures used in the shader
     */
    enableTexturesAll: function() {
      var e = this._textureStatus;
      for (var t in e)
        e[t].enabled = !0;
      this._programKey = "";
    },
    /**
     * Disable a texture, it remove a #define macro in the shader
     * @param  {string} symbol
     */
    disableTexture: function(e) {
      if (Array.isArray(e)) {
        for (var t = 0; t < e.length; t++)
          this.disableTexture(e[t]);
        return;
      }
      var r = this._textureStatus[e];
      if (r) {
        var i = !r.enabled;
        i || (r.enabled = !1, this._programKey = "");
      }
    },
    /**
     * Disable all textures used in the shader
     */
    disableTexturesAll: function() {
      var e = this._textureStatus;
      for (var t in e)
        e[t].enabled = !1;
      this._programKey = "";
    },
    /**
     * If texture of given type is enabled.
     * @param  {string}  symbol
     * @return {boolean}
     */
    isTextureEnabled: function(e) {
      var t = this._textureStatus;
      return !!t[e] && t[e].enabled;
    },
    /**
     * Get all enabled textures
     * @return {string[]}
     */
    getEnabledTextures: function() {
      var e = [], t = this._textureStatus;
      for (var r in t)
        t[r].enabled && e.push(r);
      return e;
    },
    /**
     * Mark defines are updated.
     */
    dirtyDefines: function() {
      this._programKey = "";
    },
    getProgramKey: function() {
      return this._programKey || (this._programKey = bu(
        this.vertexDefines,
        this.fragmentDefines,
        this.getEnabledTextures()
      )), this._programKey;
    }
  }
), P = {};
P.create = function() {
  var e = new Ee(2);
  return e[0] = 0, e[1] = 0, e;
};
P.clone = function(e) {
  var t = new Ee(2);
  return t[0] = e[0], t[1] = e[1], t;
};
P.fromValues = function(e, t) {
  var r = new Ee(2);
  return r[0] = e, r[1] = t, r;
};
P.copy = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e;
};
P.set = function(e, t, r) {
  return e[0] = t, e[1] = r, e;
};
P.add = function(e, t, r) {
  return e[0] = t[0] + r[0], e[1] = t[1] + r[1], e;
};
P.subtract = function(e, t, r) {
  return e[0] = t[0] - r[0], e[1] = t[1] - r[1], e;
};
P.sub = P.subtract;
P.multiply = function(e, t, r) {
  return e[0] = t[0] * r[0], e[1] = t[1] * r[1], e;
};
P.mul = P.multiply;
P.divide = function(e, t, r) {
  return e[0] = t[0] / r[0], e[1] = t[1] / r[1], e;
};
P.div = P.divide;
P.min = function(e, t, r) {
  return e[0] = Math.min(t[0], r[0]), e[1] = Math.min(t[1], r[1]), e;
};
P.max = function(e, t, r) {
  return e[0] = Math.max(t[0], r[0]), e[1] = Math.max(t[1], r[1]), e;
};
P.scale = function(e, t, r) {
  return e[0] = t[0] * r, e[1] = t[1] * r, e;
};
P.scaleAndAdd = function(e, t, r, i) {
  return e[0] = t[0] + r[0] * i, e[1] = t[1] + r[1] * i, e;
};
P.distance = function(e, t) {
  var r = t[0] - e[0], i = t[1] - e[1];
  return Math.sqrt(r * r + i * i);
};
P.dist = P.distance;
P.squaredDistance = function(e, t) {
  var r = t[0] - e[0], i = t[1] - e[1];
  return r * r + i * i;
};
P.sqrDist = P.squaredDistance;
P.length = function(e) {
  var t = e[0], r = e[1];
  return Math.sqrt(t * t + r * r);
};
P.len = P.length;
P.squaredLength = function(e) {
  var t = e[0], r = e[1];
  return t * t + r * r;
};
P.sqrLen = P.squaredLength;
P.negate = function(e, t) {
  return e[0] = -t[0], e[1] = -t[1], e;
};
P.inverse = function(e, t) {
  return e[0] = 1 / t[0], e[1] = 1 / t[1], e;
};
P.normalize = function(e, t) {
  var r = t[0], i = t[1], n = r * r + i * i;
  return n > 0 && (n = 1 / Math.sqrt(n), e[0] = t[0] * n, e[1] = t[1] * n), e;
};
P.dot = function(e, t) {
  return e[0] * t[0] + e[1] * t[1];
};
P.cross = function(e, t, r) {
  var i = t[0] * r[1] - t[1] * r[0];
  return e[0] = e[1] = 0, e[2] = i, e;
};
P.lerp = function(e, t, r, i) {
  var n = t[0], a = t[1];
  return e[0] = n + i * (r[0] - n), e[1] = a + i * (r[1] - a), e;
};
P.random = function(e, t) {
  t = t || 1;
  var r = GLMAT_RANDOM() * 2 * Math.PI;
  return e[0] = Math.cos(r) * t, e[1] = Math.sin(r) * t, e;
};
P.transformMat2 = function(e, t, r) {
  var i = t[0], n = t[1];
  return e[0] = r[0] * i + r[2] * n, e[1] = r[1] * i + r[3] * n, e;
};
P.transformMat2d = function(e, t, r) {
  var i = t[0], n = t[1];
  return e[0] = r[0] * i + r[2] * n + r[4], e[1] = r[1] * i + r[3] * n + r[5], e;
};
P.transformMat3 = function(e, t, r) {
  var i = t[0], n = t[1];
  return e[0] = r[0] * i + r[3] * n + r[6], e[1] = r[1] * i + r[4] * n + r[7], e;
};
P.transformMat4 = function(e, t, r) {
  var i = t[0], n = t[1];
  return e[0] = r[0] * i + r[4] * n + r[12], e[1] = r[1] * i + r[5] * n + r[13], e;
};
P.forEach = (function() {
  var e = P.create();
  return function(t, r, i, n, a, o) {
    var s, l;
    for (r || (r = 2), i || (i = 0), n ? l = Math.min(n * r + i, t.length) : l = t.length, s = i; s < l; s += r)
      e[0] = t[s], e[1] = t[s + 1], a(e, e, o), t[s] = e[0], t[s + 1] = e[1];
    return t;
  };
})();
var k = function(e, t) {
  e = e || 0, t = t || 0, this.array = P.fromValues(e, t), this._dirty = !0;
};
k.prototype = {
  constructor: k,
  /**
   * Add b to self
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  add: function(e) {
    return P.add(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Set x and y components
   * @param  {number}  x
   * @param  {number}  y
   * @return {clay.Vector2}
   */
  set: function(e, t) {
    return this.array[0] = e, this.array[1] = t, this._dirty = !0, this;
  },
  /**
   * Set x and y components from array
   * @param  {Float32Array|number[]} arr
   * @return {clay.Vector2}
   */
  setArray: function(e) {
    return this.array[0] = e[0], this.array[1] = e[1], this._dirty = !0, this;
  },
  /**
   * Clone a new Vector2
   * @return {clay.Vector2}
   */
  clone: function() {
    return new k(this.x, this.y);
  },
  /**
   * Copy x, y from b
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  copy: function(e) {
    return P.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Cross product of self and b, written to a Vector3 out
   * @param  {clay.Vector3} out
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  cross: function(e, t) {
    return P.cross(e.array, this.array, t.array), e._dirty = !0, this;
  },
  /**
   * Alias for distance
   * @param  {clay.Vector2} b
   * @return {number}
   */
  dist: function(e) {
    return P.dist(this.array, e.array);
  },
  /**
   * Distance between self and b
   * @param  {clay.Vector2} b
   * @return {number}
   */
  distance: function(e) {
    return P.distance(this.array, e.array);
  },
  /**
   * Alias for divide
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  div: function(e) {
    return P.div(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Divide self by b
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  divide: function(e) {
    return P.divide(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Dot product of self and b
   * @param  {clay.Vector2} b
   * @return {number}
   */
  dot: function(e) {
    return P.dot(this.array, e.array);
  },
  /**
   * Alias of length
   * @return {number}
   */
  len: function() {
    return P.len(this.array);
  },
  /**
   * Calculate the length
   * @return {number}
   */
  length: function() {
    return P.length(this.array);
  },
  /**
   * Linear interpolation between a and b
   * @param  {clay.Vector2} a
   * @param  {clay.Vector2} b
   * @param  {number}  t
   * @return {clay.Vector2}
   */
  lerp: function(e, t, r) {
    return P.lerp(this.array, e.array, t.array, r), this._dirty = !0, this;
  },
  /**
   * Minimum of self and b
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  min: function(e) {
    return P.min(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Maximum of self and b
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  max: function(e) {
    return P.max(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiply
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  mul: function(e) {
    return P.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Mutiply self and b
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  multiply: function(e) {
    return P.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Negate self
   * @return {clay.Vector2}
   */
  negate: function() {
    return P.negate(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Normalize self
   * @return {clay.Vector2}
   */
  normalize: function() {
    return P.normalize(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Generate random x, y components with a given scale
   * @param  {number} scale
   * @return {clay.Vector2}
   */
  random: function(e) {
    return P.random(this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale self
   * @param  {number}  scale
   * @return {clay.Vector2}
   */
  scale: function(e) {
    return P.scale(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale b and add to self
   * @param  {clay.Vector2} b
   * @param  {number}  scale
   * @return {clay.Vector2}
   */
  scaleAndAdd: function(e, t) {
    return P.scaleAndAdd(this.array, this.array, e.array, t), this._dirty = !0, this;
  },
  /**
   * Alias for squaredDistance
   * @param  {clay.Vector2} b
   * @return {number}
   */
  sqrDist: function(e) {
    return P.sqrDist(this.array, e.array);
  },
  /**
   * Squared distance between self and b
   * @param  {clay.Vector2} b
   * @return {number}
   */
  squaredDistance: function(e) {
    return P.squaredDistance(this.array, e.array);
  },
  /**
   * Alias for squaredLength
   * @return {number}
   */
  sqrLen: function() {
    return P.sqrLen(this.array);
  },
  /**
   * Squared length of self
   * @return {number}
   */
  squaredLength: function() {
    return P.squaredLength(this.array);
  },
  /**
   * Alias for subtract
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  sub: function(e) {
    return P.sub(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Subtract b from self
   * @param  {clay.Vector2} b
   * @return {clay.Vector2}
   */
  subtract: function(e) {
    return P.subtract(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Matrix2 m
   * @param  {clay.Matrix2} m
   * @return {clay.Vector2}
   */
  transformMat2: function(e) {
    return P.transformMat2(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Matrix2d m
   * @param  {clay.Matrix2d} m
   * @return {clay.Vector2}
   */
  transformMat2d: function(e) {
    return P.transformMat2d(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Matrix3 m
   * @param  {clay.Matrix3} m
   * @return {clay.Vector2}
   */
  transformMat3: function(e) {
    return P.transformMat3(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Matrix4 m
   * @param  {clay.Matrix4} m
   * @return {clay.Vector2}
   */
  transformMat4: function(e) {
    return P.transformMat4(this.array, this.array, e.array), this._dirty = !0, this;
  },
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
if (Object.defineProperty) {
  var va = k.prototype;
  Object.defineProperty(va, "x", {
    get: function() {
      return this.array[0];
    },
    set: function(e) {
      this.array[0] = e, this._dirty = !0;
    }
  }), Object.defineProperty(va, "y", {
    get: function() {
      return this.array[1];
    },
    set: function(e) {
      this.array[1] = e, this._dirty = !0;
    }
  });
}
k.add = function(e, t, r) {
  return P.add(e.array, t.array, r.array), e._dirty = !0, e;
};
k.set = function(e, t, r) {
  return P.set(e.array, t, r), e._dirty = !0, e;
};
k.copy = function(e, t) {
  return P.copy(e.array, t.array), e._dirty = !0, e;
};
k.cross = function(e, t, r) {
  return P.cross(e.array, t.array, r.array), e._dirty = !0, e;
};
k.dist = function(e, t) {
  return P.distance(e.array, t.array);
};
k.distance = k.dist;
k.div = function(e, t, r) {
  return P.divide(e.array, t.array, r.array), e._dirty = !0, e;
};
k.divide = k.div;
k.dot = function(e, t) {
  return P.dot(e.array, t.array);
};
k.len = function(e) {
  return P.length(e.array);
};
k.lerp = function(e, t, r, i) {
  return P.lerp(e.array, t.array, r.array, i), e._dirty = !0, e;
};
k.min = function(e, t, r) {
  return P.min(e.array, t.array, r.array), e._dirty = !0, e;
};
k.max = function(e, t, r) {
  return P.max(e.array, t.array, r.array), e._dirty = !0, e;
};
k.mul = function(e, t, r) {
  return P.multiply(e.array, t.array, r.array), e._dirty = !0, e;
};
k.multiply = k.mul;
k.negate = function(e, t) {
  return P.negate(e.array, t.array), e._dirty = !0, e;
};
k.normalize = function(e, t) {
  return P.normalize(e.array, t.array), e._dirty = !0, e;
};
k.random = function(e, t) {
  return P.random(e.array, t), e._dirty = !0, e;
};
k.scale = function(e, t, r) {
  return P.scale(e.array, t.array, r), e._dirty = !0, e;
};
k.scaleAndAdd = function(e, t, r, i) {
  return P.scaleAndAdd(e.array, t.array, r.array, i), e._dirty = !0, e;
};
k.sqrDist = function(e, t) {
  return P.sqrDist(e.array, t.array);
};
k.squaredDistance = k.sqrDist;
k.sqrLen = function(e) {
  return P.sqrLen(e.array);
};
k.squaredLength = k.sqrLen;
k.sub = function(e, t, r) {
  return P.subtract(e.array, t.array, r.array), e._dirty = !0, e;
};
k.subtract = k.sub;
k.transformMat2 = function(e, t, r) {
  return P.transformMat2(e.array, t.array, r.array), e._dirty = !0, e;
};
k.transformMat2d = function(e, t, r) {
  return P.transformMat2d(e.array, t.array, r.array), e._dirty = !0, e;
};
k.transformMat3 = function(e, t, r) {
  return P.transformMat3(e.array, t.array, r.array), e._dirty = !0, e;
};
k.transformMat4 = function(e, t, r) {
  return P.transformMat4(e.array, t.array, r.array), e._dirty = !0, e;
};
var pa = 1, ma = 2, Fi = 3, _a = {};
function Eu(e) {
  for (var t = e.split(`
`), r = 0, i = t.length; r < i; r++)
    t[r] = r + 1 + ": " + t[r];
  return t.join(`
`);
}
function ga(e, t, r) {
  if (!e.getShaderParameter(t, e.COMPILE_STATUS))
    return [e.getShaderInfoLog(t), Eu(r)].join(`
`);
}
var ya = new Q.Float32Array(16), Su = Ae.extend({
  uniformSemantics: {},
  attributes: {}
}, function() {
  this._locations = {}, this._textureSlot = 0, this._program = null;
}, {
  bind: function(e) {
    this._textureSlot = 0, e.gl.useProgram(this._program);
  },
  hasUniform: function(e) {
    var t = this._locations[e];
    return t != null;
  },
  useTextureSlot: function(e, t, r) {
    t && (e.gl.activeTexture(e.gl.TEXTURE0 + r), t.isRenderable() ? t.bind(e) : t.unbind(e));
  },
  currentTextureSlot: function() {
    return this._textureSlot;
  },
  resetTextureSlot: function(e) {
    this._textureSlot = e || 0;
  },
  takeCurrentTextureSlot: function(e, t) {
    var r = this._textureSlot;
    return this.useTextureSlot(e, t, r), this._textureSlot++, r;
  },
  setUniform: function(e, t, r, i) {
    var n = this._locations, a = n[r];
    if (a == null)
      return !1;
    switch (t) {
      case "m4":
        if (!(i instanceof Float32Array)) {
          for (var o = 0; o < i.length; o++)
            ya[o] = i[o];
          i = ya;
        }
        e.uniformMatrix4fv(a, !1, i);
        break;
      case "2i":
        e.uniform2i(a, i[0], i[1]);
        break;
      case "2f":
        e.uniform2f(a, i[0], i[1]);
        break;
      case "3i":
        e.uniform3i(a, i[0], i[1], i[2]);
        break;
      case "3f":
        e.uniform3f(a, i[0], i[1], i[2]);
        break;
      case "4i":
        e.uniform4i(a, i[0], i[1], i[2], i[3]);
        break;
      case "4f":
        e.uniform4f(a, i[0], i[1], i[2], i[3]);
        break;
      case "1i":
        e.uniform1i(a, i);
        break;
      case "1f":
        e.uniform1f(a, i);
        break;
      case "1fv":
        e.uniform1fv(a, i);
        break;
      case "1iv":
        e.uniform1iv(a, i);
        break;
      case "2iv":
        e.uniform2iv(a, i);
        break;
      case "2fv":
        e.uniform2fv(a, i);
        break;
      case "3iv":
        e.uniform3iv(a, i);
        break;
      case "3fv":
        e.uniform3fv(a, i);
        break;
      case "4iv":
        e.uniform4iv(a, i);
        break;
      case "4fv":
        e.uniform4fv(a, i);
        break;
      case "m2":
      case "m2v":
        e.uniformMatrix2fv(a, !1, i);
        break;
      case "m3":
      case "m3v":
        e.uniformMatrix3fv(a, !1, i);
        break;
      case "m4v":
        if (Array.isArray(i) && Array.isArray(i[0])) {
          for (var s = new Q.Float32Array(i.length * 16), l = 0, o = 0; o < i.length; o++)
            for (var u = i[o], f = 0; f < 16; f++)
              s[l++] = u[f];
          e.uniformMatrix4fv(a, !1, s);
        } else
          e.uniformMatrix4fv(a, !1, i);
        break;
    }
    return !0;
  },
  setUniformOfSemantic: function(e, t, r) {
    var i = this.uniformSemantics[t];
    return i ? this.setUniform(e, i.type, i.symbol, r) : !1;
  },
  // Used for creating VAO
  // Enable the attributes passed in and disable the rest
  // Example Usage:
  // enableAttributes(renderer, ["position", "texcoords"])
  enableAttributes: function(e, t, r) {
    var i = e.gl, n = this._program, a = this._locations, o;
    r ? o = r.__enabledAttributeList : o = _a[e.__uid__], o || (r ? o = r.__enabledAttributeList = [] : o = _a[e.__uid__] = []);
    for (var s = [], l = 0; l < t.length; l++) {
      var u = t[l];
      if (!this.attributes[u]) {
        s[l] = -1;
        continue;
      }
      var f = a[u];
      if (f == null) {
        if (f = i.getAttribLocation(n, u), f === -1) {
          s[l] = -1;
          continue;
        }
        a[u] = f;
      }
      s[l] = f, o[f] ? o[f] = ma : o[f] = pa;
    }
    for (var l = 0; l < o.length; l++)
      switch (o[l]) {
        case pa:
          i.enableVertexAttribArray(l), o[l] = Fi;
          break;
        case ma:
          o[l] = Fi;
          break;
        // Expired
        case Fi:
          i.disableVertexAttribArray(l), o[l] = 0;
          break;
      }
    return s;
  },
  getAttribLocation: function(e, t) {
    var r = this._locations, i = r[t];
    return i == null && (i = e.getAttribLocation(this._program, t), r[t] = i), i;
  },
  buildProgram: function(e, t, r, i) {
    var n = e.createShader(e.VERTEX_SHADER), a = e.createProgram();
    e.shaderSource(n, r), e.compileShader(n);
    var o = e.createShader(e.FRAGMENT_SHADER);
    e.shaderSource(o, i), e.compileShader(o);
    var s = ga(e, n, r);
    if (s || (s = ga(e, o, i), s))
      return s;
    if (e.attachShader(a, n), e.attachShader(a, o), t.attributeSemantics.POSITION)
      e.bindAttribLocation(a, 0, t.attributeSemantics.POSITION.symbol);
    else {
      var l = Object.keys(this.attributes);
      e.bindAttribLocation(a, 0, l[0]);
    }
    if (e.linkProgram(a), e.deleteShader(n), e.deleteShader(o), this._program = a, this.vertexCode = r, this.fragmentCode = i, !e.getProgramParameter(a, e.LINK_STATUS))
      return `Could not link program
` + e.getProgramInfoLog(a);
    for (var u = 0; u < t.uniforms.length; u++) {
      var f = t.uniforms[u];
      this._locations[f] = e.getUniformLocation(a, f);
    }
  }
}), Au = /for\s*?\(int\s*?_idx_\s*\=\s*([\w-]+)\;\s*_idx_\s*<\s*([\w-]+);\s*_idx_\s*\+\+\s*\)\s*\{\{([\s\S]+?)(?=\}\})\}\}/g;
function xa(e, t, r) {
  function i(o, s, l, u) {
    var f = "";
    isNaN(s) && (s in t ? s = t[s] : s = n[s]), isNaN(l) && (l in t ? l = t[l] : l = n[l]);
    for (var h = parseInt(s); h < parseInt(l); h++)
      f += "{" + u.replace(/float\s*\(\s*_idx_\s*\)/g, h.toFixed(1)).replace(/_idx_/g, h) + "}";
    return f;
  }
  var n = {};
  for (var a in r)
    n[a + "_COUNT"] = r[a];
  return e.replace(Au, i);
}
function Ui(e, t, r) {
  var i = [];
  if (t)
    for (var n in t) {
      var a = t[n];
      a > 0 && i.push("#define " + n.toUpperCase() + "_COUNT " + a);
    }
  if (r)
    for (var o = 0; o < r.length; o++) {
      var s = r[o];
      i.push("#define " + s.toUpperCase() + "_ENABLED");
    }
  for (var s in e) {
    var l = e[s];
    l === null ? i.push("#define " + s) : i.push("#define " + s + " " + l.toString());
  }
  return i.join(`
`);
}
function Cu(e) {
  for (var t = [], r = 0; r < e.length; r++)
    t.push("#extension GL_" + e[r] + " : enable");
  return t.join(`
`);
}
function Mu(e) {
  return ["precision", e, "float"].join(" ") + `;
` + ["precision", e, "int"].join(" ") + `;
` + ["precision", e, "sampler2D"].join(" ") + `;
`;
}
function Do(e) {
  this._renderer = e, this._cache = {};
}
Do.prototype.getProgram = function(e, t, r) {
  var i = this._cache, n = e.isSkinnedMesh && e.isSkinnedMesh(), a = e.isInstancedMesh && e.isInstancedMesh(), o = "s" + t.shader.shaderID + "m" + t.getProgramKey();
  r && (o += "se" + r.getProgramKey(e.lightGroup)), n && (o += ",sk" + e.joints.length), a && (o += ",is");
  var g = i[o];
  if (g)
    return g;
  var s = r ? r.getLightsNumbers(e.lightGroup) : {}, l = this._renderer, u = l.gl, f = t.getEnabledTextures(), h = "";
  if (n) {
    var d = {
      SKINNING: null,
      JOINT_COUNT: e.joints.length
    };
    e.joints.length > l.getMaxJointNumber() && (d.USE_SKIN_MATRICES_TEXTURE = null), h += `
` + Ui(d) + `
`;
  }
  a && (h += `
#define INSTANCING
`);
  var c = h + Ui(t.vertexDefines, s, f), v = h + Ui(t.fragmentDefines, s, f), p = c + `
` + t.shader.vertex, m = [
    "OES_standard_derivatives",
    "EXT_shader_texture_lod"
  ].filter(function(w) {
    return l.getGLExtension(w) != null;
  });
  m.indexOf("EXT_shader_texture_lod") >= 0 && (v += `
#define SUPPORT_TEXTURE_LOD`), m.indexOf("OES_standard_derivatives") >= 0 && (v += `
#define SUPPORT_STANDARD_DERIVATIVES`);
  var _ = Cu(m) + `
` + Mu(t.precision) + `
` + v + `
` + t.shader.fragment, x = xa(p, t.vertexDefines, s), y = xa(_, t.fragmentDefines, s), g = new Su();
  g.uniformSemantics = t.shader.uniformSemantics, g.attributes = t.shader.attributes;
  var b = g.buildProgram(u, t.shader, x, y);
  return g.__error = b, i[o] = g, g;
};
var Ta = /uniform\s+(bool|float|int|vec2|vec3|vec4|ivec2|ivec3|ivec4|mat2|mat3|mat4|sampler2D|samplerCube)\s+([\s\S]*?);/g, Lu = /attribute\s+(float|int|vec2|vec3|vec4)\s+([\s\S]*?);/g, wa = /#define\s+(\w+)?(\s+[\d-.]+)?\s*;?\s*\n/g, Du = {
  bool: "1i",
  int: "1i",
  sampler2D: "t",
  samplerCube: "t",
  float: "1f",
  vec2: "2f",
  vec3: "3f",
  vec4: "4f",
  ivec2: "2i",
  ivec3: "3i",
  ivec4: "4i",
  mat2: "m2",
  mat3: "m3",
  mat4: "m4"
};
function nt(e) {
  for (var t = [], r = 0; r < e; r++)
    t[r] = 0;
  return t;
}
var ba = {
  bool: function() {
    return !0;
  },
  int: function() {
    return 0;
  },
  float: function() {
    return 0;
  },
  sampler2D: function() {
    return null;
  },
  samplerCube: function() {
    return null;
  },
  vec2: function() {
    return nt(2);
  },
  vec3: function() {
    return nt(3);
  },
  vec4: function() {
    return nt(4);
  },
  ivec2: function() {
    return nt(2);
  },
  ivec3: function() {
    return nt(3);
  },
  ivec4: function() {
    return nt(4);
  },
  mat2: function() {
    return nt(4);
  },
  mat3: function() {
    return nt(9);
  },
  mat4: function() {
    return nt(16);
  },
  array: function() {
    return [];
  }
}, pn = [
  "POSITION",
  "NORMAL",
  "BINORMAL",
  "TANGENT",
  "TEXCOORD",
  "TEXCOORD_0",
  "TEXCOORD_1",
  "COLOR",
  // Skinning
  // https://github.com/KhronosGroup/glTF/blob/master/specification/README.md#semantics
  "JOINT",
  "WEIGHT"
], Po = [
  "SKIN_MATRIX",
  // Information about viewport
  "VIEWPORT_SIZE",
  "VIEWPORT",
  "DEVICEPIXELRATIO",
  // Window size for window relative coordinate
  // https://www.opengl.org/sdk/docs/man/html/gl_FragCoord.xhtml
  "WINDOW_SIZE",
  // Infomation about camera
  "NEAR",
  "FAR",
  // Time
  "TIME"
], Ro = [
  "WORLD",
  "VIEW",
  "PROJECTION",
  "WORLDVIEW",
  "VIEWPROJECTION",
  "WORLDVIEWPROJECTION",
  "WORLDINVERSE",
  "VIEWINVERSE",
  "PROJECTIONINVERSE",
  "WORLDVIEWINVERSE",
  "VIEWPROJECTIONINVERSE",
  "WORLDVIEWPROJECTIONINVERSE",
  "WORLDTRANSPOSE",
  "VIEWTRANSPOSE",
  "PROJECTIONTRANSPOSE",
  "WORLDVIEWTRANSPOSE",
  "VIEWPROJECTIONTRANSPOSE",
  "WORLDVIEWPROJECTIONTRANSPOSE",
  "WORLDINVERSETRANSPOSE",
  "VIEWINVERSETRANSPOSE",
  "PROJECTIONINVERSETRANSPOSE",
  "WORLDVIEWINVERSETRANSPOSE",
  "VIEWPROJECTIONINVERSETRANSPOSE",
  "WORLDVIEWPROJECTIONINVERSETRANSPOSE"
], Pu = {
  // WebGL does not support integer attributes
  vec4: 4,
  vec3: 3,
  vec2: 2,
  float: 1
}, zi = {}, No = {};
function Ru(e, t) {
  var r = "vertex:" + e + "fragment:" + t;
  if (zi[r])
    return zi[r];
  var i = he.genGUID();
  return zi[r] = i, No[i] = {
    vertex: e,
    fragment: t
  }, i;
}
function Ea(e) {
  return e.replace(/[ \t]*\/\/.*\n/g, "").replace(/[ \t]*\/\*[\s\S]*?\*\//g, "");
}
function Ft() {
  console.error("Wrong uniform/attributes syntax");
}
function Sa(e, t) {
  for (var r = /[,=\(\):]/, i = t.replace(/:\s*\[\s*(.*)\s*\]/g, "=" + e + "($1)").replace(/\s+/g, "").split(/(?=[,=\(\):])/g), n = [], a = 0; a < i.length; a++)
    i[a].match(r) ? n.push(
      i[a].charAt(0),
      i[a].slice(1)
    ) : n.push(i[a]);
  i = n;
  var o = 0, s = 1, l = 2, u = 3, f = 4, h = 5, d = o, c = {}, v = null, p;
  m(i[0]);
  function m(y) {
    y || Ft();
    var g = y.match(/\[(.*?)\]/);
    p = y.replace(/\[(.*?)\]/, ""), c[p] = {}, g && (c[p].isArray = !0, c[p].arraySize = g[1]);
  }
  for (var a = 1; a < i.length; a++) {
    var _ = i[a];
    if (_) {
      if (_ === "=") {
        if (d !== o && d !== u) {
          Ft();
          break;
        }
        d = s;
        continue;
      } else if (_ === ":") {
        d = f;
        continue;
      } else if (_ === ",") {
        if (d === l) {
          if (!(v instanceof Array)) {
            Ft();
            break;
          }
          v.push(+i[++a]);
        } else
          d = h;
        continue;
      } else if (_ === ")") {
        c[p].value = new Q.Float32Array(v), v = null, d = h;
        continue;
      } else if (_ === "(") {
        if (d !== l) {
          Ft();
          break;
        }
        if (!(v instanceof Array)) {
          Ft();
          break;
        }
        v.push(+i[++a]);
        continue;
      } else if (_.indexOf("vec") >= 0) {
        if (d !== s && d !== f) {
          Ft();
          break;
        }
        d = l, v = [];
        continue;
      } else if (d === s) {
        e === "bool" ? c[p].value = _ === "true" : c[p].value = parseFloat(_), v = null;
        continue;
      } else if (d === f) {
        var x = _;
        pn.indexOf(x) >= 0 || Po.indexOf(x) >= 0 || Ro.indexOf(x) >= 0 ? c[p].semantic = x : x === "ignore" || x === "unconfigurable" ? c[p].ignore = !0 : e === "bool" ? c[p].value = x === "true" : c[p].value = parseFloat(x);
        continue;
      }
      m(_), d = o;
    }
  }
  return c;
}
function U(e, t) {
  typeof e == "object" && (t = e.fragment, e = e.vertex), e = Ea(e), t = Ea(t), this._shaderID = Ru(e, t), this._vertexCode = U.parseImport(e), this._fragmentCode = U.parseImport(t), this.attributeSemantics = {}, this.matrixSemantics = {}, this.uniformSemantics = {}, this.matrixSemanticKeys = [], this.uniformTemplates = {}, this.attributes = {}, this.textures = {}, this.vertexDefines = {}, this.fragmentDefines = {}, this._parseAttributes(), this._parseUniforms(), this._parseDefines();
}
U.prototype = {
  constructor: U,
  // Create a new uniform instance for material
  createUniforms: function() {
    var e = {};
    for (var t in this.uniformTemplates) {
      var r = this.uniformTemplates[t];
      e[t] = {
        type: r.type,
        value: r.value()
      };
    }
    return e;
  },
  _parseImport: function() {
    this._vertexCode = U.parseImport(this.vertex), this._fragmentCode = U.parseImport(this.fragment);
  },
  _addSemanticUniform: function(e, t, r) {
    if (pn.indexOf(r) >= 0)
      this.attributeSemantics[r] = {
        symbol: e,
        type: t
      };
    else if (Ro.indexOf(r) >= 0) {
      var i = !1, n = r;
      r.match(/TRANSPOSE$/) && (i = !0, n = r.slice(0, -9)), this.matrixSemantics[r] = {
        symbol: e,
        type: t,
        isTranspose: i,
        semanticNoTranspose: n
      };
    } else Po.indexOf(r) >= 0 && (this.uniformSemantics[r] = {
      symbol: e,
      type: t
    });
  },
  _addMaterialUniform: function(e, t, r, i, n, a) {
    a[e] = {
      type: r,
      value: n ? ba.array : i || ba[t],
      semantic: null
    };
  },
  _parseUniforms: function() {
    var e = {}, t = this, r = "vertex";
    this._uniformList = [], this._vertexCode = this._vertexCode.replace(Ta, n), r = "fragment", this._fragmentCode = this._fragmentCode.replace(Ta, n), t.matrixSemanticKeys = Object.keys(this.matrixSemantics);
    function i(a) {
      return a != null ? function() {
        return a;
      } : null;
    }
    function n(a, o, s) {
      var l = Sa(o, s), u = [];
      for (var f in l) {
        var h = l[f], d = h.semantic, c = f, v = Du[o], p = i(l[f].value);
        l[f].isArray && (c += "[" + l[f].arraySize + "]", v += "v"), u.push(c), t._uniformList.push(f), h.ignore || ((o === "sampler2D" || o === "samplerCube") && (t.textures[f] = {
          shaderType: r,
          type: o
        }), d ? t._addSemanticUniform(f, v, d) : t._addMaterialUniform(
          f,
          o,
          v,
          p,
          l[f].isArray,
          e
        ));
      }
      return u.length > 0 ? "uniform " + o + " " + u.join(",") + `;
` : "";
    }
    this.uniformTemplates = e;
  },
  _parseAttributes: function() {
    var e = {}, t = this;
    this._vertexCode = this._vertexCode.replace(Lu, r);
    function r(i, n, a) {
      var o = Sa(n, a), s = Pu[n] || 1, l = [];
      for (var u in o) {
        var f = o[u].semantic;
        if (e[u] = {
          // TODO Can only be float
          type: "float",
          size: s,
          semantic: f || null
        }, f) {
          if (pn.indexOf(f) < 0)
            throw new Error('Unkown semantic "' + f + '"');
          t.attributeSemantics[f] = {
            symbol: u,
            type: n
          };
        }
        l.push(u);
      }
      return "attribute " + n + " " + l.join(",") + `;
`;
    }
    this.attributes = e;
  },
  _parseDefines: function() {
    var e = this, t = "vertex";
    this._vertexCode = this._vertexCode.replace(wa, r), t = "fragment", this._fragmentCode = this._fragmentCode.replace(wa, r);
    function r(i, n, a) {
      var o = t === "vertex" ? e.vertexDefines : e.fragmentDefines;
      return o[n] || (a === "false" ? o[n] = !1 : a === "true" ? o[n] = !0 : o[n] = a ? isNaN(parseFloat(a)) ? a.trim() : parseFloat(a) : null), "";
    }
  },
  /**
   * Clone a new shader
   * @return {clay.Shader}
   */
  clone: function() {
    var e = No[this._shaderID], t = new U(e.vertex, e.fragment);
    return t;
  }
};
Object.defineProperty && (Object.defineProperty(U.prototype, "shaderID", {
  get: function() {
    return this._shaderID;
  }
}), Object.defineProperty(U.prototype, "vertex", {
  get: function() {
    return this._vertexCode;
  }
}), Object.defineProperty(U.prototype, "fragment", {
  get: function() {
    return this._fragmentCode;
  }
}), Object.defineProperty(U.prototype, "uniforms", {
  get: function() {
    return this._uniformList;
  }
}));
var Nu = /(@import)\s*([0-9a-zA-Z_\-\.]*)/g;
U.parseImport = function(e) {
  return e = e.replace(Nu, function(n, r, i) {
    var n = U.source(i);
    return n ? U.parseImport(n) : (console.error('Shader chunk "' + i + '" not existed in library'), "");
  }), e;
};
var Ou = /(@export)\s*([0-9a-zA-Z_\-\.]*)\s*\n([\s\S]*?)@end/g;
U.import = function(e) {
  e.replace(Ou, function(t, r, i, a) {
    var a = a.replace(/(^[\s\t\xa0\u3000]+)|([\u3000\xa0\s\t]+\x24)/g, "");
    if (a) {
      for (var o = i.split("."), s = U.codes, l = 0, u; l < o.length - 1; )
        u = o[l++], s[u] || (s[u] = {}), s = s[u];
      u = o[l], s[u] = a;
    }
    return a;
  });
};
U.codes = {};
U.source = function(e) {
  for (var t = e.split("."), r = U.codes, i = 0; r && i < t.length; ) {
    var n = t[i++];
    r = r[n];
  }
  return typeof r != "string" ? (console.error('Shader "' + e + '" not existed in library'), "") : r;
};
const Oo = `@export clay.prez.vertex
uniform mat4 WVP : WORLDVIEWPROJECTION;
attribute vec3 pos : POSITION;
attribute vec2 uv : TEXCOORD_0;
uniform vec2 uvRepeat : [1.0, 1.0];
uniform vec2 uvOffset : [0.0, 0.0];
@import clay.chunk.skinning_header
@import clay.chunk.instancing_header
varying vec2 v_Texcoord;
void main()
{
 vec4 P = vec4(pos, 1.0);
#ifdef SKINNING
 @import clay.chunk.skin_matrix
 P = skinMatrixWS * P;
#endif
#ifdef INSTANCING
 @import clay.chunk.instancing_matrix
 P = instanceMat * P;
#endif
 gl_Position = WVP * P;
 v_Texcoord = uv * uvRepeat + uvOffset;
}
@end
@export clay.prez.fragment
uniform sampler2D alphaMap;
uniform float alphaCutoff: 0.0;
varying vec2 v_Texcoord;
void main()
{
 if (alphaCutoff > 0.0) {
 if (texture2D(alphaMap, v_Texcoord).a <= alphaCutoff) {
 discard;
 }
 }
 gl_FragColor = vec4(0.0,0.0,0.0,1.0);
}
@end`;
U.import(Oo);
var ae = D.create, Aa = {};
function Ca(e) {
  return e.material;
}
function Iu(e, t, r) {
  return t.uniforms[r].value;
}
function Bu(e, t, r, i) {
  return r !== i;
}
function Fu(e) {
  return !0;
}
function Ma() {
}
var La = {
  float: M.FLOAT,
  byte: M.BYTE,
  ubyte: M.UNSIGNED_BYTE,
  short: M.SHORT,
  ushort: M.UNSIGNED_SHORT
};
function Uu(e, t, r) {
  this.availableAttributes = e, this.availableAttributeSymbols = t, this.indicesBuffer = r, this.vao = null;
}
function zu(e) {
  var t, r;
  this.bind = function(i) {
    t || (t = Q.createCanvas(), t.width = t.height = 1, t.getContext("2d"));
    var n = i.gl, a = !r;
    a && (r = n.createTexture()), n.bindTexture(n.TEXTURE_2D, r), a && n.texImage2D(n.TEXTURE_2D, 0, n.RGBA, n.RGBA, n.UNSIGNED_BYTE, t);
  }, this.unbind = function(i) {
    i.gl.bindTexture(i.gl.TEXTURE_2D, null);
  }, this.isRenderable = function() {
    return !0;
  };
}
var Ve = Ae.extend(
  function() {
    return (
      /** @lends clay.Renderer# */
      {
        /**
         * @type {HTMLCanvasElement}
         * @readonly
         */
        canvas: null,
        /**
         * Canvas width, set by resize method
         * @type {number}
         * @private
         */
        _width: 100,
        /**
         * Canvas width, set by resize method
         * @type {number}
         * @private
         */
        _height: 100,
        /**
         * Device pixel ratio, set by setDevicePixelRatio method
         * Specially for high defination display
         * @see http://www.khronos.org/webgl/wiki/HandlingHighDPI
         * @type {number}
         * @private
         */
        devicePixelRatio: typeof window < "u" && window.devicePixelRatio || 1,
        /**
         * Clear color
         * @type {number[]}
         */
        clearColor: [0, 0, 0, 0],
        /**
         * Default:
         *     _gl.COLOR_BUFFER_BIT | _gl.DEPTH_BUFFER_BIT | _gl.STENCIL_BUFFER_BIT
         * @type {number}
         */
        clearBit: 17664,
        // Settings when getting context
        // http://www.khronos.org/registry/webgl/specs/latest/#2.4
        /**
         * If enable alpha, default true
         * @type {boolean}
         */
        alpha: !0,
        /**
         * If enable depth buffer, default true
         * @type {boolean}
         */
        depth: !0,
        /**
         * If enable stencil buffer, default false
         * @type {boolean}
         */
        stencil: !1,
        /**
         * If enable antialias, default true
         * @type {boolean}
         */
        antialias: !0,
        /**
         * If enable premultiplied alpha, default true
         * @type {boolean}
         */
        premultipliedAlpha: !0,
        /**
         * If preserve drawing buffer, default false
         * @type {boolean}
         */
        preserveDrawingBuffer: !1,
        /**
         * If throw context error, usually turned on in debug mode
         * @type {boolean}
         */
        throwError: !0,
        /**
         * WebGL Context created from given canvas
         * @type {WebGLRenderingContext}
         */
        gl: null,
        /**
         * Renderer viewport, read-only, can be set by setViewport method
         * @type {Object}
         */
        viewport: {},
        /**
         * Max joint number
         * @type {number}
         */
        maxJointNumber: 20,
        // Set by FrameBuffer#bind
        __currentFrameBuffer: null,
        _viewportStack: [],
        _clearStack: [],
        _sceneRendering: null
      }
    );
  },
  function() {
    this.canvas || (this.canvas = Q.createCanvas());
    var e = this.canvas;
    try {
      var t = {
        alpha: this.alpha,
        depth: this.depth,
        stencil: this.stencil,
        antialias: this.antialias,
        premultipliedAlpha: this.premultipliedAlpha,
        preserveDrawingBuffer: this.preserveDrawingBuffer
      };
      if (this.gl = e.getContext("webgl", t) || e.getContext("experimental-webgl", t), !this.gl)
        throw new Error();
      this._glinfo = new _u(this.gl), this.gl.targetRenderer && console.error("Already created a renderer"), this.gl.targetRenderer = this, this.resize();
    } catch (r) {
      throw "Error creating WebGL Context " + r;
    }
    this._programMgr = new Do(this), this._placeholderTexture = new zu();
  },
  /** @lends clay.Renderer.prototype. **/
  {
    /**
     * Resize the canvas
     * @param {number} width
     * @param {number} height
     */
    resize: function(e, t) {
      var r = this.canvas, i = this.devicePixelRatio;
      e != null ? (r.style && (r.style.width = e + "px", r.style.height = t + "px"), r.width = e * i, r.height = t * i, this._width = e, this._height = t) : (this._width = r.width / i, this._height = r.height / i), this.setViewport(0, 0, this._width, this._height);
    },
    /**
     * Get renderer width
     * @return {number}
     */
    getWidth: function() {
      return this._width;
    },
    /**
     * Get renderer height
     * @return {number}
     */
    getHeight: function() {
      return this._height;
    },
    /**
     * Get viewport aspect,
     * @return {number}
     */
    getViewportAspect: function() {
      var e = this.viewport;
      return e.width / e.height;
    },
    /**
     * Set devicePixelRatio
     * @param {number} devicePixelRatio
     */
    setDevicePixelRatio: function(e) {
      this.devicePixelRatio = e, this.resize(this._width, this._height);
    },
    /**
     * Get devicePixelRatio
     * @param {number} devicePixelRatio
     */
    getDevicePixelRatio: function() {
      return this.devicePixelRatio;
    },
    /**
     * Get WebGL extension
     * @param {string} name
     * @return {object}
     */
    getGLExtension: function(e) {
      return this._glinfo.getExtension(e);
    },
    /**
     * Get WebGL parameter
     * @param {string} name
     * @return {*}
     */
    getGLParameter: function(e) {
      return this._glinfo.getParameter(e);
    },
    /**
     * Set rendering viewport
     * @param {number|Object} x
     * @param {number} [y]
     * @param {number} [width]
     * @param {number} [height]
     * @param {number} [devicePixelRatio]
     *        Defaultly use the renderere devicePixelRatio
     *        It needs to be 1 when setViewport is called by frameBuffer
     *
     * @example
     *  setViewport(0,0,width,height,1)
     *  setViewport({
     *      x: 0,
     *      y: 0,
     *      width: width,
     *      height: height,
     *      devicePixelRatio: 1
     *  })
     */
    setViewport: function(e, t, r, i, n) {
      if (typeof e == "object") {
        var a = e;
        e = a.x, t = a.y, r = a.width, i = a.height, n = a.devicePixelRatio;
      }
      n = n || this.devicePixelRatio, this.gl.viewport(
        e * n,
        t * n,
        r * n,
        i * n
      ), this.viewport = {
        x: e,
        y: t,
        width: r,
        height: i,
        devicePixelRatio: n
      };
    },
    /**
     * Push current viewport into a stack
     */
    saveViewport: function() {
      this._viewportStack.push(this.viewport);
    },
    /**
     * Pop viewport from stack, restore in the renderer
     */
    restoreViewport: function() {
      this._viewportStack.length > 0 && this.setViewport(this._viewportStack.pop());
    },
    /**
     * Push current clear into a stack
     */
    saveClear: function() {
      this._clearStack.push({
        clearBit: this.clearBit,
        clearColor: this.clearColor
      });
    },
    /**
     * Pop clear from stack, restore in the renderer
     */
    restoreClear: function() {
      if (this._clearStack.length > 0) {
        var e = this._clearStack.pop();
        this.clearColor = e.clearColor, this.clearBit = e.clearBit;
      }
    },
    bindSceneRendering: function(e) {
      this._sceneRendering = e;
    },
    /**
     * Render the scene in camera to the screen or binded offline framebuffer
     * @param  {clay.Scene}       scene
     * @param  {clay.Camera}      camera
     * @param  {boolean}     [notUpdateScene] If not call the scene.update methods in the rendering, default true
     * @param  {boolean}     [preZ]           If use preZ optimization, default false
     * @return {IRenderInfo}
     */
    render: function(e, t, r, i) {
      var n = this.gl, a = this.clearColor;
      if (this.clearBit) {
        n.colorMask(!0, !0, !0, !0), n.depthMask(!0);
        var o = this.viewport, s = !1, l = o.devicePixelRatio;
        (o.width !== this._width || o.height !== this._height || l && l !== this.devicePixelRatio || o.x || o.y) && (s = !0, n.enable(n.SCISSOR_TEST), n.scissor(o.x * l, o.y * l, o.width * l, o.height * l)), n.clearColor(a[0], a[1], a[2], a[3]), n.clear(this.clearBit), s && n.disable(n.SCISSOR_TEST);
      }
      if (r || e.update(!1), e.updateLights(), t = t || e.getMainCamera(), !t) {
        console.error("Can't find camera in the scene.");
        return;
      }
      t.update();
      var u = e.updateRenderList(t, !0);
      this._sceneRendering = e;
      var f = u.opaque, h = u.transparent, d = e.material;
      e.trigger("beforerender", this, e, t, u), i ? (this.renderPreZ(f, e, t), n.depthFunc(n.LEQUAL)) : n.depthFunc(n.LESS);
      for (var c = ae(), v = T.create(), p = 0; p < h.length; p++) {
        var m = h[p];
        D.multiplyAffine(c, t.viewMatrix.array, m.worldTransform.array), T.transformMat4(v, m.position.array, c), m.__depth = v[2];
      }
      this.renderPass(f, t, {
        getMaterial: function(_) {
          return d || _.material;
        },
        sortCompare: this.opaqueSortCompare
      }), this.renderPass(h, t, {
        getMaterial: function(_) {
          return d || _.material;
        },
        sortCompare: this.transparentSortCompare
      }), e.trigger("afterrender", this, e, t, u), this._sceneRendering = null;
    },
    getProgram: function(e, t, r) {
      return t = t || e.material, this._programMgr.getProgram(e, t, r);
    },
    validateProgram: function(e) {
      if (e.__error) {
        var t = e.__error;
        if (Aa[e.__uid__])
          return;
        if (Aa[e.__uid__] = !0, this.throwError)
          throw new Error(t);
        this.trigger("error", t);
      }
    },
    updatePrograms: function(e, t, r) {
      var i = r && r.getMaterial || Ca;
      t = t || null;
      for (var n = 0; n < e.length; n++) {
        var a = e[n], o = i.call(this, a);
        if (n > 0) {
          var s = e[n - 1], l = s.joints ? s.joints.length : 0, u = a.joints ? a.joints.length : 0;
          if (u === l && a.material === s.material && a.lightGroup === s.lightGroup) {
            a.__program = s.__program;
            continue;
          }
        }
        var f = this._programMgr.getProgram(a, o, t);
        this.validateProgram(f), a.__program = f;
      }
    },
    /**
     * Render a single renderable list in camera in sequence
     * @param {clay.Renderable[]} list List of all renderables.
     * @param {clay.Camera} [camera] Camera provide view matrix and porjection matrix. It can be null.
     * @param {Object} [passConfig]
     * @param {Function} [passConfig.getMaterial] Get renderable material.
     * @param {Function} [passConfig.getUniform] Get material uniform value.
     * @param {Function} [passConfig.isMaterialChanged] If material changed.
     * @param {Function} [passConfig.beforeRender] Before render each renderable.
     * @param {Function} [passConfig.afterRender] After render each renderable
     * @param {Function} [passConfig.ifRender] If render the renderable.
     * @param {Function} [passConfig.sortCompare] Sort compare function.
     * @return {IRenderInfo}
     */
    renderPass: function(e, t, r) {
      this.trigger("beforerenderpass", this, e, t, r), r = r || {}, r.getMaterial = r.getMaterial || Ca, r.getUniform = r.getUniform || Iu, r.isMaterialChanged = r.isMaterialChanged || Bu, r.beforeRender = r.beforeRender || Ma, r.afterRender = r.afterRender || Ma;
      var i = r.ifRender || Fu;
      this.updatePrograms(e, this._sceneRendering, r), r.sortCompare && e.sort(r.sortCompare);
      var n = this.viewport, a = n.devicePixelRatio, o = [
        n.x * a,
        n.y * a,
        n.width * a,
        n.height * a
      ], s = this.devicePixelRatio, l = this.__currentFrameBuffer ? [this.__currentFrameBuffer.getTextureWidth(), this.__currentFrameBuffer.getTextureHeight()] : [this._width * s, this._height * s], u = [
        o[2],
        o[3]
      ], f = Date.now();
      t ? (D.copy(ne.VIEW, t.viewMatrix.array), D.copy(ne.PROJECTION, t.projectionMatrix.array), D.copy(ne.VIEWINVERSE, t.worldTransform.array)) : (D.identity(ne.VIEW), D.identity(ne.PROJECTION), D.identity(ne.VIEWINVERSE)), D.multiply(ne.VIEWPROJECTION, ne.PROJECTION, ne.VIEW), D.invert(ne.PROJECTIONINVERSE, ne.PROJECTION), D.invert(ne.VIEWPROJECTIONINVERSE, ne.VIEWPROJECTION);
      for (var h = this.gl, d = this._sceneRendering, c, v, p, m, _, x, y, g, b, w, C, A, R = null, H = 0; H < e.length; H++) {
        var O = e[H], z = O.worldTransform != null, G;
        if (i(O)) {
          z && (G = O.isSkinnedMesh && O.isSkinnedMesh() ? O.offsetMatrix ? O.offsetMatrix.array : ne.IDENTITY : O.worldTransform.array);
          var F = O.geometry, W = r.getMaterial.call(this, O), X = O.__program, q = W.shader, ie = F.__uid__ + "-" + X.__uid__, me = ie !== w;
          w = ie, z && (D.copy(ne.WORLD, G), D.multiply(ne.WORLDVIEWPROJECTION, ne.VIEWPROJECTION, G), D.multiplyAffine(ne.WORLDVIEW, ne.VIEW, G), (q.matrixSemantics.WORLDINVERSE || q.matrixSemantics.WORLDINVERSETRANSPOSE) && D.invert(ne.WORLDINVERSE, G), (q.matrixSemantics.WORLDVIEWINVERSE || q.matrixSemantics.WORLDVIEWINVERSETRANSPOSE) && D.invert(ne.WORLDVIEWINVERSE, ne.WORLDVIEW), (q.matrixSemantics.WORLDVIEWPROJECTIONINVERSE || q.matrixSemantics.WORLDVIEWPROJECTIONINVERSETRANSPOSE) && D.invert(ne.WORLDVIEWPROJECTIONINVERSE, ne.WORLDVIEWPROJECTION)), O.beforeRender && O.beforeRender(this), r.beforeRender.call(this, O, W, c);
          var Se = X !== v;
          Se ? (X.bind(this), X.setUniformOfSemantic(h, "VIEWPORT", o), X.setUniformOfSemantic(h, "WINDOW_SIZE", l), t && (X.setUniformOfSemantic(h, "NEAR", t.near), X.setUniformOfSemantic(h, "FAR", t.far)), X.setUniformOfSemantic(h, "DEVICEPIXELRATIO", a), X.setUniformOfSemantic(h, "TIME", f), X.setUniformOfSemantic(h, "VIEWPORT_SIZE", u), d && d.setLightUniforms(X, O.lightGroup, this)) : X = v, (Se || r.isMaterialChanged(
            O,
            p,
            W,
            c
          )) && (W.depthTest !== m && (W.depthTest ? h.enable(h.DEPTH_TEST) : h.disable(h.DEPTH_TEST), m = W.depthTest), W.depthMask !== _ && (h.depthMask(W.depthMask), _ = W.depthMask), W.transparent !== b && (W.transparent ? h.enable(h.BLEND) : h.disable(h.BLEND), b = W.transparent), W.transparent && (W.blend ? W.blend(h) : (h.blendEquationSeparate(h.FUNC_ADD, h.FUNC_ADD), h.blendFuncSeparate(h.SRC_ALPHA, h.ONE_MINUS_SRC_ALPHA, h.ONE, h.ONE_MINUS_SRC_ALPHA))), A = this._bindMaterial(
            O,
            W,
            X,
            p || null,
            c || null,
            v || null,
            r.getUniform
          ), c = W);
          var ce = q.matrixSemanticKeys;
          if (z)
            for (var ye = 0; ye < ce.length; ye++) {
              var de = ce[ye], Ce = q.matrixSemantics[de], Fe = ne[de];
              if (Ce.isTranspose) {
                var Ie = ne[Ce.semanticNoTranspose];
                D.transpose(Fe, Ie);
              }
              X.setUniform(h, Ce.type, Ce.symbol, Fe);
            }
          O.cullFace !== y && (y = O.cullFace, h.cullFace(y)), O.frontFace !== g && (g = O.frontFace, h.frontFace(g)), O.culling !== x && (x = O.culling, x ? h.enable(h.CULL_FACE) : h.disable(h.CULL_FACE)), this._updateSkeleton(O, X, A), me && (C = this._bindVAO(R, q, F, X)), this._renderObject(O, C, X), r.afterRender(this, O), O.afterRender && O.afterRender(this), v = X, p = O;
        }
      }
      this.trigger("afterrenderpass", this, e, t, r);
    },
    getMaxJointNumber: function() {
      return this.maxJointNumber;
    },
    _updateSkeleton: function(e, t, r) {
      var i = this.gl, n = e.skeleton;
      if (n)
        if (n.update(), e.joints.length > this.getMaxJointNumber()) {
          var a = n.getSubSkinMatricesTexture(e.__uid__, e.joints);
          t.useTextureSlot(this, a, r), t.setUniform(i, "1i", "skinMatricesTexture", r), t.setUniform(i, "1f", "skinMatricesTextureSize", a.width);
        } else {
          var o = n.getSubSkinMatrices(e.__uid__, e.joints);
          t.setUniformOfSemantic(i, "SKIN_MATRIX", o);
        }
    },
    _renderObject: function(e, t, r) {
      var i = this.gl, n = e.geometry, a = e.mode;
      a == null && (a = 4);
      var o = null, s = e.isInstancedMesh && e.isInstancedMesh();
      if (s && (o = this.getGLExtension("ANGLE_instanced_arrays"), !o)) {
        console.warn("Device not support ANGLE_instanced_arrays extension");
        return;
      }
      var l;
      if (s && (l = this._bindInstancedAttributes(e, r, o)), t.indicesBuffer) {
        var u = this.getGLExtension("OES_element_index_uint"), f = u && n.indices instanceof Uint32Array, h = f ? i.UNSIGNED_INT : i.UNSIGNED_SHORT;
        s ? o.drawElementsInstancedANGLE(
          a,
          t.indicesBuffer.count,
          h,
          0,
          e.getInstanceCount()
        ) : i.drawElements(a, t.indicesBuffer.count, h, 0);
      } else
        s ? o.drawArraysInstancedANGLE(a, 0, n.vertexCount, e.getInstanceCount()) : i.drawArrays(a, 0, n.vertexCount);
      if (s)
        for (var d = 0; d < l.length; d++)
          i.disableVertexAttribArray(l[d]);
    },
    _bindInstancedAttributes: function(e, t, r) {
      for (var i = this.gl, n = e.getInstancedAttributesBuffers(this), a = [], o = 0; o < n.length; o++) {
        var s = n[o], l = t.getAttribLocation(i, s.symbol);
        if (!(l < 0)) {
          var u = La[s.type] || i.FLOAT;
          i.enableVertexAttribArray(l), i.bindBuffer(i.ARRAY_BUFFER, s.buffer), i.vertexAttribPointer(l, s.size, u, !1, 0, 0), r.vertexAttribDivisorANGLE(l, s.divisor), a.push(l);
        }
      }
      return a;
    },
    _bindMaterial: function(e, t, r, i, n, a, o) {
      for (var s = this.gl, l = a === r, u = r.currentTextureSlot(), f = t.getEnabledUniforms(), h = t.getTextureUniforms(), d = this._placeholderTexture, c = 0; c < h.length; c++) {
        var v = h[c], p = o(e, t, v), m = t.uniforms[v].type;
        if (m === "t" && p)
          p.__slot = -1;
        else if (m === "tv")
          for (var _ = 0; _ < p.length; _++)
            p[_] && (p[_].__slot = -1);
      }
      d.__slot = -1;
      for (var c = 0; c < f.length; c++) {
        var v = f[c], x = t.uniforms[v], p = o(e, t, v), m = x.type, y = m === "t";
        if (y && (!p || !p.isRenderable()) && (p = d), n && l) {
          var g = o(i, n, v);
          if (y && (!g || !g.isRenderable()) && (g = d), g === p) {
            if (y)
              r.takeCurrentTextureSlot(this, null);
            else if (m === "tv" && p)
              for (var _ = 0; _ < p.length; _++)
                r.takeCurrentTextureSlot(this, null);
            continue;
          }
        }
        if (p != null)
          if (y)
            if (p.__slot < 0) {
              var b = r.currentTextureSlot(), w = r.setUniform(s, "1i", v, b);
              w && (r.takeCurrentTextureSlot(this, p), p.__slot = b);
            } else
              r.setUniform(s, "1i", v, p.__slot);
          else if (Array.isArray(p)) {
            if (p.length === 0)
              continue;
            if (m === "tv") {
              if (!r.hasUniform(v))
                continue;
              for (var C = [], _ = 0; _ < p.length; _++) {
                var A = p[_];
                if (A.__slot < 0) {
                  var b = r.currentTextureSlot();
                  C.push(b), r.takeCurrentTextureSlot(this, A), A.__slot = b;
                } else
                  C.push(A.__slot);
              }
              r.setUniform(s, "1iv", v, C);
            } else
              r.setUniform(s, x.type, v, p);
          } else
            r.setUniform(s, x.type, v, p);
      }
      var R = r.currentTextureSlot();
      return r.resetTextureSlot(u), R;
    },
    _bindVAO: function(e, t, r, i) {
      var n = !r.dynamic, a = this.gl, o = this.__uid__ + "-" + i.__uid__, s = r.__vaoCache[o];
      if (!s) {
        var l = r.getBufferChunks(this);
        if (!l || !l.length)
          return;
        for (var u = l[0], f = u.attributeBuffers, g = u.indicesBuffer, y = [], h = [], d = 0; d < f.length; d++) {
          var c = f[d], v = c.name, p = c.semantic, m;
          if (p) {
            var _ = t.attributeSemantics[p];
            m = _ && _.symbol;
          } else
            m = v;
          m && i.attributes[m] && (y.push(c), h.push(m));
        }
        s = new Uu(
          y,
          h,
          g
        ), n && (r.__vaoCache[o] = s);
      }
      var x = !0;
      e && n && (s.vao == null ? s.vao = e.createVertexArrayOES() : x = !1, e.bindVertexArrayOES(s.vao));
      var y = s.availableAttributes, g = s.indicesBuffer;
      if (x) {
        for (var b = i.enableAttributes(this, s.availableAttributeSymbols, e && n && s), d = 0; d < y.length; d++) {
          var w = b[d];
          if (w !== -1) {
            var c = y[d], C = c.buffer, A = c.size, R = La[c.type] || a.FLOAT;
            a.bindBuffer(a.ARRAY_BUFFER, C), a.vertexAttribPointer(w, A, R, !1, 0, 0);
          }
        }
        r.isUseIndices() && a.bindBuffer(a.ELEMENT_ARRAY_BUFFER, g.buffer);
      }
      return s;
    },
    renderPreZ: function(e, t, r) {
      var i = this.gl, n = this._prezMaterial || new ft({
        shader: new U(U.source("clay.prez.vertex"), U.source("clay.prez.fragment"))
      });
      this._prezMaterial = n, i.colorMask(!1, !1, !1, !1), i.depthMask(!0), this.renderPass(e, r, {
        ifRender: function(a) {
          return !a.ignorePreZ;
        },
        isMaterialChanged: function(a, o) {
          var s = a.material, l = o.material;
          return s.get("diffuseMap") !== l.get("diffuseMap") || (s.get("alphaCutoff") || 0) !== (l.get("alphaCutoff") || 0);
        },
        getUniform: function(a, o, s) {
          if (s === "alphaMap")
            return a.material.get("diffuseMap");
          if (s === "alphaCutoff") {
            if (a.material.isDefined("fragment", "ALPHA_TEST") && a.material.get("diffuseMap")) {
              var l = a.material.get("alphaCutoff");
              return l || 0;
            }
            return 0;
          } else return s === "uvRepeat" ? a.material.get("uvRepeat") : s === "uvOffset" ? a.material.get("uvOffset") : o.get(s);
        },
        getMaterial: function() {
          return n;
        },
        sort: this.opaqueSortCompare
      }), i.colorMask(!0, !0, !0, !0), i.depthMask(!0);
    },
    /**
     * Dispose given scene, including all geometris, textures and shaders in the scene
     * @param {clay.Scene} scene
     */
    disposeScene: function(e) {
      this.disposeNode(e, !0, !0), e.dispose();
    },
    /**
     * Dispose given node, including all geometries, textures and shaders attached on it or its descendant
     * @param {clay.Node} node
     * @param {boolean} [disposeGeometry=false] If dispose the geometries used in the descendant mesh
     * @param {boolean} [disposeTexture=false] If dispose the textures used in the descendant mesh
     */
    disposeNode: function(e, t, r) {
      e.getParent() && e.getParent().remove(e);
      var i = {};
      e.traverse(function(n) {
        var a = n.material;
        if (n.geometry && t && n.geometry.dispose(this), r && a && !i[a.__uid__]) {
          for (var o = a.getTextureUniforms(), s = 0; s < o.length; s++) {
            var l = o[s], u = a.uniforms[l].value, f = a.uniforms[l].type;
            if (u) {
              if (f === "t")
                u.dispose && u.dispose(this);
              else if (f === "tv")
                for (var h = 0; h < u.length; h++)
                  u[h] && u[h].dispose && u[h].dispose(this);
            }
          }
          i[a.__uid__] = !0;
        }
        n.dispose && n.dispose(this);
      }, this);
    },
    /**
     * Dispose given geometry
     * @param {clay.Geometry} geometry
     */
    disposeGeometry: function(e) {
      e.dispose(this);
    },
    /**
     * Dispose given texture
     * @param {clay.Texture} texture
     */
    disposeTexture: function(e) {
      e.dispose(this);
    },
    /**
     * Dispose given frame buffer
     * @param {clay.FrameBuffer} frameBuffer
     */
    disposeFrameBuffer: function(e) {
      e.dispose(this);
    },
    /**
     * Dispose renderer
     */
    dispose: function() {
    },
    /**
     * Convert screen coords to normalized device coordinates(NDC)
     * Screen coords can get from mouse event, it is positioned relative to canvas element
     * NDC can be used in ray casting with Camera.prototype.castRay methods
     *
     * @param  {number}       x
     * @param  {number}       y
     * @param  {clay.Vector2} [out]
     * @return {clay.Vector2}
     */
    screenToNDC: function(e, t, r) {
      r || (r = new k()), t = this._height - t;
      var i = this.viewport, n = r.array;
      return n[0] = (e - i.x) / i.width, n[0] = n[0] * 2 - 1, n[1] = (t - i.y) / i.height, n[1] = n[1] * 2 - 1, r;
    }
  }
);
Ve.opaqueSortCompare = Ve.prototype.opaqueSortCompare = function(e, t) {
  return e.renderOrder === t.renderOrder ? e.__program === t.__program ? e.material === t.material ? e.geometry.__uid__ - t.geometry.__uid__ : e.material.__uid__ - t.material.__uid__ : e.__program && t.__program ? e.__program.__uid__ - t.__program.__uid__ : 0 : e.renderOrder - t.renderOrder;
};
Ve.transparentSortCompare = Ve.prototype.transparentSortCompare = function(e, t) {
  return e.renderOrder === t.renderOrder ? e.__depth === t.__depth ? e.__program === t.__program ? e.material === t.material ? e.geometry.__uid__ - t.geometry.__uid__ : e.material.__uid__ - t.material.__uid__ : e.__program && t.__program ? e.__program.__uid__ - t.__program.__uid__ : 0 : e.__depth - t.__depth : e.renderOrder - t.renderOrder;
};
var ne = {
  IDENTITY: ae(),
  WORLD: ae(),
  VIEW: ae(),
  PROJECTION: ae(),
  WORLDVIEW: ae(),
  VIEWPROJECTION: ae(),
  WORLDVIEWPROJECTION: ae(),
  WORLDINVERSE: ae(),
  VIEWINVERSE: ae(),
  PROJECTIONINVERSE: ae(),
  WORLDVIEWINVERSE: ae(),
  VIEWPROJECTIONINVERSE: ae(),
  WORLDVIEWPROJECTIONINVERSE: ae(),
  WORLDTRANSPOSE: ae(),
  VIEWTRANSPOSE: ae(),
  PROJECTIONTRANSPOSE: ae(),
  WORLDVIEWTRANSPOSE: ae(),
  VIEWPROJECTIONTRANSPOSE: ae(),
  WORLDVIEWPROJECTIONTRANSPOSE: ae(),
  WORLDINVERSETRANSPOSE: ae(),
  VIEWINVERSETRANSPOSE: ae(),
  PROJECTIONINVERSETRANSPOSE: ae(),
  WORLDVIEWINVERSETRANSPOSE: ae(),
  VIEWPROJECTIONINVERSETRANSPOSE: ae(),
  WORLDVIEWPROJECTIONINVERSETRANSPOSE: ae()
};
Ve.COLOR_BUFFER_BIT = M.COLOR_BUFFER_BIT;
Ve.DEPTH_BUFFER_BIT = M.DEPTH_BUFFER_BIT;
Ve.STENCIL_BUFFER_BIT = M.STENCIL_BUFFER_BIT;
var or = "__dt__", Zt = function() {
  this._contextId = 0, this._caches = [], this._context = {};
};
Zt.prototype = {
  use: function(e, t) {
    var r = this._caches;
    r[e] || (r[e] = {}, t && (r[e] = t())), this._contextId = e, this._context = r[e];
  },
  put: function(e, t) {
    this._context[e] = t;
  },
  get: function(e) {
    return this._context[e];
  },
  dirty: function(e) {
    e = e || "";
    var t = or + e;
    this.put(t, !0);
  },
  dirtyAll: function(e) {
    e = e || "";
    for (var t = or + e, r = this._caches, i = 0; i < r.length; i++)
      r[i] && (r[i][t] = !0);
  },
  fresh: function(e) {
    e = e || "";
    var t = or + e;
    this.put(t, !1);
  },
  freshAll: function(e) {
    e = e || "";
    for (var t = or + e, r = this._caches, i = 0; i < r.length; i++)
      r[i] && (r[i][t] = !1);
  },
  isDirty: function(e) {
    e = e || "";
    var t = or + e, r = this._context;
    return !r.hasOwnProperty(t) || r[t] === !0;
  },
  deleteContext: function(e) {
    delete this._caches[e], this._context = {};
  },
  delete: function(e) {
    delete this._context[e];
  },
  clearAll: function() {
    this._caches = {};
  },
  getContext: function() {
    return this._context;
  },
  eachContext: function(e, t) {
    var r = Object.keys(this._caches);
    r.forEach(function(i) {
      e && e.call(t, i);
    });
  },
  miss: function(e) {
    return !this._context.hasOwnProperty(e);
  }
};
Zt.prototype.constructor = Zt;
var N = Ae.extend(
  /** @lends clay.Texture# */
  {
    /**
     * Texture width, readonly when the texture source is image
     * @type {number}
     */
    width: 512,
    /**
     * Texture height, readonly when the texture source is image
     * @type {number}
     */
    height: 512,
    /**
     * Texel data type.
     * Possible values:
     *  + {@link clay.Texture.UNSIGNED_BYTE}
     *  + {@link clay.Texture.HALF_FLOAT}
     *  + {@link clay.Texture.FLOAT}
     *  + {@link clay.Texture.UNSIGNED_INT_24_8_WEBGL}
     *  + {@link clay.Texture.UNSIGNED_INT}
     * @type {number}
     */
    type: M.UNSIGNED_BYTE,
    /**
     * Format of texel data
     * Possible values:
     *  + {@link clay.Texture.RGBA}
     *  + {@link clay.Texture.DEPTH_COMPONENT}
     *  + {@link clay.Texture.DEPTH_STENCIL}
     * @type {number}
     */
    format: M.RGBA,
    /**
     * Texture wrap. Default to be REPEAT.
     * Possible values:
     *  + {@link clay.Texture.CLAMP_TO_EDGE}
     *  + {@link clay.Texture.REPEAT}
     *  + {@link clay.Texture.MIRRORED_REPEAT}
     * @type {number}
     */
    wrapS: M.REPEAT,
    /**
     * Texture wrap. Default to be REPEAT.
     * Possible values:
     *  + {@link clay.Texture.CLAMP_TO_EDGE}
     *  + {@link clay.Texture.REPEAT}
     *  + {@link clay.Texture.MIRRORED_REPEAT}
     * @type {number}
     */
    wrapT: M.REPEAT,
    /**
     * Possible values:
     *  + {@link clay.Texture.NEAREST}
     *  + {@link clay.Texture.LINEAR}
     *  + {@link clay.Texture.NEAREST_MIPMAP_NEAREST}
     *  + {@link clay.Texture.LINEAR_MIPMAP_NEAREST}
     *  + {@link clay.Texture.NEAREST_MIPMAP_LINEAR}
     *  + {@link clay.Texture.LINEAR_MIPMAP_LINEAR}
     * @type {number}
     */
    minFilter: M.LINEAR_MIPMAP_LINEAR,
    /**
     * Possible values:
     *  + {@link clay.Texture.NEAREST}
     *  + {@link clay.Texture.LINEAR}
     * @type {number}
     */
    magFilter: M.LINEAR,
    /**
     * If enable mimap.
     * @type {boolean}
     */
    useMipmap: !0,
    /**
     * Anisotropic filtering, enabled if value is larger than 1
     * @see https://developer.mozilla.org/en-US/docs/Web/API/EXT_texture_filter_anisotropic
     * @type {number}
     */
    anisotropic: 1,
    // pixelStorei parameters, not available when texture is used as render target
    // http://www.khronos.org/opengles/sdk/docs/man/xhtml/glPixelStorei.xml
    /**
     * If flip in y axis for given image source
     * @type {boolean}
     * @default true
     */
    flipY: !0,
    /**
     * A flag to indicate if texture source is sRGB
     */
    sRGB: !0,
    /**
     * @type {number}
     * @default 4
     */
    unpackAlignment: 4,
    /**
     * @type {boolean}
     * @default false
     */
    premultiplyAlpha: !1,
    /**
     * Dynamic option for texture like video
     * @type {boolean}
     */
    dynamic: !1,
    NPOT: !1,
    // PENDING
    // Init it here to avoid deoptimization when it's assigned in application dynamically
    __used: 0
  },
  function() {
    this._cache = new Zt();
  },
  /** @lends clay.Texture.prototype */
  {
    getWebGLTexture: function(e) {
      var t = e.gl, r = this._cache;
      return r.use(e.__uid__), r.miss("webgl_texture") && r.put("webgl_texture", t.createTexture()), this.dynamic ? this.update(e) : r.isDirty() && (this.update(e), r.fresh()), r.get("webgl_texture");
    },
    bind: function() {
    },
    unbind: function() {
    },
    /**
     * Mark texture is dirty and update in the next frame
     */
    dirty: function() {
      this._cache && this._cache.dirtyAll();
    },
    update: function(e) {
    },
    // Update the common parameters of texture
    updateCommon: function(e) {
      var t = e.gl;
      t.pixelStorei(t.UNPACK_FLIP_Y_WEBGL, this.flipY), t.pixelStorei(t.UNPACK_PREMULTIPLY_ALPHA_WEBGL, this.premultiplyAlpha), t.pixelStorei(t.UNPACK_ALIGNMENT, this.unpackAlignment), this.format === M.DEPTH_COMPONENT && (this.useMipmap = !1);
      var r = e.getGLExtension("EXT_sRGB");
      this.format === N.SRGB && !r && (this.format = N.RGB), this.format === N.SRGB_ALPHA && !r && (this.format = N.RGBA), this.NPOT = !this.isPowerOfTwo();
    },
    getAvailableWrapS: function() {
      return this.NPOT ? M.CLAMP_TO_EDGE : this.wrapS;
    },
    getAvailableWrapT: function() {
      return this.NPOT ? M.CLAMP_TO_EDGE : this.wrapT;
    },
    getAvailableMinFilter: function() {
      var e = this.minFilter;
      return this.NPOT || !this.useMipmap ? e === M.NEAREST_MIPMAP_NEAREST || e === M.NEAREST_MIPMAP_LINEAR ? M.NEAREST : e === M.LINEAR_MIPMAP_LINEAR || e === M.LINEAR_MIPMAP_NEAREST ? M.LINEAR : e : e;
    },
    getAvailableMagFilter: function() {
      return this.magFilter;
    },
    nextHighestPowerOfTwo: function(e) {
      --e;
      for (var t = 1; t < 32; t <<= 1)
        e = e | e >> t;
      return e + 1;
    },
    /**
     * @param  {clay.Renderer} renderer
     */
    dispose: function(e) {
      var t = this._cache;
      t.use(e.__uid__);
      var r = t.get("webgl_texture");
      r && e.gl.deleteTexture(r), t.deleteContext(e.__uid__);
    },
    /**
     * Test if image of texture is valid and loaded.
     * @return {boolean}
     */
    isRenderable: function() {
    },
    /**
     * Test if texture size is power of two
     * @return {boolean}
     */
    isPowerOfTwo: function() {
    }
  }
);
Object.defineProperty(N.prototype, "width", {
  get: function() {
    return this._width;
  },
  set: function(e) {
    this._width = e;
  }
});
Object.defineProperty(N.prototype, "height", {
  get: function() {
    return this._height;
  },
  set: function(e) {
    this._height = e;
  }
});
N.BYTE = M.BYTE;
N.UNSIGNED_BYTE = M.UNSIGNED_BYTE;
N.SHORT = M.SHORT;
N.UNSIGNED_SHORT = M.UNSIGNED_SHORT;
N.INT = M.INT;
N.UNSIGNED_INT = M.UNSIGNED_INT;
N.FLOAT = M.FLOAT;
N.HALF_FLOAT = 36193;
N.UNSIGNED_INT_24_8_WEBGL = 34042;
N.DEPTH_COMPONENT = M.DEPTH_COMPONENT;
N.DEPTH_STENCIL = M.DEPTH_STENCIL;
N.ALPHA = M.ALPHA;
N.RGB = M.RGB;
N.RGBA = M.RGBA;
N.LUMINANCE = M.LUMINANCE;
N.LUMINANCE_ALPHA = M.LUMINANCE_ALPHA;
N.SRGB = 35904;
N.SRGB_ALPHA = 35906;
N.COMPRESSED_RGB_S3TC_DXT1_EXT = 33776;
N.COMPRESSED_RGBA_S3TC_DXT1_EXT = 33777;
N.COMPRESSED_RGBA_S3TC_DXT3_EXT = 33778;
N.COMPRESSED_RGBA_S3TC_DXT5_EXT = 33779;
N.NEAREST = M.NEAREST;
N.LINEAR = M.LINEAR;
N.NEAREST_MIPMAP_NEAREST = M.NEAREST_MIPMAP_NEAREST;
N.LINEAR_MIPMAP_NEAREST = M.LINEAR_MIPMAP_NEAREST;
N.NEAREST_MIPMAP_LINEAR = M.NEAREST_MIPMAP_LINEAR;
N.LINEAR_MIPMAP_LINEAR = M.LINEAR_MIPMAP_LINEAR;
N.REPEAT = M.REPEAT;
N.CLAMP_TO_EDGE = M.CLAMP_TO_EDGE;
N.MIRRORED_REPEAT = M.MIRRORED_REPEAT;
var Sr = {};
Sr.isPowerOfTwo = function(e) {
  return (e & e - 1) === 0;
};
Sr.nextPowerOfTwo = function(e) {
  return e--, e |= e >> 1, e |= e >> 2, e |= e >> 4, e |= e >> 8, e |= e >> 16, e++, e;
};
Sr.nearestPowerOfTwo = function(e) {
  return Math.pow(2, Math.round(Math.log(e) / Math.LN2));
};
var Da = Sr.isPowerOfTwo;
function Pa(e) {
  return Math.pow(2, Math.round(Math.log(e) / Math.LN2));
}
function Hu(e, t) {
  var r = Pa(e.width), i = Pa(e.height);
  t = t || document.createElement("canvas"), t.width = r, t.height = i;
  var n = t.getContext("2d");
  return n.drawImage(e.image, 0, 0, r, i), t;
}
var K = N.extend(function() {
  return (
    /** @lends clay.Texture2D# */
    {
      /**
       * @type {?HTMLImageElement|HTMLCanvasElemnet}
       */
      // TODO mark dirty when assigned.
      image: null,
      /**
       * Pixels data. Will be ignored if image is set.
       * @type {?Uint8Array|Float32Array}
       */
      pixels: null,
      /**
       * @type {Array.<Object>}
       * @example
       *     [{
       *         image: mipmap0,
       *         pixels: null
       *     }, {
       *         image: mipmap1,
       *         pixels: null
       *     }, ....]
       */
      mipmaps: [],
      /**
       * If convert texture to power-of-two
       * @type {boolean}
       */
      convertToPOT: !1
    }
  );
}, {
  textureType: "texture2D",
  update: function(e) {
    var t = e.gl;
    t.bindTexture(t.TEXTURE_2D, this._cache.get("webgl_texture")), this.updateCommon(e);
    var r = this.format, i = this.type, n = !!(this.convertToPOT && !this.mipmaps.length && this.image && (this.wrapS === N.REPEAT || this.wrapT === N.REPEAT) && this.NPOT);
    t.texParameteri(t.TEXTURE_2D, t.TEXTURE_WRAP_S, n ? this.wrapS : this.getAvailableWrapS()), t.texParameteri(t.TEXTURE_2D, t.TEXTURE_WRAP_T, n ? this.wrapT : this.getAvailableWrapT()), t.texParameteri(t.TEXTURE_2D, t.TEXTURE_MAG_FILTER, n ? this.magFilter : this.getAvailableMagFilter()), t.texParameteri(t.TEXTURE_2D, t.TEXTURE_MIN_FILTER, n ? this.minFilter : this.getAvailableMinFilter());
    var a = e.getGLExtension("EXT_texture_filter_anisotropic");
    if (a && this.anisotropic > 1 && t.texParameterf(t.TEXTURE_2D, a.TEXTURE_MAX_ANISOTROPY_EXT, this.anisotropic), i === 36193) {
      var o = e.getGLExtension("OES_texture_half_float");
      o || (i = M.FLOAT);
    }
    if (this.mipmaps.length)
      for (var s = this.width, l = this.height, u = 0; u < this.mipmaps.length; u++) {
        var f = this.mipmaps[u];
        this._updateTextureData(t, f, u, s, l, r, i, !1), s /= 2, l /= 2;
      }
    else
      this._updateTextureData(t, this, 0, this.width, this.height, r, i, n), this.useMipmap && (!this.NPOT || n) && t.generateMipmap(t.TEXTURE_2D);
    t.bindTexture(t.TEXTURE_2D, null);
  },
  _updateTextureData: function(e, t, r, i, n, a, o, s) {
    if (t.image) {
      var l = t.image;
      s && (this._potCanvas = Hu(this, this._potCanvas), l = this._potCanvas), e.texImage2D(e.TEXTURE_2D, r, a, a, o, l);
    } else
      a <= N.COMPRESSED_RGBA_S3TC_DXT5_EXT && a >= N.COMPRESSED_RGB_S3TC_DXT1_EXT ? e.compressedTexImage2D(e.TEXTURE_2D, r, a, i, n, 0, t.pixels) : e.texImage2D(e.TEXTURE_2D, r, a, i, n, 0, a, o, t.pixels);
  },
  /**
   * @param  {clay.Renderer} renderer
   * @memberOf clay.Texture2D.prototype
   */
  generateMipmap: function(e) {
    var t = e.gl;
    this.useMipmap && !this.NPOT && (t.bindTexture(t.TEXTURE_2D, this._cache.get("webgl_texture")), t.generateMipmap(t.TEXTURE_2D));
  },
  isPowerOfTwo: function() {
    return Da(this.width) && Da(this.height);
  },
  isRenderable: function() {
    return this.image ? this.image.width > 0 && this.image.height > 0 : !!(this.width && this.height);
  },
  bind: function(e) {
    e.gl.bindTexture(e.gl.TEXTURE_2D, this.getWebGLTexture(e));
  },
  unbind: function(e) {
    e.gl.bindTexture(e.gl.TEXTURE_2D, null);
  },
  load: function(e, t) {
    var r = Q.createImage();
    t && (r.crossOrigin = t);
    var i = this;
    return r.onload = function() {
      i.dirty(), i.trigger("success", i);
    }, r.onerror = function() {
      i.trigger("error", i);
    }, r.src = e, this.image = r, this;
  }
});
Object.defineProperty(K.prototype, "width", {
  get: function() {
    return this.image ? this.image.width : this._width;
  },
  set: function(e) {
    this.image ? console.warn("Texture from image can't set width") : (this._width !== e && this.dirty(), this._width = e);
  }
});
Object.defineProperty(K.prototype, "height", {
  get: function() {
    return this.image ? this.image.height : this._height;
  },
  set: function(e) {
    this.image ? console.warn("Texture from image can't set height") : (this._height !== e && this.dirty(), this._height = e);
  }
});
function Io(e) {
  return {
    byte: Q.Int8Array,
    ubyte: Q.Uint8Array,
    short: Q.Int16Array,
    ushort: Q.Uint16Array
  }[e] || Q.Float32Array;
}
function Hi(e) {
  return "attr_" + e;
}
function Yt(e, t, r, i) {
  switch (this.name = e, this.type = t, this.size = r, this.semantic = i || "", this.value = null, r) {
    case 1:
      this.get = function(n) {
        return this.value[n];
      }, this.set = function(n, a) {
        this.value[n] = a;
      }, this.copy = function(n, a) {
        this.value[n] = this.value[n];
      };
      break;
    case 2:
      this.get = function(n, a) {
        var o = this.value;
        return a[0] = o[n * 2], a[1] = o[n * 2 + 1], a;
      }, this.set = function(n, a) {
        var o = this.value;
        o[n * 2] = a[0], o[n * 2 + 1] = a[1];
      }, this.copy = function(n, a) {
        var o = this.value;
        a *= 2, n *= 2, o[n] = o[a], o[n + 1] = o[a + 1];
      };
      break;
    case 3:
      this.get = function(n, a) {
        var o = n * 3, s = this.value;
        return a[0] = s[o], a[1] = s[o + 1], a[2] = s[o + 2], a;
      }, this.set = function(n, a) {
        var o = n * 3, s = this.value;
        s[o] = a[0], s[o + 1] = a[1], s[o + 2] = a[2];
      }, this.copy = function(n, a) {
        var o = this.value;
        a *= 3, n *= 3, o[n] = o[a], o[n + 1] = o[a + 1], o[n + 2] = o[a + 2];
      };
      break;
    case 4:
      this.get = function(n, a) {
        var o = this.value, s = n * 4;
        return a[0] = o[s], a[1] = o[s + 1], a[2] = o[s + 2], a[3] = o[s + 3], a;
      }, this.set = function(n, a) {
        var o = this.value, s = n * 4;
        o[s] = a[0], o[s + 1] = a[1], o[s + 2] = a[2], o[s + 3] = a[3];
      }, this.copy = function(n, a) {
        var o = this.value;
        a *= 4, n *= 4, o[n] = o[a], o[n + 1] = o[a + 1], o[n + 2] = o[a + 2], o[n + 3] = o[a + 3];
      };
  }
}
Yt.prototype.init = function(e) {
  if (!this.value || this.value.length !== e * this.size) {
    var t = Io(this.type);
    this.value = new t(e * this.size);
  }
};
Yt.prototype.fromArray = function(e) {
  var t = Io(this.type), r;
  if (e[0] && e[0].length) {
    var i = 0, n = this.size;
    r = new t(e.length * n);
    for (var a = 0; a < e.length; a++)
      for (var o = 0; o < n; o++)
        r[i++] = e[a][o];
  } else
    r = new t(e);
  this.value = r;
};
Yt.prototype.clone = function(e) {
  var t = new Yt(this.name, this.type, this.size, this.semantic);
  return e && console.warn("todo"), t;
};
function Bo(e, t, r, i, n) {
  this.name = e, this.type = t, this.buffer = r, this.size = i, this.semantic = n, this.symbol = "", this.needsRemove = !1;
}
function Fo(e) {
  this.buffer = e, this.count = 0;
}
var Ne = Ae.extend(
  function() {
    return (
      /** @lends clay.GeometryBase# */
      {
        /**
         * Attributes of geometry.
         * @type {Object.<string, clay.GeometryBase.Attribute>}
         */
        attributes: {},
        /**
         * Indices of geometry.
         * @type {Uint16Array|Uint32Array}
         */
        indices: null,
        /**
         * Is vertices data dynamically updated.
         * Attributes value can't be changed after first render if dyanmic is false.
         * @type {boolean}
         */
        dynamic: !0,
        _enabledAttributes: null,
        // PENDING
        // Init it here to avoid deoptimization when it's assigned in application dynamically
        __used: 0
      }
    );
  },
  function() {
    this._cache = new Zt(), this._attributeList = Object.keys(this.attributes), this.__vaoCache = {};
  },
  /** @lends clay.GeometryBase.prototype */
  {
    /**
     * Main attribute will be used to count vertex number
     * @type {string}
     */
    mainAttribute: "",
    /**
     * User defined picking algorithm instead of default
     * triangle ray intersection
     * x, y are NDC.
     * ```typescript
     * (x, y, renderer, camera, renderable, out) => boolean
     * ```
     * @type {?Function}
     */
    pick: null,
    /**
     * User defined ray picking algorithm instead of default
     * triangle ray intersection
     * ```typescript
     * (ray: clay.Ray, renderable: clay.Renderable, out: Array) => boolean
     * ```
     * @type {?Function}
     */
    pickByRay: null,
    /**
     * Mark attributes and indices in geometry needs to update.
     * Usually called after you change the data in attributes.
     */
    dirty: function() {
      for (var e = this.getEnabledAttributes(), t = 0; t < e.length; t++)
        this.dirtyAttribute(e[t]);
      this.dirtyIndices(), this._enabledAttributes = null, this._cache.dirty("any");
    },
    /**
     * Mark the indices needs to update.
     */
    dirtyIndices: function() {
      this._cache.dirtyAll("indices");
    },
    /**
     * Mark the attributes needs to update.
     * @param {string} [attrName]
     */
    dirtyAttribute: function(e) {
      this._cache.dirtyAll(Hi(e)), this._cache.dirtyAll("attributes");
    },
    /**
     * Get indices of triangle at given index.
     * @param {number} idx
     * @param {Array.<number>} out
     * @return {Array.<number>}
     */
    getTriangleIndices: function(e, t) {
      if (e < this.triangleCount && e >= 0) {
        t || (t = []);
        var r = this.indices;
        return t[0] = r[e * 3], t[1] = r[e * 3 + 1], t[2] = r[e * 3 + 2], t;
      }
    },
    /**
     * Set indices of triangle at given index.
     * @param {number} idx
     * @param {Array.<number>} arr
     */
    setTriangleIndices: function(e, t) {
      var r = this.indices;
      r[e * 3] = t[0], r[e * 3 + 1] = t[1], r[e * 3 + 2] = t[2];
    },
    isUseIndices: function() {
      return !!this.indices;
    },
    /**
     * Initialize indices from an array.
     * @param {Array} array
     */
    initIndicesFromArray: function(e) {
      var t, r = this.vertexCount > 65535 ? Q.Uint32Array : Q.Uint16Array;
      if (e[0] && e[0].length) {
        var i = 0, n = 3;
        t = new r(e.length * n);
        for (var a = 0; a < e.length; a++)
          for (var o = 0; o < n; o++)
            t[i++] = e[a][o];
      } else
        t = new r(e);
      this.indices = t;
    },
    /**
     * Create a new attribute
     * @param {string} name
     * @param {string} type
     * @param {number} size
     * @param {string} [semantic]
     */
    createAttribute: function(e, t, r, i) {
      var n = new Yt(e, t, r, i);
      return this.attributes[e] && this.removeAttribute(e), this.attributes[e] = n, this._attributeList.push(e), n;
    },
    /**
     * Remove attribute
     * @param {string} name
     */
    removeAttribute: function(e) {
      var t = this._attributeList, r = t.indexOf(e);
      return r >= 0 ? (t.splice(r, 1), delete this.attributes[e], !0) : !1;
    },
    /**
     * Get attribute
     * @param {string} name
     * @return {clay.GeometryBase.Attribute}
     */
    getAttribute: function(e) {
      return this.attributes[e];
    },
    /**
     * Get enabled attributes name list
     * Attribute which has the same vertex number with position is treated as a enabled attribute
     * @return {string[]}
     */
    getEnabledAttributes: function() {
      var e = this._enabledAttributes, t = this._attributeList;
      if (e)
        return e;
      for (var r = [], i = this.vertexCount, n = 0; n < t.length; n++) {
        var a = t[n], o = this.attributes[a];
        o.value && o.value.length === i * o.size && r.push(a);
      }
      return this._enabledAttributes = r, r;
    },
    getBufferChunks: function(e) {
      var t = this._cache;
      t.use(e.__uid__);
      var r = t.isDirty("attributes"), i = t.isDirty("indices");
      if (r || i) {
        this._updateBuffer(e.gl, r, i);
        for (var n = this.getEnabledAttributes(), a = 0; a < n.length; a++)
          t.fresh(Hi(n[a]));
        t.fresh("attributes"), t.fresh("indices");
      }
      return t.fresh("any"), t.get("chunks");
    },
    _updateBuffer: function(e, t, r) {
      var i = this._cache, n = i.get("chunks"), a = !1;
      n || (n = [], n[0] = {
        attributeBuffers: [],
        indicesBuffer: null
      }, i.put("chunks", n), a = !0);
      var o = n[0], s = o.attributeBuffers, l = o.indicesBuffer;
      if (t || a) {
        var u = this.getEnabledAttributes(), f = {};
        if (!a)
          for (var h = 0; h < s.length; h++)
            f[s[h].name] = s[h];
        for (var d = 0; d < u.length; d++) {
          var c = u[d], v = this.attributes[c], p;
          a || (p = f[c]);
          var m;
          p ? m = p.buffer : m = e.createBuffer(), i.isDirty(Hi(c)) && (e.bindBuffer(e.ARRAY_BUFFER, m), e.bufferData(e.ARRAY_BUFFER, v.value, this.dynamic ? e.DYNAMIC_DRAW : e.STATIC_DRAW)), s[d] = new Bo(c, v.type, m, v.size, v.semantic);
        }
        for (var h = d; h < s.length; h++)
          e.deleteBuffer(s[h].buffer);
        s.length = d;
      }
      this.isUseIndices() && (r || a) && (l || (l = new Fo(e.createBuffer()), o.indicesBuffer = l), l.count = this.indices.length, e.bindBuffer(e.ELEMENT_ARRAY_BUFFER, l.buffer), e.bufferData(e.ELEMENT_ARRAY_BUFFER, this.indices, this.dynamic ? e.DYNAMIC_DRAW : e.STATIC_DRAW));
    },
    /**
     * Dispose geometry data in GL context.
     * @param {clay.Renderer} renderer
     */
    dispose: function(e) {
      var t = this._cache;
      t.use(e.__uid__);
      var r = t.get("chunks");
      if (r)
        for (var i = 0; i < r.length; i++) {
          for (var n = r[i], a = 0; a < n.attributeBuffers.length; a++) {
            var o = n.attributeBuffers[a];
            e.gl.deleteBuffer(o.buffer);
          }
          n.indicesBuffer && e.gl.deleteBuffer(n.indicesBuffer.buffer);
        }
      if (this.__vaoCache) {
        var s = e.getGLExtension("OES_vertex_array_object");
        for (var l in this.__vaoCache) {
          var u = this.__vaoCache[l].vao;
          u && s.deleteVertexArrayOES(u);
        }
      }
      this.__vaoCache = {}, t.deleteContext(e.__uid__);
    }
  }
);
Object.defineProperty && (Object.defineProperty(Ne.prototype, "vertexCount", {
  enumerable: !1,
  get: function() {
    var e = this.attributes[this.mainAttribute];
    return e || (e = this.attributes[this._attributeList[0]]), !e || !e.value ? 0 : e.value.length / e.size;
  }
}), Object.defineProperty(Ne.prototype, "triangleCount", {
  enumerable: !1,
  get: function() {
    var e = this.indices;
    return e ? e.length / 3 : 0;
  }
}));
Ne.STATIC_DRAW = M.STATIC_DRAW;
Ne.DYNAMIC_DRAW = M.DYNAMIC_DRAW;
Ne.STREAM_DRAW = M.STREAM_DRAW;
Ne.AttributeBuffer = Bo;
Ne.IndicesBuffer = Fo;
Ne.Attribute = Yt;
var Le = T.create, Ut = T.add, wt = T.set, Qe = Ne.Attribute, $ = Ne.extend(
  function() {
    return (
      /** @lends clay.Geometry# */
      {
        /**
         * Attributes of geometry. Including:
         *  + `position`
         *  + `texcoord0`
         *  + `texcoord1`
         *  + `normal`
         *  + `tangent`
         *  + `color`
         *  + `weight`
         *  + `joint`
         *  + `barycentric`
         *
         * @type {Object.<string, clay.Geometry.Attribute>}
         */
        attributes: {
          position: new Qe("position", "float", 3, "POSITION"),
          texcoord0: new Qe("texcoord0", "float", 2, "TEXCOORD_0"),
          texcoord1: new Qe("texcoord1", "float", 2, "TEXCOORD_1"),
          normal: new Qe("normal", "float", 3, "NORMAL"),
          tangent: new Qe("tangent", "float", 4, "TANGENT"),
          color: new Qe("color", "float", 4, "COLOR"),
          // Skinning attributes
          // Each vertex can be bind to 4 bones, because the
          // sum of weights is 1, so the weights is stored in vec3 and the last
          // can be calculated by 1-w.x-w.y-w.z
          weight: new Qe("weight", "float", 3, "WEIGHT"),
          joint: new Qe("joint", "float", 4, "JOINT"),
          // For wireframe display
          // http://codeflow.org/entries/2012/aug/02/easy-wireframe-display-with-barycentric-coordinates/
          barycentric: new Qe("barycentric", "float", 3, null)
        },
        /**
         * Calculated bounding box of geometry.
         * @type {clay.BoundingBox}
         */
        boundingBox: null
      }
    );
  },
  /** @lends clay.Geometry.prototype */
  {
    mainAttribute: "position",
    /**
     * Update boundingBox of Geometry
     */
    updateBoundingBox: function() {
      var e = this.boundingBox;
      e || (e = this.boundingBox = new we());
      var t = this.attributes.position.value;
      if (t && t.length) {
        var r = e.min, i = e.max, n = r.array, a = i.array;
        T.set(n, t[0], t[1], t[2]), T.set(a, t[0], t[1], t[2]);
        for (var o = 3; o < t.length; ) {
          var s = t[o++], l = t[o++], u = t[o++];
          s < n[0] && (n[0] = s), l < n[1] && (n[1] = l), u < n[2] && (n[2] = u), s > a[0] && (a[0] = s), l > a[1] && (a[1] = l), u > a[2] && (a[2] = u);
        }
        r._dirty = !0, i._dirty = !0;
      }
    },
    /**
     * Generate normals per vertex.
     */
    generateVertexNormals: function() {
      if (this.vertexCount) {
        var e = this.indices, t = this.attributes, r = t.position.value, i = t.normal.value;
        if (!i || i.length !== r.length)
          i = t.normal.value = new Q.Float32Array(r.length);
        else
          for (var n = 0; n < i.length; n++)
            i[n] = 0;
        for (var a = Le(), o = Le(), s = Le(), l = Le(), u = Le(), f = Le(), h = e ? e.length : this.vertexCount, d, c, v, p = 0; p < h; ) {
          e ? (d = e[p++], c = e[p++], v = e[p++]) : (d = p++, c = p++, v = p++), wt(a, r[d * 3], r[d * 3 + 1], r[d * 3 + 2]), wt(o, r[c * 3], r[c * 3 + 1], r[c * 3 + 2]), wt(s, r[v * 3], r[v * 3 + 1], r[v * 3 + 2]), T.sub(l, a, o), T.sub(u, o, s), T.cross(f, l, u);
          for (var n = 0; n < 3; n++)
            i[d * 3 + n] = i[d * 3 + n] + f[n], i[c * 3 + n] = i[c * 3 + n] + f[n], i[v * 3 + n] = i[v * 3 + n] + f[n];
        }
        for (var n = 0; n < i.length; )
          wt(f, i[n], i[n + 1], i[n + 2]), T.normalize(f, f), i[n++] = f[0], i[n++] = f[1], i[n++] = f[2];
        this.dirty();
      }
    },
    /**
     * Generate normals per face.
     */
    generateFaceNormals: function() {
      if (this.vertexCount) {
        this.isUniqueVertex() || this.generateUniqueVertex();
        var e = this.indices, t = this.attributes, r = t.position.value, i = t.normal.value, n = Le(), a = Le(), o = Le(), s = Le(), l = Le(), u = Le();
        i || (i = t.normal.value = new Float32Array(r.length));
        for (var f = e ? e.length : this.vertexCount, h, d, c, v = 0; v < f; ) {
          e ? (h = e[v++], d = e[v++], c = e[v++]) : (h = v++, d = v++, c = v++), wt(n, r[h * 3], r[h * 3 + 1], r[h * 3 + 2]), wt(a, r[d * 3], r[d * 3 + 1], r[d * 3 + 2]), wt(o, r[c * 3], r[c * 3 + 1], r[c * 3 + 2]), T.sub(s, n, a), T.sub(l, a, o), T.cross(u, s, l), T.normalize(u, u);
          for (var p = 0; p < 3; p++)
            i[h * 3 + p] = u[p], i[d * 3 + p] = u[p], i[c * 3 + p] = u[p];
        }
        this.dirty();
      }
    },
    /**
     * Generate tangents attributes.
     */
    generateTangents: function() {
      if (this.vertexCount) {
        var e = this.vertexCount, t = this.attributes;
        t.tangent.value || (t.tangent.value = new Float32Array(e * 4));
        var r = t.texcoord0.value, i = t.position.value, n = t.tangent.value, a = t.normal.value;
        if (!r) {
          console.warn("Geometry without texcoords can't generate tangents.");
          return;
        }
        for (var o = [], s = [], l = 0; l < e; l++)
          o[l] = [0, 0, 0], s[l] = [0, 0, 0];
        for (var u = [0, 0, 0], f = [0, 0, 0], h = this.indices, d = h ? h.length : this.vertexCount, c, v, p, l = 0; l < d; ) {
          h ? (c = h[l++], v = h[l++], p = h[l++]) : (c = l++, v = l++, p = l++);
          var m = r[c * 2], _ = r[v * 2], x = r[p * 2], y = r[c * 2 + 1], g = r[v * 2 + 1], b = r[p * 2 + 1], w = i[c * 3], C = i[v * 3], A = i[p * 3], R = i[c * 3 + 1], H = i[v * 3 + 1], O = i[p * 3 + 1], z = i[c * 3 + 2], G = i[v * 3 + 2], F = i[p * 3 + 2], W = C - w, X = A - w, q = H - R, ie = O - R, me = G - z, Se = F - z, ce = _ - m, ye = x - m, de = g - y, Ce = b - y, Fe = 1 / (ce * Ce - de * ye);
          u[0] = (Ce * W - de * X) * Fe, u[1] = (Ce * q - de * ie) * Fe, u[2] = (Ce * me - de * Se) * Fe, f[0] = (ce * X - ye * W) * Fe, f[1] = (ce * ie - ye * q) * Fe, f[2] = (ce * Se - ye * me) * Fe, Ut(o[c], o[c], u), Ut(o[v], o[v], u), Ut(o[p], o[p], u), Ut(s[c], s[c], f), Ut(s[v], s[v], f), Ut(s[p], s[p], f);
        }
        for (var Ie = Le(), tr = Le(), Ke = Le(), l = 0; l < e; l++) {
          Ke[0] = a[l * 3], Ke[1] = a[l * 3 + 1], Ke[2] = a[l * 3 + 2];
          var Ot = o[l];
          T.scale(Ie, Ke, T.dot(Ke, Ot)), T.sub(Ie, Ot, Ie), T.normalize(Ie, Ie), T.cross(tr, Ke, Ot), n[l * 4] = Ie[0], n[l * 4 + 1] = Ie[1], n[l * 4 + 2] = Ie[2], n[l * 4 + 3] = T.dot(tr, s[l]) < 0 ? -1 : 1;
        }
        this.dirty();
      }
    },
    /**
     * If vertices are not shared by different indices.
     */
    isUniqueVertex: function() {
      return this.isUseIndices() ? this.vertexCount === this.indices.length : !0;
    },
    /**
     * Create a unique vertex for each index.
     */
    generateUniqueVertex: function() {
      if (!(!this.vertexCount || !this.indices)) {
        this.indices.length > 65535 && (this.indices = new Q.Uint32Array(this.indices));
        for (var e = this.attributes, t = this.indices, r = this.getEnabledAttributes(), i = {}, n = 0; n < r.length; n++) {
          var a = r[n];
          i[a] = e[a].value, e[a].init(this.indices.length);
        }
        for (var o = 0, s = 0; s < t.length; s++) {
          for (var l = t[s], n = 0; n < r.length; n++)
            for (var a = r[n], u = e[a].value, f = e[a].size, h = 0; h < f; h++)
              u[o * f + h] = i[a][l * f + h];
          t[s] = o, o++;
        }
        this.dirty();
      }
    },
    /**
     * Generate barycentric coordinates for wireframe draw.
     */
    generateBarycentric: function() {
      if (this.vertexCount) {
        this.isUniqueVertex() || this.generateUniqueVertex();
        var e = this.attributes, t = e.barycentric.value, r = this.indices;
        if (!(t && t.length === r.length * 3)) {
          t = e.barycentric.value = new Float32Array(r.length * 3);
          for (var i = 0; i < (r ? r.length : this.vertexCount / 3); )
            for (var n = 0; n < 3; n++) {
              var a = r ? r[i++] : i * 3 + n;
              t[a * 3 + n] = 1;
            }
          this.dirty();
        }
      }
    },
    /**
     * Apply transform to geometry attributes.
     * @param {clay.Matrix4} matrix
     */
    applyTransform: function(e) {
      var t = this.attributes, r = t.position.value, i = t.normal.value, n = t.tangent.value;
      e = e.array;
      var a = D.create();
      D.invert(a, e), D.transpose(a, a);
      var o = T.transformMat4, s = T.forEach;
      s(r, 3, 0, null, o, e), i && s(i, 3, 0, null, o, a), n && s(n, 4, 0, null, o, a), this.boundingBox && this.updateBoundingBox();
    },
    /**
     * Dispose geometry data in GL context.
     * @param {clay.Renderer} renderer
     */
    dispose: function(e) {
      var t = this._cache;
      t.use(e.__uid__);
      var r = t.get("chunks");
      if (r)
        for (var i = 0; i < r.length; i++) {
          for (var n = r[i], a = 0; a < n.attributeBuffers.length; a++) {
            var o = n.attributeBuffers[a];
            e.gl.deleteBuffer(o.buffer);
          }
          n.indicesBuffer && e.gl.deleteBuffer(n.indicesBuffer.buffer);
        }
      if (this.__vaoCache) {
        var s = e.getGLExtension("OES_vertex_array_object");
        for (var l in this.__vaoCache) {
          var u = this.__vaoCache[l].vao;
          u && s.deleteVertexArrayOES(u);
        }
      }
      this.__vaoCache = {}, t.deleteContext(e.__uid__);
    }
  }
);
$.STATIC_DRAW = Ne.STATIC_DRAW;
$.DYNAMIC_DRAW = Ne.DYNAMIC_DRAW;
$.STREAM_DRAW = Ne.STREAM_DRAW;
$.AttributeBuffer = Ne.AttributeBuffer;
$.IndicesBuffer = Ne.IndicesBuffer;
$.Attribute = Qe;
const Gu = `vec3 calcAmbientSHLight(int idx, vec3 N) {
 int offset = 9 * idx;
 return ambientSHLightCoefficients[0]
 + ambientSHLightCoefficients[1] * N.x
 + ambientSHLightCoefficients[2] * N.y
 + ambientSHLightCoefficients[3] * N.z
 + ambientSHLightCoefficients[4] * N.x * N.z
 + ambientSHLightCoefficients[5] * N.z * N.y
 + ambientSHLightCoefficients[6] * N.y * N.x
 + ambientSHLightCoefficients[7] * (3.0 * N.z * N.z - 1.0)
 + ambientSHLightCoefficients[8] * (N.x * N.x - N.y * N.y);
}`;
var ke = "uniform vec3 ", sr = "uniform float ", zt = "@export clay.header.", Ht = "@end", xe = ":unconfigurable;";
const Vu = [
  zt + "directional_light",
  ke + "directionalLightDirection[DIRECTIONAL_LIGHT_COUNT]" + xe,
  ke + "directionalLightColor[DIRECTIONAL_LIGHT_COUNT]" + xe,
  Ht,
  zt + "ambient_light",
  ke + "ambientLightColor[AMBIENT_LIGHT_COUNT]" + xe,
  Ht,
  zt + "ambient_sh_light",
  ke + "ambientSHLightColor[AMBIENT_SH_LIGHT_COUNT]" + xe,
  ke + "ambientSHLightCoefficients[AMBIENT_SH_LIGHT_COUNT * 9]" + xe,
  Gu,
  Ht,
  zt + "ambient_cubemap_light",
  ke + "ambientCubemapLightColor[AMBIENT_CUBEMAP_LIGHT_COUNT]" + xe,
  "uniform samplerCube ambientCubemapLightCubemap[AMBIENT_CUBEMAP_LIGHT_COUNT]" + xe,
  "uniform sampler2D ambientCubemapLightBRDFLookup[AMBIENT_CUBEMAP_LIGHT_COUNT]" + xe,
  Ht,
  zt + "point_light",
  ke + "pointLightPosition[POINT_LIGHT_COUNT]" + xe,
  sr + "pointLightRange[POINT_LIGHT_COUNT]" + xe,
  ke + "pointLightColor[POINT_LIGHT_COUNT]" + xe,
  Ht,
  zt + "spot_light",
  ke + "spotLightPosition[SPOT_LIGHT_COUNT]" + xe,
  ke + "spotLightDirection[SPOT_LIGHT_COUNT]" + xe,
  sr + "spotLightRange[SPOT_LIGHT_COUNT]" + xe,
  sr + "spotLightUmbraAngleCosine[SPOT_LIGHT_COUNT]" + xe,
  sr + "spotLightPenumbraAngleCosine[SPOT_LIGHT_COUNT]" + xe,
  sr + "spotLightFalloffFactor[SPOT_LIGHT_COUNT]" + xe,
  ke + "spotLightColor[SPOT_LIGHT_COUNT]" + xe,
  Ht
].join(`
`);
U.import(Vu);
var Ze = Je.extend(
  function() {
    return (
      /** @lends clay.Light# */
      {
        /**
         * Light RGB color
         * @type {number[]}
         */
        color: [1, 1, 1],
        /**
         * Light intensity
         * @type {number}
         */
        intensity: 1,
        // Config for shadow map
        /**
         * If light cast shadow
         * @type {boolean}
         */
        castShadow: !0,
        /**
         * Shadow map size
         * @type {number}
         */
        shadowResolution: 512,
        /**
         * Light group, shader with same `lightGroup` will be affected
         *
         * Only useful in forward rendering
         * @type {number}
         */
        group: 0
      }
    );
  },
  /** @lends clay.Light.prototype. */
  {
    /**
     * Light type
     * @type {string}
     * @memberOf clay.Light#
     */
    type: "",
    /**
     * @return {clay.Light}
     * @memberOf clay.Light.prototype
     */
    clone: function() {
      var e = Je.prototype.clone.call(this);
      return e.color = Array.prototype.slice.call(this.color), e.intensity = this.intensity, e.castShadow = this.castShadow, e.shadowResolution = this.shadowResolution, e;
    }
  }
), mr = function(e, t) {
  this.normal = e || new S(0, 1, 0), this.distance = t || 0;
};
mr.prototype = {
  constructor: mr,
  /**
   * Distance from a given point to the plane
   * @param  {clay.Vector3} point
   * @return {number}
   */
  distanceToPoint: function(e) {
    return T.dot(e.array, this.normal.array) - this.distance;
  },
  /**
   * Calculate the projection point on the plane
   * @param  {clay.Vector3} point
   * @param  {clay.Vector3} out
   * @return {clay.Vector3}
   */
  projectPoint: function(e, t) {
    t || (t = new S());
    var r = this.distanceToPoint(e);
    return T.scaleAndAdd(t.array, e.array, this.normal.array, -r), t._dirty = !0, t;
  },
  /**
   * Normalize the plane's normal and calculate the distance
   */
  normalize: function() {
    var e = 1 / T.len(this.normal.array);
    T.scale(this.normal.array, e), this.distance *= e;
  },
  /**
   * If the plane intersect a frustum
   * @param  {clay.Frustum} Frustum
   * @return {boolean}
   */
  intersectFrustum: function(e) {
    for (var t = e.vertices, r = this.normal.array, i = T.dot(t[0].array, r) > this.distance, n = 1; n < 8; n++)
      if (T.dot(t[n].array, r) > this.distance != i)
        return !0;
  },
  /**
   * Calculate the intersection point between plane and a given line
   * @function
   * @param {clay.Vector3} start start point of line
   * @param {clay.Vector3} end end point of line
   * @param {clay.Vector3} [out]
   * @return {clay.Vector3}
   */
  intersectLine: (function() {
    var e = T.create();
    return function(t, r, i) {
      var n = this.distanceToPoint(t), a = this.distanceToPoint(r);
      if (n > 0 && a > 0 || n < 0 && a < 0)
        return null;
      var o = this.normal.array, s = this.distance, l = t.array;
      T.sub(e, r.array, t.array), T.normalize(e, e);
      var u = T.dot(o, e);
      if (u === 0)
        return null;
      i || (i = new S());
      var f = (T.dot(o, l) - s) / u;
      return T.scaleAndAdd(i.array, l, e, -f), i._dirty = !0, i;
    };
  })(),
  /**
   * Apply an affine transform matrix to plane
   * @function
   * @return {clay.Matrix4}
   */
  applyTransform: (function() {
    var e = D.create(), t = L.create(), r = L.create();
    return r[3] = 1, function(i) {
      i = i.array, T.scale(r, this.normal.array, this.distance), L.transformMat4(r, r, i), this.distance = T.dot(r, this.normal.array), D.invert(e, i), D.transpose(e, e), t[3] = 0, T.copy(t, this.normal.array), L.transformMat4(t, t, e), T.copy(this.normal.array, t);
    };
  })(),
  /**
   * Copy from another plane
   * @param  {clay.Vector3} plane
   */
  copy: function(e) {
    T.copy(this.normal.array, e.normal.array), this.normal._dirty = !0, this.distance = e.distance;
  },
  /**
   * Clone a new plane
   * @return {clay.Plane}
   */
  clone: function() {
    var e = new mr();
    return e.copy(this), e;
  }
};
var fe = T.set, Ra = T.copy, Na = T.transformMat4, Gi = Math.min, Vi = Math.max, yi = function() {
  this.planes = [];
  for (var e = 0; e < 6; e++)
    this.planes.push(new mr());
  this.boundingBox = new we(), this.vertices = [];
  for (var e = 0; e < 8; e++)
    this.vertices[e] = T.fromValues(0, 0, 0);
};
yi.prototype = {
  // http://web.archive.org/web/20120531231005/http://crazyjoke.free.fr/doc/3D/plane%20extraction.pdf
  /**
   * Set frustum from a projection matrix
   * @param {clay.Matrix4} projectionMatrix
   */
  setFromProjection: function(e) {
    var t = this.planes, r = e.array, i = r[0], n = r[1], a = r[2], o = r[3], s = r[4], l = r[5], u = r[6], f = r[7], h = r[8], d = r[9], c = r[10], v = r[11], p = r[12], m = r[13], _ = r[14], x = r[15];
    fe(t[0].normal.array, o - i, f - s, v - h), t[0].distance = -(x - p), t[0].normalize(), fe(t[1].normal.array, o + i, f + s, v + h), t[1].distance = -(x + p), t[1].normalize(), fe(t[2].normal.array, o + n, f + l, v + d), t[2].distance = -(x + m), t[2].normalize(), fe(t[3].normal.array, o - n, f - l, v - d), t[3].distance = -(x - m), t[3].normalize(), fe(t[4].normal.array, o - a, f - u, v - c), t[4].distance = -(x - _), t[4].normalize(), fe(t[5].normal.array, o + a, f + u, v + c), t[5].distance = -(x + _), t[5].normalize();
    var y = this.boundingBox, g = this.vertices;
    if (x === 0) {
      var b = l / i, w = -_ / (c - 1), C = -_ / (c + 1), A = -C / l, R = -w / l;
      y.min.set(-A * b, -A, C), y.max.set(A * b, A, w), fe(g[0], -A * b, -A, C), fe(g[1], -A * b, A, C), fe(g[2], A * b, -A, C), fe(g[3], A * b, A, C), fe(g[4], -R * b, -R, w), fe(g[5], -R * b, R, w), fe(g[6], R * b, -R, w), fe(g[7], R * b, R, w);
    } else {
      var H = (-1 - p) / i, O = (1 - p) / i, z = (1 - m) / l, G = (-1 - m) / l, F = (-1 - _) / c, W = (1 - _) / c;
      y.min.set(Math.min(H, O), Math.min(G, z), Math.min(W, F)), y.max.set(Math.max(O, H), Math.max(z, G), Math.max(F, W));
      var X = y.min.array, q = y.max.array;
      fe(g[0], X[0], X[1], X[2]), fe(g[1], X[0], q[1], X[2]), fe(g[2], q[0], X[1], X[2]), fe(g[3], q[0], q[1], X[2]), fe(g[4], X[0], X[1], q[2]), fe(g[5], X[0], q[1], q[2]), fe(g[6], q[0], X[1], q[2]), fe(g[7], q[0], q[1], q[2]);
    }
  },
  /**
   * Apply a affine transform matrix and set to the given bounding box
   * @function
   * @param {clay.BoundingBox}
   * @param {clay.Matrix4}
   * @return {clay.BoundingBox}
   */
  getTransformedBoundingBox: (function() {
    var e = T.create();
    return function(t, r) {
      var i = this.vertices, n = r.array, a = t.min, o = t.max, s = a.array, l = o.array, u = i[0];
      Na(e, u, n), Ra(s, e), Ra(l, e);
      for (var f = 1; f < 8; f++)
        u = i[f], Na(e, u, n), s[0] = Gi(e[0], s[0]), s[1] = Gi(e[1], s[1]), s[2] = Gi(e[2], s[2]), l[0] = Vi(e[0], l[0]), l[1] = Vi(e[1], l[1]), l[2] = Vi(e[2], l[2]);
      return a._dirty = !0, o._dirty = !0, t;
    };
  })()
};
var ki = 1e-5, Pt = function(e, t) {
  this.origin = e || new S(), this.direction = t || new S();
};
Pt.prototype = {
  constructor: Pt,
  // http://www.siggraph.org/education/materials/HyperGraph/raytrace/rayplane_intersection.htm
  /**
   * Calculate intersection point between ray and a give plane
   * @param  {clay.Plane} plane
   * @param  {clay.Vector3} [out]
   * @return {clay.Vector3}
   */
  intersectPlane: function(e, t) {
    var r = e.normal.array, i = e.distance, n = this.origin.array, a = this.direction.array, o = T.dot(r, a);
    if (o === 0)
      return null;
    t || (t = new S());
    var s = (T.dot(r, n) - i) / o;
    return T.scaleAndAdd(t.array, n, a, -s), t._dirty = !0, t;
  },
  /**
   * Mirror the ray against plane
   * @param  {clay.Plane} plane
   */
  mirrorAgainstPlane: function(e) {
    var t = T.dot(e.normal.array, this.direction.array);
    T.scaleAndAdd(this.direction.array, this.direction.array, e.normal.array, -t * 2), this.direction._dirty = !0;
  },
  distanceToPoint: (function() {
    var e = T.create();
    return function(t) {
      T.sub(e, t, this.origin.array);
      var r = T.dot(e, this.direction.array);
      if (r < 0)
        return T.distance(this.origin.array, t);
      var i = T.lenSquared(e);
      return Math.sqrt(i - r * r);
    };
  })(),
  /**
   * Calculate intersection point between ray and sphere
   * @param  {clay.Vector3} center
   * @param  {number} radius
   * @param  {clay.Vector3} out
   * @return {clay.Vector3}
   */
  intersectSphere: (function() {
    var e = T.create();
    return function(t, r, i) {
      var n = this.origin.array, a = this.direction.array;
      t = t.array, T.sub(e, t, n);
      var o = T.dot(e, a), s = T.squaredLength(e), l = s - o * o, u = r * r;
      if (!(l > u)) {
        var f = Math.sqrt(u - l), h = o - f, d = o + f;
        return i || (i = new S()), h < 0 ? d < 0 ? null : (T.scaleAndAdd(i.array, n, a, d), i) : (T.scaleAndAdd(i.array, n, a, h), i);
      }
    };
  })(),
  // http://www.scratchapixel.com/lessons/3d-basic-lessons/lesson-7-intersecting-simple-shapes/ray-box-intersection/
  /**
   * Calculate intersection point between ray and bounding box
   * @param {clay.BoundingBox} bbox
   * @param {clay.Vector3}
   * @return {clay.Vector3}
   */
  intersectBoundingBox: function(e, t) {
    var r = this.direction.array, i = this.origin.array, n = e.min.array, a = e.max.array, o = 1 / r[0], s = 1 / r[1], l = 1 / r[2], u, f, h, d, c, v;
    if (o >= 0 ? (u = (n[0] - i[0]) * o, f = (a[0] - i[0]) * o) : (f = (n[0] - i[0]) * o, u = (a[0] - i[0]) * o), s >= 0 ? (h = (n[1] - i[1]) * s, d = (a[1] - i[1]) * s) : (d = (n[1] - i[1]) * s, h = (a[1] - i[1]) * s), u > d || h > f || ((h > u || u !== u) && (u = h), (d < f || f !== f) && (f = d), l >= 0 ? (c = (n[2] - i[2]) * l, v = (a[2] - i[2]) * l) : (v = (n[2] - i[2]) * l, c = (a[2] - i[2]) * l), u > v || c > f) || ((c > u || u !== u) && (u = c), (v < f || f !== f) && (f = v), f < 0))
      return null;
    var p = u >= 0 ? u : f;
    return t || (t = new S()), T.scaleAndAdd(t.array, i, r, p), t;
  },
  // http://en.wikipedia.org/wiki/M%C3%B6ller%E2%80%93Trumbore_intersection_algorithm
  /**
   * Calculate intersection point between ray and three triangle vertices
   * @param {clay.Vector3} a
   * @param {clay.Vector3} b
   * @param {clay.Vector3} c
   * @param {boolean}           singleSided, CW triangle will be ignored
   * @param {clay.Vector3} [out]
   * @param {clay.Vector3} [barycenteric] barycentric coords
   * @return {clay.Vector3}
   */
  intersectTriangle: (function() {
    var e = T.create(), t = T.create(), r = T.create(), i = T.create();
    return function(n, a, o, s, l, u) {
      var f = this.direction.array, h = this.origin.array;
      n = n.array, a = a.array, o = o.array, T.sub(e, a, n), T.sub(t, o, n), T.cross(i, t, f);
      var d = T.dot(e, i);
      if (s) {
        if (d > -ki)
          return null;
      } else if (d > -ki && d < ki)
        return null;
      T.sub(r, h, n);
      var c = T.dot(i, r) / d;
      if (c < 0 || c > 1)
        return null;
      T.cross(i, e, r);
      var v = T.dot(f, i) / d;
      if (v < 0 || v > 1 || c + v > 1)
        return null;
      T.cross(i, e, t);
      var p = -T.dot(r, i) / d;
      return p < 0 ? null : (l || (l = new S()), u && S.set(u, 1 - c - v, c, v), T.scaleAndAdd(l.array, h, f, p), l);
    };
  })(),
  /**
   * Apply an affine transform matrix to the ray
   * @return {clay.Matrix4} matrix
   */
  applyTransform: function(e) {
    S.add(this.direction, this.direction, this.origin), S.transformMat4(this.origin, this.origin, e), S.transformMat4(this.direction, this.direction, e), S.sub(this.direction, this.direction, this.origin), S.normalize(this.direction, this.direction);
  },
  /**
   * Copy values from another ray
   * @param {clay.Ray} ray
   */
  copy: function(e) {
    S.copy(this.origin, e.origin), S.copy(this.direction, e.direction);
  },
  /**
   * Clone a new ray
   * @return {clay.Ray}
   */
  clone: function() {
    var e = new Pt();
    return e.copy(this), e;
  }
};
var Kt = Je.extend(
  function() {
    return (
      /** @lends clay.Camera# */
      {
        /**
         * Camera projection matrix
         * @type {clay.Matrix4}
         */
        projectionMatrix: new I(),
        /**
         * Inverse of camera projection matrix
         * @type {clay.Matrix4}
         */
        invProjectionMatrix: new I(),
        /**
         * View matrix, equal to inverse of camera's world matrix
         * @type {clay.Matrix4}
         */
        viewMatrix: new I(),
        /**
         * Camera frustum in view space
         * @type {clay.Frustum}
         */
        frustum: new yi()
      }
    );
  },
  function() {
    this.update(!0);
  },
  /** @lends clay.Camera.prototype */
  {
    update: function(e) {
      Je.prototype.update.call(this, e), I.invert(this.viewMatrix, this.worldTransform), this.updateProjectionMatrix(), I.invert(this.invProjectionMatrix, this.projectionMatrix), this.frustum.setFromProjection(this.projectionMatrix);
    },
    /**
     * Set camera view matrix
     */
    setViewMatrix: function(e) {
      I.copy(this.viewMatrix, e), I.invert(this.worldTransform, e), this.decomposeWorldTransform();
    },
    /**
     * Decompose camera projection matrix
     */
    decomposeProjectionMatrix: function() {
    },
    /**
     * Set camera projection matrix
     * @param {clay.Matrix4} projectionMatrix
     */
    setProjectionMatrix: function(e) {
      I.copy(this.projectionMatrix, e), I.invert(this.invProjectionMatrix, e), this.decomposeProjectionMatrix();
    },
    /**
     * Update projection matrix, called after update
     */
    updateProjectionMatrix: function() {
    },
    /**
     * Cast a picking ray from camera near plane to far plane
     * @function
     * @param {clay.Vector2} ndc
     * @param {clay.Ray} [out]
     * @return {clay.Ray}
     */
    castRay: (function() {
      var e = L.create();
      return function(t, r) {
        var i = r !== void 0 ? r : new Pt(), n = t.array[0], a = t.array[1];
        return L.set(e, n, a, -1, 1), L.transformMat4(e, e, this.invProjectionMatrix.array), L.transformMat4(e, e, this.worldTransform.array), T.scale(i.origin.array, e, 1 / e[3]), L.set(e, n, a, 1, 1), L.transformMat4(e, e, this.invProjectionMatrix.array), L.transformMat4(e, e, this.worldTransform.array), T.scale(e, e, 1 / e[3]), T.sub(i.direction.array, e, i.origin.array), T.normalize(i.direction.array, i.direction.array), i.direction._dirty = !0, i.origin._dirty = !0, i;
      };
    })()
    /**
     * @function
     * @name clone
     * @return {clay.Camera}
     * @memberOf clay.Camera.prototype
     */
  }
), ku = D.create(), Oa = D.create(), Wi = {};
function Wu(e) {
  var t = [], r = Object.keys(e);
  r.sort();
  for (var i = 0; i < r.length; i++) {
    var n = r[i];
    t.push(n + " " + e[n]);
  }
  var a = t.join(`
`);
  if (Wi[a])
    return Wi[a];
  var o = he.genGUID();
  return Wi[a] = o, o;
}
function xi() {
  this.opaque = [], this.transparent = [], this._opaqueCount = 0, this._transparentCount = 0;
}
xi.prototype.startCount = function() {
  this._opaqueCount = 0, this._transparentCount = 0;
};
xi.prototype.add = function(e, t) {
  t ? this.transparent[this._transparentCount++] = e : this.opaque[this._opaqueCount++] = e;
};
xi.prototype.endCount = function() {
  this.transparent.length = this._transparentCount, this.opaque.length = this._opaqueCount;
};
var gt = Je.extend(
  function() {
    return (
      /** @lends clay.Scene# */
      {
        /**
         * Global material of scene
         * @type {clay.Material}
         */
        material: null,
        lights: [],
        /**
         * Scene bounding box in view space.
         * Used when camera needs to adujst the near and far plane automatically
         * so that the view frustum contains the visible objects as tightly as possible.
         * Notice:
         *  It is updated after rendering (in the step of frustum culling passingly). So may be not so accurate, but saves a lot of calculation
         *
         * @type {clay.BoundingBox}
         */
        viewBoundingBoxLastFrame: new we(),
        // Uniforms for shadow map.
        shadowUniforms: {},
        _cameraList: [],
        // Properties to save the light information in the scene
        // Will be set in the render function
        _lightUniforms: {},
        _previousLightNumber: {},
        _lightNumber: {
          // groupId: {
          // POINT_LIGHT: 0,
          // DIRECTIONAL_LIGHT: 0,
          // SPOT_LIGHT: 0,
          // AMBIENT_LIGHT: 0,
          // AMBIENT_SH_LIGHT: 0
          // }
        },
        _lightProgramKeys: {},
        _nodeRepository: {},
        _renderLists: new Nt(20)
      }
    );
  },
  function() {
    this._scene = this;
  },
  /** @lends clay.Scene.prototype. */
  {
    // Add node to scene
    addToScene: function(e) {
      e instanceof Kt ? (this._cameraList.length > 0 && console.warn("Found multiple camera in one scene. Use the fist one."), this._cameraList.push(e)) : e instanceof Ze && this.lights.push(e), e.name && (this._nodeRepository[e.name] = e);
    },
    // Remove node from scene
    removeFromScene: function(e) {
      var t;
      e instanceof Kt ? (t = this._cameraList.indexOf(e), t >= 0 && this._cameraList.splice(t, 1)) : e instanceof Ze && (t = this.lights.indexOf(e), t >= 0 && this.lights.splice(t, 1)), e.name && delete this._nodeRepository[e.name];
    },
    /**
     * Get node by name
     * @param  {string} name
     * @return {Node}
     * @DEPRECATED
     */
    getNode: function(e) {
      return this._nodeRepository[e];
    },
    /**
     * Set main camera of the scene.
     * @param {claygl.Camera} camera
     */
    setMainCamera: function(e) {
      var t = this._cameraList.indexOf(e);
      t >= 0 && this._cameraList.splice(t, 1), this._cameraList.unshift(e);
    },
    /**
     * Get main camera of the scene.
     */
    getMainCamera: function() {
      return this._cameraList[0];
    },
    getLights: function() {
      return this.lights;
    },
    updateLights: function() {
      var e = this.lights;
      this._previousLightNumber = this._lightNumber;
      for (var t = {}, r = 0; r < e.length; r++) {
        var i = e[r];
        if (!i.invisible) {
          var n = i.group;
          t[n] || (t[n] = {}), t[n][i.type] = t[n][i.type] || 0, t[n][i.type]++;
        }
      }
      this._lightNumber = t;
      for (var a in t)
        this._lightProgramKeys[a] = Wu(t[a]);
      this._updateLightUniforms();
    },
    /**
     * Clone a node and it's children, including mesh, camera, light, etc.
     * Unlike using `Node#clone`. It will clone skeleton and remap the joints. Material will also be cloned.
     *
     * @param {clay.Node} node
     * @return {clay.Node}
     */
    cloneNode: function(e) {
      var t = e.clone(), r = {};
      function i(n, a) {
        r[n.__uid__] = a;
        for (var o = 0; o < n._children.length; o++) {
          var s = n._children[o], l = a._children[o];
          i(s, l);
        }
      }
      return i(e, t), t.traverse(function(n) {
        n.skeleton && (n.skeleton = n.skeleton.clone(r)), n.material && (n.material = n.material.clone());
      }), t;
    },
    /**
     * Traverse the scene and add the renderable object to the render list.
     * It needs camera for the frustum culling.
     *
     * @param {clay.Camera} camera
     * @param {boolean} updateSceneBoundingBox
     * @return {clay.Scene.RenderList}
     */
    updateRenderList: function(e, t) {
      var r = e.__uid__, i = this._renderLists.get(r);
      i || (i = new xi(), this._renderLists.put(r, i)), i.startCount(), t && (this.viewBoundingBoxLastFrame.min.set(1 / 0, 1 / 0, 1 / 0), this.viewBoundingBoxLastFrame.max.set(-1 / 0, -1 / 0, -1 / 0));
      var n = this.material && this.material.transparent || !1;
      return this._doUpdateRenderList(this, e, n, i, t), i.endCount(), i;
    },
    /**
     * Get render list. Used after {@link clay.Scene#updateRenderList}
     * @param {clay.Camera} camera
     * @return {clay.Scene.RenderList}
     */
    getRenderList: function(e) {
      return this._renderLists.get(e.__uid__);
    },
    _doUpdateRenderList: function(e, t, r, i, n) {
      if (!e.invisible)
        for (var a = 0; a < e._children.length; a++) {
          var o = e._children[a];
          if (o.isRenderable()) {
            var s = o.isSkinnedMesh() ? ku : o.worldTransform.array, l = o.geometry;
            D.multiplyAffine(Oa, t.viewMatrix.array, s), (n && !l.boundingBox || !this.isFrustumCulled(o, t, Oa)) && i.add(o, o.material.transparent || r);
          }
          o._children.length > 0 && this._doUpdateRenderList(o, t, r, i, n);
        }
    },
    /**
     * If an scene object is culled by camera frustum
     *
     * Object can be a renderable or a light
     *
     * @param {clay.Node} object
     * @param {clay.Camera} camera
     * @param {Array.<number>} worldViewMat represented with array
     * @param {Array.<number>} projectionMat represented with array
     */
    isFrustumCulled: (function() {
      var e = new we(), t = new I();
      return function(r, i, n) {
        var a = r.boundingBox;
        if (a || (r.skeleton && r.skeleton.boundingBox ? a = r.skeleton.boundingBox : a = r.geometry.boundingBox), !a)
          return !1;
        if (t.array = n, e.transformFrom(a, t), r.castShadow && this.viewBoundingBoxLastFrame.union(e), r.frustumCulling) {
          if (!e.intersectBoundingBox(i.frustum.boundingBox))
            return !0;
          t.array = i.projectionMatrix.array, e.max.array[2] > 0 && e.min.array[2] < 0 && (e.max.array[2] = -1e-20), e.applyProjection(t);
          var o = e.min.array, s = e.max.array;
          if (s[0] < -1 || o[0] > 1 || s[1] < -1 || o[1] > 1 || s[2] < -1 || o[2] > 1)
            return !0;
        }
        return !1;
      };
    })(),
    _updateLightUniforms: function() {
      var e = this.lights;
      e.sort(Xu);
      var t = this._lightUniforms;
      for (var r in t)
        for (var i in t[r])
          t[r][i].value.length = 0;
      for (var n = 0; n < e.length; n++) {
        var a = e[n];
        if (!a.invisible) {
          var r = a.group;
          for (var i in a.uniformTemplates) {
            var o = a.uniformTemplates[i], s = o.value(a);
            if (s != null) {
              t[r] || (t[r] = {}), t[r][i] || (t[r][i] = {
                type: "",
                value: []
              });
              var l = t[r][i];
              switch (l.type = o.type + "v", o.type) {
                case "1i":
                case "1f":
                case "t":
                  l.value.push(s);
                  break;
                case "2f":
                case "3f":
                case "4f":
                  for (var u = 0; u < s.length; u++)
                    l.value.push(s[u]);
                  break;
                default:
                  console.error("Unkown light uniform type " + o.type);
              }
            }
          }
        }
      }
    },
    getLightGroups: function() {
      var e = [];
      for (var t in this._lightNumber)
        e.push(t);
      return e;
    },
    getNumberChangedLightGroups: function() {
      var e = [];
      for (var t in this._lightNumber)
        this.isLightNumberChanged(t) && e.push(t);
      return e;
    },
    // Determine if light group is different with since last frame
    // Used to determine whether to update shader and scene's uniforms in Renderer.render
    isLightNumberChanged: function(e) {
      var t = this._previousLightNumber, r = this._lightNumber;
      for (var i in r[e])
        if (!t[e] || r[e][i] !== t[e][i])
          return !0;
      for (var i in t[e])
        if (!r[e] || r[e][i] !== t[e][i])
          return !0;
      return !1;
    },
    getLightsNumbers: function(e) {
      return this._lightNumber[e];
    },
    getProgramKey: function(e) {
      return this._lightProgramKeys[e];
    },
    setLightUniforms: /* @__PURE__ */ (function() {
      function e(t, r, i) {
        for (var n in t) {
          var a = t[n];
          if (a.type === "tv") {
            if (!r.hasUniform(n))
              continue;
            for (var o = [], s = 0; s < a.value.length; s++) {
              var l = a.value[s], u = r.takeCurrentTextureSlot(i, l);
              o.push(u);
            }
            r.setUniform(i.gl, "1iv", n, o);
          } else
            r.setUniform(i.gl, a.type, n, a.value);
        }
      }
      return function(t, r, i) {
        e(this._lightUniforms[r], t, i), e(this.shadowUniforms, t, i);
      };
    })(),
    /**
     * Dispose self, clear all the scene objects
     * But resources of gl like texuture, shader will not be disposed.
     * Mostly you should use disposeScene method in Renderer to do dispose.
     */
    dispose: function() {
      this.material = null, this._opaqueList = [], this._transparentList = [], this.lights = [], this._lightUniforms = {}, this._lightNumber = {}, this._nodeRepository = {};
    }
  }
);
function Xu(e, t) {
  if (t.castShadow && !e.castShadow)
    return !0;
}
var Uo = /* @__PURE__ */ (function() {
  function e(t) {
    this.value = t;
  }
  return e;
})(), qu = (function() {
  function e() {
    this._len = 0;
  }
  return e.prototype.insert = function(t) {
    var r = new Uo(t);
    return this.insertEntry(r), r;
  }, e.prototype.insertEntry = function(t) {
    this.head ? (this.tail.next = t, t.prev = this.tail, t.next = null, this.tail = t) : this.head = this.tail = t, this._len++;
  }, e.prototype.remove = function(t) {
    var r = t.prev, i = t.next;
    r ? r.next = i : this.head = i, i ? i.prev = r : this.tail = r, t.next = t.prev = null, this._len--;
  }, e.prototype.len = function() {
    return this._len;
  }, e.prototype.clear = function() {
    this.head = this.tail = null, this._len = 0;
  }, e;
})(), zo = (function() {
  function e(t) {
    this._list = new qu(), this._maxSize = 10, this._map = {}, this._maxSize = t;
  }
  return e.prototype.put = function(t, r) {
    var i = this._list, n = this._map, a = null;
    if (n[t] == null) {
      var o = i.len(), s = this._lastRemovedEntry;
      if (o >= this._maxSize && o > 0) {
        var l = i.head;
        i.remove(l), delete n[l.key], a = l.value, this._lastRemovedEntry = l;
      }
      s ? s.value = r : s = new Uo(r), s.key = t, i.insertEntry(s), n[t] = s;
    }
    return a;
  }, e.prototype.get = function(t) {
    var r = this._map[t], i = this._list;
    if (r != null)
      return r !== i.tail && (i.remove(r), i.insertEntry(r)), r.value;
  }, e.prototype.clear = function() {
    this._list.clear(), this._map = {};
  }, e.prototype.len = function() {
    return this._list.len();
  }, e;
})(), Fr = Sr.isPowerOfTwo, ju = ["px", "nx", "py", "ny", "pz", "nz"], Rt = N.extend(function() {
  return (
    /** @lends clay.TextureCube# */
    {
      /**
       * @type {boolean}
       * @default false
       */
      // PENDING cubemap should not flipY in default.
      // flipY: false,
      /**
       * @type {Object}
       * @property {?HTMLImageElement|HTMLCanvasElemnet} px
       * @property {?HTMLImageElement|HTMLCanvasElemnet} nx
       * @property {?HTMLImageElement|HTMLCanvasElemnet} py
       * @property {?HTMLImageElement|HTMLCanvasElemnet} ny
       * @property {?HTMLImageElement|HTMLCanvasElemnet} pz
       * @property {?HTMLImageElement|HTMLCanvasElemnet} nz
       */
      image: {
        px: null,
        nx: null,
        py: null,
        ny: null,
        pz: null,
        nz: null
      },
      /**
       * Pixels data of each side. Will be ignored if images are set.
       * @type {Object}
       * @property {?Uint8Array} px
       * @property {?Uint8Array} nx
       * @property {?Uint8Array} py
       * @property {?Uint8Array} ny
       * @property {?Uint8Array} pz
       * @property {?Uint8Array} nz
       */
      pixels: {
        px: null,
        nx: null,
        py: null,
        ny: null,
        pz: null,
        nz: null
      },
      /**
       * @type {Array.<Object>}
       */
      mipmaps: []
    }
  );
}, {
  textureType: "textureCube",
  update: function(e) {
    var t = e.gl;
    t.bindTexture(t.TEXTURE_CUBE_MAP, this._cache.get("webgl_texture")), this.updateCommon(e);
    var r = this.format, i = this.type;
    t.texParameteri(t.TEXTURE_CUBE_MAP, t.TEXTURE_WRAP_S, this.getAvailableWrapS()), t.texParameteri(t.TEXTURE_CUBE_MAP, t.TEXTURE_WRAP_T, this.getAvailableWrapT()), t.texParameteri(t.TEXTURE_CUBE_MAP, t.TEXTURE_MAG_FILTER, this.getAvailableMagFilter()), t.texParameteri(t.TEXTURE_CUBE_MAP, t.TEXTURE_MIN_FILTER, this.getAvailableMinFilter());
    var n = e.getGLExtension("EXT_texture_filter_anisotropic");
    if (n && this.anisotropic > 1 && t.texParameterf(t.TEXTURE_CUBE_MAP, n.TEXTURE_MAX_ANISOTROPY_EXT, this.anisotropic), i === 36193) {
      var a = e.getGLExtension("OES_texture_half_float");
      a || (i = M.FLOAT);
    }
    if (this.mipmaps.length)
      for (var o = this.width, s = this.height, l = 0; l < this.mipmaps.length; l++) {
        var u = this.mipmaps[l];
        this._updateTextureData(t, u, l, o, s, r, i), o /= 2, s /= 2;
      }
    else
      this._updateTextureData(t, this, 0, this.width, this.height, r, i), !this.NPOT && this.useMipmap && t.generateMipmap(t.TEXTURE_CUBE_MAP);
    t.bindTexture(t.TEXTURE_CUBE_MAP, null);
  },
  _updateTextureData: function(e, t, r, i, n, a, o) {
    for (var s = 0; s < 6; s++) {
      var l = ju[s], u = t.image && t.image[l];
      u ? e.texImage2D(e.TEXTURE_CUBE_MAP_POSITIVE_X + s, r, a, a, o, u) : e.texImage2D(e.TEXTURE_CUBE_MAP_POSITIVE_X + s, r, a, i, n, 0, a, o, t.pixels && t.pixels[l]);
    }
  },
  /**
   * @param  {clay.Renderer} renderer
   * @memberOf clay.TextureCube.prototype
   */
  generateMipmap: function(e) {
    var t = e.gl;
    this.useMipmap && !this.NPOT && (t.bindTexture(t.TEXTURE_CUBE_MAP, this._cache.get("webgl_texture")), t.generateMipmap(t.TEXTURE_CUBE_MAP));
  },
  bind: function(e) {
    e.gl.bindTexture(e.gl.TEXTURE_CUBE_MAP, this.getWebGLTexture(e));
  },
  unbind: function(e) {
    e.gl.bindTexture(e.gl.TEXTURE_CUBE_MAP, null);
  },
  // Overwrite the isPowerOfTwo method
  isPowerOfTwo: function() {
    return this.image.px ? Fr(this.image.px.width) && Fr(this.image.px.height) : Fr(this.width) && Fr(this.height);
  },
  isRenderable: function() {
    return this.image.px ? Gt(this.image.px) && Gt(this.image.nx) && Gt(this.image.py) && Gt(this.image.ny) && Gt(this.image.pz) && Gt(this.image.nz) : !!(this.width && this.height);
  },
  load: function(e, t) {
    var r = 0, i = this;
    return he.each(e, function(n, a) {
      var o = Q.createImage();
      t && (o.crossOrigin = t), o.onload = function() {
        r--, r === 0 && (i.dirty(), i.trigger("success", i));
      }, o.onerror = function() {
        r--;
      }, r++, o.src = n, i.image[a] = o;
    }), this;
  }
});
Object.defineProperty(Rt.prototype, "width", {
  get: function() {
    return this.image && this.image.px ? this.image.px.width : this._width;
  },
  set: function(e) {
    this.image && this.image.px ? console.warn("Texture from image can't set width") : (this._width !== e && this.dirty(), this._width = e);
  }
});
Object.defineProperty(Rt.prototype, "height", {
  get: function() {
    return this.image && this.image.px ? this.image.px.height : this._height;
  },
  set: function(e) {
    this.image && this.image.px ? console.warn("Texture from image can't set height") : (this._height !== e && this.dirty(), this._height = e);
  }
});
function Gt(e) {
  return e.width > 0 && e.height > 0;
}
var _e = Kt.extend(
  /** @lends clay.camera.Perspective# */
  {
    /**
     * Vertical field of view in degrees
     * @type {number}
     */
    fov: 50,
    /**
     * Aspect ratio, typically viewport width / height
     * @type {number}
     */
    aspect: 1,
    /**
     * Near bound of the frustum
     * @type {number}
     */
    near: 0.1,
    /**
     * Far bound of the frustum
     * @type {number}
     */
    far: 2e3
  },
  /** @lends clay.camera.Perspective.prototype */
  {
    updateProjectionMatrix: function() {
      var e = this.fov / 180 * Math.PI;
      this.projectionMatrix.perspective(e, this.aspect, this.near, this.far);
    },
    decomposeProjectionMatrix: function() {
      var e = this.projectionMatrix.array, t = Math.atan(1 / e[5]) * 2;
      this.fov = t / Math.PI * 180, this.aspect = e[5] / e[0], this.near = e[14] / (e[10] - 1), this.far = e[14] / (e[10] + 1);
    },
    /**
     * @return {clay.camera.Perspective}
     */
    clone: function() {
      var e = Kt.prototype.clone.call(this);
      return e.fov = this.fov, e.aspect = this.aspect, e.near = this.near, e.far = this.far, e;
    }
  }
), Ur = "framebuffer", st = "renderbuffer", Ia = st + "_width", Ba = st + "_height", Xi = st + "_attached", qi = "depthtexture_attached", bt = M.FRAMEBUFFER, lr = M.RENDERBUFFER, dr = M.DEPTH_ATTACHMENT, Ho = M.COLOR_ATTACHMENT0, pe = Ae.extend(
  /** @lends clay.FrameBuffer# */
  {
    /**
     * If use depth buffer
     * @type {boolean}
     */
    depthBuffer: !0,
    /**
     * @type {Object}
     */
    viewport: null,
    _width: 0,
    _height: 0,
    _textures: null,
    _boundRenderer: null
  },
  function() {
    this._cache = new Zt(), this._textures = {};
  },
  /**@lends clay.FrameBuffer.prototype. */
  {
    /**
     * Get attached texture width
     * {number}
     */
    // FIXME Can't use before #bind
    getTextureWidth: function() {
      return this._width;
    },
    /**
     * Get attached texture height
     * {number}
     */
    getTextureHeight: function() {
      return this._height;
    },
    /**
     * Bind the framebuffer to given renderer before rendering
     * @param  {clay.Renderer} renderer
     */
    bind: function(e) {
      if (e.__currentFrameBuffer) {
        if (e.__currentFrameBuffer === this)
          return;
        console.warn("Renderer already bound with another framebuffer. Unbind it first");
      }
      e.__currentFrameBuffer = this;
      var t = e.gl;
      t.bindFramebuffer(bt, this._getFrameBufferGL(e)), this._boundRenderer = e;
      var r = this._cache;
      r.put("viewport", e.viewport);
      var i = !1, n, a;
      for (var o in this._textures) {
        i = !0;
        var s = this._textures[o];
        s && (n = s.texture.width, a = s.texture.height, this._doAttach(e, s.texture, o, s.target));
      }
      this._width = n, this._height = a, !i && this.depthBuffer && console.error("Must attach texture before bind, or renderbuffer may have incorrect width and height."), this.viewport ? e.setViewport(this.viewport) : e.setViewport(0, 0, n, a, 1);
      var l = r.get("attached_textures");
      if (l) {
        for (var o in l)
          if (!this._textures[o]) {
            var u = l[o];
            this._doDetach(t, o, u);
          }
      }
      if (!r.get(qi) && this.depthBuffer) {
        r.miss(st) && r.put(st, t.createRenderbuffer());
        var f = r.get(st);
        (n !== r.get(Ia) || a !== r.get(Ba)) && (t.bindRenderbuffer(lr, f), t.renderbufferStorage(lr, t.DEPTH_COMPONENT16, n, a), r.put(Ia, n), r.put(Ba, a), t.bindRenderbuffer(lr, null)), r.get(Xi) || (t.framebufferRenderbuffer(bt, dr, lr, f), r.put(Xi, !0));
      }
    },
    /**
     * Unbind the frame buffer after rendering
     * @param  {clay.Renderer} renderer
     */
    unbind: function(e) {
      e.__currentFrameBuffer = null;
      var t = e.gl;
      t.bindFramebuffer(bt, null), this._boundRenderer = null, this._cache.use(e.__uid__);
      var r = this._cache.get("viewport");
      r && e.setViewport(r), this.updateMipmap(e);
    },
    // Because the data of texture is changed over time,
    // Here update the mipmaps of texture each time after rendered;
    updateMipmap: function(e) {
      var t = e.gl;
      for (var r in this._textures) {
        var i = this._textures[r];
        if (i) {
          var n = i.texture;
          if (!n.NPOT && n.useMipmap && n.minFilter === N.LINEAR_MIPMAP_LINEAR) {
            var a = n.textureType === "textureCube" ? M.TEXTURE_CUBE_MAP : M.TEXTURE_2D;
            t.bindTexture(a, n.getWebGLTexture(e)), t.generateMipmap(a), t.bindTexture(a, null);
          }
        }
      }
    },
    // 0x8CD5, 36053, FRAMEBUFFER_COMPLETE
    // 0x8CD6, 36054, FRAMEBUFFER_INCOMPLETE_ATTACHMENT
    // 0x8CD7, 36055, FRAMEBUFFER_INCOMPLETE_MISSING_ATTACHMENT
    // 0x8CD9, 36057, FRAMEBUFFER_INCOMPLETE_DIMENSIONS
    // 0x8CDD, 36061, FRAMEBUFFER_UNSUPPORTED
    checkStatus: function(e) {
      return e.checkFramebufferStatus(bt);
    },
    _getFrameBufferGL: function(e) {
      var t = this._cache;
      return t.use(e.__uid__), t.miss(Ur) && t.put(Ur, e.gl.createFramebuffer()), t.get(Ur);
    },
    /**
     * Attach a texture(RTT) to the framebuffer
     * @param  {clay.Texture} texture
     * @param  {number} [attachment=gl.COLOR_ATTACHMENT0]
     * @param  {number} [target=gl.TEXTURE_2D]
     */
    attach: function(e, t, r) {
      if (!e.width)
        throw new Error("The texture attached to color buffer is not a valid.");
      t = t || Ho, r = r || M.TEXTURE_2D;
      var i = this._boundRenderer, n = i && i.gl, a;
      if (n) {
        var o = this._cache;
        o.use(i.__uid__), a = o.get("attached_textures");
      }
      var s = this._textures[t];
      if (!(s && s.target === r && s.texture === e && a && a[t] != null)) {
        var l = !0;
        i && (l = this._doAttach(i, e, t, r), this.viewport || i.setViewport(0, 0, e.width, e.height, 1)), l && (this._textures[t] = this._textures[t] || {}, this._textures[t].texture = e, this._textures[t].target = r);
      }
    },
    _doAttach: function(e, t, r, i) {
      var n = e.gl, a = t.getWebGLTexture(e), o = this._cache.get("attached_textures");
      if (o && o[r]) {
        var s = o[r];
        if (s.texture === t && s.target === i)
          return;
      }
      r = +r;
      var l = !0;
      if (r === dr || r === M.DEPTH_STENCIL_ATTACHMENT) {
        var u = e.getGLExtension("WEBGL_depth_texture");
        if (u || (console.error("Depth texture is not supported by the browser"), l = !1), t.format !== M.DEPTH_COMPONENT && t.format !== M.DEPTH_STENCIL && (console.error("The texture attached to depth buffer is not a valid."), l = !1), l) {
          var f = this._cache.get(st);
          f && (n.framebufferRenderbuffer(bt, dr, lr, null), n.deleteRenderbuffer(f), this._cache.put(st, !1)), this._cache.put(Xi, !1), this._cache.put(qi, !0);
        }
      }
      return n.framebufferTexture2D(bt, r, i, a, 0), o || (o = {}, this._cache.put("attached_textures", o)), o[r] = o[r] || {}, o[r].texture = t, o[r].target = i, l;
    },
    _doDetach: function(e, t, r) {
      e.framebufferTexture2D(bt, t, r, null, 0);
      var i = this._cache.get("attached_textures");
      i && i[t] && (i[t] = null), (t === dr || t === M.DEPTH_STENCIL_ATTACHMENT) && this._cache.put(qi, !1);
    },
    /**
     * Detach a texture
     * @param  {number} [attachment=gl.COLOR_ATTACHMENT0]
     * @param  {number} [target=gl.TEXTURE_2D]
     */
    detach: function(e, t) {
      if (this._textures[e] = null, this._boundRenderer) {
        var r = this._cache;
        r.use(this._boundRenderer.__uid__), this._doDetach(this._boundRenderer.gl, e, t);
      }
    },
    /**
     * Dispose
     * @param  {WebGLRenderingContext} _gl
     */
    dispose: function(e) {
      var t = e.gl, r = this._cache;
      r.use(e.__uid__);
      var i = r.get(st);
      i && t.deleteRenderbuffer(i);
      var n = r.get(Ur);
      n && t.deleteFramebuffer(n), r.deleteContext(e.__uid__), this._textures = {};
    }
  }
);
pe.DEPTH_ATTACHMENT = dr;
pe.COLOR_ATTACHMENT0 = Ho;
pe.STENCIL_ATTACHMENT = M.STENCIL_ATTACHMENT;
pe.DEPTH_STENCIL_ATTACHMENT = M.DEPTH_STENCIL_ATTACHMENT;
var Zu = ["px", "nx", "py", "ny", "pz", "nz"], Nn = Ae.extend(
  function() {
    var e = (
      /** @lends clay.prePass.EnvironmentMap# */
      {
        /**
         * Camera position
         * @type {clay.Vector3}
         * @memberOf clay.prePass.EnvironmentMap#
         */
        position: new S(),
        /**
         * Camera far plane
         * @type {number}
         * @memberOf clay.prePass.EnvironmentMap#
         */
        far: 1e3,
        /**
         * Camera near plane
         * @type {number}
         * @memberOf clay.prePass.EnvironmentMap#
         */
        near: 0.1,
        /**
         * Environment cube map
         * @type {clay.TextureCube}
         * @memberOf clay.prePass.EnvironmentMap#
         */
        texture: null,
        /**
         * Used if you wan't have shadow in environment map
         * @type {clay.prePass.ShadowMap}
         */
        shadowMapPass: null
      }
    ), t = e._cameras = {
      px: new _e({ fov: 90 }),
      nx: new _e({ fov: 90 }),
      py: new _e({ fov: 90 }),
      ny: new _e({ fov: 90 }),
      pz: new _e({ fov: 90 }),
      nz: new _e({ fov: 90 })
    };
    return t.px.lookAt(S.POSITIVE_X, S.NEGATIVE_Y), t.nx.lookAt(S.NEGATIVE_X, S.NEGATIVE_Y), t.py.lookAt(S.POSITIVE_Y, S.POSITIVE_Z), t.ny.lookAt(S.NEGATIVE_Y, S.NEGATIVE_Z), t.pz.lookAt(S.POSITIVE_Z, S.NEGATIVE_Y), t.nz.lookAt(S.NEGATIVE_Z, S.NEGATIVE_Y), e._frameBuffer = new pe(), e;
  },
  /** @lends clay.prePass.EnvironmentMap# */
  {
    /**
     * @param  {string} target
     * @return  {clay.Camera}
     */
    getCamera: function(e) {
      return this._cameras[e];
    },
    /**
     * @param  {clay.Renderer} renderer
     * @param  {clay.Scene} scene
     * @param  {boolean} [notUpdateScene=false]
     */
    render: function(e, t, r) {
      var i = e.gl;
      r || t.update();
      for (var n = this.texture.width, a = 2 * Math.atan(n / (n - 0.5)) / Math.PI * 180, o = 0; o < 6; o++) {
        var s = Zu[o], l = this._cameras[s];
        if (S.copy(l.position, this.position), l.far = this.far, l.near = this.near, l.fov = a, this.shadowMapPass) {
          l.update();
          var u = t.getBoundingBox();
          u.applyTransform(l.viewMatrix), t.viewBoundingBoxLastFrame.copy(u), this.shadowMapPass.render(e, t, l, !0);
        }
        this._frameBuffer.attach(
          this.texture,
          i.COLOR_ATTACHMENT0,
          i.TEXTURE_CUBE_MAP_POSITIVE_X + o
        ), this._frameBuffer.bind(e), e.render(t, l, !0), this._frameBuffer.unbind(e);
      }
    },
    /**
     * @param {clay.Renderer} renderer
     */
    dispose: function(e) {
      this._frameBuffer.dispose(e);
    }
  }
), On = $.extend(
  /** @lends clay.geometry.Plane# */
  {
    dynamic: !1,
    /**
     * @type {number}
     */
    widthSegments: 1,
    /**
     * @type {number}
     */
    heightSegments: 1
  },
  function() {
    this.build();
  },
  /** @lends clay.geometry.Plane.prototype */
  {
    /**
     * Build plane geometry
     */
    build: function() {
      for (var e = this.heightSegments, t = this.widthSegments, r = this.attributes, i = [], n = [], a = [], o = [], s = 0; s <= e; s++)
        for (var l = s / e, u = 0; u <= t; u++) {
          var f = u / t;
          if (i.push([2 * f - 1, 2 * l - 1, 0]), n && n.push([f, l]), a && a.push([0, 0, 1]), u < t && s < e) {
            var h = u + s * (t + 1);
            o.push([h, h + 1, h + t + 1]), o.push([h + t + 1, h + 1, h + t + 2]);
          }
        }
      r.position.fromArray(i), r.texcoord0.fromArray(n), r.normal.fromArray(a), this.initIndicesFromArray(o), this.boundingBox = new we(), this.boundingBox.min.set(-1, -1, 0), this.boundingBox.max.set(1, 1, 0);
    }
  }
), le = new I(), Go = $.extend(
  /**@lends clay.geometry.Cube# */
  {
    dynamic: !1,
    /**
     * @type {number}
     */
    widthSegments: 1,
    /**
     * @type {number}
     */
    heightSegments: 1,
    /**
     * @type {number}
     */
    depthSegments: 1,
    /**
     * @type {boolean}
     */
    inside: !1
  },
  function() {
    this.build();
  },
  /** @lends clay.geometry.Cube.prototype */
  {
    /**
     * Build cube geometry
     */
    build: function() {
      var e = {
        px: Vt("px", this.depthSegments, this.heightSegments),
        nx: Vt("nx", this.depthSegments, this.heightSegments),
        py: Vt("py", this.widthSegments, this.depthSegments),
        ny: Vt("ny", this.widthSegments, this.depthSegments),
        pz: Vt("pz", this.widthSegments, this.heightSegments),
        nz: Vt("nz", this.widthSegments, this.heightSegments)
      }, t = ["position", "texcoord0", "normal"], r = 0, i = 0;
      for (var n in e)
        r += e[n].vertexCount, i += e[n].indices.length;
      for (var a = 0; a < t.length; a++)
        this.attributes[t[a]].init(r);
      this.indices = new Q.Uint16Array(i);
      var o = 0, s = 0;
      for (var n in e) {
        for (var l = e[n], a = 0; a < t.length; a++)
          for (var u = t[a], f = l.attributes[u].value, h = l.attributes[u].size, d = u === "normal", c = 0; c < f.length; c++) {
            var v = f[c];
            this.inside && d && (v = -v), this.attributes[u].value[c + h * s] = v;
          }
        for (var p = l.indices.length, c = 0; c < l.indices.length; c++)
          this.indices[c + o] = s + l.indices[this.inside ? p - c - 1 : c];
        o += l.indices.length, s += l.vertexCount;
      }
      this.boundingBox = new we(), this.boundingBox.max.set(1, 1, 1), this.boundingBox.min.set(-1, -1, -1);
    }
  }
);
function Vt(e, t, r) {
  le.identity();
  var i = new On({
    widthSegments: t,
    heightSegments: r
  });
  switch (e) {
    case "px":
      I.translate(le, le, S.POSITIVE_X), I.rotateY(le, le, Math.PI / 2);
      break;
    case "nx":
      I.translate(le, le, S.NEGATIVE_X), I.rotateY(le, le, -Math.PI / 2);
      break;
    case "py":
      I.translate(le, le, S.POSITIVE_Y), I.rotateX(le, le, -Math.PI / 2);
      break;
    case "ny":
      I.translate(le, le, S.NEGATIVE_Y), I.rotateX(le, le, Math.PI / 2);
      break;
    case "pz":
      I.translate(le, le, S.POSITIVE_Z);
      break;
    case "nz":
      I.translate(le, le, S.NEGATIVE_Z), I.rotateY(le, le, Math.PI);
      break;
  }
  return i.applyTransform(le), i;
}
const Yu = `@export clay.skybox.vertex
#define SHADER_NAME skybox
uniform mat4 world : WORLD;
uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
attribute vec3 position : POSITION;
varying vec3 v_WorldPosition;
void main()
{
 v_WorldPosition = (world * vec4(position, 1.0)).xyz;
 gl_Position = worldViewProjection * vec4(position, 1.0);
}
@end
@export clay.skybox.fragment
#define PI 3.1415926
uniform mat4 viewInverse : VIEWINVERSE;
#ifdef EQUIRECTANGULAR
uniform sampler2D environmentMap;
#else
uniform samplerCube environmentMap;
#endif
uniform float lod: 0.0;
varying vec3 v_WorldPosition;
@import clay.util.rgbm
@import clay.util.srgb
@import clay.util.ACES
void main()
{
 vec3 eyePos = viewInverse[3].xyz;
 vec3 V = normalize(v_WorldPosition - eyePos);
#ifdef EQUIRECTANGULAR
 float phi = acos(V.y);
 float theta = atan(-V.x, V.z) + PI * 0.5;
 vec2 uv = vec2(theta / 2.0 / PI, phi / PI);
 vec4 texel = decodeHDR(texture2D(environmentMap, fract(uv)));
#else
 #if defined(LOD) || defined(SUPPORT_TEXTURE_LOD)
 vec4 texel = decodeHDR(textureCubeLodEXT(environmentMap, V, lod));
 #else
 vec4 texel = decodeHDR(textureCube(environmentMap, V));
 #endif
#endif
#ifdef SRGB_DECODE
 texel = sRGBToLinear(texel);
#endif
#ifdef TONEMAPPING
 texel.rgb = ACESToneMapping(texel.rgb);
#endif
#ifdef SRGB_ENCODE
 texel = linearTosRGB(texel);
#endif
 gl_FragColor = encodeHDR(vec4(texel.rgb, 1.0));
}
@end`;
U.import(Yu);
var Tr = Oe.extend(
  function() {
    var e = new U({
      vertex: U.source("clay.skybox.vertex"),
      fragment: U.source("clay.skybox.fragment")
    }), t = new ft({
      shader: e,
      depthMask: !1
    });
    return {
      /**
       * @type {clay.Scene}
       * @memberOf clay.plugin.Skybox.prototype
       */
      scene: null,
      geometry: new Go(),
      material: t,
      environmentMap: null,
      culling: !1,
      _dummyCamera: new _e()
    };
  },
  function() {
    var e = this.scene;
    e && this.attachScene(e), this.environmentMap && this.setEnvironmentMap(this.environmentMap);
  },
  /** @lends clay.plugin.Skybox# */
  {
    /**
     * Attach the skybox to the scene
     * @param  {clay.Scene} scene
     */
    attachScene: function(e) {
      this.scene && this.detachScene(), e.skybox = this, this.scene = e, e.on("beforerender", this._beforeRenderScene, this);
    },
    /**
     * Detach from scene
     */
    detachScene: function() {
      this.scene && (this.scene.off("beforerender", this._beforeRenderScene), this.scene.skybox = null), this.scene = null;
    },
    /**
     * Dispose skybox
     * @param  {clay.Renderer} renderer
     */
    dispose: function(e) {
      this.detachScene(), this.geometry.dispose(e);
    },
    /**
     * Set environment map
     * @param {clay.TextureCube} envMap
     */
    setEnvironmentMap: function(e) {
      e.textureType === "texture2D" ? (this.material.define("EQUIRECTANGULAR"), e.minFilter = N.LINEAR) : this.material.undefine("EQUIRECTANGULAR"), this.material.set("environmentMap", e);
    },
    /**
     * Get environment map
     * @return {clay.TextureCube}
     */
    getEnvironmentMap: function() {
      return this.material.get("environmentMap");
    },
    _beforeRenderScene: function(e, t, r) {
      this.renderSkybox(e, r);
    },
    renderSkybox: function(e, t) {
      var r = this._dummyCamera;
      r.aspect = e.getViewportAspect(), r.fov = t.fov || 50, r.updateProjectionMatrix(), I.invert(r.invProjectionMatrix, r.projectionMatrix), r.worldTransform.copy(t.worldTransform), r.viewMatrix.copy(t.viewMatrix), this.position.copy(t.getWorldPosition()), this.update(), e.gl.disable(e.gl.BLEND), this.material.get("lod") > 0 ? this.material.define("fragment", "LOD") : this.material.undefine("fragment", "LOD"), e.renderPass([this], r);
    }
  }
), Ku = 542327876, Qu = 131072, $u = 512, Ju = 4;
function In(e) {
  return e.charCodeAt(0) + (e.charCodeAt(1) << 8) + (e.charCodeAt(2) << 16) + (e.charCodeAt(3) << 24);
}
var ef = 31, tf = In("DXT1"), rf = In("DXT3"), nf = In("DXT5"), af = 0, of = 1, sf = 2, lf = 3, uf = 4, ff = 7, hf = 20, cf = 21, df = 28, vf = {
  parse: function(e, t) {
    var r = new Int32Array(e, 0, ef);
    if (r[af] !== Ku || !r(hf) & Ju)
      return null;
    var i = r(cf), n = r[uf], a = r[lf], o = r[df] & $u, s = r[sf] & Qu, l, u;
    switch (i) {
      case tf:
        l = 8, u = N.COMPRESSED_RGB_S3TC_DXT1_EXT;
        break;
      case rf:
        l = 16, u = N.COMPRESSED_RGBA_S3TC_DXT3_EXT;
        break;
      case nf:
        l = 16, u = N.COMPRESSED_RGBA_S3TC_DXT5_EXT;
        break;
      default:
        return null;
    }
    var f = r[of] + 4, h = o ? 6 : 1, d = 1;
    s && (d = Math.max(1, r[ff]));
    for (var c = [], v = 0; v < h; v++) {
      var p = n, m = a;
      c[v] = new K({
        width: p,
        height: m,
        format: u
      });
      for (var _ = [], x = 0; x < d; x++) {
        var y = Math.max(4, p) / 4 * Math.max(4, m) / 4 * l, g = new Uint8Array(e, f, y);
        f += y, p *= 0.5, m *= 0.5, _[x] = g;
      }
      c[v].pixels = _[0], s && (c[v].mipmaps = _);
    }
    if (t)
      t.width = c[0].width, t.height = c[0].height, t.format = c[0].format, t.pixels = c[0].pixels, t.mipmaps = c[0].mipmaps;
    else
      return c[0];
  }
}, ei = String.fromCharCode, pf = 8, mf = 32767;
function _f(e, t, r, i) {
  if (e[3] > 0) {
    var n = Math.pow(2, e[3] - 128 - 8 + i);
    t[r + 0] = e[0] * n, t[r + 1] = e[1] * n, t[r + 2] = e[2] * n;
  } else
    t[r + 0] = 0, t[r + 1] = 0, t[r + 2] = 0;
  return t[r + 3] = 1, t;
}
function gf(e, t, r) {
  for (var i = "", n = t; n < r; n++)
    i += ei(e[n]);
  return i;
}
function yf(e, t) {
  t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3];
}
function Fa(e, t, r, i) {
  for (var n = 0, a = 0, o = i; o > 0; )
    if (e[a][0] = t[r++], e[a][1] = t[r++], e[a][2] = t[r++], e[a][3] = t[r++], e[a][0] === 1 && e[a][1] === 1 && e[a][2] === 1) {
      for (var s = e[a][3] << n >>> 0; s > 0; s--)
        yf(e[a - 1], e[a]), a++, o--;
      n += 8;
    } else
      a++, o--, n = 0;
  return r;
}
function xf(e, t, r, i) {
  if (i < pf | i > mf)
    return Fa(e, t, r, i);
  var n = t[r++];
  if (n != 2)
    return Fa(e, t, r - 1, i);
  if (e[0][1] = t[r++], e[0][2] = t[r++], n = t[r++], (e[0][2] << 8 >>> 0 | n) >>> 0 !== i)
    return null;
  for (var n = 0; n < 4; n++)
    for (var a = 0; a < i; ) {
      var o = t[r++];
      if (o > 128) {
        o = (o & 127) >>> 0;
        for (var s = t[r++]; o--; )
          e[a++][n] = s;
      } else
        for (; o--; )
          e[a++][n] = t[r++];
    }
  return r;
}
var Tf = {
  // http://www.graphics.cornell.edu/~bjw/rgbe.html
  // Blender source
  // http://radsite.lbl.gov/radiance/refer/Notes/picture_format.html
  parseRGBE: function(e, t, r) {
    r == null && (r = 0);
    var i = new Uint8Array(e), n = i.length;
    if (gf(i, 0, 2) === "#?") {
      for (var a = 2; a < n && !(ei(i[a]) === `
` && ei(i[a + 1]) === `
`); a++)
        ;
      if (!(a >= n)) {
        a += 2;
        for (var o = ""; a < n; a++) {
          var s = ei(i[a]);
          if (s === `
`)
            break;
          o += s;
        }
        var l = o.split(" "), u = parseInt(l[1]), f = parseInt(l[3]);
        if (!(!f || !u)) {
          for (var h = a + 1, d = [], c = 0; c < f; c++) {
            d[c] = [];
            for (var v = 0; v < 4; v++)
              d[c][v] = 0;
          }
          for (var p = new Float32Array(f * u * 4), m = 0, _ = 0; _ < u; _++) {
            var h = xf(d, i, h, f);
            if (!h)
              return null;
            for (var c = 0; c < f; c++)
              _f(d[c], p, m, r), m += 4;
          }
          return t || (t = new K()), t.width = f, t.height = u, t.pixels = p, t.type = N.FLOAT, t;
        }
      }
    }
  },
  parseRGBEFromPNG: function(e) {
  }
}, et = {
  /**
   * @param  {string|object} path
   * @param  {object} [option]
   * @param  {Function} [onsuccess]
   * @param  {Function} [onerror]
   * @return {clay.Texture}
   */
  loadTexture: function(e, t, r, i) {
    var n;
    if (typeof t == "function" ? (r = t, i = r, t = {}) : t = t || {}, typeof e == "string") {
      if (e.match(/.hdr$/) || t.fileType === "hdr")
        return n = new K({
          width: 0,
          height: 0,
          sRGB: !1
        }), et._fetchTexture(
          e,
          function(a) {
            Tf.parseRGBE(a, n, t.exposure), n.dirty(), r && r(n);
          },
          i
        ), n;
      e.match(/.dds$/) || t.fileType === "dds" ? (n = new K({
        width: 0,
        height: 0
      }), et._fetchTexture(
        e,
        function(a) {
          vf.parse(a, n), n.dirty(), r && r(n);
        },
        i
      )) : (n = new K(), n.load(e), n.success(r), n.error(i));
    } else typeof e == "object" && typeof e.px < "u" && (n = new Rt(), n.load(e), n.success(r), n.error(i));
    return n;
  },
  /**
   * Load a panorama texture and render it to a cube map
   * @param  {clay.Renderer} renderer
   * @param  {string} path
   * @param  {clay.TextureCube} cubeMap
   * @param  {object} [option]
   * @param  {boolean} [option.encodeRGBM]
   * @param  {number} [option.exposure]
   * @param  {Function} [onsuccess]
   * @param  {Function} [onerror]
   */
  loadPanorama: function(e, t, r, i, n, a) {
    var o = this;
    typeof i == "function" ? (n = i, a = n, i = {}) : i = i || {}, et.loadTexture(t, i, function(s) {
      s.flipY = i.flipY || !1, o.panoramaToCubeMap(e, s, r, i), s.dispose(e), n && n(r);
    }, a);
  },
  /**
   * Render a panorama texture to a cube map
   * @param  {clay.Renderer} renderer
   * @param  {clay.Texture2D} panoramaMap
   * @param  {clay.TextureCube} cubeMap
   * @param  {Object} option
   * @param  {boolean} [option.encodeRGBM]
   */
  panoramaToCubeMap: function(e, t, r, i) {
    var n = new Nn(), a = new Tr({
      scene: new gt()
    });
    return a.setEnvironmentMap(t), i = i || {}, i.encodeRGBM && a.material.define("fragment", "RGBM_ENCODE"), r.sRGB = t.sRGB, n.texture = r, n.render(e, a.scene), n.texture = null, n.dispose(e), r;
  },
  /**
   * Convert height map to normal map
   * @param {HTMLImageElement|HTMLCanvasElement} image
   * @param {boolean} [checkBump=false]
   * @return {HTMLCanvasElement}
   */
  heightToNormal: function(e, t) {
    var r = document.createElement("canvas"), i = r.width = e.width, n = r.height = e.height, a = r.getContext("2d");
    a.drawImage(e, 0, 0, i, n), t = t || !1;
    for (var o = a.getImageData(0, 0, i, n), s = a.createImageData(i, n), l = 0; l < o.data.length; l += 4) {
      if (t) {
        var u = o.data[l], f = o.data[l + 1], h = o.data[l + 2], d = Math.abs(u - f) + Math.abs(f - h);
        if (d > 20)
          return console.warn("Given image is not a height map"), e;
      }
      var c, v, p, m;
      l % (i * 4) === 0 ? (c = o.data[l], p = o.data[l + 4]) : l % (i * 4) === (i - 1) * 4 ? (c = o.data[l - 4], p = o.data[l]) : (c = o.data[l - 4], p = o.data[l + 4]), l < i * 4 ? (v = o.data[l], m = o.data[l + i * 4]) : l > i * (n - 1) * 4 ? (v = o.data[l - i * 4], m = o.data[l]) : (v = o.data[l - i * 4], m = o.data[l + i * 4]), s.data[l] = c - p + 127, s.data[l + 1] = v - m + 127, s.data[l + 2] = 255, s.data[l + 3] = 255;
    }
    return a.putImageData(s, 0, 0), r;
  },
  /**
   * Convert height map to normal map
   * @param {HTMLImageElement|HTMLCanvasElement} image
   * @param {boolean} [checkBump=false]
   * @param {number} [threshold=20]
   * @return {HTMLCanvasElement}
   */
  isHeightImage: function(e, t, r) {
    if (!e || !e.width || !e.height)
      return !1;
    var i = document.createElement("canvas"), n = i.getContext("2d"), a = t || 32;
    r = r || 20, i.width = i.height = a, n.drawImage(e, 0, 0, a, a);
    for (var o = n.getImageData(0, 0, a, a), s = 0; s < o.data.length; s += 4) {
      var l = o.data[s], u = o.data[s + 1], f = o.data[s + 2], h = Math.abs(l - u) + Math.abs(u - f);
      if (h > r)
        return !1;
    }
    return !0;
  },
  _fetchTexture: function(e, t, r) {
    Q.request.get({
      url: e,
      responseType: "arraybuffer",
      onload: t,
      onerror: r
    });
  },
  /**
   * Create a chessboard texture
   * @param  {number} [size]
   * @param  {number} [unitSize]
   * @param  {string} [color1]
   * @param  {string} [color2]
   * @return {clay.Texture2D}
   */
  createChessboard: function(e, t, r, i) {
    e = e || 512, t = t || 64, r = r || "black", i = i || "white";
    var n = Math.ceil(e / t), a = document.createElement("canvas");
    a.width = e, a.height = e;
    var o = a.getContext("2d");
    o.fillStyle = i, o.fillRect(0, 0, e, e), o.fillStyle = r;
    for (var s = 0; s < n; s++)
      for (var l = 0; l < n; l++) {
        var u = l % 2 ? s % 2 : s % 2 - 1;
        u && o.fillRect(s * t, l * t, t, t);
      }
    var f = new K({
      image: a,
      anisotropic: 8
    });
    return f;
  },
  /**
   * Create a blank pure color 1x1 texture
   * @param  {string} color
   * @return {clay.Texture2D}
   */
  createBlank: function(e) {
    var t = document.createElement("canvas");
    t.width = 1, t.height = 1;
    var r = t.getContext("2d");
    r.fillStyle = e, r.fillRect(0, 0, 1, 1);
    var i = new K({
      image: t
    });
    return i;
  }
};
function Vo(e, t) {
  if (!e.__ECGLOnRefresh) {
    e.__ECGLOnRefresh = !0;
    var r = e._refresh ? "_refresh" : "refreshImmediately", i = e[r];
    e[r] = function() {
      i.apply(this, arguments), t();
    };
  }
}
var mn = ["mousedown", "mouseup", "mousemove", "mouseover", "mouseout", "click", "dblclick", "contextmenu"];
function _n(e) {
  return "_on" + e;
}
var gn = function(e) {
  var t = this;
  this._texture = new K({
    anisotropic: 32,
    flipY: !1,
    surface: this,
    dispose: function(r) {
      t.dispose(), K.prototype.dispose.call(this, r);
    }
  }), mn.forEach(function(r) {
    this[_n(r)] = function(i) {
      i.triangle && this._meshes.forEach(function(n) {
        this.dispatchEvent(r, n, i.triangle, i.point);
      }, this);
    };
  }, this), this._meshes = [], e && this.setECharts(e), this.onupdate = null;
};
gn.prototype = {
  constructor: gn,
  getTexture: function() {
    return this._texture;
  },
  setECharts: function(e) {
    this._chart = e;
    var t = e.getDom();
    if (!(t instanceof HTMLCanvasElement))
      console.error("ECharts must init on canvas if it is used as texture."), t = document.createElement("canvas");
    else {
      var r = this;
      Vo(e.getZr(), function() {
        r._texture.dirty(), r.onupdate && r.onupdate();
      });
    }
    this._texture.image = t, this._texture.dirty(), this.onupdate && this.onupdate();
  },
  /**
   * @method
   * @param {clay.Mesh} attachedMesh
   * @param {Array.<number>} triangle Triangle indices
   * @param {clay.math.Vector3} point
   */
  dispatchEvent: (function() {
    var e = new S(), t = new S(), r = new S(), i = new k(), n = new k(), a = new k(), o = new k(), s = new S();
    return function(l, u, f, h) {
      var d = u.geometry, c = d.attributes.position, v = d.attributes.texcoord0, p = S.dot, m = S.cross;
      c.get(f[0], e.array), c.get(f[1], t.array), c.get(f[2], r.array), v.get(f[0], i.array), v.get(f[1], n.array), v.get(f[2], a.array), m(s, t, r);
      var _ = p(e, s), x = p(h, s) / _;
      m(s, r, e);
      var y = p(h, s) / _;
      m(s, e, t);
      var g = p(h, s) / _;
      k.scale(o, i, x), k.scaleAndAdd(o, o, n, y), k.scaleAndAdd(o, o, a, g);
      var b = o.x * this._chart.getWidth(), w = o.y * this._chart.getHeight();
      this._chart.getZr().handler.dispatch(l, {
        zrX: b,
        zrY: w
      });
    };
  })(),
  attachToMesh: function(e) {
    this._meshes.indexOf(e) >= 0 || (mn.forEach(function(t) {
      e.on(t, this[_n(t)], this);
    }, this), this._meshes.push(e));
  },
  detachFromMesh: function(e) {
    var t = this._meshes.indexOf(e);
    t >= 0 && this._meshes.splice(t, 1), mn.forEach(function(r) {
      e.off(r, this[_n(r)]);
    }, this);
  },
  dispose: function() {
    this._meshes.forEach(function(e) {
      this.detachFromMesh(e);
    }, this);
  }
};
var wr = Kt.extend(
  /** @lends clay.camera.Orthographic# */
  {
    /**
     * @type {number}
     */
    left: -1,
    /**
     * @type {number}
     */
    right: 1,
    /**
     * @type {number}
     */
    near: -1,
    /**
     * @type {number}
     */
    far: 1,
    /**
     * @type {number}
     */
    top: 1,
    /**
     * @type {number}
     */
    bottom: -1
  },
  /** @lends clay.camera.Orthographic.prototype */
  {
    updateProjectionMatrix: function() {
      this.projectionMatrix.ortho(this.left, this.right, this.bottom, this.top, this.near, this.far);
    },
    decomposeProjectionMatrix: function() {
      var e = this.projectionMatrix.array;
      this.left = (-1 - e[12]) / e[0], this.right = (1 - e[12]) / e[0], this.top = (1 - e[13]) / e[5], this.bottom = (-1 - e[13]) / e[5], this.near = -(-1 - e[14]) / e[10], this.far = -(1 - e[14]) / e[10];
    },
    /**
     * @return {clay.camera.Orthographic}
     */
    clone: function() {
      var e = Kt.prototype.clone.call(this);
      return e.left = this.left, e.right = this.right, e.near = this.near, e.far = this.far, e.top = this.top, e.bottom = this.bottom, e;
    }
  }
);
const wf = `
@export clay.compositor.vertex
uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
attribute vec3 position : POSITION;
attribute vec2 texcoord : TEXCOORD_0;
varying vec2 v_Texcoord;
void main()
{
 v_Texcoord = texcoord;
 gl_Position = worldViewProjection * vec4(position, 1.0);
}
@end`;
U.import(wf);
var bf = new On(), Ua = new Oe({
  geometry: bf,
  frustumCulling: !1
}), Ef = new wr(), De = Ae.extend(
  function() {
    return (
      /** @lends clay.compositor.Pass# */
      {
        /**
         * Fragment shader string
         * @type {string}
         */
        // PENDING shader or fragment ?
        fragment: "",
        /**
         * @type {Object}
         */
        outputs: null,
        /**
         * @type {clay.Material}
         */
        material: null,
        /**
         * @type {Boolean}
         */
        blendWithPrevious: !1,
        /**
         * @type {Boolean}
         */
        clearColor: !1,
        /**
         * @type {Boolean}
         */
        clearDepth: !0
      }
    );
  },
  function() {
    var e = new U(U.source("clay.compositor.vertex"), this.fragment), t = new ft({
      shader: e
    });
    t.enableTexturesAll(), this.material = t;
  },
  /** @lends clay.compositor.Pass.prototype */
  {
    /**
     * @param {string} name
     * @param {} value
     */
    setUniform: function(e, t) {
      this.material.setUniform(e, t);
    },
    /**
     * @param  {string} name
     * @return {}
     */
    getUniform: function(e) {
      var t = this.material.uniforms[e];
      if (t)
        return t.value;
    },
    /**
     * @param  {clay.Texture} texture
     * @param  {number} attachment
     */
    attachOutput: function(e, t) {
      this.outputs || (this.outputs = {}), t = t || M.COLOR_ATTACHMENT0, this.outputs[t] = e;
    },
    /**
     * @param  {clay.Texture} texture
     */
    detachOutput: function(e) {
      for (var t in this.outputs)
        this.outputs[t] === e && (this.outputs[t] = null);
    },
    bind: function(e, t) {
      if (this.outputs)
        for (var r in this.outputs) {
          var i = this.outputs[r];
          i && t.attach(i, r);
        }
      t && t.bind(e);
    },
    unbind: function(e, t) {
      t.unbind(e);
    },
    /**
     * @param  {clay.Renderer} renderer
     * @param  {clay.FrameBuffer} [frameBuffer]
     */
    render: function(e, t) {
      var r = e.gl;
      if (t) {
        this.bind(e, t);
        var i = e.getGLExtension("EXT_draw_buffers");
        if (i && this.outputs) {
          var n = [];
          for (var a in this.outputs)
            a = +a, a >= r.COLOR_ATTACHMENT0 && a <= r.COLOR_ATTACHMENT0 + 8 && n.push(a);
          i.drawBuffersEXT(n);
        }
      }
      this.trigger("beforerender", this, e);
      var o = this.clearDepth ? r.DEPTH_BUFFER_BIT : 0;
      if (r.depthMask(!0), this.clearColor) {
        o = o | r.COLOR_BUFFER_BIT, r.colorMask(!0, !0, !0, !0);
        var s = this.clearColor;
        Array.isArray(s) && r.clearColor(s[0], s[1], s[2], s[3]);
      }
      r.clear(o), this.blendWithPrevious ? (r.enable(r.BLEND), this.material.transparent = !0) : (r.disable(r.BLEND), this.material.transparent = !1), this.renderQuad(e), this.trigger("afterrender", this, e), t && this.unbind(e, t);
    },
    /**
     * Simply do quad rendering
     */
    renderQuad: function(e) {
      Ua.material = this.material, e.renderPass([Ua], Ef);
    },
    /**
     * @param  {clay.Renderer} renderer
     */
    dispose: function(e) {
    }
  }
);
const Sf = `#define SAMPLE_NUMBER 1024
#define PI 3.14159265358979
uniform sampler2D normalDistribution;
uniform vec2 viewportSize : [512, 256];
const vec3 N = vec3(0.0, 0.0, 1.0);
const float fSampleNumber = float(SAMPLE_NUMBER);
vec3 importanceSampleNormal(float i, float roughness, vec3 N) {
 vec3 H = texture2D(normalDistribution, vec2(roughness, i)).rgb;
 vec3 upVector = abs(N.y) > 0.999 ? vec3(1.0, 0.0, 0.0) : vec3(0.0, 1.0, 0.0);
 vec3 tangentX = normalize(cross(N, upVector));
 vec3 tangentZ = cross(N, tangentX);
 return normalize(tangentX * H.x + N * H.y + tangentZ * H.z);
}
float G_Smith(float roughness, float NoV, float NoL) {
 float k = roughness * roughness / 2.0;
 float G1V = NoV / (NoV * (1.0 - k) + k);
 float G1L = NoL / (NoL * (1.0 - k) + k);
 return G1L * G1V;
}
void main() {
 vec2 uv = gl_FragCoord.xy / viewportSize;
 float NoV = uv.x;
 float roughness = uv.y;
 vec3 V;
 V.x = sqrt(1.0 - NoV * NoV);
 V.y = 0.0;
 V.z = NoV;
 float A = 0.0;
 float B = 0.0;
 for (int i = 0; i < SAMPLE_NUMBER; i++) {
 vec3 H = importanceSampleNormal(float(i) / fSampleNumber, roughness, N);
 vec3 L = reflect(-V, H);
 float NoL = clamp(L.z, 0.0, 1.0);
 float NoH = clamp(H.z, 0.0, 1.0);
 float VoH = clamp(dot(V, H), 0.0, 1.0);
 if (NoL > 0.0) {
 float G = G_Smith(roughness, NoV, NoL);
 float G_Vis = G * VoH / (NoH * NoV);
 float Fc = pow(1.0 - VoH, 5.0);
 A += (1.0 - Fc) * G_Vis;
 B += Fc * G_Vis;
 }
 }
 gl_FragColor = vec4(vec2(A, B) / fSampleNumber, 0.0, 1.0);
}
`, Af = `#define SHADER_NAME prefilter
#define SAMPLE_NUMBER 1024
#define PI 3.14159265358979
uniform mat4 viewInverse : VIEWINVERSE;
uniform samplerCube environmentMap;
uniform sampler2D normalDistribution;
uniform float roughness : 0.5;
varying vec2 v_Texcoord;
varying vec3 v_WorldPosition;
@import clay.util.rgbm
vec3 importanceSampleNormal(float i, float roughness, vec3 N) {
 vec3 H = texture2D(normalDistribution, vec2(roughness, i)).rgb;
 vec3 upVector = abs(N.y) > 0.999 ? vec3(1.0, 0.0, 0.0) : vec3(0.0, 1.0, 0.0);
 vec3 tangentX = normalize(cross(N, upVector));
 vec3 tangentZ = cross(N, tangentX);
 return normalize(tangentX * H.x + N * H.y + tangentZ * H.z);
}
void main() {
 vec3 eyePos = viewInverse[3].xyz;
 vec3 V = normalize(v_WorldPosition - eyePos);
 vec3 N = V;
 vec3 prefilteredColor = vec3(0.0);
 float totalWeight = 0.0;
 float fMaxSampleNumber = float(SAMPLE_NUMBER);
 for (int i = 0; i < SAMPLE_NUMBER; i++) {
 vec3 H = importanceSampleNormal(float(i) / fMaxSampleNumber, roughness, N);
 vec3 L = reflect(-V, H);
 float NoL = clamp(dot(N, L), 0.0, 1.0);
 if (NoL > 0.0) {
 prefilteredColor += decodeHDR(textureCube(environmentMap, L)).rgb * NoL;
 totalWeight += NoL;
 }
 }
 gl_FragColor = encodeHDR(vec4(prefilteredColor / totalWeight, 1.0));
}
`;
var tt = {}, ji = ["px", "nx", "py", "ny", "pz", "nz"];
tt.prefilterEnvironmentMap = function(e, t, r, i, n) {
  (!n || !i) && (i = tt.generateNormalDistribution(), n = tt.integrateBRDF(e, i)), r = r || {};
  var a = r.width || 64, o = r.height || 64, s = r.type || t.type, l = new Rt({
    width: a,
    height: o,
    type: s,
    flipY: !1,
    mipmaps: []
  });
  l.isPowerOfTwo() || console.warn("Width and height must be power of two to enable mipmap.");
  var u = Math.min(a, o), f = Math.log(u) / Math.log(2) + 1, h = new ft({
    shader: new U({
      vertex: U.source("clay.skybox.vertex"),
      fragment: Af
    })
  });
  h.set("normalDistribution", i), r.encodeRGBM && h.define("fragment", "RGBM_ENCODE"), r.decodeRGBM && h.define("fragment", "RGBM_DECODE");
  var d = new gt(), c;
  if (t.textureType === "texture2D") {
    var v = new Rt({
      width: a,
      height: o,
      // FIXME FLOAT type will cause GL_FRAMEBUFFER_INCOMPLETE_ATTACHMENT error on iOS
      type: s === N.FLOAT ? N.HALF_FLOAT : s
    });
    et.panoramaToCubeMap(e, t, v, {
      // PENDING encodeRGBM so it can be decoded as RGBM
      encodeRGBM: r.decodeRGBM
    }), t = v;
  }
  c = new Tr({
    scene: d,
    material: h
  }), c.material.set("environmentMap", t);
  var p = new Nn({
    texture: l
  });
  r.encodeRGBM && (s = l.type = N.UNSIGNED_BYTE);
  for (var m = new K({
    width: a,
    height: o,
    type: s
  }), _ = new pe({
    depthBuffer: !1
  }), x = Q[s === N.UNSIGNED_BYTE ? "Uint8Array" : "Float32Array"], y = 0; y < f; y++) {
    l.mipmaps[y] = {
      pixels: {}
    }, c.material.set("roughness", y / (f - 1));
    for (var g = m.width, b = 2 * Math.atan(g / (g - 0.5)) / Math.PI * 180, w = 0; w < ji.length; w++) {
      var C = new x(m.width * m.height * 4);
      _.attach(m), _.bind(e);
      var A = p.getCamera(ji[w]);
      A.fov = b, e.render(d, A), e.gl.readPixels(
        0,
        0,
        m.width,
        m.height,
        N.RGBA,
        s,
        C
      ), _.unbind(e), l.mipmaps[y].pixels[ji[w]] = C;
    }
    m.width /= 2, m.height /= 2, m.dirty();
  }
  return _.dispose(e), m.dispose(e), c.dispose(e), i.dispose(e), {
    environmentMap: l,
    brdfLookup: n,
    normalDistribution: i,
    maxMipmapLevel: f
  };
};
tt.integrateBRDF = function(e, t) {
  t = t || tt.generateNormalDistribution();
  var r = new pe({
    depthBuffer: !1
  }), i = new De({
    fragment: Sf
  }), n = new K({
    width: 512,
    height: 256,
    type: N.HALF_FLOAT,
    wrapS: N.CLAMP_TO_EDGE,
    wrapT: N.CLAMP_TO_EDGE,
    minFilter: N.NEAREST,
    magFilter: N.NEAREST,
    useMipmap: !1
  });
  return i.setUniform("normalDistribution", t), i.setUniform("viewportSize", [512, 256]), i.attachOutput(n), i.render(e, r), r.dispose(e), n;
};
tt.generateNormalDistribution = function(r, i) {
  for (var r = r || 256, i = i || 1024, n = new K({
    width: r,
    height: i,
    type: N.FLOAT,
    minFilter: N.NEAREST,
    magFilter: N.NEAREST,
    wrapS: N.CLAMP_TO_EDGE,
    wrapT: N.CLAMP_TO_EDGE,
    useMipmap: !1
  }), a = new Float32Array(i * r * 4), o = [], s = 0; s < r; s++) {
    for (var l = s / r, u = l * l, f = 0; f < i; f++) {
      var h = (f << 16 | f >>> 16) >>> 0;
      h = ((h & 1431655765) << 1 | (h & 2863311530) >>> 1) >>> 0, h = ((h & 858993459) << 2 | (h & 3435973836) >>> 2) >>> 0, h = ((h & 252645135) << 4 | (h & 4042322160) >>> 4) >>> 0, h = (((h & 16711935) << 8 | (h & 4278255360) >>> 8) >>> 0) / 4294967296;
      var d = Math.sqrt((1 - h) / (1 + (u * u - 1) * h));
      o[f] = d;
    }
    for (var f = 0; f < i; f++) {
      var c = (f * r + s) * 4, d = o[f], v = Math.sqrt(1 - d * d), p = f / i, m = 2 * Math.PI * p;
      a[c] = v * Math.cos(m), a[c + 1] = d, a[c + 2] = v * Math.sin(m), a[c + 3] = 1;
    }
  }
  return n.pixels = a, n;
};
var Cf = Ze.extend(
  {
    /**
     * @type {clay.TextureCube}
     * @memberOf clay.light.AmbientCubemap#
     */
    cubemap: null,
    // TODO
    // range: 100,
    castShadow: !1,
    _normalDistribution: null,
    _brdfLookup: null
  },
  /** @lends clay.light.AmbientCubemap# */
  {
    type: "AMBIENT_CUBEMAP_LIGHT",
    /**
     * Do prefitering the cubemap
     * @param {clay.Renderer} renderer
     * @param {number} [size=32]
     */
    prefilter: function(e, t) {
      if (!e.getGLExtension("EXT_shader_texture_lod")) {
        console.warn("Device not support textureCubeLodEXT");
        return;
      }
      this._brdfLookup || (this._normalDistribution = tt.generateNormalDistribution(), this._brdfLookup = tt.integrateBRDF(e, this._normalDistribution));
      var r = this.cubemap;
      if (!r.__prefiltered) {
        var i = tt.prefilterEnvironmentMap(
          e,
          r,
          {
            encodeRGBM: !0,
            width: t,
            height: t
          },
          this._normalDistribution,
          this._brdfLookup
        );
        this.cubemap = i.environmentMap, this.cubemap.__prefiltered = !0, r.dispose(e);
      }
    },
    getBRDFLookup: function() {
      return this._brdfLookup;
    },
    uniformTemplates: {
      ambientCubemapLightColor: {
        type: "3f",
        value: function(e) {
          var t = e.color, r = e.intensity;
          return [t[0] * r, t[1] * r, t[2] * r];
        }
      },
      ambientCubemapLightCubemap: {
        type: "t",
        value: function(e) {
          return e.cubemap;
        }
      },
      ambientCubemapLightBRDFLookup: {
        type: "t",
        value: function(e) {
          return e._brdfLookup;
        }
      }
    }
    /**
     * @function
     * @name clone
     * @return {clay.light.AmbientCubemap}
     * @memberOf clay.light.AmbientCubemap.prototype
     */
  }
), Mf = Ze.extend({
  castShadow: !1,
  /**
   * Spherical Harmonic Coefficients
   * @type {Array.<number>}
   * @memberOf clay.light.AmbientSH#
   */
  coefficients: []
}, function() {
  this._coefficientsTmpArr = new Q.Float32Array(27);
}, {
  type: "AMBIENT_SH_LIGHT",
  uniformTemplates: {
    ambientSHLightColor: {
      type: "3f",
      value: function(e) {
        var t = e.color, r = e.intensity;
        return [t[0] * r, t[1] * r, t[2] * r];
      }
    },
    ambientSHLightCoefficients: {
      type: "3f",
      value: function(e) {
        for (var t = e._coefficientsTmpArr, r = 0; r < e.coefficients.length; r++)
          t[r] = e.coefficients[r];
        return t;
      }
    }
  }
  /**
   * @function
   * @name clone
   * @return {clay.light.Ambient}
   * @memberOf clay.light.Ambient.prototype
   */
}), ko = {}, Dt = ["px", "nx", "py", "ny", "pz", "nz"];
function Lf(e, t) {
  var r = e[0], i = e[1], n = e[2];
  return t === 0 ? 1 : t === 1 ? r : t === 2 ? i : t === 3 ? n : t === 4 ? r * n : t === 5 ? i * n : t === 6 ? r * i : t === 7 ? 3 * n * n - 1 : r * r - i * i;
}
var Df = {
  px: [2, 1, 0, -1, -1, 1],
  nx: [2, 1, 0, 1, -1, -1],
  py: [0, 2, 1, 1, -1, -1],
  ny: [0, 2, 1, 1, 1, 1],
  pz: [0, 1, 2, -1, -1, -1],
  nz: [0, 1, 2, 1, -1, 1]
};
function Pf(e, t, r, i) {
  for (var n = new Q.Float32Array(27), a = T.create(), o = T.create(), s = T.create(), l = 0; l < 9; l++) {
    for (var u = T.create(), f = 0; f < Dt.length; f++) {
      for (var h = t[Dt[f]], d = T.create(), c = 0, v = 0, p = Df[Dt[f]], m = 0; m < i; m++)
        for (var _ = 0; _ < r; _++) {
          a[0] = _ / (r - 1) * 2 - 1, a[1] = m / (i - 1) * 2 - 1, a[2] = -1, T.normalize(a, a), s[0] = a[p[0]] * p[3], s[1] = a[p[1]] * p[4], s[2] = a[p[2]] * p[5], o[0] = h[v++] / 255, o[1] = h[v++] / 255, o[2] = h[v++] / 255;
          var x = h[v++] / 255 * 8.12;
          o[0] *= x, o[1] *= x, o[2] *= x, T.scaleAndAdd(d, d, o, Lf(s, l) * -a[2]), c += -a[2];
        }
      T.scaleAndAdd(u, u, d, 1 / c);
    }
    n[l * 3] = u[0] / 6, n[l * 3 + 1] = u[1] / 6, n[l * 3 + 2] = u[2] / 6;
  }
  return n;
}
ko.projectEnvironmentMap = function(e, t, r) {
  r = r || {}, r.lod = r.lod || 0;
  var i, n = new gt(), a = 64;
  t.textureType === "texture2D" ? i = new Tr({
    scene: n,
    environmentMap: t
  }) : (a = t.image && t.image.px ? t.image.px.width : t.width, i = new Tr({
    scene: n,
    environmentMap: t
  }));
  var o = Math.ceil(a / Math.pow(2, r.lod)), s = Math.ceil(a / Math.pow(2, r.lod)), l = new K({
    width: o,
    height: s
  }), u = new pe();
  i.material.define("fragment", "RGBM_ENCODE"), r.decodeRGBM && i.material.define("fragment", "RGBM_DECODE"), i.material.set("lod", r.lod);
  for (var f = new Nn({
    texture: l
  }), h = {}, d = 0; d < Dt.length; d++) {
    h[Dt[d]] = new Uint8Array(o * s * 4);
    var c = f.getCamera(Dt[d]);
    c.fov = 90, u.attach(l), u.bind(e), e.render(n, c), e.gl.readPixels(
      0,
      0,
      o,
      s,
      N.RGBA,
      N.UNSIGNED_BYTE,
      h[Dt[d]]
    ), u.unbind(e);
  }
  return i.dispose(e), u.dispose(e), l.dispose(e), Pf(e, h, o, s);
};
var be = {
  firstNotNull: function() {
    for (var e = 0, t = arguments.length; e < t; e++)
      if (arguments[e] != null)
        return arguments[e];
  },
  /**
   * @param {module:echarts/data/List} data
   * @param {Object} payload Contains dataIndex (means rawIndex) / dataIndexInside / name
   *                         each of which can be Array or primary type.
   * @return {number|Array.<number>} dataIndex If not found, return undefined/null.
   */
  queryDataIndex: function(e, t) {
    if (t.dataIndexInside != null)
      return t.dataIndexInside;
    if (t.dataIndex != null)
      return it(t.dataIndex) ? ia(t.dataIndex, function(r) {
        return e.indexOfRawIndex(r);
      }) : e.indexOfRawIndex(t.dataIndex);
    if (t.name != null)
      return it(t.name) ? ia(t.name, function(r) {
        return e.indexOfName(r);
      }) : e.indexOfName(t.name);
  }
}, Rf = $.extend(
  /** @lends clay.geometry.Sphere# */
  {
    dynamic: !1,
    /**
     * @type {number}
     */
    widthSegments: 40,
    /**
     * @type {number}
     */
    heightSegments: 20,
    /**
     * @type {number}
     */
    phiStart: 0,
    /**
     * @type {number}
     */
    phiLength: Math.PI * 2,
    /**
     * @type {number}
     */
    thetaStart: 0,
    /**
     * @type {number}
     */
    thetaLength: Math.PI,
    /**
     * @type {number}
     */
    radius: 1
  },
  function() {
    this.build();
  },
  /** @lends clay.geometry.Sphere.prototype */
  {
    /**
     * Build sphere geometry
     */
    build: function() {
      var e = this.heightSegments, t = this.widthSegments, r = this.attributes.position, i = this.attributes.texcoord0, n = this.attributes.normal, a = (t + 1) * (e + 1);
      r.init(a), i.init(a), n.init(a);
      var o = a > 65535 ? Uint32Array : Uint16Array, s = this.indices = new o(t * e * 6), l, u, f, h, d, c, v, y = this.radius, p = this.phiStart, m = this.phiLength, _ = this.thetaStart, x = this.thetaLength, y = this.radius, g = [], b = [], w = 0, C = 1 / y;
      for (v = 0; v <= e; v++)
        for (c = 0; c <= t; c++)
          h = c / t, d = v / e, l = -y * Math.cos(p + h * m) * Math.sin(_ + d * x), u = y * Math.cos(_ + d * x), f = y * Math.sin(p + h * m) * Math.sin(_ + d * x), g[0] = l, g[1] = u, g[2] = f, b[0] = h, b[1] = d, r.set(w, g), i.set(w, b), g[0] *= C, g[1] *= C, g[2] *= C, n.set(w, g), w++;
      var A, R, H, O, z = t + 1, G = 0;
      for (v = 0; v < e; v++)
        for (c = 0; c < t; c++)
          R = v * z + c, A = v * z + c + 1, O = (v + 1) * z + c + 1, H = (v + 1) * z + c, s[G++] = A, s[G++] = R, s[G++] = O, s[G++] = R, s[G++] = H, s[G++] = O;
      this.boundingBox = new we(), this.boundingBox.max.set(y, y, y), this.boundingBox.min.set(-y, -y, -y);
    }
  }
), Nf = Ze.extend({
  castShadow: !1
}, {
  type: "AMBIENT_LIGHT",
  uniformTemplates: {
    ambientLightColor: {
      type: "3f",
      value: function(e) {
        var t = e.color, r = e.intensity;
        return [t[0] * r, t[1] * r, t[2] * r];
      }
    }
  }
  /**
   * @function
   * @name clone
   * @return {clay.light.Ambient}
   * @memberOf clay.light.Ambient.prototype
   */
}), Of = Ze.extend(
  /** @lends clay.light.Directional# */
  {
    /**
     * @type {number}
     */
    shadowBias: 1e-3,
    /**
     * @type {number}
     */
    shadowSlopeScale: 2,
    /**
     * Shadow cascade.
     * Use PSSM technique when it is larger than 1 and have a unique directional light in scene.
     * @type {number}
     */
    shadowCascade: 1,
    /**
     * Available when shadowCascade is larger than 1 and have a unique directional light in scene.
     * @type {number}
     */
    cascadeSplitLogFactor: 0.2
  },
  {
    type: "DIRECTIONAL_LIGHT",
    uniformTemplates: {
      directionalLightDirection: {
        type: "3f",
        value: function(e) {
          return e.__dir = e.__dir || new S(), e.__dir.copy(e.worldTransform.z).normalize().negate().array;
        }
      },
      directionalLightColor: {
        type: "3f",
        value: function(e) {
          var t = e.color, r = e.intensity;
          return [t[0] * r, t[1] * r, t[2] * r];
        }
      }
    },
    /**
     * @return {clay.light.Directional}
     * @memberOf clay.light.Directional.prototype
     */
    clone: function() {
      var e = Ze.prototype.clone.call(this);
      return e.shadowBias = this.shadowBias, e.shadowSlopeScale = this.shadowSlopeScale, e;
    }
  }
), If = Ze.extend(
  /** @lends clay.light.Point# */
  {
    /**
     * @type {number}
     */
    range: 100,
    /**
     * @type {number}
     */
    castShadow: !1
  },
  {
    type: "POINT_LIGHT",
    uniformTemplates: {
      pointLightPosition: {
        type: "3f",
        value: function(e) {
          return e.getWorldPosition().array;
        }
      },
      pointLightRange: {
        type: "1f",
        value: function(e) {
          return e.range;
        }
      },
      pointLightColor: {
        type: "3f",
        value: function(e) {
          var t = e.color, r = e.intensity;
          return [t[0] * r, t[1] * r, t[2] * r];
        }
      }
    },
    /**
     * @return {clay.light.Point}
     * @memberOf clay.light.Point.prototype
     */
    clone: function() {
      var e = Ze.prototype.clone.call(this);
      return e.range = this.range, e;
    }
  }
), Bf = Ze.extend(
  /**@lends clay.light.Spot */
  {
    /**
     * @type {number}
     */
    range: 20,
    /**
     * @type {number}
     */
    umbraAngle: 30,
    /**
     * @type {number}
     */
    penumbraAngle: 45,
    /**
     * @type {number}
     */
    falloffFactor: 2,
    /**
     * @type {number}
     */
    shadowBias: 1e-3,
    /**
     * @type {number}
     */
    shadowSlopeScale: 2
  },
  {
    type: "SPOT_LIGHT",
    uniformTemplates: {
      spotLightPosition: {
        type: "3f",
        value: function(e) {
          return e.getWorldPosition().array;
        }
      },
      spotLightRange: {
        type: "1f",
        value: function(e) {
          return e.range;
        }
      },
      spotLightUmbraAngleCosine: {
        type: "1f",
        value: function(e) {
          return Math.cos(e.umbraAngle * Math.PI / 180);
        }
      },
      spotLightPenumbraAngleCosine: {
        type: "1f",
        value: function(e) {
          return Math.cos(e.penumbraAngle * Math.PI / 180);
        }
      },
      spotLightFalloffFactor: {
        type: "1f",
        value: function(e) {
          return e.falloffFactor;
        }
      },
      spotLightDirection: {
        type: "3f",
        value: function(e) {
          return e.__dir = e.__dir || new S(), e.__dir.copy(e.worldTransform.z).negate().array;
        }
      },
      spotLightColor: {
        type: "3f",
        value: function(e) {
          var t = e.color, r = e.intensity;
          return [t[0] * r, t[1] * r, t[2] * r];
        }
      }
    },
    /**
     * @return {clay.light.Spot}
     * @memberOf clay.light.Spot.prototype
     */
    clone: function() {
      var e = Ze.prototype.clone.call(this);
      return e.range = this.range, e.umbraAngle = this.umbraAngle, e.penumbraAngle = this.penumbraAngle, e.falloffFactor = this.falloffFactor, e.shadowBias = this.shadowBias, e.shadowSlopeScale = this.shadowSlopeScale, e;
    }
  }
), Z = function(e, t, r, i) {
  e = e || 0, t = t || 0, r = r || 0, i = i || 0, this.array = L.fromValues(e, t, r, i), this._dirty = !0;
};
Z.prototype = {
  constructor: Z,
  /**
   * Add b to self
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  add: function(e) {
    return L.add(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Set x, y and z components
   * @param  {number}  x
   * @param  {number}  y
   * @param  {number}  z
   * @param  {number}  w
   * @return {clay.Vector4}
   */
  set: function(e, t, r, i) {
    return this.array[0] = e, this.array[1] = t, this.array[2] = r, this.array[3] = i, this._dirty = !0, this;
  },
  /**
   * Set x, y, z and w components from array
   * @param  {Float32Array|number[]} arr
   * @return {clay.Vector4}
   */
  setArray: function(e) {
    return this.array[0] = e[0], this.array[1] = e[1], this.array[2] = e[2], this.array[3] = e[3], this._dirty = !0, this;
  },
  /**
   * Clone a new Vector4
   * @return {clay.Vector4}
   */
  clone: function() {
    return new Z(this.x, this.y, this.z, this.w);
  },
  /**
   * Copy from b
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  copy: function(e) {
    return L.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for distance
   * @param  {clay.Vector4} b
   * @return {number}
   */
  dist: function(e) {
    return L.dist(this.array, e.array);
  },
  /**
   * Distance between self and b
   * @param  {clay.Vector4} b
   * @return {number}
   */
  distance: function(e) {
    return L.distance(this.array, e.array);
  },
  /**
   * Alias for divide
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  div: function(e) {
    return L.div(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Divide self by b
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  divide: function(e) {
    return L.divide(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Dot product of self and b
   * @param  {clay.Vector4} b
   * @return {number}
   */
  dot: function(e) {
    return L.dot(this.array, e.array);
  },
  /**
   * Alias of length
   * @return {number}
   */
  len: function() {
    return L.len(this.array);
  },
  /**
   * Calculate the length
   * @return {number}
   */
  length: function() {
    return L.length(this.array);
  },
  /**
   * Linear interpolation between a and b
   * @param  {clay.Vector4} a
   * @param  {clay.Vector4} b
   * @param  {number}  t
   * @return {clay.Vector4}
   */
  lerp: function(e, t, r) {
    return L.lerp(this.array, e.array, t.array, r), this._dirty = !0, this;
  },
  /**
   * Minimum of self and b
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  min: function(e) {
    return L.min(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Maximum of self and b
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  max: function(e) {
    return L.max(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiply
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  mul: function(e) {
    return L.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Mutiply self and b
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  multiply: function(e) {
    return L.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Negate self
   * @return {clay.Vector4}
   */
  negate: function() {
    return L.negate(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Normalize self
   * @return {clay.Vector4}
   */
  normalize: function() {
    return L.normalize(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Generate random x, y, z, w components with a given scale
   * @param  {number} scale
   * @return {clay.Vector4}
   */
  random: function(e) {
    return L.random(this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale self
   * @param  {number}  scale
   * @return {clay.Vector4}
   */
  scale: function(e) {
    return L.scale(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale b and add to self
   * @param  {clay.Vector4} b
   * @param  {number}  scale
   * @return {clay.Vector4}
   */
  scaleAndAdd: function(e, t) {
    return L.scaleAndAdd(this.array, this.array, e.array, t), this._dirty = !0, this;
  },
  /**
   * Alias for squaredDistance
   * @param  {clay.Vector4} b
   * @return {number}
   */
  sqrDist: function(e) {
    return L.sqrDist(this.array, e.array);
  },
  /**
   * Squared distance between self and b
   * @param  {clay.Vector4} b
   * @return {number}
   */
  squaredDistance: function(e) {
    return L.squaredDistance(this.array, e.array);
  },
  /**
   * Alias for squaredLength
   * @return {number}
   */
  sqrLen: function() {
    return L.sqrLen(this.array);
  },
  /**
   * Squared length of self
   * @return {number}
   */
  squaredLength: function() {
    return L.squaredLength(this.array);
  },
  /**
   * Alias for subtract
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  sub: function(e) {
    return L.sub(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Subtract b from self
   * @param  {clay.Vector4} b
   * @return {clay.Vector4}
   */
  subtract: function(e) {
    return L.subtract(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Matrix4 m
   * @param  {clay.Matrix4} m
   * @return {clay.Vector4}
   */
  transformMat4: function(e) {
    return L.transformMat4(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transform self with a Quaternion q
   * @param  {clay.Quaternion} q
   * @return {clay.Vector4}
   */
  transformQuat: function(e) {
    return L.transformQuat(this.array, this.array, e.array), this._dirty = !0, this;
  },
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
var ur = Object.defineProperty;
if (ur) {
  var zr = Z.prototype;
  ur(zr, "x", {
    get: function() {
      return this.array[0];
    },
    set: function(e) {
      this.array[0] = e, this._dirty = !0;
    }
  }), ur(zr, "y", {
    get: function() {
      return this.array[1];
    },
    set: function(e) {
      this.array[1] = e, this._dirty = !0;
    }
  }), ur(zr, "z", {
    get: function() {
      return this.array[2];
    },
    set: function(e) {
      this.array[2] = e, this._dirty = !0;
    }
  }), ur(zr, "w", {
    get: function() {
      return this.array[3];
    },
    set: function(e) {
      this.array[3] = e, this._dirty = !0;
    }
  });
}
Z.add = function(e, t, r) {
  return L.add(e.array, t.array, r.array), e._dirty = !0, e;
};
Z.set = function(e, t, r, i, n) {
  L.set(e.array, t, r, i, n), e._dirty = !0;
};
Z.copy = function(e, t) {
  return L.copy(e.array, t.array), e._dirty = !0, e;
};
Z.dist = function(e, t) {
  return L.distance(e.array, t.array);
};
Z.distance = Z.dist;
Z.div = function(e, t, r) {
  return L.divide(e.array, t.array, r.array), e._dirty = !0, e;
};
Z.divide = Z.div;
Z.dot = function(e, t) {
  return L.dot(e.array, t.array);
};
Z.len = function(e) {
  return L.length(e.array);
};
Z.lerp = function(e, t, r, i) {
  return L.lerp(e.array, t.array, r.array, i), e._dirty = !0, e;
};
Z.min = function(e, t, r) {
  return L.min(e.array, t.array, r.array), e._dirty = !0, e;
};
Z.max = function(e, t, r) {
  return L.max(e.array, t.array, r.array), e._dirty = !0, e;
};
Z.mul = function(e, t, r) {
  return L.multiply(e.array, t.array, r.array), e._dirty = !0, e;
};
Z.multiply = Z.mul;
Z.negate = function(e, t) {
  return L.negate(e.array, t.array), e._dirty = !0, e;
};
Z.normalize = function(e, t) {
  return L.normalize(e.array, t.array), e._dirty = !0, e;
};
Z.random = function(e, t) {
  return L.random(e.array, t), e._dirty = !0, e;
};
Z.scale = function(e, t, r) {
  return L.scale(e.array, t.array, r), e._dirty = !0, e;
};
Z.scaleAndAdd = function(e, t, r, i) {
  return L.scaleAndAdd(e.array, t.array, r.array, i), e._dirty = !0, e;
};
Z.sqrDist = function(e, t) {
  return L.sqrDist(e.array, t.array);
};
Z.squaredDistance = Z.sqrDist;
Z.sqrLen = function(e) {
  return L.sqrLen(e.array);
};
Z.squaredLength = Z.sqrLen;
Z.sub = function(e, t, r) {
  return L.subtract(e.array, t.array, r.array), e._dirty = !0, e;
};
Z.subtract = Z.sub;
Z.transformMat4 = function(e, t, r) {
  return L.transformMat4(e.array, t.array, r.array), e._dirty = !0, e;
};
Z.transformQuat = function(e, t, r) {
  return L.transformQuat(e.array, t.array, r.array), e._dirty = !0, e;
};
var Y = {};
Y.create = function() {
  var e = new Ee(4);
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 1, e;
};
Y.clone = function(e) {
  var t = new Ee(4);
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t;
};
Y.copy = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e;
};
Y.identity = function(e) {
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 1, e;
};
Y.transpose = function(e, t) {
  if (e === t) {
    var r = t[1];
    e[1] = t[2], e[2] = r;
  } else
    e[0] = t[0], e[1] = t[2], e[2] = t[1], e[3] = t[3];
  return e;
};
Y.invert = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = r * a - n * i;
  return o ? (o = 1 / o, e[0] = a * o, e[1] = -i * o, e[2] = -n * o, e[3] = r * o, e) : null;
};
Y.adjoint = function(e, t) {
  var r = t[0];
  return e[0] = t[3], e[1] = -t[1], e[2] = -t[2], e[3] = r, e;
};
Y.determinant = function(e) {
  return e[0] * e[3] - e[2] * e[1];
};
Y.multiply = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = r[0], l = r[1], u = r[2], f = r[3];
  return e[0] = i * s + a * l, e[1] = n * s + o * l, e[2] = i * u + a * f, e[3] = n * u + o * f, e;
};
Y.mul = Y.multiply;
Y.rotate = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = Math.sin(r), l = Math.cos(r);
  return e[0] = i * l + a * s, e[1] = n * l + o * s, e[2] = i * -s + a * l, e[3] = n * -s + o * l, e;
};
Y.scale = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = r[0], l = r[1];
  return e[0] = i * s, e[1] = n * s, e[2] = a * l, e[3] = o * l, e;
};
Y.frob = function(e) {
  return Math.sqrt(Math.pow(e[0], 2) + Math.pow(e[1], 2) + Math.pow(e[2], 2) + Math.pow(e[3], 2));
};
Y.LDU = function(e, t, r, i) {
  return e[2] = i[2] / i[0], r[0] = i[0], r[1] = i[1], r[3] = i[3] - e[2] * r[1], [e, t, r];
};
var Pe = function() {
  this.array = Y.create(), this._dirty = !0;
};
Pe.prototype = {
  constructor: Pe,
  /**
   * Set components from array
   * @param  {Float32Array|number[]} arr
   */
  setArray: function(e) {
    for (var t = 0; t < this.array.length; t++)
      this.array[t] = e[t];
    return this._dirty = !0, this;
  },
  /**
   * Clone a new Matrix2
   * @return {clay.Matrix2}
   */
  clone: function() {
    return new Pe().copy(this);
  },
  /**
   * Copy from b
   * @param  {clay.Matrix2} b
   * @return {clay.Matrix2}
   */
  copy: function(e) {
    return Y.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Calculate the adjugate of self, in-place
   * @return {clay.Matrix2}
   */
  adjoint: function() {
    return Y.adjoint(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Calculate matrix determinant
   * @return {number}
   */
  determinant: function() {
    return Y.determinant(this.array);
  },
  /**
   * Set to a identity matrix
   * @return {clay.Matrix2}
   */
  identity: function() {
    return Y.identity(this.array), this._dirty = !0, this;
  },
  /**
   * Invert self
   * @return {clay.Matrix2}
   */
  invert: function() {
    return Y.invert(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Alias for mutiply
   * @param  {clay.Matrix2} b
   * @return {clay.Matrix2}
   */
  mul: function(e) {
    return Y.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiplyLeft
   * @param  {clay.Matrix2} a
   * @return {clay.Matrix2}
   */
  mulLeft: function(e) {
    return Y.mul(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Multiply self and b
   * @param  {clay.Matrix2} b
   * @return {clay.Matrix2}
   */
  multiply: function(e) {
    return Y.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Multiply a and self, a is on the left
   * @param  {clay.Matrix2} a
   * @return {clay.Matrix2}
   */
  multiplyLeft: function(e) {
    return Y.multiply(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian
   * @param  {number}   rad
   * @return {clay.Matrix2}
   */
  rotate: function(e) {
    return Y.rotate(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale self by s
   * @param  {clay.Vector2}  s
   * @return {clay.Matrix2}
   */
  scale: function(e) {
    return Y.scale(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transpose self, in-place.
   * @return {clay.Matrix2}
   */
  transpose: function() {
    return Y.transpose(this.array, this.array), this._dirty = !0, this;
  },
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
Pe.adjoint = function(e, t) {
  return Y.adjoint(e.array, t.array), e._dirty = !0, e;
};
Pe.copy = function(e, t) {
  return Y.copy(e.array, t.array), e._dirty = !0, e;
};
Pe.determinant = function(e) {
  return Y.determinant(e.array);
};
Pe.identity = function(e) {
  return Y.identity(e.array), e._dirty = !0, e;
};
Pe.invert = function(e, t) {
  return Y.invert(e.array, t.array), e._dirty = !0, e;
};
Pe.mul = function(e, t, r) {
  return Y.mul(e.array, t.array, r.array), e._dirty = !0, e;
};
Pe.multiply = Pe.mul;
Pe.rotate = function(e, t, r) {
  return Y.rotate(e.array, t.array, r), e._dirty = !0, e;
};
Pe.scale = function(e, t, r) {
  return Y.scale(e.array, t.array, r.array), e._dirty = !0, e;
};
Pe.transpose = function(e, t) {
  return Y.transpose(e.array, t.array), e._dirty = !0, e;
};
var J = {};
J.create = function() {
  var e = new Ee(6);
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 1, e[4] = 0, e[5] = 0, e;
};
J.clone = function(e) {
  var t = new Ee(6);
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t[4] = e[4], t[5] = e[5], t;
};
J.copy = function(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[4] = t[4], e[5] = t[5], e;
};
J.identity = function(e) {
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 1, e[4] = 0, e[5] = 0, e;
};
J.invert = function(e, t) {
  var r = t[0], i = t[1], n = t[2], a = t[3], o = t[4], s = t[5], l = r * a - i * n;
  return l ? (l = 1 / l, e[0] = a * l, e[1] = -i * l, e[2] = -n * l, e[3] = r * l, e[4] = (n * s - a * o) * l, e[5] = (i * o - r * s) * l, e) : null;
};
J.determinant = function(e) {
  return e[0] * e[3] - e[1] * e[2];
};
J.multiply = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = r[0], f = r[1], h = r[2], d = r[3], c = r[4], v = r[5];
  return e[0] = i * u + a * f, e[1] = n * u + o * f, e[2] = i * h + a * d, e[3] = n * h + o * d, e[4] = i * c + a * v + s, e[5] = n * c + o * v + l, e;
};
J.mul = J.multiply;
J.rotate = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = Math.sin(r), f = Math.cos(r);
  return e[0] = i * f + a * u, e[1] = n * f + o * u, e[2] = i * -u + a * f, e[3] = n * -u + o * f, e[4] = s, e[5] = l, e;
};
J.scale = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = r[0], f = r[1];
  return e[0] = i * u, e[1] = n * u, e[2] = a * f, e[3] = o * f, e[4] = s, e[5] = l, e;
};
J.translate = function(e, t, r) {
  var i = t[0], n = t[1], a = t[2], o = t[3], s = t[4], l = t[5], u = r[0], f = r[1];
  return e[0] = i, e[1] = n, e[2] = a, e[3] = o, e[4] = i * u + a * f + s, e[5] = n * u + o * f + l, e;
};
J.frob = function(e) {
  return Math.sqrt(Math.pow(e[0], 2) + Math.pow(e[1], 2) + Math.pow(e[2], 2) + Math.pow(e[3], 2) + Math.pow(e[4], 2) + Math.pow(e[5], 2) + 1);
};
var Be = function() {
  this.array = J.create(), this._dirty = !0;
};
Be.prototype = {
  constructor: Be,
  /**
   * Set components from array
   * @param  {Float32Array|number[]} arr
   */
  setArray: function(e) {
    for (var t = 0; t < this.array.length; t++)
      this.array[t] = e[t];
    return this._dirty = !0, this;
  },
  /**
   * Clone a new Matrix2d
   * @return {clay.Matrix2d}
   */
  clone: function() {
    return new Be().copy(this);
  },
  /**
   * Copy from b
   * @param  {clay.Matrix2d} b
   * @return {clay.Matrix2d}
   */
  copy: function(e) {
    return J.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Calculate matrix determinant
   * @return {number}
   */
  determinant: function() {
    return J.determinant(this.array);
  },
  /**
   * Set to a identity matrix
   * @return {clay.Matrix2d}
   */
  identity: function() {
    return J.identity(this.array), this._dirty = !0, this;
  },
  /**
   * Invert self
   * @return {clay.Matrix2d}
   */
  invert: function() {
    return J.invert(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Alias for mutiply
   * @param  {clay.Matrix2d} b
   * @return {clay.Matrix2d}
   */
  mul: function(e) {
    return J.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiplyLeft
   * @param  {clay.Matrix2d} a
   * @return {clay.Matrix2d}
   */
  mulLeft: function(e) {
    return J.mul(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Multiply self and b
   * @param  {clay.Matrix2d} b
   * @return {clay.Matrix2d}
   */
  multiply: function(e) {
    return J.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Multiply a and self, a is on the left
   * @param  {clay.Matrix2d} a
   * @return {clay.Matrix2d}
   */
  multiplyLeft: function(e) {
    return J.multiply(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian
   * @param  {number}   rad
   * @return {clay.Matrix2d}
   */
  rotate: function(e) {
    return J.rotate(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale self by s
   * @param  {clay.Vector2}  s
   * @return {clay.Matrix2d}
   */
  scale: function(e) {
    return J.scale(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Translate self by v
   * @param  {clay.Vector2}  v
   * @return {clay.Matrix2d}
   */
  translate: function(e) {
    return J.translate(this.array, this.array, e.array), this._dirty = !0, this;
  },
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
Be.copy = function(e, t) {
  return J.copy(e.array, t.array), e._dirty = !0, e;
};
Be.determinant = function(e) {
  return J.determinant(e.array);
};
Be.identity = function(e) {
  return J.identity(e.array), e._dirty = !0, e;
};
Be.invert = function(e, t) {
  return J.invert(e.array, t.array), e._dirty = !0, e;
};
Be.mul = function(e, t, r) {
  return J.mul(e.array, t.array, r.array), e._dirty = !0, e;
};
Be.multiply = Be.mul;
Be.rotate = function(e, t, r) {
  return J.rotate(e.array, t.array, r), e._dirty = !0, e;
};
Be.scale = function(e, t, r) {
  return J.scale(e.array, t.array, r.array), e._dirty = !0, e;
};
Be.translate = function(e, t, r) {
  return J.translate(e.array, t.array, r.array), e._dirty = !0, e;
};
var ve = function() {
  this.array = V.create(), this._dirty = !0;
};
ve.prototype = {
  constructor: ve,
  /**
   * Set components from array
   * @param  {Float32Array|number[]} arr
   */
  setArray: function(e) {
    for (var t = 0; t < this.array.length; t++)
      this.array[t] = e[t];
    return this._dirty = !0, this;
  },
  /**
   * Calculate the adjugate of self, in-place
   * @return {clay.Matrix3}
   */
  adjoint: function() {
    return V.adjoint(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Clone a new Matrix3
   * @return {clay.Matrix3}
   */
  clone: function() {
    return new ve().copy(this);
  },
  /**
   * Copy from b
   * @param  {clay.Matrix3} b
   * @return {clay.Matrix3}
   */
  copy: function(e) {
    return V.copy(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Calculate matrix determinant
   * @return {number}
   */
  determinant: function() {
    return V.determinant(this.array);
  },
  /**
   * Copy the values from Matrix2d a
   * @param  {clay.Matrix2d} a
   * @return {clay.Matrix3}
   */
  fromMat2d: function(e) {
    return V.fromMat2d(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Copies the upper-left 3x3 values of Matrix4
   * @param  {clay.Matrix4} a
   * @return {clay.Matrix3}
   */
  fromMat4: function(e) {
    return V.fromMat4(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Calculates a rotation matrix from the given quaternion
   * @param  {clay.Quaternion} q
   * @return {clay.Matrix3}
   */
  fromQuat: function(e) {
    return V.fromQuat(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Set to a identity matrix
   * @return {clay.Matrix3}
   */
  identity: function() {
    return V.identity(this.array), this._dirty = !0, this;
  },
  /**
   * Invert self
   * @return {clay.Matrix3}
   */
  invert: function() {
    return V.invert(this.array, this.array), this._dirty = !0, this;
  },
  /**
   * Alias for mutiply
   * @param  {clay.Matrix3} b
   * @return {clay.Matrix3}
   */
  mul: function(e) {
    return V.mul(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Alias for multiplyLeft
   * @param  {clay.Matrix3} a
   * @return {clay.Matrix3}
   */
  mulLeft: function(e) {
    return V.mul(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Multiply self and b
   * @param  {clay.Matrix3} b
   * @return {clay.Matrix3}
   */
  multiply: function(e) {
    return V.multiply(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Multiply a and self, a is on the left
   * @param  {clay.Matrix3} a
   * @return {clay.Matrix3}
   */
  multiplyLeft: function(e) {
    return V.multiply(this.array, e.array, this.array), this._dirty = !0, this;
  },
  /**
   * Rotate self by a given radian
   * @param  {number}   rad
   * @return {clay.Matrix3}
   */
  rotate: function(e) {
    return V.rotate(this.array, this.array, e), this._dirty = !0, this;
  },
  /**
   * Scale self by s
   * @param  {clay.Vector2}  s
   * @return {clay.Matrix3}
   */
  scale: function(e) {
    return V.scale(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Translate self by v
   * @param  {clay.Vector2}  v
   * @return {clay.Matrix3}
   */
  translate: function(e) {
    return V.translate(this.array, this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Calculates a 3x3 normal matrix (transpose inverse) from the 4x4 matrix
   * @param {clay.Matrix4} a
   */
  normalFromMat4: function(e) {
    return V.normalFromMat4(this.array, e.array), this._dirty = !0, this;
  },
  /**
   * Transpose self, in-place.
   * @return {clay.Matrix2}
   */
  transpose: function() {
    return V.transpose(this.array, this.array), this._dirty = !0, this;
  },
  toString: function() {
    return "[" + Array.prototype.join.call(this.array, ",") + "]";
  },
  toArray: function() {
    return Array.prototype.slice.call(this.array);
  }
};
ve.adjoint = function(e, t) {
  return V.adjoint(e.array, t.array), e._dirty = !0, e;
};
ve.copy = function(e, t) {
  return V.copy(e.array, t.array), e._dirty = !0, e;
};
ve.determinant = function(e) {
  return V.determinant(e.array);
};
ve.identity = function(e) {
  return V.identity(e.array), e._dirty = !0, e;
};
ve.invert = function(e, t) {
  return V.invert(e.array, t.array), e;
};
ve.mul = function(e, t, r) {
  return V.mul(e.array, t.array, r.array), e._dirty = !0, e;
};
ve.multiply = ve.mul;
ve.fromMat2d = function(e, t) {
  return V.fromMat2d(e.array, t.array), e._dirty = !0, e;
};
ve.fromMat4 = function(e, t) {
  return V.fromMat4(e.array, t.array), e._dirty = !0, e;
};
ve.fromQuat = function(e, t) {
  return V.fromQuat(e.array, t.array), e._dirty = !0, e;
};
ve.normalFromMat4 = function(e, t) {
  return V.normalFromMat4(e.array, t.array), e._dirty = !0, e;
};
ve.rotate = function(e, t, r) {
  return V.rotate(e.array, t.array, r), e._dirty = !0, e;
};
ve.scale = function(e, t, r) {
  return V.scale(e.array, t.array, r.array), e._dirty = !0, e;
};
ve.transpose = function(e, t) {
  return V.transpose(e.array, t.array), e._dirty = !0, e;
};
ve.translate = function(e, t, r) {
  return V.translate(e.array, t.array, r.array), e._dirty = !0, e;
};
var _r = {
  linear: function(e) {
    return e;
  },
  quadraticIn: function(e) {
    return e * e;
  },
  quadraticOut: function(e) {
    return e * (2 - e);
  },
  quadraticInOut: function(e) {
    return (e *= 2) < 1 ? 0.5 * e * e : -0.5 * (--e * (e - 2) - 1);
  },
  cubicIn: function(e) {
    return e * e * e;
  },
  cubicOut: function(e) {
    return --e * e * e + 1;
  },
  cubicInOut: function(e) {
    return (e *= 2) < 1 ? 0.5 * e * e * e : 0.5 * ((e -= 2) * e * e + 2);
  },
  quarticIn: function(e) {
    return e * e * e * e;
  },
  quarticOut: function(e) {
    return 1 - --e * e * e * e;
  },
  quarticInOut: function(e) {
    return (e *= 2) < 1 ? 0.5 * e * e * e * e : -0.5 * ((e -= 2) * e * e * e - 2);
  },
  quinticIn: function(e) {
    return e * e * e * e * e;
  },
  quinticOut: function(e) {
    return --e * e * e * e * e + 1;
  },
  quinticInOut: function(e) {
    return (e *= 2) < 1 ? 0.5 * e * e * e * e * e : 0.5 * ((e -= 2) * e * e * e * e + 2);
  },
  sinusoidalIn: function(e) {
    return 1 - Math.cos(e * Math.PI / 2);
  },
  sinusoidalOut: function(e) {
    return Math.sin(e * Math.PI / 2);
  },
  sinusoidalInOut: function(e) {
    return 0.5 * (1 - Math.cos(Math.PI * e));
  },
  exponentialIn: function(e) {
    return e === 0 ? 0 : Math.pow(1024, e - 1);
  },
  exponentialOut: function(e) {
    return e === 1 ? 1 : 1 - Math.pow(2, -10 * e);
  },
  exponentialInOut: function(e) {
    return e === 0 ? 0 : e === 1 ? 1 : (e *= 2) < 1 ? 0.5 * Math.pow(1024, e - 1) : 0.5 * (-Math.pow(2, -10 * (e - 1)) + 2);
  },
  circularIn: function(e) {
    return 1 - Math.sqrt(1 - e * e);
  },
  circularOut: function(e) {
    return Math.sqrt(1 - --e * e);
  },
  circularInOut: function(e) {
    return (e *= 2) < 1 ? -0.5 * (Math.sqrt(1 - e * e) - 1) : 0.5 * (Math.sqrt(1 - (e -= 2) * e) + 1);
  },
  elasticIn: function(e) {
    var t, r = 0.1, i = 0.4;
    return e === 0 ? 0 : e === 1 ? 1 : (!r || r < 1 ? (r = 1, t = i / 4) : t = i * Math.asin(1 / r) / (2 * Math.PI), -(r * Math.pow(2, 10 * (e -= 1)) * Math.sin((e - t) * (2 * Math.PI) / i)));
  },
  elasticOut: function(e) {
    var t, r = 0.1, i = 0.4;
    return e === 0 ? 0 : e === 1 ? 1 : (!r || r < 1 ? (r = 1, t = i / 4) : t = i * Math.asin(1 / r) / (2 * Math.PI), r * Math.pow(2, -10 * e) * Math.sin((e - t) * (2 * Math.PI) / i) + 1);
  },
  elasticInOut: function(e) {
    var t, r = 0.1, i = 0.4;
    return e === 0 ? 0 : e === 1 ? 1 : (!r || r < 1 ? (r = 1, t = i / 4) : t = i * Math.asin(1 / r) / (2 * Math.PI), (e *= 2) < 1 ? -0.5 * (r * Math.pow(2, 10 * (e -= 1)) * Math.sin((e - t) * (2 * Math.PI) / i)) : r * Math.pow(2, -10 * (e -= 1)) * Math.sin((e - t) * (2 * Math.PI) / i) * 0.5 + 1);
  },
  backIn: function(e) {
    var t = 1.70158;
    return e * e * ((t + 1) * e - t);
  },
  backOut: function(e) {
    var t = 1.70158;
    return --e * e * ((t + 1) * e + t) + 1;
  },
  backInOut: function(e) {
    var t = 2.5949095;
    return (e *= 2) < 1 ? 0.5 * (e * e * ((t + 1) * e - t)) : 0.5 * ((e -= 2) * e * ((t + 1) * e + t) + 2);
  },
  bounceIn: function(e) {
    return 1 - _r.bounceOut(1 - e);
  },
  bounceOut: function(e) {
    return e < 1 / 2.75 ? 7.5625 * e * e : e < 2 / 2.75 ? 7.5625 * (e -= 1.5 / 2.75) * e + 0.75 : e < 2.5 / 2.75 ? 7.5625 * (e -= 2.25 / 2.75) * e + 0.9375 : 7.5625 * (e -= 2.625 / 2.75) * e + 0.984375;
  },
  bounceInOut: function(e) {
    return e < 0.5 ? _r.bounceIn(e * 2) * 0.5 : _r.bounceOut(e * 2 - 1) * 0.5 + 0.5;
  }
};
qo([
  "Function",
  "RegExp",
  "Date",
  "Error",
  "CanvasGradient",
  "CanvasPattern",
  "Image",
  "Canvas"
], function(e, t) {
  return e["[object " + t + "]"] = !0, e;
}, {});
qo([
  "Int8",
  "Uint8",
  "Uint8Clamped",
  "Int16",
  "Uint16",
  "Int32",
  "Uint32",
  "Float32",
  "Float64"
], function(e, t) {
  return e["[object " + t + "Array]"] = !0, e;
}, {});
var Wo = Array.prototype, Xo = Wo.slice, Ff = Wo.map, za = (function() {
}).constructor, Hr = za ? za.prototype : null, Uf = "__proto__";
function zf() {
  for (var e = [], t = 0; t < arguments.length; t++)
    e[t] = arguments[t];
  typeof console < "u" && console.error.apply(console, e);
}
function Hf(e, t) {
  if (Object.assign)
    Object.assign(e, t);
  else
    for (var r in t)
      t.hasOwnProperty(r) && r !== Uf && (e[r] = t[r]);
  return e;
}
function oi(e) {
  return !e || typeof e == "string" ? !1 : typeof e.length == "number";
}
function yn(e, t, r) {
  if (!e)
    return [];
  if (!t)
    return Xf(e);
  if (e.map && e.map === Ff)
    return e.map(t, r);
  for (var i = [], n = 0, a = e.length; n < a; n++)
    i.push(t.call(r, e[n], n, e));
  return i;
}
function qo(e, t, r, i) {
  if (e && t) {
    for (var n = 0, a = e.length; n < a; n++)
      r = t.call(i, r, e[n], n, e);
    return r;
  }
}
function Ha(e) {
  if (!e)
    return [];
  if (Object.keys)
    return Object.keys(e);
  var t = [];
  for (var r in e)
    e.hasOwnProperty(r) && t.push(r);
  return t;
}
function Gf(e, t) {
  for (var r = [], i = 2; i < arguments.length; i++)
    r[i - 2] = arguments[i];
  return function() {
    return e.apply(t, r.concat(Xo.call(arguments)));
  };
}
Hr && Ti(Hr.bind) && Hr.call.bind(Hr.bind);
function Ti(e) {
  return typeof e == "function";
}
function Vf(e) {
  return typeof e == "string";
}
function Zi(e) {
  return typeof e == "number";
}
function kf(e) {
  return e.colorStops != null;
}
function Wf(e) {
  return e !== e;
}
function Xf(e) {
  for (var t = [], r = 1; r < arguments.length; r++)
    t[r - 1] = arguments[r];
  return Xo.apply(e, t);
}
function Gr(e) {
  return e == null ? null : typeof e.trim == "function" ? e.trim() : e.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, "");
}
function Yi() {
}
var Vr = Math.pow, ti = Math.sqrt, Ga = 1e-8, Va = ti(3), kr = 1 / 3;
function Wr(e) {
  return e > -Ga && e < Ga;
}
function qf(e, t, r, i, n) {
  var a = 1 - n;
  return a * a * (a * e + 3 * n * t) + n * n * (n * i + 3 * a * r);
}
function jf(e, t, r, i, n, a) {
  var o = i + 3 * (t - r) - e, s = 3 * (r - t * 2 + e), l = 3 * (t - e), u = e - n, f = s * s - 3 * o * l, h = s * l - 9 * o * u, d = l * l - 3 * s * u, c = 0;
  if (Wr(f) && Wr(h))
    if (Wr(s))
      a[0] = 0;
    else {
      var v = -l / s;
      v >= 0 && v <= 1 && (a[c++] = v);
    }
  else {
    var p = h * h - 4 * f * d;
    if (Wr(p)) {
      var m = h / f, v = -s / o + m, _ = -m / 2;
      v >= 0 && v <= 1 && (a[c++] = v), _ >= 0 && _ <= 1 && (a[c++] = _);
    } else if (p > 0) {
      var x = ti(p), y = f * s + 1.5 * o * (-h + x), g = f * s + 1.5 * o * (-h - x);
      y < 0 ? y = -Vr(-y, kr) : y = Vr(y, kr), g < 0 ? g = -Vr(-g, kr) : g = Vr(g, kr);
      var v = (-s - (y + g)) / (3 * o);
      v >= 0 && v <= 1 && (a[c++] = v);
    } else {
      var b = (2 * f * s - 3 * o * h) / (2 * ti(f * f * f)), w = Math.acos(b) / 3, C = ti(f), A = Math.cos(w), v = (-s - 2 * C * A) / (3 * o), _ = (-s + C * (A + Va * Math.sin(w))) / (3 * o), R = (-s + C * (A - Va * Math.sin(w))) / (3 * o);
      v >= 0 && v <= 1 && (a[c++] = v), _ >= 0 && _ <= 1 && (a[c++] = _), R >= 0 && R <= 1 && (a[c++] = R);
    }
  }
  return c;
}
var Zf = /cubic-bezier\(([0-9,\.e ]+)\)/;
function jo(e) {
  var t = e && Zf.exec(e);
  if (t) {
    var r = t[1].split(","), i = +Gr(r[0]), n = +Gr(r[1]), a = +Gr(r[2]), o = +Gr(r[3]);
    if (isNaN(i + n + a + o))
      return;
    var s = [];
    return function(l) {
      return l <= 0 ? 0 : l >= 1 ? 1 : jf(0, i, a, 1, l, s) && qf(0, n, o, 1, s[0]);
    };
  }
}
var Yf = (function() {
  function e(t) {
    this._inited = !1, this._startTime = 0, this._pausedTime = 0, this._paused = !1, this._life = t.life || 1e3, this._delay = t.delay || 0, this.loop = t.loop || !1, this.onframe = t.onframe || Yi, this.ondestroy = t.ondestroy || Yi, this.onrestart = t.onrestart || Yi, t.easing && this.setEasing(t.easing);
  }
  return e.prototype.step = function(t, r) {
    if (this._inited || (this._startTime = t + this._delay, this._inited = !0), this._paused) {
      this._pausedTime += r;
      return;
    }
    var i = this._life, n = t - this._startTime - this._pausedTime, a = n / i;
    a < 0 && (a = 0), a = Math.min(a, 1);
    var o = this.easingFunc, s = o ? o(a) : a;
    if (this.onframe(s), a === 1)
      if (this.loop) {
        var l = n % i;
        this._startTime = t - l, this._pausedTime = 0, this.onrestart();
      } else
        return !0;
    return !1;
  }, e.prototype.pause = function() {
    this._paused = !0;
  }, e.prototype.resume = function() {
    this._paused = !1;
  }, e.prototype.setEasing = function(t) {
    this.easing = t, this.easingFunc = Ti(t) ? t : _r[t] || jo(t);
  }, e;
})(), ka = {
  transparent: [0, 0, 0, 0],
  aliceblue: [240, 248, 255, 1],
  antiquewhite: [250, 235, 215, 1],
  aqua: [0, 255, 255, 1],
  aquamarine: [127, 255, 212, 1],
  azure: [240, 255, 255, 1],
  beige: [245, 245, 220, 1],
  bisque: [255, 228, 196, 1],
  black: [0, 0, 0, 1],
  blanchedalmond: [255, 235, 205, 1],
  blue: [0, 0, 255, 1],
  blueviolet: [138, 43, 226, 1],
  brown: [165, 42, 42, 1],
  burlywood: [222, 184, 135, 1],
  cadetblue: [95, 158, 160, 1],
  chartreuse: [127, 255, 0, 1],
  chocolate: [210, 105, 30, 1],
  coral: [255, 127, 80, 1],
  cornflowerblue: [100, 149, 237, 1],
  cornsilk: [255, 248, 220, 1],
  crimson: [220, 20, 60, 1],
  cyan: [0, 255, 255, 1],
  darkblue: [0, 0, 139, 1],
  darkcyan: [0, 139, 139, 1],
  darkgoldenrod: [184, 134, 11, 1],
  darkgray: [169, 169, 169, 1],
  darkgreen: [0, 100, 0, 1],
  darkgrey: [169, 169, 169, 1],
  darkkhaki: [189, 183, 107, 1],
  darkmagenta: [139, 0, 139, 1],
  darkolivegreen: [85, 107, 47, 1],
  darkorange: [255, 140, 0, 1],
  darkorchid: [153, 50, 204, 1],
  darkred: [139, 0, 0, 1],
  darksalmon: [233, 150, 122, 1],
  darkseagreen: [143, 188, 143, 1],
  darkslateblue: [72, 61, 139, 1],
  darkslategray: [47, 79, 79, 1],
  darkslategrey: [47, 79, 79, 1],
  darkturquoise: [0, 206, 209, 1],
  darkviolet: [148, 0, 211, 1],
  deeppink: [255, 20, 147, 1],
  deepskyblue: [0, 191, 255, 1],
  dimgray: [105, 105, 105, 1],
  dimgrey: [105, 105, 105, 1],
  dodgerblue: [30, 144, 255, 1],
  firebrick: [178, 34, 34, 1],
  floralwhite: [255, 250, 240, 1],
  forestgreen: [34, 139, 34, 1],
  fuchsia: [255, 0, 255, 1],
  gainsboro: [220, 220, 220, 1],
  ghostwhite: [248, 248, 255, 1],
  gold: [255, 215, 0, 1],
  goldenrod: [218, 165, 32, 1],
  gray: [128, 128, 128, 1],
  green: [0, 128, 0, 1],
  greenyellow: [173, 255, 47, 1],
  grey: [128, 128, 128, 1],
  honeydew: [240, 255, 240, 1],
  hotpink: [255, 105, 180, 1],
  indianred: [205, 92, 92, 1],
  indigo: [75, 0, 130, 1],
  ivory: [255, 255, 240, 1],
  khaki: [240, 230, 140, 1],
  lavender: [230, 230, 250, 1],
  lavenderblush: [255, 240, 245, 1],
  lawngreen: [124, 252, 0, 1],
  lemonchiffon: [255, 250, 205, 1],
  lightblue: [173, 216, 230, 1],
  lightcoral: [240, 128, 128, 1],
  lightcyan: [224, 255, 255, 1],
  lightgoldenrodyellow: [250, 250, 210, 1],
  lightgray: [211, 211, 211, 1],
  lightgreen: [144, 238, 144, 1],
  lightgrey: [211, 211, 211, 1],
  lightpink: [255, 182, 193, 1],
  lightsalmon: [255, 160, 122, 1],
  lightseagreen: [32, 178, 170, 1],
  lightskyblue: [135, 206, 250, 1],
  lightslategray: [119, 136, 153, 1],
  lightslategrey: [119, 136, 153, 1],
  lightsteelblue: [176, 196, 222, 1],
  lightyellow: [255, 255, 224, 1],
  lime: [0, 255, 0, 1],
  limegreen: [50, 205, 50, 1],
  linen: [250, 240, 230, 1],
  magenta: [255, 0, 255, 1],
  maroon: [128, 0, 0, 1],
  mediumaquamarine: [102, 205, 170, 1],
  mediumblue: [0, 0, 205, 1],
  mediumorchid: [186, 85, 211, 1],
  mediumpurple: [147, 112, 219, 1],
  mediumseagreen: [60, 179, 113, 1],
  mediumslateblue: [123, 104, 238, 1],
  mediumspringgreen: [0, 250, 154, 1],
  mediumturquoise: [72, 209, 204, 1],
  mediumvioletred: [199, 21, 133, 1],
  midnightblue: [25, 25, 112, 1],
  mintcream: [245, 255, 250, 1],
  mistyrose: [255, 228, 225, 1],
  moccasin: [255, 228, 181, 1],
  navajowhite: [255, 222, 173, 1],
  navy: [0, 0, 128, 1],
  oldlace: [253, 245, 230, 1],
  olive: [128, 128, 0, 1],
  olivedrab: [107, 142, 35, 1],
  orange: [255, 165, 0, 1],
  orangered: [255, 69, 0, 1],
  orchid: [218, 112, 214, 1],
  palegoldenrod: [238, 232, 170, 1],
  palegreen: [152, 251, 152, 1],
  paleturquoise: [175, 238, 238, 1],
  palevioletred: [219, 112, 147, 1],
  papayawhip: [255, 239, 213, 1],
  peachpuff: [255, 218, 185, 1],
  peru: [205, 133, 63, 1],
  pink: [255, 192, 203, 1],
  plum: [221, 160, 221, 1],
  powderblue: [176, 224, 230, 1],
  purple: [128, 0, 128, 1],
  red: [255, 0, 0, 1],
  rosybrown: [188, 143, 143, 1],
  royalblue: [65, 105, 225, 1],
  saddlebrown: [139, 69, 19, 1],
  salmon: [250, 128, 114, 1],
  sandybrown: [244, 164, 96, 1],
  seagreen: [46, 139, 87, 1],
  seashell: [255, 245, 238, 1],
  sienna: [160, 82, 45, 1],
  silver: [192, 192, 192, 1],
  skyblue: [135, 206, 235, 1],
  slateblue: [106, 90, 205, 1],
  slategray: [112, 128, 144, 1],
  slategrey: [112, 128, 144, 1],
  snow: [255, 250, 250, 1],
  springgreen: [0, 255, 127, 1],
  steelblue: [70, 130, 180, 1],
  tan: [210, 180, 140, 1],
  teal: [0, 128, 128, 1],
  thistle: [216, 191, 216, 1],
  tomato: [255, 99, 71, 1],
  turquoise: [64, 224, 208, 1],
  violet: [238, 130, 238, 1],
  wheat: [245, 222, 179, 1],
  white: [255, 255, 255, 1],
  whitesmoke: [245, 245, 245, 1],
  yellow: [255, 255, 0, 1],
  yellowgreen: [154, 205, 50, 1]
};
function gr(e) {
  return e = Math.round(e), e < 0 ? 0 : e > 255 ? 255 : e;
}
function Wa(e) {
  return e < 0 ? 0 : e > 1 ? 1 : e;
}
function Ki(e) {
  var t = e;
  return t.length && t.charAt(t.length - 1) === "%" ? gr(parseFloat(t) / 100 * 255) : gr(parseInt(t, 10));
}
function yr(e) {
  var t = e;
  return t.length && t.charAt(t.length - 1) === "%" ? Wa(parseFloat(t) / 100) : Wa(parseFloat(t));
}
function Qi(e, t, r) {
  return r < 0 ? r += 1 : r > 1 && (r -= 1), r * 6 < 1 ? e + (t - e) * r * 6 : r * 2 < 1 ? t : r * 3 < 2 ? e + (t - e) * (2 / 3 - r) * 6 : e;
}
function ze(e, t, r, i, n) {
  return e[0] = t, e[1] = r, e[2] = i, e[3] = n, e;
}
function xn(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e;
}
var Zo = new zo(20), Xr = null;
function kt(e, t) {
  Xr && xn(Xr, t), Xr = Zo.put(e, Xr || t.slice());
}
function $i(e, t) {
  if (e) {
    t = t || [];
    var r = Zo.get(e);
    if (r)
      return xn(t, r);
    e = e + "";
    var i = e.replace(/ /g, "").toLowerCase();
    if (i in ka)
      return xn(t, ka[i]), kt(e, t), t;
    var n = i.length;
    if (i.charAt(0) === "#") {
      if (n === 4 || n === 5) {
        var a = parseInt(i.slice(1, 4), 16);
        if (!(a >= 0 && a <= 4095)) {
          ze(t, 0, 0, 0, 1);
          return;
        }
        return ze(t, (a & 3840) >> 4 | (a & 3840) >> 8, a & 240 | (a & 240) >> 4, a & 15 | (a & 15) << 4, n === 5 ? parseInt(i.slice(4), 16) / 15 : 1), kt(e, t), t;
      } else if (n === 7 || n === 9) {
        var a = parseInt(i.slice(1, 7), 16);
        if (!(a >= 0 && a <= 16777215)) {
          ze(t, 0, 0, 0, 1);
          return;
        }
        return ze(t, (a & 16711680) >> 16, (a & 65280) >> 8, a & 255, n === 9 ? parseInt(i.slice(7), 16) / 255 : 1), kt(e, t), t;
      }
      return;
    }
    var o = i.indexOf("("), s = i.indexOf(")");
    if (o !== -1 && s + 1 === n) {
      var l = i.substr(0, o), u = i.substr(o + 1, s - (o + 1)).split(","), f = 1;
      switch (l) {
        case "rgba":
          if (u.length !== 4)
            return u.length === 3 ? ze(t, +u[0], +u[1], +u[2], 1) : ze(t, 0, 0, 0, 1);
          f = yr(u.pop());
        case "rgb":
          if (u.length >= 3)
            return ze(t, Ki(u[0]), Ki(u[1]), Ki(u[2]), u.length === 3 ? f : yr(u[3])), kt(e, t), t;
          ze(t, 0, 0, 0, 1);
          return;
        case "hsla":
          if (u.length !== 4) {
            ze(t, 0, 0, 0, 1);
            return;
          }
          return u[3] = yr(u[3]), Xa(u, t), kt(e, t), t;
        case "hsl":
          if (u.length !== 3) {
            ze(t, 0, 0, 0, 1);
            return;
          }
          return Xa(u, t), kt(e, t), t;
        default:
          return;
      }
    }
    ze(t, 0, 0, 0, 1);
  }
}
function Xa(e, t) {
  var r = (parseFloat(e[0]) % 360 + 360) % 360 / 360, i = yr(e[1]), n = yr(e[2]), a = n <= 0.5 ? n * (i + 1) : n + i - n * i, o = n * 2 - a;
  return t = t || [], ze(t, gr(Qi(o, a, r + 1 / 3) * 255), gr(Qi(o, a, r) * 255), gr(Qi(o, a, r - 1 / 3) * 255), 1), e.length === 4 && (t[3] = e[3]), t;
}
var Kf = /* @__PURE__ */ (function() {
  function e() {
    this.firefox = !1, this.ie = !1, this.edge = !1, this.newEdge = !1, this.weChat = !1;
  }
  return e;
})(), Qf = /* @__PURE__ */ (function() {
  function e() {
    this.browser = new Kf(), this.node = !1, this.wxa = !1, this.worker = !1, this.svgSupported = !1, this.touchEventsSupported = !1, this.pointerEventsSupported = !1, this.domSupported = !1, this.transformSupported = !1, this.transform3dSupported = !1, this.hasGlobalWindow = typeof window < "u";
  }
  return e;
})(), at = new Qf();
typeof wx == "object" && typeof wx.getSystemInfoSync == "function" ? (at.wxa = !0, at.touchEventsSupported = !0) : typeof document > "u" && typeof self < "u" ? at.worker = !0 : !at.hasGlobalWindow || "Deno" in window ? (at.node = !0, at.svgSupported = !0) : $f(navigator.userAgent, at);
function $f(e, t) {
  var r = t.browser, i = e.match(/Firefox\/([\d.]+)/), n = e.match(/MSIE\s([\d.]+)/) || e.match(/Trident\/.+?rv:(([\d.]+))/), a = e.match(/Edge?\/([\d.]+)/), o = /micromessenger/i.test(e);
  i && (r.firefox = !0, r.version = i[1]), n && (r.ie = !0, r.version = n[1]), a && (r.edge = !0, r.version = a[1], r.newEdge = +a[1].split(".")[0] > 18), o && (r.weChat = !0), t.svgSupported = typeof SVGRect < "u", t.touchEventsSupported = "ontouchstart" in window && !r.ie && !r.edge, t.pointerEventsSupported = "onpointerdown" in window && (r.edge || r.ie && +r.version >= 11), t.domSupported = typeof document < "u";
  var s = document.documentElement.style;
  t.transform3dSupported = (r.ie && "transition" in s || r.edge || "WebKitCSSMatrix" in window && "m11" in new WebKitCSSMatrix() || "MozPerspective" in s) && !("OTransition" in s), t.transformSupported = t.transform3dSupported || r.ie && +r.version >= 9;
}
function Jf(e) {
  return e.type === "linear";
}
function eh(e) {
  return e.type === "radial";
}
(function() {
  return at.hasGlobalWindow && Ti(window.btoa) ? function(e) {
    return window.btoa(unescape(encodeURIComponent(e)));
  } : typeof Buffer < "u" ? function(e) {
    return Buffer.from(e).toString("base64");
  } : function(e) {
    return null;
  };
})();
var Tn = Array.prototype.slice;
function ot(e, t, r) {
  return (t - e) * r + e;
}
function Ji(e, t, r, i) {
  for (var n = t.length, a = 0; a < n; a++)
    e[a] = ot(t[a], r[a], i);
  return e;
}
function th(e, t, r, i) {
  for (var n = t.length, a = n && t[0].length, o = 0; o < n; o++) {
    e[o] || (e[o] = []);
    for (var s = 0; s < a; s++)
      e[o][s] = ot(t[o][s], r[o][s], i);
  }
  return e;
}
function qr(e, t, r, i) {
  for (var n = t.length, a = 0; a < n; a++)
    e[a] = t[a] + r[a] * i;
  return e;
}
function qa(e, t, r, i) {
  for (var n = t.length, a = n && t[0].length, o = 0; o < n; o++) {
    e[o] || (e[o] = []);
    for (var s = 0; s < a; s++)
      e[o][s] = t[o][s] + r[o][s] * i;
  }
  return e;
}
function rh(e, t) {
  for (var r = e.length, i = t.length, n = r > i ? t : e, a = Math.min(r, i), o = n[a - 1] || { color: [0, 0, 0, 0], offset: 0 }, s = a; s < Math.max(r, i); s++)
    n.push({
      offset: o.offset,
      color: o.color.slice()
    });
}
function ih(e, t, r) {
  var i = e, n = t;
  if (!(!i.push || !n.push)) {
    var a = i.length, o = n.length;
    if (a !== o) {
      var s = a > o;
      if (s)
        i.length = o;
      else
        for (var l = a; l < o; l++)
          i.push(r === 1 ? n[l] : Tn.call(n[l]));
    }
    for (var u = i[0] && i[0].length, l = 0; l < i.length; l++)
      if (r === 1)
        isNaN(i[l]) && (i[l] = n[l]);
      else
        for (var f = 0; f < u; f++)
          isNaN(i[l][f]) && (i[l][f] = n[l][f]);
  }
}
function en(e) {
  if (oi(e)) {
    var t = e.length;
    if (oi(e[0])) {
      for (var r = [], i = 0; i < t; i++)
        r.push(Tn.call(e[i]));
      return r;
    }
    return Tn.call(e);
  }
  return e;
}
function ri(e) {
  return e[0] = Math.floor(e[0]) || 0, e[1] = Math.floor(e[1]) || 0, e[2] = Math.floor(e[2]) || 0, e[3] = e[3] == null ? 1 : e[3], "rgba(" + e.join(",") + ")";
}
function nh(e) {
  return oi(e && e[0]) ? 2 : 1;
}
var jr = 0, ii = 1, Yo = 2, vr = 3, wn = 4, bn = 5, ja = 6;
function Za(e) {
  return e === wn || e === bn;
}
function Zr(e) {
  return e === ii || e === Yo;
}
var fr = [0, 0, 0, 0], ah = (function() {
  function e(t) {
    this.keyframes = [], this.discrete = !1, this._invalid = !1, this._needsSort = !1, this._lastFr = 0, this._lastFrP = 0, this.propName = t;
  }
  return e.prototype.isFinished = function() {
    return this._finished;
  }, e.prototype.setFinished = function() {
    this._finished = !0, this._additiveTrack && this._additiveTrack.setFinished();
  }, e.prototype.needsAnimate = function() {
    return this.keyframes.length >= 1;
  }, e.prototype.getAdditiveTrack = function() {
    return this._additiveTrack;
  }, e.prototype.addKeyframe = function(t, r, i) {
    this._needsSort = !0;
    var n = this.keyframes, a = n.length, o = !1, s = ja, l = r;
    if (oi(r)) {
      var u = nh(r);
      s = u, (u === 1 && !Zi(r[0]) || u === 2 && !Zi(r[0][0])) && (o = !0);
    } else if (Zi(r) && !Wf(r))
      s = jr;
    else if (Vf(r))
      if (!isNaN(+r))
        s = jr;
      else {
        var f = $i(r);
        f && (l = f, s = vr);
      }
    else if (kf(r)) {
      var h = Hf({}, l);
      h.colorStops = yn(r.colorStops, function(c) {
        return {
          offset: c.offset,
          color: $i(c.color)
        };
      }), Jf(r) ? s = wn : eh(r) && (s = bn), l = h;
    }
    a === 0 ? this.valType = s : (s !== this.valType || s === ja) && (o = !0), this.discrete = this.discrete || o;
    var d = {
      time: t,
      value: l,
      rawValue: r,
      percent: 0
    };
    return i && (d.easing = i, d.easingFunc = Ti(i) ? i : _r[i] || jo(i)), n.push(d), d;
  }, e.prototype.prepare = function(t, r) {
    var i = this.keyframes;
    this._needsSort && i.sort(function(p, m) {
      return p.time - m.time;
    });
    for (var n = this.valType, a = i.length, o = i[a - 1], s = this.discrete, l = Zr(n), u = Za(n), f = 0; f < a; f++) {
      var h = i[f], d = h.value, c = o.value;
      h.percent = h.time / t, s || (l && f !== a - 1 ? ih(d, c, n) : u && rh(d.colorStops, c.colorStops));
    }
    if (!s && n !== bn && r && this.needsAnimate() && r.needsAnimate() && n === r.valType && !r._finished) {
      this._additiveTrack = r;
      for (var v = i[0].value, f = 0; f < a; f++)
        n === jr ? i[f].additiveValue = i[f].value - v : n === vr ? i[f].additiveValue = qr([], i[f].value, v, -1) : Zr(n) && (i[f].additiveValue = n === ii ? qr([], i[f].value, v, -1) : qa([], i[f].value, v, -1));
    }
  }, e.prototype.step = function(t, r) {
    if (!this._finished) {
      this._additiveTrack && this._additiveTrack._finished && (this._additiveTrack = null);
      var i = this._additiveTrack != null, n = i ? "additiveValue" : "value", a = this.valType, o = this.keyframes, s = o.length, l = this.propName, u = a === vr, f, h = this._lastFr, d = Math.min, c, v;
      if (s === 1)
        c = v = o[0];
      else {
        if (r < 0)
          f = 0;
        else if (r < this._lastFrP) {
          var p = d(h + 1, s - 1);
          for (f = p; f >= 0 && !(o[f].percent <= r); f--)
            ;
          f = d(f, s - 2);
        } else {
          for (f = h; f < s && !(o[f].percent > r); f++)
            ;
          f = d(f - 1, s - 2);
        }
        v = o[f + 1], c = o[f];
      }
      if (c && v) {
        this._lastFr = f, this._lastFrP = r;
        var m = v.percent - c.percent, _ = m === 0 ? 1 : d((r - c.percent) / m, 1);
        v.easingFunc && (_ = v.easingFunc(_));
        var x = i ? this._additiveValue : u ? fr : t[l];
        if ((Zr(a) || u) && !x && (x = this._additiveValue = []), this.discrete)
          t[l] = _ < 1 ? c.rawValue : v.rawValue;
        else if (Zr(a))
          a === ii ? Ji(x, c[n], v[n], _) : th(x, c[n], v[n], _);
        else if (Za(a)) {
          var y = c[n], g = v[n], b = a === wn;
          t[l] = {
            type: b ? "linear" : "radial",
            x: ot(y.x, g.x, _),
            y: ot(y.y, g.y, _),
            colorStops: yn(y.colorStops, function(C, A) {
              var R = g.colorStops[A];
              return {
                offset: ot(C.offset, R.offset, _),
                color: ri(Ji([], C.color, R.color, _))
              };
            }),
            global: g.global
          }, b ? (t[l].x2 = ot(y.x2, g.x2, _), t[l].y2 = ot(y.y2, g.y2, _)) : t[l].r = ot(y.r, g.r, _);
        } else if (u)
          Ji(x, c[n], v[n], _), i || (t[l] = ri(x));
        else {
          var w = ot(c[n], v[n], _);
          i ? this._additiveValue = w : t[l] = w;
        }
        i && this._addToTarget(t);
      }
    }
  }, e.prototype._addToTarget = function(t) {
    var r = this.valType, i = this.propName, n = this._additiveValue;
    r === jr ? t[i] = t[i] + n : r === vr ? ($i(t[i], fr), qr(fr, fr, n, 1), t[i] = ri(fr)) : r === ii ? qr(t[i], t[i], n, 1) : r === Yo && qa(t[i], t[i], n, 1);
  }, e;
})(), oh = (function() {
  function e(t, r, i, n) {
    if (this._tracks = {}, this._trackKeys = [], this._maxTime = 0, this._started = 0, this._clip = null, this._target = t, this._loop = r, r && n) {
      zf("Can' use additive animation on looped animation.");
      return;
    }
    this._additiveAnimators = n, this._allowDiscrete = i;
  }
  return e.prototype.getMaxTime = function() {
    return this._maxTime;
  }, e.prototype.getDelay = function() {
    return this._delay;
  }, e.prototype.getLoop = function() {
    return this._loop;
  }, e.prototype.getTarget = function() {
    return this._target;
  }, e.prototype.changeTarget = function(t) {
    this._target = t;
  }, e.prototype.when = function(t, r, i) {
    return this.whenWithKeys(t, r, Ha(r), i);
  }, e.prototype.whenWithKeys = function(t, r, i, n) {
    for (var a = this._tracks, o = 0; o < i.length; o++) {
      var s = i[o], l = a[s];
      if (!l) {
        l = a[s] = new ah(s);
        var u = void 0, f = this._getAdditiveTrack(s);
        if (f) {
          var h = f.keyframes, d = h[h.length - 1];
          u = d && d.value, f.valType === vr && u && (u = ri(u));
        } else
          u = this._target[s];
        if (u == null)
          continue;
        t > 0 && l.addKeyframe(0, en(u), n), this._trackKeys.push(s);
      }
      l.addKeyframe(t, en(r[s]), n);
    }
    return this._maxTime = Math.max(this._maxTime, t), this;
  }, e.prototype.pause = function() {
    this._clip.pause(), this._paused = !0;
  }, e.prototype.resume = function() {
    this._clip.resume(), this._paused = !1;
  }, e.prototype.isPaused = function() {
    return !!this._paused;
  }, e.prototype.duration = function(t) {
    return this._maxTime = t, this._force = !0, this;
  }, e.prototype._doneCallback = function() {
    this._setTracksFinished(), this._clip = null;
    var t = this._doneCbs;
    if (t)
      for (var r = t.length, i = 0; i < r; i++)
        t[i].call(this);
  }, e.prototype._abortedCallback = function() {
    this._setTracksFinished();
    var t = this.animation, r = this._abortedCbs;
    if (t && t.removeClip(this._clip), this._clip = null, r)
      for (var i = 0; i < r.length; i++)
        r[i].call(this);
  }, e.prototype._setTracksFinished = function() {
    for (var t = this._tracks, r = this._trackKeys, i = 0; i < r.length; i++)
      t[r[i]].setFinished();
  }, e.prototype._getAdditiveTrack = function(t) {
    var r, i = this._additiveAnimators;
    if (i)
      for (var n = 0; n < i.length; n++) {
        var a = i[n].getTrack(t);
        a && (r = a);
      }
    return r;
  }, e.prototype.start = function(t) {
    if (!(this._started > 0)) {
      this._started = 1;
      for (var r = this, i = [], n = this._maxTime || 0, a = 0; a < this._trackKeys.length; a++) {
        var o = this._trackKeys[a], s = this._tracks[o], l = this._getAdditiveTrack(o), u = s.keyframes, f = u.length;
        if (s.prepare(n, l), s.needsAnimate())
          if (!this._allowDiscrete && s.discrete) {
            var h = u[f - 1];
            h && (r._target[s.propName] = h.rawValue), s.setFinished();
          } else
            i.push(s);
      }
      if (i.length || this._force) {
        var d = new Yf({
          life: n,
          loop: this._loop,
          delay: this._delay || 0,
          onframe: function(c) {
            r._started = 2;
            var v = r._additiveAnimators;
            if (v) {
              for (var p = !1, m = 0; m < v.length; m++)
                if (v[m]._clip) {
                  p = !0;
                  break;
                }
              p || (r._additiveAnimators = null);
            }
            for (var m = 0; m < i.length; m++)
              i[m].step(r._target, c);
            var _ = r._onframeCbs;
            if (_)
              for (var m = 0; m < _.length; m++)
                _[m](r._target, c);
          },
          ondestroy: function() {
            r._doneCallback();
          }
        });
        this._clip = d, this.animation && this.animation.addClip(d), t && d.setEasing(t);
      } else
        this._doneCallback();
      return this;
    }
  }, e.prototype.stop = function(t) {
    if (this._clip) {
      var r = this._clip;
      t && r.onframe(1), this._abortedCallback();
    }
  }, e.prototype.delay = function(t) {
    return this._delay = t, this;
  }, e.prototype.during = function(t) {
    return t && (this._onframeCbs || (this._onframeCbs = []), this._onframeCbs.push(t)), this;
  }, e.prototype.done = function(t) {
    return t && (this._doneCbs || (this._doneCbs = []), this._doneCbs.push(t)), this;
  }, e.prototype.aborted = function(t) {
    return t && (this._abortedCbs || (this._abortedCbs = []), this._abortedCbs.push(t)), this;
  }, e.prototype.getClip = function() {
    return this._clip;
  }, e.prototype.getTrack = function(t) {
    return this._tracks[t];
  }, e.prototype.getTracks = function() {
    var t = this;
    return yn(this._trackKeys, function(r) {
      return t._tracks[r];
    });
  }, e.prototype.stopTracks = function(t, r) {
    if (!t.length || !this._clip)
      return !0;
    for (var i = this._tracks, n = this._trackKeys, a = 0; a < t.length; a++) {
      var o = i[t[a]];
      o && !o.isFinished() && (r ? o.step(this._target, 1) : this._started === 1 && o.step(this._target, 0), o.setFinished());
    }
    for (var s = !0, a = 0; a < n.length; a++)
      if (!i[n[a]].isFinished()) {
        s = !1;
        break;
      }
    return s && this._abortedCallback(), s;
  }, e.prototype.saveTo = function(t, r, i) {
    if (t) {
      r = r || this._trackKeys;
      for (var n = 0; n < r.length; n++) {
        var a = r[n], o = this._tracks[a];
        if (!(!o || o.isFinished())) {
          var s = o.keyframes, l = s[i ? 0 : s.length - 1];
          l && (t[a] = en(l.rawValue));
        }
      }
    }
  }, e.prototype.__changeFinalValue = function(t, r) {
    r = r || Ha(t);
    for (var i = 0; i < r.length; i++) {
      var n = r[i], a = this._tracks[n];
      if (a) {
        var o = a.keyframes;
        if (o.length > 1) {
          var s = o.pop();
          a.addKeyframe(s.time, t[n]), a.prepare(this._maxTime, a.getAdditiveTrack());
        }
      }
    }
  }, e;
})(), sh = {
  _animators: null,
  getAnimators: function() {
    return this._animators = this._animators || [], this._animators;
  },
  animate: function(e, t) {
    this._animators = this._animators || [];
    var r = this, i;
    if (e) {
      for (var n = e.split("."), a = r, o = 0, s = n.length; o < s; o++)
        a && (a = a[n[o]]);
      a && (i = a);
    } else
      i = r;
    if (i == null)
      throw new Error("Target " + e + " not exists");
    var l = this._animators, u = new oh(i, t), f = this;
    return u.during(function() {
      f.__zr && f.__zr.refresh();
    }).done(function() {
      var h = l.indexOf(u);
      h >= 0 && l.splice(h, 1);
    }), l.push(u), this.__zr && this.__zr.animation.addAnimator(u), u;
  },
  stopAnimation: function(e) {
    this._animators = this._animators || [];
    for (var t = this._animators, r = t.length, i = 0; i < r; i++)
      t[i].stop(e);
    return t.length = 0, this;
  },
  addAnimatorsToZr: function(e) {
    if (this._animators)
      for (var t = 0; t < this._animators.length; t++)
        e.animation.addAnimator(this._animators[t]);
  },
  removeAnimatorsFromZr: function(e) {
    if (this._animators)
      for (var t = 0; t < this._animators.length; t++)
        e.animation.removeAnimator(this._animators[t]);
  }
};
const lh = `
@export clay.util.rand
highp float rand(vec2 uv) {
 const highp float a = 12.9898, b = 78.233, c = 43758.5453;
 highp float dt = dot(uv.xy, vec2(a,b)), sn = mod(dt, 3.141592653589793);
 return fract(sin(sn) * c);
}
@end
@export clay.util.calculate_attenuation
uniform float attenuationFactor : 5.0;
float lightAttenuation(float dist, float range)
{
 float attenuation = 1.0;
 attenuation = dist*dist/(range*range+1.0);
 float att_s = attenuationFactor;
 attenuation = 1.0/(attenuation*att_s+1.0);
 att_s = 1.0/(att_s+1.0);
 attenuation = attenuation - att_s;
 attenuation /= 1.0 - att_s;
 return clamp(attenuation, 0.0, 1.0);
}
@end
@export clay.util.edge_factor
#ifdef SUPPORT_STANDARD_DERIVATIVES
float edgeFactor(float width)
{
 vec3 d = fwidth(v_Barycentric);
 vec3 a3 = smoothstep(vec3(0.0), d * width, v_Barycentric);
 return min(min(a3.x, a3.y), a3.z);
}
#else
float edgeFactor(float width)
{
 return 1.0;
}
#endif
@end
@export clay.util.encode_float
vec4 encodeFloat(const in float depth)
{
 const vec4 bitShifts = vec4(256.0*256.0*256.0, 256.0*256.0, 256.0, 1.0);
 const vec4 bit_mask = vec4(0.0, 1.0/256.0, 1.0/256.0, 1.0/256.0);
 vec4 res = fract(depth * bitShifts);
 res -= res.xxyz * bit_mask;
 return res;
}
@end
@export clay.util.decode_float
float decodeFloat(const in vec4 color)
{
 const vec4 bitShifts = vec4(1.0/(256.0*256.0*256.0), 1.0/(256.0*256.0), 1.0/256.0, 1.0);
 return dot(color, bitShifts);
}
@end
@export clay.util.float
@import clay.util.encode_float
@import clay.util.decode_float
@end
@export clay.util.rgbm_decode
vec3 RGBMDecode(vec4 rgbm, float range) {
 return range * rgbm.rgb * rgbm.a;
}
@end
@export clay.util.rgbm_encode
vec4 RGBMEncode(vec3 color, float range) {
 if (dot(color, color) == 0.0) {
 return vec4(0.0);
 }
 vec4 rgbm;
 color /= range;
 rgbm.a = clamp(max(max(color.r, color.g), max(color.b, 1e-6)), 0.0, 1.0);
 rgbm.a = ceil(rgbm.a * 255.0) / 255.0;
 rgbm.rgb = color / rgbm.a;
 return rgbm;
}
@end
@export clay.util.rgbm
@import clay.util.rgbm_decode
@import clay.util.rgbm_encode
vec4 decodeHDR(vec4 color)
{
#if defined(RGBM_DECODE) || defined(RGBM)
 return vec4(RGBMDecode(color, 8.12), 1.0);
#else
 return color;
#endif
}
vec4 encodeHDR(vec4 color)
{
#if defined(RGBM_ENCODE) || defined(RGBM)
 return RGBMEncode(color.xyz, 8.12);
#else
 return color;
#endif
}
@end
@export clay.util.srgb
vec4 sRGBToLinear(in vec4 value) {
 return vec4(mix(pow(value.rgb * 0.9478672986 + vec3(0.0521327014), vec3(2.4)), value.rgb * 0.0773993808, vec3(lessThanEqual(value.rgb, vec3(0.04045)))), value.w);
}
vec4 linearTosRGB(in vec4 value) {
 return vec4(mix(pow(value.rgb, vec3(0.41666)) * 1.055 - vec3(0.055), value.rgb * 12.92, vec3(lessThanEqual(value.rgb, vec3(0.0031308)))), value.w);
}
@end
@export clay.chunk.skinning_header
#ifdef SKINNING
attribute vec3 weight : WEIGHT;
attribute vec4 joint : JOINT;
#ifdef USE_SKIN_MATRICES_TEXTURE
uniform sampler2D skinMatricesTexture : ignore;
uniform float skinMatricesTextureSize: ignore;
mat4 getSkinMatrix(sampler2D tex, float idx) {
 float j = idx * 4.0;
 float x = mod(j, skinMatricesTextureSize);
 float y = floor(j / skinMatricesTextureSize) + 0.5;
 vec2 scale = vec2(skinMatricesTextureSize);
 return mat4(
 texture2D(tex, vec2(x + 0.5, y) / scale),
 texture2D(tex, vec2(x + 1.5, y) / scale),
 texture2D(tex, vec2(x + 2.5, y) / scale),
 texture2D(tex, vec2(x + 3.5, y) / scale)
 );
}
mat4 getSkinMatrix(float idx) {
 return getSkinMatrix(skinMatricesTexture, idx);
}
#else
uniform mat4 skinMatrix[JOINT_COUNT] : SKIN_MATRIX;
mat4 getSkinMatrix(float idx) {
 return skinMatrix[int(idx)];
}
#endif
#endif
@end
@export clay.chunk.skin_matrix
mat4 skinMatrixWS = getSkinMatrix(joint.x) * weight.x;
if (weight.y > 1e-4)
{
 skinMatrixWS += getSkinMatrix(joint.y) * weight.y;
}
if (weight.z > 1e-4)
{
 skinMatrixWS += getSkinMatrix(joint.z) * weight.z;
}
float weightW = 1.0-weight.x-weight.y-weight.z;
if (weightW > 1e-4)
{
 skinMatrixWS += getSkinMatrix(joint.w) * weightW;
}
@end
@export clay.chunk.instancing_header
#ifdef INSTANCING
attribute vec4 instanceMat1;
attribute vec4 instanceMat2;
attribute vec4 instanceMat3;
#endif
@end
@export clay.chunk.instancing_matrix
mat4 instanceMat = mat4(
 vec4(instanceMat1.xyz, 0.0),
 vec4(instanceMat2.xyz, 0.0),
 vec4(instanceMat3.xyz, 0.0),
 vec4(instanceMat1.w, instanceMat2.w, instanceMat3.w, 1.0)
);
@end
@export clay.util.parallax_correct
vec3 parallaxCorrect(in vec3 dir, in vec3 pos, in vec3 boxMin, in vec3 boxMax) {
 vec3 first = (boxMax - pos) / dir;
 vec3 second = (boxMin - pos) / dir;
 vec3 further = max(first, second);
 float dist = min(further.x, min(further.y, further.z));
 vec3 fixedPos = pos + dir * dist;
 vec3 boxCenter = (boxMax + boxMin) * 0.5;
 return normalize(fixedPos - boxCenter);
}
@end
@export clay.util.clamp_sample
vec4 clampSample(const in sampler2D texture, const in vec2 coord)
{
#ifdef STEREO
 float eye = step(0.5, coord.x) * 0.5;
 vec2 coordClamped = clamp(coord, vec2(eye, 0.0), vec2(0.5 + eye, 1.0));
#else
 vec2 coordClamped = clamp(coord, vec2(0.0), vec2(1.0));
#endif
 return texture2D(texture, coordClamped);
}
@end
@export clay.util.ACES
vec3 ACESToneMapping(vec3 color)
{
 const float A = 2.51;
 const float B = 0.03;
 const float C = 2.43;
 const float D = 0.59;
 const float E = 0.14;
 return (color * (A * color + B)) / (color * (C * color + D) + E);
}
@end`, uh = `
@export ecgl.common.transformUniforms
uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
uniform mat4 worldInverseTranspose : WORLDINVERSETRANSPOSE;
uniform mat4 world : WORLD;
@end

@export ecgl.common.attributes
attribute vec3 position : POSITION;
attribute vec2 texcoord : TEXCOORD_0;
attribute vec3 normal : NORMAL;
@end

@export ecgl.common.uv.header
uniform vec2 uvRepeat : [1.0, 1.0];
uniform vec2 uvOffset : [0.0, 0.0];
uniform vec2 detailUvRepeat : [1.0, 1.0];
uniform vec2 detailUvOffset : [0.0, 0.0];

varying vec2 v_Texcoord;
varying vec2 v_DetailTexcoord;
@end

@export ecgl.common.uv.main
v_Texcoord = texcoord * uvRepeat + uvOffset;
v_DetailTexcoord = texcoord * detailUvRepeat + detailUvOffset;
@end

@export ecgl.common.uv.fragmentHeader
varying vec2 v_Texcoord;
varying vec2 v_DetailTexcoord;
@end


@export ecgl.common.albedo.main

 vec4 albedoTexel = vec4(1.0);
#ifdef DIFFUSEMAP_ENABLED
 albedoTexel = texture2D(diffuseMap, v_Texcoord);
 #ifdef SRGB_DECODE
 albedoTexel = sRGBToLinear(albedoTexel);
 #endif
#endif

#ifdef DETAILMAP_ENABLED
 vec4 detailTexel = texture2D(detailMap, v_DetailTexcoord);
 #ifdef SRGB_DECODE
 detailTexel = sRGBToLinear(detailTexel);
 #endif
 albedoTexel.rgb = mix(albedoTexel.rgb, detailTexel.rgb, detailTexel.a);
 albedoTexel.a = detailTexel.a + (1.0 - detailTexel.a) * albedoTexel.a;
#endif

@end

@export ecgl.common.wireframe.vertexHeader

#ifdef WIREFRAME_QUAD
attribute vec4 barycentric;
varying vec4 v_Barycentric;
#elif defined(WIREFRAME_TRIANGLE)
attribute vec3 barycentric;
varying vec3 v_Barycentric;
#endif

@end

@export ecgl.common.wireframe.vertexMain

#if defined(WIREFRAME_QUAD) || defined(WIREFRAME_TRIANGLE)
 v_Barycentric = barycentric;
#endif

@end


@export ecgl.common.wireframe.fragmentHeader

uniform float wireframeLineWidth : 1;
uniform vec4 wireframeLineColor: [0, 0, 0, 0.5];

#ifdef WIREFRAME_QUAD
varying vec4 v_Barycentric;
float edgeFactor () {
 vec4 d = fwidth(v_Barycentric);
 vec4 a4 = smoothstep(vec4(0.0), d * wireframeLineWidth, v_Barycentric);
 return min(min(min(a4.x, a4.y), a4.z), a4.w);
}
#elif defined(WIREFRAME_TRIANGLE)
varying vec3 v_Barycentric;
float edgeFactor () {
 vec3 d = fwidth(v_Barycentric);
 vec3 a3 = smoothstep(vec3(0.0), d * wireframeLineWidth, v_Barycentric);
 return min(min(a3.x, a3.y), a3.z);
}
#endif

@end


@export ecgl.common.wireframe.fragmentMain

#if defined(WIREFRAME_QUAD) || defined(WIREFRAME_TRIANGLE)
 if (wireframeLineWidth > 0.) {
 vec4 lineColor = wireframeLineColor;
#ifdef SRGB_DECODE
 lineColor = sRGBToLinear(lineColor);
#endif

 gl_FragColor.rgb = mix(gl_FragColor.rgb, lineColor.rgb, (1.0 - edgeFactor()) * lineColor.a);
 }
#endif
@end




@export ecgl.common.bumpMap.header

#ifdef BUMPMAP_ENABLED
uniform sampler2D bumpMap;
uniform float bumpScale : 1.0;


vec3 bumpNormal(vec3 surfPos, vec3 surfNormal, vec3 baseNormal)
{
 vec2 dSTdx = dFdx(v_Texcoord);
 vec2 dSTdy = dFdy(v_Texcoord);

 float Hll = bumpScale * texture2D(bumpMap, v_Texcoord).x;
 float dHx = bumpScale * texture2D(bumpMap, v_Texcoord + dSTdx).x - Hll;
 float dHy = bumpScale * texture2D(bumpMap, v_Texcoord + dSTdy).x - Hll;

 vec3 vSigmaX = dFdx(surfPos);
 vec3 vSigmaY = dFdy(surfPos);
 vec3 vN = surfNormal;

 vec3 R1 = cross(vSigmaY, vN);
 vec3 R2 = cross(vN, vSigmaX);

 float fDet = dot(vSigmaX, R1);

 vec3 vGrad = sign(fDet) * (dHx * R1 + dHy * R2);
 return normalize(abs(fDet) * baseNormal - vGrad);

}
#endif

@end

@export ecgl.common.normalMap.vertexHeader

#ifdef NORMALMAP_ENABLED
attribute vec4 tangent : TANGENT;
varying vec3 v_Tangent;
varying vec3 v_Bitangent;
#endif

@end

@export ecgl.common.normalMap.vertexMain

#ifdef NORMALMAP_ENABLED
 if (dot(tangent, tangent) > 0.0) {
 v_Tangent = normalize((worldInverseTranspose * vec4(tangent.xyz, 0.0)).xyz);
 v_Bitangent = normalize(cross(v_Normal, v_Tangent) * tangent.w);
 }
#endif

@end


@export ecgl.common.normalMap.fragmentHeader

#ifdef NORMALMAP_ENABLED
uniform sampler2D normalMap;
varying vec3 v_Tangent;
varying vec3 v_Bitangent;
#endif

@end

@export ecgl.common.normalMap.fragmentMain
#ifdef NORMALMAP_ENABLED
 if (dot(v_Tangent, v_Tangent) > 0.0) {
 vec3 normalTexel = texture2D(normalMap, v_DetailTexcoord).xyz;
 if (dot(normalTexel, normalTexel) > 0.0) { N = normalTexel * 2.0 - 1.0;
 mat3 tbn = mat3(v_Tangent, v_Bitangent, v_Normal);
 N = normalize(tbn * N);
 }
 }
#endif
@end



@export ecgl.common.vertexAnimation.header

#ifdef VERTEX_ANIMATION
attribute vec3 prevPosition;
attribute vec3 prevNormal;
uniform float percent;
#endif

@end

@export ecgl.common.vertexAnimation.main

#ifdef VERTEX_ANIMATION
 vec3 pos = mix(prevPosition, position, percent);
 vec3 norm = mix(prevNormal, normal, percent);
#else
 vec3 pos = position;
 vec3 norm = normal;
#endif

@end


@export ecgl.common.ssaoMap.header
#ifdef SSAOMAP_ENABLED
uniform sampler2D ssaoMap;
uniform vec4 viewport : VIEWPORT;
#endif
@end

@export ecgl.common.ssaoMap.main
 float ao = 1.0;
#ifdef SSAOMAP_ENABLED
 ao = texture2D(ssaoMap, (gl_FragCoord.xy - viewport.xy) / viewport.zw).r;
#endif
@end




@export ecgl.common.diffuseLayer.header

#if (LAYER_DIFFUSEMAP_COUNT > 0)
uniform float layerDiffuseIntensity[LAYER_DIFFUSEMAP_COUNT];
uniform sampler2D layerDiffuseMap[LAYER_DIFFUSEMAP_COUNT];
#endif

@end

@export ecgl.common.emissiveLayer.header

#if (LAYER_EMISSIVEMAP_COUNT > 0)
uniform float layerEmissionIntensity[LAYER_EMISSIVEMAP_COUNT];
uniform sampler2D layerEmissiveMap[LAYER_EMISSIVEMAP_COUNT];
#endif

@end

@export ecgl.common.layers.header
@import ecgl.common.diffuseLayer.header
@import ecgl.common.emissiveLayer.header
@end

@export ecgl.common.diffuseLayer.main

#if (LAYER_DIFFUSEMAP_COUNT > 0)
 for (int _idx_ = 0; _idx_ < LAYER_DIFFUSEMAP_COUNT; _idx_++) {{
 float intensity = layerDiffuseIntensity[_idx_];
 vec4 texel2 = texture2D(layerDiffuseMap[_idx_], v_Texcoord);
 #ifdef SRGB_DECODE
 texel2 = sRGBToLinear(texel2);
 #endif
 albedoTexel.rgb = mix(albedoTexel.rgb, texel2.rgb * intensity, texel2.a);
 albedoTexel.a = texel2.a + (1.0 - texel2.a) * albedoTexel.a;
 }}
#endif

@end

@export ecgl.common.emissiveLayer.main

#if (LAYER_EMISSIVEMAP_COUNT > 0)
 for (int _idx_ = 0; _idx_ < LAYER_EMISSIVEMAP_COUNT; _idx_++)
 {{
 vec4 texel2 = texture2D(layerEmissiveMap[_idx_], v_Texcoord) * layerEmissionIntensity[_idx_];
 #ifdef SRGB_DECODE
 texel2 = sRGBToLinear(texel2);
 #endif
 float intensity = layerEmissionIntensity[_idx_];
 gl_FragColor.rgb += texel2.rgb * texel2.a * intensity;
 }}
#endif

@end
`, fh = `@export ecgl.color.vertex

uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;

@import ecgl.common.uv.header

attribute vec2 texcoord : TEXCOORD_0;
attribute vec3 position: POSITION;

@import ecgl.common.wireframe.vertexHeader

#ifdef VERTEX_COLOR
attribute vec4 a_Color : COLOR;
varying vec4 v_Color;
#endif

#ifdef VERTEX_ANIMATION
attribute vec3 prevPosition;
uniform float percent : 1.0;
#endif

#ifdef ATMOSPHERE_ENABLED
attribute vec3 normal: NORMAL;
uniform mat4 worldInverseTranspose : WORLDINVERSETRANSPOSE;
varying vec3 v_Normal;
#endif

void main()
{
#ifdef VERTEX_ANIMATION
 vec3 pos = mix(prevPosition, position, percent);
#else
 vec3 pos = position;
#endif

 gl_Position = worldViewProjection * vec4(pos, 1.0);

 @import ecgl.common.uv.main

#ifdef VERTEX_COLOR
 v_Color = a_Color;
#endif

#ifdef ATMOSPHERE_ENABLED
 v_Normal = normalize((worldInverseTranspose * vec4(normal, 0.0)).xyz);
#endif

 @import ecgl.common.wireframe.vertexMain

}

@end

@export ecgl.color.fragment

#define LAYER_DIFFUSEMAP_COUNT 0
#define LAYER_EMISSIVEMAP_COUNT 0

uniform sampler2D diffuseMap;
uniform sampler2D detailMap;

uniform vec4 color : [1.0, 1.0, 1.0, 1.0];

#ifdef ATMOSPHERE_ENABLED
uniform mat4 viewTranspose: VIEWTRANSPOSE;
uniform vec3 glowColor;
uniform float glowPower;
varying vec3 v_Normal;
#endif

#ifdef VERTEX_COLOR
varying vec4 v_Color;
#endif

@import ecgl.common.layers.header

@import ecgl.common.uv.fragmentHeader

@import ecgl.common.wireframe.fragmentHeader

@import clay.util.srgb

void main()
{
#ifdef SRGB_DECODE
 gl_FragColor = sRGBToLinear(color);
#else
 gl_FragColor = color;
#endif

#ifdef VERTEX_COLOR
 gl_FragColor *= v_Color;
#endif

 @import ecgl.common.albedo.main

 @import ecgl.common.diffuseLayer.main

 gl_FragColor *= albedoTexel;

#ifdef ATMOSPHERE_ENABLED
 float atmoIntensity = pow(1.0 - dot(v_Normal, (viewTranspose * vec4(0.0, 0.0, 1.0, 0.0)).xyz), glowPower);
 gl_FragColor.rgb += glowColor * atmoIntensity;
#endif

 @import ecgl.common.emissiveLayer.main

 @import ecgl.common.wireframe.fragmentMain

}
@end`, hh = `/**
 * http: */

@export ecgl.lambert.vertex

@import ecgl.common.transformUniforms

@import ecgl.common.uv.header


@import ecgl.common.attributes

@import ecgl.common.wireframe.vertexHeader

#ifdef VERTEX_COLOR
attribute vec4 a_Color : COLOR;
varying vec4 v_Color;
#endif


@import ecgl.common.vertexAnimation.header


varying vec3 v_Normal;
varying vec3 v_WorldPosition;

void main()
{
 @import ecgl.common.uv.main

 @import ecgl.common.vertexAnimation.main


 gl_Position = worldViewProjection * vec4(pos, 1.0);

 v_Normal = normalize((worldInverseTranspose * vec4(norm, 0.0)).xyz);
 v_WorldPosition = (world * vec4(pos, 1.0)).xyz;

#ifdef VERTEX_COLOR
 v_Color = a_Color;
#endif

 @import ecgl.common.wireframe.vertexMain
}

@end


@export ecgl.lambert.fragment

#define LAYER_DIFFUSEMAP_COUNT 0
#define LAYER_EMISSIVEMAP_COUNT 0

#define NORMAL_UP_AXIS 1
#define NORMAL_FRONT_AXIS 2

@import ecgl.common.uv.fragmentHeader

varying vec3 v_Normal;
varying vec3 v_WorldPosition;

uniform sampler2D diffuseMap;
uniform sampler2D detailMap;

@import ecgl.common.layers.header

uniform float emissionIntensity: 1.0;

uniform vec4 color : [1.0, 1.0, 1.0, 1.0];

uniform mat4 viewInverse : VIEWINVERSE;

#ifdef ATMOSPHERE_ENABLED
uniform mat4 viewTranspose: VIEWTRANSPOSE;
uniform vec3 glowColor;
uniform float glowPower;
#endif

#ifdef AMBIENT_LIGHT_COUNT
@import clay.header.ambient_light
#endif
#ifdef AMBIENT_SH_LIGHT_COUNT
@import clay.header.ambient_sh_light
#endif

#ifdef DIRECTIONAL_LIGHT_COUNT
@import clay.header.directional_light
#endif

#ifdef VERTEX_COLOR
varying vec4 v_Color;
#endif


@import ecgl.common.ssaoMap.header

@import ecgl.common.bumpMap.header

@import clay.util.srgb

@import ecgl.common.wireframe.fragmentHeader

@import clay.plugin.compute_shadow_map

void main()
{
#ifdef SRGB_DECODE
 gl_FragColor = sRGBToLinear(color);
#else
 gl_FragColor = color;
#endif

#ifdef VERTEX_COLOR
 #ifdef SRGB_DECODE
 gl_FragColor *= sRGBToLinear(v_Color);
 #else
 gl_FragColor *= v_Color;
 #endif
#endif

 @import ecgl.common.albedo.main

 @import ecgl.common.diffuseLayer.main

 gl_FragColor *= albedoTexel;

 vec3 N = v_Normal;
#ifdef DOUBLE_SIDED
 vec3 eyePos = viewInverse[3].xyz;
 vec3 V = normalize(eyePos - v_WorldPosition);

 if (dot(N, V) < 0.0) {
 N = -N;
 }
#endif

 float ambientFactor = 1.0;

#ifdef BUMPMAP_ENABLED
 N = bumpNormal(v_WorldPosition, v_Normal, N);
 ambientFactor = dot(v_Normal, N);
#endif

 vec3 N2 = vec3(N.x, N[NORMAL_UP_AXIS], N[NORMAL_FRONT_AXIS]);

 vec3 diffuseColor = vec3(0.0, 0.0, 0.0);

 @import ecgl.common.ssaoMap.main

#ifdef AMBIENT_LIGHT_COUNT
 for(int i = 0; i < AMBIENT_LIGHT_COUNT; i++)
 {
 diffuseColor += ambientLightColor[i] * ambientFactor * ao;
 }
#endif
#ifdef AMBIENT_SH_LIGHT_COUNT
 for(int _idx_ = 0; _idx_ < AMBIENT_SH_LIGHT_COUNT; _idx_++)
 {{
 diffuseColor += calcAmbientSHLight(_idx_, N2) * ambientSHLightColor[_idx_] * ao;
 }}
#endif
#ifdef DIRECTIONAL_LIGHT_COUNT
#if defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT)
 float shadowContribsDir[DIRECTIONAL_LIGHT_COUNT];
 if(shadowEnabled)
 {
 computeShadowOfDirectionalLights(v_WorldPosition, shadowContribsDir);
 }
#endif
 for(int i = 0; i < DIRECTIONAL_LIGHT_COUNT; i++)
 {
 vec3 lightDirection = -directionalLightDirection[i];
 vec3 lightColor = directionalLightColor[i];

 float shadowContrib = 1.0;
#if defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT)
 if (shadowEnabled)
 {
 shadowContrib = shadowContribsDir[i];
 }
#endif

 float ndl = dot(N, normalize(lightDirection)) * shadowContrib;

 diffuseColor += lightColor * clamp(ndl, 0.0, 1.0);
 }
#endif

 gl_FragColor.rgb *= diffuseColor;

#ifdef ATMOSPHERE_ENABLED
 float atmoIntensity = pow(1.0 - dot(v_Normal, (viewTranspose * vec4(0.0, 0.0, 1.0, 0.0)).xyz), glowPower);
 gl_FragColor.rgb += glowColor * atmoIntensity;
#endif

 @import ecgl.common.emissiveLayer.main

 @import ecgl.common.wireframe.fragmentMain
}

@end`, ch = `@export ecgl.realistic.vertex

@import ecgl.common.transformUniforms

@import ecgl.common.uv.header

@import ecgl.common.attributes


@import ecgl.common.wireframe.vertexHeader

#ifdef VERTEX_COLOR
attribute vec4 a_Color : COLOR;
varying vec4 v_Color;
#endif

#ifdef NORMALMAP_ENABLED
attribute vec4 tangent : TANGENT;
varying vec3 v_Tangent;
varying vec3 v_Bitangent;
#endif

@import ecgl.common.vertexAnimation.header

varying vec3 v_Normal;
varying vec3 v_WorldPosition;

void main()
{

 @import ecgl.common.uv.main

 @import ecgl.common.vertexAnimation.main

 gl_Position = worldViewProjection * vec4(pos, 1.0);

 v_Normal = normalize((worldInverseTranspose * vec4(norm, 0.0)).xyz);
 v_WorldPosition = (world * vec4(pos, 1.0)).xyz;

#ifdef VERTEX_COLOR
 v_Color = a_Color;
#endif

#ifdef NORMALMAP_ENABLED
 v_Tangent = normalize((worldInverseTranspose * vec4(tangent.xyz, 0.0)).xyz);
 v_Bitangent = normalize(cross(v_Normal, v_Tangent) * tangent.w);
#endif

 @import ecgl.common.wireframe.vertexMain

}

@end



@export ecgl.realistic.fragment

#define LAYER_DIFFUSEMAP_COUNT 0
#define LAYER_EMISSIVEMAP_COUNT 0
#define PI 3.14159265358979
#define ROUGHNESS_CHANEL 0
#define METALNESS_CHANEL 1

#define NORMAL_UP_AXIS 1
#define NORMAL_FRONT_AXIS 2

#ifdef VERTEX_COLOR
varying vec4 v_Color;
#endif

@import ecgl.common.uv.fragmentHeader

varying vec3 v_Normal;
varying vec3 v_WorldPosition;

uniform sampler2D diffuseMap;

uniform sampler2D detailMap;
uniform sampler2D metalnessMap;
uniform sampler2D roughnessMap;

@import ecgl.common.layers.header

uniform float emissionIntensity: 1.0;

uniform vec4 color : [1.0, 1.0, 1.0, 1.0];

uniform float metalness : 0.0;
uniform float roughness : 0.5;

uniform mat4 viewInverse : VIEWINVERSE;

#ifdef ATMOSPHERE_ENABLED
uniform mat4 viewTranspose: VIEWTRANSPOSE;
uniform vec3 glowColor;
uniform float glowPower;
#endif

#ifdef AMBIENT_LIGHT_COUNT
@import clay.header.ambient_light
#endif

#ifdef AMBIENT_SH_LIGHT_COUNT
@import clay.header.ambient_sh_light
#endif

#ifdef AMBIENT_CUBEMAP_LIGHT_COUNT
@import clay.header.ambient_cubemap_light
#endif

#ifdef DIRECTIONAL_LIGHT_COUNT
@import clay.header.directional_light
#endif

@import ecgl.common.normalMap.fragmentHeader

@import ecgl.common.ssaoMap.header

@import ecgl.common.bumpMap.header

@import clay.util.srgb

@import clay.util.rgbm

@import ecgl.common.wireframe.fragmentHeader

@import clay.plugin.compute_shadow_map

vec3 F_Schlick(float ndv, vec3 spec) {
 return spec + (1.0 - spec) * pow(1.0 - ndv, 5.0);
}

float D_Phong(float g, float ndh) {
 float a = pow(8192.0, g);
 return (a + 2.0) / 8.0 * pow(ndh, a);
}

void main()
{
 vec4 albedoColor = color;

 vec3 eyePos = viewInverse[3].xyz;
 vec3 V = normalize(eyePos - v_WorldPosition);
#ifdef VERTEX_COLOR
 #ifdef SRGB_DECODE
 albedoColor *= sRGBToLinear(v_Color);
 #else
 albedoColor *= v_Color;
 #endif
#endif

 @import ecgl.common.albedo.main

 @import ecgl.common.diffuseLayer.main

 albedoColor *= albedoTexel;

 float m = metalness;

#ifdef METALNESSMAP_ENABLED
 float m2 = texture2D(metalnessMap, v_DetailTexcoord)[METALNESS_CHANEL];
 m = clamp(m2 + (m - 0.5) * 2.0, 0.0, 1.0);
#endif

 vec3 baseColor = albedoColor.rgb;
 albedoColor.rgb = baseColor * (1.0 - m);
 vec3 specFactor = mix(vec3(0.04), baseColor, m);

 float g = 1.0 - roughness;

#ifdef ROUGHNESSMAP_ENABLED
 float g2 = 1.0 - texture2D(roughnessMap, v_DetailTexcoord)[ROUGHNESS_CHANEL];
 g = clamp(g2 + (g - 0.5) * 2.0, 0.0, 1.0);
#endif

 vec3 N = v_Normal;

#ifdef DOUBLE_SIDED
 if (dot(N, V) < 0.0) {
 N = -N;
 }
#endif

 float ambientFactor = 1.0;

#ifdef BUMPMAP_ENABLED
 N = bumpNormal(v_WorldPosition, v_Normal, N);
 ambientFactor = dot(v_Normal, N);
#endif

@import ecgl.common.normalMap.fragmentMain

 vec3 N2 = vec3(N.x, N[NORMAL_UP_AXIS], N[NORMAL_FRONT_AXIS]);

 vec3 diffuseTerm = vec3(0.0);
 vec3 specularTerm = vec3(0.0);

 float ndv = clamp(dot(N, V), 0.0, 1.0);
 vec3 fresnelTerm = F_Schlick(ndv, specFactor);

 @import ecgl.common.ssaoMap.main

#ifdef AMBIENT_LIGHT_COUNT
 for(int _idx_ = 0; _idx_ < AMBIENT_LIGHT_COUNT; _idx_++)
 {{
 diffuseTerm += ambientLightColor[_idx_] * ambientFactor * ao;
 }}
#endif

#ifdef AMBIENT_SH_LIGHT_COUNT
 for(int _idx_ = 0; _idx_ < AMBIENT_SH_LIGHT_COUNT; _idx_++)
 {{
 diffuseTerm += calcAmbientSHLight(_idx_, N2) * ambientSHLightColor[_idx_] * ao;
 }}
#endif

#ifdef DIRECTIONAL_LIGHT_COUNT
#if defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT)
 float shadowContribsDir[DIRECTIONAL_LIGHT_COUNT];
 if(shadowEnabled)
 {
 computeShadowOfDirectionalLights(v_WorldPosition, shadowContribsDir);
 }
#endif
 for(int _idx_ = 0; _idx_ < DIRECTIONAL_LIGHT_COUNT; _idx_++)
 {{
 vec3 L = -directionalLightDirection[_idx_];
 vec3 lc = directionalLightColor[_idx_];

 vec3 H = normalize(L + V);
 float ndl = clamp(dot(N, normalize(L)), 0.0, 1.0);
 float ndh = clamp(dot(N, H), 0.0, 1.0);

 float shadowContrib = 1.0;
#if defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT)
 if (shadowEnabled)
 {
 shadowContrib = shadowContribsDir[_idx_];
 }
#endif

 vec3 li = lc * ndl * shadowContrib;

 diffuseTerm += li;
 specularTerm += li * fresnelTerm * D_Phong(g, ndh);
 }}
#endif


#ifdef AMBIENT_CUBEMAP_LIGHT_COUNT
 vec3 L = reflect(-V, N);
 L = vec3(L.x, L[NORMAL_UP_AXIS], L[NORMAL_FRONT_AXIS]);
 float rough2 = clamp(1.0 - g, 0.0, 1.0);
 float bias2 = rough2 * 5.0;
 vec2 brdfParam2 = texture2D(ambientCubemapLightBRDFLookup[0], vec2(rough2, ndv)).xy;
 vec3 envWeight2 = specFactor * brdfParam2.x + brdfParam2.y;
 vec3 envTexel2;
 for(int _idx_ = 0; _idx_ < AMBIENT_CUBEMAP_LIGHT_COUNT; _idx_++)
 {{
 envTexel2 = RGBMDecode(textureCubeLodEXT(ambientCubemapLightCubemap[_idx_], L, bias2), 8.12);
 specularTerm += ambientCubemapLightColor[_idx_] * envTexel2 * envWeight2 * ao;
 }}
#endif

 gl_FragColor.rgb = albedoColor.rgb * diffuseTerm + specularTerm;
 gl_FragColor.a = albedoColor.a;

#ifdef ATMOSPHERE_ENABLED
 float atmoIntensity = pow(1.0 - dot(v_Normal, (viewTranspose * vec4(0.0, 0.0, 1.0, 0.0)).xyz), glowPower);
 gl_FragColor.rgb += glowColor * atmoIntensity;
#endif

#ifdef SRGB_ENCODE
 gl_FragColor = linearTosRGB(gl_FragColor);
#endif

 @import ecgl.common.emissiveLayer.main

 @import ecgl.common.wireframe.fragmentMain
}

@end`, dh = `@export ecgl.hatching.vertex

@import ecgl.realistic.vertex

@end


@export ecgl.hatching.fragment

#define NORMAL_UP_AXIS 1
#define NORMAL_FRONT_AXIS 2

@import ecgl.common.uv.fragmentHeader

varying vec3 v_Normal;
varying vec3 v_WorldPosition;

uniform vec4 color : [0.0, 0.0, 0.0, 1.0];
uniform vec4 paperColor : [1.0, 1.0, 1.0, 1.0];

uniform mat4 viewInverse : VIEWINVERSE;

#ifdef AMBIENT_LIGHT_COUNT
@import clay.header.ambient_light
#endif
#ifdef AMBIENT_SH_LIGHT_COUNT
@import clay.header.ambient_sh_light
#endif

#ifdef DIRECTIONAL_LIGHT_COUNT
@import clay.header.directional_light
#endif

#ifdef VERTEX_COLOR
varying vec4 v_Color;
#endif


@import ecgl.common.ssaoMap.header

@import ecgl.common.bumpMap.header

@import clay.util.srgb

@import ecgl.common.wireframe.fragmentHeader

@import clay.plugin.compute_shadow_map

uniform sampler2D hatch1;
uniform sampler2D hatch2;
uniform sampler2D hatch3;
uniform sampler2D hatch4;
uniform sampler2D hatch5;
uniform sampler2D hatch6;

float shade(in float tone) {
 vec4 c = vec4(1. ,1., 1., 1.);
 float step = 1. / 6.;
 vec2 uv = v_DetailTexcoord;
 if (tone <= step / 2.0) {
 c = mix(vec4(0.), texture2D(hatch6, uv), 12. * tone);
 }
 else if (tone <= step) {
 c = mix(texture2D(hatch6, uv), texture2D(hatch5, uv), 6. * tone);
 }
 if(tone > step && tone <= 2. * step){
 c = mix(texture2D(hatch5, uv), texture2D(hatch4, uv) , 6. * (tone - step));
 }
 if(tone > 2. * step && tone <= 3. * step){
 c = mix(texture2D(hatch4, uv), texture2D(hatch3, uv), 6. * (tone - 2. * step));
 }
 if(tone > 3. * step && tone <= 4. * step){
 c = mix(texture2D(hatch3, uv), texture2D(hatch2, uv), 6. * (tone - 3. * step));
 }
 if(tone > 4. * step && tone <= 5. * step){
 c = mix(texture2D(hatch2, uv), texture2D(hatch1, uv), 6. * (tone - 4. * step));
 }
 if(tone > 5. * step){
 c = mix(texture2D(hatch1, uv), vec4(1.), 6. * (tone - 5. * step));
 }

 return c.r;
}

const vec3 w = vec3(0.2125, 0.7154, 0.0721);

void main()
{
#ifdef SRGB_DECODE
 vec4 inkColor = sRGBToLinear(color);
#else
 vec4 inkColor = color;
#endif

#ifdef VERTEX_COLOR
 #ifdef SRGB_DECODE
 inkColor *= sRGBToLinear(v_Color);
 #else
 inkColor *= v_Color;
 #endif
#endif

 vec3 N = v_Normal;
#ifdef DOUBLE_SIDED
 vec3 eyePos = viewInverse[3].xyz;
 vec3 V = normalize(eyePos - v_WorldPosition);

 if (dot(N, V) < 0.0) {
 N = -N;
 }
#endif

 float tone = 0.0;

 float ambientFactor = 1.0;

#ifdef BUMPMAP_ENABLED
 N = bumpNormal(v_WorldPosition, v_Normal, N);
 ambientFactor = dot(v_Normal, N);
#endif

 vec3 N2 = vec3(N.x, N[NORMAL_UP_AXIS], N[NORMAL_FRONT_AXIS]);

 @import ecgl.common.ssaoMap.main

#ifdef AMBIENT_LIGHT_COUNT
 for(int i = 0; i < AMBIENT_LIGHT_COUNT; i++)
 {
 tone += dot(ambientLightColor[i], w) * ambientFactor * ao;
 }
#endif
#ifdef AMBIENT_SH_LIGHT_COUNT
 for(int _idx_ = 0; _idx_ < AMBIENT_SH_LIGHT_COUNT; _idx_++)
 {{
 tone += dot(calcAmbientSHLight(_idx_, N2) * ambientSHLightColor[_idx_], w) * ao;
 }}
#endif
#ifdef DIRECTIONAL_LIGHT_COUNT
#if defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT)
 float shadowContribsDir[DIRECTIONAL_LIGHT_COUNT];
 if(shadowEnabled)
 {
 computeShadowOfDirectionalLights(v_WorldPosition, shadowContribsDir);
 }
#endif
 for(int i = 0; i < DIRECTIONAL_LIGHT_COUNT; i++)
 {
 vec3 lightDirection = -directionalLightDirection[i];
 float lightTone = dot(directionalLightColor[i], w);

 float shadowContrib = 1.0;
#if defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT)
 if (shadowEnabled)
 {
 shadowContrib = shadowContribsDir[i];
 }
#endif

 float ndl = dot(N, normalize(lightDirection)) * shadowContrib;

 tone += lightTone * clamp(ndl, 0.0, 1.0);
 }
#endif

 gl_FragColor = mix(inkColor, paperColor, shade(clamp(tone, 0.0, 1.0)));
 }
@end
`, vh = `@export ecgl.sm.depth.vertex

uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;

attribute vec3 position : POSITION;
attribute vec2 texcoord : TEXCOORD_0;

#ifdef VERTEX_ANIMATION
attribute vec3 prevPosition;
uniform float percent : 1.0;
#endif

varying vec4 v_ViewPosition;
varying vec2 v_Texcoord;

void main(){

#ifdef VERTEX_ANIMATION
 vec3 pos = mix(prevPosition, position, percent);
#else
 vec3 pos = position;
#endif

 v_ViewPosition = worldViewProjection * vec4(pos, 1.0);
 gl_Position = v_ViewPosition;

 v_Texcoord = texcoord;

}
@end



@export ecgl.sm.depth.fragment

@import clay.sm.depth.fragment

@end`;
Object.assign(Je.prototype, sh);
U.import(lh);
U.import(Oo);
U.import(uh);
U.import(fh);
U.import(hh);
U.import(ch);
U.import(dh);
U.import(vh);
function ph(e) {
  return !e || e === "none";
}
function Ko(e) {
  return e instanceof HTMLCanvasElement || e instanceof HTMLImageElement || e instanceof Image;
}
function mh(e) {
  return e.getZr && e.setOption;
}
var _h = gt.prototype.addToScene, gh = gt.prototype.removeFromScene;
gt.prototype.addToScene = function(e) {
  if (_h.call(this, e), this.__zr) {
    var t = this.__zr;
    e.traverse(function(r) {
      r.__zr = t, r.addAnimatorsToZr && r.addAnimatorsToZr(t);
    });
  }
};
gt.prototype.removeFromScene = function(e) {
  gh.call(this, e), e.traverse(function(t) {
    var r = t.__zr;
    t.__zr = null, r && t.removeAnimatorsFromZr && t.removeAnimatorsFromZr(r);
  });
};
ft.prototype.setTextureImage = function(e, t, r, i) {
  if (this.shader) {
    var n = r.getZr(), a = this, o;
    return a.autoUpdateTextureStatus = !1, a.disableTexture(e), ph(t) || (o = E.loadTexture(t, r, i, function(s) {
      a.enableTexture(e), n && n.refresh();
    }), a.set(e, o)), o;
  }
};
var E = {};
E.Renderer = Ve;
E.Node = Je;
E.Mesh = Oe;
E.Shader = U;
E.Material = ft;
E.Texture = N;
E.Texture2D = K;
E.Geometry = $;
E.SphereGeometry = Rf;
E.PlaneGeometry = On;
E.CubeGeometry = Go;
E.AmbientLight = Nf;
E.DirectionalLight = Of;
E.PointLight = If;
E.SpotLight = Bf;
E.PerspectiveCamera = _e;
E.OrthographicCamera = wr;
E.Vector2 = k;
E.Vector3 = S;
E.Vector4 = Z;
E.Quaternion = ee;
E.Matrix2 = Pe;
E.Matrix2d = Be;
E.Matrix3 = ve;
E.Matrix4 = I;
E.Plane = mr;
E.Ray = Pt;
E.BoundingBox = we;
E.Frustum = yi;
var Yr = null;
function yh() {
  return Yr !== null || (Yr = et.createBlank("rgba(255,255,255,0)").image), Yr;
}
function Ya(e) {
  return Math.pow(2, Math.round(Math.log(e) / Math.LN2));
}
function Ka(e) {
  if ((e.wrapS === N.REPEAT || e.wrapT === N.REPEAT) && e.image) {
    var t = Ya(e.width), r = Ya(e.height);
    if (t !== e.width || r !== e.height) {
      var i = document.createElement("canvas");
      i.width = t, i.height = r;
      var n = i.getContext("2d");
      n.drawImage(e.image, 0, 0, t, r), e.image = i;
    }
  }
}
E.loadTexture = function(e, t, r, i) {
  typeof r == "function" && (i = r, r = {}), r = r || {};
  for (var n = Object.keys(r).sort(), a = "", o = 0; o < n.length; o++)
    a += n[o] + "_" + r[n[o]] + "_";
  var s = t.__textureCache = t.__textureCache || new zo(20);
  if (mh(e)) {
    var l = e.__textureid__, u = s.get(a + l);
    if (u)
      u.texture.surface.setECharts(e), i && i(u.texture);
    else {
      var f = new gn(e);
      f.onupdate = function() {
        t.getZr().refresh();
      }, u = {
        texture: f.getTexture()
      };
      for (var o = 0; o < n.length; o++)
        u.texture[n[o]] = r[n[o]];
      l = e.__textureid__ || "__ecgl_ec__" + u.texture.__uid__, e.__textureid__ = l, s.put(a + l, u), i && i(u.texture);
    }
    return u.texture;
  } else if (Ko(e)) {
    var l = e.__textureid__, u = s.get(a + l);
    if (!u) {
      u = {
        texture: new E.Texture2D({
          image: e
        })
      };
      for (var o = 0; o < n.length; o++)
        u.texture[n[o]] = r[n[o]];
      l = e.__textureid__ || "__ecgl_image__" + u.texture.__uid__, e.__textureid__ = l, s.put(a + l, u), Ka(u.texture), i && i(u.texture);
    }
    return u.texture;
  } else {
    var u = s.get(a + e);
    if (u)
      u.callbacks ? u.callbacks.push(i) : i && i(u.texture);
    else if (e.match(/.hdr$|^data:application\/octet-stream/)) {
      u = {
        callbacks: [i]
      };
      var h = et.loadTexture(e, {
        exposure: r.exposure,
        fileType: "hdr"
      }, function() {
        h.dirty(), u.callbacks.forEach(function(v) {
          v && v(h);
        }), u.callbacks = null;
      });
      u.texture = h, s.put(a + e, u);
    } else {
      for (var h = new E.Texture2D({
        image: new Image()
      }), o = 0; o < n.length; o++)
        h[n[o]] = r[n[o]];
      u = {
        texture: h,
        callbacks: [i]
      };
      var d = h.image;
      d.onload = function() {
        h.image = d, Ka(h), h.dirty(), u.callbacks.forEach(function(p) {
          p && p(h);
        }), u.callbacks = null;
      }, d.crossOrigin = "Anonymous", d.src = e, h.image = yh(), s.put(a + e, u);
    }
    return u.texture;
  }
};
E.createAmbientCubemap = function(e, t, r, i) {
  e = e || {};
  var n = e.texture, a = be.firstNotNull(e.exposure, 1), o = new Cf({
    intensity: be.firstNotNull(e.specularIntensity, 1)
  }), s = new Mf({
    intensity: be.firstNotNull(e.diffuseIntensity, 1),
    coefficients: [0.844, 0.712, 0.691, -0.037, 0.083, 0.167, 0.343, 0.288, 0.299, -0.041, -0.021, -9e-3, -3e-3, -0.041, -0.064, -0.011, -7e-3, -4e-3, -0.031, 0.034, 0.081, -0.06, -0.049, -0.06, 0.046, 0.056, 0.05]
  });
  return o.cubemap = E.loadTexture(n, r, {
    exposure: a
  }, function() {
    o.cubemap.flipY = !1, o.prefilter(t, 32), s.coefficients = ko.projectEnvironmentMap(t, o.cubemap, {
      lod: 1
    }), i && i();
  }), {
    specular: o,
    diffuse: s
  };
};
E.createBlankTexture = et.createBlank;
E.isImage = Ko;
E.additiveBlend = function(e) {
  e.blendEquation(e.FUNC_ADD), e.blendFunc(e.SRC_ALPHA, e.ONE);
};
E.parseColor = function(e, t) {
  return e instanceof Array ? (t || (t = []), t[0] = e[0], t[1] = e[1], t[2] = e[2], e.length > 3 ? t[3] = e[3] : t[3] = 1, t) : (t = wo(e || "#000", t) || [0, 0, 0, 0], t[0] /= 255, t[1] /= 255, t[2] /= 255, t);
};
E.directionFromAlphaBeta = function(e, t) {
  var r = e / 180 * Math.PI + Math.PI / 2, i = -t / 180 * Math.PI + Math.PI / 2, n = [], a = Math.sin(r);
  return n[0] = a * Math.cos(i), n[1] = -Math.cos(r), n[2] = a * Math.sin(i), n;
};
E.getShadowResolution = function(e) {
  var t = 1024;
  switch (e) {
    case "low":
      t = 512;
      break;
    case "medium":
      break;
    case "high":
      t = 2048;
      break;
    case "ultra":
      t = 4096;
      break;
  }
  return t;
};
E.COMMON_SHADERS = ["lambert", "color", "realistic", "hatching", "shadow"];
E.createShader = function(e) {
  e === "ecgl.shadow" && (e = "ecgl.displayShadow");
  var t = U.source(e + ".vertex"), r = U.source(e + ".fragment");
  t || console.error("Vertex shader of '%s' not exits", e), r || console.error("Fragment shader of '%s' not exits", e);
  var i = new U(t, r);
  return i.name = e, i;
};
E.createMaterial = function(e, t) {
  t instanceof Array || (t = [t]);
  var r = E.createShader(e), i = new ft({
    shader: r
  });
  return t.forEach(function(n) {
    typeof n == "string" && i.define(n);
  }), i;
};
E.setMaterialFromModel = function(e, t, r, i) {
  t.autoUpdateTextureStatus = !1;
  var n = r.getModel(e + "Material"), a = n.get("detailTexture"), o = be.firstNotNull(n.get("textureTiling"), 1), s = be.firstNotNull(n.get("textureOffset"), 0);
  typeof o == "number" && (o = [o, o]), typeof s == "number" && (s = [s, s]);
  var l = o[0] > 1 || o[1] > 1 ? E.Texture.REPEAT : E.Texture.CLAMP_TO_EDGE, u = {
    anisotropic: 8,
    wrapS: l,
    wrapT: l
  };
  if (e === "realistic") {
    var f = n.get("roughness"), h = n.get("metalness");
    h != null ? isNaN(h) && (t.setTextureImage("metalnessMap", h, i, u), h = be.firstNotNull(n.get("metalnessAdjust"), 0.5)) : h = 0, f != null ? isNaN(f) && (t.setTextureImage("roughnessMap", f, i, u), f = be.firstNotNull(n.get("roughnessAdjust"), 0.5)) : f = 0.5;
    var d = n.get("normalTexture");
    t.setTextureImage("detailMap", a, i, u), t.setTextureImage("normalMap", d, i, u), t.set({
      roughness: f,
      metalness: h,
      detailUvRepeat: o,
      detailUvOffset: s
    });
  } else if (e === "lambert")
    t.setTextureImage("detailMap", a, i, u), t.set({
      detailUvRepeat: o,
      detailUvOffset: s
    });
  else if (e === "color")
    t.setTextureImage("detailMap", a, i, u), t.set({
      detailUvRepeat: o,
      detailUvOffset: s
    });
  else if (e === "hatching") {
    var c = n.get("hatchingTextures") || [];
    c.length < 6;
    for (var v = 0; v < 6; v++)
      t.setTextureImage("hatch" + (v + 1), c[v], i, {
        anisotropic: 8,
        wrapS: E.Texture.REPEAT,
        wrapT: E.Texture.REPEAT
      });
    t.set({
      detailUvRepeat: o,
      detailUvOffset: s
    });
  }
};
E.updateVertexAnimation = function(e, t, r, i) {
  var n = i.get("animation"), a = i.get("animationDurationUpdate"), o = i.get("animationEasingUpdate"), s = r.shadowDepthMaterial;
  if (n && t && a > 0 && t.geometry.vertexCount === r.geometry.vertexCount) {
    r.material.define("vertex", "VERTEX_ANIMATION"), r.ignorePreZ = !0, s && s.define("vertex", "VERTEX_ANIMATION");
    for (var l = 0; l < e.length; l++)
      r.geometry.attributes[e[l][0]].value = t.geometry.attributes[e[l][1]].value;
    r.geometry.dirty(), r.__percent = 0, r.material.set("percent", 0), r.stopAnimation(), r.animate().when(a, {
      __percent: 1
    }).during(function() {
      r.material.set("percent", r.__percent), s && s.set("percent", r.__percent);
    }).done(function() {
      r.ignorePreZ = !1, r.material.undefine("vertex", "VERTEX_ANIMATION"), s && s.undefine("vertex", "VERTEX_ANIMATION");
    }).start(o);
  } else
    r.material.undefine("vertex", "VERTEX_ANIMATION"), s && s.undefine("vertex", "VERTEX_ANIMATION");
};
var Bn = Ae.extend(
  /** @lends clay.picking.RayPicking# */
  {
    /**
     * Target scene
     * @type {clay.Scene}
     */
    scene: null,
    /**
     * Target camera
     * @type {clay.Camera}
     */
    camera: null,
    /**
     * Target renderer
     * @type {clay.Renderer}
     */
    renderer: null
  },
  function() {
    this._ray = new Pt(), this._ndc = new k();
  },
  /** @lends clay.picking.RayPicking.prototype */
  {
    /**
     * Pick the nearest intersection object in the scene
     * @param  {number} x Mouse position x
     * @param  {number} y Mouse position y
     * @param  {boolean} [forcePickAll=false] ignore ignorePicking
     * @return {clay.picking.RayPicking~Intersection}
     */
    pick: function(e, t, r) {
      var i = this.pickAll(e, t, [], r);
      return i[0] || null;
    },
    /**
     * Pick all intersection objects, wich will be sorted from near to far
     * @param  {number} x Mouse position x
     * @param  {number} y Mouse position y
     * @param  {Array} [output]
     * @param  {boolean} [forcePickAll=false] ignore ignorePicking
     * @return {Array.<clay.picking.RayPicking~Intersection>}
     */
    pickAll: function(e, t, r, i) {
      return this.renderer.screenToNDC(e, t, this._ndc), this.camera.castRay(this._ndc, this._ray), r = r || [], this._intersectNode(this.scene, r, i || !1), r.sort(this._intersectionCompareFunc), r;
    },
    _intersectNode: function(e, t, r) {
      e instanceof Re && e.isRenderable() && (!e.ignorePicking || r) && // Only triangle mesh support ray picking
      (e.mode === M.TRIANGLES && e.geometry.isUseIndices() || e.geometry.pickByRay || e.geometry.pick) && this._intersectRenderable(e, t);
      for (var i = 0; i < e._children.length; i++)
        this._intersectNode(e._children[i], t, r);
    },
    _intersectRenderable: (function() {
      var e = new S(), t = new S(), r = new S(), i = new Pt(), n = new I();
      return function(a, o) {
        var s = a.isSkinnedMesh();
        i.copy(this._ray), I.invert(n, a.worldTransform), s || i.applyTransform(n);
        var l = a.geometry, u = s ? a.skeleton.boundingBox : l.boundingBox;
        if (!(u && !i.intersectBoundingBox(u))) {
          if (l.pick) {
            l.pick(
              this._ndc.x,
              this._ndc.y,
              this.renderer,
              this.camera,
              a,
              o
            );
            return;
          } else if (l.pickByRay) {
            l.pickByRay(i, a, o);
            return;
          }
          var f = a.cullFace === M.BACK && a.frontFace === M.CCW || a.cullFace === M.FRONT && a.frontFace === M.CW, h, d = l.indices, c = l.attributes.position, v = l.attributes.weight, p = l.attributes.joint, m, _ = [];
          if (!(!c || !c.value || !d)) {
            if (s) {
              m = a.skeleton.getSubSkinMatrices(a.__uid__, a.joints);
              for (var x = 0; x < a.joints.length; x++) {
                _[x] = _[x] || [];
                for (var y = 0; y < 16; y++)
                  _[x][y] = m[x * 16 + y];
              }
              var g = [], b = [], w = [], C = [], A = [], R = l.attributes.skinnedPosition;
              (!R || !R.value) && (l.createAttribute("skinnedPosition", "f", 3), R = l.attributes.skinnedPosition, R.init(l.vertexCount));
              for (var x = 0; x < l.vertexCount; x++) {
                c.get(x, g), v.get(x, b), p.get(x, w), b[3] = 1 - b[0] - b[1] - b[2], T.set(C, 0, 0, 0);
                for (var y = 0; y < 4; y++)
                  w[y] >= 0 && b[y] > 1e-4 && (T.transformMat4(A, g, _[w[y]]), T.scaleAndAdd(C, C, A, b[y]));
                R.set(x, C);
              }
            }
            for (var x = 0; x < d.length; x += 3) {
              var H = d[x], O = d[x + 1], z = d[x + 2], G = s ? l.attributes.skinnedPosition : c;
              if (G.get(H, e.array), G.get(O, t.array), G.get(z, r.array), f ? h = i.intersectTriangle(e, t, r, a.culling) : h = i.intersectTriangle(e, r, t, a.culling), h) {
                var F = new S();
                s ? S.copy(F, h) : S.transformMat4(F, h, a.worldTransform), o.push(new Bn.Intersection(
                  h,
                  F,
                  a,
                  [H, O, z],
                  x / 3,
                  S.dist(F, this._ray.origin)
                ));
              }
            }
          }
        }
      };
    })(),
    _intersectionCompareFunc: function(e, t) {
      return e.distance - t.distance;
    }
  }
);
Bn.Intersection = function(e, t, r, i, n, a) {
  this.point = e, this.pointWorld = t, this.target = r, this.triangle = i, this.triangleIndex = n, this.distance = a;
};
var Qo;
Qo = at.hasGlobalWindow && (window.requestAnimationFrame && window.requestAnimationFrame.bind(window) || window.msRequestAnimationFrame && window.msRequestAnimationFrame.bind(window) || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame) || function(e) {
  return setTimeout(e, 16);
};
var re = function(e, t) {
  this.id = e, this.zr = t;
  try {
    this.renderer = new Ve({
      clearBit: 0,
      devicePixelRatio: t.painter.dpr,
      preserveDrawingBuffer: !0,
      // PENDING
      premultipliedAlpha: !0
    }), this.renderer.resize(t.painter.getWidth(), t.painter.getHeight());
  } catch (i) {
    this.renderer = null, this.dom = document.createElement("div"), this.dom.style.cssText = "position:absolute; left: 0; top: 0; right: 0; bottom: 0;", this.dom.className = "ecgl-nowebgl", this.dom.innerHTML = "Sorry, your browser does not support WebGL", console.error(i);
    return;
  }
  this.onglobalout = this.onglobalout.bind(this), t.on("globalout", this.onglobalout), this.dom = this.renderer.canvas;
  var r = this.dom.style;
  r.position = "absolute", r.left = "0", r.top = "0", this.views = [], this._picking = new Bn({
    renderer: this.renderer
  }), this._viewsToDispose = [], this._accumulatingId = 0, this._zrEventProxy = new bo({
    shape: {
      x: -1,
      y: -1,
      width: 2,
      height: 2
    },
    // FIXME Better solution.
    __isGLToZRProxy: !0
  }), this._backgroundColor = null, this._disposed = !1;
};
re.prototype.setUnpainted = function() {
};
re.prototype.addView = function(e) {
  if (e.layer !== this) {
    var t = this._viewsToDispose.indexOf(e);
    t >= 0 && this._viewsToDispose.splice(t, 1), this.views.push(e), e.layer = this;
    var r = this.zr;
    e.scene.traverse(function(i) {
      i.__zr = r, i.addAnimatorsToZr && i.addAnimatorsToZr(r);
    });
  }
};
function $o(e) {
  var t = e.__zr;
  e.__zr = null, t && e.removeAnimatorsFromZr && e.removeAnimatorsFromZr(t);
}
re.prototype.removeView = function(e) {
  if (e.layer === this) {
    var t = this.views.indexOf(e);
    t >= 0 && (this.views.splice(t, 1), e.scene.traverse($o, this), e.layer = null, this._viewsToDispose.push(e));
  }
};
re.prototype.removeViewsAll = function() {
  this.views.forEach(function(e) {
    e.scene.traverse($o, this), e.layer = null, this._viewsToDispose.push(e);
  }, this), this.views.length = 0;
};
re.prototype.resize = function(e, t) {
  var r = this.renderer;
  r.resize(e, t);
};
re.prototype.clear = function() {
  var e = this.renderer.gl, t = this._backgroundColor || [0, 0, 0, 0];
  e.clearColor(t[0], t[1], t[2], t[3]), e.depthMask(!0), e.colorMask(!0, !0, !0, !0), e.clear(e.DEPTH_BUFFER_BIT | e.COLOR_BUFFER_BIT);
};
re.prototype.clearDepth = function() {
  var e = this.renderer.gl;
  e.clear(e.DEPTH_BUFFER_BIT);
};
re.prototype.clearColor = function() {
  var e = this.renderer.gl;
  e.clearColor(0, 0, 0, 0), e.clear(e.COLOR_BUFFER_BIT);
};
re.prototype.needsRefresh = function() {
  this.zr.refresh();
};
re.prototype.refresh = function(e) {
  this._backgroundColor = e ? E.parseColor(e) : [0, 0, 0, 0], this.renderer.clearColor = this._backgroundColor;
  for (var t = 0; t < this.views.length; t++)
    this.views[t].prepareRender(this.renderer);
  this._doRender(!1), this._trackAndClean();
  for (var t = 0; t < this._viewsToDispose.length; t++)
    this._viewsToDispose[t].dispose(this.renderer);
  this._viewsToDispose.length = 0, this._startAccumulating();
};
re.prototype.renderToCanvas = function(e) {
  this._startAccumulating(!0), e.drawImage(this.dom, 0, 0, e.canvas.width, e.canvas.height);
};
re.prototype._doRender = function(e) {
  this.clear(), this.renderer.saveViewport();
  for (var t = 0; t < this.views.length; t++)
    this.views[t].render(this.renderer, e);
  this.renderer.restoreViewport();
};
re.prototype._stopAccumulating = function() {
  this._accumulatingId = 0, clearTimeout(this._accumulatingTimeout);
};
var xh = 1;
re.prototype._startAccumulating = function(e) {
  var t = this;
  this._stopAccumulating();
  for (var r = !1, i = 0; i < this.views.length; i++)
    r = this.views[i].needsAccumulate() || r;
  if (!r)
    return;
  function n(a) {
    if (!(!t._accumulatingId || a !== t._accumulatingId)) {
      for (var o = !0, s = 0; s < t.views.length; s++)
        o = t.views[s].isAccumulateFinished() && r;
      o || (t._doRender(!0), e ? n(a) : Qo(function() {
        n(a);
      }));
    }
  }
  this._accumulatingId = xh++, e ? n(t._accumulatingId) : this._accumulatingTimeout = setTimeout(function() {
    n(t._accumulatingId);
  }, 50);
};
re.prototype._trackAndClean = function() {
  var e = [], t = [];
  this._textureList && (si(this._textureList), si(this._geometriesList));
  for (var r = 0; r < this.views.length; r++)
    Th(this.views[r].scene, e, t);
  this._textureList && (li(this.renderer, this._textureList), li(this.renderer, this._geometriesList)), this._textureList = e, this._geometriesList = t;
};
function si(e) {
  for (var t = 0; t < e.length; t++)
    e[t].__used__ = 0;
}
function li(e, t) {
  for (var r = 0; r < t.length; r++)
    t[r].__used__ || t[r].dispose(e);
}
function Kr(e, t) {
  e.__used__ = e.__used__ || 0, e.__used__++, e.__used__ === 1 && t.push(e);
}
function Th(e, t, r) {
  var i, n;
  e.traverse(function(o) {
    if (o.isRenderable()) {
      var s = o.geometry, l = o.material;
      if (l !== i)
        for (var u = l.getTextureUniforms(), f = 0; f < u.length; f++) {
          var h = u[f], d = l.uniforms[h].value;
          if (d) {
            if (d instanceof N)
              Kr(d, t);
            else if (d instanceof Array)
              for (var c = 0; c < d.length; c++)
                d[c] instanceof N && Kr(d[c], t);
          }
        }
      s !== n && Kr(s, r), i = l, n = s;
    }
  });
  for (var a = 0; a < e.lights.length; a++)
    e.lights[a].cubemap && Kr(e.lights[a].cubemap, t);
}
re.prototype.dispose = function() {
  this._disposed || (this._stopAccumulating(), this._textureList && (si(this._textureList), si(this._geometriesList), li(this.renderer, this._textureList), li(this.renderer, this._geometriesList)), this.zr.off("globalout", this.onglobalout), this._disposed = !0);
};
re.prototype.onmousedown = function(e) {
  if (!(e.target && e.target.__isGLToZRProxy)) {
    e = e.event;
    var t = this.pickObject(e.offsetX, e.offsetY);
    t && (this._dispatchEvent("mousedown", e, t), this._dispatchDataEvent("mousedown", e, t)), this._downX = e.offsetX, this._downY = e.offsetY;
  }
};
re.prototype.onmousemove = function(e) {
  if (!(e.target && e.target.__isGLToZRProxy)) {
    e = e.event;
    var t = this.pickObject(e.offsetX, e.offsetY), r = t && t.target, i = this._hovered;
    this._hovered = t, i && r !== i.target && (i.relatedTarget = r, this._dispatchEvent("mouseout", e, i), this.zr.setCursorStyle("default")), this._dispatchEvent("mousemove", e, t), t && (this.zr.setCursorStyle("pointer"), (!i || r !== i.target) && this._dispatchEvent("mouseover", e, t)), this._dispatchDataEvent("mousemove", e, t);
  }
};
re.prototype.onmouseup = function(e) {
  if (!(e.target && e.target.__isGLToZRProxy)) {
    e = e.event;
    var t = this.pickObject(e.offsetX, e.offsetY);
    t && (this._dispatchEvent("mouseup", e, t), this._dispatchDataEvent("mouseup", e, t)), this._upX = e.offsetX, this._upY = e.offsetY;
  }
};
re.prototype.onclick = re.prototype.dblclick = function(e) {
  if (!(e.target && e.target.__isGLToZRProxy)) {
    var t = this._upX - this._downX, r = this._upY - this._downY;
    if (!(Math.sqrt(t * t + r * r) > 20)) {
      e = e.event;
      var i = this.pickObject(e.offsetX, e.offsetY);
      i && (this._dispatchEvent(e.type, e, i), this._dispatchDataEvent(e.type, e, i));
      var n = this._clickToSetFocusPoint(e);
      if (n) {
        var a = n.view.setDOFFocusOnPoint(n.distance);
        a && this.zr.refresh();
      }
    }
  }
};
re.prototype._clickToSetFocusPoint = function(e) {
  for (var t = this.renderer, r = t.viewport, i = this.views.length - 1; i >= 0; i--) {
    var n = this.views[i];
    if (n.hasDOF() && n.containPoint(e.offsetX, e.offsetY)) {
      this._picking.scene = n.scene, this._picking.camera = n.camera, t.viewport = n.viewport;
      var a = this._picking.pick(e.offsetX, e.offsetY, !0);
      if (a)
        return a.view = n, a;
    }
  }
  t.viewport = r;
};
re.prototype.onglobalout = function(e) {
  var t = this._hovered;
  t && this._dispatchEvent("mouseout", e, {
    target: t.target
  });
};
re.prototype.pickObject = function(e, t) {
  for (var r = [], i = this.renderer, n = i.viewport, a = 0; a < this.views.length; a++) {
    var o = this.views[a];
    o.containPoint(e, t) && (this._picking.scene = o.scene, this._picking.camera = o.camera, i.viewport = o.viewport, this._picking.pickAll(e, t, r));
  }
  return i.viewport = n, r.sort(function(s, l) {
    return s.distance - l.distance;
  }), r[0];
};
re.prototype._dispatchEvent = function(e, t, r) {
  r || (r = {});
  var i = r.target;
  for (r.cancelBubble = !1, r.event = t, r.type = e, r.offsetX = t.offsetX, r.offsetY = t.offsetY; i && (i.trigger(e, r), i = i.getParent(), !r.cancelBubble); )
    ;
  this._dispatchToView(e, r);
};
re.prototype._dispatchDataEvent = function(e, t, r) {
  var i = r && r.target, n = i && i.dataIndex, a = i && i.seriesIndex, o = i && i.eventData, s = !1, l = this._zrEventProxy;
  l.x = t.offsetX, l.y = t.offsetY, l.update();
  var u = {
    target: l
  };
  const f = $r(l);
  e === "mousemove" && (n != null ? n !== this._lastDataIndex && (parseInt(this._lastDataIndex, 10) >= 0 && (f.dataIndex = this._lastDataIndex, f.seriesIndex = this._lastSeriesIndex, this.zr.handler.dispatchToElement(u, "mouseout", t)), s = !0) : o != null && o !== this._lastEventData && (this._lastEventData != null && (f.eventData = this._lastEventData, this.zr.handler.dispatchToElement(u, "mouseout", t)), s = !0), this._lastEventData = o, this._lastDataIndex = n, this._lastSeriesIndex = a), f.eventData = o, f.dataIndex = n, f.seriesIndex = a, (o != null || parseInt(n, 10) >= 0 && parseInt(a, 10) >= 0) && (this.zr.handler.dispatchToElement(u, e, t), s && this.zr.handler.dispatchToElement(u, "mouseover", t));
};
re.prototype._dispatchToView = function(e, t) {
  for (var r = 0; r < this.views.length; r++)
    this.views[r].containPoint(t.offsetX, t.offsetY) && this.views[r].trigger(e, t);
};
Object.assign(re.prototype, Rn);
var wh = ["bar3D", "line3D", "map3D", "scatter3D", "surface", "lines3D", "scatterGL", "scatter3D"];
function pr(e, t) {
  if (e && e[t] && (e[t].normal || e[t].emphasis)) {
    var r = e[t].normal, i = e[t].emphasis;
    r && (e[t] = r), i && (e.emphasis = e.emphasis || {}, e.emphasis[t] = i);
  }
}
function bh(e) {
  pr(e, "itemStyle"), pr(e, "lineStyle"), pr(e, "areaStyle"), pr(e, "label");
}
function Qr(e) {
  e && (e instanceof Array || (e = [e]), rt(e, function(t) {
    if (t.axisLabel) {
      var r = t.axisLabel;
      Object.assign(r, r.textStyle), r.textStyle = null;
    }
  }));
}
function Eh(e) {
  rt(e.series, function(t) {
    hn(wh, t.type) >= 0 && (bh(t), t.coordinateSystem === "mapbox" && (t.coordinateSystem = "mapbox3D", e.mapbox3D = e.mapbox));
  }), Qr(e.xAxis3D), Qr(e.yAxis3D), Qr(e.zAxis3D), Qr(e.grid3D), pr(e.geo3D);
}
function Sh(e, t, r) {
  try {
    return Object.defineProperty(e, t, {
      value: r,
      configurable: !0,
      writable: !0
    }), !0;
  } catch {
  }
  try {
    return Reflect.set(e, t, r);
  } catch {
  }
  return !1;
}
var Ar = uu;
Sh(Ar, "graphicGL", E);
function Jo(e) {
  this._layers = {}, this._zr = e;
}
Jo.prototype.update = function(e, t) {
  var r = this, i = t.getZr();
  if (!i.getWidth() || !i.getHeight()) {
    console.warn("Dom has no width or height");
    return;
  }
  function n(s) {
    i.setSleepAfterStill(0);
    var l;
    s.coordinateSystem && s.coordinateSystem.model, l = s.get("zlevel");
    var u = r._layers, f = u[l];
    if (!f) {
      if (f = u[l] = new re("gl-" + l, i), i.painter.isSingleCanvas()) {
        f.virtual = !0;
        var h = new Ar.graphic.Image({
          z: 1e4,
          style: {
            image: f.renderer.canvas
          },
          silent: !0
        });
        f.__hostImage = h, i.add(h);
      }
      i.painter.insertLayer(l, f);
    }
    return f.__hostImage && f.__hostImage.setStyle({
      width: f.renderer.getWidth(),
      height: f.renderer.getHeight()
    }), f;
  }
  function a(s, l) {
    s && s.traverse(function(u) {
      u.isRenderable && u.isRenderable() && (u.ignorePicking = u.$ignorePicking != null ? u.$ignorePicking : l);
    });
  }
  for (var o in this._layers)
    this._layers[o].removeViewsAll();
  e.eachComponent(function(s, l) {
    if (s !== "series") {
      var u = t.getViewOfComponentModel(l), f = l.coordinateSystem;
      if (u.__ecgl__) {
        var h;
        if (f) {
          if (!f.viewGL) {
            console.error("Can't find viewGL in coordinateSystem of component " + l.id);
            return;
          }
          h = f.viewGL;
        } else {
          if (!l.viewGL) {
            console.error("Can't find viewGL of component " + l.id);
            return;
          }
          h = f.viewGL;
        }
        var h = f.viewGL, d = n(l);
        d.addView(h), u.afterRender && u.afterRender(l, e, t, d), a(u.groupGL, l.get("silent"));
      }
    }
  }), e.eachSeries(function(s) {
    var l = t.getViewOfSeriesModel(s), u = s.coordinateSystem;
    if (l.__ecgl__) {
      if (u && !u.viewGL && !l.viewGL) {
        console.error("Can't find viewGL of series " + l.id);
        return;
      }
      var f = u && u.viewGL || l.viewGL, h = n(s);
      h.addView(f), l.afterRender && l.afterRender(s, e, t, h), a(l.groupGL, s.get("silent"));
    }
  });
};
Ar.registerPostInit(function(e) {
  var t = e.getZr(), r = t.painter.dispose;
  t.painter.dispose = function() {
    typeof this.eachOtherLayer == "function" && this.eachOtherLayer(function(i) {
      i instanceof re && i.dispose();
    }), r.call(this);
  };
});
Ar.registerPostUpdate(function(e, t) {
  var r = t.getZr(), i = r.__egl = r.__egl || new Jo(r);
  i.update(e, t);
});
Ar.registerPreprocessor(Eh);
const Ye = {
  vec3: T,
  vec4: L,
  mat3: V
};
var Ah = Ye.vec3, Ch = Pn.isDimensionStacked;
function Mh(e) {
  var t = e[0], r = e[1];
  return !(t > 0 && r > 0 || t < 0 && r < 0);
}
function Lh(e, t) {
  var r = e.getData(), i = e.get("barSize");
  if (i == null) {
    var n = t.size, a, o, s = t.getAxis("x"), l = t.getAxis("y");
    s.type === "category" ? a = s.getBandWidth() * 0.7 : a = Math.round(n[0] / Math.sqrt(r.count())) * 0.6, l.type === "category" ? o = l.getBandWidth() * 0.7 : o = Math.round(n[1] / Math.sqrt(r.count())) * 0.6, i = [a, o];
  } else it(i) || (i = [i, i]);
  var u = t.getAxis("z").scale.getExtent(), f = Mh(u), h = ["x", "y", "z"].map(function(v) {
    return e.coordDimToDataDim(v)[0];
  }), d = Ch(r, h[2]), c = d ? r.getCalculationInfo("stackResultDimension") : h[2];
  r.each(h, function(v, p, m, _) {
    var x = r.get(c, _), y = d ? x - m : f ? 0 : u[0], g = t.dataToPoint([v, p, y]), b = t.dataToPoint([v, p, x]), w = Ah.dist(g, b), C = [0, b[1] < g[1] ? -1 : 1, 0];
    Math.abs(w) === 0 && (w = 0.1);
    var A = [i[0], w, i[1]];
    r.setItemLayout(_, [g, C, A]);
  }), r.setLayout("orient", [1, 0, 0]);
}
function Fn(e, t, r) {
  for (var i = e.getDataExtent(t), n = e.getDataExtent(r), a = i[1] - i[0] || i[0], o = n[1] - n[0] || n[0], s = 50, l = new Uint8Array(s * s), u = 0; u < e.count(); u++) {
    var f = e.get(t, u), h = e.get(r, u), d = Math.floor((f - i[0]) / a * (s - 1)), c = Math.floor((h - n[0]) / o * (s - 1)), v = c * s + d;
    l[v] = l[v] || 1;
  }
  for (var p = 0, u = 0; u < l.length; u++)
    l[u] && p++;
  return p / l.length;
}
var Qa = Ye.vec3, Dh = Pn.isDimensionStacked;
function Ph(e, t) {
  var r = e.getData(), i = e.get("minHeight") || 0, n = e.get("barSize"), a = ["lng", "lat", "alt"].map(function(u) {
    return e.coordDimToDataDim(u)[0];
  });
  if (n == null) {
    var o = t.radius * Math.PI, s = Fn(r, a[0], a[1]);
    n = [o / Math.sqrt(r.count() / s), o / Math.sqrt(r.count() / s)];
  } else it(n) || (n = [n, n]);
  var l = Un(r, a);
  r.each(a, function(u, f, h, d) {
    var c = r.get(l.dimension, d), v = l.isStacked ? c - h : t.altitudeAxis.scale.getExtent()[0], p = Math.max(t.altitudeAxis.dataToCoord(h), i), m = t.dataToPoint([u, f, v]), _ = t.dataToPoint([u, f, c]), x = Qa.sub([], _, m);
    Qa.normalize(x, x);
    var y = [n[0], p, n[1]];
    r.setItemLayout(d, [m, x, y]);
  }), r.setLayout("orient", S.UP.array);
}
function Rh(e, t) {
  var r = e.getData(), i = e.get("barSize"), n = e.get("minHeight") || 0, a = ["lng", "lat", "alt"].map(function(f) {
    return e.coordDimToDataDim(f)[0];
  });
  if (i == null) {
    var o = Math.min(t.size[0], t.size[2]), s = Fn(r, a[0], a[1]);
    i = [o / Math.sqrt(r.count() / s), o / Math.sqrt(r.count() / s)];
  } else it(i) || (i = [i, i]);
  var l = [0, 1, 0], u = Un(r, a);
  r.each(a, function(f, h, d, c) {
    var v = r.get(u.dimension, c), p = u.isStacked ? v - d : t.altitudeAxis.scale.getExtent()[0], m = Math.max(t.altitudeAxis.dataToCoord(d), n), _ = t.dataToPoint([f, h, p]), x = [i[0], m, i[1]];
    r.setItemLayout(c, [_, l, x]);
  }), r.setLayout("orient", [1, 0, 0]);
}
function Nh(e, t) {
  var r = e.getData(), i = e.coordDimToDataDim("lng")[0], n = e.coordDimToDataDim("lat")[0], a = e.coordDimToDataDim("alt")[0], o = e.get("barSize"), s = e.get("minHeight") || 0;
  if (o == null) {
    var l = r.getDataExtent(i), u = r.getDataExtent(n), f = t.dataToPoint([l[0], u[0]]), h = t.dataToPoint([l[1], u[1]]), d = Math.min(Math.abs(f[0] - h[0]), Math.abs(f[1] - h[1])) || 1, c = Fn(r, i, n);
    o = [d / Math.sqrt(r.count() / c), d / Math.sqrt(r.count() / c)];
  } else
    it(o) || (o = [o, o]), o[0] /= t.getScale() / 16, o[1] /= t.getScale() / 16;
  var v = [0, 0, 1], p = [i, n, a], m = Un(r, p);
  r.each(p, function(_, x, y, g) {
    var b = r.get(m.dimension, g), w = m.isStacked ? b - y : 0, C = t.dataToPoint([_, x, w]), A = t.dataToPoint([_, x, b]), R = Math.max(A[2] - C[2], s), H = [o[0], R, o[1]];
    r.setItemLayout(g, [C, v, H]);
  }), r.setLayout("orient", [1, 0, 0]);
}
function Un(e, t) {
  var r = Dh(e, t[2]);
  return {
    dimension: r ? e.getCalculationInfo("stackResultDimension") : t[2],
    isStacked: r
  };
}
function Oh(e) {
  e.registerLayout(function(t, r) {
    t.eachSeriesByType("bar3D", function(i) {
      var n = i.coordinateSystem, a = n && n.type;
      a === "globe" ? Ph(i, n) : a === "cartesian3D" ? Lh(i, n) : a === "geo3D" ? Rh(i, n) : (a === "mapbox3D" || a === "maptalks3D") && Nh(i, n);
    });
  });
}
const Ih = {
  defaultOption: {
    shading: null,
    realisticMaterial: {
      textureTiling: 1,
      textureOffset: 0,
      detailTexture: null
    },
    lambertMaterial: {
      textureTiling: 1,
      textureOffset: 0,
      detailTexture: null
    },
    colorMaterial: {
      textureTiling: 1,
      textureOffset: 0,
      detailTexture: null
    },
    hatchingMaterial: {
      textureTiling: 1,
      textureOffset: 0,
      paperColor: "#fff"
    }
  }
};
var Jt = {};
Jt.getFormattedLabel = function(e, t, r, i, n) {
  r = r || "normal";
  var a = e.getData(i), o = a.getItemModel(t), s = e.getDataParams(t, i);
  n != null && s.value instanceof Array && (s.value = s.value[n]);
  var l = o.get(r === "normal" ? ["label", "formatter"] : ["emphasis", "label", "formatter"]);
  l == null && (l = o.get(["label", "formatter"]));
  var u;
  return typeof l == "function" ? (s.status = r, u = l(s)) : typeof l == "string" && (u = Pl(l, s)), u;
};
Jt.normalizeToArray = function(e) {
  return e instanceof Array ? e : e == null ? [] : [e];
};
function Bh(e) {
  const t = e.getVisual("style");
  if (t) {
    const r = e.getVisual("drawType");
    return t[r];
  }
}
function Fh(e) {
  return e.getVisual("style").opacity;
}
function ut(e, t) {
  const r = e.getItemVisual(t, "style");
  if (r) {
    const i = e.getVisual("drawType");
    return r[i];
  }
}
function mt(e, t) {
  const r = e.getItemVisual(t, "style");
  return r && r.opacity;
}
function Uh(e, t) {
  var r = [];
  return rt(e.dimensions, function(i) {
    var n = e.getDimensionInfo(i), a = n.otherDims, o = a[t];
    o != null && o !== !1 && (r[o] = n.name);
  }), r;
}
function zn(e, t, r) {
  function i(h) {
    var d = [], c = Uh(n, "tooltip");
    c.length ? rt(c, function(p) {
      v(n.get(p, t), p);
    }) : rt(h, v);
    function v(p, m) {
      var _ = n.getDimensionInfo(m);
      if (!(!_ || _.otherDims.tooltip === !1)) {
        var x = _.type, y = "- " + (_.tooltipName || _.name) + ": " + (x === "ordinal" ? p + "" : x === "time" ? Nl("yyyy/MM/dd hh:mm:ss", p) : na(p));
        y && d.push(Lr(y));
      }
    }
    return "<br/>" + d.join("<br/>");
  }
  var n = e.getData(), a = e.getRawValue(t), o = it(a) ? i(a) : Lr(na(a)), s = n.getName(t), l = ut(n, t);
  Eo(l) && l.colorStops && (l = (l.colorStops[0] || {}).color), l = l || "transparent";
  var u = Rl(l), f = e.name;
  return f === "\0-" && (f = ""), f = f ? Lr(f) + "<br/>" : "", f + u + (s ? Lr(s) + ": " + o : o);
}
function Hn(e, t, r) {
  r = r || e.getSource();
  var i = To(e.get("coordinateSystem")) || ["x", "y", "z"], n = Ol(r, {
    dimensionsDefine: r.dimensionsDefine || e.get("dimensions"),
    encodeDefine: r.encodeDefine || e.get("encode"),
    coordDimensions: i.map(function(s) {
      var l = e.getReferringComponents(s + "Axis3D").models[0];
      return {
        type: l && l.get("type") === "category" ? "ordinal" : "float",
        name: s
        // Find stackable dimension. Which will represent value.
        // stackable: dim === 'z'
      };
    })
  });
  e.get("coordinateSystem") === "cartesian3D" && n.forEach(function(s) {
    if (i.indexOf(s.coordDim) >= 0) {
      var l = e.getReferringComponents(s.coordDim + "Axis3D").models[0];
      l && l.get("type") === "category" && (s.ordinalMeta = l.getOrdinalMeta());
    }
  });
  var a = Pn.enableDataStack(
    // Only support 'z' and `byIndex` now.
    e,
    n,
    {
      byIndex: !0,
      stackedCoordDimension: "z"
    }
  ), o = new yo(n, e);
  return o.setCalculationInfo(a), o.initData(r), o;
}
var es = gi.extend({
  type: "series.bar3D",
  dependencies: ["globe"],
  visualStyleAccessPathvisu: "itemStyle",
  getInitialData: function(e, t) {
    return Hn(this);
  },
  getFormattedLabel: function(e, t, r, i) {
    var n = Jt.getFormattedLabel(this, e, t, r, i);
    return n == null && (n = this.getData().get("z", e)), n;
  },
  formatTooltip: function(e) {
    return zn(this, e);
  },
  defaultOption: {
    coordinateSystem: "cartesian3D",
    globeIndex: 0,
    grid3DIndex: 0,
    zlevel: -10,
    // bevelSize, 0 has no bevel
    bevelSize: 0,
    // higher is smoother
    bevelSmoothness: 2,
    // Bar width and depth
    // barSize: [1, 1],
    // On grid plane when coordinateSystem is cartesian3D
    onGridPlane: "xy",
    // Shading of globe
    shading: "color",
    minHeight: 0,
    itemStyle: {
      opacity: 1
    },
    label: {
      show: !1,
      distance: 2,
      textStyle: {
        fontSize: 14,
        color: "#000",
        backgroundColor: "rgba(255,255,255,0.7)",
        padding: 3,
        borderRadius: 3
      }
    },
    emphasis: {
      label: {
        show: !0
      }
    },
    animationDurationUpdate: 500
  }
});
lt(es.prototype, Ih);
const wi = {
  convertToDynamicArray: function(e) {
    e && this.resetOffset();
    var t = this.attributes;
    for (var r in t)
      e || !t[r].value ? t[r].value = [] : t[r].value = Array.prototype.slice.call(t[r].value);
    e || !this.indices ? this.indices = [] : this.indices = Array.prototype.slice.call(this.indices);
  },
  convertToTypedArray: function() {
    var e = this.attributes;
    for (var t in e)
      e[t].value && e[t].value.length > 0 ? e[t].value = new Float32Array(e[t].value) : e[t].value = null;
    this.indices && this.indices.length > 0 && (this.indices = this.vertexCount > 65535 ? new Uint32Array(this.indices) : new Uint16Array(this.indices)), this.dirty();
  }
};
function tn(e, t, r) {
  var i = e[t];
  e[t] = e[r], e[r] = i;
}
function ts(e, t, r, i, n) {
  var a = r, o = e[t];
  tn(e, t, i);
  for (var s = r; s < i; s++)
    n(e[s], o) < 0 && (tn(e, s, a), a++);
  return tn(e, i, a), a;
}
function ui(e, t, r, i) {
  if (r < i) {
    var n = Math.floor((r + i) / 2), a = ts(e, n, r, i, t);
    ui(e, t, r, a - 1), ui(e, t, a + 1, i);
  }
}
function Qt() {
  this._parts = [];
}
Qt.prototype.step = function(e, t, r) {
  var i = e.length;
  if (r === 0) {
    this._parts = [], this._sorted = !1;
    var n = Math.floor(i / 2);
    this._parts.push({
      pivot: n,
      left: 0,
      right: i - 1
    }), this._currentSortPartIdx = 0;
  }
  if (!this._sorted) {
    var a = this._parts;
    if (a.length === 0)
      return this._sorted = !0, !0;
    if (a.length < 512) {
      for (var o = 0; o < a.length; o++)
        a[o].pivot = ts(e, a[o].pivot, a[o].left, a[o].right, t);
      for (var s = [], o = 0; o < a.length; o++) {
        var l = a[o].left, u = a[o].pivot - 1;
        u > l && s.push({
          pivot: Math.floor((u + l) / 2),
          left: l,
          right: u
        });
        var l = a[o].pivot + 1, u = a[o].right;
        u > l && s.push({
          pivot: Math.floor((u + l) / 2),
          left: l,
          right: u
        });
      }
      a = this._parts = s;
    } else
      for (var o = 0; o < Math.floor(a.length / 10); o++) {
        var f = a.length - 1 - this._currentSortPartIdx;
        if (ui(e, t, a[f].left, a[f].right), this._currentSortPartIdx++, this._currentSortPartIdx === a.length)
          return this._sorted = !0, !0;
      }
    return !1;
  }
};
Qt.sort = ui;
var qt = Ye.vec3, $a = qt.create(), Ja = qt.create(), eo = qt.create();
const zh = {
  needsSortTriangles: function() {
    return this.indices && this.sortTriangles;
  },
  needsSortTrianglesProgressively: function() {
    return this.needsSortTriangles() && this.triangleCount >= 2e4;
  },
  doSortTriangles: function(e, t) {
    var r = this.indices;
    if (t === 0) {
      var i = this.attributes.position, e = e.array;
      (!this._triangleZList || this._triangleZList.length !== this.triangleCount) && (this._triangleZList = new Float32Array(this.triangleCount), this._sortedTriangleIndices = new Uint32Array(this.triangleCount), this._indicesTmp = new r.constructor(r.length), this._triangleZListTmp = new Float32Array(this.triangleCount));
      for (var n = 0, a, o = 0; o < r.length; ) {
        i.get(r[o++], $a), i.get(r[o++], Ja), i.get(r[o++], eo);
        var s = qt.sqrDist($a, e), l = qt.sqrDist(Ja, e), u = qt.sqrDist(eo, e), f = Math.min(s, l);
        f = Math.min(f, u), o === 3 ? (a = f, f = 0) : f = f - a, this._triangleZList[n++] = f;
      }
    }
    for (var h = this._sortedTriangleIndices, o = 0; o < h.length; o++)
      h[o] = o;
    if (this.triangleCount < 2e4)
      t === 0 && this._simpleSort(!0);
    else
      for (var o = 0; o < 3; o++)
        this._progressiveQuickSort(t * 3 + o);
    for (var d = this._indicesTmp, c = this._triangleZListTmp, v = this._triangleZList, o = 0; o < this.triangleCount; o++) {
      var p = h[o] * 3, m = o * 3;
      d[m++] = r[p++], d[m++] = r[p++], d[m] = r[p], c[o] = v[h[o]];
    }
    var _ = this._indicesTmp;
    this._indicesTmp = this.indices, this.indices = _;
    var _ = this._triangleZListTmp;
    this._triangleZListTmp = this._triangleZList, this._triangleZList = _, this.dirtyIndices();
  },
  _simpleSort: function(e) {
    var t = this._triangleZList, r = this._sortedTriangleIndices;
    function i(n, a) {
      return t[a] - t[n];
    }
    e ? Array.prototype.sort.call(r, i) : Qt.sort(r, i, 0, r.length - 1);
  },
  _progressiveQuickSort: function(e) {
    var t = this._triangleZList, r = this._sortedTriangleIndices;
    this._quickSort = this._quickSort || new Qt(), this._quickSort.step(r, function(i, n) {
      return t[n] - t[i];
    }, e);
  }
};
var ue = Ye.vec3, Hh = Ye.mat3, Gn = $.extend(
  function() {
    return {
      attributes: {
        position: new $.Attribute("position", "float", 3, "POSITION"),
        normal: new $.Attribute("normal", "float", 3, "NORMAL"),
        color: new $.Attribute("color", "float", 4, "COLOR"),
        prevPosition: new $.Attribute("prevPosition", "float", 3),
        prevNormal: new $.Attribute("prevNormal", "float", 3)
      },
      dynamic: !0,
      enableNormal: !1,
      bevelSize: 1,
      bevelSegments: 0,
      // Map from vertexIndex to dataIndex.
      _dataIndices: null,
      _vertexOffset: 0,
      _triangleOffset: 0
    };
  },
  /** @lends module:echarts-gl/chart/bars/BarsGeometry.prototype */
  {
    resetOffset: function() {
      this._vertexOffset = 0, this._triangleOffset = 0;
    },
    setBarCount: function(e) {
      var t = this.enableNormal, r = this.getBarVertexCount() * e, i = this.getBarTriangleCount() * e;
      this.vertexCount !== r && (this.attributes.position.init(r), t ? this.attributes.normal.init(r) : this.attributes.normal.value = null, this.attributes.color.init(r)), this.triangleCount !== i && (this.indices = r > 65535 ? new Uint32Array(i * 3) : new Uint16Array(i * 3), this._dataIndices = new Uint32Array(r));
    },
    getBarVertexCount: function() {
      var e = this.bevelSize > 0 ? this.bevelSegments : 0;
      return e > 0 ? this._getBevelBarVertexCount(e) : this.enableNormal ? 24 : 8;
    },
    getBarTriangleCount: function() {
      var e = this.bevelSize > 0 ? this.bevelSegments : 0;
      return e > 0 ? this._getBevelBarTriangleCount(e) : 12;
    },
    _getBevelBarVertexCount: function(e) {
      return (e + 1) * 4 * (e + 1) * 2;
    },
    _getBevelBarTriangleCount: function(e) {
      var t = e * 4 + 3, r = e * 2 + 1;
      return (t + 1) * r * 2 + 4;
    },
    setColor: function(e, t) {
      for (var r = this.getBarVertexCount(), i = r * e, n = r * (e + 1), a = i; a < n; a++)
        this.attributes.color.set(a, t);
      this.dirtyAttribute("color");
    },
    /**
     * Get dataIndex of vertex.
     * @param {number} vertexIndex
     */
    getDataIndexOfVertex: function(e) {
      return this._dataIndices ? this._dataIndices[e] : null;
    },
    /**
     * Add a bar
     * @param {Array.<number>} start
     * @param {Array.<number>} end
     * @param {Array.<number>} orient  right direction
     * @param {Array.<number>} size size on x and z
     * @param {Array.<number>} color
     */
    addBar: (function() {
      for (var e = ue.create, t = ue.scaleAndAdd, r = e(), i = e(), n = e(), a = e(), o = e(), s = e(), l = e(), u = [], f = [], h = 0; h < 8; h++)
        u[h] = e();
      for (var d = [
        // PX
        [0, 1, 5, 4],
        // NX
        [2, 3, 7, 6],
        // PY
        [4, 5, 6, 7],
        // NY
        [3, 2, 1, 0],
        // PZ
        [0, 4, 7, 3],
        // NZ
        [1, 2, 6, 5]
      ], c = [0, 1, 2, 0, 2, 3], v = [], h = 0; h < d.length; h++)
        for (var p = d[h], m = 0; m < 2; m++) {
          for (var _ = [], x = 0; x < 3; x++)
            _.push(p[c[m * 3 + x]]);
          v.push(_);
        }
      return function(y, g, b, w, C, A) {
        var R = this._vertexOffset;
        if (this.bevelSize > 0 && this.bevelSegments > 0)
          this._addBevelBar(y, g, b, w, this.bevelSize, this.bevelSegments, C);
        else {
          ue.copy(n, g), ue.normalize(n, n), ue.cross(a, b, n), ue.normalize(a, a), ue.cross(i, n, a), ue.normalize(a, a), ue.negate(o, i), ue.negate(s, n), ue.negate(l, a), t(u[0], y, i, w[0] / 2), t(u[0], u[0], a, w[2] / 2), t(u[1], y, i, w[0] / 2), t(u[1], u[1], l, w[2] / 2), t(u[2], y, o, w[0] / 2), t(u[2], u[2], l, w[2] / 2), t(u[3], y, o, w[0] / 2), t(u[3], u[3], a, w[2] / 2), t(r, y, n, w[1]), t(u[4], r, i, w[0] / 2), t(u[4], u[4], a, w[2] / 2), t(u[5], r, i, w[0] / 2), t(u[5], u[5], l, w[2] / 2), t(u[6], r, o, w[0] / 2), t(u[6], u[6], l, w[2] / 2), t(u[7], r, o, w[0] / 2), t(u[7], u[7], a, w[2] / 2);
          var H = this.attributes;
          if (this.enableNormal) {
            f[0] = i, f[1] = o, f[2] = n, f[3] = s, f[4] = a, f[5] = l;
            for (var O = this._vertexOffset, z = 0; z < d.length; z++) {
              for (var G = this._triangleOffset * 3, F = 0; F < 6; F++)
                this.indices[G++] = O + c[F];
              O += 4, this._triangleOffset += 2;
            }
            for (var z = 0; z < d.length; z++)
              for (var W = f[z], F = 0; F < 4; F++) {
                var X = d[z][F];
                H.position.set(this._vertexOffset, u[X]), H.normal.set(this._vertexOffset, W), H.color.set(this._vertexOffset++, C);
              }
          } else {
            for (var z = 0; z < v.length; z++) {
              for (var G = this._triangleOffset * 3, F = 0; F < 3; F++)
                this.indices[G + F] = v[z][F] + this._vertexOffset;
              this._triangleOffset++;
            }
            for (var z = 0; z < u.length; z++)
              H.position.set(this._vertexOffset, u[z]), H.color.set(this._vertexOffset++, C);
          }
        }
        for (var q = this._vertexOffset, z = R; z < q; z++)
          this._dataIndices[z] = A;
      };
    })(),
    /**
     * Add a bar with bevel
     * @param {Array.<number>} start
     * @param {Array.<number>} end
     * @param {Array.<number>} orient  right direction
     * @param {Array.<number>} size size on x and z
     * @param {number} bevelSize
     * @param {number} bevelSegments
     * @param {Array.<number>} color
     */
    _addBevelBar: (function() {
      var e = ue.create(), t = ue.create(), r = ue.create(), i = Hh.create(), n = [], a = [1, -1, -1, 1], o = [1, 1, -1, -1], s = [2, 0];
      return function(l, u, f, h, d, c, v) {
        ue.copy(t, u), ue.normalize(t, t), ue.cross(r, f, t), ue.normalize(r, r), ue.cross(e, t, r), ue.normalize(r, r), i[0] = e[0], i[1] = e[1], i[2] = e[2], i[3] = t[0], i[4] = t[1], i[5] = t[2], i[6] = r[0], i[7] = r[1], i[8] = r[2], d = Math.min(h[0], h[2]) / 2 * d;
        for (var p = 0; p < 3; p++)
          n[p] = Math.max(h[p] - d * 2, 0);
        for (var m = (h[0] - n[0]) / 2, _ = (h[1] - n[1]) / 2, x = (h[2] - n[2]) / 2, y = [], g = [], b = this._vertexOffset, w = [], p = 0; p < 2; p++) {
          w[p] = w[p] = [];
          for (var C = 0; C <= c; C++)
            for (var A = 0; A < 4; A++) {
              (C === 0 && p === 0 || p === 1 && C === c) && w[p].push(b);
              for (var R = 0; R <= c; R++) {
                var H = R / c * Math.PI / 2 + Math.PI / 2 * A, O = C / c * Math.PI / 2 + Math.PI / 2 * p;
                g[0] = m * Math.cos(H) * Math.sin(O), g[1] = _ * Math.cos(O), g[2] = x * Math.sin(H) * Math.sin(O), y[0] = g[0] + a[A] * n[0] / 2, y[1] = g[1] + _ + s[p] * n[1] / 2, y[2] = g[2] + o[A] * n[2] / 2, Math.abs(m - _) < 1e-6 && Math.abs(_ - x) < 1e-6 || (g[0] /= m * m, g[1] /= _ * _, g[2] /= x * x), ue.normalize(g, g), ue.transformMat3(y, y, i), ue.transformMat3(g, g, i), ue.add(y, y, l), this.attributes.position.set(b, y), this.enableNormal && this.attributes.normal.set(b, g), this.attributes.color.set(b, v), b++;
              }
            }
        }
        for (var z = c * 4 + 3, G = c * 2 + 1, F = z + 1, A = 0; A < G; A++)
          for (var p = 0; p <= z; p++) {
            var W = A * F + p + this._vertexOffset, X = A * F + (p + 1) % F + this._vertexOffset, q = (A + 1) * F + (p + 1) % F + this._vertexOffset, ie = (A + 1) * F + p + this._vertexOffset;
            this.setTriangleIndices(this._triangleOffset++, [q, W, X]), this.setTriangleIndices(this._triangleOffset++, [q, ie, W]);
          }
        this.setTriangleIndices(this._triangleOffset++, [w[0][0], w[0][2], w[0][1]]), this.setTriangleIndices(this._triangleOffset++, [w[0][0], w[0][3], w[0][2]]), this.setTriangleIndices(this._triangleOffset++, [w[1][0], w[1][1], w[1][2]]), this.setTriangleIndices(this._triangleOffset++, [w[1][0], w[1][2], w[1][3]]), this._vertexOffset = b;
      };
    })()
  }
);
_t(Gn.prototype, wi);
_t(Gn.prototype, zh);
function fi(e, t, r, i, n, a, o) {
  this._zr = e, this._x = 0, this._y = 0, this._rowHeight = 0, this.width = i, this.height = n, this.offsetX = t, this.offsetY = r, this.dpr = o, this.gap = a;
}
fi.prototype = {
  constructor: fi,
  clear: function() {
    this._x = 0, this._y = 0, this._rowHeight = 0;
  },
  /**
   * Add shape to atlas
   * @param {module:zrender/graphic/Displayable} shape
   * @param {number} width
   * @param {number} height
   * @return {Array}
   */
  add: function(e, t, r) {
    var i = e.getBoundingRect();
    t == null && (t = i.width), r == null && (r = i.height), t *= this.dpr, r *= this.dpr, this._fitElement(e, t, r);
    var n = this._x, a = this._y, o = this.width * this.dpr, s = this.height * this.dpr, l = this.gap;
    if (n + t + l > o && (n = this._x = 0, a += this._rowHeight + l, this._y = a, this._rowHeight = 0), this._x += t + l, this._rowHeight = Math.max(this._rowHeight, r), a + r + l > s)
      return null;
    e.x += this.offsetX * this.dpr + n, e.y += this.offsetY * this.dpr + a, this._zr.add(e);
    var u = [this.offsetX / this.width, this.offsetY / this.height], f = [[n / o + u[0], a / s + u[1]], [(n + t) / o + u[0], (a + r) / s + u[1]]];
    return f;
  },
  /**
   * Fit element size by correct its position and scaling
   * @param {module:zrender/graphic/Displayable} el
   * @param {number} spriteWidth
   * @param {number} spriteHeight
   */
  _fitElement: function(e, t, r) {
    var i = e.getBoundingRect(), n = t / i.width, a = r / i.height;
    e.x = -i.x * n, e.y = -i.y * a, e.scaleX = n, e.scaleY = a, e.update();
  }
};
function hi(e) {
  e = e || {}, e.width = e.width || 512, e.height = e.height || 512, e.devicePixelRatio = e.devicePixelRatio || 1, e.gap = e.gap == null ? 2 : e.gap;
  var t = document.createElement("canvas");
  t.width = e.width * e.devicePixelRatio, t.height = e.height * e.devicePixelRatio, this._canvas = t, this._texture = new K({
    image: t,
    flipY: !1
  });
  var r = this;
  this._zr = Il(t), Vo(this._zr, function() {
    r._texture.dirty(), r.onupdate && r.onupdate();
  }), this._dpr = e.devicePixelRatio, this._coords = {}, this.onupdate = e.onupdate, this._gap = e.gap, this._textureAtlasNodes = [new fi(this._zr, 0, 0, e.width, e.height, this._gap, this._dpr)], this._nodeWidth = e.width, this._nodeHeight = e.height, this._currentNodeIdx = 0;
}
hi.prototype = {
  /**
   * Clear the texture atlas
   */
  clear: function() {
    for (var e = 0; e < this._textureAtlasNodes.length; e++)
      this._textureAtlasNodes[e].clear();
    this._currentNodeIdx = 0, this._zr.clear(), this._coords = {};
  },
  /**
   * @return {number}
   */
  getWidth: function() {
    return this._width;
  },
  /**
   * @return {number}
   */
  getHeight: function() {
    return this._height;
  },
  /**
   * @return {number}
   */
  getTexture: function() {
    return this._texture;
  },
  /**
   * @return {number}
   */
  getDevicePixelRatio: function() {
    return this._dpr;
  },
  getZr: function() {
    return this._zr;
  },
  _getCurrentNode: function() {
    return this._textureAtlasNodes[this._currentNodeIdx];
  },
  _expand: function() {
    if (this._currentNodeIdx++, this._textureAtlasNodes[this._currentNodeIdx])
      return this._textureAtlasNodes[this._currentNodeIdx];
    var e = 4096 / this._dpr, t = this._textureAtlasNodes, r = t.length, i = r * this._nodeWidth % e, n = Math.floor(r * this._nodeWidth / e) * this._nodeHeight;
    if (!(n >= e)) {
      var a = (i + this._nodeWidth) * this._dpr, o = (n + this._nodeHeight) * this._dpr;
      try {
        this._zr.resize({
          width: a,
          height: o
        });
      } catch {
        this._canvas.width = a, this._canvas.height = o;
      }
      var s = new fi(this._zr, i, n, this._nodeWidth, this._nodeHeight, this._gap, this._dpr);
      return this._textureAtlasNodes.push(s), s;
    }
  },
  add: function(e, t, r) {
    if (this._coords[e.id])
      return this._coords[e.id];
    var i = this._getCurrentNode().add(e, t, r);
    if (!i) {
      var n = this._expand();
      if (!n)
        return;
      i = n.add(e, t, r);
    }
    return this._coords[e.id] = i, i;
  },
  /**
   * Get coord scale after texture atlas is expanded.
   * @return {Array.<number>}
   */
  getCoordsScale: function() {
    var e = this._dpr;
    return [this._nodeWidth / this._canvas.width * e, this._nodeHeight / this._canvas.height * e];
  },
  /**
   * Get texture coords of sprite image
   * @param  {string} id Image id
   * @return {Array}
   */
  getCoords: function(e) {
    return this._coords[e];
  },
  dispose: function() {
    this._zr.dispose();
  }
};
var to = [0, 1, 2, 0, 2, 3], rs = $.extend(function() {
  return {
    attributes: {
      position: new $.Attribute("position", "float", 3, "POSITION"),
      texcoord: new $.Attribute("texcoord", "float", 2, "TEXCOORD_0"),
      offset: new $.Attribute("offset", "float", 2),
      color: new $.Attribute("color", "float", 4, "COLOR")
    }
  };
}, {
  resetOffset: function() {
    this._vertexOffset = 0, this._faceOffset = 0;
  },
  setSpriteCount: function(e) {
    this._spriteCount = e;
    var t = e * 4, r = e * 2;
    this.vertexCount !== t && (this.attributes.position.init(t), this.attributes.offset.init(t), this.attributes.color.init(t)), this.triangleCount !== r && (this.indices = t > 65535 ? new Uint32Array(r * 3) : new Uint16Array(r * 3));
  },
  setSpriteAlign: function(e, t, r, i, n) {
    r == null && (r = "left"), i == null && (i = "top");
    var a, o, s, l;
    switch (n = n || 0, r) {
      case "left":
        a = n, s = t[0] + n;
        break;
      case "center":
      case "middle":
        a = -t[0] / 2, s = t[0] / 2;
        break;
      case "right":
        a = -t[0] - n, s = -n;
        break;
    }
    switch (i) {
      case "bottom":
        o = n, l = t[1] + n;
        break;
      case "middle":
        o = -t[1] / 2, l = t[1] / 2;
        break;
      case "top":
        o = -t[1] - n, l = -n;
        break;
    }
    var u = e * 4, f = this.attributes.offset;
    f.set(u, [a, l]), f.set(u + 1, [s, l]), f.set(u + 2, [s, o]), f.set(u + 3, [a, o]);
  },
  /**
   * Add sprite
   * @param {Array.<number>} position
   * @param {Array.<number>} size [width, height]
   * @param {Array.<Array>} coords [leftBottom, rightTop]
   * @param {string} [align='left'] 'left' 'center' 'right'
   * @param {string} [verticalAlign='top'] 'top' 'middle' 'bottom'
   * @param {number} [screenMargin=0]
   */
  addSprite: function(e, t, r, i, n, a) {
    var o = this._vertexOffset;
    this.setSprite(this._vertexOffset / 4, e, t, r, i, n, a);
    for (var s = 0; s < to.length; s++)
      this.indices[this._faceOffset * 3 + s] = to[s] + o;
    return this._faceOffset += 2, this._vertexOffset += 4, o / 4;
  },
  setSprite: function(e, t, r, i, n, a, o) {
    for (var s = e * 4, l = this.attributes, u = 0; u < 4; u++)
      l.position.set(s + u, t);
    var f = l.texcoord;
    f.set(s, [i[0][0], i[0][1]]), f.set(s + 1, [i[1][0], i[0][1]]), f.set(s + 2, [i[1][0], i[1][1]]), f.set(s + 3, [i[0][0], i[1][1]]), this.setSpriteAlign(e, r, n, a, o);
  }
});
_t(rs.prototype, wi);
const Gh = `@export ecgl.labels.vertex

attribute vec3 position: POSITION;
attribute vec2 texcoord: TEXCOORD_0;
attribute vec2 offset;
#ifdef VERTEX_COLOR
attribute vec4 a_Color : COLOR;
varying vec4 v_Color;
#endif

uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
uniform vec4 viewport : VIEWPORT;

varying vec2 v_Texcoord;

void main()
{
 vec4 proj = worldViewProjection * vec4(position, 1.0);

 vec2 screen = (proj.xy / abs(proj.w) + 1.0) * 0.5 * viewport.zw;

 screen += offset;

 proj.xy = (screen / viewport.zw - 0.5) * 2.0 * abs(proj.w);
 gl_Position = proj;
#ifdef VERTEX_COLOR
 v_Color = a_Color;
#endif
 v_Texcoord = texcoord;
}
@end


@export ecgl.labels.fragment

uniform vec3 color : [1.0, 1.0, 1.0];
uniform float alpha : 1.0;
uniform sampler2D textureAtlas;
uniform vec2 uvScale: [1.0, 1.0];

#ifdef VERTEX_COLOR
varying vec4 v_Color;
#endif
varying float v_Miter;

varying vec2 v_Texcoord;

void main()
{
 gl_FragColor = vec4(color, alpha) * texture2D(textureAtlas, v_Texcoord * uvScale);
#ifdef VERTEX_COLOR
 gl_FragColor *= v_Color;
#endif
}

@end`;
E.Shader.import(Gh);
const Vn = E.Mesh.extend(function() {
  var e = new rs({
    dynamic: !0
  }), t = new E.Material({
    shader: E.createShader("ecgl.labels"),
    transparent: !0,
    depthMask: !1
  });
  return {
    geometry: e,
    material: t,
    culling: !1,
    castShadow: !1,
    ignorePicking: !0
  };
});
var is = 1, ns = 2;
function yt(e, t, r) {
  this._labelsMesh = new Vn(), this._labelTextureSurface = new hi({
    width: 512,
    height: 512,
    devicePixelRatio: r.getDevicePixelRatio(),
    onupdate: function() {
      r.getZr().refresh();
    }
  }), this._api = r, this._labelsMesh.material.set("textureAtlas", this._labelTextureSurface.getTexture());
}
yt.prototype.getLabelPosition = function(e, t, r) {
  return [0, 0, 0];
};
yt.prototype.getLabelDistance = function(e, t, r) {
  return 0;
};
yt.prototype.getMesh = function() {
  return this._labelsMesh;
};
yt.prototype.updateData = function(e, t, r) {
  t == null && (t = 0), r == null && (r = e.count()), (!this._labelsVisibilitiesBits || this._labelsVisibilitiesBits.length !== r - t) && (this._labelsVisibilitiesBits = new Uint8Array(r - t));
  for (var i = ["label", "show"], n = ["emphasis", "label", "show"], a = t; a < r; a++) {
    var o = e.getItemModel(a), s = o.get(i), l = o.get(n);
    l == null && (l = s);
    var u = (s ? is : 0) | (l ? ns : 0);
    this._labelsVisibilitiesBits[a - t] = u;
  }
  this._start = t, this._end = r, this._data = e;
};
yt.prototype.updateLabels = function(e) {
  if (this._data) {
    e = e || [];
    for (var t = e.length > 0, r = {}, i = 0; i < e.length; i++)
      r[e[i]] = !0;
    this._labelsMesh.geometry.convertToDynamicArray(!0), this._labelTextureSurface.clear();
    for (var n = ["label"], a = ["emphasis", "label"], o = this._data.hostModel, s = this._data, l = o.getModel(n), u = o.getModel(a, l), f = {
      left: "right",
      right: "left",
      top: "center",
      bottom: "center"
    }, h = {
      left: "middle",
      right: "middle",
      top: "bottom",
      bottom: "top"
    }, d = this._start; d < this._end; d++) {
      var c = !1;
      t && r[d] && (c = !0);
      var v = this._labelsVisibilitiesBits[d - this._start] & (c ? ns : is);
      if (v) {
        var p = s.getItemModel(d), m = p.getModel(c ? a : n, c ? u : l), _ = m.get("distance") || 0, x = m.get("position"), y = this._api.getDevicePixelRatio(), g = o.getFormattedLabel(d, c ? "emphasis" : "normal");
        if (g == null || g === "")
          return;
        var b = new ni({
          style: ai(m, {
            text: g,
            fill: m.get("color") || ut(s, d) || "#000",
            align: "left",
            verticalAlign: "top",
            opacity: be.firstNotNull(m.get("opacity"), mt(s, d), 1)
          })
        }), w = b.getBoundingRect(), C = 1.2;
        w.height *= C;
        var A = this._labelTextureSurface.add(b), R = f[x] || "center", H = h[x] || "bottom";
        this._labelsMesh.geometry.addSprite(this.getLabelPosition(d, x, _), [w.width * y, w.height * y], A, R, H, this.getLabelDistance(d, x, _) * y);
      }
    }
    this._labelsMesh.material.set("uvScale", this._labelTextureSurface.getCoordsScale()), this._labelTextureSurface.getZr().refreshImmediately(), this._labelsMesh.geometry.convertToTypedArray(), this._labelsMesh.geometry.dirty();
  }
};
yt.prototype.dispose = function() {
  this._labelTextureSurface.dispose();
};
var Vh = Ye.vec3;
const kh = _i.extend({
  type: "bar3D",
  __ecgl__: !0,
  init: function(e, t) {
    this.groupGL = new E.Node(), this._api = t, this._labelsBuilder = new yt(256, 256, t);
    var r = this;
    this._labelsBuilder.getLabelPosition = function(i, n, a) {
      if (r._data) {
        var o = r._data.getItemLayout(i), s = o[0], l = o[1], u = o[2][1];
        return Vh.scaleAndAdd([], s, l, a + u);
      } else
        return [0, 0];
    }, this._labelsBuilder.getMesh().renderOrder = 100;
  },
  render: function(e, t, r) {
    var i = this._prevBarMesh;
    this._prevBarMesh = this._barMesh, this._barMesh = i, this._barMesh || (this._barMesh = new E.Mesh({
      geometry: new Gn(),
      shadowDepthMaterial: new E.Material({
        shader: new E.Shader(E.Shader.source("ecgl.sm.depth.vertex"), E.Shader.source("ecgl.sm.depth.fragment"))
      }),
      // Only cartesian3D enable culling
      // FIXME Performance
      culling: e.coordinateSystem.type === "cartesian3D",
      // Render after axes
      renderOrder: 10,
      // Render normal in normal pass
      renderNormal: !0
    })), this.groupGL.remove(this._prevBarMesh), this.groupGL.add(this._barMesh), this.groupGL.add(this._labelsBuilder.getMesh());
    var n = e.coordinateSystem;
    if (this._doRender(e, r), n && n.viewGL) {
      n.viewGL.add(this.groupGL);
      var a = n.viewGL.isLinearSpace() ? "define" : "undefine";
      this._barMesh.material[a]("fragment", "SRGB_DECODE");
    }
    this._data = e.getData(), this._labelsBuilder.updateData(this._data), this._labelsBuilder.updateLabels(), this._updateAnimation(e);
  },
  _updateAnimation: function(e) {
    E.updateVertexAnimation([["prevPosition", "position"], ["prevNormal", "normal"]], this._prevBarMesh, this._barMesh, e);
  },
  _doRender: function(e, t) {
    var r = e.getData(), i = e.get("shading"), n = i !== "color", a = this, o = this._barMesh, s = "ecgl." + i;
    (!o.material || o.material.shader.name !== s) && (o.material = E.createMaterial(s, ["VERTEX_COLOR"])), E.setMaterialFromModel(i, o.material, e, t), o.geometry.enableNormal = n, o.geometry.resetOffset();
    var l = e.get("bevelSize"), u = e.get("bevelSmoothness");
    o.geometry.bevelSegments = u, o.geometry.bevelSize = l;
    var f = [], h = new Float32Array(r.count() * 4), d = 0, m = 0, c = !1;
    r.each(function(x) {
      if (r.hasValue(x)) {
        var y = ut(r, x), g = mt(r, x);
        g == null && (g = 1), E.parseColor(y, f), f[3] *= g, h[d++] = f[0], h[d++] = f[1], h[d++] = f[2], h[d++] = f[3], f[3] > 0 && (m++, f[3] < 0.99 && (c = !0));
      }
    }), o.geometry.setBarCount(m);
    var v = r.getLayout("orient"), p = this._barIndexOfData = new Int32Array(r.count()), m = 0;
    r.each(function(x) {
      if (!r.hasValue(x)) {
        p[x] = -1;
        return;
      }
      var y = r.getItemLayout(x), g = y[0], b = y[1], w = y[2], C = x * 4;
      f[0] = h[C++], f[1] = h[C++], f[2] = h[C++], f[3] = h[C++], f[3] > 0 && (a._barMesh.geometry.addBar(g, b, v, w, f, x), p[x] = m++);
    }), o.geometry.dirty(), o.geometry.updateBoundingBox();
    var _ = o.material;
    _.transparent = c, _.depthMask = !c, o.geometry.sortTriangles = c, this._initHandler(e, t);
  },
  _initHandler: function(e, t) {
    var r = e.getData(), i = this._barMesh, n = e.coordinateSystem.type === "cartesian3D";
    i.seriesIndex = e.seriesIndex;
    var a = -1;
    i.off("mousemove"), i.off("mouseout"), i.on("mousemove", function(o) {
      var s = i.geometry.getDataIndexOfVertex(o.triangle[0]);
      s !== a && (this._downplay(a), this._highlight(s), this._labelsBuilder.updateLabels([s]), n && t.dispatchAction({
        type: "grid3DShowAxisPointer",
        value: [r.get("x", s), r.get("y", s), r.get("z", s, !0)]
      })), a = s, i.dataIndex = s;
    }, this), i.on("mouseout", function(o) {
      this._downplay(a), this._labelsBuilder.updateLabels(), a = -1, i.dataIndex = -1, n && t.dispatchAction({
        type: "grid3DHideAxisPointer"
      });
    }, this);
  },
  _highlight: function(e) {
    var t = this._data;
    if (t) {
      var r = this._barIndexOfData[e];
      if (!(r < 0)) {
        var i = t.getItemModel(e), n = i.getModel("emphasis.itemStyle"), a = n.get("color"), o = n.get("opacity");
        if (a == null) {
          var s = ut(t, e);
          a = So(s, -0.4);
        }
        o == null && (o = mt(t, e));
        var l = E.parseColor(a);
        l[3] *= o, this._barMesh.geometry.setColor(r, l), this._api.getZr().refresh();
      }
    }
  },
  _downplay: function(e) {
    var t = this._data;
    if (t) {
      var r = this._barIndexOfData[e];
      if (!(r < 0)) {
        var i = ut(t, e), n = mt(t, e), a = E.parseColor(i);
        a[3] *= n, this._barMesh.geometry.setColor(r, a), this._api.getZr().refresh();
      }
    }
  },
  highlight: function(e, t, r, i) {
    this._toggleStatus("highlight", e, t, r, i);
  },
  downplay: function(e, t, r, i) {
    this._toggleStatus("downplay", e, t, r, i);
  },
  _toggleStatus: function(e, t, r, i, n) {
    var a = t.getData(), o = be.queryDataIndex(a, n), s = this;
    o != null ? rt(Jt.normalizeToArray(o), function(l) {
      e === "highlight" ? this._highlight(l) : this._downplay(l);
    }, this) : a.each(function(l) {
      e === "highlight" ? s._highlight(l) : s._downplay(l);
    });
  },
  remove: function() {
    this.groupGL.removeAll();
  },
  dispose: function() {
    this._labelsBuilder.dispose(), this.groupGL.removeAll();
  }
});
function Wh(e) {
  e.registerChartView(kh), e.registerSeriesModel(es), Oh(e), e.registerProcessor(function(t, r) {
    t.eachSeriesByType("bar3d", function(i) {
      var n = i.getData();
      n.filterSelf(function(a) {
        return n.hasValue(a);
      });
    });
  });
}
var Xh = gi.extend({
  type: "series.line3D",
  dependencies: ["grid3D"],
  visualStyleAccessPath: "lineStyle",
  visualDrawType: "stroke",
  getInitialData: function(e, t) {
    return Hn(this);
  },
  formatTooltip: function(e) {
    return zn(this, e);
  },
  defaultOption: {
    coordinateSystem: "cartesian3D",
    zlevel: -10,
    // Cartesian coordinate system
    grid3DIndex: 0,
    lineStyle: {
      width: 2
    },
    animationDurationUpdate: 500
  }
}), rn = Ye.vec3, ro = [[0, 0], [1, 1]], Cr = $.extend(
  function() {
    return {
      segmentScale: 1,
      dynamic: !0,
      /**
       * Need to use mesh to expand lines if lineWidth > MAX_LINE_WIDTH
       */
      useNativeLine: !0,
      attributes: {
        position: new $.Attribute("position", "float", 3, "POSITION"),
        positionPrev: new $.Attribute("positionPrev", "float", 3),
        positionNext: new $.Attribute("positionNext", "float", 3),
        prevPositionPrev: new $.Attribute("prevPositionPrev", "float", 3),
        prevPosition: new $.Attribute("prevPosition", "float", 3),
        prevPositionNext: new $.Attribute("prevPositionNext", "float", 3),
        offset: new $.Attribute("offset", "float", 1),
        color: new $.Attribute("color", "float", 4, "COLOR")
      }
    };
  },
  /** @lends module: echarts-gl/util/geometry/LinesGeometry.prototype */
  {
    /**
     * Reset offset
     */
    resetOffset: function() {
      this._vertexOffset = 0, this._triangleOffset = 0, this._itemVertexOffsets = [];
    },
    /**
     * @param {number} nVertex
     */
    setVertexCount: function(e) {
      var t = this.attributes;
      this.vertexCount !== e && (t.position.init(e), t.color.init(e), this.useNativeLine || (t.positionPrev.init(e), t.positionNext.init(e), t.offset.init(e)), e > 65535 ? this.indices instanceof Uint16Array && (this.indices = new Uint32Array(this.indices)) : this.indices instanceof Uint32Array && (this.indices = new Uint16Array(this.indices)));
    },
    /**
     * @param {number} nTriangle
     */
    setTriangleCount: function(e) {
      this.triangleCount !== e && (e === 0 ? this.indices = null : this.indices = this.vertexCount > 65535 ? new Uint32Array(e * 3) : new Uint16Array(e * 3));
    },
    _getCubicCurveApproxStep: function(e, t, r, i) {
      var n = rn.dist(e, t) + rn.dist(r, t) + rn.dist(i, r), a = 1 / (n + 1) * this.segmentScale;
      return a;
    },
    /**
     * Get vertex count of cubic curve
     * @param {Array.<number>} p0
     * @param {Array.<number>} p1
     * @param {Array.<number>} p2
     * @param {Array.<number>} p3
     * @return number
     */
    getCubicCurveVertexCount: function(e, t, r, i) {
      var n = this._getCubicCurveApproxStep(e, t, r, i), a = Math.ceil(1 / n);
      return this.useNativeLine ? a * 2 : a * 2 + 2;
    },
    /**
     * Get face count of cubic curve
     * @param {Array.<number>} p0
     * @param {Array.<number>} p1
     * @param {Array.<number>} p2
     * @param {Array.<number>} p3
     * @return number
     */
    getCubicCurveTriangleCount: function(e, t, r, i) {
      var n = this._getCubicCurveApproxStep(e, t, r, i), a = Math.ceil(1 / n);
      return this.useNativeLine ? 0 : a * 2;
    },
    /**
     * Get vertex count of line
     * @return {number}
     */
    getLineVertexCount: function() {
      return this.getPolylineVertexCount(ro);
    },
    /**
     * Get face count of line
     * @return {number}
     */
    getLineTriangleCount: function() {
      return this.getPolylineTriangleCount(ro);
    },
    /**
     * Get how many vertices will polyline take.
     * @type {number|Array} points Can be a 1d/2d list of points, or a number of points amount.
     * @return {number}
     */
    getPolylineVertexCount: function(e) {
      var t;
      if (typeof e == "number")
        t = e;
      else {
        var r = typeof e[0] != "number";
        t = r ? e.length : e.length / 3;
      }
      return this.useNativeLine ? (t - 1) * 2 : (t - 1) * 2 + 2;
    },
    /**
     * Get how many triangles will polyline take.
     * @type {number|Array} points Can be a 1d/2d list of points, or a number of points amount.
     * @return {number}
     */
    getPolylineTriangleCount: function(e) {
      var t;
      if (typeof e == "number")
        t = e;
      else {
        var r = typeof e[0] != "number";
        t = r ? e.length : e.length / 3;
      }
      return this.useNativeLine ? 0 : Math.max(t - 1, 0) * 2;
    },
    /**
     * Add a cubic curve
     * @param {Array.<number>} p0
     * @param {Array.<number>} p1
     * @param {Array.<number>} p2
     * @param {Array.<number>} p3
     * @param {Array.<number>} color
     * @param {number} [lineWidth=1]
     */
    addCubicCurve: function(e, t, r, i, n, a) {
      a == null && (a = 1);
      for (var o = e[0], s = e[1], l = e[2], u = t[0], f = t[1], h = t[2], d = r[0], c = r[1], v = r[2], p = i[0], m = i[1], _ = i[2], x = this._getCubicCurveApproxStep(e, t, r, i), y = x * x, g = y * x, b = 3 * x, w = 3 * y, C = 6 * y, A = 6 * g, R = o - u * 2 + d, H = s - f * 2 + c, O = l - h * 2 + v, z = (u - d) * 3 - o + p, G = (f - c) * 3 - s + m, F = (h - v) * 3 - l + _, W = o, X = s, q = l, ie = (u - o) * b + R * w + z * g, me = (f - s) * b + H * w + G * g, Se = (h - l) * b + O * w + F * g, ce = R * C + z * A, ye = H * C + G * A, de = O * C + F * A, Ce = z * A, Fe = G * A, Ie = F * A, tr = 0, Ke = 0, Ot = Math.ceil(1 / x), rr = new Float32Array((Ot + 1) * 3), rr = [], Si = 0, Ke = 0; Ke < Ot + 1; Ke++)
        rr[Si++] = W, rr[Si++] = X, rr[Si++] = q, W += ie, X += me, q += Se, ie += ce, me += ye, Se += de, ce += Ce, ye += Fe, de += Ie, tr += x, tr > 1 && (W = ie > 0 ? Math.min(W, p) : Math.max(W, p), X = me > 0 ? Math.min(X, m) : Math.max(X, m), q = Se > 0 ? Math.min(q, _) : Math.max(q, _));
      return this.addPolyline(rr, n, a);
    },
    /**
     * Add a straight line
     * @param {Array.<number>} p0
     * @param {Array.<number>} p1
     * @param {Array.<number>} color
     * @param {number} [lineWidth=1]
     */
    addLine: function(e, t, r, i) {
      return this.addPolyline([e, t], r, i);
    },
    /**
     * Add a straight line
     * @param {Array.<Array> | Array.<number>} points
     * @param {Array.<number> | Array.<Array>} color
     * @param {number} [lineWidth=1]
     * @param {number} [startOffset=0]
     * @param {number} [pointsCount] Default to be amount of points in the first argument
     */
    addPolyline: function(e, t, r, i, n) {
      if (e.length) {
        var a = typeof e[0] != "number";
        if (n == null && (n = a ? e.length : e.length / 3), !(n < 2)) {
          i == null && (i = 0), r == null && (r = 1), this._itemVertexOffsets.push(this._vertexOffset);
          var a = typeof e[0] != "number", o = a ? typeof t[0] != "number" : t.length / 4 === n, s = this.attributes.position, l = this.attributes.positionPrev, u = this.attributes.positionNext, f = this.attributes.color, h = this.attributes.offset, d = this.indices, c = this._vertexOffset, v, p;
          r = Math.max(r, 0.01);
          for (var m = i; m < n; m++) {
            if (a)
              v = e[m], o ? p = t[m] : p = t;
            else {
              var _ = m * 3;
              if (v = v || [], v[0] = e[_], v[1] = e[_ + 1], v[2] = e[_ + 2], o) {
                var x = m * 4;
                p = p || [], p[0] = t[x], p[1] = t[x + 1], p[2] = t[x + 2], p[3] = t[x + 3];
              } else
                p = t;
            }
            if (this.useNativeLine ? m > 1 && (s.copy(c, c - 1), f.copy(c, c - 1), c++) : (m < n - 1 && (l.set(c + 2, v), l.set(c + 3, v)), m > 0 && (u.set(c - 2, v), u.set(c - 1, v)), s.set(c, v), s.set(c + 1, v), f.set(c, p), f.set(c + 1, p), h.set(c, r / 2), h.set(c + 1, -r / 2), c += 2), this.useNativeLine)
              f.set(c, p), s.set(c, v), c++;
            else if (m > 0) {
              var y = this._triangleOffset * 3, d = this.indices;
              d[y] = c - 4, d[y + 1] = c - 3, d[y + 2] = c - 2, d[y + 3] = c - 3, d[y + 4] = c - 1, d[y + 5] = c - 2, this._triangleOffset += 2;
            }
          }
          if (!this.useNativeLine) {
            var g = this._vertexOffset, b = this._vertexOffset + n * 2;
            l.copy(g, g + 2), l.copy(g + 1, g + 3), u.copy(b - 1, b - 3), u.copy(b - 2, b - 4);
          }
          return this._vertexOffset = c, this._vertexOffset;
        }
      }
    },
    /**
     * Set color of single line.
     */
    setItemColor: function(e, t) {
      for (var r = this._itemVertexOffsets[e], i = e < this._itemVertexOffsets.length - 1 ? this._itemVertexOffsets[e + 1] : this._vertexOffset, n = r; n < i; n++)
        this.attributes.color.set(n, t);
      this.dirty("color");
    },
    /**
     * @return {number}
     */
    currentTriangleOffset: function() {
      return this._triangleOffset;
    },
    /**
     * @return {number}
     */
    currentVertexOffset: function() {
      return this._vertexOffset;
    }
  }
);
_t(Cr.prototype, wi);
function qh(e, t, r, i, n, a, o) {
  if (n === 0)
    return !1;
  var s = n, l = 0, u = e;
  if (o > t + s && o > i + s || o < t - s && o < i - s || a > e + s && a > r + s || a < e - s && a < r - s)
    return !1;
  if (e !== r)
    l = (t - i) / (e - r), u = (e * i - r * t) / (e - r);
  else
    return Math.abs(a - e) <= s / 2;
  var f = l * a - o + u, h = f * f / (l * l + 1);
  return h <= s / 2 * s / 2;
}
const as = `@export ecgl.lines3D.vertex

uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;

attribute vec3 position: POSITION;
attribute vec4 a_Color : COLOR;
varying vec4 v_Color;

void main()
{
 gl_Position = worldViewProjection * vec4(position, 1.0);
 v_Color = a_Color;
}

@end

@export ecgl.lines3D.fragment

uniform vec4 color : [1.0, 1.0, 1.0, 1.0];

varying vec4 v_Color;

@import clay.util.srgb

void main()
{
#ifdef SRGB_DECODE
 gl_FragColor = sRGBToLinear(color * v_Color);
#else
 gl_FragColor = color * v_Color;
#endif
}
@end



@export ecgl.lines3D.clipNear

vec4 clipNear(vec4 p1, vec4 p2) {
 float n = (p1.w - near) / (p1.w - p2.w);
 return vec4(mix(p1.xy, p2.xy, n), -near, near);
}

@end

@export ecgl.lines3D.expandLine
#ifdef VERTEX_ANIMATION
 vec4 prevProj = worldViewProjection * vec4(mix(prevPositionPrev, positionPrev, percent), 1.0);
 vec4 currProj = worldViewProjection * vec4(mix(prevPosition, position, percent), 1.0);
 vec4 nextProj = worldViewProjection * vec4(mix(prevPositionNext, positionNext, percent), 1.0);
#else
 vec4 prevProj = worldViewProjection * vec4(positionPrev, 1.0);
 vec4 currProj = worldViewProjection * vec4(position, 1.0);
 vec4 nextProj = worldViewProjection * vec4(positionNext, 1.0);
#endif

 if (currProj.w < 0.0) {
 if (nextProj.w > 0.0) {
 currProj = clipNear(currProj, nextProj);
 }
 else if (prevProj.w > 0.0) {
 currProj = clipNear(currProj, prevProj);
 }
 }

 vec2 prevScreen = (prevProj.xy / abs(prevProj.w) + 1.0) * 0.5 * viewport.zw;
 vec2 currScreen = (currProj.xy / abs(currProj.w) + 1.0) * 0.5 * viewport.zw;
 vec2 nextScreen = (nextProj.xy / abs(nextProj.w) + 1.0) * 0.5 * viewport.zw;

 vec2 dir;
 float len = offset;
 if (position == positionPrev) {
 dir = normalize(nextScreen - currScreen);
 }
 else if (position == positionNext) {
 dir = normalize(currScreen - prevScreen);
 }
 else {
 vec2 dirA = normalize(currScreen - prevScreen);
 vec2 dirB = normalize(nextScreen - currScreen);

 vec2 tanget = normalize(dirA + dirB);

 float miter = 1.0 / max(dot(tanget, dirA), 0.5);
 len *= miter;
 dir = tanget;
 }

 dir = vec2(-dir.y, dir.x) * len;
 currScreen += dir;

 currProj.xy = (currScreen / viewport.zw - 0.5) * 2.0 * abs(currProj.w);
@end


@export ecgl.meshLines3D.vertex

attribute vec3 position: POSITION;
attribute vec3 positionPrev;
attribute vec3 positionNext;
attribute float offset;
attribute vec4 a_Color : COLOR;

#ifdef VERTEX_ANIMATION
attribute vec3 prevPosition;
attribute vec3 prevPositionPrev;
attribute vec3 prevPositionNext;
uniform float percent : 1.0;
#endif

uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
uniform vec4 viewport : VIEWPORT;
uniform float near : NEAR;

varying vec4 v_Color;

@import ecgl.common.wireframe.vertexHeader

@import ecgl.lines3D.clipNear

void main()
{
 @import ecgl.lines3D.expandLine

 gl_Position = currProj;

 v_Color = a_Color;

 @import ecgl.common.wireframe.vertexMain
}
@end


@export ecgl.meshLines3D.fragment

uniform vec4 color : [1.0, 1.0, 1.0, 1.0];

varying vec4 v_Color;

@import ecgl.common.wireframe.fragmentHeader

@import clay.util.srgb

void main()
{
#ifdef SRGB_DECODE
 gl_FragColor = sRGBToLinear(color * v_Color);
#else
 gl_FragColor = color * v_Color;
#endif

 @import ecgl.common.wireframe.fragmentMain
}

@end`;
var jh = Ye.vec3;
E.Shader.import(as);
const Zh = _i.extend({
  type: "line3D",
  __ecgl__: !0,
  init: function(e, t) {
    this.groupGL = new E.Node(), this._api = t;
  },
  render: function(e, t, r) {
    var i = this._prevLine3DMesh;
    this._prevLine3DMesh = this._line3DMesh, this._line3DMesh = i, this._line3DMesh || (this._line3DMesh = new E.Mesh({
      geometry: new Cr({
        useNativeLine: !1,
        sortTriangles: !0
      }),
      material: new E.Material({
        shader: E.createShader("ecgl.meshLines3D")
      }),
      // Render after axes
      renderOrder: 10
    }), this._line3DMesh.geometry.pick = this._pick.bind(this)), this.groupGL.remove(this._prevLine3DMesh), this.groupGL.add(this._line3DMesh);
    var n = e.coordinateSystem;
    if (n && n.viewGL) {
      n.viewGL.add(this.groupGL);
      var a = n.viewGL.isLinearSpace() ? "define" : "undefine";
      this._line3DMesh.material[a]("fragment", "SRGB_DECODE");
    }
    this._doRender(e, r), this._data = e.getData(), this._camera = n.viewGL.camera, this.updateCamera(), this._updateAnimation(e);
  },
  updateCamera: function() {
    this._updateNDCPosition();
  },
  _doRender: function(e, t) {
    var r = e.getData(), i = this._line3DMesh;
    i.geometry.resetOffset();
    var n = r.getLayout("points"), a = [], o = new Float32Array(n.length / 3 * 4), s = 0, l = !1;
    r.each(function(h) {
      var d = ut(r, h), c = mt(r, h);
      c == null && (c = 1), E.parseColor(d, a), a[3] *= c, o[s++] = a[0], o[s++] = a[1], o[s++] = a[2], o[s++] = a[3], a[3] < 0.99 && (l = !0);
    }), i.geometry.setVertexCount(i.geometry.getPolylineVertexCount(n)), i.geometry.setTriangleCount(i.geometry.getPolylineTriangleCount(n)), i.geometry.addPolyline(n, o, be.firstNotNull(e.get("lineStyle.width"), 1)), i.geometry.dirty(), i.geometry.updateBoundingBox();
    var u = i.material;
    u.transparent = l, u.depthMask = !l;
    var f = e.getModel("debug.wireframe");
    f.get("show") ? (i.geometry.createAttribute("barycentric", "float", 3), i.geometry.generateBarycentric(), i.material.set("both", "WIREFRAME_TRIANGLE"), i.material.set("wireframeLineColor", E.parseColor(f.get("lineStyle.color") || "rgba(0,0,0,0.5)")), i.material.set("wireframeLineWidth", be.firstNotNull(f.get("lineStyle.width"), 1))) : i.material.set("both", "WIREFRAME_TRIANGLE"), this._points = n, this._initHandler(e, t);
  },
  _updateAnimation: function(e) {
    E.updateVertexAnimation([["prevPosition", "position"], ["prevPositionPrev", "positionPrev"], ["prevPositionNext", "positionNext"]], this._prevLine3DMesh, this._line3DMesh, e);
  },
  _initHandler: function(e, t) {
    var r = e.getData(), i = e.coordinateSystem, n = this._line3DMesh, a = -1;
    n.seriesIndex = e.seriesIndex, n.off("mousemove"), n.off("mouseout"), n.on("mousemove", function(o) {
      var s = i.pointToData(o.point.array), l;
      e.indicesOfNearest ? l = e.indicesOfNearest("x", "x", s[0])[0] : l = r.indicesOfNearest("x", s[0])[0], l !== a && (t.dispatchAction({
        type: "grid3DShowAxisPointer",
        value: [r.get("x", l), r.get("y", l), r.get("z", l)]
      }), n.dataIndex = l), a = l;
    }, this), n.on("mouseout", function(o) {
      a = -1, n.dataIndex = -1, t.dispatchAction({
        type: "grid3DHideAxisPointer"
      });
    }, this);
  },
  // _highlight: function (dataIndex) {
  //     var data = this._data;
  //     if (!data) {
  //         return;
  //     }
  // },
  // _downplay: function (dataIndex) {
  //     var data = this._data;
  //     if (!data) {
  //         return;
  //     }
  // },
  _updateNDCPosition: function() {
    var e = new I(), t = this._camera;
    I.multiply(e, t.projectionMatrix, t.viewMatrix);
    var r = this._positionNDC, i = this._points, n = i.length / 3;
    (!r || r.length / 2 !== n) && (r = this._positionNDC = new Float32Array(n * 2));
    for (var a = [], o = 0; o < n; o++) {
      var s = o * 3, l = o * 2;
      a[0] = i[s], a[1] = i[s + 1], a[2] = i[s + 2], a[3] = 1, jh.transformMat4(a, a, e.array), r[l] = a[0] / a[3], r[l + 1] = a[1] / a[3];
    }
  },
  _pick: function(e, t, r, i, n, a) {
    var o = this._positionNDC, s = this._data.hostModel, l = s.get("lineStyle.width"), u = -1, f = r.viewport.width, h = r.viewport.height, d = f * 0.5, c = h * 0.5;
    e = (e + 1) * d, t = (t + 1) * c;
    for (var v = 1; v < o.length / 2; v++) {
      var p = (o[(v - 1) * 2] + 1) * d, m = (o[(v - 1) * 2 + 1] + 1) * c, _ = (o[v * 2] + 1) * d, x = (o[v * 2 + 1] + 1) * c;
      if (qh(p, m, _, x, l, e, t)) {
        var y = (p - e) * (p - e) + (m - t) * (m - t), g = (_ - e) * (_ - e) + (x - t) * (x - t);
        u = y < g ? v - 1 : v;
      }
    }
    if (u >= 0) {
      var b = u * 3, w = new S(this._points[b], this._points[b + 1], this._points[b + 2]);
      a.push({
        dataIndex: u,
        point: w,
        pointWorld: w.clone(),
        target: this._line3DMesh,
        distance: this._camera.getWorldPosition().dist(w)
      });
    }
  },
  remove: function() {
    this.groupGL.removeAll();
  },
  dispose: function() {
    this.groupGL.removeAll();
  }
});
function Yh(e) {
  e.registerChartView(Zh), e.registerSeriesModel(Xh), e.registerLayout(function(t, r) {
    t.eachSeriesByType("line3D", function(i) {
      var n = i.getData(), a = i.coordinateSystem;
      if (a) {
        if (a.type !== "cartesian3D")
          return;
        var o = new Float32Array(n.count() * 3), s = [], l = [], u = a.dimensions, f = u.map(function(h) {
          return i.coordDimToDataDim(h)[0];
        });
        a && n.each(f, function(h, d, c, v) {
          s[0] = h, s[1] = d, s[2] = c, a.dataToPoint(s, l), o[v * 3] = l[0], o[v * 3 + 1] = l[1], o[v * 3 + 2] = l[2];
        }), n.setLayout("points", o);
      }
    });
  });
}
const Kh = gi.extend({
  type: "series.scatter3D",
  dependencies: ["globe", "grid3D", "geo3D"],
  visualStyleAccessPath: "itemStyle",
  hasSymbolVisual: !0,
  getInitialData: function(e, t) {
    return Hn(this);
  },
  getFormattedLabel: function(e, t, r, i) {
    var n = Jt.getFormattedLabel(this, e, t, r, i);
    if (n == null) {
      var a = this.getData(), o = a.dimensions[a.dimensions.length - 1];
      n = a.get(o, e);
    }
    return n;
  },
  formatTooltip: function(e) {
    return zn(this, e);
  },
  defaultOption: {
    coordinateSystem: "cartesian3D",
    zlevel: -10,
    progressive: 1e5,
    progressiveThreshold: 1e5,
    // Cartesian coordinate system
    grid3DIndex: 0,
    globeIndex: 0,
    symbol: "circle",
    symbolSize: 10,
    // Support source-over, lighter
    blendMode: "source-over",
    label: {
      show: !1,
      position: "right",
      // Screen space distance
      distance: 5,
      textStyle: {
        fontSize: 14,
        color: "#000",
        backgroundColor: "rgba(255,255,255,0.7)",
        padding: 3,
        borderRadius: 3
      }
    },
    itemStyle: {
      opacity: 0.8
    },
    emphasis: {
      label: {
        show: !0
      }
    },
    animationDurationUpdate: 500
  }
});
function nn(e, i, r) {
  var i = i || document.createElement("canvas");
  i.width = e, i.height = e;
  var n = i.getContext("2d");
  return r && r(n), i;
}
function Qh(e, t, r, i) {
  it(t) || (t = [t, t]);
  var n = ci.getMarginByStyle(r), a = t[0] + n.left + n.right, o = t[1] + n.top + n.bottom, s = Bl(e, 0, 0, t[0], t[1]), l = Math.max(a, o);
  s.x = n.left, s.y = n.top, a > o ? s.y += (l - o) / 2 : s.x += (l - a) / 2;
  var u = s.getBoundingRect();
  return s.x -= u.x, s.y -= u.y, s.setStyle(r), s.update(), s.__size = l, s;
}
function $h(e, t, r) {
  var i = t.width, n = t.height, a = e.canvas.width, o = e.canvas.height, s = i / a, l = n / o;
  function u(_) {
    return _ < 128 ? 1 : -1;
  }
  function f(_, x) {
    var y = 1 / 0;
    _ = Math.floor(_ * s), x = Math.floor(x * l);
    for (var g = x * i + _, b = t.data[g * 4], w = u(b), C = Math.max(x - r, 0); C < Math.min(x + r, n); C++)
      for (var A = Math.max(_ - r, 0); A < Math.min(_ + r, i); A++) {
        var g = C * i + A, R = t.data[g * 4], H = u(R), O = A - _, z = C - x;
        if (w !== H) {
          var G = O * O + z * z;
          G < y && (y = G);
        }
      }
    return w * Math.sqrt(y);
  }
  for (var h = e.createImageData(a, o), d = 0; d < o; d++)
    for (var c = 0; c < a; c++) {
      var v = f(c, d), p = v / r * 0.5 + 0.5, m = (d * a + c) * 4;
      h.data[m++] = (1 - p) * 255, h.data[m++] = (1 - p) * 255, h.data[m++] = (1 - p) * 255, h.data[m++] = 255;
    }
  return h;
}
var ci = {
  getMarginByStyle: function(e) {
    var t = e.minMargin || 0, r = 0;
    e.stroke && e.stroke !== "none" && (r = e.lineWidth == null ? 1 : e.lineWidth);
    var i = e.shadowBlur || 0, n = e.shadowOffsetX || 0, a = e.shadowOffsetY || 0, o = {};
    return o.left = Math.max(r / 2, -n + i, t), o.right = Math.max(r / 2, n + i, t), o.top = Math.max(r / 2, -a + i, t), o.bottom = Math.max(r / 2, a + i, t), o;
  },
  // TODO Not consider shadowOffsetX, shadowOffsetY.
  /**
   * @param {string} symbol
   * @param {number | Array.<number>} symbolSize
   * @param {Object} style
   */
  createSymbolSprite: function(e, t, r, i) {
    var n = Qh(e, t, r), a = ci.getMarginByStyle(r);
    return {
      image: nn(n.__size, i, function(o) {
        Dn(o, n);
      }),
      margin: a
    };
  },
  createSDFFromCanvas: function(e, t, r, i) {
    return nn(t, i, function(n) {
      var a = e.getContext("2d"), o = a.getImageData(0, 0, e.width, e.height);
      n.putImageData($h(n, o, r), 0, 0);
    });
  },
  createSimpleSprite: function(e, t) {
    return nn(e, t, function(r) {
      var i = e / 2;
      r.beginPath(), r.arc(i, i, 60, 0, Math.PI * 2, !1), r.closePath();
      var n = r.createRadialGradient(i, i, 0, i, i, i);
      n.addColorStop(0, "rgba(255, 255, 255, 1)"), n.addColorStop(0.5, "rgba(255, 255, 255, 0.5)"), n.addColorStop(1, "rgba(255, 255, 255, 0)"), r.fillStyle = n, r.fill();
    });
  }
}, io = Ye.vec3;
const Jh = {
  needsSortVertices: function() {
    return this.sortVertices;
  },
  needsSortVerticesProgressively: function() {
    return this.needsSortVertices() && this.vertexCount >= 2e4;
  },
  doSortVertices: function(e, t) {
    var r = this.indices, i = io.create();
    if (!r) {
      r = this.indices = this.vertexCount > 65535 ? new Uint32Array(this.vertexCount) : new Uint16Array(this.vertexCount);
      for (var n = 0; n < r.length; n++)
        r[n] = n;
    }
    if (t === 0) {
      var a = this.attributes.position, e = e.array, o = 0;
      (!this._zList || this._zList.length !== this.vertexCount) && (this._zList = new Float32Array(this.vertexCount));
      for (var s, n = 0; n < this.vertexCount; n++) {
        a.get(n, i);
        var l = io.sqrDist(i, e);
        isNaN(l) && (l = 1e7, o++), n === 0 ? (s = l, l = 0) : l = l - s, this._zList[n] = l;
      }
      this._noneCount = o;
    }
    if (this.vertexCount < 2e4)
      t === 0 && this._simpleSort(this._noneCount / this.vertexCount > 0.05);
    else
      for (var n = 0; n < 3; n++)
        this._progressiveQuickSort(t * 3 + n);
    this.dirtyIndices();
  },
  _simpleSort: function(e) {
    var t = this._zList, r = this.indices;
    function i(n, a) {
      return t[a] - t[n];
    }
    e ? Array.prototype.sort.call(r, i) : Qt.sort(r, i, 0, r.length - 1);
  },
  _progressiveQuickSort: function(e) {
    var t = this._zList, r = this.indices;
    this._quickSort = this._quickSort || new Qt(), this._quickSort.step(r, function(i, n) {
      return t[n] - t[i];
    }, e);
  }
}, ec = `@export ecgl.sdfSprite.vertex

uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
uniform float elapsedTime : 0;

attribute vec3 position : POSITION;

#ifdef VERTEX_SIZE
attribute float size;
#else
uniform float u_Size;
#endif

#ifdef VERTEX_COLOR
attribute vec4 a_FillColor: COLOR;
varying vec4 v_Color;
#endif

#ifdef VERTEX_ANIMATION
attribute vec3 prevPosition;
attribute float prevSize;
uniform float percent : 1.0;
#endif


#ifdef POSITIONTEXTURE_ENABLED
uniform sampler2D positionTexture;
#endif

varying float v_Size;

void main()
{

#ifdef POSITIONTEXTURE_ENABLED
 gl_Position = worldViewProjection * vec4(texture2D(positionTexture, position.xy).xy, -10.0, 1.0);
#else

 #ifdef VERTEX_ANIMATION
 vec3 pos = mix(prevPosition, position, percent);
 #else
 vec3 pos = position;
 #endif
 gl_Position = worldViewProjection * vec4(pos, 1.0);
#endif

#ifdef VERTEX_SIZE
#ifdef VERTEX_ANIMATION
 v_Size = mix(prevSize, size, percent);
#else
 v_Size = size;
#endif
#else
 v_Size = u_Size;
#endif

#ifdef VERTEX_COLOR
 v_Color = a_FillColor;
 #endif

 gl_PointSize = v_Size;
}

@end

@export ecgl.sdfSprite.fragment

uniform vec4 color: [1, 1, 1, 1];
uniform vec4 strokeColor: [1, 1, 1, 1];
uniform float smoothing: 0.07;

uniform float lineWidth: 0.0;

#ifdef VERTEX_COLOR
varying vec4 v_Color;
#endif

varying float v_Size;

uniform sampler2D sprite;

@import clay.util.srgb

void main()
{
 gl_FragColor = color;

 vec4 _strokeColor = strokeColor;

#ifdef VERTEX_COLOR
 gl_FragColor *= v_Color;
 #endif

#ifdef SPRITE_ENABLED
 float d = texture2D(sprite, gl_PointCoord).r;
 gl_FragColor.a *= smoothstep(0.5 - smoothing, 0.5 + smoothing, d);

 if (lineWidth > 0.0) {
 float sLineWidth = lineWidth / 2.0;

 float outlineMaxValue0 = 0.5 + sLineWidth;
 float outlineMaxValue1 = 0.5 + sLineWidth + smoothing;
 float outlineMinValue0 = 0.5 - sLineWidth - smoothing;
 float outlineMinValue1 = 0.5 - sLineWidth;

 if (d <= outlineMaxValue1 && d >= outlineMinValue0) {
 float a = _strokeColor.a;
 if (d <= outlineMinValue1) {
 a = a * smoothstep(outlineMinValue0, outlineMinValue1, d);
 }
 else {
 a = a * smoothstep(outlineMaxValue1, outlineMaxValue0, d);
 }
 gl_FragColor.rgb = mix(gl_FragColor.rgb * gl_FragColor.a, _strokeColor.rgb, a);
 gl_FragColor.a = gl_FragColor.a * (1.0 - a) + a;
 }
 }
#endif

#ifdef SRGB_DECODE
 gl_FragColor = sRGBToLinear(gl_FragColor);
#endif
}
@end`;
var an = Ye.vec4;
E.Shader.import(ec);
var tc = E.Mesh.extend(function() {
  var e = new E.Geometry({
    dynamic: !0,
    attributes: {
      color: new E.Geometry.Attribute("color", "float", 4, "COLOR"),
      position: new E.Geometry.Attribute("position", "float", 3, "POSITION"),
      size: new E.Geometry.Attribute("size", "float", 1),
      prevPosition: new E.Geometry.Attribute("prevPosition", "float", 3),
      prevSize: new E.Geometry.Attribute("prevSize", "float", 1)
    }
  });
  Object.assign(e, Jh);
  var t = new E.Material({
    shader: E.createShader("ecgl.sdfSprite"),
    transparent: !0,
    depthMask: !1
  });
  t.enableTexture("sprite"), t.define("both", "VERTEX_COLOR"), t.define("both", "VERTEX_SIZE");
  var r = new E.Texture2D({
    image: document.createElement("canvas"),
    flipY: !1
  });
  return t.set("sprite", r), e.pick = this._pick.bind(this), {
    geometry: e,
    material: t,
    mode: E.Mesh.POINTS,
    sizeScale: 1
  };
}, {
  _pick: function(e, t, r, i, n, a) {
    var o = this._positionNDC;
    if (o)
      for (var s = r.viewport, l = 2 / s.width, u = 2 / s.height, f = this.geometry.vertexCount - 1; f >= 0; f--) {
        var h;
        this.geometry.indices ? h = this.geometry.indices[f] : h = f;
        var d = o[h * 2], c = o[h * 2 + 1], v = this.geometry.attributes.size.get(h) / this.sizeScale, p = v / 2;
        if (e > d - p * l && e < d + p * l && t > c - p * u && t < c + p * u) {
          var m = new E.Vector3(), _ = new E.Vector3();
          this.geometry.attributes.position.get(h, m.array), E.Vector3.transformMat4(_, m, this.worldTransform), a.push({
            vertexIndex: h,
            point: m,
            pointWorld: _,
            target: this,
            distance: _.distance(i.getWorldPosition())
          });
        }
      }
  },
  updateNDCPosition: function(e, t, r) {
    var i = this._positionNDC, n = this.geometry;
    (!i || i.length / 2 !== n.vertexCount) && (i = this._positionNDC = new Float32Array(n.vertexCount * 2));
    for (var a = an.create(), o = 0; o < n.vertexCount; o++)
      n.attributes.position.get(o, a), a[3] = 1, an.transformMat4(a, a, e.array), an.scale(a, a, 1 / a[3]), i[o * 2] = a[0], i[o * 2 + 1] = a[1];
  }
}), no = 20, ao = -10;
function rc(e, t) {
  return e && t && e[0] === t[0] && e[1] === t[1];
}
function di(e, t) {
  this.rootNode = new E.Node(), this.is2D = e, this._labelsBuilder = new yt(256, 256, t), this._labelsBuilder.getMesh().renderOrder = 100, this.rootNode.add(this._labelsBuilder.getMesh()), this._api = t, this._spriteImageCanvas = document.createElement("canvas"), this._startDataIndex = 0, this._endDataIndex = 0, this._sizeScale = 1;
}
di.prototype = {
  constructor: di,
  /**
   * If highlight on over
   */
  highlightOnMouseover: !0,
  update: function(e, t, r, i, n) {
    var a = this._prevMesh;
    this._prevMesh = this._mesh, this._mesh = a;
    var o = e.getData();
    if (i == null && (i = 0), n == null && (n = o.count()), this._startDataIndex = i, this._endDataIndex = n - 1, !this._mesh) {
      var s = this._prevMesh && this._prevMesh.material;
      this._mesh = new tc({
        // Render after axes
        renderOrder: 10,
        // FIXME
        frustumCulling: !1
      }), s && (this._mesh.material = s);
    }
    var s = this._mesh.material, l = this._mesh.geometry, u = l.attributes;
    this.rootNode.remove(this._prevMesh), this.rootNode.add(this._mesh), this._setPositionTextureToMesh(this._mesh, this._positionTexture);
    var f = this._getSymbolInfo(e, i, n), h = r.getDevicePixelRatio(), d = e.getModel("itemStyle").getItemStyle(), c = e.get("large"), v = 1;
    f.maxSize > 2 ? (v = this._updateSymbolSprite(e, d, f, h), s.enableTexture("sprite")) : s.disableTexture("sprite"), u.position.init(n - i);
    var p = [];
    if (c) {
      s.undefine("VERTEX_SIZE"), s.undefine("VERTEX_COLOR");
      var m = Bh(o), _ = Fh(o);
      E.parseColor(m, p), p[3] *= _, s.set({
        color: p,
        u_Size: f.maxSize * this._sizeScale
      });
    } else
      s.set({
        color: [1, 1, 1, 1]
      }), s.define("VERTEX_SIZE"), s.define("VERTEX_COLOR"), u.size.init(n - i), u.color.init(n - i), this._originalOpacity = new Float32Array(n - i);
    for (var x = o.getLayout("points"), y = u.position.value, g = 0; g < n - i; g++) {
      var b = g * 3, w = g * 2;
      if (this.is2D ? (y[b] = x[w], y[b + 1] = x[w + 1], y[b + 2] = ao) : (y[b] = x[b], y[b + 1] = x[b + 1], y[b + 2] = x[b + 2]), !c) {
        var m = ut(o, g), _ = mt(o, g);
        E.parseColor(m, p), p[3] *= _, u.color.set(g, p), p[3] < 0.99;
        var C = o.getItemVisual(g, "symbolSize");
        C = C instanceof Array ? Math.max(C[0], C[1]) : C, isNaN(C) && (C = 0), u.size.value[g] = C * v * this._sizeScale, this._originalOpacity[g] = p[3];
      }
    }
    this._mesh.sizeScale = v, l.updateBoundingBox(), l.dirty(), this._updateMaterial(e, d);
    var A = e.coordinateSystem;
    if (A && A.viewGL) {
      var R = A.viewGL.isLinearSpace() ? "define" : "undefine";
      s[R]("fragment", "SRGB_DECODE");
    }
    c || this._updateLabelBuilder(e, i, n), this._updateHandler(e, t, r), this._updateAnimation(e), this._api = r;
  },
  getPointsMesh: function() {
    return this._mesh;
  },
  updateLabels: function(e) {
    this._labelsBuilder.updateLabels(e);
  },
  hideLabels: function() {
    this.rootNode.remove(this._labelsBuilder.getMesh());
  },
  showLabels: function() {
    this.rootNode.add(this._labelsBuilder.getMesh());
  },
  dispose: function() {
    this._labelsBuilder.dispose();
  },
  _updateSymbolSprite: function(e, t, r, i) {
    r.maxSize = Math.min(r.maxSize * 2, 200);
    var n = [];
    return r.aspect > 1 ? (n[0] = r.maxSize, n[1] = r.maxSize / r.aspect) : (n[1] = r.maxSize, n[0] = r.maxSize * r.aspect), n[0] = n[0] || 1, n[1] = n[1] || 1, (this._symbolType !== r.type || !rc(this._symbolSize, n) || this._lineWidth !== t.lineWidth) && (ci.createSymbolSprite(r.type, n, {
      fill: "#fff",
      lineWidth: t.lineWidth,
      stroke: "transparent",
      shadowColor: "transparent",
      minMargin: Math.min(n[0] / 2, 10)
    }, this._spriteImageCanvas), ci.createSDFFromCanvas(this._spriteImageCanvas, Math.min(this._spriteImageCanvas.width, 32), no, this._mesh.material.get("sprite").image), this._symbolType = r.type, this._symbolSize = n, this._lineWidth = t.lineWidth), this._spriteImageCanvas.width / r.maxSize * i;
  },
  _updateMaterial: function(e, t) {
    var r = e.get("blendMode") === "lighter" ? E.additiveBlend : null, i = this._mesh.material;
    i.blend = r, i.set("lineWidth", t.lineWidth / no);
    var n = E.parseColor(t.stroke);
    i.set("strokeColor", n), i.transparent = !0, i.depthMask = !1, i.depthTest = !this.is2D, i.sortVertices = !this.is2D;
  },
  _updateLabelBuilder: function(e, o, r) {
    var i = e.getData(), n = this._mesh.geometry, a = n.attributes.position.value, o = this._startDataIndex, s = this._mesh.sizeScale;
    this._labelsBuilder.updateData(i, o, r), this._labelsBuilder.getLabelPosition = function(l, u, f) {
      var h = (l - o) * 3;
      return [a[h], a[h + 1], a[h + 2]];
    }, this._labelsBuilder.getLabelDistance = function(l, u, f) {
      var h = n.attributes.size.get(l - o) / s;
      return h / 2 + f;
    }, this._labelsBuilder.updateLabels();
  },
  _updateAnimation: function(e) {
    E.updateVertexAnimation([["prevPosition", "position"], ["prevSize", "size"]], this._prevMesh, this._mesh, e);
  },
  _updateHandler: function(e, t, r) {
    var i = e.getData(), n = this._mesh, a = this, o = -1, s = e.coordinateSystem && e.coordinateSystem.type === "cartesian3D", l;
    s && (l = e.coordinateSystem.model), n.seriesIndex = e.seriesIndex, n.off("mousemove"), n.off("mouseout"), n.on("mousemove", function(u) {
      var f = u.vertexIndex + a._startDataIndex;
      f !== o && (this.highlightOnMouseover && (this.downplay(i, o), this.highlight(i, f), this._labelsBuilder.updateLabels([f])), s && r.dispatchAction({
        type: "grid3DShowAxisPointer",
        value: [i.get(e.coordDimToDataDim("x")[0], f), i.get(e.coordDimToDataDim("y")[0], f), i.get(e.coordDimToDataDim("z")[0], f)],
        grid3DIndex: l.componentIndex
      })), n.dataIndex = f, o = f;
    }, this), n.on("mouseout", function(u) {
      var f = u.vertexIndex + a._startDataIndex;
      this.highlightOnMouseover && (this.downplay(i, f), this._labelsBuilder.updateLabels()), o = -1, n.dataIndex = -1, s && r.dispatchAction({
        type: "grid3DHideAxisPointer",
        grid3DIndex: l.componentIndex
      });
    }, this);
  },
  updateLayout: function(e, t, r) {
    var i = e.getData();
    if (this._mesh) {
      var n = this._mesh.geometry.attributes.position.value, a = i.getLayout("points");
      if (this.is2D)
        for (var o = 0; o < a.length / 2; o++) {
          var s = o * 3, l = o * 2;
          n[s] = a[l], n[s + 1] = a[l + 1], n[s + 2] = ao;
        }
      else
        for (var o = 0; o < a.length; o++)
          n[o] = a[o];
      this._mesh.geometry.dirty(), r.getZr().refresh();
    }
  },
  updateView: function(e) {
    if (this._mesh) {
      var t = new I();
      I.mul(t, e.viewMatrix, this._mesh.worldTransform), I.mul(t, e.projectionMatrix, t), this._mesh.updateNDCPosition(t, this.is2D, this._api);
    }
  },
  highlight: function(e, t) {
    if (!(t > this._endDataIndex || t < this._startDataIndex)) {
      var r = e.getItemModel(t), i = r.getModel("emphasis.itemStyle"), n = i.get("color"), a = i.get("opacity");
      if (n == null) {
        var o = ut(e, t);
        n = So(o, -0.4);
      }
      a == null && (a = mt(e, t));
      var s = E.parseColor(n);
      s[3] *= a, this._mesh.geometry.attributes.color.set(t - this._startDataIndex, s), this._mesh.geometry.dirtyAttribute("color"), this._api.getZr().refresh();
    }
  },
  downplay: function(e, t) {
    if (!(t > this._endDataIndex || t < this._startDataIndex)) {
      var r = ut(e, t), i = mt(e, t), n = E.parseColor(r);
      n[3] *= i, this._mesh.geometry.attributes.color.set(t - this._startDataIndex, n), this._mesh.geometry.dirtyAttribute("color"), this._api.getZr().refresh();
    }
  },
  fadeOutAll: function(e) {
    if (this._originalOpacity) {
      for (var t = this._mesh.geometry, r = 0; r < t.vertexCount; r++) {
        var i = this._originalOpacity[r] * e;
        t.attributes.color.value[r * 4 + 3] = i;
      }
      t.dirtyAttribute("color"), this._api.getZr().refresh();
    }
  },
  fadeInAll: function() {
    this.fadeOutAll(1);
  },
  setPositionTexture: function(e) {
    this._mesh && this._setPositionTextureToMesh(this._mesh, e), this._positionTexture = e;
  },
  removePositionTexture: function() {
    this._positionTexture = null, this._mesh && this._setPositionTextureToMesh(this._mesh, null);
  },
  setSizeScale: function(e) {
    if (e !== this._sizeScale) {
      if (this._mesh) {
        var t = this._mesh.material.get("u_Size");
        this._mesh.material.set("u_Size", t / this._sizeScale * e);
        var r = this._mesh.geometry.attributes;
        if (r.size.value)
          for (var i = 0; i < r.size.value.length; i++)
            r.size.value[i] = r.size.value[i] / this._sizeScale * e;
      }
      this._sizeScale = e;
    }
  },
  _setPositionTextureToMesh: function(e, t) {
    t && e.material.set("positionTexture", t), e.material[t ? "enableTexture" : "disableTexture"]("positionTexture");
  },
  _getSymbolInfo: function(e, t, r) {
    if (e.get("large")) {
      var i = be.firstNotNull(e.get("symbolSize"), 1), s, a;
      return i instanceof Array ? (s = Math.max(i[0], i[1]), a = i[0] / i[1]) : (s = i, a = 1), {
        maxSize: i,
        type: e.get("symbol"),
        aspect: a
      };
    }
    for (var n = e.getData(), a, o = n.getItemVisual(0, "symbol") || "circle", s = 0, l = t; l < r; l++) {
      var i = n.getItemVisual(l, "symbolSize"), u = n.getItemVisual(l, "symbol"), f;
      if (i instanceof Array)
        f = i[0] / i[1], s = Math.max(Math.max(i[0], i[1]), s);
      else {
        if (isNaN(i))
          continue;
        f = 1, s = Math.max(i, s);
      }
      o = u, a = f;
    }
    return {
      maxSize: s,
      type: o,
      aspect: a
    };
  }
};
const ic = _i.extend({
  type: "scatter3D",
  hasSymbolVisual: !0,
  __ecgl__: !0,
  init: function(e, t) {
    this.groupGL = new E.Node(), this._pointsBuilderList = [], this._currentStep = 0;
  },
  render: function(e, t, r) {
    if (this.groupGL.removeAll(), !!e.getData().count()) {
      var i = e.coordinateSystem;
      if (i && i.viewGL) {
        i.viewGL.add(this.groupGL), this._camera = i.viewGL.camera;
        var n = this._pointsBuilderList[0];
        n || (n = this._pointsBuilderList[0] = new di(!1, r)), this._pointsBuilderList.length = 1, this.groupGL.add(n.rootNode), n.update(e, t, r), n.updateView(i.viewGL.camera);
      }
    }
  },
  incrementalPrepareRender: function(e, t, r) {
    var i = e.coordinateSystem;
    i && i.viewGL && (i.viewGL.add(this.groupGL), this._camera = i.viewGL.camera), this.groupGL.removeAll(), this._currentStep = 0;
  },
  incrementalRender: function(e, t, r, i) {
    if (!(e.end <= e.start)) {
      var n = this._pointsBuilderList[this._currentStep];
      n || (n = new di(!1, i), this._pointsBuilderList[this._currentStep] = n), this.groupGL.add(n.rootNode), n.update(t, r, i, e.start, e.end), n.updateView(t.coordinateSystem.viewGL.camera), this._currentStep++;
    }
  },
  updateCamera: function() {
    this._pointsBuilderList.forEach(function(e) {
      e.updateView(this._camera);
    }, this);
  },
  highlight: function(e, t, r, i) {
    this._toggleStatus("highlight", e, t, r, i);
  },
  downplay: function(e, t, r, i) {
    this._toggleStatus("downplay", e, t, r, i);
  },
  _toggleStatus: function(e, t, r, i, n) {
    var a = t.getData(), o = be.queryDataIndex(a, n), s = e === "highlight";
    o != null ? rt(Jt.normalizeToArray(o), function(l) {
      for (var u = 0; u < this._pointsBuilderList.length; u++) {
        var f = this._pointsBuilderList[u];
        s ? f.highlight(a, l) : f.downplay(a, l);
      }
    }, this) : a.each(function(l) {
      for (var u = 0; u < this._pointsBuilderList.length; u++) {
        var f = this._pointsBuilderList[u];
        s ? f.highlight(a, l) : f.downplay(a, l);
      }
    });
  },
  dispose: function() {
    this._pointsBuilderList.forEach(function(e) {
      e.dispose();
    }), this.groupGL.removeAll();
  },
  remove: function() {
    this.groupGL.removeAll();
  }
});
function nc(e) {
  e.registerChartView(ic), e.registerSeriesModel(Kh), e.registerLayout({
    seriesType: "scatter3D",
    reset: function(t) {
      var r = t.coordinateSystem;
      if (r) {
        var i = r.dimensions;
        if (i.length < 3)
          return;
        var n = i.map(function(s) {
          return t.coordDimToDataDim(s)[0];
        }), a = [], o = [];
        return {
          progress: function(s, l) {
            for (var u = new Float32Array((s.end - s.start) * 3), f = s.start; f < s.end; f++) {
              var h = (f - s.start) * 3;
              a[0] = l.get(n[0], f), a[1] = l.get(n[1], f), a[2] = l.get(n[2], f), r.dataToPoint(a, o), u[h] = o[0], u[h + 1] = o[1], u[h + 2] = o[2];
            }
            l.setLayout("points", u);
          }
        };
      }
    }
  });
}
const ac = {
  defaultOption: {
    viewControl: {
      // perspective, orthographic.
      // TODO Isometric
      projection: "perspective",
      // If rotate on on init
      autoRotate: !1,
      // cw or ccw
      autoRotateDirection: "cw",
      // Degree per second
      autoRotateSpeed: 10,
      // Start rotating after still for a given time
      // default is 3 seconds
      autoRotateAfterStill: 3,
      // Rotate, zoom damping.
      damping: 0.8,
      // Sensitivities for operations.
      // Can be array to set x,y respectively
      rotateSensitivity: 1,
      zoomSensitivity: 1,
      // Can be array to set x,y respectively
      panSensitivity: 1,
      // Which mouse button do rotate or pan
      panMouseButton: "middle",
      rotateMouseButton: "left",
      // Distance to the target
      // Only available when camera is perspective.
      distance: 150,
      // Min distance mouse can zoom in
      minDistance: 40,
      // Max distance mouse can zoom out
      maxDistance: 400,
      // Size of viewing volume.
      // Only available when camera is orthographic
      orthographicSize: 150,
      maxOrthographicSize: 400,
      minOrthographicSize: 20,
      // Center view point
      center: [0, 0, 0],
      // Alpha angle for top-down rotation
      // Positive to rotate to top.
      alpha: 0,
      // beta angle for left-right rotation
      // Positive to rotate to right.
      beta: 0,
      minAlpha: -90,
      maxAlpha: 90
      // minBeta: -Infinity
      // maxBeta: -Infinity
    }
  },
  setView: function(e) {
    e = e || {}, this.option.viewControl = this.option.viewControl || {}, e.alpha != null && (this.option.viewControl.alpha = e.alpha), e.beta != null && (this.option.viewControl.beta = e.beta), e.distance != null && (this.option.viewControl.distance = e.distance), e.center != null && (this.option.viewControl.center = e.center);
  }
}, oc = {
  defaultOption: {
    // Post effect
    postEffect: {
      enable: !1,
      bloom: {
        enable: !0,
        intensity: 0.1
      },
      depthOfField: {
        enable: !1,
        focalRange: 20,
        focalDistance: 50,
        blurRadius: 10,
        fstop: 2.8,
        quality: "medium"
      },
      screenSpaceAmbientOcclusion: {
        enable: !1,
        radius: 2,
        // low, medium, high, ultra
        quality: "medium",
        intensity: 1
      },
      screenSpaceReflection: {
        enable: !1,
        quality: "medium",
        maxRoughness: 0.8
      },
      colorCorrection: {
        enable: !0,
        exposure: 0,
        brightness: 0,
        contrast: 1,
        saturation: 1,
        lookupTexture: ""
      },
      edge: {
        enable: !1
      },
      FXAA: {
        enable: !1
      }
    },
    // Temporal super sampling when the picture is still.
    temporalSuperSampling: {
      // Only enabled when postEffect is enabled
      enable: "auto"
    }
  }
}, sc = {
  defaultOption: {
    // Light is available when material.shading is not color
    light: {
      // Main light
      main: {
        shadow: !1,
        // low, medium, high, ultra
        shadowQuality: "high",
        color: "#fff",
        intensity: 1,
        alpha: 0,
        beta: 0
      },
      ambient: {
        color: "#fff",
        intensity: 0.2
      },
      ambientCubemap: {
        // Panorama environment texture,
        // Support .hdr and commmon web formats.
        texture: null,
        // Available when texture is hdr.
        exposure: 1,
        // Intensity for diffuse term
        diffuseIntensity: 0.5,
        // Intensity for specular term, only available when shading is realastic
        specularIntensity: 0.5
      }
    }
  }
};
var vi = function() {
  this._pool = {}, this._allocatedTextures = [];
};
vi.prototype = {
  constructor: vi,
  get: function(e) {
    var t = oo(e);
    this._pool.hasOwnProperty(t) || (this._pool[t] = []);
    var r = this._pool[t];
    if (!r.length) {
      var i = new K(e);
      return this._allocatedTextures.push(i), i;
    }
    return r.pop();
  },
  put: function(e) {
    var t = oo(e);
    this._pool.hasOwnProperty(t) || (this._pool[t] = []);
    var r = this._pool[t];
    r.push(e);
  },
  clear: function(e) {
    for (var t = 0; t < this._allocatedTextures.length; t++)
      this._allocatedTextures[t].dispose(e);
    this._pool = {}, this._allocatedTextures = [];
  }
};
var os = {
  width: 512,
  height: 512,
  type: M.UNSIGNED_BYTE,
  format: M.RGBA,
  wrapS: M.CLAMP_TO_EDGE,
  wrapT: M.CLAMP_TO_EDGE,
  minFilter: M.LINEAR_MIPMAP_LINEAR,
  magFilter: M.LINEAR,
  useMipmap: !0,
  anisotropic: 1,
  flipY: !0,
  unpackAlignment: 4,
  premultiplyAlpha: !1
}, on = Object.keys(os);
function oo(e) {
  he.defaultsWithPropList(e, os, on), lc(e);
  for (var t = "", r = 0; r < on.length; r++) {
    var i = on[r], n = e[i].toString();
    t += n;
  }
  return t;
}
function lc(e) {
  var t = uc(e.width, e.height);
  e.format === M.DEPTH_COMPONENT && (e.useMipmap = !1), (!t || !e.useMipmap) && (e.minFilter == M.NEAREST_MIPMAP_NEAREST || e.minFilter == M.NEAREST_MIPMAP_LINEAR ? e.minFilter = M.NEAREST : (e.minFilter == M.LINEAR_MIPMAP_LINEAR || e.minFilter == M.LINEAR_MIPMAP_NEAREST) && (e.minFilter = M.LINEAR)), t || (e.wrapS = M.CLAMP_TO_EDGE, e.wrapT = M.CLAMP_TO_EDGE);
}
function uc(e, t) {
  return (e & e - 1) === 0 && (t & t - 1) === 0;
}
const fc = `@export clay.sm.depth.vertex
uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
attribute vec3 position : POSITION;
attribute vec2 texcoord : TEXCOORD_0;
uniform vec2 uvRepeat = vec2(1.0, 1.0);
uniform vec2 uvOffset = vec2(0.0, 0.0);
@import clay.chunk.skinning_header
@import clay.chunk.instancing_header
varying vec4 v_ViewPosition;
varying vec2 v_Texcoord;
void main(){
 vec4 P = vec4(position, 1.0);
#ifdef SKINNING
 @import clay.chunk.skin_matrix
 P = skinMatrixWS * P;
#endif
#ifdef INSTANCING
 @import clay.chunk.instancing_matrix
 P = instanceMat * P;
#endif
 v_ViewPosition = worldViewProjection * P;
 gl_Position = v_ViewPosition;
 v_Texcoord = texcoord * uvRepeat + uvOffset;
}
@end
@export clay.sm.depth.fragment
varying vec4 v_ViewPosition;
varying vec2 v_Texcoord;
uniform float bias : 0.001;
uniform float slopeScale : 1.0;
uniform sampler2D alphaMap;
uniform float alphaCutoff: 0.0;
@import clay.util.encode_float
void main(){
 float depth = v_ViewPosition.z / v_ViewPosition.w;
 if (alphaCutoff > 0.0) {
 if (texture2D(alphaMap, v_Texcoord).a <= alphaCutoff) {
 discard;
 }
 }
#ifdef USE_VSM
 depth = depth * 0.5 + 0.5;
 float moment1 = depth;
 float moment2 = depth * depth;
 #ifdef SUPPORT_STANDARD_DERIVATIVES
 float dx = dFdx(depth);
 float dy = dFdy(depth);
 moment2 += 0.25*(dx*dx+dy*dy);
 #endif
 gl_FragColor = vec4(moment1, moment2, 0.0, 1.0);
#else
 #ifdef SUPPORT_STANDARD_DERIVATIVES
 float dx = dFdx(depth);
 float dy = dFdy(depth);
 depth += sqrt(dx*dx + dy*dy) * slopeScale + bias;
 #else
 depth += bias;
 #endif
 gl_FragColor = encodeFloat(depth * 0.5 + 0.5);
#endif
}
@end
@export clay.sm.debug_depth
uniform sampler2D depthMap;
varying vec2 v_Texcoord;
@import clay.util.decode_float
void main() {
 vec4 tex = texture2D(depthMap, v_Texcoord);
#ifdef USE_VSM
 gl_FragColor = vec4(tex.rgb, 1.0);
#else
 float depth = decodeFloat(tex);
 gl_FragColor = vec4(depth, depth, depth, 1.0);
#endif
}
@end
@export clay.sm.distance.vertex
uniform mat4 worldViewProjection : WORLDVIEWPROJECTION;
uniform mat4 world : WORLD;
attribute vec3 position : POSITION;
@import clay.chunk.skinning_header
varying vec3 v_WorldPosition;
void main (){
 vec4 P = vec4(position, 1.0);
#ifdef SKINNING
 @import clay.chunk.skin_matrix
 P = skinMatrixWS * P;
#endif
#ifdef INSTANCING
 @import clay.chunk.instancing_matrix
 P = instanceMat * P;
#endif
 gl_Position = worldViewProjection * P;
 v_WorldPosition = (world * P).xyz;
}
@end
@export clay.sm.distance.fragment
uniform vec3 lightPosition;
uniform float range : 100;
varying vec3 v_WorldPosition;
@import clay.util.encode_float
void main(){
 float dist = distance(lightPosition, v_WorldPosition);
#ifdef USE_VSM
 gl_FragColor = vec4(dist, dist * dist, 0.0, 0.0);
#else
 dist = dist / range;
 gl_FragColor = encodeFloat(dist);
#endif
}
@end
@export clay.plugin.shadow_map_common
@import clay.util.decode_float
float tapShadowMap(sampler2D map, vec2 uv, float z){
 vec4 tex = texture2D(map, uv);
 return step(z, decodeFloat(tex) * 2.0 - 1.0);
}
float pcf(sampler2D map, vec2 uv, float z, float textureSize, vec2 scale) {
 float shadowContrib = tapShadowMap(map, uv, z);
 vec2 offset = vec2(1.0 / textureSize) * scale;
#ifdef PCF_KERNEL_SIZE
 for (int _idx_ = 0; _idx_ < PCF_KERNEL_SIZE; _idx_++) {{
 shadowContrib += tapShadowMap(map, uv + offset * pcfKernel[_idx_], z);
 }}
 return shadowContrib / float(PCF_KERNEL_SIZE + 1);
#else
 shadowContrib += tapShadowMap(map, uv+vec2(offset.x, 0.0), z);
 shadowContrib += tapShadowMap(map, uv+vec2(offset.x, offset.y), z);
 shadowContrib += tapShadowMap(map, uv+vec2(-offset.x, offset.y), z);
 shadowContrib += tapShadowMap(map, uv+vec2(0.0, offset.y), z);
 shadowContrib += tapShadowMap(map, uv+vec2(-offset.x, 0.0), z);
 shadowContrib += tapShadowMap(map, uv+vec2(-offset.x, -offset.y), z);
 shadowContrib += tapShadowMap(map, uv+vec2(offset.x, -offset.y), z);
 shadowContrib += tapShadowMap(map, uv+vec2(0.0, -offset.y), z);
 return shadowContrib / 9.0;
#endif
}
float pcf(sampler2D map, vec2 uv, float z, float textureSize) {
 return pcf(map, uv, z, textureSize, vec2(1.0));
}
float chebyshevUpperBound(vec2 moments, float z){
 float p = 0.0;
 z = z * 0.5 + 0.5;
 if (z <= moments.x) {
 p = 1.0;
 }
 float variance = moments.y - moments.x * moments.x;
 variance = max(variance, 0.0000001);
 float mD = moments.x - z;
 float pMax = variance / (variance + mD * mD);
 pMax = clamp((pMax-0.4)/(1.0-0.4), 0.0, 1.0);
 return max(p, pMax);
}
float computeShadowContrib(
 sampler2D map, mat4 lightVPM, vec3 position, float textureSize, vec2 scale, vec2 offset
) {
 vec4 posInLightSpace = lightVPM * vec4(position, 1.0);
 posInLightSpace.xyz /= posInLightSpace.w;
 float z = posInLightSpace.z;
 if(all(greaterThan(posInLightSpace.xyz, vec3(-0.99, -0.99, -1.0))) &&
 all(lessThan(posInLightSpace.xyz, vec3(0.99, 0.99, 1.0)))){
 vec2 uv = (posInLightSpace.xy+1.0) / 2.0;
 #ifdef USE_VSM
 vec2 moments = texture2D(map, uv * scale + offset).xy;
 return chebyshevUpperBound(moments, z);
 #else
 return pcf(map, uv * scale + offset, z, textureSize, scale);
 #endif
 }
 return 1.0;
}
float computeShadowContrib(sampler2D map, mat4 lightVPM, vec3 position, float textureSize) {
 return computeShadowContrib(map, lightVPM, position, textureSize, vec2(1.0), vec2(0.0));
}
float computeShadowContribOmni(samplerCube map, vec3 direction, float range)
{
 float dist = length(direction);
 vec4 shadowTex = textureCube(map, direction);
#ifdef USE_VSM
 vec2 moments = shadowTex.xy;
 float variance = moments.y - moments.x * moments.x;
 float mD = moments.x - dist;
 float p = variance / (variance + mD * mD);
 if(moments.x + 0.001 < dist){
 return clamp(p, 0.0, 1.0);
 }else{
 return 1.0;
 }
#else
 return step(dist, (decodeFloat(shadowTex) + 0.0002) * range);
#endif
}
@end
@export clay.plugin.compute_shadow_map
#if defined(SPOT_LIGHT_SHADOWMAP_COUNT) || defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT) || defined(POINT_LIGHT_SHADOWMAP_COUNT)
#ifdef SPOT_LIGHT_SHADOWMAP_COUNT
uniform sampler2D spotLightShadowMaps[SPOT_LIGHT_SHADOWMAP_COUNT]:unconfigurable;
uniform mat4 spotLightMatrices[SPOT_LIGHT_SHADOWMAP_COUNT]:unconfigurable;
uniform float spotLightShadowMapSizes[SPOT_LIGHT_SHADOWMAP_COUNT]:unconfigurable;
#endif
#ifdef DIRECTIONAL_LIGHT_SHADOWMAP_COUNT
#if defined(SHADOW_CASCADE)
uniform sampler2D directionalLightShadowMaps[1]:unconfigurable;
uniform mat4 directionalLightMatrices[SHADOW_CASCADE]:unconfigurable;
uniform float directionalLightShadowMapSizes[1]:unconfigurable;
uniform float shadowCascadeClipsNear[SHADOW_CASCADE]:unconfigurable;
uniform float shadowCascadeClipsFar[SHADOW_CASCADE]:unconfigurable;
#else
uniform sampler2D directionalLightShadowMaps[DIRECTIONAL_LIGHT_SHADOWMAP_COUNT]:unconfigurable;
uniform mat4 directionalLightMatrices[DIRECTIONAL_LIGHT_SHADOWMAP_COUNT]:unconfigurable;
uniform float directionalLightShadowMapSizes[DIRECTIONAL_LIGHT_SHADOWMAP_COUNT]:unconfigurable;
#endif
#endif
#ifdef POINT_LIGHT_SHADOWMAP_COUNT
uniform samplerCube pointLightShadowMaps[POINT_LIGHT_SHADOWMAP_COUNT]:unconfigurable;
#endif
uniform bool shadowEnabled : true;
#ifdef PCF_KERNEL_SIZE
uniform vec2 pcfKernel[PCF_KERNEL_SIZE];
#endif
@import clay.plugin.shadow_map_common
#if defined(SPOT_LIGHT_SHADOWMAP_COUNT)
void computeShadowOfSpotLights(vec3 position, inout float shadowContribs[SPOT_LIGHT_COUNT] ) {
 float shadowContrib;
 for(int _idx_ = 0; _idx_ < SPOT_LIGHT_SHADOWMAP_COUNT; _idx_++) {{
 shadowContrib = computeShadowContrib(
 spotLightShadowMaps[_idx_], spotLightMatrices[_idx_], position,
 spotLightShadowMapSizes[_idx_]
 );
 shadowContribs[_idx_] = shadowContrib;
 }}
 for(int _idx_ = SPOT_LIGHT_SHADOWMAP_COUNT; _idx_ < SPOT_LIGHT_COUNT; _idx_++){{
 shadowContribs[_idx_] = 1.0;
 }}
}
#endif
#if defined(DIRECTIONAL_LIGHT_SHADOWMAP_COUNT)
#ifdef SHADOW_CASCADE
void computeShadowOfDirectionalLights(vec3 position, inout float shadowContribs[DIRECTIONAL_LIGHT_COUNT]){
 float depth = (2.0 * gl_FragCoord.z - gl_DepthRange.near - gl_DepthRange.far)
 / (gl_DepthRange.far - gl_DepthRange.near);
 float shadowContrib;
 shadowContribs[0] = 1.0;
 for (int _idx_ = 0; _idx_ < SHADOW_CASCADE; _idx_++) {{
 if (
 depth >= shadowCascadeClipsNear[_idx_] &&
 depth <= shadowCascadeClipsFar[_idx_]
 ) {
 shadowContrib = computeShadowContrib(
 directionalLightShadowMaps[0], directionalLightMatrices[_idx_], position,
 directionalLightShadowMapSizes[0],
 vec2(1.0 / float(SHADOW_CASCADE), 1.0),
 vec2(float(_idx_) / float(SHADOW_CASCADE), 0.0)
 );
 shadowContribs[0] = shadowContrib;
 }
 }}
 for(int _idx_ = DIRECTIONAL_LIGHT_SHADOWMAP_COUNT; _idx_ < DIRECTIONAL_LIGHT_COUNT; _idx_++) {{
 shadowContribs[_idx_] = 1.0;
 }}
}
#else
void computeShadowOfDirectionalLights(vec3 position, inout float shadowContribs[DIRECTIONAL_LIGHT_COUNT]){
 float shadowContrib;
 for(int _idx_ = 0; _idx_ < DIRECTIONAL_LIGHT_SHADOWMAP_COUNT; _idx_++) {{
 shadowContrib = computeShadowContrib(
 directionalLightShadowMaps[_idx_], directionalLightMatrices[_idx_], position,
 directionalLightShadowMapSizes[_idx_]
 );
 shadowContribs[_idx_] = shadowContrib;
 }}
 for(int _idx_ = DIRECTIONAL_LIGHT_SHADOWMAP_COUNT; _idx_ < DIRECTIONAL_LIGHT_COUNT; _idx_++) {{
 shadowContribs[_idx_] = 1.0;
 }}
}
#endif
#endif
#if defined(POINT_LIGHT_SHADOWMAP_COUNT)
void computeShadowOfPointLights(vec3 position, inout float shadowContribs[POINT_LIGHT_COUNT] ){
 vec3 lightPosition;
 vec3 direction;
 for(int _idx_ = 0; _idx_ < POINT_LIGHT_SHADOWMAP_COUNT; _idx_++) {{
 lightPosition = pointLightPosition[_idx_];
 direction = position - lightPosition;
 shadowContribs[_idx_] = computeShadowContribOmni(pointLightShadowMaps[_idx_], direction, pointLightRange[_idx_]);
 }}
 for(int _idx_ = POINT_LIGHT_SHADOWMAP_COUNT; _idx_ < POINT_LIGHT_COUNT; _idx_++) {{
 shadowContribs[_idx_] = 1.0;
 }}
}
#endif
#endif
@end`;
var vt = ["px", "nx", "py", "ny", "pz", "nz"];
U.import(fc);
function sn(e, t, r) {
  if (r === "alphaMap")
    return e.material.get("diffuseMap");
  if (r === "alphaCutoff") {
    if (e.material.isDefined("fragment", "ALPHA_TEST") && e.material.get("diffuseMap")) {
      var i = e.material.get("alphaCutoff");
      return i || 0;
    }
    return 0;
  } else return r === "uvRepeat" ? e.material.get("uvRepeat") : r === "uvOffset" ? e.material.get("uvOffset") : t.get(r);
}
function so(e, t) {
  var r = e.material, i = t.material;
  return r.get("diffuseMap") !== i.get("diffuseMap") || (r.get("alphaCutoff") || 0) !== (i.get("alphaCutoff") || 0);
}
var $e = Ae.extend(function() {
  return (
    /** @lends clay.prePass.ShadowMap# */
    {
      /**
       * Soft shadow technique.
       * Can be {@link clay.prePass.ShadowMap.PCF} or {@link clay.prePass.ShadowMap.VSM}
       * @type {number}
       */
      softShadow: $e.PCF,
      /**
       * Soft shadow blur size
       * @type {number}
       */
      shadowBlur: 1,
      lightFrustumBias: "auto",
      kernelPCF: new Float32Array([
        1,
        0,
        1,
        1,
        -1,
        1,
        0,
        1,
        -1,
        0,
        -1,
        -1,
        1,
        -1,
        0,
        -1
      ]),
      precision: "highp",
      _lastRenderNotCastShadow: !1,
      _frameBuffer: new pe(),
      _textures: {},
      _shadowMapNumber: {
        POINT_LIGHT: 0,
        DIRECTIONAL_LIGHT: 0,
        SPOT_LIGHT: 0
      },
      _depthMaterials: {},
      _distanceMaterials: {},
      _receivers: [],
      _lightsCastShadow: [],
      _lightCameras: {},
      _lightMaterials: {},
      _texturePool: new vi()
    }
  );
}, function() {
  this._gaussianPassH = new De({
    fragment: U.source("clay.compositor.gaussian_blur")
  }), this._gaussianPassV = new De({
    fragment: U.source("clay.compositor.gaussian_blur")
  }), this._gaussianPassH.setUniform("blurSize", this.shadowBlur), this._gaussianPassH.setUniform("blurDir", 0), this._gaussianPassV.setUniform("blurSize", this.shadowBlur), this._gaussianPassV.setUniform("blurDir", 1), this._outputDepthPass = new De({
    fragment: U.source("clay.sm.debug_depth")
  });
}, {
  /**
   * Render scene to shadow textures
   * @param  {clay.Renderer} renderer
   * @param  {clay.Scene} scene
   * @param  {clay.Camera} sceneCamera
   * @param  {boolean} [notUpdateScene=false]
   * @memberOf clay.prePass.ShadowMap.prototype
   */
  render: function(e, t, r, i) {
    r || (r = t.getMainCamera()), this.trigger("beforerender", this, e, t, r), this._renderShadowPass(e, t, r, i), this.trigger("afterrender", this, e, t, r);
  },
  /**
   * Debug rendering of shadow textures
   * @param  {clay.Renderer} renderer
   * @param  {number} size
   * @memberOf clay.prePass.ShadowMap.prototype
   */
  renderDebug: function(e, t) {
    e.saveClear();
    var r = e.viewport, i = 0, n = 0, a = t || r.width / 4, o = a;
    this.softShadow === $e.VSM ? this._outputDepthPass.material.define("fragment", "USE_VSM") : this._outputDepthPass.material.undefine("fragment", "USE_VSM");
    for (var s in this._textures) {
      var l = this._textures[s];
      e.setViewport(i, n, a * l.width / l.height, o), this._outputDepthPass.setUniform("depthMap", l), this._outputDepthPass.render(e), i += a * l.width / l.height;
    }
    e.setViewport(r), e.restoreClear();
  },
  _updateReceivers: function(e, t) {
    if (t.receiveShadow ? (this._receivers.push(t), t.material.set("shadowEnabled", 1), t.material.set("pcfKernel", this.kernelPCF)) : t.material.set("shadowEnabled", 0), this.softShadow === $e.VSM)
      t.material.define("fragment", "USE_VSM"), t.material.undefine("fragment", "PCF_KERNEL_SIZE");
    else {
      t.material.undefine("fragment", "USE_VSM");
      var r = this.kernelPCF;
      r && r.length ? t.material.define("fragment", "PCF_KERNEL_SIZE", r.length / 2) : t.material.undefine("fragment", "PCF_KERNEL_SIZE");
    }
  },
  _update: function(e, t) {
    var r = this;
    t.traverse(function(a) {
      a.isRenderable() && r._updateReceivers(e, a);
    });
    for (var i = 0; i < t.lights.length; i++) {
      var n = t.lights[i];
      n.castShadow && !n.invisible && this._lightsCastShadow.push(n);
    }
  },
  _renderShadowPass: function(e, t, r, i) {
    for (var n in this._shadowMapNumber)
      this._shadowMapNumber[n] = 0;
    this._lightsCastShadow.length = 0, this._receivers.length = 0;
    var a = e.gl;
    if (i || t.update(), r && r.update(), t.updateLights(), this._update(e, t), !this._lightsCastShadow.length && this._lastRenderNotCastShadow)
      return;
    this._lastRenderNotCastShadow = this._lightsCastShadow === 0, a.enable(a.DEPTH_TEST), a.depthMask(!0), a.disable(a.BLEND), a.clearColor(1, 1, 1, 1);
    for (var o = [], s = [], l = [], u = [], f = [], h = [], d, c = 0; c < this._lightsCastShadow.length; c++) {
      var v = this._lightsCastShadow[c];
      if (v.type === "DIRECTIONAL_LIGHT") {
        if (d) {
          console.warn("Only one direectional light supported with shadow cascade");
          continue;
        }
        if (v.shadowCascade > 4) {
          console.warn("Support at most 4 cascade");
          continue;
        }
        v.shadowCascade > 1 && (d = v), this.renderDirectionalLightShadow(
          e,
          t,
          r,
          v,
          f,
          u,
          l
        );
      } else v.type === "SPOT_LIGHT" ? this.renderSpotLightShadow(
        e,
        t,
        v,
        s,
        o
      ) : v.type === "POINT_LIGHT" && this.renderPointLightShadow(
        e,
        t,
        v,
        h
      );
      this._shadowMapNumber[v.type]++;
    }
    for (var p in this._shadowMapNumber)
      for (var m = this._shadowMapNumber[p], _ = p + "_SHADOWMAP_COUNT", c = 0; c < this._receivers.length; c++) {
        var x = this._receivers[c], y = x.material;
        y.fragmentDefines[_] !== m && (m > 0 ? y.define("fragment", _, m) : y.isDefined("fragment", _) && y.undefine("fragment", _));
      }
    for (var c = 0; c < this._receivers.length; c++) {
      var x = this._receivers[c], y = x.material;
      d ? y.define("fragment", "SHADOW_CASCADE", d.shadowCascade) : y.undefine("fragment", "SHADOW_CASCADE");
    }
    var g = t.shadowUniforms;
    function b(H) {
      return H.height;
    }
    if (l.length > 0) {
      var w = l.map(b);
      if (g.directionalLightShadowMaps = { value: l, type: "tv" }, g.directionalLightMatrices = { value: u, type: "m4v" }, g.directionalLightShadowMapSizes = { value: w, type: "1fv" }, d) {
        var C = f.slice(), A = f.slice();
        C.pop(), A.shift(), C.reverse(), A.reverse(), u.reverse(), g.shadowCascadeClipsNear = { value: C, type: "1fv" }, g.shadowCascadeClipsFar = { value: A, type: "1fv" };
      }
    }
    if (o.length > 0) {
      var R = o.map(b), g = t.shadowUniforms;
      g.spotLightShadowMaps = { value: o, type: "tv" }, g.spotLightMatrices = { value: s, type: "m4v" }, g.spotLightShadowMapSizes = { value: R, type: "1fv" };
    }
    h.length > 0 && (g.pointLightShadowMaps = { value: h, type: "tv" });
  },
  renderDirectionalLightShadow: (function() {
    var e = new yi(), t = new I(), r = new we(), i = new I(), n = new I(), a = new I(), o = new I();
    return function(s, l, u, f, h, d, c) {
      var v = this._getDepthMaterial(f), p = {
        getMaterial: function(de) {
          return de.shadowDepthMaterial || v;
        },
        isMaterialChanged: so,
        getUniform: sn,
        ifRender: function(de) {
          return de.castShadow;
        },
        sortCompare: Ve.opaqueSortCompare
      };
      if (!l.viewBoundingBoxLastFrame.isFinite()) {
        var m = l.getBoundingBox();
        l.viewBoundingBoxLastFrame.copy(m).applyTransform(u.viewMatrix);
      }
      var _ = Math.min(-l.viewBoundingBoxLastFrame.min.z, u.far), x = Math.max(-l.viewBoundingBoxLastFrame.max.z, u.near), y = this._getDirectionalLightCamera(f, l, u), g = a.array;
      o.copy(y.projectionMatrix), D.invert(n.array, y.worldTransform.array), D.multiply(n.array, n.array, u.worldTransform.array), D.multiply(g, o.array, n.array);
      for (var b = [], w = u instanceof _e, C = (u.near + u.far) / (u.near - u.far), A = 2 * u.near * u.far / (u.near - u.far), R = 0; R <= f.shadowCascade; R++) {
        var H = x * Math.pow(_ / x, R / f.shadowCascade), O = x + (_ - x) * R / f.shadowCascade, z = H * f.cascadeSplitLogFactor + O * (1 - f.cascadeSplitLogFactor);
        b.push(z), h.push(-(-z * C + A) / -z);
      }
      var G = this._getTexture(f, f.shadowCascade);
      c.push(G);
      var F = s.viewport, W = s.gl;
      this._frameBuffer.attach(G), this._frameBuffer.bind(s), W.clear(W.COLOR_BUFFER_BIT | W.DEPTH_BUFFER_BIT);
      for (var R = 0; R < f.shadowCascade; R++) {
        var X = b[R], q = b[R + 1];
        w ? D.perspective(t.array, u.fov / 180 * Math.PI, u.aspect, X, q) : D.ortho(
          t.array,
          u.left,
          u.right,
          u.bottom,
          u.top,
          X,
          q
        ), e.setFromProjection(t), e.getTransformedBoundingBox(r, n), r.applyProjection(o);
        var ie = r.min.array, me = r.max.array;
        ie[0] = Math.max(ie[0], -1), ie[1] = Math.max(ie[1], -1), me[0] = Math.min(me[0], 1), me[1] = Math.min(me[1], 1), i.ortho(ie[0], me[0], ie[1], me[1], 1, -1), y.projectionMatrix.multiplyLeft(i);
        var Se = f.shadowResolution || 512;
        s.setViewport((f.shadowCascade - R - 1) * Se, 0, Se, Se, 1);
        var ce = l.updateRenderList(y);
        s.renderPass(ce.opaque, y, p), this.softShadow === $e.VSM && this._gaussianFilter(s, G, G.width);
        var ye = new I();
        ye.copy(y.viewMatrix).multiplyLeft(y.projectionMatrix), d.push(ye.array), y.projectionMatrix.copy(o);
      }
      this._frameBuffer.unbind(s), s.setViewport(F);
    };
  })(),
  renderSpotLightShadow: function(e, t, r, i, n) {
    var a = this._getTexture(r), o = this._getSpotLightCamera(r), s = e.gl;
    this._frameBuffer.attach(a), this._frameBuffer.bind(e), s.clear(s.COLOR_BUFFER_BIT | s.DEPTH_BUFFER_BIT);
    var l = this._getDepthMaterial(r), u = {
      getMaterial: function(d) {
        return d.shadowDepthMaterial || l;
      },
      isMaterialChanged: so,
      getUniform: sn,
      ifRender: function(d) {
        return d.castShadow;
      },
      sortCompare: Ve.opaqueSortCompare
    }, f = t.updateRenderList(o);
    e.renderPass(f.opaque, o, u), this._frameBuffer.unbind(e), this.softShadow === $e.VSM && this._gaussianFilter(e, a, a.width);
    var h = new I();
    h.copy(o.worldTransform).invert().multiplyLeft(o.projectionMatrix), n.push(a), i.push(h.array);
  },
  renderPointLightShadow: function(e, t, r, i) {
    var n = this._getTexture(r), a = e.gl;
    i.push(n);
    var o = this._getDepthMaterial(r), s = {
      getMaterial: function(x) {
        return x.shadowDepthMaterial || o;
      },
      getUniform: sn,
      sortCompare: Ve.opaqueSortCompare
    }, l = {
      px: [],
      py: [],
      pz: [],
      nx: [],
      ny: [],
      nz: []
    }, u = new we(), f = r.getWorldPosition().array, h = new we(), d = r.range;
    h.min.setArray(f), h.max.setArray(f);
    var c = new S(d, d, d);
    h.max.add(c), h.min.sub(c);
    var v = { px: !1, py: !1, pz: !1, nx: !1, ny: !1, nz: !1 };
    t.traverse(function(x) {
      if (x.isRenderable() && x.castShadow) {
        var y = x.geometry;
        if (!y.boundingBox) {
          for (var g = 0; g < vt.length; g++)
            l[vt[g]].push(x);
          return;
        }
        if (u.transformFrom(y.boundingBox, x.worldTransform), !u.intersectBoundingBox(h))
          return;
        u.updateVertices();
        for (var g = 0; g < vt.length; g++)
          v[vt[g]] = !1;
        for (var g = 0; g < 8; g++) {
          var b = u.vertices[g], w = b[0] - f[0], C = b[1] - f[1], A = b[2] - f[2], R = Math.abs(w), H = Math.abs(C), O = Math.abs(A);
          R > H ? R > O ? v[w > 0 ? "px" : "nx"] = !0 : v[A > 0 ? "pz" : "nz"] = !0 : H > O ? v[C > 0 ? "py" : "ny"] = !0 : v[A > 0 ? "pz" : "nz"] = !0;
        }
        for (var g = 0; g < vt.length; g++)
          v[vt[g]] && l[vt[g]].push(x);
      }
    });
    for (var p = 0; p < 6; p++) {
      var m = vt[p], _ = this._getPointLightCamera(r, m);
      this._frameBuffer.attach(n, a.COLOR_ATTACHMENT0, a.TEXTURE_CUBE_MAP_POSITIVE_X + p), this._frameBuffer.bind(e), a.clear(a.COLOR_BUFFER_BIT | a.DEPTH_BUFFER_BIT), e.renderPass(l[m], _, s);
    }
    this._frameBuffer.unbind(e);
  },
  _getDepthMaterial: function(e) {
    var t = this._lightMaterials[e.__uid__], r = e.type === "POINT_LIGHT";
    if (!t) {
      var i = r ? "clay.sm.distance." : "clay.sm.depth.";
      t = new ft({
        precision: this.precision,
        shader: new U(U.source(i + "vertex"), U.source(i + "fragment"))
      }), this._lightMaterials[e.__uid__] = t;
    }
    return e.shadowSlopeScale != null && t.setUniform("slopeScale", e.shadowSlopeScale), e.shadowBias != null && t.setUniform("bias", e.shadowBias), this.softShadow === $e.VSM ? t.define("fragment", "USE_VSM") : t.undefine("fragment", "USE_VSM"), r && (t.set("lightPosition", e.getWorldPosition().array), t.set("range", e.range)), t;
  },
  _gaussianFilter: function(e, t, r) {
    var i = {
      width: r,
      height: r,
      type: N.FLOAT
    }, n = this._texturePool.get(i);
    this._frameBuffer.attach(n), this._frameBuffer.bind(e), this._gaussianPassH.setUniform("texture", t), this._gaussianPassH.setUniform("textureWidth", r), this._gaussianPassH.render(e), this._frameBuffer.attach(t), this._gaussianPassV.setUniform("texture", n), this._gaussianPassV.setUniform("textureHeight", r), this._gaussianPassV.render(e), this._frameBuffer.unbind(e), this._texturePool.put(n);
  },
  _getTexture: function(e, t) {
    var r = e.__uid__, i = this._textures[r], n = e.shadowResolution || 512;
    return t = t || 1, i || (e.type === "POINT_LIGHT" ? i = new Rt() : i = new K(), i.width = n * t, i.height = n, this.softShadow === $e.VSM ? (i.type = N.FLOAT, i.anisotropic = 4) : (i.minFilter = M.NEAREST, i.magFilter = M.NEAREST, i.useMipmap = !1), this._textures[r] = i), i;
  },
  _getPointLightCamera: function(e, t) {
    this._lightCameras.point || (this._lightCameras.point = {
      px: new _e(),
      nx: new _e(),
      py: new _e(),
      ny: new _e(),
      pz: new _e(),
      nz: new _e()
    });
    var r = this._lightCameras.point[t];
    switch (r.far = e.range, r.fov = 90, r.position.set(0, 0, 0), t) {
      case "px":
        r.lookAt(S.POSITIVE_X, S.NEGATIVE_Y);
        break;
      case "nx":
        r.lookAt(S.NEGATIVE_X, S.NEGATIVE_Y);
        break;
      case "py":
        r.lookAt(S.POSITIVE_Y, S.POSITIVE_Z);
        break;
      case "ny":
        r.lookAt(S.NEGATIVE_Y, S.NEGATIVE_Z);
        break;
      case "pz":
        r.lookAt(S.POSITIVE_Z, S.NEGATIVE_Y);
        break;
      case "nz":
        r.lookAt(S.NEGATIVE_Z, S.NEGATIVE_Y);
        break;
    }
    return e.getWorldPosition(r.position), r.update(), r;
  },
  _getDirectionalLightCamera: (function() {
    var e = new I(), t = new we(), r = new we();
    return function(i, n, a) {
      this._lightCameras.directional || (this._lightCameras.directional = new wr());
      var o = this._lightCameras.directional;
      t.copy(n.viewBoundingBoxLastFrame), t.intersection(a.frustum.boundingBox), o.position.copy(t.min).add(t.max).scale(0.5).transformMat4(a.worldTransform), o.rotation.copy(i.rotation), o.scale.copy(i.scale), o.updateWorldTransform(), I.invert(e, o.worldTransform), I.multiply(e, e, a.worldTransform), r.copy(t).applyTransform(e);
      var s = r.min.array, l = r.max.array;
      return o.position.set((s[0] + l[0]) / 2, (s[1] + l[1]) / 2, l[2]).transformMat4(o.worldTransform), o.near = 0, o.far = -s[2] + l[2], isNaN(this.lightFrustumBias) ? o.far *= 4 : o.far += this.lightFrustumBias, o.left = s[0], o.right = l[0], o.top = l[1], o.bottom = s[1], o.update(!0), o;
    };
  })(),
  _getSpotLightCamera: function(e) {
    this._lightCameras.spot || (this._lightCameras.spot = new _e());
    var t = this._lightCameras.spot;
    return t.fov = e.penumbraAngle * 2, t.far = e.range, t.worldTransform.copy(e.worldTransform), t.updateProjectionMatrix(), D.invert(t.viewMatrix.array, t.worldTransform.array), t;
  },
  /**
   * @param  {clay.Renderer|WebGLRenderingContext} [renderer]
   * @memberOf clay.prePass.ShadowMap.prototype
   */
  // PENDING Renderer or WebGLRenderingContext
  dispose: function(e) {
    var t = e.gl || e;
    this._frameBuffer && this._frameBuffer.dispose(t);
    for (var r in this._textures)
      this._textures[r].dispose(t);
    this._texturePool.clear(e.gl), this._depthMaterials = {}, this._distanceMaterials = {}, this._textures = {}, this._lightCameras = {}, this._shadowMapNumber = {
      POINT_LIGHT: 0,
      DIRECTIONAL_LIGHT: 0,
      SPOT_LIGHT: 0
    }, this._meshMaterials = {};
    for (var i = 0; i < this._receivers.length; i++) {
      var n = this._receivers[i];
      if (n.material) {
        var a = n.material;
        a.undefine("fragment", "POINT_LIGHT_SHADOW_COUNT"), a.undefine("fragment", "DIRECTIONAL_LIGHT_SHADOW_COUNT"), a.undefine("fragment", "AMBIENT_LIGHT_SHADOW_COUNT"), a.set("shadowEnabled", 0);
      }
    }
    this._receivers = [], this._lightsCastShadow = [];
  }
});
$e.VSM = 1;
$e.PCF = 2;
var br = Ae.extend(
  function() {
    return (
      /** @lends clay.compositor.CompositorNode# */
      {
        /**
         * @type {string}
         */
        name: "",
        /**
         * Input links, will be updated by the graph
         * @example:
         *     inputName: {
         *         node: someNode,
         *         pin: 'xxxx'
         *     }
         * @type {Object}
         */
        inputLinks: {},
        /**
         * Output links, will be updated by the graph
         * @example:
         *     outputName: {
         *         node: someNode,
         *         pin: 'xxxx'
         *     }
         * @type {Object}
         */
        outputLinks: {},
        // Save the output texture of previous frame
        // Will be used when there exist a circular reference
        _prevOutputTextures: {},
        _outputTextures: {},
        // Example: { name: 2 }
        _outputReferences: {},
        _rendering: !1,
        // If rendered in this frame
        _rendered: !1,
        _compositor: null
      }
    );
  },
  /** @lends clay.compositor.CompositorNode.prototype */
  {
    // TODO Remove parameter function callback
    updateParameter: function(e, t) {
      var r = this.outputs[e], i = r.parameters, n = r._parametersCopy;
      if (n || (n = r._parametersCopy = {}), i)
        for (var a in i)
          a !== "width" && a !== "height" && (n[a] = i[a]);
      var o, s;
      return i.width instanceof Function ? o = i.width.call(this, t) : o = i.width, i.height instanceof Function ? s = i.height.call(this, t) : s = i.height, (n.width !== o || n.height !== s) && this._outputTextures[e] && this._outputTextures[e].dispose(t.gl), n.width = o, n.height = s, n;
    },
    /**
     * Set parameter
     * @param {string} name
     * @param {} value
     */
    setParameter: function(e, t) {
    },
    /**
     * Get parameter value
     * @param  {string} name
     * @return {}
     */
    getParameter: function(e) {
    },
    /**
     * Set parameters
     * @param {Object} obj
     */
    setParameters: function(e) {
      for (var t in e)
        this.setParameter(t, e[t]);
    },
    render: function() {
    },
    getOutput: function(e, t) {
      if (t == null)
        return t = e, this._outputTextures[t];
      var r = this.outputs[t];
      if (r)
        return this._rendered ? r.outputLastFrame ? this._prevOutputTextures[t] : this._outputTextures[t] : (
          // TODO
          this._rendering ? (this._prevOutputTextures[t] || (this._prevOutputTextures[t] = this._compositor.allocateTexture(r.parameters || {})), this._prevOutputTextures[t]) : (this.render(e), this._outputTextures[t])
        );
    },
    removeReference: function(e) {
      if (this._outputReferences[e]--, this._outputReferences[e] === 0) {
        var t = this.outputs[e];
        t.keepLastFrame ? (this._prevOutputTextures[e] && this._compositor.releaseTexture(this._prevOutputTextures[e]), this._prevOutputTextures[e] = this._outputTextures[e]) : this._compositor.releaseTexture(this._outputTextures[e]);
      }
    },
    link: function(e, t, r) {
      this.inputLinks[e] = {
        node: t,
        pin: r
      }, t.outputLinks[r] || (t.outputLinks[r] = []), t.outputLinks[r].push({
        node: this,
        pin: e
      }), this.pass.material.enableTexture(e);
    },
    clear: function() {
      this.inputLinks = {}, this.outputLinks = {};
    },
    updateReference: function(e) {
      if (!this._rendering) {
        this._rendering = !0;
        for (var t in this.inputLinks) {
          var r = this.inputLinks[t];
          r.node.updateReference(r.pin);
        }
        this._rendering = !1;
      }
      e && this._outputReferences[e]++;
    },
    beforeFrame: function() {
      this._rendered = !1;
      for (var e in this.outputLinks)
        this._outputReferences[e] = 0;
    },
    afterFrame: function() {
      for (var e in this.outputLinks)
        if (this._outputReferences[e] > 0) {
          var t = this.outputs[e];
          t.keepLastFrame ? (this._prevOutputTextures[e] && this._compositor.releaseTexture(this._prevOutputTextures[e]), this._prevOutputTextures[e] = this._outputTextures[e]) : this._compositor.releaseTexture(this._outputTextures[e]);
        }
    }
  }
), lo = Ae.extend(
  function() {
    return (
      /** @lends clay.compositor.Graph# */
      {
        /**
         * @type {Array.<clay.compositor.CompositorNode>}
         */
        nodes: []
      }
    );
  },
  /** @lends clay.compositor.Graph.prototype */
  {
    /**
     * Mark to update
     */
    dirty: function() {
      this._dirty = !0;
    },
    /**
     * @param {clay.compositor.CompositorNode} node
     */
    addNode: function(e) {
      this.nodes.indexOf(e) >= 0 || (this.nodes.push(e), this._dirty = !0);
    },
    /**
     * @param  {clay.compositor.CompositorNode|string} node
     */
    removeNode: function(e) {
      typeof e == "string" && (e = this.getNodeByName(e));
      var t = this.nodes.indexOf(e);
      t >= 0 && (this.nodes.splice(t, 1), this._dirty = !0);
    },
    /**
     * @param {string} name
     * @return {clay.compositor.CompositorNode}
     */
    getNodeByName: function(e) {
      for (var t = 0; t < this.nodes.length; t++)
        if (this.nodes[t].name === e)
          return this.nodes[t];
    },
    /**
     * Update links of graph
     */
    update: function() {
      for (var e = 0; e < this.nodes.length; e++)
        this.nodes[e].clear();
      for (var e = 0; e < this.nodes.length; e++) {
        var t = this.nodes[e];
        if (t.inputs) {
          for (var r in t.inputs)
            if (t.inputs[r]) {
              if (t.pass && !t.pass.material.isUniformEnabled(r)) {
                console.warn("Pin " + t.name + "." + r + " not used.");
                continue;
              }
              var i = t.inputs[r], n = this.findPin(i);
              n ? t.link(r, n.node, n.pin) : console.warn(typeof i == "string" ? "Node " + i + " not exist" : "Pin of " + i.node + "." + i.pin + " not exist");
            }
        }
      }
    },
    findPin: function(e) {
      var t;
      if ((typeof e == "string" || e instanceof br) && (e = {
        node: e
      }), typeof e.node == "string")
        for (var r = 0; r < this.nodes.length; r++) {
          var i = this.nodes[r];
          i.name === e.node && (t = i);
        }
      else
        t = e.node;
      if (t) {
        var n = e.pin;
        if (n || t.outputs && (n = Object.keys(t.outputs)[0]), t.outputs[n])
          return {
            node: t,
            pin: n
          };
      }
    }
  }
), hc = lo.extend(
  function() {
    return {
      // Output node
      _outputs: [],
      _texturePool: new vi(),
      _frameBuffer: new pe({
        depthBuffer: !1
      })
    };
  },
  /** @lends clay.compositor.Compositor.prototype */
  {
    addNode: function(e) {
      lo.prototype.addNode.call(this, e), e._compositor = this;
    },
    /**
     * @param  {clay.Renderer} renderer
     */
    render: function(e, t) {
      if (this._dirty) {
        this.update(), this._dirty = !1, this._outputs.length = 0;
        for (var r = 0; r < this.nodes.length; r++)
          this.nodes[r].outputs || this._outputs.push(this.nodes[r]);
      }
      for (var r = 0; r < this.nodes.length; r++)
        this.nodes[r].beforeFrame();
      for (var r = 0; r < this._outputs.length; r++)
        this._outputs[r].updateReference();
      for (var r = 0; r < this._outputs.length; r++)
        this._outputs[r].render(e, t);
      for (var r = 0; r < this.nodes.length; r++)
        this.nodes[r].afterFrame();
    },
    allocateTexture: function(e) {
      return this._texturePool.get(e);
    },
    releaseTexture: function(e) {
      this._texturePool.put(e);
    },
    getFrameBuffer: function() {
      return this._frameBuffer;
    },
    /**
     * Dispose compositor
     * @param {clay.Renderer} renderer
     */
    dispose: function(e) {
      this._texturePool.clear(e);
    }
  }
), cc = br.extend(
  /** @lends clay.compositor.SceneNode# */
  {
    name: "scene",
    /**
     * @type {clay.Scene}
     */
    scene: null,
    /**
     * @type {clay.Camera}
     */
    camera: null,
    /**
     * @type {boolean}
     */
    autoUpdateScene: !0,
    /**
     * @type {boolean}
     */
    preZ: !1
  },
  function() {
    this.frameBuffer = new pe();
  },
  {
    render: function(e) {
      this._rendering = !0;
      var t = e.gl;
      this.trigger("beforerender");
      var r;
      if (!this.outputs)
        r = e.render(this.scene, this.camera, !this.autoUpdateScene, this.preZ);
      else {
        var i = this.frameBuffer;
        for (var n in this.outputs) {
          var a = this.updateParameter(n, e), o = this.outputs[n], s = this._compositor.allocateTexture(a);
          this._outputTextures[n] = s;
          var l = o.attachment || t.COLOR_ATTACHMENT0;
          typeof l == "string" && (l = t[l]), i.attach(s, l);
        }
        i.bind(e);
        var u = e.getGLExtension("EXT_draw_buffers");
        if (u) {
          var f = [];
          for (var l in this.outputs)
            l = parseInt(l), l >= t.COLOR_ATTACHMENT0 && l <= t.COLOR_ATTACHMENT0 + 8 && f.push(l);
          u.drawBuffersEXT(f);
        }
        e.saveClear(), e.clearBit = M.DEPTH_BUFFER_BIT | M.COLOR_BUFFER_BIT, r = e.render(this.scene, this.camera, !this.autoUpdateScene, this.preZ), e.restoreClear(), i.unbind(e);
      }
      this.trigger("afterrender", r), this._rendering = !1, this._rendered = !0;
    }
  }
), dc = br.extend(function() {
  return (
    /** @lends clay.compositor.TextureNode# */
    {
      /**
       * @type {clay.Texture2D}
       */
      texture: null,
      // Texture node must have output without parameters
      outputs: {
        color: {}
      }
    }
  );
}, function() {
}, {
  getOutput: function(e, t) {
    return this.texture;
  },
  // Do nothing
  beforeFrame: function() {
  },
  afterFrame: function() {
  }
}), vc = br.extend(
  function() {
    return (
      /** @lends clay.compositor.FilterNode# */
      {
        /**
         * @type {string}
         */
        name: "",
        /**
         * @type {Object}
         */
        inputs: {},
        /**
         * @type {Object}
         */
        outputs: null,
        /**
         * @type {string}
         */
        shader: "",
        /**
         * Input links, will be updated by the graph
         * @example:
         *     inputName: {
         *         node: someNode,
         *         pin: 'xxxx'
         *     }
         * @type {Object}
         */
        inputLinks: {},
        /**
         * Output links, will be updated by the graph
         * @example:
         *     outputName: {
         *         node: someNode,
         *         pin: 'xxxx'
         *     }
         * @type {Object}
         */
        outputLinks: {},
        /**
         * @type {clay.compositor.Pass}
         */
        pass: null,
        // Save the output texture of previous frame
        // Will be used when there exist a circular reference
        _prevOutputTextures: {},
        _outputTextures: {},
        // Example: { name: 2 }
        _outputReferences: {},
        _rendering: !1,
        // If rendered in this frame
        _rendered: !1,
        _compositor: null
      }
    );
  },
  function() {
    var e = new De({
      fragment: this.shader
    });
    this.pass = e;
  },
  /** @lends clay.compositor.FilterNode.prototype */
  {
    /**
     * @param  {clay.Renderer} renderer
     */
    render: function(e, t) {
      this.trigger("beforerender", e), this._rendering = !0;
      var r = e.gl;
      for (var i in this.inputLinks) {
        var n = this.inputLinks[i], a = n.node.getOutput(e, n.pin);
        this.pass.setUniform(i, a);
      }
      if (!this.outputs)
        this.pass.outputs = null, this._compositor.getFrameBuffer().unbind(e), this.pass.render(e, t);
      else {
        this.pass.outputs = {};
        var o = {};
        for (var s in this.outputs) {
          var l = this.updateParameter(s, e);
          isNaN(l.width) && this.updateParameter(s, e);
          var u = this.outputs[s], f = this._compositor.allocateTexture(l);
          this._outputTextures[s] = f;
          var h = u.attachment || r.COLOR_ATTACHMENT0;
          typeof h == "string" && (h = r[h]), o[h] = f;
        }
        this._compositor.getFrameBuffer().bind(e);
        for (var h in o)
          this._compositor.getFrameBuffer().attach(
            o[h],
            h
          );
        this.pass.render(e), this._compositor.getFrameBuffer().updateMipmap(e);
      }
      for (var i in this.inputLinks) {
        var n = this.inputLinks[i];
        n.node.removeReference(n.pin);
      }
      this._rendering = !1, this._rendered = !0, this.trigger("afterrender", e);
    },
    // TODO Remove parameter function callback
    updateParameter: function(e, t) {
      var r = this.outputs[e], i = r.parameters, n = r._parametersCopy;
      if (n || (n = r._parametersCopy = {}), i)
        for (var a in i)
          a !== "width" && a !== "height" && (n[a] = i[a]);
      var o, s;
      return typeof i.width == "function" ? o = i.width.call(this, t) : o = i.width, typeof i.height == "function" ? s = i.height.call(this, t) : s = i.height, o = Math.ceil(o), s = Math.ceil(s), (n.width !== o || n.height !== s) && this._outputTextures[e] && this._outputTextures[e].dispose(t), n.width = o, n.height = s, n;
    },
    /**
     * Set parameter
     * @param {string} name
     * @param {} value
     */
    setParameter: function(e, t) {
      this.pass.setUniform(e, t);
    },
    /**
     * Get parameter value
     * @param  {string} name
     * @return {}
     */
    getParameter: function(e) {
      return this.pass.getUniform(e);
    },
    /**
     * Set parameters
     * @param {Object} obj
     */
    setParameters: function(e) {
      for (var t in e)
        this.setParameter(t, e[t]);
    },
    // /**
    //  * Set shader code
    //  * @param {string} shaderStr
    //  */
    // setShader: function (shaderStr) {
    //     var material = this.pass.material;
    //     material.shader.setFragment(shaderStr);
    //     material.attachShader(material.shader, true);
    // },
    /**
     * Proxy of pass.material.define('fragment', xxx);
     * @param  {string} symbol
     * @param  {number} [val]
     */
    define: function(e, t) {
      this.pass.material.define("fragment", e, t);
    },
    /**
     * Proxy of pass.material.undefine('fragment', xxx)
     * @param  {string} symbol
     */
    undefine: function(e) {
      this.pass.material.undefine("fragment", e);
    },
    removeReference: function(e) {
      if (this._outputReferences[e]--, this._outputReferences[e] === 0) {
        var t = this.outputs[e];
        t.keepLastFrame ? (this._prevOutputTextures[e] && this._compositor.releaseTexture(this._prevOutputTextures[e]), this._prevOutputTextures[e] = this._outputTextures[e]) : this._compositor.releaseTexture(this._outputTextures[e]);
      }
    },
    clear: function() {
      br.prototype.clear.call(this), this.pass.material.disableTexturesAll();
    }
  }
);
const pc = `@export clay.compositor.coloradjust
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform float brightness : 0.0;
uniform float contrast : 1.0;
uniform float exposure : 0.0;
uniform float gamma : 1.0;
uniform float saturation : 1.0;
const vec3 w = vec3(0.2125, 0.7154, 0.0721);
void main()
{
 vec4 tex = texture2D( texture, v_Texcoord);
 vec3 color = clamp(tex.rgb + vec3(brightness), 0.0, 1.0);
 color = clamp( (color-vec3(0.5))*contrast+vec3(0.5), 0.0, 1.0);
 color = clamp( color * pow(2.0, exposure), 0.0, 1.0);
 color = clamp( pow(color, vec3(gamma)), 0.0, 1.0);
 float luminance = dot( color, w );
 color = mix(vec3(luminance), color, saturation);
 gl_FragColor = vec4(color, tex.a);
}
@end
@export clay.compositor.brightness
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform float brightness : 0.0;
void main()
{
 vec4 tex = texture2D( texture, v_Texcoord);
 vec3 color = tex.rgb + vec3(brightness);
 gl_FragColor = vec4(color, tex.a);
}
@end
@export clay.compositor.contrast
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform float contrast : 1.0;
void main()
{
 vec4 tex = texture2D( texture, v_Texcoord);
 vec3 color = (tex.rgb-vec3(0.5))*contrast+vec3(0.5);
 gl_FragColor = vec4(color, tex.a);
}
@end
@export clay.compositor.exposure
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform float exposure : 0.0;
void main()
{
 vec4 tex = texture2D(texture, v_Texcoord);
 vec3 color = tex.rgb * pow(2.0, exposure);
 gl_FragColor = vec4(color, tex.a);
}
@end
@export clay.compositor.gamma
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform float gamma : 1.0;
void main()
{
 vec4 tex = texture2D(texture, v_Texcoord);
 vec3 color = pow(tex.rgb, vec3(gamma));
 gl_FragColor = vec4(color, tex.a);
}
@end
@export clay.compositor.saturation
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform float saturation : 1.0;
const vec3 w = vec3(0.2125, 0.7154, 0.0721);
void main()
{
 vec4 tex = texture2D(texture, v_Texcoord);
 vec3 color = tex.rgb;
 float luminance = dot(color, w);
 color = mix(vec3(luminance), color, saturation);
 gl_FragColor = vec4(color, tex.a);
}
@end`, ss = `@export clay.compositor.kernel.gaussian_9
float gaussianKernel[9];
gaussianKernel[0] = 0.07;
gaussianKernel[1] = 0.09;
gaussianKernel[2] = 0.12;
gaussianKernel[3] = 0.14;
gaussianKernel[4] = 0.16;
gaussianKernel[5] = 0.14;
gaussianKernel[6] = 0.12;
gaussianKernel[7] = 0.09;
gaussianKernel[8] = 0.07;
@end
@export clay.compositor.kernel.gaussian_13
float gaussianKernel[13];
gaussianKernel[0] = 0.02;
gaussianKernel[1] = 0.03;
gaussianKernel[2] = 0.06;
gaussianKernel[3] = 0.08;
gaussianKernel[4] = 0.11;
gaussianKernel[5] = 0.13;
gaussianKernel[6] = 0.14;
gaussianKernel[7] = 0.13;
gaussianKernel[8] = 0.11;
gaussianKernel[9] = 0.08;
gaussianKernel[10] = 0.06;
gaussianKernel[11] = 0.03;
gaussianKernel[12] = 0.02;
@end
@export clay.compositor.gaussian_blur
#define SHADER_NAME gaussian_blur
uniform sampler2D texture;varying vec2 v_Texcoord;
uniform float blurSize : 2.0;
uniform vec2 textureSize : [512.0, 512.0];
uniform float blurDir : 0.0;
@import clay.util.rgbm
@import clay.util.clamp_sample
void main (void)
{
 @import clay.compositor.kernel.gaussian_9
 vec2 off = blurSize / textureSize;
 off *= vec2(1.0 - blurDir, blurDir);
 vec4 sum = vec4(0.0);
 float weightAll = 0.0;
 for (int i = 0; i < 9; i++) {
 float w = gaussianKernel[i];
 vec4 texel = decodeHDR(clampSample(texture, v_Texcoord + float(i - 4) * off));
 sum += texel * w;
 weightAll += w;
 }
 gl_FragColor = encodeHDR(sum / max(weightAll, 0.01));
}
@end
`, mc = `@export clay.compositor.hdr.log_lum
varying vec2 v_Texcoord;
uniform sampler2D texture;
const vec3 w = vec3(0.2125, 0.7154, 0.0721);
@import clay.util.rgbm
void main()
{
 vec4 tex = decodeHDR(texture2D(texture, v_Texcoord));
 float luminance = dot(tex.rgb, w);
 luminance = log(luminance + 0.001);
 gl_FragColor = encodeHDR(vec4(vec3(luminance), 1.0));
}
@end
@export clay.compositor.hdr.lum_adaption
varying vec2 v_Texcoord;
uniform sampler2D adaptedLum;
uniform sampler2D currentLum;
uniform float frameTime : 0.02;
@import clay.util.rgbm
void main()
{
 float fAdaptedLum = decodeHDR(texture2D(adaptedLum, vec2(0.5, 0.5))).r;
 float fCurrentLum = exp(encodeHDR(texture2D(currentLum, vec2(0.5, 0.5))).r);
 fAdaptedLum += (fCurrentLum - fAdaptedLum) * (1.0 - pow(0.98, 30.0 * frameTime));
 gl_FragColor = encodeHDR(vec4(vec3(fAdaptedLum), 1.0));
}
@end
@export clay.compositor.lum
varying vec2 v_Texcoord;
uniform sampler2D texture;
const vec3 w = vec3(0.2125, 0.7154, 0.0721);
void main()
{
 vec4 tex = texture2D( texture, v_Texcoord );
 float luminance = dot(tex.rgb, w);
 gl_FragColor = vec4(vec3(luminance), 1.0);
}
@end`, ls = `
@export clay.compositor.lut
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform sampler2D lookup;
void main()
{
 vec4 tex = texture2D(texture, v_Texcoord);
 float blueColor = tex.b * 63.0;
 vec2 quad1;
 quad1.y = floor(floor(blueColor) / 8.0);
 quad1.x = floor(blueColor) - (quad1.y * 8.0);
 vec2 quad2;
 quad2.y = floor(ceil(blueColor) / 8.0);
 quad2.x = ceil(blueColor) - (quad2.y * 8.0);
 vec2 texPos1;
 texPos1.x = (quad1.x * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * tex.r);
 texPos1.y = (quad1.y * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * tex.g);
 vec2 texPos2;
 texPos2.x = (quad2.x * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * tex.r);
 texPos2.y = (quad2.y * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * tex.g);
 vec4 newColor1 = texture2D(lookup, texPos1);
 vec4 newColor2 = texture2D(lookup, texPos2);
 vec4 newColor = mix(newColor1, newColor2, fract(blueColor));
 gl_FragColor = vec4(newColor.rgb, tex.w);
}
@end`, _c = `@export clay.compositor.vignette
#define OUTPUT_ALPHA
varying vec2 v_Texcoord;
uniform sampler2D texture;
uniform float darkness: 1;
uniform float offset: 1;
@import clay.util.rgbm
void main()
{
 vec4 texel = decodeHDR(texture2D(texture, v_Texcoord));
 gl_FragColor.rgb = texel.rgb;
 vec2 uv = (v_Texcoord - vec2(0.5)) * vec2(offset);
 gl_FragColor = encodeHDR(vec4(mix(texel.rgb, vec3(1.0 - darkness), dot(uv, uv)), texel.a));
}
@end`, us = `@export clay.compositor.output
#define OUTPUT_ALPHA
varying vec2 v_Texcoord;
uniform sampler2D texture;
@import clay.util.rgbm
void main()
{
 vec4 tex = decodeHDR(texture2D(texture, v_Texcoord));
 gl_FragColor.rgb = tex.rgb;
#ifdef OUTPUT_ALPHA
 gl_FragColor.a = tex.a;
#else
 gl_FragColor.a = 1.0;
#endif
 gl_FragColor = encodeHDR(gl_FragColor);
#ifdef PREMULTIPLY_ALPHA
 gl_FragColor.rgb *= gl_FragColor.a;
#endif
}
@end`, fs = `@export clay.compositor.bright
uniform sampler2D texture;
uniform float threshold : 1;
uniform float scale : 1.0;
uniform vec2 textureSize: [512, 512];
varying vec2 v_Texcoord;
const vec3 lumWeight = vec3(0.2125, 0.7154, 0.0721);
@import clay.util.rgbm
vec4 median(vec4 a, vec4 b, vec4 c)
{
 return a + b + c - min(min(a, b), c) - max(max(a, b), c);
}
void main()
{
 vec4 texel = decodeHDR(texture2D(texture, v_Texcoord));
#ifdef ANTI_FLICKER
 vec3 d = 1.0 / textureSize.xyx * vec3(1.0, 1.0, 0.0);
 vec4 s1 = decodeHDR(texture2D(texture, v_Texcoord - d.xz));
 vec4 s2 = decodeHDR(texture2D(texture, v_Texcoord + d.xz));
 vec4 s3 = decodeHDR(texture2D(texture, v_Texcoord - d.zy));
 vec4 s4 = decodeHDR(texture2D(texture, v_Texcoord + d.zy));
 texel = median(median(texel, s1, s2), s3, s4);
#endif
 float lum = dot(texel.rgb , lumWeight);
 vec4 color;
 if (lum > threshold && texel.a > 0.0)
 {
 color = vec4(texel.rgb * scale, texel.a * scale);
 }
 else
 {
 color = vec4(0.0);
 }
 gl_FragColor = encodeHDR(color);
}
@end
`, hs = `@export clay.compositor.downsample
uniform sampler2D texture;
uniform vec2 textureSize : [512, 512];
varying vec2 v_Texcoord;
@import clay.util.rgbm
float brightness(vec3 c)
{
 return max(max(c.r, c.g), c.b);
}
@import clay.util.clamp_sample
void main()
{
 vec4 d = vec4(-1.0, -1.0, 1.0, 1.0) / textureSize.xyxy;
#ifdef ANTI_FLICKER
 vec3 s1 = decodeHDR(clampSample(texture, v_Texcoord + d.xy)).rgb;
 vec3 s2 = decodeHDR(clampSample(texture, v_Texcoord + d.zy)).rgb;
 vec3 s3 = decodeHDR(clampSample(texture, v_Texcoord + d.xw)).rgb;
 vec3 s4 = decodeHDR(clampSample(texture, v_Texcoord + d.zw)).rgb;
 float s1w = 1.0 / (brightness(s1) + 1.0);
 float s2w = 1.0 / (brightness(s2) + 1.0);
 float s3w = 1.0 / (brightness(s3) + 1.0);
 float s4w = 1.0 / (brightness(s4) + 1.0);
 float oneDivideSum = 1.0 / (s1w + s2w + s3w + s4w);
 vec4 color = vec4(
 (s1 * s1w + s2 * s2w + s3 * s3w + s4 * s4w) * oneDivideSum,
 1.0
 );
#else
 vec4 color = decodeHDR(clampSample(texture, v_Texcoord + d.xy));
 color += decodeHDR(clampSample(texture, v_Texcoord + d.zy));
 color += decodeHDR(clampSample(texture, v_Texcoord + d.xw));
 color += decodeHDR(clampSample(texture, v_Texcoord + d.zw));
 color *= 0.25;
#endif
 gl_FragColor = encodeHDR(color);
}
@end`, cs = `
@export clay.compositor.upsample
#define HIGH_QUALITY
uniform sampler2D texture;
uniform vec2 textureSize : [512, 512];
uniform float sampleScale: 0.5;
varying vec2 v_Texcoord;
@import clay.util.rgbm
@import clay.util.clamp_sample
void main()
{
#ifdef HIGH_QUALITY
 vec4 d = vec4(1.0, 1.0, -1.0, 0.0) / textureSize.xyxy * sampleScale;
 vec4 s;
 s = decodeHDR(clampSample(texture, v_Texcoord - d.xy));
 s += decodeHDR(clampSample(texture, v_Texcoord - d.wy)) * 2.0;
 s += decodeHDR(clampSample(texture, v_Texcoord - d.zy));
 s += decodeHDR(clampSample(texture, v_Texcoord + d.zw)) * 2.0;
 s += decodeHDR(clampSample(texture, v_Texcoord )) * 4.0;
 s += decodeHDR(clampSample(texture, v_Texcoord + d.xw)) * 2.0;
 s += decodeHDR(clampSample(texture, v_Texcoord + d.zy));
 s += decodeHDR(clampSample(texture, v_Texcoord + d.wy)) * 2.0;
 s += decodeHDR(clampSample(texture, v_Texcoord + d.xy));
 gl_FragColor = encodeHDR(s / 16.0);
#else
 vec4 d = vec4(-1.0, -1.0, +1.0, +1.0) / textureSize.xyxy;
 vec4 s;
 s = decodeHDR(clampSample(texture, v_Texcoord + d.xy));
 s += decodeHDR(clampSample(texture, v_Texcoord + d.zy));
 s += decodeHDR(clampSample(texture, v_Texcoord + d.xw));
 s += decodeHDR(clampSample(texture, v_Texcoord + d.zw));
 gl_FragColor = encodeHDR(s / 4.0);
#endif
}
@end`, ds = `@export clay.compositor.hdr.composite
#define TONEMAPPING
uniform sampler2D texture;
#ifdef BLOOM_ENABLED
uniform sampler2D bloom;
#endif
#ifdef LENSFLARE_ENABLED
uniform sampler2D lensflare;
uniform sampler2D lensdirt;
#endif
#ifdef LUM_ENABLED
uniform sampler2D lum;
#endif
#ifdef LUT_ENABLED
uniform sampler2D lut;
#endif
#ifdef COLOR_CORRECTION
uniform float brightness : 0.0;
uniform float contrast : 1.0;
uniform float saturation : 1.0;
#endif
#ifdef VIGNETTE
uniform float vignetteDarkness: 1.0;
uniform float vignetteOffset: 1.0;
#endif
uniform float exposure : 1.0;
uniform float bloomIntensity : 0.25;
uniform float lensflareIntensity : 1;
varying vec2 v_Texcoord;
@import clay.util.srgb
vec3 ACESToneMapping(vec3 color)
{
 const float A = 2.51;
 const float B = 0.03;
 const float C = 2.43;
 const float D = 0.59;
 const float E = 0.14;
 return (color * (A * color + B)) / (color * (C * color + D) + E);
}
float eyeAdaption(float fLum)
{
 return mix(0.2, fLum, 0.5);
}
#ifdef LUT_ENABLED
vec3 lutTransform(vec3 color) {
 float blueColor = color.b * 63.0;
 vec2 quad1;
 quad1.y = floor(floor(blueColor) / 8.0);
 quad1.x = floor(blueColor) - (quad1.y * 8.0);
 vec2 quad2;
 quad2.y = floor(ceil(blueColor) / 8.0);
 quad2.x = ceil(blueColor) - (quad2.y * 8.0);
 vec2 texPos1;
 texPos1.x = (quad1.x * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * color.r);
 texPos1.y = (quad1.y * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * color.g);
 vec2 texPos2;
 texPos2.x = (quad2.x * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * color.r);
 texPos2.y = (quad2.y * 0.125) + 0.5/512.0 + ((0.125 - 1.0/512.0) * color.g);
 vec4 newColor1 = texture2D(lut, texPos1);
 vec4 newColor2 = texture2D(lut, texPos2);
 vec4 newColor = mix(newColor1, newColor2, fract(blueColor));
 return newColor.rgb;
}
#endif
@import clay.util.rgbm
void main()
{
 vec4 texel = vec4(0.0);
 vec4 originalTexel = vec4(0.0);
#ifdef TEXTURE_ENABLED
 texel = decodeHDR(texture2D(texture, v_Texcoord));
 originalTexel = texel;
#endif
#ifdef BLOOM_ENABLED
 vec4 bloomTexel = decodeHDR(texture2D(bloom, v_Texcoord));
 texel.rgb += bloomTexel.rgb * bloomIntensity;
 texel.a += bloomTexel.a * bloomIntensity;
#endif
#ifdef LENSFLARE_ENABLED
 texel += decodeHDR(texture2D(lensflare, v_Texcoord)) * texture2D(lensdirt, v_Texcoord) * lensflareIntensity;
#endif
 texel.a = min(texel.a, 1.0);
#ifdef LUM_ENABLED
 float fLum = texture2D(lum, vec2(0.5, 0.5)).r;
 float adaptedLumDest = 3.0 / (max(0.1, 1.0 + 10.0*eyeAdaption(fLum)));
 float exposureBias = adaptedLumDest * exposure;
#else
 float exposureBias = exposure;
#endif
#ifdef TONEMAPPING
 texel.rgb *= exposureBias;
 texel.rgb = ACESToneMapping(texel.rgb);
#endif
 texel = linearTosRGB(texel);
#ifdef LUT_ENABLED
 texel.rgb = lutTransform(clamp(texel.rgb,vec3(0.0),vec3(1.0)));
#endif
#ifdef COLOR_CORRECTION
 texel.rgb = clamp(texel.rgb + vec3(brightness), 0.0, 1.0);
 texel.rgb = clamp((texel.rgb - vec3(0.5))*contrast+vec3(0.5), 0.0, 1.0);
 float lum = dot(texel.rgb, vec3(0.2125, 0.7154, 0.0721));
 texel.rgb = mix(vec3(lum), texel.rgb, saturation);
#endif
#ifdef VIGNETTE
 vec2 uv = (v_Texcoord - vec2(0.5)) * vec2(vignetteOffset);
 texel.rgb = mix(texel.rgb, vec3(1.0 - vignetteDarkness), dot(uv, uv));
#endif
 gl_FragColor = encodeHDR(texel);
#ifdef DEBUG
 #if DEBUG == 1
 gl_FragColor = encodeHDR(decodeHDR(texture2D(texture, v_Texcoord)));
 #elif DEBUG == 2
 gl_FragColor = encodeHDR(decodeHDR(texture2D(bloom, v_Texcoord)) * bloomIntensity);
 #elif DEBUG == 3
 gl_FragColor = encodeHDR(decodeHDR(texture2D(lensflare, v_Texcoord) * lensflareIntensity));
 #endif
#endif
 if (originalTexel.a <= 0.01 && gl_FragColor.a > 1e-5) {
 gl_FragColor.a = dot(gl_FragColor.rgb, vec3(0.2125, 0.7154, 0.0721));
 }
#ifdef PREMULTIPLY_ALPHA
 gl_FragColor.rgb *= gl_FragColor.a;
#endif
}
@end`, gc = `@export clay.compositor.lensflare
#define SAMPLE_NUMBER 8
uniform sampler2D texture;
uniform sampler2D lenscolor;
uniform vec2 textureSize : [512, 512];
uniform float dispersal : 0.3;
uniform float haloWidth : 0.4;
uniform float distortion : 1.0;
varying vec2 v_Texcoord;
@import clay.util.rgbm
vec4 textureDistorted(
 in vec2 texcoord,
 in vec2 direction,
 in vec3 distortion
) {
 return vec4(
 decodeHDR(texture2D(texture, texcoord + direction * distortion.r)).r,
 decodeHDR(texture2D(texture, texcoord + direction * distortion.g)).g,
 decodeHDR(texture2D(texture, texcoord + direction * distortion.b)).b,
 1.0
 );
}
void main()
{
 vec2 texcoord = -v_Texcoord + vec2(1.0); vec2 textureOffset = 1.0 / textureSize;
 vec2 ghostVec = (vec2(0.5) - texcoord) * dispersal;
 vec2 haloVec = normalize(ghostVec) * haloWidth;
 vec3 distortion = vec3(-textureOffset.x * distortion, 0.0, textureOffset.x * distortion);
 vec4 result = vec4(0.0);
 for (int i = 0; i < SAMPLE_NUMBER; i++)
 {
 vec2 offset = fract(texcoord + ghostVec * float(i));
 float weight = length(vec2(0.5) - offset) / length(vec2(0.5));
 weight = pow(1.0 - weight, 10.0);
 result += textureDistorted(offset, normalize(ghostVec), distortion) * weight;
 }
 result *= texture2D(lenscolor, vec2(length(vec2(0.5) - texcoord)) / length(vec2(0.5)));
 float weight = length(vec2(0.5) - fract(texcoord + haloVec)) / length(vec2(0.5));
 weight = pow(1.0 - weight, 10.0);
 vec2 offset = fract(texcoord + haloVec);
 result += textureDistorted(offset, normalize(ghostVec), distortion) * weight;
 gl_FragColor = result;
}
@end`, vs = `@export clay.compositor.blend
#define SHADER_NAME blend
#ifdef TEXTURE1_ENABLED
uniform sampler2D texture1;
uniform float weight1 : 1.0;
#endif
#ifdef TEXTURE2_ENABLED
uniform sampler2D texture2;
uniform float weight2 : 1.0;
#endif
#ifdef TEXTURE3_ENABLED
uniform sampler2D texture3;
uniform float weight3 : 1.0;
#endif
#ifdef TEXTURE4_ENABLED
uniform sampler2D texture4;
uniform float weight4 : 1.0;
#endif
#ifdef TEXTURE5_ENABLED
uniform sampler2D texture5;
uniform float weight5 : 1.0;
#endif
#ifdef TEXTURE6_ENABLED
uniform sampler2D texture6;
uniform float weight6 : 1.0;
#endif
varying vec2 v_Texcoord;
@import clay.util.rgbm
void main()
{
 vec4 tex = vec4(0.0);
#ifdef TEXTURE1_ENABLED
 tex += decodeHDR(texture2D(texture1, v_Texcoord)) * weight1;
#endif
#ifdef TEXTURE2_ENABLED
 tex += decodeHDR(texture2D(texture2, v_Texcoord)) * weight2;
#endif
#ifdef TEXTURE3_ENABLED
 tex += decodeHDR(texture2D(texture3, v_Texcoord)) * weight3;
#endif
#ifdef TEXTURE4_ENABLED
 tex += decodeHDR(texture2D(texture4, v_Texcoord)) * weight4;
#endif
#ifdef TEXTURE5_ENABLED
 tex += decodeHDR(texture2D(texture5, v_Texcoord)) * weight5;
#endif
#ifdef TEXTURE6_ENABLED
 tex += decodeHDR(texture2D(texture6, v_Texcoord)) * weight6;
#endif
 gl_FragColor = encodeHDR(tex);
}
@end`, ps = `@export clay.compositor.fxaa
uniform sampler2D texture;
uniform vec4 viewport : VIEWPORT;
varying vec2 v_Texcoord;
#define FXAA_REDUCE_MIN (1.0/128.0)
#define FXAA_REDUCE_MUL (1.0/8.0)
#define FXAA_SPAN_MAX 8.0
@import clay.util.rgbm
void main()
{
 vec2 resolution = 1.0 / viewport.zw;
 vec3 rgbNW = decodeHDR( texture2D( texture, ( gl_FragCoord.xy + vec2( -1.0, -1.0 ) ) * resolution ) ).xyz;
 vec3 rgbNE = decodeHDR( texture2D( texture, ( gl_FragCoord.xy + vec2( 1.0, -1.0 ) ) * resolution ) ).xyz;
 vec3 rgbSW = decodeHDR( texture2D( texture, ( gl_FragCoord.xy + vec2( -1.0, 1.0 ) ) * resolution ) ).xyz;
 vec3 rgbSE = decodeHDR( texture2D( texture, ( gl_FragCoord.xy + vec2( 1.0, 1.0 ) ) * resolution ) ).xyz;
 vec4 rgbaM = decodeHDR( texture2D( texture, gl_FragCoord.xy * resolution ) );
 vec3 rgbM = rgbaM.xyz;
 float opacity = rgbaM.w;
 vec3 luma = vec3( 0.299, 0.587, 0.114 );
 float lumaNW = dot( rgbNW, luma );
 float lumaNE = dot( rgbNE, luma );
 float lumaSW = dot( rgbSW, luma );
 float lumaSE = dot( rgbSE, luma );
 float lumaM = dot( rgbM, luma );
 float lumaMin = min( lumaM, min( min( lumaNW, lumaNE ), min( lumaSW, lumaSE ) ) );
 float lumaMax = max( lumaM, max( max( lumaNW, lumaNE) , max( lumaSW, lumaSE ) ) );
 vec2 dir;
 dir.x = -((lumaNW + lumaNE) - (lumaSW + lumaSE));
 dir.y = ((lumaNW + lumaSW) - (lumaNE + lumaSE));
 float dirReduce = max( ( lumaNW + lumaNE + lumaSW + lumaSE ) * ( 0.25 * FXAA_REDUCE_MUL ), FXAA_REDUCE_MIN );
 float rcpDirMin = 1.0 / ( min( abs( dir.x ), abs( dir.y ) ) + dirReduce );
 dir = min( vec2( FXAA_SPAN_MAX, FXAA_SPAN_MAX),
 max( vec2(-FXAA_SPAN_MAX, -FXAA_SPAN_MAX),
 dir * rcpDirMin)) * resolution;
 vec3 rgbA = decodeHDR( texture2D( texture, gl_FragCoord.xy * resolution + dir * ( 1.0 / 3.0 - 0.5 ) ) ).xyz;
 rgbA += decodeHDR( texture2D( texture, gl_FragCoord.xy * resolution + dir * ( 2.0 / 3.0 - 0.5 ) ) ).xyz;
 rgbA *= 0.5;
 vec3 rgbB = decodeHDR( texture2D( texture, gl_FragCoord.xy * resolution + dir * -0.5 ) ).xyz;
 rgbB += decodeHDR( texture2D( texture, gl_FragCoord.xy * resolution + dir * 0.5 ) ).xyz;
 rgbB *= 0.25;
 rgbB += rgbA * 0.5;
 float lumaB = dot( rgbB, luma );
 if ( ( lumaB < lumaMin ) || ( lumaB > lumaMax ) )
 {
 gl_FragColor = vec4( rgbA, opacity );
 }
 else {
 gl_FragColor = vec4( rgbB, opacity );
 }
}
@end`;
function yc(e) {
  e.import(pc), e.import(ss), e.import(mc), e.import(ls), e.import(_c), e.import(us), e.import(fs), e.import(hs), e.import(cs), e.import(ds), e.import(gc), e.import(vs), e.import(ps);
}
yc(U);
var xc = /^#source\((.*?)\)/;
function Tc(e, t) {
  var r = new hc();
  t = t || {};
  var i = {
    textures: {},
    parameters: {}
  }, n = function(s, l) {
    for (var u = 0; u < e.nodes.length; u++) {
      var f = e.nodes[u], h = wc(f, i, t);
      h && r.addNode(h);
    }
  };
  for (var a in e.parameters) {
    var o = e.parameters[a];
    i.parameters[a] = kn(o);
  }
  return Sc(e, i, t, function(s) {
    i.textures = s, n();
  }), r;
}
function wc(e, t, r) {
  var i = e.type || "filter", n, a, o;
  if (i === "filter") {
    var s = e.shader.trim(), l = xc.exec(s);
    if (l ? n = U.source(l[1].trim()) : s.charAt(0) === "#" && (n = t.shaders[s.substr(1)]), n || (n = s), !n)
      return;
  }
  if (e.inputs) {
    a = {};
    for (var u in e.inputs)
      typeof e.inputs[u] == "string" ? a[u] = e.inputs[u] : a[u] = {
        node: e.inputs[u].node,
        pin: e.inputs[u].pin
      };
  }
  if (e.outputs) {
    o = {};
    for (var u in e.outputs) {
      var f = e.outputs[u];
      o[u] = {}, f.attachment != null && (o[u].attachment = f.attachment), f.keepLastFrame != null && (o[u].keepLastFrame = f.keepLastFrame), f.outputLastFrame != null && (o[u].outputLastFrame = f.outputLastFrame), f.parameters && (o[u].parameters = kn(f.parameters));
    }
  }
  var h;
  if (i === "scene" ? h = new cc({
    name: e.name,
    scene: r.scene,
    camera: r.camera,
    outputs: o
  }) : i === "texture" ? h = new dc({
    name: e.name,
    outputs: o
  }) : h = new vc({
    name: e.name,
    shader: n,
    inputs: a,
    outputs: o
  }), h) {
    if (e.parameters)
      for (var u in e.parameters) {
        var d = e.parameters[u];
        typeof d == "string" ? (d = d.trim(), d.charAt(0) === "#" ? d = t.textures[d.substr(1)] : h.on(
          "beforerender",
          Ac(
            u,
            ms(d)
          )
        )) : typeof d == "function" && h.on("beforerender", d), h.setParameter(u, d);
      }
    if (e.defines && h.pass)
      for (var u in e.defines) {
        var d = e.defines[u];
        h.pass.material.define("fragment", u, d);
      }
  }
  return h;
}
function bc(e, t) {
  return e;
}
function Ec(e, t) {
  return t;
}
function kn(e) {
  var t = {};
  if (!e)
    return t;
  ["type", "minFilter", "magFilter", "wrapS", "wrapT", "flipY", "useMipmap"].forEach(function(i) {
    var n = e[i];
    n != null && (typeof n == "string" && (n = N[n]), t[i] = n);
  });
  var r = e.scale || 1;
  return ["width", "height"].forEach(function(i) {
    if (e[i] != null) {
      var n = e[i];
      typeof n == "string" ? (n = n.trim(), t[i] = Cc(
        i,
        ms(n),
        r
      )) : t[i] = n;
    }
  }), t.width || (t.width = bc), t.height || (t.height = Ec), e.useMipmap != null && (t.useMipmap = e.useMipmap), t;
}
function Sc(e, t, r, i) {
  if (!e.textures) {
    i({});
    return;
  }
  var n = {}, a = 0, o = !1, s = r.textureRootPath;
  he.each(e.textures, function(l, u) {
    var f, h = l.path, d = kn(l.parameters);
    if (Array.isArray(h) && h.length === 6)
      s && (h = h.map(function(c) {
        return he.relative2absolute(c, s);
      })), f = new Rt(d);
    else if (typeof h == "string")
      s && (h = he.relative2absolute(h, s)), f = new K(d);
    else
      return;
    f.load(h), a++, f.once("success", function() {
      n[u] = f, a--, a === 0 && (i(n), o = !0);
    });
  }), a === 0 && !o && i(n);
}
function Ac(e, t) {
  return function(r) {
    var i = r.getDevicePixelRatio(), n = r.getWidth(), a = r.getHeight(), o = t(n, a, i);
    this.setParameter(e, o);
  };
}
function Cc(e, t, r) {
  return r = r || 1, function(i) {
    var n = i.getDevicePixelRatio(), a = i.getWidth() * r, o = i.getHeight() * r;
    return t(a, o, n);
  };
}
function ms(e) {
  var t = /^expr\((.*)\)$/.exec(e);
  if (t)
    try {
      var r = new Function("width", "height", "dpr", "return " + t[1]);
      return r(1, 1), r;
    } catch {
      throw new Error("Invalid expression.");
    }
}
function $t(e, t) {
  for (var r = 0, i = 1 / t, n = e; n > 0; )
    r = r + i * (n % t), n = Math.floor(n / t), i = i / t;
  return r;
}
const Mc = `@export ecgl.ssao.estimate

uniform sampler2D depthTex;

uniform sampler2D normalTex;

uniform sampler2D noiseTex;

uniform vec2 depthTexSize;

uniform vec2 noiseTexSize;

uniform mat4 projection;

uniform mat4 projectionInv;

uniform mat4 viewInverseTranspose;

uniform vec3 kernel[KERNEL_SIZE];

uniform float radius : 1;

uniform float power : 1;

uniform float bias: 1e-2;

uniform float intensity: 1.0;

varying vec2 v_Texcoord;

float ssaoEstimator(in vec3 originPos, in mat3 kernelBasis) {
 float occlusion = 0.0;

 for (int i = 0; i < KERNEL_SIZE; i++) {
 vec3 samplePos = kernel[i];
#ifdef NORMALTEX_ENABLED
 samplePos = kernelBasis * samplePos;
#endif
 samplePos = samplePos * radius + originPos;

 vec4 texCoord = projection * vec4(samplePos, 1.0);
 texCoord.xy /= texCoord.w;

 vec4 depthTexel = texture2D(depthTex, texCoord.xy * 0.5 + 0.5);

 float sampleDepth = depthTexel.r * 2.0 - 1.0;
 if (projection[3][3] == 0.0) {
 sampleDepth = projection[3][2] / (sampleDepth * projection[2][3] - projection[2][2]);
 }
 else {
 sampleDepth = (sampleDepth - projection[3][2]) / projection[2][2];
 }
 
 float rangeCheck = smoothstep(0.0, 1.0, radius / abs(originPos.z - sampleDepth));
 occlusion += rangeCheck * step(samplePos.z, sampleDepth - bias);
 }
#ifdef NORMALTEX_ENABLED
 occlusion = 1.0 - occlusion / float(KERNEL_SIZE);
#else
 occlusion = 1.0 - clamp((occlusion / float(KERNEL_SIZE) - 0.6) * 2.5, 0.0, 1.0);
#endif
 return pow(occlusion, power);
}

void main()
{

 vec4 depthTexel = texture2D(depthTex, v_Texcoord);

#ifdef NORMALTEX_ENABLED
 vec4 tex = texture2D(normalTex, v_Texcoord);
 if (dot(tex.rgb, tex.rgb) == 0.0) {
 gl_FragColor = vec4(1.0);
 return;
 }
 vec3 N = tex.rgb * 2.0 - 1.0;
 N = (viewInverseTranspose * vec4(N, 0.0)).xyz;

 vec2 noiseTexCoord = depthTexSize / vec2(noiseTexSize) * v_Texcoord;
 vec3 rvec = texture2D(noiseTex, noiseTexCoord).rgb * 2.0 - 1.0;
 vec3 T = normalize(rvec - N * dot(rvec, N));
 vec3 BT = normalize(cross(N, T));
 mat3 kernelBasis = mat3(T, BT, N);
#else
 if (depthTexel.r > 0.99999) {
 gl_FragColor = vec4(1.0);
 return;
 }
 mat3 kernelBasis;
#endif

 float z = depthTexel.r * 2.0 - 1.0;

 vec4 projectedPos = vec4(v_Texcoord * 2.0 - 1.0, z, 1.0);
 vec4 p4 = projectionInv * projectedPos;

 vec3 position = p4.xyz / p4.w;

 float ao = ssaoEstimator(position, kernelBasis);
 ao = clamp(1.0 - (1.0 - ao) * intensity, 0.0, 1.0);
 gl_FragColor = vec4(vec3(ao), 1.0);
}

@end


@export ecgl.ssao.blur
#define SHADER_NAME SSAO_BLUR

uniform sampler2D ssaoTexture;

#ifdef NORMALTEX_ENABLED
uniform sampler2D normalTex;
#endif

varying vec2 v_Texcoord;

uniform vec2 textureSize;
uniform float blurSize : 1.0;

uniform int direction: 0.0;

#ifdef DEPTHTEX_ENABLED
uniform sampler2D depthTex;
uniform mat4 projection;
uniform float depthRange : 0.5;

float getLinearDepth(vec2 coord)
{
 float depth = texture2D(depthTex, coord).r * 2.0 - 1.0;
 return projection[3][2] / (depth * projection[2][3] - projection[2][2]);
}
#endif

void main()
{
 float kernel[5];
 kernel[0] = 0.122581;
 kernel[1] = 0.233062;
 kernel[2] = 0.288713;
 kernel[3] = 0.233062;
 kernel[4] = 0.122581;

 vec2 off = vec2(0.0);
 if (direction == 0) {
 off[0] = blurSize / textureSize.x;
 }
 else {
 off[1] = blurSize / textureSize.y;
 }

 vec2 coord = v_Texcoord;

 float sum = 0.0;
 float weightAll = 0.0;

#ifdef NORMALTEX_ENABLED
 vec3 centerNormal = texture2D(normalTex, v_Texcoord).rgb * 2.0 - 1.0;
#endif
#if defined(DEPTHTEX_ENABLED)
 float centerDepth = getLinearDepth(v_Texcoord);
#endif

 for (int i = 0; i < 5; i++) {
 vec2 coord = clamp(v_Texcoord + vec2(float(i) - 2.0) * off, vec2(0.0), vec2(1.0));

 float w = kernel[i];
#ifdef NORMALTEX_ENABLED
 vec3 normal = texture2D(normalTex, coord).rgb * 2.0 - 1.0;
 w *= clamp(dot(normal, centerNormal), 0.0, 1.0);
#endif
#ifdef DEPTHTEX_ENABLED
 float d = getLinearDepth(coord);
 w *= (1.0 - smoothstep(abs(centerDepth - d) / depthRange, 0.0, 1.0));
#endif

 weightAll += w;
 sum += texture2D(ssaoTexture, coord).r * w;
 }

 gl_FragColor = vec4(vec3(sum / weightAll), 1.0);
}

@end
`;
U.import(Mc);
function _s(e) {
  for (var t = new Uint8Array(e * e * 4), r = 0, i = new S(), n = 0; n < e; n++)
    for (var a = 0; a < e; a++)
      i.set(Math.random() * 2 - 1, Math.random() * 2 - 1, 0).normalize(), t[r++] = (i.x * 0.5 + 0.5) * 255, t[r++] = (i.y * 0.5 + 0.5) * 255, t[r++] = 0, t[r++] = 255;
  return t;
}
function uo(e) {
  return new K({
    pixels: _s(e),
    wrapS: N.REPEAT,
    wrapT: N.REPEAT,
    width: e,
    height: e
  });
}
function Lc(e, t, r) {
  var i = new Float32Array(e * 3);
  t = t || 0;
  for (var n = 0; n < e; n++) {
    var a = $t(n + t, 2) * (r ? 1 : 2) * Math.PI, o = $t(n + t, 3) * Math.PI, s = Math.random(), l = Math.cos(a) * Math.sin(o) * s, u = Math.cos(o) * s, f = Math.sin(a) * Math.sin(o) * s;
    i[n * 3] = l, i[n * 3 + 1] = u, i[n * 3 + 2] = f;
  }
  return i;
}
function ht(e) {
  e = e || {}, this._ssaoPass = new De({
    fragment: U.source("ecgl.ssao.estimate")
  }), this._blurPass = new De({
    fragment: U.source("ecgl.ssao.blur")
  }), this._framebuffer = new pe({
    depthBuffer: !1
  }), this._ssaoTexture = new K(), this._blurTexture = new K(), this._blurTexture2 = new K(), this._depthTex = e.depthTexture, this._normalTex = e.normalTexture, this.setNoiseSize(4), this.setKernelSize(e.kernelSize || 12), e.radius != null && this.setParameter("radius", e.radius), e.power != null && this.setParameter("power", e.power), this._normalTex || (this._ssaoPass.material.disableTexture("normalTex"), this._blurPass.material.disableTexture("normalTex")), this._depthTex || this._blurPass.material.disableTexture("depthTex"), this._blurPass.material.setUniform("normalTex", this._normalTex), this._blurPass.material.setUniform("depthTex", this._depthTex);
}
ht.prototype.setDepthTexture = function(e) {
  this._depthTex = e;
};
ht.prototype.setNormalTexture = function(e) {
  this._normalTex = e, this._ssaoPass.material[e ? "enableTexture" : "disableTexture"]("normalTex"), this.setKernelSize(this._kernelSize);
};
ht.prototype.update = function(e, t, r) {
  var i = e.getWidth(), n = e.getHeight(), a = this._ssaoPass, o = this._blurPass;
  a.setUniform("kernel", this._kernels[r % this._kernels.length]), a.setUniform("depthTex", this._depthTex), this._normalTex != null && a.setUniform("normalTex", this._normalTex), a.setUniform("depthTexSize", [this._depthTex.width, this._depthTex.height]);
  var s = new I();
  I.transpose(s, t.worldTransform), a.setUniform("projection", t.projectionMatrix.array), a.setUniform("projectionInv", t.invProjectionMatrix.array), a.setUniform("viewInverseTranspose", s.array);
  var l = this._ssaoTexture, u = this._blurTexture, f = this._blurTexture2;
  l.width = i / 2, l.height = n / 2, u.width = i, u.height = n, f.width = i, f.height = n, this._framebuffer.attach(l), this._framebuffer.bind(e), e.gl.clearColor(1, 1, 1, 1), e.gl.clear(e.gl.COLOR_BUFFER_BIT), a.render(e), o.setUniform("textureSize", [i / 2, n / 2]), o.setUniform("projection", t.projectionMatrix.array), this._framebuffer.attach(u), o.setUniform("direction", 0), o.setUniform("ssaoTexture", l), o.render(e), this._framebuffer.attach(f), o.setUniform("textureSize", [i, n]), o.setUniform("direction", 1), o.setUniform("ssaoTexture", u), o.render(e), this._framebuffer.unbind(e);
  var h = e.clearColor;
  e.gl.clearColor(h[0], h[1], h[2], h[3]);
};
ht.prototype.getTargetTexture = function() {
  return this._blurTexture2;
};
ht.prototype.setParameter = function(e, t) {
  e === "noiseTexSize" ? this.setNoiseSize(t) : e === "kernelSize" ? this.setKernelSize(t) : e === "intensity" ? this._ssaoPass.material.set("intensity", t) : this._ssaoPass.setUniform(e, t);
};
ht.prototype.setKernelSize = function(e) {
  this._kernelSize = e, this._ssaoPass.material.define("fragment", "KERNEL_SIZE", e), this._kernels = this._kernels || [];
  for (var t = 0; t < 30; t++)
    this._kernels[t] = Lc(e, t * e, !!this._normalTex);
};
ht.prototype.setNoiseSize = function(e) {
  var t = this._ssaoPass.getUniform("noiseTex");
  t ? (t.data = _s(e), t.width = t.height = e, t.dirty()) : (t = uo(e), this._ssaoPass.setUniform("noiseTex", uo(e))), this._ssaoPass.setUniform("noiseTexSize", [e, e]);
};
ht.prototype.dispose = function(e) {
  this._blurTexture.dispose(e), this._ssaoTexture.dispose(e), this._blurTexture2.dispose(e);
};
const Dc = `@export ecgl.ssr.main

#define SHADER_NAME SSR
#define MAX_ITERATION 20;
#define SAMPLE_PER_FRAME 5;
#define TOTAL_SAMPLES 128;

uniform sampler2D sourceTexture;
uniform sampler2D gBufferTexture1;
uniform sampler2D gBufferTexture2;
uniform sampler2D gBufferTexture3;
uniform samplerCube specularCubemap;
uniform float specularIntensity: 1;

uniform mat4 projection;
uniform mat4 projectionInv;
uniform mat4 toViewSpace;
uniform mat4 toWorldSpace;

uniform float maxRayDistance: 200;

uniform float pixelStride: 16;
uniform float pixelStrideZCutoff: 50; 
uniform float screenEdgeFadeStart: 0.9; 
uniform float eyeFadeStart : 0.2; uniform float eyeFadeEnd: 0.8; 
uniform float minGlossiness: 0.2; uniform float zThicknessThreshold: 1;

uniform float nearZ;
uniform vec2 viewportSize : VIEWPORT_SIZE;

uniform float jitterOffset: 0;

varying vec2 v_Texcoord;

#ifdef DEPTH_DECODE
@import clay.util.decode_float
#endif

#ifdef PHYSICALLY_CORRECT
uniform sampler2D normalDistribution;
uniform float sampleOffset: 0;
uniform vec2 normalDistributionSize;

vec3 transformNormal(vec3 H, vec3 N) {
 vec3 upVector = N.y > 0.999 ? vec3(1.0, 0.0, 0.0) : vec3(0.0, 1.0, 0.0);
 vec3 tangentX = normalize(cross(N, upVector));
 vec3 tangentZ = cross(N, tangentX);
 return normalize(tangentX * H.x + N * H.y + tangentZ * H.z);
}
vec3 importanceSampleNormalGGX(float i, float roughness, vec3 N) {
 float p = fract((i + sampleOffset) / float(TOTAL_SAMPLES));
 vec3 H = texture2D(normalDistribution,vec2(roughness, p)).rgb;
 return transformNormal(H, N);
}
float G_Smith(float g, float ndv, float ndl) {
 float roughness = 1.0 - g;
 float k = roughness * roughness / 2.0;
 float G1V = ndv / (ndv * (1.0 - k) + k);
 float G1L = ndl / (ndl * (1.0 - k) + k);
 return G1L * G1V;
}
vec3 F_Schlick(float ndv, vec3 spec) {
 return spec + (1.0 - spec) * pow(1.0 - ndv, 5.0);
}
#endif

float fetchDepth(sampler2D depthTexture, vec2 uv)
{
 vec4 depthTexel = texture2D(depthTexture, uv);
 return depthTexel.r * 2.0 - 1.0;
}

float linearDepth(float depth)
{
 if (projection[3][3] == 0.0) {
 return projection[3][2] / (depth * projection[2][3] - projection[2][2]);
 }
 else {
 return (depth - projection[3][2]) / projection[2][2];
 }
}

bool rayIntersectDepth(float rayZNear, float rayZFar, vec2 hitPixel)
{
 if (rayZFar > rayZNear)
 {
 float t = rayZFar; rayZFar = rayZNear; rayZNear = t;
 }
 float cameraZ = linearDepth(fetchDepth(gBufferTexture2, hitPixel));
 return rayZFar <= cameraZ && rayZNear >= cameraZ - zThicknessThreshold;
}


bool traceScreenSpaceRay(
 vec3 rayOrigin, vec3 rayDir, float jitter,
 out vec2 hitPixel, out vec3 hitPoint, out float iterationCount
)
{
 float rayLength = ((rayOrigin.z + rayDir.z * maxRayDistance) > -nearZ)
 ? (-nearZ - rayOrigin.z) / rayDir.z : maxRayDistance;

 vec3 rayEnd = rayOrigin + rayDir * rayLength;

 vec4 H0 = projection * vec4(rayOrigin, 1.0);
 vec4 H1 = projection * vec4(rayEnd, 1.0);

 float k0 = 1.0 / H0.w, k1 = 1.0 / H1.w;

 vec3 Q0 = rayOrigin * k0, Q1 = rayEnd * k1;

 vec2 P0 = (H0.xy * k0 * 0.5 + 0.5) * viewportSize;
 vec2 P1 = (H1.xy * k1 * 0.5 + 0.5) * viewportSize;

 P1 += dot(P1 - P0, P1 - P0) < 0.0001 ? 0.01 : 0.0;
 vec2 delta = P1 - P0;

 bool permute = false;
 if (abs(delta.x) < abs(delta.y)) {
 permute = true;
 delta = delta.yx;
 P0 = P0.yx;
 P1 = P1.yx;
 }
 float stepDir = sign(delta.x);
 float invdx = stepDir / delta.x;

 vec3 dQ = (Q1 - Q0) * invdx;
 float dk = (k1 - k0) * invdx;

 vec2 dP = vec2(stepDir, delta.y * invdx);

 float strideScaler = 1.0 - min(1.0, -rayOrigin.z / pixelStrideZCutoff);
 float pixStride = 1.0 + strideScaler * pixelStride;

 dP *= pixStride; dQ *= pixStride; dk *= pixStride;

 vec4 pqk = vec4(P0, Q0.z, k0);
 vec4 dPQK = vec4(dP, dQ.z, dk);

 pqk += dPQK * jitter;
 float rayZFar = (dPQK.z * 0.5 + pqk.z) / (dPQK.w * 0.5 + pqk.w);
 float rayZNear;

 bool intersect = false;

 vec2 texelSize = 1.0 / viewportSize;

 iterationCount = 0.0;

 for (int i = 0; i < MAX_ITERATION; i++)
 {
 pqk += dPQK;

 rayZNear = rayZFar;
 rayZFar = (dPQK.z * 0.5 + pqk.z) / (dPQK.w * 0.5 + pqk.w);

 hitPixel = permute ? pqk.yx : pqk.xy;
 hitPixel *= texelSize;

 intersect = rayIntersectDepth(rayZNear, rayZFar, hitPixel);

 iterationCount += 1.0;

 dPQK *= 1.2;

 if (intersect) {
 break;
 }
 }

 Q0.xy += dQ.xy * iterationCount;
 Q0.z = pqk.z;
 hitPoint = Q0 / pqk.w;

 return intersect;
}

float calculateAlpha(
 float iterationCount, float reflectivity,
 vec2 hitPixel, vec3 hitPoint, float dist, vec3 rayDir
)
{
 float alpha = clamp(reflectivity, 0.0, 1.0);
 alpha *= 1.0 - (iterationCount / float(MAX_ITERATION));
 vec2 hitPixelNDC = hitPixel * 2.0 - 1.0;
 float maxDimension = min(1.0, max(abs(hitPixelNDC.x), abs(hitPixelNDC.y)));
 alpha *= 1.0 - max(0.0, maxDimension - screenEdgeFadeStart) / (1.0 - screenEdgeFadeStart);

 float _eyeFadeStart = eyeFadeStart;
 float _eyeFadeEnd = eyeFadeEnd;
 if (_eyeFadeStart > _eyeFadeEnd) {
 float tmp = _eyeFadeEnd;
 _eyeFadeEnd = _eyeFadeStart;
 _eyeFadeStart = tmp;
 }

 float eyeDir = clamp(rayDir.z, _eyeFadeStart, _eyeFadeEnd);
 alpha *= 1.0 - (eyeDir - _eyeFadeStart) / (_eyeFadeEnd - _eyeFadeStart);

 alpha *= 1.0 - clamp(dist / maxRayDistance, 0.0, 1.0);

 return alpha;
}

@import clay.util.rand

@import clay.util.rgbm

void main()
{
 vec4 normalAndGloss = texture2D(gBufferTexture1, v_Texcoord);

 if (dot(normalAndGloss.rgb, vec3(1.0)) == 0.0) {
 discard;
 }

 float g = normalAndGloss.a;
#if !defined(PHYSICALLY_CORRECT)
 if (g <= minGlossiness) {
 discard;
 }
#endif

 float reflectivity = (g - minGlossiness) / (1.0 - minGlossiness);

 vec3 N = normalize(normalAndGloss.rgb * 2.0 - 1.0);
 N = normalize((toViewSpace * vec4(N, 0.0)).xyz);

 vec4 projectedPos = vec4(v_Texcoord * 2.0 - 1.0, fetchDepth(gBufferTexture2, v_Texcoord), 1.0);
 vec4 pos = projectionInv * projectedPos;
 vec3 rayOrigin = pos.xyz / pos.w;
 vec3 V = -normalize(rayOrigin);

 float ndv = clamp(dot(N, V), 0.0, 1.0);
 float iterationCount;
 float jitter = rand(fract(v_Texcoord + jitterOffset));

#ifdef PHYSICALLY_CORRECT
 vec4 color = vec4(vec3(0.0), 1.0);
 vec4 albedoMetalness = texture2D(gBufferTexture3, v_Texcoord);
 vec3 albedo = albedoMetalness.rgb;
 float m = albedoMetalness.a;
 vec3 diffuseColor = albedo * (1.0 - m);
 vec3 spec = mix(vec3(0.04), albedo, m);

 float jitter2 = rand(fract(v_Texcoord)) * float(TOTAL_SAMPLES);

 for (int i = 0; i < SAMPLE_PER_FRAME; i++) {
 vec3 H = importanceSampleNormalGGX(float(i) + jitter2, 1.0 - g, N);
 vec3 rayDir = normalize(reflect(-V, H));
#else
 vec3 rayDir = normalize(reflect(-V, N));
#endif
 vec2 hitPixel;
 vec3 hitPoint;

 bool intersect = traceScreenSpaceRay(rayOrigin, rayDir, jitter, hitPixel, hitPoint, iterationCount);

 float dist = distance(rayOrigin, hitPoint);

 vec3 hitNormal = texture2D(gBufferTexture1, hitPixel).rgb * 2.0 - 1.0;
 hitNormal = normalize((toViewSpace * vec4(hitNormal, 0.0)).xyz);
#ifdef PHYSICALLY_CORRECT
 float ndl = clamp(dot(N, rayDir), 0.0, 1.0);
 float vdh = clamp(dot(V, H), 0.0, 1.0);
 float ndh = clamp(dot(N, H), 0.0, 1.0);
 vec3 litTexel = vec3(0.0);
 if (dot(hitNormal, rayDir) < 0.0 && intersect) {
 litTexel = texture2D(sourceTexture, hitPixel).rgb;
 litTexel *= pow(clamp(1.0 - dist / 200.0, 0.0, 1.0), 3.0);

 }
 else {
 #ifdef SPECULARCUBEMAP_ENABLED
 vec3 rayDirW = normalize(toWorldSpace * vec4(rayDir, 0.0)).rgb;
 litTexel = RGBMDecode(textureCubeLodEXT(specularCubemap, rayDirW, 0.0), 8.12).rgb * specularIntensity;
#endif
 }
 color.rgb += ndl * litTexel * (
 F_Schlick(ndl, spec) * G_Smith(g, ndv, ndl) * vdh / (ndh * ndv + 0.001)
 );
 }
 color.rgb /= float(SAMPLE_PER_FRAME);
#else
 #if !defined(SPECULARCUBEMAP_ENABLED)
 if (dot(hitNormal, rayDir) >= 0.0) {
 discard;
 }
 if (!intersect) {
 discard;
 }
#endif
 float alpha = clamp(calculateAlpha(iterationCount, reflectivity, hitPixel, hitPoint, dist, rayDir), 0.0, 1.0);
 vec4 color = texture2D(sourceTexture, hitPixel);
 color.rgb *= alpha;

#ifdef SPECULARCUBEMAP_ENABLED
 vec3 rayDirW = normalize(toWorldSpace * vec4(rayDir, 0.0)).rgb;
 alpha = alpha * (intersect ? 1.0 : 0.0);
 float bias = (1.0 -g) * 5.0;
 color.rgb += (1.0 - alpha)
 * RGBMDecode(textureCubeLodEXT(specularCubemap, rayDirW, bias), 8.12).rgb
 * specularIntensity;
#endif

#endif

 gl_FragColor = encodeHDR(color);
}
@end

@export ecgl.ssr.blur

uniform sampler2D texture;
uniform sampler2D gBufferTexture1;
uniform sampler2D gBufferTexture2;
uniform mat4 projection;
uniform float depthRange : 0.05;

varying vec2 v_Texcoord;

uniform vec2 textureSize;
uniform float blurSize : 1.0;

#ifdef BLEND
 #ifdef SSAOTEX_ENABLED
uniform sampler2D ssaoTex;
 #endif
uniform sampler2D sourceTexture;
#endif

float getLinearDepth(vec2 coord)
{
 float depth = texture2D(gBufferTexture2, coord).r * 2.0 - 1.0;
 return projection[3][2] / (depth * projection[2][3] - projection[2][2]);
}

@import clay.util.rgbm


void main()
{
 @import clay.compositor.kernel.gaussian_9

 vec4 centerNTexel = texture2D(gBufferTexture1, v_Texcoord);
 float g = centerNTexel.a;
 float maxBlurSize = clamp(1.0 - g, 0.0, 1.0) * blurSize;
#ifdef VERTICAL
 vec2 off = vec2(0.0, maxBlurSize / textureSize.y);
#else
 vec2 off = vec2(maxBlurSize / textureSize.x, 0.0);
#endif

 vec2 coord = v_Texcoord;

 vec4 sum = vec4(0.0);
 float weightAll = 0.0;

 vec3 cN = centerNTexel.rgb * 2.0 - 1.0;
 float cD = getLinearDepth(v_Texcoord);
 for (int i = 0; i < 9; i++) {
 vec2 coord = clamp((float(i) - 4.0) * off + v_Texcoord, vec2(0.0), vec2(1.0));
 float w = gaussianKernel[i]
 * clamp(dot(cN, texture2D(gBufferTexture1, coord).rgb * 2.0 - 1.0), 0.0, 1.0);
 float d = getLinearDepth(coord);
 w *= (1.0 - smoothstep(abs(cD - d) / depthRange, 0.0, 1.0));

 weightAll += w;
 sum += decodeHDR(texture2D(texture, coord)) * w;
 }

#ifdef BLEND
 float aoFactor = 1.0;
 #ifdef SSAOTEX_ENABLED
 aoFactor = texture2D(ssaoTex, v_Texcoord).r;
 #endif
 gl_FragColor = encodeHDR(
 sum / weightAll * aoFactor + decodeHDR(texture2D(sourceTexture, v_Texcoord))
 );
#else
 gl_FragColor = encodeHDR(sum / weightAll);
#endif
}

@end`;
U.import(Dc);
function ct(e) {
  e = e || {}, this._ssrPass = new De({
    fragment: U.source("ecgl.ssr.main"),
    clearColor: [0, 0, 0, 0]
  }), this._blurPass1 = new De({
    fragment: U.source("ecgl.ssr.blur"),
    clearColor: [0, 0, 0, 0]
  }), this._blurPass2 = new De({
    fragment: U.source("ecgl.ssr.blur"),
    clearColor: [0, 0, 0, 0]
  }), this._blendPass = new De({
    fragment: U.source("clay.compositor.blend")
  }), this._blendPass.material.disableTexturesAll(), this._blendPass.material.enableTexture(["texture1", "texture2"]), this._ssrPass.setUniform("gBufferTexture1", e.normalTexture), this._ssrPass.setUniform("gBufferTexture2", e.depthTexture), this._blurPass1.setUniform("gBufferTexture1", e.normalTexture), this._blurPass1.setUniform("gBufferTexture2", e.depthTexture), this._blurPass2.setUniform("gBufferTexture1", e.normalTexture), this._blurPass2.setUniform("gBufferTexture2", e.depthTexture), this._blurPass2.material.define("fragment", "VERTICAL"), this._blurPass2.material.define("fragment", "BLEND"), this._ssrTexture = new K({
    type: N.HALF_FLOAT
  }), this._texture2 = new K({
    type: N.HALF_FLOAT
  }), this._texture3 = new K({
    type: N.HALF_FLOAT
  }), this._prevTexture = new K({
    type: N.HALF_FLOAT
  }), this._currentTexture = new K({
    type: N.HALF_FLOAT
  }), this._frameBuffer = new pe({
    depthBuffer: !1
  }), this._normalDistribution = null, this._totalSamples = 256, this._samplePerFrame = 4, this._ssrPass.material.define("fragment", "SAMPLE_PER_FRAME", this._samplePerFrame), this._ssrPass.material.define("fragment", "TOTAL_SAMPLES", this._totalSamples), this._downScale = 1;
}
ct.prototype.setAmbientCubemap = function(e, t) {
  this._ssrPass.material.set("specularCubemap", e), this._ssrPass.material.set("specularIntensity", t);
  var r = e && t;
  this._ssrPass.material[r ? "enableTexture" : "disableTexture"]("specularCubemap");
};
ct.prototype.update = function(e, t, r, i) {
  var n = e.getWidth(), a = e.getHeight(), o = this._ssrTexture, s = this._texture2, l = this._texture3;
  o.width = this._prevTexture.width = this._currentTexture.width = n / this._downScale, o.height = this._prevTexture.height = this._currentTexture.height = a / this._downScale, s.width = l.width = n, s.height = l.height = a;
  var u = this._frameBuffer, f = this._ssrPass, h = this._blurPass1, d = this._blurPass2, c = this._blendPass, v = new I(), p = new I();
  I.transpose(v, t.worldTransform), I.transpose(p, t.viewMatrix), f.setUniform("sourceTexture", r), f.setUniform("projection", t.projectionMatrix.array), f.setUniform("projectionInv", t.invProjectionMatrix.array), f.setUniform("toViewSpace", v.array), f.setUniform("toWorldSpace", p.array), f.setUniform("nearZ", t.near);
  var m = i / this._totalSamples * this._samplePerFrame;
  if (f.setUniform("jitterOffset", m), f.setUniform("sampleOffset", i * this._samplePerFrame), h.setUniform("textureSize", [o.width, o.height]), d.setUniform("textureSize", [n, a]), d.setUniform("sourceTexture", r), h.setUniform("projection", t.projectionMatrix.array), d.setUniform("projection", t.projectionMatrix.array), u.attach(o), u.bind(e), f.render(e), this._physicallyCorrect && (u.attach(this._currentTexture), c.setUniform("texture1", this._prevTexture), c.setUniform("texture2", o), c.material.set({
    weight1: i >= 1 ? 0.95 : 0,
    weight2: i >= 1 ? 0.05 : 1
    // weight1: frame >= 1 ? 1 : 0,
    // weight2: 1
  }), c.render(e)), u.attach(s), h.setUniform("texture", this._physicallyCorrect ? this._currentTexture : o), h.render(e), u.attach(l), d.setUniform("texture", s), d.render(e), u.unbind(e), this._physicallyCorrect) {
    var _ = this._prevTexture;
    this._prevTexture = this._currentTexture, this._currentTexture = _;
  }
};
ct.prototype.getTargetTexture = function() {
  return this._texture3;
};
ct.prototype.setParameter = function(e, t) {
  e === "maxIteration" ? this._ssrPass.material.define("fragment", "MAX_ITERATION", t) : this._ssrPass.setUniform(e, t);
};
ct.prototype.setPhysicallyCorrect = function(e) {
  e ? (this._normalDistribution || (this._normalDistribution = tt.generateNormalDistribution(64, this._totalSamples)), this._ssrPass.material.define("fragment", "PHYSICALLY_CORRECT"), this._ssrPass.material.set("normalDistribution", this._normalDistribution), this._ssrPass.material.set("normalDistributionSize", [64, this._totalSamples])) : this._ssrPass.material.undefine("fragment", "PHYSICALLY_CORRECT"), this._physicallyCorrect = e;
};
ct.prototype.setSSAOTexture = function(e) {
  var t = this._blurPass2;
  e ? (t.material.enableTexture("ssaoTex"), t.material.set("ssaoTex", e)) : t.material.disableTexture("ssaoTex");
};
ct.prototype.isFinished = function(e) {
  return this._physicallyCorrect ? e > this._totalSamples / this._samplePerFrame : !0;
};
ct.prototype.dispose = function(e) {
  this._ssrTexture.dispose(e), this._texture2.dispose(e), this._texture3.dispose(e), this._prevTexture.dispose(e), this._currentTexture.dispose(e), this._frameBuffer.dispose(e);
};
const fo = [0, 0, -0.321585265978, -0.154972575841, 0.458126042375, 0.188473391593, 0.842080129861, 0.527766490688, 0.147304551086, -0.659453822776, -0.331943915203, -0.940619700594, 0.0479226680259, 0.54812163202, 0.701581552186, -0.709825561388, -0.295436780218, 0.940589268233, -0.901489676764, 0.237713156085, 0.973570876096, -0.109899459384, -0.866792314779, -0.451805525005, 0.330975007087, 0.800048655954, -0.344275183665, 0.381779221166, -0.386139432542, -0.437418421534, -0.576478634965, -0.0148463392551, 0.385798197415, -0.262426961053, -0.666302061145, 0.682427250835, -0.628010632582, -0.732836215494, 0.10163141741, -0.987658134403, 0.711995289051, -0.320024291314, 0.0296005138058, 0.950296523438, 0.0130612307608, -0.351024443122, -0.879596633704, -0.10478487883, 0.435712737232, 0.504254490347, 0.779203817497, 0.206477676721, 0.388264289969, -0.896736162545, -0.153106280781, -0.629203242522, -0.245517550697, 0.657969239148, 0.126830499058, 0.26862328493, -0.634888119007, -0.302301223431, 0.617074219636, 0.779817204925], Pc = `@export ecgl.normal.vertex

@import ecgl.common.transformUniforms

@import ecgl.common.uv.header

@import ecgl.common.attributes

varying vec3 v_Normal;
varying vec3 v_WorldPosition;

@import ecgl.common.normalMap.vertexHeader

@import ecgl.common.vertexAnimation.header

void main()
{

 @import ecgl.common.vertexAnimation.main

 @import ecgl.common.uv.main

 v_Normal = normalize((worldInverseTranspose * vec4(normal, 0.0)).xyz);
 v_WorldPosition = (world * vec4(pos, 1.0)).xyz;

 @import ecgl.common.normalMap.vertexMain

 gl_Position = worldViewProjection * vec4(pos, 1.0);

}


@end


@export ecgl.normal.fragment

#define ROUGHNESS_CHANEL 0

uniform bool useBumpMap;
uniform bool useRoughnessMap;
uniform bool doubleSide;
uniform float roughness;

@import ecgl.common.uv.fragmentHeader

varying vec3 v_Normal;
varying vec3 v_WorldPosition;

uniform mat4 viewInverse : VIEWINVERSE;

@import ecgl.common.normalMap.fragmentHeader
@import ecgl.common.bumpMap.header

uniform sampler2D roughnessMap;

void main()
{
 vec3 N = v_Normal;
 
 bool flipNormal = false;
 if (doubleSide) {
 vec3 eyePos = viewInverse[3].xyz;
 vec3 V = normalize(eyePos - v_WorldPosition);

 if (dot(N, V) < 0.0) {
 flipNormal = true;
 }
 }

 @import ecgl.common.normalMap.fragmentMain

 if (useBumpMap) {
 N = bumpNormal(v_WorldPosition, v_Normal, N);
 }

 float g = 1.0 - roughness;

 if (useRoughnessMap) {
 float g2 = 1.0 - texture2D(roughnessMap, v_DetailTexcoord)[ROUGHNESS_CHANEL];
 g = clamp(g2 + (g - 0.5) * 2.0, 0.0, 1.0);
 }

 if (flipNormal) {
 N = -N;
 }

 gl_FragColor.rgb = (N.xyz + 1.0) * 0.5;
 gl_FragColor.a = g;
}
@end`;
U.import(Pc);
function ln(e, t, r, i, n) {
  var a = e.gl;
  t.setUniform(a, "1i", r, n), a.activeTexture(a.TEXTURE0 + n), i.isRenderable() ? i.bind(e) : i.unbind(e);
}
function Rc(e, t, r, i, n) {
  var a, o, s, l, u = e.gl;
  return function(f, h, d) {
    if (!(l && l.material === f.material)) {
      var c = f.material, v = f.__program, p = c.get("roughness");
      p == null && (p = 1);
      var m = c.get("normalMap") || t, _ = c.get("roughnessMap"), x = c.get("bumpMap"), y = c.get("uvRepeat"), g = c.get("uvOffset"), b = c.get("detailUvRepeat"), w = c.get("detailUvOffset"), C = !!x && c.isTextureEnabled("bumpMap"), A = !!_ && c.isTextureEnabled("roughnessMap"), R = c.isDefined("fragment", "DOUBLE_SIDED");
      x = x || r, _ = _ || i, d !== h ? (h.set("normalMap", m), h.set("bumpMap", x), h.set("roughnessMap", _), h.set("useBumpMap", C), h.set("useRoughnessMap", A), h.set("doubleSide", R), y != null && h.set("uvRepeat", y), g != null && h.set("uvOffset", g), b != null && h.set("detailUvRepeat", b), w != null && h.set("detailUvOffset", w), h.set("roughness", p)) : (v.setUniform(u, "1f", "roughness", p), a !== m && ln(e, v, "normalMap", m, 0), o !== x && x && ln(e, v, "bumpMap", x, 1), s !== _ && _ && ln(e, v, "roughnessMap", _, 2), y != null && v.setUniform(u, "2f", "uvRepeat", y), g != null && v.setUniform(u, "2f", "uvOffset", g), b != null && v.setUniform(u, "2f", "detailUvRepeat", b), w != null && v.setUniform(u, "2f", "detailUvOffset", w), v.setUniform(u, "1i", "useBumpMap", +C), v.setUniform(u, "1i", "useRoughnessMap", +A), v.setUniform(u, "1i", "doubleSide", +R)), a = m, o = x, s = _, l = f;
    }
  };
}
function er(e) {
  this._depthTex = new K({
    format: N.DEPTH_COMPONENT,
    type: N.UNSIGNED_INT
  }), this._normalTex = new K({
    type: N.HALF_FLOAT
  }), this._framebuffer = new pe(), this._framebuffer.attach(this._normalTex), this._framebuffer.attach(this._depthTex, pe.DEPTH_ATTACHMENT), this._normalMaterial = new ft({
    shader: new U(U.source("ecgl.normal.vertex"), U.source("ecgl.normal.fragment"))
  }), this._normalMaterial.enableTexture(["normalMap", "bumpMap", "roughnessMap"]), this._defaultNormalMap = et.createBlank("#000"), this._defaultBumpMap = et.createBlank("#000"), this._defaultRoughessMap = et.createBlank("#000"), this._debugPass = new De({
    fragment: U.source("clay.compositor.output")
  }), this._debugPass.setUniform("texture", this._normalTex), this._debugPass.material.undefine("fragment", "OUTPUT_ALPHA");
}
er.prototype.getDepthTexture = function() {
  return this._depthTex;
};
er.prototype.getNormalTexture = function() {
  return this._normalTex;
};
er.prototype.update = function(e, t, r) {
  var i = e.getWidth(), n = e.getHeight(), a = this._depthTex, o = this._normalTex, s = this._normalMaterial;
  a.width = i, a.height = n, o.width = i, o.height = n;
  var l = t.getRenderList(r).opaque;
  this._framebuffer.bind(e), e.gl.clearColor(0, 0, 0, 0), e.gl.clear(e.gl.COLOR_BUFFER_BIT | e.gl.DEPTH_BUFFER_BIT), e.gl.disable(e.gl.BLEND), e.renderPass(l, r, {
    getMaterial: function() {
      return s;
    },
    ifRender: function(u) {
      return u.renderNormal;
    },
    beforeRender: Rc(e, this._defaultNormalMap, this._defaultBumpMap, this._defaultRoughessMap, this._normalMaterial),
    sort: e.opaqueSortCompare
  }), this._framebuffer.unbind(e);
};
er.prototype.renderDebug = function(e) {
  this._debugPass.render(e);
};
er.prototype.dispose = function(e) {
  this._depthTex.dispose(e), this._normalTex.dispose(e);
};
function Mr(e) {
  e = e || {}, this._edgePass = new De({
    fragment: U.source("ecgl.edge")
  }), this._edgePass.setUniform("normalTexture", e.normalTexture), this._edgePass.setUniform("depthTexture", e.depthTexture), this._targetTexture = new K({
    type: N.HALF_FLOAT
  }), this._frameBuffer = new pe(), this._frameBuffer.attach(this._targetTexture);
}
Mr.prototype.update = function(e, t, r, i) {
  var n = e.getWidth(), a = e.getHeight(), o = this._targetTexture;
  o.width = n, o.height = a;
  var s = this._frameBuffer;
  s.bind(e), this._edgePass.setUniform("projectionInv", t.invProjectionMatrix.array), this._edgePass.setUniform("textureSize", [n, a]), this._edgePass.setUniform("texture", r), this._edgePass.render(e), s.unbind(e);
};
Mr.prototype.getTargetTexture = function() {
  return this._targetTexture;
};
Mr.prototype.setParameter = function(e, t) {
  this._edgePass.setUniform(e, t);
};
Mr.prototype.dispose = function(e) {
  this._targetTexture.dispose(e), this._frameBuffer.dispose(e);
};
const Nc = {
  nodes: [{
    name: "source",
    type: "texture",
    outputs: {
      color: {}
    }
  }, {
    name: "source_half",
    shader: "#source(clay.compositor.downsample)",
    inputs: {
      texture: "source"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 2)",
          height: "expr(height * 1.0 / 2)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0, height * 1.0] )"
    }
  }, {
    name: "bright",
    shader: "#source(clay.compositor.bright)",
    inputs: {
      texture: "source_half"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 2)",
          height: "expr(height * 1.0 / 2)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      threshold: 2,
      scale: 4,
      textureSize: "expr([width * 1.0 / 2, height / 2])"
    }
  }, {
    name: "bright_downsample_4",
    shader: "#source(clay.compositor.downsample)",
    inputs: {
      texture: "bright"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 4)",
          height: "expr(height * 1.0 / 4)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0 / 2, height / 2] )"
    }
  }, {
    name: "bright_downsample_8",
    shader: "#source(clay.compositor.downsample)",
    inputs: {
      texture: "bright_downsample_4"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 8)",
          height: "expr(height * 1.0 / 8)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0 / 4, height / 4] )"
    }
  }, {
    name: "bright_downsample_16",
    shader: "#source(clay.compositor.downsample)",
    inputs: {
      texture: "bright_downsample_8"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 16)",
          height: "expr(height * 1.0 / 16)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0 / 8, height / 8] )"
    }
  }, {
    name: "bright_downsample_32",
    shader: "#source(clay.compositor.downsample)",
    inputs: {
      texture: "bright_downsample_16"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 32)",
          height: "expr(height * 1.0 / 32)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0 / 16, height / 16] )"
    }
  }, {
    name: "bright_upsample_16_blur_h",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_downsample_32"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 16)",
          height: "expr(height * 1.0 / 16)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 0,
      textureSize: "expr( [width * 1.0 / 32, height / 32] )"
    }
  }, {
    name: "bright_upsample_16_blur_v",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_upsample_16_blur_h"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 16)",
          height: "expr(height * 1.0 / 16)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 1,
      textureSize: "expr( [width * 1.0 / 16, height * 1.0 / 16] )"
    }
  }, {
    name: "bright_upsample_8_blur_h",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_downsample_16"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 8)",
          height: "expr(height * 1.0 / 8)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 0,
      textureSize: "expr( [width * 1.0 / 16, height * 1.0 / 16] )"
    }
  }, {
    name: "bright_upsample_8_blur_v",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_upsample_8_blur_h"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 8)",
          height: "expr(height * 1.0 / 8)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 1,
      textureSize: "expr( [width * 1.0 / 8, height * 1.0 / 8] )"
    }
  }, {
    name: "bright_upsample_8_blend",
    shader: "#source(clay.compositor.blend)",
    inputs: {
      texture1: "bright_upsample_8_blur_v",
      texture2: "bright_upsample_16_blur_v"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 8)",
          height: "expr(height * 1.0 / 8)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      weight1: 0.3,
      weight2: 0.7
    }
  }, {
    name: "bright_upsample_4_blur_h",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_downsample_8"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 4)",
          height: "expr(height * 1.0 / 4)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 0,
      textureSize: "expr( [width * 1.0 / 8, height * 1.0 / 8] )"
    }
  }, {
    name: "bright_upsample_4_blur_v",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_upsample_4_blur_h"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 4)",
          height: "expr(height * 1.0 / 4)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 1,
      textureSize: "expr( [width * 1.0 / 4, height * 1.0 / 4] )"
    }
  }, {
    name: "bright_upsample_4_blend",
    shader: "#source(clay.compositor.blend)",
    inputs: {
      texture1: "bright_upsample_4_blur_v",
      texture2: "bright_upsample_8_blend"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 4)",
          height: "expr(height * 1.0 / 4)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      weight1: 0.3,
      weight2: 0.7
    }
  }, {
    name: "bright_upsample_2_blur_h",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_downsample_4"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 2)",
          height: "expr(height * 1.0 / 2)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 0,
      textureSize: "expr( [width * 1.0 / 4, height * 1.0 / 4] )"
    }
  }, {
    name: "bright_upsample_2_blur_v",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_upsample_2_blur_h"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 2)",
          height: "expr(height * 1.0 / 2)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 1,
      textureSize: "expr( [width * 1.0 / 2, height * 1.0 / 2] )"
    }
  }, {
    name: "bright_upsample_2_blend",
    shader: "#source(clay.compositor.blend)",
    inputs: {
      texture1: "bright_upsample_2_blur_v",
      texture2: "bright_upsample_4_blend"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0 / 2)",
          height: "expr(height * 1.0 / 2)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      weight1: 0.3,
      weight2: 0.7
    }
  }, {
    name: "bright_upsample_full_blur_h",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 0,
      textureSize: "expr( [width * 1.0 / 2, height * 1.0 / 2] )"
    }
  }, {
    name: "bright_upsample_full_blur_v",
    shader: "#source(clay.compositor.gaussian_blur)",
    inputs: {
      texture: "bright_upsample_full_blur_h"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      blurSize: 1,
      blurDir: 1,
      textureSize: "expr( [width * 1.0, height * 1.0] )"
    }
  }, {
    name: "bloom_composite",
    shader: "#source(clay.compositor.blend)",
    inputs: {
      texture1: "bright_upsample_full_blur_v",
      texture2: "bright_upsample_2_blend"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      weight1: 0.3,
      weight2: 0.7
    }
  }, {
    name: "coc",
    shader: "#source(ecgl.dof.coc)",
    outputs: {
      color: {
        parameters: {
          minFilter: "NEAREST",
          magFilter: "NEAREST",
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)"
        }
      }
    },
    parameters: {
      focalDist: 50,
      focalRange: 30
    }
  }, {
    name: "dof_far_blur",
    shader: "#source(ecgl.dof.diskBlur)",
    inputs: {
      texture: "source",
      coc: "coc"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0, height * 1.0] )"
    }
  }, {
    name: "dof_near_blur",
    shader: "#source(ecgl.dof.diskBlur)",
    inputs: {
      texture: "source",
      coc: "coc"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)",
          type: "HALF_FLOAT"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0, height * 1.0] )"
    },
    defines: {
      BLUR_NEARFIELD: null
    }
  }, {
    name: "dof_coc_blur",
    shader: "#source(ecgl.dof.diskBlur)",
    inputs: {
      texture: "coc"
    },
    outputs: {
      color: {
        parameters: {
          minFilter: "NEAREST",
          magFilter: "NEAREST",
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)"
        }
      }
    },
    parameters: {
      textureSize: "expr( [width * 1.0, height * 1.0] )"
    },
    defines: {
      BLUR_COC: null
    }
  }, {
    name: "dof_composite",
    shader: "#source(ecgl.dof.composite)",
    inputs: {
      original: "source",
      blurred: "dof_far_blur",
      nearfield: "dof_near_blur",
      coc: "coc",
      nearcoc: "dof_coc_blur"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)",
          type: "HALF_FLOAT"
        }
      }
    }
  }, {
    name: "composite",
    shader: "#source(clay.compositor.hdr.composite)",
    inputs: {
      texture: "source",
      bloom: "bloom_composite"
    },
    outputs: {
      color: {
        parameters: {
          width: "expr(width * 1.0)",
          height: "expr(height * 1.0)"
        }
      }
    },
    defines: {
      // Images are all premultiplied alpha before composite because of blending.
      // 'PREMULTIPLY_ALPHA': null,
      // 'DEBUG': 2
    }
  }, {
    name: "FXAA",
    shader: "#source(clay.compositor.fxaa)",
    inputs: {
      texture: "composite"
    }
  }]
}, Oc = `@export ecgl.dof.coc

uniform sampler2D depth;

uniform float zNear: 0.1;
uniform float zFar: 2000;

uniform float focalDistance: 3;
uniform float focalRange: 1;
uniform float focalLength: 30;
uniform float fstop: 2.8;

varying vec2 v_Texcoord;

@import clay.util.encode_float

void main()
{
 float z = texture2D(depth, v_Texcoord).r * 2.0 - 1.0;

 float dist = 2.0 * zNear * zFar / (zFar + zNear - z * (zFar - zNear));

 float aperture = focalLength / fstop;

 float coc;

 float uppper = focalDistance + focalRange;
 float lower = focalDistance - focalRange;
 if (dist <= uppper && dist >= lower) {
 coc = 0.5;
 }
 else {
 float focalAdjusted = dist > uppper ? uppper : lower;

 coc = abs(aperture * (focalLength * (dist - focalAdjusted)) / (dist * (focalAdjusted - focalLength)));
 coc = clamp(coc, 0.0, 2.0) / 2.00001;

 if (dist < lower) {
 coc = -coc;
 }
 coc = coc * 0.5 + 0.5;
 }

 gl_FragColor = encodeFloat(coc);
}
@end


@export ecgl.dof.composite

#define DEBUG 0

uniform sampler2D original;
uniform sampler2D blurred;
uniform sampler2D nearfield;
uniform sampler2D coc;
uniform sampler2D nearcoc;
varying vec2 v_Texcoord;

@import clay.util.rgbm
@import clay.util.float

void main()
{
 vec4 blurredColor = texture2D(blurred, v_Texcoord);
 vec4 originalColor = texture2D(original, v_Texcoord);

 float fCoc = decodeFloat(texture2D(coc, v_Texcoord));

 fCoc = abs(fCoc * 2.0 - 1.0);

 float weight = smoothstep(0.0, 1.0, fCoc);
 
#ifdef NEARFIELD_ENABLED
 vec4 nearfieldColor = texture2D(nearfield, v_Texcoord);
 float fNearCoc = decodeFloat(texture2D(nearcoc, v_Texcoord));
 fNearCoc = abs(fNearCoc * 2.0 - 1.0);

 gl_FragColor = encodeHDR(
 mix(
 nearfieldColor, mix(originalColor, blurredColor, weight),
 pow(1.0 - fNearCoc, 4.0)
 )
 );
#else
 gl_FragColor = encodeHDR(mix(originalColor, blurredColor, weight));
#endif

}

@end



@export ecgl.dof.diskBlur

#define POISSON_KERNEL_SIZE 16;

uniform sampler2D texture;
uniform sampler2D coc;
varying vec2 v_Texcoord;

uniform float blurRadius : 10.0;
uniform vec2 textureSize : [512.0, 512.0];

uniform vec2 poissonKernel[POISSON_KERNEL_SIZE];

uniform float percent;

float nrand(const in vec2 n) {
 return fract(sin(dot(n.xy ,vec2(12.9898,78.233))) * 43758.5453);
}

@import clay.util.rgbm
@import clay.util.float


void main()
{
 vec2 offset = blurRadius / textureSize;

 float rnd = 6.28318 * nrand(v_Texcoord + 0.07 * percent );
 float cosa = cos(rnd);
 float sina = sin(rnd);
 vec4 basis = vec4(cosa, -sina, sina, cosa);

#if !defined(BLUR_NEARFIELD) && !defined(BLUR_COC)
 offset *= abs(decodeFloat(texture2D(coc, v_Texcoord)) * 2.0 - 1.0);
#endif

#ifdef BLUR_COC
 float cocSum = 0.0;
#else
 vec4 color = vec4(0.0);
#endif


 float weightSum = 0.0;

 for (int i = 0; i < POISSON_KERNEL_SIZE; i++) {
 vec2 ofs = poissonKernel[i];

 ofs = vec2(dot(ofs, basis.xy), dot(ofs, basis.zw));

 vec2 uv = v_Texcoord + ofs * offset;
 vec4 texel = texture2D(texture, uv);

 float w = 1.0;
#ifdef BLUR_COC
 float fCoc = decodeFloat(texel) * 2.0 - 1.0;
 cocSum += clamp(fCoc, -1.0, 0.0) * w;
#else
 texel = texel;
 #if !defined(BLUR_NEARFIELD)
 float fCoc = decodeFloat(texture2D(coc, uv)) * 2.0 - 1.0;
 w *= abs(fCoc);
 #endif
 texel.rgb *= texel.a;
 color += texel * w;
#endif

 weightSum += w;
 }

#ifdef BLUR_COC
 gl_FragColor = encodeFloat(clamp(cocSum / weightSum, -1.0, 0.0) * 0.5 + 0.5);
#else
 color /= weightSum;
 color.rgb /= (color.a + 0.0001);
 gl_FragColor = color;
#endif
}

@end`, Ic = `@export ecgl.edge

uniform sampler2D texture;

uniform sampler2D normalTexture;
uniform sampler2D depthTexture;

uniform mat4 projectionInv;

uniform vec2 textureSize;

uniform vec4 edgeColor: [0,0,0,0.8];

varying vec2 v_Texcoord;

vec3 packColor(vec2 coord) {
 float z = texture2D(depthTexture, coord).r * 2.0 - 1.0;
 vec4 p = vec4(v_Texcoord * 2.0 - 1.0, z, 1.0);
 vec4 p4 = projectionInv * p;

 return vec3(
 texture2D(normalTexture, coord).rg,
 -p4.z / p4.w / 5.0
 );
}

void main() {
 vec2 cc = v_Texcoord;
 vec3 center = packColor(cc);

 float size = clamp(1.0 - (center.z - 10.0) / 100.0, 0.0, 1.0) * 0.5;
 float dx = size / textureSize.x;
 float dy = size / textureSize.y;

 vec2 coord;
 vec3 topLeft = packColor(cc+vec2(-dx, -dy));
 vec3 top = packColor(cc+vec2(0.0, -dy));
 vec3 topRight = packColor(cc+vec2(dx, -dy));
 vec3 left = packColor(cc+vec2(-dx, 0.0));
 vec3 right = packColor(cc+vec2(dx, 0.0));
 vec3 bottomLeft = packColor(cc+vec2(-dx, dy));
 vec3 bottom = packColor(cc+vec2(0.0, dy));
 vec3 bottomRight = packColor(cc+vec2(dx, dy));

 vec3 v = -topLeft-2.0*top-topRight+bottomLeft+2.0*bottom+bottomRight;
 vec3 h = -bottomLeft-2.0*left-topLeft+bottomRight+2.0*right+topRight;

 float edge = sqrt(dot(h, h) + dot(v, v));

 edge = smoothstep(0.8, 1.0, edge);

 gl_FragColor = mix(texture2D(texture, v_Texcoord), vec4(edgeColor.rgb, 1.0), edgeColor.a * edge);
}
@end`;
U.import(ss);
U.import(ls);
U.import(us);
U.import(fs);
U.import(hs);
U.import(cs);
U.import(ds);
U.import(vs);
U.import(ps);
U.import(Oc);
U.import(Ic);
function gs(e, t) {
  return {
    color: {
      parameters: {
        width: e,
        height: t
      }
    }
  };
}
var Wn = ["composite", "FXAA"];
function j() {
  this._width, this._height, this._dpr, this._sourceTexture = new K({
    type: N.HALF_FLOAT
  }), this._depthTexture = new K({
    format: N.DEPTH_COMPONENT,
    type: N.UNSIGNED_INT
  }), this._framebuffer = new pe(), this._framebuffer.attach(this._sourceTexture), this._framebuffer.attach(this._depthTexture, pe.DEPTH_ATTACHMENT), this._normalPass = new er(), this._compositor = Tc(Nc);
  var e = this._compositor.getNodeByName("source");
  e.texture = this._sourceTexture;
  var t = this._compositor.getNodeByName("coc");
  this._sourceNode = e, this._cocNode = t, this._compositeNode = this._compositor.getNodeByName("composite"), this._fxaaNode = this._compositor.getNodeByName("FXAA"), this._dofBlurNodes = ["dof_far_blur", "dof_near_blur", "dof_coc_blur"].map(function(i) {
    return this._compositor.getNodeByName(i);
  }, this), this._dofBlurKernel = 0, this._dofBlurKernelSize = new Float32Array(0), this._finalNodesChain = Wn.map(function(i) {
    return this._compositor.getNodeByName(i);
  }, this);
  var r = {
    normalTexture: this._normalPass.getNormalTexture(),
    depthTexture: this._normalPass.getDepthTexture()
  };
  this._ssaoPass = new ht(r), this._ssrPass = new ct(r), this._edgePass = new Mr(r);
}
j.prototype.resize = function(i, n, r) {
  r = r || 1;
  var i = i * r, n = n * r, a = this._sourceTexture, o = this._depthTexture;
  a.width = i, a.height = n, o.width = i, o.height = n;
  var s = {
    getWidth: function() {
      return i;
    },
    getHeight: function() {
      return n;
    },
    getDevicePixelRatio: function() {
      return r;
    }
  };
  function l(u, f) {
    if (typeof u[f] == "function") {
      var h = u[f].__original || u[f];
      u[f] = function(d) {
        return h.call(this, s);
      }, u[f].__original = h;
    }
  }
  this._compositor.nodes.forEach(function(u) {
    for (var f in u.outputs) {
      var h = u.outputs[f].parameters;
      h && (l(h, "width"), l(h, "height"));
    }
    for (var d in u.parameters)
      l(u.parameters, d);
  }), this._width = i, this._height = n, this._dpr = r;
};
j.prototype.getWidth = function() {
  return this._width;
};
j.prototype.getHeight = function() {
  return this._height;
};
j.prototype._ifRenderNormalPass = function() {
  return this._enableSSAO || this._enableEdge || this._enableSSR;
};
j.prototype._getPrevNode = function(e) {
  for (var t = Wn.indexOf(e.name) - 1, r = this._finalNodesChain[t]; r && !this._compositor.getNodeByName(r.name); )
    t -= 1, r = this._finalNodesChain[t];
  return r;
};
j.prototype._getNextNode = function(e) {
  for (var t = Wn.indexOf(e.name) + 1, r = this._finalNodesChain[t]; r && !this._compositor.getNodeByName(r.name); )
    t += 1, r = this._finalNodesChain[t];
  return r;
};
j.prototype._addChainNode = function(e) {
  var t = this._getPrevNode(e), r = this._getNextNode(e);
  t && (e.inputs.texture = t.name, r ? (e.outputs = gs(this.getWidth.bind(this), this.getHeight.bind(this)), r.inputs.texture = e.name) : e.outputs = null, this._compositor.addNode(e));
};
j.prototype._removeChainNode = function(e) {
  var t = this._getPrevNode(e), r = this._getNextNode(e);
  t && (r ? (t.outputs = gs(this.getWidth.bind(this), this.getHeight.bind(this)), r.inputs.texture = t.name) : t.outputs = null, this._compositor.removeNode(e));
};
j.prototype.updateNormal = function(e, t, r, i) {
  this._ifRenderNormalPass() && this._normalPass.update(e, t, r);
};
j.prototype.updateSSAO = function(e, t, r, i) {
  this._ssaoPass.update(e, r, i);
};
j.prototype.enableSSAO = function() {
  this._enableSSAO = !0;
};
j.prototype.disableSSAO = function() {
  this._enableSSAO = !1;
};
j.prototype.enableSSR = function() {
  this._enableSSR = !0;
};
j.prototype.disableSSR = function() {
  this._enableSSR = !1;
};
j.prototype.getSSAOTexture = function() {
  return this._ssaoPass.getTargetTexture();
};
j.prototype.getSourceFrameBuffer = function() {
  return this._framebuffer;
};
j.prototype.getSourceTexture = function() {
  return this._sourceTexture;
};
j.prototype.disableFXAA = function() {
  this._removeChainNode(this._fxaaNode);
};
j.prototype.enableFXAA = function() {
  this._addChainNode(this._fxaaNode);
};
j.prototype.enableBloom = function() {
  this._compositeNode.inputs.bloom = "bloom_composite", this._compositor.dirty();
};
j.prototype.disableBloom = function() {
  this._compositeNode.inputs.bloom = null, this._compositor.dirty();
};
j.prototype.enableDOF = function() {
  this._compositeNode.inputs.texture = "dof_composite", this._compositor.dirty();
};
j.prototype.disableDOF = function() {
  this._compositeNode.inputs.texture = "source", this._compositor.dirty();
};
j.prototype.enableColorCorrection = function() {
  this._compositeNode.define("COLOR_CORRECTION"), this._enableColorCorrection = !0;
};
j.prototype.disableColorCorrection = function() {
  this._compositeNode.undefine("COLOR_CORRECTION"), this._enableColorCorrection = !1;
};
j.prototype.enableEdge = function() {
  this._enableEdge = !0;
};
j.prototype.disableEdge = function() {
  this._enableEdge = !1;
};
j.prototype.setBloomIntensity = function(e) {
  this._compositeNode.setParameter("bloomIntensity", e);
};
j.prototype.setSSAOParameter = function(e, t) {
  switch (e) {
    case "quality":
      var r = {
        low: 6,
        medium: 12,
        high: 32,
        ultra: 62
      }[t] || 12;
      this._ssaoPass.setParameter("kernelSize", r);
      break;
    case "radius":
      this._ssaoPass.setParameter(e, t), this._ssaoPass.setParameter("bias", t / 200);
      break;
    case "intensity":
      this._ssaoPass.setParameter(e, t);
      break;
  }
};
j.prototype.setDOFParameter = function(e, t) {
  switch (e) {
    case "focalDistance":
    case "focalRange":
    case "fstop":
      this._cocNode.setParameter(e, t);
      break;
    case "blurRadius":
      for (var r = 0; r < this._dofBlurNodes.length; r++)
        this._dofBlurNodes[r].setParameter("blurRadius", t);
      break;
    case "quality":
      var i = {
        low: 4,
        medium: 8,
        high: 16,
        ultra: 32
      }[t] || 8;
      this._dofBlurKernelSize = i;
      for (var r = 0; r < this._dofBlurNodes.length; r++)
        this._dofBlurNodes[r].pass.material.define("POISSON_KERNEL_SIZE", i);
      this._dofBlurKernel = new Float32Array(i * 2);
      break;
  }
};
j.prototype.setSSRParameter = function(e, t) {
  if (t != null)
    switch (e) {
      case "quality":
        var r = {
          low: 10,
          medium: 15,
          high: 30,
          ultra: 80
        }[t] || 20, i = {
          low: 32,
          medium: 16,
          high: 8,
          ultra: 4
        }[t] || 16;
        this._ssrPass.setParameter("maxIteration", r), this._ssrPass.setParameter("pixelStride", i);
        break;
      case "maxRoughness":
        this._ssrPass.setParameter("minGlossiness", Math.max(Math.min(1 - t, 1), 0));
        break;
      case "physical":
        this.setPhysicallyCorrectSSR(t);
        break;
      default:
        console.warn("Unkown SSR parameter " + e);
    }
};
j.prototype.setPhysicallyCorrectSSR = function(e) {
  this._ssrPass.setPhysicallyCorrect(e);
};
j.prototype.setEdgeColor = function(e) {
  var t = E.parseColor(e);
  this._edgePass.setParameter("edgeColor", t);
};
j.prototype.setExposure = function(e) {
  this._compositeNode.setParameter("exposure", Math.pow(2, e));
};
j.prototype.setColorLookupTexture = function(e, t) {
  this._compositeNode.pass.material.setTextureImage("lut", this._enableColorCorrection ? e : "none", t, {
    minFilter: E.Texture.NEAREST,
    magFilter: E.Texture.NEAREST,
    flipY: !1
  });
};
j.prototype.setColorCorrection = function(e, t) {
  this._compositeNode.setParameter(e, t);
};
j.prototype.isSSREnabled = function() {
  return this._enableSSR;
};
j.prototype.composite = function(e, t, r, i, n) {
  var a = this._sourceTexture, o = a;
  this._enableEdge && (this._edgePass.update(e, r, a, n), a = o = this._edgePass.getTargetTexture()), this._enableSSR && (this._ssrPass.update(e, r, a, n), o = this._ssrPass.getTargetTexture(), this._ssrPass.setSSAOTexture(this._enableSSAO ? this._ssaoPass.getTargetTexture() : null)), this._sourceNode.texture = o, this._cocNode.setParameter("depth", this._depthTexture);
  for (var s = this._dofBlurKernel, l = this._dofBlurKernelSize, u = Math.floor(fo.length / 2 / l), f = n % u, h = 0; h < l * 2; h++)
    s[h] = fo[h + f * l * 2];
  for (var h = 0; h < this._dofBlurNodes.length; h++)
    this._dofBlurNodes[h].setParameter("percent", n / 30), this._dofBlurNodes[h].setParameter("poissonKernel", s);
  this._cocNode.setParameter("zNear", r.near), this._cocNode.setParameter("zFar", r.far), this._compositor.render(e, i);
};
j.prototype.dispose = function(e) {
  this._sourceTexture.dispose(e), this._depthTexture.dispose(e), this._framebuffer.dispose(e), this._compositor.dispose(e), this._normalPass.dispose(e), this._ssaoPass.dispose(e);
};
function En(e) {
  for (var t = [], r = 0; r < 30; r++)
    t.push([$t(r, 2), $t(r, 3)]);
  this._haltonSequence = t, this._frame = 0, this._sourceTex = new K(), this._sourceFb = new pe(), this._sourceFb.attach(this._sourceTex), this._prevFrameTex = new K(), this._outputTex = new K();
  var i = this._blendPass = new De({
    fragment: U.source("clay.compositor.blend")
  });
  i.material.disableTexturesAll(), i.material.enableTexture(["texture1", "texture2"]), this._blendFb = new pe({
    depthBuffer: !1
  }), this._outputPass = new De({
    fragment: U.source("clay.compositor.output"),
    // TODO, alpha is premultiplied?
    blendWithPrevious: !0
  }), this._outputPass.material.define("fragment", "OUTPUT_ALPHA"), this._outputPass.material.blend = function(n) {
    n.blendEquationSeparate(n.FUNC_ADD, n.FUNC_ADD), n.blendFuncSeparate(n.ONE, n.ONE_MINUS_SRC_ALPHA, n.ONE, n.ONE_MINUS_SRC_ALPHA);
  };
}
En.prototype = {
  constructor: En,
  /**
   * Jitter camera projectionMatrix
   * @parma {clay.Renderer} renderer
   * @param {clay.Camera} camera
   */
  jitterProjection: function(e, t) {
    var r = e.viewport, i = r.devicePixelRatio || e.getDevicePixelRatio(), n = r.width * i, a = r.height * i, o = this._haltonSequence[this._frame % this._haltonSequence.length], s = new I();
    s.array[12] = (o[0] * 2 - 1) / n, s.array[13] = (o[1] * 2 - 1) / a, I.mul(t.projectionMatrix, s, t.projectionMatrix), I.invert(t.invProjectionMatrix, t.projectionMatrix);
  },
  /**
   * Reset accumulating frame
   */
  resetFrame: function() {
    this._frame = 0;
  },
  /**
   * Return current frame
   */
  getFrame: function() {
    return this._frame;
  },
  /**
   * Get source framebuffer for usage
   */
  getSourceFrameBuffer: function() {
    return this._sourceFb;
  },
  getOutputTexture: function() {
    return this._outputTex;
  },
  resize: function(e, t) {
    this._prevFrameTex.width = e, this._prevFrameTex.height = t, this._outputTex.width = e, this._outputTex.height = t, this._sourceTex.width = e, this._sourceTex.height = t, this._prevFrameTex.dirty(), this._outputTex.dirty(), this._sourceTex.dirty();
  },
  isFinished: function() {
    return this._frame >= this._haltonSequence.length;
  },
  render: function(e, t, r) {
    var i = this._blendPass;
    this._frame === 0 ? (i.setUniform("weight1", 0), i.setUniform("weight2", 1)) : (i.setUniform("weight1", 0.9), i.setUniform("weight2", 0.1)), i.setUniform("texture1", this._prevFrameTex), i.setUniform("texture2", t || this._sourceTex), this._blendFb.attach(this._outputTex), this._blendFb.bind(e), i.render(e), this._blendFb.unbind(e), r || (this._outputPass.setUniform("texture", this._outputTex), this._outputPass.render(e));
    var n = this._prevFrameTex;
    this._prevFrameTex = this._outputTex, this._outputTex = n, this._frame++;
  },
  dispose: function(e) {
    this._sourceFb.dispose(e), this._blendFb.dispose(e), this._prevFrameTex.dispose(e), this._outputTex.dispose(e), this._sourceTex.dispose(e), this._outputPass.dispose(e), this._blendPass.dispose(e);
  }
};
function se(e) {
  e = e || "perspective", this.layer = null, this.scene = new gt(), this.rootNode = this.scene, this.viewport = {
    x: 0,
    y: 0,
    width: 0,
    height: 0
  }, this.setProjection(e), this._compositor = new j(), this._temporalSS = new En(), this._shadowMapPass = new $e();
  for (var t = [], r = 0, i = 0; i < 30; i++) {
    for (var n = [], a = 0; a < 6; a++)
      n.push($t(r, 2) * 4 - 2), n.push($t(r, 3) * 4 - 2), r++;
    t.push(n);
  }
  this._pcfKernels = t, this.scene.on("beforerender", function(o, s, l) {
    this.needsTemporalSS() && this._temporalSS.jitterProjection(o, l);
  }, this);
}
se.prototype.setProjection = function(e) {
  var t = this.camera;
  t && t.update(), e === "perspective" ? this.camera instanceof _e || (this.camera = new _e(), t && this.camera.setLocalTransform(t.localTransform)) : this.camera instanceof wr || (this.camera = new wr(), t && this.camera.setLocalTransform(t.localTransform)), this.camera.near = 0.1, this.camera.far = 2e3;
};
se.prototype.setViewport = function(e, t, r, i, n) {
  this.camera instanceof _e && (this.camera.aspect = r / i), n = n || 1, this.viewport.x = e, this.viewport.y = t, this.viewport.width = r, this.viewport.height = i, this.viewport.devicePixelRatio = n, this._compositor.resize(r * n, i * n), this._temporalSS.resize(r * n, i * n);
};
se.prototype.containPoint = function(e, t) {
  var r = this.viewport, i = this.layer.renderer.getHeight();
  return t = i - t, e >= r.x && t >= r.y && e <= r.x + r.width && t <= r.y + r.height;
};
var ho = new k();
se.prototype.castRay = function(e, t, r) {
  var i = this.layer.renderer, n = i.viewport;
  return i.viewport = this.viewport, i.screenToNDC(e, t, ho), this.camera.castRay(ho, r), i.viewport = n, r;
};
se.prototype.prepareRender = function() {
  this.scene.update(), this.camera.update(), this.scene.updateLights();
  var e = this.scene.updateRenderList(this.camera);
  this._needsSortProgressively = !1;
  for (var t = 0; t < e.transparent.length; t++) {
    var r = e.transparent[t], i = r.geometry;
    i.needsSortVerticesProgressively && i.needsSortVerticesProgressively() && (this._needsSortProgressively = !0), i.needsSortTrianglesProgressively && i.needsSortTrianglesProgressively() && (this._needsSortProgressively = !0);
  }
  this._frame = 0, this._temporalSS.resetFrame();
};
se.prototype.render = function(e, t) {
  this._doRender(e, t, this._frame), this._frame++;
};
se.prototype.needsAccumulate = function() {
  return this.needsTemporalSS() || this._needsSortProgressively;
};
se.prototype.needsTemporalSS = function() {
  var e = this._enableTemporalSS;
  return e === "auto" && (e = this._enablePostEffect), e;
};
se.prototype.hasDOF = function() {
  return this._enableDOF;
};
se.prototype.isAccumulateFinished = function() {
  return this.needsTemporalSS() ? this._temporalSS.isFinished() : this._frame > 30;
};
se.prototype._doRender = function(e, t, r) {
  var i = this.scene, n = this.camera;
  r = r || 0, this._updateTransparent(e, i, n, r), t || (this._shadowMapPass.kernelPCF = this._pcfKernels[0], this._shadowMapPass.render(e, i, n, !0)), this._updateShadowPCFKernel(r);
  var a = e.clearColor;
  if (e.gl.clearColor(a[0], a[1], a[2], a[3]), this._enablePostEffect && (this.needsTemporalSS() && this._temporalSS.jitterProjection(e, n), this._compositor.updateNormal(e, i, n, this._temporalSS.getFrame())), this._updateSSAO(e, i, n, this._temporalSS.getFrame()), this._enablePostEffect) {
    var o = this._compositor.getSourceFrameBuffer();
    o.bind(e), e.gl.clear(e.gl.DEPTH_BUFFER_BIT | e.gl.COLOR_BUFFER_BIT), e.render(i, n, !0, !0), o.unbind(e), this.needsTemporalSS() && t ? (this._compositor.composite(e, i, n, this._temporalSS.getSourceFrameBuffer(), this._temporalSS.getFrame()), e.setViewport(this.viewport), this._temporalSS.render(e)) : (e.setViewport(this.viewport), this._compositor.composite(e, i, n, null, 0));
  } else if (this.needsTemporalSS() && t) {
    var o = this._temporalSS.getSourceFrameBuffer();
    o.bind(e), e.saveClear(), e.clearBit = e.gl.DEPTH_BUFFER_BIT | e.gl.COLOR_BUFFER_BIT, e.render(i, n, !0, !0), e.restoreClear(), o.unbind(e), e.setViewport(this.viewport), this._temporalSS.render(e);
  } else
    e.setViewport(this.viewport), e.render(i, n, !0, !0);
};
se.prototype._updateTransparent = function(e, t, r, i) {
  for (var n = new S(), a = new I(), o = r.getWorldPosition(), s = t.getRenderList(r).transparent, l = 0; l < s.length; l++) {
    var u = s[l], f = u.geometry;
    I.invert(a, u.worldTransform), S.transformMat4(n, o, a), f.needsSortTriangles && f.needsSortTriangles() && f.doSortTriangles(n, i), f.needsSortVertices && f.needsSortVertices() && f.doSortVertices(n, i);
  }
};
se.prototype._updateSSAO = function(e, t, r) {
  var i = this._enableSSAO && this._enablePostEffect;
  i && this._compositor.updateSSAO(e, t, r, this._temporalSS.getFrame());
  for (var n = t.getRenderList(r), a = 0; a < n.opaque.length; a++) {
    var o = n.opaque[a];
    o.renderNormal && o.material[i ? "enableTexture" : "disableTexture"]("ssaoMap"), i && o.material.set("ssaoMap", this._compositor.getSSAOTexture());
  }
};
se.prototype._updateShadowPCFKernel = function(e) {
  for (var t = this._pcfKernels[e % this._pcfKernels.length], r = this.scene.getRenderList(this.camera), i = r.opaque, n = 0; n < i.length; n++)
    i[n].receiveShadow && (i[n].material.set("pcfKernel", t), i[n].material.define("fragment", "PCF_KERNEL_SIZE", t.length / 2));
};
se.prototype.dispose = function(e) {
  this._compositor.dispose(e.gl), this._temporalSS.dispose(e.gl), this._shadowMapPass.dispose(e);
};
se.prototype.setPostEffect = function(e, t) {
  var r = this._compositor;
  this._enablePostEffect = e.get("enable");
  var i = e.getModel("bloom"), n = e.getModel("edge"), a = e.getModel("DOF", e.getModel("depthOfField")), o = e.getModel("SSAO", e.getModel("screenSpaceAmbientOcclusion")), s = e.getModel("SSR", e.getModel("screenSpaceReflection")), l = e.getModel("FXAA"), u = e.getModel("colorCorrection");
  i.get("enable") ? r.enableBloom() : r.disableBloom(), a.get("enable") ? r.enableDOF() : r.disableDOF(), s.get("enable") ? r.enableSSR() : r.disableSSR(), u.get("enable") ? r.enableColorCorrection() : r.disableColorCorrection(), n.get("enable") ? r.enableEdge() : r.disableEdge(), l.get("enable") ? r.enableFXAA() : r.disableFXAA(), this._enableDOF = a.get("enable"), this._enableSSAO = o.get("enable"), this._enableSSAO ? r.enableSSAO() : r.disableSSAO(), r.setBloomIntensity(i.get("intensity")), r.setEdgeColor(n.get("color")), r.setColorLookupTexture(u.get("lookupTexture"), t), r.setExposure(u.get("exposure")), ["radius", "quality", "intensity"].forEach(function(f) {
    r.setSSAOParameter(f, o.get(f));
  }), ["quality", "maxRoughness", "physical"].forEach(function(f) {
    r.setSSRParameter(f, s.get(f));
  }), ["quality", "focalDistance", "focalRange", "blurRadius", "fstop"].forEach(function(f) {
    r.setDOFParameter(f, a.get(f));
  }), ["brightness", "contrast", "saturation"].forEach(function(f) {
    r.setColorCorrection(f, u.get(f));
  });
};
se.prototype.setDOFFocusOnPoint = function(e) {
  if (this._enablePostEffect)
    return e > this.camera.far || e < this.camera.near ? void 0 : (this._compositor.setDOFParameter("focalDistance", e), !0);
};
se.prototype.setTemporalSuperSampling = function(e) {
  this._enableTemporalSS = e.get("enable");
};
se.prototype.isLinearSpace = function() {
  return this._enablePostEffect;
};
se.prototype.setRootNode = function(e) {
  if (this.rootNode !== e) {
    for (var t = this.rootNode.children(), r = 0; r < t.length; r++)
      e.add(t[r]);
    e !== this.scene && this.scene.add(e), this.rootNode = e;
  }
};
se.prototype.add = function(e) {
  this.rootNode.add(e);
};
se.prototype.remove = function(e) {
  this.rootNode.remove(e);
};
se.prototype.removeAll = function(e) {
  this.rootNode.removeAll(e);
};
Object.assign(se.prototype, Rn);
var hr = be.firstNotNull, co = {
  left: 0,
  middle: 1,
  right: 2
};
function vo(e) {
  return e instanceof Array || (e = [e, e]), e;
}
var ys = Ae.extend(function() {
  return {
    /**
     * @type {module:zrender~ZRender}
     */
    zr: null,
    /**
     * @type {module:echarts-gl/core/ViewGL}
     */
    viewGL: null,
    /**
     * @type {clay.math.Vector3}
     */
    _center: new S(),
    /**
     * Minimum distance to the center
     * Only available when camera is perspective.
     * @type {number}
     * @default 0.5
     */
    minDistance: 0.5,
    /**
     * Maximum distance to the center
     * Only available when camera is perspective.
     * @type {number}
     * @default 2
     */
    maxDistance: 1.5,
    /**
     * Only available when camera is orthographic
     */
    maxOrthographicSize: 300,
    /**
     * Only available when camera is orthographic
     */
    minOrthographicSize: 30,
    /**
     * Minimum alpha rotation
     */
    minAlpha: -90,
    /**
     * Maximum alpha rotation
     */
    maxAlpha: 90,
    /**
     * Minimum beta rotation
     */
    minBeta: -1 / 0,
    /**
     * Maximum beta rotation
     */
    maxBeta: 1 / 0,
    /**
     * Start auto rotating after still for the given time
     */
    autoRotateAfterStill: 0,
    /**
     * Direction of autoRotate. cw or ccw when looking top down.
     */
    autoRotateDirection: "cw",
    /**
     * Degree per second
     */
    autoRotateSpeed: 60,
    /**
     * @param {number}
     */
    damping: 0.8,
    /**
     * @param {number}
     */
    rotateSensitivity: 1,
    /**
     * @param {number}
     */
    zoomSensitivity: 1,
    /**
     * @param {number}
     */
    panSensitivity: 1,
    panMouseButton: "middle",
    rotateMouseButton: "left",
    /**
     * Pan or rotate
     * @private
     * @type {String}
     */
    _mode: "rotate",
    /**
     * @private
     * @type {clay.Camera}
     */
    _camera: null,
    _needsUpdate: !1,
    _rotating: !1,
    // Rotation around yAxis in radian
    _phi: 0,
    // Rotation around xAxis in radian
    _theta: 0,
    _mouseX: 0,
    _mouseY: 0,
    _rotateVelocity: new k(),
    _panVelocity: new k(),
    _distance: 500,
    _zoomSpeed: 0,
    _stillTimeout: 0,
    _animators: []
  };
}, function() {
  ["_mouseDownHandler", "_mouseWheelHandler", "_mouseMoveHandler", "_mouseUpHandler", "_pinchHandler", "_contextMenuHandler", "_update"].forEach(function(e) {
    this[e] = this[e].bind(this);
  }, this);
}, {
  /**
   * Initialize.
   * Mouse event binding
   */
  init: function() {
    var e = this.zr;
    e && (e.on("mousedown", this._mouseDownHandler), e.on("globalout", this._mouseUpHandler), e.on("mousewheel", this._mouseWheelHandler), e.on("pinch", this._pinchHandler), e.animation.on("frame", this._update), e.dom.addEventListener("contextmenu", this._contextMenuHandler));
  },
  /**
   * Dispose.
   * Mouse event unbinding
   */
  dispose: function() {
    var e = this.zr;
    e && (e.off("mousedown", this._mouseDownHandler), e.off("mousemove", this._mouseMoveHandler), e.off("mouseup", this._mouseUpHandler), e.off("mousewheel", this._mouseWheelHandler), e.off("pinch", this._pinchHandler), e.off("globalout", this._mouseUpHandler), e.dom.removeEventListener("contextmenu", this._contextMenuHandler), e.animation.off("frame", this._update)), this.stopAllAnimation();
  },
  /**
   * Get distance
   * @return {number}
   */
  getDistance: function() {
    return this._distance;
  },
  /**
   * Set distance
   * @param {number} distance
   */
  setDistance: function(e) {
    this._distance = e, this._needsUpdate = !0;
  },
  /**
   * Get size of orthographic viewing volume
   * @return {number}
   */
  getOrthographicSize: function() {
    return this._orthoSize;
  },
  /**
   * Set size of orthographic viewing volume
   * @param {number} size
   */
  setOrthographicSize: function(e) {
    this._orthoSize = e, this._needsUpdate = !0;
  },
  /**
   * Get alpha rotation
   * Alpha angle for top-down rotation. Positive to rotate to top.
   *
   * Which means camera rotation around x axis.
   */
  getAlpha: function() {
    return this._theta / Math.PI * 180;
  },
  /**
   * Get beta rotation
   * Beta angle for left-right rotation. Positive to rotate to right.
   *
   * Which means camera rotation around y axis.
   */
  getBeta: function() {
    return -this._phi / Math.PI * 180;
  },
  /**
   * Get control center
   * @return {Array.<number>}
   */
  getCenter: function() {
    return this._center.toArray();
  },
  /**
   * Set alpha rotation angle
   * @param {number} alpha
   */
  setAlpha: function(e) {
    e = Math.max(Math.min(this.maxAlpha, e), this.minAlpha), this._theta = e / 180 * Math.PI, this._needsUpdate = !0;
  },
  /**
   * Set beta rotation angle
   * @param {number} beta
   */
  setBeta: function(e) {
    e = Math.max(Math.min(this.maxBeta, e), this.minBeta), this._phi = -e / 180 * Math.PI, this._needsUpdate = !0;
  },
  /**
   * Set control center
   * @param {Array.<number>} center
   */
  setCenter: function(e) {
    this._center.setArray(e);
  },
  /**
   * @param {module:echarts-gl/core/ViewGL} viewGL
   */
  setViewGL: function(e) {
    this.viewGL = e;
  },
  /**
   * @return {clay.Camera}
   */
  getCamera: function() {
    return this.viewGL.camera;
  },
  setFromViewControlModel: function(e, t) {
    t = t || {};
    var r = t.baseDistance || 0, i = t.baseOrthoSize || 1, n = e.get("projection");
    n !== "perspective" && n !== "orthographic" && n !== "isometric" && (n = "perspective"), this._projection = n, this.viewGL.setProjection(n);
    var a = e.get("distance") + r, o = e.get("orthographicSize") + i;
    [["damping", 0.8], ["autoRotate", !1], ["autoRotateAfterStill", 3], ["autoRotateDirection", "cw"], ["autoRotateSpeed", 10], ["minDistance", 30], ["maxDistance", 400], ["minOrthographicSize", 30], ["maxOrthographicSize", 300], ["minAlpha", -90], ["maxAlpha", 90], ["minBeta", -1 / 0], ["maxBeta", 1 / 0], ["rotateSensitivity", 1], ["zoomSensitivity", 1], ["panSensitivity", 1], ["panMouseButton", "left"], ["rotateMouseButton", "middle"]].forEach(function(d) {
      this[d[0]] = hr(e.get(d[0]), d[1]);
    }, this), this.minDistance += r, this.maxDistance += r, this.minOrthographicSize += i, this.maxOrthographicSize += i;
    var s = e.ecModel, l = {};
    ["animation", "animationDurationUpdate", "animationEasingUpdate"].forEach(function(d) {
      l[d] = hr(e.get(d), s && s.get(d));
    });
    var u = hr(t.alpha, e.get("alpha")) || 0, f = hr(t.beta, e.get("beta")) || 0, h = hr(t.center, e.get("center")) || [0, 0, 0];
    l.animation && l.animationDurationUpdate > 0 && this._notFirst ? this.animateTo({
      alpha: u,
      beta: f,
      center: h,
      distance: a,
      orthographicSize: o,
      easing: l.animationEasingUpdate,
      duration: l.animationDurationUpdate
    }) : (this.setDistance(a), this.setAlpha(u), this.setBeta(f), this.setCenter(h), this.setOrthographicSize(o)), this._notFirst = !0, this._validateProperties();
  },
  _validateProperties: function() {
  },
  /**
   * @param {Object} opts
   * @param {number} opts.distance
   * @param {number} opts.alpha
   * @param {number} opts.beta
   * @param {number} opts.orthographicSize
   * @param {number} [opts.duration=1000]
   * @param {number} [opts.easing='linear']
   */
  animateTo: function(e) {
    var t = this.zr, r = this, i = {}, n = {};
    return e.distance != null && (i.distance = this.getDistance(), n.distance = e.distance), e.orthographicSize != null && (i.orthographicSize = this.getOrthographicSize(), n.orthographicSize = e.orthographicSize), e.alpha != null && (i.alpha = this.getAlpha(), n.alpha = e.alpha), e.beta != null && (i.beta = this.getBeta(), n.beta = e.beta), e.center != null && (i.center = this.getCenter(), n.center = e.center), this._addAnimator(t.animation.animate(i).when(e.duration || 1e3, n).during(function() {
      i.alpha != null && r.setAlpha(i.alpha), i.beta != null && r.setBeta(i.beta), i.distance != null && r.setDistance(i.distance), i.center != null && r.setCenter(i.center), i.orthographicSize != null && r.setOrthographicSize(i.orthographicSize), r._needsUpdate = !0;
    })).start(e.easing || "linear");
  },
  /**
   * Stop all animation
   */
  stopAllAnimation: function() {
    for (var e = 0; e < this._animators.length; e++)
      this._animators[e].stop();
    this._animators.length = 0;
  },
  update: function() {
    this._needsUpdate = !0, this._update(20);
  },
  _isAnimating: function() {
    return this._animators.length > 0;
  },
  /**
   * Call update each frame
   * @param  {number} deltaTime Frame time
   */
  _update: function(e) {
    if (this._rotating) {
      var t = (this.autoRotateDirection === "cw" ? 1 : -1) * this.autoRotateSpeed / 180 * Math.PI;
      this._phi -= t * e / 1e3, this._needsUpdate = !0;
    } else this._rotateVelocity.len() > 0 && (this._needsUpdate = !0);
    (Math.abs(this._zoomSpeed) > 0.1 || this._panVelocity.len() > 0) && (this._needsUpdate = !0), this._needsUpdate && (e = Math.min(e, 50), this._updateDistanceOrSize(e), this._updatePan(e), this._updateRotate(e), this._updateTransform(), this.getCamera().update(), this.zr && this.zr.refresh(), this.trigger("update"), this._needsUpdate = !1);
  },
  _updateRotate: function(e) {
    var t = this._rotateVelocity;
    this._phi = t.y * e / 20 + this._phi, this._theta = t.x * e / 20 + this._theta, this.setAlpha(this.getAlpha()), this.setBeta(this.getBeta()), this._vectorDamping(t, Math.pow(this.damping, e / 16));
  },
  _updateDistanceOrSize: function(e) {
    this._projection === "perspective" ? this._setDistance(this._distance + this._zoomSpeed * e / 20) : this._setOrthoSize(this._orthoSize + this._zoomSpeed * e / 20), this._zoomSpeed *= Math.pow(this.damping, e / 16);
  },
  _setDistance: function(e) {
    this._distance = Math.max(Math.min(e, this.maxDistance), this.minDistance);
  },
  _setOrthoSize: function(e) {
    this._orthoSize = Math.max(Math.min(e, this.maxOrthographicSize), this.minOrthographicSize);
    var t = this.getCamera(), r = this._orthoSize, i = r / this.viewGL.viewport.height * this.viewGL.viewport.width;
    t.left = -i / 2, t.right = i / 2, t.top = r / 2, t.bottom = -r / 2;
  },
  _updatePan: function(e) {
    var t = this._panVelocity, r = this._distance, i = this.getCamera(), n = i.worldTransform.y, a = i.worldTransform.x;
    this._center.scaleAndAdd(a, -t.x * r / 200).scaleAndAdd(n, -t.y * r / 200), this._vectorDamping(t, 0);
  },
  _updateTransform: function() {
    var e = this.getCamera(), t = new S(), r = this._theta + Math.PI / 2, i = this._phi + Math.PI / 2, n = Math.sin(r);
    t.x = n * Math.cos(i), t.y = -Math.cos(r), t.z = n * Math.sin(i), e.position.copy(this._center).scaleAndAdd(t, this._distance), e.rotation.identity().rotateY(-this._phi).rotateX(-this._theta);
  },
  _startCountingStill: function() {
    clearTimeout(this._stillTimeout);
    var e = this.autoRotateAfterStill, t = this;
    !isNaN(e) && e > 0 && (this._stillTimeout = setTimeout(function() {
      t._rotating = !0;
    }, e * 1e3));
  },
  _vectorDamping: function(e, t) {
    var r = e.len();
    r = r * t, r < 1e-4 && (r = 0), e.normalize().scale(r);
  },
  _decomposeTransform: function() {
    if (this.getCamera()) {
      this.getCamera().updateWorldTransform();
      var e = this.getCamera().worldTransform.z, t = Math.asin(e.y), r = Math.atan2(e.x, e.z);
      this._theta = t, this._phi = -r, this.setBeta(this.getBeta()), this.setAlpha(this.getAlpha()), this.getCamera().aspect ? this._setDistance(this.getCamera().position.dist(this._center)) : this._setOrthoSize(this.getCamera().top - this.getCamera().bottom);
    }
  },
  _mouseDownHandler: function(e) {
    if (!e.target && !this._isAnimating()) {
      var t = e.offsetX, r = e.offsetY;
      this.viewGL && !this.viewGL.containPoint(t, r) || (this.zr.on("mousemove", this._mouseMoveHandler), this.zr.on("mouseup", this._mouseUpHandler), e.event.targetTouches ? e.event.targetTouches.length === 1 && (this._mode = "rotate") : e.event.button === co[this.rotateMouseButton] ? this._mode = "rotate" : e.event.button === co[this.panMouseButton] ? this._mode = "pan" : this._mode = "", this._rotateVelocity.set(0, 0), this._rotating = !1, this.autoRotate && this._startCountingStill(), this._mouseX = e.offsetX, this._mouseY = e.offsetY);
    }
  },
  _mouseMoveHandler: function(e) {
    if (!(e.target && e.target.__isGLToZRProxy) && !this._isAnimating()) {
      var t = vo(this.panSensitivity), r = vo(this.rotateSensitivity);
      this._mode === "rotate" ? (this._rotateVelocity.y = (e.offsetX - this._mouseX) / this.zr.getHeight() * 2 * r[0], this._rotateVelocity.x = (e.offsetY - this._mouseY) / this.zr.getWidth() * 2 * r[1]) : this._mode === "pan" && (this._panVelocity.x = (e.offsetX - this._mouseX) / this.zr.getWidth() * t[0] * 400, this._panVelocity.y = (-e.offsetY + this._mouseY) / this.zr.getHeight() * t[1] * 400), this._mouseX = e.offsetX, this._mouseY = e.offsetY, e.event.preventDefault();
    }
  },
  _mouseWheelHandler: function(e) {
    if (!this._isAnimating()) {
      var t = e.event.wheelDelta || -e.event.detail;
      this._zoomHandler(e, t);
    }
  },
  _pinchHandler: function(e) {
    this._isAnimating() || (this._zoomHandler(e, e.pinchScale > 1 ? 1 : -1), this._mode = "");
  },
  _zoomHandler: function(e, t) {
    if (t !== 0) {
      var r = e.offsetX, i = e.offsetY;
      if (!(this.viewGL && !this.viewGL.containPoint(r, i))) {
        var n;
        this._projection === "perspective" ? n = Math.max(Math.max(Math.min(this._distance - this.minDistance, this.maxDistance - this._distance)) / 20, 0.5) : n = Math.max(Math.max(Math.min(this._orthoSize - this.minOrthographicSize, this.maxOrthographicSize - this._orthoSize)) / 20, 0.5), this._zoomSpeed = (t > 0 ? -1 : 1) * n * this.zoomSensitivity, this._rotating = !1, this.autoRotate && this._mode === "rotate" && this._startCountingStill(), e.event.preventDefault();
      }
    }
  },
  _mouseUpHandler: function() {
    this.zr.off("mousemove", this._mouseMoveHandler), this.zr.off("mouseup", this._mouseUpHandler);
  },
  _isRightMouseButtonUsed: function() {
    return this.rotateMouseButton === "right" || this.panMouseButton === "right";
  },
  _contextMenuHandler: function(e) {
    this._isRightMouseButtonUsed() && e.preventDefault();
  },
  _addAnimator: function(e) {
    var t = this._animators;
    return t.push(e), e.done(function() {
      var r = t.indexOf(e);
      r >= 0 && t.splice(r, 1);
    }), e;
  }
});
Object.defineProperty(ys.prototype, "autoRotate", {
  get: function(e) {
    return this._autoRotate;
  },
  set: function(e) {
    this._autoRotate = e, this._rotating = e;
  }
});
function Sn() {
}
Sn.prototype = {
  constructor: Sn,
  setScene: function(e) {
    this._scene = e, this._skybox && this._skybox.attachScene(this._scene);
  },
  initLight: function(e) {
    this._lightRoot = e, this.mainLight = new E.DirectionalLight({
      shadowBias: 5e-3
    }), this.ambientLight = new E.AmbientLight(), e.add(this.mainLight), e.add(this.ambientLight);
  },
  dispose: function() {
    this._lightRoot && (this._lightRoot.remove(this.mainLight), this._lightRoot.remove(this.ambientLight));
  },
  updateLight: function(e) {
    var t = this.mainLight, r = this.ambientLight, i = e.getModel("light"), n = i.getModel("main"), a = i.getModel("ambient");
    t.intensity = n.get("intensity"), r.intensity = a.get("intensity"), t.color = E.parseColor(n.get("color")).slice(0, 3), r.color = E.parseColor(a.get("color")).slice(0, 3);
    var o = n.get("alpha") || 0, s = n.get("beta") || 0;
    t.position.setArray(E.directionFromAlphaBeta(o, s)), t.lookAt(E.Vector3.ZERO), t.castShadow = n.get("shadow"), t.shadowResolution = E.getShadowResolution(n.get("shadowQuality"));
  },
  updateAmbientCubemap: function(e, t, r) {
    var i = t.getModel("light.ambientCubemap"), n = i.get("texture");
    if (n) {
      this._cubemapLightsCache = this._cubemapLightsCache || {};
      var a = this._cubemapLightsCache[n];
      if (!a) {
        var o = this;
        a = this._cubemapLightsCache[n] = E.createAmbientCubemap(i.option, e, r, function() {
          o._isSkyboxFromAmbientCubemap && o._skybox.setEnvironmentMap(a.specular.cubemap), r.getZr().refresh();
        });
      }
      this._lightRoot.add(a.diffuse), this._lightRoot.add(a.specular), this._currentCubemapLights = a;
    } else this._currentCubemapLights && (this._lightRoot.remove(this._currentCubemapLights.diffuse), this._lightRoot.remove(this._currentCubemapLights.specular), this._currentCubemapLights = null);
  },
  updateSkybox: function(e, t, r) {
    var i = t.get("environment"), n = this;
    function a() {
      return n._skybox = n._skybox || new Tr(), n._skybox;
    }
    var o = a();
    if (i && i !== "none")
      if (i === "auto")
        if (this._isSkyboxFromAmbientCubemap = !0, this._currentCubemapLights) {
          var s = this._currentCubemapLights.specular.cubemap;
          o.setEnvironmentMap(s), this._scene && o.attachScene(this._scene), o.material.set("lod", 3);
        } else this._skybox && this._skybox.detachScene();
      else if (typeof i == "object" && i.colorStops || typeof i == "string" && wo(i)) {
        this._isSkyboxFromAmbientCubemap = !1;
        var l = new E.Texture2D({
          anisotropic: 8,
          flipY: !1
        });
        o.setEnvironmentMap(l);
        var u = l.image = document.createElement("canvas");
        u.width = u.height = 16;
        var f = u.getContext("2d"), h = new bo({
          shape: {
            x: 0,
            y: 0,
            width: 16,
            height: 16
          },
          style: {
            fill: i
          }
        });
        Dn(f, h), o.attachScene(this._scene);
      } else {
        this._isSkyboxFromAmbientCubemap = !1;
        var l = E.loadTexture(i, r, {
          anisotropic: 8,
          flipY: !1
        });
        o.setEnvironmentMap(l), o.attachScene(this._scene);
      }
    else
      this._skybox && this._skybox.detachScene(this._scene), this._skybox = null;
    var d = t.coordinateSystem;
    if (this._skybox)
      if (d && d.viewGL && i !== "auto" && !(i.match && i.match(/.hdr$/))) {
        var c = d.viewGL.isLinearSpace() ? "define" : "undefine";
        this._skybox.material[c]("fragment", "SRGB_DECODE");
      } else
        this._skybox.material.undefine("fragment", "SRGB_DECODE");
  }
};
var bi = Ln.extend({
  type: "grid3D",
  dependencies: ["xAxis3D", "yAxis3D", "zAxis3D"],
  defaultOption: {
    show: !0,
    zlevel: -10,
    // Layout used for viewport
    left: 0,
    top: 0,
    width: "100%",
    height: "100%",
    environment: "auto",
    // Dimension of grid3D
    boxWidth: 100,
    boxHeight: 100,
    boxDepth: 100,
    // Common axis options.
    axisPointer: {
      show: !0,
      lineStyle: {
        color: "rgba(0, 0, 0, 0.8)",
        width: 1
      },
      label: {
        show: !0,
        // (dimValue: number, value: Array) => string
        formatter: null,
        // TODO, Consider boxWidth
        margin: 8,
        // backgroundColor: '#ffbd67',
        // borderColor: '#000',
        // borderWidth: 0,
        textStyle: {
          fontSize: 14,
          color: "#fff",
          backgroundColor: "rgba(0,0,0,0.5)",
          padding: 3,
          borderRadius: 3
        }
      }
    },
    axisLine: {
      show: !0,
      lineStyle: {
        color: "#333",
        width: 2,
        type: "solid"
      }
    },
    axisTick: {
      show: !0,
      inside: !1,
      length: 3,
      lineStyle: {
        width: 1
      }
    },
    axisLabel: {
      show: !0,
      inside: !1,
      rotate: 0,
      margin: 8,
      textStyle: {
        fontSize: 12
      }
    },
    splitLine: {
      show: !0,
      lineStyle: {
        color: ["#ccc"],
        width: 1,
        type: "solid"
      }
    },
    splitArea: {
      show: !1,
      areaStyle: {
        color: ["rgba(250,250,250,0.3)", "rgba(200,200,200,0.3)"]
      }
    },
    // Light options
    light: {
      main: {
        // Alpha angle for top-down rotation
        // Positive to rotate to top.
        alpha: 30,
        // beta angle for left-right rotation
        // Positive to rotate to right.
        beta: 40
      },
      ambient: {
        intensity: 0.4
      }
    },
    viewControl: {
      // Small damping for precise control.
      // damping: 0.1,
      // Alpha angle for top-down rotation
      // Positive to rotate to top.
      alpha: 20,
      // beta angle for left-right rotation
      // Positive to rotate to right.
      beta: 40,
      autoRotate: !1,
      // Distance to the surface of grid3D.
      distance: 200,
      // Min distance to the surface of grid3D
      minDistance: 40,
      // Max distance to the surface of grid3D
      maxDistance: 400
    }
  }
});
lt(bi.prototype, ac);
lt(bi.prototype, oc);
lt(bi.prototype, sc);
var Et = Ye.vec3, xs = $.extend(
  function() {
    return {
      segmentScale: 1,
      /**
       * Need to use mesh to expand lines if lineWidth > MAX_LINE_WIDTH
       */
      useNativeLine: !0,
      attributes: {
        position: new $.Attribute("position", "float", 3, "POSITION"),
        normal: new $.Attribute("normal", "float", 3, "NORMAL"),
        color: new $.Attribute("color", "float", 4, "COLOR")
      }
    };
  },
  /** @lends module: echarts-gl/util/geometry/QuadsGeometry.prototype */
  {
    /**
     * Reset offset
     */
    resetOffset: function() {
      this._vertexOffset = 0, this._faceOffset = 0;
    },
    /**
     * @param {number} nQuad
     */
    setQuadCount: function(e) {
      var t = this.attributes, r = this.getQuadVertexCount() * e, i = this.getQuadTriangleCount() * e;
      this.vertexCount !== r && (t.position.init(r), t.normal.init(r), t.color.init(r)), this.triangleCount !== i && (this.indices = r > 65535 ? new Uint32Array(i * 3) : new Uint16Array(i * 3));
    },
    getQuadVertexCount: function() {
      return 4;
    },
    getQuadTriangleCount: function() {
      return 2;
    },
    /**
     * Add a quad, which in following order:
     * 0-----1
     * 3-----2
     */
    addQuad: (function() {
      var e = Et.create(), t = Et.create(), r = Et.create(), i = [0, 3, 1, 3, 2, 1];
      return function(n, a) {
        var o = this.attributes.position, s = this.attributes.normal, l = this.attributes.color;
        Et.sub(e, n[1], n[0]), Et.sub(t, n[2], n[1]), Et.cross(r, e, t), Et.normalize(r, r);
        for (var u = 0; u < 4; u++)
          o.set(this._vertexOffset + u, n[u]), l.set(this._vertexOffset + u, a), s.set(this._vertexOffset + u, r);
        for (var f = this._faceOffset * 3, u = 0; u < 6; u++)
          this.indices[f + u] = i[u] + this._vertexOffset;
        this._vertexOffset += 4, this._faceOffset += 2;
      };
    })()
  }
);
_t(xs.prototype, wi);
var An = be.firstNotNull, Bc = {
  // Left to right
  x: 0,
  // Far to near
  y: 2,
  // Bottom to up
  z: 1
};
function Fc(e, t, r, i) {
  var n = [0, 0, 0], a = i < 0 ? r.getExtentMin() : r.getExtentMax();
  n[Bc[r.dim]] = a, e.position.setArray(n), e.rotation.identity(), t.distance = -Math.abs(a), t.normal.set(0, 0, 0), r.dim === "x" ? (e.rotation.rotateY(i * Math.PI / 2), t.normal.x = -i) : r.dim === "z" ? (e.rotation.rotateX(-i * Math.PI / 2), t.normal.y = -i) : (i > 0 && e.rotation.rotateY(Math.PI), t.normal.z = -i);
}
function Ei(e, t, r) {
  this.rootNode = new E.Node();
  var i = new E.Mesh({
    geometry: new Cr({
      useNativeLine: !1
    }),
    material: t,
    castShadow: !1,
    ignorePicking: !0,
    $ignorePicking: !0,
    renderOrder: 1
  }), n = new E.Mesh({
    geometry: new xs(),
    material: r,
    castShadow: !1,
    culling: !1,
    ignorePicking: !0,
    $ignorePicking: !0,
    renderOrder: 0
  });
  this.rootNode.add(n), this.rootNode.add(i), this.faceInfo = e, this.plane = new E.Plane(), this.linesMesh = i, this.quadsMesh = n;
}
Ei.prototype.update = function(e, t, r) {
  var i = e.coordinateSystem, n = [i.getAxis(this.faceInfo[0]), i.getAxis(this.faceInfo[1])], a = this.linesMesh.geometry, o = this.quadsMesh.geometry;
  a.convertToDynamicArray(!0), o.convertToDynamicArray(!0), this._updateSplitLines(a, n, e, r), this._udpateSplitAreas(o, n, e, r), a.convertToTypedArray(), o.convertToTypedArray();
  var s = i.getAxis(this.faceInfo[2]);
  Fc(this.rootNode, this.plane, s, this.faceInfo[3]);
};
Ei.prototype._updateSplitLines = function(e, t, r, i) {
  var n = i.getDevicePixelRatio();
  t.forEach(function(a, o) {
    var s = a.model, l = t[1 - o].getExtent();
    if (!a.scale.isBlank()) {
      var u = s.getModel("splitLine", r.getModel("splitLine"));
      if (u.get("show")) {
        var f = u.getModel("lineStyle"), h = f.get("color"), d = An(f.get("opacity"), 1), c = An(f.get("width"), 1);
        h = it(h) ? h : [h];
        for (var v = a.getTicksCoords({
          tickModel: u
        }), p = 0, m = 0; m < v.length; m++) {
          var _ = v[m].coord, x = E.parseColor(h[p % h.length]);
          x[3] *= d;
          var y = [0, 0, 0], g = [0, 0, 0];
          y[o] = g[o] = _, y[1 - o] = l[0], g[1 - o] = l[1], e.addLine(y, g, x, c * n), p++;
        }
      }
    }
  });
};
Ei.prototype._udpateSplitAreas = function(e, t, r, i) {
  t.forEach(function(n, a) {
    var o = n.model, s = t[1 - a].getExtent();
    if (!n.scale.isBlank()) {
      var l = o.getModel("splitArea", r.getModel("splitArea"));
      if (l.get("show")) {
        var u = l.getModel("areaStyle"), f = u.get("color"), h = An(u.get("opacity"), 1);
        f = it(f) ? f : [f];
        for (var d = n.getTicksCoords({
          tickModel: l,
          clamp: !0
        }), c = 0, v = [0, 0, 0], p = [0, 0, 0], m = 0; m < d.length; m++) {
          var _ = d[m].coord, x = [0, 0, 0], y = [0, 0, 0];
          if (x[a] = y[a] = _, x[1 - a] = s[0], y[1 - a] = s[1], m === 0) {
            v = x, p = y;
            continue;
          }
          var g = E.parseColor(f[c % f.length]);
          g[3] *= h, e.addQuad([v, x, y, p], g), v = x, p = y, c++;
        }
      }
    }
  });
};
var St = be.firstNotNull, At = {
  // Left to right
  x: 0,
  // Far to near
  y: 2,
  // Bottom to up
  z: 1
};
function Xn(e, t) {
  var r = new E.Mesh({
    geometry: new Cr({
      useNativeLine: !1
    }),
    material: t,
    castShadow: !1,
    ignorePicking: !0,
    renderOrder: 2
  }), i = new Vn();
  i.material.depthMask = !1;
  var n = new E.Node();
  n.add(r), n.add(i), this.rootNode = n, this.dim = e, this.linesMesh = r, this.labelsMesh = i, this.axisLineCoords = null, this.labelElements = [];
}
var un = {
  x: "y",
  y: "x",
  z: "y"
};
Xn.prototype.update = function(e, t, r) {
  var i = e.coordinateSystem, n = i.getAxis(this.dim), a = this.linesMesh.geometry, o = this.labelsMesh.geometry;
  a.convertToDynamicArray(!0), o.convertToDynamicArray(!0);
  var s = n.model, l = n.getExtent(), O = r.getDevicePixelRatio(), u = s.getModel("axisLine", e.getModel("axisLine")), f = s.getModel("axisTick", e.getModel("axisTick")), h = s.getModel("axisLabel", e.getModel("axisLabel")), d = u.get("lineStyle.color");
  if (u.get("show")) {
    var c = u.getModel("lineStyle"), v = [0, 0, 0], p = [0, 0, 0], m = At[n.dim];
    v[m] = l[0], p[m] = l[1], this.axisLineCoords = [v, p];
    var _ = E.parseColor(d), x = St(c.get("width"), 1), y = St(c.get("opacity"), 1);
    _[3] *= y, a.addLine(v, p, _, x * O);
  }
  if (f.get("show")) {
    var g = f.getModel("lineStyle"), b = E.parseColor(St(g.get("color"), d)), x = St(g.get("width"), 1);
    b[3] *= St(g.get("opacity"), 1);
    for (var w = n.getTicksCoords(), C = f.get("length"), A = 0; A < w.length; A++) {
      var R = w[A].coord, v = [0, 0, 0], p = [0, 0, 0], m = At[n.dim], H = At[un[n.dim]];
      v[m] = p[m] = R, p[H] = C, a.addLine(v, p, b, x * O);
    }
  }
  this.labelElements = [];
  var O = r.getDevicePixelRatio();
  if (h.get("show"))
    for (var w = n.getTicksCoords(), z = s.get("data"), G = h.get("margin"), F = n.getViewLabels(), A = 0; A < F.length; A++) {
      var W = Eo(F[A].tick) ? F[A].tick.value : F[A].tickValue, X = F[A].formattedLabel, q = F[A].rawLabel, R = n.dataToCoord(W), ie = [0, 0, 0], m = At[n.dim], H = At[un[n.dim]];
      ie[m] = ie[m] = R, ie[H] = G;
      var me = h;
      z && z[W] && z[W].textStyle && (me = new xo(z[W].textStyle, h, s.ecModel));
      var Se = St(me.get("color"), d), ce = new ni({
        style: ai(me, {
          text: X,
          fill: typeof Se == "function" ? Se(
            // (1) In category axis with data zoom, tick is not the original
            // index of axis.data. So tick should not be exposed to user
            // in category axis.
            // (2) Compatible with previous version, which always returns labelStr.
            // But in interval scale labelStr is like '223,445', which maked
            // user repalce ','. So we modify it to return original val but remain
            // it as 'string' to avoid error in replacing.
            n.type === "category" ? q : n.type === "value" ? W + "" : W,
            A
          ) : Se,
          verticalAlign: "top",
          align: "left"
        })
      }), ye = t.add(ce), de = ce.getBoundingRect();
      o.addSprite(ie, [de.width * O, de.height * O], ye), this.labelElements.push(ce);
    }
  if (s.get("name")) {
    var Ce = s.getModel("nameTextStyle"), ie = [0, 0, 0], m = At[n.dim], H = At[un[n.dim]], Fe = St(Ce.get("color"), d), Ie = Ce.get("borderColor"), x = Ce.get("borderWidth");
    ie[m] = ie[m] = (l[0] + l[1]) / 2, ie[H] = s.get("nameGap");
    var ce = new ni({
      style: ai(Ce, {
        text: s.get("name"),
        fill: Fe,
        stroke: Ie,
        lineWidth: x
      })
    }), ye = t.add(ce), de = ce.getBoundingRect();
    o.addSprite(ie, [de.width * O, de.height * O], ye), ce.__idx = this.labelElements.length, this.nameLabelElement = ce;
  }
  this.labelsMesh.material.set("textureAtlas", t.getTexture()), this.labelsMesh.material.set("uvScale", t.getCoordsScale()), a.convertToTypedArray(), o.convertToTypedArray();
};
Xn.prototype.setSpriteAlign = function(e, t, r) {
  for (var i = r.getDevicePixelRatio(), n = this.labelsMesh.geometry, a = 0; a < this.labelElements.length; a++) {
    var o = this.labelElements[a], s = o.getBoundingRect();
    n.setSpriteAlign(a, [s.width * i, s.height * i], e, t);
  }
  var l = this.nameLabelElement;
  if (l) {
    var s = l.getBoundingRect();
    n.setSpriteAlign(l.__idx, [s.width * i, s.height * i], e, t), n.dirty();
  }
  this.textAlign = e, this.textVerticalAlign = t;
};
var po = be.firstNotNull;
E.Shader.import(as);
var Ct = {
  // Left to right
  x: 0,
  // Far to near
  y: 2,
  // Bottom to up
  z: 1
};
const Uc = go.extend({
  type: "grid3D",
  __ecgl__: !0,
  init: function(e, t) {
    var r = [
      // planeDim0, planeDim1, offsetDim, dir on dim3 axis(gl), plane.
      ["y", "z", "x", -1, "left"],
      ["y", "z", "x", 1, "right"],
      ["x", "y", "z", -1, "bottom"],
      ["x", "y", "z", 1, "top"],
      ["x", "z", "y", -1, "far"],
      ["x", "z", "y", 1, "near"]
    ], i = ["x", "y", "z"], n = new E.Material({
      // transparent: true,
      shader: E.createShader("ecgl.color"),
      depthMask: !1,
      transparent: !0
    }), a = new E.Material({
      // transparent: true,
      shader: E.createShader("ecgl.meshLines3D"),
      depthMask: !1,
      transparent: !0
    });
    n.define("fragment", "DOUBLE_SIDED"), n.define("both", "VERTEX_COLOR"), this.groupGL = new E.Node(), this._control = new ys({
      zr: t.getZr()
    }), this._control.init(), this._faces = r.map(function(s) {
      var l = new Ei(s, a, n);
      return this.groupGL.add(l.rootNode), l;
    }, this), this._axes = i.map(function(s) {
      var l = new Xn(s, a);
      return this.groupGL.add(l.rootNode), l;
    }, this);
    var o = t.getDevicePixelRatio();
    this._axisLabelSurface = new hi({
      width: 256,
      height: 256,
      devicePixelRatio: o
    }), this._axisLabelSurface.onupdate = function() {
      t.getZr().refresh();
    }, this._axisPointerLineMesh = new E.Mesh({
      geometry: new Cr({
        useNativeLine: !1
      }),
      material: a,
      castShadow: !1,
      // PENDING
      ignorePicking: !0,
      renderOrder: 3
    }), this.groupGL.add(this._axisPointerLineMesh), this._axisPointerLabelsSurface = new hi({
      width: 128,
      height: 128,
      devicePixelRatio: o
    }), this._axisPointerLabelsMesh = new Vn({
      ignorePicking: !0,
      renderOrder: 4,
      castShadow: !1
    }), this._axisPointerLabelsMesh.material.set("textureAtlas", this._axisPointerLabelsSurface.getTexture()), this.groupGL.add(this._axisPointerLabelsMesh), this._lightRoot = new E.Node(), this._sceneHelper = new Sn(), this._sceneHelper.initLight(this._lightRoot);
  },
  render: function(e, t, r) {
    this._model = e, this._api = r;
    var i = e.coordinateSystem;
    i.viewGL.add(this._lightRoot), e.get("show") ? i.viewGL.add(this.groupGL) : i.viewGL.remove(this.groupGL);
    var n = this._control;
    n.setViewGL(i.viewGL);
    var a = e.getModel("viewControl");
    n.setFromViewControlModel(a, 0), this._axisLabelSurface.clear(), n.off("update"), e.get("show") && (this._faces.forEach(function(o) {
      o.update(e, t, r);
    }, this), this._axes.forEach(function(o) {
      o.update(e, this._axisLabelSurface, r);
    }, this)), n.on("update", this._onCameraChange.bind(this, e, r), this), this._sceneHelper.setScene(i.viewGL.scene), this._sceneHelper.updateLight(e), i.viewGL.setPostEffect(e.getModel("postEffect"), r), i.viewGL.setTemporalSuperSampling(e.getModel("temporalSuperSampling")), this._initMouseHandler(e);
  },
  afterRender: function(e, t, r, i) {
    var n = i.renderer;
    this._sceneHelper.updateAmbientCubemap(n, e, r), this._sceneHelper.updateSkybox(n, e, r);
  },
  /**
   * showAxisPointer will be triggered by action.
   */
  showAxisPointer: function(e, t, r, i) {
    this._doShowAxisPointer(), this._updateAxisPointer(i.value);
  },
  /**
   * hideAxisPointer will be triggered by action.
   */
  hideAxisPointer: function(e, t, r, i) {
    this._doHideAxisPointer();
  },
  _initMouseHandler: function(e) {
    var t = e.coordinateSystem, r = t.viewGL;
    e.get("show") && e.get("axisPointer.show") ? r.on("mousemove", this._updateAxisPointerOnMousePosition, this) : r.off("mousemove", this._updateAxisPointerOnMousePosition);
  },
  /**
   * Try find and show axisPointer on the intersect point
   * of mouse ray with grid plane.
   */
  _updateAxisPointerOnMousePosition: function(e) {
    if (!e.target) {
      for (var t = this._model, r = t.coordinateSystem, i = r.viewGL, n = i.castRay(e.offsetX, e.offsetY, new E.Ray()), a, o = 0; o < this._faces.length; o++) {
        var s = this._faces[o];
        if (!s.rootNode.invisible) {
          s.plane.normal.dot(i.camera.worldTransform.z) < 0 && s.plane.normal.negate();
          var l = n.intersectPlane(s.plane);
          if (l) {
            var u = r.getAxis(s.faceInfo[0]), f = r.getAxis(s.faceInfo[1]), h = Ct[s.faceInfo[0]], d = Ct[s.faceInfo[1]];
            u.contain(l.array[h]) && f.contain(l.array[d]) && (a = l);
          }
        }
      }
      if (a) {
        var c = r.pointToData(a.array, [], !0);
        this._updateAxisPointer(c), this._doShowAxisPointer();
      } else
        this._doHideAxisPointer();
    }
  },
  _onCameraChange: function(e, t) {
    e.get("show") && (this._updateFaceVisibility(), this._updateAxisLinePosition());
    var r = this._control;
    t.dispatchAction({
      type: "grid3DChangeCamera",
      alpha: r.getAlpha(),
      beta: r.getBeta(),
      distance: r.getDistance(),
      center: r.getCenter(),
      from: this.uid,
      grid3DId: e.id
    });
  },
  /**
   * Update visibility of each face when camera view changed, front face will be invisible.
   * @private
   */
  _updateFaceVisibility: function() {
    var e = this._control.getCamera(), t = new E.Vector3();
    e.update();
    for (var r = 0; r < this._faces.length / 2; r++) {
      for (var i = [], n = 0; n < 2; n++) {
        var a = this._faces[r * 2 + n];
        a.rootNode.getWorldPosition(t), t.transformMat4(e.viewMatrix), i[n] = t.z;
      }
      var o = i[0] > i[1] ? 0 : 1, s = this._faces[r * 2 + o], l = this._faces[r * 2 + 1 - o];
      s.rootNode.invisible = !0, l.rootNode.invisible = !1;
    }
  },
  /**
   * Update axis line position when camera view changed.
   * @private
   */
  _updateAxisLinePosition: function() {
    var e = this._model.coordinateSystem, t = e.getAxis("x"), r = e.getAxis("y"), i = e.getAxis("z"), n = i.getExtentMax(), a = i.getExtentMin(), o = t.getExtentMin(), s = t.getExtentMax(), l = r.getExtentMax(), u = r.getExtentMin(), f = this._axes[0].rootNode, h = this._axes[1].rootNode, d = this._axes[2].rootNode, c = this._faces, v = c[4].rootNode.invisible ? u : l, p = c[2].rootNode.invisible ? n : a, m = c[0].rootNode.invisible ? o : s, _ = c[2].rootNode.invisible ? n : a, x = c[0].rootNode.invisible ? s : o, y = c[4].rootNode.invisible ? u : l;
    f.rotation.identity(), h.rotation.identity(), d.rotation.identity(), c[4].rootNode.invisible && (this._axes[0].flipped = !0, f.rotation.rotateX(Math.PI)), c[0].rootNode.invisible && (this._axes[1].flipped = !0, h.rotation.rotateZ(Math.PI)), c[4].rootNode.invisible && (this._axes[2].flipped = !0, d.rotation.rotateY(Math.PI)), f.position.set(0, p, v), h.position.set(m, _, 0), d.position.set(x, 0, y), f.update(), h.update(), d.update(), this._updateAxisLabelAlign();
  },
  /**
   * Update label align on axis when axisLine position changed.
   * @private
   */
  _updateAxisLabelAlign: function() {
    var e = this._control.getCamera(), t = [new E.Vector4(), new E.Vector4()], r = new E.Vector4();
    this.groupGL.getWorldPosition(r), r.w = 1, r.transformMat4(e.viewMatrix).transformMat4(e.projectionMatrix), r.x /= r.w, r.y /= r.w, this._axes.forEach(function(i) {
      var n = i.axisLineCoords;
      i.labelsMesh.geometry;
      for (var a = 0; a < t.length; a++)
        t[a].setArray(n[a]), t[a].w = 1, t[a].transformMat4(i.rootNode.worldTransform).transformMat4(e.viewMatrix).transformMat4(e.projectionMatrix), t[a].x /= t[a].w, t[a].y /= t[a].w;
      var o = t[1].x - t[0].x, s = t[1].y - t[0].y, l = (t[1].x + t[0].x) / 2, u = (t[1].y + t[0].y) / 2, f, h;
      Math.abs(s / o) < 0.5 ? (f = "center", h = u > r.y ? "bottom" : "top") : (h = "middle", f = l > r.x ? "left" : "right"), i.setSpriteAlign(f, h, this._api);
    }, this);
  },
  _doShowAxisPointer: function() {
    this._axisPointerLineMesh.invisible && (this._axisPointerLineMesh.invisible = !1, this._axisPointerLabelsMesh.invisible = !1, this._api.getZr().refresh());
  },
  _doHideAxisPointer: function() {
    this._axisPointerLineMesh.invisible || (this._axisPointerLineMesh.invisible = !0, this._axisPointerLabelsMesh.invisible = !0, this._api.getZr().refresh());
  },
  /**
   * @private updateAxisPointer.
   */
  _updateAxisPointer: function(e) {
    var t = this._model.coordinateSystem, r = t.dataToPoint(e), i = this._axisPointerLineMesh, n = i.geometry, a = this._model.getModel("axisPointer"), o = this._api.getDevicePixelRatio();
    n.convertToDynamicArray(!0);
    function s(A) {
      return be.firstNotNull(A.model.get("axisPointer.show"), a.get("show"));
    }
    function l(A) {
      var R = A.model.getModel("axisPointer", a), H = R.getModel("lineStyle"), O = E.parseColor(H.get("color")), z = po(H.get("width"), 1), G = po(H.get("opacity"), 1);
      return O[3] *= G, {
        color: O,
        lineWidth: z
      };
    }
    for (var u = 0; u < this._faces.length; u++) {
      var f = this._faces[u];
      if (!f.rootNode.invisible) {
        for (var h = f.faceInfo, d = h[3] < 0 ? t.getAxis(h[2]).getExtentMin() : t.getAxis(h[2]).getExtentMax(), c = Ct[h[2]], v = 0; v < 2; v++) {
          var p = h[v], m = h[1 - v], _ = t.getAxis(p), x = t.getAxis(m);
          if (s(_)) {
            var y = [0, 0, 0], g = [0, 0, 0], b = Ct[p], w = Ct[m];
            y[b] = g[b] = r[b], y[c] = g[c] = d, y[w] = x.getExtentMin(), g[w] = x.getExtentMax();
            var C = l(_);
            n.addLine(y, g, C.color, C.lineWidth * o);
          }
        }
        if (s(t.getAxis(h[2]))) {
          var y = r.slice(), g = r.slice();
          g[c] = d;
          var C = l(t.getAxis(h[2]));
          n.addLine(y, g, C.color, C.lineWidth * o);
        }
      }
    }
    n.convertToTypedArray(), this._updateAxisPointerLabelsMesh(e), this._api.getZr().refresh();
  },
  _updateAxisPointerLabelsMesh: function(e) {
    var t = this._model, r = this._axisPointerLabelsMesh, i = this._axisPointerLabelsSurface, n = t.coordinateSystem, a = t.getModel("axisPointer");
    r.geometry.convertToDynamicArray(!0), i.clear();
    var o = {
      x: "y",
      y: "x",
      z: "y"
    };
    this._axes.forEach(function(s, l) {
      var u = n.getAxis(s.dim), f = u.model, h = f.getModel("axisPointer", a), d = h.getModel("label"), c = h.get("lineStyle.color");
      if (!(!d.get("show") || !h.get("show"))) {
        var v = e[l], p = d.get("formatter"), m = u.scale.getLabel({
          value: v
        });
        if (p != null)
          m = p(m, e);
        else if (u.scale.type === "interval" || u.scale.type === "log") {
          var _ = Fl(u.scale.getTicks()[0]);
          m = v.toFixed(_ + 2);
        }
        var x = d.get("color"), y = new ni({
          style: ai(d, {
            text: m,
            fill: x || c,
            align: "left",
            verticalAlign: "top"
          })
        }), g = i.add(y), b = y.getBoundingRect(), w = this._api.getDevicePixelRatio(), C = s.rootNode.position.toArray(), A = Ct[o[s.dim]];
        C[A] += (s.flipped ? -1 : 1) * d.get("margin"), C[Ct[s.dim]] = u.dataToCoord(e[l]), r.geometry.addSprite(C, [b.width * w, b.height * w], g, s.textAlign, s.textVerticalAlign);
      }
    }, this), i.getZr().refreshImmediately(), r.material.set("uvScale", i.getCoordsScale()), r.geometry.convertToTypedArray();
  },
  dispose: function() {
    this.groupGL.removeAll(), this._control.dispose(), this._axisLabelSurface.dispose(), this._axisPointerLabelsSurface.dispose();
  }
});
function Er(e) {
  Ao.call(this, e), this.type = "cartesian3D", this.dimensions = ["x", "y", "z"], this.size = [0, 0, 0];
}
Er.prototype = {
  constructor: Er,
  model: null,
  containPoint: function(e) {
    return this.getAxis("x").contain(e[0]) && this.getAxis("y").contain(e[2]) && this.getAxis("z").contain(e[1]);
  },
  containData: function(e) {
    return this.getAxis("x").containData(e[0]) && this.getAxis("y").containData(e[1]) && this.getAxis("z").containData(e[2]);
  },
  dataToPoint: function(e, t, r) {
    return t = t || [], t[0] = this.getAxis("x").dataToCoord(e[0], r), t[2] = this.getAxis("y").dataToCoord(e[1], r), t[1] = this.getAxis("z").dataToCoord(e[2], r), t;
  },
  pointToData: function(e, t, r) {
    return t = t || [], t[0] = this.getAxis("x").coordToData(e[0], r), t[1] = this.getAxis("y").coordToData(e[2], r), t[2] = this.getAxis("z").coordToData(e[1], r), t;
  }
};
Co(Er, Ao);
function pi(e, t, r) {
  Mn.call(this, e, t, r);
}
pi.prototype = {
  constructor: pi,
  getExtentMin: function() {
    var e = this._extent;
    return Math.min(e[0], e[1]);
  },
  getExtentMax: function() {
    var e = this._extent;
    return Math.max(e[0], e[1]);
  },
  calculateCategoryInterval: function() {
    return Math.floor(this.scale.count() / 8);
  }
};
Co(pi, Mn);
function zc(e, t) {
  var r = e.getBoxLayoutParams(), i = Ul(r, {
    width: t.getWidth(),
    height: t.getHeight()
  });
  i.y = t.getHeight() - i.y - i.height, this.viewGL.setViewport(i.x, i.y, i.width, i.height, t.getDevicePixelRatio());
  var n = e.get("boxWidth"), a = e.get("boxHeight"), o = e.get("boxDepth");
  this.getAxis("x").setExtent(-n / 2, n / 2), this.getAxis("y").setExtent(o / 2, -o / 2), this.getAxis("z").setExtent(-a / 2, a / 2), this.size = [n, a, o];
}
function Hc(e, t) {
  var r = {};
  function i(n, a) {
    r[n] = r[n] || [1 / 0, -1 / 0], r[n][0] = Math.min(a[0], r[n][0]), r[n][1] = Math.max(a[1], r[n][1]);
  }
  e.eachSeries(function(n) {
    if (n.coordinateSystem === this) {
      var a = n.getData();
      ["x", "y", "z"].forEach(function(o) {
        a.mapDimensionsAll(o, !0).forEach(function(s) {
          i(o, a.getDataExtent(s, !0));
        });
      });
    }
  }, this), ["xAxis3D", "yAxis3D", "zAxis3D"].forEach(function(n) {
    e.eachComponent(n, function(a) {
      var o = n.charAt(0), s = a.getReferringComponents("grid3D").models[0], l = s.coordinateSystem;
      if (l === this) {
        var u = l.getAxis(o);
        if (!u) {
          var f = zl(r[o] || [1 / 0, -1 / 0], a);
          u = new pi(o, f), u.type = a.get("type");
          var h = u.type === "category";
          u.onBand = h && a.get("boundaryGap"), u.inverse = a.get("inverse"), a.axis = u, u.model = a, u.getLabelModel = function() {
            return a.getModel("axisLabel", s.getModel("axisLabel"));
          }, u.getTickModel = function() {
            return a.getModel("axisTick", s.getModel("axisTick"));
          }, l.addAxis(u);
        }
      }
    }, this);
  }, this), this.resize(this.model, t);
}
var Gc = {
  dimensions: Er.prototype.dimensions,
  create: function(e, t) {
    var r = [];
    e.eachComponent("grid3D", function(a) {
      a.__viewGL = a.__viewGL || new se();
      var o = new Er();
      o.model = a, o.viewGL = a.__viewGL, a.coordinateSystem = o, r.push(o), o.resize = zc, o.update = Hc;
    });
    var i = ["xAxis3D", "yAxis3D", "zAxis3D"];
    function n(a, o) {
      return i.map(function(s) {
        var l = a.getReferringComponents(s).models[0];
        return l == null && (l = o.getComponent(s)), l;
      });
    }
    return e.eachSeries(function(a) {
      if (a.get("coordinateSystem") === "cartesian3D") {
        var o = a.getReferringComponents("grid3D").models[0];
        if (o == null) {
          var s = n(a, e), o = s[0].getCoordSysModel();
          s.forEach(function(f) {
            f.getCoordSysModel();
          });
        }
        var l = o.coordinateSystem;
        a.coordinateSystem = l;
      }
    }), r;
  }
}, Ts = Ln.extend({
  type: "cartesian3DAxis",
  axis: null,
  /**
   * @override
   */
  getCoordSysModel: function() {
    return this.ecModel.queryComponents({
      mainType: "grid3D",
      index: this.option.gridIndex,
      id: this.option.gridId
    })[0];
  }
});
Hl(Ts);
var ws = {
  show: !0,
  grid3DIndex: 0,
  // 反向坐标轴
  inverse: !1,
  // 坐标轴名字
  name: "",
  // 坐标轴名字位置
  nameLocation: "middle",
  nameTextStyle: {
    fontSize: 16
  },
  // 文字与轴线距离
  nameGap: 20,
  axisPointer: {},
  axisLine: {},
  // 坐标轴小标记
  axisTick: {},
  axisLabel: {},
  // 分隔区域
  splitArea: {}
}, Vc = lt({
  // 类目起始和结束两端空白策略
  boundaryGap: !0,
  // splitArea: {
  // show: false
  // },
  // 坐标轴小标记
  axisTick: {
    // If tick is align with label when boundaryGap is true
    // Default with axisTick
    alignWithLabel: !1,
    interval: "auto"
  },
  // 坐标轴文本标签，详见axis.axisLabel
  axisLabel: {
    interval: "auto"
  },
  axisPointer: {
    label: {
      show: !1
    }
  }
}, ws), qn = lt({
  // 数值起始和结束两端空白策略
  boundaryGap: [0, 0],
  // 最小值, 设置成 'dataMin' 则从数据中计算最小值
  // min: null,
  // 最大值，设置成 'dataMax' 则从数据中计算最大值
  // max: null,
  // 脱离0值比例，放大聚焦到最终_min，_max区间
  // scale: false,
  // 分割段数，默认为5
  splitNumber: 5,
  // Minimum interval
  // minInterval: null
  axisPointer: {
    label: {}
  }
}, ws), kc = _t({
  scale: !0,
  min: "dataMin",
  max: "dataMax"
}, qn), bs = _t({
  logBase: 10
}, qn);
bs.scale = !0;
const Wc = {
  categoryAxis3D: Vc,
  valueAxis3D: qn,
  timeAxis3D: kc,
  logAxis3D: bs
};
var Xc = ["value", "category", "time", "log"];
function qc(e, t, r, i, n) {
  Xc.forEach(function(a) {
    var o = r.extend({
      type: t + "Axis3D." + a,
      /**
       * @type readOnly
       */
      __ordinalMeta: null,
      mergeDefaultAndTheme: function(s, l) {
        var u = l.getTheme();
        lt(s, u.get(a + "Axis3D")), lt(s, this.getDefaultOption()), s.type = i(t, s);
      },
      /**
       * @override
       */
      optionUpdated: function() {
        var s = this.option;
        s.type === "category" && (this.__ordinalMeta = Vl.createByAxisModel(this));
      },
      getCategories: function() {
        if (this.option.type === "category")
          return this.__ordinalMeta.categories;
      },
      getOrdinalMeta: function() {
        return this.__ordinalMeta;
      },
      defaultOption: lt(Gl(Wc[a + "Axis3D"]), n || {}, !0)
    });
    e.registerComponentModel(o);
  }), e.registerSubTypeDefaulter(t + "Axis3D", kl(i, t));
}
function jc(e, t) {
  return t.type || (t.data ? "category" : "value");
}
function Zc(e) {
  e.registerComponentModel(bi), e.registerComponentView(Uc), e.registerCoordinateSystem("grid3D", Gc), ["x", "y", "z"].forEach(function(t) {
    qc(e, t, Ts, jc, {
      name: t.toUpperCase()
    });
    const r = e.ComponentView.extend({
      type: t + "Axis3D"
    });
    e.registerComponentView(r);
  }), e.registerAction({
    type: "grid3DChangeCamera",
    event: "grid3dcamerachanged",
    update: "series:updateCamera"
  }, function(t, r) {
    r.eachComponent({
      mainType: "grid3D",
      query: t
    }, function(i) {
      i.setView(t);
    });
  }), e.registerAction({
    type: "grid3DShowAxisPointer",
    event: "grid3dshowaxispointer",
    update: "grid3D:showAxisPointer"
  }, function(t, r) {
  }), e.registerAction({
    type: "grid3DHideAxisPointer",
    event: "grid3dhideaxispointer",
    update: "grid3D:hideAxisPointer"
  }, function(t, r) {
  });
}
mi([
  Wh,
  Yh,
  nc,
  Zc,
  ql,
  jl,
  mo
]);
function $c({ echartsRef: e, option: t }) {
  return /* @__PURE__ */ Es.jsx(
    Wl,
    {
      ref: e,
      echarts: Xl,
      option: t,
      style: { width: "100%", height: "100%" },
      notMerge: !0
    }
  );
}
export {
  $c as default
};
