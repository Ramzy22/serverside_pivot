import { b as J0, r as eS, g as tS } from "./charts-DthW1Q8W.js";
var Ga = {};
/*! *****************************************************************************
Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
***************************************************************************** */
var Yl = function(t, e) {
  return Yl = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, Yl(t, e);
};
function F(t, e) {
  if (typeof e != "function" && e !== null)
    throw new TypeError("Class extends value " + String(e) + " is not a constructor or null");
  Yl(t, e);
  function r() {
    this.constructor = t;
  }
  t.prototype = e === null ? Object.create(e) : (r.prototype = e.prototype, new r());
}
var Wl = function() {
  return Wl = Object.assign || function(e) {
    for (var r, n = 1, i = arguments.length; n < i; n++) {
      r = arguments[n];
      for (var a in r) Object.prototype.hasOwnProperty.call(r, a) && (e[a] = r[a]);
    }
    return e;
  }, Wl.apply(this, arguments);
};
function rS(t, e) {
  var r = {};
  for (var n in t) Object.prototype.hasOwnProperty.call(t, n) && e.indexOf(n) < 0 && (r[n] = t[n]);
  if (t != null && typeof Object.getOwnPropertySymbols == "function")
    for (var i = 0, n = Object.getOwnPropertySymbols(t); i < n.length; i++)
      e.indexOf(n[i]) < 0 && Object.prototype.propertyIsEnumerable.call(t, n[i]) && (r[n[i]] = t[n[i]]);
  return r;
}
function nS(t, e, r, n) {
  var i = arguments.length, a = i < 3 ? e : n === null ? n = Object.getOwnPropertyDescriptor(e, r) : n, o;
  if (typeof Reflect == "object" && typeof Reflect.decorate == "function") a = Reflect.decorate(t, e, r, n);
  else for (var s = t.length - 1; s >= 0; s--) (o = t[s]) && (a = (i < 3 ? o(a) : i > 3 ? o(e, r, a) : o(e, r)) || a);
  return i > 3 && a && Object.defineProperty(e, r, a), a;
}
function iS(t, e) {
  return function(r, n) {
    e(r, n, t);
  };
}
function aS(t, e) {
  if (typeof Reflect == "object" && typeof Reflect.metadata == "function") return Reflect.metadata(t, e);
}
function oS(t, e, r, n) {
  function i(a) {
    return a instanceof r ? a : new r(function(o) {
      o(a);
    });
  }
  return new (r || (r = Promise))(function(a, o) {
    function s(f) {
      try {
        l(n.next(f));
      } catch (h) {
        o(h);
      }
    }
    function u(f) {
      try {
        l(n.throw(f));
      } catch (h) {
        o(h);
      }
    }
    function l(f) {
      f.done ? a(f.value) : i(f.value).then(s, u);
    }
    l((n = n.apply(t, e || [])).next());
  });
}
function sS(t, e) {
  var r = { label: 0, sent: function() {
    if (a[0] & 1) throw a[1];
    return a[1];
  }, trys: [], ops: [] }, n, i, a, o;
  return o = { next: s(0), throw: s(1), return: s(2) }, typeof Symbol == "function" && (o[Symbol.iterator] = function() {
    return this;
  }), o;
  function s(l) {
    return function(f) {
      return u([l, f]);
    };
  }
  function u(l) {
    if (n) throw new TypeError("Generator is already executing.");
    for (; r; ) try {
      if (n = 1, i && (a = l[0] & 2 ? i.return : l[0] ? i.throw || ((a = i.return) && a.call(i), 0) : i.next) && !(a = a.call(i, l[1])).done) return a;
      switch (i = 0, a && (l = [l[0] & 2, a.value]), l[0]) {
        case 0:
        case 1:
          a = l;
          break;
        case 4:
          return r.label++, { value: l[1], done: !1 };
        case 5:
          r.label++, i = l[1], l = [0];
          continue;
        case 7:
          l = r.ops.pop(), r.trys.pop();
          continue;
        default:
          if (a = r.trys, !(a = a.length > 0 && a[a.length - 1]) && (l[0] === 6 || l[0] === 2)) {
            r = 0;
            continue;
          }
          if (l[0] === 3 && (!a || l[1] > a[0] && l[1] < a[3])) {
            r.label = l[1];
            break;
          }
          if (l[0] === 6 && r.label < a[1]) {
            r.label = a[1], a = l;
            break;
          }
          if (a && r.label < a[2]) {
            r.label = a[2], r.ops.push(l);
            break;
          }
          a[2] && r.ops.pop(), r.trys.pop();
          continue;
      }
      l = e.call(t, r);
    } catch (f) {
      l = [6, f], i = 0;
    } finally {
      n = a = 0;
    }
    if (l[0] & 5) throw l[1];
    return { value: l[0] ? l[1] : void 0, done: !0 };
  }
}
var oh = Object.create ? (function(t, e, r, n) {
  n === void 0 && (n = r), Object.defineProperty(t, n, { enumerable: !0, get: function() {
    return e[r];
  } });
}) : (function(t, e, r, n) {
  n === void 0 && (n = r), t[n] = e[r];
});
function uS(t, e) {
  for (var r in t) r !== "default" && !Object.prototype.hasOwnProperty.call(e, r) && oh(e, t, r);
}
function Xl(t) {
  var e = typeof Symbol == "function" && Symbol.iterator, r = e && t[e], n = 0;
  if (r) return r.call(t);
  if (t && typeof t.length == "number") return {
    next: function() {
      return t && n >= t.length && (t = void 0), { value: t && t[n++], done: !t };
    }
  };
  throw new TypeError(e ? "Object is not iterable." : "Symbol.iterator is not defined.");
}
function Cg(t, e) {
  var r = typeof Symbol == "function" && t[Symbol.iterator];
  if (!r) return t;
  var n = r.call(t), i, a = [], o;
  try {
    for (; (e === void 0 || e-- > 0) && !(i = n.next()).done; ) a.push(i.value);
  } catch (s) {
    o = { error: s };
  } finally {
    try {
      i && !i.done && (r = n.return) && r.call(n);
    } finally {
      if (o) throw o.error;
    }
  }
  return a;
}
function lS() {
  for (var t = [], e = 0; e < arguments.length; e++)
    t = t.concat(Cg(arguments[e]));
  return t;
}
function fS() {
  for (var t = 0, e = 0, r = arguments.length; e < r; e++) t += arguments[e].length;
  for (var n = Array(t), i = 0, e = 0; e < r; e++)
    for (var a = arguments[e], o = 0, s = a.length; o < s; o++, i++)
      n[i] = a[o];
  return n;
}
function hS(t, e, r) {
  if (r || arguments.length === 2) for (var n = 0, i = e.length, a; n < i; n++)
    (a || !(n in e)) && (a || (a = Array.prototype.slice.call(e, 0, n)), a[n] = e[n]);
  return t.concat(a || e);
}
function fa(t) {
  return this instanceof fa ? (this.v = t, this) : new fa(t);
}
function cS(t, e, r) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var n = r.apply(t, e || []), i, a = [];
  return i = {}, o("next"), o("throw"), o("return"), i[Symbol.asyncIterator] = function() {
    return this;
  }, i;
  function o(c) {
    n[c] && (i[c] = function(v) {
      return new Promise(function(d, y) {
        a.push([c, v, d, y]) > 1 || s(c, v);
      });
    });
  }
  function s(c, v) {
    try {
      u(n[c](v));
    } catch (d) {
      h(a[0][3], d);
    }
  }
  function u(c) {
    c.value instanceof fa ? Promise.resolve(c.value.v).then(l, f) : h(a[0][2], c);
  }
  function l(c) {
    s("next", c);
  }
  function f(c) {
    s("throw", c);
  }
  function h(c, v) {
    c(v), a.shift(), a.length && s(a[0][0], a[0][1]);
  }
}
function vS(t) {
  var e, r;
  return e = {}, n("next"), n("throw", function(i) {
    throw i;
  }), n("return"), e[Symbol.iterator] = function() {
    return this;
  }, e;
  function n(i, a) {
    e[i] = t[i] ? function(o) {
      return (r = !r) ? { value: fa(t[i](o)), done: i === "return" } : a ? a(o) : o;
    } : a;
  }
}
function dS(t) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var e = t[Symbol.asyncIterator], r;
  return e ? e.call(t) : (t = typeof Xl == "function" ? Xl(t) : t[Symbol.iterator](), r = {}, n("next"), n("throw"), n("return"), r[Symbol.asyncIterator] = function() {
    return this;
  }, r);
  function n(a) {
    r[a] = t[a] && function(o) {
      return new Promise(function(s, u) {
        o = t[a](o), i(s, u, o.done, o.value);
      });
    };
  }
  function i(a, o, s, u) {
    Promise.resolve(u).then(function(l) {
      a({ value: l, done: s });
    }, o);
  }
}
function pS(t, e) {
  return Object.defineProperty ? Object.defineProperty(t, "raw", { value: e }) : t.raw = e, t;
}
var gS = Object.create ? (function(t, e) {
  Object.defineProperty(t, "default", { enumerable: !0, value: e });
}) : function(t, e) {
  t.default = e;
};
function yS(t) {
  if (t && t.__esModule) return t;
  var e = {};
  if (t != null) for (var r in t) r !== "default" && Object.prototype.hasOwnProperty.call(t, r) && oh(e, t, r);
  return gS(e, t), e;
}
function mS(t) {
  return t && t.__esModule ? t : { default: t };
}
function _S(t, e, r, n) {
  if (r === "a" && !n) throw new TypeError("Private accessor was defined without a getter");
  if (typeof e == "function" ? t !== e || !n : !e.has(t)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
  return r === "m" ? n : r === "a" ? n.call(t) : n ? n.value : e.get(t);
}
function SS(t, e, r, n, i) {
  if (n === "m") throw new TypeError("Private method is not writable");
  if (n === "a" && !i) throw new TypeError("Private accessor was defined without a setter");
  if (typeof e == "function" ? t !== e || !i : !e.has(t)) throw new TypeError("Cannot write private member to an object whose class did not declare it");
  return n === "a" ? i.call(t, r) : i ? i.value = r : e.set(t, r), r;
}
const bS = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  get __assign() {
    return Wl;
  },
  __asyncDelegator: vS,
  __asyncGenerator: cS,
  __asyncValues: dS,
  __await: fa,
  __awaiter: oS,
  __classPrivateFieldGet: _S,
  __classPrivateFieldSet: SS,
  __createBinding: oh,
  __decorate: nS,
  __exportStar: uS,
  __extends: F,
  __generator: sS,
  __importDefault: mS,
  __importStar: yS,
  __makeTemplateObject: pS,
  __metadata: aS,
  __param: iS,
  __read: Cg,
  __rest: rS,
  __spread: lS,
  __spreadArray: hS,
  __spreadArrays: fS,
  __values: Xl
}, Symbol.toStringTag, { value: "Module" })), xg = /* @__PURE__ */ J0(bS);
var Jt = {}, er = {}, Su = {}, Nc;
function TS() {
  return Nc || (Nc = 1, (function(t) {
    Object.defineProperty(t, "__esModule", {
      value: !0
    }), t.default = void 0;
    var e = 1;
    t.default = function() {
      return "".concat(e++);
    };
  })(Su)), Su;
}
var Si = {}, bi = {}, bu = {}, Fc;
function Mg() {
  return Fc || (Fc = 1, (function(t) {
    Object.defineProperty(t, "__esModule", {
      value: !0
    }), t.default = void 0, t.default = function(r) {
      var n = arguments.length > 1 && arguments[1] !== void 0 ? arguments[1] : 60, i = null;
      return function() {
        for (var a = this, o = arguments.length, s = new Array(o), u = 0; u < o; u++)
          s[u] = arguments[u];
        clearTimeout(i), i = setTimeout(function() {
          r.apply(a, s);
        }, n);
      };
    };
  })(bu)), bu;
}
var tr = {}, zc;
function sh() {
  return zc || (zc = 1, Object.defineProperty(tr, "__esModule", {
    value: !0
  }), tr.SizeSensorId = tr.SensorTabIndex = tr.SensorClassName = void 0, tr.SizeSensorId = "size-sensor-id", tr.SensorClassName = "size-sensor-object", tr.SensorTabIndex = "-1"), tr;
}
var Hc;
function wS() {
  if (Hc) return bi;
  Hc = 1, Object.defineProperty(bi, "__esModule", {
    value: !0
  }), bi.createSensor = void 0;
  var t = r(Mg()), e = sh();
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return bi.createSensor = function(i, a) {
    var o = void 0, s = [], u = function() {
      getComputedStyle(i).position === "static" && (i.style.position = "relative");
      var d = document.createElement("object");
      return d.onload = function() {
        d.contentDocument.defaultView.addEventListener("resize", l), l();
      }, d.style.display = "block", d.style.position = "absolute", d.style.top = "0", d.style.left = "0", d.style.height = "100%", d.style.width = "100%", d.style.overflow = "hidden", d.style.pointerEvents = "none", d.style.zIndex = "-1", d.style.opacity = "0", d.setAttribute("class", e.SensorClassName), d.setAttribute("tabindex", e.SensorTabIndex), d.type = "text/html", i.appendChild(d), d.data = "about:blank", d;
    }, l = (0, t.default)(function() {
      s.forEach(function(v) {
        v(i);
      });
    }), f = function(d) {
      o || (o = u()), s.indexOf(d) === -1 && s.push(d);
    }, h = function() {
      o && o.parentNode && (o.contentDocument && o.contentDocument.defaultView.removeEventListener("resize", l), o.parentNode.removeChild(o), i.removeAttribute(e.SizeSensorId), o = void 0, s = [], a && a());
    }, c = function(d) {
      var y = s.indexOf(d);
      y !== -1 && s.splice(y, 1), s.length === 0 && o && h();
    };
    return {
      element: i,
      bind: f,
      destroy: h,
      unbind: c
    };
  }, bi;
}
var Ti = {}, Vc;
function CS() {
  if (Vc) return Ti;
  Vc = 1, Object.defineProperty(Ti, "__esModule", {
    value: !0
  }), Ti.createSensor = void 0;
  var t = sh(), e = r(Mg());
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return Ti.createSensor = function(i, a) {
    var o = void 0, s = [], u = (0, e.default)(function() {
      s.forEach(function(v) {
        v(i);
      });
    }), l = function() {
      var d = new ResizeObserver(u);
      return d.observe(i), u(), d;
    }, f = function(d) {
      o || (o = l()), s.indexOf(d) === -1 && s.push(d);
    }, h = function() {
      o && o.disconnect(), s = [], o = void 0, i.removeAttribute(t.SizeSensorId), a && a();
    }, c = function(d) {
      var y = s.indexOf(d);
      y !== -1 && s.splice(y, 1), s.length === 0 && o && h();
    };
    return {
      element: i,
      bind: f,
      destroy: h,
      unbind: c
    };
  }, Ti;
}
var Gc;
function xS() {
  if (Gc) return Si;
  Gc = 1, Object.defineProperty(Si, "__esModule", {
    value: !0
  }), Si.createSensor = void 0;
  var t = wS(), e = CS();
  return Si.createSensor = typeof ResizeObserver < "u" ? e.createSensor : t.createSensor, Si;
}
var Uc;
function MS() {
  if (Uc) return er;
  Uc = 1, Object.defineProperty(er, "__esModule", {
    value: !0
  }), er.removeSensor = er.getSensor = er.Sensors = void 0;
  var t = n(TS()), e = xS(), r = sh();
  function n(o) {
    return o && o.__esModule ? o : { default: o };
  }
  var i = er.Sensors = {};
  function a(o) {
    o && i[o] && delete i[o];
  }
  return er.getSensor = function(s) {
    var u = s.getAttribute(r.SizeSensorId);
    if (u && i[u])
      return i[u];
    var l = (0, t.default)();
    s.setAttribute(r.SizeSensorId, l);
    var f = (0, e.createSensor)(s, function() {
      return a(l);
    });
    return i[l] = f, f;
  }, er.removeSensor = function(s) {
    var u = s.element.getAttribute(r.SizeSensorId);
    s.destroy(), a(u);
  }, er;
}
var Yc;
function DS() {
  if (Yc) return Jt;
  Yc = 1, Object.defineProperty(Jt, "__esModule", {
    value: !0
  }), Jt.ver = Jt.clear = Jt.bind = void 0;
  var t = MS();
  return Jt.bind = function(r, n) {
    var i = (0, t.getSensor)(r);
    return i.bind(n), function() {
      i.unbind(n);
    };
  }, Jt.clear = function(r) {
    var n = (0, t.getSensor)(r);
    (0, t.removeSensor)(n);
  }, Jt.ver = "1.0.3", Jt;
}
var wi = {}, Wc;
function IS() {
  if (Wc) return wi;
  Wc = 1, Object.defineProperty(wi, "__esModule", { value: !0 }), wi.pick = void 0;
  function t(e, r) {
    var n = {};
    return r.forEach(function(i) {
      n[i] = e[i];
    }), n;
  }
  return wi.pick = t, wi;
}
var Ci = {}, Xc;
function AS() {
  if (Xc) return Ci;
  Xc = 1, Object.defineProperty(Ci, "__esModule", { value: !0 }), Ci.isFunction = void 0;
  function t(e) {
    return typeof e == "function";
  }
  return Ci.isFunction = t, Ci;
}
var xi = {}, qc;
function PS() {
  if (qc) return xi;
  qc = 1, Object.defineProperty(xi, "__esModule", { value: !0 }), xi.isString = void 0;
  function t(e) {
    return typeof e == "string";
  }
  return xi.isString = t, xi;
}
var Mi = {}, Tu, $c;
function LS() {
  return $c || ($c = 1, Tu = function t(e, r) {
    if (e === r) return !0;
    if (e && r && typeof e == "object" && typeof r == "object") {
      if (e.constructor !== r.constructor) return !1;
      var n, i, a;
      if (Array.isArray(e)) {
        if (n = e.length, n != r.length) return !1;
        for (i = n; i-- !== 0; )
          if (!t(e[i], r[i])) return !1;
        return !0;
      }
      if (e.constructor === RegExp) return e.source === r.source && e.flags === r.flags;
      if (e.valueOf !== Object.prototype.valueOf) return e.valueOf() === r.valueOf();
      if (e.toString !== Object.prototype.toString) return e.toString() === r.toString();
      if (a = Object.keys(e), n = a.length, n !== Object.keys(r).length) return !1;
      for (i = n; i-- !== 0; )
        if (!Object.prototype.hasOwnProperty.call(r, a[i])) return !1;
      for (i = n; i-- !== 0; ) {
        var o = a[i];
        if (!t(e[o], r[o])) return !1;
      }
      return !0;
    }
    return e !== e && r !== r;
  }), Tu;
}
var Zc;
function RS() {
  if (Zc) return Mi;
  Zc = 1, Object.defineProperty(Mi, "__esModule", { value: !0 }), Mi.isEqual = void 0;
  var t = xg, e = t.__importDefault(LS());
  return Mi.isEqual = e.default, Mi;
}
var Kc;
function ES() {
  if (Kc) return Ga;
  Kc = 1, Object.defineProperty(Ga, "__esModule", { value: !0 });
  var t = xg, e = t.__importStar(eS()), r = DS(), n = IS(), i = AS(), a = PS(), o = RS(), s = (
    /** @class */
    (function(u) {
      t.__extends(l, u);
      function l(f) {
        var h = u.call(this, f) || this;
        return h.echarts = f.echarts, h.ele = null, h.isInitialResize = !0, h.eventHandlerRefs = {}, h;
      }
      return l.prototype.componentDidMount = function() {
        this.renderNewEcharts();
      }, l.prototype.componentDidUpdate = function(f) {
        var h = this.props.shouldSetOption;
        if (!((0, i.isFunction)(h) && !h(f, this.props))) {
          if (!(0, o.isEqual)(f.theme, this.props.theme) || !(0, o.isEqual)(f.opts, this.props.opts)) {
            this.dispose(), this.renderNewEcharts();
            return;
          }
          var c = this.getEchartsInstance();
          (0, o.isEqual)(f.onEvents, this.props.onEvents) || (this.unbindEvents(c), this.bindEvents(c, this.props.onEvents));
          var v = ["option", "notMerge", "replaceMerge", "lazyUpdate", "showLoading", "loadingOption"];
          (0, o.isEqual)((0, n.pick)(this.props, v), (0, n.pick)(f, v)) || this.updateEChartsOption(), (!(0, o.isEqual)(f.style, this.props.style) || !(0, o.isEqual)(f.className, this.props.className)) && this.resize();
        }
      }, l.prototype.componentWillUnmount = function() {
        this.dispose();
      }, l.prototype.initEchartsInstance = function() {
        return t.__awaiter(this, void 0, void 0, function() {
          var f = this;
          return t.__generator(this, function(h) {
            return [2, new Promise(function(c) {
              f.echarts.init(f.ele, f.props.theme, f.props.opts);
              var v = f.getEchartsInstance();
              v.on("finished", function() {
                var d = f.ele.clientWidth, y = f.ele.clientHeight;
                f.echarts.dispose(f.ele);
                var p = t.__assign({ width: d, height: y }, f.props.opts);
                c(f.echarts.init(f.ele, f.props.theme, p));
              });
            })];
          });
        });
      }, l.prototype.getEchartsInstance = function() {
        return this.echarts.getInstanceByDom(this.ele);
      }, l.prototype.dispose = function() {
        if (this.ele) {
          try {
            (0, r.clear)(this.ele);
          } catch (f) {
            console.warn(f);
          }
          this.echarts.dispose(this.ele);
        }
      }, l.prototype.renderNewEcharts = function() {
        return t.__awaiter(this, void 0, void 0, function() {
          var f, h, c, v, d, y, p = this;
          return t.__generator(this, function(g) {
            switch (g.label) {
              case 0:
                return f = this.props, h = f.onEvents, c = f.onChartReady, v = f.autoResize, d = v === void 0 ? !0 : v, [4, this.initEchartsInstance()];
              case 1:
                return g.sent(), y = this.updateEChartsOption(), this.bindEvents(y, h || {}), (0, i.isFunction)(c) && c(y), this.ele && d && (0, r.bind)(this.ele, function() {
                  p.resize();
                }), [
                  2
                  /*return*/
                ];
            }
          });
        });
      }, l.prototype.bindEvents = function(f, h) {
        var c = this, v = function(y, p) {
          if ((0, a.isString)(y) && (0, i.isFunction)(p)) {
            var g = function(m) {
              p(m, f);
            };
            f.on(y, g), c.eventHandlerRefs[y] = g;
          }
        };
        for (var d in h)
          Object.prototype.hasOwnProperty.call(h, d) && v(d, h[d]);
      }, l.prototype.unbindEvents = function(f) {
        for (var h = 0, c = Object.entries(this.eventHandlerRefs); h < c.length; h++) {
          var v = c[h], d = v[0], y = v[1];
          f.off(d, y);
        }
        this.eventHandlerRefs = {};
      }, l.prototype.updateEChartsOption = function() {
        var f = this.props, h = f.option, c = f.notMerge, v = c === void 0 ? !1 : c, d = f.replaceMerge, y = d === void 0 ? null : d, p = f.lazyUpdate, g = p === void 0 ? !1 : p, m = f.showLoading, _ = f.loadingOption, S = _ === void 0 ? null : _, b = this.getEchartsInstance();
        return b.setOption(h, { notMerge: v, replaceMerge: y, lazyUpdate: g }), m ? b.showLoading(S) : b.hideLoading(), b;
      }, l.prototype.resize = function() {
        var f = this.getEchartsInstance();
        if (!this.isInitialResize)
          try {
            f.resize({
              width: "auto",
              height: "auto"
            });
          } catch (h) {
            console.warn(h);
          }
        this.isInitialResize = !1;
      }, l.prototype.render = function() {
        var f = this, h = this.props, c = h.style, v = h.className, d = v === void 0 ? "" : v;
        h.echarts, h.option, h.theme, h.notMerge, h.replaceMerge, h.lazyUpdate, h.showLoading, h.loadingOption, h.opts, h.onChartReady, h.onEvents, h.shouldSetOption, h.autoResize;
        var y = t.__rest(h, ["style", "className", "echarts", "option", "theme", "notMerge", "replaceMerge", "lazyUpdate", "showLoading", "loadingOption", "opts", "onChartReady", "onEvents", "shouldSetOption", "autoResize"]), p = t.__assign({ height: 300 }, c);
        return e.default.createElement("div", t.__assign({ ref: function(g) {
          f.ele = g;
        }, style: p, className: "echarts-for-react ".concat(d) }, y));
      }, l;
    })(e.PureComponent)
  );
  return Ga.default = s, Ga;
}
var OS = ES();
const xR = /* @__PURE__ */ tS(OS);
var kS = /* @__PURE__ */ (function() {
  function t() {
    this.firefox = !1, this.ie = !1, this.edge = !1, this.newEdge = !1, this.weChat = !1;
  }
  return t;
})(), BS = /* @__PURE__ */ (function() {
  function t() {
    this.browser = new kS(), this.node = !1, this.wxa = !1, this.worker = !1, this.svgSupported = !1, this.touchEventsSupported = !1, this.pointerEventsSupported = !1, this.domSupported = !1, this.transformSupported = !1, this.transform3dSupported = !1, this.hasGlobalWindow = typeof window < "u";
  }
  return t;
})(), W = new BS();
typeof wx == "object" && typeof wx.getSystemInfoSync == "function" ? (W.wxa = !0, W.touchEventsSupported = !0) : typeof document > "u" && typeof self < "u" ? W.worker = !0 : !W.hasGlobalWindow || "Deno" in window || typeof navigator < "u" && typeof navigator.userAgent == "string" && navigator.userAgent.indexOf("Node.js") > -1 ? (W.node = !0, W.svgSupported = !0) : NS(navigator.userAgent, W);
function NS(t, e) {
  var r = e.browser, n = t.match(/Firefox\/([\d.]+)/), i = t.match(/MSIE\s([\d.]+)/) || t.match(/Trident\/.+?rv:(([\d.]+))/), a = t.match(/Edge?\/([\d.]+)/), o = /micromessenger/i.test(t);
  n && (r.firefox = !0, r.version = n[1]), i && (r.ie = !0, r.version = i[1]), a && (r.edge = !0, r.version = a[1], r.newEdge = +a[1].split(".")[0] > 18), o && (r.weChat = !0), e.svgSupported = typeof SVGRect < "u", e.touchEventsSupported = "ontouchstart" in window && !r.ie && !r.edge, e.pointerEventsSupported = "onpointerdown" in window && (r.edge || r.ie && +r.version >= 11);
  var s = e.domSupported = typeof document < "u";
  if (s) {
    var u = document.documentElement.style;
    e.transform3dSupported = (r.ie && "transition" in u || r.edge || "WebKitCSSMatrix" in window && "m11" in new WebKitCSSMatrix() || "MozPerspective" in u) && !("OTransition" in u), e.transformSupported = e.transform3dSupported || r.ie && +r.version >= 9;
  }
}
var uh = 12, FS = "sans-serif", Er = uh + "px " + FS, zS = 20, HS = 100, VS = "007LLmW'55;N0500LLLLLLLLLL00NNNLzWW\\\\WQb\\0FWLg\\bWb\\WQ\\WrWWQ000CL5LLFLL0LL**F*gLLLL5F0LF\\FFF5.5N";
function GS(t) {
  var e = {};
  if (typeof JSON > "u")
    return e;
  for (var r = 0; r < t.length; r++) {
    var n = String.fromCharCode(r + 32), i = (t.charCodeAt(r) - zS) / HS;
    e[n] = i;
  }
  return e;
}
var US = GS(VS), Le = {
  createCanvas: function() {
    return typeof document < "u" && document.createElement("canvas");
  },
  measureText: /* @__PURE__ */ (function() {
    var t, e;
    return function(r, n) {
      if (!t) {
        var i = Le.createCanvas();
        t = i && i.getContext("2d");
      }
      if (t)
        return e !== n && (e = t.font = n || Er), t.measureText(r);
      r = r || "", n = n || Er;
      var a = /((?:\d+)?\.?\d*)px/.exec(n), o = a && +a[1] || uh, s = 0;
      if (n.indexOf("mono") >= 0)
        s = o * r.length;
      else
        for (var u = 0; u < r.length; u++) {
          var l = US[r[u]];
          s += l == null ? o : l * o;
        }
      return { width: s };
    };
  })(),
  loadImage: function(t, e, r) {
    var n = new Image();
    return n.onload = e, n.onerror = r, n.src = t, n;
  },
  getTime: function() {
    return Date.now ? Date.now() : +/* @__PURE__ */ new Date();
  }
};
function Dg(t) {
  for (var e in Le)
    Le.hasOwnProperty(e) && t[e] && (Le[e] = t[e]);
}
var Ig = Xt([
  "Function",
  "RegExp",
  "Date",
  "Error",
  "CanvasGradient",
  "CanvasPattern",
  "Image",
  "Canvas"
], function(t, e) {
  return t["[object " + e + "]"] = !0, t;
}, {}), Ag = Xt([
  "Int8",
  "Uint8",
  "Uint8Clamped",
  "Int16",
  "Uint16",
  "Int32",
  "Uint32",
  "Float32",
  "Float64"
], function(t, e) {
  return t["[object " + e + "Array]"] = !0, t;
}, {}), pi = Object.prototype.toString, Ns = Array.prototype, YS = Ns.forEach, WS = Ns.filter, lh = Ns.slice, XS = Ns.map, jc = (function() {
}).constructor, Ua = jc ? jc.prototype : null, fh = "__proto__", wu = 2311, qS = Math.pow(2, 53) - 1;
function hh() {
  return wu >= qS && (wu = 0), wu++;
}
function Fs() {
  for (var t = [], e = 0; e < arguments.length; e++)
    t[e] = arguments[e];
  typeof console < "u" && console.error.apply(console, t);
}
function Z(t) {
  if (t == null || typeof t != "object")
    return t;
  var e = t, r = pi.call(t);
  if (r === "[object Array]") {
    if (!ei(t)) {
      e = [];
      for (var n = 0, i = t.length; n < i; n++)
        e[n] = Z(t[n]);
    }
  } else if (Ag[r]) {
    if (!ei(t)) {
      var a = t.constructor;
      if (a.from)
        e = a.from(t);
      else {
        e = new a(t.length);
        for (var n = 0, i = t.length; n < i; n++)
          e[n] = t[n];
      }
    }
  } else if (!Ig[r] && !ei(t) && !si(t)) {
    e = {};
    for (var o in t)
      t.hasOwnProperty(o) && o !== fh && (e[o] = Z(t[o]));
  }
  return e;
}
function pe(t, e, r) {
  if (!z(e) || !z(t))
    return r ? Z(e) : t;
  for (var n in e)
    if (e.hasOwnProperty(n) && n !== fh) {
      var i = t[n], a = e[n];
      z(a) && z(i) && !B(a) && !B(i) && !si(a) && !si(i) && !ql(a) && !ql(i) && !ei(a) && !ei(i) ? pe(i, a, r) : (r || !(n in t)) && (t[n] = Z(e[n]));
    }
  return t;
}
function $S(t, e) {
  for (var r = t[0], n = 1, i = t.length; n < i; n++)
    r = pe(r, t[n], e);
  return r;
}
function E(t, e) {
  if (Object.assign)
    Object.assign(t, e);
  else
    for (var r in e)
      e.hasOwnProperty(r) && r !== fh && (t[r] = e[r]);
  return t;
}
function Pg(t, e, r) {
  t = t || {};
  for (var n = 0; n < r.length; n++) {
    var i = r[n];
    t[i] = e[i];
  }
  return t;
}
function _e(t, e, r) {
  for (var n = me(e), i = 0, a = n.length; i < a; i++) {
    var o = n[i];
    (r ? e[o] != null : t[o] == null) && (t[o] = e[o]);
  }
  return t;
}
var ZS = Le.createCanvas;
function oe(t, e) {
  if (t) {
    if (t.indexOf)
      return t.indexOf(e);
    for (var r = 0, n = t.length; r < n; r++)
      if (t[r] === e)
        return r;
  }
  return -1;
}
function ch(t, e) {
  var r = t.prototype;
  function n() {
  }
  n.prototype = e.prototype, t.prototype = new n();
  for (var i in r)
    r.hasOwnProperty(i) && (t.prototype[i] = r[i]);
  t.prototype.constructor = t, t.superClass = e;
}
function Lt(t, e, r) {
  if (t = "prototype" in t ? t.prototype : t, e = "prototype" in e ? e.prototype : e, Object.getOwnPropertyNames)
    for (var n = Object.getOwnPropertyNames(e), i = 0; i < n.length; i++) {
      var a = n[i];
      a !== "constructor" && (r ? e[a] != null : t[a] == null) && (t[a] = e[a]);
    }
  else
    _e(t, e, r);
}
function Ze(t) {
  return !t || typeof t == "string" ? !1 : typeof t.length == "number";
}
function w(t, e, r) {
  if (t && e)
    if (t.forEach && t.forEach === YS)
      t.forEach(e, r);
    else if (t.length === +t.length)
      for (var n = 0, i = t.length; n < i; n++)
        e.call(r, t[n], n, t);
    else
      for (var a in t)
        t.hasOwnProperty(a) && e.call(r, t[a], a, t);
}
function H(t, e, r) {
  if (!t)
    return [];
  if (!e)
    return zs(t);
  if (t.map && t.map === XS)
    return t.map(e, r);
  for (var n = [], i = 0, a = t.length; i < a; i++)
    n.push(e.call(r, t[i], i, t));
  return n;
}
function Xt(t, e, r, n) {
  if (t && e) {
    for (var i = 0, a = t.length; i < a; i++)
      r = e.call(n, r, t[i], i, t);
    return r;
  }
}
function xe(t, e, r) {
  if (!t)
    return [];
  if (!e)
    return zs(t);
  if (t.filter && t.filter === WS)
    return t.filter(e, r);
  for (var n = [], i = 0, a = t.length; i < a; i++)
    e.call(r, t[i], i, t) && n.push(t[i]);
  return n;
}
function KS(t, e, r) {
  if (t && e) {
    for (var n = 0, i = t.length; n < i; n++)
      if (e.call(r, t[n], n, t))
        return t[n];
  }
}
function me(t) {
  if (!t)
    return [];
  if (Object.keys)
    return Object.keys(t);
  var e = [];
  for (var r in t)
    t.hasOwnProperty(r) && e.push(r);
  return e;
}
function jS(t, e) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return function() {
    return t.apply(e, r.concat(lh.call(arguments)));
  };
}
var le = Ua && X(Ua.bind) ? Ua.call.bind(Ua.bind) : jS;
function we(t) {
  for (var e = [], r = 1; r < arguments.length; r++)
    e[r - 1] = arguments[r];
  return function() {
    return t.apply(this, e.concat(lh.call(arguments)));
  };
}
function B(t) {
  return Array.isArray ? Array.isArray(t) : pi.call(t) === "[object Array]";
}
function X(t) {
  return typeof t == "function";
}
function N(t) {
  return typeof t == "string";
}
function Qo(t) {
  return pi.call(t) === "[object String]";
}
function ve(t) {
  return typeof t == "number";
}
function z(t) {
  var e = typeof t;
  return e === "function" || !!t && e === "object";
}
function ql(t) {
  return !!Ig[pi.call(t)];
}
function Ke(t) {
  return !!Ag[pi.call(t)];
}
function si(t) {
  return typeof t == "object" && typeof t.nodeType == "number" && typeof t.ownerDocument == "object";
}
function Aa(t) {
  return t.colorStops != null;
}
function Lg(t) {
  return t.image != null;
}
function QS(t) {
  return pi.call(t) === "[object RegExp]";
}
function ha(t) {
  return t !== t;
}
function ca() {
  for (var t = [], e = 0; e < arguments.length; e++)
    t[e] = arguments[e];
  for (var r = 0, n = t.length; r < n; r++)
    if (t[r] != null)
      return t[r];
}
function G(t, e) {
  return t ?? e;
}
function gn(t, e, r) {
  return t ?? e ?? r;
}
function zs(t) {
  for (var e = [], r = 1; r < arguments.length; r++)
    e[r - 1] = arguments[r];
  return lh.apply(t, e);
}
function Hs(t) {
  if (typeof t == "number")
    return [t, t, t, t];
  var e = t.length;
  return e === 2 ? [t[0], t[1], t[0], t[1]] : e === 3 ? [t[0], t[1], t[2], t[1]] : t;
}
function qt(t, e) {
  if (!t)
    throw new Error(e);
}
function It(t) {
  return t == null ? null : typeof t.trim == "function" ? t.trim() : t.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, "");
}
var Rg = "__ec_primitive__";
function va(t) {
  t[Rg] = !0;
}
function ei(t) {
  return t[Rg];
}
var JS = (function() {
  function t() {
    this.data = {};
  }
  return t.prototype.delete = function(e) {
    var r = this.has(e);
    return r && delete this.data[e], r;
  }, t.prototype.has = function(e) {
    return this.data.hasOwnProperty(e);
  }, t.prototype.get = function(e) {
    return this.data[e];
  }, t.prototype.set = function(e, r) {
    return this.data[e] = r, this;
  }, t.prototype.keys = function() {
    return me(this.data);
  }, t.prototype.forEach = function(e) {
    var r = this.data;
    for (var n in r)
      r.hasOwnProperty(n) && e(r[n], n);
  }, t;
})(), Eg = typeof Map == "function";
function e1() {
  return Eg ? /* @__PURE__ */ new Map() : new JS();
}
var Og = (function() {
  function t(e) {
    var r = B(e);
    this.data = e1();
    var n = this;
    e instanceof t ? e.each(i) : e && w(e, i);
    function i(a, o) {
      r ? n.set(a, o) : n.set(o, a);
    }
  }
  return t.prototype.hasKey = function(e) {
    return this.data.has(e);
  }, t.prototype.get = function(e) {
    return this.data.get(e);
  }, t.prototype.set = function(e, r) {
    return this.data.set(e, r), r;
  }, t.prototype.each = function(e, r) {
    this.data.forEach(function(n, i) {
      e.call(r, n, i);
    });
  }, t.prototype.keys = function() {
    var e = this.data.keys();
    return Eg ? Array.from(e) : e;
  }, t.prototype.removeKey = function(e) {
    this.data.delete(e);
  }, t;
})();
function U(t) {
  return new Og(t);
}
function kg(t, e) {
  for (var r = new t.constructor(t.length + e.length), n = 0; n < t.length; n++)
    r[n] = t[n];
  for (var i = t.length, n = 0; n < e.length; n++)
    r[n + i] = e[n];
  return r;
}
function Pa(t, e) {
  var r;
  if (Object.create)
    r = Object.create(t);
  else {
    var n = function() {
    };
    n.prototype = t, r = new n();
  }
  return e && E(r, e), r;
}
function vh(t) {
  var e = t.style;
  e.webkitUserSelect = "none", e.userSelect = "none", e.webkitTapHighlightColor = "rgba(0,0,0,0)", e["-webkit-touch-callout"] = "none";
}
function qe(t, e) {
  return t.hasOwnProperty(e);
}
function Re() {
}
var Bg = 180 / Math.PI, t1 = Number.EPSILON || Math.pow(2, -52);
const r1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  EPSILON: t1,
  HashMap: Og,
  RADIAN_TO_DEGREE: Bg,
  assert: qt,
  assignProps: Pg,
  bind: le,
  clone: Z,
  concatArray: kg,
  createCanvas: ZS,
  createHashMap: U,
  createObject: Pa,
  curry: we,
  defaults: _e,
  disableUserSelect: vh,
  each: w,
  eqNaN: ha,
  extend: E,
  filter: xe,
  find: KS,
  guid: hh,
  hasOwn: qe,
  indexOf: oe,
  inherits: ch,
  isArray: B,
  isArrayLike: Ze,
  isBuiltInObject: ql,
  isDom: si,
  isFunction: X,
  isGradientObject: Aa,
  isImagePatternObject: Lg,
  isNumber: ve,
  isObject: z,
  isPrimitive: ei,
  isRegExp: QS,
  isString: N,
  isStringSafe: Qo,
  isTypedArray: Ke,
  keys: me,
  logError: Fs,
  map: H,
  merge: pe,
  mergeAll: $S,
  mixin: Lt,
  noop: Re,
  normalizeCssArray: Hs,
  reduce: Xt,
  retrieve: ca,
  retrieve2: G,
  retrieve3: gn,
  setAsPrimitive: va,
  slice: zs,
  trim: It
}, Symbol.toStringTag, { value: "Module" }));
function xn(t, e) {
  return t == null && (t = 0), e == null && (e = 0), [t, e];
}
function n1(t, e) {
  return t[0] = e[0], t[1] = e[1], t;
}
function Ng(t) {
  return [t[0], t[1]];
}
function Ro(t, e, r) {
  return t[0] = e, t[1] = r, t;
}
function $l(t, e, r) {
  return t[0] = e[0] + r[0], t[1] = e[1] + r[1], t;
}
function i1(t, e, r, n) {
  return t[0] = e[0] + r[0] * n, t[1] = e[1] + r[1] * n, t;
}
function Fg(t, e, r) {
  return t[0] = e[0] - r[0], t[1] = e[1] - r[1], t;
}
function dh(t) {
  return Math.sqrt(ph(t));
}
var a1 = dh;
function ph(t) {
  return t[0] * t[0] + t[1] * t[1];
}
var o1 = ph;
function s1(t, e, r) {
  return t[0] = e[0] * r[0], t[1] = e[1] * r[1], t;
}
function u1(t, e, r) {
  return t[0] = e[0] / r[0], t[1] = e[1] / r[1], t;
}
function l1(t, e) {
  return t[0] * e[0] + t[1] * e[1];
}
function Eo(t, e, r) {
  return t[0] = e[0] * r, t[1] = e[1] * r, t;
}
function zg(t, e) {
  var r = dh(e);
  return r === 0 ? (t[0] = 0, t[1] = 0) : (t[0] = e[0] / r, t[1] = e[1] / r), t;
}
function Jo(t, e) {
  return Math.sqrt((t[0] - e[0]) * (t[0] - e[0]) + (t[1] - e[1]) * (t[1] - e[1]));
}
var Hg = Jo;
function Vg(t, e) {
  return (t[0] - e[0]) * (t[0] - e[0]) + (t[1] - e[1]) * (t[1] - e[1]);
}
var yn = Vg;
function f1(t, e) {
  return t[0] = -e[0], t[1] = -e[1], t;
}
function h1(t, e, r, n) {
  return t[0] = e[0] + n * (r[0] - e[0]), t[1] = e[1] + n * (r[1] - e[1]), t;
}
function ot(t, e, r) {
  var n = e[0], i = e[1];
  return t[0] = r[0] * n + r[2] * i + r[4], t[1] = r[1] * n + r[3] * i + r[5], t;
}
function Cr(t, e, r) {
  return t[0] = Math.min(e[0], r[0]), t[1] = Math.min(e[1], r[1]), t;
}
function xr(t, e, r) {
  return t[0] = Math.max(e[0], r[0]), t[1] = Math.max(e[1], r[1]), t;
}
const c1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  add: $l,
  applyTransform: ot,
  clone: Ng,
  copy: n1,
  create: xn,
  dist: Hg,
  distSquare: yn,
  distance: Jo,
  distanceSquare: Vg,
  div: u1,
  dot: l1,
  len: dh,
  lenSquare: ph,
  length: a1,
  lengthSquare: o1,
  lerp: h1,
  max: xr,
  min: Cr,
  mul: s1,
  negate: f1,
  normalize: zg,
  scale: Eo,
  scaleAndAdd: i1,
  set: Ro,
  sub: Fg
}, Symbol.toStringTag, { value: "Module" }));
var Rn = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.target = e, this.topTarget = r && r.topTarget;
  }
  return t;
})(), v1 = (function() {
  function t(e) {
    this.handler = e, e.on("mousedown", this._dragStart, this), e.on("mousemove", this._drag, this), e.on("mouseup", this._dragEnd, this);
  }
  return t.prototype._dragStart = function(e) {
    for (var r = e.target; r && !r.draggable; )
      r = r.parent || r.__hostTarget;
    r && (this._draggingTarget = r, r.dragging = !0, this._x = e.offsetX, this._y = e.offsetY, this.handler.dispatchToElement(new Rn(r, e), "dragstart", e.event));
  }, t.prototype._drag = function(e) {
    var r = this._draggingTarget;
    if (r) {
      var n = e.offsetX, i = e.offsetY, a = n - this._x, o = i - this._y;
      this._x = n, this._y = i, r.drift(a, o, e), this.handler.dispatchToElement(new Rn(r, e), "drag", e.event);
      var s = this.handler.findHover(n, i, r).target, u = this._dropTarget;
      this._dropTarget = s, r !== s && (u && s !== u && this.handler.dispatchToElement(new Rn(u, e), "dragleave", e.event), s && s !== u && this.handler.dispatchToElement(new Rn(s, e), "dragenter", e.event));
    }
  }, t.prototype._dragEnd = function(e) {
    var r = this._draggingTarget;
    r && (r.dragging = !1), this.handler.dispatchToElement(new Rn(r, e), "dragend", e.event), this._dropTarget && this.handler.dispatchToElement(new Rn(this._dropTarget, e), "drop", e.event), this._draggingTarget = null, this._dropTarget = null;
  }, t;
})(), Kt = (function() {
  function t(e) {
    e && (this._$eventProcessor = e);
  }
  return t.prototype.on = function(e, r, n, i) {
    this._$handlers || (this._$handlers = {});
    var a = this._$handlers;
    if (typeof r == "function" && (i = n, n = r, r = null), !n || !e)
      return this;
    var o = this._$eventProcessor;
    r != null && o && o.normalizeQuery && (r = o.normalizeQuery(r)), a[e] || (a[e] = []);
    for (var s = 0; s < a[e].length; s++)
      if (a[e][s].h === n)
        return this;
    var u = {
      h: n,
      query: r,
      ctx: i || this,
      callAtLast: n.zrEventfulCallAtLast
    }, l = a[e].length - 1, f = a[e][l];
    return f && f.callAtLast ? a[e].splice(l, 0, u) : a[e].push(u), this;
  }, t.prototype.isSilent = function(e) {
    var r = this._$handlers;
    return !r || !r[e] || !r[e].length;
  }, t.prototype.off = function(e, r) {
    var n = this._$handlers;
    if (!n)
      return this;
    if (!e)
      return this._$handlers = {}, this;
    if (r) {
      if (n[e]) {
        for (var i = [], a = 0, o = n[e].length; a < o; a++)
          n[e][a].h !== r && i.push(n[e][a]);
        n[e] = i;
      }
      n[e] && n[e].length === 0 && delete n[e];
    } else
      delete n[e];
    return this;
  }, t.prototype.trigger = function(e) {
    for (var r = [], n = 1; n < arguments.length; n++)
      r[n - 1] = arguments[n];
    if (!this._$handlers)
      return this;
    var i = this._$handlers[e], a = this._$eventProcessor;
    if (i)
      for (var o = r.length, s = i.length, u = 0; u < s; u++) {
        var l = i[u];
        if (!(a && a.filter && l.query != null && !a.filter(e, l.query)))
          switch (o) {
            case 0:
              l.h.call(l.ctx);
              break;
            case 1:
              l.h.call(l.ctx, r[0]);
              break;
            case 2:
              l.h.call(l.ctx, r[0], r[1]);
              break;
            default:
              l.h.apply(l.ctx, r);
              break;
          }
      }
    return a && a.afterTrigger && a.afterTrigger(e), this;
  }, t.prototype.triggerWithContext = function(e) {
    for (var r = [], n = 1; n < arguments.length; n++)
      r[n - 1] = arguments[n];
    if (!this._$handlers)
      return this;
    var i = this._$handlers[e], a = this._$eventProcessor;
    if (i)
      for (var o = r.length, s = r[o - 1], u = i.length, l = 0; l < u; l++) {
        var f = i[l];
        if (!(a && a.filter && f.query != null && !a.filter(e, f.query)))
          switch (o) {
            case 0:
              f.h.call(s);
              break;
            case 1:
              f.h.call(s, r[0]);
              break;
            case 2:
              f.h.call(s, r[0], r[1]);
              break;
            default:
              f.h.apply(s, r.slice(1, o - 1));
              break;
          }
      }
    return a && a.afterTrigger && a.afterTrigger(e), this;
  }, t;
})(), d1 = Math.log(2);
function Zl(t, e, r, n, i, a) {
  var o = n + "-" + i, s = t.length;
  if (a.hasOwnProperty(o))
    return a[o];
  if (e === 1) {
    var u = Math.round(Math.log((1 << s) - 1 & ~i) / d1);
    return t[r][u];
  }
  for (var l = n | 1 << r, f = r + 1; n & 1 << f; )
    f++;
  for (var h = 0, c = 0, v = 0; c < s; c++) {
    var d = 1 << c;
    d & i || (h += (v % 2 ? -1 : 1) * t[r][c] * Zl(t, e - 1, f, l, i | d, a), v++);
  }
  return a[o] = h, h;
}
function Qc(t, e) {
  var r = [
    [t[0], t[1], 1, 0, 0, 0, -e[0] * t[0], -e[0] * t[1]],
    [0, 0, 0, t[0], t[1], 1, -e[1] * t[0], -e[1] * t[1]],
    [t[2], t[3], 1, 0, 0, 0, -e[2] * t[2], -e[2] * t[3]],
    [0, 0, 0, t[2], t[3], 1, -e[3] * t[2], -e[3] * t[3]],
    [t[4], t[5], 1, 0, 0, 0, -e[4] * t[4], -e[4] * t[5]],
    [0, 0, 0, t[4], t[5], 1, -e[5] * t[4], -e[5] * t[5]],
    [t[6], t[7], 1, 0, 0, 0, -e[6] * t[6], -e[6] * t[7]],
    [0, 0, 0, t[6], t[7], 1, -e[7] * t[6], -e[7] * t[7]]
  ], n = {}, i = Zl(r, 8, 0, 0, 0, n);
  if (i !== 0) {
    for (var a = [], o = 0; o < 8; o++)
      for (var s = 0; s < 8; s++)
        a[s] == null && (a[s] = 0), a[s] += ((o + s) % 2 ? -1 : 1) * Zl(r, 7, o === 0 ? 1 : 0, 1 << o, 1 << s, n) / i * e[o];
    return function(u, l, f) {
      var h = l * a[6] + f * a[7] + 1;
      u[0] = (l * a[0] + f * a[1] + a[2]) / h, u[1] = (l * a[3] + f * a[4] + a[5]) / h;
    };
  }
}
var es = "___zrEVENTSAVED", Cu = [];
function p1(t, e, r, n, i) {
  return Kl(Cu, e, n, i, !0) && Kl(t, r, Cu[0], Cu[1]);
}
function g1(t, e) {
  t && r(t), e && r(e);
  function r(n) {
    var i = n[es];
    i && (i.clearMarkers && i.clearMarkers(), delete n[es]);
  }
}
function Kl(t, e, r, n, i) {
  if (e.getBoundingClientRect && W.domSupported && !Gg(e)) {
    var a = e[es] || (e[es] = {}), o = y1(e, a), s = m1(o, a, i);
    if (s)
      return s(t, r, n), !0;
  }
  return !1;
}
function y1(t, e) {
  var r = e.markers;
  if (r)
    return r;
  r = e.markers = [];
  for (var n = ["left", "right"], i = ["top", "bottom"], a = 0; a < 4; a++) {
    var o = document.createElement("div"), s = o.style, u = a % 2, l = (a >> 1) % 2;
    s.cssText = [
      "position: absolute",
      "visibility: hidden",
      "padding: 0",
      "margin: 0",
      "border-width: 0",
      "user-select: none",
      "width:0",
      "height:0",
      n[u] + ":0",
      i[l] + ":0",
      n[1 - u] + ":auto",
      i[1 - l] + ":auto",
      ""
    ].join("!important;"), t.appendChild(o), r.push(o);
  }
  return e.clearMarkers = function() {
    w(r, function(f) {
      f.parentNode && f.parentNode.removeChild(f);
    });
  }, r;
}
function m1(t, e, r) {
  for (var n = r ? "invTrans" : "trans", i = e[n], a = e.srcCoords, o = [], s = [], u = !0, l = 0; l < 4; l++) {
    var f = t[l].getBoundingClientRect(), h = 2 * l, c = f.left, v = f.top;
    o.push(c, v), u = u && a && c === a[h] && v === a[h + 1], s.push(t[l].offsetLeft, t[l].offsetTop);
  }
  return u && i ? i : (e.srcCoords = o, e[n] = r ? Qc(s, o) : Qc(o, s));
}
function Gg(t) {
  return t.nodeName.toUpperCase() === "CANVAS";
}
var _1 = /([&<>"'])/g, S1 = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;"
};
function Ue(t) {
  return t == null ? "" : (t + "").replace(_1, function(e, r) {
    return S1[r];
  });
}
var b1 = /^(?:mouse|pointer|contextmenu|drag|drop)|click/, xu = [], T1 = W.browser.firefox && +W.browser.version.split(".")[0] < 39;
function jl(t, e, r, n) {
  return r = r || {}, n ? Jc(t, e, r) : T1 && e.layerX != null && e.layerX !== e.offsetX ? (r.zrX = e.layerX, r.zrY = e.layerY) : e.offsetX != null ? (r.zrX = e.offsetX, r.zrY = e.offsetY) : Jc(t, e, r), r;
}
function Jc(t, e, r) {
  if (W.domSupported && t.getBoundingClientRect) {
    var n = e.clientX, i = e.clientY;
    if (Gg(t)) {
      var a = t.getBoundingClientRect();
      r.zrX = n - a.left, r.zrY = i - a.top;
      return;
    } else if (Kl(xu, t, n, i)) {
      r.zrX = xu[0], r.zrY = xu[1];
      return;
    }
  }
  r.zrX = r.zrY = 0;
}
function gh(t) {
  return t || window.event;
}
function dt(t, e, r) {
  if (e = gh(e), e.zrX != null)
    return e;
  var n = e.type, i = n && n.indexOf("touch") >= 0;
  if (i) {
    var o = n !== "touchend" ? e.targetTouches[0] : e.changedTouches[0];
    o && jl(t, o, e, r);
  } else {
    jl(t, e, e, r);
    var a = w1(e);
    e.zrDelta = a ? a / 120 : -(e.detail || 0) / 3;
  }
  var s = e.button;
  return e.which == null && s !== void 0 && b1.test(e.type) && (e.which = s & 1 ? 1 : s & 2 ? 3 : s & 4 ? 2 : 0), e;
}
function w1(t) {
  var e = t.wheelDelta;
  if (e)
    return e;
  var r = t.deltaX, n = t.deltaY;
  if (r == null || n == null)
    return e;
  var i = Math.abs(n !== 0 ? n : r), a = n > 0 ? -1 : n < 0 ? 1 : r > 0 ? -1 : 1;
  return 3 * i * a;
}
function C1(t, e, r, n) {
  t.addEventListener(e, r, n);
}
function x1(t, e, r, n) {
  t.removeEventListener(e, r, n);
}
var Ug = function(t) {
  t.preventDefault(), t.stopPropagation(), t.cancelBubble = !0;
}, M1 = (function() {
  function t() {
    this._track = [];
  }
  return t.prototype.recognize = function(e, r, n) {
    return this._doTrack(e, r, n), this._recognize(e);
  }, t.prototype.clear = function() {
    return this._track.length = 0, this;
  }, t.prototype._doTrack = function(e, r, n) {
    var i = e.touches;
    if (i) {
      for (var a = {
        points: [],
        touches: [],
        target: r,
        event: e
      }, o = 0, s = i.length; o < s; o++) {
        var u = i[o], l = jl(n, u, {});
        a.points.push([l.zrX, l.zrY]), a.touches.push(u);
      }
      this._track.push(a);
    }
  }, t.prototype._recognize = function(e) {
    for (var r in Mu)
      if (Mu.hasOwnProperty(r)) {
        var n = Mu[r](this._track, e);
        if (n)
          return n;
      }
  }, t;
})();
function ev(t) {
  var e = t[1][0] - t[0][0], r = t[1][1] - t[0][1];
  return Math.sqrt(e * e + r * r);
}
function D1(t) {
  return [
    (t[0][0] + t[1][0]) / 2,
    (t[0][1] + t[1][1]) / 2
  ];
}
var Mu = {
  pinch: function(t, e) {
    var r = t.length;
    if (r) {
      var n = (t[r - 1] || {}).points, i = (t[r - 2] || {}).points || n;
      if (i && i.length > 1 && n && n.length > 1) {
        var a = ev(n) / ev(i);
        !isFinite(a) && (a = 1), e.pinchScale = a;
        var o = D1(n);
        return e.pinchX = o[0], e.pinchY = o[1], {
          type: "pinch",
          target: t[0].target,
          event: e
        };
      }
    }
  }
};
function Tt() {
  return [1, 0, 0, 1, 0, 0];
}
function Mn(t) {
  return t[0] = 1, t[1] = 0, t[2] = 0, t[3] = 1, t[4] = 0, t[5] = 0, t;
}
function La(t, e) {
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t[4] = e[4], t[5] = e[5], t;
}
function mn(t, e, r) {
  var n = e[0] * r[0] + e[2] * r[1], i = e[1] * r[0] + e[3] * r[1], a = e[0] * r[2] + e[2] * r[3], o = e[1] * r[2] + e[3] * r[3], s = e[0] * r[4] + e[2] * r[5] + e[4], u = e[1] * r[4] + e[3] * r[5] + e[5];
  return t[0] = n, t[1] = i, t[2] = a, t[3] = o, t[4] = s, t[5] = u, t;
}
function ts(t, e, r) {
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t[4] = e[4] + r[0], t[5] = e[5] + r[1], t;
}
function Vs(t, e, r, n) {
  n === void 0 && (n = [0, 0]);
  var i = e[0], a = e[2], o = e[4], s = e[1], u = e[3], l = e[5], f = Math.sin(r), h = Math.cos(r);
  return t[0] = i * h + s * f, t[1] = -i * f + s * h, t[2] = a * h + u * f, t[3] = -a * f + h * u, t[4] = h * (o - n[0]) + f * (l - n[1]) + n[0], t[5] = h * (l - n[1]) - f * (o - n[0]) + n[1], t;
}
function Yg(t, e, r) {
  var n = r[0], i = r[1];
  return t[0] = e[0] * n, t[1] = e[1] * i, t[2] = e[2] * n, t[3] = e[3] * i, t[4] = e[4] * n, t[5] = e[5] * i, t;
}
function gi(t, e) {
  var r = e[0], n = e[2], i = e[4], a = e[1], o = e[3], s = e[5], u = r * o - a * n;
  return u ? (u = 1 / u, t[0] = o * u, t[1] = -a * u, t[2] = -n * u, t[3] = r * u, t[4] = (n * s - o * i) * u, t[5] = (a * i - r * s) * u, t) : null;
}
function I1(t) {
  var e = Tt();
  return La(e, t), e;
}
const A1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  clone: I1,
  copy: La,
  create: Tt,
  identity: Mn,
  invert: gi,
  mul: mn,
  rotate: Vs,
  scale: Yg,
  translate: ts
}, Symbol.toStringTag, { value: "Module" }));
var ue = (function() {
  function t(e, r) {
    this.x = e || 0, this.y = r || 0;
  }
  return t.prototype.copy = function(e) {
    return this.x = e.x, this.y = e.y, this;
  }, t.prototype.clone = function() {
    return new t(this.x, this.y);
  }, t.prototype.set = function(e, r) {
    return this.x = e, this.y = r, this;
  }, t.prototype.equal = function(e) {
    return e.x === this.x && e.y === this.y;
  }, t.prototype.add = function(e) {
    return this.x += e.x, this.y += e.y, this;
  }, t.prototype.scale = function(e) {
    this.x *= e, this.y *= e;
  }, t.prototype.scaleAndAdd = function(e, r) {
    this.x += e.x * r, this.y += e.y * r;
  }, t.prototype.sub = function(e) {
    return this.x -= e.x, this.y -= e.y, this;
  }, t.prototype.dot = function(e) {
    return this.x * e.x + this.y * e.y;
  }, t.prototype.len = function() {
    return Math.sqrt(this.x * this.x + this.y * this.y);
  }, t.prototype.lenSquare = function() {
    return this.x * this.x + this.y * this.y;
  }, t.prototype.normalize = function() {
    var e = this.len();
    return this.x /= e, this.y /= e, this;
  }, t.prototype.distance = function(e) {
    var r = this.x - e.x, n = this.y - e.y;
    return Math.sqrt(r * r + n * n);
  }, t.prototype.distanceSquare = function(e) {
    var r = this.x - e.x, n = this.y - e.y;
    return r * r + n * n;
  }, t.prototype.negate = function() {
    return this.x = -this.x, this.y = -this.y, this;
  }, t.prototype.transform = function(e) {
    if (e) {
      var r = this.x, n = this.y;
      return this.x = e[0] * r + e[2] * n + e[4], this.y = e[1] * r + e[3] * n + e[5], this;
    }
  }, t.prototype.toArray = function(e) {
    return e[0] = this.x, e[1] = this.y, e;
  }, t.prototype.fromArray = function(e) {
    this.x = e[0], this.y = e[1];
  }, t.set = function(e, r, n) {
    e.x = r, e.y = n;
  }, t.copy = function(e, r) {
    e.x = r.x, e.y = r.y;
  }, t.len = function(e) {
    return Math.sqrt(e.x * e.x + e.y * e.y);
  }, t.lenSquare = function(e) {
    return e.x * e.x + e.y * e.y;
  }, t.dot = function(e, r) {
    return e.x * r.x + e.y * r.y;
  }, t.add = function(e, r, n) {
    e.x = r.x + n.x, e.y = r.y + n.y;
  }, t.sub = function(e, r, n) {
    e.x = r.x - n.x, e.y = r.y - n.y;
  }, t.scale = function(e, r, n) {
    e.x = r.x * n, e.y = r.y * n;
  }, t.scaleAndAdd = function(e, r, n, i) {
    e.x = r.x + n.x * i, e.y = r.y + n.y * i;
  }, t.lerp = function(e, r, n, i) {
    var a = 1 - i;
    e.x = a * r.x + i * n.x, e.y = a * r.y + i * n.y;
  }, t;
})(), hn = Math.min, Kn = Math.max, Ql = Math.abs, tv = ["x", "y"], P1 = ["width", "height"], Hr = new ue(), Vr = new ue(), Gr = new ue(), Ur = new ue(), at = Wg(), Xi = at.minTv, Jl = at.maxTv, Ji = [0, 0], Y = (function() {
  function t(e, r, n, i) {
    Du(this, e, r, n, i);
  }
  return t.set = function(e, r, n, i, a) {
    return i < 0 && (r = r + i, i = -i), a < 0 && (n = n + a, a = -a), e.x = r, e.y = n, e.width = i, e.height = a, e;
  }, t.prototype.union = function(e) {
    var r = hn(e.x, this.x), n = hn(e.y, this.y);
    isFinite(this.x) && isFinite(this.width) ? this.width = Kn(e.x + e.width, this.x + this.width) - r : this.width = e.width, isFinite(this.y) && isFinite(this.height) ? this.height = Kn(e.y + e.height, this.y + this.height) - n : this.height = e.height, this.x = r, this.y = n;
  }, t.prototype.applyTransform = function(e) {
    t.applyTransform(this, this, e);
  }, t.prototype.calculateTransform = function(e) {
    return L1(Tt(), this, e);
  }, t.prototype.intersect = function(e, r, n) {
    return t.intersect(this, e, r, n);
  }, t.intersect = function(e, r, n, i) {
    n && ue.set(n, 0, 0);
    var a = i && i.outIntersectRect || null, o = i && i.clamp;
    if (a && (a.x = a.y = a.width = a.height = NaN), !e || !r)
      return !1;
    e instanceof t || (e = Du(R1, e.x, e.y, e.width, e.height)), r instanceof t || (r = Du(E1, r.x, r.y, r.width, r.height));
    var s = !!n;
    at.reset(i, s);
    var u = at.touchThreshold, l = e.x + u, f = e.x + e.width - u, h = e.y + u, c = e.y + e.height - u, v = r.x + u, d = r.x + r.width - u, y = r.y + u, p = r.y + r.height - u;
    if (l > f || h > c || v > d || y > p)
      return !1;
    var g = !(f < v || d < l || c < y || p < h);
    return (s || a) && (Ji[0] = 1 / 0, Ji[1] = 0, nv(l, f, v, d, 0, s, a, o), nv(h, c, y, p, 1, s, a, o), s && ue.copy(n, g ? at.useDir ? at.dirMinTv : Xi : Jl)), g;
  }, t.contain = function(e, r, n) {
    return r >= e.x && r <= e.x + e.width && n >= e.y && n <= e.y + e.height;
  }, t.prototype.contain = function(e, r) {
    return t.contain(this, e, r);
  }, t.prototype.clone = function() {
    return new t(this.x, this.y, this.width, this.height);
  }, t.prototype.copy = function(e) {
    rv(this, e);
  }, t.prototype.plain = function() {
    return {
      x: this.x,
      y: this.y,
      width: this.width,
      height: this.height
    };
  }, t.prototype.isFinite = function() {
    return isFinite(this.x) && isFinite(this.y) && isFinite(this.width) && isFinite(this.height);
  }, t.prototype.isZero = function() {
    return this.width === 0 || this.height === 0;
  }, t.create = function(e) {
    return new t(e ? e.x : 0, e ? e.y : 0, e ? e.width : 0, e ? e.height : 0);
  }, t.copy = function(e, r) {
    return e.x = r.x, e.y = r.y, e.width = r.width, e.height = r.height, e;
  }, t.applyTransform = function(e, r, n) {
    if (!n) {
      e !== r && rv(e, r);
      return;
    }
    if (n[1] < 1e-5 && n[1] > -1e-5 && n[2] < 1e-5 && n[2] > -1e-5) {
      var i = n[0], a = n[3], o = n[4], s = n[5];
      e.x = r.x * i + o, e.y = r.y * a + s, e.width = r.width * i, e.height = r.height * a, e.width < 0 && (e.x += e.width, e.width = -e.width), e.height < 0 && (e.y += e.height, e.height = -e.height);
      return;
    }
    Hr.x = Gr.x = r.x, Hr.y = Ur.y = r.y, Vr.x = Ur.x = r.x + r.width, Vr.y = Gr.y = r.y + r.height, Hr.transform(n), Ur.transform(n), Vr.transform(n), Gr.transform(n), e.x = hn(Hr.x, Vr.x, Gr.x, Ur.x), e.y = hn(Hr.y, Vr.y, Gr.y, Ur.y);
    var u = Kn(Hr.x, Vr.x, Gr.x, Ur.x), l = Kn(Hr.y, Vr.y, Gr.y, Ur.y);
    e.width = u - e.x, e.height = l - e.y;
  }, t.calculateTransform = function(e, r, n) {
    var i = n.width / r.width, a = n.height / r.height;
    return e = Mn(e || []), ts(e, e, Ro(Iu, -r.x, -r.y)), Yg(e, e, Ro(Iu, i, a)), ts(e, e, Ro(Iu, n.x, n.y)), e;
  }, t;
})();
Y.create;
var Du = Y.set, rv = Y.copy, L1 = Y.calculateTransform;
Y.applyTransform;
Y.contain;
var R1 = new Y(0, 0, 0, 0), E1 = new Y(0, 0, 0, 0), Iu = [];
function nv(t, e, r, n, i, a, o, s) {
  var u = Ql(e - r), l = Ql(n - t), f = hn(u, l), h = tv[i], c = tv[1 - i], v = P1[i];
  e < r || n < t ? u < l ? (a && (Jl[h] = -u), s && (o[h] = e, o[v] = 0)) : (a && (Jl[h] = l), s && (o[h] = t, o[v] = 0)) : (o && (o[h] = Kn(t, r), o[v] = hn(e, n) - o[h]), a && (f < Ji[0] || at.useDir) && (Ji[0] = hn(f, Ji[0]), (u < l || !at.bidirectional) && (Xi[h] = u, Xi[c] = 0, at.useDir && at.calcDirMTV()), (u >= l || !at.bidirectional) && (Xi[h] = -l, Xi[c] = 0, at.useDir && at.calcDirMTV())));
}
function Wg() {
  var t = 0, e = new ue(), r = new ue(), n = {
    minTv: new ue(),
    maxTv: new ue(),
    useDir: !1,
    dirMinTv: new ue(),
    touchThreshold: 0,
    bidirectional: !0,
    negativeSize: !1,
    reset: function(a, o) {
      n.touchThreshold = 0, a && a.touchThreshold != null && (n.touchThreshold = Kn(0, a.touchThreshold)), n.negativeSize = !1, o && (n.minTv.set(1 / 0, 1 / 0), n.maxTv.set(0, 0), n.useDir = !1, a && a.direction != null && (n.useDir = !0, n.dirMinTv.copy(n.minTv), r.copy(n.minTv), t = a.direction, n.bidirectional = a.bidirectional == null || !!a.bidirectional, n.bidirectional || e.set(Math.cos(t), Math.sin(t))));
    },
    calcDirMTV: function() {
      var a = n.minTv, o = n.dirMinTv, s = a.y * a.y + a.x * a.x, u = Math.sin(t), l = Math.cos(t), f = u * a.y + l * a.x;
      if (i(f)) {
        i(a.x) && i(a.y) && o.set(0, 0);
        return;
      }
      if (r.x = s * l / f, r.y = s * u / f, i(r.x) && i(r.y)) {
        o.set(0, 0);
        return;
      }
      (n.bidirectional || e.dot(r) > 0) && r.len() < o.len() && o.copy(r);
    }
  };
  function i(a) {
    return Ql(a) < 1e-10;
  }
  return n;
}
var Xg = "silent";
function O1(t, e, r) {
  return {
    type: t,
    event: r,
    target: e.target,
    topTarget: e.topTarget,
    cancelBubble: !1,
    offsetX: r.zrX,
    offsetY: r.zrY,
    gestureEvent: r.gestureEvent,
    pinchX: r.pinchX,
    pinchY: r.pinchY,
    pinchScale: r.pinchScale,
    wheelDelta: r.zrDelta,
    zrByTouch: r.zrByTouch,
    which: r.which,
    stop: k1
  };
}
function k1() {
  Ug(this.event);
}
var B1 = (function(t) {
  F(e, t);
  function e() {
    var r = t !== null && t.apply(this, arguments) || this;
    return r.handler = null, r;
  }
  return e.prototype.dispose = function() {
  }, e.prototype.setCursor = function() {
  }, e;
})(Kt), Di = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.x = e, this.y = r;
  }
  return t;
})(), N1 = [
  "click",
  "dblclick",
  "mousewheel",
  "mouseout",
  "mouseup",
  "mousedown",
  "mousemove",
  "contextmenu"
], Au = new Y(0, 0, 0, 0), qg = (function(t) {
  F(e, t);
  function e(r, n, i, a, o) {
    var s = t.call(this) || this;
    return s._hovered = new Di(0, 0), s.storage = r, s.painter = n, s.painterRoot = a, s._pointerSize = o, i = i || new B1(), s.proxy = null, s.setHandlerProxy(i), s._draggingMgr = new v1(s), s;
  }
  return e.prototype.setHandlerProxy = function(r) {
    this.proxy && this.proxy.dispose(), r && (w(N1, function(n) {
      r.on && r.on(n, this[n], this);
    }, this), r.handler = this), this.proxy = r;
  }, e.prototype.mousemove = function(r) {
    var n = r.zrX, i = r.zrY, a = $g(this, n, i), o = this._hovered, s = o.target;
    s && !s.__zr && (o = this.findHover(o.x, o.y), s = o.target);
    var u = this._hovered = a ? new Di(n, i) : this.findHover(n, i), l = u.target, f = this.proxy;
    f.setCursor && f.setCursor(l ? l.cursor : "default"), s && l !== s && this.dispatchToElement(o, "mouseout", r), this.dispatchToElement(u, "mousemove", r), l && l !== s && this.dispatchToElement(u, "mouseover", r);
  }, e.prototype.mouseout = function(r) {
    var n = r.zrEventControl;
    n !== "only_globalout" && this.dispatchToElement(this._hovered, "mouseout", r), n !== "no_globalout" && this.trigger("globalout", { type: "globalout", event: r });
  }, e.prototype.resize = function() {
    this._hovered = new Di(0, 0);
  }, e.prototype.dispatch = function(r, n) {
    var i = this[r];
    i && i.call(this, n);
  }, e.prototype.dispose = function() {
    this.proxy.dispose(), this.storage = null, this.proxy = null, this.painter = null;
  }, e.prototype.setCursorStyle = function(r) {
    var n = this.proxy;
    n.setCursor && n.setCursor(r);
  }, e.prototype.dispatchToElement = function(r, n, i) {
    r = r || {};
    var a = r.target;
    if (!(a && a.silent)) {
      for (var o = "on" + n, s = O1(n, r, i); a && (a[o] && (s.cancelBubble = !!a[o].call(a, s)), a.trigger(n, s), a = a.__hostTarget ? a.__hostTarget : a.parent, !s.cancelBubble); )
        ;
      s.cancelBubble || (this.trigger(n, s), this.painter && this.painter.eachOtherLayer && this.painter.eachOtherLayer(function(u) {
        typeof u[o] == "function" && u[o].call(u, s), u.trigger && u.trigger(n, s);
      }));
    }
  }, e.prototype.findHover = function(r, n, i) {
    var a = this.storage.getDisplayList(), o = new Di(r, n);
    if (iv(a, o, r, n, i), this._pointerSize && !o.target) {
      for (var s = [], u = this._pointerSize, l = u / 2, f = new Y(r - l, n - l, u, u), h = a.length - 1; h >= 0; h--) {
        var c = a[h];
        c !== i && !c.ignore && !c.ignoreCoarsePointer && (!c.parent || !c.parent.ignoreCoarsePointer) && (Au.copy(c.getBoundingRect()), c.transform && Au.applyTransform(c.transform), Au.intersect(f) && s.push(c));
      }
      if (s.length)
        for (var v = 4, d = Math.PI / 12, y = Math.PI * 2, p = 0; p < l; p += v)
          for (var g = 0; g < y; g += d) {
            var m = r + p * Math.cos(g), _ = n + p * Math.sin(g);
            if (iv(s, o, m, _, i), o.target)
              return o;
          }
    }
    return o;
  }, e.prototype.processGesture = function(r, n) {
    this._gestureMgr || (this._gestureMgr = new M1());
    var i = this._gestureMgr;
    n === "start" && i.clear();
    var a = i.recognize(r, this.findHover(r.zrX, r.zrY, null).target, this.proxy.dom);
    if (n === "end" && i.clear(), a) {
      var o = a.type;
      r.gestureEvent = o;
      var s = new Di();
      s.target = a.target, this.dispatchToElement(s, o, a.event);
    }
  }, e;
})(Kt);
w(["click", "mousedown", "mouseup", "mousewheel", "dblclick", "contextmenu"], function(t) {
  qg.prototype[t] = function(e) {
    var r = e.zrX, n = e.zrY, i = $g(this, r, n), a, o;
    if ((t !== "mouseup" || !i) && (a = this.findHover(r, n), o = a.target), t === "mousedown")
      this._downEl = o, this._downPoint = [e.zrX, e.zrY], this._upEl = o;
    else if (t === "mouseup")
      this._upEl = o;
    else if (t === "click") {
      if (this._downEl !== this._upEl || !this._downPoint || Hg(this._downPoint, [e.zrX, e.zrY]) > 4)
        return;
      this._downPoint = null;
    }
    this.dispatchToElement(a, t, e);
  };
});
function F1(t, e, r) {
  if (t[t.rectHover ? "rectContain" : "contain"](e, r)) {
    for (var n = t, i = void 0, a = !1; n; ) {
      if (n.ignoreClip && (a = !0), !a) {
        var o = n.getClipPath();
        if (o && !o.contain(e, r))
          return !1;
      }
      n.silent && (i = !0);
      var s = n.__hostTarget;
      n = s ? n.ignoreHostSilent ? null : s : n.parent;
    }
    return i ? Xg : !0;
  }
  return !1;
}
function iv(t, e, r, n, i) {
  for (var a = t.length - 1; a >= 0; a--) {
    var o = t[a], s = void 0;
    if (o !== i && !o.ignore && (s = F1(o, r, n)) && (!e.topTarget && (e.topTarget = o), s !== Xg)) {
      e.target = o;
      break;
    }
  }
}
function $g(t, e, r) {
  var n = t.painter;
  return e < 0 || e > n.getWidth() || r < 0 || r > n.getHeight();
}
var Zg = 32, Ii = 7;
function z1(t) {
  for (var e = 0; t >= Zg; )
    e |= t & 1, t >>= 1;
  return t + e;
}
function av(t, e, r, n) {
  var i = e + 1;
  if (i === r)
    return 1;
  if (n(t[i++], t[e]) < 0) {
    for (; i < r && n(t[i], t[i - 1]) < 0; )
      i++;
    H1(t, e, i);
  } else
    for (; i < r && n(t[i], t[i - 1]) >= 0; )
      i++;
  return i - e;
}
function H1(t, e, r) {
  for (r--; e < r; ) {
    var n = t[e];
    t[e++] = t[r], t[r--] = n;
  }
}
function ov(t, e, r, n, i) {
  for (n === e && n++; n < r; n++) {
    for (var a = t[n], o = e, s = n, u; o < s; )
      u = o + s >>> 1, i(a, t[u]) < 0 ? s = u : o = u + 1;
    var l = n - o;
    switch (l) {
      case 3:
        t[o + 3] = t[o + 2];
      case 2:
        t[o + 2] = t[o + 1];
      case 1:
        t[o + 1] = t[o];
        break;
      default:
        for (; l > 0; )
          t[o + l] = t[o + l - 1], l--;
    }
    t[o] = a;
  }
}
function Pu(t, e, r, n, i, a) {
  var o = 0, s = 0, u = 1;
  if (a(t, e[r + i]) > 0) {
    for (s = n - i; u < s && a(t, e[r + i + u]) > 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s), o += i, u += i;
  } else {
    for (s = i + 1; u < s && a(t, e[r + i - u]) <= 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s);
    var l = o;
    o = i - u, u = i - l;
  }
  for (o++; o < u; ) {
    var f = o + (u - o >>> 1);
    a(t, e[r + f]) > 0 ? o = f + 1 : u = f;
  }
  return u;
}
function Lu(t, e, r, n, i, a) {
  var o = 0, s = 0, u = 1;
  if (a(t, e[r + i]) < 0) {
    for (s = i + 1; u < s && a(t, e[r + i - u]) < 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s);
    var l = o;
    o = i - u, u = i - l;
  } else {
    for (s = n - i; u < s && a(t, e[r + i + u]) >= 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s), o += i, u += i;
  }
  for (o++; o < u; ) {
    var f = o + (u - o >>> 1);
    a(t, e[r + f]) < 0 ? u = f : o = f + 1;
  }
  return u;
}
function V1(t, e) {
  var r = Ii, n, i, a = 0, o = [];
  n = [], i = [];
  function s(v, d) {
    n[a] = v, i[a] = d, a += 1;
  }
  function u() {
    for (; a > 1; ) {
      var v = a - 2;
      if (v >= 1 && i[v - 1] <= i[v] + i[v + 1] || v >= 2 && i[v - 2] <= i[v] + i[v - 1])
        i[v - 1] < i[v + 1] && v--;
      else if (i[v] > i[v + 1])
        break;
      f(v);
    }
  }
  function l() {
    for (; a > 1; ) {
      var v = a - 2;
      v > 0 && i[v - 1] < i[v + 1] && v--, f(v);
    }
  }
  function f(v) {
    var d = n[v], y = i[v], p = n[v + 1], g = i[v + 1];
    i[v] = y + g, v === a - 3 && (n[v + 1] = n[v + 2], i[v + 1] = i[v + 2]), a--;
    var m = Lu(t[p], t, d, y, 0, e);
    d += m, y -= m, y !== 0 && (g = Pu(t[d + y - 1], t, p, g, g - 1, e), g !== 0 && (y <= g ? h(d, y, p, g) : c(d, y, p, g)));
  }
  function h(v, d, y, p) {
    var g = 0;
    for (g = 0; g < d; g++)
      o[g] = t[v + g];
    var m = 0, _ = y, S = v;
    if (t[S++] = t[_++], --p === 0) {
      for (g = 0; g < d; g++)
        t[S + g] = o[m + g];
      return;
    }
    if (d === 1) {
      for (g = 0; g < p; g++)
        t[S + g] = t[_ + g];
      t[S + p] = o[m];
      return;
    }
    for (var b = r, T, C, M; ; ) {
      T = 0, C = 0, M = !1;
      do
        if (e(t[_], o[m]) < 0) {
          if (t[S++] = t[_++], C++, T = 0, --p === 0) {
            M = !0;
            break;
          }
        } else if (t[S++] = o[m++], T++, C = 0, --d === 1) {
          M = !0;
          break;
        }
      while ((T | C) < b);
      if (M)
        break;
      do {
        if (T = Lu(t[_], o, m, d, 0, e), T !== 0) {
          for (g = 0; g < T; g++)
            t[S + g] = o[m + g];
          if (S += T, m += T, d -= T, d <= 1) {
            M = !0;
            break;
          }
        }
        if (t[S++] = t[_++], --p === 0) {
          M = !0;
          break;
        }
        if (C = Pu(o[m], t, _, p, 0, e), C !== 0) {
          for (g = 0; g < C; g++)
            t[S + g] = t[_ + g];
          if (S += C, _ += C, p -= C, p === 0) {
            M = !0;
            break;
          }
        }
        if (t[S++] = o[m++], --d === 1) {
          M = !0;
          break;
        }
        b--;
      } while (T >= Ii || C >= Ii);
      if (M)
        break;
      b < 0 && (b = 0), b += 2;
    }
    if (r = b, r < 1 && (r = 1), d === 1) {
      for (g = 0; g < p; g++)
        t[S + g] = t[_ + g];
      t[S + p] = o[m];
    } else {
      if (d === 0)
        throw new Error();
      for (g = 0; g < d; g++)
        t[S + g] = o[m + g];
    }
  }
  function c(v, d, y, p) {
    var g = 0;
    for (g = 0; g < p; g++)
      o[g] = t[y + g];
    var m = v + d - 1, _ = p - 1, S = y + p - 1, b = 0, T = 0;
    if (t[S--] = t[m--], --d === 0) {
      for (b = S - (p - 1), g = 0; g < p; g++)
        t[b + g] = o[g];
      return;
    }
    if (p === 1) {
      for (S -= d, m -= d, T = S + 1, b = m + 1, g = d - 1; g >= 0; g--)
        t[T + g] = t[b + g];
      t[S] = o[_];
      return;
    }
    for (var C = r; ; ) {
      var M = 0, x = 0, D = !1;
      do
        if (e(o[_], t[m]) < 0) {
          if (t[S--] = t[m--], M++, x = 0, --d === 0) {
            D = !0;
            break;
          }
        } else if (t[S--] = o[_--], x++, M = 0, --p === 1) {
          D = !0;
          break;
        }
      while ((M | x) < C);
      if (D)
        break;
      do {
        if (M = d - Lu(o[_], t, v, d, d - 1, e), M !== 0) {
          for (S -= M, m -= M, d -= M, T = S + 1, b = m + 1, g = M - 1; g >= 0; g--)
            t[T + g] = t[b + g];
          if (d === 0) {
            D = !0;
            break;
          }
        }
        if (t[S--] = o[_--], --p === 1) {
          D = !0;
          break;
        }
        if (x = p - Pu(t[m], o, 0, p, p - 1, e), x !== 0) {
          for (S -= x, _ -= x, p -= x, T = S + 1, b = _ + 1, g = 0; g < x; g++)
            t[T + g] = o[b + g];
          if (p <= 1) {
            D = !0;
            break;
          }
        }
        if (t[S--] = t[m--], --d === 0) {
          D = !0;
          break;
        }
        C--;
      } while (M >= Ii || x >= Ii);
      if (D)
        break;
      C < 0 && (C = 0), C += 2;
    }
    if (r = C, r < 1 && (r = 1), p === 1) {
      for (S -= d, m -= d, T = S + 1, b = m + 1, g = d - 1; g >= 0; g--)
        t[T + g] = t[b + g];
      t[S] = o[_];
    } else {
      if (p === 0)
        throw new Error();
      for (b = S - (p - 1), g = 0; g < p; g++)
        t[b + g] = o[g];
    }
  }
  return {
    mergeRuns: u,
    forceMergeRuns: l,
    pushRun: s
  };
}
function Oo(t, e, r, n) {
  r || (r = 0), n || (n = t.length);
  var i = n - r;
  if (!(i < 2)) {
    var a = 0;
    if (i < Zg) {
      a = av(t, r, n, e), ov(t, r, n, r + a, e);
      return;
    }
    var o = V1(t, e), s = z1(i);
    do {
      if (a = av(t, r, n, e), a < s) {
        var u = i;
        u > s && (u = s), ov(t, r, r + u, r + a, e), a = u;
      }
      o.pushRun(r, a), o.mergeRuns(), i -= a, r += a;
    } while (i !== 0);
    o.forceMergeRuns();
  }
}
var Je = 1, qi = 2, $n = 4, sv = !1;
function Ru() {
  sv || (sv = !0, console.warn("z / z2 / zlevel of displayable is invalid, which may cause unexpected errors"));
}
function uv(t, e) {
  return t.zlevel === e.zlevel ? t.z === e.z ? t.z2 - e.z2 : t.z - e.z : t.zlevel - e.zlevel;
}
var G1 = (function() {
  function t() {
    this._roots = [], this._displayList = [], this._displayListLen = 0, this.displayableSortFunc = uv;
  }
  return t.prototype.traverse = function(e, r) {
    for (var n = 0; n < this._roots.length; n++)
      this._roots[n].traverse(e, r);
  }, t.prototype.getDisplayList = function(e, r) {
    r = r || !1;
    var n = this._displayList;
    return (e || !n.length) && this.updateDisplayList(r), n;
  }, t.prototype.updateDisplayList = function(e) {
    this._displayListLen = 0;
    for (var r = this._roots, n = this._displayList, i = 0, a = r.length; i < a; i++)
      this._updateAndAddDisplayable(r[i], null, e);
    n.length = this._displayListLen, Oo(n, uv);
  }, t.prototype._updateAndAddDisplayable = function(e, r, n) {
    if (!(e.ignore && !n)) {
      e.beforeUpdate(), e.update(), e.afterUpdate();
      var i = e.getClipPath(), a = r && r.length, o = 0, s = e.__clipPaths;
      if (!e.ignoreClip && (a || i)) {
        if (s || (s = e.__clipPaths = []), a)
          for (var u = 0; u < r.length; u++)
            s[o++] = r[u];
        for (var l = i, f = e; l; )
          l.parent = f, l.updateTransform(), s[o++] = l, f = l, l = l.getClipPath();
      }
      if (s && (s.length = o), e.childrenRef) {
        for (var h = e.childrenRef(), c = 0; c < h.length; c++) {
          var v = h[c];
          e.__dirty && (v.__dirty |= Je), this._updateAndAddDisplayable(v, s, n);
        }
        e.__dirty = 0;
      } else {
        var d = e;
        isNaN(d.z) && (Ru(), d.z = 0), isNaN(d.z2) && (Ru(), d.z2 = 0), isNaN(d.zlevel) && (Ru(), d.zlevel = 0), this._displayList[this._displayListLen++] = d;
      }
      var y = e.getDecalElement && e.getDecalElement();
      y && this._updateAndAddDisplayable(y, s, n);
      var p = e.getTextGuideLine();
      p && this._updateAndAddDisplayable(p, s, n);
      var g = e.getTextContent();
      g && this._updateAndAddDisplayable(g, s, n);
    }
  }, t.prototype.addRoot = function(e) {
    e.__zr && e.__zr.storage === this || this._roots.push(e);
  }, t.prototype.delRoot = function(e) {
    if (e instanceof Array) {
      for (var r = 0, n = e.length; r < n; r++)
        this.delRoot(e[r]);
      return;
    }
    var i = oe(this._roots, e);
    i >= 0 && this._roots.splice(i, 1);
  }, t.prototype.delAllRoots = function() {
    this._roots = [], this._displayList = [], this._displayListLen = 0;
  }, t.prototype.getRoots = function() {
    return this._roots;
  }, t.prototype.dispose = function() {
    this._displayList = null, this._roots = null;
  }, t;
})(), rs;
rs = W.hasGlobalWindow && (window.requestAnimationFrame && window.requestAnimationFrame.bind(window) || window.msRequestAnimationFrame && window.msRequestAnimationFrame.bind(window) || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame) || function(t) {
  return setTimeout(t, 16);
};
var ea = {
  linear: function(t) {
    return t;
  },
  quadraticIn: function(t) {
    return t * t;
  },
  quadraticOut: function(t) {
    return t * (2 - t);
  },
  quadraticInOut: function(t) {
    return (t *= 2) < 1 ? 0.5 * t * t : -0.5 * (--t * (t - 2) - 1);
  },
  cubicIn: function(t) {
    return t * t * t;
  },
  cubicOut: function(t) {
    return --t * t * t + 1;
  },
  cubicInOut: function(t) {
    return (t *= 2) < 1 ? 0.5 * t * t * t : 0.5 * ((t -= 2) * t * t + 2);
  },
  quarticIn: function(t) {
    return t * t * t * t;
  },
  quarticOut: function(t) {
    return 1 - --t * t * t * t;
  },
  quarticInOut: function(t) {
    return (t *= 2) < 1 ? 0.5 * t * t * t * t : -0.5 * ((t -= 2) * t * t * t - 2);
  },
  quinticIn: function(t) {
    return t * t * t * t * t;
  },
  quinticOut: function(t) {
    return --t * t * t * t * t + 1;
  },
  quinticInOut: function(t) {
    return (t *= 2) < 1 ? 0.5 * t * t * t * t * t : 0.5 * ((t -= 2) * t * t * t * t + 2);
  },
  sinusoidalIn: function(t) {
    return 1 - Math.cos(t * Math.PI / 2);
  },
  sinusoidalOut: function(t) {
    return Math.sin(t * Math.PI / 2);
  },
  sinusoidalInOut: function(t) {
    return 0.5 * (1 - Math.cos(Math.PI * t));
  },
  exponentialIn: function(t) {
    return t === 0 ? 0 : Math.pow(1024, t - 1);
  },
  exponentialOut: function(t) {
    return t === 1 ? 1 : 1 - Math.pow(2, -10 * t);
  },
  exponentialInOut: function(t) {
    return t === 0 ? 0 : t === 1 ? 1 : (t *= 2) < 1 ? 0.5 * Math.pow(1024, t - 1) : 0.5 * (-Math.pow(2, -10 * (t - 1)) + 2);
  },
  circularIn: function(t) {
    return 1 - Math.sqrt(1 - t * t);
  },
  circularOut: function(t) {
    return Math.sqrt(1 - --t * t);
  },
  circularInOut: function(t) {
    return (t *= 2) < 1 ? -0.5 * (Math.sqrt(1 - t * t) - 1) : 0.5 * (Math.sqrt(1 - (t -= 2) * t) + 1);
  },
  elasticIn: function(t) {
    var e, r = 0.1, n = 0.4;
    return t === 0 ? 0 : t === 1 ? 1 : (!r || r < 1 ? (r = 1, e = n / 4) : e = n * Math.asin(1 / r) / (2 * Math.PI), -(r * Math.pow(2, 10 * (t -= 1)) * Math.sin((t - e) * (2 * Math.PI) / n)));
  },
  elasticOut: function(t) {
    var e, r = 0.1, n = 0.4;
    return t === 0 ? 0 : t === 1 ? 1 : (!r || r < 1 ? (r = 1, e = n / 4) : e = n * Math.asin(1 / r) / (2 * Math.PI), r * Math.pow(2, -10 * t) * Math.sin((t - e) * (2 * Math.PI) / n) + 1);
  },
  elasticInOut: function(t) {
    var e, r = 0.1, n = 0.4;
    return t === 0 ? 0 : t === 1 ? 1 : (!r || r < 1 ? (r = 1, e = n / 4) : e = n * Math.asin(1 / r) / (2 * Math.PI), (t *= 2) < 1 ? -0.5 * (r * Math.pow(2, 10 * (t -= 1)) * Math.sin((t - e) * (2 * Math.PI) / n)) : r * Math.pow(2, -10 * (t -= 1)) * Math.sin((t - e) * (2 * Math.PI) / n) * 0.5 + 1);
  },
  backIn: function(t) {
    var e = 1.70158;
    return t * t * ((e + 1) * t - e);
  },
  backOut: function(t) {
    var e = 1.70158;
    return --t * t * ((e + 1) * t + e) + 1;
  },
  backInOut: function(t) {
    var e = 2.5949095;
    return (t *= 2) < 1 ? 0.5 * (t * t * ((e + 1) * t - e)) : 0.5 * ((t -= 2) * t * ((e + 1) * t + e) + 2);
  },
  bounceIn: function(t) {
    return 1 - ea.bounceOut(1 - t);
  },
  bounceOut: function(t) {
    return t < 1 / 2.75 ? 7.5625 * t * t : t < 2 / 2.75 ? 7.5625 * (t -= 1.5 / 2.75) * t + 0.75 : t < 2.5 / 2.75 ? 7.5625 * (t -= 2.25 / 2.75) * t + 0.9375 : 7.5625 * (t -= 2.625 / 2.75) * t + 0.984375;
  },
  bounceInOut: function(t) {
    return t < 0.5 ? ea.bounceIn(t * 2) * 0.5 : ea.bounceOut(t * 2 - 1) * 0.5 + 0.5;
  }
}, Ya = Math.pow, Ar = Math.sqrt, ns = 1e-8, Kg = 1e-4, lv = Ar(3), Wa = 1 / 3, Ht = xn(), yt = xn(), ti = xn();
function Mr(t) {
  return t > -ns && t < ns;
}
function jg(t) {
  return t > ns || t < -ns;
}
function Pe(t, e, r, n, i) {
  var a = 1 - i;
  return a * a * (a * t + 3 * i * e) + i * i * (i * n + 3 * a * r);
}
function fv(t, e, r, n, i) {
  var a = 1 - i;
  return 3 * (((e - t) * a + 2 * (r - e) * i) * a + (n - r) * i * i);
}
function Qg(t, e, r, n, i, a) {
  var o = n + 3 * (e - r) - t, s = 3 * (r - e * 2 + t), u = 3 * (e - t), l = t - i, f = s * s - 3 * o * u, h = s * u - 9 * o * l, c = u * u - 3 * s * l, v = 0;
  if (Mr(f) && Mr(h))
    if (Mr(s))
      a[0] = 0;
    else {
      var d = -u / s;
      d >= 0 && d <= 1 && (a[v++] = d);
    }
  else {
    var y = h * h - 4 * f * c;
    if (Mr(y)) {
      var p = h / f, d = -s / o + p, g = -p / 2;
      d >= 0 && d <= 1 && (a[v++] = d), g >= 0 && g <= 1 && (a[v++] = g);
    } else if (y > 0) {
      var m = Ar(y), _ = f * s + 1.5 * o * (-h + m), S = f * s + 1.5 * o * (-h - m);
      _ < 0 ? _ = -Ya(-_, Wa) : _ = Ya(_, Wa), S < 0 ? S = -Ya(-S, Wa) : S = Ya(S, Wa);
      var d = (-s - (_ + S)) / (3 * o);
      d >= 0 && d <= 1 && (a[v++] = d);
    } else {
      var b = (2 * f * s - 3 * o * h) / (2 * Ar(f * f * f)), T = Math.acos(b) / 3, C = Ar(f), M = Math.cos(T), d = (-s - 2 * C * M) / (3 * o), g = (-s + C * (M + lv * Math.sin(T))) / (3 * o), x = (-s + C * (M - lv * Math.sin(T))) / (3 * o);
      d >= 0 && d <= 1 && (a[v++] = d), g >= 0 && g <= 1 && (a[v++] = g), x >= 0 && x <= 1 && (a[v++] = x);
    }
  }
  return v;
}
function Jg(t, e, r, n, i) {
  var a = 6 * r - 12 * e + 6 * t, o = 9 * e + 3 * n - 3 * t - 9 * r, s = 3 * e - 3 * t, u = 0;
  if (Mr(o)) {
    if (jg(a)) {
      var l = -s / a;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = a * a - 4 * o * s;
    if (Mr(f))
      i[0] = -a / (2 * o);
    else if (f > 0) {
      var h = Ar(f), l = (-a + h) / (2 * o), c = (-a - h) / (2 * o);
      l >= 0 && l <= 1 && (i[u++] = l), c >= 0 && c <= 1 && (i[u++] = c);
    }
  }
  return u;
}
function is(t, e, r, n, i, a) {
  var o = (e - t) * i + t, s = (r - e) * i + e, u = (n - r) * i + r, l = (s - o) * i + o, f = (u - s) * i + s, h = (f - l) * i + l;
  a[0] = t, a[1] = o, a[2] = l, a[3] = h, a[4] = h, a[5] = f, a[6] = u, a[7] = n;
}
function U1(t, e, r, n, i, a, o, s, u, l, f) {
  var h, c = 5e-3, v = 1 / 0, d, y, p, g;
  Ht[0] = u, Ht[1] = l;
  for (var m = 0; m < 1; m += 0.05)
    yt[0] = Pe(t, r, i, o, m), yt[1] = Pe(e, n, a, s, m), p = yn(Ht, yt), p < v && (h = m, v = p);
  v = 1 / 0;
  for (var _ = 0; _ < 32 && !(c < Kg); _++)
    d = h - c, y = h + c, yt[0] = Pe(t, r, i, o, d), yt[1] = Pe(e, n, a, s, d), p = yn(yt, Ht), d >= 0 && p < v ? (h = d, v = p) : (ti[0] = Pe(t, r, i, o, y), ti[1] = Pe(e, n, a, s, y), g = yn(ti, Ht), y <= 1 && g < v ? (h = y, v = g) : c *= 0.5);
  return f && (f[0] = Pe(t, r, i, o, h), f[1] = Pe(e, n, a, s, h)), Ar(v);
}
function Y1(t, e, r, n, i, a, o, s, u) {
  for (var l = t, f = e, h = 0, c = 1 / u, v = 1; v <= u; v++) {
    var d = v * c, y = Pe(t, r, i, o, d), p = Pe(e, n, a, s, d), g = y - l, m = p - f;
    h += Math.sqrt(g * g + m * m), l = y, f = p;
  }
  return h;
}
function Be(t, e, r, n) {
  var i = 1 - n;
  return i * (i * t + 2 * n * e) + n * n * r;
}
function hv(t, e, r, n) {
  return 2 * ((1 - n) * (e - t) + n * (r - e));
}
function W1(t, e, r, n, i) {
  var a = t - 2 * e + r, o = 2 * (e - t), s = t - n, u = 0;
  if (Mr(a)) {
    if (jg(o)) {
      var l = -s / o;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = o * o - 4 * a * s;
    if (Mr(f)) {
      var l = -o / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l);
    } else if (f > 0) {
      var h = Ar(f), l = (-o + h) / (2 * a), c = (-o - h) / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l), c >= 0 && c <= 1 && (i[u++] = c);
    }
  }
  return u;
}
function ey(t, e, r) {
  var n = t + r - 2 * e;
  return n === 0 ? 0.5 : (t - e) / n;
}
function as(t, e, r, n, i) {
  var a = (e - t) * n + t, o = (r - e) * n + e, s = (o - a) * n + a;
  i[0] = t, i[1] = a, i[2] = s, i[3] = s, i[4] = o, i[5] = r;
}
function X1(t, e, r, n, i, a, o, s, u) {
  var l, f = 5e-3, h = 1 / 0;
  Ht[0] = o, Ht[1] = s;
  for (var c = 0; c < 1; c += 0.05) {
    yt[0] = Be(t, r, i, c), yt[1] = Be(e, n, a, c);
    var v = yn(Ht, yt);
    v < h && (l = c, h = v);
  }
  h = 1 / 0;
  for (var d = 0; d < 32 && !(f < Kg); d++) {
    var y = l - f, p = l + f;
    yt[0] = Be(t, r, i, y), yt[1] = Be(e, n, a, y);
    var v = yn(yt, Ht);
    if (y >= 0 && v < h)
      l = y, h = v;
    else {
      ti[0] = Be(t, r, i, p), ti[1] = Be(e, n, a, p);
      var g = yn(ti, Ht);
      p <= 1 && g < h ? (l = p, h = g) : f *= 0.5;
    }
  }
  return u && (u[0] = Be(t, r, i, l), u[1] = Be(e, n, a, l)), Ar(h);
}
function q1(t, e, r, n, i, a, o) {
  for (var s = t, u = e, l = 0, f = 1 / o, h = 1; h <= o; h++) {
    var c = h * f, v = Be(t, r, i, c), d = Be(e, n, a, c), y = v - s, p = d - u;
    l += Math.sqrt(y * y + p * p), s = v, u = d;
  }
  return l;
}
var $1 = /cubic-bezier\(([0-9,\.e ]+)\)/;
function ty(t) {
  var e = t && $1.exec(t);
  if (e) {
    var r = e[1].split(","), n = +It(r[0]), i = +It(r[1]), a = +It(r[2]), o = +It(r[3]);
    if (isNaN(n + i + a + o))
      return;
    var s = [];
    return function(u) {
      return u <= 0 ? 0 : u >= 1 ? 1 : Qg(0, n, a, 1, u, s) && Pe(0, i, o, 1, s[0]);
    };
  }
}
var Z1 = (function() {
  function t(e) {
    this._inited = !1, this._startTime = 0, this._pausedTime = 0, this._paused = !1, this._life = e.life || 1e3, this._delay = e.delay || 0, this.loop = e.loop || !1, this.onframe = e.onframe || Re, this.ondestroy = e.ondestroy || Re, this.onrestart = e.onrestart || Re, e.easing && this.setEasing(e.easing);
  }
  return t.prototype.step = function(e, r) {
    if (this._inited || (this._startTime = e + this._delay, this._inited = !0), this._paused) {
      this._pausedTime += r;
      return;
    }
    var n = this._life, i = e - this._startTime - this._pausedTime, a = i / n;
    a < 0 && (a = 0), a = Math.min(a, 1);
    var o = this.easingFunc, s = o ? o(a) : a;
    if (this.onframe(s), a === 1)
      if (this.loop) {
        var u = i % n;
        this._startTime = e - u, this._pausedTime = 0, this.onrestart();
      } else
        return !0;
    return !1;
  }, t.prototype.pause = function() {
    this._paused = !0;
  }, t.prototype.resume = function() {
    this._paused = !1;
  }, t.prototype.setEasing = function(e) {
    this.easing = e, this.easingFunc = X(e) ? e : ea[e] || ty(e);
  }, t;
})(), ry = /* @__PURE__ */ (function() {
  function t(e) {
    this.value = e;
  }
  return t;
})(), K1 = (function() {
  function t() {
    this._len = 0;
  }
  return t.prototype.insert = function(e) {
    var r = new ry(e);
    return this.insertEntry(r), r;
  }, t.prototype.insertEntry = function(e) {
    this.head ? (this.tail.next = e, e.prev = this.tail, e.next = null, this.tail = e) : this.head = this.tail = e, this._len++;
  }, t.prototype.remove = function(e) {
    var r = e.prev, n = e.next;
    r ? r.next = n : this.head = n, n ? n.prev = r : this.tail = r, e.next = e.prev = null, this._len--;
  }, t.prototype.len = function() {
    return this._len;
  }, t.prototype.clear = function() {
    this.head = this.tail = null, this._len = 0;
  }, t;
})(), ui = (function() {
  function t(e) {
    this._list = new K1(), this._maxSize = 10, this._map = {}, this._maxSize = e;
  }
  return t.prototype.put = function(e, r) {
    var n = this._list, i = this._map, a = null;
    if (i[e] == null) {
      var o = n.len(), s = this._lastRemovedEntry;
      if (o >= this._maxSize && o > 0) {
        var u = n.head;
        n.remove(u), delete i[u.key], a = u.value, this._lastRemovedEntry = u;
      }
      s ? s.value = r : s = new ry(r), s.key = e, n.insertEntry(s), i[e] = s;
    }
    return a;
  }, t.prototype.get = function(e) {
    var r = this._map[e], n = this._list;
    if (r != null)
      return r !== n.tail && (n.remove(r), n.insertEntry(r)), r.value;
  }, t.prototype.clear = function() {
    this._list.clear(), this._map = {};
  }, t.prototype.len = function() {
    return this._list.len();
  }, t;
})(), cv = {
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
function At(t) {
  return t = Math.round(t), t < 0 ? 0 : t > 255 ? 255 : t;
}
function j1(t) {
  return t = Math.round(t), t < 0 ? 0 : t > 360 ? 360 : t;
}
function da(t) {
  return t < 0 ? 0 : t > 1 ? 1 : t;
}
function ko(t) {
  var e = t;
  return e.length && e.charAt(e.length - 1) === "%" ? At(parseFloat(e) / 100 * 255) : At(parseInt(e, 10));
}
function Pr(t) {
  var e = t;
  return e.length && e.charAt(e.length - 1) === "%" ? da(parseFloat(e) / 100) : da(parseFloat(e));
}
function Eu(t, e, r) {
  return r < 0 ? r += 1 : r > 1 && (r -= 1), r * 6 < 1 ? t + (e - t) * r * 6 : r * 2 < 1 ? e : r * 3 < 2 ? t + (e - t) * (2 / 3 - r) * 6 : t;
}
function Dr(t, e, r) {
  return t + (e - t) * r;
}
function ct(t, e, r, n, i) {
  return t[0] = e, t[1] = r, t[2] = n, t[3] = i, t;
}
function ef(t, e) {
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t;
}
var ny = new ui(20), Xa = null;
function En(t, e) {
  Xa && ef(Xa, e), Xa = ny.put(t, Xa || e.slice());
}
function st(t, e) {
  if (t) {
    e = e || [];
    var r = ny.get(t);
    if (r)
      return ef(e, r);
    t = t + "";
    var n = t.replace(/ /g, "").toLowerCase();
    if (n in cv)
      return ef(e, cv[n]), En(t, e), e;
    var i = n.length;
    if (n.charAt(0) === "#") {
      if (i === 4 || i === 5) {
        var a = parseInt(n.slice(1, 4), 16);
        if (!(a >= 0 && a <= 4095)) {
          ct(e, 0, 0, 0, 1);
          return;
        }
        return ct(e, (a & 3840) >> 4 | (a & 3840) >> 8, a & 240 | (a & 240) >> 4, a & 15 | (a & 15) << 4, i === 5 ? parseInt(n.slice(4), 16) / 15 : 1), En(t, e), e;
      } else if (i === 7 || i === 9) {
        var a = parseInt(n.slice(1, 7), 16);
        if (!(a >= 0 && a <= 16777215)) {
          ct(e, 0, 0, 0, 1);
          return;
        }
        return ct(e, (a & 16711680) >> 16, (a & 65280) >> 8, a & 255, i === 9 ? parseInt(n.slice(7), 16) / 255 : 1), En(t, e), e;
      }
      return;
    }
    var o = n.indexOf("("), s = n.indexOf(")");
    if (o !== -1 && s + 1 === i) {
      var u = n.substr(0, o), l = n.substr(o + 1, s - (o + 1)).split(","), f = 1;
      switch (u) {
        case "rgba":
          if (l.length !== 4)
            return l.length === 3 ? ct(e, +l[0], +l[1], +l[2], 1) : ct(e, 0, 0, 0, 1);
          f = Pr(l.pop());
        case "rgb":
          if (l.length >= 3)
            return ct(e, ko(l[0]), ko(l[1]), ko(l[2]), l.length === 3 ? f : Pr(l[3])), En(t, e), e;
          ct(e, 0, 0, 0, 1);
          return;
        case "hsla":
          if (l.length !== 4) {
            ct(e, 0, 0, 0, 1);
            return;
          }
          return l[3] = Pr(l[3]), tf(l, e), En(t, e), e;
        case "hsl":
          if (l.length !== 3) {
            ct(e, 0, 0, 0, 1);
            return;
          }
          return tf(l, e), En(t, e), e;
        default:
          return;
      }
    }
    ct(e, 0, 0, 0, 1);
  }
}
function tf(t, e) {
  var r = (parseFloat(t[0]) % 360 + 360) % 360 / 360, n = Pr(t[1]), i = Pr(t[2]), a = i <= 0.5 ? i * (n + 1) : i + n - i * n, o = i * 2 - a;
  return e = e || [], ct(e, At(Eu(o, a, r + 1 / 3) * 255), At(Eu(o, a, r) * 255), At(Eu(o, a, r - 1 / 3) * 255), 1), t.length === 4 && (e[3] = t[3]), e;
}
function Q1(t) {
  if (t) {
    var e = t[0] / 255, r = t[1] / 255, n = t[2] / 255, i = Math.min(e, r, n), a = Math.max(e, r, n), o = a - i, s = (a + i) / 2, u, l;
    if (o === 0)
      u = 0, l = 0;
    else {
      s < 0.5 ? l = o / (a + i) : l = o / (2 - a - i);
      var f = ((a - e) / 6 + o / 2) / o, h = ((a - r) / 6 + o / 2) / o, c = ((a - n) / 6 + o / 2) / o;
      e === a ? u = c - h : r === a ? u = 1 / 3 + f - c : n === a && (u = 2 / 3 + h - f), u < 0 && (u += 1), u > 1 && (u -= 1);
    }
    var v = [u * 360, l, s];
    return t[3] != null && v.push(t[3]), v;
  }
}
function rf(t, e) {
  var r = st(t);
  if (r) {
    for (var n = 0; n < 3; n++)
      e < 0 ? r[n] = r[n] * (1 - e) | 0 : r[n] = (255 - r[n]) * e + r[n] | 0, r[n] > 255 ? r[n] = 255 : r[n] < 0 && (r[n] = 0);
    return Br(r, r.length === 4 ? "rgba" : "rgb");
  }
}
function J1(t) {
  var e = st(t);
  if (e)
    return ((1 << 24) + (e[0] << 16) + (e[1] << 8) + +e[2]).toString(16).slice(1);
}
function iy(t, e, r) {
  if (!(!(e && e.length) || !(t >= 0 && t <= 1))) {
    r = r || [];
    var n = t * (e.length - 1), i = Math.floor(n), a = Math.ceil(n), o = e[i], s = e[a], u = n - i;
    return r[0] = At(Dr(o[0], s[0], u)), r[1] = At(Dr(o[1], s[1], u)), r[2] = At(Dr(o[2], s[2], u)), r[3] = da(Dr(o[3], s[3], u)), r;
  }
}
var eb = iy;
function ay(t, e, r) {
  if (!(!(e && e.length) || !(t >= 0 && t <= 1))) {
    var n = t * (e.length - 1), i = Math.floor(n), a = Math.ceil(n), o = st(e[i]), s = st(e[a]), u = n - i, l = Br([
      At(Dr(o[0], s[0], u)),
      At(Dr(o[1], s[1], u)),
      At(Dr(o[2], s[2], u)),
      da(Dr(o[3], s[3], u))
    ], "rgba");
    return r ? {
      color: l,
      leftIndex: i,
      rightIndex: a,
      value: n
    } : l;
  }
}
var tb = ay;
function os(t, e, r, n) {
  var i = st(t);
  if (t)
    return i = Q1(i), e != null && (i[0] = j1(X(e) ? e(i[0]) : e)), r != null && (i[1] = Pr(X(r) ? r(i[1]) : r)), n != null && (i[2] = Pr(X(n) ? n(i[2]) : n)), Br(tf(i), "rgba");
}
function rb(t, e) {
  var r = st(t);
  if (r && e != null)
    return r[3] = da(e), Br(r, "rgba");
}
function Br(t, e) {
  if (!(!t || !t.length)) {
    var r = t[0] + "," + t[1] + "," + t[2];
    return (e === "rgba" || e === "hsva" || e === "hsla") && (r += "," + t[3]), e + "(" + r + ")";
  }
}
function pa(t, e) {
  var r = st(t);
  return r ? (0.299 * r[0] + 0.587 * r[1] + 0.114 * r[2]) * r[3] / 255 + (1 - r[3]) * e : 0;
}
function nb() {
  return Br([
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255)
  ], "rgb");
}
var vv = new ui(100);
function nf(t) {
  if (N(t)) {
    var e = vv.get(t);
    return e || (e = rf(t, -0.1), vv.put(t, e)), e;
  } else if (Aa(t)) {
    var r = E({}, t);
    return r.colorStops = H(t.colorStops, function(n) {
      return {
        offset: n.offset,
        color: rf(n.color, -0.1)
      };
    }), r;
  }
  return t;
}
const ib = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  fastLerp: iy,
  fastMapToColor: eb,
  lerp: ay,
  lift: rf,
  liftColor: nf,
  lum: pa,
  mapToColor: tb,
  modifyAlpha: rb,
  modifyHSL: os,
  parse: st,
  parseCssFloat: Pr,
  parseCssInt: ko,
  random: nb,
  stringify: Br,
  toHex: J1
}, Symbol.toStringTag, { value: "Module" }));
function ab(t) {
  return t.type === "linear";
}
function ob(t) {
  return t.type === "radial";
}
(function() {
  return typeof Buffer < "u" && typeof Buffer.from == "function" ? function(t) {
    return Buffer.from(t).toString("base64");
  } : typeof btoa == "function" && typeof unescape == "function" && typeof encodeURIComponent == "function" ? function(t) {
    return btoa(unescape(encodeURIComponent(t)));
  } : function(t) {
    return null;
  };
})();
var af = Array.prototype.slice;
function or(t, e, r) {
  return (e - t) * r + t;
}
function Ou(t, e, r, n) {
  for (var i = e.length, a = 0; a < i; a++)
    t[a] = or(e[a], r[a], n);
  return t;
}
function sb(t, e, r, n) {
  for (var i = e.length, a = i && e[0].length, o = 0; o < i; o++) {
    t[o] || (t[o] = []);
    for (var s = 0; s < a; s++)
      t[o][s] = or(e[o][s], r[o][s], n);
  }
  return t;
}
function qa(t, e, r, n) {
  for (var i = e.length, a = 0; a < i; a++)
    t[a] = e[a] + r[a] * n;
  return t;
}
function dv(t, e, r, n) {
  for (var i = e.length, a = i && e[0].length, o = 0; o < i; o++) {
    t[o] || (t[o] = []);
    for (var s = 0; s < a; s++)
      t[o][s] = e[o][s] + r[o][s] * n;
  }
  return t;
}
function ub(t, e) {
  for (var r = t.length, n = e.length, i = r > n ? e : t, a = Math.min(r, n), o = i[a - 1] || { color: [0, 0, 0, 0], offset: 0 }, s = a; s < Math.max(r, n); s++)
    i.push({
      offset: o.offset,
      color: o.color.slice()
    });
}
function lb(t, e, r) {
  var n = t, i = e;
  if (!(!n.push || !i.push)) {
    var a = n.length, o = i.length;
    if (a !== o) {
      var s = a > o;
      if (s)
        n.length = o;
      else
        for (var u = a; u < o; u++)
          n.push(r === 1 ? i[u] : af.call(i[u]));
    }
    for (var l = n[0] && n[0].length, u = 0; u < n.length; u++)
      if (r === 1)
        isNaN(n[u]) && (n[u] = i[u]);
      else
        for (var f = 0; f < l; f++)
          isNaN(n[u][f]) && (n[u][f] = i[u][f]);
  }
}
function Bo(t) {
  if (Ze(t)) {
    var e = t.length;
    if (Ze(t[0])) {
      for (var r = [], n = 0; n < e; n++)
        r.push(af.call(t[n]));
      return r;
    }
    return af.call(t);
  }
  return t;
}
function No(t) {
  return t[0] = Math.floor(t[0]) || 0, t[1] = Math.floor(t[1]) || 0, t[2] = Math.floor(t[2]) || 0, t[3] = t[3] == null ? 1 : t[3], "rgba(" + t.join(",") + ")";
}
function fb(t) {
  return Ze(t && t[0]) ? 2 : 1;
}
var $a = 0, Fo = 1, oy = 2, $i = 3, of = 4, sf = 5, pv = 6;
function gv(t) {
  return t === of || t === sf;
}
function Za(t) {
  return t === Fo || t === oy;
}
var Ai = [0, 0, 0, 0], hb = (function() {
  function t(e) {
    this.keyframes = [], this.discrete = !1, this._invalid = !1, this._needsSort = !1, this._lastFr = 0, this._lastFrP = 0, this.propName = e;
  }
  return t.prototype.isFinished = function() {
    return this._finished;
  }, t.prototype.setFinished = function() {
    this._finished = !0, this._additiveTrack && this._additiveTrack.setFinished();
  }, t.prototype.needsAnimate = function() {
    return this.keyframes.length >= 1;
  }, t.prototype.getAdditiveTrack = function() {
    return this._additiveTrack;
  }, t.prototype.addKeyframe = function(e, r, n) {
    this._needsSort = !0;
    var i = this.keyframes, a = i.length, o = !1, s = pv, u = r;
    if (Ze(r)) {
      var l = fb(r);
      s = l, (l === 1 && !ve(r[0]) || l === 2 && !ve(r[0][0])) && (o = !0);
    } else if (ve(r) && !ha(r))
      s = $a;
    else if (N(r))
      if (!isNaN(+r))
        s = $a;
      else {
        var f = st(r);
        f && (u = f, s = $i);
      }
    else if (Aa(r)) {
      var h = E({}, u);
      h.colorStops = H(r.colorStops, function(v) {
        return {
          offset: v.offset,
          color: st(v.color)
        };
      }), ab(r) ? s = of : ob(r) && (s = sf), u = h;
    }
    a === 0 ? this.valType = s : (s !== this.valType || s === pv) && (o = !0), this.discrete = this.discrete || o;
    var c = {
      time: e,
      value: u,
      rawValue: r,
      percent: 0
    };
    return n && (c.easing = n, c.easingFunc = X(n) ? n : ea[n] || ty(n)), i.push(c), c;
  }, t.prototype.prepare = function(e, r) {
    var n = this.keyframes;
    this._needsSort && n.sort(function(y, p) {
      return y.time - p.time;
    });
    for (var i = this.valType, a = n.length, o = n[a - 1], s = this.discrete, u = Za(i), l = gv(i), f = 0; f < a; f++) {
      var h = n[f], c = h.value, v = o.value;
      h.percent = h.time / e, s || (u && f !== a - 1 ? lb(c, v, i) : l && ub(c.colorStops, v.colorStops));
    }
    if (!s && i !== sf && r && this.needsAnimate() && r.needsAnimate() && i === r.valType && !r._finished) {
      this._additiveTrack = r;
      for (var d = n[0].value, f = 0; f < a; f++)
        i === $a ? n[f].additiveValue = n[f].value - d : i === $i ? n[f].additiveValue = qa([], n[f].value, d, -1) : Za(i) && (n[f].additiveValue = i === Fo ? qa([], n[f].value, d, -1) : dv([], n[f].value, d, -1));
    }
  }, t.prototype.step = function(e, r) {
    if (!this._finished) {
      this._additiveTrack && this._additiveTrack._finished && (this._additiveTrack = null);
      var n = this._additiveTrack != null, i = n ? "additiveValue" : "value", a = this.valType, o = this.keyframes, s = o.length, u = this.propName, l = a === $i, f, h = this._lastFr, c = Math.min, v, d;
      if (s === 1)
        v = d = o[0];
      else {
        if (r < 0)
          f = 0;
        else if (r < this._lastFrP) {
          var y = c(h + 1, s - 1);
          for (f = y; f >= 0 && !(o[f].percent <= r); f--)
            ;
          f = c(f, s - 2);
        } else {
          for (f = h; f < s && !(o[f].percent > r); f++)
            ;
          f = c(f - 1, s - 2);
        }
        d = o[f + 1], v = o[f];
      }
      if (v && d) {
        this._lastFr = f, this._lastFrP = r;
        var p = d.percent - v.percent, g = p === 0 ? 1 : c((r - v.percent) / p, 1);
        d.easingFunc && (g = d.easingFunc(g));
        var m = n ? this._additiveValue : l ? Ai : e[u];
        if ((Za(a) || l) && !m && (m = this._additiveValue = []), this.discrete)
          e[u] = g < 1 ? v.rawValue : d.rawValue;
        else if (Za(a))
          a === Fo ? Ou(m, v[i], d[i], g) : sb(m, v[i], d[i], g);
        else if (gv(a)) {
          var _ = v[i], S = d[i], b = a === of;
          e[u] = {
            type: b ? "linear" : "radial",
            x: or(_.x, S.x, g),
            y: or(_.y, S.y, g),
            colorStops: H(_.colorStops, function(C, M) {
              var x = S.colorStops[M];
              return {
                offset: or(C.offset, x.offset, g),
                color: No(Ou([], C.color, x.color, g))
              };
            }),
            global: S.global
          }, b ? (e[u].x2 = or(_.x2, S.x2, g), e[u].y2 = or(_.y2, S.y2, g)) : e[u].r = or(_.r, S.r, g);
        } else if (l)
          Ou(m, v[i], d[i], g), n || (e[u] = No(m));
        else {
          var T = or(v[i], d[i], g);
          n ? this._additiveValue = T : e[u] = T;
        }
        n && this._addToTarget(e);
      }
    }
  }, t.prototype._addToTarget = function(e) {
    var r = this.valType, n = this.propName, i = this._additiveValue;
    r === $a ? e[n] = e[n] + i : r === $i ? (st(e[n], Ai), qa(Ai, Ai, i, 1), e[n] = No(Ai)) : r === Fo ? qa(e[n], e[n], i, 1) : r === oy && dv(e[n], e[n], i, 1);
  }, t;
})(), yh = (function() {
  function t(e, r, n, i) {
    if (this._tracks = {}, this._trackKeys = [], this._maxTime = 0, this._started = 0, this._clip = null, this._target = e, this._loop = r, r && i) {
      Fs("Can' use additive animation on looped animation.");
      return;
    }
    this._additiveAnimators = i, this._allowDiscrete = n;
  }
  return t.prototype.getMaxTime = function() {
    return this._maxTime;
  }, t.prototype.getDelay = function() {
    return this._delay;
  }, t.prototype.getLoop = function() {
    return this._loop;
  }, t.prototype.getTarget = function() {
    return this._target;
  }, t.prototype.changeTarget = function(e) {
    this._target = e;
  }, t.prototype.when = function(e, r, n) {
    return this.whenWithKeys(e, r, me(r), n);
  }, t.prototype.whenWithKeys = function(e, r, n, i) {
    for (var a = this._tracks, o = 0; o < n.length; o++) {
      var s = n[o], u = a[s];
      if (!u) {
        u = a[s] = new hb(s);
        var l = void 0, f = this._getAdditiveTrack(s);
        if (f) {
          var h = f.keyframes, c = h[h.length - 1];
          l = c && c.value, f.valType === $i && l && (l = No(l));
        } else
          l = this._target[s];
        if (l == null)
          continue;
        e > 0 && u.addKeyframe(0, Bo(l), i), this._trackKeys.push(s);
      }
      u.addKeyframe(e, Bo(r[s]), i);
    }
    return this._maxTime = Math.max(this._maxTime, e), this;
  }, t.prototype.pause = function() {
    this._clip.pause(), this._paused = !0;
  }, t.prototype.resume = function() {
    this._clip.resume(), this._paused = !1;
  }, t.prototype.isPaused = function() {
    return !!this._paused;
  }, t.prototype.duration = function(e) {
    return this._maxTime = e, this._force = !0, this;
  }, t.prototype._doneCallback = function() {
    this._setTracksFinished(), this._clip = null;
    var e = this._doneCbs;
    if (e)
      for (var r = e.length, n = 0; n < r; n++)
        e[n].call(this);
  }, t.prototype._abortedCallback = function() {
    this._setTracksFinished();
    var e = this.animation, r = this._abortedCbs;
    if (e && e.removeClip(this._clip), this._clip = null, r)
      for (var n = 0; n < r.length; n++)
        r[n].call(this);
  }, t.prototype._setTracksFinished = function() {
    for (var e = this._tracks, r = this._trackKeys, n = 0; n < r.length; n++)
      e[r[n]].setFinished();
  }, t.prototype._getAdditiveTrack = function(e) {
    var r, n = this._additiveAnimators;
    if (n)
      for (var i = 0; i < n.length; i++) {
        var a = n[i].getTrack(e);
        a && (r = a);
      }
    return r;
  }, t.prototype.start = function(e) {
    if (!(this._started > 0)) {
      this._started = 1;
      for (var r = this, n = [], i = this._maxTime || 0, a = 0; a < this._trackKeys.length; a++) {
        var o = this._trackKeys[a], s = this._tracks[o], u = this._getAdditiveTrack(o), l = s.keyframes, f = l.length;
        if (s.prepare(i, u), s.needsAnimate())
          if (!this._allowDiscrete && s.discrete) {
            var h = l[f - 1];
            h && (r._target[s.propName] = h.rawValue), s.setFinished();
          } else
            n.push(s);
      }
      if (n.length || this._force) {
        var c = new Z1({
          life: i,
          loop: this._loop,
          delay: this._delay || 0,
          onframe: function(v) {
            r._started = 2;
            var d = r._additiveAnimators;
            if (d) {
              for (var y = !1, p = 0; p < d.length; p++)
                if (d[p]._clip) {
                  y = !0;
                  break;
                }
              y || (r._additiveAnimators = null);
            }
            for (var p = 0; p < n.length; p++)
              n[p].step(r._target, v);
            var g = r._onframeCbs;
            if (g)
              for (var p = 0; p < g.length; p++)
                g[p](r._target, v);
          },
          ondestroy: function() {
            r._doneCallback();
          }
        });
        this._clip = c, this.animation && this.animation.addClip(c), e && c.setEasing(e);
      } else
        this._doneCallback();
      return this;
    }
  }, t.prototype.stop = function(e) {
    if (this._clip) {
      var r = this._clip;
      e && r.onframe(1), this._abortedCallback();
    }
  }, t.prototype.delay = function(e) {
    return this._delay = e, this;
  }, t.prototype.during = function(e) {
    return e && (this._onframeCbs || (this._onframeCbs = []), this._onframeCbs.push(e)), this;
  }, t.prototype.done = function(e) {
    return e && (this._doneCbs || (this._doneCbs = []), this._doneCbs.push(e)), this;
  }, t.prototype.aborted = function(e) {
    return e && (this._abortedCbs || (this._abortedCbs = []), this._abortedCbs.push(e)), this;
  }, t.prototype.getClip = function() {
    return this._clip;
  }, t.prototype.getTrack = function(e) {
    return this._tracks[e];
  }, t.prototype.getTracks = function() {
    var e = this;
    return H(this._trackKeys, function(r) {
      return e._tracks[r];
    });
  }, t.prototype.stopTracks = function(e, r) {
    if (!e.length || !this._clip)
      return !0;
    for (var n = this._tracks, i = this._trackKeys, a = 0; a < e.length; a++) {
      var o = n[e[a]];
      o && !o.isFinished() && (r ? o.step(this._target, 1) : this._started === 1 && o.step(this._target, 0), o.setFinished());
    }
    for (var s = !0, a = 0; a < i.length; a++)
      if (!n[i[a]].isFinished()) {
        s = !1;
        break;
      }
    return s && this._abortedCallback(), s;
  }, t.prototype.saveTo = function(e, r, n) {
    if (e) {
      r = r || this._trackKeys;
      for (var i = 0; i < r.length; i++) {
        var a = r[i], o = this._tracks[a];
        if (!(!o || o.isFinished())) {
          var s = o.keyframes, u = s[n ? 0 : s.length - 1];
          u && (e[a] = Bo(u.rawValue));
        }
      }
    }
  }, t.prototype.__changeFinalValue = function(e, r) {
    r = r || me(e);
    for (var n = 0; n < r.length; n++) {
      var i = r[n], a = this._tracks[i];
      if (a) {
        var o = a.keyframes;
        if (o.length > 1) {
          var s = o.pop();
          a.addKeyframe(s.time, e[i]), a.prepare(this._maxTime, a.getAdditiveTrack());
        }
      }
    }
  }, t;
})();
function jn() {
  return (/* @__PURE__ */ new Date()).getTime();
}
var cb = (function(t) {
  F(e, t);
  function e(r) {
    var n = t.call(this) || this;
    return n._running = !1, n._time = 0, n._pausedTime = 0, n._pauseStart = 0, n._paused = !1, r = r || {}, n.stage = r.stage || {}, n;
  }
  return e.prototype.addClip = function(r) {
    r.animation && this.removeClip(r), this._head ? (this._tail.next = r, r.prev = this._tail, r.next = null, this._tail = r) : this._head = this._tail = r, r.animation = this;
  }, e.prototype.addAnimator = function(r) {
    r.animation = this;
    var n = r.getClip();
    n && this.addClip(n);
  }, e.prototype.removeClip = function(r) {
    if (r.animation) {
      var n = r.prev, i = r.next;
      n ? n.next = i : this._head = i, i ? i.prev = n : this._tail = n, r.next = r.prev = r.animation = null;
    }
  }, e.prototype.removeAnimator = function(r) {
    var n = r.getClip();
    n && this.removeClip(n), r.animation = null;
  }, e.prototype.update = function(r) {
    for (var n = jn() - this._pausedTime, i = n - this._time, a = this._head; a; ) {
      var o = a.next, s = a.step(n, i);
      s && (a.ondestroy(), this.removeClip(a)), a = o;
    }
    this._time = n, r || (this.trigger("frame", i), this.stage.update && this.stage.update());
  }, e.prototype._startLoop = function() {
    var r = this;
    this._running = !0;
    function n() {
      r._running && (rs(n), !r._paused && r.update());
    }
    rs(n);
  }, e.prototype.start = function() {
    this._running || (this._time = jn(), this._pausedTime = 0, this._startLoop());
  }, e.prototype.stop = function() {
    this._running = !1;
  }, e.prototype.pause = function() {
    this._paused || (this._pauseStart = jn(), this._paused = !0);
  }, e.prototype.resume = function() {
    this._paused && (this._pausedTime += jn() - this._pauseStart, this._paused = !1);
  }, e.prototype.clear = function() {
    for (var r = this._head; r; ) {
      var n = r.next;
      r.prev = r.next = r.animation = null, r = n;
    }
    this._head = this._tail = null;
  }, e.prototype.isFinished = function() {
    return this._head == null;
  }, e.prototype.animate = function(r, n) {
    n = n || {}, this.start();
    var i = new yh(r, n.loop);
    return this.addAnimator(i), i;
  }, e;
})(Kt), vb = 300, ku = W.domSupported, Bu = (function() {
  var t = [
    "click",
    "dblclick",
    "mousewheel",
    "wheel",
    "mouseout",
    "mouseup",
    "mousedown",
    "mousemove",
    "contextmenu"
  ], e = [
    "touchstart",
    "touchend",
    "touchmove"
  ], r = {
    pointerdown: 1,
    pointerup: 1,
    pointermove: 1,
    pointerout: 1
  }, n = H(t, function(i) {
    var a = i.replace("mouse", "pointer");
    return r.hasOwnProperty(a) ? a : i;
  });
  return {
    mouse: t,
    touch: e,
    pointer: n
  };
})(), yv = {
  mouse: ["mousemove", "mouseup"],
  pointer: ["pointermove", "pointerup"]
}, mv = !1;
function uf(t) {
  var e = t.pointerType;
  return e === "pen" || e === "touch";
}
function db(t) {
  t.touching = !0, t.touchTimer != null && (clearTimeout(t.touchTimer), t.touchTimer = null), t.touchTimer = setTimeout(function() {
    t.touching = !1, t.touchTimer = null;
  }, 700);
}
function Nu(t) {
  t && (t.zrByTouch = !0);
}
function pb(t, e) {
  return dt(t.dom, new gb(t, e), !0);
}
function sy(t, e) {
  for (var r = e, n = !1; r && r.nodeType !== 9 && !(n = r.domBelongToZr || r !== e && r === t.painterRoot); )
    r = r.parentNode;
  return n;
}
var gb = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.stopPropagation = Re, this.stopImmediatePropagation = Re, this.preventDefault = Re, this.type = r.type, this.target = this.currentTarget = e.dom, this.pointerType = r.pointerType, this.clientX = r.clientX, this.clientY = r.clientY;
  }
  return t;
})(), Mt = {
  mousedown: function(t) {
    t = dt(this.dom, t), this.__mayPointerCapture = [t.zrX, t.zrY], this.trigger("mousedown", t);
  },
  mousemove: function(t) {
    t = dt(this.dom, t);
    var e = this.__mayPointerCapture;
    e && (t.zrX !== e[0] || t.zrY !== e[1]) && this.__togglePointerCapture(!0), this.trigger("mousemove", t);
  },
  mouseup: function(t) {
    t = dt(this.dom, t), this.__togglePointerCapture(!1), this.trigger("mouseup", t);
  },
  mouseout: function(t) {
    t = dt(this.dom, t);
    var e = t.toElement || t.relatedTarget;
    sy(this, e) || (this.__pointerCapturing && (t.zrEventControl = "no_globalout"), this.trigger("mouseout", t));
  },
  wheel: function(t) {
    mv = !0, t = dt(this.dom, t), this.trigger("mousewheel", t);
  },
  mousewheel: function(t) {
    mv || (t = dt(this.dom, t), this.trigger("mousewheel", t));
  },
  touchstart: function(t) {
    t = dt(this.dom, t), Nu(t), this.__lastTouchMoment = /* @__PURE__ */ new Date(), this.handler.processGesture(t, "start"), Mt.mousemove.call(this, t), Mt.mousedown.call(this, t);
  },
  touchmove: function(t) {
    t = dt(this.dom, t), Nu(t), this.handler.processGesture(t, "change"), Mt.mousemove.call(this, t);
  },
  touchend: function(t) {
    t = dt(this.dom, t), Nu(t), this.handler.processGesture(t, "end"), Mt.mouseup.call(this, t), +/* @__PURE__ */ new Date() - +this.__lastTouchMoment < vb && Mt.click.call(this, t);
  },
  pointerdown: function(t) {
    Mt.mousedown.call(this, t);
  },
  pointermove: function(t) {
    uf(t) || Mt.mousemove.call(this, t);
  },
  pointerup: function(t) {
    Mt.mouseup.call(this, t);
  },
  pointerout: function(t) {
    uf(t) || Mt.mouseout.call(this, t);
  }
};
w(["click", "dblclick", "contextmenu"], function(t) {
  Mt[t] = function(e) {
    e = dt(this.dom, e), this.trigger(t, e);
  };
});
var lf = {
  pointermove: function(t) {
    uf(t) || lf.mousemove.call(this, t);
  },
  pointerup: function(t) {
    lf.mouseup.call(this, t);
  },
  mousemove: function(t) {
    this.trigger("mousemove", t);
  },
  mouseup: function(t) {
    var e = this.__pointerCapturing;
    this.__togglePointerCapture(!1), this.trigger("mouseup", t), e && (t.zrEventControl = "only_globalout", this.trigger("mouseout", t));
  }
};
function yb(t, e) {
  var r = e.domHandlers;
  W.pointerEventsSupported ? w(Bu.pointer, function(n) {
    zo(e, n, function(i) {
      r[n].call(t, i);
    });
  }) : (W.touchEventsSupported && w(Bu.touch, function(n) {
    zo(e, n, function(i) {
      r[n].call(t, i), db(e);
    });
  }), w(Bu.mouse, function(n) {
    zo(e, n, function(i) {
      i = gh(i), e.touching || r[n].call(t, i);
    });
  }));
}
function mb(t, e) {
  W.pointerEventsSupported ? w(yv.pointer, r) : W.touchEventsSupported || w(yv.mouse, r);
  function r(n) {
    function i(a) {
      a = gh(a), sy(t, a.target) || (a = pb(t, a), e.domHandlers[n].call(t, a));
    }
    zo(e, n, i, { capture: !0 });
  }
}
function zo(t, e, r, n) {
  t.mounted[e] = r, t.listenerOpts[e] = n, C1(t.domTarget, e, r, n);
}
function Fu(t) {
  var e = t.mounted;
  for (var r in e)
    e.hasOwnProperty(r) && x1(t.domTarget, r, e[r], t.listenerOpts[r]);
  t.mounted = {};
}
var _v = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.mounted = {}, this.listenerOpts = {}, this.touching = !1, this.domTarget = e, this.domHandlers = r;
  }
  return t;
})(), _b = (function(t) {
  F(e, t);
  function e(r, n) {
    var i = t.call(this) || this;
    return i.__pointerCapturing = !1, i.dom = r, i.painterRoot = n, i._localHandlerScope = new _v(r, Mt), ku && (i._globalHandlerScope = new _v(document, lf)), yb(i, i._localHandlerScope), i;
  }
  return e.prototype.dispose = function() {
    Fu(this._localHandlerScope), ku && Fu(this._globalHandlerScope);
  }, e.prototype.setCursor = function(r) {
    this.dom.style && (this.dom.style.cursor = r || "default");
  }, e.prototype.__togglePointerCapture = function(r) {
    if (this.__mayPointerCapture = null, ku && +this.__pointerCapturing ^ +r) {
      this.__pointerCapturing = r;
      var n = this._globalHandlerScope;
      r ? mb(this, n) : Fu(n);
    }
  }, e;
})(Kt), uy = 1;
W.hasGlobalWindow && (uy = Math.max(window.devicePixelRatio || window.screen && window.screen.deviceXDPI / window.screen.logicalXDPI || 1, 1));
var ss = uy, ff = 0.4, hf = "#333", cf = "#ccc", Sb = "#eee", Sv = Mn, bv = 5e-5;
function Yr(t) {
  return t > bv || t < -bv;
}
var Wr = [], On = [], zu = Tt(), Hu = Math.abs, Ra = (function() {
  function t() {
  }
  return t.prototype.getLocalTransform = function(e) {
    return bb(this, e);
  }, t.prototype.setPosition = function(e) {
    this.x = e[0], this.y = e[1];
  }, t.prototype.setScale = function(e) {
    this.scaleX = e[0], this.scaleY = e[1];
  }, t.prototype.setSkew = function(e) {
    this.skewX = e[0], this.skewY = e[1];
  }, t.prototype.setOrigin = function(e) {
    this.originX = e[0], this.originY = e[1];
  }, t.prototype.needLocalTransform = function() {
    return Yr(this.rotation) || Yr(this.x) || Yr(this.y) || Yr(this.scaleX - 1) || Yr(this.scaleY - 1) || Yr(this.skewX) || Yr(this.skewY);
  }, t.prototype.updateTransform = function() {
    var e = this.parent && this.parent.transform, r = this.needLocalTransform(), n = this.transform;
    if (!(r || e)) {
      n && (Sv(n), this.invTransform = null);
      return;
    }
    n = n || Tt(), r ? this.getLocalTransform(n) : Sv(n), e && (r ? mn(n, e, n) : La(n, e)), this.transform = n, this._resolveGlobalScaleRatio(n), this.invTransform = this.invTransform || Tt(), gi(this.invTransform, n);
  }, t.prototype._resolveGlobalScaleRatio = function(e) {
    var r = this.globalScaleRatio;
    if (r != null && r !== 1) {
      this.getGlobalScale(Wr);
      var n = Wr[0] < 0 ? -1 : 1, i = Wr[1] < 0 ? -1 : 1, a = ((Wr[0] - n) * r + n) / Wr[0] || 0, o = ((Wr[1] - i) * r + i) / Wr[1] || 0;
      e[0] *= a, e[1] *= a, e[2] *= o, e[3] *= o;
    }
  }, t.prototype.getComputedTransform = function() {
    for (var e = this, r = []; e; )
      r.push(e), e = e.parent;
    for (; e = r.pop(); )
      e.updateTransform();
    return this.transform;
  }, t.prototype.setLocalTransform = function(e) {
    if (e) {
      var r = e[0] * e[0] + e[1] * e[1], n = e[2] * e[2] + e[3] * e[3], i = Math.atan2(e[1], e[0]), a = Math.PI / 2 + i - Math.atan2(e[3], e[2]);
      n = Math.sqrt(n) * Math.cos(a), r = Math.sqrt(r), this.skewX = a, this.skewY = 0, this.rotation = -i, this.x = +e[4], this.y = +e[5], this.scaleX = r, this.scaleY = n, this.originX = 0, this.originY = 0;
    }
  }, t.prototype.decomposeTransform = function() {
    if (this.transform) {
      var e = this.parent, r = this.transform;
      e && e.transform && (e.invTransform = e.invTransform || Tt(), mn(On, e.invTransform, r), r = On);
      var n = this.originX, i = this.originY;
      (n || i) && (zu[4] = n, zu[5] = i, mn(On, r, zu), On[4] -= n, On[5] -= i, r = On), this.setLocalTransform(r);
    }
  }, t.prototype.getGlobalScale = function(e) {
    var r = this.transform;
    return e = e || [], r ? (e[0] = Math.sqrt(r[0] * r[0] + r[1] * r[1]), e[1] = Math.sqrt(r[2] * r[2] + r[3] * r[3]), r[0] < 0 && (e[0] = -e[0]), r[3] < 0 && (e[1] = -e[1]), e) : (e[0] = 1, e[1] = 1, e);
  }, t.prototype.transformCoordToLocal = function(e, r) {
    var n = [e, r], i = this.invTransform;
    return i && ot(n, n, i), n;
  }, t.prototype.transformCoordToGlobal = function(e, r) {
    var n = [e, r], i = this.transform;
    return i && ot(n, n, i), n;
  }, t.prototype.getLineScale = function() {
    var e = this.transform;
    return e && Hu(e[0] - 1) > 1e-10 && Hu(e[3] - 1) > 1e-10 ? Math.sqrt(Hu(e[0] * e[3] - e[2] * e[1])) : 1;
  }, t.prototype.copyTransform = function(e) {
    us(this, e);
  }, t.getLocalTransform = function(e, r) {
    r = r || [];
    var n = e.originX || 0, i = e.originY || 0, a = e.scaleX, o = e.scaleY, s = e.anchorX, u = e.anchorY, l = e.rotation || 0, f = e.x, h = e.y, c = e.skewX ? Math.tan(e.skewX) : 0, v = e.skewY ? Math.tan(-e.skewY) : 0;
    if (n || i || s || u) {
      var d = n + s, y = i + u;
      r[4] = -d * a - c * y * o, r[5] = -y * o - v * d * a;
    } else
      r[4] = r[5] = 0;
    return r[0] = a, r[3] = o, r[1] = v * a, r[2] = c * o, l && Vs(r, r, l), r[4] += n + f, r[5] += i + h, r;
  }, t.initDefaultProps = (function() {
    var e = t.prototype;
    e.scaleX = e.scaleY = e.globalScaleRatio = 1, e.x = e.y = e.originX = e.originY = e.skewX = e.skewY = e.rotation = e.anchorX = e.anchorY = 0;
  })(), t;
})(), bb = Ra.getLocalTransform, Gs = [
  "x",
  "y",
  "originX",
  "originY",
  "anchorX",
  "anchorY",
  "rotation",
  "scaleX",
  "scaleY",
  "skewX",
  "skewY"
];
function us(t, e) {
  return Pg(t, e, Gs);
}
function Gt(t) {
  Ka || (Ka = new ui(100)), t = t || Er;
  var e = Ka.get(t);
  return e || (e = {
    font: t,
    strWidthCache: new ui(500),
    asciiWidthMap: null,
    asciiWidthMapTried: !1,
    stWideCharWidth: Le.measureText("国", t).width,
    asciiCharWidth: Le.measureText("a", t).width
  }, Ka.put(t, e)), e;
}
var Ka;
function Tb(t) {
  if (!(Vu >= Tv)) {
    t = t || Er;
    for (var e = [], r = +/* @__PURE__ */ new Date(), n = 0; n <= 127; n++)
      e[n] = Le.measureText(String.fromCharCode(n), t).width;
    var i = +/* @__PURE__ */ new Date() - r;
    return i > 16 ? Vu = Tv : i > 2 && Vu++, e;
  }
}
var Vu = 0, Tv = 5;
function ly(t, e) {
  return t.asciiWidthMapTried || (t.asciiWidthMap = Tb(t.font), t.asciiWidthMapTried = !0), 0 <= e && e <= 127 ? t.asciiWidthMap != null ? t.asciiWidthMap[e] : t.asciiCharWidth : t.stWideCharWidth;
}
function Ut(t, e) {
  var r = t.strWidthCache, n = r.get(e);
  return n == null && (n = Le.measureText(e, t.font).width, r.put(e, n)), n;
}
function wv(t, e, r, n) {
  var i = Ut(Gt(e), t), a = Us(e), o = li(0, i, r), s = _n(0, a, n), u = new Y(o, s, i, a);
  return u;
}
function fy(t, e, r, n) {
  var i = ((t || "") + "").split(`
`), a = i.length;
  if (a === 1)
    return wv(i[0], e, r, n);
  for (var o = new Y(0, 0, 0, 0), s = 0; s < i.length; s++) {
    var u = wv(i[s], e, r, n);
    s === 0 ? o.copy(u) : o.union(u);
  }
  return o;
}
function li(t, e, r, n) {
  return r === "right" ? n ? t += e : t -= e : r === "center" && (n ? t += e / 2 : t -= e / 2), t;
}
function _n(t, e, r, n) {
  return r === "middle" ? n ? t += e / 2 : t -= e / 2 : r === "bottom" && (n ? t += e : t -= e), t;
}
function Us(t) {
  return Gt(t).stWideCharWidth;
}
function fi(t, e) {
  return typeof t == "string" ? t.lastIndexOf("%") >= 0 ? parseFloat(t) / 100 * e : parseFloat(t) : t;
}
function hy(t, e, r) {
  var n = e.position || "inside", i = e.distance != null ? e.distance : 5, a = r.height, o = r.width, s = a / 2, u = r.x, l = r.y, f = "left", h = "top";
  if (n instanceof Array)
    u += fi(n[0], r.width), l += fi(n[1], r.height), f = null, h = null;
  else
    switch (n) {
      case "left":
        u -= i, l += s, f = "right", h = "middle";
        break;
      case "right":
        u += i + o, l += s, h = "middle";
        break;
      case "top":
        u += o / 2, l -= i, f = "center", h = "bottom";
        break;
      case "bottom":
        u += o / 2, l += a + i, f = "center";
        break;
      case "inside":
        u += o / 2, l += s, f = "center", h = "middle";
        break;
      case "insideLeft":
        u += i, l += s, h = "middle";
        break;
      case "insideRight":
        u += o - i, l += s, f = "right", h = "middle";
        break;
      case "insideTop":
        u += o / 2, l += i, f = "center";
        break;
      case "insideBottom":
        u += o / 2, l += a - i, f = "center", h = "bottom";
        break;
      case "insideTopLeft":
        u += i, l += i;
        break;
      case "insideTopRight":
        u += o - i, l += i, f = "right";
        break;
      case "insideBottomLeft":
        u += i, l += a - i, h = "bottom";
        break;
      case "insideBottomRight":
        u += o - i, l += a - i, f = "right", h = "bottom";
        break;
    }
  return t = t || {}, t.x = u, t.y = l, t.align = f, t.verticalAlign = h, t;
}
var Gu = "__zr_normal__", Uu = Gs.concat(["ignore"]), wb = Xt(Gs, function(t, e) {
  return t[e] = !0, t;
}, { ignore: !1 }), kn = {}, Cb = new Y(0, 0, 0, 0), ja = [], Ho = 0, Ys = 1, Ws = (function() {
  function t(e) {
    this.id = hh(), this.animators = [], this.currentStates = [], this.states = {}, this._init(e);
  }
  return t.prototype._init = function(e) {
    this.attr(e);
  }, t.prototype.drift = function(e, r, n) {
    switch (this.draggable) {
      case "horizontal":
        r = 0;
        break;
      case "vertical":
        e = 0;
        break;
    }
    var i = this.transform;
    i || (i = this.transform = [1, 0, 0, 1, 0, 0]), i[4] += e, i[5] += r, this.decomposeTransform(), this.markRedraw();
  }, t.prototype.beforeUpdate = function() {
  }, t.prototype.afterUpdate = function() {
  }, t.prototype.update = function() {
    this.updateTransform(), this.__dirty && this.updateInnerText();
  }, t.prototype.updateInnerText = function(e) {
    var r = this._textContent;
    if (r && (!r.ignore || e)) {
      this.textConfig || (this.textConfig = {});
      var n = this.textConfig, i = n.local, a = r.innerTransformable, o = void 0, s = void 0, u = !1;
      a.parent = i ? this : null;
      var l = !1;
      a.copyTransform(r);
      var f = n.position != null, h = n.autoOverflowArea, c = void 0;
      if ((h || f) && (c = Cb, n.layoutRect ? c.copy(n.layoutRect) : c.copy(this.getBoundingRect()), i || c.applyTransform(this.transform)), f) {
        this.calculateTextPosition ? this.calculateTextPosition(kn, n, c) : hy(kn, n, c), a.x = kn.x, a.y = kn.y, o = kn.align, s = kn.verticalAlign;
        var v = n.origin;
        if (v && n.rotation != null) {
          var d = void 0, y = void 0;
          v === "center" ? (d = c.width * 0.5, y = c.height * 0.5) : (d = fi(v[0], c.width), y = fi(v[1], c.height)), l = !0, a.originX = -a.x + d + (i ? 0 : c.x), a.originY = -a.y + y + (i ? 0 : c.y);
        }
      }
      n.rotation != null && (a.rotation = n.rotation);
      var p = n.offset;
      p && (a.x += p[0], a.y += p[1], l || (a.originX = -p[0], a.originY = -p[1]));
      var g = this._innerTextDefaultStyle || (this._innerTextDefaultStyle = {});
      if (h) {
        var m = g.overflowRect = g.overflowRect || new Y(0, 0, 0, 0);
        a.getLocalTransform(ja), gi(ja, ja), Y.copy(m, c), m.applyTransform(ja);
      } else
        g.overflowRect = null;
      var _ = n.inside == null ? typeof n.position == "string" && n.position.indexOf("inside") >= 0 : n.inside, S = void 0, b = void 0, T = void 0;
      _ && this.canBeInsideText() ? (S = n.insideFill, b = n.insideStroke, (S == null || S === "auto") && (S = this.getInsideTextFill()), (b == null || b === "auto") && (b = this.getInsideTextStroke(S), T = !0)) : (S = n.outsideFill, b = n.outsideStroke, (S == null || S === "auto") && (S = this.getOutsideFill()), (b == null || b === "auto") && (b = this.getOutsideStroke(S), T = !0)), S = S || "#000", (S !== g.fill || b !== g.stroke || T !== g.autoStroke || o !== g.align || s !== g.verticalAlign) && (u = !0, g.fill = S, g.stroke = b, g.autoStroke = T, g.align = o, g.verticalAlign = s, r.setDefaultTextStyle(g)), r.__dirty |= Je, u && r.dirtyStyle(!0);
    }
  }, t.prototype.canBeInsideText = function() {
    return !0;
  }, t.prototype.getInsideTextFill = function() {
    return "#fff";
  }, t.prototype.getInsideTextStroke = function(e) {
    return "#000";
  }, t.prototype.getOutsideFill = function() {
    return this.__zr && this.__zr.isDarkMode() ? cf : hf;
  }, t.prototype.getOutsideStroke = function(e) {
    var r = this.__zr && this.__zr.getBackgroundColor(), n = typeof r == "string" && st(r);
    n || (n = [255, 255, 255, 1]);
    for (var i = n[3], a = this.__zr.isDarkMode(), o = 0; o < 3; o++)
      n[o] = n[o] * i + (a ? 0 : 255) * (1 - i);
    return n[3] = 1, Br(n, "rgba");
  }, t.prototype.traverse = function(e, r) {
  }, t.prototype.attrKV = function(e, r) {
    e === "textConfig" ? this.setTextConfig(r) : e === "textContent" ? this.setTextContent(r) : e === "clipPath" ? this.setClipPath(r) : e === "extra" ? (this.extra = this.extra || {}, E(this.extra, r)) : this[e] = r;
  }, t.prototype.hide = function() {
    this.ignore = !0, this.markRedraw();
  }, t.prototype.show = function() {
    this.ignore = !1, this.markRedraw();
  }, t.prototype.attr = function(e, r) {
    if (typeof e == "string")
      this.attrKV(e, r);
    else if (z(e))
      for (var n = e, i = me(n), a = 0; a < i.length; a++) {
        var o = i[a];
        this.attrKV(o, e[o]);
      }
    return this.markRedraw(), this;
  }, t.prototype.saveCurrentToNormalState = function(e) {
    this._innerSaveToNormal(e);
    for (var r = this._normalState, n = 0; n < this.animators.length; n++) {
      var i = this.animators[n], a = i.__fromStateTransition;
      if (!(i.getLoop() || a && a !== Gu)) {
        var o = i.targetName, s = o ? r[o] : r;
        i.saveTo(s);
      }
    }
  }, t.prototype._innerSaveToNormal = function(e) {
    var r = this._normalState;
    r || (r = this._normalState = {}), e.textConfig && !r.textConfig && (r.textConfig = this.textConfig), this._savePrimaryToNormal(e, r, Uu);
  }, t.prototype._savePrimaryToNormal = function(e, r, n) {
    for (var i = 0; i < n.length; i++) {
      var a = n[i];
      e[a] != null && !(a in r) && (r[a] = this[a]);
    }
  }, t.prototype.hasState = function() {
    return this.currentStates.length > 0;
  }, t.prototype.getState = function(e) {
    return this.states[e];
  }, t.prototype.ensureState = function(e) {
    var r = this.states;
    return r[e] || (r[e] = {}), r[e];
  }, t.prototype.clearStates = function(e) {
    this.useState(Gu, !1, e);
  }, t.prototype.useState = function(e, r, n, i) {
    var a = e === Gu, o = this.hasState();
    if (!(!o && a)) {
      var s = this.currentStates, u = this.stateTransition;
      if (!(oe(s, e) >= 0 && (r || s.length === 1))) {
        var l;
        if (this.stateProxy && !a && (l = this.stateProxy(e)), l || (l = this.states && this.states[e]), !l && !a) {
          Fs("State " + e + " not exists.");
          return;
        }
        a || this.saveCurrentToNormalState(l);
        var f = this._textContent, h = Cv(this, f, l, i);
        h && !this.__inHover && (this.__inHover = h), this._applyStateObj(e, l, this._normalState, r, Mv(this, n, u), u);
        var c = this._textGuide;
        return f && f.useState(e, r, n, !!h), c && c.useState(e, r, n, !!h), a ? (this.currentStates = [], this._normalState = {}) : r ? this.currentStates.push(e) : this.currentStates = [e], this._updateAnimationTargets(), this.markRedraw(), !h && this.__inHover && (this.__inHover = Ho, this.__dirty &= ~Je), l;
      }
    }
  }, t.prototype.useStates = function(e, r, n) {
    if (!e.length)
      this.clearStates();
    else {
      var i = [], a = this.currentStates, o = e.length, s = o === a.length;
      if (s) {
        for (var u = 0; u < o; u++)
          if (e[u] !== a[u]) {
            s = !1;
            break;
          }
      }
      if (s)
        return;
      for (var u = 0; u < o; u++) {
        var l = e[u], f = void 0;
        this.stateProxy && (f = this.stateProxy(l, e)), f || (f = this.states[l]), f && i.push(f);
      }
      var h = i[o - 1], c = this._textContent, v = Cv(this, c, h, n);
      v && !this.__inHover && (this.__inHover = v);
      var d = this._mergeStates(i), y = this.stateTransition;
      this.saveCurrentToNormalState(d), this._applyStateObj(e.join(","), d, this._normalState, !1, Mv(this, r, y), y);
      var p = this._textGuide;
      c && c.useStates(e, r, !!v), p && p.useStates(e, r, !!v), this._updateAnimationTargets(), this.currentStates = e.slice(), this.markRedraw(), !v && this.__inHover && (this.__inHover = Ho, this.__dirty &= ~Je);
    }
  }, t.prototype.isSilent = function() {
    for (var e = this; e; ) {
      if (e.silent)
        return !0;
      var r = e.__hostTarget;
      e = r ? e.ignoreHostSilent ? null : r : e.parent;
    }
    return !1;
  }, t.prototype._updateAnimationTargets = function() {
    for (var e = 0; e < this.animators.length; e++) {
      var r = this.animators[e];
      r.targetName && r.changeTarget(this[r.targetName]);
    }
  }, t.prototype.removeState = function(e) {
    var r = oe(this.currentStates, e);
    if (r >= 0) {
      var n = this.currentStates.slice();
      n.splice(r, 1), this.useStates(n);
    }
  }, t.prototype.replaceState = function(e, r, n) {
    var i = this.currentStates.slice(), a = oe(i, e), o = oe(i, r) >= 0;
    a >= 0 ? o ? i.splice(a, 1) : i[a] = r : n && !o && i.push(r), this.useStates(i);
  }, t.prototype.toggleState = function(e, r) {
    r ? this.useState(e, !0) : this.removeState(e);
  }, t.prototype._mergeStates = function(e) {
    for (var r = {}, n, i = 0; i < e.length; i++) {
      var a = e[i];
      E(r, a), a.textConfig && (n = n || {}, E(n, a.textConfig));
    }
    return n && (r.textConfig = n), r;
  }, t.prototype._applyStateObj = function(e, r, n, i, a, o) {
    if (this.__inHover !== Ys) {
      var s = !(r && i);
      r && r.textConfig ? (this.textConfig = E({}, i ? this.textConfig : n.textConfig), E(this.textConfig, r.textConfig)) : s && n.textConfig && (this.textConfig = n.textConfig);
      for (var u = {}, l = !1, f = 0; f < Uu.length; f++) {
        var h = Uu[f], c = a && wb[h];
        r && r[h] != null ? c ? (l = !0, u[h] = r[h]) : this[h] = r[h] : s && n[h] != null && (c ? (l = !0, u[h] = n[h]) : this[h] = n[h]);
      }
      if (!a)
        for (var f = 0; f < this.animators.length; f++) {
          var v = this.animators[f], d = v.targetName;
          v.getLoop() || v.__changeFinalValue(d ? (r || n)[d] : r || n);
        }
      l && this._transitionState(e, u, o);
    }
  }, t.prototype._attachComponent = function(e) {
    if (!(e.__zr && !e.__hostTarget) && e !== this) {
      var r = this.__zr;
      r && e.addSelfToZr(r), e.__zr = r, e.__hostTarget = this;
    }
  }, t.prototype._detachComponent = function(e) {
    e.__zr && e.removeSelfFromZr(e.__zr), e.__zr = null, e.__hostTarget = null;
  }, t.prototype.getClipPath = function() {
    return this._clipPath;
  }, t.prototype.setClipPath = function(e) {
    this._clipPath && this._clipPath !== e && this.removeClipPath(), this._attachComponent(e), this._clipPath = e, this.markRedraw();
  }, t.prototype.removeClipPath = function() {
    var e = this._clipPath;
    e && (this._detachComponent(e), this._clipPath = null, this.markRedraw());
  }, t.prototype.getTextContent = function() {
    return this._textContent;
  }, t.prototype.setTextContent = function(e) {
    var r = this._textContent;
    r !== e && (r && r !== e && this.removeTextContent(), e.innerTransformable = new Ra(), this._attachComponent(e), this._textContent = e, this.markRedraw());
  }, t.prototype.setTextConfig = function(e) {
    this.textConfig || (this.textConfig = {}), E(this.textConfig, e), this.markRedraw();
  }, t.prototype.removeTextConfig = function() {
    this.textConfig = null, this.markRedraw();
  }, t.prototype.removeTextContent = function() {
    var e = this._textContent;
    e && (e.innerTransformable = null, this._detachComponent(e), this._textContent = null, this._innerTextDefaultStyle = null, this.markRedraw());
  }, t.prototype.getTextGuideLine = function() {
    return this._textGuide;
  }, t.prototype.setTextGuideLine = function(e) {
    this._textGuide && this._textGuide !== e && this.removeTextGuideLine(), this._attachComponent(e), this._textGuide = e, this.markRedraw();
  }, t.prototype.removeTextGuideLine = function() {
    var e = this._textGuide;
    e && (this._detachComponent(e), this._textGuide = null, this.markRedraw());
  }, t.prototype.markRedraw = function() {
    this.__dirty |= Je;
    var e = this.__zr;
    e && (this.__inHover ? e.refreshHover() : e.refresh()), this.__hostTarget && this.__hostTarget.markRedraw();
  }, t.prototype.dirty = function() {
    this.markRedraw();
  }, t.prototype.addSelfToZr = function(e) {
    if (this.__zr !== e) {
      this.__zr = e;
      var r = this.animators;
      if (r)
        for (var n = 0; n < r.length; n++)
          e.animation.addAnimator(r[n]);
      this._clipPath && this._clipPath.addSelfToZr(e), this._textContent && this._textContent.addSelfToZr(e), this._textGuide && this._textGuide.addSelfToZr(e);
    }
  }, t.prototype.removeSelfFromZr = function(e) {
    if (this.__zr) {
      this.__zr = null;
      var r = this.animators;
      if (r)
        for (var n = 0; n < r.length; n++)
          e.animation.removeAnimator(r[n]);
      this._clipPath && this._clipPath.removeSelfFromZr(e), this._textContent && this._textContent.removeSelfFromZr(e), this._textGuide && this._textGuide.removeSelfFromZr(e);
    }
  }, t.prototype.animate = function(e, r, n) {
    var i = e ? this[e] : this, a = new yh(i, r, n);
    return e && (a.targetName = e), this.addAnimator(a, e), a;
  }, t.prototype.addAnimator = function(e, r) {
    var n = this.__zr, i = this;
    e.during(function() {
      i.updateDuringAnimation(r);
    }).done(function() {
      var a = i.animators, o = oe(a, e);
      o >= 0 && a.splice(o, 1);
    }), this.animators.push(e), n && n.animation.addAnimator(e), n && n.wakeUp();
  }, t.prototype.updateDuringAnimation = function(e) {
    this.markRedraw();
  }, t.prototype.stopAnimation = function(e, r) {
    for (var n = this.animators, i = n.length, a = [], o = 0; o < i; o++) {
      var s = n[o];
      !e || e === s.scope ? s.stop(r) : a.push(s);
    }
    return this.animators = a, this;
  }, t.prototype.animateTo = function(e, r, n) {
    Yu(this, e, r, n);
  }, t.prototype.animateFrom = function(e, r, n) {
    Yu(this, e, r, n, !0);
  }, t.prototype._transitionState = function(e, r, n, i) {
    for (var a = Yu(this, r, n, i), o = 0; o < a.length; o++)
      a[o].__fromStateTransition = e;
  }, t.prototype.getBoundingRect = function() {
    return null;
  }, t.prototype.getPaintRect = function() {
    return null;
  }, t.initDefaultProps = (function() {
    var e = t.prototype;
    e.type = "element", e.name = "", e.ignore = e.silent = e.ignoreHostSilent = e.isGroup = e.draggable = e.dragging = e.ignoreClip = !1, e.__inHover = Ho, e.__dirty = Je;
    function r(n, i, a, o) {
      Object.defineProperty(e, n, {
        get: function() {
          if (!this[i]) {
            var u = this[i] = [];
            s(this, u);
          }
          return this[i];
        },
        set: function(u) {
          this[a] = u[0], this[o] = u[1], this[i] = u, s(this, u);
        }
      });
      function s(u, l) {
        Object.defineProperty(l, 0, {
          get: function() {
            return u[a];
          },
          set: function(f) {
            u[a] = f;
          }
        }), Object.defineProperty(l, 1, {
          get: function() {
            return u[o];
          },
          set: function(f) {
            u[o] = f;
          }
        });
      }
    }
    Object.defineProperty && (r("position", "_legacyPos", "x", "y"), r("scale", "_legacyScale", "scaleX", "scaleY"), r("origin", "_legacyOrigin", "originX", "originY"));
  })(), t;
})();
Lt(Ws, Kt);
Lt(Ws, Ra);
function Yu(t, e, r, n, i) {
  r = r || {};
  var a = [];
  cy(t, "", t, e, r, n, a, i);
  var o = a.length, s = !1, u = r.done, l = r.aborted, f = function() {
    s = !0, o--, o <= 0 && (s ? u && u() : l && l());
  }, h = function() {
    o--, o <= 0 && (s ? u && u() : l && l());
  };
  o || u && u(), a.length > 0 && r.during && a[0].during(function(d, y) {
    r.during(y);
  });
  for (var c = 0; c < a.length; c++) {
    var v = a[c];
    f && v.done(f), h && v.aborted(h), r.force && v.duration(r.duration), v.start(r.easing);
  }
  return a;
}
function Wu(t, e, r) {
  for (var n = 0; n < r; n++)
    t[n] = e[n];
}
function xb(t) {
  return Ze(t[0]);
}
function Mb(t, e, r) {
  if (Ze(e[r]))
    if (Ze(t[r]) || (t[r] = []), Ke(e[r])) {
      var n = e[r].length;
      t[r].length !== n && (t[r] = new e[r].constructor(n), Wu(t[r], e[r], n));
    } else {
      var i = e[r], a = t[r], o = i.length;
      if (xb(i))
        for (var s = i[0].length, u = 0; u < o; u++)
          a[u] ? Wu(a[u], i[u], s) : a[u] = Array.prototype.slice.call(i[u]);
      else
        Wu(a, i, o);
      a.length = i.length;
    }
  else
    t[r] = e[r];
}
function Db(t, e) {
  return t === e || Ze(t) && Ze(e) && Ib(t, e);
}
function Ib(t, e) {
  var r = t.length;
  if (r !== e.length)
    return !1;
  for (var n = 0; n < r; n++)
    if (t[n] !== e[n])
      return !1;
  return !0;
}
function cy(t, e, r, n, i, a, o, s) {
  for (var u = me(n), l = i.duration, f = i.delay, h = i.additive, c = i.setToFinal, v = !z(a), d = t.animators, y = [], p = 0; p < u.length; p++) {
    var g = u[p], m = n[g];
    if (m != null && r[g] != null && (v || a[g]))
      if (z(m) && !Ze(m) && !Aa(m)) {
        if (e) {
          s || (r[g] = m, t.updateDuringAnimation(e));
          continue;
        }
        cy(t, g, r[g], m, i, a && a[g], o, s);
      } else
        y.push(g);
    else s || (r[g] = m, t.updateDuringAnimation(e), y.push(g));
  }
  var _ = y.length;
  if (!h && _)
    for (var S = 0; S < d.length; S++) {
      var b = d[S];
      if (b.targetName === e) {
        var T = b.stopTracks(y);
        if (T) {
          var C = oe(d, b);
          d.splice(C, 1);
        }
      }
    }
  if (i.force || (y = xe(y, function(I) {
    return !Db(n[I], r[I]);
  }), _ = y.length), _ > 0 || i.force && !o.length) {
    var M = void 0, x = void 0, D = void 0;
    if (s) {
      x = {}, c && (M = {});
      for (var S = 0; S < _; S++) {
        var g = y[S];
        x[g] = r[g], c ? M[g] = n[g] : r[g] = n[g];
      }
    } else if (c) {
      D = {};
      for (var S = 0; S < _; S++) {
        var g = y[S];
        D[g] = Bo(r[g]), Mb(r, n, g);
      }
    }
    var b = new yh(r, !1, !1, h ? xe(d, function(L) {
      return L.targetName === e;
    }) : null);
    b.targetName = e, i.scope && (b.scope = i.scope), c && M && b.whenWithKeys(0, M, y), D && b.whenWithKeys(0, D, y), b.whenWithKeys(l ?? 500, s ? x : n, y).delay(f || 0), t.addAnimator(b, e), o.push(b);
  }
}
function Cv(t, e, r, n) {
  return !(r && r.hoverLayer || n) || xv(t) || e && xv(e) ? Ho : Ys;
}
function xv(t) {
  return t.type === "text" || t.type === "tspan";
}
function Mv(t, e, r) {
  return !e && !t.__inHover && r && r.duration > 0;
}
var ut = (function(t) {
  F(e, t);
  function e(r) {
    var n = t.call(this) || this;
    return n.isGroup = !0, n._children = [], n.attr(r), n;
  }
  return e.prototype.childrenRef = function() {
    return this._children;
  }, e.prototype.children = function() {
    return this._children.slice();
  }, e.prototype.childAt = function(r) {
    return this._children[r];
  }, e.prototype.childOfName = function(r) {
    for (var n = this._children, i = 0; i < n.length; i++)
      if (n[i].name === r)
        return n[i];
  }, e.prototype.childCount = function() {
    return this._children.length;
  }, e.prototype.add = function(r) {
    return r && r !== this && r.parent !== this && (this._children.push(r), this._doAdd(r)), this;
  }, e.prototype.addBefore = function(r, n) {
    if (r && r !== this && r.parent !== this && n && n.parent === this) {
      var i = this._children, a = i.indexOf(n);
      a >= 0 && (i.splice(a, 0, r), this._doAdd(r));
    }
    return this;
  }, e.prototype.replace = function(r, n) {
    var i = oe(this._children, r);
    return i >= 0 && this.replaceAt(n, i), this;
  }, e.prototype.replaceAt = function(r, n) {
    var i = this._children, a = i[n];
    if (r && r !== this && r.parent !== this && r !== a) {
      i[n] = r, a.parent = null;
      var o = this.__zr;
      o && a.removeSelfFromZr(o), this._doAdd(r);
    }
    return this;
  }, e.prototype._doAdd = function(r) {
    r.parent && r.parent.remove(r), r.parent = this;
    var n = this.__zr;
    n && n !== r.__zr && r.addSelfToZr(n), n && n.refresh();
  }, e.prototype.remove = function(r) {
    var n = this.__zr, i = this._children, a = oe(i, r);
    return a < 0 ? this : (i.splice(a, 1), r.parent = null, n && r.removeSelfFromZr(n), n && n.refresh(), this);
  }, e.prototype.removeAll = function() {
    for (var r = this._children, n = this.__zr, i = 0; i < r.length; i++) {
      var a = r[i];
      n && a.removeSelfFromZr(n), a.parent = null;
    }
    return r.length = 0, this;
  }, e.prototype.eachChild = function(r, n) {
    for (var i = this._children, a = 0; a < i.length; a++) {
      var o = i[a];
      r.call(n, o, a);
    }
    return this;
  }, e.prototype.traverse = function(r, n) {
    for (var i = 0; i < this._children.length; i++) {
      var a = this._children[i], o = r.call(n, a);
      a.isGroup && !o && a.traverse(r, n);
    }
    return this;
  }, e.prototype.addSelfToZr = function(r) {
    t.prototype.addSelfToZr.call(this, r);
    for (var n = 0; n < this._children.length; n++) {
      var i = this._children[n];
      i.addSelfToZr(r);
    }
  }, e.prototype.removeSelfFromZr = function(r) {
    t.prototype.removeSelfFromZr.call(this, r);
    for (var n = 0; n < this._children.length; n++) {
      var i = this._children[n];
      i.removeSelfFromZr(r);
    }
  }, e.prototype.getBoundingRect = function(r) {
    for (var n = new Y(0, 0, 0, 0), i = r || this._children, a = [], o = null, s = 0; s < i.length; s++) {
      var u = i[s];
      if (!(u.ignore || u.invisible)) {
        var l = u.getBoundingRect(), f = u.getLocalTransform(a);
        f ? (Y.applyTransform(n, l, f), o = o || n.clone(), o.union(n)) : (o = o || l.clone(), o.union(l));
      }
    }
    return o || n;
  }, e;
})(Ws);
ut.prototype.type = "group";
/*!
* ZRender, a high performance 2d drawing library.
*
* Copyright (c) 2013, Baidu Inc.
* All rights reserved.
*
* LICENSE
* https://github.com/ecomfe/zrender/blob/master/LICENSE
*/
var Vo = {}, cn = {};
function Ab(t) {
  delete cn[t];
}
function Pb(t) {
  if (!t)
    return !1;
  if (typeof t == "string")
    return pa(t, 1) < ff;
  if (t.colorStops) {
    for (var e = t.colorStops, r = 0, n = e.length, i = 0; i < n; i++)
      r += pa(e[i].color, 1);
    return r /= n, r < ff;
  }
  return !1;
}
var Lb = (function() {
  function t(e, r, n) {
    var i = this;
    this._sleepAfterStill = 10, this._stillFrameAccum = 0, this._needsRefresh = !0, this._needsRefreshHover = !1, this._darkMode = !1, n = n || {}, this.dom = r, this.id = e;
    var a = new G1(), o = n.renderer || "canvas";
    Vo[o] || (o = me(Vo)[0]), n.useDirtyRect = n.useDirtyRect == null ? !1 : n.useDirtyRect;
    var s = new Vo[o](r, a, n, e), u = n.ssr || s.ssrOnly;
    this.storage = a, this.painter = s;
    var l = !W.node && !W.worker && !u ? new _b(s.getViewportRoot(), s.root) : null, f = n.useCoarsePointer, h = f == null || f === "auto" ? W.touchEventsSupported : !!f, c = 44, v;
    h && (v = G(n.pointerSize, c)), this.handler = new qg(a, s, l, s.root, v), this.animation = new cb({
      stage: {
        update: u ? null : function() {
          return i._flush(!1);
        }
      }
    }), u || this.animation.start();
  }
  return t.prototype.add = function(e) {
    this._disposed || !e || (this.storage.addRoot(e), e.addSelfToZr(this), this.refresh());
  }, t.prototype.remove = function(e) {
    this._disposed || !e || (this.storage.delRoot(e), e.removeSelfFromZr(this), this.refresh());
  }, t.prototype.configLayer = function(e, r) {
    this._disposed || (this.painter.configLayer && this.painter.configLayer(e, r), this.refresh());
  }, t.prototype.setBackgroundColor = function(e) {
    this._disposed || (this.painter.setBackgroundColor && this.painter.setBackgroundColor(e), this.refresh(), this._backgroundColor = e, this._darkMode = Pb(e));
  }, t.prototype.getBackgroundColor = function() {
    return this._backgroundColor;
  }, t.prototype.setDarkMode = function(e) {
    this._darkMode = e;
  }, t.prototype.isDarkMode = function() {
    return this._darkMode;
  }, t.prototype.refreshImmediately = function(e) {
    this._disposed || this._refresh({
      animUpdate: !e,
      refresh: !0,
      refreshHover: !1
    });
  }, t.prototype._refresh = function(e) {
    e.animUpdate && this.animation.update(!0), this._needsRefresh = this._needsRefreshHover = !1, this.painter.refresh({
      refresh: e.refresh,
      refreshHover: e.refreshHover
    }), this._needsRefresh = this._needsRefreshHover = !1;
  }, t.prototype.refresh = function() {
    this._disposed || (this._needsRefresh = !0, this.animation.start());
  }, t.prototype.flush = function() {
    this._disposed || this._flush(!0);
  }, t.prototype._flush = function(e) {
    var r, n = jn(), i = this._needsRefresh, a = this._needsRefreshHover;
    (i || a) && (r = !0, this._refresh({
      animUpdate: e,
      refresh: i,
      refreshHover: a
    }));
    var o = jn();
    r ? (this._stillFrameAccum = 0, this.trigger("rendered", {
      elapsedTime: o - n
    })) : this._sleepAfterStill > 0 && (this._stillFrameAccum++, this._stillFrameAccum > this._sleepAfterStill && this.animation.stop());
  }, t.prototype.setSleepAfterStill = function(e) {
    this._sleepAfterStill = e;
  }, t.prototype.wakeUp = function() {
    this._disposed || (this.animation.start(), this._stillFrameAccum = 0);
  }, t.prototype.refreshHover = function() {
    this._needsRefreshHover = !0;
  }, t.prototype.refreshHoverImmediately = function() {
    this._disposed || this._refresh({
      animUpdate: !1,
      refresh: !1,
      refreshHover: !0
    });
  }, t.prototype.resize = function(e) {
    this._disposed || (e = e || {}, this.painter.resize(e.width, e.height), this.handler.resize());
  }, t.prototype.clearAnimation = function() {
    this._disposed || this.animation.clear();
  }, t.prototype.getWidth = function() {
    if (!this._disposed)
      return this.painter.getWidth();
  }, t.prototype.getHeight = function() {
    if (!this._disposed)
      return this.painter.getHeight();
  }, t.prototype.setCursorStyle = function(e) {
    this._disposed || this.handler.setCursorStyle(e);
  }, t.prototype.findHover = function(e, r) {
    if (!this._disposed)
      return this.handler.findHover(e, r);
  }, t.prototype.on = function(e, r, n) {
    return this._disposed || this.handler.on(e, r, n), this;
  }, t.prototype.off = function(e, r) {
    this._disposed || this.handler.off(e, r);
  }, t.prototype.trigger = function(e, r) {
    this._disposed || this.handler.trigger(e, r);
  }, t.prototype.clear = function() {
    if (!this._disposed) {
      for (var e = this.storage.getRoots(), r = 0; r < e.length; r++)
        e[r] instanceof ut && e[r].removeSelfFromZr(this);
      this.storage.delAllRoots(), this.painter.clear();
    }
  }, t.prototype.dispose = function() {
    this._disposed || (this.animation.stop(), this.clear(), this.storage.dispose(), this.painter.dispose(), this.handler.dispose(), this.animation = this.storage = this.painter = this.handler = null, this._disposed = !0, Ab(this.id));
  }, t;
})();
function vf(t, e) {
  var r = new Lb(hh(), t, e);
  return cn[r.id] = r, r;
}
function Rb(t) {
  t.dispose();
}
function Eb() {
  for (var t in cn)
    cn.hasOwnProperty(t) && cn[t].dispose();
  cn = {};
}
function Ob(t) {
  return cn[t];
}
function vy(t, e) {
  Vo[t] = e;
}
var df;
function kb(t) {
  if (typeof df == "function")
    return df(t);
}
function dy(t) {
  df = t;
}
var Bb = "6.1.0";
const Nb = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  dispose: Rb,
  disposeAll: Eb,
  getElementSSRData: kb,
  getInstance: Ob,
  init: vf,
  registerPainter: vy,
  registerSSRDataGetter: dy,
  version: Bb
}, Symbol.toStringTag, { value: "Module" }));
var Dv = 1e-4, ls = 20;
function Fb(t) {
  return t.replace(/^\s+|\s+$/g, "");
}
var et = Math.min, ge = Math.max, Fe = Math.abs, hr = Math.round, Dn = Math.floor, Xs = Math.ceil, In = Math.pow, wn = Math.log, ga = Math.LN10, zb = Math.PI, Hb = Math.random;
function pf(t, e, r, n) {
  var i = e[0], a = e[1], o = r[0], s = r[1], u = a - i, l = s - o;
  if (u === 0)
    return l === 0 ? o : (o + s) / 2;
  if (n)
    if (u > 0) {
      if (t <= i)
        return o;
      if (t >= a)
        return s;
    } else {
      if (t >= i)
        return o;
      if (t <= a)
        return s;
    }
  else {
    if (t === i)
      return o;
    if (t === a)
      return s;
  }
  return (t - i) / u * l + o;
}
var Dt = Vb;
function Vb(t, e, r) {
  switch (t) {
    case "center":
    case "middle":
      t = "50%";
      break;
    case "left":
    case "top":
      t = "0%";
      break;
    case "right":
    case "bottom":
      t = "100%";
      break;
  }
  return Gb(t, e, r);
}
function Gb(t, e, r) {
  return N(t) ? Ub(t) ? parseFloat(t) / 100 * e + (r || 0) : parseFloat(t) : t == null ? NaN : +t;
}
function Ub(t) {
  return !!Fb(t).match(/%$/);
}
function Ie(t, e, r) {
  return isNaN(e) ? r ? "" + t : +t : (e = et(ge(0, e), ls), t = (+t).toFixed(e), r ? t : +t);
}
function Yb(t, e, r) {
  return e == null && (e = 10), Ie(t, e, r);
}
function mh(t) {
  return t.sort(function(e, r) {
    return e - r;
  }), t;
}
function lr(t) {
  if (t = +t, isNaN(t))
    return 0;
  if (t > 1e-14) {
    for (var e = 1, r = 0; r < 15; r++, e *= 10)
      if (hr(t * e) / e === t)
        return r;
  }
  return py(t);
}
function py(t) {
  var e = t.toString().toLowerCase(), r = e.indexOf("e"), n = r > 0 ? +e.slice(r + 1) : 0, i = r > 0 ? r : e.length, a = e.indexOf("."), o = a < 0 ? 0 : i - 1 - a;
  return ge(0, o - n);
}
function Wb(t, e) {
  var r = Dn(wn(t[1] - t[0]) / ga), n = hr(wn(Fe(e[1] - e[0])) / ga), i = et(ge(-r + n, 0), ls);
  return isFinite(i) ? i : ls;
}
function MR(t, e, r) {
  var n = Fe(t[1] - t[0]);
  if (!isFinite(n) || n === 0)
    return NaN;
  var i = wn(2 * Fe(r || 1) * Fe(n)) / ga, a = wn(Fe(e)) / ga, o = ge(0, Xs(-i + a));
  return isFinite(o) || (o = NaN), o;
}
function Xb(t, e, r) {
  if (!t[e])
    return 0;
  var n = qb(t, r);
  return n[e] || 0;
}
function qb(t, e) {
  var r = Xt(t, function(v, d) {
    return v + (isNaN(d) ? 0 : d);
  }, 0);
  if (r === 0)
    return [];
  for (var n = In(10, e), i = H(t, function(v) {
    return (isNaN(v) ? 0 : v) / r * n * 100;
  }), a = n * 100, o = H(i, function(v) {
    return Dn(v);
  }), s = Xt(o, function(v, d) {
    return v + d;
  }, 0), u = H(i, function(v, d) {
    return v - o[d];
  }); s < a; ) {
    for (var l = Number.NEGATIVE_INFINITY, f = null, h = 0, c = u.length; h < c; ++h)
      u[h] > l && (l = u[h], f = h);
    ++o[f], u[f] = 0, ++s;
  }
  return H(o, function(v) {
    return v / n;
  });
}
function $b(t, e) {
  var r = ge(lr(t), lr(e)), n = t + e;
  return r > ls ? n : Ie(n, r);
}
var Zb = In(2, 53) - 1;
function _h(t) {
  var e = zb * 2;
  return (t % e + e) % e;
}
function ya(t) {
  return t > -Dv && t < Dv;
}
var Kb = /^(?:(\d{4})(?:[-\/](\d{1,2})(?:[-\/](\d{1,2})(?:[T ](\d{1,2})(?::(\d{1,2})(?::(\d{1,2})(?:[.,](\d+))?)?)?(Z|[\+\-]\d\d:?\d\d)?)?)?)?)?$/;
function pr(t) {
  if (t instanceof Date)
    return t;
  if (N(t)) {
    var e = Kb.exec(t);
    if (!e)
      return /* @__PURE__ */ new Date(NaN);
    if (e[8]) {
      var r = +e[4] || 0;
      return e[8].toUpperCase() !== "Z" && (r -= +e[8].slice(0, 3)), new Date(Date.UTC(+e[1], +(e[2] || 1) - 1, +e[3] || 1, r, +(e[5] || 0), +e[6] || 0, e[7] ? +e[7].substring(0, 3) : 0));
    } else
      return new Date(+e[1], +(e[2] || 1) - 1, +e[3] || 1, +e[4] || 0, +(e[5] || 0), +e[6] || 0, e[7] ? +e[7].substring(0, 3) : 0);
  } else if (t == null)
    return /* @__PURE__ */ new Date(NaN);
  return new Date(hr(t));
}
function gy(t) {
  return In(10, qs(t));
}
function qs(t) {
  if (t === 0)
    return 0;
  var e = Dn(wn(t) / ga);
  return t / In(10, e) >= 10 && e++, e;
}
var jb = 2;
function Sh(t, e) {
  var r = qs(t), n = In(10, r), i = t / n, a;
  return e === jb ? a = 1 : e ? i < 1.5 ? a = 1 : i < 2.5 ? a = 2 : i < 4 ? a = 3 : i < 7 ? a = 5 : a = 10 : i < 1 ? a = 1 : i < 2 ? a = 2 : i < 3 ? a = 3 : i < 5 ? a = 5 : a = 10, t = a * n, Ie(t, -r);
}
function Qb(t, e) {
  var r = (t.length - 1) * e + 1, n = Dn(r), i = +t[n - 1], a = r - n;
  return a ? i + a * (t[n] - i) : i;
}
function Jb(t) {
  t.sort(function(u, l) {
    return s(u, l, 0) ? -1 : 1;
  });
  for (var e = -1 / 0, r = 1, n = 0; n < t.length; ) {
    for (var i = t[n].interval, a = t[n].close, o = 0; o < 2; o++)
      i[o] <= e && (i[o] = e, a[o] = o ? 1 : 1 - r), e = i[o], r = a[o];
    i[0] === i[1] && a[0] * a[1] !== 1 ? t.splice(n, 1) : n++;
  }
  return t;
  function s(u, l, f) {
    return u.interval[f] < l.interval[f] || u.interval[f] === l.interval[f] && (u.close[f] - l.close[f] === (f ? -1 : 1) || !f && s(u, l, 1));
  }
}
function cr(t) {
  var e = parseFloat(t);
  return e == t && (e !== 0 || !N(t) || t.indexOf("x") <= 0) ? e : NaN;
}
function yy(t) {
  return !isNaN(cr(t));
}
function bh() {
  return hr(Hb() * 9);
}
function my(t, e) {
  return e === 0 ? t : my(e, t % e);
}
function Iv(t, e) {
  return t == null ? e : e == null ? t : t * e / my(t, e);
}
function vr(t) {
  return t != null && isFinite(t);
}
var eT = "[ECharts] ", tT = typeof console < "u" && console.warn && console.log;
function rT(t, e, r) {
  tT && console[t](eT + e);
}
function _y(t, e) {
  rT("error", t);
}
function Ye(t) {
  throw new Error(t);
}
function Av(t, e, r) {
  return (e - t) * r + t;
}
var Sy = "series\0", nT = "\0_ec_\0";
function $e(t) {
  return t instanceof Array ? t : t == null ? [] : [t];
}
function Pv(t, e, r) {
  if (t) {
    t[e] = t[e] || {}, t.emphasis = t.emphasis || {}, t.emphasis[e] = t.emphasis[e] || {};
    for (var n = 0, i = r.length; n < i; n++) {
      var a = r[n];
      !t.emphasis[e].hasOwnProperty(a) && t[e].hasOwnProperty(a) && (t.emphasis[e][a] = t[e][a]);
    }
  }
}
var Lv = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "rich", "tag", "color", "textBorderColor", "textBorderWidth", "width", "height", "lineHeight", "align", "verticalAlign", "baseline", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY", "backgroundColor", "borderColor", "borderWidth", "borderRadius", "padding"];
function Ea(t) {
  return z(t) && !B(t) && !(t instanceof Date) ? t.value : t;
}
function iT(t) {
  return z(t) && !(t instanceof Array);
}
function aT(t, e, r) {
  var n = r === "normalMerge", i = r === "replaceMerge", a = r === "replaceAll";
  t = t || [], e = (e || []).slice();
  var o = U();
  w(e, function(u, l) {
    if (!z(u)) {
      e[l] = null;
      return;
    }
  });
  var s = oT(t, o, r);
  return (n || i) && sT(s, t, o, e), n && uT(s, e), n || i ? lT(s, e, i) : a && fT(s, e), hT(s), s;
}
function oT(t, e, r) {
  var n = [];
  if (r === "replaceAll")
    return n;
  for (var i = 0; i < t.length; i++) {
    var a = t[i];
    a && a.id != null && e.set(a.id, i), n.push({
      existing: r === "replaceMerge" || ma(a) ? null : a,
      newOption: null,
      keyInfo: null,
      brandNew: null
    });
  }
  return n;
}
function sT(t, e, r, n) {
  w(n, function(i, a) {
    if (!(!i || i.id == null)) {
      var o = ta(i.id), s = r.get(o);
      if (s != null) {
        var u = t[s];
        qt(!u.newOption, 'Duplicated option on id "' + o + '".'), u.newOption = i, u.existing = e[s], n[a] = null;
      }
    }
  });
}
function uT(t, e) {
  w(e, function(r, n) {
    if (!(!r || r.name == null))
      for (var i = 0; i < t.length; i++) {
        var a = t[i].existing;
        if (!t[i].newOption && a && (a.id == null || r.id == null) && !ma(r) && !ma(a) && by("name", a, r)) {
          t[i].newOption = r, e[n] = null;
          return;
        }
      }
  });
}
function lT(t, e, r) {
  w(e, function(n) {
    if (n) {
      for (
        var i, a = 0;
        // Be `!resultItem` only when `nextIdx >= result.length`.
        (i = t[a]) && (i.newOption || ma(i.existing) || // In mode "replaceMerge", here no not-mapped-non-internal-existing.
        i.existing && n.id != null && !by("id", n, i.existing));
      )
        a++;
      i ? (i.newOption = n, i.brandNew = r) : t.push({
        newOption: n,
        brandNew: r,
        existing: null,
        keyInfo: null
      }), a++;
    }
  });
}
function fT(t, e) {
  w(e, function(r) {
    t.push({
      newOption: r,
      brandNew: !0,
      existing: null,
      keyInfo: null
    });
  });
}
function hT(t) {
  var e = U();
  w(t, function(r) {
    var n = r.existing;
    n && e.set(n.id, r);
  }), w(t, function(r) {
    var n = r.newOption;
    qt(!n || n.id == null || !e.get(n.id) || e.get(n.id) === r, "id duplicates: " + (n && n.id)), n && n.id != null && e.set(n.id, r), !r.keyInfo && (r.keyInfo = {});
  }), w(t, function(r, n) {
    var i = r.existing, a = r.newOption, o = r.keyInfo;
    if (z(a)) {
      if (o.name = a.name != null ? ta(a.name) : i ? i.name : Sy + n, i)
        o.id = ta(i.id);
      else if (a.id != null)
        o.id = ta(a.id);
      else {
        var s = 0;
        do
          o.id = "\0" + o.name + "\0" + s++;
        while (e.get(o.id));
      }
      e.set(o.id, r);
    }
  });
}
function by(t, e, r) {
  var n = Yt(e[t], null), i = Yt(r[t], null);
  return n != null && i != null && n === i;
}
function ta(t) {
  return Yt(t, "");
}
function Yt(t, e) {
  return t == null ? e : N(t) ? t : ve(t) || Qo(t) ? t + "" : e;
}
function Th(t) {
  var e = t.name;
  return !!(e && e.indexOf(Sy));
}
function ma(t) {
  return t && t.id != null && ta(t.id).indexOf(nT) === 0;
}
function cT(t, e, r) {
  w(t, function(n) {
    var i = n.newOption;
    z(i) && (n.keyInfo.mainType = e, n.keyInfo.subType = vT(e, i, n.existing, r));
  });
}
function vT(t, e, r, n) {
  var i = e.type ? e.type : r ? r.subType : n.determineSubType(t, e);
  return i;
}
function Oa(t, e) {
  if (e.dataIndexInside != null)
    return e.dataIndexInside;
  if (e.dataIndex != null)
    return B(e.dataIndex) ? H(e.dataIndex, function(r) {
      return t.indexOfRawIndex(r);
    }) : t.indexOfRawIndex(e.dataIndex);
  if (e.name != null)
    return B(e.name) ? H(e.name, function(r) {
      return t.indexOfName(r);
    }) : t.indexOfName(e.name);
}
function de() {
  var t = "__ec_inner_" + dT++;
  return function(e) {
    return e[t] || (e[t] = {});
  };
}
var dT = bh();
function Xu(t, e, r) {
  var n = wh(e, r), i = n.mainTypeSpecified, a = n.queryOptionMap, o = n.others, s = o, u = r ? r.defaultMainType : null;
  return !i && u && a.set(u, {}), a.each(function(l, f) {
    var h = ka(t, f, l, {
      useDefault: u === f,
      enableAll: r && r.enableAll != null ? r.enableAll : !0,
      enableNone: r && r.enableNone != null ? r.enableNone : !0
    });
    s[f + "Models"] = h.models, s[f + "Model"] = h.models[0];
  }), s;
}
function wh(t, e) {
  var r;
  if (N(t)) {
    var n = {};
    n[t + "Index"] = 0, r = n;
  } else
    r = t;
  var i = U(), a = {}, o = !1;
  return w(r, function(s, u) {
    if (u === "dataIndex" || u === "dataIndexInside") {
      a[u] = s;
      return;
    }
    var l = u.match(/^(\w+)(Index|Id|Name)$/) || [], f = l[1], h = (l[2] || "").toLowerCase();
    if (!(!f || !h || e && e.includeMainTypes && oe(e.includeMainTypes, f) < 0)) {
      o = o || !!f;
      var c = i.get(f) || i.set(f, {});
      c[h] = s;
    }
  }), {
    mainTypeSpecified: o,
    queryOptionMap: i,
    others: a
  };
}
var ur = {
  useDefault: !0,
  enableAll: !1,
  enableNone: !1
};
function ka(t, e, r, n) {
  n = n || ur;
  var i = r.index, a = r.id, o = r.name, s = {
    models: null,
    specified: i != null || a != null || o != null
  };
  if (!s.specified) {
    var u = void 0;
    return s.models = n.useDefault && (u = t.getComponent(e)) ? [u] : [], s;
  }
  if (i === "none" || i === !1) {
    if (n.enableNone)
      return s.models = [], s;
    i = -1;
  }
  return i === "all" && (n.enableAll ? i = a = o = null : i = -1), s.models = t.queryComponents({
    mainType: e,
    index: i,
    id: a,
    name: o
  }), s;
}
function pT(t, e, r) {
  var n = {};
  n[e + "Id"] = t[e + "Id"], n[e + "Index"] = t[e + "Index"], n[e + "Name"] = t[e + "Name"];
  var i = {
    mainType: e,
    query: n
  };
  return r && (i.subType = r), i;
}
function Ty(t, e, r) {
  t.setAttribute ? t.setAttribute(e, r) : t[e] = r;
}
function gT(t, e) {
  return t.getAttribute ? t.getAttribute(e) : t[e];
}
function yT(t) {
  return t === "auto" ? W.domSupported ? "html" : "richText" : t || "html";
}
function mT(t, e, r, n, i) {
  var a = e == null || e === "auto";
  if (n == null)
    return n;
  if (ve(n)) {
    var o = Av(r || 0, n, i);
    return Ie(o, a ? Math.max(lr(r || 0), lr(n)) : e);
  } else {
    if (N(n))
      return i < 1 ? r : n;
    for (var s = [], u = r, l = n, f = Math.max(u ? u.length : 0, l.length), h = 0; h < f; ++h) {
      var c = t.getDimensionInfo(h);
      if (c && c.type === "ordinal")
        s[h] = (i < 1 && u ? u : l)[h];
      else {
        var v = u && u[h] ? u[h] : 0, d = l[h], o = Av(v, d, i);
        s[h] = Ie(o, a ? Math.max(lr(v), lr(d)) : e);
      }
    }
    return s;
  }
}
function zt() {
  return [1 / 0, -1 / 0];
}
function _T(t, e) {
  dr(e) && (e < t[0] && (t[0] = e), e > t[1] && (t[1] = e));
}
function wy(t, e) {
  dr(e) && e < t[0] && (t[0] = e);
}
function Cy(t, e) {
  dr(e) && e > t[1] && (t[1] = e);
}
function ST(t, e) {
  hi(e[0], e[1]) && (e[0] < t[0] && (t[0] = e[0]), e[1] > t[1] && (t[1] = e[1]));
}
function dr(t) {
  return t != null && isFinite(t);
}
function hi(t, e) {
  return dr(t) && dr(e) && t <= e;
}
function bT(t) {
  var e = t[1] - t[0];
  return isFinite(e) && e >= 0;
}
function TT(t) {
  hi(t[0], t[1]) && t[0] > t[1] && (t[0] = t[1]);
}
function wT() {
  var t = "__ec_once_" + CT++;
  return function(e, r) {
    qe(e, t) || (e[t] = 1, r());
  };
}
var CT = bh();
function Ch(t, e, r) {
  var n = U(), i = 0;
  w(t, function(a) {
    var o = e(a), s = n.get(o) || 0;
    r && r(a, s), !s && !r && (t[i++] = a), n.set(o, s + 1);
  }), r || (t.length = i);
}
function xT(t) {
  return t.value + "";
}
function MT(t) {
  return t + "";
}
function DR(t, e) {
  return G(e, !0) ? t.seriesIndex + 2 : 0;
}
function DT(t, e, r) {
  var n = t.getData().count();
  return {
    progressiveRender: r.progressiveEnabled && e.incrementalPrepareRender && n >= r.threshold,
    large: t.get("large") && n >= t.get("largeThreshold"),
    // TODO: modDataCount should not updated if `appendData`, otherwise cause whole repaint.
    // see `test/candlestick-large3.html`
    modDataCount: t.get("progressiveChunkMode") === "mod" ? t.getData().count() : null
  };
}
function IR(t, e) {
  return {
    seriesType: t,
    overallReset: e
  };
}
function xh(t) {
  return {
    overallReset: t
  };
}
var IT = ".", Xr = "___EC__COMPONENT__CONTAINER___", xy = "___EC__EXTENDED_CLASS___";
function Vt(t) {
  var e = {
    main: "",
    sub: ""
  };
  if (t) {
    var r = t.split(IT);
    e.main = r[0] || "", e.sub = r[1] || "";
  }
  return e;
}
function AT(t) {
  qt(/^[a-zA-Z0-9_]+([.][a-zA-Z0-9_]+)?$/.test(t), 'componentType "' + t + '" illegal');
}
function PT(t) {
  return !!(t && t[xy]);
}
function Mh(t, e) {
  t.$constructor = t, t.extend = function(r) {
    var n = this, i;
    return LT(n) ? i = /** @class */
    (function(a) {
      F(o, a);
      function o() {
        return a.apply(this, arguments) || this;
      }
      return o;
    })(n) : (i = function() {
      (r.$constructor || n).apply(this, arguments);
    }, ch(i, this)), E(i.prototype, r), i[xy] = !0, i.extend = this.extend, i.superCall = OT, i.superApply = kT, i.superClass = n, i;
  };
}
function LT(t) {
  return X(t) && /^class\s/.test(Function.prototype.toString.call(t));
}
function My(t, e) {
  t.extend = e.extend;
}
var RT = Math.round(Math.random() * 10);
function ET(t) {
  var e = ["__\0is_clz", RT++].join("_");
  t.prototype[e] = !0, t.isInstance = function(r) {
    return !!(r && r[e]);
  };
}
function OT(t, e) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return this.superClass.prototype[e].apply(t, r);
}
function kT(t, e, r) {
  return this.superClass.prototype[e].apply(t, r);
}
function $s(t) {
  var e = {};
  t.registerClass = function(n) {
    var i = n.type || n.prototype.type;
    if (i) {
      AT(i), n.prototype.type = i;
      var a = Vt(i);
      if (!a.sub)
        e[a.main] = n;
      else if (a.sub !== Xr) {
        var o = r(a);
        o[a.sub] = n;
      }
    }
    return n;
  }, t.getClass = function(n, i, a) {
    var o = e[n];
    if (o && o[Xr] && (o = i ? o[i] : null), a && !o)
      throw new Error(i ? "Component " + n + "." + (i || "") + " is used but not imported." : n + ".type should be specified.");
    return o;
  }, t.getClassesByMainType = function(n) {
    var i = Vt(n), a = [], o = e[i.main];
    return o && o[Xr] ? w(o, function(s, u) {
      u !== Xr && a.push(s);
    }) : a.push(o), a;
  }, t.hasClass = function(n) {
    var i = Vt(n);
    return !!e[i.main];
  }, t.getAllClassMainTypes = function() {
    var n = [];
    return w(e, function(i, a) {
      n.push(a);
    }), n;
  }, t.hasSubTypes = function(n) {
    var i = Vt(n), a = e[i.main];
    return a && a[Xr];
  };
  function r(n) {
    var i = e[n.main];
    return (!i || !i[Xr]) && (i = e[n.main] = {}, i[Xr] = !0), i;
  }
}
function _a(t, e) {
  for (var r = 0; r < t.length; r++)
    t[r][1] || (t[r][1] = t[r][0]);
  return e = e || !1, function(n, i, a) {
    for (var o = {}, s = 0; s < t.length; s++) {
      var u = t[s][1];
      if (!(i && oe(i, u) >= 0 || a && oe(a, u) < 0)) {
        var l = n.getShallow(u, e);
        l != null && (o[t[s][0]] = l);
      }
    }
    return o;
  };
}
var BT = [
  ["fill", "color"],
  ["shadowBlur"],
  ["shadowOffsetX"],
  ["shadowOffsetY"],
  ["opacity"],
  ["shadowColor"]
  // Option decal is in `DecalObject` but style.decal is in `PatternObject`.
  // So do not transfer decal directly.
], NT = _a(BT), FT = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getAreaStyle = function(e, r) {
      return NT(this, e, r);
    }, t;
  })()
), gf = new ui(50);
function zT(t) {
  if (typeof t == "string") {
    var e = gf.get(t);
    return e && e.image;
  } else
    return t;
}
function Dy(t, e, r, n, i) {
  if (t)
    if (typeof t == "string") {
      if (e && e.__zrImageSrc === t || !r)
        return e;
      var a = gf.get(t), o = { hostEl: r, cb: n, cbPayload: i };
      return a ? (e = a.image, !Zs(e) && a.pending.push(o)) : (e = Le.loadImage(t, Rv, Rv), e.__zrImageSrc = t, gf.put(t, e.__cachedImgObj = {
        image: e,
        pending: [o]
      })), e;
    } else
      return t;
  else return e;
}
function Rv() {
  var t = this.__cachedImgObj;
  this.onload = this.onerror = this.__cachedImgObj = null;
  for (var e = 0; e < t.pending.length; e++) {
    var r = t.pending[e], n = r.cb;
    n && n(this, r.cbPayload), r.hostEl.dirty();
  }
  t.pending.length = 0;
}
function Zs(t) {
  return t && t.width && t.height;
}
var qu = /\{([a-zA-Z0-9_]+)\|([^}]*)\}/g;
function HT(t, e, r, n, i) {
  var a = {};
  return Iy(a, t, e, r, n, i), a.text;
}
function Iy(t, e, r, n, i, a) {
  if (!r) {
    t.text = "", t.isTruncated = !1;
    return;
  }
  var o = (e + "").split(`
`);
  a = Ay(r, n, i, a);
  for (var s = !1, u = {}, l = 0, f = o.length; l < f; l++)
    Py(u, o[l], a), o[l] = u.textLine, s = s || u.isTruncated;
  t.text = o.join(`
`), t.isTruncated = s;
}
function Ay(t, e, r, n) {
  n = n || {};
  var i = E({}, n);
  r = G(r, "..."), i.maxIterations = G(n.maxIterations, 2);
  var a = i.minChar = G(n.minChar, 0), o = i.fontMeasureInfo = Gt(e), s = o.asciiCharWidth;
  i.placeholder = G(n.placeholder, "");
  for (var u = t = Math.max(0, t - 1), l = 0; l < a && u >= s; l++)
    u -= s;
  var f = Ut(o, r);
  return f > u && (r = "", f = 0), u = t - f, i.ellipsis = r, i.ellipsisWidth = f, i.contentWidth = u, i.containerWidth = t, i;
}
function Py(t, e, r) {
  var n = r.containerWidth, i = r.contentWidth, a = r.fontMeasureInfo;
  if (!n) {
    t.textLine = "", t.isTruncated = !1;
    return;
  }
  var o = Ut(a, e);
  if (o <= n) {
    t.textLine = e, t.isTruncated = !1;
    return;
  }
  for (var s = 0; ; s++) {
    if (o <= i || s >= r.maxIterations) {
      e += r.ellipsis;
      break;
    }
    var u = s === 0 ? VT(e, i, a) : o > 0 ? Math.floor(e.length * i / o) : 0;
    e = e.substr(0, u), o = Ut(a, e);
  }
  e === "" && (e = r.placeholder), t.textLine = e, t.isTruncated = !0;
}
function VT(t, e, r) {
  for (var n = 0, i = 0, a = t.length; i < a && n < e; i++)
    n += ly(r, t.charCodeAt(i));
  return i;
}
function GT(t, e, r, n) {
  var i = Dh(t), a = e.overflow, o = e.padding, s = o ? o[1] + o[3] : 0, u = o ? o[0] + o[2] : 0, l = e.font, f = a === "truncate", h = Us(l), c = G(e.lineHeight, h), v = e.lineOverflow === "truncate", d = !1, y = e.width;
  y == null && r != null && (y = r - s);
  var p = e.height;
  p == null && n != null && (p = n - u);
  var g;
  y != null && (a === "break" || a === "breakAll") ? g = i ? Ly(i, e.font, y, a === "breakAll", 0).lines : [] : g = i ? i.split(`
`) : [];
  var m = g.length * c;
  if (p == null && (p = m), m > p && v) {
    var _ = Math.floor(p / c);
    d = d || g.length > _, g = g.slice(0, _), m = g.length * c;
  }
  if (i && f && y != null)
    for (var S = Ay(y, l, e.ellipsis, {
      minChar: e.truncateMinChar,
      placeholder: e.placeholder
    }), b = {}, T = 0; T < g.length; T++)
      Py(b, g[T], S), g[T] = b.textLine, d = d || b.isTruncated;
  for (var C = p, M = 0, x = Gt(l), T = 0; T < g.length; T++)
    M = Math.max(Ut(x, g[T]), M);
  y == null && (y = M);
  var D = y;
  return C += u, D += s, {
    lines: g,
    height: p,
    outerWidth: D,
    outerHeight: C,
    lineHeight: c,
    calculatedLineHeight: h,
    contentWidth: M,
    contentHeight: m,
    width: y,
    isTruncated: d
  };
}
var UT = /* @__PURE__ */ (function() {
  function t() {
  }
  return t;
})(), Ev = /* @__PURE__ */ (function() {
  function t(e) {
    this.tokens = [], e && (this.tokens = e);
  }
  return t;
})(), YT = /* @__PURE__ */ (function() {
  function t() {
    this.width = 0, this.height = 0, this.contentWidth = 0, this.contentHeight = 0, this.outerWidth = 0, this.outerHeight = 0, this.lines = [], this.isTruncated = !1;
  }
  return t;
})();
function WT(t, e, r, n, i) {
  var a = new YT(), o = Dh(t);
  if (!o)
    return a;
  var s = e.padding, u = s ? s[1] + s[3] : 0, l = s ? s[0] + s[2] : 0, f = e.width;
  f == null && r != null && (f = r - u);
  var h = e.height;
  h == null && n != null && (h = n - l);
  for (var c = e.overflow, v = (c === "break" || c === "breakAll") && f != null ? { width: f, accumWidth: 0, breakAll: c === "breakAll" } : null, d = qu.lastIndex = 0, y; (y = qu.exec(o)) != null; ) {
    var p = y.index;
    p > d && $u(a, o.substring(d, p), e, v), $u(a, y[2], e, v, y[1]), d = qu.lastIndex;
  }
  d < o.length && $u(a, o.substring(d, o.length), e, v);
  var g = [], m = 0, _ = 0, S = c === "truncate", b = e.lineOverflow === "truncate", T = {};
  function C(Se, tt, Qt) {
    Se.width = tt, Se.lineHeight = Qt, m += Qt, _ = Math.max(_, tt);
  }
  e: for (var M = 0; M < a.lines.length; M++) {
    for (var x = a.lines[M], D = 0, I = 0, L = 0; L < x.tokens.length; L++) {
      var A = x.tokens[L], P = A.styleName && e.rich[A.styleName] || {}, k = A.textPadding = P.padding, O = k ? k[1] + k[3] : 0, V = A.font = P.font || e.font;
      A.contentHeight = Us(V);
      var K = G(P.height, A.contentHeight);
      if (A.innerHeight = K, k && (K += k[0] + k[2]), A.height = K, A.lineHeight = gn(P.lineHeight, e.lineHeight, K), A.align = P && P.align || i, A.verticalAlign = P && P.verticalAlign || "middle", b && h != null && m + A.lineHeight > h) {
        var J = a.lines.length;
        L > 0 ? (x.tokens = x.tokens.slice(0, L), C(x, I, D), a.lines = a.lines.slice(0, M + 1)) : a.lines = a.lines.slice(0, M), a.isTruncated = a.isTruncated || a.lines.length < J;
        break e;
      }
      var ee = P.width, Q = ee == null || ee === "auto";
      if (typeof ee == "string" && ee.charAt(ee.length - 1) === "%")
        A.percentWidth = ee, g.push(A), A.contentWidth = Ut(Gt(V), A.text);
      else {
        if (Q) {
          var re = P.backgroundColor, ne = re && re.image;
          ne && (ne = zT(ne), Zs(ne) && (A.width = Math.max(A.width, ne.width * K / ne.height)));
        }
        var q = S && f != null ? f - I : null;
        q != null && q < A.width ? !Q || q < O ? (A.text = "", A.width = A.contentWidth = 0) : (Iy(T, A.text, q - O, V, e.ellipsis, { minChar: e.truncateMinChar }), A.text = T.text, a.isTruncated = a.isTruncated || T.isTruncated, A.width = A.contentWidth = Ut(Gt(V), A.text)) : A.contentWidth = Ut(Gt(V), A.text);
      }
      A.width += O, I += A.width, P && (D = Math.max(D, A.lineHeight));
    }
    C(x, I, D);
  }
  a.outerWidth = a.width = G(f, _), a.outerHeight = a.height = G(h, m), a.contentHeight = m, a.contentWidth = _, a.outerWidth += u, a.outerHeight += l;
  for (var M = 0; M < g.length; M++) {
    var A = g[M], se = A.percentWidth;
    A.width = parseInt(se, 10) / 100 * a.width;
  }
  return a;
}
function $u(t, e, r, n, i) {
  var a = e === "", o = i && r.rich[i] || {}, s = t.lines, u = o.font || r.font, l = !1, f, h;
  if (n) {
    var c = o.padding, v = c ? c[1] + c[3] : 0;
    if (o.width != null && o.width !== "auto") {
      var d = fi(o.width, n.width) + v;
      s.length > 0 && d + n.accumWidth > n.width && (f = e.split(`
`), l = !0), n.accumWidth = d;
    } else {
      var y = Ly(e, u, n.width, n.breakAll, n.accumWidth);
      n.accumWidth = y.accumWidth + v, h = y.linesWidths, f = y.lines;
    }
  }
  f || (f = e.split(`
`));
  for (var p = Gt(u), g = 0; g < f.length; g++) {
    var m = f[g], _ = new UT();
    if (_.styleName = i, _.text = m, _.isLineHolder = !m && !a, typeof o.width == "number" ? _.width = o.width : _.width = h ? h[g] : Ut(p, m), !g && !l) {
      var S = (s[s.length - 1] || (s[0] = new Ev())).tokens, b = S.length;
      b === 1 && S[0].isLineHolder ? S[0] = _ : (m || !b || a) && S.push(_);
    } else
      s.push(new Ev([_]));
  }
}
function XT(t) {
  var e = t.charCodeAt(0);
  return e >= 32 && e <= 591 || e >= 880 && e <= 4351 || e >= 4608 && e <= 5119 || e >= 7680 && e <= 8303;
}
var qT = Xt(",&?/;] ".split(""), function(t, e) {
  return t[e] = !0, t;
}, {});
function $T(t) {
  return XT(t) ? !!qT[t] : !0;
}
function Ly(t, e, r, n, i) {
  for (var a = [], o = [], s = "", u = "", l = 0, f = 0, h = Gt(e), c = 0; c < t.length; c++) {
    var v = t.charAt(c);
    if (v === `
`) {
      u && (s += u, f += l), a.push(s), o.push(f), s = "", u = "", l = 0, f = 0;
      continue;
    }
    var d = ly(h, v.charCodeAt(0)), y = n ? !1 : !$T(v);
    if (a.length ? f + d > r : i + f + d > r) {
      f ? (s || u) && (y ? (s || (s = u, u = "", l = 0, f = l), a.push(s), o.push(f - l), u += v, l += d, s = "", f = l) : (u && (s += u, u = "", l = 0), a.push(s), o.push(f), s = v, f = d)) : y ? (a.push(u), o.push(l), u = v, l = d) : (a.push(v), o.push(d));
      continue;
    }
    f += d, y ? (u += v, l += d) : (u && (s += u, u = "", l = 0), s += v);
  }
  return u && (s += u), s && (a.push(s), o.push(f)), a.length === 1 && (f += i), {
    accumWidth: f,
    lines: a,
    linesWidths: o
  };
}
function Ov(t, e, r, n, i, a) {
  if (t.baseX = r, t.baseY = n, t.outerWidth = t.outerHeight = null, !!e) {
    var o = e.width * 2, s = e.height * 2;
    Y.set(kv, li(r, o, i), _n(n, s, a), o, s), Y.intersect(e, kv, null, Bv);
    var u = Bv.outIntersectRect;
    t.outerWidth = u.width, t.outerHeight = u.height, t.baseX = li(u.x, u.width, i, !0), t.baseY = _n(u.y, u.height, a, !0);
  }
}
var kv = new Y(0, 0, 0, 0), Bv = { outIntersectRect: {}, clamp: !0 };
function Dh(t) {
  return t != null ? t += "" : t = "";
}
function ZT(t) {
  var e = Dh(t.text), r = t.font, n = Ut(Gt(r), e), i = Us(r);
  return yf(t, n, i, null);
}
function yf(t, e, r, n) {
  var i = new Y(li(t.x || 0, e, t.textAlign), _n(t.y || 0, r, t.textBaseline), e, r), a = n ?? (Ry(t) ? t.lineWidth : 0);
  return a > 0 && (i.x -= a / 2, i.y -= a / 2, i.width += a, i.height += a), i;
}
function Ry(t) {
  var e = t.stroke;
  return e != null && e !== "none" && t.lineWidth > 0;
}
var mf = "__zr_style_" + Math.round(Math.random() * 10), Sn = {
  shadowBlur: 0,
  shadowOffsetX: 0,
  shadowOffsetY: 0,
  shadowColor: "#000",
  opacity: 1,
  blend: "source-over"
}, Ks = {
  style: {
    shadowBlur: !0,
    shadowOffsetX: !0,
    shadowOffsetY: !0,
    shadowColor: !0,
    opacity: !0
  }
};
Sn[mf] = !0;
var Nv = ["z", "z2", "invisible"], KT = ["invisible"], Ba = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype._init = function(r) {
    for (var n = me(r), i = 0; i < n.length; i++) {
      var a = n[i];
      a === "style" ? this.useStyle(r[a]) : t.prototype.attrKV.call(this, a, r[a]);
    }
    this.style || this.useStyle({});
  }, e.prototype.beforeBrush = function(r) {
  }, e.prototype.afterBrush = function() {
  }, e.prototype.innerBeforeBrush = function() {
  }, e.prototype.innerAfterBrush = function() {
  }, e.prototype.shouldBePainted = function(r, n, i, a) {
    var o = this.transform;
    if (this.ignore || this.invisible || this.style.opacity === 0 || this.culling && jT(this, r, n) || o && !o[0] && !o[3])
      return !1;
    if (i && this.__clipPaths && this.__clipPaths.length) {
      for (var s = 0; s < this.__clipPaths.length; ++s)
        if (this.__clipPaths[s].isZeroArea())
          return !1;
    }
    if (a && this.parent)
      for (var u = this.parent; u; ) {
        if (u.ignore)
          return !1;
        u = u.parent;
      }
    return !0;
  }, e.prototype.contain = function(r, n) {
    return this.rectContain(r, n);
  }, e.prototype.traverse = function(r, n) {
    r.call(n, this);
  }, e.prototype.rectContain = function(r, n) {
    var i = this.transformCoordToLocal(r, n), a = this.getBoundingRect();
    return a.contain(i[0], i[1]);
  }, e.prototype.getPaintRect = function() {
    var r = this._paintRect;
    if (!this._paintRect || this.__dirty) {
      var n = this.transform, i = this.getBoundingRect(), a = this.style, o = a.shadowBlur || 0, s = a.shadowOffsetX || 0, u = a.shadowOffsetY || 0;
      r = this._paintRect || (this._paintRect = new Y(0, 0, 0, 0)), n ? Y.applyTransform(r, i, n) : r.copy(i), (o || s || u) && (r.width += o * 2 + Math.abs(s), r.height += o * 2 + Math.abs(u), r.x = Math.min(r.x, r.x + s - o), r.y = Math.min(r.y, r.y + u - o));
      var l = this.dirtyRectTolerance;
      r.isZero() || (r.x = Math.floor(r.x - l), r.y = Math.floor(r.y - l), r.width = Math.ceil(r.width + 1 + l * 2), r.height = Math.ceil(r.height + 1 + l * 2));
    }
    return r;
  }, e.prototype.setPrevPaintRect = function(r) {
    r ? (this._prevPaintRect = this._prevPaintRect || new Y(0, 0, 0, 0), this._prevPaintRect.copy(r)) : this._prevPaintRect = null;
  }, e.prototype.getPrevPaintRect = function() {
    return this._prevPaintRect;
  }, e.prototype.animateStyle = function(r) {
    return this.animate("style", r);
  }, e.prototype.updateDuringAnimation = function(r) {
    r === "style" ? this.dirtyStyle() : this.markRedraw();
  }, e.prototype.attrKV = function(r, n) {
    r !== "style" ? t.prototype.attrKV.call(this, r, n) : this.style ? this.setStyle(n) : this.useStyle(n);
  }, e.prototype.setStyle = function(r, n) {
    return typeof r == "string" ? this.style[r] = n : E(this.style, r), this.dirtyStyle(), this;
  }, e.prototype.dirtyStyle = function(r) {
    r || this.markRedraw(), this.__dirty |= qi, this._rect && (this._rect = null);
  }, e.prototype.dirty = function() {
    this.dirtyStyle();
  }, e.prototype.styleChanged = function() {
    return !!(this.__dirty & qi);
  }, e.prototype.styleUpdated = function() {
    this.__dirty &= ~qi;
  }, e.prototype.createStyle = function(r) {
    return Pa(Sn, r);
  }, e.prototype.useStyle = function(r) {
    r[mf] || (r = this.createStyle(r)), this.style = r, this.dirtyStyle();
  }, e.prototype._useHoverStyle = function(r) {
    this.__hoverStyle = r;
  }, e.prototype.isStyleObject = function(r) {
    return r[mf];
  }, e.prototype._innerSaveToNormal = function(r) {
    t.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.style && !n.style && (n.style = this._mergeStyle(this.createStyle(), this.style)), this._savePrimaryToNormal(r, n, Nv);
  }, e.prototype._applyStateObj = function(r, n, i, a, o, s) {
    t.prototype._applyStateObj.call(this, r, n, i, a, o, s);
    var u = !(n && a), l = this.__inHover === Ys, f;
    if (n && n.style ? o ? a ? f = n.style : (f = this._mergeStyle(this.createStyle(), i.style), this._mergeStyle(f, n.style)) : (f = this._mergeStyle(this.createStyle(), a ? this.style : i.style), this._mergeStyle(f, n.style)) : u && (f = i.style), f)
      if (o) {
        var h = this.style;
        if (this.style = this.createStyle(u ? {} : h), u)
          for (var c = me(h), v = 0; v < c.length; v++) {
            var d = c[v];
            d in f && (f[d] = f[d], this.style[d] = h[d]);
          }
        for (var y = me(f), v = 0; v < y.length; v++) {
          var d = y[v];
          this.style[d] = this.style[d];
        }
        this._transitionState(r, {
          style: f
        }, s, this.getAnimationStyleProps());
      } else
        l ? this._useHoverStyle(f) : this.useStyle(f);
    if (!l)
      for (var p = this.__inHover ? KT : Nv, v = 0; v < p.length; v++) {
        var d = p[v];
        n && n[d] != null ? this[d] = n[d] : u && i[d] != null && (this[d] = i[d]);
      }
  }, e.prototype._mergeStates = function(r) {
    for (var n = t.prototype._mergeStates.call(this, r), i, a = 0; a < r.length; a++) {
      var o = r[a];
      o.style && (i = i || {}, this._mergeStyle(i, o.style));
    }
    return i && (n.style = i), n;
  }, e.prototype._mergeStyle = function(r, n) {
    return E(r, n), r;
  }, e.prototype.getAnimationStyleProps = function() {
    return Ks;
  }, e.initDefaultProps = (function() {
    var r = e.prototype;
    r.type = "displayable", r.invisible = !1, r.z = 0, r.z2 = 0, r.zlevel = 0, r.culling = !1, r.cursor = "pointer", r.rectHover = !1, r.incremental = 0, r._rect = null, r.dirtyRectTolerance = 0, r.__dirty = Je | qi;
  })(), e;
})(Ws), Zu = new Y(0, 0, 0, 0), Ku = new Y(0, 0, 0, 0);
function jT(t, e, r) {
  return Zu.copy(t.getBoundingRect()), t.transform && Zu.applyTransform(t.transform), Ku.width = e, Ku.height = r, !Zu.intersect(Ku);
}
var mt = Math.min, _t = Math.max, ju = Math.sin, Qu = Math.cos, qr = Math.PI * 2, Qa = xn(), Ja = xn(), eo = xn();
function Fv(t, e, r, n, i, a) {
  i[0] = mt(t, r), i[1] = mt(e, n), a[0] = _t(t, r), a[1] = _t(e, n);
}
var zv = [], Hv = [];
function QT(t, e, r, n, i, a, o, s, u, l) {
  var f = Jg, h = Pe, c = f(t, r, i, o, zv);
  u[0] = 1 / 0, u[1] = 1 / 0, l[0] = -1 / 0, l[1] = -1 / 0;
  for (var v = 0; v < c; v++) {
    var d = h(t, r, i, o, zv[v]);
    u[0] = mt(d, u[0]), l[0] = _t(d, l[0]);
  }
  c = f(e, n, a, s, Hv);
  for (var v = 0; v < c; v++) {
    var y = h(e, n, a, s, Hv[v]);
    u[1] = mt(y, u[1]), l[1] = _t(y, l[1]);
  }
  u[0] = mt(t, u[0]), l[0] = _t(t, l[0]), u[0] = mt(o, u[0]), l[0] = _t(o, l[0]), u[1] = mt(e, u[1]), l[1] = _t(e, l[1]), u[1] = mt(s, u[1]), l[1] = _t(s, l[1]);
}
function JT(t, e, r, n, i, a, o, s) {
  var u = ey, l = Be, f = _t(mt(u(t, r, i), 1), 0), h = _t(mt(u(e, n, a), 1), 0), c = l(t, r, i, f), v = l(e, n, a, h);
  o[0] = mt(t, i, c), o[1] = mt(e, a, v), s[0] = _t(t, i, c), s[1] = _t(e, a, v);
}
function ew(t, e, r, n, i, a, o, s, u) {
  var l = Cr, f = xr, h = Math.abs(i - a);
  if (h % qr < 1e-4 && h > 1e-4) {
    s[0] = t - r, s[1] = e - n, u[0] = t + r, u[1] = e + n;
    return;
  }
  if (Qa[0] = Qu(i) * r + t, Qa[1] = ju(i) * n + e, Ja[0] = Qu(a) * r + t, Ja[1] = ju(a) * n + e, l(s, Qa, Ja), f(u, Qa, Ja), i = i % qr, i < 0 && (i = i + qr), a = a % qr, a < 0 && (a = a + qr), i > a && !o ? a += qr : i < a && o && (i += qr), o) {
    var c = a;
    a = i, i = c;
  }
  for (var v = 0; v < a; v += Math.PI / 2)
    v > i && (eo[0] = Qu(v) * r + t, eo[1] = ju(v) * n + e, l(s, eo, s), f(u, eo, u));
}
var ie = {
  M: 1,
  L: 2,
  C: 3,
  Q: 4,
  A: 5,
  Z: 6,
  R: 7
}, $r = [], Zr = [], Ot = [], mr = [], kt = [], Bt = [], Ju = Math.min, el = Math.max, Kr = Math.cos, jr = Math.sin, rr = Math.abs, _f = Math.PI, Tr = _f * 2, tl = typeof Float32Array < "u", Pi = [];
function rl(t) {
  var e = Math.round(t / _f * 1e8) / 1e8;
  return e % 2 * _f;
}
function tw(t, e) {
  var r = rl(t[0]);
  r < 0 && (r += Tr);
  var n = r - t[0], i = t[1];
  i += n, !e && i - r >= Tr ? i = r + Tr : e && r - i >= Tr ? i = r - Tr : !e && r > i ? i = r + (Tr - rl(r - i)) : e && r < i && (i = r - (Tr - rl(i - r))), t[0] = r, t[1] = i;
}
var ci = (function() {
  function t(e) {
    this.dpr = 1, this._xi = 0, this._yi = 0, this._x0 = 0, this._y0 = 0, this._len = 0, e && (this._saveData = !1), this._saveData && (this.data = []);
  }
  return t.prototype.increaseVersion = function() {
    this._version++;
  }, t.prototype.getVersion = function() {
    return this._version;
  }, t.prototype.setScale = function(e, r, n) {
    n = n || 0, n > 0 && (this._ux = rr(n / ss / e) || 0, this._uy = rr(n / ss / r) || 0);
  }, t.prototype.setDPR = function(e) {
    this.dpr = e;
  }, t.prototype.setContext = function(e) {
    this._ctx = e;
  }, t.prototype.getContext = function() {
    return this._ctx;
  }, t.prototype.beginPath = function() {
    return this._ctx && this._ctx.beginPath(), this.reset(), this;
  }, t.prototype.reset = function() {
    this._saveData && (this._len = 0), this._pathSegLen && (this._pathSegLen = null, this._pathLen = 0), this._version++;
  }, t.prototype.moveTo = function(e, r) {
    return this._drawPendingPt(), this.addData(ie.M, e, r), this._ctx && this._ctx.moveTo(e, r), this._x0 = e, this._y0 = r, this._xi = e, this._yi = r, this;
  }, t.prototype.lineTo = function(e, r) {
    var n = rr(e - this._xi), i = rr(r - this._yi), a = n > this._ux || i > this._uy;
    if (this.addData(ie.L, e, r), this._ctx && a && this._ctx.lineTo(e, r), a)
      this._xi = e, this._yi = r, this._pendingPtDist = 0;
    else {
      var o = n * n + i * i;
      o > this._pendingPtDist && (this._pendingPtX = e, this._pendingPtY = r, this._pendingPtDist = o);
    }
    return this;
  }, t.prototype.bezierCurveTo = function(e, r, n, i, a, o) {
    return this._drawPendingPt(), this.addData(ie.C, e, r, n, i, a, o), this._ctx && this._ctx.bezierCurveTo(e, r, n, i, a, o), this._xi = a, this._yi = o, this;
  }, t.prototype.quadraticCurveTo = function(e, r, n, i) {
    return this._drawPendingPt(), this.addData(ie.Q, e, r, n, i), this._ctx && this._ctx.quadraticCurveTo(e, r, n, i), this._xi = n, this._yi = i, this;
  }, t.prototype.arc = function(e, r, n, i, a, o) {
    this._drawPendingPt(), Pi[0] = i, Pi[1] = a, tw(Pi, o), i = Pi[0], a = Pi[1];
    var s = a - i;
    return this.addData(ie.A, e, r, n, n, i, s, 0, o ? 0 : 1), this._ctx && this._ctx.arc(e, r, n, i, a, o), this._xi = Kr(a) * n + e, this._yi = jr(a) * n + r, this;
  }, t.prototype.arcTo = function(e, r, n, i, a) {
    return this._drawPendingPt(), this._ctx && this._ctx.arcTo(e, r, n, i, a), this;
  }, t.prototype.rect = function(e, r, n, i) {
    return this._drawPendingPt(), this._ctx && this._ctx.rect(e, r, n, i), this.addData(ie.R, e, r, n, i), this;
  }, t.prototype.closePath = function() {
    this._drawPendingPt(), this.addData(ie.Z);
    var e = this._ctx, r = this._x0, n = this._y0;
    return e && e.closePath(), this._xi = r, this._yi = n, this;
  }, t.prototype.fill = function(e) {
    e && e.fill(), this.toStatic();
  }, t.prototype.stroke = function(e) {
    e && e.stroke(), this.toStatic();
  }, t.prototype.len = function() {
    return this._len;
  }, t.prototype.setData = function(e) {
    if (this._saveData) {
      var r = e.length;
      !(this.data && this.data.length === r) && tl && (this.data = new Float32Array(r));
      for (var n = 0; n < r; n++)
        this.data[n] = e[n];
      this._len = r;
    }
  }, t.prototype.appendPath = function(e) {
    if (this._saveData) {
      e instanceof Array || (e = [e]);
      for (var r = e.length, n = 0, i = this._len, a = 0; a < r; a++)
        n += e[a].len();
      var o = this.data;
      if (tl && (o instanceof Float32Array || !o) && (this.data = new Float32Array(i + n), i > 0 && o))
        for (var s = 0; s < i; s++)
          this.data[s] = o[s];
      for (var a = 0; a < r; a++)
        for (var u = e[a].data, s = 0; s < u.length; s++)
          this.data[i++] = u[s];
      this._len = i;
    }
  }, t.prototype.addData = function(e, r, n, i, a, o, s, u, l) {
    if (this._saveData) {
      var f = this.data;
      this._len + arguments.length > f.length && (this._expandData(), f = this.data);
      for (var h = 0; h < arguments.length; h++)
        f[this._len++] = arguments[h];
    }
  }, t.prototype._drawPendingPt = function() {
    this._pendingPtDist > 0 && (this._ctx && this._ctx.lineTo(this._pendingPtX, this._pendingPtY), this._pendingPtDist = 0);
  }, t.prototype._expandData = function() {
    if (!(this.data instanceof Array)) {
      for (var e = [], r = 0; r < this._len; r++)
        e[r] = this.data[r];
      this.data = e;
    }
  }, t.prototype.toStatic = function() {
    if (this._saveData) {
      this._drawPendingPt();
      var e = this.data;
      e instanceof Array && (e.length = this._len, tl && this._len > 11 && (this.data = new Float32Array(e)));
    }
  }, t.prototype.getBoundingRect = function() {
    Ot[0] = Ot[1] = kt[0] = kt[1] = Number.MAX_VALUE, mr[0] = mr[1] = Bt[0] = Bt[1] = -Number.MAX_VALUE;
    var e = this.data, r = 0, n = 0, i = 0, a = 0, o;
    for (o = 0; o < this._len; ) {
      var s = e[o++], u = o === 1;
      switch (u && (r = e[o], n = e[o + 1], i = r, a = n), s) {
        case ie.M:
          r = i = e[o++], n = a = e[o++], kt[0] = i, kt[1] = a, Bt[0] = i, Bt[1] = a;
          break;
        case ie.L:
          Fv(r, n, e[o], e[o + 1], kt, Bt), r = e[o++], n = e[o++];
          break;
        case ie.C:
          QT(r, n, e[o++], e[o++], e[o++], e[o++], e[o], e[o + 1], kt, Bt), r = e[o++], n = e[o++];
          break;
        case ie.Q:
          JT(r, n, e[o++], e[o++], e[o], e[o + 1], kt, Bt), r = e[o++], n = e[o++];
          break;
        case ie.A:
          var l = e[o++], f = e[o++], h = e[o++], c = e[o++], v = e[o++], d = e[o++] + v;
          o += 1;
          var y = !e[o++];
          u && (i = Kr(v) * h + l, a = jr(v) * c + f), ew(l, f, h, c, v, d, y, kt, Bt), r = Kr(d) * h + l, n = jr(d) * c + f;
          break;
        case ie.R:
          i = r = e[o++], a = n = e[o++];
          var p = e[o++], g = e[o++];
          Fv(i, a, i + p, a + g, kt, Bt);
          break;
        case ie.Z:
          r = i, n = a;
          break;
      }
      Cr(Ot, Ot, kt), xr(mr, mr, Bt);
    }
    return o === 0 && (Ot[0] = Ot[1] = mr[0] = mr[1] = 0), new Y(Ot[0], Ot[1], mr[0] - Ot[0], mr[1] - Ot[1]);
  }, t.prototype._calculateLength = function() {
    var e = this.data, r = this._len, n = this._ux, i = this._uy, a = 0, o = 0, s = 0, u = 0;
    this._pathSegLen || (this._pathSegLen = []);
    for (var l = this._pathSegLen, f = 0, h = 0, c = 0; c < r; ) {
      var v = e[c++], d = c === 1;
      d && (a = e[c], o = e[c + 1], s = a, u = o);
      var y = -1;
      switch (v) {
        case ie.M:
          a = s = e[c++], o = u = e[c++];
          break;
        case ie.L: {
          var p = e[c++], g = e[c++], m = p - a, _ = g - o;
          (rr(m) > n || rr(_) > i || c === r - 1) && (y = Math.sqrt(m * m + _ * _), a = p, o = g);
          break;
        }
        case ie.C: {
          var S = e[c++], b = e[c++], p = e[c++], g = e[c++], T = e[c++], C = e[c++];
          y = Y1(a, o, S, b, p, g, T, C, 10), a = T, o = C;
          break;
        }
        case ie.Q: {
          var S = e[c++], b = e[c++], p = e[c++], g = e[c++];
          y = q1(a, o, S, b, p, g, 10), a = p, o = g;
          break;
        }
        case ie.A:
          var M = e[c++], x = e[c++], D = e[c++], I = e[c++], L = e[c++], A = e[c++], P = A + L;
          c += 1, d && (s = Kr(L) * D + M, u = jr(L) * I + x), y = el(D, I) * Ju(Tr, Math.abs(A)), a = Kr(P) * D + M, o = jr(P) * I + x;
          break;
        case ie.R: {
          s = a = e[c++], u = o = e[c++];
          var k = e[c++], O = e[c++];
          y = k * 2 + O * 2;
          break;
        }
        case ie.Z: {
          var m = s - a, _ = u - o;
          y = Math.sqrt(m * m + _ * _), a = s, o = u;
          break;
        }
      }
      y >= 0 && (l[h++] = y, f += y);
    }
    return this._pathLen = f, f;
  }, t.prototype.rebuildPath = function(e, r) {
    var n = this.data, i = this._ux, a = this._uy, o = this._len, s, u, l, f, h, c, v = r < 1, d, y, p = 0, g = 0, m, _ = 0, S, b;
    if (!(v && (this._pathSegLen || this._calculateLength(), d = this._pathSegLen, y = this._pathLen, m = r * y, !m)))
      e: for (var T = 0; T < o; ) {
        var C = n[T++], M = T === 1;
        switch (M && (l = n[T], f = n[T + 1], s = l, u = f), C !== ie.L && _ > 0 && (e.lineTo(S, b), _ = 0), C) {
          case ie.M:
            s = l = n[T++], u = f = n[T++], e.moveTo(l, f);
            break;
          case ie.L: {
            h = n[T++], c = n[T++];
            var x = rr(h - l), D = rr(c - f);
            if (x > i || D > a) {
              if (v) {
                var I = d[g++];
                if (p + I > m) {
                  var L = (m - p) / I;
                  e.lineTo(l * (1 - L) + h * L, f * (1 - L) + c * L);
                  break e;
                }
                p += I;
              }
              e.lineTo(h, c), l = h, f = c, _ = 0;
            } else {
              var A = x * x + D * D;
              A > _ && (S = h, b = c, _ = A);
            }
            break;
          }
          case ie.C: {
            var P = n[T++], k = n[T++], O = n[T++], V = n[T++], K = n[T++], J = n[T++];
            if (v) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                is(l, P, O, K, L, $r), is(f, k, V, J, L, Zr), e.bezierCurveTo($r[1], Zr[1], $r[2], Zr[2], $r[3], Zr[3]);
                break e;
              }
              p += I;
            }
            e.bezierCurveTo(P, k, O, V, K, J), l = K, f = J;
            break;
          }
          case ie.Q: {
            var P = n[T++], k = n[T++], O = n[T++], V = n[T++];
            if (v) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                as(l, P, O, L, $r), as(f, k, V, L, Zr), e.quadraticCurveTo($r[1], Zr[1], $r[2], Zr[2]);
                break e;
              }
              p += I;
            }
            e.quadraticCurveTo(P, k, O, V), l = O, f = V;
            break;
          }
          case ie.A:
            var ee = n[T++], Q = n[T++], re = n[T++], ne = n[T++], q = n[T++], se = n[T++], Se = n[T++], tt = !n[T++], Qt = re > ne ? re : ne, rt = rr(re - ne) > 1e-3, Ae = q + se, $ = !1;
            if (v) {
              var I = d[g++];
              p + I > m && (Ae = q + se * (m - p) / I, $ = !0), p += I;
            }
            if (rt && e.ellipse ? e.ellipse(ee, Q, re, ne, Se, q, Ae, tt) : e.arc(ee, Q, Qt, q, Ae, tt), $)
              break e;
            M && (s = Kr(q) * re + ee, u = jr(q) * ne + Q), l = Kr(Ae) * re + ee, f = jr(Ae) * ne + Q;
            break;
          case ie.R:
            s = l = n[T], u = f = n[T + 1], h = n[T++], c = n[T++];
            var te = n[T++], zr = n[T++];
            if (v) {
              var I = d[g++];
              if (p + I > m) {
                var He = m - p;
                e.moveTo(h, c), e.lineTo(h + Ju(He, te), c), He -= te, He > 0 && e.lineTo(h + te, c + Ju(He, zr)), He -= zr, He > 0 && e.lineTo(h + el(te - He, 0), c + zr), He -= te, He > 0 && e.lineTo(h, c + el(zr - He, 0));
                break e;
              }
              p += I;
            }
            e.rect(h, c, te, zr);
            break;
          case ie.Z:
            if (v) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                e.lineTo(l * (1 - L) + s * L, f * (1 - L) + u * L);
                break e;
              }
              p += I;
            }
            e.closePath(), l = s, f = u;
        }
      }
  }, t.prototype.clone = function() {
    var e = new t(), r = this.data;
    return e.data = r.slice ? r.slice() : Array.prototype.slice.call(r), e._len = this._len, e;
  }, t.prototype.canSave = function() {
    return !!this._saveData;
  }, t.CMD = ie, t.initDefaultProps = (function() {
    var e = t.prototype;
    e._saveData = !0, e._ux = 0, e._uy = 0, e._pendingPtDist = 0, e._version = 0;
  })(), t;
})();
function Bn(t, e, r, n, i, a, o) {
  if (i === 0)
    return !1;
  var s = i, u = 0, l = t;
  if (o > e + s && o > n + s || o < e - s && o < n - s || a > t + s && a > r + s || a < t - s && a < r - s)
    return !1;
  if (t !== r)
    u = (e - n) / (t - r), l = (t * n - r * e) / (t - r);
  else
    return Math.abs(a - t) <= s / 2;
  var f = u * a - o + l, h = f * f / (u * u + 1);
  return h <= s / 2 * s / 2;
}
function rw(t, e, r, n, i, a, o, s, u, l, f) {
  if (u === 0)
    return !1;
  var h = u;
  if (f > e + h && f > n + h && f > a + h && f > s + h || f < e - h && f < n - h && f < a - h && f < s - h || l > t + h && l > r + h && l > i + h && l > o + h || l < t - h && l < r - h && l < i - h && l < o - h)
    return !1;
  var c = U1(t, e, r, n, i, a, o, s, l, f, null);
  return c <= h / 2;
}
function nw(t, e, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  if (u > e + l && u > n + l && u > a + l || u < e - l && u < n - l && u < a - l || s > t + l && s > r + l && s > i + l || s < t - l && s < r - l && s < i - l)
    return !1;
  var f = X1(t, e, r, n, i, a, s, u, null);
  return f <= l / 2;
}
var Vv = Math.PI * 2;
function to(t) {
  return t %= Vv, t < 0 && (t += Vv), t;
}
var Li = Math.PI * 2;
function iw(t, e, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  s -= t, u -= e;
  var f = Math.sqrt(s * s + u * u);
  if (f - l > r || f + l < r)
    return !1;
  if (Math.abs(n - i) % Li < 1e-4)
    return !0;
  if (a) {
    var h = n;
    n = to(i), i = to(h);
  } else
    n = to(n), i = to(i);
  n > i && (i += Li);
  var c = Math.atan2(u, s);
  return c < 0 && (c += Li), c >= n && c <= i || c + Li >= n && c + Li <= i;
}
function sr(t, e, r, n, i, a) {
  if (a > e && a > n || a < e && a < n || n === e)
    return 0;
  var o = (a - e) / (n - e), s = n < e ? 1 : -1;
  (o === 1 || o === 0) && (s = n < e ? 0.5 : -0.5);
  var u = o * (r - t) + t;
  return u === i ? 1 / 0 : u > i ? s : 0;
}
var _r = ci.CMD, Qr = Math.PI * 2, aw = 1e-4;
function ow(t, e) {
  return Math.abs(t - e) < aw;
}
var Ve = [-1, -1, -1], gt = [-1, -1];
function sw() {
  var t = gt[0];
  gt[0] = gt[1], gt[1] = t;
}
function uw(t, e, r, n, i, a, o, s, u, l) {
  if (l > e && l > n && l > a && l > s || l < e && l < n && l < a && l < s)
    return 0;
  var f = Qg(e, n, a, s, l, Ve);
  if (f === 0)
    return 0;
  for (var h = 0, c = -1, v = void 0, d = void 0, y = 0; y < f; y++) {
    var p = Ve[y], g = p === 0 || p === 1 ? 0.5 : 1, m = Pe(t, r, i, o, p);
    m < u || (c < 0 && (c = Jg(e, n, a, s, gt), gt[1] < gt[0] && c > 1 && sw(), v = Pe(e, n, a, s, gt[0]), c > 1 && (d = Pe(e, n, a, s, gt[1]))), c === 2 ? p < gt[0] ? h += v < e ? g : -g : p < gt[1] ? h += d < v ? g : -g : h += s < d ? g : -g : p < gt[0] ? h += v < e ? g : -g : h += s < v ? g : -g);
  }
  return h;
}
function lw(t, e, r, n, i, a, o, s) {
  if (s > e && s > n && s > a || s < e && s < n && s < a)
    return 0;
  var u = W1(e, n, a, s, Ve);
  if (u === 0)
    return 0;
  var l = ey(e, n, a);
  if (l >= 0 && l <= 1) {
    for (var f = 0, h = Be(e, n, a, l), c = 0; c < u; c++) {
      var v = Ve[c] === 0 || Ve[c] === 1 ? 0.5 : 1, d = Be(t, r, i, Ve[c]);
      d < o || (Ve[c] < l ? f += h < e ? v : -v : f += a < h ? v : -v);
    }
    return f;
  } else {
    var v = Ve[0] === 0 || Ve[0] === 1 ? 0.5 : 1, d = Be(t, r, i, Ve[0]);
    return d < o ? 0 : a < e ? v : -v;
  }
}
function fw(t, e, r, n, i, a, o, s) {
  if (s -= e, s > r || s < -r)
    return 0;
  var u = Math.sqrt(r * r - s * s);
  Ve[0] = -u, Ve[1] = u;
  var l = Math.abs(n - i);
  if (l < 1e-4)
    return 0;
  if (l >= Qr - 1e-4) {
    n = 0, i = Qr;
    var f = a ? 1 : -1;
    return o >= Ve[0] + t && o <= Ve[1] + t ? f : 0;
  }
  if (n > i) {
    var h = n;
    n = i, i = h;
  }
  n < 0 && (n += Qr, i += Qr);
  for (var c = 0, v = 0; v < 2; v++) {
    var d = Ve[v];
    if (d + t > o) {
      var y = Math.atan2(s, d), f = a ? 1 : -1;
      y < 0 && (y = Qr + y), (y >= n && y <= i || y + Qr >= n && y + Qr <= i) && (y > Math.PI / 2 && y < Math.PI * 1.5 && (f = -f), c += f);
    }
  }
  return c;
}
function Ey(t, e, r, n, i) {
  for (var a = t.data, o = t.len(), s = 0, u = 0, l = 0, f = 0, h = 0, c, v, d = 0; d < o; ) {
    var y = a[d++], p = d === 1;
    switch (y === _r.M && d > 1 && (r || (s += sr(u, l, f, h, n, i))), p && (u = a[d], l = a[d + 1], f = u, h = l), y) {
      case _r.M:
        f = a[d++], h = a[d++], u = f, l = h;
        break;
      case _r.L:
        if (r) {
          if (Bn(u, l, a[d], a[d + 1], e, n, i))
            return !0;
        } else
          s += sr(u, l, a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case _r.C:
        if (r) {
          if (rw(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], e, n, i))
            return !0;
        } else
          s += uw(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case _r.Q:
        if (r) {
          if (nw(u, l, a[d++], a[d++], a[d], a[d + 1], e, n, i))
            return !0;
        } else
          s += lw(u, l, a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case _r.A:
        var g = a[d++], m = a[d++], _ = a[d++], S = a[d++], b = a[d++], T = a[d++];
        d += 1;
        var C = !!(1 - a[d++]);
        c = Math.cos(b) * _ + g, v = Math.sin(b) * S + m, p ? (f = c, h = v) : s += sr(u, l, c, v, n, i);
        var M = (n - g) * S / _ + g;
        if (r) {
          if (iw(g, m, S, b, b + T, C, e, M, i))
            return !0;
        } else
          s += fw(g, m, S, b, b + T, C, M, i);
        u = Math.cos(b + T) * _ + g, l = Math.sin(b + T) * S + m;
        break;
      case _r.R:
        f = u = a[d++], h = l = a[d++];
        var x = a[d++], D = a[d++];
        if (c = f + x, v = h + D, r) {
          if (Bn(f, h, c, h, e, n, i) || Bn(c, h, c, v, e, n, i) || Bn(c, v, f, v, e, n, i) || Bn(f, v, f, h, e, n, i))
            return !0;
        } else
          s += sr(c, h, c, v, n, i), s += sr(f, v, f, h, n, i);
        break;
      case _r.Z:
        if (r) {
          if (Bn(u, l, f, h, e, n, i))
            return !0;
        } else
          s += sr(u, l, f, h, n, i);
        u = f, l = h;
        break;
    }
  }
  return !r && !ow(l, h) && (s += sr(u, l, f, h, n, i) || 0), s !== 0;
}
function hw(t, e, r) {
  return Ey(t, 0, !1, e, r);
}
function cw(t, e, r, n) {
  return Ey(t, e, !0, r, n);
}
var Oy = _e({
  fill: "#000",
  stroke: null,
  strokePercent: 1,
  fillOpacity: 1,
  strokeOpacity: 1,
  lineDashOffset: 0,
  lineWidth: 1,
  lineCap: "butt",
  miterLimit: 10,
  strokeNoScale: !1,
  strokeFirst: !1
}, Sn), vw = {
  style: _e({
    fill: !0,
    stroke: !0,
    strokePercent: !0,
    fillOpacity: !0,
    strokeOpacity: !0,
    lineDashOffset: !0,
    lineWidth: !0,
    miterLimit: !0
  }, Ks.style)
}, nl = Gs.concat([
  "invisible",
  "culling",
  "z",
  "z2",
  "zlevel",
  "parent"
]), ye = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.update = function() {
    var r = this;
    t.prototype.update.call(this);
    var n = this.style;
    if (n.decal) {
      var i = this._decalEl = this._decalEl || new e();
      i.buildPath === e.prototype.buildPath && (i.buildPath = function(u) {
        r.buildPath(u, r.shape);
      }), i.silent = !0;
      var a = i.style;
      for (var o in n)
        a[o] !== n[o] && (a[o] = n[o]);
      a.fill = n.fill ? n.decal : null, a.decal = null, a.shadowColor = null, n.strokeFirst && (a.stroke = null);
      for (var s = 0; s < nl.length; ++s)
        i[nl[s]] = this[nl[s]];
      i.__dirty |= Je;
    } else this._decalEl && (this._decalEl = null);
  }, e.prototype.getDecalElement = function() {
    return this._decalEl;
  }, e.prototype._init = function(r) {
    var n = me(r);
    this.shape = this.getDefaultShape();
    var i = this.getDefaultStyle();
    i && this.useStyle(i);
    for (var a = 0; a < n.length; a++) {
      var o = n[a], s = r[o];
      o === "style" ? this.style ? E(this.style, s) : this.useStyle(s) : o === "shape" ? E(this.shape, s) : t.prototype.attrKV.call(this, o, s);
    }
    this.style || this.useStyle({});
  }, e.prototype.getDefaultStyle = function() {
    return null;
  }, e.prototype.getDefaultShape = function() {
    return {};
  }, e.prototype.canBeInsideText = function() {
    return this.hasFill();
  }, e.prototype.getInsideTextFill = function() {
    var r = this.style.fill;
    if (r !== "none") {
      if (N(r)) {
        var n = pa(r, 0);
        return n > 0.5 ? hf : n > 0.2 ? Sb : cf;
      } else if (r)
        return cf;
    }
    return hf;
  }, e.prototype.getInsideTextStroke = function(r) {
    var n = this.style.fill;
    if (N(n)) {
      var i = this.__zr, a = !!(i && i.isDarkMode()), o = pa(r, 0) < ff;
      if (a === o)
        return n;
    }
  }, e.prototype.buildPath = function(r, n, i) {
  }, e.prototype.pathUpdated = function() {
    this.__dirty &= ~$n;
  }, e.prototype.getUpdatedPathProxy = function(r) {
    return !this.path && this.createPathProxy(), this.path.beginPath(), this.buildPath(this.path, this.shape, r), this.path;
  }, e.prototype.createPathProxy = function() {
    this.path = new ci(!1);
  }, e.prototype.hasStroke = function() {
    var r = this.style, n = r.stroke;
    return !(n == null || n === "none" || !(r.lineWidth > 0));
  }, e.prototype.hasFill = function() {
    var r = this.style, n = r.fill;
    return n != null && n !== "none";
  }, e.prototype.getBoundingRect = function() {
    var r = this._rect, n = this.style, i = !r;
    if (i) {
      var a = !1;
      this.path || (a = !0, this.createPathProxy());
      var o = this.path;
      (a || this.__dirty & $n) && (o.beginPath(), this.buildPath(o, this.shape, !1), this.pathUpdated()), r = o.getBoundingRect();
    }
    if (this._rect = r, this.hasStroke() && this.path && this.path.len() > 0) {
      var s = this._rectStroke || (this._rectStroke = r.clone());
      if (this.__dirty || i) {
        s.copy(r);
        var u = n.strokeNoScale ? this.getLineScale() : 1, l = n.lineWidth;
        if (!this.hasFill()) {
          var f = this.strokeContainThreshold;
          l = Math.max(l, f ?? 4);
        }
        u > 1e-10 && (s.width += l / u, s.height += l / u, s.x -= l / u / 2, s.y -= l / u / 2);
      }
      return s;
    }
    return r;
  }, e.prototype.contain = function(r, n) {
    var i = this.transformCoordToLocal(r, n), a = this.getBoundingRect(), o = this.style;
    if (r = i[0], n = i[1], a.contain(r, n)) {
      var s = this.path;
      if (this.hasStroke()) {
        var u = o.lineWidth, l = o.strokeNoScale ? this.getLineScale() : 1;
        if (l > 1e-10 && (this.hasFill() || (u = Math.max(u, this.strokeContainThreshold)), cw(s, u / l, r, n)))
          return !0;
      }
      if (this.hasFill())
        return hw(s, r, n);
    }
    return !1;
  }, e.prototype.dirtyShape = function() {
    this.__dirty |= $n, this._rect && (this._rect = null), this._decalEl && this._decalEl.dirtyShape(), this.markRedraw();
  }, e.prototype.dirty = function() {
    this.dirtyStyle(), this.dirtyShape();
  }, e.prototype.animateShape = function(r) {
    return this.animate("shape", r);
  }, e.prototype.updateDuringAnimation = function(r) {
    r === "style" ? this.dirtyStyle() : r === "shape" ? this.dirtyShape() : this.markRedraw();
  }, e.prototype.attrKV = function(r, n) {
    r === "shape" ? this.setShape(n) : t.prototype.attrKV.call(this, r, n);
  }, e.prototype.setShape = function(r, n) {
    var i = this.shape;
    return i || (i = this.shape = {}), typeof r == "string" ? i[r] = n : E(i, r), this.dirtyShape(), this;
  }, e.prototype.shapeChanged = function() {
    return !!(this.__dirty & $n);
  }, e.prototype.createStyle = function(r) {
    return Pa(Oy, r);
  }, e.prototype._innerSaveToNormal = function(r) {
    t.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.shape && !n.shape && (n.shape = E({}, this.shape));
  }, e.prototype._applyStateObj = function(r, n, i, a, o, s) {
    if (t.prototype._applyStateObj.call(this, r, n, i, a, o, s), this.__inHover !== Ys) {
      var u = !(n && a), l;
      if (n && n.shape ? o ? a ? l = n.shape : (l = E({}, i.shape), E(l, n.shape)) : (l = E({}, a ? this.shape : i.shape), E(l, n.shape)) : u && (l = i.shape), l)
        if (o) {
          this.shape = E({}, this.shape);
          for (var f = {}, h = me(l), c = 0; c < h.length; c++) {
            var v = h[c];
            typeof l[v] == "object" ? this.shape[v] = l[v] : f[v] = l[v];
          }
          this._transitionState(r, {
            shape: f
          }, s);
        } else
          this.shape = l, this.dirtyShape();
    }
  }, e.prototype._mergeStates = function(r) {
    for (var n = t.prototype._mergeStates.call(this, r), i, a = 0; a < r.length; a++) {
      var o = r[a];
      o.shape && (i = i || {}, this._mergeStyle(i, o.shape));
    }
    return i && (n.shape = i), n;
  }, e.prototype.getAnimationStyleProps = function() {
    return vw;
  }, e.prototype.isZeroArea = function() {
    return !1;
  }, e.extend = function(r) {
    var n = (function(a) {
      F(o, a);
      function o(s) {
        var u = a.call(this, s) || this;
        return r.init && r.init.call(u, s), u;
      }
      return o.prototype.getDefaultStyle = function() {
        return Z(r.style);
      }, o.prototype.getDefaultShape = function() {
        return Z(r.shape);
      }, o;
    })(e);
    for (var i in r)
      typeof r[i] == "function" && (n.prototype[i] = r[i]);
    return n;
  }, e.initDefaultProps = (function() {
    var r = e.prototype;
    r.type = "path", r.strokeContainThreshold = 5, r.segmentIgnoreThreshold = 0, r.subPixelOptimize = !1, r.autoBatch = !1, r.__dirty = Je | qi | $n;
  })(), e;
})(Ba), dw = _e({
  strokeFirst: !0,
  font: Er,
  x: 0,
  y: 0,
  textAlign: "left",
  textBaseline: "top",
  miterLimit: 2
}, Oy), fs = (function(t) {
  F(e, t);
  function e() {
    return t !== null && t.apply(this, arguments) || this;
  }
  return e.prototype.hasStroke = function() {
    return Ry(this.style);
  }, e.prototype.hasFill = function() {
    var r = this.style, n = r.fill;
    return n != null && n !== "none";
  }, e.prototype.createStyle = function(r) {
    return Pa(dw, r);
  }, e.prototype.setBoundingRect = function(r) {
    this._rect = r;
  }, e.prototype.getBoundingRect = function() {
    return this._rect || (this._rect = ZT(this.style)), this._rect;
  }, e.initDefaultProps = (function() {
    var r = e.prototype;
    r.dirtyRectTolerance = 10;
  })(), e;
})(Ba);
fs.prototype.type = "tspan";
var pw = _e({
  x: 0,
  y: 0
}, Sn), gw = {
  style: _e({
    x: !0,
    y: !0,
    width: !0,
    height: !0,
    sx: !0,
    sy: !0,
    sWidth: !0,
    sHeight: !0
  }, Ks.style)
};
function yw(t) {
  return !!(t && typeof t != "string" && t.width && t.height);
}
var Nr = (function(t) {
  F(e, t);
  function e() {
    return t !== null && t.apply(this, arguments) || this;
  }
  return e.prototype.createStyle = function(r) {
    return Pa(pw, r);
  }, e.prototype._getSize = function(r) {
    var n = this.style, i = n[r];
    if (i != null)
      return i;
    var a = yw(n.image) ? n.image : this.__image;
    if (!a)
      return 0;
    var o = r === "width" ? "height" : "width", s = n[o];
    return s == null ? a[r] : a[r] / a[o] * s;
  }, e.prototype.getWidth = function() {
    return this._getSize("width");
  }, e.prototype.getHeight = function() {
    return this._getSize("height");
  }, e.prototype.getAnimationStyleProps = function() {
    return gw;
  }, e.prototype.getBoundingRect = function() {
    var r = this.style;
    return this._rect || (this._rect = new Y(r.x || 0, r.y || 0, this.getWidth(), this.getHeight())), this._rect;
  }, e;
})(Ba);
Nr.prototype.type = "image";
function mw(t, e) {
  var r = e.x, n = e.y, i = e.width, a = e.height, o = e.r, s, u, l, f;
  i < 0 && (r = r + i, i = -i), a < 0 && (n = n + a, a = -a), typeof o == "number" ? s = u = l = f = o : o instanceof Array ? o.length === 1 ? s = u = l = f = o[0] : o.length === 2 ? (s = l = o[0], u = f = o[1]) : o.length === 3 ? (s = o[0], u = f = o[1], l = o[2]) : (s = o[0], u = o[1], l = o[2], f = o[3]) : s = u = l = f = 0;
  var h;
  s + u > i && (h = s + u, s *= i / h, u *= i / h), l + f > i && (h = l + f, l *= i / h, f *= i / h), u + l > a && (h = u + l, u *= a / h, l *= a / h), s + f > a && (h = s + f, s *= a / h, f *= a / h), t.moveTo(r + s, n), t.lineTo(r + i - u, n), u !== 0 && t.arc(r + i - u, n + u, u, -Math.PI / 2, 0), t.lineTo(r + i, n + a - l), l !== 0 && t.arc(r + i - l, n + a - l, l, 0, Math.PI / 2), t.lineTo(r + f, n + a), f !== 0 && t.arc(r + f, n + a - f, f, Math.PI / 2, Math.PI), t.lineTo(r, n + s), s !== 0 && t.arc(r + s, n + s, s, Math.PI, Math.PI * 1.5), t.closePath();
}
var Qn = Math.round;
function ky(t, e, r) {
  if (e) {
    var n = e.x1, i = e.x2, a = e.y1, o = e.y2;
    t.x1 = n, t.x2 = i, t.y1 = a, t.y2 = o;
    var s = r && r.lineWidth;
    return s && (Qn(n * 2) === Qn(i * 2) && (t.x1 = t.x2 = vn(n, s, !0)), Qn(a * 2) === Qn(o * 2) && (t.y1 = t.y2 = vn(a, s, !0))), t;
  }
}
function By(t, e, r) {
  if (e) {
    var n = e.x, i = e.y, a = e.width, o = e.height;
    t.x = n, t.y = i, t.width = a, t.height = o;
    var s = r && r.lineWidth;
    return s && (t.x = vn(n, s, !0), t.y = vn(i, s, !0), t.width = Math.max(vn(n + a, s, !1) - t.x, a === 0 ? 0 : 1), t.height = Math.max(vn(i + o, s, !1) - t.y, o === 0 ? 0 : 1)), t;
  }
}
function vn(t, e, r) {
  if (!e)
    return t;
  var n = Qn(t * 2);
  return (n + Qn(e)) % 2 === 0 ? n / 2 : (n + (r ? 1 : -1)) / 2;
}
var _w = /* @__PURE__ */ (function() {
  function t() {
    this.x = 0, this.y = 0, this.width = 0, this.height = 0;
  }
  return t;
})(), Sw = {}, Ee = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new _w();
  }, e.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = By(Sw, n, this.style);
      i = u.x, a = u.y, o = u.width, s = u.height, u.r = n.r, n = u;
    } else
      i = n.x, a = n.y, o = n.width, s = n.height;
    n.r ? mw(r, n) : r.rect(i, a, o, s);
  }, e.prototype.isZeroArea = function() {
    return !this.shape.width || !this.shape.height;
  }, e;
})(ye);
Ee.prototype.type = "rect";
var Gv = {
  fill: "#000"
}, Uv = 2, Nt = {}, bw = {
  style: _e({
    fill: !0,
    stroke: !0,
    fillOpacity: !0,
    strokeOpacity: !0,
    lineWidth: !0,
    fontSize: !0,
    lineHeight: !0,
    width: !0,
    height: !0,
    textShadowColor: !0,
    textShadowBlur: !0,
    textShadowOffsetX: !0,
    textShadowOffsetY: !0,
    backgroundColor: !0,
    padding: !0,
    borderColor: !0,
    borderWidth: !0,
    borderRadius: !0
  }, Ks.style)
}, je = (function(t) {
  F(e, t);
  function e(r) {
    var n = t.call(this) || this;
    return n.type = "text", n._children = [], n._defaultStyle = Gv, n.attr(r), n;
  }
  return e.prototype.childrenRef = function() {
    return this._children;
  }, e.prototype.update = function() {
    t.prototype.update.call(this), this.styleChanged() && this._updateSubTexts();
    for (var r = 0; r < this._children.length; r++) {
      var n = this._children[r];
      n.zlevel = this.zlevel, n.z = this.z, n.z2 = this.z2, n.culling = this.culling, n.cursor = this.cursor, n.invisible = this.invisible;
    }
  }, e.prototype.updateTransform = function() {
    var r = this.innerTransformable;
    r ? (r.updateTransform(), r.transform && (this.transform = r.transform)) : t.prototype.updateTransform.call(this);
  }, e.prototype.getLocalTransform = function(r) {
    var n = this.innerTransformable;
    return n ? n.getLocalTransform(r) : t.prototype.getLocalTransform.call(this, r);
  }, e.prototype.getComputedTransform = function() {
    return this.__hostTarget && (this.__hostTarget.getComputedTransform(), this.__hostTarget.updateInnerText(!0)), t.prototype.getComputedTransform.call(this);
  }, e.prototype._updateSubTexts = function() {
    this._childCursor = 0, Mw(this.style), this.style.rich ? this._updateRichTexts() : this._updatePlainTexts(), this._children.length = this._childCursor, this.styleUpdated();
  }, e.prototype.addSelfToZr = function(r) {
    t.prototype.addSelfToZr.call(this, r);
    for (var n = 0; n < this._children.length; n++)
      this._children[n].__zr = r;
  }, e.prototype.removeSelfFromZr = function(r) {
    t.prototype.removeSelfFromZr.call(this, r);
    for (var n = 0; n < this._children.length; n++)
      this._children[n].__zr = null;
  }, e.prototype.getBoundingRect = function() {
    if (this.styleChanged() && this._updateSubTexts(), !this._rect) {
      for (var r = new Y(0, 0, 0, 0), n = this._children, i = [], a = null, o = 0; o < n.length; o++) {
        var s = n[o], u = s.getBoundingRect(), l = s.getLocalTransform(i);
        l ? (r.copy(u), r.applyTransform(l), a = a || r.clone(), a.union(r)) : (a = a || u.clone(), a.union(u));
      }
      this._rect = a || r;
    }
    return this._rect;
  }, e.prototype.setDefaultTextStyle = function(r) {
    this._defaultStyle = r || Gv;
  }, e.prototype.setTextContent = function(r) {
  }, e.prototype._mergeStyle = function(r, n) {
    if (!n)
      return r;
    var i = n.rich, a = r.rich || i && {};
    return E(r, n), i && a ? (this._mergeRich(a, i), r.rich = a) : a && (r.rich = a), r;
  }, e.prototype._mergeRich = function(r, n) {
    for (var i = me(n), a = 0; a < i.length; a++) {
      var o = i[a];
      r[o] = r[o] || {}, E(r[o], n[o]);
    }
  }, e.prototype.getAnimationStyleProps = function() {
    return bw;
  }, e.prototype._getOrCreateChild = function(r) {
    var n = this._children[this._childCursor];
    return (!n || !(n instanceof r)) && (n = new r()), this._children[this._childCursor++] = n, n.__zr = this.__zr, n.parent = this, n;
  }, e.prototype._updatePlainTexts = function() {
    var r = this.style, n = r.font || Er, i = r.padding, a = this._defaultStyle, o = r.x || 0, s = r.y || 0, u = r.align || a.align || "left", l = r.verticalAlign || a.verticalAlign || "top";
    Ov(Nt, a.overflowRect, o, s, u, l), o = Nt.baseX, s = Nt.baseY;
    var f = Kv(r), h = GT(f, r, Nt.outerWidth, Nt.outerHeight), c = il(r), v = !!r.backgroundColor, d = h.outerHeight, y = h.outerWidth, p = h.lines, g = h.lineHeight;
    this.isTruncated = !!h.isTruncated;
    var m = o, _ = _n(s, h.contentHeight, l);
    if (c || i) {
      var S = li(o, y, u), b = _n(s, d, l);
      c && this._renderBackground(r, r, S, b, y, d);
    }
    _ += g / 2, i && (m = Zv(o, u, i), l === "top" ? _ += i[0] : l === "bottom" && (_ -= i[2]));
    for (var T = 0, C = !1, M = !1, x = $v("fill" in r ? r.fill : (M = !0, a.fill)), D = qv("stroke" in r ? r.stroke : !v && (!a.autoStroke || M) ? (T = Uv, C = !0, a.stroke) : null), I = r.textShadowBlur > 0, L = 0; L < p.length; L++) {
      var A = this._getOrCreateChild(fs), P = A.createStyle();
      A.useStyle(P), P.text = p[L], P.x = m, P.y = _, P.textAlign = u, P.textBaseline = "middle", P.opacity = r.opacity, P.strokeFirst = !0, I && (P.shadowBlur = r.textShadowBlur || 0, P.shadowColor = r.textShadowColor || "transparent", P.shadowOffsetX = r.textShadowOffsetX || 0, P.shadowOffsetY = r.textShadowOffsetY || 0), P.stroke = D, P.fill = x, D && (P.lineWidth = r.lineWidth || T, P.lineDash = r.lineDash, P.lineDashOffset = r.lineDashOffset || 0), P.font = n, Wv(P, r), _ += g, A.setBoundingRect(yf(P, h.contentWidth, h.calculatedLineHeight, C ? 0 : null));
    }
  }, e.prototype._updateRichTexts = function() {
    var r = this.style, n = this._defaultStyle, i = r.align || n.align, a = r.verticalAlign || n.verticalAlign, o = r.x || 0, s = r.y || 0;
    Ov(Nt, n.overflowRect, o, s, i, a), o = Nt.baseX, s = Nt.baseY;
    var u = Kv(r), l = WT(u, r, Nt.outerWidth, Nt.outerHeight, i), f = l.width, h = l.outerWidth, c = l.outerHeight, v = r.padding;
    this.isTruncated = !!l.isTruncated;
    var d = li(o, h, i), y = _n(s, c, a), p = d, g = y;
    v && (p += v[3], g += v[0]);
    var m = p + f;
    il(r) && this._renderBackground(r, r, d, y, h, c);
    for (var _ = !!r.backgroundColor, S = 0; S < l.lines.length; S++) {
      for (var b = l.lines[S], T = b.tokens, C = T.length, M = b.lineHeight, x = b.width, D = 0, I = p, L = m, A = C - 1, P = void 0; D < C && (P = T[D], !P.align || P.align === "left"); )
        this._placeToken(P, r, M, g, I, "left", _), x -= P.width, I += P.width, D++;
      for (; A >= 0 && (P = T[A], P.align === "right"); )
        this._placeToken(P, r, M, g, L, "right", _), x -= P.width, L -= P.width, A--;
      for (I += (f - (I - p) - (m - L) - x) / 2; D <= A; )
        P = T[D], this._placeToken(P, r, M, g, I + P.width / 2, "center", _), I += P.width, D++;
      g += M;
    }
  }, e.prototype._placeToken = function(r, n, i, a, o, s, u) {
    var l = n.rich[r.styleName] || {};
    l.text = r.text;
    var f = r.verticalAlign, h = a + i / 2;
    f === "top" ? h = a + r.height / 2 : f === "bottom" && (h = a + i - r.height / 2);
    var c = !r.isLineHolder && il(l);
    c && this._renderBackground(l, n, s === "right" ? o - r.width : s === "center" ? o - r.width / 2 : o, h - r.height / 2, r.width, r.height);
    var v = !!l.backgroundColor, d = r.textPadding;
    d && (o = Zv(o, s, d), h -= r.height / 2 - d[0] - r.innerHeight / 2);
    var y = this._getOrCreateChild(fs), p = y.createStyle();
    y.useStyle(p);
    var g = this._defaultStyle, m = !1, _ = 0, S = !1, b = $v("fill" in l ? l.fill : "fill" in n ? n.fill : (m = !0, g.fill)), T = qv("stroke" in l ? l.stroke : "stroke" in n ? n.stroke : !v && !u && (!g.autoStroke || m) ? (_ = Uv, S = !0, g.stroke) : null), C = l.textShadowBlur > 0 || n.textShadowBlur > 0;
    p.text = r.text, p.x = o, p.y = h, C && (p.shadowBlur = l.textShadowBlur || n.textShadowBlur || 0, p.shadowColor = l.textShadowColor || n.textShadowColor || "transparent", p.shadowOffsetX = l.textShadowOffsetX || n.textShadowOffsetX || 0, p.shadowOffsetY = l.textShadowOffsetY || n.textShadowOffsetY || 0), p.textAlign = s, p.textBaseline = "middle", p.font = r.font || Er, p.opacity = gn(l.opacity, n.opacity, 1), Wv(p, l), T && (p.lineWidth = gn(l.lineWidth, n.lineWidth, _), p.lineDash = G(l.lineDash, n.lineDash), p.lineDashOffset = n.lineDashOffset || 0, p.stroke = T), b && (p.fill = b), y.setBoundingRect(yf(p, r.contentWidth, r.contentHeight, S ? 0 : null));
  }, e.prototype._renderBackground = function(r, n, i, a, o, s) {
    var u = r.backgroundColor, l = r.borderWidth, f = r.borderColor, h = u && u.image, c = u && !h, v = r.borderRadius, d = this, y, p;
    if (c || r.lineHeight || l && f) {
      y = this._getOrCreateChild(Ee), y.useStyle(y.createStyle()), y.style.fill = null;
      var g = y.shape;
      g.x = i, g.y = a, g.width = o, g.height = s, g.r = v, y.dirtyShape();
    }
    if (c) {
      var m = y.style;
      m.fill = u || null, m.fillOpacity = G(r.fillOpacity, 1);
    } else if (h) {
      p = this._getOrCreateChild(Nr), p.onload = function() {
        d.dirtyStyle();
      };
      var _ = p.style;
      _.image = u.image, _.x = i, _.y = a, _.width = o, _.height = s;
    }
    if (l && f) {
      var m = y.style;
      m.lineWidth = l, m.stroke = f, m.strokeOpacity = G(r.strokeOpacity, 1), m.lineDash = r.borderDash, m.lineDashOffset = r.borderDashOffset || 0, y.strokeContainThreshold = 0, y.hasFill() && y.hasStroke() && (m.strokeFirst = !0, m.lineWidth *= 2);
    }
    var S = (y || p).style;
    S.shadowBlur = r.shadowBlur || 0, S.shadowColor = r.shadowColor || "transparent", S.shadowOffsetX = r.shadowOffsetX || 0, S.shadowOffsetY = r.shadowOffsetY || 0, S.opacity = gn(r.opacity, n.opacity, 1);
  }, e.makeFont = function(r) {
    var n = "";
    return xw(r) && (n = [
      r.fontStyle,
      r.fontWeight,
      Cw(r.fontSize),
      r.fontFamily || "sans-serif"
    ].join(" ")), n && It(n) || r.textFont || r.font;
  }, e;
})(Ba), Tw = { left: !0, right: 1, center: 1 }, ww = { top: 1, bottom: 1, middle: 1 }, Yv = ["fontStyle", "fontWeight", "fontSize", "fontFamily"];
function Cw(t) {
  return typeof t == "string" && (t.indexOf("px") !== -1 || t.indexOf("rem") !== -1 || t.indexOf("em") !== -1) ? t : isNaN(+t) ? uh + "px" : t + "px";
}
function Wv(t, e) {
  for (var r = 0; r < Yv.length; r++) {
    var n = Yv[r], i = e[n];
    i != null && (t[n] = i);
  }
}
function xw(t) {
  return t.fontSize != null || t.fontFamily || t.fontWeight;
}
function Mw(t) {
  return Xv(t), w(t.rich, Xv), t;
}
function Xv(t) {
  if (t) {
    t.font = je.makeFont(t);
    var e = t.align;
    e === "middle" && (e = "center"), t.align = e == null || Tw[e] ? e : "left";
    var r = t.verticalAlign;
    r === "center" && (r = "middle"), t.verticalAlign = r == null || ww[r] ? r : "top";
    var n = t.padding;
    n && (t.padding = Hs(t.padding));
  }
}
function qv(t, e) {
  return t == null || e <= 0 || t === "transparent" || t === "none" ? null : t.image || t.colorStops ? "#000" : t;
}
function $v(t) {
  return t == null || t === "none" ? null : t.image || t.colorStops ? "#000" : t;
}
function Zv(t, e, r) {
  return e === "right" ? t - r[1] : e === "center" ? t + r[3] / 2 - r[1] / 2 : t + r[3];
}
function Kv(t) {
  var e = t.text;
  return e != null && (e += ""), e;
}
function il(t) {
  return !!(t.backgroundColor || t.lineHeight || t.borderWidth && t.borderColor);
}
var he = de(), Dw = function(t, e, r, n) {
  if (n) {
    var i = he(n);
    i.dataIndex = r, i.dataType = e, i.seriesIndex = t, i.ssrType = "chart", n.type === "group" && n.traverse(function(a) {
      var o = he(a);
      o.seriesIndex = t, o.dataIndex = r, o.dataType = e, o.ssrType = "chart";
    });
  }
}, js = "undefined", Ny = "series", Fy = U(["tooltip", "label", "itemName", "itemId", "itemGroupId", "itemChildGroupId", "seriesName"]), ft = "original", ze = "arrayRows", Rt = "objectRows", jt = "keyedColumns", Lr = "typedArray", zy = "unknown", Pt = "column", An = "row", Iw = [
  "getDom",
  "getZr",
  "getWidth",
  "getHeight",
  "getDevicePixelRatio",
  "dispatchAction",
  "isSSR",
  "isDisposed",
  "on",
  "off",
  "getDataURL",
  "getConnectedDataURL",
  // 'getModel',
  "getOption",
  // 'getViewOfComponentModel',
  // 'getViewOfSeriesModel',
  "getId",
  "updateLabelLayout"
], Hy = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      w(Iw, function(r) {
        this[r] = le(e[r], e);
      }, this);
    }
    return t;
  })()
);
function Aw(t, e) {
  return e.mainType === Ny ? t.getViewOfSeriesModel(e) : t.getViewOfComponentModel(e);
}
var jv = 1, Qv = {}, Vy = de(), Ih = de(), Gy = 0, Ah = 1, Ph = 2, Or = ["emphasis", "blur", "select"], Jv = ["normal", "emphasis", "blur", "select"], Pw = 10, Lw = 9, bn = "highlight", Go = "downplay", hs = "select", Sf = "unselect", cs = "toggleSelect", Lh = "selectchanged";
function Nn(t) {
  return t != null && t !== "none";
}
function Qs(t, e, r) {
  t.onHoverStateChange && (t.hoverState || 0) !== r && t.onHoverStateChange(e), t.hoverState = r;
}
function Uy(t) {
  Qs(t, "emphasis", Ph);
}
function Yy(t) {
  t.hoverState === Ph && Qs(t, "normal", Gy);
}
function Rh(t) {
  Qs(t, "blur", Ah);
}
function Wy(t) {
  t.hoverState === Ah && Qs(t, "normal", Gy);
}
function Rw(t) {
  t.selected = !0;
}
function Ew(t) {
  t.selected = !1;
}
function ed(t, e, r) {
  e(t, r);
}
function gr(t, e, r) {
  ed(t, e, r), t.isGroup && t.traverse(function(n) {
    ed(n, e, r);
  });
}
function Ow(t, e, r, n) {
  for (var i = t.style, a = {}, o = 0; o < e.length; o++) {
    var s = e[o], u = i[s];
    a[s] = u ?? (n && n[s]);
  }
  for (var o = 0; o < t.animators.length; o++) {
    var l = t.animators[o];
    l.__fromStateTransition && l.__fromStateTransition.indexOf(r) < 0 && l.targetName === "style" && l.saveTo(a, e);
  }
  return a;
}
function kw(t, e, r, n) {
  var i = r && oe(r, "select") >= 0, a = !1;
  if (t instanceof ye) {
    var o = Vy(t), s = i && o.selectFill || o.normalFill, u = i && o.selectStroke || o.normalStroke;
    if (Nn(s) || Nn(u)) {
      n = n || {};
      var l = n.style || {};
      l.fill === "inherit" ? (a = !0, n = E({}, n), l = E({}, l), l.fill = s) : !Nn(l.fill) && Nn(s) ? (a = !0, n = E({}, n), l = E({}, l), l.fill = nf(s)) : !Nn(l.stroke) && Nn(u) && (a || (n = E({}, n), l = E({}, l)), l.stroke = nf(u)), n.style = l;
    }
  }
  if (n && n.z2 == null) {
    a || (n = E({}, n));
    var f = t.z2EmphasisLift;
    n.z2 = t.z2 + (f ?? Pw);
  }
  return n;
}
function Bw(t, e, r) {
  if (r && r.z2 == null) {
    r = E({}, r);
    var n = t.z2SelectLift;
    r.z2 = t.z2 + (n ?? Lw);
  }
  return r;
}
function Nw(t, e, r) {
  var n = oe(t.currentStates, e) >= 0, i = t.style.opacity, a = n ? null : Ow(t, ["opacity"], e, {
    opacity: 1
  });
  r = r || {};
  var o = r.style || {};
  return o.opacity == null && (r = E({}, r), o = E({
    // Already being applied 'emphasis'. DON'T mul opacity multiple times.
    opacity: n ? i : a.opacity * 0.1
  }, o), r.style = o), r;
}
function al(t, e) {
  var r = this.states[t];
  if (this.style) {
    if (t === "emphasis")
      return kw(this, t, e, r);
    if (t === "blur")
      return Nw(this, t, r);
    if (t === "select")
      return Bw(this, t, r);
  }
  return r;
}
function Fw(t) {
  t.stateProxy = al;
  var e = t.getTextContent(), r = t.getTextGuideLine();
  e && (e.stateProxy = al), r && (r.stateProxy = al);
}
function td(t, e) {
  !Zy(t, e) && !t.__highByOuter && gr(t, Uy);
}
function rd(t, e) {
  !Zy(t, e) && !t.__highByOuter && gr(t, Yy);
}
function bf(t, e) {
  t.__highByOuter |= 1 << (e || 0), gr(t, Uy);
}
function Tf(t, e) {
  !(t.__highByOuter &= ~(1 << (e || 0))) && gr(t, Yy);
}
function zw(t) {
  gr(t, Rh);
}
function Xy(t) {
  gr(t, Wy);
}
function qy(t) {
  gr(t, Rw);
}
function $y(t) {
  gr(t, Ew);
}
function Zy(t, e) {
  return t.__highDownSilentOnTouch && e.zrByTouch;
}
function Ky(t) {
  var e = t.getModel(), r = [], n = [];
  e.eachComponent(function(i, a) {
    var o = Ih(a), s = Aw(t, a), u = i === "series";
    !u && n.push(s), o.isBlured && (s.group.traverse(function(l) {
      Wy(l);
    }), u && r.push(a)), o.isBlured = !1;
  }), w(n, function(i) {
    i && i.toggleBlurSeries && i.toggleBlurSeries(r, !1, e);
  });
}
function wf(t, e, r, n) {
  var i = n.getModel();
  r = r || "coordinateSystem";
  function a(l, f) {
    for (var h = 0; h < f.length; h++) {
      var c = l.getItemGraphicEl(f[h]);
      c && Xy(c);
    }
  }
  if (t != null && !(!e || e === "none")) {
    var o = i.getSeriesByIndex(t), s = o.coordinateSystem;
    s && s.master && (s = s.master);
    var u = [];
    i.eachSeries(function(l) {
      var f = o === l, h = l.coordinateSystem;
      h && h.master && (h = h.master);
      var c = h && s ? h === s : f;
      if (!// Not blur other series if blurScope series
      (r === "series" && !f || r === "coordinateSystem" && !c || e === "series" && f)) {
        var v = n.getViewOfSeriesModel(l);
        if (v.group.traverse(function(p) {
          p.__highByOuter && f && e === "self" || Rh(p);
        }), Ze(e))
          a(l.getData(), e);
        else if (z(e))
          for (var d = me(e), y = 0; y < d.length; y++)
            a(l.getData(d[y]), e[d[y]]);
        u.push(l), Ih(l).isBlured = !0;
      }
    }), i.eachComponent(function(l, f) {
      if (l !== "series") {
        var h = n.getViewOfComponentModel(f);
        h && h.toggleBlurSeries && h.toggleBlurSeries(u, !0, i);
      }
    });
  }
}
function Cf(t, e, r) {
  if (!(t == null || e == null)) {
    var n = r.getModel().getComponent(t, e);
    if (n) {
      Ih(n).isBlured = !0;
      var i = r.getViewOfComponentModel(n);
      !i || !i.focusBlurEnabled || i.group.traverse(function(a) {
        Rh(a);
      });
    }
  }
}
function Hw(t, e, r) {
  var n = t.seriesIndex, i = t.getData(e.dataType);
  if (i) {
    var a = Oa(i, e);
    a = (B(a) ? a[0] : a) || 0;
    var o = i.getItemGraphicEl(a);
    if (!o)
      for (var s = i.count(), u = 0; !o && u < s; )
        o = i.getItemGraphicEl(u++);
    if (o) {
      var l = he(o);
      wf(n, l.focus, l.blurScope, r);
    } else {
      var f = t.get(["emphasis", "focus"]), h = t.get(["emphasis", "blurScope"]);
      f != null && wf(n, f, h, r);
    }
  }
}
function Eh(t, e, r, n) {
  var i = {
    focusSelf: !1,
    dispatchers: null
  };
  if (t == null || t === "series" || e == null || r == null)
    return i;
  var a = n.getModel().getComponent(t, e);
  if (!a)
    return i;
  var o = n.getViewOfComponentModel(a);
  if (!o || !o.findHighDownDispatchers)
    return i;
  for (var s = o.findHighDownDispatchers(r), u, l = 0; l < s.length; l++)
    if (he(s[l]).focus === "self") {
      u = !0;
      break;
    }
  return {
    focusSelf: u,
    dispatchers: s
  };
}
function Vw(t, e, r) {
  var n = he(t), i = Eh(n.componentMainType, n.componentIndex, n.componentHighDownName, r), a = i.dispatchers, o = i.focusSelf;
  a ? (o && Cf(n.componentMainType, n.componentIndex, r), w(a, function(s) {
    return td(s, e);
  })) : (wf(n.seriesIndex, n.focus, n.blurScope, r), n.focus === "self" && Cf(n.componentMainType, n.componentIndex, r), td(t, e));
}
function Gw(t, e, r) {
  Ky(r);
  var n = he(t), i = Eh(n.componentMainType, n.componentIndex, n.componentHighDownName, r).dispatchers;
  i ? w(i, function(a) {
    return rd(a, e);
  }) : rd(t, e);
}
function Uw(t, e, r) {
  if (Mf(e)) {
    var n = e.dataType, i = t.getData(n), a = Oa(i, e);
    B(a) || (a = [a]), t[e.type === cs ? "toggleSelect" : e.type === hs ? "select" : "unselect"](a, n);
  }
}
function nd(t) {
  var e = t.getAllData();
  w(e, function(r) {
    var n = r.data, i = r.type;
    n.eachItemGraphicEl(function(a, o) {
      t.isSelected(o, i) ? qy(a) : $y(a);
    });
  });
}
function Yw(t) {
  var e = [];
  return t.eachSeries(function(r) {
    var n = r.getAllData();
    w(n, function(i) {
      i.data;
      var a = i.type, o = r.getSelectedDataIndices();
      if (o.length > 0) {
        var s = {
          dataIndex: o,
          seriesIndex: r.seriesIndex
        };
        a != null && (s.dataType = a), e.push(s);
      }
    });
  }), e;
}
function vs(t, e, r) {
  jy(t, !0), gr(t, Fw), Xw(t, e, r);
}
function Ww(t) {
  jy(t, !1);
}
function AR(t, e, r, n) {
  n ? Ww(t) : vs(t, e, r);
}
function Xw(t, e, r) {
  var n = he(t);
  e != null ? (n.focus = e, n.blurScope = r) : n.focus && (n.focus = null);
}
var id = ["emphasis", "blur", "select"], qw = {
  itemStyle: "getItemStyle",
  lineStyle: "getLineStyle",
  areaStyle: "getAreaStyle"
};
function PR(t, e, r, n) {
  r = r || "itemStyle";
  for (var i = 0; i < id.length; i++) {
    var a = id[i], o = e.getModel([a, r]), s = t.ensureState(a);
    s.style = o[qw[r]]();
  }
}
function jy(t, e) {
  var r = e === !1, n = t;
  t.highDownSilentOnTouch && (n.__highDownSilentOnTouch = t.highDownSilentOnTouch), (!r || n.__highDownDispatcher) && (n.__highByOuter = n.__highByOuter || 0, n.__highDownDispatcher = !r);
}
function xf(t) {
  return !!(t && t.__highDownDispatcher);
}
function $w(t) {
  var e = Qv[t];
  return e == null && jv <= 32 && (e = Qv[t] = jv++), e;
}
function Mf(t) {
  var e = t.type;
  return e === hs || e === Sf || e === cs;
}
function ad(t) {
  var e = t.type;
  return e === bn || e === Go;
}
function Zw(t) {
  var e = Vy(t);
  e.normalFill = t.style.fill, e.normalStroke = t.style.stroke;
  var r = t.states.select || {};
  e.selectFill = r.style && r.style.fill || null, e.selectStroke = r.style && r.style.stroke || null;
}
var Fn = ci.CMD, Kw = [[], [], []], od = Math.sqrt, jw = Math.atan2;
function Qw(t, e) {
  if (e) {
    var r = t.data, n = t.len(), i, a, o, s, u, l, f = Fn.M, h = Fn.C, c = Fn.L, v = Fn.R, d = Fn.A, y = Fn.Q;
    for (o = 0, s = 0; o < n; ) {
      switch (i = r[o++], s = o, a = 0, i) {
        case f:
          a = 1;
          break;
        case c:
          a = 1;
          break;
        case h:
          a = 3;
          break;
        case y:
          a = 2;
          break;
        case d:
          var p = e[4], g = e[5], m = od(e[0] * e[0] + e[1] * e[1]), _ = od(e[2] * e[2] + e[3] * e[3]), S = jw(-e[1] / _, e[0] / m);
          r[o] *= m, r[o++] += p, r[o] *= _, r[o++] += g, r[o++] *= m, r[o++] *= _, r[o++] += S, r[o++] += S, o += 2, s = o;
          break;
        case v:
          l[0] = r[o++], l[1] = r[o++], ot(l, l, e), r[s++] = l[0], r[s++] = l[1], l[0] += r[o++], l[1] += r[o++], ot(l, l, e), r[s++] = l[0], r[s++] = l[1];
      }
      for (u = 0; u < a; u++) {
        var b = Kw[u];
        b[0] = r[o++], b[1] = r[o++], ot(b, b, e), r[s++] = b[0], r[s++] = b[1];
      }
    }
    t.increaseVersion();
  }
}
var ol = Math.sqrt, ro = Math.sin, no = Math.cos, Ri = Math.PI;
function sd(t) {
  return Math.sqrt(t[0] * t[0] + t[1] * t[1]);
}
function Df(t, e) {
  return (t[0] * e[0] + t[1] * e[1]) / (sd(t) * sd(e));
}
function ud(t, e) {
  return (t[0] * e[1] < t[1] * e[0] ? -1 : 1) * Math.acos(Df(t, e));
}
function ld(t, e, r, n, i, a, o, s, u, l, f) {
  var h = u * (Ri / 180), c = no(h) * (t - r) / 2 + ro(h) * (e - n) / 2, v = -1 * ro(h) * (t - r) / 2 + no(h) * (e - n) / 2, d = c * c / (o * o) + v * v / (s * s);
  d > 1 && (o *= ol(d), s *= ol(d));
  var y = (i === a ? -1 : 1) * ol((o * o * (s * s) - o * o * (v * v) - s * s * (c * c)) / (o * o * (v * v) + s * s * (c * c))) || 0, p = y * o * v / s, g = y * -s * c / o, m = (t + r) / 2 + no(h) * p - ro(h) * g, _ = (e + n) / 2 + ro(h) * p + no(h) * g, S = ud([1, 0], [(c - p) / o, (v - g) / s]), b = [(c - p) / o, (v - g) / s], T = [(-1 * c - p) / o, (-1 * v - g) / s], C = ud(b, T);
  if (Df(b, T) <= -1 && (C = Ri), Df(b, T) >= 1 && (C = 0), C < 0) {
    var M = Math.round(C / Ri * 1e6) / 1e6;
    C = Ri * 2 + M % 2 * Ri;
  }
  f.addData(l, m, _, o, s, S, C, h, a);
}
var Jw = /([mlvhzcqtsa])([^mlvhzcqtsa]*)/ig, eC = /-?([0-9]*\.)?[0-9]+([eE]-?[0-9]+)?/g;
function tC(t) {
  var e = new ci();
  if (!t)
    return e;
  var r = 0, n = 0, i = r, a = n, o, s = ci.CMD, u = t.match(Jw);
  if (!u)
    return e;
  for (var l = 0; l < u.length; l++) {
    for (var f = u[l], h = f.charAt(0), c = void 0, v = f.match(eC) || [], d = v.length, y = 0; y < d; y++)
      v[y] = parseFloat(v[y]);
    for (var p = 0; p < d; ) {
      var g = void 0, m = void 0, _ = void 0, S = void 0, b = void 0, T = void 0, C = void 0, M = r, x = n, D = void 0, I = void 0;
      switch (h) {
        case "l":
          r += v[p++], n += v[p++], c = s.L, e.addData(c, r, n);
          break;
        case "L":
          r = v[p++], n = v[p++], c = s.L, e.addData(c, r, n);
          break;
        case "m":
          r += v[p++], n += v[p++], c = s.M, e.addData(c, r, n), i = r, a = n, h = "l";
          break;
        case "M":
          r = v[p++], n = v[p++], c = s.M, e.addData(c, r, n), i = r, a = n, h = "L";
          break;
        case "h":
          r += v[p++], c = s.L, e.addData(c, r, n);
          break;
        case "H":
          r = v[p++], c = s.L, e.addData(c, r, n);
          break;
        case "v":
          n += v[p++], c = s.L, e.addData(c, r, n);
          break;
        case "V":
          n = v[p++], c = s.L, e.addData(c, r, n);
          break;
        case "C":
          c = s.C, e.addData(c, v[p++], v[p++], v[p++], v[p++], v[p++], v[p++]), r = v[p - 2], n = v[p - 1];
          break;
        case "c":
          c = s.C, e.addData(c, v[p++] + r, v[p++] + n, v[p++] + r, v[p++] + n, v[p++] + r, v[p++] + n), r += v[p - 2], n += v[p - 1];
          break;
        case "S":
          g = r, m = n, D = e.len(), I = e.data, o === s.C && (g += r - I[D - 4], m += n - I[D - 3]), c = s.C, M = v[p++], x = v[p++], r = v[p++], n = v[p++], e.addData(c, g, m, M, x, r, n);
          break;
        case "s":
          g = r, m = n, D = e.len(), I = e.data, o === s.C && (g += r - I[D - 4], m += n - I[D - 3]), c = s.C, M = r + v[p++], x = n + v[p++], r += v[p++], n += v[p++], e.addData(c, g, m, M, x, r, n);
          break;
        case "Q":
          M = v[p++], x = v[p++], r = v[p++], n = v[p++], c = s.Q, e.addData(c, M, x, r, n);
          break;
        case "q":
          M = v[p++] + r, x = v[p++] + n, r += v[p++], n += v[p++], c = s.Q, e.addData(c, M, x, r, n);
          break;
        case "T":
          g = r, m = n, D = e.len(), I = e.data, o === s.Q && (g += r - I[D - 4], m += n - I[D - 3]), r = v[p++], n = v[p++], c = s.Q, e.addData(c, g, m, r, n);
          break;
        case "t":
          g = r, m = n, D = e.len(), I = e.data, o === s.Q && (g += r - I[D - 4], m += n - I[D - 3]), r += v[p++], n += v[p++], c = s.Q, e.addData(c, g, m, r, n);
          break;
        case "A":
          _ = v[p++], S = v[p++], b = v[p++], T = v[p++], C = v[p++], M = r, x = n, r = v[p++], n = v[p++], c = s.A, ld(M, x, r, n, T, C, _, S, b, c, e);
          break;
        case "a":
          _ = v[p++], S = v[p++], b = v[p++], T = v[p++], C = v[p++], M = r, x = n, r += v[p++], n += v[p++], c = s.A, ld(M, x, r, n, T, C, _, S, b, c, e);
          break;
      }
    }
    (h === "z" || h === "Z") && (c = s.Z, e.addData(c), r = i, n = a), o = c;
  }
  return e.toStatic(), e;
}
var Qy = (function(t) {
  F(e, t);
  function e() {
    return t !== null && t.apply(this, arguments) || this;
  }
  return e.prototype.applyTransform = function(r) {
  }, e;
})(ye);
function Jy(t) {
  return t.setData != null;
}
function em(t, e) {
  var r = tC(t), n = E({}, e);
  return n.buildPath = function(i) {
    var a = Jy(i);
    if (a && i.canSave()) {
      i.appendPath(r);
      var o = i.getContext();
      o && i.rebuildPath(o, 1);
    } else {
      var o = a ? i.getContext() : i;
      o && r.rebuildPath(o, 1);
    }
  }, n.applyTransform = function(i) {
    Qw(r, i), this.dirtyShape();
  }, n;
}
function rC(t, e) {
  return new Qy(em(t, e));
}
function nC(t, e) {
  var r = em(t, e), n = (function(i) {
    F(a, i);
    function a(o) {
      var s = i.call(this, o) || this;
      return s.applyTransform = r.applyTransform, s.buildPath = r.buildPath, s;
    }
    return a;
  })(Qy);
  return n;
}
function iC(t, e) {
  for (var r = [], n = t.length, i = 0; i < n; i++) {
    var a = t[i];
    r.push(a.getUpdatedPathProxy(!0));
  }
  var o = new ye(e);
  return o.createPathProxy(), o.buildPath = function(s) {
    if (Jy(s)) {
      s.appendPath(r);
      var u = s.getContext();
      u && s.rebuildPath(u, 1);
    }
  }, o;
}
var aC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r = 0;
  }
  return t;
})(), Na = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new aC();
  }, e.prototype.buildPath = function(r, n) {
    r.moveTo(n.cx + n.r, n.cy), r.arc(n.cx, n.cy, n.r, 0, Math.PI * 2);
  }, e;
})(ye);
Na.prototype.type = "circle";
var oC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.rx = 0, this.ry = 0;
  }
  return t;
})(), Js = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new oC();
  }, e.prototype.buildPath = function(r, n) {
    var i = 0.5522848, a = n.cx, o = n.cy, s = n.rx, u = n.ry, l = s * i, f = u * i;
    r.moveTo(a - s, o), r.bezierCurveTo(a - s, o - f, a - l, o - u, a, o - u), r.bezierCurveTo(a + l, o - u, a + s, o - f, a + s, o), r.bezierCurveTo(a + s, o + f, a + l, o + u, a, o + u), r.bezierCurveTo(a - l, o + u, a - s, o + f, a - s, o), r.closePath();
  }, e;
})(ye);
Js.prototype.type = "ellipse";
var tm = Math.PI, sl = tm * 2, Jr = Math.sin, zn = Math.cos, sC = Math.acos, Oe = Math.atan2, fd = Math.abs, ra = Math.sqrt, Zi = Math.max, Ft = Math.min, xt = 1e-4;
function uC(t, e, r, n, i, a, o, s) {
  var u = r - t, l = n - e, f = o - i, h = s - a, c = h * u - f * l;
  if (!(c * c < xt))
    return c = (f * (e - a) - h * (t - i)) / c, [t + c * u, e + c * l];
}
function io(t, e, r, n, i, a, o) {
  var s = t - r, u = e - n, l = (o ? a : -a) / ra(s * s + u * u), f = l * u, h = -l * s, c = t + f, v = e + h, d = r + f, y = n + h, p = (c + d) / 2, g = (v + y) / 2, m = d - c, _ = y - v, S = m * m + _ * _, b = i - a, T = c * y - d * v, C = (_ < 0 ? -1 : 1) * ra(Zi(0, b * b * S - T * T)), M = (T * _ - m * C) / S, x = (-T * m - _ * C) / S, D = (T * _ + m * C) / S, I = (-T * m + _ * C) / S, L = M - p, A = x - g, P = D - p, k = I - g;
  return L * L + A * A > P * P + k * k && (M = D, x = I), {
    cx: M,
    cy: x,
    x0: -f,
    y0: -h,
    x1: M * (i / b - 1),
    y1: x * (i / b - 1)
  };
}
function lC(t) {
  var e;
  if (B(t)) {
    var r = t.length;
    if (!r)
      return t;
    r === 1 ? e = [t[0], t[0], 0, 0] : r === 2 ? e = [t[0], t[0], t[1], t[1]] : r === 3 ? e = t.concat(t[2]) : e = t;
  } else
    e = [t, t, t, t];
  return e;
}
function fC(t, e) {
  var r, n = Zi(e.r, 0), i = Zi(e.r0 || 0, 0), a = n > 0, o = i > 0;
  if (!(!a && !o)) {
    if (a || (n = i, i = 0), i > n) {
      var s = n;
      n = i, i = s;
    }
    var u = e.startAngle, l = e.endAngle;
    if (!(isNaN(u) || isNaN(l))) {
      var f = e.cx, h = e.cy, c = !!e.clockwise, v = fd(l - u), d = v > sl && v % sl;
      if (d > xt && (v = d), !(n > xt))
        t.moveTo(f, h);
      else if (v > sl - xt)
        t.moveTo(f + n * zn(u), h + n * Jr(u)), t.arc(f, h, n, u, l, !c), i > xt && (t.moveTo(f + i * zn(l), h + i * Jr(l)), t.arc(f, h, i, l, u, c));
      else {
        var y = void 0, p = void 0, g = void 0, m = void 0, _ = void 0, S = void 0, b = void 0, T = void 0, C = void 0, M = void 0, x = void 0, D = void 0, I = void 0, L = void 0, A = void 0, P = void 0, k = n * zn(u), O = n * Jr(u), V = i * zn(l), K = i * Jr(l), J = v > xt;
        if (J) {
          var ee = e.cornerRadius;
          ee && (r = lC(ee), y = r[0], p = r[1], g = r[2], m = r[3]);
          var Q = fd(n - i) / 2;
          if (_ = Ft(Q, g), S = Ft(Q, m), b = Ft(Q, y), T = Ft(Q, p), x = C = Zi(_, S), D = M = Zi(b, T), (C > xt || M > xt) && (I = n * zn(l), L = n * Jr(l), A = i * zn(u), P = i * Jr(u), v < tm)) {
            var re = uC(k, O, A, P, I, L, V, K);
            if (re) {
              var ne = k - re[0], q = O - re[1], se = I - re[0], Se = L - re[1], tt = 1 / Jr(sC((ne * se + q * Se) / (ra(ne * ne + q * q) * ra(se * se + Se * Se))) / 2), Qt = ra(re[0] * re[0] + re[1] * re[1]);
              x = Ft(C, (n - Qt) / (tt + 1)), D = Ft(M, (i - Qt) / (tt - 1));
            }
          }
        }
        if (!J)
          t.moveTo(f + k, h + O);
        else if (x > xt) {
          var rt = Ft(g, x), Ae = Ft(m, x), $ = io(A, P, k, O, n, rt, c), te = io(I, L, V, K, n, Ae, c);
          t.moveTo(f + $.cx + $.x0, h + $.cy + $.y0), x < C && rt === Ae ? t.arc(f + $.cx, h + $.cy, x, Oe($.y0, $.x0), Oe(te.y0, te.x0), !c) : (rt > 0 && t.arc(f + $.cx, h + $.cy, rt, Oe($.y0, $.x0), Oe($.y1, $.x1), !c), t.arc(f, h, n, Oe($.cy + $.y1, $.cx + $.x1), Oe(te.cy + te.y1, te.cx + te.x1), !c), Ae > 0 && t.arc(f + te.cx, h + te.cy, Ae, Oe(te.y1, te.x1), Oe(te.y0, te.x0), !c));
        } else
          t.moveTo(f + k, h + O), t.arc(f, h, n, u, l, !c);
        if (!(i > xt) || !J)
          t.lineTo(f + V, h + K);
        else if (D > xt) {
          var rt = Ft(y, D), Ae = Ft(p, D), $ = io(V, K, I, L, i, -Ae, c), te = io(k, O, A, P, i, -rt, c);
          t.lineTo(f + $.cx + $.x0, h + $.cy + $.y0), D < M && rt === Ae ? t.arc(f + $.cx, h + $.cy, D, Oe($.y0, $.x0), Oe(te.y0, te.x0), !c) : (Ae > 0 && t.arc(f + $.cx, h + $.cy, Ae, Oe($.y0, $.x0), Oe($.y1, $.x1), !c), t.arc(f, h, i, Oe($.cy + $.y1, $.cx + $.x1), Oe(te.cy + te.y1, te.cx + te.x1), c), rt > 0 && t.arc(f + te.cx, h + te.cy, rt, Oe(te.y1, te.x1), Oe(te.y0, te.x0), !c));
        } else
          t.lineTo(f + V, h + K), t.arc(f, h, i, l, u, c);
      }
      t.closePath();
    }
  }
}
var hC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r0 = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0, this.cornerRadius = 0;
  }
  return t;
})(), eu = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new hC();
  }, e.prototype.buildPath = function(r, n) {
    fC(r, n);
  }, e.prototype.isZeroArea = function() {
    return this.shape.startAngle === this.shape.endAngle || this.shape.r === this.shape.r0;
  }, e;
})(ye);
eu.prototype.type = "sector";
var cC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r = 0, this.r0 = 0;
  }
  return t;
})(), tu = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new cC();
  }, e.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.PI * 2;
    r.moveTo(i + n.r, a), r.arc(i, a, n.r, 0, o, !1), r.moveTo(i + n.r0, a), r.arc(i, a, n.r0, 0, o, !0);
  }, e;
})(ye);
tu.prototype.type = "ring";
function vC(t, e, r, n) {
  var i = [], a = [], o = [], s = [], u, l, f, h;
  if (n) {
    f = [1 / 0, 1 / 0], h = [-1 / 0, -1 / 0];
    for (var c = 0, v = t.length; c < v; c++)
      Cr(f, f, t[c]), xr(h, h, t[c]);
    Cr(f, f, n[0]), xr(h, h, n[1]);
  }
  for (var c = 0, v = t.length; c < v; c++) {
    var d = t[c];
    if (r)
      u = t[c ? c - 1 : v - 1], l = t[(c + 1) % v];
    else if (c === 0 || c === v - 1) {
      i.push(Ng(t[c]));
      continue;
    } else
      u = t[c - 1], l = t[c + 1];
    Fg(a, l, u), Eo(a, a, e);
    var y = Jo(d, u), p = Jo(d, l), g = y + p;
    g !== 0 && (y /= g, p /= g), Eo(o, a, -y), Eo(s, a, p);
    var m = $l([], d, o), _ = $l([], d, s);
    n && (xr(m, m, f), Cr(m, m, h), xr(_, _, f), Cr(_, _, h)), i.push(m), i.push(_);
  }
  return r && i.push(i.shift()), i;
}
function rm(t, e, r) {
  var n = e.smooth, i = e.points;
  if (i && i.length >= 2) {
    if (n) {
      var a = vC(i, n, r, e.smoothConstraint);
      t.moveTo(i[0][0], i[0][1]);
      for (var o = i.length, s = 0; s < (r ? o : o - 1); s++) {
        var u = a[s * 2], l = a[s * 2 + 1], f = i[(s + 1) % o];
        t.bezierCurveTo(u[0], u[1], l[0], l[1], f[0], f[1]);
      }
    } else {
      t.moveTo(i[0][0], i[0][1]);
      for (var s = 1, h = i.length; s < h; s++)
        t.lineTo(i[s][0], i[s][1]);
    }
    r && t.closePath();
  }
}
var dC = /* @__PURE__ */ (function() {
  function t() {
    this.points = null, this.smooth = 0, this.smoothConstraint = null;
  }
  return t;
})(), ru = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new dC();
  }, e.prototype.buildPath = function(r, n) {
    rm(r, n, !0);
  }, e;
})(ye);
ru.prototype.type = "polygon";
var pC = /* @__PURE__ */ (function() {
  function t() {
    this.points = null, this.percent = 1, this.smooth = 0, this.smoothConstraint = null;
  }
  return t;
})(), nu = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new pC();
  }, e.prototype.buildPath = function(r, n) {
    rm(r, n, !1);
  }, e;
})(ye);
nu.prototype.type = "polyline";
var gC = {}, yC = /* @__PURE__ */ (function() {
  function t() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.percent = 1;
  }
  return t;
})(), Pn = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new yC();
  }, e.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = ky(gC, n, this.style);
      i = u.x1, a = u.y1, o = u.x2, s = u.y2;
    } else
      i = n.x1, a = n.y1, o = n.x2, s = n.y2;
    var l = n.percent;
    l !== 0 && (r.moveTo(i, a), l < 1 && (o = i * (1 - l) + o * l, s = a * (1 - l) + s * l), r.lineTo(o, s));
  }, e.prototype.pointAt = function(r) {
    var n = this.shape;
    return [
      n.x1 * (1 - r) + n.x2 * r,
      n.y1 * (1 - r) + n.y2 * r
    ];
  }, e;
})(ye);
Pn.prototype.type = "line";
var Qe = [], mC = /* @__PURE__ */ (function() {
  function t() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.cpx1 = 0, this.cpy1 = 0, this.percent = 1;
  }
  return t;
})();
function hd(t, e, r) {
  var n = t.cpx2, i = t.cpy2;
  return n != null || i != null ? [
    (r ? fv : Pe)(t.x1, t.cpx1, t.cpx2, t.x2, e),
    (r ? fv : Pe)(t.y1, t.cpy1, t.cpy2, t.y2, e)
  ] : [
    (r ? hv : Be)(t.x1, t.cpx1, t.x2, e),
    (r ? hv : Be)(t.y1, t.cpy1, t.y2, e)
  ];
}
var iu = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new mC();
  }, e.prototype.buildPath = function(r, n) {
    var i = n.x1, a = n.y1, o = n.x2, s = n.y2, u = n.cpx1, l = n.cpy1, f = n.cpx2, h = n.cpy2, c = n.percent;
    c !== 0 && (r.moveTo(i, a), f == null || h == null ? (c < 1 && (as(i, u, o, c, Qe), u = Qe[1], o = Qe[2], as(a, l, s, c, Qe), l = Qe[1], s = Qe[2]), r.quadraticCurveTo(u, l, o, s)) : (c < 1 && (is(i, u, f, o, c, Qe), u = Qe[1], f = Qe[2], o = Qe[3], is(a, l, h, s, c, Qe), l = Qe[1], h = Qe[2], s = Qe[3]), r.bezierCurveTo(u, l, f, h, o, s)));
  }, e.prototype.pointAt = function(r) {
    return hd(this.shape, r, !1);
  }, e.prototype.tangentAt = function(r) {
    var n = hd(this.shape, r, !0);
    return zg(n, n);
  }, e;
})(ye);
iu.prototype.type = "bezier-curve";
var _C = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0;
  }
  return t;
})(), Fa = (function(t) {
  F(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new _C();
  }, e.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.max(n.r, 0), s = n.startAngle, u = n.endAngle, l = n.clockwise, f = Math.cos(s), h = Math.sin(s);
    r.moveTo(f * o + i, h * o + a), r.arc(i, a, o, s, u, !l);
  }, e;
})(ye);
Fa.prototype.type = "arc";
var nm = (function(t) {
  F(e, t);
  function e() {
    var r = t !== null && t.apply(this, arguments) || this;
    return r.type = "compound", r;
  }
  return e.prototype._updatePathDirty = function() {
    for (var r = this.shape.paths, n = this.shapeChanged(), i = 0; i < r.length; i++)
      n = n || r[i].shapeChanged();
    n && this.dirtyShape();
  }, e.prototype.beforeBrush = function() {
    this._updatePathDirty();
    for (var r = this.shape.paths || [], n = this.getGlobalScale(), i = 0; i < r.length; i++)
      r[i].path || r[i].createPathProxy(), r[i].path.setScale(n[0], n[1], r[i].segmentIgnoreThreshold);
  }, e.prototype.buildPath = function(r, n) {
    for (var i = n.paths || [], a = 0; a < i.length; a++)
      i[a].buildPath(r, i[a].shape, !0);
  }, e.prototype.afterBrush = function() {
    for (var r = this.shape.paths || [], n = 0; n < r.length; n++)
      r[n].pathUpdated();
  }, e.prototype.getBoundingRect = function() {
    return this._updatePathDirty.call(this), ye.prototype.getBoundingRect.call(this);
  }, e;
})(ye), im = (function() {
  function t(e) {
    this.colorStops = e || [];
  }
  return t.prototype.addColorStop = function(e, r) {
    this.colorStops.push({
      offset: e,
      color: r
    });
  }, t;
})(), am = (function(t) {
  F(e, t);
  function e(r, n, i, a, o, s) {
    var u = t.call(this, o) || this;
    return u.x = r ?? 0, u.y = n ?? 0, u.x2 = i ?? 1, u.y2 = a ?? 0, u.type = "linear", u.global = s || !1, u;
  }
  return e;
})(im), om = (function(t) {
  F(e, t);
  function e(r, n, i, a, o) {
    var s = t.call(this, a) || this;
    return s.x = r ?? 0.5, s.y = n ?? 0.5, s.r = i ?? 0.5, s.type = "radial", s.global = o || !1, s;
  }
  return e;
})(im), ul = Math.min, SC = Math.max, ao = Math.abs, en = [0, 0], tn = [0, 0], De = Wg(), oo = De.minTv, so = De.maxTv, sm = (function() {
  function t(e, r) {
    this._corners = [], this._axes = [], this._origin = [0, 0];
    for (var n = 0; n < 4; n++)
      this._corners[n] = new ue();
    for (var n = 0; n < 2; n++)
      this._axes[n] = new ue();
    e && this.fromBoundingRect(e, r);
  }
  return t.prototype.fromBoundingRect = function(e, r) {
    var n = this._corners, i = this._axes, a = e.x, o = e.y, s = a + e.width, u = o + e.height;
    if (n[0].set(a, o), n[1].set(s, o), n[2].set(s, u), n[3].set(a, u), r)
      for (var l = 0; l < 4; l++)
        n[l].transform(r);
    ue.sub(i[0], n[1], n[0]), ue.sub(i[1], n[3], n[0]), i[0].normalize(), i[1].normalize();
    for (var l = 0; l < 2; l++)
      this._origin[l] = i[l].dot(n[0]);
  }, t.prototype.intersect = function(e, r, n) {
    var i = !0, a = !r;
    return r && ue.set(r, 0, 0), De.reset(n, !a), !this._intersectCheckOneSide(this, e, a, 1) && (i = !1, a) || !this._intersectCheckOneSide(e, this, a, -1) && (i = !1, a) || !a && !De.negativeSize && ue.copy(r, i ? De.useDir ? De.dirMinTv : oo : so), i;
  }, t.prototype._intersectCheckOneSide = function(e, r, n, i) {
    for (var a = !0, o = 0; o < 2; o++) {
      var s = e._axes[o];
      if (e._getProjMinMaxOnAxis(o, e._corners, en), e._getProjMinMaxOnAxis(o, r._corners, tn), De.negativeSize || en[1] < tn[0] || en[0] > tn[1]) {
        if (a = !1, De.negativeSize || n)
          return a;
        var u = ao(tn[0] - en[1]), l = ao(en[0] - tn[1]);
        ul(u, l) > so.len() && (u < l ? ue.scale(so, s, -u * i) : ue.scale(so, s, l * i));
      } else if (!n) {
        var u = ao(tn[0] - en[1]), l = ao(en[0] - tn[1]);
        (De.useDir || ul(u, l) < oo.len()) && ((u < l || !De.bidirectional) && (ue.scale(oo, s, u * i), De.useDir && De.calcDirMTV()), (u >= l || !De.bidirectional) && (ue.scale(oo, s, -l * i), De.useDir && De.calcDirMTV()));
      }
    }
    return a;
  }, t.prototype._getProjMinMaxOnAxis = function(e, r, n) {
    for (var i = this._axes[e], a = this._origin, o = r[0].dot(i) + a[e], s = o, u = o, l = 1; l < r.length; l++) {
      var f = r[l].dot(i) + a[e];
      s = ul(f, s), u = SC(f, u);
    }
    n[0] = s + De.touchThreshold, n[1] = u - De.touchThreshold, De.negativeSize = n[1] < n[0];
  }, t;
})(), um = 0, bC = 1, TC = 2, wC = 1, Uo = 0, CC = [], lm = (function(t) {
  F(e, t);
  function e() {
    var r = t !== null && t.apply(this, arguments) || this;
    return r.notClear = !0, r.incremental = bC, r._displayables = [], r._temporaryDisplayables = [], r._cursor = 0, r;
  }
  return e.prototype.traverse = function(r, n) {
    r.call(n, this);
  }, e.prototype.useStyle = function() {
    this.style = {};
  }, e.prototype._useHoverStyle = function() {
    this.__hoverStyle = null;
  }, e.prototype.getCursor = function() {
    return this._cursor;
  }, e.prototype.innerAfterBrush = function() {
    this._cursor = this._displayables.length;
  }, e.prototype.clearDisplaybles = function() {
    this._displayables = [], this._temporaryDisplayables = [], this._cursor = 0, this.markRedraw(), this.notClear = !1;
  }, e.prototype.clearTemporalDisplayables = function() {
    this._temporaryDisplayables = [];
  }, e.prototype.addDisplayable = function(r, n) {
    n ? this._temporaryDisplayables.push(r) : this._displayables.push(r), this.markRedraw();
  }, e.prototype.addDisplayables = function(r, n) {
    n = n || !1;
    for (var i = 0; i < r.length; i++)
      this.addDisplayable(r[i], n);
  }, e.prototype.getDisplayables = function() {
    return this._displayables;
  }, e.prototype.getTemporalDisplayables = function() {
    return this._temporaryDisplayables;
  }, e.prototype.eachPendingDisplayable = function(r) {
    for (var n = this._cursor; n < this._displayables.length; n++)
      r && r(this._displayables[n]);
    for (var n = 0; n < this._temporaryDisplayables.length; n++)
      r && r(this._temporaryDisplayables[n]);
  }, e.prototype.update = function() {
    this.updateTransform();
    for (var r = this._cursor; r < this._displayables.length; r++) {
      var n = this._displayables[r];
      n.parent = this, n.update(), n.parent = null;
    }
    for (var r = 0; r < this._temporaryDisplayables.length; r++) {
      var n = this._temporaryDisplayables[r];
      n.parent = this, n.update(), n.parent = null;
    }
  }, e.prototype.getBoundingRect = function() {
    if (!this._rect) {
      for (var r = new Y(1 / 0, 1 / 0, -1 / 0, -1 / 0), n = 0; n < this._displayables.length; n++) {
        var i = this._displayables[n], a = i.getBoundingRect().clone();
        i.needLocalTransform() && a.applyTransform(i.getLocalTransform(CC)), r.union(a);
      }
      this._rect = r;
    }
    return this._rect;
  }, e.prototype.contain = function(r, n) {
    var i = this.transformCoordToLocal(r, n), a = this.getBoundingRect();
    if (a.contain(i[0], i[1]))
      for (var o = 0; o < this._displayables.length; o++) {
        var s = this._displayables[o];
        if (s.contain(r, n))
          return !0;
      }
    return !1;
  }, e;
})(Ba), xC = de();
function MC(t, e, r, n, i) {
  var a;
  if (e && e.ecModel) {
    var o = e.ecModel.getUpdatePayload();
    a = o && o.animation;
  }
  var s = e && e.isAnimationEnabled(), u = t === "update";
  if (s) {
    var l = void 0, f = void 0, h = void 0;
    n ? (l = G(n.duration, 200), f = G(n.easing, "cubicOut"), h = 0) : (l = e.getShallow(u ? "animationDurationUpdate" : "animationDuration"), f = e.getShallow(u ? "animationEasingUpdate" : "animationEasing"), h = e.getShallow(u ? "animationDelayUpdate" : "animationDelay")), a && (a.duration != null && (l = a.duration), a.easing != null && (f = a.easing), a.delay != null && (h = a.delay)), X(h) && (h = h(r, i)), X(l) && (l = l(r));
    var c = {
      duration: l || 0,
      delay: h,
      easing: f
    };
    return c;
  } else
    return null;
}
function Oh(t, e, r, n, i, a, o) {
  var s = !1, u;
  X(i) ? (o = a, a = i, i = null) : z(i) && (a = i.cb, o = i.during, s = i.isFrom, u = i.removeOpt, i = i.dataIndex);
  var l = t === "leave";
  l || e.stopAnimation("leave");
  var f = MC(t, n, i, l ? u || {} : null, n && n.getAnimationDelayParams ? n.getAnimationDelayParams(e, i) : null);
  if (f && f.duration > 0) {
    var h = f.duration, c = f.delay, v = f.easing, d = {
      duration: h,
      delay: c || 0,
      easing: v,
      done: a,
      force: !!a || !!o,
      // Set to final state in update/init animation.
      // So the post processing based on the path shape can be done correctly.
      setToFinal: !l,
      scope: t,
      during: o
    };
    s ? e.animateFrom(r, d) : e.animateTo(r, d);
  } else
    e.stopAnimation(), !s && e.attr(r), o && o(1), a && a();
}
function yi(t, e, r, n, i, a) {
  Oh("update", t, e, r, n, i, a);
}
function kh(t, e, r, n, i, a) {
  Oh("enter", t, e, r, n, i, a);
}
function na(t) {
  if (!t.__zr)
    return !0;
  for (var e = 0; e < t.animators.length; e++) {
    var r = t.animators[e];
    if (r.scope === "leave")
      return !0;
  }
  return !1;
}
function fm(t, e, r, n, i, a) {
  na(t) || Oh("leave", t, e, r, n, i, a);
}
function cd(t, e, r, n) {
  t.removeTextContent(), t.removeTextGuideLine(), fm(t, {
    style: {
      opacity: 0
    }
  }, e, r, n);
}
function DC(t, e, r) {
  function n() {
    t.parent && t.parent.remove(t);
  }
  t.isGroup ? t.traverse(function(i) {
    i.isGroup || cd(i, e, r, n);
  }) : cd(t, e, r, n);
}
function LR(t) {
  xC(t).oldStyle = t.style;
}
var If = {}, hm = ["x", "y"], cm = ["width", "height"], vm = 0, dm = 1, pm = 2;
function gm(t) {
  return ye.extend(t);
}
var IC = nC;
function ym(t, e) {
  return IC(t, e);
}
function wt(t, e) {
  If[t] = e;
}
function mm(t) {
  if (If.hasOwnProperty(t))
    return If[t];
}
function au(t, e, r, n) {
  var i = rC(t, e);
  return r && (n === "center" && (r = _m(r, i.getBoundingRect())), Nh(i, r)), i;
}
function Bh(t, e, r) {
  var n = new Nr({
    style: {
      image: t,
      x: e.x,
      y: e.y,
      width: e.width,
      height: e.height
    },
    onload: function(i) {
      if (r === "center") {
        var a = {
          width: i.width,
          height: i.height
        };
        n.setStyle(_m(e, a));
      }
    }
  });
  return n;
}
function _m(t, e) {
  var r = e.width / e.height, n = t.height * r, i;
  n <= t.width ? i = t.height : (n = t.width, i = n / r);
  var a = t.x + t.width / 2, o = t.y + t.height / 2;
  return {
    x: a - n / 2,
    y: o - i / 2,
    width: n,
    height: i
  };
}
var Sm = iC;
function Nh(t, e) {
  if (t.applyTransform) {
    var r = t.getBoundingRect(), n = r.calculateTransform(e);
    t.applyTransform(n);
  }
}
function Fh(t, e) {
  return ky(t, t, {
    lineWidth: e
  }), t;
}
function AC(t, e) {
  return By(t, t, e), t;
}
var PC = vn;
function bm(t, e) {
  for (var r = Mn([]); t && t !== e; )
    mn(r, t.getLocalTransform(), r), t = t.parent;
  return r;
}
function zh(t, e, r) {
  return e && !Ze(e) && (e = Ra.getLocalTransform(e)), r && (e = gi([], e)), ot([], t, e);
}
function LC(t, e, r) {
  var n = e[4] === 0 || e[5] === 0 || e[0] === 0 ? 1 : Fe(2 * e[4] / e[0]), i = e[4] === 0 || e[5] === 0 || e[2] === 0 ? 1 : Fe(2 * e[4] / e[2]), a = [t === "left" ? -n : t === "right" ? n : 0, t === "top" ? -i : t === "bottom" ? i : 0];
  return a = zh(a, e, r), Fe(a[0]) > Fe(a[1]) ? a[0] > 0 ? "right" : "left" : a[1] > 0 ? "bottom" : "top";
}
function vd(t) {
  return !t.isGroup;
}
function RC(t) {
  return t.shape != null;
}
function EC(t, e, r) {
  if (!t || !e)
    return;
  function n(o) {
    var s = {};
    return o.traverse(function(u) {
      vd(u) && u.anid && (s[u.anid] = u);
    }), s;
  }
  function i(o) {
    var s = {
      x: o.x,
      y: o.y,
      rotation: o.rotation
    };
    return RC(o) && (s.shape = Z(o.shape)), s;
  }
  var a = n(t);
  e.traverse(function(o) {
    if (vd(o) && o.anid) {
      var s = a[o.anid];
      if (s) {
        var u = i(o);
        o.attr(i(s)), yi(o, u, r, he(o).dataIndex);
      }
    }
  });
}
function Tm(t, e) {
  return H(t, function(r) {
    var n = r[0];
    n = ge(n, e.x), n = et(n, e.x + e.width);
    var i = r[1];
    return i = ge(i, e.y), i = et(i, e.y + e.height), [n, i];
  });
}
function wm(t, e) {
  var r = ge(t.x, e.x), n = et(t.x + t.width, e.x + e.width), i = ge(t.y, e.y), a = et(t.y + t.height, e.y + e.height);
  if (n >= r && a >= i)
    return {
      x: r,
      y: i,
      width: n - r,
      height: a - i
    };
}
function ou(t, e, r) {
  var n = E({
    rectHover: !0
  }, e), i = n.style = {
    strokeNoScale: !0
  };
  if (r = r || {
    x: -1,
    y: -1,
    width: 2,
    height: 2
  }, t)
    return t.indexOf("image://") === 0 ? (i.image = t.slice(8), _e(i, r), new Nr(n)) : au(t.replace("path://", ""), n, r, "center");
}
function OC(t, e, r, n, i) {
  for (var a = 0, o = i[i.length - 1]; a < i.length; a++) {
    var s = i[a];
    if (Cm(t, e, r, n, s[0], s[1], o[0], o[1]))
      return !0;
    o = s;
  }
}
function Cm(t, e, r, n, i, a, o, s) {
  var u = r - t, l = n - e, f = o - i, h = s - a, c = ll(f, h, u, l);
  if (kC(c))
    return !1;
  var v = t - i, d = e - a, y = ll(v, d, u, l) / c;
  if (y < 0 || y > 1)
    return !1;
  var p = ll(v, d, f, h) / c;
  return !(p < 0 || p > 1);
}
function ll(t, e, r, n) {
  return t * n - r * e;
}
function kC(t) {
  return t <= 1e-6 && t >= -1e-6;
}
function Af(t, e, r, n, i) {
  return e == null || (ve(e) ? fe[0] = fe[1] = fe[2] = fe[3] = e : (fe[0] = e[0], fe[1] = e[1], fe[2] = e[2], fe[3] = e[3]), n && (fe[0] = ge(0, fe[0]), fe[1] = ge(0, fe[1]), fe[2] = ge(0, fe[2]), fe[3] = ge(0, fe[3])), r && (fe[0] = -fe[0], fe[1] = -fe[1], fe[2] = -fe[2], fe[3] = -fe[3]), dd(t, fe, "x", "width", 3, 1, i && i[0] || 0), dd(t, fe, "y", "height", 0, 2, i && i[1] || 0)), t;
}
var fe = [0, 0, 0, 0];
function dd(t, e, r, n, i, a, o) {
  var s = e[a] + e[i], u = t[n];
  t[n] += s, o = ge(0, et(o, u)), t[n] < o ? (t[n] = o, t[r] += e[i] >= 0 ? -e[i] : e[a] >= 0 ? u + e[a] : Fe(s) > 1e-8 ? (u - o) * e[i] / s : 0) : t[r] -= e[i];
}
function su(t) {
  var e = t.itemTooltipOption, r = t.componentModel, n = t.itemName, i = N(e) ? {
    formatter: e
  } : e, a = r.mainType, o = r.componentIndex, s = {
    componentType: a,
    name: n,
    $vars: ["name"]
  };
  s[a + "Index"] = o;
  var u = t.formatterParamsExtra;
  u && w(me(u), function(f) {
    qe(s, f) || (s[f] = u[f], s.$vars.push(f));
  });
  var l = he(t.el);
  l.componentMainType = a, l.componentIndex = o, l.tooltipConfig = {
    name: n,
    option: _e({
      content: n,
      encodeHTMLContent: !0,
      formatterParams: s
    }, i)
  };
}
function Pf(t, e) {
  var r;
  t.isGroup && (r = e(t)), r || t.traverse(e);
}
function xm(t, e) {
  if (t)
    if (B(t))
      for (var r = 0; r < t.length; r++)
        Pf(t[r], e);
    else
      Pf(t, e);
}
function Hh(t) {
  return !t || Fe(t[1]) < uo && Fe(t[2]) < uo || Fe(t[0]) < uo && Fe(t[3]) < uo;
}
var uo = 1e-5;
function Sa(t, e) {
  return t ? Y.copy(t, e) : e.clone();
}
function Vh(t, e) {
  return e ? La(t || Tt(), e) : void 0;
}
function Mm(t) {
  return {
    z: t.get("z") || 0,
    zlevel: t.get("zlevel") || 0
  };
}
function BC(t) {
  var e = -1 / 0, r = 1 / 0;
  Pf(t, function(a) {
    n(a), n(a.getTextContent()), n(a.getTextGuideLine());
  });
  function n(a) {
    if (!(!a || a.isGroup)) {
      var o = a.currentStates;
      if (o.length)
        for (var s = 0; s < o.length; s++)
          i(a.states[o[s]]);
      i(a);
    }
  }
  function i(a) {
    if (a) {
      var o = a.z2;
      o > e && (e = o), o < r && (r = o);
    }
  }
  return r > e && (r = e = 0), {
    min: r,
    max: e
  };
}
function Dm(t, e, r) {
  Im(t, e, r, -1 / 0);
}
function Im(t, e, r, n) {
  if (t.ignoreModelZ)
    return n;
  var i = t.getTextContent(), a = t.getTextGuideLine(), o = t.isGroup;
  if (o)
    for (var s = t.childrenRef(), u = 0; u < s.length; u++)
      n = ge(Im(s[u], e, r, n), n);
  else
    t.z = e, t.zlevel = r, n = ge(t.z2 || 0, n);
  if (i && (i.z = e, i.zlevel = r, isFinite(n) && (i.z2 = n + 2)), a) {
    var l = t.textGuideLineConfig;
    a.z = e, a.zlevel = r, isFinite(n) && (a.z2 = n + (l && l.showAbove ? 1 : -1));
  }
  return n;
}
function NC(t) {
  return t.animation = {
    duration: 0
  }, t;
}
function FC(t, e) {
  return e ? La(Ki.transform, e) : Mn(Ki.transform), Ki.decomposeTransform(), us(t, Ki), t;
}
var Ki = new Ra();
Ki.transform = Tt();
function zC(t) {
  var e = t.getZr().painter;
  return e.getType() === "canvas" ? e : null;
}
wt("circle", Na);
wt("ellipse", Js);
wt("sector", eu);
wt("ring", tu);
wt("polygon", ru);
wt("polyline", nu);
wt("rect", Ee);
wt("line", Pn);
wt("bezierCurve", iu);
wt("arc", Fa);
const HC = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: Fa,
  BezierCurve: iu,
  BoundingRect: Y,
  Circle: Na,
  CompoundPath: nm,
  Ellipse: Js,
  Group: ut,
  HOVER_LAYER_FOR_INCREMENTAL: pm,
  HOVER_LAYER_FROM_THRESHOLD: dm,
  HOVER_LAYER_NO: vm,
  Image: Nr,
  IncrementalDisplayable: lm,
  Line: Pn,
  LinearGradient: am,
  OrientedBoundingRect: sm,
  Path: ye,
  Point: ue,
  Polygon: ru,
  Polyline: nu,
  RadialGradient: om,
  Rect: Ee,
  Ring: tu,
  Sector: eu,
  Text: je,
  WH: cm,
  XY: hm,
  applyTransform: zh,
  calcZ2Range: BC,
  clipPointsByRect: Tm,
  clipRectByRect: wm,
  createIcon: ou,
  decomposeTransform: FC,
  ensureCopyRect: Sa,
  ensureCopyTransform: Vh,
  expandOrShrinkRect: Af,
  extendPath: ym,
  extendShape: gm,
  getCurrentCanvasPainter: zC,
  getShapeClass: mm,
  getTransform: bm,
  groupTransition: EC,
  initProps: kh,
  isBoundingRectAxisAligned: Hh,
  isElementRemoved: na,
  lineLineIntersect: Cm,
  linePolygonIntersect: OC,
  makeImage: Bh,
  makePath: au,
  mergePath: Sm,
  payloadDisableAnimation: NC,
  registerShape: wt,
  removeElement: fm,
  removeElementWithFadeOut: DC,
  resizePath: Nh,
  retrieveZInfo: Mm,
  setTooltipConfig: su,
  subPixelOptimize: PC,
  subPixelOptimizeLine: Fh,
  subPixelOptimizeRect: AC,
  transformDirection: LC,
  traverseElements: xm,
  traverseUpdateZ: Dm,
  updateProps: yi
}, Symbol.toStringTag, { value: "Module" }));
var uu = {};
function Am(t, e) {
  for (var r = 0; r < Or.length; r++) {
    var n = Or[r], i = e[n], a = t.ensureState(n);
    a.style = a.style || {}, a.style.text = i;
  }
  var o = t.currentStates.slice();
  t.clearStates(!0), t.setStyle({
    text: e.normal
  }), t.useStates(o, !0);
}
function Lf(t, e, r) {
  var n = t.labelFetcher, i = t.labelDataIndex, a = t.labelDimIndex, o = e.normal, s;
  n && (s = n.getFormattedLabel(i, "normal", null, a, o && o.get("formatter"), r != null ? {
    interpolatedValue: r
  } : null)), s == null && (s = X(t.defaultText) ? t.defaultText(i, t, r) : t.defaultText);
  for (var u = {
    normal: s
  }, l = 0; l < Or.length; l++) {
    var f = Or[l], h = e[f];
    u[f] = G(n ? n.getFormattedLabel(i, f, null, a, h && h.get("formatter")) : null, s);
  }
  return u;
}
function VC(t, e, r, n) {
  r = r || uu;
  for (var i = t instanceof je, a = !1, o = 0; o < Jv.length; o++) {
    var s = e[Jv[o]];
    if (s && s.getShallow("show")) {
      a = !0;
      break;
    }
  }
  var u = i ? t : t.getTextContent();
  if (a) {
    i || (u || (u = new je(), t.setTextContent(u)), t.stateProxy && (u.stateProxy = t.stateProxy));
    var l = Lf(r, e), f = e.normal, h = !!f.getShallow("show"), c = Cn(f, n, r, !1, !i);
    c.text = l.normal, i || t.setTextConfig(pd(f, r, !1));
    for (var o = 0; o < Or.length; o++) {
      var v = Or[o], s = e[v];
      if (s) {
        var d = u.ensureState(v), y = !!G(s.getShallow("show"), h);
        if (y !== h && (d.ignore = !y), d.style = Cn(s, n, r, !0, !i), d.style.text = l[v], !i) {
          var p = t.ensureState(v);
          p.textConfig = pd(s, r, !0);
        }
      }
    }
    u.silent = !!f.getShallow("silent"), u.style.x != null && (c.x = u.style.x), u.style.y != null && (c.y = u.style.y), u.ignore = !h, u.useStyle(c), u.dirty(), r.enableTextSetter && (Gh(u).setLabelText = function(g) {
      var m = Lf(r, e, g);
      Am(u, m);
    });
  } else u && (u.ignore = !0);
  t.dirty();
}
function RR(t, e) {
  e = e || "label";
  for (var r = {
    normal: t.getModel(e)
  }, n = 0; n < Or.length; n++) {
    var i = Or[n];
    r[i] = t.getModel([i, e]);
  }
  return r;
}
function Cn(t, e, r, n, i) {
  var a = {};
  return GC(a, t, r, n, i), e && E(a, e), a;
}
function pd(t, e, r) {
  e = e || {};
  var n = {}, i, a = t.getShallow("rotate"), o = G(t.getShallow("distance"), r ? null : 5), s = t.getShallow("offset");
  return i = t.getShallow("position") || (r ? null : "inside"), i === "outside" && (i = e.defaultOutsidePosition || "top"), i != null && (n.position = i), s != null && (n.offset = s), a != null && (a *= Math.PI / 180, n.rotation = a), o != null && (n.distance = o), n.outsideFill = t.get("color") === "inherit" ? e.inheritColor || null : "auto", e.autoOverflowArea != null && (n.autoOverflowArea = e.autoOverflowArea), e.layoutRect != null && (n.layoutRect = e.layoutRect), n;
}
function GC(t, e, r, n, i) {
  r = r || uu;
  var a = e.ecModel, o = a && a.option.textStyle, s = UC(e), u;
  if (s) {
    u = {};
    var l = "richInheritPlainLabel", f = G(e.get(l), a ? a.get(l) : void 0);
    for (var h in s)
      if (s.hasOwnProperty(h)) {
        var c = e.getModel(["rich", h]);
        _d(u[h] = {}, c, o, e, f, r, n, i, !1, !0);
      }
  }
  u && (t.rich = u);
  var v = e.get("overflow");
  v && (t.overflow = v);
  var d = e.get("lineOverflow");
  d && (t.lineOverflow = d);
  var y = t, p = e.get("minMargin");
  if (p != null)
    p = ve(p) ? p / 2 : 0, y.margin = [p, p, p, p], y.__marginType = Jn.minMargin;
  else {
    var g = e.get("textMargin");
    g != null && (y.margin = Hs(g), y.__marginType = Jn.textMargin);
  }
  _d(t, e, o, null, null, r, n, i, !0, !1);
}
function UC(t) {
  for (var e; t && t !== t.ecModel; ) {
    var r = (t.option || uu).rich;
    if (r) {
      e = e || {};
      for (var n = me(r), i = 0; i < n.length; i++) {
        var a = n[i];
        e[a] = 1;
      }
    }
    t = t.parentModel;
  }
  return e;
}
var gd = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY"], yd = ["align", "lineHeight", "width", "height", "tag", "verticalAlign", "ellipsis"], md = ["padding", "borderWidth", "borderRadius", "borderDashOffset", "backgroundColor", "borderColor", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"];
function _d(t, e, r, n, i, a, o, s, u, l) {
  r = !o && r || uu;
  var f = a && a.inheritColor, h = e.getShallow("color"), c = e.getShallow("textBorderColor"), v = G(e.getShallow("opacity"), r.opacity);
  (h === "inherit" || h === "auto") && (f ? h = f : h = null), (c === "inherit" || c === "auto") && (f ? c = f : c = null), s || (h = h || r.color, c = c || r.textBorderColor), h != null && (t.fill = h), c != null && (t.stroke = c);
  var d = G(e.getShallow("textBorderWidth"), r.textBorderWidth);
  d != null && (t.lineWidth = d);
  var y = G(e.getShallow("textBorderType"), r.textBorderType);
  y != null && (t.lineDash = y);
  var p = G(e.getShallow("textBorderDashOffset"), r.textBorderDashOffset);
  p != null && (t.lineDashOffset = p), !o && v == null && !l && (v = a && a.defaultOpacity), v != null && (t.opacity = v), !o && !s && t.fill == null && a.inheritColor && (t.fill = a.inheritColor);
  for (var g = 0; g < gd.length; g++) {
    var m = gd[g], _ = i !== !1 && n ? gn(e.getShallow(m), n.getShallow(m), r[m]) : G(e.getShallow(m), r[m]);
    _ != null && (t[m] = _);
  }
  for (var g = 0; g < yd.length; g++) {
    var m = yd[g], _ = e.getShallow(m);
    _ != null && (t[m] = _);
  }
  if (t.verticalAlign == null) {
    var S = e.getShallow("baseline");
    S != null && (t.verticalAlign = S);
  }
  if (!u || !a.disableBox) {
    for (var g = 0; g < md.length; g++) {
      var m = md[g], _ = e.getShallow(m);
      _ != null && (t[m] = _);
    }
    var b = e.getShallow("borderType");
    b != null && (t.borderDash = b), (t.backgroundColor === "auto" || t.backgroundColor === "inherit") && f && (t.backgroundColor = f), (t.borderColor === "auto" || t.borderColor === "inherit") && f && (t.borderColor = f);
  }
}
function YC(t, e) {
  var r = e && e.getModel("textStyle");
  return It([
    // FIXME in node-canvas fontWeight is before fontStyle
    t.fontStyle || r && r.getShallow("fontStyle") || "",
    t.fontWeight || r && r.getShallow("fontWeight") || "",
    (t.fontSize || r && r.getShallow("fontSize") || 12) + "px",
    t.fontFamily || r && r.getShallow("fontFamily") || "sans-serif"
  ].join(" "));
}
var Gh = de();
function ER(t, e, r, n) {
  if (t) {
    var i = Gh(t);
    i.prevValue = i.value, i.value = r;
    var a = e.normal;
    i.valueAnimation = a.get("valueAnimation"), i.valueAnimation && (i.precision = a.get("precision"), i.defaultInterpolatedText = n, i.statesModels = e);
  }
}
function OR(t, e, r, n, i) {
  var a = Gh(t);
  if (!a.valueAnimation || a.prevValue === a.value)
    return;
  var o = a.defaultInterpolatedText, s = G(a.interpolatedValue, a.prevValue), u = a.value;
  function l(f) {
    var h = mT(r, a.precision, s, u, f);
    a.interpolatedValue = f === 1 ? null : h;
    var c = Lf({
      labelDataIndex: e,
      labelFetcher: i,
      defaultText: o ? o(h) : h + ""
    }, a.statesModels, h);
    Am(t, c);
  }
  t.percent = 0, (a.prevValue == null ? kh : yi)(t, {
    // percent is used to prevent animation from being aborted #15916
    percent: 1
  }, n, e, null, l);
}
var Jn = {
  minMargin: 1,
  textMargin: 2
}, WC = ["textStyle", "color"], fl = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "padding", "lineHeight", "rich", "width", "height", "overflow"], hl = new je(), XC = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getTextColor = function(e) {
      var r = this.ecModel;
      return this.getShallow("color") || (!e && r ? r.get(WC) : null);
    }, t.prototype.getFont = function() {
      return YC({
        fontStyle: this.getShallow("fontStyle"),
        fontWeight: this.getShallow("fontWeight"),
        fontSize: this.getShallow("fontSize"),
        fontFamily: this.getShallow("fontFamily")
      }, this.ecModel);
    }, t.prototype.getTextRect = function(e) {
      for (var r = {
        text: e,
        verticalAlign: this.getShallow("verticalAlign") || this.getShallow("baseline")
      }, n = 0; n < fl.length; n++)
        r[fl[n]] = this.getShallow(fl[n]);
      return hl.useStyle(r), hl.update(), hl.getBoundingRect();
    }, t;
  })()
), Pm = [
  ["lineWidth", "width"],
  ["stroke", "color"],
  ["opacity"],
  ["shadowBlur"],
  ["shadowOffsetX"],
  ["shadowOffsetY"],
  ["shadowColor"],
  ["lineDash", "type"],
  ["lineDashOffset", "dashOffset"],
  ["lineCap", "cap"],
  ["lineJoin", "join"],
  ["miterLimit"]
  // Option decal is in `DecalObject` but style.decal is in `PatternObject`.
  // So do not transfer decal directly.
], qC = _a(Pm), $C = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getLineStyle = function(e) {
      return qC(this, e);
    }, t;
  })()
), Lm = [
  ["fill", "color"],
  ["stroke", "borderColor"],
  ["lineWidth", "borderWidth"],
  ["opacity"],
  ["shadowBlur"],
  ["shadowOffsetX"],
  ["shadowOffsetY"],
  ["shadowColor"],
  ["lineDash", "borderType"],
  ["lineDashOffset", "borderDashOffset"],
  ["lineCap", "borderCap"],
  ["lineJoin", "borderJoin"],
  ["miterLimit", "borderMiterLimit"]
  // Option decal is in `DecalObject` but style.decal is in `PatternObject`.
  // So do not transfer decal directly.
], ZC = _a(Lm), KC = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getItemStyle = function(e, r) {
      return ZC(this, e, r);
    }, t;
  })()
), ce = (
  /** @class */
  (function() {
    function t(e, r, n) {
      this.parentModel = r, this.ecModel = n, this.option = e;
    }
    return t.prototype.init = function(e, r, n) {
    }, t.prototype.mergeOption = function(e, r) {
      pe(this.option, e, !0);
    }, t.prototype.get = function(e, r) {
      return e == null ? this.option : this._doGet(this.parsePath(e), !r && this.parentModel);
    }, t.prototype.getShallow = function(e, r) {
      var n = this.option, i = n == null ? n : n[e];
      if (i == null && !r) {
        var a = this.parentModel;
        a && (i = a.getShallow(e));
      }
      return i;
    }, t.prototype.getModel = function(e, r) {
      var n = e != null, i = n ? this.parsePath(e) : null, a = n ? this._doGet(i) : this.option;
      return r = r || this.parentModel && this.parentModel.getModel(this.resolveParentPath(i)), new t(a, r, this.ecModel);
    }, t.prototype.isEmpty = function() {
      return this.option == null;
    }, t.prototype.restoreData = function() {
    }, t.prototype.clone = function() {
      var e = this.constructor;
      return new e(Z(this.option));
    }, t.prototype.parsePath = function(e) {
      return typeof e == "string" ? e.split(".") : e;
    }, t.prototype.resolveParentPath = function(e) {
      return e;
    }, t.prototype.isAnimationEnabled = function() {
      if (!W.node && this.option) {
        if (this.option.animation != null)
          return !!this.option.animation;
        if (this.parentModel)
          return this.parentModel.isAnimationEnabled();
      }
    }, t.prototype._doGet = function(e, r) {
      var n = this.option;
      if (!e)
        return n;
      for (var i = 0; i < e.length && !(e[i] && (n = n && typeof n == "object" ? n[e[i]] : null, n == null)); i++)
        ;
      return n == null && r && (n = r._doGet(this.resolveParentPath(e), r.parentModel)), n;
    }, t;
  })()
);
Mh(ce);
ET(ce);
Lt(ce, $C);
Lt(ce, KC);
Lt(ce, FT);
Lt(ce, XC);
var jC = Math.round(Math.random() * 10);
function lu(t) {
  return [t || "", jC++].join("_");
}
function QC(t) {
  var e = {};
  t.registerSubTypeDefaulter = function(r, n) {
    var i = Vt(r);
    e[i.main] = n;
  }, t.determineSubType = function(r, n) {
    var i = n.type;
    if (!i) {
      var a = Vt(r).main;
      t.hasSubTypes(r) && e[a] && (i = e[a](n));
    }
    return i;
  };
}
function JC(t, e) {
  t.topologicalTravel = function(a, o, s, u) {
    if (!a.length)
      return;
    var l = r(o), f = l.graph, h = l.noEntryList, c = {};
    for (w(a, function(m) {
      c[m] = !0;
    }); h.length; ) {
      var v = h.pop(), d = f[v], y = !!c[v];
      y && (s.call(u, v, d.originalDeps.slice()), delete c[v]), w(d.successor, y ? g : p);
    }
    w(c, function() {
      var m = "";
      throw new Error(m);
    });
    function p(m) {
      f[m].entryCount--, f[m].entryCount === 0 && h.push(m);
    }
    function g(m) {
      c[m] = !0, p(m);
    }
  };
  function r(a) {
    var o = {}, s = [];
    return w(a, function(u) {
      var l = n(o, u), f = l.originalDeps = e(u), h = i(f, a);
      l.entryCount = h.length, l.entryCount === 0 && s.push(u), w(h, function(c) {
        oe(l.predecessor, c) < 0 && l.predecessor.push(c);
        var v = n(o, c);
        oe(v.successor, c) < 0 && v.successor.push(u);
      });
    }), {
      graph: o,
      noEntryList: s
    };
  }
  function n(a, o) {
    return a[o] || (a[o] = {
      predecessor: [],
      successor: []
    }), a[o];
  }
  function i(a, o) {
    var s = [];
    return w(a, function(u) {
      oe(o, u) >= 0 && s.push(u);
    }), s;
  }
}
function ex(t, e) {
  return pe(pe({}, t, !0), e, !0);
}
const tx = {
  time: {
    month: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
    monthAbbr: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    dayOfWeek: ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
    dayOfWeekAbbr: ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
  },
  legend: {
    selector: {
      all: "All",
      inverse: "Inv"
    }
  },
  toolbox: {
    brush: {
      title: {
        rect: "Box Select",
        polygon: "Lasso Select",
        lineX: "Horizontally Select",
        lineY: "Vertically Select",
        keep: "Keep Selections",
        clear: "Clear Selections"
      }
    },
    dataView: {
      title: "Data View",
      lang: ["Data View", "Close", "Refresh"]
    },
    dataZoom: {
      title: {
        zoom: "Zoom",
        back: "Zoom Reset"
      }
    },
    magicType: {
      title: {
        line: "Switch to Line Chart",
        bar: "Switch to Bar Chart",
        stack: "Stack",
        tiled: "Tile"
      }
    },
    restore: {
      title: "Restore"
    },
    saveAsImage: {
      title: "Save as Image",
      lang: ["Right Click to Save Image"]
    }
  },
  series: {
    typeNames: {
      pie: "Pie chart",
      bar: "Bar chart",
      line: "Line chart",
      scatter: "Scatter plot",
      effectScatter: "Ripple scatter plot",
      radar: "Radar chart",
      tree: "Tree",
      treemap: "Treemap",
      boxplot: "Boxplot",
      candlestick: "Candlestick",
      k: "K line chart",
      heatmap: "Heat map",
      map: "Map",
      parallel: "Parallel coordinate map",
      lines: "Line graph",
      graph: "Relationship graph",
      sankey: "Sankey diagram",
      funnel: "Funnel chart",
      gauge: "Gauge",
      pictorialBar: "Pictorial bar",
      themeRiver: "Theme River Map",
      sunburst: "Sunburst",
      custom: "Custom chart",
      chart: "Chart"
    }
  },
  aria: {
    general: {
      withTitle: 'This is a chart about "{title}"',
      withoutTitle: "This is a chart"
    },
    series: {
      single: {
        prefix: "",
        withName: " with type {seriesType} named {seriesName}.",
        withoutName: " with type {seriesType}."
      },
      multiple: {
        prefix: ". It consists of {seriesCount} series count.",
        withName: " The {seriesId} series is a {seriesType} representing {seriesName}.",
        withoutName: " The {seriesId} series is a {seriesType}.",
        separator: {
          middle: "",
          end: ""
        }
      }
    },
    data: {
      allData: "The data is as follows: ",
      partialData: "The first {displayCnt} items are: ",
      withName: "the data for {name} is {value}",
      withoutName: "{value}",
      separator: {
        middle: ", ",
        end: ". "
      }
    }
  }
}, rx = {
  time: {
    month: ["一月", "二月", "三月", "四月", "五月", "六月", "七月", "八月", "九月", "十月", "十一月", "十二月"],
    monthAbbr: ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"],
    dayOfWeek: ["星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"],
    dayOfWeekAbbr: ["日", "一", "二", "三", "四", "五", "六"]
  },
  legend: {
    selector: {
      all: "全选",
      inverse: "反选"
    }
  },
  toolbox: {
    brush: {
      title: {
        rect: "矩形选择",
        polygon: "圈选",
        lineX: "横向选择",
        lineY: "纵向选择",
        keep: "保持选择",
        clear: "清除选择"
      }
    },
    dataView: {
      title: "数据视图",
      lang: ["数据视图", "关闭", "刷新"]
    },
    dataZoom: {
      title: {
        zoom: "区域缩放",
        back: "区域缩放还原"
      }
    },
    magicType: {
      title: {
        line: "切换为折线图",
        bar: "切换为柱状图",
        stack: "切换为堆叠",
        tiled: "切换为平铺"
      }
    },
    restore: {
      title: "还原"
    },
    saveAsImage: {
      title: "保存为图片",
      lang: ["右键另存为图片"]
    }
  },
  series: {
    typeNames: {
      pie: "饼图",
      bar: "柱状图",
      line: "折线图",
      scatter: "散点图",
      effectScatter: "涟漪散点图",
      radar: "雷达图",
      tree: "树图",
      treemap: "矩形树图",
      boxplot: "箱型图",
      candlestick: "K线图",
      k: "K线图",
      heatmap: "热力图",
      map: "地图",
      parallel: "平行坐标图",
      lines: "线图",
      graph: "关系图",
      sankey: "桑基图",
      funnel: "漏斗图",
      gauge: "仪表盘图",
      pictorialBar: "象形柱图",
      themeRiver: "主题河流图",
      sunburst: "旭日图",
      custom: "自定义图表",
      chart: "图表"
    }
  },
  aria: {
    general: {
      withTitle: "这是一个关于“{title}”的图表。",
      withoutTitle: "这是一个图表，"
    },
    series: {
      single: {
        prefix: "",
        withName: "图表类型是{seriesType}，表示{seriesName}。",
        withoutName: "图表类型是{seriesType}。"
      },
      multiple: {
        prefix: "它由{seriesCount}个图表系列组成。",
        withName: "第{seriesId}个系列是一个表示{seriesName}的{seriesType}，",
        withoutName: "第{seriesId}个系列是一个{seriesType}，",
        separator: {
          middle: "；",
          end: "。"
        }
      }
    },
    data: {
      allData: "其数据是——",
      partialData: "其中，前{displayCnt}项是——",
      withName: "{name}的数据是{value}",
      withoutName: "{value}",
      separator: {
        middle: "，",
        end: ""
      }
    }
  }
};
var ds = "ZH", Uh = "EN", ri = Uh, Yo = {}, Yh = {}, Rm = W.domSupported ? (function() {
  var t = (document.documentElement.lang || navigator.language || navigator.browserLanguage || ri).toUpperCase();
  return t.indexOf(ds) > -1 ? ds : ri;
})() : ri;
function Wh(t, e) {
  t = t.toUpperCase(), Yh[t] = new ce(e), Yo[t] = e;
}
function nx(t) {
  if (N(t)) {
    var e = Yo[t.toUpperCase()] || {};
    return t === ds || t === Uh ? Z(e) : pe(Z(e), Z(Yo[ri]), !1);
  } else
    return pe(Z(t), Z(Yo[ri]), !1);
}
function ix(t) {
  return Yh[t];
}
function ax() {
  return Yh[ri];
}
Wh(Uh, tx);
Wh(ds, rx);
var ox = null;
function Xe() {
  return ox;
}
function Em(t, e) {
  var r = Xe(), n = e.breakOption, i = e.breakParsed;
  return !i && r && (i = r.parseAxisBreakOption(n, t)), i;
}
function ps(t) {
  var e = t.brk;
  return e ? e.breaks : [];
}
function sx(t) {
  var e = t.brk;
  return e ? e.hasBreaks() : !1;
}
var Xh = 1e3, qh = Xh * 60, ia = qh * 60, St = ia * 24, Sd = St * 365, ux = {
  year: /({yyyy}|{yy})/,
  month: /({MMMM}|{MMM}|{MM}|{M})/,
  day: /({dd}|{d})/,
  hour: /({HH}|{H}|{hh}|{h})/,
  minute: /({mm}|{m})/,
  second: /({ss}|{s})/,
  millisecond: /({SSS}|{S})/
}, Wo = {
  year: "{yyyy}",
  month: "{MMM}",
  day: "{d}",
  hour: "{HH}:{mm}",
  minute: "{HH}:{mm}",
  second: "{HH}:{mm}:{ss}",
  millisecond: "{HH}:{mm}:{ss} {SSS}"
}, lx = "{yyyy}-{MM}-{dd} {HH}:{mm}:{ss} {SSS}", lo = "{yyyy}-{MM}-{dd}", bd = {
  year: "{yyyy}",
  month: "{yyyy}-{MM}",
  day: lo,
  hour: lo + " " + Wo.hour,
  minute: lo + " " + Wo.minute,
  second: lo + " " + Wo.second,
  millisecond: lx
}, it = ["year", "month", "day", "hour", "minute", "second", "millisecond"], fx = ["year", "half-year", "quarter", "month", "week", "half-week", "day", "half-day", "quarter-day", "hour", "minute", "second", "millisecond"];
function hx(t) {
  return !N(t) && !X(t) ? cx(t) : t;
}
function cx(t) {
  t = t || {};
  var e = {}, r = !0;
  return w(it, function(n) {
    r && (r = t[n] == null);
  }), w(it, function(n, i) {
    var a = t[n];
    e[n] = {};
    for (var o = null, s = i; s >= 0; s--) {
      var u = it[s], l = z(a) && !B(a) ? a[u] : a, f = void 0;
      B(l) ? (f = l.slice(), o = f[0] || "") : N(l) ? (o = l, f = [o]) : (o == null ? o = Wo[n] : ux[u].test(o) || (o = e[u][u][0] + " " + o), f = [o], r && (f[1] = "{primary|" + o + "}")), e[n][u] = f;
    }
  }), e;
}
function Ge(t, e) {
  return t += "", "0000".substr(0, e - t.length) + t;
}
function aa(t) {
  switch (t) {
    case "half-year":
    case "quarter":
      return "month";
    case "week":
    case "half-week":
      return "day";
    case "half-day":
    case "quarter-day":
      return "hour";
    default:
      return t;
  }
}
function vx(t) {
  return t === aa(t);
}
function dx(t) {
  switch (t) {
    case "year":
    case "month":
      return "day";
    case "millisecond":
      return "millisecond";
    default:
      return "second";
  }
}
function za(t, e, r, n) {
  var i = pr(t), a = i[Om(r)](), o = i[$h(r)]() + 1, s = Math.floor((o - 1) / 3) + 1, u = i[Zh(r)](), l = i["get" + (r ? "UTC" : "") + "Day"](), f = i[Kh(r)](), h = (f - 1) % 12 + 1, c = i[jh(r)](), v = i[Qh(r)](), d = i[Jh(r)](), y = f >= 12 ? "pm" : "am", p = y.toUpperCase(), g = n instanceof ce ? n : ix(n || Rm) || ax(), m = g.getModel("time"), _ = m.get("month"), S = m.get("monthAbbr"), b = m.get("dayOfWeek"), T = m.get("dayOfWeekAbbr");
  return (e || "").replace(/{a}/g, y + "").replace(/{A}/g, p + "").replace(/{yyyy}/g, a + "").replace(/{yy}/g, Ge(a % 100 + "", 2)).replace(/{Q}/g, s + "").replace(/{MMMM}/g, _[o - 1]).replace(/{MMM}/g, S[o - 1]).replace(/{MM}/g, Ge(o, 2)).replace(/{M}/g, o + "").replace(/{dd}/g, Ge(u, 2)).replace(/{d}/g, u + "").replace(/{eeee}/g, b[l]).replace(/{ee}/g, T[l]).replace(/{e}/g, l + "").replace(/{HH}/g, Ge(f, 2)).replace(/{H}/g, f + "").replace(/{hh}/g, Ge(h + "", 2)).replace(/{h}/g, h + "").replace(/{mm}/g, Ge(c, 2)).replace(/{m}/g, c + "").replace(/{ss}/g, Ge(v, 2)).replace(/{s}/g, v + "").replace(/{SSS}/g, Ge(d, 3)).replace(/{S}/g, d + "");
}
function px(t, e, r, n, i) {
  var a = null;
  if (N(r))
    a = r;
  else if (X(r)) {
    var o = {
      time: t.time,
      level: t.time ? t.time.level : 0
    }, s = Xe();
    s && s.makeAxisLabelFormatterParamBreak(o, t.break), a = r(t.value, e, o);
  } else {
    var u = t.time;
    if (u) {
      var l = r[u.lowerTimeUnit][u.upperTimeUnit];
      a = l[Math.min(u.level, l.length - 1)] || "";
    } else {
      var f = ni(t.value, i);
      a = r[f][f][0];
    }
  }
  return za(new Date(t.value), a, i, n);
}
function ni(t, e) {
  var r = pr(t), n = r[$h(e)]() + 1, i = r[Zh(e)](), a = r[Kh(e)](), o = r[jh(e)](), s = r[Qh(e)](), u = r[Jh(e)](), l = u === 0, f = l && s === 0, h = f && o === 0, c = h && a === 0, v = c && i === 1, d = v && n === 1;
  return d ? "year" : v ? "month" : c ? "day" : h ? "hour" : f ? "minute" : l ? "second" : "millisecond";
}
function gs(t, e, r) {
  switch (e) {
    case "year":
      t[km(r)](0);
    case "month":
      t[Bm(r)](1);
    case "day":
      t[Nm(r)](0);
    case "hour":
      t[Fm(r)](0);
    case "minute":
      t[zm(r)](0);
    case "second":
      t[Hm(r)](0);
  }
  return t;
}
function Om(t) {
  return t ? "getUTCFullYear" : "getFullYear";
}
function $h(t) {
  return t ? "getUTCMonth" : "getMonth";
}
function Zh(t) {
  return t ? "getUTCDate" : "getDate";
}
function Kh(t) {
  return t ? "getUTCHours" : "getHours";
}
function jh(t) {
  return t ? "getUTCMinutes" : "getMinutes";
}
function Qh(t) {
  return t ? "getUTCSeconds" : "getSeconds";
}
function Jh(t) {
  return t ? "getUTCMilliseconds" : "getMilliseconds";
}
function gx(t) {
  return t ? "setUTCFullYear" : "setFullYear";
}
function km(t) {
  return t ? "setUTCMonth" : "setMonth";
}
function Bm(t) {
  return t ? "setUTCDate" : "setDate";
}
function Nm(t) {
  return t ? "setUTCHours" : "setHours";
}
function Fm(t) {
  return t ? "setUTCMinutes" : "setMinutes";
}
function zm(t) {
  return t ? "setUTCSeconds" : "setSeconds";
}
function Hm(t) {
  return t ? "setUTCMilliseconds" : "setMilliseconds";
}
function yx(t, e, r, n, i, a, o, s) {
  var u = new je({
    style: {
      text: t,
      font: e,
      align: r,
      verticalAlign: n,
      padding: i,
      rich: a,
      overflow: o ? "truncate" : null,
      lineHeight: s
    }
  });
  return u.getBoundingRect();
}
function ec(t) {
  if (!yy(t))
    return N(t) ? t : "-";
  var e = (t + "").split(".");
  return e[0].replace(/(\d{1,3})(?=(?:\d{3})+(?!\d))/g, "$1,") + (e.length > 1 ? "." + e[1] : "");
}
function tc(t, e) {
  return t = (t || "").toLowerCase().replace(/-(.)/g, function(r, n) {
    return n.toUpperCase();
  }), e && t && (t = t.charAt(0).toUpperCase() + t.slice(1)), t;
}
var Ha = Hs;
function Rf(t, e, r) {
  var n = "{yyyy}-{MM}-{dd} {HH}:{mm}:{ss}";
  function i(f) {
    return f && It(f) ? f : "-";
  }
  function a(f) {
    return vr(f);
  }
  var o = e === "time", s = t instanceof Date;
  if (o || s) {
    var u = o ? pr(t) : t;
    if (isNaN(+u)) {
      if (s)
        return "-";
    } else return za(u, n, r);
  }
  if (e === "ordinal")
    return Qo(t) ? i(t) : ve(t) && a(t) ? t + "" : "-";
  var l = cr(t);
  return a(l) ? ec(l) : Qo(t) ? i(t) : typeof t == "boolean" ? t + "" : "-";
}
var Td = ["a", "b", "c", "d", "e", "f", "g"], cl = function(t, e) {
  return "{" + t + (e ?? "") + "}";
};
function rc(t, e, r) {
  B(e) || (e = [e]);
  var n = e.length;
  if (!n)
    return "";
  for (var i = e[0].$vars || [], a = 0; a < i.length; a++) {
    var o = Td[a];
    t = t.replace(cl(o), cl(o, 0));
  }
  for (var s = 0; s < n; s++)
    for (var u = 0; u < i.length; u++) {
      var l = e[s][i[u]];
      t = t.replace(cl(Td[u], s), r ? Ue(l) : l);
    }
  return t;
}
function Vm(t, e) {
  var r = N(t) ? {
    color: t,
    extraCssText: e
  } : t || {}, n = r.color, i = r.type;
  e = r.extraCssText;
  var a = r.renderMode || "html";
  if (!n)
    return "";
  if (a === "html")
    return i === "subItem" ? '<span style="display:inline-block;vertical-align:middle;margin-right:8px;margin-left:3px;border-radius:4px;width:4px;height:4px;background-color:' + Ue(n) + ";" + (e || "") + '"></span>' : '<span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background-color:' + Ue(n) + ";" + (e || "") + '"></span>';
  var o = r.markerId || "markerX";
  return {
    renderMode: a,
    content: "{" + o + "|}  ",
    style: i === "subItem" ? {
      width: 4,
      height: 4,
      borderRadius: 2,
      backgroundColor: n
    } : {
      width: 10,
      height: 10,
      borderRadius: 5,
      backgroundColor: n
    }
  };
}
function mx(t, e, r) {
  (t === "week" || t === "month" || t === "quarter" || t === "half-year" || t === "year") && (t = `MM-dd
yyyy`);
  var n = pr(e), i = r ? "getUTC" : "get", a = n[i + "FullYear"](), o = n[i + "Month"]() + 1, s = n[i + "Date"](), u = n[i + "Hours"](), l = n[i + "Minutes"](), f = n[i + "Seconds"](), h = n[i + "Milliseconds"]();
  return t = t.replace("MM", Ge(o, 2)).replace("M", o).replace("yyyy", a).replace("yy", Ge(a % 100 + "", 2)).replace("dd", Ge(s, 2)).replace("d", s).replace("hh", Ge(u, 2)).replace("h", u).replace("mm", Ge(l, 2)).replace("m", l).replace("ss", Ge(f, 2)).replace("s", f).replace("SSS", Ge(h, 3)), t;
}
function _x(t) {
  return t && t.charAt(0).toUpperCase() + t.substr(1);
}
function ba(t, e) {
  return e = e || "transparent", N(t) ? t : z(t) && t.colorStops && (t.colorStops[0] || {}).color || e;
}
var Xo = {}, vl = {}, fu = (
  /** @class */
  (function() {
    function t() {
      this._normalMasterList = [], this._nonSeriesBoxMasterList = [];
    }
    return t.prototype.create = function(e, r) {
      this._nonSeriesBoxMasterList = n(Xo), this._normalMasterList = n(vl);
      function n(i, a) {
        var o = [];
        return w(i, function(s, u) {
          var l = s.create(e, r);
          o = o.concat(l || []);
        }), o;
      }
    }, t.prototype.update = function(e, r) {
      w(this._normalMasterList, function(n) {
        n.update && n.update(e, r);
      });
    }, t.prototype.getCoordinateSystems = function() {
      return this._normalMasterList.concat(this._nonSeriesBoxMasterList);
    }, t.register = function(e, r) {
      if (e === "matrix" || e === "calendar") {
        Xo[e] = r;
        return;
      }
      vl[e] = r;
    }, t.get = function(e) {
      return vl[e] || Xo[e];
    }, t;
  })()
);
function Sx(t) {
  return !!Xo[t];
}
var bx = 1, Tx = 2, Cx = U();
function Gm(t) {
  var e = t.getShallow("coord", !0), r = bx;
  if (e == null) {
    var n = Cx.get(t.type);
    n && n.getCoord2 && (r = Tx, e = n.getCoord2(t));
  }
  return {
    coord: e,
    from: r
  };
}
var ii = 0, qo = 1, xx = 2;
function Mx(t, e) {
  var r = t.getShallow("coordinateSystem"), n = t.getShallow("coordinateSystemUsage", !0), i = ii;
  if (r) {
    var a = t.mainType === "series";
    n == null && (n = a ? "data" : "box"), n === "data" ? (i = qo, a || (i = ii)) : n === "box" && (i = xx, !a && !Sx(r) && (i = ii));
  }
  return {
    coordSysType: r,
    kind: i
  };
}
function kR(t) {
  var e = t.targetModel, r = t.coordSysType, n = t.coordSysProvider, i = t.isDefaultDataCoordSys, a = Mx(e), o = a.kind, s = a.coordSysType;
  if (i && o !== qo && (o = qo, s = r), o === ii || s !== r)
    return ii;
  var u = n(r, e);
  return u ? (o === qo ? e.coordinateSystem = u : e.boxCoordinateSystem = u, o) : ii;
}
var $o = w, Dx = ["left", "right", "top", "bottom", "width", "height"], fo = [["width", "left", "right"], ["height", "top", "bottom"]];
function nc(t, e, r, n, i) {
  var a = 0, o = 0;
  n == null && (n = 1 / 0), i == null && (i = 1 / 0);
  var s = 0;
  e.eachChild(function(u, l) {
    var f = u.getBoundingRect(), h = e.childAt(l + 1), c = h && h.getBoundingRect(), v, d;
    if (t === "horizontal") {
      var y = f.width + (c ? -c.x + f.x : 0);
      v = a + y, v > n || u.newline ? (a = 0, v = y, o += s + r, s = f.height) : s = Math.max(s, f.height);
    } else {
      var p = f.height + (c ? -c.y + f.y : 0);
      d = o + p, d > i || u.newline ? (a += s + r, o = 0, d = p, s = f.width) : s = Math.max(s, f.width);
    }
    u.newline || (u.x = a, u.y = o, u.markRedraw(), t === "horizontal" ? a = v + r : o = d + r);
  });
}
var oa = nc;
we(nc, "vertical");
we(nc, "horizontal");
function Ix(t, e) {
  return {
    left: t.getShallow("left", e),
    top: t.getShallow("top", e),
    right: t.getShallow("right", e),
    bottom: t.getShallow("bottom", e),
    width: t.getShallow("width", e),
    height: t.getShallow("height", e)
  };
}
function ys(t, e, r) {
  r = Ha(r || 0);
  var n = e.width, i = e.height, a = Dt(t.left, n), o = Dt(t.top, i), s = Dt(t.right, n), u = Dt(t.bottom, i), l = Dt(t.width, n), f = Dt(t.height, i), h = r[2] + r[0], c = r[1] + r[3], v = t.aspect;
  switch (isNaN(l) && (l = n - s - c - a), isNaN(f) && (f = i - u - h - o), v != null && (isNaN(l) && isNaN(f) && (v > n / i ? l = n * 0.8 : f = i * 0.8), isNaN(l) && (l = v * f), isNaN(f) && (f = l / v)), isNaN(a) && (a = n - s - l - c), isNaN(o) && (o = i - u - f - h), t.left || t.right) {
    case "center":
      a = n / 2 - l / 2 - r[3];
      break;
    case "right":
      a = n - l - c;
      break;
  }
  switch (t.top || t.bottom) {
    case "middle":
    case "center":
      o = i / 2 - f / 2 - r[0];
      break;
    case "bottom":
      o = i - f - h;
      break;
  }
  a = a || 0, o = o || 0, isNaN(l) && (l = n - c - a - (s || 0)), isNaN(f) && (f = i - h - o - (u || 0));
  var d = new Y((e.x || 0) + a + r[3], (e.y || 0) + o + r[0], l, f);
  return d.margin = r, d;
}
var dl = {
  rect: 1
};
function Ax(t, e, r) {
  var n, i, a, o = t.boxCoordinateSystem, s;
  if (o) {
    var u = Gm(t), l = u.coord, f = u.from;
    if (o.dataToLayout) {
      a = dl.rect, s = f;
      var h = o.dataToLayout(l);
      n = h.contentRect || h.rect;
    }
  }
  return a == null && (a = dl.rect), a === dl.rect && (n || (n = {
    x: 0,
    y: 0,
    width: e.getWidth(),
    height: e.getHeight()
  }), i = [n.x + n.width / 2, n.y + n.height / 2]), {
    type: a,
    refContainer: n,
    refPoint: i,
    boxCoordFrom: s
  };
}
function ms(t) {
  var e = t.layoutMode || t.constructor.layoutMode;
  return z(e) ? e : e ? {
    type: e
  } : null;
}
function Ta(t, e, r) {
  var n = r && r.ignoreSize;
  !B(n) && (n = [n, n]);
  var i = o(fo[0], 0), a = o(fo[1], 1);
  u(fo[0], t, i), u(fo[1], t, a);
  function o(l, f) {
    var h = {}, c = 0, v = {}, d = 0, y = 2;
    if ($o(l, function(m) {
      v[m] = t[m];
    }), $o(l, function(m) {
      qe(e, m) && (h[m] = v[m] = e[m]), s(h, m) && c++, s(v, m) && d++;
    }), n[f])
      return s(e, l[1]) ? v[l[2]] = null : s(e, l[2]) && (v[l[1]] = null), v;
    if (d === y || !c)
      return v;
    if (c >= y)
      return h;
    for (var p = 0; p < l.length; p++) {
      var g = l[p];
      if (!qe(h, g) && qe(t, g)) {
        h[g] = t[g];
        break;
      }
    }
    return h;
  }
  function s(l, f) {
    return l[f] != null && l[f] !== "auto";
  }
  function u(l, f, h) {
    $o(l, function(c) {
      f[c] = h[c];
    });
  }
}
function ic(t) {
  return Px({}, t);
}
function Px(t, e) {
  return e && t && $o(Dx, function(r) {
    qe(e, r) && (t[r] = e[r]);
  }), t;
}
var Lx = de(), ae = (
  /** @class */
  (function(t) {
    F(e, t);
    function e(r, n, i) {
      var a = t.call(this, r, n, i) || this;
      return a.uid = lu("ec_cpt_model"), a;
    }
    return e.prototype.init = function(r, n, i) {
      this.mergeDefaultAndTheme(r, i);
    }, e.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = ms(this), a = i ? ic(r) : {}, o = n.getTheme();
      pe(r, o.get(this.mainType)), pe(r, this.getDefaultOption()), i && Ta(r, a, i);
    }, e.prototype.mergeOption = function(r, n) {
      pe(this.option, r, !0);
      var i = ms(this);
      i && Ta(this.option, r, i);
    }, e.prototype.optionUpdated = function(r, n) {
    }, e.prototype.getDefaultOption = function() {
      var r = this.constructor;
      if (!PT(r))
        return r.defaultOption;
      var n = Lx(this);
      if (!n.defaultOption) {
        for (var i = [], a = r; a; ) {
          var o = a.prototype.defaultOption;
          o && i.push(o), a = a.superClass;
        }
        for (var s = {}, u = i.length - 1; u >= 0; u--)
          s = pe(s, i[u], !0);
        n.defaultOption = s;
      }
      return n.defaultOption;
    }, e.prototype.getReferringComponents = function(r, n) {
      var i = r + "Index", a = r + "Id";
      return ka(this.ecModel, r, {
        index: this.get(i, !0),
        id: this.get(a, !0)
      }, n);
    }, e.prototype.getBoxLayoutParams = function() {
      return Ix(this, !1);
    }, e.prototype.getZLevelKey = function() {
      return "";
    }, e.prototype.setZLevel = function(r) {
      this.option.zlevel = r;
    }, e.protoInitialize = (function() {
      var r = e.prototype;
      r.type = "component", r.id = "", r.name = "", r.mainType = "", r.subType = "", r.componentIndex = 0;
    })(), e;
  })(ce)
);
My(ae, ce);
$s(ae);
QC(ae);
JC(ae, Rx);
function Rx(t) {
  var e = [];
  return w(ae.getClassesByMainType(t), function(r) {
    e = e.concat(r.dependencies || r.prototype.dependencies || []);
  }), e = H(e, function(r) {
    return Vt(r).main;
  }), t !== "dataset" && oe(e, "dataset") <= 0 && e.unshift("dataset"), e;
}
var j = {
  color: {},
  darkColor: {},
  size: {}
}, be = j.color = {
  theme: ["#5070dd", "#b6d634", "#505372", "#ff994d", "#0ca8df", "#ffd10a", "#fb628b", "#785db0", "#3fbe95"],
  neutral00: "#fff",
  neutral05: "#f4f7fd",
  neutral10: "#e8ebf0",
  neutral15: "#dbdee4",
  neutral20: "#cfd2d7",
  neutral25: "#c3c5cb",
  neutral30: "#b7b9be",
  neutral35: "#aaacb2",
  neutral40: "#9ea0a5",
  neutral45: "#929399",
  neutral50: "#86878c",
  neutral55: "#797b7f",
  neutral60: "#6d6e73",
  neutral65: "#616266",
  neutral70: "#54555a",
  neutral75: "#48494d",
  neutral80: "#3c3c41",
  neutral85: "#303034",
  neutral90: "#232328",
  neutral95: "#17171b",
  neutral99: "#000",
  accent05: "#eff1f9",
  accent10: "#e0e4f2",
  accent15: "#d0d6ec",
  accent20: "#c0c9e6",
  accent25: "#b1bbdf",
  accent30: "#a1aed9",
  accent35: "#91a0d3",
  accent40: "#8292cc",
  accent45: "#7285c6",
  accent50: "#6578ba",
  accent55: "#5c6da9",
  accent60: "#536298",
  accent65: "#4a5787",
  accent70: "#404c76",
  accent75: "#374165",
  accent80: "#2e3654",
  accent85: "#252b43",
  accent90: "#1b2032",
  accent95: "#121521",
  transparent: "rgba(0,0,0,0)",
  highlight: "rgba(255,231,130,0.8)"
};
E(be, {
  primary: be.neutral80,
  secondary: be.neutral70,
  tertiary: be.neutral60,
  quaternary: be.neutral50,
  disabled: be.neutral20,
  border: be.neutral30,
  borderTint: be.neutral20,
  borderShade: be.neutral40,
  background: be.neutral05,
  backgroundTint: "rgba(234,237,245,0.5)",
  backgroundTransparent: "rgba(255,255,255,0)",
  backgroundShade: be.neutral10,
  shadow: "rgba(0,0,0,0.2)",
  shadowTint: "rgba(129,130,136,0.2)",
  axisLine: be.neutral70,
  axisLineTint: be.neutral40,
  axisTick: be.neutral70,
  axisTickMinor: be.neutral60,
  axisLabel: be.neutral70,
  axisSplitLine: be.neutral15,
  axisMinorSplitLine: be.neutral05
});
for (var rn in be)
  if (be.hasOwnProperty(rn)) {
    var wd = be[rn];
    rn === "theme" ? j.darkColor.theme = be.theme.slice() : rn === "highlight" ? j.darkColor.highlight = "rgba(255,231,130,0.4)" : rn.indexOf("accent") === 0 ? j.darkColor[rn] = os(wd, null, function(t) {
      return t * 0.5;
    }, function(t) {
      return Math.min(1, 1.3 - t);
    }) : j.darkColor[rn] = os(wd, null, function(t) {
      return t * 0.9;
    }, function(t) {
      return 1 - Math.pow(t, 1.5);
    });
  }
j.size = {
  xxs: 2,
  xs: 5,
  s: 10,
  m: 15,
  l: 20,
  xl: 30,
  xxl: 40,
  xxxl: 50
};
var Um = "";
typeof navigator < "u" && (Um = navigator.platform || "");
var Hn = "rgba(0, 0, 0, 0.2)", Ym = j.color.theme[0], Ex = os(Ym, null, null, 0.9);
const Wm = {
  darkMode: "auto",
  // backgroundColor: 'rgba(0,0,0,0)',
  colorBy: "series",
  color: j.color.theme,
  gradientColor: [Ex, Ym],
  aria: {
    decal: {
      decals: [{
        color: Hn,
        dashArrayX: [1, 0],
        dashArrayY: [2, 5],
        symbolSize: 1,
        rotation: Math.PI / 6
      }, {
        color: Hn,
        symbol: "circle",
        dashArrayX: [[8, 8], [0, 8, 8, 0]],
        dashArrayY: [6, 0],
        symbolSize: 0.8
      }, {
        color: Hn,
        dashArrayX: [1, 0],
        dashArrayY: [4, 3],
        rotation: -Math.PI / 4
      }, {
        color: Hn,
        dashArrayX: [[6, 6], [0, 6, 6, 0]],
        dashArrayY: [6, 0]
      }, {
        color: Hn,
        dashArrayX: [[1, 0], [1, 6]],
        dashArrayY: [1, 0, 6, 0],
        rotation: Math.PI / 4
      }, {
        color: Hn,
        symbol: "triangle",
        dashArrayX: [[9, 9], [0, 9, 9, 0]],
        dashArrayY: [7, 2],
        symbolSize: 0.75
      }]
    }
  },
  // If xAxis and yAxis declared, grid is created by default.
  // grid: {},
  textStyle: {
    // color: '#000',
    // decoration: 'none',
    // PENDING
    fontFamily: Um.match(/^Win/) ? "Microsoft YaHei" : "sans-serif",
    // fontFamily: 'Arial, Verdana, sans-serif',
    fontSize: 12,
    fontStyle: "normal",
    fontWeight: "normal"
  },
  // http://blogs.adobe.com/webplatform/2014/02/24/using-blend-modes-in-html-canvas/
  // https://developer.mozilla.org/en-US/docs/Web/API/CanvasRenderingContext2D/globalCompositeOperation
  // Default is source-over
  blendMode: null,
  stateAnimation: {
    duration: 300,
    easing: "cubicOut"
  },
  animation: "auto",
  animationDuration: 1e3,
  animationDurationUpdate: 500,
  animationEasing: "cubicInOut",
  animationEasingUpdate: "cubicInOut",
  animationThreshold: 2e3,
  // Configuration for progressive/incremental rendering
  progressiveThreshold: 3e3,
  progressive: 400,
  // Threshold of if use single hover layer to optimize.
  // It is recommended that `hoverLayerThreshold` is equivalent to or less than
  // `progressiveThreshold`, otherwise hover will cause restart of progressive,
  // which is unexpected.
  // see example <echarts/test/heatmap-large.html>.
  hoverLayerThreshold: 3e3,
  // See: module:echarts/scale/Time
  useUTC: !1
};
var nt = {
  Must: 1,
  Might: 2,
  Not: 3
  // Other cases
}, Xm = de();
function Ox(t) {
  Xm(t).datasetMap = U();
}
function kx(t, e, r) {
  var n = {}, i = qm(e);
  if (!i || !t)
    return n;
  var a = [], o = [], s = e.ecModel, u = Xm(s).datasetMap, l = i.uid + "_" + r.seriesLayoutBy, f, h;
  t = t.slice(), w(t, function(y, p) {
    var g = z(y) ? y : t[p] = {
      name: y
    };
    g.type === "ordinal" && f == null && (f = p, h = d(g)), n[g.name] = [];
  });
  var c = u.get(l) || u.set(l, {
    categoryWayDim: h,
    valueWayDim: 0
  });
  w(t, function(y, p) {
    var g = y.name, m = d(y);
    if (f == null) {
      var _ = c.valueWayDim;
      v(n[g], _, m), v(o, _, m), c.valueWayDim += m;
    } else if (f === p)
      v(n[g], 0, m), v(a, 0, m);
    else {
      var _ = c.categoryWayDim;
      v(n[g], _, m), v(o, _, m), c.categoryWayDim += m;
    }
  });
  function v(y, p, g) {
    for (var m = 0; m < g; m++)
      y.push(p + m);
  }
  function d(y) {
    var p = y.dimsDef;
    return p ? p.length : 1;
  }
  return a.length && (n.itemName = a), o.length && (n.seriesName = o), n;
}
function qm(t) {
  var e = t.get("data", !0);
  if (!e)
    return ka(t.ecModel, "dataset", {
      index: t.get("datasetIndex", !0),
      id: t.get("datasetId", !0)
    }, ur).models[0];
}
function Bx(t) {
  return !t.get("transform", !0) && !t.get("fromTransformResult", !0) ? [] : ka(t.ecModel, "dataset", {
    index: t.get("fromDatasetIndex", !0),
    id: t.get("fromDatasetId", !0)
  }, ur).models;
}
function $m(t, e) {
  return Nx(t.data, t.sourceFormat, t.seriesLayoutBy, t.dimensionsDefine, t.startIndex, e);
}
function Nx(t, e, r, n, i, a) {
  var o, s = 5;
  if (Ke(t))
    return nt.Not;
  var u, l;
  if (n) {
    var f = n[a];
    z(f) ? (u = f.name, l = f.type) : N(f) && (u = f);
  }
  if (l != null)
    return l === "ordinal" ? nt.Must : nt.Not;
  if (e === ze) {
    var h = t;
    if (r === An) {
      for (var c = h[a], v = 0; v < (c || []).length && v < s; v++)
        if ((o = S(c[i + v])) != null)
          return o;
    } else
      for (var v = 0; v < h.length && v < s; v++) {
        var d = h[i + v];
        if (d && (o = S(d[a])) != null)
          return o;
      }
  } else if (e === Rt) {
    var y = t;
    if (!u)
      return nt.Not;
    for (var v = 0; v < y.length && v < s; v++) {
      var p = y[v];
      if (p && (o = S(p[u])) != null)
        return o;
    }
  } else if (e === jt) {
    var g = t;
    if (!u)
      return nt.Not;
    var c = g[u];
    if (!c || Ke(c))
      return nt.Not;
    for (var v = 0; v < c.length && v < s; v++)
      if ((o = S(c[v])) != null)
        return o;
  } else if (e === ft)
    for (var m = t, v = 0; v < m.length && v < s; v++) {
      var p = m[v], _ = Ea(p);
      if (!B(_))
        return nt.Not;
      if ((o = S(_[a])) != null)
        return o;
    }
  function S(b) {
    var T = N(b);
    if (b != null && isFinite(Number(b)) && b !== "")
      return T ? nt.Might : nt.Not;
    if (T && b !== "-")
      return nt.Must;
  }
  return nt.Not;
}
var Fx = U();
function zx(t, e, r) {
  var n = Fx.get(e);
  if (!n)
    return r;
  var i = n(t);
  return i ? r.concat(i) : r;
}
var Cd = de();
de();
var ac = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getColorFromPalette = function(e, r, n) {
      var i = $e(this.get("color", !0)), a = this.get("colorLayer", !0);
      return Vx(this, Cd, i, a, e, r, n);
    }, t.prototype.clearColorPalette = function() {
      Gx(this, Cd);
    }, t;
  })()
);
function Hx(t, e) {
  for (var r = t.length, n = 0; n < r; n++)
    if (t[n].length > e)
      return t[n];
  return t[r - 1];
}
function Vx(t, e, r, n, i, a, o) {
  a = a || t;
  var s = e(a), u = s.paletteIdx || 0, l = s.paletteNameMap = s.paletteNameMap || {};
  if (l.hasOwnProperty(i))
    return l[i];
  var f = o == null || !n ? r : Hx(n, o);
  if (f = f || r, !(!f || !f.length)) {
    var h = f[u];
    return i && (l[i] = h), s.paletteIdx = (u + 1) % f.length, h;
  }
}
function Gx(t, e) {
  e(t).paletteIdx = 0, e(t).paletteNameMap = {};
}
var ho, Ei, xd, Md = "\0_ec_inner", Ux = 1, oc = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      return t !== null && t.apply(this, arguments) || this;
    }
    return e.prototype.init = function(r, n, i, a, o, s) {
      a = a || {}, this.option = null, this._theme = new ce(a), this._locale = new ce(o), this._optionManager = s;
    }, e.prototype.setOption = function(r, n, i) {
      var a = Ad(n);
      this._optionManager.setOption(r, i, a), this._resetOption(null, a);
    }, e.prototype.resetOption = function(r, n) {
      return this._resetOption(r, Ad(n));
    }, e.prototype._resetOption = function(r, n) {
      var i = !1, a = this._optionManager;
      if (!r || r === "recreate") {
        var o = a.mountOption(r === "recreate");
        !this.option || r === "recreate" ? xd(this, o) : (this.restoreData(), this._mergeOption(o, n)), i = !0;
      }
      if ((r === "timeline" || r === "media") && this.restoreData(), !r || r === "recreate" || r === "timeline") {
        var s = a.getTimelineOption(this);
        s && (i = !0, this._mergeOption(s, n));
      }
      if (!r || r === "recreate" || r === "media") {
        var u = a.getMediaOption(this);
        u.length && w(u, function(l) {
          i = !0, this._mergeOption(l, n);
        }, this);
      }
      return i;
    }, e.prototype.mergeOption = function(r) {
      this._mergeOption(r, null);
    }, e.prototype._mergeOption = function(r, n) {
      var i = this.option, a = this._componentsMap, o = this._componentsCount, s = [], u = U(), l = n && n.replaceMergeMainTypeMap;
      Ox(this), w(r, function(h, c) {
        h != null && (ae.hasClass(c) ? c && (s.push(c), u.set(c, !0)) : i[c] = i[c] == null ? Z(h) : pe(i[c], h, !0));
      }), l && l.each(function(h, c) {
        ae.hasClass(c) && !u.get(c) && (s.push(c), u.set(c, !0));
      }), ae.topologicalTravel(s, ae.getAllClassMainTypes(), f, this);
      function f(h) {
        var c = zx(this, h, $e(r[h])), v = a.get(h), d = (
          // `!oldCmptList` means init. See the comment in `mappingToExists`
          v ? l && l.get(h) ? "replaceMerge" : "normalMerge" : "replaceAll"
        ), y = aT(v, c, d);
        cT(y, h, ae), i[h] = null, a.set(h, null), o.set(h, 0);
        var p = [], g = [], m = 0, _;
        w(y, function(S, b) {
          var T = S.existing, C = S.newOption;
          if (!C)
            T && (T.mergeOption({}, this), T.optionUpdated({}, !1));
          else {
            var M = h === "series", x = ae.getClass(
              h,
              S.keyInfo.subType,
              !M
              // Give a more detailed warn later if series don't exists
            );
            if (!x)
              return;
            if (h === "tooltip") {
              if (_)
                return;
              _ = !0;
            }
            if (T && T.constructor === x)
              T.name = S.keyInfo.name, T.mergeOption(C, this), T.optionUpdated(C, !1);
            else {
              var D = E({
                componentIndex: b
              }, S.keyInfo);
              T = new x(C, this, this, D), E(T, D), S.brandNew && (T.__requireNewView = !0), T.init(C, this, this), T.optionUpdated(null, !0);
            }
          }
          T ? (p.push(T.option), g.push(T), m++) : (p.push(void 0), g.push(void 0));
        }, this), i[h] = p, a.set(h, g), o.set(h, m), h === "series" && ho(this);
      }
      this._seriesIndices || ho(this);
    }, e.prototype.getOption = function() {
      var r = Z(this.option);
      return w(r, function(n, i) {
        if (ae.hasClass(i)) {
          for (var a = $e(n), o = a.length, s = !1, u = o - 1; u >= 0; u--)
            a[u] && !ma(a[u]) ? s = !0 : (a[u] = null, !s && o--);
          a.length = o, r[i] = a;
        }
      }), delete r[Md], r;
    }, e.prototype.setTheme = function(r) {
      this._theme = new ce(r), this._resetOption("recreate", null);
    }, e.prototype.getTheme = function() {
      return this._theme;
    }, e.prototype.getLocaleModel = function() {
      return this._locale;
    }, e.prototype.setUpdatePayload = function(r) {
      this._payload = r;
    }, e.prototype.getUpdatePayload = function() {
      return this._payload;
    }, e.prototype.getComponent = function(r, n) {
      var i = this._componentsMap.get(r);
      if (i) {
        var a = i[n || 0];
        if (a)
          return a;
        if (n == null) {
          for (var o = 0; o < i.length; o++)
            if (i[o])
              return i[o];
        }
      }
    }, e.prototype.queryComponents = function(r) {
      var n = r.mainType;
      if (!n)
        return [];
      var i = r.index, a = r.id, o = r.name, s = this._componentsMap.get(n);
      if (!s || !s.length)
        return [];
      var u;
      return i != null ? (u = [], w($e(i), function(l) {
        s[l] && u.push(s[l]);
      })) : a != null ? u = Dd("id", a, s) : o != null ? u = Dd("name", o, s) : u = xe(s, function(l) {
        return !!l;
      }), Id(u, r);
    }, e.prototype.findComponents = function(r) {
      var n = r.query, i = r.mainType, a = s(n), o = a ? this.queryComponents(a) : xe(this._componentsMap.get(i), function(l) {
        return !!l;
      });
      return u(Id(o, r));
      function s(l) {
        var f = i + "Index", h = i + "Id", c = i + "Name";
        return l && (l[f] != null || l[h] != null || l[c] != null) ? {
          mainType: i,
          // subType will be filtered finally.
          index: l[f],
          id: l[h],
          name: l[c]
        } : null;
      }
      function u(l) {
        return r.filter ? xe(l, r.filter) : l;
      }
    }, e.prototype.eachComponent = function(r, n, i) {
      var a = this._componentsMap;
      if (X(r)) {
        var o = n, s = r;
        a.each(function(h, c) {
          for (var v = 0; h && v < h.length; v++) {
            var d = h[v];
            d && s.call(o, c, d, d.componentIndex);
          }
        });
      } else
        for (var u = N(r) ? a.get(r) : z(r) ? this.findComponents(r) : null, l = 0; u && l < u.length; l++) {
          var f = u[l];
          f && n.call(i, f, f.componentIndex);
        }
    }, e.prototype.getSeriesByName = function(r) {
      var n = Yt(r, null);
      return xe(this._componentsMap.get("series"), function(i) {
        return !!i && n != null && i.name === n;
      });
    }, e.prototype.getSeriesByIndex = function(r) {
      return this._componentsMap.get("series")[r];
    }, e.prototype.getSeriesByType = function(r) {
      return xe(this._componentsMap.get("series"), function(n) {
        return !!n && n.subType === r;
      });
    }, e.prototype.getSeries = function() {
      return xe(this._componentsMap.get("series"), function(r) {
        return !!r;
      });
    }, e.prototype.getSeriesCount = function() {
      return this._componentsCount.get("series");
    }, e.prototype.eachSeries = function(r, n) {
      Ei(this), w(this._seriesIndices, function(i) {
        var a = this._componentsMap.get("series")[i];
        r.call(n, a, i);
      }, this);
    }, e.prototype.eachRawSeries = function(r, n) {
      w(this._componentsMap.get("series"), function(i) {
        i && r.call(n, i, i.componentIndex);
      });
    }, e.prototype.eachSeriesByType = function(r, n, i) {
      Ei(this), w(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        o.subType === r && n.call(i, o, a);
      }, this);
    }, e.prototype.eachRawSeriesByType = function(r, n, i) {
      return w(this.getSeriesByType(r), n, i);
    }, e.prototype.isSeriesFiltered = function(r) {
      return Ei(this), this._seriesIndicesMap.get(r.componentIndex) == null;
    }, e.prototype.getCurrentSeriesIndices = function() {
      return (this._seriesIndices || []).slice();
    }, e.prototype.filterSeries = function(r, n) {
      Ei(this);
      var i = [];
      w(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        r.call(n, o, a) && i.push(a);
      }, this), this._seriesIndices = i, this._seriesIndicesMap = U(i);
    }, e.prototype.restoreData = function(r) {
      ho(this);
      var n = this._componentsMap, i = [];
      n.each(function(a, o) {
        ae.hasClass(o) && i.push(o);
      }), ae.topologicalTravel(i, ae.getAllClassMainTypes(), function(a) {
        w(n.get(a), function(o) {
          o && (a !== "series" || !Yx(o, r)) && o.restoreData();
        });
      });
    }, e.internalField = (function() {
      ho = function(r) {
        var n = r._seriesIndices = [];
        w(r._componentsMap.get("series"), function(i) {
          i && n.push(i.componentIndex);
        }), r._seriesIndicesMap = U(n);
      }, Ei = function(r) {
      }, xd = function(r, n) {
        r.option = {}, r.option[Md] = Ux, r._componentsMap = U({
          series: []
        }), r._componentsCount = U();
        var i = n.aria;
        z(i) && i.enabled == null && (i.enabled = !0), Wx(n, r._theme.option), pe(n, Wm, !1), r._mergeOption(n, null);
      };
    })(), e;
  })(ce)
);
function Yx(t, e) {
  if (e) {
    var r = e.seriesIndex, n = e.seriesId, i = e.seriesName;
    return r != null && t.componentIndex !== r || n != null && t.id !== n || i != null && t.name !== i;
  }
}
function Wx(t, e) {
  var r = t.color && !t.colorLayer;
  w(e, function(n, i) {
    i === "colorLayer" && r || i === "color" && t.color || ae.hasClass(i) || (typeof n == "object" ? t[i] = t[i] ? pe(t[i], n, !1) : Z(n) : t[i] == null && (t[i] = n));
  });
}
function Dd(t, e, r) {
  if (B(e)) {
    var n = U();
    return w(e, function(a) {
      if (a != null) {
        var o = Yt(a, null);
        o != null && n.set(a, !0);
      }
    }), xe(r, function(a) {
      return a && n.get(a[t]);
    });
  } else {
    var i = Yt(e, null);
    return xe(r, function(a) {
      return a && i != null && a[t] === i;
    });
  }
}
function Id(t, e) {
  return e.hasOwnProperty("subType") ? xe(t, function(r) {
    return r && r.subType === e.subType;
  }) : t;
}
function Ad(t) {
  var e = U();
  return t && w($e(t.replaceMerge), function(r) {
    e.set(r, !0);
  }), {
    replaceMergeMainTypeMap: e
  };
}
Lt(oc, ac);
var Xx = /^(min|max)?(.+)$/, qx = (
  /** @class */
  (function() {
    function t(e) {
      this._timelineOptions = [], this._mediaList = [], this._currentMediaIndices = [], this._api = e;
    }
    return t.prototype.setOption = function(e, r, n) {
      e && (w($e(e.series), function(o) {
        o && o.data && Ke(o.data) && va(o.data);
      }), w($e(e.dataset), function(o) {
        o && o.source && Ke(o.source) && va(o.source);
      })), e = Z(e);
      var i = this._optionBackup, a = $x(e, r, !i);
      this._newBaseOption = a.baseOption, i ? (a.timelineOptions.length && (i.timelineOptions = a.timelineOptions), a.mediaList.length && (i.mediaList = a.mediaList), a.mediaDefault && (i.mediaDefault = a.mediaDefault)) : this._optionBackup = a;
    }, t.prototype.mountOption = function(e) {
      var r = this._optionBackup;
      return this._timelineOptions = r.timelineOptions, this._mediaList = r.mediaList, this._mediaDefault = r.mediaDefault, this._currentMediaIndices = [], Z(e ? r.baseOption : this._newBaseOption);
    }, t.prototype.getTimelineOption = function(e) {
      var r, n = this._timelineOptions;
      if (n.length) {
        var i = e.getComponent("timeline");
        i && (r = Z(
          // FIXME:TS as TimelineModel or quivlant interface
          n[i.getCurrentIndex()]
        ));
      }
      return r;
    }, t.prototype.getMediaOption = function(e) {
      var r = this._api.getWidth(), n = this._api.getHeight(), i = this._mediaList, a = this._mediaDefault, o = [], s = [];
      if (!i.length && !a)
        return s;
      for (var u = 0, l = i.length; u < l; u++)
        Zx(i[u].query, r, n) && o.push(u);
      return !o.length && a && (o = [-1]), o.length && !jx(o, this._currentMediaIndices) && (s = H(o, function(f) {
        return Z(f === -1 ? a.option : i[f].option);
      })), this._currentMediaIndices = o, s;
    }, t;
  })()
);
function $x(t, e, r) {
  var n = [], i, a, o = t.baseOption, s = t.timeline, u = t.options, l = t.media, f = !!t.media, h = !!(u || s || o && o.timeline);
  o ? (a = o, a.timeline || (a.timeline = s)) : ((h || f) && (t.options = t.media = null), a = t), f && B(l) && w(l, function(v) {
    v && v.option && (v.query ? n.push(v) : i || (i = v));
  }), c(a), w(u, function(v) {
    return c(v);
  }), w(n, function(v) {
    return c(v.option);
  });
  function c(v) {
    w(e, function(d) {
      d(v, r);
    });
  }
  return {
    baseOption: a,
    timelineOptions: u || [],
    mediaDefault: i,
    mediaList: n
  };
}
function Zx(t, e, r) {
  var n = {
    width: e,
    height: r,
    aspectratio: e / r
    // lower case for convenience.
  }, i = !0;
  return w(t, function(a, o) {
    var s = o.match(Xx);
    if (!(!s || !s[1] || !s[2])) {
      var u = s[1], l = s[2].toLowerCase();
      Kx(n[l], a, u) || (i = !1);
    }
  }), i;
}
function Kx(t, e, r) {
  return r === "min" ? t >= e : r === "max" ? t <= e : t === e;
}
function jx(t, e) {
  return t.join(",") === e.join(",");
}
var Ct = w, wa = z, Pd = ["areaStyle", "lineStyle", "nodeStyle", "linkStyle", "chordStyle", "label", "labelLine"];
function pl(t) {
  var e = t && t.itemStyle;
  if (e)
    for (var r = 0, n = Pd.length; r < n; r++) {
      var i = Pd[r], a = e.normal, o = e.emphasis;
      a && a[i] && (t[i] = t[i] || {}, t[i].normal ? pe(t[i].normal, a[i]) : t[i].normal = a[i], a[i] = null), o && o[i] && (t[i] = t[i] || {}, t[i].emphasis ? pe(t[i].emphasis, o[i]) : t[i].emphasis = o[i], o[i] = null);
    }
}
function Ne(t, e, r) {
  if (t && t[e] && (t[e].normal || t[e].emphasis)) {
    var n = t[e].normal, i = t[e].emphasis;
    n && (r ? (t[e].normal = t[e].emphasis = null, _e(t[e], n)) : t[e] = n), i && (t.emphasis = t.emphasis || {}, t.emphasis[e] = i, i.focus && (t.emphasis.focus = i.focus), i.blurScope && (t.emphasis.blurScope = i.blurScope));
  }
}
function ji(t) {
  Ne(t, "itemStyle"), Ne(t, "lineStyle"), Ne(t, "areaStyle"), Ne(t, "label"), Ne(t, "labelLine"), Ne(t, "upperLabel"), Ne(t, "edgeLabel");
}
function Te(t, e) {
  var r = wa(t) && t[e], n = wa(r) && r.textStyle;
  if (n)
    for (var i = 0, a = Lv.length; i < a; i++) {
      var o = Lv[i];
      n.hasOwnProperty(o) && (r[o] = n[o]);
    }
}
function pt(t) {
  t && (ji(t), Te(t, "label"), t.emphasis && Te(t.emphasis, "label"));
}
function Qx(t) {
  if (wa(t)) {
    pl(t), ji(t), Te(t, "label"), Te(t, "upperLabel"), Te(t, "edgeLabel"), t.emphasis && (Te(t.emphasis, "label"), Te(t.emphasis, "upperLabel"), Te(t.emphasis, "edgeLabel"));
    var e = t.markPoint;
    e && (pl(e), pt(e));
    var r = t.markLine;
    r && (pl(r), pt(r));
    var n = t.markArea;
    n && pt(n);
    var i = t.data;
    if (t.type === "graph") {
      i = i || t.nodes;
      var a = t.links || t.edges;
      if (a && !Ke(a))
        for (var o = 0; o < a.length; o++)
          pt(a[o]);
      w(t.categories, function(l) {
        ji(l);
      });
    }
    if (i && !Ke(i))
      for (var o = 0; o < i.length; o++)
        pt(i[o]);
    if (e = t.markPoint, e && e.data)
      for (var s = e.data, o = 0; o < s.length; o++)
        pt(s[o]);
    if (r = t.markLine, r && r.data)
      for (var u = r.data, o = 0; o < u.length; o++)
        B(u[o]) ? (pt(u[o][0]), pt(u[o][1])) : pt(u[o]);
    t.type === "gauge" ? (Te(t, "axisLabel"), Te(t, "title"), Te(t, "detail")) : t.type === "treemap" ? (Ne(t.breadcrumb, "itemStyle"), w(t.levels, function(l) {
      ji(l);
    })) : t.type === "tree" && ji(t.leaves);
  }
}
function nr(t) {
  return B(t) ? t : t ? [t] : [];
}
function Ld(t) {
  return (B(t) ? t[0] : t) || {};
}
function Jx(t, e) {
  Ct(nr(t.series), function(n) {
    wa(n) && Qx(n);
  });
  var r = ["xAxis", "yAxis", "radiusAxis", "angleAxis", "singleAxis", "parallelAxis", "radar"];
  e && r.push("valueAxis", "categoryAxis", "logAxis", "timeAxis"), Ct(r, function(n) {
    Ct(nr(t[n]), function(i) {
      i && (Te(i, "axisLabel"), Te(i.axisPointer, "label"));
    });
  }), Ct(nr(t.parallel), function(n) {
    var i = n && n.parallelAxisDefault;
    Te(i, "axisLabel"), Te(i && i.axisPointer, "label");
  }), Ct(nr(t.calendar), function(n) {
    Ne(n, "itemStyle"), Te(n, "dayLabel"), Te(n, "monthLabel"), Te(n, "yearLabel");
  }), Ct(nr(t.radar), function(n) {
    Te(n, "name"), n.name && n.axisName == null && (n.axisName = n.name, delete n.name), n.nameGap != null && n.axisNameGap == null && (n.axisNameGap = n.nameGap, delete n.nameGap);
  }), Ct(nr(t.geo), function(n) {
    wa(n) && (pt(n), Ct(nr(n.regions), function(i) {
      pt(i);
    }));
  }), Ct(nr(t.timeline), function(n) {
    pt(n), Ne(n, "label"), Ne(n, "itemStyle"), Ne(n, "controlStyle", !0);
    var i = n.data;
    B(i) && w(i, function(a) {
      z(a) && (Ne(a, "label"), Ne(a, "itemStyle"));
    });
  }), Ct(nr(t.toolbox), function(n) {
    Ne(n, "iconStyle"), Ct(n.feature, function(i) {
      Ne(i, "iconStyle");
    });
  }), Te(Ld(t.axisPointer), "label"), Te(Ld(t.tooltip).axisPointer, "label");
}
function eM(t, e) {
  for (var r = e.split(","), n = t, i = 0; i < r.length && (n = n && n[r[i]], n != null); i++)
    ;
  return n;
}
function tM(t, e, r, n) {
  for (var i = e.split(","), a = t, o, s = 0; s < i.length - 1; s++)
    o = i[s], a[o] == null && (a[o] = {}), a = a[o];
  a[i[s]] == null && (a[i[s]] = r);
}
function Rd(t) {
  t && w(rM, function(e) {
    e[0] in t && !(e[1] in t) && (t[e[1]] = t[e[0]]);
  });
}
var rM = [["x", "left"], ["y", "top"], ["x2", "right"], ["y2", "bottom"]], nM = ["grid", "geo", "parallel", "legend", "toolbox", "title", "visualMap", "dataZoom", "timeline"], gl = [["borderRadius", "barBorderRadius"], ["borderColor", "barBorderColor"], ["borderWidth", "barBorderWidth"]];
function Oi(t) {
  var e = t && t.itemStyle;
  if (e)
    for (var r = 0; r < gl.length; r++) {
      var n = gl[r][1], i = gl[r][0];
      e[n] != null && (e[i] = e[n]);
    }
}
function Ed(t) {
  t && t.alignTo === "edge" && t.margin != null && t.edgeDistance == null && (t.edgeDistance = t.margin);
}
function Od(t) {
  t && t.downplay && !t.blur && (t.blur = t.downplay);
}
function iM(t) {
  t && t.focusNodeAdjacency != null && (t.emphasis = t.emphasis || {}, t.emphasis.focus == null && (t.emphasis.focus = "adjacency"));
}
function Zm(t, e) {
  if (t)
    for (var r = 0; r < t.length; r++)
      e(t[r]), t[r] && Zm(t[r].children, e);
}
function Km(t, e) {
  Jx(t, e), t.series = $e(t.series), w(t.series, function(r) {
    if (z(r)) {
      var n = r.type;
      if (n === "line")
        r.clipOverflow != null && (r.clip = r.clipOverflow);
      else if (n === "pie" || n === "gauge") {
        r.clockWise != null && (r.clockwise = r.clockWise), Ed(r.label);
        var i = r.data;
        if (i && !Ke(i))
          for (var a = 0; a < i.length; a++)
            Ed(i[a]);
        r.hoverOffset != null && (r.emphasis = r.emphasis || {}, (r.emphasis.scaleSize = null) && (r.emphasis.scaleSize = r.hoverOffset));
      } else if (n === "gauge") {
        var o = eM(r, "pointer.color");
        o != null && tM(r, "itemStyle.color", o);
      } else if (n === "bar") {
        Oi(r), Oi(r.backgroundStyle), Oi(r.emphasis);
        var i = r.data;
        if (i && !Ke(i))
          for (var a = 0; a < i.length; a++)
            typeof i[a] == "object" && (Oi(i[a]), Oi(i[a] && i[a].emphasis));
      } else if (n === "sunburst") {
        var s = r.highlightPolicy;
        s && (r.emphasis = r.emphasis || {}, r.emphasis.focus || (r.emphasis.focus = s)), Od(r), Zm(r.data, Od);
      } else n === "graph" || n === "sankey" ? iM(r) : n === "map" && (r.mapType && !r.map && (r.map = r.mapType), r.mapLocation && _e(r, r.mapLocation));
      r.hoverAnimation != null && (r.emphasis = r.emphasis || {}, r.emphasis && r.emphasis.scale == null && (r.emphasis.scale = r.hoverAnimation)), Rd(r);
    }
  }), t.dataRange && (t.visualMap = t.dataRange), w(nM, function(r) {
    var n = t[r];
    n && (B(n) || (n = [n]), w(n, function(i) {
      Rd(i);
    }));
  });
}
var aM = xh(oM);
function oM(t) {
  var e = U();
  t.eachSeries(function(r) {
    var n = r.get("stack");
    if (n) {
      var i = e.get(n) || e.set(n, []), a = r.getData(), o = {
        // Used for calculate axis extent automatically.
        // TODO: Type getCalculationInfo return more specific type?
        stackResultDimension: a.getCalculationInfo("stackResultDimension"),
        stackedOverDimension: a.getCalculationInfo("stackedOverDimension"),
        stackedDimension: a.getCalculationInfo("stackedDimension"),
        stackedByDimension: a.getCalculationInfo("stackedByDimension"),
        isStackedByIndex: a.getCalculationInfo("isStackedByIndex"),
        data: a,
        seriesModel: r
      };
      if (!o.stackedDimension || !(o.isStackedByIndex || o.stackedByDimension))
        return;
      i.push(o);
    }
  }), e.each(function(r) {
    if (r.length !== 0) {
      var n = r[0].seriesModel, i = n.get("stackOrder") || "seriesAsc";
      i === "seriesDesc" && r.reverse(), w(r, function(a, o) {
        a.data.setCalculationInfo("stackedOnSeries", o > 0 ? r[o - 1].seriesModel : null);
      }), sM(r);
    }
  });
}
function sM(t) {
  w(t, function(e, r) {
    var n = [], i = [NaN, NaN], a = [e.stackResultDimension, e.stackedOverDimension], o = e.data, s = e.isStackedByIndex, u = e.seriesModel.get("stackStrategy") || "samesign";
    o.modify(a, function(l, f, h) {
      var c = o.get(e.stackedDimension, h);
      if (isNaN(c))
        return i;
      var v, d;
      s ? d = o.getRawIndex(h) : v = o.get(e.stackedByDimension, h);
      for (var y = NaN, p = r - 1; p >= 0; p--) {
        var g = t[p];
        if (s || (d = g.data.rawIndexOf(g.stackedByDimension, v)), d >= 0) {
          var m = g.data.getByRawIndex(g.stackResultDimension, d);
          if (u === "all" || u === "positive" && m > 0 || u === "negative" && m < 0 || u === "samesign" && c >= 0 && m > 0 || u === "samesign" && c <= 0 && m < 0) {
            c = $b(c, m), y = m;
            break;
          }
        }
      }
      return n[0] = c, n[1] = y, n;
    });
  });
}
var hu = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.data = e.data || (e.sourceFormat === jt ? {} : []), this.sourceFormat = e.sourceFormat || zy, this.seriesLayoutBy = e.seriesLayoutBy || Pt, this.startIndex = e.startIndex || 0, this.dimensionsDetectedCount = e.dimensionsDetectedCount, this.metaRawOption = e.metaRawOption;
      var r = this.dimensionsDefine = e.dimensionsDefine;
      if (r)
        for (var n = 0; n < r.length; n++) {
          var i = r[n];
          i.type == null && $m(this, n) === nt.Must && (i.type = "ordinal");
        }
    }
    return t;
  })()
);
function sc(t) {
  return t instanceof hu;
}
function Ef(t, e, r) {
  r = r || Qm(t);
  var n = e.seriesLayoutBy, i = lM(t, r, n, e.sourceHeader, e.dimensions), a = new hu({
    data: t,
    sourceFormat: r,
    seriesLayoutBy: n,
    dimensionsDefine: i.dimensionsDefine,
    startIndex: i.startIndex,
    dimensionsDetectedCount: i.dimensionsDetectedCount,
    metaRawOption: Z(e)
  });
  return a;
}
function jm(t) {
  return new hu({
    data: t,
    sourceFormat: Ke(t) ? Lr : ft
  });
}
function uM(t) {
  return new hu({
    data: t.data,
    sourceFormat: t.sourceFormat,
    seriesLayoutBy: t.seriesLayoutBy,
    dimensionsDefine: Z(t.dimensionsDefine),
    startIndex: t.startIndex,
    dimensionsDetectedCount: t.dimensionsDetectedCount
  });
}
function Qm(t) {
  var e = zy;
  if (Ke(t))
    e = Lr;
  else if (B(t)) {
    t.length === 0 && (e = ze);
    for (var r = 0, n = t.length; r < n; r++) {
      var i = t[r];
      if (i != null) {
        if (B(i) || Ke(i)) {
          e = ze;
          break;
        } else if (z(i)) {
          e = Rt;
          break;
        }
      }
    }
  } else if (z(t)) {
    for (var a in t)
      if (qe(t, a) && Ze(t[a])) {
        e = jt;
        break;
      }
  }
  return e;
}
function lM(t, e, r, n, i) {
  var a, o;
  if (!t)
    return {
      dimensionsDefine: kd(i),
      startIndex: o,
      dimensionsDetectedCount: a
    };
  if (e === ze) {
    var s = t;
    n === "auto" || n == null ? Bd(function(l) {
      l != null && l !== "-" && (N(l) ? o == null && (o = 1) : o = 0);
    }, r, s, 10) : o = ve(n) ? n : n ? 1 : 0, !i && o === 1 && (i = [], Bd(function(l, f) {
      i[f] = l != null ? l + "" : "";
    }, r, s, 1 / 0)), a = i ? i.length : r === An ? s.length : s[0] ? s[0].length : null;
  } else if (e === Rt)
    i || (i = fM(t));
  else if (e === jt)
    i || (i = [], w(t, function(l, f) {
      i.push(f);
    }));
  else if (e === ft) {
    var u = Ea(t[0]);
    a = B(u) && u.length || 1;
  }
  return {
    startIndex: o,
    dimensionsDefine: kd(i),
    dimensionsDetectedCount: a
  };
}
function fM(t) {
  for (var e = 0, r; e < t.length && !(r = t[e++]); )
    ;
  if (r)
    return me(r);
}
function kd(t) {
  if (t) {
    var e = U();
    return H(t, function(r, n) {
      r = z(r) ? r : {
        name: r
      };
      var i = {
        name: r.name,
        displayName: r.displayName,
        type: r.type
      };
      if (i.name == null)
        return i;
      i.name += "", i.displayName == null && (i.displayName = i.name);
      var a = e.get(i.name);
      return a ? i.name += "-" + a.count++ : e.set(i.name, {
        count: 1
      }), i;
    });
  }
}
function Bd(t, e, r, n) {
  if (e === An)
    for (var i = 0; i < r.length && i < n; i++)
      t(r[i] ? r[i][0] : null, i);
  else
    for (var a = r[0] || [], i = 0; i < a.length && i < n; i++)
      t(a[i], i);
}
function Jm(t) {
  var e = t.sourceFormat;
  return e === Rt || e === jt;
}
var nn, an, on, sn, Nd, Fd, e_ = (
  /** @class */
  (function() {
    function t(e, r) {
      var n = sc(e) ? e : jm(e);
      this._source = n;
      var i = this._data = n.data, a = n.sourceFormat;
      n.seriesLayoutBy, a === Lr && (this._offset = 0, this._dimSize = r, this._data = i), Fd(this, i, n);
    }
    return t.prototype.getSource = function() {
      return this._source;
    }, t.prototype.count = function() {
      return 0;
    }, t.prototype.getItem = function(e, r) {
    }, t.prototype.appendData = function(e) {
    }, t.prototype.clean = function() {
    }, t.protoInitialize = (function() {
      var e = t.prototype;
      e.pure = !1, e.persistent = !0;
    })(), t.internalField = (function() {
      var e;
      Fd = function(o, s, u) {
        var l = u.sourceFormat, f = u.seriesLayoutBy, h = u.startIndex, c = u.dimensionsDefine, v = Nd[uc(l, f)];
        if (E(o, v), l === Lr)
          o.getItem = r, o.count = i, o.fillStorage = n;
        else {
          var d = t_(l, f);
          o.getItem = le(d, null, s, h, c);
          var y = r_(l, f);
          o.count = le(y, null, s, h, c);
        }
      };
      var r = function(o, s) {
        o = o - this._offset, s = s || [];
        for (var u = this._data, l = this._dimSize, f = l * o, h = 0; h < l; h++)
          s[h] = u[f + h];
        return s;
      }, n = function(o, s, u, l) {
        for (var f = this._data, h = this._dimSize, c = 0; c < h; c++) {
          for (var v = l[c], d = v[0] == null ? 1 / 0 : v[0], y = v[1] == null ? -1 / 0 : v[1], p = s - o, g = u[c], m = 0; m < p; m++) {
            var _ = f[m * h + c];
            g[o + m] = _, _ < d && (d = _), _ > y && (y = _);
          }
          v[0] = d, v[1] = y;
        }
      }, i = function() {
        return this._data ? this._data.length / this._dimSize : 0;
      };
      Nd = (e = {}, e[ze + "_" + Pt] = {
        pure: !0,
        appendData: a
      }, e[ze + "_" + An] = {
        pure: !0,
        appendData: function() {
          throw new Error('Do not support appendData when set seriesLayoutBy: "row".');
        }
      }, e[Rt] = {
        pure: !0,
        appendData: a
      }, e[jt] = {
        pure: !0,
        appendData: function(o) {
          var s = this._data;
          w(o, function(u, l) {
            for (var f = s[l] || (s[l] = []), h = 0; h < (u || []).length; h++)
              f.push(u[h]);
          });
        }
      }, e[ft] = {
        appendData: a
      }, e[Lr] = {
        persistent: !1,
        pure: !0,
        appendData: function(o) {
          this._data = o;
        },
        // Clean self if data is already used.
        clean: function() {
          this._offset += this.count(), this._data = null;
        }
      }, e);
      function a(o) {
        for (var s = 0; s < o.length; s++)
          this._data.push(o[s]);
      }
    })(), t;
  })()
), co = function(t) {
  B(t) || _y("series.data or dataset.source must be an array.");
};
nn = {}, nn[ze + "_" + Pt] = co, nn[ze + "_" + An] = co, nn[Rt] = co, nn[jt] = function(t, e) {
  for (var r = 0; r < e.length; r++) {
    var n = e[r].name;
    n == null && _y("dimension name must not be null/undefined.");
  }
}, nn[ft] = co;
var zd = function(t, e, r, n) {
  return t[n];
}, hM = (an = {}, an[ze + "_" + Pt] = function(t, e, r, n) {
  return t[n + e];
}, an[ze + "_" + An] = function(t, e, r, n, i) {
  n += e;
  for (var a = i || [], o = t, s = 0; s < o.length; s++) {
    var u = o[s];
    a[s] = u ? u[n] : null;
  }
  return a;
}, an[Rt] = zd, an[jt] = function(t, e, r, n, i) {
  for (var a = i || [], o = 0; o < r.length; o++) {
    var s = r[o].name, u = s != null ? t[s] : null;
    a[o] = u ? u[n] : null;
  }
  return a;
}, an[ft] = zd, an);
function t_(t, e) {
  var r = hM[uc(t, e)];
  return r;
}
var Hd = function(t, e, r) {
  return t.length;
}, cM = (on = {}, on[ze + "_" + Pt] = function(t, e, r) {
  return Math.max(0, t.length - e);
}, on[ze + "_" + An] = function(t, e, r) {
  var n = t[0];
  return n ? Math.max(0, n.length - e) : 0;
}, on[Rt] = Hd, on[jt] = function(t, e, r) {
  var n = r[0].name, i = n != null ? t[n] : null;
  return i ? i.length : 0;
}, on[ft] = Hd, on);
function r_(t, e) {
  var r = cM[uc(t, e)];
  return r;
}
var yl = function(t, e, r) {
  return t[e];
}, vM = (sn = {}, sn[ze] = yl, sn[Rt] = function(t, e, r) {
  return t[r];
}, sn[jt] = yl, sn[ft] = function(t, e, r) {
  var n = Ea(t);
  return n instanceof Array ? n[e] : n;
}, sn[Lr] = yl, sn);
function n_(t) {
  var e = vM[t];
  return e;
}
function uc(t, e) {
  return t === ze ? t + "_" + e : t;
}
function _s(t, e, r) {
  if (t) {
    var n = t.getRawDataItem(e);
    if (n != null) {
      var i = t.getStore(), a = i.getSource().sourceFormat;
      if (r != null) {
        var o = t.getDimensionIndex(r), s = i.getDimensionProperty(o);
        return n_(a)(n, o, s);
      } else {
        var u = n;
        return a === ft && (u = Ea(n)), u;
      }
    }
  }
}
var dM = /\{@(.+?)\}/g, pM = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getDataParams = function(e, r) {
      var n = this.getData(r), i = this.getRawValue(e, r), a = n.getRawIndex(e), o = n.getName(e), s = n.getRawDataItem(e), u = n.getItemVisual(e, "style"), l = u && u[n.getItemVisual(e, "drawType") || "fill"], f = u && u.stroke, h = this.mainType, c = h === "series", v = n.userOutput && n.userOutput.get();
      return {
        componentType: h,
        componentSubType: this.subType,
        componentIndex: this.componentIndex,
        seriesType: c ? this.subType : null,
        seriesIndex: this.seriesIndex,
        seriesId: c ? this.id : null,
        seriesName: c ? this.name : null,
        name: o,
        dataIndex: a,
        data: s,
        dataType: r,
        value: i,
        color: l,
        borderColor: f,
        dimensionNames: v ? v.fullDimensions : null,
        encode: v ? v.encode : null,
        // Param name list for mapping `a`, `b`, `c`, `d`, `e`
        $vars: ["seriesName", "name", "value"]
      };
    }, t.prototype.getFormattedLabel = function(e, r, n, i, a, o) {
      r = r || "normal";
      var s = this.getData(n), u = this.getDataParams(e, n);
      if (o && (u.value = o.interpolatedValue), i != null && B(u.value) && (u.value = u.value[i]), !a) {
        var l = s.getItemModel(e);
        a = l.get(r === "normal" ? ["label", "formatter"] : [r, "label", "formatter"]);
      }
      if (X(a))
        return u.status = r, u.dimensionIndex = i, a(u);
      if (N(a)) {
        var f = rc(a, u);
        return f.replace(dM, function(h, c) {
          var v = c.length, d = c;
          d.charAt(0) === "[" && d.charAt(v - 1) === "]" && (d = +d.slice(1, v - 1));
          var y = _s(s, e, d);
          if (o && B(o.interpolatedValue)) {
            var p = s.getDimensionIndex(d);
            p >= 0 && (y = o.interpolatedValue[p]);
          }
          return y != null ? y + "" : "";
        });
      }
    }, t.prototype.getRawValue = function(e, r) {
      return _s(this.getData(r), e);
    }, t.prototype.formatTooltip = function(e, r, n) {
    }, t;
  })()
);
function Vd(t) {
  var e, r;
  return z(t) ? t.type && (r = t) : e = t, {
    text: e,
    // markers: markers || markersExisting,
    frag: r
  };
}
function sa(t) {
  return new gM(t);
}
var gM = (
  /** @class */
  (function() {
    function t(e) {
      e = e || {}, this._reset = e.reset, this._plan = e.plan, this._count = e.count, this._onDirty = e.onDirty, this._dirty = !0;
    }
    return t.prototype.perform = function(e) {
      var r = this._upstream, n = e && e.skip;
      if (this._dirty && r) {
        var i = this.context;
        i.data = i.outputData = r.context.outputData;
      }
      this.__pipeline && (this.__pipeline.currentTask = this);
      var a;
      this._plan && !n && (a = this._plan(this.context));
      var o = f(this._modBy), s = this._modDataCount || 0, u = f(e && e.modBy), l = e && e.modDataCount || 0;
      (o !== u || s !== l) && (a = "reset");
      function f(m) {
        return !(m >= 1) && (m = 1), m;
      }
      var h;
      (this._dirty || a === "reset") && (this._dirty = !1, h = this._doReset(n)), this._modBy = u, this._modDataCount = l;
      var c = e && e.step;
      if (r ? this._dueEnd = r._outputDueEnd : this._dueEnd = this._count ? this._count(this.context) : 1 / 0, this._progress) {
        var v = this._dueIndex, d = Math.min(c != null ? this._dueIndex + c : 1 / 0, this._dueEnd);
        if (!n && (h || v < d)) {
          var y = this._progress;
          if (B(y))
            for (var p = 0; p < y.length; p++)
              this._doProgress(y[p], v, d, u, l);
          else
            this._doProgress(y, v, d, u, l);
        }
        this._dueIndex = d;
        var g = this._settedOutputEnd != null ? this._settedOutputEnd : d;
        this._outputDueEnd = g;
      } else
        this._dueIndex = this._outputDueEnd = this._settedOutputEnd != null ? this._settedOutputEnd : this._dueEnd;
      return this.unfinished();
    }, t.prototype.dirty = function() {
      this._dirty = !0, this._onDirty && this._onDirty(this.context);
    }, t.prototype._doProgress = function(e, r, n, i, a) {
      Gd.reset(r, n, i, a), this._callingProgress = e, this._callingProgress({
        start: r,
        end: n,
        count: n - r,
        next: Gd.next
      }, this.context);
    }, t.prototype._doReset = function(e) {
      this._dueIndex = this._outputDueEnd = this._dueEnd = 0, this._settedOutputEnd = null;
      var r, n;
      !e && this._reset && (r = this._reset(this.context), r && r.progress && (n = r.forceFirstProgress, r = r.progress), B(r) && !r.length && (r = null)), this._progress = r, this._modBy = this._modDataCount = null;
      var i = this._downstream;
      return i && i.dirty(), n;
    }, t.prototype.unfinished = function() {
      return this._progress && this._dueIndex < this._dueEnd;
    }, t.prototype.pipe = function(e) {
      (this._downstream !== e || this._dirty) && (this._downstream = e, e._upstream = this, e.dirty());
    }, t.prototype.dispose = function() {
      this._disposed || (this._upstream && (this._upstream._downstream = null), this._downstream && (this._downstream._upstream = null), this._dirty = !1, this._disposed = !0);
    }, t.prototype.getUpstream = function() {
      return this._upstream;
    }, t.prototype.getDownstream = function() {
      return this._downstream;
    }, t.prototype.setOutputEnd = function(e) {
      this._outputDueEnd = this._settedOutputEnd = e;
    }, t;
  })()
), Gd = /* @__PURE__ */ (function() {
  var t, e, r, n, i, a = {
    reset: function(u, l, f, h) {
      e = u, t = l, r = f, n = h, i = Math.ceil(n / r), a.next = r > 1 && n > 0 ? s : o;
    }
  };
  return a;
  function o() {
    return e < t ? e++ : null;
  }
  function s() {
    var u = e % i * r + Math.ceil(e / i), l = e >= t ? null : u < n ? u : e;
    return e++, l;
  }
})();
function Zo(t, e) {
  var r = e && e.type;
  return r === "ordinal" ? t : (r === "time" && !ve(t) && t != null && t !== "-" && (t = +pr(t)), t == null || t === "" ? NaN : Number(t));
}
var yM = U({
  number: function(t) {
    return parseFloat(t);
  },
  time: function(t) {
    return +pr(t);
  },
  trim: function(t) {
    return N(t) ? It(t) : t;
  }
});
function BR(t) {
  return yM.get(t);
}
var i_ = {
  lt: function(t, e) {
    return t < e;
  },
  lte: function(t, e) {
    return t <= e;
  },
  gt: function(t, e) {
    return t > e;
  },
  gte: function(t, e) {
    return t >= e;
  }
}, mM = (
  /** @class */
  (function() {
    function t(e, r) {
      if (!ve(r)) {
        var n = "";
        Ye(n);
      }
      this._opFn = i_[e], this._rvalFloat = cr(r);
    }
    return t.prototype.evaluate = function(e) {
      return ve(e) ? this._opFn(e, this._rvalFloat) : this._opFn(cr(e), this._rvalFloat);
    }, t;
  })()
), _M = (
  /** @class */
  (function() {
    function t(e, r) {
      var n = e === "desc";
      this._resultLT = n ? 1 : -1, r == null && (r = n ? "min" : "max"), this._incomparable = r === "min" ? -1 / 0 : 1 / 0;
    }
    return t.prototype.evaluate = function(e, r) {
      var n = ve(e) ? e : cr(e), i = ve(r) ? r : cr(r), a = isNaN(n), o = isNaN(i);
      if (a && (n = this._incomparable), o && (i = this._incomparable), a && o) {
        var s = N(e), u = N(r);
        s && (n = u ? e : 0), u && (i = s ? r : 0);
      }
      return n < i ? this._resultLT : n > i ? -this._resultLT : 0;
    }, t;
  })()
), SM = (
  /** @class */
  (function() {
    function t(e, r) {
      this._rval = r, this._isEQ = e, this._rvalTypeof = typeof r, this._rvalFloat = cr(r);
    }
    return t.prototype.evaluate = function(e) {
      var r = e === this._rval;
      if (!r) {
        var n = typeof e;
        n !== this._rvalTypeof && (n === "number" || this._rvalTypeof === "number") && (r = cr(e) === this._rvalFloat);
      }
      return this._isEQ ? r : !r;
    }, t;
  })()
);
function NR(t, e) {
  return t === "eq" || t === "ne" ? new SM(t === "eq", e) : qe(i_, t) ? new mM(t, e) : null;
}
function bM(t) {
  var e = "", r = -1 / 0, n = -1 / 0, i = 1 / 0, a = 1 / 0;
  return t && (t.g != null && (e += "G" + t.g, r = t.g), t.ge != null && (e += "GE" + t.ge, n = t.ge), t.l != null && (e += "L" + t.l, i = t.l), t.le != null && (e += "LE" + t.le, a = t.le)), {
    key: e,
    g: r,
    ge: n,
    l: i,
    le: a
  };
}
function TM(t, e) {
  return e > t.g && e >= t.ge && e < t.l && e <= t.le;
}
var wM = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getRawData = function() {
      throw new Error("not supported");
    }, t.prototype.getRawDataItem = function(e) {
      throw new Error("not supported");
    }, t.prototype.cloneRawData = function() {
    }, t.prototype.getDimensionInfo = function(e) {
    }, t.prototype.cloneAllDimensionInfo = function() {
    }, t.prototype.count = function() {
    }, t.prototype.retrieveValue = function(e, r) {
    }, t.prototype.retrieveValueFromItem = function(e, r) {
    }, t.prototype.convertValue = function(e, r) {
      return Zo(e, r);
    }, t;
  })()
);
function CM(t, e) {
  var r = new wM(), n = t.data, i = r.sourceFormat = t.sourceFormat, a = t.startIndex, o = "";
  t.seriesLayoutBy !== Pt && Ye(o);
  var s = [], u = {}, l = t.dimensionsDefine;
  if (l)
    w(l, function(y, p) {
      var g = y.name, m = {
        index: p,
        name: g,
        displayName: y.displayName
      };
      if (s.push(m), g != null) {
        var _ = "";
        qe(u, g) && Ye(_), u[g] = m;
      }
    });
  else
    for (var f = 0; f < t.dimensionsDetectedCount; f++)
      s.push({
        index: f
      });
  var h = t_(i, Pt);
  e.__isBuiltIn && (r.getRawDataItem = function(y) {
    return h(n, a, s, y);
  }, r.getRawData = le(xM, null, t)), r.cloneRawData = le(MM, null, t);
  var c = r_(i, Pt);
  r.count = le(c, null, n, a, s);
  var v = n_(i);
  r.retrieveValue = function(y, p) {
    var g = h(n, a, s, y);
    return d(g, p);
  };
  var d = r.retrieveValueFromItem = function(y, p) {
    if (y != null) {
      var g = s[p];
      if (g)
        return v(y, p, g.name);
    }
  };
  return r.getDimensionInfo = le(DM, null, s, u), r.cloneAllDimensionInfo = le(IM, null, s), r;
}
function xM(t) {
  var e = t.sourceFormat;
  if (!lc(e)) {
    var r = "";
    Ye(r);
  }
  return t.data;
}
function MM(t) {
  var e = t.sourceFormat, r = t.data;
  if (!lc(e)) {
    var n = "";
    Ye(n);
  }
  if (e === ze) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(r[a].slice());
    return i;
  } else if (e === Rt) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(E({}, r[a]));
    return i;
  }
}
function DM(t, e, r) {
  if (r != null) {
    if (ve(r) || !isNaN(r) && !qe(e, r))
      return t[r];
    if (qe(e, r))
      return e[r];
  }
}
function IM(t) {
  return Z(t);
}
var a_ = U();
function AM(t) {
  t = Z(t);
  var e = t.type, r = "";
  e || Ye(r);
  var n = e.split(":");
  n.length !== 2 && Ye(r);
  var i = !1;
  n[0] === "echarts" && (e = n[1], i = !0), t.__isBuiltIn = i, a_.set(e, t);
}
function PM(t, e, r) {
  var n = $e(t), i = n.length, a = "";
  i || Ye(a);
  for (var o = 0, s = i; o < s; o++) {
    var u = n[o];
    e = LM(u, e), o !== s - 1 && (e.length = Math.max(e.length, 1));
  }
  return e;
}
function LM(t, e, r, n) {
  var i = "";
  e.length || Ye(i), z(t) || Ye(i);
  var a = t.type, o = a_.get(a);
  o || Ye(i);
  var s = H(e, function(l) {
    return CM(l, o);
  }), u = $e(o.transform({
    upstream: s[0],
    upstreamList: s,
    config: Z(t.config)
  }));
  return H(u, function(l, f) {
    var h = "";
    z(l) || Ye(h), l.data || Ye(h);
    var c = Qm(l.data);
    lc(c) || Ye(h);
    var v, d = e[0];
    if (d && f === 0 && !l.dimensions) {
      var y = d.startIndex;
      y && (l.data = d.data.slice(0, y).concat(l.data)), v = {
        seriesLayoutBy: Pt,
        sourceHeader: y,
        dimensions: d.metaRawOption.dimensions
      };
    } else
      v = {
        seriesLayoutBy: Pt,
        sourceHeader: 0,
        dimensions: l.dimensions
      };
    return Ef(l.data, v, null);
  });
}
function lc(t) {
  return t === ze || t === Rt;
}
var RM = typeof Uint32Array === js ? Array : Uint32Array, EM = typeof Uint16Array === js ? Array : Uint16Array, o_ = typeof Int32Array === js ? Array : Int32Array, Ud = typeof Float64Array === js ? Array : Float64Array, s_ = {
  float: Ud,
  int: o_,
  // Ordinal data type can be string or int
  ordinal: Array,
  number: Array,
  time: Ud
}, ml;
function Vn(t) {
  return t > 65535 ? RM : EM;
}
function OM(t) {
  var e = t.constructor;
  return e === Array ? t.slice() : new e(t);
}
function Yd(t, e, r, n, i) {
  var a = s_[r || "float"];
  if (i) {
    var o = t[e], s = o && o.length;
    if (s !== n) {
      for (var u = new a(n), l = 0; l < s; l++)
        u[l] = o[l];
      t[e] = u;
    }
  } else
    t[e] = new a(n);
}
var Of = (
  /** @class */
  (function() {
    function t() {
      this._chunks = [], this._rawExtent = [], this._extent = [], this._count = 0, this._rawCount = 0, this._calcDimNameToIdx = U();
    }
    return t.prototype.initData = function(e, r, n) {
      this._provider = e, this._chunks = [], this._indices = null, this.getRawIndex = this._getRawIdxIdentity;
      var i = e.getSource(), a = this.defaultDimValueGetter = ml[i.sourceFormat];
      this._dimValueGetter = n || a, this._rawExtent = [], Jm(i), this._dimensions = H(r, function(o) {
        return {
          // Only pick these two props. Not leak other properties like orderMeta.
          type: o.type,
          property: o.property
        };
      }), this._initDataFromProvider(0, e.count());
    }, t.prototype.getProvider = function() {
      return this._provider;
    }, t.prototype.getSource = function() {
      return this._provider.getSource();
    }, t.prototype.ensureCalculationDimension = function(e, r) {
      var n = this._calcDimNameToIdx, i = this._dimensions, a = n.get(e);
      if (a != null) {
        if (i[a].type === r)
          return a;
      } else
        a = i.length;
      return i[a] = {
        type: r
      }, n.set(e, a), this._chunks[a] = new s_[r || "float"](this._rawCount), this._rawExtent[a] = zt(), a;
    }, t.prototype.collectOrdinalMeta = function(e, r) {
      var n = this._chunks[e], i = this._dimensions[e], a = this._rawExtent, o = i.ordinalOffset || 0, s = n.length;
      o === 0 && (a[e] = zt());
      for (var u = a[e], l = o; l < s; l++) {
        var f = n[l] = r.parseAndCollect(n[l]);
        isNaN(f) || (u[0] = Math.min(f, u[0]), u[1] = Math.max(f, u[1]));
      }
      i.ordinalMeta = r, i.ordinalOffset = s, i.type = "ordinal";
    }, t.prototype.getOrdinalMeta = function(e) {
      var r = this._dimensions[e], n = r.ordinalMeta;
      return n;
    }, t.prototype.getDimensionProperty = function(e) {
      var r = this._dimensions[e];
      return r && r.property;
    }, t.prototype.appendData = function(e) {
      var r = this._provider, n = this.count();
      r.appendData(e);
      var i = r.count();
      return r.persistent || (i += n), n < i && this._initDataFromProvider(n, i, !0), [n, i];
    }, t.prototype.appendValues = function(e, r) {
      for (var n = this._chunks, i = this._dimensions, a = i.length, o = this._rawExtent, s = this.count(), u = s + Math.max(e.length, r || 0), l = 0; l < a; l++) {
        var f = i[l];
        Yd(n, l, f.type, u, !0);
      }
      for (var h = [], c = s; c < u; c++)
        for (var v = c - s, d = 0; d < a; d++) {
          var f = i[d], y = ml.arrayRows.call(this, e[v] || h, f.property, v, d);
          n[d][c] = y;
          var p = o[d];
          y < p[0] && (p[0] = y), y > p[1] && (p[1] = y);
        }
      return this._rawCount = this._count = u, {
        start: s,
        end: u
      };
    }, t.prototype._initDataFromProvider = function(e, r, n) {
      for (var i = this._provider, a = this._chunks, o = this._dimensions, s = o.length, u = this._rawExtent, l = H(o, function(m) {
        return m.property;
      }), f = 0; f < s; f++) {
        var h = o[f];
        u[f] || (u[f] = zt()), Yd(a, f, h.type, r, n);
      }
      if (i.fillStorage)
        i.fillStorage(e, r, a, u);
      else
        for (var c = [], v = e; v < r; v++) {
          c = i.getItem(v, c);
          for (var d = 0; d < s; d++) {
            var y = a[d], p = this._dimValueGetter(c, l[d], v, d);
            y[v] = p;
            var g = u[d];
            p < g[0] && (g[0] = p), p > g[1] && (g[1] = p);
          }
        }
      !i.persistent && i.clean && i.clean(), this._rawCount = this._count = r, this._extent = [];
    }, t.prototype.count = function() {
      return this._count;
    }, t.prototype.get = function(e, r) {
      if (!(r >= 0 && r < this._count))
        return NaN;
      var n = this._chunks[e];
      return n ? n[this.getRawIndex(r)] : NaN;
    }, t.prototype.getValues = function(e, r) {
      var n = [], i = [];
      if (r == null) {
        r = e, e = [];
        for (var a = 0; a < this._dimensions.length; a++)
          i.push(a);
      } else
        i = e;
      for (var a = 0, o = i.length; a < o; a++)
        n.push(this.get(i[a], r));
      return n;
    }, t.prototype.getByRawIndex = function(e, r) {
      if (!(r >= 0 && r < this._rawCount))
        return NaN;
      var n = this._chunks[e];
      return n ? n[r] : NaN;
    }, t.prototype.getSum = function(e) {
      var r = this._chunks[e], n = 0;
      if (r)
        for (var i = 0, a = this.count(); i < a; i++) {
          var o = this.get(e, i);
          isNaN(o) || (n += o);
        }
      return n;
    }, t.prototype.getMedian = function(e) {
      var r = [];
      this.each([e], function(i) {
        isNaN(i) || r.push(i);
      }), mh(r);
      var n = this.count();
      return n === 0 ? 0 : n % 2 === 1 ? r[(n - 1) / 2] : (r[n / 2] + r[n / 2 - 1]) / 2;
    }, t.prototype.indexOfRawIndex = function(e) {
      if (e >= this._rawCount || e < 0)
        return -1;
      if (!this._indices)
        return e;
      var r = this._indices, n = r[e];
      if (n != null && n < this._count && n === e)
        return e;
      for (var i = 0, a = this._count - 1; i <= a; ) {
        var o = (i + a) / 2 | 0;
        if (r[o] < e)
          i = o + 1;
        else if (r[o] > e)
          a = o - 1;
        else
          return o;
      }
      return -1;
    }, t.prototype.getIndices = function() {
      var e, r = this._indices;
      if (r) {
        var n = r.constructor, i = this._count;
        if (n === Array) {
          e = new n(i);
          for (var a = 0; a < i; a++)
            e[a] = r[a];
        } else
          e = new n(r.buffer, 0, i);
      } else {
        var n = Vn(this._rawCount);
        e = new n(this.count());
        for (var a = 0; a < e.length; a++)
          e[a] = a;
      }
      return e;
    }, t.prototype.filter = function(e, r) {
      if (!this._count)
        return this;
      for (var n = this.clone(), i = n.count(), a = Vn(n._rawCount), o = new a(i), s = [], u = e.length, l = 0, f = e[0], h = n._chunks, c = 0; c < i; c++) {
        var v = void 0, d = n.getRawIndex(c);
        if (u === 0)
          v = r(c);
        else if (u === 1) {
          var y = h[f][d];
          v = r(y, c);
        } else {
          for (var p = 0; p < u; p++)
            s[p] = h[e[p]][d];
          s[p] = c, v = r.apply(null, s);
        }
        v && (o[l++] = d);
      }
      return l < i && (n._indices = o), n._count = l, n._extent = [], n._updateGetRawIdx(), n;
    }, t.prototype.selectRange = function(e) {
      var r = this.clone(), n = r._count;
      if (!n)
        return this;
      var i = me(e), a = i.length;
      if (!a)
        return this;
      var o = r.count(), s = Vn(r._rawCount), u = new s(o), l = 0, f = i[0], h = e[f][0], c = e[f][1], v = r._chunks, d = !1;
      if (!r._indices) {
        var y = 0;
        if (a === 1) {
          for (var p = v[i[0]], g = 0; g < n; g++) {
            var m = p[g];
            (m >= h && m <= c || isNaN(m)) && (u[l++] = y), y++;
          }
          d = !0;
        } else if (a === 2) {
          for (var p = v[i[0]], _ = v[i[1]], S = e[i[1]][0], b = e[i[1]][1], g = 0; g < n; g++) {
            var m = p[g], T = _[g];
            (m >= h && m <= c || isNaN(m)) && (T >= S && T <= b || isNaN(T)) && (u[l++] = y), y++;
          }
          d = !0;
        }
      }
      if (!d)
        if (a === 1)
          for (var g = 0; g < o; g++) {
            var C = r.getRawIndex(g), m = v[i[0]][C];
            (m >= h && m <= c || isNaN(m)) && (u[l++] = C);
          }
        else
          for (var g = 0; g < o; g++) {
            for (var M = !0, C = r.getRawIndex(g), x = 0; x < a; x++) {
              var D = i[x], m = v[D][C];
              (m < e[D][0] || m > e[D][1]) && (M = !1);
            }
            M && (u[l++] = r.getRawIndex(g));
          }
      return l < o && (r._indices = u), r._count = l, r._extent = [], r._updateGetRawIdx(), r;
    }, t.prototype.map = function(e, r) {
      var n = this.clone(e);
      return this._updateDims(n, e, r), n;
    }, t.prototype.modify = function(e, r) {
      this._updateDims(this, e, r);
    }, t.prototype._updateDims = function(e, r, n) {
      for (var i = e._chunks, a = [], o = r.length, s = e.count(), u = [], l = e._rawExtent, f = 0; f < r.length; f++)
        l[r[f]] = zt();
      for (var h = 0; h < s; h++) {
        for (var c = e.getRawIndex(h), v = 0; v < o; v++)
          u[v] = i[r[v]][c];
        u[o] = h;
        var d = n && n.apply(null, u);
        if (d != null) {
          typeof d != "object" && (a[0] = d, d = a);
          for (var f = 0; f < d.length; f++) {
            var y = r[f], p = d[f], g = l[y], m = i[y];
            m && (m[c] = p), p < g[0] && (g[0] = p), p > g[1] && (g[1] = p);
          }
        }
      }
    }, t.prototype.lttbDownSample = function(e, r) {
      var n = this.clone([e], !0), i = n._chunks, a = i[e], o = this.count(), s = 0, u = Math.floor(1 / r), l = this.getRawIndex(0), f, h, c, v = new (Vn(this._rawCount))(Math.min((Math.ceil(o / u) + 2) * 2, o));
      v[s++] = l;
      for (var d = 1; d < o - 1; d += u) {
        for (var y = Math.min(d + u, o - 1), p = Math.min(d + u * 2, o), g = (p + y) / 2, m = 0, _ = y; _ < p; _++) {
          var S = this.getRawIndex(_), b = a[S];
          isNaN(b) || (m += b);
        }
        m /= p - y;
        var T = d, C = Math.min(d + u, o), M = d - 1, x = a[l];
        f = -1, c = T;
        for (var D = -1, I = 0, _ = T; _ < C; _++) {
          var S = this.getRawIndex(_), b = a[S];
          if (isNaN(b)) {
            I++, D < 0 && (D = S);
            continue;
          }
          h = Math.abs((M - g) * (b - x) - (M - _) * (m - x)), h > f && (f = h, c = S);
        }
        I > 0 && I < C - T && (v[s++] = Math.min(D, c), c = Math.max(D, c)), v[s++] = c, l = c;
      }
      return v[s++] = this.getRawIndex(o - 1), n._count = s, n._indices = v, n.getRawIndex = this._getRawIdx, n;
    }, t.prototype.minmaxDownSample = function(e, r) {
      for (var n = this.clone([e], !0), i = n._chunks, a = Math.floor(1 / r), o = i[e], s = this.count(), u = new (Vn(this._rawCount))(Math.ceil(s / a) * 2), l = 0, f = 0; f < s; f += a) {
        var h = f, c = o[this.getRawIndex(h)], v = f, d = o[this.getRawIndex(v)], y = a;
        f + a > s && (y = s - f);
        for (var p = 0; p < y; p++) {
          var g = this.getRawIndex(f + p), m = o[g];
          m < c && (c = m, h = f + p), m > d && (d = m, v = f + p);
        }
        var _ = this.getRawIndex(h), S = this.getRawIndex(v);
        h < v ? (u[l++] = _, u[l++] = S) : (u[l++] = S, u[l++] = _);
      }
      return n._count = l, n._indices = u, n._updateGetRawIdx(), n;
    }, t.prototype.downSample = function(e, r, n, i) {
      for (var a = this.clone([e], !0), o = a._chunks, s = [], u = Math.floor(1 / r), l = o[e], f = this.count(), h = a._rawExtent[e] = zt(), c = new (Vn(this._rawCount))(Math.ceil(f / u)), v = 0, d = 0; d < f; d += u) {
        u > f - d && (u = f - d, s.length = u);
        for (var y = 0; y < u; y++) {
          var p = this.getRawIndex(d + y);
          s[y] = l[p];
        }
        var g = n(s), m = this.getRawIndex(Math.min(d + i(s, g) || 0, f - 1));
        l[m] = g, g < h[0] && (h[0] = g), g > h[1] && (h[1] = g), c[v++] = m;
      }
      return a._count = v, a._indices = c, a._updateGetRawIdx(), a;
    }, t.prototype.each = function(e, r) {
      if (this._count)
        for (var n = e.length, i = this._chunks, a = 0, o = this.count(); a < o; a++) {
          var s = this.getRawIndex(a);
          switch (n) {
            case 0:
              r(a);
              break;
            case 1:
              r(i[e[0]][s], a);
              break;
            case 2:
              r(i[e[0]][s], i[e[1]][s], a);
              break;
            default:
              for (var u = 0, l = []; u < n; u++)
                l[u] = i[e[u]][s];
              l[u] = a, r.apply(null, l);
          }
        }
    }, t.prototype.getDataExtent = function(e, r) {
      var n = this._chunks[e], i = zt();
      if (!n)
        return i;
      var a = this.count(), o = !this._indices && !r;
      if (o)
        return this._rawExtent[e].slice();
      var s = this._extent, u = s[e] || (s[e] = {}), l = bM(r), f = l.key, h = u[f];
      if (h)
        return h.slice();
      for (var c = i[0], v = i[1], d = 0; d < a; d++) {
        var y = this.getRawIndex(d), p = n[y];
        (!r || TM(l, p)) && (p < c && (c = p), p > v && (v = p));
      }
      return u[f] = [c, v];
    }, t.prototype.getRawDataItem = function(e) {
      var r = this.getRawIndex(e);
      if (this._provider.persistent)
        return this._provider.getItem(r);
      for (var n = [], i = this._chunks, a = 0; a < i.length; a++)
        n.push(i[a][r]);
      return n;
    }, t.prototype.clone = function(e, r) {
      var n = new t(), i = this._chunks, a = e && Xt(e, function(s, u) {
        return s[u] = !0, s;
      }, {});
      if (a)
        for (var o = 0; o < i.length; o++)
          n._chunks[o] = a[o] ? OM(i[o]) : i[o];
      else
        n._chunks = i;
      return this._copyCommonProps(n), r || (n._indices = this._cloneIndices()), n._updateGetRawIdx(), n;
    }, t.prototype._copyCommonProps = function(e) {
      e._count = this._count, e._rawCount = this._rawCount, e._provider = this._provider, e._dimensions = this._dimensions, e._extent = Z(this._extent), e._rawExtent = Z(this._rawExtent);
    }, t.prototype._cloneIndices = function() {
      if (this._indices) {
        var e = this._indices.constructor, r = void 0;
        if (e === Array) {
          var n = this._indices.length;
          r = new e(n);
          for (var i = 0; i < n; i++)
            r[i] = this._indices[i];
        } else
          r = new e(this._indices);
        return r;
      }
      return null;
    }, t.prototype._getRawIdxIdentity = function(e) {
      return e;
    }, t.prototype._getRawIdx = function(e) {
      return e < this._count && e >= 0 ? this._indices[e] : -1;
    }, t.prototype._updateGetRawIdx = function() {
      this.getRawIndex = this._indices ? this._getRawIdx : this._getRawIdxIdentity;
    }, t.internalField = (function() {
      function e(r, n, i, a) {
        return Zo(r[a], this._dimensions[a]);
      }
      ml = {
        arrayRows: e,
        objectRows: function(r, n, i, a) {
          return Zo(r[n], this._dimensions[a]);
        },
        keyedColumns: e,
        original: function(r, n, i, a) {
          var o = r && (r.value == null ? r : r.value);
          return Zo(o instanceof Array ? o[a] : o, this._dimensions[a]);
        },
        typedArray: function(r, n, i, a) {
          return r[a];
        }
      };
    })(), t;
  })()
), u_ = (
  /** @class */
  (function() {
    function t(e) {
      this._sourceList = [], this._storeList = [], this._upstreamSignList = [], this._versionSignBase = 0, this._dirty = !0, this._sourceHost = e;
    }
    return t.prototype.dirty = function() {
      this._setLocalSource([], []), this._storeList = [], this._dirty = !0;
    }, t.prototype._setLocalSource = function(e, r) {
      this._sourceList = e, this._upstreamSignList = r, this._versionSignBase++, this._versionSignBase > 9e10 && (this._versionSignBase = 0);
    }, t.prototype._getVersionSign = function() {
      return this._sourceHost.uid + "_" + this._versionSignBase;
    }, t.prototype.prepareSource = function() {
      this._isDirty() && (this._createSource(), this._dirty = !1);
    }, t.prototype._createSource = function() {
      this._setLocalSource([], []);
      var e = this._sourceHost, r = this._getUpstreamSourceManagers(), n = !!r.length, i, a;
      if (vo(e)) {
        var o = e, s = void 0, u = void 0, l = void 0;
        if (n) {
          var f = r[0];
          f.prepareSource(), l = f.getSource(), s = l.data, u = l.sourceFormat, a = [f._getVersionSign()];
        } else
          s = o.get("data", !0), u = Ke(s) ? Lr : ft, a = [];
        var h = this._getSourceMetaRawOption() || {}, c = l && l.metaRawOption || {}, v = G(h.seriesLayoutBy, c.seriesLayoutBy) || null, d = G(h.sourceHeader, c.sourceHeader), y = G(h.dimensions, c.dimensions), p = v !== c.seriesLayoutBy || !!d != !!c.sourceHeader || y;
        i = p ? [Ef(s, {
          seriesLayoutBy: v,
          sourceHeader: d,
          dimensions: y
        }, u)] : [];
      } else {
        var g = e;
        if (n) {
          var m = this._applyTransform(r);
          i = m.sourceList, a = m.upstreamSignList;
        } else {
          var _ = g.get("source", !0);
          i = [Ef(_, this._getSourceMetaRawOption(), null)], a = [];
        }
      }
      this._setLocalSource(i, a);
    }, t.prototype._applyTransform = function(e) {
      var r = this._sourceHost, n = r.get("transform", !0), i = r.get("fromTransformResult", !0);
      if (i != null) {
        var a = "";
        e.length !== 1 && Xd(a);
      }
      var o, s = [], u = [];
      return w(e, function(l) {
        l.prepareSource();
        var f = l.getSource(i || 0), h = "";
        i != null && !f && Xd(h), s.push(f), u.push(l._getVersionSign());
      }), n ? o = PM(n, s, {
        datasetIndex: r.componentIndex
      }) : i != null && (o = [uM(s[0])]), {
        sourceList: o,
        upstreamSignList: u
      };
    }, t.prototype._isDirty = function() {
      if (this._dirty)
        return !0;
      for (var e = this._getUpstreamSourceManagers(), r = 0; r < e.length; r++) {
        var n = e[r];
        if (
          // Consider the case that there is ancestor diry, call it recursively.
          // The performance is probably not an issue because usually the chain is not long.
          n._isDirty() || this._upstreamSignList[r] !== n._getVersionSign()
        )
          return !0;
      }
    }, t.prototype.getSource = function(e) {
      e = e || 0;
      var r = this._sourceList[e];
      if (!r) {
        var n = this._getUpstreamSourceManagers();
        return n[0] && n[0].getSource(e);
      }
      return r;
    }, t.prototype.getSharedDataStore = function(e) {
      var r = e.makeStoreSchema();
      return this._innerGetDataStore(r.dimensions, e.source, r.hash);
    }, t.prototype._innerGetDataStore = function(e, r, n) {
      var i = 0, a = this._storeList, o = a[i];
      o || (o = a[i] = {});
      var s = o[n];
      if (!s) {
        var u = this._getUpstreamSourceManagers()[0];
        vo(this._sourceHost) && u ? s = u._innerGetDataStore(e, r, n) : (s = new Of(), s.initData(new e_(r, e.length), e)), o[n] = s;
      }
      return s;
    }, t.prototype._getUpstreamSourceManagers = function() {
      var e = this._sourceHost;
      if (vo(e)) {
        var r = qm(e);
        return r ? [r.getSourceManager()] : [];
      } else
        return H(Bx(e), function(n) {
          return n.getSourceManager();
        });
    }, t.prototype._getSourceMetaRawOption = function() {
      var e = this._sourceHost, r, n, i;
      if (vo(e))
        r = e.get("seriesLayoutBy", !0), n = e.get("sourceHeader", !0), i = e.get("dimensions", !0);
      else if (!this._getUpstreamSourceManagers().length) {
        var a = e;
        r = a.get("seriesLayoutBy", !0), n = a.get("sourceHeader", !0), i = a.get("dimensions", !0);
      }
      return {
        seriesLayoutBy: r,
        sourceHeader: n,
        dimensions: i
      };
    }, t;
  })()
);
function Wd(t) {
  var e = t.option.transform;
  e && va(t.option.transform);
}
function vo(t) {
  return t.mainType === "series";
}
function Xd(t) {
  throw new Error(t);
}
var kM = "line-height:1";
function l_(t) {
  var e = t.lineHeight;
  return e == null ? kM : "line-height:" + Ue(e + "") + "px";
}
function f_(t, e) {
  var r = t.color || j.color.tertiary, n = t.fontSize || 12, i = t.fontWeight || "400", a = t.color || j.color.secondary, o = t.fontSize || 14, s = t.fontWeight || "900";
  return e === "html" ? {
    // eslint-disable-next-line max-len
    nameStyle: "font-size:" + Ue(n + "") + "px;color:" + Ue(r) + ";font-weight:" + Ue(i + ""),
    // eslint-disable-next-line max-len
    valueStyle: "font-size:" + Ue(o + "") + "px;color:" + Ue(a) + ";font-weight:" + Ue(s + "")
  } : {
    nameStyle: {
      fontSize: n,
      fill: r,
      fontWeight: i
    },
    valueStyle: {
      fontSize: o,
      fill: a,
      fontWeight: s
    }
  };
}
var BM = [0, 10, 20, 30], NM = ["", `
`, `

`, `


`];
function Ca(t, e) {
  return e.type = t, e;
}
function kf(t) {
  return t.type === "section";
}
function h_(t) {
  return kf(t) ? FM : zM;
}
function c_(t) {
  if (kf(t)) {
    var e = 0, r = t.blocks.length, n = r > 1 || r > 0 && !t.noHeader;
    return w(t.blocks, function(i) {
      var a = c_(i);
      a >= e && (e = a + +(n && // 0 always can not be readable gap level.
      (!a || kf(i) && !i.noHeader)));
    }), e;
  }
  return 0;
}
function FM(t, e, r, n) {
  var i = e.noHeader, a = HM(c_(e)), o = [], s = e.blocks || [];
  qt(!s || B(s)), s = s || [];
  var u = t.orderMode;
  if (e.sortBlocks && u) {
    s = s.slice();
    var l = {
      valueAsc: "asc",
      valueDesc: "desc"
    };
    if (qe(l, u)) {
      var f = new _M(l[u], null);
      s.sort(function(y, p) {
        return f.evaluate(y.sortParam, p.sortParam);
      });
    } else u === "seriesDesc" && s.reverse();
  }
  w(s, function(y, p) {
    var g = e.valueFormatter, m = h_(y)(
      // Inherit valueFormatter
      g ? E(E({}, t), {
        valueFormatter: g
      }) : t,
      y,
      p > 0 ? a.html : 0,
      n
    );
    m != null && o.push(m);
  });
  var h = t.renderMode === "richText" ? o.join(a.richText) : Bf(n, o.join(""), i ? r : a.html);
  if (i)
    return h;
  var c = Rf(e.header, "ordinal", t.useUTC), v = f_(n, t.renderMode).nameStyle, d = l_(n);
  return t.renderMode === "richText" ? v_(t, c, v) + a.richText + h : Bf(n, '<div style="' + v + ";" + d + ';">' + Ue(c) + "</div>" + h, r);
}
function zM(t, e, r, n) {
  var i = t.renderMode, a = e.noName, o = e.noValue, s = !e.markerType, u = e.name, l = t.useUTC, f = e.valueFormatter || t.valueFormatter || function(S) {
    return S = B(S) ? S : [S], H(S, function(b, T) {
      return Rf(b, B(v) ? v[T] : v, l);
    });
  };
  if (!(a && o)) {
    var h = s ? "" : t.markupStyleCreator.makeTooltipMarker(e.markerType, e.markerColor || j.color.secondary, i), c = a ? "" : Rf(u, "ordinal", l), v = e.valueType, d = o ? [] : f(e.value, e.rawDataIndex), y = !s || !a, p = !s && a, g = f_(n, i), m = g.nameStyle, _ = g.valueStyle;
    return i === "richText" ? (s ? "" : h) + (a ? "" : v_(t, c, m)) + (o ? "" : UM(t, d, y, p, _)) : Bf(n, (s ? "" : h) + (a ? "" : VM(c, !s, m)) + (o ? "" : GM(d, y, p, _)), r);
  }
}
function qd(t, e, r, n, i, a) {
  if (t) {
    var o = h_(t), s = {
      useUTC: i,
      renderMode: r,
      orderMode: n,
      markupStyleCreator: e,
      valueFormatter: t.valueFormatter
    };
    return o(s, t, 0, a);
  }
}
function HM(t) {
  return {
    html: BM[t],
    richText: NM[t]
  };
}
function Bf(t, e, r) {
  var n = '<div style="clear:both"></div>', i = "margin: " + r + "px 0 0", a = l_(t);
  return '<div style="' + i + ";" + a + ';">' + e + n + "</div>";
}
function VM(t, e, r) {
  var n = e ? "margin-left:2px" : "";
  return '<span style="' + r + ";" + n + '">' + Ue(t) + "</span>";
}
function GM(t, e, r, n) {
  var i = r ? "10px" : "20px", a = e ? "float:right;margin-left:" + i : "";
  return t = B(t) ? t : [t], '<span style="' + a + ";" + n + '">' + H(t, function(o) {
    return Ue(o);
  }).join("&nbsp;&nbsp;") + "</span>";
}
function v_(t, e, r) {
  return t.markupStyleCreator.wrapRichTextStyle(e, r);
}
function UM(t, e, r, n, i) {
  var a = [i], o = n ? 10 : 20;
  return r && a.push({
    padding: [0, 0, 0, o],
    align: "right"
  }), t.markupStyleCreator.wrapRichTextStyle(B(e) ? e.join("  ") : e, a);
}
function YM(t, e) {
  var r = t.getData().getItemVisual(e, "style"), n = r[t.visualDrawType];
  return ba(n);
}
function d_(t, e) {
  var r = t.get("padding");
  return r ?? (e === "richText" ? [8, 10] : 10);
}
var _l = (
  /** @class */
  (function() {
    function t() {
      this.richTextStyles = {}, this._nextStyleNameId = bh();
    }
    return t.prototype._generateStyleName = function() {
      return "__EC_aUTo_" + this._nextStyleNameId++;
    }, t.prototype.makeTooltipMarker = function(e, r, n) {
      var i = n === "richText" ? this._generateStyleName() : null, a = Vm({
        color: r,
        type: e,
        renderMode: n,
        markerId: i
      });
      return N(a) ? a : (this.richTextStyles[i] = a.style, a.content);
    }, t.prototype.wrapRichTextStyle = function(e, r) {
      var n = {};
      B(r) ? w(r, function(a) {
        return E(n, a);
      }) : E(n, r);
      var i = this._generateStyleName();
      return this.richTextStyles[i] = n, "{" + i + "|" + e + "}";
    }, t;
  })()
);
function WM(t) {
  var e = t.series, r = t.dataIndex, n = t.multipleSeries, i = e.getData(), a = i.mapDimensionsAll("defaultedTooltip"), o = a.length, s = e.getRawValue(r), u = B(s), l = YM(e, r), f, h, c, v;
  if (o > 1 || u && !o) {
    var d = XM(s, e, r, a, l);
    f = d.inlineValues, h = d.inlineValueTypes, c = d.blocks, v = d.inlineValues[0];
  } else if (o) {
    var y = i.getDimensionInfo(a[0]);
    v = f = _s(i, r, a[0]), h = y.type;
  } else
    v = f = u ? s[0] : s;
  var p = Th(e), g = p && e.name || "", m = i.getName(r), _ = n ? g : m;
  return Ca("section", {
    header: g,
    // When series name is not specified, do not show a header line with only '-'.
    // This case always happens in tooltip.trigger: 'item'.
    noHeader: n || !p,
    sortParam: v,
    blocks: [Ca("nameValue", {
      markerType: "item",
      markerColor: l,
      // Do not mix display seriesName and itemName in one tooltip,
      // which might confuses users.
      name: _,
      // name dimension might be auto assigned, where the name might
      // be not readable. So we check trim here.
      noName: !It(_),
      value: f,
      valueType: h,
      rawDataIndex: i.getRawIndex(r)
    })].concat(c || [])
  });
}
function XM(t, e, r, n, i) {
  var a = e.getData(), o = Xt(t, function(h, c, v) {
    var d = a.getDimensionInfo(v);
    return h = h || d && d.tooltip !== !1 && d.displayName != null;
  }, !1), s = [], u = [], l = [];
  n.length ? w(n, function(h) {
    f(_s(a, r, h), h);
  }) : w(t, f);
  function f(h, c) {
    var v = a.getDimensionInfo(c);
    !v || v.otherDims.tooltip === !1 || (o ? l.push(Ca("nameValue", {
      markerType: "subItem",
      markerColor: i,
      name: v.displayName,
      value: h,
      valueType: v.type
    })) : (s.push(h), u.push(v.type)));
  }
  return {
    inlineValues: s,
    inlineValueTypes: u,
    blocks: l
  };
}
var Sr = de();
function po(t, e) {
  return t.getName(e) || t.getId(e);
}
var qM = "__universalTransitionEnabled", $t = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r._selectedDataIndicesMap = {}, r;
    }
    return e.prototype.init = function(r, n, i) {
      this.seriesIndex = this.componentIndex, this.dataTask = sa({
        count: ZM,
        reset: KM
      }), this.dataTask.context = {
        model: this
      }, this.mergeDefaultAndTheme(r, i);
      var a = Sr(this).sourceManager = new u_(this);
      a.prepareSource();
      var o = this.getInitialData(r, i);
      Zd(o, this), this.dataTask.context.data = o, Sr(this).dataBeforeProcessed = o, $d(this), this._initSelectedMapFromData(o);
    }, e.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = ms(this), a = i ? ic(r) : {}, o = this.subType;
      ae.hasClass(o) && (o += "Series"), pe(r, n.getTheme().get(this.subType)), pe(r, this.getDefaultOption()), Pv(r, "label", ["show"]), this.fillDataTextStyle(r.data), i && Ta(r, a, i);
    }, e.prototype.mergeOption = function(r, n) {
      r = pe(this.option, r, !0), this.fillDataTextStyle(r.data);
      var i = ms(this);
      i && Ta(this.option, r, i);
      var a = Sr(this).sourceManager;
      a.dirty(), a.prepareSource();
      var o = this.getInitialData(r, n);
      Zd(o, this), this.dataTask.dirty(), this.dataTask.context.data = o, Sr(this).dataBeforeProcessed = o, $d(this), this._initSelectedMapFromData(o);
    }, e.prototype.fillDataTextStyle = function(r) {
      if (r && !Ke(r))
        for (var n = ["show"], i = 0; i < r.length; i++)
          r[i] && r[i].label && Pv(r[i], "label", n);
    }, e.prototype.getInitialData = function(r, n) {
    }, e.prototype.appendData = function(r) {
      var n = this.getRawData();
      n.appendData(r.data);
    }, e.prototype.getData = function(r) {
      var n = Nf(this);
      if (n) {
        var i = n.context.data;
        return r == null || !i.getLinkedData ? i : i.getLinkedData(r);
      } else
        return Sr(this).data;
    }, e.prototype.getAllData = function() {
      var r = this.getData();
      return r && r.getLinkedDataAll ? r.getLinkedDataAll() : [{
        data: r
      }];
    }, e.prototype.setData = function(r) {
      var n = Nf(this);
      if (n) {
        var i = n.context;
        i.outputData = r, n !== this.dataTask && (i.data = r);
      }
      Sr(this).data = r;
    }, e.prototype.getEncode = function() {
      var r = this.get("encode", !0);
      if (r)
        return U(r);
    }, e.prototype.getSourceManager = function() {
      return Sr(this).sourceManager;
    }, e.prototype.getSource = function() {
      return this.getSourceManager().getSource();
    }, e.prototype.getRawData = function() {
      return Sr(this).dataBeforeProcessed;
    }, e.prototype.getColorBy = function() {
      var r = this.get("colorBy");
      return r || "series";
    }, e.prototype.isColorBySeries = function() {
      return this.getColorBy() === "series";
    }, e.prototype.getBaseAxis = function() {
      var r = this.coordinateSystem;
      return r && r.getBaseAxis && r.getBaseAxis();
    }, e.prototype.indicesOfNearest = function(r, n, i, a) {
      var o = this.getData(), s = this.coordinateSystem, u = s && s.getAxis(r);
      if (!s || !u)
        return [];
      var l = u.dataToCoord(i);
      a == null && (a = 1 / 0);
      for (var f = [], h = 1 / 0, c = -1, v = 0, d = o.getDimensionIndex(n), y = o.getStore(), p = 0, g = y.count(); p < g; p++) {
        var m = y.get(d, p), _ = u.dataToCoord(m), S = l - _, b = Math.abs(S);
        b <= a && ((b < h || b === h && S >= 0 && c < 0) && (h = b, c = S, v = 0), S === c && (f[v++] = p));
      }
      return f.length = v, f;
    }, e.prototype.formatTooltip = function(r, n, i) {
      return WM({
        series: this,
        dataIndex: r,
        multipleSeries: n
      });
    }, e.prototype.isAnimationEnabled = function() {
      var r = this.ecModel;
      if (W.node && !(r && r.ssr))
        return !1;
      var n = this.getShallow("animation");
      return n && this.getData().count() > this.getShallow("animationThreshold") && (n = !1), !!n;
    }, e.prototype.restoreData = function() {
      this.dataTask.dirty();
    }, e.prototype.getColorFromPalette = function(r, n, i) {
      var a = this.ecModel, o = ac.prototype.getColorFromPalette.call(this, r, n, i);
      return o || (o = a.getColorFromPalette(r, n, i)), o;
    }, e.prototype.coordDimToDataDim = function(r) {
      return this.getRawData().mapDimensionsAll(r);
    }, e.prototype.getProgressive = function() {
      return this.get("progressive");
    }, e.prototype.getProgressiveThreshold = function() {
      return this.get("progressiveThreshold");
    }, e.prototype.select = function(r, n) {
      this._innerSelect(this.getData(n), r);
    }, e.prototype.unselect = function(r, n) {
      var i = this.option.selectedMap;
      if (i) {
        var a = this.option.selectedMode, o = this.getData(n);
        if (a === "series" || i === "all") {
          this.option.selectedMap = {}, this._selectedDataIndicesMap = {};
          return;
        }
        for (var s = 0; s < r.length; s++) {
          var u = r[s], l = po(o, u);
          i[l] = !1, this._selectedDataIndicesMap[l] = -1;
        }
      }
    }, e.prototype.toggleSelect = function(r, n) {
      for (var i = [], a = 0; a < r.length; a++)
        i[0] = r[a], this.isSelected(r[a], n) ? this.unselect(i, n) : this.select(i, n);
    }, e.prototype.getSelectedDataIndices = function() {
      if (this.option.selectedMap === "all")
        return [].slice.call(this.getData().getIndices());
      for (var r = this._selectedDataIndicesMap, n = me(r), i = [], a = 0; a < n.length; a++) {
        var o = r[n[a]];
        o >= 0 && i.push(o);
      }
      return i;
    }, e.prototype.isSelected = function(r, n) {
      var i = this.option.selectedMap;
      if (!i)
        return !1;
      var a = this.getData(n);
      return (i === "all" || i[po(a, r)]) && !a.getItemModel(r).get(["select", "disabled"]);
    }, e.prototype.isUniversalTransitionEnabled = function() {
      if (this[qM])
        return !0;
      var r = this.option.universalTransition;
      return r ? r === !0 ? !0 : r && r.enabled : !1;
    }, e.prototype._innerSelect = function(r, n) {
      var i, a, o = this.option, s = o.selectedMode, u = n.length;
      if (!(!s || !u)) {
        if (s === "series")
          o.selectedMap = "all";
        else if (s === "multiple") {
          z(o.selectedMap) || (o.selectedMap = {});
          for (var l = o.selectedMap, f = 0; f < u; f++) {
            var h = n[f], c = po(r, h);
            l[c] = !0, this._selectedDataIndicesMap[c] = r.getRawIndex(h);
          }
        } else if (s === "single" || s === !0) {
          var v = n[u - 1], c = po(r, v);
          o.selectedMap = (i = {}, i[c] = !0, i), this._selectedDataIndicesMap = (a = {}, a[c] = r.getRawIndex(v), a);
        }
      }
    }, e.prototype._initSelectedMapFromData = function(r) {
      if (!this.option.selectedMap) {
        var n = [];
        r.hasItemOption && r.each(function(i) {
          var a = r.getRawDataItem(i);
          a && a.selected && n.push(i);
        }), n.length > 0 && this._innerSelect(r, n);
      }
    }, e.registerClass = function(r) {
      return ae.registerClass(r);
    }, e.protoInitialize = (function() {
      var r = e.prototype;
      r.type = "series.__base__", r.seriesIndex = 0, r.ignoreStyleOnData = !1, r.hasSymbolVisual = !1, r.defaultSymbol = "circle", r.visualStyleAccessPath = "itemStyle", r.visualDrawType = "fill";
    })(), e;
  })(ae)
);
Lt($t, pM);
Lt($t, ac);
My($t, ae);
function $d(t) {
  var e = t.name;
  Th(t) || (t.name = $M(t) || e);
}
function $M(t) {
  var e = t.getRawData(), r = e.mapDimensionsAll("seriesName"), n = [];
  return w(r, function(i) {
    var a = e.getDimensionInfo(i);
    a.displayName && n.push(a.displayName);
  }), n.join(" ");
}
function ZM(t) {
  return t.model.getRawData().count();
}
function KM(t) {
  var e = t.model;
  return e.setData(e.getRawData().cloneShallow()), jM;
}
function jM(t, e) {
  e.outputData && t.end > e.outputData.count() && e.model.getRawData().cloneShallow(e.outputData);
}
function Zd(t, e) {
  w(kg(t.CHANGABLE_METHODS, t.DOWNSAMPLE_METHODS), function(r) {
    t.wrapMethod(r, we(QM, e));
  });
}
function QM(t, e) {
  var r = Nf(t);
  return r && r.setOutputEnd((e || this).count()), e;
}
function Nf(t) {
  var e = (t.ecModel || {}).scheduler, r = e && e.getPipeline(t.uid);
  if (r) {
    var n = r.currentTask;
    if (n) {
      var i = n.agentStubMap;
      i && (n = i.get(t.uid));
    }
    return n;
  }
}
var lt = (
  /** @class */
  (function() {
    function t() {
      this.group = new ut(), this.uid = lu("viewComponent");
    }
    return t.prototype.init = function(e, r) {
    }, t.prototype.render = function(e, r, n, i) {
    }, t.prototype.dispose = function(e, r) {
    }, t.prototype.updateView = function(e, r, n, i) {
    }, t.prototype.updateLayout = function(e, r, n, i) {
    }, t.prototype.updateVisual = function(e, r, n, i) {
    }, t.prototype.toggleBlurSeries = function(e, r, n) {
    }, t.prototype.eachRendered = function(e) {
      var r = this.group;
      r && r.traverse(e);
    }, t;
  })()
);
Mh(lt);
$s(lt);
function JM() {
  var t = de();
  return function(e) {
    var r = t(e), n = e.pipelineContext, i = !!r.large, a = !!r.progressiveRender, o = r.large = !!(n && n.large), s = r.progressiveRender = !!(n && n.progressiveRender);
    return (i !== o || a !== s) && "reset";
  };
}
var p_ = de(), eD = JM(), Wt = (
  /** @class */
  (function() {
    function t() {
      this.group = new ut(), this.uid = lu("viewChart"), this.renderTask = sa({
        plan: tD,
        reset: rD
      }), this.renderTask.context = {
        view: this
      };
    }
    return t.prototype.init = function(e, r) {
    }, t.prototype.render = function(e, r, n, i) {
    }, t.prototype.highlight = function(e, r, n, i) {
      var a = e.getData(i && i.dataType);
      a && jd(a, i, "emphasis");
    }, t.prototype.downplay = function(e, r, n, i) {
      var a = e.getData(i && i.dataType);
      a && jd(a, i, "normal");
    }, t.prototype.remove = function(e, r) {
      this.group.removeAll();
    }, t.prototype.dispose = function(e, r) {
    }, t.prototype.updateView = function(e, r, n, i) {
      this.render(e, r, n, i);
    }, t.prototype.updateVisual = function(e, r, n, i) {
      this.render(e, r, n, i);
    }, t.prototype.eachRendered = function(e) {
      xm(this.group, e);
    }, t.markUpdateMethod = function(e, r) {
      p_(e).updateMethod = r;
    }, t.protoInitialize = (function() {
      var e = t.prototype;
      e.type = "chart";
    })(), t;
  })()
);
function Kd(t, e, r) {
  t && xf(t) && (e === "emphasis" ? bf : Tf)(t, r);
}
function jd(t, e, r) {
  var n = Oa(t, e), i = e && e.highlightKey != null ? $w(e.highlightKey) : null;
  n != null ? w($e(n), function(a) {
    Kd(t.getItemGraphicEl(a), r, i);
  }) : t.eachItemGraphicEl(function(a) {
    Kd(a, r, i);
  });
}
Mh(Wt);
$s(Wt);
function tD(t) {
  return eD(t.model);
}
function rD(t) {
  var e = t.model, r = t.ecModel, n = t.api, i = t.payload, a = e.pipelineContext.progressiveRender, o = t.view, s = i && p_(i).updateMethod, u = a ? "incrementalPrepareRender" : s && o[s] ? s : "render";
  return u !== "render" && o[u](e, r, n, i), nD[u];
}
var nD = {
  incrementalPrepareRender: {
    progress: function(t, e) {
      e.view.incrementalRender(t, e.model, e.ecModel, e.api, e.payload);
    }
  },
  render: {
    // Put view.render in `progress` to support appendData. But in this case
    // view.render should not be called in reset, otherwise it will be called
    // twise. Use `forceFirstProgress` to make sure that view.render is called
    // in any cases.
    forceFirstProgress: !0,
    progress: function(t, e) {
      e.view.render(e.model, e.ecModel, e.api, e.payload);
    }
  }
}, Ss = "\0__throttleOriginMethod", Qd = "\0__throttleRate", Jd = "\0__throttleType";
function fc(t, e, r) {
  var n, i = 0, a = 0, o = null, s, u, l, f;
  e = e || 0;
  function h() {
    a = (/* @__PURE__ */ new Date()).getTime(), o = null, t.apply(u, l || []);
  }
  var c = function() {
    for (var v = [], d = 0; d < arguments.length; d++)
      v[d] = arguments[d];
    n = (/* @__PURE__ */ new Date()).getTime(), u = this, l = v;
    var y = f || e, p = f || r;
    f = null, s = n - (p ? i : a) - y, clearTimeout(o), p ? o = setTimeout(h, y) : s >= 0 ? h() : o = setTimeout(h, -s), i = n;
  };
  return c.clear = function() {
    o && (clearTimeout(o), o = null);
  }, c.debounceNextCall = function(v) {
    f = v;
  }, c;
}
function g_(t, e, r, n) {
  var i = t[e];
  if (i) {
    var a = i[Ss] || i, o = i[Jd], s = i[Qd];
    if (s !== r || o !== n) {
      if (r == null || !n)
        return t[e] = a;
      i = t[e] = fc(a, r, n === "debounce"), i[Ss] = a, i[Jd] = n, i[Qd] = r;
    }
    return i;
  }
}
function Ff(t, e) {
  var r = t[e];
  r && r[Ss] && (r.clear && r.clear(), t[e] = r[Ss]);
}
var ep = de(), tp = {
  itemStyle: _a(Lm, !0),
  lineStyle: _a(Pm, !0)
}, iD = {
  lineStyle: "stroke",
  itemStyle: "fill"
};
function y_(t, e) {
  var r = t.visualStyleMapper || tp[e];
  return r || (console.warn("Unknown style type '" + e + "'."), tp.itemStyle);
}
function m_(t, e) {
  var r = t.visualDrawType || iD[e];
  return r || (console.warn("Unknown style type '" + e + "'."), "fill");
}
var aD = {
  createOnAllSeries: !0,
  performRawSeries: !0,
  reset: function(t, e) {
    var r = t.getData(), n = t.visualStyleAccessPath || "itemStyle", i = t.getModel(n), a = y_(t, n), o = a(i), s = i.getShallow("decal");
    s && (r.setVisual("decal", s), s.dirty = !0);
    var u = m_(t, n), l = o[u], f = X(l) ? l : null, h = o.fill === "auto" || o.stroke === "auto";
    if (!o[u] || f || h) {
      var c = t.getColorFromPalette(
        // TODO series count changed.
        t.name,
        null,
        e.getSeriesCount()
      );
      o[u] || (o[u] = c, r.setVisual("colorFromPalette", !0)), o.fill = o.fill === "auto" || X(o.fill) ? c : o.fill, o.stroke = o.stroke === "auto" || X(o.stroke) ? c : o.stroke;
    }
    if (r.setVisual("style", o), r.setVisual("drawType", u), !e.isSeriesFiltered(t) && f)
      return r.setVisual("colorFromPalette", !1), {
        dataEach: function(v, d) {
          var y = t.getDataParams(d), p = E({}, o);
          p[u] = f(y), v.setItemVisual(d, "style", p);
        }
      };
  }
}, ki = new ce(), oD = {
  createOnAllSeries: !0,
  reset: function(t, e) {
    if (!t.ignoreStyleOnData) {
      var r = t.getData(), n = t.visualStyleAccessPath || "itemStyle", i = y_(t, n), a = r.getVisual("drawType");
      return {
        dataEach: r.hasItemOption ? function(o, s) {
          var u = o.getRawDataItem(s);
          if (u && u[n]) {
            ki.option = u[n];
            var l = i(ki), f = o.ensureUniqueItemVisual(s, "style");
            E(f, l), ki.option.decal && (o.setItemVisual(s, "decal", ki.option.decal), ki.option.decal.dirty = !0), a in l && o.setItemVisual(s, "colorFromPalette", !1);
          }
        } : null
      };
    }
  }
}, sD = {
  performRawSeries: !0,
  overallReset: function(t) {
    var e = U();
    t.eachSeries(function(r) {
      if (!r.isColorBySeries()) {
        var n = r.type + "-" + r.getColorBy();
        ep(r).scope = e.get(n) || e.set(n, {});
      }
    }), t.eachSeries(function(r) {
      if (!r.isColorBySeries()) {
        var n = r.getRawData(), i = {}, a = r.getData(), o = ep(r).scope, s = r.visualStyleAccessPath || "itemStyle", u = m_(r, s);
        a.each(function(l) {
          var f = a.getRawIndex(l);
          i[f] = l;
        }), n.each(function(l) {
          var f = i[l], h = a.getItemVisual(f, "colorFromPalette");
          if (h) {
            var c = a.ensureUniqueItemVisual(f, "style"), v = n.getName(l) || l + "", d = n.count();
            c[u] = r.getColorFromPalette(v, o, d);
          }
        });
      }
    });
  }
}, go = Math.PI;
function uD(t, e) {
  e = e || {}, _e(e, {
    text: "loading",
    textColor: j.color.primary,
    fontSize: 12,
    fontWeight: "normal",
    fontStyle: "normal",
    fontFamily: "sans-serif",
    maskColor: "rgba(255,255,255,0.8)",
    showSpinner: !0,
    color: j.color.theme[0],
    spinnerRadius: 10,
    lineWidth: 5,
    zlevel: 0
  });
  var r = new ut(), n = new Ee({
    style: {
      fill: e.maskColor
    },
    zlevel: e.zlevel,
    z: 1e4
  });
  r.add(n);
  var i = new je({
    style: {
      text: e.text,
      fill: e.textColor,
      fontSize: e.fontSize,
      fontWeight: e.fontWeight,
      fontStyle: e.fontStyle,
      fontFamily: e.fontFamily
    },
    zlevel: e.zlevel,
    z: 10001
  }), a = new Ee({
    style: {
      fill: "none"
    },
    textContent: i,
    textConfig: {
      position: "right",
      distance: 10
    },
    zlevel: e.zlevel,
    z: 10001
  });
  r.add(a);
  var o;
  return e.showSpinner && (o = new Fa({
    shape: {
      startAngle: -go / 2,
      endAngle: -go / 2 + 0.1,
      r: e.spinnerRadius
    },
    style: {
      stroke: e.color,
      lineCap: "round",
      lineWidth: e.lineWidth
    },
    zlevel: e.zlevel,
    z: 10001
  }), o.animateShape(!0).when(1e3, {
    endAngle: go * 3 / 2
  }).start("circularInOut"), o.animateShape(!0).when(1e3, {
    startAngle: go * 3 / 2
  }).delay(300).start("circularInOut"), r.add(o)), r.resize = function() {
    var s = i.getBoundingRect().width, u = e.showSpinner ? e.spinnerRadius : 0, l = (t.getWidth() - u * 2 - (e.showSpinner && s ? 10 : 0) - s) / 2 - (e.showSpinner && s ? 0 : 5 + s / 2) + (e.showSpinner ? 0 : s / 2) + (s ? 0 : u), f = t.getHeight() / 2;
    e.showSpinner && o.setShape({
      cx: l,
      cy: f
    }), a.setShape({
      x: l - u,
      y: f - u,
      width: u * 2,
      height: u * 2
    }), n.setShape({
      x: 0,
      y: 0,
      width: t.getWidth(),
      height: t.getHeight()
    });
  }, r.resize(), r;
}
var __ = (
  /** @class */
  (function() {
    function t(e, r, n, i) {
      this._stageTaskMap = U(), this.ecInstance = e, this.api = r, n = this._dataProcessorHandlers = n.slice(), i = this._visualHandlers = i.slice(), this._allHandlers = n.concat(i);
    }
    return t.prototype.restoreData = function(e, r) {
      e.restoreData(r), this._stageTaskMap.each(function(n) {
        var i = n.overallTask;
        i && i.dirty();
      });
    }, t.prototype.getPerformArgs = function(e, r) {
      if (e.__pipeline) {
        var n = this._pipelineMap.get(e.__pipeline.id), i = n.context, a = !r && n.progressiveEnabled && (!i || i.progressiveRender) && e.__idxInPipeline > n.blockIndex, o = a ? n.step : null, s = i && i.modDataCount, u = s != null ? Math.ceil(s / o) : null;
        return {
          step: o,
          modBy: u,
          modDataCount: s
        };
      }
    }, t.prototype.getPipeline = function(e) {
      return this._pipelineMap.get(e);
    }, t.prototype.updateStreamModes = function(e, r) {
      var n = this._pipelineMap.get(e.uid), i = e.__preparePipelineContext ? e.__preparePipelineContext(r, n) : DT(e, r, n);
      e.pipelineContext = n.context = i;
    }, t.prototype.restorePipelines = function(e, r) {
      var n = this, i = n._pipelineMap = U();
      r.eachSeries(function(a) {
        var o = e.painter.type === "canvas" && a.getProgressive(), s = a.uid;
        i.set(s, {
          id: s,
          head: null,
          tail: null,
          threshold: a.getProgressiveThreshold(),
          progressiveEnabled: o && !(a.preventIncremental && a.preventIncremental()),
          blockIndex: -1,
          step: Math.round(o || 700),
          count: 0
        }), n._pipe(a, a.dataTask);
      });
    }, t.prototype.prepareStageTasks = function() {
      var e = this._stageTaskMap, r = this.api.getModel(), n = this.api;
      w(this._allHandlers, function(i) {
        var a = e.get(i.uid) || e.set(i.uid, {}), o = "";
        qt(!(i.reset && i.overallReset), o), i.reset && this._createSeriesStageTask(i, a, r, n), i.overallReset && this._createOverallStageTask(i, a, r, n);
      }, this);
    }, t.prototype.prepareView = function(e, r, n, i) {
      var a = e.renderTask, o = a.context;
      o.model = r, o.ecModel = n, o.api = i, a.__block = !e.incrementalPrepareRender, this._pipe(r, a);
    }, t.prototype.performDataProcessorTasks = function(e, r) {
      this._performStageTasks(this._dataProcessorHandlers, e, r, {
        block: !0
      });
    }, t.prototype.performVisualTasks = function(e, r, n) {
      this._performStageTasks(this._visualHandlers, e, r, n);
    }, t.prototype._performStageTasks = function(e, r, n, i) {
      i = i || {};
      var a = !1, o = this;
      w(e, function(u, l) {
        if (!(i.visualType && i.visualType !== u.visualType)) {
          var f = o._stageTaskMap.get(u.uid), h = f.seriesTaskMap, c = f.overallTask;
          if (c) {
            var v, d = c.agentStubMap;
            d.each(function(p) {
              s(i, p) && (p.dirty(), v = !0);
            }), v && c.dirty(), o.updatePayload(c, n);
            var y = o.getPerformArgs(c, i.block);
            d.each(function(p) {
              p.perform(y);
            }), c.perform(y) && (a = !0);
          } else h && h.each(function(p, g) {
            s(i, p) && p.dirty();
            var m = o.getPerformArgs(p, i.block);
            m.skip = !u.performRawSeries && r.isSeriesFiltered(p.context.model), o.updatePayload(p, n), p.perform(m) && (a = !0);
          });
        }
      });
      function s(u, l) {
        return u.setDirty && (!u.dirtyMap || u.dirtyMap.get(l.__pipeline.id));
      }
      this.unfinished = a || this.unfinished;
    }, t.prototype.performSeriesTasks = function(e) {
      var r;
      e.eachSeries(function(n) {
        r = n.dataTask.perform() || r;
      }), this.unfinished = r || this.unfinished;
    }, t.prototype.plan = function() {
      this._pipelineMap.each(function(e) {
        var r = e.tail;
        do {
          if (r.__block) {
            e.blockIndex = r.__idxInPipeline;
            break;
          }
          r = r.getUpstream();
        } while (r);
      });
    }, t.prototype.updatePayload = function(e, r) {
      r !== "remain" && (e.context.payload = r);
    }, t.prototype._createSeriesStageTask = function(e, r, n, i) {
      var a = this, o = r.seriesTaskMap, s = r.seriesTaskMap = U(), u = e.seriesType, l = e.getTargetSeries;
      e.createOnAllSeries ? n.eachRawSeries(f) : u ? n.eachRawSeriesByType(u, f) : l && l(n, i).each(f);
      function f(h) {
        var c = h.uid, v = s.set(c, o && o.get(c) || sa({
          plan: vD,
          reset: dD,
          count: gD
        }));
        v.context = {
          model: h,
          ecModel: n,
          api: i,
          // PENDING: `useClearVisual` not used?
          useClearVisual: e.isVisual && !e.isLayout,
          plan: e.plan,
          reset: e.reset,
          scheduler: a
        }, a._pipe(h, v);
      }
    }, t.prototype._createOverallStageTask = function(e, r, n, i) {
      var a = this, o = r.overallTask = r.overallTask || sa({
        reset: lD
      });
      o.context = {
        ecModel: n,
        api: i,
        overallReset: e.overallReset,
        scheduler: a
      };
      var s = o.agentStubMap, u = o.agentStubMap = U(), l = e.seriesType, f = e.getTargetSeries, h = e.dirtyOnOverallProgress, c = !1, v = "";
      qt(!e.createOnAllSeries, v), l ? n.eachRawSeriesByType(l, d) : f ? f(n, i).each(d) : w(n.getSeries(), d);
      function d(y) {
        var p = y.uid, g = u.set(p, s && s.get(p) || // When the result of `getTargetSeries` changed, the overallTask
        // should be set as dirty and re-performed.
        (c = !0, sa({
          reset: fD,
          onDirty: cD
        })));
        g.context = {
          model: y,
          dirtyOnOverallProgress: h
          // FIXME:TS never used, so comment it
          // modifyOutputEnd: modifyOutputEnd
        }, g.agent = o, g.__block = h, a._pipe(y, g);
      }
      c && o.dirty();
    }, t.prototype._pipe = function(e, r) {
      var n = e.uid, i = this._pipelineMap.get(n);
      !i.head && (i.head = r), i.tail && i.tail.pipe(r), i.tail = r, r.__idxInPipeline = i.count++, r.__pipeline = i;
    }, t.wrapStageHandler = function(e, r) {
      return X(e) && (e = {
        overallReset: e,
        seriesType: yD(e)
      }), e.uid = lu("stageHandler"), r && (e.visualType = r), e;
    }, t;
  })()
);
function lD(t) {
  t.overallReset(t.ecModel, t.api, t.payload);
}
function fD(t) {
  return t.dirtyOnOverallProgress && hD;
}
function hD() {
  this.agent.dirty(), this.getDownstream().dirty();
}
function cD() {
  this.agent && this.agent.dirty();
}
function vD(t) {
  return t.plan ? t.plan(t.model, t.ecModel, t.api, t.payload) : null;
}
function dD(t) {
  t.useClearVisual && t.data.clearAllVisual();
  var e = t.resetDefines = $e(t.reset(t.model, t.ecModel, t.api, t.payload));
  return e.length > 1 ? H(e, function(r, n) {
    return S_(n);
  }) : pD;
}
var pD = S_(0);
function S_(t) {
  return function(e, r) {
    var n = r.data, i = r.resetDefines[t];
    if (i && i.dataEach)
      for (var a = e.start; a < e.end; a++)
        i.dataEach(n, a);
    else i && i.progress && i.progress(e, n);
  };
}
function gD(t) {
  return t.data.count();
}
function yD(t) {
  bs = null;
  try {
    t(xa, b_);
  } catch {
  }
  return bs;
}
var xa = {}, b_ = {}, bs;
T_(xa, oc);
T_(b_, Hy);
xa.eachSeriesByType = xa.eachRawSeriesByType = function(t) {
  bs = t;
};
xa.eachComponent = function(t) {
  t.mainType === "series" && t.subType && (bs = t.subType);
};
function T_(t, e) {
  for (var r in e.prototype)
    t[r] = Re;
}
var R = j.darkColor, rp = R.background, Bi = function() {
  return {
    axisLine: {
      lineStyle: {
        color: R.axisLine
      }
    },
    splitLine: {
      lineStyle: {
        color: R.axisSplitLine
      }
    },
    splitArea: {
      areaStyle: {
        color: [R.backgroundTint, R.backgroundTransparent]
      }
    },
    minorSplitLine: {
      lineStyle: {
        color: R.axisMinorSplitLine
      }
    },
    axisLabel: {
      color: R.axisLabel
    },
    axisName: {}
  };
}, np = {
  label: {
    color: R.secondary
  },
  itemStyle: {
    borderColor: R.borderTint
  },
  dividerLineStyle: {
    color: R.border
  }
}, w_ = {
  darkMode: !0,
  color: R.theme,
  backgroundColor: rp,
  axisPointer: {
    lineStyle: {
      color: R.border
    },
    crossStyle: {
      color: R.borderShade
    },
    label: {
      color: R.tertiary
    }
  },
  legend: {
    textStyle: {
      color: R.secondary
    },
    pageTextStyle: {
      color: R.tertiary
    }
  },
  textStyle: {
    color: R.secondary
  },
  title: {
    textStyle: {
      color: R.primary
    },
    subtextStyle: {
      color: R.quaternary
    }
  },
  toolbox: {
    iconStyle: {
      borderColor: R.accent50
    },
    feature: {
      dataView: {
        backgroundColor: rp,
        textColor: R.primary,
        textareaColor: R.background,
        textareaBorderColor: R.border,
        buttonColor: R.accent50,
        buttonTextColor: R.neutral00
      }
    }
  },
  tooltip: {
    backgroundColor: R.neutral20,
    defaultBorderColor: R.border,
    textStyle: {
      color: R.tertiary
    }
  },
  dataZoom: {
    borderColor: R.accent10,
    textStyle: {
      color: R.tertiary
    },
    brushStyle: {
      color: R.backgroundTint
    },
    handleStyle: {
      color: R.neutral00,
      borderColor: R.accent20
    },
    moveHandleStyle: {
      color: R.accent40
    },
    emphasis: {
      handleStyle: {
        borderColor: R.accent50
      }
    },
    dataBackground: {
      lineStyle: {
        color: R.accent30
      },
      areaStyle: {
        color: R.accent20
      }
    },
    selectedDataBackground: {
      lineStyle: {
        color: R.accent50
      },
      areaStyle: {
        color: R.accent30
      }
    }
  },
  visualMap: {
    textStyle: {
      color: R.secondary
    },
    handleStyle: {
      borderColor: R.neutral30
    }
  },
  timeline: {
    lineStyle: {
      color: R.accent10
    },
    label: {
      color: R.tertiary
    },
    controlStyle: {
      color: R.accent30,
      borderColor: R.accent30
    }
  },
  calendar: {
    itemStyle: {
      color: R.neutral00,
      borderColor: R.neutral20
    },
    dayLabel: {
      color: R.tertiary
    },
    monthLabel: {
      color: R.secondary
    },
    yearLabel: {
      color: R.secondary
    }
  },
  matrix: {
    x: np,
    y: np,
    backgroundColor: {
      borderColor: R.axisLine
    },
    body: {
      itemStyle: {
        borderColor: R.borderTint
      }
    }
  },
  timeAxis: Bi(),
  logAxis: Bi(),
  valueAxis: Bi(),
  categoryAxis: Bi(),
  line: {
    symbol: "circle"
  },
  graph: {
    color: R.theme
  },
  gauge: {
    title: {
      color: R.secondary
    },
    axisLine: {
      lineStyle: {
        color: [[1, R.neutral05]]
      }
    },
    axisLabel: {
      color: R.axisLabel
    },
    detail: {
      color: R.primary
    }
  },
  candlestick: {
    itemStyle: {
      color: "#f64e56",
      color0: "#54ea92",
      borderColor: "#f64e56",
      borderColor0: "#54ea92"
      // borderColor: '#ca2824',
      // borderColor0: '#09a443'
    }
  },
  funnel: {
    itemStyle: {
      borderColor: R.background
    }
  },
  radar: (function() {
    var t = Bi();
    return t.axisName = {
      color: R.axisLabel
    }, t.axisLine.lineStyle.color = R.neutral20, t;
  })(),
  treemap: {
    breadcrumb: {
      itemStyle: {
        color: R.neutral20,
        textStyle: {
          color: R.secondary
        }
      },
      emphasis: {
        itemStyle: {
          color: R.neutral30
        }
      }
    }
  },
  sunburst: {
    itemStyle: {
      borderColor: R.background
    }
  },
  map: {
    itemStyle: {
      borderColor: R.border,
      areaColor: R.neutral10
    },
    label: {
      color: R.tertiary
    },
    emphasis: {
      label: {
        color: R.primary
      },
      itemStyle: {
        areaColor: R.highlight
      }
    },
    select: {
      label: {
        color: R.primary
      },
      itemStyle: {
        areaColor: R.highlight
      }
    }
  },
  geo: {
    itemStyle: {
      borderColor: R.border,
      areaColor: R.neutral10
    },
    emphasis: {
      label: {
        color: R.primary
      },
      itemStyle: {
        areaColor: R.highlight
      }
    },
    select: {
      label: {
        color: R.primary
      },
      itemStyle: {
        color: R.highlight
      }
    }
  }
};
w_.categoryAxis.splitLine.show = !1;
var mD = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.normalizeQuery = function(e) {
      var r = {}, n = {}, i = {};
      if (N(e)) {
        var a = Vt(e);
        r.mainType = a.main || null, r.subType = a.sub || null;
      } else {
        var o = ["Index", "Name", "Id"], s = {
          name: 1,
          dataIndex: 1,
          dataType: 1
        };
        w(e, function(u, l) {
          for (var f = !1, h = 0; h < o.length; h++) {
            var c = o[h], v = l.lastIndexOf(c);
            if (v > 0 && v === l.length - c.length) {
              var d = l.slice(0, v);
              d !== "data" && (r.mainType = d, r[c.toLowerCase()] = u, f = !0);
            }
          }
          s.hasOwnProperty(l) && (n[l] = u, f = !0), f || (i[l] = u);
        });
      }
      return {
        cptQuery: r,
        dataQuery: n,
        otherQuery: i
      };
    }, t.prototype.filter = function(e, r) {
      var n = this.eventInfo;
      if (!n)
        return !0;
      var i = n.targetEl, a = n.packedEvent, o = n.model, s = n.view;
      if (!o || !s)
        return !0;
      var u = r.cptQuery, l = r.dataQuery;
      return f(u, o, "mainType") && f(u, o, "subType") && f(u, o, "index", "componentIndex") && f(u, o, "name") && f(u, o, "id") && f(l, a, "name") && f(l, a, "dataIndex") && f(l, a, "dataType") && (!s.filterForExposedEvent || s.filterForExposedEvent(e, r.otherQuery, i, a));
      function f(h, c, v, d) {
        return h[v] == null || c[d || v] === h[v];
      }
    }, t.prototype.afterTrigger = function() {
      this.eventInfo = null;
    }, t;
  })()
), zf = ["symbol", "symbolSize", "symbolRotate", "symbolOffset"], ip = zf.concat(["symbolKeepAspect"]), _D = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(t, e) {
    var r = t.getData();
    if (t.legendIcon && r.setVisual("legendIcon", t.legendIcon), !t.hasSymbolVisual)
      return;
    for (var n = {}, i = {}, a = !1, o = 0; o < zf.length; o++) {
      var s = zf[o], u = t.get(s);
      X(u) ? (a = !0, i[s] = u) : n[s] = u;
    }
    if (n.symbol = n.symbol || t.defaultSymbol, r.setVisual(E({
      legendIcon: t.legendIcon || n.symbol,
      symbolKeepAspect: t.get("symbolKeepAspect")
    }, n)), e.isSeriesFiltered(t))
      return;
    var l = me(i);
    function f(h, c) {
      for (var v = t.getRawValue(c), d = t.getDataParams(c), y = 0; y < l.length; y++) {
        var p = l[y];
        h.setItemVisual(c, p, i[p](v, d));
      }
    }
    return {
      dataEach: a ? f : null
    };
  }
}, SD = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(t, e) {
    if (!t.hasSymbolVisual || e.isSeriesFiltered(t))
      return;
    var r = t.getData();
    function n(i, a) {
      for (var o = i.getItemModel(a), s = 0; s < ip.length; s++) {
        var u = ip[s], l = o.getShallow(u, !0);
        l != null && i.setItemVisual(a, u, l);
      }
    }
    return {
      dataEach: r.hasItemOption ? n : null
    };
  }
};
function bD(t, e, r) {
  switch (r) {
    case "color":
      var n = t.getItemVisual(e, "style");
      return n[t.getVisual("drawType")];
    case "opacity":
      return t.getItemVisual(e, "style").opacity;
    case "symbol":
    case "symbolSize":
    case "liftZ":
      return t.getItemVisual(e, r);
  }
}
function TD(t, e) {
  switch (e) {
    case "color":
      var r = t.getVisual("style");
      return r[t.getVisual("drawType")];
    case "opacity":
      return t.getVisual("style").opacity;
    case "symbol":
    case "symbolSize":
    case "liftZ":
      return t.getVisual(e);
  }
}
function Gn(t, e, r, n, i) {
  var a = t + e;
  r.isSilent(a) || n.eachComponent({
    mainType: "series",
    subType: "pie"
  }, function(o) {
    for (var s = o.seriesIndex, u = o.option.selectedMap, l = i.selected, f = 0; f < l.length; f++)
      if (l[f].seriesIndex === s) {
        var h = o.getData(), c = Oa(h, i.fromActionPayload);
        r.trigger(a, {
          type: a,
          seriesId: o.id,
          name: B(c) ? h.getName(c[0]) : h.getName(c),
          selected: N(u) ? u : E({}, u)
        });
      }
  });
}
function wD(t, e, r) {
  t.on("selectchanged", function(n) {
    var i = r.getModel();
    n.isFromClick ? (Gn("map", "selectchanged", e, i, n), Gn("pie", "selectchanged", e, i, n)) : n.fromAction === "select" ? (Gn("map", "selected", e, i, n), Gn("pie", "selected", e, i, n)) : n.fromAction === "unselect" && (Gn("map", "unselected", e, i, n), Gn("pie", "unselected", e, i, n));
  });
}
function Qi(t, e, r) {
  for (var n; t && !(e(t) && (n = t, r)); )
    t = t.__hostTarget || t.parent;
  return n;
}
var vt = new Kt(), C_ = {};
function CD(t, e) {
  C_[t] = e;
}
function x_(t) {
  return C_[t];
}
var cu = de();
function xD(t) {
  cu(t).prepare = {};
}
function MD(t) {
  cu(t).fullUpdate = {};
}
function DD(t) {
  return cu(t).prepare;
}
function mi(t) {
  return cu(t).fullUpdate;
}
var ID = Math.round(Math.random() * 9), AD = typeof Object.defineProperty == "function", PD = (function() {
  function t() {
    this._id = "__ec_inner_" + ID++;
  }
  return t.prototype.get = function(e) {
    return this._guard(e)[this._id];
  }, t.prototype.set = function(e, r) {
    var n = this._guard(e);
    return AD ? Object.defineProperty(n, this._id, {
      value: r,
      enumerable: !1,
      configurable: !0
    }) : n[this._id] = r, this;
  }, t.prototype.delete = function(e) {
    return this.has(e) ? (delete this._guard(e)[this._id], !0) : !1;
  }, t.prototype.has = function(e) {
    return !!this._guard(e)[this._id];
  }, t.prototype._guard = function(e) {
    if (e !== Object(e))
      throw TypeError("Value of WeakMap is not a non-null object.");
    return e;
  }, t;
})(), LD = ye.extend({
  type: "triangle",
  shape: {
    cx: 0,
    cy: 0,
    width: 0,
    height: 0
  },
  buildPath: function(t, e) {
    var r = e.cx, n = e.cy, i = e.width / 2, a = e.height / 2;
    t.moveTo(r, n - a), t.lineTo(r + i, n + a), t.lineTo(r - i, n + a), t.closePath();
  }
}), RD = ye.extend({
  type: "diamond",
  shape: {
    cx: 0,
    cy: 0,
    width: 0,
    height: 0
  },
  buildPath: function(t, e) {
    var r = e.cx, n = e.cy, i = e.width / 2, a = e.height / 2;
    t.moveTo(r, n - a), t.lineTo(r + i, n), t.lineTo(r, n + a), t.lineTo(r - i, n), t.closePath();
  }
}), ED = ye.extend({
  type: "pin",
  shape: {
    // x, y on the cusp
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  buildPath: function(t, e) {
    var r = e.x, n = e.y, i = e.width / 5 * 3, a = Math.max(i, e.height), o = i / 2, s = o * o / (a - o), u = n - a + o + s, l = Math.asin(s / o), f = Math.cos(l) * o, h = Math.sin(l), c = Math.cos(l), v = o * 0.6, d = o * 0.7;
    t.moveTo(r - f, u + s), t.arc(r, u, o, Math.PI - l, Math.PI * 2 + l), t.bezierCurveTo(r + f - h * v, u + s + c * v, r, n - d, r, n), t.bezierCurveTo(r, n - d, r - f + h * v, u + s + c * v, r - f, u + s), t.closePath();
  }
}), OD = ye.extend({
  type: "arrow",
  shape: {
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  buildPath: function(t, e) {
    var r = e.height, n = e.width, i = e.x, a = e.y, o = n / 3 * 2;
    t.moveTo(i, a), t.lineTo(i + o, a + r), t.lineTo(i, a + r / 4 * 3), t.lineTo(i - o, a + r), t.lineTo(i, a), t.closePath();
  }
}), kD = {
  line: Pn,
  rect: Ee,
  roundRect: Ee,
  square: Ee,
  circle: Na,
  diamond: RD,
  pin: ED,
  arrow: OD,
  triangle: LD
}, BD = {
  line: function(t, e, r, n, i) {
    i.x1 = t, i.y1 = e + n / 2, i.x2 = t + r, i.y2 = e + n / 2;
  },
  rect: function(t, e, r, n, i) {
    i.x = t, i.y = e, i.width = r, i.height = n;
  },
  roundRect: function(t, e, r, n, i) {
    i.x = t, i.y = e, i.width = r, i.height = n, i.r = Math.min(r, n) / 4;
  },
  square: function(t, e, r, n, i) {
    var a = Math.min(r, n);
    i.x = t, i.y = e, i.width = a, i.height = a;
  },
  circle: function(t, e, r, n, i) {
    i.cx = t + r / 2, i.cy = e + n / 2, i.r = Math.min(r, n) / 2;
  },
  diamond: function(t, e, r, n, i) {
    i.cx = t + r / 2, i.cy = e + n / 2, i.width = r, i.height = n;
  },
  pin: function(t, e, r, n, i) {
    i.x = t + r / 2, i.y = e + n / 2, i.width = r, i.height = n;
  },
  arrow: function(t, e, r, n, i) {
    i.x = t + r / 2, i.y = e + n / 2, i.width = r, i.height = n;
  },
  triangle: function(t, e, r, n, i) {
    i.cx = t + r / 2, i.cy = e + n / 2, i.width = r, i.height = n;
  }
}, Hf = {};
w(kD, function(t, e) {
  Hf[e] = new t();
});
var ND = ye.extend({
  type: "symbol",
  shape: {
    symbolType: "",
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  calculateTextPosition: function(t, e, r) {
    var n = hy(t, e, r), i = this.shape;
    return i && i.symbolType === "pin" && e.position === "inside" && (n.y = r.y + r.height * 0.4), n;
  },
  buildPath: function(t, e, r) {
    var n = e.symbolType;
    if (n !== "none") {
      var i = Hf[n];
      i || (n = "rect", i = Hf[n]), BD[n](e.x, e.y, e.width, e.height, i.shape), i.buildPath(t, i.shape, r);
    }
  }
});
function FD(t, e) {
  if (this.type !== "image") {
    var r = this.style;
    this.__isEmptyBrush ? (r.stroke = t, r.fill = e || j.color.neutral00, r.lineWidth = 2) : this.shape.symbolType === "line" ? r.stroke = t : r.fill = t, this.markRedraw();
  }
}
function vu(t, e, r, n, i, a, o) {
  var s = t.indexOf("empty") === 0;
  s && (t = t.substr(5, 1).toLowerCase() + t.substr(6));
  var u;
  return t.indexOf("image://") === 0 ? u = Bh(t.slice(8), new Y(e, r, n, i), o ? "center" : "cover") : t.indexOf("path://") === 0 ? u = au(t.slice(7), {}, new Y(e, r, n, i), o ? "center" : "cover") : u = new ND({
    shape: {
      symbolType: t,
      x: e,
      y: r,
      width: n,
      height: i
    }
  }), u.__isEmptyBrush = s, u.setColor = FD, a && u.setColor(a), u;
}
function zD(t, e) {
  if (t != null)
    return B(t) || (t = [t, t]), [Dt(t[0], e[0]) || 0, Dt(G(t[1], t[0]), e[1]) || 0];
}
function dn(t) {
  return isFinite(t);
}
function HD(t, e, r) {
  var n = e.x == null ? 0 : e.x, i = e.x2 == null ? 1 : e.x2, a = e.y == null ? 0 : e.y, o = e.y2 == null ? 0 : e.y2;
  e.global || (n = n * r.width + r.x, i = i * r.width + r.x, a = a * r.height + r.y, o = o * r.height + r.y), n = dn(n) ? n : 0, i = dn(i) ? i : 1, a = dn(a) ? a : 0, o = dn(o) ? o : 0;
  var s = t.createLinearGradient(n, a, i, o);
  return s;
}
function VD(t, e, r) {
  var n = r.width, i = r.height, a = Math.min(n, i), o = e.x == null ? 0.5 : e.x, s = e.y == null ? 0.5 : e.y, u = e.r == null ? 0.5 : e.r;
  e.global || (o = o * n + r.x, s = s * i + r.y, u = u * a), o = dn(o) ? o : 0.5, s = dn(s) ? s : 0.5, u = u >= 0 && dn(u) ? u : 0.5;
  var l = t.createRadialGradient(o, s, 0, o, s, u);
  return l;
}
function Vf(t, e, r) {
  for (var n = e.type === "radial" ? VD(t, e, r) : HD(t, e, r), i = e.colorStops, a = 0; a < i.length; a++)
    n.addColorStop(i[a].offset, i[a].color);
  return n;
}
function GD(t, e) {
  if (t === e || !t && !e)
    return !1;
  if (!t || !e || t.length !== e.length)
    return !0;
  for (var r = 0; r < t.length; r++)
    if (t[r] !== e[r])
      return !0;
  return !1;
}
function yo(t) {
  return parseInt(t, 10);
}
function mo(t, e, r) {
  var n = ["width", "height"][e], i = ["clientWidth", "clientHeight"][e], a = ["paddingLeft", "paddingTop"][e], o = ["paddingRight", "paddingBottom"][e];
  if (r[n] != null && r[n] !== "auto")
    return parseFloat(r[n]);
  var s = document.defaultView.getComputedStyle(t);
  return (t[i] || yo(s[n]) || yo(t.style[n])) - (yo(s[a]) || 0) - (yo(s[o]) || 0) || 0;
}
function UD(t, e) {
  return !t || t === "solid" || !(e > 0) ? null : t === "dashed" ? [4 * e, 2 * e] : t === "dotted" ? [e] : ve(t) ? [t] : B(t) ? t : null;
}
function M_(t) {
  var e = t.style, r = e.lineDash && e.lineWidth > 0 && UD(e.lineDash, e.lineWidth), n = e.lineDashOffset;
  if (r) {
    var i = e.strokeNoScale && t.getLineScale ? t.getLineScale() : 1;
    i && i !== 1 && (r = H(r, function(a) {
      return a / i;
    }), n /= i);
  }
  return [r, n];
}
var YD = new ci(!0);
function Ts(t) {
  var e = t.stroke;
  return !(e == null || e === "none" || !(t.lineWidth > 0));
}
function ap(t) {
  return typeof t == "string" && t !== "none";
}
function ws(t) {
  var e = t.fill;
  return e != null && e !== "none";
}
function op(t, e) {
  if (e.fillOpacity != null && e.fillOpacity !== 1) {
    var r = t.globalAlpha;
    t.globalAlpha = e.fillOpacity * e.opacity, t.fill(), t.globalAlpha = r;
  } else
    t.fill();
}
function sp(t, e) {
  if (e.strokeOpacity != null && e.strokeOpacity !== 1) {
    var r = t.globalAlpha;
    t.globalAlpha = e.strokeOpacity * e.opacity, t.stroke(), t.globalAlpha = r;
  } else
    t.stroke();
}
function Gf(t, e, r) {
  var n = Dy(e.image, e.__image, r);
  if (Zs(n)) {
    var i = t.createPattern(n, e.repeat || "repeat");
    if (typeof DOMMatrix == "function" && i && i.setTransform) {
      var a = new DOMMatrix();
      a.translateSelf(e.x || 0, e.y || 0), a.rotateSelf(0, 0, (e.rotation || 0) * Bg), a.scaleSelf(e.scaleX || 1, e.scaleY || 1), i.setTransform(a);
    }
    return i;
  }
}
function WD(t, e, r, n, i) {
  var a, o = Ts(r), s = ws(r), u = r.strokePercent, l = u < 1, f = !e.path;
  (!e.silent || l) && f && e.createPathProxy();
  var h = e.path || YD, c = e.__dirty;
  if (!n) {
    var v = r.fill, d = r.stroke, y = s && !!v.colorStops, p = o && !!d.colorStops, g = s && !!v.image, m = o && !!d.image, _ = void 0, S = void 0, b = void 0, T = void 0, C = void 0;
    (y || p) && (C = e.getBoundingRect()), y && (_ = c ? Vf(t, v, C) : e.__canvasFillGradient, e.__canvasFillGradient = _), p && (S = c ? Vf(t, d, C) : e.__canvasStrokeGradient, e.__canvasStrokeGradient = S), g && (b = c || !e.__canvasFillPattern ? Gf(t, v, e) : e.__canvasFillPattern, e.__canvasFillPattern = b), m && (T = c || !e.__canvasStrokePattern ? Gf(t, d, e) : e.__canvasStrokePattern, e.__canvasStrokePattern = T), y ? t.fillStyle = _ : g && (b ? t.fillStyle = b : s = !1), p ? t.strokeStyle = S : m && (T ? t.strokeStyle = T : o = !1);
  }
  var M = e.getGlobalScale();
  h.setScale(M[0], M[1], e.segmentIgnoreThreshold);
  var x, D;
  t.setLineDash && r.lineDash && (a = M_(e), x = a[0], D = a[1]);
  var I = !0;
  (f || c & $n) && (h.setDPR(t.dpr), l ? h.setContext(null) : (h.setContext(t), I = !1), h.reset(), e.buildPath(h, e.shape, n), h.toStatic(), e.pathUpdated()), I && h.rebuildPath(t, l ? u : 1), x && (t.setLineDash(x), t.lineDashOffset = D), n ? (i.batchFill = s, i.batchStroke = o) : r.strokeFirst ? (o && sp(t, r), s && op(t, r)) : (s && op(t, r), o && sp(t, r)), x && t.setLineDash([]);
}
function XD(t, e, r) {
  var n = e.__image = Dy(r.image, e.__image, e, e.onload);
  if (!(!n || !Zs(n))) {
    var i = r.x || 0, a = r.y || 0, o = e.getWidth(), s = e.getHeight(), u = n.width / n.height;
    if (o == null && s != null ? o = s * u : s == null && o != null ? s = o / u : o == null && s == null && (o = n.width, s = n.height), r.sWidth && r.sHeight) {
      var l = r.sx || 0, f = r.sy || 0;
      t.drawImage(n, l, f, r.sWidth, r.sHeight, i, a, o, s);
    } else if (r.sx && r.sy) {
      var l = r.sx, f = r.sy, h = o - l, c = s - f;
      t.drawImage(n, l, f, h, c, i, a, o, s);
    } else
      t.drawImage(n, i, a, o, s);
  }
}
function qD(t, e, r) {
  var n, i = r.text;
  if (i != null && (i += ""), i) {
    t.font = r.font || Er, t.textAlign = r.textAlign, t.textBaseline = r.textBaseline;
    var a = void 0, o = void 0;
    t.setLineDash && r.lineDash && (n = M_(e), a = n[0], o = n[1]), a && (t.setLineDash(a), t.lineDashOffset = o), r.strokeFirst ? (Ts(r) && t.strokeText(i, r.x, r.y), ws(r) && t.fillText(i, r.x, r.y)) : (ws(r) && t.fillText(i, r.x, r.y), Ts(r) && t.strokeText(i, r.x, r.y)), a && t.setLineDash([]);
  }
}
var up = ["shadowBlur", "shadowOffsetX", "shadowOffsetY"], lp = [
  ["lineCap", "butt"],
  ["lineJoin", "miter"],
  ["miterLimit", 10]
];
function D_(t, e, r, n, i) {
  var a = !1;
  if (!n && (r = r || {}, e === r))
    return !1;
  if (n || e.opacity !== r.opacity) {
    We(t, i), a = !0;
    var o = Math.max(Math.min(e.opacity, 1), 0);
    t.globalAlpha = isNaN(o) ? Sn.opacity : o;
  }
  (n || e.blend !== r.blend) && (a || (We(t, i), a = !0), t.globalCompositeOperation = e.blend || Sn.blend);
  for (var s = 0; s < up.length; s++) {
    var u = up[s];
    (n || e[u] !== r[u]) && (a || (We(t, i), a = !0), t[u] = t.dpr * (e[u] || 0));
  }
  return (n || e.shadowColor !== r.shadowColor) && (a || (We(t, i), a = !0), t.shadowColor = e.shadowColor || Sn.shadowColor), a;
}
function fp(t, e, r, n, i) {
  var a = e.style, o = n ? null : r && r.style || {};
  if (a === o)
    return !1;
  var s = D_(t, a, o, n, i);
  if ((n || a.fill !== o.fill) && (s || (We(t, i), s = !0), ap(a.fill) && (t.fillStyle = a.fill)), (n || a.stroke !== o.stroke) && (s || (We(t, i), s = !0), ap(a.stroke) && (t.strokeStyle = a.stroke)), (n || a.opacity !== o.opacity) && (s || (We(t, i), s = !0), t.globalAlpha = a.opacity == null ? 1 : a.opacity), e.hasStroke()) {
    var u = a.lineWidth, l = u / (a.strokeNoScale && e.getLineScale ? e.getLineScale() : 1);
    t.lineWidth !== l && (s || (We(t, i), s = !0), t.lineWidth = l);
  }
  for (var f = 0; f < lp.length; f++) {
    var h = lp[f], c = h[0];
    (n || a[c] !== o[c]) && (s || (We(t, i), s = !0), t[c] = a[c] || h[1]);
  }
  return s;
}
function $D(t, e, r, n, i) {
  return D_(t, e.style, r && r.style, n, i);
}
function I_(t, e) {
  var r = e.transform, n = t.dpr || 1;
  r ? t.setTransform(n * r[0], n * r[1], n * r[2], n * r[3], n * r[4], n * r[5]) : t.setTransform(n, 0, 0, n, 0, 0);
}
function ZD(t, e, r) {
  for (var n = !1, i = 0; i < t.length; i++) {
    var a = t[i];
    n = n || a.isZeroArea(), I_(e, a), e.beginPath(), a.buildPath(e, a.shape), e.clip();
  }
  r.allClipped = n;
}
function KD(t, e) {
  return t && e ? t[0] !== e[0] || t[1] !== e[1] || t[2] !== e[2] || t[3] !== e[3] || t[4] !== e[4] || t[5] !== e[5] : !(!t && !e);
}
var hp = 1, cp = 2, vp = 3, dp = 4;
function jD(t) {
  var e = ws(t), r = Ts(t);
  return !(t.lineDash || !(+e ^ +r) || e && typeof t.fill != "string" || r && typeof t.stroke != "string" || t.strokePercent < 1 || t.strokeOpacity < 1 || t.fillOpacity < 1);
}
function We(t, e) {
  e.batchFill && (e.batchFill = !1, t.fill()), e.batchStroke && (e.batchStroke = !1, t.stroke());
}
function hc(t, e) {
  var r = { inHover: !1, viewWidth: 0, viewHeight: 0, beforeBrushParam: {} };
  pn(t, e, r), ai(t, r);
}
function pn(t, e, r) {
  var n = e.transform;
  if (!e.shouldBePainted(r.viewWidth, r.viewHeight, !1, !1)) {
    e.__dirty &= ~Je, e.__isRendered = !1;
    return;
  }
  var i = e.__clipPaths, a = r.prevElClipPaths, o = e.style, s = !1, u = !1;
  if ((!a || GD(i, a)) && (a && (We(t, r), t.restore(), u = s = !0, r.prevElClipPaths = null, r.allClipped = !1, r.prevEl = null), i && i.length && (We(t, r), t.save(), ZD(i, t, r), s = !0, r.prevElClipPaths = i)), r.allClipped) {
    e.__dirty &= ~Je, e.__isRendered = !1;
    return;
  }
  e.beforeBrush && e.beforeBrush(r.beforeBrushParam), e.innerBeforeBrush();
  var l = r.prevEl;
  l || (u = s = !0);
  var f = e instanceof ye && e.autoBatch && jD(o);
  s || KD(n, l.transform) ? (We(t, r), I_(t, e)) : f || We(t, r), e instanceof ye ? (r.lastDrawType !== hp && (u = !0, r.lastDrawType = hp), fp(t, e, l, u, r), (!f || !r.batchFill && !r.batchStroke) && t.beginPath(), WD(t, e, o, f, r)) : e instanceof fs ? (r.lastDrawType !== vp && (u = !0, r.lastDrawType = vp), fp(t, e, l, u, r), qD(t, e, o)) : e instanceof Nr ? (r.lastDrawType !== cp && (u = !0, r.lastDrawType = cp), $D(t, e, l, u, r), XD(t, e, o)) : e.getTemporalDisplayables && (r.lastDrawType !== dp && (u = !0, r.lastDrawType = dp), QD(t, e, r)), e.innerAfterBrush(), e.afterBrush && (f && We(t, r), e.afterBrush()), r.prevEl = e, e.__dirty = 0, e.__isRendered = !0;
}
function ai(t, e) {
  We(t, e), e.prevElClipPaths && t.restore();
}
function QD(t, e, r) {
  var n = e.getDisplayables(), i = e.getTemporalDisplayables();
  t.save();
  var a = {
    prevElClipPaths: null,
    prevEl: null,
    allClipped: !1,
    viewWidth: r.viewWidth,
    viewHeight: r.viewHeight,
    inHover: r.inHover,
    beforeBrushParam: {}
  }, o, s;
  for (o = e.getCursor(), s = n.length; o < s; o++) {
    var u = n[o];
    u.beforeBrush && u.beforeBrush(r.beforeBrushParam), u.innerBeforeBrush(), pn(t, u, a), u.innerAfterBrush(), u.afterBrush && u.afterBrush(), a.prevEl = u;
  }
  ai(t, a);
  for (var l = 0, f = i.length; l < f; l++) {
    var u = i[l];
    u.beforeBrush && u.beforeBrush(r.beforeBrushParam), u.innerBeforeBrush(), pn(t, u, a), u.innerAfterBrush(), u.afterBrush && u.afterBrush(), a.prevEl = u;
  }
  ai(t, a), e.clearTemporalDisplayables(), e.notClear = !0, t.restore();
}
var Sl = new PD(), pp = new ui(100), gp = ["symbol", "symbolSize", "symbolKeepAspect", "color", "backgroundColor", "dashArrayX", "dashArrayY", "maxTileWidth", "maxTileHeight"];
function Uf(t, e) {
  if (t === "none")
    return null;
  var r = e.getDevicePixelRatio(), n = e.getZr(), i = n.painter.type === "svg";
  t.dirty && Sl.delete(t);
  var a = Sl.get(t);
  if (a)
    return a;
  var o = _e(t, {
    symbol: "rect",
    symbolSize: 1,
    symbolKeepAspect: !0,
    color: "rgba(0, 0, 0, 0.2)",
    backgroundColor: null,
    dashArrayX: 5,
    dashArrayY: 5,
    rotation: 0,
    maxTileWidth: 512,
    maxTileHeight: 512
  });
  o.backgroundColor === "none" && (o.backgroundColor = null);
  var s = {
    repeat: "repeat"
  };
  return u(s), s.rotation = o.rotation, s.scaleX = s.scaleY = i ? 1 : 1 / r, Sl.set(t, s), t.dirty = !1, s;
  function u(l) {
    for (var f = [r], h = !0, c = 0; c < gp.length; ++c) {
      var v = o[gp[c]];
      if (v != null && !B(v) && !N(v) && !ve(v) && typeof v != "boolean") {
        h = !1;
        break;
      }
      f.push(v);
    }
    var d;
    if (h) {
      d = f.join(",") + (i ? "-svg" : "");
      var y = pp.get(d);
      y && (i ? l.svgElement = y : l.image = y);
    }
    var p = P_(o.dashArrayX), g = JD(o.dashArrayY), m = A_(o.symbol), _ = eI(p), S = L_(g), b = !i && Le.createCanvas(), T = i && {
      tag: "g",
      attrs: {},
      key: "dcl",
      children: []
    }, C = x(), M;
    b && (b.width = C.width * r, b.height = C.height * r, M = b.getContext("2d")), D(), h && pp.put(d, b || T), l.image = b, l.svgElement = T, l.svgWidth = C.width, l.svgHeight = C.height;
    function x() {
      for (var I = 1, L = 0, A = _.length; L < A; ++L)
        I = Iv(I, _[L]);
      for (var P = 1, L = 0, A = m.length; L < A; ++L)
        P = Iv(P, m[L].length);
      I *= P;
      var k = S * _.length * m.length;
      return {
        width: Math.max(1, Math.min(I, o.maxTileWidth)),
        height: Math.max(1, Math.min(k, o.maxTileHeight))
      };
    }
    function D() {
      M && (M.clearRect(0, 0, b.width, b.height), o.backgroundColor && (M.fillStyle = o.backgroundColor, M.fillRect(0, 0, b.width, b.height)));
      for (var I = 0, L = 0; L < g.length; ++L)
        I += g[L];
      if (I <= 0)
        return;
      for (var A = -S, P = 0, k = 0, O = 0; A < C.height; ) {
        if (P % 2 === 0) {
          for (var V = k / 2 % m.length, K = 0, J = 0, ee = 0; K < C.width * 2; ) {
            for (var Q = 0, L = 0; L < p[O].length; ++L)
              Q += p[O][L];
            if (Q <= 0)
              break;
            if (J % 2 === 0) {
              var re = (1 - o.symbolSize) * 0.5, ne = K + p[O][J] * re, q = A + g[P] * re, se = p[O][J] * o.symbolSize, Se = g[P] * o.symbolSize, tt = ee / 2 % m[V].length;
              Qt(ne, q, se, Se, m[V][tt]);
            }
            K += p[O][J], ++ee, ++J, J === p[O].length && (J = 0);
          }
          ++O, O === p.length && (O = 0);
        }
        A += g[P], ++k, ++P, P === g.length && (P = 0);
      }
      function Qt(rt, Ae, $, te, zr) {
        var He = i ? 1 : r, kc = vu(zr, rt * He, Ae * He, $ * He, te * He, o.color, o.symbolKeepAspect);
        if (i) {
          var Bc = n.painter.renderOneToVNode(kc);
          Bc && T.children.push(Bc);
        } else
          hc(M, kc);
      }
    }
  }
}
function A_(t) {
  if (!t || t.length === 0)
    return [["rect"]];
  if (N(t))
    return [[t]];
  for (var e = !0, r = 0; r < t.length; ++r)
    if (!N(t[r])) {
      e = !1;
      break;
    }
  if (e)
    return A_([t]);
  for (var n = [], r = 0; r < t.length; ++r)
    N(t[r]) ? n.push([t[r]]) : n.push(t[r]);
  return n;
}
function P_(t) {
  if (!t || t.length === 0)
    return [[0, 0]];
  if (ve(t)) {
    var e = Math.ceil(t);
    return [[e, e]];
  }
  for (var r = !0, n = 0; n < t.length; ++n)
    if (!ve(t[n])) {
      r = !1;
      break;
    }
  if (r)
    return P_([t]);
  for (var i = [], n = 0; n < t.length; ++n)
    if (ve(t[n])) {
      var e = Math.ceil(t[n]);
      i.push([e, e]);
    } else {
      var e = H(t[n], function(s) {
        return Math.ceil(s);
      });
      e.length % 2 === 1 ? i.push(e.concat(e)) : i.push(e);
    }
  return i;
}
function JD(t) {
  if (!t || typeof t == "object" && t.length === 0)
    return [0, 0];
  if (ve(t)) {
    var e = Math.ceil(t);
    return [e, e];
  }
  var r = H(t, function(n) {
    return Math.ceil(n);
  });
  return t.length % 2 ? r.concat(r) : r;
}
function eI(t) {
  return H(t, function(e) {
    return L_(e);
  });
}
function L_(t) {
  for (var e = 0, r = 0; r < t.length; ++r)
    e += t[r];
  return t.length % 2 === 1 ? e * 2 : e;
}
var tI = xh(rI);
function rI(t, e) {
  t.eachRawSeries(function(r) {
    if (!t.isSeriesFiltered(r)) {
      var n = r.getData();
      n.hasItemVisual() && n.each(function(o) {
        var s = n.getItemVisual(o, "decal");
        if (s) {
          var u = n.ensureUniqueItemVisual(o, "style");
          u.decal = Uf(s, e);
        }
      });
      var i = n.getVisual("decal");
      if (i) {
        var a = n.getVisual("style");
        a.decal = Uf(i, e);
      }
    }
  });
}
var nI = "6.1.0", iI = {
  zrender: "6.1.0"
}, aI = 1, oI = 800, sI = 900, uI = 920, lI = 1e3, fI = 2e3, yp = 5e3, R_ = 1e3, hI = 1100, cc = 2e3, E_ = 3e3, cI = 4e3, du = 4500, vI = 4600, dI = 5e3, pI = 6e3, O_ = 7e3, k_ = {
  PROCESSOR: {
    SERIES_FILTER: oI,
    AXIS_STATISTICS: uI,
    FILTER: lI,
    STATISTIC: yp,
    STATISTICS: yp
  },
  VISUAL: {
    LAYOUT: R_,
    PROGRESSIVE_LAYOUT: hI,
    GLOBAL: cc,
    CHART: E_,
    POST_CHART_LAYOUT: vI,
    COMPONENT: cI,
    BRUSH: dI,
    CHART_ITEM: du,
    ARIA: pI,
    DECAL: O_
  }
}, Ce = "__flagInMainProcess", _o = "__mainProcessVersion", Me = "__pendingUpdate", bl = "__needsUpdateStatus", mp = /^[a-zA-Z0-9_]+$/, Tl = "__connectUpdateStatus", _p = 0, gI = 1, yI = 2;
function B_(t) {
  return function() {
    for (var e = [], r = 0; r < arguments.length; r++)
      e[r] = arguments[r];
    if (this.isDisposed()) {
      this.id;
      return;
    }
    return F_(this, t, e);
  };
}
function N_(t) {
  return function() {
    for (var e = [], r = 0; r < arguments.length; r++)
      e[r] = arguments[r];
    return F_(this, t, e);
  };
}
function F_(t, e, r) {
  return r[0] = r[0] && r[0].toLowerCase(), Kt.prototype[e].apply(t, r);
}
var z_ = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      return t !== null && t.apply(this, arguments) || this;
    }
    return e;
  })(Kt)
), H_ = z_.prototype;
H_.on = N_("on");
H_.off = N_("off");
var un, wl, So, ir, bo, Cl, xl, Un, Yn, Sp, bp, Ml, Tp, To, wp, V_, ht, Cp, Wn, Cs = (
  /** @class */
  (function(t) {
    F(e, t);
    function e(r, n, i) {
      var a = t.call(this, new mD()) || this;
      a._chartsViews = [], a._chartsMap = {}, a._componentsViews = [], a._componentsMap = {}, a._pendingActions = [], i = i || {}, a.__v_skip = !0, a._dom = r;
      var o = "canvas", s = "auto", u = !1;
      a[_o] = 1, i.ssr && dy(function(c) {
        var v = he(c), d = v.dataIndex;
        if (d != null) {
          var y = U();
          return y.set("series_index", v.seriesIndex), y.set("data_index", d), v.ssrType && y.set("ssr_type", v.ssrType), y;
        }
      });
      var l = a._zr = vf(r, {
        renderer: i.renderer || o,
        devicePixelRatio: i.devicePixelRatio,
        width: i.width,
        height: i.height,
        ssr: i.ssr,
        useDirtyRect: G(i.useDirtyRect, u),
        useCoarsePointer: G(i.useCoarsePointer, s),
        pointerSize: i.pointerSize
      });
      a._ssr = i.ssr, a._throttledZrFlush = fc(le(l.flush, l), 17), a._updateTheme(n), a._locale = nx(i.locale || Rm), a._coordSysMgr = new fu();
      var f = a._api = wp(a);
      function h(c, v) {
        return c.__prio - v.__prio;
      }
      return Oo(Ms, h), Oo(Xf, h), a._scheduler = new __(a, f, Xf, Ms), a._messageCenter = new z_(), a._initEvents(), a.resize = le(a.resize, a), l.animation.on("frame", a._onframe, a), Sp(l, a), bp(l, a), va(a), a;
    }
    return e.prototype._onframe = function() {
      if (!this._disposed) {
        var r = this._scheduler, n = this._model, i = this._api;
        if (Cp(this), this[Me]) {
          var a = this[Me].silent;
          this[Ce] = !0, Wn(this);
          try {
            un(this), ir.update.call(this, null, this[Me].updateParams);
          } catch (u) {
            throw this[Ce] = !1, this[Me] = null, u;
          }
          this._zr.flush(), this[Ce] = !1, this[Me] = null, Un.call(this, a), Yn.call(this, a);
        } else if (r.unfinished) {
          var o = aI;
          do {
            r.unfinished = !1;
            var s = Le.getTime();
            r.performSeriesTasks(n), r.performDataProcessorTasks(n), Cl(this, n), r.performVisualTasks(n), To(this, this._model, i, "remain", {}), o -= Le.getTime() - s;
          } while (o > 0 && r.unfinished);
          r.unfinished || this._zr.flush();
        }
      }
    }, e.prototype.getDom = function() {
      return this._dom;
    }, e.prototype.getId = function() {
      return this.id;
    }, e.prototype.getZr = function() {
      return this._zr;
    }, e.prototype.isSSR = function() {
      return this._ssr;
    }, e.prototype.setOption = function(r, n, i) {
      if (!this[Ce]) {
        if (this._disposed) {
          this.id;
          return;
        }
        var a, o, s;
        if (z(n) && (i = n.lazyUpdate, a = n.silent, o = n.replaceMerge, s = n.transition, n = n.notMerge), this[Ce] = !0, Wn(this), !this._model || n) {
          var u = new qx(this._api), l = this._theme, f = this._model = new oc();
          f.scheduler = this._scheduler, f.ssr = this._ssr, f.init(null, null, null, l, this._locale, u);
        }
        this._model.setOption(r, {
          replaceMerge: o
        }, qf);
        var h = {
          seriesTransition: s,
          optionChanged: !0
        };
        if (i)
          this[Me] = {
            silent: a,
            updateParams: h
          }, this[Ce] = !1, this.getZr().wakeUp();
        else {
          try {
            un(this), ir.update.call(this, null, h);
          } catch (c) {
            throw this[Me] = null, this[Ce] = !1, c;
          }
          this._ssr || this._zr.flush(), this[Me] = null, this[Ce] = !1, Un.call(this, a), Yn.call(this, a);
        }
      }
    }, e.prototype.setTheme = function(r, n) {
      if (!this[Ce]) {
        if (this._disposed) {
          this.id;
          return;
        }
        var i = this._model;
        if (i) {
          var a = n && n.silent, o = null;
          this[Me] && (a == null && (a = this[Me].silent), o = this[Me].updateParams, this[Me] = null), this[Ce] = !0, Wn(this);
          try {
            this._updateTheme(r), i.setTheme(this._theme), un(this), ir.update.call(this, {
              type: "setTheme"
            }, o);
          } catch (s) {
            throw this[Ce] = !1, s;
          }
          this[Ce] = !1, Un.call(this, a), Yn.call(this, a);
        }
      }
    }, e.prototype._updateTheme = function(r) {
      N(r) && (r = G_[r]), r && (r = Z(r), r && Km(r, !0), this._theme = r);
    }, e.prototype.getModel = function() {
      return this._model;
    }, e.prototype.getOption = function() {
      return this._model && this._model.getOption();
    }, e.prototype.getWidth = function() {
      return this._zr.getWidth();
    }, e.prototype.getHeight = function() {
      return this._zr.getHeight();
    }, e.prototype.getDevicePixelRatio = function() {
      return this._zr.painter.dpr || W.hasGlobalWindow && window.devicePixelRatio || 1;
    }, e.prototype.getRenderedCanvas = function(r) {
      return this.renderToCanvas(r);
    }, e.prototype.renderToCanvas = function(r) {
      r = r || {};
      var n = this._zr.painter;
      return n.getRenderedCanvas({
        backgroundColor: r.backgroundColor || this._model.get("backgroundColor"),
        pixelRatio: r.pixelRatio || this.getDevicePixelRatio()
      });
    }, e.prototype.renderToSVGString = function(r) {
      r = r || {};
      var n = this._zr.painter;
      return n.renderToString({
        useViewBox: r.useViewBox
      });
    }, e.prototype.getSvgDataURL = function() {
      var r = this._zr, n = r.storage.getDisplayList();
      return w(n, function(i) {
        i.stopAnimation(null, !0);
      }), r.painter.toDataURL();
    }, e.prototype.getDataURL = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      r = r || {};
      var n = r.excludeComponents, i = this._model, a = [], o = this;
      w(n, function(u) {
        i.eachComponent({
          mainType: u
        }, function(l) {
          var f = o._componentsMap[l.__viewId];
          f.group.ignore || (a.push(f), f.group.ignore = !0);
        });
      });
      var s = this._zr.painter.getType() === "svg" ? this.getSvgDataURL() : this.renderToCanvas(r).toDataURL("image/" + (r && r.type || "png"));
      return w(a, function(u) {
        u.group.ignore = !1;
      }), s;
    }, e.prototype.getConnectedDataURL = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      var n = r.type === "svg", i = this.group, a = Math.min, o = Math.max, s = 1 / 0;
      if (Ds[i]) {
        var u = s, l = s, f = -s, h = -s, c = [], v = r && r.pixelRatio || this.getDevicePixelRatio();
        w(Tn, function(_, S) {
          if (_.group === i) {
            var b = n ? _.getZr().painter.getSvgDom().innerHTML : _.renderToCanvas(Z(r)), T = _.getDom().getBoundingClientRect();
            u = a(T.left, u), l = a(T.top, l), f = o(T.right, f), h = o(T.bottom, h), c.push({
              dom: b,
              left: T.left,
              top: T.top
            });
          }
        }), u *= v, l *= v, f *= v, h *= v;
        var d = f - u, y = h - l, p = Le.createCanvas(), g = vf(p, {
          renderer: n ? "svg" : "canvas"
        });
        if (g.resize({
          width: d,
          height: y
        }), n) {
          var m = "";
          return w(c, function(_) {
            var S = _.left - u, b = _.top - l;
            m += '<g transform="translate(' + S + "," + b + ')">' + _.dom + "</g>";
          }), g.painter.getSvgRoot().innerHTML = m, r.connectedBackgroundColor && g.painter.setBackgroundColor(r.connectedBackgroundColor), g.refreshImmediately(), g.painter.toDataURL();
        } else
          return r.connectedBackgroundColor && g.add(new Ee({
            shape: {
              x: 0,
              y: 0,
              width: d,
              height: y
            },
            style: {
              fill: r.connectedBackgroundColor
            }
          })), w(c, function(_) {
            var S = new Nr({
              style: {
                x: _.left * v - u,
                y: _.top * v - l,
                image: _.dom
              }
            });
            g.add(S);
          }), g.refreshImmediately(), p.toDataURL("image/" + (r && r.type || "png"));
      } else
        return this.getDataURL(r);
    }, e.prototype.convertToPixel = function(r, n, i) {
      return bo(this, "convertToPixel", r, n, i);
    }, e.prototype.convertToLayout = function(r, n, i) {
      return bo(this, "convertToLayout", r, n, i);
    }, e.prototype.convertFromPixel = function(r, n, i) {
      return bo(this, "convertFromPixel", r, n, i);
    }, e.prototype.containPixel = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      var i = this._model, a, o = Xu(i, r);
      return w(o, function(s, u) {
        u.indexOf("Models") >= 0 && w(s, function(l) {
          var f = l.coordinateSystem;
          if (f && f.containPoint)
            a = a || !!f.containPoint(n);
          else if (u === "seriesModels") {
            var h = this._chartsMap[l.__viewId];
            h && h.containPoint && (a = a || h.containPoint(n, l));
          }
        }, this);
      }, this), !!a;
    }, e.prototype.getVisual = function(r, n) {
      var i = this._model, a = Xu(i, r, {
        defaultMainType: "series"
      }), o = a.seriesModel, s = o.getData(), u = a.hasOwnProperty("dataIndexInside") ? a.dataIndexInside : a.hasOwnProperty("dataIndex") ? s.indexOfRawIndex(a.dataIndex) : null;
      return u != null ? bD(s, u, n) : TD(s, n);
    }, e.prototype.getViewOfComponentModel = function(r) {
      return this._componentsMap[r.__viewId];
    }, e.prototype.getViewOfSeriesModel = function(r) {
      return this._chartsMap[r.__viewId];
    }, e.prototype._initEvents = function() {
      var r = this;
      w(mI, function(i) {
        var a = function(o) {
          var s = r.getModel(), u = o.target, l, f = i === "globalout";
          if (f ? l = {} : u && Qi(u, function(y) {
            var p = he(y);
            if (p && p.dataIndex != null) {
              var g = p.dataModel || s.getSeriesByIndex(p.seriesIndex);
              return l = g && g.getDataParams(p.dataIndex, p.dataType, u) || {}, !0;
            } else if (p.eventData)
              return l = E({}, p.eventData), !0;
          }, !0), l) {
            var h = l.componentType, c = l.componentIndex;
            (h === "markLine" || h === "markPoint" || h === "markArea") && (h = "series", c = l.seriesIndex);
            var v = h && c != null && s.getComponent(h, c), d = v && r[v.mainType === "series" ? "_chartsMap" : "_componentsMap"][v.__viewId];
            l.event = o, l.type = i, r._$eventProcessor.eventInfo = {
              targetEl: u,
              packedEvent: l,
              model: v,
              view: d
            }, r.trigger(i, l);
          }
        };
        a.zrEventfulCallAtLast = !0, r._zr.on(i, a, r);
      });
      var n = this._messageCenter;
      w(Wf, function(i, a) {
        n.on(a, function(o) {
          r.trigger(a, o);
        });
      }), wD(n, this, this._api);
    }, e.prototype.isDisposed = function() {
      return this._disposed;
    }, e.prototype.clear = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this.setOption({
        series: []
      }, !0);
    }, e.prototype.dispose = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this._disposed = !0;
      var r = this.getDom();
      r && Ty(this.getDom(), dc, "");
      var n = this, i = n._api, a = n._model;
      w(n._componentsViews, function(o) {
        o.dispose(a, i);
      }), w(n._chartsViews, function(o) {
        o.dispose(a, i);
      }), n._zr.dispose(), n._dom = n._model = n._chartsMap = n._componentsMap = n._chartsViews = n._componentsViews = n._scheduler = n._api = n._zr = n._throttledZrFlush = n._theme = n._coordSysMgr = n._messageCenter = null, delete Tn[n.id];
    }, e.prototype.resize = function(r) {
      if (!this[Ce]) {
        if (this._disposed) {
          this.id;
          return;
        }
        this._zr.resize(r);
        var n = this._model;
        if (this._loadingFX && this._loadingFX.resize(), !!n) {
          var i = n.resetOption("media"), a = r && r.silent;
          this[Me] && (a == null && (a = this[Me].silent), i = !0, this[Me] = null), this[Ce] = !0, Wn(this);
          try {
            i && un(this), ir.update.call(this, {
              type: "resize",
              animation: E({
                // Disable animation
                duration: 0
              }, r && r.animation)
            });
          } catch (o) {
            throw this[Ce] = !1, o;
          }
          this[Ce] = !1, Un.call(this, a), Yn.call(this, a);
        }
      }
    }, e.prototype.showLoading = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      if (z(r) && (n = r, r = ""), r = r || "default", this.hideLoading(), !!$f[r]) {
        var i = $f[r](this._api, n), a = this._zr;
        this._loadingFX = i, a.add(i);
      }
    }, e.prototype.hideLoading = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this._loadingFX && this._zr.remove(this._loadingFX), this._loadingFX = null;
    }, e.prototype.makeActionFromEvent = function(r) {
      var n = E({}, r);
      return n.type = Yf[r.type], n;
    }, e.prototype.dispatchAction = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      if (z(n) || (n = {
        silent: !!n
      }), !!xs[r.type] && this._model) {
        if (this[Ce]) {
          this._pendingActions.push(r);
          return;
        }
        var i = n.silent;
        xl.call(this, r, i);
        var a = n.flush;
        a ? this._zr.flush() : a !== !1 && W.browser.weChat && this._throttledZrFlush(), Un.call(this, i), Yn.call(this, i);
      }
    }, e.prototype.updateLabelLayout = function() {
      vt.trigger("series:layoutlabels", this._model, this._api, {
        // Not adding series labels.
        // TODO
        updatedSeries: []
      });
    }, e.prototype.appendData = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      var n = r.seriesIndex, i = this.getModel(), a = i.getSeriesByIndex(n);
      a.appendData(r), this._scheduler.unfinished = !0, this.getZr().wakeUp();
    }, e.internalField = (function() {
      un = function(h) {
        xD(h._model);
        var c = h._scheduler;
        c.restorePipelines(h._zr, h._model), c.prepareStageTasks(), wl(h, !0), wl(h, !1), c.plan();
      }, wl = function(h, c) {
        for (var v = h._model, d = h._scheduler, y = c ? h._componentsViews : h._chartsViews, p = c ? h._componentsMap : h._chartsMap, g = h._zr, m = h._api, _ = 0; _ < y.length; _++)
          y[_].__alive = !1;
        c ? v.eachComponent(function(T, C) {
          T !== "series" && S(C);
        }) : v.eachSeries(S);
        function S(T) {
          var C = T.__requireNewView;
          T.__requireNewView = !1;
          var M = "_ec_" + T.id + "_" + T.type, x = !C && p[M];
          if (!x) {
            var D = Vt(T.type), I = c ? lt.getClass(D.main, D.sub) : (
              // FIXME:TS
              // (ChartView as ChartViewConstructor).getClass('series', classType.sub)
              // For backward compat, still support a chart type declared as only subType
              // like "liquidfill", but recommend "series.liquidfill"
              // But need a base class to make a type series.
              Wt.getClass(D.sub)
            );
            x = new I(), x.init(v, m), p[M] = x, y.push(x), g.add(x.group);
          }
          T.__viewId = x.__id = M, x.__alive = !0, x.__model = T, x.group.__ecComponentInfo = {
            mainType: T.mainType,
            index: T.componentIndex
          }, !c && d.prepareView(x, T, v, m);
        }
        for (var _ = 0; _ < y.length; ) {
          var b = y[_];
          b.__alive ? _++ : (!c && b.renderTask.dispose(), g.remove(b.group), b.dispose(v, m), y.splice(_, 1), p[b.__id] === b && delete p[b.__id], b.__id = b.group.__ecComponentInfo = null);
        }
      }, So = function(h, c, v, d, y) {
        var p = h._model;
        if (p.setUpdatePayload(v), !d) {
          w([].concat(h._componentsViews).concat(h._chartsViews), S);
          return;
        }
        var g = pT(v, d, y), m = v.excludeSeriesId, _;
        m != null && (_ = U(), w($e(m), function(b) {
          var T = Yt(b, null);
          T != null && _.set(T, !0);
        })), p && p.eachComponent(g, function(b) {
          var T = _ && _.get(b.id) != null;
          if (!T)
            if (ad(v))
              if (b instanceof $t)
                v.type === bn && !v.notBlur && !b.get(["emphasis", "disabled"]) && Hw(b, v, h._api);
              else {
                var C = Eh(b.mainType, b.componentIndex, v.name, h._api), M = C.focusSelf, x = C.dispatchers;
                v.type === bn && M && !v.notBlur && Cf(b.mainType, b.componentIndex, h._api), x && w(x, function(D) {
                  v.type === bn ? bf(D) : Tf(D);
                });
              }
            else Mf(v) && b instanceof $t && (Uw(b, v, h._api), nd(b), ht(h));
        }, h), p && p.eachComponent(g, function(b) {
          var T = _ && _.get(b.id) != null;
          T || S(h[d === "series" ? "_chartsMap" : "_componentsMap"][b.__viewId]);
        }, h);
        function S(b) {
          b && b.__alive && b[c] && b[c](b.__model, p, h._api, v);
        }
      }, ir = {
        prepareAndUpdate: function(h) {
          un(this), ir.update.call(this, h, h && {
            // Needs to mark option changed if newOption is given.
            // It's from MagicType.
            // TODO If use a separate flag optionChanged in payload?
            optionChanged: h.newOption != null
          });
        },
        update: function(h, c) {
          var v = this._model, d = this._api, y = this._zr, p = this._coordSysMgr, g = this._scheduler;
          if (v) {
            MD(v), v.setUpdatePayload(h), g.restoreData(v, h), g.performSeriesTasks(v), p.create(v, d), vt.trigger("coordsys:aftercreate", v, d), g.performDataProcessorTasks(v, h), Cl(this, v), p.update(v, d), n(v), g.performVisualTasks(v, h);
            var m = v.get("backgroundColor") || "transparent";
            y.setBackgroundColor(m);
            var _ = v.get("darkMode");
            _ != null && _ !== "auto" && y.setDarkMode(_), Ml(this, v, d, h, c), vt.trigger("afterupdate", v, d);
          }
        },
        /**
         * PENDING: See INCONSISTENCY_OF_BRUSH_SELECTED_EVENT_IN_UPDATE_TRANSFORM
         */
        updateTransform: function(h) {
          var c = this, v = c._model, d = c._api;
          if (v) {
            v.setUpdatePayload(h);
            var y = [];
            v.eachComponent(function(g, m) {
              if (g !== Ny) {
                var _ = c.getViewOfComponentModel(m);
                if (_ && _.__alive)
                  if (_.updateTransform) {
                    var S = _.updateTransform(m, v, d, h);
                    S && S.update && y.push(_);
                  } else
                    y.push(_);
              }
            });
            var p = U();
            v.eachSeries(function(g) {
              var m = c._chartsMap[g.__viewId], _ = g.pipelineContext;
              if (m.updateTransform && !_.progressiveRender) {
                var S = m.updateTransform(g, v, d, h);
                S && S.update && p.set(g.uid, 1);
              } else
                p.set(g.uid, 1);
            }), c._scheduler.performVisualTasks(v, h, {
              setDirty: !0,
              dirtyMap: p
            }), To(c, v, d, h, {}, p), vt.trigger("afterupdate", v, d);
          }
        },
        updateView: function(h) {
          var c = this._model;
          c && (c.setUpdatePayload(h), Wt.markUpdateMethod(h, "updateView"), n(c), this._scheduler.performVisualTasks(c, h, {
            setDirty: !0
          }), Ml(this, c, this._api, h, {}), vt.trigger("afterupdate", c, this._api));
        },
        updateVisual: function(h) {
          var c = this, v = this._model;
          v && (v.setUpdatePayload(h), v.eachSeries(function(d) {
            d.getData().clearAllVisual();
          }), Wt.markUpdateMethod(h, "updateVisual"), n(v), this._scheduler.performVisualTasks(v, h, {
            visualType: "visual",
            setDirty: !0
          }), v.eachComponent(function(d, y) {
            if (d !== "series") {
              var p = c.getViewOfComponentModel(y);
              p && p.__alive && p.updateVisual(y, v, c._api, h);
            }
          }), v.eachSeries(function(d) {
            var y = c._chartsMap[d.__viewId];
            y.updateVisual(d, v, c._api, h);
          }), vt.trigger("afterupdate", v, this._api));
        },
        /**
         * @deprecated
         */
        updateLayout: function(h) {
          ir.update.call(this, h);
        }
      };
      function r(h, c, v, d, y) {
        if (h._disposed) {
          h.id;
          return;
        }
        for (var p = h._model, g = h._coordSysMgr.getCoordinateSystems(), m, _ = Xu(p, v), S = 0; S < g.length; S++) {
          var b = g[S];
          if (b[c] && (m = b[c](p, _, d, y)) != null)
            return m;
        }
      }
      bo = r, Cl = function(h, c) {
        var v = h._chartsMap, d = h._scheduler;
        c.eachSeries(function(y) {
          d.updateStreamModes(y, v[y.__viewId]);
        });
      }, xl = function(h, c) {
        var v = this, d = this.getModel(), y = h.type, p = h.escapeConnect, g = xs[y], m = (g.update || "update").split(":"), _ = m.pop(), S = m[0] != null && Vt(m[0]);
        this[Ce] = !0, Wn(this);
        var b = [h], T = !1;
        h.batch && (T = !0, b = H(h.batch, function(O) {
          return O = _e(E({}, O), h), O.batch = null, O;
        }));
        var C = [], M, x = [], D = g.nonRefinedEventType, I = Mf(h), L = ad(h);
        if (L && Ky(this._api), w(b, function(O) {
          var V = g.action(O, d, v._api);
          if (g.refineEvent ? x.push(V) : M = V, M = M || E({}, O), M.type = D, C.push(M), L) {
            var K = wh(h), J = K.queryOptionMap, ee = K.mainTypeSpecified, Q = ee ? J.keys()[0] : "series";
            So(v, _, O, Q), ht(v);
          } else I ? (So(v, _, O, "series"), ht(v)) : S && So(v, _, O, S.main, S.sub);
        }), _ !== "none" && !L && !I && !S)
          try {
            this[Me] ? (un(this), ir.update.call(this, h), this[Me] = null) : ir[_].call(this, h);
          } catch (O) {
            throw this[Ce] = !1, O;
          }
        if (T ? M = {
          type: D,
          escapeConnect: p,
          batch: C
        } : M = C[0], this[Ce] = !1, !c) {
          var A = void 0;
          if (g.refineEvent) {
            var P = g.refineEvent(x, h, d, this._api).eventContent;
            qt(z(P)), A = _e({
              type: g.refinedEventType
            }, P), A.fromAction = h.type, A.fromActionPayload = h, A.escapeConnect = !0;
          }
          var k = this._messageCenter;
          k.trigger(M.type, M), A && k.trigger(A.type, A);
        }
      }, Un = function(h) {
        for (var c = this._pendingActions; c.length; ) {
          var v = c.shift();
          xl.call(this, v, h);
        }
      }, Yn = function(h) {
        !h && this.trigger("updated");
      }, Sp = function(h, c) {
        h.on("rendered", function(v) {
          c.trigger("rendered", v), // Although zr is dirty if initial animation is not finished
          // and this checking is called on frame, we also check
          // animation finished for robustness.
          h.animation.isFinished() && !c[Me] && !c._scheduler.unfinished && !c._pendingActions.length ? c.trigger("finished") : h.refresh();
        });
      }, bp = function(h, c) {
        h.on("mouseover", function(v) {
          var d = v.target, y = Qi(d, xf);
          y && (Vw(y, v, c._api), ht(c));
        }).on("mouseout", function(v) {
          var d = v.target, y = Qi(d, xf);
          y && (Gw(y, v, c._api), ht(c));
        }).on("click", function(v) {
          var d = v.target, y = Qi(d, function(m) {
            return he(m).dataIndex != null;
          }, !0);
          if (y) {
            var p = y.selected ? "unselect" : "select", g = he(y);
            c._api.dispatchAction({
              type: p,
              dataType: g.dataType,
              dataIndexInside: g.dataIndex,
              seriesIndex: g.seriesIndex,
              isFromClick: !0
            });
          }
        });
      };
      function n(h) {
        h.clearColorPalette(), h.eachSeries(function(c) {
          c.clearColorPalette();
        });
      }
      function i(h) {
        var c = [], v = [], d = !1;
        if (h.eachComponent(function(m, _) {
          var S = _.get("zlevel") || 0, b = _.get("z") || 0, T = _.getZLevelKey();
          d = d || !!T, (m === "series" ? v : c).push({
            zlevel: S,
            z: b,
            idx: _.componentIndex,
            type: m,
            key: T
          });
        }), d) {
          var y = c.concat(v), p, g;
          Oo(y, function(m, _) {
            return m.zlevel === _.zlevel ? m.z - _.z : m.zlevel - _.zlevel;
          }), w(y, function(m) {
            var _ = h.getComponent(m.type, m.idx), S = m.zlevel, b = m.key;
            p != null && (S = Math.max(p, S)), b ? (S === p && b !== g && S++, g = b) : g && (S === p && S++, g = ""), p = S, _.setZLevel(S);
          });
        }
      }
      Ml = function(h, c, v, d, y) {
        i(c), Tp(h, c, v, d, y), w(h._chartsViews, function(p) {
          p.__alive = !1;
        }), To(h, c, v, d, y), w(h._chartsViews, function(p) {
          p.__alive || p.remove(c, v);
        });
      }, Tp = function(h, c, v, d, y, p) {
        w(p || h._componentsViews, function(g) {
          var m = g.__model;
          l(m, g), g.render(m, c, v, d), u(m, g), f(m, g);
        });
      }, To = function(h, c, v, d, y, p) {
        var g = h._scheduler;
        y = E(y || {}, {
          updatedSeries: c.getSeries()
        }), vt.trigger("series:beforeupdate", c, v, y);
        var m = !1;
        c.eachSeries(function(_) {
          var S = h._chartsMap[_.__viewId];
          S.__alive = !0;
          var b = S.renderTask;
          g.updatePayload(b, d), l(_, S), p && p.get(_.uid) && b.dirty(), b.perform(g.getPerformArgs(b)) && (m = !0), S.group.silent = !!_.get("silent"), s(_, S), nd(_);
        }), g.unfinished = m || g.unfinished, vt.trigger("series:layoutlabels", c, v, y), vt.trigger("series:transition", c, v, y), c.eachSeries(function(_) {
          var S = h._chartsMap[_.__viewId];
          u(_, S), f(_, S);
        }), o(h, c), vt.trigger("series:afterupdate", c, v, y);
      }, ht = function(h) {
        h[bl] = !0, h.getZr().wakeUp();
      }, Wn = function(h) {
        h[_o] = (h[_o] + 1) % 1e6;
      }, Cp = function(h) {
        h[bl] && (h.getZr().storage.traverse(function(c) {
          na(c) || a(c);
        }), h[bl] = !1);
      };
      function a(h) {
        for (var c = [], v = h.currentStates, d = 0; d < v.length; d++) {
          var y = v[d];
          y === "emphasis" || y === "blur" || y === "select" || c.push(y);
        }
        h.selected && h.states.select && c.push("select"), h.hoverState === Ph && h.states.emphasis ? c.push("emphasis") : h.hoverState === Ah && h.states.blur && c.push("blur"), h.useStates(c);
      }
      function o(h, c) {
        var v = h._zr;
        if (v.painter.type === "canvas") {
          var d = v.storage, y = 0;
          d.traverse(function(g) {
            g.isGroup || y++;
          });
          var p = y > G(c.get("hoverLayerThreshold"), Wm.hoverLayerThreshold) && !W.node && !W.worker;
          (h._usingTHL || p) && (c.eachSeries(function(g) {
            if (!g.preventUsingHoverLayer) {
              var m = h._chartsMap[g.__viewId];
              m.__alive && m.eachRendered(function(_) {
                var S = _.states.emphasis;
                S && S.hoverLayer !== pm && (S.hoverLayer = p ? dm : vm);
              });
            }
          }), h._usingTHL = p);
        }
      }
      function s(h, c) {
        var v = h.get("blendMode") || null;
        c.eachRendered(function(d) {
          d.isGroup || (d.style.blend = v);
        });
      }
      function u(h, c) {
        if (!h.preventAutoZ) {
          var v = Mm(h);
          c.eachRendered(function(d) {
            return Dm(d, v.z, v.zlevel), !0;
          });
        }
      }
      function l(h, c) {
        c.eachRendered(function(v) {
          if (!na(v)) {
            var d = v.getTextContent(), y = v.getTextGuideLine();
            v.stateTransition && (v.stateTransition = null), d && d.stateTransition && (d.stateTransition = null), y && y.stateTransition && (y.stateTransition = null), v.hasState() ? (v.prevStates = v.currentStates, v.clearStates()) : v.prevStates && (v.prevStates = null);
          }
        });
      }
      function f(h, c) {
        var v = h.getModel("stateAnimation"), d = h.isAnimationEnabled(), y = v.get("duration"), p = y > 0 ? {
          duration: y,
          delay: v.get("delay"),
          easing: v.get("easing")
          // additive: stateAnimationModel.get('additive')
        } : null;
        c.eachRendered(function(g) {
          if (g.states && g.states.emphasis) {
            if (na(g))
              return;
            if (g instanceof ye && Zw(g), g.__dirty) {
              var m = g.prevStates;
              m && g.useStates(m);
            }
            if (d) {
              g.stateTransition = p;
              var _ = g.getTextContent(), S = g.getTextGuideLine();
              _ && (_.stateTransition = p), S && (S.stateTransition = p);
            }
            g.__dirty && a(g);
          }
        });
      }
      wp = function(h) {
        return new /** @class */
        ((function(c) {
          F(v, c);
          function v() {
            return c !== null && c.apply(this, arguments) || this;
          }
          return v.prototype.getCoordinateSystems = function() {
            return h._coordSysMgr.getCoordinateSystems();
          }, v.prototype.getComponentByElement = function(d) {
            for (; d; ) {
              var y = d.__ecComponentInfo;
              if (y != null)
                return h._model.getComponent(y.mainType, y.index);
              d = d.parent;
            }
          }, v.prototype.enterEmphasis = function(d, y) {
            bf(d, y), ht(h);
          }, v.prototype.leaveEmphasis = function(d, y) {
            Tf(d, y), ht(h);
          }, v.prototype.enterBlur = function(d) {
            zw(d), ht(h);
          }, v.prototype.leaveBlur = function(d) {
            Xy(d), ht(h);
          }, v.prototype.enterSelect = function(d) {
            qy(d), ht(h);
          }, v.prototype.leaveSelect = function(d) {
            $y(d), ht(h);
          }, v.prototype.getModel = function() {
            return h.getModel();
          }, v.prototype.getViewOfComponentModel = function(d) {
            return h.getViewOfComponentModel(d);
          }, v.prototype.getViewOfSeriesModel = function(d) {
            return h.getViewOfSeriesModel(d);
          }, v.prototype.getECUpdateCycleVersion = function() {
            return h[_o];
          }, v.prototype.usingTHL = function() {
            return h._usingTHL;
          }, v;
        })(Hy))(h);
      }, V_ = function(h) {
        function c(v, d) {
          for (var y = 0; y < v.length; y++) {
            var p = v[y];
            p[Tl] = d;
          }
        }
        w(Yf, function(v, d) {
          h._messageCenter.on(d, function(y) {
            if (Ds[h.group] && h[Tl] !== _p) {
              if (y && y.escapeConnect)
                return;
              var p = h.makeActionFromEvent(y), g = [];
              w(Tn, function(m) {
                m !== h && m.group === h.group && g.push(m);
              }), c(g, _p), w(g, function(m) {
                m[Tl] !== gI && m.dispatchAction(p);
              }), c(g, yI);
            }
          });
        });
      };
    })(), e;
  })(Kt)
), vc = Cs.prototype;
vc.on = B_("on");
vc.off = B_("off");
vc.one = function(t, e, r) {
  var n = this;
  function i() {
    for (var a = [], o = 0; o < arguments.length; o++)
      a[o] = arguments[o];
    e && e.apply && e.apply(this, a), n.off(t, i);
  }
  this.on.call(this, t, i, r);
};
var mI = ["click", "dblclick", "mouseover", "mouseout", "mousemove", "mousedown", "mouseup", "globalout", "contextmenu"];
var xs = {}, Yf = {}, Wf = {}, Xf = [], qf = [], Ms = [], G_ = {}, $f = {}, Tn = {}, Ds = {}, _I = +/* @__PURE__ */ new Date() - 0, SI = +/* @__PURE__ */ new Date() - 0, dc = "_echarts_instance_";
function bI(t, e, r) {
  var n = !(r && r.ssr);
  if (n) {
    var i = pc(t);
    if (i)
      return i;
  }
  var a = new Cs(t, e, r);
  return a.id = "ec_" + _I++, Tn[a.id] = a, n && Ty(t, dc, a.id), V_(a), vt.trigger("afterinit", a), a;
}
function TI(t) {
  if (B(t)) {
    var e = t;
    t = null, w(e, function(r) {
      r.group != null && (t = r.group);
    }), t = t || "g_" + SI++, w(e, function(r) {
      r.group = t;
    });
  }
  return Ds[t] = !0, t;
}
function U_(t) {
  Ds[t] = !1;
}
var wI = U_;
function CI(t) {
  N(t) ? t = Tn[t] : t instanceof Cs || (t = pc(t)), t instanceof Cs && !t.isDisposed() && t.dispose();
}
function pc(t) {
  return Tn[gT(t, dc)];
}
function xI(t) {
  return Tn[t];
}
function gc(t, e) {
  G_[t] = e;
}
function yc(t) {
  oe(qf, t) < 0 && qf.push(t);
}
function mc(t, e) {
  _c(Xf, t, e, fI);
}
function Y_(t) {
  pu("afterinit", t);
}
function W_(t) {
  pu("afterupdate", t);
}
function pu(t, e) {
  vt.on(t, e);
}
function Ln(t, e, r) {
  var n, i, a, o, s;
  X(e) && (r = e, e = ""), z(t) ? (n = t.type, i = t.event, o = t.update, s = t.publishNonRefinedEvent, r || (r = t.action), a = t.refineEvent) : (n = t, i = e);
  function u(f) {
    return f.toLowerCase();
  }
  i = u(i || n);
  var l = a ? u(n) : i;
  xs[n] || (qt(mp.test(n) && mp.test(i)), a && qt(i !== n), xs[n] = {
    actionType: n,
    refinedEventType: i,
    nonRefinedEventType: l,
    update: o,
    action: r,
    refineEvent: a
  }, Wf[i] = 1, a && s && (Wf[l] = 1), Yf[l] = n);
}
function X_(t, e) {
  fu.register(t, e);
}
function MI(t) {
  var e = fu.get(t);
  if (e)
    return e.getDimensionsInfo ? e.getDimensionsInfo() : e.dimensions.slice();
}
function DI(t, e) {
}
function q_(t, e) {
  _c(Ms, t, e, R_, "layout");
}
function Fr(t, e) {
  _c(Ms, t, e, E_, "visual");
}
var xp = [];
function _c(t, e, r, n, i, a) {
  if ((X(e) || z(e)) && (r = e, e = n), !(oe(xp, r) >= 0)) {
    xp.push(r);
    var o = __.wrapStageHandler(r, i);
    o.__prio = e, o.__raw = r, t.push(o);
  }
}
function Sc(t, e) {
  $f[t] = e;
}
function II(t) {
  Dg({
    createCanvas: t
  });
}
function $_(t, e, r) {
  var n = x_("registerMap");
  n && n(t, e, r);
}
function AI(t) {
  var e = x_("getMap");
  return e && e(t);
}
var Z_ = AM;
Fr(cc, aD);
Fr(du, oD);
Fr(du, sD);
Fr(cc, _D);
Fr(du, SD);
Fr(O_, tI);
yc(Km);
mc(sI, aM);
Sc("default", uD);
Ln({
  type: bn,
  event: bn,
  update: bn
}, Re);
Ln({
  type: Go,
  event: Go,
  update: Go
}, Re);
Ln({
  type: hs,
  event: Lh,
  update: hs,
  action: Re,
  refineEvent: bc,
  publishNonRefinedEvent: !0
});
Ln({
  type: Sf,
  event: Lh,
  update: Sf,
  action: Re,
  refineEvent: bc,
  publishNonRefinedEvent: !0
});
Ln({
  type: cs,
  event: Lh,
  update: cs,
  action: Re,
  refineEvent: bc,
  publishNonRefinedEvent: !0
});
function bc(t, e, r, n) {
  return {
    eventContent: {
      selected: Yw(r),
      isFromClick: e.isFromClick || !1
    }
  };
}
gc("default", {});
gc("dark", w_);
var PI = {};
function Ni(t) {
  return t == null ? 0 : t.length || 1;
}
function Mp(t) {
  return t;
}
var LI = (
  /** @class */
  (function() {
    function t(e, r, n, i, a, o) {
      this._old = e, this._new = r, this._oldKeyGetter = n || Mp, this._newKeyGetter = i || Mp, this.context = a, this._diffModeMultiple = o === "multiple";
    }
    return t.prototype.add = function(e) {
      return this._add = e, this;
    }, t.prototype.update = function(e) {
      return this._update = e, this;
    }, t.prototype.updateManyToOne = function(e) {
      return this._updateManyToOne = e, this;
    }, t.prototype.updateOneToMany = function(e) {
      return this._updateOneToMany = e, this;
    }, t.prototype.updateManyToMany = function(e) {
      return this._updateManyToMany = e, this;
    }, t.prototype.remove = function(e) {
      return this._remove = e, this;
    }, t.prototype.execute = function() {
      this[this._diffModeMultiple ? "_executeMultiple" : "_executeOneToOne"]();
    }, t.prototype._executeOneToOne = function() {
      var e = this._old, r = this._new, n = {}, i = new Array(e.length), a = new Array(r.length);
      this._initIndexMap(e, null, i, "_oldKeyGetter"), this._initIndexMap(r, n, a, "_newKeyGetter");
      for (var o = 0; o < e.length; o++) {
        var s = i[o], u = n[s], l = Ni(u);
        if (l > 1) {
          var f = u.shift();
          u.length === 1 && (n[s] = u[0]), this._update && this._update(f, o);
        } else l === 1 ? (n[s] = null, this._update && this._update(u, o)) : this._remove && this._remove(o);
      }
      this._performRestAdd(a, n);
    }, t.prototype._executeMultiple = function() {
      var e = this._old, r = this._new, n = {}, i = {}, a = [], o = [];
      this._initIndexMap(e, n, a, "_oldKeyGetter"), this._initIndexMap(r, i, o, "_newKeyGetter");
      for (var s = 0; s < a.length; s++) {
        var u = a[s], l = n[u], f = i[u], h = Ni(l), c = Ni(f);
        if (h > 1 && c === 1)
          this._updateManyToOne && this._updateManyToOne(f, l), i[u] = null;
        else if (h === 1 && c > 1)
          this._updateOneToMany && this._updateOneToMany(f, l), i[u] = null;
        else if (h === 1 && c === 1)
          this._update && this._update(f, l), i[u] = null;
        else if (h > 1 && c > 1)
          this._updateManyToMany && this._updateManyToMany(f, l), i[u] = null;
        else if (h > 1)
          for (var v = 0; v < h; v++)
            this._remove && this._remove(l[v]);
        else
          this._remove && this._remove(l);
      }
      this._performRestAdd(o, i);
    }, t.prototype._performRestAdd = function(e, r) {
      for (var n = 0; n < e.length; n++) {
        var i = e[n], a = r[i], o = Ni(a);
        if (o > 1)
          for (var s = 0; s < o; s++)
            this._add && this._add(a[s]);
        else o === 1 && this._add && this._add(a);
        r[i] = null;
      }
    }, t.prototype._initIndexMap = function(e, r, n, i) {
      for (var a = this._diffModeMultiple, o = 0; o < e.length; o++) {
        var s = "_ec_" + this[i](e[o], o);
        if (a || (n[o] = s), !!r) {
          var u = r[s], l = Ni(u);
          l === 0 ? (r[s] = o, a && n.push(s)) : l === 1 ? r[s] = [u, o] : u.push(o);
        }
      }
    }, t;
  })()
), RI = (
  /** @class */
  (function() {
    function t(e, r) {
      this._encode = e, this._schema = r;
    }
    return t.prototype.get = function() {
      return {
        // Do not generate full dimension name until fist used.
        fullDimensions: this._getFullDimensionNames(),
        encode: this._encode
      };
    }, t.prototype._getFullDimensionNames = function() {
      return this._cachedDimNames || (this._cachedDimNames = this._schema ? this._schema.makeOutputDimensionNames() : []), this._cachedDimNames;
    }, t;
  })()
);
function EI(t, e) {
  var r = {}, n = r.encode = {}, i = U(), a = [], o = [], s = {};
  w(t.dimensions, function(c) {
    var v = t.getDimensionInfo(c), d = v.coordDim;
    if (d) {
      var y = v.coordDimIndex;
      Dl(n, d)[y] = c, v.isExtraCoord || (i.set(d, 1), kI(v.type) && (a[0] = c), Dl(s, d)[y] = t.getDimensionIndex(v.name)), v.defaultTooltip && o.push(c);
    }
    Fy.each(function(p, g) {
      var m = Dl(n, g), _ = v.otherDims[g];
      _ != null && _ !== !1 && (m[_] = v.name);
    });
  });
  var u = [], l = {};
  i.each(function(c, v) {
    var d = n[v];
    l[v] = d[0], u = u.concat(d);
  }), r.dataDimsOnCoord = u, r.dataDimIndicesOnCoord = H(u, function(c) {
    return t.getDimensionInfo(c).storeDimIndex;
  }), r.encodeFirstDimNotExtra = l;
  var f = n.label;
  f && f.length && (a = f.slice());
  var h = n.tooltip;
  return h && h.length ? o = h.slice() : o.length || (o = a.slice()), n.defaultedLabel = a, n.defaultedTooltip = o, r.userOutput = new RI(s, e), r;
}
function Dl(t, e) {
  return t.hasOwnProperty(e) || (t[e] = []), t[e];
}
function OI(t) {
  return t === "category" ? "ordinal" : t === "time" ? "time" : "float";
}
function kI(t) {
  return !(t === "ordinal" || t === "time");
}
var Ko = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.otherDims = {}, e != null && E(this, e);
    }
    return t;
  })()
), BI = de(), NI = {
  float: "f",
  int: "i",
  ordinal: "o",
  number: "n",
  time: "t"
}, K_ = (
  /** @class */
  (function() {
    function t(e) {
      this.dimensions = e.dimensions, this._dimOmitted = e.dimensionOmitted, this.source = e.source, this._fullDimCount = e.fullDimensionCount, this._updateDimOmitted(e.dimensionOmitted);
    }
    return t.prototype.isDimensionOmitted = function() {
      return this._dimOmitted;
    }, t.prototype._updateDimOmitted = function(e) {
      this._dimOmitted = e, e && (this._dimNameMap || (this._dimNameMap = J_(this.source)));
    }, t.prototype.getSourceDimensionIndex = function(e) {
      return G(this._dimNameMap.get(e), -1);
    }, t.prototype.getSourceDimension = function(e) {
      var r = this.source.dimensionsDefine;
      if (r)
        return r[e];
    }, t.prototype.makeStoreSchema = function() {
      for (var e = this._fullDimCount, r = Jm(this.source), n = !e0(e), i = "", a = [], o = 0, s = 0; o < e; o++) {
        var u = void 0, l = void 0, f = void 0, h = this.dimensions[s];
        if (h && h.storeDimIndex === o)
          u = r ? h.name : null, l = h.type, f = h.ordinalMeta, s++;
        else {
          var c = this.getSourceDimension(o);
          c && (u = r ? c.name : null, l = c.type);
        }
        a.push({
          property: u,
          type: l,
          ordinalMeta: f
        }), r && u != null && (!h || !h.isCalculationCoord) && (i += n ? u.replace(/\`/g, "`1").replace(/\$/g, "`2") : u), i += "$", i += NI[l] || "f", f && (i += f.uid), i += "$";
      }
      var v = this.source, d = [v.seriesLayoutBy, v.startIndex, i].join("$$");
      return {
        dimensions: a,
        hash: d
      };
    }, t.prototype.makeOutputDimensionNames = function() {
      for (var e = [], r = 0, n = 0; r < this._fullDimCount; r++) {
        var i = void 0, a = this.dimensions[n];
        if (a && a.storeDimIndex === r)
          a.isCalculationCoord || (i = a.name), n++;
        else {
          var o = this.getSourceDimension(r);
          o && (i = o.name);
        }
        e.push(i);
      }
      return e;
    }, t.prototype.appendCalculationDimension = function(e) {
      this.dimensions.push(e), e.isCalculationCoord = !0, this._fullDimCount++, this._updateDimOmitted(!0);
    }, t;
  })()
);
function j_(t) {
  return t instanceof K_;
}
function Q_(t) {
  for (var e = U(), r = 0; r < (t || []).length; r++) {
    var n = t[r], i = z(n) ? n.name : n;
    i != null && e.get(i) == null && e.set(i, r);
  }
  return e;
}
function J_(t) {
  var e = BI(t);
  return e.dimNameMap || (e.dimNameMap = Q_(t.dimensionsDefine));
}
function e0(t) {
  return t > 30;
}
var Fi = z, br = H, FI = typeof Int32Array > "u" ? Array : Int32Array, zI = "e\0\0", Dp = -1, HI = ["hasItemOption", "_nameList", "_idList", "_invertedIndicesMap", "_dimSummary", "userOutput", "_rawData", "_dimValueGetter", "_nameDimIdx", "_idDimIdx", "_nameRepeatCount"], VI = ["_approximateExtent"], Ip, wo, zi, Hi, Il, Vi, Al, t0 = (
  /** @class */
  (function() {
    function t(e, r) {
      this.type = "list", this._dimOmitted = !1, this._nameList = [], this._idList = [], this._visual = {}, this._layout = {}, this._itemVisuals = [], this._itemLayouts = [], this._graphicEls = [], this._approximateExtent = {}, this._calculationInfo = {}, this.hasItemOption = !1, this.TRANSFERABLE_METHODS = ["cloneShallow", "downSample", "minmaxDownSample", "lttbDownSample", "map"], this.CHANGABLE_METHODS = ["filterSelf", "selectRange"], this.DOWNSAMPLE_METHODS = ["downSample", "minmaxDownSample", "lttbDownSample"];
      var n, i = !1;
      j_(e) ? (n = e.dimensions, this._dimOmitted = e.isDimensionOmitted(), this._schema = e) : (i = !0, n = e), n = n || ["x", "y"];
      for (var a = {}, o = [], s = {}, u = !1, l = {}, f = 0; f < n.length; f++) {
        var h = n[f], c = N(h) ? new Ko({
          name: h
        }) : h instanceof Ko ? h : new Ko(h), v = c.name;
        c.type = c.type || "float", c.coordDim || (c.coordDim = v, c.coordDimIndex = 0);
        var d = c.otherDims = c.otherDims || {};
        o.push(v), a[v] = c, l[v] != null && (u = !0), c.createInvertedIndices && (s[v] = []), i && (c.storeDimIndex = f), d.itemName === 0 && (this._nameDimIdx = c.storeDimIndex), d.itemId === 0 && (this._idDimIdx = c.storeDimIndex);
      }
      if (this.dimensions = o, this._dimInfos = a, this._initGetDimensionInfo(u), this.hostModel = r, this._invertedIndicesMap = s, this._dimOmitted) {
        var y = this._dimIdxToName = U();
        w(o, function(p) {
          y.set(a[p].storeDimIndex, p);
        });
      }
    }
    return t.prototype.getDimension = function(e) {
      var r = this._recognizeDimIndex(e);
      if (r == null)
        return e;
      if (r = e, !this._dimOmitted)
        return this.dimensions[r];
      var n = this._dimIdxToName.get(r);
      if (n != null)
        return n;
      var i = this._schema.getSourceDimension(r);
      if (i)
        return i.name;
    }, t.prototype.getDimensionIndex = function(e) {
      var r = this._recognizeDimIndex(e);
      if (r != null)
        return r;
      if (e == null)
        return -1;
      var n = this._getDimInfo(e);
      return n ? n.storeDimIndex : this._dimOmitted ? this._schema.getSourceDimensionIndex(e) : -1;
    }, t.prototype._recognizeDimIndex = function(e) {
      if (ve(e) || e != null && !isNaN(e) && !this._getDimInfo(e) && (!this._dimOmitted || this._schema.getSourceDimensionIndex(e) < 0))
        return +e;
    }, t.prototype._getStoreDimIndex = function(e) {
      var r = this.getDimensionIndex(e);
      return r;
    }, t.prototype.getDimensionInfo = function(e) {
      return this._getDimInfo(this.getDimension(e));
    }, t.prototype._initGetDimensionInfo = function(e) {
      var r = this._dimInfos;
      this._getDimInfo = e ? function(n) {
        return r.hasOwnProperty(n) ? r[n] : void 0;
      } : function(n) {
        return r[n];
      };
    }, t.prototype.getDimensionsOnCoord = function() {
      return this._dimSummary.dataDimsOnCoord.slice();
    }, t.prototype.mapDimension = function(e, r) {
      var n = this._dimSummary;
      if (r == null)
        return n.encodeFirstDimNotExtra[e];
      var i = n.encode[e];
      return i ? i[r] : null;
    }, t.prototype.mapDimensionsAll = function(e) {
      var r = this._dimSummary, n = r.encode[e];
      return (n || []).slice();
    }, t.prototype.getStore = function() {
      return this._store;
    }, t.prototype.initData = function(e, r, n) {
      var i = this, a;
      if (e instanceof Of && (a = e), !a) {
        var o = this.dimensions, s = sc(e) || Ze(e) ? new e_(e, o.length) : e;
        a = new Of();
        var u = br(o, function(l) {
          return {
            type: i._dimInfos[l].type,
            property: l
          };
        });
        a.initData(s, u, n);
      }
      this._store = a, this._nameList = (r || []).slice(), this._idList = [], this._nameRepeatCount = {}, this._doInit(0, a.count()), this._dimSummary = EI(this, this._schema), this.userOutput = this._dimSummary.userOutput;
    }, t.prototype.appendData = function(e) {
      var r = this._store.appendData(e);
      this._doInit(r[0], r[1]);
    }, t.prototype.appendValues = function(e, r) {
      var n = this._store.appendValues(e, r && r.length), i = n.start, a = n.end, o = this._shouldMakeIdFromName();
      if (this._updateOrdinalMeta(), r)
        for (var s = i; s < a; s++) {
          var u = s - i;
          this._nameList[s] = r[u], o && Al(this, s);
        }
    }, t.prototype._updateOrdinalMeta = function() {
      for (var e = this._store, r = this.dimensions, n = 0; n < r.length; n++) {
        var i = this._dimInfos[r[n]];
        i.ordinalMeta && e.collectOrdinalMeta(i.storeDimIndex, i.ordinalMeta);
      }
    }, t.prototype._shouldMakeIdFromName = function() {
      var e = this._store.getProvider();
      return this._idDimIdx == null && e.getSource().sourceFormat !== Lr && !e.fillStorage;
    }, t.prototype._doInit = function(e, r) {
      if (!(e >= r)) {
        var n = this._store, i = n.getProvider();
        this._updateOrdinalMeta();
        var a = this._nameList, o = this._idList, s = i.getSource().sourceFormat, u = s === ft;
        if (u && !i.pure)
          for (var l = [], f = e; f < r; f++) {
            var h = i.getItem(f, l);
            if (!this.hasItemOption && iT(h) && (this.hasItemOption = !0), h) {
              var c = h.name;
              a[f] == null && c != null && (a[f] = Yt(c, null));
              var v = h.id;
              o[f] == null && v != null && (o[f] = Yt(v, null));
            }
          }
        if (this._shouldMakeIdFromName())
          for (var f = e; f < r; f++)
            Al(this, f);
        Ip(this);
      }
    }, t.prototype.getApproximateExtent = function(e, r) {
      return this._approximateExtent[e] || this._store.getDataExtent(this._getStoreDimIndex(e), r);
    }, t.prototype.setApproximateExtent = function(e, r) {
      r = this.getDimension(r), this._approximateExtent[r] = e.slice();
    }, t.prototype.getCalculationInfo = function(e) {
      return this._calculationInfo[e];
    }, t.prototype.setCalculationInfo = function(e, r) {
      Fi(e) ? E(this._calculationInfo, e) : this._calculationInfo[e] = r;
    }, t.prototype.getName = function(e) {
      var r = this.getRawIndex(e), n = this._nameList[r];
      return n == null && this._nameDimIdx != null && (n = zi(this, this._nameDimIdx, r)), n == null && (n = ""), n;
    }, t.prototype._getCategory = function(e, r) {
      var n = this._store.get(e, r), i = this._store.getOrdinalMeta(e);
      return i ? i.categories[n] : n;
    }, t.prototype.getId = function(e) {
      return wo(this, this.getRawIndex(e));
    }, t.prototype.count = function() {
      return this._store.count();
    }, t.prototype.get = function(e, r) {
      var n = this._store, i = this._dimInfos[e];
      if (i)
        return n.get(i.storeDimIndex, r);
    }, t.prototype.getByRawIndex = function(e, r) {
      var n = this._store, i = this._dimInfos[e];
      if (i)
        return n.getByRawIndex(i.storeDimIndex, r);
    }, t.prototype.getIndices = function() {
      return this._store.getIndices();
    }, t.prototype.getDataExtent = function(e) {
      return this._store.getDataExtent(this._getStoreDimIndex(e), null);
    }, t.prototype.getSum = function(e) {
      return this._store.getSum(this._getStoreDimIndex(e));
    }, t.prototype.getMedian = function(e) {
      return this._store.getMedian(this._getStoreDimIndex(e));
    }, t.prototype.getValues = function(e, r) {
      var n = this, i = this._store;
      return B(e) ? i.getValues(br(e, function(a) {
        return n._getStoreDimIndex(a);
      }), r) : i.getValues(e);
    }, t.prototype.hasValue = function(e) {
      for (var r = this._dimSummary.dataDimIndicesOnCoord, n = 0, i = r.length; n < i; n++)
        if (isNaN(this._store.get(r[n], e)))
          return !1;
      return !0;
    }, t.prototype.indexOfName = function(e) {
      for (var r = 0, n = this._store.count(); r < n; r++)
        if (this.getName(r) === e)
          return r;
      return -1;
    }, t.prototype.getRawIndex = function(e) {
      return this._store.getRawIndex(e);
    }, t.prototype.indexOfRawIndex = function(e) {
      return this._store.indexOfRawIndex(e);
    }, t.prototype.rawIndexOf = function(e, r) {
      var n = e && this._invertedIndicesMap[e], i = n && n[r];
      return i == null || isNaN(i) ? Dp : i;
    }, t.prototype.each = function(e, r, n) {
      X(e) && (n = r, r = e, e = []);
      var i = n || this, a = br(Hi(e), this._getStoreDimIndex, this);
      this._store.each(a, i ? le(r, i) : r);
    }, t.prototype.filterSelf = function(e, r, n) {
      X(e) && (n = r, r = e, e = []);
      var i = n || this, a = br(Hi(e), this._getStoreDimIndex, this);
      return this._store = this._store.filter(a, i ? le(r, i) : r), this;
    }, t.prototype.selectRange = function(e) {
      var r = this, n = {}, i = me(e);
      return w(i, function(a) {
        var o = r._getStoreDimIndex(a);
        n[o] = e[a];
      }), this._store = this._store.selectRange(n), this;
    }, t.prototype.mapArray = function(e, r, n) {
      X(e) && (n = r, r = e, e = []), n = n || this;
      var i = [];
      return this.each(e, function() {
        i.push(r && r.apply(this, arguments));
      }, n), i;
    }, t.prototype.map = function(e, r, n, i) {
      var a = n || i || this, o = br(Hi(e), this._getStoreDimIndex, this), s = Vi(this);
      return s._store = this._store.map(o, a ? le(r, a) : r), s;
    }, t.prototype.modify = function(e, r, n, i) {
      var a = n || i || this, o = br(Hi(e), this._getStoreDimIndex, this);
      this._store.modify(o, a ? le(r, a) : r);
    }, t.prototype.downSample = function(e, r, n, i) {
      var a = Vi(this);
      return a._store = this._store.downSample(this._getStoreDimIndex(e), r, n, i), a;
    }, t.prototype.minmaxDownSample = function(e, r) {
      var n = Vi(this);
      return n._store = this._store.minmaxDownSample(this._getStoreDimIndex(e), r), n;
    }, t.prototype.lttbDownSample = function(e, r) {
      var n = Vi(this);
      return n._store = this._store.lttbDownSample(this._getStoreDimIndex(e), r), n;
    }, t.prototype.getRawDataItem = function(e) {
      return this._store.getRawDataItem(e);
    }, t.prototype.getItemModel = function(e) {
      var r = this.hostModel, n = this.getRawDataItem(e);
      return new ce(n, r, r && r.ecModel);
    }, t.prototype.diff = function(e) {
      var r = this;
      return new LI(e ? e.getStore().getIndices() : [], this.getStore().getIndices(), function(n) {
        return wo(e, n);
      }, function(n) {
        return wo(r, n);
      });
    }, t.prototype.getVisual = function(e) {
      var r = this._visual;
      return r && r[e];
    }, t.prototype.setVisual = function(e, r) {
      this._visual = this._visual || {}, Fi(e) ? E(this._visual, e) : this._visual[e] = r;
    }, t.prototype.getItemVisual = function(e, r) {
      var n = this._itemVisuals[e], i = n && n[r];
      return i ?? this.getVisual(r);
    }, t.prototype.hasItemVisual = function() {
      return this._itemVisuals.length > 0;
    }, t.prototype.ensureUniqueItemVisual = function(e, r) {
      var n = this._itemVisuals, i = n[e];
      i || (i = n[e] = {});
      var a = i[r];
      return a == null && (a = this.getVisual(r), B(a) ? a = a.slice() : Fi(a) && (a = E({}, a)), i[r] = a), a;
    }, t.prototype.setItemVisual = function(e, r, n) {
      var i = this._itemVisuals[e] || {};
      this._itemVisuals[e] = i, Fi(r) ? E(i, r) : i[r] = n;
    }, t.prototype.clearAllVisual = function() {
      this._visual = {}, this._itemVisuals = [];
    }, t.prototype.setLayout = function(e, r) {
      Fi(e) ? E(this._layout, e) : this._layout[e] = r;
    }, t.prototype.getLayout = function(e) {
      return this._layout[e];
    }, t.prototype.getItemLayout = function(e) {
      return this._itemLayouts[e];
    }, t.prototype.setItemLayout = function(e, r, n) {
      this._itemLayouts[e] = n ? E(this._itemLayouts[e] || {}, r) : r;
    }, t.prototype.clearItemLayouts = function() {
      this._itemLayouts.length = 0;
    }, t.prototype.setItemGraphicEl = function(e, r) {
      var n = this.hostModel && this.hostModel.seriesIndex;
      Dw(n, this.dataType, e, r), this._graphicEls[e] = r;
    }, t.prototype.getItemGraphicEl = function(e) {
      return this._graphicEls[e];
    }, t.prototype.eachItemGraphicEl = function(e, r) {
      w(this._graphicEls, function(n, i) {
        n && e && e.call(r, n, i);
      });
    }, t.prototype.cloneShallow = function(e) {
      return e || (e = new t(this._schema ? this._schema : br(this.dimensions, this._getDimInfo, this), this.hostModel)), Il(e, this), e._store = this._store, e;
    }, t.prototype.wrapMethod = function(e, r) {
      var n = this[e];
      X(n) && (this.__wrappedMethods = this.__wrappedMethods || [], this.__wrappedMethods.push(e), this[e] = function() {
        var i = n.apply(this, arguments);
        return r.apply(this, [i].concat(zs(arguments)));
      });
    }, t.internalField = (function() {
      Ip = function(e) {
        var r = e._invertedIndicesMap;
        w(r, function(n, i) {
          var a = e._dimInfos[i], o = a.ordinalMeta, s = e._store;
          if (o) {
            n = r[i] = new FI(o.categories.length);
            for (var u = 0; u < n.length; u++)
              n[u] = Dp;
            for (var u = 0; u < s.count(); u++)
              n[s.get(a.storeDimIndex, u)] = u;
          }
        });
      }, zi = function(e, r, n) {
        return Yt(e._getCategory(r, n), null);
      }, wo = function(e, r) {
        var n = e._idList[r];
        return n == null && e._idDimIdx != null && (n = zi(e, e._idDimIdx, r)), n == null && (n = zI + r), n;
      }, Hi = function(e) {
        return B(e) || (e = e != null ? [e] : []), e;
      }, Vi = function(e) {
        var r = new t(e._schema ? e._schema : br(e.dimensions, e._getDimInfo, e), e.hostModel);
        return Il(r, e), r;
      }, Il = function(e, r) {
        w(HI.concat(r.__wrappedMethods || []), function(n) {
          r.hasOwnProperty(n) && (e[n] = r[n]);
        }), e.__wrappedMethods = r.__wrappedMethods, w(VI, function(n) {
          e[n] = Z(r[n]);
        }), e._calculationInfo = E({}, r._calculationInfo);
      }, Al = function(e, r) {
        var n = e._nameList, i = e._idList, a = e._nameDimIdx, o = e._idDimIdx, s = n[r], u = i[r];
        if (s == null && a != null && (n[r] = s = zi(e, a, r)), u == null && o != null && (i[r] = u = zi(e, o, r)), u == null && s != null) {
          var l = e._nameRepeatCount, f = l[s] = (l[s] || 0) + 1;
          u = s, f > 1 && (u += "__ec__" + f), i[r] = u;
        }
      };
    })(), t;
  })()
);
function GI(t, e) {
  return r0(t, e).dimensions;
}
function r0(t, e) {
  sc(t) || (t = jm(t)), e = e || {};
  var r = e.coordDimensions || [], n = e.dimensionsDefine || t.dimensionsDefine || [], i = U(), a = [], o = UI(t, r, n, e.dimensionsCount), s = e.canOmitUnusedDimensions && e0(o), u = n === t.dimensionsDefine, l = u ? J_(t) : Q_(n), f = e.encodeDefine;
  !f && e.encodeDefaulter && (f = e.encodeDefaulter(t, o));
  for (var h = U(f), c = new o_(o), v = 0; v < c.length; v++)
    c[v] = -1;
  function d(x) {
    var D = c[x];
    if (D < 0) {
      var I = n[x], L = z(I) ? I : {
        name: I
      }, A = new Ko(), P = L.name;
      P != null && l.get(P) != null && (A.name = A.displayName = P), L.type != null && (A.type = L.type), L.displayName != null && (A.displayName = L.displayName);
      var k = a.length;
      return c[x] = k, A.storeDimIndex = x, a.push(A), A;
    }
    return a[D];
  }
  if (!s)
    for (var v = 0; v < o; v++)
      d(v);
  h.each(function(x, D) {
    var I = $e(x).slice();
    if (I.length === 1 && !N(I[0]) && I[0] < 0) {
      h.set(D, !1);
      return;
    }
    var L = h.set(D, []);
    w(I, function(A, P) {
      var k = N(A) ? l.get(A) : A;
      k != null && k < o && (L[P] = k, p(d(k), D, P));
    });
  });
  var y = 0;
  w(r, function(x) {
    var D, I, L, A;
    if (N(x))
      D = x, A = {};
    else {
      A = x, D = A.name;
      var P = A.ordinalMeta;
      A.ordinalMeta = null, A = E({}, A), A.ordinalMeta = P, I = A.dimsDef, L = A.otherDims, A.name = A.coordDim = A.coordDimIndex = A.dimsDef = A.otherDims = null;
    }
    var k = h.get(D);
    if (k !== !1) {
      if (k = $e(k), !k.length)
        for (var O = 0; O < (I && I.length || 1); O++) {
          for (; y < o && d(y).coordDim != null; )
            y++;
          y < o && k.push(y++);
        }
      w(k, function(V, K) {
        var J = d(V);
        if (u && A.type != null && (J.type = A.type), p(_e(J, A), D, K), J.name == null && I) {
          var ee = I[K];
          !z(ee) && (ee = {
            name: ee
          }), J.name = J.displayName = ee.name, J.defaultTooltip = ee.defaultTooltip;
        }
        L && _e(J.otherDims, L);
      });
    }
  });
  function p(x, D, I) {
    Fy.get(D) != null ? x.otherDims[D] = I : (x.coordDim = D, x.coordDimIndex = I, i.set(D, !0));
  }
  var g = e.generateCoord, m = e.generateCoordCount, _ = m != null;
  m = g ? m || 1 : 0;
  var S = g || "value";
  function b(x) {
    x.name == null && (x.name = x.coordDim);
  }
  if (s)
    w(a, function(x) {
      b(x);
    }), a.sort(function(x, D) {
      return x.storeDimIndex - D.storeDimIndex;
    });
  else
    for (var T = 0; T < o; T++) {
      var C = d(T), M = C.coordDim;
      M == null && (C.coordDim = YI(S, i, _), C.coordDimIndex = 0, (!g || m <= 0) && (C.isExtraCoord = !0), m--), b(C), C.type == null && ($m(t, T) === nt.Must || C.isExtraCoord && (C.otherDims.itemName != null || C.otherDims.seriesName != null)) && (C.type = "ordinal");
    }
  return Ch(a, function(x) {
    return x.name;
  }, function(x, D) {
    D > 0 && (x.name = x.name + (D - 1));
  }), new K_({
    source: t,
    dimensions: a,
    fullDimensionCount: o,
    dimensionOmitted: s
  });
}
function UI(t, e, r, n) {
  var i = Math.max(t.dimensionsDetectedCount || 1, e.length, r.length, n || 0);
  return w(e, function(a) {
    var o;
    z(a) && (o = a.dimsDef) && (i = Math.max(i, o.length));
  }), i;
}
function YI(t, e, r) {
  if (r || e.hasKey(t)) {
    for (var n = 0; e.hasKey(t + n); )
      n++;
    t += n;
  }
  return e.set(t, !0), t;
}
var WI = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.coordSysDims = [], this.axisMap = U(), this.categoryAxisMap = U(), this.coordSysName = e;
    }
    return t;
  })()
);
function XI(t) {
  var e = t.get("coordinateSystem"), r = new WI(e), n = qI[e];
  if (n)
    return n(t, r, r.axisMap, r.categoryAxisMap), r;
}
var qI = {
  cartesian2d: function(t, e, r, n) {
    var i = t.getReferringComponents("xAxis", ur).models[0], a = t.getReferringComponents("yAxis", ur).models[0];
    e.coordSysDims = ["x", "y"], r.set("x", i), r.set("y", a), Xn(i) && (n.set("x", i), e.firstCategoryDimIndex = 0), Xn(a) && (n.set("y", a), e.firstCategoryDimIndex == null && (e.firstCategoryDimIndex = 1));
  },
  singleAxis: function(t, e, r, n) {
    var i = t.getReferringComponents("singleAxis", ur).models[0];
    e.coordSysDims = ["single"], r.set("single", i), Xn(i) && (n.set("single", i), e.firstCategoryDimIndex = 0);
  },
  polar: function(t, e, r, n) {
    var i = t.getReferringComponents("polar", ur).models[0], a = i.findAxisModel("radiusAxis"), o = i.findAxisModel("angleAxis");
    e.coordSysDims = ["radius", "angle"], r.set("radius", a), r.set("angle", o), Xn(a) && (n.set("radius", a), e.firstCategoryDimIndex = 0), Xn(o) && (n.set("angle", o), e.firstCategoryDimIndex == null && (e.firstCategoryDimIndex = 1));
  },
  geo: function(t, e, r, n) {
    e.coordSysDims = ["lng", "lat"];
  },
  parallel: function(t, e, r, n) {
    var i = t.ecModel, a = i.getComponent("parallel", t.get("parallelIndex")), o = e.coordSysDims = a.dimensions.slice();
    w(a.parallelAxisIndex, function(s, u) {
      var l = i.getComponent("parallelAxis", s), f = o[u];
      r.set(f, l), Xn(l) && (n.set(f, l), e.firstCategoryDimIndex == null && (e.firstCategoryDimIndex = u));
    });
  },
  matrix: function(t, e, r, n) {
    var i = t.getReferringComponents("matrix", ur).models[0];
    e.coordSysDims = ["x", "y"];
    var a = i.getDimensionModel("x"), o = i.getDimensionModel("y");
    r.set("x", a), r.set("y", o), n.set("x", a), n.set("y", o);
  }
};
function Xn(t) {
  return t.get("type") === "category";
}
function n0(t, e, r) {
  r = r || {};
  var n = r.byIndex, i = r.stackedCoordDimension, a, o, s;
  $I(e) ? a = e : (o = e.schema, a = o.dimensions, s = e.store);
  var u = !!(t && t.get("stack")), l, f, h, c, v = !0;
  function d(S) {
    return S.type !== "ordinal" && S.type !== "time";
  }
  if (w(a, function(S, b) {
    N(S) && (a[b] = S = {
      name: S
    }), d(S) || (v = !1);
  }), w(a, function(S, b) {
    u && !S.isExtraCoord && (!n && !l && S.ordinalMeta && (l = S), !f && d(S) && (!v || S.coordDim !== "x" && S.coordDim !== "angle") && (!i || i === S.coordDim) && (f = S));
  }), f && !n && !l && (n = !0), f) {
    h = "__\0ecstackresult_" + t.id, c = "__\0ecstackedover_" + t.id, l && (l.createInvertedIndices = !0);
    var y = f.coordDim, p = f.type, g = 0;
    w(a, function(S) {
      S.coordDim === y && g++;
    });
    var m = {
      name: h,
      coordDim: y,
      coordDimIndex: g,
      type: p,
      isExtraCoord: !0,
      isCalculationCoord: !0,
      storeDimIndex: a.length
    }, _ = {
      name: c,
      // This dimension contains stack base (generally, 0), so do not set it as
      // `stackedDimCoordDim` to avoid extent calculation, consider log scale.
      coordDim: c,
      coordDimIndex: g + 1,
      type: p,
      isExtraCoord: !0,
      isCalculationCoord: !0,
      storeDimIndex: a.length + 1
    };
    o ? (s && (m.storeDimIndex = s.ensureCalculationDimension(c, p), _.storeDimIndex = s.ensureCalculationDimension(h, p)), o.appendCalculationDimension(m), o.appendCalculationDimension(_)) : (a.push(m), a.push(_));
  }
  return {
    stackedDimension: f && f.name,
    stackedByDimension: l && l.name,
    isStackedByIndex: n,
    stackedOverDimension: c,
    stackResultDimension: h
  };
}
function $I(t) {
  return !j_(t.schema);
}
function i0(t, e) {
  return !!e && e === t.getCalculationInfo("stackedDimension");
}
function a0(t, e) {
  return i0(t, e) ? t.getCalculationInfo("stackResultDimension") : e;
}
function ZI(t, e) {
  var r = t.get("coordinateSystem"), n = fu.get(r), i;
  return e && e.coordSysDims && (i = H(e.coordSysDims, function(a) {
    var o = {
      name: a
    }, s = e.axisMap.get(a);
    if (s) {
      var u = s.get("type");
      o.type = OI(u);
    }
    return o;
  })), i || (i = n && (n.getDimensionsInfo ? n.getDimensionsInfo() : n.dimensions.slice()) || ["x", "y"]), i;
}
function KI(t, e, r) {
  var n, i;
  return r && w(t, function(a, o) {
    var s = a.coordDim, u = r.categoryAxisMap.get(s);
    u && (n == null && (n = o), a.ordinalMeta = u.getOrdinalMeta(), e && (a.createInvertedIndices = !0)), a.otherDims.itemName != null && (i = !0);
  }), !i && n != null && (t[n].otherDims.itemName = 0), n;
}
function jI(t, e, r) {
  r = r || {};
  var n = e.getSourceManager(), i, a = !1;
  i = n.getSource(), a = i.sourceFormat === ft;
  var o = XI(e), s = ZI(e, o), u = r.useEncodeDefaulter, l = X(u) ? u : u ? we(kx, s, e) : null, f = {
    coordDimensions: s,
    generateCoord: r.generateCoord,
    encodeDefine: e.getEncode(),
    encodeDefaulter: l,
    canOmitUnusedDimensions: !a
  }, h = r0(i, f), c = KI(h.dimensions, r.createInvertedIndices, o), v = a ? null : n.getSharedDataStore(h), d = n0(e, {
    schema: h,
    store: v
  }), y = new t0(h, e);
  y.setCalculationInfo(d);
  var p = c != null && QI(i) ? function(g, m, _, S) {
    return S === c ? _ : this.defaultDimValueGetter(g, m, _, S);
  } : null;
  return y.hasItemOption = !1, y.initData(
    // Try to reuse the data store in sourceManager if using dataset.
    a ? i : v,
    null,
    p
  ), y;
}
function QI(t) {
  if (t.sourceFormat === ft) {
    var e = JI(t.data || []);
    return !B(Ea(e));
  }
}
function JI(t) {
  for (var e = 0; e < t.length && t[e] == null; )
    e++;
  return t[e];
}
var Et = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.isBlank = function() {
      return this._isBlank;
    }, t.prototype.setBlank = function(e) {
      this._isBlank = e;
    }, t;
  })()
);
$s(Et);
var eA = 0, Ap = (
  /** @class */
  (function() {
    function t(e) {
      this.categories = e.categories || [], this._needCollect = e.needCollect, this._deduplication = e.deduplication, this.uid = ++eA, this._onCollect = e.onCollect;
    }
    return t.createByAxisModel = function(e) {
      var r = e.option, n = r.data, i = n && H(n, tA);
      return new t({
        categories: i,
        needCollect: !i,
        // deduplication is default in axis.
        deduplication: r.dedplication !== !1
      });
    }, t.prototype.getOrdinal = function(e) {
      return this._getOrCreateMap().get(e);
    }, t.prototype.parseAndCollect = function(e) {
      var r, n = this._needCollect;
      if (!N(e) && !n)
        return e;
      if (n && !this._deduplication)
        return r = this.categories.length, this.categories[r] = e, this._onCollect && this._onCollect(e, r), r;
      var i = this._getOrCreateMap();
      return r = i.get(e), r == null && (n ? (r = this.categories.length, this.categories[r] = e, i.set(e, r), this._onCollect && this._onCollect(e, r)) : r = NaN), r;
    }, t.prototype._getOrCreateMap = function() {
      return this._map || (this._map = U(this.categories));
    }, t;
  })()
);
function tA(t) {
  return z(t) && t.value != null ? t.value : t + "";
}
var bt = 0, Ma = 1, rA = {
  needTransform: 1,
  normalize: 1,
  scale: 1,
  transformIn: 1,
  transformOut: 1,
  contain: 1,
  getExtent: 1,
  getExtentUnsafe: 1,
  setExtent: 1,
  setExtent2: 1,
  getFilter: 1,
  sanitize: 1,
  getDefaultStartValue: 1,
  freeze: 1
}, o0 = me(rA), Is = 2, s0 = 3;
function Tc(t, e, r) {
  var n;
  t = t || {};
  var i = Xe();
  if (i) {
    var a = i.createBreakScaleMapper(e, r);
    a.hasBreaks() && (w(o0, function(o) {
      a[o] && (t[o] = le(a[o], a));
    }), n = a);
  }
  return n == null && iA(t, r), {
    brk: n,
    mapper: t
  };
}
function u0(t, e) {
  w(o0, function(r) {
    t[r] = e[r];
  });
}
function l0(t, e) {
  t.freeze = Re;
}
function Da(t) {
  return t.getExtentUnsafe(bt, Is);
}
function f0(t, e) {
  return t.getExtentUnsafe(Ma, e) || t.getExtentUnsafe(bt, e);
}
function nA(t) {
  var e = f0(t, s0);
  return e[1] - e[0];
}
function gu(t) {
  var e = t.getExtentUnsafe(bt, s0);
  return e[1] - e[0];
}
function iA(t, e) {
  var r = t || {}, n = [];
  return r._extents = n, n[bt] = e ? e.slice() : zt(), E(r, aA), r;
}
var aA = {
  needTransform: function() {
    return !1;
  },
  normalize: function(t) {
    var e = this._extents[Ma] || this._extents[bt];
    return e[1] === e[0] ? 0.5 : (t - e[0]) / (e[1] - e[0]);
  },
  scale: function(t) {
    var e = this._extents[Ma] || this._extents[bt];
    return t * (e[1] - e[0]) + e[0];
  },
  transformIn: function(t) {
    return t;
  },
  transformOut: function(t) {
    return t;
  },
  contain: function(t) {
    var e = f0(this, null);
    return t >= e[0] && t <= e[1];
  },
  getExtent: function() {
    return this._extents[bt].slice();
  },
  getExtentUnsafe: function(t) {
    return this._extents[t];
  },
  setExtent: function(t, e) {
    Pp(this._extents, bt, t, e);
  },
  setExtent2: function(t, e, r) {
    var n = this._extents;
    n[t] || (n[t] = n[bt].slice()), Pp(n, t, e, r);
  },
  freeze: function() {
  }
};
function Pp(t, e, r, n) {
  hi(r, n) && (t[e][0] = r, t[e][1] = n);
}
function oA(t) {
  return As(t) || yu(t);
}
function As(t) {
  return t.type === "interval";
}
function wc(t) {
  return t.type === "time";
}
function yu(t) {
  return t.type === "log";
}
function yr(t) {
  return t.type === "ordinal";
}
function FR(t) {
  var e = qs(t), r = In(10, e), n = hr(t / r);
  return n ? n === 2 ? n = 3 : n === 3 ? n = 5 : n *= 2 : n = 1, Ie(n * r, -e);
}
function Va(t) {
  return lr(t) + 2;
}
function Co(t, e) {
  return wn(t) / wn(e);
}
function Pl(t, e, r) {
  var n = r && r.lookup;
  if (n) {
    for (var i = 0; i < n.from.length; i++)
      if (t === n.from[i])
        return n.to[i];
  }
  return In(e, t);
}
function sA(t, e, r) {
  var n = t.slice();
  if (n[0] === n[1]) {
    var i = r && r.ctnShp;
    if (n[0] !== 0) {
      var a = Fe(n[0]);
      e[1] || (n[1] += a / 2), n[0] -= a / 2;
    } else
      i && (n[0] = -1), n[1] = 1;
  }
  return (!dr(n[0]) || !dr(n[1])) && (n[0] = 0, n[1] = 1), n[1] < n[0] && n.reverse(), n;
}
function uA(t, e) {
  return [t[0] !== e[0], t[1] !== e[1]];
}
function Cc(t, e) {
  return t = t || e, hr(ge(t, 1));
}
function h0(t, e, r) {
  var n = Da(t), i = n[0], a = t.count(), o = Math.max((e || 0) + 1, 1);
  i !== 0 && o > 1 && a / o > 2 && (i = Math.round(Math.ceil(i / o) * o)), i !== n[0] && u(n[0], !0, !0);
  for (var s = i; s <= n[1]; s += o)
    u(s, !1, s === n[0] || s === n[1]);
  s - o !== n[1] && u(n[1], !0, !0);
  function u(l, f, h) {
    r({
      value: l,
      offInterval: f
    }, h);
  }
}
var c0 = (
  /** @class */
  (function(t) {
    F(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "ordinal", n.parse = e.parse, u0(n, e.decoratedMethods);
      var i = r.ordinalMeta;
      i || (i = new Ap({})), B(i) && (i = new Ap({
        categories: H(i, function(o) {
          return z(o) ? o.value : o;
        })
      })), n._ordinalMeta = i;
      var a = Tc(
        null,
        null,
        // Do not support break in OrdinalScale yet.
        r.extent || [0, i.categories.length - 1]
      );
      return n._mapper = a.mapper, l0(n), n;
    }
    return e.parse = function(r) {
      return r == null ? r = NaN : N(r) ? (r = this._ordinalMeta.getOrdinal(r), r == null && (r = NaN)) : r = hr(r), r;
    }, e.prototype.getTicks = function() {
      var r = [];
      return h0(this, 0, function(n) {
        r.push(n);
      }), r;
    }, e.prototype.getMinorTicks = function(r) {
    }, e.prototype.setSortInfo = function(r) {
      if (r == null) {
        this._ordinalNumbersByTick = this._ticksByOrdinalNumber = null;
        return;
      }
      for (var n = r.ordinalNumbers, i = this._ordinalNumbersByTick = [], a = this._ticksByOrdinalNumber = [], o = 0, s = this._ordinalMeta.categories.length, u = et(s, n.length); o < u; ++o) {
        var l = i[o] = n[o];
        a[l] = o;
      }
      for (var f = 0; o < s; ++o) {
        for (; a[f] != null; )
          f++;
        i[o] = f, a[f] = o;
      }
    }, e.prototype._getTickNumber = function(r) {
      var n = this._ticksByOrdinalNumber;
      return n && r >= 0 && r < n.length ? n[r] : r;
    }, e.prototype.getRawOrdinalNumber = function(r) {
      var n = this._ordinalNumbersByTick;
      return n && r >= 0 && r < n.length ? n[r] : r;
    }, e.prototype.getLabel = function(r) {
      if (!this.isBlank()) {
        var n = this.getRawOrdinalNumber(r.value), i = this._ordinalMeta.categories[n];
        return i == null ? "" : i + "";
      }
    }, e.prototype.count = function() {
      var r = Da(this._mapper);
      return r[1] - r[0] + 1;
    }, e.prototype.getOrdinalMeta = function() {
      return this._ordinalMeta;
    }, e.type = "ordinal", e.decoratedMethods = {
      needTransform: function() {
        return this._mapper.needTransform();
      },
      contain: function(r) {
        return this._mapper.contain(this._getTickNumber(r)) && r >= 0 && r < this._ordinalMeta.categories.length;
      },
      normalize: function(r) {
        return this._mapper.normalize(this._getTickNumber(r));
      },
      scale: function(r) {
        return this.getRawOrdinalNumber(hr(this._mapper.scale(r)));
      },
      transformIn: function(r, n) {
        return this._mapper.transformIn(this._getTickNumber(r), n);
      },
      transformOut: function(r, n) {
        return this.getRawOrdinalNumber(this._mapper.transformOut(r, n));
      },
      getExtent: function() {
        return this._mapper.getExtent();
      },
      getExtentUnsafe: function(r, n) {
        return this._mapper.getExtentUnsafe(r, n);
      },
      /**
       * NOTICE: OrdinalScale extent should always originates from
       * `[0, ordinalMeta.categories.length - 1]`, regardless of min/max of `series.data`.
       * But settings like `xxxAxis.min/max` can still modify the extent.
       * It is handled by constructor of `ScaleRawExtentInfo`.
       */
      setExtent: function(r, n) {
        return this._mapper.setExtent(r, n);
      },
      setExtent2: function(r, n, i) {
        return this._mapper.setExtent2(r, n, i);
      }
    }, e;
  })(Et)
);
Et.registerClass(c0);
function xc(t, e, r, n) {
  for (var i = t.getTicks({
    expandToNicedExtent: !0
  }), a = [], o = t.getExtent(), s = 1; s < i.length; s++) {
    var u = i[s], l = i[s - 1];
    if (!(l.break || u.break)) {
      for (var f = 0, h = [], c = u.value - l.value, v = c / e, d = Va(v); f < e - 1; ) {
        var y = Ie(l.value + (f + 1) * v, d);
        y > o[0] && y < o[1] && h.push(y), f++;
      }
      var p = Xe();
      p && p.pruneTicksByBreak("auto", h, r, function(g) {
        return g;
      }, n, o), a.push(h);
    }
  }
  return a;
}
var oi = (
  /** @class */
  (function(t) {
    F(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "interval", n.parse = e.parse, r = r || {};
      var i = Em(n, r), a = Tc(n, i, null);
      return n.brk = a.brk, n._cfg = {
        interval: 0,
        intervalPrecision: 2,
        intervalCount: void 0,
        niceExtent: void 0
      }, n;
    }
    return e.parse = function(r) {
      return r == null || r === "" ? NaN : Number(r);
    }, e.prototype.getConfig = function() {
      return Z(this._cfg);
    }, e.prototype.setConfig = function(r) {
      var n = Da(this);
      this._cfg = r = Z(r), r.niceExtent == null && (r.niceExtent = n.slice()), r.intervalPrecision == null && (r.intervalPrecision = Va(r.interval));
    }, e.prototype.getTicks = function(r) {
      r = r || {};
      var n = this._cfg, i = n.interval, a = Da(this), o = n.niceExtent, s = n.intervalPrecision, u = Xe(), l = this.brk, f = u && l, h = [];
      if (!i)
        return h;
      if (r.breakTicks === "only_break" && f)
        return u.addBreaksToTicks(h, l.breaks, a), h;
      var c = 3e3;
      a[0] < o[0] && h.push({
        value: r.expandToNicedExtent ? Ie(o[0] - i, s) : a[0]
      });
      for (var v = function(_, S) {
        return hr((S - _) / i);
      }, d = n.intervalCount, y = o[0], p = 0; ; p++) {
        if (d == null) {
          if (y > o[1] || !isFinite(y) || !isFinite(o[1]))
            break;
        } else {
          if (p > d)
            break;
          y = et(y, o[1]), p === d && (y = o[1]);
        }
        if (h.push({
          value: y
        }), y = Ie(y + i, s), l) {
          var g = l.calcNiceTickMultiple(y, v);
          g >= 0 && (y = Ie(y + g * i, s));
        }
        if (h.length > 0 && y === h[h.length - 1].value)
          break;
        if (h.length > c)
          return [];
      }
      var m = h.length ? h[h.length - 1].value : o[1];
      return a[1] > m && h.push({
        value: r.expandToNicedExtent ? Ie(m + i, s) : a[1]
      }), f && u.pruneTicksByBreak(r.pruneByBreak, h, l.breaks, function(_) {
        return _.value;
      }, n.interval, a), f && r.breakTicks !== "none" && u.addBreaksToTicks(h, l.breaks, a), h;
    }, e.prototype.getMinorTicks = function(r) {
      return xc(this, r, ps(this), this._cfg.interval);
    }, e.prototype.getLabel = function(r, n) {
      if (r == null)
        return "";
      var i = n && n.precision;
      i == null ? i = lr(r.value) || 0 : i === "auto" && (i = this._cfg.intervalPrecision);
      var a = Ie(r.value, i, !0);
      return ec(a);
    }, e.type = "interval", e;
  })(Et)
);
Et.registerClass(oi);
var lA = function(t, e, r, n) {
  for (; r < n; ) {
    var i = r + n >>> 1;
    t[i][1] < e ? r = i + 1 : n = i;
  }
  return r;
}, v0 = (
  /** @class */
  (function(t) {
    F(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "time", n.parse = e.parse, n._locale = r.locale, n._useUTC = r.useUTC, n._interval = 0;
      var i = Em(n, r), a = Tc(n, i, null);
      return n.brk = a.brk, n;
    }
    return e.prototype.getLabel = function(r) {
      return za(r.value, bd[dx(aa(this._minLevelUnit))] || bd.second, this._useUTC, this._locale);
    }, e.prototype.getFormattedLabel = function(r, n, i) {
      return px(r, n, i, this._locale, this._useUTC);
    }, e.prototype.getTicks = function(r) {
      r = r || {};
      var n = this._interval, i = Da(this), a = Xe(), o = this.brk, s = a && o, u = [];
      if (!n)
        return u;
      var l = this._useUTC;
      if (s && r.breakTicks === "only_break")
        return Xe().addBreaksToTicks(u, o.breaks, i), u;
      u = gA(this._minLevelUnit, this._approxInterval, l, i, gu(this), o);
      var f = it.length - 1, h = 0;
      return w(u, function(c) {
        c.time && (f = Math.min(f, oe(it, c.time.upperTimeUnit)), h = Math.max(h, c.time.level));
      }), s && Xe().pruneTicksByBreak(r.pruneByBreak, u, o.breaks, function(c) {
        return c.value;
      }, this._approxInterval, i), s && r.breakTicks !== "none" && Xe().addBreaksToTicks(u, o.breaks, i, function(c) {
        for (var v = Math.max(oe(it, ni(c.vmin, l)), oe(it, ni(c.vmax, l))), d = 0, y = 0; y < it.length; y++)
          if (!d0(it[y], c.vmin, c.vmax, l)) {
            d = y;
            break;
          }
        var p = Math.min(d, f), g = Math.max(p, v);
        return {
          level: h,
          lowerTimeUnit: it[g],
          upperTimeUnit: it[p]
        };
      }), u;
    }, e.prototype.getMinorTicks = function(r) {
      return xc(this, r, ps(this), this._interval);
    }, e.prototype.setTimeInterval = function(r) {
      this._interval = r.interval, this._approxInterval = r.approxInterval, this._minLevelUnit = r.minLevelUnit;
    }, e.parse = function(r) {
      return ve(r) ? Math.round(r) : +pr(r);
    }, e.type = "time", e;
  })(Et)
), xo = [
  // Format                           interval
  ["second", Xh],
  ["minute", qh],
  ["hour", ia],
  ["quarter-day", ia * 6],
  ["half-day", ia * 12],
  ["day", St * 1.2],
  ["half-week", St * 3.5],
  ["week", St * 7],
  ["month", St * 31],
  ["quarter", St * 95],
  ["half-year", Sd / 2],
  ["year", Sd]
  // 1Y
];
function d0(t, e, r, n) {
  return gs(new Date(e), t, n).getTime() === gs(new Date(r), t, n).getTime();
}
function fA(t, e) {
  return t /= St, t > 16 ? 16 : t > 7.5 ? 7 : t > 3.5 ? 4 : t > 1.5 ? 2 : 1;
}
function hA(t) {
  var e = 30 * St;
  return t /= e, t > 6 ? 6 : t > 3 ? 3 : t > 2 ? 2 : 1;
}
function cA(t) {
  return t /= ia, t > 12 ? 12 : t > 6 ? 6 : t > 3.5 ? 4 : t > 2 ? 2 : 1;
}
function Lp(t, e) {
  return t /= e ? qh : Xh, t > 30 ? 30 : t > 20 ? 20 : t > 15 ? 15 : t > 10 ? 10 : t > 5 ? 5 : t > 2 ? 2 : 1;
}
function vA(t) {
  return ge(Sh(t, !0), 1);
}
function dA(t, e, r) {
  var n = Math.max(0, oe(it, e) - 1);
  return gs(new Date(t), it[n], r).getTime();
}
function pA(t, e) {
  var r = /* @__PURE__ */ new Date(0);
  r[t](1);
  var n = r.getTime();
  r[t](1 + e);
  var i = r.getTime() - n;
  return function(a, o) {
    return Math.max(0, Math.round((o - a) / i));
  };
}
function gA(t, e, r, n, i, a) {
  var o = 3e3, s = fx, u = 0;
  function l(O, V, K, J, ee, Q, re) {
    for (var ne = pA(ee, O), q = V, se = new Date(q); q < K && q <= n[1] && (re.push({
      value: q
    }), !(u++ > o)); )
      if (se[ee](se[J]() + O), q = se.getTime(), a) {
        var Se = a.calcNiceTickMultiple(q, ne);
        Se > 0 && (se[ee](se[J]() + Se * O), q = se.getTime());
      }
    re.push({
      value: q,
      // extent[1] should be added; deduplication will be performed later.
      notAdd: q > n[1]
    });
  }
  function f(O, V, K) {
    var J = [], ee = !V.length;
    if (!d0(aa(O), n[0], n[1], r)) {
      ee && (V = [{
        value: dA(n[0], O, r)
      }, {
        value: n[1]
      }]);
      for (var Q = 0; Q < V.length - 1; Q++) {
        var re = V[Q].value, ne = V[Q + 1].value;
        if (re !== ne) {
          var q = void 0, se = void 0, Se = void 0, tt = !1;
          switch (O) {
            case "year":
              q = Math.max(1, Math.round(e / St / 365)), se = Om(r), Se = gx(r);
              break;
            case "half-year":
            case "quarter":
            case "month":
              q = hA(e), se = $h(r), Se = km(r);
              break;
            case "week":
            // PENDING If week is added. Ignore day.
            case "half-week":
            case "day":
              q = fA(e), se = Zh(r), Se = Bm(r), tt = !0;
              break;
            case "half-day":
            case "quarter-day":
            case "hour":
              q = cA(e), se = Kh(r), Se = Nm(r);
              break;
            case "minute":
              q = Lp(e, !0), se = jh(r), Se = Fm(r);
              break;
            case "second":
              q = Lp(e, !1), se = Qh(r), Se = zm(r);
              break;
            case "millisecond":
              q = vA(e), se = Jh(r), Se = Hm(r);
              break;
          }
          ne >= n[0] && re <= n[1] && l(q, re, ne, se, Se, tt, J), O === "year" && K.length > 1 && Q === 0 && K.unshift({
            value: K[0].value - q
          });
        }
      }
      for (var Q = 0; Q < J.length; Q++)
        K.push(J[Q]);
    }
  }
  for (var h = [], c = [], v = 0, d = 0, y = 0; y < s.length; ++y) {
    var p = aa(s[y]);
    if (vx(s[y])) {
      f(s[y], h[h.length - 1] || [], c);
      var g = s[y + 1] ? aa(s[y + 1]) : null;
      if (p !== g) {
        if (c.length) {
          d = v, c.sort(function(O, V) {
            return O.value - V.value;
          });
          for (var m = [], _ = 0; _ < c.length; ++_) {
            var S = c[_].value;
            (_ === 0 || c[_ - 1].value !== S) && (m.push(c[_]), S >= n[0] && S <= n[1] && v++);
          }
          var b = i / e;
          if (v > b * 1.5 && d > b / 1.5 || (h.push(m), v > b || t === s[y]))
            break;
        }
        c = [];
      }
    }
  }
  for (var T = xe(H(h, function(O) {
    return xe(O, function(V) {
      return V.value >= n[0] && V.value <= n[1] && !V.notAdd;
    });
  }), function(O) {
    return O.length > 0;
  }), C = T.length - 1, M = [], y = 0; y < T.length; ++y)
    for (var x = T[y], D = 0; D < x.length; ++D) {
      var I = ni(x[D].value, r);
      M.push({
        value: x[D].value,
        time: {
          level: C - y,
          upperTimeUnit: I,
          lowerTimeUnit: I
        }
      });
    }
  Ch(M, xT, null), M.sort(function(O, V) {
    return O.value - V.value;
  });
  var L = M[0], A = M[M.length - 1], P = ni(n[0], r), k = ni(n[1], r);
  return (!L || L.value > n[0]) && M.unshift({
    value: n[0],
    time: {
      level: 0,
      upperTimeUnit: P,
      lowerTimeUnit: P
    },
    notNice: !0
  }), (!A || A.value < n[1]) && M.push({
    value: n[1],
    time: {
      level: 0,
      upperTimeUnit: k,
      lowerTimeUnit: k
    },
    notNice: !0
  }), M;
}
var yA = function(t, e) {
  var r = t.getExtent();
  if (r[0] === r[1] && (r[0] -= St, r[1] += St), r[1] === -1 / 0 && r[0] === 1 / 0) {
    var n = /* @__PURE__ */ new Date();
    r[1] = +new Date(n.getFullYear(), n.getMonth(), n.getDate()), r[0] = r[1] - St;
  }
  t.setExtent(r[0], r[1]);
  var i = Cc(e.splitNumber, 10), a = gu(t) / i, o = e.minInterval, s = e.maxInterval;
  o != null && a < o && (a = o), s != null && a > s && (a = s);
  var u = xo.length, l = Math.min(lA(xo, a, 0, u), u - 1), f = xo[l][1], h = xo[Math.max(l - 1, 0)][0];
  t.setTimeInterval({
    approxInterval: a,
    interval: f,
    minLevelUnit: h
  });
};
Et.registerClass(v0);
var Mo = 0, Do = 1, mA = 2, p0 = (
  /** @class */
  (function(t) {
    F(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "log", n.parse = oi.parse, n.base = r.logBase || 10;
      var i = [], a = [], o = n._lookup = {
        from: i,
        to: a
      };
      i[Mo] = i[Do] = a[Mo] = a[Do] = NaN, u0(n, e.mapperMethods);
      var s = Xe(), u = r.breakOption, l = {
        lookup: o
      };
      return s && s.parseAxisBreakOptionInwardTransform(u, n, {
        noNegative: !0
      }, mA, l), n.powStub = new oi({
        breakParsed: l.original
      }), n.intervalStub = new oi({
        breakParsed: l.transformed
      }), l0(n, n.intervalStub), n;
    }
    return e.prototype.getTicks = function(r) {
      var n = this.base, i = this.powStub, a = Xe(), o = this.intervalStub, s = o.getExtent(), u = i.getExtent(), l = {
        lookup: {
          from: s,
          to: u
        }
      };
      return H(o.getTicks(r || {}), function(f) {
        var h = f.value, c = Pl(h, n, l), v;
        if (a) {
          var d = a.getTicksBreakOutwardTransform(this, f, ps(i), this._lookup);
          d && (v = d.vBreak, c = d.tickVal);
        }
        return {
          value: c,
          break: v
        };
      }, this);
    }, e.prototype.getMinorTicks = function(r) {
      return xc(
        this,
        r,
        ps(this.powStub),
        // NOTE: minor ticks are in the log scale value to visually hint users "logarithm".
        this.intervalStub.getConfig().interval
      );
    }, e.prototype.getLabel = function(r, n) {
      return this.intervalStub.getLabel(r, n);
    }, e.type = "log", e.mapperMethods = {
      needTransform: function() {
        return !0;
      },
      normalize: function(r) {
        return this.intervalStub.normalize(Co(r, this.base));
      },
      scale: function(r) {
        return Pl(this.intervalStub.scale(r), this.base, null);
      },
      transformIn: function(r, n) {
        return r = Co(r, this.base), n && n.depth === Is ? r : this.intervalStub.transformIn(r, n);
      },
      transformOut: function(r, n) {
        var i = n ? n.depth : null;
        return Rp.depth = i, Ep.lookup = this._lookup, Pl(i === Is ? r : this.intervalStub.transformOut(r, Rp), this.base, Ep);
      },
      contain: function(r) {
        return this.powStub.contain(r);
      },
      /**
       * NOTICE: The caller should ensure `start` and `end` are both non-negative.
       */
      setExtent: function(r, n) {
        this.setExtent2(bt, r, n);
      },
      setExtent2: function(r, n, i) {
        if (!(!hi(n, i) || n <= 0 || i <= 0)) {
          var a = Op, o = Op;
          if (r === bt) {
            var s = this._lookup;
            a = s.to, o = s.from;
          }
          this.powStub.setExtent2(r, a[Mo] = n, a[Do] = i);
          var u = this.base;
          this.intervalStub.setExtent2(r, o[Mo] = Co(n, u), o[Do] = Co(i, u));
        }
      },
      getFilter: function() {
        return {
          g: 0
        };
      },
      sanitize: function(r, n) {
        return hi(n[0], n[1]) && vr(r) && r <= 0 && (r = n[0]), r;
      },
      getDefaultStartValue: function() {
        return 1;
      },
      getExtent: function() {
        return this.powStub.getExtent();
      },
      getExtentUnsafe: function(r, n) {
        return n === null ? this.powStub.getExtentUnsafe(r, null) : this.intervalStub.getExtentUnsafe(r, n);
      }
    }, e;
  })(Et)
);
Et.registerClass(p0);
var Rp = {}, Ep = {}, Op = [], _A = {
  value: 1,
  category: 1,
  time: 1,
  log: 1
}, g0 = de();
function SA(t) {
  var e = t.get("type");
  return (
    // In ec option, `xxxAxis.type` may be undefined.
    (e == null || !qe(_A, e) && !Et.getClass(e)) && (e = "value"), e
  );
}
function bA(t, e, r) {
  var n = Xe(), i;
  switch (n && (i = DA(t, e, r)), e) {
    case "category":
      return new c0({
        ordinalMeta: t.getOrdinalMeta ? t.getOrdinalMeta() : t.getCategories(),
        extent: zt()
      });
    case "time":
      return new v0({
        locale: t.ecModel.getLocaleModel(),
        useUTC: t.ecModel.get("useUTC"),
        breakOption: i
      });
    case "log":
      return new p0({
        logBase: t.get("logBase"),
        breakOption: i
      });
    case "value":
      return new oi({
        breakOption: i
      });
    default:
      return new (Et.getClass(e) || oi)({});
  }
}
function zR(t, e, r) {
  var n = t.getExtentUnsafe(bt, null), i = n[0], a = n[1];
  return hi(i, a) ? i === e || a === e ? wA : i < e && a > e ? TA : kp : kp;
}
var TA = 1, wA = 2, kp = 3;
function CA(t) {
  g0(t).noOnMyZero = !0;
}
function HR(t) {
  return g0(t).noOnMyZero;
}
function mu(t) {
  var e = t.getLabelModel().get("formatter");
  if (t.type === "time") {
    var r = hx(e);
    return function(i, a) {
      return t.scale.getFormattedLabel(i, a, r);
    };
  } else {
    if (N(e))
      return function(i) {
        var a = t.scale.getLabel(i), o = e.replace("{value}", a ?? "");
        return o;
      };
    if (X(e)) {
      if (t.type === "category")
        return function(i, a) {
          return e(
            Ps(t, i),
            i.value - t.scale.getExtent()[0],
            null
            // Using `null` just for backward compat.
          );
        };
      var n = Xe();
      return function(i, a) {
        var o = null;
        return n && (o = n.makeAxisLabelFormatterParamBreak(o, i.break)), e(Ps(t, i), a, o);
      };
    } else
      return function(i) {
        return t.scale.getLabel(i);
      };
  }
}
function Ps(t, e) {
  var r = t.scale;
  return yr(r) ? r.getLabel(e) : e.value;
}
function Mc(t) {
  var e = t.get("interval");
  return e ?? "auto";
}
function xA(t) {
  return t.type === "category" && Mc(t.getLabelModel()) === 0;
}
function MA(t, e) {
  var r = {};
  return w(t.mapDimensionsAll(e), function(n) {
    r[a0(t, n)] = !0;
  }), me(r);
}
function Ls(t) {
  return t === "middle" || t === "center";
}
function VR(t) {
  return t.getShallow("show");
}
function DA(t, e, r) {
  var n = t.get("breaks", !0);
  if (n != null)
    return !Xe() || !r || !IA(e) ? void 0 : n;
}
function IA(t) {
  return t !== "category";
}
function AA(t, e, r, n, i, a) {
  var o = yu(t), s = o ? t.intervalStub : t;
  if (s.setExtent(n[0], n[1]), o) {
    var u = t.powStub, l = {
      depth: Is
    }, f = t.transformOut(n[0], l), h = t.transformOut(n[1], l), c = uA(r, n);
    e[0] && !c[0] && (f = i[0]), e[1] && !c[1] && (h = i[1]), u.setExtent(f, h);
  }
  s.setConfig(a);
}
function Dc(t, e) {
  return yr(t) ? t.getRawOrdinalNumber(e.value) : e.value;
}
function PA(t, e) {
  return yr(t) && !!e.get("boundaryGap");
}
var LA = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.needIncludeZero = function() {
      return !this.option.scale;
    }, t.prototype.getCoordSysModel = function() {
    }, t;
  })()
), RA = wT(), Bp = "|&", _i = de(), EA = -2, GR = -1, OA = de();
function Ic(t, e) {
  var r = t.model, n = _i(mi(r.ecModel)).keyed, i = n && n.get(e);
  return i && i.get(r.uid);
}
function kA(t, e) {
  return m0(Ic(t, e));
}
function BA(t, e) {
  var r = [];
  return y0(t.model.ecModel, function(n) {
    for (var i = 0; i < e.length; i++)
      e[i] && n.serByIdx[e[i].seriesIndex] && r.push(m0(n));
  }), r;
}
function y0(t, e) {
  var r = _i(mi(t)).keyed;
  r && r.each(function(n, i) {
    n.each(function(a, o) {
      e(a, i, o);
    });
  });
}
function m0(t) {
  return {
    liPosMinGap: t ? t.liPosMinGap : void 0
  };
}
function NA(t, e) {
  var r = t.model.ecModel, n = _i(mi(r)).axSer;
  n && Ac(r, n.get(t.model.uid), e);
}
function UR(t, e, r) {
  var n = Ic(t, e);
  n && Ac(t.model.ecModel, n.sers, r);
}
function Ac(t, e, r) {
  if (e)
    for (var n = 0; n < e.length; n++) {
      var i = e[n];
      t.isSeriesFiltered(i) || r(i);
    }
}
function YR(t, e) {
  var r = Ic(t, e);
  if (!r || !r.sers.length)
    return 0;
  var n = 0;
  return Ac(t.model.ecModel, r.sers, function() {
    n++;
  }), n;
}
function WR(t, e, r) {
  var n = _i(mi(t)).keyed, i = n && n.get(e);
  i && i.each(function(a) {
    r(a.axis);
  });
}
function _0(t, e) {
  var r = t.model, n = _i(mi(r.ecModel)).keys;
  n && w(n.get(r.uid), function(i) {
    e(i);
  });
}
function FA(t) {
  var e = OA(DD(t)), r = e.keyed || (e.keyed = U());
  y0(t, function(n, i, a) {
    var o = r.get(i) || r.set(i, U()), s = o.get(a) || o.set(a, {});
    n.metrics.liPosMinGap && S0.liPosMinGap(t, n, s);
  });
}
function XR(t, e) {
  S0[t] = e;
}
var S0 = {};
function qR(t, e, r) {
  if (t) {
    var n = e.ecModel, i = _i(mi(n)), a = t.model.uid, o = i.axSer || (i.axSer = U()), s = o.get(a) || o.set(a, []);
    s.push(e);
    var u = e.subType, l = e.getBaseAxis() === t, f = Kf.get(Zf(u, l, r)) || Kf.get(Zf(u, l, null));
    if (f) {
      var h = i.keyed || (i.keyed = U()), c = i.keys || (i.keys = U()), v = f.key, d = h.get(v) || h.set(v, U()), y = d.get(a);
      y || (y = d.set(a, {
        axis: t,
        sers: [],
        serByIdx: []
      }), y.metrics = f.getMetrics(t), (c.get(a) || c.set(a, [])).push(v)), y.sers.push(e), y.serByIdx[e.seriesIndex] = e;
    }
  }
}
function Zf(t, e, r) {
  return t + Bp + G(e, !0) + Bp + (r || "");
}
function $R(t, e) {
  var r = Zf(e.seriesType, e.baseAxis, e.coordSysType);
  Kf.set(r, e), RA(t, function() {
    t.registerProcessor(t.PRIORITY.PROCESSOR.AXIS_STATISTICS, {
      // NOTE: Theoretically, `appendData` requires `dirtyOnOverallProgress: true` here to re-calculate them.
      // But this OVERALL_STAGE_TASK is applied to all series (no `getTargetSeries` specified),
      // `dirtyOnOverallProgress: true` can cause irrelevant series (e.g., series on geo)
      // to be re-rendered when `appendData` is called, which cause `appendData` meaningless,
      // thereby not setting `dirtyOnOverallProgress: true`.
      overallReset: FA
    });
  });
}
var Kf = U(), zA = de(), ZR = 1, HA = 3, b0 = (
  /** @class */
  (function() {
    function t(e, r, n, i, a) {
      var o = yr(e), s = o ? r.getCategories().length : null, u;
      if (o) {
        var l = r.getCategories(!0);
        u = l && !l.length;
      }
      var f = n.slice();
      (As(e) || yu(e) || wc(e)) && (wy(f, Gi(e, r.get("dataMin", !0))), Cy(f, Gi(e, r.get("dataMax", !0)))), bT(f) || (f[0] = f[1] = NaN);
      var h = [], c = [!1, !1], v = r.get("min", !0);
      v === "dataMin" ? (h[0] = f[0], c[0] = !0) : (h[0] = Gi(e, X(v) ? v({
        min: f[0],
        max: f[1]
      }) : v), c[0] = h[0] != null);
      var d = r.get("max", !0);
      d === "dataMax" ? (h[1] = f[1], c[1] = !0) : (h[1] = Gi(e, X(d) ? d({
        min: f[0],
        max: f[1]
      }) : d), c[1] = h[1] != null);
      var y = VA(e, r), p = o ? null : f[1] - f[0] || Math.abs(f[0]);
      h[0] == null && (h[0] = o ? u ? f[0] : s ? 0 : NaN : f[0] - y[0] * p), h[1] == null && (h[1] = o ? u ? f[1] : s ? s - 1 : NaN : f[1] + y[1] * p), !dr(h[0]) && (h[0] = NaN), !dr(h[1]) && (h[1] = NaN);
      var g = u || ha(h[0]) || ha(h[1]) || o && !s, m = As(e), _ = m && r.needIncludeZero && r.needIncludeZero();
      _ && (h[0] > 0 && h[1] > 0 && !c[0] && (h[0] = 0), h[0] < 0 && h[1] < 0 && !c[1] && (h[1] = 0));
      var S = !1;
      h[0] > h[1] && (h.reverse(), S = !0);
      var b = Gi(e, r.get("startValue", !0)), T = b != null;
      !vr(b) && i && (b = e.getDefaultStartValue ? e.getDefaultStartValue() : 0), vr(b) && (T || !m || _) && (b < h[0] && !c[0] ? (h[0] = b, c[0] = !0) : b > h[1] && !c[1] && (h[1] = b, c[1] = !0));
      var C = this._i = {
        scale: e,
        dataMM: f,
        noZoomEffMM: h,
        zoomMM: [],
        fixMM: c,
        zoomFixMM: [!1, !1],
        startValue: b,
        isBlank: g,
        incl0: _,
        tggAxInv: S,
        ctnShp: a
      };
      Np(C, h);
    }
    return t.prototype.makeNoZoom = function() {
      return this._i.noZoomEffMM.slice();
    }, t.prototype.makeFinal = function() {
      var e = this._i, r = e.zoomMM, n = e.noZoomEffMM, i = e.zoomFixMM, a = e.fixMM, o = {
        fixMM: a,
        zoomFixMM: i,
        isBlank: e.isBlank,
        incl0: e.incl0,
        tggAxInv: e.tggAxInv,
        ctnShp: e.ctnShp,
        effMM: n.slice()
      }, s = o.effMM;
      return r[0] != null && (s[0] = r[0], a[0] = i[0] = !0), r[1] != null && (s[1] = r[1], a[1] = i[1] = !0), Np(e, s), o;
    }, t.prototype.makeRenderInfo = function() {
      return {
        startValue: this._i.startValue
      };
    }, t.prototype.setZoomMM = function(e, r) {
      this._i.zoomMM[e] = r;
    }, t;
  })()
);
function Np(t, e) {
  var r = t.scale, n = t.dataMM;
  r.sanitize && (e[0] = r.sanitize(e[0], n), e[1] = r.sanitize(e[1], n), TT(e));
}
function Gi(t, e) {
  return e == null ? null : ha(e) ? NaN : t.parse(e);
}
function VA(t, e) {
  var r;
  if (yr(t))
    r = [0, 0];
  else {
    var n = e.get("boundaryGap");
    typeof n == "boolean" && (n = null), r = B(n) ? n : [n, n];
  }
  return [Fp(r[0]), Fp(r[1])];
}
function Fp(t) {
  return fi(typeof t == "boolean" ? 0 : t, 1) || 0;
}
function T0(t) {
  var e = zA(t.scale);
  return e.extent || (e.extent = zt()), e;
}
function KR(t, e) {
  T0(t).dimIdxInCoord = e.get(t.dim);
}
function jR(t, e) {
  var r = t.scale, n = t.model, i = t.dim;
  r.rawExtentInfo || GA(r, t, i, n, e);
}
function GA(t, e, r, n, i) {
  var a = T0(e), o = a.extent, s = !1;
  NA(e, function(f) {
    if (f.boxCoordinateSystem) {
      var h = Gm(f).coord, c = a.dimIdxInCoord;
      if (c >= 0) {
        if (B(h)) {
          var v = h[c];
          v != null && !B(v) && _T(o, t.parse(v));
        }
      }
    } else if (f.coordinateSystem) {
      var d = f.getData();
      if (d) {
        var y = t.getFilter ? t.getFilter() : null;
        w(MA(d, r), function(p) {
          ST(o, d.getApproximateExtent(p, y));
        });
      }
      f.__requireStartValue && f.__requireStartValue(e) && (s = !0);
    }
  });
  var u = WA(t, e, n), l = new b0(t, n, o, s, u);
  w0(t, l, i), a.extent = null;
}
function UA(t, e) {
  var r = t.scale;
  w0(r, new b0(r, t.model, e, !1, !1), HA);
}
function w0(t, e, r) {
  t.rawExtentInfo = e, e.from = r;
}
function QR(t, e) {
  Pc.set(t, e);
}
var Pc = U();
function YA(t, e, r, n, i) {
  t.rawExtentInfo || UA({
    scale: t,
    model: e
  }, i || zt());
  var a = t.rawExtentInfo.makeFinal(), o = a.effMM;
  return t.setExtent(o[0], o[1]), t.setBlank(a.isBlank), n && a.tggAxInv && r && !r.get("legacyMinMaxDontInverseAxis") && (n.inverse = !n.inverse), a;
}
function WA(t, e, r) {
  var n = PA(t, r), i = r.get("containShape", !0);
  if (i == null && !n && (i = !0), !i)
    return !1;
  var a = !1;
  return _0(e, function(o) {
    a = !!Pc.get(o) || a;
  }), a;
}
function XA(t, e, r, n) {
  if (r.ctnShp) {
    var i;
    if (_0(t, function(s) {
      var u = Pc.get(s);
      if (u) {
        var l = u(t, n);
        l && (i = i || [0, 0], wy(i, l[0]), Cy(i, l[1]), CA(t));
      }
    }), !!i) {
      var a = e.getExtent();
      if (yr(e))
        t.onBand || e.setExtent2(Ma, et(a[0], a[0] + i[0]), ge(a[1], a[1] + i[1]));
      else {
        var o = a.slice();
        r.zoomFixMM[0] || (o[0] = et(o[0], e.transformOut(e.transformIn(o[0], null) + i[0], null))), r.zoomFixMM[1] || (o[1] = ge(o[1], e.transformOut(e.transformIn(o[1], null) + i[1], null))), (o[0] < a[0] || o[1] > a[1]) && e.setExtent2(Ma, o[0], o[1]);
      }
    }
  }
}
function zp(t, e) {
  var r = yu(t), n = r ? t.intervalStub : t, i = e.fixMinMax || [], a = r ? t.getExtent() : null, o = n.getExtent(), s = sA(o, i, e.rawExtentResult);
  n.setExtent(s[0], s[1]), s = n.getExtent();
  var u = r ? $A(n, e) : qA(n, e), l = u.intervalPrecision, f = u.interval, h = e.userInterval;
  h != null && (u.interval = h, u.intervalPrecision = Va(h)), i[0] || (s[0] = Ie(Dn(s[0] / f) * f, l)), i[1] || (s[1] = Ie(Xs(s[1] / f) * f, l)), h != null && (u.niceExtent = s.slice()), AA(t, i, o, s, a, u);
}
function qA(t, e) {
  var r = Cc(e.splitNumber, 5), n = gu(t), i = e.minInterval, a = e.maxInterval, o = Sh(n / r, !0);
  i != null && o < i && (o = i), a != null && o > a && (o = a);
  var s = Va(o), u = t.getExtent(), l = [Ie(Xs(u[0] / o) * o, s), Ie(Dn(u[1] / o) * o, s)];
  return {
    interval: o,
    intervalPrecision: s,
    niceExtent: l
  };
}
function $A(t, e) {
  var r = Cc(e.splitNumber, 10), n = t.getExtent(), i = gu(t), a = ge(gy(i), 1), o = r / i * a;
  o <= 0.5 && (a *= 10);
  var s = Va(a), u = [Ie(Xs(n[0] / a) * a, s), Ie(Dn(n[1] / a) * a, s)];
  return {
    intervalPrecision: s,
    interval: a,
    niceExtent: u
  };
}
function JR(t) {
  var e = t.scale, r = t.model, n = r.axis, i = r.ecModel;
  C0(e, r, n, i, null);
}
function C0(t, e, r, n, i) {
  var a = YA(t, e, n, r, i), o = As(t) || wc(t);
  ZA(t, {
    splitNumber: e.get("splitNumber"),
    fixMinMax: a.fixMM,
    userInterval: e.get("interval"),
    minInterval: o ? e.get("minInterval") : null,
    maxInterval: o ? e.get("maxInterval") : null,
    rawExtentResult: a
  }), r && n && XA(r, t, a, n);
}
function ZA(t, e) {
  KA[t.type](t, e);
}
var KA = {
  interval: zp,
  log: zp,
  time: yA,
  ordinal: Re
};
function jA(t) {
  return jI(null, t);
}
var QA = {
  isDimensionStacked: i0,
  enableDataStack: n0,
  getStackedDimension: a0
};
function JA(t, e) {
  var r = e;
  e instanceof ce || (r = new ce(e));
  var n = SA(r), i = bA(r, n, !1);
  return t[1] < t[0] && (t = t.slice().reverse()), C0(i, r, null, null, t), i;
}
function eP(t) {
  Lt(t, LA);
}
function tP(t, e) {
  return e = e || {}, Cn(t, null, null, e.state !== "normal");
}
const rP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  createDimensions: GI,
  createList: jA,
  createScale: JA,
  createSymbol: vu,
  createTextStyle: tP,
  dataStack: QA,
  enableHoverEmphasis: vs,
  getECData: he,
  getLayoutRect: ys,
  mixinAxisModelCommonMethods: eP
}, Symbol.toStringTag, { value: "Module" }));
var Hp = [], nP = {
  registerPreprocessor: yc,
  registerProcessor: mc,
  registerPostInit: Y_,
  registerPostUpdate: W_,
  registerUpdateLifecycle: pu,
  registerAction: Ln,
  registerCoordinateSystem: X_,
  registerLayout: q_,
  registerVisual: Fr,
  registerTransform: Z_,
  registerLoading: Sc,
  registerMap: $_,
  registerImpl: CD,
  PRIORITY: k_,
  ComponentModel: ae,
  ComponentView: lt,
  SeriesModel: $t,
  ChartView: Wt,
  // TODO Use ComponentModel and SeriesModel instead of Constructor
  registerComponentModel: function(t) {
    ae.registerClass(t);
  },
  registerComponentView: function(t) {
    lt.registerClass(t);
  },
  registerSeriesModel: function(t) {
    $t.registerClass(t);
  },
  registerChartView: function(t) {
    Wt.registerClass(t);
  },
  registerCustomSeries: function(t, e) {
  },
  registerSubTypeDefaulter: function(t, e) {
    ae.registerSubTypeDefaulter(t, e);
  },
  registerPainter: function(t, e) {
    vy(t, e);
  }
};
function vi(t) {
  if (B(t)) {
    w(t, function(e) {
      vi(e);
    });
    return;
  }
  oe(Hp, t) >= 0 || (Hp.push(t), X(t) && (t = {
    install: t
  }), t.install(nP));
}
var iP = 1e-8;
function Vp(t, e) {
  return Math.abs(t - e) < iP;
}
function Gp(t, e, r) {
  var n = 0, i = t[0];
  if (!i)
    return !1;
  for (var a = 1; a < t.length; a++) {
    var o = t[a];
    n += sr(i[0], i[1], o[0], o[1], e, r), i = o;
  }
  var s = t[0];
  return (!Vp(i[0], s[0]) || !Vp(i[1], s[1])) && (n += sr(i[0], i[1], s[0], s[1], e, r)), n !== 0;
}
var aP = [];
function Ll(t, e) {
  for (var r = 0; r < t.length; r++)
    ot(t[r], t[r], e);
}
function Up(t, e, r, n) {
  for (var i = 0; i < t.length; i++) {
    var a = t[i];
    n && (a = n.project(a)), a && isFinite(a[0]) && isFinite(a[1]) && (Cr(e, e, a), xr(r, r, a));
  }
}
function oP(t) {
  for (var e = 0, r = 0, n = 0, i = t.length, a = t[i - 1][0], o = t[i - 1][1], s = 0; s < i; s++) {
    var u = t[s][0], l = t[s][1], f = a * l - u * o;
    e += f, r += (a + u) * f, n += (o + l) * f, a = u, o = l;
  }
  return e ? [r / e / 3, n / e / 3, e] : [t[0][0] || 0, t[0][1] || 0];
}
var x0 = (
  /** @class */
  (function() {
    function t(e) {
      this.name = e;
    }
    return t.prototype.setCenter = function(e) {
      this._center = e;
    }, t.prototype.getCenter = function() {
      var e = this._center;
      return e || (e = this._center = this.calcCenter()), e;
    }, t;
  })()
), Yp = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e, r) {
      this.type = "polygon", this.exterior = e, this.interiors = r;
    }
    return t;
  })()
), Wp = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.type = "linestring", this.points = e;
    }
    return t;
  })()
), sP = (
  /** @class */
  (function(t) {
    F(e, t);
    function e(r, n, i) {
      var a = t.call(this, r) || this;
      return a.type = "geoJSON", a.geometries = n, a._center = i && [i[0], i[1]], a;
    }
    return e.prototype.calcCenter = function() {
      for (var r = this.geometries, n, i = 0, a = 0; a < r.length; a++) {
        var o = r[a], s = o.exterior, u = s && s.length;
        u > i && (n = o, i = u);
      }
      if (n)
        return oP(n.exterior);
      var l = this.getBoundingRect();
      return [l.x + l.width / 2, l.y + l.height / 2];
    }, e.prototype.getBoundingRect = function(r) {
      var n = this._rect;
      if (n && !r)
        return n;
      var i = [1 / 0, 1 / 0], a = [-1 / 0, -1 / 0], o = this.geometries;
      return w(o, function(s) {
        s.type === "polygon" ? Up(s.exterior, i, a, r) : w(s.points, function(u) {
          Up(u, i, a, r);
        });
      }), isFinite(i[0]) && isFinite(i[1]) && isFinite(a[0]) && isFinite(a[1]) || (i[0] = i[1] = a[0] = a[1] = 0), n = new Y(i[0], i[1], a[0] - i[0], a[1] - i[1]), r || (this._rect = n), n;
    }, e.prototype.contain = function(r) {
      var n = this.getBoundingRect(), i = this.geometries;
      if (!n.contain(r[0], r[1]))
        return !1;
      e: for (var a = 0, o = i.length; a < o; a++) {
        var s = i[a];
        if (s.type === "polygon") {
          var u = s.exterior, l = s.interiors;
          if (Gp(u, r[0], r[1])) {
            for (var f = 0; f < (l ? l.length : 0); f++)
              if (Gp(l[f], r[0], r[1]))
                continue e;
            return !0;
          }
        }
      }
      return !1;
    }, e.prototype.transformTo = function(r, n, i, a) {
      var o = this.getBoundingRect(), s = o.width / o.height;
      i ? a || (a = i / s) : i = s * a;
      for (var u = new Y(r, n, i, a), l = o.calculateTransform(u), f = this.geometries, h = 0; h < f.length; h++) {
        var c = f[h];
        c.type === "polygon" ? (Ll(c.exterior, l), w(c.interiors, function(v) {
          Ll(v, l);
        })) : w(c.points, function(v) {
          Ll(v, l);
        });
      }
      o = this._rect, o.copy(u), this._center = [o.x + o.width / 2, o.y + o.height / 2];
    }, e.prototype.cloneShallow = function(r) {
      r == null && (r = this.name);
      var n = new e(r, this.geometries, this._center);
      return n._rect = this._rect, n.transformTo = null, n;
    }, e;
  })(x0)
);
(function(t) {
  F(e, t);
  function e(r, n) {
    var i = t.call(this, r) || this;
    return i.type = "geoSVG", i._elOnlyForCalculate = n, i;
  }
  return e.prototype.calcCenter = function() {
    for (var r = this._elOnlyForCalculate, n = r.getBoundingRect(), i = [n.x + n.width / 2, n.y + n.height / 2], a = Mn(aP), o = r; o && !o.isGeoSVGGraphicRoot; )
      mn(a, o.getLocalTransform(), a), o = o.parent;
    return gi(a, a), ot(i, i, a), i;
  }, e;
})(x0);
function uP(t) {
  if (!t.UTF8Encoding)
    return t;
  var e = t, r = e.UTF8Scale;
  r == null && (r = 1024);
  var n = e.features;
  return w(n, function(i) {
    var a = i.geometry, o = a.encodeOffsets, s = a.coordinates;
    if (o)
      switch (a.type) {
        case "LineString":
          a.coordinates = M0(s, o, r);
          break;
        case "Polygon":
          Rl(s, o, r);
          break;
        case "MultiLineString":
          Rl(s, o, r);
          break;
        case "MultiPolygon":
          w(s, function(u, l) {
            return Rl(u, o[l], r);
          });
      }
  }), e.UTF8Encoding = !1, e;
}
function Rl(t, e, r) {
  for (var n = 0; n < t.length; n++)
    t[n] = M0(t[n], e[n], r);
}
function M0(t, e, r) {
  for (var n = [], i = e[0], a = e[1], o = 0; o < t.length; o += 2) {
    var s = t.charCodeAt(o) - 64, u = t.charCodeAt(o + 1) - 64;
    s = s >> 1 ^ -(s & 1), u = u >> 1 ^ -(u & 1), s += i, u += a, i = s, a = u, n.push([s / r, u / r]);
  }
  return n;
}
function Xp(t, e) {
  return t = uP(t), H(xe(t.features, function(r) {
    return r.geometry && r.properties && r.geometry.coordinates.length > 0;
  }), function(r) {
    var n = r.properties, i = r.geometry, a = [];
    switch (i.type) {
      case "Polygon":
        var o = i.coordinates;
        a.push(new Yp(o[0], o.slice(1)));
        break;
      case "MultiPolygon":
        w(i.coordinates, function(u) {
          u[0] && a.push(new Yp(u[0], u.slice(1)));
        });
        break;
      case "LineString":
        a.push(new Wp([i.coordinates]));
        break;
      case "MultiLineString":
        a.push(new Wp(i.coordinates));
    }
    var s = new sP(n[e || "name"], a, n.cp);
    return s.properties = n, s;
  });
}
const lP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  MAX_SAFE_INTEGER: Zb,
  asc: mh,
  getPercentWithPrecision: Xb,
  getPixelPrecision: Wb,
  getPrecision: lr,
  getPrecisionSafe: py,
  isNumeric: yy,
  isRadianAroundZero: ya,
  linearMap: pf,
  nice: Sh,
  numericToNumber: cr,
  parseDate: pr,
  parsePercent: Dt,
  quantile: Qb,
  quantity: gy,
  quantityExponent: qs,
  reformIntervals: Jb,
  remRadian: _h,
  round: Yb
}, Symbol.toStringTag, { value: "Module" })), fP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  format: za,
  parse: pr,
  roundTime: gs
}, Symbol.toStringTag, { value: "Module" })), hP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: Fa,
  BezierCurve: iu,
  BoundingRect: Y,
  Circle: Na,
  CompoundPath: nm,
  Ellipse: Js,
  Group: ut,
  Image: Nr,
  IncrementalDisplayable: lm,
  Line: Pn,
  LinearGradient: am,
  Polygon: ru,
  Polyline: nu,
  RadialGradient: om,
  Rect: Ee,
  Ring: tu,
  Sector: eu,
  Text: je,
  clipPointsByRect: Tm,
  clipRectByRect: wm,
  createIcon: ou,
  extendPath: ym,
  extendShape: gm,
  getShapeClass: mm,
  getTransform: bm,
  initProps: kh,
  makeImage: Bh,
  makePath: au,
  mergePath: Sm,
  registerShape: wt,
  resizePath: Nh,
  updateProps: yi
}, Symbol.toStringTag, { value: "Module" })), cP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  addCommas: ec,
  capitalFirst: _x,
  encodeHTML: Ue,
  formatTime: mx,
  formatTpl: rc,
  getTextRect: yx,
  getTooltipMarker: Vm,
  normalizeCssArray: Ha,
  toCamelCase: tc,
  truncateText: HT
}, Symbol.toStringTag, { value: "Module" })), vP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  bind: le,
  clone: Z,
  curry: we,
  defaults: _e,
  each: w,
  extend: E,
  filter: xe,
  indexOf: oe,
  inherits: ch,
  isArray: B,
  isFunction: X,
  isObject: z,
  isString: N,
  map: H,
  merge: pe,
  reduce: Xt
}, Symbol.toStringTag, { value: "Module" }));
var dP = de(), ua = de(), kr = {
  estimate: 1,
  determine: 2
};
function Rs(t) {
  return {
    out: {
      noPxChangeTryDetermine: []
    },
    kind: t
  };
}
function pP(t, e) {
  var r = t.getLabelModel().get("customValues");
  if (r) {
    var n = t.scale;
    return {
      labels: H(D0(r, n), function(i, a) {
        return {
          formattedLabel: mu(t)(i, a),
          rawLabel: n.getLabel(i),
          tick: i
        };
      })
    };
  }
  return t.type === "category" ? yP(t, e) : _P(t);
}
function gP(t, e, r) {
  var n = t.scale, i = t.getTickModel().get("customValues");
  return i ? {
    ticks: D0(i, n)
  } : t.type === "category" ? mP(t, e) : {
    ticks: n.getTicks(r)
  };
}
function D0(t, e) {
  var r = e.getExtent(), n = [];
  return w(t, function(i) {
    i = e.parse(i), i >= r[0] && i <= r[1] && n.push(i);
  }), Ch(n, MT, null), mh(n), H(n, function(i) {
    return {
      value: i
    };
  });
}
function yP(t, e) {
  var r = t.getLabelModel(), n = I0(t, r, e);
  return !r.get("show") || t.scale.isBlank() ? {
    labels: []
  } : n;
}
function I0(t, e, r) {
  var n = bP(t), i = Mc(e), a = r.kind === kr.estimate;
  if (!a) {
    var o = P0(n, i);
    if (o)
      return o;
  }
  var s, u;
  X(i) ? s = Es(t, i, !1) : (u = i === "auto" ? TP(t, r) : i, s = Es(t, u, !1));
  var l = {
    labels: s,
    labelCategoryInterval: u
  };
  return a ? r.out.noPxChangeTryDetermine.push(function() {
    return jf(n, i, l), !0;
  }) : jf(n, i, l), l;
}
function mP(t, e) {
  var r = SP(t), n = Mc(e), i = P0(r, n);
  if (i)
    return i;
  var a, o;
  if ((!e.get("show") || t.scale.isBlank()) && (a = []), X(n))
    a = Es(t, n, !0);
  else if (n === "auto") {
    var s = I0(t, t.getLabelModel(), Rs(kr.determine));
    o = s.labelCategoryInterval, a = H(s.labels, function(u) {
      return u.tick;
    });
  } else
    o = n, a = Es(t, o, !0);
  return jf(r, n, {
    ticks: a,
    tickCategoryInterval: o
  });
}
function _P(t) {
  var e = t.scale.getTicks(), r = mu(t);
  return {
    labels: H(e, function(n, i) {
      return {
        formattedLabel: r(n, i),
        rawLabel: t.scale.getLabel(n),
        tick: n
      };
    })
  };
}
var SP = A0("axisTick"), bP = A0("axisLabel");
function A0(t) {
  return function(r) {
    return ua(r)[t] || (ua(r)[t] = {
      list: []
    });
  };
}
function P0(t, e) {
  for (var r = 0; r < t.list.length; r++)
    if (t.list[r].key === e)
      return t.list[r].value;
}
function jf(t, e, r) {
  return t.list.push({
    key: e,
    value: r
  }), r;
}
function TP(t, e) {
  if (e.kind === kr.estimate) {
    var r = t.calculateCategoryInterval(e);
    return e.out.noPxChangeTryDetermine.push(function() {
      return ua(t).autoInterval = r, !0;
    }), r;
  }
  var n = ua(t).autoInterval;
  return n ?? (ua(t).autoInterval = t.calculateCategoryInterval(e));
}
function wP(t, e) {
  var r = e.kind, n = xP(t), i = mu(t), a = (n.axisRotate - n.labelRotate) / 180 * Math.PI, o = t.scale, s = o.getExtent(), u = o.count();
  if (s[1] - s[0] < 1)
    return 0;
  var l = 1, f = 40;
  u > f && (l = Math.max(1, Math.floor(u / f)));
  for (var h = s[0], c = t.dataToCoord(h + 1) - t.dataToCoord(h), v = Math.abs(c * Math.cos(a)), d = Math.abs(c * Math.sin(a)), y = 0, p = 0; h <= s[1]; h += l) {
    var g = 0, m = 0, _ = fy(i({
      value: h
    }), n.font, "center", "top");
    g = _.width * 1.3, m = _.height * 1.3, y = Math.max(y, g, 7), p = Math.max(p, m, 7);
  }
  var S = y / v, b = p / d;
  isNaN(S) && (S = 1 / 0), isNaN(b) && (b = 1 / 0);
  var T = Math.max(0, Math.floor(Math.min(S, b)));
  if (r === kr.estimate)
    return e.out.noPxChangeTryDetermine.push(le(CP, null, t, T, u)), T;
  var C = L0(t, T, u);
  return C ?? T;
}
function CP(t, e, r) {
  return L0(t, e, r) == null;
}
function L0(t, e, r) {
  var n = dP(t.model), i = t.getExtent(), a = n.lastAutoInterval, o = n.lastTickCount;
  if (a != null && o != null && Math.abs(a - e) <= 1 && Math.abs(o - r) <= 1 && a > e && n.axisExtent0 === i[0] && n.axisExtent1 === i[1])
    return a;
  n.lastTickCount = r, n.lastAutoInterval = e, n.axisExtent0 = i[0], n.axisExtent1 = i[1];
}
function xP(t) {
  var e = t.getLabelModel();
  return {
    axisRotate: t.getRotate ? t.getRotate() : t.isHorizontal && !t.isHorizontal() ? 90 : 0,
    labelRotate: e.get("rotate") || 0,
    font: e.getFont()
  };
}
function Es(t, e, r) {
  var n = mu(t), i = t.scale, a = [], o = X(e);
  return h0(i, o ? 0 : e, function(s, u) {
    var l = i.getLabel(s);
    if (o) {
      var f = !!e(s.value, l);
      if (s.offInterval = !f, !f && !u)
        return;
    }
    a.push(r ? s : {
      formattedLabel: n(s),
      rawLabel: l,
      tick: s
    });
  }), a;
}
var MP = 0.8;
function _u(t, e) {
  e = e || {};
  var r = {
    w: NaN,
    w2: NaN
  }, n = t.scale, i = e.fromStat, a = e.min, o = nA(n);
  vr(o) || (o = NaN);
  var s = t.getExtent(), u = Fe(s[1] - s[0]);
  return yr(n) ? DP(r, t, o, u) : i && IP(r, t, o, u, i), a != null && (r.w = vr(r.w) ? ge(a, r.w) : a), r;
}
function DP(t, e, r, n) {
  var i = e.onBand, a = r + (i ? 1 : 0);
  a === 0 && (a = 1), t.w = n / a, !i && r && n && (t.w2 = t.w * r / n);
}
function IP(t, e, r, n, i) {
  var a = !1, o = -1 / 0;
  w(i.key ? [kA(e, i.key)] : BA(e, i.sers || []), function(s) {
    var u = s.liPosMinGap;
    u != null && (u > 0 ? (u > o && (o = u), a = !1) : u === EA && (a = !0));
  }), vr(r) && r > 0 && vr(o) ? (t.w = n / r * o, t.w2 = o) : a && (t.w = n * MP, t.w2 = t.w * r / n);
}
var qp = [0, 1], AP = (
  /** @class */
  (function() {
    function t(e, r, n) {
      this.onBand = !1, this.inverse = !1, this.dim = e, this.scale = r, this._extent = n || [0, 0];
    }
    return t.prototype.contain = function(e) {
      var r = this._extent, n = Math.min(r[0], r[1]), i = Math.max(r[0], r[1]);
      return e >= n && e <= i;
    }, t.prototype.containData = function(e) {
      return this.scale.contain(this.scale.parse(e));
    }, t.prototype.getExtent = function() {
      return this._extent.slice();
    }, t.prototype.setExtent = function(e, r) {
      var n = this._extent;
      n[0] = e, n[1] = r;
    }, t.prototype.dataToCoord = function(e, r) {
      var n = this.scale;
      return e = n.normalize(n.parse(e)), pf(e, qp, $p(this), r);
    }, t.prototype.coordToData = function(e, r) {
      var n = pf(e, $p(this), qp, r);
      return this.scale.scale(n);
    }, t.prototype.pointToData = function(e, r) {
    }, t.prototype.getTicksCoords = function(e) {
      e = e || {};
      var r = e.tickModel || this.getTickModel(), n = gP(this, r, {
        breakTicks: e.breakTicks,
        pruneByBreak: e.pruneByBreak
      }), i = H(n.ticks, function(s) {
        return {
          coord: this.dataToCoord(Dc(this.scale, s)),
          tick: s
        };
      }, this), a = r.get("alignWithLabel"), o = PP(this, i, a);
      return H(i, function(s) {
        return {
          coord: s.coord,
          tickValue: s.tick.value,
          onBand: o
        };
      });
    }, t.prototype.getMinorTicksCoords = function() {
      if (yr(this.scale))
        return [];
      var e = this.model.getModel("minorTick"), r = e.get("splitNumber");
      r > 0 && r < 100 || (r = 5);
      var n = this.scale.getMinorTicks(r), i = H(n, function(a) {
        return H(a, function(o) {
          return {
            coord: this.dataToCoord(o),
            tickValue: o
          };
        }, this);
      }, this);
      return i;
    }, t.prototype.getViewLabels = function(e) {
      return e = e || Rs(kr.determine), pP(this, e).labels;
    }, t.prototype.getLabelModel = function() {
      return this.model.getModel("axisLabel");
    }, t.prototype.getTickModel = function() {
      return this.model.getModel("axisTick");
    }, t.prototype.getBandWidth = function() {
      return _u(this, {
        min: 1
      }).w;
    }, t.prototype.calculateCategoryInterval = function(e) {
      return e = e || Rs(kr.determine), wP(this, e);
    }, t;
  })()
);
function $p(t) {
  var e = t.getExtent();
  if (t.onBand) {
    var r = e[1] - e[0], n = r / t.scale.count() / 2;
    e[0] += n, e[1] -= n;
  }
  return e;
}
function PP(t, e, r) {
  var n = e.length;
  if (!t.onBand || r || !n)
    return !1;
  var i = _u(t).w;
  if (!i)
    return !1;
  w(e, function(s) {
    s.coord -= i / 2;
  });
  var a = t.scale.getExtent(), o = e[n - 1];
  return o.tick.offInterval && e.pop(), e.push({
    coord: o.coord + i,
    tick: {
      value: a[1] + 1
    }
  }), !0;
}
function LP(t) {
  var e = ae.extend(t);
  return ae.registerClass(e), e;
}
function RP(t) {
  var e = lt.extend(t);
  return lt.registerClass(e), e;
}
function EP(t) {
  var e = $t.extend(t);
  return $t.registerClass(e), e;
}
function OP(t) {
  var e = Wt.extend(t);
  return Wt.registerClass(e), e;
}
var Zp = ["label", "labelLine", "layoutOption", "priority", "defaultAttr", "marginForce", "minMarginForce", "marginDefault", "suggestIgnore"], kP = 1, Os = 2, R0 = kP | Os;
function ks(t, e, r) {
  r = r || R0, e ? t.dirty |= r : t.dirty &= ~r;
}
function E0(t, e) {
  return e = e || R0, t.dirty == null || !!(t.dirty & e);
}
function Zt(t) {
  if (t)
    return E0(t) && BP(t, t.label, t), t;
}
function BP(t, e, r) {
  var n = e.getComputedTransform();
  t.transform = Vh(t.transform, n);
  var i = t.localRect = Sa(t.localRect, e.getBoundingRect()), a = e.style, o = a.margin, s = r && r.marginForce, u = r && r.minMarginForce, l = r && r.marginDefault, f = a.__marginType;
  f == null && l && (o = l, f = Jn.textMargin);
  for (var h = 0; h < 4; h++)
    El[h] = f === Jn.minMargin && u && u[h] != null ? u[h] : s && s[h] != null ? s[h] : o ? o[h] : 0;
  f === Jn.textMargin && Af(i, El, !1, !1);
  var c = t.rect = Sa(t.rect, i);
  return n && c.applyTransform(n), f === Jn.minMargin && Af(c, El, !1, !1), t.axisAligned = Hh(n), (t.label = t.label || {}).ignore = e.ignore, ks(t, !1), ks(t, !0, Os), t;
}
var El = [0, 0, 0, 0];
function NP(t, e, r) {
  return t.transform = Vh(t.transform, r), t.localRect = Sa(t.localRect, e), t.rect = Sa(t.rect, e), r && t.rect.applyTransform(r), t.axisAligned = Hh(r), t.obb = void 0, (t.label = t.label || {}).ignore = !1, t;
}
function FP(t, e) {
  if (t) {
    t.label.x += e.x, t.label.y += e.y, t.label.markRedraw();
    var r = t.transform;
    r && (r[4] += e.x, r[5] += e.y);
    var n = t.rect;
    n && (n.x += e.x, n.y += e.y);
    var i = t.obb;
    i && i.fromBoundingRect(t.localRect, r);
  }
}
function Kp(t, e) {
  for (var r = 0; r < Zp.length; r++) {
    var n = Zp[r];
    t[n] == null && (t[n] = e[n]);
  }
  return Zt(t);
}
function jp(t) {
  var e = t.obb;
  return (!e || E0(t, Os)) && (t.obb = e = e || new sm(), e.fromBoundingRect(t.localRect, t.transform), ks(t, !1, Os)), e;
}
function e2(t, e, r, n, i) {
  var a = t.length, o = hm[e], s = cm[e];
  if (a < 2)
    return !1;
  t.sort(function(C, M) {
    return C.rect[o] - M.rect[o];
  });
  for (var u = 0, l, f = !1, h = 0; h < a; h++) {
    var c = t[h], v = c.rect;
    l = v[o] - u, l < 0 && (v[o] -= l, c.label[o] -= l, f = !0), u = v[o] + v[s];
  }
  var d = t[0], y = t[a - 1], p, g;
  m(), p < 0 && b(-p, 0.8), g < 0 && b(g, 0.8), m(), _(p, g, 1), _(g, p, -1), m(), p < 0 && T(-p), g < 0 && T(g);
  function m() {
    p = d.rect[o] - r, g = n - y.rect[o] - y.rect[s];
  }
  function _(C, M, x) {
    if (C < 0) {
      var D = Math.min(M, -C);
      if (D > 0) {
        S(D * x, 0, a);
        var I = D + C;
        I < 0 && b(-I * x, 1);
      } else
        b(-C * x, 1);
    }
  }
  function S(C, M, x) {
    C !== 0 && (f = !0);
    for (var D = M; D < x; D++) {
      var I = t[D], L = I.rect;
      L[o] += C, I.label[o] += C;
    }
  }
  function b(C, M) {
    for (var x = [], D = 0, I = 1; I < a; I++) {
      var L = t[I - 1].rect, A = Math.max(t[I].rect[o] - L[o] - L[s], 0);
      x.push(A), D += A;
    }
    if (D) {
      var P = Math.min(Math.abs(C) / D, M);
      if (C > 0)
        for (var I = 0; I < a - 1; I++) {
          var k = x[I] * P;
          S(k, 0, I + 1);
        }
      else
        for (var I = a - 1; I > 0; I--) {
          var k = x[I - 1] * P;
          S(-k, I, a);
        }
    }
  }
  function T(C) {
    var M = C < 0 ? -1 : 1;
    C = Math.abs(C);
    for (var x = Math.ceil(C / (a - 1)), D = 0; D < a - 1; D++)
      if (M > 0 ? S(x, 0, D + 1) : S(-x, a - D - 1, a), C -= x, C <= 0)
        return;
  }
  return f;
}
function t2(t) {
  for (var e = 0; e < t.length; e++) {
    var r = t[e], n = r.defaultAttr, i = r.labelLine;
    r.label.attr("ignore", n.ignore), i && i.attr("ignore", n.labelGuideIgnore);
  }
}
function zP(t) {
  var e = [];
  t.sort(function(l, f) {
    return (f.suggestIgnore ? 1 : 0) - (l.suggestIgnore ? 1 : 0) || f.priority - l.priority;
  });
  function r(l) {
    if (!l.ignore) {
      var f = l.ensureState("emphasis");
      f.ignore == null && (f.ignore = !1);
    }
    l.ignore = !0;
  }
  for (var n = 0; n < t.length; n++) {
    var i = Zt(t[n]);
    if (!i.label.ignore) {
      for (var a = i.label, o = i.labelLine, s = !1, u = 0; u < e.length; u++)
        if (Lc(i, e[u], null, {
          touchThreshold: 0.05
        })) {
          s = !0;
          break;
        }
      s ? (r(a), o && r(o)) : e.push(i);
    }
  }
}
function Lc(t, e, r, n) {
  return !t || !e || t.label && t.label.ignore || e.label && e.label.ignore || !t.rect.intersect(e.rect, r, n) ? !1 : t.axisAligned && e.axisAligned ? !0 : jp(t).intersect(jp(e), r, n);
}
const r2 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Axis: AP,
  ChartView: Wt,
  ComponentModel: ae,
  ComponentView: lt,
  List: t0,
  Model: ce,
  PRIORITY: k_,
  SeriesModel: $t,
  color: ib,
  connect: TI,
  dataTool: PI,
  dependencies: iI,
  disConnect: wI,
  disconnect: U_,
  dispose: CI,
  env: W,
  extendChartView: OP,
  extendComponentModel: LP,
  extendComponentView: RP,
  extendSeriesModel: EP,
  format: cP,
  getCoordinateSystemDimensions: MI,
  getInstanceByDom: pc,
  getInstanceById: xI,
  getMap: AI,
  graphic: hP,
  helper: rP,
  init: bI,
  innerDrawElementOnCanvas: hc,
  matrix: A1,
  number: lP,
  parseGeoJSON: Xp,
  parseGeoJson: Xp,
  registerAction: Ln,
  registerCoordinateSystem: X_,
  registerCustomSeries: DI,
  registerLayout: q_,
  registerLoading: Sc,
  registerLocale: Wh,
  registerMap: $_,
  registerPostInit: Y_,
  registerPostUpdate: W_,
  registerPreprocessor: yc,
  registerProcessor: mc,
  registerTheme: gc,
  registerTransform: Z_,
  registerUpdateLifecycle: pu,
  registerVisual: Fr,
  setCanvasCreator: II,
  setPlatformAPI: Dg,
  throttle: fc,
  time: fP,
  use: vi,
  util: vP,
  vector: c1,
  version: nI,
  zrUtil: r1,
  zrender: Nb
}, Symbol.toStringTag, { value: "Module" }));
var HP = null;
function O0() {
  return HP;
}
var VP = "expandAxisBreak", Ir = Math.PI, GP = [[1, 2, 1, 2], [5, 3, 5, 3], [8, 3, 8, 3]], UP = [[0, 1, 0, 1], [0, 3, 0, 3], [0, 3, 0, 3]], di = de(), k0 = de(), YP = (
  /** @class */
  (function() {
    function t(e) {
      this.recordMap = {}, this.resolveAxisNameOverlap = e;
    }
    return t.prototype.ensureRecord = function(e) {
      var r = e.axis.dim, n = e.componentIndex, i = this.recordMap, a = i[r] || (i[r] = []);
      return a[n] || (a[n] = {
        ready: {}
      });
    }, t;
  })()
);
function WP(t, e, r, n) {
  var i = r.axis, a = e.ensureRecord(r), o = [], s, u = Rc(t.axisName) && Ls(t.nameLocation);
  w(n, function(d) {
    var y = Zt(d);
    if (!(!y || y.label.ignore)) {
      o.push(y);
      var p = a.transGroup;
      u && (p.transform ? gi(Ui, p.transform) : Mn(Ui), y.transform && mn(Ui, Ui, y.transform), Y.copy(Io, y.localRect), Io.applyTransform(Ui), s ? s.union(Io) : Y.copy(s = new Y(0, 0, 0, 0), Io));
    }
  });
  var l = Math.abs(a.dirVec.x) > 0.1 ? "x" : "y", f = a.transGroup[l];
  if (o.sort(function(d, y) {
    return Math.abs(d.label[l] - f) - Math.abs(y.label[l] - f);
  }), u && s) {
    var h = i.getExtent(), c = Math.min(h[0], h[1]), v = Math.max(h[0], h[1]) - c;
    s.union(new Y(c, 0, v, 1));
  }
  a.stOccupiedRect = s, a.labelInfoList = o;
}
var Ui = Tt(), Io = new Y(0, 0, 0, 0), XP = function(t, e, r, n, i, a) {
  if (Ls(t.nameLocation)) {
    var o = a.stOccupiedRect;
    o && B0(NP({}, o, a.transGroup.transform), n, i);
  } else
    qP(a.labelInfoList, a.dirVec, n, i);
};
function B0(t, e, r) {
  var n = new ue();
  Lc(t, e, n, {
    direction: Math.atan2(r.y, r.x),
    bidirectional: !1,
    touchThreshold: 0.05
  }) && FP(e, n);
}
function qP(t, e, r, n) {
  for (var i = ue.dot(n, e) >= 0, a = 0, o = t.length; a < o; a++) {
    var s = t[i ? a : o - 1 - a];
    s.label.ignore || B0(s, r, n);
  }
}
var Rr = (
  /** @class */
  (function() {
    function t(e, r, n, i) {
      this.group = new ut(), this._axisModel = e, this._api = r, this._local = {}, this._shared = i || new YP(XP), this._resetCfgDetermined(n);
    }
    return t.prototype.updateCfg = function(e) {
      var r = this._cfg.raw;
      r.position = e.position, r.labelOffset = e.labelOffset, this._resetCfgDetermined(r);
    }, t.prototype.__getRawCfg = function() {
      return this._cfg.raw;
    }, t.prototype._resetCfgDetermined = function(e) {
      var r = this._axisModel, n = r.getDefaultOption ? r.getDefaultOption() : {}, i = G(e.axisName, r.get("name")), a = r.get("nameMoveOverlap");
      (a == null || a === "auto") && (a = G(e.defaultNameMoveOverlap, !0));
      var o = {
        raw: e,
        position: e.position,
        rotation: e.rotation,
        nameDirection: G(e.nameDirection, 1),
        tickDirection: G(e.tickDirection, 1),
        labelDirection: G(e.labelDirection, 1),
        labelOffset: G(e.labelOffset, 0),
        silent: G(e.silent, !0),
        axisName: i,
        nameLocation: gn(r.get("nameLocation"), n.nameLocation, "end"),
        shouldNameMoveOverlap: Rc(i) && a,
        optionHideOverlap: r.get(["axisLabel", "hideOverlap"]),
        showMinorTicks: r.get(["minorTick", "show"])
      };
      this._cfg = o;
      var s = new ut({
        x: o.position[0],
        y: o.position[1],
        rotation: o.rotation
      });
      s.updateTransform(), this._transformGroup = s;
      var u = this._shared.ensureRecord(r);
      u.transGroup = this._transformGroup, u.dirVec = new ue(Math.cos(-o.rotation), Math.sin(-o.rotation));
    }, t.prototype.build = function(e, r) {
      var n = this;
      return e || (e = {
        axisLine: !0,
        axisTickLabelEstimate: !1,
        axisTickLabelDetermine: !0,
        axisName: !0
      }), w($P, function(i) {
        e[i] && ZP[i](n._cfg, n._local, n._shared, n._axisModel, n.group, n._transformGroup, n._api, r || {});
      }), this;
    }, t.innerTextLayout = function(e, r, n) {
      var i = _h(r - e), a, o;
      return ya(i) ? (o = n > 0 ? "top" : "bottom", a = "center") : ya(i - Ir) ? (o = n > 0 ? "bottom" : "top", a = "center") : (o = "middle", i > 0 && i < Ir ? a = n > 0 ? "right" : "left" : a = n > 0 ? "left" : "right"), {
        rotation: i,
        textAlign: a,
        textVerticalAlign: o
      };
    }, t.makeAxisEventDataBase = function(e) {
      var r = {
        componentType: e.mainType,
        componentIndex: e.componentIndex
      };
      return r[e.mainType + "Index"] = e.componentIndex, r;
    }, t.isLabelSilent = function(e) {
      var r = e.get("tooltip");
      return e.get("silent") || !(e.get("triggerEvent") || r && r.show);
    }, t;
  })()
), $P = ["axisLine", "axisTickLabelEstimate", "axisTickLabelDetermine", "axisName"], ZP = {
  axisLine: function(t, e, r, n, i, a, o) {
    var s = n.get(["axisLine", "show"]);
    if (s === "auto" && (s = !0, t.raw.axisLineAutoShow != null && (s = !!t.raw.axisLineAutoShow)), !!s) {
      var u = n.axis.getExtent(), l = a.transform, f = [u[0], 0], h = [u[1], 0], c = f[0] > h[0];
      l && (ot(f, f, l), ot(h, h, l));
      var v = E({
        lineCap: "round"
      }, n.getModel(["axisLine", "lineStyle"]).getLineStyle()), d = {
        strokeContainThreshold: t.raw.strokeContainThreshold || 5,
        silent: !0,
        z2: 1,
        style: v
      };
      if (n.get(["axisLine", "breakLine"]) && sx(n.axis.scale))
        O0().buildAxisBreakLine(n, i, a, d);
      else {
        var y = new Pn(E({
          shape: {
            x1: f[0],
            y1: f[1],
            x2: h[0],
            y2: h[1]
          }
        }, d));
        Fh(y.shape, y.style.lineWidth), y.anid = "line", i.add(y);
      }
      var p = n.get(["axisLine", "symbol"]);
      if (p != null) {
        var g = n.get(["axisLine", "symbolSize"]);
        N(p) && (p = [p, p]), (N(g) || ve(g)) && (g = [g, g]);
        var m = zD(n.get(["axisLine", "symbolOffset"]) || 0, g), _ = g[0], S = g[1];
        w([{
          rotate: t.rotation + Math.PI / 2,
          offset: m[0],
          r: 0
        }, {
          rotate: t.rotation - Math.PI / 2,
          offset: m[1],
          r: Math.sqrt((f[0] - h[0]) * (f[0] - h[0]) + (f[1] - h[1]) * (f[1] - h[1]))
        }], function(b, T) {
          if (p[T] !== "none" && p[T] != null) {
            var C = vu(p[T], -_ / 2, -S / 2, _, S, v.stroke, !0), M = b.r + b.offset, x = c ? h : f;
            C.attr({
              rotation: b.rotate,
              x: x[0] + M * Math.cos(t.rotation),
              y: x[1] - M * Math.sin(t.rotation),
              silent: !0,
              z2: 11
            }), i.add(C);
          }
        });
      }
    }
  },
  /**
   * [CAUTION] This method can be called multiple times, following the change due to `resetCfg` called
   *  in size measurement. Thus this method should be idempotent, and should be performant.
   */
  axisTickLabelEstimate: function(t, e, r, n, i, a, o, s) {
    var u = Jp(e, i, s);
    u && Qp(t, e, r, n, i, a, o, kr.estimate);
  },
  /**
   * Finish axis tick label build.
   * Can be only called once.
   */
  axisTickLabelDetermine: function(t, e, r, n, i, a, o, s) {
    var u = Jp(e, i, s);
    u && Qp(t, e, r, n, i, a, o, kr.determine);
    var l = JP(t, i, a, n);
    QP(t, e.labelLayoutList, l), eL(t, i, a, n, t.tickDirection);
  },
  /**
   * [CAUTION] This method can be called multiple times, following the change due to `resetCfg` called
   *  in size measurement. Thus this method should be idempotent, and should be performant.
   */
  axisName: function(t, e, r, n, i, a, o, s) {
    var u = r.ensureRecord(n);
    e.nameEl && (i.remove(e.nameEl), e.nameEl = u.nameLayout = u.nameLocation = null);
    var l = t.axisName;
    if (Rc(l)) {
      var f = t.nameLocation, h = t.nameDirection, c = n.getModel("nameTextStyle"), v = n.get("nameGap") || 0, d = n.axis.getExtent(), y = n.axis.inverse ? -1 : 1, p = new ue(0, 0), g = new ue(0, 0);
      f === "start" ? (p.x = d[0] - y * v, g.x = -y) : f === "end" ? (p.x = d[1] + y * v, g.x = y) : (p.x = (d[0] + d[1]) / 2, p.y = t.labelOffset + h * v, g.y = h);
      var m = Tt();
      g.transform(Vs(m, m, t.rotation));
      var _ = n.get("nameRotate");
      _ != null && (_ = _ * Ir / 180);
      var S, b;
      Ls(f) ? S = Rr.innerTextLayout(
        t.rotation,
        _ ?? t.rotation,
        // Adapt to axis.
        h
      ) : (S = KP(t.rotation, f, _ || 0, d), b = t.raw.axisNameAvailableWidth, b != null && (b = Math.abs(b / Math.sin(S.rotation)), !isFinite(b) && (b = null)));
      var T = c.getFont(), C = n.get("nameTruncate", !0) || {}, M = C.ellipsis, x = ca(t.raw.nameTruncateMaxWidth, C.maxWidth, b), D = s.nameMarginLevel || 0, I = new je({
        x: p.x,
        y: p.y,
        rotation: S.rotation,
        silent: Rr.isLabelSilent(n),
        style: Cn(c, {
          text: l,
          font: T,
          overflow: "truncate",
          width: x,
          ellipsis: M,
          fill: c.getTextColor() || n.get(["axisLine", "lineStyle", "color"]),
          align: c.get("align") || S.textAlign,
          verticalAlign: c.get("verticalAlign") || S.textVerticalAlign
        }),
        z2: 1
      });
      if (su({
        el: I,
        componentModel: n,
        itemName: l
      }), I.__fullText = l, I.anid = "name", n.get("triggerEvent")) {
        var L = Rr.makeAxisEventDataBase(n);
        L.targetType = "axisName", L.name = l, he(I).eventData = L;
      }
      a.add(I), I.updateTransform(), e.nameEl = I;
      var A = u.nameLayout = Zt({
        label: I,
        priority: I.z2,
        defaultAttr: {
          ignore: I.ignore
        },
        marginDefault: Ls(f) ? GP[D] : UP[D]
      });
      if (u.nameLocation = f, i.add(I), I.decomposeTransform(), t.shouldNameMoveOverlap && A) {
        var P = r.ensureRecord(n);
        r.resolveAxisNameOverlap(t, r, n, A, g, P);
      }
    }
  }
};
function Qp(t, e, r, n, i, a, o, s) {
  F0(e) || tL(t, e, i, s, n, o);
  var u = e.labelLayoutList;
  rL(t, n, u, a), aL(n, t.rotation, u);
  var l = t.optionHideOverlap;
  jP(n, u, l), l && zP(
    // Filter the already ignored labels by the previous overlap resolving methods.
    xe(u, function(f) {
      return f && !f.label.ignore;
    })
  ), WP(t, r, n, u);
}
function KP(t, e, r, n) {
  var i = _h(r - t), a, o, s = n[0] > n[1], u = e === "start" && !s || e !== "start" && s;
  return ya(i - Ir / 2) ? (o = u ? "bottom" : "top", a = "center") : ya(i - Ir * 1.5) ? (o = u ? "top" : "bottom", a = "center") : (o = "middle", i < Ir * 1.5 && i > Ir / 2 ? a = u ? "left" : "right" : a = u ? "right" : "left"), {
    rotation: i,
    textAlign: a,
    textVerticalAlign: o
  };
}
function jP(t, e, r) {
  var n = t.axis, i = t.get(["axisLabel", "customValues"]);
  if (xA(n))
    return;
  function a(l, f, h) {
    var c = Zt(e[f]), v = Zt(e[h]), d = n.scale;
    if (!(!c || !v)) {
      if (l == null) {
        if (!r && i)
          return;
        var y = di(c.label).labelInfo.tick;
        if (
          // TimeScale does not expand extent to "nice", so eliminate labels that are not nice.
          wc(d) && y.notNice || yr(d) && y.offInterval
        ) {
          Zn(c.label);
          return;
        }
      }
      if (l === !1 || c.suggestIgnore) {
        Zn(c.label);
        return;
      }
      if (v.suggestIgnore) {
        Zn(v.label);
        return;
      }
      var p = 0.1;
      if (!r) {
        var g = [0, 0, 0, 0];
        c = Kp({
          marginForce: g
        }, c), v = Kp({
          marginForce: g
        }, v);
      }
      Lc(c, v, null, {
        touchThreshold: p
      }) && Zn(l ? v.label : c.label);
    }
  }
  var o = t.get(["axisLabel", "showMinLabel"]), s = t.get(["axisLabel", "showMaxLabel"]), u = e.length;
  a(o, 0, 1), a(s, u - 1, u - 2);
}
function QP(t, e, r) {
  t.showMinorTicks || w(e, function(n) {
    if (n && n.label.ignore)
      for (var i = 0; i < r.length; i++) {
        var a = r[i], o = k0(a), s = di(n.label);
        if (o.tickValue != null && !o.onBand && o.tickValue === s.labelInfo.tick.value) {
          Zn(a);
          return;
        }
      }
  });
}
function Zn(t) {
  t && (t.ignore = !0);
}
function N0(t, e, r, n, i) {
  for (var a = [], o = [], s = [], u = 0; u < t.length; u++) {
    var l = t[u].coord;
    o[0] = l, o[1] = 0, s[0] = l, s[1] = r, e && (ot(o, o, e), ot(s, s, e));
    var f = new Pn({
      shape: {
        x1: o[0],
        y1: o[1],
        x2: s[0],
        y2: s[1]
      },
      style: n,
      z2: 2,
      autoBatch: !0,
      silent: !0
    });
    Fh(f.shape, f.style.lineWidth), f.anid = i + "_" + t[u].tickValue, a.push(f);
    var h = k0(f);
    h.onBand = !!t[u].onBand, h.tickValue = t[u].tickValue;
  }
  return a;
}
function JP(t, e, r, n) {
  var i = n.axis, a = n.getModel("axisTick"), o = a.get("show");
  if (o === "auto" && (o = !0, t.raw.axisTickAutoShow != null && (o = !!t.raw.axisTickAutoShow)), !o || i.scale.isBlank())
    return [];
  for (var s = a.getModel("lineStyle"), u = t.tickDirection * a.get("length"), l = i.getTicksCoords(), f = N0(l, r.transform, u, _e(s.getLineStyle(), {
    stroke: n.get(["axisLine", "lineStyle", "color"])
  }), "ticks"), h = 0; h < f.length; h++)
    e.add(f[h]);
  return f;
}
function eL(t, e, r, n, i) {
  var a = n.axis, o = n.getModel("minorTick");
  if (!(!t.showMinorTicks || a.scale.isBlank())) {
    var s = a.getMinorTicksCoords();
    if (s.length)
      for (var u = o.getModel("lineStyle"), l = i * o.get("length"), f = _e(u.getLineStyle(), _e(n.getModel("axisTick").getLineStyle(), {
        stroke: n.get(["axisLine", "lineStyle", "color"])
      })), h = 0; h < s.length; h++)
        for (var c = N0(s[h], r.transform, l, f, "minorticks_" + h), v = 0; v < c.length; v++)
          e.add(c[v]);
  }
}
function Jp(t, e, r) {
  if (F0(t)) {
    var n = t.axisLabelsCreationContext, i = n.out.noPxChangeTryDetermine;
    if (r.noPxChange) {
      for (var a = !0, o = 0; o < i.length; o++)
        a = a && i[o]();
      if (a)
        return !1;
    }
    i.length && (e.remove(t.labelGroup), Qf(t, null, null, null));
  }
  return !0;
}
function tL(t, e, r, n, i, a) {
  var o = i.axis, s = ca(t.raw.axisLabelShow, i.get(["axisLabel", "show"])), u = new ut();
  r.add(u);
  var l = Rs(n);
  if (!s || o.scale.isBlank()) {
    Qf(e, [], u, l);
    return;
  }
  var f = i.getModel("axisLabel"), h = o.getViewLabels(l), c = (ca(t.raw.labelRotate, f.get("rotate")) || 0) * Ir / 180, v = Rr.innerTextLayout(t.rotation, c, t.labelDirection), d = i.getCategories && i.getCategories(!0), y = [], p = i.get("triggerEvent"), g = 1 / 0, m = -1 / 0;
  w(h, function(S, b) {
    var T, C = S.tick, M = S.formattedLabel, x = S.rawLabel, D = f, I = Dc(o.scale, C);
    if (d && d[I]) {
      var L = d[I];
      z(L) && L.textStyle && (D = new ce(L.textStyle, f, i.ecModel));
    }
    var A = D.getTextColor() || i.get(["axisLine", "lineStyle", "color"]), P = D.getShallow("align", !0) || v.textAlign, k = G(D.getShallow("alignMinLabel", !0), P), O = G(D.getShallow("alignMaxLabel", !0), P), V = D.getShallow("verticalAlign", !0) || D.getShallow("baseline", !0) || v.textVerticalAlign, K = G(D.getShallow("verticalAlignMinLabel", !0), V), J = G(D.getShallow("verticalAlignMaxLabel", !0), V), ee = 10 + (((T = C.time) === null || T === void 0 ? void 0 : T.level) || 0);
    g = Math.min(g, ee), m = Math.max(m, ee);
    var Q = new je({
      // --- transform props start ---
      // All of the transform props MUST not be set here, but should be set in
      // `updateAxisLabelChangableProps`, because they may change in estimation,
      // and need to calculate based on global coord sys by `decomposeTransform`.
      x: 0,
      y: 0,
      rotation: 0,
      // --- transform props end ---
      silent: Rr.isLabelSilent(i),
      z2: ee,
      style: Cn(D, {
        text: M,
        align: b === 0 ? k : b === h.length - 1 ? O : P,
        verticalAlign: b === 0 ? K : b === h.length - 1 ? J : V,
        fill: X(A) ? A(
          // (1) In category axis with data zoom, tick is not the original
          // index of axis.data. So tick should not be exposed to user
          // in category axis.
          // (2) Compatible with previous version, which always use formatted label as
          // input. But in interval scale the formatted label is like '223,445', which
          // maked user replace ','. So we modify it to return original val but remain
          // it as 'string' to avoid error in replacing.
          o.type === "category" ? x : o.type === "value" ? I + "" : I,
          b
        ) : A
      })
    });
    Q.anid = "label_" + I;
    var re = di(Q);
    if (re.labelInfo = S, re.layoutRotation = v.rotation, su({
      el: Q,
      componentModel: i,
      itemName: M,
      formatterParamsExtra: {
        isTruncated: function() {
          return Q.isTruncated;
        },
        value: x,
        tickIndex: b
      }
    }), p) {
      var ne = Rr.makeAxisEventDataBase(i);
      ne.targetType = "axisLabel", ne.value = x, ne.tickIndex = b;
      var q = S.tick.break;
      if (q) {
        var se = q.parsedBreak;
        ne.break = {
          // type: labelItem.break.type,
          start: se.vmin,
          end: se.vmax
        };
      }
      o.type === "category" && (ne.dataIndex = I), he(Q).eventData = ne, q && iL(i, a, Q, q);
    }
    y.push(Q), u.add(Q);
  });
  var _ = H(y, function(S) {
    return {
      label: S,
      priority: di(S).labelInfo.tick.break ? S.z2 + (m - g + 1) : S.z2,
      defaultAttr: {
        ignore: S.ignore
      }
    };
  });
  Qf(e, _, u, l);
}
function F0(t) {
  return !!t.labelLayoutList;
}
function Qf(t, e, r, n) {
  t.labelLayoutList = e, t.labelGroup = r, t.axisLabelsCreationContext = n;
}
function rL(t, e, r, n) {
  var i = e.get(["axisLabel", "margin"]);
  w(r, function(a, o) {
    var s = Zt(a);
    if (s) {
      var u = s.label, l = di(u);
      s.suggestIgnore = u.ignore, u.ignore = !1, us(ar, nL);
      var f = e.axis;
      ar.x = f.dataToCoord(Dc(f.scale, l.labelInfo.tick)), ar.y = t.labelOffset + t.labelDirection * i, ar.rotation = l.layoutRotation, n.add(ar), ar.updateTransform(), n.remove(ar), ar.decomposeTransform(), us(u, ar), u.markRedraw(), ks(s, !0), Zt(s);
    }
  });
}
var ar = new Ee(), nL = new Ee();
function Rc(t) {
  return !!t;
}
function iL(t, e, r, n) {
  r.on("click", function(i) {
    var a = {
      type: VP,
      breaks: [{
        start: n.parsedBreak.breakOption.start,
        end: n.parsedBreak.breakOption.end
      }]
    };
    a[t.axis.dim + "AxisIndex"] = t.componentIndex, e.dispatchAction(a);
  });
}
function aL(t, e, r) {
  var n = Xe();
  if (n) {
    var i = n.retrieveAxisBreakPairs(r, function(o) {
      return o && di(o.label).labelInfo.tick.break;
    }, !0), a = t.get(["breakLabelLayout", "moveOverlap"], !0);
    (a === !0 || a === "auto") && w(i, function(o) {
      O0().adjustBreakLabelPair(t.axis.inverse, e, [Zt(r[o[0]]), Zt(r[o[1]])]);
    });
  }
}
function Bs(t, e, r) {
  r = r || {};
  var n = e.axis, i = {}, a = n.getAxesOnZeroOf()[0], o = n.position, s = a ? "onZero" : o, u = n.dim, l = [t.x, t.x + t.width, t.y, t.y + t.height], f = {
    left: 0,
    right: 1,
    top: 0,
    bottom: 1,
    onZero: 2
  }, h = e.get("offset") || 0, c = u === "x" ? [l[2] - h, l[3] + h] : [l[0] - h, l[1] + h];
  if (a) {
    var v = a.toGlobalCoord(a.dataToCoord(0));
    c[f.onZero] = Math.max(Math.min(v, c[1]), c[0]);
  }
  i.position = [u === "y" ? c[f[s]] : l[0], u === "x" ? c[f[s]] : l[3]], i.rotation = Math.PI / 2 * (u === "x" ? 0 : 1);
  var d = {
    top: -1,
    bottom: 1,
    left: -1,
    right: 1
  };
  i.labelDirection = i.tickDirection = i.nameDirection = d[o], i.labelOffset = a ? c[f[o]] - c[f.onZero] : 0, e.get(["axisTick", "inside"]) && (i.tickDirection = -i.tickDirection), ca(r.labelInside, e.get(["axisLabel", "inside"])) && (i.labelDirection = -i.labelDirection);
  var y = e.get(["axisLabel", "rotate"]);
  return i.labelRotate = s === "top" ? -y : y, i.z2 = 1, i;
}
function n2(t) {
  return t.coordinateSystem && t.coordinateSystem.type === "cartesian2d";
}
function i2(t) {
  var e = {
    xAxisModel: null,
    yAxisModel: null
  };
  return w(e, function(r, n) {
    var i = n.replace(/Model$/, ""), a = t.getReferringComponents(i, ur).models[0];
    e[n] = a;
  }), e;
}
function a2(t, e, r, n, i, a) {
  for (var o = Bs(t, r), s = !1, u = !1, l = 0; l < e.length; l++)
    oA(e[l].getOtherAxis(r.axis).scale) && (s = u = !0, r.axis.type === "category" && r.axis.onBand && (u = !1));
  return o.axisLineAutoShow = s, o.axisTickAutoShow = u, o.defaultNameMoveOverlap = a, new Rr(r, n, o, i);
}
function o2(t, e, r) {
  var n = Bs(e, r);
  t.updateCfg(n);
}
var s2 = (
  /** @class */
  (function() {
    function t(e) {
      this.type = "cartesian", this._dimList = [], this._axes = {}, this.name = e || "";
    }
    return t.prototype.getAxis = function(e) {
      return this._axes[e];
    }, t.prototype.getAxes = function() {
      return H(this._dimList, function(e) {
        return this._axes[e];
      }, this);
    }, t.prototype.getAxesByScale = function(e) {
      return e = e.toLowerCase(), xe(this.getAxes(), function(r) {
        return r.scale.type === e;
      });
    }, t.prototype.addAxis = function(e) {
      var r = e.dim;
      this._axes[r] = e, this._dimList.push(r);
    }, t;
  })()
);
function oL(t, e) {
  var r = {
    /**
     * key: makeKey(axis.model)
     * value: {
     *      axis,
     *      coordSys,
     *      axisPointerModel,
     *      triggerTooltip,
     *      triggerEmphasis,
     *      involveSeries,
     *      snap,
     *      seriesModels,
     *      seriesDataCount
     * }
     */
    axesInfo: {},
    seriesInvolved: !1,
    /**
     * key: makeKey(coordSys.model)
     * value: Object: key makeKey(axis.model), value: axisInfo
     */
    coordSysAxesInfo: {},
    coordSysMap: {}
  };
  return sL(r, t, e), r.seriesInvolved && lL(r, t), r;
}
function sL(t, e, r) {
  var n = e.getComponent("tooltip"), i = e.getComponent("axisPointer"), a = i.get("link", !0) || [], o = [];
  w(r.getCoordinateSystems(), function(s) {
    if (!s.axisPointerEnabled)
      return;
    var u = Ia(s.model), l = t.coordSysAxesInfo[u] = {};
    t.coordSysMap[u] = s;
    var f = s.model, h = f.getModel("tooltip", n);
    if (w(s.getAxes(), we(y, !1, null)), s.getTooltipAxes && n && h.get("show")) {
      var c = h.get("trigger") === "axis", v = h.get(["axisPointer", "type"]) === "cross", d = s.getTooltipAxes(h.get(["axisPointer", "axis"]));
      (c || v) && w(d.baseAxes, we(y, v ? "cross" : !0, c)), v && w(d.otherAxes, we(y, "cross", !1));
    }
    function y(p, g, m) {
      var _ = m.model.getModel("axisPointer", i), S = _.get("show");
      if (!(!S || S === "auto" && !p && !Jf(_))) {
        g == null && (g = _.get("triggerTooltip")), _ = p ? uL(m, h, i, e, p, g) : _;
        var b = _.get("snap"), T = _.get("triggerEmphasis"), C = Ia(m.model), M = g || b || m.type === "category", x = t.axesInfo[C] = {
          key: C,
          axis: m,
          coordSys: s,
          axisPointerModel: _,
          triggerTooltip: g,
          triggerEmphasis: T,
          involveSeries: M,
          snap: b,
          useHandle: Jf(_),
          seriesModels: [],
          linkGroup: null
        };
        l[C] = x, t.seriesInvolved = t.seriesInvolved || M;
        var D = fL(a, m);
        if (D != null) {
          var I = o[D] || (o[D] = {
            axesInfo: {}
          });
          I.axesInfo[C] = x, I.mapper = a[D].mapper, x.linkGroup = I;
        }
      }
    }
  });
}
function uL(t, e, r, n, i, a) {
  var o = e.getModel("axisPointer"), s = ["type", "snap", "lineStyle", "shadowStyle", "label", "animation", "animationDurationUpdate", "animationEasingUpdate", "z"], u = {};
  w(s, function(c) {
    u[c] = Z(o.get(c));
  }), u.snap = t.type !== "category" && !!a, o.get("type") === "cross" && (u.type = "line");
  var l = u.label || (u.label = {});
  if (l.show == null && (l.show = !1), i === "cross") {
    var f = o.get(["label", "show"]);
    if (l.show = f ?? !0, !a) {
      var h = u.lineStyle = o.get("crossStyle");
      h && _e(l, h.textStyle);
    }
  }
  return t.model.getModel("axisPointer", new ce(u, r, n));
}
function lL(t, e) {
  e.eachSeries(function(r) {
    var n = r.coordinateSystem, i = r.get(["tooltip", "trigger"], !0), a = r.get(["tooltip", "show"], !0);
    !n || !n.model || i === "none" || i === !1 || i === "item" || a === !1 || r.get(["axisPointer", "show"], !0) === !1 || w(t.coordSysAxesInfo[Ia(n.model)], function(o) {
      var s = o.axis;
      n.getAxis(s.dim) === s && (o.seriesModels.push(r), o.seriesDataCount == null && (o.seriesDataCount = 0), o.seriesDataCount += r.getData().count());
    });
  });
}
function fL(t, e) {
  for (var r = e.model, n = e.dim, i = 0; i < t.length; i++) {
    var a = t[i] || {};
    if (Ol(a[n + "AxisId"], r.id) || Ol(a[n + "AxisIndex"], r.componentIndex) || Ol(a[n + "AxisName"], r.name))
      return i;
  }
}
function Ol(t, e) {
  return t === "all" || B(t) && oe(t, e) >= 0 || t === e;
}
function hL(t) {
  var e = Ec(t);
  if (e) {
    var r = e.axisPointerModel, n = e.axis.scale, i = r.option, a = r.get("status"), o = r.get("value");
    o != null && (o = n.parse(o));
    var s = Jf(r);
    a == null && (i.status = s ? "show" : "hide");
    var u = n.getExtent();
    // Pick a value on axis when initializing.
    (o == null || o > u[1]) && (o = u[1]), o < u[0] && (o = u[0]), i.value = o, s && (i.status = e.axis.scale.isBlank() ? "hide" : "show");
  }
}
function Ec(t) {
  var e = (t.ecModel.getComponent("axisPointer") || {}).coordSysAxesInfo;
  return e && e.axesInfo[Ia(t)];
}
function cL(t) {
  var e = Ec(t);
  return e && e.axisPointerModel;
}
function Jf(t) {
  return !!t.get(["handle", "show"]);
}
function Ia(t) {
  return t.type + "||" + t.id;
}
var eg = {}, vL = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.render = function(r, n, i, a) {
      this.axisPointerClass && hL(r), t.prototype.render.apply(this, arguments), this._doUpdateAxisPointerClass(r, i, !0);
    }, e.prototype.updateAxisPointer = function(r, n, i, a) {
      this._doUpdateAxisPointerClass(r, i, !1);
    }, e.prototype.remove = function(r, n) {
      var i = this._axisPointer;
      i && i.remove(n);
    }, e.prototype.dispose = function(r, n) {
      this._disposeAxisPointer(n), t.prototype.dispose.apply(this, arguments);
    }, e.prototype._doUpdateAxisPointerClass = function(r, n, i) {
      var a = e.getAxisPointerClass(this.axisPointerClass);
      if (a) {
        var o = cL(r);
        o ? (this._axisPointer || (this._axisPointer = new a())).render(r, o, n, i) : this._disposeAxisPointer(n);
      }
    }, e.prototype._disposeAxisPointer = function(r) {
      this._axisPointer && this._axisPointer.dispose(r), this._axisPointer = null;
    }, e.registerAxisPointerClass = function(r, n) {
      eg[r] = n;
    }, e.getAxisPointerClass = function(r) {
      return r && eg[r];
    }, e.type = "axis", e;
  })(lt)
), fn = de(), tg = Z, kl = le, dL = (
  /** @class */
  (function() {
    function t() {
      this._dragging = !1, this.animationThreshold = 15;
    }
    return t.prototype.render = function(e, r, n, i) {
      var a = r.get("value"), o = r.get("status");
      if (this._axisModel = e, this._axisPointerModel = r, this._api = n, !(!i && this._lastValue === a && this._lastStatus === o)) {
        this._lastValue = a, this._lastStatus = o;
        var s = this._group, u = this._handle;
        if (!o || o === "hide") {
          s && s.hide(), u && u.hide();
          return;
        }
        s && s.show(), u && u.show();
        var l = {};
        this.makeElOption(l, a, e, r, n);
        var f = l.graphicKey;
        f !== this._lastGraphicKey && this.clear(n), this._lastGraphicKey = f;
        var h = this._moveAnimation = this.determineAnimation(e, r);
        if (!s)
          s = this._group = new ut(), this.createPointerEl(s, l, e, r), this.createLabelEl(s, l, e, r), n.getZr().add(s);
        else {
          var c = we(rg, r, h);
          this.updatePointerEl(s, l, c), this.updateLabelEl(s, l, c, r);
        }
        ig(s, r, !0), this._renderHandle(a);
      }
    }, t.prototype.remove = function(e) {
      this.clear(e);
    }, t.prototype.dispose = function(e) {
      this.clear(e);
    }, t.prototype.determineAnimation = function(e, r) {
      var n = r.get("animation"), i = e.axis, a = i.type === "category", o = r.get("snap");
      if (!o && !a)
        return !1;
      if (n === "auto" || n == null) {
        var s = this.animationThreshold;
        if (a && _u(i).w > s)
          return !0;
        if (o) {
          var u = Ec(e).seriesDataCount, l = i.getExtent();
          return Math.abs(l[0] - l[1]) / u > s;
        }
        return !1;
      }
      return n === !0;
    }, t.prototype.makeElOption = function(e, r, n, i, a) {
    }, t.prototype.createPointerEl = function(e, r, n, i) {
      var a = r.pointer;
      if (a) {
        var o = fn(e).pointerEl = new HC[a.type](tg(r.pointer));
        e.add(o);
      }
    }, t.prototype.createLabelEl = function(e, r, n, i) {
      if (r.label) {
        var a = fn(e).labelEl = new je(tg(r.label));
        e.add(a), ng(a, i);
      }
    }, t.prototype.updatePointerEl = function(e, r, n) {
      var i = fn(e).pointerEl;
      i && r.pointer && (i.setStyle(r.pointer.style), n(i, {
        shape: r.pointer.shape
      }));
    }, t.prototype.updateLabelEl = function(e, r, n, i) {
      var a = fn(e).labelEl;
      a && (a.setStyle(r.label.style), n(a, {
        // Consider text length change in vertical axis, animation should
        // be used on shape, otherwise the effect will be weird.
        // TODOTODO
        // shape: elOption.label.shape,
        x: r.label.x,
        y: r.label.y
      }), ng(a, i));
    }, t.prototype._renderHandle = function(e) {
      if (!(this._dragging || !this.updateHandleTransform)) {
        var r = this._axisPointerModel, n = this._api.getZr(), i = this._handle, a = r.getModel("handle"), o = r.get("status");
        if (!a.get("show") || !o || o === "hide") {
          i && n.remove(i), this._handle = null;
          return;
        }
        var s;
        this._handle || (s = !0, i = this._handle = ou(a.get("icon"), {
          cursor: "move",
          draggable: !0,
          onmousemove: function(l) {
            Ug(l.event);
          },
          onmousedown: kl(this._onHandleDragMove, this, 0, 0),
          drift: kl(this._onHandleDragMove, this),
          ondragend: kl(this._onHandleDragEnd, this)
        }), n.add(i)), ig(i, r, !1), i.setStyle(a.getItemStyle(null, ["color", "borderColor", "borderWidth", "opacity", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"]));
        var u = a.get("size");
        B(u) || (u = [u, u]), i.scaleX = u[0] / 2, i.scaleY = u[1] / 2, g_(this, "_doDispatchAxisPointer", a.get("throttle") || 0, "fixRate"), this._moveHandleToValue(e, s);
      }
    }, t.prototype._moveHandleToValue = function(e, r) {
      rg(this._axisPointerModel, !r && this._moveAnimation, this._handle, Bl(this.getHandleTransform(e, this._axisModel, this._axisPointerModel)));
    }, t.prototype._onHandleDragMove = function(e, r) {
      var n = this._handle;
      if (n) {
        this._dragging = !0;
        var i = this.updateHandleTransform(Bl(n), [e, r], this._axisModel, this._axisPointerModel);
        this._payloadInfo = i, n.stopAnimation(), n.attr(Bl(i)), fn(n).lastProp = null, this._doDispatchAxisPointer();
      }
    }, t.prototype._doDispatchAxisPointer = function() {
      var e = this._handle;
      if (e) {
        var r = this._payloadInfo, n = this._axisModel;
        this._api.dispatchAction({
          type: "updateAxisPointer",
          x: r.cursorPoint[0],
          y: r.cursorPoint[1],
          tooltipOption: r.tooltipOption,
          axesInfo: [{
            axisDim: n.axis.dim,
            axisIndex: n.componentIndex
          }]
        });
      }
    }, t.prototype._onHandleDragEnd = function() {
      this._dragging = !1;
      var e = this._handle;
      if (e) {
        var r = this._axisPointerModel.get("value");
        this._moveHandleToValue(r), this._api.dispatchAction({
          type: "hideTip"
        });
      }
    }, t.prototype.clear = function(e) {
      this._lastValue = null, this._lastStatus = null;
      var r = e.getZr(), n = this._group, i = this._handle;
      r && n && (this._lastGraphicKey = null, n && r.remove(n), i && r.remove(i), this._group = null, this._handle = null, this._payloadInfo = null), Ff(this, "_doDispatchAxisPointer");
    }, t.prototype.doClear = function() {
    }, t.prototype.buildLabel = function(e, r, n) {
      return n = n || 0, {
        x: e[n],
        y: e[1 - n],
        width: r[n],
        height: r[1 - n]
      };
    }, t;
  })()
);
function rg(t, e, r, n) {
  z0(fn(r).lastProp, n) || (fn(r).lastProp = n, e ? yi(r, n, t) : (r.stopAnimation(), r.attr(n)));
}
function z0(t, e) {
  if (z(t) && z(e)) {
    var r = !0;
    return w(e, function(n, i) {
      r = r && z0(t[i], n);
    }), !!r;
  } else
    return t === e;
}
function ng(t, e) {
  t[e.get(["label", "show"]) ? "show" : "hide"]();
}
function Bl(t) {
  return {
    x: t.x || 0,
    y: t.y || 0,
    rotation: t.rotation || 0
  };
}
function ig(t, e, r) {
  var n = e.get("z"), i = e.get("zlevel");
  t && t.traverse(function(a) {
    a.type !== "group" && (n != null && (a.z = n), i != null && (a.zlevel = i), a.silent = r);
  });
}
function pL(t) {
  var e = t.get("type"), r = t.getModel(e + "Style"), n;
  return e === "line" ? (n = r.getLineStyle(), n.fill = null) : e === "shadow" && (n = r.getAreaStyle(), n.stroke = null), n;
}
function gL(t, e, r, n, i) {
  var a = r.get("value"), o = H0(a, e.axis, e.ecModel, r.get("seriesDataIndices"), {
    precision: r.get(["label", "precision"]),
    formatter: r.get(["label", "formatter"])
  }), s = r.getModel("label"), u = Ha(s.get("padding") || 0), l = s.getFont(), f = fy(o, l), h = i.position, c = f.width + u[1] + u[3], v = f.height + u[0] + u[2], d = i.align;
  d === "right" && (h[0] -= c), d === "center" && (h[0] -= c / 2);
  var y = i.verticalAlign;
  y === "bottom" && (h[1] -= v), y === "middle" && (h[1] -= v / 2), yL(h, c, v, n);
  var p = s.get("backgroundColor");
  (!p || p === "auto") && (p = e.get(["axisLine", "lineStyle", "color"])), t.label = {
    // shape: {x: 0, y: 0, width: width, height: height, r: labelModel.get('borderRadius')},
    x: h[0],
    y: h[1],
    style: Cn(s, {
      text: o,
      font: l,
      fill: s.getTextColor(),
      padding: u,
      backgroundColor: p
    }),
    // Label should be over axisPointer.
    z2: 10
  };
}
function yL(t, e, r, n) {
  var i = n.getWidth(), a = n.getHeight();
  t[0] = Math.min(t[0] + e, i) - e, t[1] = Math.min(t[1] + r, a) - r, t[0] = Math.max(t[0], 0), t[1] = Math.max(t[1], 0);
}
function H0(t, e, r, n, i) {
  t = e.scale.parse(t);
  var a = e.scale.getLabel({
    value: t
  }, {
    // If `precision` is set, width can be fixed (like '12.00500'), which
    // helps to debounce when when moving label.
    precision: i.precision
  }), o = i.formatter;
  if (o) {
    var s = {
      value: Ps(e, {
        value: t
      }),
      axisDimension: e.dim,
      axisIndex: e.index,
      seriesData: []
    };
    w(n, function(u) {
      var l = r.getSeriesByIndex(u.seriesIndex), f = u.dataIndexInside, h = l && l.getDataParams(f);
      h && s.seriesData.push(h);
    }), N(o) ? a = o.replace("{value}", a) : X(o) && (a = o(s));
  }
  return a;
}
function V0(t, e, r) {
  var n = Tt();
  return Vs(n, n, r.rotation), ts(n, n, r.position), zh([t.dataToCoord(e), (r.labelOffset || 0) + (r.labelDirection || 1) * (r.labelMargin || 0)], n);
}
function mL(t, e, r, n, i, a) {
  var o = Rr.innerTextLayout(r.rotation, 0, r.labelDirection);
  r.labelMargin = i.get(["label", "margin"]), gL(e, n, i, a, {
    position: V0(n.axis, t, r),
    align: o.textAlign,
    verticalAlign: o.textVerticalAlign
  });
}
function _L(t, e, r) {
  return r = r || 0, {
    x1: t[r],
    y1: t[1 - r],
    x2: e[r],
    y2: e[1 - r]
  };
}
function SL(t, e, r) {
  return r = r || 0, {
    x: t[r],
    y: t[1 - r],
    width: e[r],
    height: e[1 - r]
  };
}
function u2(t, e, r, n, i, a) {
  return {
    cx: t,
    cy: e,
    r0: r,
    r: n,
    startAngle: i,
    endAngle: a,
    clockwise: !0
  };
}
function bL(t, e, r) {
  return _u(t, {
    fromStat: {
      sers: H(e, function(n) {
        return r.getSeriesByIndex(n.seriesIndex);
      })
    },
    min: 1
  }).w;
}
function TL(t, e, r) {
  return [ge(et(e[0], e[1]), t - r / 2), et(t + r / 2, ge(e[0], e[1]))];
}
var wL = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      return t !== null && t.apply(this, arguments) || this;
    }
    return e.prototype.makeElOption = function(r, n, i, a, o) {
      var s = i.axis, u = s.grid, l = a.get("type"), f = s.getGlobalExtent(), h = ag(u, s).getOtherAxis(s).getGlobalExtent(), c = s.toGlobalCoord(s.dataToCoord(n, !0));
      if (l && l !== "none") {
        var v = pL(a), d = CL[l](s, c, f, h, a.get("seriesDataIndices"), a.ecModel);
        d.style = v, r.graphicKey = d.type, r.pointer = d;
      }
      var y = Bs(u.getRect(), i);
      mL(n, r, y, i, a, o);
    }, e.prototype.getHandleTransform = function(r, n, i) {
      var a = Bs(n.axis.grid.getRect(), n, {
        labelInside: !1
      });
      a.labelMargin = i.get(["handle", "margin"]);
      var o = V0(n.axis, r, a);
      return {
        x: o[0],
        y: o[1],
        rotation: a.rotation + (a.labelDirection < 0 ? Math.PI : 0)
      };
    }, e.prototype.updateHandleTransform = function(r, n, i, a) {
      var o = i.axis, s = o.grid, u = o.getGlobalExtent(!0), l = ag(s, o).getOtherAxis(o).getGlobalExtent(), f = o.dim === "x" ? 0 : 1, h = [r.x, r.y];
      h[f] += n[f], h[f] = et(u[1], h[f]), h[f] = ge(u[0], h[f]);
      var c = (l[1] + l[0]) / 2, v = [c, c];
      v[f] = h[f];
      var d = [{
        verticalAlign: "middle"
      }, {
        align: "center"
      }];
      return {
        x: h[0],
        y: h[1],
        rotation: r.rotation,
        cursorPoint: v,
        tooltipOption: d[f]
      };
    }, e;
  })(dL)
);
function ag(t, e) {
  var r = {};
  return r[e.dim + "AxisIndex"] = e.index, t.getCartesian(r);
}
var CL = {
  line: function(t, e, r, n) {
    var i = _L([e, n[0]], [e, n[1]], og(t));
    return {
      type: "Line",
      subPixelOptimize: !0,
      shape: i
    };
  },
  shadow: function(t, e, r, n, i, a) {
    var o = bL(t, i, a), s = n[1] - n[0], u = TL(e, r, o), l = u[0], f = u[1];
    return {
      type: "Rect",
      shape: SL([l, n[0]], [f - l, s], og(t))
    };
  }
};
function og(t) {
  return t.dim === "x" ? 0 : 1;
}
var xL = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.type = "axisPointer", e.defaultOption = {
      // 'auto' means that show when triggered by tooltip or handle.
      show: "auto",
      // zlevel: 0,
      z: 50,
      type: "line",
      // axispointer triggered by tootip determine snap automatically,
      // see `modelHelper`.
      snap: !1,
      triggerTooltip: !0,
      triggerEmphasis: !0,
      value: null,
      status: null,
      link: [],
      // Do not set 'auto' here, otherwise global animation: false
      // will not effect at this axispointer.
      animation: null,
      animationDurationUpdate: 200,
      lineStyle: {
        color: j.color.border,
        width: 1,
        type: "dashed"
      },
      shadowStyle: {
        color: j.color.shadowTint
      },
      label: {
        show: !0,
        formatter: null,
        precision: "auto",
        margin: 3,
        color: j.color.neutral00,
        padding: [5, 7, 5, 7],
        backgroundColor: j.color.accent60,
        borderColor: null,
        borderWidth: 0,
        borderRadius: 3
      },
      handle: {
        show: !1,
        // eslint-disable-next-line
        icon: "M10.7,11.9v-1.3H9.3v1.3c-4.9,0.3-8.8,4.4-8.8,9.4c0,5,3.9,9.1,8.8,9.4h1.3c4.9-0.3,8.8-4.4,8.8-9.4C19.5,16.3,15.6,12.2,10.7,11.9z M13.3,24.4H6.7v-1.2h6.6z M13.3,22H6.7v-1.2h6.6z M13.3,19.6H6.7v-1.2h6.6z",
        size: 45,
        // handle margin is from symbol center to axis, which is stable when circular move.
        margin: 50,
        // color: '#1b8bbd'
        // color: '#2f4554'
        color: j.color.accent40,
        // For mobile performance
        throttle: 40
      }
    }, e;
  })(ae)
), fr = de(), ML = w;
function G0(t, e, r) {
  if (!W.node) {
    var n = e.getZr();
    fr(n).records || (fr(n).records = {}), DL(n, e);
    var i = fr(n).records[t] || (fr(n).records[t] = {});
    i.handler = r;
  }
}
function DL(t, e) {
  if (fr(t).initialized)
    return;
  fr(t).initialized = !0, r("click", we(Nl, "click")), r("mousemove", we(Nl, "mousemove")), r("mousewheel", we(Nl, "mousewheel")), r("globalout", AL);
  function r(n, i) {
    t.on(n, function(a) {
      var o = PL(e);
      ML(fr(t).records, function(s) {
        s && i(s, a, o.dispatchAction);
      }), IL(o.pendings, e);
    });
  }
}
function IL(t, e) {
  var r = t.showTip.length, n = t.hideTip.length, i;
  r ? i = t.showTip[r - 1] : n && (i = t.hideTip[n - 1]), i && (i.dispatchAction = null, e.dispatchAction(i));
}
function AL(t, e, r) {
  t.handler("leave", null, r);
}
function Nl(t, e, r, n) {
  e.handler(t, r, n);
}
function PL(t) {
  var e = {
    showTip: [],
    hideTip: []
  }, r = function(n) {
    var i = e[n.type];
    i ? i.push(n) : (n.dispatchAction = r, t.dispatchAction(n));
  };
  return {
    dispatchAction: r,
    pendings: e
  };
}
function eh(t, e) {
  if (!W.node) {
    var r = e.getZr(), n = (fr(r).records || {})[t];
    n && (fr(r).records[t] = null);
  }
}
var LL = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.render = function(r, n, i) {
      var a = n.getComponent("tooltip"), o = r.get("triggerOn") || a && a.get("triggerOn") || "mousemove|click|mousewheel";
      G0("axisPointer", i, function(s, u, l) {
        o !== "none" && (s === "leave" || o.indexOf(s) >= 0) && l({
          type: "updateAxisPointer",
          currTrigger: s,
          x: u && u.offsetX,
          y: u && u.offsetY
        });
      });
    }, e.prototype.remove = function(r, n) {
      eh("axisPointer", n);
    }, e.prototype.dispose = function(r, n) {
      eh("axisPointer", n);
    }, e.type = "axisPointer", e;
  })(lt)
);
function U0(t, e) {
  var r = [], n = t.seriesIndex, i;
  if (n == null || !(i = e.getSeriesByIndex(n)))
    return {
      point: []
    };
  var a = i.getData(), o = Oa(a, t);
  if (o == null || o < 0 || B(o))
    return {
      point: []
    };
  var s = a.getItemGraphicEl(o), u = i.coordinateSystem;
  if (i.getTooltipPosition)
    r = i.getTooltipPosition(o) || [];
  else if (u && u.dataToPoint)
    if (t.isStacked) {
      var l = u.getBaseAxis(), f = u.getOtherAxis(l), h = f.dim, c = l.dim, v = h === "x" || h === "radius" ? 1 : 0, d = a.mapDimension(c), y = [];
      y[v] = a.get(d, o), y[1 - v] = a.get(a.getCalculationInfo("stackResultDimension"), o), r = u.dataToPoint(y) || [];
    } else
      r = u.dataToPoint(a.getValues(H(u.dimensions, function(g) {
        return a.mapDimension(g);
      }), o)) || [];
  else if (s) {
    var p = s.getBoundingRect().clone();
    p.applyTransform(s.transform), r = [p.x + p.width / 2, p.y + p.height / 2];
  }
  return {
    point: r,
    el: s
  };
}
var sg = de();
function RL(t, e, r) {
  var n = t.currTrigger, i = [t.x, t.y], a = t, o = t.dispatchAction || le(r.dispatchAction, r), s = e.getComponent("axisPointer").coordSysAxesInfo;
  if (s) {
    jo(i) && (i = U0({
      seriesIndex: a.seriesIndex,
      // Do not use dataIndexInside from other ec instance.
      // FIXME: auto detect it?
      dataIndex: a.dataIndex
    }, e).point);
    var u = jo(i), l = a.axesInfo, f = s.axesInfo, h = n === "leave" || jo(i), c = {}, v = {}, d = {
      list: [],
      map: {}
    }, y = {
      showPointer: we(OL, v),
      showTooltip: we(kL, d)
    };
    w(s.coordSysMap, function(g, m) {
      var _ = u || g.containPoint(i);
      w(s.coordSysAxesInfo[m], function(S, b) {
        var T = S.axis, C = zL(l, S);
        if (!h && _ && (!l || C)) {
          var M = C && C.value;
          M == null && !u && (M = T.pointToData(i)), M != null && ug(S, M, y, !1, c);
        }
      });
    });
    var p = {};
    return w(f, function(g, m) {
      var _ = g.linkGroup;
      _ && !v[m] && w(_.axesInfo, function(S, b) {
        var T = v[b];
        if (S !== g && T) {
          var C = T.value;
          _.mapper && (C = g.axis.scale.parse(_.mapper(C, lg(S), lg(g)))), p[g.key] = C;
        }
      });
    }), w(p, function(g, m) {
      ug(f[m], g, y, !0, c);
    }), BL(v, f, c), NL(d, i, t, o), FL(f, o, r), c;
  }
}
function ug(t, e, r, n, i) {
  var a = t.axis;
  if (!(a.scale.isBlank() || !a.containData(e))) {
    if (!t.involveSeries) {
      r.showPointer(t, e);
      return;
    }
    var o = EL(e, t), s = o.payloadBatch, u = o.snapToValue;
    s[0] && i.seriesIndex == null && E(i, s[0]), !n && t.snap && a.containData(u) && u != null && (e = u), r.showPointer(t, e, s), r.showTooltip(t, o, u);
  }
}
function EL(t, e) {
  var r = e.axis, n = r.dim, i = t, a = [], o = Number.MAX_VALUE, s = -1;
  return w(e.seriesModels, function(u, l) {
    var f = u.getData().mapDimensionsAll(n), h, c;
    if (u.getAxisTooltipData) {
      var v = u.getAxisTooltipData(f, t, r);
      c = v.dataIndices, h = v.nestestValue;
    } else {
      if (c = u.indicesOfNearest(
        n,
        f[0],
        t,
        // Add a threshold to avoid find the wrong dataIndex
        // when data length is not same.
        // false,
        r.type === "category" ? 0.5 : null
      ), !c.length)
        return;
      h = u.getData().get(f[0], c[0]);
    }
    if (vr(h)) {
      var d = t - h, y = Math.abs(d);
      y <= o && ((y < o || d >= 0 && s < 0) && (o = y, s = d, i = h, a.length = 0), w(c, function(p) {
        a.push({
          seriesIndex: u.seriesIndex,
          dataIndexInside: p,
          dataIndex: u.getData().getRawIndex(p)
        });
      }));
    }
  }), {
    payloadBatch: a,
    snapToValue: i
  };
}
function OL(t, e, r, n) {
  t[e.key] = {
    value: r,
    payloadBatch: n
  };
}
function kL(t, e, r, n) {
  var i = r.payloadBatch, a = e.axis, o = a.model, s = e.axisPointerModel;
  if (!(!e.triggerTooltip || !i.length)) {
    var u = e.coordSys.model, l = Ia(u), f = t.map[l];
    f || (f = t.map[l] = {
      coordSysId: u.id,
      coordSysIndex: u.componentIndex,
      coordSysType: u.type,
      coordSysMainType: u.mainType,
      dataByAxis: []
    }, t.list.push(f)), f.dataByAxis.push({
      axisDim: a.dim,
      axisIndex: o.componentIndex,
      axisType: o.type,
      axisId: o.id,
      value: n,
      // Caution: viewHelper.getValueLabel is actually on "view stage", which
      // depends that all models have been updated. So it should not be performed
      // here. Considering axisPointerModel used here is volatile, which is hard
      // to be retrieve in TooltipView, we prepare parameters here.
      valueLabelOpt: {
        precision: s.get(["label", "precision"]),
        formatter: s.get(["label", "formatter"])
      },
      seriesDataIndices: i.slice()
    });
  }
}
function BL(t, e, r) {
  var n = r.axesInfo = [];
  w(e, function(i, a) {
    var o = i.axisPointerModel.option, s = t[a];
    s ? (!i.useHandle && (o.status = "show"), o.value = s.value, o.seriesDataIndices = (s.payloadBatch || []).slice()) : !i.useHandle && (o.status = "hide"), o.status === "show" && n.push({
      axisDim: i.axis.dim,
      axisIndex: i.axis.model.componentIndex,
      value: o.value
    });
  });
}
function NL(t, e, r, n) {
  if (jo(e) || !t.list.length) {
    n({
      type: "hideTip"
    });
    return;
  }
  var i = ((t.list[0].dataByAxis[0] || {}).seriesDataIndices || [])[0] || {};
  n({
    type: "showTip",
    escapeConnect: !0,
    x: e[0],
    y: e[1],
    tooltipOption: r.tooltipOption,
    position: r.position,
    dataIndexInside: i.dataIndexInside,
    dataIndex: i.dataIndex,
    seriesIndex: i.seriesIndex,
    dataByCoordSys: t.list
  });
}
function FL(t, e, r) {
  var n = r.getZr(), i = "axisPointerLastHighlights", a = sg(n)[i] || {}, o = sg(n)[i] = {};
  w(t, function(f, h) {
    var c = f.axisPointerModel.option;
    c.status === "show" && f.triggerEmphasis && w(c.seriesDataIndices, function(v) {
      o[v.seriesIndex + "|" + v.dataIndex] = v;
    });
  });
  var s = [], u = [];
  function l(f) {
    return {
      seriesIndex: f.seriesIndex,
      dataIndex: f.dataIndex
    };
  }
  w(a, function(f, h) {
    !o[h] && u.push(l(f));
  }), w(o, function(f, h) {
    !a[h] && s.push(l(f));
  }), u.length && r.dispatchAction({
    type: "downplay",
    escapeConnect: !0,
    // Not blur others when highlight in axisPointer.
    notBlur: !0,
    batch: u
  }), s.length && r.dispatchAction({
    type: "highlight",
    escapeConnect: !0,
    // Not blur others when highlight in axisPointer.
    notBlur: !0,
    batch: s
  });
}
function zL(t, e) {
  for (var r = 0; r < (t || []).length; r++) {
    var n = t[r];
    if (e.axis.dim === n.axisDim && e.axis.model.componentIndex === n.axisIndex)
      return n;
  }
}
function lg(t) {
  var e = t.axis.model, r = {}, n = r.axisDim = t.axis.dim;
  return r.axisIndex = r[n + "AxisIndex"] = e.componentIndex, r.axisName = r[n + "AxisName"] = e.name, r.axisId = r[n + "AxisId"] = e.id, r;
}
function jo(t) {
  return !t || t[0] == null || isNaN(t[0]) || t[1] == null || isNaN(t[1]);
}
function HL(t) {
  vL.registerAxisPointerClass("CartesianAxisPointer", wL), t.registerComponentModel(xL), t.registerComponentView(LL), t.registerPreprocessor(function(e) {
    if (e) {
      (!e.axisPointer || e.axisPointer.length === 0) && (e.axisPointer = {});
      var r = e.axisPointer.link;
      r && !B(r) && (e.axisPointer.link = [r]);
    }
  }), t.registerProcessor(t.PRIORITY.PROCESSOR.STATISTIC, {
    overallReset: function(e, r) {
      e.getComponent("axisPointer").coordSysAxesInfo = oL(e, r);
    }
  }), t.registerAction({
    type: "updateAxisPointer",
    event: "updateAxisPointer",
    update: ":updateAxisPointer"
  }, RL);
}
function VL(t, e) {
  var r = Ha(e.get("padding")), n = e.getItemStyle(["color", "opacity"]);
  n.fill = e.get("backgroundColor");
  var i = new Ee({
    shape: {
      x: t.x - r[3],
      y: t.y - r[0],
      width: t.width + r[1] + r[3],
      height: t.height + r[0] + r[2],
      r: e.get("borderRadius")
    },
    style: n,
    silent: !0,
    z2: -1
  });
  return i;
}
var GL = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.type = "tooltip", e.dependencies = ["axisPointer"], e.defaultOption = {
      // zlevel: 0,
      z: 60,
      show: !0,
      // tooltip main content
      showContent: !0,
      // 'trigger' only works on coordinate system.
      // 'item' | 'axis' | 'none'
      trigger: "item",
      // 'click' | 'mousemove' | 'none'
      triggerOn: "mousemove|click|mousewheel",
      alwaysShowContent: !1,
      renderMode: "auto",
      // whether restraint content inside viewRect.
      // If renderMode: 'richText', default true.
      // If renderMode: 'html', defaults to `false` (for backward compat).
      confine: null,
      showDelay: 0,
      hideDelay: 100,
      // Animation transition time, unit is second
      transitionDuration: 0.4,
      displayTransition: !0,
      enterable: !1,
      backgroundColor: j.color.neutral00,
      // box shadow
      shadowBlur: 10,
      shadowColor: "rgba(0, 0, 0, .2)",
      shadowOffsetX: 1,
      shadowOffsetY: 2,
      // tooltip border radius, unit is px, default is 4
      borderRadius: 4,
      // tooltip border width, unit is px, default is 0 (no border)
      borderWidth: 1,
      defaultBorderColor: j.color.border,
      // Tooltip inside padding, default is 5 for all direction
      // Array is allowed to set up, right, bottom, left, same with css
      // The default value: See `tooltip/tooltipMarkup.ts#getPaddingFromTooltipModel`.
      padding: null,
      // Extra css text
      extraCssText: "",
      // axis indicator, trigger by axis
      axisPointer: {
        // default is line
        // legal values: 'line' | 'shadow' | 'cross'
        type: "line",
        // Valid when type is line, appoint tooltip line locate on which line. Optional
        // legal values: 'x' | 'y' | 'angle' | 'radius' | 'auto'
        // default is 'auto', chose the axis which type is category.
        // for multiply y axis, cartesian coord chose x axis, polar chose angle axis
        axis: "auto",
        animation: "auto",
        animationDurationUpdate: 200,
        animationEasingUpdate: "exponentialOut",
        crossStyle: {
          color: j.color.borderShade,
          width: 1,
          type: "dashed",
          // TODO formatter
          textStyle: {}
        }
        // lineStyle and shadowStyle should not be specified here,
        // otherwise it will always override those styles on option.axisPointer.
      },
      textStyle: {
        color: j.color.tertiary,
        fontSize: 14
      }
    }, e;
  })(ae)
);
function Y0(t) {
  var e = t.get("confine");
  return e != null ? !!e : t.get("renderMode") === "richText";
}
function W0(t) {
  if (W.domSupported) {
    for (var e = document.documentElement.style, r = 0, n = t.length; r < n; r++)
      if (t[r] in e)
        return t[r];
  }
}
var X0 = W0(["transform", "webkitTransform", "OTransform", "MozTransform", "msTransform"]), UL = W0(["webkitTransition", "transition", "OTransition", "MozTransition", "msTransition"]);
function q0(t, e) {
  if (!t)
    return e;
  e = tc(e, !0);
  var r = t.indexOf(e);
  return t = r === -1 ? e : "-" + t.slice(0, r) + "-" + e, t.toLowerCase();
}
function YL(t, e) {
  var r = t.currentStyle || document.defaultView && document.defaultView.getComputedStyle(t);
  return r ? r[e] : null;
}
var WL = q0(UL, "transition"), Oc = q0(X0, "transform"), XL = "position:absolute;display:block;border-style:solid;white-space:nowrap;z-index:9999999;" + (W.transform3dSupported ? "will-change:transform;" : "");
function qL(t) {
  return t = t === "left" ? "right" : t === "right" ? "left" : t === "top" ? "bottom" : "top", t;
}
function $L(t, e, r) {
  if (!N(r) || r === "inside")
    return "";
  var n = t.get("backgroundColor"), i = t.get("borderWidth");
  e = ba(e);
  var a = qL(r), o = Math.max(Math.round(i) * 1.5, 6), s = "", u = Oc + ":", l;
  oe(["left", "right"], a) > -1 ? (s += "top:50%", u += "translateY(-50%) rotate(" + (l = a === "left" ? -225 : -45) + "deg)") : (s += "left:50%", u += "translateX(-50%) rotate(" + (l = a === "top" ? 225 : 45) + "deg)");
  var f = l * Math.PI / 180, h = o + i, c = h * Math.abs(Math.cos(f)) + h * Math.abs(Math.sin(f)), v = Math.round(((c - Math.SQRT2 * i) / 2 + Math.SQRT2 * i - (c - h) / 2) * 100) / 100;
  s += ";" + a + ":-" + v + "px";
  var d = e + " solid " + i + "px;", y = ["position:absolute;width:" + o + "px;height:" + o + "px;z-index:-1;", s + ";" + u + ";", "border-bottom:" + d, "border-right:" + d, "background-color:" + n + ";"];
  return '<div style="' + y.join("") + '"></div>';
}
function ZL(t, e, r) {
  var n = "cubic-bezier(0.23,1,0.32,1)", i = "", a = "";
  return r && (i = " " + t / 2 + "s " + n, a = "opacity" + i + ",visibility" + i), e || (i = " " + t + "s " + n, a += (a.length ? "," : "") + (W.transformSupported ? "" + Oc + i : ",left" + i + ",top" + i)), WL + ":" + a;
}
function fg(t, e, r) {
  var n = t.toFixed(0) + "px", i = e.toFixed(0) + "px";
  if (!W.transformSupported)
    return r ? "top:" + i + ";left:" + n + ";" : [["top", i], ["left", n]];
  var a = W.transform3dSupported, o = "translate" + (a ? "3d" : "") + "(" + n + "," + i + (a ? ",0" : "") + ")";
  return r ? "top:0;left:0;" + Oc + ":" + o + ";" : [["top", 0], ["left", 0], [X0, o]];
}
function KL(t) {
  var e = [], r = t.get("fontSize"), n = t.getTextColor();
  n && e.push("color:" + n), e.push("font:" + t.getFont());
  var i = G(t.get("lineHeight"), Math.round(r * 3 / 2));
  r && e.push("line-height:" + i + "px");
  var a = t.get("textShadowColor"), o = t.get("textShadowBlur") || 0, s = t.get("textShadowOffsetX") || 0, u = t.get("textShadowOffsetY") || 0;
  return a && o && e.push("text-shadow:" + s + "px " + u + "px " + o + "px " + a), w(["decoration", "align"], function(l) {
    var f = t.get(l);
    f && e.push("text-" + l + ":" + f);
  }), e.join(";");
}
function jL(t, e, r, n) {
  var i = [], a = t.get("transitionDuration"), o = t.get("backgroundColor"), s = t.get("shadowBlur"), u = t.get("shadowColor"), l = t.get("shadowOffsetX"), f = t.get("shadowOffsetY"), h = t.getModel("textStyle"), c = d_(t, "html"), v = l + "px " + f + "px " + s + "px " + u;
  return i.push("box-shadow:" + v), e && a > 0 && i.push(ZL(a, r, n)), o && i.push("background-color:" + o), w(["width", "color", "radius"], function(d) {
    var y = "border-" + d, p = tc(y), g = t.get(p);
    g != null && i.push(y + ":" + g + (d === "color" ? "" : "px"));
  }), i.push(KL(h)), c != null && i.push("padding:" + Ha(c).join("px ") + "px"), i.join(";") + ";";
}
function hg(t, e, r, n, i) {
  var a = e && e.painter;
  if (r) {
    var o = a && a.getViewportRoot();
    o && p1(t, o, r, n, i);
  } else {
    t[0] = n, t[1] = i;
    var s = a && a.getViewportRootOffset();
    s && (t[0] += s.offsetLeft, t[1] += s.offsetTop);
  }
  t[2] = t[0] / e.getWidth(), t[3] = t[1] / e.getHeight();
}
var QL = (
  /** @class */
  (function() {
    function t(e, r) {
      if (this._show = !1, this._styleCoord = [0, 0, 0, 0], this._enterable = !0, this._alwaysShowContent = !1, this._firstShow = !0, this._longHide = !0, W.wxa)
        return null;
      var n = document.createElement("div");
      n.domBelongToZr = !0, this.el = n;
      var i = this._zr = e.getZr(), a = r.appendTo, o = a && (N(a) ? document.querySelector(a) : si(a) ? a : X(a) && a(e.getDom()));
      hg(this._styleCoord, i, o, e.getWidth() / 2, e.getHeight() / 2), (o || e.getDom()).appendChild(n), this._api = e, this._container = o;
      var s = this;
      n.onmouseenter = function() {
        s._enterable && (clearTimeout(s._hideTimeout), s._show = !0), s._inContent = !0;
      }, n.onmousemove = function(u) {
        if (u = u || window.event, !s._enterable) {
          var l = i.handler, f = i.painter.getViewportRoot();
          dt(f, u, !0), l.dispatch("mousemove", u);
        }
      }, n.onmouseleave = function() {
        s._inContent = !1, s._enterable && s._show && s.hideLater(s._hideDelay);
      };
    }
    return t.prototype.update = function(e) {
      if (!this._container) {
        var r = this._api.getDom(), n = YL(r, "position"), i = r.style;
        i.position !== "absolute" && n !== "absolute" && (i.position = "relative");
      }
      var a = e.get("alwaysShowContent");
      a && this._moveIfResized(), this._alwaysShowContent = a, this._enableDisplayTransition = e.get("displayTransition") && e.get("transitionDuration") > 0, this.el.className = e.get("className") || "";
    }, t.prototype.show = function(e, r) {
      clearTimeout(this._hideTimeout), clearTimeout(this._longHideTimeout);
      var n = this.el, i = n.style, a = this._styleCoord;
      n.innerHTML ? i.cssText = XL + jL(e, !this._firstShow, this._longHide, this._enableDisplayTransition) + fg(a[0], a[1], !0) + ("border-color:" + ba(r) + ";") + (e.get("extraCssText") || "") + (";pointer-events:" + (this._enterable ? "auto" : "none")) : i.display = "none", this._show = !0, this._firstShow = !1, this._longHide = !1;
    }, t.prototype.setContent = function(e, r, n, i, a) {
      var o = this.el;
      if (e == null) {
        o.innerHTML = "";
        return;
      }
      var s = "";
      if (N(a) && n.get("trigger") === "item" && !Y0(n) && (s = $L(n, i, a)), N(e))
        o.innerHTML = e + s;
      else if (e) {
        o.innerHTML = "", B(e) || (e = [e]);
        for (var u = 0; u < e.length; u++)
          si(e[u]) && e[u].parentNode !== o && o.appendChild(e[u]);
        if (s && o.childNodes.length) {
          var l = document.createElement("div");
          l.innerHTML = s, o.appendChild(l);
        }
      }
    }, t.prototype.setEnterable = function(e) {
      this._enterable = e;
    }, t.prototype.getSize = function() {
      var e = this.el;
      return e ? [e.offsetWidth, e.offsetHeight] : [0, 0];
    }, t.prototype.moveTo = function(e, r) {
      if (this.el) {
        var n = this._styleCoord;
        if (hg(n, this._zr, this._container, e, r), n[0] != null && n[1] != null) {
          var i = this.el.style, a = fg(n[0], n[1]);
          w(a, function(o) {
            i[o[0]] = o[1];
          });
        }
      }
    }, t.prototype._moveIfResized = function() {
      var e = this._styleCoord[2], r = this._styleCoord[3];
      this.moveTo(e * this._zr.getWidth(), r * this._zr.getHeight());
    }, t.prototype.hide = function() {
      var e = this, r = this.el.style;
      this._enableDisplayTransition ? (r.visibility = "hidden", r.opacity = "0") : r.display = "none", W.transform3dSupported && (r.willChange = ""), this._show = !1, this._longHideTimeout = setTimeout(function() {
        return e._longHide = !0;
      }, 500);
    }, t.prototype.hideLater = function(e) {
      this._show && !(this._inContent && this._enterable) && !this._alwaysShowContent && (e ? (this._hideDelay = e, this._show = !1, this._hideTimeout = setTimeout(le(this.hide, this), e)) : this.hide());
    }, t.prototype.isShow = function() {
      return this._show;
    }, t.prototype.dispose = function() {
      clearTimeout(this._hideTimeout), clearTimeout(this._longHideTimeout);
      var e = this._zr;
      g1(e && e.painter && e.painter.getViewportRoot(), this._container);
      var r = this.el;
      if (r) {
        r.onmouseenter = r.onmousemove = r.onmouseleave = null;
        var n = r.parentNode;
        n && n.removeChild(r);
      }
      this.el = this._container = null;
    }, t;
  })()
), JL = (
  /** @class */
  (function() {
    function t(e) {
      this._show = !1, this._styleCoord = [0, 0, 0, 0], this._alwaysShowContent = !1, this._enterable = !0, this._zr = e.getZr(), vg(this._styleCoord, this._zr, e.getWidth() / 2, e.getHeight() / 2);
    }
    return t.prototype.update = function(e) {
      var r = e.get("alwaysShowContent");
      r && this._moveIfResized(), this._alwaysShowContent = r;
    }, t.prototype.show = function() {
      this._hideTimeout && clearTimeout(this._hideTimeout), this.el.show(), this._show = !0;
    }, t.prototype.setContent = function(e, r, n, i, a) {
      var o = this;
      z(e) && Ye(""), this.el && this._zr.remove(this.el);
      var s = n.getModel("textStyle");
      this.el = new je({
        style: {
          rich: r.richTextStyles,
          text: e,
          lineHeight: 22,
          borderWidth: 1,
          borderColor: i,
          textShadowColor: s.get("textShadowColor"),
          fill: n.get(["textStyle", "color"]),
          padding: d_(n, "richText"),
          verticalAlign: "top",
          align: "left"
        },
        z: n.get("z")
      }), w(["backgroundColor", "borderRadius", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"], function(l) {
        o.el.style[l] = n.get(l);
      }), w(["textShadowBlur", "textShadowOffsetX", "textShadowOffsetY"], function(l) {
        o.el.style[l] = s.get(l) || 0;
      }), this._zr.add(this.el);
      var u = this;
      this.el.on("mouseover", function() {
        u._enterable && (clearTimeout(u._hideTimeout), u._show = !0), u._inContent = !0;
      }), this.el.on("mouseout", function() {
        u._enterable && u._show && u.hideLater(u._hideDelay), u._inContent = !1;
      });
    }, t.prototype.setEnterable = function(e) {
      this._enterable = e;
    }, t.prototype.getSize = function() {
      var e = this.el, r = this.el.getBoundingRect(), n = cg(e.style);
      return [r.width + n.left + n.right, r.height + n.top + n.bottom];
    }, t.prototype.moveTo = function(e, r) {
      var n = this.el;
      if (n) {
        var i = this._styleCoord;
        vg(i, this._zr, e, r), e = i[0], r = i[1];
        var a = n.style, o = wr(a.borderWidth || 0), s = cg(a);
        n.x = e + o + s.left, n.y = r + o + s.top, n.markRedraw();
      }
    }, t.prototype._moveIfResized = function() {
      var e = this._styleCoord[2], r = this._styleCoord[3];
      this.moveTo(e * this._zr.getWidth(), r * this._zr.getHeight());
    }, t.prototype.hide = function() {
      this.el && this.el.hide(), this._show = !1;
    }, t.prototype.hideLater = function(e) {
      this._show && !(this._inContent && this._enterable) && !this._alwaysShowContent && (e ? (this._hideDelay = e, this._show = !1, this._hideTimeout = setTimeout(le(this.hide, this), e)) : this.hide());
    }, t.prototype.isShow = function() {
      return this._show;
    }, t.prototype.dispose = function() {
      this._zr.remove(this.el);
    }, t;
  })()
);
function wr(t) {
  return Math.max(0, t);
}
function cg(t) {
  var e = wr(t.shadowBlur || 0), r = wr(t.shadowOffsetX || 0), n = wr(t.shadowOffsetY || 0);
  return {
    left: wr(e - r),
    right: wr(e + r),
    top: wr(e - n),
    bottom: wr(e + n)
  };
}
function vg(t, e, r, n) {
  t[0] = r, t[1] = n, t[2] = t[0] / e.getWidth(), t[3] = t[1] / e.getHeight();
}
var eR = new Ee({
  shape: {
    x: -1,
    y: -1,
    width: 2,
    height: 2
  }
}), tR = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.init = function(r, n) {
      if (!(W.node || !n.getDom())) {
        var i = r.getComponent("tooltip"), a = this._renderMode = yT(i.get("renderMode"));
        this._tooltipContent = a === "richText" ? new JL(n) : new QL(n, {
          appendTo: i.get("appendToBody", !0) ? "body" : i.get("appendTo", !0)
        });
      }
    }, e.prototype.render = function(r, n, i) {
      if (!(W.node || !i.getDom())) {
        this.group.removeAll(), this._tooltipModel = r, this._ecModel = n, this._api = i;
        var a = this._tooltipContent;
        a.update(r), a.setEnterable(r.get("enterable")), this._initGlobalListener(), this._keepShow(), this._renderMode !== "richText" && r.get("transitionDuration") ? g_(this, "_updatePosition", 50, "fixRate") : Ff(this, "_updatePosition");
      }
    }, e.prototype._initGlobalListener = function() {
      var r = this._tooltipModel, n = r.get("triggerOn");
      G0("itemTooltip", this._api, le(function(i, a, o) {
        n !== "none" && (n.indexOf(i) >= 0 ? this._tryShow(a, o) : i === "leave" && this._hide(o));
      }, this));
    }, e.prototype._keepShow = function() {
      var r = this._tooltipModel, n = this._ecModel, i = this._api, a = r.get("triggerOn");
      if (r.get("trigger") !== "axis" && (this._lastDataByCoordSys = null, this._cbParamsList = null), this._lastX != null && this._lastY != null && a !== "none" && a !== "click") {
        var o = this;
        clearTimeout(this._refreshUpdateTimeout), this._refreshUpdateTimeout = setTimeout(function() {
          !i.isDisposed() && o.manuallyShowTip(r, n, i, {
            x: o._lastX,
            y: o._lastY,
            dataByCoordSys: o._lastDataByCoordSys
          });
        });
      }
    }, e.prototype.manuallyShowTip = function(r, n, i, a) {
      if (!(a.from === this.uid || W.node || !i.getDom())) {
        var o = dg(a, i);
        this._ticket = "";
        var s = a.dataByCoordSys, u = aR(a, n, i);
        if (u) {
          var l = u.el.getBoundingRect().clone();
          l.applyTransform(u.el.transform), this._tryShow({
            offsetX: l.x + l.width / 2,
            offsetY: l.y + l.height / 2,
            target: u.el,
            position: a.position,
            // When manully trigger, the mouse is not on the el, so we'd better to
            // position tooltip on the bottom of the el and display arrow is possible.
            positionDefault: "bottom"
          }, o);
        } else if (a.tooltip && a.x != null && a.y != null) {
          var f = eR;
          f.x = a.x, f.y = a.y, f.update(), he(f).tooltipConfig = {
            name: null,
            option: a.tooltip
          }, this._tryShow({
            offsetX: a.x,
            offsetY: a.y,
            target: f
          }, o);
        } else if (s)
          this._tryShow({
            offsetX: a.x,
            offsetY: a.y,
            position: a.position,
            dataByCoordSys: s,
            tooltipOption: a.tooltipOption
          }, o);
        else if (a.seriesIndex != null) {
          if (this._manuallyAxisShowTip(r, n, i, a))
            return;
          var h = U0(a, n), c = h.point[0], v = h.point[1];
          c != null && v != null && this._tryShow({
            offsetX: c,
            offsetY: v,
            target: h.el,
            position: a.position,
            // When manully trigger, the mouse is not on the el, so we'd better to
            // position tooltip on the bottom of the el and display arrow is possible.
            positionDefault: "bottom"
          }, o);
        } else a.x != null && a.y != null && (i.dispatchAction({
          type: "updateAxisPointer",
          x: a.x,
          y: a.y
        }), this._tryShow({
          offsetX: a.x,
          offsetY: a.y,
          position: a.position,
          target: i.getZr().findHover(a.x, a.y).target
        }, o));
      }
    }, e.prototype.manuallyHideTip = function(r, n, i, a) {
      var o = this._tooltipContent;
      this._tooltipModel && o.hideLater(this._tooltipModel.get("hideDelay")), this._lastX = this._lastY = this._lastDataByCoordSys = null, this._cbParamsList = null, a.from !== this.uid && this._hide(dg(a, i));
    }, e.prototype._manuallyAxisShowTip = function(r, n, i, a) {
      var o = a.seriesIndex, s = a.dataIndex, u = n.getComponent("axisPointer").coordSysAxesInfo;
      if (!(o == null || s == null || u == null)) {
        var l = n.getSeriesByIndex(o);
        if (l) {
          var f = l.getData(), h = Yi([f.getItemModel(s), l, (l.coordinateSystem || {}).model], this._tooltipModel);
          if (h.get("trigger") === "axis")
            return i.dispatchAction({
              type: "updateAxisPointer",
              seriesIndex: o,
              dataIndex: s,
              position: a.position
            }), !0;
        }
      }
    }, e.prototype._tryShow = function(r, n) {
      var i = r.target, a = this._tooltipModel;
      if (a) {
        this._lastX = r.offsetX, this._lastY = r.offsetY;
        var o = r.dataByCoordSys;
        if (o && o.length)
          this._showAxisTooltip(o, r);
        else if (i) {
          var s = he(i);
          if (s.ssrType === "legend")
            return;
          this._lastDataByCoordSys = null, this._cbParamsList = null;
          var u, l;
          Qi(i, function(f) {
            if (f.tooltipDisabled)
              return u = l = null, !0;
            u || l || (he(f).dataIndex != null ? u = f : he(f).tooltipConfig != null && (l = f));
          }, !0), u ? this._showSeriesItemTooltip(r, u, n) : l ? this._showComponentItemTooltip(r, l, n) : this._hide(n);
        } else
          this._lastDataByCoordSys = null, this._cbParamsList = null, this._hide(n);
      }
    }, e.prototype._showOrMove = function(r, n) {
      var i = r.get("showDelay");
      n = le(n, this), clearTimeout(this._showTimout), i > 0 ? this._showTimout = setTimeout(n, i) : n();
    }, e.prototype._showAxisTooltip = function(r, n) {
      var i = this._ecModel, a = this._tooltipModel, o = [n.offsetX, n.offsetY], s = Yi([n.tooltipOption], a), u = this._renderMode, l = [], f = Ca("section", {
        blocks: [],
        noHeader: !0
      }), h = [], c = new _l();
      w(r, function(m) {
        w(m.dataByAxis, function(_) {
          var S = i.getComponent(_.axisDim + "Axis", _.axisIndex), b = _.value, T = S.axis, C = T.scale.parse(b);
          if (!(!S || b == null)) {
            var M = H0(b, T, i, _.seriesDataIndices, _.valueLabelOpt), x = Ca("section", {
              header: M,
              noHeader: !It(M),
              sortBlocks: !0,
              blocks: []
            });
            f.blocks.push(x), w(_.seriesDataIndices, function(D) {
              var I = i.getSeriesByIndex(D.seriesIndex), L = D.dataIndexInside, A = I.getDataParams(L);
              if (!(A.dataIndex < 0)) {
                A.axisDim = _.axisDim, A.axisIndex = _.axisIndex, A.axisType = _.axisType, A.axisId = _.axisId, A.axisValue = Ps(S.axis, {
                  value: C
                }), A.axisValueLabel = M, A.marker = c.makeTooltipMarker("item", ba(A.color), u);
                var P = Vd(I.formatTooltip(L, !0, null)), k = P.frag;
                if (k) {
                  var O = Yi([I], a).get("valueFormatter");
                  x.blocks.push(O ? E({
                    valueFormatter: O
                  }, k) : k);
                }
                P.text && h.push(P.text), l.push(A);
              }
            });
          }
        });
      }), f.blocks.reverse(), h.reverse();
      var v = n.position, d = s.get("order"), y = qd(f, c, u, d, i.get("useUTC"), s.get("textStyle"));
      y && h.unshift(y);
      var p = u === "richText" ? `

` : "<br/>", g = h.join(p);
      this._showOrMove(s, function() {
        this._updateContentNotChangedOnAxis(r, l) ? this._updatePosition(s, v, o[0], o[1], this._tooltipContent, l) : this._showTooltipContent(s, g, l, Math.random() + "", o[0], o[1], v, null, c);
      });
    }, e.prototype._showSeriesItemTooltip = function(r, n, i) {
      var a = this._ecModel, o = he(n), s = o.seriesIndex, u = a.getSeriesByIndex(s), l = o.dataModel || u, f = o.dataIndex, h = o.dataType, c = l.getData(h), v = this._renderMode, d = r.positionDefault, y = Yi([c.getItemModel(f), l, u && (u.coordinateSystem || {}).model], this._tooltipModel, d ? {
        position: d
      } : null), p = y.get("trigger");
      if (!(p != null && p !== "item")) {
        var g = l.getDataParams(f, h), m = new _l();
        g.marker = m.makeTooltipMarker("item", ba(g.color), v);
        var _ = Vd(l.formatTooltip(f, !1, h)), S = y.get("order"), b = y.get("valueFormatter"), T = _.frag, C = T ? qd(b ? E({
          valueFormatter: b
        }, T) : T, m, v, S, a.get("useUTC"), y.get("textStyle")) : _.text, M = "item_" + l.name + "_" + f;
        this._showOrMove(y, function() {
          this._showTooltipContent(y, C, g, M, r.offsetX, r.offsetY, r.position, r.target, m);
        }), i({
          type: "showTip",
          dataIndexInside: f,
          dataIndex: c.getRawIndex(f),
          seriesIndex: s,
          from: this.uid
        });
      }
    }, e.prototype._showComponentItemTooltip = function(r, n, i) {
      var a = this._renderMode === "html", o = he(n), s = o.tooltipConfig, u = s.option || {}, l = u.encodeHTMLContent;
      if (N(u)) {
        var f = u;
        u = {
          content: f,
          // Fixed formatter
          formatter: f
        }, l = !0;
      }
      l && a && u.content && (u = Z(u), u.content = Ue(u.content));
      var h = [u], c = this._ecModel.getComponent(o.componentMainType, o.componentIndex);
      c && h.push(c), h.push({
        formatter: u.content
      });
      var v = r.positionDefault, d = Yi(h, this._tooltipModel, v ? {
        position: v
      } : null), y = d.get("content"), p = Math.random() + "", g = new _l();
      this._showOrMove(d, function() {
        var m = Z(d.get("formatterParams") || {});
        this._showTooltipContent(d, y, m, p, r.offsetX, r.offsetY, r.position, n, g);
      }), i({
        type: "showTip",
        from: this.uid
      });
    }, e.prototype._showTooltipContent = function(r, n, i, a, o, s, u, l, f) {
      if (this._ticket = "", !(!r.get("showContent") || !r.get("show"))) {
        var h = this._tooltipContent;
        h.setEnterable(r.get("enterable"));
        var c = r.get("formatter");
        u = u || r.get("position");
        var v = n, d = this._getNearestPoint([o, s], i, r.get("trigger"), r.get("borderColor"), r.get("defaultBorderColor", !0)), y = d.color;
        if (c)
          if (N(c)) {
            var p = r.ecModel.get("useUTC"), g = B(i) ? i[0] : i, m = g && g.axisType && g.axisType.indexOf("time") >= 0;
            v = c, m && (v = za(g.axisValue, v, p)), v = rc(v, i, !0);
          } else if (X(c)) {
            var _ = le(function(S, b) {
              S === this._ticket && (h.setContent(b, f, r, y, u), this._updatePosition(r, u, o, s, h, i, l));
            }, this);
            this._ticket = a, v = c(i, a, _);
          } else
            v = c;
        h.setContent(v, f, r, y, u), h.show(r, y), this._updatePosition(r, u, o, s, h, i, l);
      }
    }, e.prototype._getNearestPoint = function(r, n, i, a, o) {
      if (i === "axis" || B(n))
        return {
          color: a || o
        };
      if (!B(n))
        return {
          color: a || n.color || n.borderColor
        };
    }, e.prototype._updatePosition = function(r, n, i, a, o, s, u) {
      var l = this._api.getWidth(), f = this._api.getHeight();
      n = n || r.get("position");
      var h = o.getSize(), c = r.get("align"), v = r.get("verticalAlign"), d = u && u.getBoundingRect().clone();
      if (u && d.applyTransform(u.transform), X(n) && (n = n([i, a], s, o.el, d, {
        viewSize: [l, f],
        contentSize: h.slice()
      })), B(n))
        i = Dt(n[0], l), a = Dt(n[1], f);
      else if (z(n)) {
        var y = n;
        y.width = h[0], y.height = h[1];
        var p = ys(y, {
          width: l,
          height: f
        });
        i = p.x, a = p.y, c = null, v = null;
      } else if (N(n) && u) {
        var g = iR(n, d, h, r.get("borderWidth"));
        i = g[0], a = g[1];
      } else {
        var g = rR(i, a, o, l, f, c ? null : 20, v ? null : 20);
        i = g[0], a = g[1];
      }
      if (c && (i -= pg(c) ? h[0] / 2 : c === "right" ? h[0] : 0), v && (a -= pg(v) ? h[1] / 2 : v === "bottom" ? h[1] : 0), Y0(r)) {
        var g = nR(i, a, o, l, f);
        i = g[0], a = g[1];
      }
      o.moveTo(i, a);
    }, e.prototype._updateContentNotChangedOnAxis = function(r, n) {
      var i = this._lastDataByCoordSys, a = this._cbParamsList, o = !!i && i.length === r.length;
      return o && w(i, function(s, u) {
        var l = s.dataByAxis || [], f = r[u] || {}, h = f.dataByAxis || [];
        o = o && l.length === h.length, o && w(l, function(c, v) {
          var d = h[v] || {}, y = c.seriesDataIndices || [], p = d.seriesDataIndices || [];
          o = o && c.value === d.value && c.axisType === d.axisType && c.axisId === d.axisId && y.length === p.length, o && w(y, function(g, m) {
            var _ = p[m];
            o = o && g.seriesIndex === _.seriesIndex && g.dataIndex === _.dataIndex;
          }), a && w(c.seriesDataIndices, function(g) {
            var m = g.seriesIndex, _ = n[m], S = a[m];
            _ && S && S.data !== _.data && (o = !1);
          });
        });
      }), this._lastDataByCoordSys = r, this._cbParamsList = n, !!o;
    }, e.prototype._hide = function(r) {
      this._lastDataByCoordSys = null, this._cbParamsList = null, r({
        type: "hideTip",
        from: this.uid
      });
    }, e.prototype.dispose = function(r, n) {
      W.node || !n.getDom() || (Ff(this, "_updatePosition"), this._tooltipContent.dispose(), eh("itemTooltip", n), this._tooltipContent = null, this._tooltipModel = null, this._lastDataByCoordSys = null, this._cbParamsList = null);
    }, e.type = "tooltip", e;
  })(lt)
);
function Yi(t, e, r) {
  var n = e.ecModel, i;
  r ? (i = new ce(r, n, n), i = new ce(e.option, i, n)) : i = e;
  for (var a = t.length - 1; a >= 0; a--) {
    var o = t[a];
    o && (o instanceof ce && (o = o.get("tooltip", !0)), N(o) && (o = {
      formatter: o
    }), o && (i = new ce(o, i, n)));
  }
  return i;
}
function dg(t, e) {
  return t.dispatchAction || le(e.dispatchAction, e);
}
function rR(t, e, r, n, i, a, o) {
  var s = r.getSize(), u = s[0], l = s[1];
  return a != null && (t + u + a + 2 > n ? t -= u + a : t += a), o != null && (e + l + o > i ? e -= l + o : e += o), [t, e];
}
function nR(t, e, r, n, i) {
  var a = r.getSize(), o = a[0], s = a[1];
  return t = Math.min(t + o, n) - o, e = Math.min(e + s, i) - s, t = Math.max(t, 0), e = Math.max(e, 0), [t, e];
}
function iR(t, e, r, n) {
  var i = r[0], a = r[1], o = Math.ceil(Math.SQRT2 * n) + 8, s = 0, u = 0, l = e.width, f = e.height;
  switch (t) {
    case "inside":
      s = e.x + l / 2 - i / 2, u = e.y + f / 2 - a / 2;
      break;
    case "top":
      s = e.x + l / 2 - i / 2, u = e.y - a - o;
      break;
    case "bottom":
      s = e.x + l / 2 - i / 2, u = e.y + f + o;
      break;
    case "left":
      s = e.x - i - o, u = e.y + f / 2 - a / 2;
      break;
    case "right":
      s = e.x + l + o, u = e.y + f / 2 - a / 2;
  }
  return [s, u];
}
function pg(t) {
  return t === "center" || t === "middle";
}
function aR(t, e, r) {
  var n = wh(t).queryOptionMap, i = n.keys()[0];
  if (!(!i || i === "series")) {
    var a = ka(e, i, n.get(i), {
      useDefault: !1,
      enableAll: !1,
      enableNone: !1
    }), o = a.models[0];
    if (o) {
      var s = r.getViewOfComponentModel(o), u;
      if (s.group.traverse(function(l) {
        var f = he(l).tooltipConfig;
        if (f && f.name === t.name)
          return u = l, !0;
      }), u)
        return {
          componentMainType: i,
          componentIndex: o.componentIndex,
          el: u
        };
    }
  }
}
function l2(t) {
  vi(HL), t.registerComponentModel(GL), t.registerComponentView(tR), t.registerAction({
    type: "showTip",
    event: "showTip",
    update: "tooltip:manuallyShowTip"
  }, Re), t.registerAction({
    type: "hideTip",
    event: "hideTip",
    update: "tooltip:manuallyHideTip"
  }, Re);
}
var oR = function(t, e) {
  if (e === "all")
    return {
      type: "all",
      title: t.getLocaleModel().get(["legend", "selector", "all"])
    };
  if (e === "inverse")
    return {
      type: "inverse",
      title: t.getLocaleModel().get(["legend", "selector", "inverse"])
    };
}, th = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r.layoutMode = {
        type: "box",
        // legend.width/height are maxWidth/maxHeight actually,
        // whereas real width/height is calculated by its content.
        // (Setting {left: 10, right: 10} does not make sense).
        // So consider the case:
        // `setOption({legend: {left: 10});`
        // then `setOption({legend: {right: 10});`
        // The previous `left` should be cleared by setting `ignoreSize`.
        ignoreSize: !0
      }, r;
    }
    return e.prototype.init = function(r, n, i) {
      this.mergeDefaultAndTheme(r, i), r.selected = r.selected || {}, this._updateSelector(r);
    }, e.prototype.mergeOption = function(r, n) {
      t.prototype.mergeOption.call(this, r, n), this._updateSelector(r);
    }, e.prototype._updateSelector = function(r) {
      var n = r.selector, i = this.ecModel;
      n === !0 && (n = r.selector = ["all", "inverse"]), B(n) && w(n, function(a, o) {
        N(a) && (a = {
          type: a
        }), n[o] = pe(a, oR(i, a.type));
      });
    }, e.prototype.optionUpdated = function() {
      this._updateData(this.ecModel);
      var r = this._data;
      if (r[0] && this.get("selectedMode") === "single") {
        for (var n = !1, i = 0; i < r.length; i++) {
          var a = r[i].get("name");
          if (this.isSelected(a)) {
            this.select(a), n = !0;
            break;
          }
        }
        !n && this.select(r[0].get("name"));
      }
    }, e.prototype._updateData = function(r) {
      var n = [], i = [];
      r.eachRawSeries(function(u) {
        var l = u.name;
        i.push(l);
        var f;
        if (u.legendVisualProvider) {
          var h = u.legendVisualProvider, c = h.getAllNames();
          r.isSeriesFiltered(u) || (i = i.concat(c)), c.length ? n = n.concat(c) : f = !0;
        } else
          f = !0;
        f && Th(u) && n.push(u.name);
      }), this._availableNames = i;
      var a = this.get("data") || n, o = U(), s = H(a, function(u) {
        return (N(u) || ve(u)) && (u = {
          name: u
        }), o.get(u.name) ? null : (o.set(u.name, !0), new ce(u, this, this.ecModel));
      }, this);
      this._data = xe(s, function(u) {
        return !!u;
      });
    }, e.prototype.getData = function() {
      return this._data;
    }, e.prototype.select = function(r) {
      var n = this.option.selected, i = this.get("selectedMode");
      if (i === "single") {
        var a = this._data;
        w(a, function(o) {
          n[o.get("name")] = !1;
        });
      }
      n[r] = !0;
    }, e.prototype.unSelect = function(r) {
      this.get("selectedMode") !== "single" && (this.option.selected[r] = !1);
    }, e.prototype.toggleSelected = function(r) {
      var n = this.option.selected;
      n.hasOwnProperty(r) || (n[r] = !0), this[n[r] ? "unSelect" : "select"](r);
    }, e.prototype.allSelect = function() {
      var r = this._data, n = this.option.selected;
      w(r, function(i) {
        n[i.get("name", !0)] = !0;
      });
    }, e.prototype.inverseSelect = function() {
      var r = this._data, n = this.option.selected;
      w(r, function(i) {
        var a = i.get("name", !0);
        n.hasOwnProperty(a) || (n[a] = !0), n[a] = !n[a];
      });
    }, e.prototype.isSelected = function(r) {
      var n = this.option.selected;
      return !(n.hasOwnProperty(r) && !n[r]) && oe(this._availableNames, r) >= 0;
    }, e.prototype.getOrient = function() {
      return this.get("orient") === "vertical" ? {
        index: 1,
        name: "vertical"
      } : {
        index: 0,
        name: "horizontal"
      };
    }, e.type = "legend.plain", e.dependencies = ["series"], e.defaultOption = {
      // zlevel: 0,
      z: 4,
      show: !0,
      orient: "horizontal",
      left: "center",
      // right: 'center',
      // top: 0,
      bottom: j.size.m,
      align: "auto",
      backgroundColor: j.color.transparent,
      borderColor: j.color.border,
      borderRadius: 0,
      borderWidth: 0,
      padding: 5,
      itemGap: 8,
      itemWidth: 25,
      itemHeight: 14,
      symbolRotate: "inherit",
      symbolKeepAspect: !0,
      inactiveColor: j.color.disabled,
      inactiveBorderColor: j.color.disabled,
      inactiveBorderWidth: "auto",
      itemStyle: {
        color: "inherit",
        opacity: "inherit",
        borderColor: "inherit",
        borderWidth: "auto",
        borderCap: "inherit",
        borderJoin: "inherit",
        borderDashOffset: "inherit",
        borderMiterLimit: "inherit"
      },
      lineStyle: {
        width: "auto",
        color: "inherit",
        inactiveColor: j.color.disabled,
        inactiveWidth: 2,
        opacity: "inherit",
        type: "inherit",
        cap: "inherit",
        join: "inherit",
        dashOffset: "inherit",
        miterLimit: "inherit"
      },
      textStyle: {
        color: j.color.secondary
      },
      selectedMode: !0,
      selector: !1,
      selectorLabel: {
        show: !0,
        borderRadius: 10,
        padding: [3, 5, 3, 5],
        fontSize: 12,
        fontFamily: "sans-serif",
        color: j.color.tertiary,
        borderWidth: 1,
        borderColor: j.color.border
      },
      emphasis: {
        selectorLabel: {
          show: !0,
          color: j.color.quaternary
        }
      },
      selectorPosition: "auto",
      selectorItemGap: 7,
      selectorButtonGap: 10,
      tooltip: {
        show: !1
      },
      triggerEvent: !1
    }, e;
  })(ae)
), qn = we, rh = w, Ao = ut, $0 = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r.newlineDisabled = !1, r;
    }
    return e.prototype.init = function() {
      this.group.add(this._contentGroup = new Ao()), this.group.add(this._selectorGroup = new Ao()), this._isFirstRender = !0;
    }, e.prototype.getContentGroup = function() {
      return this._contentGroup;
    }, e.prototype.getSelectorGroup = function() {
      return this._selectorGroup;
    }, e.prototype.render = function(r, n, i) {
      var a = this._isFirstRender;
      if (this._isFirstRender = !1, this.resetInner(), !!r.get("show", !0)) {
        var o = r.get("align"), s = r.get("orient");
        (!o || o === "auto") && (o = r.get("left") === "right" && s === "vertical" ? "right" : "left");
        var u = r.get("selector", !0), l = r.get("selectorPosition", !0);
        u && (!l || l === "auto") && (l = s === "horizontal" ? "end" : "start"), this.renderInner(o, r, n, i, u, s, l);
        var f = Ax(r, i).refContainer, h = r.getBoxLayoutParams(), c = r.get("padding"), v = ys(h, f, c), d = this.layoutInner(r, o, v, a, u, l), y = ys(_e({
          width: d.width,
          height: d.height
        }, h), f, c);
        this.group.x = y.x - d.x, this.group.y = y.y - d.y, this.group.markRedraw(), this.group.add(this._backgroundEl = VL(
          d,
          // FXIME: most itemStyle options does not work in background because inherit is not handled yet.
          r
        ));
      }
    }, e.prototype.resetInner = function() {
      this.getContentGroup().removeAll(), this._backgroundEl && this.group.remove(this._backgroundEl), this.getSelectorGroup().removeAll();
    }, e.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this.getContentGroup(), f = U(), h = n.get("selectedMode"), c = n.get("triggerEvent"), v = [];
      i.eachRawSeries(function(d) {
        !d.get("legendHoverLink") && v.push(d.id);
      }), rh(n.getData(), function(d, y) {
        var p = this, g = d.get("name");
        if (!this.newlineDisabled && (g === "" || g === `
`)) {
          var m = new Ao();
          m.newline = !0, l.add(m);
          return;
        }
        var _ = i.getSeriesByName(g)[0];
        if (!f.get(g))
          if (_) {
            var S = _.getData(), b = S.getVisual("legendLineStyle") || {}, T = S.getVisual("legendIcon"), C = S.getVisual("style"), M = this._createItem(_, g, y, d, n, r, b, C, T, h, a);
            M.on("click", qn(gg, g, null, a, v)).on("mouseover", qn(nh, _.name, null, a, v)).on("mouseout", qn(ih, _.name, null, a, v)), i.ssr && M.eachChild(function(x) {
              var D = he(x);
              D.seriesIndex = _.seriesIndex, D.dataIndex = y, D.ssrType = "legend";
            }), c && M.eachChild(function(x) {
              p.packEventData(x, n, _, y, g);
            }), f.set(g, !0);
          } else
            i.eachRawSeries(function(x) {
              var D = this;
              if (!f.get(g) && x.legendVisualProvider) {
                var I = x.legendVisualProvider;
                if (!I.containName(g))
                  return;
                var L = I.indexOfName(g), A = I.getItemVisual(L, "style"), P = I.getItemVisual(L, "legendIcon"), k = st(A.fill);
                k && k[3] === 0 && (k[3] = 0.2, A = E(E({}, A), {
                  fill: Br(k, "rgba")
                }));
                var O = this._createItem(x, g, y, d, n, r, {}, A, P, h, a);
                O.on("click", qn(gg, null, g, a, v)).on("mouseover", qn(nh, null, g, a, v)).on("mouseout", qn(ih, null, g, a, v)), i.ssr && O.eachChild(function(V) {
                  var K = he(V);
                  K.seriesIndex = x.seriesIndex, K.dataIndex = y, K.ssrType = "legend";
                }), c && O.eachChild(function(V) {
                  D.packEventData(V, n, x, y, g);
                }), f.set(g, !0);
              }
            }, this);
      }, this), o && this._createSelector(o, n, a, s, u);
    }, e.prototype.packEventData = function(r, n, i, a, o) {
      var s = {
        componentType: "legend",
        componentIndex: n.componentIndex,
        dataIndex: a,
        value: o,
        seriesIndex: i.seriesIndex
      };
      he(r).eventData = s;
    }, e.prototype._createSelector = function(r, n, i, a, o) {
      var s = this.getSelectorGroup();
      rh(r, function(l) {
        var f = l.type, h = new je({
          style: {
            x: 0,
            y: 0,
            align: "center",
            verticalAlign: "middle"
          },
          onclick: function() {
            i.dispatchAction({
              type: f === "all" ? "legendAllSelect" : "legendInverseSelect",
              legendId: n.id
            });
          }
        });
        s.add(h);
        var c = n.getModel("selectorLabel"), v = n.getModel(["emphasis", "selectorLabel"]);
        VC(h, {
          normal: c,
          emphasis: v
        }, {
          defaultText: l.title
        }), vs(h);
      });
    }, e.prototype._createItem = function(r, n, i, a, o, s, u, l, f, h, c) {
      var v = r.visualDrawType, d = o.get("itemWidth"), y = o.get("itemHeight"), p = o.isSelected(n), g = a.get("symbolRotate"), m = a.get("symbolKeepAspect"), _ = a.get("icon");
      f = _ || f || "roundRect";
      var S = sR(f, a, u, l, v, p, c), b = new Ao(), T = a.getModel("textStyle");
      if (X(r.getLegendIcon) && (!_ || _ === "inherit"))
        b.add(r.getLegendIcon({
          itemWidth: d,
          itemHeight: y,
          icon: f,
          iconRotate: g,
          itemStyle: S.itemStyle,
          lineStyle: S.lineStyle,
          symbolKeepAspect: m
        }));
      else {
        var C = _ === "inherit" && r.getData().getVisual("symbol") ? g === "inherit" ? r.getData().getVisual("symbolRotate") : g : 0;
        b.add(uR({
          itemWidth: d,
          itemHeight: y,
          icon: f,
          iconRotate: C,
          itemStyle: S.itemStyle,
          symbolKeepAspect: m
        }));
      }
      var M = s === "left" ? d + 5 : -5, x = s, D = o.get("formatter"), I = n;
      N(D) && D ? I = D.replace("{name}", n ?? "") : X(D) && (I = D(n));
      var L = p ? T.getTextColor() : a.get("inactiveColor");
      b.add(new je({
        style: Cn(T, {
          text: I,
          x: M,
          y: y / 2,
          fill: L,
          align: x,
          verticalAlign: "middle"
        }, {
          inheritColor: L
        })
      }));
      var A = new Ee({
        shape: b.getBoundingRect(),
        style: {
          // Cannot use 'invisible' because SVG SSR will miss the node
          fill: "transparent"
        }
      }), P = a.getModel("tooltip");
      return P.get("show") && su({
        el: A,
        componentModel: o,
        itemName: n,
        itemTooltipOption: P.option
      }), b.add(A), b.eachChild(function(k) {
        k.silent = !0;
      }), A.silent = !h, this.getContentGroup().add(b), vs(b), b.__legendDataIndex = i, b;
    }, e.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getContentGroup(), l = this.getSelectorGroup();
      oa(r.get("orient"), u, r.get("itemGap"), i.width, i.height);
      var f = u.getBoundingRect(), h = [-f.x, -f.y];
      if (l.markRedraw(), u.markRedraw(), o) {
        oa(
          // Buttons in selectorGroup always layout horizontally
          "horizontal",
          l,
          r.get("selectorItemGap", !0)
        );
        var c = l.getBoundingRect(), v = [-c.x, -c.y], d = r.get("selectorButtonGap", !0), y = r.getOrient().index, p = y === 0 ? "width" : "height", g = y === 0 ? "height" : "width", m = y === 0 ? "y" : "x";
        s === "end" ? v[y] += f[p] + d : h[y] += c[p] + d, v[1 - y] += f[g] / 2 - c[g] / 2, l.x = v[0], l.y = v[1], u.x = h[0], u.y = h[1];
        var _ = {
          x: 0,
          y: 0
        };
        return _[p] = f[p] + d + c[p], _[g] = Math.max(f[g], c[g]), _[m] = Math.min(0, c[m] + v[1 - y]), _;
      } else
        return u.x = h[0], u.y = h[1], this.group.getBoundingRect();
    }, e.prototype.remove = function() {
      this.getContentGroup().removeAll(), this._isFirstRender = !0;
    }, e.type = "legend.plain", e;
  })(lt)
);
function sR(t, e, r, n, i, a, o) {
  function s(p, g) {
    p.lineWidth === "auto" && (p.lineWidth = g.lineWidth > 0 ? 2 : 0), rh(p, function(m, _) {
      p[_] === "inherit" && (p[_] = g[_]);
    });
  }
  var u = e.getModel("itemStyle"), l = u.getItemStyle(), f = t.lastIndexOf("empty", 0) === 0 ? "fill" : "stroke", h = u.getShallow("decal");
  l.decal = !h || h === "inherit" ? n.decal : Uf(h, o), l.fill === "inherit" && (l.fill = n[i]), l.stroke === "inherit" && (l.stroke = n[f]), l.opacity === "inherit" && (l.opacity = (i === "fill" ? n : r).opacity), s(l, n);
  var c = e.getModel("lineStyle"), v = c.getLineStyle();
  if (s(v, r), l.fill === "auto" && (l.fill = n.fill), l.stroke === "auto" && (l.stroke = n.fill), v.stroke === "auto" && (v.stroke = n.fill), !a) {
    var d = e.get("inactiveBorderWidth"), y = l[f];
    l.lineWidth = d === "auto" ? n.lineWidth > 0 && y ? 2 : 0 : l.lineWidth, l.fill = e.get("inactiveColor"), l.stroke = e.get("inactiveBorderColor"), v.stroke = c.get("inactiveColor"), v.lineWidth = c.get("inactiveWidth");
  }
  return {
    itemStyle: l,
    lineStyle: v
  };
}
function uR(t) {
  var e = t.icon || "roundRect", r = vu(e, 0, 0, t.itemWidth, t.itemHeight, t.itemStyle.fill, t.symbolKeepAspect);
  return r.setStyle(t.itemStyle), r.rotation = (t.iconRotate || 0) * Math.PI / 180, r.setOrigin([t.itemWidth / 2, t.itemHeight / 2]), e.indexOf("empty") > -1 && (r.style.stroke = r.style.fill, r.style.fill = j.color.neutral00, r.style.lineWidth = 2), r;
}
function gg(t, e, r, n) {
  ih(t, e, r, n), r.dispatchAction({
    type: "legendToggleSelect",
    name: t ?? e
  }), nh(t, e, r, n);
}
function nh(t, e, r, n) {
  r.usingTHL() || r.dispatchAction({
    type: "highlight",
    seriesName: t,
    name: e,
    excludeSeriesId: n
  });
}
function ih(t, e, r, n) {
  r.usingTHL() || r.dispatchAction({
    type: "downplay",
    seriesName: t,
    name: e,
    excludeSeriesId: n
  });
}
function Wi(t, e, r) {
  var n = t === "allSelect" || t === "inverseSelect", i = {}, a = [];
  r.eachComponent({
    mainType: "legend",
    query: e
  }, function(s) {
    n ? s[t]() : s[t](e.name), yg(s, i), a.push(s.componentIndex);
  });
  var o = {};
  return r.eachComponent("legend", function(s) {
    w(i, function(u, l) {
      s[u ? "select" : "unSelect"](l);
    }), yg(s, o);
  }), n ? {
    selected: o,
    // return legendIndex array to tell the developers which legends are allSelect / inverseSelect
    legendIndex: a
  } : {
    name: e.name,
    selected: o
  };
}
function yg(t, e) {
  var r = e || {};
  return w(t.getData(), function(n) {
    var i = n.get("name");
    if (!(i === `
` || i === "")) {
      var a = t.isSelected(i);
      qe(r, i) ? r[i] = r[i] && a : r[i] = a;
    }
  }), r;
}
function lR(t) {
  t.registerAction("legendToggleSelect", "legendselectchanged", we(Wi, "toggleSelected")), t.registerAction("legendAllSelect", "legendselectall", we(Wi, "allSelect")), t.registerAction("legendInverseSelect", "legendinverseselect", we(Wi, "inverseSelect")), t.registerAction("legendSelect", "legendselected", we(Wi, "select")), t.registerAction("legendUnSelect", "legendunselected", we(Wi, "unSelect"));
}
var fR = xh(hR);
function hR(t) {
  var e = t.findComponents({
    mainType: "legend"
  });
  e && e.length && t.filterSeries(function(r) {
    for (var n = 0; n < e.length; n++)
      if (!e[n].isSelected(r.name))
        return !1;
    return !0;
  });
}
function Z0(t) {
  t.registerComponentModel(th), t.registerComponentView($0), t.registerProcessor(t.PRIORITY.PROCESSOR.SERIES_FILTER, fR), t.registerSubTypeDefaulter("legend", function() {
    return "plain";
  }), lR(t);
}
var cR = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.setScrollDataIndex = function(r) {
      this.option.scrollDataIndex = r;
    }, e.prototype.init = function(r, n, i) {
      var a = ic(r);
      t.prototype.init.call(this, r, n, i), mg(this, r, a);
    }, e.prototype.mergeOption = function(r, n) {
      t.prototype.mergeOption.call(this, r, n), mg(this, this.option, r);
    }, e.type = "legend.scroll", e.defaultOption = ex(th.defaultOption, {
      scrollDataIndex: 0,
      pageButtonItemGap: 5,
      pageButtonGap: null,
      pageButtonPosition: "end",
      pageFormatter: "{current}/{total}",
      pageIcons: {
        horizontal: ["M0,0L12,-10L12,10z", "M0,0L-12,-10L-12,10z"],
        vertical: ["M0,0L20,0L10,-20z", "M0,0L20,0L10,20z"]
      },
      pageIconColor: j.color.accent50,
      pageIconInactiveColor: j.color.accent10,
      pageIconSize: 15,
      pageTextStyle: {
        color: j.color.tertiary
      },
      animationDurationUpdate: 800
    }), e;
  })(th)
);
function mg(t, e, r) {
  var n = t.getOrient(), i = [1, 1];
  i[n.index] = 0, Ta(e, r, {
    type: "box",
    ignoreSize: !!i
  });
}
var _g = ut, Fl = ["width", "height"], zl = ["x", "y"], vR = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r.newlineDisabled = !0, r._currentIndex = 0, r;
    }
    return e.prototype.init = function() {
      t.prototype.init.call(this), this.group.add(this._containerGroup = new _g()), this._containerGroup.add(this.getContentGroup()), this.group.add(this._controllerGroup = new _g());
    }, e.prototype.resetInner = function() {
      t.prototype.resetInner.call(this), this._controllerGroup.removeAll(), this._containerGroup.removeClipPath(), this._containerGroup.__rectSize = null;
    }, e.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this;
      t.prototype.renderInner.call(this, r, n, i, a, o, s, u);
      var f = this._controllerGroup, h = n.get("pageIconSize", !0), c = B(h) ? h : [h, h];
      d("pagePrev", 0);
      var v = n.getModel("pageTextStyle");
      f.add(new je({
        name: "pageText",
        style: {
          // Placeholder to calculate a proper layout.
          text: "xx/xx",
          fill: v.getTextColor(),
          font: v.getFont(),
          verticalAlign: "middle",
          align: "center"
        },
        silent: !0
      })), d("pageNext", 1);
      function d(y, p) {
        var g = y + "DataIndex", m = ou(n.get("pageIcons", !0)[n.getOrient().name][p], {
          // Buttons will be created in each render, so we do not need
          // to worry about avoiding using legendModel kept in scope.
          onclick: le(l._pageGo, l, g, n, a)
        }, {
          x: -c[0] / 2,
          y: -c[1] / 2,
          width: c[0],
          height: c[1]
        });
        m.name = y, f.add(m);
      }
    }, e.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getSelectorGroup(), l = r.getOrient().index, f = Fl[l], h = zl[l], c = Fl[1 - l], v = zl[1 - l];
      o && oa(
        // Buttons in selectorGroup always layout horizontally
        "horizontal",
        u,
        r.get("selectorItemGap", !0)
      );
      var d = r.get("selectorButtonGap", !0), y = u.getBoundingRect(), p = [-y.x, -y.y], g = Z(i);
      o && (g[f] = i[f] - y[f] - d);
      var m = this._layoutContentAndController(r, a, g, l, f, c, v, h);
      if (o) {
        if (s === "end")
          p[l] += m[f] + d;
        else {
          var _ = y[f] + d;
          p[l] -= _, m[h] -= _;
        }
        m[f] += y[f] + d, p[1 - l] += m[v] + m[c] / 2 - y[c] / 2, m[c] = Math.max(m[c], y[c]), m[v] = Math.min(m[v], y[v] + p[1 - l]), u.x = p[0], u.y = p[1], u.markRedraw();
      }
      return m;
    }, e.prototype._layoutContentAndController = function(r, n, i, a, o, s, u, l) {
      var f = this.getContentGroup(), h = this._containerGroup, c = this._controllerGroup;
      oa(r.get("orient"), f, r.get("itemGap"), a ? i.width : null, a ? null : i.height), oa(
        // Buttons in controller are layout always horizontally.
        "horizontal",
        c,
        r.get("pageButtonItemGap", !0)
      );
      var v = f.getBoundingRect(), d = c.getBoundingRect(), y = this._showController = v[o] > i[o], p = [-v.x, -v.y];
      n || (p[a] = f[l]);
      var g = [0, 0], m = [-d.x, -d.y], _ = G(r.get("pageButtonGap", !0), r.get("itemGap", !0));
      if (y) {
        var S = r.get("pageButtonPosition", !0);
        S === "end" ? m[a] += i[o] - d[o] : g[a] += d[o] + _;
      }
      m[1 - a] += v[s] / 2 - d[s] / 2, f.setPosition(p), h.setPosition(g), c.setPosition(m);
      var b = {
        x: 0,
        y: 0
      };
      if (b[o] = y ? i[o] : v[o], b[s] = Math.max(v[s], d[s]), b[u] = Math.min(0, d[u] + m[1 - a]), h.__rectSize = i[o], y) {
        var T = {
          x: 0,
          y: 0
        };
        T[o] = Math.max(i[o] - d[o] - _, 0), T[s] = b[s], h.setClipPath(new Ee({
          shape: T
        })), h.__rectSize = T[o];
      } else
        c.eachChild(function(M) {
          M.attr({
            invisible: !0,
            silent: !0
          });
        });
      var C = this._getPageInfo(r);
      return C.pageIndex != null && yi(
        f,
        {
          x: C.contentPosition[0],
          y: C.contentPosition[1]
        },
        // When switch from "show controller" to "not show controller", view should be
        // updated immediately without animation, otherwise causes weird effect.
        y ? r : null
      ), this._updatePageInfoView(r, C), b;
    }, e.prototype._pageGo = function(r, n, i) {
      var a = this._getPageInfo(n)[r];
      a != null && i.dispatchAction({
        type: "legendScroll",
        scrollDataIndex: a,
        legendId: n.id
      });
    }, e.prototype._updatePageInfoView = function(r, n) {
      var i = this._controllerGroup;
      w(["pagePrev", "pageNext"], function(f) {
        var h = f + "DataIndex", c = n[h] != null, v = i.childOfName(f);
        v && (v.setStyle("fill", c ? r.get("pageIconColor", !0) : r.get("pageIconInactiveColor", !0)), v.cursor = c ? "pointer" : "default");
      });
      var a = i.childOfName("pageText"), o = r.get("pageFormatter"), s = n.pageIndex, u = s != null ? s + 1 : 0, l = n.pageCount;
      a && o && a.setStyle("text", N(o) ? o.replace("{current}", u == null ? "" : u + "").replace("{total}", l == null ? "" : l + "") : o({
        current: u,
        total: l
      }));
    }, e.prototype._getPageInfo = function(r) {
      var n = r.get("scrollDataIndex", !0), i = this.getContentGroup(), a = this._containerGroup.__rectSize, o = r.getOrient().index, s = Fl[o], u = zl[o], l = this._findTargetItemIndex(n), f = i.children(), h = f[l], c = f.length, v = c ? 1 : 0, d = {
        contentPosition: [i.x, i.y],
        pageCount: v,
        pageIndex: v - 1,
        pagePrevDataIndex: null,
        pageNextDataIndex: null
      };
      if (!h)
        return d;
      var y = S(h);
      d.contentPosition[o] = -y.s;
      for (var p = l + 1, g = y, m = y, _ = null; p <= c; ++p)
        _ = S(f[p]), // Half of the last item is out of the window.
        (!_ && m.e > g.s + a || _ && !b(_, g.s)) && (m.i > g.i ? g = m : g = _, g && (d.pageNextDataIndex == null && (d.pageNextDataIndex = g.i), ++d.pageCount)), m = _;
      for (var p = l - 1, g = y, m = y, _ = null; p >= -1; --p)
        _ = S(f[p]), // If the the end item does not intersect with the window started
        // from the current item, a page can be settled.
        (!_ || !b(m, _.s)) && g.i < m.i && (m = g, d.pagePrevDataIndex == null && (d.pagePrevDataIndex = g.i), ++d.pageCount, ++d.pageIndex), g = _;
      return d;
      function S(T) {
        if (T) {
          var C = T.getBoundingRect(), M = C[u] + T[u];
          return {
            s: M,
            e: M + C[s],
            i: T.__legendDataIndex
          };
        }
      }
      function b(T, C) {
        return T.e >= C && T.s <= C + a;
      }
    }, e.prototype._findTargetItemIndex = function(r) {
      if (!this._showController)
        return 0;
      var n, i = this.getContentGroup(), a;
      return i.eachChild(function(o, s) {
        var u = o.__legendDataIndex;
        a == null && u != null && (a = s), u === r && (n = s);
      }), n ?? a;
    }, e.type = "legend.scroll", e;
  })($0)
);
function dR(t) {
  t.registerAction("legendScroll", "legendscroll", function(e, r) {
    var n = e.scrollDataIndex;
    n != null && r.eachComponent({
      mainType: "legend",
      subType: "scroll",
      query: e
    }, function(i) {
      i.setScrollDataIndex(n);
    });
  });
}
function pR(t) {
  vi(Z0), t.registerComponentModel(cR), t.registerComponentView(vR), dR(t);
}
function f2(t) {
  vi(Z0), vi(pR);
}
var gR = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return e.prototype.init = function(r, n, i) {
      t.prototype.init.call(this, r, n, i), this._sourceManager = new u_(this), Wd(this);
    }, e.prototype.mergeOption = function(r, n) {
      t.prototype.mergeOption.call(this, r, n), Wd(this);
    }, e.prototype.optionUpdated = function() {
      this._sourceManager.dirty();
    }, e.prototype.getSourceManager = function() {
      return this._sourceManager;
    }, e.type = "dataset", e.defaultOption = {
      seriesLayoutBy: Pt
    }, e;
  })(ae)
), yR = (
  /** @class */
  (function(t) {
    F(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return e.type = "dataset", e;
  })(lt)
);
function h2(t) {
  t.registerComponentModel(gR), t.registerComponentView(yR);
}
function Sg(t, e, r) {
  var n = Le.createCanvas(), i = e.getWidth(), a = e.getHeight(), o = n.style;
  return o && (o.position = "absolute", o.left = "0", o.top = "0", o.width = i + "px", o.height = a + "px", n.setAttribute("data-zr-dom-id", t)), n.width = i * r, n.height = a * r, n;
}
function Hl(t) {
  return !t.__cursors.get(um);
}
function bg(t) {
  var e = t.__cursors.get(um);
  return {
    startIdx: e ? e.startIdx : 0,
    endIdx: e ? e.endIdx : 0
  };
}
var K0 = (function(t) {
  F(e, t);
  function e(r, n, i) {
    var a = t.call(this) || this;
    a.motionBlur = !1, a.lastFrameAlpha = 0.7, a.dpr = 1, a.virtual = !1, a.config = {}, a.zlevel = 0, a.zlevel2 = Uo, a.maxRepaintRectCount = 5, a.__dirty = !0, a.__firstTimePaint = !0, a.__prevIdx = { startIdx: 0, endIdx: 0 };
    var o;
    i = i || ss, typeof r == "string" ? o = Sg(r, n, i) : z(r) && (o = r, r = o.id), a.id = r, a.dom = o;
    var s = o.style;
    return s && (vh(o), o.onselectstart = function() {
      return !1;
    }, s.padding = "0", s.margin = "0", s.borderWidth = "0"), a.painter = n, a.dpr = i, a;
  }
  return e.prototype.afterBrush = function() {
    this.__prevIdx = bg(this);
  }, e.prototype.initContext = function() {
    this.ctx = this.dom.getContext("2d"), this.ctx.dpr = this.dpr;
  }, e.prototype.setUnpainted = function() {
    this.__firstTimePaint = !0;
  }, e.prototype.createBackBuffer = function() {
    var r = this.dpr;
    this.domBack = Sg("back-" + this.id, this.painter, r), this.ctxBack = this.domBack.getContext("2d"), r !== 1 && this.ctxBack.scale(r, r);
  }, e.prototype.createRepaintRects = function(r, n, i, a) {
    if (this.__firstTimePaint)
      return this.__firstTimePaint = !1, null;
    var o = [], s = this.maxRepaintRectCount, u = !1, l = new Y(0, 0, 0, 0);
    function f(S) {
      if (!(!S.isFinite() || S.isZero()))
        if (o.length === 0) {
          var b = new Y(0, 0, 0, 0);
          b.copy(S), o.push(b);
        } else {
          for (var T = !1, C = 1 / 0, M = 0, x = 0; x < o.length; ++x) {
            var D = o[x];
            if (D.intersect(S)) {
              var I = new Y(0, 0, 0, 0);
              I.copy(D), I.union(S), o[x] = I, T = !0;
              break;
            } else if (u) {
              l.copy(S), l.union(D);
              var L = S.width * S.height, A = D.width * D.height, P = l.width * l.height, k = P - L - A;
              k < C && (C = k, M = x);
            }
          }
          if (u && (o[M].union(S), T = !0), !T) {
            var b = new Y(0, 0, 0, 0);
            b.copy(S), o.push(b);
          }
          u || (u = o.length >= s);
        }
    }
    for (var h = bg(this), c = h.startIdx; c < h.endIdx; ++c) {
      var v = r[c];
      if (v) {
        var d = v.shouldBePainted(i, a, !0, !0), y = v.__isRendered && (v.__dirty & Je || !d) ? v.getPrevPaintRect() : null;
        y && f(y);
        var p = d && (v.__dirty & Je || !v.__isRendered) ? v.getPaintRect() : null;
        p && f(p);
      }
    }
    for (var g = this.__prevIdx, c = g.startIdx; c < g.endIdx; ++c) {
      var v = n[c], d = v && v.shouldBePainted(i, a, !0, !0);
      if (v && (!d || !v.__zr) && v.__isRendered) {
        var y = v.getPrevPaintRect();
        y && f(y);
      }
    }
    var m;
    do {
      m = !1;
      for (var c = 0; c < o.length; ) {
        if (o[c].isZero()) {
          o.splice(c, 1);
          continue;
        }
        for (var _ = c + 1; _ < o.length; )
          o[c].intersect(o[_]) ? (m = !0, o[c].union(o[_]), o.splice(_, 1)) : _++;
        c++;
      }
    } while (m);
    return this._paintRects = o, o;
  }, e.prototype.debugGetPaintRects = function() {
    return (this._paintRects || []).slice();
  }, e.prototype.resize = function(r, n) {
    var i = this.dpr, a = this.dom, o = a.style, s = this.domBack;
    o && (o.width = r + "px", o.height = n + "px"), a.width = r * i, a.height = n * i, s && (s.width = r * i, s.height = n * i, i !== 1 && this.ctxBack.scale(i, i));
  }, e.prototype.clear = function(r, n, i) {
    var a = this.dom, o = this.ctx, s = a.width, u = a.height;
    n = n || this.clearColor;
    var l = this.motionBlur && !r, f = this.lastFrameAlpha, h = this.dpr, c = this;
    l && (this.domBack || this.createBackBuffer(), this.ctxBack.globalCompositeOperation = "copy", this.ctxBack.drawImage(a, 0, 0, s / h, u / h));
    var v = this.domBack;
    function d(y, p, g, m) {
      if (o.clearRect(y, p, g, m), n && n !== "transparent") {
        var _ = void 0;
        if (Aa(n)) {
          var S = n.global || n.__width === g && n.__height === m;
          _ = S && n.__canvasGradient || Vf(o, n, {
            x: 0,
            y: 0,
            width: g,
            height: m
          }), n.__canvasGradient = _, n.__width = g, n.__height = m;
        } else Lg(n) && (n.scaleX = n.scaleX || h, n.scaleY = n.scaleY || h, _ = Gf(o, n, {
          dirty: function() {
            c.setUnpainted(), c.painter.refresh();
          }
        }));
        o.save(), o.fillStyle = _ || n, o.fillRect(y, p, g, m), o.restore();
      }
      l && (o.save(), o.globalAlpha = f, o.drawImage(v, y, p, g, m), o.restore());
    }
    !i || l ? d(0, 0, s, u) : i.length && w(i, function(y) {
      d(y.x * h, y.y * h, y.width * h, y.height * h);
    });
  }, e;
})(Kt), Tg = 1e5, ln = 314159, Vl = void 0, mR = 1, Gl = 2;
function _R(t) {
  return t ? t.__builtin__ ? !0 : !(typeof t.resize != "function" || typeof t.refresh != "function") : !1;
}
function SR(t, e) {
  var r = document.createElement("div");
  return r.style.cssText = [
    "position:relative",
    "width:" + t + "px",
    "height:" + e + "px",
    "padding:0",
    "margin:0",
    "border-width:0"
  ].join(";") + ";", r;
}
function wg(t, e, r, n) {
  var i = new K0(t, e, e.dpr);
  return i.zlevel = r, i.zlevel2 = n, i.__builtin__ = !0, j0(i), i;
}
function j0(t) {
  t.__cursorStack = [], t.__cursors = U();
}
function bR(t) {
  return t.startIdx = t.drawIdx = t.endIdx = t.endIdxNew = 0, t.used = !1, t.first = t.last = NaN, t.notClearIdx = -1, t;
}
function TR(t, e) {
  var r = t.__cursors, n = +e;
  return r.get(n) || (t.__cursorStack.push(n), r.set(n, bR({ key: n })));
}
function Po(t, e) {
  for (var r = t.__cursorStack, n = 0; n < r.length; n++)
    e(t.__cursors.get(r[n]));
}
function Ul(t, e) {
  var r = t.layers;
  return r[e] || (r[e] = new Array(3));
}
function ke(t, e, r) {
  for (var n = t.layerStack, i = 0; i < n.length; i++) {
    var a = n[i].zl, o = n[i].zl2, s = t.layers[a][o];
    (!r || (!(r & la) || s.__builtin__) && (!(r & ah) || !s.__builtin__) && (!(r & Q0) || s !== t.hoverlayer)) && e(s, a, o, i);
  }
}
var la = 1, ah = 2, Q0 = 4, Lo = la | Q0, wR = (function() {
  function t(e, r, n, i) {
    this.type = "canvas", this._prevDisplayList = [], this._layerConfig = {}, this._needsManuallyCompositing = !1, this.type = "canvas", this._i = {
      layerStack: [],
      layers: []
    };
    var a = !e.nodeName || e.nodeName.toUpperCase() === "CANVAS";
    this._opts = n = E({}, n || {}), this.dpr = n.devicePixelRatio || ss, this._singleCanvas = a, this.root = e;
    var o = e.style;
    if (o && (vh(e), e.innerHTML = ""), this.storage = r, this._prevDisplayList = [], a) {
      var u = e, l = u.width, f = u.height;
      n.width != null && (l = n.width), n.height != null && (f = n.height), this.dpr = n.devicePixelRatio || 1, u.width = l * this.dpr, u.height = f * this.dpr, this._width = l, this._height = f;
      var h = wg(u, this, ln, Uo);
      h.initContext(), this._insertLayer(h, ln, Uo, !0), this._domRoot = e;
    } else {
      this._width = mo(e, 0, n), this._height = mo(e, 1, n);
      var s = this._domRoot = SR(this._width, this._height);
      e.appendChild(s);
    }
  }
  return t.prototype.getType = function() {
    return "canvas";
  }, t.prototype.isSingleCanvas = function() {
    return this._singleCanvas;
  }, t.prototype.getViewportRoot = function() {
    return this._domRoot;
  }, t.prototype.getViewportRootOffset = function() {
    var e = this.getViewportRoot();
    if (e)
      return {
        offsetLeft: e.offsetLeft || 0,
        offsetTop: e.offsetTop || 0
      };
  }, t.prototype.refresh = function(e) {
    var r;
    e && !z(e) ? r = { paintAll: !!e } : r = e || {};
    var n = G(r.refresh, !0), i = G(r.refreshHover, !1);
    if (i && (this._hoverLayerDirty = Gl), !n)
      return i && this._paintHoverList(this.storage.getDisplayList(!1)), this;
    var a = this.storage.getDisplayList(!0);
    this._updateLayerStatus(a, r.paintAll), this._redrawId = Math.random();
    var o = this._prevDisplayList;
    this._paintList(a, o, this._redrawId);
    var s = this._backgroundColor;
    return ke(this._i, function(u, l, f, h) {
      u.refresh && u.refresh(h === 0 ? s : null);
    }, ah), this._opts.useDirtyRect && (this._prevDisplayList = a.slice()), this;
  }, t.prototype._paintHoverList = function(e) {
    var r = this._i.hoverlayer, n = this._hoverLayerDirty;
    if (this._hoverLayerDirty = Vl, n !== Vl && (!r && n === Gl && (r = this._i.hoverlayer = this._ensureLayer(Tg)), !!r)) {
      r.clear();
      for (var i = {
        inHover: !0,
        viewWidth: this._width,
        viewHeight: this._height,
        beforeBrushParam: {}
      }, a, o = 0, s = e.length; o < s; o++) {
        var u = e[o];
        if (u.__inHover) {
          a || (a = r.ctx, a.save());
          var l = u.__hoverStyle, f = void 0;
          l && (f = u.style, u.style = l), pn(a, u, i), l && (u.style = f);
        }
      }
      a && (ai(a, i), a.restore());
    }
  }, t.prototype.getHoverLayer = function() {
    return this._ensureLayer(Tg);
  }, t.prototype.paintOne = function(e, r) {
    hc(e, r);
  }, t.prototype._paintList = function(e, r, n) {
    if (this._redrawId === n) {
      var i = this._doPaintList(e, r);
      if (this._needsManuallyCompositing && this._compositeManually(), i)
        ke(this._i, function(o) {
          o.afterBrush && o.afterBrush();
        }, Lo), this._paintHoverList(e);
      else {
        var a = this;
        rs(function() {
          a._paintList(e, r, n);
        });
      }
    }
  }, t.prototype._compositeManually = function() {
    var e = this._ensureLayer(ln).ctx, r = this._domRoot.width, n = this._domRoot.height;
    e.clearRect(0, 0, r, n), ke(this._i, function(i) {
      i.virtual && e.drawImage(i.dom, 0, 0, r, n);
    }, la);
  }, t.prototype._doPaintList = function(e, r) {
    var n = this, i = !0;
    return ke(this._i, function(a) {
      var o = !1;
      if (Po(a, function(h) {
        (h.drawIdx < h.endIdx || h.notClearIdx >= 0) && (o = !0);
      }), !(!o && !a.__dirty)) {
        var s = n._opts.useDirtyRect && !Hl(a) ? a.createRepaintRects(e, r, n._width, n._height) : null, u = n._i.layerStack[0], l = !0;
        if (a.__dirty) {
          l = !1, a.__dirty = !1;
          var f = a.zlevel === u.zl && a.zlevel2 === u.zl2 ? n._backgroundColor : null;
          a.clear(!1, f, s);
        }
        Po(a, function(h) {
          var c = n._paintPerCursor(a, h, e, s, l);
          i = i && c;
        });
      }
    }, Lo), W.wxa && ke(this._i, function(a) {
      a && a.ctx && a.ctx.draw && a.ctx.draw();
    }), i;
  }, t.prototype._paintPerCursor = function(e, r, n, i, a) {
    var o = e.ctx;
    if (i)
      if (!i.length)
        r.drawIdx = r.endIdx;
      else
        for (var s = this.dpr, u = 0; u < i.length; ++u) {
          var l = i[u];
          o.save(), o.beginPath(), o.rect(l.x * s, l.y * s, l.width * s, l.height * s), o.clip(), this._paintPerCursorInRect(e, r, n, l, a), o.restore();
        }
    else
      o.save(), this._paintPerCursorInRect(e, r, n, null, a), o.restore();
    return r.drawIdx >= r.endIdx;
  }, t.prototype._paintPerCursorInRect = function(e, r, n, i, a) {
    for (var o = {
      inHover: !1,
      allClipped: !1,
      prevEl: null,
      viewWidth: this._width,
      viewHeight: this._height,
      beforeBrushParam: { contentRetained: a }
    }, s = e.ctx, u = Hl(e), l = u && Le.getTime(), f = r.drawIdx, h = r.notClearIdx, c = h >= 0 ? Math.min(h, f) : f; c < r.endIdx; c++) {
      var v = n[c];
      if (!(c < f && !v.notClear)) {
        if (v.__inHover && (this._hoverLayerDirty = Gl), i != null) {
          var d = v.getPaintRect();
          d && d.intersect(i) && (pn(s, v, o), v.setPrevPaintRect(d));
        } else
          pn(s, v, o);
        if (u) {
          var y = Le.getTime() - l;
          if (y > 15) {
            c++;
            break;
          }
        }
      }
    }
    ai(s, o), r.drawIdx = Math.max(c, f);
  }, t.prototype.getLayer = function(e, r) {
    return this._ensureLayer(e, 0, r);
  }, t.prototype._ensureLayer = function(e, r, n) {
    r = r || 0;
    var i = this._singleCanvas;
    i && !this._needsManuallyCompositing && (e = ln, r = 0);
    var a = Ul(this._i, e)[r];
    return a || (a = wg("zr_" + e + "." + r, this, e, r), this._layerConfig[e] && pe(a, this._layerConfig[e], !0), (n || i && e !== ln) && (a.virtual = !0), this._insertLayer(a, e, r, !1), a.initContext()), a;
  }, t.prototype.insertLayer = function(e, r) {
    this._insertLayer(r, e, 0, !1);
  }, t.prototype._insertLayer = function(e, r, n, i) {
    var a = this._i, o = a.layers, s = a.layerStack, u = this._domRoot, l = null;
    if (!(o[r] && o[r][n]) && _R(e)) {
      for (var f = s.length, h = 0; h < f && (s[h].zl < r || s[h].zl === r && s[h].zl2 < n); )
        h++;
      if (h > 0 && (l = Ul(a, s[h - 1].zl)[s[h - 1].zl2]), s.splice(h, 0, { zl: r, zl2: n }), Ul(a, r)[n] = e, !i && !e.virtual)
        if (l) {
          var c = l.dom;
          c.nextSibling ? u.insertBefore(e.dom, c.nextSibling) : u.appendChild(e.dom);
        } else
          u.firstChild ? u.insertBefore(e.dom, u.firstChild) : u.appendChild(e.dom);
      e.painter || (e.painter = this);
    }
  }, t.prototype.eachLayer = function(e, r) {
    return ke(this._i, function(n, i) {
      e.call(r, n, i);
    });
  }, t.prototype.eachBuiltinLayer = function(e, r) {
    return ke(this._i, function(n, i) {
      e.call(r, n, i);
    }, la);
  }, t.prototype.eachOtherLayer = function(e, r) {
    return ke(this._i, function(n, i) {
      e.call(r, n, i);
    }, ah);
  }, t.prototype.getLayers = function() {
    var e = {};
    return ke(this._i, function(r, n, i) {
      e[r.id] = r;
    }), e;
  }, t.prototype._updateLayerStatus = function(e, r) {
    var n = this;
    if (n._singleCanvas)
      for (var i = 1; i < e.length; i++) {
        var a = e[i];
        if (a.zlevel !== e[i - 1].zlevel || a.incremental) {
          n._needsManuallyCompositing = !0;
          break;
        }
      }
    ke(n._i, function(p) {
      p.__dirty = !1, Po(p, function(g) {
        g.used = !1, g.endIdxNew = 0, g.notClearIdx = -1;
      });
    }, Lo);
    for (var o, s = null, u = null, l = !1, f = 0, h = e.length; f < h; f++) {
      var a = e[f], c = a.zlevel, v = a.incremental, d = void 0;
      if (o !== c && (o = c, l = !1), v ? (l = !0, d = wC) : d = l ? TC : Uo, (!s || c !== s.zlevel || d !== s.zlevel2) && (s = n._ensureLayer(c, d), u = null, !s.__builtin__)) {
        Fs("ZLevel " + c + " has been used by unknown layer " + s.id);
        continue;
      }
      if ((!u || v !== u.key) && (u = TR(s, v), !u.used))
        if (u.used = !0, !r && u.first === a.id) {
          var y = f - u.startIdx;
          u.startIdx = f, u.drawIdx += y, u.endIdx += y;
        } else
          s.__dirty = !0, u.first = a.id, u.startIdx = u.drawIdx = f, u.endIdx = f + 1;
      u.endIdxNew = f + 1, a.__dirty & Je && !a.__inHover && ((!v || !a.notClear && f < u.drawIdx) && (s.__dirty = !0), v && a.notClear && u.notClearIdx < 0 && (u.notClearIdx = f));
    }
    ke(n._i, function(p) {
      for (var g = p.__cursorStack, m = p.__cursors, _ = g.length - 1; _ >= 0; _--) {
        var S = m.get(g[_]);
        if (!S.used)
          p.__dirty = !0, m.removeKey(g[_]), g.splice(_, 1);
        else {
          var b = S.endIdxNew;
          (Hl(p) ? b < S.drawIdx : b !== S.endIdx || !b || e[b - 1].id !== S.last) && (p.__dirty = !0), S.endIdx = S.endIdxNew, S.last = b ? e[b - 1].id : NaN;
        }
      }
      p.__dirty && (Po(p, function(T) {
        T.drawIdx = T.startIdx;
      }), n._hoverLayerDirty === Vl && (n._hoverLayerDirty = mR));
    }, Lo);
  }, t.prototype.clear = function() {
    return ke(this._i, function(e) {
      e.clear(), j0(e);
    }, la), this;
  }, t.prototype.setBackgroundColor = function(e) {
    this._backgroundColor = e, ke(this._i, function(r) {
      r.setUnpainted();
    });
  }, t.prototype.configLayer = function(e, r) {
    if (r) {
      var n = this._layerConfig;
      n[e] ? pe(n[e], r, !0) : n[e] = r, ke(this._i, function(i, a) {
        pe(i, n[a], !0);
      });
    }
  }, t.prototype.delLayer = function(e) {
    for (var r = this._i.layerStack, n = this._i.layers, i = r.length - 1; i >= 0; i--) {
      var a = r[i];
      if (a.zl === e) {
        var o = n[e][a.zl2];
        if (o.__builtin__)
          continue;
        if (r.splice(i, 1), n[e][a.zl2] = void 0, !o.virtual) {
          var s = o.dom.parentNode;
          s && s.removeChild(o.dom);
        }
      }
    }
  }, t.prototype.resize = function(e, r) {
    if (this._domRoot.style) {
      var n = this._domRoot;
      n.style.display = "none";
      var i = this._opts, a = this.root;
      e != null && (i.width = e), r != null && (i.height = r), e = mo(a, 0, i), r = mo(a, 1, i), n.style.display = "", (this._width !== e || r !== this._height) && (n.style.width = e + "px", n.style.height = r + "px", ke(this._i, function(o) {
        o.resize(e, r);
      }), this.refresh({ paintAll: !0 })), this._width = e, this._height = r;
    } else {
      if (e == null || r == null)
        return;
      this._width = e, this._height = r, this._ensureLayer(ln).resize(e, r);
    }
    return this;
  }, t.prototype.clearLayer = function(e) {
    w(this._i.layers[e], function(r) {
      r && !r.__builtin__ && r.clear();
    });
  }, t.prototype.dispose = function() {
    this.root.innerHTML = "", this.root = this.storage = this._domRoot = this._i = null;
  }, t.prototype.getRenderedCanvas = function(e) {
    if (e = e || {}, this._singleCanvas && !this._compositeManually)
      return this._i.layers[ln][0].dom;
    var r = new K0("image", this, e.pixelRatio || this.dpr);
    r.initContext(), r.clear(!1, e.backgroundColor || this._backgroundColor);
    var n = r.ctx;
    if (e.pixelRatio <= this.dpr) {
      this.refresh();
      var i = r.dom.width, a = r.dom.height;
      ke(this._i, function(h) {
        h.__builtin__ ? n.drawImage(h.dom, 0, 0, i, a) : h.renderToCanvas && (n.save(), h.renderToCanvas(n), n.restore());
      });
    } else {
      for (var o = {
        inHover: !1,
        viewWidth: this._width,
        viewHeight: this._height,
        beforeBrushParam: {}
      }, s = this.storage.getDisplayList(!0), u = 0, l = s.length; u < l; u++) {
        var f = s[u];
        pn(n, f, o);
      }
      ai(n, o);
    }
    return r.dom;
  }, t.prototype.getWidth = function() {
    return this._width;
  }, t.prototype.getHeight = function() {
    return this._height;
  }, t;
})();
function c2(t) {
  t.registerPainter("canvas", wR);
}
export {
  DC as $,
  AP as A,
  Fe as B,
  ae as C,
  QR as D,
  Dt as E,
  w as F,
  qe as G,
  et as H,
  ge as I,
  jI as J,
  $t as K,
  GR as L,
  Zb as M,
  DT as N,
  ex as O,
  ye as P,
  hy as Q,
  Ee as R,
  eu as S,
  ve as T,
  js as U,
  H as V,
  fi as W,
  xm as X,
  kh as Y,
  yi as Z,
  F as _,
  Ie as a,
  Ls as a$,
  LR as a0,
  Gh as a1,
  ut as a2,
  he as a3,
  Wt as a4,
  E as a5,
  RR as a6,
  VC as a7,
  ER as a8,
  AR as a9,
  AA as aA,
  FR as aB,
  MR as aC,
  jb as aD,
  Xs as aE,
  Dn as aF,
  hr as aG,
  jR as aH,
  Ax as aI,
  ys as aJ,
  z as aK,
  oe as aL,
  SA as aM,
  bA as aN,
  PA as aO,
  KR as aP,
  kR as aQ,
  Q_ as aR,
  me as aS,
  JR as aT,
  VR as aU,
  a2 as aV,
  YP as aW,
  KS as aX,
  Af as aY,
  Dc as aZ,
  di as a_,
  PR as aa,
  DR as ab,
  fc as ac,
  r0 as ad,
  t0 as ae,
  ur as af,
  Lt as ag,
  LA as ah,
  _e as ai,
  pe as aj,
  ms as ak,
  Ap as al,
  O0 as am,
  _A as an,
  f0 as ao,
  gi as ap,
  Y as aq,
  ot as ar,
  s2 as as,
  sx as at,
  YA as au,
  yu as av,
  sA as aw,
  gy as ax,
  Sh as ay,
  Va as az,
  N as b,
  xR as b$,
  hm as b0,
  cm as b1,
  Gb as b2,
  G as b3,
  o2 as b4,
  zR as b5,
  kp as b6,
  HR as b7,
  kr as b8,
  ZR as b9,
  dL as bA,
  bL as bB,
  u2 as bC,
  TL as bD,
  _L as bE,
  Vs as bF,
  ts as bG,
  zh as bH,
  Rr as bI,
  Tt as bJ,
  fy as bK,
  Z as bL,
  Sm as bM,
  ce as bN,
  je as bO,
  Cn as bP,
  su as bQ,
  tu as bR,
  Na as bS,
  zb as bT,
  Ze as bU,
  BR as bV,
  NR as bW,
  QS as bX,
  _M as bY,
  Rt as bZ,
  $e as b_,
  i2 as ba,
  qR as bb,
  oA as bc,
  DA as bd,
  ha as be,
  XP as bf,
  qP as bg,
  de as bh,
  EC as bi,
  vL as bj,
  Pn as bk,
  Fh as bl,
  lt as bm,
  OI as bn,
  we as bo,
  kx as bp,
  HC as bq,
  IR as br,
  YR as bs,
  Qb as bt,
  ze as bu,
  Ye as bv,
  vi as bw,
  HL as bx,
  pL as by,
  gL as bz,
  X as c,
  II as c$,
  r2 as c0,
  h2 as c1,
  f2 as c2,
  l2 as c3,
  c2 as c4,
  Jv as c5,
  nu as c6,
  Or as c7,
  ue as c8,
  Hg as c9,
  RP as cA,
  EP as cB,
  cP as cC,
  MI as cD,
  pc as cE,
  xI as cF,
  AI as cG,
  hP as cH,
  rP as cI,
  hc as cJ,
  A1 as cK,
  lP as cL,
  Xp as cM,
  Ln as cN,
  X_ as cO,
  DI as cP,
  q_ as cQ,
  Sc as cR,
  Wh as cS,
  $_ as cT,
  W_ as cU,
  yc as cV,
  mc as cW,
  gc as cX,
  Z_ as cY,
  pu as cZ,
  Fr as c_,
  h1 as ca,
  ci as cb,
  X1 as cc,
  U1 as cd,
  to as ce,
  Ra as cf,
  Kp as cg,
  xe as ch,
  e2 as ci,
  t2 as cj,
  zP as ck,
  na as cl,
  OR as cm,
  Y_ as cn,
  bI as co,
  k_ as cp,
  ib as cq,
  TI as cr,
  PI as cs,
  iI as ct,
  wI as cu,
  U_ as cv,
  CI as cw,
  W as cx,
  OP as cy,
  LP as cz,
  XR as d,
  Dg as d0,
  fP as d1,
  vP as d2,
  c1 as d3,
  nI as d4,
  r1 as d5,
  Nb as d6,
  yh as d7,
  ui as d8,
  st as d9,
  rs as da,
  QA as db,
  rc as dc,
  Ue as dd,
  ec as de,
  Vm as df,
  mx as dg,
  GI as dh,
  vf as di,
  rf as dj,
  Bn as dk,
  vu as dl,
  py as dm,
  ch as dn,
  JA as dp,
  eP as dq,
  U as e,
  mh as f,
  vr as g,
  Ac as h,
  B as i,
  TM as j,
  EA as k,
  Bp as l,
  yr as m,
  _u as n,
  $R as o,
  bM as p,
  ic as q,
  _s as r,
  Ta as s,
  j as t,
  WR as u,
  n2 as v,
  i0 as w,
  JM as x,
  wT as y,
  UR as z
};
