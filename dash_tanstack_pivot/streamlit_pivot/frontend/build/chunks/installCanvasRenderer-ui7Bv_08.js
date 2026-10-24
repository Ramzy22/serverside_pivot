import { b as IS, r as AS, g as PS } from "./charts-DthW1Q8W.js";
var Ga = {}, ql = function(t, e) {
  return ql = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, ql(t, e);
};
function Dg(t, e) {
  if (typeof e != "function" && e !== null)
    throw new TypeError("Class extends value " + String(e) + " is not a constructor or null");
  ql(t, e);
  function r() {
    this.constructor = t;
  }
  t.prototype = e === null ? Object.create(e) : (r.prototype = e.prototype, new r());
}
var Qo = function() {
  return Qo = Object.assign || function(e) {
    for (var r, n = 1, i = arguments.length; n < i; n++) {
      r = arguments[n];
      for (var a in r) Object.prototype.hasOwnProperty.call(r, a) && (e[a] = r[a]);
    }
    return e;
  }, Qo.apply(this, arguments);
};
function Ig(t, e) {
  var r = {};
  for (var n in t) Object.prototype.hasOwnProperty.call(t, n) && e.indexOf(n) < 0 && (r[n] = t[n]);
  if (t != null && typeof Object.getOwnPropertySymbols == "function")
    for (var i = 0, n = Object.getOwnPropertySymbols(t); i < n.length; i++)
      e.indexOf(n[i]) < 0 && Object.prototype.propertyIsEnumerable.call(t, n[i]) && (r[n[i]] = t[n[i]]);
  return r;
}
function Ag(t, e, r, n) {
  var i = arguments.length, a = i < 3 ? e : n === null ? n = Object.getOwnPropertyDescriptor(e, r) : n, o;
  if (typeof Reflect == "object" && typeof Reflect.decorate == "function") a = Reflect.decorate(t, e, r, n);
  else for (var s = t.length - 1; s >= 0; s--) (o = t[s]) && (a = (i < 3 ? o(a) : i > 3 ? o(e, r, a) : o(e, r)) || a);
  return i > 3 && a && Object.defineProperty(e, r, a), a;
}
function Pg(t, e) {
  return function(r, n) {
    e(r, n, t);
  };
}
function Lg(t, e, r, n, i, a) {
  function o(g) {
    if (g !== void 0 && typeof g != "function") throw new TypeError("Function expected");
    return g;
  }
  for (var s = n.kind, u = s === "getter" ? "get" : s === "setter" ? "set" : "value", l = !e && t ? n.static ? t : t.prototype : null, f = e || (l ? Object.getOwnPropertyDescriptor(l, n.name) : {}), h, v = !1, c = r.length - 1; c >= 0; c--) {
    var d = {};
    for (var y in n) d[y] = y === "access" ? {} : n[y];
    for (var y in n.access) d.access[y] = n.access[y];
    d.addInitializer = function(g) {
      if (v) throw new TypeError("Cannot add initializers after decoration has completed");
      a.push(o(g || null));
    };
    var p = (0, r[c])(s === "accessor" ? { get: f.get, set: f.set } : f[u], d);
    if (s === "accessor") {
      if (p === void 0) continue;
      if (p === null || typeof p != "object") throw new TypeError("Object expected");
      (h = o(p.get)) && (f.get = h), (h = o(p.set)) && (f.set = h), (h = o(p.init)) && i.unshift(h);
    } else (h = o(p)) && (s === "field" ? i.unshift(h) : f[u] = h);
  }
  l && Object.defineProperty(l, n.name, f), v = !0;
}
function Eg(t, e, r) {
  for (var n = arguments.length > 2, i = 0; i < e.length; i++)
    r = n ? e[i].call(t, r) : e[i].call(t);
  return n ? r : void 0;
}
function Rg(t) {
  return typeof t == "symbol" ? t : "".concat(t);
}
function Og(t, e, r) {
  return typeof e == "symbol" && (e = e.description ? "[".concat(e.description, "]") : ""), Object.defineProperty(t, "name", { configurable: !0, value: r ? "".concat(r, " ", e) : e });
}
function kg(t, e) {
  if (typeof Reflect == "object" && typeof Reflect.metadata == "function") return Reflect.metadata(t, e);
}
function Bg(t, e, r, n) {
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
function Ng(t, e) {
  var r = { label: 0, sent: function() {
    if (a[0] & 1) throw a[1];
    return a[1];
  }, trys: [], ops: [] }, n, i, a, o = Object.create((typeof Iterator == "function" ? Iterator : Object).prototype);
  return o.next = s(0), o.throw = s(1), o.return = s(2), typeof Symbol == "function" && (o[Symbol.iterator] = function() {
    return this;
  }), o;
  function s(l) {
    return function(f) {
      return u([l, f]);
    };
  }
  function u(l) {
    if (n) throw new TypeError("Generator is already executing.");
    for (; o && (o = 0, l[0] && (r = 0)), r; ) try {
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
var zs = Object.create ? (function(t, e, r, n) {
  n === void 0 && (n = r);
  var i = Object.getOwnPropertyDescriptor(e, r);
  (!i || ("get" in i ? !e.__esModule : i.writable || i.configurable)) && (i = { enumerable: !0, get: function() {
    return e[r];
  } }), Object.defineProperty(t, n, i);
}) : (function(t, e, r, n) {
  n === void 0 && (n = r), t[n] = e[r];
});
function Fg(t, e) {
  for (var r in t) r !== "default" && !Object.prototype.hasOwnProperty.call(e, r) && zs(e, t, r);
}
function Jo(t) {
  var e = typeof Symbol == "function" && Symbol.iterator, r = e && t[e], n = 0;
  if (r) return r.call(t);
  if (t && typeof t.length == "number") return {
    next: function() {
      return t && n >= t.length && (t = void 0), { value: t && t[n++], done: !t };
    }
  };
  throw new TypeError(e ? "Object is not iterable." : "Symbol.iterator is not defined.");
}
function lh(t, e) {
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
function zg() {
  for (var t = [], e = 0; e < arguments.length; e++)
    t = t.concat(lh(arguments[e]));
  return t;
}
function Hg() {
  for (var t = 0, e = 0, r = arguments.length; e < r; e++) t += arguments[e].length;
  for (var n = Array(t), i = 0, e = 0; e < r; e++)
    for (var a = arguments[e], o = 0, s = a.length; o < s; o++, i++)
      n[i] = a[o];
  return n;
}
function Vg(t, e, r) {
  if (r || arguments.length === 2) for (var n = 0, i = e.length, a; n < i; n++)
    (a || !(n in e)) && (a || (a = Array.prototype.slice.call(e, 0, n)), a[n] = e[n]);
  return t.concat(a || Array.prototype.slice.call(e));
}
function si(t) {
  return this instanceof si ? (this.v = t, this) : new si(t);
}
function Gg(t, e, r) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var n = r.apply(t, e || []), i, a = [];
  return i = Object.create((typeof AsyncIterator == "function" ? AsyncIterator : Object).prototype), s("next"), s("throw"), s("return", o), i[Symbol.asyncIterator] = function() {
    return this;
  }, i;
  function o(c) {
    return function(d) {
      return Promise.resolve(d).then(c, h);
    };
  }
  function s(c, d) {
    n[c] && (i[c] = function(y) {
      return new Promise(function(p, g) {
        a.push([c, y, p, g]) > 1 || u(c, y);
      });
    }, d && (i[c] = d(i[c])));
  }
  function u(c, d) {
    try {
      l(n[c](d));
    } catch (y) {
      v(a[0][3], y);
    }
  }
  function l(c) {
    c.value instanceof si ? Promise.resolve(c.value.v).then(f, h) : v(a[0][2], c);
  }
  function f(c) {
    u("next", c);
  }
  function h(c) {
    u("throw", c);
  }
  function v(c, d) {
    c(d), a.shift(), a.length && u(a[0][0], a[0][1]);
  }
}
function Ug(t) {
  var e, r;
  return e = {}, n("next"), n("throw", function(i) {
    throw i;
  }), n("return"), e[Symbol.iterator] = function() {
    return this;
  }, e;
  function n(i, a) {
    e[i] = t[i] ? function(o) {
      return (r = !r) ? { value: si(t[i](o)), done: !1 } : a ? a(o) : o;
    } : a;
  }
}
function Yg(t) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var e = t[Symbol.asyncIterator], r;
  return e ? e.call(t) : (t = typeof Jo == "function" ? Jo(t) : t[Symbol.iterator](), r = {}, n("next"), n("throw"), n("return"), r[Symbol.asyncIterator] = function() {
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
function Wg(t, e) {
  return Object.defineProperty ? Object.defineProperty(t, "raw", { value: e }) : t.raw = e, t;
}
var LS = Object.create ? (function(t, e) {
  Object.defineProperty(t, "default", { enumerable: !0, value: e });
}) : function(t, e) {
  t.default = e;
}, $l = function(t) {
  return $l = Object.getOwnPropertyNames || function(e) {
    var r = [];
    for (var n in e) Object.prototype.hasOwnProperty.call(e, n) && (r[r.length] = n);
    return r;
  }, $l(t);
};
function Xg(t) {
  if (t && t.__esModule) return t;
  var e = {};
  if (t != null) for (var r = $l(t), n = 0; n < r.length; n++) r[n] !== "default" && zs(e, t, r[n]);
  return LS(e, t), e;
}
function qg(t) {
  return t && t.__esModule ? t : { default: t };
}
function $g(t, e, r, n) {
  if (r === "a" && !n) throw new TypeError("Private accessor was defined without a getter");
  if (typeof e == "function" ? t !== e || !n : !e.has(t)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
  return r === "m" ? n : r === "a" ? n.call(t) : n ? n.value : e.get(t);
}
function Zg(t, e, r, n, i) {
  if (n === "m") throw new TypeError("Private method is not writable");
  if (n === "a" && !i) throw new TypeError("Private accessor was defined without a setter");
  if (typeof e == "function" ? t !== e || !i : !e.has(t)) throw new TypeError("Cannot write private member to an object whose class did not declare it");
  return n === "a" ? i.call(t, r) : i ? i.value = r : e.set(t, r), r;
}
function Kg(t, e) {
  if (e === null || typeof e != "object" && typeof e != "function") throw new TypeError("Cannot use 'in' operator on non-object");
  return typeof t == "function" ? e === t : t.has(e);
}
function jg(t, e, r) {
  if (e != null) {
    if (typeof e != "object" && typeof e != "function") throw new TypeError("Object expected.");
    var n, i;
    if (r) {
      if (!Symbol.asyncDispose) throw new TypeError("Symbol.asyncDispose is not defined.");
      n = e[Symbol.asyncDispose];
    }
    if (n === void 0) {
      if (!Symbol.dispose) throw new TypeError("Symbol.dispose is not defined.");
      n = e[Symbol.dispose], r && (i = n);
    }
    if (typeof n != "function") throw new TypeError("Object not disposable.");
    i && (n = function() {
      try {
        i.call(this);
      } catch (a) {
        return Promise.reject(a);
      }
    }), t.stack.push({ value: e, dispose: n, async: r });
  } else r && t.stack.push({ async: !0 });
  return e;
}
var ES = typeof SuppressedError == "function" ? SuppressedError : function(t, e, r) {
  var n = new Error(r);
  return n.name = "SuppressedError", n.error = t, n.suppressed = e, n;
};
function Qg(t) {
  function e(a) {
    t.error = t.hasError ? new ES(a, t.error, "An error was suppressed during disposal.") : a, t.hasError = !0;
  }
  var r, n = 0;
  function i() {
    for (; r = t.stack.pop(); )
      try {
        if (!r.async && n === 1) return n = 0, t.stack.push(r), Promise.resolve().then(i);
        if (r.dispose) {
          var a = r.dispose.call(r.value);
          if (r.async) return n |= 2, Promise.resolve(a).then(i, function(o) {
            return e(o), i();
          });
        } else n |= 1;
      } catch (o) {
        e(o);
      }
    if (n === 1) return t.hasError ? Promise.reject(t.error) : Promise.resolve();
    if (t.hasError) throw t.error;
  }
  return i();
}
function Jg(t, e) {
  return typeof t == "string" && /^\.\.?\//.test(t) ? t.replace(/\.(tsx)$|((?:\.d)?)((?:\.[^./]+?)?)\.([cm]?)ts$/i, function(r, n, i, a, o) {
    return n ? e ? ".jsx" : ".js" : i && (!a || !o) ? r : i + a + "." + o.toLowerCase() + "js";
  }) : t;
}
const RS = {
  __extends: Dg,
  __assign: Qo,
  __rest: Ig,
  __decorate: Ag,
  __param: Pg,
  __esDecorate: Lg,
  __runInitializers: Eg,
  __propKey: Rg,
  __setFunctionName: Og,
  __metadata: kg,
  __awaiter: Bg,
  __generator: Ng,
  __createBinding: zs,
  __exportStar: Fg,
  __values: Jo,
  __read: lh,
  __spread: zg,
  __spreadArrays: Hg,
  __spreadArray: Vg,
  __await: si,
  __asyncGenerator: Gg,
  __asyncDelegator: Ug,
  __asyncValues: Yg,
  __makeTemplateObject: Wg,
  __importStar: Xg,
  __importDefault: qg,
  __classPrivateFieldGet: $g,
  __classPrivateFieldSet: Zg,
  __classPrivateFieldIn: Kg,
  __addDisposableResource: jg,
  __disposeResources: Qg,
  __rewriteRelativeImportExtension: Jg
}, OS = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  __addDisposableResource: jg,
  get __assign() {
    return Qo;
  },
  __asyncDelegator: Ug,
  __asyncGenerator: Gg,
  __asyncValues: Yg,
  __await: si,
  __awaiter: Bg,
  __classPrivateFieldGet: $g,
  __classPrivateFieldIn: Kg,
  __classPrivateFieldSet: Zg,
  __createBinding: zs,
  __decorate: Ag,
  __disposeResources: Qg,
  __esDecorate: Lg,
  __exportStar: Fg,
  __extends: Dg,
  __generator: Ng,
  __importDefault: qg,
  __importStar: Xg,
  __makeTemplateObject: Wg,
  __metadata: kg,
  __param: Pg,
  __propKey: Rg,
  __read: lh,
  __rest: Ig,
  __rewriteRelativeImportExtension: Jg,
  __runInitializers: Eg,
  __setFunctionName: Og,
  __spread: zg,
  __spreadArray: Vg,
  __spreadArrays: Hg,
  __values: Jo,
  default: RS
}, Symbol.toStringTag, { value: "Module" })), ey = /* @__PURE__ */ IS(OS);
var Jt = {}, er = {}, Tu = {}, Hc;
function kS() {
  return Hc || (Hc = 1, (function(t) {
    Object.defineProperty(t, "__esModule", {
      value: !0
    }), t.default = void 0;
    var e = 1;
    t.default = function() {
      return "".concat(e++);
    };
  })(Tu)), Tu;
}
var bi = {}, wi = {}, Cu = {}, Vc;
function ty() {
  return Vc || (Vc = 1, (function(t) {
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
  })(Cu)), Cu;
}
var tr = {}, Gc;
function fh() {
  return Gc || (Gc = 1, Object.defineProperty(tr, "__esModule", {
    value: !0
  }), tr.SizeSensorId = tr.SensorTabIndex = tr.SensorClassName = void 0, tr.SizeSensorId = "size-sensor-id", tr.SensorClassName = "size-sensor-object", tr.SensorTabIndex = "-1"), tr;
}
var Uc;
function BS() {
  if (Uc) return wi;
  Uc = 1, Object.defineProperty(wi, "__esModule", {
    value: !0
  }), wi.createSensor = void 0;
  var t = r(ty()), e = fh();
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return wi.createSensor = function(i, a) {
    var o = void 0, s = [], u = function() {
      getComputedStyle(i).position === "static" && (i.style.position = "relative");
      var d = document.createElement("object");
      return d.onload = function() {
        d.contentDocument.defaultView.addEventListener("resize", l), l();
      }, d.style.display = "block", d.style.position = "absolute", d.style.top = "0", d.style.left = "0", d.style.height = "100%", d.style.width = "100%", d.style.overflow = "hidden", d.style.pointerEvents = "none", d.style.zIndex = "-1", d.style.opacity = "0", d.setAttribute("class", e.SensorClassName), d.setAttribute("tabindex", e.SensorTabIndex), d.type = "text/html", i.appendChild(d), d.data = "about:blank", d;
    }, l = (0, t.default)(function() {
      s.forEach(function(c) {
        c(i);
      });
    }), f = function(d) {
      o || (o = u()), s.indexOf(d) === -1 && s.push(d);
    }, h = function() {
      o && o.parentNode && (o.contentDocument && o.contentDocument.defaultView.removeEventListener("resize", l), o.parentNode.removeChild(o), i.removeAttribute(e.SizeSensorId), o = void 0, s = [], a && a());
    }, v = function(d) {
      var y = s.indexOf(d);
      y !== -1 && s.splice(y, 1), s.length === 0 && o && h();
    };
    return {
      element: i,
      bind: f,
      destroy: h,
      unbind: v
    };
  }, wi;
}
var Ti = {}, Yc;
function NS() {
  if (Yc) return Ti;
  Yc = 1, Object.defineProperty(Ti, "__esModule", {
    value: !0
  }), Ti.createSensor = void 0;
  var t = fh(), e = r(ty());
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return Ti.createSensor = function(i, a) {
    var o = void 0, s = [], u = (0, e.default)(function() {
      s.forEach(function(c) {
        c(i);
      });
    }), l = function() {
      var d = new ResizeObserver(u);
      return d.observe(i), u(), d;
    }, f = function(d) {
      o || (o = l()), s.indexOf(d) === -1 && s.push(d);
    }, h = function() {
      o && o.disconnect(), s = [], o = void 0, i.removeAttribute(t.SizeSensorId), a && a();
    }, v = function(d) {
      var y = s.indexOf(d);
      y !== -1 && s.splice(y, 1), s.length === 0 && o && h();
    };
    return {
      element: i,
      bind: f,
      destroy: h,
      unbind: v
    };
  }, Ti;
}
var Wc;
function FS() {
  if (Wc) return bi;
  Wc = 1, Object.defineProperty(bi, "__esModule", {
    value: !0
  }), bi.createSensor = void 0;
  var t = BS(), e = NS();
  return bi.createSensor = typeof ResizeObserver < "u" ? e.createSensor : t.createSensor, bi;
}
var Xc;
function zS() {
  if (Xc) return er;
  Xc = 1, Object.defineProperty(er, "__esModule", {
    value: !0
  }), er.removeSensor = er.getSensor = er.Sensors = void 0;
  var t = n(kS()), e = FS(), r = fh();
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
var qc;
function HS() {
  if (qc) return Jt;
  qc = 1, Object.defineProperty(Jt, "__esModule", {
    value: !0
  }), Jt.ver = Jt.clear = Jt.bind = void 0;
  var t = zS();
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
var Ci = {}, $c;
function VS() {
  if ($c) return Ci;
  $c = 1, Object.defineProperty(Ci, "__esModule", { value: !0 }), Ci.pick = void 0;
  function t(e, r) {
    var n = {};
    return r.forEach(function(i) {
      n[i] = e[i];
    }), n;
  }
  return Ci.pick = t, Ci;
}
var xi = {}, Zc;
function GS() {
  if (Zc) return xi;
  Zc = 1, Object.defineProperty(xi, "__esModule", { value: !0 }), xi.isFunction = void 0;
  function t(e) {
    return typeof e == "function";
  }
  return xi.isFunction = t, xi;
}
var Mi = {}, Kc;
function US() {
  if (Kc) return Mi;
  Kc = 1, Object.defineProperty(Mi, "__esModule", { value: !0 }), Mi.isString = void 0;
  function t(e) {
    return typeof e == "string";
  }
  return Mi.isString = t, Mi;
}
var Di = {}, xu, jc;
function YS() {
  return jc || (jc = 1, xu = function t(e, r) {
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
  }), xu;
}
var Qc;
function WS() {
  if (Qc) return Di;
  Qc = 1, Object.defineProperty(Di, "__esModule", { value: !0 }), Di.isEqual = void 0;
  var t = ey, e = t.__importDefault(YS());
  return Di.isEqual = e.default, Di;
}
var Jc;
function XS() {
  if (Jc) return Ga;
  Jc = 1, Object.defineProperty(Ga, "__esModule", { value: !0 });
  var t = ey, e = t.__importStar(AS()), r = HS(), n = VS(), i = GS(), a = US(), o = WS(), s = (
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
          var v = this.getEchartsInstance();
          (0, o.isEqual)(f.onEvents, this.props.onEvents) || (this.unbindEvents(v), this.bindEvents(v, this.props.onEvents));
          var c = ["option", "notMerge", "replaceMerge", "lazyUpdate", "showLoading", "loadingOption"];
          (0, o.isEqual)((0, n.pick)(this.props, c), (0, n.pick)(f, c)) || this.updateEChartsOption(), (!(0, o.isEqual)(f.style, this.props.style) || !(0, o.isEqual)(f.className, this.props.className)) && this.resize();
        }
      }, l.prototype.componentWillUnmount = function() {
        this.dispose();
      }, l.prototype.initEchartsInstance = function() {
        return t.__awaiter(this, void 0, void 0, function() {
          var f = this;
          return t.__generator(this, function(h) {
            return [2, new Promise(function(v) {
              f.echarts.init(f.ele, f.props.theme, f.props.opts);
              var c = f.getEchartsInstance();
              c.on("finished", function() {
                var d = f.ele.clientWidth, y = f.ele.clientHeight;
                f.echarts.dispose(f.ele);
                var p = t.__assign({ width: d, height: y }, f.props.opts);
                v(f.echarts.init(f.ele, f.props.theme, p));
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
          var f, h, v, c, d, y, p = this;
          return t.__generator(this, function(g) {
            switch (g.label) {
              case 0:
                return f = this.props, h = f.onEvents, v = f.onChartReady, c = f.autoResize, d = c === void 0 ? !0 : c, [4, this.initEchartsInstance()];
              case 1:
                return g.sent(), y = this.updateEChartsOption(), this.bindEvents(y, h || {}), (0, i.isFunction)(v) && v(y), this.ele && d && (0, r.bind)(this.ele, function() {
                  p.resize();
                }), [
                  2
                  /*return*/
                ];
            }
          });
        });
      }, l.prototype.bindEvents = function(f, h) {
        var v = this, c = function(y, p) {
          if ((0, a.isString)(y) && (0, i.isFunction)(p)) {
            var g = function(m) {
              p(m, f);
            };
            f.on(y, g), v.eventHandlerRefs[y] = g;
          }
        };
        for (var d in h)
          Object.prototype.hasOwnProperty.call(h, d) && c(d, h[d]);
      }, l.prototype.unbindEvents = function(f) {
        for (var h = 0, v = Object.entries(this.eventHandlerRefs); h < v.length; h++) {
          var c = v[h], d = c[0], y = c[1];
          f.off(d, y);
        }
        this.eventHandlerRefs = {};
      }, l.prototype.updateEChartsOption = function() {
        var f = this.props, h = f.option, v = f.notMerge, c = v === void 0 ? !1 : v, d = f.replaceMerge, y = d === void 0 ? null : d, p = f.lazyUpdate, g = p === void 0 ? !1 : p, m = f.showLoading, _ = f.loadingOption, S = _ === void 0 ? null : _, b = this.getEchartsInstance();
        return b.setOption(h, { notMerge: c, replaceMerge: y, lazyUpdate: g }), m ? b.showLoading(S) : b.hideLoading(), b;
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
        var f = this, h = this.props, v = h.style, c = h.className, d = c === void 0 ? "" : c;
        h.echarts, h.option, h.theme, h.notMerge, h.replaceMerge, h.lazyUpdate, h.showLoading, h.loadingOption, h.opts, h.onChartReady, h.onEvents, h.shouldSetOption, h.autoResize;
        var y = t.__rest(h, ["style", "className", "echarts", "option", "theme", "notMerge", "replaceMerge", "lazyUpdate", "showLoading", "loadingOption", "opts", "onChartReady", "onEvents", "shouldSetOption", "autoResize"]), p = t.__assign({ height: 300 }, v);
        return e.default.createElement("div", t.__assign({ ref: function(g) {
          f.ele = g;
        }, style: p, className: "echarts-for-react ".concat(d) }, y));
      }, l;
    })(e.PureComponent)
  );
  return Ga.default = s, Ga;
}
var qS = XS();
const FE = /* @__PURE__ */ PS(qS);
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
var Zl = function(t, e) {
  return Zl = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, Zl(t, e);
};
function z(t, e) {
  if (typeof e != "function" && e !== null)
    throw new TypeError("Class extends value " + String(e) + " is not a constructor or null");
  Zl(t, e);
  function r() {
    this.constructor = t;
  }
  t.prototype = e === null ? Object.create(e) : (r.prototype = e.prototype, new r());
}
var $S = /* @__PURE__ */ (function() {
  function t() {
    this.firefox = !1, this.ie = !1, this.edge = !1, this.newEdge = !1, this.weChat = !1;
  }
  return t;
})(), ZS = /* @__PURE__ */ (function() {
  function t() {
    this.browser = new $S(), this.node = !1, this.wxa = !1, this.worker = !1, this.svgSupported = !1, this.touchEventsSupported = !1, this.pointerEventsSupported = !1, this.domSupported = !1, this.transformSupported = !1, this.transform3dSupported = !1, this.hasGlobalWindow = typeof window < "u";
  }
  return t;
})(), W = new ZS();
typeof wx == "object" && typeof wx.getSystemInfoSync == "function" ? (W.wxa = !0, W.touchEventsSupported = !0) : typeof document > "u" && typeof self < "u" ? W.worker = !0 : !W.hasGlobalWindow || "Deno" in window || typeof navigator < "u" && typeof navigator.userAgent == "string" && navigator.userAgent.indexOf("Node.js") > -1 ? (W.node = !0, W.svgSupported = !0) : KS(navigator.userAgent, W);
function KS(t, e) {
  var r = e.browser, n = t.match(/Firefox\/([\d.]+)/), i = t.match(/MSIE\s([\d.]+)/) || t.match(/Trident\/.+?rv:(([\d.]+))/), a = t.match(/Edge?\/([\d.]+)/), o = /micromessenger/i.test(t);
  n && (r.firefox = !0, r.version = n[1]), i && (r.ie = !0, r.version = i[1]), a && (r.edge = !0, r.version = a[1], r.newEdge = +a[1].split(".")[0] > 18), o && (r.weChat = !0), e.svgSupported = typeof SVGRect < "u", e.touchEventsSupported = "ontouchstart" in window && !r.ie && !r.edge, e.pointerEventsSupported = "onpointerdown" in window && (r.edge || r.ie && +r.version >= 11);
  var s = e.domSupported = typeof document < "u";
  if (s) {
    var u = document.documentElement.style;
    e.transform3dSupported = (r.ie && "transition" in u || r.edge || "WebKitCSSMatrix" in window && "m11" in new WebKitCSSMatrix() || "MozPerspective" in u) && !("OTransition" in u), e.transformSupported = e.transform3dSupported || r.ie && +r.version >= 9;
  }
}
var hh = 12, jS = "sans-serif", Rr = hh + "px " + jS, QS = 20, JS = 100, e1 = "007LLmW'55;N0500LLLLLLLLLL00NNNLzWW\\\\WQb\\0FWLg\\bWb\\WQ\\WrWWQ000CL5LLFLL0LL**F*gLLLL5F0LF\\FFF5.5N";
function t1(t) {
  var e = {};
  if (typeof JSON > "u")
    return e;
  for (var r = 0; r < t.length; r++) {
    var n = String.fromCharCode(r + 32), i = (t.charCodeAt(r) - QS) / JS;
    e[n] = i;
  }
  return e;
}
var r1 = t1(e1), Le = {
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
        return e !== n && (e = t.font = n || Rr), t.measureText(r);
      r = r || "", n = n || Rr;
      var a = /((?:\d+)?\.?\d*)px/.exec(n), o = a && +a[1] || hh, s = 0;
      if (n.indexOf("mono") >= 0)
        s = o * r.length;
      else
        for (var u = 0; u < r.length; u++) {
          var l = r1[r[u]];
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
function ry(t) {
  for (var e in Le)
    Le.hasOwnProperty(e) && t[e] && (Le[e] = t[e]);
}
var ny = Xt([
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
}, {}), iy = Xt([
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
}, {}), gi = Object.prototype.toString, Hs = Array.prototype, n1 = Hs.forEach, i1 = Hs.filter, ch = Hs.slice, a1 = Hs.map, ev = (function() {
}).constructor, Ua = ev ? ev.prototype : null, vh = "__proto__", Mu = 2311, o1 = Math.pow(2, 53) - 1;
function dh() {
  return Mu >= o1 && (Mu = 0), Mu++;
}
function Vs() {
  for (var t = [], e = 0; e < arguments.length; e++)
    t[e] = arguments[e];
  typeof console < "u" && console.error.apply(console, t);
}
function Z(t) {
  if (t == null || typeof t != "object")
    return t;
  var e = t, r = gi.call(t);
  if (r === "[object Array]") {
    if (!ei(t)) {
      e = [];
      for (var n = 0, i = t.length; n < i; n++)
        e[n] = Z(t[n]);
    }
  } else if (iy[r]) {
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
  } else if (!ny[r] && !ei(t) && !ui(t)) {
    e = {};
    for (var o in t)
      t.hasOwnProperty(o) && o !== vh && (e[o] = Z(t[o]));
  }
  return e;
}
function pe(t, e, r) {
  if (!F(e) || !F(t))
    return r ? Z(e) : t;
  for (var n in e)
    if (e.hasOwnProperty(n) && n !== vh) {
      var i = t[n], a = e[n];
      F(a) && F(i) && !B(a) && !B(i) && !ui(a) && !ui(i) && !Kl(a) && !Kl(i) && !ei(a) && !ei(i) ? pe(i, a, r) : (r || !(n in t)) && (t[n] = Z(e[n]));
    }
  return t;
}
function s1(t, e) {
  for (var r = t[0], n = 1, i = t.length; n < i; n++)
    r = pe(r, t[n], e);
  return r;
}
function R(t, e) {
  if (Object.assign)
    Object.assign(t, e);
  else
    for (var r in e)
      e.hasOwnProperty(r) && r !== vh && (t[r] = e[r]);
  return t;
}
function ay(t, e, r) {
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
var u1 = Le.createCanvas;
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
function ph(t, e) {
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
function T(t, e, r) {
  if (t && e)
    if (t.forEach && t.forEach === n1)
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
    return Gs(t);
  if (t.map && t.map === a1)
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
    return Gs(t);
  if (t.filter && t.filter === i1)
    return t.filter(e, r);
  for (var n = [], i = 0, a = t.length; i < a; i++)
    e.call(r, t[i], i, t) && n.push(t[i]);
  return n;
}
function l1(t, e, r) {
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
function f1(t, e) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return function() {
    return t.apply(e, r.concat(ch.call(arguments)));
  };
}
var le = Ua && X(Ua.bind) ? Ua.call.bind(Ua.bind) : f1;
function Te(t) {
  for (var e = [], r = 1; r < arguments.length; r++)
    e[r - 1] = arguments[r];
  return function() {
    return t.apply(this, e.concat(ch.call(arguments)));
  };
}
function B(t) {
  return Array.isArray ? Array.isArray(t) : gi.call(t) === "[object Array]";
}
function X(t) {
  return typeof t == "function";
}
function N(t) {
  return typeof t == "string";
}
function es(t) {
  return gi.call(t) === "[object String]";
}
function ve(t) {
  return typeof t == "number";
}
function F(t) {
  var e = typeof t;
  return e === "function" || !!t && e === "object";
}
function Kl(t) {
  return !!ny[gi.call(t)];
}
function Ke(t) {
  return !!iy[gi.call(t)];
}
function ui(t) {
  return typeof t == "object" && typeof t.nodeType == "number" && typeof t.ownerDocument == "object";
}
function Aa(t) {
  return t.colorStops != null;
}
function oy(t) {
  return t.image != null;
}
function h1(t) {
  return gi.call(t) === "[object RegExp]";
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
function Gs(t) {
  for (var e = [], r = 1; r < arguments.length; r++)
    e[r - 1] = arguments[r];
  return ch.apply(t, e);
}
function Us(t) {
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
var sy = "__ec_primitive__";
function va(t) {
  t[sy] = !0;
}
function ei(t) {
  return t[sy];
}
var c1 = (function() {
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
})(), uy = typeof Map == "function";
function v1() {
  return uy ? /* @__PURE__ */ new Map() : new c1();
}
var ly = (function() {
  function t(e) {
    var r = B(e);
    this.data = v1();
    var n = this;
    e instanceof t ? e.each(i) : e && T(e, i);
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
    return uy ? Array.from(e) : e;
  }, t.prototype.removeKey = function(e) {
    this.data.delete(e);
  }, t;
})();
function U(t) {
  return new ly(t);
}
function fy(t, e) {
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
  return e && R(r, e), r;
}
function gh(t) {
  var e = t.style;
  e.webkitUserSelect = "none", e.userSelect = "none", e.webkitTapHighlightColor = "rgba(0,0,0,0)", e["-webkit-touch-callout"] = "none";
}
function qe(t, e) {
  return t.hasOwnProperty(e);
}
function Ee() {
}
var hy = 180 / Math.PI, d1 = Number.EPSILON || Math.pow(2, -52);
const p1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  EPSILON: d1,
  HashMap: ly,
  RADIAN_TO_DEGREE: hy,
  assert: qt,
  assignProps: ay,
  bind: le,
  clone: Z,
  concatArray: fy,
  createCanvas: u1,
  createHashMap: U,
  createObject: Pa,
  curry: Te,
  defaults: _e,
  disableUserSelect: gh,
  each: T,
  eqNaN: ha,
  extend: R,
  filter: xe,
  find: l1,
  guid: dh,
  hasOwn: qe,
  indexOf: oe,
  inherits: ph,
  isArray: B,
  isArrayLike: Ze,
  isBuiltInObject: Kl,
  isDom: ui,
  isFunction: X,
  isGradientObject: Aa,
  isImagePatternObject: oy,
  isNumber: ve,
  isObject: F,
  isPrimitive: ei,
  isRegExp: h1,
  isString: N,
  isStringSafe: es,
  isTypedArray: Ke,
  keys: me,
  logError: Vs,
  map: H,
  merge: pe,
  mergeAll: s1,
  mixin: Lt,
  noop: Ee,
  normalizeCssArray: Us,
  reduce: Xt,
  retrieve: ca,
  retrieve2: G,
  retrieve3: gn,
  setAsPrimitive: va,
  slice: Gs,
  trim: It
}, Symbol.toStringTag, { value: "Module" }));
function xn(t, e) {
  return t == null && (t = 0), e == null && (e = 0), [t, e];
}
function g1(t, e) {
  return t[0] = e[0], t[1] = e[1], t;
}
function cy(t) {
  return [t[0], t[1]];
}
function Eo(t, e, r) {
  return t[0] = e, t[1] = r, t;
}
function jl(t, e, r) {
  return t[0] = e[0] + r[0], t[1] = e[1] + r[1], t;
}
function y1(t, e, r, n) {
  return t[0] = e[0] + r[0] * n, t[1] = e[1] + r[1] * n, t;
}
function vy(t, e, r) {
  return t[0] = e[0] - r[0], t[1] = e[1] - r[1], t;
}
function yh(t) {
  return Math.sqrt(mh(t));
}
var m1 = yh;
function mh(t) {
  return t[0] * t[0] + t[1] * t[1];
}
var _1 = mh;
function S1(t, e, r) {
  return t[0] = e[0] * r[0], t[1] = e[1] * r[1], t;
}
function b1(t, e, r) {
  return t[0] = e[0] / r[0], t[1] = e[1] / r[1], t;
}
function w1(t, e) {
  return t[0] * e[0] + t[1] * e[1];
}
function Ro(t, e, r) {
  return t[0] = e[0] * r, t[1] = e[1] * r, t;
}
function dy(t, e) {
  var r = yh(e);
  return r === 0 ? (t[0] = 0, t[1] = 0) : (t[0] = e[0] / r, t[1] = e[1] / r), t;
}
function ts(t, e) {
  return Math.sqrt((t[0] - e[0]) * (t[0] - e[0]) + (t[1] - e[1]) * (t[1] - e[1]));
}
var py = ts;
function gy(t, e) {
  return (t[0] - e[0]) * (t[0] - e[0]) + (t[1] - e[1]) * (t[1] - e[1]);
}
var yn = gy;
function T1(t, e) {
  return t[0] = -e[0], t[1] = -e[1], t;
}
function C1(t, e, r, n) {
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
const x1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  add: jl,
  applyTransform: ot,
  clone: cy,
  copy: g1,
  create: xn,
  dist: py,
  distSquare: yn,
  distance: ts,
  distanceSquare: gy,
  div: b1,
  dot: w1,
  len: yh,
  lenSquare: mh,
  length: m1,
  lengthSquare: _1,
  lerp: C1,
  max: xr,
  min: Cr,
  mul: S1,
  negate: T1,
  normalize: dy,
  scale: Ro,
  scaleAndAdd: y1,
  set: Eo,
  sub: vy
}, Symbol.toStringTag, { value: "Module" }));
var En = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.target = e, this.topTarget = r && r.topTarget;
  }
  return t;
})(), M1 = (function() {
  function t(e) {
    this.handler = e, e.on("mousedown", this._dragStart, this), e.on("mousemove", this._drag, this), e.on("mouseup", this._dragEnd, this);
  }
  return t.prototype._dragStart = function(e) {
    for (var r = e.target; r && !r.draggable; )
      r = r.parent || r.__hostTarget;
    r && (this._draggingTarget = r, r.dragging = !0, this._x = e.offsetX, this._y = e.offsetY, this.handler.dispatchToElement(new En(r, e), "dragstart", e.event));
  }, t.prototype._drag = function(e) {
    var r = this._draggingTarget;
    if (r) {
      var n = e.offsetX, i = e.offsetY, a = n - this._x, o = i - this._y;
      this._x = n, this._y = i, r.drift(a, o, e), this.handler.dispatchToElement(new En(r, e), "drag", e.event);
      var s = this.handler.findHover(n, i, r).target, u = this._dropTarget;
      this._dropTarget = s, r !== s && (u && s !== u && this.handler.dispatchToElement(new En(u, e), "dragleave", e.event), s && s !== u && this.handler.dispatchToElement(new En(s, e), "dragenter", e.event));
    }
  }, t.prototype._dragEnd = function(e) {
    var r = this._draggingTarget;
    r && (r.dragging = !1), this.handler.dispatchToElement(new En(r, e), "dragend", e.event), this._dropTarget && this.handler.dispatchToElement(new En(this._dropTarget, e), "drop", e.event), this._draggingTarget = null, this._dropTarget = null;
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
})(), D1 = Math.log(2);
function Ql(t, e, r, n, i, a) {
  var o = n + "-" + i, s = t.length;
  if (a.hasOwnProperty(o))
    return a[o];
  if (e === 1) {
    var u = Math.round(Math.log((1 << s) - 1 & ~i) / D1);
    return t[r][u];
  }
  for (var l = n | 1 << r, f = r + 1; n & 1 << f; )
    f++;
  for (var h = 0, v = 0, c = 0; v < s; v++) {
    var d = 1 << v;
    d & i || (h += (c % 2 ? -1 : 1) * t[r][v] * Ql(t, e - 1, f, l, i | d, a), c++);
  }
  return a[o] = h, h;
}
function tv(t, e) {
  var r = [
    [t[0], t[1], 1, 0, 0, 0, -e[0] * t[0], -e[0] * t[1]],
    [0, 0, 0, t[0], t[1], 1, -e[1] * t[0], -e[1] * t[1]],
    [t[2], t[3], 1, 0, 0, 0, -e[2] * t[2], -e[2] * t[3]],
    [0, 0, 0, t[2], t[3], 1, -e[3] * t[2], -e[3] * t[3]],
    [t[4], t[5], 1, 0, 0, 0, -e[4] * t[4], -e[4] * t[5]],
    [0, 0, 0, t[4], t[5], 1, -e[5] * t[4], -e[5] * t[5]],
    [t[6], t[7], 1, 0, 0, 0, -e[6] * t[6], -e[6] * t[7]],
    [0, 0, 0, t[6], t[7], 1, -e[7] * t[6], -e[7] * t[7]]
  ], n = {}, i = Ql(r, 8, 0, 0, 0, n);
  if (i !== 0) {
    for (var a = [], o = 0; o < 8; o++)
      for (var s = 0; s < 8; s++)
        a[s] == null && (a[s] = 0), a[s] += ((o + s) % 2 ? -1 : 1) * Ql(r, 7, o === 0 ? 1 : 0, 1 << o, 1 << s, n) / i * e[o];
    return function(u, l, f) {
      var h = l * a[6] + f * a[7] + 1;
      u[0] = (l * a[0] + f * a[1] + a[2]) / h, u[1] = (l * a[3] + f * a[4] + a[5]) / h;
    };
  }
}
var rs = "___zrEVENTSAVED", Du = [];
function I1(t, e, r, n, i) {
  return Jl(Du, e, n, i, !0) && Jl(t, r, Du[0], Du[1]);
}
function A1(t, e) {
  t && r(t), e && r(e);
  function r(n) {
    var i = n[rs];
    i && (i.clearMarkers && i.clearMarkers(), delete n[rs]);
  }
}
function Jl(t, e, r, n, i) {
  if (e.getBoundingClientRect && W.domSupported && !yy(e)) {
    var a = e[rs] || (e[rs] = {}), o = P1(e, a), s = L1(o, a, i);
    if (s)
      return s(t, r, n), !0;
  }
  return !1;
}
function P1(t, e) {
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
    T(r, function(f) {
      f.parentNode && f.parentNode.removeChild(f);
    });
  }, r;
}
function L1(t, e, r) {
  for (var n = r ? "invTrans" : "trans", i = e[n], a = e.srcCoords, o = [], s = [], u = !0, l = 0; l < 4; l++) {
    var f = t[l].getBoundingClientRect(), h = 2 * l, v = f.left, c = f.top;
    o.push(v, c), u = u && a && v === a[h] && c === a[h + 1], s.push(t[l].offsetLeft, t[l].offsetTop);
  }
  return u && i ? i : (e.srcCoords = o, e[n] = r ? tv(s, o) : tv(o, s));
}
function yy(t) {
  return t.nodeName.toUpperCase() === "CANVAS";
}
var E1 = /([&<>"'])/g, R1 = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;"
};
function Ue(t) {
  return t == null ? "" : (t + "").replace(E1, function(e, r) {
    return R1[r];
  });
}
var O1 = /^(?:mouse|pointer|contextmenu|drag|drop)|click/, Iu = [], k1 = W.browser.firefox && +W.browser.version.split(".")[0] < 39;
function ef(t, e, r, n) {
  return r = r || {}, n ? rv(t, e, r) : k1 && e.layerX != null && e.layerX !== e.offsetX ? (r.zrX = e.layerX, r.zrY = e.layerY) : e.offsetX != null ? (r.zrX = e.offsetX, r.zrY = e.offsetY) : rv(t, e, r), r;
}
function rv(t, e, r) {
  if (W.domSupported && t.getBoundingClientRect) {
    var n = e.clientX, i = e.clientY;
    if (yy(t)) {
      var a = t.getBoundingClientRect();
      r.zrX = n - a.left, r.zrY = i - a.top;
      return;
    } else if (Jl(Iu, t, n, i)) {
      r.zrX = Iu[0], r.zrY = Iu[1];
      return;
    }
  }
  r.zrX = r.zrY = 0;
}
function _h(t) {
  return t || window.event;
}
function dt(t, e, r) {
  if (e = _h(e), e.zrX != null)
    return e;
  var n = e.type, i = n && n.indexOf("touch") >= 0;
  if (i) {
    var o = n !== "touchend" ? e.targetTouches[0] : e.changedTouches[0];
    o && ef(t, o, e, r);
  } else {
    ef(t, e, e, r);
    var a = B1(e);
    e.zrDelta = a ? a / 120 : -(e.detail || 0) / 3;
  }
  var s = e.button;
  return e.which == null && s !== void 0 && O1.test(e.type) && (e.which = s & 1 ? 1 : s & 2 ? 3 : s & 4 ? 2 : 0), e;
}
function B1(t) {
  var e = t.wheelDelta;
  if (e)
    return e;
  var r = t.deltaX, n = t.deltaY;
  if (r == null || n == null)
    return e;
  var i = Math.abs(n !== 0 ? n : r), a = n > 0 ? -1 : n < 0 ? 1 : r > 0 ? -1 : 1;
  return 3 * i * a;
}
function N1(t, e, r, n) {
  t.addEventListener(e, r, n);
}
function F1(t, e, r, n) {
  t.removeEventListener(e, r, n);
}
var my = function(t) {
  t.preventDefault(), t.stopPropagation(), t.cancelBubble = !0;
}, z1 = (function() {
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
        var u = i[o], l = ef(n, u, {});
        a.points.push([l.zrX, l.zrY]), a.touches.push(u);
      }
      this._track.push(a);
    }
  }, t.prototype._recognize = function(e) {
    for (var r in Au)
      if (Au.hasOwnProperty(r)) {
        var n = Au[r](this._track, e);
        if (n)
          return n;
      }
  }, t;
})();
function nv(t) {
  var e = t[1][0] - t[0][0], r = t[1][1] - t[0][1];
  return Math.sqrt(e * e + r * r);
}
function H1(t) {
  return [
    (t[0][0] + t[1][0]) / 2,
    (t[0][1] + t[1][1]) / 2
  ];
}
var Au = {
  pinch: function(t, e) {
    var r = t.length;
    if (r) {
      var n = (t[r - 1] || {}).points, i = (t[r - 2] || {}).points || n;
      if (i && i.length > 1 && n && n.length > 1) {
        var a = nv(n) / nv(i);
        !isFinite(a) && (a = 1), e.pinchScale = a;
        var o = H1(n);
        return e.pinchX = o[0], e.pinchY = o[1], {
          type: "pinch",
          target: t[0].target,
          event: e
        };
      }
    }
  }
};
function wt() {
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
function ns(t, e, r) {
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t[4] = e[4] + r[0], t[5] = e[5] + r[1], t;
}
function Ys(t, e, r, n) {
  n === void 0 && (n = [0, 0]);
  var i = e[0], a = e[2], o = e[4], s = e[1], u = e[3], l = e[5], f = Math.sin(r), h = Math.cos(r);
  return t[0] = i * h + s * f, t[1] = -i * f + s * h, t[2] = a * h + u * f, t[3] = -a * f + h * u, t[4] = h * (o - n[0]) + f * (l - n[1]) + n[0], t[5] = h * (l - n[1]) - f * (o - n[0]) + n[1], t;
}
function _y(t, e, r) {
  var n = r[0], i = r[1];
  return t[0] = e[0] * n, t[1] = e[1] * i, t[2] = e[2] * n, t[3] = e[3] * i, t[4] = e[4] * n, t[5] = e[5] * i, t;
}
function yi(t, e) {
  var r = e[0], n = e[2], i = e[4], a = e[1], o = e[3], s = e[5], u = r * o - a * n;
  return u ? (u = 1 / u, t[0] = o * u, t[1] = -a * u, t[2] = -n * u, t[3] = r * u, t[4] = (n * s - o * i) * u, t[5] = (a * i - r * s) * u, t) : null;
}
function V1(t) {
  var e = wt();
  return La(e, t), e;
}
const G1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  clone: V1,
  copy: La,
  create: wt,
  identity: Mn,
  invert: yi,
  mul: mn,
  rotate: Ys,
  scale: _y,
  translate: ns
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
})(), hn = Math.min, Kn = Math.max, tf = Math.abs, iv = ["x", "y"], U1 = ["width", "height"], Hr = new ue(), Vr = new ue(), Gr = new ue(), Ur = new ue(), at = Sy(), qi = at.minTv, rf = at.maxTv, ea = [0, 0], Y = (function() {
  function t(e, r, n, i) {
    Pu(this, e, r, n, i);
  }
  return t.set = function(e, r, n, i, a) {
    return i < 0 && (r = r + i, i = -i), a < 0 && (n = n + a, a = -a), e.x = r, e.y = n, e.width = i, e.height = a, e;
  }, t.prototype.union = function(e) {
    var r = hn(e.x, this.x), n = hn(e.y, this.y);
    isFinite(this.x) && isFinite(this.width) ? this.width = Kn(e.x + e.width, this.x + this.width) - r : this.width = e.width, isFinite(this.y) && isFinite(this.height) ? this.height = Kn(e.y + e.height, this.y + this.height) - n : this.height = e.height, this.x = r, this.y = n;
  }, t.prototype.applyTransform = function(e) {
    t.applyTransform(this, this, e);
  }, t.prototype.calculateTransform = function(e) {
    return Y1(wt(), this, e);
  }, t.prototype.intersect = function(e, r, n) {
    return t.intersect(this, e, r, n);
  }, t.intersect = function(e, r, n, i) {
    n && ue.set(n, 0, 0);
    var a = i && i.outIntersectRect || null, o = i && i.clamp;
    if (a && (a.x = a.y = a.width = a.height = NaN), !e || !r)
      return !1;
    e instanceof t || (e = Pu(W1, e.x, e.y, e.width, e.height)), r instanceof t || (r = Pu(X1, r.x, r.y, r.width, r.height));
    var s = !!n;
    at.reset(i, s);
    var u = at.touchThreshold, l = e.x + u, f = e.x + e.width - u, h = e.y + u, v = e.y + e.height - u, c = r.x + u, d = r.x + r.width - u, y = r.y + u, p = r.y + r.height - u;
    if (l > f || h > v || c > d || y > p)
      return !1;
    var g = !(f < c || d < l || v < y || p < h);
    return (s || a) && (ea[0] = 1 / 0, ea[1] = 0, ov(l, f, c, d, 0, s, a, o), ov(h, v, y, p, 1, s, a, o), s && ue.copy(n, g ? at.useDir ? at.dirMinTv : qi : rf)), g;
  }, t.contain = function(e, r, n) {
    return r >= e.x && r <= e.x + e.width && n >= e.y && n <= e.y + e.height;
  }, t.prototype.contain = function(e, r) {
    return t.contain(this, e, r);
  }, t.prototype.clone = function() {
    return new t(this.x, this.y, this.width, this.height);
  }, t.prototype.copy = function(e) {
    av(this, e);
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
      e !== r && av(e, r);
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
    return e = Mn(e || []), ns(e, e, Eo(Lu, -r.x, -r.y)), _y(e, e, Eo(Lu, i, a)), ns(e, e, Eo(Lu, n.x, n.y)), e;
  }, t;
})();
Y.create;
var Pu = Y.set, av = Y.copy, Y1 = Y.calculateTransform;
Y.applyTransform;
Y.contain;
var W1 = new Y(0, 0, 0, 0), X1 = new Y(0, 0, 0, 0), Lu = [];
function ov(t, e, r, n, i, a, o, s) {
  var u = tf(e - r), l = tf(n - t), f = hn(u, l), h = iv[i], v = iv[1 - i], c = U1[i];
  e < r || n < t ? u < l ? (a && (rf[h] = -u), s && (o[h] = e, o[c] = 0)) : (a && (rf[h] = l), s && (o[h] = t, o[c] = 0)) : (o && (o[h] = Kn(t, r), o[c] = hn(e, n) - o[h]), a && (f < ea[0] || at.useDir) && (ea[0] = hn(f, ea[0]), (u < l || !at.bidirectional) && (qi[h] = u, qi[v] = 0, at.useDir && at.calcDirMTV()), (u >= l || !at.bidirectional) && (qi[h] = -l, qi[v] = 0, at.useDir && at.calcDirMTV())));
}
function Sy() {
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
    return tf(a) < 1e-10;
  }
  return n;
}
var by = "silent";
function q1(t, e, r) {
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
    stop: $1
  };
}
function $1() {
  my(this.event);
}
var Z1 = (function(t) {
  z(e, t);
  function e() {
    var r = t !== null && t.apply(this, arguments) || this;
    return r.handler = null, r;
  }
  return e.prototype.dispose = function() {
  }, e.prototype.setCursor = function() {
  }, e;
})(Kt), Ii = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.x = e, this.y = r;
  }
  return t;
})(), K1 = [
  "click",
  "dblclick",
  "mousewheel",
  "mouseout",
  "mouseup",
  "mousedown",
  "mousemove",
  "contextmenu"
], Eu = new Y(0, 0, 0, 0), wy = (function(t) {
  z(e, t);
  function e(r, n, i, a, o) {
    var s = t.call(this) || this;
    return s._hovered = new Ii(0, 0), s.storage = r, s.painter = n, s.painterRoot = a, s._pointerSize = o, i = i || new Z1(), s.proxy = null, s.setHandlerProxy(i), s._draggingMgr = new M1(s), s;
  }
  return e.prototype.setHandlerProxy = function(r) {
    this.proxy && this.proxy.dispose(), r && (T(K1, function(n) {
      r.on && r.on(n, this[n], this);
    }, this), r.handler = this), this.proxy = r;
  }, e.prototype.mousemove = function(r) {
    var n = r.zrX, i = r.zrY, a = Ty(this, n, i), o = this._hovered, s = o.target;
    s && !s.__zr && (o = this.findHover(o.x, o.y), s = o.target);
    var u = this._hovered = a ? new Ii(n, i) : this.findHover(n, i), l = u.target, f = this.proxy;
    f.setCursor && f.setCursor(l ? l.cursor : "default"), s && l !== s && this.dispatchToElement(o, "mouseout", r), this.dispatchToElement(u, "mousemove", r), l && l !== s && this.dispatchToElement(u, "mouseover", r);
  }, e.prototype.mouseout = function(r) {
    var n = r.zrEventControl;
    n !== "only_globalout" && this.dispatchToElement(this._hovered, "mouseout", r), n !== "no_globalout" && this.trigger("globalout", { type: "globalout", event: r });
  }, e.prototype.resize = function() {
    this._hovered = new Ii(0, 0);
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
      for (var o = "on" + n, s = q1(n, r, i); a && (a[o] && (s.cancelBubble = !!a[o].call(a, s)), a.trigger(n, s), a = a.__hostTarget ? a.__hostTarget : a.parent, !s.cancelBubble); )
        ;
      s.cancelBubble || (this.trigger(n, s), this.painter && this.painter.eachOtherLayer && this.painter.eachOtherLayer(function(u) {
        typeof u[o] == "function" && u[o].call(u, s), u.trigger && u.trigger(n, s);
      }));
    }
  }, e.prototype.findHover = function(r, n, i) {
    var a = this.storage.getDisplayList(), o = new Ii(r, n);
    if (sv(a, o, r, n, i), this._pointerSize && !o.target) {
      for (var s = [], u = this._pointerSize, l = u / 2, f = new Y(r - l, n - l, u, u), h = a.length - 1; h >= 0; h--) {
        var v = a[h];
        v !== i && !v.ignore && !v.ignoreCoarsePointer && (!v.parent || !v.parent.ignoreCoarsePointer) && (Eu.copy(v.getBoundingRect()), v.transform && Eu.applyTransform(v.transform), Eu.intersect(f) && s.push(v));
      }
      if (s.length)
        for (var c = 4, d = Math.PI / 12, y = Math.PI * 2, p = 0; p < l; p += c)
          for (var g = 0; g < y; g += d) {
            var m = r + p * Math.cos(g), _ = n + p * Math.sin(g);
            if (sv(s, o, m, _, i), o.target)
              return o;
          }
    }
    return o;
  }, e.prototype.processGesture = function(r, n) {
    this._gestureMgr || (this._gestureMgr = new z1());
    var i = this._gestureMgr;
    n === "start" && i.clear();
    var a = i.recognize(r, this.findHover(r.zrX, r.zrY, null).target, this.proxy.dom);
    if (n === "end" && i.clear(), a) {
      var o = a.type;
      r.gestureEvent = o;
      var s = new Ii();
      s.target = a.target, this.dispatchToElement(s, o, a.event);
    }
  }, e;
})(Kt);
T(["click", "mousedown", "mouseup", "mousewheel", "dblclick", "contextmenu"], function(t) {
  wy.prototype[t] = function(e) {
    var r = e.zrX, n = e.zrY, i = Ty(this, r, n), a, o;
    if ((t !== "mouseup" || !i) && (a = this.findHover(r, n), o = a.target), t === "mousedown")
      this._downEl = o, this._downPoint = [e.zrX, e.zrY], this._upEl = o;
    else if (t === "mouseup")
      this._upEl = o;
    else if (t === "click") {
      if (this._downEl !== this._upEl || !this._downPoint || py(this._downPoint, [e.zrX, e.zrY]) > 4)
        return;
      this._downPoint = null;
    }
    this.dispatchToElement(a, t, e);
  };
});
function j1(t, e, r) {
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
    return i ? by : !0;
  }
  return !1;
}
function sv(t, e, r, n, i) {
  for (var a = t.length - 1; a >= 0; a--) {
    var o = t[a], s = void 0;
    if (o !== i && !o.ignore && (s = j1(o, r, n)) && (!e.topTarget && (e.topTarget = o), s !== by)) {
      e.target = o;
      break;
    }
  }
}
function Ty(t, e, r) {
  var n = t.painter;
  return e < 0 || e > n.getWidth() || r < 0 || r > n.getHeight();
}
var Cy = 32, Ai = 7;
function Q1(t) {
  for (var e = 0; t >= Cy; )
    e |= t & 1, t >>= 1;
  return t + e;
}
function uv(t, e, r, n) {
  var i = e + 1;
  if (i === r)
    return 1;
  if (n(t[i++], t[e]) < 0) {
    for (; i < r && n(t[i], t[i - 1]) < 0; )
      i++;
    J1(t, e, i);
  } else
    for (; i < r && n(t[i], t[i - 1]) >= 0; )
      i++;
  return i - e;
}
function J1(t, e, r) {
  for (r--; e < r; ) {
    var n = t[e];
    t[e++] = t[r], t[r--] = n;
  }
}
function lv(t, e, r, n, i) {
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
function Ru(t, e, r, n, i, a) {
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
function Ou(t, e, r, n, i, a) {
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
function eb(t, e) {
  var r = Ai, n, i, a = 0, o = [];
  n = [], i = [];
  function s(c, d) {
    n[a] = c, i[a] = d, a += 1;
  }
  function u() {
    for (; a > 1; ) {
      var c = a - 2;
      if (c >= 1 && i[c - 1] <= i[c] + i[c + 1] || c >= 2 && i[c - 2] <= i[c] + i[c - 1])
        i[c - 1] < i[c + 1] && c--;
      else if (i[c] > i[c + 1])
        break;
      f(c);
    }
  }
  function l() {
    for (; a > 1; ) {
      var c = a - 2;
      c > 0 && i[c - 1] < i[c + 1] && c--, f(c);
    }
  }
  function f(c) {
    var d = n[c], y = i[c], p = n[c + 1], g = i[c + 1];
    i[c] = y + g, c === a - 3 && (n[c + 1] = n[c + 2], i[c + 1] = i[c + 2]), a--;
    var m = Ou(t[p], t, d, y, 0, e);
    d += m, y -= m, y !== 0 && (g = Ru(t[d + y - 1], t, p, g, g - 1, e), g !== 0 && (y <= g ? h(d, y, p, g) : v(d, y, p, g)));
  }
  function h(c, d, y, p) {
    var g = 0;
    for (g = 0; g < d; g++)
      o[g] = t[c + g];
    var m = 0, _ = y, S = c;
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
    for (var b = r, w, C, M; ; ) {
      w = 0, C = 0, M = !1;
      do
        if (e(t[_], o[m]) < 0) {
          if (t[S++] = t[_++], C++, w = 0, --p === 0) {
            M = !0;
            break;
          }
        } else if (t[S++] = o[m++], w++, C = 0, --d === 1) {
          M = !0;
          break;
        }
      while ((w | C) < b);
      if (M)
        break;
      do {
        if (w = Ou(t[_], o, m, d, 0, e), w !== 0) {
          for (g = 0; g < w; g++)
            t[S + g] = o[m + g];
          if (S += w, m += w, d -= w, d <= 1) {
            M = !0;
            break;
          }
        }
        if (t[S++] = t[_++], --p === 0) {
          M = !0;
          break;
        }
        if (C = Ru(o[m], t, _, p, 0, e), C !== 0) {
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
      } while (w >= Ai || C >= Ai);
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
  function v(c, d, y, p) {
    var g = 0;
    for (g = 0; g < p; g++)
      o[g] = t[y + g];
    var m = c + d - 1, _ = p - 1, S = y + p - 1, b = 0, w = 0;
    if (t[S--] = t[m--], --d === 0) {
      for (b = S - (p - 1), g = 0; g < p; g++)
        t[b + g] = o[g];
      return;
    }
    if (p === 1) {
      for (S -= d, m -= d, w = S + 1, b = m + 1, g = d - 1; g >= 0; g--)
        t[w + g] = t[b + g];
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
        if (M = d - Ou(o[_], t, c, d, d - 1, e), M !== 0) {
          for (S -= M, m -= M, d -= M, w = S + 1, b = m + 1, g = M - 1; g >= 0; g--)
            t[w + g] = t[b + g];
          if (d === 0) {
            D = !0;
            break;
          }
        }
        if (t[S--] = o[_--], --p === 1) {
          D = !0;
          break;
        }
        if (x = p - Ru(t[m], o, 0, p, p - 1, e), x !== 0) {
          for (S -= x, _ -= x, p -= x, w = S + 1, b = _ + 1, g = 0; g < x; g++)
            t[w + g] = o[b + g];
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
      } while (M >= Ai || x >= Ai);
      if (D)
        break;
      C < 0 && (C = 0), C += 2;
    }
    if (r = C, r < 1 && (r = 1), p === 1) {
      for (S -= d, m -= d, w = S + 1, b = m + 1, g = d - 1; g >= 0; g--)
        t[w + g] = t[b + g];
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
    if (i < Cy) {
      a = uv(t, r, n, e), lv(t, r, n, r + a, e);
      return;
    }
    var o = eb(t, e), s = Q1(i);
    do {
      if (a = uv(t, r, n, e), a < s) {
        var u = i;
        u > s && (u = s), lv(t, r, r + u, r + a, e), a = u;
      }
      o.pushRun(r, a), o.mergeRuns(), i -= a, r += a;
    } while (i !== 0);
    o.forceMergeRuns();
  }
}
var Je = 1, $i = 2, $n = 4, fv = !1;
function ku() {
  fv || (fv = !0, console.warn("z / z2 / zlevel of displayable is invalid, which may cause unexpected errors"));
}
function hv(t, e) {
  return t.zlevel === e.zlevel ? t.z === e.z ? t.z2 - e.z2 : t.z - e.z : t.zlevel - e.zlevel;
}
var tb = (function() {
  function t() {
    this._roots = [], this._displayList = [], this._displayListLen = 0, this.displayableSortFunc = hv;
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
    n.length = this._displayListLen, Oo(n, hv);
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
        for (var h = e.childrenRef(), v = 0; v < h.length; v++) {
          var c = h[v];
          e.__dirty && (c.__dirty |= Je), this._updateAndAddDisplayable(c, s, n);
        }
        e.__dirty = 0;
      } else {
        var d = e;
        isNaN(d.z) && (ku(), d.z = 0), isNaN(d.z2) && (ku(), d.z2 = 0), isNaN(d.zlevel) && (ku(), d.zlevel = 0), this._displayList[this._displayListLen++] = d;
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
})(), is;
is = W.hasGlobalWindow && (window.requestAnimationFrame && window.requestAnimationFrame.bind(window) || window.msRequestAnimationFrame && window.msRequestAnimationFrame.bind(window) || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame) || function(t) {
  return setTimeout(t, 16);
};
var ta = {
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
    return 1 - ta.bounceOut(1 - t);
  },
  bounceOut: function(t) {
    return t < 1 / 2.75 ? 7.5625 * t * t : t < 2 / 2.75 ? 7.5625 * (t -= 1.5 / 2.75) * t + 0.75 : t < 2.5 / 2.75 ? 7.5625 * (t -= 2.25 / 2.75) * t + 0.9375 : 7.5625 * (t -= 2.625 / 2.75) * t + 0.984375;
  },
  bounceInOut: function(t) {
    return t < 0.5 ? ta.bounceIn(t * 2) * 0.5 : ta.bounceOut(t * 2 - 1) * 0.5 + 0.5;
  }
}, Ya = Math.pow, Ar = Math.sqrt, as = 1e-8, xy = 1e-4, cv = Ar(3), Wa = 1 / 3, Ht = xn(), yt = xn(), ti = xn();
function Mr(t) {
  return t > -as && t < as;
}
function My(t) {
  return t > as || t < -as;
}
function Pe(t, e, r, n, i) {
  var a = 1 - i;
  return a * a * (a * t + 3 * i * e) + i * i * (i * n + 3 * a * r);
}
function vv(t, e, r, n, i) {
  var a = 1 - i;
  return 3 * (((e - t) * a + 2 * (r - e) * i) * a + (n - r) * i * i);
}
function Dy(t, e, r, n, i, a) {
  var o = n + 3 * (e - r) - t, s = 3 * (r - e * 2 + t), u = 3 * (e - t), l = t - i, f = s * s - 3 * o * u, h = s * u - 9 * o * l, v = u * u - 3 * s * l, c = 0;
  if (Mr(f) && Mr(h))
    if (Mr(s))
      a[0] = 0;
    else {
      var d = -u / s;
      d >= 0 && d <= 1 && (a[c++] = d);
    }
  else {
    var y = h * h - 4 * f * v;
    if (Mr(y)) {
      var p = h / f, d = -s / o + p, g = -p / 2;
      d >= 0 && d <= 1 && (a[c++] = d), g >= 0 && g <= 1 && (a[c++] = g);
    } else if (y > 0) {
      var m = Ar(y), _ = f * s + 1.5 * o * (-h + m), S = f * s + 1.5 * o * (-h - m);
      _ < 0 ? _ = -Ya(-_, Wa) : _ = Ya(_, Wa), S < 0 ? S = -Ya(-S, Wa) : S = Ya(S, Wa);
      var d = (-s - (_ + S)) / (3 * o);
      d >= 0 && d <= 1 && (a[c++] = d);
    } else {
      var b = (2 * f * s - 3 * o * h) / (2 * Ar(f * f * f)), w = Math.acos(b) / 3, C = Ar(f), M = Math.cos(w), d = (-s - 2 * C * M) / (3 * o), g = (-s + C * (M + cv * Math.sin(w))) / (3 * o), x = (-s + C * (M - cv * Math.sin(w))) / (3 * o);
      d >= 0 && d <= 1 && (a[c++] = d), g >= 0 && g <= 1 && (a[c++] = g), x >= 0 && x <= 1 && (a[c++] = x);
    }
  }
  return c;
}
function Iy(t, e, r, n, i) {
  var a = 6 * r - 12 * e + 6 * t, o = 9 * e + 3 * n - 3 * t - 9 * r, s = 3 * e - 3 * t, u = 0;
  if (Mr(o)) {
    if (My(a)) {
      var l = -s / a;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = a * a - 4 * o * s;
    if (Mr(f))
      i[0] = -a / (2 * o);
    else if (f > 0) {
      var h = Ar(f), l = (-a + h) / (2 * o), v = (-a - h) / (2 * o);
      l >= 0 && l <= 1 && (i[u++] = l), v >= 0 && v <= 1 && (i[u++] = v);
    }
  }
  return u;
}
function os(t, e, r, n, i, a) {
  var o = (e - t) * i + t, s = (r - e) * i + e, u = (n - r) * i + r, l = (s - o) * i + o, f = (u - s) * i + s, h = (f - l) * i + l;
  a[0] = t, a[1] = o, a[2] = l, a[3] = h, a[4] = h, a[5] = f, a[6] = u, a[7] = n;
}
function rb(t, e, r, n, i, a, o, s, u, l, f) {
  var h, v = 5e-3, c = 1 / 0, d, y, p, g;
  Ht[0] = u, Ht[1] = l;
  for (var m = 0; m < 1; m += 0.05)
    yt[0] = Pe(t, r, i, o, m), yt[1] = Pe(e, n, a, s, m), p = yn(Ht, yt), p < c && (h = m, c = p);
  c = 1 / 0;
  for (var _ = 0; _ < 32 && !(v < xy); _++)
    d = h - v, y = h + v, yt[0] = Pe(t, r, i, o, d), yt[1] = Pe(e, n, a, s, d), p = yn(yt, Ht), d >= 0 && p < c ? (h = d, c = p) : (ti[0] = Pe(t, r, i, o, y), ti[1] = Pe(e, n, a, s, y), g = yn(ti, Ht), y <= 1 && g < c ? (h = y, c = g) : v *= 0.5);
  return f && (f[0] = Pe(t, r, i, o, h), f[1] = Pe(e, n, a, s, h)), Ar(c);
}
function nb(t, e, r, n, i, a, o, s, u) {
  for (var l = t, f = e, h = 0, v = 1 / u, c = 1; c <= u; c++) {
    var d = c * v, y = Pe(t, r, i, o, d), p = Pe(e, n, a, s, d), g = y - l, m = p - f;
    h += Math.sqrt(g * g + m * m), l = y, f = p;
  }
  return h;
}
function Be(t, e, r, n) {
  var i = 1 - n;
  return i * (i * t + 2 * n * e) + n * n * r;
}
function dv(t, e, r, n) {
  return 2 * ((1 - n) * (e - t) + n * (r - e));
}
function ib(t, e, r, n, i) {
  var a = t - 2 * e + r, o = 2 * (e - t), s = t - n, u = 0;
  if (Mr(a)) {
    if (My(o)) {
      var l = -s / o;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = o * o - 4 * a * s;
    if (Mr(f)) {
      var l = -o / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l);
    } else if (f > 0) {
      var h = Ar(f), l = (-o + h) / (2 * a), v = (-o - h) / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l), v >= 0 && v <= 1 && (i[u++] = v);
    }
  }
  return u;
}
function Ay(t, e, r) {
  var n = t + r - 2 * e;
  return n === 0 ? 0.5 : (t - e) / n;
}
function ss(t, e, r, n, i) {
  var a = (e - t) * n + t, o = (r - e) * n + e, s = (o - a) * n + a;
  i[0] = t, i[1] = a, i[2] = s, i[3] = s, i[4] = o, i[5] = r;
}
function ab(t, e, r, n, i, a, o, s, u) {
  var l, f = 5e-3, h = 1 / 0;
  Ht[0] = o, Ht[1] = s;
  for (var v = 0; v < 1; v += 0.05) {
    yt[0] = Be(t, r, i, v), yt[1] = Be(e, n, a, v);
    var c = yn(Ht, yt);
    c < h && (l = v, h = c);
  }
  h = 1 / 0;
  for (var d = 0; d < 32 && !(f < xy); d++) {
    var y = l - f, p = l + f;
    yt[0] = Be(t, r, i, y), yt[1] = Be(e, n, a, y);
    var c = yn(yt, Ht);
    if (y >= 0 && c < h)
      l = y, h = c;
    else {
      ti[0] = Be(t, r, i, p), ti[1] = Be(e, n, a, p);
      var g = yn(ti, Ht);
      p <= 1 && g < h ? (l = p, h = g) : f *= 0.5;
    }
  }
  return u && (u[0] = Be(t, r, i, l), u[1] = Be(e, n, a, l)), Ar(h);
}
function ob(t, e, r, n, i, a, o) {
  for (var s = t, u = e, l = 0, f = 1 / o, h = 1; h <= o; h++) {
    var v = h * f, c = Be(t, r, i, v), d = Be(e, n, a, v), y = c - s, p = d - u;
    l += Math.sqrt(y * y + p * p), s = c, u = d;
  }
  return l;
}
var sb = /cubic-bezier\(([0-9,\.e ]+)\)/;
function Py(t) {
  var e = t && sb.exec(t);
  if (e) {
    var r = e[1].split(","), n = +It(r[0]), i = +It(r[1]), a = +It(r[2]), o = +It(r[3]);
    if (isNaN(n + i + a + o))
      return;
    var s = [];
    return function(u) {
      return u <= 0 ? 0 : u >= 1 ? 1 : Dy(0, n, a, 1, u, s) && Pe(0, i, o, 1, s[0]);
    };
  }
}
var ub = (function() {
  function t(e) {
    this._inited = !1, this._startTime = 0, this._pausedTime = 0, this._paused = !1, this._life = e.life || 1e3, this._delay = e.delay || 0, this.loop = e.loop || !1, this.onframe = e.onframe || Ee, this.ondestroy = e.ondestroy || Ee, this.onrestart = e.onrestart || Ee, e.easing && this.setEasing(e.easing);
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
    this.easing = e, this.easingFunc = X(e) ? e : ta[e] || Py(e);
  }, t;
})(), Ly = /* @__PURE__ */ (function() {
  function t(e) {
    this.value = e;
  }
  return t;
})(), lb = (function() {
  function t() {
    this._len = 0;
  }
  return t.prototype.insert = function(e) {
    var r = new Ly(e);
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
})(), li = (function() {
  function t(e) {
    this._list = new lb(), this._maxSize = 10, this._map = {}, this._maxSize = e;
  }
  return t.prototype.put = function(e, r) {
    var n = this._list, i = this._map, a = null;
    if (i[e] == null) {
      var o = n.len(), s = this._lastRemovedEntry;
      if (o >= this._maxSize && o > 0) {
        var u = n.head;
        n.remove(u), delete i[u.key], a = u.value, this._lastRemovedEntry = u;
      }
      s ? s.value = r : s = new Ly(r), s.key = e, n.insertEntry(s), i[e] = s;
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
})(), pv = {
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
function fb(t) {
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
function Bu(t, e, r) {
  return r < 0 ? r += 1 : r > 1 && (r -= 1), r * 6 < 1 ? t + (e - t) * r * 6 : r * 2 < 1 ? e : r * 3 < 2 ? t + (e - t) * (2 / 3 - r) * 6 : t;
}
function Dr(t, e, r) {
  return t + (e - t) * r;
}
function ct(t, e, r, n, i) {
  return t[0] = e, t[1] = r, t[2] = n, t[3] = i, t;
}
function nf(t, e) {
  return t[0] = e[0], t[1] = e[1], t[2] = e[2], t[3] = e[3], t;
}
var Ey = new li(20), Xa = null;
function Rn(t, e) {
  Xa && nf(Xa, e), Xa = Ey.put(t, Xa || e.slice());
}
function st(t, e) {
  if (t) {
    e = e || [];
    var r = Ey.get(t);
    if (r)
      return nf(e, r);
    t = t + "";
    var n = t.replace(/ /g, "").toLowerCase();
    if (n in pv)
      return nf(e, pv[n]), Rn(t, e), e;
    var i = n.length;
    if (n.charAt(0) === "#") {
      if (i === 4 || i === 5) {
        var a = parseInt(n.slice(1, 4), 16);
        if (!(a >= 0 && a <= 4095)) {
          ct(e, 0, 0, 0, 1);
          return;
        }
        return ct(e, (a & 3840) >> 4 | (a & 3840) >> 8, a & 240 | (a & 240) >> 4, a & 15 | (a & 15) << 4, i === 5 ? parseInt(n.slice(4), 16) / 15 : 1), Rn(t, e), e;
      } else if (i === 7 || i === 9) {
        var a = parseInt(n.slice(1, 7), 16);
        if (!(a >= 0 && a <= 16777215)) {
          ct(e, 0, 0, 0, 1);
          return;
        }
        return ct(e, (a & 16711680) >> 16, (a & 65280) >> 8, a & 255, i === 9 ? parseInt(n.slice(7), 16) / 255 : 1), Rn(t, e), e;
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
            return ct(e, ko(l[0]), ko(l[1]), ko(l[2]), l.length === 3 ? f : Pr(l[3])), Rn(t, e), e;
          ct(e, 0, 0, 0, 1);
          return;
        case "hsla":
          if (l.length !== 4) {
            ct(e, 0, 0, 0, 1);
            return;
          }
          return l[3] = Pr(l[3]), af(l, e), Rn(t, e), e;
        case "hsl":
          if (l.length !== 3) {
            ct(e, 0, 0, 0, 1);
            return;
          }
          return af(l, e), Rn(t, e), e;
        default:
          return;
      }
    }
    ct(e, 0, 0, 0, 1);
  }
}
function af(t, e) {
  var r = (parseFloat(t[0]) % 360 + 360) % 360 / 360, n = Pr(t[1]), i = Pr(t[2]), a = i <= 0.5 ? i * (n + 1) : i + n - i * n, o = i * 2 - a;
  return e = e || [], ct(e, At(Bu(o, a, r + 1 / 3) * 255), At(Bu(o, a, r) * 255), At(Bu(o, a, r - 1 / 3) * 255), 1), t.length === 4 && (e[3] = t[3]), e;
}
function hb(t) {
  if (t) {
    var e = t[0] / 255, r = t[1] / 255, n = t[2] / 255, i = Math.min(e, r, n), a = Math.max(e, r, n), o = a - i, s = (a + i) / 2, u, l;
    if (o === 0)
      u = 0, l = 0;
    else {
      s < 0.5 ? l = o / (a + i) : l = o / (2 - a - i);
      var f = ((a - e) / 6 + o / 2) / o, h = ((a - r) / 6 + o / 2) / o, v = ((a - n) / 6 + o / 2) / o;
      e === a ? u = v - h : r === a ? u = 1 / 3 + f - v : n === a && (u = 2 / 3 + h - f), u < 0 && (u += 1), u > 1 && (u -= 1);
    }
    var c = [u * 360, l, s];
    return t[3] != null && c.push(t[3]), c;
  }
}
function of(t, e) {
  var r = st(t);
  if (r) {
    for (var n = 0; n < 3; n++)
      e < 0 ? r[n] = r[n] * (1 - e) | 0 : r[n] = (255 - r[n]) * e + r[n] | 0, r[n] > 255 ? r[n] = 255 : r[n] < 0 && (r[n] = 0);
    return Br(r, r.length === 4 ? "rgba" : "rgb");
  }
}
function cb(t) {
  var e = st(t);
  if (e)
    return ((1 << 24) + (e[0] << 16) + (e[1] << 8) + +e[2]).toString(16).slice(1);
}
function Ry(t, e, r) {
  if (!(!(e && e.length) || !(t >= 0 && t <= 1))) {
    r = r || [];
    var n = t * (e.length - 1), i = Math.floor(n), a = Math.ceil(n), o = e[i], s = e[a], u = n - i;
    return r[0] = At(Dr(o[0], s[0], u)), r[1] = At(Dr(o[1], s[1], u)), r[2] = At(Dr(o[2], s[2], u)), r[3] = da(Dr(o[3], s[3], u)), r;
  }
}
var vb = Ry;
function Oy(t, e, r) {
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
var db = Oy;
function us(t, e, r, n) {
  var i = st(t);
  if (t)
    return i = hb(i), e != null && (i[0] = fb(X(e) ? e(i[0]) : e)), r != null && (i[1] = Pr(X(r) ? r(i[1]) : r)), n != null && (i[2] = Pr(X(n) ? n(i[2]) : n)), Br(af(i), "rgba");
}
function pb(t, e) {
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
function gb() {
  return Br([
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255)
  ], "rgb");
}
var gv = new li(100);
function sf(t) {
  if (N(t)) {
    var e = gv.get(t);
    return e || (e = of(t, -0.1), gv.put(t, e)), e;
  } else if (Aa(t)) {
    var r = R({}, t);
    return r.colorStops = H(t.colorStops, function(n) {
      return {
        offset: n.offset,
        color: of(n.color, -0.1)
      };
    }), r;
  }
  return t;
}
const yb = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  fastLerp: Ry,
  fastMapToColor: vb,
  lerp: Oy,
  lift: of,
  liftColor: sf,
  lum: pa,
  mapToColor: db,
  modifyAlpha: pb,
  modifyHSL: us,
  parse: st,
  parseCssFloat: Pr,
  parseCssInt: ko,
  random: gb,
  stringify: Br,
  toHex: cb
}, Symbol.toStringTag, { value: "Module" }));
function mb(t) {
  return t.type === "linear";
}
function _b(t) {
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
var uf = Array.prototype.slice;
function or(t, e, r) {
  return (e - t) * r + t;
}
function Nu(t, e, r, n) {
  for (var i = e.length, a = 0; a < i; a++)
    t[a] = or(e[a], r[a], n);
  return t;
}
function Sb(t, e, r, n) {
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
function yv(t, e, r, n) {
  for (var i = e.length, a = i && e[0].length, o = 0; o < i; o++) {
    t[o] || (t[o] = []);
    for (var s = 0; s < a; s++)
      t[o][s] = e[o][s] + r[o][s] * n;
  }
  return t;
}
function bb(t, e) {
  for (var r = t.length, n = e.length, i = r > n ? e : t, a = Math.min(r, n), o = i[a - 1] || { color: [0, 0, 0, 0], offset: 0 }, s = a; s < Math.max(r, n); s++)
    i.push({
      offset: o.offset,
      color: o.color.slice()
    });
}
function wb(t, e, r) {
  var n = t, i = e;
  if (!(!n.push || !i.push)) {
    var a = n.length, o = i.length;
    if (a !== o) {
      var s = a > o;
      if (s)
        n.length = o;
      else
        for (var u = a; u < o; u++)
          n.push(r === 1 ? i[u] : uf.call(i[u]));
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
        r.push(uf.call(t[n]));
      return r;
    }
    return uf.call(t);
  }
  return t;
}
function No(t) {
  return t[0] = Math.floor(t[0]) || 0, t[1] = Math.floor(t[1]) || 0, t[2] = Math.floor(t[2]) || 0, t[3] = t[3] == null ? 1 : t[3], "rgba(" + t.join(",") + ")";
}
function Tb(t) {
  return Ze(t && t[0]) ? 2 : 1;
}
var $a = 0, Fo = 1, ky = 2, Zi = 3, lf = 4, ff = 5, mv = 6;
function _v(t) {
  return t === lf || t === ff;
}
function Za(t) {
  return t === Fo || t === ky;
}
var Pi = [0, 0, 0, 0], Cb = (function() {
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
    var i = this.keyframes, a = i.length, o = !1, s = mv, u = r;
    if (Ze(r)) {
      var l = Tb(r);
      s = l, (l === 1 && !ve(r[0]) || l === 2 && !ve(r[0][0])) && (o = !0);
    } else if (ve(r) && !ha(r))
      s = $a;
    else if (N(r))
      if (!isNaN(+r))
        s = $a;
      else {
        var f = st(r);
        f && (u = f, s = Zi);
      }
    else if (Aa(r)) {
      var h = R({}, u);
      h.colorStops = H(r.colorStops, function(c) {
        return {
          offset: c.offset,
          color: st(c.color)
        };
      }), mb(r) ? s = lf : _b(r) && (s = ff), u = h;
    }
    a === 0 ? this.valType = s : (s !== this.valType || s === mv) && (o = !0), this.discrete = this.discrete || o;
    var v = {
      time: e,
      value: u,
      rawValue: r,
      percent: 0
    };
    return n && (v.easing = n, v.easingFunc = X(n) ? n : ta[n] || Py(n)), i.push(v), v;
  }, t.prototype.prepare = function(e, r) {
    var n = this.keyframes;
    this._needsSort && n.sort(function(y, p) {
      return y.time - p.time;
    });
    for (var i = this.valType, a = n.length, o = n[a - 1], s = this.discrete, u = Za(i), l = _v(i), f = 0; f < a; f++) {
      var h = n[f], v = h.value, c = o.value;
      h.percent = h.time / e, s || (u && f !== a - 1 ? wb(v, c, i) : l && bb(v.colorStops, c.colorStops));
    }
    if (!s && i !== ff && r && this.needsAnimate() && r.needsAnimate() && i === r.valType && !r._finished) {
      this._additiveTrack = r;
      for (var d = n[0].value, f = 0; f < a; f++)
        i === $a ? n[f].additiveValue = n[f].value - d : i === Zi ? n[f].additiveValue = qa([], n[f].value, d, -1) : Za(i) && (n[f].additiveValue = i === Fo ? qa([], n[f].value, d, -1) : yv([], n[f].value, d, -1));
    }
  }, t.prototype.step = function(e, r) {
    if (!this._finished) {
      this._additiveTrack && this._additiveTrack._finished && (this._additiveTrack = null);
      var n = this._additiveTrack != null, i = n ? "additiveValue" : "value", a = this.valType, o = this.keyframes, s = o.length, u = this.propName, l = a === Zi, f, h = this._lastFr, v = Math.min, c, d;
      if (s === 1)
        c = d = o[0];
      else {
        if (r < 0)
          f = 0;
        else if (r < this._lastFrP) {
          var y = v(h + 1, s - 1);
          for (f = y; f >= 0 && !(o[f].percent <= r); f--)
            ;
          f = v(f, s - 2);
        } else {
          for (f = h; f < s && !(o[f].percent > r); f++)
            ;
          f = v(f - 1, s - 2);
        }
        d = o[f + 1], c = o[f];
      }
      if (c && d) {
        this._lastFr = f, this._lastFrP = r;
        var p = d.percent - c.percent, g = p === 0 ? 1 : v((r - c.percent) / p, 1);
        d.easingFunc && (g = d.easingFunc(g));
        var m = n ? this._additiveValue : l ? Pi : e[u];
        if ((Za(a) || l) && !m && (m = this._additiveValue = []), this.discrete)
          e[u] = g < 1 ? c.rawValue : d.rawValue;
        else if (Za(a))
          a === Fo ? Nu(m, c[i], d[i], g) : Sb(m, c[i], d[i], g);
        else if (_v(a)) {
          var _ = c[i], S = d[i], b = a === lf;
          e[u] = {
            type: b ? "linear" : "radial",
            x: or(_.x, S.x, g),
            y: or(_.y, S.y, g),
            colorStops: H(_.colorStops, function(C, M) {
              var x = S.colorStops[M];
              return {
                offset: or(C.offset, x.offset, g),
                color: No(Nu([], C.color, x.color, g))
              };
            }),
            global: S.global
          }, b ? (e[u].x2 = or(_.x2, S.x2, g), e[u].y2 = or(_.y2, S.y2, g)) : e[u].r = or(_.r, S.r, g);
        } else if (l)
          Nu(m, c[i], d[i], g), n || (e[u] = No(m));
        else {
          var w = or(c[i], d[i], g);
          n ? this._additiveValue = w : e[u] = w;
        }
        n && this._addToTarget(e);
      }
    }
  }, t.prototype._addToTarget = function(e) {
    var r = this.valType, n = this.propName, i = this._additiveValue;
    r === $a ? e[n] = e[n] + i : r === Zi ? (st(e[n], Pi), qa(Pi, Pi, i, 1), e[n] = No(Pi)) : r === Fo ? qa(e[n], e[n], i, 1) : r === ky && yv(e[n], e[n], i, 1);
  }, t;
})(), Sh = (function() {
  function t(e, r, n, i) {
    if (this._tracks = {}, this._trackKeys = [], this._maxTime = 0, this._started = 0, this._clip = null, this._target = e, this._loop = r, r && i) {
      Vs("Can' use additive animation on looped animation.");
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
        u = a[s] = new Cb(s);
        var l = void 0, f = this._getAdditiveTrack(s);
        if (f) {
          var h = f.keyframes, v = h[h.length - 1];
          l = v && v.value, f.valType === Zi && l && (l = No(l));
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
        var v = new ub({
          life: i,
          loop: this._loop,
          delay: this._delay || 0,
          onframe: function(c) {
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
              n[p].step(r._target, c);
            var g = r._onframeCbs;
            if (g)
              for (var p = 0; p < g.length; p++)
                g[p](r._target, c);
          },
          ondestroy: function() {
            r._doneCallback();
          }
        });
        this._clip = v, this.animation && this.animation.addClip(v), e && v.setEasing(e);
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
var xb = (function(t) {
  z(e, t);
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
      r._running && (is(n), !r._paused && r.update());
    }
    is(n);
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
    var i = new Sh(r, n.loop);
    return this.addAnimator(i), i;
  }, e;
})(Kt), Mb = 300, Fu = W.domSupported, zu = (function() {
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
})(), Sv = {
  mouse: ["mousemove", "mouseup"],
  pointer: ["pointermove", "pointerup"]
}, bv = !1;
function hf(t) {
  var e = t.pointerType;
  return e === "pen" || e === "touch";
}
function Db(t) {
  t.touching = !0, t.touchTimer != null && (clearTimeout(t.touchTimer), t.touchTimer = null), t.touchTimer = setTimeout(function() {
    t.touching = !1, t.touchTimer = null;
  }, 700);
}
function Hu(t) {
  t && (t.zrByTouch = !0);
}
function Ib(t, e) {
  return dt(t.dom, new Ab(t, e), !0);
}
function By(t, e) {
  for (var r = e, n = !1; r && r.nodeType !== 9 && !(n = r.domBelongToZr || r !== e && r === t.painterRoot); )
    r = r.parentNode;
  return n;
}
var Ab = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.stopPropagation = Ee, this.stopImmediatePropagation = Ee, this.preventDefault = Ee, this.type = r.type, this.target = this.currentTarget = e.dom, this.pointerType = r.pointerType, this.clientX = r.clientX, this.clientY = r.clientY;
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
    By(this, e) || (this.__pointerCapturing && (t.zrEventControl = "no_globalout"), this.trigger("mouseout", t));
  },
  wheel: function(t) {
    bv = !0, t = dt(this.dom, t), this.trigger("mousewheel", t);
  },
  mousewheel: function(t) {
    bv || (t = dt(this.dom, t), this.trigger("mousewheel", t));
  },
  touchstart: function(t) {
    t = dt(this.dom, t), Hu(t), this.__lastTouchMoment = /* @__PURE__ */ new Date(), this.handler.processGesture(t, "start"), Mt.mousemove.call(this, t), Mt.mousedown.call(this, t);
  },
  touchmove: function(t) {
    t = dt(this.dom, t), Hu(t), this.handler.processGesture(t, "change"), Mt.mousemove.call(this, t);
  },
  touchend: function(t) {
    t = dt(this.dom, t), Hu(t), this.handler.processGesture(t, "end"), Mt.mouseup.call(this, t), +/* @__PURE__ */ new Date() - +this.__lastTouchMoment < Mb && Mt.click.call(this, t);
  },
  pointerdown: function(t) {
    Mt.mousedown.call(this, t);
  },
  pointermove: function(t) {
    hf(t) || Mt.mousemove.call(this, t);
  },
  pointerup: function(t) {
    Mt.mouseup.call(this, t);
  },
  pointerout: function(t) {
    hf(t) || Mt.mouseout.call(this, t);
  }
};
T(["click", "dblclick", "contextmenu"], function(t) {
  Mt[t] = function(e) {
    e = dt(this.dom, e), this.trigger(t, e);
  };
});
var cf = {
  pointermove: function(t) {
    hf(t) || cf.mousemove.call(this, t);
  },
  pointerup: function(t) {
    cf.mouseup.call(this, t);
  },
  mousemove: function(t) {
    this.trigger("mousemove", t);
  },
  mouseup: function(t) {
    var e = this.__pointerCapturing;
    this.__togglePointerCapture(!1), this.trigger("mouseup", t), e && (t.zrEventControl = "only_globalout", this.trigger("mouseout", t));
  }
};
function Pb(t, e) {
  var r = e.domHandlers;
  W.pointerEventsSupported ? T(zu.pointer, function(n) {
    zo(e, n, function(i) {
      r[n].call(t, i);
    });
  }) : (W.touchEventsSupported && T(zu.touch, function(n) {
    zo(e, n, function(i) {
      r[n].call(t, i), Db(e);
    });
  }), T(zu.mouse, function(n) {
    zo(e, n, function(i) {
      i = _h(i), e.touching || r[n].call(t, i);
    });
  }));
}
function Lb(t, e) {
  W.pointerEventsSupported ? T(Sv.pointer, r) : W.touchEventsSupported || T(Sv.mouse, r);
  function r(n) {
    function i(a) {
      a = _h(a), By(t, a.target) || (a = Ib(t, a), e.domHandlers[n].call(t, a));
    }
    zo(e, n, i, { capture: !0 });
  }
}
function zo(t, e, r, n) {
  t.mounted[e] = r, t.listenerOpts[e] = n, N1(t.domTarget, e, r, n);
}
function Vu(t) {
  var e = t.mounted;
  for (var r in e)
    e.hasOwnProperty(r) && F1(t.domTarget, r, e[r], t.listenerOpts[r]);
  t.mounted = {};
}
var wv = /* @__PURE__ */ (function() {
  function t(e, r) {
    this.mounted = {}, this.listenerOpts = {}, this.touching = !1, this.domTarget = e, this.domHandlers = r;
  }
  return t;
})(), Eb = (function(t) {
  z(e, t);
  function e(r, n) {
    var i = t.call(this) || this;
    return i.__pointerCapturing = !1, i.dom = r, i.painterRoot = n, i._localHandlerScope = new wv(r, Mt), Fu && (i._globalHandlerScope = new wv(document, cf)), Pb(i, i._localHandlerScope), i;
  }
  return e.prototype.dispose = function() {
    Vu(this._localHandlerScope), Fu && Vu(this._globalHandlerScope);
  }, e.prototype.setCursor = function(r) {
    this.dom.style && (this.dom.style.cursor = r || "default");
  }, e.prototype.__togglePointerCapture = function(r) {
    if (this.__mayPointerCapture = null, Fu && +this.__pointerCapturing ^ +r) {
      this.__pointerCapturing = r;
      var n = this._globalHandlerScope;
      r ? Lb(this, n) : Vu(n);
    }
  }, e;
})(Kt), Ny = 1;
W.hasGlobalWindow && (Ny = Math.max(window.devicePixelRatio || window.screen && window.screen.deviceXDPI / window.screen.logicalXDPI || 1, 1));
var ls = Ny, vf = 0.4, df = "#333", pf = "#ccc", Rb = "#eee", Tv = Mn, Cv = 5e-5;
function Yr(t) {
  return t > Cv || t < -Cv;
}
var Wr = [], On = [], Gu = wt(), Uu = Math.abs, Ea = (function() {
  function t() {
  }
  return t.prototype.getLocalTransform = function(e) {
    return Ob(this, e);
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
      n && (Tv(n), this.invTransform = null);
      return;
    }
    n = n || wt(), r ? this.getLocalTransform(n) : Tv(n), e && (r ? mn(n, e, n) : La(n, e)), this.transform = n, this._resolveGlobalScaleRatio(n), this.invTransform = this.invTransform || wt(), yi(this.invTransform, n);
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
      e && e.transform && (e.invTransform = e.invTransform || wt(), mn(On, e.invTransform, r), r = On);
      var n = this.originX, i = this.originY;
      (n || i) && (Gu[4] = n, Gu[5] = i, mn(On, r, Gu), On[4] -= n, On[5] -= i, r = On), this.setLocalTransform(r);
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
    return e && Uu(e[0] - 1) > 1e-10 && Uu(e[3] - 1) > 1e-10 ? Math.sqrt(Uu(e[0] * e[3] - e[2] * e[1])) : 1;
  }, t.prototype.copyTransform = function(e) {
    fs(this, e);
  }, t.getLocalTransform = function(e, r) {
    r = r || [];
    var n = e.originX || 0, i = e.originY || 0, a = e.scaleX, o = e.scaleY, s = e.anchorX, u = e.anchorY, l = e.rotation || 0, f = e.x, h = e.y, v = e.skewX ? Math.tan(e.skewX) : 0, c = e.skewY ? Math.tan(-e.skewY) : 0;
    if (n || i || s || u) {
      var d = n + s, y = i + u;
      r[4] = -d * a - v * y * o, r[5] = -y * o - c * d * a;
    } else
      r[4] = r[5] = 0;
    return r[0] = a, r[3] = o, r[1] = c * a, r[2] = v * o, l && Ys(r, r, l), r[4] += n + f, r[5] += i + h, r;
  }, t.initDefaultProps = (function() {
    var e = t.prototype;
    e.scaleX = e.scaleY = e.globalScaleRatio = 1, e.x = e.y = e.originX = e.originY = e.skewX = e.skewY = e.rotation = e.anchorX = e.anchorY = 0;
  })(), t;
})(), Ob = Ea.getLocalTransform, Ws = [
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
function fs(t, e) {
  return ay(t, e, Ws);
}
function Gt(t) {
  Ka || (Ka = new li(100)), t = t || Rr;
  var e = Ka.get(t);
  return e || (e = {
    font: t,
    strWidthCache: new li(500),
    asciiWidthMap: null,
    asciiWidthMapTried: !1,
    stWideCharWidth: Le.measureText("国", t).width,
    asciiCharWidth: Le.measureText("a", t).width
  }, Ka.put(t, e)), e;
}
var Ka;
function kb(t) {
  if (!(Yu >= xv)) {
    t = t || Rr;
    for (var e = [], r = +/* @__PURE__ */ new Date(), n = 0; n <= 127; n++)
      e[n] = Le.measureText(String.fromCharCode(n), t).width;
    var i = +/* @__PURE__ */ new Date() - r;
    return i > 16 ? Yu = xv : i > 2 && Yu++, e;
  }
}
var Yu = 0, xv = 5;
function Fy(t, e) {
  return t.asciiWidthMapTried || (t.asciiWidthMap = kb(t.font), t.asciiWidthMapTried = !0), 0 <= e && e <= 127 ? t.asciiWidthMap != null ? t.asciiWidthMap[e] : t.asciiCharWidth : t.stWideCharWidth;
}
function Ut(t, e) {
  var r = t.strWidthCache, n = r.get(e);
  return n == null && (n = Le.measureText(e, t.font).width, r.put(e, n)), n;
}
function Mv(t, e, r, n) {
  var i = Ut(Gt(e), t), a = Xs(e), o = fi(0, i, r), s = _n(0, a, n), u = new Y(o, s, i, a);
  return u;
}
function zy(t, e, r, n) {
  var i = ((t || "") + "").split(`
`), a = i.length;
  if (a === 1)
    return Mv(i[0], e, r, n);
  for (var o = new Y(0, 0, 0, 0), s = 0; s < i.length; s++) {
    var u = Mv(i[s], e, r, n);
    s === 0 ? o.copy(u) : o.union(u);
  }
  return o;
}
function fi(t, e, r, n) {
  return r === "right" ? n ? t += e : t -= e : r === "center" && (n ? t += e / 2 : t -= e / 2), t;
}
function _n(t, e, r, n) {
  return r === "middle" ? n ? t += e / 2 : t -= e / 2 : r === "bottom" && (n ? t += e : t -= e), t;
}
function Xs(t) {
  return Gt(t).stWideCharWidth;
}
function hi(t, e) {
  return typeof t == "string" ? t.lastIndexOf("%") >= 0 ? parseFloat(t) / 100 * e : parseFloat(t) : t;
}
function Hy(t, e, r) {
  var n = e.position || "inside", i = e.distance != null ? e.distance : 5, a = r.height, o = r.width, s = a / 2, u = r.x, l = r.y, f = "left", h = "top";
  if (n instanceof Array)
    u += hi(n[0], r.width), l += hi(n[1], r.height), f = null, h = null;
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
var Wu = "__zr_normal__", Xu = Ws.concat(["ignore"]), Bb = Xt(Ws, function(t, e) {
  return t[e] = !0, t;
}, { ignore: !1 }), kn = {}, Nb = new Y(0, 0, 0, 0), ja = [], Ho = 0, qs = 1, $s = (function() {
  function t(e) {
    this.id = dh(), this.animators = [], this.currentStates = [], this.states = {}, this._init(e);
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
      var f = n.position != null, h = n.autoOverflowArea, v = void 0;
      if ((h || f) && (v = Nb, n.layoutRect ? v.copy(n.layoutRect) : v.copy(this.getBoundingRect()), i || v.applyTransform(this.transform)), f) {
        this.calculateTextPosition ? this.calculateTextPosition(kn, n, v) : Hy(kn, n, v), a.x = kn.x, a.y = kn.y, o = kn.align, s = kn.verticalAlign;
        var c = n.origin;
        if (c && n.rotation != null) {
          var d = void 0, y = void 0;
          c === "center" ? (d = v.width * 0.5, y = v.height * 0.5) : (d = hi(c[0], v.width), y = hi(c[1], v.height)), l = !0, a.originX = -a.x + d + (i ? 0 : v.x), a.originY = -a.y + y + (i ? 0 : v.y);
        }
      }
      n.rotation != null && (a.rotation = n.rotation);
      var p = n.offset;
      p && (a.x += p[0], a.y += p[1], l || (a.originX = -p[0], a.originY = -p[1]));
      var g = this._innerTextDefaultStyle || (this._innerTextDefaultStyle = {});
      if (h) {
        var m = g.overflowRect = g.overflowRect || new Y(0, 0, 0, 0);
        a.getLocalTransform(ja), yi(ja, ja), Y.copy(m, v), m.applyTransform(ja);
      } else
        g.overflowRect = null;
      var _ = n.inside == null ? typeof n.position == "string" && n.position.indexOf("inside") >= 0 : n.inside, S = void 0, b = void 0, w = void 0;
      _ && this.canBeInsideText() ? (S = n.insideFill, b = n.insideStroke, (S == null || S === "auto") && (S = this.getInsideTextFill()), (b == null || b === "auto") && (b = this.getInsideTextStroke(S), w = !0)) : (S = n.outsideFill, b = n.outsideStroke, (S == null || S === "auto") && (S = this.getOutsideFill()), (b == null || b === "auto") && (b = this.getOutsideStroke(S), w = !0)), S = S || "#000", (S !== g.fill || b !== g.stroke || w !== g.autoStroke || o !== g.align || s !== g.verticalAlign) && (u = !0, g.fill = S, g.stroke = b, g.autoStroke = w, g.align = o, g.verticalAlign = s, r.setDefaultTextStyle(g)), r.__dirty |= Je, u && r.dirtyStyle(!0);
    }
  }, t.prototype.canBeInsideText = function() {
    return !0;
  }, t.prototype.getInsideTextFill = function() {
    return "#fff";
  }, t.prototype.getInsideTextStroke = function(e) {
    return "#000";
  }, t.prototype.getOutsideFill = function() {
    return this.__zr && this.__zr.isDarkMode() ? pf : df;
  }, t.prototype.getOutsideStroke = function(e) {
    var r = this.__zr && this.__zr.getBackgroundColor(), n = typeof r == "string" && st(r);
    n || (n = [255, 255, 255, 1]);
    for (var i = n[3], a = this.__zr.isDarkMode(), o = 0; o < 3; o++)
      n[o] = n[o] * i + (a ? 0 : 255) * (1 - i);
    return n[3] = 1, Br(n, "rgba");
  }, t.prototype.traverse = function(e, r) {
  }, t.prototype.attrKV = function(e, r) {
    e === "textConfig" ? this.setTextConfig(r) : e === "textContent" ? this.setTextContent(r) : e === "clipPath" ? this.setClipPath(r) : e === "extra" ? (this.extra = this.extra || {}, R(this.extra, r)) : this[e] = r;
  }, t.prototype.hide = function() {
    this.ignore = !0, this.markRedraw();
  }, t.prototype.show = function() {
    this.ignore = !1, this.markRedraw();
  }, t.prototype.attr = function(e, r) {
    if (typeof e == "string")
      this.attrKV(e, r);
    else if (F(e))
      for (var n = e, i = me(n), a = 0; a < i.length; a++) {
        var o = i[a];
        this.attrKV(o, e[o]);
      }
    return this.markRedraw(), this;
  }, t.prototype.saveCurrentToNormalState = function(e) {
    this._innerSaveToNormal(e);
    for (var r = this._normalState, n = 0; n < this.animators.length; n++) {
      var i = this.animators[n], a = i.__fromStateTransition;
      if (!(i.getLoop() || a && a !== Wu)) {
        var o = i.targetName, s = o ? r[o] : r;
        i.saveTo(s);
      }
    }
  }, t.prototype._innerSaveToNormal = function(e) {
    var r = this._normalState;
    r || (r = this._normalState = {}), e.textConfig && !r.textConfig && (r.textConfig = this.textConfig), this._savePrimaryToNormal(e, r, Xu);
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
    this.useState(Wu, !1, e);
  }, t.prototype.useState = function(e, r, n, i) {
    var a = e === Wu, o = this.hasState();
    if (!(!o && a)) {
      var s = this.currentStates, u = this.stateTransition;
      if (!(oe(s, e) >= 0 && (r || s.length === 1))) {
        var l;
        if (this.stateProxy && !a && (l = this.stateProxy(e)), l || (l = this.states && this.states[e]), !l && !a) {
          Vs("State " + e + " not exists.");
          return;
        }
        a || this.saveCurrentToNormalState(l);
        var f = this._textContent, h = Dv(this, f, l, i);
        h && !this.__inHover && (this.__inHover = h), this._applyStateObj(e, l, this._normalState, r, Av(this, n, u), u);
        var v = this._textGuide;
        return f && f.useState(e, r, n, !!h), v && v.useState(e, r, n, !!h), a ? (this.currentStates = [], this._normalState = {}) : r ? this.currentStates.push(e) : this.currentStates = [e], this._updateAnimationTargets(), this.markRedraw(), !h && this.__inHover && (this.__inHover = Ho, this.__dirty &= ~Je), l;
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
      var h = i[o - 1], v = this._textContent, c = Dv(this, v, h, n);
      c && !this.__inHover && (this.__inHover = c);
      var d = this._mergeStates(i), y = this.stateTransition;
      this.saveCurrentToNormalState(d), this._applyStateObj(e.join(","), d, this._normalState, !1, Av(this, r, y), y);
      var p = this._textGuide;
      v && v.useStates(e, r, !!c), p && p.useStates(e, r, !!c), this._updateAnimationTargets(), this.currentStates = e.slice(), this.markRedraw(), !c && this.__inHover && (this.__inHover = Ho, this.__dirty &= ~Je);
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
      R(r, a), a.textConfig && (n = n || {}, R(n, a.textConfig));
    }
    return n && (r.textConfig = n), r;
  }, t.prototype._applyStateObj = function(e, r, n, i, a, o) {
    if (this.__inHover !== qs) {
      var s = !(r && i);
      r && r.textConfig ? (this.textConfig = R({}, i ? this.textConfig : n.textConfig), R(this.textConfig, r.textConfig)) : s && n.textConfig && (this.textConfig = n.textConfig);
      for (var u = {}, l = !1, f = 0; f < Xu.length; f++) {
        var h = Xu[f], v = a && Bb[h];
        r && r[h] != null ? v ? (l = !0, u[h] = r[h]) : this[h] = r[h] : s && n[h] != null && (v ? (l = !0, u[h] = n[h]) : this[h] = n[h]);
      }
      if (!a)
        for (var f = 0; f < this.animators.length; f++) {
          var c = this.animators[f], d = c.targetName;
          c.getLoop() || c.__changeFinalValue(d ? (r || n)[d] : r || n);
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
    r !== e && (r && r !== e && this.removeTextContent(), e.innerTransformable = new Ea(), this._attachComponent(e), this._textContent = e, this.markRedraw());
  }, t.prototype.setTextConfig = function(e) {
    this.textConfig || (this.textConfig = {}), R(this.textConfig, e), this.markRedraw();
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
    var i = e ? this[e] : this, a = new Sh(i, r, n);
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
    qu(this, e, r, n);
  }, t.prototype.animateFrom = function(e, r, n) {
    qu(this, e, r, n, !0);
  }, t.prototype._transitionState = function(e, r, n, i) {
    for (var a = qu(this, r, n, i), o = 0; o < a.length; o++)
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
Lt($s, Kt);
Lt($s, Ea);
function qu(t, e, r, n, i) {
  r = r || {};
  var a = [];
  Vy(t, "", t, e, r, n, a, i);
  var o = a.length, s = !1, u = r.done, l = r.aborted, f = function() {
    s = !0, o--, o <= 0 && (s ? u && u() : l && l());
  }, h = function() {
    o--, o <= 0 && (s ? u && u() : l && l());
  };
  o || u && u(), a.length > 0 && r.during && a[0].during(function(d, y) {
    r.during(y);
  });
  for (var v = 0; v < a.length; v++) {
    var c = a[v];
    f && c.done(f), h && c.aborted(h), r.force && c.duration(r.duration), c.start(r.easing);
  }
  return a;
}
function $u(t, e, r) {
  for (var n = 0; n < r; n++)
    t[n] = e[n];
}
function Fb(t) {
  return Ze(t[0]);
}
function zb(t, e, r) {
  if (Ze(e[r]))
    if (Ze(t[r]) || (t[r] = []), Ke(e[r])) {
      var n = e[r].length;
      t[r].length !== n && (t[r] = new e[r].constructor(n), $u(t[r], e[r], n));
    } else {
      var i = e[r], a = t[r], o = i.length;
      if (Fb(i))
        for (var s = i[0].length, u = 0; u < o; u++)
          a[u] ? $u(a[u], i[u], s) : a[u] = Array.prototype.slice.call(i[u]);
      else
        $u(a, i, o);
      a.length = i.length;
    }
  else
    t[r] = e[r];
}
function Hb(t, e) {
  return t === e || Ze(t) && Ze(e) && Vb(t, e);
}
function Vb(t, e) {
  var r = t.length;
  if (r !== e.length)
    return !1;
  for (var n = 0; n < r; n++)
    if (t[n] !== e[n])
      return !1;
  return !0;
}
function Vy(t, e, r, n, i, a, o, s) {
  for (var u = me(n), l = i.duration, f = i.delay, h = i.additive, v = i.setToFinal, c = !F(a), d = t.animators, y = [], p = 0; p < u.length; p++) {
    var g = u[p], m = n[g];
    if (m != null && r[g] != null && (c || a[g]))
      if (F(m) && !Ze(m) && !Aa(m)) {
        if (e) {
          s || (r[g] = m, t.updateDuringAnimation(e));
          continue;
        }
        Vy(t, g, r[g], m, i, a && a[g], o, s);
      } else
        y.push(g);
    else s || (r[g] = m, t.updateDuringAnimation(e), y.push(g));
  }
  var _ = y.length;
  if (!h && _)
    for (var S = 0; S < d.length; S++) {
      var b = d[S];
      if (b.targetName === e) {
        var w = b.stopTracks(y);
        if (w) {
          var C = oe(d, b);
          d.splice(C, 1);
        }
      }
    }
  if (i.force || (y = xe(y, function(I) {
    return !Hb(n[I], r[I]);
  }), _ = y.length), _ > 0 || i.force && !o.length) {
    var M = void 0, x = void 0, D = void 0;
    if (s) {
      x = {}, v && (M = {});
      for (var S = 0; S < _; S++) {
        var g = y[S];
        x[g] = r[g], v ? M[g] = n[g] : r[g] = n[g];
      }
    } else if (v) {
      D = {};
      for (var S = 0; S < _; S++) {
        var g = y[S];
        D[g] = Bo(r[g]), zb(r, n, g);
      }
    }
    var b = new Sh(r, !1, !1, h ? xe(d, function(L) {
      return L.targetName === e;
    }) : null);
    b.targetName = e, i.scope && (b.scope = i.scope), v && M && b.whenWithKeys(0, M, y), D && b.whenWithKeys(0, D, y), b.whenWithKeys(l ?? 500, s ? x : n, y).delay(f || 0), t.addAnimator(b, e), o.push(b);
  }
}
function Dv(t, e, r, n) {
  return !(r && r.hoverLayer || n) || Iv(t) || e && Iv(e) ? Ho : qs;
}
function Iv(t) {
  return t.type === "text" || t.type === "tspan";
}
function Av(t, e, r) {
  return !e && !t.__inHover && r && r.duration > 0;
}
var ut = (function(t) {
  z(e, t);
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
})($s);
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
function Gb(t) {
  delete cn[t];
}
function Ub(t) {
  if (!t)
    return !1;
  if (typeof t == "string")
    return pa(t, 1) < vf;
  if (t.colorStops) {
    for (var e = t.colorStops, r = 0, n = e.length, i = 0; i < n; i++)
      r += pa(e[i].color, 1);
    return r /= n, r < vf;
  }
  return !1;
}
var Yb = (function() {
  function t(e, r, n) {
    var i = this;
    this._sleepAfterStill = 10, this._stillFrameAccum = 0, this._needsRefresh = !0, this._needsRefreshHover = !1, this._darkMode = !1, n = n || {}, this.dom = r, this.id = e;
    var a = new tb(), o = n.renderer || "canvas";
    Vo[o] || (o = me(Vo)[0]), n.useDirtyRect = n.useDirtyRect == null ? !1 : n.useDirtyRect;
    var s = new Vo[o](r, a, n, e), u = n.ssr || s.ssrOnly;
    this.storage = a, this.painter = s;
    var l = !W.node && !W.worker && !u ? new Eb(s.getViewportRoot(), s.root) : null, f = n.useCoarsePointer, h = f == null || f === "auto" ? W.touchEventsSupported : !!f, v = 44, c;
    h && (c = G(n.pointerSize, v)), this.handler = new wy(a, s, l, s.root, c), this.animation = new xb({
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
    this._disposed || (this.painter.setBackgroundColor && this.painter.setBackgroundColor(e), this.refresh(), this._backgroundColor = e, this._darkMode = Ub(e));
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
    this._disposed || (this.animation.stop(), this.clear(), this.storage.dispose(), this.painter.dispose(), this.handler.dispose(), this.animation = this.storage = this.painter = this.handler = null, this._disposed = !0, Gb(this.id));
  }, t;
})();
function gf(t, e) {
  var r = new Yb(dh(), t, e);
  return cn[r.id] = r, r;
}
function Wb(t) {
  t.dispose();
}
function Xb() {
  for (var t in cn)
    cn.hasOwnProperty(t) && cn[t].dispose();
  cn = {};
}
function qb(t) {
  return cn[t];
}
function Gy(t, e) {
  Vo[t] = e;
}
var yf;
function $b(t) {
  if (typeof yf == "function")
    return yf(t);
}
function Uy(t) {
  yf = t;
}
var Zb = "6.1.0";
const Kb = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  dispose: Wb,
  disposeAll: Xb,
  getElementSSRData: $b,
  getInstance: qb,
  init: gf,
  registerPainter: Gy,
  registerSSRDataGetter: Uy,
  version: Zb
}, Symbol.toStringTag, { value: "Module" }));
var Pv = 1e-4, hs = 20;
function jb(t) {
  return t.replace(/^\s+|\s+$/g, "");
}
var et = Math.min, ge = Math.max, Fe = Math.abs, hr = Math.round, Dn = Math.floor, Zs = Math.ceil, In = Math.pow, Tn = Math.log, ga = Math.LN10, Qb = Math.PI, Jb = Math.random;
function mf(t, e, r, n) {
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
var Dt = ew;
function ew(t, e, r) {
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
  return tw(t, e, r);
}
function tw(t, e, r) {
  return N(t) ? rw(t) ? parseFloat(t) / 100 * e + (r || 0) : parseFloat(t) : t == null ? NaN : +t;
}
function rw(t) {
  return !!jb(t).match(/%$/);
}
function Ie(t, e, r) {
  return isNaN(e) ? r ? "" + t : +t : (e = et(ge(0, e), hs), t = (+t).toFixed(e), r ? t : +t);
}
function nw(t, e, r) {
  return e == null && (e = 10), Ie(t, e, r);
}
function bh(t) {
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
  return Yy(t);
}
function Yy(t) {
  var e = t.toString().toLowerCase(), r = e.indexOf("e"), n = r > 0 ? +e.slice(r + 1) : 0, i = r > 0 ? r : e.length, a = e.indexOf("."), o = a < 0 ? 0 : i - 1 - a;
  return ge(0, o - n);
}
function iw(t, e) {
  var r = Dn(Tn(t[1] - t[0]) / ga), n = hr(Tn(Fe(e[1] - e[0])) / ga), i = et(ge(-r + n, 0), hs);
  return isFinite(i) ? i : hs;
}
function zE(t, e, r) {
  var n = Fe(t[1] - t[0]);
  if (!isFinite(n) || n === 0)
    return NaN;
  var i = Tn(2 * Fe(r || 1) * Fe(n)) / ga, a = Tn(Fe(e)) / ga, o = ge(0, Zs(-i + a));
  return isFinite(o) || (o = NaN), o;
}
function aw(t, e, r) {
  if (!t[e])
    return 0;
  var n = ow(t, r);
  return n[e] || 0;
}
function ow(t, e) {
  var r = Xt(t, function(c, d) {
    return c + (isNaN(d) ? 0 : d);
  }, 0);
  if (r === 0)
    return [];
  for (var n = In(10, e), i = H(t, function(c) {
    return (isNaN(c) ? 0 : c) / r * n * 100;
  }), a = n * 100, o = H(i, function(c) {
    return Dn(c);
  }), s = Xt(o, function(c, d) {
    return c + d;
  }, 0), u = H(i, function(c, d) {
    return c - o[d];
  }); s < a; ) {
    for (var l = Number.NEGATIVE_INFINITY, f = null, h = 0, v = u.length; h < v; ++h)
      u[h] > l && (l = u[h], f = h);
    ++o[f], u[f] = 0, ++s;
  }
  return H(o, function(c) {
    return c / n;
  });
}
function sw(t, e) {
  var r = ge(lr(t), lr(e)), n = t + e;
  return r > hs ? n : Ie(n, r);
}
var uw = In(2, 53) - 1;
function wh(t) {
  var e = Qb * 2;
  return (t % e + e) % e;
}
function ya(t) {
  return t > -Pv && t < Pv;
}
var lw = /^(?:(\d{4})(?:[-\/](\d{1,2})(?:[-\/](\d{1,2})(?:[T ](\d{1,2})(?::(\d{1,2})(?::(\d{1,2})(?:[.,](\d+))?)?)?(Z|[\+\-]\d\d:?\d\d)?)?)?)?)?$/;
function pr(t) {
  if (t instanceof Date)
    return t;
  if (N(t)) {
    var e = lw.exec(t);
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
function Wy(t) {
  return In(10, Ks(t));
}
function Ks(t) {
  if (t === 0)
    return 0;
  var e = Dn(Tn(t) / ga);
  return t / In(10, e) >= 10 && e++, e;
}
var fw = 2;
function Th(t, e) {
  var r = Ks(t), n = In(10, r), i = t / n, a;
  return e === fw ? a = 1 : e ? i < 1.5 ? a = 1 : i < 2.5 ? a = 2 : i < 4 ? a = 3 : i < 7 ? a = 5 : a = 10 : i < 1 ? a = 1 : i < 2 ? a = 2 : i < 3 ? a = 3 : i < 5 ? a = 5 : a = 10, t = a * n, Ie(t, -r);
}
function hw(t, e) {
  var r = (t.length - 1) * e + 1, n = Dn(r), i = +t[n - 1], a = r - n;
  return a ? i + a * (t[n] - i) : i;
}
function cw(t) {
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
function Xy(t) {
  return !isNaN(cr(t));
}
function Ch() {
  return hr(Jb() * 9);
}
function qy(t, e) {
  return e === 0 ? t : qy(e, t % e);
}
function Lv(t, e) {
  return t == null ? e : e == null ? t : t * e / qy(t, e);
}
function vr(t) {
  return t != null && isFinite(t);
}
var vw = "[ECharts] ", dw = typeof console < "u" && console.warn && console.log;
function pw(t, e, r) {
  dw && console[t](vw + e);
}
function $y(t, e) {
  pw("error", t);
}
function Ye(t) {
  throw new Error(t);
}
function Ev(t, e, r) {
  return (e - t) * r + t;
}
var Zy = "series\0", gw = "\0_ec_\0";
function $e(t) {
  return t instanceof Array ? t : t == null ? [] : [t];
}
function Rv(t, e, r) {
  if (t) {
    t[e] = t[e] || {}, t.emphasis = t.emphasis || {}, t.emphasis[e] = t.emphasis[e] || {};
    for (var n = 0, i = r.length; n < i; n++) {
      var a = r[n];
      !t.emphasis[e].hasOwnProperty(a) && t[e].hasOwnProperty(a) && (t.emphasis[e][a] = t[e][a]);
    }
  }
}
var Ov = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "rich", "tag", "color", "textBorderColor", "textBorderWidth", "width", "height", "lineHeight", "align", "verticalAlign", "baseline", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY", "backgroundColor", "borderColor", "borderWidth", "borderRadius", "padding"];
function Ra(t) {
  return F(t) && !B(t) && !(t instanceof Date) ? t.value : t;
}
function yw(t) {
  return F(t) && !(t instanceof Array);
}
function mw(t, e, r) {
  var n = r === "normalMerge", i = r === "replaceMerge", a = r === "replaceAll";
  t = t || [], e = (e || []).slice();
  var o = U();
  T(e, function(u, l) {
    if (!F(u)) {
      e[l] = null;
      return;
    }
  });
  var s = _w(t, o, r);
  return (n || i) && Sw(s, t, o, e), n && bw(s, e), n || i ? ww(s, e, i) : a && Tw(s, e), Cw(s), s;
}
function _w(t, e, r) {
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
function Sw(t, e, r, n) {
  T(n, function(i, a) {
    if (!(!i || i.id == null)) {
      var o = ra(i.id), s = r.get(o);
      if (s != null) {
        var u = t[s];
        qt(!u.newOption, 'Duplicated option on id "' + o + '".'), u.newOption = i, u.existing = e[s], n[a] = null;
      }
    }
  });
}
function bw(t, e) {
  T(e, function(r, n) {
    if (!(!r || r.name == null))
      for (var i = 0; i < t.length; i++) {
        var a = t[i].existing;
        if (!t[i].newOption && a && (a.id == null || r.id == null) && !ma(r) && !ma(a) && Ky("name", a, r)) {
          t[i].newOption = r, e[n] = null;
          return;
        }
      }
  });
}
function ww(t, e, r) {
  T(e, function(n) {
    if (n) {
      for (
        var i, a = 0;
        // Be `!resultItem` only when `nextIdx >= result.length`.
        (i = t[a]) && (i.newOption || ma(i.existing) || // In mode "replaceMerge", here no not-mapped-non-internal-existing.
        i.existing && n.id != null && !Ky("id", n, i.existing));
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
function Tw(t, e) {
  T(e, function(r) {
    t.push({
      newOption: r,
      brandNew: !0,
      existing: null,
      keyInfo: null
    });
  });
}
function Cw(t) {
  var e = U();
  T(t, function(r) {
    var n = r.existing;
    n && e.set(n.id, r);
  }), T(t, function(r) {
    var n = r.newOption;
    qt(!n || n.id == null || !e.get(n.id) || e.get(n.id) === r, "id duplicates: " + (n && n.id)), n && n.id != null && e.set(n.id, r), !r.keyInfo && (r.keyInfo = {});
  }), T(t, function(r, n) {
    var i = r.existing, a = r.newOption, o = r.keyInfo;
    if (F(a)) {
      if (o.name = a.name != null ? ra(a.name) : i ? i.name : Zy + n, i)
        o.id = ra(i.id);
      else if (a.id != null)
        o.id = ra(a.id);
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
function Ky(t, e, r) {
  var n = Yt(e[t], null), i = Yt(r[t], null);
  return n != null && i != null && n === i;
}
function ra(t) {
  return Yt(t, "");
}
function Yt(t, e) {
  return t == null ? e : N(t) ? t : ve(t) || es(t) ? t + "" : e;
}
function xh(t) {
  var e = t.name;
  return !!(e && e.indexOf(Zy));
}
function ma(t) {
  return t && t.id != null && ra(t.id).indexOf(gw) === 0;
}
function xw(t, e, r) {
  T(t, function(n) {
    var i = n.newOption;
    F(i) && (n.keyInfo.mainType = e, n.keyInfo.subType = Mw(e, i, n.existing, r));
  });
}
function Mw(t, e, r, n) {
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
  var t = "__ec_inner_" + Dw++;
  return function(e) {
    return e[t] || (e[t] = {});
  };
}
var Dw = Ch();
function Zu(t, e, r) {
  var n = Mh(e, r), i = n.mainTypeSpecified, a = n.queryOptionMap, o = n.others, s = o, u = r ? r.defaultMainType : null;
  return !i && u && a.set(u, {}), a.each(function(l, f) {
    var h = ka(t, f, l, {
      useDefault: u === f,
      enableAll: r && r.enableAll != null ? r.enableAll : !0,
      enableNone: r && r.enableNone != null ? r.enableNone : !0
    });
    s[f + "Models"] = h.models, s[f + "Model"] = h.models[0];
  }), s;
}
function Mh(t, e) {
  var r;
  if (N(t)) {
    var n = {};
    n[t + "Index"] = 0, r = n;
  } else
    r = t;
  var i = U(), a = {}, o = !1;
  return T(r, function(s, u) {
    if (u === "dataIndex" || u === "dataIndexInside") {
      a[u] = s;
      return;
    }
    var l = u.match(/^(\w+)(Index|Id|Name)$/) || [], f = l[1], h = (l[2] || "").toLowerCase();
    if (!(!f || !h || e && e.includeMainTypes && oe(e.includeMainTypes, f) < 0)) {
      o = o || !!f;
      var v = i.get(f) || i.set(f, {});
      v[h] = s;
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
function Iw(t, e, r) {
  var n = {};
  n[e + "Id"] = t[e + "Id"], n[e + "Index"] = t[e + "Index"], n[e + "Name"] = t[e + "Name"];
  var i = {
    mainType: e,
    query: n
  };
  return r && (i.subType = r), i;
}
function jy(t, e, r) {
  t.setAttribute ? t.setAttribute(e, r) : t[e] = r;
}
function Aw(t, e) {
  return t.getAttribute ? t.getAttribute(e) : t[e];
}
function Pw(t) {
  return t === "auto" ? W.domSupported ? "html" : "richText" : t || "html";
}
function Lw(t, e, r, n, i) {
  var a = e == null || e === "auto";
  if (n == null)
    return n;
  if (ve(n)) {
    var o = Ev(r || 0, n, i);
    return Ie(o, a ? Math.max(lr(r || 0), lr(n)) : e);
  } else {
    if (N(n))
      return i < 1 ? r : n;
    for (var s = [], u = r, l = n, f = Math.max(u ? u.length : 0, l.length), h = 0; h < f; ++h) {
      var v = t.getDimensionInfo(h);
      if (v && v.type === "ordinal")
        s[h] = (i < 1 && u ? u : l)[h];
      else {
        var c = u && u[h] ? u[h] : 0, d = l[h], o = Ev(c, d, i);
        s[h] = Ie(o, a ? Math.max(lr(c), lr(d)) : e);
      }
    }
    return s;
  }
}
function zt() {
  return [1 / 0, -1 / 0];
}
function Ew(t, e) {
  dr(e) && (e < t[0] && (t[0] = e), e > t[1] && (t[1] = e));
}
function Qy(t, e) {
  dr(e) && e < t[0] && (t[0] = e);
}
function Jy(t, e) {
  dr(e) && e > t[1] && (t[1] = e);
}
function Rw(t, e) {
  ci(e[0], e[1]) && (e[0] < t[0] && (t[0] = e[0]), e[1] > t[1] && (t[1] = e[1]));
}
function dr(t) {
  return t != null && isFinite(t);
}
function ci(t, e) {
  return dr(t) && dr(e) && t <= e;
}
function Ow(t) {
  var e = t[1] - t[0];
  return isFinite(e) && e >= 0;
}
function kw(t) {
  ci(t[0], t[1]) && t[0] > t[1] && (t[0] = t[1]);
}
function Bw() {
  var t = "__ec_once_" + Nw++;
  return function(e, r) {
    qe(e, t) || (e[t] = 1, r());
  };
}
var Nw = Ch();
function Dh(t, e, r) {
  var n = U(), i = 0;
  T(t, function(a) {
    var o = e(a), s = n.get(o) || 0;
    r && r(a, s), !s && !r && (t[i++] = a), n.set(o, s + 1);
  }), r || (t.length = i);
}
function Fw(t) {
  return t.value + "";
}
function zw(t) {
  return t + "";
}
function HE(t, e) {
  return G(e, !0) ? t.seriesIndex + 2 : 0;
}
function Hw(t, e, r) {
  var n = t.getData().count();
  return {
    progressiveRender: r.progressiveEnabled && e.incrementalPrepareRender && n >= r.threshold,
    large: t.get("large") && n >= t.get("largeThreshold"),
    // TODO: modDataCount should not updated if `appendData`, otherwise cause whole repaint.
    // see `test/candlestick-large3.html`
    modDataCount: t.get("progressiveChunkMode") === "mod" ? t.getData().count() : null
  };
}
function VE(t, e) {
  return {
    seriesType: t,
    overallReset: e
  };
}
function Ih(t) {
  return {
    overallReset: t
  };
}
var Vw = ".", Xr = "___EC__COMPONENT__CONTAINER___", em = "___EC__EXTENDED_CLASS___";
function Vt(t) {
  var e = {
    main: "",
    sub: ""
  };
  if (t) {
    var r = t.split(Vw);
    e.main = r[0] || "", e.sub = r[1] || "";
  }
  return e;
}
function Gw(t) {
  qt(/^[a-zA-Z0-9_]+([.][a-zA-Z0-9_]+)?$/.test(t), 'componentType "' + t + '" illegal');
}
function Uw(t) {
  return !!(t && t[em]);
}
function Ah(t, e) {
  t.$constructor = t, t.extend = function(r) {
    var n = this, i;
    return Yw(n) ? i = /** @class */
    (function(a) {
      z(o, a);
      function o() {
        return a.apply(this, arguments) || this;
      }
      return o;
    })(n) : (i = function() {
      (r.$constructor || n).apply(this, arguments);
    }, ph(i, this)), R(i.prototype, r), i[em] = !0, i.extend = this.extend, i.superCall = qw, i.superApply = $w, i.superClass = n, i;
  };
}
function Yw(t) {
  return X(t) && /^class\s/.test(Function.prototype.toString.call(t));
}
function tm(t, e) {
  t.extend = e.extend;
}
var Ww = Math.round(Math.random() * 10);
function Xw(t) {
  var e = ["__\0is_clz", Ww++].join("_");
  t.prototype[e] = !0, t.isInstance = function(r) {
    return !!(r && r[e]);
  };
}
function qw(t, e) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return this.superClass.prototype[e].apply(t, r);
}
function $w(t, e, r) {
  return this.superClass.prototype[e].apply(t, r);
}
function js(t) {
  var e = {};
  t.registerClass = function(n) {
    var i = n.type || n.prototype.type;
    if (i) {
      Gw(i), n.prototype.type = i;
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
    return o && o[Xr] ? T(o, function(s, u) {
      u !== Xr && a.push(s);
    }) : a.push(o), a;
  }, t.hasClass = function(n) {
    var i = Vt(n);
    return !!e[i.main];
  }, t.getAllClassMainTypes = function() {
    var n = [];
    return T(e, function(i, a) {
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
var Zw = [
  ["fill", "color"],
  ["shadowBlur"],
  ["shadowOffsetX"],
  ["shadowOffsetY"],
  ["opacity"],
  ["shadowColor"]
  // Option decal is in `DecalObject` but style.decal is in `PatternObject`.
  // So do not transfer decal directly.
], Kw = _a(Zw), jw = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getAreaStyle = function(e, r) {
      return Kw(this, e, r);
    }, t;
  })()
), _f = new li(50);
function Qw(t) {
  if (typeof t == "string") {
    var e = _f.get(t);
    return e && e.image;
  } else
    return t;
}
function rm(t, e, r, n, i) {
  if (t)
    if (typeof t == "string") {
      if (e && e.__zrImageSrc === t || !r)
        return e;
      var a = _f.get(t), o = { hostEl: r, cb: n, cbPayload: i };
      return a ? (e = a.image, !Qs(e) && a.pending.push(o)) : (e = Le.loadImage(t, kv, kv), e.__zrImageSrc = t, _f.put(t, e.__cachedImgObj = {
        image: e,
        pending: [o]
      })), e;
    } else
      return t;
  else return e;
}
function kv() {
  var t = this.__cachedImgObj;
  this.onload = this.onerror = this.__cachedImgObj = null;
  for (var e = 0; e < t.pending.length; e++) {
    var r = t.pending[e], n = r.cb;
    n && n(this, r.cbPayload), r.hostEl.dirty();
  }
  t.pending.length = 0;
}
function Qs(t) {
  return t && t.width && t.height;
}
var Ku = /\{([a-zA-Z0-9_]+)\|([^}]*)\}/g;
function Jw(t, e, r, n, i) {
  var a = {};
  return nm(a, t, e, r, n, i), a.text;
}
function nm(t, e, r, n, i, a) {
  if (!r) {
    t.text = "", t.isTruncated = !1;
    return;
  }
  var o = (e + "").split(`
`);
  a = im(r, n, i, a);
  for (var s = !1, u = {}, l = 0, f = o.length; l < f; l++)
    am(u, o[l], a), o[l] = u.textLine, s = s || u.isTruncated;
  t.text = o.join(`
`), t.isTruncated = s;
}
function im(t, e, r, n) {
  n = n || {};
  var i = R({}, n);
  r = G(r, "..."), i.maxIterations = G(n.maxIterations, 2);
  var a = i.minChar = G(n.minChar, 0), o = i.fontMeasureInfo = Gt(e), s = o.asciiCharWidth;
  i.placeholder = G(n.placeholder, "");
  for (var u = t = Math.max(0, t - 1), l = 0; l < a && u >= s; l++)
    u -= s;
  var f = Ut(o, r);
  return f > u && (r = "", f = 0), u = t - f, i.ellipsis = r, i.ellipsisWidth = f, i.contentWidth = u, i.containerWidth = t, i;
}
function am(t, e, r) {
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
    var u = s === 0 ? eT(e, i, a) : o > 0 ? Math.floor(e.length * i / o) : 0;
    e = e.substr(0, u), o = Ut(a, e);
  }
  e === "" && (e = r.placeholder), t.textLine = e, t.isTruncated = !0;
}
function eT(t, e, r) {
  for (var n = 0, i = 0, a = t.length; i < a && n < e; i++)
    n += Fy(r, t.charCodeAt(i));
  return i;
}
function tT(t, e, r, n) {
  var i = Ph(t), a = e.overflow, o = e.padding, s = o ? o[1] + o[3] : 0, u = o ? o[0] + o[2] : 0, l = e.font, f = a === "truncate", h = Xs(l), v = G(e.lineHeight, h), c = e.lineOverflow === "truncate", d = !1, y = e.width;
  y == null && r != null && (y = r - s);
  var p = e.height;
  p == null && n != null && (p = n - u);
  var g;
  y != null && (a === "break" || a === "breakAll") ? g = i ? om(i, e.font, y, a === "breakAll", 0).lines : [] : g = i ? i.split(`
`) : [];
  var m = g.length * v;
  if (p == null && (p = m), m > p && c) {
    var _ = Math.floor(p / v);
    d = d || g.length > _, g = g.slice(0, _), m = g.length * v;
  }
  if (i && f && y != null)
    for (var S = im(y, l, e.ellipsis, {
      minChar: e.truncateMinChar,
      placeholder: e.placeholder
    }), b = {}, w = 0; w < g.length; w++)
      am(b, g[w], S), g[w] = b.textLine, d = d || b.isTruncated;
  for (var C = p, M = 0, x = Gt(l), w = 0; w < g.length; w++)
    M = Math.max(Ut(x, g[w]), M);
  y == null && (y = M);
  var D = y;
  return C += u, D += s, {
    lines: g,
    height: p,
    outerWidth: D,
    outerHeight: C,
    lineHeight: v,
    calculatedLineHeight: h,
    contentWidth: M,
    contentHeight: m,
    width: y,
    isTruncated: d
  };
}
var rT = /* @__PURE__ */ (function() {
  function t() {
  }
  return t;
})(), Bv = /* @__PURE__ */ (function() {
  function t(e) {
    this.tokens = [], e && (this.tokens = e);
  }
  return t;
})(), nT = /* @__PURE__ */ (function() {
  function t() {
    this.width = 0, this.height = 0, this.contentWidth = 0, this.contentHeight = 0, this.outerWidth = 0, this.outerHeight = 0, this.lines = [], this.isTruncated = !1;
  }
  return t;
})();
function iT(t, e, r, n, i) {
  var a = new nT(), o = Ph(t);
  if (!o)
    return a;
  var s = e.padding, u = s ? s[1] + s[3] : 0, l = s ? s[0] + s[2] : 0, f = e.width;
  f == null && r != null && (f = r - u);
  var h = e.height;
  h == null && n != null && (h = n - l);
  for (var v = e.overflow, c = (v === "break" || v === "breakAll") && f != null ? { width: f, accumWidth: 0, breakAll: v === "breakAll" } : null, d = Ku.lastIndex = 0, y; (y = Ku.exec(o)) != null; ) {
    var p = y.index;
    p > d && ju(a, o.substring(d, p), e, c), ju(a, y[2], e, c, y[1]), d = Ku.lastIndex;
  }
  d < o.length && ju(a, o.substring(d, o.length), e, c);
  var g = [], m = 0, _ = 0, S = v === "truncate", b = e.lineOverflow === "truncate", w = {};
  function C(Se, tt, Qt) {
    Se.width = tt, Se.lineHeight = Qt, m += Qt, _ = Math.max(_, tt);
  }
  e: for (var M = 0; M < a.lines.length; M++) {
    for (var x = a.lines[M], D = 0, I = 0, L = 0; L < x.tokens.length; L++) {
      var A = x.tokens[L], P = A.styleName && e.rich[A.styleName] || {}, k = A.textPadding = P.padding, O = k ? k[1] + k[3] : 0, V = A.font = P.font || e.font;
      A.contentHeight = Xs(V);
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
          ne && (ne = Qw(ne), Qs(ne) && (A.width = Math.max(A.width, ne.width * K / ne.height)));
        }
        var q = S && f != null ? f - I : null;
        q != null && q < A.width ? !Q || q < O ? (A.text = "", A.width = A.contentWidth = 0) : (nm(w, A.text, q - O, V, e.ellipsis, { minChar: e.truncateMinChar }), A.text = w.text, a.isTruncated = a.isTruncated || w.isTruncated, A.width = A.contentWidth = Ut(Gt(V), A.text)) : A.contentWidth = Ut(Gt(V), A.text);
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
function ju(t, e, r, n, i) {
  var a = e === "", o = i && r.rich[i] || {}, s = t.lines, u = o.font || r.font, l = !1, f, h;
  if (n) {
    var v = o.padding, c = v ? v[1] + v[3] : 0;
    if (o.width != null && o.width !== "auto") {
      var d = hi(o.width, n.width) + c;
      s.length > 0 && d + n.accumWidth > n.width && (f = e.split(`
`), l = !0), n.accumWidth = d;
    } else {
      var y = om(e, u, n.width, n.breakAll, n.accumWidth);
      n.accumWidth = y.accumWidth + c, h = y.linesWidths, f = y.lines;
    }
  }
  f || (f = e.split(`
`));
  for (var p = Gt(u), g = 0; g < f.length; g++) {
    var m = f[g], _ = new rT();
    if (_.styleName = i, _.text = m, _.isLineHolder = !m && !a, typeof o.width == "number" ? _.width = o.width : _.width = h ? h[g] : Ut(p, m), !g && !l) {
      var S = (s[s.length - 1] || (s[0] = new Bv())).tokens, b = S.length;
      b === 1 && S[0].isLineHolder ? S[0] = _ : (m || !b || a) && S.push(_);
    } else
      s.push(new Bv([_]));
  }
}
function aT(t) {
  var e = t.charCodeAt(0);
  return e >= 32 && e <= 591 || e >= 880 && e <= 4351 || e >= 4608 && e <= 5119 || e >= 7680 && e <= 8303;
}
var oT = Xt(",&?/;] ".split(""), function(t, e) {
  return t[e] = !0, t;
}, {});
function sT(t) {
  return aT(t) ? !!oT[t] : !0;
}
function om(t, e, r, n, i) {
  for (var a = [], o = [], s = "", u = "", l = 0, f = 0, h = Gt(e), v = 0; v < t.length; v++) {
    var c = t.charAt(v);
    if (c === `
`) {
      u && (s += u, f += l), a.push(s), o.push(f), s = "", u = "", l = 0, f = 0;
      continue;
    }
    var d = Fy(h, c.charCodeAt(0)), y = n ? !1 : !sT(c);
    if (a.length ? f + d > r : i + f + d > r) {
      f ? (s || u) && (y ? (s || (s = u, u = "", l = 0, f = l), a.push(s), o.push(f - l), u += c, l += d, s = "", f = l) : (u && (s += u, u = "", l = 0), a.push(s), o.push(f), s = c, f = d)) : y ? (a.push(u), o.push(l), u = c, l = d) : (a.push(c), o.push(d));
      continue;
    }
    f += d, y ? (u += c, l += d) : (u && (s += u, u = "", l = 0), s += c);
  }
  return u && (s += u), s && (a.push(s), o.push(f)), a.length === 1 && (f += i), {
    accumWidth: f,
    lines: a,
    linesWidths: o
  };
}
function Nv(t, e, r, n, i, a) {
  if (t.baseX = r, t.baseY = n, t.outerWidth = t.outerHeight = null, !!e) {
    var o = e.width * 2, s = e.height * 2;
    Y.set(Fv, fi(r, o, i), _n(n, s, a), o, s), Y.intersect(e, Fv, null, zv);
    var u = zv.outIntersectRect;
    t.outerWidth = u.width, t.outerHeight = u.height, t.baseX = fi(u.x, u.width, i, !0), t.baseY = _n(u.y, u.height, a, !0);
  }
}
var Fv = new Y(0, 0, 0, 0), zv = { outIntersectRect: {}, clamp: !0 };
function Ph(t) {
  return t != null ? t += "" : t = "";
}
function uT(t) {
  var e = Ph(t.text), r = t.font, n = Ut(Gt(r), e), i = Xs(r);
  return Sf(t, n, i, null);
}
function Sf(t, e, r, n) {
  var i = new Y(fi(t.x || 0, e, t.textAlign), _n(t.y || 0, r, t.textBaseline), e, r), a = n ?? (sm(t) ? t.lineWidth : 0);
  return a > 0 && (i.x -= a / 2, i.y -= a / 2, i.width += a, i.height += a), i;
}
function sm(t) {
  var e = t.stroke;
  return e != null && e !== "none" && t.lineWidth > 0;
}
var bf = "__zr_style_" + Math.round(Math.random() * 10), Sn = {
  shadowBlur: 0,
  shadowOffsetX: 0,
  shadowOffsetY: 0,
  shadowColor: "#000",
  opacity: 1,
  blend: "source-over"
}, Js = {
  style: {
    shadowBlur: !0,
    shadowOffsetX: !0,
    shadowOffsetY: !0,
    shadowColor: !0,
    opacity: !0
  }
};
Sn[bf] = !0;
var Hv = ["z", "z2", "invisible"], lT = ["invisible"], Ba = (function(t) {
  z(e, t);
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
    if (this.ignore || this.invisible || this.style.opacity === 0 || this.culling && fT(this, r, n) || o && !o[0] && !o[3])
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
    return typeof r == "string" ? this.style[r] = n : R(this.style, r), this.dirtyStyle(), this;
  }, e.prototype.dirtyStyle = function(r) {
    r || this.markRedraw(), this.__dirty |= $i, this._rect && (this._rect = null);
  }, e.prototype.dirty = function() {
    this.dirtyStyle();
  }, e.prototype.styleChanged = function() {
    return !!(this.__dirty & $i);
  }, e.prototype.styleUpdated = function() {
    this.__dirty &= ~$i;
  }, e.prototype.createStyle = function(r) {
    return Pa(Sn, r);
  }, e.prototype.useStyle = function(r) {
    r[bf] || (r = this.createStyle(r)), this.style = r, this.dirtyStyle();
  }, e.prototype._useHoverStyle = function(r) {
    this.__hoverStyle = r;
  }, e.prototype.isStyleObject = function(r) {
    return r[bf];
  }, e.prototype._innerSaveToNormal = function(r) {
    t.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.style && !n.style && (n.style = this._mergeStyle(this.createStyle(), this.style)), this._savePrimaryToNormal(r, n, Hv);
  }, e.prototype._applyStateObj = function(r, n, i, a, o, s) {
    t.prototype._applyStateObj.call(this, r, n, i, a, o, s);
    var u = !(n && a), l = this.__inHover === qs, f;
    if (n && n.style ? o ? a ? f = n.style : (f = this._mergeStyle(this.createStyle(), i.style), this._mergeStyle(f, n.style)) : (f = this._mergeStyle(this.createStyle(), a ? this.style : i.style), this._mergeStyle(f, n.style)) : u && (f = i.style), f)
      if (o) {
        var h = this.style;
        if (this.style = this.createStyle(u ? {} : h), u)
          for (var v = me(h), c = 0; c < v.length; c++) {
            var d = v[c];
            d in f && (f[d] = f[d], this.style[d] = h[d]);
          }
        for (var y = me(f), c = 0; c < y.length; c++) {
          var d = y[c];
          this.style[d] = this.style[d];
        }
        this._transitionState(r, {
          style: f
        }, s, this.getAnimationStyleProps());
      } else
        l ? this._useHoverStyle(f) : this.useStyle(f);
    if (!l)
      for (var p = this.__inHover ? lT : Hv, c = 0; c < p.length; c++) {
        var d = p[c];
        n && n[d] != null ? this[d] = n[d] : u && i[d] != null && (this[d] = i[d]);
      }
  }, e.prototype._mergeStates = function(r) {
    for (var n = t.prototype._mergeStates.call(this, r), i, a = 0; a < r.length; a++) {
      var o = r[a];
      o.style && (i = i || {}, this._mergeStyle(i, o.style));
    }
    return i && (n.style = i), n;
  }, e.prototype._mergeStyle = function(r, n) {
    return R(r, n), r;
  }, e.prototype.getAnimationStyleProps = function() {
    return Js;
  }, e.initDefaultProps = (function() {
    var r = e.prototype;
    r.type = "displayable", r.invisible = !1, r.z = 0, r.z2 = 0, r.zlevel = 0, r.culling = !1, r.cursor = "pointer", r.rectHover = !1, r.incremental = 0, r._rect = null, r.dirtyRectTolerance = 0, r.__dirty = Je | $i;
  })(), e;
})($s), Qu = new Y(0, 0, 0, 0), Ju = new Y(0, 0, 0, 0);
function fT(t, e, r) {
  return Qu.copy(t.getBoundingRect()), t.transform && Qu.applyTransform(t.transform), Ju.width = e, Ju.height = r, !Qu.intersect(Ju);
}
var mt = Math.min, _t = Math.max, el = Math.sin, tl = Math.cos, qr = Math.PI * 2, Qa = xn(), Ja = xn(), eo = xn();
function Vv(t, e, r, n, i, a) {
  i[0] = mt(t, r), i[1] = mt(e, n), a[0] = _t(t, r), a[1] = _t(e, n);
}
var Gv = [], Uv = [];
function hT(t, e, r, n, i, a, o, s, u, l) {
  var f = Iy, h = Pe, v = f(t, r, i, o, Gv);
  u[0] = 1 / 0, u[1] = 1 / 0, l[0] = -1 / 0, l[1] = -1 / 0;
  for (var c = 0; c < v; c++) {
    var d = h(t, r, i, o, Gv[c]);
    u[0] = mt(d, u[0]), l[0] = _t(d, l[0]);
  }
  v = f(e, n, a, s, Uv);
  for (var c = 0; c < v; c++) {
    var y = h(e, n, a, s, Uv[c]);
    u[1] = mt(y, u[1]), l[1] = _t(y, l[1]);
  }
  u[0] = mt(t, u[0]), l[0] = _t(t, l[0]), u[0] = mt(o, u[0]), l[0] = _t(o, l[0]), u[1] = mt(e, u[1]), l[1] = _t(e, l[1]), u[1] = mt(s, u[1]), l[1] = _t(s, l[1]);
}
function cT(t, e, r, n, i, a, o, s) {
  var u = Ay, l = Be, f = _t(mt(u(t, r, i), 1), 0), h = _t(mt(u(e, n, a), 1), 0), v = l(t, r, i, f), c = l(e, n, a, h);
  o[0] = mt(t, i, v), o[1] = mt(e, a, c), s[0] = _t(t, i, v), s[1] = _t(e, a, c);
}
function vT(t, e, r, n, i, a, o, s, u) {
  var l = Cr, f = xr, h = Math.abs(i - a);
  if (h % qr < 1e-4 && h > 1e-4) {
    s[0] = t - r, s[1] = e - n, u[0] = t + r, u[1] = e + n;
    return;
  }
  if (Qa[0] = tl(i) * r + t, Qa[1] = el(i) * n + e, Ja[0] = tl(a) * r + t, Ja[1] = el(a) * n + e, l(s, Qa, Ja), f(u, Qa, Ja), i = i % qr, i < 0 && (i = i + qr), a = a % qr, a < 0 && (a = a + qr), i > a && !o ? a += qr : i < a && o && (i += qr), o) {
    var v = a;
    a = i, i = v;
  }
  for (var c = 0; c < a; c += Math.PI / 2)
    c > i && (eo[0] = tl(c) * r + t, eo[1] = el(c) * n + e, l(s, eo, s), f(u, eo, u));
}
var ie = {
  M: 1,
  L: 2,
  C: 3,
  Q: 4,
  A: 5,
  Z: 6,
  R: 7
}, $r = [], Zr = [], Ot = [], mr = [], kt = [], Bt = [], rl = Math.min, nl = Math.max, Kr = Math.cos, jr = Math.sin, rr = Math.abs, wf = Math.PI, wr = wf * 2, il = typeof Float32Array < "u", Li = [];
function al(t) {
  var e = Math.round(t / wf * 1e8) / 1e8;
  return e % 2 * wf;
}
function dT(t, e) {
  var r = al(t[0]);
  r < 0 && (r += wr);
  var n = r - t[0], i = t[1];
  i += n, !e && i - r >= wr ? i = r + wr : e && r - i >= wr ? i = r - wr : !e && r > i ? i = r + (wr - al(r - i)) : e && r < i && (i = r - (wr - al(i - r))), t[0] = r, t[1] = i;
}
var vi = (function() {
  function t(e) {
    this.dpr = 1, this._xi = 0, this._yi = 0, this._x0 = 0, this._y0 = 0, this._len = 0, e && (this._saveData = !1), this._saveData && (this.data = []);
  }
  return t.prototype.increaseVersion = function() {
    this._version++;
  }, t.prototype.getVersion = function() {
    return this._version;
  }, t.prototype.setScale = function(e, r, n) {
    n = n || 0, n > 0 && (this._ux = rr(n / ls / e) || 0, this._uy = rr(n / ls / r) || 0);
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
    this._drawPendingPt(), Li[0] = i, Li[1] = a, dT(Li, o), i = Li[0], a = Li[1];
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
      !(this.data && this.data.length === r) && il && (this.data = new Float32Array(r));
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
      if (il && (o instanceof Float32Array || !o) && (this.data = new Float32Array(i + n), i > 0 && o))
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
      e instanceof Array && (e.length = this._len, il && this._len > 11 && (this.data = new Float32Array(e)));
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
          Vv(r, n, e[o], e[o + 1], kt, Bt), r = e[o++], n = e[o++];
          break;
        case ie.C:
          hT(r, n, e[o++], e[o++], e[o++], e[o++], e[o], e[o + 1], kt, Bt), r = e[o++], n = e[o++];
          break;
        case ie.Q:
          cT(r, n, e[o++], e[o++], e[o], e[o + 1], kt, Bt), r = e[o++], n = e[o++];
          break;
        case ie.A:
          var l = e[o++], f = e[o++], h = e[o++], v = e[o++], c = e[o++], d = e[o++] + c;
          o += 1;
          var y = !e[o++];
          u && (i = Kr(c) * h + l, a = jr(c) * v + f), vT(l, f, h, v, c, d, y, kt, Bt), r = Kr(d) * h + l, n = jr(d) * v + f;
          break;
        case ie.R:
          i = r = e[o++], a = n = e[o++];
          var p = e[o++], g = e[o++];
          Vv(i, a, i + p, a + g, kt, Bt);
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
    for (var l = this._pathSegLen, f = 0, h = 0, v = 0; v < r; ) {
      var c = e[v++], d = v === 1;
      d && (a = e[v], o = e[v + 1], s = a, u = o);
      var y = -1;
      switch (c) {
        case ie.M:
          a = s = e[v++], o = u = e[v++];
          break;
        case ie.L: {
          var p = e[v++], g = e[v++], m = p - a, _ = g - o;
          (rr(m) > n || rr(_) > i || v === r - 1) && (y = Math.sqrt(m * m + _ * _), a = p, o = g);
          break;
        }
        case ie.C: {
          var S = e[v++], b = e[v++], p = e[v++], g = e[v++], w = e[v++], C = e[v++];
          y = nb(a, o, S, b, p, g, w, C, 10), a = w, o = C;
          break;
        }
        case ie.Q: {
          var S = e[v++], b = e[v++], p = e[v++], g = e[v++];
          y = ob(a, o, S, b, p, g, 10), a = p, o = g;
          break;
        }
        case ie.A:
          var M = e[v++], x = e[v++], D = e[v++], I = e[v++], L = e[v++], A = e[v++], P = A + L;
          v += 1, d && (s = Kr(L) * D + M, u = jr(L) * I + x), y = nl(D, I) * rl(wr, Math.abs(A)), a = Kr(P) * D + M, o = jr(P) * I + x;
          break;
        case ie.R: {
          s = a = e[v++], u = o = e[v++];
          var k = e[v++], O = e[v++];
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
    var n = this.data, i = this._ux, a = this._uy, o = this._len, s, u, l, f, h, v, c = r < 1, d, y, p = 0, g = 0, m, _ = 0, S, b;
    if (!(c && (this._pathSegLen || this._calculateLength(), d = this._pathSegLen, y = this._pathLen, m = r * y, !m)))
      e: for (var w = 0; w < o; ) {
        var C = n[w++], M = w === 1;
        switch (M && (l = n[w], f = n[w + 1], s = l, u = f), C !== ie.L && _ > 0 && (e.lineTo(S, b), _ = 0), C) {
          case ie.M:
            s = l = n[w++], u = f = n[w++], e.moveTo(l, f);
            break;
          case ie.L: {
            h = n[w++], v = n[w++];
            var x = rr(h - l), D = rr(v - f);
            if (x > i || D > a) {
              if (c) {
                var I = d[g++];
                if (p + I > m) {
                  var L = (m - p) / I;
                  e.lineTo(l * (1 - L) + h * L, f * (1 - L) + v * L);
                  break e;
                }
                p += I;
              }
              e.lineTo(h, v), l = h, f = v, _ = 0;
            } else {
              var A = x * x + D * D;
              A > _ && (S = h, b = v, _ = A);
            }
            break;
          }
          case ie.C: {
            var P = n[w++], k = n[w++], O = n[w++], V = n[w++], K = n[w++], J = n[w++];
            if (c) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                os(l, P, O, K, L, $r), os(f, k, V, J, L, Zr), e.bezierCurveTo($r[1], Zr[1], $r[2], Zr[2], $r[3], Zr[3]);
                break e;
              }
              p += I;
            }
            e.bezierCurveTo(P, k, O, V, K, J), l = K, f = J;
            break;
          }
          case ie.Q: {
            var P = n[w++], k = n[w++], O = n[w++], V = n[w++];
            if (c) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                ss(l, P, O, L, $r), ss(f, k, V, L, Zr), e.quadraticCurveTo($r[1], Zr[1], $r[2], Zr[2]);
                break e;
              }
              p += I;
            }
            e.quadraticCurveTo(P, k, O, V), l = O, f = V;
            break;
          }
          case ie.A:
            var ee = n[w++], Q = n[w++], re = n[w++], ne = n[w++], q = n[w++], se = n[w++], Se = n[w++], tt = !n[w++], Qt = re > ne ? re : ne, rt = rr(re - ne) > 1e-3, Ae = q + se, $ = !1;
            if (c) {
              var I = d[g++];
              p + I > m && (Ae = q + se * (m - p) / I, $ = !0), p += I;
            }
            if (rt && e.ellipse ? e.ellipse(ee, Q, re, ne, Se, q, Ae, tt) : e.arc(ee, Q, Qt, q, Ae, tt), $)
              break e;
            M && (s = Kr(q) * re + ee, u = jr(q) * ne + Q), l = Kr(Ae) * re + ee, f = jr(Ae) * ne + Q;
            break;
          case ie.R:
            s = l = n[w], u = f = n[w + 1], h = n[w++], v = n[w++];
            var te = n[w++], zr = n[w++];
            if (c) {
              var I = d[g++];
              if (p + I > m) {
                var He = m - p;
                e.moveTo(h, v), e.lineTo(h + rl(He, te), v), He -= te, He > 0 && e.lineTo(h + te, v + rl(He, zr)), He -= zr, He > 0 && e.lineTo(h + nl(te - He, 0), v + zr), He -= te, He > 0 && e.lineTo(h, v + nl(zr - He, 0));
                break e;
              }
              p += I;
            }
            e.rect(h, v, te, zr);
            break;
          case ie.Z:
            if (c) {
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
function pT(t, e, r, n, i, a, o, s, u, l, f) {
  if (u === 0)
    return !1;
  var h = u;
  if (f > e + h && f > n + h && f > a + h && f > s + h || f < e - h && f < n - h && f < a - h && f < s - h || l > t + h && l > r + h && l > i + h && l > o + h || l < t - h && l < r - h && l < i - h && l < o - h)
    return !1;
  var v = rb(t, e, r, n, i, a, o, s, l, f, null);
  return v <= h / 2;
}
function gT(t, e, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  if (u > e + l && u > n + l && u > a + l || u < e - l && u < n - l && u < a - l || s > t + l && s > r + l && s > i + l || s < t - l && s < r - l && s < i - l)
    return !1;
  var f = ab(t, e, r, n, i, a, s, u, null);
  return f <= l / 2;
}
var Yv = Math.PI * 2;
function to(t) {
  return t %= Yv, t < 0 && (t += Yv), t;
}
var Ei = Math.PI * 2;
function yT(t, e, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  s -= t, u -= e;
  var f = Math.sqrt(s * s + u * u);
  if (f - l > r || f + l < r)
    return !1;
  if (Math.abs(n - i) % Ei < 1e-4)
    return !0;
  if (a) {
    var h = n;
    n = to(i), i = to(h);
  } else
    n = to(n), i = to(i);
  n > i && (i += Ei);
  var v = Math.atan2(u, s);
  return v < 0 && (v += Ei), v >= n && v <= i || v + Ei >= n && v + Ei <= i;
}
function sr(t, e, r, n, i, a) {
  if (a > e && a > n || a < e && a < n || n === e)
    return 0;
  var o = (a - e) / (n - e), s = n < e ? 1 : -1;
  (o === 1 || o === 0) && (s = n < e ? 0.5 : -0.5);
  var u = o * (r - t) + t;
  return u === i ? 1 / 0 : u > i ? s : 0;
}
var _r = vi.CMD, Qr = Math.PI * 2, mT = 1e-4;
function _T(t, e) {
  return Math.abs(t - e) < mT;
}
var Ve = [-1, -1, -1], gt = [-1, -1];
function ST() {
  var t = gt[0];
  gt[0] = gt[1], gt[1] = t;
}
function bT(t, e, r, n, i, a, o, s, u, l) {
  if (l > e && l > n && l > a && l > s || l < e && l < n && l < a && l < s)
    return 0;
  var f = Dy(e, n, a, s, l, Ve);
  if (f === 0)
    return 0;
  for (var h = 0, v = -1, c = void 0, d = void 0, y = 0; y < f; y++) {
    var p = Ve[y], g = p === 0 || p === 1 ? 0.5 : 1, m = Pe(t, r, i, o, p);
    m < u || (v < 0 && (v = Iy(e, n, a, s, gt), gt[1] < gt[0] && v > 1 && ST(), c = Pe(e, n, a, s, gt[0]), v > 1 && (d = Pe(e, n, a, s, gt[1]))), v === 2 ? p < gt[0] ? h += c < e ? g : -g : p < gt[1] ? h += d < c ? g : -g : h += s < d ? g : -g : p < gt[0] ? h += c < e ? g : -g : h += s < c ? g : -g);
  }
  return h;
}
function wT(t, e, r, n, i, a, o, s) {
  if (s > e && s > n && s > a || s < e && s < n && s < a)
    return 0;
  var u = ib(e, n, a, s, Ve);
  if (u === 0)
    return 0;
  var l = Ay(e, n, a);
  if (l >= 0 && l <= 1) {
    for (var f = 0, h = Be(e, n, a, l), v = 0; v < u; v++) {
      var c = Ve[v] === 0 || Ve[v] === 1 ? 0.5 : 1, d = Be(t, r, i, Ve[v]);
      d < o || (Ve[v] < l ? f += h < e ? c : -c : f += a < h ? c : -c);
    }
    return f;
  } else {
    var c = Ve[0] === 0 || Ve[0] === 1 ? 0.5 : 1, d = Be(t, r, i, Ve[0]);
    return d < o ? 0 : a < e ? c : -c;
  }
}
function TT(t, e, r, n, i, a, o, s) {
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
  for (var v = 0, c = 0; c < 2; c++) {
    var d = Ve[c];
    if (d + t > o) {
      var y = Math.atan2(s, d), f = a ? 1 : -1;
      y < 0 && (y = Qr + y), (y >= n && y <= i || y + Qr >= n && y + Qr <= i) && (y > Math.PI / 2 && y < Math.PI * 1.5 && (f = -f), v += f);
    }
  }
  return v;
}
function um(t, e, r, n, i) {
  for (var a = t.data, o = t.len(), s = 0, u = 0, l = 0, f = 0, h = 0, v, c, d = 0; d < o; ) {
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
          if (pT(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], e, n, i))
            return !0;
        } else
          s += bT(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case _r.Q:
        if (r) {
          if (gT(u, l, a[d++], a[d++], a[d], a[d + 1], e, n, i))
            return !0;
        } else
          s += wT(u, l, a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case _r.A:
        var g = a[d++], m = a[d++], _ = a[d++], S = a[d++], b = a[d++], w = a[d++];
        d += 1;
        var C = !!(1 - a[d++]);
        v = Math.cos(b) * _ + g, c = Math.sin(b) * S + m, p ? (f = v, h = c) : s += sr(u, l, v, c, n, i);
        var M = (n - g) * S / _ + g;
        if (r) {
          if (yT(g, m, S, b, b + w, C, e, M, i))
            return !0;
        } else
          s += TT(g, m, S, b, b + w, C, M, i);
        u = Math.cos(b + w) * _ + g, l = Math.sin(b + w) * S + m;
        break;
      case _r.R:
        f = u = a[d++], h = l = a[d++];
        var x = a[d++], D = a[d++];
        if (v = f + x, c = h + D, r) {
          if (Bn(f, h, v, h, e, n, i) || Bn(v, h, v, c, e, n, i) || Bn(v, c, f, c, e, n, i) || Bn(f, c, f, h, e, n, i))
            return !0;
        } else
          s += sr(v, h, v, c, n, i), s += sr(f, c, f, h, n, i);
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
  return !r && !_T(l, h) && (s += sr(u, l, f, h, n, i) || 0), s !== 0;
}
function CT(t, e, r) {
  return um(t, 0, !1, e, r);
}
function xT(t, e, r, n) {
  return um(t, e, !0, r, n);
}
var lm = _e({
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
}, Sn), MT = {
  style: _e({
    fill: !0,
    stroke: !0,
    strokePercent: !0,
    fillOpacity: !0,
    strokeOpacity: !0,
    lineDashOffset: !0,
    lineWidth: !0,
    miterLimit: !0
  }, Js.style)
}, ol = Ws.concat([
  "invisible",
  "culling",
  "z",
  "z2",
  "zlevel",
  "parent"
]), ye = (function(t) {
  z(e, t);
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
      for (var s = 0; s < ol.length; ++s)
        i[ol[s]] = this[ol[s]];
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
      o === "style" ? this.style ? R(this.style, s) : this.useStyle(s) : o === "shape" ? R(this.shape, s) : t.prototype.attrKV.call(this, o, s);
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
        return n > 0.5 ? df : n > 0.2 ? Rb : pf;
      } else if (r)
        return pf;
    }
    return df;
  }, e.prototype.getInsideTextStroke = function(r) {
    var n = this.style.fill;
    if (N(n)) {
      var i = this.__zr, a = !!(i && i.isDarkMode()), o = pa(r, 0) < vf;
      if (a === o)
        return n;
    }
  }, e.prototype.buildPath = function(r, n, i) {
  }, e.prototype.pathUpdated = function() {
    this.__dirty &= ~$n;
  }, e.prototype.getUpdatedPathProxy = function(r) {
    return !this.path && this.createPathProxy(), this.path.beginPath(), this.buildPath(this.path, this.shape, r), this.path;
  }, e.prototype.createPathProxy = function() {
    this.path = new vi(!1);
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
        if (l > 1e-10 && (this.hasFill() || (u = Math.max(u, this.strokeContainThreshold)), xT(s, u / l, r, n)))
          return !0;
      }
      if (this.hasFill())
        return CT(s, r, n);
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
    return i || (i = this.shape = {}), typeof r == "string" ? i[r] = n : R(i, r), this.dirtyShape(), this;
  }, e.prototype.shapeChanged = function() {
    return !!(this.__dirty & $n);
  }, e.prototype.createStyle = function(r) {
    return Pa(lm, r);
  }, e.prototype._innerSaveToNormal = function(r) {
    t.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.shape && !n.shape && (n.shape = R({}, this.shape));
  }, e.prototype._applyStateObj = function(r, n, i, a, o, s) {
    if (t.prototype._applyStateObj.call(this, r, n, i, a, o, s), this.__inHover !== qs) {
      var u = !(n && a), l;
      if (n && n.shape ? o ? a ? l = n.shape : (l = R({}, i.shape), R(l, n.shape)) : (l = R({}, a ? this.shape : i.shape), R(l, n.shape)) : u && (l = i.shape), l)
        if (o) {
          this.shape = R({}, this.shape);
          for (var f = {}, h = me(l), v = 0; v < h.length; v++) {
            var c = h[v];
            typeof l[c] == "object" ? this.shape[c] = l[c] : f[c] = l[c];
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
    return MT;
  }, e.prototype.isZeroArea = function() {
    return !1;
  }, e.extend = function(r) {
    var n = (function(a) {
      z(o, a);
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
    r.type = "path", r.strokeContainThreshold = 5, r.segmentIgnoreThreshold = 0, r.subPixelOptimize = !1, r.autoBatch = !1, r.__dirty = Je | $i | $n;
  })(), e;
})(Ba), DT = _e({
  strokeFirst: !0,
  font: Rr,
  x: 0,
  y: 0,
  textAlign: "left",
  textBaseline: "top",
  miterLimit: 2
}, lm), cs = (function(t) {
  z(e, t);
  function e() {
    return t !== null && t.apply(this, arguments) || this;
  }
  return e.prototype.hasStroke = function() {
    return sm(this.style);
  }, e.prototype.hasFill = function() {
    var r = this.style, n = r.fill;
    return n != null && n !== "none";
  }, e.prototype.createStyle = function(r) {
    return Pa(DT, r);
  }, e.prototype.setBoundingRect = function(r) {
    this._rect = r;
  }, e.prototype.getBoundingRect = function() {
    return this._rect || (this._rect = uT(this.style)), this._rect;
  }, e.initDefaultProps = (function() {
    var r = e.prototype;
    r.dirtyRectTolerance = 10;
  })(), e;
})(Ba);
cs.prototype.type = "tspan";
var IT = _e({
  x: 0,
  y: 0
}, Sn), AT = {
  style: _e({
    x: !0,
    y: !0,
    width: !0,
    height: !0,
    sx: !0,
    sy: !0,
    sWidth: !0,
    sHeight: !0
  }, Js.style)
};
function PT(t) {
  return !!(t && typeof t != "string" && t.width && t.height);
}
var Nr = (function(t) {
  z(e, t);
  function e() {
    return t !== null && t.apply(this, arguments) || this;
  }
  return e.prototype.createStyle = function(r) {
    return Pa(IT, r);
  }, e.prototype._getSize = function(r) {
    var n = this.style, i = n[r];
    if (i != null)
      return i;
    var a = PT(n.image) ? n.image : this.__image;
    if (!a)
      return 0;
    var o = r === "width" ? "height" : "width", s = n[o];
    return s == null ? a[r] : a[r] / a[o] * s;
  }, e.prototype.getWidth = function() {
    return this._getSize("width");
  }, e.prototype.getHeight = function() {
    return this._getSize("height");
  }, e.prototype.getAnimationStyleProps = function() {
    return AT;
  }, e.prototype.getBoundingRect = function() {
    var r = this.style;
    return this._rect || (this._rect = new Y(r.x || 0, r.y || 0, this.getWidth(), this.getHeight())), this._rect;
  }, e;
})(Ba);
Nr.prototype.type = "image";
function LT(t, e) {
  var r = e.x, n = e.y, i = e.width, a = e.height, o = e.r, s, u, l, f;
  i < 0 && (r = r + i, i = -i), a < 0 && (n = n + a, a = -a), typeof o == "number" ? s = u = l = f = o : o instanceof Array ? o.length === 1 ? s = u = l = f = o[0] : o.length === 2 ? (s = l = o[0], u = f = o[1]) : o.length === 3 ? (s = o[0], u = f = o[1], l = o[2]) : (s = o[0], u = o[1], l = o[2], f = o[3]) : s = u = l = f = 0;
  var h;
  s + u > i && (h = s + u, s *= i / h, u *= i / h), l + f > i && (h = l + f, l *= i / h, f *= i / h), u + l > a && (h = u + l, u *= a / h, l *= a / h), s + f > a && (h = s + f, s *= a / h, f *= a / h), t.moveTo(r + s, n), t.lineTo(r + i - u, n), u !== 0 && t.arc(r + i - u, n + u, u, -Math.PI / 2, 0), t.lineTo(r + i, n + a - l), l !== 0 && t.arc(r + i - l, n + a - l, l, 0, Math.PI / 2), t.lineTo(r + f, n + a), f !== 0 && t.arc(r + f, n + a - f, f, Math.PI / 2, Math.PI), t.lineTo(r, n + s), s !== 0 && t.arc(r + s, n + s, s, Math.PI, Math.PI * 1.5), t.closePath();
}
var Qn = Math.round;
function fm(t, e, r) {
  if (e) {
    var n = e.x1, i = e.x2, a = e.y1, o = e.y2;
    t.x1 = n, t.x2 = i, t.y1 = a, t.y2 = o;
    var s = r && r.lineWidth;
    return s && (Qn(n * 2) === Qn(i * 2) && (t.x1 = t.x2 = vn(n, s, !0)), Qn(a * 2) === Qn(o * 2) && (t.y1 = t.y2 = vn(a, s, !0))), t;
  }
}
function hm(t, e, r) {
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
var ET = /* @__PURE__ */ (function() {
  function t() {
    this.x = 0, this.y = 0, this.width = 0, this.height = 0;
  }
  return t;
})(), RT = {}, Re = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new ET();
  }, e.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = hm(RT, n, this.style);
      i = u.x, a = u.y, o = u.width, s = u.height, u.r = n.r, n = u;
    } else
      i = n.x, a = n.y, o = n.width, s = n.height;
    n.r ? LT(r, n) : r.rect(i, a, o, s);
  }, e.prototype.isZeroArea = function() {
    return !this.shape.width || !this.shape.height;
  }, e;
})(ye);
Re.prototype.type = "rect";
var Wv = {
  fill: "#000"
}, Xv = 2, Nt = {}, OT = {
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
  }, Js.style)
}, je = (function(t) {
  z(e, t);
  function e(r) {
    var n = t.call(this) || this;
    return n.type = "text", n._children = [], n._defaultStyle = Wv, n.attr(r), n;
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
    this._childCursor = 0, zT(this.style), this.style.rich ? this._updateRichTexts() : this._updatePlainTexts(), this._children.length = this._childCursor, this.styleUpdated();
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
    this._defaultStyle = r || Wv;
  }, e.prototype.setTextContent = function(r) {
  }, e.prototype._mergeStyle = function(r, n) {
    if (!n)
      return r;
    var i = n.rich, a = r.rich || i && {};
    return R(r, n), i && a ? (this._mergeRich(a, i), r.rich = a) : a && (r.rich = a), r;
  }, e.prototype._mergeRich = function(r, n) {
    for (var i = me(n), a = 0; a < i.length; a++) {
      var o = i[a];
      r[o] = r[o] || {}, R(r[o], n[o]);
    }
  }, e.prototype.getAnimationStyleProps = function() {
    return OT;
  }, e.prototype._getOrCreateChild = function(r) {
    var n = this._children[this._childCursor];
    return (!n || !(n instanceof r)) && (n = new r()), this._children[this._childCursor++] = n, n.__zr = this.__zr, n.parent = this, n;
  }, e.prototype._updatePlainTexts = function() {
    var r = this.style, n = r.font || Rr, i = r.padding, a = this._defaultStyle, o = r.x || 0, s = r.y || 0, u = r.align || a.align || "left", l = r.verticalAlign || a.verticalAlign || "top";
    Nv(Nt, a.overflowRect, o, s, u, l), o = Nt.baseX, s = Nt.baseY;
    var f = Jv(r), h = tT(f, r, Nt.outerWidth, Nt.outerHeight), v = sl(r), c = !!r.backgroundColor, d = h.outerHeight, y = h.outerWidth, p = h.lines, g = h.lineHeight;
    this.isTruncated = !!h.isTruncated;
    var m = o, _ = _n(s, h.contentHeight, l);
    if (v || i) {
      var S = fi(o, y, u), b = _n(s, d, l);
      v && this._renderBackground(r, r, S, b, y, d);
    }
    _ += g / 2, i && (m = Qv(o, u, i), l === "top" ? _ += i[0] : l === "bottom" && (_ -= i[2]));
    for (var w = 0, C = !1, M = !1, x = jv("fill" in r ? r.fill : (M = !0, a.fill)), D = Kv("stroke" in r ? r.stroke : !c && (!a.autoStroke || M) ? (w = Xv, C = !0, a.stroke) : null), I = r.textShadowBlur > 0, L = 0; L < p.length; L++) {
      var A = this._getOrCreateChild(cs), P = A.createStyle();
      A.useStyle(P), P.text = p[L], P.x = m, P.y = _, P.textAlign = u, P.textBaseline = "middle", P.opacity = r.opacity, P.strokeFirst = !0, I && (P.shadowBlur = r.textShadowBlur || 0, P.shadowColor = r.textShadowColor || "transparent", P.shadowOffsetX = r.textShadowOffsetX || 0, P.shadowOffsetY = r.textShadowOffsetY || 0), P.stroke = D, P.fill = x, D && (P.lineWidth = r.lineWidth || w, P.lineDash = r.lineDash, P.lineDashOffset = r.lineDashOffset || 0), P.font = n, $v(P, r), _ += g, A.setBoundingRect(Sf(P, h.contentWidth, h.calculatedLineHeight, C ? 0 : null));
    }
  }, e.prototype._updateRichTexts = function() {
    var r = this.style, n = this._defaultStyle, i = r.align || n.align, a = r.verticalAlign || n.verticalAlign, o = r.x || 0, s = r.y || 0;
    Nv(Nt, n.overflowRect, o, s, i, a), o = Nt.baseX, s = Nt.baseY;
    var u = Jv(r), l = iT(u, r, Nt.outerWidth, Nt.outerHeight, i), f = l.width, h = l.outerWidth, v = l.outerHeight, c = r.padding;
    this.isTruncated = !!l.isTruncated;
    var d = fi(o, h, i), y = _n(s, v, a), p = d, g = y;
    c && (p += c[3], g += c[0]);
    var m = p + f;
    sl(r) && this._renderBackground(r, r, d, y, h, v);
    for (var _ = !!r.backgroundColor, S = 0; S < l.lines.length; S++) {
      for (var b = l.lines[S], w = b.tokens, C = w.length, M = b.lineHeight, x = b.width, D = 0, I = p, L = m, A = C - 1, P = void 0; D < C && (P = w[D], !P.align || P.align === "left"); )
        this._placeToken(P, r, M, g, I, "left", _), x -= P.width, I += P.width, D++;
      for (; A >= 0 && (P = w[A], P.align === "right"); )
        this._placeToken(P, r, M, g, L, "right", _), x -= P.width, L -= P.width, A--;
      for (I += (f - (I - p) - (m - L) - x) / 2; D <= A; )
        P = w[D], this._placeToken(P, r, M, g, I + P.width / 2, "center", _), I += P.width, D++;
      g += M;
    }
  }, e.prototype._placeToken = function(r, n, i, a, o, s, u) {
    var l = n.rich[r.styleName] || {};
    l.text = r.text;
    var f = r.verticalAlign, h = a + i / 2;
    f === "top" ? h = a + r.height / 2 : f === "bottom" && (h = a + i - r.height / 2);
    var v = !r.isLineHolder && sl(l);
    v && this._renderBackground(l, n, s === "right" ? o - r.width : s === "center" ? o - r.width / 2 : o, h - r.height / 2, r.width, r.height);
    var c = !!l.backgroundColor, d = r.textPadding;
    d && (o = Qv(o, s, d), h -= r.height / 2 - d[0] - r.innerHeight / 2);
    var y = this._getOrCreateChild(cs), p = y.createStyle();
    y.useStyle(p);
    var g = this._defaultStyle, m = !1, _ = 0, S = !1, b = jv("fill" in l ? l.fill : "fill" in n ? n.fill : (m = !0, g.fill)), w = Kv("stroke" in l ? l.stroke : "stroke" in n ? n.stroke : !c && !u && (!g.autoStroke || m) ? (_ = Xv, S = !0, g.stroke) : null), C = l.textShadowBlur > 0 || n.textShadowBlur > 0;
    p.text = r.text, p.x = o, p.y = h, C && (p.shadowBlur = l.textShadowBlur || n.textShadowBlur || 0, p.shadowColor = l.textShadowColor || n.textShadowColor || "transparent", p.shadowOffsetX = l.textShadowOffsetX || n.textShadowOffsetX || 0, p.shadowOffsetY = l.textShadowOffsetY || n.textShadowOffsetY || 0), p.textAlign = s, p.textBaseline = "middle", p.font = r.font || Rr, p.opacity = gn(l.opacity, n.opacity, 1), $v(p, l), w && (p.lineWidth = gn(l.lineWidth, n.lineWidth, _), p.lineDash = G(l.lineDash, n.lineDash), p.lineDashOffset = n.lineDashOffset || 0, p.stroke = w), b && (p.fill = b), y.setBoundingRect(Sf(p, r.contentWidth, r.contentHeight, S ? 0 : null));
  }, e.prototype._renderBackground = function(r, n, i, a, o, s) {
    var u = r.backgroundColor, l = r.borderWidth, f = r.borderColor, h = u && u.image, v = u && !h, c = r.borderRadius, d = this, y, p;
    if (v || r.lineHeight || l && f) {
      y = this._getOrCreateChild(Re), y.useStyle(y.createStyle()), y.style.fill = null;
      var g = y.shape;
      g.x = i, g.y = a, g.width = o, g.height = s, g.r = c, y.dirtyShape();
    }
    if (v) {
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
    return FT(r) && (n = [
      r.fontStyle,
      r.fontWeight,
      NT(r.fontSize),
      r.fontFamily || "sans-serif"
    ].join(" ")), n && It(n) || r.textFont || r.font;
  }, e;
})(Ba), kT = { left: !0, right: 1, center: 1 }, BT = { top: 1, bottom: 1, middle: 1 }, qv = ["fontStyle", "fontWeight", "fontSize", "fontFamily"];
function NT(t) {
  return typeof t == "string" && (t.indexOf("px") !== -1 || t.indexOf("rem") !== -1 || t.indexOf("em") !== -1) ? t : isNaN(+t) ? hh + "px" : t + "px";
}
function $v(t, e) {
  for (var r = 0; r < qv.length; r++) {
    var n = qv[r], i = e[n];
    i != null && (t[n] = i);
  }
}
function FT(t) {
  return t.fontSize != null || t.fontFamily || t.fontWeight;
}
function zT(t) {
  return Zv(t), T(t.rich, Zv), t;
}
function Zv(t) {
  if (t) {
    t.font = je.makeFont(t);
    var e = t.align;
    e === "middle" && (e = "center"), t.align = e == null || kT[e] ? e : "left";
    var r = t.verticalAlign;
    r === "center" && (r = "middle"), t.verticalAlign = r == null || BT[r] ? r : "top";
    var n = t.padding;
    n && (t.padding = Us(t.padding));
  }
}
function Kv(t, e) {
  return t == null || e <= 0 || t === "transparent" || t === "none" ? null : t.image || t.colorStops ? "#000" : t;
}
function jv(t) {
  return t == null || t === "none" ? null : t.image || t.colorStops ? "#000" : t;
}
function Qv(t, e, r) {
  return e === "right" ? t - r[1] : e === "center" ? t + r[3] / 2 - r[1] / 2 : t + r[3];
}
function Jv(t) {
  var e = t.text;
  return e != null && (e += ""), e;
}
function sl(t) {
  return !!(t.backgroundColor || t.lineHeight || t.borderWidth && t.borderColor);
}
var he = de(), HT = function(t, e, r, n) {
  if (n) {
    var i = he(n);
    i.dataIndex = r, i.dataType = e, i.seriesIndex = t, i.ssrType = "chart", n.type === "group" && n.traverse(function(a) {
      var o = he(a);
      o.seriesIndex = t, o.dataIndex = r, o.dataType = e, o.ssrType = "chart";
    });
  }
}, eu = "undefined", cm = "series", vm = U(["tooltip", "label", "itemName", "itemId", "itemGroupId", "itemChildGroupId", "seriesName"]), ft = "original", ze = "arrayRows", Et = "objectRows", jt = "keyedColumns", Lr = "typedArray", dm = "unknown", Pt = "column", An = "row", VT = [
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
], pm = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      T(VT, function(r) {
        this[r] = le(e[r], e);
      }, this);
    }
    return t;
  })()
);
function GT(t, e) {
  return e.mainType === cm ? t.getViewOfSeriesModel(e) : t.getViewOfComponentModel(e);
}
var ed = 1, td = {}, gm = de(), Lh = de(), ym = 0, Eh = 1, Rh = 2, Or = ["emphasis", "blur", "select"], rd = ["normal", "emphasis", "blur", "select"], UT = 10, YT = 9, bn = "highlight", Go = "downplay", vs = "select", Tf = "unselect", ds = "toggleSelect", Oh = "selectchanged";
function Nn(t) {
  return t != null && t !== "none";
}
function tu(t, e, r) {
  t.onHoverStateChange && (t.hoverState || 0) !== r && t.onHoverStateChange(e), t.hoverState = r;
}
function mm(t) {
  tu(t, "emphasis", Rh);
}
function _m(t) {
  t.hoverState === Rh && tu(t, "normal", ym);
}
function kh(t) {
  tu(t, "blur", Eh);
}
function Sm(t) {
  t.hoverState === Eh && tu(t, "normal", ym);
}
function WT(t) {
  t.selected = !0;
}
function XT(t) {
  t.selected = !1;
}
function nd(t, e, r) {
  e(t, r);
}
function gr(t, e, r) {
  nd(t, e, r), t.isGroup && t.traverse(function(n) {
    nd(n, e, r);
  });
}
function qT(t, e, r, n) {
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
function $T(t, e, r, n) {
  var i = r && oe(r, "select") >= 0, a = !1;
  if (t instanceof ye) {
    var o = gm(t), s = i && o.selectFill || o.normalFill, u = i && o.selectStroke || o.normalStroke;
    if (Nn(s) || Nn(u)) {
      n = n || {};
      var l = n.style || {};
      l.fill === "inherit" ? (a = !0, n = R({}, n), l = R({}, l), l.fill = s) : !Nn(l.fill) && Nn(s) ? (a = !0, n = R({}, n), l = R({}, l), l.fill = sf(s)) : !Nn(l.stroke) && Nn(u) && (a || (n = R({}, n), l = R({}, l)), l.stroke = sf(u)), n.style = l;
    }
  }
  if (n && n.z2 == null) {
    a || (n = R({}, n));
    var f = t.z2EmphasisLift;
    n.z2 = t.z2 + (f ?? UT);
  }
  return n;
}
function ZT(t, e, r) {
  if (r && r.z2 == null) {
    r = R({}, r);
    var n = t.z2SelectLift;
    r.z2 = t.z2 + (n ?? YT);
  }
  return r;
}
function KT(t, e, r) {
  var n = oe(t.currentStates, e) >= 0, i = t.style.opacity, a = n ? null : qT(t, ["opacity"], e, {
    opacity: 1
  });
  r = r || {};
  var o = r.style || {};
  return o.opacity == null && (r = R({}, r), o = R({
    // Already being applied 'emphasis'. DON'T mul opacity multiple times.
    opacity: n ? i : a.opacity * 0.1
  }, o), r.style = o), r;
}
function ul(t, e) {
  var r = this.states[t];
  if (this.style) {
    if (t === "emphasis")
      return $T(this, t, e, r);
    if (t === "blur")
      return KT(this, t, r);
    if (t === "select")
      return ZT(this, t, r);
  }
  return r;
}
function jT(t) {
  t.stateProxy = ul;
  var e = t.getTextContent(), r = t.getTextGuideLine();
  e && (e.stateProxy = ul), r && (r.stateProxy = ul);
}
function id(t, e) {
  !Cm(t, e) && !t.__highByOuter && gr(t, mm);
}
function ad(t, e) {
  !Cm(t, e) && !t.__highByOuter && gr(t, _m);
}
function Cf(t, e) {
  t.__highByOuter |= 1 << (e || 0), gr(t, mm);
}
function xf(t, e) {
  !(t.__highByOuter &= ~(1 << (e || 0))) && gr(t, _m);
}
function QT(t) {
  gr(t, kh);
}
function bm(t) {
  gr(t, Sm);
}
function wm(t) {
  gr(t, WT);
}
function Tm(t) {
  gr(t, XT);
}
function Cm(t, e) {
  return t.__highDownSilentOnTouch && e.zrByTouch;
}
function xm(t) {
  var e = t.getModel(), r = [], n = [];
  e.eachComponent(function(i, a) {
    var o = Lh(a), s = GT(t, a), u = i === "series";
    !u && n.push(s), o.isBlured && (s.group.traverse(function(l) {
      Sm(l);
    }), u && r.push(a)), o.isBlured = !1;
  }), T(n, function(i) {
    i && i.toggleBlurSeries && i.toggleBlurSeries(r, !1, e);
  });
}
function Mf(t, e, r, n) {
  var i = n.getModel();
  r = r || "coordinateSystem";
  function a(l, f) {
    for (var h = 0; h < f.length; h++) {
      var v = l.getItemGraphicEl(f[h]);
      v && bm(v);
    }
  }
  if (t != null && !(!e || e === "none")) {
    var o = i.getSeriesByIndex(t), s = o.coordinateSystem;
    s && s.master && (s = s.master);
    var u = [];
    i.eachSeries(function(l) {
      var f = o === l, h = l.coordinateSystem;
      h && h.master && (h = h.master);
      var v = h && s ? h === s : f;
      if (!// Not blur other series if blurScope series
      (r === "series" && !f || r === "coordinateSystem" && !v || e === "series" && f)) {
        var c = n.getViewOfSeriesModel(l);
        if (c.group.traverse(function(p) {
          p.__highByOuter && f && e === "self" || kh(p);
        }), Ze(e))
          a(l.getData(), e);
        else if (F(e))
          for (var d = me(e), y = 0; y < d.length; y++)
            a(l.getData(d[y]), e[d[y]]);
        u.push(l), Lh(l).isBlured = !0;
      }
    }), i.eachComponent(function(l, f) {
      if (l !== "series") {
        var h = n.getViewOfComponentModel(f);
        h && h.toggleBlurSeries && h.toggleBlurSeries(u, !0, i);
      }
    });
  }
}
function Df(t, e, r) {
  if (!(t == null || e == null)) {
    var n = r.getModel().getComponent(t, e);
    if (n) {
      Lh(n).isBlured = !0;
      var i = r.getViewOfComponentModel(n);
      !i || !i.focusBlurEnabled || i.group.traverse(function(a) {
        kh(a);
      });
    }
  }
}
function JT(t, e, r) {
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
      Mf(n, l.focus, l.blurScope, r);
    } else {
      var f = t.get(["emphasis", "focus"]), h = t.get(["emphasis", "blurScope"]);
      f != null && Mf(n, f, h, r);
    }
  }
}
function Bh(t, e, r, n) {
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
function eC(t, e, r) {
  var n = he(t), i = Bh(n.componentMainType, n.componentIndex, n.componentHighDownName, r), a = i.dispatchers, o = i.focusSelf;
  a ? (o && Df(n.componentMainType, n.componentIndex, r), T(a, function(s) {
    return id(s, e);
  })) : (Mf(n.seriesIndex, n.focus, n.blurScope, r), n.focus === "self" && Df(n.componentMainType, n.componentIndex, r), id(t, e));
}
function tC(t, e, r) {
  xm(r);
  var n = he(t), i = Bh(n.componentMainType, n.componentIndex, n.componentHighDownName, r).dispatchers;
  i ? T(i, function(a) {
    return ad(a, e);
  }) : ad(t, e);
}
function rC(t, e, r) {
  if (Af(e)) {
    var n = e.dataType, i = t.getData(n), a = Oa(i, e);
    B(a) || (a = [a]), t[e.type === ds ? "toggleSelect" : e.type === vs ? "select" : "unselect"](a, n);
  }
}
function od(t) {
  var e = t.getAllData();
  T(e, function(r) {
    var n = r.data, i = r.type;
    n.eachItemGraphicEl(function(a, o) {
      t.isSelected(o, i) ? wm(a) : Tm(a);
    });
  });
}
function nC(t) {
  var e = [];
  return t.eachSeries(function(r) {
    var n = r.getAllData();
    T(n, function(i) {
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
function ps(t, e, r) {
  Mm(t, !0), gr(t, jT), aC(t, e, r);
}
function iC(t) {
  Mm(t, !1);
}
function GE(t, e, r, n) {
  n ? iC(t) : ps(t, e, r);
}
function aC(t, e, r) {
  var n = he(t);
  e != null ? (n.focus = e, n.blurScope = r) : n.focus && (n.focus = null);
}
var sd = ["emphasis", "blur", "select"], oC = {
  itemStyle: "getItemStyle",
  lineStyle: "getLineStyle",
  areaStyle: "getAreaStyle"
};
function UE(t, e, r, n) {
  r = r || "itemStyle";
  for (var i = 0; i < sd.length; i++) {
    var a = sd[i], o = e.getModel([a, r]), s = t.ensureState(a);
    s.style = o[oC[r]]();
  }
}
function Mm(t, e) {
  var r = e === !1, n = t;
  t.highDownSilentOnTouch && (n.__highDownSilentOnTouch = t.highDownSilentOnTouch), (!r || n.__highDownDispatcher) && (n.__highByOuter = n.__highByOuter || 0, n.__highDownDispatcher = !r);
}
function If(t) {
  return !!(t && t.__highDownDispatcher);
}
function sC(t) {
  var e = td[t];
  return e == null && ed <= 32 && (e = td[t] = ed++), e;
}
function Af(t) {
  var e = t.type;
  return e === vs || e === Tf || e === ds;
}
function ud(t) {
  var e = t.type;
  return e === bn || e === Go;
}
function uC(t) {
  var e = gm(t);
  e.normalFill = t.style.fill, e.normalStroke = t.style.stroke;
  var r = t.states.select || {};
  e.selectFill = r.style && r.style.fill || null, e.selectStroke = r.style && r.style.stroke || null;
}
var Fn = vi.CMD, lC = [[], [], []], ld = Math.sqrt, fC = Math.atan2;
function hC(t, e) {
  if (e) {
    var r = t.data, n = t.len(), i, a, o, s, u, l, f = Fn.M, h = Fn.C, v = Fn.L, c = Fn.R, d = Fn.A, y = Fn.Q;
    for (o = 0, s = 0; o < n; ) {
      switch (i = r[o++], s = o, a = 0, i) {
        case f:
          a = 1;
          break;
        case v:
          a = 1;
          break;
        case h:
          a = 3;
          break;
        case y:
          a = 2;
          break;
        case d:
          var p = e[4], g = e[5], m = ld(e[0] * e[0] + e[1] * e[1]), _ = ld(e[2] * e[2] + e[3] * e[3]), S = fC(-e[1] / _, e[0] / m);
          r[o] *= m, r[o++] += p, r[o] *= _, r[o++] += g, r[o++] *= m, r[o++] *= _, r[o++] += S, r[o++] += S, o += 2, s = o;
          break;
        case c:
          l[0] = r[o++], l[1] = r[o++], ot(l, l, e), r[s++] = l[0], r[s++] = l[1], l[0] += r[o++], l[1] += r[o++], ot(l, l, e), r[s++] = l[0], r[s++] = l[1];
      }
      for (u = 0; u < a; u++) {
        var b = lC[u];
        b[0] = r[o++], b[1] = r[o++], ot(b, b, e), r[s++] = b[0], r[s++] = b[1];
      }
    }
    t.increaseVersion();
  }
}
var ll = Math.sqrt, ro = Math.sin, no = Math.cos, Ri = Math.PI;
function fd(t) {
  return Math.sqrt(t[0] * t[0] + t[1] * t[1]);
}
function Pf(t, e) {
  return (t[0] * e[0] + t[1] * e[1]) / (fd(t) * fd(e));
}
function hd(t, e) {
  return (t[0] * e[1] < t[1] * e[0] ? -1 : 1) * Math.acos(Pf(t, e));
}
function cd(t, e, r, n, i, a, o, s, u, l, f) {
  var h = u * (Ri / 180), v = no(h) * (t - r) / 2 + ro(h) * (e - n) / 2, c = -1 * ro(h) * (t - r) / 2 + no(h) * (e - n) / 2, d = v * v / (o * o) + c * c / (s * s);
  d > 1 && (o *= ll(d), s *= ll(d));
  var y = (i === a ? -1 : 1) * ll((o * o * (s * s) - o * o * (c * c) - s * s * (v * v)) / (o * o * (c * c) + s * s * (v * v))) || 0, p = y * o * c / s, g = y * -s * v / o, m = (t + r) / 2 + no(h) * p - ro(h) * g, _ = (e + n) / 2 + ro(h) * p + no(h) * g, S = hd([1, 0], [(v - p) / o, (c - g) / s]), b = [(v - p) / o, (c - g) / s], w = [(-1 * v - p) / o, (-1 * c - g) / s], C = hd(b, w);
  if (Pf(b, w) <= -1 && (C = Ri), Pf(b, w) >= 1 && (C = 0), C < 0) {
    var M = Math.round(C / Ri * 1e6) / 1e6;
    C = Ri * 2 + M % 2 * Ri;
  }
  f.addData(l, m, _, o, s, S, C, h, a);
}
var cC = /([mlvhzcqtsa])([^mlvhzcqtsa]*)/ig, vC = /-?([0-9]*\.)?[0-9]+([eE]-?[0-9]+)?/g;
function dC(t) {
  var e = new vi();
  if (!t)
    return e;
  var r = 0, n = 0, i = r, a = n, o, s = vi.CMD, u = t.match(cC);
  if (!u)
    return e;
  for (var l = 0; l < u.length; l++) {
    for (var f = u[l], h = f.charAt(0), v = void 0, c = f.match(vC) || [], d = c.length, y = 0; y < d; y++)
      c[y] = parseFloat(c[y]);
    for (var p = 0; p < d; ) {
      var g = void 0, m = void 0, _ = void 0, S = void 0, b = void 0, w = void 0, C = void 0, M = r, x = n, D = void 0, I = void 0;
      switch (h) {
        case "l":
          r += c[p++], n += c[p++], v = s.L, e.addData(v, r, n);
          break;
        case "L":
          r = c[p++], n = c[p++], v = s.L, e.addData(v, r, n);
          break;
        case "m":
          r += c[p++], n += c[p++], v = s.M, e.addData(v, r, n), i = r, a = n, h = "l";
          break;
        case "M":
          r = c[p++], n = c[p++], v = s.M, e.addData(v, r, n), i = r, a = n, h = "L";
          break;
        case "h":
          r += c[p++], v = s.L, e.addData(v, r, n);
          break;
        case "H":
          r = c[p++], v = s.L, e.addData(v, r, n);
          break;
        case "v":
          n += c[p++], v = s.L, e.addData(v, r, n);
          break;
        case "V":
          n = c[p++], v = s.L, e.addData(v, r, n);
          break;
        case "C":
          v = s.C, e.addData(v, c[p++], c[p++], c[p++], c[p++], c[p++], c[p++]), r = c[p - 2], n = c[p - 1];
          break;
        case "c":
          v = s.C, e.addData(v, c[p++] + r, c[p++] + n, c[p++] + r, c[p++] + n, c[p++] + r, c[p++] + n), r += c[p - 2], n += c[p - 1];
          break;
        case "S":
          g = r, m = n, D = e.len(), I = e.data, o === s.C && (g += r - I[D - 4], m += n - I[D - 3]), v = s.C, M = c[p++], x = c[p++], r = c[p++], n = c[p++], e.addData(v, g, m, M, x, r, n);
          break;
        case "s":
          g = r, m = n, D = e.len(), I = e.data, o === s.C && (g += r - I[D - 4], m += n - I[D - 3]), v = s.C, M = r + c[p++], x = n + c[p++], r += c[p++], n += c[p++], e.addData(v, g, m, M, x, r, n);
          break;
        case "Q":
          M = c[p++], x = c[p++], r = c[p++], n = c[p++], v = s.Q, e.addData(v, M, x, r, n);
          break;
        case "q":
          M = c[p++] + r, x = c[p++] + n, r += c[p++], n += c[p++], v = s.Q, e.addData(v, M, x, r, n);
          break;
        case "T":
          g = r, m = n, D = e.len(), I = e.data, o === s.Q && (g += r - I[D - 4], m += n - I[D - 3]), r = c[p++], n = c[p++], v = s.Q, e.addData(v, g, m, r, n);
          break;
        case "t":
          g = r, m = n, D = e.len(), I = e.data, o === s.Q && (g += r - I[D - 4], m += n - I[D - 3]), r += c[p++], n += c[p++], v = s.Q, e.addData(v, g, m, r, n);
          break;
        case "A":
          _ = c[p++], S = c[p++], b = c[p++], w = c[p++], C = c[p++], M = r, x = n, r = c[p++], n = c[p++], v = s.A, cd(M, x, r, n, w, C, _, S, b, v, e);
          break;
        case "a":
          _ = c[p++], S = c[p++], b = c[p++], w = c[p++], C = c[p++], M = r, x = n, r += c[p++], n += c[p++], v = s.A, cd(M, x, r, n, w, C, _, S, b, v, e);
          break;
      }
    }
    (h === "z" || h === "Z") && (v = s.Z, e.addData(v), r = i, n = a), o = v;
  }
  return e.toStatic(), e;
}
var Dm = (function(t) {
  z(e, t);
  function e() {
    return t !== null && t.apply(this, arguments) || this;
  }
  return e.prototype.applyTransform = function(r) {
  }, e;
})(ye);
function Im(t) {
  return t.setData != null;
}
function Am(t, e) {
  var r = dC(t), n = R({}, e);
  return n.buildPath = function(i) {
    var a = Im(i);
    if (a && i.canSave()) {
      i.appendPath(r);
      var o = i.getContext();
      o && i.rebuildPath(o, 1);
    } else {
      var o = a ? i.getContext() : i;
      o && r.rebuildPath(o, 1);
    }
  }, n.applyTransform = function(i) {
    hC(r, i), this.dirtyShape();
  }, n;
}
function pC(t, e) {
  return new Dm(Am(t, e));
}
function gC(t, e) {
  var r = Am(t, e), n = (function(i) {
    z(a, i);
    function a(o) {
      var s = i.call(this, o) || this;
      return s.applyTransform = r.applyTransform, s.buildPath = r.buildPath, s;
    }
    return a;
  })(Dm);
  return n;
}
function yC(t, e) {
  for (var r = [], n = t.length, i = 0; i < n; i++) {
    var a = t[i];
    r.push(a.getUpdatedPathProxy(!0));
  }
  var o = new ye(e);
  return o.createPathProxy(), o.buildPath = function(s) {
    if (Im(s)) {
      s.appendPath(r);
      var u = s.getContext();
      u && s.rebuildPath(u, 1);
    }
  }, o;
}
var mC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r = 0;
  }
  return t;
})(), Na = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new mC();
  }, e.prototype.buildPath = function(r, n) {
    r.moveTo(n.cx + n.r, n.cy), r.arc(n.cx, n.cy, n.r, 0, Math.PI * 2);
  }, e;
})(ye);
Na.prototype.type = "circle";
var _C = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.rx = 0, this.ry = 0;
  }
  return t;
})(), ru = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new _C();
  }, e.prototype.buildPath = function(r, n) {
    var i = 0.5522848, a = n.cx, o = n.cy, s = n.rx, u = n.ry, l = s * i, f = u * i;
    r.moveTo(a - s, o), r.bezierCurveTo(a - s, o - f, a - l, o - u, a, o - u), r.bezierCurveTo(a + l, o - u, a + s, o - f, a + s, o), r.bezierCurveTo(a + s, o + f, a + l, o + u, a, o + u), r.bezierCurveTo(a - l, o + u, a - s, o + f, a - s, o), r.closePath();
  }, e;
})(ye);
ru.prototype.type = "ellipse";
var Pm = Math.PI, fl = Pm * 2, Jr = Math.sin, zn = Math.cos, SC = Math.acos, Oe = Math.atan2, vd = Math.abs, na = Math.sqrt, Ki = Math.max, Ft = Math.min, xt = 1e-4;
function bC(t, e, r, n, i, a, o, s) {
  var u = r - t, l = n - e, f = o - i, h = s - a, v = h * u - f * l;
  if (!(v * v < xt))
    return v = (f * (e - a) - h * (t - i)) / v, [t + v * u, e + v * l];
}
function io(t, e, r, n, i, a, o) {
  var s = t - r, u = e - n, l = (o ? a : -a) / na(s * s + u * u), f = l * u, h = -l * s, v = t + f, c = e + h, d = r + f, y = n + h, p = (v + d) / 2, g = (c + y) / 2, m = d - v, _ = y - c, S = m * m + _ * _, b = i - a, w = v * y - d * c, C = (_ < 0 ? -1 : 1) * na(Ki(0, b * b * S - w * w)), M = (w * _ - m * C) / S, x = (-w * m - _ * C) / S, D = (w * _ + m * C) / S, I = (-w * m + _ * C) / S, L = M - p, A = x - g, P = D - p, k = I - g;
  return L * L + A * A > P * P + k * k && (M = D, x = I), {
    cx: M,
    cy: x,
    x0: -f,
    y0: -h,
    x1: M * (i / b - 1),
    y1: x * (i / b - 1)
  };
}
function wC(t) {
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
function TC(t, e) {
  var r, n = Ki(e.r, 0), i = Ki(e.r0 || 0, 0), a = n > 0, o = i > 0;
  if (!(!a && !o)) {
    if (a || (n = i, i = 0), i > n) {
      var s = n;
      n = i, i = s;
    }
    var u = e.startAngle, l = e.endAngle;
    if (!(isNaN(u) || isNaN(l))) {
      var f = e.cx, h = e.cy, v = !!e.clockwise, c = vd(l - u), d = c > fl && c % fl;
      if (d > xt && (c = d), !(n > xt))
        t.moveTo(f, h);
      else if (c > fl - xt)
        t.moveTo(f + n * zn(u), h + n * Jr(u)), t.arc(f, h, n, u, l, !v), i > xt && (t.moveTo(f + i * zn(l), h + i * Jr(l)), t.arc(f, h, i, l, u, v));
      else {
        var y = void 0, p = void 0, g = void 0, m = void 0, _ = void 0, S = void 0, b = void 0, w = void 0, C = void 0, M = void 0, x = void 0, D = void 0, I = void 0, L = void 0, A = void 0, P = void 0, k = n * zn(u), O = n * Jr(u), V = i * zn(l), K = i * Jr(l), J = c > xt;
        if (J) {
          var ee = e.cornerRadius;
          ee && (r = wC(ee), y = r[0], p = r[1], g = r[2], m = r[3]);
          var Q = vd(n - i) / 2;
          if (_ = Ft(Q, g), S = Ft(Q, m), b = Ft(Q, y), w = Ft(Q, p), x = C = Ki(_, S), D = M = Ki(b, w), (C > xt || M > xt) && (I = n * zn(l), L = n * Jr(l), A = i * zn(u), P = i * Jr(u), c < Pm)) {
            var re = bC(k, O, A, P, I, L, V, K);
            if (re) {
              var ne = k - re[0], q = O - re[1], se = I - re[0], Se = L - re[1], tt = 1 / Jr(SC((ne * se + q * Se) / (na(ne * ne + q * q) * na(se * se + Se * Se))) / 2), Qt = na(re[0] * re[0] + re[1] * re[1]);
              x = Ft(C, (n - Qt) / (tt + 1)), D = Ft(M, (i - Qt) / (tt - 1));
            }
          }
        }
        if (!J)
          t.moveTo(f + k, h + O);
        else if (x > xt) {
          var rt = Ft(g, x), Ae = Ft(m, x), $ = io(A, P, k, O, n, rt, v), te = io(I, L, V, K, n, Ae, v);
          t.moveTo(f + $.cx + $.x0, h + $.cy + $.y0), x < C && rt === Ae ? t.arc(f + $.cx, h + $.cy, x, Oe($.y0, $.x0), Oe(te.y0, te.x0), !v) : (rt > 0 && t.arc(f + $.cx, h + $.cy, rt, Oe($.y0, $.x0), Oe($.y1, $.x1), !v), t.arc(f, h, n, Oe($.cy + $.y1, $.cx + $.x1), Oe(te.cy + te.y1, te.cx + te.x1), !v), Ae > 0 && t.arc(f + te.cx, h + te.cy, Ae, Oe(te.y1, te.x1), Oe(te.y0, te.x0), !v));
        } else
          t.moveTo(f + k, h + O), t.arc(f, h, n, u, l, !v);
        if (!(i > xt) || !J)
          t.lineTo(f + V, h + K);
        else if (D > xt) {
          var rt = Ft(y, D), Ae = Ft(p, D), $ = io(V, K, I, L, i, -Ae, v), te = io(k, O, A, P, i, -rt, v);
          t.lineTo(f + $.cx + $.x0, h + $.cy + $.y0), D < M && rt === Ae ? t.arc(f + $.cx, h + $.cy, D, Oe($.y0, $.x0), Oe(te.y0, te.x0), !v) : (Ae > 0 && t.arc(f + $.cx, h + $.cy, Ae, Oe($.y0, $.x0), Oe($.y1, $.x1), !v), t.arc(f, h, i, Oe($.cy + $.y1, $.cx + $.x1), Oe(te.cy + te.y1, te.cx + te.x1), v), rt > 0 && t.arc(f + te.cx, h + te.cy, rt, Oe(te.y1, te.x1), Oe(te.y0, te.x0), !v));
        } else
          t.lineTo(f + V, h + K), t.arc(f, h, i, l, u, v);
      }
      t.closePath();
    }
  }
}
var CC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r0 = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0, this.cornerRadius = 0;
  }
  return t;
})(), nu = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new CC();
  }, e.prototype.buildPath = function(r, n) {
    TC(r, n);
  }, e.prototype.isZeroArea = function() {
    return this.shape.startAngle === this.shape.endAngle || this.shape.r === this.shape.r0;
  }, e;
})(ye);
nu.prototype.type = "sector";
var xC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r = 0, this.r0 = 0;
  }
  return t;
})(), iu = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new xC();
  }, e.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.PI * 2;
    r.moveTo(i + n.r, a), r.arc(i, a, n.r, 0, o, !1), r.moveTo(i + n.r0, a), r.arc(i, a, n.r0, 0, o, !0);
  }, e;
})(ye);
iu.prototype.type = "ring";
function MC(t, e, r, n) {
  var i = [], a = [], o = [], s = [], u, l, f, h;
  if (n) {
    f = [1 / 0, 1 / 0], h = [-1 / 0, -1 / 0];
    for (var v = 0, c = t.length; v < c; v++)
      Cr(f, f, t[v]), xr(h, h, t[v]);
    Cr(f, f, n[0]), xr(h, h, n[1]);
  }
  for (var v = 0, c = t.length; v < c; v++) {
    var d = t[v];
    if (r)
      u = t[v ? v - 1 : c - 1], l = t[(v + 1) % c];
    else if (v === 0 || v === c - 1) {
      i.push(cy(t[v]));
      continue;
    } else
      u = t[v - 1], l = t[v + 1];
    vy(a, l, u), Ro(a, a, e);
    var y = ts(d, u), p = ts(d, l), g = y + p;
    g !== 0 && (y /= g, p /= g), Ro(o, a, -y), Ro(s, a, p);
    var m = jl([], d, o), _ = jl([], d, s);
    n && (xr(m, m, f), Cr(m, m, h), xr(_, _, f), Cr(_, _, h)), i.push(m), i.push(_);
  }
  return r && i.push(i.shift()), i;
}
function Lm(t, e, r) {
  var n = e.smooth, i = e.points;
  if (i && i.length >= 2) {
    if (n) {
      var a = MC(i, n, r, e.smoothConstraint);
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
var DC = /* @__PURE__ */ (function() {
  function t() {
    this.points = null, this.smooth = 0, this.smoothConstraint = null;
  }
  return t;
})(), au = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultShape = function() {
    return new DC();
  }, e.prototype.buildPath = function(r, n) {
    Lm(r, n, !0);
  }, e;
})(ye);
au.prototype.type = "polygon";
var IC = /* @__PURE__ */ (function() {
  function t() {
    this.points = null, this.percent = 1, this.smooth = 0, this.smoothConstraint = null;
  }
  return t;
})(), ou = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new IC();
  }, e.prototype.buildPath = function(r, n) {
    Lm(r, n, !1);
  }, e;
})(ye);
ou.prototype.type = "polyline";
var AC = {}, PC = /* @__PURE__ */ (function() {
  function t() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.percent = 1;
  }
  return t;
})(), Pn = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new PC();
  }, e.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = fm(AC, n, this.style);
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
var Qe = [], LC = /* @__PURE__ */ (function() {
  function t() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.cpx1 = 0, this.cpy1 = 0, this.percent = 1;
  }
  return t;
})();
function dd(t, e, r) {
  var n = t.cpx2, i = t.cpy2;
  return n != null || i != null ? [
    (r ? vv : Pe)(t.x1, t.cpx1, t.cpx2, t.x2, e),
    (r ? vv : Pe)(t.y1, t.cpy1, t.cpy2, t.y2, e)
  ] : [
    (r ? dv : Be)(t.x1, t.cpx1, t.x2, e),
    (r ? dv : Be)(t.y1, t.cpy1, t.y2, e)
  ];
}
var su = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new LC();
  }, e.prototype.buildPath = function(r, n) {
    var i = n.x1, a = n.y1, o = n.x2, s = n.y2, u = n.cpx1, l = n.cpy1, f = n.cpx2, h = n.cpy2, v = n.percent;
    v !== 0 && (r.moveTo(i, a), f == null || h == null ? (v < 1 && (ss(i, u, o, v, Qe), u = Qe[1], o = Qe[2], ss(a, l, s, v, Qe), l = Qe[1], s = Qe[2]), r.quadraticCurveTo(u, l, o, s)) : (v < 1 && (os(i, u, f, o, v, Qe), u = Qe[1], f = Qe[2], o = Qe[3], os(a, l, h, s, v, Qe), l = Qe[1], h = Qe[2], s = Qe[3]), r.bezierCurveTo(u, l, f, h, o, s)));
  }, e.prototype.pointAt = function(r) {
    return dd(this.shape, r, !1);
  }, e.prototype.tangentAt = function(r) {
    var n = dd(this.shape, r, !0);
    return dy(n, n);
  }, e;
})(ye);
su.prototype.type = "bezier-curve";
var EC = /* @__PURE__ */ (function() {
  function t() {
    this.cx = 0, this.cy = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0;
  }
  return t;
})(), Fa = (function(t) {
  z(e, t);
  function e(r) {
    return t.call(this, r) || this;
  }
  return e.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, e.prototype.getDefaultShape = function() {
    return new EC();
  }, e.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.max(n.r, 0), s = n.startAngle, u = n.endAngle, l = n.clockwise, f = Math.cos(s), h = Math.sin(s);
    r.moveTo(f * o + i, h * o + a), r.arc(i, a, o, s, u, !l);
  }, e;
})(ye);
Fa.prototype.type = "arc";
var Em = (function(t) {
  z(e, t);
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
})(ye), Rm = (function() {
  function t(e) {
    this.colorStops = e || [];
  }
  return t.prototype.addColorStop = function(e, r) {
    this.colorStops.push({
      offset: e,
      color: r
    });
  }, t;
})(), Om = (function(t) {
  z(e, t);
  function e(r, n, i, a, o, s) {
    var u = t.call(this, o) || this;
    return u.x = r ?? 0, u.y = n ?? 0, u.x2 = i ?? 1, u.y2 = a ?? 0, u.type = "linear", u.global = s || !1, u;
  }
  return e;
})(Rm), km = (function(t) {
  z(e, t);
  function e(r, n, i, a, o) {
    var s = t.call(this, a) || this;
    return s.x = r ?? 0.5, s.y = n ?? 0.5, s.r = i ?? 0.5, s.type = "radial", s.global = o || !1, s;
  }
  return e;
})(Rm), hl = Math.min, RC = Math.max, ao = Math.abs, en = [0, 0], tn = [0, 0], De = Sy(), oo = De.minTv, so = De.maxTv, Bm = (function() {
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
        hl(u, l) > so.len() && (u < l ? ue.scale(so, s, -u * i) : ue.scale(so, s, l * i));
      } else if (!n) {
        var u = ao(tn[0] - en[1]), l = ao(en[0] - tn[1]);
        (De.useDir || hl(u, l) < oo.len()) && ((u < l || !De.bidirectional) && (ue.scale(oo, s, u * i), De.useDir && De.calcDirMTV()), (u >= l || !De.bidirectional) && (ue.scale(oo, s, -l * i), De.useDir && De.calcDirMTV()));
      }
    }
    return a;
  }, t.prototype._getProjMinMaxOnAxis = function(e, r, n) {
    for (var i = this._axes[e], a = this._origin, o = r[0].dot(i) + a[e], s = o, u = o, l = 1; l < r.length; l++) {
      var f = r[l].dot(i) + a[e];
      s = hl(f, s), u = RC(f, u);
    }
    n[0] = s + De.touchThreshold, n[1] = u - De.touchThreshold, De.negativeSize = n[1] < n[0];
  }, t;
})(), Nm = 0, OC = 1, kC = 2, BC = 1, Uo = 0, NC = [], Fm = (function(t) {
  z(e, t);
  function e() {
    var r = t !== null && t.apply(this, arguments) || this;
    return r.notClear = !0, r.incremental = OC, r._displayables = [], r._temporaryDisplayables = [], r._cursor = 0, r;
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
        i.needLocalTransform() && a.applyTransform(i.getLocalTransform(NC)), r.union(a);
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
})(Ba), FC = de();
function zC(t, e, r, n, i) {
  var a;
  if (e && e.ecModel) {
    var o = e.ecModel.getUpdatePayload();
    a = o && o.animation;
  }
  var s = e && e.isAnimationEnabled(), u = t === "update";
  if (s) {
    var l = void 0, f = void 0, h = void 0;
    n ? (l = G(n.duration, 200), f = G(n.easing, "cubicOut"), h = 0) : (l = e.getShallow(u ? "animationDurationUpdate" : "animationDuration"), f = e.getShallow(u ? "animationEasingUpdate" : "animationEasing"), h = e.getShallow(u ? "animationDelayUpdate" : "animationDelay")), a && (a.duration != null && (l = a.duration), a.easing != null && (f = a.easing), a.delay != null && (h = a.delay)), X(h) && (h = h(r, i)), X(l) && (l = l(r));
    var v = {
      duration: l || 0,
      delay: h,
      easing: f
    };
    return v;
  } else
    return null;
}
function Nh(t, e, r, n, i, a, o) {
  var s = !1, u;
  X(i) ? (o = a, a = i, i = null) : F(i) && (a = i.cb, o = i.during, s = i.isFrom, u = i.removeOpt, i = i.dataIndex);
  var l = t === "leave";
  l || e.stopAnimation("leave");
  var f = zC(t, n, i, l ? u || {} : null, n && n.getAnimationDelayParams ? n.getAnimationDelayParams(e, i) : null);
  if (f && f.duration > 0) {
    var h = f.duration, v = f.delay, c = f.easing, d = {
      duration: h,
      delay: v || 0,
      easing: c,
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
function mi(t, e, r, n, i, a) {
  Nh("update", t, e, r, n, i, a);
}
function Fh(t, e, r, n, i, a) {
  Nh("enter", t, e, r, n, i, a);
}
function ia(t) {
  if (!t.__zr)
    return !0;
  for (var e = 0; e < t.animators.length; e++) {
    var r = t.animators[e];
    if (r.scope === "leave")
      return !0;
  }
  return !1;
}
function zm(t, e, r, n, i, a) {
  ia(t) || Nh("leave", t, e, r, n, i, a);
}
function pd(t, e, r, n) {
  t.removeTextContent(), t.removeTextGuideLine(), zm(t, {
    style: {
      opacity: 0
    }
  }, e, r, n);
}
function HC(t, e, r) {
  function n() {
    t.parent && t.parent.remove(t);
  }
  t.isGroup ? t.traverse(function(i) {
    i.isGroup || pd(i, e, r, n);
  }) : pd(t, e, r, n);
}
function YE(t) {
  FC(t).oldStyle = t.style;
}
var Lf = {}, Hm = ["x", "y"], Vm = ["width", "height"], Gm = 0, Um = 1, Ym = 2;
function Wm(t) {
  return ye.extend(t);
}
var VC = gC;
function Xm(t, e) {
  return VC(t, e);
}
function Tt(t, e) {
  Lf[t] = e;
}
function qm(t) {
  if (Lf.hasOwnProperty(t))
    return Lf[t];
}
function uu(t, e, r, n) {
  var i = pC(t, e);
  return r && (n === "center" && (r = $m(r, i.getBoundingRect())), Hh(i, r)), i;
}
function zh(t, e, r) {
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
        n.setStyle($m(e, a));
      }
    }
  });
  return n;
}
function $m(t, e) {
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
var Zm = yC;
function Hh(t, e) {
  if (t.applyTransform) {
    var r = t.getBoundingRect(), n = r.calculateTransform(e);
    t.applyTransform(n);
  }
}
function Vh(t, e) {
  return fm(t, t, {
    lineWidth: e
  }), t;
}
function GC(t, e) {
  return hm(t, t, e), t;
}
var UC = vn;
function Km(t, e) {
  for (var r = Mn([]); t && t !== e; )
    mn(r, t.getLocalTransform(), r), t = t.parent;
  return r;
}
function Gh(t, e, r) {
  return e && !Ze(e) && (e = Ea.getLocalTransform(e)), r && (e = yi([], e)), ot([], t, e);
}
function YC(t, e, r) {
  var n = e[4] === 0 || e[5] === 0 || e[0] === 0 ? 1 : Fe(2 * e[4] / e[0]), i = e[4] === 0 || e[5] === 0 || e[2] === 0 ? 1 : Fe(2 * e[4] / e[2]), a = [t === "left" ? -n : t === "right" ? n : 0, t === "top" ? -i : t === "bottom" ? i : 0];
  return a = Gh(a, e, r), Fe(a[0]) > Fe(a[1]) ? a[0] > 0 ? "right" : "left" : a[1] > 0 ? "bottom" : "top";
}
function gd(t) {
  return !t.isGroup;
}
function WC(t) {
  return t.shape != null;
}
function XC(t, e, r) {
  if (!t || !e)
    return;
  function n(o) {
    var s = {};
    return o.traverse(function(u) {
      gd(u) && u.anid && (s[u.anid] = u);
    }), s;
  }
  function i(o) {
    var s = {
      x: o.x,
      y: o.y,
      rotation: o.rotation
    };
    return WC(o) && (s.shape = Z(o.shape)), s;
  }
  var a = n(t);
  e.traverse(function(o) {
    if (gd(o) && o.anid) {
      var s = a[o.anid];
      if (s) {
        var u = i(o);
        o.attr(i(s)), mi(o, u, r, he(o).dataIndex);
      }
    }
  });
}
function jm(t, e) {
  return H(t, function(r) {
    var n = r[0];
    n = ge(n, e.x), n = et(n, e.x + e.width);
    var i = r[1];
    return i = ge(i, e.y), i = et(i, e.y + e.height), [n, i];
  });
}
function Qm(t, e) {
  var r = ge(t.x, e.x), n = et(t.x + t.width, e.x + e.width), i = ge(t.y, e.y), a = et(t.y + t.height, e.y + e.height);
  if (n >= r && a >= i)
    return {
      x: r,
      y: i,
      width: n - r,
      height: a - i
    };
}
function lu(t, e, r) {
  var n = R({
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
    return t.indexOf("image://") === 0 ? (i.image = t.slice(8), _e(i, r), new Nr(n)) : uu(t.replace("path://", ""), n, r, "center");
}
function qC(t, e, r, n, i) {
  for (var a = 0, o = i[i.length - 1]; a < i.length; a++) {
    var s = i[a];
    if (Jm(t, e, r, n, s[0], s[1], o[0], o[1]))
      return !0;
    o = s;
  }
}
function Jm(t, e, r, n, i, a, o, s) {
  var u = r - t, l = n - e, f = o - i, h = s - a, v = cl(f, h, u, l);
  if ($C(v))
    return !1;
  var c = t - i, d = e - a, y = cl(c, d, u, l) / v;
  if (y < 0 || y > 1)
    return !1;
  var p = cl(c, d, f, h) / v;
  return !(p < 0 || p > 1);
}
function cl(t, e, r, n) {
  return t * n - r * e;
}
function $C(t) {
  return t <= 1e-6 && t >= -1e-6;
}
function Ef(t, e, r, n, i) {
  return e == null || (ve(e) ? fe[0] = fe[1] = fe[2] = fe[3] = e : (fe[0] = e[0], fe[1] = e[1], fe[2] = e[2], fe[3] = e[3]), n && (fe[0] = ge(0, fe[0]), fe[1] = ge(0, fe[1]), fe[2] = ge(0, fe[2]), fe[3] = ge(0, fe[3])), r && (fe[0] = -fe[0], fe[1] = -fe[1], fe[2] = -fe[2], fe[3] = -fe[3]), yd(t, fe, "x", "width", 3, 1, i && i[0] || 0), yd(t, fe, "y", "height", 0, 2, i && i[1] || 0)), t;
}
var fe = [0, 0, 0, 0];
function yd(t, e, r, n, i, a, o) {
  var s = e[a] + e[i], u = t[n];
  t[n] += s, o = ge(0, et(o, u)), t[n] < o ? (t[n] = o, t[r] += e[i] >= 0 ? -e[i] : e[a] >= 0 ? u + e[a] : Fe(s) > 1e-8 ? (u - o) * e[i] / s : 0) : t[r] -= e[i];
}
function fu(t) {
  var e = t.itemTooltipOption, r = t.componentModel, n = t.itemName, i = N(e) ? {
    formatter: e
  } : e, a = r.mainType, o = r.componentIndex, s = {
    componentType: a,
    name: n,
    $vars: ["name"]
  };
  s[a + "Index"] = o;
  var u = t.formatterParamsExtra;
  u && T(me(u), function(f) {
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
function Rf(t, e) {
  var r;
  t.isGroup && (r = e(t)), r || t.traverse(e);
}
function e_(t, e) {
  if (t)
    if (B(t))
      for (var r = 0; r < t.length; r++)
        Rf(t[r], e);
    else
      Rf(t, e);
}
function Uh(t) {
  return !t || Fe(t[1]) < uo && Fe(t[2]) < uo || Fe(t[0]) < uo && Fe(t[3]) < uo;
}
var uo = 1e-5;
function Sa(t, e) {
  return t ? Y.copy(t, e) : e.clone();
}
function Yh(t, e) {
  return e ? La(t || wt(), e) : void 0;
}
function t_(t) {
  return {
    z: t.get("z") || 0,
    zlevel: t.get("zlevel") || 0
  };
}
function ZC(t) {
  var e = -1 / 0, r = 1 / 0;
  Rf(t, function(a) {
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
function r_(t, e, r) {
  n_(t, e, r, -1 / 0);
}
function n_(t, e, r, n) {
  if (t.ignoreModelZ)
    return n;
  var i = t.getTextContent(), a = t.getTextGuideLine(), o = t.isGroup;
  if (o)
    for (var s = t.childrenRef(), u = 0; u < s.length; u++)
      n = ge(n_(s[u], e, r, n), n);
  else
    t.z = e, t.zlevel = r, n = ge(t.z2 || 0, n);
  if (i && (i.z = e, i.zlevel = r, isFinite(n) && (i.z2 = n + 2)), a) {
    var l = t.textGuideLineConfig;
    a.z = e, a.zlevel = r, isFinite(n) && (a.z2 = n + (l && l.showAbove ? 1 : -1));
  }
  return n;
}
function KC(t) {
  return t.animation = {
    duration: 0
  }, t;
}
function jC(t, e) {
  return e ? La(ji.transform, e) : Mn(ji.transform), ji.decomposeTransform(), fs(t, ji), t;
}
var ji = new Ea();
ji.transform = wt();
function QC(t) {
  var e = t.getZr().painter;
  return e.getType() === "canvas" ? e : null;
}
Tt("circle", Na);
Tt("ellipse", ru);
Tt("sector", nu);
Tt("ring", iu);
Tt("polygon", au);
Tt("polyline", ou);
Tt("rect", Re);
Tt("line", Pn);
Tt("bezierCurve", su);
Tt("arc", Fa);
const JC = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: Fa,
  BezierCurve: su,
  BoundingRect: Y,
  Circle: Na,
  CompoundPath: Em,
  Ellipse: ru,
  Group: ut,
  HOVER_LAYER_FOR_INCREMENTAL: Ym,
  HOVER_LAYER_FROM_THRESHOLD: Um,
  HOVER_LAYER_NO: Gm,
  Image: Nr,
  IncrementalDisplayable: Fm,
  Line: Pn,
  LinearGradient: Om,
  OrientedBoundingRect: Bm,
  Path: ye,
  Point: ue,
  Polygon: au,
  Polyline: ou,
  RadialGradient: km,
  Rect: Re,
  Ring: iu,
  Sector: nu,
  Text: je,
  WH: Vm,
  XY: Hm,
  applyTransform: Gh,
  calcZ2Range: ZC,
  clipPointsByRect: jm,
  clipRectByRect: Qm,
  createIcon: lu,
  decomposeTransform: jC,
  ensureCopyRect: Sa,
  ensureCopyTransform: Yh,
  expandOrShrinkRect: Ef,
  extendPath: Xm,
  extendShape: Wm,
  getCurrentCanvasPainter: QC,
  getShapeClass: qm,
  getTransform: Km,
  groupTransition: XC,
  initProps: Fh,
  isBoundingRectAxisAligned: Uh,
  isElementRemoved: ia,
  lineLineIntersect: Jm,
  linePolygonIntersect: qC,
  makeImage: zh,
  makePath: uu,
  mergePath: Zm,
  payloadDisableAnimation: KC,
  registerShape: Tt,
  removeElement: zm,
  removeElementWithFadeOut: HC,
  resizePath: Hh,
  retrieveZInfo: t_,
  setTooltipConfig: fu,
  subPixelOptimize: UC,
  subPixelOptimizeLine: Vh,
  subPixelOptimizeRect: GC,
  transformDirection: YC,
  traverseElements: e_,
  traverseUpdateZ: r_,
  updateProps: mi
}, Symbol.toStringTag, { value: "Module" }));
var hu = {};
function i_(t, e) {
  for (var r = 0; r < Or.length; r++) {
    var n = Or[r], i = e[n], a = t.ensureState(n);
    a.style = a.style || {}, a.style.text = i;
  }
  var o = t.currentStates.slice();
  t.clearStates(!0), t.setStyle({
    text: e.normal
  }), t.useStates(o, !0);
}
function Of(t, e, r) {
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
function ex(t, e, r, n) {
  r = r || hu;
  for (var i = t instanceof je, a = !1, o = 0; o < rd.length; o++) {
    var s = e[rd[o]];
    if (s && s.getShallow("show")) {
      a = !0;
      break;
    }
  }
  var u = i ? t : t.getTextContent();
  if (a) {
    i || (u || (u = new je(), t.setTextContent(u)), t.stateProxy && (u.stateProxy = t.stateProxy));
    var l = Of(r, e), f = e.normal, h = !!f.getShallow("show"), v = Cn(f, n, r, !1, !i);
    v.text = l.normal, i || t.setTextConfig(md(f, r, !1));
    for (var o = 0; o < Or.length; o++) {
      var c = Or[o], s = e[c];
      if (s) {
        var d = u.ensureState(c), y = !!G(s.getShallow("show"), h);
        if (y !== h && (d.ignore = !y), d.style = Cn(s, n, r, !0, !i), d.style.text = l[c], !i) {
          var p = t.ensureState(c);
          p.textConfig = md(s, r, !0);
        }
      }
    }
    u.silent = !!f.getShallow("silent"), u.style.x != null && (v.x = u.style.x), u.style.y != null && (v.y = u.style.y), u.ignore = !h, u.useStyle(v), u.dirty(), r.enableTextSetter && (Wh(u).setLabelText = function(g) {
      var m = Of(r, e, g);
      i_(u, m);
    });
  } else u && (u.ignore = !0);
  t.dirty();
}
function WE(t, e) {
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
  return tx(a, t, r, n, i), e && R(a, e), a;
}
function md(t, e, r) {
  e = e || {};
  var n = {}, i, a = t.getShallow("rotate"), o = G(t.getShallow("distance"), r ? null : 5), s = t.getShallow("offset");
  return i = t.getShallow("position") || (r ? null : "inside"), i === "outside" && (i = e.defaultOutsidePosition || "top"), i != null && (n.position = i), s != null && (n.offset = s), a != null && (a *= Math.PI / 180, n.rotation = a), o != null && (n.distance = o), n.outsideFill = t.get("color") === "inherit" ? e.inheritColor || null : "auto", e.autoOverflowArea != null && (n.autoOverflowArea = e.autoOverflowArea), e.layoutRect != null && (n.layoutRect = e.layoutRect), n;
}
function tx(t, e, r, n, i) {
  r = r || hu;
  var a = e.ecModel, o = a && a.option.textStyle, s = rx(e), u;
  if (s) {
    u = {};
    var l = "richInheritPlainLabel", f = G(e.get(l), a ? a.get(l) : void 0);
    for (var h in s)
      if (s.hasOwnProperty(h)) {
        var v = e.getModel(["rich", h]);
        wd(u[h] = {}, v, o, e, f, r, n, i, !1, !0);
      }
  }
  u && (t.rich = u);
  var c = e.get("overflow");
  c && (t.overflow = c);
  var d = e.get("lineOverflow");
  d && (t.lineOverflow = d);
  var y = t, p = e.get("minMargin");
  if (p != null)
    p = ve(p) ? p / 2 : 0, y.margin = [p, p, p, p], y.__marginType = Jn.minMargin;
  else {
    var g = e.get("textMargin");
    g != null && (y.margin = Us(g), y.__marginType = Jn.textMargin);
  }
  wd(t, e, o, null, null, r, n, i, !0, !1);
}
function rx(t) {
  for (var e; t && t !== t.ecModel; ) {
    var r = (t.option || hu).rich;
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
var _d = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY"], Sd = ["align", "lineHeight", "width", "height", "tag", "verticalAlign", "ellipsis"], bd = ["padding", "borderWidth", "borderRadius", "borderDashOffset", "backgroundColor", "borderColor", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"];
function wd(t, e, r, n, i, a, o, s, u, l) {
  r = !o && r || hu;
  var f = a && a.inheritColor, h = e.getShallow("color"), v = e.getShallow("textBorderColor"), c = G(e.getShallow("opacity"), r.opacity);
  (h === "inherit" || h === "auto") && (f ? h = f : h = null), (v === "inherit" || v === "auto") && (f ? v = f : v = null), s || (h = h || r.color, v = v || r.textBorderColor), h != null && (t.fill = h), v != null && (t.stroke = v);
  var d = G(e.getShallow("textBorderWidth"), r.textBorderWidth);
  d != null && (t.lineWidth = d);
  var y = G(e.getShallow("textBorderType"), r.textBorderType);
  y != null && (t.lineDash = y);
  var p = G(e.getShallow("textBorderDashOffset"), r.textBorderDashOffset);
  p != null && (t.lineDashOffset = p), !o && c == null && !l && (c = a && a.defaultOpacity), c != null && (t.opacity = c), !o && !s && t.fill == null && a.inheritColor && (t.fill = a.inheritColor);
  for (var g = 0; g < _d.length; g++) {
    var m = _d[g], _ = i !== !1 && n ? gn(e.getShallow(m), n.getShallow(m), r[m]) : G(e.getShallow(m), r[m]);
    _ != null && (t[m] = _);
  }
  for (var g = 0; g < Sd.length; g++) {
    var m = Sd[g], _ = e.getShallow(m);
    _ != null && (t[m] = _);
  }
  if (t.verticalAlign == null) {
    var S = e.getShallow("baseline");
    S != null && (t.verticalAlign = S);
  }
  if (!u || !a.disableBox) {
    for (var g = 0; g < bd.length; g++) {
      var m = bd[g], _ = e.getShallow(m);
      _ != null && (t[m] = _);
    }
    var b = e.getShallow("borderType");
    b != null && (t.borderDash = b), (t.backgroundColor === "auto" || t.backgroundColor === "inherit") && f && (t.backgroundColor = f), (t.borderColor === "auto" || t.borderColor === "inherit") && f && (t.borderColor = f);
  }
}
function nx(t, e) {
  var r = e && e.getModel("textStyle");
  return It([
    // FIXME in node-canvas fontWeight is before fontStyle
    t.fontStyle || r && r.getShallow("fontStyle") || "",
    t.fontWeight || r && r.getShallow("fontWeight") || "",
    (t.fontSize || r && r.getShallow("fontSize") || 12) + "px",
    t.fontFamily || r && r.getShallow("fontFamily") || "sans-serif"
  ].join(" "));
}
var Wh = de();
function XE(t, e, r, n) {
  if (t) {
    var i = Wh(t);
    i.prevValue = i.value, i.value = r;
    var a = e.normal;
    i.valueAnimation = a.get("valueAnimation"), i.valueAnimation && (i.precision = a.get("precision"), i.defaultInterpolatedText = n, i.statesModels = e);
  }
}
function qE(t, e, r, n, i) {
  var a = Wh(t);
  if (!a.valueAnimation || a.prevValue === a.value)
    return;
  var o = a.defaultInterpolatedText, s = G(a.interpolatedValue, a.prevValue), u = a.value;
  function l(f) {
    var h = Lw(r, a.precision, s, u, f);
    a.interpolatedValue = f === 1 ? null : h;
    var v = Of({
      labelDataIndex: e,
      labelFetcher: i,
      defaultText: o ? o(h) : h + ""
    }, a.statesModels, h);
    i_(t, v);
  }
  t.percent = 0, (a.prevValue == null ? Fh : mi)(t, {
    // percent is used to prevent animation from being aborted #15916
    percent: 1
  }, n, e, null, l);
}
var Jn = {
  minMargin: 1,
  textMargin: 2
}, ix = ["textStyle", "color"], vl = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "padding", "lineHeight", "rich", "width", "height", "overflow"], dl = new je(), ax = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getTextColor = function(e) {
      var r = this.ecModel;
      return this.getShallow("color") || (!e && r ? r.get(ix) : null);
    }, t.prototype.getFont = function() {
      return nx({
        fontStyle: this.getShallow("fontStyle"),
        fontWeight: this.getShallow("fontWeight"),
        fontSize: this.getShallow("fontSize"),
        fontFamily: this.getShallow("fontFamily")
      }, this.ecModel);
    }, t.prototype.getTextRect = function(e) {
      for (var r = {
        text: e,
        verticalAlign: this.getShallow("verticalAlign") || this.getShallow("baseline")
      }, n = 0; n < vl.length; n++)
        r[vl[n]] = this.getShallow(vl[n]);
      return dl.useStyle(r), dl.update(), dl.getBoundingRect();
    }, t;
  })()
), a_ = [
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
], ox = _a(a_), sx = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getLineStyle = function(e) {
      return ox(this, e);
    }, t;
  })()
), o_ = [
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
], ux = _a(o_), lx = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getItemStyle = function(e, r) {
      return ux(this, e, r);
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
Ah(ce);
Xw(ce);
Lt(ce, sx);
Lt(ce, lx);
Lt(ce, jw);
Lt(ce, ax);
var fx = Math.round(Math.random() * 10);
function cu(t) {
  return [t || "", fx++].join("_");
}
function hx(t) {
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
function cx(t, e) {
  t.topologicalTravel = function(a, o, s, u) {
    if (!a.length)
      return;
    var l = r(o), f = l.graph, h = l.noEntryList, v = {};
    for (T(a, function(m) {
      v[m] = !0;
    }); h.length; ) {
      var c = h.pop(), d = f[c], y = !!v[c];
      y && (s.call(u, c, d.originalDeps.slice()), delete v[c]), T(d.successor, y ? g : p);
    }
    T(v, function() {
      var m = "";
      throw new Error(m);
    });
    function p(m) {
      f[m].entryCount--, f[m].entryCount === 0 && h.push(m);
    }
    function g(m) {
      v[m] = !0, p(m);
    }
  };
  function r(a) {
    var o = {}, s = [];
    return T(a, function(u) {
      var l = n(o, u), f = l.originalDeps = e(u), h = i(f, a);
      l.entryCount = h.length, l.entryCount === 0 && s.push(u), T(h, function(v) {
        oe(l.predecessor, v) < 0 && l.predecessor.push(v);
        var c = n(o, v);
        oe(c.successor, v) < 0 && c.successor.push(u);
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
    return T(a, function(u) {
      oe(o, u) >= 0 && s.push(u);
    }), s;
  }
}
function vx(t, e) {
  return pe(pe({}, t, !0), e, !0);
}
const dx = {
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
}, px = {
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
var gs = "ZH", Xh = "EN", ri = Xh, Yo = {}, qh = {}, s_ = W.domSupported ? (function() {
  var t = (document.documentElement.lang || navigator.language || navigator.browserLanguage || ri).toUpperCase();
  return t.indexOf(gs) > -1 ? gs : ri;
})() : ri;
function $h(t, e) {
  t = t.toUpperCase(), qh[t] = new ce(e), Yo[t] = e;
}
function gx(t) {
  if (N(t)) {
    var e = Yo[t.toUpperCase()] || {};
    return t === gs || t === Xh ? Z(e) : pe(Z(e), Z(Yo[ri]), !1);
  } else
    return pe(Z(t), Z(Yo[ri]), !1);
}
function yx(t) {
  return qh[t];
}
function mx() {
  return qh[ri];
}
$h(Xh, dx);
$h(gs, px);
var _x = null;
function Xe() {
  return _x;
}
function u_(t, e) {
  var r = Xe(), n = e.breakOption, i = e.breakParsed;
  return !i && r && (i = r.parseAxisBreakOption(n, t)), i;
}
function ys(t) {
  var e = t.brk;
  return e ? e.breaks : [];
}
function Sx(t) {
  var e = t.brk;
  return e ? e.hasBreaks() : !1;
}
var Zh = 1e3, Kh = Zh * 60, aa = Kh * 60, St = aa * 24, Td = St * 365, bx = {
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
}, Tx = "{yyyy}-{MM}-{dd} {HH}:{mm}:{ss} {SSS}", lo = "{yyyy}-{MM}-{dd}", Cd = {
  year: "{yyyy}",
  month: "{yyyy}-{MM}",
  day: lo,
  hour: lo + " " + Wo.hour,
  minute: lo + " " + Wo.minute,
  second: lo + " " + Wo.second,
  millisecond: Tx
}, it = ["year", "month", "day", "hour", "minute", "second", "millisecond"], Cx = ["year", "half-year", "quarter", "month", "week", "half-week", "day", "half-day", "quarter-day", "hour", "minute", "second", "millisecond"];
function xx(t) {
  return !N(t) && !X(t) ? Mx(t) : t;
}
function Mx(t) {
  t = t || {};
  var e = {}, r = !0;
  return T(it, function(n) {
    r && (r = t[n] == null);
  }), T(it, function(n, i) {
    var a = t[n];
    e[n] = {};
    for (var o = null, s = i; s >= 0; s--) {
      var u = it[s], l = F(a) && !B(a) ? a[u] : a, f = void 0;
      B(l) ? (f = l.slice(), o = f[0] || "") : N(l) ? (o = l, f = [o]) : (o == null ? o = Wo[n] : bx[u].test(o) || (o = e[u][u][0] + " " + o), f = [o], r && (f[1] = "{primary|" + o + "}")), e[n][u] = f;
    }
  }), e;
}
function Ge(t, e) {
  return t += "", "0000".substr(0, e - t.length) + t;
}
function oa(t) {
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
function Dx(t) {
  return t === oa(t);
}
function Ix(t) {
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
  var i = pr(t), a = i[l_(r)](), o = i[jh(r)]() + 1, s = Math.floor((o - 1) / 3) + 1, u = i[Qh(r)](), l = i["get" + (r ? "UTC" : "") + "Day"](), f = i[Jh(r)](), h = (f - 1) % 12 + 1, v = i[ec(r)](), c = i[tc(r)](), d = i[rc(r)](), y = f >= 12 ? "pm" : "am", p = y.toUpperCase(), g = n instanceof ce ? n : yx(n || s_) || mx(), m = g.getModel("time"), _ = m.get("month"), S = m.get("monthAbbr"), b = m.get("dayOfWeek"), w = m.get("dayOfWeekAbbr");
  return (e || "").replace(/{a}/g, y + "").replace(/{A}/g, p + "").replace(/{yyyy}/g, a + "").replace(/{yy}/g, Ge(a % 100 + "", 2)).replace(/{Q}/g, s + "").replace(/{MMMM}/g, _[o - 1]).replace(/{MMM}/g, S[o - 1]).replace(/{MM}/g, Ge(o, 2)).replace(/{M}/g, o + "").replace(/{dd}/g, Ge(u, 2)).replace(/{d}/g, u + "").replace(/{eeee}/g, b[l]).replace(/{ee}/g, w[l]).replace(/{e}/g, l + "").replace(/{HH}/g, Ge(f, 2)).replace(/{H}/g, f + "").replace(/{hh}/g, Ge(h + "", 2)).replace(/{h}/g, h + "").replace(/{mm}/g, Ge(v, 2)).replace(/{m}/g, v + "").replace(/{ss}/g, Ge(c, 2)).replace(/{s}/g, c + "").replace(/{SSS}/g, Ge(d, 3)).replace(/{S}/g, d + "");
}
function Ax(t, e, r, n, i) {
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
  var r = pr(t), n = r[jh(e)]() + 1, i = r[Qh(e)](), a = r[Jh(e)](), o = r[ec(e)](), s = r[tc(e)](), u = r[rc(e)](), l = u === 0, f = l && s === 0, h = f && o === 0, v = h && a === 0, c = v && i === 1, d = c && n === 1;
  return d ? "year" : c ? "month" : v ? "day" : h ? "hour" : f ? "minute" : l ? "second" : "millisecond";
}
function ms(t, e, r) {
  switch (e) {
    case "year":
      t[f_(r)](0);
    case "month":
      t[h_(r)](1);
    case "day":
      t[c_(r)](0);
    case "hour":
      t[v_(r)](0);
    case "minute":
      t[d_(r)](0);
    case "second":
      t[p_(r)](0);
  }
  return t;
}
function l_(t) {
  return t ? "getUTCFullYear" : "getFullYear";
}
function jh(t) {
  return t ? "getUTCMonth" : "getMonth";
}
function Qh(t) {
  return t ? "getUTCDate" : "getDate";
}
function Jh(t) {
  return t ? "getUTCHours" : "getHours";
}
function ec(t) {
  return t ? "getUTCMinutes" : "getMinutes";
}
function tc(t) {
  return t ? "getUTCSeconds" : "getSeconds";
}
function rc(t) {
  return t ? "getUTCMilliseconds" : "getMilliseconds";
}
function Px(t) {
  return t ? "setUTCFullYear" : "setFullYear";
}
function f_(t) {
  return t ? "setUTCMonth" : "setMonth";
}
function h_(t) {
  return t ? "setUTCDate" : "setDate";
}
function c_(t) {
  return t ? "setUTCHours" : "setHours";
}
function v_(t) {
  return t ? "setUTCMinutes" : "setMinutes";
}
function d_(t) {
  return t ? "setUTCSeconds" : "setSeconds";
}
function p_(t) {
  return t ? "setUTCMilliseconds" : "setMilliseconds";
}
function Lx(t, e, r, n, i, a, o, s) {
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
function nc(t) {
  if (!Xy(t))
    return N(t) ? t : "-";
  var e = (t + "").split(".");
  return e[0].replace(/(\d{1,3})(?=(?:\d{3})+(?!\d))/g, "$1,") + (e.length > 1 ? "." + e[1] : "");
}
function ic(t, e) {
  return t = (t || "").toLowerCase().replace(/-(.)/g, function(r, n) {
    return n.toUpperCase();
  }), e && t && (t = t.charAt(0).toUpperCase() + t.slice(1)), t;
}
var Ha = Us;
function kf(t, e, r) {
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
    return es(t) ? i(t) : ve(t) && a(t) ? t + "" : "-";
  var l = cr(t);
  return a(l) ? nc(l) : es(t) ? i(t) : typeof t == "boolean" ? t + "" : "-";
}
var xd = ["a", "b", "c", "d", "e", "f", "g"], pl = function(t, e) {
  return "{" + t + (e ?? "") + "}";
};
function ac(t, e, r) {
  B(e) || (e = [e]);
  var n = e.length;
  if (!n)
    return "";
  for (var i = e[0].$vars || [], a = 0; a < i.length; a++) {
    var o = xd[a];
    t = t.replace(pl(o), pl(o, 0));
  }
  for (var s = 0; s < n; s++)
    for (var u = 0; u < i.length; u++) {
      var l = e[s][i[u]];
      t = t.replace(pl(xd[u], s), r ? Ue(l) : l);
    }
  return t;
}
function g_(t, e) {
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
function Ex(t, e, r) {
  (t === "week" || t === "month" || t === "quarter" || t === "half-year" || t === "year") && (t = `MM-dd
yyyy`);
  var n = pr(e), i = r ? "getUTC" : "get", a = n[i + "FullYear"](), o = n[i + "Month"]() + 1, s = n[i + "Date"](), u = n[i + "Hours"](), l = n[i + "Minutes"](), f = n[i + "Seconds"](), h = n[i + "Milliseconds"]();
  return t = t.replace("MM", Ge(o, 2)).replace("M", o).replace("yyyy", a).replace("yy", Ge(a % 100 + "", 2)).replace("dd", Ge(s, 2)).replace("d", s).replace("hh", Ge(u, 2)).replace("h", u).replace("mm", Ge(l, 2)).replace("m", l).replace("ss", Ge(f, 2)).replace("s", f).replace("SSS", Ge(h, 3)), t;
}
function Rx(t) {
  return t && t.charAt(0).toUpperCase() + t.substr(1);
}
function ba(t, e) {
  return e = e || "transparent", N(t) ? t : F(t) && t.colorStops && (t.colorStops[0] || {}).color || e;
}
var Xo = {}, gl = {}, vu = (
  /** @class */
  (function() {
    function t() {
      this._normalMasterList = [], this._nonSeriesBoxMasterList = [];
    }
    return t.prototype.create = function(e, r) {
      this._nonSeriesBoxMasterList = n(Xo), this._normalMasterList = n(gl);
      function n(i, a) {
        var o = [];
        return T(i, function(s, u) {
          var l = s.create(e, r);
          o = o.concat(l || []);
        }), o;
      }
    }, t.prototype.update = function(e, r) {
      T(this._normalMasterList, function(n) {
        n.update && n.update(e, r);
      });
    }, t.prototype.getCoordinateSystems = function() {
      return this._normalMasterList.concat(this._nonSeriesBoxMasterList);
    }, t.register = function(e, r) {
      if (e === "matrix" || e === "calendar") {
        Xo[e] = r;
        return;
      }
      gl[e] = r;
    }, t.get = function(e) {
      return gl[e] || Xo[e];
    }, t;
  })()
);
function Ox(t) {
  return !!Xo[t];
}
var kx = 1, Bx = 2, Nx = U();
function y_(t) {
  var e = t.getShallow("coord", !0), r = kx;
  if (e == null) {
    var n = Nx.get(t.type);
    n && n.getCoord2 && (r = Bx, e = n.getCoord2(t));
  }
  return {
    coord: e,
    from: r
  };
}
var ii = 0, qo = 1, Fx = 2;
function zx(t, e) {
  var r = t.getShallow("coordinateSystem"), n = t.getShallow("coordinateSystemUsage", !0), i = ii;
  if (r) {
    var a = t.mainType === "series";
    n == null && (n = a ? "data" : "box"), n === "data" ? (i = qo, a || (i = ii)) : n === "box" && (i = Fx, !a && !Ox(r) && (i = ii));
  }
  return {
    coordSysType: r,
    kind: i
  };
}
function $E(t) {
  var e = t.targetModel, r = t.coordSysType, n = t.coordSysProvider, i = t.isDefaultDataCoordSys, a = zx(e), o = a.kind, s = a.coordSysType;
  if (i && o !== qo && (o = qo, s = r), o === ii || s !== r)
    return ii;
  var u = n(r, e);
  return u ? (o === qo ? e.coordinateSystem = u : e.boxCoordinateSystem = u, o) : ii;
}
var $o = T, Hx = ["left", "right", "top", "bottom", "width", "height"], fo = [["width", "left", "right"], ["height", "top", "bottom"]];
function oc(t, e, r, n, i) {
  var a = 0, o = 0;
  n == null && (n = 1 / 0), i == null && (i = 1 / 0);
  var s = 0;
  e.eachChild(function(u, l) {
    var f = u.getBoundingRect(), h = e.childAt(l + 1), v = h && h.getBoundingRect(), c, d;
    if (t === "horizontal") {
      var y = f.width + (v ? -v.x + f.x : 0);
      c = a + y, c > n || u.newline ? (a = 0, c = y, o += s + r, s = f.height) : s = Math.max(s, f.height);
    } else {
      var p = f.height + (v ? -v.y + f.y : 0);
      d = o + p, d > i || u.newline ? (a += s + r, o = 0, d = p, s = f.width) : s = Math.max(s, f.width);
    }
    u.newline || (u.x = a, u.y = o, u.markRedraw(), t === "horizontal" ? a = c + r : o = d + r);
  });
}
var sa = oc;
Te(oc, "vertical");
Te(oc, "horizontal");
function Vx(t, e) {
  return {
    left: t.getShallow("left", e),
    top: t.getShallow("top", e),
    right: t.getShallow("right", e),
    bottom: t.getShallow("bottom", e),
    width: t.getShallow("width", e),
    height: t.getShallow("height", e)
  };
}
function _s(t, e, r) {
  r = Ha(r || 0);
  var n = e.width, i = e.height, a = Dt(t.left, n), o = Dt(t.top, i), s = Dt(t.right, n), u = Dt(t.bottom, i), l = Dt(t.width, n), f = Dt(t.height, i), h = r[2] + r[0], v = r[1] + r[3], c = t.aspect;
  switch (isNaN(l) && (l = n - s - v - a), isNaN(f) && (f = i - u - h - o), c != null && (isNaN(l) && isNaN(f) && (c > n / i ? l = n * 0.8 : f = i * 0.8), isNaN(l) && (l = c * f), isNaN(f) && (f = l / c)), isNaN(a) && (a = n - s - l - v), isNaN(o) && (o = i - u - f - h), t.left || t.right) {
    case "center":
      a = n / 2 - l / 2 - r[3];
      break;
    case "right":
      a = n - l - v;
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
  a = a || 0, o = o || 0, isNaN(l) && (l = n - v - a - (s || 0)), isNaN(f) && (f = i - h - o - (u || 0));
  var d = new Y((e.x || 0) + a + r[3], (e.y || 0) + o + r[0], l, f);
  return d.margin = r, d;
}
var yl = {
  rect: 1
};
function Gx(t, e, r) {
  var n, i, a, o = t.boxCoordinateSystem, s;
  if (o) {
    var u = y_(t), l = u.coord, f = u.from;
    if (o.dataToLayout) {
      a = yl.rect, s = f;
      var h = o.dataToLayout(l);
      n = h.contentRect || h.rect;
    }
  }
  return a == null && (a = yl.rect), a === yl.rect && (n || (n = {
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
function Ss(t) {
  var e = t.layoutMode || t.constructor.layoutMode;
  return F(e) ? e : e ? {
    type: e
  } : null;
}
function wa(t, e, r) {
  var n = r && r.ignoreSize;
  !B(n) && (n = [n, n]);
  var i = o(fo[0], 0), a = o(fo[1], 1);
  u(fo[0], t, i), u(fo[1], t, a);
  function o(l, f) {
    var h = {}, v = 0, c = {}, d = 0, y = 2;
    if ($o(l, function(m) {
      c[m] = t[m];
    }), $o(l, function(m) {
      qe(e, m) && (h[m] = c[m] = e[m]), s(h, m) && v++, s(c, m) && d++;
    }), n[f])
      return s(e, l[1]) ? c[l[2]] = null : s(e, l[2]) && (c[l[1]] = null), c;
    if (d === y || !v)
      return c;
    if (v >= y)
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
    $o(l, function(v) {
      f[v] = h[v];
    });
  }
}
function sc(t) {
  return Ux({}, t);
}
function Ux(t, e) {
  return e && t && $o(Hx, function(r) {
    qe(e, r) && (t[r] = e[r]);
  }), t;
}
var Yx = de(), ae = (
  /** @class */
  (function(t) {
    z(e, t);
    function e(r, n, i) {
      var a = t.call(this, r, n, i) || this;
      return a.uid = cu("ec_cpt_model"), a;
    }
    return e.prototype.init = function(r, n, i) {
      this.mergeDefaultAndTheme(r, i);
    }, e.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = Ss(this), a = i ? sc(r) : {}, o = n.getTheme();
      pe(r, o.get(this.mainType)), pe(r, this.getDefaultOption()), i && wa(r, a, i);
    }, e.prototype.mergeOption = function(r, n) {
      pe(this.option, r, !0);
      var i = Ss(this);
      i && wa(this.option, r, i);
    }, e.prototype.optionUpdated = function(r, n) {
    }, e.prototype.getDefaultOption = function() {
      var r = this.constructor;
      if (!Uw(r))
        return r.defaultOption;
      var n = Yx(this);
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
      return Vx(this, !1);
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
tm(ae, ce);
js(ae);
hx(ae);
cx(ae, Wx);
function Wx(t) {
  var e = [];
  return T(ae.getClassesByMainType(t), function(r) {
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
R(be, {
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
    var Md = be[rn];
    rn === "theme" ? j.darkColor.theme = be.theme.slice() : rn === "highlight" ? j.darkColor.highlight = "rgba(255,231,130,0.4)" : rn.indexOf("accent") === 0 ? j.darkColor[rn] = us(Md, null, function(t) {
      return t * 0.5;
    }, function(t) {
      return Math.min(1, 1.3 - t);
    }) : j.darkColor[rn] = us(Md, null, function(t) {
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
var m_ = "";
typeof navigator < "u" && (m_ = navigator.platform || "");
var Hn = "rgba(0, 0, 0, 0.2)", __ = j.color.theme[0], Xx = us(__, null, null, 0.9);
const S_ = {
  darkMode: "auto",
  // backgroundColor: 'rgba(0,0,0,0)',
  colorBy: "series",
  color: j.color.theme,
  gradientColor: [Xx, __],
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
    fontFamily: m_.match(/^Win/) ? "Microsoft YaHei" : "sans-serif",
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
}, b_ = de();
function qx(t) {
  b_(t).datasetMap = U();
}
function $x(t, e, r) {
  var n = {}, i = w_(e);
  if (!i || !t)
    return n;
  var a = [], o = [], s = e.ecModel, u = b_(s).datasetMap, l = i.uid + "_" + r.seriesLayoutBy, f, h;
  t = t.slice(), T(t, function(y, p) {
    var g = F(y) ? y : t[p] = {
      name: y
    };
    g.type === "ordinal" && f == null && (f = p, h = d(g)), n[g.name] = [];
  });
  var v = u.get(l) || u.set(l, {
    categoryWayDim: h,
    valueWayDim: 0
  });
  T(t, function(y, p) {
    var g = y.name, m = d(y);
    if (f == null) {
      var _ = v.valueWayDim;
      c(n[g], _, m), c(o, _, m), v.valueWayDim += m;
    } else if (f === p)
      c(n[g], 0, m), c(a, 0, m);
    else {
      var _ = v.categoryWayDim;
      c(n[g], _, m), c(o, _, m), v.categoryWayDim += m;
    }
  });
  function c(y, p, g) {
    for (var m = 0; m < g; m++)
      y.push(p + m);
  }
  function d(y) {
    var p = y.dimsDef;
    return p ? p.length : 1;
  }
  return a.length && (n.itemName = a), o.length && (n.seriesName = o), n;
}
function w_(t) {
  var e = t.get("data", !0);
  if (!e)
    return ka(t.ecModel, "dataset", {
      index: t.get("datasetIndex", !0),
      id: t.get("datasetId", !0)
    }, ur).models[0];
}
function Zx(t) {
  return !t.get("transform", !0) && !t.get("fromTransformResult", !0) ? [] : ka(t.ecModel, "dataset", {
    index: t.get("fromDatasetIndex", !0),
    id: t.get("fromDatasetId", !0)
  }, ur).models;
}
function T_(t, e) {
  return Kx(t.data, t.sourceFormat, t.seriesLayoutBy, t.dimensionsDefine, t.startIndex, e);
}
function Kx(t, e, r, n, i, a) {
  var o, s = 5;
  if (Ke(t))
    return nt.Not;
  var u, l;
  if (n) {
    var f = n[a];
    F(f) ? (u = f.name, l = f.type) : N(f) && (u = f);
  }
  if (l != null)
    return l === "ordinal" ? nt.Must : nt.Not;
  if (e === ze) {
    var h = t;
    if (r === An) {
      for (var v = h[a], c = 0; c < (v || []).length && c < s; c++)
        if ((o = S(v[i + c])) != null)
          return o;
    } else
      for (var c = 0; c < h.length && c < s; c++) {
        var d = h[i + c];
        if (d && (o = S(d[a])) != null)
          return o;
      }
  } else if (e === Et) {
    var y = t;
    if (!u)
      return nt.Not;
    for (var c = 0; c < y.length && c < s; c++) {
      var p = y[c];
      if (p && (o = S(p[u])) != null)
        return o;
    }
  } else if (e === jt) {
    var g = t;
    if (!u)
      return nt.Not;
    var v = g[u];
    if (!v || Ke(v))
      return nt.Not;
    for (var c = 0; c < v.length && c < s; c++)
      if ((o = S(v[c])) != null)
        return o;
  } else if (e === ft)
    for (var m = t, c = 0; c < m.length && c < s; c++) {
      var p = m[c], _ = Ra(p);
      if (!B(_))
        return nt.Not;
      if ((o = S(_[a])) != null)
        return o;
    }
  function S(b) {
    var w = N(b);
    if (b != null && isFinite(Number(b)) && b !== "")
      return w ? nt.Might : nt.Not;
    if (w && b !== "-")
      return nt.Must;
  }
  return nt.Not;
}
var jx = U();
function Qx(t, e, r) {
  var n = jx.get(e);
  if (!n)
    return r;
  var i = n(t);
  return i ? r.concat(i) : r;
}
var Dd = de();
de();
var uc = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getColorFromPalette = function(e, r, n) {
      var i = $e(this.get("color", !0)), a = this.get("colorLayer", !0);
      return eM(this, Dd, i, a, e, r, n);
    }, t.prototype.clearColorPalette = function() {
      tM(this, Dd);
    }, t;
  })()
);
function Jx(t, e) {
  for (var r = t.length, n = 0; n < r; n++)
    if (t[n].length > e)
      return t[n];
  return t[r - 1];
}
function eM(t, e, r, n, i, a, o) {
  a = a || t;
  var s = e(a), u = s.paletteIdx || 0, l = s.paletteNameMap = s.paletteNameMap || {};
  if (l.hasOwnProperty(i))
    return l[i];
  var f = o == null || !n ? r : Jx(n, o);
  if (f = f || r, !(!f || !f.length)) {
    var h = f[u];
    return i && (l[i] = h), s.paletteIdx = (u + 1) % f.length, h;
  }
}
function tM(t, e) {
  e(t).paletteIdx = 0, e(t).paletteNameMap = {};
}
var ho, Oi, Id, Ad = "\0_ec_inner", rM = 1, lc = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      return t !== null && t.apply(this, arguments) || this;
    }
    return e.prototype.init = function(r, n, i, a, o, s) {
      a = a || {}, this.option = null, this._theme = new ce(a), this._locale = new ce(o), this._optionManager = s;
    }, e.prototype.setOption = function(r, n, i) {
      var a = Ed(n);
      this._optionManager.setOption(r, i, a), this._resetOption(null, a);
    }, e.prototype.resetOption = function(r, n) {
      return this._resetOption(r, Ed(n));
    }, e.prototype._resetOption = function(r, n) {
      var i = !1, a = this._optionManager;
      if (!r || r === "recreate") {
        var o = a.mountOption(r === "recreate");
        !this.option || r === "recreate" ? Id(this, o) : (this.restoreData(), this._mergeOption(o, n)), i = !0;
      }
      if ((r === "timeline" || r === "media") && this.restoreData(), !r || r === "recreate" || r === "timeline") {
        var s = a.getTimelineOption(this);
        s && (i = !0, this._mergeOption(s, n));
      }
      if (!r || r === "recreate" || r === "media") {
        var u = a.getMediaOption(this);
        u.length && T(u, function(l) {
          i = !0, this._mergeOption(l, n);
        }, this);
      }
      return i;
    }, e.prototype.mergeOption = function(r) {
      this._mergeOption(r, null);
    }, e.prototype._mergeOption = function(r, n) {
      var i = this.option, a = this._componentsMap, o = this._componentsCount, s = [], u = U(), l = n && n.replaceMergeMainTypeMap;
      qx(this), T(r, function(h, v) {
        h != null && (ae.hasClass(v) ? v && (s.push(v), u.set(v, !0)) : i[v] = i[v] == null ? Z(h) : pe(i[v], h, !0));
      }), l && l.each(function(h, v) {
        ae.hasClass(v) && !u.get(v) && (s.push(v), u.set(v, !0));
      }), ae.topologicalTravel(s, ae.getAllClassMainTypes(), f, this);
      function f(h) {
        var v = Qx(this, h, $e(r[h])), c = a.get(h), d = (
          // `!oldCmptList` means init. See the comment in `mappingToExists`
          c ? l && l.get(h) ? "replaceMerge" : "normalMerge" : "replaceAll"
        ), y = mw(c, v, d);
        xw(y, h, ae), i[h] = null, a.set(h, null), o.set(h, 0);
        var p = [], g = [], m = 0, _;
        T(y, function(S, b) {
          var w = S.existing, C = S.newOption;
          if (!C)
            w && (w.mergeOption({}, this), w.optionUpdated({}, !1));
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
            if (w && w.constructor === x)
              w.name = S.keyInfo.name, w.mergeOption(C, this), w.optionUpdated(C, !1);
            else {
              var D = R({
                componentIndex: b
              }, S.keyInfo);
              w = new x(C, this, this, D), R(w, D), S.brandNew && (w.__requireNewView = !0), w.init(C, this, this), w.optionUpdated(null, !0);
            }
          }
          w ? (p.push(w.option), g.push(w), m++) : (p.push(void 0), g.push(void 0));
        }, this), i[h] = p, a.set(h, g), o.set(h, m), h === "series" && ho(this);
      }
      this._seriesIndices || ho(this);
    }, e.prototype.getOption = function() {
      var r = Z(this.option);
      return T(r, function(n, i) {
        if (ae.hasClass(i)) {
          for (var a = $e(n), o = a.length, s = !1, u = o - 1; u >= 0; u--)
            a[u] && !ma(a[u]) ? s = !0 : (a[u] = null, !s && o--);
          a.length = o, r[i] = a;
        }
      }), delete r[Ad], r;
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
      return i != null ? (u = [], T($e(i), function(l) {
        s[l] && u.push(s[l]);
      })) : a != null ? u = Pd("id", a, s) : o != null ? u = Pd("name", o, s) : u = xe(s, function(l) {
        return !!l;
      }), Ld(u, r);
    }, e.prototype.findComponents = function(r) {
      var n = r.query, i = r.mainType, a = s(n), o = a ? this.queryComponents(a) : xe(this._componentsMap.get(i), function(l) {
        return !!l;
      });
      return u(Ld(o, r));
      function s(l) {
        var f = i + "Index", h = i + "Id", v = i + "Name";
        return l && (l[f] != null || l[h] != null || l[v] != null) ? {
          mainType: i,
          // subType will be filtered finally.
          index: l[f],
          id: l[h],
          name: l[v]
        } : null;
      }
      function u(l) {
        return r.filter ? xe(l, r.filter) : l;
      }
    }, e.prototype.eachComponent = function(r, n, i) {
      var a = this._componentsMap;
      if (X(r)) {
        var o = n, s = r;
        a.each(function(h, v) {
          for (var c = 0; h && c < h.length; c++) {
            var d = h[c];
            d && s.call(o, v, d, d.componentIndex);
          }
        });
      } else
        for (var u = N(r) ? a.get(r) : F(r) ? this.findComponents(r) : null, l = 0; u && l < u.length; l++) {
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
      Oi(this), T(this._seriesIndices, function(i) {
        var a = this._componentsMap.get("series")[i];
        r.call(n, a, i);
      }, this);
    }, e.prototype.eachRawSeries = function(r, n) {
      T(this._componentsMap.get("series"), function(i) {
        i && r.call(n, i, i.componentIndex);
      });
    }, e.prototype.eachSeriesByType = function(r, n, i) {
      Oi(this), T(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        o.subType === r && n.call(i, o, a);
      }, this);
    }, e.prototype.eachRawSeriesByType = function(r, n, i) {
      return T(this.getSeriesByType(r), n, i);
    }, e.prototype.isSeriesFiltered = function(r) {
      return Oi(this), this._seriesIndicesMap.get(r.componentIndex) == null;
    }, e.prototype.getCurrentSeriesIndices = function() {
      return (this._seriesIndices || []).slice();
    }, e.prototype.filterSeries = function(r, n) {
      Oi(this);
      var i = [];
      T(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        r.call(n, o, a) && i.push(a);
      }, this), this._seriesIndices = i, this._seriesIndicesMap = U(i);
    }, e.prototype.restoreData = function(r) {
      ho(this);
      var n = this._componentsMap, i = [];
      n.each(function(a, o) {
        ae.hasClass(o) && i.push(o);
      }), ae.topologicalTravel(i, ae.getAllClassMainTypes(), function(a) {
        T(n.get(a), function(o) {
          o && (a !== "series" || !nM(o, r)) && o.restoreData();
        });
      });
    }, e.internalField = (function() {
      ho = function(r) {
        var n = r._seriesIndices = [];
        T(r._componentsMap.get("series"), function(i) {
          i && n.push(i.componentIndex);
        }), r._seriesIndicesMap = U(n);
      }, Oi = function(r) {
      }, Id = function(r, n) {
        r.option = {}, r.option[Ad] = rM, r._componentsMap = U({
          series: []
        }), r._componentsCount = U();
        var i = n.aria;
        F(i) && i.enabled == null && (i.enabled = !0), iM(n, r._theme.option), pe(n, S_, !1), r._mergeOption(n, null);
      };
    })(), e;
  })(ce)
);
function nM(t, e) {
  if (e) {
    var r = e.seriesIndex, n = e.seriesId, i = e.seriesName;
    return r != null && t.componentIndex !== r || n != null && t.id !== n || i != null && t.name !== i;
  }
}
function iM(t, e) {
  var r = t.color && !t.colorLayer;
  T(e, function(n, i) {
    i === "colorLayer" && r || i === "color" && t.color || ae.hasClass(i) || (typeof n == "object" ? t[i] = t[i] ? pe(t[i], n, !1) : Z(n) : t[i] == null && (t[i] = n));
  });
}
function Pd(t, e, r) {
  if (B(e)) {
    var n = U();
    return T(e, function(a) {
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
function Ld(t, e) {
  return e.hasOwnProperty("subType") ? xe(t, function(r) {
    return r && r.subType === e.subType;
  }) : t;
}
function Ed(t) {
  var e = U();
  return t && T($e(t.replaceMerge), function(r) {
    e.set(r, !0);
  }), {
    replaceMergeMainTypeMap: e
  };
}
Lt(lc, uc);
var aM = /^(min|max)?(.+)$/, oM = (
  /** @class */
  (function() {
    function t(e) {
      this._timelineOptions = [], this._mediaList = [], this._currentMediaIndices = [], this._api = e;
    }
    return t.prototype.setOption = function(e, r, n) {
      e && (T($e(e.series), function(o) {
        o && o.data && Ke(o.data) && va(o.data);
      }), T($e(e.dataset), function(o) {
        o && o.source && Ke(o.source) && va(o.source);
      })), e = Z(e);
      var i = this._optionBackup, a = sM(e, r, !i);
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
        uM(i[u].query, r, n) && o.push(u);
      return !o.length && a && (o = [-1]), o.length && !fM(o, this._currentMediaIndices) && (s = H(o, function(f) {
        return Z(f === -1 ? a.option : i[f].option);
      })), this._currentMediaIndices = o, s;
    }, t;
  })()
);
function sM(t, e, r) {
  var n = [], i, a, o = t.baseOption, s = t.timeline, u = t.options, l = t.media, f = !!t.media, h = !!(u || s || o && o.timeline);
  o ? (a = o, a.timeline || (a.timeline = s)) : ((h || f) && (t.options = t.media = null), a = t), f && B(l) && T(l, function(c) {
    c && c.option && (c.query ? n.push(c) : i || (i = c));
  }), v(a), T(u, function(c) {
    return v(c);
  }), T(n, function(c) {
    return v(c.option);
  });
  function v(c) {
    T(e, function(d) {
      d(c, r);
    });
  }
  return {
    baseOption: a,
    timelineOptions: u || [],
    mediaDefault: i,
    mediaList: n
  };
}
function uM(t, e, r) {
  var n = {
    width: e,
    height: r,
    aspectratio: e / r
    // lower case for convenience.
  }, i = !0;
  return T(t, function(a, o) {
    var s = o.match(aM);
    if (!(!s || !s[1] || !s[2])) {
      var u = s[1], l = s[2].toLowerCase();
      lM(n[l], a, u) || (i = !1);
    }
  }), i;
}
function lM(t, e, r) {
  return r === "min" ? t >= e : r === "max" ? t <= e : t === e;
}
function fM(t, e) {
  return t.join(",") === e.join(",");
}
var Ct = T, Ta = F, Rd = ["areaStyle", "lineStyle", "nodeStyle", "linkStyle", "chordStyle", "label", "labelLine"];
function ml(t) {
  var e = t && t.itemStyle;
  if (e)
    for (var r = 0, n = Rd.length; r < n; r++) {
      var i = Rd[r], a = e.normal, o = e.emphasis;
      a && a[i] && (t[i] = t[i] || {}, t[i].normal ? pe(t[i].normal, a[i]) : t[i].normal = a[i], a[i] = null), o && o[i] && (t[i] = t[i] || {}, t[i].emphasis ? pe(t[i].emphasis, o[i]) : t[i].emphasis = o[i], o[i] = null);
    }
}
function Ne(t, e, r) {
  if (t && t[e] && (t[e].normal || t[e].emphasis)) {
    var n = t[e].normal, i = t[e].emphasis;
    n && (r ? (t[e].normal = t[e].emphasis = null, _e(t[e], n)) : t[e] = n), i && (t.emphasis = t.emphasis || {}, t.emphasis[e] = i, i.focus && (t.emphasis.focus = i.focus), i.blurScope && (t.emphasis.blurScope = i.blurScope));
  }
}
function Qi(t) {
  Ne(t, "itemStyle"), Ne(t, "lineStyle"), Ne(t, "areaStyle"), Ne(t, "label"), Ne(t, "labelLine"), Ne(t, "upperLabel"), Ne(t, "edgeLabel");
}
function we(t, e) {
  var r = Ta(t) && t[e], n = Ta(r) && r.textStyle;
  if (n)
    for (var i = 0, a = Ov.length; i < a; i++) {
      var o = Ov[i];
      n.hasOwnProperty(o) && (r[o] = n[o]);
    }
}
function pt(t) {
  t && (Qi(t), we(t, "label"), t.emphasis && we(t.emphasis, "label"));
}
function hM(t) {
  if (Ta(t)) {
    ml(t), Qi(t), we(t, "label"), we(t, "upperLabel"), we(t, "edgeLabel"), t.emphasis && (we(t.emphasis, "label"), we(t.emphasis, "upperLabel"), we(t.emphasis, "edgeLabel"));
    var e = t.markPoint;
    e && (ml(e), pt(e));
    var r = t.markLine;
    r && (ml(r), pt(r));
    var n = t.markArea;
    n && pt(n);
    var i = t.data;
    if (t.type === "graph") {
      i = i || t.nodes;
      var a = t.links || t.edges;
      if (a && !Ke(a))
        for (var o = 0; o < a.length; o++)
          pt(a[o]);
      T(t.categories, function(l) {
        Qi(l);
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
    t.type === "gauge" ? (we(t, "axisLabel"), we(t, "title"), we(t, "detail")) : t.type === "treemap" ? (Ne(t.breadcrumb, "itemStyle"), T(t.levels, function(l) {
      Qi(l);
    })) : t.type === "tree" && Qi(t.leaves);
  }
}
function nr(t) {
  return B(t) ? t : t ? [t] : [];
}
function Od(t) {
  return (B(t) ? t[0] : t) || {};
}
function cM(t, e) {
  Ct(nr(t.series), function(n) {
    Ta(n) && hM(n);
  });
  var r = ["xAxis", "yAxis", "radiusAxis", "angleAxis", "singleAxis", "parallelAxis", "radar"];
  e && r.push("valueAxis", "categoryAxis", "logAxis", "timeAxis"), Ct(r, function(n) {
    Ct(nr(t[n]), function(i) {
      i && (we(i, "axisLabel"), we(i.axisPointer, "label"));
    });
  }), Ct(nr(t.parallel), function(n) {
    var i = n && n.parallelAxisDefault;
    we(i, "axisLabel"), we(i && i.axisPointer, "label");
  }), Ct(nr(t.calendar), function(n) {
    Ne(n, "itemStyle"), we(n, "dayLabel"), we(n, "monthLabel"), we(n, "yearLabel");
  }), Ct(nr(t.radar), function(n) {
    we(n, "name"), n.name && n.axisName == null && (n.axisName = n.name, delete n.name), n.nameGap != null && n.axisNameGap == null && (n.axisNameGap = n.nameGap, delete n.nameGap);
  }), Ct(nr(t.geo), function(n) {
    Ta(n) && (pt(n), Ct(nr(n.regions), function(i) {
      pt(i);
    }));
  }), Ct(nr(t.timeline), function(n) {
    pt(n), Ne(n, "label"), Ne(n, "itemStyle"), Ne(n, "controlStyle", !0);
    var i = n.data;
    B(i) && T(i, function(a) {
      F(a) && (Ne(a, "label"), Ne(a, "itemStyle"));
    });
  }), Ct(nr(t.toolbox), function(n) {
    Ne(n, "iconStyle"), Ct(n.feature, function(i) {
      Ne(i, "iconStyle");
    });
  }), we(Od(t.axisPointer), "label"), we(Od(t.tooltip).axisPointer, "label");
}
function vM(t, e) {
  for (var r = e.split(","), n = t, i = 0; i < r.length && (n = n && n[r[i]], n != null); i++)
    ;
  return n;
}
function dM(t, e, r, n) {
  for (var i = e.split(","), a = t, o, s = 0; s < i.length - 1; s++)
    o = i[s], a[o] == null && (a[o] = {}), a = a[o];
  a[i[s]] == null && (a[i[s]] = r);
}
function kd(t) {
  t && T(pM, function(e) {
    e[0] in t && !(e[1] in t) && (t[e[1]] = t[e[0]]);
  });
}
var pM = [["x", "left"], ["y", "top"], ["x2", "right"], ["y2", "bottom"]], gM = ["grid", "geo", "parallel", "legend", "toolbox", "title", "visualMap", "dataZoom", "timeline"], _l = [["borderRadius", "barBorderRadius"], ["borderColor", "barBorderColor"], ["borderWidth", "barBorderWidth"]];
function ki(t) {
  var e = t && t.itemStyle;
  if (e)
    for (var r = 0; r < _l.length; r++) {
      var n = _l[r][1], i = _l[r][0];
      e[n] != null && (e[i] = e[n]);
    }
}
function Bd(t) {
  t && t.alignTo === "edge" && t.margin != null && t.edgeDistance == null && (t.edgeDistance = t.margin);
}
function Nd(t) {
  t && t.downplay && !t.blur && (t.blur = t.downplay);
}
function yM(t) {
  t && t.focusNodeAdjacency != null && (t.emphasis = t.emphasis || {}, t.emphasis.focus == null && (t.emphasis.focus = "adjacency"));
}
function C_(t, e) {
  if (t)
    for (var r = 0; r < t.length; r++)
      e(t[r]), t[r] && C_(t[r].children, e);
}
function x_(t, e) {
  cM(t, e), t.series = $e(t.series), T(t.series, function(r) {
    if (F(r)) {
      var n = r.type;
      if (n === "line")
        r.clipOverflow != null && (r.clip = r.clipOverflow);
      else if (n === "pie" || n === "gauge") {
        r.clockWise != null && (r.clockwise = r.clockWise), Bd(r.label);
        var i = r.data;
        if (i && !Ke(i))
          for (var a = 0; a < i.length; a++)
            Bd(i[a]);
        r.hoverOffset != null && (r.emphasis = r.emphasis || {}, (r.emphasis.scaleSize = null) && (r.emphasis.scaleSize = r.hoverOffset));
      } else if (n === "gauge") {
        var o = vM(r, "pointer.color");
        o != null && dM(r, "itemStyle.color", o);
      } else if (n === "bar") {
        ki(r), ki(r.backgroundStyle), ki(r.emphasis);
        var i = r.data;
        if (i && !Ke(i))
          for (var a = 0; a < i.length; a++)
            typeof i[a] == "object" && (ki(i[a]), ki(i[a] && i[a].emphasis));
      } else if (n === "sunburst") {
        var s = r.highlightPolicy;
        s && (r.emphasis = r.emphasis || {}, r.emphasis.focus || (r.emphasis.focus = s)), Nd(r), C_(r.data, Nd);
      } else n === "graph" || n === "sankey" ? yM(r) : n === "map" && (r.mapType && !r.map && (r.map = r.mapType), r.mapLocation && _e(r, r.mapLocation));
      r.hoverAnimation != null && (r.emphasis = r.emphasis || {}, r.emphasis && r.emphasis.scale == null && (r.emphasis.scale = r.hoverAnimation)), kd(r);
    }
  }), t.dataRange && (t.visualMap = t.dataRange), T(gM, function(r) {
    var n = t[r];
    n && (B(n) || (n = [n]), T(n, function(i) {
      kd(i);
    }));
  });
}
var mM = Ih(_M);
function _M(t) {
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
      i === "seriesDesc" && r.reverse(), T(r, function(a, o) {
        a.data.setCalculationInfo("stackedOnSeries", o > 0 ? r[o - 1].seriesModel : null);
      }), SM(r);
    }
  });
}
function SM(t) {
  T(t, function(e, r) {
    var n = [], i = [NaN, NaN], a = [e.stackResultDimension, e.stackedOverDimension], o = e.data, s = e.isStackedByIndex, u = e.seriesModel.get("stackStrategy") || "samesign";
    o.modify(a, function(l, f, h) {
      var v = o.get(e.stackedDimension, h);
      if (isNaN(v))
        return i;
      var c, d;
      s ? d = o.getRawIndex(h) : c = o.get(e.stackedByDimension, h);
      for (var y = NaN, p = r - 1; p >= 0; p--) {
        var g = t[p];
        if (s || (d = g.data.rawIndexOf(g.stackedByDimension, c)), d >= 0) {
          var m = g.data.getByRawIndex(g.stackResultDimension, d);
          if (u === "all" || u === "positive" && m > 0 || u === "negative" && m < 0 || u === "samesign" && v >= 0 && m > 0 || u === "samesign" && v <= 0 && m < 0) {
            v = sw(v, m), y = m;
            break;
          }
        }
      }
      return n[0] = v, n[1] = y, n;
    });
  });
}
var du = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.data = e.data || (e.sourceFormat === jt ? {} : []), this.sourceFormat = e.sourceFormat || dm, this.seriesLayoutBy = e.seriesLayoutBy || Pt, this.startIndex = e.startIndex || 0, this.dimensionsDetectedCount = e.dimensionsDetectedCount, this.metaRawOption = e.metaRawOption;
      var r = this.dimensionsDefine = e.dimensionsDefine;
      if (r)
        for (var n = 0; n < r.length; n++) {
          var i = r[n];
          i.type == null && T_(this, n) === nt.Must && (i.type = "ordinal");
        }
    }
    return t;
  })()
);
function fc(t) {
  return t instanceof du;
}
function Bf(t, e, r) {
  r = r || D_(t);
  var n = e.seriesLayoutBy, i = wM(t, r, n, e.sourceHeader, e.dimensions), a = new du({
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
function M_(t) {
  return new du({
    data: t,
    sourceFormat: Ke(t) ? Lr : ft
  });
}
function bM(t) {
  return new du({
    data: t.data,
    sourceFormat: t.sourceFormat,
    seriesLayoutBy: t.seriesLayoutBy,
    dimensionsDefine: Z(t.dimensionsDefine),
    startIndex: t.startIndex,
    dimensionsDetectedCount: t.dimensionsDetectedCount
  });
}
function D_(t) {
  var e = dm;
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
        } else if (F(i)) {
          e = Et;
          break;
        }
      }
    }
  } else if (F(t)) {
    for (var a in t)
      if (qe(t, a) && Ze(t[a])) {
        e = jt;
        break;
      }
  }
  return e;
}
function wM(t, e, r, n, i) {
  var a, o;
  if (!t)
    return {
      dimensionsDefine: Fd(i),
      startIndex: o,
      dimensionsDetectedCount: a
    };
  if (e === ze) {
    var s = t;
    n === "auto" || n == null ? zd(function(l) {
      l != null && l !== "-" && (N(l) ? o == null && (o = 1) : o = 0);
    }, r, s, 10) : o = ve(n) ? n : n ? 1 : 0, !i && o === 1 && (i = [], zd(function(l, f) {
      i[f] = l != null ? l + "" : "";
    }, r, s, 1 / 0)), a = i ? i.length : r === An ? s.length : s[0] ? s[0].length : null;
  } else if (e === Et)
    i || (i = TM(t));
  else if (e === jt)
    i || (i = [], T(t, function(l, f) {
      i.push(f);
    }));
  else if (e === ft) {
    var u = Ra(t[0]);
    a = B(u) && u.length || 1;
  }
  return {
    startIndex: o,
    dimensionsDefine: Fd(i),
    dimensionsDetectedCount: a
  };
}
function TM(t) {
  for (var e = 0, r; e < t.length && !(r = t[e++]); )
    ;
  if (r)
    return me(r);
}
function Fd(t) {
  if (t) {
    var e = U();
    return H(t, function(r, n) {
      r = F(r) ? r : {
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
function zd(t, e, r, n) {
  if (e === An)
    for (var i = 0; i < r.length && i < n; i++)
      t(r[i] ? r[i][0] : null, i);
  else
    for (var a = r[0] || [], i = 0; i < a.length && i < n; i++)
      t(a[i], i);
}
function I_(t) {
  var e = t.sourceFormat;
  return e === Et || e === jt;
}
var nn, an, on, sn, Hd, Vd, A_ = (
  /** @class */
  (function() {
    function t(e, r) {
      var n = fc(e) ? e : M_(e);
      this._source = n;
      var i = this._data = n.data, a = n.sourceFormat;
      n.seriesLayoutBy, a === Lr && (this._offset = 0, this._dimSize = r, this._data = i), Vd(this, i, n);
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
      Vd = function(o, s, u) {
        var l = u.sourceFormat, f = u.seriesLayoutBy, h = u.startIndex, v = u.dimensionsDefine, c = Hd[hc(l, f)];
        if (R(o, c), l === Lr)
          o.getItem = r, o.count = i, o.fillStorage = n;
        else {
          var d = P_(l, f);
          o.getItem = le(d, null, s, h, v);
          var y = L_(l, f);
          o.count = le(y, null, s, h, v);
        }
      };
      var r = function(o, s) {
        o = o - this._offset, s = s || [];
        for (var u = this._data, l = this._dimSize, f = l * o, h = 0; h < l; h++)
          s[h] = u[f + h];
        return s;
      }, n = function(o, s, u, l) {
        for (var f = this._data, h = this._dimSize, v = 0; v < h; v++) {
          for (var c = l[v], d = c[0] == null ? 1 / 0 : c[0], y = c[1] == null ? -1 / 0 : c[1], p = s - o, g = u[v], m = 0; m < p; m++) {
            var _ = f[m * h + v];
            g[o + m] = _, _ < d && (d = _), _ > y && (y = _);
          }
          c[0] = d, c[1] = y;
        }
      }, i = function() {
        return this._data ? this._data.length / this._dimSize : 0;
      };
      Hd = (e = {}, e[ze + "_" + Pt] = {
        pure: !0,
        appendData: a
      }, e[ze + "_" + An] = {
        pure: !0,
        appendData: function() {
          throw new Error('Do not support appendData when set seriesLayoutBy: "row".');
        }
      }, e[Et] = {
        pure: !0,
        appendData: a
      }, e[jt] = {
        pure: !0,
        appendData: function(o) {
          var s = this._data;
          T(o, function(u, l) {
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
  B(t) || $y("series.data or dataset.source must be an array.");
};
nn = {}, nn[ze + "_" + Pt] = co, nn[ze + "_" + An] = co, nn[Et] = co, nn[jt] = function(t, e) {
  for (var r = 0; r < e.length; r++) {
    var n = e[r].name;
    n == null && $y("dimension name must not be null/undefined.");
  }
}, nn[ft] = co;
var Gd = function(t, e, r, n) {
  return t[n];
}, CM = (an = {}, an[ze + "_" + Pt] = function(t, e, r, n) {
  return t[n + e];
}, an[ze + "_" + An] = function(t, e, r, n, i) {
  n += e;
  for (var a = i || [], o = t, s = 0; s < o.length; s++) {
    var u = o[s];
    a[s] = u ? u[n] : null;
  }
  return a;
}, an[Et] = Gd, an[jt] = function(t, e, r, n, i) {
  for (var a = i || [], o = 0; o < r.length; o++) {
    var s = r[o].name, u = s != null ? t[s] : null;
    a[o] = u ? u[n] : null;
  }
  return a;
}, an[ft] = Gd, an);
function P_(t, e) {
  var r = CM[hc(t, e)];
  return r;
}
var Ud = function(t, e, r) {
  return t.length;
}, xM = (on = {}, on[ze + "_" + Pt] = function(t, e, r) {
  return Math.max(0, t.length - e);
}, on[ze + "_" + An] = function(t, e, r) {
  var n = t[0];
  return n ? Math.max(0, n.length - e) : 0;
}, on[Et] = Ud, on[jt] = function(t, e, r) {
  var n = r[0].name, i = n != null ? t[n] : null;
  return i ? i.length : 0;
}, on[ft] = Ud, on);
function L_(t, e) {
  var r = xM[hc(t, e)];
  return r;
}
var Sl = function(t, e, r) {
  return t[e];
}, MM = (sn = {}, sn[ze] = Sl, sn[Et] = function(t, e, r) {
  return t[r];
}, sn[jt] = Sl, sn[ft] = function(t, e, r) {
  var n = Ra(t);
  return n instanceof Array ? n[e] : n;
}, sn[Lr] = Sl, sn);
function E_(t) {
  var e = MM[t];
  return e;
}
function hc(t, e) {
  return t === ze ? t + "_" + e : t;
}
function bs(t, e, r) {
  if (t) {
    var n = t.getRawDataItem(e);
    if (n != null) {
      var i = t.getStore(), a = i.getSource().sourceFormat;
      if (r != null) {
        var o = t.getDimensionIndex(r), s = i.getDimensionProperty(o);
        return E_(a)(n, o, s);
      } else {
        var u = n;
        return a === ft && (u = Ra(n)), u;
      }
    }
  }
}
var DM = /\{@(.+?)\}/g, IM = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.getDataParams = function(e, r) {
      var n = this.getData(r), i = this.getRawValue(e, r), a = n.getRawIndex(e), o = n.getName(e), s = n.getRawDataItem(e), u = n.getItemVisual(e, "style"), l = u && u[n.getItemVisual(e, "drawType") || "fill"], f = u && u.stroke, h = this.mainType, v = h === "series", c = n.userOutput && n.userOutput.get();
      return {
        componentType: h,
        componentSubType: this.subType,
        componentIndex: this.componentIndex,
        seriesType: v ? this.subType : null,
        seriesIndex: this.seriesIndex,
        seriesId: v ? this.id : null,
        seriesName: v ? this.name : null,
        name: o,
        dataIndex: a,
        data: s,
        dataType: r,
        value: i,
        color: l,
        borderColor: f,
        dimensionNames: c ? c.fullDimensions : null,
        encode: c ? c.encode : null,
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
        var f = ac(a, u);
        return f.replace(DM, function(h, v) {
          var c = v.length, d = v;
          d.charAt(0) === "[" && d.charAt(c - 1) === "]" && (d = +d.slice(1, c - 1));
          var y = bs(s, e, d);
          if (o && B(o.interpolatedValue)) {
            var p = s.getDimensionIndex(d);
            p >= 0 && (y = o.interpolatedValue[p]);
          }
          return y != null ? y + "" : "";
        });
      }
    }, t.prototype.getRawValue = function(e, r) {
      return bs(this.getData(r), e);
    }, t.prototype.formatTooltip = function(e, r, n) {
    }, t;
  })()
);
function Yd(t) {
  var e, r;
  return F(t) ? t.type && (r = t) : e = t, {
    text: e,
    // markers: markers || markersExisting,
    frag: r
  };
}
function ua(t) {
  return new AM(t);
}
var AM = (
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
      var v = e && e.step;
      if (r ? this._dueEnd = r._outputDueEnd : this._dueEnd = this._count ? this._count(this.context) : 1 / 0, this._progress) {
        var c = this._dueIndex, d = Math.min(v != null ? this._dueIndex + v : 1 / 0, this._dueEnd);
        if (!n && (h || c < d)) {
          var y = this._progress;
          if (B(y))
            for (var p = 0; p < y.length; p++)
              this._doProgress(y[p], c, d, u, l);
          else
            this._doProgress(y, c, d, u, l);
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
      Wd.reset(r, n, i, a), this._callingProgress = e, this._callingProgress({
        start: r,
        end: n,
        count: n - r,
        next: Wd.next
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
), Wd = /* @__PURE__ */ (function() {
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
var PM = U({
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
function ZE(t) {
  return PM.get(t);
}
var R_ = {
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
}, LM = (
  /** @class */
  (function() {
    function t(e, r) {
      if (!ve(r)) {
        var n = "";
        Ye(n);
      }
      this._opFn = R_[e], this._rvalFloat = cr(r);
    }
    return t.prototype.evaluate = function(e) {
      return ve(e) ? this._opFn(e, this._rvalFloat) : this._opFn(cr(e), this._rvalFloat);
    }, t;
  })()
), EM = (
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
), RM = (
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
function KE(t, e) {
  return t === "eq" || t === "ne" ? new RM(t === "eq", e) : qe(R_, t) ? new LM(t, e) : null;
}
function OM(t) {
  var e = "", r = -1 / 0, n = -1 / 0, i = 1 / 0, a = 1 / 0;
  return t && (t.g != null && (e += "G" + t.g, r = t.g), t.ge != null && (e += "GE" + t.ge, n = t.ge), t.l != null && (e += "L" + t.l, i = t.l), t.le != null && (e += "LE" + t.le, a = t.le)), {
    key: e,
    g: r,
    ge: n,
    l: i,
    le: a
  };
}
function kM(t, e) {
  return e > t.g && e >= t.ge && e < t.l && e <= t.le;
}
var BM = (
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
function NM(t, e) {
  var r = new BM(), n = t.data, i = r.sourceFormat = t.sourceFormat, a = t.startIndex, o = "";
  t.seriesLayoutBy !== Pt && Ye(o);
  var s = [], u = {}, l = t.dimensionsDefine;
  if (l)
    T(l, function(y, p) {
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
  var h = P_(i, Pt);
  e.__isBuiltIn && (r.getRawDataItem = function(y) {
    return h(n, a, s, y);
  }, r.getRawData = le(FM, null, t)), r.cloneRawData = le(zM, null, t);
  var v = L_(i, Pt);
  r.count = le(v, null, n, a, s);
  var c = E_(i);
  r.retrieveValue = function(y, p) {
    var g = h(n, a, s, y);
    return d(g, p);
  };
  var d = r.retrieveValueFromItem = function(y, p) {
    if (y != null) {
      var g = s[p];
      if (g)
        return c(y, p, g.name);
    }
  };
  return r.getDimensionInfo = le(HM, null, s, u), r.cloneAllDimensionInfo = le(VM, null, s), r;
}
function FM(t) {
  var e = t.sourceFormat;
  if (!cc(e)) {
    var r = "";
    Ye(r);
  }
  return t.data;
}
function zM(t) {
  var e = t.sourceFormat, r = t.data;
  if (!cc(e)) {
    var n = "";
    Ye(n);
  }
  if (e === ze) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(r[a].slice());
    return i;
  } else if (e === Et) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(R({}, r[a]));
    return i;
  }
}
function HM(t, e, r) {
  if (r != null) {
    if (ve(r) || !isNaN(r) && !qe(e, r))
      return t[r];
    if (qe(e, r))
      return e[r];
  }
}
function VM(t) {
  return Z(t);
}
var O_ = U();
function GM(t) {
  t = Z(t);
  var e = t.type, r = "";
  e || Ye(r);
  var n = e.split(":");
  n.length !== 2 && Ye(r);
  var i = !1;
  n[0] === "echarts" && (e = n[1], i = !0), t.__isBuiltIn = i, O_.set(e, t);
}
function UM(t, e, r) {
  var n = $e(t), i = n.length, a = "";
  i || Ye(a);
  for (var o = 0, s = i; o < s; o++) {
    var u = n[o];
    e = YM(u, e), o !== s - 1 && (e.length = Math.max(e.length, 1));
  }
  return e;
}
function YM(t, e, r, n) {
  var i = "";
  e.length || Ye(i), F(t) || Ye(i);
  var a = t.type, o = O_.get(a);
  o || Ye(i);
  var s = H(e, function(l) {
    return NM(l, o);
  }), u = $e(o.transform({
    upstream: s[0],
    upstreamList: s,
    config: Z(t.config)
  }));
  return H(u, function(l, f) {
    var h = "";
    F(l) || Ye(h), l.data || Ye(h);
    var v = D_(l.data);
    cc(v) || Ye(h);
    var c, d = e[0];
    if (d && f === 0 && !l.dimensions) {
      var y = d.startIndex;
      y && (l.data = d.data.slice(0, y).concat(l.data)), c = {
        seriesLayoutBy: Pt,
        sourceHeader: y,
        dimensions: d.metaRawOption.dimensions
      };
    } else
      c = {
        seriesLayoutBy: Pt,
        sourceHeader: 0,
        dimensions: l.dimensions
      };
    return Bf(l.data, c, null);
  });
}
function cc(t) {
  return t === ze || t === Et;
}
var WM = typeof Uint32Array === eu ? Array : Uint32Array, XM = typeof Uint16Array === eu ? Array : Uint16Array, k_ = typeof Int32Array === eu ? Array : Int32Array, Xd = typeof Float64Array === eu ? Array : Float64Array, B_ = {
  float: Xd,
  int: k_,
  // Ordinal data type can be string or int
  ordinal: Array,
  number: Array,
  time: Xd
}, bl;
function Vn(t) {
  return t > 65535 ? WM : XM;
}
function qM(t) {
  var e = t.constructor;
  return e === Array ? t.slice() : new e(t);
}
function qd(t, e, r, n, i) {
  var a = B_[r || "float"];
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
var Nf = (
  /** @class */
  (function() {
    function t() {
      this._chunks = [], this._rawExtent = [], this._extent = [], this._count = 0, this._rawCount = 0, this._calcDimNameToIdx = U();
    }
    return t.prototype.initData = function(e, r, n) {
      this._provider = e, this._chunks = [], this._indices = null, this.getRawIndex = this._getRawIdxIdentity;
      var i = e.getSource(), a = this.defaultDimValueGetter = bl[i.sourceFormat];
      this._dimValueGetter = n || a, this._rawExtent = [], I_(i), this._dimensions = H(r, function(o) {
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
      }, n.set(e, a), this._chunks[a] = new B_[r || "float"](this._rawCount), this._rawExtent[a] = zt(), a;
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
        qd(n, l, f.type, u, !0);
      }
      for (var h = [], v = s; v < u; v++)
        for (var c = v - s, d = 0; d < a; d++) {
          var f = i[d], y = bl.arrayRows.call(this, e[c] || h, f.property, c, d);
          n[d][v] = y;
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
        u[f] || (u[f] = zt()), qd(a, f, h.type, r, n);
      }
      if (i.fillStorage)
        i.fillStorage(e, r, a, u);
      else
        for (var v = [], c = e; c < r; c++) {
          v = i.getItem(c, v);
          for (var d = 0; d < s; d++) {
            var y = a[d], p = this._dimValueGetter(v, l[d], c, d);
            y[c] = p;
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
      }), bh(r);
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
      for (var n = this.clone(), i = n.count(), a = Vn(n._rawCount), o = new a(i), s = [], u = e.length, l = 0, f = e[0], h = n._chunks, v = 0; v < i; v++) {
        var c = void 0, d = n.getRawIndex(v);
        if (u === 0)
          c = r(v);
        else if (u === 1) {
          var y = h[f][d];
          c = r(y, v);
        } else {
          for (var p = 0; p < u; p++)
            s[p] = h[e[p]][d];
          s[p] = v, c = r.apply(null, s);
        }
        c && (o[l++] = d);
      }
      return l < i && (n._indices = o), n._count = l, n._extent = [], n._updateGetRawIdx(), n;
    }, t.prototype.selectRange = function(e) {
      var r = this.clone(), n = r._count;
      if (!n)
        return this;
      var i = me(e), a = i.length;
      if (!a)
        return this;
      var o = r.count(), s = Vn(r._rawCount), u = new s(o), l = 0, f = i[0], h = e[f][0], v = e[f][1], c = r._chunks, d = !1;
      if (!r._indices) {
        var y = 0;
        if (a === 1) {
          for (var p = c[i[0]], g = 0; g < n; g++) {
            var m = p[g];
            (m >= h && m <= v || isNaN(m)) && (u[l++] = y), y++;
          }
          d = !0;
        } else if (a === 2) {
          for (var p = c[i[0]], _ = c[i[1]], S = e[i[1]][0], b = e[i[1]][1], g = 0; g < n; g++) {
            var m = p[g], w = _[g];
            (m >= h && m <= v || isNaN(m)) && (w >= S && w <= b || isNaN(w)) && (u[l++] = y), y++;
          }
          d = !0;
        }
      }
      if (!d)
        if (a === 1)
          for (var g = 0; g < o; g++) {
            var C = r.getRawIndex(g), m = c[i[0]][C];
            (m >= h && m <= v || isNaN(m)) && (u[l++] = C);
          }
        else
          for (var g = 0; g < o; g++) {
            for (var M = !0, C = r.getRawIndex(g), x = 0; x < a; x++) {
              var D = i[x], m = c[D][C];
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
        for (var v = e.getRawIndex(h), c = 0; c < o; c++)
          u[c] = i[r[c]][v];
        u[o] = h;
        var d = n && n.apply(null, u);
        if (d != null) {
          typeof d != "object" && (a[0] = d, d = a);
          for (var f = 0; f < d.length; f++) {
            var y = r[f], p = d[f], g = l[y], m = i[y];
            m && (m[v] = p), p < g[0] && (g[0] = p), p > g[1] && (g[1] = p);
          }
        }
      }
    }, t.prototype.lttbDownSample = function(e, r) {
      var n = this.clone([e], !0), i = n._chunks, a = i[e], o = this.count(), s = 0, u = Math.floor(1 / r), l = this.getRawIndex(0), f, h, v, c = new (Vn(this._rawCount))(Math.min((Math.ceil(o / u) + 2) * 2, o));
      c[s++] = l;
      for (var d = 1; d < o - 1; d += u) {
        for (var y = Math.min(d + u, o - 1), p = Math.min(d + u * 2, o), g = (p + y) / 2, m = 0, _ = y; _ < p; _++) {
          var S = this.getRawIndex(_), b = a[S];
          isNaN(b) || (m += b);
        }
        m /= p - y;
        var w = d, C = Math.min(d + u, o), M = d - 1, x = a[l];
        f = -1, v = w;
        for (var D = -1, I = 0, _ = w; _ < C; _++) {
          var S = this.getRawIndex(_), b = a[S];
          if (isNaN(b)) {
            I++, D < 0 && (D = S);
            continue;
          }
          h = Math.abs((M - g) * (b - x) - (M - _) * (m - x)), h > f && (f = h, v = S);
        }
        I > 0 && I < C - w && (c[s++] = Math.min(D, v), v = Math.max(D, v)), c[s++] = v, l = v;
      }
      return c[s++] = this.getRawIndex(o - 1), n._count = s, n._indices = c, n.getRawIndex = this._getRawIdx, n;
    }, t.prototype.minmaxDownSample = function(e, r) {
      for (var n = this.clone([e], !0), i = n._chunks, a = Math.floor(1 / r), o = i[e], s = this.count(), u = new (Vn(this._rawCount))(Math.ceil(s / a) * 2), l = 0, f = 0; f < s; f += a) {
        var h = f, v = o[this.getRawIndex(h)], c = f, d = o[this.getRawIndex(c)], y = a;
        f + a > s && (y = s - f);
        for (var p = 0; p < y; p++) {
          var g = this.getRawIndex(f + p), m = o[g];
          m < v && (v = m, h = f + p), m > d && (d = m, c = f + p);
        }
        var _ = this.getRawIndex(h), S = this.getRawIndex(c);
        h < c ? (u[l++] = _, u[l++] = S) : (u[l++] = S, u[l++] = _);
      }
      return n._count = l, n._indices = u, n._updateGetRawIdx(), n;
    }, t.prototype.downSample = function(e, r, n, i) {
      for (var a = this.clone([e], !0), o = a._chunks, s = [], u = Math.floor(1 / r), l = o[e], f = this.count(), h = a._rawExtent[e] = zt(), v = new (Vn(this._rawCount))(Math.ceil(f / u)), c = 0, d = 0; d < f; d += u) {
        u > f - d && (u = f - d, s.length = u);
        for (var y = 0; y < u; y++) {
          var p = this.getRawIndex(d + y);
          s[y] = l[p];
        }
        var g = n(s), m = this.getRawIndex(Math.min(d + i(s, g) || 0, f - 1));
        l[m] = g, g < h[0] && (h[0] = g), g > h[1] && (h[1] = g), v[c++] = m;
      }
      return a._count = c, a._indices = v, a._updateGetRawIdx(), a;
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
      var s = this._extent, u = s[e] || (s[e] = {}), l = OM(r), f = l.key, h = u[f];
      if (h)
        return h.slice();
      for (var v = i[0], c = i[1], d = 0; d < a; d++) {
        var y = this.getRawIndex(d), p = n[y];
        (!r || kM(l, p)) && (p < v && (v = p), p > c && (c = p));
      }
      return u[f] = [v, c];
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
          n._chunks[o] = a[o] ? qM(i[o]) : i[o];
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
      bl = {
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
), N_ = (
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
        var h = this._getSourceMetaRawOption() || {}, v = l && l.metaRawOption || {}, c = G(h.seriesLayoutBy, v.seriesLayoutBy) || null, d = G(h.sourceHeader, v.sourceHeader), y = G(h.dimensions, v.dimensions), p = c !== v.seriesLayoutBy || !!d != !!v.sourceHeader || y;
        i = p ? [Bf(s, {
          seriesLayoutBy: c,
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
          i = [Bf(_, this._getSourceMetaRawOption(), null)], a = [];
        }
      }
      this._setLocalSource(i, a);
    }, t.prototype._applyTransform = function(e) {
      var r = this._sourceHost, n = r.get("transform", !0), i = r.get("fromTransformResult", !0);
      if (i != null) {
        var a = "";
        e.length !== 1 && Zd(a);
      }
      var o, s = [], u = [];
      return T(e, function(l) {
        l.prepareSource();
        var f = l.getSource(i || 0), h = "";
        i != null && !f && Zd(h), s.push(f), u.push(l._getVersionSign());
      }), n ? o = UM(n, s, {
        datasetIndex: r.componentIndex
      }) : i != null && (o = [bM(s[0])]), {
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
        vo(this._sourceHost) && u ? s = u._innerGetDataStore(e, r, n) : (s = new Nf(), s.initData(new A_(r, e.length), e)), o[n] = s;
      }
      return s;
    }, t.prototype._getUpstreamSourceManagers = function() {
      var e = this._sourceHost;
      if (vo(e)) {
        var r = w_(e);
        return r ? [r.getSourceManager()] : [];
      } else
        return H(Zx(e), function(n) {
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
function $d(t) {
  var e = t.option.transform;
  e && va(t.option.transform);
}
function vo(t) {
  return t.mainType === "series";
}
function Zd(t) {
  throw new Error(t);
}
var $M = "line-height:1";
function F_(t) {
  var e = t.lineHeight;
  return e == null ? $M : "line-height:" + Ue(e + "") + "px";
}
function z_(t, e) {
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
var ZM = [0, 10, 20, 30], KM = ["", `
`, `

`, `


`];
function Ca(t, e) {
  return e.type = t, e;
}
function Ff(t) {
  return t.type === "section";
}
function H_(t) {
  return Ff(t) ? jM : QM;
}
function V_(t) {
  if (Ff(t)) {
    var e = 0, r = t.blocks.length, n = r > 1 || r > 0 && !t.noHeader;
    return T(t.blocks, function(i) {
      var a = V_(i);
      a >= e && (e = a + +(n && // 0 always can not be readable gap level.
      (!a || Ff(i) && !i.noHeader)));
    }), e;
  }
  return 0;
}
function jM(t, e, r, n) {
  var i = e.noHeader, a = JM(V_(e)), o = [], s = e.blocks || [];
  qt(!s || B(s)), s = s || [];
  var u = t.orderMode;
  if (e.sortBlocks && u) {
    s = s.slice();
    var l = {
      valueAsc: "asc",
      valueDesc: "desc"
    };
    if (qe(l, u)) {
      var f = new EM(l[u], null);
      s.sort(function(y, p) {
        return f.evaluate(y.sortParam, p.sortParam);
      });
    } else u === "seriesDesc" && s.reverse();
  }
  T(s, function(y, p) {
    var g = e.valueFormatter, m = H_(y)(
      // Inherit valueFormatter
      g ? R(R({}, t), {
        valueFormatter: g
      }) : t,
      y,
      p > 0 ? a.html : 0,
      n
    );
    m != null && o.push(m);
  });
  var h = t.renderMode === "richText" ? o.join(a.richText) : zf(n, o.join(""), i ? r : a.html);
  if (i)
    return h;
  var v = kf(e.header, "ordinal", t.useUTC), c = z_(n, t.renderMode).nameStyle, d = F_(n);
  return t.renderMode === "richText" ? G_(t, v, c) + a.richText + h : zf(n, '<div style="' + c + ";" + d + ';">' + Ue(v) + "</div>" + h, r);
}
function QM(t, e, r, n) {
  var i = t.renderMode, a = e.noName, o = e.noValue, s = !e.markerType, u = e.name, l = t.useUTC, f = e.valueFormatter || t.valueFormatter || function(S) {
    return S = B(S) ? S : [S], H(S, function(b, w) {
      return kf(b, B(c) ? c[w] : c, l);
    });
  };
  if (!(a && o)) {
    var h = s ? "" : t.markupStyleCreator.makeTooltipMarker(e.markerType, e.markerColor || j.color.secondary, i), v = a ? "" : kf(u, "ordinal", l), c = e.valueType, d = o ? [] : f(e.value, e.rawDataIndex), y = !s || !a, p = !s && a, g = z_(n, i), m = g.nameStyle, _ = g.valueStyle;
    return i === "richText" ? (s ? "" : h) + (a ? "" : G_(t, v, m)) + (o ? "" : rD(t, d, y, p, _)) : zf(n, (s ? "" : h) + (a ? "" : eD(v, !s, m)) + (o ? "" : tD(d, y, p, _)), r);
  }
}
function Kd(t, e, r, n, i, a) {
  if (t) {
    var o = H_(t), s = {
      useUTC: i,
      renderMode: r,
      orderMode: n,
      markupStyleCreator: e,
      valueFormatter: t.valueFormatter
    };
    return o(s, t, 0, a);
  }
}
function JM(t) {
  return {
    html: ZM[t],
    richText: KM[t]
  };
}
function zf(t, e, r) {
  var n = '<div style="clear:both"></div>', i = "margin: " + r + "px 0 0", a = F_(t);
  return '<div style="' + i + ";" + a + ';">' + e + n + "</div>";
}
function eD(t, e, r) {
  var n = e ? "margin-left:2px" : "";
  return '<span style="' + r + ";" + n + '">' + Ue(t) + "</span>";
}
function tD(t, e, r, n) {
  var i = r ? "10px" : "20px", a = e ? "float:right;margin-left:" + i : "";
  return t = B(t) ? t : [t], '<span style="' + a + ";" + n + '">' + H(t, function(o) {
    return Ue(o);
  }).join("&nbsp;&nbsp;") + "</span>";
}
function G_(t, e, r) {
  return t.markupStyleCreator.wrapRichTextStyle(e, r);
}
function rD(t, e, r, n, i) {
  var a = [i], o = n ? 10 : 20;
  return r && a.push({
    padding: [0, 0, 0, o],
    align: "right"
  }), t.markupStyleCreator.wrapRichTextStyle(B(e) ? e.join("  ") : e, a);
}
function nD(t, e) {
  var r = t.getData().getItemVisual(e, "style"), n = r[t.visualDrawType];
  return ba(n);
}
function U_(t, e) {
  var r = t.get("padding");
  return r ?? (e === "richText" ? [8, 10] : 10);
}
var wl = (
  /** @class */
  (function() {
    function t() {
      this.richTextStyles = {}, this._nextStyleNameId = Ch();
    }
    return t.prototype._generateStyleName = function() {
      return "__EC_aUTo_" + this._nextStyleNameId++;
    }, t.prototype.makeTooltipMarker = function(e, r, n) {
      var i = n === "richText" ? this._generateStyleName() : null, a = g_({
        color: r,
        type: e,
        renderMode: n,
        markerId: i
      });
      return N(a) ? a : (this.richTextStyles[i] = a.style, a.content);
    }, t.prototype.wrapRichTextStyle = function(e, r) {
      var n = {};
      B(r) ? T(r, function(a) {
        return R(n, a);
      }) : R(n, r);
      var i = this._generateStyleName();
      return this.richTextStyles[i] = n, "{" + i + "|" + e + "}";
    }, t;
  })()
);
function iD(t) {
  var e = t.series, r = t.dataIndex, n = t.multipleSeries, i = e.getData(), a = i.mapDimensionsAll("defaultedTooltip"), o = a.length, s = e.getRawValue(r), u = B(s), l = nD(e, r), f, h, v, c;
  if (o > 1 || u && !o) {
    var d = aD(s, e, r, a, l);
    f = d.inlineValues, h = d.inlineValueTypes, v = d.blocks, c = d.inlineValues[0];
  } else if (o) {
    var y = i.getDimensionInfo(a[0]);
    c = f = bs(i, r, a[0]), h = y.type;
  } else
    c = f = u ? s[0] : s;
  var p = xh(e), g = p && e.name || "", m = i.getName(r), _ = n ? g : m;
  return Ca("section", {
    header: g,
    // When series name is not specified, do not show a header line with only '-'.
    // This case always happens in tooltip.trigger: 'item'.
    noHeader: n || !p,
    sortParam: c,
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
    })].concat(v || [])
  });
}
function aD(t, e, r, n, i) {
  var a = e.getData(), o = Xt(t, function(h, v, c) {
    var d = a.getDimensionInfo(c);
    return h = h || d && d.tooltip !== !1 && d.displayName != null;
  }, !1), s = [], u = [], l = [];
  n.length ? T(n, function(h) {
    f(bs(a, r, h), h);
  }) : T(t, f);
  function f(h, v) {
    var c = a.getDimensionInfo(v);
    !c || c.otherDims.tooltip === !1 || (o ? l.push(Ca("nameValue", {
      markerType: "subItem",
      markerColor: i,
      name: c.displayName,
      value: h,
      valueType: c.type
    })) : (s.push(h), u.push(c.type)));
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
var oD = "__universalTransitionEnabled", $t = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r._selectedDataIndicesMap = {}, r;
    }
    return e.prototype.init = function(r, n, i) {
      this.seriesIndex = this.componentIndex, this.dataTask = ua({
        count: uD,
        reset: lD
      }), this.dataTask.context = {
        model: this
      }, this.mergeDefaultAndTheme(r, i);
      var a = Sr(this).sourceManager = new N_(this);
      a.prepareSource();
      var o = this.getInitialData(r, i);
      Qd(o, this), this.dataTask.context.data = o, Sr(this).dataBeforeProcessed = o, jd(this), this._initSelectedMapFromData(o);
    }, e.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = Ss(this), a = i ? sc(r) : {}, o = this.subType;
      ae.hasClass(o) && (o += "Series"), pe(r, n.getTheme().get(this.subType)), pe(r, this.getDefaultOption()), Rv(r, "label", ["show"]), this.fillDataTextStyle(r.data), i && wa(r, a, i);
    }, e.prototype.mergeOption = function(r, n) {
      r = pe(this.option, r, !0), this.fillDataTextStyle(r.data);
      var i = Ss(this);
      i && wa(this.option, r, i);
      var a = Sr(this).sourceManager;
      a.dirty(), a.prepareSource();
      var o = this.getInitialData(r, n);
      Qd(o, this), this.dataTask.dirty(), this.dataTask.context.data = o, Sr(this).dataBeforeProcessed = o, jd(this), this._initSelectedMapFromData(o);
    }, e.prototype.fillDataTextStyle = function(r) {
      if (r && !Ke(r))
        for (var n = ["show"], i = 0; i < r.length; i++)
          r[i] && r[i].label && Rv(r[i], "label", n);
    }, e.prototype.getInitialData = function(r, n) {
    }, e.prototype.appendData = function(r) {
      var n = this.getRawData();
      n.appendData(r.data);
    }, e.prototype.getData = function(r) {
      var n = Hf(this);
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
      var n = Hf(this);
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
      for (var f = [], h = 1 / 0, v = -1, c = 0, d = o.getDimensionIndex(n), y = o.getStore(), p = 0, g = y.count(); p < g; p++) {
        var m = y.get(d, p), _ = u.dataToCoord(m), S = l - _, b = Math.abs(S);
        b <= a && ((b < h || b === h && S >= 0 && v < 0) && (h = b, v = S, c = 0), S === v && (f[c++] = p));
      }
      return f.length = c, f;
    }, e.prototype.formatTooltip = function(r, n, i) {
      return iD({
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
      var a = this.ecModel, o = uc.prototype.getColorFromPalette.call(this, r, n, i);
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
      if (this[oD])
        return !0;
      var r = this.option.universalTransition;
      return r ? r === !0 ? !0 : r && r.enabled : !1;
    }, e.prototype._innerSelect = function(r, n) {
      var i, a, o = this.option, s = o.selectedMode, u = n.length;
      if (!(!s || !u)) {
        if (s === "series")
          o.selectedMap = "all";
        else if (s === "multiple") {
          F(o.selectedMap) || (o.selectedMap = {});
          for (var l = o.selectedMap, f = 0; f < u; f++) {
            var h = n[f], v = po(r, h);
            l[v] = !0, this._selectedDataIndicesMap[v] = r.getRawIndex(h);
          }
        } else if (s === "single" || s === !0) {
          var c = n[u - 1], v = po(r, c);
          o.selectedMap = (i = {}, i[v] = !0, i), this._selectedDataIndicesMap = (a = {}, a[v] = r.getRawIndex(c), a);
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
Lt($t, IM);
Lt($t, uc);
tm($t, ae);
function jd(t) {
  var e = t.name;
  xh(t) || (t.name = sD(t) || e);
}
function sD(t) {
  var e = t.getRawData(), r = e.mapDimensionsAll("seriesName"), n = [];
  return T(r, function(i) {
    var a = e.getDimensionInfo(i);
    a.displayName && n.push(a.displayName);
  }), n.join(" ");
}
function uD(t) {
  return t.model.getRawData().count();
}
function lD(t) {
  var e = t.model;
  return e.setData(e.getRawData().cloneShallow()), fD;
}
function fD(t, e) {
  e.outputData && t.end > e.outputData.count() && e.model.getRawData().cloneShallow(e.outputData);
}
function Qd(t, e) {
  T(fy(t.CHANGABLE_METHODS, t.DOWNSAMPLE_METHODS), function(r) {
    t.wrapMethod(r, Te(hD, e));
  });
}
function hD(t, e) {
  var r = Hf(t);
  return r && r.setOutputEnd((e || this).count()), e;
}
function Hf(t) {
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
      this.group = new ut(), this.uid = cu("viewComponent");
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
Ah(lt);
js(lt);
function cD() {
  var t = de();
  return function(e) {
    var r = t(e), n = e.pipelineContext, i = !!r.large, a = !!r.progressiveRender, o = r.large = !!(n && n.large), s = r.progressiveRender = !!(n && n.progressiveRender);
    return (i !== o || a !== s) && "reset";
  };
}
var Y_ = de(), vD = cD(), Wt = (
  /** @class */
  (function() {
    function t() {
      this.group = new ut(), this.uid = cu("viewChart"), this.renderTask = ua({
        plan: dD,
        reset: pD
      }), this.renderTask.context = {
        view: this
      };
    }
    return t.prototype.init = function(e, r) {
    }, t.prototype.render = function(e, r, n, i) {
    }, t.prototype.highlight = function(e, r, n, i) {
      var a = e.getData(i && i.dataType);
      a && ep(a, i, "emphasis");
    }, t.prototype.downplay = function(e, r, n, i) {
      var a = e.getData(i && i.dataType);
      a && ep(a, i, "normal");
    }, t.prototype.remove = function(e, r) {
      this.group.removeAll();
    }, t.prototype.dispose = function(e, r) {
    }, t.prototype.updateView = function(e, r, n, i) {
      this.render(e, r, n, i);
    }, t.prototype.updateVisual = function(e, r, n, i) {
      this.render(e, r, n, i);
    }, t.prototype.eachRendered = function(e) {
      e_(this.group, e);
    }, t.markUpdateMethod = function(e, r) {
      Y_(e).updateMethod = r;
    }, t.protoInitialize = (function() {
      var e = t.prototype;
      e.type = "chart";
    })(), t;
  })()
);
function Jd(t, e, r) {
  t && If(t) && (e === "emphasis" ? Cf : xf)(t, r);
}
function ep(t, e, r) {
  var n = Oa(t, e), i = e && e.highlightKey != null ? sC(e.highlightKey) : null;
  n != null ? T($e(n), function(a) {
    Jd(t.getItemGraphicEl(a), r, i);
  }) : t.eachItemGraphicEl(function(a) {
    Jd(a, r, i);
  });
}
Ah(Wt);
js(Wt);
function dD(t) {
  return vD(t.model);
}
function pD(t) {
  var e = t.model, r = t.ecModel, n = t.api, i = t.payload, a = e.pipelineContext.progressiveRender, o = t.view, s = i && Y_(i).updateMethod, u = a ? "incrementalPrepareRender" : s && o[s] ? s : "render";
  return u !== "render" && o[u](e, r, n, i), gD[u];
}
var gD = {
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
}, ws = "\0__throttleOriginMethod", tp = "\0__throttleRate", rp = "\0__throttleType";
function vc(t, e, r) {
  var n, i = 0, a = 0, o = null, s, u, l, f;
  e = e || 0;
  function h() {
    a = (/* @__PURE__ */ new Date()).getTime(), o = null, t.apply(u, l || []);
  }
  var v = function() {
    for (var c = [], d = 0; d < arguments.length; d++)
      c[d] = arguments[d];
    n = (/* @__PURE__ */ new Date()).getTime(), u = this, l = c;
    var y = f || e, p = f || r;
    f = null, s = n - (p ? i : a) - y, clearTimeout(o), p ? o = setTimeout(h, y) : s >= 0 ? h() : o = setTimeout(h, -s), i = n;
  };
  return v.clear = function() {
    o && (clearTimeout(o), o = null);
  }, v.debounceNextCall = function(c) {
    f = c;
  }, v;
}
function W_(t, e, r, n) {
  var i = t[e];
  if (i) {
    var a = i[ws] || i, o = i[rp], s = i[tp];
    if (s !== r || o !== n) {
      if (r == null || !n)
        return t[e] = a;
      i = t[e] = vc(a, r, n === "debounce"), i[ws] = a, i[rp] = n, i[tp] = r;
    }
    return i;
  }
}
function Vf(t, e) {
  var r = t[e];
  r && r[ws] && (r.clear && r.clear(), t[e] = r[ws]);
}
var np = de(), ip = {
  itemStyle: _a(o_, !0),
  lineStyle: _a(a_, !0)
}, yD = {
  lineStyle: "stroke",
  itemStyle: "fill"
};
function X_(t, e) {
  var r = t.visualStyleMapper || ip[e];
  return r || (console.warn("Unknown style type '" + e + "'."), ip.itemStyle);
}
function q_(t, e) {
  var r = t.visualDrawType || yD[e];
  return r || (console.warn("Unknown style type '" + e + "'."), "fill");
}
var mD = {
  createOnAllSeries: !0,
  performRawSeries: !0,
  reset: function(t, e) {
    var r = t.getData(), n = t.visualStyleAccessPath || "itemStyle", i = t.getModel(n), a = X_(t, n), o = a(i), s = i.getShallow("decal");
    s && (r.setVisual("decal", s), s.dirty = !0);
    var u = q_(t, n), l = o[u], f = X(l) ? l : null, h = o.fill === "auto" || o.stroke === "auto";
    if (!o[u] || f || h) {
      var v = t.getColorFromPalette(
        // TODO series count changed.
        t.name,
        null,
        e.getSeriesCount()
      );
      o[u] || (o[u] = v, r.setVisual("colorFromPalette", !0)), o.fill = o.fill === "auto" || X(o.fill) ? v : o.fill, o.stroke = o.stroke === "auto" || X(o.stroke) ? v : o.stroke;
    }
    if (r.setVisual("style", o), r.setVisual("drawType", u), !e.isSeriesFiltered(t) && f)
      return r.setVisual("colorFromPalette", !1), {
        dataEach: function(c, d) {
          var y = t.getDataParams(d), p = R({}, o);
          p[u] = f(y), c.setItemVisual(d, "style", p);
        }
      };
  }
}, Bi = new ce(), _D = {
  createOnAllSeries: !0,
  reset: function(t, e) {
    if (!t.ignoreStyleOnData) {
      var r = t.getData(), n = t.visualStyleAccessPath || "itemStyle", i = X_(t, n), a = r.getVisual("drawType");
      return {
        dataEach: r.hasItemOption ? function(o, s) {
          var u = o.getRawDataItem(s);
          if (u && u[n]) {
            Bi.option = u[n];
            var l = i(Bi), f = o.ensureUniqueItemVisual(s, "style");
            R(f, l), Bi.option.decal && (o.setItemVisual(s, "decal", Bi.option.decal), Bi.option.decal.dirty = !0), a in l && o.setItemVisual(s, "colorFromPalette", !1);
          }
        } : null
      };
    }
  }
}, SD = {
  performRawSeries: !0,
  overallReset: function(t) {
    var e = U();
    t.eachSeries(function(r) {
      if (!r.isColorBySeries()) {
        var n = r.type + "-" + r.getColorBy();
        np(r).scope = e.get(n) || e.set(n, {});
      }
    }), t.eachSeries(function(r) {
      if (!r.isColorBySeries()) {
        var n = r.getRawData(), i = {}, a = r.getData(), o = np(r).scope, s = r.visualStyleAccessPath || "itemStyle", u = q_(r, s);
        a.each(function(l) {
          var f = a.getRawIndex(l);
          i[f] = l;
        }), n.each(function(l) {
          var f = i[l], h = a.getItemVisual(f, "colorFromPalette");
          if (h) {
            var v = a.ensureUniqueItemVisual(f, "style"), c = n.getName(l) || l + "", d = n.count();
            v[u] = r.getColorFromPalette(c, o, d);
          }
        });
      }
    });
  }
}, go = Math.PI;
function bD(t, e) {
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
  var r = new ut(), n = new Re({
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
  }), a = new Re({
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
var $_ = (
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
      var n = this._pipelineMap.get(e.uid), i = e.__preparePipelineContext ? e.__preparePipelineContext(r, n) : Hw(e, r, n);
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
      T(this._allHandlers, function(i) {
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
      T(e, function(u, l) {
        if (!(i.visualType && i.visualType !== u.visualType)) {
          var f = o._stageTaskMap.get(u.uid), h = f.seriesTaskMap, v = f.overallTask;
          if (v) {
            var c, d = v.agentStubMap;
            d.each(function(p) {
              s(i, p) && (p.dirty(), c = !0);
            }), c && v.dirty(), o.updatePayload(v, n);
            var y = o.getPerformArgs(v, i.block);
            d.each(function(p) {
              p.perform(y);
            }), v.perform(y) && (a = !0);
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
        var v = h.uid, c = s.set(v, o && o.get(v) || ua({
          plan: MD,
          reset: DD,
          count: AD
        }));
        c.context = {
          model: h,
          ecModel: n,
          api: i,
          // PENDING: `useClearVisual` not used?
          useClearVisual: e.isVisual && !e.isLayout,
          plan: e.plan,
          reset: e.reset,
          scheduler: a
        }, a._pipe(h, c);
      }
    }, t.prototype._createOverallStageTask = function(e, r, n, i) {
      var a = this, o = r.overallTask = r.overallTask || ua({
        reset: wD
      });
      o.context = {
        ecModel: n,
        api: i,
        overallReset: e.overallReset,
        scheduler: a
      };
      var s = o.agentStubMap, u = o.agentStubMap = U(), l = e.seriesType, f = e.getTargetSeries, h = e.dirtyOnOverallProgress, v = !1, c = "";
      qt(!e.createOnAllSeries, c), l ? n.eachRawSeriesByType(l, d) : f ? f(n, i).each(d) : T(n.getSeries(), d);
      function d(y) {
        var p = y.uid, g = u.set(p, s && s.get(p) || // When the result of `getTargetSeries` changed, the overallTask
        // should be set as dirty and re-performed.
        (v = !0, ua({
          reset: TD,
          onDirty: xD
        })));
        g.context = {
          model: y,
          dirtyOnOverallProgress: h
          // FIXME:TS never used, so comment it
          // modifyOutputEnd: modifyOutputEnd
        }, g.agent = o, g.__block = h, a._pipe(y, g);
      }
      v && o.dirty();
    }, t.prototype._pipe = function(e, r) {
      var n = e.uid, i = this._pipelineMap.get(n);
      !i.head && (i.head = r), i.tail && i.tail.pipe(r), i.tail = r, r.__idxInPipeline = i.count++, r.__pipeline = i;
    }, t.wrapStageHandler = function(e, r) {
      return X(e) && (e = {
        overallReset: e,
        seriesType: PD(e)
      }), e.uid = cu("stageHandler"), r && (e.visualType = r), e;
    }, t;
  })()
);
function wD(t) {
  t.overallReset(t.ecModel, t.api, t.payload);
}
function TD(t) {
  return t.dirtyOnOverallProgress && CD;
}
function CD() {
  this.agent.dirty(), this.getDownstream().dirty();
}
function xD() {
  this.agent && this.agent.dirty();
}
function MD(t) {
  return t.plan ? t.plan(t.model, t.ecModel, t.api, t.payload) : null;
}
function DD(t) {
  t.useClearVisual && t.data.clearAllVisual();
  var e = t.resetDefines = $e(t.reset(t.model, t.ecModel, t.api, t.payload));
  return e.length > 1 ? H(e, function(r, n) {
    return Z_(n);
  }) : ID;
}
var ID = Z_(0);
function Z_(t) {
  return function(e, r) {
    var n = r.data, i = r.resetDefines[t];
    if (i && i.dataEach)
      for (var a = e.start; a < e.end; a++)
        i.dataEach(n, a);
    else i && i.progress && i.progress(e, n);
  };
}
function AD(t) {
  return t.data.count();
}
function PD(t) {
  Ts = null;
  try {
    t(xa, K_);
  } catch {
  }
  return Ts;
}
var xa = {}, K_ = {}, Ts;
j_(xa, lc);
j_(K_, pm);
xa.eachSeriesByType = xa.eachRawSeriesByType = function(t) {
  Ts = t;
};
xa.eachComponent = function(t) {
  t.mainType === "series" && t.subType && (Ts = t.subType);
};
function j_(t, e) {
  for (var r in e.prototype)
    t[r] = Ee;
}
var E = j.darkColor, ap = E.background, Ni = function() {
  return {
    axisLine: {
      lineStyle: {
        color: E.axisLine
      }
    },
    splitLine: {
      lineStyle: {
        color: E.axisSplitLine
      }
    },
    splitArea: {
      areaStyle: {
        color: [E.backgroundTint, E.backgroundTransparent]
      }
    },
    minorSplitLine: {
      lineStyle: {
        color: E.axisMinorSplitLine
      }
    },
    axisLabel: {
      color: E.axisLabel
    },
    axisName: {}
  };
}, op = {
  label: {
    color: E.secondary
  },
  itemStyle: {
    borderColor: E.borderTint
  },
  dividerLineStyle: {
    color: E.border
  }
}, Q_ = {
  darkMode: !0,
  color: E.theme,
  backgroundColor: ap,
  axisPointer: {
    lineStyle: {
      color: E.border
    },
    crossStyle: {
      color: E.borderShade
    },
    label: {
      color: E.tertiary
    }
  },
  legend: {
    textStyle: {
      color: E.secondary
    },
    pageTextStyle: {
      color: E.tertiary
    }
  },
  textStyle: {
    color: E.secondary
  },
  title: {
    textStyle: {
      color: E.primary
    },
    subtextStyle: {
      color: E.quaternary
    }
  },
  toolbox: {
    iconStyle: {
      borderColor: E.accent50
    },
    feature: {
      dataView: {
        backgroundColor: ap,
        textColor: E.primary,
        textareaColor: E.background,
        textareaBorderColor: E.border,
        buttonColor: E.accent50,
        buttonTextColor: E.neutral00
      }
    }
  },
  tooltip: {
    backgroundColor: E.neutral20,
    defaultBorderColor: E.border,
    textStyle: {
      color: E.tertiary
    }
  },
  dataZoom: {
    borderColor: E.accent10,
    textStyle: {
      color: E.tertiary
    },
    brushStyle: {
      color: E.backgroundTint
    },
    handleStyle: {
      color: E.neutral00,
      borderColor: E.accent20
    },
    moveHandleStyle: {
      color: E.accent40
    },
    emphasis: {
      handleStyle: {
        borderColor: E.accent50
      }
    },
    dataBackground: {
      lineStyle: {
        color: E.accent30
      },
      areaStyle: {
        color: E.accent20
      }
    },
    selectedDataBackground: {
      lineStyle: {
        color: E.accent50
      },
      areaStyle: {
        color: E.accent30
      }
    }
  },
  visualMap: {
    textStyle: {
      color: E.secondary
    },
    handleStyle: {
      borderColor: E.neutral30
    }
  },
  timeline: {
    lineStyle: {
      color: E.accent10
    },
    label: {
      color: E.tertiary
    },
    controlStyle: {
      color: E.accent30,
      borderColor: E.accent30
    }
  },
  calendar: {
    itemStyle: {
      color: E.neutral00,
      borderColor: E.neutral20
    },
    dayLabel: {
      color: E.tertiary
    },
    monthLabel: {
      color: E.secondary
    },
    yearLabel: {
      color: E.secondary
    }
  },
  matrix: {
    x: op,
    y: op,
    backgroundColor: {
      borderColor: E.axisLine
    },
    body: {
      itemStyle: {
        borderColor: E.borderTint
      }
    }
  },
  timeAxis: Ni(),
  logAxis: Ni(),
  valueAxis: Ni(),
  categoryAxis: Ni(),
  line: {
    symbol: "circle"
  },
  graph: {
    color: E.theme
  },
  gauge: {
    title: {
      color: E.secondary
    },
    axisLine: {
      lineStyle: {
        color: [[1, E.neutral05]]
      }
    },
    axisLabel: {
      color: E.axisLabel
    },
    detail: {
      color: E.primary
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
      borderColor: E.background
    }
  },
  radar: (function() {
    var t = Ni();
    return t.axisName = {
      color: E.axisLabel
    }, t.axisLine.lineStyle.color = E.neutral20, t;
  })(),
  treemap: {
    breadcrumb: {
      itemStyle: {
        color: E.neutral20,
        textStyle: {
          color: E.secondary
        }
      },
      emphasis: {
        itemStyle: {
          color: E.neutral30
        }
      }
    }
  },
  sunburst: {
    itemStyle: {
      borderColor: E.background
    }
  },
  map: {
    itemStyle: {
      borderColor: E.border,
      areaColor: E.neutral10
    },
    label: {
      color: E.tertiary
    },
    emphasis: {
      label: {
        color: E.primary
      },
      itemStyle: {
        areaColor: E.highlight
      }
    },
    select: {
      label: {
        color: E.primary
      },
      itemStyle: {
        areaColor: E.highlight
      }
    }
  },
  geo: {
    itemStyle: {
      borderColor: E.border,
      areaColor: E.neutral10
    },
    emphasis: {
      label: {
        color: E.primary
      },
      itemStyle: {
        areaColor: E.highlight
      }
    },
    select: {
      label: {
        color: E.primary
      },
      itemStyle: {
        color: E.highlight
      }
    }
  }
};
Q_.categoryAxis.splitLine.show = !1;
var LD = (
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
        T(e, function(u, l) {
          for (var f = !1, h = 0; h < o.length; h++) {
            var v = o[h], c = l.lastIndexOf(v);
            if (c > 0 && c === l.length - v.length) {
              var d = l.slice(0, c);
              d !== "data" && (r.mainType = d, r[v.toLowerCase()] = u, f = !0);
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
      function f(h, v, c, d) {
        return h[c] == null || v[d || c] === h[c];
      }
    }, t.prototype.afterTrigger = function() {
      this.eventInfo = null;
    }, t;
  })()
), Gf = ["symbol", "symbolSize", "symbolRotate", "symbolOffset"], sp = Gf.concat(["symbolKeepAspect"]), ED = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(t, e) {
    var r = t.getData();
    if (t.legendIcon && r.setVisual("legendIcon", t.legendIcon), !t.hasSymbolVisual)
      return;
    for (var n = {}, i = {}, a = !1, o = 0; o < Gf.length; o++) {
      var s = Gf[o], u = t.get(s);
      X(u) ? (a = !0, i[s] = u) : n[s] = u;
    }
    if (n.symbol = n.symbol || t.defaultSymbol, r.setVisual(R({
      legendIcon: t.legendIcon || n.symbol,
      symbolKeepAspect: t.get("symbolKeepAspect")
    }, n)), e.isSeriesFiltered(t))
      return;
    var l = me(i);
    function f(h, v) {
      for (var c = t.getRawValue(v), d = t.getDataParams(v), y = 0; y < l.length; y++) {
        var p = l[y];
        h.setItemVisual(v, p, i[p](c, d));
      }
    }
    return {
      dataEach: a ? f : null
    };
  }
}, RD = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(t, e) {
    if (!t.hasSymbolVisual || e.isSeriesFiltered(t))
      return;
    var r = t.getData();
    function n(i, a) {
      for (var o = i.getItemModel(a), s = 0; s < sp.length; s++) {
        var u = sp[s], l = o.getShallow(u, !0);
        l != null && i.setItemVisual(a, u, l);
      }
    }
    return {
      dataEach: r.hasItemOption ? n : null
    };
  }
};
function OD(t, e, r) {
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
function kD(t, e) {
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
        var h = o.getData(), v = Oa(h, i.fromActionPayload);
        r.trigger(a, {
          type: a,
          seriesId: o.id,
          name: B(v) ? h.getName(v[0]) : h.getName(v),
          selected: N(u) ? u : R({}, u)
        });
      }
  });
}
function BD(t, e, r) {
  t.on("selectchanged", function(n) {
    var i = r.getModel();
    n.isFromClick ? (Gn("map", "selectchanged", e, i, n), Gn("pie", "selectchanged", e, i, n)) : n.fromAction === "select" ? (Gn("map", "selected", e, i, n), Gn("pie", "selected", e, i, n)) : n.fromAction === "unselect" && (Gn("map", "unselected", e, i, n), Gn("pie", "unselected", e, i, n));
  });
}
function Ji(t, e, r) {
  for (var n; t && !(e(t) && (n = t, r)); )
    t = t.__hostTarget || t.parent;
  return n;
}
var vt = new Kt(), J_ = {};
function ND(t, e) {
  J_[t] = e;
}
function e0(t) {
  return J_[t];
}
var pu = de();
function FD(t) {
  pu(t).prepare = {};
}
function zD(t) {
  pu(t).fullUpdate = {};
}
function HD(t) {
  return pu(t).prepare;
}
function _i(t) {
  return pu(t).fullUpdate;
}
var VD = Math.round(Math.random() * 9), GD = typeof Object.defineProperty == "function", UD = (function() {
  function t() {
    this._id = "__ec_inner_" + VD++;
  }
  return t.prototype.get = function(e) {
    return this._guard(e)[this._id];
  }, t.prototype.set = function(e, r) {
    var n = this._guard(e);
    return GD ? Object.defineProperty(n, this._id, {
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
})(), YD = ye.extend({
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
}), WD = ye.extend({
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
}), XD = ye.extend({
  type: "pin",
  shape: {
    // x, y on the cusp
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  buildPath: function(t, e) {
    var r = e.x, n = e.y, i = e.width / 5 * 3, a = Math.max(i, e.height), o = i / 2, s = o * o / (a - o), u = n - a + o + s, l = Math.asin(s / o), f = Math.cos(l) * o, h = Math.sin(l), v = Math.cos(l), c = o * 0.6, d = o * 0.7;
    t.moveTo(r - f, u + s), t.arc(r, u, o, Math.PI - l, Math.PI * 2 + l), t.bezierCurveTo(r + f - h * c, u + s + v * c, r, n - d, r, n), t.bezierCurveTo(r, n - d, r - f + h * c, u + s + v * c, r - f, u + s), t.closePath();
  }
}), qD = ye.extend({
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
}), $D = {
  line: Pn,
  rect: Re,
  roundRect: Re,
  square: Re,
  circle: Na,
  diamond: WD,
  pin: XD,
  arrow: qD,
  triangle: YD
}, ZD = {
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
}, Uf = {};
T($D, function(t, e) {
  Uf[e] = new t();
});
var KD = ye.extend({
  type: "symbol",
  shape: {
    symbolType: "",
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  calculateTextPosition: function(t, e, r) {
    var n = Hy(t, e, r), i = this.shape;
    return i && i.symbolType === "pin" && e.position === "inside" && (n.y = r.y + r.height * 0.4), n;
  },
  buildPath: function(t, e, r) {
    var n = e.symbolType;
    if (n !== "none") {
      var i = Uf[n];
      i || (n = "rect", i = Uf[n]), ZD[n](e.x, e.y, e.width, e.height, i.shape), i.buildPath(t, i.shape, r);
    }
  }
});
function jD(t, e) {
  if (this.type !== "image") {
    var r = this.style;
    this.__isEmptyBrush ? (r.stroke = t, r.fill = e || j.color.neutral00, r.lineWidth = 2) : this.shape.symbolType === "line" ? r.stroke = t : r.fill = t, this.markRedraw();
  }
}
function gu(t, e, r, n, i, a, o) {
  var s = t.indexOf("empty") === 0;
  s && (t = t.substr(5, 1).toLowerCase() + t.substr(6));
  var u;
  return t.indexOf("image://") === 0 ? u = zh(t.slice(8), new Y(e, r, n, i), o ? "center" : "cover") : t.indexOf("path://") === 0 ? u = uu(t.slice(7), {}, new Y(e, r, n, i), o ? "center" : "cover") : u = new KD({
    shape: {
      symbolType: t,
      x: e,
      y: r,
      width: n,
      height: i
    }
  }), u.__isEmptyBrush = s, u.setColor = jD, a && u.setColor(a), u;
}
function QD(t, e) {
  if (t != null)
    return B(t) || (t = [t, t]), [Dt(t[0], e[0]) || 0, Dt(G(t[1], t[0]), e[1]) || 0];
}
function dn(t) {
  return isFinite(t);
}
function JD(t, e, r) {
  var n = e.x == null ? 0 : e.x, i = e.x2 == null ? 1 : e.x2, a = e.y == null ? 0 : e.y, o = e.y2 == null ? 0 : e.y2;
  e.global || (n = n * r.width + r.x, i = i * r.width + r.x, a = a * r.height + r.y, o = o * r.height + r.y), n = dn(n) ? n : 0, i = dn(i) ? i : 1, a = dn(a) ? a : 0, o = dn(o) ? o : 0;
  var s = t.createLinearGradient(n, a, i, o);
  return s;
}
function eI(t, e, r) {
  var n = r.width, i = r.height, a = Math.min(n, i), o = e.x == null ? 0.5 : e.x, s = e.y == null ? 0.5 : e.y, u = e.r == null ? 0.5 : e.r;
  e.global || (o = o * n + r.x, s = s * i + r.y, u = u * a), o = dn(o) ? o : 0.5, s = dn(s) ? s : 0.5, u = u >= 0 && dn(u) ? u : 0.5;
  var l = t.createRadialGradient(o, s, 0, o, s, u);
  return l;
}
function Yf(t, e, r) {
  for (var n = e.type === "radial" ? eI(t, e, r) : JD(t, e, r), i = e.colorStops, a = 0; a < i.length; a++)
    n.addColorStop(i[a].offset, i[a].color);
  return n;
}
function tI(t, e) {
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
function rI(t, e) {
  return !t || t === "solid" || !(e > 0) ? null : t === "dashed" ? [4 * e, 2 * e] : t === "dotted" ? [e] : ve(t) ? [t] : B(t) ? t : null;
}
function t0(t) {
  var e = t.style, r = e.lineDash && e.lineWidth > 0 && rI(e.lineDash, e.lineWidth), n = e.lineDashOffset;
  if (r) {
    var i = e.strokeNoScale && t.getLineScale ? t.getLineScale() : 1;
    i && i !== 1 && (r = H(r, function(a) {
      return a / i;
    }), n /= i);
  }
  return [r, n];
}
var nI = new vi(!0);
function Cs(t) {
  var e = t.stroke;
  return !(e == null || e === "none" || !(t.lineWidth > 0));
}
function up(t) {
  return typeof t == "string" && t !== "none";
}
function xs(t) {
  var e = t.fill;
  return e != null && e !== "none";
}
function lp(t, e) {
  if (e.fillOpacity != null && e.fillOpacity !== 1) {
    var r = t.globalAlpha;
    t.globalAlpha = e.fillOpacity * e.opacity, t.fill(), t.globalAlpha = r;
  } else
    t.fill();
}
function fp(t, e) {
  if (e.strokeOpacity != null && e.strokeOpacity !== 1) {
    var r = t.globalAlpha;
    t.globalAlpha = e.strokeOpacity * e.opacity, t.stroke(), t.globalAlpha = r;
  } else
    t.stroke();
}
function Wf(t, e, r) {
  var n = rm(e.image, e.__image, r);
  if (Qs(n)) {
    var i = t.createPattern(n, e.repeat || "repeat");
    if (typeof DOMMatrix == "function" && i && i.setTransform) {
      var a = new DOMMatrix();
      a.translateSelf(e.x || 0, e.y || 0), a.rotateSelf(0, 0, (e.rotation || 0) * hy), a.scaleSelf(e.scaleX || 1, e.scaleY || 1), i.setTransform(a);
    }
    return i;
  }
}
function iI(t, e, r, n, i) {
  var a, o = Cs(r), s = xs(r), u = r.strokePercent, l = u < 1, f = !e.path;
  (!e.silent || l) && f && e.createPathProxy();
  var h = e.path || nI, v = e.__dirty;
  if (!n) {
    var c = r.fill, d = r.stroke, y = s && !!c.colorStops, p = o && !!d.colorStops, g = s && !!c.image, m = o && !!d.image, _ = void 0, S = void 0, b = void 0, w = void 0, C = void 0;
    (y || p) && (C = e.getBoundingRect()), y && (_ = v ? Yf(t, c, C) : e.__canvasFillGradient, e.__canvasFillGradient = _), p && (S = v ? Yf(t, d, C) : e.__canvasStrokeGradient, e.__canvasStrokeGradient = S), g && (b = v || !e.__canvasFillPattern ? Wf(t, c, e) : e.__canvasFillPattern, e.__canvasFillPattern = b), m && (w = v || !e.__canvasStrokePattern ? Wf(t, d, e) : e.__canvasStrokePattern, e.__canvasStrokePattern = w), y ? t.fillStyle = _ : g && (b ? t.fillStyle = b : s = !1), p ? t.strokeStyle = S : m && (w ? t.strokeStyle = w : o = !1);
  }
  var M = e.getGlobalScale();
  h.setScale(M[0], M[1], e.segmentIgnoreThreshold);
  var x, D;
  t.setLineDash && r.lineDash && (a = t0(e), x = a[0], D = a[1]);
  var I = !0;
  (f || v & $n) && (h.setDPR(t.dpr), l ? h.setContext(null) : (h.setContext(t), I = !1), h.reset(), e.buildPath(h, e.shape, n), h.toStatic(), e.pathUpdated()), I && h.rebuildPath(t, l ? u : 1), x && (t.setLineDash(x), t.lineDashOffset = D), n ? (i.batchFill = s, i.batchStroke = o) : r.strokeFirst ? (o && fp(t, r), s && lp(t, r)) : (s && lp(t, r), o && fp(t, r)), x && t.setLineDash([]);
}
function aI(t, e, r) {
  var n = e.__image = rm(r.image, e.__image, e, e.onload);
  if (!(!n || !Qs(n))) {
    var i = r.x || 0, a = r.y || 0, o = e.getWidth(), s = e.getHeight(), u = n.width / n.height;
    if (o == null && s != null ? o = s * u : s == null && o != null ? s = o / u : o == null && s == null && (o = n.width, s = n.height), r.sWidth && r.sHeight) {
      var l = r.sx || 0, f = r.sy || 0;
      t.drawImage(n, l, f, r.sWidth, r.sHeight, i, a, o, s);
    } else if (r.sx && r.sy) {
      var l = r.sx, f = r.sy, h = o - l, v = s - f;
      t.drawImage(n, l, f, h, v, i, a, o, s);
    } else
      t.drawImage(n, i, a, o, s);
  }
}
function oI(t, e, r) {
  var n, i = r.text;
  if (i != null && (i += ""), i) {
    t.font = r.font || Rr, t.textAlign = r.textAlign, t.textBaseline = r.textBaseline;
    var a = void 0, o = void 0;
    t.setLineDash && r.lineDash && (n = t0(e), a = n[0], o = n[1]), a && (t.setLineDash(a), t.lineDashOffset = o), r.strokeFirst ? (Cs(r) && t.strokeText(i, r.x, r.y), xs(r) && t.fillText(i, r.x, r.y)) : (xs(r) && t.fillText(i, r.x, r.y), Cs(r) && t.strokeText(i, r.x, r.y)), a && t.setLineDash([]);
  }
}
var hp = ["shadowBlur", "shadowOffsetX", "shadowOffsetY"], cp = [
  ["lineCap", "butt"],
  ["lineJoin", "miter"],
  ["miterLimit", 10]
];
function r0(t, e, r, n, i) {
  var a = !1;
  if (!n && (r = r || {}, e === r))
    return !1;
  if (n || e.opacity !== r.opacity) {
    We(t, i), a = !0;
    var o = Math.max(Math.min(e.opacity, 1), 0);
    t.globalAlpha = isNaN(o) ? Sn.opacity : o;
  }
  (n || e.blend !== r.blend) && (a || (We(t, i), a = !0), t.globalCompositeOperation = e.blend || Sn.blend);
  for (var s = 0; s < hp.length; s++) {
    var u = hp[s];
    (n || e[u] !== r[u]) && (a || (We(t, i), a = !0), t[u] = t.dpr * (e[u] || 0));
  }
  return (n || e.shadowColor !== r.shadowColor) && (a || (We(t, i), a = !0), t.shadowColor = e.shadowColor || Sn.shadowColor), a;
}
function vp(t, e, r, n, i) {
  var a = e.style, o = n ? null : r && r.style || {};
  if (a === o)
    return !1;
  var s = r0(t, a, o, n, i);
  if ((n || a.fill !== o.fill) && (s || (We(t, i), s = !0), up(a.fill) && (t.fillStyle = a.fill)), (n || a.stroke !== o.stroke) && (s || (We(t, i), s = !0), up(a.stroke) && (t.strokeStyle = a.stroke)), (n || a.opacity !== o.opacity) && (s || (We(t, i), s = !0), t.globalAlpha = a.opacity == null ? 1 : a.opacity), e.hasStroke()) {
    var u = a.lineWidth, l = u / (a.strokeNoScale && e.getLineScale ? e.getLineScale() : 1);
    t.lineWidth !== l && (s || (We(t, i), s = !0), t.lineWidth = l);
  }
  for (var f = 0; f < cp.length; f++) {
    var h = cp[f], v = h[0];
    (n || a[v] !== o[v]) && (s || (We(t, i), s = !0), t[v] = a[v] || h[1]);
  }
  return s;
}
function sI(t, e, r, n, i) {
  return r0(t, e.style, r && r.style, n, i);
}
function n0(t, e) {
  var r = e.transform, n = t.dpr || 1;
  r ? t.setTransform(n * r[0], n * r[1], n * r[2], n * r[3], n * r[4], n * r[5]) : t.setTransform(n, 0, 0, n, 0, 0);
}
function uI(t, e, r) {
  for (var n = !1, i = 0; i < t.length; i++) {
    var a = t[i];
    n = n || a.isZeroArea(), n0(e, a), e.beginPath(), a.buildPath(e, a.shape), e.clip();
  }
  r.allClipped = n;
}
function lI(t, e) {
  return t && e ? t[0] !== e[0] || t[1] !== e[1] || t[2] !== e[2] || t[3] !== e[3] || t[4] !== e[4] || t[5] !== e[5] : !(!t && !e);
}
var dp = 1, pp = 2, gp = 3, yp = 4;
function fI(t) {
  var e = xs(t), r = Cs(t);
  return !(t.lineDash || !(+e ^ +r) || e && typeof t.fill != "string" || r && typeof t.stroke != "string" || t.strokePercent < 1 || t.strokeOpacity < 1 || t.fillOpacity < 1);
}
function We(t, e) {
  e.batchFill && (e.batchFill = !1, t.fill()), e.batchStroke && (e.batchStroke = !1, t.stroke());
}
function dc(t, e) {
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
  if ((!a || tI(i, a)) && (a && (We(t, r), t.restore(), u = s = !0, r.prevElClipPaths = null, r.allClipped = !1, r.prevEl = null), i && i.length && (We(t, r), t.save(), uI(i, t, r), s = !0, r.prevElClipPaths = i)), r.allClipped) {
    e.__dirty &= ~Je, e.__isRendered = !1;
    return;
  }
  e.beforeBrush && e.beforeBrush(r.beforeBrushParam), e.innerBeforeBrush();
  var l = r.prevEl;
  l || (u = s = !0);
  var f = e instanceof ye && e.autoBatch && fI(o);
  s || lI(n, l.transform) ? (We(t, r), n0(t, e)) : f || We(t, r), e instanceof ye ? (r.lastDrawType !== dp && (u = !0, r.lastDrawType = dp), vp(t, e, l, u, r), (!f || !r.batchFill && !r.batchStroke) && t.beginPath(), iI(t, e, o, f, r)) : e instanceof cs ? (r.lastDrawType !== gp && (u = !0, r.lastDrawType = gp), vp(t, e, l, u, r), oI(t, e, o)) : e instanceof Nr ? (r.lastDrawType !== pp && (u = !0, r.lastDrawType = pp), sI(t, e, l, u, r), aI(t, e, o)) : e.getTemporalDisplayables && (r.lastDrawType !== yp && (u = !0, r.lastDrawType = yp), hI(t, e, r)), e.innerAfterBrush(), e.afterBrush && (f && We(t, r), e.afterBrush()), r.prevEl = e, e.__dirty = 0, e.__isRendered = !0;
}
function ai(t, e) {
  We(t, e), e.prevElClipPaths && t.restore();
}
function hI(t, e, r) {
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
var Tl = new UD(), mp = new li(100), _p = ["symbol", "symbolSize", "symbolKeepAspect", "color", "backgroundColor", "dashArrayX", "dashArrayY", "maxTileWidth", "maxTileHeight"];
function Xf(t, e) {
  if (t === "none")
    return null;
  var r = e.getDevicePixelRatio(), n = e.getZr(), i = n.painter.type === "svg";
  t.dirty && Tl.delete(t);
  var a = Tl.get(t);
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
  return u(s), s.rotation = o.rotation, s.scaleX = s.scaleY = i ? 1 : 1 / r, Tl.set(t, s), t.dirty = !1, s;
  function u(l) {
    for (var f = [r], h = !0, v = 0; v < _p.length; ++v) {
      var c = o[_p[v]];
      if (c != null && !B(c) && !N(c) && !ve(c) && typeof c != "boolean") {
        h = !1;
        break;
      }
      f.push(c);
    }
    var d;
    if (h) {
      d = f.join(",") + (i ? "-svg" : "");
      var y = mp.get(d);
      y && (i ? l.svgElement = y : l.image = y);
    }
    var p = a0(o.dashArrayX), g = cI(o.dashArrayY), m = i0(o.symbol), _ = vI(p), S = o0(g), b = !i && Le.createCanvas(), w = i && {
      tag: "g",
      attrs: {},
      key: "dcl",
      children: []
    }, C = x(), M;
    b && (b.width = C.width * r, b.height = C.height * r, M = b.getContext("2d")), D(), h && mp.put(d, b || w), l.image = b, l.svgElement = w, l.svgWidth = C.width, l.svgHeight = C.height;
    function x() {
      for (var I = 1, L = 0, A = _.length; L < A; ++L)
        I = Lv(I, _[L]);
      for (var P = 1, L = 0, A = m.length; L < A; ++L)
        P = Lv(P, m[L].length);
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
        var He = i ? 1 : r, Fc = gu(zr, rt * He, Ae * He, $ * He, te * He, o.color, o.symbolKeepAspect);
        if (i) {
          var zc = n.painter.renderOneToVNode(Fc);
          zc && w.children.push(zc);
        } else
          dc(M, Fc);
      }
    }
  }
}
function i0(t) {
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
    return i0([t]);
  for (var n = [], r = 0; r < t.length; ++r)
    N(t[r]) ? n.push([t[r]]) : n.push(t[r]);
  return n;
}
function a0(t) {
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
    return a0([t]);
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
function cI(t) {
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
function vI(t) {
  return H(t, function(e) {
    return o0(e);
  });
}
function o0(t) {
  for (var e = 0, r = 0; r < t.length; ++r)
    e += t[r];
  return t.length % 2 === 1 ? e * 2 : e;
}
var dI = Ih(pI);
function pI(t, e) {
  t.eachRawSeries(function(r) {
    if (!t.isSeriesFiltered(r)) {
      var n = r.getData();
      n.hasItemVisual() && n.each(function(o) {
        var s = n.getItemVisual(o, "decal");
        if (s) {
          var u = n.ensureUniqueItemVisual(o, "style");
          u.decal = Xf(s, e);
        }
      });
      var i = n.getVisual("decal");
      if (i) {
        var a = n.getVisual("style");
        a.decal = Xf(i, e);
      }
    }
  });
}
var gI = "6.1.0", yI = {
  zrender: "6.1.0"
}, mI = 1, _I = 800, SI = 900, bI = 920, wI = 1e3, TI = 2e3, Sp = 5e3, s0 = 1e3, CI = 1100, pc = 2e3, u0 = 3e3, xI = 4e3, yu = 4500, MI = 4600, DI = 5e3, II = 6e3, l0 = 7e3, f0 = {
  PROCESSOR: {
    SERIES_FILTER: _I,
    AXIS_STATISTICS: bI,
    FILTER: wI,
    STATISTIC: Sp,
    STATISTICS: Sp
  },
  VISUAL: {
    LAYOUT: s0,
    PROGRESSIVE_LAYOUT: CI,
    GLOBAL: pc,
    CHART: u0,
    POST_CHART_LAYOUT: MI,
    COMPONENT: xI,
    BRUSH: DI,
    CHART_ITEM: yu,
    ARIA: II,
    DECAL: l0
  }
}, Ce = "__flagInMainProcess", _o = "__mainProcessVersion", Me = "__pendingUpdate", Cl = "__needsUpdateStatus", bp = /^[a-zA-Z0-9_]+$/, xl = "__connectUpdateStatus", wp = 0, AI = 1, PI = 2;
function h0(t) {
  return function() {
    for (var e = [], r = 0; r < arguments.length; r++)
      e[r] = arguments[r];
    if (this.isDisposed()) {
      this.id;
      return;
    }
    return v0(this, t, e);
  };
}
function c0(t) {
  return function() {
    for (var e = [], r = 0; r < arguments.length; r++)
      e[r] = arguments[r];
    return v0(this, t, e);
  };
}
function v0(t, e, r) {
  return r[0] = r[0] && r[0].toLowerCase(), Kt.prototype[e].apply(t, r);
}
var d0 = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      return t !== null && t.apply(this, arguments) || this;
    }
    return e;
  })(Kt)
), p0 = d0.prototype;
p0.on = c0("on");
p0.off = c0("off");
var un, Ml, So, ir, bo, Dl, Il, Un, Yn, Tp, Cp, Al, xp, wo, Mp, g0, ht, Dp, Wn, Ms = (
  /** @class */
  (function(t) {
    z(e, t);
    function e(r, n, i) {
      var a = t.call(this, new LD()) || this;
      a._chartsViews = [], a._chartsMap = {}, a._componentsViews = [], a._componentsMap = {}, a._pendingActions = [], i = i || {}, a.__v_skip = !0, a._dom = r;
      var o = "canvas", s = "auto", u = !1;
      a[_o] = 1, i.ssr && Uy(function(v) {
        var c = he(v), d = c.dataIndex;
        if (d != null) {
          var y = U();
          return y.set("series_index", c.seriesIndex), y.set("data_index", d), c.ssrType && y.set("ssr_type", c.ssrType), y;
        }
      });
      var l = a._zr = gf(r, {
        renderer: i.renderer || o,
        devicePixelRatio: i.devicePixelRatio,
        width: i.width,
        height: i.height,
        ssr: i.ssr,
        useDirtyRect: G(i.useDirtyRect, u),
        useCoarsePointer: G(i.useCoarsePointer, s),
        pointerSize: i.pointerSize
      });
      a._ssr = i.ssr, a._throttledZrFlush = vc(le(l.flush, l), 17), a._updateTheme(n), a._locale = gx(i.locale || s_), a._coordSysMgr = new vu();
      var f = a._api = Mp(a);
      function h(v, c) {
        return v.__prio - c.__prio;
      }
      return Oo(Is, h), Oo(Zf, h), a._scheduler = new $_(a, f, Zf, Is), a._messageCenter = new d0(), a._initEvents(), a.resize = le(a.resize, a), l.animation.on("frame", a._onframe, a), Tp(l, a), Cp(l, a), va(a), a;
    }
    return e.prototype._onframe = function() {
      if (!this._disposed) {
        var r = this._scheduler, n = this._model, i = this._api;
        if (Dp(this), this[Me]) {
          var a = this[Me].silent;
          this[Ce] = !0, Wn(this);
          try {
            un(this), ir.update.call(this, null, this[Me].updateParams);
          } catch (u) {
            throw this[Ce] = !1, this[Me] = null, u;
          }
          this._zr.flush(), this[Ce] = !1, this[Me] = null, Un.call(this, a), Yn.call(this, a);
        } else if (r.unfinished) {
          var o = mI;
          do {
            r.unfinished = !1;
            var s = Le.getTime();
            r.performSeriesTasks(n), r.performDataProcessorTasks(n), Dl(this, n), r.performVisualTasks(n), wo(this, this._model, i, "remain", {}), o -= Le.getTime() - s;
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
        if (F(n) && (i = n.lazyUpdate, a = n.silent, o = n.replaceMerge, s = n.transition, n = n.notMerge), this[Ce] = !0, Wn(this), !this._model || n) {
          var u = new oM(this._api), l = this._theme, f = this._model = new lc();
          f.scheduler = this._scheduler, f.ssr = this._ssr, f.init(null, null, null, l, this._locale, u);
        }
        this._model.setOption(r, {
          replaceMerge: o
        }, Kf);
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
          } catch (v) {
            throw this[Me] = null, this[Ce] = !1, v;
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
      N(r) && (r = y0[r]), r && (r = Z(r), r && x_(r, !0), this._theme = r);
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
      return T(n, function(i) {
        i.stopAnimation(null, !0);
      }), r.painter.toDataURL();
    }, e.prototype.getDataURL = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      r = r || {};
      var n = r.excludeComponents, i = this._model, a = [], o = this;
      T(n, function(u) {
        i.eachComponent({
          mainType: u
        }, function(l) {
          var f = o._componentsMap[l.__viewId];
          f.group.ignore || (a.push(f), f.group.ignore = !0);
        });
      });
      var s = this._zr.painter.getType() === "svg" ? this.getSvgDataURL() : this.renderToCanvas(r).toDataURL("image/" + (r && r.type || "png"));
      return T(a, function(u) {
        u.group.ignore = !1;
      }), s;
    }, e.prototype.getConnectedDataURL = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      var n = r.type === "svg", i = this.group, a = Math.min, o = Math.max, s = 1 / 0;
      if (As[i]) {
        var u = s, l = s, f = -s, h = -s, v = [], c = r && r.pixelRatio || this.getDevicePixelRatio();
        T(wn, function(_, S) {
          if (_.group === i) {
            var b = n ? _.getZr().painter.getSvgDom().innerHTML : _.renderToCanvas(Z(r)), w = _.getDom().getBoundingClientRect();
            u = a(w.left, u), l = a(w.top, l), f = o(w.right, f), h = o(w.bottom, h), v.push({
              dom: b,
              left: w.left,
              top: w.top
            });
          }
        }), u *= c, l *= c, f *= c, h *= c;
        var d = f - u, y = h - l, p = Le.createCanvas(), g = gf(p, {
          renderer: n ? "svg" : "canvas"
        });
        if (g.resize({
          width: d,
          height: y
        }), n) {
          var m = "";
          return T(v, function(_) {
            var S = _.left - u, b = _.top - l;
            m += '<g transform="translate(' + S + "," + b + ')">' + _.dom + "</g>";
          }), g.painter.getSvgRoot().innerHTML = m, r.connectedBackgroundColor && g.painter.setBackgroundColor(r.connectedBackgroundColor), g.refreshImmediately(), g.painter.toDataURL();
        } else
          return r.connectedBackgroundColor && g.add(new Re({
            shape: {
              x: 0,
              y: 0,
              width: d,
              height: y
            },
            style: {
              fill: r.connectedBackgroundColor
            }
          })), T(v, function(_) {
            var S = new Nr({
              style: {
                x: _.left * c - u,
                y: _.top * c - l,
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
      var i = this._model, a, o = Zu(i, r);
      return T(o, function(s, u) {
        u.indexOf("Models") >= 0 && T(s, function(l) {
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
      var i = this._model, a = Zu(i, r, {
        defaultMainType: "series"
      }), o = a.seriesModel, s = o.getData(), u = a.hasOwnProperty("dataIndexInside") ? a.dataIndexInside : a.hasOwnProperty("dataIndex") ? s.indexOfRawIndex(a.dataIndex) : null;
      return u != null ? OD(s, u, n) : kD(s, n);
    }, e.prototype.getViewOfComponentModel = function(r) {
      return this._componentsMap[r.__viewId];
    }, e.prototype.getViewOfSeriesModel = function(r) {
      return this._chartsMap[r.__viewId];
    }, e.prototype._initEvents = function() {
      var r = this;
      T(LI, function(i) {
        var a = function(o) {
          var s = r.getModel(), u = o.target, l, f = i === "globalout";
          if (f ? l = {} : u && Ji(u, function(y) {
            var p = he(y);
            if (p && p.dataIndex != null) {
              var g = p.dataModel || s.getSeriesByIndex(p.seriesIndex);
              return l = g && g.getDataParams(p.dataIndex, p.dataType, u) || {}, !0;
            } else if (p.eventData)
              return l = R({}, p.eventData), !0;
          }, !0), l) {
            var h = l.componentType, v = l.componentIndex;
            (h === "markLine" || h === "markPoint" || h === "markArea") && (h = "series", v = l.seriesIndex);
            var c = h && v != null && s.getComponent(h, v), d = c && r[c.mainType === "series" ? "_chartsMap" : "_componentsMap"][c.__viewId];
            l.event = o, l.type = i, r._$eventProcessor.eventInfo = {
              targetEl: u,
              packedEvent: l,
              model: c,
              view: d
            }, r.trigger(i, l);
          }
        };
        a.zrEventfulCallAtLast = !0, r._zr.on(i, a, r);
      });
      var n = this._messageCenter;
      T($f, function(i, a) {
        n.on(a, function(o) {
          r.trigger(a, o);
        });
      }), BD(n, this, this._api);
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
      r && jy(this.getDom(), yc, "");
      var n = this, i = n._api, a = n._model;
      T(n._componentsViews, function(o) {
        o.dispose(a, i);
      }), T(n._chartsViews, function(o) {
        o.dispose(a, i);
      }), n._zr.dispose(), n._dom = n._model = n._chartsMap = n._componentsMap = n._chartsViews = n._componentsViews = n._scheduler = n._api = n._zr = n._throttledZrFlush = n._theme = n._coordSysMgr = n._messageCenter = null, delete wn[n.id];
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
              animation: R({
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
      if (F(r) && (n = r, r = ""), r = r || "default", this.hideLoading(), !!jf[r]) {
        var i = jf[r](this._api, n), a = this._zr;
        this._loadingFX = i, a.add(i);
      }
    }, e.prototype.hideLoading = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this._loadingFX && this._zr.remove(this._loadingFX), this._loadingFX = null;
    }, e.prototype.makeActionFromEvent = function(r) {
      var n = R({}, r);
      return n.type = qf[r.type], n;
    }, e.prototype.dispatchAction = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      if (F(n) || (n = {
        silent: !!n
      }), !!Ds[r.type] && this._model) {
        if (this[Ce]) {
          this._pendingActions.push(r);
          return;
        }
        var i = n.silent;
        Il.call(this, r, i);
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
        FD(h._model);
        var v = h._scheduler;
        v.restorePipelines(h._zr, h._model), v.prepareStageTasks(), Ml(h, !0), Ml(h, !1), v.plan();
      }, Ml = function(h, v) {
        for (var c = h._model, d = h._scheduler, y = v ? h._componentsViews : h._chartsViews, p = v ? h._componentsMap : h._chartsMap, g = h._zr, m = h._api, _ = 0; _ < y.length; _++)
          y[_].__alive = !1;
        v ? c.eachComponent(function(w, C) {
          w !== "series" && S(C);
        }) : c.eachSeries(S);
        function S(w) {
          var C = w.__requireNewView;
          w.__requireNewView = !1;
          var M = "_ec_" + w.id + "_" + w.type, x = !C && p[M];
          if (!x) {
            var D = Vt(w.type), I = v ? lt.getClass(D.main, D.sub) : (
              // FIXME:TS
              // (ChartView as ChartViewConstructor).getClass('series', classType.sub)
              // For backward compat, still support a chart type declared as only subType
              // like "liquidfill", but recommend "series.liquidfill"
              // But need a base class to make a type series.
              Wt.getClass(D.sub)
            );
            x = new I(), x.init(c, m), p[M] = x, y.push(x), g.add(x.group);
          }
          w.__viewId = x.__id = M, x.__alive = !0, x.__model = w, x.group.__ecComponentInfo = {
            mainType: w.mainType,
            index: w.componentIndex
          }, !v && d.prepareView(x, w, c, m);
        }
        for (var _ = 0; _ < y.length; ) {
          var b = y[_];
          b.__alive ? _++ : (!v && b.renderTask.dispose(), g.remove(b.group), b.dispose(c, m), y.splice(_, 1), p[b.__id] === b && delete p[b.__id], b.__id = b.group.__ecComponentInfo = null);
        }
      }, So = function(h, v, c, d, y) {
        var p = h._model;
        if (p.setUpdatePayload(c), !d) {
          T([].concat(h._componentsViews).concat(h._chartsViews), S);
          return;
        }
        var g = Iw(c, d, y), m = c.excludeSeriesId, _;
        m != null && (_ = U(), T($e(m), function(b) {
          var w = Yt(b, null);
          w != null && _.set(w, !0);
        })), p && p.eachComponent(g, function(b) {
          var w = _ && _.get(b.id) != null;
          if (!w)
            if (ud(c))
              if (b instanceof $t)
                c.type === bn && !c.notBlur && !b.get(["emphasis", "disabled"]) && JT(b, c, h._api);
              else {
                var C = Bh(b.mainType, b.componentIndex, c.name, h._api), M = C.focusSelf, x = C.dispatchers;
                c.type === bn && M && !c.notBlur && Df(b.mainType, b.componentIndex, h._api), x && T(x, function(D) {
                  c.type === bn ? Cf(D) : xf(D);
                });
              }
            else Af(c) && b instanceof $t && (rC(b, c, h._api), od(b), ht(h));
        }, h), p && p.eachComponent(g, function(b) {
          var w = _ && _.get(b.id) != null;
          w || S(h[d === "series" ? "_chartsMap" : "_componentsMap"][b.__viewId]);
        }, h);
        function S(b) {
          b && b.__alive && b[v] && b[v](b.__model, p, h._api, c);
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
        update: function(h, v) {
          var c = this._model, d = this._api, y = this._zr, p = this._coordSysMgr, g = this._scheduler;
          if (c) {
            zD(c), c.setUpdatePayload(h), g.restoreData(c, h), g.performSeriesTasks(c), p.create(c, d), vt.trigger("coordsys:aftercreate", c, d), g.performDataProcessorTasks(c, h), Dl(this, c), p.update(c, d), n(c), g.performVisualTasks(c, h);
            var m = c.get("backgroundColor") || "transparent";
            y.setBackgroundColor(m);
            var _ = c.get("darkMode");
            _ != null && _ !== "auto" && y.setDarkMode(_), Al(this, c, d, h, v), vt.trigger("afterupdate", c, d);
          }
        },
        /**
         * PENDING: See INCONSISTENCY_OF_BRUSH_SELECTED_EVENT_IN_UPDATE_TRANSFORM
         */
        updateTransform: function(h) {
          var v = this, c = v._model, d = v._api;
          if (c) {
            c.setUpdatePayload(h);
            var y = [];
            c.eachComponent(function(g, m) {
              if (g !== cm) {
                var _ = v.getViewOfComponentModel(m);
                if (_ && _.__alive)
                  if (_.updateTransform) {
                    var S = _.updateTransform(m, c, d, h);
                    S && S.update && y.push(_);
                  } else
                    y.push(_);
              }
            });
            var p = U();
            c.eachSeries(function(g) {
              var m = v._chartsMap[g.__viewId], _ = g.pipelineContext;
              if (m.updateTransform && !_.progressiveRender) {
                var S = m.updateTransform(g, c, d, h);
                S && S.update && p.set(g.uid, 1);
              } else
                p.set(g.uid, 1);
            }), v._scheduler.performVisualTasks(c, h, {
              setDirty: !0,
              dirtyMap: p
            }), wo(v, c, d, h, {}, p), vt.trigger("afterupdate", c, d);
          }
        },
        updateView: function(h) {
          var v = this._model;
          v && (v.setUpdatePayload(h), Wt.markUpdateMethod(h, "updateView"), n(v), this._scheduler.performVisualTasks(v, h, {
            setDirty: !0
          }), Al(this, v, this._api, h, {}), vt.trigger("afterupdate", v, this._api));
        },
        updateVisual: function(h) {
          var v = this, c = this._model;
          c && (c.setUpdatePayload(h), c.eachSeries(function(d) {
            d.getData().clearAllVisual();
          }), Wt.markUpdateMethod(h, "updateVisual"), n(c), this._scheduler.performVisualTasks(c, h, {
            visualType: "visual",
            setDirty: !0
          }), c.eachComponent(function(d, y) {
            if (d !== "series") {
              var p = v.getViewOfComponentModel(y);
              p && p.__alive && p.updateVisual(y, c, v._api, h);
            }
          }), c.eachSeries(function(d) {
            var y = v._chartsMap[d.__viewId];
            y.updateVisual(d, c, v._api, h);
          }), vt.trigger("afterupdate", c, this._api));
        },
        /**
         * @deprecated
         */
        updateLayout: function(h) {
          ir.update.call(this, h);
        }
      };
      function r(h, v, c, d, y) {
        if (h._disposed) {
          h.id;
          return;
        }
        for (var p = h._model, g = h._coordSysMgr.getCoordinateSystems(), m, _ = Zu(p, c), S = 0; S < g.length; S++) {
          var b = g[S];
          if (b[v] && (m = b[v](p, _, d, y)) != null)
            return m;
        }
      }
      bo = r, Dl = function(h, v) {
        var c = h._chartsMap, d = h._scheduler;
        v.eachSeries(function(y) {
          d.updateStreamModes(y, c[y.__viewId]);
        });
      }, Il = function(h, v) {
        var c = this, d = this.getModel(), y = h.type, p = h.escapeConnect, g = Ds[y], m = (g.update || "update").split(":"), _ = m.pop(), S = m[0] != null && Vt(m[0]);
        this[Ce] = !0, Wn(this);
        var b = [h], w = !1;
        h.batch && (w = !0, b = H(h.batch, function(O) {
          return O = _e(R({}, O), h), O.batch = null, O;
        }));
        var C = [], M, x = [], D = g.nonRefinedEventType, I = Af(h), L = ud(h);
        if (L && xm(this._api), T(b, function(O) {
          var V = g.action(O, d, c._api);
          if (g.refineEvent ? x.push(V) : M = V, M = M || R({}, O), M.type = D, C.push(M), L) {
            var K = Mh(h), J = K.queryOptionMap, ee = K.mainTypeSpecified, Q = ee ? J.keys()[0] : "series";
            So(c, _, O, Q), ht(c);
          } else I ? (So(c, _, O, "series"), ht(c)) : S && So(c, _, O, S.main, S.sub);
        }), _ !== "none" && !L && !I && !S)
          try {
            this[Me] ? (un(this), ir.update.call(this, h), this[Me] = null) : ir[_].call(this, h);
          } catch (O) {
            throw this[Ce] = !1, O;
          }
        if (w ? M = {
          type: D,
          escapeConnect: p,
          batch: C
        } : M = C[0], this[Ce] = !1, !v) {
          var A = void 0;
          if (g.refineEvent) {
            var P = g.refineEvent(x, h, d, this._api).eventContent;
            qt(F(P)), A = _e({
              type: g.refinedEventType
            }, P), A.fromAction = h.type, A.fromActionPayload = h, A.escapeConnect = !0;
          }
          var k = this._messageCenter;
          k.trigger(M.type, M), A && k.trigger(A.type, A);
        }
      }, Un = function(h) {
        for (var v = this._pendingActions; v.length; ) {
          var c = v.shift();
          Il.call(this, c, h);
        }
      }, Yn = function(h) {
        !h && this.trigger("updated");
      }, Tp = function(h, v) {
        h.on("rendered", function(c) {
          v.trigger("rendered", c), // Although zr is dirty if initial animation is not finished
          // and this checking is called on frame, we also check
          // animation finished for robustness.
          h.animation.isFinished() && !v[Me] && !v._scheduler.unfinished && !v._pendingActions.length ? v.trigger("finished") : h.refresh();
        });
      }, Cp = function(h, v) {
        h.on("mouseover", function(c) {
          var d = c.target, y = Ji(d, If);
          y && (eC(y, c, v._api), ht(v));
        }).on("mouseout", function(c) {
          var d = c.target, y = Ji(d, If);
          y && (tC(y, c, v._api), ht(v));
        }).on("click", function(c) {
          var d = c.target, y = Ji(d, function(m) {
            return he(m).dataIndex != null;
          }, !0);
          if (y) {
            var p = y.selected ? "unselect" : "select", g = he(y);
            v._api.dispatchAction({
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
        h.clearColorPalette(), h.eachSeries(function(v) {
          v.clearColorPalette();
        });
      }
      function i(h) {
        var v = [], c = [], d = !1;
        if (h.eachComponent(function(m, _) {
          var S = _.get("zlevel") || 0, b = _.get("z") || 0, w = _.getZLevelKey();
          d = d || !!w, (m === "series" ? c : v).push({
            zlevel: S,
            z: b,
            idx: _.componentIndex,
            type: m,
            key: w
          });
        }), d) {
          var y = v.concat(c), p, g;
          Oo(y, function(m, _) {
            return m.zlevel === _.zlevel ? m.z - _.z : m.zlevel - _.zlevel;
          }), T(y, function(m) {
            var _ = h.getComponent(m.type, m.idx), S = m.zlevel, b = m.key;
            p != null && (S = Math.max(p, S)), b ? (S === p && b !== g && S++, g = b) : g && (S === p && S++, g = ""), p = S, _.setZLevel(S);
          });
        }
      }
      Al = function(h, v, c, d, y) {
        i(v), xp(h, v, c, d, y), T(h._chartsViews, function(p) {
          p.__alive = !1;
        }), wo(h, v, c, d, y), T(h._chartsViews, function(p) {
          p.__alive || p.remove(v, c);
        });
      }, xp = function(h, v, c, d, y, p) {
        T(p || h._componentsViews, function(g) {
          var m = g.__model;
          l(m, g), g.render(m, v, c, d), u(m, g), f(m, g);
        });
      }, wo = function(h, v, c, d, y, p) {
        var g = h._scheduler;
        y = R(y || {}, {
          updatedSeries: v.getSeries()
        }), vt.trigger("series:beforeupdate", v, c, y);
        var m = !1;
        v.eachSeries(function(_) {
          var S = h._chartsMap[_.__viewId];
          S.__alive = !0;
          var b = S.renderTask;
          g.updatePayload(b, d), l(_, S), p && p.get(_.uid) && b.dirty(), b.perform(g.getPerformArgs(b)) && (m = !0), S.group.silent = !!_.get("silent"), s(_, S), od(_);
        }), g.unfinished = m || g.unfinished, vt.trigger("series:layoutlabels", v, c, y), vt.trigger("series:transition", v, c, y), v.eachSeries(function(_) {
          var S = h._chartsMap[_.__viewId];
          u(_, S), f(_, S);
        }), o(h, v), vt.trigger("series:afterupdate", v, c, y);
      }, ht = function(h) {
        h[Cl] = !0, h.getZr().wakeUp();
      }, Wn = function(h) {
        h[_o] = (h[_o] + 1) % 1e6;
      }, Dp = function(h) {
        h[Cl] && (h.getZr().storage.traverse(function(v) {
          ia(v) || a(v);
        }), h[Cl] = !1);
      };
      function a(h) {
        for (var v = [], c = h.currentStates, d = 0; d < c.length; d++) {
          var y = c[d];
          y === "emphasis" || y === "blur" || y === "select" || v.push(y);
        }
        h.selected && h.states.select && v.push("select"), h.hoverState === Rh && h.states.emphasis ? v.push("emphasis") : h.hoverState === Eh && h.states.blur && v.push("blur"), h.useStates(v);
      }
      function o(h, v) {
        var c = h._zr;
        if (c.painter.type === "canvas") {
          var d = c.storage, y = 0;
          d.traverse(function(g) {
            g.isGroup || y++;
          });
          var p = y > G(v.get("hoverLayerThreshold"), S_.hoverLayerThreshold) && !W.node && !W.worker;
          (h._usingTHL || p) && (v.eachSeries(function(g) {
            if (!g.preventUsingHoverLayer) {
              var m = h._chartsMap[g.__viewId];
              m.__alive && m.eachRendered(function(_) {
                var S = _.states.emphasis;
                S && S.hoverLayer !== Ym && (S.hoverLayer = p ? Um : Gm);
              });
            }
          }), h._usingTHL = p);
        }
      }
      function s(h, v) {
        var c = h.get("blendMode") || null;
        v.eachRendered(function(d) {
          d.isGroup || (d.style.blend = c);
        });
      }
      function u(h, v) {
        if (!h.preventAutoZ) {
          var c = t_(h);
          v.eachRendered(function(d) {
            return r_(d, c.z, c.zlevel), !0;
          });
        }
      }
      function l(h, v) {
        v.eachRendered(function(c) {
          if (!ia(c)) {
            var d = c.getTextContent(), y = c.getTextGuideLine();
            c.stateTransition && (c.stateTransition = null), d && d.stateTransition && (d.stateTransition = null), y && y.stateTransition && (y.stateTransition = null), c.hasState() ? (c.prevStates = c.currentStates, c.clearStates()) : c.prevStates && (c.prevStates = null);
          }
        });
      }
      function f(h, v) {
        var c = h.getModel("stateAnimation"), d = h.isAnimationEnabled(), y = c.get("duration"), p = y > 0 ? {
          duration: y,
          delay: c.get("delay"),
          easing: c.get("easing")
          // additive: stateAnimationModel.get('additive')
        } : null;
        v.eachRendered(function(g) {
          if (g.states && g.states.emphasis) {
            if (ia(g))
              return;
            if (g instanceof ye && uC(g), g.__dirty) {
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
      Mp = function(h) {
        return new /** @class */
        ((function(v) {
          z(c, v);
          function c() {
            return v !== null && v.apply(this, arguments) || this;
          }
          return c.prototype.getCoordinateSystems = function() {
            return h._coordSysMgr.getCoordinateSystems();
          }, c.prototype.getComponentByElement = function(d) {
            for (; d; ) {
              var y = d.__ecComponentInfo;
              if (y != null)
                return h._model.getComponent(y.mainType, y.index);
              d = d.parent;
            }
          }, c.prototype.enterEmphasis = function(d, y) {
            Cf(d, y), ht(h);
          }, c.prototype.leaveEmphasis = function(d, y) {
            xf(d, y), ht(h);
          }, c.prototype.enterBlur = function(d) {
            QT(d), ht(h);
          }, c.prototype.leaveBlur = function(d) {
            bm(d), ht(h);
          }, c.prototype.enterSelect = function(d) {
            wm(d), ht(h);
          }, c.prototype.leaveSelect = function(d) {
            Tm(d), ht(h);
          }, c.prototype.getModel = function() {
            return h.getModel();
          }, c.prototype.getViewOfComponentModel = function(d) {
            return h.getViewOfComponentModel(d);
          }, c.prototype.getViewOfSeriesModel = function(d) {
            return h.getViewOfSeriesModel(d);
          }, c.prototype.getECUpdateCycleVersion = function() {
            return h[_o];
          }, c.prototype.usingTHL = function() {
            return h._usingTHL;
          }, c;
        })(pm))(h);
      }, g0 = function(h) {
        function v(c, d) {
          for (var y = 0; y < c.length; y++) {
            var p = c[y];
            p[xl] = d;
          }
        }
        T(qf, function(c, d) {
          h._messageCenter.on(d, function(y) {
            if (As[h.group] && h[xl] !== wp) {
              if (y && y.escapeConnect)
                return;
              var p = h.makeActionFromEvent(y), g = [];
              T(wn, function(m) {
                m !== h && m.group === h.group && g.push(m);
              }), v(g, wp), T(g, function(m) {
                m[xl] !== AI && m.dispatchAction(p);
              }), v(g, PI);
            }
          });
        });
      };
    })(), e;
  })(Kt)
), gc = Ms.prototype;
gc.on = h0("on");
gc.off = h0("off");
gc.one = function(t, e, r) {
  var n = this;
  function i() {
    for (var a = [], o = 0; o < arguments.length; o++)
      a[o] = arguments[o];
    e && e.apply && e.apply(this, a), n.off(t, i);
  }
  this.on.call(this, t, i, r);
};
var LI = ["click", "dblclick", "mouseover", "mouseout", "mousemove", "mousedown", "mouseup", "globalout", "contextmenu"];
var Ds = {}, qf = {}, $f = {}, Zf = [], Kf = [], Is = [], y0 = {}, jf = {}, wn = {}, As = {}, EI = +/* @__PURE__ */ new Date() - 0, RI = +/* @__PURE__ */ new Date() - 0, yc = "_echarts_instance_";
function OI(t, e, r) {
  var n = !(r && r.ssr);
  if (n) {
    var i = mc(t);
    if (i)
      return i;
  }
  var a = new Ms(t, e, r);
  return a.id = "ec_" + EI++, wn[a.id] = a, n && jy(t, yc, a.id), g0(a), vt.trigger("afterinit", a), a;
}
function kI(t) {
  if (B(t)) {
    var e = t;
    t = null, T(e, function(r) {
      r.group != null && (t = r.group);
    }), t = t || "g_" + RI++, T(e, function(r) {
      r.group = t;
    });
  }
  return As[t] = !0, t;
}
function m0(t) {
  As[t] = !1;
}
var BI = m0;
function NI(t) {
  N(t) ? t = wn[t] : t instanceof Ms || (t = mc(t)), t instanceof Ms && !t.isDisposed() && t.dispose();
}
function mc(t) {
  return wn[Aw(t, yc)];
}
function FI(t) {
  return wn[t];
}
function _c(t, e) {
  y0[t] = e;
}
function Sc(t) {
  oe(Kf, t) < 0 && Kf.push(t);
}
function bc(t, e) {
  wc(Zf, t, e, TI);
}
function _0(t) {
  mu("afterinit", t);
}
function S0(t) {
  mu("afterupdate", t);
}
function mu(t, e) {
  vt.on(t, e);
}
function Ln(t, e, r) {
  var n, i, a, o, s;
  X(e) && (r = e, e = ""), F(t) ? (n = t.type, i = t.event, o = t.update, s = t.publishNonRefinedEvent, r || (r = t.action), a = t.refineEvent) : (n = t, i = e);
  function u(f) {
    return f.toLowerCase();
  }
  i = u(i || n);
  var l = a ? u(n) : i;
  Ds[n] || (qt(bp.test(n) && bp.test(i)), a && qt(i !== n), Ds[n] = {
    actionType: n,
    refinedEventType: i,
    nonRefinedEventType: l,
    update: o,
    action: r,
    refineEvent: a
  }, $f[i] = 1, a && s && ($f[l] = 1), qf[l] = n);
}
function b0(t, e) {
  vu.register(t, e);
}
function zI(t) {
  var e = vu.get(t);
  if (e)
    return e.getDimensionsInfo ? e.getDimensionsInfo() : e.dimensions.slice();
}
function HI(t, e) {
}
function w0(t, e) {
  wc(Is, t, e, s0, "layout");
}
function Fr(t, e) {
  wc(Is, t, e, u0, "visual");
}
var Ip = [];
function wc(t, e, r, n, i, a) {
  if ((X(e) || F(e)) && (r = e, e = n), !(oe(Ip, r) >= 0)) {
    Ip.push(r);
    var o = $_.wrapStageHandler(r, i);
    o.__prio = e, o.__raw = r, t.push(o);
  }
}
function Tc(t, e) {
  jf[t] = e;
}
function VI(t) {
  ry({
    createCanvas: t
  });
}
function T0(t, e, r) {
  var n = e0("registerMap");
  n && n(t, e, r);
}
function GI(t) {
  var e = e0("getMap");
  return e && e(t);
}
var C0 = GM;
Fr(pc, mD);
Fr(yu, _D);
Fr(yu, SD);
Fr(pc, ED);
Fr(yu, RD);
Fr(l0, dI);
Sc(x_);
bc(SI, mM);
Tc("default", bD);
Ln({
  type: bn,
  event: bn,
  update: bn
}, Ee);
Ln({
  type: Go,
  event: Go,
  update: Go
}, Ee);
Ln({
  type: vs,
  event: Oh,
  update: vs,
  action: Ee,
  refineEvent: Cc,
  publishNonRefinedEvent: !0
});
Ln({
  type: Tf,
  event: Oh,
  update: Tf,
  action: Ee,
  refineEvent: Cc,
  publishNonRefinedEvent: !0
});
Ln({
  type: ds,
  event: Oh,
  update: ds,
  action: Ee,
  refineEvent: Cc,
  publishNonRefinedEvent: !0
});
function Cc(t, e, r, n) {
  return {
    eventContent: {
      selected: nC(r),
      isFromClick: e.isFromClick || !1
    }
  };
}
_c("default", {});
_c("dark", Q_);
var UI = {};
function Fi(t) {
  return t == null ? 0 : t.length || 1;
}
function Ap(t) {
  return t;
}
var YI = (
  /** @class */
  (function() {
    function t(e, r, n, i, a, o) {
      this._old = e, this._new = r, this._oldKeyGetter = n || Ap, this._newKeyGetter = i || Ap, this.context = a, this._diffModeMultiple = o === "multiple";
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
        var s = i[o], u = n[s], l = Fi(u);
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
        var u = a[s], l = n[u], f = i[u], h = Fi(l), v = Fi(f);
        if (h > 1 && v === 1)
          this._updateManyToOne && this._updateManyToOne(f, l), i[u] = null;
        else if (h === 1 && v > 1)
          this._updateOneToMany && this._updateOneToMany(f, l), i[u] = null;
        else if (h === 1 && v === 1)
          this._update && this._update(f, l), i[u] = null;
        else if (h > 1 && v > 1)
          this._updateManyToMany && this._updateManyToMany(f, l), i[u] = null;
        else if (h > 1)
          for (var c = 0; c < h; c++)
            this._remove && this._remove(l[c]);
        else
          this._remove && this._remove(l);
      }
      this._performRestAdd(o, i);
    }, t.prototype._performRestAdd = function(e, r) {
      for (var n = 0; n < e.length; n++) {
        var i = e[n], a = r[i], o = Fi(a);
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
          var u = r[s], l = Fi(u);
          l === 0 ? (r[s] = o, a && n.push(s)) : l === 1 ? r[s] = [u, o] : u.push(o);
        }
      }
    }, t;
  })()
), WI = (
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
function XI(t, e) {
  var r = {}, n = r.encode = {}, i = U(), a = [], o = [], s = {};
  T(t.dimensions, function(v) {
    var c = t.getDimensionInfo(v), d = c.coordDim;
    if (d) {
      var y = c.coordDimIndex;
      Pl(n, d)[y] = v, c.isExtraCoord || (i.set(d, 1), $I(c.type) && (a[0] = v), Pl(s, d)[y] = t.getDimensionIndex(c.name)), c.defaultTooltip && o.push(v);
    }
    vm.each(function(p, g) {
      var m = Pl(n, g), _ = c.otherDims[g];
      _ != null && _ !== !1 && (m[_] = c.name);
    });
  });
  var u = [], l = {};
  i.each(function(v, c) {
    var d = n[c];
    l[c] = d[0], u = u.concat(d);
  }), r.dataDimsOnCoord = u, r.dataDimIndicesOnCoord = H(u, function(v) {
    return t.getDimensionInfo(v).storeDimIndex;
  }), r.encodeFirstDimNotExtra = l;
  var f = n.label;
  f && f.length && (a = f.slice());
  var h = n.tooltip;
  return h && h.length ? o = h.slice() : o.length || (o = a.slice()), n.defaultedLabel = a, n.defaultedTooltip = o, r.userOutput = new WI(s, e), r;
}
function Pl(t, e) {
  return t.hasOwnProperty(e) || (t[e] = []), t[e];
}
function qI(t) {
  return t === "category" ? "ordinal" : t === "time" ? "time" : "float";
}
function $I(t) {
  return !(t === "ordinal" || t === "time");
}
var Ko = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.otherDims = {}, e != null && R(this, e);
    }
    return t;
  })()
), ZI = de(), KI = {
  float: "f",
  int: "i",
  ordinal: "o",
  number: "n",
  time: "t"
}, x0 = (
  /** @class */
  (function() {
    function t(e) {
      this.dimensions = e.dimensions, this._dimOmitted = e.dimensionOmitted, this.source = e.source, this._fullDimCount = e.fullDimensionCount, this._updateDimOmitted(e.dimensionOmitted);
    }
    return t.prototype.isDimensionOmitted = function() {
      return this._dimOmitted;
    }, t.prototype._updateDimOmitted = function(e) {
      this._dimOmitted = e, e && (this._dimNameMap || (this._dimNameMap = I0(this.source)));
    }, t.prototype.getSourceDimensionIndex = function(e) {
      return G(this._dimNameMap.get(e), -1);
    }, t.prototype.getSourceDimension = function(e) {
      var r = this.source.dimensionsDefine;
      if (r)
        return r[e];
    }, t.prototype.makeStoreSchema = function() {
      for (var e = this._fullDimCount, r = I_(this.source), n = !A0(e), i = "", a = [], o = 0, s = 0; o < e; o++) {
        var u = void 0, l = void 0, f = void 0, h = this.dimensions[s];
        if (h && h.storeDimIndex === o)
          u = r ? h.name : null, l = h.type, f = h.ordinalMeta, s++;
        else {
          var v = this.getSourceDimension(o);
          v && (u = r ? v.name : null, l = v.type);
        }
        a.push({
          property: u,
          type: l,
          ordinalMeta: f
        }), r && u != null && (!h || !h.isCalculationCoord) && (i += n ? u.replace(/\`/g, "`1").replace(/\$/g, "`2") : u), i += "$", i += KI[l] || "f", f && (i += f.uid), i += "$";
      }
      var c = this.source, d = [c.seriesLayoutBy, c.startIndex, i].join("$$");
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
function M0(t) {
  return t instanceof x0;
}
function D0(t) {
  for (var e = U(), r = 0; r < (t || []).length; r++) {
    var n = t[r], i = F(n) ? n.name : n;
    i != null && e.get(i) == null && e.set(i, r);
  }
  return e;
}
function I0(t) {
  var e = ZI(t);
  return e.dimNameMap || (e.dimNameMap = D0(t.dimensionsDefine));
}
function A0(t) {
  return t > 30;
}
var zi = F, br = H, jI = typeof Int32Array > "u" ? Array : Int32Array, QI = "e\0\0", Pp = -1, JI = ["hasItemOption", "_nameList", "_idList", "_invertedIndicesMap", "_dimSummary", "userOutput", "_rawData", "_dimValueGetter", "_nameDimIdx", "_idDimIdx", "_nameRepeatCount"], eA = ["_approximateExtent"], Lp, To, Hi, Vi, Ll, Gi, El, P0 = (
  /** @class */
  (function() {
    function t(e, r) {
      this.type = "list", this._dimOmitted = !1, this._nameList = [], this._idList = [], this._visual = {}, this._layout = {}, this._itemVisuals = [], this._itemLayouts = [], this._graphicEls = [], this._approximateExtent = {}, this._calculationInfo = {}, this.hasItemOption = !1, this.TRANSFERABLE_METHODS = ["cloneShallow", "downSample", "minmaxDownSample", "lttbDownSample", "map"], this.CHANGABLE_METHODS = ["filterSelf", "selectRange"], this.DOWNSAMPLE_METHODS = ["downSample", "minmaxDownSample", "lttbDownSample"];
      var n, i = !1;
      M0(e) ? (n = e.dimensions, this._dimOmitted = e.isDimensionOmitted(), this._schema = e) : (i = !0, n = e), n = n || ["x", "y"];
      for (var a = {}, o = [], s = {}, u = !1, l = {}, f = 0; f < n.length; f++) {
        var h = n[f], v = N(h) ? new Ko({
          name: h
        }) : h instanceof Ko ? h : new Ko(h), c = v.name;
        v.type = v.type || "float", v.coordDim || (v.coordDim = c, v.coordDimIndex = 0);
        var d = v.otherDims = v.otherDims || {};
        o.push(c), a[c] = v, l[c] != null && (u = !0), v.createInvertedIndices && (s[c] = []), i && (v.storeDimIndex = f), d.itemName === 0 && (this._nameDimIdx = v.storeDimIndex), d.itemId === 0 && (this._idDimIdx = v.storeDimIndex);
      }
      if (this.dimensions = o, this._dimInfos = a, this._initGetDimensionInfo(u), this.hostModel = r, this._invertedIndicesMap = s, this._dimOmitted) {
        var y = this._dimIdxToName = U();
        T(o, function(p) {
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
      if (e instanceof Nf && (a = e), !a) {
        var o = this.dimensions, s = fc(e) || Ze(e) ? new A_(e, o.length) : e;
        a = new Nf();
        var u = br(o, function(l) {
          return {
            type: i._dimInfos[l].type,
            property: l
          };
        });
        a.initData(s, u, n);
      }
      this._store = a, this._nameList = (r || []).slice(), this._idList = [], this._nameRepeatCount = {}, this._doInit(0, a.count()), this._dimSummary = XI(this, this._schema), this.userOutput = this._dimSummary.userOutput;
    }, t.prototype.appendData = function(e) {
      var r = this._store.appendData(e);
      this._doInit(r[0], r[1]);
    }, t.prototype.appendValues = function(e, r) {
      var n = this._store.appendValues(e, r && r.length), i = n.start, a = n.end, o = this._shouldMakeIdFromName();
      if (this._updateOrdinalMeta(), r)
        for (var s = i; s < a; s++) {
          var u = s - i;
          this._nameList[s] = r[u], o && El(this, s);
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
            if (!this.hasItemOption && yw(h) && (this.hasItemOption = !0), h) {
              var v = h.name;
              a[f] == null && v != null && (a[f] = Yt(v, null));
              var c = h.id;
              o[f] == null && c != null && (o[f] = Yt(c, null));
            }
          }
        if (this._shouldMakeIdFromName())
          for (var f = e; f < r; f++)
            El(this, f);
        Lp(this);
      }
    }, t.prototype.getApproximateExtent = function(e, r) {
      return this._approximateExtent[e] || this._store.getDataExtent(this._getStoreDimIndex(e), r);
    }, t.prototype.setApproximateExtent = function(e, r) {
      r = this.getDimension(r), this._approximateExtent[r] = e.slice();
    }, t.prototype.getCalculationInfo = function(e) {
      return this._calculationInfo[e];
    }, t.prototype.setCalculationInfo = function(e, r) {
      zi(e) ? R(this._calculationInfo, e) : this._calculationInfo[e] = r;
    }, t.prototype.getName = function(e) {
      var r = this.getRawIndex(e), n = this._nameList[r];
      return n == null && this._nameDimIdx != null && (n = Hi(this, this._nameDimIdx, r)), n == null && (n = ""), n;
    }, t.prototype._getCategory = function(e, r) {
      var n = this._store.get(e, r), i = this._store.getOrdinalMeta(e);
      return i ? i.categories[n] : n;
    }, t.prototype.getId = function(e) {
      return To(this, this.getRawIndex(e));
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
      return i == null || isNaN(i) ? Pp : i;
    }, t.prototype.each = function(e, r, n) {
      X(e) && (n = r, r = e, e = []);
      var i = n || this, a = br(Vi(e), this._getStoreDimIndex, this);
      this._store.each(a, i ? le(r, i) : r);
    }, t.prototype.filterSelf = function(e, r, n) {
      X(e) && (n = r, r = e, e = []);
      var i = n || this, a = br(Vi(e), this._getStoreDimIndex, this);
      return this._store = this._store.filter(a, i ? le(r, i) : r), this;
    }, t.prototype.selectRange = function(e) {
      var r = this, n = {}, i = me(e);
      return T(i, function(a) {
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
      var a = n || i || this, o = br(Vi(e), this._getStoreDimIndex, this), s = Gi(this);
      return s._store = this._store.map(o, a ? le(r, a) : r), s;
    }, t.prototype.modify = function(e, r, n, i) {
      var a = n || i || this, o = br(Vi(e), this._getStoreDimIndex, this);
      this._store.modify(o, a ? le(r, a) : r);
    }, t.prototype.downSample = function(e, r, n, i) {
      var a = Gi(this);
      return a._store = this._store.downSample(this._getStoreDimIndex(e), r, n, i), a;
    }, t.prototype.minmaxDownSample = function(e, r) {
      var n = Gi(this);
      return n._store = this._store.minmaxDownSample(this._getStoreDimIndex(e), r), n;
    }, t.prototype.lttbDownSample = function(e, r) {
      var n = Gi(this);
      return n._store = this._store.lttbDownSample(this._getStoreDimIndex(e), r), n;
    }, t.prototype.getRawDataItem = function(e) {
      return this._store.getRawDataItem(e);
    }, t.prototype.getItemModel = function(e) {
      var r = this.hostModel, n = this.getRawDataItem(e);
      return new ce(n, r, r && r.ecModel);
    }, t.prototype.diff = function(e) {
      var r = this;
      return new YI(e ? e.getStore().getIndices() : [], this.getStore().getIndices(), function(n) {
        return To(e, n);
      }, function(n) {
        return To(r, n);
      });
    }, t.prototype.getVisual = function(e) {
      var r = this._visual;
      return r && r[e];
    }, t.prototype.setVisual = function(e, r) {
      this._visual = this._visual || {}, zi(e) ? R(this._visual, e) : this._visual[e] = r;
    }, t.prototype.getItemVisual = function(e, r) {
      var n = this._itemVisuals[e], i = n && n[r];
      return i ?? this.getVisual(r);
    }, t.prototype.hasItemVisual = function() {
      return this._itemVisuals.length > 0;
    }, t.prototype.ensureUniqueItemVisual = function(e, r) {
      var n = this._itemVisuals, i = n[e];
      i || (i = n[e] = {});
      var a = i[r];
      return a == null && (a = this.getVisual(r), B(a) ? a = a.slice() : zi(a) && (a = R({}, a)), i[r] = a), a;
    }, t.prototype.setItemVisual = function(e, r, n) {
      var i = this._itemVisuals[e] || {};
      this._itemVisuals[e] = i, zi(r) ? R(i, r) : i[r] = n;
    }, t.prototype.clearAllVisual = function() {
      this._visual = {}, this._itemVisuals = [];
    }, t.prototype.setLayout = function(e, r) {
      zi(e) ? R(this._layout, e) : this._layout[e] = r;
    }, t.prototype.getLayout = function(e) {
      return this._layout[e];
    }, t.prototype.getItemLayout = function(e) {
      return this._itemLayouts[e];
    }, t.prototype.setItemLayout = function(e, r, n) {
      this._itemLayouts[e] = n ? R(this._itemLayouts[e] || {}, r) : r;
    }, t.prototype.clearItemLayouts = function() {
      this._itemLayouts.length = 0;
    }, t.prototype.setItemGraphicEl = function(e, r) {
      var n = this.hostModel && this.hostModel.seriesIndex;
      HT(n, this.dataType, e, r), this._graphicEls[e] = r;
    }, t.prototype.getItemGraphicEl = function(e) {
      return this._graphicEls[e];
    }, t.prototype.eachItemGraphicEl = function(e, r) {
      T(this._graphicEls, function(n, i) {
        n && e && e.call(r, n, i);
      });
    }, t.prototype.cloneShallow = function(e) {
      return e || (e = new t(this._schema ? this._schema : br(this.dimensions, this._getDimInfo, this), this.hostModel)), Ll(e, this), e._store = this._store, e;
    }, t.prototype.wrapMethod = function(e, r) {
      var n = this[e];
      X(n) && (this.__wrappedMethods = this.__wrappedMethods || [], this.__wrappedMethods.push(e), this[e] = function() {
        var i = n.apply(this, arguments);
        return r.apply(this, [i].concat(Gs(arguments)));
      });
    }, t.internalField = (function() {
      Lp = function(e) {
        var r = e._invertedIndicesMap;
        T(r, function(n, i) {
          var a = e._dimInfos[i], o = a.ordinalMeta, s = e._store;
          if (o) {
            n = r[i] = new jI(o.categories.length);
            for (var u = 0; u < n.length; u++)
              n[u] = Pp;
            for (var u = 0; u < s.count(); u++)
              n[s.get(a.storeDimIndex, u)] = u;
          }
        });
      }, Hi = function(e, r, n) {
        return Yt(e._getCategory(r, n), null);
      }, To = function(e, r) {
        var n = e._idList[r];
        return n == null && e._idDimIdx != null && (n = Hi(e, e._idDimIdx, r)), n == null && (n = QI + r), n;
      }, Vi = function(e) {
        return B(e) || (e = e != null ? [e] : []), e;
      }, Gi = function(e) {
        var r = new t(e._schema ? e._schema : br(e.dimensions, e._getDimInfo, e), e.hostModel);
        return Ll(r, e), r;
      }, Ll = function(e, r) {
        T(JI.concat(r.__wrappedMethods || []), function(n) {
          r.hasOwnProperty(n) && (e[n] = r[n]);
        }), e.__wrappedMethods = r.__wrappedMethods, T(eA, function(n) {
          e[n] = Z(r[n]);
        }), e._calculationInfo = R({}, r._calculationInfo);
      }, El = function(e, r) {
        var n = e._nameList, i = e._idList, a = e._nameDimIdx, o = e._idDimIdx, s = n[r], u = i[r];
        if (s == null && a != null && (n[r] = s = Hi(e, a, r)), u == null && o != null && (i[r] = u = Hi(e, o, r)), u == null && s != null) {
          var l = e._nameRepeatCount, f = l[s] = (l[s] || 0) + 1;
          u = s, f > 1 && (u += "__ec__" + f), i[r] = u;
        }
      };
    })(), t;
  })()
);
function tA(t, e) {
  return L0(t, e).dimensions;
}
function L0(t, e) {
  fc(t) || (t = M_(t)), e = e || {};
  var r = e.coordDimensions || [], n = e.dimensionsDefine || t.dimensionsDefine || [], i = U(), a = [], o = rA(t, r, n, e.dimensionsCount), s = e.canOmitUnusedDimensions && A0(o), u = n === t.dimensionsDefine, l = u ? I0(t) : D0(n), f = e.encodeDefine;
  !f && e.encodeDefaulter && (f = e.encodeDefaulter(t, o));
  for (var h = U(f), v = new k_(o), c = 0; c < v.length; c++)
    v[c] = -1;
  function d(x) {
    var D = v[x];
    if (D < 0) {
      var I = n[x], L = F(I) ? I : {
        name: I
      }, A = new Ko(), P = L.name;
      P != null && l.get(P) != null && (A.name = A.displayName = P), L.type != null && (A.type = L.type), L.displayName != null && (A.displayName = L.displayName);
      var k = a.length;
      return v[x] = k, A.storeDimIndex = x, a.push(A), A;
    }
    return a[D];
  }
  if (!s)
    for (var c = 0; c < o; c++)
      d(c);
  h.each(function(x, D) {
    var I = $e(x).slice();
    if (I.length === 1 && !N(I[0]) && I[0] < 0) {
      h.set(D, !1);
      return;
    }
    var L = h.set(D, []);
    T(I, function(A, P) {
      var k = N(A) ? l.get(A) : A;
      k != null && k < o && (L[P] = k, p(d(k), D, P));
    });
  });
  var y = 0;
  T(r, function(x) {
    var D, I, L, A;
    if (N(x))
      D = x, A = {};
    else {
      A = x, D = A.name;
      var P = A.ordinalMeta;
      A.ordinalMeta = null, A = R({}, A), A.ordinalMeta = P, I = A.dimsDef, L = A.otherDims, A.name = A.coordDim = A.coordDimIndex = A.dimsDef = A.otherDims = null;
    }
    var k = h.get(D);
    if (k !== !1) {
      if (k = $e(k), !k.length)
        for (var O = 0; O < (I && I.length || 1); O++) {
          for (; y < o && d(y).coordDim != null; )
            y++;
          y < o && k.push(y++);
        }
      T(k, function(V, K) {
        var J = d(V);
        if (u && A.type != null && (J.type = A.type), p(_e(J, A), D, K), J.name == null && I) {
          var ee = I[K];
          !F(ee) && (ee = {
            name: ee
          }), J.name = J.displayName = ee.name, J.defaultTooltip = ee.defaultTooltip;
        }
        L && _e(J.otherDims, L);
      });
    }
  });
  function p(x, D, I) {
    vm.get(D) != null ? x.otherDims[D] = I : (x.coordDim = D, x.coordDimIndex = I, i.set(D, !0));
  }
  var g = e.generateCoord, m = e.generateCoordCount, _ = m != null;
  m = g ? m || 1 : 0;
  var S = g || "value";
  function b(x) {
    x.name == null && (x.name = x.coordDim);
  }
  if (s)
    T(a, function(x) {
      b(x);
    }), a.sort(function(x, D) {
      return x.storeDimIndex - D.storeDimIndex;
    });
  else
    for (var w = 0; w < o; w++) {
      var C = d(w), M = C.coordDim;
      M == null && (C.coordDim = nA(S, i, _), C.coordDimIndex = 0, (!g || m <= 0) && (C.isExtraCoord = !0), m--), b(C), C.type == null && (T_(t, w) === nt.Must || C.isExtraCoord && (C.otherDims.itemName != null || C.otherDims.seriesName != null)) && (C.type = "ordinal");
    }
  return Dh(a, function(x) {
    return x.name;
  }, function(x, D) {
    D > 0 && (x.name = x.name + (D - 1));
  }), new x0({
    source: t,
    dimensions: a,
    fullDimensionCount: o,
    dimensionOmitted: s
  });
}
function rA(t, e, r, n) {
  var i = Math.max(t.dimensionsDetectedCount || 1, e.length, r.length, n || 0);
  return T(e, function(a) {
    var o;
    F(a) && (o = a.dimsDef) && (i = Math.max(i, o.length));
  }), i;
}
function nA(t, e, r) {
  if (r || e.hasKey(t)) {
    for (var n = 0; e.hasKey(t + n); )
      n++;
    t += n;
  }
  return e.set(t, !0), t;
}
var iA = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.coordSysDims = [], this.axisMap = U(), this.categoryAxisMap = U(), this.coordSysName = e;
    }
    return t;
  })()
);
function aA(t) {
  var e = t.get("coordinateSystem"), r = new iA(e), n = oA[e];
  if (n)
    return n(t, r, r.axisMap, r.categoryAxisMap), r;
}
var oA = {
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
    T(a.parallelAxisIndex, function(s, u) {
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
function E0(t, e, r) {
  r = r || {};
  var n = r.byIndex, i = r.stackedCoordDimension, a, o, s;
  sA(e) ? a = e : (o = e.schema, a = o.dimensions, s = e.store);
  var u = !!(t && t.get("stack")), l, f, h, v, c = !0;
  function d(S) {
    return S.type !== "ordinal" && S.type !== "time";
  }
  if (T(a, function(S, b) {
    N(S) && (a[b] = S = {
      name: S
    }), d(S) || (c = !1);
  }), T(a, function(S, b) {
    u && !S.isExtraCoord && (!n && !l && S.ordinalMeta && (l = S), !f && d(S) && (!c || S.coordDim !== "x" && S.coordDim !== "angle") && (!i || i === S.coordDim) && (f = S));
  }), f && !n && !l && (n = !0), f) {
    h = "__\0ecstackresult_" + t.id, v = "__\0ecstackedover_" + t.id, l && (l.createInvertedIndices = !0);
    var y = f.coordDim, p = f.type, g = 0;
    T(a, function(S) {
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
      name: v,
      // This dimension contains stack base (generally, 0), so do not set it as
      // `stackedDimCoordDim` to avoid extent calculation, consider log scale.
      coordDim: v,
      coordDimIndex: g + 1,
      type: p,
      isExtraCoord: !0,
      isCalculationCoord: !0,
      storeDimIndex: a.length + 1
    };
    o ? (s && (m.storeDimIndex = s.ensureCalculationDimension(v, p), _.storeDimIndex = s.ensureCalculationDimension(h, p)), o.appendCalculationDimension(m), o.appendCalculationDimension(_)) : (a.push(m), a.push(_));
  }
  return {
    stackedDimension: f && f.name,
    stackedByDimension: l && l.name,
    isStackedByIndex: n,
    stackedOverDimension: v,
    stackResultDimension: h
  };
}
function sA(t) {
  return !M0(t.schema);
}
function R0(t, e) {
  return !!e && e === t.getCalculationInfo("stackedDimension");
}
function O0(t, e) {
  return R0(t, e) ? t.getCalculationInfo("stackResultDimension") : e;
}
function uA(t, e) {
  var r = t.get("coordinateSystem"), n = vu.get(r), i;
  return e && e.coordSysDims && (i = H(e.coordSysDims, function(a) {
    var o = {
      name: a
    }, s = e.axisMap.get(a);
    if (s) {
      var u = s.get("type");
      o.type = qI(u);
    }
    return o;
  })), i || (i = n && (n.getDimensionsInfo ? n.getDimensionsInfo() : n.dimensions.slice()) || ["x", "y"]), i;
}
function lA(t, e, r) {
  var n, i;
  return r && T(t, function(a, o) {
    var s = a.coordDim, u = r.categoryAxisMap.get(s);
    u && (n == null && (n = o), a.ordinalMeta = u.getOrdinalMeta(), e && (a.createInvertedIndices = !0)), a.otherDims.itemName != null && (i = !0);
  }), !i && n != null && (t[n].otherDims.itemName = 0), n;
}
function fA(t, e, r) {
  r = r || {};
  var n = e.getSourceManager(), i, a = !1;
  i = n.getSource(), a = i.sourceFormat === ft;
  var o = aA(e), s = uA(e, o), u = r.useEncodeDefaulter, l = X(u) ? u : u ? Te($x, s, e) : null, f = {
    coordDimensions: s,
    generateCoord: r.generateCoord,
    encodeDefine: e.getEncode(),
    encodeDefaulter: l,
    canOmitUnusedDimensions: !a
  }, h = L0(i, f), v = lA(h.dimensions, r.createInvertedIndices, o), c = a ? null : n.getSharedDataStore(h), d = E0(e, {
    schema: h,
    store: c
  }), y = new P0(h, e);
  y.setCalculationInfo(d);
  var p = v != null && hA(i) ? function(g, m, _, S) {
    return S === v ? _ : this.defaultDimValueGetter(g, m, _, S);
  } : null;
  return y.hasItemOption = !1, y.initData(
    // Try to reuse the data store in sourceManager if using dataset.
    a ? i : c,
    null,
    p
  ), y;
}
function hA(t) {
  if (t.sourceFormat === ft) {
    var e = cA(t.data || []);
    return !B(Ra(e));
  }
}
function cA(t) {
  for (var e = 0; e < t.length && t[e] == null; )
    e++;
  return t[e];
}
var Rt = (
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
js(Rt);
var vA = 0, Ep = (
  /** @class */
  (function() {
    function t(e) {
      this.categories = e.categories || [], this._needCollect = e.needCollect, this._deduplication = e.deduplication, this.uid = ++vA, this._onCollect = e.onCollect;
    }
    return t.createByAxisModel = function(e) {
      var r = e.option, n = r.data, i = n && H(n, dA);
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
function dA(t) {
  return F(t) && t.value != null ? t.value : t + "";
}
var bt = 0, Ma = 1, pA = {
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
}, k0 = me(pA), Ps = 2, B0 = 3;
function xc(t, e, r) {
  var n;
  t = t || {};
  var i = Xe();
  if (i) {
    var a = i.createBreakScaleMapper(e, r);
    a.hasBreaks() && (T(k0, function(o) {
      a[o] && (t[o] = le(a[o], a));
    }), n = a);
  }
  return n == null && yA(t, r), {
    brk: n,
    mapper: t
  };
}
function N0(t, e) {
  T(k0, function(r) {
    t[r] = e[r];
  });
}
function F0(t, e) {
  t.freeze = Ee;
}
function Da(t) {
  return t.getExtentUnsafe(bt, Ps);
}
function z0(t, e) {
  return t.getExtentUnsafe(Ma, e) || t.getExtentUnsafe(bt, e);
}
function gA(t) {
  var e = z0(t, B0);
  return e[1] - e[0];
}
function _u(t) {
  var e = t.getExtentUnsafe(bt, B0);
  return e[1] - e[0];
}
function yA(t, e) {
  var r = t || {}, n = [];
  return r._extents = n, n[bt] = e ? e.slice() : zt(), R(r, mA), r;
}
var mA = {
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
    var e = z0(this, null);
    return t >= e[0] && t <= e[1];
  },
  getExtent: function() {
    return this._extents[bt].slice();
  },
  getExtentUnsafe: function(t) {
    return this._extents[t];
  },
  setExtent: function(t, e) {
    Rp(this._extents, bt, t, e);
  },
  setExtent2: function(t, e, r) {
    var n = this._extents;
    n[t] || (n[t] = n[bt].slice()), Rp(n, t, e, r);
  },
  freeze: function() {
  }
};
function Rp(t, e, r, n) {
  ci(r, n) && (t[e][0] = r, t[e][1] = n);
}
function _A(t) {
  return Ls(t) || Su(t);
}
function Ls(t) {
  return t.type === "interval";
}
function Mc(t) {
  return t.type === "time";
}
function Su(t) {
  return t.type === "log";
}
function yr(t) {
  return t.type === "ordinal";
}
function jE(t) {
  var e = Ks(t), r = In(10, e), n = hr(t / r);
  return n ? n === 2 ? n = 3 : n === 3 ? n = 5 : n *= 2 : n = 1, Ie(n * r, -e);
}
function Va(t) {
  return lr(t) + 2;
}
function Co(t, e) {
  return Tn(t) / Tn(e);
}
function Rl(t, e, r) {
  var n = r && r.lookup;
  if (n) {
    for (var i = 0; i < n.from.length; i++)
      if (t === n.from[i])
        return n.to[i];
  }
  return In(e, t);
}
function SA(t, e, r) {
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
function bA(t, e) {
  return [t[0] !== e[0], t[1] !== e[1]];
}
function Dc(t, e) {
  return t = t || e, hr(ge(t, 1));
}
function H0(t, e, r) {
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
var V0 = (
  /** @class */
  (function(t) {
    z(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "ordinal", n.parse = e.parse, N0(n, e.decoratedMethods);
      var i = r.ordinalMeta;
      i || (i = new Ep({})), B(i) && (i = new Ep({
        categories: H(i, function(o) {
          return F(o) ? o.value : o;
        })
      })), n._ordinalMeta = i;
      var a = xc(
        null,
        null,
        // Do not support break in OrdinalScale yet.
        r.extent || [0, i.categories.length - 1]
      );
      return n._mapper = a.mapper, F0(n), n;
    }
    return e.parse = function(r) {
      return r == null ? r = NaN : N(r) ? (r = this._ordinalMeta.getOrdinal(r), r == null && (r = NaN)) : r = hr(r), r;
    }, e.prototype.getTicks = function() {
      var r = [];
      return H0(this, 0, function(n) {
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
  })(Rt)
);
Rt.registerClass(V0);
function Ic(t, e, r, n) {
  for (var i = t.getTicks({
    expandToNicedExtent: !0
  }), a = [], o = t.getExtent(), s = 1; s < i.length; s++) {
    var u = i[s], l = i[s - 1];
    if (!(l.break || u.break)) {
      for (var f = 0, h = [], v = u.value - l.value, c = v / e, d = Va(c); f < e - 1; ) {
        var y = Ie(l.value + (f + 1) * c, d);
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
    z(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "interval", n.parse = e.parse, r = r || {};
      var i = u_(n, r), a = xc(n, i, null);
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
      var v = 3e3;
      a[0] < o[0] && h.push({
        value: r.expandToNicedExtent ? Ie(o[0] - i, s) : a[0]
      });
      for (var c = function(_, S) {
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
          var g = l.calcNiceTickMultiple(y, c);
          g >= 0 && (y = Ie(y + g * i, s));
        }
        if (h.length > 0 && y === h[h.length - 1].value)
          break;
        if (h.length > v)
          return [];
      }
      var m = h.length ? h[h.length - 1].value : o[1];
      return a[1] > m && h.push({
        value: r.expandToNicedExtent ? Ie(m + i, s) : a[1]
      }), f && u.pruneTicksByBreak(r.pruneByBreak, h, l.breaks, function(_) {
        return _.value;
      }, n.interval, a), f && r.breakTicks !== "none" && u.addBreaksToTicks(h, l.breaks, a), h;
    }, e.prototype.getMinorTicks = function(r) {
      return Ic(this, r, ys(this), this._cfg.interval);
    }, e.prototype.getLabel = function(r, n) {
      if (r == null)
        return "";
      var i = n && n.precision;
      i == null ? i = lr(r.value) || 0 : i === "auto" && (i = this._cfg.intervalPrecision);
      var a = Ie(r.value, i, !0);
      return nc(a);
    }, e.type = "interval", e;
  })(Rt)
);
Rt.registerClass(oi);
var wA = function(t, e, r, n) {
  for (; r < n; ) {
    var i = r + n >>> 1;
    t[i][1] < e ? r = i + 1 : n = i;
  }
  return r;
}, G0 = (
  /** @class */
  (function(t) {
    z(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "time", n.parse = e.parse, n._locale = r.locale, n._useUTC = r.useUTC, n._interval = 0;
      var i = u_(n, r), a = xc(n, i, null);
      return n.brk = a.brk, n;
    }
    return e.prototype.getLabel = function(r) {
      return za(r.value, Cd[Ix(oa(this._minLevelUnit))] || Cd.second, this._useUTC, this._locale);
    }, e.prototype.getFormattedLabel = function(r, n, i) {
      return Ax(r, n, i, this._locale, this._useUTC);
    }, e.prototype.getTicks = function(r) {
      r = r || {};
      var n = this._interval, i = Da(this), a = Xe(), o = this.brk, s = a && o, u = [];
      if (!n)
        return u;
      var l = this._useUTC;
      if (s && r.breakTicks === "only_break")
        return Xe().addBreaksToTicks(u, o.breaks, i), u;
      u = AA(this._minLevelUnit, this._approxInterval, l, i, _u(this), o);
      var f = it.length - 1, h = 0;
      return T(u, function(v) {
        v.time && (f = Math.min(f, oe(it, v.time.upperTimeUnit)), h = Math.max(h, v.time.level));
      }), s && Xe().pruneTicksByBreak(r.pruneByBreak, u, o.breaks, function(v) {
        return v.value;
      }, this._approxInterval, i), s && r.breakTicks !== "none" && Xe().addBreaksToTicks(u, o.breaks, i, function(v) {
        for (var c = Math.max(oe(it, ni(v.vmin, l)), oe(it, ni(v.vmax, l))), d = 0, y = 0; y < it.length; y++)
          if (!U0(it[y], v.vmin, v.vmax, l)) {
            d = y;
            break;
          }
        var p = Math.min(d, f), g = Math.max(p, c);
        return {
          level: h,
          lowerTimeUnit: it[g],
          upperTimeUnit: it[p]
        };
      }), u;
    }, e.prototype.getMinorTicks = function(r) {
      return Ic(this, r, ys(this), this._interval);
    }, e.prototype.setTimeInterval = function(r) {
      this._interval = r.interval, this._approxInterval = r.approxInterval, this._minLevelUnit = r.minLevelUnit;
    }, e.parse = function(r) {
      return ve(r) ? Math.round(r) : +pr(r);
    }, e.type = "time", e;
  })(Rt)
), xo = [
  // Format                           interval
  ["second", Zh],
  ["minute", Kh],
  ["hour", aa],
  ["quarter-day", aa * 6],
  ["half-day", aa * 12],
  ["day", St * 1.2],
  ["half-week", St * 3.5],
  ["week", St * 7],
  ["month", St * 31],
  ["quarter", St * 95],
  ["half-year", Td / 2],
  ["year", Td]
  // 1Y
];
function U0(t, e, r, n) {
  return ms(new Date(e), t, n).getTime() === ms(new Date(r), t, n).getTime();
}
function TA(t, e) {
  return t /= St, t > 16 ? 16 : t > 7.5 ? 7 : t > 3.5 ? 4 : t > 1.5 ? 2 : 1;
}
function CA(t) {
  var e = 30 * St;
  return t /= e, t > 6 ? 6 : t > 3 ? 3 : t > 2 ? 2 : 1;
}
function xA(t) {
  return t /= aa, t > 12 ? 12 : t > 6 ? 6 : t > 3.5 ? 4 : t > 2 ? 2 : 1;
}
function Op(t, e) {
  return t /= e ? Kh : Zh, t > 30 ? 30 : t > 20 ? 20 : t > 15 ? 15 : t > 10 ? 10 : t > 5 ? 5 : t > 2 ? 2 : 1;
}
function MA(t) {
  return ge(Th(t, !0), 1);
}
function DA(t, e, r) {
  var n = Math.max(0, oe(it, e) - 1);
  return ms(new Date(t), it[n], r).getTime();
}
function IA(t, e) {
  var r = /* @__PURE__ */ new Date(0);
  r[t](1);
  var n = r.getTime();
  r[t](1 + e);
  var i = r.getTime() - n;
  return function(a, o) {
    return Math.max(0, Math.round((o - a) / i));
  };
}
function AA(t, e, r, n, i, a) {
  var o = 3e3, s = Cx, u = 0;
  function l(O, V, K, J, ee, Q, re) {
    for (var ne = IA(ee, O), q = V, se = new Date(q); q < K && q <= n[1] && (re.push({
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
    if (!U0(oa(O), n[0], n[1], r)) {
      ee && (V = [{
        value: DA(n[0], O, r)
      }, {
        value: n[1]
      }]);
      for (var Q = 0; Q < V.length - 1; Q++) {
        var re = V[Q].value, ne = V[Q + 1].value;
        if (re !== ne) {
          var q = void 0, se = void 0, Se = void 0, tt = !1;
          switch (O) {
            case "year":
              q = Math.max(1, Math.round(e / St / 365)), se = l_(r), Se = Px(r);
              break;
            case "half-year":
            case "quarter":
            case "month":
              q = CA(e), se = jh(r), Se = f_(r);
              break;
            case "week":
            // PENDING If week is added. Ignore day.
            case "half-week":
            case "day":
              q = TA(e), se = Qh(r), Se = h_(r), tt = !0;
              break;
            case "half-day":
            case "quarter-day":
            case "hour":
              q = xA(e), se = Jh(r), Se = c_(r);
              break;
            case "minute":
              q = Op(e, !0), se = ec(r), Se = v_(r);
              break;
            case "second":
              q = Op(e, !1), se = tc(r), Se = d_(r);
              break;
            case "millisecond":
              q = MA(e), se = rc(r), Se = p_(r);
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
  for (var h = [], v = [], c = 0, d = 0, y = 0; y < s.length; ++y) {
    var p = oa(s[y]);
    if (Dx(s[y])) {
      f(s[y], h[h.length - 1] || [], v);
      var g = s[y + 1] ? oa(s[y + 1]) : null;
      if (p !== g) {
        if (v.length) {
          d = c, v.sort(function(O, V) {
            return O.value - V.value;
          });
          for (var m = [], _ = 0; _ < v.length; ++_) {
            var S = v[_].value;
            (_ === 0 || v[_ - 1].value !== S) && (m.push(v[_]), S >= n[0] && S <= n[1] && c++);
          }
          var b = i / e;
          if (c > b * 1.5 && d > b / 1.5 || (h.push(m), c > b || t === s[y]))
            break;
        }
        v = [];
      }
    }
  }
  for (var w = xe(H(h, function(O) {
    return xe(O, function(V) {
      return V.value >= n[0] && V.value <= n[1] && !V.notAdd;
    });
  }), function(O) {
    return O.length > 0;
  }), C = w.length - 1, M = [], y = 0; y < w.length; ++y)
    for (var x = w[y], D = 0; D < x.length; ++D) {
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
  Dh(M, Fw, null), M.sort(function(O, V) {
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
var PA = function(t, e) {
  var r = t.getExtent();
  if (r[0] === r[1] && (r[0] -= St, r[1] += St), r[1] === -1 / 0 && r[0] === 1 / 0) {
    var n = /* @__PURE__ */ new Date();
    r[1] = +new Date(n.getFullYear(), n.getMonth(), n.getDate()), r[0] = r[1] - St;
  }
  t.setExtent(r[0], r[1]);
  var i = Dc(e.splitNumber, 10), a = _u(t) / i, o = e.minInterval, s = e.maxInterval;
  o != null && a < o && (a = o), s != null && a > s && (a = s);
  var u = xo.length, l = Math.min(wA(xo, a, 0, u), u - 1), f = xo[l][1], h = xo[Math.max(l - 1, 0)][0];
  t.setTimeInterval({
    approxInterval: a,
    interval: f,
    minLevelUnit: h
  });
};
Rt.registerClass(G0);
var Mo = 0, Do = 1, LA = 2, Y0 = (
  /** @class */
  (function(t) {
    z(e, t);
    function e(r) {
      var n = t.call(this) || this;
      n.type = "log", n.parse = oi.parse, n.base = r.logBase || 10;
      var i = [], a = [], o = n._lookup = {
        from: i,
        to: a
      };
      i[Mo] = i[Do] = a[Mo] = a[Do] = NaN, N0(n, e.mapperMethods);
      var s = Xe(), u = r.breakOption, l = {
        lookup: o
      };
      return s && s.parseAxisBreakOptionInwardTransform(u, n, {
        noNegative: !0
      }, LA, l), n.powStub = new oi({
        breakParsed: l.original
      }), n.intervalStub = new oi({
        breakParsed: l.transformed
      }), F0(n, n.intervalStub), n;
    }
    return e.prototype.getTicks = function(r) {
      var n = this.base, i = this.powStub, a = Xe(), o = this.intervalStub, s = o.getExtent(), u = i.getExtent(), l = {
        lookup: {
          from: s,
          to: u
        }
      };
      return H(o.getTicks(r || {}), function(f) {
        var h = f.value, v = Rl(h, n, l), c;
        if (a) {
          var d = a.getTicksBreakOutwardTransform(this, f, ys(i), this._lookup);
          d && (c = d.vBreak, v = d.tickVal);
        }
        return {
          value: v,
          break: c
        };
      }, this);
    }, e.prototype.getMinorTicks = function(r) {
      return Ic(
        this,
        r,
        ys(this.powStub),
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
        return Rl(this.intervalStub.scale(r), this.base, null);
      },
      transformIn: function(r, n) {
        return r = Co(r, this.base), n && n.depth === Ps ? r : this.intervalStub.transformIn(r, n);
      },
      transformOut: function(r, n) {
        var i = n ? n.depth : null;
        return kp.depth = i, Bp.lookup = this._lookup, Rl(i === Ps ? r : this.intervalStub.transformOut(r, kp), this.base, Bp);
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
        if (!(!ci(n, i) || n <= 0 || i <= 0)) {
          var a = Np, o = Np;
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
        return ci(n[0], n[1]) && vr(r) && r <= 0 && (r = n[0]), r;
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
  })(Rt)
);
Rt.registerClass(Y0);
var kp = {}, Bp = {}, Np = [], EA = {
  value: 1,
  category: 1,
  time: 1,
  log: 1
}, W0 = de();
function RA(t) {
  var e = t.get("type");
  return (
    // In ec option, `xxxAxis.type` may be undefined.
    (e == null || !qe(EA, e) && !Rt.getClass(e)) && (e = "value"), e
  );
}
function OA(t, e, r) {
  var n = Xe(), i;
  switch (n && (i = HA(t, e, r)), e) {
    case "category":
      return new V0({
        ordinalMeta: t.getOrdinalMeta ? t.getOrdinalMeta() : t.getCategories(),
        extent: zt()
      });
    case "time":
      return new G0({
        locale: t.ecModel.getLocaleModel(),
        useUTC: t.ecModel.get("useUTC"),
        breakOption: i
      });
    case "log":
      return new Y0({
        logBase: t.get("logBase"),
        breakOption: i
      });
    case "value":
      return new oi({
        breakOption: i
      });
    default:
      return new (Rt.getClass(e) || oi)({});
  }
}
function QE(t, e, r) {
  var n = t.getExtentUnsafe(bt, null), i = n[0], a = n[1];
  return ci(i, a) ? i === e || a === e ? BA : i < e && a > e ? kA : Fp : Fp;
}
var kA = 1, BA = 2, Fp = 3;
function NA(t) {
  W0(t).noOnMyZero = !0;
}
function JE(t) {
  return W0(t).noOnMyZero;
}
function bu(t) {
  var e = t.getLabelModel().get("formatter");
  if (t.type === "time") {
    var r = xx(e);
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
            Es(t, i),
            i.value - t.scale.getExtent()[0],
            null
            // Using `null` just for backward compat.
          );
        };
      var n = Xe();
      return function(i, a) {
        var o = null;
        return n && (o = n.makeAxisLabelFormatterParamBreak(o, i.break)), e(Es(t, i), a, o);
      };
    } else
      return function(i) {
        return t.scale.getLabel(i);
      };
  }
}
function Es(t, e) {
  var r = t.scale;
  return yr(r) ? r.getLabel(e) : e.value;
}
function Ac(t) {
  var e = t.get("interval");
  return e ?? "auto";
}
function FA(t) {
  return t.type === "category" && Ac(t.getLabelModel()) === 0;
}
function zA(t, e) {
  var r = {};
  return T(t.mapDimensionsAll(e), function(n) {
    r[O0(t, n)] = !0;
  }), me(r);
}
function Rs(t) {
  return t === "middle" || t === "center";
}
function eR(t) {
  return t.getShallow("show");
}
function HA(t, e, r) {
  var n = t.get("breaks", !0);
  if (n != null)
    return !Xe() || !r || !VA(e) ? void 0 : n;
}
function VA(t) {
  return t !== "category";
}
function GA(t, e, r, n, i, a) {
  var o = Su(t), s = o ? t.intervalStub : t;
  if (s.setExtent(n[0], n[1]), o) {
    var u = t.powStub, l = {
      depth: Ps
    }, f = t.transformOut(n[0], l), h = t.transformOut(n[1], l), v = bA(r, n);
    e[0] && !v[0] && (f = i[0]), e[1] && !v[1] && (h = i[1]), u.setExtent(f, h);
  }
  s.setConfig(a);
}
function Pc(t, e) {
  return yr(t) ? t.getRawOrdinalNumber(e.value) : e.value;
}
function UA(t, e) {
  return yr(t) && !!e.get("boundaryGap");
}
var YA = (
  /** @class */
  (function() {
    function t() {
    }
    return t.prototype.needIncludeZero = function() {
      return !this.option.scale;
    }, t.prototype.getCoordSysModel = function() {
    }, t;
  })()
), WA = Bw(), zp = "|&", Si = de(), XA = -2, tR = -1, qA = de();
function Lc(t, e) {
  var r = t.model, n = Si(_i(r.ecModel)).keyed, i = n && n.get(e);
  return i && i.get(r.uid);
}
function $A(t, e) {
  return q0(Lc(t, e));
}
function ZA(t, e) {
  var r = [];
  return X0(t.model.ecModel, function(n) {
    for (var i = 0; i < e.length; i++)
      e[i] && n.serByIdx[e[i].seriesIndex] && r.push(q0(n));
  }), r;
}
function X0(t, e) {
  var r = Si(_i(t)).keyed;
  r && r.each(function(n, i) {
    n.each(function(a, o) {
      e(a, i, o);
    });
  });
}
function q0(t) {
  return {
    liPosMinGap: t ? t.liPosMinGap : void 0
  };
}
function KA(t, e) {
  var r = t.model.ecModel, n = Si(_i(r)).axSer;
  n && Ec(r, n.get(t.model.uid), e);
}
function rR(t, e, r) {
  var n = Lc(t, e);
  n && Ec(t.model.ecModel, n.sers, r);
}
function Ec(t, e, r) {
  if (e)
    for (var n = 0; n < e.length; n++) {
      var i = e[n];
      t.isSeriesFiltered(i) || r(i);
    }
}
function nR(t, e) {
  var r = Lc(t, e);
  if (!r || !r.sers.length)
    return 0;
  var n = 0;
  return Ec(t.model.ecModel, r.sers, function() {
    n++;
  }), n;
}
function iR(t, e, r) {
  var n = Si(_i(t)).keyed, i = n && n.get(e);
  i && i.each(function(a) {
    r(a.axis);
  });
}
function $0(t, e) {
  var r = t.model, n = Si(_i(r.ecModel)).keys;
  n && T(n.get(r.uid), function(i) {
    e(i);
  });
}
function jA(t) {
  var e = qA(HD(t)), r = e.keyed || (e.keyed = U());
  X0(t, function(n, i, a) {
    var o = r.get(i) || r.set(i, U()), s = o.get(a) || o.set(a, {});
    n.metrics.liPosMinGap && Z0.liPosMinGap(t, n, s);
  });
}
function aR(t, e) {
  Z0[t] = e;
}
var Z0 = {};
function oR(t, e, r) {
  if (t) {
    var n = e.ecModel, i = Si(_i(n)), a = t.model.uid, o = i.axSer || (i.axSer = U()), s = o.get(a) || o.set(a, []);
    s.push(e);
    var u = e.subType, l = e.getBaseAxis() === t, f = Jf.get(Qf(u, l, r)) || Jf.get(Qf(u, l, null));
    if (f) {
      var h = i.keyed || (i.keyed = U()), v = i.keys || (i.keys = U()), c = f.key, d = h.get(c) || h.set(c, U()), y = d.get(a);
      y || (y = d.set(a, {
        axis: t,
        sers: [],
        serByIdx: []
      }), y.metrics = f.getMetrics(t), (v.get(a) || v.set(a, [])).push(c)), y.sers.push(e), y.serByIdx[e.seriesIndex] = e;
    }
  }
}
function Qf(t, e, r) {
  return t + zp + G(e, !0) + zp + (r || "");
}
function sR(t, e) {
  var r = Qf(e.seriesType, e.baseAxis, e.coordSysType);
  Jf.set(r, e), WA(t, function() {
    t.registerProcessor(t.PRIORITY.PROCESSOR.AXIS_STATISTICS, {
      // NOTE: Theoretically, `appendData` requires `dirtyOnOverallProgress: true` here to re-calculate them.
      // But this OVERALL_STAGE_TASK is applied to all series (no `getTargetSeries` specified),
      // `dirtyOnOverallProgress: true` can cause irrelevant series (e.g., series on geo)
      // to be re-rendered when `appendData` is called, which cause `appendData` meaningless,
      // thereby not setting `dirtyOnOverallProgress: true`.
      overallReset: jA
    });
  });
}
var Jf = U(), QA = de(), uR = 1, JA = 3, K0 = (
  /** @class */
  (function() {
    function t(e, r, n, i, a) {
      var o = yr(e), s = o ? r.getCategories().length : null, u;
      if (o) {
        var l = r.getCategories(!0);
        u = l && !l.length;
      }
      var f = n.slice();
      (Ls(e) || Su(e) || Mc(e)) && (Qy(f, Ui(e, r.get("dataMin", !0))), Jy(f, Ui(e, r.get("dataMax", !0)))), Ow(f) || (f[0] = f[1] = NaN);
      var h = [], v = [!1, !1], c = r.get("min", !0);
      c === "dataMin" ? (h[0] = f[0], v[0] = !0) : (h[0] = Ui(e, X(c) ? c({
        min: f[0],
        max: f[1]
      }) : c), v[0] = h[0] != null);
      var d = r.get("max", !0);
      d === "dataMax" ? (h[1] = f[1], v[1] = !0) : (h[1] = Ui(e, X(d) ? d({
        min: f[0],
        max: f[1]
      }) : d), v[1] = h[1] != null);
      var y = eP(e, r), p = o ? null : f[1] - f[0] || Math.abs(f[0]);
      h[0] == null && (h[0] = o ? u ? f[0] : s ? 0 : NaN : f[0] - y[0] * p), h[1] == null && (h[1] = o ? u ? f[1] : s ? s - 1 : NaN : f[1] + y[1] * p), !dr(h[0]) && (h[0] = NaN), !dr(h[1]) && (h[1] = NaN);
      var g = u || ha(h[0]) || ha(h[1]) || o && !s, m = Ls(e), _ = m && r.needIncludeZero && r.needIncludeZero();
      _ && (h[0] > 0 && h[1] > 0 && !v[0] && (h[0] = 0), h[0] < 0 && h[1] < 0 && !v[1] && (h[1] = 0));
      var S = !1;
      h[0] > h[1] && (h.reverse(), S = !0);
      var b = Ui(e, r.get("startValue", !0)), w = b != null;
      !vr(b) && i && (b = e.getDefaultStartValue ? e.getDefaultStartValue() : 0), vr(b) && (w || !m || _) && (b < h[0] && !v[0] ? (h[0] = b, v[0] = !0) : b > h[1] && !v[1] && (h[1] = b, v[1] = !0));
      var C = this._i = {
        scale: e,
        dataMM: f,
        noZoomEffMM: h,
        zoomMM: [],
        fixMM: v,
        zoomFixMM: [!1, !1],
        startValue: b,
        isBlank: g,
        incl0: _,
        tggAxInv: S,
        ctnShp: a
      };
      Hp(C, h);
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
      return r[0] != null && (s[0] = r[0], a[0] = i[0] = !0), r[1] != null && (s[1] = r[1], a[1] = i[1] = !0), Hp(e, s), o;
    }, t.prototype.makeRenderInfo = function() {
      return {
        startValue: this._i.startValue
      };
    }, t.prototype.setZoomMM = function(e, r) {
      this._i.zoomMM[e] = r;
    }, t;
  })()
);
function Hp(t, e) {
  var r = t.scale, n = t.dataMM;
  r.sanitize && (e[0] = r.sanitize(e[0], n), e[1] = r.sanitize(e[1], n), kw(e));
}
function Ui(t, e) {
  return e == null ? null : ha(e) ? NaN : t.parse(e);
}
function eP(t, e) {
  var r;
  if (yr(t))
    r = [0, 0];
  else {
    var n = e.get("boundaryGap");
    typeof n == "boolean" && (n = null), r = B(n) ? n : [n, n];
  }
  return [Vp(r[0]), Vp(r[1])];
}
function Vp(t) {
  return hi(typeof t == "boolean" ? 0 : t, 1) || 0;
}
function j0(t) {
  var e = QA(t.scale);
  return e.extent || (e.extent = zt()), e;
}
function lR(t, e) {
  j0(t).dimIdxInCoord = e.get(t.dim);
}
function fR(t, e) {
  var r = t.scale, n = t.model, i = t.dim;
  r.rawExtentInfo || tP(r, t, i, n, e);
}
function tP(t, e, r, n, i) {
  var a = j0(e), o = a.extent, s = !1;
  KA(e, function(f) {
    if (f.boxCoordinateSystem) {
      var h = y_(f).coord, v = a.dimIdxInCoord;
      if (v >= 0) {
        if (B(h)) {
          var c = h[v];
          c != null && !B(c) && Ew(o, t.parse(c));
        }
      }
    } else if (f.coordinateSystem) {
      var d = f.getData();
      if (d) {
        var y = t.getFilter ? t.getFilter() : null;
        T(zA(d, r), function(p) {
          Rw(o, d.getApproximateExtent(p, y));
        });
      }
      f.__requireStartValue && f.__requireStartValue(e) && (s = !0);
    }
  });
  var u = iP(t, e, n), l = new K0(t, n, o, s, u);
  Q0(t, l, i), a.extent = null;
}
function rP(t, e) {
  var r = t.scale;
  Q0(r, new K0(r, t.model, e, !1, !1), JA);
}
function Q0(t, e, r) {
  t.rawExtentInfo = e, e.from = r;
}
function hR(t, e) {
  Rc.set(t, e);
}
var Rc = U();
function nP(t, e, r, n, i) {
  t.rawExtentInfo || rP({
    scale: t,
    model: e
  }, i || zt());
  var a = t.rawExtentInfo.makeFinal(), o = a.effMM;
  return t.setExtent(o[0], o[1]), t.setBlank(a.isBlank), n && a.tggAxInv && r && !r.get("legacyMinMaxDontInverseAxis") && (n.inverse = !n.inverse), a;
}
function iP(t, e, r) {
  var n = UA(t, r), i = r.get("containShape", !0);
  if (i == null && !n && (i = !0), !i)
    return !1;
  var a = !1;
  return $0(e, function(o) {
    a = !!Rc.get(o) || a;
  }), a;
}
function aP(t, e, r, n) {
  if (r.ctnShp) {
    var i;
    if ($0(t, function(s) {
      var u = Rc.get(s);
      if (u) {
        var l = u(t, n);
        l && (i = i || [0, 0], Qy(i, l[0]), Jy(i, l[1]), NA(t));
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
function Gp(t, e) {
  var r = Su(t), n = r ? t.intervalStub : t, i = e.fixMinMax || [], a = r ? t.getExtent() : null, o = n.getExtent(), s = SA(o, i, e.rawExtentResult);
  n.setExtent(s[0], s[1]), s = n.getExtent();
  var u = r ? sP(n, e) : oP(n, e), l = u.intervalPrecision, f = u.interval, h = e.userInterval;
  h != null && (u.interval = h, u.intervalPrecision = Va(h)), i[0] || (s[0] = Ie(Dn(s[0] / f) * f, l)), i[1] || (s[1] = Ie(Zs(s[1] / f) * f, l)), h != null && (u.niceExtent = s.slice()), GA(t, i, o, s, a, u);
}
function oP(t, e) {
  var r = Dc(e.splitNumber, 5), n = _u(t), i = e.minInterval, a = e.maxInterval, o = Th(n / r, !0);
  i != null && o < i && (o = i), a != null && o > a && (o = a);
  var s = Va(o), u = t.getExtent(), l = [Ie(Zs(u[0] / o) * o, s), Ie(Dn(u[1] / o) * o, s)];
  return {
    interval: o,
    intervalPrecision: s,
    niceExtent: l
  };
}
function sP(t, e) {
  var r = Dc(e.splitNumber, 10), n = t.getExtent(), i = _u(t), a = ge(Wy(i), 1), o = r / i * a;
  o <= 0.5 && (a *= 10);
  var s = Va(a), u = [Ie(Zs(n[0] / a) * a, s), Ie(Dn(n[1] / a) * a, s)];
  return {
    intervalPrecision: s,
    interval: a,
    niceExtent: u
  };
}
function cR(t) {
  var e = t.scale, r = t.model, n = r.axis, i = r.ecModel;
  J0(e, r, n, i, null);
}
function J0(t, e, r, n, i) {
  var a = nP(t, e, n, r, i), o = Ls(t) || Mc(t);
  uP(t, {
    splitNumber: e.get("splitNumber"),
    fixMinMax: a.fixMM,
    userInterval: e.get("interval"),
    minInterval: o ? e.get("minInterval") : null,
    maxInterval: o ? e.get("maxInterval") : null,
    rawExtentResult: a
  }), r && n && aP(r, t, a, n);
}
function uP(t, e) {
  lP[t.type](t, e);
}
var lP = {
  interval: Gp,
  log: Gp,
  time: PA,
  ordinal: Ee
};
function fP(t) {
  return fA(null, t);
}
var hP = {
  isDimensionStacked: R0,
  enableDataStack: E0,
  getStackedDimension: O0
};
function cP(t, e) {
  var r = e;
  e instanceof ce || (r = new ce(e));
  var n = RA(r), i = OA(r, n, !1);
  return t[1] < t[0] && (t = t.slice().reverse()), J0(i, r, null, null, t), i;
}
function vP(t) {
  Lt(t, YA);
}
function dP(t, e) {
  return e = e || {}, Cn(t, null, null, e.state !== "normal");
}
const pP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  createDimensions: tA,
  createList: fP,
  createScale: cP,
  createSymbol: gu,
  createTextStyle: dP,
  dataStack: hP,
  enableHoverEmphasis: ps,
  getECData: he,
  getLayoutRect: _s,
  mixinAxisModelCommonMethods: vP
}, Symbol.toStringTag, { value: "Module" }));
var Up = [], gP = {
  registerPreprocessor: Sc,
  registerProcessor: bc,
  registerPostInit: _0,
  registerPostUpdate: S0,
  registerUpdateLifecycle: mu,
  registerAction: Ln,
  registerCoordinateSystem: b0,
  registerLayout: w0,
  registerVisual: Fr,
  registerTransform: C0,
  registerLoading: Tc,
  registerMap: T0,
  registerImpl: ND,
  PRIORITY: f0,
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
    Gy(t, e);
  }
};
function di(t) {
  if (B(t)) {
    T(t, function(e) {
      di(e);
    });
    return;
  }
  oe(Up, t) >= 0 || (Up.push(t), X(t) && (t = {
    install: t
  }), t.install(gP));
}
var yP = 1e-8;
function Yp(t, e) {
  return Math.abs(t - e) < yP;
}
function Wp(t, e, r) {
  var n = 0, i = t[0];
  if (!i)
    return !1;
  for (var a = 1; a < t.length; a++) {
    var o = t[a];
    n += sr(i[0], i[1], o[0], o[1], e, r), i = o;
  }
  var s = t[0];
  return (!Yp(i[0], s[0]) || !Yp(i[1], s[1])) && (n += sr(i[0], i[1], s[0], s[1], e, r)), n !== 0;
}
var mP = [];
function Ol(t, e) {
  for (var r = 0; r < t.length; r++)
    ot(t[r], t[r], e);
}
function Xp(t, e, r, n) {
  for (var i = 0; i < t.length; i++) {
    var a = t[i];
    n && (a = n.project(a)), a && isFinite(a[0]) && isFinite(a[1]) && (Cr(e, e, a), xr(r, r, a));
  }
}
function _P(t) {
  for (var e = 0, r = 0, n = 0, i = t.length, a = t[i - 1][0], o = t[i - 1][1], s = 0; s < i; s++) {
    var u = t[s][0], l = t[s][1], f = a * l - u * o;
    e += f, r += (a + u) * f, n += (o + l) * f, a = u, o = l;
  }
  return e ? [r / e / 3, n / e / 3, e] : [t[0][0] || 0, t[0][1] || 0];
}
var eS = (
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
), qp = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e, r) {
      this.type = "polygon", this.exterior = e, this.interiors = r;
    }
    return t;
  })()
), $p = (
  /** @class */
  /* @__PURE__ */ (function() {
    function t(e) {
      this.type = "linestring", this.points = e;
    }
    return t;
  })()
), SP = (
  /** @class */
  (function(t) {
    z(e, t);
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
        return _P(n.exterior);
      var l = this.getBoundingRect();
      return [l.x + l.width / 2, l.y + l.height / 2];
    }, e.prototype.getBoundingRect = function(r) {
      var n = this._rect;
      if (n && !r)
        return n;
      var i = [1 / 0, 1 / 0], a = [-1 / 0, -1 / 0], o = this.geometries;
      return T(o, function(s) {
        s.type === "polygon" ? Xp(s.exterior, i, a, r) : T(s.points, function(u) {
          Xp(u, i, a, r);
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
          if (Wp(u, r[0], r[1])) {
            for (var f = 0; f < (l ? l.length : 0); f++)
              if (Wp(l[f], r[0], r[1]))
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
        var v = f[h];
        v.type === "polygon" ? (Ol(v.exterior, l), T(v.interiors, function(c) {
          Ol(c, l);
        })) : T(v.points, function(c) {
          Ol(c, l);
        });
      }
      o = this._rect, o.copy(u), this._center = [o.x + o.width / 2, o.y + o.height / 2];
    }, e.prototype.cloneShallow = function(r) {
      r == null && (r = this.name);
      var n = new e(r, this.geometries, this._center);
      return n._rect = this._rect, n.transformTo = null, n;
    }, e;
  })(eS)
);
(function(t) {
  z(e, t);
  function e(r, n) {
    var i = t.call(this, r) || this;
    return i.type = "geoSVG", i._elOnlyForCalculate = n, i;
  }
  return e.prototype.calcCenter = function() {
    for (var r = this._elOnlyForCalculate, n = r.getBoundingRect(), i = [n.x + n.width / 2, n.y + n.height / 2], a = Mn(mP), o = r; o && !o.isGeoSVGGraphicRoot; )
      mn(a, o.getLocalTransform(), a), o = o.parent;
    return yi(a, a), ot(i, i, a), i;
  }, e;
})(eS);
function bP(t) {
  if (!t.UTF8Encoding)
    return t;
  var e = t, r = e.UTF8Scale;
  r == null && (r = 1024);
  var n = e.features;
  return T(n, function(i) {
    var a = i.geometry, o = a.encodeOffsets, s = a.coordinates;
    if (o)
      switch (a.type) {
        case "LineString":
          a.coordinates = tS(s, o, r);
          break;
        case "Polygon":
          kl(s, o, r);
          break;
        case "MultiLineString":
          kl(s, o, r);
          break;
        case "MultiPolygon":
          T(s, function(u, l) {
            return kl(u, o[l], r);
          });
      }
  }), e.UTF8Encoding = !1, e;
}
function kl(t, e, r) {
  for (var n = 0; n < t.length; n++)
    t[n] = tS(t[n], e[n], r);
}
function tS(t, e, r) {
  for (var n = [], i = e[0], a = e[1], o = 0; o < t.length; o += 2) {
    var s = t.charCodeAt(o) - 64, u = t.charCodeAt(o + 1) - 64;
    s = s >> 1 ^ -(s & 1), u = u >> 1 ^ -(u & 1), s += i, u += a, i = s, a = u, n.push([s / r, u / r]);
  }
  return n;
}
function Zp(t, e) {
  return t = bP(t), H(xe(t.features, function(r) {
    return r.geometry && r.properties && r.geometry.coordinates.length > 0;
  }), function(r) {
    var n = r.properties, i = r.geometry, a = [];
    switch (i.type) {
      case "Polygon":
        var o = i.coordinates;
        a.push(new qp(o[0], o.slice(1)));
        break;
      case "MultiPolygon":
        T(i.coordinates, function(u) {
          u[0] && a.push(new qp(u[0], u.slice(1)));
        });
        break;
      case "LineString":
        a.push(new $p([i.coordinates]));
        break;
      case "MultiLineString":
        a.push(new $p(i.coordinates));
    }
    var s = new SP(n[e || "name"], a, n.cp);
    return s.properties = n, s;
  });
}
const wP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  MAX_SAFE_INTEGER: uw,
  asc: bh,
  getPercentWithPrecision: aw,
  getPixelPrecision: iw,
  getPrecision: lr,
  getPrecisionSafe: Yy,
  isNumeric: Xy,
  isRadianAroundZero: ya,
  linearMap: mf,
  nice: Th,
  numericToNumber: cr,
  parseDate: pr,
  parsePercent: Dt,
  quantile: hw,
  quantity: Wy,
  quantityExponent: Ks,
  reformIntervals: cw,
  remRadian: wh,
  round: nw
}, Symbol.toStringTag, { value: "Module" })), TP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  format: za,
  parse: pr,
  roundTime: ms
}, Symbol.toStringTag, { value: "Module" })), CP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: Fa,
  BezierCurve: su,
  BoundingRect: Y,
  Circle: Na,
  CompoundPath: Em,
  Ellipse: ru,
  Group: ut,
  Image: Nr,
  IncrementalDisplayable: Fm,
  Line: Pn,
  LinearGradient: Om,
  Polygon: au,
  Polyline: ou,
  RadialGradient: km,
  Rect: Re,
  Ring: iu,
  Sector: nu,
  Text: je,
  clipPointsByRect: jm,
  clipRectByRect: Qm,
  createIcon: lu,
  extendPath: Xm,
  extendShape: Wm,
  getShapeClass: qm,
  getTransform: Km,
  initProps: Fh,
  makeImage: zh,
  makePath: uu,
  mergePath: Zm,
  registerShape: Tt,
  resizePath: Hh,
  updateProps: mi
}, Symbol.toStringTag, { value: "Module" })), xP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  addCommas: nc,
  capitalFirst: Rx,
  encodeHTML: Ue,
  formatTime: Ex,
  formatTpl: ac,
  getTextRect: Lx,
  getTooltipMarker: g_,
  normalizeCssArray: Ha,
  toCamelCase: ic,
  truncateText: Jw
}, Symbol.toStringTag, { value: "Module" })), MP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  bind: le,
  clone: Z,
  curry: Te,
  defaults: _e,
  each: T,
  extend: R,
  filter: xe,
  indexOf: oe,
  inherits: ph,
  isArray: B,
  isFunction: X,
  isObject: F,
  isString: N,
  map: H,
  merge: pe,
  reduce: Xt
}, Symbol.toStringTag, { value: "Module" }));
var DP = de(), la = de(), kr = {
  estimate: 1,
  determine: 2
};
function Os(t) {
  return {
    out: {
      noPxChangeTryDetermine: []
    },
    kind: t
  };
}
function IP(t, e) {
  var r = t.getLabelModel().get("customValues");
  if (r) {
    var n = t.scale;
    return {
      labels: H(rS(r, n), function(i, a) {
        return {
          formattedLabel: bu(t)(i, a),
          rawLabel: n.getLabel(i),
          tick: i
        };
      })
    };
  }
  return t.type === "category" ? PP(t, e) : EP(t);
}
function AP(t, e, r) {
  var n = t.scale, i = t.getTickModel().get("customValues");
  return i ? {
    ticks: rS(i, n)
  } : t.type === "category" ? LP(t, e) : {
    ticks: n.getTicks(r)
  };
}
function rS(t, e) {
  var r = e.getExtent(), n = [];
  return T(t, function(i) {
    i = e.parse(i), i >= r[0] && i <= r[1] && n.push(i);
  }), Dh(n, zw, null), bh(n), H(n, function(i) {
    return {
      value: i
    };
  });
}
function PP(t, e) {
  var r = t.getLabelModel(), n = nS(t, r, e);
  return !r.get("show") || t.scale.isBlank() ? {
    labels: []
  } : n;
}
function nS(t, e, r) {
  var n = OP(t), i = Ac(e), a = r.kind === kr.estimate;
  if (!a) {
    var o = aS(n, i);
    if (o)
      return o;
  }
  var s, u;
  X(i) ? s = ks(t, i, !1) : (u = i === "auto" ? kP(t, r) : i, s = ks(t, u, !1));
  var l = {
    labels: s,
    labelCategoryInterval: u
  };
  return a ? r.out.noPxChangeTryDetermine.push(function() {
    return eh(n, i, l), !0;
  }) : eh(n, i, l), l;
}
function LP(t, e) {
  var r = RP(t), n = Ac(e), i = aS(r, n);
  if (i)
    return i;
  var a, o;
  if ((!e.get("show") || t.scale.isBlank()) && (a = []), X(n))
    a = ks(t, n, !0);
  else if (n === "auto") {
    var s = nS(t, t.getLabelModel(), Os(kr.determine));
    o = s.labelCategoryInterval, a = H(s.labels, function(u) {
      return u.tick;
    });
  } else
    o = n, a = ks(t, o, !0);
  return eh(r, n, {
    ticks: a,
    tickCategoryInterval: o
  });
}
function EP(t) {
  var e = t.scale.getTicks(), r = bu(t);
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
var RP = iS("axisTick"), OP = iS("axisLabel");
function iS(t) {
  return function(r) {
    return la(r)[t] || (la(r)[t] = {
      list: []
    });
  };
}
function aS(t, e) {
  for (var r = 0; r < t.list.length; r++)
    if (t.list[r].key === e)
      return t.list[r].value;
}
function eh(t, e, r) {
  return t.list.push({
    key: e,
    value: r
  }), r;
}
function kP(t, e) {
  if (e.kind === kr.estimate) {
    var r = t.calculateCategoryInterval(e);
    return e.out.noPxChangeTryDetermine.push(function() {
      return la(t).autoInterval = r, !0;
    }), r;
  }
  var n = la(t).autoInterval;
  return n ?? (la(t).autoInterval = t.calculateCategoryInterval(e));
}
function BP(t, e) {
  var r = e.kind, n = FP(t), i = bu(t), a = (n.axisRotate - n.labelRotate) / 180 * Math.PI, o = t.scale, s = o.getExtent(), u = o.count();
  if (s[1] - s[0] < 1)
    return 0;
  var l = 1, f = 40;
  u > f && (l = Math.max(1, Math.floor(u / f)));
  for (var h = s[0], v = t.dataToCoord(h + 1) - t.dataToCoord(h), c = Math.abs(v * Math.cos(a)), d = Math.abs(v * Math.sin(a)), y = 0, p = 0; h <= s[1]; h += l) {
    var g = 0, m = 0, _ = zy(i({
      value: h
    }), n.font, "center", "top");
    g = _.width * 1.3, m = _.height * 1.3, y = Math.max(y, g, 7), p = Math.max(p, m, 7);
  }
  var S = y / c, b = p / d;
  isNaN(S) && (S = 1 / 0), isNaN(b) && (b = 1 / 0);
  var w = Math.max(0, Math.floor(Math.min(S, b)));
  if (r === kr.estimate)
    return e.out.noPxChangeTryDetermine.push(le(NP, null, t, w, u)), w;
  var C = oS(t, w, u);
  return C ?? w;
}
function NP(t, e, r) {
  return oS(t, e, r) == null;
}
function oS(t, e, r) {
  var n = DP(t.model), i = t.getExtent(), a = n.lastAutoInterval, o = n.lastTickCount;
  if (a != null && o != null && Math.abs(a - e) <= 1 && Math.abs(o - r) <= 1 && a > e && n.axisExtent0 === i[0] && n.axisExtent1 === i[1])
    return a;
  n.lastTickCount = r, n.lastAutoInterval = e, n.axisExtent0 = i[0], n.axisExtent1 = i[1];
}
function FP(t) {
  var e = t.getLabelModel();
  return {
    axisRotate: t.getRotate ? t.getRotate() : t.isHorizontal && !t.isHorizontal() ? 90 : 0,
    labelRotate: e.get("rotate") || 0,
    font: e.getFont()
  };
}
function ks(t, e, r) {
  var n = bu(t), i = t.scale, a = [], o = X(e);
  return H0(i, o ? 0 : e, function(s, u) {
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
var zP = 0.8;
function wu(t, e) {
  e = e || {};
  var r = {
    w: NaN,
    w2: NaN
  }, n = t.scale, i = e.fromStat, a = e.min, o = gA(n);
  vr(o) || (o = NaN);
  var s = t.getExtent(), u = Fe(s[1] - s[0]);
  return yr(n) ? HP(r, t, o, u) : i && VP(r, t, o, u, i), a != null && (r.w = vr(r.w) ? ge(a, r.w) : a), r;
}
function HP(t, e, r, n) {
  var i = e.onBand, a = r + (i ? 1 : 0);
  a === 0 && (a = 1), t.w = n / a, !i && r && n && (t.w2 = t.w * r / n);
}
function VP(t, e, r, n, i) {
  var a = !1, o = -1 / 0;
  T(i.key ? [$A(e, i.key)] : ZA(e, i.sers || []), function(s) {
    var u = s.liPosMinGap;
    u != null && (u > 0 ? (u > o && (o = u), a = !1) : u === XA && (a = !0));
  }), vr(r) && r > 0 && vr(o) ? (t.w = n / r * o, t.w2 = o) : a && (t.w = n * zP, t.w2 = t.w * r / n);
}
var Kp = [0, 1], GP = (
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
      return e = n.normalize(n.parse(e)), mf(e, Kp, jp(this), r);
    }, t.prototype.coordToData = function(e, r) {
      var n = mf(e, jp(this), Kp, r);
      return this.scale.scale(n);
    }, t.prototype.pointToData = function(e, r) {
    }, t.prototype.getTicksCoords = function(e) {
      e = e || {};
      var r = e.tickModel || this.getTickModel(), n = AP(this, r, {
        breakTicks: e.breakTicks,
        pruneByBreak: e.pruneByBreak
      }), i = H(n.ticks, function(s) {
        return {
          coord: this.dataToCoord(Pc(this.scale, s)),
          tick: s
        };
      }, this), a = r.get("alignWithLabel"), o = UP(this, i, a);
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
      return e = e || Os(kr.determine), IP(this, e).labels;
    }, t.prototype.getLabelModel = function() {
      return this.model.getModel("axisLabel");
    }, t.prototype.getTickModel = function() {
      return this.model.getModel("axisTick");
    }, t.prototype.getBandWidth = function() {
      return wu(this, {
        min: 1
      }).w;
    }, t.prototype.calculateCategoryInterval = function(e) {
      return e = e || Os(kr.determine), BP(this, e);
    }, t;
  })()
);
function jp(t) {
  var e = t.getExtent();
  if (t.onBand) {
    var r = e[1] - e[0], n = r / t.scale.count() / 2;
    e[0] += n, e[1] -= n;
  }
  return e;
}
function UP(t, e, r) {
  var n = e.length;
  if (!t.onBand || r || !n)
    return !1;
  var i = wu(t).w;
  if (!i)
    return !1;
  T(e, function(s) {
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
function YP(t) {
  var e = ae.extend(t);
  return ae.registerClass(e), e;
}
function WP(t) {
  var e = lt.extend(t);
  return lt.registerClass(e), e;
}
function XP(t) {
  var e = $t.extend(t);
  return $t.registerClass(e), e;
}
function qP(t) {
  var e = Wt.extend(t);
  return Wt.registerClass(e), e;
}
var Qp = ["label", "labelLine", "layoutOption", "priority", "defaultAttr", "marginForce", "minMarginForce", "marginDefault", "suggestIgnore"], $P = 1, Bs = 2, sS = $P | Bs;
function Ns(t, e, r) {
  r = r || sS, e ? t.dirty |= r : t.dirty &= ~r;
}
function uS(t, e) {
  return e = e || sS, t.dirty == null || !!(t.dirty & e);
}
function Zt(t) {
  if (t)
    return uS(t) && ZP(t, t.label, t), t;
}
function ZP(t, e, r) {
  var n = e.getComputedTransform();
  t.transform = Yh(t.transform, n);
  var i = t.localRect = Sa(t.localRect, e.getBoundingRect()), a = e.style, o = a.margin, s = r && r.marginForce, u = r && r.minMarginForce, l = r && r.marginDefault, f = a.__marginType;
  f == null && l && (o = l, f = Jn.textMargin);
  for (var h = 0; h < 4; h++)
    Bl[h] = f === Jn.minMargin && u && u[h] != null ? u[h] : s && s[h] != null ? s[h] : o ? o[h] : 0;
  f === Jn.textMargin && Ef(i, Bl, !1, !1);
  var v = t.rect = Sa(t.rect, i);
  return n && v.applyTransform(n), f === Jn.minMargin && Ef(v, Bl, !1, !1), t.axisAligned = Uh(n), (t.label = t.label || {}).ignore = e.ignore, Ns(t, !1), Ns(t, !0, Bs), t;
}
var Bl = [0, 0, 0, 0];
function KP(t, e, r) {
  return t.transform = Yh(t.transform, r), t.localRect = Sa(t.localRect, e), t.rect = Sa(t.rect, e), r && t.rect.applyTransform(r), t.axisAligned = Uh(r), t.obb = void 0, (t.label = t.label || {}).ignore = !1, t;
}
function jP(t, e) {
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
function Jp(t, e) {
  for (var r = 0; r < Qp.length; r++) {
    var n = Qp[r];
    t[n] == null && (t[n] = e[n]);
  }
  return Zt(t);
}
function eg(t) {
  var e = t.obb;
  return (!e || uS(t, Bs)) && (t.obb = e = e || new Bm(), e.fromBoundingRect(t.localRect, t.transform), Ns(t, !1, Bs)), e;
}
function vR(t, e, r, n, i) {
  var a = t.length, o = Hm[e], s = Vm[e];
  if (a < 2)
    return !1;
  t.sort(function(C, M) {
    return C.rect[o] - M.rect[o];
  });
  for (var u = 0, l, f = !1, h = 0; h < a; h++) {
    var v = t[h], c = v.rect;
    l = c[o] - u, l < 0 && (c[o] -= l, v.label[o] -= l, f = !0), u = c[o] + c[s];
  }
  var d = t[0], y = t[a - 1], p, g;
  m(), p < 0 && b(-p, 0.8), g < 0 && b(g, 0.8), m(), _(p, g, 1), _(g, p, -1), m(), p < 0 && w(-p), g < 0 && w(g);
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
  function w(C) {
    var M = C < 0 ? -1 : 1;
    C = Math.abs(C);
    for (var x = Math.ceil(C / (a - 1)), D = 0; D < a - 1; D++)
      if (M > 0 ? S(x, 0, D + 1) : S(-x, a - D - 1, a), C -= x, C <= 0)
        return;
  }
  return f;
}
function dR(t) {
  for (var e = 0; e < t.length; e++) {
    var r = t[e], n = r.defaultAttr, i = r.labelLine;
    r.label.attr("ignore", n.ignore), i && i.attr("ignore", n.labelGuideIgnore);
  }
}
function QP(t) {
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
        if (Oc(i, e[u], null, {
          touchThreshold: 0.05
        })) {
          s = !0;
          break;
        }
      s ? (r(a), o && r(o)) : e.push(i);
    }
  }
}
function Oc(t, e, r, n) {
  return !t || !e || t.label && t.label.ignore || e.label && e.label.ignore || !t.rect.intersect(e.rect, r, n) ? !1 : t.axisAligned && e.axisAligned ? !0 : eg(t).intersect(eg(e), r, n);
}
const pR = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Axis: GP,
  ChartView: Wt,
  ComponentModel: ae,
  ComponentView: lt,
  List: P0,
  Model: ce,
  PRIORITY: f0,
  SeriesModel: $t,
  color: yb,
  connect: kI,
  dataTool: UI,
  dependencies: yI,
  disConnect: BI,
  disconnect: m0,
  dispose: NI,
  env: W,
  extendChartView: qP,
  extendComponentModel: YP,
  extendComponentView: WP,
  extendSeriesModel: XP,
  format: xP,
  getCoordinateSystemDimensions: zI,
  getInstanceByDom: mc,
  getInstanceById: FI,
  getMap: GI,
  graphic: CP,
  helper: pP,
  init: OI,
  innerDrawElementOnCanvas: dc,
  matrix: G1,
  number: wP,
  parseGeoJSON: Zp,
  parseGeoJson: Zp,
  registerAction: Ln,
  registerCoordinateSystem: b0,
  registerCustomSeries: HI,
  registerLayout: w0,
  registerLoading: Tc,
  registerLocale: $h,
  registerMap: T0,
  registerPostInit: _0,
  registerPostUpdate: S0,
  registerPreprocessor: Sc,
  registerProcessor: bc,
  registerTheme: _c,
  registerTransform: C0,
  registerUpdateLifecycle: mu,
  registerVisual: Fr,
  setCanvasCreator: VI,
  setPlatformAPI: ry,
  throttle: vc,
  time: TP,
  use: di,
  util: MP,
  vector: x1,
  version: gI,
  zrUtil: p1,
  zrender: Kb
}, Symbol.toStringTag, { value: "Module" }));
var JP = null;
function lS() {
  return JP;
}
var eL = "expandAxisBreak", Ir = Math.PI, tL = [[1, 2, 1, 2], [5, 3, 5, 3], [8, 3, 8, 3]], rL = [[0, 1, 0, 1], [0, 3, 0, 3], [0, 3, 0, 3]], pi = de(), fS = de(), nL = (
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
function iL(t, e, r, n) {
  var i = r.axis, a = e.ensureRecord(r), o = [], s, u = kc(t.axisName) && Rs(t.nameLocation);
  T(n, function(d) {
    var y = Zt(d);
    if (!(!y || y.label.ignore)) {
      o.push(y);
      var p = a.transGroup;
      u && (p.transform ? yi(Yi, p.transform) : Mn(Yi), y.transform && mn(Yi, Yi, y.transform), Y.copy(Io, y.localRect), Io.applyTransform(Yi), s ? s.union(Io) : Y.copy(s = new Y(0, 0, 0, 0), Io));
    }
  });
  var l = Math.abs(a.dirVec.x) > 0.1 ? "x" : "y", f = a.transGroup[l];
  if (o.sort(function(d, y) {
    return Math.abs(d.label[l] - f) - Math.abs(y.label[l] - f);
  }), u && s) {
    var h = i.getExtent(), v = Math.min(h[0], h[1]), c = Math.max(h[0], h[1]) - v;
    s.union(new Y(v, 0, c, 1));
  }
  a.stOccupiedRect = s, a.labelInfoList = o;
}
var Yi = wt(), Io = new Y(0, 0, 0, 0), aL = function(t, e, r, n, i, a) {
  if (Rs(t.nameLocation)) {
    var o = a.stOccupiedRect;
    o && hS(KP({}, o, a.transGroup.transform), n, i);
  } else
    oL(a.labelInfoList, a.dirVec, n, i);
};
function hS(t, e, r) {
  var n = new ue();
  Oc(t, e, n, {
    direction: Math.atan2(r.y, r.x),
    bidirectional: !1,
    touchThreshold: 0.05
  }) && jP(e, n);
}
function oL(t, e, r, n) {
  for (var i = ue.dot(n, e) >= 0, a = 0, o = t.length; a < o; a++) {
    var s = t[i ? a : o - 1 - a];
    s.label.ignore || hS(s, r, n);
  }
}
var Er = (
  /** @class */
  (function() {
    function t(e, r, n, i) {
      this.group = new ut(), this._axisModel = e, this._api = r, this._local = {}, this._shared = i || new nL(aL), this._resetCfgDetermined(n);
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
        shouldNameMoveOverlap: kc(i) && a,
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
      }), T(sL, function(i) {
        e[i] && uL[i](n._cfg, n._local, n._shared, n._axisModel, n.group, n._transformGroup, n._api, r || {});
      }), this;
    }, t.innerTextLayout = function(e, r, n) {
      var i = wh(r - e), a, o;
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
), sL = ["axisLine", "axisTickLabelEstimate", "axisTickLabelDetermine", "axisName"], uL = {
  axisLine: function(t, e, r, n, i, a, o) {
    var s = n.get(["axisLine", "show"]);
    if (s === "auto" && (s = !0, t.raw.axisLineAutoShow != null && (s = !!t.raw.axisLineAutoShow)), !!s) {
      var u = n.axis.getExtent(), l = a.transform, f = [u[0], 0], h = [u[1], 0], v = f[0] > h[0];
      l && (ot(f, f, l), ot(h, h, l));
      var c = R({
        lineCap: "round"
      }, n.getModel(["axisLine", "lineStyle"]).getLineStyle()), d = {
        strokeContainThreshold: t.raw.strokeContainThreshold || 5,
        silent: !0,
        z2: 1,
        style: c
      };
      if (n.get(["axisLine", "breakLine"]) && Sx(n.axis.scale))
        lS().buildAxisBreakLine(n, i, a, d);
      else {
        var y = new Pn(R({
          shape: {
            x1: f[0],
            y1: f[1],
            x2: h[0],
            y2: h[1]
          }
        }, d));
        Vh(y.shape, y.style.lineWidth), y.anid = "line", i.add(y);
      }
      var p = n.get(["axisLine", "symbol"]);
      if (p != null) {
        var g = n.get(["axisLine", "symbolSize"]);
        N(p) && (p = [p, p]), (N(g) || ve(g)) && (g = [g, g]);
        var m = QD(n.get(["axisLine", "symbolOffset"]) || 0, g), _ = g[0], S = g[1];
        T([{
          rotate: t.rotation + Math.PI / 2,
          offset: m[0],
          r: 0
        }, {
          rotate: t.rotation - Math.PI / 2,
          offset: m[1],
          r: Math.sqrt((f[0] - h[0]) * (f[0] - h[0]) + (f[1] - h[1]) * (f[1] - h[1]))
        }], function(b, w) {
          if (p[w] !== "none" && p[w] != null) {
            var C = gu(p[w], -_ / 2, -S / 2, _, S, c.stroke, !0), M = b.r + b.offset, x = v ? h : f;
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
    var u = rg(e, i, s);
    u && tg(t, e, r, n, i, a, o, kr.estimate);
  },
  /**
   * Finish axis tick label build.
   * Can be only called once.
   */
  axisTickLabelDetermine: function(t, e, r, n, i, a, o, s) {
    var u = rg(e, i, s);
    u && tg(t, e, r, n, i, a, o, kr.determine);
    var l = cL(t, i, a, n);
    hL(t, e.labelLayoutList, l), vL(t, i, a, n, t.tickDirection);
  },
  /**
   * [CAUTION] This method can be called multiple times, following the change due to `resetCfg` called
   *  in size measurement. Thus this method should be idempotent, and should be performant.
   */
  axisName: function(t, e, r, n, i, a, o, s) {
    var u = r.ensureRecord(n);
    e.nameEl && (i.remove(e.nameEl), e.nameEl = u.nameLayout = u.nameLocation = null);
    var l = t.axisName;
    if (kc(l)) {
      var f = t.nameLocation, h = t.nameDirection, v = n.getModel("nameTextStyle"), c = n.get("nameGap") || 0, d = n.axis.getExtent(), y = n.axis.inverse ? -1 : 1, p = new ue(0, 0), g = new ue(0, 0);
      f === "start" ? (p.x = d[0] - y * c, g.x = -y) : f === "end" ? (p.x = d[1] + y * c, g.x = y) : (p.x = (d[0] + d[1]) / 2, p.y = t.labelOffset + h * c, g.y = h);
      var m = wt();
      g.transform(Ys(m, m, t.rotation));
      var _ = n.get("nameRotate");
      _ != null && (_ = _ * Ir / 180);
      var S, b;
      Rs(f) ? S = Er.innerTextLayout(
        t.rotation,
        _ ?? t.rotation,
        // Adapt to axis.
        h
      ) : (S = lL(t.rotation, f, _ || 0, d), b = t.raw.axisNameAvailableWidth, b != null && (b = Math.abs(b / Math.sin(S.rotation)), !isFinite(b) && (b = null)));
      var w = v.getFont(), C = n.get("nameTruncate", !0) || {}, M = C.ellipsis, x = ca(t.raw.nameTruncateMaxWidth, C.maxWidth, b), D = s.nameMarginLevel || 0, I = new je({
        x: p.x,
        y: p.y,
        rotation: S.rotation,
        silent: Er.isLabelSilent(n),
        style: Cn(v, {
          text: l,
          font: w,
          overflow: "truncate",
          width: x,
          ellipsis: M,
          fill: v.getTextColor() || n.get(["axisLine", "lineStyle", "color"]),
          align: v.get("align") || S.textAlign,
          verticalAlign: v.get("verticalAlign") || S.textVerticalAlign
        }),
        z2: 1
      });
      if (fu({
        el: I,
        componentModel: n,
        itemName: l
      }), I.__fullText = l, I.anid = "name", n.get("triggerEvent")) {
        var L = Er.makeAxisEventDataBase(n);
        L.targetType = "axisName", L.name = l, he(I).eventData = L;
      }
      a.add(I), I.updateTransform(), e.nameEl = I;
      var A = u.nameLayout = Zt({
        label: I,
        priority: I.z2,
        defaultAttr: {
          ignore: I.ignore
        },
        marginDefault: Rs(f) ? tL[D] : rL[D]
      });
      if (u.nameLocation = f, i.add(I), I.decomposeTransform(), t.shouldNameMoveOverlap && A) {
        var P = r.ensureRecord(n);
        r.resolveAxisNameOverlap(t, r, n, A, g, P);
      }
    }
  }
};
function tg(t, e, r, n, i, a, o, s) {
  vS(e) || dL(t, e, i, s, n, o);
  var u = e.labelLayoutList;
  pL(t, n, u, a), mL(n, t.rotation, u);
  var l = t.optionHideOverlap;
  fL(n, u, l), l && QP(
    // Filter the already ignored labels by the previous overlap resolving methods.
    xe(u, function(f) {
      return f && !f.label.ignore;
    })
  ), iL(t, r, n, u);
}
function lL(t, e, r, n) {
  var i = wh(r - t), a, o, s = n[0] > n[1], u = e === "start" && !s || e !== "start" && s;
  return ya(i - Ir / 2) ? (o = u ? "bottom" : "top", a = "center") : ya(i - Ir * 1.5) ? (o = u ? "top" : "bottom", a = "center") : (o = "middle", i < Ir * 1.5 && i > Ir / 2 ? a = u ? "left" : "right" : a = u ? "right" : "left"), {
    rotation: i,
    textAlign: a,
    textVerticalAlign: o
  };
}
function fL(t, e, r) {
  var n = t.axis, i = t.get(["axisLabel", "customValues"]);
  if (FA(n))
    return;
  function a(l, f, h) {
    var v = Zt(e[f]), c = Zt(e[h]), d = n.scale;
    if (!(!v || !c)) {
      if (l == null) {
        if (!r && i)
          return;
        var y = pi(v.label).labelInfo.tick;
        if (
          // TimeScale does not expand extent to "nice", so eliminate labels that are not nice.
          Mc(d) && y.notNice || yr(d) && y.offInterval
        ) {
          Zn(v.label);
          return;
        }
      }
      if (l === !1 || v.suggestIgnore) {
        Zn(v.label);
        return;
      }
      if (c.suggestIgnore) {
        Zn(c.label);
        return;
      }
      var p = 0.1;
      if (!r) {
        var g = [0, 0, 0, 0];
        v = Jp({
          marginForce: g
        }, v), c = Jp({
          marginForce: g
        }, c);
      }
      Oc(v, c, null, {
        touchThreshold: p
      }) && Zn(l ? c.label : v.label);
    }
  }
  var o = t.get(["axisLabel", "showMinLabel"]), s = t.get(["axisLabel", "showMaxLabel"]), u = e.length;
  a(o, 0, 1), a(s, u - 1, u - 2);
}
function hL(t, e, r) {
  t.showMinorTicks || T(e, function(n) {
    if (n && n.label.ignore)
      for (var i = 0; i < r.length; i++) {
        var a = r[i], o = fS(a), s = pi(n.label);
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
function cS(t, e, r, n, i) {
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
    Vh(f.shape, f.style.lineWidth), f.anid = i + "_" + t[u].tickValue, a.push(f);
    var h = fS(f);
    h.onBand = !!t[u].onBand, h.tickValue = t[u].tickValue;
  }
  return a;
}
function cL(t, e, r, n) {
  var i = n.axis, a = n.getModel("axisTick"), o = a.get("show");
  if (o === "auto" && (o = !0, t.raw.axisTickAutoShow != null && (o = !!t.raw.axisTickAutoShow)), !o || i.scale.isBlank())
    return [];
  for (var s = a.getModel("lineStyle"), u = t.tickDirection * a.get("length"), l = i.getTicksCoords(), f = cS(l, r.transform, u, _e(s.getLineStyle(), {
    stroke: n.get(["axisLine", "lineStyle", "color"])
  }), "ticks"), h = 0; h < f.length; h++)
    e.add(f[h]);
  return f;
}
function vL(t, e, r, n, i) {
  var a = n.axis, o = n.getModel("minorTick");
  if (!(!t.showMinorTicks || a.scale.isBlank())) {
    var s = a.getMinorTicksCoords();
    if (s.length)
      for (var u = o.getModel("lineStyle"), l = i * o.get("length"), f = _e(u.getLineStyle(), _e(n.getModel("axisTick").getLineStyle(), {
        stroke: n.get(["axisLine", "lineStyle", "color"])
      })), h = 0; h < s.length; h++)
        for (var v = cS(s[h], r.transform, l, f, "minorticks_" + h), c = 0; c < v.length; c++)
          e.add(v[c]);
  }
}
function rg(t, e, r) {
  if (vS(t)) {
    var n = t.axisLabelsCreationContext, i = n.out.noPxChangeTryDetermine;
    if (r.noPxChange) {
      for (var a = !0, o = 0; o < i.length; o++)
        a = a && i[o]();
      if (a)
        return !1;
    }
    i.length && (e.remove(t.labelGroup), th(t, null, null, null));
  }
  return !0;
}
function dL(t, e, r, n, i, a) {
  var o = i.axis, s = ca(t.raw.axisLabelShow, i.get(["axisLabel", "show"])), u = new ut();
  r.add(u);
  var l = Os(n);
  if (!s || o.scale.isBlank()) {
    th(e, [], u, l);
    return;
  }
  var f = i.getModel("axisLabel"), h = o.getViewLabels(l), v = (ca(t.raw.labelRotate, f.get("rotate")) || 0) * Ir / 180, c = Er.innerTextLayout(t.rotation, v, t.labelDirection), d = i.getCategories && i.getCategories(!0), y = [], p = i.get("triggerEvent"), g = 1 / 0, m = -1 / 0;
  T(h, function(S, b) {
    var w, C = S.tick, M = S.formattedLabel, x = S.rawLabel, D = f, I = Pc(o.scale, C);
    if (d && d[I]) {
      var L = d[I];
      F(L) && L.textStyle && (D = new ce(L.textStyle, f, i.ecModel));
    }
    var A = D.getTextColor() || i.get(["axisLine", "lineStyle", "color"]), P = D.getShallow("align", !0) || c.textAlign, k = G(D.getShallow("alignMinLabel", !0), P), O = G(D.getShallow("alignMaxLabel", !0), P), V = D.getShallow("verticalAlign", !0) || D.getShallow("baseline", !0) || c.textVerticalAlign, K = G(D.getShallow("verticalAlignMinLabel", !0), V), J = G(D.getShallow("verticalAlignMaxLabel", !0), V), ee = 10 + (((w = C.time) === null || w === void 0 ? void 0 : w.level) || 0);
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
      silent: Er.isLabelSilent(i),
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
    var re = pi(Q);
    if (re.labelInfo = S, re.layoutRotation = c.rotation, fu({
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
      var ne = Er.makeAxisEventDataBase(i);
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
      o.type === "category" && (ne.dataIndex = I), he(Q).eventData = ne, q && yL(i, a, Q, q);
    }
    y.push(Q), u.add(Q);
  });
  var _ = H(y, function(S) {
    return {
      label: S,
      priority: pi(S).labelInfo.tick.break ? S.z2 + (m - g + 1) : S.z2,
      defaultAttr: {
        ignore: S.ignore
      }
    };
  });
  th(e, _, u, l);
}
function vS(t) {
  return !!t.labelLayoutList;
}
function th(t, e, r, n) {
  t.labelLayoutList = e, t.labelGroup = r, t.axisLabelsCreationContext = n;
}
function pL(t, e, r, n) {
  var i = e.get(["axisLabel", "margin"]);
  T(r, function(a, o) {
    var s = Zt(a);
    if (s) {
      var u = s.label, l = pi(u);
      s.suggestIgnore = u.ignore, u.ignore = !1, fs(ar, gL);
      var f = e.axis;
      ar.x = f.dataToCoord(Pc(f.scale, l.labelInfo.tick)), ar.y = t.labelOffset + t.labelDirection * i, ar.rotation = l.layoutRotation, n.add(ar), ar.updateTransform(), n.remove(ar), ar.decomposeTransform(), fs(u, ar), u.markRedraw(), Ns(s, !0), Zt(s);
    }
  });
}
var ar = new Re(), gL = new Re();
function kc(t) {
  return !!t;
}
function yL(t, e, r, n) {
  r.on("click", function(i) {
    var a = {
      type: eL,
      breaks: [{
        start: n.parsedBreak.breakOption.start,
        end: n.parsedBreak.breakOption.end
      }]
    };
    a[t.axis.dim + "AxisIndex"] = t.componentIndex, e.dispatchAction(a);
  });
}
function mL(t, e, r) {
  var n = Xe();
  if (n) {
    var i = n.retrieveAxisBreakPairs(r, function(o) {
      return o && pi(o.label).labelInfo.tick.break;
    }, !0), a = t.get(["breakLabelLayout", "moveOverlap"], !0);
    (a === !0 || a === "auto") && T(i, function(o) {
      lS().adjustBreakLabelPair(t.axis.inverse, e, [Zt(r[o[0]]), Zt(r[o[1]])]);
    });
  }
}
function Fs(t, e, r) {
  r = r || {};
  var n = e.axis, i = {}, a = n.getAxesOnZeroOf()[0], o = n.position, s = a ? "onZero" : o, u = n.dim, l = [t.x, t.x + t.width, t.y, t.y + t.height], f = {
    left: 0,
    right: 1,
    top: 0,
    bottom: 1,
    onZero: 2
  }, h = e.get("offset") || 0, v = u === "x" ? [l[2] - h, l[3] + h] : [l[0] - h, l[1] + h];
  if (a) {
    var c = a.toGlobalCoord(a.dataToCoord(0));
    v[f.onZero] = Math.max(Math.min(c, v[1]), v[0]);
  }
  i.position = [u === "y" ? v[f[s]] : l[0], u === "x" ? v[f[s]] : l[3]], i.rotation = Math.PI / 2 * (u === "x" ? 0 : 1);
  var d = {
    top: -1,
    bottom: 1,
    left: -1,
    right: 1
  };
  i.labelDirection = i.tickDirection = i.nameDirection = d[o], i.labelOffset = a ? v[f[o]] - v[f.onZero] : 0, e.get(["axisTick", "inside"]) && (i.tickDirection = -i.tickDirection), ca(r.labelInside, e.get(["axisLabel", "inside"])) && (i.labelDirection = -i.labelDirection);
  var y = e.get(["axisLabel", "rotate"]);
  return i.labelRotate = s === "top" ? -y : y, i.z2 = 1, i;
}
function gR(t) {
  return t.coordinateSystem && t.coordinateSystem.type === "cartesian2d";
}
function yR(t) {
  var e = {
    xAxisModel: null,
    yAxisModel: null
  };
  return T(e, function(r, n) {
    var i = n.replace(/Model$/, ""), a = t.getReferringComponents(i, ur).models[0];
    e[n] = a;
  }), e;
}
function mR(t, e, r, n, i, a) {
  for (var o = Fs(t, r), s = !1, u = !1, l = 0; l < e.length; l++)
    _A(e[l].getOtherAxis(r.axis).scale) && (s = u = !0, r.axis.type === "category" && r.axis.onBand && (u = !1));
  return o.axisLineAutoShow = s, o.axisTickAutoShow = u, o.defaultNameMoveOverlap = a, new Er(r, n, o, i);
}
function _R(t, e, r) {
  var n = Fs(e, r);
  t.updateCfg(n);
}
var SR = (
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
function _L(t, e) {
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
  return SL(r, t, e), r.seriesInvolved && wL(r, t), r;
}
function SL(t, e, r) {
  var n = e.getComponent("tooltip"), i = e.getComponent("axisPointer"), a = i.get("link", !0) || [], o = [];
  T(r.getCoordinateSystems(), function(s) {
    if (!s.axisPointerEnabled)
      return;
    var u = Ia(s.model), l = t.coordSysAxesInfo[u] = {};
    t.coordSysMap[u] = s;
    var f = s.model, h = f.getModel("tooltip", n);
    if (T(s.getAxes(), Te(y, !1, null)), s.getTooltipAxes && n && h.get("show")) {
      var v = h.get("trigger") === "axis", c = h.get(["axisPointer", "type"]) === "cross", d = s.getTooltipAxes(h.get(["axisPointer", "axis"]));
      (v || c) && T(d.baseAxes, Te(y, c ? "cross" : !0, v)), c && T(d.otherAxes, Te(y, "cross", !1));
    }
    function y(p, g, m) {
      var _ = m.model.getModel("axisPointer", i), S = _.get("show");
      if (!(!S || S === "auto" && !p && !rh(_))) {
        g == null && (g = _.get("triggerTooltip")), _ = p ? bL(m, h, i, e, p, g) : _;
        var b = _.get("snap"), w = _.get("triggerEmphasis"), C = Ia(m.model), M = g || b || m.type === "category", x = t.axesInfo[C] = {
          key: C,
          axis: m,
          coordSys: s,
          axisPointerModel: _,
          triggerTooltip: g,
          triggerEmphasis: w,
          involveSeries: M,
          snap: b,
          useHandle: rh(_),
          seriesModels: [],
          linkGroup: null
        };
        l[C] = x, t.seriesInvolved = t.seriesInvolved || M;
        var D = TL(a, m);
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
function bL(t, e, r, n, i, a) {
  var o = e.getModel("axisPointer"), s = ["type", "snap", "lineStyle", "shadowStyle", "label", "animation", "animationDurationUpdate", "animationEasingUpdate", "z"], u = {};
  T(s, function(v) {
    u[v] = Z(o.get(v));
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
function wL(t, e) {
  e.eachSeries(function(r) {
    var n = r.coordinateSystem, i = r.get(["tooltip", "trigger"], !0), a = r.get(["tooltip", "show"], !0);
    !n || !n.model || i === "none" || i === !1 || i === "item" || a === !1 || r.get(["axisPointer", "show"], !0) === !1 || T(t.coordSysAxesInfo[Ia(n.model)], function(o) {
      var s = o.axis;
      n.getAxis(s.dim) === s && (o.seriesModels.push(r), o.seriesDataCount == null && (o.seriesDataCount = 0), o.seriesDataCount += r.getData().count());
    });
  });
}
function TL(t, e) {
  for (var r = e.model, n = e.dim, i = 0; i < t.length; i++) {
    var a = t[i] || {};
    if (Nl(a[n + "AxisId"], r.id) || Nl(a[n + "AxisIndex"], r.componentIndex) || Nl(a[n + "AxisName"], r.name))
      return i;
  }
}
function Nl(t, e) {
  return t === "all" || B(t) && oe(t, e) >= 0 || t === e;
}
function CL(t) {
  var e = Bc(t);
  if (e) {
    var r = e.axisPointerModel, n = e.axis.scale, i = r.option, a = r.get("status"), o = r.get("value");
    o != null && (o = n.parse(o));
    var s = rh(r);
    a == null && (i.status = s ? "show" : "hide");
    var u = n.getExtent();
    // Pick a value on axis when initializing.
    (o == null || o > u[1]) && (o = u[1]), o < u[0] && (o = u[0]), i.value = o, s && (i.status = e.axis.scale.isBlank() ? "hide" : "show");
  }
}
function Bc(t) {
  var e = (t.ecModel.getComponent("axisPointer") || {}).coordSysAxesInfo;
  return e && e.axesInfo[Ia(t)];
}
function xL(t) {
  var e = Bc(t);
  return e && e.axisPointerModel;
}
function rh(t) {
  return !!t.get(["handle", "show"]);
}
function Ia(t) {
  return t.type + "||" + t.id;
}
var ng = {}, ML = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.render = function(r, n, i, a) {
      this.axisPointerClass && CL(r), t.prototype.render.apply(this, arguments), this._doUpdateAxisPointerClass(r, i, !0);
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
        var o = xL(r);
        o ? (this._axisPointer || (this._axisPointer = new a())).render(r, o, n, i) : this._disposeAxisPointer(n);
      }
    }, e.prototype._disposeAxisPointer = function(r) {
      this._axisPointer && this._axisPointer.dispose(r), this._axisPointer = null;
    }, e.registerAxisPointerClass = function(r, n) {
      ng[r] = n;
    }, e.getAxisPointerClass = function(r) {
      return r && ng[r];
    }, e.type = "axis", e;
  })(lt)
), fn = de(), ig = Z, Fl = le, DL = (
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
          var v = Te(ag, r, h);
          this.updatePointerEl(s, l, v), this.updateLabelEl(s, l, v, r);
        }
        sg(s, r, !0), this._renderHandle(a);
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
        if (a && wu(i).w > s)
          return !0;
        if (o) {
          var u = Bc(e).seriesDataCount, l = i.getExtent();
          return Math.abs(l[0] - l[1]) / u > s;
        }
        return !1;
      }
      return n === !0;
    }, t.prototype.makeElOption = function(e, r, n, i, a) {
    }, t.prototype.createPointerEl = function(e, r, n, i) {
      var a = r.pointer;
      if (a) {
        var o = fn(e).pointerEl = new JC[a.type](ig(r.pointer));
        e.add(o);
      }
    }, t.prototype.createLabelEl = function(e, r, n, i) {
      if (r.label) {
        var a = fn(e).labelEl = new je(ig(r.label));
        e.add(a), og(a, i);
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
      }), og(a, i));
    }, t.prototype._renderHandle = function(e) {
      if (!(this._dragging || !this.updateHandleTransform)) {
        var r = this._axisPointerModel, n = this._api.getZr(), i = this._handle, a = r.getModel("handle"), o = r.get("status");
        if (!a.get("show") || !o || o === "hide") {
          i && n.remove(i), this._handle = null;
          return;
        }
        var s;
        this._handle || (s = !0, i = this._handle = lu(a.get("icon"), {
          cursor: "move",
          draggable: !0,
          onmousemove: function(l) {
            my(l.event);
          },
          onmousedown: Fl(this._onHandleDragMove, this, 0, 0),
          drift: Fl(this._onHandleDragMove, this),
          ondragend: Fl(this._onHandleDragEnd, this)
        }), n.add(i)), sg(i, r, !1), i.setStyle(a.getItemStyle(null, ["color", "borderColor", "borderWidth", "opacity", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"]));
        var u = a.get("size");
        B(u) || (u = [u, u]), i.scaleX = u[0] / 2, i.scaleY = u[1] / 2, W_(this, "_doDispatchAxisPointer", a.get("throttle") || 0, "fixRate"), this._moveHandleToValue(e, s);
      }
    }, t.prototype._moveHandleToValue = function(e, r) {
      ag(this._axisPointerModel, !r && this._moveAnimation, this._handle, zl(this.getHandleTransform(e, this._axisModel, this._axisPointerModel)));
    }, t.prototype._onHandleDragMove = function(e, r) {
      var n = this._handle;
      if (n) {
        this._dragging = !0;
        var i = this.updateHandleTransform(zl(n), [e, r], this._axisModel, this._axisPointerModel);
        this._payloadInfo = i, n.stopAnimation(), n.attr(zl(i)), fn(n).lastProp = null, this._doDispatchAxisPointer();
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
      r && n && (this._lastGraphicKey = null, n && r.remove(n), i && r.remove(i), this._group = null, this._handle = null, this._payloadInfo = null), Vf(this, "_doDispatchAxisPointer");
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
function ag(t, e, r, n) {
  dS(fn(r).lastProp, n) || (fn(r).lastProp = n, e ? mi(r, n, t) : (r.stopAnimation(), r.attr(n)));
}
function dS(t, e) {
  if (F(t) && F(e)) {
    var r = !0;
    return T(e, function(n, i) {
      r = r && dS(t[i], n);
    }), !!r;
  } else
    return t === e;
}
function og(t, e) {
  t[e.get(["label", "show"]) ? "show" : "hide"]();
}
function zl(t) {
  return {
    x: t.x || 0,
    y: t.y || 0,
    rotation: t.rotation || 0
  };
}
function sg(t, e, r) {
  var n = e.get("z"), i = e.get("zlevel");
  t && t.traverse(function(a) {
    a.type !== "group" && (n != null && (a.z = n), i != null && (a.zlevel = i), a.silent = r);
  });
}
function IL(t) {
  var e = t.get("type"), r = t.getModel(e + "Style"), n;
  return e === "line" ? (n = r.getLineStyle(), n.fill = null) : e === "shadow" && (n = r.getAreaStyle(), n.stroke = null), n;
}
function AL(t, e, r, n, i) {
  var a = r.get("value"), o = pS(a, e.axis, e.ecModel, r.get("seriesDataIndices"), {
    precision: r.get(["label", "precision"]),
    formatter: r.get(["label", "formatter"])
  }), s = r.getModel("label"), u = Ha(s.get("padding") || 0), l = s.getFont(), f = zy(o, l), h = i.position, v = f.width + u[1] + u[3], c = f.height + u[0] + u[2], d = i.align;
  d === "right" && (h[0] -= v), d === "center" && (h[0] -= v / 2);
  var y = i.verticalAlign;
  y === "bottom" && (h[1] -= c), y === "middle" && (h[1] -= c / 2), PL(h, v, c, n);
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
function PL(t, e, r, n) {
  var i = n.getWidth(), a = n.getHeight();
  t[0] = Math.min(t[0] + e, i) - e, t[1] = Math.min(t[1] + r, a) - r, t[0] = Math.max(t[0], 0), t[1] = Math.max(t[1], 0);
}
function pS(t, e, r, n, i) {
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
      value: Es(e, {
        value: t
      }),
      axisDimension: e.dim,
      axisIndex: e.index,
      seriesData: []
    };
    T(n, function(u) {
      var l = r.getSeriesByIndex(u.seriesIndex), f = u.dataIndexInside, h = l && l.getDataParams(f);
      h && s.seriesData.push(h);
    }), N(o) ? a = o.replace("{value}", a) : X(o) && (a = o(s));
  }
  return a;
}
function gS(t, e, r) {
  var n = wt();
  return Ys(n, n, r.rotation), ns(n, n, r.position), Gh([t.dataToCoord(e), (r.labelOffset || 0) + (r.labelDirection || 1) * (r.labelMargin || 0)], n);
}
function LL(t, e, r, n, i, a) {
  var o = Er.innerTextLayout(r.rotation, 0, r.labelDirection);
  r.labelMargin = i.get(["label", "margin"]), AL(e, n, i, a, {
    position: gS(n.axis, t, r),
    align: o.textAlign,
    verticalAlign: o.textVerticalAlign
  });
}
function EL(t, e, r) {
  return r = r || 0, {
    x1: t[r],
    y1: t[1 - r],
    x2: e[r],
    y2: e[1 - r]
  };
}
function RL(t, e, r) {
  return r = r || 0, {
    x: t[r],
    y: t[1 - r],
    width: e[r],
    height: e[1 - r]
  };
}
function bR(t, e, r, n, i, a) {
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
function OL(t, e, r) {
  return wu(t, {
    fromStat: {
      sers: H(e, function(n) {
        return r.getSeriesByIndex(n.seriesIndex);
      })
    },
    min: 1
  }).w;
}
function kL(t, e, r) {
  return [ge(et(e[0], e[1]), t - r / 2), et(t + r / 2, ge(e[0], e[1]))];
}
var BL = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      return t !== null && t.apply(this, arguments) || this;
    }
    return e.prototype.makeElOption = function(r, n, i, a, o) {
      var s = i.axis, u = s.grid, l = a.get("type"), f = s.getGlobalExtent(), h = ug(u, s).getOtherAxis(s).getGlobalExtent(), v = s.toGlobalCoord(s.dataToCoord(n, !0));
      if (l && l !== "none") {
        var c = IL(a), d = NL[l](s, v, f, h, a.get("seriesDataIndices"), a.ecModel);
        d.style = c, r.graphicKey = d.type, r.pointer = d;
      }
      var y = Fs(u.getRect(), i);
      LL(n, r, y, i, a, o);
    }, e.prototype.getHandleTransform = function(r, n, i) {
      var a = Fs(n.axis.grid.getRect(), n, {
        labelInside: !1
      });
      a.labelMargin = i.get(["handle", "margin"]);
      var o = gS(n.axis, r, a);
      return {
        x: o[0],
        y: o[1],
        rotation: a.rotation + (a.labelDirection < 0 ? Math.PI : 0)
      };
    }, e.prototype.updateHandleTransform = function(r, n, i, a) {
      var o = i.axis, s = o.grid, u = o.getGlobalExtent(!0), l = ug(s, o).getOtherAxis(o).getGlobalExtent(), f = o.dim === "x" ? 0 : 1, h = [r.x, r.y];
      h[f] += n[f], h[f] = et(u[1], h[f]), h[f] = ge(u[0], h[f]);
      var v = (l[1] + l[0]) / 2, c = [v, v];
      c[f] = h[f];
      var d = [{
        verticalAlign: "middle"
      }, {
        align: "center"
      }];
      return {
        x: h[0],
        y: h[1],
        rotation: r.rotation,
        cursorPoint: c,
        tooltipOption: d[f]
      };
    }, e;
  })(DL)
);
function ug(t, e) {
  var r = {};
  return r[e.dim + "AxisIndex"] = e.index, t.getCartesian(r);
}
var NL = {
  line: function(t, e, r, n) {
    var i = EL([e, n[0]], [e, n[1]], lg(t));
    return {
      type: "Line",
      subPixelOptimize: !0,
      shape: i
    };
  },
  shadow: function(t, e, r, n, i, a) {
    var o = OL(t, i, a), s = n[1] - n[0], u = kL(e, r, o), l = u[0], f = u[1];
    return {
      type: "Rect",
      shape: RL([l, n[0]], [f - l, s], lg(t))
    };
  }
};
function lg(t) {
  return t.dim === "x" ? 0 : 1;
}
var FL = (
  /** @class */
  (function(t) {
    z(e, t);
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
), fr = de(), zL = T;
function yS(t, e, r) {
  if (!W.node) {
    var n = e.getZr();
    fr(n).records || (fr(n).records = {}), HL(n, e);
    var i = fr(n).records[t] || (fr(n).records[t] = {});
    i.handler = r;
  }
}
function HL(t, e) {
  if (fr(t).initialized)
    return;
  fr(t).initialized = !0, r("click", Te(Hl, "click")), r("mousemove", Te(Hl, "mousemove")), r("mousewheel", Te(Hl, "mousewheel")), r("globalout", GL);
  function r(n, i) {
    t.on(n, function(a) {
      var o = UL(e);
      zL(fr(t).records, function(s) {
        s && i(s, a, o.dispatchAction);
      }), VL(o.pendings, e);
    });
  }
}
function VL(t, e) {
  var r = t.showTip.length, n = t.hideTip.length, i;
  r ? i = t.showTip[r - 1] : n && (i = t.hideTip[n - 1]), i && (i.dispatchAction = null, e.dispatchAction(i));
}
function GL(t, e, r) {
  t.handler("leave", null, r);
}
function Hl(t, e, r, n) {
  e.handler(t, r, n);
}
function UL(t) {
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
function nh(t, e) {
  if (!W.node) {
    var r = e.getZr(), n = (fr(r).records || {})[t];
    n && (fr(r).records[t] = null);
  }
}
var YL = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.render = function(r, n, i) {
      var a = n.getComponent("tooltip"), o = r.get("triggerOn") || a && a.get("triggerOn") || "mousemove|click|mousewheel";
      yS("axisPointer", i, function(s, u, l) {
        o !== "none" && (s === "leave" || o.indexOf(s) >= 0) && l({
          type: "updateAxisPointer",
          currTrigger: s,
          x: u && u.offsetX,
          y: u && u.offsetY
        });
      });
    }, e.prototype.remove = function(r, n) {
      nh("axisPointer", n);
    }, e.prototype.dispose = function(r, n) {
      nh("axisPointer", n);
    }, e.type = "axisPointer", e;
  })(lt)
);
function mS(t, e) {
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
      var l = u.getBaseAxis(), f = u.getOtherAxis(l), h = f.dim, v = l.dim, c = h === "x" || h === "radius" ? 1 : 0, d = a.mapDimension(v), y = [];
      y[c] = a.get(d, o), y[1 - c] = a.get(a.getCalculationInfo("stackResultDimension"), o), r = u.dataToPoint(y) || [];
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
var fg = de();
function WL(t, e, r) {
  var n = t.currTrigger, i = [t.x, t.y], a = t, o = t.dispatchAction || le(r.dispatchAction, r), s = e.getComponent("axisPointer").coordSysAxesInfo;
  if (s) {
    jo(i) && (i = mS({
      seriesIndex: a.seriesIndex,
      // Do not use dataIndexInside from other ec instance.
      // FIXME: auto detect it?
      dataIndex: a.dataIndex
    }, e).point);
    var u = jo(i), l = a.axesInfo, f = s.axesInfo, h = n === "leave" || jo(i), v = {}, c = {}, d = {
      list: [],
      map: {}
    }, y = {
      showPointer: Te(qL, c),
      showTooltip: Te($L, d)
    };
    T(s.coordSysMap, function(g, m) {
      var _ = u || g.containPoint(i);
      T(s.coordSysAxesInfo[m], function(S, b) {
        var w = S.axis, C = QL(l, S);
        if (!h && _ && (!l || C)) {
          var M = C && C.value;
          M == null && !u && (M = w.pointToData(i)), M != null && hg(S, M, y, !1, v);
        }
      });
    });
    var p = {};
    return T(f, function(g, m) {
      var _ = g.linkGroup;
      _ && !c[m] && T(_.axesInfo, function(S, b) {
        var w = c[b];
        if (S !== g && w) {
          var C = w.value;
          _.mapper && (C = g.axis.scale.parse(_.mapper(C, cg(S), cg(g)))), p[g.key] = C;
        }
      });
    }), T(p, function(g, m) {
      hg(f[m], g, y, !0, v);
    }), ZL(c, f, v), KL(d, i, t, o), jL(f, o, r), v;
  }
}
function hg(t, e, r, n, i) {
  var a = t.axis;
  if (!(a.scale.isBlank() || !a.containData(e))) {
    if (!t.involveSeries) {
      r.showPointer(t, e);
      return;
    }
    var o = XL(e, t), s = o.payloadBatch, u = o.snapToValue;
    s[0] && i.seriesIndex == null && R(i, s[0]), !n && t.snap && a.containData(u) && u != null && (e = u), r.showPointer(t, e, s), r.showTooltip(t, o, u);
  }
}
function XL(t, e) {
  var r = e.axis, n = r.dim, i = t, a = [], o = Number.MAX_VALUE, s = -1;
  return T(e.seriesModels, function(u, l) {
    var f = u.getData().mapDimensionsAll(n), h, v;
    if (u.getAxisTooltipData) {
      var c = u.getAxisTooltipData(f, t, r);
      v = c.dataIndices, h = c.nestestValue;
    } else {
      if (v = u.indicesOfNearest(
        n,
        f[0],
        t,
        // Add a threshold to avoid find the wrong dataIndex
        // when data length is not same.
        // false,
        r.type === "category" ? 0.5 : null
      ), !v.length)
        return;
      h = u.getData().get(f[0], v[0]);
    }
    if (vr(h)) {
      var d = t - h, y = Math.abs(d);
      y <= o && ((y < o || d >= 0 && s < 0) && (o = y, s = d, i = h, a.length = 0), T(v, function(p) {
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
function qL(t, e, r, n) {
  t[e.key] = {
    value: r,
    payloadBatch: n
  };
}
function $L(t, e, r, n) {
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
function ZL(t, e, r) {
  var n = r.axesInfo = [];
  T(e, function(i, a) {
    var o = i.axisPointerModel.option, s = t[a];
    s ? (!i.useHandle && (o.status = "show"), o.value = s.value, o.seriesDataIndices = (s.payloadBatch || []).slice()) : !i.useHandle && (o.status = "hide"), o.status === "show" && n.push({
      axisDim: i.axis.dim,
      axisIndex: i.axis.model.componentIndex,
      value: o.value
    });
  });
}
function KL(t, e, r, n) {
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
function jL(t, e, r) {
  var n = r.getZr(), i = "axisPointerLastHighlights", a = fg(n)[i] || {}, o = fg(n)[i] = {};
  T(t, function(f, h) {
    var v = f.axisPointerModel.option;
    v.status === "show" && f.triggerEmphasis && T(v.seriesDataIndices, function(c) {
      o[c.seriesIndex + "|" + c.dataIndex] = c;
    });
  });
  var s = [], u = [];
  function l(f) {
    return {
      seriesIndex: f.seriesIndex,
      dataIndex: f.dataIndex
    };
  }
  T(a, function(f, h) {
    !o[h] && u.push(l(f));
  }), T(o, function(f, h) {
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
function QL(t, e) {
  for (var r = 0; r < (t || []).length; r++) {
    var n = t[r];
    if (e.axis.dim === n.axisDim && e.axis.model.componentIndex === n.axisIndex)
      return n;
  }
}
function cg(t) {
  var e = t.axis.model, r = {}, n = r.axisDim = t.axis.dim;
  return r.axisIndex = r[n + "AxisIndex"] = e.componentIndex, r.axisName = r[n + "AxisName"] = e.name, r.axisId = r[n + "AxisId"] = e.id, r;
}
function jo(t) {
  return !t || t[0] == null || isNaN(t[0]) || t[1] == null || isNaN(t[1]);
}
function JL(t) {
  ML.registerAxisPointerClass("CartesianAxisPointer", BL), t.registerComponentModel(FL), t.registerComponentView(YL), t.registerPreprocessor(function(e) {
    if (e) {
      (!e.axisPointer || e.axisPointer.length === 0) && (e.axisPointer = {});
      var r = e.axisPointer.link;
      r && !B(r) && (e.axisPointer.link = [r]);
    }
  }), t.registerProcessor(t.PRIORITY.PROCESSOR.STATISTIC, {
    overallReset: function(e, r) {
      e.getComponent("axisPointer").coordSysAxesInfo = _L(e, r);
    }
  }), t.registerAction({
    type: "updateAxisPointer",
    event: "updateAxisPointer",
    update: ":updateAxisPointer"
  }, WL);
}
function eE(t, e) {
  var r = Ha(e.get("padding")), n = e.getItemStyle(["color", "opacity"]);
  n.fill = e.get("backgroundColor");
  var i = new Re({
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
var tE = (
  /** @class */
  (function(t) {
    z(e, t);
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
function _S(t) {
  var e = t.get("confine");
  return e != null ? !!e : t.get("renderMode") === "richText";
}
function SS(t) {
  if (W.domSupported) {
    for (var e = document.documentElement.style, r = 0, n = t.length; r < n; r++)
      if (t[r] in e)
        return t[r];
  }
}
var bS = SS(["transform", "webkitTransform", "OTransform", "MozTransform", "msTransform"]), rE = SS(["webkitTransition", "transition", "OTransition", "MozTransition", "msTransition"]);
function wS(t, e) {
  if (!t)
    return e;
  e = ic(e, !0);
  var r = t.indexOf(e);
  return t = r === -1 ? e : "-" + t.slice(0, r) + "-" + e, t.toLowerCase();
}
function nE(t, e) {
  var r = t.currentStyle || document.defaultView && document.defaultView.getComputedStyle(t);
  return r ? r[e] : null;
}
var iE = wS(rE, "transition"), Nc = wS(bS, "transform"), aE = "position:absolute;display:block;border-style:solid;white-space:nowrap;z-index:9999999;" + (W.transform3dSupported ? "will-change:transform;" : "");
function oE(t) {
  return t = t === "left" ? "right" : t === "right" ? "left" : t === "top" ? "bottom" : "top", t;
}
function sE(t, e, r) {
  if (!N(r) || r === "inside")
    return "";
  var n = t.get("backgroundColor"), i = t.get("borderWidth");
  e = ba(e);
  var a = oE(r), o = Math.max(Math.round(i) * 1.5, 6), s = "", u = Nc + ":", l;
  oe(["left", "right"], a) > -1 ? (s += "top:50%", u += "translateY(-50%) rotate(" + (l = a === "left" ? -225 : -45) + "deg)") : (s += "left:50%", u += "translateX(-50%) rotate(" + (l = a === "top" ? 225 : 45) + "deg)");
  var f = l * Math.PI / 180, h = o + i, v = h * Math.abs(Math.cos(f)) + h * Math.abs(Math.sin(f)), c = Math.round(((v - Math.SQRT2 * i) / 2 + Math.SQRT2 * i - (v - h) / 2) * 100) / 100;
  s += ";" + a + ":-" + c + "px";
  var d = e + " solid " + i + "px;", y = ["position:absolute;width:" + o + "px;height:" + o + "px;z-index:-1;", s + ";" + u + ";", "border-bottom:" + d, "border-right:" + d, "background-color:" + n + ";"];
  return '<div style="' + y.join("") + '"></div>';
}
function uE(t, e, r) {
  var n = "cubic-bezier(0.23,1,0.32,1)", i = "", a = "";
  return r && (i = " " + t / 2 + "s " + n, a = "opacity" + i + ",visibility" + i), e || (i = " " + t + "s " + n, a += (a.length ? "," : "") + (W.transformSupported ? "" + Nc + i : ",left" + i + ",top" + i)), iE + ":" + a;
}
function vg(t, e, r) {
  var n = t.toFixed(0) + "px", i = e.toFixed(0) + "px";
  if (!W.transformSupported)
    return r ? "top:" + i + ";left:" + n + ";" : [["top", i], ["left", n]];
  var a = W.transform3dSupported, o = "translate" + (a ? "3d" : "") + "(" + n + "," + i + (a ? ",0" : "") + ")";
  return r ? "top:0;left:0;" + Nc + ":" + o + ";" : [["top", 0], ["left", 0], [bS, o]];
}
function lE(t) {
  var e = [], r = t.get("fontSize"), n = t.getTextColor();
  n && e.push("color:" + n), e.push("font:" + t.getFont());
  var i = G(t.get("lineHeight"), Math.round(r * 3 / 2));
  r && e.push("line-height:" + i + "px");
  var a = t.get("textShadowColor"), o = t.get("textShadowBlur") || 0, s = t.get("textShadowOffsetX") || 0, u = t.get("textShadowOffsetY") || 0;
  return a && o && e.push("text-shadow:" + s + "px " + u + "px " + o + "px " + a), T(["decoration", "align"], function(l) {
    var f = t.get(l);
    f && e.push("text-" + l + ":" + f);
  }), e.join(";");
}
function fE(t, e, r, n) {
  var i = [], a = t.get("transitionDuration"), o = t.get("backgroundColor"), s = t.get("shadowBlur"), u = t.get("shadowColor"), l = t.get("shadowOffsetX"), f = t.get("shadowOffsetY"), h = t.getModel("textStyle"), v = U_(t, "html"), c = l + "px " + f + "px " + s + "px " + u;
  return i.push("box-shadow:" + c), e && a > 0 && i.push(uE(a, r, n)), o && i.push("background-color:" + o), T(["width", "color", "radius"], function(d) {
    var y = "border-" + d, p = ic(y), g = t.get(p);
    g != null && i.push(y + ":" + g + (d === "color" ? "" : "px"));
  }), i.push(lE(h)), v != null && i.push("padding:" + Ha(v).join("px ") + "px"), i.join(";") + ";";
}
function dg(t, e, r, n, i) {
  var a = e && e.painter;
  if (r) {
    var o = a && a.getViewportRoot();
    o && I1(t, o, r, n, i);
  } else {
    t[0] = n, t[1] = i;
    var s = a && a.getViewportRootOffset();
    s && (t[0] += s.offsetLeft, t[1] += s.offsetTop);
  }
  t[2] = t[0] / e.getWidth(), t[3] = t[1] / e.getHeight();
}
var hE = (
  /** @class */
  (function() {
    function t(e, r) {
      if (this._show = !1, this._styleCoord = [0, 0, 0, 0], this._enterable = !0, this._alwaysShowContent = !1, this._firstShow = !0, this._longHide = !0, W.wxa)
        return null;
      var n = document.createElement("div");
      n.domBelongToZr = !0, this.el = n;
      var i = this._zr = e.getZr(), a = r.appendTo, o = a && (N(a) ? document.querySelector(a) : ui(a) ? a : X(a) && a(e.getDom()));
      dg(this._styleCoord, i, o, e.getWidth() / 2, e.getHeight() / 2), (o || e.getDom()).appendChild(n), this._api = e, this._container = o;
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
        var r = this._api.getDom(), n = nE(r, "position"), i = r.style;
        i.position !== "absolute" && n !== "absolute" && (i.position = "relative");
      }
      var a = e.get("alwaysShowContent");
      a && this._moveIfResized(), this._alwaysShowContent = a, this._enableDisplayTransition = e.get("displayTransition") && e.get("transitionDuration") > 0, this.el.className = e.get("className") || "";
    }, t.prototype.show = function(e, r) {
      clearTimeout(this._hideTimeout), clearTimeout(this._longHideTimeout);
      var n = this.el, i = n.style, a = this._styleCoord;
      n.innerHTML ? i.cssText = aE + fE(e, !this._firstShow, this._longHide, this._enableDisplayTransition) + vg(a[0], a[1], !0) + ("border-color:" + ba(r) + ";") + (e.get("extraCssText") || "") + (";pointer-events:" + (this._enterable ? "auto" : "none")) : i.display = "none", this._show = !0, this._firstShow = !1, this._longHide = !1;
    }, t.prototype.setContent = function(e, r, n, i, a) {
      var o = this.el;
      if (e == null) {
        o.innerHTML = "";
        return;
      }
      var s = "";
      if (N(a) && n.get("trigger") === "item" && !_S(n) && (s = sE(n, i, a)), N(e))
        o.innerHTML = e + s;
      else if (e) {
        o.innerHTML = "", B(e) || (e = [e]);
        for (var u = 0; u < e.length; u++)
          ui(e[u]) && e[u].parentNode !== o && o.appendChild(e[u]);
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
        if (dg(n, this._zr, this._container, e, r), n[0] != null && n[1] != null) {
          var i = this.el.style, a = vg(n[0], n[1]);
          T(a, function(o) {
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
      A1(e && e.painter && e.painter.getViewportRoot(), this._container);
      var r = this.el;
      if (r) {
        r.onmouseenter = r.onmousemove = r.onmouseleave = null;
        var n = r.parentNode;
        n && n.removeChild(r);
      }
      this.el = this._container = null;
    }, t;
  })()
), cE = (
  /** @class */
  (function() {
    function t(e) {
      this._show = !1, this._styleCoord = [0, 0, 0, 0], this._alwaysShowContent = !1, this._enterable = !0, this._zr = e.getZr(), gg(this._styleCoord, this._zr, e.getWidth() / 2, e.getHeight() / 2);
    }
    return t.prototype.update = function(e) {
      var r = e.get("alwaysShowContent");
      r && this._moveIfResized(), this._alwaysShowContent = r;
    }, t.prototype.show = function() {
      this._hideTimeout && clearTimeout(this._hideTimeout), this.el.show(), this._show = !0;
    }, t.prototype.setContent = function(e, r, n, i, a) {
      var o = this;
      F(e) && Ye(""), this.el && this._zr.remove(this.el);
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
          padding: U_(n, "richText"),
          verticalAlign: "top",
          align: "left"
        },
        z: n.get("z")
      }), T(["backgroundColor", "borderRadius", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"], function(l) {
        o.el.style[l] = n.get(l);
      }), T(["textShadowBlur", "textShadowOffsetX", "textShadowOffsetY"], function(l) {
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
      var e = this.el, r = this.el.getBoundingRect(), n = pg(e.style);
      return [r.width + n.left + n.right, r.height + n.top + n.bottom];
    }, t.prototype.moveTo = function(e, r) {
      var n = this.el;
      if (n) {
        var i = this._styleCoord;
        gg(i, this._zr, e, r), e = i[0], r = i[1];
        var a = n.style, o = Tr(a.borderWidth || 0), s = pg(a);
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
function Tr(t) {
  return Math.max(0, t);
}
function pg(t) {
  var e = Tr(t.shadowBlur || 0), r = Tr(t.shadowOffsetX || 0), n = Tr(t.shadowOffsetY || 0);
  return {
    left: Tr(e - r),
    right: Tr(e + r),
    top: Tr(e - n),
    bottom: Tr(e + n)
  };
}
function gg(t, e, r, n) {
  t[0] = r, t[1] = n, t[2] = t[0] / e.getWidth(), t[3] = t[1] / e.getHeight();
}
var vE = new Re({
  shape: {
    x: -1,
    y: -1,
    width: 2,
    height: 2
  }
}), dE = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.init = function(r, n) {
      if (!(W.node || !n.getDom())) {
        var i = r.getComponent("tooltip"), a = this._renderMode = Pw(i.get("renderMode"));
        this._tooltipContent = a === "richText" ? new cE(n) : new hE(n, {
          appendTo: i.get("appendToBody", !0) ? "body" : i.get("appendTo", !0)
        });
      }
    }, e.prototype.render = function(r, n, i) {
      if (!(W.node || !i.getDom())) {
        this.group.removeAll(), this._tooltipModel = r, this._ecModel = n, this._api = i;
        var a = this._tooltipContent;
        a.update(r), a.setEnterable(r.get("enterable")), this._initGlobalListener(), this._keepShow(), this._renderMode !== "richText" && r.get("transitionDuration") ? W_(this, "_updatePosition", 50, "fixRate") : Vf(this, "_updatePosition");
      }
    }, e.prototype._initGlobalListener = function() {
      var r = this._tooltipModel, n = r.get("triggerOn");
      yS("itemTooltip", this._api, le(function(i, a, o) {
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
        var o = yg(a, i);
        this._ticket = "";
        var s = a.dataByCoordSys, u = mE(a, n, i);
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
          var f = vE;
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
          var h = mS(a, n), v = h.point[0], c = h.point[1];
          v != null && c != null && this._tryShow({
            offsetX: v,
            offsetY: c,
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
      this._tooltipModel && o.hideLater(this._tooltipModel.get("hideDelay")), this._lastX = this._lastY = this._lastDataByCoordSys = null, this._cbParamsList = null, a.from !== this.uid && this._hide(yg(a, i));
    }, e.prototype._manuallyAxisShowTip = function(r, n, i, a) {
      var o = a.seriesIndex, s = a.dataIndex, u = n.getComponent("axisPointer").coordSysAxesInfo;
      if (!(o == null || s == null || u == null)) {
        var l = n.getSeriesByIndex(o);
        if (l) {
          var f = l.getData(), h = Wi([f.getItemModel(s), l, (l.coordinateSystem || {}).model], this._tooltipModel);
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
          Ji(i, function(f) {
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
      var i = this._ecModel, a = this._tooltipModel, o = [n.offsetX, n.offsetY], s = Wi([n.tooltipOption], a), u = this._renderMode, l = [], f = Ca("section", {
        blocks: [],
        noHeader: !0
      }), h = [], v = new wl();
      T(r, function(m) {
        T(m.dataByAxis, function(_) {
          var S = i.getComponent(_.axisDim + "Axis", _.axisIndex), b = _.value, w = S.axis, C = w.scale.parse(b);
          if (!(!S || b == null)) {
            var M = pS(b, w, i, _.seriesDataIndices, _.valueLabelOpt), x = Ca("section", {
              header: M,
              noHeader: !It(M),
              sortBlocks: !0,
              blocks: []
            });
            f.blocks.push(x), T(_.seriesDataIndices, function(D) {
              var I = i.getSeriesByIndex(D.seriesIndex), L = D.dataIndexInside, A = I.getDataParams(L);
              if (!(A.dataIndex < 0)) {
                A.axisDim = _.axisDim, A.axisIndex = _.axisIndex, A.axisType = _.axisType, A.axisId = _.axisId, A.axisValue = Es(S.axis, {
                  value: C
                }), A.axisValueLabel = M, A.marker = v.makeTooltipMarker("item", ba(A.color), u);
                var P = Yd(I.formatTooltip(L, !0, null)), k = P.frag;
                if (k) {
                  var O = Wi([I], a).get("valueFormatter");
                  x.blocks.push(O ? R({
                    valueFormatter: O
                  }, k) : k);
                }
                P.text && h.push(P.text), l.push(A);
              }
            });
          }
        });
      }), f.blocks.reverse(), h.reverse();
      var c = n.position, d = s.get("order"), y = Kd(f, v, u, d, i.get("useUTC"), s.get("textStyle"));
      y && h.unshift(y);
      var p = u === "richText" ? `

` : "<br/>", g = h.join(p);
      this._showOrMove(s, function() {
        this._updateContentNotChangedOnAxis(r, l) ? this._updatePosition(s, c, o[0], o[1], this._tooltipContent, l) : this._showTooltipContent(s, g, l, Math.random() + "", o[0], o[1], c, null, v);
      });
    }, e.prototype._showSeriesItemTooltip = function(r, n, i) {
      var a = this._ecModel, o = he(n), s = o.seriesIndex, u = a.getSeriesByIndex(s), l = o.dataModel || u, f = o.dataIndex, h = o.dataType, v = l.getData(h), c = this._renderMode, d = r.positionDefault, y = Wi([v.getItemModel(f), l, u && (u.coordinateSystem || {}).model], this._tooltipModel, d ? {
        position: d
      } : null), p = y.get("trigger");
      if (!(p != null && p !== "item")) {
        var g = l.getDataParams(f, h), m = new wl();
        g.marker = m.makeTooltipMarker("item", ba(g.color), c);
        var _ = Yd(l.formatTooltip(f, !1, h)), S = y.get("order"), b = y.get("valueFormatter"), w = _.frag, C = w ? Kd(b ? R({
          valueFormatter: b
        }, w) : w, m, c, S, a.get("useUTC"), y.get("textStyle")) : _.text, M = "item_" + l.name + "_" + f;
        this._showOrMove(y, function() {
          this._showTooltipContent(y, C, g, M, r.offsetX, r.offsetY, r.position, r.target, m);
        }), i({
          type: "showTip",
          dataIndexInside: f,
          dataIndex: v.getRawIndex(f),
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
      var h = [u], v = this._ecModel.getComponent(o.componentMainType, o.componentIndex);
      v && h.push(v), h.push({
        formatter: u.content
      });
      var c = r.positionDefault, d = Wi(h, this._tooltipModel, c ? {
        position: c
      } : null), y = d.get("content"), p = Math.random() + "", g = new wl();
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
        var v = r.get("formatter");
        u = u || r.get("position");
        var c = n, d = this._getNearestPoint([o, s], i, r.get("trigger"), r.get("borderColor"), r.get("defaultBorderColor", !0)), y = d.color;
        if (v)
          if (N(v)) {
            var p = r.ecModel.get("useUTC"), g = B(i) ? i[0] : i, m = g && g.axisType && g.axisType.indexOf("time") >= 0;
            c = v, m && (c = za(g.axisValue, c, p)), c = ac(c, i, !0);
          } else if (X(v)) {
            var _ = le(function(S, b) {
              S === this._ticket && (h.setContent(b, f, r, y, u), this._updatePosition(r, u, o, s, h, i, l));
            }, this);
            this._ticket = a, c = v(i, a, _);
          } else
            c = v;
        h.setContent(c, f, r, y, u), h.show(r, y), this._updatePosition(r, u, o, s, h, i, l);
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
      var h = o.getSize(), v = r.get("align"), c = r.get("verticalAlign"), d = u && u.getBoundingRect().clone();
      if (u && d.applyTransform(u.transform), X(n) && (n = n([i, a], s, o.el, d, {
        viewSize: [l, f],
        contentSize: h.slice()
      })), B(n))
        i = Dt(n[0], l), a = Dt(n[1], f);
      else if (F(n)) {
        var y = n;
        y.width = h[0], y.height = h[1];
        var p = _s(y, {
          width: l,
          height: f
        });
        i = p.x, a = p.y, v = null, c = null;
      } else if (N(n) && u) {
        var g = yE(n, d, h, r.get("borderWidth"));
        i = g[0], a = g[1];
      } else {
        var g = pE(i, a, o, l, f, v ? null : 20, c ? null : 20);
        i = g[0], a = g[1];
      }
      if (v && (i -= mg(v) ? h[0] / 2 : v === "right" ? h[0] : 0), c && (a -= mg(c) ? h[1] / 2 : c === "bottom" ? h[1] : 0), _S(r)) {
        var g = gE(i, a, o, l, f);
        i = g[0], a = g[1];
      }
      o.moveTo(i, a);
    }, e.prototype._updateContentNotChangedOnAxis = function(r, n) {
      var i = this._lastDataByCoordSys, a = this._cbParamsList, o = !!i && i.length === r.length;
      return o && T(i, function(s, u) {
        var l = s.dataByAxis || [], f = r[u] || {}, h = f.dataByAxis || [];
        o = o && l.length === h.length, o && T(l, function(v, c) {
          var d = h[c] || {}, y = v.seriesDataIndices || [], p = d.seriesDataIndices || [];
          o = o && v.value === d.value && v.axisType === d.axisType && v.axisId === d.axisId && y.length === p.length, o && T(y, function(g, m) {
            var _ = p[m];
            o = o && g.seriesIndex === _.seriesIndex && g.dataIndex === _.dataIndex;
          }), a && T(v.seriesDataIndices, function(g) {
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
      W.node || !n.getDom() || (Vf(this, "_updatePosition"), this._tooltipContent.dispose(), nh("itemTooltip", n), this._tooltipContent = null, this._tooltipModel = null, this._lastDataByCoordSys = null, this._cbParamsList = null);
    }, e.type = "tooltip", e;
  })(lt)
);
function Wi(t, e, r) {
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
function yg(t, e) {
  return t.dispatchAction || le(e.dispatchAction, e);
}
function pE(t, e, r, n, i, a, o) {
  var s = r.getSize(), u = s[0], l = s[1];
  return a != null && (t + u + a + 2 > n ? t -= u + a : t += a), o != null && (e + l + o > i ? e -= l + o : e += o), [t, e];
}
function gE(t, e, r, n, i) {
  var a = r.getSize(), o = a[0], s = a[1];
  return t = Math.min(t + o, n) - o, e = Math.min(e + s, i) - s, t = Math.max(t, 0), e = Math.max(e, 0), [t, e];
}
function yE(t, e, r, n) {
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
function mg(t) {
  return t === "center" || t === "middle";
}
function mE(t, e, r) {
  var n = Mh(t).queryOptionMap, i = n.keys()[0];
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
function wR(t) {
  di(JL), t.registerComponentModel(tE), t.registerComponentView(dE), t.registerAction({
    type: "showTip",
    event: "showTip",
    update: "tooltip:manuallyShowTip"
  }, Ee), t.registerAction({
    type: "hideTip",
    event: "hideTip",
    update: "tooltip:manuallyHideTip"
  }, Ee);
}
var _E = function(t, e) {
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
}, ih = (
  /** @class */
  (function(t) {
    z(e, t);
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
      n === !0 && (n = r.selector = ["all", "inverse"]), B(n) && T(n, function(a, o) {
        N(a) && (a = {
          type: a
        }), n[o] = pe(a, _E(i, a.type));
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
          var h = u.legendVisualProvider, v = h.getAllNames();
          r.isSeriesFiltered(u) || (i = i.concat(v)), v.length ? n = n.concat(v) : f = !0;
        } else
          f = !0;
        f && xh(u) && n.push(u.name);
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
        T(a, function(o) {
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
      T(r, function(i) {
        n[i.get("name", !0)] = !0;
      });
    }, e.prototype.inverseSelect = function() {
      var r = this._data, n = this.option.selected;
      T(r, function(i) {
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
), qn = Te, ah = T, Ao = ut, TS = (
  /** @class */
  (function(t) {
    z(e, t);
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
        var f = Gx(r, i).refContainer, h = r.getBoxLayoutParams(), v = r.get("padding"), c = _s(h, f, v), d = this.layoutInner(r, o, c, a, u, l), y = _s(_e({
          width: d.width,
          height: d.height
        }, h), f, v);
        this.group.x = y.x - d.x, this.group.y = y.y - d.y, this.group.markRedraw(), this.group.add(this._backgroundEl = eE(
          d,
          // FXIME: most itemStyle options does not work in background because inherit is not handled yet.
          r
        ));
      }
    }, e.prototype.resetInner = function() {
      this.getContentGroup().removeAll(), this._backgroundEl && this.group.remove(this._backgroundEl), this.getSelectorGroup().removeAll();
    }, e.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this.getContentGroup(), f = U(), h = n.get("selectedMode"), v = n.get("triggerEvent"), c = [];
      i.eachRawSeries(function(d) {
        !d.get("legendHoverLink") && c.push(d.id);
      }), ah(n.getData(), function(d, y) {
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
            var S = _.getData(), b = S.getVisual("legendLineStyle") || {}, w = S.getVisual("legendIcon"), C = S.getVisual("style"), M = this._createItem(_, g, y, d, n, r, b, C, w, h, a);
            M.on("click", qn(_g, g, null, a, c)).on("mouseover", qn(oh, _.name, null, a, c)).on("mouseout", qn(sh, _.name, null, a, c)), i.ssr && M.eachChild(function(x) {
              var D = he(x);
              D.seriesIndex = _.seriesIndex, D.dataIndex = y, D.ssrType = "legend";
            }), v && M.eachChild(function(x) {
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
                k && k[3] === 0 && (k[3] = 0.2, A = R(R({}, A), {
                  fill: Br(k, "rgba")
                }));
                var O = this._createItem(x, g, y, d, n, r, {}, A, P, h, a);
                O.on("click", qn(_g, null, g, a, c)).on("mouseover", qn(oh, null, g, a, c)).on("mouseout", qn(sh, null, g, a, c)), i.ssr && O.eachChild(function(V) {
                  var K = he(V);
                  K.seriesIndex = x.seriesIndex, K.dataIndex = y, K.ssrType = "legend";
                }), v && O.eachChild(function(V) {
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
      ah(r, function(l) {
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
        var v = n.getModel("selectorLabel"), c = n.getModel(["emphasis", "selectorLabel"]);
        ex(h, {
          normal: v,
          emphasis: c
        }, {
          defaultText: l.title
        }), ps(h);
      });
    }, e.prototype._createItem = function(r, n, i, a, o, s, u, l, f, h, v) {
      var c = r.visualDrawType, d = o.get("itemWidth"), y = o.get("itemHeight"), p = o.isSelected(n), g = a.get("symbolRotate"), m = a.get("symbolKeepAspect"), _ = a.get("icon");
      f = _ || f || "roundRect";
      var S = SE(f, a, u, l, c, p, v), b = new Ao(), w = a.getModel("textStyle");
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
        b.add(bE({
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
      var L = p ? w.getTextColor() : a.get("inactiveColor");
      b.add(new je({
        style: Cn(w, {
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
      var A = new Re({
        shape: b.getBoundingRect(),
        style: {
          // Cannot use 'invisible' because SVG SSR will miss the node
          fill: "transparent"
        }
      }), P = a.getModel("tooltip");
      return P.get("show") && fu({
        el: A,
        componentModel: o,
        itemName: n,
        itemTooltipOption: P.option
      }), b.add(A), b.eachChild(function(k) {
        k.silent = !0;
      }), A.silent = !h, this.getContentGroup().add(b), ps(b), b.__legendDataIndex = i, b;
    }, e.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getContentGroup(), l = this.getSelectorGroup();
      sa(r.get("orient"), u, r.get("itemGap"), i.width, i.height);
      var f = u.getBoundingRect(), h = [-f.x, -f.y];
      if (l.markRedraw(), u.markRedraw(), o) {
        sa(
          // Buttons in selectorGroup always layout horizontally
          "horizontal",
          l,
          r.get("selectorItemGap", !0)
        );
        var v = l.getBoundingRect(), c = [-v.x, -v.y], d = r.get("selectorButtonGap", !0), y = r.getOrient().index, p = y === 0 ? "width" : "height", g = y === 0 ? "height" : "width", m = y === 0 ? "y" : "x";
        s === "end" ? c[y] += f[p] + d : h[y] += v[p] + d, c[1 - y] += f[g] / 2 - v[g] / 2, l.x = c[0], l.y = c[1], u.x = h[0], u.y = h[1];
        var _ = {
          x: 0,
          y: 0
        };
        return _[p] = f[p] + d + v[p], _[g] = Math.max(f[g], v[g]), _[m] = Math.min(0, v[m] + c[1 - y]), _;
      } else
        return u.x = h[0], u.y = h[1], this.group.getBoundingRect();
    }, e.prototype.remove = function() {
      this.getContentGroup().removeAll(), this._isFirstRender = !0;
    }, e.type = "legend.plain", e;
  })(lt)
);
function SE(t, e, r, n, i, a, o) {
  function s(p, g) {
    p.lineWidth === "auto" && (p.lineWidth = g.lineWidth > 0 ? 2 : 0), ah(p, function(m, _) {
      p[_] === "inherit" && (p[_] = g[_]);
    });
  }
  var u = e.getModel("itemStyle"), l = u.getItemStyle(), f = t.lastIndexOf("empty", 0) === 0 ? "fill" : "stroke", h = u.getShallow("decal");
  l.decal = !h || h === "inherit" ? n.decal : Xf(h, o), l.fill === "inherit" && (l.fill = n[i]), l.stroke === "inherit" && (l.stroke = n[f]), l.opacity === "inherit" && (l.opacity = (i === "fill" ? n : r).opacity), s(l, n);
  var v = e.getModel("lineStyle"), c = v.getLineStyle();
  if (s(c, r), l.fill === "auto" && (l.fill = n.fill), l.stroke === "auto" && (l.stroke = n.fill), c.stroke === "auto" && (c.stroke = n.fill), !a) {
    var d = e.get("inactiveBorderWidth"), y = l[f];
    l.lineWidth = d === "auto" ? n.lineWidth > 0 && y ? 2 : 0 : l.lineWidth, l.fill = e.get("inactiveColor"), l.stroke = e.get("inactiveBorderColor"), c.stroke = v.get("inactiveColor"), c.lineWidth = v.get("inactiveWidth");
  }
  return {
    itemStyle: l,
    lineStyle: c
  };
}
function bE(t) {
  var e = t.icon || "roundRect", r = gu(e, 0, 0, t.itemWidth, t.itemHeight, t.itemStyle.fill, t.symbolKeepAspect);
  return r.setStyle(t.itemStyle), r.rotation = (t.iconRotate || 0) * Math.PI / 180, r.setOrigin([t.itemWidth / 2, t.itemHeight / 2]), e.indexOf("empty") > -1 && (r.style.stroke = r.style.fill, r.style.fill = j.color.neutral00, r.style.lineWidth = 2), r;
}
function _g(t, e, r, n) {
  sh(t, e, r, n), r.dispatchAction({
    type: "legendToggleSelect",
    name: t ?? e
  }), oh(t, e, r, n);
}
function oh(t, e, r, n) {
  r.usingTHL() || r.dispatchAction({
    type: "highlight",
    seriesName: t,
    name: e,
    excludeSeriesId: n
  });
}
function sh(t, e, r, n) {
  r.usingTHL() || r.dispatchAction({
    type: "downplay",
    seriesName: t,
    name: e,
    excludeSeriesId: n
  });
}
function Xi(t, e, r) {
  var n = t === "allSelect" || t === "inverseSelect", i = {}, a = [];
  r.eachComponent({
    mainType: "legend",
    query: e
  }, function(s) {
    n ? s[t]() : s[t](e.name), Sg(s, i), a.push(s.componentIndex);
  });
  var o = {};
  return r.eachComponent("legend", function(s) {
    T(i, function(u, l) {
      s[u ? "select" : "unSelect"](l);
    }), Sg(s, o);
  }), n ? {
    selected: o,
    // return legendIndex array to tell the developers which legends are allSelect / inverseSelect
    legendIndex: a
  } : {
    name: e.name,
    selected: o
  };
}
function Sg(t, e) {
  var r = e || {};
  return T(t.getData(), function(n) {
    var i = n.get("name");
    if (!(i === `
` || i === "")) {
      var a = t.isSelected(i);
      qe(r, i) ? r[i] = r[i] && a : r[i] = a;
    }
  }), r;
}
function wE(t) {
  t.registerAction("legendToggleSelect", "legendselectchanged", Te(Xi, "toggleSelected")), t.registerAction("legendAllSelect", "legendselectall", Te(Xi, "allSelect")), t.registerAction("legendInverseSelect", "legendinverseselect", Te(Xi, "inverseSelect")), t.registerAction("legendSelect", "legendselected", Te(Xi, "select")), t.registerAction("legendUnSelect", "legendunselected", Te(Xi, "unSelect"));
}
var TE = Ih(CE);
function CE(t) {
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
function CS(t) {
  t.registerComponentModel(ih), t.registerComponentView(TS), t.registerProcessor(t.PRIORITY.PROCESSOR.SERIES_FILTER, TE), t.registerSubTypeDefaulter("legend", function() {
    return "plain";
  }), wE(t);
}
var xE = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r;
    }
    return e.prototype.setScrollDataIndex = function(r) {
      this.option.scrollDataIndex = r;
    }, e.prototype.init = function(r, n, i) {
      var a = sc(r);
      t.prototype.init.call(this, r, n, i), bg(this, r, a);
    }, e.prototype.mergeOption = function(r, n) {
      t.prototype.mergeOption.call(this, r, n), bg(this, this.option, r);
    }, e.type = "legend.scroll", e.defaultOption = vx(ih.defaultOption, {
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
  })(ih)
);
function bg(t, e, r) {
  var n = t.getOrient(), i = [1, 1];
  i[n.index] = 0, wa(e, r, {
    type: "box",
    ignoreSize: !!i
  });
}
var wg = ut, Vl = ["width", "height"], Gl = ["x", "y"], ME = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = e.type, r.newlineDisabled = !0, r._currentIndex = 0, r;
    }
    return e.prototype.init = function() {
      t.prototype.init.call(this), this.group.add(this._containerGroup = new wg()), this._containerGroup.add(this.getContentGroup()), this.group.add(this._controllerGroup = new wg());
    }, e.prototype.resetInner = function() {
      t.prototype.resetInner.call(this), this._controllerGroup.removeAll(), this._containerGroup.removeClipPath(), this._containerGroup.__rectSize = null;
    }, e.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this;
      t.prototype.renderInner.call(this, r, n, i, a, o, s, u);
      var f = this._controllerGroup, h = n.get("pageIconSize", !0), v = B(h) ? h : [h, h];
      d("pagePrev", 0);
      var c = n.getModel("pageTextStyle");
      f.add(new je({
        name: "pageText",
        style: {
          // Placeholder to calculate a proper layout.
          text: "xx/xx",
          fill: c.getTextColor(),
          font: c.getFont(),
          verticalAlign: "middle",
          align: "center"
        },
        silent: !0
      })), d("pageNext", 1);
      function d(y, p) {
        var g = y + "DataIndex", m = lu(n.get("pageIcons", !0)[n.getOrient().name][p], {
          // Buttons will be created in each render, so we do not need
          // to worry about avoiding using legendModel kept in scope.
          onclick: le(l._pageGo, l, g, n, a)
        }, {
          x: -v[0] / 2,
          y: -v[1] / 2,
          width: v[0],
          height: v[1]
        });
        m.name = y, f.add(m);
      }
    }, e.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getSelectorGroup(), l = r.getOrient().index, f = Vl[l], h = Gl[l], v = Vl[1 - l], c = Gl[1 - l];
      o && sa(
        // Buttons in selectorGroup always layout horizontally
        "horizontal",
        u,
        r.get("selectorItemGap", !0)
      );
      var d = r.get("selectorButtonGap", !0), y = u.getBoundingRect(), p = [-y.x, -y.y], g = Z(i);
      o && (g[f] = i[f] - y[f] - d);
      var m = this._layoutContentAndController(r, a, g, l, f, v, c, h);
      if (o) {
        if (s === "end")
          p[l] += m[f] + d;
        else {
          var _ = y[f] + d;
          p[l] -= _, m[h] -= _;
        }
        m[f] += y[f] + d, p[1 - l] += m[c] + m[v] / 2 - y[v] / 2, m[v] = Math.max(m[v], y[v]), m[c] = Math.min(m[c], y[c] + p[1 - l]), u.x = p[0], u.y = p[1], u.markRedraw();
      }
      return m;
    }, e.prototype._layoutContentAndController = function(r, n, i, a, o, s, u, l) {
      var f = this.getContentGroup(), h = this._containerGroup, v = this._controllerGroup;
      sa(r.get("orient"), f, r.get("itemGap"), a ? i.width : null, a ? null : i.height), sa(
        // Buttons in controller are layout always horizontally.
        "horizontal",
        v,
        r.get("pageButtonItemGap", !0)
      );
      var c = f.getBoundingRect(), d = v.getBoundingRect(), y = this._showController = c[o] > i[o], p = [-c.x, -c.y];
      n || (p[a] = f[l]);
      var g = [0, 0], m = [-d.x, -d.y], _ = G(r.get("pageButtonGap", !0), r.get("itemGap", !0));
      if (y) {
        var S = r.get("pageButtonPosition", !0);
        S === "end" ? m[a] += i[o] - d[o] : g[a] += d[o] + _;
      }
      m[1 - a] += c[s] / 2 - d[s] / 2, f.setPosition(p), h.setPosition(g), v.setPosition(m);
      var b = {
        x: 0,
        y: 0
      };
      if (b[o] = y ? i[o] : c[o], b[s] = Math.max(c[s], d[s]), b[u] = Math.min(0, d[u] + m[1 - a]), h.__rectSize = i[o], y) {
        var w = {
          x: 0,
          y: 0
        };
        w[o] = Math.max(i[o] - d[o] - _, 0), w[s] = b[s], h.setClipPath(new Re({
          shape: w
        })), h.__rectSize = w[o];
      } else
        v.eachChild(function(M) {
          M.attr({
            invisible: !0,
            silent: !0
          });
        });
      var C = this._getPageInfo(r);
      return C.pageIndex != null && mi(
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
      T(["pagePrev", "pageNext"], function(f) {
        var h = f + "DataIndex", v = n[h] != null, c = i.childOfName(f);
        c && (c.setStyle("fill", v ? r.get("pageIconColor", !0) : r.get("pageIconInactiveColor", !0)), c.cursor = v ? "pointer" : "default");
      });
      var a = i.childOfName("pageText"), o = r.get("pageFormatter"), s = n.pageIndex, u = s != null ? s + 1 : 0, l = n.pageCount;
      a && o && a.setStyle("text", N(o) ? o.replace("{current}", u == null ? "" : u + "").replace("{total}", l == null ? "" : l + "") : o({
        current: u,
        total: l
      }));
    }, e.prototype._getPageInfo = function(r) {
      var n = r.get("scrollDataIndex", !0), i = this.getContentGroup(), a = this._containerGroup.__rectSize, o = r.getOrient().index, s = Vl[o], u = Gl[o], l = this._findTargetItemIndex(n), f = i.children(), h = f[l], v = f.length, c = v ? 1 : 0, d = {
        contentPosition: [i.x, i.y],
        pageCount: c,
        pageIndex: c - 1,
        pagePrevDataIndex: null,
        pageNextDataIndex: null
      };
      if (!h)
        return d;
      var y = S(h);
      d.contentPosition[o] = -y.s;
      for (var p = l + 1, g = y, m = y, _ = null; p <= v; ++p)
        _ = S(f[p]), // Half of the last item is out of the window.
        (!_ && m.e > g.s + a || _ && !b(_, g.s)) && (m.i > g.i ? g = m : g = _, g && (d.pageNextDataIndex == null && (d.pageNextDataIndex = g.i), ++d.pageCount)), m = _;
      for (var p = l - 1, g = y, m = y, _ = null; p >= -1; --p)
        _ = S(f[p]), // If the the end item does not intersect with the window started
        // from the current item, a page can be settled.
        (!_ || !b(m, _.s)) && g.i < m.i && (m = g, d.pagePrevDataIndex == null && (d.pagePrevDataIndex = g.i), ++d.pageCount, ++d.pageIndex), g = _;
      return d;
      function S(w) {
        if (w) {
          var C = w.getBoundingRect(), M = C[u] + w[u];
          return {
            s: M,
            e: M + C[s],
            i: w.__legendDataIndex
          };
        }
      }
      function b(w, C) {
        return w.e >= C && w.s <= C + a;
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
  })(TS)
);
function DE(t) {
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
function IE(t) {
  di(CS), t.registerComponentModel(xE), t.registerComponentView(ME), DE(t);
}
function TR(t) {
  di(CS), di(IE);
}
var AE = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return e.prototype.init = function(r, n, i) {
      t.prototype.init.call(this, r, n, i), this._sourceManager = new N_(this), $d(this);
    }, e.prototype.mergeOption = function(r, n) {
      t.prototype.mergeOption.call(this, r, n), $d(this);
    }, e.prototype.optionUpdated = function() {
      this._sourceManager.dirty();
    }, e.prototype.getSourceManager = function() {
      return this._sourceManager;
    }, e.type = "dataset", e.defaultOption = {
      seriesLayoutBy: Pt
    }, e;
  })(ae)
), PE = (
  /** @class */
  (function(t) {
    z(e, t);
    function e() {
      var r = t !== null && t.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return e.type = "dataset", e;
  })(lt)
);
function CR(t) {
  t.registerComponentModel(AE), t.registerComponentView(PE);
}
function Tg(t, e, r) {
  var n = Le.createCanvas(), i = e.getWidth(), a = e.getHeight(), o = n.style;
  return o && (o.position = "absolute", o.left = "0", o.top = "0", o.width = i + "px", o.height = a + "px", n.setAttribute("data-zr-dom-id", t)), n.width = i * r, n.height = a * r, n;
}
function Ul(t) {
  return !t.__cursors.get(Nm);
}
function Cg(t) {
  var e = t.__cursors.get(Nm);
  return {
    startIdx: e ? e.startIdx : 0,
    endIdx: e ? e.endIdx : 0
  };
}
var xS = (function(t) {
  z(e, t);
  function e(r, n, i) {
    var a = t.call(this) || this;
    a.motionBlur = !1, a.lastFrameAlpha = 0.7, a.dpr = 1, a.virtual = !1, a.config = {}, a.zlevel = 0, a.zlevel2 = Uo, a.maxRepaintRectCount = 5, a.__dirty = !0, a.__firstTimePaint = !0, a.__prevIdx = { startIdx: 0, endIdx: 0 };
    var o;
    i = i || ls, typeof r == "string" ? o = Tg(r, n, i) : F(r) && (o = r, r = o.id), a.id = r, a.dom = o;
    var s = o.style;
    return s && (gh(o), o.onselectstart = function() {
      return !1;
    }, s.padding = "0", s.margin = "0", s.borderWidth = "0"), a.painter = n, a.dpr = i, a;
  }
  return e.prototype.afterBrush = function() {
    this.__prevIdx = Cg(this);
  }, e.prototype.initContext = function() {
    this.ctx = this.dom.getContext("2d"), this.ctx.dpr = this.dpr;
  }, e.prototype.setUnpainted = function() {
    this.__firstTimePaint = !0;
  }, e.prototype.createBackBuffer = function() {
    var r = this.dpr;
    this.domBack = Tg("back-" + this.id, this.painter, r), this.ctxBack = this.domBack.getContext("2d"), r !== 1 && this.ctxBack.scale(r, r);
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
          for (var w = !1, C = 1 / 0, M = 0, x = 0; x < o.length; ++x) {
            var D = o[x];
            if (D.intersect(S)) {
              var I = new Y(0, 0, 0, 0);
              I.copy(D), I.union(S), o[x] = I, w = !0;
              break;
            } else if (u) {
              l.copy(S), l.union(D);
              var L = S.width * S.height, A = D.width * D.height, P = l.width * l.height, k = P - L - A;
              k < C && (C = k, M = x);
            }
          }
          if (u && (o[M].union(S), w = !0), !w) {
            var b = new Y(0, 0, 0, 0);
            b.copy(S), o.push(b);
          }
          u || (u = o.length >= s);
        }
    }
    for (var h = Cg(this), v = h.startIdx; v < h.endIdx; ++v) {
      var c = r[v];
      if (c) {
        var d = c.shouldBePainted(i, a, !0, !0), y = c.__isRendered && (c.__dirty & Je || !d) ? c.getPrevPaintRect() : null;
        y && f(y);
        var p = d && (c.__dirty & Je || !c.__isRendered) ? c.getPaintRect() : null;
        p && f(p);
      }
    }
    for (var g = this.__prevIdx, v = g.startIdx; v < g.endIdx; ++v) {
      var c = n[v], d = c && c.shouldBePainted(i, a, !0, !0);
      if (c && (!d || !c.__zr) && c.__isRendered) {
        var y = c.getPrevPaintRect();
        y && f(y);
      }
    }
    var m;
    do {
      m = !1;
      for (var v = 0; v < o.length; ) {
        if (o[v].isZero()) {
          o.splice(v, 1);
          continue;
        }
        for (var _ = v + 1; _ < o.length; )
          o[v].intersect(o[_]) ? (m = !0, o[v].union(o[_]), o.splice(_, 1)) : _++;
        v++;
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
    var l = this.motionBlur && !r, f = this.lastFrameAlpha, h = this.dpr, v = this;
    l && (this.domBack || this.createBackBuffer(), this.ctxBack.globalCompositeOperation = "copy", this.ctxBack.drawImage(a, 0, 0, s / h, u / h));
    var c = this.domBack;
    function d(y, p, g, m) {
      if (o.clearRect(y, p, g, m), n && n !== "transparent") {
        var _ = void 0;
        if (Aa(n)) {
          var S = n.global || n.__width === g && n.__height === m;
          _ = S && n.__canvasGradient || Yf(o, n, {
            x: 0,
            y: 0,
            width: g,
            height: m
          }), n.__canvasGradient = _, n.__width = g, n.__height = m;
        } else oy(n) && (n.scaleX = n.scaleX || h, n.scaleY = n.scaleY || h, _ = Wf(o, n, {
          dirty: function() {
            v.setUnpainted(), v.painter.refresh();
          }
        }));
        o.save(), o.fillStyle = _ || n, o.fillRect(y, p, g, m), o.restore();
      }
      l && (o.save(), o.globalAlpha = f, o.drawImage(c, y, p, g, m), o.restore());
    }
    !i || l ? d(0, 0, s, u) : i.length && T(i, function(y) {
      d(y.x * h, y.y * h, y.width * h, y.height * h);
    });
  }, e;
})(Kt), xg = 1e5, ln = 314159, Yl = void 0, LE = 1, Wl = 2;
function EE(t) {
  return t ? t.__builtin__ ? !0 : !(typeof t.resize != "function" || typeof t.refresh != "function") : !1;
}
function RE(t, e) {
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
function Mg(t, e, r, n) {
  var i = new xS(t, e, e.dpr);
  return i.zlevel = r, i.zlevel2 = n, i.__builtin__ = !0, MS(i), i;
}
function MS(t) {
  t.__cursorStack = [], t.__cursors = U();
}
function OE(t) {
  return t.startIdx = t.drawIdx = t.endIdx = t.endIdxNew = 0, t.used = !1, t.first = t.last = NaN, t.notClearIdx = -1, t;
}
function kE(t, e) {
  var r = t.__cursors, n = +e;
  return r.get(n) || (t.__cursorStack.push(n), r.set(n, OE({ key: n })));
}
function Po(t, e) {
  for (var r = t.__cursorStack, n = 0; n < r.length; n++)
    e(t.__cursors.get(r[n]));
}
function Xl(t, e) {
  var r = t.layers;
  return r[e] || (r[e] = new Array(3));
}
function ke(t, e, r) {
  for (var n = t.layerStack, i = 0; i < n.length; i++) {
    var a = n[i].zl, o = n[i].zl2, s = t.layers[a][o];
    (!r || (!(r & fa) || s.__builtin__) && (!(r & uh) || !s.__builtin__) && (!(r & DS) || s !== t.hoverlayer)) && e(s, a, o, i);
  }
}
var fa = 1, uh = 2, DS = 4, Lo = fa | DS, BE = (function() {
  function t(e, r, n, i) {
    this.type = "canvas", this._prevDisplayList = [], this._layerConfig = {}, this._needsManuallyCompositing = !1, this.type = "canvas", this._i = {
      layerStack: [],
      layers: []
    };
    var a = !e.nodeName || e.nodeName.toUpperCase() === "CANVAS";
    this._opts = n = R({}, n || {}), this.dpr = n.devicePixelRatio || ls, this._singleCanvas = a, this.root = e;
    var o = e.style;
    if (o && (gh(e), e.innerHTML = ""), this.storage = r, this._prevDisplayList = [], a) {
      var u = e, l = u.width, f = u.height;
      n.width != null && (l = n.width), n.height != null && (f = n.height), this.dpr = n.devicePixelRatio || 1, u.width = l * this.dpr, u.height = f * this.dpr, this._width = l, this._height = f;
      var h = Mg(u, this, ln, Uo);
      h.initContext(), this._insertLayer(h, ln, Uo, !0), this._domRoot = e;
    } else {
      this._width = mo(e, 0, n), this._height = mo(e, 1, n);
      var s = this._domRoot = RE(this._width, this._height);
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
    e && !F(e) ? r = { paintAll: !!e } : r = e || {};
    var n = G(r.refresh, !0), i = G(r.refreshHover, !1);
    if (i && (this._hoverLayerDirty = Wl), !n)
      return i && this._paintHoverList(this.storage.getDisplayList(!1)), this;
    var a = this.storage.getDisplayList(!0);
    this._updateLayerStatus(a, r.paintAll), this._redrawId = Math.random();
    var o = this._prevDisplayList;
    this._paintList(a, o, this._redrawId);
    var s = this._backgroundColor;
    return ke(this._i, function(u, l, f, h) {
      u.refresh && u.refresh(h === 0 ? s : null);
    }, uh), this._opts.useDirtyRect && (this._prevDisplayList = a.slice()), this;
  }, t.prototype._paintHoverList = function(e) {
    var r = this._i.hoverlayer, n = this._hoverLayerDirty;
    if (this._hoverLayerDirty = Yl, n !== Yl && (!r && n === Wl && (r = this._i.hoverlayer = this._ensureLayer(xg)), !!r)) {
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
    return this._ensureLayer(xg);
  }, t.prototype.paintOne = function(e, r) {
    dc(e, r);
  }, t.prototype._paintList = function(e, r, n) {
    if (this._redrawId === n) {
      var i = this._doPaintList(e, r);
      if (this._needsManuallyCompositing && this._compositeManually(), i)
        ke(this._i, function(o) {
          o.afterBrush && o.afterBrush();
        }, Lo), this._paintHoverList(e);
      else {
        var a = this;
        is(function() {
          a._paintList(e, r, n);
        });
      }
    }
  }, t.prototype._compositeManually = function() {
    var e = this._ensureLayer(ln).ctx, r = this._domRoot.width, n = this._domRoot.height;
    e.clearRect(0, 0, r, n), ke(this._i, function(i) {
      i.virtual && e.drawImage(i.dom, 0, 0, r, n);
    }, fa);
  }, t.prototype._doPaintList = function(e, r) {
    var n = this, i = !0;
    return ke(this._i, function(a) {
      var o = !1;
      if (Po(a, function(h) {
        (h.drawIdx < h.endIdx || h.notClearIdx >= 0) && (o = !0);
      }), !(!o && !a.__dirty)) {
        var s = n._opts.useDirtyRect && !Ul(a) ? a.createRepaintRects(e, r, n._width, n._height) : null, u = n._i.layerStack[0], l = !0;
        if (a.__dirty) {
          l = !1, a.__dirty = !1;
          var f = a.zlevel === u.zl && a.zlevel2 === u.zl2 ? n._backgroundColor : null;
          a.clear(!1, f, s);
        }
        Po(a, function(h) {
          var v = n._paintPerCursor(a, h, e, s, l);
          i = i && v;
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
    }, s = e.ctx, u = Ul(e), l = u && Le.getTime(), f = r.drawIdx, h = r.notClearIdx, v = h >= 0 ? Math.min(h, f) : f; v < r.endIdx; v++) {
      var c = n[v];
      if (!(v < f && !c.notClear)) {
        if (c.__inHover && (this._hoverLayerDirty = Wl), i != null) {
          var d = c.getPaintRect();
          d && d.intersect(i) && (pn(s, c, o), c.setPrevPaintRect(d));
        } else
          pn(s, c, o);
        if (u) {
          var y = Le.getTime() - l;
          if (y > 15) {
            v++;
            break;
          }
        }
      }
    }
    ai(s, o), r.drawIdx = Math.max(v, f);
  }, t.prototype.getLayer = function(e, r) {
    return this._ensureLayer(e, 0, r);
  }, t.prototype._ensureLayer = function(e, r, n) {
    r = r || 0;
    var i = this._singleCanvas;
    i && !this._needsManuallyCompositing && (e = ln, r = 0);
    var a = Xl(this._i, e)[r];
    return a || (a = Mg("zr_" + e + "." + r, this, e, r), this._layerConfig[e] && pe(a, this._layerConfig[e], !0), (n || i && e !== ln) && (a.virtual = !0), this._insertLayer(a, e, r, !1), a.initContext()), a;
  }, t.prototype.insertLayer = function(e, r) {
    this._insertLayer(r, e, 0, !1);
  }, t.prototype._insertLayer = function(e, r, n, i) {
    var a = this._i, o = a.layers, s = a.layerStack, u = this._domRoot, l = null;
    if (!(o[r] && o[r][n]) && EE(e)) {
      for (var f = s.length, h = 0; h < f && (s[h].zl < r || s[h].zl === r && s[h].zl2 < n); )
        h++;
      if (h > 0 && (l = Xl(a, s[h - 1].zl)[s[h - 1].zl2]), s.splice(h, 0, { zl: r, zl2: n }), Xl(a, r)[n] = e, !i && !e.virtual)
        if (l) {
          var v = l.dom;
          v.nextSibling ? u.insertBefore(e.dom, v.nextSibling) : u.appendChild(e.dom);
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
    }, fa);
  }, t.prototype.eachOtherLayer = function(e, r) {
    return ke(this._i, function(n, i) {
      e.call(r, n, i);
    }, uh);
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
      var a = e[f], v = a.zlevel, c = a.incremental, d = void 0;
      if (o !== v && (o = v, l = !1), c ? (l = !0, d = BC) : d = l ? kC : Uo, (!s || v !== s.zlevel || d !== s.zlevel2) && (s = n._ensureLayer(v, d), u = null, !s.__builtin__)) {
        Vs("ZLevel " + v + " has been used by unknown layer " + s.id);
        continue;
      }
      if ((!u || c !== u.key) && (u = kE(s, c), !u.used))
        if (u.used = !0, !r && u.first === a.id) {
          var y = f - u.startIdx;
          u.startIdx = f, u.drawIdx += y, u.endIdx += y;
        } else
          s.__dirty = !0, u.first = a.id, u.startIdx = u.drawIdx = f, u.endIdx = f + 1;
      u.endIdxNew = f + 1, a.__dirty & Je && !a.__inHover && ((!c || !a.notClear && f < u.drawIdx) && (s.__dirty = !0), c && a.notClear && u.notClearIdx < 0 && (u.notClearIdx = f));
    }
    ke(n._i, function(p) {
      for (var g = p.__cursorStack, m = p.__cursors, _ = g.length - 1; _ >= 0; _--) {
        var S = m.get(g[_]);
        if (!S.used)
          p.__dirty = !0, m.removeKey(g[_]), g.splice(_, 1);
        else {
          var b = S.endIdxNew;
          (Ul(p) ? b < S.drawIdx : b !== S.endIdx || !b || e[b - 1].id !== S.last) && (p.__dirty = !0), S.endIdx = S.endIdxNew, S.last = b ? e[b - 1].id : NaN;
        }
      }
      p.__dirty && (Po(p, function(w) {
        w.drawIdx = w.startIdx;
      }), n._hoverLayerDirty === Yl && (n._hoverLayerDirty = LE));
    }, Lo);
  }, t.prototype.clear = function() {
    return ke(this._i, function(e) {
      e.clear(), MS(e);
    }, fa), this;
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
    T(this._i.layers[e], function(r) {
      r && !r.__builtin__ && r.clear();
    });
  }, t.prototype.dispose = function() {
    this.root.innerHTML = "", this.root = this.storage = this._domRoot = this._i = null;
  }, t.prototype.getRenderedCanvas = function(e) {
    if (e = e || {}, this._singleCanvas && !this._compositeManually)
      return this._i.layers[ln][0].dom;
    var r = new xS("image", this, e.pixelRatio || this.dpr);
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
function xR(t) {
  t.registerPainter("canvas", BE);
}
export {
  HC as $,
  GP as A,
  Fe as B,
  ae as C,
  hR as D,
  Dt as E,
  T as F,
  qe as G,
  et as H,
  ge as I,
  fA as J,
  $t as K,
  tR as L,
  uw as M,
  Hw as N,
  vx as O,
  ye as P,
  Hy as Q,
  Re as R,
  nu as S,
  ve as T,
  eu as U,
  H as V,
  hi as W,
  e_ as X,
  Fh as Y,
  mi as Z,
  z as _,
  Ie as a,
  Rs as a$,
  YE as a0,
  Wh as a1,
  ut as a2,
  he as a3,
  Wt as a4,
  R as a5,
  WE as a6,
  ex as a7,
  XE as a8,
  GE as a9,
  GA as aA,
  jE as aB,
  zE as aC,
  fw as aD,
  Zs as aE,
  Dn as aF,
  hr as aG,
  fR as aH,
  Gx as aI,
  _s as aJ,
  F as aK,
  oe as aL,
  RA as aM,
  OA as aN,
  UA as aO,
  lR as aP,
  $E as aQ,
  D0 as aR,
  me as aS,
  cR as aT,
  eR as aU,
  mR as aV,
  nL as aW,
  l1 as aX,
  Ef as aY,
  Pc as aZ,
  pi as a_,
  UE as aa,
  HE as ab,
  vc as ac,
  L0 as ad,
  P0 as ae,
  ur as af,
  Lt as ag,
  YA as ah,
  _e as ai,
  pe as aj,
  Ss as ak,
  Ep as al,
  lS as am,
  EA as an,
  z0 as ao,
  yi as ap,
  Y as aq,
  ot as ar,
  SR as as,
  Sx as at,
  nP as au,
  Su as av,
  SA as aw,
  Wy as ax,
  Th as ay,
  Va as az,
  N as b,
  FE as b$,
  Hm as b0,
  Vm as b1,
  tw as b2,
  G as b3,
  _R as b4,
  QE as b5,
  Fp as b6,
  JE as b7,
  kr as b8,
  uR as b9,
  DL as bA,
  OL as bB,
  bR as bC,
  kL as bD,
  EL as bE,
  Ys as bF,
  ns as bG,
  Gh as bH,
  Er as bI,
  wt as bJ,
  zy as bK,
  Z as bL,
  Zm as bM,
  ce as bN,
  je as bO,
  Cn as bP,
  fu as bQ,
  iu as bR,
  Na as bS,
  Qb as bT,
  Ze as bU,
  ZE as bV,
  KE as bW,
  h1 as bX,
  EM as bY,
  Et as bZ,
  $e as b_,
  yR as ba,
  oR as bb,
  _A as bc,
  HA as bd,
  ha as be,
  aL as bf,
  oL as bg,
  de as bh,
  XC as bi,
  ML as bj,
  Pn as bk,
  Vh as bl,
  lt as bm,
  qI as bn,
  Te as bo,
  $x as bp,
  JC as bq,
  VE as br,
  nR as bs,
  hw as bt,
  ze as bu,
  Ye as bv,
  di as bw,
  JL as bx,
  IL as by,
  AL as bz,
  X as c,
  VI as c$,
  pR as c0,
  CR as c1,
  TR as c2,
  wR as c3,
  xR as c4,
  rd as c5,
  ou as c6,
  Or as c7,
  ue as c8,
  py as c9,
  WP as cA,
  XP as cB,
  xP as cC,
  zI as cD,
  mc as cE,
  FI as cF,
  GI as cG,
  CP as cH,
  pP as cI,
  dc as cJ,
  G1 as cK,
  wP as cL,
  Zp as cM,
  Ln as cN,
  b0 as cO,
  HI as cP,
  w0 as cQ,
  Tc as cR,
  $h as cS,
  T0 as cT,
  S0 as cU,
  Sc as cV,
  bc as cW,
  _c as cX,
  C0 as cY,
  mu as cZ,
  Fr as c_,
  C1 as ca,
  vi as cb,
  ab as cc,
  rb as cd,
  to as ce,
  Ea as cf,
  Jp as cg,
  xe as ch,
  vR as ci,
  dR as cj,
  QP as ck,
  ia as cl,
  qE as cm,
  _0 as cn,
  OI as co,
  f0 as cp,
  yb as cq,
  kI as cr,
  UI as cs,
  yI as ct,
  BI as cu,
  m0 as cv,
  NI as cw,
  W as cx,
  qP as cy,
  YP as cz,
  aR as d,
  ry as d0,
  TP as d1,
  MP as d2,
  x1 as d3,
  gI as d4,
  p1 as d5,
  Kb as d6,
  st as d7,
  hP as d8,
  ac as d9,
  Ue as da,
  nc as db,
  g_ as dc,
  Ex as dd,
  tA as de,
  gf as df,
  of as dg,
  gu as dh,
  Yy as di,
  ph as dj,
  cP as dk,
  vP as dl,
  U as e,
  bh as f,
  vr as g,
  Ec as h,
  B as i,
  kM as j,
  XA as k,
  zp as l,
  yr as m,
  wu as n,
  sR as o,
  OM as p,
  sc as q,
  bs as r,
  wa as s,
  j as t,
  iR as u,
  gR as v,
  R0 as w,
  cD as x,
  Bw as y,
  rR as z
};
