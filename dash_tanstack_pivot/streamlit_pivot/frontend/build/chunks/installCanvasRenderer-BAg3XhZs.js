import { b as G_, r as V_, g as W_ } from "./charts-DthW1Q8W.js";
var aa = {}, Pu = function(e, t) {
  return Pu = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, Pu(e, t);
};
function md(e, t) {
  if (typeof t != "function" && t !== null)
    throw new TypeError("Class extends value " + String(t) + " is not a constructor or null");
  Pu(e, t);
  function r() {
    this.constructor = e;
  }
  e.prototype = t === null ? Object.create(t) : (r.prototype = t.prototype, new r());
}
var ro = function() {
  return ro = Object.assign || function(t) {
    for (var r, n = 1, i = arguments.length; n < i; n++) {
      r = arguments[n];
      for (var a in r) Object.prototype.hasOwnProperty.call(r, a) && (t[a] = r[a]);
    }
    return t;
  }, ro.apply(this, arguments);
};
function Sd(e, t) {
  var r = {};
  for (var n in e) Object.prototype.hasOwnProperty.call(e, n) && t.indexOf(n) < 0 && (r[n] = e[n]);
  if (e != null && typeof Object.getOwnPropertySymbols == "function")
    for (var i = 0, n = Object.getOwnPropertySymbols(e); i < n.length; i++)
      t.indexOf(n[i]) < 0 && Object.prototype.propertyIsEnumerable.call(e, n[i]) && (r[n[i]] = e[n[i]]);
  return r;
}
function wd(e, t, r, n) {
  var i = arguments.length, a = i < 3 ? t : n === null ? n = Object.getOwnPropertyDescriptor(t, r) : n, o;
  if (typeof Reflect == "object" && typeof Reflect.decorate == "function") a = Reflect.decorate(e, t, r, n);
  else for (var s = e.length - 1; s >= 0; s--) (o = e[s]) && (a = (i < 3 ? o(a) : i > 3 ? o(t, r, a) : o(t, r)) || a);
  return i > 3 && a && Object.defineProperty(t, r, a), a;
}
function bd(e, t) {
  return function(r, n) {
    t(r, n, e);
  };
}
function Td(e, t, r, n, i, a) {
  function o(g) {
    if (g !== void 0 && typeof g != "function") throw new TypeError("Function expected");
    return g;
  }
  for (var s = n.kind, u = s === "getter" ? "get" : s === "setter" ? "set" : "value", l = !t && e ? n.static ? e : e.prototype : null, f = t || (l ? Object.getOwnPropertyDescriptor(l, n.name) : {}), h, v = !1, c = r.length - 1; c >= 0; c--) {
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
function xd(e, t, r) {
  for (var n = arguments.length > 2, i = 0; i < t.length; i++)
    r = n ? t[i].call(e, r) : t[i].call(e);
  return n ? r : void 0;
}
function Cd(e) {
  return typeof e == "symbol" ? e : "".concat(e);
}
function Md(e, t, r) {
  return typeof t == "symbol" && (t = t.description ? "[".concat(t.description, "]") : ""), Object.defineProperty(e, "name", { configurable: !0, value: r ? "".concat(r, " ", t) : t });
}
function Dd(e, t) {
  if (typeof Reflect == "object" && typeof Reflect.metadata == "function") return Reflect.metadata(e, t);
}
function Pd(e, t, r, n) {
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
    l((n = n.apply(e, t || [])).next());
  });
}
function Id(e, t) {
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
      l = t.call(e, r);
    } catch (f) {
      l = [6, f], i = 0;
    } finally {
      n = a = 0;
    }
    if (l[0] & 5) throw l[1];
    return { value: l[0] ? l[1] : void 0, done: !0 };
  }
}
var Ao = Object.create ? (function(e, t, r, n) {
  n === void 0 && (n = r);
  var i = Object.getOwnPropertyDescriptor(t, r);
  (!i || ("get" in i ? !t.__esModule : i.writable || i.configurable)) && (i = { enumerable: !0, get: function() {
    return t[r];
  } }), Object.defineProperty(e, n, i);
}) : (function(e, t, r, n) {
  n === void 0 && (n = r), e[n] = t[r];
});
function Ad(e, t) {
  for (var r in e) r !== "default" && !Object.prototype.hasOwnProperty.call(t, r) && Ao(t, e, r);
}
function no(e) {
  var t = typeof Symbol == "function" && Symbol.iterator, r = t && e[t], n = 0;
  if (r) return r.call(e);
  if (e && typeof e.length == "number") return {
    next: function() {
      return e && n >= e.length && (e = void 0), { value: e && e[n++], done: !e };
    }
  };
  throw new TypeError(t ? "Object is not iterable." : "Symbol.iterator is not defined.");
}
function Ll(e, t) {
  var r = typeof Symbol == "function" && e[Symbol.iterator];
  if (!r) return e;
  var n = r.call(e), i, a = [], o;
  try {
    for (; (t === void 0 || t-- > 0) && !(i = n.next()).done; ) a.push(i.value);
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
function Ld() {
  for (var e = [], t = 0; t < arguments.length; t++)
    e = e.concat(Ll(arguments[t]));
  return e;
}
function Rd() {
  for (var e = 0, t = 0, r = arguments.length; t < r; t++) e += arguments[t].length;
  for (var n = Array(e), i = 0, t = 0; t < r; t++)
    for (var a = arguments[t], o = 0, s = a.length; o < s; o++, i++)
      n[i] = a[o];
  return n;
}
function Ed(e, t, r) {
  if (r || arguments.length === 2) for (var n = 0, i = t.length, a; n < i; n++)
    (a || !(n in t)) && (a || (a = Array.prototype.slice.call(t, 0, n)), a[n] = t[n]);
  return e.concat(a || Array.prototype.slice.call(t));
}
function Pn(e) {
  return this instanceof Pn ? (this.v = e, this) : new Pn(e);
}
function Od(e, t, r) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var n = r.apply(e, t || []), i, a = [];
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
    c.value instanceof Pn ? Promise.resolve(c.value.v).then(f, h) : v(a[0][2], c);
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
function kd(e) {
  var t, r;
  return t = {}, n("next"), n("throw", function(i) {
    throw i;
  }), n("return"), t[Symbol.iterator] = function() {
    return this;
  }, t;
  function n(i, a) {
    t[i] = e[i] ? function(o) {
      return (r = !r) ? { value: Pn(e[i](o)), done: !1 } : a ? a(o) : o;
    } : a;
  }
}
function Bd(e) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var t = e[Symbol.asyncIterator], r;
  return t ? t.call(e) : (e = typeof no == "function" ? no(e) : e[Symbol.iterator](), r = {}, n("next"), n("throw"), n("return"), r[Symbol.asyncIterator] = function() {
    return this;
  }, r);
  function n(a) {
    r[a] = e[a] && function(o) {
      return new Promise(function(s, u) {
        o = e[a](o), i(s, u, o.done, o.value);
      });
    };
  }
  function i(a, o, s, u) {
    Promise.resolve(u).then(function(l) {
      a({ value: l, done: s });
    }, o);
  }
}
function Fd(e, t) {
  return Object.defineProperty ? Object.defineProperty(e, "raw", { value: t }) : e.raw = t, e;
}
var U_ = Object.create ? (function(e, t) {
  Object.defineProperty(e, "default", { enumerable: !0, value: t });
}) : function(e, t) {
  e.default = t;
}, Iu = function(e) {
  return Iu = Object.getOwnPropertyNames || function(t) {
    var r = [];
    for (var n in t) Object.prototype.hasOwnProperty.call(t, n) && (r[r.length] = n);
    return r;
  }, Iu(e);
};
function Nd(e) {
  if (e && e.__esModule) return e;
  var t = {};
  if (e != null) for (var r = Iu(e), n = 0; n < r.length; n++) r[n] !== "default" && Ao(t, e, r[n]);
  return U_(t, e), t;
}
function zd(e) {
  return e && e.__esModule ? e : { default: e };
}
function Hd(e, t, r, n) {
  if (r === "a" && !n) throw new TypeError("Private accessor was defined without a getter");
  if (typeof t == "function" ? e !== t || !n : !t.has(e)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
  return r === "m" ? n : r === "a" ? n.call(e) : n ? n.value : t.get(e);
}
function Gd(e, t, r, n, i) {
  if (n === "m") throw new TypeError("Private method is not writable");
  if (n === "a" && !i) throw new TypeError("Private accessor was defined without a setter");
  if (typeof t == "function" ? e !== t || !i : !t.has(e)) throw new TypeError("Cannot write private member to an object whose class did not declare it");
  return n === "a" ? i.call(e, r) : i ? i.value = r : t.set(e, r), r;
}
function Vd(e, t) {
  if (t === null || typeof t != "object" && typeof t != "function") throw new TypeError("Cannot use 'in' operator on non-object");
  return typeof e == "function" ? t === e : e.has(t);
}
function Wd(e, t, r) {
  if (t != null) {
    if (typeof t != "object" && typeof t != "function") throw new TypeError("Object expected.");
    var n, i;
    if (r) {
      if (!Symbol.asyncDispose) throw new TypeError("Symbol.asyncDispose is not defined.");
      n = t[Symbol.asyncDispose];
    }
    if (n === void 0) {
      if (!Symbol.dispose) throw new TypeError("Symbol.dispose is not defined.");
      n = t[Symbol.dispose], r && (i = n);
    }
    if (typeof n != "function") throw new TypeError("Object not disposable.");
    i && (n = function() {
      try {
        i.call(this);
      } catch (a) {
        return Promise.reject(a);
      }
    }), e.stack.push({ value: t, dispose: n, async: r });
  } else r && e.stack.push({ async: !0 });
  return t;
}
var Y_ = typeof SuppressedError == "function" ? SuppressedError : function(e, t, r) {
  var n = new Error(r);
  return n.name = "SuppressedError", n.error = e, n.suppressed = t, n;
};
function Ud(e) {
  function t(a) {
    e.error = e.hasError ? new Y_(a, e.error, "An error was suppressed during disposal.") : a, e.hasError = !0;
  }
  var r, n = 0;
  function i() {
    for (; r = e.stack.pop(); )
      try {
        if (!r.async && n === 1) return n = 0, e.stack.push(r), Promise.resolve().then(i);
        if (r.dispose) {
          var a = r.dispose.call(r.value);
          if (r.async) return n |= 2, Promise.resolve(a).then(i, function(o) {
            return t(o), i();
          });
        } else n |= 1;
      } catch (o) {
        t(o);
      }
    if (n === 1) return e.hasError ? Promise.reject(e.error) : Promise.resolve();
    if (e.hasError) throw e.error;
  }
  return i();
}
function Yd(e, t) {
  return typeof e == "string" && /^\.\.?\//.test(e) ? e.replace(/\.(tsx)$|((?:\.d)?)((?:\.[^./]+?)?)\.([cm]?)ts$/i, function(r, n, i, a, o) {
    return n ? t ? ".jsx" : ".js" : i && (!a || !o) ? r : i + a + "." + o.toLowerCase() + "js";
  }) : e;
}
const q_ = {
  __extends: md,
  __assign: ro,
  __rest: Sd,
  __decorate: wd,
  __param: bd,
  __esDecorate: Td,
  __runInitializers: xd,
  __propKey: Cd,
  __setFunctionName: Md,
  __metadata: Dd,
  __awaiter: Pd,
  __generator: Id,
  __createBinding: Ao,
  __exportStar: Ad,
  __values: no,
  __read: Ll,
  __spread: Ld,
  __spreadArrays: Rd,
  __spreadArray: Ed,
  __await: Pn,
  __asyncGenerator: Od,
  __asyncDelegator: kd,
  __asyncValues: Bd,
  __makeTemplateObject: Fd,
  __importStar: Nd,
  __importDefault: zd,
  __classPrivateFieldGet: Hd,
  __classPrivateFieldSet: Gd,
  __classPrivateFieldIn: Vd,
  __addDisposableResource: Wd,
  __disposeResources: Ud,
  __rewriteRelativeImportExtension: Yd
}, X_ = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  __addDisposableResource: Wd,
  get __assign() {
    return ro;
  },
  __asyncDelegator: kd,
  __asyncGenerator: Od,
  __asyncValues: Bd,
  __await: Pn,
  __awaiter: Pd,
  __classPrivateFieldGet: Hd,
  __classPrivateFieldIn: Vd,
  __classPrivateFieldSet: Gd,
  __createBinding: Ao,
  __decorate: wd,
  __disposeResources: Ud,
  __esDecorate: Td,
  __exportStar: Ad,
  __extends: md,
  __generator: Id,
  __importDefault: zd,
  __importStar: Nd,
  __makeTemplateObject: Fd,
  __metadata: Dd,
  __param: bd,
  __propKey: Cd,
  __read: Ll,
  __rest: Sd,
  __rewriteRelativeImportExtension: Yd,
  __runInitializers: xd,
  __setFunctionName: Md,
  __spread: Ld,
  __spreadArray: Ed,
  __spreadArrays: Rd,
  __values: no,
  default: q_
}, Symbol.toStringTag, { value: "Module" })), qd = /* @__PURE__ */ G_(X_);
var Oe = {}, ke = {}, cs = {}, Zf;
function $_() {
  return Zf || (Zf = 1, (function(e) {
    Object.defineProperty(e, "__esModule", {
      value: !0
    }), e.default = void 0;
    var t = 1;
    e.default = function() {
      return "".concat(t++);
    };
  })(cs)), cs;
}
var Fn = {}, Nn = {}, vs = {}, jf;
function Xd() {
  return jf || (jf = 1, (function(e) {
    Object.defineProperty(e, "__esModule", {
      value: !0
    }), e.default = void 0, e.default = function(r) {
      var n = arguments.length > 1 && arguments[1] !== void 0 ? arguments[1] : 60, i = null;
      return function() {
        for (var a = this, o = arguments.length, s = new Array(o), u = 0; u < o; u++)
          s[u] = arguments[u];
        clearTimeout(i), i = setTimeout(function() {
          r.apply(a, s);
        }, n);
      };
    };
  })(vs)), vs;
}
var Be = {}, Kf;
function Rl() {
  return Kf || (Kf = 1, Object.defineProperty(Be, "__esModule", {
    value: !0
  }), Be.SizeSensorId = Be.SensorTabIndex = Be.SensorClassName = void 0, Be.SizeSensorId = "size-sensor-id", Be.SensorClassName = "size-sensor-object", Be.SensorTabIndex = "-1"), Be;
}
var Qf;
function Z_() {
  if (Qf) return Nn;
  Qf = 1, Object.defineProperty(Nn, "__esModule", {
    value: !0
  }), Nn.createSensor = void 0;
  var e = r(Xd()), t = Rl();
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return Nn.createSensor = function(i, a) {
    var o = void 0, s = [], u = function() {
      getComputedStyle(i).position === "static" && (i.style.position = "relative");
      var d = document.createElement("object");
      return d.onload = function() {
        d.contentDocument.defaultView.addEventListener("resize", l), l();
      }, d.style.display = "block", d.style.position = "absolute", d.style.top = "0", d.style.left = "0", d.style.height = "100%", d.style.width = "100%", d.style.overflow = "hidden", d.style.pointerEvents = "none", d.style.zIndex = "-1", d.style.opacity = "0", d.setAttribute("class", t.SensorClassName), d.setAttribute("tabindex", t.SensorTabIndex), d.type = "text/html", i.appendChild(d), d.data = "about:blank", d;
    }, l = (0, e.default)(function() {
      s.forEach(function(c) {
        c(i);
      });
    }), f = function(d) {
      o || (o = u()), s.indexOf(d) === -1 && s.push(d);
    }, h = function() {
      o && o.parentNode && (o.contentDocument && o.contentDocument.defaultView.removeEventListener("resize", l), o.parentNode.removeChild(o), i.removeAttribute(t.SizeSensorId), o = void 0, s = [], a && a());
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
  }, Nn;
}
var zn = {}, Jf;
function j_() {
  if (Jf) return zn;
  Jf = 1, Object.defineProperty(zn, "__esModule", {
    value: !0
  }), zn.createSensor = void 0;
  var e = Rl(), t = r(Xd());
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return zn.createSensor = function(i, a) {
    var o = void 0, s = [], u = (0, t.default)(function() {
      s.forEach(function(c) {
        c(i);
      });
    }), l = function() {
      var d = new ResizeObserver(u);
      return d.observe(i), u(), d;
    }, f = function(d) {
      o || (o = l()), s.indexOf(d) === -1 && s.push(d);
    }, h = function() {
      o && o.disconnect(), s = [], o = void 0, i.removeAttribute(e.SizeSensorId), a && a();
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
  }, zn;
}
var th;
function K_() {
  if (th) return Fn;
  th = 1, Object.defineProperty(Fn, "__esModule", {
    value: !0
  }), Fn.createSensor = void 0;
  var e = Z_(), t = j_();
  return Fn.createSensor = typeof ResizeObserver < "u" ? t.createSensor : e.createSensor, Fn;
}
var eh;
function Q_() {
  if (eh) return ke;
  eh = 1, Object.defineProperty(ke, "__esModule", {
    value: !0
  }), ke.removeSensor = ke.getSensor = ke.Sensors = void 0;
  var e = n($_()), t = K_(), r = Rl();
  function n(o) {
    return o && o.__esModule ? o : { default: o };
  }
  var i = ke.Sensors = {};
  function a(o) {
    o && i[o] && delete i[o];
  }
  return ke.getSensor = function(s) {
    var u = s.getAttribute(r.SizeSensorId);
    if (u && i[u])
      return i[u];
    var l = (0, e.default)();
    s.setAttribute(r.SizeSensorId, l);
    var f = (0, t.createSensor)(s, function() {
      return a(l);
    });
    return i[l] = f, f;
  }, ke.removeSensor = function(s) {
    var u = s.element.getAttribute(r.SizeSensorId);
    s.destroy(), a(u);
  }, ke;
}
var rh;
function J_() {
  if (rh) return Oe;
  rh = 1, Object.defineProperty(Oe, "__esModule", {
    value: !0
  }), Oe.ver = Oe.clear = Oe.bind = void 0;
  var e = Q_();
  return Oe.bind = function(r, n) {
    var i = (0, e.getSensor)(r);
    return i.bind(n), function() {
      i.unbind(n);
    };
  }, Oe.clear = function(r) {
    var n = (0, e.getSensor)(r);
    (0, e.removeSensor)(n);
  }, Oe.ver = "1.0.3", Oe;
}
var Hn = {}, nh;
function t0() {
  if (nh) return Hn;
  nh = 1, Object.defineProperty(Hn, "__esModule", { value: !0 }), Hn.pick = void 0;
  function e(t, r) {
    var n = {};
    return r.forEach(function(i) {
      n[i] = t[i];
    }), n;
  }
  return Hn.pick = e, Hn;
}
var Gn = {}, ih;
function e0() {
  if (ih) return Gn;
  ih = 1, Object.defineProperty(Gn, "__esModule", { value: !0 }), Gn.isFunction = void 0;
  function e(t) {
    return typeof t == "function";
  }
  return Gn.isFunction = e, Gn;
}
var Vn = {}, ah;
function r0() {
  if (ah) return Vn;
  ah = 1, Object.defineProperty(Vn, "__esModule", { value: !0 }), Vn.isString = void 0;
  function e(t) {
    return typeof t == "string";
  }
  return Vn.isString = e, Vn;
}
var Wn = {}, ds, oh;
function n0() {
  return oh || (oh = 1, ds = function e(t, r) {
    if (t === r) return !0;
    if (t && r && typeof t == "object" && typeof r == "object") {
      if (t.constructor !== r.constructor) return !1;
      var n, i, a;
      if (Array.isArray(t)) {
        if (n = t.length, n != r.length) return !1;
        for (i = n; i-- !== 0; )
          if (!e(t[i], r[i])) return !1;
        return !0;
      }
      if (t.constructor === RegExp) return t.source === r.source && t.flags === r.flags;
      if (t.valueOf !== Object.prototype.valueOf) return t.valueOf() === r.valueOf();
      if (t.toString !== Object.prototype.toString) return t.toString() === r.toString();
      if (a = Object.keys(t), n = a.length, n !== Object.keys(r).length) return !1;
      for (i = n; i-- !== 0; )
        if (!Object.prototype.hasOwnProperty.call(r, a[i])) return !1;
      for (i = n; i-- !== 0; ) {
        var o = a[i];
        if (!e(t[o], r[o])) return !1;
      }
      return !0;
    }
    return t !== t && r !== r;
  }), ds;
}
var sh;
function i0() {
  if (sh) return Wn;
  sh = 1, Object.defineProperty(Wn, "__esModule", { value: !0 }), Wn.isEqual = void 0;
  var e = qd, t = e.__importDefault(n0());
  return Wn.isEqual = t.default, Wn;
}
var uh;
function a0() {
  if (uh) return aa;
  uh = 1, Object.defineProperty(aa, "__esModule", { value: !0 });
  var e = qd, t = e.__importStar(V_()), r = J_(), n = t0(), i = e0(), a = r0(), o = i0(), s = (
    /** @class */
    (function(u) {
      e.__extends(l, u);
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
        return e.__awaiter(this, void 0, void 0, function() {
          var f = this;
          return e.__generator(this, function(h) {
            return [2, new Promise(function(v) {
              f.echarts.init(f.ele, f.props.theme, f.props.opts);
              var c = f.getEchartsInstance();
              c.on("finished", function() {
                var d = f.ele.clientWidth, y = f.ele.clientHeight;
                f.echarts.dispose(f.ele);
                var p = e.__assign({ width: d, height: y }, f.props.opts);
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
        return e.__awaiter(this, void 0, void 0, function() {
          var f, h, v, c, d, y, p = this;
          return e.__generator(this, function(g) {
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
            var g = function(_) {
              p(_, f);
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
        var f = this.props, h = f.option, v = f.notMerge, c = v === void 0 ? !1 : v, d = f.replaceMerge, y = d === void 0 ? null : d, p = f.lazyUpdate, g = p === void 0 ? !1 : p, _ = f.showLoading, m = f.loadingOption, S = m === void 0 ? null : m, b = this.getEchartsInstance();
        return b.setOption(h, { notMerge: c, replaceMerge: y, lazyUpdate: g }), _ ? b.showLoading(S) : b.hideLoading(), b;
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
        var y = e.__rest(h, ["style", "className", "echarts", "option", "theme", "notMerge", "replaceMerge", "lazyUpdate", "showLoading", "loadingOption", "opts", "onChartReady", "onEvents", "shouldSetOption", "autoResize"]), p = e.__assign({ height: 300 }, v);
        return t.default.createElement("div", e.__assign({ ref: function(g) {
          f.ele = g;
        }, style: p, className: "echarts-for-react ".concat(d) }, y));
      }, l;
    })(t.PureComponent)
  );
  return aa.default = s, aa;
}
var o0 = a0();
const QD = /* @__PURE__ */ W_(o0);
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
var Au = function(e, t) {
  return Au = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, Au(e, t);
};
function lt(e, t) {
  if (typeof t != "function" && t !== null)
    throw new TypeError("Class extends value " + String(t) + " is not a constructor or null");
  Au(e, t);
  function r() {
    this.constructor = e;
  }
  e.prototype = t === null ? Object.create(t) : (r.prototype = t.prototype, new r());
}
var s0 = /* @__PURE__ */ (function() {
  function e() {
    this.firefox = !1, this.ie = !1, this.edge = !1, this.newEdge = !1, this.weChat = !1;
  }
  return e;
})(), u0 = /* @__PURE__ */ (function() {
  function e() {
    this.browser = new s0(), this.node = !1, this.wxa = !1, this.worker = !1, this.svgSupported = !1, this.touchEventsSupported = !1, this.pointerEventsSupported = !1, this.domSupported = !1, this.transformSupported = !1, this.transform3dSupported = !1, this.hasGlobalWindow = typeof window < "u";
  }
  return e;
})(), z = new u0();
typeof wx == "object" && typeof wx.getSystemInfoSync == "function" ? (z.wxa = !0, z.touchEventsSupported = !0) : typeof document > "u" && typeof self < "u" ? z.worker = !0 : !z.hasGlobalWindow || "Deno" in window ? (z.node = !0, z.svgSupported = !0) : l0(navigator.userAgent, z);
function l0(e, t) {
  var r = t.browser, n = e.match(/Firefox\/([\d.]+)/), i = e.match(/MSIE\s([\d.]+)/) || e.match(/Trident\/.+?rv:(([\d.]+))/), a = e.match(/Edge?\/([\d.]+)/), o = /micromessenger/i.test(e);
  n && (r.firefox = !0, r.version = n[1]), i && (r.ie = !0, r.version = i[1]), a && (r.edge = !0, r.version = a[1], r.newEdge = +a[1].split(".")[0] > 18), o && (r.weChat = !0), t.svgSupported = typeof SVGRect < "u", t.touchEventsSupported = "ontouchstart" in window && !r.ie && !r.edge, t.pointerEventsSupported = "onpointerdown" in window && (r.edge || r.ie && +r.version >= 11), t.domSupported = typeof document < "u";
  var s = document.documentElement.style;
  t.transform3dSupported = (r.ie && "transition" in s || r.edge || "WebKitCSSMatrix" in window && "m11" in new WebKitCSSMatrix() || "MozPerspective" in s) && !("OTransition" in s), t.transformSupported = t.transform3dSupported || r.ie && +r.version >= 9;
}
var El = 12, f0 = "sans-serif", jr = El + "px " + f0, h0 = 20, c0 = 100, v0 = "007LLmW'55;N0500LLLLLLLLLL00NNNLzWW\\\\WQb\\0FWLg\\bWb\\WQ\\WrWWQ000CL5LLFLL0LL**F*gLLLL5F0LF\\FFF5.5N";
function d0(e) {
  var t = {};
  if (typeof JSON > "u")
    return t;
  for (var r = 0; r < e.length; r++) {
    var n = String.fromCharCode(r + 32), i = (e.charCodeAt(r) - h0) / c0;
    t[n] = i;
  }
  return t;
}
var p0 = d0(v0), Ve = {
  createCanvas: function() {
    return typeof document < "u" && document.createElement("canvas");
  },
  measureText: /* @__PURE__ */ (function() {
    var e, t;
    return function(r, n) {
      if (!e) {
        var i = Ve.createCanvas();
        e = i && i.getContext("2d");
      }
      if (e)
        return t !== n && (t = e.font = n || jr), e.measureText(r);
      r = r || "", n = n || jr;
      var a = /((?:\d+)?\.?\d*)px/.exec(n), o = a && +a[1] || El, s = 0;
      if (n.indexOf("mono") >= 0)
        s = o * r.length;
      else
        for (var u = 0; u < r.length; u++) {
          var l = p0[r[u]];
          s += l == null ? o : l * o;
        }
      return { width: s };
    };
  })(),
  loadImage: function(e, t, r) {
    var n = new Image();
    return n.onload = t, n.onerror = r, n.src = e, n;
  }
};
function $d(e) {
  for (var t in Ve)
    e[t] && (Ve[t] = e[t]);
}
var Zd = Pe([
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
}, {}), jd = Pe([
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
}, {}), En = Object.prototype.toString, Lo = Array.prototype, g0 = Lo.forEach, y0 = Lo.filter, Ol = Lo.slice, _0 = Lo.map, lh = (function() {
}).constructor, oa = lh ? lh.prototype : null, kl = "__proto__", m0 = 2311;
function Bl() {
  return m0++;
}
function Ro() {
  for (var e = [], t = 0; t < arguments.length; t++)
    e[t] = arguments[t];
  typeof console < "u" && console.error.apply(console, e);
}
function $(e) {
  if (e == null || typeof e != "object")
    return e;
  var t = e, r = En.call(e);
  if (r === "[object Array]") {
    if (!Tn(e)) {
      t = [];
      for (var n = 0, i = e.length; n < i; n++)
        t[n] = $(e[n]);
    }
  } else if (jd[r]) {
    if (!Tn(e)) {
      var a = e.constructor;
      if (a.from)
        t = a.from(e);
      else {
        t = new a(e.length);
        for (var n = 0, i = e.length; n < i; n++)
          t[n] = e[n];
      }
    }
  } else if (!Zd[r] && !Tn(e) && !In(e)) {
    t = {};
    for (var o in e)
      e.hasOwnProperty(o) && o !== kl && (t[o] = $(e[o]));
  }
  return t;
}
function it(e, t, r) {
  if (!N(t) || !N(e))
    return r ? $(t) : e;
  for (var n in t)
    if (t.hasOwnProperty(n) && n !== kl) {
      var i = e[n], a = t[n];
      N(a) && N(i) && !k(a) && !k(i) && !In(a) && !In(i) && !Lu(a) && !Lu(i) && !Tn(a) && !Tn(i) ? it(i, a, r) : (r || !(n in e)) && (e[n] = $(t[n]));
    }
  return e;
}
function S0(e, t) {
  for (var r = e[0], n = 1, i = e.length; n < i; n++)
    r = it(r, e[n], t);
  return r;
}
function E(e, t) {
  if (Object.assign)
    Object.assign(e, t);
  else
    for (var r in t)
      t.hasOwnProperty(r) && r !== kl && (e[r] = t[r]);
  return e;
}
function ft(e, t, r) {
  for (var n = ct(t), i = 0, a = n.length; i < a; i++) {
    var o = n[i];
    (r ? t[o] != null : e[o] == null) && (e[o] = t[o]);
  }
  return e;
}
var w0 = Ve.createCanvas;
function rt(e, t) {
  if (e) {
    if (e.indexOf)
      return e.indexOf(t);
    for (var r = 0, n = e.length; r < n; r++)
      if (e[r] === t)
        return r;
  }
  return -1;
}
function Fl(e, t) {
  var r = e.prototype;
  function n() {
  }
  n.prototype = t.prototype, e.prototype = new n();
  for (var i in r)
    r.hasOwnProperty(i) && (e.prototype[i] = r[i]);
  e.prototype.constructor = e, e.superClass = t;
}
function ye(e, t, r) {
  if (e = "prototype" in e ? e.prototype : e, t = "prototype" in t ? t.prototype : t, Object.getOwnPropertyNames)
    for (var n = Object.getOwnPropertyNames(t), i = 0; i < n.length; i++) {
      var a = n[i];
      a !== "constructor" && (r ? t[a] != null : e[a] == null) && (e[a] = t[a]);
    }
  else
    ft(e, t, r);
}
function Ot(e) {
  return !e || typeof e == "string" ? !1 : typeof e.length == "number";
}
function x(e, t, r) {
  if (e && t)
    if (e.forEach && e.forEach === g0)
      e.forEach(t, r);
    else if (e.length === +e.length)
      for (var n = 0, i = e.length; n < i; n++)
        t.call(r, e[n], n, e);
    else
      for (var a in e)
        e.hasOwnProperty(a) && t.call(r, e[a], a, e);
}
function F(e, t, r) {
  if (!e)
    return [];
  if (!t)
    return Eo(e);
  if (e.map && e.map === _0)
    return e.map(t, r);
  for (var n = [], i = 0, a = e.length; i < a; i++)
    n.push(t.call(r, e[i], i, e));
  return n;
}
function Pe(e, t, r, n) {
  if (e && t) {
    for (var i = 0, a = e.length; i < a; i++)
      r = t.call(n, r, e[i], i, e);
    return r;
  }
}
function dt(e, t, r) {
  if (!e)
    return [];
  if (!t)
    return Eo(e);
  if (e.filter && e.filter === y0)
    return e.filter(t, r);
  for (var n = [], i = 0, a = e.length; i < a; i++)
    t.call(r, e[i], i, e) && n.push(e[i]);
  return n;
}
function b0(e, t, r) {
  if (e && t) {
    for (var n = 0, i = e.length; n < i; n++)
      if (t.call(r, e[n], n, e))
        return e[n];
  }
}
function ct(e) {
  if (!e)
    return [];
  if (Object.keys)
    return Object.keys(e);
  var t = [];
  for (var r in e)
    e.hasOwnProperty(r) && t.push(r);
  return t;
}
function T0(e, t) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return function() {
    return e.apply(t, r.concat(Ol.call(arguments)));
  };
}
var ot = oa && U(oa.bind) ? oa.call.bind(oa.bind) : T0;
function pt(e) {
  for (var t = [], r = 1; r < arguments.length; r++)
    t[r - 1] = arguments[r];
  return function() {
    return e.apply(this, t.concat(Ol.call(arguments)));
  };
}
function k(e) {
  return Array.isArray ? Array.isArray(e) : En.call(e) === "[object Array]";
}
function U(e) {
  return typeof e == "function";
}
function B(e) {
  return typeof e == "string";
}
function io(e) {
  return En.call(e) === "[object String]";
}
function ut(e) {
  return typeof e == "number";
}
function N(e) {
  var t = typeof e;
  return t === "function" || !!e && t === "object";
}
function Lu(e) {
  return !!Zd[En.call(e)];
}
function kt(e) {
  return !!jd[En.call(e)];
}
function In(e) {
  return typeof e == "object" && typeof e.nodeType == "number" && typeof e.ownerDocument == "object";
}
function qi(e) {
  return e.colorStops != null;
}
function Kd(e) {
  return e.image != null;
}
function x0(e) {
  return En.call(e) === "[object RegExp]";
}
function Ii(e) {
  return e !== e;
}
function An() {
  for (var e = [], t = 0; t < arguments.length; t++)
    e[t] = arguments[t];
  for (var r = 0, n = e.length; r < n; r++)
    if (e[r] != null)
      return e[r];
}
function Y(e, t) {
  return e ?? t;
}
function yi(e, t, r) {
  return e ?? t ?? r;
}
function Eo(e) {
  for (var t = [], r = 1; r < arguments.length; r++)
    t[r - 1] = arguments[r];
  return Ol.apply(e, t);
}
function Nl(e) {
  if (typeof e == "number")
    return [e, e, e, e];
  var t = e.length;
  return t === 2 ? [e[0], e[1], e[0], e[1]] : t === 3 ? [e[0], e[1], e[2], e[1]] : e;
}
function Ie(e, t) {
  if (!e)
    throw new Error(t);
}
function pe(e) {
  return e == null ? null : typeof e.trim == "function" ? e.trim() : e.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, "");
}
var Qd = "__ec_primitive__";
function Ai(e) {
  e[Qd] = !0;
}
function Tn(e) {
  return e[Qd];
}
var C0 = (function() {
  function e() {
    this.data = {};
  }
  return e.prototype.delete = function(t) {
    var r = this.has(t);
    return r && delete this.data[t], r;
  }, e.prototype.has = function(t) {
    return this.data.hasOwnProperty(t);
  }, e.prototype.get = function(t) {
    return this.data[t];
  }, e.prototype.set = function(t, r) {
    return this.data[t] = r, this;
  }, e.prototype.keys = function() {
    return ct(this.data);
  }, e.prototype.forEach = function(t) {
    var r = this.data;
    for (var n in r)
      r.hasOwnProperty(n) && t(r[n], n);
  }, e;
})(), Jd = typeof Map == "function";
function M0() {
  return Jd ? /* @__PURE__ */ new Map() : new C0();
}
var tp = (function() {
  function e(t) {
    var r = k(t);
    this.data = M0();
    var n = this;
    t instanceof e ? t.each(i) : t && x(t, i);
    function i(a, o) {
      r ? n.set(a, o) : n.set(o, a);
    }
  }
  return e.prototype.hasKey = function(t) {
    return this.data.has(t);
  }, e.prototype.get = function(t) {
    return this.data.get(t);
  }, e.prototype.set = function(t, r) {
    return this.data.set(t, r), r;
  }, e.prototype.each = function(t, r) {
    this.data.forEach(function(n, i) {
      t.call(r, n, i);
    });
  }, e.prototype.keys = function() {
    var t = this.data.keys();
    return Jd ? Array.from(t) : t;
  }, e.prototype.removeKey = function(t) {
    this.data.delete(t);
  }, e;
})();
function W(e) {
  return new tp(e);
}
function ep(e, t) {
  for (var r = new e.constructor(e.length + t.length), n = 0; n < e.length; n++)
    r[n] = e[n];
  for (var i = e.length, n = 0; n < t.length; n++)
    r[n + i] = t[n];
  return r;
}
function Xi(e, t) {
  var r;
  if (Object.create)
    r = Object.create(e);
  else {
    var n = function() {
    };
    n.prototype = e, r = new n();
  }
  return t && E(r, t), r;
}
function zl(e) {
  var t = e.style;
  t.webkitUserSelect = "none", t.userSelect = "none", t.webkitTapHighlightColor = "rgba(0,0,0,0)", t["-webkit-touch-callout"] = "none";
}
function We(e, t) {
  return e.hasOwnProperty(t);
}
function Lt() {
}
var rp = 180 / Math.PI;
const D0 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  HashMap: tp,
  RADIAN_TO_DEGREE: rp,
  assert: Ie,
  bind: ot,
  clone: $,
  concatArray: ep,
  createCanvas: w0,
  createHashMap: W,
  createObject: Xi,
  curry: pt,
  defaults: ft,
  disableUserSelect: zl,
  each: x,
  eqNaN: Ii,
  extend: E,
  filter: dt,
  find: b0,
  guid: Bl,
  hasOwn: We,
  indexOf: rt,
  inherits: Fl,
  isArray: k,
  isArrayLike: Ot,
  isBuiltInObject: Lu,
  isDom: In,
  isFunction: U,
  isGradientObject: qi,
  isImagePatternObject: Kd,
  isNumber: ut,
  isObject: N,
  isPrimitive: Tn,
  isRegExp: x0,
  isString: B,
  isStringSafe: io,
  isTypedArray: kt,
  keys: ct,
  logError: Ro,
  map: F,
  merge: it,
  mergeAll: S0,
  mixin: ye,
  noop: Lt,
  normalizeCssArray: Nl,
  reduce: Pe,
  retrieve: An,
  retrieve2: Y,
  retrieve3: yi,
  setAsPrimitive: Ai,
  slice: Eo,
  trim: pe
}, Symbol.toStringTag, { value: "Module" }));
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
var Ru = function(e, t) {
  return Ru = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, Ru(e, t);
};
function tt(e, t) {
  if (typeof t != "function" && t !== null)
    throw new TypeError("Class extends value " + String(t) + " is not a constructor or null");
  Ru(e, t);
  function r() {
    this.constructor = e;
  }
  e.prototype = t === null ? Object.create(t) : (r.prototype = t.prototype, new r());
}
function Jr(e, t) {
  return e == null && (e = 0), t == null && (t = 0), [e, t];
}
function P0(e, t) {
  return e[0] = t[0], e[1] = t[1], e;
}
function np(e) {
  return [e[0], e[1]];
}
function I0(e, t, r) {
  return e[0] = t, e[1] = r, e;
}
function Eu(e, t, r) {
  return e[0] = t[0] + r[0], e[1] = t[1] + r[1], e;
}
function A0(e, t, r, n) {
  return e[0] = t[0] + r[0] * n, e[1] = t[1] + r[1] * n, e;
}
function ip(e, t, r) {
  return e[0] = t[0] - r[0], e[1] = t[1] - r[1], e;
}
function Hl(e) {
  return Math.sqrt(Gl(e));
}
var L0 = Hl;
function Gl(e) {
  return e[0] * e[0] + e[1] * e[1];
}
var R0 = Gl;
function E0(e, t, r) {
  return e[0] = t[0] * r[0], e[1] = t[1] * r[1], e;
}
function O0(e, t, r) {
  return e[0] = t[0] / r[0], e[1] = t[1] / r[1], e;
}
function k0(e, t) {
  return e[0] * t[0] + e[1] * t[1];
}
function Va(e, t, r) {
  return e[0] = t[0] * r, e[1] = t[1] * r, e;
}
function ap(e, t) {
  var r = Hl(t);
  return r === 0 ? (e[0] = 0, e[1] = 0) : (e[0] = t[0] / r, e[1] = t[1] / r), e;
}
function ao(e, t) {
  return Math.sqrt((e[0] - t[0]) * (e[0] - t[0]) + (e[1] - t[1]) * (e[1] - t[1]));
}
var op = ao;
function sp(e, t) {
  return (e[0] - t[0]) * (e[0] - t[0]) + (e[1] - t[1]) * (e[1] - t[1]);
}
var Ur = sp;
function B0(e, t) {
  return e[0] = -t[0], e[1] = -t[1], e;
}
function F0(e, t, r, n) {
  return e[0] = t[0] + n * (r[0] - t[0]), e[1] = t[1] + n * (r[1] - t[1]), e;
}
function Yt(e, t, r) {
  var n = t[0], i = t[1];
  return e[0] = r[0] * n + r[2] * i + r[4], e[1] = r[1] * n + r[3] * i + r[5], e;
}
function er(e, t, r) {
  return e[0] = Math.min(t[0], r[0]), e[1] = Math.min(t[1], r[1]), e;
}
function rr(e, t, r) {
  return e[0] = Math.max(t[0], r[0]), e[1] = Math.max(t[1], r[1]), e;
}
const N0 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  add: Eu,
  applyTransform: Yt,
  clone: np,
  copy: P0,
  create: Jr,
  dist: op,
  distSquare: Ur,
  distance: ao,
  distanceSquare: sp,
  div: O0,
  dot: k0,
  len: Hl,
  lenSquare: Gl,
  length: L0,
  lengthSquare: R0,
  lerp: F0,
  max: rr,
  min: er,
  mul: E0,
  negate: B0,
  normalize: ap,
  scale: Va,
  scaleAndAdd: A0,
  set: I0,
  sub: ip
}, Symbol.toStringTag, { value: "Module" }));
var nn = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.target = t, this.topTarget = r && r.topTarget;
  }
  return e;
})(), z0 = (function() {
  function e(t) {
    this.handler = t, t.on("mousedown", this._dragStart, this), t.on("mousemove", this._drag, this), t.on("mouseup", this._dragEnd, this);
  }
  return e.prototype._dragStart = function(t) {
    for (var r = t.target; r && !r.draggable; )
      r = r.parent || r.__hostTarget;
    r && (this._draggingTarget = r, r.dragging = !0, this._x = t.offsetX, this._y = t.offsetY, this.handler.dispatchToElement(new nn(r, t), "dragstart", t.event));
  }, e.prototype._drag = function(t) {
    var r = this._draggingTarget;
    if (r) {
      var n = t.offsetX, i = t.offsetY, a = n - this._x, o = i - this._y;
      this._x = n, this._y = i, r.drift(a, o, t), this.handler.dispatchToElement(new nn(r, t), "drag", t.event);
      var s = this.handler.findHover(n, i, r).target, u = this._dropTarget;
      this._dropTarget = s, r !== s && (u && s !== u && this.handler.dispatchToElement(new nn(u, t), "dragleave", t.event), s && s !== u && this.handler.dispatchToElement(new nn(s, t), "dragenter", t.event));
    }
  }, e.prototype._dragEnd = function(t) {
    var r = this._draggingTarget;
    r && (r.dragging = !1), this.handler.dispatchToElement(new nn(r, t), "dragend", t.event), this._dropTarget && this.handler.dispatchToElement(new nn(this._dropTarget, t), "drop", t.event), this._draggingTarget = null, this._dropTarget = null;
  }, e;
})(), Le = (function() {
  function e(t) {
    t && (this._$eventProcessor = t);
  }
  return e.prototype.on = function(t, r, n, i) {
    this._$handlers || (this._$handlers = {});
    var a = this._$handlers;
    if (typeof r == "function" && (i = n, n = r, r = null), !n || !t)
      return this;
    var o = this._$eventProcessor;
    r != null && o && o.normalizeQuery && (r = o.normalizeQuery(r)), a[t] || (a[t] = []);
    for (var s = 0; s < a[t].length; s++)
      if (a[t][s].h === n)
        return this;
    var u = {
      h: n,
      query: r,
      ctx: i || this,
      callAtLast: n.zrEventfulCallAtLast
    }, l = a[t].length - 1, f = a[t][l];
    return f && f.callAtLast ? a[t].splice(l, 0, u) : a[t].push(u), this;
  }, e.prototype.isSilent = function(t) {
    var r = this._$handlers;
    return !r || !r[t] || !r[t].length;
  }, e.prototype.off = function(t, r) {
    var n = this._$handlers;
    if (!n)
      return this;
    if (!t)
      return this._$handlers = {}, this;
    if (r) {
      if (n[t]) {
        for (var i = [], a = 0, o = n[t].length; a < o; a++)
          n[t][a].h !== r && i.push(n[t][a]);
        n[t] = i;
      }
      n[t] && n[t].length === 0 && delete n[t];
    } else
      delete n[t];
    return this;
  }, e.prototype.trigger = function(t) {
    for (var r = [], n = 1; n < arguments.length; n++)
      r[n - 1] = arguments[n];
    if (!this._$handlers)
      return this;
    var i = this._$handlers[t], a = this._$eventProcessor;
    if (i)
      for (var o = r.length, s = i.length, u = 0; u < s; u++) {
        var l = i[u];
        if (!(a && a.filter && l.query != null && !a.filter(t, l.query)))
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
    return a && a.afterTrigger && a.afterTrigger(t), this;
  }, e.prototype.triggerWithContext = function(t) {
    for (var r = [], n = 1; n < arguments.length; n++)
      r[n - 1] = arguments[n];
    if (!this._$handlers)
      return this;
    var i = this._$handlers[t], a = this._$eventProcessor;
    if (i)
      for (var o = r.length, s = r[o - 1], u = i.length, l = 0; l < u; l++) {
        var f = i[l];
        if (!(a && a.filter && f.query != null && !a.filter(t, f.query)))
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
    return a && a.afterTrigger && a.afterTrigger(t), this;
  }, e;
})(), H0 = Math.log(2);
function Ou(e, t, r, n, i, a) {
  var o = n + "-" + i, s = e.length;
  if (a.hasOwnProperty(o))
    return a[o];
  if (t === 1) {
    var u = Math.round(Math.log((1 << s) - 1 & ~i) / H0);
    return e[r][u];
  }
  for (var l = n | 1 << r, f = r + 1; n & 1 << f; )
    f++;
  for (var h = 0, v = 0, c = 0; v < s; v++) {
    var d = 1 << v;
    d & i || (h += (c % 2 ? -1 : 1) * e[r][v] * Ou(e, t - 1, f, l, i | d, a), c++);
  }
  return a[o] = h, h;
}
function fh(e, t) {
  var r = [
    [e[0], e[1], 1, 0, 0, 0, -t[0] * e[0], -t[0] * e[1]],
    [0, 0, 0, e[0], e[1], 1, -t[1] * e[0], -t[1] * e[1]],
    [e[2], e[3], 1, 0, 0, 0, -t[2] * e[2], -t[2] * e[3]],
    [0, 0, 0, e[2], e[3], 1, -t[3] * e[2], -t[3] * e[3]],
    [e[4], e[5], 1, 0, 0, 0, -t[4] * e[4], -t[4] * e[5]],
    [0, 0, 0, e[4], e[5], 1, -t[5] * e[4], -t[5] * e[5]],
    [e[6], e[7], 1, 0, 0, 0, -t[6] * e[6], -t[6] * e[7]],
    [0, 0, 0, e[6], e[7], 1, -t[7] * e[6], -t[7] * e[7]]
  ], n = {}, i = Ou(r, 8, 0, 0, 0, n);
  if (i !== 0) {
    for (var a = [], o = 0; o < 8; o++)
      for (var s = 0; s < 8; s++)
        a[s] == null && (a[s] = 0), a[s] += ((o + s) % 2 ? -1 : 1) * Ou(r, 7, o === 0 ? 1 : 0, 1 << o, 1 << s, n) / i * t[o];
    return function(u, l, f) {
      var h = l * a[6] + f * a[7] + 1;
      u[0] = (l * a[0] + f * a[1] + a[2]) / h, u[1] = (l * a[3] + f * a[4] + a[5]) / h;
    };
  }
}
var hh = "___zrEVENTSAVED", ps = [];
function G0(e, t, r, n, i) {
  return ku(ps, t, n, i, !0) && ku(e, r, ps[0], ps[1]);
}
function ku(e, t, r, n, i) {
  if (t.getBoundingClientRect && z.domSupported && !up(t)) {
    var a = t[hh] || (t[hh] = {}), o = V0(t, a), s = W0(o, a, i);
    if (s)
      return s(e, r, n), !0;
  }
  return !1;
}
function V0(e, t) {
  var r = t.markers;
  if (r)
    return r;
  r = t.markers = [];
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
    ].join("!important;"), e.appendChild(o), r.push(o);
  }
  return r;
}
function W0(e, t, r) {
  for (var n = r ? "invTrans" : "trans", i = t[n], a = t.srcCoords, o = [], s = [], u = !0, l = 0; l < 4; l++) {
    var f = e[l].getBoundingClientRect(), h = 2 * l, v = f.left, c = f.top;
    o.push(v, c), u = u && a && v === a[h] && c === a[h + 1], s.push(e[l].offsetLeft, e[l].offsetTop);
  }
  return u && i ? i : (t.srcCoords = o, t[n] = r ? fh(s, o) : fh(o, s));
}
function up(e) {
  return e.nodeName.toUpperCase() === "CANVAS";
}
var U0 = /([&<>"'])/g, Y0 = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;"
};
function Pt(e) {
  return e == null ? "" : (e + "").replace(U0, function(t, r) {
    return Y0[r];
  });
}
var q0 = /^(?:mouse|pointer|contextmenu|drag|drop)|click/, gs = [], X0 = z.browser.firefox && +z.browser.version.split(".")[0] < 39;
function Bu(e, t, r, n) {
  return r = r || {}, n ? ch(e, t, r) : X0 && t.layerX != null && t.layerX !== t.offsetX ? (r.zrX = t.layerX, r.zrY = t.layerY) : t.offsetX != null ? (r.zrX = t.offsetX, r.zrY = t.offsetY) : ch(e, t, r), r;
}
function ch(e, t, r) {
  if (z.domSupported && e.getBoundingClientRect) {
    var n = t.clientX, i = t.clientY;
    if (up(e)) {
      var a = e.getBoundingClientRect();
      r.zrX = n - a.left, r.zrY = i - a.top;
      return;
    } else if (ku(gs, e, n, i)) {
      r.zrX = gs[0], r.zrY = gs[1];
      return;
    }
  }
  r.zrX = r.zrY = 0;
}
function Vl(e) {
  return e || window.event;
}
function Jt(e, t, r) {
  if (t = Vl(t), t.zrX != null)
    return t;
  var n = t.type, i = n && n.indexOf("touch") >= 0;
  if (i) {
    var o = n !== "touchend" ? t.targetTouches[0] : t.changedTouches[0];
    o && Bu(e, o, t, r);
  } else {
    Bu(e, t, t, r);
    var a = $0(t);
    t.zrDelta = a ? a / 120 : -(t.detail || 0) / 3;
  }
  var s = t.button;
  return t.which == null && s !== void 0 && q0.test(t.type) && (t.which = s & 1 ? 1 : s & 2 ? 3 : s & 4 ? 2 : 0), t;
}
function $0(e) {
  var t = e.wheelDelta;
  if (t)
    return t;
  var r = e.deltaX, n = e.deltaY;
  if (r == null || n == null)
    return t;
  var i = Math.abs(n !== 0 ? n : r), a = n > 0 ? -1 : n < 0 ? 1 : r > 0 ? -1 : 1;
  return 3 * i * a;
}
function Z0(e, t, r, n) {
  e.addEventListener(t, r, n);
}
function j0(e, t, r, n) {
  e.removeEventListener(t, r, n);
}
var lp = function(e) {
  e.preventDefault(), e.stopPropagation(), e.cancelBubble = !0;
}, K0 = (function() {
  function e() {
    this._track = [];
  }
  return e.prototype.recognize = function(t, r, n) {
    return this._doTrack(t, r, n), this._recognize(t);
  }, e.prototype.clear = function() {
    return this._track.length = 0, this;
  }, e.prototype._doTrack = function(t, r, n) {
    var i = t.touches;
    if (i) {
      for (var a = {
        points: [],
        touches: [],
        target: r,
        event: t
      }, o = 0, s = i.length; o < s; o++) {
        var u = i[o], l = Bu(n, u, {});
        a.points.push([l.zrX, l.zrY]), a.touches.push(u);
      }
      this._track.push(a);
    }
  }, e.prototype._recognize = function(t) {
    for (var r in ys)
      if (ys.hasOwnProperty(r)) {
        var n = ys[r](this._track, t);
        if (n)
          return n;
      }
  }, e;
})();
function vh(e) {
  var t = e[1][0] - e[0][0], r = e[1][1] - e[0][1];
  return Math.sqrt(t * t + r * r);
}
function Q0(e) {
  return [
    (e[0][0] + e[1][0]) / 2,
    (e[0][1] + e[1][1]) / 2
  ];
}
var ys = {
  pinch: function(e, t) {
    var r = e.length;
    if (r) {
      var n = (e[r - 1] || {}).points, i = (e[r - 2] || {}).points || n;
      if (i && i.length > 1 && n && n.length > 1) {
        var a = vh(n) / vh(i);
        !isFinite(a) && (a = 1), t.pinchScale = a;
        var o = Q0(n);
        return t.pinchX = o[0], t.pinchY = o[1], {
          type: "pinch",
          target: e[0].target,
          event: t
        };
      }
    }
  }
};
function sr() {
  return [1, 0, 0, 1, 0, 0];
}
function $i(e) {
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 1, e[4] = 0, e[5] = 0, e;
}
function Wl(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[4] = t[4], e[5] = t[5], e;
}
function ur(e, t, r) {
  var n = t[0] * r[0] + t[2] * r[1], i = t[1] * r[0] + t[3] * r[1], a = t[0] * r[2] + t[2] * r[3], o = t[1] * r[2] + t[3] * r[3], s = t[0] * r[4] + t[2] * r[5] + t[4], u = t[1] * r[4] + t[3] * r[5] + t[5];
  return e[0] = n, e[1] = i, e[2] = a, e[3] = o, e[4] = s, e[5] = u, e;
}
function oo(e, t, r) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[4] = t[4] + r[0], e[5] = t[5] + r[1], e;
}
function Oo(e, t, r, n) {
  n === void 0 && (n = [0, 0]);
  var i = t[0], a = t[2], o = t[4], s = t[1], u = t[3], l = t[5], f = Math.sin(r), h = Math.cos(r);
  return e[0] = i * h + s * f, e[1] = -i * f + s * h, e[2] = a * h + u * f, e[3] = -a * f + h * u, e[4] = h * (o - n[0]) + f * (l - n[1]) + n[0], e[5] = h * (l - n[1]) - f * (o - n[0]) + n[1], e;
}
function fp(e, t, r) {
  var n = r[0], i = r[1];
  return e[0] = t[0] * n, e[1] = t[1] * i, e[2] = t[2] * n, e[3] = t[3] * i, e[4] = t[4] * n, e[5] = t[5] * i, e;
}
function ko(e, t) {
  var r = t[0], n = t[2], i = t[4], a = t[1], o = t[3], s = t[5], u = r * o - a * n;
  return u ? (u = 1 / u, e[0] = o * u, e[1] = -a * u, e[2] = -n * u, e[3] = r * u, e[4] = (n * s - o * i) * u, e[5] = (a * i - r * s) * u, e) : null;
}
function J0(e) {
  var t = sr();
  return Wl(t, e), t;
}
const tm = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  clone: J0,
  copy: Wl,
  create: sr,
  identity: $i,
  invert: ko,
  mul: ur,
  rotate: Oo,
  scale: fp,
  translate: oo
}, Symbol.toStringTag, { value: "Module" }));
var et = (function() {
  function e(t, r) {
    this.x = t || 0, this.y = r || 0;
  }
  return e.prototype.copy = function(t) {
    return this.x = t.x, this.y = t.y, this;
  }, e.prototype.clone = function() {
    return new e(this.x, this.y);
  }, e.prototype.set = function(t, r) {
    return this.x = t, this.y = r, this;
  }, e.prototype.equal = function(t) {
    return t.x === this.x && t.y === this.y;
  }, e.prototype.add = function(t) {
    return this.x += t.x, this.y += t.y, this;
  }, e.prototype.scale = function(t) {
    this.x *= t, this.y *= t;
  }, e.prototype.scaleAndAdd = function(t, r) {
    this.x += t.x * r, this.y += t.y * r;
  }, e.prototype.sub = function(t) {
    return this.x -= t.x, this.y -= t.y, this;
  }, e.prototype.dot = function(t) {
    return this.x * t.x + this.y * t.y;
  }, e.prototype.len = function() {
    return Math.sqrt(this.x * this.x + this.y * this.y);
  }, e.prototype.lenSquare = function() {
    return this.x * this.x + this.y * this.y;
  }, e.prototype.normalize = function() {
    var t = this.len();
    return this.x /= t, this.y /= t, this;
  }, e.prototype.distance = function(t) {
    var r = this.x - t.x, n = this.y - t.y;
    return Math.sqrt(r * r + n * n);
  }, e.prototype.distanceSquare = function(t) {
    var r = this.x - t.x, n = this.y - t.y;
    return r * r + n * n;
  }, e.prototype.negate = function() {
    return this.x = -this.x, this.y = -this.y, this;
  }, e.prototype.transform = function(t) {
    if (t) {
      var r = this.x, n = this.y;
      return this.x = t[0] * r + t[2] * n + t[4], this.y = t[1] * r + t[3] * n + t[5], this;
    }
  }, e.prototype.toArray = function(t) {
    return t[0] = this.x, t[1] = this.y, t;
  }, e.prototype.fromArray = function(t) {
    this.x = t[0], this.y = t[1];
  }, e.set = function(t, r, n) {
    t.x = r, t.y = n;
  }, e.copy = function(t, r) {
    t.x = r.x, t.y = r.y;
  }, e.len = function(t) {
    return Math.sqrt(t.x * t.x + t.y * t.y);
  }, e.lenSquare = function(t) {
    return t.x * t.x + t.y * t.y;
  }, e.dot = function(t, r) {
    return t.x * r.x + t.y * r.y;
  }, e.add = function(t, r, n) {
    t.x = r.x + n.x, t.y = r.y + n.y;
  }, e.sub = function(t, r, n) {
    t.x = r.x - n.x, t.y = r.y - n.y;
  }, e.scale = function(t, r, n) {
    t.x = r.x * n, t.y = r.y * n;
  }, e.scaleAndAdd = function(t, r, n, i) {
    t.x = r.x + n.x * i, t.y = r.y + n.y * i;
  }, e.lerp = function(t, r, n, i) {
    var a = 1 - i;
    t.x = a * r.x + i * n.x, t.y = a * r.y + i * n.y;
  }, e;
})(), sa = Math.min, ua = Math.max, _r = new et(), mr = new et(), Sr = new et(), wr = new et(), Un = new et(), Yn = new et(), j = (function() {
  function e(t, r, n, i) {
    n < 0 && (t = t + n, n = -n), i < 0 && (r = r + i, i = -i), this.x = t, this.y = r, this.width = n, this.height = i;
  }
  return e.prototype.union = function(t) {
    var r = sa(t.x, this.x), n = sa(t.y, this.y);
    isFinite(this.x) && isFinite(this.width) ? this.width = ua(t.x + t.width, this.x + this.width) - r : this.width = t.width, isFinite(this.y) && isFinite(this.height) ? this.height = ua(t.y + t.height, this.y + this.height) - n : this.height = t.height, this.x = r, this.y = n;
  }, e.prototype.applyTransform = function(t) {
    e.applyTransform(this, this, t);
  }, e.prototype.calculateTransform = function(t) {
    var r = this, n = t.width / r.width, i = t.height / r.height, a = sr();
    return oo(a, a, [-r.x, -r.y]), fp(a, a, [n, i]), oo(a, a, [t.x, t.y]), a;
  }, e.prototype.intersect = function(t, r) {
    if (!t)
      return !1;
    t instanceof e || (t = e.create(t));
    var n = this, i = n.x, a = n.x + n.width, o = n.y, s = n.y + n.height, u = t.x, l = t.x + t.width, f = t.y, h = t.y + t.height, v = !(a < u || l < i || s < f || h < o);
    if (r) {
      var c = 1 / 0, d = 0, y = Math.abs(a - u), p = Math.abs(l - i), g = Math.abs(s - f), _ = Math.abs(h - o), m = Math.min(y, p), S = Math.min(g, _);
      a < u || l < i ? m > d && (d = m, y < p ? et.set(Yn, -y, 0) : et.set(Yn, p, 0)) : m < c && (c = m, y < p ? et.set(Un, y, 0) : et.set(Un, -p, 0)), s < f || h < o ? S > d && (d = S, g < _ ? et.set(Yn, 0, -g) : et.set(Yn, 0, _)) : m < c && (c = m, g < _ ? et.set(Un, 0, g) : et.set(Un, 0, -_));
    }
    return r && et.copy(r, v ? Un : Yn), v;
  }, e.prototype.contain = function(t, r) {
    var n = this;
    return t >= n.x && t <= n.x + n.width && r >= n.y && r <= n.y + n.height;
  }, e.prototype.clone = function() {
    return new e(this.x, this.y, this.width, this.height);
  }, e.prototype.copy = function(t) {
    e.copy(this, t);
  }, e.prototype.plain = function() {
    return {
      x: this.x,
      y: this.y,
      width: this.width,
      height: this.height
    };
  }, e.prototype.isFinite = function() {
    return isFinite(this.x) && isFinite(this.y) && isFinite(this.width) && isFinite(this.height);
  }, e.prototype.isZero = function() {
    return this.width === 0 || this.height === 0;
  }, e.create = function(t) {
    return new e(t.x, t.y, t.width, t.height);
  }, e.copy = function(t, r) {
    t.x = r.x, t.y = r.y, t.width = r.width, t.height = r.height;
  }, e.applyTransform = function(t, r, n) {
    if (!n) {
      t !== r && e.copy(t, r);
      return;
    }
    if (n[1] < 1e-5 && n[1] > -1e-5 && n[2] < 1e-5 && n[2] > -1e-5) {
      var i = n[0], a = n[3], o = n[4], s = n[5];
      t.x = r.x * i + o, t.y = r.y * a + s, t.width = r.width * i, t.height = r.height * a, t.width < 0 && (t.x += t.width, t.width = -t.width), t.height < 0 && (t.y += t.height, t.height = -t.height);
      return;
    }
    _r.x = Sr.x = r.x, _r.y = wr.y = r.y, mr.x = wr.x = r.x + r.width, mr.y = Sr.y = r.y + r.height, _r.transform(n), wr.transform(n), mr.transform(n), Sr.transform(n), t.x = sa(_r.x, mr.x, Sr.x, wr.x), t.y = sa(_r.y, mr.y, Sr.y, wr.y);
    var u = ua(_r.x, mr.x, Sr.x, wr.x), l = ua(_r.y, mr.y, Sr.y, wr.y);
    t.width = u - t.x, t.height = l - t.y;
  }, e;
})(), hp = "silent";
function em(e, t, r) {
  return {
    type: e,
    event: r,
    target: t.target,
    topTarget: t.topTarget,
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
    stop: rm
  };
}
function rm() {
  lp(this.event);
}
var nm = (function(e) {
  tt(t, e);
  function t() {
    var r = e !== null && e.apply(this, arguments) || this;
    return r.handler = null, r;
  }
  return t.prototype.dispose = function() {
  }, t.prototype.setCursor = function() {
  }, t;
})(Le), qn = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.x = t, this.y = r;
  }
  return e;
})(), im = [
  "click",
  "dblclick",
  "mousewheel",
  "mouseout",
  "mouseup",
  "mousedown",
  "mousemove",
  "contextmenu"
], _s = new j(0, 0, 0, 0), cp = (function(e) {
  tt(t, e);
  function t(r, n, i, a, o) {
    var s = e.call(this) || this;
    return s._hovered = new qn(0, 0), s.storage = r, s.painter = n, s.painterRoot = a, s._pointerSize = o, i = i || new nm(), s.proxy = null, s.setHandlerProxy(i), s._draggingMgr = new z0(s), s;
  }
  return t.prototype.setHandlerProxy = function(r) {
    this.proxy && this.proxy.dispose(), r && (x(im, function(n) {
      r.on && r.on(n, this[n], this);
    }, this), r.handler = this), this.proxy = r;
  }, t.prototype.mousemove = function(r) {
    var n = r.zrX, i = r.zrY, a = vp(this, n, i), o = this._hovered, s = o.target;
    s && !s.__zr && (o = this.findHover(o.x, o.y), s = o.target);
    var u = this._hovered = a ? new qn(n, i) : this.findHover(n, i), l = u.target, f = this.proxy;
    f.setCursor && f.setCursor(l ? l.cursor : "default"), s && l !== s && this.dispatchToElement(o, "mouseout", r), this.dispatchToElement(u, "mousemove", r), l && l !== s && this.dispatchToElement(u, "mouseover", r);
  }, t.prototype.mouseout = function(r) {
    var n = r.zrEventControl;
    n !== "only_globalout" && this.dispatchToElement(this._hovered, "mouseout", r), n !== "no_globalout" && this.trigger("globalout", { type: "globalout", event: r });
  }, t.prototype.resize = function() {
    this._hovered = new qn(0, 0);
  }, t.prototype.dispatch = function(r, n) {
    var i = this[r];
    i && i.call(this, n);
  }, t.prototype.dispose = function() {
    this.proxy.dispose(), this.storage = null, this.proxy = null, this.painter = null;
  }, t.prototype.setCursorStyle = function(r) {
    var n = this.proxy;
    n.setCursor && n.setCursor(r);
  }, t.prototype.dispatchToElement = function(r, n, i) {
    r = r || {};
    var a = r.target;
    if (!(a && a.silent)) {
      for (var o = "on" + n, s = em(n, r, i); a && (a[o] && (s.cancelBubble = !!a[o].call(a, s)), a.trigger(n, s), a = a.__hostTarget ? a.__hostTarget : a.parent, !s.cancelBubble); )
        ;
      s.cancelBubble || (this.trigger(n, s), this.painter && this.painter.eachOtherLayer && this.painter.eachOtherLayer(function(u) {
        typeof u[o] == "function" && u[o].call(u, s), u.trigger && u.trigger(n, s);
      }));
    }
  }, t.prototype.findHover = function(r, n, i) {
    var a = this.storage.getDisplayList(), o = new qn(r, n);
    if (dh(a, o, r, n, i), this._pointerSize && !o.target) {
      for (var s = [], u = this._pointerSize, l = u / 2, f = new j(r - l, n - l, u, u), h = a.length - 1; h >= 0; h--) {
        var v = a[h];
        v !== i && !v.ignore && !v.ignoreCoarsePointer && (!v.parent || !v.parent.ignoreCoarsePointer) && (_s.copy(v.getBoundingRect()), v.transform && _s.applyTransform(v.transform), _s.intersect(f) && s.push(v));
      }
      if (s.length)
        for (var c = 4, d = Math.PI / 12, y = Math.PI * 2, p = 0; p < l; p += c)
          for (var g = 0; g < y; g += d) {
            var _ = r + p * Math.cos(g), m = n + p * Math.sin(g);
            if (dh(s, o, _, m, i), o.target)
              return o;
          }
    }
    return o;
  }, t.prototype.processGesture = function(r, n) {
    this._gestureMgr || (this._gestureMgr = new K0());
    var i = this._gestureMgr;
    n === "start" && i.clear();
    var a = i.recognize(r, this.findHover(r.zrX, r.zrY, null).target, this.proxy.dom);
    if (n === "end" && i.clear(), a) {
      var o = a.type;
      r.gestureEvent = o;
      var s = new qn();
      s.target = a.target, this.dispatchToElement(s, o, a.event);
    }
  }, t;
})(Le);
x(["click", "mousedown", "mouseup", "mousewheel", "dblclick", "contextmenu"], function(e) {
  cp.prototype[e] = function(t) {
    var r = t.zrX, n = t.zrY, i = vp(this, r, n), a, o;
    if ((e !== "mouseup" || !i) && (a = this.findHover(r, n), o = a.target), e === "mousedown")
      this._downEl = o, this._downPoint = [t.zrX, t.zrY], this._upEl = o;
    else if (e === "mouseup")
      this._upEl = o;
    else if (e === "click") {
      if (this._downEl !== this._upEl || !this._downPoint || op(this._downPoint, [t.zrX, t.zrY]) > 4)
        return;
      this._downPoint = null;
    }
    this.dispatchToElement(a, e, t);
  };
});
function am(e, t, r) {
  if (e[e.rectHover ? "rectContain" : "contain"](t, r)) {
    for (var n = e, i = void 0, a = !1; n; ) {
      if (n.ignoreClip && (a = !0), !a) {
        var o = n.getClipPath();
        if (o && !o.contain(t, r))
          return !1;
      }
      n.silent && (i = !0);
      var s = n.__hostTarget;
      n = s || n.parent;
    }
    return i ? hp : !0;
  }
  return !1;
}
function dh(e, t, r, n, i) {
  for (var a = e.length - 1; a >= 0; a--) {
    var o = e[a], s = void 0;
    if (o !== i && !o.ignore && (s = am(o, r, n)) && (!t.topTarget && (t.topTarget = o), s !== hp)) {
      t.target = o;
      break;
    }
  }
}
function vp(e, t, r) {
  var n = e.painter;
  return t < 0 || t > n.getWidth() || r < 0 || r > n.getHeight();
}
var dp = 32, Xn = 7;
function om(e) {
  for (var t = 0; e >= dp; )
    t |= e & 1, e >>= 1;
  return e + t;
}
function ph(e, t, r, n) {
  var i = t + 1;
  if (i === r)
    return 1;
  if (n(e[i++], e[t]) < 0) {
    for (; i < r && n(e[i], e[i - 1]) < 0; )
      i++;
    sm(e, t, i);
  } else
    for (; i < r && n(e[i], e[i - 1]) >= 0; )
      i++;
  return i - t;
}
function sm(e, t, r) {
  for (r--; t < r; ) {
    var n = e[t];
    e[t++] = e[r], e[r--] = n;
  }
}
function gh(e, t, r, n, i) {
  for (n === t && n++; n < r; n++) {
    for (var a = e[n], o = t, s = n, u; o < s; )
      u = o + s >>> 1, i(a, e[u]) < 0 ? s = u : o = u + 1;
    var l = n - o;
    switch (l) {
      case 3:
        e[o + 3] = e[o + 2];
      case 2:
        e[o + 2] = e[o + 1];
      case 1:
        e[o + 1] = e[o];
        break;
      default:
        for (; l > 0; )
          e[o + l] = e[o + l - 1], l--;
    }
    e[o] = a;
  }
}
function ms(e, t, r, n, i, a) {
  var o = 0, s = 0, u = 1;
  if (a(e, t[r + i]) > 0) {
    for (s = n - i; u < s && a(e, t[r + i + u]) > 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s), o += i, u += i;
  } else {
    for (s = i + 1; u < s && a(e, t[r + i - u]) <= 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s);
    var l = o;
    o = i - u, u = i - l;
  }
  for (o++; o < u; ) {
    var f = o + (u - o >>> 1);
    a(e, t[r + f]) > 0 ? o = f + 1 : u = f;
  }
  return u;
}
function Ss(e, t, r, n, i, a) {
  var o = 0, s = 0, u = 1;
  if (a(e, t[r + i]) < 0) {
    for (s = i + 1; u < s && a(e, t[r + i - u]) < 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s);
    var l = o;
    o = i - u, u = i - l;
  } else {
    for (s = n - i; u < s && a(e, t[r + i + u]) >= 0; )
      o = u, u = (u << 1) + 1, u <= 0 && (u = s);
    u > s && (u = s), o += i, u += i;
  }
  for (o++; o < u; ) {
    var f = o + (u - o >>> 1);
    a(e, t[r + f]) < 0 ? u = f : o = f + 1;
  }
  return u;
}
function um(e, t) {
  var r = Xn, n, i, a = 0, o = [];
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
    var _ = Ss(e[p], e, d, y, 0, t);
    d += _, y -= _, y !== 0 && (g = ms(e[d + y - 1], e, p, g, g - 1, t), g !== 0 && (y <= g ? h(d, y, p, g) : v(d, y, p, g)));
  }
  function h(c, d, y, p) {
    var g = 0;
    for (g = 0; g < d; g++)
      o[g] = e[c + g];
    var _ = 0, m = y, S = c;
    if (e[S++] = e[m++], --p === 0) {
      for (g = 0; g < d; g++)
        e[S + g] = o[_ + g];
      return;
    }
    if (d === 1) {
      for (g = 0; g < p; g++)
        e[S + g] = e[m + g];
      e[S + p] = o[_];
      return;
    }
    for (var b = r, w, T, C; ; ) {
      w = 0, T = 0, C = !1;
      do
        if (t(e[m], o[_]) < 0) {
          if (e[S++] = e[m++], T++, w = 0, --p === 0) {
            C = !0;
            break;
          }
        } else if (e[S++] = o[_++], w++, T = 0, --d === 1) {
          C = !0;
          break;
        }
      while ((w | T) < b);
      if (C)
        break;
      do {
        if (w = Ss(e[m], o, _, d, 0, t), w !== 0) {
          for (g = 0; g < w; g++)
            e[S + g] = o[_ + g];
          if (S += w, _ += w, d -= w, d <= 1) {
            C = !0;
            break;
          }
        }
        if (e[S++] = e[m++], --p === 0) {
          C = !0;
          break;
        }
        if (T = ms(o[_], e, m, p, 0, t), T !== 0) {
          for (g = 0; g < T; g++)
            e[S + g] = e[m + g];
          if (S += T, m += T, p -= T, p === 0) {
            C = !0;
            break;
          }
        }
        if (e[S++] = o[_++], --d === 1) {
          C = !0;
          break;
        }
        b--;
      } while (w >= Xn || T >= Xn);
      if (C)
        break;
      b < 0 && (b = 0), b += 2;
    }
    if (r = b, r < 1 && (r = 1), d === 1) {
      for (g = 0; g < p; g++)
        e[S + g] = e[m + g];
      e[S + p] = o[_];
    } else {
      if (d === 0)
        throw new Error();
      for (g = 0; g < d; g++)
        e[S + g] = o[_ + g];
    }
  }
  function v(c, d, y, p) {
    var g = 0;
    for (g = 0; g < p; g++)
      o[g] = e[y + g];
    var _ = c + d - 1, m = p - 1, S = y + p - 1, b = 0, w = 0;
    if (e[S--] = e[_--], --d === 0) {
      for (b = S - (p - 1), g = 0; g < p; g++)
        e[b + g] = o[g];
      return;
    }
    if (p === 1) {
      for (S -= d, _ -= d, w = S + 1, b = _ + 1, g = d - 1; g >= 0; g--)
        e[w + g] = e[b + g];
      e[S] = o[m];
      return;
    }
    for (var T = r; ; ) {
      var C = 0, M = 0, D = !1;
      do
        if (t(o[m], e[_]) < 0) {
          if (e[S--] = e[_--], C++, M = 0, --d === 0) {
            D = !0;
            break;
          }
        } else if (e[S--] = o[m--], M++, C = 0, --p === 1) {
          D = !0;
          break;
        }
      while ((C | M) < T);
      if (D)
        break;
      do {
        if (C = d - Ss(o[m], e, c, d, d - 1, t), C !== 0) {
          for (S -= C, _ -= C, d -= C, w = S + 1, b = _ + 1, g = C - 1; g >= 0; g--)
            e[w + g] = e[b + g];
          if (d === 0) {
            D = !0;
            break;
          }
        }
        if (e[S--] = o[m--], --p === 1) {
          D = !0;
          break;
        }
        if (M = p - ms(e[_], o, 0, p, p - 1, t), M !== 0) {
          for (S -= M, m -= M, p -= M, w = S + 1, b = m + 1, g = 0; g < M; g++)
            e[w + g] = o[b + g];
          if (p <= 1) {
            D = !0;
            break;
          }
        }
        if (e[S--] = e[_--], --d === 0) {
          D = !0;
          break;
        }
        T--;
      } while (C >= Xn || M >= Xn);
      if (D)
        break;
      T < 0 && (T = 0), T += 2;
    }
    if (r = T, r < 1 && (r = 1), p === 1) {
      for (S -= d, _ -= d, w = S + 1, b = _ + 1, g = d - 1; g >= 0; g--)
        e[w + g] = e[b + g];
      e[S] = o[m];
    } else {
      if (p === 0)
        throw new Error();
      for (b = S - (p - 1), g = 0; g < p; g++)
        e[b + g] = o[g];
    }
  }
  return {
    mergeRuns: u,
    forceMergeRuns: l,
    pushRun: s
  };
}
function Wa(e, t, r, n) {
  r || (r = 0), n || (n = e.length);
  var i = n - r;
  if (!(i < 2)) {
    var a = 0;
    if (i < dp) {
      a = ph(e, r, n, t), gh(e, r, n, r + a, t);
      return;
    }
    var o = um(e, t), s = om(i);
    do {
      if (a = ph(e, r, n, t), a < s) {
        var u = i;
        u > s && (u = s), gh(e, r, r + u, r + a, t), a = u;
      }
      o.pushRun(r, a), o.mergeRuns(), i -= a, r += a;
    } while (i !== 0);
    o.forceMergeRuns();
  }
}
var Ut = 1, fi = 2, mn = 4, yh = !1;
function ws() {
  yh || (yh = !0, console.warn("z / z2 / zlevel of displayable is invalid, which may cause unexpected errors"));
}
function _h(e, t) {
  return e.zlevel === t.zlevel ? e.z === t.z ? e.z2 - t.z2 : e.z - t.z : e.zlevel - t.zlevel;
}
var lm = (function() {
  function e() {
    this._roots = [], this._displayList = [], this._displayListLen = 0, this.displayableSortFunc = _h;
  }
  return e.prototype.traverse = function(t, r) {
    for (var n = 0; n < this._roots.length; n++)
      this._roots[n].traverse(t, r);
  }, e.prototype.getDisplayList = function(t, r) {
    r = r || !1;
    var n = this._displayList;
    return (t || !n.length) && this.updateDisplayList(r), n;
  }, e.prototype.updateDisplayList = function(t) {
    this._displayListLen = 0;
    for (var r = this._roots, n = this._displayList, i = 0, a = r.length; i < a; i++)
      this._updateAndAddDisplayable(r[i], null, t);
    n.length = this._displayListLen, Wa(n, _h);
  }, e.prototype._updateAndAddDisplayable = function(t, r, n) {
    if (!(t.ignore && !n)) {
      t.beforeUpdate(), t.update(), t.afterUpdate();
      var i = t.getClipPath();
      if (t.ignoreClip)
        r = null;
      else if (i) {
        r ? r = r.slice() : r = [];
        for (var a = i, o = t; a; )
          a.parent = o, a.updateTransform(), r.push(a), o = a, a = a.getClipPath();
      }
      if (t.childrenRef) {
        for (var s = t.childrenRef(), u = 0; u < s.length; u++) {
          var l = s[u];
          t.__dirty && (l.__dirty |= Ut), this._updateAndAddDisplayable(l, r, n);
        }
        t.__dirty = 0;
      } else {
        var f = t;
        r && r.length ? f.__clipPaths = r : f.__clipPaths && f.__clipPaths.length > 0 && (f.__clipPaths = []), isNaN(f.z) && (ws(), f.z = 0), isNaN(f.z2) && (ws(), f.z2 = 0), isNaN(f.zlevel) && (ws(), f.zlevel = 0), this._displayList[this._displayListLen++] = f;
      }
      var h = t.getDecalElement && t.getDecalElement();
      h && this._updateAndAddDisplayable(h, r, n);
      var v = t.getTextGuideLine();
      v && this._updateAndAddDisplayable(v, r, n);
      var c = t.getTextContent();
      c && this._updateAndAddDisplayable(c, r, n);
    }
  }, e.prototype.addRoot = function(t) {
    t.__zr && t.__zr.storage === this || this._roots.push(t);
  }, e.prototype.delRoot = function(t) {
    if (t instanceof Array) {
      for (var r = 0, n = t.length; r < n; r++)
        this.delRoot(t[r]);
      return;
    }
    var i = rt(this._roots, t);
    i >= 0 && this._roots.splice(i, 1);
  }, e.prototype.delAllRoots = function() {
    this._roots = [], this._displayList = [], this._displayListLen = 0;
  }, e.prototype.getRoots = function() {
    return this._roots;
  }, e.prototype.dispose = function() {
    this._displayList = null, this._roots = null;
  }, e;
})(), so;
so = z.hasGlobalWindow && (window.requestAnimationFrame && window.requestAnimationFrame.bind(window) || window.msRequestAnimationFrame && window.msRequestAnimationFrame.bind(window) || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame) || function(e) {
  return setTimeout(e, 16);
};
var _i = {
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
    var t, r = 0.1, n = 0.4;
    return e === 0 ? 0 : e === 1 ? 1 : (!r || r < 1 ? (r = 1, t = n / 4) : t = n * Math.asin(1 / r) / (2 * Math.PI), -(r * Math.pow(2, 10 * (e -= 1)) * Math.sin((e - t) * (2 * Math.PI) / n)));
  },
  elasticOut: function(e) {
    var t, r = 0.1, n = 0.4;
    return e === 0 ? 0 : e === 1 ? 1 : (!r || r < 1 ? (r = 1, t = n / 4) : t = n * Math.asin(1 / r) / (2 * Math.PI), r * Math.pow(2, -10 * e) * Math.sin((e - t) * (2 * Math.PI) / n) + 1);
  },
  elasticInOut: function(e) {
    var t, r = 0.1, n = 0.4;
    return e === 0 ? 0 : e === 1 ? 1 : (!r || r < 1 ? (r = 1, t = n / 4) : t = n * Math.asin(1 / r) / (2 * Math.PI), (e *= 2) < 1 ? -0.5 * (r * Math.pow(2, 10 * (e -= 1)) * Math.sin((e - t) * (2 * Math.PI) / n)) : r * Math.pow(2, -10 * (e -= 1)) * Math.sin((e - t) * (2 * Math.PI) / n) * 0.5 + 1);
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
    return 1 - _i.bounceOut(1 - e);
  },
  bounceOut: function(e) {
    return e < 1 / 2.75 ? 7.5625 * e * e : e < 2 / 2.75 ? 7.5625 * (e -= 1.5 / 2.75) * e + 0.75 : e < 2.5 / 2.75 ? 7.5625 * (e -= 2.25 / 2.75) * e + 0.9375 : 7.5625 * (e -= 2.625 / 2.75) * e + 0.984375;
  },
  bounceInOut: function(e) {
    return e < 0.5 ? _i.bounceIn(e * 2) * 0.5 : _i.bounceOut(e * 2 - 1) * 0.5 + 0.5;
  }
}, la = Math.pow, lr = Math.sqrt, uo = 1e-8, pp = 1e-4, mh = lr(3), fa = 1 / 3, be = Jr(), re = Jr(), xn = Jr();
function nr(e) {
  return e > -uo && e < uo;
}
function gp(e) {
  return e > uo || e < -uo;
}
function _t(e, t, r, n, i) {
  var a = 1 - i;
  return a * a * (a * e + 3 * i * t) + i * i * (i * n + 3 * a * r);
}
function Sh(e, t, r, n, i) {
  var a = 1 - i;
  return 3 * (((t - e) * a + 2 * (r - t) * i) * a + (n - r) * i * i);
}
function yp(e, t, r, n, i, a) {
  var o = n + 3 * (t - r) - e, s = 3 * (r - t * 2 + e), u = 3 * (t - e), l = e - i, f = s * s - 3 * o * u, h = s * u - 9 * o * l, v = u * u - 3 * s * l, c = 0;
  if (nr(f) && nr(h))
    if (nr(s))
      a[0] = 0;
    else {
      var d = -u / s;
      d >= 0 && d <= 1 && (a[c++] = d);
    }
  else {
    var y = h * h - 4 * f * v;
    if (nr(y)) {
      var p = h / f, d = -s / o + p, g = -p / 2;
      d >= 0 && d <= 1 && (a[c++] = d), g >= 0 && g <= 1 && (a[c++] = g);
    } else if (y > 0) {
      var _ = lr(y), m = f * s + 1.5 * o * (-h + _), S = f * s + 1.5 * o * (-h - _);
      m < 0 ? m = -la(-m, fa) : m = la(m, fa), S < 0 ? S = -la(-S, fa) : S = la(S, fa);
      var d = (-s - (m + S)) / (3 * o);
      d >= 0 && d <= 1 && (a[c++] = d);
    } else {
      var b = (2 * f * s - 3 * o * h) / (2 * lr(f * f * f)), w = Math.acos(b) / 3, T = lr(f), C = Math.cos(w), d = (-s - 2 * T * C) / (3 * o), g = (-s + T * (C + mh * Math.sin(w))) / (3 * o), M = (-s + T * (C - mh * Math.sin(w))) / (3 * o);
      d >= 0 && d <= 1 && (a[c++] = d), g >= 0 && g <= 1 && (a[c++] = g), M >= 0 && M <= 1 && (a[c++] = M);
    }
  }
  return c;
}
function _p(e, t, r, n, i) {
  var a = 6 * r - 12 * t + 6 * e, o = 9 * t + 3 * n - 3 * e - 9 * r, s = 3 * t - 3 * e, u = 0;
  if (nr(o)) {
    if (gp(a)) {
      var l = -s / a;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = a * a - 4 * o * s;
    if (nr(f))
      i[0] = -a / (2 * o);
    else if (f > 0) {
      var h = lr(f), l = (-a + h) / (2 * o), v = (-a - h) / (2 * o);
      l >= 0 && l <= 1 && (i[u++] = l), v >= 0 && v <= 1 && (i[u++] = v);
    }
  }
  return u;
}
function lo(e, t, r, n, i, a) {
  var o = (t - e) * i + e, s = (r - t) * i + t, u = (n - r) * i + r, l = (s - o) * i + o, f = (u - s) * i + s, h = (f - l) * i + l;
  a[0] = e, a[1] = o, a[2] = l, a[3] = h, a[4] = h, a[5] = f, a[6] = u, a[7] = n;
}
function fm(e, t, r, n, i, a, o, s, u, l, f) {
  var h, v = 5e-3, c = 1 / 0, d, y, p, g;
  be[0] = u, be[1] = l;
  for (var _ = 0; _ < 1; _ += 0.05)
    re[0] = _t(e, r, i, o, _), re[1] = _t(t, n, a, s, _), p = Ur(be, re), p < c && (h = _, c = p);
  c = 1 / 0;
  for (var m = 0; m < 32 && !(v < pp); m++)
    d = h - v, y = h + v, re[0] = _t(e, r, i, o, d), re[1] = _t(t, n, a, s, d), p = Ur(re, be), d >= 0 && p < c ? (h = d, c = p) : (xn[0] = _t(e, r, i, o, y), xn[1] = _t(t, n, a, s, y), g = Ur(xn, be), y <= 1 && g < c ? (h = y, c = g) : v *= 0.5);
  return f && (f[0] = _t(e, r, i, o, h), f[1] = _t(t, n, a, s, h)), lr(c);
}
function hm(e, t, r, n, i, a, o, s, u) {
  for (var l = e, f = t, h = 0, v = 1 / u, c = 1; c <= u; c++) {
    var d = c * v, y = _t(e, r, i, o, d), p = _t(t, n, a, s, d), g = y - l, _ = p - f;
    h += Math.sqrt(g * g + _ * _), l = y, f = p;
  }
  return h;
}
function Tt(e, t, r, n) {
  var i = 1 - n;
  return i * (i * e + 2 * n * t) + n * n * r;
}
function wh(e, t, r, n) {
  return 2 * ((1 - n) * (t - e) + n * (r - t));
}
function cm(e, t, r, n, i) {
  var a = e - 2 * t + r, o = 2 * (t - e), s = e - n, u = 0;
  if (nr(a)) {
    if (gp(o)) {
      var l = -s / o;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = o * o - 4 * a * s;
    if (nr(f)) {
      var l = -o / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l);
    } else if (f > 0) {
      var h = lr(f), l = (-o + h) / (2 * a), v = (-o - h) / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l), v >= 0 && v <= 1 && (i[u++] = v);
    }
  }
  return u;
}
function mp(e, t, r) {
  var n = e + r - 2 * t;
  return n === 0 ? 0.5 : (e - t) / n;
}
function fo(e, t, r, n, i) {
  var a = (t - e) * n + e, o = (r - t) * n + t, s = (o - a) * n + a;
  i[0] = e, i[1] = a, i[2] = s, i[3] = s, i[4] = o, i[5] = r;
}
function vm(e, t, r, n, i, a, o, s, u) {
  var l, f = 5e-3, h = 1 / 0;
  be[0] = o, be[1] = s;
  for (var v = 0; v < 1; v += 0.05) {
    re[0] = Tt(e, r, i, v), re[1] = Tt(t, n, a, v);
    var c = Ur(be, re);
    c < h && (l = v, h = c);
  }
  h = 1 / 0;
  for (var d = 0; d < 32 && !(f < pp); d++) {
    var y = l - f, p = l + f;
    re[0] = Tt(e, r, i, y), re[1] = Tt(t, n, a, y);
    var c = Ur(re, be);
    if (y >= 0 && c < h)
      l = y, h = c;
    else {
      xn[0] = Tt(e, r, i, p), xn[1] = Tt(t, n, a, p);
      var g = Ur(xn, be);
      p <= 1 && g < h ? (l = p, h = g) : f *= 0.5;
    }
  }
  return u && (u[0] = Tt(e, r, i, l), u[1] = Tt(t, n, a, l)), lr(h);
}
function dm(e, t, r, n, i, a, o) {
  for (var s = e, u = t, l = 0, f = 1 / o, h = 1; h <= o; h++) {
    var v = h * f, c = Tt(e, r, i, v), d = Tt(t, n, a, v), y = c - s, p = d - u;
    l += Math.sqrt(y * y + p * p), s = c, u = d;
  }
  return l;
}
var pm = /cubic-bezier\(([0-9,\.e ]+)\)/;
function Sp(e) {
  var t = e && pm.exec(e);
  if (t) {
    var r = t[1].split(","), n = +pe(r[0]), i = +pe(r[1]), a = +pe(r[2]), o = +pe(r[3]);
    if (isNaN(n + i + a + o))
      return;
    var s = [];
    return function(u) {
      return u <= 0 ? 0 : u >= 1 ? 1 : yp(0, n, a, 1, u, s) && _t(0, i, o, 1, s[0]);
    };
  }
}
var gm = (function() {
  function e(t) {
    this._inited = !1, this._startTime = 0, this._pausedTime = 0, this._paused = !1, this._life = t.life || 1e3, this._delay = t.delay || 0, this.loop = t.loop || !1, this.onframe = t.onframe || Lt, this.ondestroy = t.ondestroy || Lt, this.onrestart = t.onrestart || Lt, t.easing && this.setEasing(t.easing);
  }
  return e.prototype.step = function(t, r) {
    if (this._inited || (this._startTime = t + this._delay, this._inited = !0), this._paused) {
      this._pausedTime += r;
      return;
    }
    var n = this._life, i = t - this._startTime - this._pausedTime, a = i / n;
    a < 0 && (a = 0), a = Math.min(a, 1);
    var o = this.easingFunc, s = o ? o(a) : a;
    if (this.onframe(s), a === 1)
      if (this.loop) {
        var u = i % n;
        this._startTime = t - u, this._pausedTime = 0, this.onrestart();
      } else
        return !0;
    return !1;
  }, e.prototype.pause = function() {
    this._paused = !0;
  }, e.prototype.resume = function() {
    this._paused = !1;
  }, e.prototype.setEasing = function(t) {
    this.easing = t, this.easingFunc = U(t) ? t : _i[t] || Sp(t);
  }, e;
})(), wp = /* @__PURE__ */ (function() {
  function e(t) {
    this.value = t;
  }
  return e;
})(), ym = (function() {
  function e() {
    this._len = 0;
  }
  return e.prototype.insert = function(t) {
    var r = new wp(t);
    return this.insertEntry(r), r;
  }, e.prototype.insertEntry = function(t) {
    this.head ? (this.tail.next = t, t.prev = this.tail, t.next = null, this.tail = t) : this.head = this.tail = t, this._len++;
  }, e.prototype.remove = function(t) {
    var r = t.prev, n = t.next;
    r ? r.next = n : this.head = n, n ? n.prev = r : this.tail = r, t.next = t.prev = null, this._len--;
  }, e.prototype.len = function() {
    return this._len;
  }, e.prototype.clear = function() {
    this.head = this.tail = null, this._len = 0;
  }, e;
})(), Zi = (function() {
  function e(t) {
    this._list = new ym(), this._maxSize = 10, this._map = {}, this._maxSize = t;
  }
  return e.prototype.put = function(t, r) {
    var n = this._list, i = this._map, a = null;
    if (i[t] == null) {
      var o = n.len(), s = this._lastRemovedEntry;
      if (o >= this._maxSize && o > 0) {
        var u = n.head;
        n.remove(u), delete i[u.key], a = u.value, this._lastRemovedEntry = u;
      }
      s ? s.value = r : s = new wp(r), s.key = t, n.insertEntry(s), i[t] = s;
    }
    return a;
  }, e.prototype.get = function(t) {
    var r = this._map[t], n = this._list;
    if (r != null)
      return r !== n.tail && (n.remove(r), n.insertEntry(r)), r.value;
  }, e.prototype.clear = function() {
    this._list.clear(), this._map = {};
  }, e.prototype.len = function() {
    return this._list.len();
  }, e;
})(), bh = {
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
function ge(e) {
  return e = Math.round(e), e < 0 ? 0 : e > 255 ? 255 : e;
}
function _m(e) {
  return e = Math.round(e), e < 0 ? 0 : e > 360 ? 360 : e;
}
function Li(e) {
  return e < 0 ? 0 : e > 1 ? 1 : e;
}
function bs(e) {
  var t = e;
  return t.length && t.charAt(t.length - 1) === "%" ? ge(parseFloat(t) / 100 * 255) : ge(parseInt(t, 10));
}
function Yr(e) {
  var t = e;
  return t.length && t.charAt(t.length - 1) === "%" ? Li(parseFloat(t) / 100) : Li(parseFloat(t));
}
function Ts(e, t, r) {
  return r < 0 ? r += 1 : r > 1 && (r -= 1), r * 6 < 1 ? e + (t - e) * r * 6 : r * 2 < 1 ? t : r * 3 < 2 ? e + (t - e) * (2 / 3 - r) * 6 : e;
}
function ir(e, t, r) {
  return e + (t - e) * r;
}
function Qt(e, t, r, n, i) {
  return e[0] = t, e[1] = r, e[2] = n, e[3] = i, e;
}
function Fu(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e;
}
var bp = new Zi(20), ha = null;
function an(e, t) {
  ha && Fu(ha, t), ha = bp.put(e, ha || t.slice());
}
function qt(e, t) {
  if (e) {
    t = t || [];
    var r = bp.get(e);
    if (r)
      return Fu(t, r);
    e = e + "";
    var n = e.replace(/ /g, "").toLowerCase();
    if (n in bh)
      return Fu(t, bh[n]), an(e, t), t;
    var i = n.length;
    if (n.charAt(0) === "#") {
      if (i === 4 || i === 5) {
        var a = parseInt(n.slice(1, 4), 16);
        if (!(a >= 0 && a <= 4095)) {
          Qt(t, 0, 0, 0, 1);
          return;
        }
        return Qt(t, (a & 3840) >> 4 | (a & 3840) >> 8, a & 240 | (a & 240) >> 4, a & 15 | (a & 15) << 4, i === 5 ? parseInt(n.slice(4), 16) / 15 : 1), an(e, t), t;
      } else if (i === 7 || i === 9) {
        var a = parseInt(n.slice(1, 7), 16);
        if (!(a >= 0 && a <= 16777215)) {
          Qt(t, 0, 0, 0, 1);
          return;
        }
        return Qt(t, (a & 16711680) >> 16, (a & 65280) >> 8, a & 255, i === 9 ? parseInt(n.slice(7), 16) / 255 : 1), an(e, t), t;
      }
      return;
    }
    var o = n.indexOf("("), s = n.indexOf(")");
    if (o !== -1 && s + 1 === i) {
      var u = n.substr(0, o), l = n.substr(o + 1, s - (o + 1)).split(","), f = 1;
      switch (u) {
        case "rgba":
          if (l.length !== 4)
            return l.length === 3 ? Qt(t, +l[0], +l[1], +l[2], 1) : Qt(t, 0, 0, 0, 1);
          f = Yr(l.pop());
        case "rgb":
          if (l.length >= 3)
            return Qt(t, bs(l[0]), bs(l[1]), bs(l[2]), l.length === 3 ? f : Yr(l[3])), an(e, t), t;
          Qt(t, 0, 0, 0, 1);
          return;
        case "hsla":
          if (l.length !== 4) {
            Qt(t, 0, 0, 0, 1);
            return;
          }
          return l[3] = Yr(l[3]), Nu(l, t), an(e, t), t;
        case "hsl":
          if (l.length !== 3) {
            Qt(t, 0, 0, 0, 1);
            return;
          }
          return Nu(l, t), an(e, t), t;
        default:
          return;
      }
    }
    Qt(t, 0, 0, 0, 1);
  }
}
function Nu(e, t) {
  var r = (parseFloat(e[0]) % 360 + 360) % 360 / 360, n = Yr(e[1]), i = Yr(e[2]), a = i <= 0.5 ? i * (n + 1) : i + n - i * n, o = i * 2 - a;
  return t = t || [], Qt(t, ge(Ts(o, a, r + 1 / 3) * 255), ge(Ts(o, a, r) * 255), ge(Ts(o, a, r - 1 / 3) * 255), 1), e.length === 4 && (t[3] = e[3]), t;
}
function mm(e) {
  if (e) {
    var t = e[0] / 255, r = e[1] / 255, n = e[2] / 255, i = Math.min(t, r, n), a = Math.max(t, r, n), o = a - i, s = (a + i) / 2, u, l;
    if (o === 0)
      u = 0, l = 0;
    else {
      s < 0.5 ? l = o / (a + i) : l = o / (2 - a - i);
      var f = ((a - t) / 6 + o / 2) / o, h = ((a - r) / 6 + o / 2) / o, v = ((a - n) / 6 + o / 2) / o;
      t === a ? u = v - h : r === a ? u = 1 / 3 + f - v : n === a && (u = 2 / 3 + h - f), u < 0 && (u += 1), u > 1 && (u -= 1);
    }
    var c = [u * 360, l, s];
    return e[3] != null && c.push(e[3]), c;
  }
}
function zu(e, t) {
  var r = qt(e);
  if (r) {
    for (var n = 0; n < 3; n++)
      t < 0 ? r[n] = r[n] * (1 - t) | 0 : r[n] = (255 - r[n]) * t + r[n] | 0, r[n] > 255 ? r[n] = 255 : r[n] < 0 && (r[n] = 0);
    return cr(r, r.length === 4 ? "rgba" : "rgb");
  }
}
function Sm(e) {
  var t = qt(e);
  if (t)
    return ((1 << 24) + (t[0] << 16) + (t[1] << 8) + +t[2]).toString(16).slice(1);
}
function Tp(e, t, r) {
  if (!(!(t && t.length) || !(e >= 0 && e <= 1))) {
    r = r || [];
    var n = e * (t.length - 1), i = Math.floor(n), a = Math.ceil(n), o = t[i], s = t[a], u = n - i;
    return r[0] = ge(ir(o[0], s[0], u)), r[1] = ge(ir(o[1], s[1], u)), r[2] = ge(ir(o[2], s[2], u)), r[3] = Li(ir(o[3], s[3], u)), r;
  }
}
var wm = Tp;
function xp(e, t, r) {
  if (!(!(t && t.length) || !(e >= 0 && e <= 1))) {
    var n = e * (t.length - 1), i = Math.floor(n), a = Math.ceil(n), o = qt(t[i]), s = qt(t[a]), u = n - i, l = cr([
      ge(ir(o[0], s[0], u)),
      ge(ir(o[1], s[1], u)),
      ge(ir(o[2], s[2], u)),
      Li(ir(o[3], s[3], u))
    ], "rgba");
    return r ? {
      color: l,
      leftIndex: i,
      rightIndex: a,
      value: n
    } : l;
  }
}
var bm = xp;
function Tm(e, t, r, n) {
  var i = qt(e);
  if (e)
    return i = mm(i), t != null && (i[0] = _m(t)), r != null && (i[1] = Yr(r)), n != null && (i[2] = Yr(n)), cr(Nu(i), "rgba");
}
function xm(e, t) {
  var r = qt(e);
  if (r && t != null)
    return r[3] = Li(t), cr(r, "rgba");
}
function cr(e, t) {
  if (!(!e || !e.length)) {
    var r = e[0] + "," + e[1] + "," + e[2];
    return (t === "rgba" || t === "hsva" || t === "hsla") && (r += "," + e[3]), t + "(" + r + ")";
  }
}
function Ri(e, t) {
  var r = qt(e);
  return r ? (0.299 * r[0] + 0.587 * r[1] + 0.114 * r[2]) * r[3] / 255 + (1 - r[3]) * t : 0;
}
function Cm() {
  return cr([
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255)
  ], "rgb");
}
var Th = new Zi(100);
function Hu(e) {
  if (B(e)) {
    var t = Th.get(e);
    return t || (t = zu(e, -0.1), Th.put(e, t)), t;
  } else if (qi(e)) {
    var r = E({}, e);
    return r.colorStops = F(e.colorStops, function(n) {
      return {
        offset: n.offset,
        color: zu(n.color, -0.1)
      };
    }), r;
  }
  return e;
}
const Mm = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  fastLerp: Tp,
  fastMapToColor: wm,
  lerp: xp,
  lift: zu,
  liftColor: Hu,
  lum: Ri,
  mapToColor: bm,
  modifyAlpha: xm,
  modifyHSL: Tm,
  parse: qt,
  random: Cm,
  stringify: cr,
  toHex: Sm
}, Symbol.toStringTag, { value: "Module" }));
function Dm(e) {
  return e.type === "linear";
}
function Pm(e) {
  return e.type === "radial";
}
(function() {
  return z.hasGlobalWindow && U(window.btoa) ? function(e) {
    return window.btoa(unescape(encodeURIComponent(e)));
  } : typeof Buffer < "u" ? function(e) {
    return Buffer.from(e).toString("base64");
  } : function(e) {
    return null;
  };
})();
var Gu = Array.prototype.slice;
function ze(e, t, r) {
  return (t - e) * r + e;
}
function xs(e, t, r, n) {
  for (var i = t.length, a = 0; a < i; a++)
    e[a] = ze(t[a], r[a], n);
  return e;
}
function Im(e, t, r, n) {
  for (var i = t.length, a = i && t[0].length, o = 0; o < i; o++) {
    e[o] || (e[o] = []);
    for (var s = 0; s < a; s++)
      e[o][s] = ze(t[o][s], r[o][s], n);
  }
  return e;
}
function ca(e, t, r, n) {
  for (var i = t.length, a = 0; a < i; a++)
    e[a] = t[a] + r[a] * n;
  return e;
}
function xh(e, t, r, n) {
  for (var i = t.length, a = i && t[0].length, o = 0; o < i; o++) {
    e[o] || (e[o] = []);
    for (var s = 0; s < a; s++)
      e[o][s] = t[o][s] + r[o][s] * n;
  }
  return e;
}
function Am(e, t) {
  for (var r = e.length, n = t.length, i = r > n ? t : e, a = Math.min(r, n), o = i[a - 1] || { color: [0, 0, 0, 0], offset: 0 }, s = a; s < Math.max(r, n); s++)
    i.push({
      offset: o.offset,
      color: o.color.slice()
    });
}
function Lm(e, t, r) {
  var n = e, i = t;
  if (!(!n.push || !i.push)) {
    var a = n.length, o = i.length;
    if (a !== o) {
      var s = a > o;
      if (s)
        n.length = o;
      else
        for (var u = a; u < o; u++)
          n.push(r === 1 ? i[u] : Gu.call(i[u]));
    }
    for (var l = n[0] && n[0].length, u = 0; u < n.length; u++)
      if (r === 1)
        isNaN(n[u]) && (n[u] = i[u]);
      else
        for (var f = 0; f < l; f++)
          isNaN(n[u][f]) && (n[u][f] = i[u][f]);
  }
}
function Ua(e) {
  if (Ot(e)) {
    var t = e.length;
    if (Ot(e[0])) {
      for (var r = [], n = 0; n < t; n++)
        r.push(Gu.call(e[n]));
      return r;
    }
    return Gu.call(e);
  }
  return e;
}
function Ya(e) {
  return e[0] = Math.floor(e[0]) || 0, e[1] = Math.floor(e[1]) || 0, e[2] = Math.floor(e[2]) || 0, e[3] = e[3] == null ? 1 : e[3], "rgba(" + e.join(",") + ")";
}
function Rm(e) {
  return Ot(e && e[0]) ? 2 : 1;
}
var va = 0, qa = 1, Cp = 2, hi = 3, Vu = 4, Wu = 5, Ch = 6;
function Mh(e) {
  return e === Vu || e === Wu;
}
function da(e) {
  return e === qa || e === Cp;
}
var $n = [0, 0, 0, 0], Em = (function() {
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
  }, e.prototype.addKeyframe = function(t, r, n) {
    this._needsSort = !0;
    var i = this.keyframes, a = i.length, o = !1, s = Ch, u = r;
    if (Ot(r)) {
      var l = Rm(r);
      s = l, (l === 1 && !ut(r[0]) || l === 2 && !ut(r[0][0])) && (o = !0);
    } else if (ut(r) && !Ii(r))
      s = va;
    else if (B(r))
      if (!isNaN(+r))
        s = va;
      else {
        var f = qt(r);
        f && (u = f, s = hi);
      }
    else if (qi(r)) {
      var h = E({}, u);
      h.colorStops = F(r.colorStops, function(c) {
        return {
          offset: c.offset,
          color: qt(c.color)
        };
      }), Dm(r) ? s = Vu : Pm(r) && (s = Wu), u = h;
    }
    a === 0 ? this.valType = s : (s !== this.valType || s === Ch) && (o = !0), this.discrete = this.discrete || o;
    var v = {
      time: t,
      value: u,
      rawValue: r,
      percent: 0
    };
    return n && (v.easing = n, v.easingFunc = U(n) ? n : _i[n] || Sp(n)), i.push(v), v;
  }, e.prototype.prepare = function(t, r) {
    var n = this.keyframes;
    this._needsSort && n.sort(function(y, p) {
      return y.time - p.time;
    });
    for (var i = this.valType, a = n.length, o = n[a - 1], s = this.discrete, u = da(i), l = Mh(i), f = 0; f < a; f++) {
      var h = n[f], v = h.value, c = o.value;
      h.percent = h.time / t, s || (u && f !== a - 1 ? Lm(v, c, i) : l && Am(v.colorStops, c.colorStops));
    }
    if (!s && i !== Wu && r && this.needsAnimate() && r.needsAnimate() && i === r.valType && !r._finished) {
      this._additiveTrack = r;
      for (var d = n[0].value, f = 0; f < a; f++)
        i === va ? n[f].additiveValue = n[f].value - d : i === hi ? n[f].additiveValue = ca([], n[f].value, d, -1) : da(i) && (n[f].additiveValue = i === qa ? ca([], n[f].value, d, -1) : xh([], n[f].value, d, -1));
    }
  }, e.prototype.step = function(t, r) {
    if (!this._finished) {
      this._additiveTrack && this._additiveTrack._finished && (this._additiveTrack = null);
      var n = this._additiveTrack != null, i = n ? "additiveValue" : "value", a = this.valType, o = this.keyframes, s = o.length, u = this.propName, l = a === hi, f, h = this._lastFr, v = Math.min, c, d;
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
        var _ = n ? this._additiveValue : l ? $n : t[u];
        if ((da(a) || l) && !_ && (_ = this._additiveValue = []), this.discrete)
          t[u] = g < 1 ? c.rawValue : d.rawValue;
        else if (da(a))
          a === qa ? xs(_, c[i], d[i], g) : Im(_, c[i], d[i], g);
        else if (Mh(a)) {
          var m = c[i], S = d[i], b = a === Vu;
          t[u] = {
            type: b ? "linear" : "radial",
            x: ze(m.x, S.x, g),
            y: ze(m.y, S.y, g),
            colorStops: F(m.colorStops, function(T, C) {
              var M = S.colorStops[C];
              return {
                offset: ze(T.offset, M.offset, g),
                color: Ya(xs([], T.color, M.color, g))
              };
            }),
            global: S.global
          }, b ? (t[u].x2 = ze(m.x2, S.x2, g), t[u].y2 = ze(m.y2, S.y2, g)) : t[u].r = ze(m.r, S.r, g);
        } else if (l)
          xs(_, c[i], d[i], g), n || (t[u] = Ya(_));
        else {
          var w = ze(c[i], d[i], g);
          n ? this._additiveValue = w : t[u] = w;
        }
        n && this._addToTarget(t);
      }
    }
  }, e.prototype._addToTarget = function(t) {
    var r = this.valType, n = this.propName, i = this._additiveValue;
    r === va ? t[n] = t[n] + i : r === hi ? (qt(t[n], $n), ca($n, $n, i, 1), t[n] = Ya($n)) : r === qa ? ca(t[n], t[n], i, 1) : r === Cp && xh(t[n], t[n], i, 1);
  }, e;
})(), Ul = (function() {
  function e(t, r, n, i) {
    if (this._tracks = {}, this._trackKeys = [], this._maxTime = 0, this._started = 0, this._clip = null, this._target = t, this._loop = r, r && i) {
      Ro("Can' use additive animation on looped animation.");
      return;
    }
    this._additiveAnimators = i, this._allowDiscrete = n;
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
  }, e.prototype.when = function(t, r, n) {
    return this.whenWithKeys(t, r, ct(r), n);
  }, e.prototype.whenWithKeys = function(t, r, n, i) {
    for (var a = this._tracks, o = 0; o < n.length; o++) {
      var s = n[o], u = a[s];
      if (!u) {
        u = a[s] = new Em(s);
        var l = void 0, f = this._getAdditiveTrack(s);
        if (f) {
          var h = f.keyframes, v = h[h.length - 1];
          l = v && v.value, f.valType === hi && l && (l = Ya(l));
        } else
          l = this._target[s];
        if (l == null)
          continue;
        t > 0 && u.addKeyframe(0, Ua(l), i), this._trackKeys.push(s);
      }
      u.addKeyframe(t, Ua(r[s]), i);
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
      for (var r = t.length, n = 0; n < r; n++)
        t[n].call(this);
  }, e.prototype._abortedCallback = function() {
    this._setTracksFinished();
    var t = this.animation, r = this._abortedCbs;
    if (t && t.removeClip(this._clip), this._clip = null, r)
      for (var n = 0; n < r.length; n++)
        r[n].call(this);
  }, e.prototype._setTracksFinished = function() {
    for (var t = this._tracks, r = this._trackKeys, n = 0; n < r.length; n++)
      t[r[n]].setFinished();
  }, e.prototype._getAdditiveTrack = function(t) {
    var r, n = this._additiveAnimators;
    if (n)
      for (var i = 0; i < n.length; i++) {
        var a = n[i].getTrack(t);
        a && (r = a);
      }
    return r;
  }, e.prototype.start = function(t) {
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
        var v = new gm({
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
        this._clip = v, this.animation && this.animation.addClip(v), t && v.setEasing(t);
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
    return F(this._trackKeys, function(r) {
      return t._tracks[r];
    });
  }, e.prototype.stopTracks = function(t, r) {
    if (!t.length || !this._clip)
      return !0;
    for (var n = this._tracks, i = this._trackKeys, a = 0; a < t.length; a++) {
      var o = n[t[a]];
      o && !o.isFinished() && (r ? o.step(this._target, 1) : this._started === 1 && o.step(this._target, 0), o.setFinished());
    }
    for (var s = !0, a = 0; a < i.length; a++)
      if (!n[i[a]].isFinished()) {
        s = !1;
        break;
      }
    return s && this._abortedCallback(), s;
  }, e.prototype.saveTo = function(t, r, n) {
    if (t) {
      r = r || this._trackKeys;
      for (var i = 0; i < r.length; i++) {
        var a = r[i], o = this._tracks[a];
        if (!(!o || o.isFinished())) {
          var s = o.keyframes, u = s[n ? 0 : s.length - 1];
          u && (t[a] = Ua(u.rawValue));
        }
      }
    }
  }, e.prototype.__changeFinalValue = function(t, r) {
    r = r || ct(t);
    for (var n = 0; n < r.length; n++) {
      var i = r[n], a = this._tracks[i];
      if (a) {
        var o = a.keyframes;
        if (o.length > 1) {
          var s = o.pop();
          a.addKeyframe(s.time, t[i]), a.prepare(this._maxTime, a.getAdditiveTrack());
        }
      }
    }
  }, e;
})();
function wn() {
  return (/* @__PURE__ */ new Date()).getTime();
}
var Om = (function(e) {
  tt(t, e);
  function t(r) {
    var n = e.call(this) || this;
    return n._running = !1, n._time = 0, n._pausedTime = 0, n._pauseStart = 0, n._paused = !1, r = r || {}, n.stage = r.stage || {}, n;
  }
  return t.prototype.addClip = function(r) {
    r.animation && this.removeClip(r), this._head ? (this._tail.next = r, r.prev = this._tail, r.next = null, this._tail = r) : this._head = this._tail = r, r.animation = this;
  }, t.prototype.addAnimator = function(r) {
    r.animation = this;
    var n = r.getClip();
    n && this.addClip(n);
  }, t.prototype.removeClip = function(r) {
    if (r.animation) {
      var n = r.prev, i = r.next;
      n ? n.next = i : this._head = i, i ? i.prev = n : this._tail = n, r.next = r.prev = r.animation = null;
    }
  }, t.prototype.removeAnimator = function(r) {
    var n = r.getClip();
    n && this.removeClip(n), r.animation = null;
  }, t.prototype.update = function(r) {
    for (var n = wn() - this._pausedTime, i = n - this._time, a = this._head; a; ) {
      var o = a.next, s = a.step(n, i);
      s && (a.ondestroy(), this.removeClip(a)), a = o;
    }
    this._time = n, r || (this.trigger("frame", i), this.stage.update && this.stage.update());
  }, t.prototype._startLoop = function() {
    var r = this;
    this._running = !0;
    function n() {
      r._running && (so(n), !r._paused && r.update());
    }
    so(n);
  }, t.prototype.start = function() {
    this._running || (this._time = wn(), this._pausedTime = 0, this._startLoop());
  }, t.prototype.stop = function() {
    this._running = !1;
  }, t.prototype.pause = function() {
    this._paused || (this._pauseStart = wn(), this._paused = !0);
  }, t.prototype.resume = function() {
    this._paused && (this._pausedTime += wn() - this._pauseStart, this._paused = !1);
  }, t.prototype.clear = function() {
    for (var r = this._head; r; ) {
      var n = r.next;
      r.prev = r.next = r.animation = null, r = n;
    }
    this._head = this._tail = null;
  }, t.prototype.isFinished = function() {
    return this._head == null;
  }, t.prototype.animate = function(r, n) {
    n = n || {}, this.start();
    var i = new Ul(r, n.loop);
    return this.addAnimator(i), i;
  }, t;
})(Le), km = 300, Cs = z.domSupported, Ms = (function() {
  var e = [
    "click",
    "dblclick",
    "mousewheel",
    "wheel",
    "mouseout",
    "mouseup",
    "mousedown",
    "mousemove",
    "contextmenu"
  ], t = [
    "touchstart",
    "touchend",
    "touchmove"
  ], r = {
    pointerdown: 1,
    pointerup: 1,
    pointermove: 1,
    pointerout: 1
  }, n = F(e, function(i) {
    var a = i.replace("mouse", "pointer");
    return r.hasOwnProperty(a) ? a : i;
  });
  return {
    mouse: e,
    touch: t,
    pointer: n
  };
})(), Dh = {
  mouse: ["mousemove", "mouseup"],
  pointer: ["pointermove", "pointerup"]
}, Ph = !1;
function Uu(e) {
  var t = e.pointerType;
  return t === "pen" || t === "touch";
}
function Bm(e) {
  e.touching = !0, e.touchTimer != null && (clearTimeout(e.touchTimer), e.touchTimer = null), e.touchTimer = setTimeout(function() {
    e.touching = !1, e.touchTimer = null;
  }, 700);
}
function Ds(e) {
  e && (e.zrByTouch = !0);
}
function Fm(e, t) {
  return Jt(e.dom, new Nm(e, t), !0);
}
function Mp(e, t) {
  for (var r = t, n = !1; r && r.nodeType !== 9 && !(n = r.domBelongToZr || r !== t && r === e.painterRoot); )
    r = r.parentNode;
  return n;
}
var Nm = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.stopPropagation = Lt, this.stopImmediatePropagation = Lt, this.preventDefault = Lt, this.type = r.type, this.target = this.currentTarget = t.dom, this.pointerType = r.pointerType, this.clientX = r.clientX, this.clientY = r.clientY;
  }
  return e;
})(), ve = {
  mousedown: function(e) {
    e = Jt(this.dom, e), this.__mayPointerCapture = [e.zrX, e.zrY], this.trigger("mousedown", e);
  },
  mousemove: function(e) {
    e = Jt(this.dom, e);
    var t = this.__mayPointerCapture;
    t && (e.zrX !== t[0] || e.zrY !== t[1]) && this.__togglePointerCapture(!0), this.trigger("mousemove", e);
  },
  mouseup: function(e) {
    e = Jt(this.dom, e), this.__togglePointerCapture(!1), this.trigger("mouseup", e);
  },
  mouseout: function(e) {
    e = Jt(this.dom, e);
    var t = e.toElement || e.relatedTarget;
    Mp(this, t) || (this.__pointerCapturing && (e.zrEventControl = "no_globalout"), this.trigger("mouseout", e));
  },
  wheel: function(e) {
    Ph = !0, e = Jt(this.dom, e), this.trigger("mousewheel", e);
  },
  mousewheel: function(e) {
    Ph || (e = Jt(this.dom, e), this.trigger("mousewheel", e));
  },
  touchstart: function(e) {
    e = Jt(this.dom, e), Ds(e), this.__lastTouchMoment = /* @__PURE__ */ new Date(), this.handler.processGesture(e, "start"), ve.mousemove.call(this, e), ve.mousedown.call(this, e);
  },
  touchmove: function(e) {
    e = Jt(this.dom, e), Ds(e), this.handler.processGesture(e, "change"), ve.mousemove.call(this, e);
  },
  touchend: function(e) {
    e = Jt(this.dom, e), Ds(e), this.handler.processGesture(e, "end"), ve.mouseup.call(this, e), +/* @__PURE__ */ new Date() - +this.__lastTouchMoment < km && ve.click.call(this, e);
  },
  pointerdown: function(e) {
    ve.mousedown.call(this, e);
  },
  pointermove: function(e) {
    Uu(e) || ve.mousemove.call(this, e);
  },
  pointerup: function(e) {
    ve.mouseup.call(this, e);
  },
  pointerout: function(e) {
    Uu(e) || ve.mouseout.call(this, e);
  }
};
x(["click", "dblclick", "contextmenu"], function(e) {
  ve[e] = function(t) {
    t = Jt(this.dom, t), this.trigger(e, t);
  };
});
var Yu = {
  pointermove: function(e) {
    Uu(e) || Yu.mousemove.call(this, e);
  },
  pointerup: function(e) {
    Yu.mouseup.call(this, e);
  },
  mousemove: function(e) {
    this.trigger("mousemove", e);
  },
  mouseup: function(e) {
    var t = this.__pointerCapturing;
    this.__togglePointerCapture(!1), this.trigger("mouseup", e), t && (e.zrEventControl = "only_globalout", this.trigger("mouseout", e));
  }
};
function zm(e, t) {
  var r = t.domHandlers;
  z.pointerEventsSupported ? x(Ms.pointer, function(n) {
    Xa(t, n, function(i) {
      r[n].call(e, i);
    });
  }) : (z.touchEventsSupported && x(Ms.touch, function(n) {
    Xa(t, n, function(i) {
      r[n].call(e, i), Bm(t);
    });
  }), x(Ms.mouse, function(n) {
    Xa(t, n, function(i) {
      i = Vl(i), t.touching || r[n].call(e, i);
    });
  }));
}
function Hm(e, t) {
  z.pointerEventsSupported ? x(Dh.pointer, r) : z.touchEventsSupported || x(Dh.mouse, r);
  function r(n) {
    function i(a) {
      a = Vl(a), Mp(e, a.target) || (a = Fm(e, a), t.domHandlers[n].call(e, a));
    }
    Xa(t, n, i, { capture: !0 });
  }
}
function Xa(e, t, r, n) {
  e.mounted[t] = r, e.listenerOpts[t] = n, Z0(e.domTarget, t, r, n);
}
function Ps(e) {
  var t = e.mounted;
  for (var r in t)
    t.hasOwnProperty(r) && j0(e.domTarget, r, t[r], e.listenerOpts[r]);
  e.mounted = {};
}
var Ih = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.mounted = {}, this.listenerOpts = {}, this.touching = !1, this.domTarget = t, this.domHandlers = r;
  }
  return e;
})(), Gm = (function(e) {
  tt(t, e);
  function t(r, n) {
    var i = e.call(this) || this;
    return i.__pointerCapturing = !1, i.dom = r, i.painterRoot = n, i._localHandlerScope = new Ih(r, ve), Cs && (i._globalHandlerScope = new Ih(document, Yu)), zm(i, i._localHandlerScope), i;
  }
  return t.prototype.dispose = function() {
    Ps(this._localHandlerScope), Cs && Ps(this._globalHandlerScope);
  }, t.prototype.setCursor = function(r) {
    this.dom.style && (this.dom.style.cursor = r || "default");
  }, t.prototype.__togglePointerCapture = function(r) {
    if (this.__mayPointerCapture = null, Cs && +this.__pointerCapturing ^ +r) {
      this.__pointerCapturing = r;
      var n = this._globalHandlerScope;
      r ? Hm(this, n) : Ps(n);
    }
  }, t;
})(Le), Dp = 1;
z.hasGlobalWindow && (Dp = Math.max(window.devicePixelRatio || window.screen && window.screen.deviceXDPI / window.screen.logicalXDPI || 1, 1));
var ho = Dp, qu = 0.4, Xu = "#333", $u = "#ccc", Vm = "#eee", Ah = $i, Lh = 5e-5;
function br(e) {
  return e > Lh || e < -Lh;
}
var Tr = [], on = [], Is = sr(), As = Math.abs, Yl = (function() {
  function e() {
  }
  return e.prototype.getLocalTransform = function(t) {
    return e.getLocalTransform(this, t);
  }, e.prototype.setPosition = function(t) {
    this.x = t[0], this.y = t[1];
  }, e.prototype.setScale = function(t) {
    this.scaleX = t[0], this.scaleY = t[1];
  }, e.prototype.setSkew = function(t) {
    this.skewX = t[0], this.skewY = t[1];
  }, e.prototype.setOrigin = function(t) {
    this.originX = t[0], this.originY = t[1];
  }, e.prototype.needLocalTransform = function() {
    return br(this.rotation) || br(this.x) || br(this.y) || br(this.scaleX - 1) || br(this.scaleY - 1) || br(this.skewX) || br(this.skewY);
  }, e.prototype.updateTransform = function() {
    var t = this.parent && this.parent.transform, r = this.needLocalTransform(), n = this.transform;
    if (!(r || t)) {
      n && (Ah(n), this.invTransform = null);
      return;
    }
    n = n || sr(), r ? this.getLocalTransform(n) : Ah(n), t && (r ? ur(n, t, n) : Wl(n, t)), this.transform = n, this._resolveGlobalScaleRatio(n);
  }, e.prototype._resolveGlobalScaleRatio = function(t) {
    var r = this.globalScaleRatio;
    if (r != null && r !== 1) {
      this.getGlobalScale(Tr);
      var n = Tr[0] < 0 ? -1 : 1, i = Tr[1] < 0 ? -1 : 1, a = ((Tr[0] - n) * r + n) / Tr[0] || 0, o = ((Tr[1] - i) * r + i) / Tr[1] || 0;
      t[0] *= a, t[1] *= a, t[2] *= o, t[3] *= o;
    }
    this.invTransform = this.invTransform || sr(), ko(this.invTransform, t);
  }, e.prototype.getComputedTransform = function() {
    for (var t = this, r = []; t; )
      r.push(t), t = t.parent;
    for (; t = r.pop(); )
      t.updateTransform();
    return this.transform;
  }, e.prototype.setLocalTransform = function(t) {
    if (t) {
      var r = t[0] * t[0] + t[1] * t[1], n = t[2] * t[2] + t[3] * t[3], i = Math.atan2(t[1], t[0]), a = Math.PI / 2 + i - Math.atan2(t[3], t[2]);
      n = Math.sqrt(n) * Math.cos(a), r = Math.sqrt(r), this.skewX = a, this.skewY = 0, this.rotation = -i, this.x = +t[4], this.y = +t[5], this.scaleX = r, this.scaleY = n, this.originX = 0, this.originY = 0;
    }
  }, e.prototype.decomposeTransform = function() {
    if (this.transform) {
      var t = this.parent, r = this.transform;
      t && t.transform && (t.invTransform = t.invTransform || sr(), ur(on, t.invTransform, r), r = on);
      var n = this.originX, i = this.originY;
      (n || i) && (Is[4] = n, Is[5] = i, ur(on, r, Is), on[4] -= n, on[5] -= i, r = on), this.setLocalTransform(r);
    }
  }, e.prototype.getGlobalScale = function(t) {
    var r = this.transform;
    return t = t || [], r ? (t[0] = Math.sqrt(r[0] * r[0] + r[1] * r[1]), t[1] = Math.sqrt(r[2] * r[2] + r[3] * r[3]), r[0] < 0 && (t[0] = -t[0]), r[3] < 0 && (t[1] = -t[1]), t) : (t[0] = 1, t[1] = 1, t);
  }, e.prototype.transformCoordToLocal = function(t, r) {
    var n = [t, r], i = this.invTransform;
    return i && Yt(n, n, i), n;
  }, e.prototype.transformCoordToGlobal = function(t, r) {
    var n = [t, r], i = this.transform;
    return i && Yt(n, n, i), n;
  }, e.prototype.getLineScale = function() {
    var t = this.transform;
    return t && As(t[0] - 1) > 1e-10 && As(t[3] - 1) > 1e-10 ? Math.sqrt(As(t[0] * t[3] - t[2] * t[1])) : 1;
  }, e.prototype.copyTransform = function(t) {
    Wm(this, t);
  }, e.getLocalTransform = function(t, r) {
    r = r || [];
    var n = t.originX || 0, i = t.originY || 0, a = t.scaleX, o = t.scaleY, s = t.anchorX, u = t.anchorY, l = t.rotation || 0, f = t.x, h = t.y, v = t.skewX ? Math.tan(t.skewX) : 0, c = t.skewY ? Math.tan(-t.skewY) : 0;
    if (n || i || s || u) {
      var d = n + s, y = i + u;
      r[4] = -d * a - v * y * o, r[5] = -y * o - c * d * a;
    } else
      r[4] = r[5] = 0;
    return r[0] = a, r[3] = o, r[1] = c * a, r[2] = v * o, l && Oo(r, r, l), r[4] += n + f, r[5] += i + h, r;
  }, e.initDefaultProps = (function() {
    var t = e.prototype;
    t.scaleX = t.scaleY = t.globalScaleRatio = 1, t.x = t.y = t.originX = t.originY = t.skewX = t.skewY = t.rotation = t.anchorX = t.anchorY = 0;
  })(), e;
})(), Ei = [
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
function Wm(e, t) {
  for (var r = 0; r < Ei.length; r++) {
    var n = Ei[r];
    e[n] = t[n];
  }
}
var Rh = {};
function Xt(e, t) {
  t = t || jr;
  var r = Rh[t];
  r || (r = Rh[t] = new Zi(500));
  var n = r.get(e);
  return n == null && (n = Ve.measureText(e, t).width, r.put(e, n)), n;
}
function Eh(e, t, r, n) {
  var i = Xt(e, t), a = Xl(t), o = ci(0, i, r), s = Sn(0, a, n), u = new j(o, s, i, a);
  return u;
}
function ql(e, t, r, n) {
  var i = ((e || "") + "").split(`
`), a = i.length;
  if (a === 1)
    return Eh(i[0], t, r, n);
  for (var o = new j(0, 0, 0, 0), s = 0; s < i.length; s++) {
    var u = Eh(i[s], t, r, n);
    s === 0 ? o.copy(u) : o.union(u);
  }
  return o;
}
function ci(e, t, r) {
  return r === "right" ? e -= t : r === "center" && (e -= t / 2), e;
}
function Sn(e, t, r) {
  return r === "middle" ? e -= t / 2 : r === "bottom" && (e -= t), e;
}
function Xl(e) {
  return Xt("国", e);
}
function Kr(e, t) {
  return typeof e == "string" ? e.lastIndexOf("%") >= 0 ? parseFloat(e) / 100 * t : parseFloat(e) : e;
}
function Pp(e, t, r) {
  var n = t.position || "inside", i = t.distance != null ? t.distance : 5, a = r.height, o = r.width, s = a / 2, u = r.x, l = r.y, f = "left", h = "top";
  if (n instanceof Array)
    u += Kr(n[0], r.width), l += Kr(n[1], r.height), f = null, h = null;
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
  return e = e || {}, e.x = u, e.y = l, e.align = f, e.verticalAlign = h, e;
}
var Ls = "__zr_normal__", Rs = Ei.concat(["ignore"]), Um = Pe(Ei, function(e, t) {
  return e[t] = !0, e;
}, { ignore: !1 }), sn = {}, Ym = new j(0, 0, 0, 0), Bo = (function() {
  function e(t) {
    this.id = Bl(), this.animators = [], this.currentStates = [], this.states = {}, this._init(t);
  }
  return e.prototype._init = function(t) {
    this.attr(t);
  }, e.prototype.drift = function(t, r, n) {
    switch (this.draggable) {
      case "horizontal":
        r = 0;
        break;
      case "vertical":
        t = 0;
        break;
    }
    var i = this.transform;
    i || (i = this.transform = [1, 0, 0, 1, 0, 0]), i[4] += t, i[5] += r, this.decomposeTransform(), this.markRedraw();
  }, e.prototype.beforeUpdate = function() {
  }, e.prototype.afterUpdate = function() {
  }, e.prototype.update = function() {
    this.updateTransform(), this.__dirty && this.updateInnerText();
  }, e.prototype.updateInnerText = function(t) {
    var r = this._textContent;
    if (r && (!r.ignore || t)) {
      this.textConfig || (this.textConfig = {});
      var n = this.textConfig, i = n.local, a = r.innerTransformable, o = void 0, s = void 0, u = !1;
      a.parent = i ? this : null;
      var l = !1;
      if (a.copyTransform(r), n.position != null) {
        var f = Ym;
        n.layoutRect ? f.copy(n.layoutRect) : f.copy(this.getBoundingRect()), i || f.applyTransform(this.transform), this.calculateTextPosition ? this.calculateTextPosition(sn, n, f) : Pp(sn, n, f), a.x = sn.x, a.y = sn.y, o = sn.align, s = sn.verticalAlign;
        var h = n.origin;
        if (h && n.rotation != null) {
          var v = void 0, c = void 0;
          h === "center" ? (v = f.width * 0.5, c = f.height * 0.5) : (v = Kr(h[0], f.width), c = Kr(h[1], f.height)), l = !0, a.originX = -a.x + v + (i ? 0 : f.x), a.originY = -a.y + c + (i ? 0 : f.y);
        }
      }
      n.rotation != null && (a.rotation = n.rotation);
      var d = n.offset;
      d && (a.x += d[0], a.y += d[1], l || (a.originX = -d[0], a.originY = -d[1]));
      var y = n.inside == null ? typeof n.position == "string" && n.position.indexOf("inside") >= 0 : n.inside, p = this._innerTextDefaultStyle || (this._innerTextDefaultStyle = {}), g = void 0, _ = void 0, m = void 0;
      y && this.canBeInsideText() ? (g = n.insideFill, _ = n.insideStroke, (g == null || g === "auto") && (g = this.getInsideTextFill()), (_ == null || _ === "auto") && (_ = this.getInsideTextStroke(g), m = !0)) : (g = n.outsideFill, _ = n.outsideStroke, (g == null || g === "auto") && (g = this.getOutsideFill()), (_ == null || _ === "auto") && (_ = this.getOutsideStroke(g), m = !0)), g = g || "#000", (g !== p.fill || _ !== p.stroke || m !== p.autoStroke || o !== p.align || s !== p.verticalAlign) && (u = !0, p.fill = g, p.stroke = _, p.autoStroke = m, p.align = o, p.verticalAlign = s, r.setDefaultTextStyle(p)), r.__dirty |= Ut, u && r.dirtyStyle(!0);
    }
  }, e.prototype.canBeInsideText = function() {
    return !0;
  }, e.prototype.getInsideTextFill = function() {
    return "#fff";
  }, e.prototype.getInsideTextStroke = function(t) {
    return "#000";
  }, e.prototype.getOutsideFill = function() {
    return this.__zr && this.__zr.isDarkMode() ? $u : Xu;
  }, e.prototype.getOutsideStroke = function(t) {
    var r = this.__zr && this.__zr.getBackgroundColor(), n = typeof r == "string" && qt(r);
    n || (n = [255, 255, 255, 1]);
    for (var i = n[3], a = this.__zr.isDarkMode(), o = 0; o < 3; o++)
      n[o] = n[o] * i + (a ? 0 : 255) * (1 - i);
    return n[3] = 1, cr(n, "rgba");
  }, e.prototype.traverse = function(t, r) {
  }, e.prototype.attrKV = function(t, r) {
    t === "textConfig" ? this.setTextConfig(r) : t === "textContent" ? this.setTextContent(r) : t === "clipPath" ? this.setClipPath(r) : t === "extra" ? (this.extra = this.extra || {}, E(this.extra, r)) : this[t] = r;
  }, e.prototype.hide = function() {
    this.ignore = !0, this.markRedraw();
  }, e.prototype.show = function() {
    this.ignore = !1, this.markRedraw();
  }, e.prototype.attr = function(t, r) {
    if (typeof t == "string")
      this.attrKV(t, r);
    else if (N(t))
      for (var n = t, i = ct(n), a = 0; a < i.length; a++) {
        var o = i[a];
        this.attrKV(o, t[o]);
      }
    return this.markRedraw(), this;
  }, e.prototype.saveCurrentToNormalState = function(t) {
    this._innerSaveToNormal(t);
    for (var r = this._normalState, n = 0; n < this.animators.length; n++) {
      var i = this.animators[n], a = i.__fromStateTransition;
      if (!(i.getLoop() || a && a !== Ls)) {
        var o = i.targetName, s = o ? r[o] : r;
        i.saveTo(s);
      }
    }
  }, e.prototype._innerSaveToNormal = function(t) {
    var r = this._normalState;
    r || (r = this._normalState = {}), t.textConfig && !r.textConfig && (r.textConfig = this.textConfig), this._savePrimaryToNormal(t, r, Rs);
  }, e.prototype._savePrimaryToNormal = function(t, r, n) {
    for (var i = 0; i < n.length; i++) {
      var a = n[i];
      t[a] != null && !(a in r) && (r[a] = this[a]);
    }
  }, e.prototype.hasState = function() {
    return this.currentStates.length > 0;
  }, e.prototype.getState = function(t) {
    return this.states[t];
  }, e.prototype.ensureState = function(t) {
    var r = this.states;
    return r[t] || (r[t] = {}), r[t];
  }, e.prototype.clearStates = function(t) {
    this.useState(Ls, !1, t);
  }, e.prototype.useState = function(t, r, n, i) {
    var a = t === Ls, o = this.hasState();
    if (!(!o && a)) {
      var s = this.currentStates, u = this.stateTransition;
      if (!(rt(s, t) >= 0 && (r || s.length === 1))) {
        var l;
        if (this.stateProxy && !a && (l = this.stateProxy(t)), l || (l = this.states && this.states[t]), !l && !a) {
          Ro("State " + t + " not exists.");
          return;
        }
        a || this.saveCurrentToNormalState(l);
        var f = !!(l && l.hoverLayer || i);
        f && this._toggleHoverLayerFlag(!0), this._applyStateObj(t, l, this._normalState, r, !n && !this.__inHover && u && u.duration > 0, u);
        var h = this._textContent, v = this._textGuide;
        return h && h.useState(t, r, n, f), v && v.useState(t, r, n, f), a ? (this.currentStates = [], this._normalState = {}) : r ? this.currentStates.push(t) : this.currentStates = [t], this._updateAnimationTargets(), this.markRedraw(), !f && this.__inHover && (this._toggleHoverLayerFlag(!1), this.__dirty &= ~Ut), l;
      }
    }
  }, e.prototype.useStates = function(t, r, n) {
    if (!t.length)
      this.clearStates();
    else {
      var i = [], a = this.currentStates, o = t.length, s = o === a.length;
      if (s) {
        for (var u = 0; u < o; u++)
          if (t[u] !== a[u]) {
            s = !1;
            break;
          }
      }
      if (s)
        return;
      for (var u = 0; u < o; u++) {
        var l = t[u], f = void 0;
        this.stateProxy && (f = this.stateProxy(l, t)), f || (f = this.states[l]), f && i.push(f);
      }
      var h = i[o - 1], v = !!(h && h.hoverLayer || n);
      v && this._toggleHoverLayerFlag(!0);
      var c = this._mergeStates(i), d = this.stateTransition;
      this.saveCurrentToNormalState(c), this._applyStateObj(t.join(","), c, this._normalState, !1, !r && !this.__inHover && d && d.duration > 0, d);
      var y = this._textContent, p = this._textGuide;
      y && y.useStates(t, r, v), p && p.useStates(t, r, v), this._updateAnimationTargets(), this.currentStates = t.slice(), this.markRedraw(), !v && this.__inHover && (this._toggleHoverLayerFlag(!1), this.__dirty &= ~Ut);
    }
  }, e.prototype.isSilent = function() {
    for (var t = this.silent, r = this.parent; !t && r; ) {
      if (r.silent) {
        t = !0;
        break;
      }
      r = r.parent;
    }
    return t;
  }, e.prototype._updateAnimationTargets = function() {
    for (var t = 0; t < this.animators.length; t++) {
      var r = this.animators[t];
      r.targetName && r.changeTarget(this[r.targetName]);
    }
  }, e.prototype.removeState = function(t) {
    var r = rt(this.currentStates, t);
    if (r >= 0) {
      var n = this.currentStates.slice();
      n.splice(r, 1), this.useStates(n);
    }
  }, e.prototype.replaceState = function(t, r, n) {
    var i = this.currentStates.slice(), a = rt(i, t), o = rt(i, r) >= 0;
    a >= 0 ? o ? i.splice(a, 1) : i[a] = r : n && !o && i.push(r), this.useStates(i);
  }, e.prototype.toggleState = function(t, r) {
    r ? this.useState(t, !0) : this.removeState(t);
  }, e.prototype._mergeStates = function(t) {
    for (var r = {}, n, i = 0; i < t.length; i++) {
      var a = t[i];
      E(r, a), a.textConfig && (n = n || {}, E(n, a.textConfig));
    }
    return n && (r.textConfig = n), r;
  }, e.prototype._applyStateObj = function(t, r, n, i, a, o) {
    var s = !(r && i);
    r && r.textConfig ? (this.textConfig = E({}, i ? this.textConfig : n.textConfig), E(this.textConfig, r.textConfig)) : s && n.textConfig && (this.textConfig = n.textConfig);
    for (var u = {}, l = !1, f = 0; f < Rs.length; f++) {
      var h = Rs[f], v = a && Um[h];
      r && r[h] != null ? v ? (l = !0, u[h] = r[h]) : this[h] = r[h] : s && n[h] != null && (v ? (l = !0, u[h] = n[h]) : this[h] = n[h]);
    }
    if (!a)
      for (var f = 0; f < this.animators.length; f++) {
        var c = this.animators[f], d = c.targetName;
        c.getLoop() || c.__changeFinalValue(d ? (r || n)[d] : r || n);
      }
    l && this._transitionState(t, u, o);
  }, e.prototype._attachComponent = function(t) {
    if (!(t.__zr && !t.__hostTarget) && t !== this) {
      var r = this.__zr;
      r && t.addSelfToZr(r), t.__zr = r, t.__hostTarget = this;
    }
  }, e.prototype._detachComponent = function(t) {
    t.__zr && t.removeSelfFromZr(t.__zr), t.__zr = null, t.__hostTarget = null;
  }, e.prototype.getClipPath = function() {
    return this._clipPath;
  }, e.prototype.setClipPath = function(t) {
    this._clipPath && this._clipPath !== t && this.removeClipPath(), this._attachComponent(t), this._clipPath = t, this.markRedraw();
  }, e.prototype.removeClipPath = function() {
    var t = this._clipPath;
    t && (this._detachComponent(t), this._clipPath = null, this.markRedraw());
  }, e.prototype.getTextContent = function() {
    return this._textContent;
  }, e.prototype.setTextContent = function(t) {
    var r = this._textContent;
    r !== t && (r && r !== t && this.removeTextContent(), t.innerTransformable = new Yl(), this._attachComponent(t), this._textContent = t, this.markRedraw());
  }, e.prototype.setTextConfig = function(t) {
    this.textConfig || (this.textConfig = {}), E(this.textConfig, t), this.markRedraw();
  }, e.prototype.removeTextConfig = function() {
    this.textConfig = null, this.markRedraw();
  }, e.prototype.removeTextContent = function() {
    var t = this._textContent;
    t && (t.innerTransformable = null, this._detachComponent(t), this._textContent = null, this._innerTextDefaultStyle = null, this.markRedraw());
  }, e.prototype.getTextGuideLine = function() {
    return this._textGuide;
  }, e.prototype.setTextGuideLine = function(t) {
    this._textGuide && this._textGuide !== t && this.removeTextGuideLine(), this._attachComponent(t), this._textGuide = t, this.markRedraw();
  }, e.prototype.removeTextGuideLine = function() {
    var t = this._textGuide;
    t && (this._detachComponent(t), this._textGuide = null, this.markRedraw());
  }, e.prototype.markRedraw = function() {
    this.__dirty |= Ut;
    var t = this.__zr;
    t && (this.__inHover ? t.refreshHover() : t.refresh()), this.__hostTarget && this.__hostTarget.markRedraw();
  }, e.prototype.dirty = function() {
    this.markRedraw();
  }, e.prototype._toggleHoverLayerFlag = function(t) {
    this.__inHover = t;
    var r = this._textContent, n = this._textGuide;
    r && (r.__inHover = t), n && (n.__inHover = t);
  }, e.prototype.addSelfToZr = function(t) {
    if (this.__zr !== t) {
      this.__zr = t;
      var r = this.animators;
      if (r)
        for (var n = 0; n < r.length; n++)
          t.animation.addAnimator(r[n]);
      this._clipPath && this._clipPath.addSelfToZr(t), this._textContent && this._textContent.addSelfToZr(t), this._textGuide && this._textGuide.addSelfToZr(t);
    }
  }, e.prototype.removeSelfFromZr = function(t) {
    if (this.__zr) {
      this.__zr = null;
      var r = this.animators;
      if (r)
        for (var n = 0; n < r.length; n++)
          t.animation.removeAnimator(r[n]);
      this._clipPath && this._clipPath.removeSelfFromZr(t), this._textContent && this._textContent.removeSelfFromZr(t), this._textGuide && this._textGuide.removeSelfFromZr(t);
    }
  }, e.prototype.animate = function(t, r, n) {
    var i = t ? this[t] : this, a = new Ul(i, r, n);
    return t && (a.targetName = t), this.addAnimator(a, t), a;
  }, e.prototype.addAnimator = function(t, r) {
    var n = this.__zr, i = this;
    t.during(function() {
      i.updateDuringAnimation(r);
    }).done(function() {
      var a = i.animators, o = rt(a, t);
      o >= 0 && a.splice(o, 1);
    }), this.animators.push(t), n && n.animation.addAnimator(t), n && n.wakeUp();
  }, e.prototype.updateDuringAnimation = function(t) {
    this.markRedraw();
  }, e.prototype.stopAnimation = function(t, r) {
    for (var n = this.animators, i = n.length, a = [], o = 0; o < i; o++) {
      var s = n[o];
      !t || t === s.scope ? s.stop(r) : a.push(s);
    }
    return this.animators = a, this;
  }, e.prototype.animateTo = function(t, r, n) {
    Es(this, t, r, n);
  }, e.prototype.animateFrom = function(t, r, n) {
    Es(this, t, r, n, !0);
  }, e.prototype._transitionState = function(t, r, n, i) {
    for (var a = Es(this, r, n, i), o = 0; o < a.length; o++)
      a[o].__fromStateTransition = t;
  }, e.prototype.getBoundingRect = function() {
    return null;
  }, e.prototype.getPaintRect = function() {
    return null;
  }, e.initDefaultProps = (function() {
    var t = e.prototype;
    t.type = "element", t.name = "", t.ignore = t.silent = t.isGroup = t.draggable = t.dragging = t.ignoreClip = t.__inHover = !1, t.__dirty = Ut;
    function r(n, i, a, o) {
      Object.defineProperty(t, n, {
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
  })(), e;
})();
ye(Bo, Le);
ye(Bo, Yl);
function Es(e, t, r, n, i) {
  r = r || {};
  var a = [];
  Ip(e, "", e, t, r, n, a, i);
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
function Os(e, t, r) {
  for (var n = 0; n < r; n++)
    e[n] = t[n];
}
function qm(e) {
  return Ot(e[0]);
}
function Xm(e, t, r) {
  if (Ot(t[r]))
    if (Ot(e[r]) || (e[r] = []), kt(t[r])) {
      var n = t[r].length;
      e[r].length !== n && (e[r] = new t[r].constructor(n), Os(e[r], t[r], n));
    } else {
      var i = t[r], a = e[r], o = i.length;
      if (qm(i))
        for (var s = i[0].length, u = 0; u < o; u++)
          a[u] ? Os(a[u], i[u], s) : a[u] = Array.prototype.slice.call(i[u]);
      else
        Os(a, i, o);
      a.length = i.length;
    }
  else
    e[r] = t[r];
}
function $m(e, t) {
  return e === t || Ot(e) && Ot(t) && Zm(e, t);
}
function Zm(e, t) {
  var r = e.length;
  if (r !== t.length)
    return !1;
  for (var n = 0; n < r; n++)
    if (e[n] !== t[n])
      return !1;
  return !0;
}
function Ip(e, t, r, n, i, a, o, s) {
  for (var u = ct(n), l = i.duration, f = i.delay, h = i.additive, v = i.setToFinal, c = !N(a), d = e.animators, y = [], p = 0; p < u.length; p++) {
    var g = u[p], _ = n[g];
    if (_ != null && r[g] != null && (c || a[g]))
      if (N(_) && !Ot(_) && !qi(_)) {
        if (t) {
          s || (r[g] = _, e.updateDuringAnimation(t));
          continue;
        }
        Ip(e, g, r[g], _, i, a && a[g], o, s);
      } else
        y.push(g);
    else s || (r[g] = _, e.updateDuringAnimation(t), y.push(g));
  }
  var m = y.length;
  if (!h && m)
    for (var S = 0; S < d.length; S++) {
      var b = d[S];
      if (b.targetName === t) {
        var w = b.stopTracks(y);
        if (w) {
          var T = rt(d, b);
          d.splice(T, 1);
        }
      }
    }
  if (i.force || (y = dt(y, function(P) {
    return !$m(n[P], r[P]);
  }), m = y.length), m > 0 || i.force && !o.length) {
    var C = void 0, M = void 0, D = void 0;
    if (s) {
      M = {}, v && (C = {});
      for (var S = 0; S < m; S++) {
        var g = y[S];
        M[g] = r[g], v ? C[g] = n[g] : r[g] = n[g];
      }
    } else if (v) {
      D = {};
      for (var S = 0; S < m; S++) {
        var g = y[S];
        D[g] = Ua(r[g]), Xm(r, n, g);
      }
    }
    var b = new Ul(r, !1, !1, h ? dt(d, function(A) {
      return A.targetName === t;
    }) : null);
    b.targetName = t, i.scope && (b.scope = i.scope), v && C && b.whenWithKeys(0, C, y), D && b.whenWithKeys(0, D, y), b.whenWithKeys(l ?? 500, s ? M : n, y).delay(f || 0), e.addAnimator(b, t), o.push(b);
  }
}
var oe = (function(e) {
  tt(t, e);
  function t(r) {
    var n = e.call(this) || this;
    return n.isGroup = !0, n._children = [], n.attr(r), n;
  }
  return t.prototype.childrenRef = function() {
    return this._children;
  }, t.prototype.children = function() {
    return this._children.slice();
  }, t.prototype.childAt = function(r) {
    return this._children[r];
  }, t.prototype.childOfName = function(r) {
    for (var n = this._children, i = 0; i < n.length; i++)
      if (n[i].name === r)
        return n[i];
  }, t.prototype.childCount = function() {
    return this._children.length;
  }, t.prototype.add = function(r) {
    return r && r !== this && r.parent !== this && (this._children.push(r), this._doAdd(r)), this;
  }, t.prototype.addBefore = function(r, n) {
    if (r && r !== this && r.parent !== this && n && n.parent === this) {
      var i = this._children, a = i.indexOf(n);
      a >= 0 && (i.splice(a, 0, r), this._doAdd(r));
    }
    return this;
  }, t.prototype.replace = function(r, n) {
    var i = rt(this._children, r);
    return i >= 0 && this.replaceAt(n, i), this;
  }, t.prototype.replaceAt = function(r, n) {
    var i = this._children, a = i[n];
    if (r && r !== this && r.parent !== this && r !== a) {
      i[n] = r, a.parent = null;
      var o = this.__zr;
      o && a.removeSelfFromZr(o), this._doAdd(r);
    }
    return this;
  }, t.prototype._doAdd = function(r) {
    r.parent && r.parent.remove(r), r.parent = this;
    var n = this.__zr;
    n && n !== r.__zr && r.addSelfToZr(n), n && n.refresh();
  }, t.prototype.remove = function(r) {
    var n = this.__zr, i = this._children, a = rt(i, r);
    return a < 0 ? this : (i.splice(a, 1), r.parent = null, n && r.removeSelfFromZr(n), n && n.refresh(), this);
  }, t.prototype.removeAll = function() {
    for (var r = this._children, n = this.__zr, i = 0; i < r.length; i++) {
      var a = r[i];
      n && a.removeSelfFromZr(n), a.parent = null;
    }
    return r.length = 0, this;
  }, t.prototype.eachChild = function(r, n) {
    for (var i = this._children, a = 0; a < i.length; a++) {
      var o = i[a];
      r.call(n, o, a);
    }
    return this;
  }, t.prototype.traverse = function(r, n) {
    for (var i = 0; i < this._children.length; i++) {
      var a = this._children[i], o = r.call(n, a);
      a.isGroup && !o && a.traverse(r, n);
    }
    return this;
  }, t.prototype.addSelfToZr = function(r) {
    e.prototype.addSelfToZr.call(this, r);
    for (var n = 0; n < this._children.length; n++) {
      var i = this._children[n];
      i.addSelfToZr(r);
    }
  }, t.prototype.removeSelfFromZr = function(r) {
    e.prototype.removeSelfFromZr.call(this, r);
    for (var n = 0; n < this._children.length; n++) {
      var i = this._children[n];
      i.removeSelfFromZr(r);
    }
  }, t.prototype.getBoundingRect = function(r) {
    for (var n = new j(0, 0, 0, 0), i = r || this._children, a = [], o = null, s = 0; s < i.length; s++) {
      var u = i[s];
      if (!(u.ignore || u.invisible)) {
        var l = u.getBoundingRect(), f = u.getLocalTransform(a);
        f ? (j.applyTransform(n, l, f), o = o || n.clone(), o.union(n)) : (o = o || l.clone(), o.union(l));
      }
    }
    return o || n;
  }, t;
})(Bo);
oe.prototype.type = "group";
/*!
* ZRender, a high performance 2d drawing library.
*
* Copyright (c) 2013, Baidu Inc.
* All rights reserved.
*
* LICENSE
* https://github.com/ecomfe/zrender/blob/master/LICENSE.txt
*/
var $a = {}, Hr = {};
function jm(e) {
  delete Hr[e];
}
function Km(e) {
  if (!e)
    return !1;
  if (typeof e == "string")
    return Ri(e, 1) < qu;
  if (e.colorStops) {
    for (var t = e.colorStops, r = 0, n = t.length, i = 0; i < n; i++)
      r += Ri(t[i].color, 1);
    return r /= n, r < qu;
  }
  return !1;
}
var Qm = (function() {
  function e(t, r, n) {
    var i = this;
    this._sleepAfterStill = 10, this._stillFrameAccum = 0, this._needsRefresh = !0, this._needsRefreshHover = !0, this._darkMode = !1, n = n || {}, this.dom = r, this.id = t;
    var a = new lm(), o = n.renderer || "canvas";
    $a[o] || (o = ct($a)[0]), n.useDirtyRect = n.useDirtyRect == null ? !1 : n.useDirtyRect;
    var s = new $a[o](r, a, n, t), u = n.ssr || s.ssrOnly;
    this.storage = a, this.painter = s;
    var l = !z.node && !z.worker && !u ? new Gm(s.getViewportRoot(), s.root) : null, f = n.useCoarsePointer, h = f == null || f === "auto" ? z.touchEventsSupported : !!f, v = 44, c;
    h && (c = Y(n.pointerSize, v)), this.handler = new cp(a, s, l, s.root, c), this.animation = new Om({
      stage: {
        update: u ? null : function() {
          return i._flush(!0);
        }
      }
    }), u || this.animation.start();
  }
  return e.prototype.add = function(t) {
    this._disposed || !t || (this.storage.addRoot(t), t.addSelfToZr(this), this.refresh());
  }, e.prototype.remove = function(t) {
    this._disposed || !t || (this.storage.delRoot(t), t.removeSelfFromZr(this), this.refresh());
  }, e.prototype.configLayer = function(t, r) {
    this._disposed || (this.painter.configLayer && this.painter.configLayer(t, r), this.refresh());
  }, e.prototype.setBackgroundColor = function(t) {
    this._disposed || (this.painter.setBackgroundColor && this.painter.setBackgroundColor(t), this.refresh(), this._backgroundColor = t, this._darkMode = Km(t));
  }, e.prototype.getBackgroundColor = function() {
    return this._backgroundColor;
  }, e.prototype.setDarkMode = function(t) {
    this._darkMode = t;
  }, e.prototype.isDarkMode = function() {
    return this._darkMode;
  }, e.prototype.refreshImmediately = function(t) {
    this._disposed || (t || this.animation.update(!0), this._needsRefresh = !1, this.painter.refresh(), this._needsRefresh = !1);
  }, e.prototype.refresh = function() {
    this._disposed || (this._needsRefresh = !0, this.animation.start());
  }, e.prototype.flush = function() {
    this._disposed || this._flush(!1);
  }, e.prototype._flush = function(t) {
    var r, n = wn();
    this._needsRefresh && (r = !0, this.refreshImmediately(t)), this._needsRefreshHover && (r = !0, this.refreshHoverImmediately());
    var i = wn();
    r ? (this._stillFrameAccum = 0, this.trigger("rendered", {
      elapsedTime: i - n
    })) : this._sleepAfterStill > 0 && (this._stillFrameAccum++, this._stillFrameAccum > this._sleepAfterStill && this.animation.stop());
  }, e.prototype.setSleepAfterStill = function(t) {
    this._sleepAfterStill = t;
  }, e.prototype.wakeUp = function() {
    this._disposed || (this.animation.start(), this._stillFrameAccum = 0);
  }, e.prototype.refreshHover = function() {
    this._needsRefreshHover = !0;
  }, e.prototype.refreshHoverImmediately = function() {
    this._disposed || (this._needsRefreshHover = !1, this.painter.refreshHover && this.painter.getType() === "canvas" && this.painter.refreshHover());
  }, e.prototype.resize = function(t) {
    this._disposed || (t = t || {}, this.painter.resize(t.width, t.height), this.handler.resize());
  }, e.prototype.clearAnimation = function() {
    this._disposed || this.animation.clear();
  }, e.prototype.getWidth = function() {
    if (!this._disposed)
      return this.painter.getWidth();
  }, e.prototype.getHeight = function() {
    if (!this._disposed)
      return this.painter.getHeight();
  }, e.prototype.setCursorStyle = function(t) {
    this._disposed || this.handler.setCursorStyle(t);
  }, e.prototype.findHover = function(t, r) {
    if (!this._disposed)
      return this.handler.findHover(t, r);
  }, e.prototype.on = function(t, r, n) {
    return this._disposed || this.handler.on(t, r, n), this;
  }, e.prototype.off = function(t, r) {
    this._disposed || this.handler.off(t, r);
  }, e.prototype.trigger = function(t, r) {
    this._disposed || this.handler.trigger(t, r);
  }, e.prototype.clear = function() {
    if (!this._disposed) {
      for (var t = this.storage.getRoots(), r = 0; r < t.length; r++)
        t[r] instanceof oe && t[r].removeSelfFromZr(this);
      this.storage.delAllRoots(), this.painter.clear();
    }
  }, e.prototype.dispose = function() {
    this._disposed || (this.animation.stop(), this.clear(), this.storage.dispose(), this.painter.dispose(), this.handler.dispose(), this.animation = this.storage = this.painter = this.handler = null, this._disposed = !0, jm(this.id));
  }, e;
})();
function Zu(e, t) {
  var r = new Qm(Bl(), e, t);
  return Hr[r.id] = r, r;
}
function Jm(e) {
  e.dispose();
}
function t1() {
  for (var e in Hr)
    Hr.hasOwnProperty(e) && Hr[e].dispose();
  Hr = {};
}
function e1(e) {
  return Hr[e];
}
function Ap(e, t) {
  $a[e] = t;
}
var ju;
function r1(e) {
  if (typeof ju == "function")
    return ju(e);
}
function Lp(e) {
  ju = e;
}
var n1 = "5.6.1";
const i1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  dispose: Jm,
  disposeAll: t1,
  getElementSSRData: r1,
  getInstance: e1,
  init: Zu,
  registerPainter: Ap,
  registerSSRDataGetter: Lp,
  version: n1
}, Symbol.toStringTag, { value: "Module" }));
var Oh = 1e-4, Rp = 20;
function a1(e) {
  return e.replace(/^\s+|\s+$/g, "");
}
function Ku(e, t, r, n) {
  var i = t[0], a = t[1], o = r[0], s = r[1], u = a - i, l = s - o;
  if (u === 0)
    return l === 0 ? o : (o + s) / 2;
  if (n)
    if (u > 0) {
      if (e <= i)
        return o;
      if (e >= a)
        return s;
    } else {
      if (e >= i)
        return o;
      if (e <= a)
        return s;
    }
  else {
    if (e === i)
      return o;
    if (e === a)
      return s;
  }
  return (e - i) / u * l + o;
}
function It(e, t) {
  switch (e) {
    case "center":
    case "middle":
      e = "50%";
      break;
    case "left":
    case "top":
      e = "0%";
      break;
    case "right":
    case "bottom":
      e = "100%";
      break;
  }
  return B(e) ? a1(e).match(/%$/) ? parseFloat(e) / 100 * t : parseFloat(e) : e == null ? NaN : +e;
}
function Ht(e, t, r) {
  return t == null && (t = 10), t = Math.min(Math.max(0, t), Rp), e = (+e).toFixed(t), r ? e : +e;
}
function o1(e) {
  return e.sort(function(t, r) {
    return t - r;
  }), e;
}
function Te(e) {
  if (e = +e, isNaN(e))
    return 0;
  if (e > 1e-14) {
    for (var t = 1, r = 0; r < 15; r++, t *= 10)
      if (Math.round(e * t) / t === e)
        return r;
  }
  return Ep(e);
}
function Ep(e) {
  var t = e.toString().toLowerCase(), r = t.indexOf("e"), n = r > 0 ? +t.slice(r + 1) : 0, i = r > 0 ? r : t.length, a = t.indexOf("."), o = a < 0 ? 0 : i - 1 - a;
  return Math.max(0, o - n);
}
function Op(e, t) {
  var r = Math.log, n = Math.LN10, i = Math.floor(r(e[1] - e[0]) / n), a = Math.round(r(Math.abs(t[1] - t[0])) / n), o = Math.min(Math.max(-i + a, 0), 20);
  return isFinite(o) ? o : 20;
}
function s1(e, t, r) {
  if (!e[t])
    return 0;
  var n = u1(e, r);
  return n[t] || 0;
}
function u1(e, t) {
  var r = Pe(e, function(c, d) {
    return c + (isNaN(d) ? 0 : d);
  }, 0);
  if (r === 0)
    return [];
  for (var n = Math.pow(10, t), i = F(e, function(c) {
    return (isNaN(c) ? 0 : c) / r * n * 100;
  }), a = n * 100, o = F(i, function(c) {
    return Math.floor(c);
  }), s = Pe(o, function(c, d) {
    return c + d;
  }, 0), u = F(i, function(c, d) {
    return c - o[d];
  }); s < a; ) {
    for (var l = Number.NEGATIVE_INFINITY, f = null, h = 0, v = u.length; h < v; ++h)
      u[h] > l && (l = u[h], f = h);
    ++o[f], u[f] = 0, ++s;
  }
  return F(o, function(c) {
    return c / n;
  });
}
function l1(e, t) {
  var r = Math.max(Te(e), Te(t)), n = e + t;
  return r > Rp ? n : Ht(n, r);
}
var f1 = 9007199254740991;
function $l(e) {
  var t = Math.PI * 2;
  return (e % t + t) % t;
}
function Oi(e) {
  return e > -Oh && e < Oh;
}
var h1 = /^(?:(\d{4})(?:[-\/](\d{1,2})(?:[-\/](\d{1,2})(?:[T ](\d{1,2})(?::(\d{1,2})(?::(\d{1,2})(?:[.,](\d+))?)?)?(Z|[\+\-]\d\d:?\d\d)?)?)?)?)?$/;
function se(e) {
  if (e instanceof Date)
    return e;
  if (B(e)) {
    var t = h1.exec(e);
    if (!t)
      return /* @__PURE__ */ new Date(NaN);
    if (t[8]) {
      var r = +t[4] || 0;
      return t[8].toUpperCase() !== "Z" && (r -= +t[8].slice(0, 3)), new Date(Date.UTC(+t[1], +(t[2] || 1) - 1, +t[3] || 1, r, +(t[5] || 0), +t[6] || 0, t[7] ? +t[7].substring(0, 3) : 0));
    } else
      return new Date(+t[1], +(t[2] || 1) - 1, +t[3] || 1, +t[4] || 0, +(t[5] || 0), +t[6] || 0, t[7] ? +t[7].substring(0, 3) : 0);
  } else if (e == null)
    return /* @__PURE__ */ new Date(NaN);
  return new Date(Math.round(e));
}
function kp(e) {
  return Math.pow(10, Fo(e));
}
function Fo(e) {
  if (e === 0)
    return 0;
  var t = Math.floor(Math.log(e) / Math.LN10);
  return e / Math.pow(10, t) >= 10 && t++, t;
}
function Zl(e, t) {
  var r = Fo(e), n = Math.pow(10, r), i = e / n, a;
  return t ? i < 1.5 ? a = 1 : i < 2.5 ? a = 2 : i < 4 ? a = 3 : i < 7 ? a = 5 : a = 10 : i < 1 ? a = 1 : i < 2 ? a = 2 : i < 3 ? a = 3 : i < 5 ? a = 5 : a = 10, e = a * n, r >= -20 ? +e.toFixed(r < 0 ? -r : 0) : e;
}
function c1(e, t) {
  var r = (e.length - 1) * t + 1, n = Math.floor(r), i = +e[n - 1], a = r - n;
  return a ? i + a * (e[n] - i) : i;
}
function v1(e) {
  e.sort(function(u, l) {
    return s(u, l, 0) ? -1 : 1;
  });
  for (var t = -1 / 0, r = 1, n = 0; n < e.length; ) {
    for (var i = e[n].interval, a = e[n].close, o = 0; o < 2; o++)
      i[o] <= t && (i[o] = t, a[o] = o ? 1 : 1 - r), t = i[o], r = a[o];
    i[0] === i[1] && a[0] * a[1] !== 1 ? e.splice(n, 1) : n++;
  }
  return e;
  function s(u, l, f) {
    return u.interval[f] < l.interval[f] || u.interval[f] === l.interval[f] && (u.close[f] - l.close[f] === (f ? -1 : 1) || !f && s(u, l, 1));
  }
}
function Ue(e) {
  var t = parseFloat(e);
  return t == e && (t !== 0 || !B(e) || e.indexOf("x") <= 0) ? t : NaN;
}
function Bp(e) {
  return !isNaN(Ue(e));
}
function Fp() {
  return Math.round(Math.random() * 9);
}
function Np(e, t) {
  return t === 0 ? e : Np(t, e % t);
}
function kh(e, t) {
  return e == null ? t : t == null ? e : e * t / Np(e, t);
}
function At(e) {
  throw new Error(e);
}
function Bh(e, t, r) {
  return (t - e) * r + e;
}
var zp = "series\0", d1 = "\0_ec_\0";
function Rt(e) {
  return e instanceof Array ? e : e == null ? [] : [e];
}
function Fh(e, t, r) {
  if (e) {
    e[t] = e[t] || {}, e.emphasis = e.emphasis || {}, e.emphasis[t] = e.emphasis[t] || {};
    for (var n = 0, i = r.length; n < i; n++) {
      var a = r[n];
      !e.emphasis[t].hasOwnProperty(a) && e[t].hasOwnProperty(a) && (e.emphasis[t][a] = e[t][a]);
    }
  }
}
var Nh = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "rich", "tag", "color", "textBorderColor", "textBorderWidth", "width", "height", "lineHeight", "align", "verticalAlign", "baseline", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY", "backgroundColor", "borderColor", "borderWidth", "borderRadius", "padding"];
function ji(e) {
  return N(e) && !k(e) && !(e instanceof Date) ? e.value : e;
}
function p1(e) {
  return N(e) && !(e instanceof Array);
}
function g1(e, t, r) {
  var n = r === "normalMerge", i = r === "replaceMerge", a = r === "replaceAll";
  e = e || [], t = (t || []).slice();
  var o = W();
  x(t, function(u, l) {
    if (!N(u)) {
      t[l] = null;
      return;
    }
  });
  var s = y1(e, o, r);
  return (n || i) && _1(s, e, o, t), n && m1(s, t), n || i ? S1(s, t, i) : a && w1(s, t), b1(s), s;
}
function y1(e, t, r) {
  var n = [];
  if (r === "replaceAll")
    return n;
  for (var i = 0; i < e.length; i++) {
    var a = e[i];
    a && a.id != null && t.set(a.id, i), n.push({
      existing: r === "replaceMerge" || ki(a) ? null : a,
      newOption: null,
      keyInfo: null,
      brandNew: null
    });
  }
  return n;
}
function _1(e, t, r, n) {
  x(n, function(i, a) {
    if (!(!i || i.id == null)) {
      var o = mi(i.id), s = r.get(o);
      if (s != null) {
        var u = e[s];
        Ie(!u.newOption, 'Duplicated option on id "' + o + '".'), u.newOption = i, u.existing = t[s], n[a] = null;
      }
    }
  });
}
function m1(e, t) {
  x(t, function(r, n) {
    if (!(!r || r.name == null))
      for (var i = 0; i < e.length; i++) {
        var a = e[i].existing;
        if (!e[i].newOption && a && (a.id == null || r.id == null) && !ki(r) && !ki(a) && Hp("name", a, r)) {
          e[i].newOption = r, t[n] = null;
          return;
        }
      }
  });
}
function S1(e, t, r) {
  x(t, function(n) {
    if (n) {
      for (
        var i, a = 0;
        // Be `!resultItem` only when `nextIdx >= result.length`.
        (i = e[a]) && (i.newOption || ki(i.existing) || // In mode "replaceMerge", here no not-mapped-non-internal-existing.
        i.existing && n.id != null && !Hp("id", n, i.existing));
      )
        a++;
      i ? (i.newOption = n, i.brandNew = r) : e.push({
        newOption: n,
        brandNew: r,
        existing: null,
        keyInfo: null
      }), a++;
    }
  });
}
function w1(e, t) {
  x(t, function(r) {
    e.push({
      newOption: r,
      brandNew: !0,
      existing: null,
      keyInfo: null
    });
  });
}
function b1(e) {
  var t = W();
  x(e, function(r) {
    var n = r.existing;
    n && t.set(n.id, r);
  }), x(e, function(r) {
    var n = r.newOption;
    Ie(!n || n.id == null || !t.get(n.id) || t.get(n.id) === r, "id duplicates: " + (n && n.id)), n && n.id != null && t.set(n.id, r), !r.keyInfo && (r.keyInfo = {});
  }), x(e, function(r, n) {
    var i = r.existing, a = r.newOption, o = r.keyInfo;
    if (N(a)) {
      if (o.name = a.name != null ? mi(a.name) : i ? i.name : zp + n, i)
        o.id = mi(i.id);
      else if (a.id != null)
        o.id = mi(a.id);
      else {
        var s = 0;
        do
          o.id = "\0" + o.name + "\0" + s++;
        while (t.get(o.id));
      }
      t.set(o.id, r);
    }
  });
}
function Hp(e, t, r) {
  var n = Ce(t[e], null), i = Ce(r[e], null);
  return n != null && i != null && n === i;
}
function mi(e) {
  return Ce(e, "");
}
function Ce(e, t) {
  return e == null ? t : B(e) ? e : ut(e) || io(e) ? e + "" : t;
}
function jl(e) {
  var t = e.name;
  return !!(t && t.indexOf(zp));
}
function ki(e) {
  return e && e.id != null && mi(e.id).indexOf(d1) === 0;
}
function T1(e, t, r) {
  x(e, function(n) {
    var i = n.newOption;
    N(i) && (n.keyInfo.mainType = t, n.keyInfo.subType = x1(t, i, n.existing, r));
  });
}
function x1(e, t, r, n) {
  var i = t.type ? t.type : r ? r.subType : n.determineSubType(e, t);
  return i;
}
function Ki(e, t) {
  if (t.dataIndexInside != null)
    return t.dataIndexInside;
  if (t.dataIndex != null)
    return k(t.dataIndex) ? F(t.dataIndex, function(r) {
      return e.indexOfRawIndex(r);
    }) : e.indexOfRawIndex(t.dataIndex);
  if (t.name != null)
    return k(t.name) ? F(t.name, function(r) {
      return e.indexOfName(r);
    }) : e.indexOfName(t.name);
}
function gt() {
  var e = "__ec_inner_" + C1++;
  return function(t) {
    return t[e] || (t[e] = {});
  };
}
var C1 = Fp();
function ks(e, t, r) {
  var n = Kl(t, r), i = n.mainTypeSpecified, a = n.queryOptionMap, o = n.others, s = o, u = r ? r.defaultMainType : null;
  return !i && u && a.set(u, {}), a.each(function(l, f) {
    var h = Qi(e, f, l, {
      useDefault: u === f,
      enableAll: r && r.enableAll != null ? r.enableAll : !0,
      enableNone: r && r.enableNone != null ? r.enableNone : !0
    });
    s[f + "Models"] = h.models, s[f + "Model"] = h.models[0];
  }), s;
}
function Kl(e, t) {
  var r;
  if (B(e)) {
    var n = {};
    n[e + "Index"] = 0, r = n;
  } else
    r = e;
  var i = W(), a = {}, o = !1;
  return x(r, function(s, u) {
    if (u === "dataIndex" || u === "dataIndexInside") {
      a[u] = s;
      return;
    }
    var l = u.match(/^(\w+)(Index|Id|Name)$/) || [], f = l[1], h = (l[2] || "").toLowerCase();
    if (!(!f || !h || t && t.includeMainTypes && rt(t.includeMainTypes, f) < 0)) {
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
var ar = {
  useDefault: !0,
  enableAll: !1,
  enableNone: !1
};
function Qi(e, t, r, n) {
  n = n || ar;
  var i = r.index, a = r.id, o = r.name, s = {
    models: null,
    specified: i != null || a != null || o != null
  };
  if (!s.specified) {
    var u = void 0;
    return s.models = n.useDefault && (u = e.getComponent(t)) ? [u] : [], s;
  }
  return i === "none" || i === !1 ? (Ie(n.enableNone, '`"none"` or `false` is not a valid value on index option.'), s.models = [], s) : (i === "all" && (Ie(n.enableAll, '`"all"` is not a valid value on index option.'), i = a = o = null), s.models = e.queryComponents({
    mainType: t,
    index: i,
    id: a,
    name: o
  }), s);
}
function Gp(e, t, r) {
  e.setAttribute ? e.setAttribute(t, r) : e[t] = r;
}
function M1(e, t) {
  return e.getAttribute ? e.getAttribute(t) : e[t];
}
function D1(e) {
  return e === "auto" ? z.domSupported ? "html" : "richText" : e || "html";
}
function P1(e, t, r, n, i) {
  var a = t == null || t === "auto";
  if (n == null)
    return n;
  if (ut(n)) {
    var o = Bh(r || 0, n, i);
    return Ht(o, a ? Math.max(Te(r || 0), Te(n)) : t);
  } else {
    if (B(n))
      return i < 1 ? r : n;
    for (var s = [], u = r, l = n, f = Math.max(u ? u.length : 0, l.length), h = 0; h < f; ++h) {
      var v = e.getDimensionInfo(h);
      if (v && v.type === "ordinal")
        s[h] = (i < 1 && u ? u : l)[h];
      else {
        var c = u && u[h] ? u[h] : 0, d = l[h], o = Bh(c, d, i);
        s[h] = Ht(o, a ? Math.max(Te(c), Te(d)) : t);
      }
    }
    return s;
  }
}
var I1 = ".", xr = "___EC__COMPONENT__CONTAINER___", Vp = "___EC__EXTENDED_CLASS___";
function xe(e) {
  var t = {
    main: "",
    sub: ""
  };
  if (e) {
    var r = e.split(I1);
    t.main = r[0] || "", t.sub = r[1] || "";
  }
  return t;
}
function A1(e) {
  Ie(/^[a-zA-Z0-9_]+([.][a-zA-Z0-9_]+)?$/.test(e), 'componentType "' + e + '" illegal');
}
function L1(e) {
  return !!(e && e[Vp]);
}
function Ql(e, t) {
  e.$constructor = e, e.extend = function(r) {
    var n = this, i;
    return R1(n) ? i = /** @class */
    (function(a) {
      lt(o, a);
      function o() {
        return a.apply(this, arguments) || this;
      }
      return o;
    })(n) : (i = function() {
      (r.$constructor || n).apply(this, arguments);
    }, Fl(i, this)), E(i.prototype, r), i[Vp] = !0, i.extend = this.extend, i.superCall = k1, i.superApply = B1, i.superClass = n, i;
  };
}
function R1(e) {
  return U(e) && /^class\s/.test(Function.prototype.toString.call(e));
}
function Wp(e, t) {
  e.extend = t.extend;
}
var E1 = Math.round(Math.random() * 10);
function O1(e) {
  var t = ["__\0is_clz", E1++].join("_");
  e.prototype[t] = !0, e.isInstance = function(r) {
    return !!(r && r[t]);
  };
}
function k1(e, t) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return this.superClass.prototype[t].apply(e, r);
}
function B1(e, t, r) {
  return this.superClass.prototype[t].apply(e, r);
}
function No(e) {
  var t = {};
  e.registerClass = function(n) {
    var i = n.type || n.prototype.type;
    if (i) {
      A1(i), n.prototype.type = i;
      var a = xe(i);
      if (!a.sub)
        t[a.main] = n;
      else if (a.sub !== xr) {
        var o = r(a);
        o[a.sub] = n;
      }
    }
    return n;
  }, e.getClass = function(n, i, a) {
    var o = t[n];
    if (o && o[xr] && (o = i ? o[i] : null), a && !o)
      throw new Error(i ? "Component " + n + "." + (i || "") + " is used but not imported." : n + ".type should be specified.");
    return o;
  }, e.getClassesByMainType = function(n) {
    var i = xe(n), a = [], o = t[i.main];
    return o && o[xr] ? x(o, function(s, u) {
      u !== xr && a.push(s);
    }) : a.push(o), a;
  }, e.hasClass = function(n) {
    var i = xe(n);
    return !!t[i.main];
  }, e.getAllClassMainTypes = function() {
    var n = [];
    return x(t, function(i, a) {
      n.push(a);
    }), n;
  }, e.hasSubTypes = function(n) {
    var i = xe(n), a = t[i.main];
    return a && a[xr];
  };
  function r(n) {
    var i = t[n.main];
    return (!i || !i[xr]) && (i = t[n.main] = {}, i[xr] = !0), i;
  }
}
function Bi(e, t) {
  for (var r = 0; r < e.length; r++)
    e[r][1] || (e[r][1] = e[r][0]);
  return t = t || !1, function(n, i, a) {
    for (var o = {}, s = 0; s < e.length; s++) {
      var u = e[s][1];
      if (!(i && rt(i, u) >= 0 || a && rt(a, u) < 0)) {
        var l = n.getShallow(u, t);
        l != null && (o[e[s][0]] = l);
      }
    }
    return o;
  };
}
var F1 = [
  ["fill", "color"],
  ["shadowBlur"],
  ["shadowOffsetX"],
  ["shadowOffsetY"],
  ["opacity"],
  ["shadowColor"]
  // Option decal is in `DecalObject` but style.decal is in `PatternObject`.
  // So do not transfer decal directly.
], N1 = Bi(F1), z1 = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getAreaStyle = function(t, r) {
      return N1(this, t, r);
    }, e;
  })()
), Qu = new Zi(50);
function H1(e) {
  if (typeof e == "string") {
    var t = Qu.get(e);
    return t && t.image;
  } else
    return e;
}
function Up(e, t, r, n, i) {
  if (e)
    if (typeof e == "string") {
      if (t && t.__zrImageSrc === e || !r)
        return t;
      var a = Qu.get(e), o = { hostEl: r, cb: n, cbPayload: i };
      return a ? (t = a.image, !zo(t) && a.pending.push(o)) : (t = Ve.loadImage(e, zh, zh), t.__zrImageSrc = e, Qu.put(e, t.__cachedImgObj = {
        image: t,
        pending: [o]
      })), t;
    } else
      return e;
  else return t;
}
function zh() {
  var e = this.__cachedImgObj;
  this.onload = this.onerror = this.__cachedImgObj = null;
  for (var t = 0; t < e.pending.length; t++) {
    var r = e.pending[t], n = r.cb;
    n && n(this, r.cbPayload), r.hostEl.dirty();
  }
  e.pending.length = 0;
}
function zo(e) {
  return e && e.width && e.height;
}
var Bs = /\{([a-zA-Z0-9_]+)\|([^}]*)\}/g;
function G1(e, t, r, n, i) {
  var a = {};
  return Yp(a, e, t, r, n, i), a.text;
}
function Yp(e, t, r, n, i, a) {
  if (!r) {
    e.text = "", e.isTruncated = !1;
    return;
  }
  var o = (t + "").split(`
`);
  a = qp(r, n, i, a);
  for (var s = !1, u = {}, l = 0, f = o.length; l < f; l++)
    Xp(u, o[l], a), o[l] = u.textLine, s = s || u.isTruncated;
  e.text = o.join(`
`), e.isTruncated = s;
}
function qp(e, t, r, n) {
  n = n || {};
  var i = E({}, n);
  i.font = t, r = Y(r, "..."), i.maxIterations = Y(n.maxIterations, 2);
  var a = i.minChar = Y(n.minChar, 0);
  i.cnCharWidth = Xt("国", t);
  var o = i.ascCharWidth = Xt("a", t);
  i.placeholder = Y(n.placeholder, "");
  for (var s = e = Math.max(0, e - 1), u = 0; u < a && s >= o; u++)
    s -= o;
  var l = Xt(r, t);
  return l > s && (r = "", l = 0), s = e - l, i.ellipsis = r, i.ellipsisWidth = l, i.contentWidth = s, i.containerWidth = e, i;
}
function Xp(e, t, r) {
  var n = r.containerWidth, i = r.font, a = r.contentWidth;
  if (!n) {
    e.textLine = "", e.isTruncated = !1;
    return;
  }
  var o = Xt(t, i);
  if (o <= n) {
    e.textLine = t, e.isTruncated = !1;
    return;
  }
  for (var s = 0; ; s++) {
    if (o <= a || s >= r.maxIterations) {
      t += r.ellipsis;
      break;
    }
    var u = s === 0 ? V1(t, a, r.ascCharWidth, r.cnCharWidth) : o > 0 ? Math.floor(t.length * a / o) : 0;
    t = t.substr(0, u), o = Xt(t, i);
  }
  t === "" && (t = r.placeholder), e.textLine = t, e.isTruncated = !0;
}
function V1(e, t, r, n) {
  for (var i = 0, a = 0, o = e.length; a < o && i < t; a++) {
    var s = e.charCodeAt(a);
    i += 0 <= s && s <= 127 ? r : n;
  }
  return a;
}
function W1(e, t) {
  e != null && (e += "");
  var r = t.overflow, n = t.padding, i = t.font, a = r === "truncate", o = Xl(i), s = Y(t.lineHeight, o), u = !!t.backgroundColor, l = t.lineOverflow === "truncate", f = !1, h = t.width, v;
  h != null && (r === "break" || r === "breakAll") ? v = e ? $p(e, t.font, h, r === "breakAll", 0).lines : [] : v = e ? e.split(`
`) : [];
  var c = v.length * s, d = Y(t.height, c);
  if (c > d && l) {
    var y = Math.floor(d / s);
    f = f || v.length > y, v = v.slice(0, y);
  }
  if (e && a && h != null)
    for (var p = qp(h, i, t.ellipsis, {
      minChar: t.truncateMinChar,
      placeholder: t.placeholder
    }), g = {}, _ = 0; _ < v.length; _++)
      Xp(g, v[_], p), v[_] = g.textLine, f = f || g.isTruncated;
  for (var m = d, S = 0, _ = 0; _ < v.length; _++)
    S = Math.max(Xt(v[_], i), S);
  h == null && (h = S);
  var b = S;
  return n && (m += n[0] + n[2], b += n[1] + n[3], h += n[1] + n[3]), u && (b = h), {
    lines: v,
    height: d,
    outerWidth: b,
    outerHeight: m,
    lineHeight: s,
    calculatedLineHeight: o,
    contentWidth: S,
    contentHeight: c,
    width: h,
    isTruncated: f
  };
}
var U1 = /* @__PURE__ */ (function() {
  function e() {
  }
  return e;
})(), Hh = /* @__PURE__ */ (function() {
  function e(t) {
    this.tokens = [], t && (this.tokens = t);
  }
  return e;
})(), Y1 = /* @__PURE__ */ (function() {
  function e() {
    this.width = 0, this.height = 0, this.contentWidth = 0, this.contentHeight = 0, this.outerWidth = 0, this.outerHeight = 0, this.lines = [], this.isTruncated = !1;
  }
  return e;
})();
function q1(e, t) {
  var r = new Y1();
  if (e != null && (e += ""), !e)
    return r;
  for (var n = t.width, i = t.height, a = t.overflow, o = (a === "break" || a === "breakAll") && n != null ? { width: n, accumWidth: 0, breakAll: a === "breakAll" } : null, s = Bs.lastIndex = 0, u; (u = Bs.exec(e)) != null; ) {
    var l = u.index;
    l > s && Fs(r, e.substring(s, l), t, o), Fs(r, u[2], t, o, u[1]), s = Bs.lastIndex;
  }
  s < e.length && Fs(r, e.substring(s, e.length), t, o);
  var f = [], h = 0, v = 0, c = t.padding, d = a === "truncate", y = t.lineOverflow === "truncate", p = {};
  function g(X, nt, J) {
    X.width = nt, X.lineHeight = J, h += J, v = Math.max(v, nt);
  }
  t: for (var _ = 0; _ < r.lines.length; _++) {
    for (var m = r.lines[_], S = 0, b = 0, w = 0; w < m.tokens.length; w++) {
      var T = m.tokens[w], C = T.styleName && t.rich[T.styleName] || {}, M = T.textPadding = C.padding, D = M ? M[1] + M[3] : 0, P = T.font = C.font || t.font;
      T.contentHeight = Xl(P);
      var A = Y(C.height, T.contentHeight);
      if (T.innerHeight = A, M && (A += M[0] + M[2]), T.height = A, T.lineHeight = yi(C.lineHeight, t.lineHeight, A), T.align = C && C.align || t.align, T.verticalAlign = C && C.verticalAlign || "middle", y && i != null && h + T.lineHeight > i) {
        var L = r.lines.length;
        w > 0 ? (m.tokens = m.tokens.slice(0, w), g(m, b, S), r.lines = r.lines.slice(0, _ + 1)) : r.lines = r.lines.slice(0, _), r.isTruncated = r.isTruncated || r.lines.length < L;
        break t;
      }
      var I = C.width, R = I == null || I === "auto";
      if (typeof I == "string" && I.charAt(I.length - 1) === "%")
        T.percentWidth = I, f.push(T), T.contentWidth = Xt(T.text, P);
      else {
        if (R) {
          var O = C.backgroundColor, q = O && O.image;
          q && (q = H1(q), zo(q) && (T.width = Math.max(T.width, q.width * A / q.height)));
        }
        var H = d && n != null ? n - b : null;
        H != null && H < T.width ? !R || H < D ? (T.text = "", T.width = T.contentWidth = 0) : (Yp(p, T.text, H - D, P, t.ellipsis, { minChar: t.truncateMinChar }), T.text = p.text, r.isTruncated = r.isTruncated || p.isTruncated, T.width = T.contentWidth = Xt(T.text, P)) : T.contentWidth = Xt(T.text, P);
      }
      T.width += D, b += T.width, C && (S = Math.max(S, T.lineHeight));
    }
    g(m, b, S);
  }
  r.outerWidth = r.width = Y(n, v), r.outerHeight = r.height = Y(i, h), r.contentHeight = h, r.contentWidth = v, c && (r.outerWidth += c[1] + c[3], r.outerHeight += c[0] + c[2]);
  for (var _ = 0; _ < f.length; _++) {
    var T = f[_], G = T.percentWidth;
    T.width = parseInt(G, 10) / 100 * r.width;
  }
  return r;
}
function Fs(e, t, r, n, i) {
  var a = t === "", o = i && r.rich[i] || {}, s = e.lines, u = o.font || r.font, l = !1, f, h;
  if (n) {
    var v = o.padding, c = v ? v[1] + v[3] : 0;
    if (o.width != null && o.width !== "auto") {
      var d = Kr(o.width, n.width) + c;
      s.length > 0 && d + n.accumWidth > n.width && (f = t.split(`
`), l = !0), n.accumWidth = d;
    } else {
      var y = $p(t, u, n.width, n.breakAll, n.accumWidth);
      n.accumWidth = y.accumWidth + c, h = y.linesWidths, f = y.lines;
    }
  } else
    f = t.split(`
`);
  for (var p = 0; p < f.length; p++) {
    var g = f[p], _ = new U1();
    if (_.styleName = i, _.text = g, _.isLineHolder = !g && !a, typeof o.width == "number" ? _.width = o.width : _.width = h ? h[p] : Xt(g, u), !p && !l) {
      var m = (s[s.length - 1] || (s[0] = new Hh())).tokens, S = m.length;
      S === 1 && m[0].isLineHolder ? m[0] = _ : (g || !S || a) && m.push(_);
    } else
      s.push(new Hh([_]));
  }
}
function X1(e) {
  var t = e.charCodeAt(0);
  return t >= 32 && t <= 591 || t >= 880 && t <= 4351 || t >= 4608 && t <= 5119 || t >= 7680 && t <= 8303;
}
var $1 = Pe(",&?/;] ".split(""), function(e, t) {
  return e[t] = !0, e;
}, {});
function Z1(e) {
  return X1(e) ? !!$1[e] : !0;
}
function $p(e, t, r, n, i) {
  for (var a = [], o = [], s = "", u = "", l = 0, f = 0, h = 0; h < e.length; h++) {
    var v = e.charAt(h);
    if (v === `
`) {
      u && (s += u, f += l), a.push(s), o.push(f), s = "", u = "", l = 0, f = 0;
      continue;
    }
    var c = Xt(v, t), d = n ? !1 : !Z1(v);
    if (a.length ? f + c > r : i + f + c > r) {
      f ? (s || u) && (d ? (s || (s = u, u = "", l = 0, f = l), a.push(s), o.push(f - l), u += v, l += c, s = "", f = l) : (u && (s += u, u = "", l = 0), a.push(s), o.push(f), s = v, f = c)) : d ? (a.push(u), o.push(l), u = v, l = c) : (a.push(v), o.push(c));
      continue;
    }
    f += c, d ? (u += v, l += c) : (u && (s += u, u = "", l = 0), s += v);
  }
  return !a.length && !s && (s = e, u = "", l = 0), u && (s += u), s && (a.push(s), o.push(f)), a.length === 1 && (f += i), {
    accumWidth: f,
    lines: a,
    linesWidths: o
  };
}
var Ju = "__zr_style_" + Math.round(Math.random() * 10), qr = {
  shadowBlur: 0,
  shadowOffsetX: 0,
  shadowOffsetY: 0,
  shadowColor: "#000",
  opacity: 1,
  blend: "source-over"
}, Ho = {
  style: {
    shadowBlur: !0,
    shadowOffsetX: !0,
    shadowOffsetY: !0,
    shadowColor: !0,
    opacity: !0
  }
};
qr[Ju] = !0;
var Gh = ["z", "z2", "invisible"], j1 = ["invisible"], Ji = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype._init = function(r) {
    for (var n = ct(r), i = 0; i < n.length; i++) {
      var a = n[i];
      a === "style" ? this.useStyle(r[a]) : e.prototype.attrKV.call(this, a, r[a]);
    }
    this.style || this.useStyle({});
  }, t.prototype.beforeBrush = function() {
  }, t.prototype.afterBrush = function() {
  }, t.prototype.innerBeforeBrush = function() {
  }, t.prototype.innerAfterBrush = function() {
  }, t.prototype.shouldBePainted = function(r, n, i, a) {
    var o = this.transform;
    if (this.ignore || this.invisible || this.style.opacity === 0 || this.culling && K1(this, r, n) || o && !o[0] && !o[3])
      return !1;
    if (i && this.__clipPaths) {
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
  }, t.prototype.contain = function(r, n) {
    return this.rectContain(r, n);
  }, t.prototype.traverse = function(r, n) {
    r.call(n, this);
  }, t.prototype.rectContain = function(r, n) {
    var i = this.transformCoordToLocal(r, n), a = this.getBoundingRect();
    return a.contain(i[0], i[1]);
  }, t.prototype.getPaintRect = function() {
    var r = this._paintRect;
    if (!this._paintRect || this.__dirty) {
      var n = this.transform, i = this.getBoundingRect(), a = this.style, o = a.shadowBlur || 0, s = a.shadowOffsetX || 0, u = a.shadowOffsetY || 0;
      r = this._paintRect || (this._paintRect = new j(0, 0, 0, 0)), n ? j.applyTransform(r, i, n) : r.copy(i), (o || s || u) && (r.width += o * 2 + Math.abs(s), r.height += o * 2 + Math.abs(u), r.x = Math.min(r.x, r.x + s - o), r.y = Math.min(r.y, r.y + u - o));
      var l = this.dirtyRectTolerance;
      r.isZero() || (r.x = Math.floor(r.x - l), r.y = Math.floor(r.y - l), r.width = Math.ceil(r.width + 1 + l * 2), r.height = Math.ceil(r.height + 1 + l * 2));
    }
    return r;
  }, t.prototype.setPrevPaintRect = function(r) {
    r ? (this._prevPaintRect = this._prevPaintRect || new j(0, 0, 0, 0), this._prevPaintRect.copy(r)) : this._prevPaintRect = null;
  }, t.prototype.getPrevPaintRect = function() {
    return this._prevPaintRect;
  }, t.prototype.animateStyle = function(r) {
    return this.animate("style", r);
  }, t.prototype.updateDuringAnimation = function(r) {
    r === "style" ? this.dirtyStyle() : this.markRedraw();
  }, t.prototype.attrKV = function(r, n) {
    r !== "style" ? e.prototype.attrKV.call(this, r, n) : this.style ? this.setStyle(n) : this.useStyle(n);
  }, t.prototype.setStyle = function(r, n) {
    return typeof r == "string" ? this.style[r] = n : E(this.style, r), this.dirtyStyle(), this;
  }, t.prototype.dirtyStyle = function(r) {
    r || this.markRedraw(), this.__dirty |= fi, this._rect && (this._rect = null);
  }, t.prototype.dirty = function() {
    this.dirtyStyle();
  }, t.prototype.styleChanged = function() {
    return !!(this.__dirty & fi);
  }, t.prototype.styleUpdated = function() {
    this.__dirty &= ~fi;
  }, t.prototype.createStyle = function(r) {
    return Xi(qr, r);
  }, t.prototype.useStyle = function(r) {
    r[Ju] || (r = this.createStyle(r)), this.__inHover ? this.__hoverStyle = r : this.style = r, this.dirtyStyle();
  }, t.prototype.isStyleObject = function(r) {
    return r[Ju];
  }, t.prototype._innerSaveToNormal = function(r) {
    e.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.style && !n.style && (n.style = this._mergeStyle(this.createStyle(), this.style)), this._savePrimaryToNormal(r, n, Gh);
  }, t.prototype._applyStateObj = function(r, n, i, a, o, s) {
    e.prototype._applyStateObj.call(this, r, n, i, a, o, s);
    var u = !(n && a), l;
    if (n && n.style ? o ? a ? l = n.style : (l = this._mergeStyle(this.createStyle(), i.style), this._mergeStyle(l, n.style)) : (l = this._mergeStyle(this.createStyle(), a ? this.style : i.style), this._mergeStyle(l, n.style)) : u && (l = i.style), l)
      if (o) {
        var f = this.style;
        if (this.style = this.createStyle(u ? {} : f), u)
          for (var h = ct(f), v = 0; v < h.length; v++) {
            var c = h[v];
            c in l && (l[c] = l[c], this.style[c] = f[c]);
          }
        for (var d = ct(l), v = 0; v < d.length; v++) {
          var c = d[v];
          this.style[c] = this.style[c];
        }
        this._transitionState(r, {
          style: l
        }, s, this.getAnimationStyleProps());
      } else
        this.useStyle(l);
    for (var y = this.__inHover ? j1 : Gh, v = 0; v < y.length; v++) {
      var c = y[v];
      n && n[c] != null ? this[c] = n[c] : u && i[c] != null && (this[c] = i[c]);
    }
  }, t.prototype._mergeStates = function(r) {
    for (var n = e.prototype._mergeStates.call(this, r), i, a = 0; a < r.length; a++) {
      var o = r[a];
      o.style && (i = i || {}, this._mergeStyle(i, o.style));
    }
    return i && (n.style = i), n;
  }, t.prototype._mergeStyle = function(r, n) {
    return E(r, n), r;
  }, t.prototype.getAnimationStyleProps = function() {
    return Ho;
  }, t.initDefaultProps = (function() {
    var r = t.prototype;
    r.type = "displayable", r.invisible = !1, r.z = 0, r.z2 = 0, r.zlevel = 0, r.culling = !1, r.cursor = "pointer", r.rectHover = !1, r.incremental = !1, r._rect = null, r.dirtyRectTolerance = 0, r.__dirty = Ut | fi;
  })(), t;
})(Bo), Ns = new j(0, 0, 0, 0), zs = new j(0, 0, 0, 0);
function K1(e, t, r) {
  return Ns.copy(e.getBoundingRect()), e.transform && Ns.applyTransform(e.transform), zs.width = t, zs.height = r, !Ns.intersect(zs);
}
var ne = Math.min, ie = Math.max, Hs = Math.sin, Gs = Math.cos, Cr = Math.PI * 2, pa = Jr(), ga = Jr(), ya = Jr();
function Vh(e, t, r, n, i, a) {
  i[0] = ne(e, r), i[1] = ne(t, n), a[0] = ie(e, r), a[1] = ie(t, n);
}
var Wh = [], Uh = [];
function Q1(e, t, r, n, i, a, o, s, u, l) {
  var f = _p, h = _t, v = f(e, r, i, o, Wh);
  u[0] = 1 / 0, u[1] = 1 / 0, l[0] = -1 / 0, l[1] = -1 / 0;
  for (var c = 0; c < v; c++) {
    var d = h(e, r, i, o, Wh[c]);
    u[0] = ne(d, u[0]), l[0] = ie(d, l[0]);
  }
  v = f(t, n, a, s, Uh);
  for (var c = 0; c < v; c++) {
    var y = h(t, n, a, s, Uh[c]);
    u[1] = ne(y, u[1]), l[1] = ie(y, l[1]);
  }
  u[0] = ne(e, u[0]), l[0] = ie(e, l[0]), u[0] = ne(o, u[0]), l[0] = ie(o, l[0]), u[1] = ne(t, u[1]), l[1] = ie(t, l[1]), u[1] = ne(s, u[1]), l[1] = ie(s, l[1]);
}
function J1(e, t, r, n, i, a, o, s) {
  var u = mp, l = Tt, f = ie(ne(u(e, r, i), 1), 0), h = ie(ne(u(t, n, a), 1), 0), v = l(e, r, i, f), c = l(t, n, a, h);
  o[0] = ne(e, i, v), o[1] = ne(t, a, c), s[0] = ie(e, i, v), s[1] = ie(t, a, c);
}
function tS(e, t, r, n, i, a, o, s, u) {
  var l = er, f = rr, h = Math.abs(i - a);
  if (h % Cr < 1e-4 && h > 1e-4) {
    s[0] = e - r, s[1] = t - n, u[0] = e + r, u[1] = t + n;
    return;
  }
  if (pa[0] = Gs(i) * r + e, pa[1] = Hs(i) * n + t, ga[0] = Gs(a) * r + e, ga[1] = Hs(a) * n + t, l(s, pa, ga), f(u, pa, ga), i = i % Cr, i < 0 && (i = i + Cr), a = a % Cr, a < 0 && (a = a + Cr), i > a && !o ? a += Cr : i < a && o && (i += Cr), o) {
    var v = a;
    a = i, i = v;
  }
  for (var c = 0; c < a; c += Math.PI / 2)
    c > i && (ya[0] = Gs(c) * r + e, ya[1] = Hs(c) * n + t, l(s, ya, s), f(u, ya, u));
}
var K = {
  M: 1,
  L: 2,
  C: 3,
  Q: 4,
  A: 5,
  Z: 6,
  R: 7
}, Mr = [], Dr = [], _e = [], $e = [], me = [], Se = [], Vs = Math.min, Ws = Math.max, Pr = Math.cos, Ir = Math.sin, Fe = Math.abs, tl = Math.PI, Je = tl * 2, Us = typeof Float32Array < "u", Zn = [];
function Ys(e) {
  var t = Math.round(e / tl * 1e8) / 1e8;
  return t % 2 * tl;
}
function eS(e, t) {
  var r = Ys(e[0]);
  r < 0 && (r += Je);
  var n = r - e[0], i = e[1];
  i += n, !t && i - r >= Je ? i = r + Je : t && r - i >= Je ? i = r - Je : !t && r > i ? i = r + (Je - Ys(r - i)) : t && r < i && (i = r - (Je - Ys(i - r))), e[0] = r, e[1] = i;
}
var Ln = (function() {
  function e(t) {
    this.dpr = 1, this._xi = 0, this._yi = 0, this._x0 = 0, this._y0 = 0, this._len = 0, t && (this._saveData = !1), this._saveData && (this.data = []);
  }
  return e.prototype.increaseVersion = function() {
    this._version++;
  }, e.prototype.getVersion = function() {
    return this._version;
  }, e.prototype.setScale = function(t, r, n) {
    n = n || 0, n > 0 && (this._ux = Fe(n / ho / t) || 0, this._uy = Fe(n / ho / r) || 0);
  }, e.prototype.setDPR = function(t) {
    this.dpr = t;
  }, e.prototype.setContext = function(t) {
    this._ctx = t;
  }, e.prototype.getContext = function() {
    return this._ctx;
  }, e.prototype.beginPath = function() {
    return this._ctx && this._ctx.beginPath(), this.reset(), this;
  }, e.prototype.reset = function() {
    this._saveData && (this._len = 0), this._pathSegLen && (this._pathSegLen = null, this._pathLen = 0), this._version++;
  }, e.prototype.moveTo = function(t, r) {
    return this._drawPendingPt(), this.addData(K.M, t, r), this._ctx && this._ctx.moveTo(t, r), this._x0 = t, this._y0 = r, this._xi = t, this._yi = r, this;
  }, e.prototype.lineTo = function(t, r) {
    var n = Fe(t - this._xi), i = Fe(r - this._yi), a = n > this._ux || i > this._uy;
    if (this.addData(K.L, t, r), this._ctx && a && this._ctx.lineTo(t, r), a)
      this._xi = t, this._yi = r, this._pendingPtDist = 0;
    else {
      var o = n * n + i * i;
      o > this._pendingPtDist && (this._pendingPtX = t, this._pendingPtY = r, this._pendingPtDist = o);
    }
    return this;
  }, e.prototype.bezierCurveTo = function(t, r, n, i, a, o) {
    return this._drawPendingPt(), this.addData(K.C, t, r, n, i, a, o), this._ctx && this._ctx.bezierCurveTo(t, r, n, i, a, o), this._xi = a, this._yi = o, this;
  }, e.prototype.quadraticCurveTo = function(t, r, n, i) {
    return this._drawPendingPt(), this.addData(K.Q, t, r, n, i), this._ctx && this._ctx.quadraticCurveTo(t, r, n, i), this._xi = n, this._yi = i, this;
  }, e.prototype.arc = function(t, r, n, i, a, o) {
    this._drawPendingPt(), Zn[0] = i, Zn[1] = a, eS(Zn, o), i = Zn[0], a = Zn[1];
    var s = a - i;
    return this.addData(K.A, t, r, n, n, i, s, 0, o ? 0 : 1), this._ctx && this._ctx.arc(t, r, n, i, a, o), this._xi = Pr(a) * n + t, this._yi = Ir(a) * n + r, this;
  }, e.prototype.arcTo = function(t, r, n, i, a) {
    return this._drawPendingPt(), this._ctx && this._ctx.arcTo(t, r, n, i, a), this;
  }, e.prototype.rect = function(t, r, n, i) {
    return this._drawPendingPt(), this._ctx && this._ctx.rect(t, r, n, i), this.addData(K.R, t, r, n, i), this;
  }, e.prototype.closePath = function() {
    this._drawPendingPt(), this.addData(K.Z);
    var t = this._ctx, r = this._x0, n = this._y0;
    return t && t.closePath(), this._xi = r, this._yi = n, this;
  }, e.prototype.fill = function(t) {
    t && t.fill(), this.toStatic();
  }, e.prototype.stroke = function(t) {
    t && t.stroke(), this.toStatic();
  }, e.prototype.len = function() {
    return this._len;
  }, e.prototype.setData = function(t) {
    var r = t.length;
    !(this.data && this.data.length === r) && Us && (this.data = new Float32Array(r));
    for (var n = 0; n < r; n++)
      this.data[n] = t[n];
    this._len = r;
  }, e.prototype.appendPath = function(t) {
    t instanceof Array || (t = [t]);
    for (var r = t.length, n = 0, i = this._len, a = 0; a < r; a++)
      n += t[a].len();
    Us && this.data instanceof Float32Array && (this.data = new Float32Array(i + n));
    for (var a = 0; a < r; a++)
      for (var o = t[a].data, s = 0; s < o.length; s++)
        this.data[i++] = o[s];
    this._len = i;
  }, e.prototype.addData = function(t, r, n, i, a, o, s, u, l) {
    if (this._saveData) {
      var f = this.data;
      this._len + arguments.length > f.length && (this._expandData(), f = this.data);
      for (var h = 0; h < arguments.length; h++)
        f[this._len++] = arguments[h];
    }
  }, e.prototype._drawPendingPt = function() {
    this._pendingPtDist > 0 && (this._ctx && this._ctx.lineTo(this._pendingPtX, this._pendingPtY), this._pendingPtDist = 0);
  }, e.prototype._expandData = function() {
    if (!(this.data instanceof Array)) {
      for (var t = [], r = 0; r < this._len; r++)
        t[r] = this.data[r];
      this.data = t;
    }
  }, e.prototype.toStatic = function() {
    if (this._saveData) {
      this._drawPendingPt();
      var t = this.data;
      t instanceof Array && (t.length = this._len, Us && this._len > 11 && (this.data = new Float32Array(t)));
    }
  }, e.prototype.getBoundingRect = function() {
    _e[0] = _e[1] = me[0] = me[1] = Number.MAX_VALUE, $e[0] = $e[1] = Se[0] = Se[1] = -Number.MAX_VALUE;
    var t = this.data, r = 0, n = 0, i = 0, a = 0, o;
    for (o = 0; o < this._len; ) {
      var s = t[o++], u = o === 1;
      switch (u && (r = t[o], n = t[o + 1], i = r, a = n), s) {
        case K.M:
          r = i = t[o++], n = a = t[o++], me[0] = i, me[1] = a, Se[0] = i, Se[1] = a;
          break;
        case K.L:
          Vh(r, n, t[o], t[o + 1], me, Se), r = t[o++], n = t[o++];
          break;
        case K.C:
          Q1(r, n, t[o++], t[o++], t[o++], t[o++], t[o], t[o + 1], me, Se), r = t[o++], n = t[o++];
          break;
        case K.Q:
          J1(r, n, t[o++], t[o++], t[o], t[o + 1], me, Se), r = t[o++], n = t[o++];
          break;
        case K.A:
          var l = t[o++], f = t[o++], h = t[o++], v = t[o++], c = t[o++], d = t[o++] + c;
          o += 1;
          var y = !t[o++];
          u && (i = Pr(c) * h + l, a = Ir(c) * v + f), tS(l, f, h, v, c, d, y, me, Se), r = Pr(d) * h + l, n = Ir(d) * v + f;
          break;
        case K.R:
          i = r = t[o++], a = n = t[o++];
          var p = t[o++], g = t[o++];
          Vh(i, a, i + p, a + g, me, Se);
          break;
        case K.Z:
          r = i, n = a;
          break;
      }
      er(_e, _e, me), rr($e, $e, Se);
    }
    return o === 0 && (_e[0] = _e[1] = $e[0] = $e[1] = 0), new j(_e[0], _e[1], $e[0] - _e[0], $e[1] - _e[1]);
  }, e.prototype._calculateLength = function() {
    var t = this.data, r = this._len, n = this._ux, i = this._uy, a = 0, o = 0, s = 0, u = 0;
    this._pathSegLen || (this._pathSegLen = []);
    for (var l = this._pathSegLen, f = 0, h = 0, v = 0; v < r; ) {
      var c = t[v++], d = v === 1;
      d && (a = t[v], o = t[v + 1], s = a, u = o);
      var y = -1;
      switch (c) {
        case K.M:
          a = s = t[v++], o = u = t[v++];
          break;
        case K.L: {
          var p = t[v++], g = t[v++], _ = p - a, m = g - o;
          (Fe(_) > n || Fe(m) > i || v === r - 1) && (y = Math.sqrt(_ * _ + m * m), a = p, o = g);
          break;
        }
        case K.C: {
          var S = t[v++], b = t[v++], p = t[v++], g = t[v++], w = t[v++], T = t[v++];
          y = hm(a, o, S, b, p, g, w, T, 10), a = w, o = T;
          break;
        }
        case K.Q: {
          var S = t[v++], b = t[v++], p = t[v++], g = t[v++];
          y = dm(a, o, S, b, p, g, 10), a = p, o = g;
          break;
        }
        case K.A:
          var C = t[v++], M = t[v++], D = t[v++], P = t[v++], A = t[v++], L = t[v++], I = L + A;
          v += 1, d && (s = Pr(A) * D + C, u = Ir(A) * P + M), y = Ws(D, P) * Vs(Je, Math.abs(L)), a = Pr(I) * D + C, o = Ir(I) * P + M;
          break;
        case K.R: {
          s = a = t[v++], u = o = t[v++];
          var R = t[v++], O = t[v++];
          y = R * 2 + O * 2;
          break;
        }
        case K.Z: {
          var _ = s - a, m = u - o;
          y = Math.sqrt(_ * _ + m * m), a = s, o = u;
          break;
        }
      }
      y >= 0 && (l[h++] = y, f += y);
    }
    return this._pathLen = f, f;
  }, e.prototype.rebuildPath = function(t, r) {
    var n = this.data, i = this._ux, a = this._uy, o = this._len, s, u, l, f, h, v, c = r < 1, d, y, p = 0, g = 0, _, m = 0, S, b;
    if (!(c && (this._pathSegLen || this._calculateLength(), d = this._pathSegLen, y = this._pathLen, _ = r * y, !_)))
      t: for (var w = 0; w < o; ) {
        var T = n[w++], C = w === 1;
        switch (C && (l = n[w], f = n[w + 1], s = l, u = f), T !== K.L && m > 0 && (t.lineTo(S, b), m = 0), T) {
          case K.M:
            s = l = n[w++], u = f = n[w++], t.moveTo(l, f);
            break;
          case K.L: {
            h = n[w++], v = n[w++];
            var M = Fe(h - l), D = Fe(v - f);
            if (M > i || D > a) {
              if (c) {
                var P = d[g++];
                if (p + P > _) {
                  var A = (_ - p) / P;
                  t.lineTo(l * (1 - A) + h * A, f * (1 - A) + v * A);
                  break t;
                }
                p += P;
              }
              t.lineTo(h, v), l = h, f = v, m = 0;
            } else {
              var L = M * M + D * D;
              L > m && (S = h, b = v, m = L);
            }
            break;
          }
          case K.C: {
            var I = n[w++], R = n[w++], O = n[w++], q = n[w++], H = n[w++], G = n[w++];
            if (c) {
              var P = d[g++];
              if (p + P > _) {
                var A = (_ - p) / P;
                lo(l, I, O, H, A, Mr), lo(f, R, q, G, A, Dr), t.bezierCurveTo(Mr[1], Dr[1], Mr[2], Dr[2], Mr[3], Dr[3]);
                break t;
              }
              p += P;
            }
            t.bezierCurveTo(I, R, O, q, H, G), l = H, f = G;
            break;
          }
          case K.Q: {
            var I = n[w++], R = n[w++], O = n[w++], q = n[w++];
            if (c) {
              var P = d[g++];
              if (p + P > _) {
                var A = (_ - p) / P;
                fo(l, I, O, A, Mr), fo(f, R, q, A, Dr), t.quadraticCurveTo(Mr[1], Dr[1], Mr[2], Dr[2]);
                break t;
              }
              p += P;
            }
            t.quadraticCurveTo(I, R, O, q), l = O, f = q;
            break;
          }
          case K.A:
            var X = n[w++], nt = n[w++], J = n[w++], mt = n[w++], Zt = n[w++], Xe = n[w++], pr = n[w++], gr = !n[w++], rn = J > mt ? J : mt, Vt = Fe(J - mt) > 1e-3, yt = Zt + Xe, V = !1;
            if (c) {
              var P = d[g++];
              p + P > _ && (yt = Zt + Xe * (_ - p) / P, V = !0), p += P;
            }
            if (Vt && t.ellipse ? t.ellipse(X, nt, J, mt, pr, Zt, yt, gr) : t.arc(X, nt, rn, Zt, yt, gr), V)
              break t;
            C && (s = Pr(Zt) * J + X, u = Ir(Zt) * mt + nt), l = Pr(yt) * J + X, f = Ir(yt) * mt + nt;
            break;
          case K.R:
            s = l = n[w], u = f = n[w + 1], h = n[w++], v = n[w++];
            var Z = n[w++], yr = n[w++];
            if (c) {
              var P = d[g++];
              if (p + P > _) {
                var Ct = _ - p;
                t.moveTo(h, v), t.lineTo(h + Vs(Ct, Z), v), Ct -= Z, Ct > 0 && t.lineTo(h + Z, v + Vs(Ct, yr)), Ct -= yr, Ct > 0 && t.lineTo(h + Ws(Z - Ct, 0), v + yr), Ct -= Z, Ct > 0 && t.lineTo(h, v + Ws(yr - Ct, 0));
                break t;
              }
              p += P;
            }
            t.rect(h, v, Z, yr);
            break;
          case K.Z:
            if (c) {
              var P = d[g++];
              if (p + P > _) {
                var A = (_ - p) / P;
                t.lineTo(l * (1 - A) + s * A, f * (1 - A) + u * A);
                break t;
              }
              p += P;
            }
            t.closePath(), l = s, f = u;
        }
      }
  }, e.prototype.clone = function() {
    var t = new e(), r = this.data;
    return t.data = r.slice ? r.slice() : Array.prototype.slice.call(r), t._len = this._len, t;
  }, e.CMD = K, e.initDefaultProps = (function() {
    var t = e.prototype;
    t._saveData = !0, t._ux = 0, t._uy = 0, t._pendingPtDist = 0, t._version = 0;
  })(), e;
})();
function un(e, t, r, n, i, a, o) {
  if (i === 0)
    return !1;
  var s = i, u = 0, l = e;
  if (o > t + s && o > n + s || o < t - s && o < n - s || a > e + s && a > r + s || a < e - s && a < r - s)
    return !1;
  if (e !== r)
    u = (t - n) / (e - r), l = (e * n - r * t) / (e - r);
  else
    return Math.abs(a - e) <= s / 2;
  var f = u * a - o + l, h = f * f / (u * u + 1);
  return h <= s / 2 * s / 2;
}
function rS(e, t, r, n, i, a, o, s, u, l, f) {
  if (u === 0)
    return !1;
  var h = u;
  if (f > t + h && f > n + h && f > a + h && f > s + h || f < t - h && f < n - h && f < a - h && f < s - h || l > e + h && l > r + h && l > i + h && l > o + h || l < e - h && l < r - h && l < i - h && l < o - h)
    return !1;
  var v = fm(e, t, r, n, i, a, o, s, l, f, null);
  return v <= h / 2;
}
function nS(e, t, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  if (u > t + l && u > n + l && u > a + l || u < t - l && u < n - l && u < a - l || s > e + l && s > r + l && s > i + l || s < e - l && s < r - l && s < i - l)
    return !1;
  var f = vm(e, t, r, n, i, a, s, u, null);
  return f <= l / 2;
}
var Yh = Math.PI * 2;
function _a(e) {
  return e %= Yh, e < 0 && (e += Yh), e;
}
var jn = Math.PI * 2;
function iS(e, t, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  s -= e, u -= t;
  var f = Math.sqrt(s * s + u * u);
  if (f - l > r || f + l < r)
    return !1;
  if (Math.abs(n - i) % jn < 1e-4)
    return !0;
  if (a) {
    var h = n;
    n = _a(i), i = _a(h);
  } else
    n = _a(n), i = _a(i);
  n > i && (i += jn);
  var v = Math.atan2(u, s);
  return v < 0 && (v += jn), v >= n && v <= i || v + jn >= n && v + jn <= i;
}
function He(e, t, r, n, i, a) {
  if (a > t && a > n || a < t && a < n || n === t)
    return 0;
  var o = (a - t) / (n - t), s = n < t ? 1 : -1;
  (o === 1 || o === 0) && (s = n < t ? 0.5 : -0.5);
  var u = o * (r - e) + e;
  return u === i ? 1 / 0 : u > i ? s : 0;
}
var Ze = Ln.CMD, Ar = Math.PI * 2, aS = 1e-4;
function oS(e, t) {
  return Math.abs(e - t) < aS;
}
var Mt = [-1, -1, -1], ee = [-1, -1];
function sS() {
  var e = ee[0];
  ee[0] = ee[1], ee[1] = e;
}
function uS(e, t, r, n, i, a, o, s, u, l) {
  if (l > t && l > n && l > a && l > s || l < t && l < n && l < a && l < s)
    return 0;
  var f = yp(t, n, a, s, l, Mt);
  if (f === 0)
    return 0;
  for (var h = 0, v = -1, c = void 0, d = void 0, y = 0; y < f; y++) {
    var p = Mt[y], g = p === 0 || p === 1 ? 0.5 : 1, _ = _t(e, r, i, o, p);
    _ < u || (v < 0 && (v = _p(t, n, a, s, ee), ee[1] < ee[0] && v > 1 && sS(), c = _t(t, n, a, s, ee[0]), v > 1 && (d = _t(t, n, a, s, ee[1]))), v === 2 ? p < ee[0] ? h += c < t ? g : -g : p < ee[1] ? h += d < c ? g : -g : h += s < d ? g : -g : p < ee[0] ? h += c < t ? g : -g : h += s < c ? g : -g);
  }
  return h;
}
function lS(e, t, r, n, i, a, o, s) {
  if (s > t && s > n && s > a || s < t && s < n && s < a)
    return 0;
  var u = cm(t, n, a, s, Mt);
  if (u === 0)
    return 0;
  var l = mp(t, n, a);
  if (l >= 0 && l <= 1) {
    for (var f = 0, h = Tt(t, n, a, l), v = 0; v < u; v++) {
      var c = Mt[v] === 0 || Mt[v] === 1 ? 0.5 : 1, d = Tt(e, r, i, Mt[v]);
      d < o || (Mt[v] < l ? f += h < t ? c : -c : f += a < h ? c : -c);
    }
    return f;
  } else {
    var c = Mt[0] === 0 || Mt[0] === 1 ? 0.5 : 1, d = Tt(e, r, i, Mt[0]);
    return d < o ? 0 : a < t ? c : -c;
  }
}
function fS(e, t, r, n, i, a, o, s) {
  if (s -= t, s > r || s < -r)
    return 0;
  var u = Math.sqrt(r * r - s * s);
  Mt[0] = -u, Mt[1] = u;
  var l = Math.abs(n - i);
  if (l < 1e-4)
    return 0;
  if (l >= Ar - 1e-4) {
    n = 0, i = Ar;
    var f = a ? 1 : -1;
    return o >= Mt[0] + e && o <= Mt[1] + e ? f : 0;
  }
  if (n > i) {
    var h = n;
    n = i, i = h;
  }
  n < 0 && (n += Ar, i += Ar);
  for (var v = 0, c = 0; c < 2; c++) {
    var d = Mt[c];
    if (d + e > o) {
      var y = Math.atan2(s, d), f = a ? 1 : -1;
      y < 0 && (y = Ar + y), (y >= n && y <= i || y + Ar >= n && y + Ar <= i) && (y > Math.PI / 2 && y < Math.PI * 1.5 && (f = -f), v += f);
    }
  }
  return v;
}
function Zp(e, t, r, n, i) {
  for (var a = e.data, o = e.len(), s = 0, u = 0, l = 0, f = 0, h = 0, v, c, d = 0; d < o; ) {
    var y = a[d++], p = d === 1;
    switch (y === Ze.M && d > 1 && (r || (s += He(u, l, f, h, n, i))), p && (u = a[d], l = a[d + 1], f = u, h = l), y) {
      case Ze.M:
        f = a[d++], h = a[d++], u = f, l = h;
        break;
      case Ze.L:
        if (r) {
          if (un(u, l, a[d], a[d + 1], t, n, i))
            return !0;
        } else
          s += He(u, l, a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case Ze.C:
        if (r) {
          if (rS(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], t, n, i))
            return !0;
        } else
          s += uS(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case Ze.Q:
        if (r) {
          if (nS(u, l, a[d++], a[d++], a[d], a[d + 1], t, n, i))
            return !0;
        } else
          s += lS(u, l, a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case Ze.A:
        var g = a[d++], _ = a[d++], m = a[d++], S = a[d++], b = a[d++], w = a[d++];
        d += 1;
        var T = !!(1 - a[d++]);
        v = Math.cos(b) * m + g, c = Math.sin(b) * S + _, p ? (f = v, h = c) : s += He(u, l, v, c, n, i);
        var C = (n - g) * S / m + g;
        if (r) {
          if (iS(g, _, S, b, b + w, T, t, C, i))
            return !0;
        } else
          s += fS(g, _, S, b, b + w, T, C, i);
        u = Math.cos(b + w) * m + g, l = Math.sin(b + w) * S + _;
        break;
      case Ze.R:
        f = u = a[d++], h = l = a[d++];
        var M = a[d++], D = a[d++];
        if (v = f + M, c = h + D, r) {
          if (un(f, h, v, h, t, n, i) || un(v, h, v, c, t, n, i) || un(v, c, f, c, t, n, i) || un(f, c, f, h, t, n, i))
            return !0;
        } else
          s += He(v, h, v, c, n, i), s += He(f, c, f, h, n, i);
        break;
      case Ze.Z:
        if (r) {
          if (un(u, l, f, h, t, n, i))
            return !0;
        } else
          s += He(u, l, f, h, n, i);
        u = f, l = h;
        break;
    }
  }
  return !r && !oS(l, h) && (s += He(u, l, f, h, n, i) || 0), s !== 0;
}
function hS(e, t, r) {
  return Zp(e, 0, !1, t, r);
}
function cS(e, t, r, n) {
  return Zp(e, t, !0, r, n);
}
var jp = ft({
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
}, qr), vS = {
  style: ft({
    fill: !0,
    stroke: !0,
    strokePercent: !0,
    fillOpacity: !0,
    strokeOpacity: !0,
    lineDashOffset: !0,
    lineWidth: !0,
    miterLimit: !0
  }, Ho.style)
}, qs = Ei.concat([
  "invisible",
  "culling",
  "z",
  "z2",
  "zlevel",
  "parent"
]), ht = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.update = function() {
    var r = this;
    e.prototype.update.call(this);
    var n = this.style;
    if (n.decal) {
      var i = this._decalEl = this._decalEl || new t();
      i.buildPath === t.prototype.buildPath && (i.buildPath = function(u) {
        r.buildPath(u, r.shape);
      }), i.silent = !0;
      var a = i.style;
      for (var o in n)
        a[o] !== n[o] && (a[o] = n[o]);
      a.fill = n.fill ? n.decal : null, a.decal = null, a.shadowColor = null, n.strokeFirst && (a.stroke = null);
      for (var s = 0; s < qs.length; ++s)
        i[qs[s]] = this[qs[s]];
      i.__dirty |= Ut;
    } else this._decalEl && (this._decalEl = null);
  }, t.prototype.getDecalElement = function() {
    return this._decalEl;
  }, t.prototype._init = function(r) {
    var n = ct(r);
    this.shape = this.getDefaultShape();
    var i = this.getDefaultStyle();
    i && this.useStyle(i);
    for (var a = 0; a < n.length; a++) {
      var o = n[a], s = r[o];
      o === "style" ? this.style ? E(this.style, s) : this.useStyle(s) : o === "shape" ? E(this.shape, s) : e.prototype.attrKV.call(this, o, s);
    }
    this.style || this.useStyle({});
  }, t.prototype.getDefaultStyle = function() {
    return null;
  }, t.prototype.getDefaultShape = function() {
    return {};
  }, t.prototype.canBeInsideText = function() {
    return this.hasFill();
  }, t.prototype.getInsideTextFill = function() {
    var r = this.style.fill;
    if (r !== "none") {
      if (B(r)) {
        var n = Ri(r, 0);
        return n > 0.5 ? Xu : n > 0.2 ? Vm : $u;
      } else if (r)
        return $u;
    }
    return Xu;
  }, t.prototype.getInsideTextStroke = function(r) {
    var n = this.style.fill;
    if (B(n)) {
      var i = this.__zr, a = !!(i && i.isDarkMode()), o = Ri(r, 0) < qu;
      if (a === o)
        return n;
    }
  }, t.prototype.buildPath = function(r, n, i) {
  }, t.prototype.pathUpdated = function() {
    this.__dirty &= ~mn;
  }, t.prototype.getUpdatedPathProxy = function(r) {
    return !this.path && this.createPathProxy(), this.path.beginPath(), this.buildPath(this.path, this.shape, r), this.path;
  }, t.prototype.createPathProxy = function() {
    this.path = new Ln(!1);
  }, t.prototype.hasStroke = function() {
    var r = this.style, n = r.stroke;
    return !(n == null || n === "none" || !(r.lineWidth > 0));
  }, t.prototype.hasFill = function() {
    var r = this.style, n = r.fill;
    return n != null && n !== "none";
  }, t.prototype.getBoundingRect = function() {
    var r = this._rect, n = this.style, i = !r;
    if (i) {
      var a = !1;
      this.path || (a = !0, this.createPathProxy());
      var o = this.path;
      (a || this.__dirty & mn) && (o.beginPath(), this.buildPath(o, this.shape, !1), this.pathUpdated()), r = o.getBoundingRect();
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
  }, t.prototype.contain = function(r, n) {
    var i = this.transformCoordToLocal(r, n), a = this.getBoundingRect(), o = this.style;
    if (r = i[0], n = i[1], a.contain(r, n)) {
      var s = this.path;
      if (this.hasStroke()) {
        var u = o.lineWidth, l = o.strokeNoScale ? this.getLineScale() : 1;
        if (l > 1e-10 && (this.hasFill() || (u = Math.max(u, this.strokeContainThreshold)), cS(s, u / l, r, n)))
          return !0;
      }
      if (this.hasFill())
        return hS(s, r, n);
    }
    return !1;
  }, t.prototype.dirtyShape = function() {
    this.__dirty |= mn, this._rect && (this._rect = null), this._decalEl && this._decalEl.dirtyShape(), this.markRedraw();
  }, t.prototype.dirty = function() {
    this.dirtyStyle(), this.dirtyShape();
  }, t.prototype.animateShape = function(r) {
    return this.animate("shape", r);
  }, t.prototype.updateDuringAnimation = function(r) {
    r === "style" ? this.dirtyStyle() : r === "shape" ? this.dirtyShape() : this.markRedraw();
  }, t.prototype.attrKV = function(r, n) {
    r === "shape" ? this.setShape(n) : e.prototype.attrKV.call(this, r, n);
  }, t.prototype.setShape = function(r, n) {
    var i = this.shape;
    return i || (i = this.shape = {}), typeof r == "string" ? i[r] = n : E(i, r), this.dirtyShape(), this;
  }, t.prototype.shapeChanged = function() {
    return !!(this.__dirty & mn);
  }, t.prototype.createStyle = function(r) {
    return Xi(jp, r);
  }, t.prototype._innerSaveToNormal = function(r) {
    e.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.shape && !n.shape && (n.shape = E({}, this.shape));
  }, t.prototype._applyStateObj = function(r, n, i, a, o, s) {
    e.prototype._applyStateObj.call(this, r, n, i, a, o, s);
    var u = !(n && a), l;
    if (n && n.shape ? o ? a ? l = n.shape : (l = E({}, i.shape), E(l, n.shape)) : (l = E({}, a ? this.shape : i.shape), E(l, n.shape)) : u && (l = i.shape), l)
      if (o) {
        this.shape = E({}, this.shape);
        for (var f = {}, h = ct(l), v = 0; v < h.length; v++) {
          var c = h[v];
          typeof l[c] == "object" ? this.shape[c] = l[c] : f[c] = l[c];
        }
        this._transitionState(r, {
          shape: f
        }, s);
      } else
        this.shape = l, this.dirtyShape();
  }, t.prototype._mergeStates = function(r) {
    for (var n = e.prototype._mergeStates.call(this, r), i, a = 0; a < r.length; a++) {
      var o = r[a];
      o.shape && (i = i || {}, this._mergeStyle(i, o.shape));
    }
    return i && (n.shape = i), n;
  }, t.prototype.getAnimationStyleProps = function() {
    return vS;
  }, t.prototype.isZeroArea = function() {
    return !1;
  }, t.extend = function(r) {
    var n = (function(a) {
      tt(o, a);
      function o(s) {
        var u = a.call(this, s) || this;
        return r.init && r.init.call(u, s), u;
      }
      return o.prototype.getDefaultStyle = function() {
        return $(r.style);
      }, o.prototype.getDefaultShape = function() {
        return $(r.shape);
      }, o;
    })(t);
    for (var i in r)
      typeof r[i] == "function" && (n.prototype[i] = r[i]);
    return n;
  }, t.initDefaultProps = (function() {
    var r = t.prototype;
    r.type = "path", r.strokeContainThreshold = 5, r.segmentIgnoreThreshold = 0, r.subPixelOptimize = !1, r.autoBatch = !1, r.__dirty = Ut | fi | mn;
  })(), t;
})(Ji), dS = ft({
  strokeFirst: !0,
  font: jr,
  x: 0,
  y: 0,
  textAlign: "left",
  textBaseline: "top",
  miterLimit: 2
}, jp), co = (function(e) {
  tt(t, e);
  function t() {
    return e !== null && e.apply(this, arguments) || this;
  }
  return t.prototype.hasStroke = function() {
    var r = this.style, n = r.stroke;
    return n != null && n !== "none" && r.lineWidth > 0;
  }, t.prototype.hasFill = function() {
    var r = this.style, n = r.fill;
    return n != null && n !== "none";
  }, t.prototype.createStyle = function(r) {
    return Xi(dS, r);
  }, t.prototype.setBoundingRect = function(r) {
    this._rect = r;
  }, t.prototype.getBoundingRect = function() {
    var r = this.style;
    if (!this._rect) {
      var n = r.text;
      n != null ? n += "" : n = "";
      var i = ql(n, r.font, r.textAlign, r.textBaseline);
      if (i.x += r.x || 0, i.y += r.y || 0, this.hasStroke()) {
        var a = r.lineWidth;
        i.x -= a / 2, i.y -= a / 2, i.width += a, i.height += a;
      }
      this._rect = i;
    }
    return this._rect;
  }, t.initDefaultProps = (function() {
    var r = t.prototype;
    r.dirtyRectTolerance = 10;
  })(), t;
})(Ji);
co.prototype.type = "tspan";
var pS = ft({
  x: 0,
  y: 0
}, qr), gS = {
  style: ft({
    x: !0,
    y: !0,
    width: !0,
    height: !0,
    sx: !0,
    sy: !0,
    sWidth: !0,
    sHeight: !0
  }, Ho.style)
};
function yS(e) {
  return !!(e && typeof e != "string" && e.width && e.height);
}
var vr = (function(e) {
  tt(t, e);
  function t() {
    return e !== null && e.apply(this, arguments) || this;
  }
  return t.prototype.createStyle = function(r) {
    return Xi(pS, r);
  }, t.prototype._getSize = function(r) {
    var n = this.style, i = n[r];
    if (i != null)
      return i;
    var a = yS(n.image) ? n.image : this.__image;
    if (!a)
      return 0;
    var o = r === "width" ? "height" : "width", s = n[o];
    return s == null ? a[r] : a[r] / a[o] * s;
  }, t.prototype.getWidth = function() {
    return this._getSize("width");
  }, t.prototype.getHeight = function() {
    return this._getSize("height");
  }, t.prototype.getAnimationStyleProps = function() {
    return gS;
  }, t.prototype.getBoundingRect = function() {
    var r = this.style;
    return this._rect || (this._rect = new j(r.x || 0, r.y || 0, this.getWidth(), this.getHeight())), this._rect;
  }, t;
})(Ji);
vr.prototype.type = "image";
function _S(e, t) {
  var r = t.x, n = t.y, i = t.width, a = t.height, o = t.r, s, u, l, f;
  i < 0 && (r = r + i, i = -i), a < 0 && (n = n + a, a = -a), typeof o == "number" ? s = u = l = f = o : o instanceof Array ? o.length === 1 ? s = u = l = f = o[0] : o.length === 2 ? (s = l = o[0], u = f = o[1]) : o.length === 3 ? (s = o[0], u = f = o[1], l = o[2]) : (s = o[0], u = o[1], l = o[2], f = o[3]) : s = u = l = f = 0;
  var h;
  s + u > i && (h = s + u, s *= i / h, u *= i / h), l + f > i && (h = l + f, l *= i / h, f *= i / h), u + l > a && (h = u + l, u *= a / h, l *= a / h), s + f > a && (h = s + f, s *= a / h, f *= a / h), e.moveTo(r + s, n), e.lineTo(r + i - u, n), u !== 0 && e.arc(r + i - u, n + u, u, -Math.PI / 2, 0), e.lineTo(r + i, n + a - l), l !== 0 && e.arc(r + i - l, n + a - l, l, 0, Math.PI / 2), e.lineTo(r + f, n + a), f !== 0 && e.arc(r + f, n + a - f, f, Math.PI / 2, Math.PI), e.lineTo(r, n + s), s !== 0 && e.arc(r + s, n + s, s, Math.PI, Math.PI * 1.5);
}
var bn = Math.round;
function Kp(e, t, r) {
  if (t) {
    var n = t.x1, i = t.x2, a = t.y1, o = t.y2;
    e.x1 = n, e.x2 = i, e.y1 = a, e.y2 = o;
    var s = r && r.lineWidth;
    return s && (bn(n * 2) === bn(i * 2) && (e.x1 = e.x2 = Gr(n, s, !0)), bn(a * 2) === bn(o * 2) && (e.y1 = e.y2 = Gr(a, s, !0))), e;
  }
}
function Qp(e, t, r) {
  if (t) {
    var n = t.x, i = t.y, a = t.width, o = t.height;
    e.x = n, e.y = i, e.width = a, e.height = o;
    var s = r && r.lineWidth;
    return s && (e.x = Gr(n, s, !0), e.y = Gr(i, s, !0), e.width = Math.max(Gr(n + a, s, !1) - e.x, a === 0 ? 0 : 1), e.height = Math.max(Gr(i + o, s, !1) - e.y, o === 0 ? 0 : 1)), e;
  }
}
function Gr(e, t, r) {
  if (!t)
    return e;
  var n = bn(e * 2);
  return (n + bn(t)) % 2 === 0 ? n / 2 : (n + (r ? 1 : -1)) / 2;
}
var mS = /* @__PURE__ */ (function() {
  function e() {
    this.x = 0, this.y = 0, this.width = 0, this.height = 0;
  }
  return e;
})(), SS = {}, Et = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new mS();
  }, t.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = Qp(SS, n, this.style);
      i = u.x, a = u.y, o = u.width, s = u.height, u.r = n.r, n = u;
    } else
      i = n.x, a = n.y, o = n.width, s = n.height;
    n.r ? _S(r, n) : r.rect(i, a, o, s);
  }, t.prototype.isZeroArea = function() {
    return !this.shape.width || !this.shape.height;
  }, t;
})(ht);
Et.prototype.type = "rect";
var qh = {
  fill: "#000"
}, Xh = 2, wS = {
  style: ft({
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
  }, Ho.style)
}, Bt = (function(e) {
  tt(t, e);
  function t(r) {
    var n = e.call(this) || this;
    return n.type = "text", n._children = [], n._defaultStyle = qh, n.attr(r), n;
  }
  return t.prototype.childrenRef = function() {
    return this._children;
  }, t.prototype.update = function() {
    e.prototype.update.call(this), this.styleChanged() && this._updateSubTexts();
    for (var r = 0; r < this._children.length; r++) {
      var n = this._children[r];
      n.zlevel = this.zlevel, n.z = this.z, n.z2 = this.z2, n.culling = this.culling, n.cursor = this.cursor, n.invisible = this.invisible;
    }
  }, t.prototype.updateTransform = function() {
    var r = this.innerTransformable;
    r ? (r.updateTransform(), r.transform && (this.transform = r.transform)) : e.prototype.updateTransform.call(this);
  }, t.prototype.getLocalTransform = function(r) {
    var n = this.innerTransformable;
    return n ? n.getLocalTransform(r) : e.prototype.getLocalTransform.call(this, r);
  }, t.prototype.getComputedTransform = function() {
    return this.__hostTarget && (this.__hostTarget.getComputedTransform(), this.__hostTarget.updateInnerText(!0)), e.prototype.getComputedTransform.call(this);
  }, t.prototype._updateSubTexts = function() {
    this._childCursor = 0, MS(this.style), this.style.rich ? this._updateRichTexts() : this._updatePlainTexts(), this._children.length = this._childCursor, this.styleUpdated();
  }, t.prototype.addSelfToZr = function(r) {
    e.prototype.addSelfToZr.call(this, r);
    for (var n = 0; n < this._children.length; n++)
      this._children[n].__zr = r;
  }, t.prototype.removeSelfFromZr = function(r) {
    e.prototype.removeSelfFromZr.call(this, r);
    for (var n = 0; n < this._children.length; n++)
      this._children[n].__zr = null;
  }, t.prototype.getBoundingRect = function() {
    if (this.styleChanged() && this._updateSubTexts(), !this._rect) {
      for (var r = new j(0, 0, 0, 0), n = this._children, i = [], a = null, o = 0; o < n.length; o++) {
        var s = n[o], u = s.getBoundingRect(), l = s.getLocalTransform(i);
        l ? (r.copy(u), r.applyTransform(l), a = a || r.clone(), a.union(r)) : (a = a || u.clone(), a.union(u));
      }
      this._rect = a || r;
    }
    return this._rect;
  }, t.prototype.setDefaultTextStyle = function(r) {
    this._defaultStyle = r || qh;
  }, t.prototype.setTextContent = function(r) {
  }, t.prototype._mergeStyle = function(r, n) {
    if (!n)
      return r;
    var i = n.rich, a = r.rich || i && {};
    return E(r, n), i && a ? (this._mergeRich(a, i), r.rich = a) : a && (r.rich = a), r;
  }, t.prototype._mergeRich = function(r, n) {
    for (var i = ct(n), a = 0; a < i.length; a++) {
      var o = i[a];
      r[o] = r[o] || {}, E(r[o], n[o]);
    }
  }, t.prototype.getAnimationStyleProps = function() {
    return wS;
  }, t.prototype._getOrCreateChild = function(r) {
    var n = this._children[this._childCursor];
    return (!n || !(n instanceof r)) && (n = new r()), this._children[this._childCursor++] = n, n.__zr = this.__zr, n.parent = this, n;
  }, t.prototype._updatePlainTexts = function() {
    var r = this.style, n = r.font || jr, i = r.padding, a = tc(r), o = W1(a, r), s = Xs(r), u = !!r.backgroundColor, l = o.outerHeight, f = o.outerWidth, h = o.contentWidth, v = o.lines, c = o.lineHeight, d = this._defaultStyle;
    this.isTruncated = !!o.isTruncated;
    var y = r.x || 0, p = r.y || 0, g = r.align || d.align || "left", _ = r.verticalAlign || d.verticalAlign || "top", m = y, S = Sn(p, o.contentHeight, _);
    if (s || i) {
      var b = ci(y, f, g), w = Sn(p, l, _);
      s && this._renderBackground(r, r, b, w, f, l);
    }
    S += c / 2, i && (m = Jh(y, g, i), _ === "top" ? S += i[0] : _ === "bottom" && (S -= i[2]));
    for (var T = 0, C = !1, M = Qh("fill" in r ? r.fill : (C = !0, d.fill)), D = Kh("stroke" in r ? r.stroke : !u && (!d.autoStroke || C) ? (T = Xh, d.stroke) : null), P = r.textShadowBlur > 0, A = r.width != null && (r.overflow === "truncate" || r.overflow === "break" || r.overflow === "breakAll"), L = o.calculatedLineHeight, I = 0; I < v.length; I++) {
      var R = this._getOrCreateChild(co), O = R.createStyle();
      R.useStyle(O), O.text = v[I], O.x = m, O.y = S, O.textAlign = g, O.textBaseline = "middle", O.opacity = r.opacity, O.strokeFirst = !0, P && (O.shadowBlur = r.textShadowBlur || 0, O.shadowColor = r.textShadowColor || "transparent", O.shadowOffsetX = r.textShadowOffsetX || 0, O.shadowOffsetY = r.textShadowOffsetY || 0), O.stroke = D, O.fill = M, D && (O.lineWidth = r.lineWidth || T, O.lineDash = r.lineDash, O.lineDashOffset = r.lineDashOffset || 0), O.font = n, Zh(O, r), S += c, A && R.setBoundingRect(new j(ci(O.x, h, O.textAlign), Sn(O.y, L, O.textBaseline), h, L));
    }
  }, t.prototype._updateRichTexts = function() {
    var r = this.style, n = tc(r), i = q1(n, r), a = i.width, o = i.outerWidth, s = i.outerHeight, u = r.padding, l = r.x || 0, f = r.y || 0, h = this._defaultStyle, v = r.align || h.align, c = r.verticalAlign || h.verticalAlign;
    this.isTruncated = !!i.isTruncated;
    var d = ci(l, o, v), y = Sn(f, s, c), p = d, g = y;
    u && (p += u[3], g += u[0]);
    var _ = p + a;
    Xs(r) && this._renderBackground(r, r, d, y, o, s);
    for (var m = !!r.backgroundColor, S = 0; S < i.lines.length; S++) {
      for (var b = i.lines[S], w = b.tokens, T = w.length, C = b.lineHeight, M = b.width, D = 0, P = p, A = _, L = T - 1, I = void 0; D < T && (I = w[D], !I.align || I.align === "left"); )
        this._placeToken(I, r, C, g, P, "left", m), M -= I.width, P += I.width, D++;
      for (; L >= 0 && (I = w[L], I.align === "right"); )
        this._placeToken(I, r, C, g, A, "right", m), M -= I.width, A -= I.width, L--;
      for (P += (a - (P - p) - (_ - A) - M) / 2; D <= L; )
        I = w[D], this._placeToken(I, r, C, g, P + I.width / 2, "center", m), P += I.width, D++;
      g += C;
    }
  }, t.prototype._placeToken = function(r, n, i, a, o, s, u) {
    var l = n.rich[r.styleName] || {};
    l.text = r.text;
    var f = r.verticalAlign, h = a + i / 2;
    f === "top" ? h = a + r.height / 2 : f === "bottom" && (h = a + i - r.height / 2);
    var v = !r.isLineHolder && Xs(l);
    v && this._renderBackground(l, n, s === "right" ? o - r.width : s === "center" ? o - r.width / 2 : o, h - r.height / 2, r.width, r.height);
    var c = !!l.backgroundColor, d = r.textPadding;
    d && (o = Jh(o, s, d), h -= r.height / 2 - d[0] - r.innerHeight / 2);
    var y = this._getOrCreateChild(co), p = y.createStyle();
    y.useStyle(p);
    var g = this._defaultStyle, _ = !1, m = 0, S = Qh("fill" in l ? l.fill : "fill" in n ? n.fill : (_ = !0, g.fill)), b = Kh("stroke" in l ? l.stroke : "stroke" in n ? n.stroke : !c && !u && (!g.autoStroke || _) ? (m = Xh, g.stroke) : null), w = l.textShadowBlur > 0 || n.textShadowBlur > 0;
    p.text = r.text, p.x = o, p.y = h, w && (p.shadowBlur = l.textShadowBlur || n.textShadowBlur || 0, p.shadowColor = l.textShadowColor || n.textShadowColor || "transparent", p.shadowOffsetX = l.textShadowOffsetX || n.textShadowOffsetX || 0, p.shadowOffsetY = l.textShadowOffsetY || n.textShadowOffsetY || 0), p.textAlign = s, p.textBaseline = "middle", p.font = r.font || jr, p.opacity = yi(l.opacity, n.opacity, 1), Zh(p, l), b && (p.lineWidth = yi(l.lineWidth, n.lineWidth, m), p.lineDash = Y(l.lineDash, n.lineDash), p.lineDashOffset = n.lineDashOffset || 0, p.stroke = b), S && (p.fill = S);
    var T = r.contentWidth, C = r.contentHeight;
    y.setBoundingRect(new j(ci(p.x, T, p.textAlign), Sn(p.y, C, p.textBaseline), T, C));
  }, t.prototype._renderBackground = function(r, n, i, a, o, s) {
    var u = r.backgroundColor, l = r.borderWidth, f = r.borderColor, h = u && u.image, v = u && !h, c = r.borderRadius, d = this, y, p;
    if (v || r.lineHeight || l && f) {
      y = this._getOrCreateChild(Et), y.useStyle(y.createStyle()), y.style.fill = null;
      var g = y.shape;
      g.x = i, g.y = a, g.width = o, g.height = s, g.r = c, y.dirtyShape();
    }
    if (v) {
      var _ = y.style;
      _.fill = u || null, _.fillOpacity = Y(r.fillOpacity, 1);
    } else if (h) {
      p = this._getOrCreateChild(vr), p.onload = function() {
        d.dirtyStyle();
      };
      var m = p.style;
      m.image = u.image, m.x = i, m.y = a, m.width = o, m.height = s;
    }
    if (l && f) {
      var _ = y.style;
      _.lineWidth = l, _.stroke = f, _.strokeOpacity = Y(r.strokeOpacity, 1), _.lineDash = r.borderDash, _.lineDashOffset = r.borderDashOffset || 0, y.strokeContainThreshold = 0, y.hasFill() && y.hasStroke() && (_.strokeFirst = !0, _.lineWidth *= 2);
    }
    var S = (y || p).style;
    S.shadowBlur = r.shadowBlur || 0, S.shadowColor = r.shadowColor || "transparent", S.shadowOffsetX = r.shadowOffsetX || 0, S.shadowOffsetY = r.shadowOffsetY || 0, S.opacity = yi(r.opacity, n.opacity, 1);
  }, t.makeFont = function(r) {
    var n = "";
    return CS(r) && (n = [
      r.fontStyle,
      r.fontWeight,
      xS(r.fontSize),
      r.fontFamily || "sans-serif"
    ].join(" ")), n && pe(n) || r.textFont || r.font;
  }, t;
})(Ji), bS = { left: !0, right: 1, center: 1 }, TS = { top: 1, bottom: 1, middle: 1 }, $h = ["fontStyle", "fontWeight", "fontSize", "fontFamily"];
function xS(e) {
  return typeof e == "string" && (e.indexOf("px") !== -1 || e.indexOf("rem") !== -1 || e.indexOf("em") !== -1) ? e : isNaN(+e) ? El + "px" : e + "px";
}
function Zh(e, t) {
  for (var r = 0; r < $h.length; r++) {
    var n = $h[r], i = t[n];
    i != null && (e[n] = i);
  }
}
function CS(e) {
  return e.fontSize != null || e.fontFamily || e.fontWeight;
}
function MS(e) {
  return jh(e), x(e.rich, jh), e;
}
function jh(e) {
  if (e) {
    e.font = Bt.makeFont(e);
    var t = e.align;
    t === "middle" && (t = "center"), e.align = t == null || bS[t] ? t : "left";
    var r = e.verticalAlign;
    r === "center" && (r = "middle"), e.verticalAlign = r == null || TS[r] ? r : "top";
    var n = e.padding;
    n && (e.padding = Nl(e.padding));
  }
}
function Kh(e, t) {
  return e == null || t <= 0 || e === "transparent" || e === "none" ? null : e.image || e.colorStops ? "#000" : e;
}
function Qh(e) {
  return e == null || e === "none" ? null : e.image || e.colorStops ? "#000" : e;
}
function Jh(e, t, r) {
  return t === "right" ? e - r[1] : t === "center" ? e + r[3] / 2 - r[1] / 2 : e + r[3];
}
function tc(e) {
  var t = e.text;
  return t != null && (t += ""), t;
}
function Xs(e) {
  return !!(e.backgroundColor || e.lineHeight || e.borderWidth && e.borderColor);
}
var at = gt(), DS = function(e, t, r, n) {
  if (n) {
    var i = at(n);
    i.dataIndex = r, i.dataType = t, i.seriesIndex = e, i.ssrType = "chart", n.type === "group" && n.traverse(function(a) {
      var o = at(a);
      o.seriesIndex = e, o.dataIndex = r, o.dataType = t, o.ssrType = "chart";
    });
  }
}, ec = 1, rc = {}, Jp = gt(), Jl = gt(), tg = 0, tf = 1, ef = 2, hr = ["emphasis", "blur", "select"], nc = ["normal", "emphasis", "blur", "select"], PS = 10, IS = 9, Xr = "highlight", Za = "downplay", Si = "select", ja = "unselect", wi = "toggleSelect";
function ln(e) {
  return e != null && e !== "none";
}
function Go(e, t, r) {
  e.onHoverStateChange && (e.hoverState || 0) !== r && e.onHoverStateChange(t), e.hoverState = r;
}
function eg(e) {
  Go(e, "emphasis", ef);
}
function rg(e) {
  e.hoverState === ef && Go(e, "normal", tg);
}
function rf(e) {
  Go(e, "blur", tf);
}
function ng(e) {
  e.hoverState === tf && Go(e, "normal", tg);
}
function AS(e) {
  e.selected = !0;
}
function LS(e) {
  e.selected = !1;
}
function ic(e, t, r) {
  t(e, r);
}
function Ye(e, t, r) {
  ic(e, t, r), e.isGroup && e.traverse(function(n) {
    ic(n, t, r);
  });
}
function RS(e, t, r, n) {
  for (var i = e.style, a = {}, o = 0; o < t.length; o++) {
    var s = t[o], u = i[s];
    a[s] = u ?? (n && n[s]);
  }
  for (var o = 0; o < e.animators.length; o++) {
    var l = e.animators[o];
    l.__fromStateTransition && l.__fromStateTransition.indexOf(r) < 0 && l.targetName === "style" && l.saveTo(a, t);
  }
  return a;
}
function ES(e, t, r, n) {
  var i = r && rt(r, "select") >= 0, a = !1;
  if (e instanceof ht) {
    var o = Jp(e), s = i && o.selectFill || o.normalFill, u = i && o.selectStroke || o.normalStroke;
    if (ln(s) || ln(u)) {
      n = n || {};
      var l = n.style || {};
      l.fill === "inherit" ? (a = !0, n = E({}, n), l = E({}, l), l.fill = s) : !ln(l.fill) && ln(s) ? (a = !0, n = E({}, n), l = E({}, l), l.fill = Hu(s)) : !ln(l.stroke) && ln(u) && (a || (n = E({}, n), l = E({}, l)), l.stroke = Hu(u)), n.style = l;
    }
  }
  if (n && n.z2 == null) {
    a || (n = E({}, n));
    var f = e.z2EmphasisLift;
    n.z2 = e.z2 + (f ?? PS);
  }
  return n;
}
function OS(e, t, r) {
  if (r && r.z2 == null) {
    r = E({}, r);
    var n = e.z2SelectLift;
    r.z2 = e.z2 + (n ?? IS);
  }
  return r;
}
function kS(e, t, r) {
  var n = rt(e.currentStates, t) >= 0, i = e.style.opacity, a = n ? null : RS(e, ["opacity"], t, {
    opacity: 1
  });
  r = r || {};
  var o = r.style || {};
  return o.opacity == null && (r = E({}, r), o = E({
    // Already being applied 'emphasis'. DON'T mul opacity multiple times.
    opacity: n ? i : a.opacity * 0.1
  }, o), r.style = o), r;
}
function $s(e, t) {
  var r = this.states[e];
  if (this.style) {
    if (e === "emphasis")
      return ES(this, e, t, r);
    if (e === "blur")
      return kS(this, e, r);
    if (e === "select")
      return OS(this, e, r);
  }
  return r;
}
function BS(e) {
  e.stateProxy = $s;
  var t = e.getTextContent(), r = e.getTextGuideLine();
  t && (t.stateProxy = $s), r && (r.stateProxy = $s);
}
function ac(e, t) {
  !sg(e, t) && !e.__highByOuter && Ye(e, eg);
}
function oc(e, t) {
  !sg(e, t) && !e.__highByOuter && Ye(e, rg);
}
function el(e, t) {
  e.__highByOuter |= 1 << (t || 0), Ye(e, eg);
}
function rl(e, t) {
  !(e.__highByOuter &= ~(1 << (t || 0))) && Ye(e, rg);
}
function FS(e) {
  Ye(e, rf);
}
function ig(e) {
  Ye(e, ng);
}
function ag(e) {
  Ye(e, AS);
}
function og(e) {
  Ye(e, LS);
}
function sg(e, t) {
  return e.__highDownSilentOnTouch && t.zrByTouch;
}
function ug(e) {
  var t = e.getModel(), r = [], n = [];
  t.eachComponent(function(i, a) {
    var o = Jl(a), s = i === "series", u = s ? e.getViewOfSeriesModel(a) : e.getViewOfComponentModel(a);
    !s && n.push(u), o.isBlured && (u.group.traverse(function(l) {
      ng(l);
    }), s && r.push(a)), o.isBlured = !1;
  }), x(n, function(i) {
    i && i.toggleBlurSeries && i.toggleBlurSeries(r, !1, t);
  });
}
function nl(e, t, r, n) {
  var i = n.getModel();
  r = r || "coordinateSystem";
  function a(l, f) {
    for (var h = 0; h < f.length; h++) {
      var v = l.getItemGraphicEl(f[h]);
      v && ig(v);
    }
  }
  if (e != null && !(!t || t === "none")) {
    var o = i.getSeriesByIndex(e), s = o.coordinateSystem;
    s && s.master && (s = s.master);
    var u = [];
    i.eachSeries(function(l) {
      var f = o === l, h = l.coordinateSystem;
      h && h.master && (h = h.master);
      var v = h && s ? h === s : f;
      if (!// Not blur other series if blurScope series
      (r === "series" && !f || r === "coordinateSystem" && !v || t === "series" && f)) {
        var c = n.getViewOfSeriesModel(l);
        if (c.group.traverse(function(p) {
          p.__highByOuter && f && t === "self" || rf(p);
        }), Ot(t))
          a(l.getData(), t);
        else if (N(t))
          for (var d = ct(t), y = 0; y < d.length; y++)
            a(l.getData(d[y]), t[d[y]]);
        u.push(l), Jl(l).isBlured = !0;
      }
    }), i.eachComponent(function(l, f) {
      if (l !== "series") {
        var h = n.getViewOfComponentModel(f);
        h && h.toggleBlurSeries && h.toggleBlurSeries(u, !0, i);
      }
    });
  }
}
function il(e, t, r) {
  if (!(e == null || t == null)) {
    var n = r.getModel().getComponent(e, t);
    if (n) {
      Jl(n).isBlured = !0;
      var i = r.getViewOfComponentModel(n);
      !i || !i.focusBlurEnabled || i.group.traverse(function(a) {
        rf(a);
      });
    }
  }
}
function NS(e, t, r) {
  var n = e.seriesIndex, i = e.getData(t.dataType);
  if (i) {
    var a = Ki(i, t);
    a = (k(a) ? a[0] : a) || 0;
    var o = i.getItemGraphicEl(a);
    if (!o)
      for (var s = i.count(), u = 0; !o && u < s; )
        o = i.getItemGraphicEl(u++);
    if (o) {
      var l = at(o);
      nl(n, l.focus, l.blurScope, r);
    } else {
      var f = e.get(["emphasis", "focus"]), h = e.get(["emphasis", "blurScope"]);
      f != null && nl(n, f, h, r);
    }
  }
}
function nf(e, t, r, n) {
  var i = {
    focusSelf: !1,
    dispatchers: null
  };
  if (e == null || e === "series" || t == null || r == null)
    return i;
  var a = n.getModel().getComponent(e, t);
  if (!a)
    return i;
  var o = n.getViewOfComponentModel(a);
  if (!o || !o.findHighDownDispatchers)
    return i;
  for (var s = o.findHighDownDispatchers(r), u, l = 0; l < s.length; l++)
    if (at(s[l]).focus === "self") {
      u = !0;
      break;
    }
  return {
    focusSelf: u,
    dispatchers: s
  };
}
function zS(e, t, r) {
  var n = at(e), i = nf(n.componentMainType, n.componentIndex, n.componentHighDownName, r), a = i.dispatchers, o = i.focusSelf;
  a ? (o && il(n.componentMainType, n.componentIndex, r), x(a, function(s) {
    return ac(s, t);
  })) : (nl(n.seriesIndex, n.focus, n.blurScope, r), n.focus === "self" && il(n.componentMainType, n.componentIndex, r), ac(e, t));
}
function HS(e, t, r) {
  ug(r);
  var n = at(e), i = nf(n.componentMainType, n.componentIndex, n.componentHighDownName, r).dispatchers;
  i ? x(i, function(a) {
    return oc(a, t);
  }) : oc(e, t);
}
function GS(e, t, r) {
  if (ol(t)) {
    var n = t.dataType, i = e.getData(n), a = Ki(i, t);
    k(a) || (a = [a]), e[t.type === wi ? "toggleSelect" : t.type === Si ? "select" : "unselect"](a, n);
  }
}
function sc(e) {
  var t = e.getAllData();
  x(t, function(r) {
    var n = r.data, i = r.type;
    n.eachItemGraphicEl(function(a, o) {
      e.isSelected(o, i) ? ag(a) : og(a);
    });
  });
}
function VS(e) {
  var t = [];
  return e.eachSeries(function(r) {
    var n = r.getAllData();
    x(n, function(i) {
      i.data;
      var a = i.type, o = r.getSelectedDataIndices();
      if (o.length > 0) {
        var s = {
          dataIndex: o,
          seriesIndex: r.seriesIndex
        };
        a != null && (s.dataType = a), t.push(s);
      }
    });
  }), t;
}
function vo(e, t, r) {
  lg(e, !0), Ye(e, BS), US(e, t, r);
}
function WS(e) {
  lg(e, !1);
}
function JD(e, t, r, n) {
  n ? WS(e) : vo(e, t, r);
}
function US(e, t, r) {
  var n = at(e);
  t != null ? (n.focus = t, n.blurScope = r) : n.focus && (n.focus = null);
}
var uc = ["emphasis", "blur", "select"], YS = {
  itemStyle: "getItemStyle",
  lineStyle: "getLineStyle",
  areaStyle: "getAreaStyle"
};
function tP(e, t, r, n) {
  r = r || "itemStyle";
  for (var i = 0; i < uc.length; i++) {
    var a = uc[i], o = t.getModel([a, r]), s = e.ensureState(a);
    s.style = o[YS[r]]();
  }
}
function lg(e, t) {
  var r = t === !1, n = e;
  e.highDownSilentOnTouch && (n.__highDownSilentOnTouch = e.highDownSilentOnTouch), (!r || n.__highDownDispatcher) && (n.__highByOuter = n.__highByOuter || 0, n.__highDownDispatcher = !r);
}
function al(e) {
  return !!(e && e.__highDownDispatcher);
}
function qS(e) {
  var t = rc[e];
  return t == null && ec <= 32 && (t = rc[e] = ec++), t;
}
function ol(e) {
  var t = e.type;
  return t === Si || t === ja || t === wi;
}
function lc(e) {
  var t = e.type;
  return t === Xr || t === Za;
}
function XS(e) {
  var t = Jp(e);
  t.normalFill = e.style.fill, t.normalStroke = e.style.stroke;
  var r = e.states.select || {};
  t.selectFill = r.style && r.style.fill || null, t.selectStroke = r.style && r.style.stroke || null;
}
var fn = Ln.CMD, $S = [[], [], []], fc = Math.sqrt, ZS = Math.atan2;
function jS(e, t) {
  if (t) {
    var r = e.data, n = e.len(), i, a, o, s, u, l, f = fn.M, h = fn.C, v = fn.L, c = fn.R, d = fn.A, y = fn.Q;
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
          var p = t[4], g = t[5], _ = fc(t[0] * t[0] + t[1] * t[1]), m = fc(t[2] * t[2] + t[3] * t[3]), S = ZS(-t[1] / m, t[0] / _);
          r[o] *= _, r[o++] += p, r[o] *= m, r[o++] += g, r[o++] *= _, r[o++] *= m, r[o++] += S, r[o++] += S, o += 2, s = o;
          break;
        case c:
          l[0] = r[o++], l[1] = r[o++], Yt(l, l, t), r[s++] = l[0], r[s++] = l[1], l[0] += r[o++], l[1] += r[o++], Yt(l, l, t), r[s++] = l[0], r[s++] = l[1];
      }
      for (u = 0; u < a; u++) {
        var b = $S[u];
        b[0] = r[o++], b[1] = r[o++], Yt(b, b, t), r[s++] = b[0], r[s++] = b[1];
      }
    }
    e.increaseVersion();
  }
}
var Zs = Math.sqrt, ma = Math.sin, Sa = Math.cos, Kn = Math.PI;
function hc(e) {
  return Math.sqrt(e[0] * e[0] + e[1] * e[1]);
}
function sl(e, t) {
  return (e[0] * t[0] + e[1] * t[1]) / (hc(e) * hc(t));
}
function cc(e, t) {
  return (e[0] * t[1] < e[1] * t[0] ? -1 : 1) * Math.acos(sl(e, t));
}
function vc(e, t, r, n, i, a, o, s, u, l, f) {
  var h = u * (Kn / 180), v = Sa(h) * (e - r) / 2 + ma(h) * (t - n) / 2, c = -1 * ma(h) * (e - r) / 2 + Sa(h) * (t - n) / 2, d = v * v / (o * o) + c * c / (s * s);
  d > 1 && (o *= Zs(d), s *= Zs(d));
  var y = (i === a ? -1 : 1) * Zs((o * o * (s * s) - o * o * (c * c) - s * s * (v * v)) / (o * o * (c * c) + s * s * (v * v))) || 0, p = y * o * c / s, g = y * -s * v / o, _ = (e + r) / 2 + Sa(h) * p - ma(h) * g, m = (t + n) / 2 + ma(h) * p + Sa(h) * g, S = cc([1, 0], [(v - p) / o, (c - g) / s]), b = [(v - p) / o, (c - g) / s], w = [(-1 * v - p) / o, (-1 * c - g) / s], T = cc(b, w);
  if (sl(b, w) <= -1 && (T = Kn), sl(b, w) >= 1 && (T = 0), T < 0) {
    var C = Math.round(T / Kn * 1e6) / 1e6;
    T = Kn * 2 + C % 2 * Kn;
  }
  f.addData(l, _, m, o, s, S, T, h, a);
}
var KS = /([mlvhzcqtsa])([^mlvhzcqtsa]*)/ig, QS = /-?([0-9]*\.)?[0-9]+([eE]-?[0-9]+)?/g;
function JS(e) {
  var t = new Ln();
  if (!e)
    return t;
  var r = 0, n = 0, i = r, a = n, o, s = Ln.CMD, u = e.match(KS);
  if (!u)
    return t;
  for (var l = 0; l < u.length; l++) {
    for (var f = u[l], h = f.charAt(0), v = void 0, c = f.match(QS) || [], d = c.length, y = 0; y < d; y++)
      c[y] = parseFloat(c[y]);
    for (var p = 0; p < d; ) {
      var g = void 0, _ = void 0, m = void 0, S = void 0, b = void 0, w = void 0, T = void 0, C = r, M = n, D = void 0, P = void 0;
      switch (h) {
        case "l":
          r += c[p++], n += c[p++], v = s.L, t.addData(v, r, n);
          break;
        case "L":
          r = c[p++], n = c[p++], v = s.L, t.addData(v, r, n);
          break;
        case "m":
          r += c[p++], n += c[p++], v = s.M, t.addData(v, r, n), i = r, a = n, h = "l";
          break;
        case "M":
          r = c[p++], n = c[p++], v = s.M, t.addData(v, r, n), i = r, a = n, h = "L";
          break;
        case "h":
          r += c[p++], v = s.L, t.addData(v, r, n);
          break;
        case "H":
          r = c[p++], v = s.L, t.addData(v, r, n);
          break;
        case "v":
          n += c[p++], v = s.L, t.addData(v, r, n);
          break;
        case "V":
          n = c[p++], v = s.L, t.addData(v, r, n);
          break;
        case "C":
          v = s.C, t.addData(v, c[p++], c[p++], c[p++], c[p++], c[p++], c[p++]), r = c[p - 2], n = c[p - 1];
          break;
        case "c":
          v = s.C, t.addData(v, c[p++] + r, c[p++] + n, c[p++] + r, c[p++] + n, c[p++] + r, c[p++] + n), r += c[p - 2], n += c[p - 1];
          break;
        case "S":
          g = r, _ = n, D = t.len(), P = t.data, o === s.C && (g += r - P[D - 4], _ += n - P[D - 3]), v = s.C, C = c[p++], M = c[p++], r = c[p++], n = c[p++], t.addData(v, g, _, C, M, r, n);
          break;
        case "s":
          g = r, _ = n, D = t.len(), P = t.data, o === s.C && (g += r - P[D - 4], _ += n - P[D - 3]), v = s.C, C = r + c[p++], M = n + c[p++], r += c[p++], n += c[p++], t.addData(v, g, _, C, M, r, n);
          break;
        case "Q":
          C = c[p++], M = c[p++], r = c[p++], n = c[p++], v = s.Q, t.addData(v, C, M, r, n);
          break;
        case "q":
          C = c[p++] + r, M = c[p++] + n, r += c[p++], n += c[p++], v = s.Q, t.addData(v, C, M, r, n);
          break;
        case "T":
          g = r, _ = n, D = t.len(), P = t.data, o === s.Q && (g += r - P[D - 4], _ += n - P[D - 3]), r = c[p++], n = c[p++], v = s.Q, t.addData(v, g, _, r, n);
          break;
        case "t":
          g = r, _ = n, D = t.len(), P = t.data, o === s.Q && (g += r - P[D - 4], _ += n - P[D - 3]), r += c[p++], n += c[p++], v = s.Q, t.addData(v, g, _, r, n);
          break;
        case "A":
          m = c[p++], S = c[p++], b = c[p++], w = c[p++], T = c[p++], C = r, M = n, r = c[p++], n = c[p++], v = s.A, vc(C, M, r, n, w, T, m, S, b, v, t);
          break;
        case "a":
          m = c[p++], S = c[p++], b = c[p++], w = c[p++], T = c[p++], C = r, M = n, r += c[p++], n += c[p++], v = s.A, vc(C, M, r, n, w, T, m, S, b, v, t);
          break;
      }
    }
    (h === "z" || h === "Z") && (v = s.Z, t.addData(v), r = i, n = a), o = v;
  }
  return t.toStatic(), t;
}
var fg = (function(e) {
  tt(t, e);
  function t() {
    return e !== null && e.apply(this, arguments) || this;
  }
  return t.prototype.applyTransform = function(r) {
  }, t;
})(ht);
function hg(e) {
  return e.setData != null;
}
function cg(e, t) {
  var r = JS(e), n = E({}, t);
  return n.buildPath = function(i) {
    if (hg(i)) {
      i.setData(r.data);
      var a = i.getContext();
      a && i.rebuildPath(a, 1);
    } else {
      var a = i;
      r.rebuildPath(a, 1);
    }
  }, n.applyTransform = function(i) {
    jS(r, i), this.dirtyShape();
  }, n;
}
function tw(e, t) {
  return new fg(cg(e, t));
}
function ew(e, t) {
  var r = cg(e, t), n = (function(i) {
    tt(a, i);
    function a(o) {
      var s = i.call(this, o) || this;
      return s.applyTransform = r.applyTransform, s.buildPath = r.buildPath, s;
    }
    return a;
  })(fg);
  return n;
}
function rw(e, t) {
  for (var r = [], n = e.length, i = 0; i < n; i++) {
    var a = e[i];
    r.push(a.getUpdatedPathProxy(!0));
  }
  var o = new ht(t);
  return o.createPathProxy(), o.buildPath = function(s) {
    if (hg(s)) {
      s.appendPath(r);
      var u = s.getContext();
      u && s.rebuildPath(u, 1);
    }
  }, o;
}
var nw = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r = 0;
  }
  return e;
})(), ta = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new nw();
  }, t.prototype.buildPath = function(r, n) {
    r.moveTo(n.cx + n.r, n.cy), r.arc(n.cx, n.cy, n.r, 0, Math.PI * 2);
  }, t;
})(ht);
ta.prototype.type = "circle";
var iw = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.rx = 0, this.ry = 0;
  }
  return e;
})(), Vo = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new iw();
  }, t.prototype.buildPath = function(r, n) {
    var i = 0.5522848, a = n.cx, o = n.cy, s = n.rx, u = n.ry, l = s * i, f = u * i;
    r.moveTo(a - s, o), r.bezierCurveTo(a - s, o - f, a - l, o - u, a, o - u), r.bezierCurveTo(a + l, o - u, a + s, o - f, a + s, o), r.bezierCurveTo(a + s, o + f, a + l, o + u, a, o + u), r.bezierCurveTo(a - l, o + u, a - s, o + f, a - s, o), r.closePath();
  }, t;
})(ht);
Vo.prototype.type = "ellipse";
var vg = Math.PI, js = vg * 2, Lr = Math.sin, hn = Math.cos, aw = Math.acos, St = Math.atan2, dc = Math.abs, bi = Math.sqrt, vi = Math.max, we = Math.min, ce = 1e-4;
function ow(e, t, r, n, i, a, o, s) {
  var u = r - e, l = n - t, f = o - i, h = s - a, v = h * u - f * l;
  if (!(v * v < ce))
    return v = (f * (t - a) - h * (e - i)) / v, [e + v * u, t + v * l];
}
function wa(e, t, r, n, i, a, o) {
  var s = e - r, u = t - n, l = (o ? a : -a) / bi(s * s + u * u), f = l * u, h = -l * s, v = e + f, c = t + h, d = r + f, y = n + h, p = (v + d) / 2, g = (c + y) / 2, _ = d - v, m = y - c, S = _ * _ + m * m, b = i - a, w = v * y - d * c, T = (m < 0 ? -1 : 1) * bi(vi(0, b * b * S - w * w)), C = (w * m - _ * T) / S, M = (-w * _ - m * T) / S, D = (w * m + _ * T) / S, P = (-w * _ + m * T) / S, A = C - p, L = M - g, I = D - p, R = P - g;
  return A * A + L * L > I * I + R * R && (C = D, M = P), {
    cx: C,
    cy: M,
    x0: -f,
    y0: -h,
    x1: C * (i / b - 1),
    y1: M * (i / b - 1)
  };
}
function sw(e) {
  var t;
  if (k(e)) {
    var r = e.length;
    if (!r)
      return e;
    r === 1 ? t = [e[0], e[0], 0, 0] : r === 2 ? t = [e[0], e[0], e[1], e[1]] : r === 3 ? t = e.concat(e[2]) : t = e;
  } else
    t = [e, e, e, e];
  return t;
}
function uw(e, t) {
  var r, n = vi(t.r, 0), i = vi(t.r0 || 0, 0), a = n > 0, o = i > 0;
  if (!(!a && !o)) {
    if (a || (n = i, i = 0), i > n) {
      var s = n;
      n = i, i = s;
    }
    var u = t.startAngle, l = t.endAngle;
    if (!(isNaN(u) || isNaN(l))) {
      var f = t.cx, h = t.cy, v = !!t.clockwise, c = dc(l - u), d = c > js && c % js;
      if (d > ce && (c = d), !(n > ce))
        e.moveTo(f, h);
      else if (c > js - ce)
        e.moveTo(f + n * hn(u), h + n * Lr(u)), e.arc(f, h, n, u, l, !v), i > ce && (e.moveTo(f + i * hn(l), h + i * Lr(l)), e.arc(f, h, i, l, u, v));
      else {
        var y = void 0, p = void 0, g = void 0, _ = void 0, m = void 0, S = void 0, b = void 0, w = void 0, T = void 0, C = void 0, M = void 0, D = void 0, P = void 0, A = void 0, L = void 0, I = void 0, R = n * hn(u), O = n * Lr(u), q = i * hn(l), H = i * Lr(l), G = c > ce;
        if (G) {
          var X = t.cornerRadius;
          X && (r = sw(X), y = r[0], p = r[1], g = r[2], _ = r[3]);
          var nt = dc(n - i) / 2;
          if (m = we(nt, g), S = we(nt, _), b = we(nt, y), w = we(nt, p), M = T = vi(m, S), D = C = vi(b, w), (T > ce || C > ce) && (P = n * hn(l), A = n * Lr(l), L = i * hn(u), I = i * Lr(u), c < vg)) {
            var J = ow(R, O, L, I, P, A, q, H);
            if (J) {
              var mt = R - J[0], Zt = O - J[1], Xe = P - J[0], pr = A - J[1], gr = 1 / Lr(aw((mt * Xe + Zt * pr) / (bi(mt * mt + Zt * Zt) * bi(Xe * Xe + pr * pr))) / 2), rn = bi(J[0] * J[0] + J[1] * J[1]);
              M = we(T, (n - rn) / (gr + 1)), D = we(C, (i - rn) / (gr - 1));
            }
          }
        }
        if (!G)
          e.moveTo(f + R, h + O);
        else if (M > ce) {
          var Vt = we(g, M), yt = we(_, M), V = wa(L, I, R, O, n, Vt, v), Z = wa(P, A, q, H, n, yt, v);
          e.moveTo(f + V.cx + V.x0, h + V.cy + V.y0), M < T && Vt === yt ? e.arc(f + V.cx, h + V.cy, M, St(V.y0, V.x0), St(Z.y0, Z.x0), !v) : (Vt > 0 && e.arc(f + V.cx, h + V.cy, Vt, St(V.y0, V.x0), St(V.y1, V.x1), !v), e.arc(f, h, n, St(V.cy + V.y1, V.cx + V.x1), St(Z.cy + Z.y1, Z.cx + Z.x1), !v), yt > 0 && e.arc(f + Z.cx, h + Z.cy, yt, St(Z.y1, Z.x1), St(Z.y0, Z.x0), !v));
        } else
          e.moveTo(f + R, h + O), e.arc(f, h, n, u, l, !v);
        if (!(i > ce) || !G)
          e.lineTo(f + q, h + H);
        else if (D > ce) {
          var Vt = we(y, D), yt = we(p, D), V = wa(q, H, P, A, i, -yt, v), Z = wa(R, O, L, I, i, -Vt, v);
          e.lineTo(f + V.cx + V.x0, h + V.cy + V.y0), D < C && Vt === yt ? e.arc(f + V.cx, h + V.cy, D, St(V.y0, V.x0), St(Z.y0, Z.x0), !v) : (yt > 0 && e.arc(f + V.cx, h + V.cy, yt, St(V.y0, V.x0), St(V.y1, V.x1), !v), e.arc(f, h, i, St(V.cy + V.y1, V.cx + V.x1), St(Z.cy + Z.y1, Z.cx + Z.x1), v), Vt > 0 && e.arc(f + Z.cx, h + Z.cy, Vt, St(Z.y1, Z.x1), St(Z.y0, Z.x0), !v));
        } else
          e.lineTo(f + q, h + H), e.arc(f, h, i, l, u, v);
      }
      e.closePath();
    }
  }
}
var lw = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r0 = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0, this.cornerRadius = 0;
  }
  return e;
})(), Wo = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new lw();
  }, t.prototype.buildPath = function(r, n) {
    uw(r, n);
  }, t.prototype.isZeroArea = function() {
    return this.shape.startAngle === this.shape.endAngle || this.shape.r === this.shape.r0;
  }, t;
})(ht);
Wo.prototype.type = "sector";
var fw = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r = 0, this.r0 = 0;
  }
  return e;
})(), Uo = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new fw();
  }, t.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.PI * 2;
    r.moveTo(i + n.r, a), r.arc(i, a, n.r, 0, o, !1), r.moveTo(i + n.r0, a), r.arc(i, a, n.r0, 0, o, !0);
  }, t;
})(ht);
Uo.prototype.type = "ring";
function hw(e, t, r, n) {
  var i = [], a = [], o = [], s = [], u, l, f, h;
  if (n) {
    f = [1 / 0, 1 / 0], h = [-1 / 0, -1 / 0];
    for (var v = 0, c = e.length; v < c; v++)
      er(f, f, e[v]), rr(h, h, e[v]);
    er(f, f, n[0]), rr(h, h, n[1]);
  }
  for (var v = 0, c = e.length; v < c; v++) {
    var d = e[v];
    if (r)
      u = e[v ? v - 1 : c - 1], l = e[(v + 1) % c];
    else if (v === 0 || v === c - 1) {
      i.push(np(e[v]));
      continue;
    } else
      u = e[v - 1], l = e[v + 1];
    ip(a, l, u), Va(a, a, t);
    var y = ao(d, u), p = ao(d, l), g = y + p;
    g !== 0 && (y /= g, p /= g), Va(o, a, -y), Va(s, a, p);
    var _ = Eu([], d, o), m = Eu([], d, s);
    n && (rr(_, _, f), er(_, _, h), rr(m, m, f), er(m, m, h)), i.push(_), i.push(m);
  }
  return r && i.push(i.shift()), i;
}
function dg(e, t, r) {
  var n = t.smooth, i = t.points;
  if (i && i.length >= 2) {
    if (n) {
      var a = hw(i, n, r, t.smoothConstraint);
      e.moveTo(i[0][0], i[0][1]);
      for (var o = i.length, s = 0; s < (r ? o : o - 1); s++) {
        var u = a[s * 2], l = a[s * 2 + 1], f = i[(s + 1) % o];
        e.bezierCurveTo(u[0], u[1], l[0], l[1], f[0], f[1]);
      }
    } else {
      e.moveTo(i[0][0], i[0][1]);
      for (var s = 1, h = i.length; s < h; s++)
        e.lineTo(i[s][0], i[s][1]);
    }
    r && e.closePath();
  }
}
var cw = /* @__PURE__ */ (function() {
  function e() {
    this.points = null, this.smooth = 0, this.smoothConstraint = null;
  }
  return e;
})(), Yo = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new cw();
  }, t.prototype.buildPath = function(r, n) {
    dg(r, n, !0);
  }, t;
})(ht);
Yo.prototype.type = "polygon";
var vw = /* @__PURE__ */ (function() {
  function e() {
    this.points = null, this.percent = 1, this.smooth = 0, this.smoothConstraint = null;
  }
  return e;
})(), qo = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new vw();
  }, t.prototype.buildPath = function(r, n) {
    dg(r, n, !1);
  }, t;
})(ht);
qo.prototype.type = "polyline";
var dw = {}, pw = /* @__PURE__ */ (function() {
  function e() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.percent = 1;
  }
  return e;
})(), tn = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new pw();
  }, t.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = Kp(dw, n, this.style);
      i = u.x1, a = u.y1, o = u.x2, s = u.y2;
    } else
      i = n.x1, a = n.y1, o = n.x2, s = n.y2;
    var l = n.percent;
    l !== 0 && (r.moveTo(i, a), l < 1 && (o = i * (1 - l) + o * l, s = a * (1 - l) + s * l), r.lineTo(o, s));
  }, t.prototype.pointAt = function(r) {
    var n = this.shape;
    return [
      n.x1 * (1 - r) + n.x2 * r,
      n.y1 * (1 - r) + n.y2 * r
    ];
  }, t;
})(ht);
tn.prototype.type = "line";
var Ft = [], gw = /* @__PURE__ */ (function() {
  function e() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.cpx1 = 0, this.cpy1 = 0, this.percent = 1;
  }
  return e;
})();
function pc(e, t, r) {
  var n = e.cpx2, i = e.cpy2;
  return n != null || i != null ? [
    (r ? Sh : _t)(e.x1, e.cpx1, e.cpx2, e.x2, t),
    (r ? Sh : _t)(e.y1, e.cpy1, e.cpy2, e.y2, t)
  ] : [
    (r ? wh : Tt)(e.x1, e.cpx1, e.x2, t),
    (r ? wh : Tt)(e.y1, e.cpy1, e.y2, t)
  ];
}
var Xo = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new gw();
  }, t.prototype.buildPath = function(r, n) {
    var i = n.x1, a = n.y1, o = n.x2, s = n.y2, u = n.cpx1, l = n.cpy1, f = n.cpx2, h = n.cpy2, v = n.percent;
    v !== 0 && (r.moveTo(i, a), f == null || h == null ? (v < 1 && (fo(i, u, o, v, Ft), u = Ft[1], o = Ft[2], fo(a, l, s, v, Ft), l = Ft[1], s = Ft[2]), r.quadraticCurveTo(u, l, o, s)) : (v < 1 && (lo(i, u, f, o, v, Ft), u = Ft[1], f = Ft[2], o = Ft[3], lo(a, l, h, s, v, Ft), l = Ft[1], h = Ft[2], s = Ft[3]), r.bezierCurveTo(u, l, f, h, o, s)));
  }, t.prototype.pointAt = function(r) {
    return pc(this.shape, r, !1);
  }, t.prototype.tangentAt = function(r) {
    var n = pc(this.shape, r, !0);
    return ap(n, n);
  }, t;
})(ht);
Xo.prototype.type = "bezier-curve";
var yw = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0;
  }
  return e;
})(), ea = (function(e) {
  tt(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new yw();
  }, t.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.max(n.r, 0), s = n.startAngle, u = n.endAngle, l = n.clockwise, f = Math.cos(s), h = Math.sin(s);
    r.moveTo(f * o + i, h * o + a), r.arc(i, a, o, s, u, !l);
  }, t;
})(ht);
ea.prototype.type = "arc";
var pg = (function(e) {
  tt(t, e);
  function t() {
    var r = e !== null && e.apply(this, arguments) || this;
    return r.type = "compound", r;
  }
  return t.prototype._updatePathDirty = function() {
    for (var r = this.shape.paths, n = this.shapeChanged(), i = 0; i < r.length; i++)
      n = n || r[i].shapeChanged();
    n && this.dirtyShape();
  }, t.prototype.beforeBrush = function() {
    this._updatePathDirty();
    for (var r = this.shape.paths || [], n = this.getGlobalScale(), i = 0; i < r.length; i++)
      r[i].path || r[i].createPathProxy(), r[i].path.setScale(n[0], n[1], r[i].segmentIgnoreThreshold);
  }, t.prototype.buildPath = function(r, n) {
    for (var i = n.paths || [], a = 0; a < i.length; a++)
      i[a].buildPath(r, i[a].shape, !0);
  }, t.prototype.afterBrush = function() {
    for (var r = this.shape.paths || [], n = 0; n < r.length; n++)
      r[n].pathUpdated();
  }, t.prototype.getBoundingRect = function() {
    return this._updatePathDirty.call(this), ht.prototype.getBoundingRect.call(this);
  }, t;
})(ht), gg = (function() {
  function e(t) {
    this.colorStops = t || [];
  }
  return e.prototype.addColorStop = function(t, r) {
    this.colorStops.push({
      offset: t,
      color: r
    });
  }, e;
})(), yg = (function(e) {
  tt(t, e);
  function t(r, n, i, a, o, s) {
    var u = e.call(this, o) || this;
    return u.x = r ?? 0, u.y = n ?? 0, u.x2 = i ?? 1, u.y2 = a ?? 0, u.type = "linear", u.global = s || !1, u;
  }
  return t;
})(gg), _g = (function(e) {
  tt(t, e);
  function t(r, n, i, a, o) {
    var s = e.call(this, a) || this;
    return s.x = r ?? 0.5, s.y = n ?? 0.5, s.r = i ?? 0.5, s.type = "radial", s.global = o || !1, s;
  }
  return t;
})(gg), Rr = [0, 0], Er = [0, 0], ba = new et(), Ta = new et(), po = (function() {
  function e(t, r) {
    this._corners = [], this._axes = [], this._origin = [0, 0];
    for (var n = 0; n < 4; n++)
      this._corners[n] = new et();
    for (var n = 0; n < 2; n++)
      this._axes[n] = new et();
    t && this.fromBoundingRect(t, r);
  }
  return e.prototype.fromBoundingRect = function(t, r) {
    var n = this._corners, i = this._axes, a = t.x, o = t.y, s = a + t.width, u = o + t.height;
    if (n[0].set(a, o), n[1].set(s, o), n[2].set(s, u), n[3].set(a, u), r)
      for (var l = 0; l < 4; l++)
        n[l].transform(r);
    et.sub(i[0], n[1], n[0]), et.sub(i[1], n[3], n[0]), i[0].normalize(), i[1].normalize();
    for (var l = 0; l < 2; l++)
      this._origin[l] = i[l].dot(n[0]);
  }, e.prototype.intersect = function(t, r) {
    var n = !0, i = !r;
    return ba.set(1 / 0, 1 / 0), Ta.set(0, 0), !this._intersectCheckOneSide(this, t, ba, Ta, i, 1) && (n = !1, i) || !this._intersectCheckOneSide(t, this, ba, Ta, i, -1) && (n = !1, i) || i || et.copy(r, n ? ba : Ta), n;
  }, e.prototype._intersectCheckOneSide = function(t, r, n, i, a, o) {
    for (var s = !0, u = 0; u < 2; u++) {
      var l = this._axes[u];
      if (this._getProjMinMaxOnAxis(u, t._corners, Rr), this._getProjMinMaxOnAxis(u, r._corners, Er), Rr[1] < Er[0] || Rr[0] > Er[1]) {
        if (s = !1, a)
          return s;
        var f = Math.abs(Er[0] - Rr[1]), h = Math.abs(Rr[0] - Er[1]);
        Math.min(f, h) > i.len() && (f < h ? et.scale(i, l, -f * o) : et.scale(i, l, h * o));
      } else if (n) {
        var f = Math.abs(Er[0] - Rr[1]), h = Math.abs(Rr[0] - Er[1]);
        Math.min(f, h) < n.len() && (f < h ? et.scale(n, l, f * o) : et.scale(n, l, -h * o));
      }
    }
    return s;
  }, e.prototype._getProjMinMaxOnAxis = function(t, r, n) {
    for (var i = this._axes[t], a = this._origin, o = r[0].dot(i) + a[t], s = o, u = o, l = 1; l < r.length; l++) {
      var f = r[l].dot(i) + a[t];
      s = Math.min(f, s), u = Math.max(f, u);
    }
    n[0] = s, n[1] = u;
  }, e;
})(), _w = [], mg = (function(e) {
  tt(t, e);
  function t() {
    var r = e !== null && e.apply(this, arguments) || this;
    return r.notClear = !0, r.incremental = !0, r._displayables = [], r._temporaryDisplayables = [], r._cursor = 0, r;
  }
  return t.prototype.traverse = function(r, n) {
    r.call(n, this);
  }, t.prototype.useStyle = function() {
    this.style = {};
  }, t.prototype.getCursor = function() {
    return this._cursor;
  }, t.prototype.innerAfterBrush = function() {
    this._cursor = this._displayables.length;
  }, t.prototype.clearDisplaybles = function() {
    this._displayables = [], this._temporaryDisplayables = [], this._cursor = 0, this.markRedraw(), this.notClear = !1;
  }, t.prototype.clearTemporalDisplayables = function() {
    this._temporaryDisplayables = [];
  }, t.prototype.addDisplayable = function(r, n) {
    n ? this._temporaryDisplayables.push(r) : this._displayables.push(r), this.markRedraw();
  }, t.prototype.addDisplayables = function(r, n) {
    n = n || !1;
    for (var i = 0; i < r.length; i++)
      this.addDisplayable(r[i], n);
  }, t.prototype.getDisplayables = function() {
    return this._displayables;
  }, t.prototype.getTemporalDisplayables = function() {
    return this._temporaryDisplayables;
  }, t.prototype.eachPendingDisplayable = function(r) {
    for (var n = this._cursor; n < this._displayables.length; n++)
      r && r(this._displayables[n]);
    for (var n = 0; n < this._temporaryDisplayables.length; n++)
      r && r(this._temporaryDisplayables[n]);
  }, t.prototype.update = function() {
    this.updateTransform();
    for (var r = this._cursor; r < this._displayables.length; r++) {
      var n = this._displayables[r];
      n.parent = this, n.update(), n.parent = null;
    }
    for (var r = 0; r < this._temporaryDisplayables.length; r++) {
      var n = this._temporaryDisplayables[r];
      n.parent = this, n.update(), n.parent = null;
    }
  }, t.prototype.getBoundingRect = function() {
    if (!this._rect) {
      for (var r = new j(1 / 0, 1 / 0, -1 / 0, -1 / 0), n = 0; n < this._displayables.length; n++) {
        var i = this._displayables[n], a = i.getBoundingRect().clone();
        i.needLocalTransform() && a.applyTransform(i.getLocalTransform(_w)), r.union(a);
      }
      this._rect = r;
    }
    return this._rect;
  }, t.prototype.contain = function(r, n) {
    var i = this.transformCoordToLocal(r, n), a = this.getBoundingRect();
    if (a.contain(i[0], i[1]))
      for (var o = 0; o < this._displayables.length; o++) {
        var s = this._displayables[o];
        if (s.contain(r, n))
          return !0;
      }
    return !1;
  }, t;
})(Ji), mw = gt();
function Sw(e, t, r, n, i) {
  var a;
  if (t && t.ecModel) {
    var o = t.ecModel.getUpdatePayload();
    a = o && o.animation;
  }
  var s = t && t.isAnimationEnabled(), u = e === "update";
  if (s) {
    var l = void 0, f = void 0, h = void 0;
    n ? (l = Y(n.duration, 200), f = Y(n.easing, "cubicOut"), h = 0) : (l = t.getShallow(u ? "animationDurationUpdate" : "animationDuration"), f = t.getShallow(u ? "animationEasingUpdate" : "animationEasing"), h = t.getShallow(u ? "animationDelayUpdate" : "animationDelay")), a && (a.duration != null && (l = a.duration), a.easing != null && (f = a.easing), a.delay != null && (h = a.delay)), U(h) && (h = h(r, i)), U(l) && (l = l(r));
    var v = {
      duration: l || 0,
      delay: h,
      easing: f
    };
    return v;
  } else
    return null;
}
function af(e, t, r, n, i, a, o) {
  var s = !1, u;
  U(i) ? (o = a, a = i, i = null) : N(i) && (a = i.cb, o = i.during, s = i.isFrom, u = i.removeOpt, i = i.dataIndex);
  var l = e === "leave";
  l || t.stopAnimation("leave");
  var f = Sw(e, n, i, l ? u || {} : null, n && n.getAnimationDelayParams ? n.getAnimationDelayParams(t, i) : null);
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
      scope: e,
      during: o
    };
    s ? t.animateFrom(r, d) : t.animateTo(r, d);
  } else
    t.stopAnimation(), !s && t.attr(r), o && o(1), a && a();
}
function On(e, t, r, n, i, a) {
  af("update", e, t, r, n, i, a);
}
function of(e, t, r, n, i, a) {
  af("enter", e, t, r, n, i, a);
}
function Ti(e) {
  if (!e.__zr)
    return !0;
  for (var t = 0; t < e.animators.length; t++) {
    var r = e.animators[t];
    if (r.scope === "leave")
      return !0;
  }
  return !1;
}
function Sg(e, t, r, n, i, a) {
  Ti(e) || af("leave", e, t, r, n, i, a);
}
function gc(e, t, r, n) {
  e.removeTextContent(), e.removeTextGuideLine(), Sg(e, {
    style: {
      opacity: 0
    }
  }, t, r, n);
}
function ww(e, t, r) {
  function n() {
    e.parent && e.parent.remove(e);
  }
  e.isGroup ? e.traverse(function(i) {
    i.isGroup || gc(i, t, r, n);
  }) : gc(e, t, r, n);
}
function eP(e) {
  mw(e).oldStyle = e.style;
}
var go = Math.max, yo = Math.min, ul = {};
function wg(e) {
  return ht.extend(e);
}
var bw = ew;
function bg(e, t) {
  return bw(e, t);
}
function ue(e, t) {
  ul[e] = t;
}
function Tg(e) {
  if (ul.hasOwnProperty(e))
    return ul[e];
}
function $o(e, t, r, n) {
  var i = tw(e, t);
  return r && (n === "center" && (r = xg(r, i.getBoundingRect())), uf(i, r)), i;
}
function sf(e, t, r) {
  var n = new vr({
    style: {
      image: e,
      x: t.x,
      y: t.y,
      width: t.width,
      height: t.height
    },
    onload: function(i) {
      if (r === "center") {
        var a = {
          width: i.width,
          height: i.height
        };
        n.setStyle(xg(t, a));
      }
    }
  });
  return n;
}
function xg(e, t) {
  var r = t.width / t.height, n = e.height * r, i;
  n <= e.width ? i = e.height : (n = e.width, i = n / r);
  var a = e.x + e.width / 2, o = e.y + e.height / 2;
  return {
    x: a - n / 2,
    y: o - i / 2,
    width: n,
    height: i
  };
}
var Cg = rw;
function uf(e, t) {
  if (e.applyTransform) {
    var r = e.getBoundingRect(), n = r.calculateTransform(t);
    e.applyTransform(n);
  }
}
function lf(e, t) {
  return Kp(e, e, {
    lineWidth: t
  }), e;
}
function Tw(e) {
  return Qp(e.shape, e.shape, e.style), e;
}
var xw = Gr;
function Mg(e, t) {
  for (var r = $i([]); e && e !== t; )
    ur(r, e.getLocalTransform(), r), e = e.parent;
  return r;
}
function ff(e, t, r) {
  return t && !Ot(t) && (t = Yl.getLocalTransform(t)), r && (t = ko([], t)), Yt([], e, t);
}
function Cw(e, t, r) {
  var n = t[4] === 0 || t[5] === 0 || t[0] === 0 ? 1 : Math.abs(2 * t[4] / t[0]), i = t[4] === 0 || t[5] === 0 || t[2] === 0 ? 1 : Math.abs(2 * t[4] / t[2]), a = [e === "left" ? -n : e === "right" ? n : 0, e === "top" ? -i : e === "bottom" ? i : 0];
  return a = ff(a, t, r), Math.abs(a[0]) > Math.abs(a[1]) ? a[0] > 0 ? "right" : "left" : a[1] > 0 ? "bottom" : "top";
}
function yc(e) {
  return !e.isGroup;
}
function Mw(e) {
  return e.shape != null;
}
function Dw(e, t, r) {
  if (!e || !t)
    return;
  function n(o) {
    var s = {};
    return o.traverse(function(u) {
      yc(u) && u.anid && (s[u.anid] = u);
    }), s;
  }
  function i(o) {
    var s = {
      x: o.x,
      y: o.y,
      rotation: o.rotation
    };
    return Mw(o) && (s.shape = E({}, o.shape)), s;
  }
  var a = n(e);
  t.traverse(function(o) {
    if (yc(o) && o.anid) {
      var s = a[o.anid];
      if (s) {
        var u = i(o);
        o.attr(i(s)), On(o, u, r, at(o).dataIndex);
      }
    }
  });
}
function Dg(e, t) {
  return F(e, function(r) {
    var n = r[0];
    n = go(n, t.x), n = yo(n, t.x + t.width);
    var i = r[1];
    return i = go(i, t.y), i = yo(i, t.y + t.height), [n, i];
  });
}
function Pg(e, t) {
  var r = go(e.x, t.x), n = yo(e.x + e.width, t.x + t.width), i = go(e.y, t.y), a = yo(e.y + e.height, t.y + t.height);
  if (n >= r && a >= i)
    return {
      x: r,
      y: i,
      width: n - r,
      height: a - i
    };
}
function Zo(e, t, r) {
  var n = E({
    rectHover: !0
  }, t), i = n.style = {
    strokeNoScale: !0
  };
  if (r = r || {
    x: -1,
    y: -1,
    width: 2,
    height: 2
  }, e)
    return e.indexOf("image://") === 0 ? (i.image = e.slice(8), ft(i, r), new vr(n)) : $o(e.replace("path://", ""), n, r, "center");
}
function Pw(e, t, r, n, i) {
  for (var a = 0, o = i[i.length - 1]; a < i.length; a++) {
    var s = i[a];
    if (Ig(e, t, r, n, s[0], s[1], o[0], o[1]))
      return !0;
    o = s;
  }
}
function Ig(e, t, r, n, i, a, o, s) {
  var u = r - e, l = n - t, f = o - i, h = s - a, v = Ks(f, h, u, l);
  if (Iw(v))
    return !1;
  var c = e - i, d = t - a, y = Ks(c, d, u, l) / v;
  if (y < 0 || y > 1)
    return !1;
  var p = Ks(c, d, f, h) / v;
  return !(p < 0 || p > 1);
}
function Ks(e, t, r, n) {
  return e * n - r * t;
}
function Iw(e) {
  return e <= 1e-6 && e >= -1e-6;
}
function jo(e) {
  var t = e.itemTooltipOption, r = e.componentModel, n = e.itemName, i = B(t) ? {
    formatter: t
  } : t, a = r.mainType, o = r.componentIndex, s = {
    componentType: a,
    name: n,
    $vars: ["name"]
  };
  s[a + "Index"] = o;
  var u = e.formatterParamsExtra;
  u && x(ct(u), function(f) {
    We(s, f) || (s[f] = u[f], s.$vars.push(f));
  });
  var l = at(e.el);
  l.componentMainType = a, l.componentIndex = o, l.tooltipConfig = {
    name: n,
    option: ft({
      content: n,
      encodeHTMLContent: !0,
      formatterParams: s
    }, i)
  };
}
function _c(e, t) {
  var r;
  e.isGroup && (r = t(e)), r || e.traverse(t);
}
function Ag(e, t) {
  if (e)
    if (k(e))
      for (var r = 0; r < e.length; r++)
        _c(e[r], t);
    else
      _c(e, t);
}
ue("circle", ta);
ue("ellipse", Vo);
ue("sector", Wo);
ue("ring", Uo);
ue("polygon", Yo);
ue("polyline", qo);
ue("rect", Et);
ue("line", tn);
ue("bezierCurve", Xo);
ue("arc", ea);
const Aw = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: ea,
  BezierCurve: Xo,
  BoundingRect: j,
  Circle: ta,
  CompoundPath: pg,
  Ellipse: Vo,
  Group: oe,
  Image: vr,
  IncrementalDisplayable: mg,
  Line: tn,
  LinearGradient: yg,
  OrientedBoundingRect: po,
  Path: ht,
  Point: et,
  Polygon: Yo,
  Polyline: qo,
  RadialGradient: _g,
  Rect: Et,
  Ring: Uo,
  Sector: Wo,
  Text: Bt,
  applyTransform: ff,
  clipPointsByRect: Dg,
  clipRectByRect: Pg,
  createIcon: Zo,
  extendPath: bg,
  extendShape: wg,
  getShapeClass: Tg,
  getTransform: Mg,
  groupTransition: Dw,
  initProps: of,
  isElementRemoved: Ti,
  lineLineIntersect: Ig,
  linePolygonIntersect: Pw,
  makeImage: sf,
  makePath: $o,
  mergePath: Cg,
  registerShape: ue,
  removeElement: Sg,
  removeElementWithFadeOut: ww,
  resizePath: uf,
  setTooltipConfig: jo,
  subPixelOptimize: xw,
  subPixelOptimizeLine: lf,
  subPixelOptimizeRect: Tw,
  transformDirection: Cw,
  traverseElements: Ag,
  updateProps: On
}, Symbol.toStringTag, { value: "Module" }));
var Ko = {};
function Lg(e, t) {
  for (var r = 0; r < hr.length; r++) {
    var n = hr[r], i = t[n], a = e.ensureState(n);
    a.style = a.style || {}, a.style.text = i;
  }
  var o = e.currentStates.slice();
  e.clearStates(!0), e.setStyle({
    text: t.normal
  }), e.useStates(o, !0);
}
function ll(e, t, r) {
  var n = e.labelFetcher, i = e.labelDataIndex, a = e.labelDimIndex, o = t.normal, s;
  n && (s = n.getFormattedLabel(i, "normal", null, a, o && o.get("formatter"), r != null ? {
    interpolatedValue: r
  } : null)), s == null && (s = U(e.defaultText) ? e.defaultText(i, e, r) : e.defaultText);
  for (var u = {
    normal: s
  }, l = 0; l < hr.length; l++) {
    var f = hr[l], h = t[f];
    u[f] = Y(n ? n.getFormattedLabel(i, f, null, a, h && h.get("formatter")) : null, s);
  }
  return u;
}
function Lw(e, t, r, n) {
  r = r || Ko;
  for (var i = e instanceof Bt, a = !1, o = 0; o < nc.length; o++) {
    var s = t[nc[o]];
    if (s && s.getShallow("show")) {
      a = !0;
      break;
    }
  }
  var u = i ? e : e.getTextContent();
  if (a) {
    i || (u || (u = new Bt(), e.setTextContent(u)), e.stateProxy && (u.stateProxy = e.stateProxy));
    var l = ll(r, t), f = t.normal, h = !!f.getShallow("show"), v = Qr(f, n, r, !1, !i);
    v.text = l.normal, i || e.setTextConfig(mc(f, r, !1));
    for (var o = 0; o < hr.length; o++) {
      var c = hr[o], s = t[c];
      if (s) {
        var d = u.ensureState(c), y = !!Y(s.getShallow("show"), h);
        if (y !== h && (d.ignore = !y), d.style = Qr(s, n, r, !0, !i), d.style.text = l[c], !i) {
          var p = e.ensureState(c);
          p.textConfig = mc(s, r, !0);
        }
      }
    }
    u.silent = !!f.getShallow("silent"), u.style.x != null && (v.x = u.style.x), u.style.y != null && (v.y = u.style.y), u.ignore = !h, u.useStyle(v), u.dirty(), r.enableTextSetter && (hf(u).setLabelText = function(g) {
      var _ = ll(r, t, g);
      Lg(u, _);
    });
  } else u && (u.ignore = !0);
  e.dirty();
}
function rP(e, t) {
  t = t || "label";
  for (var r = {
    normal: e.getModel(t)
  }, n = 0; n < hr.length; n++) {
    var i = hr[n];
    r[i] = e.getModel([i, t]);
  }
  return r;
}
function Qr(e, t, r, n, i) {
  var a = {};
  return Rw(a, e, r, n, i), t && E(a, t), a;
}
function mc(e, t, r) {
  t = t || {};
  var n = {}, i, a = e.getShallow("rotate"), o = Y(e.getShallow("distance"), r ? null : 5), s = e.getShallow("offset");
  return i = e.getShallow("position") || (r ? null : "inside"), i === "outside" && (i = t.defaultOutsidePosition || "top"), i != null && (n.position = i), s != null && (n.offset = s), a != null && (a *= Math.PI / 180, n.rotation = a), o != null && (n.distance = o), n.outsideFill = e.get("color") === "inherit" ? t.inheritColor || null : "auto", n;
}
function Rw(e, t, r, n, i) {
  r = r || Ko;
  var a = t.ecModel, o = a && a.option.textStyle, s = Ew(t), u;
  if (s) {
    u = {};
    for (var l in s)
      if (s.hasOwnProperty(l)) {
        var f = t.getModel(["rich", l]);
        Tc(u[l] = {}, f, o, r, n, i, !1, !0);
      }
  }
  u && (e.rich = u);
  var h = t.get("overflow");
  h && (e.overflow = h);
  var v = t.get("minMargin");
  v != null && (e.margin = v), Tc(e, t, o, r, n, i, !0, !1);
}
function Ew(e) {
  for (var t; e && e !== e.ecModel; ) {
    var r = (e.option || Ko).rich;
    if (r) {
      t = t || {};
      for (var n = ct(r), i = 0; i < n.length; i++) {
        var a = n[i];
        t[a] = 1;
      }
    }
    e = e.parentModel;
  }
  return t;
}
var Sc = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY"], wc = ["align", "lineHeight", "width", "height", "tag", "verticalAlign", "ellipsis"], bc = ["padding", "borderWidth", "borderRadius", "borderDashOffset", "backgroundColor", "borderColor", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"];
function Tc(e, t, r, n, i, a, o, s) {
  r = !i && r || Ko;
  var u = n && n.inheritColor, l = t.getShallow("color"), f = t.getShallow("textBorderColor"), h = Y(t.getShallow("opacity"), r.opacity);
  (l === "inherit" || l === "auto") && (u ? l = u : l = null), (f === "inherit" || f === "auto") && (u ? f = u : f = null), a || (l = l || r.color, f = f || r.textBorderColor), l != null && (e.fill = l), f != null && (e.stroke = f);
  var v = Y(t.getShallow("textBorderWidth"), r.textBorderWidth);
  v != null && (e.lineWidth = v);
  var c = Y(t.getShallow("textBorderType"), r.textBorderType);
  c != null && (e.lineDash = c);
  var d = Y(t.getShallow("textBorderDashOffset"), r.textBorderDashOffset);
  d != null && (e.lineDashOffset = d), !i && h == null && !s && (h = n && n.defaultOpacity), h != null && (e.opacity = h), !i && !a && e.fill == null && n.inheritColor && (e.fill = n.inheritColor);
  for (var y = 0; y < Sc.length; y++) {
    var p = Sc[y], g = Y(t.getShallow(p), r[p]);
    g != null && (e[p] = g);
  }
  for (var y = 0; y < wc.length; y++) {
    var p = wc[y], g = t.getShallow(p);
    g != null && (e[p] = g);
  }
  if (e.verticalAlign == null) {
    var _ = t.getShallow("baseline");
    _ != null && (e.verticalAlign = _);
  }
  if (!o || !n.disableBox) {
    for (var y = 0; y < bc.length; y++) {
      var p = bc[y], g = t.getShallow(p);
      g != null && (e[p] = g);
    }
    var m = t.getShallow("borderType");
    m != null && (e.borderDash = m), (e.backgroundColor === "auto" || e.backgroundColor === "inherit") && u && (e.backgroundColor = u), (e.borderColor === "auto" || e.borderColor === "inherit") && u && (e.borderColor = u);
  }
}
function Ow(e, t) {
  var r = t && t.getModel("textStyle");
  return pe([
    // FIXME in node-canvas fontWeight is before fontStyle
    e.fontStyle || r && r.getShallow("fontStyle") || "",
    e.fontWeight || r && r.getShallow("fontWeight") || "",
    (e.fontSize || r && r.getShallow("fontSize") || 12) + "px",
    e.fontFamily || r && r.getShallow("fontFamily") || "sans-serif"
  ].join(" "));
}
var hf = gt();
function nP(e, t, r, n) {
  if (e) {
    var i = hf(e);
    i.prevValue = i.value, i.value = r;
    var a = t.normal;
    i.valueAnimation = a.get("valueAnimation"), i.valueAnimation && (i.precision = a.get("precision"), i.defaultInterpolatedText = n, i.statesModels = t);
  }
}
function iP(e, t, r, n, i) {
  var a = hf(e);
  if (!a.valueAnimation || a.prevValue === a.value)
    return;
  var o = a.defaultInterpolatedText, s = Y(a.interpolatedValue, a.prevValue), u = a.value;
  function l(f) {
    var h = P1(r, a.precision, s, u, f);
    a.interpolatedValue = f === 1 ? null : h;
    var v = ll({
      labelDataIndex: t,
      labelFetcher: i,
      defaultText: o ? o(h) : h + ""
    }, a.statesModels, h);
    Lg(e, v);
  }
  e.percent = 0, (a.prevValue == null ? of : On)(e, {
    // percent is used to prevent animation from being aborted #15916
    percent: 1
  }, n, t, null, l);
}
var kw = ["textStyle", "color"], Qs = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "padding", "lineHeight", "rich", "width", "height", "overflow"], Js = new Bt(), Bw = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getTextColor = function(t) {
      var r = this.ecModel;
      return this.getShallow("color") || (!t && r ? r.get(kw) : null);
    }, e.prototype.getFont = function() {
      return Ow({
        fontStyle: this.getShallow("fontStyle"),
        fontWeight: this.getShallow("fontWeight"),
        fontSize: this.getShallow("fontSize"),
        fontFamily: this.getShallow("fontFamily")
      }, this.ecModel);
    }, e.prototype.getTextRect = function(t) {
      for (var r = {
        text: t,
        verticalAlign: this.getShallow("verticalAlign") || this.getShallow("baseline")
      }, n = 0; n < Qs.length; n++)
        r[Qs[n]] = this.getShallow(Qs[n]);
      return Js.useStyle(r), Js.update(), Js.getBoundingRect();
    }, e;
  })()
), Rg = [
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
], Fw = Bi(Rg), Nw = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getLineStyle = function(t) {
      return Fw(this, t);
    }, e;
  })()
), Eg = [
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
], zw = Bi(Eg), Hw = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getItemStyle = function(t, r) {
      return zw(this, t, r);
    }, e;
  })()
), st = (
  /** @class */
  (function() {
    function e(t, r, n) {
      this.parentModel = r, this.ecModel = n, this.option = t;
    }
    return e.prototype.init = function(t, r, n) {
    }, e.prototype.mergeOption = function(t, r) {
      it(this.option, t, !0);
    }, e.prototype.get = function(t, r) {
      return t == null ? this.option : this._doGet(this.parsePath(t), !r && this.parentModel);
    }, e.prototype.getShallow = function(t, r) {
      var n = this.option, i = n == null ? n : n[t];
      if (i == null && !r) {
        var a = this.parentModel;
        a && (i = a.getShallow(t));
      }
      return i;
    }, e.prototype.getModel = function(t, r) {
      var n = t != null, i = n ? this.parsePath(t) : null, a = n ? this._doGet(i) : this.option;
      return r = r || this.parentModel && this.parentModel.getModel(this.resolveParentPath(i)), new e(a, r, this.ecModel);
    }, e.prototype.isEmpty = function() {
      return this.option == null;
    }, e.prototype.restoreData = function() {
    }, e.prototype.clone = function() {
      var t = this.constructor;
      return new t($(this.option));
    }, e.prototype.parsePath = function(t) {
      return typeof t == "string" ? t.split(".") : t;
    }, e.prototype.resolveParentPath = function(t) {
      return t;
    }, e.prototype.isAnimationEnabled = function() {
      if (!z.node && this.option) {
        if (this.option.animation != null)
          return !!this.option.animation;
        if (this.parentModel)
          return this.parentModel.isAnimationEnabled();
      }
    }, e.prototype._doGet = function(t, r) {
      var n = this.option;
      if (!t)
        return n;
      for (var i = 0; i < t.length && !(t[i] && (n = n && typeof n == "object" ? n[t[i]] : null, n == null)); i++)
        ;
      return n == null && r && (n = r._doGet(this.resolveParentPath(t), r.parentModel)), n;
    }, e;
  })()
);
Ql(st);
O1(st);
ye(st, Nw);
ye(st, Hw);
ye(st, z1);
ye(st, Bw);
var Gw = Math.round(Math.random() * 10);
function Qo(e) {
  return [e || "", Gw++].join("_");
}
function Vw(e) {
  var t = {};
  e.registerSubTypeDefaulter = function(r, n) {
    var i = xe(r);
    t[i.main] = n;
  }, e.determineSubType = function(r, n) {
    var i = n.type;
    if (!i) {
      var a = xe(r).main;
      e.hasSubTypes(r) && t[a] && (i = t[a](n));
    }
    return i;
  };
}
function Ww(e, t) {
  e.topologicalTravel = function(a, o, s, u) {
    if (!a.length)
      return;
    var l = r(o), f = l.graph, h = l.noEntryList, v = {};
    for (x(a, function(_) {
      v[_] = !0;
    }); h.length; ) {
      var c = h.pop(), d = f[c], y = !!v[c];
      y && (s.call(u, c, d.originalDeps.slice()), delete v[c]), x(d.successor, y ? g : p);
    }
    x(v, function() {
      var _ = "";
      throw new Error(_);
    });
    function p(_) {
      f[_].entryCount--, f[_].entryCount === 0 && h.push(_);
    }
    function g(_) {
      v[_] = !0, p(_);
    }
  };
  function r(a) {
    var o = {}, s = [];
    return x(a, function(u) {
      var l = n(o, u), f = l.originalDeps = t(u), h = i(f, a);
      l.entryCount = h.length, l.entryCount === 0 && s.push(u), x(h, function(v) {
        rt(l.predecessor, v) < 0 && l.predecessor.push(v);
        var c = n(o, v);
        rt(c.successor, v) < 0 && c.successor.push(u);
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
    return x(a, function(u) {
      rt(o, u) >= 0 && s.push(u);
    }), s;
  }
}
function Uw(e, t) {
  return it(it({}, e, !0), t, !0);
}
const Yw = {
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
}, qw = {
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
var _o = "ZH", cf = "EN", Cn = cf, Ka = {}, vf = {}, Og = z.domSupported ? (function() {
  var e = (
    /* eslint-disable-next-line */
    (document.documentElement.lang || navigator.language || navigator.browserLanguage || Cn).toUpperCase()
  );
  return e.indexOf(_o) > -1 ? _o : Cn;
})() : Cn;
function df(e, t) {
  e = e.toUpperCase(), vf[e] = new st(t), Ka[e] = t;
}
function Xw(e) {
  if (B(e)) {
    var t = Ka[e.toUpperCase()] || {};
    return e === _o || e === cf ? $(t) : it($(t), $(Ka[Cn]), !1);
  } else
    return it($(e), $(Ka[Cn]), !1);
}
function $w(e) {
  return vf[e];
}
function Zw() {
  return vf[Cn];
}
df(cf, Yw);
df(_o, qw);
var pf = 1e3, gf = pf * 60, xi = gf * 60, ae = xi * 24, xc = ae * 365, di = {
  year: "{yyyy}",
  month: "{MMM}",
  day: "{d}",
  hour: "{HH}:{mm}",
  minute: "{HH}:{mm}",
  second: "{HH}:{mm}:{ss}",
  millisecond: "{HH}:{mm}:{ss} {SSS}",
  none: "{yyyy}-{MM}-{dd} {HH}:{mm}:{ss} {SSS}"
}, xa = "{yyyy}-{MM}-{dd}", Cc = {
  year: "{yyyy}",
  month: "{yyyy}-{MM}",
  day: xa,
  hour: xa + " " + di.hour,
  minute: xa + " " + di.minute,
  second: xa + " " + di.second,
  millisecond: di.none
}, tu = ["year", "month", "day", "hour", "minute", "second", "millisecond"], kg = ["year", "half-year", "quarter", "month", "week", "half-week", "day", "half-day", "quarter-day", "hour", "minute", "second", "millisecond"];
function Dt(e, t) {
  return e += "", "0000".substr(0, t - e.length) + e;
}
function Mn(e) {
  switch (e) {
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
      return e;
  }
}
function jw(e) {
  return e === Mn(e);
}
function Kw(e) {
  switch (e) {
    case "year":
    case "month":
      return "day";
    case "millisecond":
      return "millisecond";
    default:
      return "second";
  }
}
function ra(e, t, r, n) {
  var i = se(e), a = i[yf(r)](), o = i[Dn(r)]() + 1, s = Math.floor((o - 1) / 3) + 1, u = i[Jo(r)](), l = i["get" + (r ? "UTC" : "") + "Day"](), f = i[Fi(r)](), h = (f - 1) % 12 + 1, v = i[ts(r)](), c = i[es(r)](), d = i[rs(r)](), y = f >= 12 ? "pm" : "am", p = y.toUpperCase(), g = n instanceof st ? n : $w(n || Og) || Zw(), _ = g.getModel("time"), m = _.get("month"), S = _.get("monthAbbr"), b = _.get("dayOfWeek"), w = _.get("dayOfWeekAbbr");
  return (t || "").replace(/{a}/g, y + "").replace(/{A}/g, p + "").replace(/{yyyy}/g, a + "").replace(/{yy}/g, Dt(a % 100 + "", 2)).replace(/{Q}/g, s + "").replace(/{MMMM}/g, m[o - 1]).replace(/{MMM}/g, S[o - 1]).replace(/{MM}/g, Dt(o, 2)).replace(/{M}/g, o + "").replace(/{dd}/g, Dt(u, 2)).replace(/{d}/g, u + "").replace(/{eeee}/g, b[l]).replace(/{ee}/g, w[l]).replace(/{e}/g, l + "").replace(/{HH}/g, Dt(f, 2)).replace(/{H}/g, f + "").replace(/{hh}/g, Dt(h + "", 2)).replace(/{h}/g, h + "").replace(/{mm}/g, Dt(v, 2)).replace(/{m}/g, v + "").replace(/{ss}/g, Dt(c, 2)).replace(/{s}/g, c + "").replace(/{SSS}/g, Dt(d, 3)).replace(/{S}/g, d + "");
}
function Qw(e, t, r, n, i) {
  var a = null;
  if (B(r))
    a = r;
  else if (U(r))
    a = r(e.value, t, {
      level: e.level
    });
  else {
    var o = E({}, di);
    if (e.level > 0)
      for (var s = 0; s < tu.length; ++s)
        o[tu[s]] = "{primary|" + o[tu[s]] + "}";
    var u = r ? r.inherit === !1 ? r : ft(r, o) : o, l = Bg(e.value, i);
    if (u[l])
      a = u[l];
    else if (u.inherit) {
      for (var f = kg.indexOf(l), s = f - 1; s >= 0; --s)
        if (u[l]) {
          a = u[l];
          break;
        }
      a = a || o.none;
    }
    if (k(a)) {
      var h = e.level == null ? 0 : e.level >= 0 ? e.level : a.length + e.level;
      h = Math.min(h, a.length - 1), a = a[h];
    }
  }
  return ra(new Date(e.value), a, i, n);
}
function Bg(e, t) {
  var r = se(e), n = r[Dn(t)]() + 1, i = r[Jo(t)](), a = r[Fi(t)](), o = r[ts(t)](), s = r[es(t)](), u = r[rs(t)](), l = u === 0, f = l && s === 0, h = f && o === 0, v = h && a === 0, c = v && i === 1, d = c && n === 1;
  return d ? "year" : c ? "month" : v ? "day" : h ? "hour" : f ? "minute" : l ? "second" : "millisecond";
}
function Mc(e, t, r) {
  var n = ut(e) ? se(e) : e;
  switch (t = t || Bg(e, r), t) {
    case "year":
      return n[yf(r)]();
    case "half-year":
      return n[Dn(r)]() >= 6 ? 1 : 0;
    case "quarter":
      return Math.floor((n[Dn(r)]() + 1) / 4);
    case "month":
      return n[Dn(r)]();
    case "day":
      return n[Jo(r)]();
    case "half-day":
      return n[Fi(r)]() / 24;
    case "hour":
      return n[Fi(r)]();
    case "minute":
      return n[ts(r)]();
    case "second":
      return n[es(r)]();
    case "millisecond":
      return n[rs(r)]();
  }
}
function yf(e) {
  return e ? "getUTCFullYear" : "getFullYear";
}
function Dn(e) {
  return e ? "getUTCMonth" : "getMonth";
}
function Jo(e) {
  return e ? "getUTCDate" : "getDate";
}
function Fi(e) {
  return e ? "getUTCHours" : "getHours";
}
function ts(e) {
  return e ? "getUTCMinutes" : "getMinutes";
}
function es(e) {
  return e ? "getUTCSeconds" : "getSeconds";
}
function rs(e) {
  return e ? "getUTCMilliseconds" : "getMilliseconds";
}
function Jw(e) {
  return e ? "setUTCFullYear" : "setFullYear";
}
function Fg(e) {
  return e ? "setUTCMonth" : "setMonth";
}
function Ng(e) {
  return e ? "setUTCDate" : "setDate";
}
function zg(e) {
  return e ? "setUTCHours" : "setHours";
}
function Hg(e) {
  return e ? "setUTCMinutes" : "setMinutes";
}
function Gg(e) {
  return e ? "setUTCSeconds" : "setSeconds";
}
function Vg(e) {
  return e ? "setUTCMilliseconds" : "setMilliseconds";
}
function tb(e, t, r, n, i, a, o, s) {
  var u = new Bt({
    style: {
      text: e,
      font: t,
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
function _f(e) {
  if (!Bp(e))
    return B(e) ? e : "-";
  var t = (e + "").split(".");
  return t[0].replace(/(\d{1,3})(?=(?:\d{3})+(?!\d))/g, "$1,") + (t.length > 1 ? "." + t[1] : "");
}
function mf(e, t) {
  return e = (e || "").toLowerCase().replace(/-(.)/g, function(r, n) {
    return n.toUpperCase();
  }), t && e && (e = e.charAt(0).toUpperCase() + e.slice(1)), e;
}
var na = Nl;
function fl(e, t, r) {
  var n = "{yyyy}-{MM}-{dd} {HH}:{mm}:{ss}";
  function i(f) {
    return f && pe(f) ? f : "-";
  }
  function a(f) {
    return !!(f != null && !isNaN(f) && isFinite(f));
  }
  var o = t === "time", s = e instanceof Date;
  if (o || s) {
    var u = o ? se(e) : e;
    if (isNaN(+u)) {
      if (s)
        return "-";
    } else return ra(u, n, r);
  }
  if (t === "ordinal")
    return io(e) ? i(e) : ut(e) && a(e) ? e + "" : "-";
  var l = Ue(e);
  return a(l) ? _f(l) : io(e) ? i(e) : typeof e == "boolean" ? e + "" : "-";
}
var Dc = ["a", "b", "c", "d", "e", "f", "g"], eu = function(e, t) {
  return "{" + e + (t ?? "") + "}";
};
function Sf(e, t, r) {
  k(t) || (t = [t]);
  var n = t.length;
  if (!n)
    return "";
  for (var i = t[0].$vars || [], a = 0; a < i.length; a++) {
    var o = Dc[a];
    e = e.replace(eu(o), eu(o, 0));
  }
  for (var s = 0; s < n; s++)
    for (var u = 0; u < i.length; u++) {
      var l = t[s][i[u]];
      e = e.replace(eu(Dc[u], s), r ? Pt(l) : l);
    }
  return e;
}
function Wg(e, t) {
  var r = B(e) ? {
    color: e,
    extraCssText: t
  } : e || {}, n = r.color, i = r.type;
  t = r.extraCssText;
  var a = r.renderMode || "html";
  if (!n)
    return "";
  if (a === "html")
    return i === "subItem" ? '<span style="display:inline-block;vertical-align:middle;margin-right:8px;margin-left:3px;border-radius:4px;width:4px;height:4px;background-color:' + Pt(n) + ";" + (t || "") + '"></span>' : '<span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background-color:' + Pt(n) + ";" + (t || "") + '"></span>';
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
function eb(e, t, r) {
  (e === "week" || e === "month" || e === "quarter" || e === "half-year" || e === "year") && (e = `MM-dd
yyyy`);
  var n = se(t), i = r ? "getUTC" : "get", a = n[i + "FullYear"](), o = n[i + "Month"]() + 1, s = n[i + "Date"](), u = n[i + "Hours"](), l = n[i + "Minutes"](), f = n[i + "Seconds"](), h = n[i + "Milliseconds"]();
  return e = e.replace("MM", Dt(o, 2)).replace("M", o).replace("yyyy", a).replace("yy", Dt(a % 100 + "", 2)).replace("dd", Dt(s, 2)).replace("d", s).replace("hh", Dt(u, 2)).replace("h", u).replace("mm", Dt(l, 2)).replace("m", l).replace("ss", Dt(f, 2)).replace("s", f).replace("SSS", Dt(h, 3)), e;
}
function rb(e) {
  return e && e.charAt(0).toUpperCase() + e.substr(1);
}
function Ni(e, t) {
  return t = t || "transparent", B(e) ? e : N(e) && e.colorStops && (e.colorStops[0] || {}).color || t;
}
var Qa = x, nb = ["left", "right", "top", "bottom", "width", "height"], Ca = [["width", "left", "right"], ["height", "top", "bottom"]];
function wf(e, t, r, n, i) {
  var a = 0, o = 0;
  n == null && (n = 1 / 0), i == null && (i = 1 / 0);
  var s = 0;
  t.eachChild(function(u, l) {
    var f = u.getBoundingRect(), h = t.childAt(l + 1), v = h && h.getBoundingRect(), c, d;
    if (e === "horizontal") {
      var y = f.width + (v ? -v.x + f.x : 0);
      c = a + y, c > n || u.newline ? (a = 0, c = y, o += s + r, s = f.height) : s = Math.max(s, f.height);
    } else {
      var p = f.height + (v ? -v.y + f.y : 0);
      d = o + p, d > i || u.newline ? (a += s + r, o = 0, d = p, s = f.width) : s = Math.max(s, f.width);
    }
    u.newline || (u.x = a, u.y = o, u.markRedraw(), e === "horizontal" ? a = c + r : o = d + r);
  });
}
var Ci = wf;
pt(wf, "vertical");
pt(wf, "horizontal");
function mo(e, t, r) {
  r = na(r || 0);
  var n = t.width, i = t.height, a = It(e.left, n), o = It(e.top, i), s = It(e.right, n), u = It(e.bottom, i), l = It(e.width, n), f = It(e.height, i), h = r[2] + r[0], v = r[1] + r[3], c = e.aspect;
  switch (isNaN(l) && (l = n - s - v - a), isNaN(f) && (f = i - u - h - o), c != null && (isNaN(l) && isNaN(f) && (c > n / i ? l = n * 0.8 : f = i * 0.8), isNaN(l) && (l = c * f), isNaN(f) && (f = l / c)), isNaN(a) && (a = n - s - l - v), isNaN(o) && (o = i - u - f - h), e.left || e.right) {
    case "center":
      a = n / 2 - l / 2 - r[3];
      break;
    case "right":
      a = n - l - v;
      break;
  }
  switch (e.top || e.bottom) {
    case "middle":
    case "center":
      o = i / 2 - f / 2 - r[0];
      break;
    case "bottom":
      o = i - f - h;
      break;
  }
  a = a || 0, o = o || 0, isNaN(l) && (l = n - v - a - (s || 0)), isNaN(f) && (f = i - h - o - (u || 0));
  var d = new j(a + r[3], o + r[0], l, f);
  return d.margin = r, d;
}
function So(e) {
  var t = e.layoutMode || e.constructor.layoutMode;
  return N(t) ? t : t ? {
    type: t
  } : null;
}
function zi(e, t, r) {
  var n = r && r.ignoreSize;
  !k(n) && (n = [n, n]);
  var i = o(Ca[0], 0), a = o(Ca[1], 1);
  l(Ca[0], e, i), l(Ca[1], e, a);
  function o(f, h) {
    var v = {}, c = 0, d = {}, y = 0, p = 2;
    if (Qa(f, function(m) {
      d[m] = e[m];
    }), Qa(f, function(m) {
      s(t, m) && (v[m] = d[m] = t[m]), u(v, m) && c++, u(d, m) && y++;
    }), n[h])
      return u(t, f[1]) ? d[f[2]] = null : u(t, f[2]) && (d[f[1]] = null), d;
    if (y === p || !c)
      return d;
    if (c >= p)
      return v;
    for (var g = 0; g < f.length; g++) {
      var _ = f[g];
      if (!s(v, _) && s(e, _)) {
        v[_] = e[_];
        break;
      }
    }
    return v;
  }
  function s(f, h) {
    return f.hasOwnProperty(h);
  }
  function u(f, h) {
    return f[h] != null && f[h] !== "auto";
  }
  function l(f, h, v) {
    Qa(f, function(c) {
      h[c] = v[c];
    });
  }
}
function bf(e) {
  return ib({}, e);
}
function ib(e, t) {
  return t && e && Qa(nb, function(r) {
    t.hasOwnProperty(r) && (e[r] = t[r]);
  }), e;
}
var ab = gt(), Q = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t(r, n, i) {
      var a = e.call(this, r, n, i) || this;
      return a.uid = Qo("ec_cpt_model"), a;
    }
    return t.prototype.init = function(r, n, i) {
      this.mergeDefaultAndTheme(r, i);
    }, t.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = So(this), a = i ? bf(r) : {}, o = n.getTheme();
      it(r, o.get(this.mainType)), it(r, this.getDefaultOption()), i && zi(r, a, i);
    }, t.prototype.mergeOption = function(r, n) {
      it(this.option, r, !0);
      var i = So(this);
      i && zi(this.option, r, i);
    }, t.prototype.optionUpdated = function(r, n) {
    }, t.prototype.getDefaultOption = function() {
      var r = this.constructor;
      if (!L1(r))
        return r.defaultOption;
      var n = ab(this);
      if (!n.defaultOption) {
        for (var i = [], a = r; a; ) {
          var o = a.prototype.defaultOption;
          o && i.push(o), a = a.superClass;
        }
        for (var s = {}, u = i.length - 1; u >= 0; u--)
          s = it(s, i[u], !0);
        n.defaultOption = s;
      }
      return n.defaultOption;
    }, t.prototype.getReferringComponents = function(r, n) {
      var i = r + "Index", a = r + "Id";
      return Qi(this.ecModel, r, {
        index: this.get(i, !0),
        id: this.get(a, !0)
      }, n);
    }, t.prototype.getBoxLayoutParams = function() {
      var r = this;
      return {
        left: r.get("left"),
        top: r.get("top"),
        right: r.get("right"),
        bottom: r.get("bottom"),
        width: r.get("width"),
        height: r.get("height")
      };
    }, t.prototype.getZLevelKey = function() {
      return "";
    }, t.prototype.setZLevel = function(r) {
      this.option.zlevel = r;
    }, t.protoInitialize = (function() {
      var r = t.prototype;
      r.type = "component", r.id = "", r.name = "", r.mainType = "", r.subType = "", r.componentIndex = 0;
    })(), t;
  })(st)
);
Wp(Q, st);
No(Q);
Vw(Q);
Ww(Q, ob);
function ob(e) {
  var t = [];
  return x(Q.getClassesByMainType(e), function(r) {
    t = t.concat(r.dependencies || r.prototype.dependencies || []);
  }), t = F(t, function(r) {
    return xe(r).main;
  }), e !== "dataset" && rt(t, "dataset") <= 0 && t.unshift("dataset"), t;
}
var Ug = "";
typeof navigator < "u" && (Ug = navigator.platform || "");
var cn = "rgba(0, 0, 0, 0.2)";
const sb = {
  darkMode: "auto",
  // backgroundColor: 'rgba(0,0,0,0)',
  colorBy: "series",
  color: ["#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de", "#3ba272", "#fc8452", "#9a60b4", "#ea7ccc"],
  gradientColor: ["#f6efa6", "#d88273", "#bf444c"],
  aria: {
    decal: {
      decals: [{
        color: cn,
        dashArrayX: [1, 0],
        dashArrayY: [2, 5],
        symbolSize: 1,
        rotation: Math.PI / 6
      }, {
        color: cn,
        symbol: "circle",
        dashArrayX: [[8, 8], [0, 8, 8, 0]],
        dashArrayY: [6, 0],
        symbolSize: 0.8
      }, {
        color: cn,
        dashArrayX: [1, 0],
        dashArrayY: [4, 3],
        rotation: -Math.PI / 4
      }, {
        color: cn,
        dashArrayX: [[6, 6], [0, 6, 6, 0]],
        dashArrayY: [6, 0]
      }, {
        color: cn,
        dashArrayX: [[1, 0], [1, 6]],
        dashArrayY: [1, 0, 6, 0],
        rotation: Math.PI / 4
      }, {
        color: cn,
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
    fontFamily: Ug.match(/^Win/) ? "Microsoft YaHei" : "sans-serif",
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
var Yg = W(["tooltip", "label", "itemName", "itemId", "itemGroupId", "itemChildGroupId", "seriesName"]), le = "original", Gt = "arrayRows", Re = "objectRows", qe = "keyedColumns", fr = "typedArray", qg = "unknown", Me = "column", kn = "row", Wt = {
  Must: 1,
  Might: 2,
  Not: 3
  // Other cases
}, Xg = gt();
function ub(e) {
  Xg(e).datasetMap = W();
}
function lb(e, t, r) {
  var n = {}, i = $g(t);
  if (!i || !e)
    return n;
  var a = [], o = [], s = t.ecModel, u = Xg(s).datasetMap, l = i.uid + "_" + r.seriesLayoutBy, f, h;
  e = e.slice(), x(e, function(y, p) {
    var g = N(y) ? y : e[p] = {
      name: y
    };
    g.type === "ordinal" && f == null && (f = p, h = d(g)), n[g.name] = [];
  });
  var v = u.get(l) || u.set(l, {
    categoryWayDim: h,
    valueWayDim: 0
  });
  x(e, function(y, p) {
    var g = y.name, _ = d(y);
    if (f == null) {
      var m = v.valueWayDim;
      c(n[g], m, _), c(o, m, _), v.valueWayDim += _;
    } else if (f === p)
      c(n[g], 0, _), c(a, 0, _);
    else {
      var m = v.categoryWayDim;
      c(n[g], m, _), c(o, m, _), v.categoryWayDim += _;
    }
  });
  function c(y, p, g) {
    for (var _ = 0; _ < g; _++)
      y.push(p + _);
  }
  function d(y) {
    var p = y.dimsDef;
    return p ? p.length : 1;
  }
  return a.length && (n.itemName = a), o.length && (n.seriesName = o), n;
}
function $g(e) {
  var t = e.get("data", !0);
  if (!t)
    return Qi(e.ecModel, "dataset", {
      index: e.get("datasetIndex", !0),
      id: e.get("datasetId", !0)
    }, ar).models[0];
}
function fb(e) {
  return !e.get("transform", !0) && !e.get("fromTransformResult", !0) ? [] : Qi(e.ecModel, "dataset", {
    index: e.get("fromDatasetIndex", !0),
    id: e.get("fromDatasetId", !0)
  }, ar).models;
}
function Zg(e, t) {
  return hb(e.data, e.sourceFormat, e.seriesLayoutBy, e.dimensionsDefine, e.startIndex, t);
}
function hb(e, t, r, n, i, a) {
  var o, s = 5;
  if (kt(e))
    return Wt.Not;
  var u, l;
  if (n) {
    var f = n[a];
    N(f) ? (u = f.name, l = f.type) : B(f) && (u = f);
  }
  if (l != null)
    return l === "ordinal" ? Wt.Must : Wt.Not;
  if (t === Gt) {
    var h = e;
    if (r === kn) {
      for (var v = h[a], c = 0; c < (v || []).length && c < s; c++)
        if ((o = S(v[i + c])) != null)
          return o;
    } else
      for (var c = 0; c < h.length && c < s; c++) {
        var d = h[i + c];
        if (d && (o = S(d[a])) != null)
          return o;
      }
  } else if (t === Re) {
    var y = e;
    if (!u)
      return Wt.Not;
    for (var c = 0; c < y.length && c < s; c++) {
      var p = y[c];
      if (p && (o = S(p[u])) != null)
        return o;
    }
  } else if (t === qe) {
    var g = e;
    if (!u)
      return Wt.Not;
    var v = g[u];
    if (!v || kt(v))
      return Wt.Not;
    for (var c = 0; c < v.length && c < s; c++)
      if ((o = S(v[c])) != null)
        return o;
  } else if (t === le)
    for (var _ = e, c = 0; c < _.length && c < s; c++) {
      var p = _[c], m = ji(p);
      if (!k(m))
        return Wt.Not;
      if ((o = S(m[a])) != null)
        return o;
    }
  function S(b) {
    var w = B(b);
    if (b != null && Number.isFinite(Number(b)) && b !== "")
      return w ? Wt.Might : Wt.Not;
    if (w && b !== "-")
      return Wt.Must;
  }
  return Wt.Not;
}
var cb = W();
function vb(e, t, r) {
  var n = cb.get(t);
  if (!n)
    return r;
  var i = n(e);
  return i ? r.concat(i) : r;
}
var Pc = gt();
gt();
var Tf = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getColorFromPalette = function(t, r, n) {
      var i = Rt(this.get("color", !0)), a = this.get("colorLayer", !0);
      return pb(this, Pc, i, a, t, r, n);
    }, e.prototype.clearColorPalette = function() {
      gb(this, Pc);
    }, e;
  })()
);
function db(e, t) {
  for (var r = e.length, n = 0; n < r; n++)
    if (e[n].length > t)
      return e[n];
  return e[r - 1];
}
function pb(e, t, r, n, i, a, o) {
  a = a || e;
  var s = t(a), u = s.paletteIdx || 0, l = s.paletteNameMap = s.paletteNameMap || {};
  if (l.hasOwnProperty(i))
    return l[i];
  var f = o == null || !n ? r : db(n, o);
  if (f = f || r, !(!f || !f.length)) {
    var h = f[u];
    return i && (l[i] = h), s.paletteIdx = (u + 1) % f.length, h;
  }
}
function gb(e, t) {
  t(e).paletteIdx = 0, t(e).paletteNameMap = {};
}
var Ma, Qn, Ic, Ac = "\0_ec_inner", yb = 1, xf = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      return e !== null && e.apply(this, arguments) || this;
    }
    return t.prototype.init = function(r, n, i, a, o, s) {
      a = a || {}, this.option = null, this._theme = new st(a), this._locale = new st(o), this._optionManager = s;
    }, t.prototype.setOption = function(r, n, i) {
      var a = Ec(n);
      this._optionManager.setOption(r, i, a), this._resetOption(null, a);
    }, t.prototype.resetOption = function(r, n) {
      return this._resetOption(r, Ec(n));
    }, t.prototype._resetOption = function(r, n) {
      var i = !1, a = this._optionManager;
      if (!r || r === "recreate") {
        var o = a.mountOption(r === "recreate");
        !this.option || r === "recreate" ? Ic(this, o) : (this.restoreData(), this._mergeOption(o, n)), i = !0;
      }
      if ((r === "timeline" || r === "media") && this.restoreData(), !r || r === "recreate" || r === "timeline") {
        var s = a.getTimelineOption(this);
        s && (i = !0, this._mergeOption(s, n));
      }
      if (!r || r === "recreate" || r === "media") {
        var u = a.getMediaOption(this);
        u.length && x(u, function(l) {
          i = !0, this._mergeOption(l, n);
        }, this);
      }
      return i;
    }, t.prototype.mergeOption = function(r) {
      this._mergeOption(r, null);
    }, t.prototype._mergeOption = function(r, n) {
      var i = this.option, a = this._componentsMap, o = this._componentsCount, s = [], u = W(), l = n && n.replaceMergeMainTypeMap;
      ub(this), x(r, function(h, v) {
        h != null && (Q.hasClass(v) ? v && (s.push(v), u.set(v, !0)) : i[v] = i[v] == null ? $(h) : it(i[v], h, !0));
      }), l && l.each(function(h, v) {
        Q.hasClass(v) && !u.get(v) && (s.push(v), u.set(v, !0));
      }), Q.topologicalTravel(s, Q.getAllClassMainTypes(), f, this);
      function f(h) {
        var v = vb(this, h, Rt(r[h])), c = a.get(h), d = (
          // `!oldCmptList` means init. See the comment in `mappingToExists`
          c ? l && l.get(h) ? "replaceMerge" : "normalMerge" : "replaceAll"
        ), y = g1(c, v, d);
        T1(y, h, Q), i[h] = null, a.set(h, null), o.set(h, 0);
        var p = [], g = [], _ = 0, m;
        x(y, function(S, b) {
          var w = S.existing, T = S.newOption;
          if (!T)
            w && (w.mergeOption({}, this), w.optionUpdated({}, !1));
          else {
            var C = h === "series", M = Q.getClass(
              h,
              S.keyInfo.subType,
              !C
              // Give a more detailed warn later if series don't exists
            );
            if (!M)
              return;
            if (h === "tooltip") {
              if (m)
                return;
              m = !0;
            }
            if (w && w.constructor === M)
              w.name = S.keyInfo.name, w.mergeOption(T, this), w.optionUpdated(T, !1);
            else {
              var D = E({
                componentIndex: b
              }, S.keyInfo);
              w = new M(T, this, this, D), E(w, D), S.brandNew && (w.__requireNewView = !0), w.init(T, this, this), w.optionUpdated(null, !0);
            }
          }
          w ? (p.push(w.option), g.push(w), _++) : (p.push(void 0), g.push(void 0));
        }, this), i[h] = p, a.set(h, g), o.set(h, _), h === "series" && Ma(this);
      }
      this._seriesIndices || Ma(this);
    }, t.prototype.getOption = function() {
      var r = $(this.option);
      return x(r, function(n, i) {
        if (Q.hasClass(i)) {
          for (var a = Rt(n), o = a.length, s = !1, u = o - 1; u >= 0; u--)
            a[u] && !ki(a[u]) ? s = !0 : (a[u] = null, !s && o--);
          a.length = o, r[i] = a;
        }
      }), delete r[Ac], r;
    }, t.prototype.getTheme = function() {
      return this._theme;
    }, t.prototype.getLocaleModel = function() {
      return this._locale;
    }, t.prototype.setUpdatePayload = function(r) {
      this._payload = r;
    }, t.prototype.getUpdatePayload = function() {
      return this._payload;
    }, t.prototype.getComponent = function(r, n) {
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
    }, t.prototype.queryComponents = function(r) {
      var n = r.mainType;
      if (!n)
        return [];
      var i = r.index, a = r.id, o = r.name, s = this._componentsMap.get(n);
      if (!s || !s.length)
        return [];
      var u;
      return i != null ? (u = [], x(Rt(i), function(l) {
        s[l] && u.push(s[l]);
      })) : a != null ? u = Lc("id", a, s) : o != null ? u = Lc("name", o, s) : u = dt(s, function(l) {
        return !!l;
      }), Rc(u, r);
    }, t.prototype.findComponents = function(r) {
      var n = r.query, i = r.mainType, a = s(n), o = a ? this.queryComponents(a) : dt(this._componentsMap.get(i), function(l) {
        return !!l;
      });
      return u(Rc(o, r));
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
        return r.filter ? dt(l, r.filter) : l;
      }
    }, t.prototype.eachComponent = function(r, n, i) {
      var a = this._componentsMap;
      if (U(r)) {
        var o = n, s = r;
        a.each(function(h, v) {
          for (var c = 0; h && c < h.length; c++) {
            var d = h[c];
            d && s.call(o, v, d, d.componentIndex);
          }
        });
      } else
        for (var u = B(r) ? a.get(r) : N(r) ? this.findComponents(r) : null, l = 0; u && l < u.length; l++) {
          var f = u[l];
          f && n.call(i, f, f.componentIndex);
        }
    }, t.prototype.getSeriesByName = function(r) {
      var n = Ce(r, null);
      return dt(this._componentsMap.get("series"), function(i) {
        return !!i && n != null && i.name === n;
      });
    }, t.prototype.getSeriesByIndex = function(r) {
      return this._componentsMap.get("series")[r];
    }, t.prototype.getSeriesByType = function(r) {
      return dt(this._componentsMap.get("series"), function(n) {
        return !!n && n.subType === r;
      });
    }, t.prototype.getSeries = function() {
      return dt(this._componentsMap.get("series"), function(r) {
        return !!r;
      });
    }, t.prototype.getSeriesCount = function() {
      return this._componentsCount.get("series");
    }, t.prototype.eachSeries = function(r, n) {
      Qn(this), x(this._seriesIndices, function(i) {
        var a = this._componentsMap.get("series")[i];
        r.call(n, a, i);
      }, this);
    }, t.prototype.eachRawSeries = function(r, n) {
      x(this._componentsMap.get("series"), function(i) {
        i && r.call(n, i, i.componentIndex);
      });
    }, t.prototype.eachSeriesByType = function(r, n, i) {
      Qn(this), x(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        o.subType === r && n.call(i, o, a);
      }, this);
    }, t.prototype.eachRawSeriesByType = function(r, n, i) {
      return x(this.getSeriesByType(r), n, i);
    }, t.prototype.isSeriesFiltered = function(r) {
      return Qn(this), this._seriesIndicesMap.get(r.componentIndex) == null;
    }, t.prototype.getCurrentSeriesIndices = function() {
      return (this._seriesIndices || []).slice();
    }, t.prototype.filterSeries = function(r, n) {
      Qn(this);
      var i = [];
      x(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        r.call(n, o, a) && i.push(a);
      }, this), this._seriesIndices = i, this._seriesIndicesMap = W(i);
    }, t.prototype.restoreData = function(r) {
      Ma(this);
      var n = this._componentsMap, i = [];
      n.each(function(a, o) {
        Q.hasClass(o) && i.push(o);
      }), Q.topologicalTravel(i, Q.getAllClassMainTypes(), function(a) {
        x(n.get(a), function(o) {
          o && (a !== "series" || !_b(o, r)) && o.restoreData();
        });
      });
    }, t.internalField = (function() {
      Ma = function(r) {
        var n = r._seriesIndices = [];
        x(r._componentsMap.get("series"), function(i) {
          i && n.push(i.componentIndex);
        }), r._seriesIndicesMap = W(n);
      }, Qn = function(r) {
      }, Ic = function(r, n) {
        r.option = {}, r.option[Ac] = yb, r._componentsMap = W({
          series: []
        }), r._componentsCount = W();
        var i = n.aria;
        N(i) && i.enabled == null && (i.enabled = !0), mb(n, r._theme.option), it(n, sb, !1), r._mergeOption(n, null);
      };
    })(), t;
  })(st)
);
function _b(e, t) {
  if (t) {
    var r = t.seriesIndex, n = t.seriesId, i = t.seriesName;
    return r != null && e.componentIndex !== r || n != null && e.id !== n || i != null && e.name !== i;
  }
}
function mb(e, t) {
  var r = e.color && !e.colorLayer;
  x(t, function(n, i) {
    i === "colorLayer" && r || Q.hasClass(i) || (typeof n == "object" ? e[i] = e[i] ? it(e[i], n, !1) : $(n) : e[i] == null && (e[i] = n));
  });
}
function Lc(e, t, r) {
  if (k(t)) {
    var n = W();
    return x(t, function(a) {
      if (a != null) {
        var o = Ce(a, null);
        o != null && n.set(a, !0);
      }
    }), dt(r, function(a) {
      return a && n.get(a[e]);
    });
  } else {
    var i = Ce(t, null);
    return dt(r, function(a) {
      return a && i != null && a[e] === i;
    });
  }
}
function Rc(e, t) {
  return t.hasOwnProperty("subType") ? dt(e, function(r) {
    return r && r.subType === t.subType;
  }) : e;
}
function Ec(e) {
  var t = W();
  return e && x(Rt(e.replaceMerge), function(r) {
    t.set(r, !0);
  }), {
    replaceMergeMainTypeMap: t
  };
}
ye(xf, Tf);
var Sb = [
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
], jg = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      x(Sb, function(r) {
        this[r] = ot(t[r], t);
      }, this);
    }
    return e;
  })()
), ru = {}, ns = (
  /** @class */
  (function() {
    function e() {
      this._coordinateSystems = [];
    }
    return e.prototype.create = function(t, r) {
      var n = [];
      x(ru, function(i, a) {
        var o = i.create(t, r);
        n = n.concat(o || []);
      }), this._coordinateSystems = n;
    }, e.prototype.update = function(t, r) {
      x(this._coordinateSystems, function(n) {
        n.update && n.update(t, r);
      });
    }, e.prototype.getCoordinateSystems = function() {
      return this._coordinateSystems.slice();
    }, e.register = function(t, r) {
      ru[t] = r;
    }, e.get = function(t) {
      return ru[t];
    }, e;
  })()
), wb = /^(min|max)?(.+)$/, bb = (
  /** @class */
  (function() {
    function e(t) {
      this._timelineOptions = [], this._mediaList = [], this._currentMediaIndices = [], this._api = t;
    }
    return e.prototype.setOption = function(t, r, n) {
      t && (x(Rt(t.series), function(o) {
        o && o.data && kt(o.data) && Ai(o.data);
      }), x(Rt(t.dataset), function(o) {
        o && o.source && kt(o.source) && Ai(o.source);
      })), t = $(t);
      var i = this._optionBackup, a = Tb(t, r, !i);
      this._newBaseOption = a.baseOption, i ? (a.timelineOptions.length && (i.timelineOptions = a.timelineOptions), a.mediaList.length && (i.mediaList = a.mediaList), a.mediaDefault && (i.mediaDefault = a.mediaDefault)) : this._optionBackup = a;
    }, e.prototype.mountOption = function(t) {
      var r = this._optionBackup;
      return this._timelineOptions = r.timelineOptions, this._mediaList = r.mediaList, this._mediaDefault = r.mediaDefault, this._currentMediaIndices = [], $(t ? r.baseOption : this._newBaseOption);
    }, e.prototype.getTimelineOption = function(t) {
      var r, n = this._timelineOptions;
      if (n.length) {
        var i = t.getComponent("timeline");
        i && (r = $(
          // FIXME:TS as TimelineModel or quivlant interface
          n[i.getCurrentIndex()]
        ));
      }
      return r;
    }, e.prototype.getMediaOption = function(t) {
      var r = this._api.getWidth(), n = this._api.getHeight(), i = this._mediaList, a = this._mediaDefault, o = [], s = [];
      if (!i.length && !a)
        return s;
      for (var u = 0, l = i.length; u < l; u++)
        xb(i[u].query, r, n) && o.push(u);
      return !o.length && a && (o = [-1]), o.length && !Mb(o, this._currentMediaIndices) && (s = F(o, function(f) {
        return $(f === -1 ? a.option : i[f].option);
      })), this._currentMediaIndices = o, s;
    }, e;
  })()
);
function Tb(e, t, r) {
  var n = [], i, a, o = e.baseOption, s = e.timeline, u = e.options, l = e.media, f = !!e.media, h = !!(u || s || o && o.timeline);
  o ? (a = o, a.timeline || (a.timeline = s)) : ((h || f) && (e.options = e.media = null), a = e), f && k(l) && x(l, function(c) {
    c && c.option && (c.query ? n.push(c) : i || (i = c));
  }), v(a), x(u, function(c) {
    return v(c);
  }), x(n, function(c) {
    return v(c.option);
  });
  function v(c) {
    x(t, function(d) {
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
function xb(e, t, r) {
  var n = {
    width: t,
    height: r,
    aspectratio: t / r
    // lower case for convenience.
  }, i = !0;
  return x(e, function(a, o) {
    var s = o.match(wb);
    if (!(!s || !s[1] || !s[2])) {
      var u = s[1], l = s[2].toLowerCase();
      Cb(n[l], a, u) || (i = !1);
    }
  }), i;
}
function Cb(e, t, r) {
  return r === "min" ? e >= t : r === "max" ? e <= t : e === t;
}
function Mb(e, t) {
  return e.join(",") === t.join(",");
}
var fe = x, Hi = N, Oc = ["areaStyle", "lineStyle", "nodeStyle", "linkStyle", "chordStyle", "label", "labelLine"];
function nu(e) {
  var t = e && e.itemStyle;
  if (t)
    for (var r = 0, n = Oc.length; r < n; r++) {
      var i = Oc[r], a = t.normal, o = t.emphasis;
      a && a[i] && (e[i] = e[i] || {}, e[i].normal ? it(e[i].normal, a[i]) : e[i].normal = a[i], a[i] = null), o && o[i] && (e[i] = e[i] || {}, e[i].emphasis ? it(e[i].emphasis, o[i]) : e[i].emphasis = o[i], o[i] = null);
    }
}
function xt(e, t, r) {
  if (e && e[t] && (e[t].normal || e[t].emphasis)) {
    var n = e[t].normal, i = e[t].emphasis;
    n && (r ? (e[t].normal = e[t].emphasis = null, ft(e[t], n)) : e[t] = n), i && (e.emphasis = e.emphasis || {}, e.emphasis[t] = i, i.focus && (e.emphasis.focus = i.focus), i.blurScope && (e.emphasis.blurScope = i.blurScope));
  }
}
function pi(e) {
  xt(e, "itemStyle"), xt(e, "lineStyle"), xt(e, "areaStyle"), xt(e, "label"), xt(e, "labelLine"), xt(e, "upperLabel"), xt(e, "edgeLabel");
}
function vt(e, t) {
  var r = Hi(e) && e[t], n = Hi(r) && r.textStyle;
  if (n)
    for (var i = 0, a = Nh.length; i < a; i++) {
      var o = Nh[i];
      n.hasOwnProperty(o) && (r[o] = n[o]);
    }
}
function te(e) {
  e && (pi(e), vt(e, "label"), e.emphasis && vt(e.emphasis, "label"));
}
function Db(e) {
  if (Hi(e)) {
    nu(e), pi(e), vt(e, "label"), vt(e, "upperLabel"), vt(e, "edgeLabel"), e.emphasis && (vt(e.emphasis, "label"), vt(e.emphasis, "upperLabel"), vt(e.emphasis, "edgeLabel"));
    var t = e.markPoint;
    t && (nu(t), te(t));
    var r = e.markLine;
    r && (nu(r), te(r));
    var n = e.markArea;
    n && te(n);
    var i = e.data;
    if (e.type === "graph") {
      i = i || e.nodes;
      var a = e.links || e.edges;
      if (a && !kt(a))
        for (var o = 0; o < a.length; o++)
          te(a[o]);
      x(e.categories, function(l) {
        pi(l);
      });
    }
    if (i && !kt(i))
      for (var o = 0; o < i.length; o++)
        te(i[o]);
    if (t = e.markPoint, t && t.data)
      for (var s = t.data, o = 0; o < s.length; o++)
        te(s[o]);
    if (r = e.markLine, r && r.data)
      for (var u = r.data, o = 0; o < u.length; o++)
        k(u[o]) ? (te(u[o][0]), te(u[o][1])) : te(u[o]);
    e.type === "gauge" ? (vt(e, "axisLabel"), vt(e, "title"), vt(e, "detail")) : e.type === "treemap" ? (xt(e.breadcrumb, "itemStyle"), x(e.levels, function(l) {
      pi(l);
    })) : e.type === "tree" && pi(e.leaves);
  }
}
function Ne(e) {
  return k(e) ? e : e ? [e] : [];
}
function kc(e) {
  return (k(e) ? e[0] : e) || {};
}
function Pb(e, t) {
  fe(Ne(e.series), function(n) {
    Hi(n) && Db(n);
  });
  var r = ["xAxis", "yAxis", "radiusAxis", "angleAxis", "singleAxis", "parallelAxis", "radar"];
  t && r.push("valueAxis", "categoryAxis", "logAxis", "timeAxis"), fe(r, function(n) {
    fe(Ne(e[n]), function(i) {
      i && (vt(i, "axisLabel"), vt(i.axisPointer, "label"));
    });
  }), fe(Ne(e.parallel), function(n) {
    var i = n && n.parallelAxisDefault;
    vt(i, "axisLabel"), vt(i && i.axisPointer, "label");
  }), fe(Ne(e.calendar), function(n) {
    xt(n, "itemStyle"), vt(n, "dayLabel"), vt(n, "monthLabel"), vt(n, "yearLabel");
  }), fe(Ne(e.radar), function(n) {
    vt(n, "name"), n.name && n.axisName == null && (n.axisName = n.name, delete n.name), n.nameGap != null && n.axisNameGap == null && (n.axisNameGap = n.nameGap, delete n.nameGap);
  }), fe(Ne(e.geo), function(n) {
    Hi(n) && (te(n), fe(Ne(n.regions), function(i) {
      te(i);
    }));
  }), fe(Ne(e.timeline), function(n) {
    te(n), xt(n, "label"), xt(n, "itemStyle"), xt(n, "controlStyle", !0);
    var i = n.data;
    k(i) && x(i, function(a) {
      N(a) && (xt(a, "label"), xt(a, "itemStyle"));
    });
  }), fe(Ne(e.toolbox), function(n) {
    xt(n, "iconStyle"), fe(n.feature, function(i) {
      xt(i, "iconStyle");
    });
  }), vt(kc(e.axisPointer), "label"), vt(kc(e.tooltip).axisPointer, "label");
}
function Ib(e, t) {
  for (var r = t.split(","), n = e, i = 0; i < r.length && (n = n && n[r[i]], n != null); i++)
    ;
  return n;
}
function Ab(e, t, r, n) {
  for (var i = t.split(","), a = e, o, s = 0; s < i.length - 1; s++)
    o = i[s], a[o] == null && (a[o] = {}), a = a[o];
  a[i[s]] == null && (a[i[s]] = r);
}
function Bc(e) {
  e && x(Lb, function(t) {
    t[0] in e && !(t[1] in e) && (e[t[1]] = e[t[0]]);
  });
}
var Lb = [["x", "left"], ["y", "top"], ["x2", "right"], ["y2", "bottom"]], Rb = ["grid", "geo", "parallel", "legend", "toolbox", "title", "visualMap", "dataZoom", "timeline"], iu = [["borderRadius", "barBorderRadius"], ["borderColor", "barBorderColor"], ["borderWidth", "barBorderWidth"]];
function Jn(e) {
  var t = e && e.itemStyle;
  if (t)
    for (var r = 0; r < iu.length; r++) {
      var n = iu[r][1], i = iu[r][0];
      t[n] != null && (t[i] = t[n]);
    }
}
function Fc(e) {
  e && e.alignTo === "edge" && e.margin != null && e.edgeDistance == null && (e.edgeDistance = e.margin);
}
function Nc(e) {
  e && e.downplay && !e.blur && (e.blur = e.downplay);
}
function Eb(e) {
  e && e.focusNodeAdjacency != null && (e.emphasis = e.emphasis || {}, e.emphasis.focus == null && (e.emphasis.focus = "adjacency"));
}
function Kg(e, t) {
  if (e)
    for (var r = 0; r < e.length; r++)
      t(e[r]), e[r] && Kg(e[r].children, t);
}
function Qg(e, t) {
  Pb(e, t), e.series = Rt(e.series), x(e.series, function(r) {
    if (N(r)) {
      var n = r.type;
      if (n === "line")
        r.clipOverflow != null && (r.clip = r.clipOverflow);
      else if (n === "pie" || n === "gauge") {
        r.clockWise != null && (r.clockwise = r.clockWise), Fc(r.label);
        var i = r.data;
        if (i && !kt(i))
          for (var a = 0; a < i.length; a++)
            Fc(i[a]);
        r.hoverOffset != null && (r.emphasis = r.emphasis || {}, (r.emphasis.scaleSize = null) && (r.emphasis.scaleSize = r.hoverOffset));
      } else if (n === "gauge") {
        var o = Ib(r, "pointer.color");
        o != null && Ab(r, "itemStyle.color", o);
      } else if (n === "bar") {
        Jn(r), Jn(r.backgroundStyle), Jn(r.emphasis);
        var i = r.data;
        if (i && !kt(i))
          for (var a = 0; a < i.length; a++)
            typeof i[a] == "object" && (Jn(i[a]), Jn(i[a] && i[a].emphasis));
      } else if (n === "sunburst") {
        var s = r.highlightPolicy;
        s && (r.emphasis = r.emphasis || {}, r.emphasis.focus || (r.emphasis.focus = s)), Nc(r), Kg(r.data, Nc);
      } else n === "graph" || n === "sankey" ? Eb(r) : n === "map" && (r.mapType && !r.map && (r.map = r.mapType), r.mapLocation && ft(r, r.mapLocation));
      r.hoverAnimation != null && (r.emphasis = r.emphasis || {}, r.emphasis && r.emphasis.scale == null && (r.emphasis.scale = r.hoverAnimation)), Bc(r);
    }
  }), e.dataRange && (e.visualMap = e.dataRange), x(Rb, function(r) {
    var n = e[r];
    n && (k(n) || (n = [n]), x(n, function(i) {
      Bc(i);
    }));
  });
}
function Ob(e) {
  var t = W();
  e.eachSeries(function(r) {
    var n = r.get("stack");
    if (n) {
      var i = t.get(n) || t.set(n, []), a = r.getData(), o = {
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
      i.length && a.setCalculationInfo("stackedOnSeries", i[i.length - 1].seriesModel), i.push(o);
    }
  }), t.each(kb);
}
function kb(e) {
  x(e, function(t, r) {
    var n = [], i = [NaN, NaN], a = [t.stackResultDimension, t.stackedOverDimension], o = t.data, s = t.isStackedByIndex, u = t.seriesModel.get("stackStrategy") || "samesign";
    o.modify(a, function(l, f, h) {
      var v = o.get(t.stackedDimension, h);
      if (isNaN(v))
        return i;
      var c, d;
      s ? d = o.getRawIndex(h) : c = o.get(t.stackedByDimension, h);
      for (var y = NaN, p = r - 1; p >= 0; p--) {
        var g = e[p];
        if (s || (d = g.data.rawIndexOf(g.stackedByDimension, c)), d >= 0) {
          var _ = g.data.getByRawIndex(g.stackResultDimension, d);
          if (u === "all" || u === "positive" && _ > 0 || u === "negative" && _ < 0 || u === "samesign" && v >= 0 && _ > 0 || u === "samesign" && v <= 0 && _ < 0) {
            v = l1(v, _), y = _;
            break;
          }
        }
      }
      return n[0] = v, n[1] = y, n;
    });
  });
}
var is = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.data = t.data || (t.sourceFormat === qe ? {} : []), this.sourceFormat = t.sourceFormat || qg, this.seriesLayoutBy = t.seriesLayoutBy || Me, this.startIndex = t.startIndex || 0, this.dimensionsDetectedCount = t.dimensionsDetectedCount, this.metaRawOption = t.metaRawOption;
      var r = this.dimensionsDefine = t.dimensionsDefine;
      if (r)
        for (var n = 0; n < r.length; n++) {
          var i = r[n];
          i.type == null && Zg(this, n) === Wt.Must && (i.type = "ordinal");
        }
    }
    return e;
  })()
);
function Cf(e) {
  return e instanceof is;
}
function hl(e, t, r) {
  r = r || ty(e);
  var n = t.seriesLayoutBy, i = Fb(e, r, n, t.sourceHeader, t.dimensions), a = new is({
    data: e,
    sourceFormat: r,
    seriesLayoutBy: n,
    dimensionsDefine: i.dimensionsDefine,
    startIndex: i.startIndex,
    dimensionsDetectedCount: i.dimensionsDetectedCount,
    metaRawOption: $(t)
  });
  return a;
}
function Jg(e) {
  return new is({
    data: e,
    sourceFormat: kt(e) ? fr : le
  });
}
function Bb(e) {
  return new is({
    data: e.data,
    sourceFormat: e.sourceFormat,
    seriesLayoutBy: e.seriesLayoutBy,
    dimensionsDefine: $(e.dimensionsDefine),
    startIndex: e.startIndex,
    dimensionsDetectedCount: e.dimensionsDetectedCount
  });
}
function ty(e) {
  var t = qg;
  if (kt(e))
    t = fr;
  else if (k(e)) {
    e.length === 0 && (t = Gt);
    for (var r = 0, n = e.length; r < n; r++) {
      var i = e[r];
      if (i != null) {
        if (k(i) || kt(i)) {
          t = Gt;
          break;
        } else if (N(i)) {
          t = Re;
          break;
        }
      }
    }
  } else if (N(e)) {
    for (var a in e)
      if (We(e, a) && Ot(e[a])) {
        t = qe;
        break;
      }
  }
  return t;
}
function Fb(e, t, r, n, i) {
  var a, o;
  if (!e)
    return {
      dimensionsDefine: zc(i),
      startIndex: o,
      dimensionsDetectedCount: a
    };
  if (t === Gt) {
    var s = e;
    n === "auto" || n == null ? Hc(function(l) {
      l != null && l !== "-" && (B(l) ? o == null && (o = 1) : o = 0);
    }, r, s, 10) : o = ut(n) ? n : n ? 1 : 0, !i && o === 1 && (i = [], Hc(function(l, f) {
      i[f] = l != null ? l + "" : "";
    }, r, s, 1 / 0)), a = i ? i.length : r === kn ? s.length : s[0] ? s[0].length : null;
  } else if (t === Re)
    i || (i = Nb(e));
  else if (t === qe)
    i || (i = [], x(e, function(l, f) {
      i.push(f);
    }));
  else if (t === le) {
    var u = ji(e[0]);
    a = k(u) && u.length || 1;
  }
  return {
    startIndex: o,
    dimensionsDefine: zc(i),
    dimensionsDetectedCount: a
  };
}
function Nb(e) {
  for (var t = 0, r; t < e.length && !(r = e[t++]); )
    ;
  if (r)
    return ct(r);
}
function zc(e) {
  if (e) {
    var t = W();
    return F(e, function(r, n) {
      r = N(r) ? r : {
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
      var a = t.get(i.name);
      return a ? i.name += "-" + a.count++ : t.set(i.name, {
        count: 1
      }), i;
    });
  }
}
function Hc(e, t, r, n) {
  if (t === kn)
    for (var i = 0; i < r.length && i < n; i++)
      e(r[i] ? r[i][0] : null, i);
  else
    for (var a = r[0] || [], i = 0; i < a.length && i < n; i++)
      e(a[i], i);
}
function ey(e) {
  var t = e.sourceFormat;
  return t === Re || t === qe;
}
var Or, kr, Br, Gc, Vc, ry = (
  /** @class */
  (function() {
    function e(t, r) {
      var n = Cf(t) ? t : Jg(t);
      this._source = n;
      var i = this._data = n.data;
      n.sourceFormat === fr && (this._offset = 0, this._dimSize = r, this._data = i), Vc(this, i, n);
    }
    return e.prototype.getSource = function() {
      return this._source;
    }, e.prototype.count = function() {
      return 0;
    }, e.prototype.getItem = function(t, r) {
    }, e.prototype.appendData = function(t) {
    }, e.prototype.clean = function() {
    }, e.protoInitialize = (function() {
      var t = e.prototype;
      t.pure = !1, t.persistent = !0;
    })(), e.internalField = (function() {
      var t;
      Vc = function(o, s, u) {
        var l = u.sourceFormat, f = u.seriesLayoutBy, h = u.startIndex, v = u.dimensionsDefine, c = Gc[Mf(l, f)];
        if (E(o, c), l === fr)
          o.getItem = r, o.count = i, o.fillStorage = n;
        else {
          var d = ny(l, f);
          o.getItem = ot(d, null, s, h, v);
          var y = iy(l, f);
          o.count = ot(y, null, s, h, v);
        }
      };
      var r = function(o, s) {
        o = o - this._offset, s = s || [];
        for (var u = this._data, l = this._dimSize, f = l * o, h = 0; h < l; h++)
          s[h] = u[f + h];
        return s;
      }, n = function(o, s, u, l) {
        for (var f = this._data, h = this._dimSize, v = 0; v < h; v++) {
          for (var c = l[v], d = c[0] == null ? 1 / 0 : c[0], y = c[1] == null ? -1 / 0 : c[1], p = s - o, g = u[v], _ = 0; _ < p; _++) {
            var m = f[_ * h + v];
            g[o + _] = m, m < d && (d = m), m > y && (y = m);
          }
          c[0] = d, c[1] = y;
        }
      }, i = function() {
        return this._data ? this._data.length / this._dimSize : 0;
      };
      Gc = (t = {}, t[Gt + "_" + Me] = {
        pure: !0,
        appendData: a
      }, t[Gt + "_" + kn] = {
        pure: !0,
        appendData: function() {
          throw new Error('Do not support appendData when set seriesLayoutBy: "row".');
        }
      }, t[Re] = {
        pure: !0,
        appendData: a
      }, t[qe] = {
        pure: !0,
        appendData: function(o) {
          var s = this._data;
          x(o, function(u, l) {
            for (var f = s[l] || (s[l] = []), h = 0; h < (u || []).length; h++)
              f.push(u[h]);
          });
        }
      }, t[le] = {
        appendData: a
      }, t[fr] = {
        persistent: !1,
        pure: !0,
        appendData: function(o) {
          this._data = o;
        },
        // Clean self if data is already used.
        clean: function() {
          this._offset += this.count(), this._data = null;
        }
      }, t);
      function a(o) {
        for (var s = 0; s < o.length; s++)
          this._data.push(o[s]);
      }
    })(), e;
  })()
), Wc = function(e, t, r, n) {
  return e[n];
}, zb = (Or = {}, Or[Gt + "_" + Me] = function(e, t, r, n) {
  return e[n + t];
}, Or[Gt + "_" + kn] = function(e, t, r, n, i) {
  n += t;
  for (var a = i || [], o = e, s = 0; s < o.length; s++) {
    var u = o[s];
    a[s] = u ? u[n] : null;
  }
  return a;
}, Or[Re] = Wc, Or[qe] = function(e, t, r, n, i) {
  for (var a = i || [], o = 0; o < r.length; o++) {
    var s = r[o].name, u = e[s];
    a[o] = u ? u[n] : null;
  }
  return a;
}, Or[le] = Wc, Or);
function ny(e, t) {
  var r = zb[Mf(e, t)];
  return r;
}
var Uc = function(e, t, r) {
  return e.length;
}, Hb = (kr = {}, kr[Gt + "_" + Me] = function(e, t, r) {
  return Math.max(0, e.length - t);
}, kr[Gt + "_" + kn] = function(e, t, r) {
  var n = e[0];
  return n ? Math.max(0, n.length - t) : 0;
}, kr[Re] = Uc, kr[qe] = function(e, t, r) {
  var n = r[0].name, i = e[n];
  return i ? i.length : 0;
}, kr[le] = Uc, kr);
function iy(e, t) {
  var r = Hb[Mf(e, t)];
  return r;
}
var au = function(e, t, r) {
  return e[t];
}, Gb = (Br = {}, Br[Gt] = au, Br[Re] = function(e, t, r) {
  return e[r];
}, Br[qe] = au, Br[le] = function(e, t, r) {
  var n = ji(e);
  return n instanceof Array ? n[t] : n;
}, Br[fr] = au, Br);
function ay(e) {
  var t = Gb[e];
  return t;
}
function Mf(e, t) {
  return e === Gt ? e + "_" + t : e;
}
function wo(e, t, r) {
  if (e) {
    var n = e.getRawDataItem(t);
    if (n != null) {
      var i = e.getStore(), a = i.getSource().sourceFormat;
      if (r != null) {
        var o = e.getDimensionIndex(r), s = i.getDimensionProperty(o);
        return ay(a)(n, o, s);
      } else {
        var u = n;
        return a === le && (u = ji(n)), u;
      }
    }
  }
}
var Vb = /\{@(.+?)\}/g, Wb = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getDataParams = function(t, r) {
      var n = this.getData(r), i = this.getRawValue(t, r), a = n.getRawIndex(t), o = n.getName(t), s = n.getRawDataItem(t), u = n.getItemVisual(t, "style"), l = u && u[n.getItemVisual(t, "drawType") || "fill"], f = u && u.stroke, h = this.mainType, v = h === "series", c = n.userOutput && n.userOutput.get();
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
    }, e.prototype.getFormattedLabel = function(t, r, n, i, a, o) {
      r = r || "normal";
      var s = this.getData(n), u = this.getDataParams(t, n);
      if (o && (u.value = o.interpolatedValue), i != null && k(u.value) && (u.value = u.value[i]), !a) {
        var l = s.getItemModel(t);
        a = l.get(r === "normal" ? ["label", "formatter"] : [r, "label", "formatter"]);
      }
      if (U(a))
        return u.status = r, u.dimensionIndex = i, a(u);
      if (B(a)) {
        var f = Sf(a, u);
        return f.replace(Vb, function(h, v) {
          var c = v.length, d = v;
          d.charAt(0) === "[" && d.charAt(c - 1) === "]" && (d = +d.slice(1, c - 1));
          var y = wo(s, t, d);
          if (o && k(o.interpolatedValue)) {
            var p = s.getDimensionIndex(d);
            p >= 0 && (y = o.interpolatedValue[p]);
          }
          return y != null ? y + "" : "";
        });
      }
    }, e.prototype.getRawValue = function(t, r) {
      return wo(this.getData(r), t);
    }, e.prototype.formatTooltip = function(t, r, n) {
    }, e;
  })()
);
function Yc(e) {
  var t, r;
  return N(e) ? e.type && (r = e) : t = e, {
    text: t,
    // markers: markers || markersExisting,
    frag: r
  };
}
function Mi(e) {
  return new Ub(e);
}
var Ub = (
  /** @class */
  (function() {
    function e(t) {
      t = t || {}, this._reset = t.reset, this._plan = t.plan, this._count = t.count, this._onDirty = t.onDirty, this._dirty = !0;
    }
    return e.prototype.perform = function(t) {
      var r = this._upstream, n = t && t.skip;
      if (this._dirty && r) {
        var i = this.context;
        i.data = i.outputData = r.context.outputData;
      }
      this.__pipeline && (this.__pipeline.currentTask = this);
      var a;
      this._plan && !n && (a = this._plan(this.context));
      var o = f(this._modBy), s = this._modDataCount || 0, u = f(t && t.modBy), l = t && t.modDataCount || 0;
      (o !== u || s !== l) && (a = "reset");
      function f(_) {
        return !(_ >= 1) && (_ = 1), _;
      }
      var h;
      (this._dirty || a === "reset") && (this._dirty = !1, h = this._doReset(n)), this._modBy = u, this._modDataCount = l;
      var v = t && t.step;
      if (r ? this._dueEnd = r._outputDueEnd : this._dueEnd = this._count ? this._count(this.context) : 1 / 0, this._progress) {
        var c = this._dueIndex, d = Math.min(v != null ? this._dueIndex + v : 1 / 0, this._dueEnd);
        if (!n && (h || c < d)) {
          var y = this._progress;
          if (k(y))
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
    }, e.prototype.dirty = function() {
      this._dirty = !0, this._onDirty && this._onDirty(this.context);
    }, e.prototype._doProgress = function(t, r, n, i, a) {
      qc.reset(r, n, i, a), this._callingProgress = t, this._callingProgress({
        start: r,
        end: n,
        count: n - r,
        next: qc.next
      }, this.context);
    }, e.prototype._doReset = function(t) {
      this._dueIndex = this._outputDueEnd = this._dueEnd = 0, this._settedOutputEnd = null;
      var r, n;
      !t && this._reset && (r = this._reset(this.context), r && r.progress && (n = r.forceFirstProgress, r = r.progress), k(r) && !r.length && (r = null)), this._progress = r, this._modBy = this._modDataCount = null;
      var i = this._downstream;
      return i && i.dirty(), n;
    }, e.prototype.unfinished = function() {
      return this._progress && this._dueIndex < this._dueEnd;
    }, e.prototype.pipe = function(t) {
      (this._downstream !== t || this._dirty) && (this._downstream = t, t._upstream = this, t.dirty());
    }, e.prototype.dispose = function() {
      this._disposed || (this._upstream && (this._upstream._downstream = null), this._downstream && (this._downstream._upstream = null), this._dirty = !1, this._disposed = !0);
    }, e.prototype.getUpstream = function() {
      return this._upstream;
    }, e.prototype.getDownstream = function() {
      return this._downstream;
    }, e.prototype.setOutputEnd = function(t) {
      this._outputDueEnd = this._settedOutputEnd = t;
    }, e;
  })()
), qc = /* @__PURE__ */ (function() {
  var e, t, r, n, i, a = {
    reset: function(u, l, f, h) {
      t = u, e = l, r = f, n = h, i = Math.ceil(n / r), a.next = r > 1 && n > 0 ? s : o;
    }
  };
  return a;
  function o() {
    return t < e ? t++ : null;
  }
  function s() {
    var u = t % i * r + Math.ceil(t / i), l = t >= e ? null : u < n ? u : t;
    return t++, l;
  }
})();
function Ja(e, t) {
  var r = t && t.type;
  return r === "ordinal" ? e : (r === "time" && !ut(e) && e != null && e !== "-" && (e = +se(e)), e == null || e === "" ? NaN : Number(e));
}
var Yb = W({
  number: function(e) {
    return parseFloat(e);
  },
  time: function(e) {
    return +se(e);
  },
  trim: function(e) {
    return B(e) ? pe(e) : e;
  }
});
function aP(e) {
  return Yb.get(e);
}
var oy = {
  lt: function(e, t) {
    return e < t;
  },
  lte: function(e, t) {
    return e <= t;
  },
  gt: function(e, t) {
    return e > t;
  },
  gte: function(e, t) {
    return e >= t;
  }
}, qb = (
  /** @class */
  (function() {
    function e(t, r) {
      if (!ut(r)) {
        var n = "";
        At(n);
      }
      this._opFn = oy[t], this._rvalFloat = Ue(r);
    }
    return e.prototype.evaluate = function(t) {
      return ut(t) ? this._opFn(t, this._rvalFloat) : this._opFn(Ue(t), this._rvalFloat);
    }, e;
  })()
), Xb = (
  /** @class */
  (function() {
    function e(t, r) {
      var n = t === "desc";
      this._resultLT = n ? 1 : -1, r == null && (r = n ? "min" : "max"), this._incomparable = r === "min" ? -1 / 0 : 1 / 0;
    }
    return e.prototype.evaluate = function(t, r) {
      var n = ut(t) ? t : Ue(t), i = ut(r) ? r : Ue(r), a = isNaN(n), o = isNaN(i);
      if (a && (n = this._incomparable), o && (i = this._incomparable), a && o) {
        var s = B(t), u = B(r);
        s && (n = u ? t : 0), u && (i = s ? r : 0);
      }
      return n < i ? this._resultLT : n > i ? -this._resultLT : 0;
    }, e;
  })()
), $b = (
  /** @class */
  (function() {
    function e(t, r) {
      this._rval = r, this._isEQ = t, this._rvalTypeof = typeof r, this._rvalFloat = Ue(r);
    }
    return e.prototype.evaluate = function(t) {
      var r = t === this._rval;
      if (!r) {
        var n = typeof t;
        n !== this._rvalTypeof && (n === "number" || this._rvalTypeof === "number") && (r = Ue(t) === this._rvalFloat);
      }
      return this._isEQ ? r : !r;
    }, e;
  })()
);
function oP(e, t) {
  return e === "eq" || e === "ne" ? new $b(e === "eq", t) : We(oy, e) ? new qb(e, t) : null;
}
var Zb = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getRawData = function() {
      throw new Error("not supported");
    }, e.prototype.getRawDataItem = function(t) {
      throw new Error("not supported");
    }, e.prototype.cloneRawData = function() {
    }, e.prototype.getDimensionInfo = function(t) {
    }, e.prototype.cloneAllDimensionInfo = function() {
    }, e.prototype.count = function() {
    }, e.prototype.retrieveValue = function(t, r) {
    }, e.prototype.retrieveValueFromItem = function(t, r) {
    }, e.prototype.convertValue = function(t, r) {
      return Ja(t, r);
    }, e;
  })()
);
function jb(e, t) {
  var r = new Zb(), n = e.data, i = r.sourceFormat = e.sourceFormat, a = e.startIndex, o = "";
  e.seriesLayoutBy !== Me && At(o);
  var s = [], u = {}, l = e.dimensionsDefine;
  if (l)
    x(l, function(y, p) {
      var g = y.name, _ = {
        index: p,
        name: g,
        displayName: y.displayName
      };
      if (s.push(_), g != null) {
        var m = "";
        We(u, g) && At(m), u[g] = _;
      }
    });
  else
    for (var f = 0; f < e.dimensionsDetectedCount; f++)
      s.push({
        index: f
      });
  var h = ny(i, Me);
  t.__isBuiltIn && (r.getRawDataItem = function(y) {
    return h(n, a, s, y);
  }, r.getRawData = ot(Kb, null, e)), r.cloneRawData = ot(Qb, null, e);
  var v = iy(i, Me);
  r.count = ot(v, null, n, a, s);
  var c = ay(i);
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
  return r.getDimensionInfo = ot(Jb, null, s, u), r.cloneAllDimensionInfo = ot(tT, null, s), r;
}
function Kb(e) {
  var t = e.sourceFormat;
  if (!Df(t)) {
    var r = "";
    At(r);
  }
  return e.data;
}
function Qb(e) {
  var t = e.sourceFormat, r = e.data;
  if (!Df(t)) {
    var n = "";
    At(n);
  }
  if (t === Gt) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(r[a].slice());
    return i;
  } else if (t === Re) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(E({}, r[a]));
    return i;
  }
}
function Jb(e, t, r) {
  if (r != null) {
    if (ut(r) || !isNaN(r) && !We(t, r))
      return e[r];
    if (We(t, r))
      return t[r];
  }
}
function tT(e) {
  return $(e);
}
var sy = W();
function eT(e) {
  e = $(e);
  var t = e.type, r = "";
  t || At(r);
  var n = t.split(":");
  n.length !== 2 && At(r);
  var i = !1;
  n[0] === "echarts" && (t = n[1], i = !0), e.__isBuiltIn = i, sy.set(t, e);
}
function rT(e, t, r) {
  var n = Rt(e), i = n.length, a = "";
  i || At(a);
  for (var o = 0, s = i; o < s; o++) {
    var u = n[o];
    t = nT(u, t), o !== s - 1 && (t.length = Math.max(t.length, 1));
  }
  return t;
}
function nT(e, t, r, n) {
  var i = "";
  t.length || At(i), N(e) || At(i);
  var a = e.type, o = sy.get(a);
  o || At(i);
  var s = F(t, function(l) {
    return jb(l, o);
  }), u = Rt(o.transform({
    upstream: s[0],
    upstreamList: s,
    config: $(e.config)
  }));
  return F(u, function(l, f) {
    var h = "";
    N(l) || At(h), l.data || At(h);
    var v = ty(l.data);
    Df(v) || At(h);
    var c, d = t[0];
    if (d && f === 0 && !l.dimensions) {
      var y = d.startIndex;
      y && (l.data = d.data.slice(0, y).concat(l.data)), c = {
        seriesLayoutBy: Me,
        sourceHeader: y,
        dimensions: d.metaRawOption.dimensions
      };
    } else
      c = {
        seriesLayoutBy: Me,
        sourceHeader: 0,
        dimensions: l.dimensions
      };
    return hl(l.data, c, null);
  });
}
function Df(e) {
  return e === Gt || e === Re;
}
var as = "undefined", iT = typeof Uint32Array === as ? Array : Uint32Array, aT = typeof Uint16Array === as ? Array : Uint16Array, uy = typeof Int32Array === as ? Array : Int32Array, Xc = typeof Float64Array === as ? Array : Float64Array, ly = {
  float: Xc,
  int: uy,
  // Ordinal data type can be string or int
  ordinal: Array,
  number: Array,
  time: Xc
}, ou;
function vn(e) {
  return e > 65535 ? iT : aT;
}
function dn() {
  return [1 / 0, -1 / 0];
}
function oT(e) {
  var t = e.constructor;
  return t === Array ? e.slice() : new t(e);
}
function $c(e, t, r, n, i) {
  var a = ly[r || "float"];
  if (i) {
    var o = e[t], s = o && o.length;
    if (s !== n) {
      for (var u = new a(n), l = 0; l < s; l++)
        u[l] = o[l];
      e[t] = u;
    }
  } else
    e[t] = new a(n);
}
var cl = (
  /** @class */
  (function() {
    function e() {
      this._chunks = [], this._rawExtent = [], this._extent = [], this._count = 0, this._rawCount = 0, this._calcDimNameToIdx = W();
    }
    return e.prototype.initData = function(t, r, n) {
      this._provider = t, this._chunks = [], this._indices = null, this.getRawIndex = this._getRawIdxIdentity;
      var i = t.getSource(), a = this.defaultDimValueGetter = ou[i.sourceFormat];
      this._dimValueGetter = n || a, this._rawExtent = [], ey(i), this._dimensions = F(r, function(o) {
        return {
          // Only pick these two props. Not leak other properties like orderMeta.
          type: o.type,
          property: o.property
        };
      }), this._initDataFromProvider(0, t.count());
    }, e.prototype.getProvider = function() {
      return this._provider;
    }, e.prototype.getSource = function() {
      return this._provider.getSource();
    }, e.prototype.ensureCalculationDimension = function(t, r) {
      var n = this._calcDimNameToIdx, i = this._dimensions, a = n.get(t);
      if (a != null) {
        if (i[a].type === r)
          return a;
      } else
        a = i.length;
      return i[a] = {
        type: r
      }, n.set(t, a), this._chunks[a] = new ly[r || "float"](this._rawCount), this._rawExtent[a] = dn(), a;
    }, e.prototype.collectOrdinalMeta = function(t, r) {
      var n = this._chunks[t], i = this._dimensions[t], a = this._rawExtent, o = i.ordinalOffset || 0, s = n.length;
      o === 0 && (a[t] = dn());
      for (var u = a[t], l = o; l < s; l++) {
        var f = n[l] = r.parseAndCollect(n[l]);
        isNaN(f) || (u[0] = Math.min(f, u[0]), u[1] = Math.max(f, u[1]));
      }
      i.ordinalMeta = r, i.ordinalOffset = s, i.type = "ordinal";
    }, e.prototype.getOrdinalMeta = function(t) {
      var r = this._dimensions[t], n = r.ordinalMeta;
      return n;
    }, e.prototype.getDimensionProperty = function(t) {
      var r = this._dimensions[t];
      return r && r.property;
    }, e.prototype.appendData = function(t) {
      var r = this._provider, n = this.count();
      r.appendData(t);
      var i = r.count();
      return r.persistent || (i += n), n < i && this._initDataFromProvider(n, i, !0), [n, i];
    }, e.prototype.appendValues = function(t, r) {
      for (var n = this._chunks, i = this._dimensions, a = i.length, o = this._rawExtent, s = this.count(), u = s + Math.max(t.length, r || 0), l = 0; l < a; l++) {
        var f = i[l];
        $c(n, l, f.type, u, !0);
      }
      for (var h = [], v = s; v < u; v++)
        for (var c = v - s, d = 0; d < a; d++) {
          var f = i[d], y = ou.arrayRows.call(this, t[c] || h, f.property, c, d);
          n[d][v] = y;
          var p = o[d];
          y < p[0] && (p[0] = y), y > p[1] && (p[1] = y);
        }
      return this._rawCount = this._count = u, {
        start: s,
        end: u
      };
    }, e.prototype._initDataFromProvider = function(t, r, n) {
      for (var i = this._provider, a = this._chunks, o = this._dimensions, s = o.length, u = this._rawExtent, l = F(o, function(_) {
        return _.property;
      }), f = 0; f < s; f++) {
        var h = o[f];
        u[f] || (u[f] = dn()), $c(a, f, h.type, r, n);
      }
      if (i.fillStorage)
        i.fillStorage(t, r, a, u);
      else
        for (var v = [], c = t; c < r; c++) {
          v = i.getItem(c, v);
          for (var d = 0; d < s; d++) {
            var y = a[d], p = this._dimValueGetter(v, l[d], c, d);
            y[c] = p;
            var g = u[d];
            p < g[0] && (g[0] = p), p > g[1] && (g[1] = p);
          }
        }
      !i.persistent && i.clean && i.clean(), this._rawCount = this._count = r, this._extent = [];
    }, e.prototype.count = function() {
      return this._count;
    }, e.prototype.get = function(t, r) {
      if (!(r >= 0 && r < this._count))
        return NaN;
      var n = this._chunks[t];
      return n ? n[this.getRawIndex(r)] : NaN;
    }, e.prototype.getValues = function(t, r) {
      var n = [], i = [];
      if (r == null) {
        r = t, t = [];
        for (var a = 0; a < this._dimensions.length; a++)
          i.push(a);
      } else
        i = t;
      for (var a = 0, o = i.length; a < o; a++)
        n.push(this.get(i[a], r));
      return n;
    }, e.prototype.getByRawIndex = function(t, r) {
      if (!(r >= 0 && r < this._rawCount))
        return NaN;
      var n = this._chunks[t];
      return n ? n[r] : NaN;
    }, e.prototype.getSum = function(t) {
      var r = this._chunks[t], n = 0;
      if (r)
        for (var i = 0, a = this.count(); i < a; i++) {
          var o = this.get(t, i);
          isNaN(o) || (n += o);
        }
      return n;
    }, e.prototype.getMedian = function(t) {
      var r = [];
      this.each([t], function(a) {
        isNaN(a) || r.push(a);
      });
      var n = r.sort(function(a, o) {
        return a - o;
      }), i = this.count();
      return i === 0 ? 0 : i % 2 === 1 ? n[(i - 1) / 2] : (n[i / 2] + n[i / 2 - 1]) / 2;
    }, e.prototype.indexOfRawIndex = function(t) {
      if (t >= this._rawCount || t < 0)
        return -1;
      if (!this._indices)
        return t;
      var r = this._indices, n = r[t];
      if (n != null && n < this._count && n === t)
        return t;
      for (var i = 0, a = this._count - 1; i <= a; ) {
        var o = (i + a) / 2 | 0;
        if (r[o] < t)
          i = o + 1;
        else if (r[o] > t)
          a = o - 1;
        else
          return o;
      }
      return -1;
    }, e.prototype.indicesOfNearest = function(t, r, n) {
      var i = this._chunks, a = i[t], o = [];
      if (!a)
        return o;
      n == null && (n = 1 / 0);
      for (var s = 1 / 0, u = -1, l = 0, f = 0, h = this.count(); f < h; f++) {
        var v = this.getRawIndex(f), c = r - a[v], d = Math.abs(c);
        d <= n && ((d < s || d === s && c >= 0 && u < 0) && (s = d, u = c, l = 0), c === u && (o[l++] = f));
      }
      return o.length = l, o;
    }, e.prototype.getIndices = function() {
      var t, r = this._indices;
      if (r) {
        var n = r.constructor, i = this._count;
        if (n === Array) {
          t = new n(i);
          for (var a = 0; a < i; a++)
            t[a] = r[a];
        } else
          t = new n(r.buffer, 0, i);
      } else {
        var n = vn(this._rawCount);
        t = new n(this.count());
        for (var a = 0; a < t.length; a++)
          t[a] = a;
      }
      return t;
    }, e.prototype.filter = function(t, r) {
      if (!this._count)
        return this;
      for (var n = this.clone(), i = n.count(), a = vn(n._rawCount), o = new a(i), s = [], u = t.length, l = 0, f = t[0], h = n._chunks, v = 0; v < i; v++) {
        var c = void 0, d = n.getRawIndex(v);
        if (u === 0)
          c = r(v);
        else if (u === 1) {
          var y = h[f][d];
          c = r(y, v);
        } else {
          for (var p = 0; p < u; p++)
            s[p] = h[t[p]][d];
          s[p] = v, c = r.apply(null, s);
        }
        c && (o[l++] = d);
      }
      return l < i && (n._indices = o), n._count = l, n._extent = [], n._updateGetRawIdx(), n;
    }, e.prototype.selectRange = function(t) {
      var r = this.clone(), n = r._count;
      if (!n)
        return this;
      var i = ct(t), a = i.length;
      if (!a)
        return this;
      var o = r.count(), s = vn(r._rawCount), u = new s(o), l = 0, f = i[0], h = t[f][0], v = t[f][1], c = r._chunks, d = !1;
      if (!r._indices) {
        var y = 0;
        if (a === 1) {
          for (var p = c[i[0]], g = 0; g < n; g++) {
            var _ = p[g];
            (_ >= h && _ <= v || isNaN(_)) && (u[l++] = y), y++;
          }
          d = !0;
        } else if (a === 2) {
          for (var p = c[i[0]], m = c[i[1]], S = t[i[1]][0], b = t[i[1]][1], g = 0; g < n; g++) {
            var _ = p[g], w = m[g];
            (_ >= h && _ <= v || isNaN(_)) && (w >= S && w <= b || isNaN(w)) && (u[l++] = y), y++;
          }
          d = !0;
        }
      }
      if (!d)
        if (a === 1)
          for (var g = 0; g < o; g++) {
            var T = r.getRawIndex(g), _ = c[i[0]][T];
            (_ >= h && _ <= v || isNaN(_)) && (u[l++] = T);
          }
        else
          for (var g = 0; g < o; g++) {
            for (var C = !0, T = r.getRawIndex(g), M = 0; M < a; M++) {
              var D = i[M], _ = c[D][T];
              (_ < t[D][0] || _ > t[D][1]) && (C = !1);
            }
            C && (u[l++] = r.getRawIndex(g));
          }
      return l < o && (r._indices = u), r._count = l, r._extent = [], r._updateGetRawIdx(), r;
    }, e.prototype.map = function(t, r) {
      var n = this.clone(t);
      return this._updateDims(n, t, r), n;
    }, e.prototype.modify = function(t, r) {
      this._updateDims(this, t, r);
    }, e.prototype._updateDims = function(t, r, n) {
      for (var i = t._chunks, a = [], o = r.length, s = t.count(), u = [], l = t._rawExtent, f = 0; f < r.length; f++)
        l[r[f]] = dn();
      for (var h = 0; h < s; h++) {
        for (var v = t.getRawIndex(h), c = 0; c < o; c++)
          u[c] = i[r[c]][v];
        u[o] = h;
        var d = n && n.apply(null, u);
        if (d != null) {
          typeof d != "object" && (a[0] = d, d = a);
          for (var f = 0; f < d.length; f++) {
            var y = r[f], p = d[f], g = l[y], _ = i[y];
            _ && (_[v] = p), p < g[0] && (g[0] = p), p > g[1] && (g[1] = p);
          }
        }
      }
    }, e.prototype.lttbDownSample = function(t, r) {
      var n = this.clone([t], !0), i = n._chunks, a = i[t], o = this.count(), s = 0, u = Math.floor(1 / r), l = this.getRawIndex(0), f, h, v, c = new (vn(this._rawCount))(Math.min((Math.ceil(o / u) + 2) * 2, o));
      c[s++] = l;
      for (var d = 1; d < o - 1; d += u) {
        for (var y = Math.min(d + u, o - 1), p = Math.min(d + u * 2, o), g = (p + y) / 2, _ = 0, m = y; m < p; m++) {
          var S = this.getRawIndex(m), b = a[S];
          isNaN(b) || (_ += b);
        }
        _ /= p - y;
        var w = d, T = Math.min(d + u, o), C = d - 1, M = a[l];
        f = -1, v = w;
        for (var D = -1, P = 0, m = w; m < T; m++) {
          var S = this.getRawIndex(m), b = a[S];
          if (isNaN(b)) {
            P++, D < 0 && (D = S);
            continue;
          }
          h = Math.abs((C - g) * (b - M) - (C - m) * (_ - M)), h > f && (f = h, v = S);
        }
        P > 0 && P < T - w && (c[s++] = Math.min(D, v), v = Math.max(D, v)), c[s++] = v, l = v;
      }
      return c[s++] = this.getRawIndex(o - 1), n._count = s, n._indices = c, n.getRawIndex = this._getRawIdx, n;
    }, e.prototype.minmaxDownSample = function(t, r) {
      for (var n = this.clone([t], !0), i = n._chunks, a = Math.floor(1 / r), o = i[t], s = this.count(), u = new (vn(this._rawCount))(Math.ceil(s / a) * 2), l = 0, f = 0; f < s; f += a) {
        var h = f, v = o[this.getRawIndex(h)], c = f, d = o[this.getRawIndex(c)], y = a;
        f + a > s && (y = s - f);
        for (var p = 0; p < y; p++) {
          var g = this.getRawIndex(f + p), _ = o[g];
          _ < v && (v = _, h = f + p), _ > d && (d = _, c = f + p);
        }
        var m = this.getRawIndex(h), S = this.getRawIndex(c);
        h < c ? (u[l++] = m, u[l++] = S) : (u[l++] = S, u[l++] = m);
      }
      return n._count = l, n._indices = u, n._updateGetRawIdx(), n;
    }, e.prototype.downSample = function(t, r, n, i) {
      for (var a = this.clone([t], !0), o = a._chunks, s = [], u = Math.floor(1 / r), l = o[t], f = this.count(), h = a._rawExtent[t] = dn(), v = new (vn(this._rawCount))(Math.ceil(f / u)), c = 0, d = 0; d < f; d += u) {
        u > f - d && (u = f - d, s.length = u);
        for (var y = 0; y < u; y++) {
          var p = this.getRawIndex(d + y);
          s[y] = l[p];
        }
        var g = n(s), _ = this.getRawIndex(Math.min(d + i(s, g) || 0, f - 1));
        l[_] = g, g < h[0] && (h[0] = g), g > h[1] && (h[1] = g), v[c++] = _;
      }
      return a._count = c, a._indices = v, a._updateGetRawIdx(), a;
    }, e.prototype.each = function(t, r) {
      if (this._count)
        for (var n = t.length, i = this._chunks, a = 0, o = this.count(); a < o; a++) {
          var s = this.getRawIndex(a);
          switch (n) {
            case 0:
              r(a);
              break;
            case 1:
              r(i[t[0]][s], a);
              break;
            case 2:
              r(i[t[0]][s], i[t[1]][s], a);
              break;
            default:
              for (var u = 0, l = []; u < n; u++)
                l[u] = i[t[u]][s];
              l[u] = a, r.apply(null, l);
          }
        }
    }, e.prototype.getDataExtent = function(t) {
      var r = this._chunks[t], n = dn();
      if (!r)
        return n;
      var i = this.count(), a = !this._indices, o;
      if (a)
        return this._rawExtent[t].slice();
      if (o = this._extent[t], o)
        return o.slice();
      o = n;
      for (var s = o[0], u = o[1], l = 0; l < i; l++) {
        var f = this.getRawIndex(l), h = r[f];
        h < s && (s = h), h > u && (u = h);
      }
      return o = [s, u], this._extent[t] = o, o;
    }, e.prototype.getRawDataItem = function(t) {
      var r = this.getRawIndex(t);
      if (this._provider.persistent)
        return this._provider.getItem(r);
      for (var n = [], i = this._chunks, a = 0; a < i.length; a++)
        n.push(i[a][r]);
      return n;
    }, e.prototype.clone = function(t, r) {
      var n = new e(), i = this._chunks, a = t && Pe(t, function(s, u) {
        return s[u] = !0, s;
      }, {});
      if (a)
        for (var o = 0; o < i.length; o++)
          n._chunks[o] = a[o] ? oT(i[o]) : i[o];
      else
        n._chunks = i;
      return this._copyCommonProps(n), r || (n._indices = this._cloneIndices()), n._updateGetRawIdx(), n;
    }, e.prototype._copyCommonProps = function(t) {
      t._count = this._count, t._rawCount = this._rawCount, t._provider = this._provider, t._dimensions = this._dimensions, t._extent = $(this._extent), t._rawExtent = $(this._rawExtent);
    }, e.prototype._cloneIndices = function() {
      if (this._indices) {
        var t = this._indices.constructor, r = void 0;
        if (t === Array) {
          var n = this._indices.length;
          r = new t(n);
          for (var i = 0; i < n; i++)
            r[i] = this._indices[i];
        } else
          r = new t(this._indices);
        return r;
      }
      return null;
    }, e.prototype._getRawIdxIdentity = function(t) {
      return t;
    }, e.prototype._getRawIdx = function(t) {
      return t < this._count && t >= 0 ? this._indices[t] : -1;
    }, e.prototype._updateGetRawIdx = function() {
      this.getRawIndex = this._indices ? this._getRawIdx : this._getRawIdxIdentity;
    }, e.internalField = (function() {
      function t(r, n, i, a) {
        return Ja(r[a], this._dimensions[a]);
      }
      ou = {
        arrayRows: t,
        objectRows: function(r, n, i, a) {
          return Ja(r[n], this._dimensions[a]);
        },
        keyedColumns: t,
        original: function(r, n, i, a) {
          var o = r && (r.value == null ? r : r.value);
          return Ja(o instanceof Array ? o[a] : o, this._dimensions[a]);
        },
        typedArray: function(r, n, i, a) {
          return r[a];
        }
      };
    })(), e;
  })()
), fy = (
  /** @class */
  (function() {
    function e(t) {
      this._sourceList = [], this._storeList = [], this._upstreamSignList = [], this._versionSignBase = 0, this._dirty = !0, this._sourceHost = t;
    }
    return e.prototype.dirty = function() {
      this._setLocalSource([], []), this._storeList = [], this._dirty = !0;
    }, e.prototype._setLocalSource = function(t, r) {
      this._sourceList = t, this._upstreamSignList = r, this._versionSignBase++, this._versionSignBase > 9e10 && (this._versionSignBase = 0);
    }, e.prototype._getVersionSign = function() {
      return this._sourceHost.uid + "_" + this._versionSignBase;
    }, e.prototype.prepareSource = function() {
      this._isDirty() && (this._createSource(), this._dirty = !1);
    }, e.prototype._createSource = function() {
      this._setLocalSource([], []);
      var t = this._sourceHost, r = this._getUpstreamSourceManagers(), n = !!r.length, i, a;
      if (Da(t)) {
        var o = t, s = void 0, u = void 0, l = void 0;
        if (n) {
          var f = r[0];
          f.prepareSource(), l = f.getSource(), s = l.data, u = l.sourceFormat, a = [f._getVersionSign()];
        } else
          s = o.get("data", !0), u = kt(s) ? fr : le, a = [];
        var h = this._getSourceMetaRawOption() || {}, v = l && l.metaRawOption || {}, c = Y(h.seriesLayoutBy, v.seriesLayoutBy) || null, d = Y(h.sourceHeader, v.sourceHeader), y = Y(h.dimensions, v.dimensions), p = c !== v.seriesLayoutBy || !!d != !!v.sourceHeader || y;
        i = p ? [hl(s, {
          seriesLayoutBy: c,
          sourceHeader: d,
          dimensions: y
        }, u)] : [];
      } else {
        var g = t;
        if (n) {
          var _ = this._applyTransform(r);
          i = _.sourceList, a = _.upstreamSignList;
        } else {
          var m = g.get("source", !0);
          i = [hl(m, this._getSourceMetaRawOption(), null)], a = [];
        }
      }
      this._setLocalSource(i, a);
    }, e.prototype._applyTransform = function(t) {
      var r = this._sourceHost, n = r.get("transform", !0), i = r.get("fromTransformResult", !0);
      if (i != null) {
        var a = "";
        t.length !== 1 && jc(a);
      }
      var o, s = [], u = [];
      return x(t, function(l) {
        l.prepareSource();
        var f = l.getSource(i || 0), h = "";
        i != null && !f && jc(h), s.push(f), u.push(l._getVersionSign());
      }), n ? o = rT(n, s, {
        datasetIndex: r.componentIndex
      }) : i != null && (o = [Bb(s[0])]), {
        sourceList: o,
        upstreamSignList: u
      };
    }, e.prototype._isDirty = function() {
      if (this._dirty)
        return !0;
      for (var t = this._getUpstreamSourceManagers(), r = 0; r < t.length; r++) {
        var n = t[r];
        if (
          // Consider the case that there is ancestor diry, call it recursively.
          // The performance is probably not an issue because usually the chain is not long.
          n._isDirty() || this._upstreamSignList[r] !== n._getVersionSign()
        )
          return !0;
      }
    }, e.prototype.getSource = function(t) {
      t = t || 0;
      var r = this._sourceList[t];
      if (!r) {
        var n = this._getUpstreamSourceManagers();
        return n[0] && n[0].getSource(t);
      }
      return r;
    }, e.prototype.getSharedDataStore = function(t) {
      var r = t.makeStoreSchema();
      return this._innerGetDataStore(r.dimensions, t.source, r.hash);
    }, e.prototype._innerGetDataStore = function(t, r, n) {
      var i = 0, a = this._storeList, o = a[i];
      o || (o = a[i] = {});
      var s = o[n];
      if (!s) {
        var u = this._getUpstreamSourceManagers()[0];
        Da(this._sourceHost) && u ? s = u._innerGetDataStore(t, r, n) : (s = new cl(), s.initData(new ry(r, t.length), t)), o[n] = s;
      }
      return s;
    }, e.prototype._getUpstreamSourceManagers = function() {
      var t = this._sourceHost;
      if (Da(t)) {
        var r = $g(t);
        return r ? [r.getSourceManager()] : [];
      } else
        return F(fb(t), function(n) {
          return n.getSourceManager();
        });
    }, e.prototype._getSourceMetaRawOption = function() {
      var t = this._sourceHost, r, n, i;
      if (Da(t))
        r = t.get("seriesLayoutBy", !0), n = t.get("sourceHeader", !0), i = t.get("dimensions", !0);
      else if (!this._getUpstreamSourceManagers().length) {
        var a = t;
        r = a.get("seriesLayoutBy", !0), n = a.get("sourceHeader", !0), i = a.get("dimensions", !0);
      }
      return {
        seriesLayoutBy: r,
        sourceHeader: n,
        dimensions: i
      };
    }, e;
  })()
);
function Zc(e) {
  var t = e.option.transform;
  t && Ai(e.option.transform);
}
function Da(e) {
  return e.mainType === "series";
}
function jc(e) {
  throw new Error(e);
}
var sT = "line-height:1";
function hy(e) {
  var t = e.lineHeight;
  return t == null ? sT : "line-height:" + Pt(t + "") + "px";
}
function cy(e, t) {
  var r = e.color || "#6e7079", n = e.fontSize || 12, i = e.fontWeight || "400", a = e.color || "#464646", o = e.fontSize || 14, s = e.fontWeight || "900";
  return t === "html" ? {
    // eslint-disable-next-line max-len
    nameStyle: "font-size:" + Pt(n + "") + "px;color:" + Pt(r) + ";font-weight:" + Pt(i + ""),
    // eslint-disable-next-line max-len
    valueStyle: "font-size:" + Pt(o + "") + "px;color:" + Pt(a) + ";font-weight:" + Pt(s + "")
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
var uT = [0, 10, 20, 30], lT = ["", `
`, `

`, `


`];
function Gi(e, t) {
  return t.type = e, t;
}
function vl(e) {
  return e.type === "section";
}
function vy(e) {
  return vl(e) ? fT : hT;
}
function dy(e) {
  if (vl(e)) {
    var t = 0, r = e.blocks.length, n = r > 1 || r > 0 && !e.noHeader;
    return x(e.blocks, function(i) {
      var a = dy(i);
      a >= t && (t = a + +(n && // 0 always can not be readable gap level.
      (!a || vl(i) && !i.noHeader)));
    }), t;
  }
  return 0;
}
function fT(e, t, r, n) {
  var i = t.noHeader, a = cT(dy(t)), o = [], s = t.blocks || [];
  Ie(!s || k(s)), s = s || [];
  var u = e.orderMode;
  if (t.sortBlocks && u) {
    s = s.slice();
    var l = {
      valueAsc: "asc",
      valueDesc: "desc"
    };
    if (We(l, u)) {
      var f = new Xb(l[u], null);
      s.sort(function(y, p) {
        return f.evaluate(y.sortParam, p.sortParam);
      });
    } else u === "seriesDesc" && s.reverse();
  }
  x(s, function(y, p) {
    var g = t.valueFormatter, _ = vy(y)(
      // Inherit valueFormatter
      g ? E(E({}, e), {
        valueFormatter: g
      }) : e,
      y,
      p > 0 ? a.html : 0,
      n
    );
    _ != null && o.push(_);
  });
  var h = e.renderMode === "richText" ? o.join(a.richText) : dl(n, o.join(""), i ? r : a.html);
  if (i)
    return h;
  var v = fl(t.header, "ordinal", e.useUTC), c = cy(n, e.renderMode).nameStyle, d = hy(n);
  return e.renderMode === "richText" ? py(e, v, c) + a.richText + h : dl(n, '<div style="' + c + ";" + d + ';">' + Pt(v) + "</div>" + h, r);
}
function hT(e, t, r, n) {
  var i = e.renderMode, a = t.noName, o = t.noValue, s = !t.markerType, u = t.name, l = e.useUTC, f = t.valueFormatter || e.valueFormatter || function(S) {
    return S = k(S) ? S : [S], F(S, function(b, w) {
      return fl(b, k(c) ? c[w] : c, l);
    });
  };
  if (!(a && o)) {
    var h = s ? "" : e.markupStyleCreator.makeTooltipMarker(t.markerType, t.markerColor || "#333", i), v = a ? "" : fl(u, "ordinal", l), c = t.valueType, d = o ? [] : f(t.value, t.dataIndex), y = !s || !a, p = !s && a, g = cy(n, i), _ = g.nameStyle, m = g.valueStyle;
    return i === "richText" ? (s ? "" : h) + (a ? "" : py(e, v, _)) + (o ? "" : pT(e, d, y, p, m)) : dl(n, (s ? "" : h) + (a ? "" : vT(v, !s, _)) + (o ? "" : dT(d, y, p, m)), r);
  }
}
function Kc(e, t, r, n, i, a) {
  if (e) {
    var o = vy(e), s = {
      useUTC: i,
      renderMode: r,
      orderMode: n,
      markupStyleCreator: t,
      valueFormatter: e.valueFormatter
    };
    return o(s, e, 0, a);
  }
}
function cT(e) {
  return {
    html: uT[e],
    richText: lT[e]
  };
}
function dl(e, t, r) {
  var n = '<div style="clear:both"></div>', i = "margin: " + r + "px 0 0", a = hy(e);
  return '<div style="' + i + ";" + a + ';">' + t + n + "</div>";
}
function vT(e, t, r) {
  var n = t ? "margin-left:2px" : "";
  return '<span style="' + r + ";" + n + '">' + Pt(e) + "</span>";
}
function dT(e, t, r, n) {
  var i = r ? "10px" : "20px", a = t ? "float:right;margin-left:" + i : "";
  return e = k(e) ? e : [e], '<span style="' + a + ";" + n + '">' + F(e, function(o) {
    return Pt(o);
  }).join("&nbsp;&nbsp;") + "</span>";
}
function py(e, t, r) {
  return e.markupStyleCreator.wrapRichTextStyle(t, r);
}
function pT(e, t, r, n, i) {
  var a = [i], o = n ? 10 : 20;
  return r && a.push({
    padding: [0, 0, 0, o],
    align: "right"
  }), e.markupStyleCreator.wrapRichTextStyle(k(t) ? t.join("  ") : t, a);
}
function gT(e, t) {
  var r = e.getData().getItemVisual(t, "style"), n = r[e.visualDrawType];
  return Ni(n);
}
function gy(e, t) {
  var r = e.get("padding");
  return r ?? (t === "richText" ? [8, 10] : 10);
}
var su = (
  /** @class */
  (function() {
    function e() {
      this.richTextStyles = {}, this._nextStyleNameId = Fp();
    }
    return e.prototype._generateStyleName = function() {
      return "__EC_aUTo_" + this._nextStyleNameId++;
    }, e.prototype.makeTooltipMarker = function(t, r, n) {
      var i = n === "richText" ? this._generateStyleName() : null, a = Wg({
        color: r,
        type: t,
        renderMode: n,
        markerId: i
      });
      return B(a) ? a : (this.richTextStyles[i] = a.style, a.content);
    }, e.prototype.wrapRichTextStyle = function(t, r) {
      var n = {};
      k(r) ? x(r, function(a) {
        return E(n, a);
      }) : E(n, r);
      var i = this._generateStyleName();
      return this.richTextStyles[i] = n, "{" + i + "|" + t + "}";
    }, e;
  })()
);
function yT(e) {
  var t = e.series, r = e.dataIndex, n = e.multipleSeries, i = t.getData(), a = i.mapDimensionsAll("defaultedTooltip"), o = a.length, s = t.getRawValue(r), u = k(s), l = gT(t, r), f, h, v, c;
  if (o > 1 || u && !o) {
    var d = _T(s, t, r, a, l);
    f = d.inlineValues, h = d.inlineValueTypes, v = d.blocks, c = d.inlineValues[0];
  } else if (o) {
    var y = i.getDimensionInfo(a[0]);
    c = f = wo(i, r, a[0]), h = y.type;
  } else
    c = f = u ? s[0] : s;
  var p = jl(t), g = p && t.name || "", _ = i.getName(r), m = n ? g : _;
  return Gi("section", {
    header: g,
    // When series name is not specified, do not show a header line with only '-'.
    // This case always happens in tooltip.trigger: 'item'.
    noHeader: n || !p,
    sortParam: c,
    blocks: [Gi("nameValue", {
      markerType: "item",
      markerColor: l,
      // Do not mix display seriesName and itemName in one tooltip,
      // which might confuses users.
      name: m,
      // name dimension might be auto assigned, where the name might
      // be not readable. So we check trim here.
      noName: !pe(m),
      value: f,
      valueType: h,
      dataIndex: r
    })].concat(v || [])
  });
}
function _T(e, t, r, n, i) {
  var a = t.getData(), o = Pe(e, function(h, v, c) {
    var d = a.getDimensionInfo(c);
    return h = h || d && d.tooltip !== !1 && d.displayName != null;
  }, !1), s = [], u = [], l = [];
  n.length ? x(n, function(h) {
    f(wo(a, r, h), h);
  }) : x(e, f);
  function f(h, v) {
    var c = a.getDimensionInfo(v);
    !c || c.otherDims.tooltip === !1 || (o ? l.push(Gi("nameValue", {
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
var je = gt();
function Pa(e, t) {
  return e.getName(t) || e.getId(t);
}
var mT = "__universalTransitionEnabled", Ae = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r._selectedDataIndicesMap = {}, r;
    }
    return t.prototype.init = function(r, n, i) {
      this.seriesIndex = this.componentIndex, this.dataTask = Mi({
        count: wT,
        reset: bT
      }), this.dataTask.context = {
        model: this
      }, this.mergeDefaultAndTheme(r, i);
      var a = je(this).sourceManager = new fy(this);
      a.prepareSource();
      var o = this.getInitialData(r, i);
      Jc(o, this), this.dataTask.context.data = o, je(this).dataBeforeProcessed = o, Qc(this), this._initSelectedMapFromData(o);
    }, t.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = So(this), a = i ? bf(r) : {}, o = this.subType;
      Q.hasClass(o) && (o += "Series"), it(r, n.getTheme().get(this.subType)), it(r, this.getDefaultOption()), Fh(r, "label", ["show"]), this.fillDataTextStyle(r.data), i && zi(r, a, i);
    }, t.prototype.mergeOption = function(r, n) {
      r = it(this.option, r, !0), this.fillDataTextStyle(r.data);
      var i = So(this);
      i && zi(this.option, r, i);
      var a = je(this).sourceManager;
      a.dirty(), a.prepareSource();
      var o = this.getInitialData(r, n);
      Jc(o, this), this.dataTask.dirty(), this.dataTask.context.data = o, je(this).dataBeforeProcessed = o, Qc(this), this._initSelectedMapFromData(o);
    }, t.prototype.fillDataTextStyle = function(r) {
      if (r && !kt(r))
        for (var n = ["show"], i = 0; i < r.length; i++)
          r[i] && r[i].label && Fh(r[i], "label", n);
    }, t.prototype.getInitialData = function(r, n) {
    }, t.prototype.appendData = function(r) {
      var n = this.getRawData();
      n.appendData(r.data);
    }, t.prototype.getData = function(r) {
      var n = pl(this);
      if (n) {
        var i = n.context.data;
        return r == null || !i.getLinkedData ? i : i.getLinkedData(r);
      } else
        return je(this).data;
    }, t.prototype.getAllData = function() {
      var r = this.getData();
      return r && r.getLinkedDataAll ? r.getLinkedDataAll() : [{
        data: r
      }];
    }, t.prototype.setData = function(r) {
      var n = pl(this);
      if (n) {
        var i = n.context;
        i.outputData = r, n !== this.dataTask && (i.data = r);
      }
      je(this).data = r;
    }, t.prototype.getEncode = function() {
      var r = this.get("encode", !0);
      if (r)
        return W(r);
    }, t.prototype.getSourceManager = function() {
      return je(this).sourceManager;
    }, t.prototype.getSource = function() {
      return this.getSourceManager().getSource();
    }, t.prototype.getRawData = function() {
      return je(this).dataBeforeProcessed;
    }, t.prototype.getColorBy = function() {
      var r = this.get("colorBy");
      return r || "series";
    }, t.prototype.isColorBySeries = function() {
      return this.getColorBy() === "series";
    }, t.prototype.getBaseAxis = function() {
      var r = this.coordinateSystem;
      return r && r.getBaseAxis && r.getBaseAxis();
    }, t.prototype.formatTooltip = function(r, n, i) {
      return yT({
        series: this,
        dataIndex: r,
        multipleSeries: n
      });
    }, t.prototype.isAnimationEnabled = function() {
      var r = this.ecModel;
      if (z.node && !(r && r.ssr))
        return !1;
      var n = this.getShallow("animation");
      return n && this.getData().count() > this.getShallow("animationThreshold") && (n = !1), !!n;
    }, t.prototype.restoreData = function() {
      this.dataTask.dirty();
    }, t.prototype.getColorFromPalette = function(r, n, i) {
      var a = this.ecModel, o = Tf.prototype.getColorFromPalette.call(this, r, n, i);
      return o || (o = a.getColorFromPalette(r, n, i)), o;
    }, t.prototype.coordDimToDataDim = function(r) {
      return this.getRawData().mapDimensionsAll(r);
    }, t.prototype.getProgressive = function() {
      return this.get("progressive");
    }, t.prototype.getProgressiveThreshold = function() {
      return this.get("progressiveThreshold");
    }, t.prototype.select = function(r, n) {
      this._innerSelect(this.getData(n), r);
    }, t.prototype.unselect = function(r, n) {
      var i = this.option.selectedMap;
      if (i) {
        var a = this.option.selectedMode, o = this.getData(n);
        if (a === "series" || i === "all") {
          this.option.selectedMap = {}, this._selectedDataIndicesMap = {};
          return;
        }
        for (var s = 0; s < r.length; s++) {
          var u = r[s], l = Pa(o, u);
          i[l] = !1, this._selectedDataIndicesMap[l] = -1;
        }
      }
    }, t.prototype.toggleSelect = function(r, n) {
      for (var i = [], a = 0; a < r.length; a++)
        i[0] = r[a], this.isSelected(r[a], n) ? this.unselect(i, n) : this.select(i, n);
    }, t.prototype.getSelectedDataIndices = function() {
      if (this.option.selectedMap === "all")
        return [].slice.call(this.getData().getIndices());
      for (var r = this._selectedDataIndicesMap, n = ct(r), i = [], a = 0; a < n.length; a++) {
        var o = r[n[a]];
        o >= 0 && i.push(o);
      }
      return i;
    }, t.prototype.isSelected = function(r, n) {
      var i = this.option.selectedMap;
      if (!i)
        return !1;
      var a = this.getData(n);
      return (i === "all" || i[Pa(a, r)]) && !a.getItemModel(r).get(["select", "disabled"]);
    }, t.prototype.isUniversalTransitionEnabled = function() {
      if (this[mT])
        return !0;
      var r = this.option.universalTransition;
      return r ? r === !0 ? !0 : r && r.enabled : !1;
    }, t.prototype._innerSelect = function(r, n) {
      var i, a, o = this.option, s = o.selectedMode, u = n.length;
      if (!(!s || !u)) {
        if (s === "series")
          o.selectedMap = "all";
        else if (s === "multiple") {
          N(o.selectedMap) || (o.selectedMap = {});
          for (var l = o.selectedMap, f = 0; f < u; f++) {
            var h = n[f], v = Pa(r, h);
            l[v] = !0, this._selectedDataIndicesMap[v] = r.getRawIndex(h);
          }
        } else if (s === "single" || s === !0) {
          var c = n[u - 1], v = Pa(r, c);
          o.selectedMap = (i = {}, i[v] = !0, i), this._selectedDataIndicesMap = (a = {}, a[v] = r.getRawIndex(c), a);
        }
      }
    }, t.prototype._initSelectedMapFromData = function(r) {
      if (!this.option.selectedMap) {
        var n = [];
        r.hasItemOption && r.each(function(i) {
          var a = r.getRawDataItem(i);
          a && a.selected && n.push(i);
        }), n.length > 0 && this._innerSelect(r, n);
      }
    }, t.registerClass = function(r) {
      return Q.registerClass(r);
    }, t.protoInitialize = (function() {
      var r = t.prototype;
      r.type = "series.__base__", r.seriesIndex = 0, r.ignoreStyleOnData = !1, r.hasSymbolVisual = !1, r.defaultSymbol = "circle", r.visualStyleAccessPath = "itemStyle", r.visualDrawType = "fill";
    })(), t;
  })(Q)
);
ye(Ae, Wb);
ye(Ae, Tf);
Wp(Ae, Q);
function Qc(e) {
  var t = e.name;
  jl(e) || (e.name = ST(e) || t);
}
function ST(e) {
  var t = e.getRawData(), r = t.mapDimensionsAll("seriesName"), n = [];
  return x(r, function(i) {
    var a = t.getDimensionInfo(i);
    a.displayName && n.push(a.displayName);
  }), n.join(" ");
}
function wT(e) {
  return e.model.getRawData().count();
}
function bT(e) {
  var t = e.model;
  return t.setData(t.getRawData().cloneShallow()), TT;
}
function TT(e, t) {
  t.outputData && e.end > t.outputData.count() && t.model.getRawData().cloneShallow(t.outputData);
}
function Jc(e, t) {
  x(ep(e.CHANGABLE_METHODS, e.DOWNSAMPLE_METHODS), function(r) {
    e.wrapMethod(r, pt(xT, t));
  });
}
function xT(e, t) {
  var r = pl(e);
  return r && r.setOutputEnd((t || this).count()), t;
}
function pl(e) {
  var t = (e.ecModel || {}).scheduler, r = t && t.getPipeline(e.uid);
  if (r) {
    var n = r.currentTask;
    if (n) {
      var i = n.agentStubMap;
      i && (n = i.get(e.uid));
    }
    return n;
  }
}
var $t = (
  /** @class */
  (function() {
    function e() {
      this.group = new oe(), this.uid = Qo("viewComponent");
    }
    return e.prototype.init = function(t, r) {
    }, e.prototype.render = function(t, r, n, i) {
    }, e.prototype.dispose = function(t, r) {
    }, e.prototype.updateView = function(t, r, n, i) {
    }, e.prototype.updateLayout = function(t, r, n, i) {
    }, e.prototype.updateVisual = function(t, r, n, i) {
    }, e.prototype.toggleBlurSeries = function(t, r, n) {
    }, e.prototype.eachRendered = function(t) {
      var r = this.group;
      r && r.traverse(t);
    }, e;
  })()
);
Ql($t);
No($t);
function yy() {
  var e = gt();
  return function(t) {
    var r = e(t), n = t.pipelineContext, i = !!r.large, a = !!r.progressiveRender, o = r.large = !!(n && n.large), s = r.progressiveRender = !!(n && n.progressiveRender);
    return (i !== o || a !== s) && "reset";
  };
}
var _y = gt(), CT = yy(), De = (
  /** @class */
  (function() {
    function e() {
      this.group = new oe(), this.uid = Qo("viewChart"), this.renderTask = Mi({
        plan: MT,
        reset: DT
      }), this.renderTask.context = {
        view: this
      };
    }
    return e.prototype.init = function(t, r) {
    }, e.prototype.render = function(t, r, n, i) {
    }, e.prototype.highlight = function(t, r, n, i) {
      var a = t.getData(i && i.dataType);
      a && ev(a, i, "emphasis");
    }, e.prototype.downplay = function(t, r, n, i) {
      var a = t.getData(i && i.dataType);
      a && ev(a, i, "normal");
    }, e.prototype.remove = function(t, r) {
      this.group.removeAll();
    }, e.prototype.dispose = function(t, r) {
    }, e.prototype.updateView = function(t, r, n, i) {
      this.render(t, r, n, i);
    }, e.prototype.updateLayout = function(t, r, n, i) {
      this.render(t, r, n, i);
    }, e.prototype.updateVisual = function(t, r, n, i) {
      this.render(t, r, n, i);
    }, e.prototype.eachRendered = function(t) {
      Ag(this.group, t);
    }, e.markUpdateMethod = function(t, r) {
      _y(t).updateMethod = r;
    }, e.protoInitialize = (function() {
      var t = e.prototype;
      t.type = "chart";
    })(), e;
  })()
);
function tv(e, t, r) {
  e && al(e) && (t === "emphasis" ? el : rl)(e, r);
}
function ev(e, t, r) {
  var n = Ki(e, t), i = t && t.highlightKey != null ? qS(t.highlightKey) : null;
  n != null ? x(Rt(n), function(a) {
    tv(e.getItemGraphicEl(a), r, i);
  }) : e.eachItemGraphicEl(function(a) {
    tv(a, r, i);
  });
}
Ql(De);
No(De);
function MT(e) {
  return CT(e.model);
}
function DT(e) {
  var t = e.model, r = e.ecModel, n = e.api, i = e.payload, a = t.pipelineContext.progressiveRender, o = e.view, s = i && _y(i).updateMethod, u = a ? "incrementalPrepareRender" : s && o[s] ? s : "render";
  return u !== "render" && o[u](t, r, n, i), PT[u];
}
var PT = {
  incrementalPrepareRender: {
    progress: function(e, t) {
      t.view.incrementalRender(e, t.model, t.ecModel, t.api, t.payload);
    }
  },
  render: {
    // Put view.render in `progress` to support appendData. But in this case
    // view.render should not be called in reset, otherwise it will be called
    // twise. Use `forceFirstProgress` to make sure that view.render is called
    // in any cases.
    forceFirstProgress: !0,
    progress: function(e, t) {
      t.view.render(t.model, t.ecModel, t.api, t.payload);
    }
  }
}, bo = "\0__throttleOriginMethod", rv = "\0__throttleRate", nv = "\0__throttleType";
function Pf(e, t, r) {
  var n, i = 0, a = 0, o = null, s, u, l, f;
  t = t || 0;
  function h() {
    a = (/* @__PURE__ */ new Date()).getTime(), o = null, e.apply(u, l || []);
  }
  var v = function() {
    for (var c = [], d = 0; d < arguments.length; d++)
      c[d] = arguments[d];
    n = (/* @__PURE__ */ new Date()).getTime(), u = this, l = c;
    var y = f || t, p = f || r;
    f = null, s = n - (p ? i : a) - y, clearTimeout(o), p ? o = setTimeout(h, y) : s >= 0 ? h() : o = setTimeout(h, -s), i = n;
  };
  return v.clear = function() {
    o && (clearTimeout(o), o = null);
  }, v.debounceNextCall = function(c) {
    f = c;
  }, v;
}
function my(e, t, r, n) {
  var i = e[t];
  if (i) {
    var a = i[bo] || i, o = i[nv], s = i[rv];
    if (s !== r || o !== n) {
      if (r == null || !n)
        return e[t] = a;
      i = e[t] = Pf(a, r, n === "debounce"), i[bo] = a, i[nv] = n, i[rv] = r;
    }
    return i;
  }
}
function gl(e, t) {
  var r = e[t];
  r && r[bo] && (r.clear && r.clear(), e[t] = r[bo]);
}
var iv = gt(), av = {
  itemStyle: Bi(Eg, !0),
  lineStyle: Bi(Rg, !0)
}, IT = {
  lineStyle: "stroke",
  itemStyle: "fill"
};
function Sy(e, t) {
  var r = e.visualStyleMapper || av[t];
  return r || (console.warn("Unknown style type '" + t + "'."), av.itemStyle);
}
function wy(e, t) {
  var r = e.visualDrawType || IT[t];
  return r || (console.warn("Unknown style type '" + t + "'."), "fill");
}
var AT = {
  createOnAllSeries: !0,
  performRawSeries: !0,
  reset: function(e, t) {
    var r = e.getData(), n = e.visualStyleAccessPath || "itemStyle", i = e.getModel(n), a = Sy(e, n), o = a(i), s = i.getShallow("decal");
    s && (r.setVisual("decal", s), s.dirty = !0);
    var u = wy(e, n), l = o[u], f = U(l) ? l : null, h = o.fill === "auto" || o.stroke === "auto";
    if (!o[u] || f || h) {
      var v = e.getColorFromPalette(
        // TODO series count changed.
        e.name,
        null,
        t.getSeriesCount()
      );
      o[u] || (o[u] = v, r.setVisual("colorFromPalette", !0)), o.fill = o.fill === "auto" || U(o.fill) ? v : o.fill, o.stroke = o.stroke === "auto" || U(o.stroke) ? v : o.stroke;
    }
    if (r.setVisual("style", o), r.setVisual("drawType", u), !t.isSeriesFiltered(e) && f)
      return r.setVisual("colorFromPalette", !1), {
        dataEach: function(c, d) {
          var y = e.getDataParams(d), p = E({}, o);
          p[u] = f(y), c.setItemVisual(d, "style", p);
        }
      };
  }
}, ti = new st(), LT = {
  createOnAllSeries: !0,
  performRawSeries: !0,
  reset: function(e, t) {
    if (!(e.ignoreStyleOnData || t.isSeriesFiltered(e))) {
      var r = e.getData(), n = e.visualStyleAccessPath || "itemStyle", i = Sy(e, n), a = r.getVisual("drawType");
      return {
        dataEach: r.hasItemOption ? function(o, s) {
          var u = o.getRawDataItem(s);
          if (u && u[n]) {
            ti.option = u[n];
            var l = i(ti), f = o.ensureUniqueItemVisual(s, "style");
            E(f, l), ti.option.decal && (o.setItemVisual(s, "decal", ti.option.decal), ti.option.decal.dirty = !0), a in l && o.setItemVisual(s, "colorFromPalette", !1);
          }
        } : null
      };
    }
  }
}, RT = {
  performRawSeries: !0,
  overallReset: function(e) {
    var t = W();
    e.eachSeries(function(r) {
      var n = r.getColorBy();
      if (!r.isColorBySeries()) {
        var i = r.type + "-" + n, a = t.get(i);
        a || (a = {}, t.set(i, a)), iv(r).scope = a;
      }
    }), e.eachSeries(function(r) {
      if (!(r.isColorBySeries() || e.isSeriesFiltered(r))) {
        var n = r.getRawData(), i = {}, a = r.getData(), o = iv(r).scope, s = r.visualStyleAccessPath || "itemStyle", u = wy(r, s);
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
}, Ia = Math.PI;
function ET(e, t) {
  t = t || {}, ft(t, {
    text: "loading",
    textColor: "#000",
    fontSize: 12,
    fontWeight: "normal",
    fontStyle: "normal",
    fontFamily: "sans-serif",
    maskColor: "rgba(255, 255, 255, 0.8)",
    showSpinner: !0,
    color: "#5470c6",
    spinnerRadius: 10,
    lineWidth: 5,
    zlevel: 0
  });
  var r = new oe(), n = new Et({
    style: {
      fill: t.maskColor
    },
    zlevel: t.zlevel,
    z: 1e4
  });
  r.add(n);
  var i = new Bt({
    style: {
      text: t.text,
      fill: t.textColor,
      fontSize: t.fontSize,
      fontWeight: t.fontWeight,
      fontStyle: t.fontStyle,
      fontFamily: t.fontFamily
    },
    zlevel: t.zlevel,
    z: 10001
  }), a = new Et({
    style: {
      fill: "none"
    },
    textContent: i,
    textConfig: {
      position: "right",
      distance: 10
    },
    zlevel: t.zlevel,
    z: 10001
  });
  r.add(a);
  var o;
  return t.showSpinner && (o = new ea({
    shape: {
      startAngle: -Ia / 2,
      endAngle: -Ia / 2 + 0.1,
      r: t.spinnerRadius
    },
    style: {
      stroke: t.color,
      lineCap: "round",
      lineWidth: t.lineWidth
    },
    zlevel: t.zlevel,
    z: 10001
  }), o.animateShape(!0).when(1e3, {
    endAngle: Ia * 3 / 2
  }).start("circularInOut"), o.animateShape(!0).when(1e3, {
    startAngle: Ia * 3 / 2
  }).delay(300).start("circularInOut"), r.add(o)), r.resize = function() {
    var s = i.getBoundingRect().width, u = t.showSpinner ? t.spinnerRadius : 0, l = (e.getWidth() - u * 2 - (t.showSpinner && s ? 10 : 0) - s) / 2 - (t.showSpinner && s ? 0 : 5 + s / 2) + (t.showSpinner ? 0 : s / 2) + (s ? 0 : u), f = e.getHeight() / 2;
    t.showSpinner && o.setShape({
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
      width: e.getWidth(),
      height: e.getHeight()
    });
  }, r.resize(), r;
}
var by = (
  /** @class */
  (function() {
    function e(t, r, n, i) {
      this._stageTaskMap = W(), this.ecInstance = t, this.api = r, n = this._dataProcessorHandlers = n.slice(), i = this._visualHandlers = i.slice(), this._allHandlers = n.concat(i);
    }
    return e.prototype.restoreData = function(t, r) {
      t.restoreData(r), this._stageTaskMap.each(function(n) {
        var i = n.overallTask;
        i && i.dirty();
      });
    }, e.prototype.getPerformArgs = function(t, r) {
      if (t.__pipeline) {
        var n = this._pipelineMap.get(t.__pipeline.id), i = n.context, a = !r && n.progressiveEnabled && (!i || i.progressiveRender) && t.__idxInPipeline > n.blockIndex, o = a ? n.step : null, s = i && i.modDataCount, u = s != null ? Math.ceil(s / o) : null;
        return {
          step: o,
          modBy: u,
          modDataCount: s
        };
      }
    }, e.prototype.getPipeline = function(t) {
      return this._pipelineMap.get(t);
    }, e.prototype.updateStreamModes = function(t, r) {
      var n = this._pipelineMap.get(t.uid), i = t.getData(), a = i.count(), o = n.progressiveEnabled && r.incrementalPrepareRender && a >= n.threshold, s = t.get("large") && a >= t.get("largeThreshold"), u = t.get("progressiveChunkMode") === "mod" ? a : null;
      t.pipelineContext = n.context = {
        progressiveRender: o,
        modDataCount: u,
        large: s
      };
    }, e.prototype.restorePipelines = function(t) {
      var r = this, n = r._pipelineMap = W();
      t.eachSeries(function(i) {
        var a = i.getProgressive(), o = i.uid;
        n.set(o, {
          id: o,
          head: null,
          tail: null,
          threshold: i.getProgressiveThreshold(),
          progressiveEnabled: a && !(i.preventIncremental && i.preventIncremental()),
          blockIndex: -1,
          step: Math.round(a || 700),
          count: 0
        }), r._pipe(i, i.dataTask);
      });
    }, e.prototype.prepareStageTasks = function() {
      var t = this._stageTaskMap, r = this.api.getModel(), n = this.api;
      x(this._allHandlers, function(i) {
        var a = t.get(i.uid) || t.set(i.uid, {}), o = "";
        Ie(!(i.reset && i.overallReset), o), i.reset && this._createSeriesStageTask(i, a, r, n), i.overallReset && this._createOverallStageTask(i, a, r, n);
      }, this);
    }, e.prototype.prepareView = function(t, r, n, i) {
      var a = t.renderTask, o = a.context;
      o.model = r, o.ecModel = n, o.api = i, a.__block = !t.incrementalPrepareRender, this._pipe(r, a);
    }, e.prototype.performDataProcessorTasks = function(t, r) {
      this._performStageTasks(this._dataProcessorHandlers, t, r, {
        block: !0
      });
    }, e.prototype.performVisualTasks = function(t, r, n) {
      this._performStageTasks(this._visualHandlers, t, r, n);
    }, e.prototype._performStageTasks = function(t, r, n, i) {
      i = i || {};
      var a = !1, o = this;
      x(t, function(u, l) {
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
            var _ = o.getPerformArgs(p, i.block);
            _.skip = !u.performRawSeries && r.isSeriesFiltered(p.context.model), o.updatePayload(p, n), p.perform(_) && (a = !0);
          });
        }
      });
      function s(u, l) {
        return u.setDirty && (!u.dirtyMap || u.dirtyMap.get(l.__pipeline.id));
      }
      this.unfinished = a || this.unfinished;
    }, e.prototype.performSeriesTasks = function(t) {
      var r;
      t.eachSeries(function(n) {
        r = n.dataTask.perform() || r;
      }), this.unfinished = r || this.unfinished;
    }, e.prototype.plan = function() {
      this._pipelineMap.each(function(t) {
        var r = t.tail;
        do {
          if (r.__block) {
            t.blockIndex = r.__idxInPipeline;
            break;
          }
          r = r.getUpstream();
        } while (r);
      });
    }, e.prototype.updatePayload = function(t, r) {
      r !== "remain" && (t.context.payload = r);
    }, e.prototype._createSeriesStageTask = function(t, r, n, i) {
      var a = this, o = r.seriesTaskMap, s = r.seriesTaskMap = W(), u = t.seriesType, l = t.getTargetSeries;
      t.createOnAllSeries ? n.eachRawSeries(f) : u ? n.eachRawSeriesByType(u, f) : l && l(n, i).each(f);
      function f(h) {
        var v = h.uid, c = s.set(v, o && o.get(v) || Mi({
          plan: NT,
          reset: zT,
          count: GT
        }));
        c.context = {
          model: h,
          ecModel: n,
          api: i,
          // PENDING: `useClearVisual` not used?
          useClearVisual: t.isVisual && !t.isLayout,
          plan: t.plan,
          reset: t.reset,
          scheduler: a
        }, a._pipe(h, c);
      }
    }, e.prototype._createOverallStageTask = function(t, r, n, i) {
      var a = this, o = r.overallTask = r.overallTask || Mi({
        reset: OT
      });
      o.context = {
        ecModel: n,
        api: i,
        overallReset: t.overallReset,
        scheduler: a
      };
      var s = o.agentStubMap, u = o.agentStubMap = W(), l = t.seriesType, f = t.getTargetSeries, h = !0, v = !1, c = "";
      Ie(!t.createOnAllSeries, c), l ? n.eachRawSeriesByType(l, d) : f ? f(n, i).each(d) : (h = !1, x(n.getSeries(), d));
      function d(y) {
        var p = y.uid, g = u.set(p, s && s.get(p) || // When the result of `getTargetSeries` changed, the overallTask
        // should be set as dirty and re-performed.
        (v = !0, Mi({
          reset: kT,
          onDirty: FT
        })));
        g.context = {
          model: y,
          overallProgress: h
          // FIXME:TS never used, so comment it
          // modifyOutputEnd: modifyOutputEnd
        }, g.agent = o, g.__block = h, a._pipe(y, g);
      }
      v && o.dirty();
    }, e.prototype._pipe = function(t, r) {
      var n = t.uid, i = this._pipelineMap.get(n);
      !i.head && (i.head = r), i.tail && i.tail.pipe(r), i.tail = r, r.__idxInPipeline = i.count++, r.__pipeline = i;
    }, e.wrapStageHandler = function(t, r) {
      return U(t) && (t = {
        overallReset: t,
        seriesType: VT(t)
      }), t.uid = Qo("stageHandler"), r && (t.visualType = r), t;
    }, e;
  })()
);
function OT(e) {
  e.overallReset(e.ecModel, e.api, e.payload);
}
function kT(e) {
  return e.overallProgress && BT;
}
function BT() {
  this.agent.dirty(), this.getDownstream().dirty();
}
function FT() {
  this.agent && this.agent.dirty();
}
function NT(e) {
  return e.plan ? e.plan(e.model, e.ecModel, e.api, e.payload) : null;
}
function zT(e) {
  e.useClearVisual && e.data.clearAllVisual();
  var t = e.resetDefines = Rt(e.reset(e.model, e.ecModel, e.api, e.payload));
  return t.length > 1 ? F(t, function(r, n) {
    return Ty(n);
  }) : HT;
}
var HT = Ty(0);
function Ty(e) {
  return function(t, r) {
    var n = r.data, i = r.resetDefines[e];
    if (i && i.dataEach)
      for (var a = t.start; a < t.end; a++)
        i.dataEach(n, a);
    else i && i.progress && i.progress(t, n);
  };
}
function GT(e) {
  return e.data.count();
}
function VT(e) {
  To = null;
  try {
    e(Vi, xy);
  } catch {
  }
  return To;
}
var Vi = {}, xy = {}, To;
Cy(Vi, xf);
Cy(xy, jg);
Vi.eachSeriesByType = Vi.eachRawSeriesByType = function(e) {
  To = e;
};
Vi.eachComponent = function(e) {
  e.mainType === "series" && e.subType && (To = e.subType);
};
function Cy(e, t) {
  for (var r in t.prototype)
    e[r] = Lt;
}
var ov = ["#37A2DA", "#32C5E9", "#67E0E3", "#9FE6B8", "#FFDB5C", "#ff9f7f", "#fb7293", "#E062AE", "#E690D1", "#e7bcf3", "#9d96f5", "#8378EA", "#96BFFF"];
const WT = {
  color: ov,
  colorLayer: [["#37A2DA", "#ffd85c", "#fd7b5f"], ["#37A2DA", "#67E0E3", "#FFDB5C", "#ff9f7f", "#E062AE", "#9d96f5"], ["#37A2DA", "#32C5E9", "#9FE6B8", "#FFDB5C", "#ff9f7f", "#fb7293", "#e7bcf3", "#8378EA", "#96BFFF"], ov]
};
var bt = "#B9B8CE", sv = "#100C2A", Aa = function() {
  return {
    axisLine: {
      lineStyle: {
        color: bt
      }
    },
    splitLine: {
      lineStyle: {
        color: "#484753"
      }
    },
    splitArea: {
      areaStyle: {
        color: ["rgba(255,255,255,0.02)", "rgba(255,255,255,0.05)"]
      }
    },
    minorSplitLine: {
      lineStyle: {
        color: "#20203B"
      }
    }
  };
}, uv = ["#4992ff", "#7cffb2", "#fddd60", "#ff6e76", "#58d9f9", "#05c091", "#ff8a45", "#8d48e3", "#dd79ff"], My = {
  darkMode: !0,
  color: uv,
  backgroundColor: sv,
  axisPointer: {
    lineStyle: {
      color: "#817f91"
    },
    crossStyle: {
      color: "#817f91"
    },
    label: {
      // TODO Contrast of label backgorundColor
      color: "#fff"
    }
  },
  legend: {
    textStyle: {
      color: bt
    },
    pageTextStyle: {
      color: bt
    }
  },
  textStyle: {
    color: bt
  },
  title: {
    textStyle: {
      color: "#EEF1FA"
    },
    subtextStyle: {
      color: "#B9B8CE"
    }
  },
  toolbox: {
    iconStyle: {
      borderColor: bt
    }
  },
  dataZoom: {
    borderColor: "#71708A",
    textStyle: {
      color: bt
    },
    brushStyle: {
      color: "rgba(135,163,206,0.3)"
    },
    handleStyle: {
      color: "#353450",
      borderColor: "#C5CBE3"
    },
    moveHandleStyle: {
      color: "#B0B6C3",
      opacity: 0.3
    },
    fillerColor: "rgba(135,163,206,0.2)",
    emphasis: {
      handleStyle: {
        borderColor: "#91B7F2",
        color: "#4D587D"
      },
      moveHandleStyle: {
        color: "#636D9A",
        opacity: 0.7
      }
    },
    dataBackground: {
      lineStyle: {
        color: "#71708A",
        width: 1
      },
      areaStyle: {
        color: "#71708A"
      }
    },
    selectedDataBackground: {
      lineStyle: {
        color: "#87A3CE"
      },
      areaStyle: {
        color: "#87A3CE"
      }
    }
  },
  visualMap: {
    textStyle: {
      color: bt
    }
  },
  timeline: {
    lineStyle: {
      color: bt
    },
    label: {
      color: bt
    },
    controlStyle: {
      color: bt,
      borderColor: bt
    }
  },
  calendar: {
    itemStyle: {
      color: sv
    },
    dayLabel: {
      color: bt
    },
    monthLabel: {
      color: bt
    },
    yearLabel: {
      color: bt
    }
  },
  timeAxis: Aa(),
  logAxis: Aa(),
  valueAxis: Aa(),
  categoryAxis: Aa(),
  line: {
    symbol: "circle"
  },
  graph: {
    color: uv
  },
  gauge: {
    title: {
      color: bt
    },
    axisLine: {
      lineStyle: {
        color: [[1, "rgba(207,212,219,0.2)"]]
      }
    },
    axisLabel: {
      color: bt
    },
    detail: {
      color: "#EEF1FA"
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
  }
};
My.categoryAxis.splitLine.show = !1;
var UT = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.normalizeQuery = function(t) {
      var r = {}, n = {}, i = {};
      if (B(t)) {
        var a = xe(t);
        r.mainType = a.main || null, r.subType = a.sub || null;
      } else {
        var o = ["Index", "Name", "Id"], s = {
          name: 1,
          dataIndex: 1,
          dataType: 1
        };
        x(t, function(u, l) {
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
    }, e.prototype.filter = function(t, r) {
      var n = this.eventInfo;
      if (!n)
        return !0;
      var i = n.targetEl, a = n.packedEvent, o = n.model, s = n.view;
      if (!o || !s)
        return !0;
      var u = r.cptQuery, l = r.dataQuery;
      return f(u, o, "mainType") && f(u, o, "subType") && f(u, o, "index", "componentIndex") && f(u, o, "name") && f(u, o, "id") && f(l, a, "name") && f(l, a, "dataIndex") && f(l, a, "dataType") && (!s.filterForExposedEvent || s.filterForExposedEvent(t, r.otherQuery, i, a));
      function f(h, v, c, d) {
        return h[c] == null || v[d || c] === h[c];
      }
    }, e.prototype.afterTrigger = function() {
      this.eventInfo = null;
    }, e;
  })()
), yl = ["symbol", "symbolSize", "symbolRotate", "symbolOffset"], lv = yl.concat(["symbolKeepAspect"]), YT = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(e, t) {
    var r = e.getData();
    if (e.legendIcon && r.setVisual("legendIcon", e.legendIcon), !e.hasSymbolVisual)
      return;
    for (var n = {}, i = {}, a = !1, o = 0; o < yl.length; o++) {
      var s = yl[o], u = e.get(s);
      U(u) ? (a = !0, i[s] = u) : n[s] = u;
    }
    if (n.symbol = n.symbol || e.defaultSymbol, r.setVisual(E({
      legendIcon: e.legendIcon || n.symbol,
      symbolKeepAspect: e.get("symbolKeepAspect")
    }, n)), t.isSeriesFiltered(e))
      return;
    var l = ct(i);
    function f(h, v) {
      for (var c = e.getRawValue(v), d = e.getDataParams(v), y = 0; y < l.length; y++) {
        var p = l[y];
        h.setItemVisual(v, p, i[p](c, d));
      }
    }
    return {
      dataEach: a ? f : null
    };
  }
}, qT = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(e, t) {
    if (!e.hasSymbolVisual || t.isSeriesFiltered(e))
      return;
    var r = e.getData();
    function n(i, a) {
      for (var o = i.getItemModel(a), s = 0; s < lv.length; s++) {
        var u = lv[s], l = o.getShallow(u, !0);
        l != null && i.setItemVisual(a, u, l);
      }
    }
    return {
      dataEach: r.hasItemOption ? n : null
    };
  }
};
function XT(e, t, r) {
  switch (r) {
    case "color":
      var n = e.getItemVisual(t, "style");
      return n[e.getVisual("drawType")];
    case "opacity":
      return e.getItemVisual(t, "style").opacity;
    case "symbol":
    case "symbolSize":
    case "liftZ":
      return e.getItemVisual(t, r);
  }
}
function $T(e, t) {
  switch (t) {
    case "color":
      var r = e.getVisual("style");
      return r[e.getVisual("drawType")];
    case "opacity":
      return e.getVisual("style").opacity;
    case "symbol":
    case "symbolSize":
    case "liftZ":
      return e.getVisual(t);
  }
}
function pn(e, t, r, n, i) {
  var a = e + t;
  r.isSilent(a) || n.eachComponent({
    mainType: "series",
    subType: "pie"
  }, function(o) {
    for (var s = o.seriesIndex, u = o.option.selectedMap, l = i.selected, f = 0; f < l.length; f++)
      if (l[f].seriesIndex === s) {
        var h = o.getData(), v = Ki(h, i.fromActionPayload);
        r.trigger(a, {
          type: a,
          seriesId: o.id,
          name: k(v) ? h.getName(v[0]) : h.getName(v),
          selected: B(u) ? u : E({}, u)
        });
      }
  });
}
function ZT(e, t, r) {
  e.on("selectchanged", function(n) {
    var i = r.getModel();
    n.isFromClick ? (pn("map", "selectchanged", t, i, n), pn("pie", "selectchanged", t, i, n)) : n.fromAction === "select" ? (pn("map", "selected", t, i, n), pn("pie", "selected", t, i, n)) : n.fromAction === "unselect" && (pn("map", "unselected", t, i, n), pn("pie", "unselected", t, i, n));
  });
}
function gi(e, t, r) {
  for (var n; e && !(t(e) && (n = e, r)); )
    e = e.__hostTarget || e.parent;
  return n;
}
var jT = Math.round(Math.random() * 9), KT = typeof Object.defineProperty == "function", QT = (function() {
  function e() {
    this._id = "__ec_inner_" + jT++;
  }
  return e.prototype.get = function(t) {
    return this._guard(t)[this._id];
  }, e.prototype.set = function(t, r) {
    var n = this._guard(t);
    return KT ? Object.defineProperty(n, this._id, {
      value: r,
      enumerable: !1,
      configurable: !0
    }) : n[this._id] = r, this;
  }, e.prototype.delete = function(t) {
    return this.has(t) ? (delete this._guard(t)[this._id], !0) : !1;
  }, e.prototype.has = function(t) {
    return !!this._guard(t)[this._id];
  }, e.prototype._guard = function(t) {
    if (t !== Object(t))
      throw TypeError("Value of WeakMap is not a non-null object.");
    return t;
  }, e;
})(), JT = ht.extend({
  type: "triangle",
  shape: {
    cx: 0,
    cy: 0,
    width: 0,
    height: 0
  },
  buildPath: function(e, t) {
    var r = t.cx, n = t.cy, i = t.width / 2, a = t.height / 2;
    e.moveTo(r, n - a), e.lineTo(r + i, n + a), e.lineTo(r - i, n + a), e.closePath();
  }
}), tx = ht.extend({
  type: "diamond",
  shape: {
    cx: 0,
    cy: 0,
    width: 0,
    height: 0
  },
  buildPath: function(e, t) {
    var r = t.cx, n = t.cy, i = t.width / 2, a = t.height / 2;
    e.moveTo(r, n - a), e.lineTo(r + i, n), e.lineTo(r, n + a), e.lineTo(r - i, n), e.closePath();
  }
}), ex = ht.extend({
  type: "pin",
  shape: {
    // x, y on the cusp
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  buildPath: function(e, t) {
    var r = t.x, n = t.y, i = t.width / 5 * 3, a = Math.max(i, t.height), o = i / 2, s = o * o / (a - o), u = n - a + o + s, l = Math.asin(s / o), f = Math.cos(l) * o, h = Math.sin(l), v = Math.cos(l), c = o * 0.6, d = o * 0.7;
    e.moveTo(r - f, u + s), e.arc(r, u, o, Math.PI - l, Math.PI * 2 + l), e.bezierCurveTo(r + f - h * c, u + s + v * c, r, n - d, r, n), e.bezierCurveTo(r, n - d, r - f + h * c, u + s + v * c, r - f, u + s), e.closePath();
  }
}), rx = ht.extend({
  type: "arrow",
  shape: {
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  buildPath: function(e, t) {
    var r = t.height, n = t.width, i = t.x, a = t.y, o = n / 3 * 2;
    e.moveTo(i, a), e.lineTo(i + o, a + r), e.lineTo(i, a + r / 4 * 3), e.lineTo(i - o, a + r), e.lineTo(i, a), e.closePath();
  }
}), nx = {
  line: tn,
  rect: Et,
  roundRect: Et,
  square: Et,
  circle: ta,
  diamond: tx,
  pin: ex,
  arrow: rx,
  triangle: JT
}, ix = {
  line: function(e, t, r, n, i) {
    i.x1 = e, i.y1 = t + n / 2, i.x2 = e + r, i.y2 = t + n / 2;
  },
  rect: function(e, t, r, n, i) {
    i.x = e, i.y = t, i.width = r, i.height = n;
  },
  roundRect: function(e, t, r, n, i) {
    i.x = e, i.y = t, i.width = r, i.height = n, i.r = Math.min(r, n) / 4;
  },
  square: function(e, t, r, n, i) {
    var a = Math.min(r, n);
    i.x = e, i.y = t, i.width = a, i.height = a;
  },
  circle: function(e, t, r, n, i) {
    i.cx = e + r / 2, i.cy = t + n / 2, i.r = Math.min(r, n) / 2;
  },
  diamond: function(e, t, r, n, i) {
    i.cx = e + r / 2, i.cy = t + n / 2, i.width = r, i.height = n;
  },
  pin: function(e, t, r, n, i) {
    i.x = e + r / 2, i.y = t + n / 2, i.width = r, i.height = n;
  },
  arrow: function(e, t, r, n, i) {
    i.x = e + r / 2, i.y = t + n / 2, i.width = r, i.height = n;
  },
  triangle: function(e, t, r, n, i) {
    i.cx = e + r / 2, i.cy = t + n / 2, i.width = r, i.height = n;
  }
}, _l = {};
x(nx, function(e, t) {
  _l[t] = new e();
});
var ax = ht.extend({
  type: "symbol",
  shape: {
    symbolType: "",
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  calculateTextPosition: function(e, t, r) {
    var n = Pp(e, t, r), i = this.shape;
    return i && i.symbolType === "pin" && t.position === "inside" && (n.y = r.y + r.height * 0.4), n;
  },
  buildPath: function(e, t, r) {
    var n = t.symbolType;
    if (n !== "none") {
      var i = _l[n];
      i || (n = "rect", i = _l[n]), ix[n](t.x, t.y, t.width, t.height, i.shape), i.buildPath(e, i.shape, r);
    }
  }
});
function ox(e, t) {
  if (this.type !== "image") {
    var r = this.style;
    this.__isEmptyBrush ? (r.stroke = e, r.fill = t || "#fff", r.lineWidth = 2) : this.shape.symbolType === "line" ? r.stroke = e : r.fill = e, this.markRedraw();
  }
}
function os(e, t, r, n, i, a, o) {
  var s = e.indexOf("empty") === 0;
  s && (e = e.substr(5, 1).toLowerCase() + e.substr(6));
  var u;
  return e.indexOf("image://") === 0 ? u = sf(e.slice(8), new j(t, r, n, i), o ? "center" : "cover") : e.indexOf("path://") === 0 ? u = $o(e.slice(7), {}, new j(t, r, n, i), o ? "center" : "cover") : u = new ax({
    shape: {
      symbolType: e,
      x: t,
      y: r,
      width: n,
      height: i
    }
  }), u.__isEmptyBrush = s, u.setColor = ox, a && u.setColor(a), u;
}
function sx(e, t) {
  if (e != null)
    return k(e) || (e = [e, e]), [It(e[0], t[0]) || 0, It(Y(e[1], e[0]), t[1]) || 0];
}
function Vr(e) {
  return isFinite(e);
}
function ux(e, t, r) {
  var n = t.x == null ? 0 : t.x, i = t.x2 == null ? 1 : t.x2, a = t.y == null ? 0 : t.y, o = t.y2 == null ? 0 : t.y2;
  t.global || (n = n * r.width + r.x, i = i * r.width + r.x, a = a * r.height + r.y, o = o * r.height + r.y), n = Vr(n) ? n : 0, i = Vr(i) ? i : 1, a = Vr(a) ? a : 0, o = Vr(o) ? o : 0;
  var s = e.createLinearGradient(n, a, i, o);
  return s;
}
function lx(e, t, r) {
  var n = r.width, i = r.height, a = Math.min(n, i), o = t.x == null ? 0.5 : t.x, s = t.y == null ? 0.5 : t.y, u = t.r == null ? 0.5 : t.r;
  t.global || (o = o * n + r.x, s = s * i + r.y, u = u * a), o = Vr(o) ? o : 0.5, s = Vr(s) ? s : 0.5, u = u >= 0 && Vr(u) ? u : 0.5;
  var l = e.createRadialGradient(o, s, 0, o, s, u);
  return l;
}
function ml(e, t, r) {
  for (var n = t.type === "radial" ? lx(e, t, r) : ux(e, t, r), i = t.colorStops, a = 0; a < i.length; a++)
    n.addColorStop(i[a].offset, i[a].color);
  return n;
}
function fx(e, t) {
  if (e === t || !e && !t)
    return !1;
  if (!e || !t || e.length !== t.length)
    return !0;
  for (var r = 0; r < e.length; r++)
    if (e[r] !== t[r])
      return !0;
  return !1;
}
function La(e) {
  return parseInt(e, 10);
}
function Ra(e, t, r) {
  var n = ["width", "height"][t], i = ["clientWidth", "clientHeight"][t], a = ["paddingLeft", "paddingTop"][t], o = ["paddingRight", "paddingBottom"][t];
  if (r[n] != null && r[n] !== "auto")
    return parseFloat(r[n]);
  var s = document.defaultView.getComputedStyle(e);
  return (e[i] || La(s[n]) || La(e.style[n])) - (La(s[a]) || 0) - (La(s[o]) || 0) | 0;
}
function hx(e, t) {
  return !e || e === "solid" || !(t > 0) ? null : e === "dashed" ? [4 * t, 2 * t] : e === "dotted" ? [t] : ut(e) ? [e] : k(e) ? e : null;
}
function Dy(e) {
  var t = e.style, r = t.lineDash && t.lineWidth > 0 && hx(t.lineDash, t.lineWidth), n = t.lineDashOffset;
  if (r) {
    var i = t.strokeNoScale && e.getLineScale ? e.getLineScale() : 1;
    i && i !== 1 && (r = F(r, function(a) {
      return a / i;
    }), n /= i);
  }
  return [r, n];
}
var cx = new Ln(!0);
function xo(e) {
  var t = e.stroke;
  return !(t == null || t === "none" || !(e.lineWidth > 0));
}
function fv(e) {
  return typeof e == "string" && e !== "none";
}
function Co(e) {
  var t = e.fill;
  return t != null && t !== "none";
}
function hv(e, t) {
  if (t.fillOpacity != null && t.fillOpacity !== 1) {
    var r = e.globalAlpha;
    e.globalAlpha = t.fillOpacity * t.opacity, e.fill(), e.globalAlpha = r;
  } else
    e.fill();
}
function cv(e, t) {
  if (t.strokeOpacity != null && t.strokeOpacity !== 1) {
    var r = e.globalAlpha;
    e.globalAlpha = t.strokeOpacity * t.opacity, e.stroke(), e.globalAlpha = r;
  } else
    e.stroke();
}
function Sl(e, t, r) {
  var n = Up(t.image, t.__image, r);
  if (zo(n)) {
    var i = e.createPattern(n, t.repeat || "repeat");
    if (typeof DOMMatrix == "function" && i && i.setTransform) {
      var a = new DOMMatrix();
      a.translateSelf(t.x || 0, t.y || 0), a.rotateSelf(0, 0, (t.rotation || 0) * rp), a.scaleSelf(t.scaleX || 1, t.scaleY || 1), i.setTransform(a);
    }
    return i;
  }
}
function vx(e, t, r, n) {
  var i, a = xo(r), o = Co(r), s = r.strokePercent, u = s < 1, l = !t.path;
  (!t.silent || u) && l && t.createPathProxy();
  var f = t.path || cx, h = t.__dirty;
  if (!n) {
    var v = r.fill, c = r.stroke, d = o && !!v.colorStops, y = a && !!c.colorStops, p = o && !!v.image, g = a && !!c.image, _ = void 0, m = void 0, S = void 0, b = void 0, w = void 0;
    (d || y) && (w = t.getBoundingRect()), d && (_ = h ? ml(e, v, w) : t.__canvasFillGradient, t.__canvasFillGradient = _), y && (m = h ? ml(e, c, w) : t.__canvasStrokeGradient, t.__canvasStrokeGradient = m), p && (S = h || !t.__canvasFillPattern ? Sl(e, v, t) : t.__canvasFillPattern, t.__canvasFillPattern = S), g && (b = h || !t.__canvasStrokePattern ? Sl(e, c, t) : t.__canvasStrokePattern, t.__canvasStrokePattern = S), d ? e.fillStyle = _ : p && (S ? e.fillStyle = S : o = !1), y ? e.strokeStyle = m : g && (b ? e.strokeStyle = b : a = !1);
  }
  var T = t.getGlobalScale();
  f.setScale(T[0], T[1], t.segmentIgnoreThreshold);
  var C, M;
  e.setLineDash && r.lineDash && (i = Dy(t), C = i[0], M = i[1]);
  var D = !0;
  (l || h & mn) && (f.setDPR(e.dpr), u ? f.setContext(null) : (f.setContext(e), D = !1), f.reset(), t.buildPath(f, t.shape, n), f.toStatic(), t.pathUpdated()), D && f.rebuildPath(e, u ? s : 1), C && (e.setLineDash(C), e.lineDashOffset = M), n || (r.strokeFirst ? (a && cv(e, r), o && hv(e, r)) : (o && hv(e, r), a && cv(e, r))), C && e.setLineDash([]);
}
function dx(e, t, r) {
  var n = t.__image = Up(r.image, t.__image, t, t.onload);
  if (!(!n || !zo(n))) {
    var i = r.x || 0, a = r.y || 0, o = t.getWidth(), s = t.getHeight(), u = n.width / n.height;
    if (o == null && s != null ? o = s * u : s == null && o != null ? s = o / u : o == null && s == null && (o = n.width, s = n.height), r.sWidth && r.sHeight) {
      var l = r.sx || 0, f = r.sy || 0;
      e.drawImage(n, l, f, r.sWidth, r.sHeight, i, a, o, s);
    } else if (r.sx && r.sy) {
      var l = r.sx, f = r.sy, h = o - l, v = s - f;
      e.drawImage(n, l, f, h, v, i, a, o, s);
    } else
      e.drawImage(n, i, a, o, s);
  }
}
function px(e, t, r) {
  var n, i = r.text;
  if (i != null && (i += ""), i) {
    e.font = r.font || jr, e.textAlign = r.textAlign, e.textBaseline = r.textBaseline;
    var a = void 0, o = void 0;
    e.setLineDash && r.lineDash && (n = Dy(t), a = n[0], o = n[1]), a && (e.setLineDash(a), e.lineDashOffset = o), r.strokeFirst ? (xo(r) && e.strokeText(i, r.x, r.y), Co(r) && e.fillText(i, r.x, r.y)) : (Co(r) && e.fillText(i, r.x, r.y), xo(r) && e.strokeText(i, r.x, r.y)), a && e.setLineDash([]);
  }
}
var vv = ["shadowBlur", "shadowOffsetX", "shadowOffsetY"], dv = [
  ["lineCap", "butt"],
  ["lineJoin", "miter"],
  ["miterLimit", 10]
];
function Py(e, t, r, n, i) {
  var a = !1;
  if (!n && (r = r || {}, t === r))
    return !1;
  if (n || t.opacity !== r.opacity) {
    zt(e, i), a = !0;
    var o = Math.max(Math.min(t.opacity, 1), 0);
    e.globalAlpha = isNaN(o) ? qr.opacity : o;
  }
  (n || t.blend !== r.blend) && (a || (zt(e, i), a = !0), e.globalCompositeOperation = t.blend || qr.blend);
  for (var s = 0; s < vv.length; s++) {
    var u = vv[s];
    (n || t[u] !== r[u]) && (a || (zt(e, i), a = !0), e[u] = e.dpr * (t[u] || 0));
  }
  return (n || t.shadowColor !== r.shadowColor) && (a || (zt(e, i), a = !0), e.shadowColor = t.shadowColor || qr.shadowColor), a;
}
function pv(e, t, r, n, i) {
  var a = Wi(t, i.inHover), o = n ? null : r && Wi(r, i.inHover) || {};
  if (a === o)
    return !1;
  var s = Py(e, a, o, n, i);
  if ((n || a.fill !== o.fill) && (s || (zt(e, i), s = !0), fv(a.fill) && (e.fillStyle = a.fill)), (n || a.stroke !== o.stroke) && (s || (zt(e, i), s = !0), fv(a.stroke) && (e.strokeStyle = a.stroke)), (n || a.opacity !== o.opacity) && (s || (zt(e, i), s = !0), e.globalAlpha = a.opacity == null ? 1 : a.opacity), t.hasStroke()) {
    var u = a.lineWidth, l = u / (a.strokeNoScale && t.getLineScale ? t.getLineScale() : 1);
    e.lineWidth !== l && (s || (zt(e, i), s = !0), e.lineWidth = l);
  }
  for (var f = 0; f < dv.length; f++) {
    var h = dv[f], v = h[0];
    (n || a[v] !== o[v]) && (s || (zt(e, i), s = !0), e[v] = a[v] || h[1]);
  }
  return s;
}
function gx(e, t, r, n, i) {
  return Py(e, Wi(t, i.inHover), r && Wi(r, i.inHover), n, i);
}
function Iy(e, t) {
  var r = t.transform, n = e.dpr || 1;
  r ? e.setTransform(n * r[0], n * r[1], n * r[2], n * r[3], n * r[4], n * r[5]) : e.setTransform(n, 0, 0, n, 0, 0);
}
function yx(e, t, r) {
  for (var n = !1, i = 0; i < e.length; i++) {
    var a = e[i];
    n = n || a.isZeroArea(), Iy(t, a), t.beginPath(), a.buildPath(t, a.shape), t.clip();
  }
  r.allClipped = n;
}
function _x(e, t) {
  return e && t ? e[0] !== t[0] || e[1] !== t[1] || e[2] !== t[2] || e[3] !== t[3] || e[4] !== t[4] || e[5] !== t[5] : !(!e && !t);
}
var gv = 1, yv = 2, _v = 3, mv = 4;
function mx(e) {
  var t = Co(e), r = xo(e);
  return !(e.lineDash || !(+t ^ +r) || t && typeof e.fill != "string" || r && typeof e.stroke != "string" || e.strokePercent < 1 || e.strokeOpacity < 1 || e.fillOpacity < 1);
}
function zt(e, t) {
  t.batchFill && e.fill(), t.batchStroke && e.stroke(), t.batchFill = "", t.batchStroke = "";
}
function Wi(e, t) {
  return t && e.__hoverStyle || e.style;
}
function If(e, t) {
  Wr(e, t, { inHover: !1, viewWidth: 0, viewHeight: 0 }, !0);
}
function Wr(e, t, r, n) {
  var i = t.transform;
  if (!t.shouldBePainted(r.viewWidth, r.viewHeight, !1, !1)) {
    t.__dirty &= ~Ut, t.__isRendered = !1;
    return;
  }
  var a = t.__clipPaths, o = r.prevElClipPaths, s = !1, u = !1;
  if ((!o || fx(a, o)) && (o && o.length && (zt(e, r), e.restore(), u = s = !0, r.prevElClipPaths = null, r.allClipped = !1, r.prevEl = null), a && a.length && (zt(e, r), e.save(), yx(a, e, r), s = !0), r.prevElClipPaths = a), r.allClipped) {
    t.__isRendered = !1;
    return;
  }
  t.beforeBrush && t.beforeBrush(), t.innerBeforeBrush();
  var l = r.prevEl;
  l || (u = s = !0);
  var f = t instanceof ht && t.autoBatch && mx(t.style);
  s || _x(i, l.transform) ? (zt(e, r), Iy(e, t)) : f || zt(e, r);
  var h = Wi(t, r.inHover);
  t instanceof ht ? (r.lastDrawType !== gv && (u = !0, r.lastDrawType = gv), pv(e, t, l, u, r), (!f || !r.batchFill && !r.batchStroke) && e.beginPath(), vx(e, t, h, f), f && (r.batchFill = h.fill || "", r.batchStroke = h.stroke || "")) : t instanceof co ? (r.lastDrawType !== _v && (u = !0, r.lastDrawType = _v), pv(e, t, l, u, r), px(e, t, h)) : t instanceof vr ? (r.lastDrawType !== yv && (u = !0, r.lastDrawType = yv), gx(e, t, l, u, r), dx(e, t, h)) : t.getTemporalDisplayables && (r.lastDrawType !== mv && (u = !0, r.lastDrawType = mv), Sx(e, t, r)), f && n && zt(e, r), t.innerAfterBrush(), t.afterBrush && t.afterBrush(), r.prevEl = t, t.__dirty = 0, t.__isRendered = !0;
}
function Sx(e, t, r) {
  var n = t.getDisplayables(), i = t.getTemporalDisplayables();
  e.save();
  var a = {
    prevElClipPaths: null,
    prevEl: null,
    allClipped: !1,
    viewWidth: r.viewWidth,
    viewHeight: r.viewHeight,
    inHover: r.inHover
  }, o, s;
  for (o = t.getCursor(), s = n.length; o < s; o++) {
    var u = n[o];
    u.beforeBrush && u.beforeBrush(), u.innerBeforeBrush(), Wr(e, u, a, o === s - 1), u.innerAfterBrush(), u.afterBrush && u.afterBrush(), a.prevEl = u;
  }
  for (var l = 0, f = i.length; l < f; l++) {
    var u = i[l];
    u.beforeBrush && u.beforeBrush(), u.innerBeforeBrush(), Wr(e, u, a, l === f - 1), u.innerAfterBrush(), u.afterBrush && u.afterBrush(), a.prevEl = u;
  }
  t.clearTemporalDisplayables(), t.notClear = !0, e.restore();
}
var uu = new QT(), Sv = new Zi(100), wv = ["symbol", "symbolSize", "symbolKeepAspect", "color", "backgroundColor", "dashArrayX", "dashArrayY", "maxTileWidth", "maxTileHeight"];
function wl(e, t) {
  if (e === "none")
    return null;
  var r = t.getDevicePixelRatio(), n = t.getZr(), i = n.painter.type === "svg";
  e.dirty && uu.delete(e);
  var a = uu.get(e);
  if (a)
    return a;
  var o = ft(e, {
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
  return u(s), s.rotation = o.rotation, s.scaleX = s.scaleY = i ? 1 : 1 / r, uu.set(e, s), e.dirty = !1, s;
  function u(l) {
    for (var f = [r], h = !0, v = 0; v < wv.length; ++v) {
      var c = o[wv[v]];
      if (c != null && !k(c) && !B(c) && !ut(c) && typeof c != "boolean") {
        h = !1;
        break;
      }
      f.push(c);
    }
    var d;
    if (h) {
      d = f.join(",") + (i ? "-svg" : "");
      var y = Sv.get(d);
      y && (i ? l.svgElement = y : l.image = y);
    }
    var p = Ly(o.dashArrayX), g = bx(o.dashArrayY), _ = Ay(o.symbol), m = Tx(p), S = Ry(g), b = !i && Ve.createCanvas(), w = i && {
      tag: "g",
      attrs: {},
      key: "dcl",
      children: []
    }, T = M(), C;
    b && (b.width = T.width * r, b.height = T.height * r, C = b.getContext("2d")), D(), h && Sv.put(d, b || w), l.image = b, l.svgElement = w, l.svgWidth = T.width, l.svgHeight = T.height;
    function M() {
      for (var P = 1, A = 0, L = m.length; A < L; ++A)
        P = kh(P, m[A]);
      for (var I = 1, A = 0, L = _.length; A < L; ++A)
        I = kh(I, _[A].length);
      P *= I;
      var R = S * m.length * _.length;
      return {
        width: Math.max(1, Math.min(P, o.maxTileWidth)),
        height: Math.max(1, Math.min(R, o.maxTileHeight))
      };
    }
    function D() {
      C && (C.clearRect(0, 0, b.width, b.height), o.backgroundColor && (C.fillStyle = o.backgroundColor, C.fillRect(0, 0, b.width, b.height)));
      for (var P = 0, A = 0; A < g.length; ++A)
        P += g[A];
      if (P <= 0)
        return;
      for (var L = -S, I = 0, R = 0, O = 0; L < T.height; ) {
        if (I % 2 === 0) {
          for (var q = R / 2 % _.length, H = 0, G = 0, X = 0; H < T.width * 2; ) {
            for (var nt = 0, A = 0; A < p[O].length; ++A)
              nt += p[O][A];
            if (nt <= 0)
              break;
            if (G % 2 === 0) {
              var J = (1 - o.symbolSize) * 0.5, mt = H + p[O][G] * J, Zt = L + g[I] * J, Xe = p[O][G] * o.symbolSize, pr = g[I] * o.symbolSize, gr = X / 2 % _[q].length;
              rn(mt, Zt, Xe, pr, _[q][gr]);
            }
            H += p[O][G], ++X, ++G, G === p[O].length && (G = 0);
          }
          ++O, O === p.length && (O = 0);
        }
        L += g[I], ++R, ++I, I === g.length && (I = 0);
      }
      function rn(Vt, yt, V, Z, yr) {
        var Ct = i ? 1 : r, Xf = os(yr, Vt * Ct, yt * Ct, V * Ct, Z * Ct, o.color, o.symbolKeepAspect);
        if (i) {
          var $f = n.painter.renderOneToVNode(Xf);
          $f && w.children.push($f);
        } else
          If(C, Xf);
      }
    }
  }
}
function Ay(e) {
  if (!e || e.length === 0)
    return [["rect"]];
  if (B(e))
    return [[e]];
  for (var t = !0, r = 0; r < e.length; ++r)
    if (!B(e[r])) {
      t = !1;
      break;
    }
  if (t)
    return Ay([e]);
  for (var n = [], r = 0; r < e.length; ++r)
    B(e[r]) ? n.push([e[r]]) : n.push(e[r]);
  return n;
}
function Ly(e) {
  if (!e || e.length === 0)
    return [[0, 0]];
  if (ut(e)) {
    var t = Math.ceil(e);
    return [[t, t]];
  }
  for (var r = !0, n = 0; n < e.length; ++n)
    if (!ut(e[n])) {
      r = !1;
      break;
    }
  if (r)
    return Ly([e]);
  for (var i = [], n = 0; n < e.length; ++n)
    if (ut(e[n])) {
      var t = Math.ceil(e[n]);
      i.push([t, t]);
    } else {
      var t = F(e[n], function(s) {
        return Math.ceil(s);
      });
      t.length % 2 === 1 ? i.push(t.concat(t)) : i.push(t);
    }
  return i;
}
function bx(e) {
  if (!e || typeof e == "object" && e.length === 0)
    return [0, 0];
  if (ut(e)) {
    var t = Math.ceil(e);
    return [t, t];
  }
  var r = F(e, function(n) {
    return Math.ceil(n);
  });
  return e.length % 2 ? r.concat(r) : r;
}
function Tx(e) {
  return F(e, function(t) {
    return Ry(t);
  });
}
function Ry(e) {
  for (var t = 0, r = 0; r < e.length; ++r)
    t += e[r];
  return e.length % 2 === 1 ? t * 2 : t;
}
function xx(e, t) {
  e.eachRawSeries(function(r) {
    if (!e.isSeriesFiltered(r)) {
      var n = r.getData();
      n.hasItemVisual() && n.each(function(o) {
        var s = n.getItemVisual(o, "decal");
        if (s) {
          var u = n.ensureUniqueItemVisual(o, "style");
          u.decal = wl(s, t);
        }
      });
      var i = n.getVisual("decal");
      if (i) {
        var a = n.getVisual("style");
        a.decal = wl(i, t);
      }
    }
  });
}
var de = new Le(), Ey = {};
function Cx(e, t) {
  Ey[e] = t;
}
function Oy(e) {
  return Ey[e];
}
var Mx = "5.6.0", Dx = {
  zrender: "5.6.1"
}, Px = 1, Ix = 800, Ax = 900, Lx = 1e3, Rx = 2e3, Ex = 5e3, ky = 1e3, Ox = 1100, Af = 2e3, By = 3e3, kx = 4e3, ss = 4500, Bx = 4600, Fx = 5e3, Nx = 6e3, Fy = 7e3, Ny = {
  PROCESSOR: {
    FILTER: Lx,
    SERIES_FILTER: Ix,
    STATISTIC: Ex
  },
  VISUAL: {
    LAYOUT: ky,
    PROGRESSIVE_LAYOUT: Ox,
    GLOBAL: Af,
    CHART: By,
    POST_CHART_LAYOUT: Bx,
    COMPONENT: kx,
    BRUSH: Fx,
    CHART_ITEM: ss,
    ARIA: Nx,
    DECAL: Fy
  }
}, wt = "__flagInMainProcess", Nt = "__pendingUpdate", lu = "__needsUpdateStatus", bv = /^[a-zA-Z0-9_]+$/, fu = "__connectUpdateStatus", Tv = 0, zx = 1, Hx = 2;
function zy(e) {
  return function() {
    for (var t = [], r = 0; r < arguments.length; r++)
      t[r] = arguments[r];
    if (this.isDisposed()) {
      this.id;
      return;
    }
    return Gy(this, e, t);
  };
}
function Hy(e) {
  return function() {
    for (var t = [], r = 0; r < arguments.length; r++)
      t[r] = arguments[r];
    return Gy(this, e, t);
  };
}
function Gy(e, t, r) {
  return r[0] = r[0] && r[0].toLowerCase(), Le.prototype[t].apply(e, r);
}
var Vy = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      return e !== null && e.apply(this, arguments) || this;
    }
    return t;
  })(Le)
), Wy = Vy.prototype;
Wy.on = Hy("on");
Wy.off = Hy("off");
var gn, hu, Ea, Ke, cu, vu, du, ei, ri, xv, Cv, pu, Mv, Oa, Dv, Uy, jt, Pv, Mo = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t(r, n, i) {
      var a = e.call(this, new UT()) || this;
      a._chartsViews = [], a._chartsMap = {}, a._componentsViews = [], a._componentsMap = {}, a._pendingActions = [], i = i || {}, B(n) && (n = Yy[n]), a._dom = r;
      var o = "canvas", s = "auto", u = !1;
      i.ssr && Lp(function(v) {
        var c = at(v), d = c.dataIndex;
        if (d != null) {
          var y = W();
          return y.set("series_index", c.seriesIndex), y.set("data_index", d), c.ssrType && y.set("ssr_type", c.ssrType), y;
        }
      });
      var l = a._zr = Zu(r, {
        renderer: i.renderer || o,
        devicePixelRatio: i.devicePixelRatio,
        width: i.width,
        height: i.height,
        ssr: i.ssr,
        useDirtyRect: Y(i.useDirtyRect, u),
        useCoarsePointer: Y(i.useCoarsePointer, s),
        pointerSize: i.pointerSize
      });
      a._ssr = i.ssr, a._throttledZrFlush = Pf(ot(l.flush, l), 17), n = $(n), n && Qg(n, !0), a._theme = n, a._locale = Xw(i.locale || Og), a._coordSysMgr = new ns();
      var f = a._api = Dv(a);
      function h(v, c) {
        return v.__prio - c.__prio;
      }
      return Wa(Po, h), Wa(bl, h), a._scheduler = new by(a, f, bl, Po), a._messageCenter = new Vy(), a._initEvents(), a.resize = ot(a.resize, a), l.animation.on("frame", a._onframe, a), xv(l, a), Cv(l, a), Ai(a), a;
    }
    return t.prototype._onframe = function() {
      if (!this._disposed) {
        Pv(this);
        var r = this._scheduler;
        if (this[Nt]) {
          var n = this[Nt].silent;
          this[wt] = !0;
          try {
            gn(this), Ke.update.call(this, null, this[Nt].updateParams);
          } catch (u) {
            throw this[wt] = !1, this[Nt] = null, u;
          }
          this._zr.flush(), this[wt] = !1, this[Nt] = null, ei.call(this, n), ri.call(this, n);
        } else if (r.unfinished) {
          var i = Px, a = this._model, o = this._api;
          r.unfinished = !1;
          do {
            var s = +/* @__PURE__ */ new Date();
            r.performSeriesTasks(a), r.performDataProcessorTasks(a), vu(this, a), r.performVisualTasks(a), Oa(this, this._model, o, "remain", {}), i -= +/* @__PURE__ */ new Date() - s;
          } while (i > 0 && r.unfinished);
          r.unfinished || this._zr.flush();
        }
      }
    }, t.prototype.getDom = function() {
      return this._dom;
    }, t.prototype.getId = function() {
      return this.id;
    }, t.prototype.getZr = function() {
      return this._zr;
    }, t.prototype.isSSR = function() {
      return this._ssr;
    }, t.prototype.setOption = function(r, n, i) {
      if (!this[wt]) {
        if (this._disposed) {
          this.id;
          return;
        }
        var a, o, s;
        if (N(n) && (i = n.lazyUpdate, a = n.silent, o = n.replaceMerge, s = n.transition, n = n.notMerge), this[wt] = !0, !this._model || n) {
          var u = new bb(this._api), l = this._theme, f = this._model = new xf();
          f.scheduler = this._scheduler, f.ssr = this._ssr, f.init(null, null, null, l, this._locale, u);
        }
        this._model.setOption(r, {
          replaceMerge: o
        }, Tl);
        var h = {
          seriesTransition: s,
          optionChanged: !0
        };
        if (i)
          this[Nt] = {
            silent: a,
            updateParams: h
          }, this[wt] = !1, this.getZr().wakeUp();
        else {
          try {
            gn(this), Ke.update.call(this, null, h);
          } catch (v) {
            throw this[Nt] = null, this[wt] = !1, v;
          }
          this._ssr || this._zr.flush(), this[Nt] = null, this[wt] = !1, ei.call(this, a), ri.call(this, a);
        }
      }
    }, t.prototype.setTheme = function() {
    }, t.prototype.getModel = function() {
      return this._model;
    }, t.prototype.getOption = function() {
      return this._model && this._model.getOption();
    }, t.prototype.getWidth = function() {
      return this._zr.getWidth();
    }, t.prototype.getHeight = function() {
      return this._zr.getHeight();
    }, t.prototype.getDevicePixelRatio = function() {
      return this._zr.painter.dpr || z.hasGlobalWindow && window.devicePixelRatio || 1;
    }, t.prototype.getRenderedCanvas = function(r) {
      return this.renderToCanvas(r);
    }, t.prototype.renderToCanvas = function(r) {
      r = r || {};
      var n = this._zr.painter;
      return n.getRenderedCanvas({
        backgroundColor: r.backgroundColor || this._model.get("backgroundColor"),
        pixelRatio: r.pixelRatio || this.getDevicePixelRatio()
      });
    }, t.prototype.renderToSVGString = function(r) {
      r = r || {};
      var n = this._zr.painter;
      return n.renderToString({
        useViewBox: r.useViewBox
      });
    }, t.prototype.getSvgDataURL = function() {
      if (z.svgSupported) {
        var r = this._zr, n = r.storage.getDisplayList();
        return x(n, function(i) {
          i.stopAnimation(null, !0);
        }), r.painter.toDataURL();
      }
    }, t.prototype.getDataURL = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      r = r || {};
      var n = r.excludeComponents, i = this._model, a = [], o = this;
      x(n, function(u) {
        i.eachComponent({
          mainType: u
        }, function(l) {
          var f = o._componentsMap[l.__viewId];
          f.group.ignore || (a.push(f), f.group.ignore = !0);
        });
      });
      var s = this._zr.painter.getType() === "svg" ? this.getSvgDataURL() : this.renderToCanvas(r).toDataURL("image/" + (r && r.type || "png"));
      return x(a, function(u) {
        u.group.ignore = !1;
      }), s;
    }, t.prototype.getConnectedDataURL = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      var n = r.type === "svg", i = this.group, a = Math.min, o = Math.max, s = 1 / 0;
      if (Io[i]) {
        var u = s, l = s, f = -s, h = -s, v = [], c = r && r.pixelRatio || this.getDevicePixelRatio();
        x($r, function(m, S) {
          if (m.group === i) {
            var b = n ? m.getZr().painter.getSvgDom().innerHTML : m.renderToCanvas($(r)), w = m.getDom().getBoundingClientRect();
            u = a(w.left, u), l = a(w.top, l), f = o(w.right, f), h = o(w.bottom, h), v.push({
              dom: b,
              left: w.left,
              top: w.top
            });
          }
        }), u *= c, l *= c, f *= c, h *= c;
        var d = f - u, y = h - l, p = Ve.createCanvas(), g = Zu(p, {
          renderer: n ? "svg" : "canvas"
        });
        if (g.resize({
          width: d,
          height: y
        }), n) {
          var _ = "";
          return x(v, function(m) {
            var S = m.left - u, b = m.top - l;
            _ += '<g transform="translate(' + S + "," + b + ')">' + m.dom + "</g>";
          }), g.painter.getSvgRoot().innerHTML = _, r.connectedBackgroundColor && g.painter.setBackgroundColor(r.connectedBackgroundColor), g.refreshImmediately(), g.painter.toDataURL();
        } else
          return r.connectedBackgroundColor && g.add(new Et({
            shape: {
              x: 0,
              y: 0,
              width: d,
              height: y
            },
            style: {
              fill: r.connectedBackgroundColor
            }
          })), x(v, function(m) {
            var S = new vr({
              style: {
                x: m.left * c - u,
                y: m.top * c - l,
                image: m.dom
              }
            });
            g.add(S);
          }), g.refreshImmediately(), p.toDataURL("image/" + (r && r.type || "png"));
      } else
        return this.getDataURL(r);
    }, t.prototype.convertToPixel = function(r, n) {
      return cu(this, "convertToPixel", r, n);
    }, t.prototype.convertFromPixel = function(r, n) {
      return cu(this, "convertFromPixel", r, n);
    }, t.prototype.containPixel = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      var i = this._model, a, o = ks(i, r);
      return x(o, function(s, u) {
        u.indexOf("Models") >= 0 && x(s, function(l) {
          var f = l.coordinateSystem;
          if (f && f.containPoint)
            a = a || !!f.containPoint(n);
          else if (u === "seriesModels") {
            var h = this._chartsMap[l.__viewId];
            h && h.containPoint && (a = a || h.containPoint(n, l));
          }
        }, this);
      }, this), !!a;
    }, t.prototype.getVisual = function(r, n) {
      var i = this._model, a = ks(i, r, {
        defaultMainType: "series"
      }), o = a.seriesModel, s = o.getData(), u = a.hasOwnProperty("dataIndexInside") ? a.dataIndexInside : a.hasOwnProperty("dataIndex") ? s.indexOfRawIndex(a.dataIndex) : null;
      return u != null ? XT(s, u, n) : $T(s, n);
    }, t.prototype.getViewOfComponentModel = function(r) {
      return this._componentsMap[r.__viewId];
    }, t.prototype.getViewOfSeriesModel = function(r) {
      return this._chartsMap[r.__viewId];
    }, t.prototype._initEvents = function() {
      var r = this;
      x(Gx, function(n) {
        var i = function(a) {
          var o = r.getModel(), s = a.target, u, l = n === "globalout";
          if (l ? u = {} : s && gi(s, function(d) {
            var y = at(d);
            if (y && y.dataIndex != null) {
              var p = y.dataModel || o.getSeriesByIndex(y.seriesIndex);
              return u = p && p.getDataParams(y.dataIndex, y.dataType, s) || {}, !0;
            } else if (y.eventData)
              return u = E({}, y.eventData), !0;
          }, !0), u) {
            var f = u.componentType, h = u.componentIndex;
            (f === "markLine" || f === "markPoint" || f === "markArea") && (f = "series", h = u.seriesIndex);
            var v = f && h != null && o.getComponent(f, h), c = v && r[v.mainType === "series" ? "_chartsMap" : "_componentsMap"][v.__viewId];
            u.event = a, u.type = n, r._$eventProcessor.eventInfo = {
              targetEl: s,
              packedEvent: u,
              model: v,
              view: c
            }, r.trigger(n, u);
          }
        };
        i.zrEventfulCallAtLast = !0, r._zr.on(n, i, r);
      }), x(Di, function(n, i) {
        r._messageCenter.on(i, function(a) {
          this.trigger(i, a);
        }, r);
      }), x(["selectchanged"], function(n) {
        r._messageCenter.on(n, function(i) {
          this.trigger(n, i);
        }, r);
      }), ZT(this._messageCenter, this, this._api);
    }, t.prototype.isDisposed = function() {
      return this._disposed;
    }, t.prototype.clear = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this.setOption({
        series: []
      }, !0);
    }, t.prototype.dispose = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this._disposed = !0;
      var r = this.getDom();
      r && Gp(this.getDom(), Rf, "");
      var n = this, i = n._api, a = n._model;
      x(n._componentsViews, function(o) {
        o.dispose(a, i);
      }), x(n._chartsViews, function(o) {
        o.dispose(a, i);
      }), n._zr.dispose(), n._dom = n._model = n._chartsMap = n._componentsMap = n._chartsViews = n._componentsViews = n._scheduler = n._api = n._zr = n._throttledZrFlush = n._theme = n._coordSysMgr = n._messageCenter = null, delete $r[n.id];
    }, t.prototype.resize = function(r) {
      if (!this[wt]) {
        if (this._disposed) {
          this.id;
          return;
        }
        this._zr.resize(r);
        var n = this._model;
        if (this._loadingFX && this._loadingFX.resize(), !!n) {
          var i = n.resetOption("media"), a = r && r.silent;
          this[Nt] && (a == null && (a = this[Nt].silent), i = !0, this[Nt] = null), this[wt] = !0;
          try {
            i && gn(this), Ke.update.call(this, {
              type: "resize",
              animation: E({
                // Disable animation
                duration: 0
              }, r && r.animation)
            });
          } catch (o) {
            throw this[wt] = !1, o;
          }
          this[wt] = !1, ei.call(this, a), ri.call(this, a);
        }
      }
    }, t.prototype.showLoading = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      if (N(r) && (n = r, r = ""), r = r || "default", this.hideLoading(), !!xl[r]) {
        var i = xl[r](this._api, n), a = this._zr;
        this._loadingFX = i, a.add(i);
      }
    }, t.prototype.hideLoading = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this._loadingFX && this._zr.remove(this._loadingFX), this._loadingFX = null;
    }, t.prototype.makeActionFromEvent = function(r) {
      var n = E({}, r);
      return n.type = Di[r.type], n;
    }, t.prototype.dispatchAction = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      if (N(n) || (n = {
        silent: !!n
      }), !!Do[r.type] && this._model) {
        if (this[wt]) {
          this._pendingActions.push(r);
          return;
        }
        var i = n.silent;
        du.call(this, r, i);
        var a = n.flush;
        a ? this._zr.flush() : a !== !1 && z.browser.weChat && this._throttledZrFlush(), ei.call(this, i), ri.call(this, i);
      }
    }, t.prototype.updateLabelLayout = function() {
      de.trigger("series:layoutlabels", this._model, this._api, {
        // Not adding series labels.
        // TODO
        updatedSeries: []
      });
    }, t.prototype.appendData = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      var n = r.seriesIndex, i = this.getModel(), a = i.getSeriesByIndex(n);
      a.appendData(r), this._scheduler.unfinished = !0, this.getZr().wakeUp();
    }, t.internalField = (function() {
      gn = function(h) {
        var v = h._scheduler;
        v.restorePipelines(h._model), v.prepareStageTasks(), hu(h, !0), hu(h, !1), v.plan();
      }, hu = function(h, v) {
        for (var c = h._model, d = h._scheduler, y = v ? h._componentsViews : h._chartsViews, p = v ? h._componentsMap : h._chartsMap, g = h._zr, _ = h._api, m = 0; m < y.length; m++)
          y[m].__alive = !1;
        v ? c.eachComponent(function(w, T) {
          w !== "series" && S(T);
        }) : c.eachSeries(S);
        function S(w) {
          var T = w.__requireNewView;
          w.__requireNewView = !1;
          var C = "_ec_" + w.id + "_" + w.type, M = !T && p[C];
          if (!M) {
            var D = xe(w.type), P = v ? $t.getClass(D.main, D.sub) : (
              // FIXME:TS
              // (ChartView as ChartViewConstructor).getClass('series', classType.sub)
              // For backward compat, still support a chart type declared as only subType
              // like "liquidfill", but recommend "series.liquidfill"
              // But need a base class to make a type series.
              De.getClass(D.sub)
            );
            M = new P(), M.init(c, _), p[C] = M, y.push(M), g.add(M.group);
          }
          w.__viewId = M.__id = C, M.__alive = !0, M.__model = w, M.group.__ecComponentInfo = {
            mainType: w.mainType,
            index: w.componentIndex
          }, !v && d.prepareView(M, w, c, _);
        }
        for (var m = 0; m < y.length; ) {
          var b = y[m];
          b.__alive ? m++ : (!v && b.renderTask.dispose(), g.remove(b.group), b.dispose(c, _), y.splice(m, 1), p[b.__id] === b && delete p[b.__id], b.__id = b.group.__ecComponentInfo = null);
        }
      }, Ea = function(h, v, c, d, y) {
        var p = h._model;
        if (p.setUpdatePayload(c), !d) {
          x([].concat(h._componentsViews).concat(h._chartsViews), b);
          return;
        }
        var g = {};
        g[d + "Id"] = c[d + "Id"], g[d + "Index"] = c[d + "Index"], g[d + "Name"] = c[d + "Name"];
        var _ = {
          mainType: d,
          query: g
        };
        y && (_.subType = y);
        var m = c.excludeSeriesId, S;
        m != null && (S = W(), x(Rt(m), function(w) {
          var T = Ce(w, null);
          T != null && S.set(T, !0);
        })), p && p.eachComponent(_, function(w) {
          var T = S && S.get(w.id) != null;
          if (!T)
            if (lc(c))
              if (w instanceof Ae)
                c.type === Xr && !c.notBlur && !w.get(["emphasis", "disabled"]) && NS(w, c, h._api);
              else {
                var C = nf(w.mainType, w.componentIndex, c.name, h._api), M = C.focusSelf, D = C.dispatchers;
                c.type === Xr && M && !c.notBlur && il(w.mainType, w.componentIndex, h._api), D && x(D, function(P) {
                  c.type === Xr ? el(P) : rl(P);
                });
              }
            else ol(c) && w instanceof Ae && (GS(w, c, h._api), sc(w), jt(h));
        }, h), p && p.eachComponent(_, function(w) {
          var T = S && S.get(w.id) != null;
          T || b(h[d === "series" ? "_chartsMap" : "_componentsMap"][w.__viewId]);
        }, h);
        function b(w) {
          w && w.__alive && w[v] && w[v](w.__model, p, h._api, c);
        }
      }, Ke = {
        prepareAndUpdate: function(h) {
          gn(this), Ke.update.call(this, h, {
            // Needs to mark option changed if newOption is given.
            // It's from MagicType.
            // TODO If use a separate flag optionChanged in payload?
            optionChanged: h.newOption != null
          });
        },
        update: function(h, v) {
          var c = this._model, d = this._api, y = this._zr, p = this._coordSysMgr, g = this._scheduler;
          if (c) {
            c.setUpdatePayload(h), g.restoreData(c, h), g.performSeriesTasks(c), p.create(c, d), g.performDataProcessorTasks(c, h), vu(this, c), p.update(c, d), r(c), g.performVisualTasks(c, h), pu(this, c, d, h, v);
            var _ = c.get("backgroundColor") || "transparent", m = c.get("darkMode");
            y.setBackgroundColor(_), m != null && m !== "auto" && y.setDarkMode(m), de.trigger("afterupdate", c, d);
          }
        },
        updateTransform: function(h) {
          var v = this, c = this._model, d = this._api;
          if (c) {
            c.setUpdatePayload(h);
            var y = [];
            c.eachComponent(function(g, _) {
              if (g !== "series") {
                var m = v.getViewOfComponentModel(_);
                if (m && m.__alive)
                  if (m.updateTransform) {
                    var S = m.updateTransform(_, c, d, h);
                    S && S.update && y.push(m);
                  } else
                    y.push(m);
              }
            });
            var p = W();
            c.eachSeries(function(g) {
              var _ = v._chartsMap[g.__viewId];
              if (_.updateTransform) {
                var m = _.updateTransform(g, c, d, h);
                m && m.update && p.set(g.uid, 1);
              } else
                p.set(g.uid, 1);
            }), r(c), this._scheduler.performVisualTasks(c, h, {
              setDirty: !0,
              dirtyMap: p
            }), Oa(this, c, d, h, {}, p), de.trigger("afterupdate", c, d);
          }
        },
        updateView: function(h) {
          var v = this._model;
          v && (v.setUpdatePayload(h), De.markUpdateMethod(h, "updateView"), r(v), this._scheduler.performVisualTasks(v, h, {
            setDirty: !0
          }), pu(this, v, this._api, h, {}), de.trigger("afterupdate", v, this._api));
        },
        updateVisual: function(h) {
          var v = this, c = this._model;
          c && (c.setUpdatePayload(h), c.eachSeries(function(d) {
            d.getData().clearAllVisual();
          }), De.markUpdateMethod(h, "updateVisual"), r(c), this._scheduler.performVisualTasks(c, h, {
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
          }), de.trigger("afterupdate", c, this._api));
        },
        updateLayout: function(h) {
          Ke.update.call(this, h);
        }
      }, cu = function(h, v, c, d) {
        if (h._disposed) {
          h.id;
          return;
        }
        for (var y = h._model, p = h._coordSysMgr.getCoordinateSystems(), g, _ = ks(y, c), m = 0; m < p.length; m++) {
          var S = p[m];
          if (S[v] && (g = S[v](y, _, d)) != null)
            return g;
        }
      }, vu = function(h, v) {
        var c = h._chartsMap, d = h._scheduler;
        v.eachSeries(function(y) {
          d.updateStreamModes(y, c[y.__viewId]);
        });
      }, du = function(h, v) {
        var c = this, d = this.getModel(), y = h.type, p = h.escapeConnect, g = Do[y], _ = g.actionInfo, m = (_.update || "update").split(":"), S = m.pop(), b = m[0] != null && xe(m[0]);
        this[wt] = !0;
        var w = [h], T = !1;
        h.batch && (T = !0, w = F(h.batch, function(I) {
          return I = ft(E({}, I), h), I.batch = null, I;
        }));
        var C = [], M, D = ol(h), P = lc(h);
        if (P && ug(this._api), x(w, function(I) {
          if (M = g.action(I, c._model, c._api), M = M || E({}, I), M.type = _.event || M.type, C.push(M), P) {
            var R = Kl(h), O = R.queryOptionMap, q = R.mainTypeSpecified, H = q ? O.keys()[0] : "series";
            Ea(c, S, I, H), jt(c);
          } else D ? (Ea(c, S, I, "series"), jt(c)) : b && Ea(c, S, I, b.main, b.sub);
        }), S !== "none" && !P && !D && !b)
          try {
            this[Nt] ? (gn(this), Ke.update.call(this, h), this[Nt] = null) : Ke[S].call(this, h);
          } catch (I) {
            throw this[wt] = !1, I;
          }
        if (T ? M = {
          type: _.event || y,
          escapeConnect: p,
          batch: C
        } : M = C[0], this[wt] = !1, !v) {
          var A = this._messageCenter;
          if (A.trigger(M.type, M), D) {
            var L = {
              type: "selectchanged",
              escapeConnect: p,
              selected: VS(d),
              isFromClick: h.isFromClick || !1,
              fromAction: h.type,
              fromActionPayload: h
            };
            A.trigger(L.type, L);
          }
        }
      }, ei = function(h) {
        for (var v = this._pendingActions; v.length; ) {
          var c = v.shift();
          du.call(this, c, h);
        }
      }, ri = function(h) {
        !h && this.trigger("updated");
      }, xv = function(h, v) {
        h.on("rendered", function(c) {
          v.trigger("rendered", c), // Although zr is dirty if initial animation is not finished
          // and this checking is called on frame, we also check
          // animation finished for robustness.
          h.animation.isFinished() && !v[Nt] && !v._scheduler.unfinished && !v._pendingActions.length && v.trigger("finished");
        });
      }, Cv = function(h, v) {
        h.on("mouseover", function(c) {
          var d = c.target, y = gi(d, al);
          y && (zS(y, c, v._api), jt(v));
        }).on("mouseout", function(c) {
          var d = c.target, y = gi(d, al);
          y && (HS(y, c, v._api), jt(v));
        }).on("click", function(c) {
          var d = c.target, y = gi(d, function(_) {
            return at(_).dataIndex != null;
          }, !0);
          if (y) {
            var p = y.selected ? "unselect" : "select", g = at(y);
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
      function r(h) {
        h.clearColorPalette(), h.eachSeries(function(v) {
          v.clearColorPalette();
        });
      }
      function n(h) {
        var v = [], c = [], d = !1;
        if (h.eachComponent(function(_, m) {
          var S = m.get("zlevel") || 0, b = m.get("z") || 0, w = m.getZLevelKey();
          d = d || !!w, (_ === "series" ? c : v).push({
            zlevel: S,
            z: b,
            idx: m.componentIndex,
            type: _,
            key: w
          });
        }), d) {
          var y = v.concat(c), p, g;
          Wa(y, function(_, m) {
            return _.zlevel === m.zlevel ? _.z - m.z : _.zlevel - m.zlevel;
          }), x(y, function(_) {
            var m = h.getComponent(_.type, _.idx), S = _.zlevel, b = _.key;
            p != null && (S = Math.max(p, S)), b ? (S === p && b !== g && S++, g = b) : g && (S === p && S++, g = ""), p = S, m.setZLevel(S);
          });
        }
      }
      pu = function(h, v, c, d, y) {
        n(v), Mv(h, v, c, d, y), x(h._chartsViews, function(p) {
          p.__alive = !1;
        }), Oa(h, v, c, d, y), x(h._chartsViews, function(p) {
          p.__alive || p.remove(v, c);
        });
      }, Mv = function(h, v, c, d, y, p) {
        x(p || h._componentsViews, function(g) {
          var _ = g.__model;
          l(_, g), g.render(_, v, c, d), s(_, g), f(_, g);
        });
      }, Oa = function(h, v, c, d, y, p) {
        var g = h._scheduler;
        y = E(y || {}, {
          updatedSeries: v.getSeries()
        }), de.trigger("series:beforeupdate", v, c, y);
        var _ = !1;
        v.eachSeries(function(m) {
          var S = h._chartsMap[m.__viewId];
          S.__alive = !0;
          var b = S.renderTask;
          g.updatePayload(b, d), l(m, S), p && p.get(m.uid) && b.dirty(), b.perform(g.getPerformArgs(b)) && (_ = !0), S.group.silent = !!m.get("silent"), o(m, S), sc(m);
        }), g.unfinished = _ || g.unfinished, de.trigger("series:layoutlabels", v, c, y), de.trigger("series:transition", v, c, y), v.eachSeries(function(m) {
          var S = h._chartsMap[m.__viewId];
          s(m, S), f(m, S);
        }), a(h, v), de.trigger("series:afterupdate", v, c, y);
      }, jt = function(h) {
        h[lu] = !0, h.getZr().wakeUp();
      }, Pv = function(h) {
        h[lu] && (h.getZr().storage.traverse(function(v) {
          Ti(v) || i(v);
        }), h[lu] = !1);
      };
      function i(h) {
        for (var v = [], c = h.currentStates, d = 0; d < c.length; d++) {
          var y = c[d];
          y === "emphasis" || y === "blur" || y === "select" || v.push(y);
        }
        h.selected && h.states.select && v.push("select"), h.hoverState === ef && h.states.emphasis ? v.push("emphasis") : h.hoverState === tf && h.states.blur && v.push("blur"), h.useStates(v);
      }
      function a(h, v) {
        var c = h._zr, d = c.storage, y = 0;
        d.traverse(function(p) {
          p.isGroup || y++;
        }), y > v.get("hoverLayerThreshold") && !z.node && !z.worker && v.eachSeries(function(p) {
          if (!p.preventUsingHoverLayer) {
            var g = h._chartsMap[p.__viewId];
            g.__alive && g.eachRendered(function(_) {
              _.states.emphasis && (_.states.emphasis.hoverLayer = !0);
            });
          }
        });
      }
      function o(h, v) {
        var c = h.get("blendMode") || null;
        v.eachRendered(function(d) {
          d.isGroup || (d.style.blend = c);
        });
      }
      function s(h, v) {
        if (!h.preventAutoZ) {
          var c = h.get("z") || 0, d = h.get("zlevel") || 0;
          v.eachRendered(function(y) {
            return u(y, c, d, -1 / 0), !0;
          });
        }
      }
      function u(h, v, c, d) {
        var y = h.getTextContent(), p = h.getTextGuideLine(), g = h.isGroup;
        if (g)
          for (var _ = h.childrenRef(), m = 0; m < _.length; m++)
            d = Math.max(u(_[m], v, c, d), d);
        else
          h.z = v, h.zlevel = c, d = Math.max(h.z2, d);
        if (y && (y.z = v, y.zlevel = c, isFinite(d) && (y.z2 = d + 2)), p) {
          var S = h.textGuideLineConfig;
          p.z = v, p.zlevel = c, isFinite(d) && (p.z2 = d + (S && S.showAbove ? 1 : -1));
        }
        return d;
      }
      function l(h, v) {
        v.eachRendered(function(c) {
          if (!Ti(c)) {
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
            if (Ti(g))
              return;
            if (g instanceof ht && XS(g), g.__dirty) {
              var _ = g.prevStates;
              _ && g.useStates(_);
            }
            if (d) {
              g.stateTransition = p;
              var m = g.getTextContent(), S = g.getTextGuideLine();
              m && (m.stateTransition = p), S && (S.stateTransition = p);
            }
            g.__dirty && i(g);
          }
        });
      }
      Dv = function(h) {
        return new /** @class */
        ((function(v) {
          lt(c, v);
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
            el(d, y), jt(h);
          }, c.prototype.leaveEmphasis = function(d, y) {
            rl(d, y), jt(h);
          }, c.prototype.enterBlur = function(d) {
            FS(d), jt(h);
          }, c.prototype.leaveBlur = function(d) {
            ig(d), jt(h);
          }, c.prototype.enterSelect = function(d) {
            ag(d), jt(h);
          }, c.prototype.leaveSelect = function(d) {
            og(d), jt(h);
          }, c.prototype.getModel = function() {
            return h.getModel();
          }, c.prototype.getViewOfComponentModel = function(d) {
            return h.getViewOfComponentModel(d);
          }, c.prototype.getViewOfSeriesModel = function(d) {
            return h.getViewOfSeriesModel(d);
          }, c;
        })(jg))(h);
      }, Uy = function(h) {
        function v(c, d) {
          for (var y = 0; y < c.length; y++) {
            var p = c[y];
            p[fu] = d;
          }
        }
        x(Di, function(c, d) {
          h._messageCenter.on(d, function(y) {
            if (Io[h.group] && h[fu] !== Tv) {
              if (y && y.escapeConnect)
                return;
              var p = h.makeActionFromEvent(y), g = [];
              x($r, function(_) {
                _ !== h && _.group === h.group && g.push(_);
              }), v(g, Tv), x(g, function(_) {
                _[fu] !== zx && _.dispatchAction(p);
              }), v(g, Hx);
            }
          });
        });
      };
    })(), t;
  })(Le)
), Lf = Mo.prototype;
Lf.on = zy("on");
Lf.off = zy("off");
Lf.one = function(e, t, r) {
  var n = this;
  function i() {
    for (var a = [], o = 0; o < arguments.length; o++)
      a[o] = arguments[o];
    t && t.apply && t.apply(this, a), n.off(e, i);
  }
  this.on.call(this, e, i, r);
};
var Gx = ["click", "dblclick", "mouseover", "mouseout", "mousemove", "mousedown", "mouseup", "globalout", "contextmenu"];
var Do = {}, Di = {}, bl = [], Tl = [], Po = [], Yy = {}, xl = {}, $r = {}, Io = {}, Vx = +/* @__PURE__ */ new Date() - 0, Wx = +/* @__PURE__ */ new Date() - 0, Rf = "_echarts_instance_";
function Ux(e, t, r) {
  var n = !(r && r.ssr);
  if (n) {
    var i = Ef(e);
    if (i)
      return i;
  }
  var a = new Mo(e, t, r);
  return a.id = "ec_" + Vx++, $r[a.id] = a, n && Gp(e, Rf, a.id), Uy(a), de.trigger("afterinit", a), a;
}
function Yx(e) {
  if (k(e)) {
    var t = e;
    e = null, x(t, function(r) {
      r.group != null && (e = r.group);
    }), e = e || "g_" + Wx++, x(t, function(r) {
      r.group = e;
    });
  }
  return Io[e] = !0, e;
}
function qy(e) {
  Io[e] = !1;
}
var qx = qy;
function Xx(e) {
  B(e) ? e = $r[e] : e instanceof Mo || (e = Ef(e)), e instanceof Mo && !e.isDisposed() && e.dispose();
}
function Ef(e) {
  return $r[M1(e, Rf)];
}
function $x(e) {
  return $r[e];
}
function Of(e, t) {
  Yy[e] = t;
}
function kf(e) {
  rt(Tl, e) < 0 && Tl.push(e);
}
function Bf(e, t) {
  Ff(bl, e, t, Rx);
}
function Xy(e) {
  us("afterinit", e);
}
function $y(e) {
  us("afterupdate", e);
}
function us(e, t) {
  de.on(e, t);
}
function en(e, t, r) {
  U(t) && (r = t, t = "");
  var n = N(e) ? e.type : [e, e = {
    event: t
  }][0];
  e.event = (e.event || n).toLowerCase(), t = e.event, !Di[t] && (Ie(bv.test(n) && bv.test(t)), Do[n] || (Do[n] = {
    action: r,
    actionInfo: e
  }), Di[t] = n);
}
function Zy(e, t) {
  ns.register(e, t);
}
function Zx(e) {
  var t = ns.get(e);
  if (t)
    return t.getDimensionsInfo ? t.getDimensionsInfo() : t.dimensions.slice();
}
function jy(e, t) {
  Ff(Po, e, t, ky, "layout");
}
function dr(e, t) {
  Ff(Po, e, t, By, "visual");
}
var Iv = [];
function Ff(e, t, r, n, i) {
  if ((U(t) || N(t)) && (r = t, t = n), !(rt(Iv, r) >= 0)) {
    Iv.push(r);
    var a = by.wrapStageHandler(r, i);
    a.__prio = t, a.__raw = r, e.push(a);
  }
}
function Nf(e, t) {
  xl[e] = t;
}
function jx(e) {
  $d({
    createCanvas: e
  });
}
function Ky(e, t, r) {
  var n = Oy("registerMap");
  n && n(e, t, r);
}
function Kx(e) {
  var t = Oy("getMap");
  return t && t(e);
}
var Qy = eT;
dr(Af, AT);
dr(ss, LT);
dr(ss, RT);
dr(Af, YT);
dr(ss, qT);
dr(Fy, xx);
kf(Qg);
Bf(Ax, Ob);
Nf("default", ET);
en({
  type: Xr,
  event: Xr,
  update: Xr
}, Lt);
en({
  type: Za,
  event: Za,
  update: Za
}, Lt);
en({
  type: Si,
  event: Si,
  update: Si
}, Lt);
en({
  type: ja,
  event: ja,
  update: ja
}, Lt);
en({
  type: wi,
  event: wi,
  update: wi
}, Lt);
Of("light", WT);
Of("dark", My);
var Qx = {};
function ni(e) {
  return e == null ? 0 : e.length || 1;
}
function Av(e) {
  return e;
}
var Jx = (
  /** @class */
  (function() {
    function e(t, r, n, i, a, o) {
      this._old = t, this._new = r, this._oldKeyGetter = n || Av, this._newKeyGetter = i || Av, this.context = a, this._diffModeMultiple = o === "multiple";
    }
    return e.prototype.add = function(t) {
      return this._add = t, this;
    }, e.prototype.update = function(t) {
      return this._update = t, this;
    }, e.prototype.updateManyToOne = function(t) {
      return this._updateManyToOne = t, this;
    }, e.prototype.updateOneToMany = function(t) {
      return this._updateOneToMany = t, this;
    }, e.prototype.updateManyToMany = function(t) {
      return this._updateManyToMany = t, this;
    }, e.prototype.remove = function(t) {
      return this._remove = t, this;
    }, e.prototype.execute = function() {
      this[this._diffModeMultiple ? "_executeMultiple" : "_executeOneToOne"]();
    }, e.prototype._executeOneToOne = function() {
      var t = this._old, r = this._new, n = {}, i = new Array(t.length), a = new Array(r.length);
      this._initIndexMap(t, null, i, "_oldKeyGetter"), this._initIndexMap(r, n, a, "_newKeyGetter");
      for (var o = 0; o < t.length; o++) {
        var s = i[o], u = n[s], l = ni(u);
        if (l > 1) {
          var f = u.shift();
          u.length === 1 && (n[s] = u[0]), this._update && this._update(f, o);
        } else l === 1 ? (n[s] = null, this._update && this._update(u, o)) : this._remove && this._remove(o);
      }
      this._performRestAdd(a, n);
    }, e.prototype._executeMultiple = function() {
      var t = this._old, r = this._new, n = {}, i = {}, a = [], o = [];
      this._initIndexMap(t, n, a, "_oldKeyGetter"), this._initIndexMap(r, i, o, "_newKeyGetter");
      for (var s = 0; s < a.length; s++) {
        var u = a[s], l = n[u], f = i[u], h = ni(l), v = ni(f);
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
    }, e.prototype._performRestAdd = function(t, r) {
      for (var n = 0; n < t.length; n++) {
        var i = t[n], a = r[i], o = ni(a);
        if (o > 1)
          for (var s = 0; s < o; s++)
            this._add && this._add(a[s]);
        else o === 1 && this._add && this._add(a);
        r[i] = null;
      }
    }, e.prototype._initIndexMap = function(t, r, n, i) {
      for (var a = this._diffModeMultiple, o = 0; o < t.length; o++) {
        var s = "_ec_" + this[i](t[o], o);
        if (a || (n[o] = s), !!r) {
          var u = r[s], l = ni(u);
          l === 0 ? (r[s] = o, a && n.push(s)) : l === 1 ? r[s] = [u, o] : u.push(o);
        }
      }
    }, e;
  })()
), tC = (
  /** @class */
  (function() {
    function e(t, r) {
      this._encode = t, this._schema = r;
    }
    return e.prototype.get = function() {
      return {
        // Do not generate full dimension name until fist used.
        fullDimensions: this._getFullDimensionNames(),
        encode: this._encode
      };
    }, e.prototype._getFullDimensionNames = function() {
      return this._cachedDimNames || (this._cachedDimNames = this._schema ? this._schema.makeOutputDimensionNames() : []), this._cachedDimNames;
    }, e;
  })()
);
function eC(e, t) {
  var r = {}, n = r.encode = {}, i = W(), a = [], o = [], s = {};
  x(e.dimensions, function(v) {
    var c = e.getDimensionInfo(v), d = c.coordDim;
    if (d) {
      var y = c.coordDimIndex;
      gu(n, d)[y] = v, c.isExtraCoord || (i.set(d, 1), nC(c.type) && (a[0] = v), gu(s, d)[y] = e.getDimensionIndex(c.name)), c.defaultTooltip && o.push(v);
    }
    Yg.each(function(p, g) {
      var _ = gu(n, g), m = c.otherDims[g];
      m != null && m !== !1 && (_[m] = c.name);
    });
  });
  var u = [], l = {};
  i.each(function(v, c) {
    var d = n[c];
    l[c] = d[0], u = u.concat(d);
  }), r.dataDimsOnCoord = u, r.dataDimIndicesOnCoord = F(u, function(v) {
    return e.getDimensionInfo(v).storeDimIndex;
  }), r.encodeFirstDimNotExtra = l;
  var f = n.label;
  f && f.length && (a = f.slice());
  var h = n.tooltip;
  return h && h.length ? o = h.slice() : o.length || (o = a.slice()), n.defaultedLabel = a, n.defaultedTooltip = o, r.userOutput = new tC(s, t), r;
}
function gu(e, t) {
  return e.hasOwnProperty(t) || (e[t] = []), e[t];
}
function rC(e) {
  return e === "category" ? "ordinal" : e === "time" ? "time" : "float";
}
function nC(e) {
  return !(e === "ordinal" || e === "time");
}
var to = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.otherDims = {}, t != null && E(this, t);
    }
    return e;
  })()
), iC = gt(), aC = {
  float: "f",
  int: "i",
  ordinal: "o",
  number: "n",
  time: "t"
}, Jy = (
  /** @class */
  (function() {
    function e(t) {
      this.dimensions = t.dimensions, this._dimOmitted = t.dimensionOmitted, this.source = t.source, this._fullDimCount = t.fullDimensionCount, this._updateDimOmitted(t.dimensionOmitted);
    }
    return e.prototype.isDimensionOmitted = function() {
      return this._dimOmitted;
    }, e.prototype._updateDimOmitted = function(t) {
      this._dimOmitted = t, t && (this._dimNameMap || (this._dimNameMap = r_(this.source)));
    }, e.prototype.getSourceDimensionIndex = function(t) {
      return Y(this._dimNameMap.get(t), -1);
    }, e.prototype.getSourceDimension = function(t) {
      var r = this.source.dimensionsDefine;
      if (r)
        return r[t];
    }, e.prototype.makeStoreSchema = function() {
      for (var t = this._fullDimCount, r = ey(this.source), n = !n_(t), i = "", a = [], o = 0, s = 0; o < t; o++) {
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
        }), r && u != null && (!h || !h.isCalculationCoord) && (i += n ? u.replace(/\`/g, "`1").replace(/\$/g, "`2") : u), i += "$", i += aC[l] || "f", f && (i += f.uid), i += "$";
      }
      var c = this.source, d = [c.seriesLayoutBy, c.startIndex, i].join("$$");
      return {
        dimensions: a,
        hash: d
      };
    }, e.prototype.makeOutputDimensionNames = function() {
      for (var t = [], r = 0, n = 0; r < this._fullDimCount; r++) {
        var i = void 0, a = this.dimensions[n];
        if (a && a.storeDimIndex === r)
          a.isCalculationCoord || (i = a.name), n++;
        else {
          var o = this.getSourceDimension(r);
          o && (i = o.name);
        }
        t.push(i);
      }
      return t;
    }, e.prototype.appendCalculationDimension = function(t) {
      this.dimensions.push(t), t.isCalculationCoord = !0, this._fullDimCount++, this._updateDimOmitted(!0);
    }, e;
  })()
);
function t_(e) {
  return e instanceof Jy;
}
function e_(e) {
  for (var t = W(), r = 0; r < (e || []).length; r++) {
    var n = e[r], i = N(n) ? n.name : n;
    i != null && t.get(i) == null && t.set(i, r);
  }
  return t;
}
function r_(e) {
  var t = iC(e);
  return t.dimNameMap || (t.dimNameMap = e_(e.dimensionsDefine));
}
function n_(e) {
  return e > 30;
}
var ii = N, Qe = F, oC = typeof Int32Array > "u" ? Array : Int32Array, sC = "e\0\0", Lv = -1, uC = ["hasItemOption", "_nameList", "_idList", "_invertedIndicesMap", "_dimSummary", "userOutput", "_rawData", "_dimValueGetter", "_nameDimIdx", "_idDimIdx", "_nameRepeatCount"], lC = ["_approximateExtent"], Rv, ka, ai, oi, yu, si, _u, i_ = (
  /** @class */
  (function() {
    function e(t, r) {
      this.type = "list", this._dimOmitted = !1, this._nameList = [], this._idList = [], this._visual = {}, this._layout = {}, this._itemVisuals = [], this._itemLayouts = [], this._graphicEls = [], this._approximateExtent = {}, this._calculationInfo = {}, this.hasItemOption = !1, this.TRANSFERABLE_METHODS = ["cloneShallow", "downSample", "minmaxDownSample", "lttbDownSample", "map"], this.CHANGABLE_METHODS = ["filterSelf", "selectRange"], this.DOWNSAMPLE_METHODS = ["downSample", "minmaxDownSample", "lttbDownSample"];
      var n, i = !1;
      t_(t) ? (n = t.dimensions, this._dimOmitted = t.isDimensionOmitted(), this._schema = t) : (i = !0, n = t), n = n || ["x", "y"];
      for (var a = {}, o = [], s = {}, u = !1, l = {}, f = 0; f < n.length; f++) {
        var h = n[f], v = B(h) ? new to({
          name: h
        }) : h instanceof to ? h : new to(h), c = v.name;
        v.type = v.type || "float", v.coordDim || (v.coordDim = c, v.coordDimIndex = 0);
        var d = v.otherDims = v.otherDims || {};
        o.push(c), a[c] = v, l[c] != null && (u = !0), v.createInvertedIndices && (s[c] = []), d.itemName === 0 && (this._nameDimIdx = f), d.itemId === 0 && (this._idDimIdx = f), i && (v.storeDimIndex = f);
      }
      if (this.dimensions = o, this._dimInfos = a, this._initGetDimensionInfo(u), this.hostModel = r, this._invertedIndicesMap = s, this._dimOmitted) {
        var y = this._dimIdxToName = W();
        x(o, function(p) {
          y.set(a[p].storeDimIndex, p);
        });
      }
    }
    return e.prototype.getDimension = function(t) {
      var r = this._recognizeDimIndex(t);
      if (r == null)
        return t;
      if (r = t, !this._dimOmitted)
        return this.dimensions[r];
      var n = this._dimIdxToName.get(r);
      if (n != null)
        return n;
      var i = this._schema.getSourceDimension(r);
      if (i)
        return i.name;
    }, e.prototype.getDimensionIndex = function(t) {
      var r = this._recognizeDimIndex(t);
      if (r != null)
        return r;
      if (t == null)
        return -1;
      var n = this._getDimInfo(t);
      return n ? n.storeDimIndex : this._dimOmitted ? this._schema.getSourceDimensionIndex(t) : -1;
    }, e.prototype._recognizeDimIndex = function(t) {
      if (ut(t) || t != null && !isNaN(t) && !this._getDimInfo(t) && (!this._dimOmitted || this._schema.getSourceDimensionIndex(t) < 0))
        return +t;
    }, e.prototype._getStoreDimIndex = function(t) {
      var r = this.getDimensionIndex(t);
      return r;
    }, e.prototype.getDimensionInfo = function(t) {
      return this._getDimInfo(this.getDimension(t));
    }, e.prototype._initGetDimensionInfo = function(t) {
      var r = this._dimInfos;
      this._getDimInfo = t ? function(n) {
        return r.hasOwnProperty(n) ? r[n] : void 0;
      } : function(n) {
        return r[n];
      };
    }, e.prototype.getDimensionsOnCoord = function() {
      return this._dimSummary.dataDimsOnCoord.slice();
    }, e.prototype.mapDimension = function(t, r) {
      var n = this._dimSummary;
      if (r == null)
        return n.encodeFirstDimNotExtra[t];
      var i = n.encode[t];
      return i ? i[r] : null;
    }, e.prototype.mapDimensionsAll = function(t) {
      var r = this._dimSummary, n = r.encode[t];
      return (n || []).slice();
    }, e.prototype.getStore = function() {
      return this._store;
    }, e.prototype.initData = function(t, r, n) {
      var i = this, a;
      if (t instanceof cl && (a = t), !a) {
        var o = this.dimensions, s = Cf(t) || Ot(t) ? new ry(t, o.length) : t;
        a = new cl();
        var u = Qe(o, function(l) {
          return {
            type: i._dimInfos[l].type,
            property: l
          };
        });
        a.initData(s, u, n);
      }
      this._store = a, this._nameList = (r || []).slice(), this._idList = [], this._nameRepeatCount = {}, this._doInit(0, a.count()), this._dimSummary = eC(this, this._schema), this.userOutput = this._dimSummary.userOutput;
    }, e.prototype.appendData = function(t) {
      var r = this._store.appendData(t);
      this._doInit(r[0], r[1]);
    }, e.prototype.appendValues = function(t, r) {
      var n = this._store.appendValues(t, r && r.length), i = n.start, a = n.end, o = this._shouldMakeIdFromName();
      if (this._updateOrdinalMeta(), r)
        for (var s = i; s < a; s++) {
          var u = s - i;
          this._nameList[s] = r[u], o && _u(this, s);
        }
    }, e.prototype._updateOrdinalMeta = function() {
      for (var t = this._store, r = this.dimensions, n = 0; n < r.length; n++) {
        var i = this._dimInfos[r[n]];
        i.ordinalMeta && t.collectOrdinalMeta(i.storeDimIndex, i.ordinalMeta);
      }
    }, e.prototype._shouldMakeIdFromName = function() {
      var t = this._store.getProvider();
      return this._idDimIdx == null && t.getSource().sourceFormat !== fr && !t.fillStorage;
    }, e.prototype._doInit = function(t, r) {
      if (!(t >= r)) {
        var n = this._store, i = n.getProvider();
        this._updateOrdinalMeta();
        var a = this._nameList, o = this._idList, s = i.getSource().sourceFormat, u = s === le;
        if (u && !i.pure)
          for (var l = [], f = t; f < r; f++) {
            var h = i.getItem(f, l);
            if (!this.hasItemOption && p1(h) && (this.hasItemOption = !0), h) {
              var v = h.name;
              a[f] == null && v != null && (a[f] = Ce(v, null));
              var c = h.id;
              o[f] == null && c != null && (o[f] = Ce(c, null));
            }
          }
        if (this._shouldMakeIdFromName())
          for (var f = t; f < r; f++)
            _u(this, f);
        Rv(this);
      }
    }, e.prototype.getApproximateExtent = function(t) {
      return this._approximateExtent[t] || this._store.getDataExtent(this._getStoreDimIndex(t));
    }, e.prototype.setApproximateExtent = function(t, r) {
      r = this.getDimension(r), this._approximateExtent[r] = t.slice();
    }, e.prototype.getCalculationInfo = function(t) {
      return this._calculationInfo[t];
    }, e.prototype.setCalculationInfo = function(t, r) {
      ii(t) ? E(this._calculationInfo, t) : this._calculationInfo[t] = r;
    }, e.prototype.getName = function(t) {
      var r = this.getRawIndex(t), n = this._nameList[r];
      return n == null && this._nameDimIdx != null && (n = ai(this, this._nameDimIdx, r)), n == null && (n = ""), n;
    }, e.prototype._getCategory = function(t, r) {
      var n = this._store.get(t, r), i = this._store.getOrdinalMeta(t);
      return i ? i.categories[n] : n;
    }, e.prototype.getId = function(t) {
      return ka(this, this.getRawIndex(t));
    }, e.prototype.count = function() {
      return this._store.count();
    }, e.prototype.get = function(t, r) {
      var n = this._store, i = this._dimInfos[t];
      if (i)
        return n.get(i.storeDimIndex, r);
    }, e.prototype.getByRawIndex = function(t, r) {
      var n = this._store, i = this._dimInfos[t];
      if (i)
        return n.getByRawIndex(i.storeDimIndex, r);
    }, e.prototype.getIndices = function() {
      return this._store.getIndices();
    }, e.prototype.getDataExtent = function(t) {
      return this._store.getDataExtent(this._getStoreDimIndex(t));
    }, e.prototype.getSum = function(t) {
      return this._store.getSum(this._getStoreDimIndex(t));
    }, e.prototype.getMedian = function(t) {
      return this._store.getMedian(this._getStoreDimIndex(t));
    }, e.prototype.getValues = function(t, r) {
      var n = this, i = this._store;
      return k(t) ? i.getValues(Qe(t, function(a) {
        return n._getStoreDimIndex(a);
      }), r) : i.getValues(t);
    }, e.prototype.hasValue = function(t) {
      for (var r = this._dimSummary.dataDimIndicesOnCoord, n = 0, i = r.length; n < i; n++)
        if (isNaN(this._store.get(r[n], t)))
          return !1;
      return !0;
    }, e.prototype.indexOfName = function(t) {
      for (var r = 0, n = this._store.count(); r < n; r++)
        if (this.getName(r) === t)
          return r;
      return -1;
    }, e.prototype.getRawIndex = function(t) {
      return this._store.getRawIndex(t);
    }, e.prototype.indexOfRawIndex = function(t) {
      return this._store.indexOfRawIndex(t);
    }, e.prototype.rawIndexOf = function(t, r) {
      var n = t && this._invertedIndicesMap[t], i = n && n[r];
      return i == null || isNaN(i) ? Lv : i;
    }, e.prototype.indicesOfNearest = function(t, r, n) {
      return this._store.indicesOfNearest(this._getStoreDimIndex(t), r, n);
    }, e.prototype.each = function(t, r, n) {
      U(t) && (n = r, r = t, t = []);
      var i = n || this, a = Qe(oi(t), this._getStoreDimIndex, this);
      this._store.each(a, i ? ot(r, i) : r);
    }, e.prototype.filterSelf = function(t, r, n) {
      U(t) && (n = r, r = t, t = []);
      var i = n || this, a = Qe(oi(t), this._getStoreDimIndex, this);
      return this._store = this._store.filter(a, i ? ot(r, i) : r), this;
    }, e.prototype.selectRange = function(t) {
      var r = this, n = {}, i = ct(t);
      return x(i, function(a) {
        var o = r._getStoreDimIndex(a);
        n[o] = t[a];
      }), this._store = this._store.selectRange(n), this;
    }, e.prototype.mapArray = function(t, r, n) {
      U(t) && (n = r, r = t, t = []), n = n || this;
      var i = [];
      return this.each(t, function() {
        i.push(r && r.apply(this, arguments));
      }, n), i;
    }, e.prototype.map = function(t, r, n, i) {
      var a = n || i || this, o = Qe(oi(t), this._getStoreDimIndex, this), s = si(this);
      return s._store = this._store.map(o, a ? ot(r, a) : r), s;
    }, e.prototype.modify = function(t, r, n, i) {
      var a = n || i || this, o = Qe(oi(t), this._getStoreDimIndex, this);
      this._store.modify(o, a ? ot(r, a) : r);
    }, e.prototype.downSample = function(t, r, n, i) {
      var a = si(this);
      return a._store = this._store.downSample(this._getStoreDimIndex(t), r, n, i), a;
    }, e.prototype.minmaxDownSample = function(t, r) {
      var n = si(this);
      return n._store = this._store.minmaxDownSample(this._getStoreDimIndex(t), r), n;
    }, e.prototype.lttbDownSample = function(t, r) {
      var n = si(this);
      return n._store = this._store.lttbDownSample(this._getStoreDimIndex(t), r), n;
    }, e.prototype.getRawDataItem = function(t) {
      return this._store.getRawDataItem(t);
    }, e.prototype.getItemModel = function(t) {
      var r = this.hostModel, n = this.getRawDataItem(t);
      return new st(n, r, r && r.ecModel);
    }, e.prototype.diff = function(t) {
      var r = this;
      return new Jx(t ? t.getStore().getIndices() : [], this.getStore().getIndices(), function(n) {
        return ka(t, n);
      }, function(n) {
        return ka(r, n);
      });
    }, e.prototype.getVisual = function(t) {
      var r = this._visual;
      return r && r[t];
    }, e.prototype.setVisual = function(t, r) {
      this._visual = this._visual || {}, ii(t) ? E(this._visual, t) : this._visual[t] = r;
    }, e.prototype.getItemVisual = function(t, r) {
      var n = this._itemVisuals[t], i = n && n[r];
      return i ?? this.getVisual(r);
    }, e.prototype.hasItemVisual = function() {
      return this._itemVisuals.length > 0;
    }, e.prototype.ensureUniqueItemVisual = function(t, r) {
      var n = this._itemVisuals, i = n[t];
      i || (i = n[t] = {});
      var a = i[r];
      return a == null && (a = this.getVisual(r), k(a) ? a = a.slice() : ii(a) && (a = E({}, a)), i[r] = a), a;
    }, e.prototype.setItemVisual = function(t, r, n) {
      var i = this._itemVisuals[t] || {};
      this._itemVisuals[t] = i, ii(r) ? E(i, r) : i[r] = n;
    }, e.prototype.clearAllVisual = function() {
      this._visual = {}, this._itemVisuals = [];
    }, e.prototype.setLayout = function(t, r) {
      ii(t) ? E(this._layout, t) : this._layout[t] = r;
    }, e.prototype.getLayout = function(t) {
      return this._layout[t];
    }, e.prototype.getItemLayout = function(t) {
      return this._itemLayouts[t];
    }, e.prototype.setItemLayout = function(t, r, n) {
      this._itemLayouts[t] = n ? E(this._itemLayouts[t] || {}, r) : r;
    }, e.prototype.clearItemLayouts = function() {
      this._itemLayouts.length = 0;
    }, e.prototype.setItemGraphicEl = function(t, r) {
      var n = this.hostModel && this.hostModel.seriesIndex;
      DS(n, this.dataType, t, r), this._graphicEls[t] = r;
    }, e.prototype.getItemGraphicEl = function(t) {
      return this._graphicEls[t];
    }, e.prototype.eachItemGraphicEl = function(t, r) {
      x(this._graphicEls, function(n, i) {
        n && t && t.call(r, n, i);
      });
    }, e.prototype.cloneShallow = function(t) {
      return t || (t = new e(this._schema ? this._schema : Qe(this.dimensions, this._getDimInfo, this), this.hostModel)), yu(t, this), t._store = this._store, t;
    }, e.prototype.wrapMethod = function(t, r) {
      var n = this[t];
      U(n) && (this.__wrappedMethods = this.__wrappedMethods || [], this.__wrappedMethods.push(t), this[t] = function() {
        var i = n.apply(this, arguments);
        return r.apply(this, [i].concat(Eo(arguments)));
      });
    }, e.internalField = (function() {
      Rv = function(t) {
        var r = t._invertedIndicesMap;
        x(r, function(n, i) {
          var a = t._dimInfos[i], o = a.ordinalMeta, s = t._store;
          if (o) {
            n = r[i] = new oC(o.categories.length);
            for (var u = 0; u < n.length; u++)
              n[u] = Lv;
            for (var u = 0; u < s.count(); u++)
              n[s.get(a.storeDimIndex, u)] = u;
          }
        });
      }, ai = function(t, r, n) {
        return Ce(t._getCategory(r, n), null);
      }, ka = function(t, r) {
        var n = t._idList[r];
        return n == null && t._idDimIdx != null && (n = ai(t, t._idDimIdx, r)), n == null && (n = sC + r), n;
      }, oi = function(t) {
        return k(t) || (t = t != null ? [t] : []), t;
      }, si = function(t) {
        var r = new e(t._schema ? t._schema : Qe(t.dimensions, t._getDimInfo, t), t.hostModel);
        return yu(r, t), r;
      }, yu = function(t, r) {
        x(uC.concat(r.__wrappedMethods || []), function(n) {
          r.hasOwnProperty(n) && (t[n] = r[n]);
        }), t.__wrappedMethods = r.__wrappedMethods, x(lC, function(n) {
          t[n] = $(r[n]);
        }), t._calculationInfo = E({}, r._calculationInfo);
      }, _u = function(t, r) {
        var n = t._nameList, i = t._idList, a = t._nameDimIdx, o = t._idDimIdx, s = n[r], u = i[r];
        if (s == null && a != null && (n[r] = s = ai(t, a, r)), u == null && o != null && (i[r] = u = ai(t, o, r)), u == null && s != null) {
          var l = t._nameRepeatCount, f = l[s] = (l[s] || 0) + 1;
          u = s, f > 1 && (u += "__ec__" + f), i[r] = u;
        }
      };
    })(), e;
  })()
);
function fC(e, t) {
  return a_(e, t).dimensions;
}
function a_(e, t) {
  Cf(e) || (e = Jg(e)), t = t || {};
  var r = t.coordDimensions || [], n = t.dimensionsDefine || e.dimensionsDefine || [], i = W(), a = [], o = cC(e, r, n, t.dimensionsCount), s = t.canOmitUnusedDimensions && n_(o), u = n === e.dimensionsDefine, l = u ? r_(e) : e_(n), f = t.encodeDefine;
  !f && t.encodeDefaulter && (f = t.encodeDefaulter(e, o));
  for (var h = W(f), v = new uy(o), c = 0; c < v.length; c++)
    v[c] = -1;
  function d(M) {
    var D = v[M];
    if (D < 0) {
      var P = n[M], A = N(P) ? P : {
        name: P
      }, L = new to(), I = A.name;
      I != null && l.get(I) != null && (L.name = L.displayName = I), A.type != null && (L.type = A.type), A.displayName != null && (L.displayName = A.displayName);
      var R = a.length;
      return v[M] = R, L.storeDimIndex = M, a.push(L), L;
    }
    return a[D];
  }
  if (!s)
    for (var c = 0; c < o; c++)
      d(c);
  h.each(function(M, D) {
    var P = Rt(M).slice();
    if (P.length === 1 && !B(P[0]) && P[0] < 0) {
      h.set(D, !1);
      return;
    }
    var A = h.set(D, []);
    x(P, function(L, I) {
      var R = B(L) ? l.get(L) : L;
      R != null && R < o && (A[I] = R, p(d(R), D, I));
    });
  });
  var y = 0;
  x(r, function(M) {
    var D, P, A, L;
    if (B(M))
      D = M, L = {};
    else {
      L = M, D = L.name;
      var I = L.ordinalMeta;
      L.ordinalMeta = null, L = E({}, L), L.ordinalMeta = I, P = L.dimsDef, A = L.otherDims, L.name = L.coordDim = L.coordDimIndex = L.dimsDef = L.otherDims = null;
    }
    var R = h.get(D);
    if (R !== !1) {
      if (R = Rt(R), !R.length)
        for (var O = 0; O < (P && P.length || 1); O++) {
          for (; y < o && d(y).coordDim != null; )
            y++;
          y < o && R.push(y++);
        }
      x(R, function(q, H) {
        var G = d(q);
        if (u && L.type != null && (G.type = L.type), p(ft(G, L), D, H), G.name == null && P) {
          var X = P[H];
          !N(X) && (X = {
            name: X
          }), G.name = G.displayName = X.name, G.defaultTooltip = X.defaultTooltip;
        }
        A && ft(G.otherDims, A);
      });
    }
  });
  function p(M, D, P) {
    Yg.get(D) != null ? M.otherDims[D] = P : (M.coordDim = D, M.coordDimIndex = P, i.set(D, !0));
  }
  var g = t.generateCoord, _ = t.generateCoordCount, m = _ != null;
  _ = g ? _ || 1 : 0;
  var S = g || "value";
  function b(M) {
    M.name == null && (M.name = M.coordDim);
  }
  if (s)
    x(a, function(M) {
      b(M);
    }), a.sort(function(M, D) {
      return M.storeDimIndex - D.storeDimIndex;
    });
  else
    for (var w = 0; w < o; w++) {
      var T = d(w), C = T.coordDim;
      C == null && (T.coordDim = vC(S, i, m), T.coordDimIndex = 0, (!g || _ <= 0) && (T.isExtraCoord = !0), _--), b(T), T.type == null && (Zg(e, w) === Wt.Must || T.isExtraCoord && (T.otherDims.itemName != null || T.otherDims.seriesName != null)) && (T.type = "ordinal");
    }
  return hC(a), new Jy({
    source: e,
    dimensions: a,
    fullDimensionCount: o,
    dimensionOmitted: s
  });
}
function hC(e) {
  for (var t = W(), r = 0; r < e.length; r++) {
    var n = e[r], i = n.name, a = t.get(i) || 0;
    a > 0 && (n.name = i + (a - 1)), a++, t.set(i, a);
  }
}
function cC(e, t, r, n) {
  var i = Math.max(e.dimensionsDetectedCount || 1, t.length, r.length, n || 0);
  return x(t, function(a) {
    var o;
    N(a) && (o = a.dimsDef) && (i = Math.max(i, o.length));
  }), i;
}
function vC(e, t, r) {
  if (r || t.hasKey(e)) {
    for (var n = 0; t.hasKey(e + n); )
      n++;
    e += n;
  }
  return t.set(e, !0), e;
}
var dC = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.coordSysDims = [], this.axisMap = W(), this.categoryAxisMap = W(), this.coordSysName = t;
    }
    return e;
  })()
);
function pC(e) {
  var t = e.get("coordinateSystem"), r = new dC(t), n = gC[t];
  if (n)
    return n(e, r, r.axisMap, r.categoryAxisMap), r;
}
var gC = {
  cartesian2d: function(e, t, r, n) {
    var i = e.getReferringComponents("xAxis", ar).models[0], a = e.getReferringComponents("yAxis", ar).models[0];
    t.coordSysDims = ["x", "y"], r.set("x", i), r.set("y", a), yn(i) && (n.set("x", i), t.firstCategoryDimIndex = 0), yn(a) && (n.set("y", a), t.firstCategoryDimIndex == null && (t.firstCategoryDimIndex = 1));
  },
  singleAxis: function(e, t, r, n) {
    var i = e.getReferringComponents("singleAxis", ar).models[0];
    t.coordSysDims = ["single"], r.set("single", i), yn(i) && (n.set("single", i), t.firstCategoryDimIndex = 0);
  },
  polar: function(e, t, r, n) {
    var i = e.getReferringComponents("polar", ar).models[0], a = i.findAxisModel("radiusAxis"), o = i.findAxisModel("angleAxis");
    t.coordSysDims = ["radius", "angle"], r.set("radius", a), r.set("angle", o), yn(a) && (n.set("radius", a), t.firstCategoryDimIndex = 0), yn(o) && (n.set("angle", o), t.firstCategoryDimIndex == null && (t.firstCategoryDimIndex = 1));
  },
  geo: function(e, t, r, n) {
    t.coordSysDims = ["lng", "lat"];
  },
  parallel: function(e, t, r, n) {
    var i = e.ecModel, a = i.getComponent("parallel", e.get("parallelIndex")), o = t.coordSysDims = a.dimensions.slice();
    x(a.parallelAxisIndex, function(s, u) {
      var l = i.getComponent("parallelAxis", s), f = o[u];
      r.set(f, l), yn(l) && (n.set(f, l), t.firstCategoryDimIndex == null && (t.firstCategoryDimIndex = u));
    });
  }
};
function yn(e) {
  return e.get("type") === "category";
}
function o_(e, t, r) {
  r = r || {};
  var n = r.byIndex, i = r.stackedCoordDimension, a, o, s;
  yC(t) ? a = t : (o = t.schema, a = o.dimensions, s = t.store);
  var u = !!(e && e.get("stack")), l, f, h, v;
  if (x(a, function(_, m) {
    B(_) && (a[m] = _ = {
      name: _
    }), u && !_.isExtraCoord && (!n && !l && _.ordinalMeta && (l = _), !f && _.type !== "ordinal" && _.type !== "time" && (!i || i === _.coordDim) && (f = _));
  }), f && !n && !l && (n = !0), f) {
    h = "__\0ecstackresult_" + e.id, v = "__\0ecstackedover_" + e.id, l && (l.createInvertedIndices = !0);
    var c = f.coordDim, d = f.type, y = 0;
    x(a, function(_) {
      _.coordDim === c && y++;
    });
    var p = {
      name: h,
      coordDim: c,
      coordDimIndex: y,
      type: d,
      isExtraCoord: !0,
      isCalculationCoord: !0,
      storeDimIndex: a.length
    }, g = {
      name: v,
      // This dimension contains stack base (generally, 0), so do not set it as
      // `stackedDimCoordDim` to avoid extent calculation, consider log scale.
      coordDim: v,
      coordDimIndex: y + 1,
      type: d,
      isExtraCoord: !0,
      isCalculationCoord: !0,
      storeDimIndex: a.length + 1
    };
    o ? (s && (p.storeDimIndex = s.ensureCalculationDimension(v, d), g.storeDimIndex = s.ensureCalculationDimension(h, d)), o.appendCalculationDimension(p), o.appendCalculationDimension(g)) : (a.push(p), a.push(g));
  }
  return {
    stackedDimension: f && f.name,
    stackedByDimension: l && l.name,
    isStackedByIndex: n,
    stackedOverDimension: v,
    stackResultDimension: h
  };
}
function yC(e) {
  return !t_(e.schema);
}
function zf(e, t) {
  return !!t && t === e.getCalculationInfo("stackedDimension");
}
function s_(e, t) {
  return zf(e, t) ? e.getCalculationInfo("stackResultDimension") : t;
}
function _C(e, t) {
  var r = e.get("coordinateSystem"), n = ns.get(r), i;
  return t && t.coordSysDims && (i = F(t.coordSysDims, function(a) {
    var o = {
      name: a
    }, s = t.axisMap.get(a);
    if (s) {
      var u = s.get("type");
      o.type = rC(u);
    }
    return o;
  })), i || (i = n && (n.getDimensionsInfo ? n.getDimensionsInfo() : n.dimensions.slice()) || ["x", "y"]), i;
}
function mC(e, t, r) {
  var n, i;
  return r && x(e, function(a, o) {
    var s = a.coordDim, u = r.categoryAxisMap.get(s);
    u && (n == null && (n = o), a.ordinalMeta = u.getOrdinalMeta(), t && (a.createInvertedIndices = !0)), a.otherDims.itemName != null && (i = !0);
  }), !i && n != null && (e[n].otherDims.itemName = 0), n;
}
function SC(e, t, r) {
  r = r || {};
  var n = t.getSourceManager(), i, a = !1;
  i = n.getSource(), a = i.sourceFormat === le;
  var o = pC(t), s = _C(t, o), u = r.useEncodeDefaulter, l = U(u) ? u : u ? pt(lb, s, t) : null, f = {
    coordDimensions: s,
    generateCoord: r.generateCoord,
    encodeDefine: t.getEncode(),
    encodeDefaulter: l,
    canOmitUnusedDimensions: !a
  }, h = a_(i, f), v = mC(h.dimensions, r.createInvertedIndices, o), c = a ? null : n.getSharedDataStore(h), d = o_(t, {
    schema: h,
    store: c
  }), y = new i_(h, t);
  y.setCalculationInfo(d);
  var p = v != null && wC(i) ? function(g, _, m, S) {
    return S === v ? m : this.defaultDimValueGetter(g, _, m, S);
  } : null;
  return y.hasItemOption = !1, y.initData(
    // Try to reuse the data store in sourceManager if using dataset.
    a ? i : c,
    null,
    p
  ), y;
}
function wC(e) {
  if (e.sourceFormat === le) {
    var t = bC(e.data || []);
    return !k(ji(t));
  }
}
function bC(e) {
  for (var t = 0; t < e.length && e[t] == null; )
    t++;
  return e[t];
}
var Ee = (
  /** @class */
  (function() {
    function e(t) {
      this._setting = t || {}, this._extent = [1 / 0, -1 / 0];
    }
    return e.prototype.getSetting = function(t) {
      return this._setting[t];
    }, e.prototype.unionExtent = function(t) {
      var r = this._extent;
      t[0] < r[0] && (r[0] = t[0]), t[1] > r[1] && (r[1] = t[1]);
    }, e.prototype.unionExtentFromData = function(t, r) {
      this.unionExtent(t.getApproximateExtent(r));
    }, e.prototype.getExtent = function() {
      return this._extent.slice();
    }, e.prototype.setExtent = function(t, r) {
      var n = this._extent;
      isNaN(t) || (n[0] = t), isNaN(r) || (n[1] = r);
    }, e.prototype.isInExtentRange = function(t) {
      return this._extent[0] <= t && this._extent[1] >= t;
    }, e.prototype.isBlank = function() {
      return this._isBlank;
    }, e.prototype.setBlank = function(t) {
      this._isBlank = t;
    }, e;
  })()
);
No(Ee);
var TC = 0, Ev = (
  /** @class */
  (function() {
    function e(t) {
      this.categories = t.categories || [], this._needCollect = t.needCollect, this._deduplication = t.deduplication, this.uid = ++TC;
    }
    return e.createByAxisModel = function(t) {
      var r = t.option, n = r.data, i = n && F(n, xC);
      return new e({
        categories: i,
        needCollect: !i,
        // deduplication is default in axis.
        deduplication: r.dedplication !== !1
      });
    }, e.prototype.getOrdinal = function(t) {
      return this._getOrCreateMap().get(t);
    }, e.prototype.parseAndCollect = function(t) {
      var r, n = this._needCollect;
      if (!B(t) && !n)
        return t;
      if (n && !this._deduplication)
        return r = this.categories.length, this.categories[r] = t, r;
      var i = this._getOrCreateMap();
      return r = i.get(t), r == null && (n ? (r = this.categories.length, this.categories[r] = t, i.set(t, r)) : r = NaN), r;
    }, e.prototype._getOrCreateMap = function() {
      return this._map || (this._map = W(this.categories));
    }, e;
  })()
);
function xC(e) {
  return N(e) && e.value != null ? e.value : e + "";
}
function sP(e) {
  return e.type === "interval" || e.type === "log";
}
function CC(e, t, r, n) {
  var i = {}, a = e[1] - e[0], o = i.interval = Zl(a / t, !0);
  r != null && o < r && (o = i.interval = r), n != null && o > n && (o = i.interval = n);
  var s = i.intervalPrecision = u_(o), u = i.niceTickExtent = [Ht(Math.ceil(e[0] / o) * o, s), Ht(Math.floor(e[1] / o) * o, s)];
  return MC(u, e), i;
}
function uP(e) {
  var t = Math.pow(10, Fo(e)), r = e / t;
  return r ? r === 2 ? r = 3 : r === 3 ? r = 5 : r *= 2 : r = 1, Ht(r * t);
}
function u_(e) {
  return Te(e) + 2;
}
function Ov(e, t, r) {
  e[t] = Math.max(Math.min(e[t], r[1]), r[0]);
}
function MC(e, t) {
  !isFinite(e[0]) && (e[0] = t[0]), !isFinite(e[1]) && (e[1] = t[1]), Ov(e, 0, t), Ov(e, 1, t), e[0] > e[1] && (e[0] = e[1]);
}
function ls(e, t) {
  return e >= t[0] && e <= t[1];
}
function fs(e, t) {
  return t[1] === t[0] ? 0.5 : (e - t[0]) / (t[1] - t[0]);
}
function hs(e, t) {
  return e * (t[1] - t[0]) + t[0];
}
var Hf = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t(r) {
      var n = e.call(this, r) || this;
      n.type = "ordinal";
      var i = n.getSetting("ordinalMeta");
      return i || (i = new Ev({})), k(i) && (i = new Ev({
        categories: F(i, function(a) {
          return N(a) ? a.value : a;
        })
      })), n._ordinalMeta = i, n._extent = n.getSetting("extent") || [0, i.categories.length - 1], n;
    }
    return t.prototype.parse = function(r) {
      return r == null ? NaN : B(r) ? this._ordinalMeta.getOrdinal(r) : Math.round(r);
    }, t.prototype.contain = function(r) {
      return r = this.parse(r), ls(r, this._extent) && this._ordinalMeta.categories[r] != null;
    }, t.prototype.normalize = function(r) {
      return r = this._getTickNumber(this.parse(r)), fs(r, this._extent);
    }, t.prototype.scale = function(r) {
      return r = Math.round(hs(r, this._extent)), this.getRawOrdinalNumber(r);
    }, t.prototype.getTicks = function() {
      for (var r = [], n = this._extent, i = n[0]; i <= n[1]; )
        r.push({
          value: i
        }), i++;
      return r;
    }, t.prototype.getMinorTicks = function(r) {
    }, t.prototype.setSortInfo = function(r) {
      if (r == null) {
        this._ordinalNumbersByTick = this._ticksByOrdinalNumber = null;
        return;
      }
      for (var n = r.ordinalNumbers, i = this._ordinalNumbersByTick = [], a = this._ticksByOrdinalNumber = [], o = 0, s = this._ordinalMeta.categories.length, u = Math.min(s, n.length); o < u; ++o) {
        var l = n[o];
        i[o] = l, a[l] = o;
      }
      for (var f = 0; o < s; ++o) {
        for (; a[f] != null; )
          f++;
        i.push(f), a[f] = o;
      }
    }, t.prototype._getTickNumber = function(r) {
      var n = this._ticksByOrdinalNumber;
      return n && r >= 0 && r < n.length ? n[r] : r;
    }, t.prototype.getRawOrdinalNumber = function(r) {
      var n = this._ordinalNumbersByTick;
      return n && r >= 0 && r < n.length ? n[r] : r;
    }, t.prototype.getLabel = function(r) {
      if (!this.isBlank()) {
        var n = this.getRawOrdinalNumber(r.value), i = this._ordinalMeta.categories[n];
        return i == null ? "" : i + "";
      }
    }, t.prototype.count = function() {
      return this._extent[1] - this._extent[0] + 1;
    }, t.prototype.unionExtentFromData = function(r, n) {
      this.unionExtent(r.getApproximateExtent(n));
    }, t.prototype.isInExtentRange = function(r) {
      return r = this._getTickNumber(r), this._extent[0] <= r && this._extent[1] >= r;
    }, t.prototype.getOrdinalMeta = function() {
      return this._ordinalMeta;
    }, t.prototype.calcNiceTicks = function() {
    }, t.prototype.calcNiceExtent = function() {
    }, t.type = "ordinal", t;
  })(Ee)
);
Ee.registerClass(Hf);
var Fr = Ht, ia = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = "interval", r._interval = 0, r._intervalPrecision = 2, r;
    }
    return t.prototype.parse = function(r) {
      return r;
    }, t.prototype.contain = function(r) {
      return ls(r, this._extent);
    }, t.prototype.normalize = function(r) {
      return fs(r, this._extent);
    }, t.prototype.scale = function(r) {
      return hs(r, this._extent);
    }, t.prototype.setExtent = function(r, n) {
      var i = this._extent;
      isNaN(r) || (i[0] = parseFloat(r)), isNaN(n) || (i[1] = parseFloat(n));
    }, t.prototype.unionExtent = function(r) {
      var n = this._extent;
      r[0] < n[0] && (n[0] = r[0]), r[1] > n[1] && (n[1] = r[1]), this.setExtent(n[0], n[1]);
    }, t.prototype.getInterval = function() {
      return this._interval;
    }, t.prototype.setInterval = function(r) {
      this._interval = r, this._niceExtent = this._extent.slice(), this._intervalPrecision = u_(r);
    }, t.prototype.getTicks = function(r) {
      var n = this._interval, i = this._extent, a = this._niceExtent, o = this._intervalPrecision, s = [];
      if (!n)
        return s;
      var u = 1e4;
      i[0] < a[0] && (r ? s.push({
        value: Fr(a[0] - n, o)
      }) : s.push({
        value: i[0]
      }));
      for (var l = a[0]; l <= a[1] && (s.push({
        value: l
      }), l = Fr(l + n, o), l !== s[s.length - 1].value); )
        if (s.length > u)
          return [];
      var f = s.length ? s[s.length - 1].value : a[1];
      return i[1] > f && (r ? s.push({
        value: Fr(f + n, o)
      }) : s.push({
        value: i[1]
      })), s;
    }, t.prototype.getMinorTicks = function(r) {
      for (var n = this.getTicks(!0), i = [], a = this.getExtent(), o = 1; o < n.length; o++) {
        for (var s = n[o], u = n[o - 1], l = 0, f = [], h = s.value - u.value, v = h / r; l < r - 1; ) {
          var c = Fr(u.value + (l + 1) * v);
          c > a[0] && c < a[1] && f.push(c), l++;
        }
        i.push(f);
      }
      return i;
    }, t.prototype.getLabel = function(r, n) {
      if (r == null)
        return "";
      var i = n && n.precision;
      i == null ? i = Te(r.value) || 0 : i === "auto" && (i = this._intervalPrecision);
      var a = Fr(r.value, i, !0);
      return _f(a);
    }, t.prototype.calcNiceTicks = function(r, n, i) {
      r = r || 5;
      var a = this._extent, o = a[1] - a[0];
      if (isFinite(o)) {
        o < 0 && (o = -o, a.reverse());
        var s = CC(a, r, n, i);
        this._intervalPrecision = s.intervalPrecision, this._interval = s.interval, this._niceExtent = s.niceTickExtent;
      }
    }, t.prototype.calcNiceExtent = function(r) {
      var n = this._extent;
      if (n[0] === n[1])
        if (n[0] !== 0) {
          var i = Math.abs(n[0]);
          r.fixMax || (n[1] += i / 2), n[0] -= i / 2;
        } else
          n[1] = 1;
      var a = n[1] - n[0];
      isFinite(a) || (n[0] = 0, n[1] = 1), this.calcNiceTicks(r.splitNumber, r.minInterval, r.maxInterval);
      var o = this._interval;
      r.fixMin || (n[0] = Fr(Math.floor(n[0] / o) * o)), r.fixMax || (n[1] = Fr(Math.ceil(n[1] / o) * o));
    }, t.prototype.setNiceExtent = function(r, n) {
      this._niceExtent = [r, n];
    }, t.type = "interval", t;
  })(Ee)
);
Ee.registerClass(ia);
var l_ = typeof Float32Array < "u", DC = l_ ? Float32Array : Array;
function mu(e) {
  return k(e) ? l_ ? new Float32Array(e) : e : new DC(e);
}
var PC = "__ec_stack_";
function f_(e) {
  return e.get("stack") || PC + e.seriesIndex;
}
function Gf(e) {
  return e.dim + e.index;
}
function h_(e, t) {
  var r = [];
  return t.eachSeriesByType(e, function(n) {
    v_(n) && r.push(n);
  }), r;
}
function IC(e) {
  var t = {};
  x(e, function(u) {
    var l = u.coordinateSystem, f = l.getBaseAxis();
    if (!(f.type !== "time" && f.type !== "value"))
      for (var h = u.getData(), v = f.dim + "_" + f.index, c = h.getDimensionIndex(h.mapDimension(f.dim)), d = h.getStore(), y = 0, p = d.count(); y < p; ++y) {
        var g = d.get(c, y);
        t[v] ? t[v].push(g) : t[v] = [g];
      }
  });
  var r = {};
  for (var n in t)
    if (t.hasOwnProperty(n)) {
      var i = t[n];
      if (i) {
        i.sort(function(u, l) {
          return u - l;
        });
        for (var a = null, o = 1; o < i.length; ++o) {
          var s = i[o] - i[o - 1];
          s > 0 && (a = a === null ? s : Math.min(a, s));
        }
        r[n] = a;
      }
    }
  return r;
}
function c_(e) {
  var t = IC(e), r = [];
  return x(e, function(n) {
    var i = n.coordinateSystem, a = i.getBaseAxis(), o = a.getExtent(), s;
    if (a.type === "category")
      s = a.getBandWidth();
    else if (a.type === "value" || a.type === "time") {
      var u = a.dim + "_" + a.index, l = t[u], f = Math.abs(o[1] - o[0]), h = a.scale.getExtent(), v = Math.abs(h[1] - h[0]);
      s = l ? f / v * l : f;
    } else {
      var c = n.getData();
      s = Math.abs(o[1] - o[0]) / c.count();
    }
    var d = It(n.get("barWidth"), s), y = It(n.get("barMaxWidth"), s), p = It(
      // barMinWidth by default is 0.5 / 1 in cartesian. Because in value axis,
      // the auto-calculated bar width might be less than 0.5 / 1.
      n.get("barMinWidth") || (d_(n) ? 0.5 : 1),
      s
    ), g = n.get("barGap"), _ = n.get("barCategoryGap");
    r.push({
      bandWidth: s,
      barWidth: d,
      barMaxWidth: y,
      barMinWidth: p,
      barGap: g,
      barCategoryGap: _,
      axisKey: Gf(a),
      stackId: f_(n)
    });
  }), AC(r);
}
function AC(e) {
  var t = {};
  x(e, function(n, i) {
    var a = n.axisKey, o = n.bandWidth, s = t[a] || {
      bandWidth: o,
      remainedWidth: o,
      autoWidthCount: 0,
      categoryGap: null,
      gap: "20%",
      stacks: {}
    }, u = s.stacks;
    t[a] = s;
    var l = n.stackId;
    u[l] || s.autoWidthCount++, u[l] = u[l] || {
      width: 0,
      maxWidth: 0
    };
    var f = n.barWidth;
    f && !u[l].width && (u[l].width = f, f = Math.min(s.remainedWidth, f), s.remainedWidth -= f);
    var h = n.barMaxWidth;
    h && (u[l].maxWidth = h);
    var v = n.barMinWidth;
    v && (u[l].minWidth = v);
    var c = n.barGap;
    c != null && (s.gap = c);
    var d = n.barCategoryGap;
    d != null && (s.categoryGap = d);
  });
  var r = {};
  return x(t, function(n, i) {
    r[i] = {};
    var a = n.stacks, o = n.bandWidth, s = n.categoryGap;
    if (s == null) {
      var u = ct(a).length;
      s = Math.max(35 - u * 4, 15) + "%";
    }
    var l = It(s, o), f = It(n.gap, 1), h = n.remainedWidth, v = n.autoWidthCount, c = (h - l) / (v + (v - 1) * f);
    c = Math.max(c, 0), x(a, function(g) {
      var _ = g.maxWidth, m = g.minWidth;
      if (g.width) {
        var S = g.width;
        _ && (S = Math.min(S, _)), m && (S = Math.max(S, m)), g.width = S, h -= S + f * S, v--;
      } else {
        var S = c;
        _ && _ < S && (S = Math.min(_, h)), m && m > S && (S = m), S !== c && (g.width = S, h -= S + f * S, v--);
      }
    }), c = (h - l) / (v + (v - 1) * f), c = Math.max(c, 0);
    var d = 0, y;
    x(a, function(g, _) {
      g.width || (g.width = c), y = g, d += g.width * (1 + f);
    }), y && (d -= y.width * f);
    var p = -d / 2;
    x(a, function(g, _) {
      r[i][_] = r[i][_] || {
        bandWidth: o,
        offset: p,
        width: g.width
      }, p += g.width * (1 + f);
    });
  }), r;
}
function LC(e, t, r) {
  if (e && t) {
    var n = e[Gf(t)];
    return n;
  }
}
function lP(e, t) {
  var r = h_(e, t), n = c_(r);
  x(r, function(i) {
    var a = i.getData(), o = i.coordinateSystem, s = o.getBaseAxis(), u = f_(i), l = n[Gf(s)][u], f = l.offset, h = l.width;
    a.setLayout({
      bandWidth: l.bandWidth,
      offset: f,
      size: h
    });
  });
}
function fP(e) {
  return {
    seriesType: e,
    plan: yy(),
    reset: function(t) {
      if (v_(t)) {
        var r = t.getData(), n = t.coordinateSystem, i = n.getBaseAxis(), a = n.getOtherAxis(i), o = r.getDimensionIndex(r.mapDimension(a.dim)), s = r.getDimensionIndex(r.mapDimension(i.dim)), u = t.get("showBackground", !0), l = r.mapDimension(a.dim), f = r.getCalculationInfo("stackResultDimension"), h = zf(r, l) && !!r.getCalculationInfo("stackedOnSeries"), v = a.isHorizontal(), c = RC(i, a), d = d_(t), y = t.get("barMinHeight") || 0, p = f && r.getDimensionIndex(f), g = r.getLayout("size"), _ = r.getLayout("offset");
        return {
          progress: function(m, S) {
            for (var b = m.count, w = d && mu(b * 3), T = d && u && mu(b * 3), C = d && mu(b), M = n.master.getRect(), D = v ? M.width : M.height, P, A = S.getStore(), L = 0; (P = m.next()) != null; ) {
              var I = A.get(h ? p : o, P), R = A.get(s, P), O = c, q = void 0;
              h && (q = +I - A.get(o, P));
              var H = void 0, G = void 0, X = void 0, nt = void 0;
              if (v) {
                var J = n.dataToPoint([I, R]);
                if (h) {
                  var mt = n.dataToPoint([q, R]);
                  O = mt[0];
                }
                H = O, G = J[1] + _, X = J[0] - O, nt = g, Math.abs(X) < y && (X = (X < 0 ? -1 : 1) * y);
              } else {
                var J = n.dataToPoint([R, I]);
                if (h) {
                  var mt = n.dataToPoint([R, q]);
                  O = mt[1];
                }
                H = J[0] + _, G = O, X = g, nt = J[1] - O, Math.abs(nt) < y && (nt = (nt <= 0 ? -1 : 1) * y);
              }
              d ? (w[L] = H, w[L + 1] = G, w[L + 2] = v ? X : nt, T && (T[L] = v ? M.x : H, T[L + 1] = v ? G : M.y, T[L + 2] = D), C[P] = P) : S.setItemLayout(P, {
                x: H,
                y: G,
                width: X,
                height: nt
              }), L += 3;
            }
            d && S.setLayout({
              largePoints: w,
              largeDataIndices: C,
              largeBackgroundPoints: T,
              valueAxisHorizontal: v
            });
          }
        };
      }
    }
  };
}
function v_(e) {
  return e.coordinateSystem && e.coordinateSystem.type === "cartesian2d";
}
function d_(e) {
  return e.pipelineContext && e.pipelineContext.large;
}
function RC(e, t) {
  var r = t.model.get("startValue");
  return r || (r = 0), t.toGlobalCoord(t.dataToCoord(t.type === "log" ? r > 0 ? r : 1 : r));
}
var EC = function(e, t, r, n) {
  for (; r < n; ) {
    var i = r + n >>> 1;
    e[i][1] < t ? r = i + 1 : n = i;
  }
  return r;
}, p_ = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t(r) {
      var n = e.call(this, r) || this;
      return n.type = "time", n;
    }
    return t.prototype.getLabel = function(r) {
      var n = this.getSetting("useUTC");
      return ra(r.value, Cc[Kw(Mn(this._minLevelUnit))] || Cc.second, n, this.getSetting("locale"));
    }, t.prototype.getFormattedLabel = function(r, n, i) {
      var a = this.getSetting("useUTC"), o = this.getSetting("locale");
      return Qw(r, n, i, o, a);
    }, t.prototype.getTicks = function() {
      var r = this._interval, n = this._extent, i = [];
      if (!r)
        return i;
      i.push({
        value: n[0],
        level: 0
      });
      var a = this.getSetting("useUTC"), o = HC(this._minLevelUnit, this._approxInterval, a, n);
      return i = i.concat(o), i.push({
        value: n[1],
        level: 0
      }), i;
    }, t.prototype.calcNiceExtent = function(r) {
      var n = this._extent;
      if (n[0] === n[1] && (n[0] -= ae, n[1] += ae), n[1] === -1 / 0 && n[0] === 1 / 0) {
        var i = /* @__PURE__ */ new Date();
        n[1] = +new Date(i.getFullYear(), i.getMonth(), i.getDate()), n[0] = n[1] - ae;
      }
      this.calcNiceTicks(r.splitNumber, r.minInterval, r.maxInterval);
    }, t.prototype.calcNiceTicks = function(r, n, i) {
      r = r || 10;
      var a = this._extent, o = a[1] - a[0];
      this._approxInterval = o / r, n != null && this._approxInterval < n && (this._approxInterval = n), i != null && this._approxInterval > i && (this._approxInterval = i);
      var s = Ba.length, u = Math.min(EC(Ba, this._approxInterval, 0, s), s - 1);
      this._interval = Ba[u][1], this._minLevelUnit = Ba[Math.max(u - 1, 0)][0];
    }, t.prototype.parse = function(r) {
      return ut(r) ? r : +se(r);
    }, t.prototype.contain = function(r) {
      return ls(this.parse(r), this._extent);
    }, t.prototype.normalize = function(r) {
      return fs(this.parse(r), this._extent);
    }, t.prototype.scale = function(r) {
      return hs(r, this._extent);
    }, t.type = "time", t;
  })(ia)
), Ba = [
  // Format                           interval
  ["second", pf],
  ["minute", gf],
  ["hour", xi],
  ["quarter-day", xi * 6],
  ["half-day", xi * 12],
  ["day", ae * 1.2],
  ["half-week", ae * 3.5],
  ["week", ae * 7],
  ["month", ae * 31],
  ["quarter", ae * 95],
  ["half-year", xc / 2],
  ["year", xc]
  // 1Y
];
function OC(e, t, r, n) {
  var i = se(t), a = se(r), o = function(d) {
    return Mc(i, d, n) === Mc(a, d, n);
  }, s = function() {
    return o("year");
  }, u = function() {
    return s() && o("month");
  }, l = function() {
    return u() && o("day");
  }, f = function() {
    return l() && o("hour");
  }, h = function() {
    return f() && o("minute");
  }, v = function() {
    return h() && o("second");
  }, c = function() {
    return v() && o("millisecond");
  };
  switch (e) {
    case "year":
      return s();
    case "month":
      return u();
    case "day":
      return l();
    case "hour":
      return f();
    case "minute":
      return h();
    case "second":
      return v();
    case "millisecond":
      return c();
  }
}
function kC(e, t) {
  return e /= ae, e > 16 ? 16 : e > 7.5 ? 7 : e > 3.5 ? 4 : e > 1.5 ? 2 : 1;
}
function BC(e) {
  var t = 30 * ae;
  return e /= t, e > 6 ? 6 : e > 3 ? 3 : e > 2 ? 2 : 1;
}
function FC(e) {
  return e /= xi, e > 12 ? 12 : e > 6 ? 6 : e > 3.5 ? 4 : e > 2 ? 2 : 1;
}
function kv(e, t) {
  return e /= t ? gf : pf, e > 30 ? 30 : e > 20 ? 20 : e > 15 ? 15 : e > 10 ? 10 : e > 5 ? 5 : e > 2 ? 2 : 1;
}
function NC(e) {
  return Zl(e, !0);
}
function zC(e, t, r) {
  var n = new Date(e);
  switch (Mn(t)) {
    case "year":
    case "month":
      n[Fg(r)](0);
    case "day":
      n[Ng(r)](1);
    case "hour":
      n[zg(r)](0);
    case "minute":
      n[Hg(r)](0);
    case "second":
      n[Gg(r)](0), n[Vg(r)](0);
  }
  return n.getTime();
}
function HC(e, t, r, n) {
  var i = 1e4, a = kg, o = 0;
  function s(D, P, A, L, I, R, O) {
    for (var q = new Date(P), H = P, G = q[L](); H < A && H <= n[1]; )
      O.push({
        value: H
      }), G += D, q[I](G), H = q.getTime();
    O.push({
      value: H,
      notAdd: !0
    });
  }
  function u(D, P, A) {
    var L = [], I = !P.length;
    if (!OC(Mn(D), n[0], n[1], r)) {
      I && (P = [{
        // TODO Optimize. Not include so may ticks.
        value: zC(new Date(n[0]), D, r)
      }, {
        value: n[1]
      }]);
      for (var R = 0; R < P.length - 1; R++) {
        var O = P[R].value, q = P[R + 1].value;
        if (O !== q) {
          var H = void 0, G = void 0, X = void 0, nt = !1;
          switch (D) {
            case "year":
              H = Math.max(1, Math.round(t / ae / 365)), G = yf(r), X = Jw(r);
              break;
            case "half-year":
            case "quarter":
            case "month":
              H = BC(t), G = Dn(r), X = Fg(r);
              break;
            case "week":
            // PENDING If week is added. Ignore day.
            case "half-week":
            case "day":
              H = kC(t), G = Jo(r), X = Ng(r), nt = !0;
              break;
            case "half-day":
            case "quarter-day":
            case "hour":
              H = FC(t), G = Fi(r), X = zg(r);
              break;
            case "minute":
              H = kv(t, !0), G = ts(r), X = Hg(r);
              break;
            case "second":
              H = kv(t, !1), G = es(r), X = Gg(r);
              break;
            case "millisecond":
              H = NC(t), G = rs(r), X = Vg(r);
              break;
          }
          s(H, O, q, G, X, nt, L), D === "year" && A.length > 1 && R === 0 && A.unshift({
            value: A[0].value - H
          });
        }
      }
      for (var R = 0; R < L.length; R++)
        A.push(L[R]);
      return L;
    }
  }
  for (var l = [], f = [], h = 0, v = 0, c = 0; c < a.length && o++ < i; ++c) {
    var d = Mn(a[c]);
    if (jw(a[c])) {
      u(a[c], l[l.length - 1] || [], f);
      var y = a[c + 1] ? Mn(a[c + 1]) : null;
      if (d !== y) {
        if (f.length) {
          v = h, f.sort(function(D, P) {
            return D.value - P.value;
          });
          for (var p = [], g = 0; g < f.length; ++g) {
            var _ = f[g].value;
            (g === 0 || f[g - 1].value !== _) && (p.push(f[g]), _ >= n[0] && _ <= n[1] && h++);
          }
          var m = (n[1] - n[0]) / t;
          if (h > m * 1.5 && v > m / 1.5 || (l.push(p), h > m || e === a[c]))
            break;
        }
        f = [];
      }
    }
  }
  for (var S = dt(F(l, function(D) {
    return dt(D, function(P) {
      return P.value >= n[0] && P.value <= n[1] && !P.notAdd;
    });
  }), function(D) {
    return D.length > 0;
  }), b = [], w = S.length - 1, c = 0; c < S.length; ++c)
    for (var T = S[c], C = 0; C < T.length; ++C)
      b.push({
        value: T[C].value,
        level: w - c
      });
  b.sort(function(D, P) {
    return D.value - P.value;
  });
  for (var M = [], c = 0; c < b.length; ++c)
    (c === 0 || b[c].value !== b[c - 1].value) && M.push(b[c]);
  return M;
}
Ee.registerClass(p_);
var Bv = Ee.prototype, Pi = ia.prototype, GC = Ht, VC = Math.floor, WC = Math.ceil, Fa = Math.pow, he = Math.log, Vf = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = "log", r.base = 10, r._originalScale = new ia(), r._interval = 0, r;
    }
    return t.prototype.getTicks = function(r) {
      var n = this._originalScale, i = this._extent, a = n.getExtent(), o = Pi.getTicks.call(this, r);
      return F(o, function(s) {
        var u = s.value, l = Ht(Fa(this.base, u));
        return l = u === i[0] && this._fixMin ? Na(l, a[0]) : l, l = u === i[1] && this._fixMax ? Na(l, a[1]) : l, {
          value: l
        };
      }, this);
    }, t.prototype.setExtent = function(r, n) {
      var i = he(this.base);
      r = he(Math.max(0, r)) / i, n = he(Math.max(0, n)) / i, Pi.setExtent.call(this, r, n);
    }, t.prototype.getExtent = function() {
      var r = this.base, n = Bv.getExtent.call(this);
      n[0] = Fa(r, n[0]), n[1] = Fa(r, n[1]);
      var i = this._originalScale, a = i.getExtent();
      return this._fixMin && (n[0] = Na(n[0], a[0])), this._fixMax && (n[1] = Na(n[1], a[1])), n;
    }, t.prototype.unionExtent = function(r) {
      this._originalScale.unionExtent(r);
      var n = this.base;
      r[0] = he(r[0]) / he(n), r[1] = he(r[1]) / he(n), Bv.unionExtent.call(this, r);
    }, t.prototype.unionExtentFromData = function(r, n) {
      this.unionExtent(r.getApproximateExtent(n));
    }, t.prototype.calcNiceTicks = function(r) {
      r = r || 10;
      var n = this._extent, i = n[1] - n[0];
      if (!(i === 1 / 0 || i <= 0)) {
        var a = kp(i), o = r / i * a;
        for (o <= 0.5 && (a *= 10); !isNaN(a) && Math.abs(a) < 1 && Math.abs(a) > 0; )
          a *= 10;
        var s = [Ht(WC(n[0] / a) * a), Ht(VC(n[1] / a) * a)];
        this._interval = a, this._niceExtent = s;
      }
    }, t.prototype.calcNiceExtent = function(r) {
      Pi.calcNiceExtent.call(this, r), this._fixMin = r.fixMin, this._fixMax = r.fixMax;
    }, t.prototype.parse = function(r) {
      return r;
    }, t.prototype.contain = function(r) {
      return r = he(r) / he(this.base), ls(r, this._extent);
    }, t.prototype.normalize = function(r) {
      return r = he(r) / he(this.base), fs(r, this._extent);
    }, t.prototype.scale = function(r) {
      return r = hs(r, this._extent), Fa(this.base, r);
    }, t.type = "log", t;
  })(Ee)
), g_ = Vf.prototype;
g_.getMinorTicks = Pi.getMinorTicks;
g_.getLabel = Pi.getLabel;
function Na(e, t) {
  return GC(e, Te(t));
}
Ee.registerClass(Vf);
var UC = (
  /** @class */
  (function() {
    function e(t, r, n) {
      this._prepareParams(t, r, n);
    }
    return e.prototype._prepareParams = function(t, r, n) {
      n[1] < n[0] && (n = [NaN, NaN]), this._dataMin = n[0], this._dataMax = n[1];
      var i = this._isOrdinal = t.type === "ordinal";
      this._needCrossZero = t.type === "interval" && r.getNeedCrossZero && r.getNeedCrossZero();
      var a = r.get("min", !0);
      a == null && (a = r.get("startValue", !0));
      var o = this._modelMinRaw = a;
      U(o) ? this._modelMinNum = za(t, o({
        min: n[0],
        max: n[1]
      })) : o !== "dataMin" && (this._modelMinNum = za(t, o));
      var s = this._modelMaxRaw = r.get("max", !0);
      if (U(s) ? this._modelMaxNum = za(t, s({
        min: n[0],
        max: n[1]
      })) : s !== "dataMax" && (this._modelMaxNum = za(t, s)), i)
        this._axisDataLen = r.getCategories().length;
      else {
        var u = r.get("boundaryGap"), l = k(u) ? u : [u || 0, u || 0];
        typeof l[0] == "boolean" || typeof l[1] == "boolean" ? this._boundaryGapInner = [0, 0] : this._boundaryGapInner = [Kr(l[0], 1), Kr(l[1], 1)];
      }
    }, e.prototype.calculate = function() {
      var t = this._isOrdinal, r = this._dataMin, n = this._dataMax, i = this._axisDataLen, a = this._boundaryGapInner, o = t ? null : n - r || Math.abs(r), s = this._modelMinRaw === "dataMin" ? r : this._modelMinNum, u = this._modelMaxRaw === "dataMax" ? n : this._modelMaxNum, l = s != null, f = u != null;
      s == null && (s = t ? i ? 0 : NaN : r - a[0] * o), u == null && (u = t ? i ? i - 1 : NaN : n + a[1] * o), (s == null || !isFinite(s)) && (s = NaN), (u == null || !isFinite(u)) && (u = NaN);
      var h = Ii(s) || Ii(u) || t && !i;
      this._needCrossZero && (s > 0 && u > 0 && !l && (s = 0), s < 0 && u < 0 && !f && (u = 0));
      var v = this._determinedMin, c = this._determinedMax;
      return v != null && (s = v, l = !0), c != null && (u = c, f = !0), {
        min: s,
        max: u,
        minFixed: l,
        maxFixed: f,
        isBlank: h
      };
    }, e.prototype.modifyDataMinMax = function(t, r) {
      this[qC[t]] = r;
    }, e.prototype.setDeterminedMinMax = function(t, r) {
      var n = YC[t];
      this[n] = r;
    }, e.prototype.freeze = function() {
      this.frozen = !0;
    }, e;
  })()
), YC = {
  min: "_determinedMin",
  max: "_determinedMax"
}, qC = {
  min: "_dataMin",
  max: "_dataMax"
};
function XC(e, t, r) {
  var n = e.rawExtentInfo;
  return n || (n = new UC(e, t, r), e.rawExtentInfo = n, n);
}
function za(e, t) {
  return t == null ? null : Ii(t) ? NaN : e.parse(t);
}
function $C(e, t) {
  var r = e.type, n = XC(e, t, e.getExtent()).calculate();
  e.setBlank(n.isBlank);
  var i = n.min, a = n.max, o = t.ecModel;
  if (o && r === "time") {
    var s = h_("bar", o), u = !1;
    if (x(s, function(h) {
      u = u || h.getBaseAxis() === t.axis;
    }), u) {
      var l = c_(s), f = ZC(i, a, t, l);
      i = f.min, a = f.max;
    }
  }
  return {
    extent: [i, a],
    // "fix" means "fixed", the value should not be
    // changed in the subsequent steps.
    fixMin: n.minFixed,
    fixMax: n.maxFixed
  };
}
function ZC(e, t, r, n) {
  var i = r.axis.getExtent(), a = Math.abs(i[1] - i[0]), o = LC(n, r.axis);
  if (o === void 0)
    return {
      min: e,
      max: t
    };
  var s = 1 / 0;
  x(o, function(c) {
    s = Math.min(c.offset, s);
  });
  var u = -1 / 0;
  x(o, function(c) {
    u = Math.max(c.offset + c.width, u);
  }), s = Math.abs(s), u = Math.abs(u);
  var l = s + u, f = t - e, h = 1 - (s + u) / a, v = f / h - f;
  return t += v * (u / l), e -= v * (s / l), {
    min: e,
    max: t
  };
}
function jC(e, t) {
  var r = t, n = $C(e, r), i = n.extent, a = r.get("splitNumber");
  e instanceof Vf && (e.base = r.get("logBase"));
  var o = e.type, s = r.get("interval"), u = o === "interval" || o === "time";
  e.setExtent(i[0], i[1]), e.calcNiceExtent({
    splitNumber: a,
    fixMin: n.fixMin,
    fixMax: n.fixMax,
    minInterval: u ? r.get("minInterval") : null,
    maxInterval: u ? r.get("maxInterval") : null
  }), s != null && e.setInterval && e.setInterval(s);
}
function KC(e, t) {
  if (t = t || e.get("type"), t)
    switch (t) {
      // Buildin scale
      case "category":
        return new Hf({
          ordinalMeta: e.getOrdinalMeta ? e.getOrdinalMeta() : e.getCategories(),
          extent: [1 / 0, -1 / 0]
        });
      case "time":
        return new p_({
          locale: e.ecModel.getLocaleModel(),
          useUTC: e.ecModel.get("useUTC")
        });
      default:
        return new (Ee.getClass(t) || ia)();
    }
}
function hP(e) {
  var t = e.scale.getExtent(), r = t[0], n = t[1];
  return !(r > 0 && n > 0 || r < 0 && n < 0);
}
function Bn(e) {
  var t = e.getLabelModel().get("formatter"), r = e.type === "category" ? e.scale.getExtent()[0] : null;
  return e.scale.type === "time" ? /* @__PURE__ */ (function(n) {
    return function(i, a) {
      return e.scale.getFormattedLabel(i, a, n);
    };
  })(t) : B(t) ? /* @__PURE__ */ (function(n) {
    return function(i) {
      var a = e.scale.getLabel(i), o = n.replace("{value}", a ?? "");
      return o;
    };
  })(t) : U(t) ? /* @__PURE__ */ (function(n) {
    return function(i, a) {
      return r != null && (a = i.value - r), n(Wf(e, i), a, i.level != null ? {
        level: i.level
      } : null);
    };
  })(t) : function(n) {
    return e.scale.getLabel(n);
  };
}
function Wf(e, t) {
  return e.type === "category" ? e.scale.getLabel(t) : t.value;
}
function cP(e) {
  var t = e.model, r = e.scale;
  if (!(!t.get(["axisLabel", "show"]) || r.isBlank())) {
    var n, i, a = r.getExtent();
    r instanceof Hf ? i = r.count() : (n = r.getTicks(), i = n.length);
    var o = e.getLabelModel(), s = Bn(e), u, l = 1;
    i > 40 && (l = Math.ceil(i / 40));
    for (var f = 0; f < i; f += l) {
      var h = n ? n[f] : {
        value: a[0] + f
      }, v = s(h, f), c = o.getTextRect(v), d = QC(c, o.get("rotate") || 0);
      u ? u.union(d) : u = d;
    }
    return u;
  }
}
function QC(e, t) {
  var r = t * Math.PI / 180, n = e.width, i = e.height, a = n * Math.abs(Math.cos(r)) + Math.abs(i * Math.sin(r)), o = n * Math.abs(Math.sin(r)) + Math.abs(i * Math.cos(r)), s = new j(e.x, e.y, a, o);
  return s;
}
function Uf(e) {
  var t = e.get("interval");
  return t ?? "auto";
}
function y_(e) {
  return e.type === "category" && Uf(e.getLabelModel()) === 0;
}
function vP(e, t) {
  var r = {};
  return x(e.mapDimensionsAll(t), function(n) {
    r[s_(e, n)] = !0;
  }), ct(r);
}
var JC = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getNeedCrossZero = function() {
      var t = this.option;
      return !t.scale;
    }, e.prototype.getCoordSysModel = function() {
    }, e;
  })()
);
function tM(e) {
  return SC(null, e);
}
var eM = {
  isDimensionStacked: zf,
  enableDataStack: o_,
  getStackedDimension: s_
};
function rM(e, t) {
  var r = t;
  t instanceof st || (r = new st(t));
  var n = KC(r);
  return n.setExtent(e[0], e[1]), jC(n, r), n;
}
function nM(e) {
  ye(e, JC);
}
function iM(e, t) {
  return t = t || {}, Qr(e, null, null, t.state !== "normal");
}
const aM = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  createDimensions: fC,
  createList: tM,
  createScale: rM,
  createSymbol: os,
  createTextStyle: iM,
  dataStack: eM,
  enableHoverEmphasis: vo,
  getECData: at,
  getLayoutRect: mo,
  mixinAxisModelCommonMethods: nM
}, Symbol.toStringTag, { value: "Module" }));
var Fv = [], oM = {
  registerPreprocessor: kf,
  registerProcessor: Bf,
  registerPostInit: Xy,
  registerPostUpdate: $y,
  registerUpdateLifecycle: us,
  registerAction: en,
  registerCoordinateSystem: Zy,
  registerLayout: jy,
  registerVisual: dr,
  registerTransform: Qy,
  registerLoading: Nf,
  registerMap: Ky,
  registerImpl: Cx,
  PRIORITY: Ny,
  ComponentModel: Q,
  ComponentView: $t,
  SeriesModel: Ae,
  ChartView: De,
  // TODO Use ComponentModel and SeriesModel instead of Constructor
  registerComponentModel: function(e) {
    Q.registerClass(e);
  },
  registerComponentView: function(e) {
    $t.registerClass(e);
  },
  registerSeriesModel: function(e) {
    Ae.registerClass(e);
  },
  registerChartView: function(e) {
    De.registerClass(e);
  },
  registerSubTypeDefaulter: function(e, t) {
    Q.registerSubTypeDefaulter(e, t);
  },
  registerPainter: function(e, t) {
    Ap(e, t);
  }
};
function Rn(e) {
  if (k(e)) {
    x(e, function(t) {
      Rn(t);
    });
    return;
  }
  rt(Fv, e) >= 0 || (Fv.push(e), U(e) && (e = {
    install: e
  }), e.install(oM));
}
var sM = 1e-8;
function Nv(e, t) {
  return Math.abs(e - t) < sM;
}
function zv(e, t, r) {
  var n = 0, i = e[0];
  if (!i)
    return !1;
  for (var a = 1; a < e.length; a++) {
    var o = e[a];
    n += He(i[0], i[1], o[0], o[1], t, r), i = o;
  }
  var s = e[0];
  return (!Nv(i[0], s[0]) || !Nv(i[1], s[1])) && (n += He(i[0], i[1], s[0], s[1], t, r)), n !== 0;
}
var uM = [];
function Su(e, t) {
  for (var r = 0; r < e.length; r++)
    Yt(e[r], e[r], t);
}
function Hv(e, t, r, n) {
  for (var i = 0; i < e.length; i++) {
    var a = e[i];
    n && (a = n.project(a)), a && isFinite(a[0]) && isFinite(a[1]) && (er(t, t, a), rr(r, r, a));
  }
}
function lM(e) {
  for (var t = 0, r = 0, n = 0, i = e.length, a = e[i - 1][0], o = e[i - 1][1], s = 0; s < i; s++) {
    var u = e[s][0], l = e[s][1], f = a * l - u * o;
    t += f, r += (a + u) * f, n += (o + l) * f, a = u, o = l;
  }
  return t ? [r / t / 3, n / t / 3, t] : [e[0][0] || 0, e[0][1] || 0];
}
var __ = (
  /** @class */
  (function() {
    function e(t) {
      this.name = t;
    }
    return e.prototype.setCenter = function(t) {
      this._center = t;
    }, e.prototype.getCenter = function() {
      var t = this._center;
      return t || (t = this._center = this.calcCenter()), t;
    }, e;
  })()
), Gv = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t, r) {
      this.type = "polygon", this.exterior = t, this.interiors = r;
    }
    return e;
  })()
), Vv = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.type = "linestring", this.points = t;
    }
    return e;
  })()
), fM = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t(r, n, i) {
      var a = e.call(this, r) || this;
      return a.type = "geoJSON", a.geometries = n, a._center = i && [i[0], i[1]], a;
    }
    return t.prototype.calcCenter = function() {
      for (var r = this.geometries, n, i = 0, a = 0; a < r.length; a++) {
        var o = r[a], s = o.exterior, u = s && s.length;
        u > i && (n = o, i = u);
      }
      if (n)
        return lM(n.exterior);
      var l = this.getBoundingRect();
      return [l.x + l.width / 2, l.y + l.height / 2];
    }, t.prototype.getBoundingRect = function(r) {
      var n = this._rect;
      if (n && !r)
        return n;
      var i = [1 / 0, 1 / 0], a = [-1 / 0, -1 / 0], o = this.geometries;
      return x(o, function(s) {
        s.type === "polygon" ? Hv(s.exterior, i, a, r) : x(s.points, function(u) {
          Hv(u, i, a, r);
        });
      }), isFinite(i[0]) && isFinite(i[1]) && isFinite(a[0]) && isFinite(a[1]) || (i[0] = i[1] = a[0] = a[1] = 0), n = new j(i[0], i[1], a[0] - i[0], a[1] - i[1]), r || (this._rect = n), n;
    }, t.prototype.contain = function(r) {
      var n = this.getBoundingRect(), i = this.geometries;
      if (!n.contain(r[0], r[1]))
        return !1;
      t: for (var a = 0, o = i.length; a < o; a++) {
        var s = i[a];
        if (s.type === "polygon") {
          var u = s.exterior, l = s.interiors;
          if (zv(u, r[0], r[1])) {
            for (var f = 0; f < (l ? l.length : 0); f++)
              if (zv(l[f], r[0], r[1]))
                continue t;
            return !0;
          }
        }
      }
      return !1;
    }, t.prototype.transformTo = function(r, n, i, a) {
      var o = this.getBoundingRect(), s = o.width / o.height;
      i ? a || (a = i / s) : i = s * a;
      for (var u = new j(r, n, i, a), l = o.calculateTransform(u), f = this.geometries, h = 0; h < f.length; h++) {
        var v = f[h];
        v.type === "polygon" ? (Su(v.exterior, l), x(v.interiors, function(c) {
          Su(c, l);
        })) : x(v.points, function(c) {
          Su(c, l);
        });
      }
      o = this._rect, o.copy(u), this._center = [o.x + o.width / 2, o.y + o.height / 2];
    }, t.prototype.cloneShallow = function(r) {
      r == null && (r = this.name);
      var n = new t(r, this.geometries, this._center);
      return n._rect = this._rect, n.transformTo = null, n;
    }, t;
  })(__)
);
(function(e) {
  lt(t, e);
  function t(r, n) {
    var i = e.call(this, r) || this;
    return i.type = "geoSVG", i._elOnlyForCalculate = n, i;
  }
  return t.prototype.calcCenter = function() {
    for (var r = this._elOnlyForCalculate, n = r.getBoundingRect(), i = [n.x + n.width / 2, n.y + n.height / 2], a = $i(uM), o = r; o && !o.isGeoSVGGraphicRoot; )
      ur(a, o.getLocalTransform(), a), o = o.parent;
    return ko(a, a), Yt(i, i, a), i;
  }, t;
})(__);
function hM(e) {
  if (!e.UTF8Encoding)
    return e;
  var t = e, r = t.UTF8Scale;
  r == null && (r = 1024);
  var n = t.features;
  return x(n, function(i) {
    var a = i.geometry, o = a.encodeOffsets, s = a.coordinates;
    if (o)
      switch (a.type) {
        case "LineString":
          a.coordinates = m_(s, o, r);
          break;
        case "Polygon":
          wu(s, o, r);
          break;
        case "MultiLineString":
          wu(s, o, r);
          break;
        case "MultiPolygon":
          x(s, function(u, l) {
            return wu(u, o[l], r);
          });
      }
  }), t.UTF8Encoding = !1, t;
}
function wu(e, t, r) {
  for (var n = 0; n < e.length; n++)
    e[n] = m_(e[n], t[n], r);
}
function m_(e, t, r) {
  for (var n = [], i = t[0], a = t[1], o = 0; o < e.length; o += 2) {
    var s = e.charCodeAt(o) - 64, u = e.charCodeAt(o + 1) - 64;
    s = s >> 1 ^ -(s & 1), u = u >> 1 ^ -(u & 1), s += i, u += a, i = s, a = u, n.push([s / r, u / r]);
  }
  return n;
}
function Wv(e, t) {
  return e = hM(e), F(dt(e.features, function(r) {
    return r.geometry && r.properties && r.geometry.coordinates.length > 0;
  }), function(r) {
    var n = r.properties, i = r.geometry, a = [];
    switch (i.type) {
      case "Polygon":
        var o = i.coordinates;
        a.push(new Gv(o[0], o.slice(1)));
        break;
      case "MultiPolygon":
        x(i.coordinates, function(u) {
          u[0] && a.push(new Gv(u[0], u.slice(1)));
        });
        break;
      case "LineString":
        a.push(new Vv([i.coordinates]));
        break;
      case "MultiLineString":
        a.push(new Vv(i.coordinates));
    }
    var s = new fM(n[t || "name"], a, n.cp);
    return s.properties = n, s;
  });
}
const cM = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  MAX_SAFE_INTEGER: f1,
  asc: o1,
  getPercentWithPrecision: s1,
  getPixelPrecision: Op,
  getPrecision: Te,
  getPrecisionSafe: Ep,
  isNumeric: Bp,
  isRadianAroundZero: Oi,
  linearMap: Ku,
  nice: Zl,
  numericToNumber: Ue,
  parseDate: se,
  quantile: c1,
  quantity: kp,
  quantityExponent: Fo,
  reformIntervals: v1,
  remRadian: $l,
  round: Ht
}, Symbol.toStringTag, { value: "Module" })), vM = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  format: ra,
  parse: se
}, Symbol.toStringTag, { value: "Module" })), dM = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: ea,
  BezierCurve: Xo,
  BoundingRect: j,
  Circle: ta,
  CompoundPath: pg,
  Ellipse: Vo,
  Group: oe,
  Image: vr,
  IncrementalDisplayable: mg,
  Line: tn,
  LinearGradient: yg,
  Polygon: Yo,
  Polyline: qo,
  RadialGradient: _g,
  Rect: Et,
  Ring: Uo,
  Sector: Wo,
  Text: Bt,
  clipPointsByRect: Dg,
  clipRectByRect: Pg,
  createIcon: Zo,
  extendPath: bg,
  extendShape: wg,
  getShapeClass: Tg,
  getTransform: Mg,
  initProps: of,
  makeImage: sf,
  makePath: $o,
  mergePath: Cg,
  registerShape: ue,
  resizePath: uf,
  updateProps: On
}, Symbol.toStringTag, { value: "Module" })), pM = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  addCommas: _f,
  capitalFirst: rb,
  encodeHTML: Pt,
  formatTime: eb,
  formatTpl: Sf,
  getTextRect: tb,
  getTooltipMarker: Wg,
  normalizeCssArray: na,
  toCamelCase: mf,
  truncateText: G1
}, Symbol.toStringTag, { value: "Module" })), gM = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  bind: ot,
  clone: $,
  curry: pt,
  defaults: ft,
  each: x,
  extend: E,
  filter: dt,
  indexOf: rt,
  inherits: Fl,
  isArray: k,
  isFunction: U,
  isObject: N,
  isString: B,
  map: F,
  merge: it,
  reduce: Pe
}, Symbol.toStringTag, { value: "Module" }));
var Ui = gt();
function S_(e, t) {
  var r = F(t, function(n) {
    return e.scale.parse(n);
  });
  return e.type === "time" && r.length > 0 && (r.sort(), r.unshift(r[0]), r.push(r[r.length - 1])), r;
}
function yM(e) {
  var t = e.getLabelModel().get("customValues");
  if (t) {
    var r = Bn(e), n = e.scale.getExtent(), i = S_(e, t), a = dt(i, function(o) {
      return o >= n[0] && o <= n[1];
    });
    return {
      labels: F(a, function(o) {
        var s = {
          value: o
        };
        return {
          formattedLabel: r(s),
          rawLabel: e.scale.getLabel(s),
          tickValue: o
        };
      })
    };
  }
  return e.type === "category" ? mM(e) : wM(e);
}
function _M(e, t) {
  var r = e.getTickModel().get("customValues");
  if (r) {
    var n = e.scale.getExtent(), i = S_(e, r);
    return {
      ticks: dt(i, function(a) {
        return a >= n[0] && a <= n[1];
      })
    };
  }
  return e.type === "category" ? SM(e, t) : {
    ticks: F(e.scale.getTicks(), function(a) {
      return a.value;
    })
  };
}
function mM(e) {
  var t = e.getLabelModel(), r = w_(e, t);
  return !t.get("show") || e.scale.isBlank() ? {
    labels: [],
    labelCategoryInterval: r.labelCategoryInterval
  } : r;
}
function w_(e, t) {
  var r = b_(e, "labels"), n = Uf(t), i = T_(r, n);
  if (i)
    return i;
  var a, o;
  return U(n) ? a = M_(e, n) : (o = n === "auto" ? bM(e) : n, a = C_(e, o)), x_(r, n, {
    labels: a,
    labelCategoryInterval: o
  });
}
function SM(e, t) {
  var r = b_(e, "ticks"), n = Uf(t), i = T_(r, n);
  if (i)
    return i;
  var a, o;
  if ((!t.get("show") || e.scale.isBlank()) && (a = []), U(n))
    a = M_(e, n, !0);
  else if (n === "auto") {
    var s = w_(e, e.getLabelModel());
    o = s.labelCategoryInterval, a = F(s.labels, function(u) {
      return u.tickValue;
    });
  } else
    o = n, a = C_(e, o, !0);
  return x_(r, n, {
    ticks: a,
    tickCategoryInterval: o
  });
}
function wM(e) {
  var t = e.scale.getTicks(), r = Bn(e);
  return {
    labels: F(t, function(n, i) {
      return {
        level: n.level,
        formattedLabel: r(n, i),
        rawLabel: e.scale.getLabel(n),
        tickValue: n.value
      };
    })
  };
}
function b_(e, t) {
  return Ui(e)[t] || (Ui(e)[t] = []);
}
function T_(e, t) {
  for (var r = 0; r < e.length; r++)
    if (e[r].key === t)
      return e[r].value;
}
function x_(e, t, r) {
  return e.push({
    key: t,
    value: r
  }), r;
}
function bM(e) {
  var t = Ui(e).autoInterval;
  return t ?? (Ui(e).autoInterval = e.calculateCategoryInterval());
}
function TM(e) {
  var t = xM(e), r = Bn(e), n = (t.axisRotate - t.labelRotate) / 180 * Math.PI, i = e.scale, a = i.getExtent(), o = i.count();
  if (a[1] - a[0] < 1)
    return 0;
  var s = 1;
  o > 40 && (s = Math.max(1, Math.floor(o / 40)));
  for (var u = a[0], l = e.dataToCoord(u + 1) - e.dataToCoord(u), f = Math.abs(l * Math.cos(n)), h = Math.abs(l * Math.sin(n)), v = 0, c = 0; u <= a[1]; u += s) {
    var d = 0, y = 0, p = ql(r({
      value: u
    }), t.font, "center", "top");
    d = p.width * 1.3, y = p.height * 1.3, v = Math.max(v, d, 7), c = Math.max(c, y, 7);
  }
  var g = v / f, _ = c / h;
  isNaN(g) && (g = 1 / 0), isNaN(_) && (_ = 1 / 0);
  var m = Math.max(0, Math.floor(Math.min(g, _))), S = Ui(e.model), b = e.getExtent(), w = S.lastAutoInterval, T = S.lastTickCount;
  return w != null && T != null && Math.abs(w - m) <= 1 && Math.abs(T - o) <= 1 && w > m && S.axisExtent0 === b[0] && S.axisExtent1 === b[1] ? m = w : (S.lastTickCount = o, S.lastAutoInterval = m, S.axisExtent0 = b[0], S.axisExtent1 = b[1]), m;
}
function xM(e) {
  var t = e.getLabelModel();
  return {
    axisRotate: e.getRotate ? e.getRotate() : e.isHorizontal && !e.isHorizontal() ? 90 : 0,
    labelRotate: t.get("rotate") || 0,
    font: t.getFont()
  };
}
function C_(e, t, r) {
  var n = Bn(e), i = e.scale, a = i.getExtent(), o = e.getLabelModel(), s = [], u = Math.max((t || 0) + 1, 1), l = a[0], f = i.count();
  l !== 0 && u > 1 && f / u > 2 && (l = Math.round(Math.ceil(l / u) * u));
  var h = y_(e), v = o.get("showMinLabel") || h, c = o.get("showMaxLabel") || h;
  v && l !== a[0] && y(a[0]);
  for (var d = l; d <= a[1]; d += u)
    y(d);
  c && d - u !== a[1] && y(a[1]);
  function y(p) {
    var g = {
      value: p
    };
    s.push(r ? p : {
      formattedLabel: n(g),
      rawLabel: i.getLabel(g),
      tickValue: p
    });
  }
  return s;
}
function M_(e, t, r) {
  var n = e.scale, i = Bn(e), a = [];
  return x(n.getTicks(), function(o) {
    var s = n.getLabel(o), u = o.value;
    t(o.value, s) && a.push(r ? u : {
      formattedLabel: i(o),
      rawLabel: s,
      tickValue: u
    });
  }), a;
}
var Uv = [0, 1], CM = (
  /** @class */
  (function() {
    function e(t, r, n) {
      this.onBand = !1, this.inverse = !1, this.dim = t, this.scale = r, this._extent = n || [0, 0];
    }
    return e.prototype.contain = function(t) {
      var r = this._extent, n = Math.min(r[0], r[1]), i = Math.max(r[0], r[1]);
      return t >= n && t <= i;
    }, e.prototype.containData = function(t) {
      return this.scale.contain(t);
    }, e.prototype.getExtent = function() {
      return this._extent.slice();
    }, e.prototype.getPixelPrecision = function(t) {
      return Op(t || this.scale.getExtent(), this._extent);
    }, e.prototype.setExtent = function(t, r) {
      var n = this._extent;
      n[0] = t, n[1] = r;
    }, e.prototype.dataToCoord = function(t, r) {
      var n = this._extent, i = this.scale;
      return t = i.normalize(t), this.onBand && i.type === "ordinal" && (n = n.slice(), Yv(n, i.count())), Ku(t, Uv, n, r);
    }, e.prototype.coordToData = function(t, r) {
      var n = this._extent, i = this.scale;
      this.onBand && i.type === "ordinal" && (n = n.slice(), Yv(n, i.count()));
      var a = Ku(t, n, Uv, r);
      return this.scale.scale(a);
    }, e.prototype.pointToData = function(t, r) {
    }, e.prototype.getTicksCoords = function(t) {
      t = t || {};
      var r = t.tickModel || this.getTickModel(), n = _M(this, r), i = n.ticks, a = F(i, function(s) {
        return {
          coord: this.dataToCoord(this.scale.type === "ordinal" ? this.scale.getRawOrdinalNumber(s) : s),
          tickValue: s
        };
      }, this), o = r.get("alignWithLabel");
      return MM(this, a, o, t.clamp), a;
    }, e.prototype.getMinorTicksCoords = function() {
      if (this.scale.type === "ordinal")
        return [];
      var t = this.model.getModel("minorTick"), r = t.get("splitNumber");
      r > 0 && r < 100 || (r = 5);
      var n = this.scale.getMinorTicks(r), i = F(n, function(a) {
        return F(a, function(o) {
          return {
            coord: this.dataToCoord(o),
            tickValue: o
          };
        }, this);
      }, this);
      return i;
    }, e.prototype.getViewLabels = function() {
      return yM(this).labels;
    }, e.prototype.getLabelModel = function() {
      return this.model.getModel("axisLabel");
    }, e.prototype.getTickModel = function() {
      return this.model.getModel("axisTick");
    }, e.prototype.getBandWidth = function() {
      var t = this._extent, r = this.scale.getExtent(), n = r[1] - r[0] + (this.onBand ? 1 : 0);
      n === 0 && (n = 1);
      var i = Math.abs(t[1] - t[0]);
      return Math.abs(i) / n;
    }, e.prototype.calculateCategoryInterval = function() {
      return TM(this);
    }, e;
  })()
);
function Yv(e, t) {
  var r = e[1] - e[0], n = t, i = r / n / 2;
  e[0] += i, e[1] -= i;
}
function MM(e, t, r, n) {
  var i = t.length;
  if (!e.onBand || r || !i)
    return;
  var a = e.getExtent(), o, s;
  if (i === 1)
    t[0].coord = a[0], o = t[1] = {
      coord: a[1],
      tickValue: t[0].tickValue
    };
  else {
    var u = t[i - 1].tickValue - t[0].tickValue, l = (t[i - 1].coord - t[0].coord) / u;
    x(t, function(c) {
      c.coord -= l / 2;
    });
    var f = e.scale.getExtent();
    s = 1 + f[1] - t[i - 1].tickValue, o = {
      coord: t[i - 1].coord + l * s,
      tickValue: f[1] + 1
    }, t.push(o);
  }
  var h = a[0] > a[1];
  v(t[0].coord, a[0]) && (n ? t[0].coord = a[0] : t.shift()), n && v(a[0], t[0].coord) && t.unshift({
    coord: a[0]
  }), v(a[1], o.coord) && (n ? o.coord = a[1] : t.pop()), n && v(o.coord, a[1]) && t.push({
    coord: a[1]
  });
  function v(c, d) {
    return c = Ht(c), d = Ht(d), h ? c > d : c < d;
  }
}
function DM(e) {
  var t = Q.extend(e);
  return Q.registerClass(t), t;
}
function PM(e) {
  var t = $t.extend(e);
  return $t.registerClass(t), t;
}
function IM(e) {
  var t = Ae.extend(e);
  return Ae.registerClass(t), t;
}
function AM(e) {
  var t = De.extend(e);
  return De.registerClass(t), t;
}
function LM(e) {
  for (var t = [], r = 0; r < e.length; r++) {
    var n = e[r];
    if (!n.defaultAttr.ignore) {
      var i = n.label, a = i.getComputedTransform(), o = i.getBoundingRect(), s = !a || a[1] < 1e-5 && a[2] < 1e-5, u = i.style.margin || 0, l = o.clone();
      l.applyTransform(a), l.x -= u / 2, l.y -= u / 2, l.width += u, l.height += u;
      var f = s ? new po(o, a) : null;
      t.push({
        label: i,
        labelLine: n.labelLine,
        rect: l,
        localRect: o,
        obb: f,
        priority: n.priority,
        defaultAttr: n.defaultAttr,
        layoutOption: n.computedLayoutOption,
        axisAligned: s,
        transform: a
      });
    }
  }
  return t;
}
function D_(e, t, r, n, i, a) {
  var o = e.length;
  if (o < 2)
    return;
  e.sort(function(w, T) {
    return w.rect[t] - T.rect[t];
  });
  for (var s = 0, u, l = !1, f = 0; f < o; f++) {
    var h = e[f], v = h.rect;
    u = v[t] - s, u < 0 && (v[t] -= u, h.label[t] -= u, l = !0), s = v[t] + v[r];
  }
  var c = e[0], d = e[o - 1], y, p;
  g(), y < 0 && S(-y, 0.8), p < 0 && S(p, 0.8), g(), _(y, p, 1), _(p, y, -1), g(), y < 0 && b(-y), p < 0 && b(p);
  function g() {
    y = c.rect[t] - n, p = i - d.rect[t] - d.rect[r];
  }
  function _(w, T, C) {
    if (w < 0) {
      var M = Math.min(T, -w);
      if (M > 0) {
        m(M * C, 0, o);
        var D = M + w;
        D < 0 && S(-D * C, 1);
      } else
        S(-w * C, 1);
    }
  }
  function m(w, T, C) {
    w !== 0 && (l = !0);
    for (var M = T; M < C; M++) {
      var D = e[M], P = D.rect;
      P[t] += w, D.label[t] += w;
    }
  }
  function S(w, T) {
    for (var C = [], M = 0, D = 1; D < o; D++) {
      var P = e[D - 1].rect, A = Math.max(e[D].rect[t] - P[t] - P[r], 0);
      C.push(A), M += A;
    }
    if (M) {
      var L = Math.min(Math.abs(w) / M, T);
      if (w > 0)
        for (var D = 0; D < o - 1; D++) {
          var I = C[D] * L;
          m(I, 0, D + 1);
        }
      else
        for (var D = o - 1; D > 0; D--) {
          var I = C[D - 1] * L;
          m(-I, D, o);
        }
    }
  }
  function b(w) {
    var T = w < 0 ? -1 : 1;
    w = Math.abs(w);
    for (var C = Math.ceil(w / (o - 1)), M = 0; M < o - 1; M++)
      if (T > 0 ? m(C, 0, M + 1) : m(-C, o - M - 1, o), w -= C, w <= 0)
        return;
  }
  return l;
}
function dP(e, t, r, n) {
  return D_(e, "x", "width", t, r);
}
function pP(e, t, r, n) {
  return D_(e, "y", "height", t, r);
}
function RM(e) {
  var t = [];
  e.sort(function(y, p) {
    return p.priority - y.priority;
  });
  var r = new j(0, 0, 0, 0);
  function n(y) {
    if (!y.ignore) {
      var p = y.ensureState("emphasis");
      p.ignore == null && (p.ignore = !1);
    }
    y.ignore = !0;
  }
  for (var i = 0; i < e.length; i++) {
    var a = e[i], o = a.axisAligned, s = a.localRect, u = a.transform, l = a.label, f = a.labelLine;
    r.copy(a.rect), r.width -= 0.1, r.height -= 0.1, r.x += 0.05, r.y += 0.05;
    for (var h = a.obb, v = !1, c = 0; c < t.length; c++) {
      var d = t[c];
      if (r.intersect(d.rect)) {
        if (o && d.axisAligned) {
          v = !0;
          break;
        }
        if (d.obb || (d.obb = new po(d.localRect, d.transform)), h || (h = new po(s, u)), h.intersect(d.obb)) {
          v = !0;
          break;
        }
      }
    }
    v ? (n(l), f && n(f)) : (l.attr("ignore", a.defaultAttr.ignore), f && f.attr("ignore", a.defaultAttr.labelGuideIgnore), t.push(a));
  }
}
const gP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Axis: CM,
  ChartView: De,
  ComponentModel: Q,
  ComponentView: $t,
  List: i_,
  Model: st,
  PRIORITY: Ny,
  SeriesModel: Ae,
  color: Mm,
  connect: Yx,
  dataTool: Qx,
  dependencies: Dx,
  disConnect: qx,
  disconnect: qy,
  dispose: Xx,
  env: z,
  extendChartView: AM,
  extendComponentModel: DM,
  extendComponentView: PM,
  extendSeriesModel: IM,
  format: pM,
  getCoordinateSystemDimensions: Zx,
  getInstanceByDom: Ef,
  getInstanceById: $x,
  getMap: Kx,
  graphic: dM,
  helper: aM,
  init: Ux,
  innerDrawElementOnCanvas: If,
  matrix: tm,
  number: cM,
  parseGeoJSON: Wv,
  parseGeoJson: Wv,
  registerAction: en,
  registerCoordinateSystem: Zy,
  registerLayout: jy,
  registerLoading: Nf,
  registerLocale: df,
  registerMap: Ky,
  registerPostInit: Xy,
  registerPostUpdate: $y,
  registerPreprocessor: kf,
  registerProcessor: Bf,
  registerTheme: Of,
  registerTransform: Qy,
  registerUpdateLifecycle: us,
  registerVisual: dr,
  setCanvasCreator: jx,
  setPlatformAPI: $d,
  throttle: Pf,
  time: vM,
  use: Rn,
  util: gM,
  vector: N0,
  version: Mx,
  zrUtil: D0,
  zrender: i1
}, Symbol.toStringTag, { value: "Module" }));
var yP = (
  /** @class */
  (function() {
    function e(t) {
      this.type = "cartesian", this._dimList = [], this._axes = {}, this.name = t || "";
    }
    return e.prototype.getAxis = function(t) {
      return this._axes[t];
    }, e.prototype.getAxes = function() {
      return F(this._dimList, function(t) {
        return this._axes[t];
      }, this);
    }, e.prototype.getAxesByScale = function(t) {
      return t = t.toLowerCase(), dt(this.getAxes(), function(r) {
        return r.scale.type === t;
      });
    }, e.prototype.addAxis = function(t) {
      var r = t.dim;
      this._axes[r] = t, this._dimList.push(r);
    }, e;
  })()
);
function qv(e, t, r) {
  r = r || {};
  var n = e.coordinateSystem, i = t.axis, a = {}, o = i.getAxesOnZeroOf()[0], s = i.position, u = o ? "onZero" : s, l = i.dim, f = n.getRect(), h = [f.x, f.x + f.width, f.y, f.y + f.height], v = {
    left: 0,
    right: 1,
    top: 0,
    bottom: 1,
    onZero: 2
  }, c = t.get("offset") || 0, d = l === "x" ? [h[2] - c, h[3] + c] : [h[0] - c, h[1] + c];
  if (o) {
    var y = o.toGlobalCoord(o.dataToCoord(0));
    d[v.onZero] = Math.max(Math.min(y, d[1]), d[0]);
  }
  a.position = [l === "y" ? d[v[u]] : h[0], l === "x" ? d[v[u]] : h[3]], a.rotation = Math.PI / 2 * (l === "x" ? 0 : 1);
  var p = {
    top: -1,
    bottom: 1,
    left: -1,
    right: 1
  };
  a.labelDirection = a.tickDirection = a.nameDirection = p[s], a.labelOffset = o ? d[v[s]] - d[v.onZero] : 0, t.get(["axisTick", "inside"]) && (a.tickDirection = -a.tickDirection), An(r.labelInside, t.get(["axisLabel", "inside"])) && (a.labelDirection = -a.labelDirection);
  var g = t.get(["axisLabel", "rotate"]);
  return a.labelRotate = u === "top" ? -g : g, a.z2 = 1, a;
}
function _P(e) {
  return e.get("coordinateSystem") === "cartesian2d";
}
function mP(e) {
  var t = {
    xAxisModel: null,
    yAxisModel: null
  };
  return x(t, function(r, n) {
    var i = n.replace(/Model$/, ""), a = e.getReferringComponents(i, ar).models[0];
    t[n] = a;
  }), t;
}
var or = Math.PI, Zr = (
  /** @class */
  (function() {
    function e(t, r) {
      this.group = new oe(), this.opt = r, this.axisModel = t, ft(r, {
        labelOffset: 0,
        nameDirection: 1,
        tickDirection: 1,
        labelDirection: 1,
        silent: !0,
        handleAutoShown: function() {
          return !0;
        }
      });
      var n = new oe({
        x: r.position[0],
        y: r.position[1],
        rotation: r.rotation
      });
      n.updateTransform(), this._transformGroup = n;
    }
    return e.prototype.hasBuilder = function(t) {
      return !!Xv[t];
    }, e.prototype.add = function(t) {
      Xv[t](this.opt, this.axisModel, this.group, this._transformGroup);
    }, e.prototype.getGroup = function() {
      return this.group;
    }, e.innerTextLayout = function(t, r, n) {
      var i = $l(r - t), a, o;
      return Oi(i) ? (o = n > 0 ? "top" : "bottom", a = "center") : Oi(i - or) ? (o = n > 0 ? "bottom" : "top", a = "center") : (o = "middle", i > 0 && i < or ? a = n > 0 ? "right" : "left" : a = n > 0 ? "left" : "right"), {
        rotation: i,
        textAlign: a,
        textVerticalAlign: o
      };
    }, e.makeAxisEventDataBase = function(t) {
      var r = {
        componentType: t.mainType,
        componentIndex: t.componentIndex
      };
      return r[t.mainType + "Index"] = t.componentIndex, r;
    }, e.isLabelSilent = function(t) {
      var r = t.get("tooltip");
      return t.get("silent") || !(t.get("triggerEvent") || r && r.show);
    }, e;
  })()
), Xv = {
  axisLine: function(e, t, r, n) {
    var i = t.get(["axisLine", "show"]);
    if (i === "auto" && e.handleAutoShown && (i = e.handleAutoShown("axisLine")), !!i) {
      var a = t.axis.getExtent(), o = n.transform, s = [a[0], 0], u = [a[1], 0], l = s[0] > u[0];
      o && (Yt(s, s, o), Yt(u, u, o));
      var f = E({
        lineCap: "round"
      }, t.getModel(["axisLine", "lineStyle"]).getLineStyle()), h = new tn({
        shape: {
          x1: s[0],
          y1: s[1],
          x2: u[0],
          y2: u[1]
        },
        style: f,
        strokeContainThreshold: e.strokeContainThreshold || 5,
        silent: !0,
        z2: 1
      });
      lf(h.shape, h.style.lineWidth), h.anid = "line", r.add(h);
      var v = t.get(["axisLine", "symbol"]);
      if (v != null) {
        var c = t.get(["axisLine", "symbolSize"]);
        B(v) && (v = [v, v]), (B(c) || ut(c)) && (c = [c, c]);
        var d = sx(t.get(["axisLine", "symbolOffset"]) || 0, c), y = c[0], p = c[1];
        x([{
          rotate: e.rotation + Math.PI / 2,
          offset: d[0],
          r: 0
        }, {
          rotate: e.rotation - Math.PI / 2,
          offset: d[1],
          r: Math.sqrt((s[0] - u[0]) * (s[0] - u[0]) + (s[1] - u[1]) * (s[1] - u[1]))
        }], function(g, _) {
          if (v[_] !== "none" && v[_] != null) {
            var m = os(v[_], -y / 2, -p / 2, y, p, f.stroke, !0), S = g.r + g.offset, b = l ? u : s;
            m.attr({
              rotation: g.rotate,
              x: b[0] + S * Math.cos(e.rotation),
              y: b[1] - S * Math.sin(e.rotation),
              silent: !0,
              z2: 11
            }), r.add(m);
          }
        });
      }
    }
  },
  axisTickLabel: function(e, t, r, n) {
    var i = kM(r, n, t, e), a = FM(r, n, t, e);
    if (OM(t, a, i), BM(r, n, t, e.tickDirection), t.get(["axisLabel", "hideOverlap"])) {
      var o = LM(F(a, function(s) {
        return {
          label: s,
          priority: s.z2,
          defaultAttr: {
            ignore: s.ignore
          }
        };
      }));
      RM(o);
    }
  },
  axisName: function(e, t, r, n) {
    var i = An(e.axisName, t.get("name"));
    if (i) {
      var a = t.get("nameLocation"), o = e.nameDirection, s = t.getModel("nameTextStyle"), u = t.get("nameGap") || 0, l = t.axis.getExtent(), f = l[0] > l[1] ? -1 : 1, h = [
        a === "start" ? l[0] - f * u : a === "end" ? l[1] + f * u : (l[0] + l[1]) / 2,
        // Reuse labelOffset.
        Zv(a) ? e.labelOffset + o * u : 0
      ], v, c = t.get("nameRotate");
      c != null && (c = c * or / 180);
      var d;
      Zv(a) ? v = Zr.innerTextLayout(
        e.rotation,
        c ?? e.rotation,
        // Adapt to axis.
        o
      ) : (v = EM(e.rotation, a, c || 0, l), d = e.axisNameAvailableWidth, d != null && (d = Math.abs(d / Math.sin(v.rotation)), !isFinite(d) && (d = null)));
      var y = s.getFont(), p = t.get("nameTruncate", !0) || {}, g = p.ellipsis, _ = An(e.nameTruncateMaxWidth, p.maxWidth, d), m = new Bt({
        x: h[0],
        y: h[1],
        rotation: v.rotation,
        silent: Zr.isLabelSilent(t),
        style: Qr(s, {
          text: i,
          font: y,
          overflow: "truncate",
          width: _,
          ellipsis: g,
          fill: s.getTextColor() || t.get(["axisLine", "lineStyle", "color"]),
          align: s.get("align") || v.textAlign,
          verticalAlign: s.get("verticalAlign") || v.textVerticalAlign
        }),
        z2: 1
      });
      if (jo({
        el: m,
        componentModel: t,
        itemName: i
      }), m.__fullText = i, m.anid = "name", t.get("triggerEvent")) {
        var S = Zr.makeAxisEventDataBase(t);
        S.targetType = "axisName", S.name = i, at(m).eventData = S;
      }
      n.add(m), m.updateTransform(), r.add(m), m.decomposeTransform();
    }
  }
};
function EM(e, t, r, n) {
  var i = $l(r - e), a, o, s = n[0] > n[1], u = t === "start" && !s || t !== "start" && s;
  return Oi(i - or / 2) ? (o = u ? "bottom" : "top", a = "center") : Oi(i - or * 1.5) ? (o = u ? "top" : "bottom", a = "center") : (o = "middle", i < or * 1.5 && i > or / 2 ? a = u ? "left" : "right" : a = u ? "right" : "left"), {
    rotation: i,
    textAlign: a,
    textVerticalAlign: o
  };
}
function OM(e, t, r) {
  if (!y_(e.axis)) {
    var n = e.get(["axisLabel", "showMinLabel"]), i = e.get(["axisLabel", "showMaxLabel"]);
    t = t || [], r = r || [];
    var a = t[0], o = t[1], s = t[t.length - 1], u = t[t.length - 2], l = r[0], f = r[1], h = r[r.length - 1], v = r[r.length - 2];
    n === !1 ? (Kt(a), Kt(l)) : $v(a, o) && (n ? (Kt(o), Kt(f)) : (Kt(a), Kt(l))), i === !1 ? (Kt(s), Kt(h)) : $v(u, s) && (i ? (Kt(u), Kt(v)) : (Kt(s), Kt(h)));
  }
}
function Kt(e) {
  e && (e.ignore = !0);
}
function $v(e, t) {
  var r = e && e.getBoundingRect().clone(), n = t && t.getBoundingRect().clone();
  if (!(!r || !n)) {
    var i = $i([]);
    return Oo(i, i, -e.rotation), r.applyTransform(ur([], i, e.getLocalTransform())), n.applyTransform(ur([], i, t.getLocalTransform())), r.intersect(n);
  }
}
function Zv(e) {
  return e === "middle" || e === "center";
}
function P_(e, t, r, n, i) {
  for (var a = [], o = [], s = [], u = 0; u < e.length; u++) {
    var l = e[u].coord;
    o[0] = l, o[1] = 0, s[0] = l, s[1] = r, t && (Yt(o, o, t), Yt(s, s, t));
    var f = new tn({
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
    lf(f.shape, f.style.lineWidth), f.anid = i + "_" + e[u].tickValue, a.push(f);
  }
  return a;
}
function kM(e, t, r, n) {
  var i = r.axis, a = r.getModel("axisTick"), o = a.get("show");
  if (o === "auto" && n.handleAutoShown && (o = n.handleAutoShown("axisTick")), !(!o || i.scale.isBlank())) {
    for (var s = a.getModel("lineStyle"), u = n.tickDirection * a.get("length"), l = i.getTicksCoords(), f = P_(l, t.transform, u, ft(s.getLineStyle(), {
      stroke: r.get(["axisLine", "lineStyle", "color"])
    }), "ticks"), h = 0; h < f.length; h++)
      e.add(f[h]);
    return f;
  }
}
function BM(e, t, r, n) {
  var i = r.axis, a = r.getModel("minorTick");
  if (!(!a.get("show") || i.scale.isBlank())) {
    var o = i.getMinorTicksCoords();
    if (o.length)
      for (var s = a.getModel("lineStyle"), u = n * a.get("length"), l = ft(s.getLineStyle(), ft(r.getModel("axisTick").getLineStyle(), {
        stroke: r.get(["axisLine", "lineStyle", "color"])
      })), f = 0; f < o.length; f++)
        for (var h = P_(o[f], t.transform, u, l, "minorticks_" + f), v = 0; v < h.length; v++)
          e.add(h[v]);
  }
}
function FM(e, t, r, n) {
  var i = r.axis, a = An(n.axisLabelShow, r.get(["axisLabel", "show"]));
  if (!(!a || i.scale.isBlank())) {
    var o = r.getModel("axisLabel"), s = o.get("margin"), u = i.getViewLabels(), l = (An(n.labelRotate, o.get("rotate")) || 0) * or / 180, f = Zr.innerTextLayout(n.rotation, l, n.labelDirection), h = r.getCategories && r.getCategories(!0), v = [], c = Zr.isLabelSilent(r), d = r.get("triggerEvent");
    return x(u, function(y, p) {
      var g = i.scale.type === "ordinal" ? i.scale.getRawOrdinalNumber(y.tickValue) : y.tickValue, _ = y.formattedLabel, m = y.rawLabel, S = o;
      if (h && h[g]) {
        var b = h[g];
        N(b) && b.textStyle && (S = new st(b.textStyle, o, r.ecModel));
      }
      var w = S.getTextColor() || r.get(["axisLine", "lineStyle", "color"]), T = i.dataToCoord(g), C = S.getShallow("align", !0) || f.textAlign, M = Y(S.getShallow("alignMinLabel", !0), C), D = Y(S.getShallow("alignMaxLabel", !0), C), P = S.getShallow("verticalAlign", !0) || S.getShallow("baseline", !0) || f.textVerticalAlign, A = Y(S.getShallow("verticalAlignMinLabel", !0), P), L = Y(S.getShallow("verticalAlignMaxLabel", !0), P), I = new Bt({
        x: T,
        y: n.labelOffset + n.labelDirection * s,
        rotation: f.rotation,
        silent: c,
        z2: 10 + (y.level || 0),
        style: Qr(S, {
          text: _,
          align: p === 0 ? M : p === u.length - 1 ? D : C,
          verticalAlign: p === 0 ? A : p === u.length - 1 ? L : P,
          fill: U(w) ? w(
            // (1) In category axis with data zoom, tick is not the original
            // index of axis.data. So tick should not be exposed to user
            // in category axis.
            // (2) Compatible with previous version, which always use formatted label as
            // input. But in interval scale the formatted label is like '223,445', which
            // maked user replace ','. So we modify it to return original val but remain
            // it as 'string' to avoid error in replacing.
            i.type === "category" ? m : i.type === "value" ? g + "" : g,
            p
          ) : w
        })
      });
      if (I.anid = "label_" + g, jo({
        el: I,
        componentModel: r,
        itemName: _,
        formatterParamsExtra: {
          isTruncated: function() {
            return I.isTruncated;
          },
          value: m,
          tickIndex: p
        }
      }), d) {
        var R = Zr.makeAxisEventDataBase(r);
        R.targetType = "axisLabel", R.value = m, R.tickIndex = p, i.type === "category" && (R.dataIndex = g), at(I).eventData = R;
      }
      t.add(I), I.updateTransform(), v.push(I), e.add(I), I.decomposeTransform();
    }), v;
  }
}
function NM(e, t) {
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
  return zM(r, e, t), r.seriesInvolved && GM(r, e), r;
}
function zM(e, t, r) {
  var n = t.getComponent("tooltip"), i = t.getComponent("axisPointer"), a = i.get("link", !0) || [], o = [];
  x(r.getCoordinateSystems(), function(s) {
    if (!s.axisPointerEnabled)
      return;
    var u = Yi(s.model), l = e.coordSysAxesInfo[u] = {};
    e.coordSysMap[u] = s;
    var f = s.model, h = f.getModel("tooltip", n);
    if (x(s.getAxes(), pt(y, !1, null)), s.getTooltipAxes && n && h.get("show")) {
      var v = h.get("trigger") === "axis", c = h.get(["axisPointer", "type"]) === "cross", d = s.getTooltipAxes(h.get(["axisPointer", "axis"]));
      (v || c) && x(d.baseAxes, pt(y, c ? "cross" : !0, v)), c && x(d.otherAxes, pt(y, "cross", !1));
    }
    function y(p, g, _) {
      var m = _.model.getModel("axisPointer", i), S = m.get("show");
      if (!(!S || S === "auto" && !p && !Cl(m))) {
        g == null && (g = m.get("triggerTooltip")), m = p ? HM(_, h, i, t, p, g) : m;
        var b = m.get("snap"), w = m.get("triggerEmphasis"), T = Yi(_.model), C = g || b || _.type === "category", M = e.axesInfo[T] = {
          key: T,
          axis: _,
          coordSys: s,
          axisPointerModel: m,
          triggerTooltip: g,
          triggerEmphasis: w,
          involveSeries: C,
          snap: b,
          useHandle: Cl(m),
          seriesModels: [],
          linkGroup: null
        };
        l[T] = M, e.seriesInvolved = e.seriesInvolved || C;
        var D = VM(a, _);
        if (D != null) {
          var P = o[D] || (o[D] = {
            axesInfo: {}
          });
          P.axesInfo[T] = M, P.mapper = a[D].mapper, M.linkGroup = P;
        }
      }
    }
  });
}
function HM(e, t, r, n, i, a) {
  var o = t.getModel("axisPointer"), s = ["type", "snap", "lineStyle", "shadowStyle", "label", "animation", "animationDurationUpdate", "animationEasingUpdate", "z"], u = {};
  x(s, function(v) {
    u[v] = $(o.get(v));
  }), u.snap = e.type !== "category" && !!a, o.get("type") === "cross" && (u.type = "line");
  var l = u.label || (u.label = {});
  if (l.show == null && (l.show = !1), i === "cross") {
    var f = o.get(["label", "show"]);
    if (l.show = f ?? !0, !a) {
      var h = u.lineStyle = o.get("crossStyle");
      h && ft(l, h.textStyle);
    }
  }
  return e.model.getModel("axisPointer", new st(u, r, n));
}
function GM(e, t) {
  t.eachSeries(function(r) {
    var n = r.coordinateSystem, i = r.get(["tooltip", "trigger"], !0), a = r.get(["tooltip", "show"], !0);
    !n || i === "none" || i === !1 || i === "item" || a === !1 || r.get(["axisPointer", "show"], !0) === !1 || x(e.coordSysAxesInfo[Yi(n.model)], function(o) {
      var s = o.axis;
      n.getAxis(s.dim) === s && (o.seriesModels.push(r), o.seriesDataCount == null && (o.seriesDataCount = 0), o.seriesDataCount += r.getData().count());
    });
  });
}
function VM(e, t) {
  for (var r = t.model, n = t.dim, i = 0; i < e.length; i++) {
    var a = e[i] || {};
    if (bu(a[n + "AxisId"], r.id) || bu(a[n + "AxisIndex"], r.componentIndex) || bu(a[n + "AxisName"], r.name))
      return i;
  }
}
function bu(e, t) {
  return e === "all" || k(e) && rt(e, t) >= 0 || e === t;
}
function WM(e) {
  var t = Yf(e);
  if (t) {
    var r = t.axisPointerModel, n = t.axis.scale, i = r.option, a = r.get("status"), o = r.get("value");
    o != null && (o = n.parse(o));
    var s = Cl(r);
    a == null && (i.status = s ? "show" : "hide");
    var u = n.getExtent().slice();
    u[0] > u[1] && u.reverse(), // Pick a value on axis when initializing.
    (o == null || o > u[1]) && (o = u[1]), o < u[0] && (o = u[0]), i.value = o, s && (i.status = t.axis.scale.isBlank() ? "hide" : "show");
  }
}
function Yf(e) {
  var t = (e.ecModel.getComponent("axisPointer") || {}).coordSysAxesInfo;
  return t && t.axesInfo[Yi(e)];
}
function UM(e) {
  var t = Yf(e);
  return t && t.axisPointerModel;
}
function Cl(e) {
  return !!e.get(["handle", "show"]);
}
function Yi(e) {
  return e.type + "||" + e.id;
}
var jv = {}, YM = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.render = function(r, n, i, a) {
      this.axisPointerClass && WM(r), e.prototype.render.apply(this, arguments), this._doUpdateAxisPointerClass(r, i, !0);
    }, t.prototype.updateAxisPointer = function(r, n, i, a) {
      this._doUpdateAxisPointerClass(r, i, !1);
    }, t.prototype.remove = function(r, n) {
      var i = this._axisPointer;
      i && i.remove(n);
    }, t.prototype.dispose = function(r, n) {
      this._disposeAxisPointer(n), e.prototype.dispose.apply(this, arguments);
    }, t.prototype._doUpdateAxisPointerClass = function(r, n, i) {
      var a = t.getAxisPointerClass(this.axisPointerClass);
      if (a) {
        var o = UM(r);
        o ? (this._axisPointer || (this._axisPointer = new a())).render(r, o, n, i) : this._disposeAxisPointer(n);
      }
    }, t.prototype._disposeAxisPointer = function(r) {
      this._axisPointer && this._axisPointer.dispose(r), this._axisPointer = null;
    }, t.registerAxisPointerClass = function(r, n) {
      jv[r] = n;
    }, t.getAxisPointerClass = function(r) {
      return r && jv[r];
    }, t.type = "axis", t;
  })($t)
), zr = gt(), Kv = $, Tu = ot, qM = (
  /** @class */
  (function() {
    function e() {
      this._dragging = !1, this.animationThreshold = 15;
    }
    return e.prototype.render = function(t, r, n, i) {
      var a = r.get("value"), o = r.get("status");
      if (this._axisModel = t, this._axisPointerModel = r, this._api = n, !(!i && this._lastValue === a && this._lastStatus === o)) {
        this._lastValue = a, this._lastStatus = o;
        var s = this._group, u = this._handle;
        if (!o || o === "hide") {
          s && s.hide(), u && u.hide();
          return;
        }
        s && s.show(), u && u.show();
        var l = {};
        this.makeElOption(l, a, t, r, n);
        var f = l.graphicKey;
        f !== this._lastGraphicKey && this.clear(n), this._lastGraphicKey = f;
        var h = this._moveAnimation = this.determineAnimation(t, r);
        if (!s)
          s = this._group = new oe(), this.createPointerEl(s, l, t, r), this.createLabelEl(s, l, t, r), n.getZr().add(s);
        else {
          var v = pt(Qv, r, h);
          this.updatePointerEl(s, l, v), this.updateLabelEl(s, l, v, r);
        }
        td(s, r, !0), this._renderHandle(a);
      }
    }, e.prototype.remove = function(t) {
      this.clear(t);
    }, e.prototype.dispose = function(t) {
      this.clear(t);
    }, e.prototype.determineAnimation = function(t, r) {
      var n = r.get("animation"), i = t.axis, a = i.type === "category", o = r.get("snap");
      if (!o && !a)
        return !1;
      if (n === "auto" || n == null) {
        var s = this.animationThreshold;
        if (a && i.getBandWidth() > s)
          return !0;
        if (o) {
          var u = Yf(t).seriesDataCount, l = i.getExtent();
          return Math.abs(l[0] - l[1]) / u > s;
        }
        return !1;
      }
      return n === !0;
    }, e.prototype.makeElOption = function(t, r, n, i, a) {
    }, e.prototype.createPointerEl = function(t, r, n, i) {
      var a = r.pointer;
      if (a) {
        var o = zr(t).pointerEl = new Aw[a.type](Kv(r.pointer));
        t.add(o);
      }
    }, e.prototype.createLabelEl = function(t, r, n, i) {
      if (r.label) {
        var a = zr(t).labelEl = new Bt(Kv(r.label));
        t.add(a), Jv(a, i);
      }
    }, e.prototype.updatePointerEl = function(t, r, n) {
      var i = zr(t).pointerEl;
      i && r.pointer && (i.setStyle(r.pointer.style), n(i, {
        shape: r.pointer.shape
      }));
    }, e.prototype.updateLabelEl = function(t, r, n, i) {
      var a = zr(t).labelEl;
      a && (a.setStyle(r.label.style), n(a, {
        // Consider text length change in vertical axis, animation should
        // be used on shape, otherwise the effect will be weird.
        // TODOTODO
        // shape: elOption.label.shape,
        x: r.label.x,
        y: r.label.y
      }), Jv(a, i));
    }, e.prototype._renderHandle = function(t) {
      if (!(this._dragging || !this.updateHandleTransform)) {
        var r = this._axisPointerModel, n = this._api.getZr(), i = this._handle, a = r.getModel("handle"), o = r.get("status");
        if (!a.get("show") || !o || o === "hide") {
          i && n.remove(i), this._handle = null;
          return;
        }
        var s;
        this._handle || (s = !0, i = this._handle = Zo(a.get("icon"), {
          cursor: "move",
          draggable: !0,
          onmousemove: function(l) {
            lp(l.event);
          },
          onmousedown: Tu(this._onHandleDragMove, this, 0, 0),
          drift: Tu(this._onHandleDragMove, this),
          ondragend: Tu(this._onHandleDragEnd, this)
        }), n.add(i)), td(i, r, !1), i.setStyle(a.getItemStyle(null, ["color", "borderColor", "borderWidth", "opacity", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"]));
        var u = a.get("size");
        k(u) || (u = [u, u]), i.scaleX = u[0] / 2, i.scaleY = u[1] / 2, my(this, "_doDispatchAxisPointer", a.get("throttle") || 0, "fixRate"), this._moveHandleToValue(t, s);
      }
    }, e.prototype._moveHandleToValue = function(t, r) {
      Qv(this._axisPointerModel, !r && this._moveAnimation, this._handle, xu(this.getHandleTransform(t, this._axisModel, this._axisPointerModel)));
    }, e.prototype._onHandleDragMove = function(t, r) {
      var n = this._handle;
      if (n) {
        this._dragging = !0;
        var i = this.updateHandleTransform(xu(n), [t, r], this._axisModel, this._axisPointerModel);
        this._payloadInfo = i, n.stopAnimation(), n.attr(xu(i)), zr(n).lastProp = null, this._doDispatchAxisPointer();
      }
    }, e.prototype._doDispatchAxisPointer = function() {
      var t = this._handle;
      if (t) {
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
    }, e.prototype._onHandleDragEnd = function() {
      this._dragging = !1;
      var t = this._handle;
      if (t) {
        var r = this._axisPointerModel.get("value");
        this._moveHandleToValue(r), this._api.dispatchAction({
          type: "hideTip"
        });
      }
    }, e.prototype.clear = function(t) {
      this._lastValue = null, this._lastStatus = null;
      var r = t.getZr(), n = this._group, i = this._handle;
      r && n && (this._lastGraphicKey = null, n && r.remove(n), i && r.remove(i), this._group = null, this._handle = null, this._payloadInfo = null), gl(this, "_doDispatchAxisPointer");
    }, e.prototype.doClear = function() {
    }, e.prototype.buildLabel = function(t, r, n) {
      return n = n || 0, {
        x: t[n],
        y: t[1 - n],
        width: r[n],
        height: r[1 - n]
      };
    }, e;
  })()
);
function Qv(e, t, r, n) {
  I_(zr(r).lastProp, n) || (zr(r).lastProp = n, t ? On(r, n, e) : (r.stopAnimation(), r.attr(n)));
}
function I_(e, t) {
  if (N(e) && N(t)) {
    var r = !0;
    return x(t, function(n, i) {
      r = r && I_(e[i], n);
    }), !!r;
  } else
    return e === t;
}
function Jv(e, t) {
  e[t.get(["label", "show"]) ? "show" : "hide"]();
}
function xu(e) {
  return {
    x: e.x || 0,
    y: e.y || 0,
    rotation: e.rotation || 0
  };
}
function td(e, t, r) {
  var n = t.get("z"), i = t.get("zlevel");
  e && e.traverse(function(a) {
    a.type !== "group" && (n != null && (a.z = n), i != null && (a.zlevel = i), a.silent = r);
  });
}
function XM(e) {
  var t = e.get("type"), r = e.getModel(t + "Style"), n;
  return t === "line" ? (n = r.getLineStyle(), n.fill = null) : t === "shadow" && (n = r.getAreaStyle(), n.stroke = null), n;
}
function $M(e, t, r, n, i) {
  var a = r.get("value"), o = A_(a, t.axis, t.ecModel, r.get("seriesDataIndices"), {
    precision: r.get(["label", "precision"]),
    formatter: r.get(["label", "formatter"])
  }), s = r.getModel("label"), u = na(s.get("padding") || 0), l = s.getFont(), f = ql(o, l), h = i.position, v = f.width + u[1] + u[3], c = f.height + u[0] + u[2], d = i.align;
  d === "right" && (h[0] -= v), d === "center" && (h[0] -= v / 2);
  var y = i.verticalAlign;
  y === "bottom" && (h[1] -= c), y === "middle" && (h[1] -= c / 2), ZM(h, v, c, n);
  var p = s.get("backgroundColor");
  (!p || p === "auto") && (p = t.get(["axisLine", "lineStyle", "color"])), e.label = {
    // shape: {x: 0, y: 0, width: width, height: height, r: labelModel.get('borderRadius')},
    x: h[0],
    y: h[1],
    style: Qr(s, {
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
function ZM(e, t, r, n) {
  var i = n.getWidth(), a = n.getHeight();
  e[0] = Math.min(e[0] + t, i) - t, e[1] = Math.min(e[1] + r, a) - r, e[0] = Math.max(e[0], 0), e[1] = Math.max(e[1], 0);
}
function A_(e, t, r, n, i) {
  e = t.scale.parse(e);
  var a = t.scale.getLabel({
    value: e
  }, {
    // If `precision` is set, width can be fixed (like '12.00500'), which
    // helps to debounce when when moving label.
    precision: i.precision
  }), o = i.formatter;
  if (o) {
    var s = {
      value: Wf(t, {
        value: e
      }),
      axisDimension: t.dim,
      axisIndex: t.index,
      seriesData: []
    };
    x(n, function(u) {
      var l = r.getSeriesByIndex(u.seriesIndex), f = u.dataIndexInside, h = l && l.getDataParams(f);
      h && s.seriesData.push(h);
    }), B(o) ? a = o.replace("{value}", a) : U(o) && (a = o(s));
  }
  return a;
}
function L_(e, t, r) {
  var n = sr();
  return Oo(n, n, r.rotation), oo(n, n, r.position), ff([e.dataToCoord(t), (r.labelOffset || 0) + (r.labelDirection || 1) * (r.labelMargin || 0)], n);
}
function jM(e, t, r, n, i, a) {
  var o = Zr.innerTextLayout(r.rotation, 0, r.labelDirection);
  r.labelMargin = i.get(["label", "margin"]), $M(t, n, i, a, {
    position: L_(n.axis, e, r),
    align: o.textAlign,
    verticalAlign: o.textVerticalAlign
  });
}
function KM(e, t, r) {
  return r = r || 0, {
    x1: e[r],
    y1: e[1 - r],
    x2: t[r],
    y2: t[1 - r]
  };
}
function QM(e, t, r) {
  return r = r || 0, {
    x: e[r],
    y: e[1 - r],
    width: t[r],
    height: t[1 - r]
  };
}
function SP(e, t, r, n, i, a) {
  return {
    cx: e,
    cy: t,
    r0: r,
    r: n,
    startAngle: i,
    endAngle: a,
    clockwise: !0
  };
}
var JM = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      return e !== null && e.apply(this, arguments) || this;
    }
    return t.prototype.makeElOption = function(r, n, i, a, o) {
      var s = i.axis, u = s.grid, l = a.get("type"), f = ed(u, s).getOtherAxis(s).getGlobalExtent(), h = s.toGlobalCoord(s.dataToCoord(n, !0));
      if (l && l !== "none") {
        var v = XM(a), c = tD[l](s, h, f);
        c.style = v, r.graphicKey = c.type, r.pointer = c;
      }
      var d = qv(u.model, i);
      jM(
        // @ts-ignore
        n,
        r,
        d,
        i,
        a,
        o
      );
    }, t.prototype.getHandleTransform = function(r, n, i) {
      var a = qv(n.axis.grid.model, n, {
        labelInside: !1
      });
      a.labelMargin = i.get(["handle", "margin"]);
      var o = L_(n.axis, r, a);
      return {
        x: o[0],
        y: o[1],
        rotation: a.rotation + (a.labelDirection < 0 ? Math.PI : 0)
      };
    }, t.prototype.updateHandleTransform = function(r, n, i, a) {
      var o = i.axis, s = o.grid, u = o.getGlobalExtent(!0), l = ed(s, o).getOtherAxis(o).getGlobalExtent(), f = o.dim === "x" ? 0 : 1, h = [r.x, r.y];
      h[f] += n[f], h[f] = Math.min(u[1], h[f]), h[f] = Math.max(u[0], h[f]);
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
    }, t;
  })(qM)
);
function ed(e, t) {
  var r = {};
  return r[t.dim + "AxisIndex"] = t.index, e.getCartesian(r);
}
var tD = {
  line: function(e, t, r) {
    var n = KM([t, r[0]], [t, r[1]], rd(e));
    return {
      type: "Line",
      subPixelOptimize: !0,
      shape: n
    };
  },
  shadow: function(e, t, r) {
    var n = Math.max(1, e.getBandWidth()), i = r[1] - r[0];
    return {
      type: "Rect",
      shape: QM([t - n / 2, r[0]], [n, i], rd(e))
    };
  }
};
function rd(e) {
  return e.dim === "x" ? 0 : 1;
}
var eD = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.type = "axisPointer", t.defaultOption = {
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
        color: "#B9BEC9",
        width: 1,
        type: "dashed"
      },
      shadowStyle: {
        color: "rgba(210,219,238,0.2)"
      },
      label: {
        show: !0,
        formatter: null,
        precision: "auto",
        margin: 3,
        color: "#fff",
        padding: [5, 7, 5, 7],
        backgroundColor: "auto",
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
        color: "#333",
        shadowBlur: 3,
        shadowColor: "#aaa",
        shadowOffsetX: 0,
        shadowOffsetY: 2,
        // For mobile performance
        throttle: 40
      }
    }, t;
  })(Q)
), Ge = gt(), rD = x;
function R_(e, t, r) {
  if (!z.node) {
    var n = t.getZr();
    Ge(n).records || (Ge(n).records = {}), nD(n, t);
    var i = Ge(n).records[e] || (Ge(n).records[e] = {});
    i.handler = r;
  }
}
function nD(e, t) {
  if (Ge(e).initialized)
    return;
  Ge(e).initialized = !0, r("click", pt(nd, "click")), r("mousemove", pt(nd, "mousemove")), r("globalout", aD);
  function r(n, i) {
    e.on(n, function(a) {
      var o = oD(t);
      rD(Ge(e).records, function(s) {
        s && i(s, a, o.dispatchAction);
      }), iD(o.pendings, t);
    });
  }
}
function iD(e, t) {
  var r = e.showTip.length, n = e.hideTip.length, i;
  r ? i = e.showTip[r - 1] : n && (i = e.hideTip[n - 1]), i && (i.dispatchAction = null, t.dispatchAction(i));
}
function aD(e, t, r) {
  e.handler("leave", null, r);
}
function nd(e, t, r, n) {
  t.handler(e, r, n);
}
function oD(e) {
  var t = {
    showTip: [],
    hideTip: []
  }, r = function(n) {
    var i = t[n.type];
    i ? i.push(n) : (n.dispatchAction = r, e.dispatchAction(n));
  };
  return {
    dispatchAction: r,
    pendings: t
  };
}
function Ml(e, t) {
  if (!z.node) {
    var r = t.getZr(), n = (Ge(r).records || {})[e];
    n && (Ge(r).records[e] = null);
  }
}
var sD = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.render = function(r, n, i) {
      var a = n.getComponent("tooltip"), o = r.get("triggerOn") || a && a.get("triggerOn") || "mousemove|click";
      R_("axisPointer", i, function(s, u, l) {
        o !== "none" && (s === "leave" || o.indexOf(s) >= 0) && l({
          type: "updateAxisPointer",
          currTrigger: s,
          x: u && u.offsetX,
          y: u && u.offsetY
        });
      });
    }, t.prototype.remove = function(r, n) {
      Ml("axisPointer", n);
    }, t.prototype.dispose = function(r, n) {
      Ml("axisPointer", n);
    }, t.type = "axisPointer", t;
  })($t)
);
function E_(e, t) {
  var r = [], n = e.seriesIndex, i;
  if (n == null || !(i = t.getSeriesByIndex(n)))
    return {
      point: []
    };
  var a = i.getData(), o = Ki(a, e);
  if (o == null || o < 0 || k(o))
    return {
      point: []
    };
  var s = a.getItemGraphicEl(o), u = i.coordinateSystem;
  if (i.getTooltipPosition)
    r = i.getTooltipPosition(o) || [];
  else if (u && u.dataToPoint)
    if (e.isStacked) {
      var l = u.getBaseAxis(), f = u.getOtherAxis(l), h = f.dim, v = l.dim, c = h === "x" || h === "radius" ? 1 : 0, d = a.mapDimension(v), y = [];
      y[c] = a.get(d, o), y[1 - c] = a.get(a.getCalculationInfo("stackResultDimension"), o), r = u.dataToPoint(y) || [];
    } else
      r = u.dataToPoint(a.getValues(F(u.dimensions, function(g) {
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
var id = gt();
function uD(e, t, r) {
  var n = e.currTrigger, i = [e.x, e.y], a = e, o = e.dispatchAction || ot(r.dispatchAction, r), s = t.getComponent("axisPointer").coordSysAxesInfo;
  if (s) {
    eo(i) && (i = E_({
      seriesIndex: a.seriesIndex,
      // Do not use dataIndexInside from other ec instance.
      // FIXME: auto detect it?
      dataIndex: a.dataIndex
    }, t).point);
    var u = eo(i), l = a.axesInfo, f = s.axesInfo, h = n === "leave" || eo(i), v = {}, c = {}, d = {
      list: [],
      map: {}
    }, y = {
      showPointer: pt(fD, c),
      showTooltip: pt(hD, d)
    };
    x(s.coordSysMap, function(g, _) {
      var m = u || g.containPoint(i);
      x(s.coordSysAxesInfo[_], function(S, b) {
        var w = S.axis, T = pD(l, S);
        if (!h && m && (!l || T)) {
          var C = T && T.value;
          C == null && !u && (C = w.pointToData(i)), C != null && ad(S, C, y, !1, v);
        }
      });
    });
    var p = {};
    return x(f, function(g, _) {
      var m = g.linkGroup;
      m && !c[_] && x(m.axesInfo, function(S, b) {
        var w = c[b];
        if (S !== g && w) {
          var T = w.value;
          m.mapper && (T = g.axis.scale.parse(m.mapper(T, od(S), od(g)))), p[g.key] = T;
        }
      });
    }), x(p, function(g, _) {
      ad(f[_], g, y, !0, v);
    }), cD(c, f, v), vD(d, i, e, o), dD(f, o, r), v;
  }
}
function ad(e, t, r, n, i) {
  var a = e.axis;
  if (!(a.scale.isBlank() || !a.containData(t))) {
    if (!e.involveSeries) {
      r.showPointer(e, t);
      return;
    }
    var o = lD(t, e), s = o.payloadBatch, u = o.snapToValue;
    s[0] && i.seriesIndex == null && E(i, s[0]), !n && e.snap && a.containData(u) && u != null && (t = u), r.showPointer(e, t, s), r.showTooltip(e, o, u);
  }
}
function lD(e, t) {
  var r = t.axis, n = r.dim, i = e, a = [], o = Number.MAX_VALUE, s = -1;
  return x(t.seriesModels, function(u, l) {
    var f = u.getData().mapDimensionsAll(n), h, v;
    if (u.getAxisTooltipData) {
      var c = u.getAxisTooltipData(f, e, r);
      v = c.dataIndices, h = c.nestestValue;
    } else {
      if (v = u.getData().indicesOfNearest(
        f[0],
        e,
        // Add a threshold to avoid find the wrong dataIndex
        // when data length is not same.
        // false,
        r.type === "category" ? 0.5 : null
      ), !v.length)
        return;
      h = u.getData().get(f[0], v[0]);
    }
    if (!(h == null || !isFinite(h))) {
      var d = e - h, y = Math.abs(d);
      y <= o && ((y < o || d >= 0 && s < 0) && (o = y, s = d, i = h, a.length = 0), x(v, function(p) {
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
function fD(e, t, r, n) {
  e[t.key] = {
    value: r,
    payloadBatch: n
  };
}
function hD(e, t, r, n) {
  var i = r.payloadBatch, a = t.axis, o = a.model, s = t.axisPointerModel;
  if (!(!t.triggerTooltip || !i.length)) {
    var u = t.coordSys.model, l = Yi(u), f = e.map[l];
    f || (f = e.map[l] = {
      coordSysId: u.id,
      coordSysIndex: u.componentIndex,
      coordSysType: u.type,
      coordSysMainType: u.mainType,
      dataByAxis: []
    }, e.list.push(f)), f.dataByAxis.push({
      axisDim: a.dim,
      axisIndex: o.componentIndex,
      axisType: o.type,
      axisId: o.id,
      value: n,
      // Caustion: viewHelper.getValueLabel is actually on "view stage", which
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
function cD(e, t, r) {
  var n = r.axesInfo = [];
  x(t, function(i, a) {
    var o = i.axisPointerModel.option, s = e[a];
    s ? (!i.useHandle && (o.status = "show"), o.value = s.value, o.seriesDataIndices = (s.payloadBatch || []).slice()) : !i.useHandle && (o.status = "hide"), o.status === "show" && n.push({
      axisDim: i.axis.dim,
      axisIndex: i.axis.model.componentIndex,
      value: o.value
    });
  });
}
function vD(e, t, r, n) {
  if (eo(t) || !e.list.length) {
    n({
      type: "hideTip"
    });
    return;
  }
  var i = ((e.list[0].dataByAxis[0] || {}).seriesDataIndices || [])[0] || {};
  n({
    type: "showTip",
    escapeConnect: !0,
    x: t[0],
    y: t[1],
    tooltipOption: r.tooltipOption,
    position: r.position,
    dataIndexInside: i.dataIndexInside,
    dataIndex: i.dataIndex,
    seriesIndex: i.seriesIndex,
    dataByCoordSys: e.list
  });
}
function dD(e, t, r) {
  var n = r.getZr(), i = "axisPointerLastHighlights", a = id(n)[i] || {}, o = id(n)[i] = {};
  x(e, function(l, f) {
    var h = l.axisPointerModel.option;
    h.status === "show" && l.triggerEmphasis && x(h.seriesDataIndices, function(v) {
      var c = v.seriesIndex + " | " + v.dataIndex;
      o[c] = v;
    });
  });
  var s = [], u = [];
  x(a, function(l, f) {
    !o[f] && u.push(l);
  }), x(o, function(l, f) {
    !a[f] && s.push(l);
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
function pD(e, t) {
  for (var r = 0; r < (e || []).length; r++) {
    var n = e[r];
    if (t.axis.dim === n.axisDim && t.axis.model.componentIndex === n.axisIndex)
      return n;
  }
}
function od(e) {
  var t = e.axis.model, r = {}, n = r.axisDim = e.axis.dim;
  return r.axisIndex = r[n + "AxisIndex"] = t.componentIndex, r.axisName = r[n + "AxisName"] = t.name, r.axisId = r[n + "AxisId"] = t.id, r;
}
function eo(e) {
  return !e || e[0] == null || isNaN(e[0]) || e[1] == null || isNaN(e[1]);
}
function gD(e) {
  YM.registerAxisPointerClass("CartesianAxisPointer", JM), e.registerComponentModel(eD), e.registerComponentView(sD), e.registerPreprocessor(function(t) {
    if (t) {
      (!t.axisPointer || t.axisPointer.length === 0) && (t.axisPointer = {});
      var r = t.axisPointer.link;
      r && !k(r) && (t.axisPointer.link = [r]);
    }
  }), e.registerProcessor(e.PRIORITY.PROCESSOR.STATISTIC, function(t, r) {
    t.getComponent("axisPointer").coordSysAxesInfo = NM(t, r);
  }), e.registerAction({
    type: "updateAxisPointer",
    event: "updateAxisPointer",
    update: ":updateAxisPointer"
  }, uD);
}
function yD(e, t) {
  var r = na(t.get("padding")), n = t.getItemStyle(["color", "opacity"]);
  return n.fill = t.get("backgroundColor"), e = new Et({
    shape: {
      x: e.x - r[3],
      y: e.y - r[0],
      width: e.width + r[1] + r[3],
      height: e.height + r[0] + r[2],
      r: t.get("borderRadius")
    },
    style: n,
    silent: !0,
    z2: -1
  }), e;
}
var _D = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.type = "tooltip", t.dependencies = ["axisPointer"], t.defaultOption = {
      // zlevel: 0,
      z: 60,
      show: !0,
      // tooltip main content
      showContent: !0,
      // 'trigger' only works on coordinate system.
      // 'item' | 'axis' | 'none'
      trigger: "item",
      // 'click' | 'mousemove' | 'none'
      triggerOn: "mousemove|click",
      alwaysShowContent: !1,
      displayMode: "single",
      renderMode: "auto",
      // whether restraint content inside viewRect.
      // If renderMode: 'richText', default true.
      // If renderMode: 'html', defaut false (for backward compat).
      confine: null,
      showDelay: 0,
      hideDelay: 100,
      // Animation transition time, unit is second
      transitionDuration: 0.4,
      enterable: !1,
      backgroundColor: "#fff",
      // box shadow
      shadowBlur: 10,
      shadowColor: "rgba(0, 0, 0, .2)",
      shadowOffsetX: 1,
      shadowOffsetY: 2,
      // tooltip border radius, unit is px, default is 4
      borderRadius: 4,
      // tooltip border width, unit is px, default is 0 (no border)
      borderWidth: 1,
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
          color: "#999",
          width: 1,
          type: "dashed",
          // TODO formatter
          textStyle: {}
        }
        // lineStyle and shadowStyle should not be specified here,
        // otherwise it will always override those styles on option.axisPointer.
      },
      textStyle: {
        color: "#666",
        fontSize: 14
      }
    }, t;
  })(Q)
);
function O_(e) {
  var t = e.get("confine");
  return t != null ? !!t : e.get("renderMode") === "richText";
}
function k_(e) {
  if (z.domSupported) {
    for (var t = document.documentElement.style, r = 0, n = e.length; r < n; r++)
      if (e[r] in t)
        return e[r];
  }
}
var B_ = k_(["transform", "webkitTransform", "OTransform", "MozTransform", "msTransform"]), mD = k_(["webkitTransition", "transition", "OTransition", "MozTransition", "msTransition"]);
function F_(e, t) {
  if (!e)
    return t;
  t = mf(t, !0);
  var r = e.indexOf(t);
  return e = r === -1 ? t : "-" + e.slice(0, r) + "-" + t, e.toLowerCase();
}
function SD(e, t) {
  var r = e.currentStyle || document.defaultView && document.defaultView.getComputedStyle(e);
  return r ? r[t] : null;
}
var wD = F_(mD, "transition"), qf = F_(B_, "transform"), bD = "position:absolute;display:block;border-style:solid;white-space:nowrap;z-index:9999999;" + (z.transform3dSupported ? "will-change:transform;" : "");
function TD(e) {
  return e = e === "left" ? "right" : e === "right" ? "left" : e === "top" ? "bottom" : "top", e;
}
function xD(e, t, r) {
  if (!B(r) || r === "inside")
    return "";
  var n = e.get("backgroundColor"), i = e.get("borderWidth");
  t = Ni(t);
  var a = TD(r), o = Math.max(Math.round(i) * 1.5, 6), s = "", u = qf + ":", l;
  rt(["left", "right"], a) > -1 ? (s += "top:50%", u += "translateY(-50%) rotate(" + (l = a === "left" ? -225 : -45) + "deg)") : (s += "left:50%", u += "translateX(-50%) rotate(" + (l = a === "top" ? 225 : 45) + "deg)");
  var f = l * Math.PI / 180, h = o + i, v = h * Math.abs(Math.cos(f)) + h * Math.abs(Math.sin(f)), c = Math.round(((v - Math.SQRT2 * i) / 2 + Math.SQRT2 * i - (v - h) / 2) * 100) / 100;
  s += ";" + a + ":-" + c + "px";
  var d = t + " solid " + i + "px;", y = ["position:absolute;width:" + o + "px;height:" + o + "px;z-index:-1;", s + ";" + u + ";", "border-bottom:" + d, "border-right:" + d, "background-color:" + n + ";"];
  return '<div style="' + y.join("") + '"></div>';
}
function CD(e, t) {
  var r = "cubic-bezier(0.23,1,0.32,1)", n = " " + e / 2 + "s " + r, i = "opacity" + n + ",visibility" + n;
  return t || (n = " " + e + "s " + r, i += z.transformSupported ? "," + qf + n : ",left" + n + ",top" + n), wD + ":" + i;
}
function sd(e, t, r) {
  var n = e.toFixed(0) + "px", i = t.toFixed(0) + "px";
  if (!z.transformSupported)
    return r ? "top:" + i + ";left:" + n + ";" : [["top", i], ["left", n]];
  var a = z.transform3dSupported, o = "translate" + (a ? "3d" : "") + "(" + n + "," + i + (a ? ",0" : "") + ")";
  return r ? "top:0;left:0;" + qf + ":" + o + ";" : [["top", 0], ["left", 0], [B_, o]];
}
function MD(e) {
  var t = [], r = e.get("fontSize"), n = e.getTextColor();
  n && t.push("color:" + n), t.push("font:" + e.getFont());
  var i = Y(e.get("lineHeight"), Math.round(r * 3 / 2));
  r && t.push("line-height:" + i + "px");
  var a = e.get("textShadowColor"), o = e.get("textShadowBlur") || 0, s = e.get("textShadowOffsetX") || 0, u = e.get("textShadowOffsetY") || 0;
  return a && o && t.push("text-shadow:" + s + "px " + u + "px " + o + "px " + a), x(["decoration", "align"], function(l) {
    var f = e.get(l);
    f && t.push("text-" + l + ":" + f);
  }), t.join(";");
}
function DD(e, t, r) {
  var n = [], i = e.get("transitionDuration"), a = e.get("backgroundColor"), o = e.get("shadowBlur"), s = e.get("shadowColor"), u = e.get("shadowOffsetX"), l = e.get("shadowOffsetY"), f = e.getModel("textStyle"), h = gy(e, "html"), v = u + "px " + l + "px " + o + "px " + s;
  return n.push("box-shadow:" + v), t && i && n.push(CD(i, r)), a && n.push("background-color:" + a), x(["width", "color", "radius"], function(c) {
    var d = "border-" + c, y = mf(d), p = e.get(y);
    p != null && n.push(d + ":" + p + (c === "color" ? "" : "px"));
  }), n.push(MD(f)), h != null && n.push("padding:" + na(h).join("px ") + "px"), n.join(";") + ";";
}
function ud(e, t, r, n, i) {
  var a = t && t.painter;
  if (r) {
    var o = a && a.getViewportRoot();
    o && G0(e, o, r, n, i);
  } else {
    e[0] = n, e[1] = i;
    var s = a && a.getViewportRootOffset();
    s && (e[0] += s.offsetLeft, e[1] += s.offsetTop);
  }
  e[2] = e[0] / t.getWidth(), e[3] = e[1] / t.getHeight();
}
var PD = (
  /** @class */
  (function() {
    function e(t, r) {
      if (this._show = !1, this._styleCoord = [0, 0, 0, 0], this._enterable = !0, this._alwaysShowContent = !1, this._firstShow = !0, this._longHide = !0, z.wxa)
        return null;
      var n = document.createElement("div");
      n.domBelongToZr = !0, this.el = n;
      var i = this._zr = t.getZr(), a = r.appendTo, o = a && (B(a) ? document.querySelector(a) : In(a) ? a : U(a) && a(t.getDom()));
      ud(this._styleCoord, i, o, t.getWidth() / 2, t.getHeight() / 2), (o || t.getDom()).appendChild(n), this._api = t, this._container = o;
      var s = this;
      n.onmouseenter = function() {
        s._enterable && (clearTimeout(s._hideTimeout), s._show = !0), s._inContent = !0;
      }, n.onmousemove = function(u) {
        if (u = u || window.event, !s._enterable) {
          var l = i.handler, f = i.painter.getViewportRoot();
          Jt(f, u, !0), l.dispatch("mousemove", u);
        }
      }, n.onmouseleave = function() {
        s._inContent = !1, s._enterable && s._show && s.hideLater(s._hideDelay);
      };
    }
    return e.prototype.update = function(t) {
      if (!this._container) {
        var r = this._api.getDom(), n = SD(r, "position"), i = r.style;
        i.position !== "absolute" && n !== "absolute" && (i.position = "relative");
      }
      var a = t.get("alwaysShowContent");
      a && this._moveIfResized(), this._alwaysShowContent = a, this.el.className = t.get("className") || "";
    }, e.prototype.show = function(t, r) {
      clearTimeout(this._hideTimeout), clearTimeout(this._longHideTimeout);
      var n = this.el, i = n.style, a = this._styleCoord;
      n.innerHTML ? i.cssText = bD + DD(t, !this._firstShow, this._longHide) + sd(a[0], a[1], !0) + ("border-color:" + Ni(r) + ";") + (t.get("extraCssText") || "") + (";pointer-events:" + (this._enterable ? "auto" : "none")) : i.display = "none", this._show = !0, this._firstShow = !1, this._longHide = !1;
    }, e.prototype.setContent = function(t, r, n, i, a) {
      var o = this.el;
      if (t == null) {
        o.innerHTML = "";
        return;
      }
      var s = "";
      if (B(a) && n.get("trigger") === "item" && !O_(n) && (s = xD(n, i, a)), B(t))
        o.innerHTML = t + s;
      else if (t) {
        o.innerHTML = "", k(t) || (t = [t]);
        for (var u = 0; u < t.length; u++)
          In(t[u]) && t[u].parentNode !== o && o.appendChild(t[u]);
        if (s && o.childNodes.length) {
          var l = document.createElement("div");
          l.innerHTML = s, o.appendChild(l);
        }
      }
    }, e.prototype.setEnterable = function(t) {
      this._enterable = t;
    }, e.prototype.getSize = function() {
      var t = this.el;
      return t ? [t.offsetWidth, t.offsetHeight] : [0, 0];
    }, e.prototype.moveTo = function(t, r) {
      if (this.el) {
        var n = this._styleCoord;
        if (ud(n, this._zr, this._container, t, r), n[0] != null && n[1] != null) {
          var i = this.el.style, a = sd(n[0], n[1]);
          x(a, function(o) {
            i[o[0]] = o[1];
          });
        }
      }
    }, e.prototype._moveIfResized = function() {
      var t = this._styleCoord[2], r = this._styleCoord[3];
      this.moveTo(t * this._zr.getWidth(), r * this._zr.getHeight());
    }, e.prototype.hide = function() {
      var t = this, r = this.el.style;
      r.visibility = "hidden", r.opacity = "0", z.transform3dSupported && (r.willChange = ""), this._show = !1, this._longHideTimeout = setTimeout(function() {
        return t._longHide = !0;
      }, 500);
    }, e.prototype.hideLater = function(t) {
      this._show && !(this._inContent && this._enterable) && !this._alwaysShowContent && (t ? (this._hideDelay = t, this._show = !1, this._hideTimeout = setTimeout(ot(this.hide, this), t)) : this.hide());
    }, e.prototype.isShow = function() {
      return this._show;
    }, e.prototype.dispose = function() {
      clearTimeout(this._hideTimeout), clearTimeout(this._longHideTimeout);
      var t = this.el.parentNode;
      t && t.removeChild(this.el), this.el = this._container = null;
    }, e;
  })()
), ID = (
  /** @class */
  (function() {
    function e(t) {
      this._show = !1, this._styleCoord = [0, 0, 0, 0], this._alwaysShowContent = !1, this._enterable = !0, this._zr = t.getZr(), fd(this._styleCoord, this._zr, t.getWidth() / 2, t.getHeight() / 2);
    }
    return e.prototype.update = function(t) {
      var r = t.get("alwaysShowContent");
      r && this._moveIfResized(), this._alwaysShowContent = r;
    }, e.prototype.show = function() {
      this._hideTimeout && clearTimeout(this._hideTimeout), this.el.show(), this._show = !0;
    }, e.prototype.setContent = function(t, r, n, i, a) {
      var o = this;
      N(t) && At(""), this.el && this._zr.remove(this.el);
      var s = n.getModel("textStyle");
      this.el = new Bt({
        style: {
          rich: r.richTextStyles,
          text: t,
          lineHeight: 22,
          borderWidth: 1,
          borderColor: i,
          textShadowColor: s.get("textShadowColor"),
          fill: n.get(["textStyle", "color"]),
          padding: gy(n, "richText"),
          verticalAlign: "top",
          align: "left"
        },
        z: n.get("z")
      }), x(["backgroundColor", "borderRadius", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"], function(l) {
        o.el.style[l] = n.get(l);
      }), x(["textShadowBlur", "textShadowOffsetX", "textShadowOffsetY"], function(l) {
        o.el.style[l] = s.get(l) || 0;
      }), this._zr.add(this.el);
      var u = this;
      this.el.on("mouseover", function() {
        u._enterable && (clearTimeout(u._hideTimeout), u._show = !0), u._inContent = !0;
      }), this.el.on("mouseout", function() {
        u._enterable && u._show && u.hideLater(u._hideDelay), u._inContent = !1;
      });
    }, e.prototype.setEnterable = function(t) {
      this._enterable = t;
    }, e.prototype.getSize = function() {
      var t = this.el, r = this.el.getBoundingRect(), n = ld(t.style);
      return [r.width + n.left + n.right, r.height + n.top + n.bottom];
    }, e.prototype.moveTo = function(t, r) {
      var n = this.el;
      if (n) {
        var i = this._styleCoord;
        fd(i, this._zr, t, r), t = i[0], r = i[1];
        var a = n.style, o = tr(a.borderWidth || 0), s = ld(a);
        n.x = t + o + s.left, n.y = r + o + s.top, n.markRedraw();
      }
    }, e.prototype._moveIfResized = function() {
      var t = this._styleCoord[2], r = this._styleCoord[3];
      this.moveTo(t * this._zr.getWidth(), r * this._zr.getHeight());
    }, e.prototype.hide = function() {
      this.el && this.el.hide(), this._show = !1;
    }, e.prototype.hideLater = function(t) {
      this._show && !(this._inContent && this._enterable) && !this._alwaysShowContent && (t ? (this._hideDelay = t, this._show = !1, this._hideTimeout = setTimeout(ot(this.hide, this), t)) : this.hide());
    }, e.prototype.isShow = function() {
      return this._show;
    }, e.prototype.dispose = function() {
      this._zr.remove(this.el);
    }, e;
  })()
);
function tr(e) {
  return Math.max(0, e);
}
function ld(e) {
  var t = tr(e.shadowBlur || 0), r = tr(e.shadowOffsetX || 0), n = tr(e.shadowOffsetY || 0);
  return {
    left: tr(t - r),
    right: tr(t + r),
    top: tr(t - n),
    bottom: tr(t + n)
  };
}
function fd(e, t, r, n) {
  e[0] = r, e[1] = n, e[2] = e[0] / t.getWidth(), e[3] = e[1] / t.getHeight();
}
var AD = new Et({
  shape: {
    x: -1,
    y: -1,
    width: 2,
    height: 2
  }
}), LD = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.init = function(r, n) {
      if (!(z.node || !n.getDom())) {
        var i = r.getComponent("tooltip"), a = this._renderMode = D1(i.get("renderMode"));
        this._tooltipContent = a === "richText" ? new ID(n) : new PD(n, {
          appendTo: i.get("appendToBody", !0) ? "body" : i.get("appendTo", !0)
        });
      }
    }, t.prototype.render = function(r, n, i) {
      if (!(z.node || !i.getDom())) {
        this.group.removeAll(), this._tooltipModel = r, this._ecModel = n, this._api = i;
        var a = this._tooltipContent;
        a.update(r), a.setEnterable(r.get("enterable")), this._initGlobalListener(), this._keepShow(), this._renderMode !== "richText" && r.get("transitionDuration") ? my(this, "_updatePosition", 50, "fixRate") : gl(this, "_updatePosition");
      }
    }, t.prototype._initGlobalListener = function() {
      var r = this._tooltipModel, n = r.get("triggerOn");
      R_("itemTooltip", this._api, ot(function(i, a, o) {
        n !== "none" && (n.indexOf(i) >= 0 ? this._tryShow(a, o) : i === "leave" && this._hide(o));
      }, this));
    }, t.prototype._keepShow = function() {
      var r = this._tooltipModel, n = this._ecModel, i = this._api, a = r.get("triggerOn");
      if (this._lastX != null && this._lastY != null && a !== "none" && a !== "click") {
        var o = this;
        clearTimeout(this._refreshUpdateTimeout), this._refreshUpdateTimeout = setTimeout(function() {
          !i.isDisposed() && o.manuallyShowTip(r, n, i, {
            x: o._lastX,
            y: o._lastY,
            dataByCoordSys: o._lastDataByCoordSys
          });
        });
      }
    }, t.prototype.manuallyShowTip = function(r, n, i, a) {
      if (!(a.from === this.uid || z.node || !i.getDom())) {
        var o = hd(a, i);
        this._ticket = "";
        var s = a.dataByCoordSys, u = kD(a, n, i);
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
          var f = AD;
          f.x = a.x, f.y = a.y, f.update(), at(f).tooltipConfig = {
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
          var h = E_(a, n), v = h.point[0], c = h.point[1];
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
    }, t.prototype.manuallyHideTip = function(r, n, i, a) {
      var o = this._tooltipContent;
      this._tooltipModel && o.hideLater(this._tooltipModel.get("hideDelay")), this._lastX = this._lastY = this._lastDataByCoordSys = null, a.from !== this.uid && this._hide(hd(a, i));
    }, t.prototype._manuallyAxisShowTip = function(r, n, i, a) {
      var o = a.seriesIndex, s = a.dataIndex, u = n.getComponent("axisPointer").coordSysAxesInfo;
      if (!(o == null || s == null || u == null)) {
        var l = n.getSeriesByIndex(o);
        if (l) {
          var f = l.getData(), h = ui([f.getItemModel(s), l, (l.coordinateSystem || {}).model], this._tooltipModel);
          if (h.get("trigger") === "axis")
            return i.dispatchAction({
              type: "updateAxisPointer",
              seriesIndex: o,
              dataIndex: s,
              position: a.position
            }), !0;
        }
      }
    }, t.prototype._tryShow = function(r, n) {
      var i = r.target, a = this._tooltipModel;
      if (a) {
        this._lastX = r.offsetX, this._lastY = r.offsetY;
        var o = r.dataByCoordSys;
        if (o && o.length)
          this._showAxisTooltip(o, r);
        else if (i) {
          var s = at(i);
          if (s.ssrType === "legend")
            return;
          this._lastDataByCoordSys = null;
          var u, l;
          gi(i, function(f) {
            if (at(f).dataIndex != null)
              return u = f, !0;
            if (at(f).tooltipConfig != null)
              return l = f, !0;
          }, !0), u ? this._showSeriesItemTooltip(r, u, n) : l ? this._showComponentItemTooltip(r, l, n) : this._hide(n);
        } else
          this._lastDataByCoordSys = null, this._hide(n);
      }
    }, t.prototype._showOrMove = function(r, n) {
      var i = r.get("showDelay");
      n = ot(n, this), clearTimeout(this._showTimout), i > 0 ? this._showTimout = setTimeout(n, i) : n();
    }, t.prototype._showAxisTooltip = function(r, n) {
      var i = this._ecModel, a = this._tooltipModel, o = [n.offsetX, n.offsetY], s = ui([n.tooltipOption], a), u = this._renderMode, l = [], f = Gi("section", {
        blocks: [],
        noHeader: !0
      }), h = [], v = new su();
      x(r, function(_) {
        x(_.dataByAxis, function(m) {
          var S = i.getComponent(m.axisDim + "Axis", m.axisIndex), b = m.value;
          if (!(!S || b == null)) {
            var w = A_(b, S.axis, i, m.seriesDataIndices, m.valueLabelOpt), T = Gi("section", {
              header: w,
              noHeader: !pe(w),
              sortBlocks: !0,
              blocks: []
            });
            f.blocks.push(T), x(m.seriesDataIndices, function(C) {
              var M = i.getSeriesByIndex(C.seriesIndex), D = C.dataIndexInside, P = M.getDataParams(D);
              if (!(P.dataIndex < 0)) {
                P.axisDim = m.axisDim, P.axisIndex = m.axisIndex, P.axisType = m.axisType, P.axisId = m.axisId, P.axisValue = Wf(S.axis, {
                  value: b
                }), P.axisValueLabel = w, P.marker = v.makeTooltipMarker("item", Ni(P.color), u);
                var A = Yc(M.formatTooltip(D, !0, null)), L = A.frag;
                if (L) {
                  var I = ui([M], a).get("valueFormatter");
                  T.blocks.push(I ? E({
                    valueFormatter: I
                  }, L) : L);
                }
                A.text && h.push(A.text), l.push(P);
              }
            });
          }
        });
      }), f.blocks.reverse(), h.reverse();
      var c = n.position, d = s.get("order"), y = Kc(f, v, u, d, i.get("useUTC"), s.get("textStyle"));
      y && h.unshift(y);
      var p = u === "richText" ? `

` : "<br/>", g = h.join(p);
      this._showOrMove(s, function() {
        this._updateContentNotChangedOnAxis(r, l) ? this._updatePosition(s, c, o[0], o[1], this._tooltipContent, l) : this._showTooltipContent(s, g, l, Math.random() + "", o[0], o[1], c, null, v);
      });
    }, t.prototype._showSeriesItemTooltip = function(r, n, i) {
      var a = this._ecModel, o = at(n), s = o.seriesIndex, u = a.getSeriesByIndex(s), l = o.dataModel || u, f = o.dataIndex, h = o.dataType, v = l.getData(h), c = this._renderMode, d = r.positionDefault, y = ui([v.getItemModel(f), l, u && (u.coordinateSystem || {}).model], this._tooltipModel, d ? {
        position: d
      } : null), p = y.get("trigger");
      if (!(p != null && p !== "item")) {
        var g = l.getDataParams(f, h), _ = new su();
        g.marker = _.makeTooltipMarker("item", Ni(g.color), c);
        var m = Yc(l.formatTooltip(f, !1, h)), S = y.get("order"), b = y.get("valueFormatter"), w = m.frag, T = w ? Kc(b ? E({
          valueFormatter: b
        }, w) : w, _, c, S, a.get("useUTC"), y.get("textStyle")) : m.text, C = "item_" + l.name + "_" + f;
        this._showOrMove(y, function() {
          this._showTooltipContent(y, T, g, C, r.offsetX, r.offsetY, r.position, r.target, _);
        }), i({
          type: "showTip",
          dataIndexInside: f,
          dataIndex: v.getRawIndex(f),
          seriesIndex: s,
          from: this.uid
        });
      }
    }, t.prototype._showComponentItemTooltip = function(r, n, i) {
      var a = this._renderMode === "html", o = at(n), s = o.tooltipConfig, u = s.option || {}, l = u.encodeHTMLContent;
      if (B(u)) {
        var f = u;
        u = {
          content: f,
          // Fixed formatter
          formatter: f
        }, l = !0;
      }
      l && a && u.content && (u = $(u), u.content = Pt(u.content));
      var h = [u], v = this._ecModel.getComponent(o.componentMainType, o.componentIndex);
      v && h.push(v), h.push({
        formatter: u.content
      });
      var c = r.positionDefault, d = ui(h, this._tooltipModel, c ? {
        position: c
      } : null), y = d.get("content"), p = Math.random() + "", g = new su();
      this._showOrMove(d, function() {
        var _ = $(d.get("formatterParams") || {});
        this._showTooltipContent(d, y, _, p, r.offsetX, r.offsetY, r.position, n, g);
      }), i({
        type: "showTip",
        from: this.uid
      });
    }, t.prototype._showTooltipContent = function(r, n, i, a, o, s, u, l, f) {
      if (this._ticket = "", !(!r.get("showContent") || !r.get("show"))) {
        var h = this._tooltipContent;
        h.setEnterable(r.get("enterable"));
        var v = r.get("formatter");
        u = u || r.get("position");
        var c = n, d = this._getNearestPoint([o, s], i, r.get("trigger"), r.get("borderColor")), y = d.color;
        if (v)
          if (B(v)) {
            var p = r.ecModel.get("useUTC"), g = k(i) ? i[0] : i, _ = g && g.axisType && g.axisType.indexOf("time") >= 0;
            c = v, _ && (c = ra(g.axisValue, c, p)), c = Sf(c, i, !0);
          } else if (U(v)) {
            var m = ot(function(S, b) {
              S === this._ticket && (h.setContent(b, f, r, y, u), this._updatePosition(r, u, o, s, h, i, l));
            }, this);
            this._ticket = a, c = v(i, a, m);
          } else
            c = v;
        h.setContent(c, f, r, y, u), h.show(r, y), this._updatePosition(r, u, o, s, h, i, l);
      }
    }, t.prototype._getNearestPoint = function(r, n, i, a) {
      if (i === "axis" || k(n))
        return {
          color: a || (this._renderMode === "html" ? "#fff" : "none")
        };
      if (!k(n))
        return {
          color: a || n.color || n.borderColor
        };
    }, t.prototype._updatePosition = function(r, n, i, a, o, s, u) {
      var l = this._api.getWidth(), f = this._api.getHeight();
      n = n || r.get("position");
      var h = o.getSize(), v = r.get("align"), c = r.get("verticalAlign"), d = u && u.getBoundingRect().clone();
      if (u && d.applyTransform(u.transform), U(n) && (n = n([i, a], s, o.el, d, {
        viewSize: [l, f],
        contentSize: h.slice()
      })), k(n))
        i = It(n[0], l), a = It(n[1], f);
      else if (N(n)) {
        var y = n;
        y.width = h[0], y.height = h[1];
        var p = mo(y, {
          width: l,
          height: f
        });
        i = p.x, a = p.y, v = null, c = null;
      } else if (B(n) && u) {
        var g = OD(n, d, h, r.get("borderWidth"));
        i = g[0], a = g[1];
      } else {
        var g = RD(i, a, o, l, f, v ? null : 20, c ? null : 20);
        i = g[0], a = g[1];
      }
      if (v && (i -= cd(v) ? h[0] / 2 : v === "right" ? h[0] : 0), c && (a -= cd(c) ? h[1] / 2 : c === "bottom" ? h[1] : 0), O_(r)) {
        var g = ED(i, a, o, l, f);
        i = g[0], a = g[1];
      }
      o.moveTo(i, a);
    }, t.prototype._updateContentNotChangedOnAxis = function(r, n) {
      var i = this._lastDataByCoordSys, a = this._cbParamsList, o = !!i && i.length === r.length;
      return o && x(i, function(s, u) {
        var l = s.dataByAxis || [], f = r[u] || {}, h = f.dataByAxis || [];
        o = o && l.length === h.length, o && x(l, function(v, c) {
          var d = h[c] || {}, y = v.seriesDataIndices || [], p = d.seriesDataIndices || [];
          o = o && v.value === d.value && v.axisType === d.axisType && v.axisId === d.axisId && y.length === p.length, o && x(y, function(g, _) {
            var m = p[_];
            o = o && g.seriesIndex === m.seriesIndex && g.dataIndex === m.dataIndex;
          }), a && x(v.seriesDataIndices, function(g) {
            var _ = g.seriesIndex, m = n[_], S = a[_];
            m && S && S.data !== m.data && (o = !1);
          });
        });
      }), this._lastDataByCoordSys = r, this._cbParamsList = n, !!o;
    }, t.prototype._hide = function(r) {
      this._lastDataByCoordSys = null, r({
        type: "hideTip",
        from: this.uid
      });
    }, t.prototype.dispose = function(r, n) {
      z.node || !n.getDom() || (gl(this, "_updatePosition"), this._tooltipContent.dispose(), Ml("itemTooltip", n));
    }, t.type = "tooltip", t;
  })($t)
);
function ui(e, t, r) {
  var n = t.ecModel, i;
  r ? (i = new st(r, n, n), i = new st(t.option, i, n)) : i = t;
  for (var a = e.length - 1; a >= 0; a--) {
    var o = e[a];
    o && (o instanceof st && (o = o.get("tooltip", !0)), B(o) && (o = {
      formatter: o
    }), o && (i = new st(o, i, n)));
  }
  return i;
}
function hd(e, t) {
  return e.dispatchAction || ot(t.dispatchAction, t);
}
function RD(e, t, r, n, i, a, o) {
  var s = r.getSize(), u = s[0], l = s[1];
  return a != null && (e + u + a + 2 > n ? e -= u + a : e += a), o != null && (t + l + o > i ? t -= l + o : t += o), [e, t];
}
function ED(e, t, r, n, i) {
  var a = r.getSize(), o = a[0], s = a[1];
  return e = Math.min(e + o, n) - o, t = Math.min(t + s, i) - s, e = Math.max(e, 0), t = Math.max(t, 0), [e, t];
}
function OD(e, t, r, n) {
  var i = r[0], a = r[1], o = Math.ceil(Math.SQRT2 * n) + 8, s = 0, u = 0, l = t.width, f = t.height;
  switch (e) {
    case "inside":
      s = t.x + l / 2 - i / 2, u = t.y + f / 2 - a / 2;
      break;
    case "top":
      s = t.x + l / 2 - i / 2, u = t.y - a - o;
      break;
    case "bottom":
      s = t.x + l / 2 - i / 2, u = t.y + f + o;
      break;
    case "left":
      s = t.x - i - o, u = t.y + f / 2 - a / 2;
      break;
    case "right":
      s = t.x + l + o, u = t.y + f / 2 - a / 2;
  }
  return [s, u];
}
function cd(e) {
  return e === "center" || e === "middle";
}
function kD(e, t, r) {
  var n = Kl(e).queryOptionMap, i = n.keys()[0];
  if (!(!i || i === "series")) {
    var a = Qi(t, i, n.get(i), {
      useDefault: !1,
      enableAll: !1,
      enableNone: !1
    }), o = a.models[0];
    if (o) {
      var s = r.getViewOfComponentModel(o), u;
      if (s.group.traverse(function(l) {
        var f = at(l).tooltipConfig;
        if (f && f.name === e.name)
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
function wP(e) {
  Rn(gD), e.registerComponentModel(_D), e.registerComponentView(LD), e.registerAction({
    type: "showTip",
    event: "showTip",
    update: "tooltip:manuallyShowTip"
  }, Lt), e.registerAction({
    type: "hideTip",
    event: "hideTip",
    update: "tooltip:manuallyHideTip"
  }, Lt);
}
var BD = function(e, t) {
  if (t === "all")
    return {
      type: "all",
      title: e.getLocaleModel().get(["legend", "selector", "all"])
    };
  if (t === "inverse")
    return {
      type: "inverse",
      title: e.getLocaleModel().get(["legend", "selector", "inverse"])
    };
}, Dl = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r.layoutMode = {
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
    return t.prototype.init = function(r, n, i) {
      this.mergeDefaultAndTheme(r, i), r.selected = r.selected || {}, this._updateSelector(r);
    }, t.prototype.mergeOption = function(r, n) {
      e.prototype.mergeOption.call(this, r, n), this._updateSelector(r);
    }, t.prototype._updateSelector = function(r) {
      var n = r.selector, i = this.ecModel;
      n === !0 && (n = r.selector = ["all", "inverse"]), k(n) && x(n, function(a, o) {
        B(a) && (a = {
          type: a
        }), n[o] = it(a, BD(i, a.type));
      });
    }, t.prototype.optionUpdated = function() {
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
    }, t.prototype._updateData = function(r) {
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
        f && jl(u) && n.push(u.name);
      }), this._availableNames = i;
      var a = this.get("data") || n, o = W(), s = F(a, function(u) {
        return (B(u) || ut(u)) && (u = {
          name: u
        }), o.get(u.name) ? null : (o.set(u.name, !0), new st(u, this, this.ecModel));
      }, this);
      this._data = dt(s, function(u) {
        return !!u;
      });
    }, t.prototype.getData = function() {
      return this._data;
    }, t.prototype.select = function(r) {
      var n = this.option.selected, i = this.get("selectedMode");
      if (i === "single") {
        var a = this._data;
        x(a, function(o) {
          n[o.get("name")] = !1;
        });
      }
      n[r] = !0;
    }, t.prototype.unSelect = function(r) {
      this.get("selectedMode") !== "single" && (this.option.selected[r] = !1);
    }, t.prototype.toggleSelected = function(r) {
      var n = this.option.selected;
      n.hasOwnProperty(r) || (n[r] = !0), this[n[r] ? "unSelect" : "select"](r);
    }, t.prototype.allSelect = function() {
      var r = this._data, n = this.option.selected;
      x(r, function(i) {
        n[i.get("name", !0)] = !0;
      });
    }, t.prototype.inverseSelect = function() {
      var r = this._data, n = this.option.selected;
      x(r, function(i) {
        var a = i.get("name", !0);
        n.hasOwnProperty(a) || (n[a] = !0), n[a] = !n[a];
      });
    }, t.prototype.isSelected = function(r) {
      var n = this.option.selected;
      return !(n.hasOwnProperty(r) && !n[r]) && rt(this._availableNames, r) >= 0;
    }, t.prototype.getOrient = function() {
      return this.get("orient") === "vertical" ? {
        index: 1,
        name: "vertical"
      } : {
        index: 0,
        name: "horizontal"
      };
    }, t.type = "legend.plain", t.dependencies = ["series"], t.defaultOption = {
      // zlevel: 0,
      z: 4,
      show: !0,
      orient: "horizontal",
      left: "center",
      // right: 'center',
      top: 0,
      // bottom: null,
      align: "auto",
      backgroundColor: "rgba(0,0,0,0)",
      borderColor: "#ccc",
      borderRadius: 0,
      borderWidth: 0,
      padding: 5,
      itemGap: 10,
      itemWidth: 25,
      itemHeight: 14,
      symbolRotate: "inherit",
      symbolKeepAspect: !0,
      inactiveColor: "#ccc",
      inactiveBorderColor: "#ccc",
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
        inactiveColor: "#ccc",
        inactiveWidth: 2,
        opacity: "inherit",
        type: "inherit",
        cap: "inherit",
        join: "inherit",
        dashOffset: "inherit",
        miterLimit: "inherit"
      },
      textStyle: {
        color: "#333"
      },
      selectedMode: !0,
      selector: !1,
      selectorLabel: {
        show: !0,
        borderRadius: 10,
        padding: [3, 5, 3, 5],
        fontSize: 12,
        fontFamily: "sans-serif",
        color: "#666",
        borderWidth: 1,
        borderColor: "#666"
      },
      emphasis: {
        selectorLabel: {
          show: !0,
          color: "#eee",
          backgroundColor: "#666"
        }
      },
      selectorPosition: "auto",
      selectorItemGap: 7,
      selectorButtonGap: 10,
      tooltip: {
        show: !1
      }
    }, t;
  })(Q)
), _n = pt, Pl = x, Ha = oe, N_ = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r.newlineDisabled = !1, r;
    }
    return t.prototype.init = function() {
      this.group.add(this._contentGroup = new Ha()), this.group.add(this._selectorGroup = new Ha()), this._isFirstRender = !0;
    }, t.prototype.getContentGroup = function() {
      return this._contentGroup;
    }, t.prototype.getSelectorGroup = function() {
      return this._selectorGroup;
    }, t.prototype.render = function(r, n, i) {
      var a = this._isFirstRender;
      if (this._isFirstRender = !1, this.resetInner(), !!r.get("show", !0)) {
        var o = r.get("align"), s = r.get("orient");
        (!o || o === "auto") && (o = r.get("left") === "right" && s === "vertical" ? "right" : "left");
        var u = r.get("selector", !0), l = r.get("selectorPosition", !0);
        u && (!l || l === "auto") && (l = s === "horizontal" ? "end" : "start"), this.renderInner(o, r, n, i, u, s, l);
        var f = r.getBoxLayoutParams(), h = {
          width: i.getWidth(),
          height: i.getHeight()
        }, v = r.get("padding"), c = mo(f, h, v), d = this.layoutInner(r, o, c, a, u, l), y = mo(ft({
          width: d.width,
          height: d.height
        }, f), h, v);
        this.group.x = y.x - d.x, this.group.y = y.y - d.y, this.group.markRedraw(), this.group.add(this._backgroundEl = yD(d, r));
      }
    }, t.prototype.resetInner = function() {
      this.getContentGroup().removeAll(), this._backgroundEl && this.group.remove(this._backgroundEl), this.getSelectorGroup().removeAll();
    }, t.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this.getContentGroup(), f = W(), h = n.get("selectedMode"), v = [];
      i.eachRawSeries(function(c) {
        !c.get("legendHoverLink") && v.push(c.id);
      }), Pl(n.getData(), function(c, d) {
        var y = c.get("name");
        if (!this.newlineDisabled && (y === "" || y === `
`)) {
          var p = new Ha();
          p.newline = !0, l.add(p);
          return;
        }
        var g = i.getSeriesByName(y)[0];
        if (!f.get(y))
          if (g) {
            var _ = g.getData(), m = _.getVisual("legendLineStyle") || {}, S = _.getVisual("legendIcon"), b = _.getVisual("style"), w = this._createItem(g, y, d, c, n, r, m, b, S, h, a);
            w.on("click", _n(vd, y, null, a, v)).on("mouseover", _n(Il, g.name, null, a, v)).on("mouseout", _n(Al, g.name, null, a, v)), i.ssr && w.eachChild(function(T) {
              var C = at(T);
              C.seriesIndex = g.seriesIndex, C.dataIndex = d, C.ssrType = "legend";
            }), f.set(y, !0);
          } else
            i.eachRawSeries(function(T) {
              if (!f.get(y) && T.legendVisualProvider) {
                var C = T.legendVisualProvider;
                if (!C.containName(y))
                  return;
                var M = C.indexOfName(y), D = C.getItemVisual(M, "style"), P = C.getItemVisual(M, "legendIcon"), A = qt(D.fill);
                A && A[3] === 0 && (A[3] = 0.2, D = E(E({}, D), {
                  fill: cr(A, "rgba")
                }));
                var L = this._createItem(T, y, d, c, n, r, {}, D, P, h, a);
                L.on("click", _n(vd, null, y, a, v)).on("mouseover", _n(Il, null, y, a, v)).on("mouseout", _n(Al, null, y, a, v)), i.ssr && L.eachChild(function(I) {
                  var R = at(I);
                  R.seriesIndex = T.seriesIndex, R.dataIndex = d, R.ssrType = "legend";
                }), f.set(y, !0);
              }
            }, this);
      }, this), o && this._createSelector(o, n, a, s, u);
    }, t.prototype._createSelector = function(r, n, i, a, o) {
      var s = this.getSelectorGroup();
      Pl(r, function(l) {
        var f = l.type, h = new Bt({
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
        Lw(h, {
          normal: v,
          emphasis: c
        }, {
          defaultText: l.title
        }), vo(h);
      });
    }, t.prototype._createItem = function(r, n, i, a, o, s, u, l, f, h, v) {
      var c = r.visualDrawType, d = o.get("itemWidth"), y = o.get("itemHeight"), p = o.isSelected(n), g = a.get("symbolRotate"), _ = a.get("symbolKeepAspect"), m = a.get("icon");
      f = m || f || "roundRect";
      var S = FD(f, a, u, l, c, p, v), b = new Ha(), w = a.getModel("textStyle");
      if (U(r.getLegendIcon) && (!m || m === "inherit"))
        b.add(r.getLegendIcon({
          itemWidth: d,
          itemHeight: y,
          icon: f,
          iconRotate: g,
          itemStyle: S.itemStyle,
          lineStyle: S.lineStyle,
          symbolKeepAspect: _
        }));
      else {
        var T = m === "inherit" && r.getData().getVisual("symbol") ? g === "inherit" ? r.getData().getVisual("symbolRotate") : g : 0;
        b.add(ND({
          itemWidth: d,
          itemHeight: y,
          icon: f,
          iconRotate: T,
          itemStyle: S.itemStyle,
          symbolKeepAspect: _
        }));
      }
      var C = s === "left" ? d + 5 : -5, M = s, D = o.get("formatter"), P = n;
      B(D) && D ? P = D.replace("{name}", n ?? "") : U(D) && (P = D(n));
      var A = p ? w.getTextColor() : a.get("inactiveColor");
      b.add(new Bt({
        style: Qr(w, {
          text: P,
          x: C,
          y: y / 2,
          fill: A,
          align: M,
          verticalAlign: "middle"
        }, {
          inheritColor: A
        })
      }));
      var L = new Et({
        shape: b.getBoundingRect(),
        style: {
          // Cannot use 'invisible' because SVG SSR will miss the node
          fill: "transparent"
        }
      }), I = a.getModel("tooltip");
      return I.get("show") && jo({
        el: L,
        componentModel: o,
        itemName: n,
        itemTooltipOption: I.option
      }), b.add(L), b.eachChild(function(R) {
        R.silent = !0;
      }), L.silent = !h, this.getContentGroup().add(b), vo(b), b.__legendDataIndex = i, b;
    }, t.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getContentGroup(), l = this.getSelectorGroup();
      Ci(r.get("orient"), u, r.get("itemGap"), i.width, i.height);
      var f = u.getBoundingRect(), h = [-f.x, -f.y];
      if (l.markRedraw(), u.markRedraw(), o) {
        Ci(
          // Buttons in selectorGroup always layout horizontally
          "horizontal",
          l,
          r.get("selectorItemGap", !0)
        );
        var v = l.getBoundingRect(), c = [-v.x, -v.y], d = r.get("selectorButtonGap", !0), y = r.getOrient().index, p = y === 0 ? "width" : "height", g = y === 0 ? "height" : "width", _ = y === 0 ? "y" : "x";
        s === "end" ? c[y] += f[p] + d : h[y] += v[p] + d, c[1 - y] += f[g] / 2 - v[g] / 2, l.x = c[0], l.y = c[1], u.x = h[0], u.y = h[1];
        var m = {
          x: 0,
          y: 0
        };
        return m[p] = f[p] + d + v[p], m[g] = Math.max(f[g], v[g]), m[_] = Math.min(0, v[_] + c[1 - y]), m;
      } else
        return u.x = h[0], u.y = h[1], this.group.getBoundingRect();
    }, t.prototype.remove = function() {
      this.getContentGroup().removeAll(), this._isFirstRender = !0;
    }, t.type = "legend.plain", t;
  })($t)
);
function FD(e, t, r, n, i, a, o) {
  function s(p, g) {
    p.lineWidth === "auto" && (p.lineWidth = g.lineWidth > 0 ? 2 : 0), Pl(p, function(_, m) {
      p[m] === "inherit" && (p[m] = g[m]);
    });
  }
  var u = t.getModel("itemStyle"), l = u.getItemStyle(), f = e.lastIndexOf("empty", 0) === 0 ? "fill" : "stroke", h = u.getShallow("decal");
  l.decal = !h || h === "inherit" ? n.decal : wl(h, o), l.fill === "inherit" && (l.fill = n[i]), l.stroke === "inherit" && (l.stroke = n[f]), l.opacity === "inherit" && (l.opacity = (i === "fill" ? n : r).opacity), s(l, n);
  var v = t.getModel("lineStyle"), c = v.getLineStyle();
  if (s(c, r), l.fill === "auto" && (l.fill = n.fill), l.stroke === "auto" && (l.stroke = n.fill), c.stroke === "auto" && (c.stroke = n.fill), !a) {
    var d = t.get("inactiveBorderWidth"), y = l[f];
    l.lineWidth = d === "auto" ? n.lineWidth > 0 && y ? 2 : 0 : l.lineWidth, l.fill = t.get("inactiveColor"), l.stroke = t.get("inactiveBorderColor"), c.stroke = v.get("inactiveColor"), c.lineWidth = v.get("inactiveWidth");
  }
  return {
    itemStyle: l,
    lineStyle: c
  };
}
function ND(e) {
  var t = e.icon || "roundRect", r = os(t, 0, 0, e.itemWidth, e.itemHeight, e.itemStyle.fill, e.symbolKeepAspect);
  return r.setStyle(e.itemStyle), r.rotation = (e.iconRotate || 0) * Math.PI / 180, r.setOrigin([e.itemWidth / 2, e.itemHeight / 2]), t.indexOf("empty") > -1 && (r.style.stroke = r.style.fill, r.style.fill = "#fff", r.style.lineWidth = 2), r;
}
function vd(e, t, r, n) {
  Al(e, t, r, n), r.dispatchAction({
    type: "legendToggleSelect",
    name: e ?? t
  }), Il(e, t, r, n);
}
function z_(e) {
  for (var t = e.getZr().storage.getDisplayList(), r, n = 0, i = t.length; n < i && !(r = t[n].states.emphasis); )
    n++;
  return r && r.hoverLayer;
}
function Il(e, t, r, n) {
  z_(r) || r.dispatchAction({
    type: "highlight",
    seriesName: e,
    name: t,
    excludeSeriesId: n
  });
}
function Al(e, t, r, n) {
  z_(r) || r.dispatchAction({
    type: "downplay",
    seriesName: e,
    name: t,
    excludeSeriesId: n
  });
}
function zD(e) {
  var t = e.findComponents({
    mainType: "legend"
  });
  t && t.length && e.filterSeries(function(r) {
    for (var n = 0; n < t.length; n++)
      if (!t[n].isSelected(r.name))
        return !1;
    return !0;
  });
}
function li(e, t, r) {
  var n = e === "allSelect" || e === "inverseSelect", i = {}, a = [];
  r.eachComponent({
    mainType: "legend",
    query: t
  }, function(s) {
    n ? s[e]() : s[e](t.name), dd(s, i), a.push(s.componentIndex);
  });
  var o = {};
  return r.eachComponent("legend", function(s) {
    x(i, function(u, l) {
      s[u ? "select" : "unSelect"](l);
    }), dd(s, o);
  }), n ? {
    selected: o,
    // return legendIndex array to tell the developers which legends are allSelect / inverseSelect
    legendIndex: a
  } : {
    name: t.name,
    selected: o
  };
}
function dd(e, t) {
  var r = t || {};
  return x(e.getData(), function(n) {
    var i = n.get("name");
    if (!(i === `
` || i === "")) {
      var a = e.isSelected(i);
      We(r, i) ? r[i] = r[i] && a : r[i] = a;
    }
  }), r;
}
function HD(e) {
  e.registerAction("legendToggleSelect", "legendselectchanged", pt(li, "toggleSelected")), e.registerAction("legendAllSelect", "legendselectall", pt(li, "allSelect")), e.registerAction("legendInverseSelect", "legendinverseselect", pt(li, "inverseSelect")), e.registerAction("legendSelect", "legendselected", pt(li, "select")), e.registerAction("legendUnSelect", "legendunselected", pt(li, "unSelect"));
}
function H_(e) {
  e.registerComponentModel(Dl), e.registerComponentView(N_), e.registerProcessor(e.PRIORITY.PROCESSOR.SERIES_FILTER, zD), e.registerSubTypeDefaulter("legend", function() {
    return "plain";
  }), HD(e);
}
var GD = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.setScrollDataIndex = function(r) {
      this.option.scrollDataIndex = r;
    }, t.prototype.init = function(r, n, i) {
      var a = bf(r);
      e.prototype.init.call(this, r, n, i), pd(this, r, a);
    }, t.prototype.mergeOption = function(r, n) {
      e.prototype.mergeOption.call(this, r, n), pd(this, this.option, r);
    }, t.type = "legend.scroll", t.defaultOption = Uw(Dl.defaultOption, {
      scrollDataIndex: 0,
      pageButtonItemGap: 5,
      pageButtonGap: null,
      pageButtonPosition: "end",
      pageFormatter: "{current}/{total}",
      pageIcons: {
        horizontal: ["M0,0L12,-10L12,10z", "M0,0L-12,-10L-12,10z"],
        vertical: ["M0,0L20,0L10,-20z", "M0,0L20,0L10,20z"]
      },
      pageIconColor: "#2f4554",
      pageIconInactiveColor: "#aaa",
      pageIconSize: 15,
      pageTextStyle: {
        color: "#333"
      },
      animationDurationUpdate: 800
    }), t;
  })(Dl)
);
function pd(e, t, r) {
  var n = e.getOrient(), i = [1, 1];
  i[n.index] = 0, zi(t, r, {
    type: "box",
    ignoreSize: !!i
  });
}
var gd = oe, Cu = ["width", "height"], Mu = ["x", "y"], VD = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r.newlineDisabled = !0, r._currentIndex = 0, r;
    }
    return t.prototype.init = function() {
      e.prototype.init.call(this), this.group.add(this._containerGroup = new gd()), this._containerGroup.add(this.getContentGroup()), this.group.add(this._controllerGroup = new gd());
    }, t.prototype.resetInner = function() {
      e.prototype.resetInner.call(this), this._controllerGroup.removeAll(), this._containerGroup.removeClipPath(), this._containerGroup.__rectSize = null;
    }, t.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this;
      e.prototype.renderInner.call(this, r, n, i, a, o, s, u);
      var f = this._controllerGroup, h = n.get("pageIconSize", !0), v = k(h) ? h : [h, h];
      d("pagePrev", 0);
      var c = n.getModel("pageTextStyle");
      f.add(new Bt({
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
        var g = y + "DataIndex", _ = Zo(n.get("pageIcons", !0)[n.getOrient().name][p], {
          // Buttons will be created in each render, so we do not need
          // to worry about avoiding using legendModel kept in scope.
          onclick: ot(l._pageGo, l, g, n, a)
        }, {
          x: -v[0] / 2,
          y: -v[1] / 2,
          width: v[0],
          height: v[1]
        });
        _.name = y, f.add(_);
      }
    }, t.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getSelectorGroup(), l = r.getOrient().index, f = Cu[l], h = Mu[l], v = Cu[1 - l], c = Mu[1 - l];
      o && Ci(
        // Buttons in selectorGroup always layout horizontally
        "horizontal",
        u,
        r.get("selectorItemGap", !0)
      );
      var d = r.get("selectorButtonGap", !0), y = u.getBoundingRect(), p = [-y.x, -y.y], g = $(i);
      o && (g[f] = i[f] - y[f] - d);
      var _ = this._layoutContentAndController(r, a, g, l, f, v, c, h);
      if (o) {
        if (s === "end")
          p[l] += _[f] + d;
        else {
          var m = y[f] + d;
          p[l] -= m, _[h] -= m;
        }
        _[f] += y[f] + d, p[1 - l] += _[c] + _[v] / 2 - y[v] / 2, _[v] = Math.max(_[v], y[v]), _[c] = Math.min(_[c], y[c] + p[1 - l]), u.x = p[0], u.y = p[1], u.markRedraw();
      }
      return _;
    }, t.prototype._layoutContentAndController = function(r, n, i, a, o, s, u, l) {
      var f = this.getContentGroup(), h = this._containerGroup, v = this._controllerGroup;
      Ci(r.get("orient"), f, r.get("itemGap"), a ? i.width : null, a ? null : i.height), Ci(
        // Buttons in controller are layout always horizontally.
        "horizontal",
        v,
        r.get("pageButtonItemGap", !0)
      );
      var c = f.getBoundingRect(), d = v.getBoundingRect(), y = this._showController = c[o] > i[o], p = [-c.x, -c.y];
      n || (p[a] = f[l]);
      var g = [0, 0], _ = [-d.x, -d.y], m = Y(r.get("pageButtonGap", !0), r.get("itemGap", !0));
      if (y) {
        var S = r.get("pageButtonPosition", !0);
        S === "end" ? _[a] += i[o] - d[o] : g[a] += d[o] + m;
      }
      _[1 - a] += c[s] / 2 - d[s] / 2, f.setPosition(p), h.setPosition(g), v.setPosition(_);
      var b = {
        x: 0,
        y: 0
      };
      if (b[o] = y ? i[o] : c[o], b[s] = Math.max(c[s], d[s]), b[u] = Math.min(0, d[u] + _[1 - a]), h.__rectSize = i[o], y) {
        var w = {
          x: 0,
          y: 0
        };
        w[o] = Math.max(i[o] - d[o] - m, 0), w[s] = b[s], h.setClipPath(new Et({
          shape: w
        })), h.__rectSize = w[o];
      } else
        v.eachChild(function(C) {
          C.attr({
            invisible: !0,
            silent: !0
          });
        });
      var T = this._getPageInfo(r);
      return T.pageIndex != null && On(
        f,
        {
          x: T.contentPosition[0],
          y: T.contentPosition[1]
        },
        // When switch from "show controller" to "not show controller", view should be
        // updated immediately without animation, otherwise causes weird effect.
        y ? r : null
      ), this._updatePageInfoView(r, T), b;
    }, t.prototype._pageGo = function(r, n, i) {
      var a = this._getPageInfo(n)[r];
      a != null && i.dispatchAction({
        type: "legendScroll",
        scrollDataIndex: a,
        legendId: n.id
      });
    }, t.prototype._updatePageInfoView = function(r, n) {
      var i = this._controllerGroup;
      x(["pagePrev", "pageNext"], function(f) {
        var h = f + "DataIndex", v = n[h] != null, c = i.childOfName(f);
        c && (c.setStyle("fill", v ? r.get("pageIconColor", !0) : r.get("pageIconInactiveColor", !0)), c.cursor = v ? "pointer" : "default");
      });
      var a = i.childOfName("pageText"), o = r.get("pageFormatter"), s = n.pageIndex, u = s != null ? s + 1 : 0, l = n.pageCount;
      a && o && a.setStyle("text", B(o) ? o.replace("{current}", u == null ? "" : u + "").replace("{total}", l == null ? "" : l + "") : o({
        current: u,
        total: l
      }));
    }, t.prototype._getPageInfo = function(r) {
      var n = r.get("scrollDataIndex", !0), i = this.getContentGroup(), a = this._containerGroup.__rectSize, o = r.getOrient().index, s = Cu[o], u = Mu[o], l = this._findTargetItemIndex(n), f = i.children(), h = f[l], v = f.length, c = v ? 1 : 0, d = {
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
      for (var p = l + 1, g = y, _ = y, m = null; p <= v; ++p)
        m = S(f[p]), // Half of the last item is out of the window.
        (!m && _.e > g.s + a || m && !b(m, g.s)) && (_.i > g.i ? g = _ : g = m, g && (d.pageNextDataIndex == null && (d.pageNextDataIndex = g.i), ++d.pageCount)), _ = m;
      for (var p = l - 1, g = y, _ = y, m = null; p >= -1; --p)
        m = S(f[p]), // If the the end item does not intersect with the window started
        // from the current item, a page can be settled.
        (!m || !b(_, m.s)) && g.i < _.i && (_ = g, d.pagePrevDataIndex == null && (d.pagePrevDataIndex = g.i), ++d.pageCount, ++d.pageIndex), g = m;
      return d;
      function S(w) {
        if (w) {
          var T = w.getBoundingRect(), C = T[u] + w[u];
          return {
            s: C,
            e: C + T[s],
            i: w.__legendDataIndex
          };
        }
      }
      function b(w, T) {
        return w.e >= T && w.s <= T + a;
      }
    }, t.prototype._findTargetItemIndex = function(r) {
      if (!this._showController)
        return 0;
      var n, i = this.getContentGroup(), a;
      return i.eachChild(function(o, s) {
        var u = o.__legendDataIndex;
        a == null && u != null && (a = s), u === r && (n = s);
      }), n ?? a;
    }, t.type = "legend.scroll", t;
  })(N_)
);
function WD(e) {
  e.registerAction("legendScroll", "legendscroll", function(t, r) {
    var n = t.scrollDataIndex;
    n != null && r.eachComponent({
      mainType: "legend",
      subType: "scroll",
      query: t
    }, function(i) {
      i.setScrollDataIndex(n);
    });
  });
}
function UD(e) {
  Rn(H_), e.registerComponentModel(GD), e.registerComponentView(VD), WD(e);
}
function bP(e) {
  Rn(H_), Rn(UD);
}
var YD = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return t.prototype.init = function(r, n, i) {
      e.prototype.init.call(this, r, n, i), this._sourceManager = new fy(this), Zc(this);
    }, t.prototype.mergeOption = function(r, n) {
      e.prototype.mergeOption.call(this, r, n), Zc(this);
    }, t.prototype.optionUpdated = function() {
      this._sourceManager.dirty();
    }, t.prototype.getSourceManager = function() {
      return this._sourceManager;
    }, t.type = "dataset", t.defaultOption = {
      seriesLayoutBy: Me
    }, t;
  })(Q)
), qD = (
  /** @class */
  (function(e) {
    lt(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return t.type = "dataset", t;
  })($t)
);
function TP(e) {
  e.registerComponentModel(YD), e.registerComponentView(qD);
}
function yd(e, t, r) {
  var n = Ve.createCanvas(), i = t.getWidth(), a = t.getHeight(), o = n.style;
  return o && (o.position = "absolute", o.left = "0", o.top = "0", o.width = i + "px", o.height = a + "px", n.setAttribute("data-zr-dom-id", e)), n.width = i * r, n.height = a * r, n;
}
var Du = (function(e) {
  tt(t, e);
  function t(r, n, i) {
    var a = e.call(this) || this;
    a.motionBlur = !1, a.lastFrameAlpha = 0.7, a.dpr = 1, a.virtual = !1, a.config = {}, a.incremental = !1, a.zlevel = 0, a.maxRepaintRectCount = 5, a.__dirty = !0, a.__firstTimePaint = !0, a.__used = !1, a.__drawIndex = 0, a.__startIndex = 0, a.__endIndex = 0, a.__prevStartIndex = null, a.__prevEndIndex = null;
    var o;
    i = i || ho, typeof r == "string" ? o = yd(r, n, i) : N(r) && (o = r, r = o.id), a.id = r, a.dom = o;
    var s = o.style;
    return s && (zl(o), o.onselectstart = function() {
      return !1;
    }, s.padding = "0", s.margin = "0", s.borderWidth = "0"), a.painter = n, a.dpr = i, a;
  }
  return t.prototype.getElementCount = function() {
    return this.__endIndex - this.__startIndex;
  }, t.prototype.afterBrush = function() {
    this.__prevStartIndex = this.__startIndex, this.__prevEndIndex = this.__endIndex;
  }, t.prototype.initContext = function() {
    this.ctx = this.dom.getContext("2d"), this.ctx.dpr = this.dpr;
  }, t.prototype.setUnpainted = function() {
    this.__firstTimePaint = !0;
  }, t.prototype.createBackBuffer = function() {
    var r = this.dpr;
    this.domBack = yd("back-" + this.id, this.painter, r), this.ctxBack = this.domBack.getContext("2d"), r !== 1 && this.ctxBack.scale(r, r);
  }, t.prototype.createRepaintRects = function(r, n, i, a) {
    if (this.__firstTimePaint)
      return this.__firstTimePaint = !1, null;
    var o = [], s = this.maxRepaintRectCount, u = !1, l = new j(0, 0, 0, 0);
    function f(_) {
      if (!(!_.isFinite() || _.isZero()))
        if (o.length === 0) {
          var m = new j(0, 0, 0, 0);
          m.copy(_), o.push(m);
        } else {
          for (var S = !1, b = 1 / 0, w = 0, T = 0; T < o.length; ++T) {
            var C = o[T];
            if (C.intersect(_)) {
              var M = new j(0, 0, 0, 0);
              M.copy(C), M.union(_), o[T] = M, S = !0;
              break;
            } else if (u) {
              l.copy(_), l.union(C);
              var D = _.width * _.height, P = C.width * C.height, A = l.width * l.height, L = A - D - P;
              L < b && (b = L, w = T);
            }
          }
          if (u && (o[w].union(_), S = !0), !S) {
            var m = new j(0, 0, 0, 0);
            m.copy(_), o.push(m);
          }
          u || (u = o.length >= s);
        }
    }
    for (var h = this.__startIndex; h < this.__endIndex; ++h) {
      var v = r[h];
      if (v) {
        var c = v.shouldBePainted(i, a, !0, !0), d = v.__isRendered && (v.__dirty & Ut || !c) ? v.getPrevPaintRect() : null;
        d && f(d);
        var y = c && (v.__dirty & Ut || !v.__isRendered) ? v.getPaintRect() : null;
        y && f(y);
      }
    }
    for (var h = this.__prevStartIndex; h < this.__prevEndIndex; ++h) {
      var v = n[h], c = v && v.shouldBePainted(i, a, !0, !0);
      if (v && (!c || !v.__zr) && v.__isRendered) {
        var d = v.getPrevPaintRect();
        d && f(d);
      }
    }
    var p;
    do {
      p = !1;
      for (var h = 0; h < o.length; ) {
        if (o[h].isZero()) {
          o.splice(h, 1);
          continue;
        }
        for (var g = h + 1; g < o.length; )
          o[h].intersect(o[g]) ? (p = !0, o[h].union(o[g]), o.splice(g, 1)) : g++;
        h++;
      }
    } while (p);
    return this._paintRects = o, o;
  }, t.prototype.debugGetPaintRects = function() {
    return (this._paintRects || []).slice();
  }, t.prototype.resize = function(r, n) {
    var i = this.dpr, a = this.dom, o = a.style, s = this.domBack;
    o && (o.width = r + "px", o.height = n + "px"), a.width = r * i, a.height = n * i, s && (s.width = r * i, s.height = n * i, i !== 1 && this.ctxBack.scale(i, i));
  }, t.prototype.clear = function(r, n, i) {
    var a = this.dom, o = this.ctx, s = a.width, u = a.height;
    n = n || this.clearColor;
    var l = this.motionBlur && !r, f = this.lastFrameAlpha, h = this.dpr, v = this;
    l && (this.domBack || this.createBackBuffer(), this.ctxBack.globalCompositeOperation = "copy", this.ctxBack.drawImage(a, 0, 0, s / h, u / h));
    var c = this.domBack;
    function d(y, p, g, _) {
      if (o.clearRect(y, p, g, _), n && n !== "transparent") {
        var m = void 0;
        if (qi(n)) {
          var S = n.global || n.__width === g && n.__height === _;
          m = S && n.__canvasGradient || ml(o, n, {
            x: 0,
            y: 0,
            width: g,
            height: _
          }), n.__canvasGradient = m, n.__width = g, n.__height = _;
        } else Kd(n) && (n.scaleX = n.scaleX || h, n.scaleY = n.scaleY || h, m = Sl(o, n, {
          dirty: function() {
            v.setUnpainted(), v.painter.refresh();
          }
        }));
        o.save(), o.fillStyle = m || n, o.fillRect(y, p, g, _), o.restore();
      }
      l && (o.save(), o.globalAlpha = f, o.drawImage(c, y, p, g, _), o.restore());
    }
    !i || l ? d(0, 0, s, u) : i.length && x(i, function(y) {
      d(y.x * h, y.y * h, y.width * h, y.height * h);
    });
  }, t;
})(Le), _d = 1e5, Nr = 314159, Ga = 0.01, XD = 1e-3;
function $D(e) {
  return e ? e.__builtin__ ? !0 : !(typeof e.resize != "function" || typeof e.refresh != "function") : !1;
}
function ZD(e, t) {
  var r = document.createElement("div");
  return r.style.cssText = [
    "position:relative",
    "width:" + e + "px",
    "height:" + t + "px",
    "padding:0",
    "margin:0",
    "border-width:0"
  ].join(";") + ";", r;
}
var jD = (function() {
  function e(t, r, n, i) {
    this.type = "canvas", this._zlevelList = [], this._prevDisplayList = [], this._layers = {}, this._layerConfig = {}, this._needsManuallyCompositing = !1, this.type = "canvas";
    var a = !t.nodeName || t.nodeName.toUpperCase() === "CANVAS";
    this._opts = n = E({}, n || {}), this.dpr = n.devicePixelRatio || ho, this._singleCanvas = a, this.root = t;
    var o = t.style;
    o && (zl(t), t.innerHTML = ""), this.storage = r;
    var s = this._zlevelList;
    this._prevDisplayList = [];
    var u = this._layers;
    if (a) {
      var f = t, h = f.width, v = f.height;
      n.width != null && (h = n.width), n.height != null && (v = n.height), this.dpr = n.devicePixelRatio || 1, f.width = h * this.dpr, f.height = v * this.dpr, this._width = h, this._height = v;
      var c = new Du(f, this, this.dpr);
      c.__builtin__ = !0, c.initContext(), u[Nr] = c, c.zlevel = Nr, s.push(Nr), this._domRoot = t;
    } else {
      this._width = Ra(t, 0, n), this._height = Ra(t, 1, n);
      var l = this._domRoot = ZD(this._width, this._height);
      t.appendChild(l);
    }
  }
  return e.prototype.getType = function() {
    return "canvas";
  }, e.prototype.isSingleCanvas = function() {
    return this._singleCanvas;
  }, e.prototype.getViewportRoot = function() {
    return this._domRoot;
  }, e.prototype.getViewportRootOffset = function() {
    var t = this.getViewportRoot();
    if (t)
      return {
        offsetLeft: t.offsetLeft || 0,
        offsetTop: t.offsetTop || 0
      };
  }, e.prototype.refresh = function(t) {
    var r = this.storage.getDisplayList(!0), n = this._prevDisplayList, i = this._zlevelList;
    this._redrawId = Math.random(), this._paintList(r, n, t, this._redrawId);
    for (var a = 0; a < i.length; a++) {
      var o = i[a], s = this._layers[o];
      if (!s.__builtin__ && s.refresh) {
        var u = a === 0 ? this._backgroundColor : null;
        s.refresh(u);
      }
    }
    return this._opts.useDirtyRect && (this._prevDisplayList = r.slice()), this;
  }, e.prototype.refreshHover = function() {
    this._paintHoverList(this.storage.getDisplayList(!1));
  }, e.prototype._paintHoverList = function(t) {
    var r = t.length, n = this._hoverlayer;
    if (n && n.clear(), !!r) {
      for (var i = {
        inHover: !0,
        viewWidth: this._width,
        viewHeight: this._height
      }, a, o = 0; o < r; o++) {
        var s = t[o];
        s.__inHover && (n || (n = this._hoverlayer = this.getLayer(_d)), a || (a = n.ctx, a.save()), Wr(a, s, i, o === r - 1));
      }
      a && a.restore();
    }
  }, e.prototype.getHoverLayer = function() {
    return this.getLayer(_d);
  }, e.prototype.paintOne = function(t, r) {
    If(t, r);
  }, e.prototype._paintList = function(t, r, n, i) {
    if (this._redrawId === i) {
      n = n || !1, this._updateLayerStatus(t);
      var a = this._doPaintList(t, r, n), o = a.finished, s = a.needsRefreshHover;
      if (this._needsManuallyCompositing && this._compositeManually(), s && this._paintHoverList(t), o)
        this.eachLayer(function(l) {
          l.afterBrush && l.afterBrush();
        });
      else {
        var u = this;
        so(function() {
          u._paintList(t, r, n, i);
        });
      }
    }
  }, e.prototype._compositeManually = function() {
    var t = this.getLayer(Nr).ctx, r = this._domRoot.width, n = this._domRoot.height;
    t.clearRect(0, 0, r, n), this.eachBuiltinLayer(function(i) {
      i.virtual && t.drawImage(i.dom, 0, 0, r, n);
    });
  }, e.prototype._doPaintList = function(t, r, n) {
    for (var i = this, a = [], o = this._opts.useDirtyRect, s = 0; s < this._zlevelList.length; s++) {
      var u = this._zlevelList[s], l = this._layers[u];
      l.__builtin__ && l !== this._hoverlayer && (l.__dirty || n) && a.push(l);
    }
    for (var f = !0, h = !1, v = function(y) {
      var p = a[y], g = p.ctx, _ = o && p.createRepaintRects(t, r, c._width, c._height), m = n ? p.__startIndex : p.__drawIndex, S = !n && p.incremental && Date.now, b = S && Date.now(), w = p.zlevel === c._zlevelList[0] ? c._backgroundColor : null;
      if (p.__startIndex === p.__endIndex)
        p.clear(!1, w, _);
      else if (m === p.__startIndex) {
        var T = t[m];
        (!T.incremental || !T.notClear || n) && p.clear(!1, w, _);
      }
      m === -1 && (console.error("For some unknown reason. drawIndex is -1"), m = p.__startIndex);
      var C, M = function(L) {
        var I = {
          inHover: !1,
          allClipped: !1,
          prevEl: null,
          viewWidth: i._width,
          viewHeight: i._height
        };
        for (C = m; C < p.__endIndex; C++) {
          var R = t[C];
          if (R.__inHover && (h = !0), i._doPaintEl(R, p, o, L, I, C === p.__endIndex - 1), S) {
            var O = Date.now() - b;
            if (O > 15)
              break;
          }
        }
        I.prevElClipPaths && g.restore();
      };
      if (_)
        if (_.length === 0)
          C = p.__endIndex;
        else
          for (var D = c.dpr, P = 0; P < _.length; ++P) {
            var A = _[P];
            g.save(), g.beginPath(), g.rect(A.x * D, A.y * D, A.width * D, A.height * D), g.clip(), M(A), g.restore();
          }
      else
        g.save(), M(), g.restore();
      p.__drawIndex = C, p.__drawIndex < p.__endIndex && (f = !1);
    }, c = this, d = 0; d < a.length; d++)
      v(d);
    return z.wxa && x(this._layers, function(y) {
      y && y.ctx && y.ctx.draw && y.ctx.draw();
    }), {
      finished: f,
      needsRefreshHover: h
    };
  }, e.prototype._doPaintEl = function(t, r, n, i, a, o) {
    var s = r.ctx;
    if (n) {
      var u = t.getPaintRect();
      (!i || u && u.intersect(i)) && (Wr(s, t, a, o), t.setPrevPaintRect(u));
    } else
      Wr(s, t, a, o);
  }, e.prototype.getLayer = function(t, r) {
    this._singleCanvas && !this._needsManuallyCompositing && (t = Nr);
    var n = this._layers[t];
    return n || (n = new Du("zr_" + t, this, this.dpr), n.zlevel = t, n.__builtin__ = !0, this._layerConfig[t] ? it(n, this._layerConfig[t], !0) : this._layerConfig[t - Ga] && it(n, this._layerConfig[t - Ga], !0), r && (n.virtual = r), this.insertLayer(t, n), n.initContext()), n;
  }, e.prototype.insertLayer = function(t, r) {
    var n = this._layers, i = this._zlevelList, a = i.length, o = this._domRoot, s = null, u = -1;
    if (!n[t] && $D(r)) {
      if (a > 0 && t > i[0]) {
        for (u = 0; u < a - 1 && !(i[u] < t && i[u + 1] > t); u++)
          ;
        s = n[i[u]];
      }
      if (i.splice(u + 1, 0, t), n[t] = r, !r.virtual)
        if (s) {
          var l = s.dom;
          l.nextSibling ? o.insertBefore(r.dom, l.nextSibling) : o.appendChild(r.dom);
        } else
          o.firstChild ? o.insertBefore(r.dom, o.firstChild) : o.appendChild(r.dom);
      r.painter || (r.painter = this);
    }
  }, e.prototype.eachLayer = function(t, r) {
    for (var n = this._zlevelList, i = 0; i < n.length; i++) {
      var a = n[i];
      t.call(r, this._layers[a], a);
    }
  }, e.prototype.eachBuiltinLayer = function(t, r) {
    for (var n = this._zlevelList, i = 0; i < n.length; i++) {
      var a = n[i], o = this._layers[a];
      o.__builtin__ && t.call(r, o, a);
    }
  }, e.prototype.eachOtherLayer = function(t, r) {
    for (var n = this._zlevelList, i = 0; i < n.length; i++) {
      var a = n[i], o = this._layers[a];
      o.__builtin__ || t.call(r, o, a);
    }
  }, e.prototype.getLayers = function() {
    return this._layers;
  }, e.prototype._updateLayerStatus = function(t) {
    this.eachBuiltinLayer(function(h, v) {
      h.__dirty = h.__used = !1;
    });
    function r(h) {
      a && (a.__endIndex !== h && (a.__dirty = !0), a.__endIndex = h);
    }
    if (this._singleCanvas)
      for (var n = 1; n < t.length; n++) {
        var i = t[n];
        if (i.zlevel !== t[n - 1].zlevel || i.incremental) {
          this._needsManuallyCompositing = !0;
          break;
        }
      }
    var a = null, o = 0, s, u;
    for (u = 0; u < t.length; u++) {
      var i = t[u], l = i.zlevel, f = void 0;
      s !== l && (s = l, o = 0), i.incremental ? (f = this.getLayer(l + XD, this._needsManuallyCompositing), f.incremental = !0, o = 1) : f = this.getLayer(l + (o > 0 ? Ga : 0), this._needsManuallyCompositing), f.__builtin__ || Ro("ZLevel " + l + " has been used by unkown layer " + f.id), f !== a && (f.__used = !0, f.__startIndex !== u && (f.__dirty = !0), f.__startIndex = u, f.incremental ? f.__drawIndex = -1 : f.__drawIndex = u, r(u), a = f), i.__dirty & Ut && !i.__inHover && (f.__dirty = !0, f.incremental && f.__drawIndex < 0 && (f.__drawIndex = u));
    }
    r(u), this.eachBuiltinLayer(function(h, v) {
      !h.__used && h.getElementCount() > 0 && (h.__dirty = !0, h.__startIndex = h.__endIndex = h.__drawIndex = 0), h.__dirty && h.__drawIndex < 0 && (h.__drawIndex = h.__startIndex);
    });
  }, e.prototype.clear = function() {
    return this.eachBuiltinLayer(this._clearLayer), this;
  }, e.prototype._clearLayer = function(t) {
    t.clear();
  }, e.prototype.setBackgroundColor = function(t) {
    this._backgroundColor = t, x(this._layers, function(r) {
      r.setUnpainted();
    });
  }, e.prototype.configLayer = function(t, r) {
    if (r) {
      var n = this._layerConfig;
      n[t] ? it(n[t], r, !0) : n[t] = r;
      for (var i = 0; i < this._zlevelList.length; i++) {
        var a = this._zlevelList[i];
        if (a === t || a === t + Ga) {
          var o = this._layers[a];
          it(o, n[t], !0);
        }
      }
    }
  }, e.prototype.delLayer = function(t) {
    var r = this._layers, n = this._zlevelList, i = r[t];
    i && (i.dom.parentNode.removeChild(i.dom), delete r[t], n.splice(rt(n, t), 1));
  }, e.prototype.resize = function(t, r) {
    if (this._domRoot.style) {
      var n = this._domRoot;
      n.style.display = "none";
      var i = this._opts, a = this.root;
      if (t != null && (i.width = t), r != null && (i.height = r), t = Ra(a, 0, i), r = Ra(a, 1, i), n.style.display = "", this._width !== t || r !== this._height) {
        n.style.width = t + "px", n.style.height = r + "px";
        for (var o in this._layers)
          this._layers.hasOwnProperty(o) && this._layers[o].resize(t, r);
        this.refresh(!0);
      }
      this._width = t, this._height = r;
    } else {
      if (t == null || r == null)
        return;
      this._width = t, this._height = r, this.getLayer(Nr).resize(t, r);
    }
    return this;
  }, e.prototype.clearLayer = function(t) {
    var r = this._layers[t];
    r && r.clear();
  }, e.prototype.dispose = function() {
    this.root.innerHTML = "", this.root = this.storage = this._domRoot = this._layers = null;
  }, e.prototype.getRenderedCanvas = function(t) {
    if (t = t || {}, this._singleCanvas && !this._compositeManually)
      return this._layers[Nr].dom;
    var r = new Du("image", this, t.pixelRatio || this.dpr);
    r.initContext(), r.clear(!1, t.backgroundColor || this._backgroundColor);
    var n = r.ctx;
    if (t.pixelRatio <= this.dpr) {
      this.refresh();
      var i = r.dom.width, a = r.dom.height;
      this.eachLayer(function(h) {
        h.__builtin__ ? n.drawImage(h.dom, 0, 0, i, a) : h.renderToCanvas && (n.save(), h.renderToCanvas(n), n.restore());
      });
    } else
      for (var o = {
        inHover: !1,
        viewWidth: this._width,
        viewHeight: this._height
      }, s = this.storage.getDisplayList(!0), u = 0, l = s.length; u < l; u++) {
        var f = s[u];
        Wr(n, f, o, u === l - 1);
      }
    return r.dom;
  }, e.prototype.getWidth = function() {
    return this._width;
  }, e.prototype.getHeight = function() {
    return this._height;
  }, e;
})();
function xP(e) {
  e.registerPainter("canvas", jD);
}
export {
  ia as $,
  Pf as A,
  pt as B,
  De as C,
  lP as D,
  fP as E,
  a_ as F,
  oe as G,
  i_ as H,
  Q as I,
  ar as J,
  ye as K,
  JC as L,
  ft as M,
  it as N,
  So as O,
  ht as P,
  bf as Q,
  Et as R,
  Wo as S,
  zi as T,
  Ev as U,
  ko as V,
  j as W,
  Yt as X,
  yP as Y,
  CM as Z,
  lt as _,
  Ht as a,
  bP as a$,
  $C as a0,
  uP as a1,
  mo as a2,
  cP as a3,
  N as a4,
  rt as a5,
  KC as a6,
  _P as a7,
  mP as a8,
  vP as a9,
  SP as aA,
  KM as aB,
  Oo as aC,
  oo as aD,
  ff as aE,
  sr as aF,
  ql as aG,
  $ as aH,
  Cg as aI,
  st as aJ,
  Bt as aK,
  Qr as aL,
  Uo as aM,
  ta as aN,
  dt as aO,
  zf as aP,
  Ot as aQ,
  aP as aR,
  We as aS,
  oP as aT,
  x0 as aU,
  Xb as aV,
  Re as aW,
  Rt as aX,
  QD as aY,
  gP as aZ,
  TP as a_,
  ct as aa,
  sP as ab,
  jC as ac,
  hP as ad,
  W as ae,
  gt as af,
  qv as ag,
  Zr as ah,
  Dw as ai,
  YM as aj,
  tn as ak,
  lf as al,
  $t as am,
  rC as an,
  lb as ao,
  Aw as ap,
  It as aq,
  o1 as ar,
  c1 as as,
  Gt as at,
  At as au,
  Rn as av,
  gD as aw,
  XM as ax,
  $M as ay,
  qM as az,
  B as b,
  wP as b0,
  xP as b1,
  nc as b2,
  Y as b3,
  qo as b4,
  hr as b5,
  et as b6,
  op as b7,
  F0 as b8,
  Ln as b9,
  Zu as bA,
  zu as bB,
  un as bC,
  os as bD,
  If as bE,
  Ep as bF,
  Fl as bG,
  rM as bH,
  nM as bI,
  vm as ba,
  fm as bb,
  _a as bc,
  Yl as bd,
  LM as be,
  dP as bf,
  pP as bg,
  RM as bh,
  Ti as bi,
  iP as bj,
  Ul as bk,
  Zi as bl,
  qt as bm,
  so as bn,
  Xy as bo,
  $y as bp,
  kf as bq,
  vr as br,
  eM as bs,
  Sf as bt,
  Pt as bu,
  _f as bv,
  Wg as bw,
  eb as bx,
  Zx as by,
  fC as bz,
  U as c,
  SC as d,
  x as e,
  Ae as f,
  Uw as g,
  Pp as h,
  k as i,
  ut as j,
  of as k,
  hf as l,
  F as m,
  ww as n,
  at as o,
  Kr as p,
  E as q,
  wo as r,
  eP as s,
  Ag as t,
  On as u,
  rP as v,
  Lw as w,
  nP as x,
  JD as y,
  tP as z
};
