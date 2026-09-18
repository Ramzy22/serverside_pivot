import { b as xS, r as MS, g as DS } from "./charts-DthW1Q8W.js";
var Ha = {}, Xl = function(e, t) {
  return Xl = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, Xl(e, t);
};
function Dg(e, t) {
  if (typeof t != "function" && t !== null)
    throw new TypeError("Class extends value " + String(t) + " is not a constructor or null");
  Xl(e, t);
  function r() {
    this.constructor = e;
  }
  e.prototype = t === null ? Object.create(t) : (r.prototype = t.prototype, new r());
}
var jo = function() {
  return jo = Object.assign || function(t) {
    for (var r, n = 1, i = arguments.length; n < i; n++) {
      r = arguments[n];
      for (var a in r) Object.prototype.hasOwnProperty.call(r, a) && (t[a] = r[a]);
    }
    return t;
  }, jo.apply(this, arguments);
};
function Ig(e, t) {
  var r = {};
  for (var n in e) Object.prototype.hasOwnProperty.call(e, n) && t.indexOf(n) < 0 && (r[n] = e[n]);
  if (e != null && typeof Object.getOwnPropertySymbols == "function")
    for (var i = 0, n = Object.getOwnPropertySymbols(e); i < n.length; i++)
      t.indexOf(n[i]) < 0 && Object.prototype.propertyIsEnumerable.call(e, n[i]) && (r[n[i]] = e[n[i]]);
  return r;
}
function Ag(e, t, r, n) {
  var i = arguments.length, a = i < 3 ? t : n === null ? n = Object.getOwnPropertyDescriptor(t, r) : n, o;
  if (typeof Reflect == "object" && typeof Reflect.decorate == "function") a = Reflect.decorate(e, t, r, n);
  else for (var s = e.length - 1; s >= 0; s--) (o = e[s]) && (a = (i < 3 ? o(a) : i > 3 ? o(t, r, a) : o(t, r)) || a);
  return i > 3 && a && Object.defineProperty(t, r, a), a;
}
function Pg(e, t) {
  return function(r, n) {
    t(r, n, e);
  };
}
function Lg(e, t, r, n, i, a) {
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
function Eg(e, t, r) {
  for (var n = arguments.length > 2, i = 0; i < t.length; i++)
    r = n ? t[i].call(e, r) : t[i].call(e);
  return n ? r : void 0;
}
function Rg(e) {
  return typeof e == "symbol" ? e : "".concat(e);
}
function Og(e, t, r) {
  return typeof t == "symbol" && (t = t.description ? "[".concat(t.description, "]") : ""), Object.defineProperty(e, "name", { configurable: !0, value: r ? "".concat(r, " ", t) : t });
}
function kg(e, t) {
  if (typeof Reflect == "object" && typeof Reflect.metadata == "function") return Reflect.metadata(e, t);
}
function Bg(e, t, r, n) {
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
function Ng(e, t) {
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
var Ns = Object.create ? (function(e, t, r, n) {
  n === void 0 && (n = r);
  var i = Object.getOwnPropertyDescriptor(t, r);
  (!i || ("get" in i ? !t.__esModule : i.writable || i.configurable)) && (i = { enumerable: !0, get: function() {
    return t[r];
  } }), Object.defineProperty(e, n, i);
}) : (function(e, t, r, n) {
  n === void 0 && (n = r), e[n] = t[r];
});
function Fg(e, t) {
  for (var r in e) r !== "default" && !Object.prototype.hasOwnProperty.call(t, r) && Ns(t, e, r);
}
function Qo(e) {
  var t = typeof Symbol == "function" && Symbol.iterator, r = t && e[t], n = 0;
  if (r) return r.call(e);
  if (e && typeof e.length == "number") return {
    next: function() {
      return e && n >= e.length && (e = void 0), { value: e && e[n++], done: !e };
    }
  };
  throw new TypeError(t ? "Object is not iterable." : "Symbol.iterator is not defined.");
}
function uh(e, t) {
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
function zg() {
  for (var e = [], t = 0; t < arguments.length; t++)
    e = e.concat(uh(arguments[t]));
  return e;
}
function Hg() {
  for (var e = 0, t = 0, r = arguments.length; t < r; t++) e += arguments[t].length;
  for (var n = Array(e), i = 0, t = 0; t < r; t++)
    for (var a = arguments[t], o = 0, s = a.length; o < s; o++, i++)
      n[i] = a[o];
  return n;
}
function Vg(e, t, r) {
  if (r || arguments.length === 2) for (var n = 0, i = t.length, a; n < i; n++)
    (a || !(n in t)) && (a || (a = Array.prototype.slice.call(t, 0, n)), a[n] = t[n]);
  return e.concat(a || Array.prototype.slice.call(t));
}
function ai(e) {
  return this instanceof ai ? (this.v = e, this) : new ai(e);
}
function Gg(e, t, r) {
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
    c.value instanceof ai ? Promise.resolve(c.value.v).then(f, h) : v(a[0][2], c);
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
function Ug(e) {
  var t, r;
  return t = {}, n("next"), n("throw", function(i) {
    throw i;
  }), n("return"), t[Symbol.iterator] = function() {
    return this;
  }, t;
  function n(i, a) {
    t[i] = e[i] ? function(o) {
      return (r = !r) ? { value: ai(e[i](o)), done: !1 } : a ? a(o) : o;
    } : a;
  }
}
function Yg(e) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var t = e[Symbol.asyncIterator], r;
  return t ? t.call(e) : (e = typeof Qo == "function" ? Qo(e) : e[Symbol.iterator](), r = {}, n("next"), n("throw"), n("return"), r[Symbol.asyncIterator] = function() {
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
function Wg(e, t) {
  return Object.defineProperty ? Object.defineProperty(e, "raw", { value: t }) : e.raw = t, e;
}
var IS = Object.create ? (function(e, t) {
  Object.defineProperty(e, "default", { enumerable: !0, value: t });
}) : function(e, t) {
  e.default = t;
}, ql = function(e) {
  return ql = Object.getOwnPropertyNames || function(t) {
    var r = [];
    for (var n in t) Object.prototype.hasOwnProperty.call(t, n) && (r[r.length] = n);
    return r;
  }, ql(e);
};
function Xg(e) {
  if (e && e.__esModule) return e;
  var t = {};
  if (e != null) for (var r = ql(e), n = 0; n < r.length; n++) r[n] !== "default" && Ns(t, e, r[n]);
  return IS(t, e), t;
}
function qg(e) {
  return e && e.__esModule ? e : { default: e };
}
function $g(e, t, r, n) {
  if (r === "a" && !n) throw new TypeError("Private accessor was defined without a getter");
  if (typeof t == "function" ? e !== t || !n : !t.has(e)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
  return r === "m" ? n : r === "a" ? n.call(e) : n ? n.value : t.get(e);
}
function Zg(e, t, r, n, i) {
  if (n === "m") throw new TypeError("Private method is not writable");
  if (n === "a" && !i) throw new TypeError("Private accessor was defined without a setter");
  if (typeof t == "function" ? e !== t || !i : !t.has(e)) throw new TypeError("Cannot write private member to an object whose class did not declare it");
  return n === "a" ? i.call(e, r) : i ? i.value = r : t.set(e, r), r;
}
function Kg(e, t) {
  if (t === null || typeof t != "object" && typeof t != "function") throw new TypeError("Cannot use 'in' operator on non-object");
  return typeof e == "function" ? t === e : e.has(t);
}
function jg(e, t, r) {
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
var AS = typeof SuppressedError == "function" ? SuppressedError : function(e, t, r) {
  var n = new Error(r);
  return n.name = "SuppressedError", n.error = e, n.suppressed = t, n;
};
function Qg(e) {
  function t(a) {
    e.error = e.hasError ? new AS(a, e.error, "An error was suppressed during disposal.") : a, e.hasError = !0;
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
function Jg(e, t) {
  return typeof e == "string" && /^\.\.?\//.test(e) ? e.replace(/\.(tsx)$|((?:\.d)?)((?:\.[^./]+?)?)\.([cm]?)ts$/i, function(r, n, i, a, o) {
    return n ? t ? ".jsx" : ".js" : i && (!a || !o) ? r : i + a + "." + o.toLowerCase() + "js";
  }) : e;
}
const PS = {
  __extends: Dg,
  __assign: jo,
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
  __createBinding: Ns,
  __exportStar: Fg,
  __values: Qo,
  __read: uh,
  __spread: zg,
  __spreadArrays: Hg,
  __spreadArray: Vg,
  __await: ai,
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
}, LS = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  __addDisposableResource: jg,
  get __assign() {
    return jo;
  },
  __asyncDelegator: Ug,
  __asyncGenerator: Gg,
  __asyncValues: Yg,
  __await: ai,
  __awaiter: Bg,
  __classPrivateFieldGet: $g,
  __classPrivateFieldIn: Kg,
  __classPrivateFieldSet: Zg,
  __createBinding: Ns,
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
  __read: uh,
  __rest: Ig,
  __rewriteRelativeImportExtension: Jg,
  __runInitializers: Eg,
  __setFunctionName: Og,
  __spread: zg,
  __spreadArray: Vg,
  __spreadArrays: Hg,
  __values: Qo,
  default: PS
}, Symbol.toStringTag, { value: "Module" })), ty = /* @__PURE__ */ xS(LS);
var Ke = {}, je = {}, wu = {}, Hc;
function ES() {
  return Hc || (Hc = 1, (function(e) {
    Object.defineProperty(e, "__esModule", {
      value: !0
    }), e.default = void 0;
    var t = 1;
    e.default = function() {
      return "".concat(t++);
    };
  })(wu)), wu;
}
var mi = {}, _i = {}, Tu = {}, Vc;
function ey() {
  return Vc || (Vc = 1, (function(e) {
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
  })(Tu)), Tu;
}
var Qe = {}, Gc;
function lh() {
  return Gc || (Gc = 1, Object.defineProperty(Qe, "__esModule", {
    value: !0
  }), Qe.SizeSensorId = Qe.SensorTabIndex = Qe.SensorClassName = void 0, Qe.SizeSensorId = "size-sensor-id", Qe.SensorClassName = "size-sensor-object", Qe.SensorTabIndex = "-1"), Qe;
}
var Uc;
function RS() {
  if (Uc) return _i;
  Uc = 1, Object.defineProperty(_i, "__esModule", {
    value: !0
  }), _i.createSensor = void 0;
  var e = r(ey()), t = lh();
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return _i.createSensor = function(i, a) {
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
  }, _i;
}
var Si = {}, Yc;
function OS() {
  if (Yc) return Si;
  Yc = 1, Object.defineProperty(Si, "__esModule", {
    value: !0
  }), Si.createSensor = void 0;
  var e = lh(), t = r(ey());
  function r(n) {
    return n && n.__esModule ? n : { default: n };
  }
  return Si.createSensor = function(i, a) {
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
  }, Si;
}
var Wc;
function kS() {
  if (Wc) return mi;
  Wc = 1, Object.defineProperty(mi, "__esModule", {
    value: !0
  }), mi.createSensor = void 0;
  var e = RS(), t = OS();
  return mi.createSensor = typeof ResizeObserver < "u" ? t.createSensor : e.createSensor, mi;
}
var Xc;
function BS() {
  if (Xc) return je;
  Xc = 1, Object.defineProperty(je, "__esModule", {
    value: !0
  }), je.removeSensor = je.getSensor = je.Sensors = void 0;
  var e = n(ES()), t = kS(), r = lh();
  function n(o) {
    return o && o.__esModule ? o : { default: o };
  }
  var i = je.Sensors = {};
  function a(o) {
    o && i[o] && delete i[o];
  }
  return je.getSensor = function(s) {
    var u = s.getAttribute(r.SizeSensorId);
    if (u && i[u])
      return i[u];
    var l = (0, e.default)();
    s.setAttribute(r.SizeSensorId, l);
    var f = (0, t.createSensor)(s, function() {
      return a(l);
    });
    return i[l] = f, f;
  }, je.removeSensor = function(s) {
    var u = s.element.getAttribute(r.SizeSensorId);
    s.destroy(), a(u);
  }, je;
}
var qc;
function NS() {
  if (qc) return Ke;
  qc = 1, Object.defineProperty(Ke, "__esModule", {
    value: !0
  }), Ke.ver = Ke.clear = Ke.bind = void 0;
  var e = BS();
  return Ke.bind = function(r, n) {
    var i = (0, e.getSensor)(r);
    return i.bind(n), function() {
      i.unbind(n);
    };
  }, Ke.clear = function(r) {
    var n = (0, e.getSensor)(r);
    (0, e.removeSensor)(n);
  }, Ke.ver = "1.0.3", Ke;
}
var bi = {}, $c;
function FS() {
  if ($c) return bi;
  $c = 1, Object.defineProperty(bi, "__esModule", { value: !0 }), bi.pick = void 0;
  function e(t, r) {
    var n = {};
    return r.forEach(function(i) {
      n[i] = t[i];
    }), n;
  }
  return bi.pick = e, bi;
}
var wi = {}, Zc;
function zS() {
  if (Zc) return wi;
  Zc = 1, Object.defineProperty(wi, "__esModule", { value: !0 }), wi.isFunction = void 0;
  function e(t) {
    return typeof t == "function";
  }
  return wi.isFunction = e, wi;
}
var Ti = {}, Kc;
function HS() {
  if (Kc) return Ti;
  Kc = 1, Object.defineProperty(Ti, "__esModule", { value: !0 }), Ti.isString = void 0;
  function e(t) {
    return typeof t == "string";
  }
  return Ti.isString = e, Ti;
}
var Ci = {}, Cu, jc;
function VS() {
  return jc || (jc = 1, Cu = function e(t, r) {
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
  }), Cu;
}
var Qc;
function GS() {
  if (Qc) return Ci;
  Qc = 1, Object.defineProperty(Ci, "__esModule", { value: !0 }), Ci.isEqual = void 0;
  var e = ty, t = e.__importDefault(VS());
  return Ci.isEqual = t.default, Ci;
}
var Jc;
function US() {
  if (Jc) return Ha;
  Jc = 1, Object.defineProperty(Ha, "__esModule", { value: !0 });
  var e = ty, t = e.__importStar(MS()), r = NS(), n = FS(), i = zS(), a = HS(), o = GS(), s = (
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
        var y = e.__rest(h, ["style", "className", "echarts", "option", "theme", "notMerge", "replaceMerge", "lazyUpdate", "showLoading", "loadingOption", "opts", "onChartReady", "onEvents", "shouldSetOption", "autoResize"]), p = e.__assign({ height: 300 }, v);
        return t.default.createElement("div", e.__assign({ ref: function(g) {
          f.ele = g;
        }, style: p, className: "echarts-for-react ".concat(d) }, y));
      }, l;
    })(t.PureComponent)
  );
  return Ha.default = s, Ha;
}
var YS = US();
const OE = /* @__PURE__ */ DS(YS);
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
var $l = function(e, t) {
  return $l = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(r, n) {
    r.__proto__ = n;
  } || function(r, n) {
    for (var i in n) Object.prototype.hasOwnProperty.call(n, i) && (r[i] = n[i]);
  }, $l(e, t);
};
function z(e, t) {
  if (typeof t != "function" && t !== null)
    throw new TypeError("Class extends value " + String(t) + " is not a constructor or null");
  $l(e, t);
  function r() {
    this.constructor = e;
  }
  e.prototype = t === null ? Object.create(t) : (r.prototype = t.prototype, new r());
}
var WS = /* @__PURE__ */ (function() {
  function e() {
    this.firefox = !1, this.ie = !1, this.edge = !1, this.newEdge = !1, this.weChat = !1;
  }
  return e;
})(), XS = /* @__PURE__ */ (function() {
  function e() {
    this.browser = new WS(), this.node = !1, this.wxa = !1, this.worker = !1, this.svgSupported = !1, this.touchEventsSupported = !1, this.pointerEventsSupported = !1, this.domSupported = !1, this.transformSupported = !1, this.transform3dSupported = !1, this.hasGlobalWindow = typeof window < "u";
  }
  return e;
})(), W = new XS();
typeof wx == "object" && typeof wx.getSystemInfoSync == "function" ? (W.wxa = !0, W.touchEventsSupported = !0) : typeof document > "u" && typeof self < "u" ? W.worker = !0 : !W.hasGlobalWindow || "Deno" in window || typeof navigator < "u" && typeof navigator.userAgent == "string" && navigator.userAgent.indexOf("Node.js") > -1 ? (W.node = !0, W.svgSupported = !0) : qS(navigator.userAgent, W);
function qS(e, t) {
  var r = t.browser, n = e.match(/Firefox\/([\d.]+)/), i = e.match(/MSIE\s([\d.]+)/) || e.match(/Trident\/.+?rv:(([\d.]+))/), a = e.match(/Edge?\/([\d.]+)/), o = /micromessenger/i.test(e);
  n && (r.firefox = !0, r.version = n[1]), i && (r.ie = !0, r.version = i[1]), a && (r.edge = !0, r.version = a[1], r.newEdge = +a[1].split(".")[0] > 18), o && (r.weChat = !0), t.svgSupported = typeof SVGRect < "u", t.touchEventsSupported = "ontouchstart" in window && !r.ie && !r.edge, t.pointerEventsSupported = "onpointerdown" in window && (r.edge || r.ie && +r.version >= 11);
  var s = t.domSupported = typeof document < "u";
  if (s) {
    var u = document.documentElement.style;
    t.transform3dSupported = (r.ie && "transition" in u || r.edge || "WebKitCSSMatrix" in window && "m11" in new WebKitCSSMatrix() || "MozPerspective" in u) && !("OTransition" in u), t.transformSupported = t.transform3dSupported || r.ie && +r.version >= 9;
  }
}
var fh = 12, $S = "sans-serif", Pr = fh + "px " + $S, ZS = 20, KS = 100, jS = "007LLmW'55;N0500LLLLLLLLLL00NNNLzWW\\\\WQb\\0FWLg\\bWb\\WQ\\WrWWQ000CL5LLFLL0LL**F*gLLLL5F0LF\\FFF5.5N";
function QS(e) {
  var t = {};
  if (typeof JSON > "u")
    return t;
  for (var r = 0; r < e.length; r++) {
    var n = String.fromCharCode(r + 32), i = (e.charCodeAt(r) - ZS) / KS;
    t[n] = i;
  }
  return t;
}
var JS = QS(jS), Lt = {
  createCanvas: function() {
    return typeof document < "u" && document.createElement("canvas");
  },
  measureText: /* @__PURE__ */ (function() {
    var e, t;
    return function(r, n) {
      if (!e) {
        var i = Lt.createCanvas();
        e = i && i.getContext("2d");
      }
      if (e)
        return t !== n && (t = e.font = n || Pr), e.measureText(r);
      r = r || "", n = n || Pr;
      var a = /((?:\d+)?\.?\d*)px/.exec(n), o = a && +a[1] || fh, s = 0;
      if (n.indexOf("mono") >= 0)
        s = o * r.length;
      else
        for (var u = 0; u < r.length; u++) {
          var l = JS[r[u]];
          s += l == null ? o : l * o;
        }
      return { width: s };
    };
  })(),
  loadImage: function(e, t, r) {
    var n = new Image();
    return n.onload = t, n.onerror = r, n.src = e, n;
  },
  getTime: function() {
    return Date.now ? Date.now() : +/* @__PURE__ */ new Date();
  }
};
function ry(e) {
  for (var t in Lt)
    Lt.hasOwnProperty(t) && e[t] && (Lt[t] = e[t]);
}
var ny = Ye([
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
}, {}), iy = Ye([
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
}, {}), vi = Object.prototype.toString, Fs = Array.prototype, t1 = Fs.forEach, e1 = Fs.filter, hh = Fs.slice, r1 = Fs.map, tv = (function() {
}).constructor, Va = tv ? tv.prototype : null, ch = "__proto__", xu = 2311, n1 = Math.pow(2, 53) - 1;
function vh() {
  return xu >= n1 && (xu = 0), xu++;
}
function zs() {
  for (var e = [], t = 0; t < arguments.length; t++)
    e[t] = arguments[t];
  typeof console < "u" && console.error.apply(console, e);
}
function Z(e) {
  if (e == null || typeof e != "object")
    return e;
  var t = e, r = vi.call(e);
  if (r === "[object Array]") {
    if (!Jn(e)) {
      t = [];
      for (var n = 0, i = e.length; n < i; n++)
        t[n] = Z(e[n]);
    }
  } else if (iy[r]) {
    if (!Jn(e)) {
      var a = e.constructor;
      if (a.from)
        t = a.from(e);
      else {
        t = new a(e.length);
        for (var n = 0, i = e.length; n < i; n++)
          t[n] = e[n];
      }
    }
  } else if (!ny[r] && !Jn(e) && !oi(e)) {
    t = {};
    for (var o in e)
      e.hasOwnProperty(o) && o !== ch && (t[o] = Z(e[o]));
  }
  return t;
}
function pt(e, t, r) {
  if (!F(t) || !F(e))
    return r ? Z(t) : e;
  for (var n in t)
    if (t.hasOwnProperty(n) && n !== ch) {
      var i = e[n], a = t[n];
      F(a) && F(i) && !B(a) && !B(i) && !oi(a) && !oi(i) && !Zl(a) && !Zl(i) && !Jn(a) && !Jn(i) ? pt(i, a, r) : (r || !(n in e)) && (e[n] = Z(t[n]));
    }
  return e;
}
function i1(e, t) {
  for (var r = e[0], n = 1, i = e.length; n < i; n++)
    r = pt(r, e[n], t);
  return r;
}
function R(e, t) {
  if (Object.assign)
    Object.assign(e, t);
  else
    for (var r in t)
      t.hasOwnProperty(r) && r !== ch && (e[r] = t[r]);
  return e;
}
function ay(e, t, r) {
  e = e || {};
  for (var n = 0; n < r.length; n++) {
    var i = r[n];
    e[i] = t[i];
  }
  return e;
}
function _t(e, t, r) {
  for (var n = mt(t), i = 0, a = n.length; i < a; i++) {
    var o = n[i];
    (r ? t[o] != null : e[o] == null) && (e[o] = t[o]);
  }
  return e;
}
var a1 = Lt.createCanvas;
function st(e, t) {
  if (e) {
    if (e.indexOf)
      return e.indexOf(t);
    for (var r = 0, n = e.length; r < n; r++)
      if (e[r] === t)
        return r;
  }
  return -1;
}
function dh(e, t) {
  var r = e.prototype;
  function n() {
  }
  n.prototype = t.prototype, e.prototype = new n();
  for (var i in r)
    r.hasOwnProperty(i) && (e.prototype[i] = r[i]);
  e.prototype.constructor = e, e.superClass = t;
}
function Ae(e, t, r) {
  if (e = "prototype" in e ? e.prototype : e, t = "prototype" in t ? t.prototype : t, Object.getOwnPropertyNames)
    for (var n = Object.getOwnPropertyNames(t), i = 0; i < n.length; i++) {
      var a = n[i];
      a !== "constructor" && (r ? t[a] != null : e[a] == null) && (e[a] = t[a]);
    }
  else
    _t(e, t, r);
}
function $t(e) {
  return !e || typeof e == "string" ? !1 : typeof e.length == "number";
}
function T(e, t, r) {
  if (e && t)
    if (e.forEach && e.forEach === t1)
      e.forEach(t, r);
    else if (e.length === +e.length)
      for (var n = 0, i = e.length; n < i; n++)
        t.call(r, e[n], n, e);
    else
      for (var a in e)
        e.hasOwnProperty(a) && t.call(r, e[a], a, e);
}
function H(e, t, r) {
  if (!e)
    return [];
  if (!t)
    return Hs(e);
  if (e.map && e.map === r1)
    return e.map(t, r);
  for (var n = [], i = 0, a = e.length; i < a; i++)
    n.push(t.call(r, e[i], i, e));
  return n;
}
function Ye(e, t, r, n) {
  if (e && t) {
    for (var i = 0, a = e.length; i < a; i++)
      r = t.call(n, r, e[i], i, e);
    return r;
  }
}
function xt(e, t, r) {
  if (!e)
    return [];
  if (!t)
    return Hs(e);
  if (e.filter && e.filter === e1)
    return e.filter(t, r);
  for (var n = [], i = 0, a = e.length; i < a; i++)
    t.call(r, e[i], i, e) && n.push(e[i]);
  return n;
}
function o1(e, t, r) {
  if (e && t) {
    for (var n = 0, i = e.length; n < i; n++)
      if (t.call(r, e[n], n, e))
        return e[n];
  }
}
function mt(e) {
  if (!e)
    return [];
  if (Object.keys)
    return Object.keys(e);
  var t = [];
  for (var r in e)
    e.hasOwnProperty(r) && t.push(r);
  return t;
}
function s1(e, t) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return function() {
    return e.apply(t, r.concat(hh.call(arguments)));
  };
}
var ht = Va && X(Va.bind) ? Va.call.bind(Va.bind) : s1;
function Tt(e) {
  for (var t = [], r = 1; r < arguments.length; r++)
    t[r - 1] = arguments[r];
  return function() {
    return e.apply(this, t.concat(hh.call(arguments)));
  };
}
function B(e) {
  return Array.isArray ? Array.isArray(e) : vi.call(e) === "[object Array]";
}
function X(e) {
  return typeof e == "function";
}
function N(e) {
  return typeof e == "string";
}
function Jo(e) {
  return vi.call(e) === "[object String]";
}
function vt(e) {
  return typeof e == "number";
}
function F(e) {
  var t = typeof e;
  return t === "function" || !!e && t === "object";
}
function Zl(e) {
  return !!ny[vi.call(e)];
}
function Zt(e) {
  return !!iy[vi.call(e)];
}
function oi(e) {
  return typeof e == "object" && typeof e.nodeType == "number" && typeof e.ownerDocument == "object";
}
function Da(e) {
  return e.colorStops != null;
}
function oy(e) {
  return e.image != null;
}
function u1(e) {
  return vi.call(e) === "[object RegExp]";
}
function ua(e) {
  return e !== e;
}
function la() {
  for (var e = [], t = 0; t < arguments.length; t++)
    e[t] = arguments[t];
  for (var r = 0, n = e.length; r < n; r++)
    if (e[r] != null)
      return e[r];
}
function G(e, t) {
  return e ?? t;
}
function dn(e, t, r) {
  return e ?? t ?? r;
}
function Hs(e) {
  for (var t = [], r = 1; r < arguments.length; r++)
    t[r - 1] = arguments[r];
  return hh.apply(e, t);
}
function Vs(e) {
  if (typeof e == "number")
    return [e, e, e, e];
  var t = e.length;
  return t === 2 ? [e[0], e[1], e[0], e[1]] : t === 3 ? [e[0], e[1], e[2], e[1]] : e;
}
function We(e, t) {
  if (!e)
    throw new Error(t);
}
function Me(e) {
  return e == null ? null : typeof e.trim == "function" ? e.trim() : e.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, "");
}
var sy = "__ec_primitive__";
function fa(e) {
  e[sy] = !0;
}
function Jn(e) {
  return e[sy];
}
var l1 = (function() {
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
    return mt(this.data);
  }, e.prototype.forEach = function(t) {
    var r = this.data;
    for (var n in r)
      r.hasOwnProperty(n) && t(r[n], n);
  }, e;
})(), uy = typeof Map == "function";
function f1() {
  return uy ? /* @__PURE__ */ new Map() : new l1();
}
var ly = (function() {
  function e(t) {
    var r = B(t);
    this.data = f1();
    var n = this;
    t instanceof e ? t.each(i) : t && T(t, i);
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
    return uy ? Array.from(t) : t;
  }, e.prototype.removeKey = function(t) {
    this.data.delete(t);
  }, e;
})();
function U(e) {
  return new ly(e);
}
function fy(e, t) {
  for (var r = new e.constructor(e.length + t.length), n = 0; n < e.length; n++)
    r[n] = e[n];
  for (var i = e.length, n = 0; n < t.length; n++)
    r[n + i] = t[n];
  return r;
}
function Ia(e, t) {
  var r;
  if (Object.create)
    r = Object.create(e);
  else {
    var n = function() {
    };
    n.prototype = e, r = new n();
  }
  return t && R(r, t), r;
}
function ph(e) {
  var t = e.style;
  t.webkitUserSelect = "none", t.userSelect = "none", t.webkitTapHighlightColor = "rgba(0,0,0,0)", t["-webkit-touch-callout"] = "none";
}
function Xt(e, t) {
  return e.hasOwnProperty(t);
}
function Et() {
}
var hy = 180 / Math.PI, h1 = Number.EPSILON || Math.pow(2, -52);
const c1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  EPSILON: h1,
  HashMap: ly,
  RADIAN_TO_DEGREE: hy,
  assert: We,
  assignProps: ay,
  bind: ht,
  clone: Z,
  concatArray: fy,
  createCanvas: a1,
  createHashMap: U,
  createObject: Ia,
  curry: Tt,
  defaults: _t,
  disableUserSelect: ph,
  each: T,
  eqNaN: ua,
  extend: R,
  filter: xt,
  find: o1,
  guid: vh,
  hasOwn: Xt,
  indexOf: st,
  inherits: dh,
  isArray: B,
  isArrayLike: $t,
  isBuiltInObject: Zl,
  isDom: oi,
  isFunction: X,
  isGradientObject: Da,
  isImagePatternObject: oy,
  isNumber: vt,
  isObject: F,
  isPrimitive: Jn,
  isRegExp: u1,
  isString: N,
  isStringSafe: Jo,
  isTypedArray: Zt,
  keys: mt,
  logError: zs,
  map: H,
  merge: pt,
  mergeAll: i1,
  mixin: Ae,
  noop: Et,
  normalizeCssArray: Vs,
  reduce: Ye,
  retrieve: la,
  retrieve2: G,
  retrieve3: dn,
  setAsPrimitive: fa,
  slice: Hs,
  trim: Me
}, Symbol.toStringTag, { value: "Module" }));
function Cn(e, t) {
  return e == null && (e = 0), t == null && (t = 0), [e, t];
}
function v1(e, t) {
  return e[0] = t[0], e[1] = t[1], e;
}
function cy(e) {
  return [e[0], e[1]];
}
function Po(e, t, r) {
  return e[0] = t, e[1] = r, e;
}
function Kl(e, t, r) {
  return e[0] = t[0] + r[0], e[1] = t[1] + r[1], e;
}
function d1(e, t, r, n) {
  return e[0] = t[0] + r[0] * n, e[1] = t[1] + r[1] * n, e;
}
function vy(e, t, r) {
  return e[0] = t[0] - r[0], e[1] = t[1] - r[1], e;
}
function gh(e) {
  return Math.sqrt(yh(e));
}
var p1 = gh;
function yh(e) {
  return e[0] * e[0] + e[1] * e[1];
}
var g1 = yh;
function y1(e, t, r) {
  return e[0] = t[0] * r[0], e[1] = t[1] * r[1], e;
}
function m1(e, t, r) {
  return e[0] = t[0] / r[0], e[1] = t[1] / r[1], e;
}
function _1(e, t) {
  return e[0] * t[0] + e[1] * t[1];
}
function Lo(e, t, r) {
  return e[0] = t[0] * r, e[1] = t[1] * r, e;
}
function dy(e, t) {
  var r = gh(t);
  return r === 0 ? (e[0] = 0, e[1] = 0) : (e[0] = t[0] / r, e[1] = t[1] / r), e;
}
function ts(e, t) {
  return Math.sqrt((e[0] - t[0]) * (e[0] - t[0]) + (e[1] - t[1]) * (e[1] - t[1]));
}
var py = ts;
function gy(e, t) {
  return (e[0] - t[0]) * (e[0] - t[0]) + (e[1] - t[1]) * (e[1] - t[1]);
}
var pn = gy;
function S1(e, t) {
  return e[0] = -t[0], e[1] = -t[1], e;
}
function b1(e, t, r, n) {
  return e[0] = t[0] + n * (r[0] - t[0]), e[1] = t[1] + n * (r[1] - t[1]), e;
}
function ie(e, t, r) {
  var n = t[0], i = t[1];
  return e[0] = r[0] * n + r[2] * i + r[4], e[1] = r[1] * n + r[3] * i + r[5], e;
}
function br(e, t, r) {
  return e[0] = Math.min(t[0], r[0]), e[1] = Math.min(t[1], r[1]), e;
}
function wr(e, t, r) {
  return e[0] = Math.max(t[0], r[0]), e[1] = Math.max(t[1], r[1]), e;
}
const w1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  add: Kl,
  applyTransform: ie,
  clone: cy,
  copy: v1,
  create: Cn,
  dist: py,
  distSquare: pn,
  distance: ts,
  distanceSquare: gy,
  div: m1,
  dot: _1,
  len: gh,
  lenSquare: yh,
  length: p1,
  lengthSquare: g1,
  lerp: b1,
  max: wr,
  min: br,
  mul: y1,
  negate: S1,
  normalize: dy,
  scale: Lo,
  scaleAndAdd: d1,
  set: Po,
  sub: vy
}, Symbol.toStringTag, { value: "Module" }));
var Ln = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.target = t, this.topTarget = r && r.topTarget;
  }
  return e;
})(), T1 = (function() {
  function e(t) {
    this.handler = t, t.on("mousedown", this._dragStart, this), t.on("mousemove", this._drag, this), t.on("mouseup", this._dragEnd, this);
  }
  return e.prototype._dragStart = function(t) {
    for (var r = t.target; r && !r.draggable; )
      r = r.parent || r.__hostTarget;
    r && (this._draggingTarget = r, r.dragging = !0, this._x = t.offsetX, this._y = t.offsetY, this.handler.dispatchToElement(new Ln(r, t), "dragstart", t.event));
  }, e.prototype._drag = function(t) {
    var r = this._draggingTarget;
    if (r) {
      var n = t.offsetX, i = t.offsetY, a = n - this._x, o = i - this._y;
      this._x = n, this._y = i, r.drift(a, o, t), this.handler.dispatchToElement(new Ln(r, t), "drag", t.event);
      var s = this.handler.findHover(n, i, r).target, u = this._dropTarget;
      this._dropTarget = s, r !== s && (u && s !== u && this.handler.dispatchToElement(new Ln(u, t), "dragleave", t.event), s && s !== u && this.handler.dispatchToElement(new Ln(s, t), "dragenter", t.event));
    }
  }, e.prototype._dragEnd = function(t) {
    var r = this._draggingTarget;
    r && (r.dragging = !1), this.handler.dispatchToElement(new Ln(r, t), "dragend", t.event), this._dropTarget && this.handler.dispatchToElement(new Ln(this._dropTarget, t), "drop", t.event), this._draggingTarget = null, this._dropTarget = null;
  }, e;
})(), qe = (function() {
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
})(), C1 = Math.log(2);
function jl(e, t, r, n, i, a) {
  var o = n + "-" + i, s = e.length;
  if (a.hasOwnProperty(o))
    return a[o];
  if (t === 1) {
    var u = Math.round(Math.log((1 << s) - 1 & ~i) / C1);
    return e[r][u];
  }
  for (var l = n | 1 << r, f = r + 1; n & 1 << f; )
    f++;
  for (var h = 0, v = 0, c = 0; v < s; v++) {
    var d = 1 << v;
    d & i || (h += (c % 2 ? -1 : 1) * e[r][v] * jl(e, t - 1, f, l, i | d, a), c++);
  }
  return a[o] = h, h;
}
function ev(e, t) {
  var r = [
    [e[0], e[1], 1, 0, 0, 0, -t[0] * e[0], -t[0] * e[1]],
    [0, 0, 0, e[0], e[1], 1, -t[1] * e[0], -t[1] * e[1]],
    [e[2], e[3], 1, 0, 0, 0, -t[2] * e[2], -t[2] * e[3]],
    [0, 0, 0, e[2], e[3], 1, -t[3] * e[2], -t[3] * e[3]],
    [e[4], e[5], 1, 0, 0, 0, -t[4] * e[4], -t[4] * e[5]],
    [0, 0, 0, e[4], e[5], 1, -t[5] * e[4], -t[5] * e[5]],
    [e[6], e[7], 1, 0, 0, 0, -t[6] * e[6], -t[6] * e[7]],
    [0, 0, 0, e[6], e[7], 1, -t[7] * e[6], -t[7] * e[7]]
  ], n = {}, i = jl(r, 8, 0, 0, 0, n);
  if (i !== 0) {
    for (var a = [], o = 0; o < 8; o++)
      for (var s = 0; s < 8; s++)
        a[s] == null && (a[s] = 0), a[s] += ((o + s) % 2 ? -1 : 1) * jl(r, 7, o === 0 ? 1 : 0, 1 << o, 1 << s, n) / i * t[o];
    return function(u, l, f) {
      var h = l * a[6] + f * a[7] + 1;
      u[0] = (l * a[0] + f * a[1] + a[2]) / h, u[1] = (l * a[3] + f * a[4] + a[5]) / h;
    };
  }
}
var es = "___zrEVENTSAVED", Mu = [];
function x1(e, t, r, n, i) {
  return Ql(Mu, t, n, i, !0) && Ql(e, r, Mu[0], Mu[1]);
}
function M1(e, t) {
  e && r(e), t && r(t);
  function r(n) {
    var i = n[es];
    i && (i.clearMarkers && i.clearMarkers(), delete n[es]);
  }
}
function Ql(e, t, r, n, i) {
  if (t.getBoundingClientRect && W.domSupported && !yy(t)) {
    var a = t[es] || (t[es] = {}), o = D1(t, a), s = I1(o, a, i);
    if (s)
      return s(e, r, n), !0;
  }
  return !1;
}
function D1(e, t) {
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
  return t.clearMarkers = function() {
    T(r, function(f) {
      f.parentNode && f.parentNode.removeChild(f);
    });
  }, r;
}
function I1(e, t, r) {
  for (var n = r ? "invTrans" : "trans", i = t[n], a = t.srcCoords, o = [], s = [], u = !0, l = 0; l < 4; l++) {
    var f = e[l].getBoundingClientRect(), h = 2 * l, v = f.left, c = f.top;
    o.push(v, c), u = u && a && v === a[h] && c === a[h + 1], s.push(e[l].offsetLeft, e[l].offsetTop);
  }
  return u && i ? i : (t.srcCoords = o, t[n] = r ? ev(s, o) : ev(o, s));
}
function yy(e) {
  return e.nodeName.toUpperCase() === "CANVAS";
}
var A1 = /([&<>"'])/g, P1 = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;"
};
function Ut(e) {
  return e == null ? "" : (e + "").replace(A1, function(t, r) {
    return P1[r];
  });
}
var L1 = /^(?:mouse|pointer|contextmenu|drag|drop)|click/, Du = [], E1 = W.browser.firefox && +W.browser.version.split(".")[0] < 39;
function Jl(e, t, r, n) {
  return r = r || {}, n ? rv(e, t, r) : E1 && t.layerX != null && t.layerX !== t.offsetX ? (r.zrX = t.layerX, r.zrY = t.layerY) : t.offsetX != null ? (r.zrX = t.offsetX, r.zrY = t.offsetY) : rv(e, t, r), r;
}
function rv(e, t, r) {
  if (W.domSupported && e.getBoundingClientRect) {
    var n = t.clientX, i = t.clientY;
    if (yy(e)) {
      var a = e.getBoundingClientRect();
      r.zrX = n - a.left, r.zrY = i - a.top;
      return;
    } else if (Ql(Du, e, n, i)) {
      r.zrX = Du[0], r.zrY = Du[1];
      return;
    }
  }
  r.zrX = r.zrY = 0;
}
function mh(e) {
  return e || window.event;
}
function ce(e, t, r) {
  if (t = mh(t), t.zrX != null)
    return t;
  var n = t.type, i = n && n.indexOf("touch") >= 0;
  if (i) {
    var o = n !== "touchend" ? t.targetTouches[0] : t.changedTouches[0];
    o && Jl(e, o, t, r);
  } else {
    Jl(e, t, t, r);
    var a = R1(t);
    t.zrDelta = a ? a / 120 : -(t.detail || 0) / 3;
  }
  var s = t.button;
  return t.which == null && s !== void 0 && L1.test(t.type) && (t.which = s & 1 ? 1 : s & 2 ? 3 : s & 4 ? 2 : 0), t;
}
function R1(e) {
  var t = e.wheelDelta;
  if (t)
    return t;
  var r = e.deltaX, n = e.deltaY;
  if (r == null || n == null)
    return t;
  var i = Math.abs(n !== 0 ? n : r), a = n > 0 ? -1 : n < 0 ? 1 : r > 0 ? -1 : 1;
  return 3 * i * a;
}
function O1(e, t, r, n) {
  e.addEventListener(t, r, n);
}
function k1(e, t, r, n) {
  e.removeEventListener(t, r, n);
}
var my = function(e) {
  e.preventDefault(), e.stopPropagation(), e.cancelBubble = !0;
}, B1 = (function() {
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
        var u = i[o], l = Jl(n, u, {});
        a.points.push([l.zrX, l.zrY]), a.touches.push(u);
      }
      this._track.push(a);
    }
  }, e.prototype._recognize = function(t) {
    for (var r in Iu)
      if (Iu.hasOwnProperty(r)) {
        var n = Iu[r](this._track, t);
        if (n)
          return n;
      }
  }, e;
})();
function nv(e) {
  var t = e[1][0] - e[0][0], r = e[1][1] - e[0][1];
  return Math.sqrt(t * t + r * r);
}
function N1(e) {
  return [
    (e[0][0] + e[1][0]) / 2,
    (e[0][1] + e[1][1]) / 2
  ];
}
var Iu = {
  pinch: function(e, t) {
    var r = e.length;
    if (r) {
      var n = (e[r - 1] || {}).points, i = (e[r - 2] || {}).points || n;
      if (i && i.length > 1 && n && n.length > 1) {
        var a = nv(n) / nv(i);
        !isFinite(a) && (a = 1), t.pinchScale = a;
        var o = N1(n);
        return t.pinchX = o[0], t.pinchY = o[1], {
          type: "pinch",
          target: e[0].target,
          event: t
        };
      }
    }
  }
};
function Se() {
  return [1, 0, 0, 1, 0, 0];
}
function xn(e) {
  return e[0] = 1, e[1] = 0, e[2] = 0, e[3] = 1, e[4] = 0, e[5] = 0, e;
}
function Aa(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[4] = t[4], e[5] = t[5], e;
}
function gn(e, t, r) {
  var n = t[0] * r[0] + t[2] * r[1], i = t[1] * r[0] + t[3] * r[1], a = t[0] * r[2] + t[2] * r[3], o = t[1] * r[2] + t[3] * r[3], s = t[0] * r[4] + t[2] * r[5] + t[4], u = t[1] * r[4] + t[3] * r[5] + t[5];
  return e[0] = n, e[1] = i, e[2] = a, e[3] = o, e[4] = s, e[5] = u, e;
}
function rs(e, t, r) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e[4] = t[4] + r[0], e[5] = t[5] + r[1], e;
}
function Gs(e, t, r, n) {
  n === void 0 && (n = [0, 0]);
  var i = t[0], a = t[2], o = t[4], s = t[1], u = t[3], l = t[5], f = Math.sin(r), h = Math.cos(r);
  return e[0] = i * h + s * f, e[1] = -i * f + s * h, e[2] = a * h + u * f, e[3] = -a * f + h * u, e[4] = h * (o - n[0]) + f * (l - n[1]) + n[0], e[5] = h * (l - n[1]) - f * (o - n[0]) + n[1], e;
}
function _y(e, t, r) {
  var n = r[0], i = r[1];
  return e[0] = t[0] * n, e[1] = t[1] * i, e[2] = t[2] * n, e[3] = t[3] * i, e[4] = t[4] * n, e[5] = t[5] * i, e;
}
function di(e, t) {
  var r = t[0], n = t[2], i = t[4], a = t[1], o = t[3], s = t[5], u = r * o - a * n;
  return u ? (u = 1 / u, e[0] = o * u, e[1] = -a * u, e[2] = -n * u, e[3] = r * u, e[4] = (n * s - o * i) * u, e[5] = (a * i - r * s) * u, e) : null;
}
function F1(e) {
  var t = Se();
  return Aa(t, e), t;
}
const z1 = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  clone: F1,
  copy: Aa,
  create: Se,
  identity: xn,
  invert: di,
  mul: gn,
  rotate: Gs,
  scale: _y,
  translate: rs
}, Symbol.toStringTag, { value: "Module" }));
var ut = (function() {
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
})(), ln = Math.min, Zn = Math.max, tf = Math.abs, iv = ["x", "y"], H1 = ["width", "height"], Fr = new ut(), zr = new ut(), Hr = new ut(), Vr = new ut(), ne = Sy(), Yi = ne.minTv, ef = ne.maxTv, ji = [0, 0], Y = (function() {
  function e(t, r, n, i) {
    Au(this, t, r, n, i);
  }
  return e.set = function(t, r, n, i, a) {
    return i < 0 && (r = r + i, i = -i), a < 0 && (n = n + a, a = -a), t.x = r, t.y = n, t.width = i, t.height = a, t;
  }, e.prototype.union = function(t) {
    var r = ln(t.x, this.x), n = ln(t.y, this.y);
    isFinite(this.x) && isFinite(this.width) ? this.width = Zn(t.x + t.width, this.x + this.width) - r : this.width = t.width, isFinite(this.y) && isFinite(this.height) ? this.height = Zn(t.y + t.height, this.y + this.height) - n : this.height = t.height, this.x = r, this.y = n;
  }, e.prototype.applyTransform = function(t) {
    e.applyTransform(this, this, t);
  }, e.prototype.calculateTransform = function(t) {
    return V1(Se(), this, t);
  }, e.prototype.intersect = function(t, r, n) {
    return e.intersect(this, t, r, n);
  }, e.intersect = function(t, r, n, i) {
    n && ut.set(n, 0, 0);
    var a = i && i.outIntersectRect || null, o = i && i.clamp;
    if (a && (a.x = a.y = a.width = a.height = NaN), !t || !r)
      return !1;
    t instanceof e || (t = Au(G1, t.x, t.y, t.width, t.height)), r instanceof e || (r = Au(U1, r.x, r.y, r.width, r.height));
    var s = !!n;
    ne.reset(i, s);
    var u = ne.touchThreshold, l = t.x + u, f = t.x + t.width - u, h = t.y + u, v = t.y + t.height - u, c = r.x + u, d = r.x + r.width - u, y = r.y + u, p = r.y + r.height - u;
    if (l > f || h > v || c > d || y > p)
      return !1;
    var g = !(f < c || d < l || v < y || p < h);
    return (s || a) && (ji[0] = 1 / 0, ji[1] = 0, ov(l, f, c, d, 0, s, a, o), ov(h, v, y, p, 1, s, a, o), s && ut.copy(n, g ? ne.useDir ? ne.dirMinTv : Yi : ef)), g;
  }, e.contain = function(t, r, n) {
    return r >= t.x && r <= t.x + t.width && n >= t.y && n <= t.y + t.height;
  }, e.prototype.contain = function(t, r) {
    return e.contain(this, t, r);
  }, e.prototype.clone = function() {
    return new e(this.x, this.y, this.width, this.height);
  }, e.prototype.copy = function(t) {
    av(this, t);
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
    return new e(t ? t.x : 0, t ? t.y : 0, t ? t.width : 0, t ? t.height : 0);
  }, e.copy = function(t, r) {
    return t.x = r.x, t.y = r.y, t.width = r.width, t.height = r.height, t;
  }, e.applyTransform = function(t, r, n) {
    if (!n) {
      t !== r && av(t, r);
      return;
    }
    if (n[1] < 1e-5 && n[1] > -1e-5 && n[2] < 1e-5 && n[2] > -1e-5) {
      var i = n[0], a = n[3], o = n[4], s = n[5];
      t.x = r.x * i + o, t.y = r.y * a + s, t.width = r.width * i, t.height = r.height * a, t.width < 0 && (t.x += t.width, t.width = -t.width), t.height < 0 && (t.y += t.height, t.height = -t.height);
      return;
    }
    Fr.x = Hr.x = r.x, Fr.y = Vr.y = r.y, zr.x = Vr.x = r.x + r.width, zr.y = Hr.y = r.y + r.height, Fr.transform(n), Vr.transform(n), zr.transform(n), Hr.transform(n), t.x = ln(Fr.x, zr.x, Hr.x, Vr.x), t.y = ln(Fr.y, zr.y, Hr.y, Vr.y);
    var u = Zn(Fr.x, zr.x, Hr.x, Vr.x), l = Zn(Fr.y, zr.y, Hr.y, Vr.y);
    t.width = u - t.x, t.height = l - t.y;
  }, e.calculateTransform = function(t, r, n) {
    var i = n.width / r.width, a = n.height / r.height;
    return t = xn(t || []), rs(t, t, Po(Pu, -r.x, -r.y)), _y(t, t, Po(Pu, i, a)), rs(t, t, Po(Pu, n.x, n.y)), t;
  }, e;
})();
Y.create;
var Au = Y.set, av = Y.copy, V1 = Y.calculateTransform;
Y.applyTransform;
Y.contain;
var G1 = new Y(0, 0, 0, 0), U1 = new Y(0, 0, 0, 0), Pu = [];
function ov(e, t, r, n, i, a, o, s) {
  var u = tf(t - r), l = tf(n - e), f = ln(u, l), h = iv[i], v = iv[1 - i], c = H1[i];
  t < r || n < e ? u < l ? (a && (ef[h] = -u), s && (o[h] = t, o[c] = 0)) : (a && (ef[h] = l), s && (o[h] = e, o[c] = 0)) : (o && (o[h] = Zn(e, r), o[c] = ln(t, n) - o[h]), a && (f < ji[0] || ne.useDir) && (ji[0] = ln(f, ji[0]), (u < l || !ne.bidirectional) && (Yi[h] = u, Yi[v] = 0, ne.useDir && ne.calcDirMTV()), (u >= l || !ne.bidirectional) && (Yi[h] = -l, Yi[v] = 0, ne.useDir && ne.calcDirMTV())));
}
function Sy() {
  var e = 0, t = new ut(), r = new ut(), n = {
    minTv: new ut(),
    maxTv: new ut(),
    useDir: !1,
    dirMinTv: new ut(),
    touchThreshold: 0,
    bidirectional: !0,
    negativeSize: !1,
    reset: function(a, o) {
      n.touchThreshold = 0, a && a.touchThreshold != null && (n.touchThreshold = Zn(0, a.touchThreshold)), n.negativeSize = !1, o && (n.minTv.set(1 / 0, 1 / 0), n.maxTv.set(0, 0), n.useDir = !1, a && a.direction != null && (n.useDir = !0, n.dirMinTv.copy(n.minTv), r.copy(n.minTv), e = a.direction, n.bidirectional = a.bidirectional == null || !!a.bidirectional, n.bidirectional || t.set(Math.cos(e), Math.sin(e))));
    },
    calcDirMTV: function() {
      var a = n.minTv, o = n.dirMinTv, s = a.y * a.y + a.x * a.x, u = Math.sin(e), l = Math.cos(e), f = u * a.y + l * a.x;
      if (i(f)) {
        i(a.x) && i(a.y) && o.set(0, 0);
        return;
      }
      if (r.x = s * l / f, r.y = s * u / f, i(r.x) && i(r.y)) {
        o.set(0, 0);
        return;
      }
      (n.bidirectional || t.dot(r) > 0) && r.len() < o.len() && o.copy(r);
    }
  };
  function i(a) {
    return tf(a) < 1e-10;
  }
  return n;
}
var by = "silent";
function Y1(e, t, r) {
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
    stop: W1
  };
}
function W1() {
  my(this.event);
}
var X1 = (function(e) {
  z(t, e);
  function t() {
    var r = e !== null && e.apply(this, arguments) || this;
    return r.handler = null, r;
  }
  return t.prototype.dispose = function() {
  }, t.prototype.setCursor = function() {
  }, t;
})(qe), xi = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.x = t, this.y = r;
  }
  return e;
})(), q1 = [
  "click",
  "dblclick",
  "mousewheel",
  "mouseout",
  "mouseup",
  "mousedown",
  "mousemove",
  "contextmenu"
], Lu = new Y(0, 0, 0, 0), wy = (function(e) {
  z(t, e);
  function t(r, n, i, a, o) {
    var s = e.call(this) || this;
    return s._hovered = new xi(0, 0), s.storage = r, s.painter = n, s.painterRoot = a, s._pointerSize = o, i = i || new X1(), s.proxy = null, s.setHandlerProxy(i), s._draggingMgr = new T1(s), s;
  }
  return t.prototype.setHandlerProxy = function(r) {
    this.proxy && this.proxy.dispose(), r && (T(q1, function(n) {
      r.on && r.on(n, this[n], this);
    }, this), r.handler = this), this.proxy = r;
  }, t.prototype.mousemove = function(r) {
    var n = r.zrX, i = r.zrY, a = Ty(this, n, i), o = this._hovered, s = o.target;
    s && !s.__zr && (o = this.findHover(o.x, o.y), s = o.target);
    var u = this._hovered = a ? new xi(n, i) : this.findHover(n, i), l = u.target, f = this.proxy;
    f.setCursor && f.setCursor(l ? l.cursor : "default"), s && l !== s && this.dispatchToElement(o, "mouseout", r), this.dispatchToElement(u, "mousemove", r), l && l !== s && this.dispatchToElement(u, "mouseover", r);
  }, t.prototype.mouseout = function(r) {
    var n = r.zrEventControl;
    n !== "only_globalout" && this.dispatchToElement(this._hovered, "mouseout", r), n !== "no_globalout" && this.trigger("globalout", { type: "globalout", event: r });
  }, t.prototype.resize = function() {
    this._hovered = new xi(0, 0);
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
      for (var o = "on" + n, s = Y1(n, r, i); a && (a[o] && (s.cancelBubble = !!a[o].call(a, s)), a.trigger(n, s), a = a.__hostTarget ? a.__hostTarget : a.parent, !s.cancelBubble); )
        ;
      s.cancelBubble || (this.trigger(n, s), this.painter && this.painter.eachOtherLayer && this.painter.eachOtherLayer(function(u) {
        typeof u[o] == "function" && u[o].call(u, s), u.trigger && u.trigger(n, s);
      }));
    }
  }, t.prototype.findHover = function(r, n, i) {
    var a = this.storage.getDisplayList(), o = new xi(r, n);
    if (sv(a, o, r, n, i), this._pointerSize && !o.target) {
      for (var s = [], u = this._pointerSize, l = u / 2, f = new Y(r - l, n - l, u, u), h = a.length - 1; h >= 0; h--) {
        var v = a[h];
        v !== i && !v.ignore && !v.ignoreCoarsePointer && (!v.parent || !v.parent.ignoreCoarsePointer) && (Lu.copy(v.getBoundingRect()), v.transform && Lu.applyTransform(v.transform), Lu.intersect(f) && s.push(v));
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
  }, t.prototype.processGesture = function(r, n) {
    this._gestureMgr || (this._gestureMgr = new B1());
    var i = this._gestureMgr;
    n === "start" && i.clear();
    var a = i.recognize(r, this.findHover(r.zrX, r.zrY, null).target, this.proxy.dom);
    if (n === "end" && i.clear(), a) {
      var o = a.type;
      r.gestureEvent = o;
      var s = new xi();
      s.target = a.target, this.dispatchToElement(s, o, a.event);
    }
  }, t;
})(qe);
T(["click", "mousedown", "mouseup", "mousewheel", "dblclick", "contextmenu"], function(e) {
  wy.prototype[e] = function(t) {
    var r = t.zrX, n = t.zrY, i = Ty(this, r, n), a, o;
    if ((e !== "mouseup" || !i) && (a = this.findHover(r, n), o = a.target), e === "mousedown")
      this._downEl = o, this._downPoint = [t.zrX, t.zrY], this._upEl = o;
    else if (e === "mouseup")
      this._upEl = o;
    else if (e === "click") {
      if (this._downEl !== this._upEl || !this._downPoint || py(this._downPoint, [t.zrX, t.zrY]) > 4)
        return;
      this._downPoint = null;
    }
    this.dispatchToElement(a, e, t);
  };
});
function $1(e, t, r) {
  if (e[e.rectHover ? "rectContain" : "contain"](t, r)) {
    for (var n = e, i = void 0, a = !1; n; ) {
      if (n.ignoreClip && (a = !0), !a) {
        var o = n.getClipPath();
        if (o && !o.contain(t, r))
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
function sv(e, t, r, n, i) {
  for (var a = e.length - 1; a >= 0; a--) {
    var o = e[a], s = void 0;
    if (o !== i && !o.ignore && (s = $1(o, r, n)) && (!t.topTarget && (t.topTarget = o), s !== by)) {
      t.target = o;
      break;
    }
  }
}
function Ty(e, t, r) {
  var n = e.painter;
  return t < 0 || t > n.getWidth() || r < 0 || r > n.getHeight();
}
var Cy = 32, Mi = 7;
function Z1(e) {
  for (var t = 0; e >= Cy; )
    t |= e & 1, e >>= 1;
  return e + t;
}
function uv(e, t, r, n) {
  var i = t + 1;
  if (i === r)
    return 1;
  if (n(e[i++], e[t]) < 0) {
    for (; i < r && n(e[i], e[i - 1]) < 0; )
      i++;
    K1(e, t, i);
  } else
    for (; i < r && n(e[i], e[i - 1]) >= 0; )
      i++;
  return i - t;
}
function K1(e, t, r) {
  for (r--; t < r; ) {
    var n = e[t];
    e[t++] = e[r], e[r--] = n;
  }
}
function lv(e, t, r, n, i) {
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
function Eu(e, t, r, n, i, a) {
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
function Ru(e, t, r, n, i, a) {
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
function j1(e, t) {
  var r = Mi, n, i, a = 0, o = [];
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
    var m = Ru(e[p], e, d, y, 0, t);
    d += m, y -= m, y !== 0 && (g = Eu(e[d + y - 1], e, p, g, g - 1, t), g !== 0 && (y <= g ? h(d, y, p, g) : v(d, y, p, g)));
  }
  function h(c, d, y, p) {
    var g = 0;
    for (g = 0; g < d; g++)
      o[g] = e[c + g];
    var m = 0, _ = y, S = c;
    if (e[S++] = e[_++], --p === 0) {
      for (g = 0; g < d; g++)
        e[S + g] = o[m + g];
      return;
    }
    if (d === 1) {
      for (g = 0; g < p; g++)
        e[S + g] = e[_ + g];
      e[S + p] = o[m];
      return;
    }
    for (var b = r, w, C, M; ; ) {
      w = 0, C = 0, M = !1;
      do
        if (t(e[_], o[m]) < 0) {
          if (e[S++] = e[_++], C++, w = 0, --p === 0) {
            M = !0;
            break;
          }
        } else if (e[S++] = o[m++], w++, C = 0, --d === 1) {
          M = !0;
          break;
        }
      while ((w | C) < b);
      if (M)
        break;
      do {
        if (w = Ru(e[_], o, m, d, 0, t), w !== 0) {
          for (g = 0; g < w; g++)
            e[S + g] = o[m + g];
          if (S += w, m += w, d -= w, d <= 1) {
            M = !0;
            break;
          }
        }
        if (e[S++] = e[_++], --p === 0) {
          M = !0;
          break;
        }
        if (C = Eu(o[m], e, _, p, 0, t), C !== 0) {
          for (g = 0; g < C; g++)
            e[S + g] = e[_ + g];
          if (S += C, _ += C, p -= C, p === 0) {
            M = !0;
            break;
          }
        }
        if (e[S++] = o[m++], --d === 1) {
          M = !0;
          break;
        }
        b--;
      } while (w >= Mi || C >= Mi);
      if (M)
        break;
      b < 0 && (b = 0), b += 2;
    }
    if (r = b, r < 1 && (r = 1), d === 1) {
      for (g = 0; g < p; g++)
        e[S + g] = e[_ + g];
      e[S + p] = o[m];
    } else {
      if (d === 0)
        throw new Error();
      for (g = 0; g < d; g++)
        e[S + g] = o[m + g];
    }
  }
  function v(c, d, y, p) {
    var g = 0;
    for (g = 0; g < p; g++)
      o[g] = e[y + g];
    var m = c + d - 1, _ = p - 1, S = y + p - 1, b = 0, w = 0;
    if (e[S--] = e[m--], --d === 0) {
      for (b = S - (p - 1), g = 0; g < p; g++)
        e[b + g] = o[g];
      return;
    }
    if (p === 1) {
      for (S -= d, m -= d, w = S + 1, b = m + 1, g = d - 1; g >= 0; g--)
        e[w + g] = e[b + g];
      e[S] = o[_];
      return;
    }
    for (var C = r; ; ) {
      var M = 0, x = 0, D = !1;
      do
        if (t(o[_], e[m]) < 0) {
          if (e[S--] = e[m--], M++, x = 0, --d === 0) {
            D = !0;
            break;
          }
        } else if (e[S--] = o[_--], x++, M = 0, --p === 1) {
          D = !0;
          break;
        }
      while ((M | x) < C);
      if (D)
        break;
      do {
        if (M = d - Ru(o[_], e, c, d, d - 1, t), M !== 0) {
          for (S -= M, m -= M, d -= M, w = S + 1, b = m + 1, g = M - 1; g >= 0; g--)
            e[w + g] = e[b + g];
          if (d === 0) {
            D = !0;
            break;
          }
        }
        if (e[S--] = o[_--], --p === 1) {
          D = !0;
          break;
        }
        if (x = p - Eu(e[m], o, 0, p, p - 1, t), x !== 0) {
          for (S -= x, _ -= x, p -= x, w = S + 1, b = _ + 1, g = 0; g < x; g++)
            e[w + g] = o[b + g];
          if (p <= 1) {
            D = !0;
            break;
          }
        }
        if (e[S--] = e[m--], --d === 0) {
          D = !0;
          break;
        }
        C--;
      } while (M >= Mi || x >= Mi);
      if (D)
        break;
      C < 0 && (C = 0), C += 2;
    }
    if (r = C, r < 1 && (r = 1), p === 1) {
      for (S -= d, m -= d, w = S + 1, b = m + 1, g = d - 1; g >= 0; g--)
        e[w + g] = e[b + g];
      e[S] = o[_];
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
function Eo(e, t, r, n) {
  r || (r = 0), n || (n = e.length);
  var i = n - r;
  if (!(i < 2)) {
    var a = 0;
    if (i < Cy) {
      a = uv(e, r, n, t), lv(e, r, n, r + a, t);
      return;
    }
    var o = j1(e, t), s = Z1(i);
    do {
      if (a = uv(e, r, n, t), a < s) {
        var u = i;
        u > s && (u = s), lv(e, r, r + u, r + a, t), a = u;
      }
      o.pushRun(r, a), o.mergeRuns(), i -= a, r += a;
    } while (i !== 0);
    o.forceMergeRuns();
  }
}
var Qt = 1, Wi = 2, qn = 4, fv = !1;
function Ou() {
  fv || (fv = !0, console.warn("z / z2 / zlevel of displayable is invalid, which may cause unexpected errors"));
}
function hv(e, t) {
  return e.zlevel === t.zlevel ? e.z === t.z ? e.z2 - t.z2 : e.z - t.z : e.zlevel - t.zlevel;
}
var Q1 = (function() {
  function e() {
    this._roots = [], this._displayList = [], this._displayListLen = 0, this.displayableSortFunc = hv;
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
    n.length = this._displayListLen, Eo(n, hv);
  }, e.prototype._updateAndAddDisplayable = function(t, r, n) {
    if (!(t.ignore && !n)) {
      t.beforeUpdate(), t.update(), t.afterUpdate();
      var i = t.getClipPath(), a = r && r.length, o = 0, s = t.__clipPaths;
      if (!t.ignoreClip && (a || i)) {
        if (s || (s = t.__clipPaths = []), a)
          for (var u = 0; u < r.length; u++)
            s[o++] = r[u];
        for (var l = i, f = t; l; )
          l.parent = f, l.updateTransform(), s[o++] = l, f = l, l = l.getClipPath();
      }
      if (s && (s.length = o), t.childrenRef) {
        for (var h = t.childrenRef(), v = 0; v < h.length; v++) {
          var c = h[v];
          t.__dirty && (c.__dirty |= Qt), this._updateAndAddDisplayable(c, s, n);
        }
        t.__dirty = 0;
      } else {
        var d = t;
        isNaN(d.z) && (Ou(), d.z = 0), isNaN(d.z2) && (Ou(), d.z2 = 0), isNaN(d.zlevel) && (Ou(), d.zlevel = 0), this._displayList[this._displayListLen++] = d;
      }
      var y = t.getDecalElement && t.getDecalElement();
      y && this._updateAndAddDisplayable(y, s, n);
      var p = t.getTextGuideLine();
      p && this._updateAndAddDisplayable(p, s, n);
      var g = t.getTextContent();
      g && this._updateAndAddDisplayable(g, s, n);
    }
  }, e.prototype.addRoot = function(t) {
    t.__zr && t.__zr.storage === this || this._roots.push(t);
  }, e.prototype.delRoot = function(t) {
    if (t instanceof Array) {
      for (var r = 0, n = t.length; r < n; r++)
        this.delRoot(t[r]);
      return;
    }
    var i = st(this._roots, t);
    i >= 0 && this._roots.splice(i, 1);
  }, e.prototype.delAllRoots = function() {
    this._roots = [], this._displayList = [], this._displayListLen = 0;
  }, e.prototype.getRoots = function() {
    return this._roots;
  }, e.prototype.dispose = function() {
    this._displayList = null, this._roots = null;
  }, e;
})(), ns;
ns = W.hasGlobalWindow && (window.requestAnimationFrame && window.requestAnimationFrame.bind(window) || window.msRequestAnimationFrame && window.msRequestAnimationFrame.bind(window) || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame) || function(e) {
  return setTimeout(e, 16);
};
var Qi = {
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
    return 1 - Qi.bounceOut(1 - e);
  },
  bounceOut: function(e) {
    return e < 1 / 2.75 ? 7.5625 * e * e : e < 2 / 2.75 ? 7.5625 * (e -= 1.5 / 2.75) * e + 0.75 : e < 2.5 / 2.75 ? 7.5625 * (e -= 2.25 / 2.75) * e + 0.9375 : 7.5625 * (e -= 2.625 / 2.75) * e + 0.984375;
  },
  bounceInOut: function(e) {
    return e < 0.5 ? Qi.bounceIn(e * 2) * 0.5 : Qi.bounceOut(e * 2 - 1) * 0.5 + 0.5;
  }
}, Ga = Math.pow, Mr = Math.sqrt, is = 1e-8, xy = 1e-4, cv = Mr(3), Ua = 1 / 3, Fe = Cn(), pe = Cn(), ti = Cn();
function Tr(e) {
  return e > -is && e < is;
}
function My(e) {
  return e > is || e < -is;
}
function Pt(e, t, r, n, i) {
  var a = 1 - i;
  return a * a * (a * e + 3 * i * t) + i * i * (i * n + 3 * a * r);
}
function vv(e, t, r, n, i) {
  var a = 1 - i;
  return 3 * (((t - e) * a + 2 * (r - t) * i) * a + (n - r) * i * i);
}
function Dy(e, t, r, n, i, a) {
  var o = n + 3 * (t - r) - e, s = 3 * (r - t * 2 + e), u = 3 * (t - e), l = e - i, f = s * s - 3 * o * u, h = s * u - 9 * o * l, v = u * u - 3 * s * l, c = 0;
  if (Tr(f) && Tr(h))
    if (Tr(s))
      a[0] = 0;
    else {
      var d = -u / s;
      d >= 0 && d <= 1 && (a[c++] = d);
    }
  else {
    var y = h * h - 4 * f * v;
    if (Tr(y)) {
      var p = h / f, d = -s / o + p, g = -p / 2;
      d >= 0 && d <= 1 && (a[c++] = d), g >= 0 && g <= 1 && (a[c++] = g);
    } else if (y > 0) {
      var m = Mr(y), _ = f * s + 1.5 * o * (-h + m), S = f * s + 1.5 * o * (-h - m);
      _ < 0 ? _ = -Ga(-_, Ua) : _ = Ga(_, Ua), S < 0 ? S = -Ga(-S, Ua) : S = Ga(S, Ua);
      var d = (-s - (_ + S)) / (3 * o);
      d >= 0 && d <= 1 && (a[c++] = d);
    } else {
      var b = (2 * f * s - 3 * o * h) / (2 * Mr(f * f * f)), w = Math.acos(b) / 3, C = Mr(f), M = Math.cos(w), d = (-s - 2 * C * M) / (3 * o), g = (-s + C * (M + cv * Math.sin(w))) / (3 * o), x = (-s + C * (M - cv * Math.sin(w))) / (3 * o);
      d >= 0 && d <= 1 && (a[c++] = d), g >= 0 && g <= 1 && (a[c++] = g), x >= 0 && x <= 1 && (a[c++] = x);
    }
  }
  return c;
}
function Iy(e, t, r, n, i) {
  var a = 6 * r - 12 * t + 6 * e, o = 9 * t + 3 * n - 3 * e - 9 * r, s = 3 * t - 3 * e, u = 0;
  if (Tr(o)) {
    if (My(a)) {
      var l = -s / a;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = a * a - 4 * o * s;
    if (Tr(f))
      i[0] = -a / (2 * o);
    else if (f > 0) {
      var h = Mr(f), l = (-a + h) / (2 * o), v = (-a - h) / (2 * o);
      l >= 0 && l <= 1 && (i[u++] = l), v >= 0 && v <= 1 && (i[u++] = v);
    }
  }
  return u;
}
function as(e, t, r, n, i, a) {
  var o = (t - e) * i + e, s = (r - t) * i + t, u = (n - r) * i + r, l = (s - o) * i + o, f = (u - s) * i + s, h = (f - l) * i + l;
  a[0] = e, a[1] = o, a[2] = l, a[3] = h, a[4] = h, a[5] = f, a[6] = u, a[7] = n;
}
function J1(e, t, r, n, i, a, o, s, u, l, f) {
  var h, v = 5e-3, c = 1 / 0, d, y, p, g;
  Fe[0] = u, Fe[1] = l;
  for (var m = 0; m < 1; m += 0.05)
    pe[0] = Pt(e, r, i, o, m), pe[1] = Pt(t, n, a, s, m), p = pn(Fe, pe), p < c && (h = m, c = p);
  c = 1 / 0;
  for (var _ = 0; _ < 32 && !(v < xy); _++)
    d = h - v, y = h + v, pe[0] = Pt(e, r, i, o, d), pe[1] = Pt(t, n, a, s, d), p = pn(pe, Fe), d >= 0 && p < c ? (h = d, c = p) : (ti[0] = Pt(e, r, i, o, y), ti[1] = Pt(t, n, a, s, y), g = pn(ti, Fe), y <= 1 && g < c ? (h = y, c = g) : v *= 0.5);
  return f && (f[0] = Pt(e, r, i, o, h), f[1] = Pt(t, n, a, s, h)), Mr(c);
}
function tb(e, t, r, n, i, a, o, s, u) {
  for (var l = e, f = t, h = 0, v = 1 / u, c = 1; c <= u; c++) {
    var d = c * v, y = Pt(e, r, i, o, d), p = Pt(t, n, a, s, d), g = y - l, m = p - f;
    h += Math.sqrt(g * g + m * m), l = y, f = p;
  }
  return h;
}
function Bt(e, t, r, n) {
  var i = 1 - n;
  return i * (i * e + 2 * n * t) + n * n * r;
}
function dv(e, t, r, n) {
  return 2 * ((1 - n) * (t - e) + n * (r - t));
}
function eb(e, t, r, n, i) {
  var a = e - 2 * t + r, o = 2 * (t - e), s = e - n, u = 0;
  if (Tr(a)) {
    if (My(o)) {
      var l = -s / o;
      l >= 0 && l <= 1 && (i[u++] = l);
    }
  } else {
    var f = o * o - 4 * a * s;
    if (Tr(f)) {
      var l = -o / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l);
    } else if (f > 0) {
      var h = Mr(f), l = (-o + h) / (2 * a), v = (-o - h) / (2 * a);
      l >= 0 && l <= 1 && (i[u++] = l), v >= 0 && v <= 1 && (i[u++] = v);
    }
  }
  return u;
}
function Ay(e, t, r) {
  var n = e + r - 2 * t;
  return n === 0 ? 0.5 : (e - t) / n;
}
function os(e, t, r, n, i) {
  var a = (t - e) * n + e, o = (r - t) * n + t, s = (o - a) * n + a;
  i[0] = e, i[1] = a, i[2] = s, i[3] = s, i[4] = o, i[5] = r;
}
function rb(e, t, r, n, i, a, o, s, u) {
  var l, f = 5e-3, h = 1 / 0;
  Fe[0] = o, Fe[1] = s;
  for (var v = 0; v < 1; v += 0.05) {
    pe[0] = Bt(e, r, i, v), pe[1] = Bt(t, n, a, v);
    var c = pn(Fe, pe);
    c < h && (l = v, h = c);
  }
  h = 1 / 0;
  for (var d = 0; d < 32 && !(f < xy); d++) {
    var y = l - f, p = l + f;
    pe[0] = Bt(e, r, i, y), pe[1] = Bt(t, n, a, y);
    var c = pn(pe, Fe);
    if (y >= 0 && c < h)
      l = y, h = c;
    else {
      ti[0] = Bt(e, r, i, p), ti[1] = Bt(t, n, a, p);
      var g = pn(ti, Fe);
      p <= 1 && g < h ? (l = p, h = g) : f *= 0.5;
    }
  }
  return u && (u[0] = Bt(e, r, i, l), u[1] = Bt(t, n, a, l)), Mr(h);
}
function nb(e, t, r, n, i, a, o) {
  for (var s = e, u = t, l = 0, f = 1 / o, h = 1; h <= o; h++) {
    var v = h * f, c = Bt(e, r, i, v), d = Bt(t, n, a, v), y = c - s, p = d - u;
    l += Math.sqrt(y * y + p * p), s = c, u = d;
  }
  return l;
}
var ib = /cubic-bezier\(([0-9,\.e ]+)\)/;
function Py(e) {
  var t = e && ib.exec(e);
  if (t) {
    var r = t[1].split(","), n = +Me(r[0]), i = +Me(r[1]), a = +Me(r[2]), o = +Me(r[3]);
    if (isNaN(n + i + a + o))
      return;
    var s = [];
    return function(u) {
      return u <= 0 ? 0 : u >= 1 ? 1 : Dy(0, n, a, 1, u, s) && Pt(0, i, o, 1, s[0]);
    };
  }
}
var ab = (function() {
  function e(t) {
    this._inited = !1, this._startTime = 0, this._pausedTime = 0, this._paused = !1, this._life = t.life || 1e3, this._delay = t.delay || 0, this.loop = t.loop || !1, this.onframe = t.onframe || Et, this.ondestroy = t.ondestroy || Et, this.onrestart = t.onrestart || Et, t.easing && this.setEasing(t.easing);
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
    this.easing = t, this.easingFunc = X(t) ? t : Qi[t] || Py(t);
  }, e;
})(), Ly = /* @__PURE__ */ (function() {
  function e(t) {
    this.value = t;
  }
  return e;
})(), ob = (function() {
  function e() {
    this._len = 0;
  }
  return e.prototype.insert = function(t) {
    var r = new Ly(t);
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
})(), si = (function() {
  function e(t) {
    this._list = new ob(), this._maxSize = 10, this._map = {}, this._maxSize = t;
  }
  return e.prototype.put = function(t, r) {
    var n = this._list, i = this._map, a = null;
    if (i[t] == null) {
      var o = n.len(), s = this._lastRemovedEntry;
      if (o >= this._maxSize && o > 0) {
        var u = n.head;
        n.remove(u), delete i[u.key], a = u.value, this._lastRemovedEntry = u;
      }
      s ? s.value = r : s = new Ly(r), s.key = t, n.insertEntry(s), i[t] = s;
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
function De(e) {
  return e = Math.round(e), e < 0 ? 0 : e > 255 ? 255 : e;
}
function sb(e) {
  return e = Math.round(e), e < 0 ? 0 : e > 360 ? 360 : e;
}
function ha(e) {
  return e < 0 ? 0 : e > 1 ? 1 : e;
}
function Ro(e) {
  var t = e;
  return t.length && t.charAt(t.length - 1) === "%" ? De(parseFloat(t) / 100 * 255) : De(parseInt(t, 10));
}
function Dr(e) {
  var t = e;
  return t.length && t.charAt(t.length - 1) === "%" ? ha(parseFloat(t) / 100) : ha(parseFloat(t));
}
function ku(e, t, r) {
  return r < 0 ? r += 1 : r > 1 && (r -= 1), r * 6 < 1 ? e + (t - e) * r * 6 : r * 2 < 1 ? t : r * 3 < 2 ? e + (t - e) * (2 / 3 - r) * 6 : e;
}
function Cr(e, t, r) {
  return e + (t - e) * r;
}
function fe(e, t, r, n, i) {
  return e[0] = t, e[1] = r, e[2] = n, e[3] = i, e;
}
function rf(e, t) {
  return e[0] = t[0], e[1] = t[1], e[2] = t[2], e[3] = t[3], e;
}
var Ey = new si(20), Ya = null;
function En(e, t) {
  Ya && rf(Ya, t), Ya = Ey.put(e, Ya || t.slice());
}
function ae(e, t) {
  if (e) {
    t = t || [];
    var r = Ey.get(e);
    if (r)
      return rf(t, r);
    e = e + "";
    var n = e.replace(/ /g, "").toLowerCase();
    if (n in pv)
      return rf(t, pv[n]), En(e, t), t;
    var i = n.length;
    if (n.charAt(0) === "#") {
      if (i === 4 || i === 5) {
        var a = parseInt(n.slice(1, 4), 16);
        if (!(a >= 0 && a <= 4095)) {
          fe(t, 0, 0, 0, 1);
          return;
        }
        return fe(t, (a & 3840) >> 4 | (a & 3840) >> 8, a & 240 | (a & 240) >> 4, a & 15 | (a & 15) << 4, i === 5 ? parseInt(n.slice(4), 16) / 15 : 1), En(e, t), t;
      } else if (i === 7 || i === 9) {
        var a = parseInt(n.slice(1, 7), 16);
        if (!(a >= 0 && a <= 16777215)) {
          fe(t, 0, 0, 0, 1);
          return;
        }
        return fe(t, (a & 16711680) >> 16, (a & 65280) >> 8, a & 255, i === 9 ? parseInt(n.slice(7), 16) / 255 : 1), En(e, t), t;
      }
      return;
    }
    var o = n.indexOf("("), s = n.indexOf(")");
    if (o !== -1 && s + 1 === i) {
      var u = n.substr(0, o), l = n.substr(o + 1, s - (o + 1)).split(","), f = 1;
      switch (u) {
        case "rgba":
          if (l.length !== 4)
            return l.length === 3 ? fe(t, +l[0], +l[1], +l[2], 1) : fe(t, 0, 0, 0, 1);
          f = Dr(l.pop());
        case "rgb":
          if (l.length >= 3)
            return fe(t, Ro(l[0]), Ro(l[1]), Ro(l[2]), l.length === 3 ? f : Dr(l[3])), En(e, t), t;
          fe(t, 0, 0, 0, 1);
          return;
        case "hsla":
          if (l.length !== 4) {
            fe(t, 0, 0, 0, 1);
            return;
          }
          return l[3] = Dr(l[3]), nf(l, t), En(e, t), t;
        case "hsl":
          if (l.length !== 3) {
            fe(t, 0, 0, 0, 1);
            return;
          }
          return nf(l, t), En(e, t), t;
        default:
          return;
      }
    }
    fe(t, 0, 0, 0, 1);
  }
}
function nf(e, t) {
  var r = (parseFloat(e[0]) % 360 + 360) % 360 / 360, n = Dr(e[1]), i = Dr(e[2]), a = i <= 0.5 ? i * (n + 1) : i + n - i * n, o = i * 2 - a;
  return t = t || [], fe(t, De(ku(o, a, r + 1 / 3) * 255), De(ku(o, a, r) * 255), De(ku(o, a, r - 1 / 3) * 255), 1), e.length === 4 && (t[3] = e[3]), t;
}
function ub(e) {
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
function af(e, t) {
  var r = ae(e);
  if (r) {
    for (var n = 0; n < 3; n++)
      t < 0 ? r[n] = r[n] * (1 - t) | 0 : r[n] = (255 - r[n]) * t + r[n] | 0, r[n] > 255 ? r[n] = 255 : r[n] < 0 && (r[n] = 0);
    return Or(r, r.length === 4 ? "rgba" : "rgb");
  }
}
function lb(e) {
  var t = ae(e);
  if (t)
    return ((1 << 24) + (t[0] << 16) + (t[1] << 8) + +t[2]).toString(16).slice(1);
}
function Ry(e, t, r) {
  if (!(!(t && t.length) || !(e >= 0 && e <= 1))) {
    r = r || [];
    var n = e * (t.length - 1), i = Math.floor(n), a = Math.ceil(n), o = t[i], s = t[a], u = n - i;
    return r[0] = De(Cr(o[0], s[0], u)), r[1] = De(Cr(o[1], s[1], u)), r[2] = De(Cr(o[2], s[2], u)), r[3] = ha(Cr(o[3], s[3], u)), r;
  }
}
var fb = Ry;
function Oy(e, t, r) {
  if (!(!(t && t.length) || !(e >= 0 && e <= 1))) {
    var n = e * (t.length - 1), i = Math.floor(n), a = Math.ceil(n), o = ae(t[i]), s = ae(t[a]), u = n - i, l = Or([
      De(Cr(o[0], s[0], u)),
      De(Cr(o[1], s[1], u)),
      De(Cr(o[2], s[2], u)),
      ha(Cr(o[3], s[3], u))
    ], "rgba");
    return r ? {
      color: l,
      leftIndex: i,
      rightIndex: a,
      value: n
    } : l;
  }
}
var hb = Oy;
function ss(e, t, r, n) {
  var i = ae(e);
  if (e)
    return i = ub(i), t != null && (i[0] = sb(X(t) ? t(i[0]) : t)), r != null && (i[1] = Dr(X(r) ? r(i[1]) : r)), n != null && (i[2] = Dr(X(n) ? n(i[2]) : n)), Or(nf(i), "rgba");
}
function cb(e, t) {
  var r = ae(e);
  if (r && t != null)
    return r[3] = ha(t), Or(r, "rgba");
}
function Or(e, t) {
  if (!(!e || !e.length)) {
    var r = e[0] + "," + e[1] + "," + e[2];
    return (t === "rgba" || t === "hsva" || t === "hsla") && (r += "," + e[3]), t + "(" + r + ")";
  }
}
function ca(e, t) {
  var r = ae(e);
  return r ? (0.299 * r[0] + 0.587 * r[1] + 0.114 * r[2]) * r[3] / 255 + (1 - r[3]) * t : 0;
}
function vb() {
  return Or([
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255),
    Math.round(Math.random() * 255)
  ], "rgb");
}
var gv = new si(100);
function of(e) {
  if (N(e)) {
    var t = gv.get(e);
    return t || (t = af(e, -0.1), gv.put(e, t)), t;
  } else if (Da(e)) {
    var r = R({}, e);
    return r.colorStops = H(e.colorStops, function(n) {
      return {
        offset: n.offset,
        color: af(n.color, -0.1)
      };
    }), r;
  }
  return e;
}
const db = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  fastLerp: Ry,
  fastMapToColor: fb,
  lerp: Oy,
  lift: af,
  liftColor: of,
  lum: ca,
  mapToColor: hb,
  modifyAlpha: cb,
  modifyHSL: ss,
  parse: ae,
  parseCssFloat: Dr,
  parseCssInt: Ro,
  random: vb,
  stringify: Or,
  toHex: lb
}, Symbol.toStringTag, { value: "Module" }));
function pb(e) {
  return e.type === "linear";
}
function gb(e) {
  return e.type === "radial";
}
(function() {
  return typeof Buffer < "u" && typeof Buffer.from == "function" ? function(e) {
    return Buffer.from(e).toString("base64");
  } : typeof btoa == "function" && typeof unescape == "function" && typeof encodeURIComponent == "function" ? function(e) {
    return btoa(unescape(encodeURIComponent(e)));
  } : function(e) {
    return null;
  };
})();
var sf = Array.prototype.slice;
function nr(e, t, r) {
  return (t - e) * r + e;
}
function Bu(e, t, r, n) {
  for (var i = t.length, a = 0; a < i; a++)
    e[a] = nr(t[a], r[a], n);
  return e;
}
function yb(e, t, r, n) {
  for (var i = t.length, a = i && t[0].length, o = 0; o < i; o++) {
    e[o] || (e[o] = []);
    for (var s = 0; s < a; s++)
      e[o][s] = nr(t[o][s], r[o][s], n);
  }
  return e;
}
function Wa(e, t, r, n) {
  for (var i = t.length, a = 0; a < i; a++)
    e[a] = t[a] + r[a] * n;
  return e;
}
function yv(e, t, r, n) {
  for (var i = t.length, a = i && t[0].length, o = 0; o < i; o++) {
    e[o] || (e[o] = []);
    for (var s = 0; s < a; s++)
      e[o][s] = t[o][s] + r[o][s] * n;
  }
  return e;
}
function mb(e, t) {
  for (var r = e.length, n = t.length, i = r > n ? t : e, a = Math.min(r, n), o = i[a - 1] || { color: [0, 0, 0, 0], offset: 0 }, s = a; s < Math.max(r, n); s++)
    i.push({
      offset: o.offset,
      color: o.color.slice()
    });
}
function _b(e, t, r) {
  var n = e, i = t;
  if (!(!n.push || !i.push)) {
    var a = n.length, o = i.length;
    if (a !== o) {
      var s = a > o;
      if (s)
        n.length = o;
      else
        for (var u = a; u < o; u++)
          n.push(r === 1 ? i[u] : sf.call(i[u]));
    }
    for (var l = n[0] && n[0].length, u = 0; u < n.length; u++)
      if (r === 1)
        isNaN(n[u]) && (n[u] = i[u]);
      else
        for (var f = 0; f < l; f++)
          isNaN(n[u][f]) && (n[u][f] = i[u][f]);
  }
}
function Oo(e) {
  if ($t(e)) {
    var t = e.length;
    if ($t(e[0])) {
      for (var r = [], n = 0; n < t; n++)
        r.push(sf.call(e[n]));
      return r;
    }
    return sf.call(e);
  }
  return e;
}
function ko(e) {
  return e[0] = Math.floor(e[0]) || 0, e[1] = Math.floor(e[1]) || 0, e[2] = Math.floor(e[2]) || 0, e[3] = e[3] == null ? 1 : e[3], "rgba(" + e.join(",") + ")";
}
function Sb(e) {
  return $t(e && e[0]) ? 2 : 1;
}
var Xa = 0, Bo = 1, ky = 2, Xi = 3, uf = 4, lf = 5, mv = 6;
function _v(e) {
  return e === uf || e === lf;
}
function qa(e) {
  return e === Bo || e === ky;
}
var Di = [0, 0, 0, 0], bb = (function() {
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
    var i = this.keyframes, a = i.length, o = !1, s = mv, u = r;
    if ($t(r)) {
      var l = Sb(r);
      s = l, (l === 1 && !vt(r[0]) || l === 2 && !vt(r[0][0])) && (o = !0);
    } else if (vt(r) && !ua(r))
      s = Xa;
    else if (N(r))
      if (!isNaN(+r))
        s = Xa;
      else {
        var f = ae(r);
        f && (u = f, s = Xi);
      }
    else if (Da(r)) {
      var h = R({}, u);
      h.colorStops = H(r.colorStops, function(c) {
        return {
          offset: c.offset,
          color: ae(c.color)
        };
      }), pb(r) ? s = uf : gb(r) && (s = lf), u = h;
    }
    a === 0 ? this.valType = s : (s !== this.valType || s === mv) && (o = !0), this.discrete = this.discrete || o;
    var v = {
      time: t,
      value: u,
      rawValue: r,
      percent: 0
    };
    return n && (v.easing = n, v.easingFunc = X(n) ? n : Qi[n] || Py(n)), i.push(v), v;
  }, e.prototype.prepare = function(t, r) {
    var n = this.keyframes;
    this._needsSort && n.sort(function(y, p) {
      return y.time - p.time;
    });
    for (var i = this.valType, a = n.length, o = n[a - 1], s = this.discrete, u = qa(i), l = _v(i), f = 0; f < a; f++) {
      var h = n[f], v = h.value, c = o.value;
      h.percent = h.time / t, s || (u && f !== a - 1 ? _b(v, c, i) : l && mb(v.colorStops, c.colorStops));
    }
    if (!s && i !== lf && r && this.needsAnimate() && r.needsAnimate() && i === r.valType && !r._finished) {
      this._additiveTrack = r;
      for (var d = n[0].value, f = 0; f < a; f++)
        i === Xa ? n[f].additiveValue = n[f].value - d : i === Xi ? n[f].additiveValue = Wa([], n[f].value, d, -1) : qa(i) && (n[f].additiveValue = i === Bo ? Wa([], n[f].value, d, -1) : yv([], n[f].value, d, -1));
    }
  }, e.prototype.step = function(t, r) {
    if (!this._finished) {
      this._additiveTrack && this._additiveTrack._finished && (this._additiveTrack = null);
      var n = this._additiveTrack != null, i = n ? "additiveValue" : "value", a = this.valType, o = this.keyframes, s = o.length, u = this.propName, l = a === Xi, f, h = this._lastFr, v = Math.min, c, d;
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
        var m = n ? this._additiveValue : l ? Di : t[u];
        if ((qa(a) || l) && !m && (m = this._additiveValue = []), this.discrete)
          t[u] = g < 1 ? c.rawValue : d.rawValue;
        else if (qa(a))
          a === Bo ? Bu(m, c[i], d[i], g) : yb(m, c[i], d[i], g);
        else if (_v(a)) {
          var _ = c[i], S = d[i], b = a === uf;
          t[u] = {
            type: b ? "linear" : "radial",
            x: nr(_.x, S.x, g),
            y: nr(_.y, S.y, g),
            colorStops: H(_.colorStops, function(C, M) {
              var x = S.colorStops[M];
              return {
                offset: nr(C.offset, x.offset, g),
                color: ko(Bu([], C.color, x.color, g))
              };
            }),
            global: S.global
          }, b ? (t[u].x2 = nr(_.x2, S.x2, g), t[u].y2 = nr(_.y2, S.y2, g)) : t[u].r = nr(_.r, S.r, g);
        } else if (l)
          Bu(m, c[i], d[i], g), n || (t[u] = ko(m));
        else {
          var w = nr(c[i], d[i], g);
          n ? this._additiveValue = w : t[u] = w;
        }
        n && this._addToTarget(t);
      }
    }
  }, e.prototype._addToTarget = function(t) {
    var r = this.valType, n = this.propName, i = this._additiveValue;
    r === Xa ? t[n] = t[n] + i : r === Xi ? (ae(t[n], Di), Wa(Di, Di, i, 1), t[n] = ko(Di)) : r === Bo ? Wa(t[n], t[n], i, 1) : r === ky && yv(t[n], t[n], i, 1);
  }, e;
})(), _h = (function() {
  function e(t, r, n, i) {
    if (this._tracks = {}, this._trackKeys = [], this._maxTime = 0, this._started = 0, this._clip = null, this._target = t, this._loop = r, r && i) {
      zs("Can' use additive animation on looped animation.");
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
    return this.whenWithKeys(t, r, mt(r), n);
  }, e.prototype.whenWithKeys = function(t, r, n, i) {
    for (var a = this._tracks, o = 0; o < n.length; o++) {
      var s = n[o], u = a[s];
      if (!u) {
        u = a[s] = new bb(s);
        var l = void 0, f = this._getAdditiveTrack(s);
        if (f) {
          var h = f.keyframes, v = h[h.length - 1];
          l = v && v.value, f.valType === Xi && l && (l = ko(l));
        } else
          l = this._target[s];
        if (l == null)
          continue;
        t > 0 && u.addKeyframe(0, Oo(l), i), this._trackKeys.push(s);
      }
      u.addKeyframe(t, Oo(r[s]), i);
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
        var v = new ab({
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
    return H(this._trackKeys, function(r) {
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
          u && (t[a] = Oo(u.rawValue));
        }
      }
    }
  }, e.prototype.__changeFinalValue = function(t, r) {
    r = r || mt(t);
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
function Kn() {
  return (/* @__PURE__ */ new Date()).getTime();
}
var wb = (function(e) {
  z(t, e);
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
    for (var n = Kn() - this._pausedTime, i = n - this._time, a = this._head; a; ) {
      var o = a.next, s = a.step(n, i);
      s && (a.ondestroy(), this.removeClip(a)), a = o;
    }
    this._time = n, r || (this.trigger("frame", i), this.stage.update && this.stage.update());
  }, t.prototype._startLoop = function() {
    var r = this;
    this._running = !0;
    function n() {
      r._running && (ns(n), !r._paused && r.update());
    }
    ns(n);
  }, t.prototype.start = function() {
    this._running || (this._time = Kn(), this._pausedTime = 0, this._startLoop());
  }, t.prototype.stop = function() {
    this._running = !1;
  }, t.prototype.pause = function() {
    this._paused || (this._pauseStart = Kn(), this._paused = !0);
  }, t.prototype.resume = function() {
    this._paused && (this._pausedTime += Kn() - this._pauseStart, this._paused = !1);
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
    var i = new _h(r, n.loop);
    return this.addAnimator(i), i;
  }, t;
})(qe), Tb = 300, Nu = W.domSupported, Fu = (function() {
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
  }, n = H(e, function(i) {
    var a = i.replace("mouse", "pointer");
    return r.hasOwnProperty(a) ? a : i;
  });
  return {
    mouse: e,
    touch: t,
    pointer: n
  };
})(), Sv = {
  mouse: ["mousemove", "mouseup"],
  pointer: ["pointermove", "pointerup"]
}, bv = !1;
function ff(e) {
  var t = e.pointerType;
  return t === "pen" || t === "touch";
}
function Cb(e) {
  e.touching = !0, e.touchTimer != null && (clearTimeout(e.touchTimer), e.touchTimer = null), e.touchTimer = setTimeout(function() {
    e.touching = !1, e.touchTimer = null;
  }, 700);
}
function zu(e) {
  e && (e.zrByTouch = !0);
}
function xb(e, t) {
  return ce(e.dom, new Mb(e, t), !0);
}
function By(e, t) {
  for (var r = t, n = !1; r && r.nodeType !== 9 && !(n = r.domBelongToZr || r !== t && r === e.painterRoot); )
    r = r.parentNode;
  return n;
}
var Mb = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.stopPropagation = Et, this.stopImmediatePropagation = Et, this.preventDefault = Et, this.type = r.type, this.target = this.currentTarget = t.dom, this.pointerType = r.pointerType, this.clientX = r.clientX, this.clientY = r.clientY;
  }
  return e;
})(), Ce = {
  mousedown: function(e) {
    e = ce(this.dom, e), this.__mayPointerCapture = [e.zrX, e.zrY], this.trigger("mousedown", e);
  },
  mousemove: function(e) {
    e = ce(this.dom, e);
    var t = this.__mayPointerCapture;
    t && (e.zrX !== t[0] || e.zrY !== t[1]) && this.__togglePointerCapture(!0), this.trigger("mousemove", e);
  },
  mouseup: function(e) {
    e = ce(this.dom, e), this.__togglePointerCapture(!1), this.trigger("mouseup", e);
  },
  mouseout: function(e) {
    e = ce(this.dom, e);
    var t = e.toElement || e.relatedTarget;
    By(this, t) || (this.__pointerCapturing && (e.zrEventControl = "no_globalout"), this.trigger("mouseout", e));
  },
  wheel: function(e) {
    bv = !0, e = ce(this.dom, e), this.trigger("mousewheel", e);
  },
  mousewheel: function(e) {
    bv || (e = ce(this.dom, e), this.trigger("mousewheel", e));
  },
  touchstart: function(e) {
    e = ce(this.dom, e), zu(e), this.__lastTouchMoment = /* @__PURE__ */ new Date(), this.handler.processGesture(e, "start"), Ce.mousemove.call(this, e), Ce.mousedown.call(this, e);
  },
  touchmove: function(e) {
    e = ce(this.dom, e), zu(e), this.handler.processGesture(e, "change"), Ce.mousemove.call(this, e);
  },
  touchend: function(e) {
    e = ce(this.dom, e), zu(e), this.handler.processGesture(e, "end"), Ce.mouseup.call(this, e), +/* @__PURE__ */ new Date() - +this.__lastTouchMoment < Tb && Ce.click.call(this, e);
  },
  pointerdown: function(e) {
    Ce.mousedown.call(this, e);
  },
  pointermove: function(e) {
    ff(e) || Ce.mousemove.call(this, e);
  },
  pointerup: function(e) {
    Ce.mouseup.call(this, e);
  },
  pointerout: function(e) {
    ff(e) || Ce.mouseout.call(this, e);
  }
};
T(["click", "dblclick", "contextmenu"], function(e) {
  Ce[e] = function(t) {
    t = ce(this.dom, t), this.trigger(e, t);
  };
});
var hf = {
  pointermove: function(e) {
    ff(e) || hf.mousemove.call(this, e);
  },
  pointerup: function(e) {
    hf.mouseup.call(this, e);
  },
  mousemove: function(e) {
    this.trigger("mousemove", e);
  },
  mouseup: function(e) {
    var t = this.__pointerCapturing;
    this.__togglePointerCapture(!1), this.trigger("mouseup", e), t && (e.zrEventControl = "only_globalout", this.trigger("mouseout", e));
  }
};
function Db(e, t) {
  var r = t.domHandlers;
  W.pointerEventsSupported ? T(Fu.pointer, function(n) {
    No(t, n, function(i) {
      r[n].call(e, i);
    });
  }) : (W.touchEventsSupported && T(Fu.touch, function(n) {
    No(t, n, function(i) {
      r[n].call(e, i), Cb(t);
    });
  }), T(Fu.mouse, function(n) {
    No(t, n, function(i) {
      i = mh(i), t.touching || r[n].call(e, i);
    });
  }));
}
function Ib(e, t) {
  W.pointerEventsSupported ? T(Sv.pointer, r) : W.touchEventsSupported || T(Sv.mouse, r);
  function r(n) {
    function i(a) {
      a = mh(a), By(e, a.target) || (a = xb(e, a), t.domHandlers[n].call(e, a));
    }
    No(t, n, i, { capture: !0 });
  }
}
function No(e, t, r, n) {
  e.mounted[t] = r, e.listenerOpts[t] = n, O1(e.domTarget, t, r, n);
}
function Hu(e) {
  var t = e.mounted;
  for (var r in t)
    t.hasOwnProperty(r) && k1(e.domTarget, r, t[r], e.listenerOpts[r]);
  e.mounted = {};
}
var wv = /* @__PURE__ */ (function() {
  function e(t, r) {
    this.mounted = {}, this.listenerOpts = {}, this.touching = !1, this.domTarget = t, this.domHandlers = r;
  }
  return e;
})(), Ab = (function(e) {
  z(t, e);
  function t(r, n) {
    var i = e.call(this) || this;
    return i.__pointerCapturing = !1, i.dom = r, i.painterRoot = n, i._localHandlerScope = new wv(r, Ce), Nu && (i._globalHandlerScope = new wv(document, hf)), Db(i, i._localHandlerScope), i;
  }
  return t.prototype.dispose = function() {
    Hu(this._localHandlerScope), Nu && Hu(this._globalHandlerScope);
  }, t.prototype.setCursor = function(r) {
    this.dom.style && (this.dom.style.cursor = r || "default");
  }, t.prototype.__togglePointerCapture = function(r) {
    if (this.__mayPointerCapture = null, Nu && +this.__pointerCapturing ^ +r) {
      this.__pointerCapturing = r;
      var n = this._globalHandlerScope;
      r ? Ib(this, n) : Hu(n);
    }
  }, t;
})(qe), Ny = 1;
W.hasGlobalWindow && (Ny = Math.max(window.devicePixelRatio || window.screen && window.screen.deviceXDPI / window.screen.logicalXDPI || 1, 1));
var us = Ny, cf = 0.4, vf = "#333", df = "#ccc", Pb = "#eee", Tv = xn, Cv = 5e-5;
function Gr(e) {
  return e > Cv || e < -Cv;
}
var Ur = [], Rn = [], Vu = Se(), Gu = Math.abs, Pa = (function() {
  function e() {
  }
  return e.prototype.getLocalTransform = function(t) {
    return Lb(this, t);
  }, e.prototype.setPosition = function(t) {
    this.x = t[0], this.y = t[1];
  }, e.prototype.setScale = function(t) {
    this.scaleX = t[0], this.scaleY = t[1];
  }, e.prototype.setSkew = function(t) {
    this.skewX = t[0], this.skewY = t[1];
  }, e.prototype.setOrigin = function(t) {
    this.originX = t[0], this.originY = t[1];
  }, e.prototype.needLocalTransform = function() {
    return Gr(this.rotation) || Gr(this.x) || Gr(this.y) || Gr(this.scaleX - 1) || Gr(this.scaleY - 1) || Gr(this.skewX) || Gr(this.skewY);
  }, e.prototype.updateTransform = function() {
    var t = this.parent && this.parent.transform, r = this.needLocalTransform(), n = this.transform;
    if (!(r || t)) {
      n && (Tv(n), this.invTransform = null);
      return;
    }
    n = n || Se(), r ? this.getLocalTransform(n) : Tv(n), t && (r ? gn(n, t, n) : Aa(n, t)), this.transform = n, this._resolveGlobalScaleRatio(n), this.invTransform = this.invTransform || Se(), di(this.invTransform, n);
  }, e.prototype._resolveGlobalScaleRatio = function(t) {
    var r = this.globalScaleRatio;
    if (r != null && r !== 1) {
      this.getGlobalScale(Ur);
      var n = Ur[0] < 0 ? -1 : 1, i = Ur[1] < 0 ? -1 : 1, a = ((Ur[0] - n) * r + n) / Ur[0] || 0, o = ((Ur[1] - i) * r + i) / Ur[1] || 0;
      t[0] *= a, t[1] *= a, t[2] *= o, t[3] *= o;
    }
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
      t && t.transform && (t.invTransform = t.invTransform || Se(), gn(Rn, t.invTransform, r), r = Rn);
      var n = this.originX, i = this.originY;
      (n || i) && (Vu[4] = n, Vu[5] = i, gn(Rn, r, Vu), Rn[4] -= n, Rn[5] -= i, r = Rn), this.setLocalTransform(r);
    }
  }, e.prototype.getGlobalScale = function(t) {
    var r = this.transform;
    return t = t || [], r ? (t[0] = Math.sqrt(r[0] * r[0] + r[1] * r[1]), t[1] = Math.sqrt(r[2] * r[2] + r[3] * r[3]), r[0] < 0 && (t[0] = -t[0]), r[3] < 0 && (t[1] = -t[1]), t) : (t[0] = 1, t[1] = 1, t);
  }, e.prototype.transformCoordToLocal = function(t, r) {
    var n = [t, r], i = this.invTransform;
    return i && ie(n, n, i), n;
  }, e.prototype.transformCoordToGlobal = function(t, r) {
    var n = [t, r], i = this.transform;
    return i && ie(n, n, i), n;
  }, e.prototype.getLineScale = function() {
    var t = this.transform;
    return t && Gu(t[0] - 1) > 1e-10 && Gu(t[3] - 1) > 1e-10 ? Math.sqrt(Gu(t[0] * t[3] - t[2] * t[1])) : 1;
  }, e.prototype.copyTransform = function(t) {
    ls(this, t);
  }, e.getLocalTransform = function(t, r) {
    r = r || [];
    var n = t.originX || 0, i = t.originY || 0, a = t.scaleX, o = t.scaleY, s = t.anchorX, u = t.anchorY, l = t.rotation || 0, f = t.x, h = t.y, v = t.skewX ? Math.tan(t.skewX) : 0, c = t.skewY ? Math.tan(-t.skewY) : 0;
    if (n || i || s || u) {
      var d = n + s, y = i + u;
      r[4] = -d * a - v * y * o, r[5] = -y * o - c * d * a;
    } else
      r[4] = r[5] = 0;
    return r[0] = a, r[3] = o, r[1] = c * a, r[2] = v * o, l && Gs(r, r, l), r[4] += n + f, r[5] += i + h, r;
  }, e.initDefaultProps = (function() {
    var t = e.prototype;
    t.scaleX = t.scaleY = t.globalScaleRatio = 1, t.x = t.y = t.originX = t.originY = t.skewX = t.skewY = t.rotation = t.anchorX = t.anchorY = 0;
  })(), e;
})(), Lb = Pa.getLocalTransform, Us = [
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
function ls(e, t) {
  return ay(e, t, Us);
}
function He(e) {
  $a || ($a = new si(100)), e = e || Pr;
  var t = $a.get(e);
  return t || (t = {
    font: e,
    strWidthCache: new si(500),
    asciiWidthMap: null,
    asciiWidthMapTried: !1,
    stWideCharWidth: Lt.measureText("国", e).width,
    asciiCharWidth: Lt.measureText("a", e).width
  }, $a.put(e, t)), t;
}
var $a;
function Eb(e) {
  if (!(Uu >= xv)) {
    e = e || Pr;
    for (var t = [], r = +/* @__PURE__ */ new Date(), n = 0; n <= 127; n++)
      t[n] = Lt.measureText(String.fromCharCode(n), e).width;
    var i = +/* @__PURE__ */ new Date() - r;
    return i > 16 ? Uu = xv : i > 2 && Uu++, t;
  }
}
var Uu = 0, xv = 5;
function Fy(e, t) {
  return e.asciiWidthMapTried || (e.asciiWidthMap = Eb(e.font), e.asciiWidthMapTried = !0), 0 <= t && t <= 127 ? e.asciiWidthMap != null ? e.asciiWidthMap[t] : e.asciiCharWidth : e.stWideCharWidth;
}
function Ve(e, t) {
  var r = e.strWidthCache, n = r.get(t);
  return n == null && (n = Lt.measureText(t, e.font).width, r.put(t, n)), n;
}
function Mv(e, t, r, n) {
  var i = Ve(He(t), e), a = Ys(t), o = ui(0, i, r), s = yn(0, a, n), u = new Y(o, s, i, a);
  return u;
}
function zy(e, t, r, n) {
  var i = ((e || "") + "").split(`
`), a = i.length;
  if (a === 1)
    return Mv(i[0], t, r, n);
  for (var o = new Y(0, 0, 0, 0), s = 0; s < i.length; s++) {
    var u = Mv(i[s], t, r, n);
    s === 0 ? o.copy(u) : o.union(u);
  }
  return o;
}
function ui(e, t, r, n) {
  return r === "right" ? n ? e += t : e -= t : r === "center" && (n ? e += t / 2 : e -= t / 2), e;
}
function yn(e, t, r, n) {
  return r === "middle" ? n ? e += t / 2 : e -= t / 2 : r === "bottom" && (n ? e += t : e -= t), e;
}
function Ys(e) {
  return He(e).stWideCharWidth;
}
function li(e, t) {
  return typeof e == "string" ? e.lastIndexOf("%") >= 0 ? parseFloat(e) / 100 * t : parseFloat(e) : e;
}
function Hy(e, t, r) {
  var n = t.position || "inside", i = t.distance != null ? t.distance : 5, a = r.height, o = r.width, s = a / 2, u = r.x, l = r.y, f = "left", h = "top";
  if (n instanceof Array)
    u += li(n[0], r.width), l += li(n[1], r.height), f = null, h = null;
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
var Yu = "__zr_normal__", Wu = Us.concat(["ignore"]), Rb = Ye(Us, function(e, t) {
  return e[t] = !0, e;
}, { ignore: !1 }), On = {}, Ob = new Y(0, 0, 0, 0), Za = [], Fo = 0, Ws = 1, Xs = (function() {
  function e(t) {
    this.id = vh(), this.animators = [], this.currentStates = [], this.states = {}, this._init(t);
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
      a.copyTransform(r);
      var f = n.position != null, h = n.autoOverflowArea, v = void 0;
      if ((h || f) && (v = Ob, n.layoutRect ? v.copy(n.layoutRect) : v.copy(this.getBoundingRect()), i || v.applyTransform(this.transform)), f) {
        this.calculateTextPosition ? this.calculateTextPosition(On, n, v) : Hy(On, n, v), a.x = On.x, a.y = On.y, o = On.align, s = On.verticalAlign;
        var c = n.origin;
        if (c && n.rotation != null) {
          var d = void 0, y = void 0;
          c === "center" ? (d = v.width * 0.5, y = v.height * 0.5) : (d = li(c[0], v.width), y = li(c[1], v.height)), l = !0, a.originX = -a.x + d + (i ? 0 : v.x), a.originY = -a.y + y + (i ? 0 : v.y);
        }
      }
      n.rotation != null && (a.rotation = n.rotation);
      var p = n.offset;
      p && (a.x += p[0], a.y += p[1], l || (a.originX = -p[0], a.originY = -p[1]));
      var g = this._innerTextDefaultStyle || (this._innerTextDefaultStyle = {});
      if (h) {
        var m = g.overflowRect = g.overflowRect || new Y(0, 0, 0, 0);
        a.getLocalTransform(Za), di(Za, Za), Y.copy(m, v), m.applyTransform(Za);
      } else
        g.overflowRect = null;
      var _ = n.inside == null ? typeof n.position == "string" && n.position.indexOf("inside") >= 0 : n.inside, S = void 0, b = void 0, w = void 0;
      _ && this.canBeInsideText() ? (S = n.insideFill, b = n.insideStroke, (S == null || S === "auto") && (S = this.getInsideTextFill()), (b == null || b === "auto") && (b = this.getInsideTextStroke(S), w = !0)) : (S = n.outsideFill, b = n.outsideStroke, (S == null || S === "auto") && (S = this.getOutsideFill()), (b == null || b === "auto") && (b = this.getOutsideStroke(S), w = !0)), S = S || "#000", (S !== g.fill || b !== g.stroke || w !== g.autoStroke || o !== g.align || s !== g.verticalAlign) && (u = !0, g.fill = S, g.stroke = b, g.autoStroke = w, g.align = o, g.verticalAlign = s, r.setDefaultTextStyle(g)), r.__dirty |= Qt, u && r.dirtyStyle(!0);
    }
  }, e.prototype.canBeInsideText = function() {
    return !0;
  }, e.prototype.getInsideTextFill = function() {
    return "#fff";
  }, e.prototype.getInsideTextStroke = function(t) {
    return "#000";
  }, e.prototype.getOutsideFill = function() {
    return this.__zr && this.__zr.isDarkMode() ? df : vf;
  }, e.prototype.getOutsideStroke = function(t) {
    var r = this.__zr && this.__zr.getBackgroundColor(), n = typeof r == "string" && ae(r);
    n || (n = [255, 255, 255, 1]);
    for (var i = n[3], a = this.__zr.isDarkMode(), o = 0; o < 3; o++)
      n[o] = n[o] * i + (a ? 0 : 255) * (1 - i);
    return n[3] = 1, Or(n, "rgba");
  }, e.prototype.traverse = function(t, r) {
  }, e.prototype.attrKV = function(t, r) {
    t === "textConfig" ? this.setTextConfig(r) : t === "textContent" ? this.setTextContent(r) : t === "clipPath" ? this.setClipPath(r) : t === "extra" ? (this.extra = this.extra || {}, R(this.extra, r)) : this[t] = r;
  }, e.prototype.hide = function() {
    this.ignore = !0, this.markRedraw();
  }, e.prototype.show = function() {
    this.ignore = !1, this.markRedraw();
  }, e.prototype.attr = function(t, r) {
    if (typeof t == "string")
      this.attrKV(t, r);
    else if (F(t))
      for (var n = t, i = mt(n), a = 0; a < i.length; a++) {
        var o = i[a];
        this.attrKV(o, t[o]);
      }
    return this.markRedraw(), this;
  }, e.prototype.saveCurrentToNormalState = function(t) {
    this._innerSaveToNormal(t);
    for (var r = this._normalState, n = 0; n < this.animators.length; n++) {
      var i = this.animators[n], a = i.__fromStateTransition;
      if (!(i.getLoop() || a && a !== Yu)) {
        var o = i.targetName, s = o ? r[o] : r;
        i.saveTo(s);
      }
    }
  }, e.prototype._innerSaveToNormal = function(t) {
    var r = this._normalState;
    r || (r = this._normalState = {}), t.textConfig && !r.textConfig && (r.textConfig = this.textConfig), this._savePrimaryToNormal(t, r, Wu);
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
    this.useState(Yu, !1, t);
  }, e.prototype.useState = function(t, r, n, i) {
    var a = t === Yu, o = this.hasState();
    if (!(!o && a)) {
      var s = this.currentStates, u = this.stateTransition;
      if (!(st(s, t) >= 0 && (r || s.length === 1))) {
        var l;
        if (this.stateProxy && !a && (l = this.stateProxy(t)), l || (l = this.states && this.states[t]), !l && !a) {
          zs("State " + t + " not exists.");
          return;
        }
        a || this.saveCurrentToNormalState(l);
        var f = this._textContent, h = Dv(this, f, l, i);
        h && !this.__inHover && (this.__inHover = h), this._applyStateObj(t, l, this._normalState, r, Av(this, n, u), u);
        var v = this._textGuide;
        return f && f.useState(t, r, n, !!h), v && v.useState(t, r, n, !!h), a ? (this.currentStates = [], this._normalState = {}) : r ? this.currentStates.push(t) : this.currentStates = [t], this._updateAnimationTargets(), this.markRedraw(), !h && this.__inHover && (this.__inHover = Fo, this.__dirty &= ~Qt), l;
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
      var h = i[o - 1], v = this._textContent, c = Dv(this, v, h, n);
      c && !this.__inHover && (this.__inHover = c);
      var d = this._mergeStates(i), y = this.stateTransition;
      this.saveCurrentToNormalState(d), this._applyStateObj(t.join(","), d, this._normalState, !1, Av(this, r, y), y);
      var p = this._textGuide;
      v && v.useStates(t, r, !!c), p && p.useStates(t, r, !!c), this._updateAnimationTargets(), this.currentStates = t.slice(), this.markRedraw(), !c && this.__inHover && (this.__inHover = Fo, this.__dirty &= ~Qt);
    }
  }, e.prototype.isSilent = function() {
    for (var t = this; t; ) {
      if (t.silent)
        return !0;
      var r = t.__hostTarget;
      t = r ? t.ignoreHostSilent ? null : r : t.parent;
    }
    return !1;
  }, e.prototype._updateAnimationTargets = function() {
    for (var t = 0; t < this.animators.length; t++) {
      var r = this.animators[t];
      r.targetName && r.changeTarget(this[r.targetName]);
    }
  }, e.prototype.removeState = function(t) {
    var r = st(this.currentStates, t);
    if (r >= 0) {
      var n = this.currentStates.slice();
      n.splice(r, 1), this.useStates(n);
    }
  }, e.prototype.replaceState = function(t, r, n) {
    var i = this.currentStates.slice(), a = st(i, t), o = st(i, r) >= 0;
    a >= 0 ? o ? i.splice(a, 1) : i[a] = r : n && !o && i.push(r), this.useStates(i);
  }, e.prototype.toggleState = function(t, r) {
    r ? this.useState(t, !0) : this.removeState(t);
  }, e.prototype._mergeStates = function(t) {
    for (var r = {}, n, i = 0; i < t.length; i++) {
      var a = t[i];
      R(r, a), a.textConfig && (n = n || {}, R(n, a.textConfig));
    }
    return n && (r.textConfig = n), r;
  }, e.prototype._applyStateObj = function(t, r, n, i, a, o) {
    if (this.__inHover !== Ws) {
      var s = !(r && i);
      r && r.textConfig ? (this.textConfig = R({}, i ? this.textConfig : n.textConfig), R(this.textConfig, r.textConfig)) : s && n.textConfig && (this.textConfig = n.textConfig);
      for (var u = {}, l = !1, f = 0; f < Wu.length; f++) {
        var h = Wu[f], v = a && Rb[h];
        r && r[h] != null ? v ? (l = !0, u[h] = r[h]) : this[h] = r[h] : s && n[h] != null && (v ? (l = !0, u[h] = n[h]) : this[h] = n[h]);
      }
      if (!a)
        for (var f = 0; f < this.animators.length; f++) {
          var c = this.animators[f], d = c.targetName;
          c.getLoop() || c.__changeFinalValue(d ? (r || n)[d] : r || n);
        }
      l && this._transitionState(t, u, o);
    }
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
    r !== t && (r && r !== t && this.removeTextContent(), t.innerTransformable = new Pa(), this._attachComponent(t), this._textContent = t, this.markRedraw());
  }, e.prototype.setTextConfig = function(t) {
    this.textConfig || (this.textConfig = {}), R(this.textConfig, t), this.markRedraw();
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
    this.__dirty |= Qt;
    var t = this.__zr;
    t && (this.__inHover ? t.refreshHover() : t.refresh()), this.__hostTarget && this.__hostTarget.markRedraw();
  }, e.prototype.dirty = function() {
    this.markRedraw();
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
    var i = t ? this[t] : this, a = new _h(i, r, n);
    return t && (a.targetName = t), this.addAnimator(a, t), a;
  }, e.prototype.addAnimator = function(t, r) {
    var n = this.__zr, i = this;
    t.during(function() {
      i.updateDuringAnimation(r);
    }).done(function() {
      var a = i.animators, o = st(a, t);
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
    Xu(this, t, r, n);
  }, e.prototype.animateFrom = function(t, r, n) {
    Xu(this, t, r, n, !0);
  }, e.prototype._transitionState = function(t, r, n, i) {
    for (var a = Xu(this, r, n, i), o = 0; o < a.length; o++)
      a[o].__fromStateTransition = t;
  }, e.prototype.getBoundingRect = function() {
    return null;
  }, e.prototype.getPaintRect = function() {
    return null;
  }, e.initDefaultProps = (function() {
    var t = e.prototype;
    t.type = "element", t.name = "", t.ignore = t.silent = t.ignoreHostSilent = t.isGroup = t.draggable = t.dragging = t.ignoreClip = !1, t.__inHover = Fo, t.__dirty = Qt;
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
Ae(Xs, qe);
Ae(Xs, Pa);
function Xu(e, t, r, n, i) {
  r = r || {};
  var a = [];
  Vy(e, "", e, t, r, n, a, i);
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
function qu(e, t, r) {
  for (var n = 0; n < r; n++)
    e[n] = t[n];
}
function kb(e) {
  return $t(e[0]);
}
function Bb(e, t, r) {
  if ($t(t[r]))
    if ($t(e[r]) || (e[r] = []), Zt(t[r])) {
      var n = t[r].length;
      e[r].length !== n && (e[r] = new t[r].constructor(n), qu(e[r], t[r], n));
    } else {
      var i = t[r], a = e[r], o = i.length;
      if (kb(i))
        for (var s = i[0].length, u = 0; u < o; u++)
          a[u] ? qu(a[u], i[u], s) : a[u] = Array.prototype.slice.call(i[u]);
      else
        qu(a, i, o);
      a.length = i.length;
    }
  else
    e[r] = t[r];
}
function Nb(e, t) {
  return e === t || $t(e) && $t(t) && Fb(e, t);
}
function Fb(e, t) {
  var r = e.length;
  if (r !== t.length)
    return !1;
  for (var n = 0; n < r; n++)
    if (e[n] !== t[n])
      return !1;
  return !0;
}
function Vy(e, t, r, n, i, a, o, s) {
  for (var u = mt(n), l = i.duration, f = i.delay, h = i.additive, v = i.setToFinal, c = !F(a), d = e.animators, y = [], p = 0; p < u.length; p++) {
    var g = u[p], m = n[g];
    if (m != null && r[g] != null && (c || a[g]))
      if (F(m) && !$t(m) && !Da(m)) {
        if (t) {
          s || (r[g] = m, e.updateDuringAnimation(t));
          continue;
        }
        Vy(e, g, r[g], m, i, a && a[g], o, s);
      } else
        y.push(g);
    else s || (r[g] = m, e.updateDuringAnimation(t), y.push(g));
  }
  var _ = y.length;
  if (!h && _)
    for (var S = 0; S < d.length; S++) {
      var b = d[S];
      if (b.targetName === t) {
        var w = b.stopTracks(y);
        if (w) {
          var C = st(d, b);
          d.splice(C, 1);
        }
      }
    }
  if (i.force || (y = xt(y, function(I) {
    return !Nb(n[I], r[I]);
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
        D[g] = Oo(r[g]), Bb(r, n, g);
      }
    }
    var b = new _h(r, !1, !1, h ? xt(d, function(L) {
      return L.targetName === t;
    }) : null);
    b.targetName = t, i.scope && (b.scope = i.scope), v && M && b.whenWithKeys(0, M, y), D && b.whenWithKeys(0, D, y), b.whenWithKeys(l ?? 500, s ? x : n, y).delay(f || 0), e.addAnimator(b, t), o.push(b);
  }
}
function Dv(e, t, r, n) {
  return !(r && r.hoverLayer || n) || Iv(e) || t && Iv(t) ? Fo : Ws;
}
function Iv(e) {
  return e.type === "text" || e.type === "tspan";
}
function Av(e, t, r) {
  return !t && !e.__inHover && r && r.duration > 0;
}
var oe = (function(e) {
  z(t, e);
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
    var i = st(this._children, r);
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
    var n = this.__zr, i = this._children, a = st(i, r);
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
    for (var n = new Y(0, 0, 0, 0), i = r || this._children, a = [], o = null, s = 0; s < i.length; s++) {
      var u = i[s];
      if (!(u.ignore || u.invisible)) {
        var l = u.getBoundingRect(), f = u.getLocalTransform(a);
        f ? (Y.applyTransform(n, l, f), o = o || n.clone(), o.union(n)) : (o = o || l.clone(), o.union(l));
      }
    }
    return o || n;
  }, t;
})(Xs);
oe.prototype.type = "group";
/*!
* ZRender, a high performance 2d drawing library.
*
* Copyright (c) 2013, Baidu Inc.
* All rights reserved.
*
* LICENSE
* https://github.com/ecomfe/zrender/blob/master/LICENSE
*/
var zo = {}, fn = {};
function zb(e) {
  delete fn[e];
}
function Hb(e) {
  if (!e)
    return !1;
  if (typeof e == "string")
    return ca(e, 1) < cf;
  if (e.colorStops) {
    for (var t = e.colorStops, r = 0, n = t.length, i = 0; i < n; i++)
      r += ca(t[i].color, 1);
    return r /= n, r < cf;
  }
  return !1;
}
var Vb = (function() {
  function e(t, r, n) {
    var i = this;
    this._sleepAfterStill = 10, this._stillFrameAccum = 0, this._needsRefresh = !0, this._needsRefreshHover = !1, this._darkMode = !1, n = n || {}, this.dom = r, this.id = t;
    var a = new Q1(), o = n.renderer || "canvas";
    zo[o] || (o = mt(zo)[0]), n.useDirtyRect = n.useDirtyRect == null ? !1 : n.useDirtyRect;
    var s = new zo[o](r, a, n, t), u = n.ssr || s.ssrOnly;
    this.storage = a, this.painter = s;
    var l = !W.node && !W.worker && !u ? new Ab(s.getViewportRoot(), s.root) : null, f = n.useCoarsePointer, h = f == null || f === "auto" ? W.touchEventsSupported : !!f, v = 44, c;
    h && (c = G(n.pointerSize, v)), this.handler = new wy(a, s, l, s.root, c), this.animation = new wb({
      stage: {
        update: u ? null : function() {
          return i._flush(!1);
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
    this._disposed || (this.painter.setBackgroundColor && this.painter.setBackgroundColor(t), this.refresh(), this._backgroundColor = t, this._darkMode = Hb(t));
  }, e.prototype.getBackgroundColor = function() {
    return this._backgroundColor;
  }, e.prototype.setDarkMode = function(t) {
    this._darkMode = t;
  }, e.prototype.isDarkMode = function() {
    return this._darkMode;
  }, e.prototype.refreshImmediately = function(t) {
    this._disposed || this._refresh({
      animUpdate: !t,
      refresh: !0,
      refreshHover: !1
    });
  }, e.prototype._refresh = function(t) {
    t.animUpdate && this.animation.update(!0), this._needsRefresh = this._needsRefreshHover = !1, this.painter.refresh({
      refresh: t.refresh,
      refreshHover: t.refreshHover
    }), this._needsRefresh = this._needsRefreshHover = !1;
  }, e.prototype.refresh = function() {
    this._disposed || (this._needsRefresh = !0, this.animation.start());
  }, e.prototype.flush = function() {
    this._disposed || this._flush(!0);
  }, e.prototype._flush = function(t) {
    var r, n = Kn(), i = this._needsRefresh, a = this._needsRefreshHover;
    (i || a) && (r = !0, this._refresh({
      animUpdate: t,
      refresh: i,
      refreshHover: a
    }));
    var o = Kn();
    r ? (this._stillFrameAccum = 0, this.trigger("rendered", {
      elapsedTime: o - n
    })) : this._sleepAfterStill > 0 && (this._stillFrameAccum++, this._stillFrameAccum > this._sleepAfterStill && this.animation.stop());
  }, e.prototype.setSleepAfterStill = function(t) {
    this._sleepAfterStill = t;
  }, e.prototype.wakeUp = function() {
    this._disposed || (this.animation.start(), this._stillFrameAccum = 0);
  }, e.prototype.refreshHover = function() {
    this._needsRefreshHover = !0;
  }, e.prototype.refreshHoverImmediately = function() {
    this._disposed || this._refresh({
      animUpdate: !1,
      refresh: !1,
      refreshHover: !0
    });
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
    this._disposed || (this.animation.stop(), this.clear(), this.storage.dispose(), this.painter.dispose(), this.handler.dispose(), this.animation = this.storage = this.painter = this.handler = null, this._disposed = !0, zb(this.id));
  }, e;
})();
function pf(e, t) {
  var r = new Vb(vh(), e, t);
  return fn[r.id] = r, r;
}
function Gb(e) {
  e.dispose();
}
function Ub() {
  for (var e in fn)
    fn.hasOwnProperty(e) && fn[e].dispose();
  fn = {};
}
function Yb(e) {
  return fn[e];
}
function Gy(e, t) {
  zo[e] = t;
}
var gf;
function Wb(e) {
  if (typeof gf == "function")
    return gf(e);
}
function Uy(e) {
  gf = e;
}
var Xb = "6.1.0";
const qb = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  dispose: Gb,
  disposeAll: Ub,
  getElementSSRData: Wb,
  getInstance: Yb,
  init: pf,
  registerPainter: Gy,
  registerSSRDataGetter: Uy,
  version: Xb
}, Symbol.toStringTag, { value: "Module" }));
var Pv = 1e-4, fs = 20;
function $b(e) {
  return e.replace(/^\s+|\s+$/g, "");
}
var Jt = Math.min, gt = Math.max, Ft = Math.abs, ur = Math.round, Mn = Math.floor, qs = Math.ceil, Dn = Math.pow, wn = Math.log, va = Math.LN10, Zb = Math.PI, Kb = Math.random;
function yf(e, t, r, n) {
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
var xe = jb;
function jb(e, t, r) {
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
  return Qb(e, t, r);
}
function Qb(e, t, r) {
  return N(e) ? Jb(e) ? parseFloat(e) / 100 * t + (r || 0) : parseFloat(e) : e == null ? NaN : +e;
}
function Jb(e) {
  return !!$b(e).match(/%$/);
}
function It(e, t, r) {
  return isNaN(t) ? r ? "" + e : +e : (t = Jt(gt(0, t), fs), e = (+e).toFixed(t), r ? e : +e);
}
function tw(e, t, r) {
  return t == null && (t = 10), It(e, t, r);
}
function Sh(e) {
  return e.sort(function(t, r) {
    return t - r;
  }), e;
}
function or(e) {
  if (e = +e, isNaN(e))
    return 0;
  if (e > 1e-14) {
    for (var t = 1, r = 0; r < 15; r++, t *= 10)
      if (ur(e * t) / t === e)
        return r;
  }
  return Yy(e);
}
function Yy(e) {
  var t = e.toString().toLowerCase(), r = t.indexOf("e"), n = r > 0 ? +t.slice(r + 1) : 0, i = r > 0 ? r : t.length, a = t.indexOf("."), o = a < 0 ? 0 : i - 1 - a;
  return gt(0, o - n);
}
function ew(e, t) {
  var r = Mn(wn(e[1] - e[0]) / va), n = ur(wn(Ft(t[1] - t[0])) / va), i = Jt(gt(-r + n, 0), fs);
  return isFinite(i) ? i : fs;
}
function kE(e, t, r) {
  var n = Ft(e[1] - e[0]);
  if (!isFinite(n) || n === 0)
    return NaN;
  var i = wn(2 * Ft(r || 1) * Ft(n)) / va, a = wn(Ft(t)) / va, o = gt(0, qs(-i + a));
  return isFinite(o) || (o = NaN), o;
}
function rw(e, t, r) {
  if (!e[t])
    return 0;
  var n = nw(e, r);
  return n[t] || 0;
}
function nw(e, t) {
  var r = Ye(e, function(c, d) {
    return c + (isNaN(d) ? 0 : d);
  }, 0);
  if (r === 0)
    return [];
  for (var n = Dn(10, t), i = H(e, function(c) {
    return (isNaN(c) ? 0 : c) / r * n * 100;
  }), a = n * 100, o = H(i, function(c) {
    return Mn(c);
  }), s = Ye(o, function(c, d) {
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
function iw(e, t) {
  var r = gt(or(e), or(t)), n = e + t;
  return r > fs ? n : It(n, r);
}
var aw = Dn(2, 53) - 1;
function bh(e) {
  var t = Zb * 2;
  return (e % t + t) % t;
}
function da(e) {
  return e > -Pv && e < Pv;
}
var ow = /^(?:(\d{4})(?:[-\/](\d{1,2})(?:[-\/](\d{1,2})(?:[T ](\d{1,2})(?::(\d{1,2})(?::(\d{1,2})(?:[.,](\d+))?)?)?(Z|[\+\-]\d\d:?\d\d)?)?)?)?)?$/;
function cr(e) {
  if (e instanceof Date)
    return e;
  if (N(e)) {
    var t = ow.exec(e);
    if (!t)
      return /* @__PURE__ */ new Date(NaN);
    if (t[8]) {
      var r = +t[4] || 0;
      return t[8].toUpperCase() !== "Z" && (r -= +t[8].slice(0, 3)), new Date(Date.UTC(+t[1], +(t[2] || 1) - 1, +t[3] || 1, r, +(t[5] || 0), +t[6] || 0, t[7] ? +t[7].substring(0, 3) : 0));
    } else
      return new Date(+t[1], +(t[2] || 1) - 1, +t[3] || 1, +t[4] || 0, +(t[5] || 0), +t[6] || 0, t[7] ? +t[7].substring(0, 3) : 0);
  } else if (e == null)
    return /* @__PURE__ */ new Date(NaN);
  return new Date(ur(e));
}
function Wy(e) {
  return Dn(10, $s(e));
}
function $s(e) {
  if (e === 0)
    return 0;
  var t = Mn(wn(e) / va);
  return e / Dn(10, t) >= 10 && t++, t;
}
var sw = 2;
function wh(e, t) {
  var r = $s(e), n = Dn(10, r), i = e / n, a;
  return t === sw ? a = 1 : t ? i < 1.5 ? a = 1 : i < 2.5 ? a = 2 : i < 4 ? a = 3 : i < 7 ? a = 5 : a = 10 : i < 1 ? a = 1 : i < 2 ? a = 2 : i < 3 ? a = 3 : i < 5 ? a = 5 : a = 10, e = a * n, It(e, -r);
}
function uw(e, t) {
  var r = (e.length - 1) * t + 1, n = Mn(r), i = +e[n - 1], a = r - n;
  return a ? i + a * (e[n] - i) : i;
}
function lw(e) {
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
function lr(e) {
  var t = parseFloat(e);
  return t == e && (t !== 0 || !N(e) || e.indexOf("x") <= 0) ? t : NaN;
}
function Xy(e) {
  return !isNaN(lr(e));
}
function Th() {
  return ur(Kb() * 9);
}
function qy(e, t) {
  return t === 0 ? e : qy(t, e % t);
}
function Lv(e, t) {
  return e == null ? t : t == null ? e : e * t / qy(e, t);
}
function fr(e) {
  return e != null && isFinite(e);
}
var fw = "[ECharts] ", hw = typeof console < "u" && console.warn && console.log;
function cw(e, t, r) {
  hw && console[e](fw + t);
}
function $y(e, t) {
  cw("error", e);
}
function Yt(e) {
  throw new Error(e);
}
function Ev(e, t, r) {
  return (t - e) * r + e;
}
var Zy = "series\0", vw = "\0_ec_\0";
function qt(e) {
  return e instanceof Array ? e : e == null ? [] : [e];
}
function Rv(e, t, r) {
  if (e) {
    e[t] = e[t] || {}, e.emphasis = e.emphasis || {}, e.emphasis[t] = e.emphasis[t] || {};
    for (var n = 0, i = r.length; n < i; n++) {
      var a = r[n];
      !e.emphasis[t].hasOwnProperty(a) && e[t].hasOwnProperty(a) && (e.emphasis[t][a] = e[t][a]);
    }
  }
}
var Ov = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "rich", "tag", "color", "textBorderColor", "textBorderWidth", "width", "height", "lineHeight", "align", "verticalAlign", "baseline", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY", "backgroundColor", "borderColor", "borderWidth", "borderRadius", "padding"];
function La(e) {
  return F(e) && !B(e) && !(e instanceof Date) ? e.value : e;
}
function dw(e) {
  return F(e) && !(e instanceof Array);
}
function pw(e, t, r) {
  var n = r === "normalMerge", i = r === "replaceMerge", a = r === "replaceAll";
  e = e || [], t = (t || []).slice();
  var o = U();
  T(t, function(u, l) {
    if (!F(u)) {
      t[l] = null;
      return;
    }
  });
  var s = gw(e, o, r);
  return (n || i) && yw(s, e, o, t), n && mw(s, t), n || i ? _w(s, t, i) : a && Sw(s, t), bw(s), s;
}
function gw(e, t, r) {
  var n = [];
  if (r === "replaceAll")
    return n;
  for (var i = 0; i < e.length; i++) {
    var a = e[i];
    a && a.id != null && t.set(a.id, i), n.push({
      existing: r === "replaceMerge" || pa(a) ? null : a,
      newOption: null,
      keyInfo: null,
      brandNew: null
    });
  }
  return n;
}
function yw(e, t, r, n) {
  T(n, function(i, a) {
    if (!(!i || i.id == null)) {
      var o = Ji(i.id), s = r.get(o);
      if (s != null) {
        var u = e[s];
        We(!u.newOption, 'Duplicated option on id "' + o + '".'), u.newOption = i, u.existing = t[s], n[a] = null;
      }
    }
  });
}
function mw(e, t) {
  T(t, function(r, n) {
    if (!(!r || r.name == null))
      for (var i = 0; i < e.length; i++) {
        var a = e[i].existing;
        if (!e[i].newOption && a && (a.id == null || r.id == null) && !pa(r) && !pa(a) && Ky("name", a, r)) {
          e[i].newOption = r, t[n] = null;
          return;
        }
      }
  });
}
function _w(e, t, r) {
  T(t, function(n) {
    if (n) {
      for (
        var i, a = 0;
        // Be `!resultItem` only when `nextIdx >= result.length`.
        (i = e[a]) && (i.newOption || pa(i.existing) || // In mode "replaceMerge", here no not-mapped-non-internal-existing.
        i.existing && n.id != null && !Ky("id", n, i.existing));
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
function Sw(e, t) {
  T(t, function(r) {
    e.push({
      newOption: r,
      brandNew: !0,
      existing: null,
      keyInfo: null
    });
  });
}
function bw(e) {
  var t = U();
  T(e, function(r) {
    var n = r.existing;
    n && t.set(n.id, r);
  }), T(e, function(r) {
    var n = r.newOption;
    We(!n || n.id == null || !t.get(n.id) || t.get(n.id) === r, "id duplicates: " + (n && n.id)), n && n.id != null && t.set(n.id, r), !r.keyInfo && (r.keyInfo = {});
  }), T(e, function(r, n) {
    var i = r.existing, a = r.newOption, o = r.keyInfo;
    if (F(a)) {
      if (o.name = a.name != null ? Ji(a.name) : i ? i.name : Zy + n, i)
        o.id = Ji(i.id);
      else if (a.id != null)
        o.id = Ji(a.id);
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
function Ky(e, t, r) {
  var n = Ge(t[e], null), i = Ge(r[e], null);
  return n != null && i != null && n === i;
}
function Ji(e) {
  return Ge(e, "");
}
function Ge(e, t) {
  return e == null ? t : N(e) ? e : vt(e) || Jo(e) ? e + "" : t;
}
function Ch(e) {
  var t = e.name;
  return !!(t && t.indexOf(Zy));
}
function pa(e) {
  return e && e.id != null && Ji(e.id).indexOf(vw) === 0;
}
function ww(e, t, r) {
  T(e, function(n) {
    var i = n.newOption;
    F(i) && (n.keyInfo.mainType = t, n.keyInfo.subType = Tw(t, i, n.existing, r));
  });
}
function Tw(e, t, r, n) {
  var i = t.type ? t.type : r ? r.subType : n.determineSubType(e, t);
  return i;
}
function Ea(e, t) {
  if (t.dataIndexInside != null)
    return t.dataIndexInside;
  if (t.dataIndex != null)
    return B(t.dataIndex) ? H(t.dataIndex, function(r) {
      return e.indexOfRawIndex(r);
    }) : e.indexOfRawIndex(t.dataIndex);
  if (t.name != null)
    return B(t.name) ? H(t.name, function(r) {
      return e.indexOfName(r);
    }) : e.indexOfName(t.name);
}
function dt() {
  var e = "__ec_inner_" + Cw++;
  return function(t) {
    return t[e] || (t[e] = {});
  };
}
var Cw = Th();
function $u(e, t, r) {
  var n = xh(t, r), i = n.mainTypeSpecified, a = n.queryOptionMap, o = n.others, s = o, u = r ? r.defaultMainType : null;
  return !i && u && a.set(u, {}), a.each(function(l, f) {
    var h = Ra(e, f, l, {
      useDefault: u === f,
      enableAll: r && r.enableAll != null ? r.enableAll : !0,
      enableNone: r && r.enableNone != null ? r.enableNone : !0
    });
    s[f + "Models"] = h.models, s[f + "Model"] = h.models[0];
  }), s;
}
function xh(e, t) {
  var r;
  if (N(e)) {
    var n = {};
    n[e + "Index"] = 0, r = n;
  } else
    r = e;
  var i = U(), a = {}, o = !1;
  return T(r, function(s, u) {
    if (u === "dataIndex" || u === "dataIndexInside") {
      a[u] = s;
      return;
    }
    var l = u.match(/^(\w+)(Index|Id|Name)$/) || [], f = l[1], h = (l[2] || "").toLowerCase();
    if (!(!f || !h || t && t.includeMainTypes && st(t.includeMainTypes, f) < 0)) {
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
function Ra(e, t, r, n) {
  n = n || ar;
  var i = r.index, a = r.id, o = r.name, s = {
    models: null,
    specified: i != null || a != null || o != null
  };
  if (!s.specified) {
    var u = void 0;
    return s.models = n.useDefault && (u = e.getComponent(t)) ? [u] : [], s;
  }
  if (i === "none" || i === !1) {
    if (n.enableNone)
      return s.models = [], s;
    i = -1;
  }
  return i === "all" && (n.enableAll ? i = a = o = null : i = -1), s.models = e.queryComponents({
    mainType: t,
    index: i,
    id: a,
    name: o
  }), s;
}
function xw(e, t, r) {
  var n = {};
  n[t + "Id"] = e[t + "Id"], n[t + "Index"] = e[t + "Index"], n[t + "Name"] = e[t + "Name"];
  var i = {
    mainType: t,
    query: n
  };
  return r && (i.subType = r), i;
}
function jy(e, t, r) {
  e.setAttribute ? e.setAttribute(t, r) : e[t] = r;
}
function Mw(e, t) {
  return e.getAttribute ? e.getAttribute(t) : e[t];
}
function Dw(e) {
  return e === "auto" ? W.domSupported ? "html" : "richText" : e || "html";
}
function Iw(e, t, r, n, i) {
  var a = t == null || t === "auto";
  if (n == null)
    return n;
  if (vt(n)) {
    var o = Ev(r || 0, n, i);
    return It(o, a ? Math.max(or(r || 0), or(n)) : t);
  } else {
    if (N(n))
      return i < 1 ? r : n;
    for (var s = [], u = r, l = n, f = Math.max(u ? u.length : 0, l.length), h = 0; h < f; ++h) {
      var v = e.getDimensionInfo(h);
      if (v && v.type === "ordinal")
        s[h] = (i < 1 && u ? u : l)[h];
      else {
        var c = u && u[h] ? u[h] : 0, d = l[h], o = Ev(c, d, i);
        s[h] = It(o, a ? Math.max(or(c), or(d)) : t);
      }
    }
    return s;
  }
}
function Ne() {
  return [1 / 0, -1 / 0];
}
function Aw(e, t) {
  hr(t) && (t < e[0] && (e[0] = t), t > e[1] && (e[1] = t));
}
function Qy(e, t) {
  hr(t) && t < e[0] && (e[0] = t);
}
function Jy(e, t) {
  hr(t) && t > e[1] && (e[1] = t);
}
function Pw(e, t) {
  fi(t[0], t[1]) && (t[0] < e[0] && (e[0] = t[0]), t[1] > e[1] && (e[1] = t[1]));
}
function hr(e) {
  return e != null && isFinite(e);
}
function fi(e, t) {
  return hr(e) && hr(t) && e <= t;
}
function Lw(e) {
  var t = e[1] - e[0];
  return isFinite(t) && t >= 0;
}
function Ew(e) {
  fi(e[0], e[1]) && e[0] > e[1] && (e[0] = e[1]);
}
function Rw() {
  var e = "__ec_once_" + Ow++;
  return function(t, r) {
    Xt(t, e) || (t[e] = 1, r());
  };
}
var Ow = Th();
function Mh(e, t, r) {
  var n = U(), i = 0;
  T(e, function(a) {
    var o = t(a), s = n.get(o) || 0;
    r && r(a, s), !s && !r && (e[i++] = a), n.set(o, s + 1);
  }), r || (e.length = i);
}
function kw(e) {
  return e.value + "";
}
function Bw(e) {
  return e + "";
}
function BE(e, t) {
  return G(t, !0) ? e.seriesIndex + 2 : 0;
}
function Nw(e, t, r) {
  var n = e.getData().count();
  return {
    progressiveRender: r.progressiveEnabled && t.incrementalPrepareRender && n >= r.threshold,
    large: e.get("large") && n >= e.get("largeThreshold"),
    // TODO: modDataCount should not updated if `appendData`, otherwise cause whole repaint.
    // see `test/candlestick-large3.html`
    modDataCount: e.get("progressiveChunkMode") === "mod" ? e.getData().count() : null
  };
}
function NE(e, t) {
  return {
    seriesType: e,
    overallReset: t
  };
}
function Dh(e) {
  return {
    overallReset: e
  };
}
var Fw = ".", Yr = "___EC__COMPONENT__CONTAINER___", tm = "___EC__EXTENDED_CLASS___";
function ze(e) {
  var t = {
    main: "",
    sub: ""
  };
  if (e) {
    var r = e.split(Fw);
    t.main = r[0] || "", t.sub = r[1] || "";
  }
  return t;
}
function zw(e) {
  We(/^[a-zA-Z0-9_]+([.][a-zA-Z0-9_]+)?$/.test(e), 'componentType "' + e + '" illegal');
}
function Hw(e) {
  return !!(e && e[tm]);
}
function Ih(e, t) {
  e.$constructor = e, e.extend = function(r) {
    var n = this, i;
    return Vw(n) ? i = /** @class */
    (function(a) {
      z(o, a);
      function o() {
        return a.apply(this, arguments) || this;
      }
      return o;
    })(n) : (i = function() {
      (r.$constructor || n).apply(this, arguments);
    }, dh(i, this)), R(i.prototype, r), i[tm] = !0, i.extend = this.extend, i.superCall = Yw, i.superApply = Ww, i.superClass = n, i;
  };
}
function Vw(e) {
  return X(e) && /^class\s/.test(Function.prototype.toString.call(e));
}
function em(e, t) {
  e.extend = t.extend;
}
var Gw = Math.round(Math.random() * 10);
function Uw(e) {
  var t = ["__\0is_clz", Gw++].join("_");
  e.prototype[t] = !0, e.isInstance = function(r) {
    return !!(r && r[t]);
  };
}
function Yw(e, t) {
  for (var r = [], n = 2; n < arguments.length; n++)
    r[n - 2] = arguments[n];
  return this.superClass.prototype[t].apply(e, r);
}
function Ww(e, t, r) {
  return this.superClass.prototype[t].apply(e, r);
}
function Zs(e) {
  var t = {};
  e.registerClass = function(n) {
    var i = n.type || n.prototype.type;
    if (i) {
      zw(i), n.prototype.type = i;
      var a = ze(i);
      if (!a.sub)
        t[a.main] = n;
      else if (a.sub !== Yr) {
        var o = r(a);
        o[a.sub] = n;
      }
    }
    return n;
  }, e.getClass = function(n, i, a) {
    var o = t[n];
    if (o && o[Yr] && (o = i ? o[i] : null), a && !o)
      throw new Error(i ? "Component " + n + "." + (i || "") + " is used but not imported." : n + ".type should be specified.");
    return o;
  }, e.getClassesByMainType = function(n) {
    var i = ze(n), a = [], o = t[i.main];
    return o && o[Yr] ? T(o, function(s, u) {
      u !== Yr && a.push(s);
    }) : a.push(o), a;
  }, e.hasClass = function(n) {
    var i = ze(n);
    return !!t[i.main];
  }, e.getAllClassMainTypes = function() {
    var n = [];
    return T(t, function(i, a) {
      n.push(a);
    }), n;
  }, e.hasSubTypes = function(n) {
    var i = ze(n), a = t[i.main];
    return a && a[Yr];
  };
  function r(n) {
    var i = t[n.main];
    return (!i || !i[Yr]) && (i = t[n.main] = {}, i[Yr] = !0), i;
  }
}
function ga(e, t) {
  for (var r = 0; r < e.length; r++)
    e[r][1] || (e[r][1] = e[r][0]);
  return t = t || !1, function(n, i, a) {
    for (var o = {}, s = 0; s < e.length; s++) {
      var u = e[s][1];
      if (!(i && st(i, u) >= 0 || a && st(a, u) < 0)) {
        var l = n.getShallow(u, t);
        l != null && (o[e[s][0]] = l);
      }
    }
    return o;
  };
}
var Xw = [
  ["fill", "color"],
  ["shadowBlur"],
  ["shadowOffsetX"],
  ["shadowOffsetY"],
  ["opacity"],
  ["shadowColor"]
  // Option decal is in `DecalObject` but style.decal is in `PatternObject`.
  // So do not transfer decal directly.
], qw = ga(Xw), $w = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getAreaStyle = function(t, r) {
      return qw(this, t, r);
    }, e;
  })()
), mf = new si(50);
function Zw(e) {
  if (typeof e == "string") {
    var t = mf.get(e);
    return t && t.image;
  } else
    return e;
}
function rm(e, t, r, n, i) {
  if (e)
    if (typeof e == "string") {
      if (t && t.__zrImageSrc === e || !r)
        return t;
      var a = mf.get(e), o = { hostEl: r, cb: n, cbPayload: i };
      return a ? (t = a.image, !Ks(t) && a.pending.push(o)) : (t = Lt.loadImage(e, kv, kv), t.__zrImageSrc = e, mf.put(e, t.__cachedImgObj = {
        image: t,
        pending: [o]
      })), t;
    } else
      return e;
  else return t;
}
function kv() {
  var e = this.__cachedImgObj;
  this.onload = this.onerror = this.__cachedImgObj = null;
  for (var t = 0; t < e.pending.length; t++) {
    var r = e.pending[t], n = r.cb;
    n && n(this, r.cbPayload), r.hostEl.dirty();
  }
  e.pending.length = 0;
}
function Ks(e) {
  return e && e.width && e.height;
}
var Zu = /\{([a-zA-Z0-9_]+)\|([^}]*)\}/g;
function Kw(e, t, r, n, i) {
  var a = {};
  return nm(a, e, t, r, n, i), a.text;
}
function nm(e, t, r, n, i, a) {
  if (!r) {
    e.text = "", e.isTruncated = !1;
    return;
  }
  var o = (t + "").split(`
`);
  a = im(r, n, i, a);
  for (var s = !1, u = {}, l = 0, f = o.length; l < f; l++)
    am(u, o[l], a), o[l] = u.textLine, s = s || u.isTruncated;
  e.text = o.join(`
`), e.isTruncated = s;
}
function im(e, t, r, n) {
  n = n || {};
  var i = R({}, n);
  r = G(r, "..."), i.maxIterations = G(n.maxIterations, 2);
  var a = i.minChar = G(n.minChar, 0), o = i.fontMeasureInfo = He(t), s = o.asciiCharWidth;
  i.placeholder = G(n.placeholder, "");
  for (var u = e = Math.max(0, e - 1), l = 0; l < a && u >= s; l++)
    u -= s;
  var f = Ve(o, r);
  return f > u && (r = "", f = 0), u = e - f, i.ellipsis = r, i.ellipsisWidth = f, i.contentWidth = u, i.containerWidth = e, i;
}
function am(e, t, r) {
  var n = r.containerWidth, i = r.contentWidth, a = r.fontMeasureInfo;
  if (!n) {
    e.textLine = "", e.isTruncated = !1;
    return;
  }
  var o = Ve(a, t);
  if (o <= n) {
    e.textLine = t, e.isTruncated = !1;
    return;
  }
  for (var s = 0; ; s++) {
    if (o <= i || s >= r.maxIterations) {
      t += r.ellipsis;
      break;
    }
    var u = s === 0 ? jw(t, i, a) : o > 0 ? Math.floor(t.length * i / o) : 0;
    t = t.substr(0, u), o = Ve(a, t);
  }
  t === "" && (t = r.placeholder), e.textLine = t, e.isTruncated = !0;
}
function jw(e, t, r) {
  for (var n = 0, i = 0, a = e.length; i < a && n < t; i++)
    n += Fy(r, e.charCodeAt(i));
  return i;
}
function Qw(e, t, r, n) {
  var i = Ah(e), a = t.overflow, o = t.padding, s = o ? o[1] + o[3] : 0, u = o ? o[0] + o[2] : 0, l = t.font, f = a === "truncate", h = Ys(l), v = G(t.lineHeight, h), c = t.lineOverflow === "truncate", d = !1, y = t.width;
  y == null && r != null && (y = r - s);
  var p = t.height;
  p == null && n != null && (p = n - u);
  var g;
  y != null && (a === "break" || a === "breakAll") ? g = i ? om(i, t.font, y, a === "breakAll", 0).lines : [] : g = i ? i.split(`
`) : [];
  var m = g.length * v;
  if (p == null && (p = m), m > p && c) {
    var _ = Math.floor(p / v);
    d = d || g.length > _, g = g.slice(0, _), m = g.length * v;
  }
  if (i && f && y != null)
    for (var S = im(y, l, t.ellipsis, {
      minChar: t.truncateMinChar,
      placeholder: t.placeholder
    }), b = {}, w = 0; w < g.length; w++)
      am(b, g[w], S), g[w] = b.textLine, d = d || b.isTruncated;
  for (var C = p, M = 0, x = He(l), w = 0; w < g.length; w++)
    M = Math.max(Ve(x, g[w]), M);
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
var Jw = /* @__PURE__ */ (function() {
  function e() {
  }
  return e;
})(), Bv = /* @__PURE__ */ (function() {
  function e(t) {
    this.tokens = [], t && (this.tokens = t);
  }
  return e;
})(), tT = /* @__PURE__ */ (function() {
  function e() {
    this.width = 0, this.height = 0, this.contentWidth = 0, this.contentHeight = 0, this.outerWidth = 0, this.outerHeight = 0, this.lines = [], this.isTruncated = !1;
  }
  return e;
})();
function eT(e, t, r, n, i) {
  var a = new tT(), o = Ah(e);
  if (!o)
    return a;
  var s = t.padding, u = s ? s[1] + s[3] : 0, l = s ? s[0] + s[2] : 0, f = t.width;
  f == null && r != null && (f = r - u);
  var h = t.height;
  h == null && n != null && (h = n - l);
  for (var v = t.overflow, c = (v === "break" || v === "breakAll") && f != null ? { width: f, accumWidth: 0, breakAll: v === "breakAll" } : null, d = Zu.lastIndex = 0, y; (y = Zu.exec(o)) != null; ) {
    var p = y.index;
    p > d && Ku(a, o.substring(d, p), t, c), Ku(a, y[2], t, c, y[1]), d = Zu.lastIndex;
  }
  d < o.length && Ku(a, o.substring(d, o.length), t, c);
  var g = [], m = 0, _ = 0, S = v === "truncate", b = t.lineOverflow === "truncate", w = {};
  function C(St, te, Ze) {
    St.width = te, St.lineHeight = Ze, m += Ze, _ = Math.max(_, te);
  }
  t: for (var M = 0; M < a.lines.length; M++) {
    for (var x = a.lines[M], D = 0, I = 0, L = 0; L < x.tokens.length; L++) {
      var A = x.tokens[L], P = A.styleName && t.rich[A.styleName] || {}, k = A.textPadding = P.padding, O = k ? k[1] + k[3] : 0, V = A.font = P.font || t.font;
      A.contentHeight = Ys(V);
      var K = G(P.height, A.contentHeight);
      if (A.innerHeight = K, k && (K += k[0] + k[2]), A.height = K, A.lineHeight = dn(P.lineHeight, t.lineHeight, K), A.align = P && P.align || i, A.verticalAlign = P && P.verticalAlign || "middle", b && h != null && m + A.lineHeight > h) {
        var J = a.lines.length;
        L > 0 ? (x.tokens = x.tokens.slice(0, L), C(x, I, D), a.lines = a.lines.slice(0, M + 1)) : a.lines = a.lines.slice(0, M), a.isTruncated = a.isTruncated || a.lines.length < J;
        break t;
      }
      var tt = P.width, Q = tt == null || tt === "auto";
      if (typeof tt == "string" && tt.charAt(tt.length - 1) === "%")
        A.percentWidth = tt, g.push(A), A.contentWidth = Ve(He(V), A.text);
      else {
        if (Q) {
          var rt = P.backgroundColor, nt = rt && rt.image;
          nt && (nt = Zw(nt), Ks(nt) && (A.width = Math.max(A.width, nt.width * K / nt.height)));
        }
        var q = S && f != null ? f - I : null;
        q != null && q < A.width ? !Q || q < O ? (A.text = "", A.width = A.contentWidth = 0) : (nm(w, A.text, q - O, V, t.ellipsis, { minChar: t.truncateMinChar }), A.text = w.text, a.isTruncated = a.isTruncated || w.isTruncated, A.width = A.contentWidth = Ve(He(V), A.text)) : A.contentWidth = Ve(He(V), A.text);
      }
      A.width += O, I += A.width, P && (D = Math.max(D, A.lineHeight));
    }
    C(x, I, D);
  }
  a.outerWidth = a.width = G(f, _), a.outerHeight = a.height = G(h, m), a.contentHeight = m, a.contentWidth = _, a.outerWidth += u, a.outerHeight += l;
  for (var M = 0; M < g.length; M++) {
    var A = g[M], ot = A.percentWidth;
    A.width = parseInt(ot, 10) / 100 * a.width;
  }
  return a;
}
function Ku(e, t, r, n, i) {
  var a = t === "", o = i && r.rich[i] || {}, s = e.lines, u = o.font || r.font, l = !1, f, h;
  if (n) {
    var v = o.padding, c = v ? v[1] + v[3] : 0;
    if (o.width != null && o.width !== "auto") {
      var d = li(o.width, n.width) + c;
      s.length > 0 && d + n.accumWidth > n.width && (f = t.split(`
`), l = !0), n.accumWidth = d;
    } else {
      var y = om(t, u, n.width, n.breakAll, n.accumWidth);
      n.accumWidth = y.accumWidth + c, h = y.linesWidths, f = y.lines;
    }
  }
  f || (f = t.split(`
`));
  for (var p = He(u), g = 0; g < f.length; g++) {
    var m = f[g], _ = new Jw();
    if (_.styleName = i, _.text = m, _.isLineHolder = !m && !a, typeof o.width == "number" ? _.width = o.width : _.width = h ? h[g] : Ve(p, m), !g && !l) {
      var S = (s[s.length - 1] || (s[0] = new Bv())).tokens, b = S.length;
      b === 1 && S[0].isLineHolder ? S[0] = _ : (m || !b || a) && S.push(_);
    } else
      s.push(new Bv([_]));
  }
}
function rT(e) {
  var t = e.charCodeAt(0);
  return t >= 32 && t <= 591 || t >= 880 && t <= 4351 || t >= 4608 && t <= 5119 || t >= 7680 && t <= 8303;
}
var nT = Ye(",&?/;] ".split(""), function(e, t) {
  return e[t] = !0, e;
}, {});
function iT(e) {
  return rT(e) ? !!nT[e] : !0;
}
function om(e, t, r, n, i) {
  for (var a = [], o = [], s = "", u = "", l = 0, f = 0, h = He(t), v = 0; v < e.length; v++) {
    var c = e.charAt(v);
    if (c === `
`) {
      u && (s += u, f += l), a.push(s), o.push(f), s = "", u = "", l = 0, f = 0;
      continue;
    }
    var d = Fy(h, c.charCodeAt(0)), y = n ? !1 : !iT(c);
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
function Nv(e, t, r, n, i, a) {
  if (e.baseX = r, e.baseY = n, e.outerWidth = e.outerHeight = null, !!t) {
    var o = t.width * 2, s = t.height * 2;
    Y.set(Fv, ui(r, o, i), yn(n, s, a), o, s), Y.intersect(t, Fv, null, zv);
    var u = zv.outIntersectRect;
    e.outerWidth = u.width, e.outerHeight = u.height, e.baseX = ui(u.x, u.width, i, !0), e.baseY = yn(u.y, u.height, a, !0);
  }
}
var Fv = new Y(0, 0, 0, 0), zv = { outIntersectRect: {}, clamp: !0 };
function Ah(e) {
  return e != null ? e += "" : e = "";
}
function aT(e) {
  var t = Ah(e.text), r = e.font, n = Ve(He(r), t), i = Ys(r);
  return _f(e, n, i, null);
}
function _f(e, t, r, n) {
  var i = new Y(ui(e.x || 0, t, e.textAlign), yn(e.y || 0, r, e.textBaseline), t, r), a = n ?? (sm(e) ? e.lineWidth : 0);
  return a > 0 && (i.x -= a / 2, i.y -= a / 2, i.width += a, i.height += a), i;
}
function sm(e) {
  var t = e.stroke;
  return t != null && t !== "none" && e.lineWidth > 0;
}
var Sf = "__zr_style_" + Math.round(Math.random() * 10), mn = {
  shadowBlur: 0,
  shadowOffsetX: 0,
  shadowOffsetY: 0,
  shadowColor: "#000",
  opacity: 1,
  blend: "source-over"
}, js = {
  style: {
    shadowBlur: !0,
    shadowOffsetX: !0,
    shadowOffsetY: !0,
    shadowColor: !0,
    opacity: !0
  }
};
mn[Sf] = !0;
var Hv = ["z", "z2", "invisible"], oT = ["invisible"], Oa = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype._init = function(r) {
    for (var n = mt(r), i = 0; i < n.length; i++) {
      var a = n[i];
      a === "style" ? this.useStyle(r[a]) : e.prototype.attrKV.call(this, a, r[a]);
    }
    this.style || this.useStyle({});
  }, t.prototype.beforeBrush = function(r) {
  }, t.prototype.afterBrush = function() {
  }, t.prototype.innerBeforeBrush = function() {
  }, t.prototype.innerAfterBrush = function() {
  }, t.prototype.shouldBePainted = function(r, n, i, a) {
    var o = this.transform;
    if (this.ignore || this.invisible || this.style.opacity === 0 || this.culling && sT(this, r, n) || o && !o[0] && !o[3])
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
      r = this._paintRect || (this._paintRect = new Y(0, 0, 0, 0)), n ? Y.applyTransform(r, i, n) : r.copy(i), (o || s || u) && (r.width += o * 2 + Math.abs(s), r.height += o * 2 + Math.abs(u), r.x = Math.min(r.x, r.x + s - o), r.y = Math.min(r.y, r.y + u - o));
      var l = this.dirtyRectTolerance;
      r.isZero() || (r.x = Math.floor(r.x - l), r.y = Math.floor(r.y - l), r.width = Math.ceil(r.width + 1 + l * 2), r.height = Math.ceil(r.height + 1 + l * 2));
    }
    return r;
  }, t.prototype.setPrevPaintRect = function(r) {
    r ? (this._prevPaintRect = this._prevPaintRect || new Y(0, 0, 0, 0), this._prevPaintRect.copy(r)) : this._prevPaintRect = null;
  }, t.prototype.getPrevPaintRect = function() {
    return this._prevPaintRect;
  }, t.prototype.animateStyle = function(r) {
    return this.animate("style", r);
  }, t.prototype.updateDuringAnimation = function(r) {
    r === "style" ? this.dirtyStyle() : this.markRedraw();
  }, t.prototype.attrKV = function(r, n) {
    r !== "style" ? e.prototype.attrKV.call(this, r, n) : this.style ? this.setStyle(n) : this.useStyle(n);
  }, t.prototype.setStyle = function(r, n) {
    return typeof r == "string" ? this.style[r] = n : R(this.style, r), this.dirtyStyle(), this;
  }, t.prototype.dirtyStyle = function(r) {
    r || this.markRedraw(), this.__dirty |= Wi, this._rect && (this._rect = null);
  }, t.prototype.dirty = function() {
    this.dirtyStyle();
  }, t.prototype.styleChanged = function() {
    return !!(this.__dirty & Wi);
  }, t.prototype.styleUpdated = function() {
    this.__dirty &= ~Wi;
  }, t.prototype.createStyle = function(r) {
    return Ia(mn, r);
  }, t.prototype.useStyle = function(r) {
    r[Sf] || (r = this.createStyle(r)), this.style = r, this.dirtyStyle();
  }, t.prototype._useHoverStyle = function(r) {
    this.__hoverStyle = r;
  }, t.prototype.isStyleObject = function(r) {
    return r[Sf];
  }, t.prototype._innerSaveToNormal = function(r) {
    e.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.style && !n.style && (n.style = this._mergeStyle(this.createStyle(), this.style)), this._savePrimaryToNormal(r, n, Hv);
  }, t.prototype._applyStateObj = function(r, n, i, a, o, s) {
    e.prototype._applyStateObj.call(this, r, n, i, a, o, s);
    var u = !(n && a), l = this.__inHover === Ws, f;
    if (n && n.style ? o ? a ? f = n.style : (f = this._mergeStyle(this.createStyle(), i.style), this._mergeStyle(f, n.style)) : (f = this._mergeStyle(this.createStyle(), a ? this.style : i.style), this._mergeStyle(f, n.style)) : u && (f = i.style), f)
      if (o) {
        var h = this.style;
        if (this.style = this.createStyle(u ? {} : h), u)
          for (var v = mt(h), c = 0; c < v.length; c++) {
            var d = v[c];
            d in f && (f[d] = f[d], this.style[d] = h[d]);
          }
        for (var y = mt(f), c = 0; c < y.length; c++) {
          var d = y[c];
          this.style[d] = this.style[d];
        }
        this._transitionState(r, {
          style: f
        }, s, this.getAnimationStyleProps());
      } else
        l ? this._useHoverStyle(f) : this.useStyle(f);
    if (!l)
      for (var p = this.__inHover ? oT : Hv, c = 0; c < p.length; c++) {
        var d = p[c];
        n && n[d] != null ? this[d] = n[d] : u && i[d] != null && (this[d] = i[d]);
      }
  }, t.prototype._mergeStates = function(r) {
    for (var n = e.prototype._mergeStates.call(this, r), i, a = 0; a < r.length; a++) {
      var o = r[a];
      o.style && (i = i || {}, this._mergeStyle(i, o.style));
    }
    return i && (n.style = i), n;
  }, t.prototype._mergeStyle = function(r, n) {
    return R(r, n), r;
  }, t.prototype.getAnimationStyleProps = function() {
    return js;
  }, t.initDefaultProps = (function() {
    var r = t.prototype;
    r.type = "displayable", r.invisible = !1, r.z = 0, r.z2 = 0, r.zlevel = 0, r.culling = !1, r.cursor = "pointer", r.rectHover = !1, r.incremental = 0, r._rect = null, r.dirtyRectTolerance = 0, r.__dirty = Qt | Wi;
  })(), t;
})(Xs), ju = new Y(0, 0, 0, 0), Qu = new Y(0, 0, 0, 0);
function sT(e, t, r) {
  return ju.copy(e.getBoundingRect()), e.transform && ju.applyTransform(e.transform), Qu.width = t, Qu.height = r, !ju.intersect(Qu);
}
var ge = Math.min, ye = Math.max, Ju = Math.sin, tl = Math.cos, Wr = Math.PI * 2, Ka = Cn(), ja = Cn(), Qa = Cn();
function Vv(e, t, r, n, i, a) {
  i[0] = ge(e, r), i[1] = ge(t, n), a[0] = ye(e, r), a[1] = ye(t, n);
}
var Gv = [], Uv = [];
function uT(e, t, r, n, i, a, o, s, u, l) {
  var f = Iy, h = Pt, v = f(e, r, i, o, Gv);
  u[0] = 1 / 0, u[1] = 1 / 0, l[0] = -1 / 0, l[1] = -1 / 0;
  for (var c = 0; c < v; c++) {
    var d = h(e, r, i, o, Gv[c]);
    u[0] = ge(d, u[0]), l[0] = ye(d, l[0]);
  }
  v = f(t, n, a, s, Uv);
  for (var c = 0; c < v; c++) {
    var y = h(t, n, a, s, Uv[c]);
    u[1] = ge(y, u[1]), l[1] = ye(y, l[1]);
  }
  u[0] = ge(e, u[0]), l[0] = ye(e, l[0]), u[0] = ge(o, u[0]), l[0] = ye(o, l[0]), u[1] = ge(t, u[1]), l[1] = ye(t, l[1]), u[1] = ge(s, u[1]), l[1] = ye(s, l[1]);
}
function lT(e, t, r, n, i, a, o, s) {
  var u = Ay, l = Bt, f = ye(ge(u(e, r, i), 1), 0), h = ye(ge(u(t, n, a), 1), 0), v = l(e, r, i, f), c = l(t, n, a, h);
  o[0] = ge(e, i, v), o[1] = ge(t, a, c), s[0] = ye(e, i, v), s[1] = ye(t, a, c);
}
function fT(e, t, r, n, i, a, o, s, u) {
  var l = br, f = wr, h = Math.abs(i - a);
  if (h % Wr < 1e-4 && h > 1e-4) {
    s[0] = e - r, s[1] = t - n, u[0] = e + r, u[1] = t + n;
    return;
  }
  if (Ka[0] = tl(i) * r + e, Ka[1] = Ju(i) * n + t, ja[0] = tl(a) * r + e, ja[1] = Ju(a) * n + t, l(s, Ka, ja), f(u, Ka, ja), i = i % Wr, i < 0 && (i = i + Wr), a = a % Wr, a < 0 && (a = a + Wr), i > a && !o ? a += Wr : i < a && o && (i += Wr), o) {
    var v = a;
    a = i, i = v;
  }
  for (var c = 0; c < a; c += Math.PI / 2)
    c > i && (Qa[0] = tl(c) * r + e, Qa[1] = Ju(c) * n + t, l(s, Qa, s), f(u, Qa, u));
}
var it = {
  M: 1,
  L: 2,
  C: 3,
  Q: 4,
  A: 5,
  Z: 6,
  R: 7
}, Xr = [], qr = [], Ee = [], pr = [], Re = [], Oe = [], el = Math.min, rl = Math.max, $r = Math.cos, Zr = Math.sin, Je = Math.abs, bf = Math.PI, _r = bf * 2, nl = typeof Float32Array < "u", Ii = [];
function il(e) {
  var t = Math.round(e / bf * 1e8) / 1e8;
  return t % 2 * bf;
}
function hT(e, t) {
  var r = il(e[0]);
  r < 0 && (r += _r);
  var n = r - e[0], i = e[1];
  i += n, !t && i - r >= _r ? i = r + _r : t && r - i >= _r ? i = r - _r : !t && r > i ? i = r + (_r - il(r - i)) : t && r < i && (i = r - (_r - il(i - r))), e[0] = r, e[1] = i;
}
var hi = (function() {
  function e(t) {
    this.dpr = 1, this._xi = 0, this._yi = 0, this._x0 = 0, this._y0 = 0, this._len = 0, t && (this._saveData = !1), this._saveData && (this.data = []);
  }
  return e.prototype.increaseVersion = function() {
    this._version++;
  }, e.prototype.getVersion = function() {
    return this._version;
  }, e.prototype.setScale = function(t, r, n) {
    n = n || 0, n > 0 && (this._ux = Je(n / us / t) || 0, this._uy = Je(n / us / r) || 0);
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
    return this._drawPendingPt(), this.addData(it.M, t, r), this._ctx && this._ctx.moveTo(t, r), this._x0 = t, this._y0 = r, this._xi = t, this._yi = r, this;
  }, e.prototype.lineTo = function(t, r) {
    var n = Je(t - this._xi), i = Je(r - this._yi), a = n > this._ux || i > this._uy;
    if (this.addData(it.L, t, r), this._ctx && a && this._ctx.lineTo(t, r), a)
      this._xi = t, this._yi = r, this._pendingPtDist = 0;
    else {
      var o = n * n + i * i;
      o > this._pendingPtDist && (this._pendingPtX = t, this._pendingPtY = r, this._pendingPtDist = o);
    }
    return this;
  }, e.prototype.bezierCurveTo = function(t, r, n, i, a, o) {
    return this._drawPendingPt(), this.addData(it.C, t, r, n, i, a, o), this._ctx && this._ctx.bezierCurveTo(t, r, n, i, a, o), this._xi = a, this._yi = o, this;
  }, e.prototype.quadraticCurveTo = function(t, r, n, i) {
    return this._drawPendingPt(), this.addData(it.Q, t, r, n, i), this._ctx && this._ctx.quadraticCurveTo(t, r, n, i), this._xi = n, this._yi = i, this;
  }, e.prototype.arc = function(t, r, n, i, a, o) {
    this._drawPendingPt(), Ii[0] = i, Ii[1] = a, hT(Ii, o), i = Ii[0], a = Ii[1];
    var s = a - i;
    return this.addData(it.A, t, r, n, n, i, s, 0, o ? 0 : 1), this._ctx && this._ctx.arc(t, r, n, i, a, o), this._xi = $r(a) * n + t, this._yi = Zr(a) * n + r, this;
  }, e.prototype.arcTo = function(t, r, n, i, a) {
    return this._drawPendingPt(), this._ctx && this._ctx.arcTo(t, r, n, i, a), this;
  }, e.prototype.rect = function(t, r, n, i) {
    return this._drawPendingPt(), this._ctx && this._ctx.rect(t, r, n, i), this.addData(it.R, t, r, n, i), this;
  }, e.prototype.closePath = function() {
    this._drawPendingPt(), this.addData(it.Z);
    var t = this._ctx, r = this._x0, n = this._y0;
    return t && t.closePath(), this._xi = r, this._yi = n, this;
  }, e.prototype.fill = function(t) {
    t && t.fill(), this.toStatic();
  }, e.prototype.stroke = function(t) {
    t && t.stroke(), this.toStatic();
  }, e.prototype.len = function() {
    return this._len;
  }, e.prototype.setData = function(t) {
    if (this._saveData) {
      var r = t.length;
      !(this.data && this.data.length === r) && nl && (this.data = new Float32Array(r));
      for (var n = 0; n < r; n++)
        this.data[n] = t[n];
      this._len = r;
    }
  }, e.prototype.appendPath = function(t) {
    if (this._saveData) {
      t instanceof Array || (t = [t]);
      for (var r = t.length, n = 0, i = this._len, a = 0; a < r; a++)
        n += t[a].len();
      var o = this.data;
      if (nl && (o instanceof Float32Array || !o) && (this.data = new Float32Array(i + n), i > 0 && o))
        for (var s = 0; s < i; s++)
          this.data[s] = o[s];
      for (var a = 0; a < r; a++)
        for (var u = t[a].data, s = 0; s < u.length; s++)
          this.data[i++] = u[s];
      this._len = i;
    }
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
      t instanceof Array && (t.length = this._len, nl && this._len > 11 && (this.data = new Float32Array(t)));
    }
  }, e.prototype.getBoundingRect = function() {
    Ee[0] = Ee[1] = Re[0] = Re[1] = Number.MAX_VALUE, pr[0] = pr[1] = Oe[0] = Oe[1] = -Number.MAX_VALUE;
    var t = this.data, r = 0, n = 0, i = 0, a = 0, o;
    for (o = 0; o < this._len; ) {
      var s = t[o++], u = o === 1;
      switch (u && (r = t[o], n = t[o + 1], i = r, a = n), s) {
        case it.M:
          r = i = t[o++], n = a = t[o++], Re[0] = i, Re[1] = a, Oe[0] = i, Oe[1] = a;
          break;
        case it.L:
          Vv(r, n, t[o], t[o + 1], Re, Oe), r = t[o++], n = t[o++];
          break;
        case it.C:
          uT(r, n, t[o++], t[o++], t[o++], t[o++], t[o], t[o + 1], Re, Oe), r = t[o++], n = t[o++];
          break;
        case it.Q:
          lT(r, n, t[o++], t[o++], t[o], t[o + 1], Re, Oe), r = t[o++], n = t[o++];
          break;
        case it.A:
          var l = t[o++], f = t[o++], h = t[o++], v = t[o++], c = t[o++], d = t[o++] + c;
          o += 1;
          var y = !t[o++];
          u && (i = $r(c) * h + l, a = Zr(c) * v + f), fT(l, f, h, v, c, d, y, Re, Oe), r = $r(d) * h + l, n = Zr(d) * v + f;
          break;
        case it.R:
          i = r = t[o++], a = n = t[o++];
          var p = t[o++], g = t[o++];
          Vv(i, a, i + p, a + g, Re, Oe);
          break;
        case it.Z:
          r = i, n = a;
          break;
      }
      br(Ee, Ee, Re), wr(pr, pr, Oe);
    }
    return o === 0 && (Ee[0] = Ee[1] = pr[0] = pr[1] = 0), new Y(Ee[0], Ee[1], pr[0] - Ee[0], pr[1] - Ee[1]);
  }, e.prototype._calculateLength = function() {
    var t = this.data, r = this._len, n = this._ux, i = this._uy, a = 0, o = 0, s = 0, u = 0;
    this._pathSegLen || (this._pathSegLen = []);
    for (var l = this._pathSegLen, f = 0, h = 0, v = 0; v < r; ) {
      var c = t[v++], d = v === 1;
      d && (a = t[v], o = t[v + 1], s = a, u = o);
      var y = -1;
      switch (c) {
        case it.M:
          a = s = t[v++], o = u = t[v++];
          break;
        case it.L: {
          var p = t[v++], g = t[v++], m = p - a, _ = g - o;
          (Je(m) > n || Je(_) > i || v === r - 1) && (y = Math.sqrt(m * m + _ * _), a = p, o = g);
          break;
        }
        case it.C: {
          var S = t[v++], b = t[v++], p = t[v++], g = t[v++], w = t[v++], C = t[v++];
          y = tb(a, o, S, b, p, g, w, C, 10), a = w, o = C;
          break;
        }
        case it.Q: {
          var S = t[v++], b = t[v++], p = t[v++], g = t[v++];
          y = nb(a, o, S, b, p, g, 10), a = p, o = g;
          break;
        }
        case it.A:
          var M = t[v++], x = t[v++], D = t[v++], I = t[v++], L = t[v++], A = t[v++], P = A + L;
          v += 1, d && (s = $r(L) * D + M, u = Zr(L) * I + x), y = rl(D, I) * el(_r, Math.abs(A)), a = $r(P) * D + M, o = Zr(P) * I + x;
          break;
        case it.R: {
          s = a = t[v++], u = o = t[v++];
          var k = t[v++], O = t[v++];
          y = k * 2 + O * 2;
          break;
        }
        case it.Z: {
          var m = s - a, _ = u - o;
          y = Math.sqrt(m * m + _ * _), a = s, o = u;
          break;
        }
      }
      y >= 0 && (l[h++] = y, f += y);
    }
    return this._pathLen = f, f;
  }, e.prototype.rebuildPath = function(t, r) {
    var n = this.data, i = this._ux, a = this._uy, o = this._len, s, u, l, f, h, v, c = r < 1, d, y, p = 0, g = 0, m, _ = 0, S, b;
    if (!(c && (this._pathSegLen || this._calculateLength(), d = this._pathSegLen, y = this._pathLen, m = r * y, !m)))
      t: for (var w = 0; w < o; ) {
        var C = n[w++], M = w === 1;
        switch (M && (l = n[w], f = n[w + 1], s = l, u = f), C !== it.L && _ > 0 && (t.lineTo(S, b), _ = 0), C) {
          case it.M:
            s = l = n[w++], u = f = n[w++], t.moveTo(l, f);
            break;
          case it.L: {
            h = n[w++], v = n[w++];
            var x = Je(h - l), D = Je(v - f);
            if (x > i || D > a) {
              if (c) {
                var I = d[g++];
                if (p + I > m) {
                  var L = (m - p) / I;
                  t.lineTo(l * (1 - L) + h * L, f * (1 - L) + v * L);
                  break t;
                }
                p += I;
              }
              t.lineTo(h, v), l = h, f = v, _ = 0;
            } else {
              var A = x * x + D * D;
              A > _ && (S = h, b = v, _ = A);
            }
            break;
          }
          case it.C: {
            var P = n[w++], k = n[w++], O = n[w++], V = n[w++], K = n[w++], J = n[w++];
            if (c) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                as(l, P, O, K, L, Xr), as(f, k, V, J, L, qr), t.bezierCurveTo(Xr[1], qr[1], Xr[2], qr[2], Xr[3], qr[3]);
                break t;
              }
              p += I;
            }
            t.bezierCurveTo(P, k, O, V, K, J), l = K, f = J;
            break;
          }
          case it.Q: {
            var P = n[w++], k = n[w++], O = n[w++], V = n[w++];
            if (c) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                os(l, P, O, L, Xr), os(f, k, V, L, qr), t.quadraticCurveTo(Xr[1], qr[1], Xr[2], qr[2]);
                break t;
              }
              p += I;
            }
            t.quadraticCurveTo(P, k, O, V), l = O, f = V;
            break;
          }
          case it.A:
            var tt = n[w++], Q = n[w++], rt = n[w++], nt = n[w++], q = n[w++], ot = n[w++], St = n[w++], te = !n[w++], Ze = rt > nt ? rt : nt, ee = Je(rt - nt) > 1e-3, At = q + ot, $ = !1;
            if (c) {
              var I = d[g++];
              p + I > m && (At = q + ot * (m - p) / I, $ = !0), p += I;
            }
            if (ee && t.ellipse ? t.ellipse(tt, Q, rt, nt, St, q, At, te) : t.arc(tt, Q, Ze, q, At, te), $)
              break t;
            M && (s = $r(q) * rt + tt, u = Zr(q) * nt + Q), l = $r(At) * rt + tt, f = Zr(At) * nt + Q;
            break;
          case it.R:
            s = l = n[w], u = f = n[w + 1], h = n[w++], v = n[w++];
            var et = n[w++], Nr = n[w++];
            if (c) {
              var I = d[g++];
              if (p + I > m) {
                var Ht = m - p;
                t.moveTo(h, v), t.lineTo(h + el(Ht, et), v), Ht -= et, Ht > 0 && t.lineTo(h + et, v + el(Ht, Nr)), Ht -= Nr, Ht > 0 && t.lineTo(h + rl(et - Ht, 0), v + Nr), Ht -= et, Ht > 0 && t.lineTo(h, v + rl(Nr - Ht, 0));
                break t;
              }
              p += I;
            }
            t.rect(h, v, et, Nr);
            break;
          case it.Z:
            if (c) {
              var I = d[g++];
              if (p + I > m) {
                var L = (m - p) / I;
                t.lineTo(l * (1 - L) + s * L, f * (1 - L) + u * L);
                break t;
              }
              p += I;
            }
            t.closePath(), l = s, f = u;
        }
      }
  }, e.prototype.clone = function() {
    var t = new e(), r = this.data;
    return t.data = r.slice ? r.slice() : Array.prototype.slice.call(r), t._len = this._len, t;
  }, e.prototype.canSave = function() {
    return !!this._saveData;
  }, e.CMD = it, e.initDefaultProps = (function() {
    var t = e.prototype;
    t._saveData = !0, t._ux = 0, t._uy = 0, t._pendingPtDist = 0, t._version = 0;
  })(), e;
})();
function kn(e, t, r, n, i, a, o) {
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
function cT(e, t, r, n, i, a, o, s, u, l, f) {
  if (u === 0)
    return !1;
  var h = u;
  if (f > t + h && f > n + h && f > a + h && f > s + h || f < t - h && f < n - h && f < a - h && f < s - h || l > e + h && l > r + h && l > i + h && l > o + h || l < e - h && l < r - h && l < i - h && l < o - h)
    return !1;
  var v = J1(e, t, r, n, i, a, o, s, l, f, null);
  return v <= h / 2;
}
function vT(e, t, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  if (u > t + l && u > n + l && u > a + l || u < t - l && u < n - l && u < a - l || s > e + l && s > r + l && s > i + l || s < e - l && s < r - l && s < i - l)
    return !1;
  var f = rb(e, t, r, n, i, a, s, u, null);
  return f <= l / 2;
}
var Yv = Math.PI * 2;
function Ja(e) {
  return e %= Yv, e < 0 && (e += Yv), e;
}
var Ai = Math.PI * 2;
function dT(e, t, r, n, i, a, o, s, u) {
  if (o === 0)
    return !1;
  var l = o;
  s -= e, u -= t;
  var f = Math.sqrt(s * s + u * u);
  if (f - l > r || f + l < r)
    return !1;
  if (Math.abs(n - i) % Ai < 1e-4)
    return !0;
  if (a) {
    var h = n;
    n = Ja(i), i = Ja(h);
  } else
    n = Ja(n), i = Ja(i);
  n > i && (i += Ai);
  var v = Math.atan2(u, s);
  return v < 0 && (v += Ai), v >= n && v <= i || v + Ai >= n && v + Ai <= i;
}
function ir(e, t, r, n, i, a) {
  if (a > t && a > n || a < t && a < n || n === t)
    return 0;
  var o = (a - t) / (n - t), s = n < t ? 1 : -1;
  (o === 1 || o === 0) && (s = n < t ? 0.5 : -0.5);
  var u = o * (r - e) + e;
  return u === i ? 1 / 0 : u > i ? s : 0;
}
var gr = hi.CMD, Kr = Math.PI * 2, pT = 1e-4;
function gT(e, t) {
  return Math.abs(e - t) < pT;
}
var Vt = [-1, -1, -1], de = [-1, -1];
function yT() {
  var e = de[0];
  de[0] = de[1], de[1] = e;
}
function mT(e, t, r, n, i, a, o, s, u, l) {
  if (l > t && l > n && l > a && l > s || l < t && l < n && l < a && l < s)
    return 0;
  var f = Dy(t, n, a, s, l, Vt);
  if (f === 0)
    return 0;
  for (var h = 0, v = -1, c = void 0, d = void 0, y = 0; y < f; y++) {
    var p = Vt[y], g = p === 0 || p === 1 ? 0.5 : 1, m = Pt(e, r, i, o, p);
    m < u || (v < 0 && (v = Iy(t, n, a, s, de), de[1] < de[0] && v > 1 && yT(), c = Pt(t, n, a, s, de[0]), v > 1 && (d = Pt(t, n, a, s, de[1]))), v === 2 ? p < de[0] ? h += c < t ? g : -g : p < de[1] ? h += d < c ? g : -g : h += s < d ? g : -g : p < de[0] ? h += c < t ? g : -g : h += s < c ? g : -g);
  }
  return h;
}
function _T(e, t, r, n, i, a, o, s) {
  if (s > t && s > n && s > a || s < t && s < n && s < a)
    return 0;
  var u = eb(t, n, a, s, Vt);
  if (u === 0)
    return 0;
  var l = Ay(t, n, a);
  if (l >= 0 && l <= 1) {
    for (var f = 0, h = Bt(t, n, a, l), v = 0; v < u; v++) {
      var c = Vt[v] === 0 || Vt[v] === 1 ? 0.5 : 1, d = Bt(e, r, i, Vt[v]);
      d < o || (Vt[v] < l ? f += h < t ? c : -c : f += a < h ? c : -c);
    }
    return f;
  } else {
    var c = Vt[0] === 0 || Vt[0] === 1 ? 0.5 : 1, d = Bt(e, r, i, Vt[0]);
    return d < o ? 0 : a < t ? c : -c;
  }
}
function ST(e, t, r, n, i, a, o, s) {
  if (s -= t, s > r || s < -r)
    return 0;
  var u = Math.sqrt(r * r - s * s);
  Vt[0] = -u, Vt[1] = u;
  var l = Math.abs(n - i);
  if (l < 1e-4)
    return 0;
  if (l >= Kr - 1e-4) {
    n = 0, i = Kr;
    var f = a ? 1 : -1;
    return o >= Vt[0] + e && o <= Vt[1] + e ? f : 0;
  }
  if (n > i) {
    var h = n;
    n = i, i = h;
  }
  n < 0 && (n += Kr, i += Kr);
  for (var v = 0, c = 0; c < 2; c++) {
    var d = Vt[c];
    if (d + e > o) {
      var y = Math.atan2(s, d), f = a ? 1 : -1;
      y < 0 && (y = Kr + y), (y >= n && y <= i || y + Kr >= n && y + Kr <= i) && (y > Math.PI / 2 && y < Math.PI * 1.5 && (f = -f), v += f);
    }
  }
  return v;
}
function um(e, t, r, n, i) {
  for (var a = e.data, o = e.len(), s = 0, u = 0, l = 0, f = 0, h = 0, v, c, d = 0; d < o; ) {
    var y = a[d++], p = d === 1;
    switch (y === gr.M && d > 1 && (r || (s += ir(u, l, f, h, n, i))), p && (u = a[d], l = a[d + 1], f = u, h = l), y) {
      case gr.M:
        f = a[d++], h = a[d++], u = f, l = h;
        break;
      case gr.L:
        if (r) {
          if (kn(u, l, a[d], a[d + 1], t, n, i))
            return !0;
        } else
          s += ir(u, l, a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case gr.C:
        if (r) {
          if (cT(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], t, n, i))
            return !0;
        } else
          s += mT(u, l, a[d++], a[d++], a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case gr.Q:
        if (r) {
          if (vT(u, l, a[d++], a[d++], a[d], a[d + 1], t, n, i))
            return !0;
        } else
          s += _T(u, l, a[d++], a[d++], a[d], a[d + 1], n, i) || 0;
        u = a[d++], l = a[d++];
        break;
      case gr.A:
        var g = a[d++], m = a[d++], _ = a[d++], S = a[d++], b = a[d++], w = a[d++];
        d += 1;
        var C = !!(1 - a[d++]);
        v = Math.cos(b) * _ + g, c = Math.sin(b) * S + m, p ? (f = v, h = c) : s += ir(u, l, v, c, n, i);
        var M = (n - g) * S / _ + g;
        if (r) {
          if (dT(g, m, S, b, b + w, C, t, M, i))
            return !0;
        } else
          s += ST(g, m, S, b, b + w, C, M, i);
        u = Math.cos(b + w) * _ + g, l = Math.sin(b + w) * S + m;
        break;
      case gr.R:
        f = u = a[d++], h = l = a[d++];
        var x = a[d++], D = a[d++];
        if (v = f + x, c = h + D, r) {
          if (kn(f, h, v, h, t, n, i) || kn(v, h, v, c, t, n, i) || kn(v, c, f, c, t, n, i) || kn(f, c, f, h, t, n, i))
            return !0;
        } else
          s += ir(v, h, v, c, n, i), s += ir(f, c, f, h, n, i);
        break;
      case gr.Z:
        if (r) {
          if (kn(u, l, f, h, t, n, i))
            return !0;
        } else
          s += ir(u, l, f, h, n, i);
        u = f, l = h;
        break;
    }
  }
  return !r && !gT(l, h) && (s += ir(u, l, f, h, n, i) || 0), s !== 0;
}
function bT(e, t, r) {
  return um(e, 0, !1, t, r);
}
function wT(e, t, r, n) {
  return um(e, t, !0, r, n);
}
var lm = _t({
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
}, mn), TT = {
  style: _t({
    fill: !0,
    stroke: !0,
    strokePercent: !0,
    fillOpacity: !0,
    strokeOpacity: !0,
    lineDashOffset: !0,
    lineWidth: !0,
    miterLimit: !0
  }, js.style)
}, al = Us.concat([
  "invisible",
  "culling",
  "z",
  "z2",
  "zlevel",
  "parent"
]), yt = (function(e) {
  z(t, e);
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
      for (var s = 0; s < al.length; ++s)
        i[al[s]] = this[al[s]];
      i.__dirty |= Qt;
    } else this._decalEl && (this._decalEl = null);
  }, t.prototype.getDecalElement = function() {
    return this._decalEl;
  }, t.prototype._init = function(r) {
    var n = mt(r);
    this.shape = this.getDefaultShape();
    var i = this.getDefaultStyle();
    i && this.useStyle(i);
    for (var a = 0; a < n.length; a++) {
      var o = n[a], s = r[o];
      o === "style" ? this.style ? R(this.style, s) : this.useStyle(s) : o === "shape" ? R(this.shape, s) : e.prototype.attrKV.call(this, o, s);
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
      if (N(r)) {
        var n = ca(r, 0);
        return n > 0.5 ? vf : n > 0.2 ? Pb : df;
      } else if (r)
        return df;
    }
    return vf;
  }, t.prototype.getInsideTextStroke = function(r) {
    var n = this.style.fill;
    if (N(n)) {
      var i = this.__zr, a = !!(i && i.isDarkMode()), o = ca(r, 0) < cf;
      if (a === o)
        return n;
    }
  }, t.prototype.buildPath = function(r, n, i) {
  }, t.prototype.pathUpdated = function() {
    this.__dirty &= ~qn;
  }, t.prototype.getUpdatedPathProxy = function(r) {
    return !this.path && this.createPathProxy(), this.path.beginPath(), this.buildPath(this.path, this.shape, r), this.path;
  }, t.prototype.createPathProxy = function() {
    this.path = new hi(!1);
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
      (a || this.__dirty & qn) && (o.beginPath(), this.buildPath(o, this.shape, !1), this.pathUpdated()), r = o.getBoundingRect();
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
        if (l > 1e-10 && (this.hasFill() || (u = Math.max(u, this.strokeContainThreshold)), wT(s, u / l, r, n)))
          return !0;
      }
      if (this.hasFill())
        return bT(s, r, n);
    }
    return !1;
  }, t.prototype.dirtyShape = function() {
    this.__dirty |= qn, this._rect && (this._rect = null), this._decalEl && this._decalEl.dirtyShape(), this.markRedraw();
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
    return i || (i = this.shape = {}), typeof r == "string" ? i[r] = n : R(i, r), this.dirtyShape(), this;
  }, t.prototype.shapeChanged = function() {
    return !!(this.__dirty & qn);
  }, t.prototype.createStyle = function(r) {
    return Ia(lm, r);
  }, t.prototype._innerSaveToNormal = function(r) {
    e.prototype._innerSaveToNormal.call(this, r);
    var n = this._normalState;
    r.shape && !n.shape && (n.shape = R({}, this.shape));
  }, t.prototype._applyStateObj = function(r, n, i, a, o, s) {
    if (e.prototype._applyStateObj.call(this, r, n, i, a, o, s), this.__inHover !== Ws) {
      var u = !(n && a), l;
      if (n && n.shape ? o ? a ? l = n.shape : (l = R({}, i.shape), R(l, n.shape)) : (l = R({}, a ? this.shape : i.shape), R(l, n.shape)) : u && (l = i.shape), l)
        if (o) {
          this.shape = R({}, this.shape);
          for (var f = {}, h = mt(l), v = 0; v < h.length; v++) {
            var c = h[v];
            typeof l[c] == "object" ? this.shape[c] = l[c] : f[c] = l[c];
          }
          this._transitionState(r, {
            shape: f
          }, s);
        } else
          this.shape = l, this.dirtyShape();
    }
  }, t.prototype._mergeStates = function(r) {
    for (var n = e.prototype._mergeStates.call(this, r), i, a = 0; a < r.length; a++) {
      var o = r[a];
      o.shape && (i = i || {}, this._mergeStyle(i, o.shape));
    }
    return i && (n.shape = i), n;
  }, t.prototype.getAnimationStyleProps = function() {
    return TT;
  }, t.prototype.isZeroArea = function() {
    return !1;
  }, t.extend = function(r) {
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
    })(t);
    for (var i in r)
      typeof r[i] == "function" && (n.prototype[i] = r[i]);
    return n;
  }, t.initDefaultProps = (function() {
    var r = t.prototype;
    r.type = "path", r.strokeContainThreshold = 5, r.segmentIgnoreThreshold = 0, r.subPixelOptimize = !1, r.autoBatch = !1, r.__dirty = Qt | Wi | qn;
  })(), t;
})(Oa), CT = _t({
  strokeFirst: !0,
  font: Pr,
  x: 0,
  y: 0,
  textAlign: "left",
  textBaseline: "top",
  miterLimit: 2
}, lm), hs = (function(e) {
  z(t, e);
  function t() {
    return e !== null && e.apply(this, arguments) || this;
  }
  return t.prototype.hasStroke = function() {
    return sm(this.style);
  }, t.prototype.hasFill = function() {
    var r = this.style, n = r.fill;
    return n != null && n !== "none";
  }, t.prototype.createStyle = function(r) {
    return Ia(CT, r);
  }, t.prototype.setBoundingRect = function(r) {
    this._rect = r;
  }, t.prototype.getBoundingRect = function() {
    return this._rect || (this._rect = aT(this.style)), this._rect;
  }, t.initDefaultProps = (function() {
    var r = t.prototype;
    r.dirtyRectTolerance = 10;
  })(), t;
})(Oa);
hs.prototype.type = "tspan";
var xT = _t({
  x: 0,
  y: 0
}, mn), MT = {
  style: _t({
    x: !0,
    y: !0,
    width: !0,
    height: !0,
    sx: !0,
    sy: !0,
    sWidth: !0,
    sHeight: !0
  }, js.style)
};
function DT(e) {
  return !!(e && typeof e != "string" && e.width && e.height);
}
var kr = (function(e) {
  z(t, e);
  function t() {
    return e !== null && e.apply(this, arguments) || this;
  }
  return t.prototype.createStyle = function(r) {
    return Ia(xT, r);
  }, t.prototype._getSize = function(r) {
    var n = this.style, i = n[r];
    if (i != null)
      return i;
    var a = DT(n.image) ? n.image : this.__image;
    if (!a)
      return 0;
    var o = r === "width" ? "height" : "width", s = n[o];
    return s == null ? a[r] : a[r] / a[o] * s;
  }, t.prototype.getWidth = function() {
    return this._getSize("width");
  }, t.prototype.getHeight = function() {
    return this._getSize("height");
  }, t.prototype.getAnimationStyleProps = function() {
    return MT;
  }, t.prototype.getBoundingRect = function() {
    var r = this.style;
    return this._rect || (this._rect = new Y(r.x || 0, r.y || 0, this.getWidth(), this.getHeight())), this._rect;
  }, t;
})(Oa);
kr.prototype.type = "image";
function IT(e, t) {
  var r = t.x, n = t.y, i = t.width, a = t.height, o = t.r, s, u, l, f;
  i < 0 && (r = r + i, i = -i), a < 0 && (n = n + a, a = -a), typeof o == "number" ? s = u = l = f = o : o instanceof Array ? o.length === 1 ? s = u = l = f = o[0] : o.length === 2 ? (s = l = o[0], u = f = o[1]) : o.length === 3 ? (s = o[0], u = f = o[1], l = o[2]) : (s = o[0], u = o[1], l = o[2], f = o[3]) : s = u = l = f = 0;
  var h;
  s + u > i && (h = s + u, s *= i / h, u *= i / h), l + f > i && (h = l + f, l *= i / h, f *= i / h), u + l > a && (h = u + l, u *= a / h, l *= a / h), s + f > a && (h = s + f, s *= a / h, f *= a / h), e.moveTo(r + s, n), e.lineTo(r + i - u, n), u !== 0 && e.arc(r + i - u, n + u, u, -Math.PI / 2, 0), e.lineTo(r + i, n + a - l), l !== 0 && e.arc(r + i - l, n + a - l, l, 0, Math.PI / 2), e.lineTo(r + f, n + a), f !== 0 && e.arc(r + f, n + a - f, f, Math.PI / 2, Math.PI), e.lineTo(r, n + s), s !== 0 && e.arc(r + s, n + s, s, Math.PI, Math.PI * 1.5), e.closePath();
}
var jn = Math.round;
function fm(e, t, r) {
  if (t) {
    var n = t.x1, i = t.x2, a = t.y1, o = t.y2;
    e.x1 = n, e.x2 = i, e.y1 = a, e.y2 = o;
    var s = r && r.lineWidth;
    return s && (jn(n * 2) === jn(i * 2) && (e.x1 = e.x2 = hn(n, s, !0)), jn(a * 2) === jn(o * 2) && (e.y1 = e.y2 = hn(a, s, !0))), e;
  }
}
function hm(e, t, r) {
  if (t) {
    var n = t.x, i = t.y, a = t.width, o = t.height;
    e.x = n, e.y = i, e.width = a, e.height = o;
    var s = r && r.lineWidth;
    return s && (e.x = hn(n, s, !0), e.y = hn(i, s, !0), e.width = Math.max(hn(n + a, s, !1) - e.x, a === 0 ? 0 : 1), e.height = Math.max(hn(i + o, s, !1) - e.y, o === 0 ? 0 : 1)), e;
  }
}
function hn(e, t, r) {
  if (!t)
    return e;
  var n = jn(e * 2);
  return (n + jn(t)) % 2 === 0 ? n / 2 : (n + (r ? 1 : -1)) / 2;
}
var AT = /* @__PURE__ */ (function() {
  function e() {
    this.x = 0, this.y = 0, this.width = 0, this.height = 0;
  }
  return e;
})(), PT = {}, Rt = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new AT();
  }, t.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = hm(PT, n, this.style);
      i = u.x, a = u.y, o = u.width, s = u.height, u.r = n.r, n = u;
    } else
      i = n.x, a = n.y, o = n.width, s = n.height;
    n.r ? IT(r, n) : r.rect(i, a, o, s);
  }, t.prototype.isZeroArea = function() {
    return !this.shape.width || !this.shape.height;
  }, t;
})(yt);
Rt.prototype.type = "rect";
var Wv = {
  fill: "#000"
}, Xv = 2, ke = {}, LT = {
  style: _t({
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
  }, js.style)
}, Kt = (function(e) {
  z(t, e);
  function t(r) {
    var n = e.call(this) || this;
    return n.type = "text", n._children = [], n._defaultStyle = Wv, n.attr(r), n;
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
    this._childCursor = 0, BT(this.style), this.style.rich ? this._updateRichTexts() : this._updatePlainTexts(), this._children.length = this._childCursor, this.styleUpdated();
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
      for (var r = new Y(0, 0, 0, 0), n = this._children, i = [], a = null, o = 0; o < n.length; o++) {
        var s = n[o], u = s.getBoundingRect(), l = s.getLocalTransform(i);
        l ? (r.copy(u), r.applyTransform(l), a = a || r.clone(), a.union(r)) : (a = a || u.clone(), a.union(u));
      }
      this._rect = a || r;
    }
    return this._rect;
  }, t.prototype.setDefaultTextStyle = function(r) {
    this._defaultStyle = r || Wv;
  }, t.prototype.setTextContent = function(r) {
  }, t.prototype._mergeStyle = function(r, n) {
    if (!n)
      return r;
    var i = n.rich, a = r.rich || i && {};
    return R(r, n), i && a ? (this._mergeRich(a, i), r.rich = a) : a && (r.rich = a), r;
  }, t.prototype._mergeRich = function(r, n) {
    for (var i = mt(n), a = 0; a < i.length; a++) {
      var o = i[a];
      r[o] = r[o] || {}, R(r[o], n[o]);
    }
  }, t.prototype.getAnimationStyleProps = function() {
    return LT;
  }, t.prototype._getOrCreateChild = function(r) {
    var n = this._children[this._childCursor];
    return (!n || !(n instanceof r)) && (n = new r()), this._children[this._childCursor++] = n, n.__zr = this.__zr, n.parent = this, n;
  }, t.prototype._updatePlainTexts = function() {
    var r = this.style, n = r.font || Pr, i = r.padding, a = this._defaultStyle, o = r.x || 0, s = r.y || 0, u = r.align || a.align || "left", l = r.verticalAlign || a.verticalAlign || "top";
    Nv(ke, a.overflowRect, o, s, u, l), o = ke.baseX, s = ke.baseY;
    var f = Jv(r), h = Qw(f, r, ke.outerWidth, ke.outerHeight), v = ol(r), c = !!r.backgroundColor, d = h.outerHeight, y = h.outerWidth, p = h.lines, g = h.lineHeight;
    this.isTruncated = !!h.isTruncated;
    var m = o, _ = yn(s, h.contentHeight, l);
    if (v || i) {
      var S = ui(o, y, u), b = yn(s, d, l);
      v && this._renderBackground(r, r, S, b, y, d);
    }
    _ += g / 2, i && (m = Qv(o, u, i), l === "top" ? _ += i[0] : l === "bottom" && (_ -= i[2]));
    for (var w = 0, C = !1, M = !1, x = jv("fill" in r ? r.fill : (M = !0, a.fill)), D = Kv("stroke" in r ? r.stroke : !c && (!a.autoStroke || M) ? (w = Xv, C = !0, a.stroke) : null), I = r.textShadowBlur > 0, L = 0; L < p.length; L++) {
      var A = this._getOrCreateChild(hs), P = A.createStyle();
      A.useStyle(P), P.text = p[L], P.x = m, P.y = _, P.textAlign = u, P.textBaseline = "middle", P.opacity = r.opacity, P.strokeFirst = !0, I && (P.shadowBlur = r.textShadowBlur || 0, P.shadowColor = r.textShadowColor || "transparent", P.shadowOffsetX = r.textShadowOffsetX || 0, P.shadowOffsetY = r.textShadowOffsetY || 0), P.stroke = D, P.fill = x, D && (P.lineWidth = r.lineWidth || w, P.lineDash = r.lineDash, P.lineDashOffset = r.lineDashOffset || 0), P.font = n, $v(P, r), _ += g, A.setBoundingRect(_f(P, h.contentWidth, h.calculatedLineHeight, C ? 0 : null));
    }
  }, t.prototype._updateRichTexts = function() {
    var r = this.style, n = this._defaultStyle, i = r.align || n.align, a = r.verticalAlign || n.verticalAlign, o = r.x || 0, s = r.y || 0;
    Nv(ke, n.overflowRect, o, s, i, a), o = ke.baseX, s = ke.baseY;
    var u = Jv(r), l = eT(u, r, ke.outerWidth, ke.outerHeight, i), f = l.width, h = l.outerWidth, v = l.outerHeight, c = r.padding;
    this.isTruncated = !!l.isTruncated;
    var d = ui(o, h, i), y = yn(s, v, a), p = d, g = y;
    c && (p += c[3], g += c[0]);
    var m = p + f;
    ol(r) && this._renderBackground(r, r, d, y, h, v);
    for (var _ = !!r.backgroundColor, S = 0; S < l.lines.length; S++) {
      for (var b = l.lines[S], w = b.tokens, C = w.length, M = b.lineHeight, x = b.width, D = 0, I = p, L = m, A = C - 1, P = void 0; D < C && (P = w[D], !P.align || P.align === "left"); )
        this._placeToken(P, r, M, g, I, "left", _), x -= P.width, I += P.width, D++;
      for (; A >= 0 && (P = w[A], P.align === "right"); )
        this._placeToken(P, r, M, g, L, "right", _), x -= P.width, L -= P.width, A--;
      for (I += (f - (I - p) - (m - L) - x) / 2; D <= A; )
        P = w[D], this._placeToken(P, r, M, g, I + P.width / 2, "center", _), I += P.width, D++;
      g += M;
    }
  }, t.prototype._placeToken = function(r, n, i, a, o, s, u) {
    var l = n.rich[r.styleName] || {};
    l.text = r.text;
    var f = r.verticalAlign, h = a + i / 2;
    f === "top" ? h = a + r.height / 2 : f === "bottom" && (h = a + i - r.height / 2);
    var v = !r.isLineHolder && ol(l);
    v && this._renderBackground(l, n, s === "right" ? o - r.width : s === "center" ? o - r.width / 2 : o, h - r.height / 2, r.width, r.height);
    var c = !!l.backgroundColor, d = r.textPadding;
    d && (o = Qv(o, s, d), h -= r.height / 2 - d[0] - r.innerHeight / 2);
    var y = this._getOrCreateChild(hs), p = y.createStyle();
    y.useStyle(p);
    var g = this._defaultStyle, m = !1, _ = 0, S = !1, b = jv("fill" in l ? l.fill : "fill" in n ? n.fill : (m = !0, g.fill)), w = Kv("stroke" in l ? l.stroke : "stroke" in n ? n.stroke : !c && !u && (!g.autoStroke || m) ? (_ = Xv, S = !0, g.stroke) : null), C = l.textShadowBlur > 0 || n.textShadowBlur > 0;
    p.text = r.text, p.x = o, p.y = h, C && (p.shadowBlur = l.textShadowBlur || n.textShadowBlur || 0, p.shadowColor = l.textShadowColor || n.textShadowColor || "transparent", p.shadowOffsetX = l.textShadowOffsetX || n.textShadowOffsetX || 0, p.shadowOffsetY = l.textShadowOffsetY || n.textShadowOffsetY || 0), p.textAlign = s, p.textBaseline = "middle", p.font = r.font || Pr, p.opacity = dn(l.opacity, n.opacity, 1), $v(p, l), w && (p.lineWidth = dn(l.lineWidth, n.lineWidth, _), p.lineDash = G(l.lineDash, n.lineDash), p.lineDashOffset = n.lineDashOffset || 0, p.stroke = w), b && (p.fill = b), y.setBoundingRect(_f(p, r.contentWidth, r.contentHeight, S ? 0 : null));
  }, t.prototype._renderBackground = function(r, n, i, a, o, s) {
    var u = r.backgroundColor, l = r.borderWidth, f = r.borderColor, h = u && u.image, v = u && !h, c = r.borderRadius, d = this, y, p;
    if (v || r.lineHeight || l && f) {
      y = this._getOrCreateChild(Rt), y.useStyle(y.createStyle()), y.style.fill = null;
      var g = y.shape;
      g.x = i, g.y = a, g.width = o, g.height = s, g.r = c, y.dirtyShape();
    }
    if (v) {
      var m = y.style;
      m.fill = u || null, m.fillOpacity = G(r.fillOpacity, 1);
    } else if (h) {
      p = this._getOrCreateChild(kr), p.onload = function() {
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
    S.shadowBlur = r.shadowBlur || 0, S.shadowColor = r.shadowColor || "transparent", S.shadowOffsetX = r.shadowOffsetX || 0, S.shadowOffsetY = r.shadowOffsetY || 0, S.opacity = dn(r.opacity, n.opacity, 1);
  }, t.makeFont = function(r) {
    var n = "";
    return kT(r) && (n = [
      r.fontStyle,
      r.fontWeight,
      OT(r.fontSize),
      r.fontFamily || "sans-serif"
    ].join(" ")), n && Me(n) || r.textFont || r.font;
  }, t;
})(Oa), ET = { left: !0, right: 1, center: 1 }, RT = { top: 1, bottom: 1, middle: 1 }, qv = ["fontStyle", "fontWeight", "fontSize", "fontFamily"];
function OT(e) {
  return typeof e == "string" && (e.indexOf("px") !== -1 || e.indexOf("rem") !== -1 || e.indexOf("em") !== -1) ? e : isNaN(+e) ? fh + "px" : e + "px";
}
function $v(e, t) {
  for (var r = 0; r < qv.length; r++) {
    var n = qv[r], i = t[n];
    i != null && (e[n] = i);
  }
}
function kT(e) {
  return e.fontSize != null || e.fontFamily || e.fontWeight;
}
function BT(e) {
  return Zv(e), T(e.rich, Zv), e;
}
function Zv(e) {
  if (e) {
    e.font = Kt.makeFont(e);
    var t = e.align;
    t === "middle" && (t = "center"), e.align = t == null || ET[t] ? t : "left";
    var r = e.verticalAlign;
    r === "center" && (r = "middle"), e.verticalAlign = r == null || RT[r] ? r : "top";
    var n = e.padding;
    n && (e.padding = Vs(e.padding));
  }
}
function Kv(e, t) {
  return e == null || t <= 0 || e === "transparent" || e === "none" ? null : e.image || e.colorStops ? "#000" : e;
}
function jv(e) {
  return e == null || e === "none" ? null : e.image || e.colorStops ? "#000" : e;
}
function Qv(e, t, r) {
  return t === "right" ? e - r[1] : t === "center" ? e + r[3] / 2 - r[1] / 2 : e + r[3];
}
function Jv(e) {
  var t = e.text;
  return t != null && (t += ""), t;
}
function ol(e) {
  return !!(e.backgroundColor || e.lineHeight || e.borderWidth && e.borderColor);
}
var ft = dt(), NT = function(e, t, r, n) {
  if (n) {
    var i = ft(n);
    i.dataIndex = r, i.dataType = t, i.seriesIndex = e, i.ssrType = "chart", n.type === "group" && n.traverse(function(a) {
      var o = ft(a);
      o.seriesIndex = e, o.dataIndex = r, o.dataType = t, o.ssrType = "chart";
    });
  }
}, Qs = "undefined", cm = "series", vm = U(["tooltip", "label", "itemName", "itemId", "itemGroupId", "itemChildGroupId", "seriesName"]), ue = "original", zt = "arrayRows", Pe = "objectRows", $e = "keyedColumns", Ir = "typedArray", dm = "unknown", Ie = "column", In = "row", FT = [
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
    function e(t) {
      T(FT, function(r) {
        this[r] = ht(t[r], t);
      }, this);
    }
    return e;
  })()
);
function zT(e, t) {
  return t.mainType === cm ? e.getViewOfSeriesModel(t) : e.getViewOfComponentModel(t);
}
var td = 1, ed = {}, gm = dt(), Ph = dt(), ym = 0, Lh = 1, Eh = 2, Lr = ["emphasis", "blur", "select"], rd = ["normal", "emphasis", "blur", "select"], HT = 10, VT = 9, _n = "highlight", Ho = "downplay", cs = "select", wf = "unselect", vs = "toggleSelect", Rh = "selectchanged";
function Bn(e) {
  return e != null && e !== "none";
}
function Js(e, t, r) {
  e.onHoverStateChange && (e.hoverState || 0) !== r && e.onHoverStateChange(t), e.hoverState = r;
}
function mm(e) {
  Js(e, "emphasis", Eh);
}
function _m(e) {
  e.hoverState === Eh && Js(e, "normal", ym);
}
function Oh(e) {
  Js(e, "blur", Lh);
}
function Sm(e) {
  e.hoverState === Lh && Js(e, "normal", ym);
}
function GT(e) {
  e.selected = !0;
}
function UT(e) {
  e.selected = !1;
}
function nd(e, t, r) {
  t(e, r);
}
function vr(e, t, r) {
  nd(e, t, r), e.isGroup && e.traverse(function(n) {
    nd(n, t, r);
  });
}
function YT(e, t, r, n) {
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
function WT(e, t, r, n) {
  var i = r && st(r, "select") >= 0, a = !1;
  if (e instanceof yt) {
    var o = gm(e), s = i && o.selectFill || o.normalFill, u = i && o.selectStroke || o.normalStroke;
    if (Bn(s) || Bn(u)) {
      n = n || {};
      var l = n.style || {};
      l.fill === "inherit" ? (a = !0, n = R({}, n), l = R({}, l), l.fill = s) : !Bn(l.fill) && Bn(s) ? (a = !0, n = R({}, n), l = R({}, l), l.fill = of(s)) : !Bn(l.stroke) && Bn(u) && (a || (n = R({}, n), l = R({}, l)), l.stroke = of(u)), n.style = l;
    }
  }
  if (n && n.z2 == null) {
    a || (n = R({}, n));
    var f = e.z2EmphasisLift;
    n.z2 = e.z2 + (f ?? HT);
  }
  return n;
}
function XT(e, t, r) {
  if (r && r.z2 == null) {
    r = R({}, r);
    var n = e.z2SelectLift;
    r.z2 = e.z2 + (n ?? VT);
  }
  return r;
}
function qT(e, t, r) {
  var n = st(e.currentStates, t) >= 0, i = e.style.opacity, a = n ? null : YT(e, ["opacity"], t, {
    opacity: 1
  });
  r = r || {};
  var o = r.style || {};
  return o.opacity == null && (r = R({}, r), o = R({
    // Already being applied 'emphasis'. DON'T mul opacity multiple times.
    opacity: n ? i : a.opacity * 0.1
  }, o), r.style = o), r;
}
function sl(e, t) {
  var r = this.states[e];
  if (this.style) {
    if (e === "emphasis")
      return WT(this, e, t, r);
    if (e === "blur")
      return qT(this, e, r);
    if (e === "select")
      return XT(this, e, r);
  }
  return r;
}
function $T(e) {
  e.stateProxy = sl;
  var t = e.getTextContent(), r = e.getTextGuideLine();
  t && (t.stateProxy = sl), r && (r.stateProxy = sl);
}
function id(e, t) {
  !Cm(e, t) && !e.__highByOuter && vr(e, mm);
}
function ad(e, t) {
  !Cm(e, t) && !e.__highByOuter && vr(e, _m);
}
function Tf(e, t) {
  e.__highByOuter |= 1 << (t || 0), vr(e, mm);
}
function Cf(e, t) {
  !(e.__highByOuter &= ~(1 << (t || 0))) && vr(e, _m);
}
function ZT(e) {
  vr(e, Oh);
}
function bm(e) {
  vr(e, Sm);
}
function wm(e) {
  vr(e, GT);
}
function Tm(e) {
  vr(e, UT);
}
function Cm(e, t) {
  return e.__highDownSilentOnTouch && t.zrByTouch;
}
function xm(e) {
  var t = e.getModel(), r = [], n = [];
  t.eachComponent(function(i, a) {
    var o = Ph(a), s = zT(e, a), u = i === "series";
    !u && n.push(s), o.isBlured && (s.group.traverse(function(l) {
      Sm(l);
    }), u && r.push(a)), o.isBlured = !1;
  }), T(n, function(i) {
    i && i.toggleBlurSeries && i.toggleBlurSeries(r, !1, t);
  });
}
function xf(e, t, r, n) {
  var i = n.getModel();
  r = r || "coordinateSystem";
  function a(l, f) {
    for (var h = 0; h < f.length; h++) {
      var v = l.getItemGraphicEl(f[h]);
      v && bm(v);
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
          p.__highByOuter && f && t === "self" || Oh(p);
        }), $t(t))
          a(l.getData(), t);
        else if (F(t))
          for (var d = mt(t), y = 0; y < d.length; y++)
            a(l.getData(d[y]), t[d[y]]);
        u.push(l), Ph(l).isBlured = !0;
      }
    }), i.eachComponent(function(l, f) {
      if (l !== "series") {
        var h = n.getViewOfComponentModel(f);
        h && h.toggleBlurSeries && h.toggleBlurSeries(u, !0, i);
      }
    });
  }
}
function Mf(e, t, r) {
  if (!(e == null || t == null)) {
    var n = r.getModel().getComponent(e, t);
    if (n) {
      Ph(n).isBlured = !0;
      var i = r.getViewOfComponentModel(n);
      !i || !i.focusBlurEnabled || i.group.traverse(function(a) {
        Oh(a);
      });
    }
  }
}
function KT(e, t, r) {
  var n = e.seriesIndex, i = e.getData(t.dataType);
  if (i) {
    var a = Ea(i, t);
    a = (B(a) ? a[0] : a) || 0;
    var o = i.getItemGraphicEl(a);
    if (!o)
      for (var s = i.count(), u = 0; !o && u < s; )
        o = i.getItemGraphicEl(u++);
    if (o) {
      var l = ft(o);
      xf(n, l.focus, l.blurScope, r);
    } else {
      var f = e.get(["emphasis", "focus"]), h = e.get(["emphasis", "blurScope"]);
      f != null && xf(n, f, h, r);
    }
  }
}
function kh(e, t, r, n) {
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
    if (ft(s[l]).focus === "self") {
      u = !0;
      break;
    }
  return {
    focusSelf: u,
    dispatchers: s
  };
}
function jT(e, t, r) {
  var n = ft(e), i = kh(n.componentMainType, n.componentIndex, n.componentHighDownName, r), a = i.dispatchers, o = i.focusSelf;
  a ? (o && Mf(n.componentMainType, n.componentIndex, r), T(a, function(s) {
    return id(s, t);
  })) : (xf(n.seriesIndex, n.focus, n.blurScope, r), n.focus === "self" && Mf(n.componentMainType, n.componentIndex, r), id(e, t));
}
function QT(e, t, r) {
  xm(r);
  var n = ft(e), i = kh(n.componentMainType, n.componentIndex, n.componentHighDownName, r).dispatchers;
  i ? T(i, function(a) {
    return ad(a, t);
  }) : ad(e, t);
}
function JT(e, t, r) {
  if (If(t)) {
    var n = t.dataType, i = e.getData(n), a = Ea(i, t);
    B(a) || (a = [a]), e[t.type === vs ? "toggleSelect" : t.type === cs ? "select" : "unselect"](a, n);
  }
}
function od(e) {
  var t = e.getAllData();
  T(t, function(r) {
    var n = r.data, i = r.type;
    n.eachItemGraphicEl(function(a, o) {
      e.isSelected(o, i) ? wm(a) : Tm(a);
    });
  });
}
function tC(e) {
  var t = [];
  return e.eachSeries(function(r) {
    var n = r.getAllData();
    T(n, function(i) {
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
function ds(e, t, r) {
  Mm(e, !0), vr(e, $T), rC(e, t, r);
}
function eC(e) {
  Mm(e, !1);
}
function FE(e, t, r, n) {
  n ? eC(e) : ds(e, t, r);
}
function rC(e, t, r) {
  var n = ft(e);
  t != null ? (n.focus = t, n.blurScope = r) : n.focus && (n.focus = null);
}
var sd = ["emphasis", "blur", "select"], nC = {
  itemStyle: "getItemStyle",
  lineStyle: "getLineStyle",
  areaStyle: "getAreaStyle"
};
function zE(e, t, r, n) {
  r = r || "itemStyle";
  for (var i = 0; i < sd.length; i++) {
    var a = sd[i], o = t.getModel([a, r]), s = e.ensureState(a);
    s.style = o[nC[r]]();
  }
}
function Mm(e, t) {
  var r = t === !1, n = e;
  e.highDownSilentOnTouch && (n.__highDownSilentOnTouch = e.highDownSilentOnTouch), (!r || n.__highDownDispatcher) && (n.__highByOuter = n.__highByOuter || 0, n.__highDownDispatcher = !r);
}
function Df(e) {
  return !!(e && e.__highDownDispatcher);
}
function iC(e) {
  var t = ed[e];
  return t == null && td <= 32 && (t = ed[e] = td++), t;
}
function If(e) {
  var t = e.type;
  return t === cs || t === wf || t === vs;
}
function ud(e) {
  var t = e.type;
  return t === _n || t === Ho;
}
function aC(e) {
  var t = gm(e);
  t.normalFill = e.style.fill, t.normalStroke = e.style.stroke;
  var r = e.states.select || {};
  t.selectFill = r.style && r.style.fill || null, t.selectStroke = r.style && r.style.stroke || null;
}
var Nn = hi.CMD, oC = [[], [], []], ld = Math.sqrt, sC = Math.atan2;
function uC(e, t) {
  if (t) {
    var r = e.data, n = e.len(), i, a, o, s, u, l, f = Nn.M, h = Nn.C, v = Nn.L, c = Nn.R, d = Nn.A, y = Nn.Q;
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
          var p = t[4], g = t[5], m = ld(t[0] * t[0] + t[1] * t[1]), _ = ld(t[2] * t[2] + t[3] * t[3]), S = sC(-t[1] / _, t[0] / m);
          r[o] *= m, r[o++] += p, r[o] *= _, r[o++] += g, r[o++] *= m, r[o++] *= _, r[o++] += S, r[o++] += S, o += 2, s = o;
          break;
        case c:
          l[0] = r[o++], l[1] = r[o++], ie(l, l, t), r[s++] = l[0], r[s++] = l[1], l[0] += r[o++], l[1] += r[o++], ie(l, l, t), r[s++] = l[0], r[s++] = l[1];
      }
      for (u = 0; u < a; u++) {
        var b = oC[u];
        b[0] = r[o++], b[1] = r[o++], ie(b, b, t), r[s++] = b[0], r[s++] = b[1];
      }
    }
    e.increaseVersion();
  }
}
var ul = Math.sqrt, to = Math.sin, eo = Math.cos, Pi = Math.PI;
function fd(e) {
  return Math.sqrt(e[0] * e[0] + e[1] * e[1]);
}
function Af(e, t) {
  return (e[0] * t[0] + e[1] * t[1]) / (fd(e) * fd(t));
}
function hd(e, t) {
  return (e[0] * t[1] < e[1] * t[0] ? -1 : 1) * Math.acos(Af(e, t));
}
function cd(e, t, r, n, i, a, o, s, u, l, f) {
  var h = u * (Pi / 180), v = eo(h) * (e - r) / 2 + to(h) * (t - n) / 2, c = -1 * to(h) * (e - r) / 2 + eo(h) * (t - n) / 2, d = v * v / (o * o) + c * c / (s * s);
  d > 1 && (o *= ul(d), s *= ul(d));
  var y = (i === a ? -1 : 1) * ul((o * o * (s * s) - o * o * (c * c) - s * s * (v * v)) / (o * o * (c * c) + s * s * (v * v))) || 0, p = y * o * c / s, g = y * -s * v / o, m = (e + r) / 2 + eo(h) * p - to(h) * g, _ = (t + n) / 2 + to(h) * p + eo(h) * g, S = hd([1, 0], [(v - p) / o, (c - g) / s]), b = [(v - p) / o, (c - g) / s], w = [(-1 * v - p) / o, (-1 * c - g) / s], C = hd(b, w);
  if (Af(b, w) <= -1 && (C = Pi), Af(b, w) >= 1 && (C = 0), C < 0) {
    var M = Math.round(C / Pi * 1e6) / 1e6;
    C = Pi * 2 + M % 2 * Pi;
  }
  f.addData(l, m, _, o, s, S, C, h, a);
}
var lC = /([mlvhzcqtsa])([^mlvhzcqtsa]*)/ig, fC = /-?([0-9]*\.)?[0-9]+([eE]-?[0-9]+)?/g;
function hC(e) {
  var t = new hi();
  if (!e)
    return t;
  var r = 0, n = 0, i = r, a = n, o, s = hi.CMD, u = e.match(lC);
  if (!u)
    return t;
  for (var l = 0; l < u.length; l++) {
    for (var f = u[l], h = f.charAt(0), v = void 0, c = f.match(fC) || [], d = c.length, y = 0; y < d; y++)
      c[y] = parseFloat(c[y]);
    for (var p = 0; p < d; ) {
      var g = void 0, m = void 0, _ = void 0, S = void 0, b = void 0, w = void 0, C = void 0, M = r, x = n, D = void 0, I = void 0;
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
          g = r, m = n, D = t.len(), I = t.data, o === s.C && (g += r - I[D - 4], m += n - I[D - 3]), v = s.C, M = c[p++], x = c[p++], r = c[p++], n = c[p++], t.addData(v, g, m, M, x, r, n);
          break;
        case "s":
          g = r, m = n, D = t.len(), I = t.data, o === s.C && (g += r - I[D - 4], m += n - I[D - 3]), v = s.C, M = r + c[p++], x = n + c[p++], r += c[p++], n += c[p++], t.addData(v, g, m, M, x, r, n);
          break;
        case "Q":
          M = c[p++], x = c[p++], r = c[p++], n = c[p++], v = s.Q, t.addData(v, M, x, r, n);
          break;
        case "q":
          M = c[p++] + r, x = c[p++] + n, r += c[p++], n += c[p++], v = s.Q, t.addData(v, M, x, r, n);
          break;
        case "T":
          g = r, m = n, D = t.len(), I = t.data, o === s.Q && (g += r - I[D - 4], m += n - I[D - 3]), r = c[p++], n = c[p++], v = s.Q, t.addData(v, g, m, r, n);
          break;
        case "t":
          g = r, m = n, D = t.len(), I = t.data, o === s.Q && (g += r - I[D - 4], m += n - I[D - 3]), r += c[p++], n += c[p++], v = s.Q, t.addData(v, g, m, r, n);
          break;
        case "A":
          _ = c[p++], S = c[p++], b = c[p++], w = c[p++], C = c[p++], M = r, x = n, r = c[p++], n = c[p++], v = s.A, cd(M, x, r, n, w, C, _, S, b, v, t);
          break;
        case "a":
          _ = c[p++], S = c[p++], b = c[p++], w = c[p++], C = c[p++], M = r, x = n, r += c[p++], n += c[p++], v = s.A, cd(M, x, r, n, w, C, _, S, b, v, t);
          break;
      }
    }
    (h === "z" || h === "Z") && (v = s.Z, t.addData(v), r = i, n = a), o = v;
  }
  return t.toStatic(), t;
}
var Dm = (function(e) {
  z(t, e);
  function t() {
    return e !== null && e.apply(this, arguments) || this;
  }
  return t.prototype.applyTransform = function(r) {
  }, t;
})(yt);
function Im(e) {
  return e.setData != null;
}
function Am(e, t) {
  var r = hC(e), n = R({}, t);
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
    uC(r, i), this.dirtyShape();
  }, n;
}
function cC(e, t) {
  return new Dm(Am(e, t));
}
function vC(e, t) {
  var r = Am(e, t), n = (function(i) {
    z(a, i);
    function a(o) {
      var s = i.call(this, o) || this;
      return s.applyTransform = r.applyTransform, s.buildPath = r.buildPath, s;
    }
    return a;
  })(Dm);
  return n;
}
function dC(e, t) {
  for (var r = [], n = e.length, i = 0; i < n; i++) {
    var a = e[i];
    r.push(a.getUpdatedPathProxy(!0));
  }
  var o = new yt(t);
  return o.createPathProxy(), o.buildPath = function(s) {
    if (Im(s)) {
      s.appendPath(r);
      var u = s.getContext();
      u && s.rebuildPath(u, 1);
    }
  }, o;
}
var pC = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r = 0;
  }
  return e;
})(), ka = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new pC();
  }, t.prototype.buildPath = function(r, n) {
    r.moveTo(n.cx + n.r, n.cy), r.arc(n.cx, n.cy, n.r, 0, Math.PI * 2);
  }, t;
})(yt);
ka.prototype.type = "circle";
var gC = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.rx = 0, this.ry = 0;
  }
  return e;
})(), tu = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new gC();
  }, t.prototype.buildPath = function(r, n) {
    var i = 0.5522848, a = n.cx, o = n.cy, s = n.rx, u = n.ry, l = s * i, f = u * i;
    r.moveTo(a - s, o), r.bezierCurveTo(a - s, o - f, a - l, o - u, a, o - u), r.bezierCurveTo(a + l, o - u, a + s, o - f, a + s, o), r.bezierCurveTo(a + s, o + f, a + l, o + u, a, o + u), r.bezierCurveTo(a - l, o + u, a - s, o + f, a - s, o), r.closePath();
  }, t;
})(yt);
tu.prototype.type = "ellipse";
var Pm = Math.PI, ll = Pm * 2, jr = Math.sin, Fn = Math.cos, yC = Math.acos, Ot = Math.atan2, vd = Math.abs, ta = Math.sqrt, qi = Math.max, Be = Math.min, Te = 1e-4;
function mC(e, t, r, n, i, a, o, s) {
  var u = r - e, l = n - t, f = o - i, h = s - a, v = h * u - f * l;
  if (!(v * v < Te))
    return v = (f * (t - a) - h * (e - i)) / v, [e + v * u, t + v * l];
}
function ro(e, t, r, n, i, a, o) {
  var s = e - r, u = t - n, l = (o ? a : -a) / ta(s * s + u * u), f = l * u, h = -l * s, v = e + f, c = t + h, d = r + f, y = n + h, p = (v + d) / 2, g = (c + y) / 2, m = d - v, _ = y - c, S = m * m + _ * _, b = i - a, w = v * y - d * c, C = (_ < 0 ? -1 : 1) * ta(qi(0, b * b * S - w * w)), M = (w * _ - m * C) / S, x = (-w * m - _ * C) / S, D = (w * _ + m * C) / S, I = (-w * m + _ * C) / S, L = M - p, A = x - g, P = D - p, k = I - g;
  return L * L + A * A > P * P + k * k && (M = D, x = I), {
    cx: M,
    cy: x,
    x0: -f,
    y0: -h,
    x1: M * (i / b - 1),
    y1: x * (i / b - 1)
  };
}
function _C(e) {
  var t;
  if (B(e)) {
    var r = e.length;
    if (!r)
      return e;
    r === 1 ? t = [e[0], e[0], 0, 0] : r === 2 ? t = [e[0], e[0], e[1], e[1]] : r === 3 ? t = e.concat(e[2]) : t = e;
  } else
    t = [e, e, e, e];
  return t;
}
function SC(e, t) {
  var r, n = qi(t.r, 0), i = qi(t.r0 || 0, 0), a = n > 0, o = i > 0;
  if (!(!a && !o)) {
    if (a || (n = i, i = 0), i > n) {
      var s = n;
      n = i, i = s;
    }
    var u = t.startAngle, l = t.endAngle;
    if (!(isNaN(u) || isNaN(l))) {
      var f = t.cx, h = t.cy, v = !!t.clockwise, c = vd(l - u), d = c > ll && c % ll;
      if (d > Te && (c = d), !(n > Te))
        e.moveTo(f, h);
      else if (c > ll - Te)
        e.moveTo(f + n * Fn(u), h + n * jr(u)), e.arc(f, h, n, u, l, !v), i > Te && (e.moveTo(f + i * Fn(l), h + i * jr(l)), e.arc(f, h, i, l, u, v));
      else {
        var y = void 0, p = void 0, g = void 0, m = void 0, _ = void 0, S = void 0, b = void 0, w = void 0, C = void 0, M = void 0, x = void 0, D = void 0, I = void 0, L = void 0, A = void 0, P = void 0, k = n * Fn(u), O = n * jr(u), V = i * Fn(l), K = i * jr(l), J = c > Te;
        if (J) {
          var tt = t.cornerRadius;
          tt && (r = _C(tt), y = r[0], p = r[1], g = r[2], m = r[3]);
          var Q = vd(n - i) / 2;
          if (_ = Be(Q, g), S = Be(Q, m), b = Be(Q, y), w = Be(Q, p), x = C = qi(_, S), D = M = qi(b, w), (C > Te || M > Te) && (I = n * Fn(l), L = n * jr(l), A = i * Fn(u), P = i * jr(u), c < Pm)) {
            var rt = mC(k, O, A, P, I, L, V, K);
            if (rt) {
              var nt = k - rt[0], q = O - rt[1], ot = I - rt[0], St = L - rt[1], te = 1 / jr(yC((nt * ot + q * St) / (ta(nt * nt + q * q) * ta(ot * ot + St * St))) / 2), Ze = ta(rt[0] * rt[0] + rt[1] * rt[1]);
              x = Be(C, (n - Ze) / (te + 1)), D = Be(M, (i - Ze) / (te - 1));
            }
          }
        }
        if (!J)
          e.moveTo(f + k, h + O);
        else if (x > Te) {
          var ee = Be(g, x), At = Be(m, x), $ = ro(A, P, k, O, n, ee, v), et = ro(I, L, V, K, n, At, v);
          e.moveTo(f + $.cx + $.x0, h + $.cy + $.y0), x < C && ee === At ? e.arc(f + $.cx, h + $.cy, x, Ot($.y0, $.x0), Ot(et.y0, et.x0), !v) : (ee > 0 && e.arc(f + $.cx, h + $.cy, ee, Ot($.y0, $.x0), Ot($.y1, $.x1), !v), e.arc(f, h, n, Ot($.cy + $.y1, $.cx + $.x1), Ot(et.cy + et.y1, et.cx + et.x1), !v), At > 0 && e.arc(f + et.cx, h + et.cy, At, Ot(et.y1, et.x1), Ot(et.y0, et.x0), !v));
        } else
          e.moveTo(f + k, h + O), e.arc(f, h, n, u, l, !v);
        if (!(i > Te) || !J)
          e.lineTo(f + V, h + K);
        else if (D > Te) {
          var ee = Be(y, D), At = Be(p, D), $ = ro(V, K, I, L, i, -At, v), et = ro(k, O, A, P, i, -ee, v);
          e.lineTo(f + $.cx + $.x0, h + $.cy + $.y0), D < M && ee === At ? e.arc(f + $.cx, h + $.cy, D, Ot($.y0, $.x0), Ot(et.y0, et.x0), !v) : (At > 0 && e.arc(f + $.cx, h + $.cy, At, Ot($.y0, $.x0), Ot($.y1, $.x1), !v), e.arc(f, h, i, Ot($.cy + $.y1, $.cx + $.x1), Ot(et.cy + et.y1, et.cx + et.x1), v), ee > 0 && e.arc(f + et.cx, h + et.cy, ee, Ot(et.y1, et.x1), Ot(et.y0, et.x0), !v));
        } else
          e.lineTo(f + V, h + K), e.arc(f, h, i, l, u, v);
      }
      e.closePath();
    }
  }
}
var bC = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r0 = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0, this.cornerRadius = 0;
  }
  return e;
})(), eu = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new bC();
  }, t.prototype.buildPath = function(r, n) {
    SC(r, n);
  }, t.prototype.isZeroArea = function() {
    return this.shape.startAngle === this.shape.endAngle || this.shape.r === this.shape.r0;
  }, t;
})(yt);
eu.prototype.type = "sector";
var wC = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r = 0, this.r0 = 0;
  }
  return e;
})(), ru = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new wC();
  }, t.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.PI * 2;
    r.moveTo(i + n.r, a), r.arc(i, a, n.r, 0, o, !1), r.moveTo(i + n.r0, a), r.arc(i, a, n.r0, 0, o, !0);
  }, t;
})(yt);
ru.prototype.type = "ring";
function TC(e, t, r, n) {
  var i = [], a = [], o = [], s = [], u, l, f, h;
  if (n) {
    f = [1 / 0, 1 / 0], h = [-1 / 0, -1 / 0];
    for (var v = 0, c = e.length; v < c; v++)
      br(f, f, e[v]), wr(h, h, e[v]);
    br(f, f, n[0]), wr(h, h, n[1]);
  }
  for (var v = 0, c = e.length; v < c; v++) {
    var d = e[v];
    if (r)
      u = e[v ? v - 1 : c - 1], l = e[(v + 1) % c];
    else if (v === 0 || v === c - 1) {
      i.push(cy(e[v]));
      continue;
    } else
      u = e[v - 1], l = e[v + 1];
    vy(a, l, u), Lo(a, a, t);
    var y = ts(d, u), p = ts(d, l), g = y + p;
    g !== 0 && (y /= g, p /= g), Lo(o, a, -y), Lo(s, a, p);
    var m = Kl([], d, o), _ = Kl([], d, s);
    n && (wr(m, m, f), br(m, m, h), wr(_, _, f), br(_, _, h)), i.push(m), i.push(_);
  }
  return r && i.push(i.shift()), i;
}
function Lm(e, t, r) {
  var n = t.smooth, i = t.points;
  if (i && i.length >= 2) {
    if (n) {
      var a = TC(i, n, r, t.smoothConstraint);
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
var CC = /* @__PURE__ */ (function() {
  function e() {
    this.points = null, this.smooth = 0, this.smoothConstraint = null;
  }
  return e;
})(), nu = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultShape = function() {
    return new CC();
  }, t.prototype.buildPath = function(r, n) {
    Lm(r, n, !0);
  }, t;
})(yt);
nu.prototype.type = "polygon";
var xC = /* @__PURE__ */ (function() {
  function e() {
    this.points = null, this.percent = 1, this.smooth = 0, this.smoothConstraint = null;
  }
  return e;
})(), iu = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new xC();
  }, t.prototype.buildPath = function(r, n) {
    Lm(r, n, !1);
  }, t;
})(yt);
iu.prototype.type = "polyline";
var MC = {}, DC = /* @__PURE__ */ (function() {
  function e() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.percent = 1;
  }
  return e;
})(), An = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new DC();
  }, t.prototype.buildPath = function(r, n) {
    var i, a, o, s;
    if (this.subPixelOptimize) {
      var u = fm(MC, n, this.style);
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
})(yt);
An.prototype.type = "line";
var jt = [], IC = /* @__PURE__ */ (function() {
  function e() {
    this.x1 = 0, this.y1 = 0, this.x2 = 0, this.y2 = 0, this.cpx1 = 0, this.cpy1 = 0, this.percent = 1;
  }
  return e;
})();
function dd(e, t, r) {
  var n = e.cpx2, i = e.cpy2;
  return n != null || i != null ? [
    (r ? vv : Pt)(e.x1, e.cpx1, e.cpx2, e.x2, t),
    (r ? vv : Pt)(e.y1, e.cpy1, e.cpy2, e.y2, t)
  ] : [
    (r ? dv : Bt)(e.x1, e.cpx1, e.x2, t),
    (r ? dv : Bt)(e.y1, e.cpy1, e.y2, t)
  ];
}
var au = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new IC();
  }, t.prototype.buildPath = function(r, n) {
    var i = n.x1, a = n.y1, o = n.x2, s = n.y2, u = n.cpx1, l = n.cpy1, f = n.cpx2, h = n.cpy2, v = n.percent;
    v !== 0 && (r.moveTo(i, a), f == null || h == null ? (v < 1 && (os(i, u, o, v, jt), u = jt[1], o = jt[2], os(a, l, s, v, jt), l = jt[1], s = jt[2]), r.quadraticCurveTo(u, l, o, s)) : (v < 1 && (as(i, u, f, o, v, jt), u = jt[1], f = jt[2], o = jt[3], as(a, l, h, s, v, jt), l = jt[1], h = jt[2], s = jt[3]), r.bezierCurveTo(u, l, f, h, o, s)));
  }, t.prototype.pointAt = function(r) {
    return dd(this.shape, r, !1);
  }, t.prototype.tangentAt = function(r) {
    var n = dd(this.shape, r, !0);
    return dy(n, n);
  }, t;
})(yt);
au.prototype.type = "bezier-curve";
var AC = /* @__PURE__ */ (function() {
  function e() {
    this.cx = 0, this.cy = 0, this.r = 0, this.startAngle = 0, this.endAngle = Math.PI * 2, this.clockwise = !0;
  }
  return e;
})(), Ba = (function(e) {
  z(t, e);
  function t(r) {
    return e.call(this, r) || this;
  }
  return t.prototype.getDefaultStyle = function() {
    return {
      stroke: "#000",
      fill: null
    };
  }, t.prototype.getDefaultShape = function() {
    return new AC();
  }, t.prototype.buildPath = function(r, n) {
    var i = n.cx, a = n.cy, o = Math.max(n.r, 0), s = n.startAngle, u = n.endAngle, l = n.clockwise, f = Math.cos(s), h = Math.sin(s);
    r.moveTo(f * o + i, h * o + a), r.arc(i, a, o, s, u, !l);
  }, t;
})(yt);
Ba.prototype.type = "arc";
var Em = (function(e) {
  z(t, e);
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
    return this._updatePathDirty.call(this), yt.prototype.getBoundingRect.call(this);
  }, t;
})(yt), Rm = (function() {
  function e(t) {
    this.colorStops = t || [];
  }
  return e.prototype.addColorStop = function(t, r) {
    this.colorStops.push({
      offset: t,
      color: r
    });
  }, e;
})(), Om = (function(e) {
  z(t, e);
  function t(r, n, i, a, o, s) {
    var u = e.call(this, o) || this;
    return u.x = r ?? 0, u.y = n ?? 0, u.x2 = i ?? 1, u.y2 = a ?? 0, u.type = "linear", u.global = s || !1, u;
  }
  return t;
})(Rm), km = (function(e) {
  z(t, e);
  function t(r, n, i, a, o) {
    var s = e.call(this, a) || this;
    return s.x = r ?? 0.5, s.y = n ?? 0.5, s.r = i ?? 0.5, s.type = "radial", s.global = o || !1, s;
  }
  return t;
})(Rm), fl = Math.min, PC = Math.max, no = Math.abs, Qr = [0, 0], Jr = [0, 0], Dt = Sy(), io = Dt.minTv, ao = Dt.maxTv, Bm = (function() {
  function e(t, r) {
    this._corners = [], this._axes = [], this._origin = [0, 0];
    for (var n = 0; n < 4; n++)
      this._corners[n] = new ut();
    for (var n = 0; n < 2; n++)
      this._axes[n] = new ut();
    t && this.fromBoundingRect(t, r);
  }
  return e.prototype.fromBoundingRect = function(t, r) {
    var n = this._corners, i = this._axes, a = t.x, o = t.y, s = a + t.width, u = o + t.height;
    if (n[0].set(a, o), n[1].set(s, o), n[2].set(s, u), n[3].set(a, u), r)
      for (var l = 0; l < 4; l++)
        n[l].transform(r);
    ut.sub(i[0], n[1], n[0]), ut.sub(i[1], n[3], n[0]), i[0].normalize(), i[1].normalize();
    for (var l = 0; l < 2; l++)
      this._origin[l] = i[l].dot(n[0]);
  }, e.prototype.intersect = function(t, r, n) {
    var i = !0, a = !r;
    return r && ut.set(r, 0, 0), Dt.reset(n, !a), !this._intersectCheckOneSide(this, t, a, 1) && (i = !1, a) || !this._intersectCheckOneSide(t, this, a, -1) && (i = !1, a) || !a && !Dt.negativeSize && ut.copy(r, i ? Dt.useDir ? Dt.dirMinTv : io : ao), i;
  }, e.prototype._intersectCheckOneSide = function(t, r, n, i) {
    for (var a = !0, o = 0; o < 2; o++) {
      var s = t._axes[o];
      if (t._getProjMinMaxOnAxis(o, t._corners, Qr), t._getProjMinMaxOnAxis(o, r._corners, Jr), Dt.negativeSize || Qr[1] < Jr[0] || Qr[0] > Jr[1]) {
        if (a = !1, Dt.negativeSize || n)
          return a;
        var u = no(Jr[0] - Qr[1]), l = no(Qr[0] - Jr[1]);
        fl(u, l) > ao.len() && (u < l ? ut.scale(ao, s, -u * i) : ut.scale(ao, s, l * i));
      } else if (!n) {
        var u = no(Jr[0] - Qr[1]), l = no(Qr[0] - Jr[1]);
        (Dt.useDir || fl(u, l) < io.len()) && ((u < l || !Dt.bidirectional) && (ut.scale(io, s, u * i), Dt.useDir && Dt.calcDirMTV()), (u >= l || !Dt.bidirectional) && (ut.scale(io, s, -l * i), Dt.useDir && Dt.calcDirMTV()));
      }
    }
    return a;
  }, e.prototype._getProjMinMaxOnAxis = function(t, r, n) {
    for (var i = this._axes[t], a = this._origin, o = r[0].dot(i) + a[t], s = o, u = o, l = 1; l < r.length; l++) {
      var f = r[l].dot(i) + a[t];
      s = fl(f, s), u = PC(f, u);
    }
    n[0] = s + Dt.touchThreshold, n[1] = u - Dt.touchThreshold, Dt.negativeSize = n[1] < n[0];
  }, e;
})(), Nm = 0, LC = 1, EC = 2, RC = 1, Vo = 0, OC = [], Fm = (function(e) {
  z(t, e);
  function t() {
    var r = e !== null && e.apply(this, arguments) || this;
    return r.notClear = !0, r.incremental = LC, r._displayables = [], r._temporaryDisplayables = [], r._cursor = 0, r;
  }
  return t.prototype.traverse = function(r, n) {
    r.call(n, this);
  }, t.prototype.useStyle = function() {
    this.style = {};
  }, t.prototype._useHoverStyle = function() {
    this.__hoverStyle = null;
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
      for (var r = new Y(1 / 0, 1 / 0, -1 / 0, -1 / 0), n = 0; n < this._displayables.length; n++) {
        var i = this._displayables[n], a = i.getBoundingRect().clone();
        i.needLocalTransform() && a.applyTransform(i.getLocalTransform(OC)), r.union(a);
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
})(Oa), kC = dt();
function BC(e, t, r, n, i) {
  var a;
  if (t && t.ecModel) {
    var o = t.ecModel.getUpdatePayload();
    a = o && o.animation;
  }
  var s = t && t.isAnimationEnabled(), u = e === "update";
  if (s) {
    var l = void 0, f = void 0, h = void 0;
    n ? (l = G(n.duration, 200), f = G(n.easing, "cubicOut"), h = 0) : (l = t.getShallow(u ? "animationDurationUpdate" : "animationDuration"), f = t.getShallow(u ? "animationEasingUpdate" : "animationEasing"), h = t.getShallow(u ? "animationDelayUpdate" : "animationDelay")), a && (a.duration != null && (l = a.duration), a.easing != null && (f = a.easing), a.delay != null && (h = a.delay)), X(h) && (h = h(r, i)), X(l) && (l = l(r));
    var v = {
      duration: l || 0,
      delay: h,
      easing: f
    };
    return v;
  } else
    return null;
}
function Bh(e, t, r, n, i, a, o) {
  var s = !1, u;
  X(i) ? (o = a, a = i, i = null) : F(i) && (a = i.cb, o = i.during, s = i.isFrom, u = i.removeOpt, i = i.dataIndex);
  var l = e === "leave";
  l || t.stopAnimation("leave");
  var f = BC(e, n, i, l ? u || {} : null, n && n.getAnimationDelayParams ? n.getAnimationDelayParams(t, i) : null);
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
function pi(e, t, r, n, i, a) {
  Bh("update", e, t, r, n, i, a);
}
function Nh(e, t, r, n, i, a) {
  Bh("enter", e, t, r, n, i, a);
}
function ea(e) {
  if (!e.__zr)
    return !0;
  for (var t = 0; t < e.animators.length; t++) {
    var r = e.animators[t];
    if (r.scope === "leave")
      return !0;
  }
  return !1;
}
function zm(e, t, r, n, i, a) {
  ea(e) || Bh("leave", e, t, r, n, i, a);
}
function pd(e, t, r, n) {
  e.removeTextContent(), e.removeTextGuideLine(), zm(e, {
    style: {
      opacity: 0
    }
  }, t, r, n);
}
function NC(e, t, r) {
  function n() {
    e.parent && e.parent.remove(e);
  }
  e.isGroup ? e.traverse(function(i) {
    i.isGroup || pd(i, t, r, n);
  }) : pd(e, t, r, n);
}
function HE(e) {
  kC(e).oldStyle = e.style;
}
var Pf = {}, Hm = ["x", "y"], Vm = ["width", "height"], Gm = 0, Um = 1, Ym = 2;
function Wm(e) {
  return yt.extend(e);
}
var FC = vC;
function Xm(e, t) {
  return FC(e, t);
}
function be(e, t) {
  Pf[e] = t;
}
function qm(e) {
  if (Pf.hasOwnProperty(e))
    return Pf[e];
}
function ou(e, t, r, n) {
  var i = cC(e, t);
  return r && (n === "center" && (r = $m(r, i.getBoundingRect())), zh(i, r)), i;
}
function Fh(e, t, r) {
  var n = new kr({
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
        n.setStyle($m(t, a));
      }
    }
  });
  return n;
}
function $m(e, t) {
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
var Zm = dC;
function zh(e, t) {
  if (e.applyTransform) {
    var r = e.getBoundingRect(), n = r.calculateTransform(t);
    e.applyTransform(n);
  }
}
function Hh(e, t) {
  return fm(e, e, {
    lineWidth: t
  }), e;
}
function zC(e, t) {
  return hm(e, e, t), e;
}
var HC = hn;
function Km(e, t) {
  for (var r = xn([]); e && e !== t; )
    gn(r, e.getLocalTransform(), r), e = e.parent;
  return r;
}
function Vh(e, t, r) {
  return t && !$t(t) && (t = Pa.getLocalTransform(t)), r && (t = di([], t)), ie([], e, t);
}
function VC(e, t, r) {
  var n = t[4] === 0 || t[5] === 0 || t[0] === 0 ? 1 : Ft(2 * t[4] / t[0]), i = t[4] === 0 || t[5] === 0 || t[2] === 0 ? 1 : Ft(2 * t[4] / t[2]), a = [e === "left" ? -n : e === "right" ? n : 0, e === "top" ? -i : e === "bottom" ? i : 0];
  return a = Vh(a, t, r), Ft(a[0]) > Ft(a[1]) ? a[0] > 0 ? "right" : "left" : a[1] > 0 ? "bottom" : "top";
}
function gd(e) {
  return !e.isGroup;
}
function GC(e) {
  return e.shape != null;
}
function UC(e, t, r) {
  if (!e || !t)
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
    return GC(o) && (s.shape = Z(o.shape)), s;
  }
  var a = n(e);
  t.traverse(function(o) {
    if (gd(o) && o.anid) {
      var s = a[o.anid];
      if (s) {
        var u = i(o);
        o.attr(i(s)), pi(o, u, r, ft(o).dataIndex);
      }
    }
  });
}
function jm(e, t) {
  return H(e, function(r) {
    var n = r[0];
    n = gt(n, t.x), n = Jt(n, t.x + t.width);
    var i = r[1];
    return i = gt(i, t.y), i = Jt(i, t.y + t.height), [n, i];
  });
}
function Qm(e, t) {
  var r = gt(e.x, t.x), n = Jt(e.x + e.width, t.x + t.width), i = gt(e.y, t.y), a = Jt(e.y + e.height, t.y + t.height);
  if (n >= r && a >= i)
    return {
      x: r,
      y: i,
      width: n - r,
      height: a - i
    };
}
function su(e, t, r) {
  var n = R({
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
    return e.indexOf("image://") === 0 ? (i.image = e.slice(8), _t(i, r), new kr(n)) : ou(e.replace("path://", ""), n, r, "center");
}
function YC(e, t, r, n, i) {
  for (var a = 0, o = i[i.length - 1]; a < i.length; a++) {
    var s = i[a];
    if (Jm(e, t, r, n, s[0], s[1], o[0], o[1]))
      return !0;
    o = s;
  }
}
function Jm(e, t, r, n, i, a, o, s) {
  var u = r - e, l = n - t, f = o - i, h = s - a, v = hl(f, h, u, l);
  if (WC(v))
    return !1;
  var c = e - i, d = t - a, y = hl(c, d, u, l) / v;
  if (y < 0 || y > 1)
    return !1;
  var p = hl(c, d, f, h) / v;
  return !(p < 0 || p > 1);
}
function hl(e, t, r, n) {
  return e * n - r * t;
}
function WC(e) {
  return e <= 1e-6 && e >= -1e-6;
}
function Lf(e, t, r, n, i) {
  return t == null || (vt(t) ? lt[0] = lt[1] = lt[2] = lt[3] = t : (lt[0] = t[0], lt[1] = t[1], lt[2] = t[2], lt[3] = t[3]), n && (lt[0] = gt(0, lt[0]), lt[1] = gt(0, lt[1]), lt[2] = gt(0, lt[2]), lt[3] = gt(0, lt[3])), r && (lt[0] = -lt[0], lt[1] = -lt[1], lt[2] = -lt[2], lt[3] = -lt[3]), yd(e, lt, "x", "width", 3, 1, i && i[0] || 0), yd(e, lt, "y", "height", 0, 2, i && i[1] || 0)), e;
}
var lt = [0, 0, 0, 0];
function yd(e, t, r, n, i, a, o) {
  var s = t[a] + t[i], u = e[n];
  e[n] += s, o = gt(0, Jt(o, u)), e[n] < o ? (e[n] = o, e[r] += t[i] >= 0 ? -t[i] : t[a] >= 0 ? u + t[a] : Ft(s) > 1e-8 ? (u - o) * t[i] / s : 0) : e[r] -= t[i];
}
function uu(e) {
  var t = e.itemTooltipOption, r = e.componentModel, n = e.itemName, i = N(t) ? {
    formatter: t
  } : t, a = r.mainType, o = r.componentIndex, s = {
    componentType: a,
    name: n,
    $vars: ["name"]
  };
  s[a + "Index"] = o;
  var u = e.formatterParamsExtra;
  u && T(mt(u), function(f) {
    Xt(s, f) || (s[f] = u[f], s.$vars.push(f));
  });
  var l = ft(e.el);
  l.componentMainType = a, l.componentIndex = o, l.tooltipConfig = {
    name: n,
    option: _t({
      content: n,
      encodeHTMLContent: !0,
      formatterParams: s
    }, i)
  };
}
function Ef(e, t) {
  var r;
  e.isGroup && (r = t(e)), r || e.traverse(t);
}
function t_(e, t) {
  if (e)
    if (B(e))
      for (var r = 0; r < e.length; r++)
        Ef(e[r], t);
    else
      Ef(e, t);
}
function Gh(e) {
  return !e || Ft(e[1]) < oo && Ft(e[2]) < oo || Ft(e[0]) < oo && Ft(e[3]) < oo;
}
var oo = 1e-5;
function ya(e, t) {
  return e ? Y.copy(e, t) : t.clone();
}
function Uh(e, t) {
  return t ? Aa(e || Se(), t) : void 0;
}
function e_(e) {
  return {
    z: e.get("z") || 0,
    zlevel: e.get("zlevel") || 0
  };
}
function XC(e) {
  var t = -1 / 0, r = 1 / 0;
  Ef(e, function(a) {
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
      o > t && (t = o), o < r && (r = o);
    }
  }
  return r > t && (r = t = 0), {
    min: r,
    max: t
  };
}
function r_(e, t, r) {
  n_(e, t, r, -1 / 0);
}
function n_(e, t, r, n) {
  if (e.ignoreModelZ)
    return n;
  var i = e.getTextContent(), a = e.getTextGuideLine(), o = e.isGroup;
  if (o)
    for (var s = e.childrenRef(), u = 0; u < s.length; u++)
      n = gt(n_(s[u], t, r, n), n);
  else
    e.z = t, e.zlevel = r, n = gt(e.z2 || 0, n);
  if (i && (i.z = t, i.zlevel = r, isFinite(n) && (i.z2 = n + 2)), a) {
    var l = e.textGuideLineConfig;
    a.z = t, a.zlevel = r, isFinite(n) && (a.z2 = n + (l && l.showAbove ? 1 : -1));
  }
  return n;
}
function qC(e) {
  return e.animation = {
    duration: 0
  }, e;
}
function $C(e, t) {
  return t ? Aa($i.transform, t) : xn($i.transform), $i.decomposeTransform(), ls(e, $i), e;
}
var $i = new Pa();
$i.transform = Se();
function ZC(e) {
  var t = e.getZr().painter;
  return t.getType() === "canvas" ? t : null;
}
be("circle", ka);
be("ellipse", tu);
be("sector", eu);
be("ring", ru);
be("polygon", nu);
be("polyline", iu);
be("rect", Rt);
be("line", An);
be("bezierCurve", au);
be("arc", Ba);
const KC = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: Ba,
  BezierCurve: au,
  BoundingRect: Y,
  Circle: ka,
  CompoundPath: Em,
  Ellipse: tu,
  Group: oe,
  HOVER_LAYER_FOR_INCREMENTAL: Ym,
  HOVER_LAYER_FROM_THRESHOLD: Um,
  HOVER_LAYER_NO: Gm,
  Image: kr,
  IncrementalDisplayable: Fm,
  Line: An,
  LinearGradient: Om,
  OrientedBoundingRect: Bm,
  Path: yt,
  Point: ut,
  Polygon: nu,
  Polyline: iu,
  RadialGradient: km,
  Rect: Rt,
  Ring: ru,
  Sector: eu,
  Text: Kt,
  WH: Vm,
  XY: Hm,
  applyTransform: Vh,
  calcZ2Range: XC,
  clipPointsByRect: jm,
  clipRectByRect: Qm,
  createIcon: su,
  decomposeTransform: $C,
  ensureCopyRect: ya,
  ensureCopyTransform: Uh,
  expandOrShrinkRect: Lf,
  extendPath: Xm,
  extendShape: Wm,
  getCurrentCanvasPainter: ZC,
  getShapeClass: qm,
  getTransform: Km,
  groupTransition: UC,
  initProps: Nh,
  isBoundingRectAxisAligned: Gh,
  isElementRemoved: ea,
  lineLineIntersect: Jm,
  linePolygonIntersect: YC,
  makeImage: Fh,
  makePath: ou,
  mergePath: Zm,
  payloadDisableAnimation: qC,
  registerShape: be,
  removeElement: zm,
  removeElementWithFadeOut: NC,
  resizePath: zh,
  retrieveZInfo: e_,
  setTooltipConfig: uu,
  subPixelOptimize: HC,
  subPixelOptimizeLine: Hh,
  subPixelOptimizeRect: zC,
  transformDirection: VC,
  traverseElements: t_,
  traverseUpdateZ: r_,
  updateProps: pi
}, Symbol.toStringTag, { value: "Module" }));
var lu = {};
function i_(e, t) {
  for (var r = 0; r < Lr.length; r++) {
    var n = Lr[r], i = t[n], a = e.ensureState(n);
    a.style = a.style || {}, a.style.text = i;
  }
  var o = e.currentStates.slice();
  e.clearStates(!0), e.setStyle({
    text: t.normal
  }), e.useStates(o, !0);
}
function Rf(e, t, r) {
  var n = e.labelFetcher, i = e.labelDataIndex, a = e.labelDimIndex, o = t.normal, s;
  n && (s = n.getFormattedLabel(i, "normal", null, a, o && o.get("formatter"), r != null ? {
    interpolatedValue: r
  } : null)), s == null && (s = X(e.defaultText) ? e.defaultText(i, e, r) : e.defaultText);
  for (var u = {
    normal: s
  }, l = 0; l < Lr.length; l++) {
    var f = Lr[l], h = t[f];
    u[f] = G(n ? n.getFormattedLabel(i, f, null, a, h && h.get("formatter")) : null, s);
  }
  return u;
}
function jC(e, t, r, n) {
  r = r || lu;
  for (var i = e instanceof Kt, a = !1, o = 0; o < rd.length; o++) {
    var s = t[rd[o]];
    if (s && s.getShallow("show")) {
      a = !0;
      break;
    }
  }
  var u = i ? e : e.getTextContent();
  if (a) {
    i || (u || (u = new Kt(), e.setTextContent(u)), e.stateProxy && (u.stateProxy = e.stateProxy));
    var l = Rf(r, t), f = t.normal, h = !!f.getShallow("show"), v = Tn(f, n, r, !1, !i);
    v.text = l.normal, i || e.setTextConfig(md(f, r, !1));
    for (var o = 0; o < Lr.length; o++) {
      var c = Lr[o], s = t[c];
      if (s) {
        var d = u.ensureState(c), y = !!G(s.getShallow("show"), h);
        if (y !== h && (d.ignore = !y), d.style = Tn(s, n, r, !0, !i), d.style.text = l[c], !i) {
          var p = e.ensureState(c);
          p.textConfig = md(s, r, !0);
        }
      }
    }
    u.silent = !!f.getShallow("silent"), u.style.x != null && (v.x = u.style.x), u.style.y != null && (v.y = u.style.y), u.ignore = !h, u.useStyle(v), u.dirty(), r.enableTextSetter && (Yh(u).setLabelText = function(g) {
      var m = Rf(r, t, g);
      i_(u, m);
    });
  } else u && (u.ignore = !0);
  e.dirty();
}
function VE(e, t) {
  t = t || "label";
  for (var r = {
    normal: e.getModel(t)
  }, n = 0; n < Lr.length; n++) {
    var i = Lr[n];
    r[i] = e.getModel([i, t]);
  }
  return r;
}
function Tn(e, t, r, n, i) {
  var a = {};
  return QC(a, e, r, n, i), t && R(a, t), a;
}
function md(e, t, r) {
  t = t || {};
  var n = {}, i, a = e.getShallow("rotate"), o = G(e.getShallow("distance"), r ? null : 5), s = e.getShallow("offset");
  return i = e.getShallow("position") || (r ? null : "inside"), i === "outside" && (i = t.defaultOutsidePosition || "top"), i != null && (n.position = i), s != null && (n.offset = s), a != null && (a *= Math.PI / 180, n.rotation = a), o != null && (n.distance = o), n.outsideFill = e.get("color") === "inherit" ? t.inheritColor || null : "auto", t.autoOverflowArea != null && (n.autoOverflowArea = t.autoOverflowArea), t.layoutRect != null && (n.layoutRect = t.layoutRect), n;
}
function QC(e, t, r, n, i) {
  r = r || lu;
  var a = t.ecModel, o = a && a.option.textStyle, s = JC(t), u;
  if (s) {
    u = {};
    var l = "richInheritPlainLabel", f = G(t.get(l), a ? a.get(l) : void 0);
    for (var h in s)
      if (s.hasOwnProperty(h)) {
        var v = t.getModel(["rich", h]);
        wd(u[h] = {}, v, o, t, f, r, n, i, !1, !0);
      }
  }
  u && (e.rich = u);
  var c = t.get("overflow");
  c && (e.overflow = c);
  var d = t.get("lineOverflow");
  d && (e.lineOverflow = d);
  var y = e, p = t.get("minMargin");
  if (p != null)
    p = vt(p) ? p / 2 : 0, y.margin = [p, p, p, p], y.__marginType = Qn.minMargin;
  else {
    var g = t.get("textMargin");
    g != null && (y.margin = Vs(g), y.__marginType = Qn.textMargin);
  }
  wd(e, t, o, null, null, r, n, i, !0, !1);
}
function JC(e) {
  for (var t; e && e !== e.ecModel; ) {
    var r = (e.option || lu).rich;
    if (r) {
      t = t || {};
      for (var n = mt(r), i = 0; i < n.length; i++) {
        var a = n[i];
        t[a] = 1;
      }
    }
    e = e.parentModel;
  }
  return t;
}
var _d = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "textShadowColor", "textShadowBlur", "textShadowOffsetX", "textShadowOffsetY"], Sd = ["align", "lineHeight", "width", "height", "tag", "verticalAlign", "ellipsis"], bd = ["padding", "borderWidth", "borderRadius", "borderDashOffset", "backgroundColor", "borderColor", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"];
function wd(e, t, r, n, i, a, o, s, u, l) {
  r = !o && r || lu;
  var f = a && a.inheritColor, h = t.getShallow("color"), v = t.getShallow("textBorderColor"), c = G(t.getShallow("opacity"), r.opacity);
  (h === "inherit" || h === "auto") && (f ? h = f : h = null), (v === "inherit" || v === "auto") && (f ? v = f : v = null), s || (h = h || r.color, v = v || r.textBorderColor), h != null && (e.fill = h), v != null && (e.stroke = v);
  var d = G(t.getShallow("textBorderWidth"), r.textBorderWidth);
  d != null && (e.lineWidth = d);
  var y = G(t.getShallow("textBorderType"), r.textBorderType);
  y != null && (e.lineDash = y);
  var p = G(t.getShallow("textBorderDashOffset"), r.textBorderDashOffset);
  p != null && (e.lineDashOffset = p), !o && c == null && !l && (c = a && a.defaultOpacity), c != null && (e.opacity = c), !o && !s && e.fill == null && a.inheritColor && (e.fill = a.inheritColor);
  for (var g = 0; g < _d.length; g++) {
    var m = _d[g], _ = i !== !1 && n ? dn(t.getShallow(m), n.getShallow(m), r[m]) : G(t.getShallow(m), r[m]);
    _ != null && (e[m] = _);
  }
  for (var g = 0; g < Sd.length; g++) {
    var m = Sd[g], _ = t.getShallow(m);
    _ != null && (e[m] = _);
  }
  if (e.verticalAlign == null) {
    var S = t.getShallow("baseline");
    S != null && (e.verticalAlign = S);
  }
  if (!u || !a.disableBox) {
    for (var g = 0; g < bd.length; g++) {
      var m = bd[g], _ = t.getShallow(m);
      _ != null && (e[m] = _);
    }
    var b = t.getShallow("borderType");
    b != null && (e.borderDash = b), (e.backgroundColor === "auto" || e.backgroundColor === "inherit") && f && (e.backgroundColor = f), (e.borderColor === "auto" || e.borderColor === "inherit") && f && (e.borderColor = f);
  }
}
function tx(e, t) {
  var r = t && t.getModel("textStyle");
  return Me([
    // FIXME in node-canvas fontWeight is before fontStyle
    e.fontStyle || r && r.getShallow("fontStyle") || "",
    e.fontWeight || r && r.getShallow("fontWeight") || "",
    (e.fontSize || r && r.getShallow("fontSize") || 12) + "px",
    e.fontFamily || r && r.getShallow("fontFamily") || "sans-serif"
  ].join(" "));
}
var Yh = dt();
function GE(e, t, r, n) {
  if (e) {
    var i = Yh(e);
    i.prevValue = i.value, i.value = r;
    var a = t.normal;
    i.valueAnimation = a.get("valueAnimation"), i.valueAnimation && (i.precision = a.get("precision"), i.defaultInterpolatedText = n, i.statesModels = t);
  }
}
function UE(e, t, r, n, i) {
  var a = Yh(e);
  if (!a.valueAnimation || a.prevValue === a.value)
    return;
  var o = a.defaultInterpolatedText, s = G(a.interpolatedValue, a.prevValue), u = a.value;
  function l(f) {
    var h = Iw(r, a.precision, s, u, f);
    a.interpolatedValue = f === 1 ? null : h;
    var v = Rf({
      labelDataIndex: t,
      labelFetcher: i,
      defaultText: o ? o(h) : h + ""
    }, a.statesModels, h);
    i_(e, v);
  }
  e.percent = 0, (a.prevValue == null ? Nh : pi)(e, {
    // percent is used to prevent animation from being aborted #15916
    percent: 1
  }, n, t, null, l);
}
var Qn = {
  minMargin: 1,
  textMargin: 2
}, ex = ["textStyle", "color"], cl = ["fontStyle", "fontWeight", "fontSize", "fontFamily", "padding", "lineHeight", "rich", "width", "height", "overflow"], vl = new Kt(), rx = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getTextColor = function(t) {
      var r = this.ecModel;
      return this.getShallow("color") || (!t && r ? r.get(ex) : null);
    }, e.prototype.getFont = function() {
      return tx({
        fontStyle: this.getShallow("fontStyle"),
        fontWeight: this.getShallow("fontWeight"),
        fontSize: this.getShallow("fontSize"),
        fontFamily: this.getShallow("fontFamily")
      }, this.ecModel);
    }, e.prototype.getTextRect = function(t) {
      for (var r = {
        text: t,
        verticalAlign: this.getShallow("verticalAlign") || this.getShallow("baseline")
      }, n = 0; n < cl.length; n++)
        r[cl[n]] = this.getShallow(cl[n]);
      return vl.useStyle(r), vl.update(), vl.getBoundingRect();
    }, e;
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
], nx = ga(a_), ix = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getLineStyle = function(t) {
      return nx(this, t);
    }, e;
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
], ax = ga(o_), ox = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getItemStyle = function(t, r) {
      return ax(this, t, r);
    }, e;
  })()
), ct = (
  /** @class */
  (function() {
    function e(t, r, n) {
      this.parentModel = r, this.ecModel = n, this.option = t;
    }
    return e.prototype.init = function(t, r, n) {
    }, e.prototype.mergeOption = function(t, r) {
      pt(this.option, t, !0);
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
      return new t(Z(this.option));
    }, e.prototype.parsePath = function(t) {
      return typeof t == "string" ? t.split(".") : t;
    }, e.prototype.resolveParentPath = function(t) {
      return t;
    }, e.prototype.isAnimationEnabled = function() {
      if (!W.node && this.option) {
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
Ih(ct);
Uw(ct);
Ae(ct, ix);
Ae(ct, ox);
Ae(ct, $w);
Ae(ct, rx);
var sx = Math.round(Math.random() * 10);
function fu(e) {
  return [e || "", sx++].join("_");
}
function ux(e) {
  var t = {};
  e.registerSubTypeDefaulter = function(r, n) {
    var i = ze(r);
    t[i.main] = n;
  }, e.determineSubType = function(r, n) {
    var i = n.type;
    if (!i) {
      var a = ze(r).main;
      e.hasSubTypes(r) && t[a] && (i = t[a](n));
    }
    return i;
  };
}
function lx(e, t) {
  e.topologicalTravel = function(a, o, s, u) {
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
      var l = n(o, u), f = l.originalDeps = t(u), h = i(f, a);
      l.entryCount = h.length, l.entryCount === 0 && s.push(u), T(h, function(v) {
        st(l.predecessor, v) < 0 && l.predecessor.push(v);
        var c = n(o, v);
        st(c.successor, v) < 0 && c.successor.push(u);
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
      st(o, u) >= 0 && s.push(u);
    }), s;
  }
}
function fx(e, t) {
  return pt(pt({}, e, !0), t, !0);
}
const hx = {
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
}, cx = {
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
var ps = "ZH", Wh = "EN", ei = Wh, Go = {}, Xh = {}, s_ = W.domSupported ? (function() {
  var e = (document.documentElement.lang || navigator.language || navigator.browserLanguage || ei).toUpperCase();
  return e.indexOf(ps) > -1 ? ps : ei;
})() : ei;
function qh(e, t) {
  e = e.toUpperCase(), Xh[e] = new ct(t), Go[e] = t;
}
function vx(e) {
  if (N(e)) {
    var t = Go[e.toUpperCase()] || {};
    return e === ps || e === Wh ? Z(t) : pt(Z(t), Z(Go[ei]), !1);
  } else
    return pt(Z(e), Z(Go[ei]), !1);
}
function dx(e) {
  return Xh[e];
}
function px() {
  return Xh[ei];
}
qh(Wh, hx);
qh(ps, cx);
var gx = null;
function hu() {
  return gx;
}
function u_(e, t) {
  t.breakOption;
  var r = t.breakParsed;
  return r;
}
function $h(e) {
  var t = e.brk;
  return t ? t.breaks : [];
}
function yx(e) {
  var t = e.brk;
  return t ? t.hasBreaks() : !1;
}
var Zh = 1e3, Kh = Zh * 60, ra = Kh * 60, me = ra * 24, Td = me * 365, mx = {
  year: /({yyyy}|{yy})/,
  month: /({MMMM}|{MMM}|{MM}|{M})/,
  day: /({dd}|{d})/,
  hour: /({HH}|{H}|{hh}|{h})/,
  minute: /({mm}|{m})/,
  second: /({ss}|{s})/,
  millisecond: /({SSS}|{S})/
}, Uo = {
  year: "{yyyy}",
  month: "{MMM}",
  day: "{d}",
  hour: "{HH}:{mm}",
  minute: "{HH}:{mm}",
  second: "{HH}:{mm}:{ss}",
  millisecond: "{HH}:{mm}:{ss} {SSS}"
}, _x = "{yyyy}-{MM}-{dd} {HH}:{mm}:{ss} {SSS}", so = "{yyyy}-{MM}-{dd}", Cd = {
  year: "{yyyy}",
  month: "{yyyy}-{MM}",
  day: so,
  hour: so + " " + Uo.hour,
  minute: so + " " + Uo.minute,
  second: so + " " + Uo.second,
  millisecond: _x
}, Sn = ["year", "month", "day", "hour", "minute", "second", "millisecond"], Sx = ["year", "half-year", "quarter", "month", "week", "half-week", "day", "half-day", "quarter-day", "hour", "minute", "second", "millisecond"];
function bx(e) {
  return !N(e) && !X(e) ? Tx(e) : e;
}
function Tx(e) {
  e = e || {};
  var t = {}, r = !0;
  return T(Sn, function(n) {
    r && (r = e[n] == null);
  }), T(Sn, function(n, i) {
    var a = e[n];
    t[n] = {};
    for (var o = null, s = i; s >= 0; s--) {
      var u = Sn[s], l = F(a) && !B(a) ? a[u] : a, f = void 0;
      B(l) ? (f = l.slice(), o = f[0] || "") : N(l) ? (o = l, f = [o]) : (o == null ? o = Uo[n] : mx[u].test(o) || (o = t[u][u][0] + " " + o), f = [o], r && (f[1] = "{primary|" + o + "}")), t[n][u] = f;
    }
  }), t;
}
function Gt(e, t) {
  return e += "", "0000".substr(0, t - e.length) + e;
}
function na(e) {
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
function Cx(e) {
  return e === na(e);
}
function xx(e) {
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
function Na(e, t, r, n) {
  var i = cr(e), a = i[l_(r)](), o = i[jh(r)]() + 1, s = Math.floor((o - 1) / 3) + 1, u = i[Qh(r)](), l = i["get" + (r ? "UTC" : "") + "Day"](), f = i[Jh(r)](), h = (f - 1) % 12 + 1, v = i[tc(r)](), c = i[ec(r)](), d = i[rc(r)](), y = f >= 12 ? "pm" : "am", p = y.toUpperCase(), g = n instanceof ct ? n : dx(n || s_) || px(), m = g.getModel("time"), _ = m.get("month"), S = m.get("monthAbbr"), b = m.get("dayOfWeek"), w = m.get("dayOfWeekAbbr");
  return (t || "").replace(/{a}/g, y + "").replace(/{A}/g, p + "").replace(/{yyyy}/g, a + "").replace(/{yy}/g, Gt(a % 100 + "", 2)).replace(/{Q}/g, s + "").replace(/{MMMM}/g, _[o - 1]).replace(/{MMM}/g, S[o - 1]).replace(/{MM}/g, Gt(o, 2)).replace(/{M}/g, o + "").replace(/{dd}/g, Gt(u, 2)).replace(/{d}/g, u + "").replace(/{eeee}/g, b[l]).replace(/{ee}/g, w[l]).replace(/{e}/g, l + "").replace(/{HH}/g, Gt(f, 2)).replace(/{H}/g, f + "").replace(/{hh}/g, Gt(h + "", 2)).replace(/{h}/g, h + "").replace(/{mm}/g, Gt(v, 2)).replace(/{m}/g, v + "").replace(/{ss}/g, Gt(c, 2)).replace(/{s}/g, c + "").replace(/{SSS}/g, Gt(d, 3)).replace(/{S}/g, d + "");
}
function Mx(e, t, r, n, i) {
  var a = null;
  if (N(r))
    a = r;
  else if (X(r)) {
    var o = {
      time: e.time,
      level: e.time ? e.time.level : 0
    }, s = hu();
    s && s.makeAxisLabelFormatterParamBreak(o, e.break), a = r(e.value, t, o);
  } else {
    var u = e.time;
    if (u) {
      var l = r[u.lowerTimeUnit][u.upperTimeUnit];
      a = l[Math.min(u.level, l.length - 1)] || "";
    } else {
      var f = Yo(e.value, i);
      a = r[f][f][0];
    }
  }
  return Na(new Date(e.value), a, i, n);
}
function Yo(e, t) {
  var r = cr(e), n = r[jh(t)]() + 1, i = r[Qh(t)](), a = r[Jh(t)](), o = r[tc(t)](), s = r[ec(t)](), u = r[rc(t)](), l = u === 0, f = l && s === 0, h = f && o === 0, v = h && a === 0, c = v && i === 1, d = c && n === 1;
  return d ? "year" : c ? "month" : v ? "day" : h ? "hour" : f ? "minute" : l ? "second" : "millisecond";
}
function gs(e, t, r) {
  switch (t) {
    case "year":
      e[f_(r)](0);
    case "month":
      e[h_(r)](1);
    case "day":
      e[c_(r)](0);
    case "hour":
      e[v_(r)](0);
    case "minute":
      e[d_(r)](0);
    case "second":
      e[p_(r)](0);
  }
  return e;
}
function l_(e) {
  return e ? "getUTCFullYear" : "getFullYear";
}
function jh(e) {
  return e ? "getUTCMonth" : "getMonth";
}
function Qh(e) {
  return e ? "getUTCDate" : "getDate";
}
function Jh(e) {
  return e ? "getUTCHours" : "getHours";
}
function tc(e) {
  return e ? "getUTCMinutes" : "getMinutes";
}
function ec(e) {
  return e ? "getUTCSeconds" : "getSeconds";
}
function rc(e) {
  return e ? "getUTCMilliseconds" : "getMilliseconds";
}
function Dx(e) {
  return e ? "setUTCFullYear" : "setFullYear";
}
function f_(e) {
  return e ? "setUTCMonth" : "setMonth";
}
function h_(e) {
  return e ? "setUTCDate" : "setDate";
}
function c_(e) {
  return e ? "setUTCHours" : "setHours";
}
function v_(e) {
  return e ? "setUTCMinutes" : "setMinutes";
}
function d_(e) {
  return e ? "setUTCSeconds" : "setSeconds";
}
function p_(e) {
  return e ? "setUTCMilliseconds" : "setMilliseconds";
}
function Ix(e, t, r, n, i, a, o, s) {
  var u = new Kt({
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
function nc(e) {
  if (!Xy(e))
    return N(e) ? e : "-";
  var t = (e + "").split(".");
  return t[0].replace(/(\d{1,3})(?=(?:\d{3})+(?!\d))/g, "$1,") + (t.length > 1 ? "." + t[1] : "");
}
function ic(e, t) {
  return e = (e || "").toLowerCase().replace(/-(.)/g, function(r, n) {
    return n.toUpperCase();
  }), t && e && (e = e.charAt(0).toUpperCase() + e.slice(1)), e;
}
var Fa = Vs;
function Of(e, t, r) {
  var n = "{yyyy}-{MM}-{dd} {HH}:{mm}:{ss}";
  function i(f) {
    return f && Me(f) ? f : "-";
  }
  function a(f) {
    return fr(f);
  }
  var o = t === "time", s = e instanceof Date;
  if (o || s) {
    var u = o ? cr(e) : e;
    if (isNaN(+u)) {
      if (s)
        return "-";
    } else return Na(u, n, r);
  }
  if (t === "ordinal")
    return Jo(e) ? i(e) : vt(e) && a(e) ? e + "" : "-";
  var l = lr(e);
  return a(l) ? nc(l) : Jo(e) ? i(e) : typeof e == "boolean" ? e + "" : "-";
}
var xd = ["a", "b", "c", "d", "e", "f", "g"], dl = function(e, t) {
  return "{" + e + (t ?? "") + "}";
};
function ac(e, t, r) {
  B(t) || (t = [t]);
  var n = t.length;
  if (!n)
    return "";
  for (var i = t[0].$vars || [], a = 0; a < i.length; a++) {
    var o = xd[a];
    e = e.replace(dl(o), dl(o, 0));
  }
  for (var s = 0; s < n; s++)
    for (var u = 0; u < i.length; u++) {
      var l = t[s][i[u]];
      e = e.replace(dl(xd[u], s), r ? Ut(l) : l);
    }
  return e;
}
function g_(e, t) {
  var r = N(e) ? {
    color: e,
    extraCssText: t
  } : e || {}, n = r.color, i = r.type;
  t = r.extraCssText;
  var a = r.renderMode || "html";
  if (!n)
    return "";
  if (a === "html")
    return i === "subItem" ? '<span style="display:inline-block;vertical-align:middle;margin-right:8px;margin-left:3px;border-radius:4px;width:4px;height:4px;background-color:' + Ut(n) + ";" + (t || "") + '"></span>' : '<span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background-color:' + Ut(n) + ";" + (t || "") + '"></span>';
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
function Ax(e, t, r) {
  (e === "week" || e === "month" || e === "quarter" || e === "half-year" || e === "year") && (e = `MM-dd
yyyy`);
  var n = cr(t), i = r ? "getUTC" : "get", a = n[i + "FullYear"](), o = n[i + "Month"]() + 1, s = n[i + "Date"](), u = n[i + "Hours"](), l = n[i + "Minutes"](), f = n[i + "Seconds"](), h = n[i + "Milliseconds"]();
  return e = e.replace("MM", Gt(o, 2)).replace("M", o).replace("yyyy", a).replace("yy", Gt(a % 100 + "", 2)).replace("dd", Gt(s, 2)).replace("d", s).replace("hh", Gt(u, 2)).replace("h", u).replace("mm", Gt(l, 2)).replace("m", l).replace("ss", Gt(f, 2)).replace("s", f).replace("SSS", Gt(h, 3)), e;
}
function Px(e) {
  return e && e.charAt(0).toUpperCase() + e.substr(1);
}
function ma(e, t) {
  return t = t || "transparent", N(e) ? e : F(e) && e.colorStops && (e.colorStops[0] || {}).color || t;
}
var Wo = {}, pl = {}, cu = (
  /** @class */
  (function() {
    function e() {
      this._normalMasterList = [], this._nonSeriesBoxMasterList = [];
    }
    return e.prototype.create = function(t, r) {
      this._nonSeriesBoxMasterList = n(Wo), this._normalMasterList = n(pl);
      function n(i, a) {
        var o = [];
        return T(i, function(s, u) {
          var l = s.create(t, r);
          o = o.concat(l || []);
        }), o;
      }
    }, e.prototype.update = function(t, r) {
      T(this._normalMasterList, function(n) {
        n.update && n.update(t, r);
      });
    }, e.prototype.getCoordinateSystems = function() {
      return this._normalMasterList.concat(this._nonSeriesBoxMasterList);
    }, e.register = function(t, r) {
      if (t === "matrix" || t === "calendar") {
        Wo[t] = r;
        return;
      }
      pl[t] = r;
    }, e.get = function(t) {
      return pl[t] || Wo[t];
    }, e;
  })()
);
function Lx(e) {
  return !!Wo[e];
}
var Ex = 1, Rx = 2, Ox = U();
function y_(e) {
  var t = e.getShallow("coord", !0), r = Ex;
  if (t == null) {
    var n = Ox.get(e.type);
    n && n.getCoord2 && (r = Rx, t = n.getCoord2(e));
  }
  return {
    coord: t,
    from: r
  };
}
var ri = 0, Xo = 1, kx = 2;
function Bx(e, t) {
  var r = e.getShallow("coordinateSystem"), n = e.getShallow("coordinateSystemUsage", !0), i = ri;
  if (r) {
    var a = e.mainType === "series";
    n == null && (n = a ? "data" : "box"), n === "data" ? (i = Xo, a || (i = ri)) : n === "box" && (i = kx, !a && !Lx(r) && (i = ri));
  }
  return {
    coordSysType: r,
    kind: i
  };
}
function YE(e) {
  var t = e.targetModel, r = e.coordSysType, n = e.coordSysProvider, i = e.isDefaultDataCoordSys, a = Bx(t), o = a.kind, s = a.coordSysType;
  if (i && o !== Xo && (o = Xo, s = r), o === ri || s !== r)
    return ri;
  var u = n(r, t);
  return u ? (o === Xo ? t.coordinateSystem = u : t.boxCoordinateSystem = u, o) : ri;
}
var qo = T, Nx = ["left", "right", "top", "bottom", "width", "height"], uo = [["width", "left", "right"], ["height", "top", "bottom"]];
function oc(e, t, r, n, i) {
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
var ia = oc;
Tt(oc, "vertical");
Tt(oc, "horizontal");
function Fx(e, t) {
  return {
    left: e.getShallow("left", t),
    top: e.getShallow("top", t),
    right: e.getShallow("right", t),
    bottom: e.getShallow("bottom", t),
    width: e.getShallow("width", t),
    height: e.getShallow("height", t)
  };
}
function ys(e, t, r) {
  r = Fa(r || 0);
  var n = t.width, i = t.height, a = xe(e.left, n), o = xe(e.top, i), s = xe(e.right, n), u = xe(e.bottom, i), l = xe(e.width, n), f = xe(e.height, i), h = r[2] + r[0], v = r[1] + r[3], c = e.aspect;
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
  var d = new Y((t.x || 0) + a + r[3], (t.y || 0) + o + r[0], l, f);
  return d.margin = r, d;
}
var gl = {
  rect: 1
};
function zx(e, t, r) {
  var n, i, a, o = e.boxCoordinateSystem, s;
  if (o) {
    var u = y_(e), l = u.coord, f = u.from;
    if (o.dataToLayout) {
      a = gl.rect, s = f;
      var h = o.dataToLayout(l);
      n = h.contentRect || h.rect;
    }
  }
  return a == null && (a = gl.rect), a === gl.rect && (n || (n = {
    x: 0,
    y: 0,
    width: t.getWidth(),
    height: t.getHeight()
  }), i = [n.x + n.width / 2, n.y + n.height / 2]), {
    type: a,
    refContainer: n,
    refPoint: i,
    boxCoordFrom: s
  };
}
function ms(e) {
  var t = e.layoutMode || e.constructor.layoutMode;
  return F(t) ? t : t ? {
    type: t
  } : null;
}
function _a(e, t, r) {
  var n = r && r.ignoreSize;
  !B(n) && (n = [n, n]);
  var i = o(uo[0], 0), a = o(uo[1], 1);
  u(uo[0], e, i), u(uo[1], e, a);
  function o(l, f) {
    var h = {}, v = 0, c = {}, d = 0, y = 2;
    if (qo(l, function(m) {
      c[m] = e[m];
    }), qo(l, function(m) {
      Xt(t, m) && (h[m] = c[m] = t[m]), s(h, m) && v++, s(c, m) && d++;
    }), n[f])
      return s(t, l[1]) ? c[l[2]] = null : s(t, l[2]) && (c[l[1]] = null), c;
    if (d === y || !v)
      return c;
    if (v >= y)
      return h;
    for (var p = 0; p < l.length; p++) {
      var g = l[p];
      if (!Xt(h, g) && Xt(e, g)) {
        h[g] = e[g];
        break;
      }
    }
    return h;
  }
  function s(l, f) {
    return l[f] != null && l[f] !== "auto";
  }
  function u(l, f, h) {
    qo(l, function(v) {
      f[v] = h[v];
    });
  }
}
function sc(e) {
  return Hx({}, e);
}
function Hx(e, t) {
  return t && e && qo(Nx, function(r) {
    Xt(t, r) && (e[r] = t[r]);
  }), e;
}
var Vx = dt(), at = (
  /** @class */
  (function(e) {
    z(t, e);
    function t(r, n, i) {
      var a = e.call(this, r, n, i) || this;
      return a.uid = fu("ec_cpt_model"), a;
    }
    return t.prototype.init = function(r, n, i) {
      this.mergeDefaultAndTheme(r, i);
    }, t.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = ms(this), a = i ? sc(r) : {}, o = n.getTheme();
      pt(r, o.get(this.mainType)), pt(r, this.getDefaultOption()), i && _a(r, a, i);
    }, t.prototype.mergeOption = function(r, n) {
      pt(this.option, r, !0);
      var i = ms(this);
      i && _a(this.option, r, i);
    }, t.prototype.optionUpdated = function(r, n) {
    }, t.prototype.getDefaultOption = function() {
      var r = this.constructor;
      if (!Hw(r))
        return r.defaultOption;
      var n = Vx(this);
      if (!n.defaultOption) {
        for (var i = [], a = r; a; ) {
          var o = a.prototype.defaultOption;
          o && i.push(o), a = a.superClass;
        }
        for (var s = {}, u = i.length - 1; u >= 0; u--)
          s = pt(s, i[u], !0);
        n.defaultOption = s;
      }
      return n.defaultOption;
    }, t.prototype.getReferringComponents = function(r, n) {
      var i = r + "Index", a = r + "Id";
      return Ra(this.ecModel, r, {
        index: this.get(i, !0),
        id: this.get(a, !0)
      }, n);
    }, t.prototype.getBoxLayoutParams = function() {
      return Fx(this, !1);
    }, t.prototype.getZLevelKey = function() {
      return "";
    }, t.prototype.setZLevel = function(r) {
      this.option.zlevel = r;
    }, t.protoInitialize = (function() {
      var r = t.prototype;
      r.type = "component", r.id = "", r.name = "", r.mainType = "", r.subType = "", r.componentIndex = 0;
    })(), t;
  })(ct)
);
em(at, ct);
Zs(at);
ux(at);
lx(at, Gx);
function Gx(e) {
  var t = [];
  return T(at.getClassesByMainType(e), function(r) {
    t = t.concat(r.dependencies || r.prototype.dependencies || []);
  }), t = H(t, function(r) {
    return ze(r).main;
  }), e !== "dataset" && st(t, "dataset") <= 0 && t.unshift("dataset"), t;
}
var j = {
  color: {},
  darkColor: {},
  size: {}
}, bt = j.color = {
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
R(bt, {
  primary: bt.neutral80,
  secondary: bt.neutral70,
  tertiary: bt.neutral60,
  quaternary: bt.neutral50,
  disabled: bt.neutral20,
  border: bt.neutral30,
  borderTint: bt.neutral20,
  borderShade: bt.neutral40,
  background: bt.neutral05,
  backgroundTint: "rgba(234,237,245,0.5)",
  backgroundTransparent: "rgba(255,255,255,0)",
  backgroundShade: bt.neutral10,
  shadow: "rgba(0,0,0,0.2)",
  shadowTint: "rgba(129,130,136,0.2)",
  axisLine: bt.neutral70,
  axisLineTint: bt.neutral40,
  axisTick: bt.neutral70,
  axisTickMinor: bt.neutral60,
  axisLabel: bt.neutral70,
  axisSplitLine: bt.neutral15,
  axisMinorSplitLine: bt.neutral05
});
for (var tn in bt)
  if (bt.hasOwnProperty(tn)) {
    var Md = bt[tn];
    tn === "theme" ? j.darkColor.theme = bt.theme.slice() : tn === "highlight" ? j.darkColor.highlight = "rgba(255,231,130,0.4)" : tn.indexOf("accent") === 0 ? j.darkColor[tn] = ss(Md, null, function(e) {
      return e * 0.5;
    }, function(e) {
      return Math.min(1, 1.3 - e);
    }) : j.darkColor[tn] = ss(Md, null, function(e) {
      return e * 0.9;
    }, function(e) {
      return 1 - Math.pow(e, 1.5);
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
var zn = "rgba(0, 0, 0, 0.2)", __ = j.color.theme[0], Ux = ss(__, null, null, 0.9);
const S_ = {
  darkMode: "auto",
  // backgroundColor: 'rgba(0,0,0,0)',
  colorBy: "series",
  color: j.color.theme,
  gradientColor: [Ux, __],
  aria: {
    decal: {
      decals: [{
        color: zn,
        dashArrayX: [1, 0],
        dashArrayY: [2, 5],
        symbolSize: 1,
        rotation: Math.PI / 6
      }, {
        color: zn,
        symbol: "circle",
        dashArrayX: [[8, 8], [0, 8, 8, 0]],
        dashArrayY: [6, 0],
        symbolSize: 0.8
      }, {
        color: zn,
        dashArrayX: [1, 0],
        dashArrayY: [4, 3],
        rotation: -Math.PI / 4
      }, {
        color: zn,
        dashArrayX: [[6, 6], [0, 6, 6, 0]],
        dashArrayY: [6, 0]
      }, {
        color: zn,
        dashArrayX: [[1, 0], [1, 6]],
        dashArrayY: [1, 0, 6, 0],
        rotation: Math.PI / 4
      }, {
        color: zn,
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
var re = {
  Must: 1,
  Might: 2,
  Not: 3
  // Other cases
}, b_ = dt();
function Yx(e) {
  b_(e).datasetMap = U();
}
function Wx(e, t, r) {
  var n = {}, i = w_(t);
  if (!i || !e)
    return n;
  var a = [], o = [], s = t.ecModel, u = b_(s).datasetMap, l = i.uid + "_" + r.seriesLayoutBy, f, h;
  e = e.slice(), T(e, function(y, p) {
    var g = F(y) ? y : e[p] = {
      name: y
    };
    g.type === "ordinal" && f == null && (f = p, h = d(g)), n[g.name] = [];
  });
  var v = u.get(l) || u.set(l, {
    categoryWayDim: h,
    valueWayDim: 0
  });
  T(e, function(y, p) {
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
function w_(e) {
  var t = e.get("data", !0);
  if (!t)
    return Ra(e.ecModel, "dataset", {
      index: e.get("datasetIndex", !0),
      id: e.get("datasetId", !0)
    }, ar).models[0];
}
function Xx(e) {
  return !e.get("transform", !0) && !e.get("fromTransformResult", !0) ? [] : Ra(e.ecModel, "dataset", {
    index: e.get("fromDatasetIndex", !0),
    id: e.get("fromDatasetId", !0)
  }, ar).models;
}
function T_(e, t) {
  return qx(e.data, e.sourceFormat, e.seriesLayoutBy, e.dimensionsDefine, e.startIndex, t);
}
function qx(e, t, r, n, i, a) {
  var o, s = 5;
  if (Zt(e))
    return re.Not;
  var u, l;
  if (n) {
    var f = n[a];
    F(f) ? (u = f.name, l = f.type) : N(f) && (u = f);
  }
  if (l != null)
    return l === "ordinal" ? re.Must : re.Not;
  if (t === zt) {
    var h = e;
    if (r === In) {
      for (var v = h[a], c = 0; c < (v || []).length && c < s; c++)
        if ((o = S(v[i + c])) != null)
          return o;
    } else
      for (var c = 0; c < h.length && c < s; c++) {
        var d = h[i + c];
        if (d && (o = S(d[a])) != null)
          return o;
      }
  } else if (t === Pe) {
    var y = e;
    if (!u)
      return re.Not;
    for (var c = 0; c < y.length && c < s; c++) {
      var p = y[c];
      if (p && (o = S(p[u])) != null)
        return o;
    }
  } else if (t === $e) {
    var g = e;
    if (!u)
      return re.Not;
    var v = g[u];
    if (!v || Zt(v))
      return re.Not;
    for (var c = 0; c < v.length && c < s; c++)
      if ((o = S(v[c])) != null)
        return o;
  } else if (t === ue)
    for (var m = e, c = 0; c < m.length && c < s; c++) {
      var p = m[c], _ = La(p);
      if (!B(_))
        return re.Not;
      if ((o = S(_[a])) != null)
        return o;
    }
  function S(b) {
    var w = N(b);
    if (b != null && isFinite(Number(b)) && b !== "")
      return w ? re.Might : re.Not;
    if (w && b !== "-")
      return re.Must;
  }
  return re.Not;
}
var $x = U();
function Zx(e, t, r) {
  var n = $x.get(t);
  if (!n)
    return r;
  var i = n(e);
  return i ? r.concat(i) : r;
}
var Dd = dt();
dt();
var uc = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.getColorFromPalette = function(t, r, n) {
      var i = qt(this.get("color", !0)), a = this.get("colorLayer", !0);
      return jx(this, Dd, i, a, t, r, n);
    }, e.prototype.clearColorPalette = function() {
      Qx(this, Dd);
    }, e;
  })()
);
function Kx(e, t) {
  for (var r = e.length, n = 0; n < r; n++)
    if (e[n].length > t)
      return e[n];
  return e[r - 1];
}
function jx(e, t, r, n, i, a, o) {
  a = a || e;
  var s = t(a), u = s.paletteIdx || 0, l = s.paletteNameMap = s.paletteNameMap || {};
  if (l.hasOwnProperty(i))
    return l[i];
  var f = o == null || !n ? r : Kx(n, o);
  if (f = f || r, !(!f || !f.length)) {
    var h = f[u];
    return i && (l[i] = h), s.paletteIdx = (u + 1) % f.length, h;
  }
}
function Qx(e, t) {
  t(e).paletteIdx = 0, t(e).paletteNameMap = {};
}
var lo, Li, Id, Ad = "\0_ec_inner", Jx = 1, lc = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      return e !== null && e.apply(this, arguments) || this;
    }
    return t.prototype.init = function(r, n, i, a, o, s) {
      a = a || {}, this.option = null, this._theme = new ct(a), this._locale = new ct(o), this._optionManager = s;
    }, t.prototype.setOption = function(r, n, i) {
      var a = Ed(n);
      this._optionManager.setOption(r, i, a), this._resetOption(null, a);
    }, t.prototype.resetOption = function(r, n) {
      return this._resetOption(r, Ed(n));
    }, t.prototype._resetOption = function(r, n) {
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
    }, t.prototype.mergeOption = function(r) {
      this._mergeOption(r, null);
    }, t.prototype._mergeOption = function(r, n) {
      var i = this.option, a = this._componentsMap, o = this._componentsCount, s = [], u = U(), l = n && n.replaceMergeMainTypeMap;
      Yx(this), T(r, function(h, v) {
        h != null && (at.hasClass(v) ? v && (s.push(v), u.set(v, !0)) : i[v] = i[v] == null ? Z(h) : pt(i[v], h, !0));
      }), l && l.each(function(h, v) {
        at.hasClass(v) && !u.get(v) && (s.push(v), u.set(v, !0));
      }), at.topologicalTravel(s, at.getAllClassMainTypes(), f, this);
      function f(h) {
        var v = Zx(this, h, qt(r[h])), c = a.get(h), d = (
          // `!oldCmptList` means init. See the comment in `mappingToExists`
          c ? l && l.get(h) ? "replaceMerge" : "normalMerge" : "replaceAll"
        ), y = pw(c, v, d);
        ww(y, h, at), i[h] = null, a.set(h, null), o.set(h, 0);
        var p = [], g = [], m = 0, _;
        T(y, function(S, b) {
          var w = S.existing, C = S.newOption;
          if (!C)
            w && (w.mergeOption({}, this), w.optionUpdated({}, !1));
          else {
            var M = h === "series", x = at.getClass(
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
        }, this), i[h] = p, a.set(h, g), o.set(h, m), h === "series" && lo(this);
      }
      this._seriesIndices || lo(this);
    }, t.prototype.getOption = function() {
      var r = Z(this.option);
      return T(r, function(n, i) {
        if (at.hasClass(i)) {
          for (var a = qt(n), o = a.length, s = !1, u = o - 1; u >= 0; u--)
            a[u] && !pa(a[u]) ? s = !0 : (a[u] = null, !s && o--);
          a.length = o, r[i] = a;
        }
      }), delete r[Ad], r;
    }, t.prototype.setTheme = function(r) {
      this._theme = new ct(r), this._resetOption("recreate", null);
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
      return i != null ? (u = [], T(qt(i), function(l) {
        s[l] && u.push(s[l]);
      })) : a != null ? u = Pd("id", a, s) : o != null ? u = Pd("name", o, s) : u = xt(s, function(l) {
        return !!l;
      }), Ld(u, r);
    }, t.prototype.findComponents = function(r) {
      var n = r.query, i = r.mainType, a = s(n), o = a ? this.queryComponents(a) : xt(this._componentsMap.get(i), function(l) {
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
        return r.filter ? xt(l, r.filter) : l;
      }
    }, t.prototype.eachComponent = function(r, n, i) {
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
    }, t.prototype.getSeriesByName = function(r) {
      var n = Ge(r, null);
      return xt(this._componentsMap.get("series"), function(i) {
        return !!i && n != null && i.name === n;
      });
    }, t.prototype.getSeriesByIndex = function(r) {
      return this._componentsMap.get("series")[r];
    }, t.prototype.getSeriesByType = function(r) {
      return xt(this._componentsMap.get("series"), function(n) {
        return !!n && n.subType === r;
      });
    }, t.prototype.getSeries = function() {
      return xt(this._componentsMap.get("series"), function(r) {
        return !!r;
      });
    }, t.prototype.getSeriesCount = function() {
      return this._componentsCount.get("series");
    }, t.prototype.eachSeries = function(r, n) {
      Li(this), T(this._seriesIndices, function(i) {
        var a = this._componentsMap.get("series")[i];
        r.call(n, a, i);
      }, this);
    }, t.prototype.eachRawSeries = function(r, n) {
      T(this._componentsMap.get("series"), function(i) {
        i && r.call(n, i, i.componentIndex);
      });
    }, t.prototype.eachSeriesByType = function(r, n, i) {
      Li(this), T(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        o.subType === r && n.call(i, o, a);
      }, this);
    }, t.prototype.eachRawSeriesByType = function(r, n, i) {
      return T(this.getSeriesByType(r), n, i);
    }, t.prototype.isSeriesFiltered = function(r) {
      return Li(this), this._seriesIndicesMap.get(r.componentIndex) == null;
    }, t.prototype.getCurrentSeriesIndices = function() {
      return (this._seriesIndices || []).slice();
    }, t.prototype.filterSeries = function(r, n) {
      Li(this);
      var i = [];
      T(this._seriesIndices, function(a) {
        var o = this._componentsMap.get("series")[a];
        r.call(n, o, a) && i.push(a);
      }, this), this._seriesIndices = i, this._seriesIndicesMap = U(i);
    }, t.prototype.restoreData = function(r) {
      lo(this);
      var n = this._componentsMap, i = [];
      n.each(function(a, o) {
        at.hasClass(o) && i.push(o);
      }), at.topologicalTravel(i, at.getAllClassMainTypes(), function(a) {
        T(n.get(a), function(o) {
          o && (a !== "series" || !tM(o, r)) && o.restoreData();
        });
      });
    }, t.internalField = (function() {
      lo = function(r) {
        var n = r._seriesIndices = [];
        T(r._componentsMap.get("series"), function(i) {
          i && n.push(i.componentIndex);
        }), r._seriesIndicesMap = U(n);
      }, Li = function(r) {
      }, Id = function(r, n) {
        r.option = {}, r.option[Ad] = Jx, r._componentsMap = U({
          series: []
        }), r._componentsCount = U();
        var i = n.aria;
        F(i) && i.enabled == null && (i.enabled = !0), eM(n, r._theme.option), pt(n, S_, !1), r._mergeOption(n, null);
      };
    })(), t;
  })(ct)
);
function tM(e, t) {
  if (t) {
    var r = t.seriesIndex, n = t.seriesId, i = t.seriesName;
    return r != null && e.componentIndex !== r || n != null && e.id !== n || i != null && e.name !== i;
  }
}
function eM(e, t) {
  var r = e.color && !e.colorLayer;
  T(t, function(n, i) {
    i === "colorLayer" && r || i === "color" && e.color || at.hasClass(i) || (typeof n == "object" ? e[i] = e[i] ? pt(e[i], n, !1) : Z(n) : e[i] == null && (e[i] = n));
  });
}
function Pd(e, t, r) {
  if (B(t)) {
    var n = U();
    return T(t, function(a) {
      if (a != null) {
        var o = Ge(a, null);
        o != null && n.set(a, !0);
      }
    }), xt(r, function(a) {
      return a && n.get(a[e]);
    });
  } else {
    var i = Ge(t, null);
    return xt(r, function(a) {
      return a && i != null && a[e] === i;
    });
  }
}
function Ld(e, t) {
  return t.hasOwnProperty("subType") ? xt(e, function(r) {
    return r && r.subType === t.subType;
  }) : e;
}
function Ed(e) {
  var t = U();
  return e && T(qt(e.replaceMerge), function(r) {
    t.set(r, !0);
  }), {
    replaceMergeMainTypeMap: t
  };
}
Ae(lc, uc);
var rM = /^(min|max)?(.+)$/, nM = (
  /** @class */
  (function() {
    function e(t) {
      this._timelineOptions = [], this._mediaList = [], this._currentMediaIndices = [], this._api = t;
    }
    return e.prototype.setOption = function(t, r, n) {
      t && (T(qt(t.series), function(o) {
        o && o.data && Zt(o.data) && fa(o.data);
      }), T(qt(t.dataset), function(o) {
        o && o.source && Zt(o.source) && fa(o.source);
      })), t = Z(t);
      var i = this._optionBackup, a = iM(t, r, !i);
      this._newBaseOption = a.baseOption, i ? (a.timelineOptions.length && (i.timelineOptions = a.timelineOptions), a.mediaList.length && (i.mediaList = a.mediaList), a.mediaDefault && (i.mediaDefault = a.mediaDefault)) : this._optionBackup = a;
    }, e.prototype.mountOption = function(t) {
      var r = this._optionBackup;
      return this._timelineOptions = r.timelineOptions, this._mediaList = r.mediaList, this._mediaDefault = r.mediaDefault, this._currentMediaIndices = [], Z(t ? r.baseOption : this._newBaseOption);
    }, e.prototype.getTimelineOption = function(t) {
      var r, n = this._timelineOptions;
      if (n.length) {
        var i = t.getComponent("timeline");
        i && (r = Z(
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
        aM(i[u].query, r, n) && o.push(u);
      return !o.length && a && (o = [-1]), o.length && !sM(o, this._currentMediaIndices) && (s = H(o, function(f) {
        return Z(f === -1 ? a.option : i[f].option);
      })), this._currentMediaIndices = o, s;
    }, e;
  })()
);
function iM(e, t, r) {
  var n = [], i, a, o = e.baseOption, s = e.timeline, u = e.options, l = e.media, f = !!e.media, h = !!(u || s || o && o.timeline);
  o ? (a = o, a.timeline || (a.timeline = s)) : ((h || f) && (e.options = e.media = null), a = e), f && B(l) && T(l, function(c) {
    c && c.option && (c.query ? n.push(c) : i || (i = c));
  }), v(a), T(u, function(c) {
    return v(c);
  }), T(n, function(c) {
    return v(c.option);
  });
  function v(c) {
    T(t, function(d) {
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
function aM(e, t, r) {
  var n = {
    width: t,
    height: r,
    aspectratio: t / r
    // lower case for convenience.
  }, i = !0;
  return T(e, function(a, o) {
    var s = o.match(rM);
    if (!(!s || !s[1] || !s[2])) {
      var u = s[1], l = s[2].toLowerCase();
      oM(n[l], a, u) || (i = !1);
    }
  }), i;
}
function oM(e, t, r) {
  return r === "min" ? e >= t : r === "max" ? e <= t : e === t;
}
function sM(e, t) {
  return e.join(",") === t.join(",");
}
var we = T, Sa = F, Rd = ["areaStyle", "lineStyle", "nodeStyle", "linkStyle", "chordStyle", "label", "labelLine"];
function yl(e) {
  var t = e && e.itemStyle;
  if (t)
    for (var r = 0, n = Rd.length; r < n; r++) {
      var i = Rd[r], a = t.normal, o = t.emphasis;
      a && a[i] && (e[i] = e[i] || {}, e[i].normal ? pt(e[i].normal, a[i]) : e[i].normal = a[i], a[i] = null), o && o[i] && (e[i] = e[i] || {}, e[i].emphasis ? pt(e[i].emphasis, o[i]) : e[i].emphasis = o[i], o[i] = null);
    }
}
function Nt(e, t, r) {
  if (e && e[t] && (e[t].normal || e[t].emphasis)) {
    var n = e[t].normal, i = e[t].emphasis;
    n && (r ? (e[t].normal = e[t].emphasis = null, _t(e[t], n)) : e[t] = n), i && (e.emphasis = e.emphasis || {}, e.emphasis[t] = i, i.focus && (e.emphasis.focus = i.focus), i.blurScope && (e.emphasis.blurScope = i.blurScope));
  }
}
function Zi(e) {
  Nt(e, "itemStyle"), Nt(e, "lineStyle"), Nt(e, "areaStyle"), Nt(e, "label"), Nt(e, "labelLine"), Nt(e, "upperLabel"), Nt(e, "edgeLabel");
}
function wt(e, t) {
  var r = Sa(e) && e[t], n = Sa(r) && r.textStyle;
  if (n)
    for (var i = 0, a = Ov.length; i < a; i++) {
      var o = Ov[i];
      n.hasOwnProperty(o) && (r[o] = n[o]);
    }
}
function ve(e) {
  e && (Zi(e), wt(e, "label"), e.emphasis && wt(e.emphasis, "label"));
}
function uM(e) {
  if (Sa(e)) {
    yl(e), Zi(e), wt(e, "label"), wt(e, "upperLabel"), wt(e, "edgeLabel"), e.emphasis && (wt(e.emphasis, "label"), wt(e.emphasis, "upperLabel"), wt(e.emphasis, "edgeLabel"));
    var t = e.markPoint;
    t && (yl(t), ve(t));
    var r = e.markLine;
    r && (yl(r), ve(r));
    var n = e.markArea;
    n && ve(n);
    var i = e.data;
    if (e.type === "graph") {
      i = i || e.nodes;
      var a = e.links || e.edges;
      if (a && !Zt(a))
        for (var o = 0; o < a.length; o++)
          ve(a[o]);
      T(e.categories, function(l) {
        Zi(l);
      });
    }
    if (i && !Zt(i))
      for (var o = 0; o < i.length; o++)
        ve(i[o]);
    if (t = e.markPoint, t && t.data)
      for (var s = t.data, o = 0; o < s.length; o++)
        ve(s[o]);
    if (r = e.markLine, r && r.data)
      for (var u = r.data, o = 0; o < u.length; o++)
        B(u[o]) ? (ve(u[o][0]), ve(u[o][1])) : ve(u[o]);
    e.type === "gauge" ? (wt(e, "axisLabel"), wt(e, "title"), wt(e, "detail")) : e.type === "treemap" ? (Nt(e.breadcrumb, "itemStyle"), T(e.levels, function(l) {
      Zi(l);
    })) : e.type === "tree" && Zi(e.leaves);
  }
}
function tr(e) {
  return B(e) ? e : e ? [e] : [];
}
function Od(e) {
  return (B(e) ? e[0] : e) || {};
}
function lM(e, t) {
  we(tr(e.series), function(n) {
    Sa(n) && uM(n);
  });
  var r = ["xAxis", "yAxis", "radiusAxis", "angleAxis", "singleAxis", "parallelAxis", "radar"];
  t && r.push("valueAxis", "categoryAxis", "logAxis", "timeAxis"), we(r, function(n) {
    we(tr(e[n]), function(i) {
      i && (wt(i, "axisLabel"), wt(i.axisPointer, "label"));
    });
  }), we(tr(e.parallel), function(n) {
    var i = n && n.parallelAxisDefault;
    wt(i, "axisLabel"), wt(i && i.axisPointer, "label");
  }), we(tr(e.calendar), function(n) {
    Nt(n, "itemStyle"), wt(n, "dayLabel"), wt(n, "monthLabel"), wt(n, "yearLabel");
  }), we(tr(e.radar), function(n) {
    wt(n, "name"), n.name && n.axisName == null && (n.axisName = n.name, delete n.name), n.nameGap != null && n.axisNameGap == null && (n.axisNameGap = n.nameGap, delete n.nameGap);
  }), we(tr(e.geo), function(n) {
    Sa(n) && (ve(n), we(tr(n.regions), function(i) {
      ve(i);
    }));
  }), we(tr(e.timeline), function(n) {
    ve(n), Nt(n, "label"), Nt(n, "itemStyle"), Nt(n, "controlStyle", !0);
    var i = n.data;
    B(i) && T(i, function(a) {
      F(a) && (Nt(a, "label"), Nt(a, "itemStyle"));
    });
  }), we(tr(e.toolbox), function(n) {
    Nt(n, "iconStyle"), we(n.feature, function(i) {
      Nt(i, "iconStyle");
    });
  }), wt(Od(e.axisPointer), "label"), wt(Od(e.tooltip).axisPointer, "label");
}
function fM(e, t) {
  for (var r = t.split(","), n = e, i = 0; i < r.length && (n = n && n[r[i]], n != null); i++)
    ;
  return n;
}
function hM(e, t, r, n) {
  for (var i = t.split(","), a = e, o, s = 0; s < i.length - 1; s++)
    o = i[s], a[o] == null && (a[o] = {}), a = a[o];
  a[i[s]] == null && (a[i[s]] = r);
}
function kd(e) {
  e && T(cM, function(t) {
    t[0] in e && !(t[1] in e) && (e[t[1]] = e[t[0]]);
  });
}
var cM = [["x", "left"], ["y", "top"], ["x2", "right"], ["y2", "bottom"]], vM = ["grid", "geo", "parallel", "legend", "toolbox", "title", "visualMap", "dataZoom", "timeline"], ml = [["borderRadius", "barBorderRadius"], ["borderColor", "barBorderColor"], ["borderWidth", "barBorderWidth"]];
function Ei(e) {
  var t = e && e.itemStyle;
  if (t)
    for (var r = 0; r < ml.length; r++) {
      var n = ml[r][1], i = ml[r][0];
      t[n] != null && (t[i] = t[n]);
    }
}
function Bd(e) {
  e && e.alignTo === "edge" && e.margin != null && e.edgeDistance == null && (e.edgeDistance = e.margin);
}
function Nd(e) {
  e && e.downplay && !e.blur && (e.blur = e.downplay);
}
function dM(e) {
  e && e.focusNodeAdjacency != null && (e.emphasis = e.emphasis || {}, e.emphasis.focus == null && (e.emphasis.focus = "adjacency"));
}
function C_(e, t) {
  if (e)
    for (var r = 0; r < e.length; r++)
      t(e[r]), e[r] && C_(e[r].children, t);
}
function x_(e, t) {
  lM(e, t), e.series = qt(e.series), T(e.series, function(r) {
    if (F(r)) {
      var n = r.type;
      if (n === "line")
        r.clipOverflow != null && (r.clip = r.clipOverflow);
      else if (n === "pie" || n === "gauge") {
        r.clockWise != null && (r.clockwise = r.clockWise), Bd(r.label);
        var i = r.data;
        if (i && !Zt(i))
          for (var a = 0; a < i.length; a++)
            Bd(i[a]);
        r.hoverOffset != null && (r.emphasis = r.emphasis || {}, (r.emphasis.scaleSize = null) && (r.emphasis.scaleSize = r.hoverOffset));
      } else if (n === "gauge") {
        var o = fM(r, "pointer.color");
        o != null && hM(r, "itemStyle.color", o);
      } else if (n === "bar") {
        Ei(r), Ei(r.backgroundStyle), Ei(r.emphasis);
        var i = r.data;
        if (i && !Zt(i))
          for (var a = 0; a < i.length; a++)
            typeof i[a] == "object" && (Ei(i[a]), Ei(i[a] && i[a].emphasis));
      } else if (n === "sunburst") {
        var s = r.highlightPolicy;
        s && (r.emphasis = r.emphasis || {}, r.emphasis.focus || (r.emphasis.focus = s)), Nd(r), C_(r.data, Nd);
      } else n === "graph" || n === "sankey" ? dM(r) : n === "map" && (r.mapType && !r.map && (r.map = r.mapType), r.mapLocation && _t(r, r.mapLocation));
      r.hoverAnimation != null && (r.emphasis = r.emphasis || {}, r.emphasis && r.emphasis.scale == null && (r.emphasis.scale = r.hoverAnimation)), kd(r);
    }
  }), e.dataRange && (e.visualMap = e.dataRange), T(vM, function(r) {
    var n = e[r];
    n && (B(n) || (n = [n]), T(n, function(i) {
      kd(i);
    }));
  });
}
var pM = Dh(gM);
function gM(e) {
  var t = U();
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
      i.push(o);
    }
  }), t.each(function(r) {
    if (r.length !== 0) {
      var n = r[0].seriesModel, i = n.get("stackOrder") || "seriesAsc";
      i === "seriesDesc" && r.reverse(), T(r, function(a, o) {
        a.data.setCalculationInfo("stackedOnSeries", o > 0 ? r[o - 1].seriesModel : null);
      }), yM(r);
    }
  });
}
function yM(e) {
  T(e, function(t, r) {
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
          var m = g.data.getByRawIndex(g.stackResultDimension, d);
          if (u === "all" || u === "positive" && m > 0 || u === "negative" && m < 0 || u === "samesign" && v >= 0 && m > 0 || u === "samesign" && v <= 0 && m < 0) {
            v = iw(v, m), y = m;
            break;
          }
        }
      }
      return n[0] = v, n[1] = y, n;
    });
  });
}
var vu = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.data = t.data || (t.sourceFormat === $e ? {} : []), this.sourceFormat = t.sourceFormat || dm, this.seriesLayoutBy = t.seriesLayoutBy || Ie, this.startIndex = t.startIndex || 0, this.dimensionsDetectedCount = t.dimensionsDetectedCount, this.metaRawOption = t.metaRawOption;
      var r = this.dimensionsDefine = t.dimensionsDefine;
      if (r)
        for (var n = 0; n < r.length; n++) {
          var i = r[n];
          i.type == null && T_(this, n) === re.Must && (i.type = "ordinal");
        }
    }
    return e;
  })()
);
function fc(e) {
  return e instanceof vu;
}
function kf(e, t, r) {
  r = r || D_(e);
  var n = t.seriesLayoutBy, i = _M(e, r, n, t.sourceHeader, t.dimensions), a = new vu({
    data: e,
    sourceFormat: r,
    seriesLayoutBy: n,
    dimensionsDefine: i.dimensionsDefine,
    startIndex: i.startIndex,
    dimensionsDetectedCount: i.dimensionsDetectedCount,
    metaRawOption: Z(t)
  });
  return a;
}
function M_(e) {
  return new vu({
    data: e,
    sourceFormat: Zt(e) ? Ir : ue
  });
}
function mM(e) {
  return new vu({
    data: e.data,
    sourceFormat: e.sourceFormat,
    seriesLayoutBy: e.seriesLayoutBy,
    dimensionsDefine: Z(e.dimensionsDefine),
    startIndex: e.startIndex,
    dimensionsDetectedCount: e.dimensionsDetectedCount
  });
}
function D_(e) {
  var t = dm;
  if (Zt(e))
    t = Ir;
  else if (B(e)) {
    e.length === 0 && (t = zt);
    for (var r = 0, n = e.length; r < n; r++) {
      var i = e[r];
      if (i != null) {
        if (B(i) || Zt(i)) {
          t = zt;
          break;
        } else if (F(i)) {
          t = Pe;
          break;
        }
      }
    }
  } else if (F(e)) {
    for (var a in e)
      if (Xt(e, a) && $t(e[a])) {
        t = $e;
        break;
      }
  }
  return t;
}
function _M(e, t, r, n, i) {
  var a, o;
  if (!e)
    return {
      dimensionsDefine: Fd(i),
      startIndex: o,
      dimensionsDetectedCount: a
    };
  if (t === zt) {
    var s = e;
    n === "auto" || n == null ? zd(function(l) {
      l != null && l !== "-" && (N(l) ? o == null && (o = 1) : o = 0);
    }, r, s, 10) : o = vt(n) ? n : n ? 1 : 0, !i && o === 1 && (i = [], zd(function(l, f) {
      i[f] = l != null ? l + "" : "";
    }, r, s, 1 / 0)), a = i ? i.length : r === In ? s.length : s[0] ? s[0].length : null;
  } else if (t === Pe)
    i || (i = SM(e));
  else if (t === $e)
    i || (i = [], T(e, function(l, f) {
      i.push(f);
    }));
  else if (t === ue) {
    var u = La(e[0]);
    a = B(u) && u.length || 1;
  }
  return {
    startIndex: o,
    dimensionsDefine: Fd(i),
    dimensionsDetectedCount: a
  };
}
function SM(e) {
  for (var t = 0, r; t < e.length && !(r = e[t++]); )
    ;
  if (r)
    return mt(r);
}
function Fd(e) {
  if (e) {
    var t = U();
    return H(e, function(r, n) {
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
      var a = t.get(i.name);
      return a ? i.name += "-" + a.count++ : t.set(i.name, {
        count: 1
      }), i;
    });
  }
}
function zd(e, t, r, n) {
  if (t === In)
    for (var i = 0; i < r.length && i < n; i++)
      e(r[i] ? r[i][0] : null, i);
  else
    for (var a = r[0] || [], i = 0; i < a.length && i < n; i++)
      e(a[i], i);
}
function I_(e) {
  var t = e.sourceFormat;
  return t === Pe || t === $e;
}
var en, rn, nn, an, Hd, Vd, A_ = (
  /** @class */
  (function() {
    function e(t, r) {
      var n = fc(t) ? t : M_(t);
      this._source = n;
      var i = this._data = n.data, a = n.sourceFormat;
      n.seriesLayoutBy, a === Ir && (this._offset = 0, this._dimSize = r, this._data = i), Vd(this, i, n);
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
      Vd = function(o, s, u) {
        var l = u.sourceFormat, f = u.seriesLayoutBy, h = u.startIndex, v = u.dimensionsDefine, c = Hd[hc(l, f)];
        if (R(o, c), l === Ir)
          o.getItem = r, o.count = i, o.fillStorage = n;
        else {
          var d = P_(l, f);
          o.getItem = ht(d, null, s, h, v);
          var y = L_(l, f);
          o.count = ht(y, null, s, h, v);
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
      Hd = (t = {}, t[zt + "_" + Ie] = {
        pure: !0,
        appendData: a
      }, t[zt + "_" + In] = {
        pure: !0,
        appendData: function() {
          throw new Error('Do not support appendData when set seriesLayoutBy: "row".');
        }
      }, t[Pe] = {
        pure: !0,
        appendData: a
      }, t[$e] = {
        pure: !0,
        appendData: function(o) {
          var s = this._data;
          T(o, function(u, l) {
            for (var f = s[l] || (s[l] = []), h = 0; h < (u || []).length; h++)
              f.push(u[h]);
          });
        }
      }, t[ue] = {
        appendData: a
      }, t[Ir] = {
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
), fo = function(e) {
  B(e) || $y("series.data or dataset.source must be an array.");
};
en = {}, en[zt + "_" + Ie] = fo, en[zt + "_" + In] = fo, en[Pe] = fo, en[$e] = function(e, t) {
  for (var r = 0; r < t.length; r++) {
    var n = t[r].name;
    n == null && $y("dimension name must not be null/undefined.");
  }
}, en[ue] = fo;
var Gd = function(e, t, r, n) {
  return e[n];
}, bM = (rn = {}, rn[zt + "_" + Ie] = function(e, t, r, n) {
  return e[n + t];
}, rn[zt + "_" + In] = function(e, t, r, n, i) {
  n += t;
  for (var a = i || [], o = e, s = 0; s < o.length; s++) {
    var u = o[s];
    a[s] = u ? u[n] : null;
  }
  return a;
}, rn[Pe] = Gd, rn[$e] = function(e, t, r, n, i) {
  for (var a = i || [], o = 0; o < r.length; o++) {
    var s = r[o].name, u = s != null ? e[s] : null;
    a[o] = u ? u[n] : null;
  }
  return a;
}, rn[ue] = Gd, rn);
function P_(e, t) {
  var r = bM[hc(e, t)];
  return r;
}
var Ud = function(e, t, r) {
  return e.length;
}, wM = (nn = {}, nn[zt + "_" + Ie] = function(e, t, r) {
  return Math.max(0, e.length - t);
}, nn[zt + "_" + In] = function(e, t, r) {
  var n = e[0];
  return n ? Math.max(0, n.length - t) : 0;
}, nn[Pe] = Ud, nn[$e] = function(e, t, r) {
  var n = r[0].name, i = n != null ? e[n] : null;
  return i ? i.length : 0;
}, nn[ue] = Ud, nn);
function L_(e, t) {
  var r = wM[hc(e, t)];
  return r;
}
var _l = function(e, t, r) {
  return e[t];
}, TM = (an = {}, an[zt] = _l, an[Pe] = function(e, t, r) {
  return e[r];
}, an[$e] = _l, an[ue] = function(e, t, r) {
  var n = La(e);
  return n instanceof Array ? n[t] : n;
}, an[Ir] = _l, an);
function E_(e) {
  var t = TM[e];
  return t;
}
function hc(e, t) {
  return e === zt ? e + "_" + t : e;
}
function _s(e, t, r) {
  if (e) {
    var n = e.getRawDataItem(t);
    if (n != null) {
      var i = e.getStore(), a = i.getSource().sourceFormat;
      if (r != null) {
        var o = e.getDimensionIndex(r), s = i.getDimensionProperty(o);
        return E_(a)(n, o, s);
      } else {
        var u = n;
        return a === ue && (u = La(n)), u;
      }
    }
  }
}
var CM = /\{@(.+?)\}/g, xM = (
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
      if (o && (u.value = o.interpolatedValue), i != null && B(u.value) && (u.value = u.value[i]), !a) {
        var l = s.getItemModel(t);
        a = l.get(r === "normal" ? ["label", "formatter"] : [r, "label", "formatter"]);
      }
      if (X(a))
        return u.status = r, u.dimensionIndex = i, a(u);
      if (N(a)) {
        var f = ac(a, u);
        return f.replace(CM, function(h, v) {
          var c = v.length, d = v;
          d.charAt(0) === "[" && d.charAt(c - 1) === "]" && (d = +d.slice(1, c - 1));
          var y = _s(s, t, d);
          if (o && B(o.interpolatedValue)) {
            var p = s.getDimensionIndex(d);
            p >= 0 && (y = o.interpolatedValue[p]);
          }
          return y != null ? y + "" : "";
        });
      }
    }, e.prototype.getRawValue = function(t, r) {
      return _s(this.getData(r), t);
    }, e.prototype.formatTooltip = function(t, r, n) {
    }, e;
  })()
);
function Yd(e) {
  var t, r;
  return F(e) ? e.type && (r = e) : t = e, {
    text: t,
    // markers: markers || markersExisting,
    frag: r
  };
}
function aa(e) {
  return new MM(e);
}
var MM = (
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
      function f(m) {
        return !(m >= 1) && (m = 1), m;
      }
      var h;
      (this._dirty || a === "reset") && (this._dirty = !1, h = this._doReset(n)), this._modBy = u, this._modDataCount = l;
      var v = t && t.step;
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
    }, e.prototype.dirty = function() {
      this._dirty = !0, this._onDirty && this._onDirty(this.context);
    }, e.prototype._doProgress = function(t, r, n, i, a) {
      Wd.reset(r, n, i, a), this._callingProgress = t, this._callingProgress({
        start: r,
        end: n,
        count: n - r,
        next: Wd.next
      }, this.context);
    }, e.prototype._doReset = function(t) {
      this._dueIndex = this._outputDueEnd = this._dueEnd = 0, this._settedOutputEnd = null;
      var r, n;
      !t && this._reset && (r = this._reset(this.context), r && r.progress && (n = r.forceFirstProgress, r = r.progress), B(r) && !r.length && (r = null)), this._progress = r, this._modBy = this._modDataCount = null;
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
), Wd = /* @__PURE__ */ (function() {
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
function $o(e, t) {
  var r = t && t.type;
  return r === "ordinal" ? e : (r === "time" && !vt(e) && e != null && e !== "-" && (e = +cr(e)), e == null || e === "" ? NaN : Number(e));
}
var DM = U({
  number: function(e) {
    return parseFloat(e);
  },
  time: function(e) {
    return +cr(e);
  },
  trim: function(e) {
    return N(e) ? Me(e) : e;
  }
});
function WE(e) {
  return DM.get(e);
}
var R_ = {
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
}, IM = (
  /** @class */
  (function() {
    function e(t, r) {
      if (!vt(r)) {
        var n = "";
        Yt(n);
      }
      this._opFn = R_[t], this._rvalFloat = lr(r);
    }
    return e.prototype.evaluate = function(t) {
      return vt(t) ? this._opFn(t, this._rvalFloat) : this._opFn(lr(t), this._rvalFloat);
    }, e;
  })()
), AM = (
  /** @class */
  (function() {
    function e(t, r) {
      var n = t === "desc";
      this._resultLT = n ? 1 : -1, r == null && (r = n ? "min" : "max"), this._incomparable = r === "min" ? -1 / 0 : 1 / 0;
    }
    return e.prototype.evaluate = function(t, r) {
      var n = vt(t) ? t : lr(t), i = vt(r) ? r : lr(r), a = isNaN(n), o = isNaN(i);
      if (a && (n = this._incomparable), o && (i = this._incomparable), a && o) {
        var s = N(t), u = N(r);
        s && (n = u ? t : 0), u && (i = s ? r : 0);
      }
      return n < i ? this._resultLT : n > i ? -this._resultLT : 0;
    }, e;
  })()
), PM = (
  /** @class */
  (function() {
    function e(t, r) {
      this._rval = r, this._isEQ = t, this._rvalTypeof = typeof r, this._rvalFloat = lr(r);
    }
    return e.prototype.evaluate = function(t) {
      var r = t === this._rval;
      if (!r) {
        var n = typeof t;
        n !== this._rvalTypeof && (n === "number" || this._rvalTypeof === "number") && (r = lr(t) === this._rvalFloat);
      }
      return this._isEQ ? r : !r;
    }, e;
  })()
);
function XE(e, t) {
  return e === "eq" || e === "ne" ? new PM(e === "eq", t) : Xt(R_, e) ? new IM(e, t) : null;
}
function LM(e) {
  var t = "", r = -1 / 0, n = -1 / 0, i = 1 / 0, a = 1 / 0;
  return e && (e.g != null && (t += "G" + e.g, r = e.g), e.ge != null && (t += "GE" + e.ge, n = e.ge), e.l != null && (t += "L" + e.l, i = e.l), e.le != null && (t += "LE" + e.le, a = e.le)), {
    key: t,
    g: r,
    ge: n,
    l: i,
    le: a
  };
}
function EM(e, t) {
  return t > e.g && t >= e.ge && t < e.l && t <= e.le;
}
var RM = (
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
      return $o(t, r);
    }, e;
  })()
);
function OM(e, t) {
  var r = new RM(), n = e.data, i = r.sourceFormat = e.sourceFormat, a = e.startIndex, o = "";
  e.seriesLayoutBy !== Ie && Yt(o);
  var s = [], u = {}, l = e.dimensionsDefine;
  if (l)
    T(l, function(y, p) {
      var g = y.name, m = {
        index: p,
        name: g,
        displayName: y.displayName
      };
      if (s.push(m), g != null) {
        var _ = "";
        Xt(u, g) && Yt(_), u[g] = m;
      }
    });
  else
    for (var f = 0; f < e.dimensionsDetectedCount; f++)
      s.push({
        index: f
      });
  var h = P_(i, Ie);
  t.__isBuiltIn && (r.getRawDataItem = function(y) {
    return h(n, a, s, y);
  }, r.getRawData = ht(kM, null, e)), r.cloneRawData = ht(BM, null, e);
  var v = L_(i, Ie);
  r.count = ht(v, null, n, a, s);
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
  return r.getDimensionInfo = ht(NM, null, s, u), r.cloneAllDimensionInfo = ht(FM, null, s), r;
}
function kM(e) {
  var t = e.sourceFormat;
  if (!cc(t)) {
    var r = "";
    Yt(r);
  }
  return e.data;
}
function BM(e) {
  var t = e.sourceFormat, r = e.data;
  if (!cc(t)) {
    var n = "";
    Yt(n);
  }
  if (t === zt) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(r[a].slice());
    return i;
  } else if (t === Pe) {
    for (var i = [], a = 0, o = r.length; a < o; a++)
      i.push(R({}, r[a]));
    return i;
  }
}
function NM(e, t, r) {
  if (r != null) {
    if (vt(r) || !isNaN(r) && !Xt(t, r))
      return e[r];
    if (Xt(t, r))
      return t[r];
  }
}
function FM(e) {
  return Z(e);
}
var O_ = U();
function zM(e) {
  e = Z(e);
  var t = e.type, r = "";
  t || Yt(r);
  var n = t.split(":");
  n.length !== 2 && Yt(r);
  var i = !1;
  n[0] === "echarts" && (t = n[1], i = !0), e.__isBuiltIn = i, O_.set(t, e);
}
function HM(e, t, r) {
  var n = qt(e), i = n.length, a = "";
  i || Yt(a);
  for (var o = 0, s = i; o < s; o++) {
    var u = n[o];
    t = VM(u, t), o !== s - 1 && (t.length = Math.max(t.length, 1));
  }
  return t;
}
function VM(e, t, r, n) {
  var i = "";
  t.length || Yt(i), F(e) || Yt(i);
  var a = e.type, o = O_.get(a);
  o || Yt(i);
  var s = H(t, function(l) {
    return OM(l, o);
  }), u = qt(o.transform({
    upstream: s[0],
    upstreamList: s,
    config: Z(e.config)
  }));
  return H(u, function(l, f) {
    var h = "";
    F(l) || Yt(h), l.data || Yt(h);
    var v = D_(l.data);
    cc(v) || Yt(h);
    var c, d = t[0];
    if (d && f === 0 && !l.dimensions) {
      var y = d.startIndex;
      y && (l.data = d.data.slice(0, y).concat(l.data)), c = {
        seriesLayoutBy: Ie,
        sourceHeader: y,
        dimensions: d.metaRawOption.dimensions
      };
    } else
      c = {
        seriesLayoutBy: Ie,
        sourceHeader: 0,
        dimensions: l.dimensions
      };
    return kf(l.data, c, null);
  });
}
function cc(e) {
  return e === zt || e === Pe;
}
var GM = typeof Uint32Array === Qs ? Array : Uint32Array, UM = typeof Uint16Array === Qs ? Array : Uint16Array, k_ = typeof Int32Array === Qs ? Array : Int32Array, Xd = typeof Float64Array === Qs ? Array : Float64Array, B_ = {
  float: Xd,
  int: k_,
  // Ordinal data type can be string or int
  ordinal: Array,
  number: Array,
  time: Xd
}, Sl;
function Hn(e) {
  return e > 65535 ? GM : UM;
}
function YM(e) {
  var t = e.constructor;
  return t === Array ? e.slice() : new t(e);
}
function qd(e, t, r, n, i) {
  var a = B_[r || "float"];
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
var Bf = (
  /** @class */
  (function() {
    function e() {
      this._chunks = [], this._rawExtent = [], this._extent = [], this._count = 0, this._rawCount = 0, this._calcDimNameToIdx = U();
    }
    return e.prototype.initData = function(t, r, n) {
      this._provider = t, this._chunks = [], this._indices = null, this.getRawIndex = this._getRawIdxIdentity;
      var i = t.getSource(), a = this.defaultDimValueGetter = Sl[i.sourceFormat];
      this._dimValueGetter = n || a, this._rawExtent = [], I_(i), this._dimensions = H(r, function(o) {
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
      }, n.set(t, a), this._chunks[a] = new B_[r || "float"](this._rawCount), this._rawExtent[a] = Ne(), a;
    }, e.prototype.collectOrdinalMeta = function(t, r) {
      var n = this._chunks[t], i = this._dimensions[t], a = this._rawExtent, o = i.ordinalOffset || 0, s = n.length;
      o === 0 && (a[t] = Ne());
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
        qd(n, l, f.type, u, !0);
      }
      for (var h = [], v = s; v < u; v++)
        for (var c = v - s, d = 0; d < a; d++) {
          var f = i[d], y = Sl.arrayRows.call(this, t[c] || h, f.property, c, d);
          n[d][v] = y;
          var p = o[d];
          y < p[0] && (p[0] = y), y > p[1] && (p[1] = y);
        }
      return this._rawCount = this._count = u, {
        start: s,
        end: u
      };
    }, e.prototype._initDataFromProvider = function(t, r, n) {
      for (var i = this._provider, a = this._chunks, o = this._dimensions, s = o.length, u = this._rawExtent, l = H(o, function(m) {
        return m.property;
      }), f = 0; f < s; f++) {
        var h = o[f];
        u[f] || (u[f] = Ne()), qd(a, f, h.type, r, n);
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
      this.each([t], function(i) {
        isNaN(i) || r.push(i);
      }), Sh(r);
      var n = this.count();
      return n === 0 ? 0 : n % 2 === 1 ? r[(n - 1) / 2] : (r[n / 2] + r[n / 2 - 1]) / 2;
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
        var n = Hn(this._rawCount);
        t = new n(this.count());
        for (var a = 0; a < t.length; a++)
          t[a] = a;
      }
      return t;
    }, e.prototype.filter = function(t, r) {
      if (!this._count)
        return this;
      for (var n = this.clone(), i = n.count(), a = Hn(n._rawCount), o = new a(i), s = [], u = t.length, l = 0, f = t[0], h = n._chunks, v = 0; v < i; v++) {
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
      var i = mt(t), a = i.length;
      if (!a)
        return this;
      var o = r.count(), s = Hn(r._rawCount), u = new s(o), l = 0, f = i[0], h = t[f][0], v = t[f][1], c = r._chunks, d = !1;
      if (!r._indices) {
        var y = 0;
        if (a === 1) {
          for (var p = c[i[0]], g = 0; g < n; g++) {
            var m = p[g];
            (m >= h && m <= v || isNaN(m)) && (u[l++] = y), y++;
          }
          d = !0;
        } else if (a === 2) {
          for (var p = c[i[0]], _ = c[i[1]], S = t[i[1]][0], b = t[i[1]][1], g = 0; g < n; g++) {
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
              (m < t[D][0] || m > t[D][1]) && (M = !1);
            }
            M && (u[l++] = r.getRawIndex(g));
          }
      return l < o && (r._indices = u), r._count = l, r._extent = [], r._updateGetRawIdx(), r;
    }, e.prototype.map = function(t, r) {
      var n = this.clone(t);
      return this._updateDims(n, t, r), n;
    }, e.prototype.modify = function(t, r) {
      this._updateDims(this, t, r);
    }, e.prototype._updateDims = function(t, r, n) {
      for (var i = t._chunks, a = [], o = r.length, s = t.count(), u = [], l = t._rawExtent, f = 0; f < r.length; f++)
        l[r[f]] = Ne();
      for (var h = 0; h < s; h++) {
        for (var v = t.getRawIndex(h), c = 0; c < o; c++)
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
    }, e.prototype.lttbDownSample = function(t, r) {
      var n = this.clone([t], !0), i = n._chunks, a = i[t], o = this.count(), s = 0, u = Math.floor(1 / r), l = this.getRawIndex(0), f, h, v, c = new (Hn(this._rawCount))(Math.min((Math.ceil(o / u) + 2) * 2, o));
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
    }, e.prototype.minmaxDownSample = function(t, r) {
      for (var n = this.clone([t], !0), i = n._chunks, a = Math.floor(1 / r), o = i[t], s = this.count(), u = new (Hn(this._rawCount))(Math.ceil(s / a) * 2), l = 0, f = 0; f < s; f += a) {
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
    }, e.prototype.downSample = function(t, r, n, i) {
      for (var a = this.clone([t], !0), o = a._chunks, s = [], u = Math.floor(1 / r), l = o[t], f = this.count(), h = a._rawExtent[t] = Ne(), v = new (Hn(this._rawCount))(Math.ceil(f / u)), c = 0, d = 0; d < f; d += u) {
        u > f - d && (u = f - d, s.length = u);
        for (var y = 0; y < u; y++) {
          var p = this.getRawIndex(d + y);
          s[y] = l[p];
        }
        var g = n(s), m = this.getRawIndex(Math.min(d + i(s, g) || 0, f - 1));
        l[m] = g, g < h[0] && (h[0] = g), g > h[1] && (h[1] = g), v[c++] = m;
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
    }, e.prototype.getDataExtent = function(t, r) {
      var n = this._chunks[t], i = Ne();
      if (!n)
        return i;
      var a = this.count(), o = !this._indices && !r;
      if (o)
        return this._rawExtent[t].slice();
      var s = this._extent, u = s[t] || (s[t] = {}), l = LM(r), f = l.key, h = u[f];
      if (h)
        return h.slice();
      for (var v = i[0], c = i[1], d = 0; d < a; d++) {
        var y = this.getRawIndex(d), p = n[y];
        (!r || EM(l, p)) && (p < v && (v = p), p > c && (c = p));
      }
      return u[f] = [v, c];
    }, e.prototype.getRawDataItem = function(t) {
      var r = this.getRawIndex(t);
      if (this._provider.persistent)
        return this._provider.getItem(r);
      for (var n = [], i = this._chunks, a = 0; a < i.length; a++)
        n.push(i[a][r]);
      return n;
    }, e.prototype.clone = function(t, r) {
      var n = new e(), i = this._chunks, a = t && Ye(t, function(s, u) {
        return s[u] = !0, s;
      }, {});
      if (a)
        for (var o = 0; o < i.length; o++)
          n._chunks[o] = a[o] ? YM(i[o]) : i[o];
      else
        n._chunks = i;
      return this._copyCommonProps(n), r || (n._indices = this._cloneIndices()), n._updateGetRawIdx(), n;
    }, e.prototype._copyCommonProps = function(t) {
      t._count = this._count, t._rawCount = this._rawCount, t._provider = this._provider, t._dimensions = this._dimensions, t._extent = Z(this._extent), t._rawExtent = Z(this._rawExtent);
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
        return $o(r[a], this._dimensions[a]);
      }
      Sl = {
        arrayRows: t,
        objectRows: function(r, n, i, a) {
          return $o(r[n], this._dimensions[a]);
        },
        keyedColumns: t,
        original: function(r, n, i, a) {
          var o = r && (r.value == null ? r : r.value);
          return $o(o instanceof Array ? o[a] : o, this._dimensions[a]);
        },
        typedArray: function(r, n, i, a) {
          return r[a];
        }
      };
    })(), e;
  })()
), N_ = (
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
      if (ho(t)) {
        var o = t, s = void 0, u = void 0, l = void 0;
        if (n) {
          var f = r[0];
          f.prepareSource(), l = f.getSource(), s = l.data, u = l.sourceFormat, a = [f._getVersionSign()];
        } else
          s = o.get("data", !0), u = Zt(s) ? Ir : ue, a = [];
        var h = this._getSourceMetaRawOption() || {}, v = l && l.metaRawOption || {}, c = G(h.seriesLayoutBy, v.seriesLayoutBy) || null, d = G(h.sourceHeader, v.sourceHeader), y = G(h.dimensions, v.dimensions), p = c !== v.seriesLayoutBy || !!d != !!v.sourceHeader || y;
        i = p ? [kf(s, {
          seriesLayoutBy: c,
          sourceHeader: d,
          dimensions: y
        }, u)] : [];
      } else {
        var g = t;
        if (n) {
          var m = this._applyTransform(r);
          i = m.sourceList, a = m.upstreamSignList;
        } else {
          var _ = g.get("source", !0);
          i = [kf(_, this._getSourceMetaRawOption(), null)], a = [];
        }
      }
      this._setLocalSource(i, a);
    }, e.prototype._applyTransform = function(t) {
      var r = this._sourceHost, n = r.get("transform", !0), i = r.get("fromTransformResult", !0);
      if (i != null) {
        var a = "";
        t.length !== 1 && Zd(a);
      }
      var o, s = [], u = [];
      return T(t, function(l) {
        l.prepareSource();
        var f = l.getSource(i || 0), h = "";
        i != null && !f && Zd(h), s.push(f), u.push(l._getVersionSign());
      }), n ? o = HM(n, s, {
        datasetIndex: r.componentIndex
      }) : i != null && (o = [mM(s[0])]), {
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
        ho(this._sourceHost) && u ? s = u._innerGetDataStore(t, r, n) : (s = new Bf(), s.initData(new A_(r, t.length), t)), o[n] = s;
      }
      return s;
    }, e.prototype._getUpstreamSourceManagers = function() {
      var t = this._sourceHost;
      if (ho(t)) {
        var r = w_(t);
        return r ? [r.getSourceManager()] : [];
      } else
        return H(Xx(t), function(n) {
          return n.getSourceManager();
        });
    }, e.prototype._getSourceMetaRawOption = function() {
      var t = this._sourceHost, r, n, i;
      if (ho(t))
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
function $d(e) {
  var t = e.option.transform;
  t && fa(e.option.transform);
}
function ho(e) {
  return e.mainType === "series";
}
function Zd(e) {
  throw new Error(e);
}
var WM = "line-height:1";
function F_(e) {
  var t = e.lineHeight;
  return t == null ? WM : "line-height:" + Ut(t + "") + "px";
}
function z_(e, t) {
  var r = e.color || j.color.tertiary, n = e.fontSize || 12, i = e.fontWeight || "400", a = e.color || j.color.secondary, o = e.fontSize || 14, s = e.fontWeight || "900";
  return t === "html" ? {
    // eslint-disable-next-line max-len
    nameStyle: "font-size:" + Ut(n + "") + "px;color:" + Ut(r) + ";font-weight:" + Ut(i + ""),
    // eslint-disable-next-line max-len
    valueStyle: "font-size:" + Ut(o + "") + "px;color:" + Ut(a) + ";font-weight:" + Ut(s + "")
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
var XM = [0, 10, 20, 30], qM = ["", `
`, `

`, `


`];
function ba(e, t) {
  return t.type = e, t;
}
function Nf(e) {
  return e.type === "section";
}
function H_(e) {
  return Nf(e) ? $M : ZM;
}
function V_(e) {
  if (Nf(e)) {
    var t = 0, r = e.blocks.length, n = r > 1 || r > 0 && !e.noHeader;
    return T(e.blocks, function(i) {
      var a = V_(i);
      a >= t && (t = a + +(n && // 0 always can not be readable gap level.
      (!a || Nf(i) && !i.noHeader)));
    }), t;
  }
  return 0;
}
function $M(e, t, r, n) {
  var i = t.noHeader, a = KM(V_(t)), o = [], s = t.blocks || [];
  We(!s || B(s)), s = s || [];
  var u = e.orderMode;
  if (t.sortBlocks && u) {
    s = s.slice();
    var l = {
      valueAsc: "asc",
      valueDesc: "desc"
    };
    if (Xt(l, u)) {
      var f = new AM(l[u], null);
      s.sort(function(y, p) {
        return f.evaluate(y.sortParam, p.sortParam);
      });
    } else u === "seriesDesc" && s.reverse();
  }
  T(s, function(y, p) {
    var g = t.valueFormatter, m = H_(y)(
      // Inherit valueFormatter
      g ? R(R({}, e), {
        valueFormatter: g
      }) : e,
      y,
      p > 0 ? a.html : 0,
      n
    );
    m != null && o.push(m);
  });
  var h = e.renderMode === "richText" ? o.join(a.richText) : Ff(n, o.join(""), i ? r : a.html);
  if (i)
    return h;
  var v = Of(t.header, "ordinal", e.useUTC), c = z_(n, e.renderMode).nameStyle, d = F_(n);
  return e.renderMode === "richText" ? G_(e, v, c) + a.richText + h : Ff(n, '<div style="' + c + ";" + d + ';">' + Ut(v) + "</div>" + h, r);
}
function ZM(e, t, r, n) {
  var i = e.renderMode, a = t.noName, o = t.noValue, s = !t.markerType, u = t.name, l = e.useUTC, f = t.valueFormatter || e.valueFormatter || function(S) {
    return S = B(S) ? S : [S], H(S, function(b, w) {
      return Of(b, B(c) ? c[w] : c, l);
    });
  };
  if (!(a && o)) {
    var h = s ? "" : e.markupStyleCreator.makeTooltipMarker(t.markerType, t.markerColor || j.color.secondary, i), v = a ? "" : Of(u, "ordinal", l), c = t.valueType, d = o ? [] : f(t.value, t.rawDataIndex), y = !s || !a, p = !s && a, g = z_(n, i), m = g.nameStyle, _ = g.valueStyle;
    return i === "richText" ? (s ? "" : h) + (a ? "" : G_(e, v, m)) + (o ? "" : JM(e, d, y, p, _)) : Ff(n, (s ? "" : h) + (a ? "" : jM(v, !s, m)) + (o ? "" : QM(d, y, p, _)), r);
  }
}
function Kd(e, t, r, n, i, a) {
  if (e) {
    var o = H_(e), s = {
      useUTC: i,
      renderMode: r,
      orderMode: n,
      markupStyleCreator: t,
      valueFormatter: e.valueFormatter
    };
    return o(s, e, 0, a);
  }
}
function KM(e) {
  return {
    html: XM[e],
    richText: qM[e]
  };
}
function Ff(e, t, r) {
  var n = '<div style="clear:both"></div>', i = "margin: " + r + "px 0 0", a = F_(e);
  return '<div style="' + i + ";" + a + ';">' + t + n + "</div>";
}
function jM(e, t, r) {
  var n = t ? "margin-left:2px" : "";
  return '<span style="' + r + ";" + n + '">' + Ut(e) + "</span>";
}
function QM(e, t, r, n) {
  var i = r ? "10px" : "20px", a = t ? "float:right;margin-left:" + i : "";
  return e = B(e) ? e : [e], '<span style="' + a + ";" + n + '">' + H(e, function(o) {
    return Ut(o);
  }).join("&nbsp;&nbsp;") + "</span>";
}
function G_(e, t, r) {
  return e.markupStyleCreator.wrapRichTextStyle(t, r);
}
function JM(e, t, r, n, i) {
  var a = [i], o = n ? 10 : 20;
  return r && a.push({
    padding: [0, 0, 0, o],
    align: "right"
  }), e.markupStyleCreator.wrapRichTextStyle(B(t) ? t.join("  ") : t, a);
}
function tD(e, t) {
  var r = e.getData().getItemVisual(t, "style"), n = r[e.visualDrawType];
  return ma(n);
}
function U_(e, t) {
  var r = e.get("padding");
  return r ?? (t === "richText" ? [8, 10] : 10);
}
var bl = (
  /** @class */
  (function() {
    function e() {
      this.richTextStyles = {}, this._nextStyleNameId = Th();
    }
    return e.prototype._generateStyleName = function() {
      return "__EC_aUTo_" + this._nextStyleNameId++;
    }, e.prototype.makeTooltipMarker = function(t, r, n) {
      var i = n === "richText" ? this._generateStyleName() : null, a = g_({
        color: r,
        type: t,
        renderMode: n,
        markerId: i
      });
      return N(a) ? a : (this.richTextStyles[i] = a.style, a.content);
    }, e.prototype.wrapRichTextStyle = function(t, r) {
      var n = {};
      B(r) ? T(r, function(a) {
        return R(n, a);
      }) : R(n, r);
      var i = this._generateStyleName();
      return this.richTextStyles[i] = n, "{" + i + "|" + t + "}";
    }, e;
  })()
);
function eD(e) {
  var t = e.series, r = e.dataIndex, n = e.multipleSeries, i = t.getData(), a = i.mapDimensionsAll("defaultedTooltip"), o = a.length, s = t.getRawValue(r), u = B(s), l = tD(t, r), f, h, v, c;
  if (o > 1 || u && !o) {
    var d = rD(s, t, r, a, l);
    f = d.inlineValues, h = d.inlineValueTypes, v = d.blocks, c = d.inlineValues[0];
  } else if (o) {
    var y = i.getDimensionInfo(a[0]);
    c = f = _s(i, r, a[0]), h = y.type;
  } else
    c = f = u ? s[0] : s;
  var p = Ch(t), g = p && t.name || "", m = i.getName(r), _ = n ? g : m;
  return ba("section", {
    header: g,
    // When series name is not specified, do not show a header line with only '-'.
    // This case always happens in tooltip.trigger: 'item'.
    noHeader: n || !p,
    sortParam: c,
    blocks: [ba("nameValue", {
      markerType: "item",
      markerColor: l,
      // Do not mix display seriesName and itemName in one tooltip,
      // which might confuses users.
      name: _,
      // name dimension might be auto assigned, where the name might
      // be not readable. So we check trim here.
      noName: !Me(_),
      value: f,
      valueType: h,
      rawDataIndex: i.getRawIndex(r)
    })].concat(v || [])
  });
}
function rD(e, t, r, n, i) {
  var a = t.getData(), o = Ye(e, function(h, v, c) {
    var d = a.getDimensionInfo(c);
    return h = h || d && d.tooltip !== !1 && d.displayName != null;
  }, !1), s = [], u = [], l = [];
  n.length ? T(n, function(h) {
    f(_s(a, r, h), h);
  }) : T(e, f);
  function f(h, v) {
    var c = a.getDimensionInfo(v);
    !c || c.otherDims.tooltip === !1 || (o ? l.push(ba("nameValue", {
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
var yr = dt();
function co(e, t) {
  return e.getName(t) || e.getId(t);
}
var nD = "__universalTransitionEnabled", Xe = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r._selectedDataIndicesMap = {}, r;
    }
    return t.prototype.init = function(r, n, i) {
      this.seriesIndex = this.componentIndex, this.dataTask = aa({
        count: aD,
        reset: oD
      }), this.dataTask.context = {
        model: this
      }, this.mergeDefaultAndTheme(r, i);
      var a = yr(this).sourceManager = new N_(this);
      a.prepareSource();
      var o = this.getInitialData(r, i);
      Qd(o, this), this.dataTask.context.data = o, yr(this).dataBeforeProcessed = o, jd(this), this._initSelectedMapFromData(o);
    }, t.prototype.mergeDefaultAndTheme = function(r, n) {
      var i = ms(this), a = i ? sc(r) : {}, o = this.subType;
      at.hasClass(o) && (o += "Series"), pt(r, n.getTheme().get(this.subType)), pt(r, this.getDefaultOption()), Rv(r, "label", ["show"]), this.fillDataTextStyle(r.data), i && _a(r, a, i);
    }, t.prototype.mergeOption = function(r, n) {
      r = pt(this.option, r, !0), this.fillDataTextStyle(r.data);
      var i = ms(this);
      i && _a(this.option, r, i);
      var a = yr(this).sourceManager;
      a.dirty(), a.prepareSource();
      var o = this.getInitialData(r, n);
      Qd(o, this), this.dataTask.dirty(), this.dataTask.context.data = o, yr(this).dataBeforeProcessed = o, jd(this), this._initSelectedMapFromData(o);
    }, t.prototype.fillDataTextStyle = function(r) {
      if (r && !Zt(r))
        for (var n = ["show"], i = 0; i < r.length; i++)
          r[i] && r[i].label && Rv(r[i], "label", n);
    }, t.prototype.getInitialData = function(r, n) {
    }, t.prototype.appendData = function(r) {
      var n = this.getRawData();
      n.appendData(r.data);
    }, t.prototype.getData = function(r) {
      var n = zf(this);
      if (n) {
        var i = n.context.data;
        return r == null || !i.getLinkedData ? i : i.getLinkedData(r);
      } else
        return yr(this).data;
    }, t.prototype.getAllData = function() {
      var r = this.getData();
      return r && r.getLinkedDataAll ? r.getLinkedDataAll() : [{
        data: r
      }];
    }, t.prototype.setData = function(r) {
      var n = zf(this);
      if (n) {
        var i = n.context;
        i.outputData = r, n !== this.dataTask && (i.data = r);
      }
      yr(this).data = r;
    }, t.prototype.getEncode = function() {
      var r = this.get("encode", !0);
      if (r)
        return U(r);
    }, t.prototype.getSourceManager = function() {
      return yr(this).sourceManager;
    }, t.prototype.getSource = function() {
      return this.getSourceManager().getSource();
    }, t.prototype.getRawData = function() {
      return yr(this).dataBeforeProcessed;
    }, t.prototype.getColorBy = function() {
      var r = this.get("colorBy");
      return r || "series";
    }, t.prototype.isColorBySeries = function() {
      return this.getColorBy() === "series";
    }, t.prototype.getBaseAxis = function() {
      var r = this.coordinateSystem;
      return r && r.getBaseAxis && r.getBaseAxis();
    }, t.prototype.indicesOfNearest = function(r, n, i, a) {
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
    }, t.prototype.formatTooltip = function(r, n, i) {
      return eD({
        series: this,
        dataIndex: r,
        multipleSeries: n
      });
    }, t.prototype.isAnimationEnabled = function() {
      var r = this.ecModel;
      if (W.node && !(r && r.ssr))
        return !1;
      var n = this.getShallow("animation");
      return n && this.getData().count() > this.getShallow("animationThreshold") && (n = !1), !!n;
    }, t.prototype.restoreData = function() {
      this.dataTask.dirty();
    }, t.prototype.getColorFromPalette = function(r, n, i) {
      var a = this.ecModel, o = uc.prototype.getColorFromPalette.call(this, r, n, i);
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
          var u = r[s], l = co(o, u);
          i[l] = !1, this._selectedDataIndicesMap[l] = -1;
        }
      }
    }, t.prototype.toggleSelect = function(r, n) {
      for (var i = [], a = 0; a < r.length; a++)
        i[0] = r[a], this.isSelected(r[a], n) ? this.unselect(i, n) : this.select(i, n);
    }, t.prototype.getSelectedDataIndices = function() {
      if (this.option.selectedMap === "all")
        return [].slice.call(this.getData().getIndices());
      for (var r = this._selectedDataIndicesMap, n = mt(r), i = [], a = 0; a < n.length; a++) {
        var o = r[n[a]];
        o >= 0 && i.push(o);
      }
      return i;
    }, t.prototype.isSelected = function(r, n) {
      var i = this.option.selectedMap;
      if (!i)
        return !1;
      var a = this.getData(n);
      return (i === "all" || i[co(a, r)]) && !a.getItemModel(r).get(["select", "disabled"]);
    }, t.prototype.isUniversalTransitionEnabled = function() {
      if (this[nD])
        return !0;
      var r = this.option.universalTransition;
      return r ? r === !0 ? !0 : r && r.enabled : !1;
    }, t.prototype._innerSelect = function(r, n) {
      var i, a, o = this.option, s = o.selectedMode, u = n.length;
      if (!(!s || !u)) {
        if (s === "series")
          o.selectedMap = "all";
        else if (s === "multiple") {
          F(o.selectedMap) || (o.selectedMap = {});
          for (var l = o.selectedMap, f = 0; f < u; f++) {
            var h = n[f], v = co(r, h);
            l[v] = !0, this._selectedDataIndicesMap[v] = r.getRawIndex(h);
          }
        } else if (s === "single" || s === !0) {
          var c = n[u - 1], v = co(r, c);
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
      return at.registerClass(r);
    }, t.protoInitialize = (function() {
      var r = t.prototype;
      r.type = "series.__base__", r.seriesIndex = 0, r.ignoreStyleOnData = !1, r.hasSymbolVisual = !1, r.defaultSymbol = "circle", r.visualStyleAccessPath = "itemStyle", r.visualDrawType = "fill";
    })(), t;
  })(at)
);
Ae(Xe, xM);
Ae(Xe, uc);
em(Xe, at);
function jd(e) {
  var t = e.name;
  Ch(e) || (e.name = iD(e) || t);
}
function iD(e) {
  var t = e.getRawData(), r = t.mapDimensionsAll("seriesName"), n = [];
  return T(r, function(i) {
    var a = t.getDimensionInfo(i);
    a.displayName && n.push(a.displayName);
  }), n.join(" ");
}
function aD(e) {
  return e.model.getRawData().count();
}
function oD(e) {
  var t = e.model;
  return t.setData(t.getRawData().cloneShallow()), sD;
}
function sD(e, t) {
  t.outputData && e.end > t.outputData.count() && t.model.getRawData().cloneShallow(t.outputData);
}
function Qd(e, t) {
  T(fy(e.CHANGABLE_METHODS, e.DOWNSAMPLE_METHODS), function(r) {
    e.wrapMethod(r, Tt(uD, t));
  });
}
function uD(e, t) {
  var r = zf(e);
  return r && r.setOutputEnd((t || this).count()), t;
}
function zf(e) {
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
var se = (
  /** @class */
  (function() {
    function e() {
      this.group = new oe(), this.uid = fu("viewComponent");
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
Ih(se);
Zs(se);
function lD() {
  var e = dt();
  return function(t) {
    var r = e(t), n = t.pipelineContext, i = !!r.large, a = !!r.progressiveRender, o = r.large = !!(n && n.large), s = r.progressiveRender = !!(n && n.progressiveRender);
    return (i !== o || a !== s) && "reset";
  };
}
var Y_ = dt(), fD = lD(), Ue = (
  /** @class */
  (function() {
    function e() {
      this.group = new oe(), this.uid = fu("viewChart"), this.renderTask = aa({
        plan: hD,
        reset: cD
      }), this.renderTask.context = {
        view: this
      };
    }
    return e.prototype.init = function(t, r) {
    }, e.prototype.render = function(t, r, n, i) {
    }, e.prototype.highlight = function(t, r, n, i) {
      var a = t.getData(i && i.dataType);
      a && tp(a, i, "emphasis");
    }, e.prototype.downplay = function(t, r, n, i) {
      var a = t.getData(i && i.dataType);
      a && tp(a, i, "normal");
    }, e.prototype.remove = function(t, r) {
      this.group.removeAll();
    }, e.prototype.dispose = function(t, r) {
    }, e.prototype.updateView = function(t, r, n, i) {
      this.render(t, r, n, i);
    }, e.prototype.updateVisual = function(t, r, n, i) {
      this.render(t, r, n, i);
    }, e.prototype.eachRendered = function(t) {
      t_(this.group, t);
    }, e.markUpdateMethod = function(t, r) {
      Y_(t).updateMethod = r;
    }, e.protoInitialize = (function() {
      var t = e.prototype;
      t.type = "chart";
    })(), e;
  })()
);
function Jd(e, t, r) {
  e && Df(e) && (t === "emphasis" ? Tf : Cf)(e, r);
}
function tp(e, t, r) {
  var n = Ea(e, t), i = t && t.highlightKey != null ? iC(t.highlightKey) : null;
  n != null ? T(qt(n), function(a) {
    Jd(e.getItemGraphicEl(a), r, i);
  }) : e.eachItemGraphicEl(function(a) {
    Jd(a, r, i);
  });
}
Ih(Ue);
Zs(Ue);
function hD(e) {
  return fD(e.model);
}
function cD(e) {
  var t = e.model, r = e.ecModel, n = e.api, i = e.payload, a = t.pipelineContext.progressiveRender, o = e.view, s = i && Y_(i).updateMethod, u = a ? "incrementalPrepareRender" : s && o[s] ? s : "render";
  return u !== "render" && o[u](t, r, n, i), vD[u];
}
var vD = {
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
}, Ss = "\0__throttleOriginMethod", ep = "\0__throttleRate", rp = "\0__throttleType";
function vc(e, t, r) {
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
function W_(e, t, r, n) {
  var i = e[t];
  if (i) {
    var a = i[Ss] || i, o = i[rp], s = i[ep];
    if (s !== r || o !== n) {
      if (r == null || !n)
        return e[t] = a;
      i = e[t] = vc(a, r, n === "debounce"), i[Ss] = a, i[rp] = n, i[ep] = r;
    }
    return i;
  }
}
function Hf(e, t) {
  var r = e[t];
  r && r[Ss] && (r.clear && r.clear(), e[t] = r[Ss]);
}
var np = dt(), ip = {
  itemStyle: ga(o_, !0),
  lineStyle: ga(a_, !0)
}, dD = {
  lineStyle: "stroke",
  itemStyle: "fill"
};
function X_(e, t) {
  var r = e.visualStyleMapper || ip[t];
  return r || (console.warn("Unknown style type '" + t + "'."), ip.itemStyle);
}
function q_(e, t) {
  var r = e.visualDrawType || dD[t];
  return r || (console.warn("Unknown style type '" + t + "'."), "fill");
}
var pD = {
  createOnAllSeries: !0,
  performRawSeries: !0,
  reset: function(e, t) {
    var r = e.getData(), n = e.visualStyleAccessPath || "itemStyle", i = e.getModel(n), a = X_(e, n), o = a(i), s = i.getShallow("decal");
    s && (r.setVisual("decal", s), s.dirty = !0);
    var u = q_(e, n), l = o[u], f = X(l) ? l : null, h = o.fill === "auto" || o.stroke === "auto";
    if (!o[u] || f || h) {
      var v = e.getColorFromPalette(
        // TODO series count changed.
        e.name,
        null,
        t.getSeriesCount()
      );
      o[u] || (o[u] = v, r.setVisual("colorFromPalette", !0)), o.fill = o.fill === "auto" || X(o.fill) ? v : o.fill, o.stroke = o.stroke === "auto" || X(o.stroke) ? v : o.stroke;
    }
    if (r.setVisual("style", o), r.setVisual("drawType", u), !t.isSeriesFiltered(e) && f)
      return r.setVisual("colorFromPalette", !1), {
        dataEach: function(c, d) {
          var y = e.getDataParams(d), p = R({}, o);
          p[u] = f(y), c.setItemVisual(d, "style", p);
        }
      };
  }
}, Ri = new ct(), gD = {
  createOnAllSeries: !0,
  reset: function(e, t) {
    if (!e.ignoreStyleOnData) {
      var r = e.getData(), n = e.visualStyleAccessPath || "itemStyle", i = X_(e, n), a = r.getVisual("drawType");
      return {
        dataEach: r.hasItemOption ? function(o, s) {
          var u = o.getRawDataItem(s);
          if (u && u[n]) {
            Ri.option = u[n];
            var l = i(Ri), f = o.ensureUniqueItemVisual(s, "style");
            R(f, l), Ri.option.decal && (o.setItemVisual(s, "decal", Ri.option.decal), Ri.option.decal.dirty = !0), a in l && o.setItemVisual(s, "colorFromPalette", !1);
          }
        } : null
      };
    }
  }
}, yD = {
  performRawSeries: !0,
  overallReset: function(e) {
    var t = U();
    e.eachSeries(function(r) {
      if (!r.isColorBySeries()) {
        var n = r.type + "-" + r.getColorBy();
        np(r).scope = t.get(n) || t.set(n, {});
      }
    }), e.eachSeries(function(r) {
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
}, vo = Math.PI;
function mD(e, t) {
  t = t || {}, _t(t, {
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
  var r = new oe(), n = new Rt({
    style: {
      fill: t.maskColor
    },
    zlevel: t.zlevel,
    z: 1e4
  });
  r.add(n);
  var i = new Kt({
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
  }), a = new Rt({
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
  return t.showSpinner && (o = new Ba({
    shape: {
      startAngle: -vo / 2,
      endAngle: -vo / 2 + 0.1,
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
    endAngle: vo * 3 / 2
  }).start("circularInOut"), o.animateShape(!0).when(1e3, {
    startAngle: vo * 3 / 2
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
var $_ = (
  /** @class */
  (function() {
    function e(t, r, n, i) {
      this._stageTaskMap = U(), this.ecInstance = t, this.api = r, n = this._dataProcessorHandlers = n.slice(), i = this._visualHandlers = i.slice(), this._allHandlers = n.concat(i);
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
      var n = this._pipelineMap.get(t.uid), i = t.__preparePipelineContext ? t.__preparePipelineContext(r, n) : Nw(t, r, n);
      t.pipelineContext = n.context = i;
    }, e.prototype.restorePipelines = function(t, r) {
      var n = this, i = n._pipelineMap = U();
      r.eachSeries(function(a) {
        var o = t.painter.type === "canvas" && a.getProgressive(), s = a.uid;
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
    }, e.prototype.prepareStageTasks = function() {
      var t = this._stageTaskMap, r = this.api.getModel(), n = this.api;
      T(this._allHandlers, function(i) {
        var a = t.get(i.uid) || t.set(i.uid, {}), o = "";
        We(!(i.reset && i.overallReset), o), i.reset && this._createSeriesStageTask(i, a, r, n), i.overallReset && this._createOverallStageTask(i, a, r, n);
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
      T(t, function(u, l) {
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
      var a = this, o = r.seriesTaskMap, s = r.seriesTaskMap = U(), u = t.seriesType, l = t.getTargetSeries;
      t.createOnAllSeries ? n.eachRawSeries(f) : u ? n.eachRawSeriesByType(u, f) : l && l(n, i).each(f);
      function f(h) {
        var v = h.uid, c = s.set(v, o && o.get(v) || aa({
          plan: TD,
          reset: CD,
          count: MD
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
      var a = this, o = r.overallTask = r.overallTask || aa({
        reset: _D
      });
      o.context = {
        ecModel: n,
        api: i,
        overallReset: t.overallReset,
        scheduler: a
      };
      var s = o.agentStubMap, u = o.agentStubMap = U(), l = t.seriesType, f = t.getTargetSeries, h = t.dirtyOnOverallProgress, v = !1, c = "";
      We(!t.createOnAllSeries, c), l ? n.eachRawSeriesByType(l, d) : f ? f(n, i).each(d) : T(n.getSeries(), d);
      function d(y) {
        var p = y.uid, g = u.set(p, s && s.get(p) || // When the result of `getTargetSeries` changed, the overallTask
        // should be set as dirty and re-performed.
        (v = !0, aa({
          reset: SD,
          onDirty: wD
        })));
        g.context = {
          model: y,
          dirtyOnOverallProgress: h
          // FIXME:TS never used, so comment it
          // modifyOutputEnd: modifyOutputEnd
        }, g.agent = o, g.__block = h, a._pipe(y, g);
      }
      v && o.dirty();
    }, e.prototype._pipe = function(t, r) {
      var n = t.uid, i = this._pipelineMap.get(n);
      !i.head && (i.head = r), i.tail && i.tail.pipe(r), i.tail = r, r.__idxInPipeline = i.count++, r.__pipeline = i;
    }, e.wrapStageHandler = function(t, r) {
      return X(t) && (t = {
        overallReset: t,
        seriesType: DD(t)
      }), t.uid = fu("stageHandler"), r && (t.visualType = r), t;
    }, e;
  })()
);
function _D(e) {
  e.overallReset(e.ecModel, e.api, e.payload);
}
function SD(e) {
  return e.dirtyOnOverallProgress && bD;
}
function bD() {
  this.agent.dirty(), this.getDownstream().dirty();
}
function wD() {
  this.agent && this.agent.dirty();
}
function TD(e) {
  return e.plan ? e.plan(e.model, e.ecModel, e.api, e.payload) : null;
}
function CD(e) {
  e.useClearVisual && e.data.clearAllVisual();
  var t = e.resetDefines = qt(e.reset(e.model, e.ecModel, e.api, e.payload));
  return t.length > 1 ? H(t, function(r, n) {
    return Z_(n);
  }) : xD;
}
var xD = Z_(0);
function Z_(e) {
  return function(t, r) {
    var n = r.data, i = r.resetDefines[e];
    if (i && i.dataEach)
      for (var a = t.start; a < t.end; a++)
        i.dataEach(n, a);
    else i && i.progress && i.progress(t, n);
  };
}
function MD(e) {
  return e.data.count();
}
function DD(e) {
  bs = null;
  try {
    e(wa, K_);
  } catch {
  }
  return bs;
}
var wa = {}, K_ = {}, bs;
j_(wa, lc);
j_(K_, pm);
wa.eachSeriesByType = wa.eachRawSeriesByType = function(e) {
  bs = e;
};
wa.eachComponent = function(e) {
  e.mainType === "series" && e.subType && (bs = e.subType);
};
function j_(e, t) {
  for (var r in t.prototype)
    e[r] = Et;
}
var E = j.darkColor, ap = E.background, Oi = function() {
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
  timeAxis: Oi(),
  logAxis: Oi(),
  valueAxis: Oi(),
  categoryAxis: Oi(),
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
    var e = Oi();
    return e.axisName = {
      color: E.axisLabel
    }, e.axisLine.lineStyle.color = E.neutral20, e;
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
var ID = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.normalizeQuery = function(t) {
      var r = {}, n = {}, i = {};
      if (N(t)) {
        var a = ze(t);
        r.mainType = a.main || null, r.subType = a.sub || null;
      } else {
        var o = ["Index", "Name", "Id"], s = {
          name: 1,
          dataIndex: 1,
          dataType: 1
        };
        T(t, function(u, l) {
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
), Vf = ["symbol", "symbolSize", "symbolRotate", "symbolOffset"], sp = Vf.concat(["symbolKeepAspect"]), AD = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(e, t) {
    var r = e.getData();
    if (e.legendIcon && r.setVisual("legendIcon", e.legendIcon), !e.hasSymbolVisual)
      return;
    for (var n = {}, i = {}, a = !1, o = 0; o < Vf.length; o++) {
      var s = Vf[o], u = e.get(s);
      X(u) ? (a = !0, i[s] = u) : n[s] = u;
    }
    if (n.symbol = n.symbol || e.defaultSymbol, r.setVisual(R({
      legendIcon: e.legendIcon || n.symbol,
      symbolKeepAspect: e.get("symbolKeepAspect")
    }, n)), t.isSeriesFiltered(e))
      return;
    var l = mt(i);
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
}, PD = {
  createOnAllSeries: !0,
  // For legend.
  performRawSeries: !0,
  reset: function(e, t) {
    if (!e.hasSymbolVisual || t.isSeriesFiltered(e))
      return;
    var r = e.getData();
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
function LD(e, t, r) {
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
function ED(e, t) {
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
function Vn(e, t, r, n, i) {
  var a = e + t;
  r.isSilent(a) || n.eachComponent({
    mainType: "series",
    subType: "pie"
  }, function(o) {
    for (var s = o.seriesIndex, u = o.option.selectedMap, l = i.selected, f = 0; f < l.length; f++)
      if (l[f].seriesIndex === s) {
        var h = o.getData(), v = Ea(h, i.fromActionPayload);
        r.trigger(a, {
          type: a,
          seriesId: o.id,
          name: B(v) ? h.getName(v[0]) : h.getName(v),
          selected: N(u) ? u : R({}, u)
        });
      }
  });
}
function RD(e, t, r) {
  e.on("selectchanged", function(n) {
    var i = r.getModel();
    n.isFromClick ? (Vn("map", "selectchanged", t, i, n), Vn("pie", "selectchanged", t, i, n)) : n.fromAction === "select" ? (Vn("map", "selected", t, i, n), Vn("pie", "selected", t, i, n)) : n.fromAction === "unselect" && (Vn("map", "unselected", t, i, n), Vn("pie", "unselected", t, i, n));
  });
}
function Ki(e, t, r) {
  for (var n; e && !(t(e) && (n = e, r)); )
    e = e.__hostTarget || e.parent;
  return n;
}
var he = new qe(), J_ = {};
function OD(e, t) {
  J_[e] = t;
}
function t0(e) {
  return J_[e];
}
var du = dt();
function kD(e) {
  du(e).prepare = {};
}
function BD(e) {
  du(e).fullUpdate = {};
}
function ND(e) {
  return du(e).prepare;
}
function gi(e) {
  return du(e).fullUpdate;
}
var FD = Math.round(Math.random() * 9), zD = typeof Object.defineProperty == "function", HD = (function() {
  function e() {
    this._id = "__ec_inner_" + FD++;
  }
  return e.prototype.get = function(t) {
    return this._guard(t)[this._id];
  }, e.prototype.set = function(t, r) {
    var n = this._guard(t);
    return zD ? Object.defineProperty(n, this._id, {
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
})(), VD = yt.extend({
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
}), GD = yt.extend({
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
}), UD = yt.extend({
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
}), YD = yt.extend({
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
}), WD = {
  line: An,
  rect: Rt,
  roundRect: Rt,
  square: Rt,
  circle: ka,
  diamond: GD,
  pin: UD,
  arrow: YD,
  triangle: VD
}, XD = {
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
}, Gf = {};
T(WD, function(e, t) {
  Gf[t] = new e();
});
var qD = yt.extend({
  type: "symbol",
  shape: {
    symbolType: "",
    x: 0,
    y: 0,
    width: 0,
    height: 0
  },
  calculateTextPosition: function(e, t, r) {
    var n = Hy(e, t, r), i = this.shape;
    return i && i.symbolType === "pin" && t.position === "inside" && (n.y = r.y + r.height * 0.4), n;
  },
  buildPath: function(e, t, r) {
    var n = t.symbolType;
    if (n !== "none") {
      var i = Gf[n];
      i || (n = "rect", i = Gf[n]), XD[n](t.x, t.y, t.width, t.height, i.shape), i.buildPath(e, i.shape, r);
    }
  }
});
function $D(e, t) {
  if (this.type !== "image") {
    var r = this.style;
    this.__isEmptyBrush ? (r.stroke = e, r.fill = t || j.color.neutral00, r.lineWidth = 2) : this.shape.symbolType === "line" ? r.stroke = e : r.fill = e, this.markRedraw();
  }
}
function pu(e, t, r, n, i, a, o) {
  var s = e.indexOf("empty") === 0;
  s && (e = e.substr(5, 1).toLowerCase() + e.substr(6));
  var u;
  return e.indexOf("image://") === 0 ? u = Fh(e.slice(8), new Y(t, r, n, i), o ? "center" : "cover") : e.indexOf("path://") === 0 ? u = ou(e.slice(7), {}, new Y(t, r, n, i), o ? "center" : "cover") : u = new qD({
    shape: {
      symbolType: e,
      x: t,
      y: r,
      width: n,
      height: i
    }
  }), u.__isEmptyBrush = s, u.setColor = $D, a && u.setColor(a), u;
}
function ZD(e, t) {
  if (e != null)
    return B(e) || (e = [e, e]), [xe(e[0], t[0]) || 0, xe(G(e[1], e[0]), t[1]) || 0];
}
function cn(e) {
  return isFinite(e);
}
function KD(e, t, r) {
  var n = t.x == null ? 0 : t.x, i = t.x2 == null ? 1 : t.x2, a = t.y == null ? 0 : t.y, o = t.y2 == null ? 0 : t.y2;
  t.global || (n = n * r.width + r.x, i = i * r.width + r.x, a = a * r.height + r.y, o = o * r.height + r.y), n = cn(n) ? n : 0, i = cn(i) ? i : 1, a = cn(a) ? a : 0, o = cn(o) ? o : 0;
  var s = e.createLinearGradient(n, a, i, o);
  return s;
}
function jD(e, t, r) {
  var n = r.width, i = r.height, a = Math.min(n, i), o = t.x == null ? 0.5 : t.x, s = t.y == null ? 0.5 : t.y, u = t.r == null ? 0.5 : t.r;
  t.global || (o = o * n + r.x, s = s * i + r.y, u = u * a), o = cn(o) ? o : 0.5, s = cn(s) ? s : 0.5, u = u >= 0 && cn(u) ? u : 0.5;
  var l = e.createRadialGradient(o, s, 0, o, s, u);
  return l;
}
function Uf(e, t, r) {
  for (var n = t.type === "radial" ? jD(e, t, r) : KD(e, t, r), i = t.colorStops, a = 0; a < i.length; a++)
    n.addColorStop(i[a].offset, i[a].color);
  return n;
}
function QD(e, t) {
  if (e === t || !e && !t)
    return !1;
  if (!e || !t || e.length !== t.length)
    return !0;
  for (var r = 0; r < e.length; r++)
    if (e[r] !== t[r])
      return !0;
  return !1;
}
function po(e) {
  return parseInt(e, 10);
}
function go(e, t, r) {
  var n = ["width", "height"][t], i = ["clientWidth", "clientHeight"][t], a = ["paddingLeft", "paddingTop"][t], o = ["paddingRight", "paddingBottom"][t];
  if (r[n] != null && r[n] !== "auto")
    return parseFloat(r[n]);
  var s = document.defaultView.getComputedStyle(e);
  return (e[i] || po(s[n]) || po(e.style[n])) - (po(s[a]) || 0) - (po(s[o]) || 0) || 0;
}
function JD(e, t) {
  return !e || e === "solid" || !(t > 0) ? null : e === "dashed" ? [4 * t, 2 * t] : e === "dotted" ? [t] : vt(e) ? [e] : B(e) ? e : null;
}
function e0(e) {
  var t = e.style, r = t.lineDash && t.lineWidth > 0 && JD(t.lineDash, t.lineWidth), n = t.lineDashOffset;
  if (r) {
    var i = t.strokeNoScale && e.getLineScale ? e.getLineScale() : 1;
    i && i !== 1 && (r = H(r, function(a) {
      return a / i;
    }), n /= i);
  }
  return [r, n];
}
var tI = new hi(!0);
function ws(e) {
  var t = e.stroke;
  return !(t == null || t === "none" || !(e.lineWidth > 0));
}
function up(e) {
  return typeof e == "string" && e !== "none";
}
function Ts(e) {
  var t = e.fill;
  return t != null && t !== "none";
}
function lp(e, t) {
  if (t.fillOpacity != null && t.fillOpacity !== 1) {
    var r = e.globalAlpha;
    e.globalAlpha = t.fillOpacity * t.opacity, e.fill(), e.globalAlpha = r;
  } else
    e.fill();
}
function fp(e, t) {
  if (t.strokeOpacity != null && t.strokeOpacity !== 1) {
    var r = e.globalAlpha;
    e.globalAlpha = t.strokeOpacity * t.opacity, e.stroke(), e.globalAlpha = r;
  } else
    e.stroke();
}
function Yf(e, t, r) {
  var n = rm(t.image, t.__image, r);
  if (Ks(n)) {
    var i = e.createPattern(n, t.repeat || "repeat");
    if (typeof DOMMatrix == "function" && i && i.setTransform) {
      var a = new DOMMatrix();
      a.translateSelf(t.x || 0, t.y || 0), a.rotateSelf(0, 0, (t.rotation || 0) * hy), a.scaleSelf(t.scaleX || 1, t.scaleY || 1), i.setTransform(a);
    }
    return i;
  }
}
function eI(e, t, r, n, i) {
  var a, o = ws(r), s = Ts(r), u = r.strokePercent, l = u < 1, f = !t.path;
  (!t.silent || l) && f && t.createPathProxy();
  var h = t.path || tI, v = t.__dirty;
  if (!n) {
    var c = r.fill, d = r.stroke, y = s && !!c.colorStops, p = o && !!d.colorStops, g = s && !!c.image, m = o && !!d.image, _ = void 0, S = void 0, b = void 0, w = void 0, C = void 0;
    (y || p) && (C = t.getBoundingRect()), y && (_ = v ? Uf(e, c, C) : t.__canvasFillGradient, t.__canvasFillGradient = _), p && (S = v ? Uf(e, d, C) : t.__canvasStrokeGradient, t.__canvasStrokeGradient = S), g && (b = v || !t.__canvasFillPattern ? Yf(e, c, t) : t.__canvasFillPattern, t.__canvasFillPattern = b), m && (w = v || !t.__canvasStrokePattern ? Yf(e, d, t) : t.__canvasStrokePattern, t.__canvasStrokePattern = w), y ? e.fillStyle = _ : g && (b ? e.fillStyle = b : s = !1), p ? e.strokeStyle = S : m && (w ? e.strokeStyle = w : o = !1);
  }
  var M = t.getGlobalScale();
  h.setScale(M[0], M[1], t.segmentIgnoreThreshold);
  var x, D;
  e.setLineDash && r.lineDash && (a = e0(t), x = a[0], D = a[1]);
  var I = !0;
  (f || v & qn) && (h.setDPR(e.dpr), l ? h.setContext(null) : (h.setContext(e), I = !1), h.reset(), t.buildPath(h, t.shape, n), h.toStatic(), t.pathUpdated()), I && h.rebuildPath(e, l ? u : 1), x && (e.setLineDash(x), e.lineDashOffset = D), n ? (i.batchFill = s, i.batchStroke = o) : r.strokeFirst ? (o && fp(e, r), s && lp(e, r)) : (s && lp(e, r), o && fp(e, r)), x && e.setLineDash([]);
}
function rI(e, t, r) {
  var n = t.__image = rm(r.image, t.__image, t, t.onload);
  if (!(!n || !Ks(n))) {
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
function nI(e, t, r) {
  var n, i = r.text;
  if (i != null && (i += ""), i) {
    e.font = r.font || Pr, e.textAlign = r.textAlign, e.textBaseline = r.textBaseline;
    var a = void 0, o = void 0;
    e.setLineDash && r.lineDash && (n = e0(t), a = n[0], o = n[1]), a && (e.setLineDash(a), e.lineDashOffset = o), r.strokeFirst ? (ws(r) && e.strokeText(i, r.x, r.y), Ts(r) && e.fillText(i, r.x, r.y)) : (Ts(r) && e.fillText(i, r.x, r.y), ws(r) && e.strokeText(i, r.x, r.y)), a && e.setLineDash([]);
  }
}
var hp = ["shadowBlur", "shadowOffsetX", "shadowOffsetY"], cp = [
  ["lineCap", "butt"],
  ["lineJoin", "miter"],
  ["miterLimit", 10]
];
function r0(e, t, r, n, i) {
  var a = !1;
  if (!n && (r = r || {}, t === r))
    return !1;
  if (n || t.opacity !== r.opacity) {
    Wt(e, i), a = !0;
    var o = Math.max(Math.min(t.opacity, 1), 0);
    e.globalAlpha = isNaN(o) ? mn.opacity : o;
  }
  (n || t.blend !== r.blend) && (a || (Wt(e, i), a = !0), e.globalCompositeOperation = t.blend || mn.blend);
  for (var s = 0; s < hp.length; s++) {
    var u = hp[s];
    (n || t[u] !== r[u]) && (a || (Wt(e, i), a = !0), e[u] = e.dpr * (t[u] || 0));
  }
  return (n || t.shadowColor !== r.shadowColor) && (a || (Wt(e, i), a = !0), e.shadowColor = t.shadowColor || mn.shadowColor), a;
}
function vp(e, t, r, n, i) {
  var a = t.style, o = n ? null : r && r.style || {};
  if (a === o)
    return !1;
  var s = r0(e, a, o, n, i);
  if ((n || a.fill !== o.fill) && (s || (Wt(e, i), s = !0), up(a.fill) && (e.fillStyle = a.fill)), (n || a.stroke !== o.stroke) && (s || (Wt(e, i), s = !0), up(a.stroke) && (e.strokeStyle = a.stroke)), (n || a.opacity !== o.opacity) && (s || (Wt(e, i), s = !0), e.globalAlpha = a.opacity == null ? 1 : a.opacity), t.hasStroke()) {
    var u = a.lineWidth, l = u / (a.strokeNoScale && t.getLineScale ? t.getLineScale() : 1);
    e.lineWidth !== l && (s || (Wt(e, i), s = !0), e.lineWidth = l);
  }
  for (var f = 0; f < cp.length; f++) {
    var h = cp[f], v = h[0];
    (n || a[v] !== o[v]) && (s || (Wt(e, i), s = !0), e[v] = a[v] || h[1]);
  }
  return s;
}
function iI(e, t, r, n, i) {
  return r0(e, t.style, r && r.style, n, i);
}
function n0(e, t) {
  var r = t.transform, n = e.dpr || 1;
  r ? e.setTransform(n * r[0], n * r[1], n * r[2], n * r[3], n * r[4], n * r[5]) : e.setTransform(n, 0, 0, n, 0, 0);
}
function aI(e, t, r) {
  for (var n = !1, i = 0; i < e.length; i++) {
    var a = e[i];
    n = n || a.isZeroArea(), n0(t, a), t.beginPath(), a.buildPath(t, a.shape), t.clip();
  }
  r.allClipped = n;
}
function oI(e, t) {
  return e && t ? e[0] !== t[0] || e[1] !== t[1] || e[2] !== t[2] || e[3] !== t[3] || e[4] !== t[4] || e[5] !== t[5] : !(!e && !t);
}
var dp = 1, pp = 2, gp = 3, yp = 4;
function sI(e) {
  var t = Ts(e), r = ws(e);
  return !(e.lineDash || !(+t ^ +r) || t && typeof e.fill != "string" || r && typeof e.stroke != "string" || e.strokePercent < 1 || e.strokeOpacity < 1 || e.fillOpacity < 1);
}
function Wt(e, t) {
  t.batchFill && (t.batchFill = !1, e.fill()), t.batchStroke && (t.batchStroke = !1, e.stroke());
}
function dc(e, t) {
  var r = { inHover: !1, viewWidth: 0, viewHeight: 0, beforeBrushParam: {} };
  vn(e, t, r), ni(e, r);
}
function vn(e, t, r) {
  var n = t.transform;
  if (!t.shouldBePainted(r.viewWidth, r.viewHeight, !1, !1)) {
    t.__dirty &= ~Qt, t.__isRendered = !1;
    return;
  }
  var i = t.__clipPaths, a = r.prevElClipPaths, o = t.style, s = !1, u = !1;
  if ((!a || QD(i, a)) && (a && (Wt(e, r), e.restore(), u = s = !0, r.prevElClipPaths = null, r.allClipped = !1, r.prevEl = null), i && i.length && (Wt(e, r), e.save(), aI(i, e, r), s = !0, r.prevElClipPaths = i)), r.allClipped) {
    t.__dirty &= ~Qt, t.__isRendered = !1;
    return;
  }
  t.beforeBrush && t.beforeBrush(r.beforeBrushParam), t.innerBeforeBrush();
  var l = r.prevEl;
  l || (u = s = !0);
  var f = t instanceof yt && t.autoBatch && sI(o);
  s || oI(n, l.transform) ? (Wt(e, r), n0(e, t)) : f || Wt(e, r), t instanceof yt ? (r.lastDrawType !== dp && (u = !0, r.lastDrawType = dp), vp(e, t, l, u, r), (!f || !r.batchFill && !r.batchStroke) && e.beginPath(), eI(e, t, o, f, r)) : t instanceof hs ? (r.lastDrawType !== gp && (u = !0, r.lastDrawType = gp), vp(e, t, l, u, r), nI(e, t, o)) : t instanceof kr ? (r.lastDrawType !== pp && (u = !0, r.lastDrawType = pp), iI(e, t, l, u, r), rI(e, t, o)) : t.getTemporalDisplayables && (r.lastDrawType !== yp && (u = !0, r.lastDrawType = yp), uI(e, t, r)), t.innerAfterBrush(), t.afterBrush && (f && Wt(e, r), t.afterBrush()), r.prevEl = t, t.__dirty = 0, t.__isRendered = !0;
}
function ni(e, t) {
  Wt(e, t), t.prevElClipPaths && e.restore();
}
function uI(e, t, r) {
  var n = t.getDisplayables(), i = t.getTemporalDisplayables();
  e.save();
  var a = {
    prevElClipPaths: null,
    prevEl: null,
    allClipped: !1,
    viewWidth: r.viewWidth,
    viewHeight: r.viewHeight,
    inHover: r.inHover,
    beforeBrushParam: {}
  }, o, s;
  for (o = t.getCursor(), s = n.length; o < s; o++) {
    var u = n[o];
    u.beforeBrush && u.beforeBrush(r.beforeBrushParam), u.innerBeforeBrush(), vn(e, u, a), u.innerAfterBrush(), u.afterBrush && u.afterBrush(), a.prevEl = u;
  }
  ni(e, a);
  for (var l = 0, f = i.length; l < f; l++) {
    var u = i[l];
    u.beforeBrush && u.beforeBrush(r.beforeBrushParam), u.innerBeforeBrush(), vn(e, u, a), u.innerAfterBrush(), u.afterBrush && u.afterBrush(), a.prevEl = u;
  }
  ni(e, a), t.clearTemporalDisplayables(), t.notClear = !0, e.restore();
}
var wl = new HD(), mp = new si(100), _p = ["symbol", "symbolSize", "symbolKeepAspect", "color", "backgroundColor", "dashArrayX", "dashArrayY", "maxTileWidth", "maxTileHeight"];
function Wf(e, t) {
  if (e === "none")
    return null;
  var r = t.getDevicePixelRatio(), n = t.getZr(), i = n.painter.type === "svg";
  e.dirty && wl.delete(e);
  var a = wl.get(e);
  if (a)
    return a;
  var o = _t(e, {
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
  return u(s), s.rotation = o.rotation, s.scaleX = s.scaleY = i ? 1 : 1 / r, wl.set(e, s), e.dirty = !1, s;
  function u(l) {
    for (var f = [r], h = !0, v = 0; v < _p.length; ++v) {
      var c = o[_p[v]];
      if (c != null && !B(c) && !N(c) && !vt(c) && typeof c != "boolean") {
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
    var p = a0(o.dashArrayX), g = lI(o.dashArrayY), m = i0(o.symbol), _ = fI(p), S = o0(g), b = !i && Lt.createCanvas(), w = i && {
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
          for (var V = k / 2 % m.length, K = 0, J = 0, tt = 0; K < C.width * 2; ) {
            for (var Q = 0, L = 0; L < p[O].length; ++L)
              Q += p[O][L];
            if (Q <= 0)
              break;
            if (J % 2 === 0) {
              var rt = (1 - o.symbolSize) * 0.5, nt = K + p[O][J] * rt, q = A + g[P] * rt, ot = p[O][J] * o.symbolSize, St = g[P] * o.symbolSize, te = tt / 2 % m[V].length;
              Ze(nt, q, ot, St, m[V][te]);
            }
            K += p[O][J], ++tt, ++J, J === p[O].length && (J = 0);
          }
          ++O, O === p.length && (O = 0);
        }
        A += g[P], ++k, ++P, P === g.length && (P = 0);
      }
      function Ze(ee, At, $, et, Nr) {
        var Ht = i ? 1 : r, Fc = pu(Nr, ee * Ht, At * Ht, $ * Ht, et * Ht, o.color, o.symbolKeepAspect);
        if (i) {
          var zc = n.painter.renderOneToVNode(Fc);
          zc && w.children.push(zc);
        } else
          dc(M, Fc);
      }
    }
  }
}
function i0(e) {
  if (!e || e.length === 0)
    return [["rect"]];
  if (N(e))
    return [[e]];
  for (var t = !0, r = 0; r < e.length; ++r)
    if (!N(e[r])) {
      t = !1;
      break;
    }
  if (t)
    return i0([e]);
  for (var n = [], r = 0; r < e.length; ++r)
    N(e[r]) ? n.push([e[r]]) : n.push(e[r]);
  return n;
}
function a0(e) {
  if (!e || e.length === 0)
    return [[0, 0]];
  if (vt(e)) {
    var t = Math.ceil(e);
    return [[t, t]];
  }
  for (var r = !0, n = 0; n < e.length; ++n)
    if (!vt(e[n])) {
      r = !1;
      break;
    }
  if (r)
    return a0([e]);
  for (var i = [], n = 0; n < e.length; ++n)
    if (vt(e[n])) {
      var t = Math.ceil(e[n]);
      i.push([t, t]);
    } else {
      var t = H(e[n], function(s) {
        return Math.ceil(s);
      });
      t.length % 2 === 1 ? i.push(t.concat(t)) : i.push(t);
    }
  return i;
}
function lI(e) {
  if (!e || typeof e == "object" && e.length === 0)
    return [0, 0];
  if (vt(e)) {
    var t = Math.ceil(e);
    return [t, t];
  }
  var r = H(e, function(n) {
    return Math.ceil(n);
  });
  return e.length % 2 ? r.concat(r) : r;
}
function fI(e) {
  return H(e, function(t) {
    return o0(t);
  });
}
function o0(e) {
  for (var t = 0, r = 0; r < e.length; ++r)
    t += e[r];
  return e.length % 2 === 1 ? t * 2 : t;
}
var hI = Dh(cI);
function cI(e, t) {
  e.eachRawSeries(function(r) {
    if (!e.isSeriesFiltered(r)) {
      var n = r.getData();
      n.hasItemVisual() && n.each(function(o) {
        var s = n.getItemVisual(o, "decal");
        if (s) {
          var u = n.ensureUniqueItemVisual(o, "style");
          u.decal = Wf(s, t);
        }
      });
      var i = n.getVisual("decal");
      if (i) {
        var a = n.getVisual("style");
        a.decal = Wf(i, t);
      }
    }
  });
}
var vI = "6.1.0", dI = {
  zrender: "6.1.0"
}, pI = 1, gI = 800, yI = 900, mI = 920, _I = 1e3, SI = 2e3, Sp = 5e3, s0 = 1e3, bI = 1100, pc = 2e3, u0 = 3e3, wI = 4e3, gu = 4500, TI = 4600, CI = 5e3, xI = 6e3, l0 = 7e3, f0 = {
  PROCESSOR: {
    SERIES_FILTER: gI,
    AXIS_STATISTICS: mI,
    FILTER: _I,
    STATISTIC: Sp,
    STATISTICS: Sp
  },
  VISUAL: {
    LAYOUT: s0,
    PROGRESSIVE_LAYOUT: bI,
    GLOBAL: pc,
    CHART: u0,
    POST_CHART_LAYOUT: TI,
    COMPONENT: wI,
    BRUSH: CI,
    CHART_ITEM: gu,
    ARIA: xI,
    DECAL: l0
  }
}, Ct = "__flagInMainProcess", yo = "__mainProcessVersion", Mt = "__pendingUpdate", Tl = "__needsUpdateStatus", bp = /^[a-zA-Z0-9_]+$/, Cl = "__connectUpdateStatus", wp = 0, MI = 1, DI = 2;
function h0(e) {
  return function() {
    for (var t = [], r = 0; r < arguments.length; r++)
      t[r] = arguments[r];
    if (this.isDisposed()) {
      this.id;
      return;
    }
    return v0(this, e, t);
  };
}
function c0(e) {
  return function() {
    for (var t = [], r = 0; r < arguments.length; r++)
      t[r] = arguments[r];
    return v0(this, e, t);
  };
}
function v0(e, t, r) {
  return r[0] = r[0] && r[0].toLowerCase(), qe.prototype[t].apply(e, r);
}
var d0 = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      return e !== null && e.apply(this, arguments) || this;
    }
    return t;
  })(qe)
), p0 = d0.prototype;
p0.on = c0("on");
p0.off = c0("off");
var on, xl, mo, er, _o, Ml, Dl, Gn, Un, Tp, Cp, Il, xp, So, Mp, g0, le, Dp, Yn, Cs = (
  /** @class */
  (function(e) {
    z(t, e);
    function t(r, n, i) {
      var a = e.call(this, new ID()) || this;
      a._chartsViews = [], a._chartsMap = {}, a._componentsViews = [], a._componentsMap = {}, a._pendingActions = [], i = i || {}, a.__v_skip = !0, a._dom = r;
      var o = "canvas", s = "auto", u = !1;
      a[yo] = 1, i.ssr && Uy(function(v) {
        var c = ft(v), d = c.dataIndex;
        if (d != null) {
          var y = U();
          return y.set("series_index", c.seriesIndex), y.set("data_index", d), c.ssrType && y.set("ssr_type", c.ssrType), y;
        }
      });
      var l = a._zr = pf(r, {
        renderer: i.renderer || o,
        devicePixelRatio: i.devicePixelRatio,
        width: i.width,
        height: i.height,
        ssr: i.ssr,
        useDirtyRect: G(i.useDirtyRect, u),
        useCoarsePointer: G(i.useCoarsePointer, s),
        pointerSize: i.pointerSize
      });
      a._ssr = i.ssr, a._throttledZrFlush = vc(ht(l.flush, l), 17), a._updateTheme(n), a._locale = vx(i.locale || s_), a._coordSysMgr = new cu();
      var f = a._api = Mp(a);
      function h(v, c) {
        return v.__prio - c.__prio;
      }
      return Eo(Ms, h), Eo($f, h), a._scheduler = new $_(a, f, $f, Ms), a._messageCenter = new d0(), a._initEvents(), a.resize = ht(a.resize, a), l.animation.on("frame", a._onframe, a), Tp(l, a), Cp(l, a), fa(a), a;
    }
    return t.prototype._onframe = function() {
      if (!this._disposed) {
        var r = this._scheduler, n = this._model, i = this._api;
        if (Dp(this), this[Mt]) {
          var a = this[Mt].silent;
          this[Ct] = !0, Yn(this);
          try {
            on(this), er.update.call(this, null, this[Mt].updateParams);
          } catch (u) {
            throw this[Ct] = !1, this[Mt] = null, u;
          }
          this._zr.flush(), this[Ct] = !1, this[Mt] = null, Gn.call(this, a), Un.call(this, a);
        } else if (r.unfinished) {
          var o = pI;
          do {
            r.unfinished = !1;
            var s = Lt.getTime();
            r.performSeriesTasks(n), r.performDataProcessorTasks(n), Ml(this, n), r.performVisualTasks(n), So(this, this._model, i, "remain", {}), o -= Lt.getTime() - s;
          } while (o > 0 && r.unfinished);
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
      if (!this[Ct]) {
        if (this._disposed) {
          this.id;
          return;
        }
        var a, o, s;
        if (F(n) && (i = n.lazyUpdate, a = n.silent, o = n.replaceMerge, s = n.transition, n = n.notMerge), this[Ct] = !0, Yn(this), !this._model || n) {
          var u = new nM(this._api), l = this._theme, f = this._model = new lc();
          f.scheduler = this._scheduler, f.ssr = this._ssr, f.init(null, null, null, l, this._locale, u);
        }
        this._model.setOption(r, {
          replaceMerge: o
        }, Zf);
        var h = {
          seriesTransition: s,
          optionChanged: !0
        };
        if (i)
          this[Mt] = {
            silent: a,
            updateParams: h
          }, this[Ct] = !1, this.getZr().wakeUp();
        else {
          try {
            on(this), er.update.call(this, null, h);
          } catch (v) {
            throw this[Mt] = null, this[Ct] = !1, v;
          }
          this._ssr || this._zr.flush(), this[Mt] = null, this[Ct] = !1, Gn.call(this, a), Un.call(this, a);
        }
      }
    }, t.prototype.setTheme = function(r, n) {
      if (!this[Ct]) {
        if (this._disposed) {
          this.id;
          return;
        }
        var i = this._model;
        if (i) {
          var a = n && n.silent, o = null;
          this[Mt] && (a == null && (a = this[Mt].silent), o = this[Mt].updateParams, this[Mt] = null), this[Ct] = !0, Yn(this);
          try {
            this._updateTheme(r), i.setTheme(this._theme), on(this), er.update.call(this, {
              type: "setTheme"
            }, o);
          } catch (s) {
            throw this[Ct] = !1, s;
          }
          this[Ct] = !1, Gn.call(this, a), Un.call(this, a);
        }
      }
    }, t.prototype._updateTheme = function(r) {
      N(r) && (r = y0[r]), r && (r = Z(r), r && x_(r, !0), this._theme = r);
    }, t.prototype.getModel = function() {
      return this._model;
    }, t.prototype.getOption = function() {
      return this._model && this._model.getOption();
    }, t.prototype.getWidth = function() {
      return this._zr.getWidth();
    }, t.prototype.getHeight = function() {
      return this._zr.getHeight();
    }, t.prototype.getDevicePixelRatio = function() {
      return this._zr.painter.dpr || W.hasGlobalWindow && window.devicePixelRatio || 1;
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
      var r = this._zr, n = r.storage.getDisplayList();
      return T(n, function(i) {
        i.stopAnimation(null, !0);
      }), r.painter.toDataURL();
    }, t.prototype.getDataURL = function(r) {
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
    }, t.prototype.getConnectedDataURL = function(r) {
      if (this._disposed) {
        this.id;
        return;
      }
      var n = r.type === "svg", i = this.group, a = Math.min, o = Math.max, s = 1 / 0;
      if (Ds[i]) {
        var u = s, l = s, f = -s, h = -s, v = [], c = r && r.pixelRatio || this.getDevicePixelRatio();
        T(bn, function(_, S) {
          if (_.group === i) {
            var b = n ? _.getZr().painter.getSvgDom().innerHTML : _.renderToCanvas(Z(r)), w = _.getDom().getBoundingClientRect();
            u = a(w.left, u), l = a(w.top, l), f = o(w.right, f), h = o(w.bottom, h), v.push({
              dom: b,
              left: w.left,
              top: w.top
            });
          }
        }), u *= c, l *= c, f *= c, h *= c;
        var d = f - u, y = h - l, p = Lt.createCanvas(), g = pf(p, {
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
          return r.connectedBackgroundColor && g.add(new Rt({
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
            var S = new kr({
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
    }, t.prototype.convertToPixel = function(r, n, i) {
      return _o(this, "convertToPixel", r, n, i);
    }, t.prototype.convertToLayout = function(r, n, i) {
      return _o(this, "convertToLayout", r, n, i);
    }, t.prototype.convertFromPixel = function(r, n, i) {
      return _o(this, "convertFromPixel", r, n, i);
    }, t.prototype.containPixel = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      var i = this._model, a, o = $u(i, r);
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
    }, t.prototype.getVisual = function(r, n) {
      var i = this._model, a = $u(i, r, {
        defaultMainType: "series"
      }), o = a.seriesModel, s = o.getData(), u = a.hasOwnProperty("dataIndexInside") ? a.dataIndexInside : a.hasOwnProperty("dataIndex") ? s.indexOfRawIndex(a.dataIndex) : null;
      return u != null ? LD(s, u, n) : ED(s, n);
    }, t.prototype.getViewOfComponentModel = function(r) {
      return this._componentsMap[r.__viewId];
    }, t.prototype.getViewOfSeriesModel = function(r) {
      return this._chartsMap[r.__viewId];
    }, t.prototype._initEvents = function() {
      var r = this;
      T(II, function(i) {
        var a = function(o) {
          var s = r.getModel(), u = o.target, l, f = i === "globalout";
          if (f ? l = {} : u && Ki(u, function(y) {
            var p = ft(y);
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
      T(qf, function(i, a) {
        n.on(a, function(o) {
          r.trigger(a, o);
        });
      }), RD(n, this, this._api);
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
      r && jy(this.getDom(), yc, "");
      var n = this, i = n._api, a = n._model;
      T(n._componentsViews, function(o) {
        o.dispose(a, i);
      }), T(n._chartsViews, function(o) {
        o.dispose(a, i);
      }), n._zr.dispose(), n._dom = n._model = n._chartsMap = n._componentsMap = n._chartsViews = n._componentsViews = n._scheduler = n._api = n._zr = n._throttledZrFlush = n._theme = n._coordSysMgr = n._messageCenter = null, delete bn[n.id];
    }, t.prototype.resize = function(r) {
      if (!this[Ct]) {
        if (this._disposed) {
          this.id;
          return;
        }
        this._zr.resize(r);
        var n = this._model;
        if (this._loadingFX && this._loadingFX.resize(), !!n) {
          var i = n.resetOption("media"), a = r && r.silent;
          this[Mt] && (a == null && (a = this[Mt].silent), i = !0, this[Mt] = null), this[Ct] = !0, Yn(this);
          try {
            i && on(this), er.update.call(this, {
              type: "resize",
              animation: R({
                // Disable animation
                duration: 0
              }, r && r.animation)
            });
          } catch (o) {
            throw this[Ct] = !1, o;
          }
          this[Ct] = !1, Gn.call(this, a), Un.call(this, a);
        }
      }
    }, t.prototype.showLoading = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      if (F(r) && (n = r, r = ""), r = r || "default", this.hideLoading(), !!Kf[r]) {
        var i = Kf[r](this._api, n), a = this._zr;
        this._loadingFX = i, a.add(i);
      }
    }, t.prototype.hideLoading = function() {
      if (this._disposed) {
        this.id;
        return;
      }
      this._loadingFX && this._zr.remove(this._loadingFX), this._loadingFX = null;
    }, t.prototype.makeActionFromEvent = function(r) {
      var n = R({}, r);
      return n.type = Xf[r.type], n;
    }, t.prototype.dispatchAction = function(r, n) {
      if (this._disposed) {
        this.id;
        return;
      }
      if (F(n) || (n = {
        silent: !!n
      }), !!xs[r.type] && this._model) {
        if (this[Ct]) {
          this._pendingActions.push(r);
          return;
        }
        var i = n.silent;
        Dl.call(this, r, i);
        var a = n.flush;
        a ? this._zr.flush() : a !== !1 && W.browser.weChat && this._throttledZrFlush(), Gn.call(this, i), Un.call(this, i);
      }
    }, t.prototype.updateLabelLayout = function() {
      he.trigger("series:layoutlabels", this._model, this._api, {
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
      on = function(h) {
        kD(h._model);
        var v = h._scheduler;
        v.restorePipelines(h._zr, h._model), v.prepareStageTasks(), xl(h, !0), xl(h, !1), v.plan();
      }, xl = function(h, v) {
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
            var D = ze(w.type), I = v ? se.getClass(D.main, D.sub) : (
              // FIXME:TS
              // (ChartView as ChartViewConstructor).getClass('series', classType.sub)
              // For backward compat, still support a chart type declared as only subType
              // like "liquidfill", but recommend "series.liquidfill"
              // But need a base class to make a type series.
              Ue.getClass(D.sub)
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
      }, mo = function(h, v, c, d, y) {
        var p = h._model;
        if (p.setUpdatePayload(c), !d) {
          T([].concat(h._componentsViews).concat(h._chartsViews), S);
          return;
        }
        var g = xw(c, d, y), m = c.excludeSeriesId, _;
        m != null && (_ = U(), T(qt(m), function(b) {
          var w = Ge(b, null);
          w != null && _.set(w, !0);
        })), p && p.eachComponent(g, function(b) {
          var w = _ && _.get(b.id) != null;
          if (!w)
            if (ud(c))
              if (b instanceof Xe)
                c.type === _n && !c.notBlur && !b.get(["emphasis", "disabled"]) && KT(b, c, h._api);
              else {
                var C = kh(b.mainType, b.componentIndex, c.name, h._api), M = C.focusSelf, x = C.dispatchers;
                c.type === _n && M && !c.notBlur && Mf(b.mainType, b.componentIndex, h._api), x && T(x, function(D) {
                  c.type === _n ? Tf(D) : Cf(D);
                });
              }
            else If(c) && b instanceof Xe && (JT(b, c, h._api), od(b), le(h));
        }, h), p && p.eachComponent(g, function(b) {
          var w = _ && _.get(b.id) != null;
          w || S(h[d === "series" ? "_chartsMap" : "_componentsMap"][b.__viewId]);
        }, h);
        function S(b) {
          b && b.__alive && b[v] && b[v](b.__model, p, h._api, c);
        }
      }, er = {
        prepareAndUpdate: function(h) {
          on(this), er.update.call(this, h, h && {
            // Needs to mark option changed if newOption is given.
            // It's from MagicType.
            // TODO If use a separate flag optionChanged in payload?
            optionChanged: h.newOption != null
          });
        },
        update: function(h, v) {
          var c = this._model, d = this._api, y = this._zr, p = this._coordSysMgr, g = this._scheduler;
          if (c) {
            BD(c), c.setUpdatePayload(h), g.restoreData(c, h), g.performSeriesTasks(c), p.create(c, d), he.trigger("coordsys:aftercreate", c, d), g.performDataProcessorTasks(c, h), Ml(this, c), p.update(c, d), n(c), g.performVisualTasks(c, h);
            var m = c.get("backgroundColor") || "transparent";
            y.setBackgroundColor(m);
            var _ = c.get("darkMode");
            _ != null && _ !== "auto" && y.setDarkMode(_), Il(this, c, d, h, v), he.trigger("afterupdate", c, d);
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
            }), So(v, c, d, h, {}, p), he.trigger("afterupdate", c, d);
          }
        },
        updateView: function(h) {
          var v = this._model;
          v && (v.setUpdatePayload(h), Ue.markUpdateMethod(h, "updateView"), n(v), this._scheduler.performVisualTasks(v, h, {
            setDirty: !0
          }), Il(this, v, this._api, h, {}), he.trigger("afterupdate", v, this._api));
        },
        updateVisual: function(h) {
          var v = this, c = this._model;
          c && (c.setUpdatePayload(h), c.eachSeries(function(d) {
            d.getData().clearAllVisual();
          }), Ue.markUpdateMethod(h, "updateVisual"), n(c), this._scheduler.performVisualTasks(c, h, {
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
          }), he.trigger("afterupdate", c, this._api));
        },
        /**
         * @deprecated
         */
        updateLayout: function(h) {
          er.update.call(this, h);
        }
      };
      function r(h, v, c, d, y) {
        if (h._disposed) {
          h.id;
          return;
        }
        for (var p = h._model, g = h._coordSysMgr.getCoordinateSystems(), m, _ = $u(p, c), S = 0; S < g.length; S++) {
          var b = g[S];
          if (b[v] && (m = b[v](p, _, d, y)) != null)
            return m;
        }
      }
      _o = r, Ml = function(h, v) {
        var c = h._chartsMap, d = h._scheduler;
        v.eachSeries(function(y) {
          d.updateStreamModes(y, c[y.__viewId]);
        });
      }, Dl = function(h, v) {
        var c = this, d = this.getModel(), y = h.type, p = h.escapeConnect, g = xs[y], m = (g.update || "update").split(":"), _ = m.pop(), S = m[0] != null && ze(m[0]);
        this[Ct] = !0, Yn(this);
        var b = [h], w = !1;
        h.batch && (w = !0, b = H(h.batch, function(O) {
          return O = _t(R({}, O), h), O.batch = null, O;
        }));
        var C = [], M, x = [], D = g.nonRefinedEventType, I = If(h), L = ud(h);
        if (L && xm(this._api), T(b, function(O) {
          var V = g.action(O, d, c._api);
          if (g.refineEvent ? x.push(V) : M = V, M = M || R({}, O), M.type = D, C.push(M), L) {
            var K = xh(h), J = K.queryOptionMap, tt = K.mainTypeSpecified, Q = tt ? J.keys()[0] : "series";
            mo(c, _, O, Q), le(c);
          } else I ? (mo(c, _, O, "series"), le(c)) : S && mo(c, _, O, S.main, S.sub);
        }), _ !== "none" && !L && !I && !S)
          try {
            this[Mt] ? (on(this), er.update.call(this, h), this[Mt] = null) : er[_].call(this, h);
          } catch (O) {
            throw this[Ct] = !1, O;
          }
        if (w ? M = {
          type: D,
          escapeConnect: p,
          batch: C
        } : M = C[0], this[Ct] = !1, !v) {
          var A = void 0;
          if (g.refineEvent) {
            var P = g.refineEvent(x, h, d, this._api).eventContent;
            We(F(P)), A = _t({
              type: g.refinedEventType
            }, P), A.fromAction = h.type, A.fromActionPayload = h, A.escapeConnect = !0;
          }
          var k = this._messageCenter;
          k.trigger(M.type, M), A && k.trigger(A.type, A);
        }
      }, Gn = function(h) {
        for (var v = this._pendingActions; v.length; ) {
          var c = v.shift();
          Dl.call(this, c, h);
        }
      }, Un = function(h) {
        !h && this.trigger("updated");
      }, Tp = function(h, v) {
        h.on("rendered", function(c) {
          v.trigger("rendered", c), // Although zr is dirty if initial animation is not finished
          // and this checking is called on frame, we also check
          // animation finished for robustness.
          h.animation.isFinished() && !v[Mt] && !v._scheduler.unfinished && !v._pendingActions.length ? v.trigger("finished") : h.refresh();
        });
      }, Cp = function(h, v) {
        h.on("mouseover", function(c) {
          var d = c.target, y = Ki(d, Df);
          y && (jT(y, c, v._api), le(v));
        }).on("mouseout", function(c) {
          var d = c.target, y = Ki(d, Df);
          y && (QT(y, c, v._api), le(v));
        }).on("click", function(c) {
          var d = c.target, y = Ki(d, function(m) {
            return ft(m).dataIndex != null;
          }, !0);
          if (y) {
            var p = y.selected ? "unselect" : "select", g = ft(y);
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
          Eo(y, function(m, _) {
            return m.zlevel === _.zlevel ? m.z - _.z : m.zlevel - _.zlevel;
          }), T(y, function(m) {
            var _ = h.getComponent(m.type, m.idx), S = m.zlevel, b = m.key;
            p != null && (S = Math.max(p, S)), b ? (S === p && b !== g && S++, g = b) : g && (S === p && S++, g = ""), p = S, _.setZLevel(S);
          });
        }
      }
      Il = function(h, v, c, d, y) {
        i(v), xp(h, v, c, d, y), T(h._chartsViews, function(p) {
          p.__alive = !1;
        }), So(h, v, c, d, y), T(h._chartsViews, function(p) {
          p.__alive || p.remove(v, c);
        });
      }, xp = function(h, v, c, d, y, p) {
        T(p || h._componentsViews, function(g) {
          var m = g.__model;
          l(m, g), g.render(m, v, c, d), u(m, g), f(m, g);
        });
      }, So = function(h, v, c, d, y, p) {
        var g = h._scheduler;
        y = R(y || {}, {
          updatedSeries: v.getSeries()
        }), he.trigger("series:beforeupdate", v, c, y);
        var m = !1;
        v.eachSeries(function(_) {
          var S = h._chartsMap[_.__viewId];
          S.__alive = !0;
          var b = S.renderTask;
          g.updatePayload(b, d), l(_, S), p && p.get(_.uid) && b.dirty(), b.perform(g.getPerformArgs(b)) && (m = !0), S.group.silent = !!_.get("silent"), s(_, S), od(_);
        }), g.unfinished = m || g.unfinished, he.trigger("series:layoutlabels", v, c, y), he.trigger("series:transition", v, c, y), v.eachSeries(function(_) {
          var S = h._chartsMap[_.__viewId];
          u(_, S), f(_, S);
        }), o(h, v), he.trigger("series:afterupdate", v, c, y);
      }, le = function(h) {
        h[Tl] = !0, h.getZr().wakeUp();
      }, Yn = function(h) {
        h[yo] = (h[yo] + 1) % 1e6;
      }, Dp = function(h) {
        h[Tl] && (h.getZr().storage.traverse(function(v) {
          ea(v) || a(v);
        }), h[Tl] = !1);
      };
      function a(h) {
        for (var v = [], c = h.currentStates, d = 0; d < c.length; d++) {
          var y = c[d];
          y === "emphasis" || y === "blur" || y === "select" || v.push(y);
        }
        h.selected && h.states.select && v.push("select"), h.hoverState === Eh && h.states.emphasis ? v.push("emphasis") : h.hoverState === Lh && h.states.blur && v.push("blur"), h.useStates(v);
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
          var c = e_(h);
          v.eachRendered(function(d) {
            return r_(d, c.z, c.zlevel), !0;
          });
        }
      }
      function l(h, v) {
        v.eachRendered(function(c) {
          if (!ea(c)) {
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
            if (ea(g))
              return;
            if (g instanceof yt && aC(g), g.__dirty) {
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
            Tf(d, y), le(h);
          }, c.prototype.leaveEmphasis = function(d, y) {
            Cf(d, y), le(h);
          }, c.prototype.enterBlur = function(d) {
            ZT(d), le(h);
          }, c.prototype.leaveBlur = function(d) {
            bm(d), le(h);
          }, c.prototype.enterSelect = function(d) {
            wm(d), le(h);
          }, c.prototype.leaveSelect = function(d) {
            Tm(d), le(h);
          }, c.prototype.getModel = function() {
            return h.getModel();
          }, c.prototype.getViewOfComponentModel = function(d) {
            return h.getViewOfComponentModel(d);
          }, c.prototype.getViewOfSeriesModel = function(d) {
            return h.getViewOfSeriesModel(d);
          }, c.prototype.getECUpdateCycleVersion = function() {
            return h[yo];
          }, c.prototype.usingTHL = function() {
            return h._usingTHL;
          }, c;
        })(pm))(h);
      }, g0 = function(h) {
        function v(c, d) {
          for (var y = 0; y < c.length; y++) {
            var p = c[y];
            p[Cl] = d;
          }
        }
        T(Xf, function(c, d) {
          h._messageCenter.on(d, function(y) {
            if (Ds[h.group] && h[Cl] !== wp) {
              if (y && y.escapeConnect)
                return;
              var p = h.makeActionFromEvent(y), g = [];
              T(bn, function(m) {
                m !== h && m.group === h.group && g.push(m);
              }), v(g, wp), T(g, function(m) {
                m[Cl] !== MI && m.dispatchAction(p);
              }), v(g, DI);
            }
          });
        });
      };
    })(), t;
  })(qe)
), gc = Cs.prototype;
gc.on = h0("on");
gc.off = h0("off");
gc.one = function(e, t, r) {
  var n = this;
  function i() {
    for (var a = [], o = 0; o < arguments.length; o++)
      a[o] = arguments[o];
    t && t.apply && t.apply(this, a), n.off(e, i);
  }
  this.on.call(this, e, i, r);
};
var II = ["click", "dblclick", "mouseover", "mouseout", "mousemove", "mousedown", "mouseup", "globalout", "contextmenu"];
var xs = {}, Xf = {}, qf = {}, $f = [], Zf = [], Ms = [], y0 = {}, Kf = {}, bn = {}, Ds = {}, AI = +/* @__PURE__ */ new Date() - 0, PI = +/* @__PURE__ */ new Date() - 0, yc = "_echarts_instance_";
function LI(e, t, r) {
  var n = !(r && r.ssr);
  if (n) {
    var i = mc(e);
    if (i)
      return i;
  }
  var a = new Cs(e, t, r);
  return a.id = "ec_" + AI++, bn[a.id] = a, n && jy(e, yc, a.id), g0(a), he.trigger("afterinit", a), a;
}
function EI(e) {
  if (B(e)) {
    var t = e;
    e = null, T(t, function(r) {
      r.group != null && (e = r.group);
    }), e = e || "g_" + PI++, T(t, function(r) {
      r.group = e;
    });
  }
  return Ds[e] = !0, e;
}
function m0(e) {
  Ds[e] = !1;
}
var RI = m0;
function OI(e) {
  N(e) ? e = bn[e] : e instanceof Cs || (e = mc(e)), e instanceof Cs && !e.isDisposed() && e.dispose();
}
function mc(e) {
  return bn[Mw(e, yc)];
}
function kI(e) {
  return bn[e];
}
function _c(e, t) {
  y0[e] = t;
}
function Sc(e) {
  st(Zf, e) < 0 && Zf.push(e);
}
function bc(e, t) {
  wc($f, e, t, SI);
}
function _0(e) {
  yu("afterinit", e);
}
function S0(e) {
  yu("afterupdate", e);
}
function yu(e, t) {
  he.on(e, t);
}
function Pn(e, t, r) {
  var n, i, a, o, s;
  X(t) && (r = t, t = ""), F(e) ? (n = e.type, i = e.event, o = e.update, s = e.publishNonRefinedEvent, r || (r = e.action), a = e.refineEvent) : (n = e, i = t);
  function u(f) {
    return f.toLowerCase();
  }
  i = u(i || n);
  var l = a ? u(n) : i;
  xs[n] || (We(bp.test(n) && bp.test(i)), a && We(i !== n), xs[n] = {
    actionType: n,
    refinedEventType: i,
    nonRefinedEventType: l,
    update: o,
    action: r,
    refineEvent: a
  }, qf[i] = 1, a && s && (qf[l] = 1), Xf[l] = n);
}
function b0(e, t) {
  cu.register(e, t);
}
function BI(e) {
  var t = cu.get(e);
  if (t)
    return t.getDimensionsInfo ? t.getDimensionsInfo() : t.dimensions.slice();
}
function NI(e, t) {
}
function w0(e, t) {
  wc(Ms, e, t, s0, "layout");
}
function Br(e, t) {
  wc(Ms, e, t, u0, "visual");
}
var Ip = [];
function wc(e, t, r, n, i, a) {
  if ((X(t) || F(t)) && (r = t, t = n), !(st(Ip, r) >= 0)) {
    Ip.push(r);
    var o = $_.wrapStageHandler(r, i);
    o.__prio = t, o.__raw = r, e.push(o);
  }
}
function Tc(e, t) {
  Kf[e] = t;
}
function FI(e) {
  ry({
    createCanvas: e
  });
}
function T0(e, t, r) {
  var n = t0("registerMap");
  n && n(e, t, r);
}
function zI(e) {
  var t = t0("getMap");
  return t && t(e);
}
var C0 = zM;
Br(pc, pD);
Br(gu, gD);
Br(gu, yD);
Br(pc, AD);
Br(gu, PD);
Br(l0, hI);
Sc(x_);
bc(yI, pM);
Tc("default", mD);
Pn({
  type: _n,
  event: _n,
  update: _n
}, Et);
Pn({
  type: Ho,
  event: Ho,
  update: Ho
}, Et);
Pn({
  type: cs,
  event: Rh,
  update: cs,
  action: Et,
  refineEvent: Cc,
  publishNonRefinedEvent: !0
});
Pn({
  type: wf,
  event: Rh,
  update: wf,
  action: Et,
  refineEvent: Cc,
  publishNonRefinedEvent: !0
});
Pn({
  type: vs,
  event: Rh,
  update: vs,
  action: Et,
  refineEvent: Cc,
  publishNonRefinedEvent: !0
});
function Cc(e, t, r, n) {
  return {
    eventContent: {
      selected: tC(r),
      isFromClick: t.isFromClick || !1
    }
  };
}
_c("default", {});
_c("dark", Q_);
var HI = {};
function ki(e) {
  return e == null ? 0 : e.length || 1;
}
function Ap(e) {
  return e;
}
var VI = (
  /** @class */
  (function() {
    function e(t, r, n, i, a, o) {
      this._old = t, this._new = r, this._oldKeyGetter = n || Ap, this._newKeyGetter = i || Ap, this.context = a, this._diffModeMultiple = o === "multiple";
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
        var s = i[o], u = n[s], l = ki(u);
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
        var u = a[s], l = n[u], f = i[u], h = ki(l), v = ki(f);
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
        var i = t[n], a = r[i], o = ki(a);
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
          var u = r[s], l = ki(u);
          l === 0 ? (r[s] = o, a && n.push(s)) : l === 1 ? r[s] = [u, o] : u.push(o);
        }
      }
    }, e;
  })()
), GI = (
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
function UI(e, t) {
  var r = {}, n = r.encode = {}, i = U(), a = [], o = [], s = {};
  T(e.dimensions, function(v) {
    var c = e.getDimensionInfo(v), d = c.coordDim;
    if (d) {
      var y = c.coordDimIndex;
      Al(n, d)[y] = v, c.isExtraCoord || (i.set(d, 1), WI(c.type) && (a[0] = v), Al(s, d)[y] = e.getDimensionIndex(c.name)), c.defaultTooltip && o.push(v);
    }
    vm.each(function(p, g) {
      var m = Al(n, g), _ = c.otherDims[g];
      _ != null && _ !== !1 && (m[_] = c.name);
    });
  });
  var u = [], l = {};
  i.each(function(v, c) {
    var d = n[c];
    l[c] = d[0], u = u.concat(d);
  }), r.dataDimsOnCoord = u, r.dataDimIndicesOnCoord = H(u, function(v) {
    return e.getDimensionInfo(v).storeDimIndex;
  }), r.encodeFirstDimNotExtra = l;
  var f = n.label;
  f && f.length && (a = f.slice());
  var h = n.tooltip;
  return h && h.length ? o = h.slice() : o.length || (o = a.slice()), n.defaultedLabel = a, n.defaultedTooltip = o, r.userOutput = new GI(s, t), r;
}
function Al(e, t) {
  return e.hasOwnProperty(t) || (e[t] = []), e[t];
}
function YI(e) {
  return e === "category" ? "ordinal" : e === "time" ? "time" : "float";
}
function WI(e) {
  return !(e === "ordinal" || e === "time");
}
var Zo = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.otherDims = {}, t != null && R(this, t);
    }
    return e;
  })()
), XI = dt(), qI = {
  float: "f",
  int: "i",
  ordinal: "o",
  number: "n",
  time: "t"
}, x0 = (
  /** @class */
  (function() {
    function e(t) {
      this.dimensions = t.dimensions, this._dimOmitted = t.dimensionOmitted, this.source = t.source, this._fullDimCount = t.fullDimensionCount, this._updateDimOmitted(t.dimensionOmitted);
    }
    return e.prototype.isDimensionOmitted = function() {
      return this._dimOmitted;
    }, e.prototype._updateDimOmitted = function(t) {
      this._dimOmitted = t, t && (this._dimNameMap || (this._dimNameMap = I0(this.source)));
    }, e.prototype.getSourceDimensionIndex = function(t) {
      return G(this._dimNameMap.get(t), -1);
    }, e.prototype.getSourceDimension = function(t) {
      var r = this.source.dimensionsDefine;
      if (r)
        return r[t];
    }, e.prototype.makeStoreSchema = function() {
      for (var t = this._fullDimCount, r = I_(this.source), n = !A0(t), i = "", a = [], o = 0, s = 0; o < t; o++) {
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
        }), r && u != null && (!h || !h.isCalculationCoord) && (i += n ? u.replace(/\`/g, "`1").replace(/\$/g, "`2") : u), i += "$", i += qI[l] || "f", f && (i += f.uid), i += "$";
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
function M0(e) {
  return e instanceof x0;
}
function D0(e) {
  for (var t = U(), r = 0; r < (e || []).length; r++) {
    var n = e[r], i = F(n) ? n.name : n;
    i != null && t.get(i) == null && t.set(i, r);
  }
  return t;
}
function I0(e) {
  var t = XI(e);
  return t.dimNameMap || (t.dimNameMap = D0(e.dimensionsDefine));
}
function A0(e) {
  return e > 30;
}
var Bi = F, mr = H, $I = typeof Int32Array > "u" ? Array : Int32Array, ZI = "e\0\0", Pp = -1, KI = ["hasItemOption", "_nameList", "_idList", "_invertedIndicesMap", "_dimSummary", "userOutput", "_rawData", "_dimValueGetter", "_nameDimIdx", "_idDimIdx", "_nameRepeatCount"], jI = ["_approximateExtent"], Lp, bo, Ni, Fi, Pl, zi, Ll, P0 = (
  /** @class */
  (function() {
    function e(t, r) {
      this.type = "list", this._dimOmitted = !1, this._nameList = [], this._idList = [], this._visual = {}, this._layout = {}, this._itemVisuals = [], this._itemLayouts = [], this._graphicEls = [], this._approximateExtent = {}, this._calculationInfo = {}, this.hasItemOption = !1, this.TRANSFERABLE_METHODS = ["cloneShallow", "downSample", "minmaxDownSample", "lttbDownSample", "map"], this.CHANGABLE_METHODS = ["filterSelf", "selectRange"], this.DOWNSAMPLE_METHODS = ["downSample", "minmaxDownSample", "lttbDownSample"];
      var n, i = !1;
      M0(t) ? (n = t.dimensions, this._dimOmitted = t.isDimensionOmitted(), this._schema = t) : (i = !0, n = t), n = n || ["x", "y"];
      for (var a = {}, o = [], s = {}, u = !1, l = {}, f = 0; f < n.length; f++) {
        var h = n[f], v = N(h) ? new Zo({
          name: h
        }) : h instanceof Zo ? h : new Zo(h), c = v.name;
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
      if (vt(t) || t != null && !isNaN(t) && !this._getDimInfo(t) && (!this._dimOmitted || this._schema.getSourceDimensionIndex(t) < 0))
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
      if (t instanceof Bf && (a = t), !a) {
        var o = this.dimensions, s = fc(t) || $t(t) ? new A_(t, o.length) : t;
        a = new Bf();
        var u = mr(o, function(l) {
          return {
            type: i._dimInfos[l].type,
            property: l
          };
        });
        a.initData(s, u, n);
      }
      this._store = a, this._nameList = (r || []).slice(), this._idList = [], this._nameRepeatCount = {}, this._doInit(0, a.count()), this._dimSummary = UI(this, this._schema), this.userOutput = this._dimSummary.userOutput;
    }, e.prototype.appendData = function(t) {
      var r = this._store.appendData(t);
      this._doInit(r[0], r[1]);
    }, e.prototype.appendValues = function(t, r) {
      var n = this._store.appendValues(t, r && r.length), i = n.start, a = n.end, o = this._shouldMakeIdFromName();
      if (this._updateOrdinalMeta(), r)
        for (var s = i; s < a; s++) {
          var u = s - i;
          this._nameList[s] = r[u], o && Ll(this, s);
        }
    }, e.prototype._updateOrdinalMeta = function() {
      for (var t = this._store, r = this.dimensions, n = 0; n < r.length; n++) {
        var i = this._dimInfos[r[n]];
        i.ordinalMeta && t.collectOrdinalMeta(i.storeDimIndex, i.ordinalMeta);
      }
    }, e.prototype._shouldMakeIdFromName = function() {
      var t = this._store.getProvider();
      return this._idDimIdx == null && t.getSource().sourceFormat !== Ir && !t.fillStorage;
    }, e.prototype._doInit = function(t, r) {
      if (!(t >= r)) {
        var n = this._store, i = n.getProvider();
        this._updateOrdinalMeta();
        var a = this._nameList, o = this._idList, s = i.getSource().sourceFormat, u = s === ue;
        if (u && !i.pure)
          for (var l = [], f = t; f < r; f++) {
            var h = i.getItem(f, l);
            if (!this.hasItemOption && dw(h) && (this.hasItemOption = !0), h) {
              var v = h.name;
              a[f] == null && v != null && (a[f] = Ge(v, null));
              var c = h.id;
              o[f] == null && c != null && (o[f] = Ge(c, null));
            }
          }
        if (this._shouldMakeIdFromName())
          for (var f = t; f < r; f++)
            Ll(this, f);
        Lp(this);
      }
    }, e.prototype.getApproximateExtent = function(t, r) {
      return this._approximateExtent[t] || this._store.getDataExtent(this._getStoreDimIndex(t), r);
    }, e.prototype.setApproximateExtent = function(t, r) {
      r = this.getDimension(r), this._approximateExtent[r] = t.slice();
    }, e.prototype.getCalculationInfo = function(t) {
      return this._calculationInfo[t];
    }, e.prototype.setCalculationInfo = function(t, r) {
      Bi(t) ? R(this._calculationInfo, t) : this._calculationInfo[t] = r;
    }, e.prototype.getName = function(t) {
      var r = this.getRawIndex(t), n = this._nameList[r];
      return n == null && this._nameDimIdx != null && (n = Ni(this, this._nameDimIdx, r)), n == null && (n = ""), n;
    }, e.prototype._getCategory = function(t, r) {
      var n = this._store.get(t, r), i = this._store.getOrdinalMeta(t);
      return i ? i.categories[n] : n;
    }, e.prototype.getId = function(t) {
      return bo(this, this.getRawIndex(t));
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
      return this._store.getDataExtent(this._getStoreDimIndex(t), null);
    }, e.prototype.getSum = function(t) {
      return this._store.getSum(this._getStoreDimIndex(t));
    }, e.prototype.getMedian = function(t) {
      return this._store.getMedian(this._getStoreDimIndex(t));
    }, e.prototype.getValues = function(t, r) {
      var n = this, i = this._store;
      return B(t) ? i.getValues(mr(t, function(a) {
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
      return i == null || isNaN(i) ? Pp : i;
    }, e.prototype.each = function(t, r, n) {
      X(t) && (n = r, r = t, t = []);
      var i = n || this, a = mr(Fi(t), this._getStoreDimIndex, this);
      this._store.each(a, i ? ht(r, i) : r);
    }, e.prototype.filterSelf = function(t, r, n) {
      X(t) && (n = r, r = t, t = []);
      var i = n || this, a = mr(Fi(t), this._getStoreDimIndex, this);
      return this._store = this._store.filter(a, i ? ht(r, i) : r), this;
    }, e.prototype.selectRange = function(t) {
      var r = this, n = {}, i = mt(t);
      return T(i, function(a) {
        var o = r._getStoreDimIndex(a);
        n[o] = t[a];
      }), this._store = this._store.selectRange(n), this;
    }, e.prototype.mapArray = function(t, r, n) {
      X(t) && (n = r, r = t, t = []), n = n || this;
      var i = [];
      return this.each(t, function() {
        i.push(r && r.apply(this, arguments));
      }, n), i;
    }, e.prototype.map = function(t, r, n, i) {
      var a = n || i || this, o = mr(Fi(t), this._getStoreDimIndex, this), s = zi(this);
      return s._store = this._store.map(o, a ? ht(r, a) : r), s;
    }, e.prototype.modify = function(t, r, n, i) {
      var a = n || i || this, o = mr(Fi(t), this._getStoreDimIndex, this);
      this._store.modify(o, a ? ht(r, a) : r);
    }, e.prototype.downSample = function(t, r, n, i) {
      var a = zi(this);
      return a._store = this._store.downSample(this._getStoreDimIndex(t), r, n, i), a;
    }, e.prototype.minmaxDownSample = function(t, r) {
      var n = zi(this);
      return n._store = this._store.minmaxDownSample(this._getStoreDimIndex(t), r), n;
    }, e.prototype.lttbDownSample = function(t, r) {
      var n = zi(this);
      return n._store = this._store.lttbDownSample(this._getStoreDimIndex(t), r), n;
    }, e.prototype.getRawDataItem = function(t) {
      return this._store.getRawDataItem(t);
    }, e.prototype.getItemModel = function(t) {
      var r = this.hostModel, n = this.getRawDataItem(t);
      return new ct(n, r, r && r.ecModel);
    }, e.prototype.diff = function(t) {
      var r = this;
      return new VI(t ? t.getStore().getIndices() : [], this.getStore().getIndices(), function(n) {
        return bo(t, n);
      }, function(n) {
        return bo(r, n);
      });
    }, e.prototype.getVisual = function(t) {
      var r = this._visual;
      return r && r[t];
    }, e.prototype.setVisual = function(t, r) {
      this._visual = this._visual || {}, Bi(t) ? R(this._visual, t) : this._visual[t] = r;
    }, e.prototype.getItemVisual = function(t, r) {
      var n = this._itemVisuals[t], i = n && n[r];
      return i ?? this.getVisual(r);
    }, e.prototype.hasItemVisual = function() {
      return this._itemVisuals.length > 0;
    }, e.prototype.ensureUniqueItemVisual = function(t, r) {
      var n = this._itemVisuals, i = n[t];
      i || (i = n[t] = {});
      var a = i[r];
      return a == null && (a = this.getVisual(r), B(a) ? a = a.slice() : Bi(a) && (a = R({}, a)), i[r] = a), a;
    }, e.prototype.setItemVisual = function(t, r, n) {
      var i = this._itemVisuals[t] || {};
      this._itemVisuals[t] = i, Bi(r) ? R(i, r) : i[r] = n;
    }, e.prototype.clearAllVisual = function() {
      this._visual = {}, this._itemVisuals = [];
    }, e.prototype.setLayout = function(t, r) {
      Bi(t) ? R(this._layout, t) : this._layout[t] = r;
    }, e.prototype.getLayout = function(t) {
      return this._layout[t];
    }, e.prototype.getItemLayout = function(t) {
      return this._itemLayouts[t];
    }, e.prototype.setItemLayout = function(t, r, n) {
      this._itemLayouts[t] = n ? R(this._itemLayouts[t] || {}, r) : r;
    }, e.prototype.clearItemLayouts = function() {
      this._itemLayouts.length = 0;
    }, e.prototype.setItemGraphicEl = function(t, r) {
      var n = this.hostModel && this.hostModel.seriesIndex;
      NT(n, this.dataType, t, r), this._graphicEls[t] = r;
    }, e.prototype.getItemGraphicEl = function(t) {
      return this._graphicEls[t];
    }, e.prototype.eachItemGraphicEl = function(t, r) {
      T(this._graphicEls, function(n, i) {
        n && t && t.call(r, n, i);
      });
    }, e.prototype.cloneShallow = function(t) {
      return t || (t = new e(this._schema ? this._schema : mr(this.dimensions, this._getDimInfo, this), this.hostModel)), Pl(t, this), t._store = this._store, t;
    }, e.prototype.wrapMethod = function(t, r) {
      var n = this[t];
      X(n) && (this.__wrappedMethods = this.__wrappedMethods || [], this.__wrappedMethods.push(t), this[t] = function() {
        var i = n.apply(this, arguments);
        return r.apply(this, [i].concat(Hs(arguments)));
      });
    }, e.internalField = (function() {
      Lp = function(t) {
        var r = t._invertedIndicesMap;
        T(r, function(n, i) {
          var a = t._dimInfos[i], o = a.ordinalMeta, s = t._store;
          if (o) {
            n = r[i] = new $I(o.categories.length);
            for (var u = 0; u < n.length; u++)
              n[u] = Pp;
            for (var u = 0; u < s.count(); u++)
              n[s.get(a.storeDimIndex, u)] = u;
          }
        });
      }, Ni = function(t, r, n) {
        return Ge(t._getCategory(r, n), null);
      }, bo = function(t, r) {
        var n = t._idList[r];
        return n == null && t._idDimIdx != null && (n = Ni(t, t._idDimIdx, r)), n == null && (n = ZI + r), n;
      }, Fi = function(t) {
        return B(t) || (t = t != null ? [t] : []), t;
      }, zi = function(t) {
        var r = new e(t._schema ? t._schema : mr(t.dimensions, t._getDimInfo, t), t.hostModel);
        return Pl(r, t), r;
      }, Pl = function(t, r) {
        T(KI.concat(r.__wrappedMethods || []), function(n) {
          r.hasOwnProperty(n) && (t[n] = r[n]);
        }), t.__wrappedMethods = r.__wrappedMethods, T(jI, function(n) {
          t[n] = Z(r[n]);
        }), t._calculationInfo = R({}, r._calculationInfo);
      }, Ll = function(t, r) {
        var n = t._nameList, i = t._idList, a = t._nameDimIdx, o = t._idDimIdx, s = n[r], u = i[r];
        if (s == null && a != null && (n[r] = s = Ni(t, a, r)), u == null && o != null && (i[r] = u = Ni(t, o, r)), u == null && s != null) {
          var l = t._nameRepeatCount, f = l[s] = (l[s] || 0) + 1;
          u = s, f > 1 && (u += "__ec__" + f), i[r] = u;
        }
      };
    })(), e;
  })()
);
function QI(e, t) {
  return L0(e, t).dimensions;
}
function L0(e, t) {
  fc(e) || (e = M_(e)), t = t || {};
  var r = t.coordDimensions || [], n = t.dimensionsDefine || e.dimensionsDefine || [], i = U(), a = [], o = JI(e, r, n, t.dimensionsCount), s = t.canOmitUnusedDimensions && A0(o), u = n === e.dimensionsDefine, l = u ? I0(e) : D0(n), f = t.encodeDefine;
  !f && t.encodeDefaulter && (f = t.encodeDefaulter(e, o));
  for (var h = U(f), v = new k_(o), c = 0; c < v.length; c++)
    v[c] = -1;
  function d(x) {
    var D = v[x];
    if (D < 0) {
      var I = n[x], L = F(I) ? I : {
        name: I
      }, A = new Zo(), P = L.name;
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
    var I = qt(x).slice();
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
      if (k = qt(k), !k.length)
        for (var O = 0; O < (I && I.length || 1); O++) {
          for (; y < o && d(y).coordDim != null; )
            y++;
          y < o && k.push(y++);
        }
      T(k, function(V, K) {
        var J = d(V);
        if (u && A.type != null && (J.type = A.type), p(_t(J, A), D, K), J.name == null && I) {
          var tt = I[K];
          !F(tt) && (tt = {
            name: tt
          }), J.name = J.displayName = tt.name, J.defaultTooltip = tt.defaultTooltip;
        }
        L && _t(J.otherDims, L);
      });
    }
  });
  function p(x, D, I) {
    vm.get(D) != null ? x.otherDims[D] = I : (x.coordDim = D, x.coordDimIndex = I, i.set(D, !0));
  }
  var g = t.generateCoord, m = t.generateCoordCount, _ = m != null;
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
      M == null && (C.coordDim = tA(S, i, _), C.coordDimIndex = 0, (!g || m <= 0) && (C.isExtraCoord = !0), m--), b(C), C.type == null && (T_(e, w) === re.Must || C.isExtraCoord && (C.otherDims.itemName != null || C.otherDims.seriesName != null)) && (C.type = "ordinal");
    }
  return Mh(a, function(x) {
    return x.name;
  }, function(x, D) {
    D > 0 && (x.name = x.name + (D - 1));
  }), new x0({
    source: e,
    dimensions: a,
    fullDimensionCount: o,
    dimensionOmitted: s
  });
}
function JI(e, t, r, n) {
  var i = Math.max(e.dimensionsDetectedCount || 1, t.length, r.length, n || 0);
  return T(t, function(a) {
    var o;
    F(a) && (o = a.dimsDef) && (i = Math.max(i, o.length));
  }), i;
}
function tA(e, t, r) {
  if (r || t.hasKey(e)) {
    for (var n = 0; t.hasKey(e + n); )
      n++;
    e += n;
  }
  return t.set(e, !0), e;
}
var eA = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.coordSysDims = [], this.axisMap = U(), this.categoryAxisMap = U(), this.coordSysName = t;
    }
    return e;
  })()
);
function rA(e) {
  var t = e.get("coordinateSystem"), r = new eA(t), n = nA[t];
  if (n)
    return n(e, r, r.axisMap, r.categoryAxisMap), r;
}
var nA = {
  cartesian2d: function(e, t, r, n) {
    var i = e.getReferringComponents("xAxis", ar).models[0], a = e.getReferringComponents("yAxis", ar).models[0];
    t.coordSysDims = ["x", "y"], r.set("x", i), r.set("y", a), Wn(i) && (n.set("x", i), t.firstCategoryDimIndex = 0), Wn(a) && (n.set("y", a), t.firstCategoryDimIndex == null && (t.firstCategoryDimIndex = 1));
  },
  singleAxis: function(e, t, r, n) {
    var i = e.getReferringComponents("singleAxis", ar).models[0];
    t.coordSysDims = ["single"], r.set("single", i), Wn(i) && (n.set("single", i), t.firstCategoryDimIndex = 0);
  },
  polar: function(e, t, r, n) {
    var i = e.getReferringComponents("polar", ar).models[0], a = i.findAxisModel("radiusAxis"), o = i.findAxisModel("angleAxis");
    t.coordSysDims = ["radius", "angle"], r.set("radius", a), r.set("angle", o), Wn(a) && (n.set("radius", a), t.firstCategoryDimIndex = 0), Wn(o) && (n.set("angle", o), t.firstCategoryDimIndex == null && (t.firstCategoryDimIndex = 1));
  },
  geo: function(e, t, r, n) {
    t.coordSysDims = ["lng", "lat"];
  },
  parallel: function(e, t, r, n) {
    var i = e.ecModel, a = i.getComponent("parallel", e.get("parallelIndex")), o = t.coordSysDims = a.dimensions.slice();
    T(a.parallelAxisIndex, function(s, u) {
      var l = i.getComponent("parallelAxis", s), f = o[u];
      r.set(f, l), Wn(l) && (n.set(f, l), t.firstCategoryDimIndex == null && (t.firstCategoryDimIndex = u));
    });
  },
  matrix: function(e, t, r, n) {
    var i = e.getReferringComponents("matrix", ar).models[0];
    t.coordSysDims = ["x", "y"];
    var a = i.getDimensionModel("x"), o = i.getDimensionModel("y");
    r.set("x", a), r.set("y", o), n.set("x", a), n.set("y", o);
  }
};
function Wn(e) {
  return e.get("type") === "category";
}
function E0(e, t, r) {
  r = r || {};
  var n = r.byIndex, i = r.stackedCoordDimension, a, o, s;
  iA(t) ? a = t : (o = t.schema, a = o.dimensions, s = t.store);
  var u = !!(e && e.get("stack")), l, f, h, v, c = !0;
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
    h = "__\0ecstackresult_" + e.id, v = "__\0ecstackedover_" + e.id, l && (l.createInvertedIndices = !0);
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
function iA(e) {
  return !M0(e.schema);
}
function R0(e, t) {
  return !!t && t === e.getCalculationInfo("stackedDimension");
}
function O0(e, t) {
  return R0(e, t) ? e.getCalculationInfo("stackResultDimension") : t;
}
function aA(e, t) {
  var r = e.get("coordinateSystem"), n = cu.get(r), i;
  return t && t.coordSysDims && (i = H(t.coordSysDims, function(a) {
    var o = {
      name: a
    }, s = t.axisMap.get(a);
    if (s) {
      var u = s.get("type");
      o.type = YI(u);
    }
    return o;
  })), i || (i = n && (n.getDimensionsInfo ? n.getDimensionsInfo() : n.dimensions.slice()) || ["x", "y"]), i;
}
function oA(e, t, r) {
  var n, i;
  return r && T(e, function(a, o) {
    var s = a.coordDim, u = r.categoryAxisMap.get(s);
    u && (n == null && (n = o), a.ordinalMeta = u.getOrdinalMeta(), t && (a.createInvertedIndices = !0)), a.otherDims.itemName != null && (i = !0);
  }), !i && n != null && (e[n].otherDims.itemName = 0), n;
}
function sA(e, t, r) {
  r = r || {};
  var n = t.getSourceManager(), i, a = !1;
  i = n.getSource(), a = i.sourceFormat === ue;
  var o = rA(t), s = aA(t, o), u = r.useEncodeDefaulter, l = X(u) ? u : u ? Tt(Wx, s, t) : null, f = {
    coordDimensions: s,
    generateCoord: r.generateCoord,
    encodeDefine: t.getEncode(),
    encodeDefaulter: l,
    canOmitUnusedDimensions: !a
  }, h = L0(i, f), v = oA(h.dimensions, r.createInvertedIndices, o), c = a ? null : n.getSharedDataStore(h), d = E0(t, {
    schema: h,
    store: c
  }), y = new P0(h, t);
  y.setCalculationInfo(d);
  var p = v != null && uA(i) ? function(g, m, _, S) {
    return S === v ? _ : this.defaultDimValueGetter(g, m, _, S);
  } : null;
  return y.hasItemOption = !1, y.initData(
    // Try to reuse the data store in sourceManager if using dataset.
    a ? i : c,
    null,
    p
  ), y;
}
function uA(e) {
  if (e.sourceFormat === ue) {
    var t = lA(e.data || []);
    return !B(La(t));
  }
}
function lA(e) {
  for (var t = 0; t < e.length && e[t] == null; )
    t++;
  return e[t];
}
var Le = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.isBlank = function() {
      return this._isBlank;
    }, e.prototype.setBlank = function(t) {
      this._isBlank = t;
    }, e;
  })()
);
Zs(Le);
var fA = 0, Ep = (
  /** @class */
  (function() {
    function e(t) {
      this.categories = t.categories || [], this._needCollect = t.needCollect, this._deduplication = t.deduplication, this.uid = ++fA, this._onCollect = t.onCollect;
    }
    return e.createByAxisModel = function(t) {
      var r = t.option, n = r.data, i = n && H(n, hA);
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
      if (!N(t) && !n)
        return t;
      if (n && !this._deduplication)
        return r = this.categories.length, this.categories[r] = t, this._onCollect && this._onCollect(t, r), r;
      var i = this._getOrCreateMap();
      return r = i.get(t), r == null && (n ? (r = this.categories.length, this.categories[r] = t, i.set(t, r), this._onCollect && this._onCollect(t, r)) : r = NaN), r;
    }, e.prototype._getOrCreateMap = function() {
      return this._map || (this._map = U(this.categories));
    }, e;
  })()
);
function hA(e) {
  return F(e) && e.value != null ? e.value : e + "";
}
var _e = 0, Ta = 1, cA = {
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
}, vA = mt(cA), Is = 2, k0 = 3;
function xc(e, t, r) {
  var n;
  return e = e || {}, pA(e, r), {
    brk: n,
    mapper: e
  };
}
function B0(e, t) {
  T(vA, function(r) {
    e[r] = t[r];
  });
}
function N0(e, t) {
  e.freeze = Et;
}
function Ca(e) {
  return e.getExtentUnsafe(_e, Is);
}
function F0(e, t) {
  return e.getExtentUnsafe(Ta, t) || e.getExtentUnsafe(_e, t);
}
function dA(e) {
  var t = F0(e, k0);
  return t[1] - t[0];
}
function mu(e) {
  var t = e.getExtentUnsafe(_e, k0);
  return t[1] - t[0];
}
function pA(e, t) {
  var r = e || {}, n = [];
  return r._extents = n, n[_e] = t ? t.slice() : Ne(), R(r, gA), r;
}
var gA = {
  needTransform: function() {
    return !1;
  },
  normalize: function(e) {
    var t = this._extents[Ta] || this._extents[_e];
    return t[1] === t[0] ? 0.5 : (e - t[0]) / (t[1] - t[0]);
  },
  scale: function(e) {
    var t = this._extents[Ta] || this._extents[_e];
    return e * (t[1] - t[0]) + t[0];
  },
  transformIn: function(e) {
    return e;
  },
  transformOut: function(e) {
    return e;
  },
  contain: function(e) {
    var t = F0(this, null);
    return e >= t[0] && e <= t[1];
  },
  getExtent: function() {
    return this._extents[_e].slice();
  },
  getExtentUnsafe: function(e) {
    return this._extents[e];
  },
  setExtent: function(e, t) {
    Rp(this._extents, _e, e, t);
  },
  setExtent2: function(e, t, r) {
    var n = this._extents;
    n[e] || (n[e] = n[_e].slice()), Rp(n, e, t, r);
  },
  freeze: function() {
  }
};
function Rp(e, t, r, n) {
  fi(r, n) && (e[t][0] = r, e[t][1] = n);
}
function yA(e) {
  return As(e) || _u(e);
}
function As(e) {
  return e.type === "interval";
}
function Mc(e) {
  return e.type === "time";
}
function _u(e) {
  return e.type === "log";
}
function dr(e) {
  return e.type === "ordinal";
}
function qE(e) {
  var t = $s(e), r = Dn(10, t), n = ur(e / r);
  return n ? n === 2 ? n = 3 : n === 3 ? n = 5 : n *= 2 : n = 1, It(n * r, -t);
}
function za(e) {
  return or(e) + 2;
}
function wo(e, t) {
  return wn(e) / wn(t);
}
function El(e, t, r) {
  var n = r && r.lookup;
  if (n) {
    for (var i = 0; i < n.from.length; i++)
      if (e === n.from[i])
        return n.to[i];
  }
  return Dn(t, e);
}
function mA(e, t, r) {
  var n = e.slice();
  if (n[0] === n[1]) {
    var i = r && r.ctnShp;
    if (n[0] !== 0) {
      var a = Ft(n[0]);
      t[1] || (n[1] += a / 2), n[0] -= a / 2;
    } else
      i && (n[0] = -1), n[1] = 1;
  }
  return (!hr(n[0]) || !hr(n[1])) && (n[0] = 0, n[1] = 1), n[1] < n[0] && n.reverse(), n;
}
function _A(e, t) {
  return [e[0] !== t[0], e[1] !== t[1]];
}
function Dc(e, t) {
  return e = e || t, ur(gt(e, 1));
}
function z0(e, t, r) {
  var n = Ca(e), i = n[0], a = e.count(), o = Math.max((t || 0) + 1, 1);
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
var H0 = (
  /** @class */
  (function(e) {
    z(t, e);
    function t(r) {
      var n = e.call(this) || this;
      n.type = "ordinal", n.parse = t.parse, B0(n, t.decoratedMethods);
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
      return n._mapper = a.mapper, N0(n), n;
    }
    return t.parse = function(r) {
      return r == null ? r = NaN : N(r) ? (r = this._ordinalMeta.getOrdinal(r), r == null && (r = NaN)) : r = ur(r), r;
    }, t.prototype.getTicks = function() {
      var r = [];
      return z0(this, 0, function(n) {
        r.push(n);
      }), r;
    }, t.prototype.getMinorTicks = function(r) {
    }, t.prototype.setSortInfo = function(r) {
      if (r == null) {
        this._ordinalNumbersByTick = this._ticksByOrdinalNumber = null;
        return;
      }
      for (var n = r.ordinalNumbers, i = this._ordinalNumbersByTick = [], a = this._ticksByOrdinalNumber = [], o = 0, s = this._ordinalMeta.categories.length, u = Jt(s, n.length); o < u; ++o) {
        var l = i[o] = n[o];
        a[l] = o;
      }
      for (var f = 0; o < s; ++o) {
        for (; a[f] != null; )
          f++;
        i[o] = f, a[f] = o;
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
      var r = Ca(this._mapper);
      return r[1] - r[0] + 1;
    }, t.prototype.getOrdinalMeta = function() {
      return this._ordinalMeta;
    }, t.type = "ordinal", t.decoratedMethods = {
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
        return this.getRawOrdinalNumber(ur(this._mapper.scale(r)));
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
    }, t;
  })(Le)
);
Le.registerClass(H0);
function Ic(e, t, r, n) {
  for (var i = e.getTicks({
    expandToNicedExtent: !0
  }), a = [], o = e.getExtent(), s = 1; s < i.length; s++) {
    var u = i[s], l = i[s - 1];
    if (!(l.break || u.break)) {
      for (var f = 0, h = [], v = u.value - l.value, c = v / t, d = za(c); f < t - 1; ) {
        var y = It(l.value + (f + 1) * c, d);
        y > o[0] && y < o[1] && h.push(y), f++;
      }
      var p = hu();
      p && p.pruneTicksByBreak("auto", h, r, function(g) {
        return g;
      }, n, o), a.push(h);
    }
  }
  return a;
}
var ii = (
  /** @class */
  (function(e) {
    z(t, e);
    function t(r) {
      var n = e.call(this) || this;
      n.type = "interval", n.parse = t.parse, r = r || {};
      var i = u_(n, r), a = xc(n, i, null);
      return n.brk = a.brk, n._cfg = {
        interval: 0,
        intervalPrecision: 2,
        intervalCount: void 0,
        niceExtent: void 0
      }, n;
    }
    return t.parse = function(r) {
      return r == null || r === "" ? NaN : Number(r);
    }, t.prototype.getConfig = function() {
      return Z(this._cfg);
    }, t.prototype.setConfig = function(r) {
      var n = Ca(this);
      this._cfg = r = Z(r), r.niceExtent == null && (r.niceExtent = n.slice()), r.intervalPrecision == null && (r.intervalPrecision = za(r.interval));
    }, t.prototype.getTicks = function(r) {
      r = r || {};
      var n = this._cfg, i = n.interval, a = Ca(this), o = n.niceExtent, s = n.intervalPrecision, u = hu(), l = this.brk, f = u, h = [];
      if (!i)
        return h;
      r.breakTicks;
      var v = 3e3;
      a[0] < o[0] && h.push({
        value: r.expandToNicedExtent ? It(o[0] - i, s) : a[0]
      });
      for (var c = function(_, S) {
        return ur((S - _) / i);
      }, d = n.intervalCount, y = o[0], p = 0; ; p++) {
        if (d == null) {
          if (y > o[1] || !isFinite(y) || !isFinite(o[1]))
            break;
        } else {
          if (p > d)
            break;
          y = Jt(y, o[1]), p === d && (y = o[1]);
        }
        if (h.push({
          value: y
        }), y = It(y + i, s), l) {
          var g = l.calcNiceTickMultiple(y, c);
          g >= 0 && (y = It(y + g * i, s));
        }
        if (h.length > 0 && y === h[h.length - 1].value)
          break;
        if (h.length > v)
          return [];
      }
      var m = h.length ? h[h.length - 1].value : o[1];
      return a[1] > m && h.push({
        value: r.expandToNicedExtent ? It(m + i, s) : a[1]
      }), h;
    }, t.prototype.getMinorTicks = function(r) {
      return Ic(this, r, $h(this), this._cfg.interval);
    }, t.prototype.getLabel = function(r, n) {
      if (r == null)
        return "";
      var i = n && n.precision;
      i == null ? i = or(r.value) || 0 : i === "auto" && (i = this._cfg.intervalPrecision);
      var a = It(r.value, i, !0);
      return nc(a);
    }, t.type = "interval", t;
  })(Le)
);
Le.registerClass(ii);
var SA = function(e, t, r, n) {
  for (; r < n; ) {
    var i = r + n >>> 1;
    e[i][1] < t ? r = i + 1 : n = i;
  }
  return r;
}, V0 = (
  /** @class */
  (function(e) {
    z(t, e);
    function t(r) {
      var n = e.call(this) || this;
      n.type = "time", n.parse = t.parse, n._locale = r.locale, n._useUTC = r.useUTC, n._interval = 0;
      var i = u_(n, r), a = xc(n, i, null);
      return n.brk = a.brk, n;
    }
    return t.prototype.getLabel = function(r) {
      return Na(r.value, Cd[xx(na(this._minLevelUnit))] || Cd.second, this._useUTC, this._locale);
    }, t.prototype.getFormattedLabel = function(r, n, i) {
      return Mx(r, n, i, this._locale, this._useUTC);
    }, t.prototype.getTicks = function(r) {
      var n = this._interval, i = Ca(this), a = this.brk, o = [];
      if (!n)
        return o;
      var s = this._useUTC;
      o = IA(this._minLevelUnit, this._approxInterval, s, i, mu(this), a);
      var u = Sn.length - 1, l = 0;
      return T(o, function(f) {
        f.time && (u = Math.min(u, st(Sn, f.time.upperTimeUnit)), l = Math.max(l, f.time.level));
      }), o;
    }, t.prototype.getMinorTicks = function(r) {
      return Ic(this, r, $h(this), this._interval);
    }, t.prototype.setTimeInterval = function(r) {
      this._interval = r.interval, this._approxInterval = r.approxInterval, this._minLevelUnit = r.minLevelUnit;
    }, t.parse = function(r) {
      return vt(r) ? Math.round(r) : +cr(r);
    }, t.type = "time", t;
  })(Le)
), To = [
  // Format                           interval
  ["second", Zh],
  ["minute", Kh],
  ["hour", ra],
  ["quarter-day", ra * 6],
  ["half-day", ra * 12],
  ["day", me * 1.2],
  ["half-week", me * 3.5],
  ["week", me * 7],
  ["month", me * 31],
  ["quarter", me * 95],
  ["half-year", Td / 2],
  ["year", Td]
  // 1Y
];
function bA(e, t, r, n) {
  return gs(new Date(t), e, n).getTime() === gs(new Date(r), e, n).getTime();
}
function wA(e, t) {
  return e /= me, e > 16 ? 16 : e > 7.5 ? 7 : e > 3.5 ? 4 : e > 1.5 ? 2 : 1;
}
function TA(e) {
  var t = 30 * me;
  return e /= t, e > 6 ? 6 : e > 3 ? 3 : e > 2 ? 2 : 1;
}
function CA(e) {
  return e /= ra, e > 12 ? 12 : e > 6 ? 6 : e > 3.5 ? 4 : e > 2 ? 2 : 1;
}
function Op(e, t) {
  return e /= t ? Kh : Zh, e > 30 ? 30 : e > 20 ? 20 : e > 15 ? 15 : e > 10 ? 10 : e > 5 ? 5 : e > 2 ? 2 : 1;
}
function xA(e) {
  return gt(wh(e, !0), 1);
}
function MA(e, t, r) {
  var n = Math.max(0, st(Sn, t) - 1);
  return gs(new Date(e), Sn[n], r).getTime();
}
function DA(e, t) {
  var r = /* @__PURE__ */ new Date(0);
  r[e](1);
  var n = r.getTime();
  r[e](1 + t);
  var i = r.getTime() - n;
  return function(a, o) {
    return Math.max(0, Math.round((o - a) / i));
  };
}
function IA(e, t, r, n, i, a) {
  var o = 3e3, s = Sx, u = 0;
  function l(O, V, K, J, tt, Q, rt) {
    for (var nt = DA(tt, O), q = V, ot = new Date(q); q < K && q <= n[1] && (rt.push({
      value: q
    }), !(u++ > o)); )
      if (ot[tt](ot[J]() + O), q = ot.getTime(), a) {
        var St = a.calcNiceTickMultiple(q, nt);
        St > 0 && (ot[tt](ot[J]() + St * O), q = ot.getTime());
      }
    rt.push({
      value: q,
      // extent[1] should be added; deduplication will be performed later.
      notAdd: q > n[1]
    });
  }
  function f(O, V, K) {
    var J = [], tt = !V.length;
    if (!bA(na(O), n[0], n[1], r)) {
      tt && (V = [{
        value: MA(n[0], O, r)
      }, {
        value: n[1]
      }]);
      for (var Q = 0; Q < V.length - 1; Q++) {
        var rt = V[Q].value, nt = V[Q + 1].value;
        if (rt !== nt) {
          var q = void 0, ot = void 0, St = void 0, te = !1;
          switch (O) {
            case "year":
              q = Math.max(1, Math.round(t / me / 365)), ot = l_(r), St = Dx(r);
              break;
            case "half-year":
            case "quarter":
            case "month":
              q = TA(t), ot = jh(r), St = f_(r);
              break;
            case "week":
            // PENDING If week is added. Ignore day.
            case "half-week":
            case "day":
              q = wA(t), ot = Qh(r), St = h_(r), te = !0;
              break;
            case "half-day":
            case "quarter-day":
            case "hour":
              q = CA(t), ot = Jh(r), St = c_(r);
              break;
            case "minute":
              q = Op(t, !0), ot = tc(r), St = v_(r);
              break;
            case "second":
              q = Op(t, !1), ot = ec(r), St = d_(r);
              break;
            case "millisecond":
              q = xA(t), ot = rc(r), St = p_(r);
              break;
          }
          nt >= n[0] && rt <= n[1] && l(q, rt, nt, ot, St, te, J), O === "year" && K.length > 1 && Q === 0 && K.unshift({
            value: K[0].value - q
          });
        }
      }
      for (var Q = 0; Q < J.length; Q++)
        K.push(J[Q]);
    }
  }
  for (var h = [], v = [], c = 0, d = 0, y = 0; y < s.length; ++y) {
    var p = na(s[y]);
    if (Cx(s[y])) {
      f(s[y], h[h.length - 1] || [], v);
      var g = s[y + 1] ? na(s[y + 1]) : null;
      if (p !== g) {
        if (v.length) {
          d = c, v.sort(function(O, V) {
            return O.value - V.value;
          });
          for (var m = [], _ = 0; _ < v.length; ++_) {
            var S = v[_].value;
            (_ === 0 || v[_ - 1].value !== S) && (m.push(v[_]), S >= n[0] && S <= n[1] && c++);
          }
          var b = i / t;
          if (c > b * 1.5 && d > b / 1.5 || (h.push(m), c > b || e === s[y]))
            break;
        }
        v = [];
      }
    }
  }
  for (var w = xt(H(h, function(O) {
    return xt(O, function(V) {
      return V.value >= n[0] && V.value <= n[1] && !V.notAdd;
    });
  }), function(O) {
    return O.length > 0;
  }), C = w.length - 1, M = [], y = 0; y < w.length; ++y)
    for (var x = w[y], D = 0; D < x.length; ++D) {
      var I = Yo(x[D].value, r);
      M.push({
        value: x[D].value,
        time: {
          level: C - y,
          upperTimeUnit: I,
          lowerTimeUnit: I
        }
      });
    }
  Mh(M, kw, null), M.sort(function(O, V) {
    return O.value - V.value;
  });
  var L = M[0], A = M[M.length - 1], P = Yo(n[0], r), k = Yo(n[1], r);
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
var AA = function(e, t) {
  var r = e.getExtent();
  if (r[0] === r[1] && (r[0] -= me, r[1] += me), r[1] === -1 / 0 && r[0] === 1 / 0) {
    var n = /* @__PURE__ */ new Date();
    r[1] = +new Date(n.getFullYear(), n.getMonth(), n.getDate()), r[0] = r[1] - me;
  }
  e.setExtent(r[0], r[1]);
  var i = Dc(t.splitNumber, 10), a = mu(e) / i, o = t.minInterval, s = t.maxInterval;
  o != null && a < o && (a = o), s != null && a > s && (a = s);
  var u = To.length, l = Math.min(SA(To, a, 0, u), u - 1), f = To[l][1], h = To[Math.max(l - 1, 0)][0];
  e.setTimeInterval({
    approxInterval: a,
    interval: f,
    minLevelUnit: h
  });
};
Le.registerClass(V0);
var Co = 0, xo = 1, G0 = (
  /** @class */
  (function(e) {
    z(t, e);
    function t(r) {
      var n = e.call(this) || this;
      n.type = "log", n.parse = ii.parse, n.base = r.logBase || 10;
      var i = [], a = [];
      n._lookup = {
        from: i,
        to: a
      }, i[Co] = i[xo] = a[Co] = a[xo] = NaN, B0(n, t.mapperMethods), r.breakOption;
      var o = {};
      return n.powStub = new ii({
        breakParsed: o.original
      }), n.intervalStub = new ii({
        breakParsed: o.transformed
      }), N0(n, n.intervalStub), n;
    }
    return t.prototype.getTicks = function(r) {
      var n = this.base, i = this.powStub, a = this.intervalStub, o = a.getExtent(), s = i.getExtent(), u = {
        lookup: {
          from: o,
          to: s
        }
      };
      return H(a.getTicks(r || {}), function(l) {
        var f = l.value, h = El(f, n, u), v;
        return {
          value: h,
          break: v
        };
      }, this);
    }, t.prototype.getMinorTicks = function(r) {
      return Ic(
        this,
        r,
        $h(this.powStub),
        // NOTE: minor ticks are in the log scale value to visually hint users "logarithm".
        this.intervalStub.getConfig().interval
      );
    }, t.prototype.getLabel = function(r, n) {
      return this.intervalStub.getLabel(r, n);
    }, t.type = "log", t.mapperMethods = {
      needTransform: function() {
        return !0;
      },
      normalize: function(r) {
        return this.intervalStub.normalize(wo(r, this.base));
      },
      scale: function(r) {
        return El(this.intervalStub.scale(r), this.base, null);
      },
      transformIn: function(r, n) {
        return r = wo(r, this.base), n && n.depth === Is ? r : this.intervalStub.transformIn(r, n);
      },
      transformOut: function(r, n) {
        var i = n ? n.depth : null;
        return kp.depth = i, Bp.lookup = this._lookup, El(i === Is ? r : this.intervalStub.transformOut(r, kp), this.base, Bp);
      },
      contain: function(r) {
        return this.powStub.contain(r);
      },
      /**
       * NOTICE: The caller should ensure `start` and `end` are both non-negative.
       */
      setExtent: function(r, n) {
        this.setExtent2(_e, r, n);
      },
      setExtent2: function(r, n, i) {
        if (!(!fi(n, i) || n <= 0 || i <= 0)) {
          var a = Np, o = Np;
          if (r === _e) {
            var s = this._lookup;
            a = s.to, o = s.from;
          }
          this.powStub.setExtent2(r, a[Co] = n, a[xo] = i);
          var u = this.base;
          this.intervalStub.setExtent2(r, o[Co] = wo(n, u), o[xo] = wo(i, u));
        }
      },
      getFilter: function() {
        return {
          g: 0
        };
      },
      sanitize: function(r, n) {
        return fi(n[0], n[1]) && fr(r) && r <= 0 && (r = n[0]), r;
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
    }, t;
  })(Le)
);
Le.registerClass(G0);
var kp = {}, Bp = {}, Np = [], PA = {
  value: 1,
  category: 1,
  time: 1,
  log: 1
}, U0 = dt();
function LA(e) {
  var t = e.get("type");
  return (
    // In ec option, `xxxAxis.type` may be undefined.
    (t == null || !Xt(PA, t) && !Le.getClass(t)) && (t = "value"), t
  );
}
function EA(e, t, r) {
  var n;
  switch (t) {
    case "category":
      return new H0({
        ordinalMeta: e.getOrdinalMeta ? e.getOrdinalMeta() : e.getCategories(),
        extent: Ne()
      });
    case "time":
      return new V0({
        locale: e.ecModel.getLocaleModel(),
        useUTC: e.ecModel.get("useUTC"),
        breakOption: n
      });
    case "log":
      return new G0({
        logBase: e.get("logBase"),
        breakOption: n
      });
    case "value":
      return new ii({
        breakOption: n
      });
    default:
      return new (Le.getClass(t) || ii)({});
  }
}
function $E(e, t, r) {
  var n = e.getExtentUnsafe(_e, null), i = n[0], a = n[1];
  return fi(i, a) ? i === t || a === t ? OA : i < t && a > t ? RA : Fp : Fp;
}
var RA = 1, OA = 2, Fp = 3;
function kA(e) {
  U0(e).noOnMyZero = !0;
}
function ZE(e) {
  return U0(e).noOnMyZero;
}
function Su(e) {
  var t = e.getLabelModel().get("formatter");
  if (e.type === "time") {
    var r = bx(t);
    return function(i, a) {
      return e.scale.getFormattedLabel(i, a, r);
    };
  } else {
    if (N(t))
      return function(i) {
        var a = e.scale.getLabel(i), o = t.replace("{value}", a ?? "");
        return o;
      };
    if (X(t)) {
      if (e.type === "category")
        return function(i, a) {
          return t(
            Ps(e, i),
            i.value - e.scale.getExtent()[0],
            null
            // Using `null` just for backward compat.
          );
        };
      var n = hu();
      return function(i, a) {
        var o = null;
        return n && (o = n.makeAxisLabelFormatterParamBreak(o, i.break)), t(Ps(e, i), a, o);
      };
    } else
      return function(i) {
        return e.scale.getLabel(i);
      };
  }
}
function Ps(e, t) {
  var r = e.scale;
  return dr(r) ? r.getLabel(t) : t.value;
}
function Ac(e) {
  var t = e.get("interval");
  return t ?? "auto";
}
function BA(e) {
  return e.type === "category" && Ac(e.getLabelModel()) === 0;
}
function NA(e, t) {
  var r = {};
  return T(e.mapDimensionsAll(t), function(n) {
    r[O0(e, n)] = !0;
  }), mt(r);
}
function Ls(e) {
  return e === "middle" || e === "center";
}
function KE(e) {
  return e.getShallow("show");
}
function jE(e, t, r) {
  var n = e.get("breaks", !0);
  n == null;
}
function FA(e, t, r, n, i, a) {
  var o = _u(e), s = o ? e.intervalStub : e;
  if (s.setExtent(n[0], n[1]), o) {
    var u = e.powStub, l = {
      depth: Is
    }, f = e.transformOut(n[0], l), h = e.transformOut(n[1], l), v = _A(r, n);
    t[0] && !v[0] && (f = i[0]), t[1] && !v[1] && (h = i[1]), u.setExtent(f, h);
  }
  s.setConfig(a);
}
function Pc(e, t) {
  return dr(e) ? e.getRawOrdinalNumber(t.value) : t.value;
}
function zA(e, t) {
  return dr(e) && !!t.get("boundaryGap");
}
var HA = (
  /** @class */
  (function() {
    function e() {
    }
    return e.prototype.needIncludeZero = function() {
      return !this.option.scale;
    }, e.prototype.getCoordSysModel = function() {
    }, e;
  })()
), VA = Rw(), zp = "|&", yi = dt(), GA = -2, QE = -1, UA = dt();
function Lc(e, t) {
  var r = e.model, n = yi(gi(r.ecModel)).keyed, i = n && n.get(t);
  return i && i.get(r.uid);
}
function YA(e, t) {
  return W0(Lc(e, t));
}
function WA(e, t) {
  var r = [];
  return Y0(e.model.ecModel, function(n) {
    for (var i = 0; i < t.length; i++)
      t[i] && n.serByIdx[t[i].seriesIndex] && r.push(W0(n));
  }), r;
}
function Y0(e, t) {
  var r = yi(gi(e)).keyed;
  r && r.each(function(n, i) {
    n.each(function(a, o) {
      t(a, i, o);
    });
  });
}
function W0(e) {
  return {
    liPosMinGap: e ? e.liPosMinGap : void 0
  };
}
function XA(e, t) {
  var r = e.model.ecModel, n = yi(gi(r)).axSer;
  n && Ec(r, n.get(e.model.uid), t);
}
function JE(e, t, r) {
  var n = Lc(e, t);
  n && Ec(e.model.ecModel, n.sers, r);
}
function Ec(e, t, r) {
  if (t)
    for (var n = 0; n < t.length; n++) {
      var i = t[n];
      e.isSeriesFiltered(i) || r(i);
    }
}
function tR(e, t) {
  var r = Lc(e, t);
  if (!r || !r.sers.length)
    return 0;
  var n = 0;
  return Ec(e.model.ecModel, r.sers, function() {
    n++;
  }), n;
}
function eR(e, t, r) {
  var n = yi(gi(e)).keyed, i = n && n.get(t);
  i && i.each(function(a) {
    r(a.axis);
  });
}
function X0(e, t) {
  var r = e.model, n = yi(gi(r.ecModel)).keys;
  n && T(n.get(r.uid), function(i) {
    t(i);
  });
}
function qA(e) {
  var t = UA(ND(e)), r = t.keyed || (t.keyed = U());
  Y0(e, function(n, i, a) {
    var o = r.get(i) || r.set(i, U()), s = o.get(a) || o.set(a, {});
    n.metrics.liPosMinGap && q0.liPosMinGap(e, n, s);
  });
}
function rR(e, t) {
  q0[e] = t;
}
var q0 = {};
function nR(e, t, r) {
  if (e) {
    var n = t.ecModel, i = yi(gi(n)), a = e.model.uid, o = i.axSer || (i.axSer = U()), s = o.get(a) || o.set(a, []);
    s.push(t);
    var u = t.subType, l = t.getBaseAxis() === e, f = Qf.get(jf(u, l, r)) || Qf.get(jf(u, l, null));
    if (f) {
      var h = i.keyed || (i.keyed = U()), v = i.keys || (i.keys = U()), c = f.key, d = h.get(c) || h.set(c, U()), y = d.get(a);
      y || (y = d.set(a, {
        axis: e,
        sers: [],
        serByIdx: []
      }), y.metrics = f.getMetrics(e), (v.get(a) || v.set(a, [])).push(c)), y.sers.push(t), y.serByIdx[t.seriesIndex] = t;
    }
  }
}
function jf(e, t, r) {
  return e + zp + G(t, !0) + zp + (r || "");
}
function iR(e, t) {
  var r = jf(t.seriesType, t.baseAxis, t.coordSysType);
  Qf.set(r, t), VA(e, function() {
    e.registerProcessor(e.PRIORITY.PROCESSOR.AXIS_STATISTICS, {
      // NOTE: Theoretically, `appendData` requires `dirtyOnOverallProgress: true` here to re-calculate them.
      // But this OVERALL_STAGE_TASK is applied to all series (no `getTargetSeries` specified),
      // `dirtyOnOverallProgress: true` can cause irrelevant series (e.g., series on geo)
      // to be re-rendered when `appendData` is called, which cause `appendData` meaningless,
      // thereby not setting `dirtyOnOverallProgress: true`.
      overallReset: qA
    });
  });
}
var Qf = U(), $A = dt(), aR = 1, ZA = 3, $0 = (
  /** @class */
  (function() {
    function e(t, r, n, i, a) {
      var o = dr(t), s = o ? r.getCategories().length : null, u;
      if (o) {
        var l = r.getCategories(!0);
        u = l && !l.length;
      }
      var f = n.slice();
      (As(t) || _u(t) || Mc(t)) && (Qy(f, Hi(t, r.get("dataMin", !0))), Jy(f, Hi(t, r.get("dataMax", !0)))), Lw(f) || (f[0] = f[1] = NaN);
      var h = [], v = [!1, !1], c = r.get("min", !0);
      c === "dataMin" ? (h[0] = f[0], v[0] = !0) : (h[0] = Hi(t, X(c) ? c({
        min: f[0],
        max: f[1]
      }) : c), v[0] = h[0] != null);
      var d = r.get("max", !0);
      d === "dataMax" ? (h[1] = f[1], v[1] = !0) : (h[1] = Hi(t, X(d) ? d({
        min: f[0],
        max: f[1]
      }) : d), v[1] = h[1] != null);
      var y = KA(t, r), p = o ? null : f[1] - f[0] || Math.abs(f[0]);
      h[0] == null && (h[0] = o ? u ? f[0] : s ? 0 : NaN : f[0] - y[0] * p), h[1] == null && (h[1] = o ? u ? f[1] : s ? s - 1 : NaN : f[1] + y[1] * p), !hr(h[0]) && (h[0] = NaN), !hr(h[1]) && (h[1] = NaN);
      var g = u || ua(h[0]) || ua(h[1]) || o && !s, m = As(t), _ = m && r.needIncludeZero && r.needIncludeZero();
      _ && (h[0] > 0 && h[1] > 0 && !v[0] && (h[0] = 0), h[0] < 0 && h[1] < 0 && !v[1] && (h[1] = 0));
      var S = !1;
      h[0] > h[1] && (h.reverse(), S = !0);
      var b = Hi(t, r.get("startValue", !0)), w = b != null;
      !fr(b) && i && (b = t.getDefaultStartValue ? t.getDefaultStartValue() : 0), fr(b) && (w || !m || _) && (b < h[0] && !v[0] ? (h[0] = b, v[0] = !0) : b > h[1] && !v[1] && (h[1] = b, v[1] = !0));
      var C = this._i = {
        scale: t,
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
    return e.prototype.makeNoZoom = function() {
      return this._i.noZoomEffMM.slice();
    }, e.prototype.makeFinal = function() {
      var t = this._i, r = t.zoomMM, n = t.noZoomEffMM, i = t.zoomFixMM, a = t.fixMM, o = {
        fixMM: a,
        zoomFixMM: i,
        isBlank: t.isBlank,
        incl0: t.incl0,
        tggAxInv: t.tggAxInv,
        ctnShp: t.ctnShp,
        effMM: n.slice()
      }, s = o.effMM;
      return r[0] != null && (s[0] = r[0], a[0] = i[0] = !0), r[1] != null && (s[1] = r[1], a[1] = i[1] = !0), Hp(t, s), o;
    }, e.prototype.makeRenderInfo = function() {
      return {
        startValue: this._i.startValue
      };
    }, e.prototype.setZoomMM = function(t, r) {
      this._i.zoomMM[t] = r;
    }, e;
  })()
);
function Hp(e, t) {
  var r = e.scale, n = e.dataMM;
  r.sanitize && (t[0] = r.sanitize(t[0], n), t[1] = r.sanitize(t[1], n), Ew(t));
}
function Hi(e, t) {
  return t == null ? null : ua(t) ? NaN : e.parse(t);
}
function KA(e, t) {
  var r;
  if (dr(e))
    r = [0, 0];
  else {
    var n = t.get("boundaryGap");
    typeof n == "boolean" && (n = null), r = B(n) ? n : [n, n];
  }
  return [Vp(r[0]), Vp(r[1])];
}
function Vp(e) {
  return li(typeof e == "boolean" ? 0 : e, 1) || 0;
}
function Z0(e) {
  var t = $A(e.scale);
  return t.extent || (t.extent = Ne()), t;
}
function oR(e, t) {
  Z0(e).dimIdxInCoord = t.get(e.dim);
}
function sR(e, t) {
  var r = e.scale, n = e.model, i = e.dim;
  r.rawExtentInfo || jA(r, e, i, n, t);
}
function jA(e, t, r, n, i) {
  var a = Z0(t), o = a.extent, s = !1;
  XA(t, function(f) {
    if (f.boxCoordinateSystem) {
      var h = y_(f).coord, v = a.dimIdxInCoord;
      if (v >= 0) {
        if (B(h)) {
          var c = h[v];
          c != null && !B(c) && Aw(o, e.parse(c));
        }
      }
    } else if (f.coordinateSystem) {
      var d = f.getData();
      if (d) {
        var y = e.getFilter ? e.getFilter() : null;
        T(NA(d, r), function(p) {
          Pw(o, d.getApproximateExtent(p, y));
        });
      }
      f.__requireStartValue && f.__requireStartValue(t) && (s = !0);
    }
  });
  var u = tP(e, t, n), l = new $0(e, n, o, s, u);
  K0(e, l, i), a.extent = null;
}
function QA(e, t) {
  var r = e.scale;
  K0(r, new $0(r, e.model, t, !1, !1), ZA);
}
function K0(e, t, r) {
  e.rawExtentInfo = t, t.from = r;
}
function uR(e, t) {
  Rc.set(e, t);
}
var Rc = U();
function JA(e, t, r, n, i) {
  e.rawExtentInfo || QA({
    scale: e,
    model: t
  }, i || Ne());
  var a = e.rawExtentInfo.makeFinal(), o = a.effMM;
  return e.setExtent(o[0], o[1]), e.setBlank(a.isBlank), n && a.tggAxInv && r && !r.get("legacyMinMaxDontInverseAxis") && (n.inverse = !n.inverse), a;
}
function tP(e, t, r) {
  var n = zA(e, r), i = r.get("containShape", !0);
  if (i == null && !n && (i = !0), !i)
    return !1;
  var a = !1;
  return X0(t, function(o) {
    a = !!Rc.get(o) || a;
  }), a;
}
function eP(e, t, r, n) {
  if (r.ctnShp) {
    var i;
    if (X0(e, function(s) {
      var u = Rc.get(s);
      if (u) {
        var l = u(e, n);
        l && (i = i || [0, 0], Qy(i, l[0]), Jy(i, l[1]), kA(e));
      }
    }), !!i) {
      var a = t.getExtent();
      if (dr(t))
        e.onBand || t.setExtent2(Ta, Jt(a[0], a[0] + i[0]), gt(a[1], a[1] + i[1]));
      else {
        var o = a.slice();
        r.zoomFixMM[0] || (o[0] = Jt(o[0], t.transformOut(t.transformIn(o[0], null) + i[0], null))), r.zoomFixMM[1] || (o[1] = gt(o[1], t.transformOut(t.transformIn(o[1], null) + i[1], null))), (o[0] < a[0] || o[1] > a[1]) && t.setExtent2(Ta, o[0], o[1]);
      }
    }
  }
}
function Gp(e, t) {
  var r = _u(e), n = r ? e.intervalStub : e, i = t.fixMinMax || [], a = r ? e.getExtent() : null, o = n.getExtent(), s = mA(o, i, t.rawExtentResult);
  n.setExtent(s[0], s[1]), s = n.getExtent();
  var u = r ? nP(n, t) : rP(n, t), l = u.intervalPrecision, f = u.interval, h = t.userInterval;
  h != null && (u.interval = h, u.intervalPrecision = za(h)), i[0] || (s[0] = It(Mn(s[0] / f) * f, l)), i[1] || (s[1] = It(qs(s[1] / f) * f, l)), h != null && (u.niceExtent = s.slice()), FA(e, i, o, s, a, u);
}
function rP(e, t) {
  var r = Dc(t.splitNumber, 5), n = mu(e), i = t.minInterval, a = t.maxInterval, o = wh(n / r, !0);
  i != null && o < i && (o = i), a != null && o > a && (o = a);
  var s = za(o), u = e.getExtent(), l = [It(qs(u[0] / o) * o, s), It(Mn(u[1] / o) * o, s)];
  return {
    interval: o,
    intervalPrecision: s,
    niceExtent: l
  };
}
function nP(e, t) {
  var r = Dc(t.splitNumber, 10), n = e.getExtent(), i = mu(e), a = gt(Wy(i), 1), o = r / i * a;
  o <= 0.5 && (a *= 10);
  var s = za(a), u = [It(qs(n[0] / a) * a, s), It(Mn(n[1] / a) * a, s)];
  return {
    intervalPrecision: s,
    interval: a,
    niceExtent: u
  };
}
function lR(e) {
  var t = e.scale, r = e.model, n = r.axis, i = r.ecModel;
  j0(t, r, n, i, null);
}
function j0(e, t, r, n, i) {
  var a = JA(e, t, n, r, i), o = As(e) || Mc(e);
  iP(e, {
    splitNumber: t.get("splitNumber"),
    fixMinMax: a.fixMM,
    userInterval: t.get("interval"),
    minInterval: o ? t.get("minInterval") : null,
    maxInterval: o ? t.get("maxInterval") : null,
    rawExtentResult: a
  }), r && n && eP(r, e, a, n);
}
function iP(e, t) {
  aP[e.type](e, t);
}
var aP = {
  interval: Gp,
  log: Gp,
  time: AA,
  ordinal: Et
};
function oP(e) {
  return sA(null, e);
}
var sP = {
  isDimensionStacked: R0,
  enableDataStack: E0,
  getStackedDimension: O0
};
function uP(e, t) {
  var r = t;
  t instanceof ct || (r = new ct(t));
  var n = LA(r), i = EA(r, n);
  return e[1] < e[0] && (e = e.slice().reverse()), j0(i, r, null, null, e), i;
}
function lP(e) {
  Ae(e, HA);
}
function fP(e, t) {
  return t = t || {}, Tn(e, null, null, t.state !== "normal");
}
const hP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  createDimensions: QI,
  createList: oP,
  createScale: uP,
  createSymbol: pu,
  createTextStyle: fP,
  dataStack: sP,
  enableHoverEmphasis: ds,
  getECData: ft,
  getLayoutRect: ys,
  mixinAxisModelCommonMethods: lP
}, Symbol.toStringTag, { value: "Module" }));
var Up = [], cP = {
  registerPreprocessor: Sc,
  registerProcessor: bc,
  registerPostInit: _0,
  registerPostUpdate: S0,
  registerUpdateLifecycle: yu,
  registerAction: Pn,
  registerCoordinateSystem: b0,
  registerLayout: w0,
  registerVisual: Br,
  registerTransform: C0,
  registerLoading: Tc,
  registerMap: T0,
  registerImpl: OD,
  PRIORITY: f0,
  ComponentModel: at,
  ComponentView: se,
  SeriesModel: Xe,
  ChartView: Ue,
  // TODO Use ComponentModel and SeriesModel instead of Constructor
  registerComponentModel: function(e) {
    at.registerClass(e);
  },
  registerComponentView: function(e) {
    se.registerClass(e);
  },
  registerSeriesModel: function(e) {
    Xe.registerClass(e);
  },
  registerChartView: function(e) {
    Ue.registerClass(e);
  },
  registerCustomSeries: function(e, t) {
  },
  registerSubTypeDefaulter: function(e, t) {
    at.registerSubTypeDefaulter(e, t);
  },
  registerPainter: function(e, t) {
    Gy(e, t);
  }
};
function ci(e) {
  if (B(e)) {
    T(e, function(t) {
      ci(t);
    });
    return;
  }
  st(Up, e) >= 0 || (Up.push(e), X(e) && (e = {
    install: e
  }), e.install(cP));
}
var vP = 1e-8;
function Yp(e, t) {
  return Math.abs(e - t) < vP;
}
function Wp(e, t, r) {
  var n = 0, i = e[0];
  if (!i)
    return !1;
  for (var a = 1; a < e.length; a++) {
    var o = e[a];
    n += ir(i[0], i[1], o[0], o[1], t, r), i = o;
  }
  var s = e[0];
  return (!Yp(i[0], s[0]) || !Yp(i[1], s[1])) && (n += ir(i[0], i[1], s[0], s[1], t, r)), n !== 0;
}
var dP = [];
function Rl(e, t) {
  for (var r = 0; r < e.length; r++)
    ie(e[r], e[r], t);
}
function Xp(e, t, r, n) {
  for (var i = 0; i < e.length; i++) {
    var a = e[i];
    n && (a = n.project(a)), a && isFinite(a[0]) && isFinite(a[1]) && (br(t, t, a), wr(r, r, a));
  }
}
function pP(e) {
  for (var t = 0, r = 0, n = 0, i = e.length, a = e[i - 1][0], o = e[i - 1][1], s = 0; s < i; s++) {
    var u = e[s][0], l = e[s][1], f = a * l - u * o;
    t += f, r += (a + u) * f, n += (o + l) * f, a = u, o = l;
  }
  return t ? [r / t / 3, n / t / 3, t] : [e[0][0] || 0, e[0][1] || 0];
}
var Q0 = (
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
), qp = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t, r) {
      this.type = "polygon", this.exterior = t, this.interiors = r;
    }
    return e;
  })()
), $p = (
  /** @class */
  /* @__PURE__ */ (function() {
    function e(t) {
      this.type = "linestring", this.points = t;
    }
    return e;
  })()
), gP = (
  /** @class */
  (function(e) {
    z(t, e);
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
        return pP(n.exterior);
      var l = this.getBoundingRect();
      return [l.x + l.width / 2, l.y + l.height / 2];
    }, t.prototype.getBoundingRect = function(r) {
      var n = this._rect;
      if (n && !r)
        return n;
      var i = [1 / 0, 1 / 0], a = [-1 / 0, -1 / 0], o = this.geometries;
      return T(o, function(s) {
        s.type === "polygon" ? Xp(s.exterior, i, a, r) : T(s.points, function(u) {
          Xp(u, i, a, r);
        });
      }), isFinite(i[0]) && isFinite(i[1]) && isFinite(a[0]) && isFinite(a[1]) || (i[0] = i[1] = a[0] = a[1] = 0), n = new Y(i[0], i[1], a[0] - i[0], a[1] - i[1]), r || (this._rect = n), n;
    }, t.prototype.contain = function(r) {
      var n = this.getBoundingRect(), i = this.geometries;
      if (!n.contain(r[0], r[1]))
        return !1;
      t: for (var a = 0, o = i.length; a < o; a++) {
        var s = i[a];
        if (s.type === "polygon") {
          var u = s.exterior, l = s.interiors;
          if (Wp(u, r[0], r[1])) {
            for (var f = 0; f < (l ? l.length : 0); f++)
              if (Wp(l[f], r[0], r[1]))
                continue t;
            return !0;
          }
        }
      }
      return !1;
    }, t.prototype.transformTo = function(r, n, i, a) {
      var o = this.getBoundingRect(), s = o.width / o.height;
      i ? a || (a = i / s) : i = s * a;
      for (var u = new Y(r, n, i, a), l = o.calculateTransform(u), f = this.geometries, h = 0; h < f.length; h++) {
        var v = f[h];
        v.type === "polygon" ? (Rl(v.exterior, l), T(v.interiors, function(c) {
          Rl(c, l);
        })) : T(v.points, function(c) {
          Rl(c, l);
        });
      }
      o = this._rect, o.copy(u), this._center = [o.x + o.width / 2, o.y + o.height / 2];
    }, t.prototype.cloneShallow = function(r) {
      r == null && (r = this.name);
      var n = new t(r, this.geometries, this._center);
      return n._rect = this._rect, n.transformTo = null, n;
    }, t;
  })(Q0)
);
(function(e) {
  z(t, e);
  function t(r, n) {
    var i = e.call(this, r) || this;
    return i.type = "geoSVG", i._elOnlyForCalculate = n, i;
  }
  return t.prototype.calcCenter = function() {
    for (var r = this._elOnlyForCalculate, n = r.getBoundingRect(), i = [n.x + n.width / 2, n.y + n.height / 2], a = xn(dP), o = r; o && !o.isGeoSVGGraphicRoot; )
      gn(a, o.getLocalTransform(), a), o = o.parent;
    return di(a, a), ie(i, i, a), i;
  }, t;
})(Q0);
function yP(e) {
  if (!e.UTF8Encoding)
    return e;
  var t = e, r = t.UTF8Scale;
  r == null && (r = 1024);
  var n = t.features;
  return T(n, function(i) {
    var a = i.geometry, o = a.encodeOffsets, s = a.coordinates;
    if (o)
      switch (a.type) {
        case "LineString":
          a.coordinates = J0(s, o, r);
          break;
        case "Polygon":
          Ol(s, o, r);
          break;
        case "MultiLineString":
          Ol(s, o, r);
          break;
        case "MultiPolygon":
          T(s, function(u, l) {
            return Ol(u, o[l], r);
          });
      }
  }), t.UTF8Encoding = !1, t;
}
function Ol(e, t, r) {
  for (var n = 0; n < e.length; n++)
    e[n] = J0(e[n], t[n], r);
}
function J0(e, t, r) {
  for (var n = [], i = t[0], a = t[1], o = 0; o < e.length; o += 2) {
    var s = e.charCodeAt(o) - 64, u = e.charCodeAt(o + 1) - 64;
    s = s >> 1 ^ -(s & 1), u = u >> 1 ^ -(u & 1), s += i, u += a, i = s, a = u, n.push([s / r, u / r]);
  }
  return n;
}
function Zp(e, t) {
  return e = yP(e), H(xt(e.features, function(r) {
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
    var s = new gP(n[t || "name"], a, n.cp);
    return s.properties = n, s;
  });
}
const mP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  MAX_SAFE_INTEGER: aw,
  asc: Sh,
  getPercentWithPrecision: rw,
  getPixelPrecision: ew,
  getPrecision: or,
  getPrecisionSafe: Yy,
  isNumeric: Xy,
  isRadianAroundZero: da,
  linearMap: yf,
  nice: wh,
  numericToNumber: lr,
  parseDate: cr,
  parsePercent: xe,
  quantile: uw,
  quantity: Wy,
  quantityExponent: $s,
  reformIntervals: lw,
  remRadian: bh,
  round: tw
}, Symbol.toStringTag, { value: "Module" })), _P = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  format: Na,
  parse: cr,
  roundTime: gs
}, Symbol.toStringTag, { value: "Module" })), SP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Arc: Ba,
  BezierCurve: au,
  BoundingRect: Y,
  Circle: ka,
  CompoundPath: Em,
  Ellipse: tu,
  Group: oe,
  Image: kr,
  IncrementalDisplayable: Fm,
  Line: An,
  LinearGradient: Om,
  Polygon: nu,
  Polyline: iu,
  RadialGradient: km,
  Rect: Rt,
  Ring: ru,
  Sector: eu,
  Text: Kt,
  clipPointsByRect: jm,
  clipRectByRect: Qm,
  createIcon: su,
  extendPath: Xm,
  extendShape: Wm,
  getShapeClass: qm,
  getTransform: Km,
  initProps: Nh,
  makeImage: Fh,
  makePath: ou,
  mergePath: Zm,
  registerShape: be,
  resizePath: zh,
  updateProps: pi
}, Symbol.toStringTag, { value: "Module" })), bP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  addCommas: nc,
  capitalFirst: Px,
  encodeHTML: Ut,
  formatTime: Ax,
  formatTpl: ac,
  getTextRect: Ix,
  getTooltipMarker: g_,
  normalizeCssArray: Fa,
  toCamelCase: ic,
  truncateText: Kw
}, Symbol.toStringTag, { value: "Module" })), wP = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  bind: ht,
  clone: Z,
  curry: Tt,
  defaults: _t,
  each: T,
  extend: R,
  filter: xt,
  indexOf: st,
  inherits: dh,
  isArray: B,
  isFunction: X,
  isObject: F,
  isString: N,
  map: H,
  merge: pt,
  reduce: Ye
}, Symbol.toStringTag, { value: "Module" }));
var TP = dt(), oa = dt(), Er = {
  estimate: 1,
  determine: 2
};
function Es(e) {
  return {
    out: {
      noPxChangeTryDetermine: []
    },
    kind: e
  };
}
function CP(e, t) {
  var r = e.getLabelModel().get("customValues");
  if (r) {
    var n = e.scale;
    return {
      labels: H(tS(r, n), function(i, a) {
        return {
          formattedLabel: Su(e)(i, a),
          rawLabel: n.getLabel(i),
          tick: i
        };
      })
    };
  }
  return e.type === "category" ? MP(e, t) : IP(e);
}
function xP(e, t, r) {
  var n = e.scale, i = e.getTickModel().get("customValues");
  return i ? {
    ticks: tS(i, n)
  } : e.type === "category" ? DP(e, t) : {
    ticks: n.getTicks(r)
  };
}
function tS(e, t) {
  var r = t.getExtent(), n = [];
  return T(e, function(i) {
    i = t.parse(i), i >= r[0] && i <= r[1] && n.push(i);
  }), Mh(n, Bw, null), Sh(n), H(n, function(i) {
    return {
      value: i
    };
  });
}
function MP(e, t) {
  var r = e.getLabelModel(), n = eS(e, r, t);
  return !r.get("show") || e.scale.isBlank() ? {
    labels: []
  } : n;
}
function eS(e, t, r) {
  var n = PP(e), i = Ac(t), a = r.kind === Er.estimate;
  if (!a) {
    var o = nS(n, i);
    if (o)
      return o;
  }
  var s, u;
  X(i) ? s = Rs(e, i, !1) : (u = i === "auto" ? LP(e, r) : i, s = Rs(e, u, !1));
  var l = {
    labels: s,
    labelCategoryInterval: u
  };
  return a ? r.out.noPxChangeTryDetermine.push(function() {
    return Jf(n, i, l), !0;
  }) : Jf(n, i, l), l;
}
function DP(e, t) {
  var r = AP(e), n = Ac(t), i = nS(r, n);
  if (i)
    return i;
  var a, o;
  if ((!t.get("show") || e.scale.isBlank()) && (a = []), X(n))
    a = Rs(e, n, !0);
  else if (n === "auto") {
    var s = eS(e, e.getLabelModel(), Es(Er.determine));
    o = s.labelCategoryInterval, a = H(s.labels, function(u) {
      return u.tick;
    });
  } else
    o = n, a = Rs(e, o, !0);
  return Jf(r, n, {
    ticks: a,
    tickCategoryInterval: o
  });
}
function IP(e) {
  var t = e.scale.getTicks(), r = Su(e);
  return {
    labels: H(t, function(n, i) {
      return {
        formattedLabel: r(n, i),
        rawLabel: e.scale.getLabel(n),
        tick: n
      };
    })
  };
}
var AP = rS("axisTick"), PP = rS("axisLabel");
function rS(e) {
  return function(r) {
    return oa(r)[e] || (oa(r)[e] = {
      list: []
    });
  };
}
function nS(e, t) {
  for (var r = 0; r < e.list.length; r++)
    if (e.list[r].key === t)
      return e.list[r].value;
}
function Jf(e, t, r) {
  return e.list.push({
    key: t,
    value: r
  }), r;
}
function LP(e, t) {
  if (t.kind === Er.estimate) {
    var r = e.calculateCategoryInterval(t);
    return t.out.noPxChangeTryDetermine.push(function() {
      return oa(e).autoInterval = r, !0;
    }), r;
  }
  var n = oa(e).autoInterval;
  return n ?? (oa(e).autoInterval = e.calculateCategoryInterval(t));
}
function EP(e, t) {
  var r = t.kind, n = OP(e), i = Su(e), a = (n.axisRotate - n.labelRotate) / 180 * Math.PI, o = e.scale, s = o.getExtent(), u = o.count();
  if (s[1] - s[0] < 1)
    return 0;
  var l = 1, f = 40;
  u > f && (l = Math.max(1, Math.floor(u / f)));
  for (var h = s[0], v = e.dataToCoord(h + 1) - e.dataToCoord(h), c = Math.abs(v * Math.cos(a)), d = Math.abs(v * Math.sin(a)), y = 0, p = 0; h <= s[1]; h += l) {
    var g = 0, m = 0, _ = zy(i({
      value: h
    }), n.font, "center", "top");
    g = _.width * 1.3, m = _.height * 1.3, y = Math.max(y, g, 7), p = Math.max(p, m, 7);
  }
  var S = y / c, b = p / d;
  isNaN(S) && (S = 1 / 0), isNaN(b) && (b = 1 / 0);
  var w = Math.max(0, Math.floor(Math.min(S, b)));
  if (r === Er.estimate)
    return t.out.noPxChangeTryDetermine.push(ht(RP, null, e, w, u)), w;
  var C = iS(e, w, u);
  return C ?? w;
}
function RP(e, t, r) {
  return iS(e, t, r) == null;
}
function iS(e, t, r) {
  var n = TP(e.model), i = e.getExtent(), a = n.lastAutoInterval, o = n.lastTickCount;
  if (a != null && o != null && Math.abs(a - t) <= 1 && Math.abs(o - r) <= 1 && a > t && n.axisExtent0 === i[0] && n.axisExtent1 === i[1])
    return a;
  n.lastTickCount = r, n.lastAutoInterval = t, n.axisExtent0 = i[0], n.axisExtent1 = i[1];
}
function OP(e) {
  var t = e.getLabelModel();
  return {
    axisRotate: e.getRotate ? e.getRotate() : e.isHorizontal && !e.isHorizontal() ? 90 : 0,
    labelRotate: t.get("rotate") || 0,
    font: t.getFont()
  };
}
function Rs(e, t, r) {
  var n = Su(e), i = e.scale, a = [], o = X(t);
  return z0(i, o ? 0 : t, function(s, u) {
    var l = i.getLabel(s);
    if (o) {
      var f = !!t(s.value, l);
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
var kP = 0.8;
function bu(e, t) {
  t = t || {};
  var r = {
    w: NaN,
    w2: NaN
  }, n = e.scale, i = t.fromStat, a = t.min, o = dA(n);
  fr(o) || (o = NaN);
  var s = e.getExtent(), u = Ft(s[1] - s[0]);
  return dr(n) ? BP(r, e, o, u) : i && NP(r, e, o, u, i), a != null && (r.w = fr(r.w) ? gt(a, r.w) : a), r;
}
function BP(e, t, r, n) {
  var i = t.onBand, a = r + (i ? 1 : 0);
  a === 0 && (a = 1), e.w = n / a, !i && r && n && (e.w2 = e.w * r / n);
}
function NP(e, t, r, n, i) {
  var a = !1, o = -1 / 0;
  T(i.key ? [YA(t, i.key)] : WA(t, i.sers || []), function(s) {
    var u = s.liPosMinGap;
    u != null && (u > 0 ? (u > o && (o = u), a = !1) : u === GA && (a = !0));
  }), fr(r) && r > 0 && fr(o) ? (e.w = n / r * o, e.w2 = o) : a && (e.w = n * kP, e.w2 = e.w * r / n);
}
var Kp = [0, 1], FP = (
  /** @class */
  (function() {
    function e(t, r, n) {
      this.onBand = !1, this.inverse = !1, this.dim = t, this.scale = r, this._extent = n || [0, 0];
    }
    return e.prototype.contain = function(t) {
      var r = this._extent, n = Math.min(r[0], r[1]), i = Math.max(r[0], r[1]);
      return t >= n && t <= i;
    }, e.prototype.containData = function(t) {
      return this.scale.contain(this.scale.parse(t));
    }, e.prototype.getExtent = function() {
      return this._extent.slice();
    }, e.prototype.setExtent = function(t, r) {
      var n = this._extent;
      n[0] = t, n[1] = r;
    }, e.prototype.dataToCoord = function(t, r) {
      var n = this.scale;
      return t = n.normalize(n.parse(t)), yf(t, Kp, jp(this), r);
    }, e.prototype.coordToData = function(t, r) {
      var n = yf(t, jp(this), Kp, r);
      return this.scale.scale(n);
    }, e.prototype.pointToData = function(t, r) {
    }, e.prototype.getTicksCoords = function(t) {
      t = t || {};
      var r = t.tickModel || this.getTickModel(), n = xP(this, r, {
        breakTicks: t.breakTicks,
        pruneByBreak: t.pruneByBreak
      }), i = H(n.ticks, function(s) {
        return {
          coord: this.dataToCoord(Pc(this.scale, s)),
          tick: s
        };
      }, this), a = r.get("alignWithLabel"), o = zP(this, i, a);
      return H(i, function(s) {
        return {
          coord: s.coord,
          tickValue: s.tick.value,
          onBand: o
        };
      });
    }, e.prototype.getMinorTicksCoords = function() {
      if (dr(this.scale))
        return [];
      var t = this.model.getModel("minorTick"), r = t.get("splitNumber");
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
    }, e.prototype.getViewLabels = function(t) {
      return t = t || Es(Er.determine), CP(this, t).labels;
    }, e.prototype.getLabelModel = function() {
      return this.model.getModel("axisLabel");
    }, e.prototype.getTickModel = function() {
      return this.model.getModel("axisTick");
    }, e.prototype.getBandWidth = function() {
      return bu(this, {
        min: 1
      }).w;
    }, e.prototype.calculateCategoryInterval = function(t) {
      return t = t || Es(Er.determine), EP(this, t);
    }, e;
  })()
);
function jp(e) {
  var t = e.getExtent();
  if (e.onBand) {
    var r = t[1] - t[0], n = r / e.scale.count() / 2;
    t[0] += n, t[1] -= n;
  }
  return t;
}
function zP(e, t, r) {
  var n = t.length;
  if (!e.onBand || r || !n)
    return !1;
  var i = bu(e).w;
  if (!i)
    return !1;
  T(t, function(s) {
    s.coord -= i / 2;
  });
  var a = e.scale.getExtent(), o = t[n - 1];
  return o.tick.offInterval && t.pop(), t.push({
    coord: o.coord + i,
    tick: {
      value: a[1] + 1
    }
  }), !0;
}
function HP(e) {
  var t = at.extend(e);
  return at.registerClass(t), t;
}
function VP(e) {
  var t = se.extend(e);
  return se.registerClass(t), t;
}
function GP(e) {
  var t = Xe.extend(e);
  return Xe.registerClass(t), t;
}
function UP(e) {
  var t = Ue.extend(e);
  return Ue.registerClass(t), t;
}
var Qp = ["label", "labelLine", "layoutOption", "priority", "defaultAttr", "marginForce", "minMarginForce", "marginDefault", "suggestIgnore"], YP = 1, Os = 2, aS = YP | Os;
function ks(e, t, r) {
  r = r || aS, t ? e.dirty |= r : e.dirty &= ~r;
}
function oS(e, t) {
  return t = t || aS, e.dirty == null || !!(e.dirty & t);
}
function Rr(e) {
  if (e)
    return oS(e) && WP(e, e.label, e), e;
}
function WP(e, t, r) {
  var n = t.getComputedTransform();
  e.transform = Uh(e.transform, n);
  var i = e.localRect = ya(e.localRect, t.getBoundingRect()), a = t.style, o = a.margin, s = r && r.marginForce, u = r && r.minMarginForce, l = r && r.marginDefault, f = a.__marginType;
  f == null && l && (o = l, f = Qn.textMargin);
  for (var h = 0; h < 4; h++)
    kl[h] = f === Qn.minMargin && u && u[h] != null ? u[h] : s && s[h] != null ? s[h] : o ? o[h] : 0;
  f === Qn.textMargin && Lf(i, kl, !1, !1);
  var v = e.rect = ya(e.rect, i);
  return n && v.applyTransform(n), f === Qn.minMargin && Lf(v, kl, !1, !1), e.axisAligned = Gh(n), (e.label = e.label || {}).ignore = t.ignore, ks(e, !1), ks(e, !0, Os), e;
}
var kl = [0, 0, 0, 0];
function XP(e, t, r) {
  return e.transform = Uh(e.transform, r), e.localRect = ya(e.localRect, t), e.rect = ya(e.rect, t), r && e.rect.applyTransform(r), e.axisAligned = Gh(r), e.obb = void 0, (e.label = e.label || {}).ignore = !1, e;
}
function qP(e, t) {
  if (e) {
    e.label.x += t.x, e.label.y += t.y, e.label.markRedraw();
    var r = e.transform;
    r && (r[4] += t.x, r[5] += t.y);
    var n = e.rect;
    n && (n.x += t.x, n.y += t.y);
    var i = e.obb;
    i && i.fromBoundingRect(e.localRect, r);
  }
}
function Jp(e, t) {
  for (var r = 0; r < Qp.length; r++) {
    var n = Qp[r];
    e[n] == null && (e[n] = t[n]);
  }
  return Rr(e);
}
function tg(e) {
  var t = e.obb;
  return (!t || oS(e, Os)) && (e.obb = t = t || new Bm(), t.fromBoundingRect(e.localRect, e.transform), ks(e, !1, Os)), t;
}
function fR(e, t, r, n, i) {
  var a = e.length, o = Hm[t], s = Vm[t];
  if (a < 2)
    return !1;
  e.sort(function(C, M) {
    return C.rect[o] - M.rect[o];
  });
  for (var u = 0, l, f = !1, h = 0; h < a; h++) {
    var v = e[h], c = v.rect;
    l = c[o] - u, l < 0 && (c[o] -= l, v.label[o] -= l, f = !0), u = c[o] + c[s];
  }
  var d = e[0], y = e[a - 1], p, g;
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
      var I = e[D], L = I.rect;
      L[o] += C, I.label[o] += C;
    }
  }
  function b(C, M) {
    for (var x = [], D = 0, I = 1; I < a; I++) {
      var L = e[I - 1].rect, A = Math.max(e[I].rect[o] - L[o] - L[s], 0);
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
function hR(e) {
  for (var t = 0; t < e.length; t++) {
    var r = e[t], n = r.defaultAttr, i = r.labelLine;
    r.label.attr("ignore", n.ignore), i && i.attr("ignore", n.labelGuideIgnore);
  }
}
function $P(e) {
  var t = [];
  e.sort(function(l, f) {
    return (f.suggestIgnore ? 1 : 0) - (l.suggestIgnore ? 1 : 0) || f.priority - l.priority;
  });
  function r(l) {
    if (!l.ignore) {
      var f = l.ensureState("emphasis");
      f.ignore == null && (f.ignore = !1);
    }
    l.ignore = !0;
  }
  for (var n = 0; n < e.length; n++) {
    var i = Rr(e[n]);
    if (!i.label.ignore) {
      for (var a = i.label, o = i.labelLine, s = !1, u = 0; u < t.length; u++)
        if (Oc(i, t[u], null, {
          touchThreshold: 0.05
        })) {
          s = !0;
          break;
        }
      s ? (r(a), o && r(o)) : t.push(i);
    }
  }
}
function Oc(e, t, r, n) {
  return !e || !t || e.label && e.label.ignore || t.label && t.label.ignore || !e.rect.intersect(t.rect, r, n) ? !1 : e.axisAligned && t.axisAligned ? !0 : tg(e).intersect(tg(t), r, n);
}
const cR = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  Axis: FP,
  ChartView: Ue,
  ComponentModel: at,
  ComponentView: se,
  List: P0,
  Model: ct,
  PRIORITY: f0,
  SeriesModel: Xe,
  color: db,
  connect: EI,
  dataTool: HI,
  dependencies: dI,
  disConnect: RI,
  disconnect: m0,
  dispose: OI,
  env: W,
  extendChartView: UP,
  extendComponentModel: HP,
  extendComponentView: VP,
  extendSeriesModel: GP,
  format: bP,
  getCoordinateSystemDimensions: BI,
  getInstanceByDom: mc,
  getInstanceById: kI,
  getMap: zI,
  graphic: SP,
  helper: hP,
  init: LI,
  innerDrawElementOnCanvas: dc,
  matrix: z1,
  number: mP,
  parseGeoJSON: Zp,
  parseGeoJson: Zp,
  registerAction: Pn,
  registerCoordinateSystem: b0,
  registerCustomSeries: NI,
  registerLayout: w0,
  registerLoading: Tc,
  registerLocale: qh,
  registerMap: T0,
  registerPostInit: _0,
  registerPostUpdate: S0,
  registerPreprocessor: Sc,
  registerProcessor: bc,
  registerTheme: _c,
  registerTransform: C0,
  registerUpdateLifecycle: yu,
  registerVisual: Br,
  setCanvasCreator: FI,
  setPlatformAPI: ry,
  throttle: vc,
  time: _P,
  use: ci,
  util: wP,
  vector: w1,
  version: vI,
  zrUtil: c1,
  zrender: qb
}, Symbol.toStringTag, { value: "Module" }));
var ZP = null;
function KP() {
  return ZP;
}
var jP = "expandAxisBreak", xr = Math.PI, QP = [[1, 2, 1, 2], [5, 3, 5, 3], [8, 3, 8, 3]], JP = [[0, 1, 0, 1], [0, 3, 0, 3], [0, 3, 0, 3]], xa = dt(), sS = dt(), tL = (
  /** @class */
  (function() {
    function e(t) {
      this.recordMap = {}, this.resolveAxisNameOverlap = t;
    }
    return e.prototype.ensureRecord = function(t) {
      var r = t.axis.dim, n = t.componentIndex, i = this.recordMap, a = i[r] || (i[r] = []);
      return a[n] || (a[n] = {
        ready: {}
      });
    }, e;
  })()
);
function eL(e, t, r, n) {
  var i = r.axis, a = t.ensureRecord(r), o = [], s, u = kc(e.axisName) && Ls(e.nameLocation);
  T(n, function(d) {
    var y = Rr(d);
    if (!(!y || y.label.ignore)) {
      o.push(y);
      var p = a.transGroup;
      u && (p.transform ? di(Vi, p.transform) : xn(Vi), y.transform && gn(Vi, Vi, y.transform), Y.copy(Mo, y.localRect), Mo.applyTransform(Vi), s ? s.union(Mo) : Y.copy(s = new Y(0, 0, 0, 0), Mo));
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
var Vi = Se(), Mo = new Y(0, 0, 0, 0), rL = function(e, t, r, n, i, a) {
  if (Ls(e.nameLocation)) {
    var o = a.stOccupiedRect;
    o && uS(XP({}, o, a.transGroup.transform), n, i);
  } else
    nL(a.labelInfoList, a.dirVec, n, i);
};
function uS(e, t, r) {
  var n = new ut();
  Oc(e, t, n, {
    direction: Math.atan2(r.y, r.x),
    bidirectional: !1,
    touchThreshold: 0.05
  }) && qP(t, n);
}
function nL(e, t, r, n) {
  for (var i = ut.dot(n, t) >= 0, a = 0, o = e.length; a < o; a++) {
    var s = e[i ? a : o - 1 - a];
    s.label.ignore || uS(s, r, n);
  }
}
var Ar = (
  /** @class */
  (function() {
    function e(t, r, n, i) {
      this.group = new oe(), this._axisModel = t, this._api = r, this._local = {}, this._shared = i || new tL(rL), this._resetCfgDetermined(n);
    }
    return e.prototype.updateCfg = function(t) {
      var r = this._cfg.raw;
      r.position = t.position, r.labelOffset = t.labelOffset, this._resetCfgDetermined(r);
    }, e.prototype.__getRawCfg = function() {
      return this._cfg.raw;
    }, e.prototype._resetCfgDetermined = function(t) {
      var r = this._axisModel, n = r.getDefaultOption ? r.getDefaultOption() : {}, i = G(t.axisName, r.get("name")), a = r.get("nameMoveOverlap");
      (a == null || a === "auto") && (a = G(t.defaultNameMoveOverlap, !0));
      var o = {
        raw: t,
        position: t.position,
        rotation: t.rotation,
        nameDirection: G(t.nameDirection, 1),
        tickDirection: G(t.tickDirection, 1),
        labelDirection: G(t.labelDirection, 1),
        labelOffset: G(t.labelOffset, 0),
        silent: G(t.silent, !0),
        axisName: i,
        nameLocation: dn(r.get("nameLocation"), n.nameLocation, "end"),
        shouldNameMoveOverlap: kc(i) && a,
        optionHideOverlap: r.get(["axisLabel", "hideOverlap"]),
        showMinorTicks: r.get(["minorTick", "show"])
      };
      this._cfg = o;
      var s = new oe({
        x: o.position[0],
        y: o.position[1],
        rotation: o.rotation
      });
      s.updateTransform(), this._transformGroup = s;
      var u = this._shared.ensureRecord(r);
      u.transGroup = this._transformGroup, u.dirVec = new ut(Math.cos(-o.rotation), Math.sin(-o.rotation));
    }, e.prototype.build = function(t, r) {
      var n = this;
      return t || (t = {
        axisLine: !0,
        axisTickLabelEstimate: !1,
        axisTickLabelDetermine: !0,
        axisName: !0
      }), T(iL, function(i) {
        t[i] && aL[i](n._cfg, n._local, n._shared, n._axisModel, n.group, n._transformGroup, n._api, r || {});
      }), this;
    }, e.innerTextLayout = function(t, r, n) {
      var i = bh(r - t), a, o;
      return da(i) ? (o = n > 0 ? "top" : "bottom", a = "center") : da(i - xr) ? (o = n > 0 ? "bottom" : "top", a = "center") : (o = "middle", i > 0 && i < xr ? a = n > 0 ? "right" : "left" : a = n > 0 ? "left" : "right"), {
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
), iL = ["axisLine", "axisTickLabelEstimate", "axisTickLabelDetermine", "axisName"], aL = {
  axisLine: function(e, t, r, n, i, a, o) {
    var s = n.get(["axisLine", "show"]);
    if (s === "auto" && (s = !0, e.raw.axisLineAutoShow != null && (s = !!e.raw.axisLineAutoShow)), !!s) {
      var u = n.axis.getExtent(), l = a.transform, f = [u[0], 0], h = [u[1], 0], v = f[0] > h[0];
      l && (ie(f, f, l), ie(h, h, l));
      var c = R({
        lineCap: "round"
      }, n.getModel(["axisLine", "lineStyle"]).getLineStyle()), d = {
        strokeContainThreshold: e.raw.strokeContainThreshold || 5,
        silent: !0,
        z2: 1,
        style: c
      };
      if (n.get(["axisLine", "breakLine"]) && yx(n.axis.scale))
        KP().buildAxisBreakLine(n, i, a, d);
      else {
        var y = new An(R({
          shape: {
            x1: f[0],
            y1: f[1],
            x2: h[0],
            y2: h[1]
          }
        }, d));
        Hh(y.shape, y.style.lineWidth), y.anid = "line", i.add(y);
      }
      var p = n.get(["axisLine", "symbol"]);
      if (p != null) {
        var g = n.get(["axisLine", "symbolSize"]);
        N(p) && (p = [p, p]), (N(g) || vt(g)) && (g = [g, g]);
        var m = ZD(n.get(["axisLine", "symbolOffset"]) || 0, g), _ = g[0], S = g[1];
        T([{
          rotate: e.rotation + Math.PI / 2,
          offset: m[0],
          r: 0
        }, {
          rotate: e.rotation - Math.PI / 2,
          offset: m[1],
          r: Math.sqrt((f[0] - h[0]) * (f[0] - h[0]) + (f[1] - h[1]) * (f[1] - h[1]))
        }], function(b, w) {
          if (p[w] !== "none" && p[w] != null) {
            var C = pu(p[w], -_ / 2, -S / 2, _, S, c.stroke, !0), M = b.r + b.offset, x = v ? h : f;
            C.attr({
              rotation: b.rotate,
              x: x[0] + M * Math.cos(e.rotation),
              y: x[1] - M * Math.sin(e.rotation),
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
  axisTickLabelEstimate: function(e, t, r, n, i, a, o, s) {
    var u = rg(t, i, s);
    u && eg(e, t, r, n, i, a, o, Er.estimate);
  },
  /**
   * Finish axis tick label build.
   * Can be only called once.
   */
  axisTickLabelDetermine: function(e, t, r, n, i, a, o, s) {
    var u = rg(t, i, s);
    u && eg(e, t, r, n, i, a, o, Er.determine);
    var l = lL(e, i, a, n);
    uL(e, t.labelLayoutList, l), fL(e, i, a, n, e.tickDirection);
  },
  /**
   * [CAUTION] This method can be called multiple times, following the change due to `resetCfg` called
   *  in size measurement. Thus this method should be idempotent, and should be performant.
   */
  axisName: function(e, t, r, n, i, a, o, s) {
    var u = r.ensureRecord(n);
    t.nameEl && (i.remove(t.nameEl), t.nameEl = u.nameLayout = u.nameLocation = null);
    var l = e.axisName;
    if (kc(l)) {
      var f = e.nameLocation, h = e.nameDirection, v = n.getModel("nameTextStyle"), c = n.get("nameGap") || 0, d = n.axis.getExtent(), y = n.axis.inverse ? -1 : 1, p = new ut(0, 0), g = new ut(0, 0);
      f === "start" ? (p.x = d[0] - y * c, g.x = -y) : f === "end" ? (p.x = d[1] + y * c, g.x = y) : (p.x = (d[0] + d[1]) / 2, p.y = e.labelOffset + h * c, g.y = h);
      var m = Se();
      g.transform(Gs(m, m, e.rotation));
      var _ = n.get("nameRotate");
      _ != null && (_ = _ * xr / 180);
      var S, b;
      Ls(f) ? S = Ar.innerTextLayout(
        e.rotation,
        _ ?? e.rotation,
        // Adapt to axis.
        h
      ) : (S = oL(e.rotation, f, _ || 0, d), b = e.raw.axisNameAvailableWidth, b != null && (b = Math.abs(b / Math.sin(S.rotation)), !isFinite(b) && (b = null)));
      var w = v.getFont(), C = n.get("nameTruncate", !0) || {}, M = C.ellipsis, x = la(e.raw.nameTruncateMaxWidth, C.maxWidth, b), D = s.nameMarginLevel || 0, I = new Kt({
        x: p.x,
        y: p.y,
        rotation: S.rotation,
        silent: Ar.isLabelSilent(n),
        style: Tn(v, {
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
      if (uu({
        el: I,
        componentModel: n,
        itemName: l
      }), I.__fullText = l, I.anid = "name", n.get("triggerEvent")) {
        var L = Ar.makeAxisEventDataBase(n);
        L.targetType = "axisName", L.name = l, ft(I).eventData = L;
      }
      a.add(I), I.updateTransform(), t.nameEl = I;
      var A = u.nameLayout = Rr({
        label: I,
        priority: I.z2,
        defaultAttr: {
          ignore: I.ignore
        },
        marginDefault: Ls(f) ? QP[D] : JP[D]
      });
      if (u.nameLocation = f, i.add(I), I.decomposeTransform(), e.shouldNameMoveOverlap && A) {
        var P = r.ensureRecord(n);
        r.resolveAxisNameOverlap(e, r, n, A, g, P);
      }
    }
  }
};
function eg(e, t, r, n, i, a, o, s) {
  fS(t) || hL(e, t, i, s, n, o);
  var u = t.labelLayoutList;
  cL(e, n, u, a), e.rotation;
  var l = e.optionHideOverlap;
  sL(n, u, l), l && $P(
    // Filter the already ignored labels by the previous overlap resolving methods.
    xt(u, function(f) {
      return f && !f.label.ignore;
    })
  ), eL(e, r, n, u);
}
function oL(e, t, r, n) {
  var i = bh(r - e), a, o, s = n[0] > n[1], u = t === "start" && !s || t !== "start" && s;
  return da(i - xr / 2) ? (o = u ? "bottom" : "top", a = "center") : da(i - xr * 1.5) ? (o = u ? "top" : "bottom", a = "center") : (o = "middle", i < xr * 1.5 && i > xr / 2 ? a = u ? "left" : "right" : a = u ? "right" : "left"), {
    rotation: i,
    textAlign: a,
    textVerticalAlign: o
  };
}
function sL(e, t, r) {
  var n = e.axis, i = e.get(["axisLabel", "customValues"]);
  if (BA(n))
    return;
  function a(l, f, h) {
    var v = Rr(t[f]), c = Rr(t[h]), d = n.scale;
    if (!(!v || !c)) {
      if (l == null) {
        if (!r && i)
          return;
        var y = xa(v.label).labelInfo.tick;
        if (
          // TimeScale does not expand extent to "nice", so eliminate labels that are not nice.
          Mc(d) && y.notNice || dr(d) && y.offInterval
        ) {
          $n(v.label);
          return;
        }
      }
      if (l === !1 || v.suggestIgnore) {
        $n(v.label);
        return;
      }
      if (c.suggestIgnore) {
        $n(c.label);
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
      }) && $n(l ? c.label : v.label);
    }
  }
  var o = e.get(["axisLabel", "showMinLabel"]), s = e.get(["axisLabel", "showMaxLabel"]), u = t.length;
  a(o, 0, 1), a(s, u - 1, u - 2);
}
function uL(e, t, r) {
  e.showMinorTicks || T(t, function(n) {
    if (n && n.label.ignore)
      for (var i = 0; i < r.length; i++) {
        var a = r[i], o = sS(a), s = xa(n.label);
        if (o.tickValue != null && !o.onBand && o.tickValue === s.labelInfo.tick.value) {
          $n(a);
          return;
        }
      }
  });
}
function $n(e) {
  e && (e.ignore = !0);
}
function lS(e, t, r, n, i) {
  for (var a = [], o = [], s = [], u = 0; u < e.length; u++) {
    var l = e[u].coord;
    o[0] = l, o[1] = 0, s[0] = l, s[1] = r, t && (ie(o, o, t), ie(s, s, t));
    var f = new An({
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
    Hh(f.shape, f.style.lineWidth), f.anid = i + "_" + e[u].tickValue, a.push(f);
    var h = sS(f);
    h.onBand = !!e[u].onBand, h.tickValue = e[u].tickValue;
  }
  return a;
}
function lL(e, t, r, n) {
  var i = n.axis, a = n.getModel("axisTick"), o = a.get("show");
  if (o === "auto" && (o = !0, e.raw.axisTickAutoShow != null && (o = !!e.raw.axisTickAutoShow)), !o || i.scale.isBlank())
    return [];
  for (var s = a.getModel("lineStyle"), u = e.tickDirection * a.get("length"), l = i.getTicksCoords(), f = lS(l, r.transform, u, _t(s.getLineStyle(), {
    stroke: n.get(["axisLine", "lineStyle", "color"])
  }), "ticks"), h = 0; h < f.length; h++)
    t.add(f[h]);
  return f;
}
function fL(e, t, r, n, i) {
  var a = n.axis, o = n.getModel("minorTick");
  if (!(!e.showMinorTicks || a.scale.isBlank())) {
    var s = a.getMinorTicksCoords();
    if (s.length)
      for (var u = o.getModel("lineStyle"), l = i * o.get("length"), f = _t(u.getLineStyle(), _t(n.getModel("axisTick").getLineStyle(), {
        stroke: n.get(["axisLine", "lineStyle", "color"])
      })), h = 0; h < s.length; h++)
        for (var v = lS(s[h], r.transform, l, f, "minorticks_" + h), c = 0; c < v.length; c++)
          t.add(v[c]);
  }
}
function rg(e, t, r) {
  if (fS(e)) {
    var n = e.axisLabelsCreationContext, i = n.out.noPxChangeTryDetermine;
    if (r.noPxChange) {
      for (var a = !0, o = 0; o < i.length; o++)
        a = a && i[o]();
      if (a)
        return !1;
    }
    i.length && (t.remove(e.labelGroup), th(e, null, null, null));
  }
  return !0;
}
function hL(e, t, r, n, i, a) {
  var o = i.axis, s = la(e.raw.axisLabelShow, i.get(["axisLabel", "show"])), u = new oe();
  r.add(u);
  var l = Es(n);
  if (!s || o.scale.isBlank()) {
    th(t, [], u, l);
    return;
  }
  var f = i.getModel("axisLabel"), h = o.getViewLabels(l), v = (la(e.raw.labelRotate, f.get("rotate")) || 0) * xr / 180, c = Ar.innerTextLayout(e.rotation, v, e.labelDirection), d = i.getCategories && i.getCategories(!0), y = [], p = i.get("triggerEvent"), g = 1 / 0, m = -1 / 0;
  T(h, function(S, b) {
    var w, C = S.tick, M = S.formattedLabel, x = S.rawLabel, D = f, I = Pc(o.scale, C);
    if (d && d[I]) {
      var L = d[I];
      F(L) && L.textStyle && (D = new ct(L.textStyle, f, i.ecModel));
    }
    var A = D.getTextColor() || i.get(["axisLine", "lineStyle", "color"]), P = D.getShallow("align", !0) || c.textAlign, k = G(D.getShallow("alignMinLabel", !0), P), O = G(D.getShallow("alignMaxLabel", !0), P), V = D.getShallow("verticalAlign", !0) || D.getShallow("baseline", !0) || c.textVerticalAlign, K = G(D.getShallow("verticalAlignMinLabel", !0), V), J = G(D.getShallow("verticalAlignMaxLabel", !0), V), tt = 10 + (((w = C.time) === null || w === void 0 ? void 0 : w.level) || 0);
    g = Math.min(g, tt), m = Math.max(m, tt);
    var Q = new Kt({
      // --- transform props start ---
      // All of the transform props MUST not be set here, but should be set in
      // `updateAxisLabelChangableProps`, because they may change in estimation,
      // and need to calculate based on global coord sys by `decomposeTransform`.
      x: 0,
      y: 0,
      rotation: 0,
      // --- transform props end ---
      silent: Ar.isLabelSilent(i),
      z2: tt,
      style: Tn(D, {
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
    var rt = xa(Q);
    if (rt.labelInfo = S, rt.layoutRotation = c.rotation, uu({
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
      var nt = Ar.makeAxisEventDataBase(i);
      nt.targetType = "axisLabel", nt.value = x, nt.tickIndex = b;
      var q = S.tick.break;
      if (q) {
        var ot = q.parsedBreak;
        nt.break = {
          // type: labelItem.break.type,
          start: ot.vmin,
          end: ot.vmax
        };
      }
      o.type === "category" && (nt.dataIndex = I), ft(Q).eventData = nt, q && dL(i, a, Q, q);
    }
    y.push(Q), u.add(Q);
  });
  var _ = H(y, function(S) {
    return {
      label: S,
      priority: xa(S).labelInfo.tick.break ? S.z2 + (m - g + 1) : S.z2,
      defaultAttr: {
        ignore: S.ignore
      }
    };
  });
  th(t, _, u, l);
}
function fS(e) {
  return !!e.labelLayoutList;
}
function th(e, t, r, n) {
  e.labelLayoutList = t, e.labelGroup = r, e.axisLabelsCreationContext = n;
}
function cL(e, t, r, n) {
  var i = t.get(["axisLabel", "margin"]);
  T(r, function(a, o) {
    var s = Rr(a);
    if (s) {
      var u = s.label, l = xa(u);
      s.suggestIgnore = u.ignore, u.ignore = !1, ls(rr, vL);
      var f = t.axis;
      rr.x = f.dataToCoord(Pc(f.scale, l.labelInfo.tick)), rr.y = e.labelOffset + e.labelDirection * i, rr.rotation = l.layoutRotation, n.add(rr), rr.updateTransform(), n.remove(rr), rr.decomposeTransform(), ls(u, rr), u.markRedraw(), ks(s, !0), Rr(s);
    }
  });
}
var rr = new Rt(), vL = new Rt();
function kc(e) {
  return !!e;
}
function dL(e, t, r, n) {
  r.on("click", function(i) {
    var a = {
      type: jP,
      breaks: [{
        start: n.parsedBreak.breakOption.start,
        end: n.parsedBreak.breakOption.end
      }]
    };
    a[e.axis.dim + "AxisIndex"] = e.componentIndex, t.dispatchAction(a);
  });
}
function Bs(e, t, r) {
  r = r || {};
  var n = t.axis, i = {}, a = n.getAxesOnZeroOf()[0], o = n.position, s = a ? "onZero" : o, u = n.dim, l = [e.x, e.x + e.width, e.y, e.y + e.height], f = {
    left: 0,
    right: 1,
    top: 0,
    bottom: 1,
    onZero: 2
  }, h = t.get("offset") || 0, v = u === "x" ? [l[2] - h, l[3] + h] : [l[0] - h, l[1] + h];
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
  i.labelDirection = i.tickDirection = i.nameDirection = d[o], i.labelOffset = a ? v[f[o]] - v[f.onZero] : 0, t.get(["axisTick", "inside"]) && (i.tickDirection = -i.tickDirection), la(r.labelInside, t.get(["axisLabel", "inside"])) && (i.labelDirection = -i.labelDirection);
  var y = t.get(["axisLabel", "rotate"]);
  return i.labelRotate = s === "top" ? -y : y, i.z2 = 1, i;
}
function vR(e) {
  return e.coordinateSystem && e.coordinateSystem.type === "cartesian2d";
}
function dR(e) {
  var t = {
    xAxisModel: null,
    yAxisModel: null
  };
  return T(t, function(r, n) {
    var i = n.replace(/Model$/, ""), a = e.getReferringComponents(i, ar).models[0];
    t[n] = a;
  }), t;
}
function pR(e, t, r, n, i, a) {
  for (var o = Bs(e, r), s = !1, u = !1, l = 0; l < t.length; l++)
    yA(t[l].getOtherAxis(r.axis).scale) && (s = u = !0, r.axis.type === "category" && r.axis.onBand && (u = !1));
  return o.axisLineAutoShow = s, o.axisTickAutoShow = u, o.defaultNameMoveOverlap = a, new Ar(r, n, o, i);
}
function gR(e, t, r) {
  var n = Bs(t, r);
  e.updateCfg(n);
}
var yR = (
  /** @class */
  (function() {
    function e(t) {
      this.type = "cartesian", this._dimList = [], this._axes = {}, this.name = t || "";
    }
    return e.prototype.getAxis = function(t) {
      return this._axes[t];
    }, e.prototype.getAxes = function() {
      return H(this._dimList, function(t) {
        return this._axes[t];
      }, this);
    }, e.prototype.getAxesByScale = function(t) {
      return t = t.toLowerCase(), xt(this.getAxes(), function(r) {
        return r.scale.type === t;
      });
    }, e.prototype.addAxis = function(t) {
      var r = t.dim;
      this._axes[r] = t, this._dimList.push(r);
    }, e;
  })()
);
function pL(e, t) {
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
  return gL(r, e, t), r.seriesInvolved && mL(r, e), r;
}
function gL(e, t, r) {
  var n = t.getComponent("tooltip"), i = t.getComponent("axisPointer"), a = i.get("link", !0) || [], o = [];
  T(r.getCoordinateSystems(), function(s) {
    if (!s.axisPointerEnabled)
      return;
    var u = Ma(s.model), l = e.coordSysAxesInfo[u] = {};
    e.coordSysMap[u] = s;
    var f = s.model, h = f.getModel("tooltip", n);
    if (T(s.getAxes(), Tt(y, !1, null)), s.getTooltipAxes && n && h.get("show")) {
      var v = h.get("trigger") === "axis", c = h.get(["axisPointer", "type"]) === "cross", d = s.getTooltipAxes(h.get(["axisPointer", "axis"]));
      (v || c) && T(d.baseAxes, Tt(y, c ? "cross" : !0, v)), c && T(d.otherAxes, Tt(y, "cross", !1));
    }
    function y(p, g, m) {
      var _ = m.model.getModel("axisPointer", i), S = _.get("show");
      if (!(!S || S === "auto" && !p && !eh(_))) {
        g == null && (g = _.get("triggerTooltip")), _ = p ? yL(m, h, i, t, p, g) : _;
        var b = _.get("snap"), w = _.get("triggerEmphasis"), C = Ma(m.model), M = g || b || m.type === "category", x = e.axesInfo[C] = {
          key: C,
          axis: m,
          coordSys: s,
          axisPointerModel: _,
          triggerTooltip: g,
          triggerEmphasis: w,
          involveSeries: M,
          snap: b,
          useHandle: eh(_),
          seriesModels: [],
          linkGroup: null
        };
        l[C] = x, e.seriesInvolved = e.seriesInvolved || M;
        var D = _L(a, m);
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
function yL(e, t, r, n, i, a) {
  var o = t.getModel("axisPointer"), s = ["type", "snap", "lineStyle", "shadowStyle", "label", "animation", "animationDurationUpdate", "animationEasingUpdate", "z"], u = {};
  T(s, function(v) {
    u[v] = Z(o.get(v));
  }), u.snap = e.type !== "category" && !!a, o.get("type") === "cross" && (u.type = "line");
  var l = u.label || (u.label = {});
  if (l.show == null && (l.show = !1), i === "cross") {
    var f = o.get(["label", "show"]);
    if (l.show = f ?? !0, !a) {
      var h = u.lineStyle = o.get("crossStyle");
      h && _t(l, h.textStyle);
    }
  }
  return e.model.getModel("axisPointer", new ct(u, r, n));
}
function mL(e, t) {
  t.eachSeries(function(r) {
    var n = r.coordinateSystem, i = r.get(["tooltip", "trigger"], !0), a = r.get(["tooltip", "show"], !0);
    !n || !n.model || i === "none" || i === !1 || i === "item" || a === !1 || r.get(["axisPointer", "show"], !0) === !1 || T(e.coordSysAxesInfo[Ma(n.model)], function(o) {
      var s = o.axis;
      n.getAxis(s.dim) === s && (o.seriesModels.push(r), o.seriesDataCount == null && (o.seriesDataCount = 0), o.seriesDataCount += r.getData().count());
    });
  });
}
function _L(e, t) {
  for (var r = t.model, n = t.dim, i = 0; i < e.length; i++) {
    var a = e[i] || {};
    if (Bl(a[n + "AxisId"], r.id) || Bl(a[n + "AxisIndex"], r.componentIndex) || Bl(a[n + "AxisName"], r.name))
      return i;
  }
}
function Bl(e, t) {
  return e === "all" || B(e) && st(e, t) >= 0 || e === t;
}
function SL(e) {
  var t = Bc(e);
  if (t) {
    var r = t.axisPointerModel, n = t.axis.scale, i = r.option, a = r.get("status"), o = r.get("value");
    o != null && (o = n.parse(o));
    var s = eh(r);
    a == null && (i.status = s ? "show" : "hide");
    var u = n.getExtent();
    // Pick a value on axis when initializing.
    (o == null || o > u[1]) && (o = u[1]), o < u[0] && (o = u[0]), i.value = o, s && (i.status = t.axis.scale.isBlank() ? "hide" : "show");
  }
}
function Bc(e) {
  var t = (e.ecModel.getComponent("axisPointer") || {}).coordSysAxesInfo;
  return t && t.axesInfo[Ma(e)];
}
function bL(e) {
  var t = Bc(e);
  return t && t.axisPointerModel;
}
function eh(e) {
  return !!e.get(["handle", "show"]);
}
function Ma(e) {
  return e.type + "||" + e.id;
}
var ng = {}, wL = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.render = function(r, n, i, a) {
      this.axisPointerClass && SL(r), e.prototype.render.apply(this, arguments), this._doUpdateAxisPointerClass(r, i, !0);
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
        var o = bL(r);
        o ? (this._axisPointer || (this._axisPointer = new a())).render(r, o, n, i) : this._disposeAxisPointer(n);
      }
    }, t.prototype._disposeAxisPointer = function(r) {
      this._axisPointer && this._axisPointer.dispose(r), this._axisPointer = null;
    }, t.registerAxisPointerClass = function(r, n) {
      ng[r] = n;
    }, t.getAxisPointerClass = function(r) {
      return r && ng[r];
    }, t.type = "axis", t;
  })(se)
), un = dt(), ig = Z, Nl = ht, TL = (
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
          var v = Tt(ag, r, h);
          this.updatePointerEl(s, l, v), this.updateLabelEl(s, l, v, r);
        }
        sg(s, r, !0), this._renderHandle(a);
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
        if (a && bu(i).w > s)
          return !0;
        if (o) {
          var u = Bc(t).seriesDataCount, l = i.getExtent();
          return Math.abs(l[0] - l[1]) / u > s;
        }
        return !1;
      }
      return n === !0;
    }, e.prototype.makeElOption = function(t, r, n, i, a) {
    }, e.prototype.createPointerEl = function(t, r, n, i) {
      var a = r.pointer;
      if (a) {
        var o = un(t).pointerEl = new KC[a.type](ig(r.pointer));
        t.add(o);
      }
    }, e.prototype.createLabelEl = function(t, r, n, i) {
      if (r.label) {
        var a = un(t).labelEl = new Kt(ig(r.label));
        t.add(a), og(a, i);
      }
    }, e.prototype.updatePointerEl = function(t, r, n) {
      var i = un(t).pointerEl;
      i && r.pointer && (i.setStyle(r.pointer.style), n(i, {
        shape: r.pointer.shape
      }));
    }, e.prototype.updateLabelEl = function(t, r, n, i) {
      var a = un(t).labelEl;
      a && (a.setStyle(r.label.style), n(a, {
        // Consider text length change in vertical axis, animation should
        // be used on shape, otherwise the effect will be weird.
        // TODOTODO
        // shape: elOption.label.shape,
        x: r.label.x,
        y: r.label.y
      }), og(a, i));
    }, e.prototype._renderHandle = function(t) {
      if (!(this._dragging || !this.updateHandleTransform)) {
        var r = this._axisPointerModel, n = this._api.getZr(), i = this._handle, a = r.getModel("handle"), o = r.get("status");
        if (!a.get("show") || !o || o === "hide") {
          i && n.remove(i), this._handle = null;
          return;
        }
        var s;
        this._handle || (s = !0, i = this._handle = su(a.get("icon"), {
          cursor: "move",
          draggable: !0,
          onmousemove: function(l) {
            my(l.event);
          },
          onmousedown: Nl(this._onHandleDragMove, this, 0, 0),
          drift: Nl(this._onHandleDragMove, this),
          ondragend: Nl(this._onHandleDragEnd, this)
        }), n.add(i)), sg(i, r, !1), i.setStyle(a.getItemStyle(null, ["color", "borderColor", "borderWidth", "opacity", "shadowColor", "shadowBlur", "shadowOffsetX", "shadowOffsetY"]));
        var u = a.get("size");
        B(u) || (u = [u, u]), i.scaleX = u[0] / 2, i.scaleY = u[1] / 2, W_(this, "_doDispatchAxisPointer", a.get("throttle") || 0, "fixRate"), this._moveHandleToValue(t, s);
      }
    }, e.prototype._moveHandleToValue = function(t, r) {
      ag(this._axisPointerModel, !r && this._moveAnimation, this._handle, Fl(this.getHandleTransform(t, this._axisModel, this._axisPointerModel)));
    }, e.prototype._onHandleDragMove = function(t, r) {
      var n = this._handle;
      if (n) {
        this._dragging = !0;
        var i = this.updateHandleTransform(Fl(n), [t, r], this._axisModel, this._axisPointerModel);
        this._payloadInfo = i, n.stopAnimation(), n.attr(Fl(i)), un(n).lastProp = null, this._doDispatchAxisPointer();
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
      r && n && (this._lastGraphicKey = null, n && r.remove(n), i && r.remove(i), this._group = null, this._handle = null, this._payloadInfo = null), Hf(this, "_doDispatchAxisPointer");
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
function ag(e, t, r, n) {
  hS(un(r).lastProp, n) || (un(r).lastProp = n, t ? pi(r, n, e) : (r.stopAnimation(), r.attr(n)));
}
function hS(e, t) {
  if (F(e) && F(t)) {
    var r = !0;
    return T(t, function(n, i) {
      r = r && hS(e[i], n);
    }), !!r;
  } else
    return e === t;
}
function og(e, t) {
  e[t.get(["label", "show"]) ? "show" : "hide"]();
}
function Fl(e) {
  return {
    x: e.x || 0,
    y: e.y || 0,
    rotation: e.rotation || 0
  };
}
function sg(e, t, r) {
  var n = t.get("z"), i = t.get("zlevel");
  e && e.traverse(function(a) {
    a.type !== "group" && (n != null && (a.z = n), i != null && (a.zlevel = i), a.silent = r);
  });
}
function CL(e) {
  var t = e.get("type"), r = e.getModel(t + "Style"), n;
  return t === "line" ? (n = r.getLineStyle(), n.fill = null) : t === "shadow" && (n = r.getAreaStyle(), n.stroke = null), n;
}
function xL(e, t, r, n, i) {
  var a = r.get("value"), o = cS(a, t.axis, t.ecModel, r.get("seriesDataIndices"), {
    precision: r.get(["label", "precision"]),
    formatter: r.get(["label", "formatter"])
  }), s = r.getModel("label"), u = Fa(s.get("padding") || 0), l = s.getFont(), f = zy(o, l), h = i.position, v = f.width + u[1] + u[3], c = f.height + u[0] + u[2], d = i.align;
  d === "right" && (h[0] -= v), d === "center" && (h[0] -= v / 2);
  var y = i.verticalAlign;
  y === "bottom" && (h[1] -= c), y === "middle" && (h[1] -= c / 2), ML(h, v, c, n);
  var p = s.get("backgroundColor");
  (!p || p === "auto") && (p = t.get(["axisLine", "lineStyle", "color"])), e.label = {
    // shape: {x: 0, y: 0, width: width, height: height, r: labelModel.get('borderRadius')},
    x: h[0],
    y: h[1],
    style: Tn(s, {
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
function ML(e, t, r, n) {
  var i = n.getWidth(), a = n.getHeight();
  e[0] = Math.min(e[0] + t, i) - t, e[1] = Math.min(e[1] + r, a) - r, e[0] = Math.max(e[0], 0), e[1] = Math.max(e[1], 0);
}
function cS(e, t, r, n, i) {
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
      value: Ps(t, {
        value: e
      }),
      axisDimension: t.dim,
      axisIndex: t.index,
      seriesData: []
    };
    T(n, function(u) {
      var l = r.getSeriesByIndex(u.seriesIndex), f = u.dataIndexInside, h = l && l.getDataParams(f);
      h && s.seriesData.push(h);
    }), N(o) ? a = o.replace("{value}", a) : X(o) && (a = o(s));
  }
  return a;
}
function vS(e, t, r) {
  var n = Se();
  return Gs(n, n, r.rotation), rs(n, n, r.position), Vh([e.dataToCoord(t), (r.labelOffset || 0) + (r.labelDirection || 1) * (r.labelMargin || 0)], n);
}
function DL(e, t, r, n, i, a) {
  var o = Ar.innerTextLayout(r.rotation, 0, r.labelDirection);
  r.labelMargin = i.get(["label", "margin"]), xL(t, n, i, a, {
    position: vS(n.axis, e, r),
    align: o.textAlign,
    verticalAlign: o.textVerticalAlign
  });
}
function IL(e, t, r) {
  return r = r || 0, {
    x1: e[r],
    y1: e[1 - r],
    x2: t[r],
    y2: t[1 - r]
  };
}
function AL(e, t, r) {
  return r = r || 0, {
    x: e[r],
    y: e[1 - r],
    width: t[r],
    height: t[1 - r]
  };
}
function mR(e, t, r, n, i, a) {
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
function PL(e, t, r) {
  return bu(e, {
    fromStat: {
      sers: H(t, function(n) {
        return r.getSeriesByIndex(n.seriesIndex);
      })
    },
    min: 1
  }).w;
}
function LL(e, t, r) {
  return [gt(Jt(t[0], t[1]), e - r / 2), Jt(e + r / 2, gt(t[0], t[1]))];
}
var EL = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      return e !== null && e.apply(this, arguments) || this;
    }
    return t.prototype.makeElOption = function(r, n, i, a, o) {
      var s = i.axis, u = s.grid, l = a.get("type"), f = s.getGlobalExtent(), h = ug(u, s).getOtherAxis(s).getGlobalExtent(), v = s.toGlobalCoord(s.dataToCoord(n, !0));
      if (l && l !== "none") {
        var c = CL(a), d = RL[l](s, v, f, h, a.get("seriesDataIndices"), a.ecModel);
        d.style = c, r.graphicKey = d.type, r.pointer = d;
      }
      var y = Bs(u.getRect(), i);
      DL(n, r, y, i, a, o);
    }, t.prototype.getHandleTransform = function(r, n, i) {
      var a = Bs(n.axis.grid.getRect(), n, {
        labelInside: !1
      });
      a.labelMargin = i.get(["handle", "margin"]);
      var o = vS(n.axis, r, a);
      return {
        x: o[0],
        y: o[1],
        rotation: a.rotation + (a.labelDirection < 0 ? Math.PI : 0)
      };
    }, t.prototype.updateHandleTransform = function(r, n, i, a) {
      var o = i.axis, s = o.grid, u = o.getGlobalExtent(!0), l = ug(s, o).getOtherAxis(o).getGlobalExtent(), f = o.dim === "x" ? 0 : 1, h = [r.x, r.y];
      h[f] += n[f], h[f] = Jt(u[1], h[f]), h[f] = gt(u[0], h[f]);
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
  })(TL)
);
function ug(e, t) {
  var r = {};
  return r[t.dim + "AxisIndex"] = t.index, e.getCartesian(r);
}
var RL = {
  line: function(e, t, r, n) {
    var i = IL([t, n[0]], [t, n[1]], lg(e));
    return {
      type: "Line",
      subPixelOptimize: !0,
      shape: i
    };
  },
  shadow: function(e, t, r, n, i, a) {
    var o = PL(e, i, a), s = n[1] - n[0], u = LL(t, r, o), l = u[0], f = u[1];
    return {
      type: "Rect",
      shape: AL([l, n[0]], [f - l, s], lg(e))
    };
  }
};
function lg(e) {
  return e.dim === "x" ? 0 : 1;
}
var OL = (
  /** @class */
  (function(e) {
    z(t, e);
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
    }, t;
  })(at)
), sr = dt(), kL = T;
function dS(e, t, r) {
  if (!W.node) {
    var n = t.getZr();
    sr(n).records || (sr(n).records = {}), BL(n, t);
    var i = sr(n).records[e] || (sr(n).records[e] = {});
    i.handler = r;
  }
}
function BL(e, t) {
  if (sr(e).initialized)
    return;
  sr(e).initialized = !0, r("click", Tt(zl, "click")), r("mousemove", Tt(zl, "mousemove")), r("mousewheel", Tt(zl, "mousewheel")), r("globalout", FL);
  function r(n, i) {
    e.on(n, function(a) {
      var o = zL(t);
      kL(sr(e).records, function(s) {
        s && i(s, a, o.dispatchAction);
      }), NL(o.pendings, t);
    });
  }
}
function NL(e, t) {
  var r = e.showTip.length, n = e.hideTip.length, i;
  r ? i = e.showTip[r - 1] : n && (i = e.hideTip[n - 1]), i && (i.dispatchAction = null, t.dispatchAction(i));
}
function FL(e, t, r) {
  e.handler("leave", null, r);
}
function zl(e, t, r, n) {
  t.handler(e, r, n);
}
function zL(e) {
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
function rh(e, t) {
  if (!W.node) {
    var r = t.getZr(), n = (sr(r).records || {})[e];
    n && (sr(r).records[e] = null);
  }
}
var HL = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.render = function(r, n, i) {
      var a = n.getComponent("tooltip"), o = r.get("triggerOn") || a && a.get("triggerOn") || "mousemove|click|mousewheel";
      dS("axisPointer", i, function(s, u, l) {
        o !== "none" && (s === "leave" || o.indexOf(s) >= 0) && l({
          type: "updateAxisPointer",
          currTrigger: s,
          x: u && u.offsetX,
          y: u && u.offsetY
        });
      });
    }, t.prototype.remove = function(r, n) {
      rh("axisPointer", n);
    }, t.prototype.dispose = function(r, n) {
      rh("axisPointer", n);
    }, t.type = "axisPointer", t;
  })(se)
);
function pS(e, t) {
  var r = [], n = e.seriesIndex, i;
  if (n == null || !(i = t.getSeriesByIndex(n)))
    return {
      point: []
    };
  var a = i.getData(), o = Ea(a, e);
  if (o == null || o < 0 || B(o))
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
var fg = dt();
function VL(e, t, r) {
  var n = e.currTrigger, i = [e.x, e.y], a = e, o = e.dispatchAction || ht(r.dispatchAction, r), s = t.getComponent("axisPointer").coordSysAxesInfo;
  if (s) {
    Ko(i) && (i = pS({
      seriesIndex: a.seriesIndex,
      // Do not use dataIndexInside from other ec instance.
      // FIXME: auto detect it?
      dataIndex: a.dataIndex
    }, t).point);
    var u = Ko(i), l = a.axesInfo, f = s.axesInfo, h = n === "leave" || Ko(i), v = {}, c = {}, d = {
      list: [],
      map: {}
    }, y = {
      showPointer: Tt(UL, c),
      showTooltip: Tt(YL, d)
    };
    T(s.coordSysMap, function(g, m) {
      var _ = u || g.containPoint(i);
      T(s.coordSysAxesInfo[m], function(S, b) {
        var w = S.axis, C = $L(l, S);
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
    }), WL(c, f, v), XL(d, i, e, o), qL(f, o, r), v;
  }
}
function hg(e, t, r, n, i) {
  var a = e.axis;
  if (!(a.scale.isBlank() || !a.containData(t))) {
    if (!e.involveSeries) {
      r.showPointer(e, t);
      return;
    }
    var o = GL(t, e), s = o.payloadBatch, u = o.snapToValue;
    s[0] && i.seriesIndex == null && R(i, s[0]), !n && e.snap && a.containData(u) && u != null && (t = u), r.showPointer(e, t, s), r.showTooltip(e, o, u);
  }
}
function GL(e, t) {
  var r = t.axis, n = r.dim, i = e, a = [], o = Number.MAX_VALUE, s = -1;
  return T(t.seriesModels, function(u, l) {
    var f = u.getData().mapDimensionsAll(n), h, v;
    if (u.getAxisTooltipData) {
      var c = u.getAxisTooltipData(f, e, r);
      v = c.dataIndices, h = c.nestestValue;
    } else {
      if (v = u.indicesOfNearest(
        n,
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
    if (fr(h)) {
      var d = e - h, y = Math.abs(d);
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
function UL(e, t, r, n) {
  e[t.key] = {
    value: r,
    payloadBatch: n
  };
}
function YL(e, t, r, n) {
  var i = r.payloadBatch, a = t.axis, o = a.model, s = t.axisPointerModel;
  if (!(!t.triggerTooltip || !i.length)) {
    var u = t.coordSys.model, l = Ma(u), f = e.map[l];
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
function WL(e, t, r) {
  var n = r.axesInfo = [];
  T(t, function(i, a) {
    var o = i.axisPointerModel.option, s = e[a];
    s ? (!i.useHandle && (o.status = "show"), o.value = s.value, o.seriesDataIndices = (s.payloadBatch || []).slice()) : !i.useHandle && (o.status = "hide"), o.status === "show" && n.push({
      axisDim: i.axis.dim,
      axisIndex: i.axis.model.componentIndex,
      value: o.value
    });
  });
}
function XL(e, t, r, n) {
  if (Ko(t) || !e.list.length) {
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
function qL(e, t, r) {
  var n = r.getZr(), i = "axisPointerLastHighlights", a = fg(n)[i] || {}, o = fg(n)[i] = {};
  T(e, function(f, h) {
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
function $L(e, t) {
  for (var r = 0; r < (e || []).length; r++) {
    var n = e[r];
    if (t.axis.dim === n.axisDim && t.axis.model.componentIndex === n.axisIndex)
      return n;
  }
}
function cg(e) {
  var t = e.axis.model, r = {}, n = r.axisDim = e.axis.dim;
  return r.axisIndex = r[n + "AxisIndex"] = t.componentIndex, r.axisName = r[n + "AxisName"] = t.name, r.axisId = r[n + "AxisId"] = t.id, r;
}
function Ko(e) {
  return !e || e[0] == null || isNaN(e[0]) || e[1] == null || isNaN(e[1]);
}
function ZL(e) {
  wL.registerAxisPointerClass("CartesianAxisPointer", EL), e.registerComponentModel(OL), e.registerComponentView(HL), e.registerPreprocessor(function(t) {
    if (t) {
      (!t.axisPointer || t.axisPointer.length === 0) && (t.axisPointer = {});
      var r = t.axisPointer.link;
      r && !B(r) && (t.axisPointer.link = [r]);
    }
  }), e.registerProcessor(e.PRIORITY.PROCESSOR.STATISTIC, {
    overallReset: function(t, r) {
      t.getComponent("axisPointer").coordSysAxesInfo = pL(t, r);
    }
  }), e.registerAction({
    type: "updateAxisPointer",
    event: "updateAxisPointer",
    update: ":updateAxisPointer"
  }, VL);
}
function KL(e, t) {
  var r = Fa(t.get("padding")), n = t.getItemStyle(["color", "opacity"]);
  n.fill = t.get("backgroundColor");
  var i = new Rt({
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
  });
  return i;
}
var jL = (
  /** @class */
  (function(e) {
    z(t, e);
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
    }, t;
  })(at)
);
function gS(e) {
  var t = e.get("confine");
  return t != null ? !!t : e.get("renderMode") === "richText";
}
function yS(e) {
  if (W.domSupported) {
    for (var t = document.documentElement.style, r = 0, n = e.length; r < n; r++)
      if (e[r] in t)
        return e[r];
  }
}
var mS = yS(["transform", "webkitTransform", "OTransform", "MozTransform", "msTransform"]), QL = yS(["webkitTransition", "transition", "OTransition", "MozTransition", "msTransition"]);
function _S(e, t) {
  if (!e)
    return t;
  t = ic(t, !0);
  var r = e.indexOf(t);
  return e = r === -1 ? t : "-" + e.slice(0, r) + "-" + t, e.toLowerCase();
}
function JL(e, t) {
  var r = e.currentStyle || document.defaultView && document.defaultView.getComputedStyle(e);
  return r ? r[t] : null;
}
var tE = _S(QL, "transition"), Nc = _S(mS, "transform"), eE = "position:absolute;display:block;border-style:solid;white-space:nowrap;z-index:9999999;" + (W.transform3dSupported ? "will-change:transform;" : "");
function rE(e) {
  return e = e === "left" ? "right" : e === "right" ? "left" : e === "top" ? "bottom" : "top", e;
}
function nE(e, t, r) {
  if (!N(r) || r === "inside")
    return "";
  var n = e.get("backgroundColor"), i = e.get("borderWidth");
  t = ma(t);
  var a = rE(r), o = Math.max(Math.round(i) * 1.5, 6), s = "", u = Nc + ":", l;
  st(["left", "right"], a) > -1 ? (s += "top:50%", u += "translateY(-50%) rotate(" + (l = a === "left" ? -225 : -45) + "deg)") : (s += "left:50%", u += "translateX(-50%) rotate(" + (l = a === "top" ? 225 : 45) + "deg)");
  var f = l * Math.PI / 180, h = o + i, v = h * Math.abs(Math.cos(f)) + h * Math.abs(Math.sin(f)), c = Math.round(((v - Math.SQRT2 * i) / 2 + Math.SQRT2 * i - (v - h) / 2) * 100) / 100;
  s += ";" + a + ":-" + c + "px";
  var d = t + " solid " + i + "px;", y = ["position:absolute;width:" + o + "px;height:" + o + "px;z-index:-1;", s + ";" + u + ";", "border-bottom:" + d, "border-right:" + d, "background-color:" + n + ";"];
  return '<div style="' + y.join("") + '"></div>';
}
function iE(e, t, r) {
  var n = "cubic-bezier(0.23,1,0.32,1)", i = "", a = "";
  return r && (i = " " + e / 2 + "s " + n, a = "opacity" + i + ",visibility" + i), t || (i = " " + e + "s " + n, a += (a.length ? "," : "") + (W.transformSupported ? "" + Nc + i : ",left" + i + ",top" + i)), tE + ":" + a;
}
function vg(e, t, r) {
  var n = e.toFixed(0) + "px", i = t.toFixed(0) + "px";
  if (!W.transformSupported)
    return r ? "top:" + i + ";left:" + n + ";" : [["top", i], ["left", n]];
  var a = W.transform3dSupported, o = "translate" + (a ? "3d" : "") + "(" + n + "," + i + (a ? ",0" : "") + ")";
  return r ? "top:0;left:0;" + Nc + ":" + o + ";" : [["top", 0], ["left", 0], [mS, o]];
}
function aE(e) {
  var t = [], r = e.get("fontSize"), n = e.getTextColor();
  n && t.push("color:" + n), t.push("font:" + e.getFont());
  var i = G(e.get("lineHeight"), Math.round(r * 3 / 2));
  r && t.push("line-height:" + i + "px");
  var a = e.get("textShadowColor"), o = e.get("textShadowBlur") || 0, s = e.get("textShadowOffsetX") || 0, u = e.get("textShadowOffsetY") || 0;
  return a && o && t.push("text-shadow:" + s + "px " + u + "px " + o + "px " + a), T(["decoration", "align"], function(l) {
    var f = e.get(l);
    f && t.push("text-" + l + ":" + f);
  }), t.join(";");
}
function oE(e, t, r, n) {
  var i = [], a = e.get("transitionDuration"), o = e.get("backgroundColor"), s = e.get("shadowBlur"), u = e.get("shadowColor"), l = e.get("shadowOffsetX"), f = e.get("shadowOffsetY"), h = e.getModel("textStyle"), v = U_(e, "html"), c = l + "px " + f + "px " + s + "px " + u;
  return i.push("box-shadow:" + c), t && a > 0 && i.push(iE(a, r, n)), o && i.push("background-color:" + o), T(["width", "color", "radius"], function(d) {
    var y = "border-" + d, p = ic(y), g = e.get(p);
    g != null && i.push(y + ":" + g + (d === "color" ? "" : "px"));
  }), i.push(aE(h)), v != null && i.push("padding:" + Fa(v).join("px ") + "px"), i.join(";") + ";";
}
function dg(e, t, r, n, i) {
  var a = t && t.painter;
  if (r) {
    var o = a && a.getViewportRoot();
    o && x1(e, o, r, n, i);
  } else {
    e[0] = n, e[1] = i;
    var s = a && a.getViewportRootOffset();
    s && (e[0] += s.offsetLeft, e[1] += s.offsetTop);
  }
  e[2] = e[0] / t.getWidth(), e[3] = e[1] / t.getHeight();
}
var sE = (
  /** @class */
  (function() {
    function e(t, r) {
      if (this._show = !1, this._styleCoord = [0, 0, 0, 0], this._enterable = !0, this._alwaysShowContent = !1, this._firstShow = !0, this._longHide = !0, W.wxa)
        return null;
      var n = document.createElement("div");
      n.domBelongToZr = !0, this.el = n;
      var i = this._zr = t.getZr(), a = r.appendTo, o = a && (N(a) ? document.querySelector(a) : oi(a) ? a : X(a) && a(t.getDom()));
      dg(this._styleCoord, i, o, t.getWidth() / 2, t.getHeight() / 2), (o || t.getDom()).appendChild(n), this._api = t, this._container = o;
      var s = this;
      n.onmouseenter = function() {
        s._enterable && (clearTimeout(s._hideTimeout), s._show = !0), s._inContent = !0;
      }, n.onmousemove = function(u) {
        if (u = u || window.event, !s._enterable) {
          var l = i.handler, f = i.painter.getViewportRoot();
          ce(f, u, !0), l.dispatch("mousemove", u);
        }
      }, n.onmouseleave = function() {
        s._inContent = !1, s._enterable && s._show && s.hideLater(s._hideDelay);
      };
    }
    return e.prototype.update = function(t) {
      if (!this._container) {
        var r = this._api.getDom(), n = JL(r, "position"), i = r.style;
        i.position !== "absolute" && n !== "absolute" && (i.position = "relative");
      }
      var a = t.get("alwaysShowContent");
      a && this._moveIfResized(), this._alwaysShowContent = a, this._enableDisplayTransition = t.get("displayTransition") && t.get("transitionDuration") > 0, this.el.className = t.get("className") || "";
    }, e.prototype.show = function(t, r) {
      clearTimeout(this._hideTimeout), clearTimeout(this._longHideTimeout);
      var n = this.el, i = n.style, a = this._styleCoord;
      n.innerHTML ? i.cssText = eE + oE(t, !this._firstShow, this._longHide, this._enableDisplayTransition) + vg(a[0], a[1], !0) + ("border-color:" + ma(r) + ";") + (t.get("extraCssText") || "") + (";pointer-events:" + (this._enterable ? "auto" : "none")) : i.display = "none", this._show = !0, this._firstShow = !1, this._longHide = !1;
    }, e.prototype.setContent = function(t, r, n, i, a) {
      var o = this.el;
      if (t == null) {
        o.innerHTML = "";
        return;
      }
      var s = "";
      if (N(a) && n.get("trigger") === "item" && !gS(n) && (s = nE(n, i, a)), N(t))
        o.innerHTML = t + s;
      else if (t) {
        o.innerHTML = "", B(t) || (t = [t]);
        for (var u = 0; u < t.length; u++)
          oi(t[u]) && t[u].parentNode !== o && o.appendChild(t[u]);
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
        if (dg(n, this._zr, this._container, t, r), n[0] != null && n[1] != null) {
          var i = this.el.style, a = vg(n[0], n[1]);
          T(a, function(o) {
            i[o[0]] = o[1];
          });
        }
      }
    }, e.prototype._moveIfResized = function() {
      var t = this._styleCoord[2], r = this._styleCoord[3];
      this.moveTo(t * this._zr.getWidth(), r * this._zr.getHeight());
    }, e.prototype.hide = function() {
      var t = this, r = this.el.style;
      this._enableDisplayTransition ? (r.visibility = "hidden", r.opacity = "0") : r.display = "none", W.transform3dSupported && (r.willChange = ""), this._show = !1, this._longHideTimeout = setTimeout(function() {
        return t._longHide = !0;
      }, 500);
    }, e.prototype.hideLater = function(t) {
      this._show && !(this._inContent && this._enterable) && !this._alwaysShowContent && (t ? (this._hideDelay = t, this._show = !1, this._hideTimeout = setTimeout(ht(this.hide, this), t)) : this.hide());
    }, e.prototype.isShow = function() {
      return this._show;
    }, e.prototype.dispose = function() {
      clearTimeout(this._hideTimeout), clearTimeout(this._longHideTimeout);
      var t = this._zr;
      M1(t && t.painter && t.painter.getViewportRoot(), this._container);
      var r = this.el;
      if (r) {
        r.onmouseenter = r.onmousemove = r.onmouseleave = null;
        var n = r.parentNode;
        n && n.removeChild(r);
      }
      this.el = this._container = null;
    }, e;
  })()
), uE = (
  /** @class */
  (function() {
    function e(t) {
      this._show = !1, this._styleCoord = [0, 0, 0, 0], this._alwaysShowContent = !1, this._enterable = !0, this._zr = t.getZr(), gg(this._styleCoord, this._zr, t.getWidth() / 2, t.getHeight() / 2);
    }
    return e.prototype.update = function(t) {
      var r = t.get("alwaysShowContent");
      r && this._moveIfResized(), this._alwaysShowContent = r;
    }, e.prototype.show = function() {
      this._hideTimeout && clearTimeout(this._hideTimeout), this.el.show(), this._show = !0;
    }, e.prototype.setContent = function(t, r, n, i, a) {
      var o = this;
      F(t) && Yt(""), this.el && this._zr.remove(this.el);
      var s = n.getModel("textStyle");
      this.el = new Kt({
        style: {
          rich: r.richTextStyles,
          text: t,
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
    }, e.prototype.setEnterable = function(t) {
      this._enterable = t;
    }, e.prototype.getSize = function() {
      var t = this.el, r = this.el.getBoundingRect(), n = pg(t.style);
      return [r.width + n.left + n.right, r.height + n.top + n.bottom];
    }, e.prototype.moveTo = function(t, r) {
      var n = this.el;
      if (n) {
        var i = this._styleCoord;
        gg(i, this._zr, t, r), t = i[0], r = i[1];
        var a = n.style, o = Sr(a.borderWidth || 0), s = pg(a);
        n.x = t + o + s.left, n.y = r + o + s.top, n.markRedraw();
      }
    }, e.prototype._moveIfResized = function() {
      var t = this._styleCoord[2], r = this._styleCoord[3];
      this.moveTo(t * this._zr.getWidth(), r * this._zr.getHeight());
    }, e.prototype.hide = function() {
      this.el && this.el.hide(), this._show = !1;
    }, e.prototype.hideLater = function(t) {
      this._show && !(this._inContent && this._enterable) && !this._alwaysShowContent && (t ? (this._hideDelay = t, this._show = !1, this._hideTimeout = setTimeout(ht(this.hide, this), t)) : this.hide());
    }, e.prototype.isShow = function() {
      return this._show;
    }, e.prototype.dispose = function() {
      this._zr.remove(this.el);
    }, e;
  })()
);
function Sr(e) {
  return Math.max(0, e);
}
function pg(e) {
  var t = Sr(e.shadowBlur || 0), r = Sr(e.shadowOffsetX || 0), n = Sr(e.shadowOffsetY || 0);
  return {
    left: Sr(t - r),
    right: Sr(t + r),
    top: Sr(t - n),
    bottom: Sr(t + n)
  };
}
function gg(e, t, r, n) {
  e[0] = r, e[1] = n, e[2] = e[0] / t.getWidth(), e[3] = e[1] / t.getHeight();
}
var lE = new Rt({
  shape: {
    x: -1,
    y: -1,
    width: 2,
    height: 2
  }
}), fE = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.init = function(r, n) {
      if (!(W.node || !n.getDom())) {
        var i = r.getComponent("tooltip"), a = this._renderMode = Dw(i.get("renderMode"));
        this._tooltipContent = a === "richText" ? new uE(n) : new sE(n, {
          appendTo: i.get("appendToBody", !0) ? "body" : i.get("appendTo", !0)
        });
      }
    }, t.prototype.render = function(r, n, i) {
      if (!(W.node || !i.getDom())) {
        this.group.removeAll(), this._tooltipModel = r, this._ecModel = n, this._api = i;
        var a = this._tooltipContent;
        a.update(r), a.setEnterable(r.get("enterable")), this._initGlobalListener(), this._keepShow(), this._renderMode !== "richText" && r.get("transitionDuration") ? W_(this, "_updatePosition", 50, "fixRate") : Hf(this, "_updatePosition");
      }
    }, t.prototype._initGlobalListener = function() {
      var r = this._tooltipModel, n = r.get("triggerOn");
      dS("itemTooltip", this._api, ht(function(i, a, o) {
        n !== "none" && (n.indexOf(i) >= 0 ? this._tryShow(a, o) : i === "leave" && this._hide(o));
      }, this));
    }, t.prototype._keepShow = function() {
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
    }, t.prototype.manuallyShowTip = function(r, n, i, a) {
      if (!(a.from === this.uid || W.node || !i.getDom())) {
        var o = yg(a, i);
        this._ticket = "";
        var s = a.dataByCoordSys, u = dE(a, n, i);
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
          var f = lE;
          f.x = a.x, f.y = a.y, f.update(), ft(f).tooltipConfig = {
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
          var h = pS(a, n), v = h.point[0], c = h.point[1];
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
      this._tooltipModel && o.hideLater(this._tooltipModel.get("hideDelay")), this._lastX = this._lastY = this._lastDataByCoordSys = null, this._cbParamsList = null, a.from !== this.uid && this._hide(yg(a, i));
    }, t.prototype._manuallyAxisShowTip = function(r, n, i, a) {
      var o = a.seriesIndex, s = a.dataIndex, u = n.getComponent("axisPointer").coordSysAxesInfo;
      if (!(o == null || s == null || u == null)) {
        var l = n.getSeriesByIndex(o);
        if (l) {
          var f = l.getData(), h = Gi([f.getItemModel(s), l, (l.coordinateSystem || {}).model], this._tooltipModel);
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
          var s = ft(i);
          if (s.ssrType === "legend")
            return;
          this._lastDataByCoordSys = null, this._cbParamsList = null;
          var u, l;
          Ki(i, function(f) {
            if (f.tooltipDisabled)
              return u = l = null, !0;
            u || l || (ft(f).dataIndex != null ? u = f : ft(f).tooltipConfig != null && (l = f));
          }, !0), u ? this._showSeriesItemTooltip(r, u, n) : l ? this._showComponentItemTooltip(r, l, n) : this._hide(n);
        } else
          this._lastDataByCoordSys = null, this._cbParamsList = null, this._hide(n);
      }
    }, t.prototype._showOrMove = function(r, n) {
      var i = r.get("showDelay");
      n = ht(n, this), clearTimeout(this._showTimout), i > 0 ? this._showTimout = setTimeout(n, i) : n();
    }, t.prototype._showAxisTooltip = function(r, n) {
      var i = this._ecModel, a = this._tooltipModel, o = [n.offsetX, n.offsetY], s = Gi([n.tooltipOption], a), u = this._renderMode, l = [], f = ba("section", {
        blocks: [],
        noHeader: !0
      }), h = [], v = new bl();
      T(r, function(m) {
        T(m.dataByAxis, function(_) {
          var S = i.getComponent(_.axisDim + "Axis", _.axisIndex), b = _.value, w = S.axis, C = w.scale.parse(b);
          if (!(!S || b == null)) {
            var M = cS(b, w, i, _.seriesDataIndices, _.valueLabelOpt), x = ba("section", {
              header: M,
              noHeader: !Me(M),
              sortBlocks: !0,
              blocks: []
            });
            f.blocks.push(x), T(_.seriesDataIndices, function(D) {
              var I = i.getSeriesByIndex(D.seriesIndex), L = D.dataIndexInside, A = I.getDataParams(L);
              if (!(A.dataIndex < 0)) {
                A.axisDim = _.axisDim, A.axisIndex = _.axisIndex, A.axisType = _.axisType, A.axisId = _.axisId, A.axisValue = Ps(S.axis, {
                  value: C
                }), A.axisValueLabel = M, A.marker = v.makeTooltipMarker("item", ma(A.color), u);
                var P = Yd(I.formatTooltip(L, !0, null)), k = P.frag;
                if (k) {
                  var O = Gi([I], a).get("valueFormatter");
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
    }, t.prototype._showSeriesItemTooltip = function(r, n, i) {
      var a = this._ecModel, o = ft(n), s = o.seriesIndex, u = a.getSeriesByIndex(s), l = o.dataModel || u, f = o.dataIndex, h = o.dataType, v = l.getData(h), c = this._renderMode, d = r.positionDefault, y = Gi([v.getItemModel(f), l, u && (u.coordinateSystem || {}).model], this._tooltipModel, d ? {
        position: d
      } : null), p = y.get("trigger");
      if (!(p != null && p !== "item")) {
        var g = l.getDataParams(f, h), m = new bl();
        g.marker = m.makeTooltipMarker("item", ma(g.color), c);
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
    }, t.prototype._showComponentItemTooltip = function(r, n, i) {
      var a = this._renderMode === "html", o = ft(n), s = o.tooltipConfig, u = s.option || {}, l = u.encodeHTMLContent;
      if (N(u)) {
        var f = u;
        u = {
          content: f,
          // Fixed formatter
          formatter: f
        }, l = !0;
      }
      l && a && u.content && (u = Z(u), u.content = Ut(u.content));
      var h = [u], v = this._ecModel.getComponent(o.componentMainType, o.componentIndex);
      v && h.push(v), h.push({
        formatter: u.content
      });
      var c = r.positionDefault, d = Gi(h, this._tooltipModel, c ? {
        position: c
      } : null), y = d.get("content"), p = Math.random() + "", g = new bl();
      this._showOrMove(d, function() {
        var m = Z(d.get("formatterParams") || {});
        this._showTooltipContent(d, y, m, p, r.offsetX, r.offsetY, r.position, n, g);
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
        var c = n, d = this._getNearestPoint([o, s], i, r.get("trigger"), r.get("borderColor"), r.get("defaultBorderColor", !0)), y = d.color;
        if (v)
          if (N(v)) {
            var p = r.ecModel.get("useUTC"), g = B(i) ? i[0] : i, m = g && g.axisType && g.axisType.indexOf("time") >= 0;
            c = v, m && (c = Na(g.axisValue, c, p)), c = ac(c, i, !0);
          } else if (X(v)) {
            var _ = ht(function(S, b) {
              S === this._ticket && (h.setContent(b, f, r, y, u), this._updatePosition(r, u, o, s, h, i, l));
            }, this);
            this._ticket = a, c = v(i, a, _);
          } else
            c = v;
        h.setContent(c, f, r, y, u), h.show(r, y), this._updatePosition(r, u, o, s, h, i, l);
      }
    }, t.prototype._getNearestPoint = function(r, n, i, a, o) {
      if (i === "axis" || B(n))
        return {
          color: a || o
        };
      if (!B(n))
        return {
          color: a || n.color || n.borderColor
        };
    }, t.prototype._updatePosition = function(r, n, i, a, o, s, u) {
      var l = this._api.getWidth(), f = this._api.getHeight();
      n = n || r.get("position");
      var h = o.getSize(), v = r.get("align"), c = r.get("verticalAlign"), d = u && u.getBoundingRect().clone();
      if (u && d.applyTransform(u.transform), X(n) && (n = n([i, a], s, o.el, d, {
        viewSize: [l, f],
        contentSize: h.slice()
      })), B(n))
        i = xe(n[0], l), a = xe(n[1], f);
      else if (F(n)) {
        var y = n;
        y.width = h[0], y.height = h[1];
        var p = ys(y, {
          width: l,
          height: f
        });
        i = p.x, a = p.y, v = null, c = null;
      } else if (N(n) && u) {
        var g = vE(n, d, h, r.get("borderWidth"));
        i = g[0], a = g[1];
      } else {
        var g = hE(i, a, o, l, f, v ? null : 20, c ? null : 20);
        i = g[0], a = g[1];
      }
      if (v && (i -= mg(v) ? h[0] / 2 : v === "right" ? h[0] : 0), c && (a -= mg(c) ? h[1] / 2 : c === "bottom" ? h[1] : 0), gS(r)) {
        var g = cE(i, a, o, l, f);
        i = g[0], a = g[1];
      }
      o.moveTo(i, a);
    }, t.prototype._updateContentNotChangedOnAxis = function(r, n) {
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
    }, t.prototype._hide = function(r) {
      this._lastDataByCoordSys = null, this._cbParamsList = null, r({
        type: "hideTip",
        from: this.uid
      });
    }, t.prototype.dispose = function(r, n) {
      W.node || !n.getDom() || (Hf(this, "_updatePosition"), this._tooltipContent.dispose(), rh("itemTooltip", n), this._tooltipContent = null, this._tooltipModel = null, this._lastDataByCoordSys = null, this._cbParamsList = null);
    }, t.type = "tooltip", t;
  })(se)
);
function Gi(e, t, r) {
  var n = t.ecModel, i;
  r ? (i = new ct(r, n, n), i = new ct(t.option, i, n)) : i = t;
  for (var a = e.length - 1; a >= 0; a--) {
    var o = e[a];
    o && (o instanceof ct && (o = o.get("tooltip", !0)), N(o) && (o = {
      formatter: o
    }), o && (i = new ct(o, i, n)));
  }
  return i;
}
function yg(e, t) {
  return e.dispatchAction || ht(t.dispatchAction, t);
}
function hE(e, t, r, n, i, a, o) {
  var s = r.getSize(), u = s[0], l = s[1];
  return a != null && (e + u + a + 2 > n ? e -= u + a : e += a), o != null && (t + l + o > i ? t -= l + o : t += o), [e, t];
}
function cE(e, t, r, n, i) {
  var a = r.getSize(), o = a[0], s = a[1];
  return e = Math.min(e + o, n) - o, t = Math.min(t + s, i) - s, e = Math.max(e, 0), t = Math.max(t, 0), [e, t];
}
function vE(e, t, r, n) {
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
function mg(e) {
  return e === "center" || e === "middle";
}
function dE(e, t, r) {
  var n = xh(e).queryOptionMap, i = n.keys()[0];
  if (!(!i || i === "series")) {
    var a = Ra(t, i, n.get(i), {
      useDefault: !1,
      enableAll: !1,
      enableNone: !1
    }), o = a.models[0];
    if (o) {
      var s = r.getViewOfComponentModel(o), u;
      if (s.group.traverse(function(l) {
        var f = ft(l).tooltipConfig;
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
function _R(e) {
  ci(ZL), e.registerComponentModel(jL), e.registerComponentView(fE), e.registerAction({
    type: "showTip",
    event: "showTip",
    update: "tooltip:manuallyShowTip"
  }, Et), e.registerAction({
    type: "hideTip",
    event: "hideTip",
    update: "tooltip:manuallyHideTip"
  }, Et);
}
var pE = function(e, t) {
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
}, nh = (
  /** @class */
  (function(e) {
    z(t, e);
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
      n === !0 && (n = r.selector = ["all", "inverse"]), B(n) && T(n, function(a, o) {
        N(a) && (a = {
          type: a
        }), n[o] = pt(a, pE(i, a.type));
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
        f && Ch(u) && n.push(u.name);
      }), this._availableNames = i;
      var a = this.get("data") || n, o = U(), s = H(a, function(u) {
        return (N(u) || vt(u)) && (u = {
          name: u
        }), o.get(u.name) ? null : (o.set(u.name, !0), new ct(u, this, this.ecModel));
      }, this);
      this._data = xt(s, function(u) {
        return !!u;
      });
    }, t.prototype.getData = function() {
      return this._data;
    }, t.prototype.select = function(r) {
      var n = this.option.selected, i = this.get("selectedMode");
      if (i === "single") {
        var a = this._data;
        T(a, function(o) {
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
      T(r, function(i) {
        n[i.get("name", !0)] = !0;
      });
    }, t.prototype.inverseSelect = function() {
      var r = this._data, n = this.option.selected;
      T(r, function(i) {
        var a = i.get("name", !0);
        n.hasOwnProperty(a) || (n[a] = !0), n[a] = !n[a];
      });
    }, t.prototype.isSelected = function(r) {
      var n = this.option.selected;
      return !(n.hasOwnProperty(r) && !n[r]) && st(this._availableNames, r) >= 0;
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
    }, t;
  })(at)
), Xn = Tt, ih = T, Do = oe, SS = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r.newlineDisabled = !1, r;
    }
    return t.prototype.init = function() {
      this.group.add(this._contentGroup = new Do()), this.group.add(this._selectorGroup = new Do()), this._isFirstRender = !0;
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
        var f = zx(r, i).refContainer, h = r.getBoxLayoutParams(), v = r.get("padding"), c = ys(h, f, v), d = this.layoutInner(r, o, c, a, u, l), y = ys(_t({
          width: d.width,
          height: d.height
        }, h), f, v);
        this.group.x = y.x - d.x, this.group.y = y.y - d.y, this.group.markRedraw(), this.group.add(this._backgroundEl = KL(
          d,
          // FXIME: most itemStyle options does not work in background because inherit is not handled yet.
          r
        ));
      }
    }, t.prototype.resetInner = function() {
      this.getContentGroup().removeAll(), this._backgroundEl && this.group.remove(this._backgroundEl), this.getSelectorGroup().removeAll();
    }, t.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this.getContentGroup(), f = U(), h = n.get("selectedMode"), v = n.get("triggerEvent"), c = [];
      i.eachRawSeries(function(d) {
        !d.get("legendHoverLink") && c.push(d.id);
      }), ih(n.getData(), function(d, y) {
        var p = this, g = d.get("name");
        if (!this.newlineDisabled && (g === "" || g === `
`)) {
          var m = new Do();
          m.newline = !0, l.add(m);
          return;
        }
        var _ = i.getSeriesByName(g)[0];
        if (!f.get(g))
          if (_) {
            var S = _.getData(), b = S.getVisual("legendLineStyle") || {}, w = S.getVisual("legendIcon"), C = S.getVisual("style"), M = this._createItem(_, g, y, d, n, r, b, C, w, h, a);
            M.on("click", Xn(_g, g, null, a, c)).on("mouseover", Xn(ah, _.name, null, a, c)).on("mouseout", Xn(oh, _.name, null, a, c)), i.ssr && M.eachChild(function(x) {
              var D = ft(x);
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
                var L = I.indexOfName(g), A = I.getItemVisual(L, "style"), P = I.getItemVisual(L, "legendIcon"), k = ae(A.fill);
                k && k[3] === 0 && (k[3] = 0.2, A = R(R({}, A), {
                  fill: Or(k, "rgba")
                }));
                var O = this._createItem(x, g, y, d, n, r, {}, A, P, h, a);
                O.on("click", Xn(_g, null, g, a, c)).on("mouseover", Xn(ah, null, g, a, c)).on("mouseout", Xn(oh, null, g, a, c)), i.ssr && O.eachChild(function(V) {
                  var K = ft(V);
                  K.seriesIndex = x.seriesIndex, K.dataIndex = y, K.ssrType = "legend";
                }), v && O.eachChild(function(V) {
                  D.packEventData(V, n, x, y, g);
                }), f.set(g, !0);
              }
            }, this);
      }, this), o && this._createSelector(o, n, a, s, u);
    }, t.prototype.packEventData = function(r, n, i, a, o) {
      var s = {
        componentType: "legend",
        componentIndex: n.componentIndex,
        dataIndex: a,
        value: o,
        seriesIndex: i.seriesIndex
      };
      ft(r).eventData = s;
    }, t.prototype._createSelector = function(r, n, i, a, o) {
      var s = this.getSelectorGroup();
      ih(r, function(l) {
        var f = l.type, h = new Kt({
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
        jC(h, {
          normal: v,
          emphasis: c
        }, {
          defaultText: l.title
        }), ds(h);
      });
    }, t.prototype._createItem = function(r, n, i, a, o, s, u, l, f, h, v) {
      var c = r.visualDrawType, d = o.get("itemWidth"), y = o.get("itemHeight"), p = o.isSelected(n), g = a.get("symbolRotate"), m = a.get("symbolKeepAspect"), _ = a.get("icon");
      f = _ || f || "roundRect";
      var S = gE(f, a, u, l, c, p, v), b = new Do(), w = a.getModel("textStyle");
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
        b.add(yE({
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
      b.add(new Kt({
        style: Tn(w, {
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
      var A = new Rt({
        shape: b.getBoundingRect(),
        style: {
          // Cannot use 'invisible' because SVG SSR will miss the node
          fill: "transparent"
        }
      }), P = a.getModel("tooltip");
      return P.get("show") && uu({
        el: A,
        componentModel: o,
        itemName: n,
        itemTooltipOption: P.option
      }), b.add(A), b.eachChild(function(k) {
        k.silent = !0;
      }), A.silent = !h, this.getContentGroup().add(b), ds(b), b.__legendDataIndex = i, b;
    }, t.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getContentGroup(), l = this.getSelectorGroup();
      ia(r.get("orient"), u, r.get("itemGap"), i.width, i.height);
      var f = u.getBoundingRect(), h = [-f.x, -f.y];
      if (l.markRedraw(), u.markRedraw(), o) {
        ia(
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
    }, t.prototype.remove = function() {
      this.getContentGroup().removeAll(), this._isFirstRender = !0;
    }, t.type = "legend.plain", t;
  })(se)
);
function gE(e, t, r, n, i, a, o) {
  function s(p, g) {
    p.lineWidth === "auto" && (p.lineWidth = g.lineWidth > 0 ? 2 : 0), ih(p, function(m, _) {
      p[_] === "inherit" && (p[_] = g[_]);
    });
  }
  var u = t.getModel("itemStyle"), l = u.getItemStyle(), f = e.lastIndexOf("empty", 0) === 0 ? "fill" : "stroke", h = u.getShallow("decal");
  l.decal = !h || h === "inherit" ? n.decal : Wf(h, o), l.fill === "inherit" && (l.fill = n[i]), l.stroke === "inherit" && (l.stroke = n[f]), l.opacity === "inherit" && (l.opacity = (i === "fill" ? n : r).opacity), s(l, n);
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
function yE(e) {
  var t = e.icon || "roundRect", r = pu(t, 0, 0, e.itemWidth, e.itemHeight, e.itemStyle.fill, e.symbolKeepAspect);
  return r.setStyle(e.itemStyle), r.rotation = (e.iconRotate || 0) * Math.PI / 180, r.setOrigin([e.itemWidth / 2, e.itemHeight / 2]), t.indexOf("empty") > -1 && (r.style.stroke = r.style.fill, r.style.fill = j.color.neutral00, r.style.lineWidth = 2), r;
}
function _g(e, t, r, n) {
  oh(e, t, r, n), r.dispatchAction({
    type: "legendToggleSelect",
    name: e ?? t
  }), ah(e, t, r, n);
}
function ah(e, t, r, n) {
  r.usingTHL() || r.dispatchAction({
    type: "highlight",
    seriesName: e,
    name: t,
    excludeSeriesId: n
  });
}
function oh(e, t, r, n) {
  r.usingTHL() || r.dispatchAction({
    type: "downplay",
    seriesName: e,
    name: t,
    excludeSeriesId: n
  });
}
function Ui(e, t, r) {
  var n = e === "allSelect" || e === "inverseSelect", i = {}, a = [];
  r.eachComponent({
    mainType: "legend",
    query: t
  }, function(s) {
    n ? s[e]() : s[e](t.name), Sg(s, i), a.push(s.componentIndex);
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
    name: t.name,
    selected: o
  };
}
function Sg(e, t) {
  var r = t || {};
  return T(e.getData(), function(n) {
    var i = n.get("name");
    if (!(i === `
` || i === "")) {
      var a = e.isSelected(i);
      Xt(r, i) ? r[i] = r[i] && a : r[i] = a;
    }
  }), r;
}
function mE(e) {
  e.registerAction("legendToggleSelect", "legendselectchanged", Tt(Ui, "toggleSelected")), e.registerAction("legendAllSelect", "legendselectall", Tt(Ui, "allSelect")), e.registerAction("legendInverseSelect", "legendinverseselect", Tt(Ui, "inverseSelect")), e.registerAction("legendSelect", "legendselected", Tt(Ui, "select")), e.registerAction("legendUnSelect", "legendunselected", Tt(Ui, "unSelect"));
}
var _E = Dh(SE);
function SE(e) {
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
function bS(e) {
  e.registerComponentModel(nh), e.registerComponentView(SS), e.registerProcessor(e.PRIORITY.PROCESSOR.SERIES_FILTER, _E), e.registerSubTypeDefaulter("legend", function() {
    return "plain";
  }), mE(e);
}
var bE = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r;
    }
    return t.prototype.setScrollDataIndex = function(r) {
      this.option.scrollDataIndex = r;
    }, t.prototype.init = function(r, n, i) {
      var a = sc(r);
      e.prototype.init.call(this, r, n, i), bg(this, r, a);
    }, t.prototype.mergeOption = function(r, n) {
      e.prototype.mergeOption.call(this, r, n), bg(this, this.option, r);
    }, t.type = "legend.scroll", t.defaultOption = fx(nh.defaultOption, {
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
    }), t;
  })(nh)
);
function bg(e, t, r) {
  var n = e.getOrient(), i = [1, 1];
  i[n.index] = 0, _a(t, r, {
    type: "box",
    ignoreSize: !!i
  });
}
var wg = oe, Hl = ["width", "height"], Vl = ["x", "y"], wE = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = t.type, r.newlineDisabled = !0, r._currentIndex = 0, r;
    }
    return t.prototype.init = function() {
      e.prototype.init.call(this), this.group.add(this._containerGroup = new wg()), this._containerGroup.add(this.getContentGroup()), this.group.add(this._controllerGroup = new wg());
    }, t.prototype.resetInner = function() {
      e.prototype.resetInner.call(this), this._controllerGroup.removeAll(), this._containerGroup.removeClipPath(), this._containerGroup.__rectSize = null;
    }, t.prototype.renderInner = function(r, n, i, a, o, s, u) {
      var l = this;
      e.prototype.renderInner.call(this, r, n, i, a, o, s, u);
      var f = this._controllerGroup, h = n.get("pageIconSize", !0), v = B(h) ? h : [h, h];
      d("pagePrev", 0);
      var c = n.getModel("pageTextStyle");
      f.add(new Kt({
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
        var g = y + "DataIndex", m = su(n.get("pageIcons", !0)[n.getOrient().name][p], {
          // Buttons will be created in each render, so we do not need
          // to worry about avoiding using legendModel kept in scope.
          onclick: ht(l._pageGo, l, g, n, a)
        }, {
          x: -v[0] / 2,
          y: -v[1] / 2,
          width: v[0],
          height: v[1]
        });
        m.name = y, f.add(m);
      }
    }, t.prototype.layoutInner = function(r, n, i, a, o, s) {
      var u = this.getSelectorGroup(), l = r.getOrient().index, f = Hl[l], h = Vl[l], v = Hl[1 - l], c = Vl[1 - l];
      o && ia(
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
    }, t.prototype._layoutContentAndController = function(r, n, i, a, o, s, u, l) {
      var f = this.getContentGroup(), h = this._containerGroup, v = this._controllerGroup;
      ia(r.get("orient"), f, r.get("itemGap"), a ? i.width : null, a ? null : i.height), ia(
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
        w[o] = Math.max(i[o] - d[o] - _, 0), w[s] = b[s], h.setClipPath(new Rt({
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
      return C.pageIndex != null && pi(
        f,
        {
          x: C.contentPosition[0],
          y: C.contentPosition[1]
        },
        // When switch from "show controller" to "not show controller", view should be
        // updated immediately without animation, otherwise causes weird effect.
        y ? r : null
      ), this._updatePageInfoView(r, C), b;
    }, t.prototype._pageGo = function(r, n, i) {
      var a = this._getPageInfo(n)[r];
      a != null && i.dispatchAction({
        type: "legendScroll",
        scrollDataIndex: a,
        legendId: n.id
      });
    }, t.prototype._updatePageInfoView = function(r, n) {
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
    }, t.prototype._getPageInfo = function(r) {
      var n = r.get("scrollDataIndex", !0), i = this.getContentGroup(), a = this._containerGroup.__rectSize, o = r.getOrient().index, s = Hl[o], u = Vl[o], l = this._findTargetItemIndex(n), f = i.children(), h = f[l], v = f.length, c = v ? 1 : 0, d = {
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
    }, t.prototype._findTargetItemIndex = function(r) {
      if (!this._showController)
        return 0;
      var n, i = this.getContentGroup(), a;
      return i.eachChild(function(o, s) {
        var u = o.__legendDataIndex;
        a == null && u != null && (a = s), u === r && (n = s);
      }), n ?? a;
    }, t.type = "legend.scroll", t;
  })(SS)
);
function TE(e) {
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
function CE(e) {
  ci(bS), e.registerComponentModel(bE), e.registerComponentView(wE), TE(e);
}
function SR(e) {
  ci(bS), ci(CE);
}
var xE = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return t.prototype.init = function(r, n, i) {
      e.prototype.init.call(this, r, n, i), this._sourceManager = new N_(this), $d(this);
    }, t.prototype.mergeOption = function(r, n) {
      e.prototype.mergeOption.call(this, r, n), $d(this);
    }, t.prototype.optionUpdated = function() {
      this._sourceManager.dirty();
    }, t.prototype.getSourceManager = function() {
      return this._sourceManager;
    }, t.type = "dataset", t.defaultOption = {
      seriesLayoutBy: Ie
    }, t;
  })(at)
), ME = (
  /** @class */
  (function(e) {
    z(t, e);
    function t() {
      var r = e !== null && e.apply(this, arguments) || this;
      return r.type = "dataset", r;
    }
    return t.type = "dataset", t;
  })(se)
);
function bR(e) {
  e.registerComponentModel(xE), e.registerComponentView(ME);
}
function Tg(e, t, r) {
  var n = Lt.createCanvas(), i = t.getWidth(), a = t.getHeight(), o = n.style;
  return o && (o.position = "absolute", o.left = "0", o.top = "0", o.width = i + "px", o.height = a + "px", n.setAttribute("data-zr-dom-id", e)), n.width = i * r, n.height = a * r, n;
}
function Gl(e) {
  return !e.__cursors.get(Nm);
}
function Cg(e) {
  var t = e.__cursors.get(Nm);
  return {
    startIdx: t ? t.startIdx : 0,
    endIdx: t ? t.endIdx : 0
  };
}
var wS = (function(e) {
  z(t, e);
  function t(r, n, i) {
    var a = e.call(this) || this;
    a.motionBlur = !1, a.lastFrameAlpha = 0.7, a.dpr = 1, a.virtual = !1, a.config = {}, a.zlevel = 0, a.zlevel2 = Vo, a.maxRepaintRectCount = 5, a.__dirty = !0, a.__firstTimePaint = !0, a.__prevIdx = { startIdx: 0, endIdx: 0 };
    var o;
    i = i || us, typeof r == "string" ? o = Tg(r, n, i) : F(r) && (o = r, r = o.id), a.id = r, a.dom = o;
    var s = o.style;
    return s && (ph(o), o.onselectstart = function() {
      return !1;
    }, s.padding = "0", s.margin = "0", s.borderWidth = "0"), a.painter = n, a.dpr = i, a;
  }
  return t.prototype.afterBrush = function() {
    this.__prevIdx = Cg(this);
  }, t.prototype.initContext = function() {
    this.ctx = this.dom.getContext("2d"), this.ctx.dpr = this.dpr;
  }, t.prototype.setUnpainted = function() {
    this.__firstTimePaint = !0;
  }, t.prototype.createBackBuffer = function() {
    var r = this.dpr;
    this.domBack = Tg("back-" + this.id, this.painter, r), this.ctxBack = this.domBack.getContext("2d"), r !== 1 && this.ctxBack.scale(r, r);
  }, t.prototype.createRepaintRects = function(r, n, i, a) {
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
        var d = c.shouldBePainted(i, a, !0, !0), y = c.__isRendered && (c.__dirty & Qt || !d) ? c.getPrevPaintRect() : null;
        y && f(y);
        var p = d && (c.__dirty & Qt || !c.__isRendered) ? c.getPaintRect() : null;
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
    function d(y, p, g, m) {
      if (o.clearRect(y, p, g, m), n && n !== "transparent") {
        var _ = void 0;
        if (Da(n)) {
          var S = n.global || n.__width === g && n.__height === m;
          _ = S && n.__canvasGradient || Uf(o, n, {
            x: 0,
            y: 0,
            width: g,
            height: m
          }), n.__canvasGradient = _, n.__width = g, n.__height = m;
        } else oy(n) && (n.scaleX = n.scaleX || h, n.scaleY = n.scaleY || h, _ = Yf(o, n, {
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
  }, t;
})(qe), xg = 1e5, sn = 314159, Ul = void 0, DE = 1, Yl = 2;
function IE(e) {
  return e ? e.__builtin__ ? !0 : !(typeof e.resize != "function" || typeof e.refresh != "function") : !1;
}
function AE(e, t) {
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
function Mg(e, t, r, n) {
  var i = new wS(e, t, t.dpr);
  return i.zlevel = r, i.zlevel2 = n, i.__builtin__ = !0, TS(i), i;
}
function TS(e) {
  e.__cursorStack = [], e.__cursors = U();
}
function PE(e) {
  return e.startIdx = e.drawIdx = e.endIdx = e.endIdxNew = 0, e.used = !1, e.first = e.last = NaN, e.notClearIdx = -1, e;
}
function LE(e, t) {
  var r = e.__cursors, n = +t;
  return r.get(n) || (e.__cursorStack.push(n), r.set(n, PE({ key: n })));
}
function Io(e, t) {
  for (var r = e.__cursorStack, n = 0; n < r.length; n++)
    t(e.__cursors.get(r[n]));
}
function Wl(e, t) {
  var r = e.layers;
  return r[t] || (r[t] = new Array(3));
}
function kt(e, t, r) {
  for (var n = e.layerStack, i = 0; i < n.length; i++) {
    var a = n[i].zl, o = n[i].zl2, s = e.layers[a][o];
    (!r || (!(r & sa) || s.__builtin__) && (!(r & sh) || !s.__builtin__) && (!(r & CS) || s !== e.hoverlayer)) && t(s, a, o, i);
  }
}
var sa = 1, sh = 2, CS = 4, Ao = sa | CS, EE = (function() {
  function e(t, r, n, i) {
    this.type = "canvas", this._prevDisplayList = [], this._layerConfig = {}, this._needsManuallyCompositing = !1, this.type = "canvas", this._i = {
      layerStack: [],
      layers: []
    };
    var a = !t.nodeName || t.nodeName.toUpperCase() === "CANVAS";
    this._opts = n = R({}, n || {}), this.dpr = n.devicePixelRatio || us, this._singleCanvas = a, this.root = t;
    var o = t.style;
    if (o && (ph(t), t.innerHTML = ""), this.storage = r, this._prevDisplayList = [], a) {
      var u = t, l = u.width, f = u.height;
      n.width != null && (l = n.width), n.height != null && (f = n.height), this.dpr = n.devicePixelRatio || 1, u.width = l * this.dpr, u.height = f * this.dpr, this._width = l, this._height = f;
      var h = Mg(u, this, sn, Vo);
      h.initContext(), this._insertLayer(h, sn, Vo, !0), this._domRoot = t;
    } else {
      this._width = go(t, 0, n), this._height = go(t, 1, n);
      var s = this._domRoot = AE(this._width, this._height);
      t.appendChild(s);
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
    var r;
    t && !F(t) ? r = { paintAll: !!t } : r = t || {};
    var n = G(r.refresh, !0), i = G(r.refreshHover, !1);
    if (i && (this._hoverLayerDirty = Yl), !n)
      return i && this._paintHoverList(this.storage.getDisplayList(!1)), this;
    var a = this.storage.getDisplayList(!0);
    this._updateLayerStatus(a, r.paintAll), this._redrawId = Math.random();
    var o = this._prevDisplayList;
    this._paintList(a, o, this._redrawId);
    var s = this._backgroundColor;
    return kt(this._i, function(u, l, f, h) {
      u.refresh && u.refresh(h === 0 ? s : null);
    }, sh), this._opts.useDirtyRect && (this._prevDisplayList = a.slice()), this;
  }, e.prototype._paintHoverList = function(t) {
    var r = this._i.hoverlayer, n = this._hoverLayerDirty;
    if (this._hoverLayerDirty = Ul, n !== Ul && (!r && n === Yl && (r = this._i.hoverlayer = this._ensureLayer(xg)), !!r)) {
      r.clear();
      for (var i = {
        inHover: !0,
        viewWidth: this._width,
        viewHeight: this._height,
        beforeBrushParam: {}
      }, a, o = 0, s = t.length; o < s; o++) {
        var u = t[o];
        if (u.__inHover) {
          a || (a = r.ctx, a.save());
          var l = u.__hoverStyle, f = void 0;
          l && (f = u.style, u.style = l), vn(a, u, i), l && (u.style = f);
        }
      }
      a && (ni(a, i), a.restore());
    }
  }, e.prototype.getHoverLayer = function() {
    return this._ensureLayer(xg);
  }, e.prototype.paintOne = function(t, r) {
    dc(t, r);
  }, e.prototype._paintList = function(t, r, n) {
    if (this._redrawId === n) {
      var i = this._doPaintList(t, r);
      if (this._needsManuallyCompositing && this._compositeManually(), i)
        kt(this._i, function(o) {
          o.afterBrush && o.afterBrush();
        }, Ao), this._paintHoverList(t);
      else {
        var a = this;
        ns(function() {
          a._paintList(t, r, n);
        });
      }
    }
  }, e.prototype._compositeManually = function() {
    var t = this._ensureLayer(sn).ctx, r = this._domRoot.width, n = this._domRoot.height;
    t.clearRect(0, 0, r, n), kt(this._i, function(i) {
      i.virtual && t.drawImage(i.dom, 0, 0, r, n);
    }, sa);
  }, e.prototype._doPaintList = function(t, r) {
    var n = this, i = !0;
    return kt(this._i, function(a) {
      var o = !1;
      if (Io(a, function(h) {
        (h.drawIdx < h.endIdx || h.notClearIdx >= 0) && (o = !0);
      }), !(!o && !a.__dirty)) {
        var s = n._opts.useDirtyRect && !Gl(a) ? a.createRepaintRects(t, r, n._width, n._height) : null, u = n._i.layerStack[0], l = !0;
        if (a.__dirty) {
          l = !1, a.__dirty = !1;
          var f = a.zlevel === u.zl && a.zlevel2 === u.zl2 ? n._backgroundColor : null;
          a.clear(!1, f, s);
        }
        Io(a, function(h) {
          var v = n._paintPerCursor(a, h, t, s, l);
          i = i && v;
        });
      }
    }, Ao), W.wxa && kt(this._i, function(a) {
      a && a.ctx && a.ctx.draw && a.ctx.draw();
    }), i;
  }, e.prototype._paintPerCursor = function(t, r, n, i, a) {
    var o = t.ctx;
    if (i)
      if (!i.length)
        r.drawIdx = r.endIdx;
      else
        for (var s = this.dpr, u = 0; u < i.length; ++u) {
          var l = i[u];
          o.save(), o.beginPath(), o.rect(l.x * s, l.y * s, l.width * s, l.height * s), o.clip(), this._paintPerCursorInRect(t, r, n, l, a), o.restore();
        }
    else
      o.save(), this._paintPerCursorInRect(t, r, n, null, a), o.restore();
    return r.drawIdx >= r.endIdx;
  }, e.prototype._paintPerCursorInRect = function(t, r, n, i, a) {
    for (var o = {
      inHover: !1,
      allClipped: !1,
      prevEl: null,
      viewWidth: this._width,
      viewHeight: this._height,
      beforeBrushParam: { contentRetained: a }
    }, s = t.ctx, u = Gl(t), l = u && Lt.getTime(), f = r.drawIdx, h = r.notClearIdx, v = h >= 0 ? Math.min(h, f) : f; v < r.endIdx; v++) {
      var c = n[v];
      if (!(v < f && !c.notClear)) {
        if (c.__inHover && (this._hoverLayerDirty = Yl), i != null) {
          var d = c.getPaintRect();
          d && d.intersect(i) && (vn(s, c, o), c.setPrevPaintRect(d));
        } else
          vn(s, c, o);
        if (u) {
          var y = Lt.getTime() - l;
          if (y > 15) {
            v++;
            break;
          }
        }
      }
    }
    ni(s, o), r.drawIdx = Math.max(v, f);
  }, e.prototype.getLayer = function(t, r) {
    return this._ensureLayer(t, 0, r);
  }, e.prototype._ensureLayer = function(t, r, n) {
    r = r || 0;
    var i = this._singleCanvas;
    i && !this._needsManuallyCompositing && (t = sn, r = 0);
    var a = Wl(this._i, t)[r];
    return a || (a = Mg("zr_" + t + "." + r, this, t, r), this._layerConfig[t] && pt(a, this._layerConfig[t], !0), (n || i && t !== sn) && (a.virtual = !0), this._insertLayer(a, t, r, !1), a.initContext()), a;
  }, e.prototype.insertLayer = function(t, r) {
    this._insertLayer(r, t, 0, !1);
  }, e.prototype._insertLayer = function(t, r, n, i) {
    var a = this._i, o = a.layers, s = a.layerStack, u = this._domRoot, l = null;
    if (!(o[r] && o[r][n]) && IE(t)) {
      for (var f = s.length, h = 0; h < f && (s[h].zl < r || s[h].zl === r && s[h].zl2 < n); )
        h++;
      if (h > 0 && (l = Wl(a, s[h - 1].zl)[s[h - 1].zl2]), s.splice(h, 0, { zl: r, zl2: n }), Wl(a, r)[n] = t, !i && !t.virtual)
        if (l) {
          var v = l.dom;
          v.nextSibling ? u.insertBefore(t.dom, v.nextSibling) : u.appendChild(t.dom);
        } else
          u.firstChild ? u.insertBefore(t.dom, u.firstChild) : u.appendChild(t.dom);
      t.painter || (t.painter = this);
    }
  }, e.prototype.eachLayer = function(t, r) {
    return kt(this._i, function(n, i) {
      t.call(r, n, i);
    });
  }, e.prototype.eachBuiltinLayer = function(t, r) {
    return kt(this._i, function(n, i) {
      t.call(r, n, i);
    }, sa);
  }, e.prototype.eachOtherLayer = function(t, r) {
    return kt(this._i, function(n, i) {
      t.call(r, n, i);
    }, sh);
  }, e.prototype.getLayers = function() {
    var t = {};
    return kt(this._i, function(r, n, i) {
      t[r.id] = r;
    }), t;
  }, e.prototype._updateLayerStatus = function(t, r) {
    var n = this;
    if (n._singleCanvas)
      for (var i = 1; i < t.length; i++) {
        var a = t[i];
        if (a.zlevel !== t[i - 1].zlevel || a.incremental) {
          n._needsManuallyCompositing = !0;
          break;
        }
      }
    kt(n._i, function(p) {
      p.__dirty = !1, Io(p, function(g) {
        g.used = !1, g.endIdxNew = 0, g.notClearIdx = -1;
      });
    }, Ao);
    for (var o, s = null, u = null, l = !1, f = 0, h = t.length; f < h; f++) {
      var a = t[f], v = a.zlevel, c = a.incremental, d = void 0;
      if (o !== v && (o = v, l = !1), c ? (l = !0, d = RC) : d = l ? EC : Vo, (!s || v !== s.zlevel || d !== s.zlevel2) && (s = n._ensureLayer(v, d), u = null, !s.__builtin__)) {
        zs("ZLevel " + v + " has been used by unknown layer " + s.id);
        continue;
      }
      if ((!u || c !== u.key) && (u = LE(s, c), !u.used))
        if (u.used = !0, !r && u.first === a.id) {
          var y = f - u.startIdx;
          u.startIdx = f, u.drawIdx += y, u.endIdx += y;
        } else
          s.__dirty = !0, u.first = a.id, u.startIdx = u.drawIdx = f, u.endIdx = f + 1;
      u.endIdxNew = f + 1, a.__dirty & Qt && !a.__inHover && ((!c || !a.notClear && f < u.drawIdx) && (s.__dirty = !0), c && a.notClear && u.notClearIdx < 0 && (u.notClearIdx = f));
    }
    kt(n._i, function(p) {
      for (var g = p.__cursorStack, m = p.__cursors, _ = g.length - 1; _ >= 0; _--) {
        var S = m.get(g[_]);
        if (!S.used)
          p.__dirty = !0, m.removeKey(g[_]), g.splice(_, 1);
        else {
          var b = S.endIdxNew;
          (Gl(p) ? b < S.drawIdx : b !== S.endIdx || !b || t[b - 1].id !== S.last) && (p.__dirty = !0), S.endIdx = S.endIdxNew, S.last = b ? t[b - 1].id : NaN;
        }
      }
      p.__dirty && (Io(p, function(w) {
        w.drawIdx = w.startIdx;
      }), n._hoverLayerDirty === Ul && (n._hoverLayerDirty = DE));
    }, Ao);
  }, e.prototype.clear = function() {
    return kt(this._i, function(t) {
      t.clear(), TS(t);
    }, sa), this;
  }, e.prototype.setBackgroundColor = function(t) {
    this._backgroundColor = t, kt(this._i, function(r) {
      r.setUnpainted();
    });
  }, e.prototype.configLayer = function(t, r) {
    if (r) {
      var n = this._layerConfig;
      n[t] ? pt(n[t], r, !0) : n[t] = r, kt(this._i, function(i, a) {
        pt(i, n[a], !0);
      });
    }
  }, e.prototype.delLayer = function(t) {
    for (var r = this._i.layerStack, n = this._i.layers, i = r.length - 1; i >= 0; i--) {
      var a = r[i];
      if (a.zl === t) {
        var o = n[t][a.zl2];
        if (o.__builtin__)
          continue;
        if (r.splice(i, 1), n[t][a.zl2] = void 0, !o.virtual) {
          var s = o.dom.parentNode;
          s && s.removeChild(o.dom);
        }
      }
    }
  }, e.prototype.resize = function(t, r) {
    if (this._domRoot.style) {
      var n = this._domRoot;
      n.style.display = "none";
      var i = this._opts, a = this.root;
      t != null && (i.width = t), r != null && (i.height = r), t = go(a, 0, i), r = go(a, 1, i), n.style.display = "", (this._width !== t || r !== this._height) && (n.style.width = t + "px", n.style.height = r + "px", kt(this._i, function(o) {
        o.resize(t, r);
      }), this.refresh({ paintAll: !0 })), this._width = t, this._height = r;
    } else {
      if (t == null || r == null)
        return;
      this._width = t, this._height = r, this._ensureLayer(sn).resize(t, r);
    }
    return this;
  }, e.prototype.clearLayer = function(t) {
    T(this._i.layers[t], function(r) {
      r && !r.__builtin__ && r.clear();
    });
  }, e.prototype.dispose = function() {
    this.root.innerHTML = "", this.root = this.storage = this._domRoot = this._i = null;
  }, e.prototype.getRenderedCanvas = function(t) {
    if (t = t || {}, this._singleCanvas && !this._compositeManually)
      return this._i.layers[sn][0].dom;
    var r = new wS("image", this, t.pixelRatio || this.dpr);
    r.initContext(), r.clear(!1, t.backgroundColor || this._backgroundColor);
    var n = r.ctx;
    if (t.pixelRatio <= this.dpr) {
      this.refresh();
      var i = r.dom.width, a = r.dom.height;
      kt(this._i, function(h) {
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
        vn(n, f, o);
      }
      ni(n, o);
    }
    return r.dom;
  }, e.prototype.getWidth = function() {
    return this._width;
  }, e.prototype.getHeight = function() {
    return this._height;
  }, e;
})();
function wR(e) {
  e.registerPainter("canvas", EE);
}
export {
  NC as $,
  FP as A,
  JE as B,
  at as C,
  Ft as D,
  xe as E,
  T as F,
  Xt as G,
  Jt as H,
  gt as I,
  sA as J,
  Xe as K,
  QE as L,
  aw as M,
  Nw as N,
  fx as O,
  yt as P,
  Hy as Q,
  Rt as R,
  eu as S,
  vt as T,
  Qs as U,
  H as V,
  li as W,
  t_ as X,
  Nh as Y,
  pi as Z,
  z as _,
  It as a,
  Hm as a$,
  HE as a0,
  Yh as a1,
  oe as a2,
  ft as a3,
  Ue as a4,
  R as a5,
  VE as a6,
  jC as a7,
  GE as a8,
  FE as a9,
  qE as aA,
  kE as aB,
  sw as aC,
  qs as aD,
  Mn as aE,
  ur as aF,
  sR as aG,
  zx as aH,
  ys as aI,
  F as aJ,
  st as aK,
  LA as aL,
  EA as aM,
  zA as aN,
  oR as aO,
  YE as aP,
  D0 as aQ,
  mt as aR,
  lR as aS,
  KE as aT,
  pR as aU,
  tL as aV,
  o1 as aW,
  Lf as aX,
  Pc as aY,
  xa as aZ,
  Ls as a_,
  zE as aa,
  BE as ab,
  vc as ac,
  L0 as ad,
  P0 as ae,
  ar as af,
  Ae as ag,
  HA as ah,
  _t as ai,
  pt as aj,
  ms as ak,
  Ep as al,
  PA as am,
  F0 as an,
  di as ao,
  Y as ap,
  ie as aq,
  yR as ar,
  yx as as,
  JA as at,
  _u as au,
  mA as av,
  Wy as aw,
  wh as ax,
  za as ay,
  FA as az,
  N as b,
  cR as b$,
  Vm as b0,
  Qb as b1,
  G as b2,
  gR as b3,
  $E as b4,
  Fp as b5,
  ZE as b6,
  Er as b7,
  rL as b8,
  nL as b9,
  PL as bA,
  mR as bB,
  LL as bC,
  IL as bD,
  Gs as bE,
  rs as bF,
  Vh as bG,
  Ar as bH,
  Se as bI,
  zy as bJ,
  Z as bK,
  Zm as bL,
  ct as bM,
  Kt as bN,
  Tn as bO,
  uu as bP,
  ru as bQ,
  ka as bR,
  Zb as bS,
  $t as bT,
  WE as bU,
  XE as bV,
  u1 as bW,
  AM as bX,
  Pe as bY,
  qt as bZ,
  OE as b_,
  aR as ba,
  dR as bb,
  nR as bc,
  yA as bd,
  jE as be,
  ua as bf,
  dt as bg,
  UC as bh,
  wL as bi,
  An as bj,
  Hh as bk,
  se as bl,
  YI as bm,
  Tt as bn,
  Wx as bo,
  KC as bp,
  NE as bq,
  tR as br,
  uw as bs,
  zt as bt,
  Yt as bu,
  ci as bv,
  ZL as bw,
  CL as bx,
  xL as by,
  TL as bz,
  X as c,
  ry as c$,
  bR as c0,
  SR as c1,
  _R as c2,
  wR as c3,
  rd as c4,
  iu as c5,
  Lr as c6,
  ut as c7,
  py as c8,
  b1 as c9,
  GP as cA,
  bP as cB,
  BI as cC,
  mc as cD,
  kI as cE,
  zI as cF,
  SP as cG,
  hP as cH,
  dc as cI,
  z1 as cJ,
  mP as cK,
  Zp as cL,
  Pn as cM,
  b0 as cN,
  NI as cO,
  w0 as cP,
  Tc as cQ,
  qh as cR,
  T0 as cS,
  S0 as cT,
  Sc as cU,
  bc as cV,
  _c as cW,
  C0 as cX,
  yu as cY,
  Br as cZ,
  FI as c_,
  hi as ca,
  rb as cb,
  J1 as cc,
  Ja as cd,
  Pa as ce,
  Jp as cf,
  xt as cg,
  fR as ch,
  hR as ci,
  $P as cj,
  ea as ck,
  UE as cl,
  _0 as cm,
  LI as cn,
  f0 as co,
  db as cp,
  EI as cq,
  HI as cr,
  dI as cs,
  RI as ct,
  m0 as cu,
  OI as cv,
  W as cw,
  UP as cx,
  HP as cy,
  VP as cz,
  rR as d,
  _P as d0,
  wP as d1,
  w1 as d2,
  vI as d3,
  c1 as d4,
  qb as d5,
  ae as d6,
  sP as d7,
  ac as d8,
  Ut as d9,
  nc as da,
  g_ as db,
  Ax as dc,
  QI as dd,
  pf as de,
  af as df,
  pu as dg,
  Yy as dh,
  dh as di,
  uP as dj,
  lP as dk,
  U as e,
  Sh as f,
  fr as g,
  Ec as h,
  B as i,
  EM as j,
  GA as k,
  dr as l,
  zp as m,
  bu as n,
  iR as o,
  LM as p,
  sc as q,
  _s as r,
  _a as s,
  j as t,
  eR as u,
  vR as v,
  R0 as w,
  lD as x,
  Rw as y,
  uR as z
};
